"""
MergedLeafTranslator for re-distributing LEAF datasets.

This translator merges all users from LEAF datasets (femnist, celeba, etc.)
and re-splits them using the configured splitter (LDA, IID, etc.).

支持两种模式：
1. 原始模式：加载全部数据后合并分割
2. 延迟加载模式：只加载标签，分割后再延迟加载实际数据（内存高效）
"""

import logging
import numpy as np
from torch.utils.data import ConcatDataset, Dataset, Subset
from federatedscope.core.data.base_translator import BaseDataTranslator
from federatedscope.core.data import ClientData, StandaloneDataDict
from federatedscope.core.auxiliaries.splitter_builder import get_splitter
from federatedscope.core.splitters.utils import dirichlet_distribution_noniid_slice

logger = logging.getLogger(__name__)


class MergedLeafTranslator(BaseDataTranslator):
    """
    Translator that merges all LEAF users' data and re-distributes using splitter.

    For LEAF datasets (femnist, celeba, shakespeare, etc.), this translator:
    1. Merges all pre-split users into a single dataset
    2. Re-splits using the configured splitter (LDA, IID, etc.)
    3. Creates balanced or heterogeneous clients based on splitter config

    支持延迟加载模式（当检测到 __lazy_merged__ 标记时自动启用）：
    - 只加载标签进行 LDA 分割
    - 分割后每个 client 只加载自己的数据
    - 内存峰值从 N×全量数据 降到 全量数据/N

    Args:
        global_cfg: Global configuration
        client_cfgs: Client-specific configurations
    """

    def split(self, dataset):
        """
        Merge LEAF users and re-split using splitter.

        Args:
            dataset: LEAF dataset dict with format:
                {client_id: {'train': data, 'test': data, 'val': data}}
                或延迟加载格式:
                {'__lazy_merged__': {'train': LazyLoadDataset, ...}}

        Returns:
            dict: dict of ClientData with client_idx as key
        """
        if not isinstance(dataset, dict):
            raise TypeError(f'Expected dict for LEAF dataset, got {type(dataset)}')

        # 检查是否是延迟加载模式
        if '__lazy_merged__' in dataset:
            datadict = self._split_lazy(dataset['__lazy_merged__'])
        else:
            datadict = self._split_original(dataset)

        # === 关键优化：只保留当前进程需要的 client，丢掉其他 client 的数据 ===
        # 这样每个进程的内存使用与 client_num 无关，只与自己负责的 client 有关
        datadict = self._filter_to_current_client(datadict)

        return datadict

    def _filter_to_current_client(self, datadict):
        """
        过滤 datadict，只保留当前进程负责的 client 数据。

        在分布式模式下，每个进程只需要自己那个 client 的数据，
        不需要为所有 client 创建 DataLoader，这样可以大幅减少内存使用。

        内存优化效果：
        - 修改前：每个进程内存 ∝ client_num（所有 client 的 DataLoader）
        - 修改后：每个进程内存 ∝ 1（只有自己的 DataLoader）
        """
        # 获取当前进程负责的 client ID
        # distribute.data_idx: 0 表示 server，1~N 表示 client
        my_data_idx = getattr(self.global_cfg.distribute, 'data_idx', None)

        # 如果没有设置 data_idx 或者是 standalone 模式，保留所有数据
        if my_data_idx is None:
            return datadict

        # Server (data_idx=0) 需要保留 server 数据
        if my_data_idx == 0:
            # Server 只需要 key=0 的数据，不需要 client 数据
            if 0 in datadict:
                return {0: datadict[0]}
            return datadict

        # Client 进程：只保留自己的数据和 server 的基本数据
        filtered = {}

        # 保留 server 数据（但清空 train 以节省内存）
        if 0 in datadict:
            server_cd = datadict[0]
            # Server 的 train 数据对 client 没用，清空以节省内存
            server_cd.train_data = None
            filtered[0] = server_cd

        # 保留当前 client 的数据
        if my_data_idx in datadict:
            filtered[my_data_idx] = datadict[my_data_idx]
            logger.info(f"🎯 Filtered datadict: keeping only client {my_data_idx} "
                       f"(dropped {len(datadict) - len(filtered)} other clients)")
        else:
            raise ValueError(f"client data_idx {my_data_idx} not in datadict keys {list(datadict.keys())}")

        return filtered

    def _split_lazy(self, lazy_data):
        """
        延迟加载模式的分割逻辑。

        只使用标签进行 LDA 分割，然后创建指向原始数据的子集。
        内存高效：分割阶段只需要 ~1MB（标签），训练阶段每个 client 只加载自己的数据。

        Args:
            lazy_data: dict with {'train': LazyLoadDataset, 'val': ..., 'test': ...}

        Returns:
            dict: dict of ClientData with client_idx as key
        """
        from federatedscope.cv.dataset.leaf_cv import LazyLoadDataset

        client_num = self.global_cfg.federate.client_num

        logger.info(f"🚀 MergedLeafTranslator: Using LAZY-LOAD mode")
        logger.info(f"   Memory efficient: only labels loaded for splitting")

        train_dataset = lazy_data['train']
        val_dataset = lazy_data['val']
        test_dataset = lazy_data['test']

        logger.info(f"📊 Dataset sizes - Train: {len(train_dataset)}, "
                   f"Val: {len(val_dataset)}, Test: {len(test_dataset)}")

        # 获取标签（内存占用极小）
        train_labels = np.array(train_dataset.get_labels())
        val_labels = np.array(val_dataset.get_labels()) if len(val_dataset) > 0 else None
        test_labels = np.array(test_dataset.get_labels()) if len(test_dataset) > 0 else None

        # 使用 LDA 分割（只基于标签，不需要加载实际数据）
        splitter_type = self.global_cfg.data.splitter
        if splitter_type == 'lda':
            alpha = 0.5  # 默认值
            if self.global_cfg.data.splitter_args:
                for arg in self.global_cfg.data.splitter_args:
                    if 'alpha' in arg:
                        alpha = arg['alpha']

            logger.info(f"🎲 Using LDA splitter with alpha={alpha}")

            # 分割训练集
            train_idx_slice = dirichlet_distribution_noniid_slice(
                train_labels, client_num, alpha)

            # 使用相同的标签分布分割验证集和测试集
            train_label_distribution = [train_labels[idxs].tolist() for idxs in train_idx_slice]

            if val_labels is not None and len(val_labels) > 0:
                val_idx_slice = dirichlet_distribution_noniid_slice(
                    val_labels, client_num, alpha, prior=train_label_distribution)
            else:
                val_idx_slice = [[] for _ in range(client_num)]

            if test_labels is not None and len(test_labels) > 0:
                if self.global_cfg.data.share_test_dataset:
                    # 所有 client 共享完整测试集
                    test_idx_slice = [list(range(len(test_labels))) for _ in range(client_num)]
                else:
                    test_idx_slice = dirichlet_distribution_noniid_slice(
                        test_labels, client_num, alpha, prior=train_label_distribution)
            else:
                test_idx_slice = [[] for _ in range(client_num)]

        else:
            # IID 分割
            logger.info(f"🎲 Using IID splitter")
            indices = np.random.permutation(len(train_labels))
            split_size = len(train_labels) // client_num
            train_idx_slice = [indices[i*split_size:(i+1)*split_size].tolist()
                              for i in range(client_num)]

            if val_labels is not None and len(val_labels) > 0:
                val_indices = np.random.permutation(len(val_labels))
                val_split_size = len(val_labels) // client_num
                val_idx_slice = [val_indices[i*val_split_size:(i+1)*val_split_size].tolist()
                                for i in range(client_num)]
            else:
                val_idx_slice = [[] for _ in range(client_num)]

            if test_labels is not None and len(test_labels) > 0:
                if self.global_cfg.data.share_test_dataset:
                    test_idx_slice = [list(range(len(test_labels))) for _ in range(client_num)]
                else:
                    test_indices = np.random.permutation(len(test_labels))
                    test_split_size = len(test_labels) // client_num
                    test_idx_slice = [test_indices[i*test_split_size:(i+1)*test_split_size].tolist()
                                     for i in range(client_num)]
            else:
                test_idx_slice = [[] for _ in range(client_num)]

        # === 关键优化：只为当前进程负责的 client 创建数据 ===
        my_data_idx = getattr(self.global_cfg.distribute, 'data_idx', None)

        data_dict = {}

        # Server 数据 (key=0)
        if my_data_idx is None or my_data_idx == 0:
            # Standalone 或 Server：保留完整数据
            data_dict[0] = ClientData(self.global_cfg,
                                     train=train_dataset,
                                     val=val_dataset,
                                     test=test_dataset)
        else:
            # Client 进程：server 不需要 train
            data_dict[0] = ClientData(self.global_cfg,
                                     train=None,
                                     val=val_dataset,
                                     test=test_dataset)

        for client_id in range(1, client_num + 1):
            # === 关键：跳过不属于当前进程的 client ===
            if my_data_idx is not None and my_data_idx > 0:
                if client_id != my_data_idx:
                    continue  # 不创建 subset，不加载数据

            idx = client_id - 1

            # 创建该 client 的子集（lazy=False 会预加载数据）
            client_train = train_dataset.subset(train_idx_slice[idx]) if train_idx_slice[idx] else []
            client_val = val_dataset.subset(val_idx_slice[idx]) if val_idx_slice[idx] else []
            client_test = test_dataset.subset(test_idx_slice[idx]) if test_idx_slice[idx] else []

            if self.client_cfgs is not None:
                client_cfg = self.global_cfg.clone()
                client_cfg.merge_from_other_cfg(
                    self.client_cfgs.get(f'client_{client_id}'))
            else:
                client_cfg = self.global_cfg

            data_dict[client_id] = ClientData(client_cfg,
                                              train=client_train,
                                              val=client_val,
                                              test=client_test)

            logger.info(f"🎯 _split_lazy: Created data for client {client_id} only")

        logger.info(f"✅ Re-distributed into {client_num} clients using lazy-load mode")
        logger.info(f"   Each client will only load its own data subset during training")

        return data_dict

    def _split_original(self, dataset):
        """
        原始模式的分割逻辑（保持向后兼容）。

        Args:
            dataset: LEAF dataset dict with format:
                {client_id: {'train': data, 'test': data, 'val': data}}

        Returns:
            dict: dict of ClientData with client_idx as key
        """
        logger.info(f"🔄 MergedLeafTranslator: Merging {len(dataset)} LEAF users "
                   f"before re-splitting into {self.global_cfg.federate.client_num} clients")

        # 1. Collect all data from LEAF users
        all_train_datasets = []
        all_val_datasets = []
        all_test_datasets = []

        for client_id, client_data in dataset.items():
            if 'train' in client_data and len(client_data['train']) > 0:
                all_train_datasets.append(client_data['train'])
            if 'val' in client_data and len(client_data['val']) > 0:
                all_val_datasets.append(client_data['val'])
            if 'test' in client_data and len(client_data['test']) > 0:
                all_test_datasets.append(client_data['test'])

        # 2. Merge into single datasets using ConcatDataset
        # ConcatDataset preserves the original dataset structure while concatenating
        merged_train = ConcatDataset(all_train_datasets) if all_train_datasets else []
        merged_val = ConcatDataset(all_val_datasets) if all_val_datasets else []
        merged_test = ConcatDataset(all_test_datasets) if all_test_datasets else []

        logger.info(f"📊 Merged dataset sizes - Train: {len(merged_train)}, "
                   f"Val: {len(merged_val)}, Test: {len(merged_test)}")

        # 3. Use BaseDataTranslator's split_to_client to re-distribute
        # This will use the configured splitter (LDA/IID) to create new client splits
        datadict = self.split_to_client(merged_train, merged_val, merged_test)

        logger.info(f"✅ Re-distributed into {len(datadict)-1} clients using "
                   f"'{self.global_cfg.data.splitter}' splitter")

        return datadict
