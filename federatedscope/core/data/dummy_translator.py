import logging
from federatedscope.core.data.base_translator import BaseDataTranslator
from federatedscope.core.data.base_data import ClientData

logger = logging.getLogger(__name__)


class DummyDataTranslator(BaseDataTranslator):
    """
    ``DummyDataTranslator`` convert datadict to ``StandaloneDataDict``. \
    Compared to ``core.data.base_translator.BaseDataTranslator``, it do not \
    perform FL split.
    """
    def split(self, dataset):
        """
        Perform ML split

        Returns:
            dict of ``ClientData`` with client_idx as key to build \
            ``StandaloneDataDict``
        """
        if not isinstance(dataset, dict):
            raise TypeError(f'Not support data type {type(dataset)}')

        # 获取当前进程负责的 client ID（分布式模式下的优化）
        my_data_idx = getattr(self.global_cfg.distribute, 'data_idx', None)

        datadict = {}
        for client_id in dataset.keys():
            # === 关键优化：只为当前进程负责的 client 创建 ClientData ===
            # 这样每个进程的内存使用与 client_num 无关
            if my_data_idx is not None and my_data_idx > 0:
                # Client 进程：只处理自己的数据
                if client_id != my_data_idx and client_id != 0:
                    continue
            elif my_data_idx == 0:
                # Server 进程：只处理 server 数据 (client_id=0 在 LEAF 中可能不存在)
                pass  # Server 需要看到所有数据用于评估

            if self.client_cfgs is not None:
                client_cfg = self.global_cfg.clone()
                client_cfg.merge_from_other_cfg(
                    self.client_cfgs.get(f'client_{client_id}'))
            else:
                client_cfg = self.global_cfg

            if isinstance(dataset[client_id], dict):
                datadict[client_id] = ClientData(client_cfg,
                                                 **dataset[client_id])
            else:
                # Do not have train/val/test
                train, val, test = self.split_train_val_test(
                    dataset[client_id], client_cfg)
                tmp_dict = dict(train=train, val=val, test=test)
                # Only for graph-level task, get number of graph labels
                if client_cfg.model.task.startswith('graph') and \
                        client_cfg.model.out_channels == 0:
                    s = set()
                    for g in dataset[client_id]:
                        s.add(g.y.item())
                    tmp_dict['num_label'] = len(s)
                datadict[client_id] = ClientData(client_cfg, **tmp_dict)

        if my_data_idx is not None and my_data_idx > 0:
            logger.info(f"🎯 DummyDataTranslator: keeping only client {my_data_idx} data")

        return datadict
