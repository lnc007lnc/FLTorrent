import os
import random
import json
import torch
import math
import logging

import numpy as np
import os.path as osp

from PIL import Image
from tqdm import tqdm

from sklearn.model_selection import train_test_split
from torch.utils.data import Dataset

from federatedscope.core.data.utils import save_local_data, download_url
from federatedscope.cv.dataset.leaf import LEAF

logger = logging.getLogger(__name__)

IMAGE_SIZE = {'femnist': (28, 28), 'celeba': (84, 84, 3)}
MODE = {'femnist': 'L', 'celeba': 'RGB'}


class LazyLoadDataset(Dataset):
    """
    延迟加载数据集，用于解决分布式 FL 中内存峰值问题。

    两阶段加载策略：
    1. 初始阶段（lazy=True）：只存储标签和索引，用于 LDA 分割（~1MB）
    2. 分割后（lazy=False）：预加载该 client 的所有数据到内存（只有 1/N 数据）

    这样既避免了分割阶段的内存峰值，又保证了训练阶段的速度。
    """
    def __init__(self, task_indices, processed_dir, name, transform=None, target_transform=None, lazy=True):
        """
        Args:
            task_indices: List of (task_id, local_idx, label, split_type) 元组
            processed_dir: processed 数据目录路径
            name: 数据集名称 ('femnist' or 'celeba')
            transform: 图像变换
            target_transform: 标签变换
            lazy: 是否延迟加载。True=只存索引，False=立即预加载数据
        """
        self.task_indices = task_indices  # [(task_id, local_idx, label, split_type), ...]
        self.processed_dir = processed_dir
        self.name = name
        self.transform = transform
        self.target_transform = target_transform

        # 缓存已加载的 task 数据
        self._task_cache = {}

        # 如果不是延迟模式，立即预加载所有数据
        if not lazy and task_indices:
            self._preload()

    def __len__(self):
        return len(self.task_indices)

    def __getitem__(self, idx):
        task_id, local_idx, label, split_type = self.task_indices[idx]

        # 延迟加载：只在需要时才加载该 task 的数据
        cache_key = (task_id, split_type)
        if cache_key not in self._task_cache:
            task_path = osp.join(self.processed_dir, f"task_{task_id}", f"{split_type}.pt")
            data, targets = torch.load(task_path, weights_only=False)
            self._task_cache[cache_key] = (data, targets)

        data, targets = self._task_cache[cache_key]
        img_data = data[local_idx]
        target = targets[local_idx].item() if hasattr(targets[local_idx], 'item') else label

        # 转换为图像
        img = np.resize(img_data.numpy().astype(np.uint8), IMAGE_SIZE[self.name])
        img = Image.fromarray(img, mode=MODE[self.name])

        if self.transform is not None:
            img = self.transform(img)
        if self.target_transform is not None:
            target = self.target_transform(target)

        return img, target

    def get_labels(self):
        """返回所有标签，用于 LDA 分割"""
        return [item[2] for item in self.task_indices]

    def subset(self, indices):
        """
        创建子集，包含指定索引的数据。
        子集创建时会立即预加载数据到内存（因为此时已完成分割，数据量是 1/N）。
        """
        new_task_indices = [self.task_indices[i] for i in indices]
        # 分割后的子集使用 lazy=False，立即预加载数据
        return LazyLoadDataset(
            new_task_indices,
            self.processed_dir,
            self.name,
            self.transform,
            self.target_transform,
            lazy=False  # 分割完成后立即加载到内存
        )

    def _preload(self):
        """预加载该数据集的所有数据到内存"""
        # 收集需要加载的 task 文件
        tasks_to_load = set()
        for task_id, local_idx, label, split_type in self.task_indices:
            tasks_to_load.add((task_id, split_type))

        logger.info(f"📥 Preloading {len(tasks_to_load)} task files ({len(self.task_indices)} samples)...")

        # 一次性加载所有需要的 task 数据
        for task_id, split_type in tasks_to_load:
            cache_key = (task_id, split_type)
            if cache_key not in self._task_cache:
                task_path = osp.join(self.processed_dir, f"task_{task_id}", f"{split_type}.pt")
                data, targets = torch.load(task_path, weights_only=False)
                self._task_cache[cache_key] = (data, targets)

        logger.info(f"✅ Preload complete. Ready for fast training.")

    def clear_cache(self):
        """清除缓存以释放内存"""
        self._task_cache.clear()


class LEAF_CV(LEAF):
    """
    LEAF CV dataset from "LEAF: A Benchmark for Federated Settings"

    leaf.cmu.edu

    Arguments:
        root (str): root path.
        name (str): name of dataset, ‘femnist’ or ‘celeba’.
        s_frac (float): fraction of the dataset to be used; default=0.3.
        tr_frac (float): train set proportion for each task; default=0.8.
        val_frac (float): valid set proportion for each task; default=0.0.
        train_tasks_frac (float): fraction of test tasks; default=1.0.
        transform: transform for x.
        target_transform: transform for y.

    """
    def __init__(self,
                 root,
                 name,
                 s_frac=0.3,
                 tr_frac=0.8,
                 val_frac=0.0,
                 train_tasks_frac=1.0,
                 seed=123,
                 transform=None,
                 target_transform=None):
        self.s_frac = s_frac
        self.tr_frac = tr_frac
        self.val_frac = val_frac
        self.seed = seed
        self.train_tasks_frac = train_tasks_frac
        super(LEAF_CV, self).__init__(root, name, transform, target_transform)
        files = os.listdir(self.processed_dir)
        files = [f for f in files if f.startswith('task_')]
        if len(files):
            # Sort by idx
            files.sort(key=lambda k: int(k[5:]))

            for file in files:
                train_data, train_targets = torch.load(
                    osp.join(self.processed_dir, file, 'train.pt'))
                test_data, test_targets = torch.load(
                    osp.join(self.processed_dir, file, 'test.pt'))
                self.data_dict[int(file[5:])] = {
                    'train': (train_data, train_targets),
                    'test': (test_data, test_targets)
                }
                if osp.exists(osp.join(self.processed_dir, file, 'val.pt')):
                    val_data, val_targets = torch.load(
                        osp.join(self.processed_dir, file, 'val.pt'))
                    self.data_dict[int(file[5:])]['val'] = (val_data,
                                                            val_targets)
        else:
            raise RuntimeError(
                'Please delete ‘processed’ folder and try again!')

    @property
    def raw_file_names(self):
        names = [f'{self.name}_all_data.zip']
        return names

    def download(self):
        # Download to `self.raw_dir`.
        url = 'https://federatedscope.oss-cn-beijing.aliyuncs.com'
        os.makedirs(self.raw_dir, exist_ok=True)
        for name in self.raw_file_names:
            download_url(f'{url}/{name}', self.raw_dir)

    def __getitem__(self, index):
        """
        Arguments:
            index (int): Index

        :returns:
            dict: {'train':[(image, target)],
                   'test':[(image, target)],
                   'val':[(image, target)]}
            where target is the target class.
        """
        img_dict = {}
        data = self.data_dict[index]
        for key in data:
            img_dict[key] = []
            imgs, targets = data[key]
            for idx in range(targets.shape[0]):
                img = np.resize(imgs[idx].numpy().astype(np.uint8),
                                IMAGE_SIZE[self.name])
                img = Image.fromarray(img, mode=MODE[self.name])
                if self.transform is not None:
                    img = self.transform(img)

                if self.target_transform is not None:
                    targets[idx] = self.target_transform(targets[idx])

                img_dict[key].append((img, targets[idx]))

        return img_dict

    def process(self):
        raw_path = osp.join(self.raw_dir, "all_data")
        files = os.listdir(raw_path)
        files = [f for f in files if f.endswith('.json')]

        n_tasks = math.ceil(len(files) * self.s_frac)
        random.shuffle(files)
        files = files[:n_tasks]

        print("Preprocess data (Please leave enough space)...")

        idx = 0
        for num, file in enumerate(tqdm(files)):

            with open(osp.join(raw_path, file), 'r') as f:
                raw_data = json.load(f)

            # Numpy to Tensor
            for writer, v in raw_data['user_data'].items():
                data, targets = v['x'], v['y']

                if len(v['x']) > 2:
                    data = torch.tensor(np.stack(data))
                    targets = torch.LongTensor(np.stack(targets))
                else:
                    data = torch.tensor(data)
                    targets = torch.LongTensor(targets)

                train_data, test_data, train_targets, test_targets =\
                    train_test_split(
                        data,
                        targets,
                        train_size=self.tr_frac,
                        random_state=self.seed
                    )

                if self.val_frac > 0:
                    val_data, test_data, val_targets, test_targets = \
                        train_test_split(
                            test_data,
                            test_targets,
                            train_size=self.val_frac / (1.-self.tr_frac),
                            random_state=self.seed
                        )

                else:
                    val_data, val_targets = None, None
                save_path = osp.join(self.processed_dir, f"task_{idx}")
                os.makedirs(save_path, exist_ok=True)

                save_local_data(dir_path=save_path,
                                train_data=train_data,
                                train_targets=train_targets,
                                test_data=test_data,
                                test_targets=test_targets,
                                val_data=val_data,
                                val_targets=val_targets)
                idx += 1


def load_leaf_cv_lazy(root, name, transform=None, target_transform=None):
    """
    创建延迟加载的 LEAF CV 数据集。

    只加载标签信息到内存，图像数据在实际访问时才加载。
    内存使用：从 ~9GB (全部图像) 降到 ~1MB (只有标签和索引)

    Args:
        root: 数据根目录
        name: 数据集名称 ('femnist' or 'celeba')
        transform: 图像变换
        target_transform: 标签变换

    Returns:
        dict: {'train': LazyLoadDataset, 'val': LazyLoadDataset, 'test': LazyLoadDataset}
    """
    processed_dir = osp.join(root, name, 'processed')

    if not osp.exists(processed_dir):
        raise RuntimeError(f"Processed directory not found: {processed_dir}. "
                           "Please run data preprocessing first.")

    files = os.listdir(processed_dir)
    files = [f for f in files if f.startswith('task_')]
    files.sort(key=lambda k: int(k[5:]))

    logger.info(f"📂 Loading labels from {len(files)} tasks (lazy mode)...")

    train_indices = []
    val_indices = []
    test_indices = []

    # 只加载标签，不加载图像数据
    # 使用 mmap 模式避免将整个文件加载到内存
    for file in tqdm(files, desc="Loading labels only"):
        task_id = int(file[5:])
        task_path = osp.join(processed_dir, file)

        # 加载 train 标签（使用 mmap 避免加载图像数据到内存）
        train_path = osp.join(task_path, 'train.pt')
        if osp.exists(train_path):
            loaded = torch.load(train_path, weights_only=False, mmap=True)
            train_targets = loaded[1]  # 只取 targets
            for local_idx, label in enumerate(train_targets.tolist()):
                train_indices.append((task_id, local_idx, label, 'train'))
            del loaded  # 显式释放

        # 加载 val 标签
        val_path = osp.join(task_path, 'val.pt')
        if osp.exists(val_path):
            loaded = torch.load(val_path, weights_only=False, mmap=True)
            val_targets = loaded[1]
            for local_idx, label in enumerate(val_targets.tolist()):
                val_indices.append((task_id, local_idx, label, 'val'))
            del loaded

        # 加载 test 标签
        test_path = osp.join(task_path, 'test.pt')
        if osp.exists(test_path):
            loaded = torch.load(test_path, weights_only=False, mmap=True)
            test_targets = loaded[1]
            for local_idx, label in enumerate(test_targets.tolist()):
                test_indices.append((task_id, local_idx, label, 'test'))
            del loaded

    logger.info(f"✅ Labels loaded - Train: {len(train_indices)}, "
                f"Val: {len(val_indices)}, Test: {len(test_indices)}")

    # 创建延迟加载数据集
    result = {
        'train': LazyLoadDataset(train_indices, processed_dir, name, transform, target_transform),
        'val': LazyLoadDataset(val_indices, processed_dir, name, transform, target_transform),
        'test': LazyLoadDataset(test_indices, processed_dir, name, transform, target_transform),
    }

    return result
