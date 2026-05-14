"""CINIC-10 Dataset support for FederatedScope.

CINIC-10 is a drop-in replacement for CIFAR-10 with 270,000 images (90,000 per split).
It combines CIFAR-10 images with downsampled ImageNet images from matching classes.

Data Processing Pipeline (similar to CelebA):
1. Merge train + valid + test data (270,000 images total)
2. Use LDA/IID splitter to distribute data to clients based on alpha
3. Each client internally splits data into train/val/test based on DATA_SPLITS

Dataset structure:
    cinic-10/
        train/
            airplane/, automobile/, bird/, cat/, deer/, dog/, frog/, horse/, ship/, truck/
        valid/
            ...
        test/
            ...

Download:
    wget https://datashare.ed.ac.uk/bitstream/handle/10283/3192/CINIC-10.tar.gz
    tar -xzf CINIC-10.tar.gz
"""
import os
import logging
import numpy as np
import torch
from PIL import Image
from torch.utils.data import ConcatDataset, Subset
from torchvision.datasets import ImageFolder
from torchvision import transforms
from federatedscope.register import register_data
from federatedscope.core.data import ClientData, StandaloneDataDict
from federatedscope.core.data.utils import convert_data_mode
from federatedscope.core.auxiliaries.utils import setup_seed
from federatedscope.core.splitters.utils import dirichlet_distribution_noniid_slice

logger = logging.getLogger(__name__)

# Fallback ToTensor to ensure output is always a tensor (prevents PIL.Image reaching collate_fn)
_FALLBACK_TO_TENSOR = transforms.ToTensor()

# CINIC-10 mean and std (computed from the dataset)
CINIC_MEAN = [0.47889522, 0.47227842, 0.43047404]
CINIC_STD = [0.24205776, 0.23828046, 0.25874835]


def get_cinic10_transforms(train=True):
    """Get standard transforms for CINIC-10.

    Note: ToTensor is NOT included here because TransformSubset.__getitem__
    has a fallback that converts PIL to tensor first. This prevents double
    ToTensor calls and ensures robustness against any code path that might
    bypass the transform pipeline.
    """
    if train:
        return transforms.Compose([
            transforms.RandomCrop(32, padding=4),
            transforms.RandomHorizontalFlip(),
            # ToTensor is applied in TransformSubset.__getitem__ as fallback
            transforms.Normalize(CINIC_MEAN, CINIC_STD),
        ])
    else:
        return transforms.Compose([
            # ToTensor is applied in TransformSubset.__getitem__ as fallback
            transforms.Normalize(CINIC_MEAN, CINIC_STD),
        ])


def load_cinic10_data(config, client_cfgs=None):
    """Load CINIC-10 dataset for FederatedScope.

    Processing Pipeline (similar to CelebA's MergedLeafTranslator):
    1. Merge train + valid + test data
    2. Use LDA splitter to distribute data to clients based on alpha
    3. Each client internally splits data into train/val/test based on DATA_SPLITS

    Args:
        config: FederatedScope config
        client_cfgs: Client-specific configs (optional)

    Returns:
        (StandaloneDataDict, modified_config) or None if not cinic10
    """
    if config.data.type.lower() != 'cinic10':
        return None

    # Fix seed for reproducibility
    setup_seed(12345)

    root = config.data.root
    train_dir = os.path.join(root, 'train')
    valid_dir = os.path.join(root, 'valid')
    test_dir = os.path.join(root, 'test')

    # Check if directories exist
    for d, name in [(train_dir, 'train'), (valid_dir, 'valid'), (test_dir, 'test')]:
        if not os.path.exists(d):
            raise FileNotFoundError(
                f"CINIC-10 {name} directory not found at {d}. "
                f"Please download CINIC-10:\n"
                f"  wget https://datashare.ed.ac.uk/bitstream/handle/10283/3192/CINIC-10.tar.gz\n"
                f"  tar -xzf CINIC-10.tar.gz -C {root}"
            )

    # CRITICAL: Apply ToTensor at ImageFolder level to ensure ALL code paths output Tensor
    # This prevents PIL.Image from reaching collate_fn even if TransformSubset is bypassed
    # (e.g., by merge_data, convert_data_mode, or other FederatedScope internal logic)
    base_transform = transforms.ToTensor()
    dataset_train = ImageFolder(train_dir, transform=base_transform)
    dataset_valid = ImageFolder(valid_dir, transform=base_transform)
    dataset_test = ImageFolder(test_dir, transform=base_transform)

    logger.info(f"CINIC-10 raw data: train={len(dataset_train)}, "
                f"valid={len(dataset_valid)}, test={len(dataset_test)}")

    # === Step 1: Merge all data ===
    merged_dataset = ConcatDataset([dataset_train, dataset_valid, dataset_test])
    total_size = len(merged_dataset)
    logger.info(f"CINIC-10 merged total: {total_size} images")

    # Extract all labels for LDA splitting
    all_labels = []
    for ds in [dataset_train, dataset_valid, dataset_test]:
        all_labels.extend([sample[1] for sample in ds.samples])
    all_labels = np.array(all_labels)

    # === Step 2: Split to clients using LDA/IID ===
    client_num = config.federate.client_num
    splitter_type = config.data.splitter

    if splitter_type == 'lda':
        # Get alpha parameter
        alpha = 0.5  # default
        if config.data.splitter_args:
            for arg in config.data.splitter_args:
                if 'alpha' in arg:
                    alpha = arg['alpha']

        logger.info(f"Using LDA splitter with alpha={alpha} for {client_num} clients")
        client_idx_slices = dirichlet_distribution_noniid_slice(
            all_labels, client_num, alpha)
    else:
        # IID splitting
        logger.info(f"Using IID splitter for {client_num} clients")
        indices = np.random.permutation(total_size)
        split_size = total_size // client_num
        client_idx_slices = [indices[i*split_size:(i+1)*split_size].tolist()
                            for i in range(client_num)]

    # === Step 3: Create train/val/test splits for each client ===
    splits = config.data.splits  # e.g., [0.8, 0.1, 0.1]

    # Get current process's client ID (optimization for distributed mode)
    my_data_idx = getattr(config.distribute, 'data_idx', None)

    data_dict = {}

    # Create transform wrappers for training and evaluation
    train_transform = get_cinic10_transforms(train=True)
    test_transform = get_cinic10_transforms(train=False)

    # === First pass: compute train/val/test indices for ALL clients ===
    # This ensures consistent splits and allows collecting global test set
    all_client_splits = {}
    global_test_indices = []

    for client_id in range(1, client_num + 1):
        idx = client_id - 1
        # Convert to numpy array for consistent handling (LDA returns ndarray, IID returns list)
        client_indices = np.array(client_idx_slices[idx])

        if len(client_indices) == 0:
            logger.warning(f"Client {client_id} has no data!")
            all_client_splits[client_id] = ([], [], [])
            continue

        # Split client's data into train/val/test
        np.random.shuffle(client_indices)
        n = len(client_indices)
        train_end = int(n * splits[0])
        val_end = int(n * (splits[0] + splits[1]))

        train_indices = client_indices[:train_end].tolist()
        val_indices = client_indices[train_end:val_end].tolist()
        test_indices = client_indices[val_end:].tolist()

        all_client_splits[client_id] = (train_indices, val_indices, test_indices)
        global_test_indices.extend(test_indices)

    # Create global test dataset by merging all clients' test portions
    # This avoids data leakage: test data is NOT in any client's train set
    global_test_dataset = TransformSubset(merged_dataset, global_test_indices, test_transform)
    logger.info(f"Global test set: {len(global_test_indices)} samples "
               f"(merged from {client_num} clients' test portions, no overlap with train)")

    # === Server data (key=0) ===
    if my_data_idx is None or my_data_idx == 0:
        # Standalone or Server: use global test set for evaluation
        data_dict[0] = ClientData(config,
                                  train=None,
                                  val=global_test_dataset,
                                  test=global_test_dataset)
    else:
        # Client process: only need test set for evaluation
        data_dict[0] = ClientData(config,
                                  train=None,
                                  val=None,
                                  test=global_test_dataset)

    # === Second pass: create ClientData for each client ===
    for client_id in range(1, client_num + 1):
        # Distributed mode: only create data for this process's client
        if my_data_idx is not None and my_data_idx > 0:
            if client_id != my_data_idx:
                continue

        train_indices, val_indices, test_indices = all_client_splits[client_id]

        if len(train_indices) == 0:
            continue

        # Create subsets with appropriate transforms
        client_train = TransformSubset(merged_dataset, train_indices, train_transform)
        client_val = TransformSubset(merged_dataset, val_indices, test_transform)
        # Each client uses the global test set (merged from all clients' test portions)
        client_test = global_test_dataset

        if client_cfgs is not None:
            client_cfg = config.clone()
            client_cfg.merge_from_other_cfg(client_cfgs.get(f'client_{client_id}'))
        else:
            client_cfg = config

        data_dict[client_id] = ClientData(client_cfg,
                                          train=client_train,
                                          val=client_val,
                                          test=client_test)

        logger.info(f"Client {client_id}: train={len(train_indices)}, "
                   f"val={len(val_indices)}, test=global({len(global_test_indices)})")

    if my_data_idx is not None and my_data_idx > 0:
        logger.info(f"CINIC-10: Created data for client {my_data_idx} only")

    logger.info(f"CINIC-10 distributed to {client_num} clients using {splitter_type} splitter")
    logger.info(f"   Total: {total_size}, Per client avg: {total_size // client_num}")

    # Wrap into StandaloneDataDict
    datadict = StandaloneDataDict(data_dict, config)

    # Convert to appropriate mode for distributed FL
    datadict = convert_data_mode(datadict, config)

    # Restore user seed
    setup_seed(config.seed)

    return datadict, config


class TransformSubset(Subset):
    """Subset with custom transform applied during __getitem__.

    Input is already Tensor (from ImageFolder with ToTensor transform).
    This class applies additional augmentation and normalization.
    """

    def __init__(self, dataset, indices, transform=None):
        super().__init__(dataset, indices)
        self.transform = transform

    def __getitem__(self, idx):
        # Get data from parent (already Tensor from ImageFolder with ToTensor)
        x, y = super().__getitem__(idx)

        # Safety fallback: convert PIL to Tensor if somehow bypassed
        # This should never trigger since ImageFolder now uses ToTensor
        if isinstance(x, Image.Image):
            x = _FALLBACK_TO_TENSOR(x)

        # Apply additional transforms (augmentation + normalization)
        if self.transform is not None:
            x = self.transform(x)

        return x, y


# Register with FederatedScope
register_data('cinic10', load_cinic10_data)
