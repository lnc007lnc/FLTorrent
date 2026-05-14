import logging
from federatedscope.cv.dataset.leaf_cv import LEAF_CV, load_leaf_cv_lazy, LazyLoadDataset
from federatedscope.core.auxiliaries.transform_builder import get_transform
from federatedscope.core.data.utils import (get_num_users_to_load,
                                            should_load_all_leaf_users)

logger = logging.getLogger(__name__)


def load_cv_dataset(config=None):
    """
    Return the dataset of ``femnist`` or ``celeba``.

    Args:
        config: configurations for FL, see ``federatedscope.core.configs``

    Returns:
        FL dataset dict, with ``client_id`` as key.

    Note:
      ``load_cv_dataset()`` will return a dict as shown below:
        ```
        {'client_id': {'train': dataset, 'test': dataset, 'val': dataset}}
        ```
    """
    splits = config.data.splits

    path = config.data.root
    name = config.data.type.lower()
    transforms_funcs, val_transforms_funcs, test_transforms_funcs = \
        get_transform(config, 'torchvision')

    # 检查是否使用延迟加载模式 (merge_leaf_before_split=True 时自动启用)
    use_lazy_load = should_load_all_leaf_users(config)

    if name in ['femnist', 'celeba']:
        if use_lazy_load:
            # 延迟加载模式：只加载标签，不加载图像数据
            # 内存使用：~1MB vs ~9GB
            logger.info("🚀 Using lazy-load mode for LEAF dataset (memory efficient)")
            lazy_data = load_leaf_cv_lazy(
                root=path,
                name=name,
                transform=transforms_funcs.get('transform'),
                target_transform=transforms_funcs.get('target_transform')
            )
            # 返回合并后的数据集，用于 MergedLeafTranslator
            # 这里返回一个特殊格式，表示已经是合并后的延迟加载数据
            return {'__lazy_merged__': lazy_data}, config
        else:
            # 原始模式：加载全部数据
            dataset = LEAF_CV(root=path,
                              name=name,
                              s_frac=config.data.subsample,
                              tr_frac=splits[0],
                              val_frac=splits[1],
                              seed=1234,
                              **transforms_funcs)
    else:
        raise ValueError(f'No dataset named: {name}!')

    # 原始模式的处理逻辑
    # Determine how many users to load (use utility function)
    num_users_to_load = get_num_users_to_load(dataset, config)

    # Update client_num for original mode only
    if not should_load_all_leaf_users(config):
        config.merge_from_list(['federate.client_num', num_users_to_load])

    # Convert list to dict
    data_dict = dict()
    for user_idx in range(1, num_users_to_load + 1):
        data_dict[user_idx] = dataset[user_idx - 1]

    return data_dict, config
