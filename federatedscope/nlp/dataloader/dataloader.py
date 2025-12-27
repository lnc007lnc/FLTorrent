from federatedscope.nlp.dataset.leaf_nlp import LEAF_NLP
from federatedscope.nlp.dataset.leaf_twitter import LEAF_TWITTER
from federatedscope.nlp.dataset.leaf_synthetic import LEAF_SYNTHETIC
from federatedscope.core.auxiliaries.transform_builder import get_transform
from federatedscope.core.data.utils import (get_num_users_to_load,
                                            should_load_all_leaf_users)


def load_nlp_dataset(config=None):
    """
    Return the dataset of ``shakespeare``, ``subreddit``, ``twitter``, \
    or ``synthetic``.

    Args:
        config: configurations for FL, see ``federatedscope.core.configs``

    Returns:
        FL dataset dict, with ``client_id`` as key.

    Note:
      ``load_nlp_dataset()`` will return a dict as shown below:
        ```
        {'client_id': {'train': dataset, 'test': dataset, 'val': dataset}}
        ```
    """
    splits = config.data.splits

    path = config.data.root
    name = config.data.type.lower()
    transforms_funcs, _, _ = get_transform(config, 'torchtext')

    if name in ['shakespeare', 'subreddit']:
        dataset = LEAF_NLP(root=path,
                           name=name,
                           s_frac=config.data.subsample,
                           tr_frac=splits[0],
                           val_frac=splits[1],
                           seed=config.seed,
                           **transforms_funcs)
    elif name == 'twitter':
        dataset = LEAF_TWITTER(root=path,
                               name='twitter',
                               s_frac=config.data.subsample,
                               tr_frac=splits[0],
                               val_frac=splits[1],
                               seed=config.seed,
                               **transforms_funcs)
    elif name == 'synthetic':
        dataset = LEAF_SYNTHETIC(root=path)
    else:
        raise ValueError(f'No dataset named: {name}!')

    # Determine how many users to load (use utility function)
    num_users_to_load = get_num_users_to_load(dataset, config)

    # Update client_num for original mode only
    if not should_load_all_leaf_users(config):
        config.merge_from_list(['federate.client_num', num_users_to_load])

    # get local dataset
    data_dict = dict()
    for user_idx in range(1, num_users_to_load + 1):
        data_dict[user_idx] = dataset[user_idx - 1]

    return data_dict, config
