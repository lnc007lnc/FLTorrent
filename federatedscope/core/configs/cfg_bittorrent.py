"""
BitTorrent配置模块
定义BitTorrent chunk交换系统的配置参数
"""

import logging
from federatedscope.core.configs.config import CN
from federatedscope.register import register_config

logger = logging.getLogger(__name__)


def extend_bittorrent_cfg(cfg):
    """
    扩展配置系统，添加BitTorrent相关配置
    
    Args:
        cfg: 全局配置对象
    """
    # 🔧 BitTorrent配置section
    cfg.bittorrent = CN()
    
    # 基础开关配置
    cfg.bittorrent.enable = False  # 是否启用BitTorrent chunk交换
    
    # 超时和重试配置
    cfg.bittorrent.timeout = 60.0  # BitTorrent交换的超时时间(秒)
    cfg.bittorrent.max_retries = 3  # chunk请求的最大重试次数
    cfg.bittorrent.request_timeout = 5.0  # 单个chunk请求的超时时间(秒)
    
    # 协议参数配置
    cfg.bittorrent.max_upload_slots = 4  # 最大上传槽位数
    cfg.bittorrent.optimistic_unchoke_interval = 30.0  # optimistic unchoke间隔时间(秒)
    cfg.bittorrent.regular_unchoke_interval = 10.0  # 常规unchoke间隔时间(秒)
    cfg.bittorrent.end_game_threshold = 10  # 进入End Game模式的剩余chunk阈值
    
    # 完成策略配置
    cfg.bittorrent.min_completion_ratio = 0.8  # 允许继续的最小完成比例
    cfg.bittorrent.require_full_completion = True  # 是否要求所有客户端完全完成
    
    # 算法选择配置
    cfg.bittorrent.chunk_selection = 'rarest_first'  # chunk选择算法: 'rarest_first', 'random', 'sequential'
    
    # 优化配置
    cfg.bittorrent.enable_have_suppression = True  # 是否启用have消息抑制
    cfg.bittorrent.batch_have_interval = 1.0  # 批量have消息发送间隔(秒)
    
    # 存储配置
    cfg.bittorrent.keep_remote_chunks = 5  # 保留远程chunks的轮数
    cfg.bittorrent.cleanup_policy = 'separate'  # 清理策略: 'separate', 'unified'
    
    # 日志和调试配置
    cfg.bittorrent.verbose = False  # 是否启用详细日志
    cfg.bittorrent.save_statistics = True  # 是否保存交换统计信息
    cfg.bittorrent.log_dir = 'bittorrent_logs'  # BitTorrent日志保存目录



def assert_bittorrent_cfg(cfg):
    """验证BitTorrent配置参数的有效性"""
    if not cfg.bittorrent.enable:
        return  # 如果禁用BitTorrent，跳过验证
    
    # 验证超时参数
    assert cfg.bittorrent.timeout > 0, \
        f"cfg.bittorrent.timeout must be positive, but got {cfg.bittorrent.timeout}"
    
    assert cfg.bittorrent.request_timeout > 0, \
        f"cfg.bittorrent.request_timeout must be positive, but got {cfg.bittorrent.request_timeout}"
    
    # 验证重试参数
    assert cfg.bittorrent.max_retries >= 0, \
        f"cfg.bittorrent.max_retries must be non-negative, but got {cfg.bittorrent.max_retries}"
    
    # 验证协议参数
    assert cfg.bittorrent.max_upload_slots > 0, \
        f"cfg.bittorrent.max_upload_slots must be positive, but got {cfg.bittorrent.max_upload_slots}"
    
    assert 0.0 < cfg.bittorrent.min_completion_ratio <= 1.0, \
        f"cfg.bittorrent.min_completion_ratio must be in (0.0, 1.0], but got {cfg.bittorrent.min_completion_ratio}"
    
    # 验证算法选择
    valid_algorithms = ['rarest_first', 'random', 'sequential']
    assert cfg.bittorrent.chunk_selection in valid_algorithms, \
        f"cfg.bittorrent.chunk_selection must be one of {valid_algorithms}, but got {cfg.bittorrent.chunk_selection}"
    
    # 验证清理策略
    valid_policies = ['separate', 'unified']
    assert cfg.bittorrent.cleanup_policy in valid_policies, \
        f"cfg.bittorrent.cleanup_policy must be one of {valid_policies}, but got {cfg.bittorrent.cleanup_policy}"
    
    # 记录配置信息
    if cfg.bittorrent.enable:
        logger.info(f"BitTorrent chunk exchange enabled with {cfg.bittorrent.chunk_selection} algorithm")
        if cfg.bittorrent.verbose:
            logger.info(f"BitTorrent configuration: timeout={cfg.bittorrent.timeout}s, "
                       f"upload_slots={cfg.bittorrent.max_upload_slots}, "
                       f"completion_ratio={cfg.bittorrent.min_completion_ratio}")


register_config("bittorrent", extend_bittorrent_cfg)