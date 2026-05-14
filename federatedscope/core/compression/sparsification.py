"""
Top-k sparsification utilities shared by CHOCO-SGD (Koloskova et al. 2019)
and GossipFL (Tang et al. 2022) baselines.

Encoding strategy: keep the same state_dict shape but zero out non-top-k entries.
The "bytes-on-wire" metric is computed analytically (kept_count * (4B index + 4B value))
because the underlying transport (BT chunks) ships dense tensors regardless.
"""

import logging
import re
import torch

logger = logging.getLogger(__name__)


_BN_RE = re.compile(r'(running_mean|running_var|num_batches_tracked|\.bn[0-9]*\.|batch_norm)', re.IGNORECASE)


def _is_bn_key(name: str) -> bool:
    """Mirror of Cadence's BN filter."""
    return bool(_BN_RE.search(name))


def topk_sparsify_tensor(t: torch.Tensor, k_ratio: float) -> torch.Tensor:
    """Return tensor with all but top-(k_ratio*numel) entries zeroed out, by magnitude."""
    if not t.is_floating_point() or t.dim() == 0:
        return t.clone()
    n = t.numel()
    k = max(1, int(round(n * k_ratio)))
    if k >= n:
        return t.clone()
    flat = t.detach().reshape(-1)
    _, idx = torch.topk(flat.abs(), k)
    out = torch.zeros_like(flat)
    out[idx] = flat[idx]
    return out.reshape(t.shape)


def _topk_blockwise_tensor(t: torch.Tensor, k_ratio: float, block_size: int) -> torch.Tensor:
    """Per-block top-k: split flat tensor into chunks of block_size, sparsify each."""
    if block_size <= 0 or block_size >= t.numel():
        return topk_sparsify_tensor(t, k_ratio)
    flat = t.detach().reshape(-1).clone()
    out = torch.zeros_like(flat)
    n = flat.numel()
    for start in range(0, n, block_size):
        end = min(start + block_size, n)
        block = flat[start:end]
        k = max(1, int(round(block.numel() * k_ratio)))
        if k >= block.numel():
            out[start:end] = block
            continue
        _, idx = torch.topk(block.abs(), k)
        out[start:end].scatter_(0, idx, block.gather(0, idx))
    return out.reshape(t.shape)


def topk_sparsify_state_dict(state_dict, k_ratio: float, skip_bn: bool = True, block_size: int = 0):
    """Top-k sparsify every floating-point tensor in a state_dict.

    Args:
        state_dict: dict[str, Tensor]
        k_ratio: keep ratio in (0, 1] (e.g., 0.01 = top 1%)
        skip_bn: if True, BN running stats / batch counters are passed through unchanged.
        block_size: 0 = per-tensor top-k (CHOCO-SGD style); >0 = per-block top-k inside each
            tensor (GossipFL S-BAG style). The same k_ratio is applied within each block.

    Returns:
        sparse_state_dict, info dict with 'bytes_on_wire', 'kept_count', 'orig_count',
        'compression_ratio'.
    """
    if not (0 < k_ratio <= 1):
        raise ValueError(f"k_ratio must be in (0, 1], got {k_ratio}")
    sparse = {}
    kept = 0
    orig = 0
    for name, t in state_dict.items():
        if skip_bn and _is_bn_key(name):
            sparse[name] = t.clone() if hasattr(t, 'clone') else t
            continue
        if not isinstance(t, torch.Tensor) or not t.is_floating_point() or t.dim() == 0:
            sparse[name] = t.clone() if hasattr(t, 'clone') else t
            continue
        n = t.numel()
        if block_size > 0:
            sparse[name] = _topk_blockwise_tensor(t, k_ratio, block_size)
            n_blocks = (n + block_size - 1) // block_size
            kept += sum(max(1, int(round(min(block_size, n - i*block_size) * k_ratio)))
                        for i in range(n_blocks))
            orig += n
        else:
            k = max(1, int(round(n * k_ratio)))
            if k >= n:
                sparse[name] = t.clone()
                kept += n
                orig += n
                continue
            sparse[name] = topk_sparsify_tensor(t, k_ratio)
            kept += k
            orig += n
    bytes_on_wire = kept * 8                 # 4B int32 index + 4B float32 value
    return sparse, {
        'bytes_on_wire': bytes_on_wire,
        'kept_count': kept,
        'orig_count': orig,
        'compression_ratio': (kept / orig) if orig > 0 else 0.0,
        'block_size': block_size,
    }


def state_dict_diff(a, b):
    """Per-key tensor diff a - b, only for floating-point tensors with matching shape."""
    out = {}
    for k, va in a.items():
        if k not in b:
            continue
        vb = b[k]
        if isinstance(va, torch.Tensor) and isinstance(vb, torch.Tensor) and va.shape == vb.shape and va.is_floating_point():
            out[k] = (va.detach() - vb.detach())
        else:
            out[k] = None
    return out


def state_dict_add(target, delta, alpha: float = 1.0):
    """In-place: target += alpha * delta, for matching float tensors."""
    for k, dv in delta.items():
        if dv is None:
            continue
        if k not in target:
            continue
        tv = target[k]
        if isinstance(tv, torch.Tensor) and isinstance(dv, torch.Tensor) and tv.shape == dv.shape and tv.is_floating_point():
            target[k] = tv + alpha * dv.to(tv.dtype).to(tv.device)
    return target


def state_dict_zeros_like(state_dict):
    """Build a state_dict-shaped object with zeros for every float tensor; non-floats are cloned."""
    out = {}
    for k, v in state_dict.items():
        if isinstance(v, torch.Tensor) and v.is_floating_point():
            out[k] = torch.zeros_like(v)
        elif hasattr(v, 'clone'):
            out[k] = v.clone()
        else:
            out[k] = v
    return out
