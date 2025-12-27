"""
🛡️ Application-Layer Reliable Transport (ALRT) Protocol
在gRPC之上实现可靠传输，解决无sudo权限时的静默丢包问题

核心机制：
1. 消息分片（Fragmentation）- 解决大消息超过buffer zone问题
2. 消息序列号 + ACK确认
3. 选择性重传（只重传丢失的分片）
4. 滑动窗口流控
5. 分片重组（Reassembly）

关键问题：
  系统Buffer: rmem_max = wmem_max ≈ 208KB (无sudo无法修改)
  gRPC配置: max_message_length = 300MB
  差距1400倍！大chunk超过socket buffer导致静默丢包

解决方案：
  将大消息拆分成 < FRAGMENT_SIZE (默认128KB) 的小片段
  每个片段独立确认和重传
"""

import threading
import time
import logging
import queue
from dataclasses import dataclass, field
from typing import Dict, Optional, Any, Callable, Set, List, Tuple
from collections import OrderedDict
import hashlib
import pickle
import zlib
import uuid
import struct

logger = logging.getLogger(__name__)

# ============================================================================
# 🔧 核心配置：基于系统buffer限制
# ============================================================================
# 系统限制: rmem_max = wmem_max ≈ 208KB (无sudo无法修改)
# 安全余量: 使用128KB作为分片大小，留出空间给协议头
FRAGMENT_SIZE = 128 * 1024  # 128KB - 安全分片大小
MAX_MESSAGE_SIZE = 300 * 1024 * 1024  # 300MB - gRPC配置的最大消息


@dataclass
class Fragment:
    """消息分片"""
    message_id: str          # 原始消息的唯一ID
    fragment_index: int      # 分片索引 (0-based)
    total_fragments: int     # 总分片数
    data: bytes              # 分片数据
    checksum: str = ""       # 分片校验和

    def compute_checksum(self) -> str:
        """计算分片校验和"""
        return hashlib.md5(self.data).hexdigest()[:8]

    def verify(self) -> bool:
        """验证分片完整性"""
        if not self.checksum:
            return True
        return self.compute_checksum() == self.checksum


class MessageFragmenter:
    """
    🔪 消息分片器
    将大消息拆分成小于buffer zone的片段
    """

    def __init__(self, fragment_size: int = FRAGMENT_SIZE, enable_compression: bool = True):
        self.fragment_size = fragment_size
        self.enable_compression = enable_compression
        self.stats = {
            'messages_fragmented': 0,
            'fragments_created': 0,
            'bytes_before_compression': 0,
            'bytes_after_compression': 0,
        }

    def fragment(self, payload: Any, msg_type: str) -> Tuple[str, List[Fragment]]:
        """
        将payload分片
        返回: (message_id, fragments_list)
        """
        # 序列化
        data = pickle.dumps(payload)
        original_size = len(data)
        self.stats['bytes_before_compression'] += original_size

        # 可选压缩（对大数据有效）
        if self.enable_compression and original_size > 1024:
            compressed = zlib.compress(data, level=6)
            if len(compressed) < original_size * 0.9:  # 压缩率>10%才使用
                data = b'ZLIB' + compressed  # 添加压缩标记

        self.stats['bytes_after_compression'] += len(data)

        # 生成消息ID
        message_id = f"{msg_type}_{uuid.uuid4().hex[:8]}"

        # 分片
        fragments = []
        total_fragments = (len(data) + self.fragment_size - 1) // self.fragment_size

        for i in range(total_fragments):
            start = i * self.fragment_size
            end = min(start + self.fragment_size, len(data))
            fragment_data = data[start:end]

            fragment = Fragment(
                message_id=message_id,
                fragment_index=i,
                total_fragments=total_fragments,
                data=fragment_data,
            )
            fragment.checksum = fragment.compute_checksum()
            fragments.append(fragment)

        self.stats['messages_fragmented'] += 1
        self.stats['fragments_created'] += len(fragments)

        if total_fragments > 1:
            logger.debug(f"[Fragmenter] Split message {message_id}: {original_size} bytes -> "
                        f"{len(data)} bytes (compressed) -> {total_fragments} fragments")

        return message_id, fragments

    def get_compression_ratio(self) -> float:
        """获取压缩率"""
        if self.stats['bytes_before_compression'] == 0:
            return 1.0
        return self.stats['bytes_after_compression'] / self.stats['bytes_before_compression']


# ============================================================================
# 🚀 轻量级分片传输 (Lightweight Fragmented Transport, LFT)
# ============================================================================
# 设计目标：最小化CPU开销，适用于64核模拟50节点的场景
#
# 与完整ALRT的区别：
# - 无压缩：省去zlib的CPU开销
# - CRC32代替MD5：快5倍
# - 无滑动窗口/ACK：依赖gRPC的重试机制
# - 零拷贝设计：直接操作bytes，避免对象创建
# - 无额外线程：所有操作同步完成
# ============================================================================

# 使用CRC32代替MD5（快5倍）
def fast_checksum(data: bytes) -> int:
    """快速校验和：CRC32比MD5快5倍"""
    return zlib.crc32(data) & 0xffffffff


class LightweightFragmenter:
    """
    🚀 轻量级分片器 - 最小CPU开销

    特点：
    - 无压缩
    - CRC32校验（比MD5快5倍）
    - 零拷贝：直接memoryview切片
    - 无对象创建开销
    """

    __slots__ = ('fragment_size', 'msg_counter')

    def __init__(self, fragment_size: int = FRAGMENT_SIZE):
        self.fragment_size = fragment_size
        self.msg_counter = 0

    def fragment_bytes(self, data: bytes) -> List[Tuple[int, int, int, bytes, int]]:
        """
        分片bytes数据

        返回: List[(msg_id, frag_idx, total_frags, data_slice, crc32)]
        使用tuple而非dataclass，减少对象创建开销
        """
        total_size = len(data)
        if total_size <= self.fragment_size:
            # 小消息不需要分片
            return [(self.msg_counter, 0, 1, data, fast_checksum(data))]

        msg_id = self.msg_counter
        self.msg_counter += 1

        # 使用memoryview实现零拷贝切片
        mv = memoryview(data)
        total_frags = (total_size + self.fragment_size - 1) // self.fragment_size

        fragments = []
        for i in range(total_frags):
            start = i * self.fragment_size
            end = min(start + self.fragment_size, total_size)
            frag_data = bytes(mv[start:end])  # 只在这里创建bytes
            fragments.append((msg_id, i, total_frags, frag_data, fast_checksum(frag_data)))

        return fragments


class LightweightReassembler:
    """
    🚀 轻量级重组器 - 最小CPU开销

    特点：
    - 无锁设计（每个peer独立的reassembler）
    - 预分配缓冲区
    - 最小化dict操作
    """

    __slots__ = ('pending', 'timeout')

    def __init__(self, timeout: float = 60.0):
        # msg_id -> [total_frags, received_count, first_time, frags_dict]
        self.pending: Dict[int, List] = {}
        self.timeout = timeout

    def add_fragment(self, msg_id: int, frag_idx: int, total_frags: int,
                     data: bytes, checksum: int) -> Optional[bytes]:
        """
        添加分片，返回重组后的完整数据（如果完成）
        """
        # 验证CRC32
        if fast_checksum(data) != checksum:
            return None

        # 初始化或获取pending entry
        if msg_id not in self.pending:
            self.pending[msg_id] = [total_frags, 0, time.time(), {}]

        entry = self.pending[msg_id]
        frags_dict = entry[3]

        # 避免重复分片
        if frag_idx in frags_dict:
            return None

        frags_dict[frag_idx] = data
        entry[1] += 1  # received_count++

        # 检查是否完整
        if entry[1] == entry[0]:
            # 重组：预分配bytearray避免多次拷贝
            total_size = sum(len(frags_dict[i]) for i in range(total_frags))
            result = bytearray(total_size)
            offset = 0
            for i in range(total_frags):
                frag = frags_dict[i]
                result[offset:offset+len(frag)] = frag
                offset += len(frag)

            del self.pending[msg_id]
            return bytes(result)

        return None

    def cleanup(self) -> int:
        """清理超时消息，返回清理数量"""
        now = time.time()
        expired = [mid for mid, entry in self.pending.items()
                   if now - entry[2] > self.timeout]
        for mid in expired:
            del self.pending[mid]
        return len(expired)


class ZeroCopyTransport:
    """
    🚀 零拷贝传输层 - 直接集成到gRPC发送

    使用方式：
        transport = ZeroCopyTransport(original_send_func)
        transport.send(large_data)  # 自动分片发送

    CPU开销分析（vs 完整ALRT）：
    - 无pickle：数据已经是bytes
    - 无压缩：省去zlib compress
    - CRC32代替MD5：快5倍
    - 无线程：同步操作
    - 无锁：每个连接独立

    预估CPU节省：~70%
    """

    def __init__(self,
                 send_func: Callable[[bytes], bool],
                 fragment_size: int = FRAGMENT_SIZE,
                 max_retries: int = 3):
        self.send_func = send_func
        self.fragmenter = LightweightFragmenter(fragment_size)
        self.max_retries = max_retries

        # 统计（可选，调试用）
        self.stats_enabled = False
        self.bytes_sent = 0
        self.fragments_sent = 0
        self.retries = 0

    def send(self, data: bytes) -> bool:
        """
        发送数据，自动分片
        返回是否全部成功
        """
        fragments = self.fragmenter.fragment_bytes(data)

        for msg_id, frag_idx, total_frags, frag_data, checksum in fragments:
            # 构造分片头（16字节固定开销）
            header = struct.pack('<IIII', msg_id, frag_idx, total_frags, checksum)
            packet = header + frag_data

            # 发送（带重试）
            success = False
            for attempt in range(self.max_retries):
                try:
                    if self.send_func(packet):
                        success = True
                        break
                except Exception:
                    if attempt < self.max_retries - 1:
                        time.sleep(0.1 * (attempt + 1))  # 简单退避
                    self.retries += 1

            if not success:
                return False

            if self.stats_enabled:
                self.bytes_sent += len(packet)
                self.fragments_sent += 1

        return True

    def receive(self, packet: bytes, reassembler: LightweightReassembler) -> Optional[bytes]:
        """
        接收数据，自动重组
        """
        if len(packet) < 16:
            return None

        # 解析头
        msg_id, frag_idx, total_frags, checksum = struct.unpack('<IIII', packet[:16])
        frag_data = packet[16:]

        return reassembler.add_fragment(msg_id, frag_idx, total_frags, frag_data, checksum)


class MessageReassembler:
    """
    🔧 消息重组器
    将分片重新组装成完整消息
    """

    def __init__(self, timeout: float = 60.0):
        self.timeout = timeout
        self.pending: Dict[str, Dict[int, Fragment]] = {}  # message_id -> {index: fragment}
        self.metadata: Dict[str, Tuple[int, float]] = {}   # message_id -> (total_fragments, first_received_time)
        self.lock = threading.Lock()
        self.stats = {
            'messages_reassembled': 0,
            'fragments_received': 0,
            'duplicate_fragments': 0,
            'corrupted_fragments': 0,
            'timeout_messages': 0,
        }

    def add_fragment(self, fragment: Fragment) -> Optional[Any]:
        """
        添加分片，如果消息完整则返回重组后的payload
        """
        # 验证分片完整性
        if not fragment.verify():
            logger.warning(f"[Reassembler] Corrupted fragment: {fragment.message_id}[{fragment.fragment_index}]")
            self.stats['corrupted_fragments'] += 1
            return None

        self.stats['fragments_received'] += 1
        message_id = fragment.message_id

        with self.lock:
            # 初始化消息缓冲
            if message_id not in self.pending:
                self.pending[message_id] = {}
                self.metadata[message_id] = (fragment.total_fragments, time.time())

            # 检查重复分片
            if fragment.fragment_index in self.pending[message_id]:
                self.stats['duplicate_fragments'] += 1
                logger.debug(f"[Reassembler] Duplicate fragment: {message_id}[{fragment.fragment_index}]")

            # 存储分片
            self.pending[message_id][fragment.fragment_index] = fragment

            # 检查是否完整
            total_fragments, _ = self.metadata[message_id]
            if len(self.pending[message_id]) == total_fragments:
                return self._reassemble(message_id)

        return None

    def _reassemble(self, message_id: str) -> Optional[Any]:
        """重组消息"""
        fragments = self.pending.pop(message_id)
        self.metadata.pop(message_id, None)

        # 按顺序拼接数据
        sorted_fragments = sorted(fragments.values(), key=lambda f: f.fragment_index)
        data = b''.join(f.data for f in sorted_fragments)

        # 解压缩（如果需要）
        if data.startswith(b'ZLIB'):
            data = zlib.decompress(data[4:])

        # 反序列化
        try:
            payload = pickle.loads(data)
            self.stats['messages_reassembled'] += 1
            logger.debug(f"[Reassembler] Reassembled message {message_id}: {len(sorted_fragments)} fragments")
            return payload
        except Exception as e:
            logger.error(f"[Reassembler] Failed to deserialize {message_id}: {e}")
            return None

    def cleanup_timeout(self) -> List[str]:
        """清理超时的未完成消息"""
        now = time.time()
        timeout_ids = []

        with self.lock:
            for message_id, (total, first_time) in list(self.metadata.items()):
                if now - first_time > self.timeout:
                    received = len(self.pending.get(message_id, {}))
                    logger.warning(f"[Reassembler] Timeout message {message_id}: {received}/{total} fragments")
                    self.pending.pop(message_id, None)
                    self.metadata.pop(message_id, None)
                    self.stats['timeout_messages'] += 1
                    timeout_ids.append(message_id)

        return timeout_ids

    def get_missing_fragments(self, message_id: str) -> List[int]:
        """获取缺失的分片索引（用于请求重传）"""
        with self.lock:
            if message_id not in self.pending:
                return []

            total, _ = self.metadata.get(message_id, (0, 0))
            received = set(self.pending[message_id].keys())
            return [i for i in range(total) if i not in received]


@dataclass
class ReliableMessage:
    """带可靠性元数据的消息包装"""
    seq_num: int                    # 序列号
    payload: Any                    # 原始消息内容
    msg_type: str                   # 消息类型
    sender_id: int                  # 发送者ID
    receiver_id: int                # 接收者ID
    timestamp: float = field(default_factory=time.time)
    checksum: str = ""              # 校验和（可选，用于完整性验证）
    is_ack: bool = False            # 是否是ACK消息
    ack_seq: int = -1               # ACK的序列号（如果is_ack=True）
    is_retransmit: bool = False     # 是否是重传

    def compute_checksum(self) -> str:
        """计算payload的校验和"""
        if self.payload is None:
            return ""
        try:
            data = pickle.dumps(self.payload)
            return hashlib.md5(data).hexdigest()[:8]
        except:
            return ""


class SlidingWindow:
    """滑动窗口管理器"""

    def __init__(self, window_size: int = 32):
        self.window_size = window_size
        self.base = 0                    # 窗口基址（最小未确认序列号）
        self.next_seq = 0                # 下一个可用序列号
        self.pending: OrderedDict[int, ReliableMessage] = OrderedDict()  # 待确认消息
        self.send_times: Dict[int, float] = {}  # 发送时间记录
        self.lock = threading.Lock()

    def can_send(self) -> bool:
        """检查窗口是否有空间发送新消息"""
        with self.lock:
            return self.next_seq < self.base + self.window_size

    def allocate_seq(self) -> int:
        """分配新的序列号"""
        with self.lock:
            seq = self.next_seq
            self.next_seq += 1
            return seq

    def add_pending(self, msg: ReliableMessage):
        """添加待确认消息"""
        with self.lock:
            self.pending[msg.seq_num] = msg
            self.send_times[msg.seq_num] = time.time()

    def ack_received(self, seq_num: int) -> bool:
        """处理ACK，返回是否成功"""
        with self.lock:
            if seq_num in self.pending:
                del self.pending[seq_num]
                if seq_num in self.send_times:
                    del self.send_times[seq_num]
                # 滑动窗口基址
                while self.base not in self.pending and self.base < self.next_seq:
                    self.base += 1
                return True
            return False

    def get_timeout_messages(self, timeout: float = 5.0) -> list:
        """获取超时未确认的消息"""
        with self.lock:
            now = time.time()
            timeout_msgs = []
            for seq, msg in self.pending.items():
                if seq in self.send_times and now - self.send_times[seq] > timeout:
                    timeout_msgs.append(msg)
                    self.send_times[seq] = now  # 重置发送时间
            return timeout_msgs

    def get_pending_count(self) -> int:
        """获取待确认消息数量"""
        with self.lock:
            return len(self.pending)


class ReliableTransportLayer:
    """
    🛡️ 可靠传输层
    包装现有的gRPC通信，提供应用层可靠性保证
    """

    def __init__(
        self,
        node_id: int,
        send_func: Callable[[Any], None],      # 底层发送函数
        on_message_received: Callable[[Any], None],  # 消息接收回调
        window_size: int = 32,
        ack_timeout: float = 5.0,
        max_retries: int = 5,
        heartbeat_interval: float = 30.0,
        enable_checksum: bool = True
    ):
        self.node_id = node_id
        self.send_func = send_func
        self.on_message_received = on_message_received

        # 配置参数
        self.window_size = window_size
        self.ack_timeout = ack_timeout
        self.max_retries = max_retries
        self.heartbeat_interval = heartbeat_interval
        self.enable_checksum = enable_checksum

        # 每个peer的滑动窗口
        self.send_windows: Dict[int, SlidingWindow] = {}

        # 接收端状态
        self.expected_seq: Dict[int, int] = {}      # 每个peer期望的下一个序列号
        self.received_seqs: Dict[int, Set[int]] = {}  # 每个peer已接收的序列号集合
        self.out_of_order_buffer: Dict[int, Dict[int, ReliableMessage]] = {}  # 乱序缓冲区

        # 统计信息
        self.stats = {
            'messages_sent': 0,
            'messages_received': 0,
            'acks_sent': 0,
            'acks_received': 0,
            'retransmits': 0,
            'duplicates_dropped': 0,
            'checksum_errors': 0,
        }

        # 线程控制
        self.running = False
        self.retransmit_thread = None
        self.lock = threading.Lock()

        logger.info(f"[ALRT] Node {node_id}: Reliable transport initialized "
                   f"(window={window_size}, timeout={ack_timeout}s, max_retries={max_retries})")

    def start(self):
        """启动可靠传输层"""
        self.running = True
        self.retransmit_thread = threading.Thread(
            target=self._retransmit_loop,
            daemon=True,
            name=f"ALRT-Retransmit-{self.node_id}"
        )
        self.retransmit_thread.start()
        logger.info(f"[ALRT] Node {self.node_id}: Transport layer started")

    def stop(self):
        """停止可靠传输层"""
        self.running = False
        if self.retransmit_thread:
            self.retransmit_thread.join(timeout=2.0)
        logger.info(f"[ALRT] Node {self.node_id}: Transport layer stopped")

    def _get_send_window(self, peer_id: int) -> SlidingWindow:
        """获取或创建peer的发送窗口"""
        if peer_id not in self.send_windows:
            self.send_windows[peer_id] = SlidingWindow(self.window_size)
        return self.send_windows[peer_id]

    def send_reliable(self, receiver_id: int, msg_type: str, payload: Any) -> bool:
        """
        可靠发送消息
        返回是否成功加入发送队列（不保证已送达）
        """
        window = self._get_send_window(receiver_id)

        # 检查窗口是否已满
        if not window.can_send():
            logger.warning(f"[ALRT] Node {self.node_id}: Send window full for peer {receiver_id}")
            # 可以选择阻塞等待或直接返回失败
            # 这里选择等待一小段时间
            wait_start = time.time()
            while not window.can_send() and time.time() - wait_start < 1.0:
                time.sleep(0.01)
            if not window.can_send():
                return False

        # 分配序列号
        seq_num = window.allocate_seq()

        # 创建可靠消息
        reliable_msg = ReliableMessage(
            seq_num=seq_num,
            payload=payload,
            msg_type=msg_type,
            sender_id=self.node_id,
            receiver_id=receiver_id,
        )

        if self.enable_checksum:
            reliable_msg.checksum = reliable_msg.compute_checksum()

        # 添加到待确认队列
        window.add_pending(reliable_msg)

        # 发送
        self._do_send(reliable_msg)
        self.stats['messages_sent'] += 1

        logger.debug(f"[ALRT] Node {self.node_id}: Sent msg seq={seq_num} to peer {receiver_id}")
        return True

    def _do_send(self, msg: ReliableMessage):
        """实际发送消息"""
        try:
            # 将可靠消息包装后通过底层发送
            wrapped = {
                '_alrt_wrapper': True,
                'reliable_msg': msg
            }
            self.send_func(wrapped)
        except Exception as e:
            logger.error(f"[ALRT] Node {self.node_id}: Send failed: {e}")

    def _send_ack(self, receiver_id: int, ack_seq: int):
        """发送ACK确认"""
        ack_msg = ReliableMessage(
            seq_num=-1,  # ACK不需要序列号
            payload=None,
            msg_type='_alrt_ack',
            sender_id=self.node_id,
            receiver_id=receiver_id,
            is_ack=True,
            ack_seq=ack_seq
        )
        self._do_send(ack_msg)
        self.stats['acks_sent'] += 1
        logger.debug(f"[ALRT] Node {self.node_id}: Sent ACK for seq={ack_seq} to peer {receiver_id}")

    def on_raw_message_received(self, raw_msg: Any):
        """
        处理接收到的原始消息
        需要在gRPC接收端调用此方法
        """
        # 检查是否是ALRT包装的消息
        if isinstance(raw_msg, dict) and raw_msg.get('_alrt_wrapper'):
            reliable_msg = raw_msg['reliable_msg']
            self._handle_reliable_message(reliable_msg)
        else:
            # 非ALRT消息，直接透传
            self.on_message_received(raw_msg)

    def _handle_reliable_message(self, msg: ReliableMessage):
        """处理可靠消息"""
        sender_id = msg.sender_id

        # 处理ACK消息
        if msg.is_ack:
            self._handle_ack(sender_id, msg.ack_seq)
            return

        # 校验checksum
        if self.enable_checksum and msg.checksum:
            computed = msg.compute_checksum()
            if computed != msg.checksum:
                logger.warning(f"[ALRT] Node {self.node_id}: Checksum mismatch for seq={msg.seq_num} from peer {sender_id}")
                self.stats['checksum_errors'] += 1
                # 不发送ACK，等待重传
                return

        # 初始化接收端状态
        if sender_id not in self.expected_seq:
            self.expected_seq[sender_id] = 0
            self.received_seqs[sender_id] = set()
            self.out_of_order_buffer[sender_id] = {}

        seq_num = msg.seq_num

        # 检查是否是重复消息
        if seq_num in self.received_seqs[sender_id]:
            logger.debug(f"[ALRT] Node {self.node_id}: Duplicate msg seq={seq_num} from peer {sender_id}, sending ACK")
            self.stats['duplicates_dropped'] += 1
            self._send_ack(sender_id, seq_num)  # 重复消息也要发ACK
            return

        # 记录已接收
        self.received_seqs[sender_id].add(seq_num)
        self._send_ack(sender_id, seq_num)

        # 检查是否按序到达
        expected = self.expected_seq[sender_id]

        if seq_num == expected:
            # 按序到达，处理消息
            self._deliver_message(msg)
            self.expected_seq[sender_id] = seq_num + 1

            # 检查乱序缓冲区是否有后续消息可以处理
            self._process_out_of_order_buffer(sender_id)
        else:
            # 乱序到达，缓存
            self.out_of_order_buffer[sender_id][seq_num] = msg
            logger.debug(f"[ALRT] Node {self.node_id}: Out-of-order msg seq={seq_num} from peer {sender_id}, expected={expected}")

    def _process_out_of_order_buffer(self, sender_id: int):
        """处理乱序缓冲区"""
        buffer = self.out_of_order_buffer[sender_id]
        while self.expected_seq[sender_id] in buffer:
            seq = self.expected_seq[sender_id]
            msg = buffer.pop(seq)
            self._deliver_message(msg)
            self.expected_seq[sender_id] = seq + 1

    def _deliver_message(self, msg: ReliableMessage):
        """交付消息给上层"""
        self.stats['messages_received'] += 1
        logger.debug(f"[ALRT] Node {self.node_id}: Delivering msg seq={msg.seq_num} type={msg.msg_type}")
        # 调用上层回调，传递原始payload
        self.on_message_received(msg.payload)

    def _handle_ack(self, sender_id: int, ack_seq: int):
        """处理收到的ACK"""
        window = self._get_send_window(sender_id)
        if window.ack_received(ack_seq):
            self.stats['acks_received'] += 1
            logger.debug(f"[ALRT] Node {self.node_id}: ACK received for seq={ack_seq} from peer {sender_id}")

    def _retransmit_loop(self):
        """重传循环"""
        retry_counts: Dict[tuple, int] = {}  # (peer_id, seq_num) -> retry_count

        while self.running:
            time.sleep(1.0)  # 每秒检查一次

            for peer_id, window in list(self.send_windows.items()):
                timeout_msgs = window.get_timeout_messages(self.ack_timeout)

                for msg in timeout_msgs:
                    key = (peer_id, msg.seq_num)
                    retry_counts[key] = retry_counts.get(key, 0) + 1

                    if retry_counts[key] > self.max_retries:
                        logger.error(f"[ALRT] Node {self.node_id}: Max retries exceeded for seq={msg.seq_num} to peer {peer_id}")
                        # 可以选择：通知上层、放弃消息、或继续尝试
                        continue

                    # 重传
                    msg.is_retransmit = True
                    self._do_send(msg)
                    self.stats['retransmits'] += 1
                    logger.warning(f"[ALRT] Node {self.node_id}: Retransmit seq={msg.seq_num} to peer {peer_id} "
                                 f"(attempt {retry_counts[key]}/{self.max_retries})")

    def get_stats(self) -> dict:
        """获取统计信息"""
        stats = self.stats.copy()
        stats['pending_messages'] = sum(w.get_pending_count() for w in self.send_windows.values())
        return stats

    def get_stats_summary(self) -> str:
        """获取统计摘要"""
        stats = self.get_stats()
        return (f"[ALRT Stats] sent={stats['messages_sent']}, recv={stats['messages_received']}, "
                f"retrans={stats['retransmits']}, acks={stats['acks_received']}, "
                f"pending={stats['pending_messages']}, dups={stats['duplicates_dropped']}")


class ReliableCommManagerWrapper:
    """
    🛡️ 可靠通信管理器包装器
    将现有的gRPCCommManager包装成可靠版本
    """

    def __init__(self, comm_manager, node_id: int, **kwargs):
        """
        Args:
            comm_manager: 原始的gRPCCommManager实例
            node_id: 当前节点ID
            **kwargs: 传递给ReliableTransportLayer的参数
        """
        self.comm_manager = comm_manager
        self.node_id = node_id

        # 保存原始的receive处理方法
        self._original_receive = comm_manager.receive if hasattr(comm_manager, 'receive') else None

        # 消息回调队列
        self.received_queue = queue.Queue()

        # 创建可靠传输层
        self.transport = ReliableTransportLayer(
            node_id=node_id,
            send_func=self._raw_send,
            on_message_received=self._on_reliable_message,
            **kwargs
        )

        self.transport.start()
        logger.info(f"[ALRT] ReliableCommManagerWrapper initialized for node {node_id}")

    def _raw_send(self, wrapped_msg):
        """底层发送（包装后的ALRT消息）"""
        # 这里需要将ALRT消息通过原始comm_manager发送
        # 假设comm_manager.send接受Message对象
        from federatedscope.core.message import Message

        reliable_msg = wrapped_msg.get('reliable_msg')
        if reliable_msg:
            # 创建一个特殊的Message用于传输ALRT消息
            msg = Message(
                msg_type='_alrt_transport',
                sender=self.node_id,
                receiver=[reliable_msg.receiver_id],
                content=wrapped_msg
            )
            self.comm_manager.send(msg)

    def _on_reliable_message(self, payload):
        """可靠消息到达回调"""
        self.received_queue.put(payload)

    def send(self, message):
        """
        可靠发送消息
        """
        receiver = message.receiver
        if receiver is not None:
            if not isinstance(receiver, list):
                receiver = [receiver]

            for each_receiver in receiver:
                self.transport.send_reliable(
                    receiver_id=each_receiver,
                    msg_type=message.msg_type,
                    payload=message
                )
        else:
            # 广播给所有邻居
            for each_receiver in self.comm_manager.neighbors:
                self.transport.send_reliable(
                    receiver_id=each_receiver,
                    msg_type=message.msg_type,
                    payload=message
                )

    def receive(self):
        """
        接收消息（需要与原始receive协调）
        """
        # 如果有可靠传输层的消息，优先返回
        try:
            return self.received_queue.get_nowait()
        except queue.Empty:
            pass

        # 否则调用原始receive
        if self._original_receive:
            raw_msg = self._original_receive()

            # 检查是否是ALRT传输消息
            if hasattr(raw_msg, 'msg_type') and raw_msg.msg_type == '_alrt_transport':
                self.transport.on_raw_message_received(raw_msg.content)
                # 递归获取实际消息
                return self.receive()

            return raw_msg

        return None

    def get_stats(self) -> str:
        """获取ALRT统计信息"""
        return self.transport.get_stats_summary()

    def stop(self):
        """停止可靠传输层"""
        self.transport.stop()

    # 代理其他方法给原始comm_manager
    def __getattr__(self, name):
        return getattr(self.comm_manager, name)


# ============================================================================
# Transparent Wrapper Layer for gRPC Communication
# ============================================================================
# Usage: Minimal modification to existing code
#   - Call lft_pack() before sending large data
#   - Call lft_unpack() after receiving data
# ============================================================================

# Global instances (thread-safe for single sender per connection)
_fragmenter_cache: Dict[int, LightweightFragmenter] = {}
_reassembler_cache: Dict[int, LightweightReassembler] = {}

# LFT packet header format: magic(4) + msg_id(4) + frag_idx(4) + total(4) + crc(4) = 20 bytes
LFT_HEADER_FORMAT = '<IIIII'
LFT_HEADER_SIZE = 20
LFT_MAGIC = 0x4C465421  # "LFT!" identifier


def get_fragmenter(node_id: int = 0) -> LightweightFragmenter:
    """Get or create fragmenter for a node (cached)"""
    if node_id not in _fragmenter_cache:
        _fragmenter_cache[node_id] = LightweightFragmenter(FRAGMENT_SIZE)
    return _fragmenter_cache[node_id]


def get_reassembler(node_id: int = 0) -> LightweightReassembler:
    """Get or create reassembler for a node (cached)"""
    if node_id not in _reassembler_cache:
        _reassembler_cache[node_id] = LightweightReassembler(timeout=60.0)
    return _reassembler_cache[node_id]


def lft_pack(data: bytes, node_id: int = 0) -> List[bytes]:
    """
    Pack data into LFT fragments.

    Args:
        data: Raw bytes to send
        node_id: Sender node ID (for fragmenter cache)

    Returns:
        List of fragment packets, each with header + payload

    Usage:
        packets = lft_pack(large_data)
        for pkt in packets:
            grpc_send(pkt)
    """
    if len(data) <= FRAGMENT_SIZE:
        # Small message: single fragment with header
        crc = fast_checksum(data)
        header = struct.pack(LFT_HEADER_FORMAT, LFT_MAGIC, 0, 0, 1, crc)
        return [header + data]

    # Large message: fragment
    fragmenter = get_fragmenter(node_id)
    fragments = fragmenter.fragment_bytes(data)

    packets = []
    for msg_id, frag_idx, total_frags, frag_data, crc in fragments:
        header = struct.pack(LFT_HEADER_FORMAT, LFT_MAGIC, msg_id, frag_idx, total_frags, crc)
        packets.append(header + frag_data)

    return packets


def lft_unpack(packet: bytes, node_id: int = 0) -> Optional[bytes]:
    """
    Unpack LFT fragment packet.

    Args:
        packet: Received packet (header + payload)
        node_id: Receiver node ID (for reassembler cache)

    Returns:
        - Complete reassembled data if all fragments received
        - None if waiting for more fragments
        - Original packet if not an LFT packet

    Usage:
        data = lft_unpack(received_packet)
        if data is not None:
            process(data)
    """
    # Check if it's an LFT packet
    if len(packet) < LFT_HEADER_SIZE:
        return packet  # Not LFT, return as-is

    magic, msg_id, frag_idx, total_frags, crc = struct.unpack(LFT_HEADER_FORMAT, packet[:LFT_HEADER_SIZE])

    if magic != LFT_MAGIC:
        return packet  # Not LFT packet, return as-is

    payload = packet[LFT_HEADER_SIZE:]

    # Verify CRC32
    if fast_checksum(payload) != crc:
        logger.warning(f"[LFT] CRC mismatch: msg={msg_id}, frag={frag_idx}")
        return None

    # Single fragment: return directly
    if total_frags == 1:
        return payload

    # Multi-fragment: reassemble
    reassembler = get_reassembler(node_id)
    return reassembler.add_fragment(msg_id, frag_idx, total_frags, payload, crc)


def lft_is_enabled() -> bool:
    """Check if LFT is enabled (based on buffer size detection)"""
    return FRAGMENT_SIZE < 200 * 1024  # Enabled if fragment size is reasonable


def lft_get_stats(node_id: int = 0) -> dict:
    """Get LFT statistics for a node"""
    stats = {
        'fragment_size': FRAGMENT_SIZE,
        'enabled': lft_is_enabled(),
    }

    if node_id in _fragmenter_cache:
        stats['messages_fragmented'] = _fragmenter_cache[node_id].msg_counter

    if node_id in _reassembler_cache:
        reassembler = _reassembler_cache[node_id]
        stats['pending_messages'] = len(reassembler.pending)

    return stats


def lft_cleanup(node_id: int = 0) -> int:
    """Cleanup timeout pending messages"""
    if node_id in _reassembler_cache:
        return _reassembler_cache[node_id].cleanup()
    return 0
