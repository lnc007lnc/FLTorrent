"""
Per-round overhead statistics collector for Cadence.

Tracks bitmap+HAVE/bitfield metadata bytes, importance-score metadata bytes,
scoring CPU time, and post-aggregation compensation CPU time. One collector
per client; flush_round() appends a JSON line to a JSONL file.
"""

import json
import os
import threading
import logging

logger = logging.getLogger(__name__)


HAVE_HEADER_BYTES = 32          # msg_type + sender + receiver + state framing
HAVE_CONTENT_BYTES = 18         # round_num + source_client_id + chunk_id (3 varints, ~6B each)
IMPORTANCE_FIELD_BYTES = 9      # importance_score float field tag + 8B double

BITFIELD_HEADER_BYTES = 36      # msg_type + sender + receiver + state framing + list overhead
BITFIELD_ENTRY_CONTENT_BYTES = 24   # round + source + chunk (3 varints) + dict overhead
BITFIELD_ENTRY_IMPORTANCE_BYTES = 9  # importance_score per entry


class OverheadStatsCollector:
    """Per-client per-round overhead counters; thread-safe."""

    def __init__(self, client_id: int, output_file: str):
        self.client_id = int(client_id)
        self.output_file = output_file
        self._lock = threading.Lock()
        self._reset_unlocked()
        if self.output_file:
            os.makedirs(os.path.dirname(self.output_file), exist_ok=True)

    def _reset_unlocked(self):
        self.have_msg_count = 0
        self.have_metadata_bytes = 0       # bitmap/HAVE control overhead (excl. importance)
        self.have_importance_bytes = 0     # importance score field bytes
        self.bitfield_msg_count = 0
        self.bitfield_metadata_bytes = 0
        self.bitfield_importance_bytes = 0
        self.scoring_ms = 0.0
        self.scoring_calls = 0
        self.compensation_ms = 0.0
        self.compensation_calls = 0
        # Swarming-isolation metrics (P0-3)
        self.served_own_count = 0          # uploads served from chunks the peer originally owned
        self.served_received_count = 0     # uploads served from chunks received in the same round
        self.served_total_count = 0        # served_own + served_received + dropped (owner-only)
        self.served_owner_only_dropped = 0 # requests dropped because of owner_only_serving
        self.requests_sent = 0             # total requests this peer issued
        self.requests_duplicate = 0        # retries (same chunk requested >1 time)
        self.received_chunk_count = 0      # chunks successfully received this round
        self.received_importance_sum = 0.0 # sum of importance of received chunks (numerator of utility-rho)
        self.total_importance_sum = 0.0    # sum of importance of all chunks the peer needed (denominator)
        self.received_source_set = set()   # distinct source clients we received from this round

    def reset(self):
        with self._lock:
            self._reset_unlocked()

    def record_have(self, recipients: int = 1, with_importance: bool = True):
        """One HAVE broadcast event; recipients = number of neighbors it goes to."""
        if recipients <= 0:
            return
        meta = (HAVE_HEADER_BYTES + HAVE_CONTENT_BYTES) * recipients
        imp = (IMPORTANCE_FIELD_BYTES * recipients) if with_importance else 0
        with self._lock:
            self.have_msg_count += recipients
            self.have_metadata_bytes += meta
            self.have_importance_bytes += imp

    def record_bitfield(self, n_entries: int, with_importance: bool = True):
        """One bitfield message containing n_entries chunks."""
        n_entries = max(0, int(n_entries))
        meta = BITFIELD_HEADER_BYTES + BITFIELD_ENTRY_CONTENT_BYTES * n_entries
        imp = (BITFIELD_ENTRY_IMPORTANCE_BYTES * n_entries) if with_importance else 0
        with self._lock:
            self.bitfield_msg_count += 1
            self.bitfield_metadata_bytes += meta
            self.bitfield_importance_bytes += imp

    def record_scoring(self, elapsed_ms: float):
        with self._lock:
            self.scoring_ms += float(elapsed_ms)
            self.scoring_calls += 1

    def record_compensation(self, elapsed_ms: float):
        with self._lock:
            self.compensation_ms += float(elapsed_ms)
            self.compensation_calls += 1

    # === Swarming-isolation recorders (P0-3) ===
    def record_serve(self, is_own: bool, dropped: bool = False):
        """Record one upload-serve event.

        is_own  : True if served from peer's originally-owned chunks
        dropped : True if request was dropped because of owner_only_serving
        """
        with self._lock:
            self.served_total_count += 1
            if dropped:
                self.served_owner_only_dropped += 1
            elif is_own:
                self.served_own_count += 1
            else:
                self.served_received_count += 1

    def record_request_sent(self, is_duplicate: bool = False):
        with self._lock:
            self.requests_sent += 1
            if is_duplicate:
                self.requests_duplicate += 1

    def record_received_chunk(self, source_id: int, importance: float = 0.0):
        with self._lock:
            self.received_chunk_count += 1
            self.received_importance_sum += float(importance)
            self.received_source_set.add(int(source_id))

    def record_total_importance(self, importance_sum: float):
        """Set the denominator for utility-rho (called once per round, before flush)."""
        with self._lock:
            self.total_importance_sum = float(importance_sum)

    def snapshot(self, round_num: int) -> dict:
        with self._lock:
            utility_rho = (self.received_importance_sum / self.total_importance_sum
                           if self.total_importance_sum > 0 else None)
            non_owner_serving_frac = (self.served_received_count / self.served_total_count
                                      if self.served_total_count > 0 else None)
            dup_request_ratio = (self.requests_duplicate / self.requests_sent
                                 if self.requests_sent > 0 else None)
            return {
                'client_id': self.client_id,
                'round_num': int(round_num),
                # overhead (P2)
                'have_msg_count': self.have_msg_count,
                'have_metadata_bytes': self.have_metadata_bytes,
                'have_importance_bytes': self.have_importance_bytes,
                'bitfield_msg_count': self.bitfield_msg_count,
                'bitfield_metadata_bytes': self.bitfield_metadata_bytes,
                'bitfield_importance_bytes': self.bitfield_importance_bytes,
                'scoring_ms': self.scoring_ms,
                'scoring_calls': self.scoring_calls,
                'compensation_ms': self.compensation_ms,
                'compensation_calls': self.compensation_calls,
                # swarming-isolation (P0-3)
                'served_own_count': self.served_own_count,
                'served_received_count': self.served_received_count,
                'served_total_count': self.served_total_count,
                'served_owner_only_dropped': self.served_owner_only_dropped,
                'non_owner_serving_frac': non_owner_serving_frac,
                'requests_sent': self.requests_sent,
                'requests_duplicate': self.requests_duplicate,
                'dup_request_ratio': dup_request_ratio,
                'received_chunk_count': self.received_chunk_count,
                'received_importance_sum': self.received_importance_sum,
                'total_importance_sum': self.total_importance_sum,
                'utility_rho': utility_rho,
                'distinct_sources': len(self.received_source_set),
            }

    def flush_round(self, round_num: int):
        if not self.output_file:
            return
        record = self.snapshot(round_num)
        try:
            with open(self.output_file, 'a') as f:
                f.write(json.dumps(record) + '\n')
        except Exception as e:
            logger.warning(f"[OverheadStats] Client {self.client_id}: failed to flush round {round_num}: {e}")
        finally:
            self.reset()
