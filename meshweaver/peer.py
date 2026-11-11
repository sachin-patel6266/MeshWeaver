"""Peer records and the Kademlia XOR-distance routing table."""

from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass(slots=True)
class PeerInfo:
    """Signed, authenticated peer status used for routing and scheduling."""

    node_id: str
    host: str
    port: int
    cpu_percent: float = 0.0
    memory_percent: float = 0.0
    last_seen: float = field(default_factory=time.monotonic)
    tasks_running: int = 0

    def as_dict(self) -> dict[str, str | int | float]:
        return {
            "node_id": self.node_id,
            "host": self.host,
            "port": self.port,
            "cpu_percent": self.cpu_percent,
            "memory_percent": self.memory_percent,
            "tasks_running": self.tasks_running,
        }

    @classmethod
    def from_dict(cls, value: dict[str, object]) -> "PeerInfo":
        return cls(
            node_id=str(value["node_id"]),
            host=str(value["host"]),
            port=int(value["port"]),
            cpu_percent=float(value.get("cpu_percent", 0.0)),
            memory_percent=float(value.get("memory_percent", 0.0)),
            tasks_running=int(value.get("tasks_running", 0)),
        )


class KademliaRoutingTable:
    """A bounded Kademlia-style routing table with 256 XOR-distance buckets."""

    def __init__(self, local_node_id: str, bucket_size: int = 8):
        self.local_node_id = int(local_node_id, 16)
        self.bucket_size = bucket_size
        self._buckets: list[dict[str, PeerInfo]] = [dict() for _ in range(256)]

    def _bucket_index(self, node_id: str) -> int:
        distance = self.local_node_id ^ int(node_id, 16)
        if distance == 0:
            raise ValueError("A node cannot add itself to its routing table.")
        return distance.bit_length() - 1

    def add(self, peer: PeerInfo) -> None:
        bucket = self._buckets[self._bucket_index(peer.node_id)]
        bucket.pop(peer.node_id, None)
        bucket[peer.node_id] = peer
        while len(bucket) > self.bucket_size:
            bucket.pop(next(iter(bucket)))

    def remove(self, node_id: str) -> None:
        if node_id == self.local_node_id.to_bytes(32, "big").hex():
            return
        try:
            self._buckets[self._bucket_index(node_id)].pop(node_id, None)
        except ValueError:
            return

    def get(self, node_id: str) -> PeerInfo | None:
        if node_id == self.local_node_id.to_bytes(32, "big").hex():
            return None
        return self._buckets[self._bucket_index(node_id)].get(node_id)

    def all_peers(self) -> list[PeerInfo]:
        return [peer for bucket in self._buckets for peer in bucket.values()]

    def closest(self, target_id: str, count: int | None = None) -> list[PeerInfo]:
        peers = sorted(
            self.all_peers(),
            key=lambda peer: int(peer.node_id, 16) ^ int(target_id, 16),
        )
        return peers if count is None else peers[:count]

