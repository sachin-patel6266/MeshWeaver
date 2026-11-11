"""Configuration shared by MeshWeaver nodes."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(slots=True)
class NodeConfig:
    """Network and scheduling settings for one MeshWeaver process."""

    host: str = "127.0.0.1"
    port: int = 8765
    advertise_host: str | None = None
    udp_port: int = 8766
    multicast_group: str = "239.192.0.42"
    discovery: bool = True
    gossip_interval: float = 5.0
    heartbeat_interval: float = 2.0
    heartbeat_timeout: float = 8.0
    rpc_timeout: float = 5.0
    peer_ttl: float = 30.0
    replication_factor: int = 8
    max_message_bytes: int = 8 * 1024 * 1024
    task_timeout: float = 120.0
    bootstrap: list[tuple[str, int]] = field(default_factory=list)

    @property
    def public_host(self) -> str:
        return self.advertise_host or self.host

