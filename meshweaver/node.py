"""Async peer node: signed RPC, Kademlia routing, gossip, and task execution."""

from __future__ import annotations

import asyncio
import base64
from collections import OrderedDict
import json
import logging
import inspect
import socket
import ssl
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable

import cloudpickle
import psutil
from cryptography import x509
from cryptography.hazmat.primitives import hashes

from meshweaver.config import NodeConfig
from meshweaver.identity import (
    Identity,
    load_trusted_certificates,
    tls_contexts,
    verify_signature,
)
from meshweaver.peer import KademliaRoutingTable, PeerInfo
from meshweaver.protocol import ProtocolError, encode_envelope, read_envelope

logger = logging.getLogger("meshweaver")


@dataclass(slots=True)
class PendingTask:
    task_id: str
    payload: bytes
    result: asyncio.Future[Any]
    attempted: set[str] = field(default_factory=set)
    assigned_to: str | None = None
    reassigned: asyncio.Event = field(default_factory=asyncio.Event)


@dataclass(slots=True)
class RemoteTaskRecord:
    sender_id: str
    task_id: str
    origin_host: str
    origin_port: int
    state: str = "running"
    result_message: dict[str, Any] | None = None
    result_size: int = 0


class _DiscoveryProtocol(asyncio.DatagramProtocol):
    def __init__(self, node: "MeshWeaverNode"):
        self.node = node

    def datagram_received(self, data: bytes, addr: tuple[str, int]) -> None:
        asyncio.create_task(self.node._handle_discovery(data, addr))

    def error_received(self, exc: Exception) -> None:
        logger.debug("UDP discovery error: %s", exc)


class MeshWeaverNode:
    """A trusted-member asyncio node in a decentralized MeshWeaver cluster."""

    def __init__(
        self,
        identity: Identity,
        trusted_certificates_path: str,
        config: NodeConfig | None = None,
    ):
        self.identity = identity
        self.config = config or NodeConfig()
        self.trusted_certificates_path = trusted_certificates_path
        self.trusted_certificates = load_trusted_certificates(trusted_certificates_path)
        if identity.fingerprint not in self.trusted_certificates:
            raise ValueError("The local identity certificate must be in the trust bundle.")
        self.server_tls, self.client_tls = tls_contexts(
            identity, trusted_certificates_path
        )
        self.routing = KademliaRoutingTable(
            identity.node_id, self.config.replication_factor
        )
        self._server: asyncio.AbstractServer | None = None
        self._datagram_transport: asyncio.DatagramTransport | None = None
        self._background: list[asyncio.Task[Any]] = []
        self._closed = False
        self.cpu_percent = 0.0
        self.memory_percent = 0.0
        self.started_at = time.monotonic()
        self._pending: dict[str, PendingTask] = {}
        self._running: dict[str, asyncio.Task[Any]] = {}
        self._completed: list[tuple[str, str]] = []
        self._remote_tasks: OrderedDict[tuple[str, str], RemoteTaskRecord] = OrderedDict()
        self._remote_result_cache_bytes = 0

    @property
    def node_id(self) -> str:
        return self.identity.node_id

    @property
    def peers(self) -> list[PeerInfo]:
        return self.routing.all_peers()

    @property
    def own_info(self) -> PeerInfo:
        return PeerInfo(
            node_id=self.node_id,
            host=self.config.public_host,
            port=self.config.port,
            cpu_percent=self.cpu_percent,
            memory_percent=self.memory_percent,
            tasks_running=len(self._running),
        )

    @property
    def task_status(self) -> list[tuple[str, str]]:
        return [
            *((task_id, "running") for task_id in self._running),
            *((task_id, "waiting") for task_id in self._pending),
            *self._completed[-10:],
        ]

    async def start(self) -> None:
        """Start the mutual-TLS TCP server, UDP discovery, and maintenance loops."""
        self._closed = False
        self._server = await asyncio.start_server(
            self._handle_connection,
            self.config.host,
            self.config.port,
            ssl=self.server_tls,
            limit=self.config.max_message_bytes + 1,
        )
        if self.config.port == 0 and self._server.sockets:
            self.config.port = int(self._server.sockets[0].getsockname()[1])
        if self.config.discovery:
            await self._start_discovery()
        self._background = [
            asyncio.create_task(self._metrics_loop(), name="meshweaver-metrics"),
            asyncio.create_task(self._gossip_loop(), name="meshweaver-gossip"),
            asyncio.create_task(self._heartbeat_loop(), name="meshweaver-heartbeat"),
        ]
        if self.config.discovery:
            self._background.append(
                asyncio.create_task(self._discovery_loop(), name="meshweaver-discovery")
            )
        for host, port in self.config.bootstrap:
            try:
                await self._connect_bootstrap(host, port)
            except (OSError, ssl.SSLError, asyncio.TimeoutError) as exc:
                logger.warning("Could not connect to bootstrap peer %s:%s: %s", host, port, exc)

    async def close(self) -> None:
        """Stop background work and close all listeners."""
        if self._closed:
            return
        self._closed = True
        for task in self._background:
            task.cancel()
        if self._background:
            await asyncio.gather(*self._background, return_exceptions=True)
        for task in self._running.values():
            task.cancel()
        if self._running:
            await asyncio.gather(*self._running.values(), return_exceptions=True)
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
        if self._datagram_transport is not None:
            self._datagram_transport.close()

    async def submit(
        self,
        function: Callable[..., Any],
        *args: Any,
        timeout: float | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a signed cloudpickle task on the least-loaded trusted peer."""
        task_id = uuid.uuid4().hex
        payload = cloudpickle.dumps((function, args, kwargs))
        if len(payload) > self.config.max_message_bytes // 2:
            raise ValueError("Serialized task exceeds the configured message size limit.")
        future = asyncio.get_running_loop().create_future()
        pending = PendingTask(task_id=task_id, payload=payload, result=future)
        self._pending[task_id] = pending
        deadline = time.monotonic() + (
            timeout if timeout is not None else self.config.task_timeout
        )
        try:
            while not future.done():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(f"Task {task_id} exceeded its execution timeout.")
                peer = await self._select_peer(pending.attempted)
                pending.attempted.add(peer.node_id)
                pending.assigned_to = peer.node_id
                pending.reassigned.clear()
                if peer.node_id == self.node_id:
                    run = asyncio.create_task(
                        self._execute_local(task_id, payload), name=f"task-{task_id}"
                    )
                    self._running[task_id] = run
                    assignment_wait = asyncio.create_task(pending.reassigned.wait())
                    done, _ = await asyncio.wait(
                        {run, assignment_wait},
                        timeout=remaining,
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    if assignment_wait not in done:
                        assignment_wait.cancel()
                        await asyncio.gather(assignment_wait, return_exceptions=True)
                    if run in done:
                        await run
                    elif not done:
                        run.cancel()
                        await asyncio.gather(run, return_exceptions=True)
                    continue
                request = {
                    "type": "task_submit",
                    "task_id": task_id,
                    "payload": base64.b64encode(payload).decode("ascii"),
                    "origin_host": self.config.public_host,
                    "origin_port": self.config.port,
                }
                try:
                    response = await self._rpc(peer.host, peer.port, request)
                except (OSError, ssl.SSLError, ProtocolError, asyncio.TimeoutError):
                    self.routing.remove(peer.node_id)
                    await self._reassign_from(peer.node_id)
                    continue
                if response.get("status") != "accepted":
                    raise RuntimeError(f"Peer rejected task {task_id}: {response.get('error')}")
                assignment_wait = asyncio.create_task(pending.reassigned.wait())
                done, _ = await asyncio.wait(
                    {future, assignment_wait},
                    timeout=remaining,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if assignment_wait not in done:
                    assignment_wait.cancel()
                    await asyncio.gather(assignment_wait, return_exceptions=True)
                if future in done:
                    break
                if not done:
                    raise TimeoutError(f"Task {task_id} exceeded its execution timeout.")
            return await future
        finally:
            self._pending.pop(task_id, None)

    async def find_node(self, target_id: str) -> list[PeerInfo]:
        """Perform an iterative Kademlia lookup and return the closest known peers."""
        asked: set[str] = set()
        while True:
            candidates = [
                peer
                for peer in self.routing.closest(target_id, self.config.replication_factor)
                if peer.node_id not in asked
            ]
            if not candidates:
                break
            asked.update(peer.node_id for peer in candidates)
            for peer in candidates:
                try:
                    response = await self._rpc(
                        peer.host, peer.port, {"type": "find_node", "target_id": target_id}
                    )
                    for record in response.get("peers", []):
                        self._learn_peer(PeerInfo.from_dict(record))
                except (
                    OSError,
                    ssl.SSLError,
                    ProtocolError,
                    asyncio.TimeoutError,
                    KeyError,
                    ValueError,
                ):
                    self.routing.remove(peer.node_id)
        return self.routing.closest(target_id, self.config.replication_factor)

    async def _select_peer(self, attempted: set[str]) -> PeerInfo:
        self_info = self.own_info
        candidates = [peer for peer in self.peers if peer.node_id not in attempted]
        if self.node_id not in attempted:
            candidates.append(self_info)
        if not candidates:
            raise RuntimeError("No untried trusted peers remain for task reassignment.")
        return min(
            candidates,
            key=lambda peer: (peer.cpu_percent, peer.memory_percent, peer.tasks_running),
        )

    async def _execute_local(self, task_id: str, payload: bytes) -> None:
        try:
            function, args, kwargs = cloudpickle.loads(payload)
            result = await self._invoke(function, args, kwargs)
            pending = self._pending.get(task_id)
            self._record_completion(task_id, "completed")
            if pending is not None and pending.assigned_to == self.node_id:
                if not pending.result.done():
                    pending.result.set_result(result)
        except Exception as exc:
            pending = self._pending.get(task_id)
            self._record_completion(task_id, "failed")
            if pending is not None and pending.assigned_to == self.node_id:
                if not pending.result.done():
                    pending.result.set_exception(exc)
        finally:
            self._running.pop(task_id, None)

    async def _invoke(self, function: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
        if inspect.iscoroutinefunction(function):
            return await asyncio.wait_for(function(*args, **kwargs), self.config.task_timeout)
        result = await asyncio.wait_for(
            asyncio.to_thread(function, *args, **kwargs), self.config.task_timeout
        )
        if asyncio.iscoroutine(result):
            return await asyncio.wait_for(result, self.config.task_timeout)
        return result

    async def _execute_received(
        self,
        record: RemoteTaskRecord,
        payload: bytes,
    ) -> None:
        task_id = record.task_id
        try:
            function, args, kwargs = cloudpickle.loads(payload)
            result = await self._invoke(function, args, kwargs)
            serialized = cloudpickle.dumps(result)
            if len(serialized) > self.config.max_message_bytes // 2:
                raise ValueError("Serialized task result exceeds the message size limit.")
            message = {
                "type": "task_result",
                "task_id": task_id,
                "payload": base64.b64encode(serialized).decode("ascii"),
            }
            record.state = "completed"
            status = "completed"
        except asyncio.CancelledError:
            record.state = "cancelled"
            self._record_completion(task_id, "cancelled")
            raise
        except Exception as exc:
            logger.exception("Task %s failed on this peer", task_id)
            message = {"type": "task_error", "task_id": task_id, "error": str(exc)}
            record.state = "failed"
            status = "failed"
        self._record_completion(task_id, status)
        self._running.pop(task_id, None)
        self._cache_remote_result(record, message)
        self._prune_remote_tasks((record.sender_id, task_id))
        await self._deliver_remote_result(record)

    async def _deliver_remote_result(self, record: RemoteTaskRecord) -> None:
        if record.result_message is None:
            return
        try:
            await self._rpc(
                record.origin_host, record.origin_port, record.result_message
            )
        except (OSError, ssl.SSLError, ProtocolError, asyncio.TimeoutError):
            logger.warning("Unable to deliver the result for task %s", record.task_id)

    def _record_completion(self, task_id: str, status: str) -> None:
        for index, (recorded_id, _) in enumerate(self._completed):
            if recorded_id == task_id:
                self._completed[index] = (task_id, status)
                return
        self._completed.append((task_id, status))
        if len(self._completed) > 1000:
            del self._completed[:-1000]

    def _cache_remote_result(
        self, record: RemoteTaskRecord, message: dict[str, Any]
    ) -> None:
        encoded_size = len(json.dumps(message, separators=(",", ":")).encode("utf-8"))
        if encoded_size > 16 * 1024 * 1024:
            return
        while self._remote_result_cache_bytes + encoded_size > 16 * 1024 * 1024:
            oldest = next(
                (
                    existing
                    for existing in self._remote_tasks.values()
                    if existing.result_message is not None
                ),
                None,
            )
            if oldest is None:
                return
            self._remote_result_cache_bytes -= oldest.result_size
            oldest.result_message = None
            oldest.result_size = 0
        record.result_message = message
        record.result_size = encoded_size
        self._remote_result_cache_bytes += encoded_size

    def _prune_remote_tasks(
        self, protected_key: tuple[str, str] | None = None
    ) -> None:
        while len(self._remote_tasks) > 1024:
            removable_key = next(
                (
                    key
                    for key, record in self._remote_tasks.items()
                    if record.state != "running" and key != protected_key
                ),
                None,
            )
            if removable_key is None:
                return
            removed = self._remote_tasks.pop(removable_key)
            self._remote_result_cache_bytes -= removed.result_size

    async def _handle_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            ssl_object = writer.get_extra_info("ssl_object")
            certificate_bytes = ssl_object.getpeercert(binary_form=True) if ssl_object else None
            if not certificate_bytes:
                raise ProtocolError("RPC requires a mutually authenticated TLS connection.")
            certificate = x509.load_der_x509_certificate(certificate_bytes)
            fingerprint = certificate.fingerprint(hashes.SHA256()).hex()
            trusted = self.trusted_certificates.get(fingerprint)
            if trusted is None:
                raise ProtocolError("TLS peer certificate is not in the local trust bundle.")
            envelope = await read_envelope(reader, self.config.max_message_bytes)
            body = {"sender": envelope.get("sender"), "message": envelope.get("message")}
            if body["sender"] != fingerprint or not verify_signature(
                trusted, body, str(envelope.get("signature", ""))
            ):
                raise ProtocolError("RPC signature is invalid or does not match the TLS peer.")
            message = body["message"]
            if not isinstance(message, dict):
                raise ProtocolError("RPC message body must be an object.")
            remote_address = writer.get_extra_info("peername")
            response = await self._dispatch(fingerprint, message, remote_address)
            writer.write(encode_envelope(self.identity, response))
            await writer.drain()
        except (ProtocolError, OSError, ValueError, KeyError) as exc:
            logger.debug("Rejected RPC connection: %s", exc)
            try:
                writer.write(encode_envelope(self.identity, {"status": "error", "error": str(exc)}))
                await writer.drain()
            except OSError:
                pass
        except Exception:
            logger.exception("Unexpected RPC handler failure")
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except OSError:
                pass

    async def _dispatch(
        self, sender_id: str, message: dict[str, Any], remote_address: Any
    ) -> dict[str, Any]:
        kind = message.get("type")
        if kind == "ping":
            self._learn_sender(sender_id, message, remote_address)
            return {"status": "ok", "peer": self.own_info.as_dict()}
        if kind == "gossip":
            self._learn_sender(sender_id, message, remote_address)
            for record in message.get("peers", []):
                if not isinstance(record, dict):
                    continue
                try:
                    peer = PeerInfo.from_dict(record)
                    if peer.node_id in self.trusted_certificates:
                        self._learn_peer(peer)
                except (KeyError, TypeError, ValueError):
                    continue
            return {"status": "ok"}
        if kind == "find_node":
            target_id = str(message.get("target_id", self.node_id))
            peers = self.routing.closest(target_id, self.config.replication_factor)
            return {"status": "ok", "peers": [peer.as_dict() for peer in peers]}
        if kind == "task_submit":
            self._learn_sender(sender_id, message, remote_address)
            task_id = str(message.get("task_id", ""))
            if not task_id:
                return {"status": "error", "error": "Missing task id."}
            task_key = (sender_id, task_id)
            existing = self._remote_tasks.get(task_key)
            if existing is not None:
                self._remote_tasks.move_to_end(task_key)
                if existing.state in {"completed", "failed"}:
                    await self._deliver_remote_result(existing)
                return {"status": "accepted", "duplicate": True}
            origin_host = str(message.get("origin_host", ""))
            origin_port = int(message.get("origin_port", 0))
            if not origin_host or not 1 <= origin_port <= 65535:
                return {"status": "error", "error": "Invalid result callback address."}
            try:
                payload = base64.b64decode(str(message.get("payload", "")), validate=True)
            except ValueError as exc:
                return {"status": "error", "error": f"Invalid task payload: {exc}"}
            if len(payload) > self.config.max_message_bytes // 2:
                return {"status": "error", "error": "Task payload exceeds configured size limit."}
            record = RemoteTaskRecord(
                sender_id=sender_id,
                task_id=task_id,
                origin_host=origin_host,
                origin_port=origin_port,
            )
            self._remote_tasks[task_key] = record
            task = asyncio.create_task(
                self._execute_received(record, payload),
                name=f"task-{task_id}",
            )
            self._running[task_id] = task
            self._prune_remote_tasks()
            return {"status": "accepted"}
        if kind in {"task_result", "task_error"}:
            pending = self._pending.get(str(message.get("task_id", "")))
            if pending is None or pending.assigned_to != sender_id:
                return {"status": "ignored"}
            if kind == "task_error":
                if not pending.result.done():
                    self._record_completion(pending.task_id, "failed")
                    pending.result.set_exception(RuntimeError(str(message.get("error", "Remote task failed."))))
                return {"status": "received"}
            try:
                result_bytes = base64.b64decode(str(message.get("payload", "")), validate=True)
                if len(result_bytes) > self.config.max_message_bytes // 2:
                    raise ValueError("Result exceeds configured size limit.")
                result = cloudpickle.loads(result_bytes)
            except Exception as exc:
                if not pending.result.done():
                    self._record_completion(pending.task_id, "failed")
                    pending.result.set_exception(ValueError(f"Invalid signed task result: {exc}"))
            else:
                if not pending.result.done():
                    self._record_completion(pending.task_id, "completed")
                    pending.result.set_result(result)
            return {"status": "received"}
        return {"status": "error", "error": f"Unknown message type: {kind}"}

    async def _rpc(
        self, host: str, port: int, message: dict[str, Any]
    ) -> dict[str, Any]:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(
                host,
                port,
                ssl=self.client_tls,
                server_hostname="localhost",
                limit=self.config.max_message_bytes + 1,
            ),
            timeout=self.config.rpc_timeout,
        )
        try:
            writer.write(encode_envelope(self.identity, message))
            await writer.drain()
            response = await asyncio.wait_for(
                read_envelope(reader, self.config.max_message_bytes),
                timeout=self.config.rpc_timeout,
            )
            ssl_object = writer.get_extra_info("ssl_object")
            certificate_bytes = ssl_object.getpeercert(binary_form=True) if ssl_object else None
            if not certificate_bytes:
                raise ProtocolError("Remote peer did not present a TLS certificate.")
            certificate = x509.load_der_x509_certificate(certificate_bytes)
            fingerprint = certificate.fingerprint(hashes.SHA256()).hex()
            trusted = self.trusted_certificates.get(fingerprint)
            body = {"sender": response.get("sender"), "message": response.get("message")}
            if trusted is None or body["sender"] != fingerprint or not verify_signature(
                trusted, body, str(response.get("signature", ""))
            ):
                raise ProtocolError("Remote RPC response signature is invalid.")
            if not isinstance(body["message"], dict):
                raise ProtocolError("Remote RPC response is not a JSON object.")
            return body["message"]
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except OSError:
                pass

    async def _connect_bootstrap(self, host: str, port: int) -> None:
        response = await self._rpc(
            host,
            port,
            {
                "type": "ping",
                "host": self.config.public_host,
                "port": self.config.port,
                "cpu_percent": self.cpu_percent,
                "memory_percent": self.memory_percent,
                "tasks_running": len(self._running),
            },
        )
        peer = PeerInfo.from_dict(response["peer"])
        self._learn_peer(peer)
        await self._rpc(peer.host, peer.port, {"type": "ping", **self.own_info.as_dict()})

    def _learn_sender(self, sender_id: str, message: dict[str, Any], remote_address: Any) -> None:
        host = str(message.get("host") or (remote_address[0] if remote_address else ""))
        port = int(message.get("port", 0))
        if not host or not 1 <= port <= 65535:
            return
        self._learn_peer(
            PeerInfo(
                node_id=sender_id,
                host=host,
                port=port,
                cpu_percent=float(message.get("cpu_percent", 0.0)),
                memory_percent=float(message.get("memory_percent", 0.0)),
                tasks_running=int(message.get("tasks_running", 0)),
            )
        )

    def _learn_peer(self, peer: PeerInfo) -> None:
        if peer.node_id == self.node_id or peer.node_id not in self.trusted_certificates:
            return
        self.routing.add(peer)

    async def _start_discovery(self) -> None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind(("", self.config.udp_port))
            membership = socket.inet_aton(self.config.multicast_group) + socket.inet_aton(
                "0.0.0.0"
            )
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, membership)
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 1)
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)
            loop = asyncio.get_running_loop()
            transport, _ = await loop.create_datagram_endpoint(
                lambda: _DiscoveryProtocol(self), sock=sock
            )
            self._datagram_transport = transport
        except Exception:
            sock.close()
            raise

    async def _discovery_loop(self) -> None:
        while not self._closed:
            message = {
                "type": "discovery",
                **self.own_info.as_dict(),
                "host": self.config.public_host,
            }
            if self._datagram_transport is not None:
                self._datagram_transport.sendto(
                    encode_envelope(self.identity, message),
                    (self.config.multicast_group, self.config.udp_port),
                )
            await asyncio.sleep(self.config.gossip_interval)

    async def _handle_discovery(self, data: bytes, address: tuple[str, int]) -> None:
        try:
            envelope = json.loads(data)
            body = {"sender": envelope.get("sender"), "message": envelope.get("message")}
            sender_id = str(body["sender"])
            trusted = self.trusted_certificates.get(sender_id)
            if trusted is None or not verify_signature(
                trusted, body, str(envelope.get("signature", ""))
            ):
                return
            message = body["message"]
            if not isinstance(message, dict) or message.get("type") != "discovery":
                return
            self._learn_sender(sender_id, message, address)
        except (ValueError, TypeError, KeyError, json.JSONDecodeError):
            return

    async def _metrics_loop(self) -> None:
        while not self._closed:
            self.cpu_percent = psutil.cpu_percent(interval=None)
            self.memory_percent = psutil.virtual_memory().percent
            await asyncio.sleep(1)

    async def _gossip_loop(self) -> None:
        while not self._closed:
            await asyncio.sleep(self.config.gossip_interval)
            records = [self.own_info.as_dict(), *(peer.as_dict() for peer in self.peers)]
            for peer in list(self.peers):
                try:
                    await self._rpc(
                        peer.host, peer.port, {"type": "gossip", "peers": records}
                    )
                    peer.last_seen = time.monotonic()
                except (OSError, ssl.SSLError, ProtocolError, asyncio.TimeoutError):
                    logger.debug("Gossip to peer %s failed", peer.node_id[:12])

    async def _heartbeat_loop(self) -> None:
        while not self._closed:
            await asyncio.sleep(self.config.heartbeat_interval)
            now = time.monotonic()
            for peer in list(self.peers):
                try:
                    response = await self._rpc(
                        peer.host,
                        peer.port,
                        {
                            "type": "ping",
                            "host": self.config.public_host,
                            "port": self.config.port,
                            "cpu_percent": self.cpu_percent,
                            "memory_percent": self.memory_percent,
                            "tasks_running": len(self._running),
                        },
                    )
                    fresh = PeerInfo.from_dict(response["peer"])
                    fresh.last_seen = now
                    self._learn_peer(fresh)
                    peer.last_seen = now
                    peer.cpu_percent = fresh.cpu_percent
                    peer.memory_percent = fresh.memory_percent
                    peer.tasks_running = fresh.tasks_running
                except (OSError, ssl.SSLError, ProtocolError, asyncio.TimeoutError, KeyError, ValueError):
                    if now - peer.last_seen >= self.config.heartbeat_timeout:
                        self.routing.remove(peer.node_id)
                        await self._reassign_from(peer.node_id)
                        logger.warning("Peer %s is unavailable and was removed", peer.node_id[:12])
            expired = [
                peer
                for peer in self.peers
                if now - peer.last_seen > self.config.peer_ttl
            ]
            for peer in expired:
                self.routing.remove(peer.node_id)
                await self._reassign_from(peer.node_id)

    async def _reassign_from(self, node_id: str) -> None:
        for pending in self._pending.values():
            if pending.assigned_to == node_id and not pending.result.done():
                pending.reassigned.set()
