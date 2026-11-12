from __future__ import annotations

import asyncio
import base64
import uuid

import cloudpickle
from meshweaver.config import NodeConfig
from meshweaver.identity import Identity, write_certificate_bundle
from meshweaver.node import MeshWeaverNode


_EXECUTION_MARKERS: list[str] = []


def multiply(left: int, right: int) -> int:
    return left * right


async def delayed_value(delay: float, marker: str) -> str:
    _EXECUTION_MARKERS.append(marker)
    await asyncio.sleep(delay)
    return marker


def record_execution(marker: str) -> str:
    _EXECUTION_MARKERS.append(marker)
    return marker


async def test_signed_task_runs_on_bootstrapped_peer(tmp_path, monkeypatch):
    monkeypatch.setattr("meshweaver.node.psutil.cpu_percent", lambda **_: 100.0)
    worker_identity = Identity.create(tmp_path / "worker", "worker")
    client_identity = Identity.create(tmp_path / "client", "client")
    bundle = tmp_path / "trust.pem"
    write_certificate_bundle(
        [worker_identity.certificate_pem, client_identity.certificate_pem], bundle
    )
    worker = MeshWeaverNode(
        worker_identity,
        str(bundle),
        NodeConfig(host="127.0.0.1", port=0, discovery=False, task_timeout=5),
    )
    client = MeshWeaverNode(
        client_identity,
        str(bundle),
        NodeConfig(
            host="127.0.0.1",
            port=0,
            discovery=False,
            task_timeout=5,
            bootstrap=[],
        ),
    )
    await worker.start()
    client.config.bootstrap = [("127.0.0.1", worker.config.port)]
    try:
        await client.start()

        async def select_worker(attempted: set[str]):
            peer = client.routing.get(worker.node_id)
            if peer is None or peer.node_id in attempted:
                raise RuntimeError("The designated test worker is unavailable.")
            return peer

        monkeypatch.setattr(client, "_select_peer", select_worker)
        submission = asyncio.create_task(client.submit(multiply, 6, 7, timeout=5))
        for _ in range(100):
            if worker._running or worker._completed:
                break
            await asyncio.sleep(0.01)
        assert worker._running or worker._completed, (
            "The task was not dispatched to the remote worker."
        )
        assert await submission == 42
        assert client.peers
        assert any(status == "completed" for _, status in worker._completed)
    finally:
        await client.close()
        await worker.close()


async def test_task_is_reassigned_when_worker_disappears(tmp_path, monkeypatch):
    monkeypatch.setattr("meshweaver.node.psutil.cpu_percent", lambda **_: 100.0)
    worker_one_identity = Identity.create(tmp_path / "worker-one", "worker-one")
    worker_two_identity = Identity.create(tmp_path / "worker-two", "worker-two")
    client_identity = Identity.create(tmp_path / "client", "client")
    bundle = tmp_path / "trust.pem"
    write_certificate_bundle(
        [
            worker_one_identity.certificate_pem,
            worker_two_identity.certificate_pem,
            client_identity.certificate_pem,
        ],
        bundle,
    )
    worker_one = MeshWeaverNode(
        worker_one_identity,
        str(bundle),
        NodeConfig(host="127.0.0.1", port=0, discovery=False),
    )
    worker_two = MeshWeaverNode(
        worker_two_identity,
        str(bundle),
        NodeConfig(host="127.0.0.1", port=0, discovery=False),
    )
    client = MeshWeaverNode(
        client_identity,
        str(bundle),
        NodeConfig(
            host="127.0.0.1",
            port=0,
            discovery=False,
            heartbeat_interval=0.05,
            heartbeat_timeout=0.5,
            rpc_timeout=1,
            task_timeout=5,
            bootstrap=[],
        ),
    )
    await worker_one.start()
    await worker_two.start()
    client.config.bootstrap = [
        ("127.0.0.1", worker_one.config.port),
        ("127.0.0.1", worker_two.config.port),
    ]
    try:
        await client.start()
        client.cpu_percent = 100
        worker_one.cpu_percent = 0
        worker_two.cpu_percent = 50

        async def select_in_order(attempted: set[str]):
            for candidate in (worker_one, worker_two):
                peer = client.routing.get(candidate.node_id)
                if peer is not None and peer.node_id not in attempted:
                    return peer
            raise RuntimeError("No replacement test worker is available.")

        monkeypatch.setattr(client, "_select_peer", select_in_order)
        marker = uuid.uuid4().hex
        submission = asyncio.create_task(
            client.submit(delayed_value, 0.3, marker, timeout=5)
        )
        task_id = None
        for _ in range(300):
            if worker_one._running:
                task_id = next(iter(worker_one._running))
                break
            await asyncio.sleep(0.01)
        assert worker_one._running, "The designated first worker did not receive the task."
        assert task_id is not None
        for _ in range(100):
            if marker in _EXECUTION_MARKERS:
                break
            await asyncio.sleep(0.01)
        assert marker in _EXECUTION_MARKERS, "The assigned worker did not start executing the task."
        await worker_one.close()
        assert await submission == marker
        assert (task_id, "completed") in worker_two._completed
        assert _EXECUTION_MARKERS.count(marker) == 2
    finally:
        await client.close()
        await worker_one.close()
        await worker_two.close()


async def test_worker_records_completion_before_result_ack(tmp_path, monkeypatch):
    worker_identity = Identity.create(tmp_path / "worker", "worker")
    client_identity = Identity.create(tmp_path / "client", "client")
    bundle = tmp_path / "trust.pem"
    write_certificate_bundle(
        [worker_identity.certificate_pem, client_identity.certificate_pem], bundle
    )
    worker = MeshWeaverNode(
        worker_identity,
        str(bundle),
        NodeConfig(host="127.0.0.1", port=0, discovery=False),
    )
    client = MeshWeaverNode(
        client_identity,
        str(bundle),
        NodeConfig(host="127.0.0.1", port=0, discovery=False),
    )
    result_received = asyncio.Event()
    release_ack = asyncio.Event()
    dispatch = client._dispatch

    async def hold_result_ack(sender_id, message, remote_address):
        response = await dispatch(sender_id, message, remote_address)
        if message.get("type") == "task_result":
            result_received.set()
            await release_ack.wait()
        return response

    monkeypatch.setattr(client, "_dispatch", hold_result_ack)
    await worker.start()
    client.config.bootstrap = [("127.0.0.1", worker.config.port)]
    try:
        await client.start()

        async def select_worker(attempted: set[str]):
            peer = client.routing.get(worker.node_id)
            if peer is None or peer.node_id in attempted:
                raise RuntimeError("The designated test worker is unavailable.")
            return peer

        monkeypatch.setattr(client, "_select_peer", select_worker)
        submission = asyncio.create_task(client.submit(multiply, 3, 7, timeout=5))
        assert await submission == 21
        assert result_received.is_set()
        assert worker._completed
        task_id, status = worker._completed[-1]
        assert status == "completed"
        assert task_id not in worker._running
    finally:
        release_ack.set()
        await client.close()
        await worker.close()


async def test_duplicate_remote_submission_is_not_executed_twice(tmp_path):
    worker_identity = Identity.create(tmp_path / "worker", "worker")
    client_identity = Identity.create(tmp_path / "client", "client")
    bundle = tmp_path / "trust.pem"
    write_certificate_bundle(
        [worker_identity.certificate_pem, client_identity.certificate_pem], bundle
    )
    worker = MeshWeaverNode(
        worker_identity,
        str(bundle),
        NodeConfig(host="127.0.0.1", port=0, discovery=False),
    )
    client = MeshWeaverNode(
        client_identity,
        str(bundle),
        NodeConfig(host="127.0.0.1", port=0, discovery=False),
    )
    marker = uuid.uuid4().hex
    task_id = uuid.uuid4().hex
    payload = cloudpickle.dumps((record_execution, (marker,), {}))
    await worker.start()
    await client.start()
    try:
        request = {
            "type": "task_submit",
            "task_id": task_id,
            "payload": base64.b64encode(payload).decode("ascii"),
            "origin_host": client.config.public_host,
            "origin_port": client.config.port,
        }
        first = await client._rpc("127.0.0.1", worker.config.port, request)
        duplicate = await client._rpc("127.0.0.1", worker.config.port, request)
        assert first["status"] == "accepted"
        assert duplicate == {"status": "accepted", "duplicate": True}
        for _ in range(100):
            if (task_id, "completed") in worker._completed:
                break
            await asyncio.sleep(0.01)
        assert (task_id, "completed") in worker._completed

        replay = await client._rpc("127.0.0.1", worker.config.port, request)
        assert replay == {"status": "accepted", "duplicate": True}
        assert _EXECUTION_MARKERS.count(marker) == 1
        assert worker.task_status.count((task_id, "completed")) == 1
    finally:
        await client.close()
        await worker.close()

