from __future__ import annotations

import asyncio

from meshweaver.config import NodeConfig
from meshweaver.identity import Identity, write_certificate_bundle
from meshweaver.node import MeshWeaverNode


def multiply(left: int, right: int) -> int:
    return left * right


async def delayed_value(delay: float) -> str:
    await asyncio.sleep(delay)
    return "completed after reassignment"


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
        submission = asyncio.create_task(client.submit(multiply, 6, 7, timeout=5))
        for _ in range(100):
            if worker._running:
                break
            await asyncio.sleep(0.01)
        assert worker._running, "The task was not dispatched to the remote worker."
        assert await submission == 42
        assert client.peers
        for _ in range(100):
            if any(status == "completed" for _, status in worker._completed):
                break
            await asyncio.sleep(0.01)
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
        submission = asyncio.create_task(client.submit(delayed_value, 0.3, timeout=5))
        assigned_worker = None
        for _ in range(300):
            if worker_one._running:
                assigned_worker = worker_one
                break
            if worker_two._running:
                assigned_worker = worker_two
                break
            await asyncio.sleep(0.01)
        assert assigned_worker is not None, "No remote peer received the task."
        replacement = worker_two if assigned_worker is worker_one else worker_one
        await assigned_worker.close()
        assert await submission == "completed after reassignment"
        for _ in range(100):
            if any(status == "completed" for _, status in replacement._completed):
                break
            await asyncio.sleep(0.01)
        assert any(status == "completed" for _, status in replacement._completed)
    finally:
        await client.close()
        await worker_one.close()
        await worker_two.close()

