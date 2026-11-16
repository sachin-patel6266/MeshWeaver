# MeshWeaver

MeshWeaver is a decentralized, asyncio-based task broker. Nodes discover one another over UDP multicast, exchange CPU/RAM and routing information through signed gossip, and use authenticated TLS/TCP for Kademlia-style peer routing and task execution. It operates without a central broker or external message queue.

## Features

- Asynchronous TCP RPC protected by mutual TLS 1.2+ and signed Ed25519 envelopes.
- Signed UDP multicast discovery and five-second resource and peer gossip.
- A 256-bucket Kademlia XOR-distance routing table and iterative `find_node`.
- CPU-aware task placement with reassignment when workers become unavailable.
- Cloudpickle-based task and result transport for trusted peers.
- A Rich live dashboard for peer resources and task states.
- A local ten-worker demonstration.
- Automated unit and integration tests.

## Security Model

Each node has a unique Ed25519 key and self-signed TLS certificate. A node establishes TLS connections only with certificates included in its explicit PEM trust bundle. It also verifies each message signature against the certificate authenticated by TLS.

Task payloads are deserialized only after these checks pass.

**Trusted peers' functions run with the receiving operating system user's permissions.**

Cloudpickle is not a sandbox. Only enroll peers whose code you trust, and use appropriate network and firewall controls. Use a separate restricted operating system account or container when tasks require isolation.

The project provides authentication and encryption, not a certificate authority, account system, or admission service. Exchange certificates through a trusted channel and distribute a trust bundle to every cluster member. A trust bundle grants each listed identity permission to submit executable tasks.

## Installation

Python 3.10 or later is required.

From the project directory, run:

```powershell
py -3 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m pip install -e .
```

In VS Code, select `.venv` using **Python: Select Interpreter**.

If PowerShell activation is restricted, use:

```powershell
.\.venv\Scripts\python.exe
```

instead of changing the execution policy.

## Starting a Trusted Cluster

Create one identity for each participant and distribute the resulting `identity.crt` files through a trusted channel.

```powershell
python -m meshweaver init --identity-dir .\nodes\node1 --name node1
python -m meshweaver init --identity-dir .\nodes\node2 --name node2
python -m meshweaver init --identity-dir .\nodes\client --name client
```

Build a trust bundle containing every cluster member's certificate and every submitting client's certificate. Create it once and copy the same bundle to every node.

```powershell
python -m meshweaver trust-bundle --output .\cluster-trust.pem --certs `
  .\nodes\node1\identity.crt .\nodes\node2\identity.crt .\nodes\client\identity.crt
```

Start each node in a separate terminal. The bootstrap node does not need a bootstrap address. Other nodes can connect through any known peer.

```powershell
python -m meshweaver node --identity-dir .\nodes\node1 --trusted-certs .\cluster-trust.pem --port 8765

python -m meshweaver node --identity-dir .\nodes\node2 --trusted-certs .\cluster-trust.pem --port 8766 --bootstrap 127.0.0.1:8765
```

By default, the node displays the Rich dashboard and listens for multicast discovery on UDP port 8766.

On separate machines:

- Set `--host 0.0.0.0`.
- Set `--advertise-host <reachable-IP>`.
- Allow the configured TCP port and multicast UDP port through the local firewall.

If multicast is unavailable, use `--no-discovery` and provide `--bootstrap HOST:PORT` on every joining node.

Each process must use a unique TCP listening port when running on the same machine.

## Submitting a Task

The submitter needs its own trusted identity and a callable that can be imported on the worker. The cluster must trust the submitter's certificate.

```powershell
python -m meshweaver submit --identity-dir .\nodes\client --trusted-certs .\cluster-trust.pem `
  --bootstrap 127.0.0.1:8765 --function meshweaver.examples:count_primes `
  --args "[50000]" --timeout 60
```

Application callables use the format:

`package.module:function`

Arguments and keyword arguments are supplied as JSON.

Examples:

```powershell
--args "[1,2]"
--kwargs "{""limit"":100}"
```

Python callers may instead use:

```python
await node.submit(function, *args, **kwargs)
```

Task delivery is at-least-once if a worker fails during execution. Tasks with external side effects should therefore be idempotent.

A worker suppresses recent duplicate task IDs from the same authenticated sender and can replay a cached result. However, this in-memory deduplication is bounded and does not prevent duplicate execution across different workers after a failure or after a worker restarts.

## Ten-Node Local Demonstration

Install the requirements and run the demonstration:

```powershell
python -m pip install -r requirements.txt
python .\scripts\run_demo.py
```

The script creates ten independent worker identities plus a client, starts ten local TCP nodes, waits for peer and gossip convergence, submits a prime-counting task, prints its result, and stops the nodes.

Temporary keys and the trust bundle are removed when the script exits.

Use the normal `node` command to view the interactive dashboard.

## Running Tests

Run the automated test suite:

```powershell
python -m pytest
```

The integration tests create independent TLS identities and execute signed task submissions between local asyncio nodes. No external broker service is required.

## Verified Demo Results

The ten-node local demonstration was executed successfully on the development machine.

| Parameter | Result |
|---|---|
| Worker nodes | 10 |
| Task | Prime number counting |
| Input limit | 50,000 |
| Prime numbers found | 5,133 |
| Demonstration status | Successfully completed |

### Demo Output

```json
{
  "result": {
    "limit": 50000,
    "count": 5133
  }
}
```

### Verification Summary

- Ten-node demonstration completed successfully.
- Prime-counting task returned the expected result.
- Automated test suite: 9 tests passed in the reported local run.

*Note: Test and demonstration results describe the executions performed during development. Run the commands above to verify the current version of the project.*