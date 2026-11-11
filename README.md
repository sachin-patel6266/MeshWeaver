# MeshWeaver

MeshWeaver is a decentralized, asyncio-based task broker. Nodes discover one
another over UDP multicast, exchange CPU/RAM and routing information with signed
gossip, and use authenticated TLS/TCP for Kademlia-style peer routing and task
execution. There is no central broker or external queue.

## Features

- Async TCP RPC protected by mutual TLS 1.2+ and signed Ed25519 envelopes.
- Signed UDP multicast discovery and five-second resource/peer gossip.
- A 256-bucket Kademlia XOR-distance routing table and iterative `find_node`.
- CPU-aware placement; unavailable workers cause pending tasks to be reassigned.
- Cloudpickle task and result transport for trusted peers.
- A Rich live dashboard for peer resources and task states.
- A local ten-worker demonstration and automated unit/integration tests.

## Security model

Each node has a unique Ed25519 key and self-signed TLS certificate. A node will
only establish TLS with certificates in its explicit PEM trust bundle; it also
checks each message signature against the certificate authenticated by TLS.
Task payloads are deserialized only after these checks pass. **Trusted peers'
functions run with the receiving operating system user's permissions.**
Cloudpickle is not a sandbox: only enroll peers whose code you trust, and use
network/firewall controls appropriate to your environment. Use a separate,
restricted OS account or container when tasks need isolation.

The project provides authentication and encryption, not a certificate
authority, account system, or admission service. Exchange certificates out of
band and distribute a trust bundle to every member. A trust bundle grants each
listed identity permission to submit executable tasks.

## Install

Python 3.10 or later is required. From the project directory:

```powershell
py -3 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m pip install -e .
```

In VS Code, select `.venv` with **Python: Select Interpreter**. On a
PowerShell-restricted machine, activate with
`.\.venv\Scripts\python.exe` rather than changing the execution policy.

## Start a trusted cluster

Create one identity for each participant, and distribute the resulting
`identity.crt` files over a trusted channel:

```powershell
python -m meshweaver init --identity-dir .\nodes\node1 --name node1
python -m meshweaver init --identity-dir .\nodes\node2 --name node2
python -m meshweaver init --identity-dir .\nodes\client --name client
```

Build a trust bundle containing every cluster member and every submitting
client certificate. Create it once and copy the same bundle to every node:

```powershell
python -m meshweaver trust-bundle --output .\cluster-trust.pem --certs `
  .\nodes\node1\identity.crt .\nodes\node2\identity.crt .\nodes\client\identity.crt
```

Start each node in a separate terminal. The bootstrap node has no bootstrap
address; other nodes can use any known peer:

```powershell
python -m meshweaver node --identity-dir .\nodes\node1 --trusted-certs .\cluster-trust.pem --port 8765
python -m meshweaver node --identity-dir .\nodes\node2 --trusted-certs .\cluster-trust.pem --port 8766 --bootstrap 127.0.0.1:8765
```

By default, the node displays the Rich dashboard and listens for multicast
discovery on UDP port 8766. On separate machines, set `--host 0.0.0.0` and
`--advertise-host <reachable-IP>`; allow the configured TCP port and multicast
UDP port through the local firewall. If multicast is unavailable, use
`--no-discovery` and provide `--bootstrap HOST:PORT` on every joining node.
Each process must use a unique TCP listening port on the same machine.

## Submit a task

The submitter needs its own trusted identity and a callable importable on the
worker. The cluster must trust the submitter's certificate:

```powershell
python -m meshweaver submit --identity-dir .\nodes\client --trusted-certs .\cluster-trust.pem `
  --bootstrap 127.0.0.1:8765 --function meshweaver.examples:count_primes `
  --args "[50000]" --timeout 60
```

Application callables use the form `package.module:function`. Arguments and
keyword arguments are JSON (`--args "[1,2]"`, `--kwargs "{""limit"":100}"`).
Python callers may instead use `await node.submit(function, *args, **kwargs)`.
Task delivery is at-least-once if a worker fails during execution; tasks with
external side effects should be idempotent.

## Ten-node local demonstration

```powershell
python -m pip install -r requirements.txt
python .\scripts\run_demo.py
```

The script creates ten independent worker identities plus a client, starts ten
local TCP nodes, waits for peer/gossip convergence, submits a prime-counting
task, prints its result, and stops the nodes. Its temporary keys and trust
bundle are removed when it exits. Use the normal `node` command to view the
interactive dashboard.

## Run tests

```powershell
python -m pytest
```

The integration test creates two independent TLS identities and runs an actual
signed task submission between local asyncio nodes. No broker service is
required.

