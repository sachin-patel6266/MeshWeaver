"""Command-line entry point for identities, nodes, and task submission."""

from __future__ import annotations

import argparse
import asyncio
import importlib
import json
import logging
import sys
from pathlib import Path
from typing import Any

from rich.console import Console
from rich.panel import Panel

from meshweaver import __version__
from meshweaver.config import NodeConfig
from meshweaver.dashboard import run_dashboard
from meshweaver.identity import Identity, write_certificate_bundle
from meshweaver.node import MeshWeaverNode

console = Console()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="meshweaver",
        description="A zero-central-server asynchronous P2P task broker.",
    )
    parser.add_argument("--version", action="version", version=f"MeshWeaver {__version__}")
    commands = parser.add_subparsers(dest="command", required=True)

    init = commands.add_parser("init", help="Create a new Ed25519 node identity.")
    init.add_argument("--identity-dir", required=True)
    init.add_argument("--name", default="meshweaver-node")

    bundle = commands.add_parser("trust-bundle", help="Create a PEM bundle from trusted peer certificates.")
    bundle.add_argument("--output", required=True)
    bundle.add_argument("--certs", nargs="+", required=True)

    node = commands.add_parser("node", help="Start a MeshWeaver node.")
    _add_identity_options(node)
    node.add_argument("--host", default="127.0.0.1")
    node.add_argument("--advertise-host")
    node.add_argument("--port", type=int, default=8765)
    node.add_argument("--udp-port", type=int, default=8766)
    node.add_argument("--bootstrap", action="append", default=[], metavar="HOST:PORT")
    node.add_argument("--no-discovery", action="store_true")
    node.add_argument("--no-dashboard", action="store_true")
    node.add_argument("--heartbeat-timeout", type=float, default=8.0)
    node.add_argument("--task-timeout", type=float, default=120.0)

    submit = commands.add_parser("submit", help="Submit a Python callable to a trusted peer.")
    _add_identity_options(submit)
    submit.add_argument("--bootstrap", action="append", required=True, metavar="HOST:PORT")
    submit.add_argument("--function", required=True, help="Callable as python_module:function_name")
    submit.add_argument("--args", default="[]", help="JSON array of positional arguments.")
    submit.add_argument("--kwargs", default="{}", help="JSON object of keyword arguments.")
    submit.add_argument("--timeout", type=float, default=120.0)

    return parser


def _add_identity_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--identity-dir", required=True)
    parser.add_argument("--trusted-certs", required=True)


def _load_callable(reference: str) -> Any:
    try:
        module_name, function_name = reference.split(":", maxsplit=1)
    except ValueError as exc:
        raise ValueError("Function must be specified as python_module:function_name.") from exc
    module = importlib.import_module(module_name)
    function = getattr(module, function_name)
    if not callable(function):
        raise TypeError(f"{reference} does not refer to a callable.")
    return function


def _parse_bootstrap(values: list[str]) -> list[tuple[str, int]]:
    result: list[tuple[str, int]] = []
    for value in values:
        host, separator, raw_port = value.rpartition(":")
        if not separator or not host:
            raise ValueError(f"Invalid bootstrap address {value!r}; expected HOST:PORT.")
        port = int(raw_port)
        if not 1 <= port <= 65535:
            raise ValueError(f"Invalid bootstrap port in {value!r}.")
        result.append((host.strip("[]"), port))
    return result


async def _run_node(args: argparse.Namespace) -> None:
    identity = Identity.load(args.identity_dir)
    config = NodeConfig(
        host=args.host,
        port=args.port,
        advertise_host=args.advertise_host,
        udp_port=args.udp_port,
        discovery=not args.no_discovery,
        heartbeat_timeout=args.heartbeat_timeout,
        task_timeout=args.task_timeout,
        bootstrap=_parse_bootstrap(args.bootstrap),
    )
    node = MeshWeaverNode(identity, args.trusted_certs, config)
    await node.start()
    console.print(
        Panel(
            f"Node [cyan]{node.node_id[:16]}[/cyan] listening on "
            f"[green]{config.public_host}:{config.port}[/green]",
            title="MeshWeaver",
        )
    )
    try:
        if args.no_dashboard:
            await asyncio.Event().wait()
        else:
            await run_dashboard(node)
    finally:
        await node.close()


async def _run_submit(args: argparse.Namespace) -> None:
    identity = Identity.load(args.identity_dir)
    function = _load_callable(args.function)
    positional = json.loads(args.args)
    keyword = json.loads(args.kwargs)
    if not isinstance(positional, list) or not isinstance(keyword, dict):
        raise ValueError("--args must be a JSON array and --kwargs a JSON object.")
    bootstrap = _parse_bootstrap(args.bootstrap)
    config = NodeConfig(host="127.0.0.1", port=0, discovery=False, bootstrap=bootstrap)
    node = MeshWeaverNode(identity, args.trusted_certs, config)
    await node.start()
    try:
        result = await node.submit(function, *positional, timeout=args.timeout, **keyword)
        console.print_json(json.dumps({"result": result}, default=repr))
    finally:
        await node.close()


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        if args.command == "init":
            identity = Identity.create(args.identity_dir, args.name)
            console.print(
                f"Created identity [cyan]{identity.node_id}[/cyan] in {Path(args.identity_dir)}"
            )
        elif args.command == "trust-bundle":
            certificates = [Path(path).read_bytes() for path in args.certs]
            write_certificate_bundle(certificates, args.output)
            console.print(f"Created trust bundle with {len(certificates)} certificates: {args.output}")
        elif args.command == "node":
            asyncio.run(_run_node(args))
        elif args.command == "submit":
            asyncio.run(_run_submit(args))
    except KeyboardInterrupt:
        console.print("\nMeshWeaver stopped.")
    except (OSError, ValueError, TypeError, RuntimeError, ImportError, AttributeError) as exc:
        logging.getLogger("meshweaver").error("%s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

