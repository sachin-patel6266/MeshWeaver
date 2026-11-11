"""Rich terminal dashboard for a running MeshWeaver node."""

from __future__ import annotations

import asyncio

from rich import box
from rich.console import Group
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from meshweaver.node import MeshWeaverNode


def render_dashboard(node: MeshWeaverNode) -> Group:
    """Render a fresh snapshot of peers, resource use, and local tasks."""
    peer_table = Table(box=box.SIMPLE, expand=True)
    peer_table.add_column("Peer", style="cyan")
    peer_table.add_column("Address")
    peer_table.add_column("CPU", justify="right")
    peer_table.add_column("RAM", justify="right")
    peer_table.add_column("Tasks", justify="right")
    peer_table.add_row(
        f"{node.node_id[:12]} (local)",
        f"{node.config.public_host}:{node.config.port}",
        f"{node.cpu_percent:.1f}%",
        f"{node.memory_percent:.1f}%",
        str(len(node._running)),
    )
    for peer in sorted(node.peers, key=lambda item: item.node_id):
        peer_table.add_row(
            peer.node_id[:12],
            f"{peer.host}:{peer.port}",
            f"{peer.cpu_percent:.1f}%",
            f"{peer.memory_percent:.1f}%",
            str(peer.tasks_running),
        )

    task_table = Table(box=box.SIMPLE, expand=True)
    task_table.add_column("Task ID", style="magenta")
    task_table.add_column("Status")
    for task_id, status in node.task_status:
        task_table.add_row(task_id[:12], status)
    if not node.task_status:
        task_table.add_row("No tasks yet", "-")

    title = Text(
        f"MeshWeaver  |  node {node.node_id[:12]}  |  "
        f"TCP {node.config.public_host}:{node.config.port}",
        style="bold green",
    )
    return Group(
        Panel(title, border_style="green"),
        Panel(peer_table, title=f"Trusted peers ({len(node.peers) + 1})"),
        Panel(task_table, title="Task activity"),
    )


async def run_dashboard(node: MeshWeaverNode) -> None:
    """Keep the live Rich dashboard refreshed until the node is stopped."""
    with Live(render_dashboard(node), refresh_per_second=2, screen=True) as live:
        while True:
            await asyncio.sleep(0.5)
            live.update(render_dashboard(node))

