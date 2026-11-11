from __future__ import annotations

import pytest

from meshweaver.peer import KademliaRoutingTable, PeerInfo


def test_kademlia_orders_peers_by_xor_distance():
    local_id = f"{0:064x}"
    table = KademliaRoutingTable(local_id, bucket_size=8)
    peers = [
        PeerInfo(node_id=f"{node_id:064x}", host="127.0.0.1", port=8000 + node_id)
        for node_id in (1, 2, 4)
    ]
    for peer in peers:
        table.add(peer)

    assert [peer.node_id for peer in table.closest(f"{3:064x}", 2)] == [
        f"{2:064x}",
        f"{1:064x}",
    ]
    table.remove(peers[1].node_id)
    assert table.get(peers[1].node_id) is None


def test_kademlia_rejects_self():
    table = KademliaRoutingTable(f"{0:064x}")
    with pytest.raises(ValueError):
        table.add(PeerInfo(f"{0:064x}", "127.0.0.1", 8000))

