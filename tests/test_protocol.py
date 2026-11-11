from __future__ import annotations

import asyncio
import json

import pytest

from meshweaver.identity import Identity, verify_signature
from meshweaver.protocol import ProtocolError, encode_envelope, read_envelope


@pytest.mark.asyncio
async def test_signed_envelope_round_trip(tmp_path):
    identity = Identity.create(tmp_path / "node", "node")
    wire = encode_envelope(identity, {"type": "ping"})
    reader = asyncio.StreamReader()
    reader.feed_data(wire)
    reader.feed_eof()

    envelope = await read_envelope(reader, 1024)
    body = {"sender": envelope["sender"], "message": envelope["message"]}
    assert verify_signature(identity.certificate, body, envelope["signature"])
    assert json.loads(wire)["message"]["type"] == "ping"


@pytest.mark.asyncio
async def test_rejects_oversized_frame():
    reader = asyncio.StreamReader()
    reader.feed_data(b'{"value":"too long"}\n')
    reader.feed_eof()
    with pytest.raises(ProtocolError):
        await read_envelope(reader, 8)

