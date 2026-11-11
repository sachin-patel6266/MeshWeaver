"""Length-bounded newline-delimited signed JSON RPC framing."""

from __future__ import annotations

import json
from typing import Any

from meshweaver.identity import Identity


class ProtocolError(ValueError):
    """Raised when an RPC frame is malformed or exceeds configured limits."""


def signed_envelope(identity: Identity, message: dict[str, Any]) -> dict[str, Any]:
    """Wrap a message with the sender id and a signature over the full body."""
    body = {"sender": identity.node_id, "message": message}
    return {**body, "signature": identity.sign(body)}


def encode_envelope(identity: Identity, message: dict[str, Any]) -> bytes:
    encoded = json.dumps(
        signed_envelope(identity, message), separators=(",", ":"), ensure_ascii=True
    ).encode("utf-8")
    return encoded + b"\n"


async def read_envelope(reader: Any, max_bytes: int) -> dict[str, Any]:
    """Read one bounded line-delimited JSON envelope."""
    try:
        line = await reader.readline()
    except (ValueError, asyncio_limit_error()) as exc:
        raise ProtocolError("RPC frame exceeds the configured size limit.") from exc
    if not line:
        raise ProtocolError("Connection closed before an RPC message was received.")
    if len(line) > max_bytes:
        raise ProtocolError("RPC frame exceeds the configured size limit.")
    try:
        value = json.loads(line)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ProtocolError("RPC frame is not valid UTF-8 JSON.") from exc
    if not isinstance(value, dict):
        raise ProtocolError("RPC envelope must be a JSON object.")
    return value


def asyncio_limit_error() -> type[Exception]:
    """Return StreamReader's line-limit exception without importing internals."""
    import asyncio

    return asyncio.LimitOverrunError
