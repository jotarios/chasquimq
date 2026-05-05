"""MessagePack helpers for the high-level shim.

Wire format mirrors `chasquimq-node` exactly: the producer encodes only
the user `data` value (not a `(name, data)` tuple). The job ``name``
travels at the Redis Streams framing layer in the entry's ``n`` field,
not inside the msgpack payload — see ``Producer::add_with_options`` and
``ParsedEntry::name`` on the engine side.
"""

from __future__ import annotations

from typing import Any

import msgpack


def encode_payload(data: Any) -> bytes:
    return msgpack.packb(data, use_bin_type=True)


def decode_payload(buf: bytes) -> Any:
    return msgpack.unpackb(buf, raw=False)
