"""MessagePack helpers for the high-level shim.

Wire format mirrors `chasquimq-node` exactly: the producer encodes only
the user `data` value (not a `(name, data)` tuple). The job ``name``
field is not carried on the wire today; the worker shim hands ``''`` to
the user handler as ``Job.name`` until a future engine slice persists
the name in the encoded ``Job<T>`` envelope.
"""

from __future__ import annotations

from typing import Any

import msgpack


def encode_payload(data: Any) -> bytes:
    return msgpack.packb(data, use_bin_type=True)


def decode_payload(buf: bytes) -> Any:
    return msgpack.unpackb(buf, raw=False)
