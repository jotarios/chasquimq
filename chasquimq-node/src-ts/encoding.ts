// MessagePack helpers shared by the Queue (produce) and Worker (consume)
// shims. The native binding takes/returns raw `Buffer` payloads — this
// module is the JS-side bridge that turns user data into the engine's
// expected wire format and back again.
//
// Implementation note: `@msgpack/msgpack` returns a `Uint8Array`. We wrap
// it with `Buffer.from(buf, byteOffset, byteLength)`, which is a zero-copy
// view onto the same backing memory — the native producer then performs
// exactly one copy at the FFI boundary into engine-managed `Bytes`.

import { encode, decode } from '@msgpack/msgpack'

/**
 * Encode arbitrary user data into a MessagePack `Buffer`.
 *
 * The returned Buffer is a view onto the bytes already produced by
 * `@msgpack/msgpack`; no extra copy is made on the JS side.
 */
export function encodePayload(data: unknown): Buffer {
  const u8 = encode(data)
  return Buffer.from(u8.buffer, u8.byteOffset, u8.byteLength)
}

/**
 * Decode a MessagePack `Buffer` (or `Uint8Array`) back into JS data.
 * Used by the Worker shim when handing raw native bytes to a processor.
 */
export function decodePayload(buf: Buffer | Uint8Array): unknown {
  return decode(buf)
}
