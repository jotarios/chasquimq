//! Batched ack flusher (Phase 6 — to be implemented).

// Stub. See plan: drains a bounded mpsc, batches IDs (max ack_batch or after
// ack_idle_ms), issues one pipelined `XACKDEL <key> <group> <ids…>`.
