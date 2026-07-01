# API Friction Notes — QUIC Adapter Minimal (Round 3)

## Resolved

- Control stream: `SEND_CONTROL` actions carry `stream_id`. No special-casing.
- Stream open failure: `reject_open` path shown.
- Lifetime categories: documented in header comment.
- Advancing vs observing: labeled on each loop phase.

## Remaining friction

1. **`moq_stream_id_from_u64` visual noise:** every adapter writes this
   wrapper dozens of times. Type safety prevents `moq_stream_ref_t` /
   `moq_stream_id_t` confusion, which is a real bug class. Keep the types.

2. **`bind_stream` is advancing inside action drain:** when the adapter
   calls `bind_stream` from within the action dispatch, this is an
   advancing call. Action data pointers from earlier in the batch are
   consumed before the bind, so this is safe in practice but subtle.

3. **Missing `on_stream_opened`:** the example only shows the readable
   stream path. Peer-initiated streams need `moq_session_on_stream_opened`.
   Some QUIC stacks (msquic) distinguish "new stream" from "data on
   existing stream."

4. **Batch size:** `acts[64]` is arbitrary. Consider `MOQ_ACTIONS_BATCH_HINT`.

## Pre-header design tasks

- Session identity in handles ensures cross-session handle misuse fails
  deterministically, which matters when adapters manage multiple sessions.
