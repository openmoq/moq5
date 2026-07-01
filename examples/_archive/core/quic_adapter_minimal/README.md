# QUIC Adapter — Minimal

**Goal:** Show the complete polling loop that bridges libmoq's sans-I/O
core to a real QUIC stack. This is the most important example for adapter
authors.

**User story:** "I'm writing a picoquic adapter for libmoq. Show me the
exact loop shape so I know what to implement."

**API surface exercised:**
- `moq_session_on_stream_bytes` — feed received data (advancing)
- `moq_session_on_timer` — advance time (advancing)
- `moq_session_poll_actions` — drain outbound I/O instructions (observing)
- `moq_session_poll_events` — drain application notifications (observing)
- `moq_session_next_deadline_us` — when to wake up (observing)
- `moq_session_bind_stream` / `moq_session_reject_open` — two-phase stream binding
- `moq_stream_id_from_u64` — wrap QUIC stream ID into typed `moq_stream_id_t`
- All action kinds: SEND_CONTROL, OPEN_UNI/BIDI, SEND_STREAM, FIN, RESET, STOP_SENDING, DATAGRAM, CLOSE
- The full 5-phase loop: feed → timers → actions → events → sleep

**Status:** Aspirational sketch. Does not compile.
