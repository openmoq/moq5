# Setup Handshake

**Goal:** Complete client/server MoQ setup handshake using only the
public `moq_session_*` API and byte buffers. No QUIC stack, no sockets.

**What this demonstrates:**
- `moq_session_cfg_t` with `MOQ_PERSPECTIVE_CLIENT` / `MOQ_PERSPECTIVE_SERVER`
- `moq_session_create` / `moq_session_destroy`
- client-only `moq_session_start` initiating the setup handshake
- `moq_session_on_control_bytes` feeding wire bytes between sessions
- `moq_session_poll_actions` draining outbound I/O instructions
- `moq_session_poll_events` receiving SETUP_COMPLETE notification
- State transitions: IDLE -> SETUP_SENT -> ESTABLISHED

**This is the exact pattern SimPair will automate.**

**Status:** Uses real API. Not yet built as a CMake target.
