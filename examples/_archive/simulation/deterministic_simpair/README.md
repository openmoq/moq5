# Deterministic SimPair

**Goal:** Two sessions wired back-to-back. Publisher sends an object,
subscriber receives it. No sockets, no threads, fully deterministic.

**User story:** "I'm adding a new protocol feature. I want to write a
test that verifies it works end-to-end in under a millisecond, with
a seed I can replay on failure."

**API surface exercised:**
- `moq_simpair_new` / `moq_simpair_destroy` with config struct
- `moq_simpair_session_a` / `moq_simpair_session_b`
- `moq_simpair_run` — drive until quiescent
- `moq_simpair_now_us` — read virtual time
- `moq_session_state` — check session state
- `moq_session_subscribe_ok` — accept with auto track alias
- `moq_subscription_is_valid` — handle validity check
- `MOQ_SUBSCRIPTION_INVALID` — sentinel handle
- Per-event-kind structs: `moq_incoming_subscribe_event_t`, `moq_object_event_t`
- All subscribe/publish/event APIs in simulation context

**Status:** Aspirational sketch. Does not compile.
