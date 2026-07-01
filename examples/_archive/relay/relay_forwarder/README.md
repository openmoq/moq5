# Relay Forwarder

**Goal:** Minimal relay forwarding objects between an upstream publisher
and a downstream subscriber. Two sessions, handle mapping, namespace
routing, extension preservation.

**User story:** "I'm building a CDN relay. When a subscriber connects and
requests a track, I subscribe upstream, then forward objects as they
arrive. I must preserve unknown extension headers."

**Why this matters:** relay code exercises the hardest API surfaces. If it
reads poorly, the core abstraction is wrong.

**API surface exercised:**
- Two sessions with different roles in the same process
- `moq_incoming_subscribe_event_t` with typed `subscription` handle
- `moq_session_subscribe` on upstream (namespace borrowed from downstream event)
- `moq_session_subscribe_ok` / `moq_session_subscribe_error` — wire-aligned naming
- `moq_session_publish_object` with forwarded extensions
- `moq_session_publish_done` with forwarded status
- `moq_subscription_eq` for handle-based relay track lookup
- Cross-session lifetime: advancing on session A doesn't invalidate borrows from B
- Per-event-kind named structs for type-safe access
- Three-way lifecycle: `goaway` / `close` / `destroy`

**Status:** Aspirational sketch. Does not compile.
