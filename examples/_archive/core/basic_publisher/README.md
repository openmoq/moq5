# Basic Publisher

**Goal:** Publish a track, wait for a subscriber, send objects in a loop.

**User story:** "I'm building a chat server. I want to publish a message
track for a room and send each message as an MoQ object."

**API surface exercised:**
- `moq_session_announce_namespace` — advertise namespace availability
- `moq_announcement_t` — handle for the namespace announcement
- `MOQ_EVENT_INCOMING_SUBSCRIBE` with `moq_incoming_subscribe_event_t`
- `moq_session_subscribe_ok` — accept subscription (track alias auto-assigned)
- `moq_session_publish_object` — send object data (copies payload)
- `moq_subscription_is_valid` — check handle validity without touching internals
- `MOQ_SUBSCRIPTION_INVALID` — sentinel for "no active subscription"
- Object params struct with group/object/priority/payload
- Three-way lifecycle: `goaway` / `close` / `destroy`

**Status:** Aspirational sketch. Does not compile.
