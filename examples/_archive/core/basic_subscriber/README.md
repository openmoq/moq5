# Basic Subscriber

**Goal:** Subscribe to a single track, receive objects, print them, shut down
when the track ends.

**User story:** "I'm building a chat client. I want to subscribe to a room's
message track and display each message as it arrives."

**API surface exercised:**
- `moq_config_create` / `moq_config_set_role` / `moq_config_destroy`
- `moq_session_create` / `moq_session_destroy`
- `moq_session_subscribe` with namespace (byte-span tuple) + track name + filter
- `moq_session_poll_events` loop
- Per-event-kind named structs: `moq_object_event_t`, `moq_subscription_done_event_t`
- Event dispatch: SETUP_COMPLETE, SUBSCRIPTION_ESTABLISHED, OBJECT, SUBSCRIPTION_DONE, SESSION_CLOSED
- Object payload borrowing lifetime (advancing vs observing calls)

**Status:** Aspirational sketch. Does not compile.
