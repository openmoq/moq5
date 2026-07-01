# API Friction Notes — Relay Forwarder (Round 3)

## Resolved

- Handle in events: `isub->subscription`, `est->subscription`, `obj->subscription`, `done_ev->subscription`, `err->subscription` — all typed.
- Handle comparison: `moq_subscription_eq(a, b)` — no `._id` access anywhere.
- Handle sentinel: `MOQ_SUBSCRIPTION_INVALID` (not used in relay but available).
- Track alias: auto-assigned. Relay doesn't manage aliases.
- Namespace: byte-span tuple from event, passed directly to upstream subscribe.
- Naming: `moq_session_announce_namespace`, `subscribe_ok`, `subscribe_error`.
- Per-event structs: `moq_incoming_subscribe_event_t`, `moq_subscription_established_event_t`, `moq_object_event_t`, `moq_subscription_done_event_t`, `moq_subscription_error_event_t`.
- Lifecycle: `goaway`/`close`/`destroy` documented as three ops.

## Remaining friction

1. **Handle mapping table:** relay maintains `relay_track_t` linking upstream
   and downstream handles. Correct but boilerplate. Optional relay helper
   module could reduce this.

2. **Cross-session namespace borrow:** relay passes namespace from downstream
   event directly to upstream subscribe. Safe because advancing on upstream
   doesn't invalidate downstream borrows. Subtle — must document prominently.

3. **Fan-out not shown:** real relay has many downstream subscribers per
   upstream track. `publish_object` called per subscriber. Batch helper
   deferred.

4. **GOAWAY / migration not shown:** separate example needed.

5. **Namespace announcement forwarding not shown:** upstream PUBLISH_NAMESPACE
   → downstream. Separate flow.

## Pre-header design tasks

- **Session identity in handles:** relays hold upstream and downstream handles
  of the same C type (`moq_subscription_t`). Passing upstream handle to
  downstream session must fail deterministically. Session tag/salt bits in
  the 64-bit handle layout, or explicit cross-session validation in every
  handle-taking function.

## Design confidence

The relay sketch reads well after Round 3. The typed-handle-in-event and
`moq_subscription_eq` changes eliminated all `._id` access. The
`subscribe_ok`/`subscribe_error` naming aligns with wire vocabulary and
makes the relay's accept/reject path read naturally. Cross-session lifetime
model is correct and documented. Remaining friction is about missing features
(fan-out, migration), not awkward API shape.
