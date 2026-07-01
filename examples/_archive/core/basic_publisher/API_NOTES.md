# API Friction Notes — Basic Publisher (Round 3)

## Resolved

- Handle in events: `isub->subscription` provides typed handle directly.
- Track alias: auto-assigned. No manual alias management.
- Namespace: `moq_bytes_t` array, byte-safe, no variadic macro.
- Constructor: `moq_config_create` with out-param.
- Naming: `moq_session_announce_namespace` (not `publish_namespace`).
- Accept: `moq_session_subscribe_ok` matching wire message name.
- Handle type: `moq_announcement_t` (not `moq_namespace_handle_t`).
- Handle comparison: `moq_subscription_is_valid(h)` instead of `h._id != 0`.
- Lifecycle: `goaway` / `close` / `destroy` documented as three distinct ops.

## Remaining friction

1. **Per-subscription vs per-track publishing:** `publish_object` takes
   `moq_subscription_t`. Correct for single-subscriber. A relay/publisher
   with many subscribers calls once per subscriber. Fan-out helper deferred.

2. **Group lifecycle:** manual `group_id`/`object_id` still present. Future
   convenience layer: `moq_session_group_begin` / `group_push` / `group_end`
   with auto-incrementing object IDs. Raw IDs stay for relays and tools.

3. **`active_sub` stash pattern:** publisher stores handle from event for
   later use. Clean with typed handles, but still caller-managed state.

## Pre-header design tasks

- **Session identity in handles:** 64-bit handle layout should include
  session tag/salt bits to catch cross-session handle misuse deterministically,
  not probabilistically through slot/generation mismatch. Critical for relays
  holding upstream and downstream handles of the same type.
