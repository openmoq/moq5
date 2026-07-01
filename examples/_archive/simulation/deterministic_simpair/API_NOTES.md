# API Friction Notes — Deterministic SimPair (Round 3)

## Resolved

- Handle extraction: `isub->subscription` gives typed handle directly.
- Handle validity: `moq_subscription_is_valid(pub_sub)` instead of `._id != 0`.
- Handle sentinel: `MOQ_SUBSCRIPTION_INVALID` for "no active subscription."
- Track alias: auto-assigned via `MOQ_SUBSCRIBE_OK_PARAMS_INIT`.
- Accept naming: `moq_session_subscribe_ok` matches wire vocabulary.
- Object access: `moq_object_event_t *o = &evts[i].u.object` — named struct.

## Remaining friction

1. **Manual publisher-side accept loop:** test must poll events on publisher
   to extract subscription handle. Verbose for simple tests. Future:
   `moq_simpair_set_auto_accept(sp, MOQ_SIM_B, true)` convenience.

2. **SimPair must NOT consume events during `run`:** critical invariant.
   Actions are routed; events belong to the caller.

3. **Trace enablement:** not shown. Real tests should enable tracing for
   replayable failure diagnostics.

## Pre-header design tasks

- Session identity in handles: SimPair creates two sessions — a handle
  from session A passed to session B must fail deterministically.
