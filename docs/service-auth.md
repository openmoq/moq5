# Resource credentials

Media senders and receivers can attach opaque credentials to resource requests
without using the session or transport APIs. Include `<moq/media_auth.h>` and
link `moq::service` (or `libmoq-service.pc`). The library does not issue tokens,
parse CAT claims, or verify application credentials.

## Endpoint and resource credentials

Endpoint SETUP credentials authenticate the connection. They do not implicitly
authenticate publication or subscription requests. Configure resource credentials
separately with `moq_media_sender_cfg_t.resource_auth` or
`moq_media_receiver_cfg_t.resource_auth`.

Initialize sender and receiver configurations with their `_cfg_init_sized`
functions and `sizeof(cfg)` before setting this appended field. Their legacy
pointer-only initializers stamp only the frozen prefix; setting a later field
does not enable it. `resource_auth == NULL` keeps anonymous resource requests.

## Credential sources

Initialize `moq_media_auth_cfg_t` with `moq_media_auth_cfg_init(&auth)` or
`moq_media_auth_cfg_init_sized(&auth, sizeof auth)`. Set `on_outcome`, and choose
exactly one source:

- Static credentials: set a nonempty `tokens` array and `token_count`; leave
  `select` NULL. Every selected resource operation uses those credentials.
- Per-resource selection: set `select`; leave `tokens` NULL and `token_count`
  zero. The callback receives the action and full namespace/track identity,
  including catalog and generated tracks. Namespace publication has no track
  name.

A configured source cannot silently fall back to anonymous access. Selection
starts at DENY with zero tokens. `MOQ_OK` plus ALLOW requires 1 to 16 unique
token type/value pairs. Values are opaque binary, may be empty, and are limited
to 16 KiB each and 32 KiB in total. Complete-message limits can be smaller.
Draft-16 also counts ordinary request options against its internal parameter
budget; a valid selection can therefore receive TOKEN_LIMIT with BUFFER.
Draft-16 token types fit 62 bits; draft-18 and draft-22 types fit `uint64_t`.
A configured source on an unsupported profile reports `UNSUPPORTED_PROFILE`.
Tokens use direct values, not cross-stream aliases. Draft-22's combined
SUBSCRIBE/fill is one SUBSCRIBE selection; it does not select FETCH credentials.

`MOQ_OK` plus DENY refuses the operation. Selector `MOQ_ERR_WOULD_BLOCK` is an
invalid selection, not an asynchronous refresh request. Obtain or refresh
credentials outside the callback; this interface performs no issuer I/O.

## Ownership and threads

Construction copies the configuration and static token descriptors/bytes before
connecting or attaching the service. Invalid input or allocation failure returns
synchronously, attaches no owner, and invokes no outcome callback. Callback code
and context must remain valid until the sender or receiver is destroyed.

Selected bytes remain application-owned and must survive until the next
selection for that owner, or owner destruction; never return callback-stack
storage. The service copies them before the next selection or pump return and
retains that selection across transient request-admission retries. Canceling
or replacing an unsent intent releases its retained selection.

Both callbacks execute on the endpoint network thread, with its mutex held and
outside the service-private mutex. They must not block, perform network I/O,
reenter endpoint/service/facade APIs (including queries), or throw C++ exceptions
across C. Record observations in application-owned storage and act later from
the application thread. Callback request/outcome records are borrowed only for
the callback; copy any identity bytes needed afterward. Never log token values.

## Failure observations

`on_outcome` reports a terminal local credential failure exactly once, after its
state is latched. Switch on `outcome`, not merely `result`: POLICY_DENIED carries
`MOQ_OK`, which does not mean admission succeeded. The callback is not a success
notification, peer rejection, or UPDATE acknowledgment.

Failure of required catalog or namespace work ends that media service, not a
shared endpoint. Optional media resources fail locally; sender writes then
return WRONG_STATE without taking ownership. A receiver's failed track generation
stays FAILED and cannot be resubscribed; recovery requires a new catalog
generation or recreating the receiver. Joining FETCH credential failure remains
request-local and leaves the live subscription available.

Successful local submission still does not prove peer acceptance. Continue to
observe ordinary readiness, track state, peer terminal events, and media delivery.
Exact outcome/result mappings and callback contracts are in `media_auth.h`.
