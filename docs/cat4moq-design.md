# CAT4MoQ support in libmoq/moq5

Status: proposed, 2026-09-20. No implementation changes accompany this document.
Branch: `feature/cat4moq`, created from fetched `origin/main` at `c2900aa`.

## Objective and boundaries

Support C4M-01 credential carriage in libmoq and explicitly selected compatibility
with the current moqx and Red5 relay profiles. First unblock authenticated MoQXR
publishing through the managed libmoq backend. Then add receiver parity and an
optional production verifier for moq5-relay. These are separately testable
milestones; transporting a token does not verify it or establish complete C4M
conformance.

The core remains sans-I/O and has no CBOR/COSE/crypto dependency. Existing no-auth
callers retain their behavior. CMAF remains MoQXR's default media packaging.
Credentials are issued externally; libmoq does not need a token issuer to carry
or verify them. Neither a routing node ID nor a trusted-looking peering marker
is authorization. Client tokens stop at ingress; network relay hops use their
own scoped credentials.

## Specification and reference evidence

Read the checked-in [C4M-01 text](draft-ietf-moq-c4m-01.txt) first. It is copied
unchanged from the sibling MoQXR checkout. The
[IETF document page](https://datatracker.ietf.org/doc/draft-ietf-moq-c4m/)
was checked on 2026-09-20 and identifies revision 01 as the latest published
revision. Treat its provisional assignments as configurable, not final registry
values.

Transport requirements were checked first against the sibling local text files
`../moqxr/docs/draft-ietf-moq-transport-16.txt` and
`../moqxr/docs/draft-ietf-moq-transport-18.txt`: authorization is legal in SETUP
and supported request parameters, and token payload semantics depend on the
Token Type. Published references:
[draft 16](https://www.ietf.org/archive/id/draft-ietf-moq-transport-16.txt),
[draft 18](https://www.ietf.org/archive/id/draft-ietf-moq-transport-18.txt).
The initial implementation targets the two profiles implemented here, 16 and 18.

Compatibility evidence comes from the sibling moqx and red5-moq-relay source,
particularly `src/auth/AuthTokenIssuer.cpp`, `scripts/issue-cat-token.py`, and
`CatTokenParser.java`. The MoQXR CAT branch provides existing issuers, vectors,
CLI selection and an interoperability harness. Recheck peer revisions and
configuration for every live run; its native-backend results do not validate
libmoq. Use the sibling `../moq-pub` application as an additional reference and
smoke-test client where it supports the selected transport and profile.

| Explicit profile | Transport type | Authenticated structure and scopes |
| --- | --- | --- |
| `c4m01` | 1 | Serialized CBOR CWT; component-based namespace scope, exact/prefix/suffix matches, optional final nil; 1–3 scope elements |
| `moqx` | 16 by compatibility default | Catapult-derived key, HMAC over `Signature1`, protected text key ID, claim 65000 containing CBOR bytes |
| `red5-cose` | 16 by compatibility default | HMAC over `MAC0`, protected byte-string key ID, claim 100 containing scopes directly; current parser expects three scope elements |

A type override is explicit policy. Type 16 alone cannot distinguish the two
legacy profiles. Do not sniff and downgrade, translate claims, or re-sign
credentials in transport code. Type 1 support in the library cannot make a
current legacy relay accept C4M-01 semantics. Use separate verifier instances or
an explicit type/profile mapping with no ambiguous mapping per listener.

## Findings at c2900aa

| Area | Existing support | Required change |
| --- | --- | --- |
| Opaque bytes | `moq_auth_token_t` carries type plus byte span | `session_auth.c:264` rejects every NUL byte inbound; remove content interpretation and replace tests that enshrine it |
| SETUP | Incoming resolved tokens; advertised alias-cache size | Public outbound config is missing; draft-16 uses a 256-byte buffer and four parameter slots, draft-18 a 32-byte buffer |
| Core requests | Most session request configs accept tokens | Preserve draft-specific encoding, alias transactions and scratch ownership |
| Namespace publisher | Track PUBLISH has auth | `moq_pub_add_track()` constructs PUBLISH_NAMESPACE without credentials |
| Subscriber facade | SUBSCRIBE and standalone FETCH have auth | Joining FETCH and subscription update configs omit it |
| Managed endpoint | Automatically starts SETUP | Own and pass credentials before transport/session startup on every backend |
| Raw QUIC routing | Endpoint parses URL path | Endpoint explicitly does not emit PATH yet; add client SETUP authority/path without changing SNI semantics |
| Media sender | Creates catalog and media requests | No source for namespace/catalog/media/generated-track credentials |
| Media receiver | Creates SUBSCRIBE, FETCH and updates | No source for credentials; config initializers need ABI-safe sized variants |
| Relay verifier | Bounded sync hook and resolved tokens | CLI has allow-all/toy policies, no CAT verification |
| Relay actions | Most request entry points call hook | SUBSCRIBE_UPDATED applies Forward without authorizing REQUEST_UPDATE |
| Relay lifetime | Retained grants and revoke machinery | Revalidation output honored for SUBSCRIBE/PUBLISH_NAMESPACE only |
| Relay upstream | Generated upstream SUBSCRIBE | No relay-owned credentials |

Pointers are to the audited revision; function names are more stable than lines.
The four existing auth tests pass, but `test_session_auth.c` explicitly expects
NUL-containing token data to fail. Passing them is not binary-CWT coverage.

## Client credential contract

Keep `moq_auth_token_t` as the transport representation: raw serialized token
bytes, not base64 and not an already encoded AuthorizationToken envelope.
Introduce the following service API in `service/include/moq/auth.h`:

```c
#define MOQ_AUTH_SOURCE_MAX_TOKENS 16
#define MOQ_AUTH_SOURCE_MAX_TOKEN_BYTES 16384
#define MOQ_AUTH_SOURCE_MAX_TOTAL_BYTES 32768

typedef enum moq_auth_action {
    MOQ_AUTH_CLIENT_SETUP = 0, MOQ_AUTH_SERVER_SETUP = 1,
    MOQ_AUTH_PUBLISH_NAMESPACE = 2, MOQ_AUTH_SUBSCRIBE_NAMESPACE = 3,
    MOQ_AUTH_SUBSCRIBE = 4, MOQ_AUTH_REQUEST_UPDATE = 5,
    MOQ_AUTH_PUBLISH = 6, MOQ_AUTH_FETCH = 7, MOQ_AUTH_TRACK_STATUS = 8
} moq_auth_action_t;

typedef struct moq_auth_request {
    moq_auth_action_t action;
    moq_namespace_t ns;
    moq_bytes_t name;
} moq_auth_request_t;

typedef moq_result_t (*moq_auth_select_fn)(
    void *ctx, const moq_auth_request_t *request,
    moq_auth_token_t *out_tokens, size_t capacity, size_t *out_count);

typedef struct moq_auth_source {
    uint32_t struct_size;
    const moq_auth_token_t *tokens;
    size_t token_count;
    moq_auth_select_fn select;
    void *ctx;
} moq_auth_source_t;

MOQ_API void moq_auth_source_init_sized(moq_auth_source_t *source, size_t size);
```

These are proposed new symbols. A source is either a static list or a selector;
configuring both is `MOQ_ERR_INVAL`. Absence of a source explicitly preserves
anonymous operation. A present source must return at least one token; a selector
error or empty result fails the operation, with no fallback. Token type is an
unsigned QUIC-varint value; validate pointer/count pairs, integer overflow and
all three bounds. Larger legal transport values remain possible through the
core request API subject to its budgets; these are service limits.

Append `const moq_auth_source_t *setup_auth` to endpoint config and
`const moq_auth_source_t *request_auth` to sender and receiver configs. Static
bytes and source configuration are deep-copied at connect/create/attach.
Selector code and context must outlive the owning endpoint/service. The selector
writes descriptors into caller-provided slots; token bytes must remain valid
until the next invocation for that owner or its destruction. Stack-local byte
arrays are invalid. The library copies the returned bytes before yielding or
making another selector call.

SETUP selection runs synchronously during connect, before transport creation.
Later request selection runs on the endpoint's owner/pump thread. Callbacks must
not block, perform issuer I/O, call back into the same owner, or throw through C.
Shared contexts across owners are the caller's synchronization responsibility.
Refresh issuer data outside the callback. Retain a selected owned list across
WOULD_BLOCK retries of the same logical request; select again for a new request
or reconnection. No per-object callback is needed. Refreshing catalog objects
does not refresh a PUBLISH grant or implement token revalidation.

The request contains the actual namespace components and track name. It is not
a joined slash path. Initial catalog namespace advertisement, explicit catalog
PUBLISH, media PUBLISH, generated SAP/timeline tracks and dynamic tracks all
use this path. A shared live namespace advertisement is not silently replaced
when another track joins it. A subscriber-initiated namespace flow need not emit
PUBLISH; tests must force PUBLISH before asserting rejection for that action.

### Core and facade additions

Append `setup_auth_tokens`/`setup_auth_token_count`, `setup_authority` and
`setup_path` to `moq_session_cfg_t`. Use existing byte and token types, own copies
at session creation, and encode USE_VALUE before the first SETUP. Apply route
fields only to raw-QUIC client SETUP; WebTransport routes via CONNECT. Never emit
client route options from server SETUP. Network address, SNI and authority/path
remain distinct. Reject invalid routing/credentials before opening a managed
connection. Compute full SETUP size with checked arithmetic; require it to fit
the configured send budget and protocol limits, replacing small fixed buffers.
Managed configuration must provision sufficient bounded send capacity for the
selected SETUP; it must not silently truncate or drop tokens.

Append `namespace_auth_tokens`/`namespace_auth_token_count` to
`moq_pub_track_cfg_t`. Append `auth_tokens`/`auth_token_count` to
`moq_sub_update_cfg_t` and `moq_sub_joining_fetch_cfg_t`. These facade API inputs
remain borrowed for the duration of the synchronous call. Change joining FETCH
to use the sized core initializer so its auth tail is enabled.

Preserve frozen config floors, including old tail padding. All new pointer/count
pairs require both complete fields before access. Missing tails mean no auth.
Add sized initializers for joining FETCH and all three receiver config presets;
old pointer-only initializers must write only their historical extent. Test old,
truncated and oversized callers in C and C++, including canary bytes and OOM.

### Managed adapters

Pass owned SETUP tokens and route information through picoquic, pico_wt, MsQuic,
mvfst, proxygen, wtquic Network.framework and wtquic MsQuic before every immediate
or negotiated/deferred session creation. Direct managed facade users get the
same public fields and ownership guarantees. No post-connect injection.
An unimplemented backend must reject credential configuration with
`MOQ_ERR_UNSUPPORTED` before I/O, never connect anonymously. Report that backend
as unsupported until its credential-capture test passes. Linux builds cannot
establish Network.framework coverage; use Apple CI for it.

## Optional moq5-relay verifier

Keep it outside the session/core library. Use an optional relay-private C target
with OpenSSL 3 `libcrypto` for HMAC-SHA256, HKDF and constant-time verification,
and a bounded CBOR reader restricted to required definite-length structures.
No Folly/Catapult dependency is added to C core. Limit token size to 16 KiB,
CBOR nesting to 16 and visited items to 1024; fuzz the decoder before exposing
it to untrusted network input. Preserve original protected/payload bytes for
MAC construction. Verify authenticity before using claims to grant access.

Initial supported cryptographic suite is HS256 / COSE algorithm 5 with static
keys. Key discovery, asymmetric algorithms, encrypted tokens, token composition
and DPoP proofs are outside this milestone: reject tokens requiring an
unsupported feature. This is an explicit subset, not a claim of full C4M-01
conformance. Require issuer, audience, expiration and applicable MoQ scope as
local policy; validate optional not-before. Reject duplicate security claims or
headers, ambiguous key IDs, malformed CBOR, unsupported critical headers,
invalid key sizes, profile/type mismatch and algorithm confusion.

C4M matching must support the draft's optional scope elements, tuple component
matching and final nil rule. Legacy profiles retain their exact legacy grammar
and key derivation. Add independently signed cross-profile vectors; do not use
a decoder accepting its own encoder output as the only oracle. Claim labels
for C4M are explicit configuration and included in test vectors.

`moqr_auth_request_t` is a frozen v1 API. Do not append fields silently. The
verifier context receives a host-provided snapshot of Unix seconds and monotonic
microseconds; request `now_us` is monotonic and must never be compared directly
with CWT `exp`/`nbf`. Use monotonic scheduling for deadlines; host updates the
wall-clock snapshot each pump. On wall-clock rollback, retain the last observed
wall-time floor for that verifier lifetime so an expired grant cannot revive.

Install authorization before mutation at REQUEST_UPDATE and verify setup
direction for both client- and server-role bindings. Missing request credentials
are denied by default. Optional SETUP fallback is separately configured,
connection-scoped and bounded; never fall back after explicit credentials fail.
Transport aliases resolve bytes only and are not verifier-result caching.

For the first verifier milestone, extend or reject required lifetime handling
per action explicitly. Existing SUBSCRIBE and PUBLISH_NAMESPACE grant machinery
can schedule requested revalidation and reject a now-expired token at a recheck.
For C4M, absent or zero `moqt-reval` forbids token revalidation: check expiry at
admission, then retain that grant without scheduled token rechecks. Deployments
requiring expiry during an ongoing stream must require a positive interval.
Reject intervals below the host-configured minimum and positive revalidation on
other actions until their grant teardown is implemented. Do not promise ongoing
expiry/revocation for a grant that cannot enforce it. Denied updates leave Forward and previous
credentials unchanged; accepted updates must update the retained grant's
credential material atomically, or be rejected if that is not supported.

Generated upstream SETUP/SUBSCRIBE uses relay-owned source configuration from
the client milestone. Never forward ingress token bytes, share setup fallback
between connections, or reuse a client's grant as peer authentication. Network
peering tests must prove scoped signed authorization at each hop. In-process
shard replication remains a separate trusted-process boundary.

## Delivery and acceptance

1. [Client implementation plan](superpowers/plans/2026-09-20-cat4moq-client.md):
   binary-safe core, outbound SETUP and routing, adapter propagation, managed
   sender/receiver ownership and MoQXR integration.
2. [Relay implementation plan](superpowers/plans/2026-09-20-cat4moq-relay.md):
   optional explicit-profile verifier, complete admission checks, bounded
   lifetime behavior, CLI and signed relay-hop tests.

Client acceptance requires draft-16/18 capture of exact bytes and scopes, old
ABI callers remaining anonymous, selector failures causing no unauthorized
request, OOM/retry cleanup, and protected-relay tests proving catalog and actual
media delivery with libmoq selected. Run moqx, Red5 moqx, and Red5 cose separately;
record unsupported transport/backend combinations instead of assuming parity.
Tampered, expired, wrong namespace/action/track and cross-profile cases must be
rejected. A failed subscriber alone is not proof that a publisher was denied.

Verifier acceptance adds fixed independent cryptographic vectors, malformed-CBOR
fuzzing, update denial without mutation, expiry/revalidation teardown, per-hop
credential isolation and live signed publication/subscription. Full C4M support
remains gated on the explicitly unsupported features above.

## Baseline validation

At `c2900aa`, built `test_auth_cache`, `test_session_auth`, `test_d18_auth` and
`test_scenario_auth` using `build/dev`; the corresponding CTest selection passed
**4/4**. This is the existing implementation baseline, not a new CAT interop run.
No production source files were changed in the planning task.
