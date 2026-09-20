# CAT4MoQ Client Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Carry externally issued C4M-01 and explicit current-relay compatibility credentials through core and managed libmoq, then enable authenticated MoQXR publishing.

**Architecture:** Core transports opaque bytes. Service-owned static lists or resource selectors populate SETUP and actual request configs before automatic protocol work. Managed adapters preserve ownership through immediate and deferred session creation.

**Tech Stack:** C11, existing C++ adapter facades, CMake/CTest and the existing MoQXR C++ interoperability harness; no new core crypto dependency.

**Spec:** [CAT4MoQ design](../../cat4moq-design.md), including its exact proposed `moq_auth_source_t` API and ownership contract.

## Global constraints

- The core remains sans-I/O and has no CBOR/COSE/crypto dependency.
- Existing no-auth callers retain their behavior.
- CMAF remains MoQXR's default media packaging.
- Client tokens stop at ingress; network relay hops use their own scoped credentials.
- Initial transport profiles are draft 16 and draft 18; compatibility profiles are explicit.
- Preserve frozen config floors, including old tail padding.
- Checked boxes below reflect recorded tests and independent review; platform/runtime limitations are recorded separately.

## Task 1: Make inbound token payloads binary-safe

**Files:** modify `core/src/session/session_auth.c`, `core/src/session/profile_d16.c` if a caller needs its error classification adjusted; extend `tests/unit/test_session_auth.c`, `tests/unit/test_d18_auth.c`, `tests/unit/test_auth_cache.c` and `tests/scenario/test_scenario_auth.c`.

**Interface:** existing `moq_auth_token_value_semantically_valid(const uint8_t *, size_t)` must not interpret token payload bytes. Preserve structural envelope/alias validation in `process_auth_tokens()` and existing event borrow epochs.

- [x] Replace the test expecting `a\0b` to be malformed with exact-byte acceptance. Add direct helper regression in the existing internal-auth test target:

  ```c
  const uint8_t payload[] = {0xd2, 0x84, 0x40, 0x00, 0x80, 0xff};
  assert(moq_auth_token_value_semantically_valid(payload, sizeof payload));
  ```

  This is an opaque transport fixture, not a valid signed CWT. Add actual signed CBOR vectors to interoperability coverage separately. Exercise USE_VALUE, REGISTER/USE_ALIAS, replacement, failed-transaction rollback and same bytes in incoming SETUP on both drafts. Assert byte equality on the resolved event before invalidating its borrow.
- [x] Run the focused tests below and record the expected binary regression failure.
- [x] Remove the `memchr(..., 0, ...)` content rule. Preserve pointer/length and wire-action validation. Treat token-value content, including an empty value where the envelope permits it, as the token-type verifier's decision; do not invent a generic CBOR parser in transport.
- [x] Run the focused tests; malformed aliases, truncated wire encodings, oversized counts and rollback must still fail with their original protocol errors.

  ```sh
  cmake --build build/dev --target test_auth_cache test_session_auth test_d18_auth test_scenario_auth -j4
  ctest --test-dir build/dev -R '^(auth_cache|session_auth|d18_auth|scenario_auth)$' --output-on-failure
  ```
- [x] Review and commit the tested binary-carriage change independently.

## Task 2: Own outbound SETUP tokens and raw-QUIC routing

**Files:** modify `core/include/moq/session.h`, `core/src/session/session.c`, `core/src/session/session_internal.h`, `core/src/session/session_setup.c`, `core/src/session/profile_d16.c`, `core/src/session/profile_d18.c`; extend `tests/unit/test_session_auth.c`, `tests/unit/test_d18_setup_options.c`, `tests/unit/test_d18_auth.c`, `tests/unit/test_oom.c`.

**Interface:** append the following after the full historical config extent, preserving its floor and any padding:

```c
const moq_auth_token_t *setup_auth_tokens;
size_t setup_auth_token_count;
moq_bytes_t setup_authority;
moq_bytes_t setup_path;
```

Core copies all bytes at `moq_session_create()`. `moq_session_destroy()` and every failed-create path free them. Server SETUP may carry credentials but not client route options.

- [x] Add SimPair cases configuring client and server credentials for both drafts. Modify/free the caller's buffers immediately after create, then start the session and assert the peer receives the original bytes. Include 1 KiB credentials to expose both fixed-buffer bugs, multiple tokens, zero bytes, type 1 and type 16.
- [x] Add raw-client authority/path round trips, forbidden server-route config, old config prefixes, partial token pointer/count tails, historical padding canaries, oversized caller structs and allocation-failure sweeps. Confirm expected failures before adding fields/encoding.
- [x] Deep-copy tokens/routes before returning success; check count/length arithmetic and QUIC-varint ranges. Replace `params[4]`, the 256-byte draft-16 buffer and the 32-byte draft-18 buffer with storage sized for the checked complete SETUP. Encode each token using USE_VALUE. Preflight full size against `send_buffer_size` and protocol bounds; fail with `MOQ_ERR_BUFFER` before queuing any partial SETUP if it cannot fit.

  The caller pattern after this task is:

  ```c
  moq_session_cfg_t cfg;
  moq_session_cfg_init_sized(&cfg, sizeof cfg, moq_alloc_default(), MOQ_PERSPECTIVE_CLIENT);
  cfg.setup_auth_tokens = tokens;
  cfg.setup_auth_token_count = count;
  cfg.setup_authority = MOQ_BYTES_LITERAL("relay.example:4433");
  cfg.setup_path = MOQ_BYTES_LITERAL("/moq");
  cfg.send_buffer_size = 65536;
  ```

- [x] Build the affected tests and run `ctest --test-dir build/dev -R '(auth|d18_setup|oom|session_foundation)' --output-on-failure`. Run ASan/UBSan on new ownership and truncated-config cases. Confirm an over-budget token fails deterministically without partially sending SETUP.
- [x] Commit core SETUP support with API ownership comments and captured-wire evidence.

## Task 3: Preserve SETUP through every managed adapter

**Files:** modify the managed config headers and their constructors/session creation paths:

| Header | Implementation |
| --- | --- |
| `adapters/picoquic/include/moq/picoquic_threaded.h` | `adapters/picoquic/moq_picoquic_threaded.c` |
| `adapters/pico_wt/include/moq/pico_wt_managed.h` | `adapters/pico_wt/pico_wt_managed.c` |
| `adapters/msquic/include/moq/msquic_managed.h` | `adapters/msquic/msquic_managed.c`, `adapters/msquic/msquic_adapter.c` |
| `adapters/mvfst/include/moq/mvfst.h` | `adapters/mvfst/src/mvfst_adapter.cpp` |
| `adapters/proxygen/include/moq/proxygen_wt_managed.h` | `adapters/proxygen/src/wt_managed.cpp` |
| `adapters/wtquic/include/moq/wtquic_network_managed.h` | `adapters/wtquic/wtquic_network_managed.c` |
| `adapters/wtquic/include/moq/wtquic_msquic_managed.h` | `adapters/wtquic/wtquic_msquic_managed.c` |

Extend existing config ABI tests in those adapter test directories. Add `test_setup_auth` fixtures to each backend's existing test infrastructure and register them in its `CMakeLists.txt` (backend-qualified CTest names).

**Interface:** append Task 2's raw token list fields to managed configs; add route fields to raw-QUIC facades only. Each facade owns copies until all deferred sessions/worker shutdown complete. Populate the sized session cfg before `moq_session_create()` on every creation path.

- [x] Add a test for immediate and negotiated/deferred SETUP using caller bytes mutated immediately after facade construction. Capture the peer SETUP tokens, not just a successful constructor return. Check server accept paths where supported.
- [x] Run each available adapter fixture to expose missing tokens. Preserve the original version negotiation tests.
- [x] Add checked cloning and propagation. Ensure failure cleanup precedes thread/socket startup when validation fails. For a backend not yet implemented, return `MOQ_ERR_UNSUPPORTED` on any configured token list before network work. Do not treat the unsupported branch as successful parity.

  ```c
  session_cfg.setup_auth_tokens = owned_tokens;
  session_cfg.setup_auth_token_count = owned_token_count;
  /* session_cfg is initialized with moq_session_cfg_init_sized. */
  ```

  Here `owned_tokens` and `owned_token_count` are the adapter instance's retained fields, not borrowed config memory.
- [x] Build and run the seven backend fixture sets in their supported environments. Run Network.framework on Apple CI; record unavailable dependencies as untested. Add old/truncated config and OOM tests to each new owned path.
- [x] Commit backend changes in independently reviewable backend groups, retaining fail-before-I/O behavior throughout.

Validation qualification: peer-capture fixtures ran for raw picoquic, PicoWT and
raw MsQuic. Raw mvfst compilation is blocked by the installed Fizz API mismatch;
its implementation is unverified at runtime. Proxygen and WTquic paths explicitly
reject configured credentials; Apple execution was unavailable. Broad-suite
baseline failures are recorded in the execution results, not counted as passing
backend parity.

## Task 4: Add service sources and wire endpoint SETUP

**Files:** create `service/include/moq/auth.h`, `service/src/auth_source.h`, `service/src/auth_source.c`, `service/tests/test_auth_source.c`; modify `service/CMakeLists.txt`, `service/include/moq/endpoint.h`, `service/src/endpoint.c`, `service/tests/test_endpoint_resolve.c`, `service/tests/test_endpoint_lifecycle.c`.

**Interfaces:** implement the exact public types/signature in the design. Append `const moq_auth_source_t *setup_auth` to `moq_endpoint_cfg_t`. Private source storage clones descriptors/static bytes with the endpoint allocator and retains selector/context. Private selection returns an owned bounded token list for one logical operation; its release routine is used on every terminal path.

- [x] Add table-driven tests for null source, static binary data, provider-selected data, empty present source, both sources set, callback error, invalid pointers, type overflow, 17 tokens, 16 KiB+1 token, total >32 KiB and allocator failure at each clone. Verify provider calls receive the requested action/resource and no token data enters diagnostics.
- [x] Add a fake-backend endpoint test where setup provider failure produces zero transport-create calls; another where a successful CLIENT_SETUP selection reaches the facade before auto-start. Run the new tests to establish RED.
- [x] Implement bounded source copy/selection. Invoke setup selection synchronously before choosing/starting transport; own the selected list across deferred negotiation. Source failures are terminal; never retry as anonymous. Provision bounded SETUP send capacity for the selected list and route options. Use raw-QUIC URL authority/path for Task 2; preserve existing WebTransport CONNECT routing and SNI policy.

  ```c
  moq_auth_source_t source;
  moq_auth_source_init_sized(&source, sizeof source);
  source.tokens = tokens;
  source.token_count = count;
  moq_endpoint_cfg_init_sized(&endpoint_cfg, sizeof endpoint_cfg);
  endpoint_cfg.setup_auth = &source;
  ```

  All descriptors and bytes in this static-source example may be released after connect returns; the endpoint retains its own copies.
- [x] Register CTest `auth_source`; run it plus endpoint resolve/lifecycle/handshake/post-contract tests. Confirm old endpoint callers have absent auth and unsupported backends fail before socket creation.
- [x] Commit service source and endpoint integration with public thread/lifetime/error documentation.

## Task 5: Authorize namespace, catalog and media sender requests

**Files:** modify `core/include/moq/publisher.h`, `core/src/facade/publisher.c`, `service/include/moq/media_sender.h`, `service/src/media_sender.c`; extend `tests/unit/test_publisher.c`, `service/tests/test_media_sender_catalog.c`, `service/tests/test_media_sender_lifecycle.c`, `service/tests/test_media_sender_refresh.c`, `service/tests/test_media_sender_reject.c`.

**Interfaces:** append `namespace_auth_tokens`/`namespace_auth_token_count` to `moq_pub_track_cfg_t`. Append `const moq_auth_source_t *request_auth` to sender cfg. Existing `moq_pub_publish_cfg_t.auth_tokens` carries selected PUBLISH credentials.

- [x] Inspect outbound requests in the existing simulated sender peer. Record `(action, namespace tuple, track name, type, bytes)` for initial namespace advertisement, catalog PUBLISH, media PUBLISH, generated SAP/timeline and dynamic tracks. Return distinct binary tokens from the selector for each resource and assert exact matches. Shared namespace reuse must not emit a second advertisement or replace its credentials.
- [x] Add failure cases: reject namespace selection before track registration; reject catalog/media selection without emitting that request; mutate original static buffers after create; WOULD_BLOCK retains the same selected bytes and calls the provider once for that request. Run tests for expected failures.
- [x] Use Task 4 selection/ownership helpers at `sender_hook()` and `sender_add_pub_track()`. Select namespace auth only for a new advertisement; pass it through `moq_pub_add_track()`. Populate catalog/media `moq_pub_publish_cfg_t` from the selected PUBLISH list. Preserve pending selection until the request succeeds or terminates, and release it on reject/destroy.

  ```c
  track_cfg.namespace_auth_tokens = namespace_tokens;
  track_cfg.namespace_auth_token_count = namespace_token_count;
  publish_cfg.auth_tokens = publish_tokens;
  publish_cfg.auth_token_count = publish_token_count;
  ```

  These variables are the owned lists selected for the actual wire resource; use sized config initializers. Do not select again for each media/catalog object.
- [x] Run publisher and all enabled media_sender CTests. Check retained catalog Joining FETCH and payload delivery still pass with no auth. Run ownership tests under sanitizers and review asynchronous rejection handling.
- [x] Commit sender parity. Do not change its existing ACCEPT_ALL inbound policy or describe this task as a CAT verifier.

## Task 6: Add receiver FETCH and update credential parity

**Files:** modify `core/include/moq/subscriber.h`, `core/src/facade/subscriber.c`, `service/include/moq/media_receiver.h`, `service/src/media_receiver.c`; extend `tests/unit/test_subscriber.c`, `service/tests/test_media_receiver_fetch_catalog.c`, `service/tests/test_media_receiver_update_ack.c`, `service/tests/test_media_receiver_scripted.c`.

**Interfaces:** append `auth_tokens`/`auth_token_count` to subscriber joining-FETCH and update configs; append `const moq_auth_source_t *request_auth` to receiver cfg. Add `moq_sub_joining_fetch_cfg_init_sized(cfg, size)` and `moq_media_receiver_cfg_init_sized(cfg, size)`, `moq_media_receiver_cfg_init_live_sized(cfg, size)`, `moq_media_receiver_cfg_init_flow_control_sized(cfg, size)`, using the existing pointer types and `size_t` size.

- [x] Freeze current receiver/joining-FETCH config extents and add canary tests for all old initializers before extending layouts. Add captured credentials for catalog/media/manual SUBSCRIBE, catalog joining FETCH and Forward-state REQUEST_UPDATE. Inject selector denial for each path; assert no unauthorized message and no false local success.
- [x] Run the new tests and record missing-token failures.
- [x] Initialize the core joining-FETCH cfg with its sized initializer and pass credentials through both missing facade paths. Clone the receiver source at create/attach. Select by FETCH and REQUEST_UPDATE separately from SUBSCRIBE, retaining owned bytes across retries. Existing catalog-fetch parent relationship does not eliminate the separate request credential requirement.

  ```c
  core_fetch_cfg.auth_tokens = joining_cfg->auth_tokens;
  core_fetch_cfg.auth_token_count = joining_cfg->auth_token_count;
  core_update_cfg.auth_tokens = update_cfg->auth_tokens;
  core_update_cfg.auth_token_count = update_cfg->auth_token_count;
  ```

  Read appended fields only when the complete pair is within `struct_size`.
- [x] Run subscriber and all enabled media_receiver CTests, including update acknowledgement, catalog bootstrap, teardown retry and no-auth baseline. Run config/ownership tests under sanitizers.
- [x] Commit receiver parity and document callback failure behavior.

## Task 7: Enable MoQXR and prove current-relay compatibility

**Files in moq5:** modify `service/include/moq/auth.h`, `docs/integration.md`, `docs/README.md`; update the relevant C++/Swift service wrappers only where initializer/API changes require it. Add `#define MOQ_SERVICE_AUTH_API_VERSION 1` when Tasks 1–6 are complete for the supported backend set.

**Coordinated files in sibling moqxr:** `src/transport/libmoq_publisher.cpp`, `include/openmoq/publisher/transport/libmoq_publisher.h`, `tests/libmoq_translation_test.cpp`, `scripts/test-cat4moq-interop.py`, `docs/cat4moq-plan.md`. This is a separate repository change and commit, required for end-to-end acceptance.

**Interface:** map `cat4moq::Credential` to raw `moq_auth_token_t`, and its resource provider to `moq_auth_select_fn`. Setup remains separately selected in MoQXR. Catch all C++ exceptions before returning through the C callback and retain byte ownership under the service contract.

- [x] Capture selected setup/request tokens in the shared MoQXR authorization bridge for both drafts and all profiles. Verify batch, stdin, SRT and live-object paths use this same bridge; retain fail-before-I/O expectations with older libmoq. Exercise provider failures and terminal error propagation. Live authenticated media validation covers stdin; distinct positive captures for the other three input paths remain additional coverage.
- [x] Implement mapping guarded by `MOQ_SERVICE_AUTH_API_VERSION`. Profile determines the default type (1 or 16); explicit overrides survive unchanged. Legacy `AuthorizationToken` inputs are already encoded envelopes: strictly decode supported USE_VALUE forms for the selected draft, or keep a clear unsupported error before I/O for other forms. Never nest an envelope inside a new token or drop it.
- [x] Build MoQXR with the actual dependency selected:

  ```sh
  cmake -S ../moqxr -B ../moqxr/build-libmoq-cat4moq \
    -DOPENMOQ_USE_LIBMOQ_PUBLISHER=ON \
    -DOPENMOQ_LIBMOQ_SOURCE_DIR="$PWD"
  cmake --build ../moqxr/build-libmoq-cat4moq -j4
  ctest --test-dir ../moqxr/build-libmoq-cat4moq --output-on-failure
  python3 ../moqxr/scripts/test-cat4moq-interop.py \
    --targets moqx --publisher-backend libmoq \
    --publisher ../moqxr/build-libmoq-cat4moq/openmoq-publisher
  ```

  Run from the moq5 root. Record backend, transport/draft, relay revisions, issuer/validator decisions and measured subscriber catalog/media bytes. Add a WebTransport publisher run when supported; raw-QUIC publishing plus a WebTransport subscriber does not prove WebTransport publisher support.
- [x] Run signed negative cases (tamper, expiry, wrong namespace/action/track, profile mismatch). Prove publisher rejection independently of subscriber failure. Force a PUBLISH request for PUBLISH-action tests; namespace-only subscriber-initiated flow is valid without it. Run C4M-01 byte/claim vectors against a controlled verifier fixture; current legacy relay acceptance is not required for type 1.
- [x] Run the appropriate full moq5 suites, `git diff --check`, and a C++ review of changed adapters/wrappers. Report missing platform/backend coverage explicitly. Commit each repository independently with exact dependency requirements and no generated author tagline.

## Review gates

Task 1 must precede all positive binary-CWT tests. Task 2 precedes Task 3; Task 4
consumes their APIs. Tasks 5 and 6 depend on Task 4 and can be reviewed separately.
Task 7 closes the managed-client milestone. The
[relay plan](2026-09-20-cat4moq-relay.md) is separately deliverable and is not a
prerequisite for current moqx/Red5 compatibility tests.

## Execution results (2026-09-20)

The managed client implementation advertises `MOQ_SERVICE_AUTH_API_VERSION 1`
in `35b3d31`. Core and services carry opaque credentials; verification remains
the separate relay milestone. Endpoint selection runs before I/O, and sender/
receiver selectors use the actual namespace and track request. Sized config
initializers preserve historical callers. Peer refusal codes are available via
`moq_media_sender_peer_request_error()` without exposing peer reason text.

### Validation and backend scope

- Core/service ASan+UBSan: **152/152**, leak detection disabled. Log:
  `/tmp/moq5-service-final-tests.log`.
- Raw picoquic/PicoWT SETUP and endpoint auth sanitizer fixtures: **3/3**.
  Log: `/tmp/moq5-endpoint-final-tests.log`. Runtime peer capture also passed
  for raw MsQuic, including both drafts and asymmetric 16 KiB SETUP credentials.
  Caller-byte mutation, allocation failures and old/truncated configurations
  are covered. Endpoint-only source/lifecycle/resolve/handshake/post tests pass.
- Broader picoquic/PicoWT regressions: **49/53**; raw MsQuic: **29/31**. All six
  failures reproduce with baseline adapters matching `efc596c`:
  `pico_wt_capsule_sequence` (chunk 36/parser crash), `pico_wt_d18` (announce
  expectation), `pico_wt_rx_lifecycle` (pending receive),
  `pico_wt_managed_close` (clean close incomplete),
  `msquic_managed_shared_link` and `msquic_pinned_fail_closed` (nested consumer
  links missing `QuicAddr*` functions). Logs:
  `/tmp/managed-regressions-final.log`,
  `/tmp/managed-msquic-regressions-final.log`; baseline logs:
  `/tmp/managed-baseline-tests.log`, `/tmp/managed-baseline-close-tests.log`,
  `/tmp/managed-baseline-msquic-tests.log`. These suites are not clean.
- Raw mvfst owns and forwards credentials, but build/runtime validation is
  blocked by the installed Fizz `OpenSSLCertificateVerifier` constructor API.
  Proxygen WT and both WTquic facades explicitly reject configured credentials
  before I/O. Those runtime dependencies and Apple CI were unavailable.
- Large credentials above cover SETUP. Existing draft-18 per-request receive
  buffers remain capped at 4096 bytes. The service's 16 KiB input limit is not
  a guarantee that every action credential of that size is accepted.
- Coordinated MoQXR build with the actual feature header: **24/24 CTests**.
  Its adapter and translation tests also compile against old `c2900aa` headers.
  Unit captures validate both drafts, all profiles, legacy envelope unwrapping,
  binary ownership, actual provider resources and terminal failures. All four
  input routes share this mapping; authenticated live media tests use stdin.
- Controlled type-1 C4M-01 fixture: **18 MAC/type/claim decisions**, using private
  labels -65537/-65538. Current issuer/Red5-validator fixtures: **30 decisions**.
  Neither constitutes production C4M-01 relay support or secure peering.

### Implementation decisions

The requested existing `feature/cat4moq` checkout was retained because it had
only this task's plans. Independent source-helper and sender work proceeded
alongside isolated task implementations without overlapping files. The client
milestone precedes the separately planned relay verifier. The unsized draft-18
SETUP decoder-options layout remains unchanged; an additive encoder API carries
routes. Shared MoQXR mapping tests and inspected call sites cover all four input
routes, with real authenticated media coverage limited to stdin. These choices
avoid an ABI break and keep the backend/runtime coverage boundaries explicit.


### Final managed relay matrix

The selected topologies passed **60/60 cases**: ten cases for each of moqx,
Red5 `moqx` and Red5 `cose`, over both raw-QUIC and WebTransport publishing,
transport draft 18. Subscribers used WebTransport: full Playa player for moqx,
and Playa connection API for Red5. Each positive media case received video
initialization (790 bytes), audio initialization (728 bytes), video groups 0/1
(150,593 bytes), and audio groups 0/1 (17,791 bytes). Each topology also accepted
both explicit PUBLISH requests. All eight denial cases per topology independently
rejected the publisher and observed no catalog/media for eight seconds.

| Publisher transport | Subscriber API and selected targets | Selected cases | Artifacts |
| --- | --- | --- | --- |
| Raw QUIC | Player, moqx | 10/10 | `/tmp/cat4moq-interop-mhkpxba7` |
| WebTransport | Player, moqx | 10/10 | `/tmp/cat4moq-interop-tevsvrbf` |
| Raw QUIC | Connection, both Red5 profiles | 20/20 | `/tmp/cat4moq-interop-5nuppi7o` |
| WebTransport | Connection, both Red5 profiles | 20/20 | `/tmp/cat4moq-interop-d1lev_5v` |

The last two runs also tried moqx in connection mode: wrong-action/wrong-track
observations failed on each transport with an upstream-session-closed request
error. Those combined runs exited 1 and are not clean all-target runs. Their
Red5 cases passed individually; the separate moqx player runs exited 0. The
connection fixture accepts only catalog-absent code 16 as retryable, preserving
other errors as failed observation rather than evidence of authorization denial.

Revisions: moqx `e988f967`, Red5 `43d1e97`, Playa `7b41d74`, moq5 `35b3d31`.
The publisher was MoQXR `8a69380` plus this integration change; tested binary
SHA-256 `c93f528249208972148d916d400e53537572be36a1d24fc2153174c8b96c6a23`.
The type-1 fixture passed 18 decisions and current issuers passed 30 decisions.
These are client compatibility and transport-delivery results, not production
C4M-01 verification, authenticated SRT delivery, rendered frames or secure peering.
Player routing loss is filed as [moq-playa #17](https://github.com/openmoq/moq-playa/issues/17).
