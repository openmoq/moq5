# CAT4MoQ Relay Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an opt-in, bounded CAT verifier and enforce its decisions throughout moq5-relay admission, active grants and generated upstream requests.

**Architecture:** A relay-private verifier implements the existing synchronous authorization hook. The host supplies clock snapshots and static key policy; the binding owns retained credential lifetimes and revocation. Network peers use their own credentials.

**Tech Stack:** C11, OpenSSL 3 libcrypto, a restricted bounded CBOR reader, CMake/CTest and existing relay/shard/loopback tests.

**Spec:** [CAT4MoQ design](../../cat4moq-design.md). This is a separate milestone after binary carriage from the [client plan](2026-09-20-cat4moq-client.md). Outbound peer integration consumes that plan's SETUP and source APIs.

## Global constraints

- The core remains sans-I/O and has no CBOR/COSE/crypto dependency.
- Existing no-auth callers retain their behavior.
- Client tokens stop at ingress; network relay hops use their own scoped credentials.
- `moqr_auth_request_t` is a frozen v1 API. Do not append fields silently.
- Initial supported cryptographic suite is HS256 / COSE algorithm 5 with static keys.
- Key discovery, asymmetric algorithms, encrypted tokens, token composition and DPoP proofs are outside this milestone: reject tokens requiring an unsupported feature.
- Full C4M-01 conformance is not the acceptance claim for this subset.

## Task 1: Explicit-profile cryptographic verification

**Create:** `relay/private/cat_verifier.h`, `relay/private/cat_verifier.c`, `relay/private/cat_cbor.h`, `relay/private/cat_cbor.c`, `relay/tests/test_relay_cat_verifier.c`, `relay/tests/fixtures/cat/` for fixed public test vectors, `fuzz/fuzz_cat_cbor.c` registered with the repository fuzz build convention.
**Modify:** root `CMakeLists.txt`, `relay/CMakeLists.txt`, `relay/tests/CMakeLists.txt`, `fuzz/CMakeLists.txt`.

**Interfaces (private, not installed SDK):**

```c
typedef enum moqr_cat_profile {
    MOQR_CAT_C4M01, MOQR_CAT_MOQX, MOQR_CAT_RED5_COSE
} moqr_cat_profile_t;
typedef struct moqr_cat_key {
    moq_bytes_t kid;
    moq_bytes_t secret;
} moqr_cat_key_t;
typedef struct moqr_cat_cfg {
    moqr_cat_profile_t profile;
    uint64_t token_type;
    int64_t moqt_label;
    int64_t reval_label;
    uint64_t minimum_revalidation_us;
    moq_bytes_t issuer;
    moq_bytes_t audience;
    const moqr_cat_key_t *keys;
    size_t key_count;
} moqr_cat_cfg_t;
typedef struct moqr_cat moqr_cat_t;
moq_result_t moqr_cat_create(const moqr_cat_cfg_t *cfg,
                            const moq_alloc_t *alloc, moqr_cat_t **out);
void moqr_cat_set_time(moqr_cat_t *cat, uint64_t unix_seconds,
                      uint64_t monotonic_us);
void moqr_cat_authorize(void *ctx, const moqr_auth_request_t *req,
                        moqr_auth_verdict_t *out);
void moqr_cat_destroy(moqr_cat_t *cat);
```

Configuration is deep-copied; reject duplicate key IDs, no keys, >32 keys, secrets
shorter than 32 bytes, issuer/audience absence, or colliding provisional labels.
Require a positive `minimum_revalidation_us` matching the host pump budget.
The profile fixes key-ID CBOR type and signature context. Initial C4M fixtures
use explicit experimental labels 65001/65002, with no claim of IANA assignment.
Require configured labels for production C4M mode. Maximum token 16 KiB,
aggregate input 32 KiB, 16 tokens, nesting 16, visited items 1024.

- [ ] Check in signed vectors made with independent issuer tooling for each profile, with issuer/audience/expiration and binary namespace/name components. Use fixed timestamps and test-only secrets. Cross-check moqx/Red5 vectors with their actual issuer/validator. C4M vectors follow the normative grammar, not unverified illustrative examples.
- [ ] Add positive tests plus byte tamper, wrong key, key-ID type mismatch, algorithm confusion, duplicate map keys, malformed lengths, nesting/item budget, unsupported critical headers and all cross-profile combinations. Run `relay_cat_verifier` to demonstrate failure before implementation.
- [ ] Add `MOQ_BUILD_RELAY_CAT=OFF` by default, producing private `moq-relay-cat` with `find_package(OpenSSL 3 REQUIRED COMPONENTS Crypto)` only when enabled. Parse bounded definite-length CBOR, preserve original protected/payload slices, and verify full MAC in constant time before interpreting permission claims. C4M uses the supported standard COSE_Mac0 form, including its legal tagging forms; reject other cryptographic forms explicitly.

  The moqx key derivation and authenticated input are exact:

  ```text
  PRK = HMAC-SHA256("moqx-catapult-v1", configured_secret)
  K   = HMAC-SHA256(PRK, "moqx-catapult-hmac-token-verify" || 0x01)
  MAC = HMAC-SHA256(K, CBOR(["Signature1", protected, h'', payload]))
  ```

  Red5 cose uses the configured raw key and `MAC0` context. C4M uses the standard
  MAC0 structure for the supported algorithm. Do not canonicalize and re-encode
  protected headers or claims before computing the MAC.
- [ ] Run vector tests with ASan/UBSan and fuzz the decoder and complete token-entry function with a seeded corpus. Missing/invalid tokens must yield DENY without reading beyond slices or allocating unbounded memory. Build with CAT OFF to prove no crypto dependency leaks into core.
- [ ] Commit parser/crypto separately after security review; no network listener enables it yet.

## Task 2: Claims, scopes and deterministic time

**Modify:** `relay/private/cat_verifier.c`, `relay/tests/test_relay_cat_verifier.c`, fixed vector fixtures.
**Interface:** `moqr_cat_set_time()` updates an owner-thread snapshot before relay pumping; the hook reads it without I/O. Reject authorization when no snapshot exists. Preserve the last wall-time floor on rollback. `moqr_cat_authorize()` fills only the existing verdict layout.

- [ ] Add fixed-clock tests: current valid, `now == exp`, before `nbf`, wall rollback, clock advance, interval overflow, negative/non-finite revalidation, fractional positive intervals and sub-microsecond intervals. Test issuer/audience mismatch and missing required claims independently of signature failure.
- [ ] Add normative C4M scope fixtures with 1–3 elements, exact/prefix/suffix component matches, trailing nil, malformed nil position, binary components containing NUL, and distinctions such as `["a/b"]` versus `["a", "b"]`. Test legacy scopes with their own grammar. Run to establish RED.
- [ ] Implement local mandatory issuer/audience/exp/moqt policy, optional nbf, profile-specific scopes and safe interval conversion. Deny unsupported authorization constraints rather than ignoring them; maintain an explicit allowlist of understood claims and harmless metadata. Token composition/proof claims require rejection. Do not combine partial scopes from unrelated tokens into a new permission. The initial strict policy requires every provided token to validate and independently authorize the complete action/resource; any invalid token denies the request. Test mixed valid/invalid and separately scoped lists explicitly.

  Deadline computation after validating units/ranges follows:

  ```text
  now_unix = max(last_wall_floor,
                 snapshot_unix + elapsed_monotonic_seconds)
  if now_unix >= exp: DENY(EXPIRED)
  expiry_delay_us = checked((exp - now_unix) * 1000000)
  if moqt_reval is absent or zero: next_check_us = 0
  else: next_check_us = min(expiry_delay_us, validated_positive_reval_us)
  ```

  Reject a positive interval below `minimum_revalidation_us`, including
  sub-microsecond values, rather than rounding it up. Round supported fractional
  intervals toward an earlier check; never turn them into permanent grants.
  For C4M, absent/zero reval prohibits token revalidation, so expiry is checked
  at admission only. Require issuers to supply positive reval when the deployment
  needs ongoing expiry enforcement. The binding must retain a grant when the
  verifier returns a nonzero deadline, or reject the operation.
- [ ] Run fixed-clock and scope vectors; ensure no test uses the host's current wall clock. Update profile documentation with unsupported feature behavior and experimental-label agreement.
- [ ] Commit claims and timing policy with negative coverage.

## Task 3: Enforce every action and active lifetime

**Modify:** `relay/src/bind/moqr_bind.c`, `relay/src/bind/moqr_bind_auth.h`, `relay/src/core/relay.c`, `relay/include/moq/relay/auth.h` documentation only; extend `relay/tests/test_relay_session_binding.c`, `relay/tests/test_relay_control.c`, `tools/moq5-relay/tests/test_relay_auth_shards.c`.

**Interfaces:** retain `moqr_authorize_fn` and the v1 request layout. Use existing `bind_authorize()`, `bind_grant_reserve()` and `grant_tick()` for supported retained grants. Any new public retention/capacity config must be sized/versioned. Authentication and protocol alias caching remain separate.

- [ ] Add a denied REQUEST_UPDATE carrying a valid but wrong-action token; assert the hook runs before Forward changes and neither data delivery nor stored credentials change. Add setup-direction tests for client/server-role bindings.
- [ ] Add positive-revalidation and absent/zero-revalidation tests for every action. Initially expected behavior is rejection for an action lacking revocable retention, rather than accepting an ignored interval. SUBSCRIBE and PUBLISH_NAMESPACE must revoke through existing teardown on expiration, policy change, DENY/DEFER recheck, OOM, connection retirement and shard owner withdrawal.
- [ ] Gate REQUEST_UPDATE before mutation. Preserve setup direction. For SUBSCRIBE/PUBLISH_NAMESPACE retain copied token material and install the computed deadline. Reject nonzero deadlines where the current call passes NULL for the lease output. For accepted credential refresh on an existing subscription, require both update permission and authorization to continue the original subscription; replace retained tokens/deadline atomically only after both checks pass.
- [ ] Expand continuing-grant lifetime support before enabling those actions in the CAT CLI: bind SETUP expiry to connection close, PUBLISH expiry to publication termination, SUBSCRIBE_NAMESPACE expiry to namespace-subscription cancellation and FETCH expiry to stopping any active response. Each new grant needs bounded reservation before acceptance, deep-copy failure rollback, timer scheduling and explicit terminal wire behavior per draft. TRACK_STATUS requires admission verification before its one-shot response and has no active grant once the response completes; it must still reject any positive revalidation requirement that cannot be meaningfully honored.

  Admission ordering for every newly covered path is:

  ```text
  resolve tokens -> authorize -> reserve/copy grant if needed
                -> mutate/accept -> deliver data
  ```

  Until an action's teardown tests pass, keep its nonzero-deadline path denied.
  A setup token requiring revalidation will therefore fail closed until
  connection lifetime support is present; do not bypass that rejection.
- [ ] Add optional SETUP fallback only with an explicit config switch and bounded connection-owned copies. No-token requests may use it; explicit failed tokens never do. Test two connections with different scopes, aliases, retirement and allocation failure. Default remains request credentials required.
- [ ] Run `relay_session_binding`, `relay_control`, shard authorization tests and all enabled relay tests. Assert no object chunks arrive after revocation, including mid-object delivery. Commit admission and each new lifetime class as independently tested changes.

## Task 4: CLI policy and relay-owned upstream credentials

**Modify:** `tools/moq5-relay/cli/config.h`, `tools/moq5-relay/cli/config.c`, `tools/moq5-relay/cli/main.c`, `tools/moq5-relay/CMakeLists.txt`, `relay/src/bind/moqr_bind.c`; extend `tools/moq5-relay/tests/test_relay_cli_config.c`, `tools/moq5-relay/tests/test_relay_loopback.c`, `tools/moq5-relay/tests/test_relay_auth_shards.c`. Add `docs/relay-cat-auth.md` and link it from `docs/README.md`.

**Interface:** add an explicit `cat` authorization policy alongside `allow_all` and `toy`, with required profile, issuer, audience and key-file settings; optional setup-fallback defaults false. C4M requires claim labels, legacy profiles use their fixed defaults. Outbound setup/request sources are distinct configuration from incoming verification keys and are copied through the client-plan APIs.

- [ ] Add CLI parser tests for CAT disabled at build time, missing/invalid settings, conflicting profiles, ambiguous keys, invalid labels, unreadable/oversized files and secret-free diagnostics. Run them before wiring config. Never downgrade invalid CAT configuration to allow-all.
- [ ] Instantiate one verifier per owner with immutable key policy; refresh its time snapshot before each pump, and destroy after retained grants terminate. Load keys synchronously at startup; rotation replaces policy on the owner and forces affected grants with positive revalidation to recheck. Zero/absent
  C4M revalidation must retain its specified behavior; emergency operator
  connection termination is a separate administrative action. Document restart-based rotation if hot replacement is not exposed in the initial CLI.
- [ ] Add relay-owned token source selection for generated upstream SETUP and `MOQR_INTENT_UPSTREAM_SUBSCRIBE`, using the actual upstream namespace/name. Provider failure prevents upstream creation; never copy ingress client tokens into the generated cfg.

  ```c
  upstream_cfg.auth_tokens = relay_owned_tokens;
  upstream_cfg.auth_token_count = relay_owned_token_count;
  ```

  Retain the selected list across backpressure retries and release on completion,
  cancellation or failure. Service source helpers can be reused by the host;
  do not make transport-independent relay-core depend on service.
- [ ] Run a protected loopback publisher -> relay -> subscriber with fixed signed credentials. Assert catalog and media bytes; test expiry during delivery with positive revalidation, invalid signature, cross-profile rejection, denied update and key removal. Run a two-relay chain with separately identifiable credentials at ingress and each upstream hop, asserting that no hop receives the client token. Exercise moqx/Red5 peers with explicit matching profiles; do not infer C4M compatibility from those legacy runs.
- [ ] Configure and run the new suite:

  ```sh
  cmake -S . -B build/cat-relay -DMOQ_BUILD_TESTS=ON \
    -DMOQ_BUILD_SIM=ON -DMOQ_BUILD_SERVICE=ON -DMOQ_BUILD_RELAY=ON \
    -DMOQ_BUILD_RELAY_CAT=ON
  cmake --build build/cat-relay -j4
  ctest --test-dir build/cat-relay -R '(cat|auth|relay)' --output-on-failure
  ```

  Add the existing backend/dependency options needed for live loopback in the
  chosen environment and record them with results. A simulator-only run does
  not establish socket interoperability.
- [ ] Review bounded storage, crypto policy, clocks, all action paths and secret-free diagnostics; run `git diff --check`. Commit the tested CLI/integration work with the supported subset and remaining conformance limits documented.

## Completion evidence

Record actual commands, enabled backends, peer revisions, signed-vector
provenance, test counts and received payload measurements. No result from the
client-carriage milestone substitutes for verifier, revocation or relay-hop
validation. DPoP/composition/asymmetric support requires a separate design and
new conformance evidence before expanding the support claim.
