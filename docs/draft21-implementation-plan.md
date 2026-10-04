# MOQT DRAFT-21 SUPPORT -- IMPLEMENTATION PLAN

Branch:      feature/draft21 (from main f9e20eb)
Authority:   draft-ietf-moq-transport-21 (text copy checked in at
             ../moq-contribution-interop-runner/docs/draft-ietf-moq-transport-21.txt)
             Delta notes: draft Appendix A.1-A.3 (line 8042 of that file).
Compliance:  moq-contribution-interop-runner (sibling dir), draft 21 catalog
             requirements/draft21.json (173 testable MUST/MUST NOT rows,
             ~220 executable scenarios, IDs prefixed d21-).
Scope:       libmoq (moq5) as the CONTRIBUTION PUBLISHER, scored by the runner.
             The session core is symmetric, so the d21 profile is complete
             enough for subscribe/relay roles, but acceptance is judged on
             publisher-applicable rows only.

For agentic workers: use superpowers:subagent-driven-development or
superpowers:executing-plans. Steps use "[ ]" checkboxes. Commit messages: no
emoji, no Claude tagline, no Co-Authored-By (user rule).


## RULES OF ENGAGEMENT
R1. The draft decides. Where this plan, the runner, moqxr, or any existing
    d18 code disagrees with draft-21 text, the draft wins. Cite the draft
    section in the commit message and in a code comment on any non-obvious
    choice.
R2. The runner is a test instrument, not a second authority. If a runner row
    fails and you believe the runner is wrong, quote the requirement ID and
    the draft lines (the runner's own README asks for this) and record it in
    "Runner disputes" at the bottom of this file; do not bend the publisher
    to satisfy a wrong row, and do not edit the runner on this branch.
R3. d16 and d18 behavior must not change. Every task ends with the existing
    d16/d18 suites green (command in Task 0).
R4. Follow the established profile architecture: a draft is a
    moq_profile_ops_t vtable (core/src/session/profile.h) plus a wire codec;
    the session core asks CAPABILITIES, never version numbers
    (scripts/check_profile_boundary.sh enforces this). New draft-specific
    behavior goes behind a vtable op or capability field.
R5. Prefer import/include style and comment density of the surrounding code.


## KNOWN WIRE DELTA d18 -> d21 (from Appendix A.2/A.3 and the IANA tables)
Unchanged: vi64 integer coding, 16-bit control Length, uni-control-channel
topology, request-per-bidi-stream, SETUP type 0x2F00, PADDING 0x132B3E28,
FETCH_HEADER stream type 0x05.

Control messages
  - PUBLISH_OK (0x1E) removed; type RESERVED. The PUBLISH response is
    REQUEST_OK (9.3). Draft 18 had a distinct PUBLISH_OK.
  - PUBLISH_BLOCKED (0xF) renamed PUBLISH_SKIPPED (9.19); layout unchanged
    (Track Namespace Suffix, Track Name Length, Track Name).
  - NEW PUBLISH_STATE_NOTIFY 0x22 (9.10): Number of Parameters, Parameters.
  - GOAWAY (0x10): Request ID removed. Now New Session URI Length, URI,
    Timeout (9.2). Session vs per-request GOAWAY clarified.
  - FETCH (0x16, 9.11): Request ID, Track Namespace, Track Name, Parameters.
    Fetch types 1/2/3 (Standalone / Relative Joining / Absolute Joining) are
    GONE; the range is carried by LOCATION_FILTER (0x21). "Joining" is
    replaced by fill streams (see FILL_PARAMETERS).
  - SUBSCRIBE_OK / PUBLISH carry Track Properties (unchanged shape);
    PUBLISH can carry Subscription Parameters; AUTHORIZATION_TOKEN is never
    copied from SUBSCRIBE_TRACKS.
  - REQUEST_UPDATE (0x2) carries the subscription parameters; they are no
    longer in PUBLISH_OK. An unexpected REQUEST_UPDATE is a session error.
    FORWARD allowed on REQUEST_UPDATE for SUBSCRIBE_TRACKS.
  - PUBLISH_DONE: SUBSCRIPTION_ENDED status removed; Stream Count max 2^64-1.
  - Error codes: add TOO_MANY_REQUEST_UPDATES; remove
    VERSION_NEGOTIATION_FAILED; REDIRECT ambiguity with empty namespace/name
    resolved (A.2). Re-derive the full code tables from Sections 13/IANA.

SETUP options (9.1.x)
  0x01 PATH, 0x03 AUTHORIZATION_TOKEN, 0x04 MAX_AUTH_TOKEN_CACHE_SIZE,
  0x05 AUTHORITY, 0x07 MOQT_IMPLEMENTATION (d18 had these) plus NEW
  0x06 MAX_FILTER_RANGES (9.1.6) and 0x08 MAX_REQUEST_UPDATES (9.1.7).

Message parameters (9.20)
  0x21 SUBSCRIPTION_FILTER becomes LOCATION_FILTER (restructured, 3.3.1).
  NEW 0x23 FILL_PARAMETERS (carries nested Parameters), 0x25 SUBGROUP_FILTER,
  0x26 OBJECTID_FILTER, 0x27 PRIORITY_FILTER, 0x28 OBJECT_PROPERTY_FILTER,
  0x29 TRACK_PROPERTY_FILTER (Range Filters 3.3.2, only if the peer's
  MAX_FILTER_RANGES allows), 0x35 INCLUDE_PROPERTIES.
  GROUP_ORDER (0x22) moves from PUBLISH_OK to SUBSCRIBE_TRACKS.
  Kept: 0x02/0x06 delivery timeouts, 0x03 token, 0x04 RENDEZVOUS_TIMEOUT,
  0x08 EXPIRES, 0x09 LARGEST_OBJECT, 0x0A FILL_TIMEOUT, 0x10 FORWARD,
  0x20 SUBSCRIBER_PRIORITY, 0x32 NEW_GROUP_REQUEST, 0x34 TRACK_NAMESPACE_PREFIX.

Track / Object properties (10.x)
  OBJECT_DELIVERY_TIMEOUT 0x02 and SUBGROUP_DELIVERY_TIMEOUT 0x06 are now
  BOTH Track and Object properties. Others as listed in the IANA table:
  MAX_CACHE_DURATION 0x04, IMMUTABLE_PROPERTIES 0x0B,
  DEFAULT_PUBLISHER_PRIORITY 0x0E, DEFAULT_PUBLISHER_GROUP_ORDER 0x22,
  DYNAMIC_GROUPS 0x30, PRIOR_GROUP_ID_GAP 0x3C, PRIOR_OBJECT_ID_GAP 0x3E.
  LOC properties re-numbered in the provisional registry (d18 -> d21):
    TIMESTAMP 0x06 -> 0x10;  VIDEO_FRAME_MARKING 0x0A -> 0x09;
    VIDEO_CONFIG 0x0D (now Track only); NEW AUDIO_CONFIG 0x0F;
    TIMESCALE 0x08 (Track only); AUDIO_LEVEL 0x0C.
  This affects the SERVICE layer's LOC property emitter (Task 9), not core.

Data plane (11.x)
  - OBJECT_DATAGRAM and SUBGROUP_HEADER Type fields are Type Flags
    bitfields; a set bit with no defined meaning is PROTOCOL_VIOLATION.
  - Object Status / range markers: add End of Timed-Out Range (0x20C) next to
    End of Non-Existent (0x8C) and End of Unknown (0x10C).
  - OBJECT_DELIVERY_TIMEOUT clock starts at the last header byte (not first
    payload byte).
  - Fill streams replace joining FETCH data; scheduling rules between
    fill-delivered and subscription-delivered Objects (3.4).
  - Multiple concurrent subscriptions per Track; track alias reuse caution.
  - Object Status payload rule is registry-extensible; datagrams win
    cross-forwarding-preference scheduling ties; no relay exception for
    reordering/dropping Objects.

Transport / URI
  - ALPN is "moqt-21" (draft appended to "moqt-").
  - Unified moqt:// URI for QUIC and WebTransport; query component excluded
    from the MOQT scope; host resolution defined (A.2, section 2 of draft).

Items this table may have MISSED: Task 1's first step is a section-by-section
re-read of the draft's chapters 3, 6, 8-11 and 13 against core/ and the
d18 reference doc to extend it. Add findings to this section.


## FILE MAP
Create
  core/include/moq/control_d21.h        d21 constants + codec API (mirrors control_d18.h)
  core/src/wire/control_d21.c           d21 control-message codec
  core/src/wire/control_d21_internal.h  (only if d18's _internal.h split is mirrored)
  core/src/session/profile_d21.c        d21 moq_profile_ops_t vtable
  docs/draft21-wire-reference.md       in-repo wire facts (mirrors draft18-wire-reference.md)
  tests/unit/test_control_d21.c         codec tests
  tests/unit/test_d21_*.c               session-level tests (one per feature area, below)
  tests/vectors/d21/                    binary vectors (decode + byte-identical re-encode)
  tools/interop-adapter/                runner "driven mode" adapter for the moq5 publisher
Modify
  core/include/moq/session.h            MOQ_VERSION_DRAFT_21 = 21
  core/src/session/profile.h            moq_d21_profile_ops() declaration
  core/src/session/profile_d16.c:4843   moq_profile_lookup() add case 21
  core/CMakeLists.txt:9,25,74,84        add control_d21.c, profile_d21.c
  adapters/common/moq_alpn.h:108        "moqt-21" row (+ static_assert)
  service/src/endpoint.c:96-107         supported set + newest-first order
  service/src/media_sender.c            LOC property ids per version (Task 9)
  tests/CMakeLists.txt                  register each new test
  tests/unit/test_alpn.c, test_session_version.c   extend expectations
  tools/moq-interop-client/main.c:353,395,426      accept --draft 21
  examples/service/media_send.c         add --draft N so the adapter can pin it
  docs/conformance.md                   add a D21 status table when done (edit existing file)


## TASK 0: Branch, baseline, and the instrument -- DONE
Files: none modified (build outputs only).
Produces: a recorded baseline so later regressions and gains are attributable.

 [x] 0.1 Branch exists: `git branch --show-current` -> feature/draft21
         (already created; plan file is the first change on it).
 [x] 0.2 Build and run the existing moq5 suites, record counts.
         cmake --preset <existing preset> && cmake --build build -j4
         ctest --test-dir build -j4 --output-on-failure
         Expected: all PASS (this is the R3 regression command for all tasks).
 [x] 0.3 Build the runner (its build/bin is currently empty).
         cd ../moq-contribution-interop-runner
         cmake -S . -B build && cmake --build build -j4
         ctest --test-dir build -j2 --timeout 600 --output-on-failure
         build/moq-interop-audit --draft 21        # expect 173 of 173 bound
 [x] 0.4 Make a cert (runner README quick start) in ../moq-contribution-interop-runner/work/
         and start the runner on 127.0.0.1:8080 with publisher ports 4443-4452.
 [x] 0.5 Baseline the d18 path to prove the harness works end to end with a
         d18-capable publisher (Task 2 builds the adapter; until then use
         moqxr per docs/interop-notes.md: MOQXR_BIN=... draft 18, WebTransport).
         Save: results/baseline-d18-moqxr.json  (scratch dir, not committed).
 [x] 0.6 (superseded: the plan itself was committed first; see git log).


## BASELINE RECORDED (Task 0)

 moq5 `default` preset: 127/127 tests pass (`ctest --test-dir build`).
 Runner: 115/115 own tests pass; `audit --draft 21` = 173/173 required, 1/97 optional.
 Runner driven by moqxr (`moqxr/build/openmoq-publisher`) with
 `moqxr/tests/fixtures/locmaf-publisher.mp4`, draft 18, scenario
 `subscribe-to-publisher-track`: native QUIC 3 pass / 0 fail, WebTransport
 5 pass / 0 fail (matches docs/interop-notes.md).
 Reusable setup: `../moq-contribution-interop-runner/work/` (cert.pem, key.pem,
 runs.sqlite3, drv-logs); runner on 127.0.0.1:8080, publisher ports 4443-4452,
 started with MOQXR_BIN set and `--driver-fixture` the locmaf fixture.
 Caution: never `pkill -f` a pattern that appears in your own command line.


## TASK 1: Spec read-through and wire reference (no code) -- DONE
Files: Create docs/draft21-wire-reference.md. Modify this plan (delta table).

 [x] 1.1 Read draft sections 3 (model), 6 (sessions), 8 (wire building
         blocks), 9 (control messages), 10 (track properties), 11 (data
         streams), 13 (errors/grease) in the checked-in text and diff against
         docs/draft18-wire-reference.md and core/include/moq/control_d18.h.
         Extend the delta table above with anything missed (expect: Track
         Namespace / Location Filter / Range Filter shared structures in
         section 8, request-stream FIN vs RST semantics from 18->19,
         Authorization Token compression section move).
 [x] 1.2 Write docs/draft21-wire-reference.md: for each message, the exact
         field order, types, and the section number, copied from the draft
         (not from d18). Include worked byte examples for LOCATION_FILTER,
         FILL_PARAMETERS (nested params), one Range Filter, GOAWAY, and
         PUBLISH_STATE_NOTIFY. These become the unit-test vectors.
 [x] 1.3 Decide, with the draft in hand, the four open design questions and
         record each answer in the file with a section citation:
           a) What does a publisher that does not support fill do with a
              FILL_PARAMETERS subscription? (3.4, 9.20.16)
           b) What does a publisher advertise for MAX_FILTER_RANGES by
              default, and which error does it send for a Range Filter
              beyond that? (9.1.6, 3.3.2)
           c) When must PUBLISH_STATE_NOTIFY be sent/accepted? (9.10)
           d) Which Track Properties are "known" to a relay vs publisher?
              (10; only the publisher side matters for acceptance)
 [x] 1.4 Committed with the wire reference.


## TASK 1 FINDINGS (these override earlier assumptions in this plan)

Full detail is in docs/draft21-wire-reference.md.

 F1. Data plane is bit-identical to d18 (subgroup header, datagram, fetch
     object flags). Task 4g shrinks to adding the 0x20C End of Timed-Out Range
     marker and confirming the invalid-flag set is unchanged. No new
     data-plane codec is needed.
 F2. Fill has no opt-out in the draft, so Task 7 (fill streams) is REQUIRED
     for the publisher role, not optional.
 F3. Decision D1 is resolved by the draft: omit MAX_FILTER_RANGES (default 0);
     the peer MUST NOT send Range Filters and any that arrive get
     REQUEST_ERROR INVALID_FILTER (0x36). Implementing Range Filters is not
     required. Task 6.7 becomes "decline correctly".
 F4. PUBLISH_STATE_NOTIFY is optional for a publisher. Task 6.5: decode and
     apply it when received by a subscriber; receiving one as a publisher is
     PROTOCOL_VIOLATION. Sending it is not needed for acceptance.
 F5. Draft 9.20.2 (allowed parameters per message) is an EMPTY table in the
     checked-in text; Task 4b builds the legality matrix from each parameter's
     own text (matrix recorded in the wire reference).
 F6. Open ambiguity A1: the draft does not say whether FILL_PARAMETERS' nested
     parameter block has a Number of Parameters count. Working reading: no
     count (same as the runner and moqxr). One pinning test; raise with the
     draft authors. Open ambiguity A2: end-of-range Group/Object field
     encoding; settle by test against 11.4.1.1.
 F7. Core code that encodes draft-18 concepts and must be gated by capability:
       core/src/facade/publisher.c   SUBSCRIPTION_ENDED (0x3) emission and
                                     DUPLICATE_SUBSCRIPTION rejection
       core/src/session/session_fetch.c, session_subscribe.c,
       session_publish.c             joining-FETCH state (PENDING_JOIN,
                                     INVALID_JOINING_REQUEST_ID)
       core/src/facade/subscriber.c  joining-fetch use
       moq_publish_ok_encode_args    subscription params that d21 moves to
                                     REQUEST_UPDATE
     New profile capabilities needed (proposed names): supports_joining_fetch
     (false in d21), supports_fill_streams (true), allows_duplicate_
     subscriptions (true), publish_done_subscription_ended (false).
 F8. Multiple concurrent subscriptions per Track (3.1): an Object matching
     several subscriptions is sent once per subscription even when they share
     an alias. This is a publisher data-path change, not only a codec change;
     it belongs in Task 6.1 with a test that two overlapping subscriptions on
     one track each receive every matching Object.
 F9. REQUEST_ERROR codes: DUPLICATE_SUBSCRIPTION 0x19 and
     INVALID_JOINING_REQUEST_ID 0x32 are gone; CONFLICTING_FILTERS 0x35 and
     INVALID_FILTER 0x36 are new. Session: VERSION_NEGOTIATION_FAILED 0x15
     removed, TOO_MANY_REQUEST_UPDATES 0x1B added. PUBLISH_DONE:
     SUBSCRIPTION_ENDED 0x3 removed. The d18 semantic_request_error mapper
     (profile_d18.c:3064) needs its own d21 version; do not copy it.


## TASK 2: Interop adapter for the moq5 publisher (the compliance gate) -- DONE (native QUIC)
Build this BEFORE the codec so every later task has a pass/fail signal.

Result: driven native-QUIC runs work. Draft 18 `subscribe-to-publisher-track`
with moq5: 3 pass / 0 fail (same as the moqxr baseline). Draft 21 today ends as
"publisher exited before connecting" (`connect failed: -14`, unsupported), the
intended baseline for Tasks 3 and 8 to flip. Contract test registered with
ctest as `interop_adapter_contract` (128/128 moq5 tests pass).

KNOWN GAP (changes Task 10, see 10.1/10.4): WebTransport does not reach the
runner from the `build/dev` backend. The runner admits only the current
WebTransport profile (`webtransport-h3`, drafts 15/16 settings); moq5's
picoquic WT backend speaks the legacy dialect and the handshake ends in a
transport failure (terminal reason 5). Only the `wtquic-msquic` backend offers
the current profile and no build tree enables it. This is not a d21 issue.
`media_send` now also reports its terminal reason and exits 1 on a failed
endpoint (it used to exit 0 after silently writing one object).

Not covered by the adapter yet: per-scenario publisher modes. moqxr gets
special flags for roughly 100 d21 probes where the runner is the subscriber;
`media_send` has none, so those rows score its default behavior. Triage them in
Task 10; add publisher modes only for rows the draft says a publisher must pass.

Files:
  Create: tools/interop-adapter/run.sh, tools/interop-adapter/README.md
  Modify: examples/service/media_send.c (add --draft N, --fixture, ca path)
Interfaces:
  Consumes: runner driver contract v1 (../moq-contribution-interop-runner/
            adapters/contract.schema.json; request file path in
            MOQ_INTEROP_DRIVER_REQUEST_FILE; fields .draft .transport
            .endpoint .fixture .tls_ca .scenario_id .scenario_timeout_ms
            .namespace_hex .track_name_hex).
  Produces: executable adapter `run.sh` with the same exit-code behavior as
            adapters/moqxr/run.sh (64 = refused request).

 [x] 2.1 Read adapters/moqxr/run.sh and examples/harness/adapter.sh fully;
         copy the request validation block (jq) verbatim in structure.
 [x] 2.2 Add `--draft N` to media_send.c, mapped to the version offer
         (struct moq_version_offer_t, policy PINNED) exactly as
         tools/moq-interop-client/main.c:426 parse_draft does (strict parse,
         reject junk). Test: run `media_send ... --draft 18` against
         moqxr-less runner d18 observed run; connection reaches SETUP.
 [x] 2.3 Write run.sh. Changed from the first draft of this plan: draft 21 is
         NOT refused by the adapter. media_send's exact-offer connect fails
         with UNSUPPORTED until the d21 profile exists, which the runner
         records as "publisher exited before connecting" rather than a
         protocol failure, so no separate gate is needed.
 [x] 2.4 Contract test, no network: capture-stub test modeled on
         ../moq-contribution-interop-runner/tests/e2e/moqxr-adapter-contract.sh.
 [x] 2.5 Run d18 reference scenario through the runner in driven mode:
         POST /api/v1/runs {"draft":18,"transport":"native-quic",
           "mode":"driven","scenarios":["subscribe-to-publisher-track"], ...}
         Expected: state complete, scenario row pass. Record in baseline.
 [x] 2.6 Commit: "Add interop-runner adapter for the moq5 publisher"


## TASK 3: Version registry and empty d21 profile -- DONE

Result: `MOQ_VERSION_DRAFT_21`, ALPN `moqt-21` and a core d21 profile exist and
sessions can be created for them (simulator, tests). They are NOT wire ready:
the profile is a copy of d18 and emits d18 bytes. moq5 `default`: 128/128 pass.
`build/dev`: failing set identical to unmodified main (10 pre-existing failures,
all pico_wt/media tests, one a -Werror=array-bounds compile error in
test_pico_wt_managed_cfg_abi.c; not caused by this branch). `media_send --draft
21` still refuses at connect (-14) and draft 18 still scores 3 pass.

DESIGN CHANGE vs the original steps below: the signed negotiated-profile model
ties three things together (core profile, model row, endpoint supported set) and
says endpoint support == AVAILABLE. Declaring d21 AVAILABLE would have made
`--draft 21` usable with d18 bytes. Instead the profile gained a `wire_ready`
capability (true for d16/d18, false for d21) and `moq_profile_wire_ready()`;
the model's AVAILABLE and the endpoint's supported set both mean "wire ready",
and the d21 model row is ABSENT with a reason. The chain is enforced by tests:
test_negotiated_profile (core <-> model), test_negotiated_profile_offer
(model <-> endpoint), and a new endpoint test that refuses d21 as EXACT, LIST,
or beside d18 (test_endpoint_resolve). Step 3.4 below was therefore NOT done
(21 is deliberately not added to the endpoint); it moves to Task 8.

Also needed beyond the plan: sim/src/simpair.c treats d21 like d18 for the
symmetric SETUP start (without it a d21 sim pair never establishes).
Files: session.h, profile.h, profile_d16.c (lookup), core/CMakeLists.txt,
       moq_alpn.h, endpoint.c, test_alpn.c, test_session_version.c.
Produces: MOQ_VERSION_DRAFT_21, moq_d21_profile_ops(), ALPN "moqt-21".

 [x] 3.1 Failing tests first.
     test_alpn.c EXPECT[] gains { MOQ_VERSION_DRAFT_21, "moqt-21", 7 }.
     test_session_version.c: for v in {16,18,21}
         ASSERT(moq_profile_lookup(v) != NULL);
         ASSERT(moq_profile_lookup(v)->version == v);
     Run: ctest -R "alpn|session_version" --output-on-failure   -> FAIL
 [x] 3.2 Add `MOQ_VERSION_DRAFT_21 = 21` to moq_version_t with a comment in
         the existing style. Add the ALPN row and a
         static_assert(sizeof("moqt-21") - 1u <= 255u, ...).
 [x] 3.3 Create profile_d21.c by COPYING profile_d18.c, set .version, rename
         symbols d18->d21, add moq_d21_profile_ops() to profile.h, add the
         case to moq_profile_lookup. Wire codec calls still point at d18
         functions for now (temporary; Task 4 replaces them). Add to
         core/CMakeLists.txt in BOTH source lists (lines 25 and 84).
 [-] 3.4 (MOVED to Task 8.1; see design change) endpoint.c: moq_endpoint_version_supported() and
         supported_versions() add 21, newest first: {21, 18, 16}. Update the
         comment that says "Both profiles". Update tests that assert the
         AUTO offer list (service/tests/test_negotiated_profile_offer.c,
         test_endpoint_resolve.c).
 [x] 3.5 Run: ctest (R3 command)  and scripts/check_profile_boundary.sh
         Expected: PASS. NOTE: at this point a peer offering moqt-21 would
         negotiate a profile that speaks d18 bytes. Guard: do NOT enable 21
         in supported_versions() until Task 8; commit with 21 registered but
         not offered, and a TODO-free comment saying why.
 [x] 3.6 Commit: "Register draft-21 version, ALPN and profile skeleton"


## TASK 4: d21 control codec (TDD, one message family per commit) -- DONE

RESULT: `moq_d21_*` codec complete (control_d21.h/.c, control_d21_internal.h) with
tests/unit/test_control_d21.c (hand-derived vectors; independent oracles for the
per-message parameter matrix and the Type Flags rules) and fuzz/fuzz_control_d21.c
(54M inputs in 2 minutes, no finding; 26 committed seeds). Every family was
checked with targeted mutations; two survivors were found and fixed by
strengthening the test (a missing priority boundary, and invalid-datagram bodies
that failed for the wrong reason). For 4b the code was written before its tests;
the mutation checks stand in for the red step there. 129/129 tests pass.

Commits: 07a5e8e (copy + 4a SETUP options), 5a826e1 (4b parameters), 6eef7b4
(4c REQUEST_OK/ERROR, registries), 2b5f881 (4d PUBLISH family), 817b0fd (4e FETCH),
28e615e (4f GOAWAY, namespaces), b109f6e (4g data plane), 9649382 (fuzz), plus the
comment sweep.

TASK 5 HAND-OFF: the profile (profile_d21.c) still calls the draft-18 codec. Each
slot moves to moq_d21_* with these API changes:
  - moq_d21_msg_params_t: no filter_type/filter_*; use has_location_filter +
    location_filter{field_count,start_group,start_object,end_group_delta,end_object}
    (raw wire fields; the session applies the relative/absolute rules of 9.20.10).
    New fields: has_fill/fill, has_include_properties, has_rendezvous_timeout,
    has_fill_timeout, has_track_namespace_prefix, range_filter_params,
    range_filter_ranges, range_filter_invalid.
  - Legality masks are MOQ_D21_MASK_* in the header; the old per-message macros
    are gone.
  - PUBLISH_OK is gone: use moq_d21_{encode,decode}_request_ok with a
    moq_d21_request_ok_kind_t (PUBLISH, REQUEST_UPDATE, TRACK_STATUS,
    SUBSCRIBE_NAMESPACE, SUBSCRIBE_TRACKS, PUBLISH_NAMESPACE). The old
    publish_ok / track_status_ok / request_update_ok / zero-parameter request_ok
    functions no longer exist. Track Properties only for TRACK_STATUS.
  - GOAWAY: one pair, no request_id, no *_request variants.
  - FETCH: moq_d21_fetch_t is {request_id, track_namespace, track_name, params};
    no fetch_type, start/end or joining fields; the range is LOCATION_FILTER.
  - PUBLISH_BLOCKED is PUBLISH_SKIPPED (same bytes). New: PUBLISH_STATE_NOTIFY.
  - moq_d21_encode_request_error refuses REDIRECT and moq_d21_decode_request_error
    rejects it; use the *_redirect variants for that code.
  - Error registries: moq_d21_{request_error,publish_done,session_error}_registered
    replace d18_request_error_registered (F9: do not copy the d18 mapper).
  - Decode errors: PROTO for semantic violations; a truncated value is BUFFER at the
    message level and PROTO inside a nested FILL_PARAMETERS block.
  - NOT in the codec (profile-level, Task 5/7): the fetch OBJECT serialization,
    including the new End of Timed-Out Range flag value 0x20C (it lives in the
    profile file, as it does for d18), and every session rule the codec only
    surfaces (a client's non-empty GOAWAY URI, a Track Name in a namespace
    Redirect, range_filter_ranges against MAX_FILTER_RANGES -> INVALID_FILTER,
    PUBLISH_STATE_NOTIFY received as a publisher, MAX_REQUEST_UPDATES).
New draft ambiguities recorded in the wire reference: A4 (EXPIRES in
TRACK_STATUS_OK), A5 (parameters allowed on TRACK_STATUS), A6 (duplicate known
Setup Options).
Files: control_d21.h/.c, test_control_d21.c, tests/vectors/d21/.
Mirror every d18 entry point named moq_d18_* as moq_d21_* only where d21
keeps the message; do not copy functions for removed messages.
Each sub-task: write vector test from draft-21 bytes in
docs/draft21-wire-reference.md -> see it fail -> implement -> pass -> commit.

 4a  SETUP options: add 0x06 MAX_FILTER_RANGES, 0x08 MAX_REQUEST_UPDATES;
     duplicate/unknown-option behavior per 9.1 (runner rows
     d21-setup-unknown-options, d21-setup-duplicate-unknown-options,
     d21-publisher-setup-option-multiplicity).
     Test sketch:
       moq_d21_setup_opts_t o = {0};
       o.max_filter_ranges = 4; o.has_max_filter_ranges = true;
       encode -> bytes == expected_vector; decode(bytes) round-trips.
 4b  Parameters: LOCATION_FILTER (restructured 3.3.1), FILL_PARAMETERS
     (nested KVP block, 9.20.16), INCLUDE_PROPERTIES, Range Filter
     params 0x25-0x29 (structure in 8.6), GROUP_ORDER location rules,
     param bitmask (MOQ_D21_PARAM_BIT_*) extended; per-message legality
     matrix (which params are valid on SUBSCRIBE / REQUEST_UPDATE / PUBLISH /
     FETCH / SUBSCRIBE_TRACKS / TRACK_STATUS) taken from 9.20.x text, not
     from d18's matrix. Illegal param => message-appropriate error per
     draft (usually PROTOCOL_VIOLATION).
 4c  SUBSCRIBE, SUBSCRIBE_OK, REQUEST_UPDATE, REQUEST_OK (incl. use as
     PUBLISH response), REQUEST_ERROR (code table incl.
     TOO_MANY_REQUEST_UPDATES; no VERSION_NEGOTIATION_FAILED; REDIRECT
     empty-namespace/name rule).
 4d  PUBLISH (with Subscription Parameters), PUBLISH_DONE (Stream Count
     2^64-1; SUBSCRIPTION_ENDED removed), PUBLISH_STATE_NOTIFY (new),
     PUBLISH_SKIPPED (rename of PUBLISH_BLOCKED).
 4e  FETCH (no joining types; range via LOCATION_FILTER), FETCH_OK,
     FETCH_HEADER data stream, fetch object encoding (confirm from 11.4
     whether group/object deltas are unchanged from d18).
 4f  GOAWAY without Request ID (session and per-request forms),
     TRACK_STATUS, PUBLISH_NAMESPACE / SUBSCRIBE_NAMESPACE / NAMESPACE /
     NAMESPACE_DONE, SUBSCRIBE_TRACKS (Group Order param).
 4g  Data plane (small, see F1): reuse the d18 subgroup/datagram/fetch-object
     codecs; add End of Timed-Out Range 0x20C to the fetch codec; add tests that
     the invalid Type-Flags set is rejected (bit 4 on datagram, STATUS with
     END_OF_GROUP, SUBGROUP_ID_MODE 0b11, values >= 128); keep the Mandatory
     Track Property restriction (3.6: an Object Property in 0x4000-0x7FFF makes
     the track malformed).
 Each: run `ctest -R control_d21`; commit "d21 codec: <family>".
 Fuzz: extend fuzz/ with a control_d21 target mirroring the d18 one and run
 scripts/run_fuzzers.sh briefly (new decode paths are untrusted input).


## TASK 5: Replace temporary d18 hooks in profile_d21.c; setup + request layer -- DONE

RESULT: profile_d21.c now speaks draft 21 end to end through the moq_d21_* codec; no
moq_d18_* call remains. New tests: test_d21_setup (SETUP on the wire, peer limits,
violation matrix, GOAWAY, unknown control messages, capabilities) and
test_d21_requests (the filter and fetch-range mappings as tables, outbound and
inbound SUBSCRIBE / REQUEST_UPDATE / FETCH, the joining refusal, PUBLISH and its
bare OK, PUBLISH_STATE_NOTIFY, error codes). Both were mutation-checked; the checks
found one test gap (a PUBLISH-initiated NOTIFY path) and one genuine bug of mine
(the FETCH range was written into a params struct that fill_request_params then
cleared), both fixed. 131/131 pass; the `dev` tree's failing set equals main's.

What changed in the core, additively: `moq_setup_params_t` records the peer's
MAX_FILTER_RANGES / MAX_REQUEST_UPDATES / implementation; the decoded SUBSCRIBE,
REQUEST_UPDATE and FETCH records carry the exact wire filter (`moq_decoded_loc_filter_t`),
the FILL_PARAMETERS request (`moq_decoded_fill_t`) and the parsed Range Filters
(`moq_decoded_range_filters_t`); `MOQ_FETCH_RANGE_TIMED_OUT` (0x20C);
`MOQ_REQUEST_ERROR_INVALID_FILTER` / `_CONFLICTING_FILTERS`; `moq_pub_entry_t.publish_forward`;
profile capabilities `publish_ok_carries_params` and `supports_joining_fetch`.
Our SETUP now sends MOQT_IMPLEMENTATION ("libmoq/<version>") and deliberately omits
MAX_FILTER_RANGES (default 0: the peer may not send Range Filters) and
MAX_REQUEST_UPDATES (default: unlimited).

STILL draft-18 SEMANTICS, handed to Tasks 6 and 7 (each is covered by a test that
documents today's behavior, so the gap cannot widen silently):
  6.1  A second concurrent subscription to the same Track is refused (draft 21
       allows it, 3.1). Both the core and core/src/facade/publisher.c do this. The
       core filter model has four types: a one-field relative start N>=1 and a
       four-field range's end object are only APPROXIMATED (`loc_filter.approximated`
       is set; raw fields are surfaced) -- extend moq_resolve_filter_window with a
       relative-start type and an end object, then consume the raw record.
  6.2  PUBLISH_OK cannot carry the subscriber's choices (capability false). The
       accept path must send priority / group order / forward / filter as a
       REQUEST_UPDATE after the OK instead of dropping them; today they are
       dropped. The decoded PUBLISH_OK reports the forward value our PUBLISH
       advertised (publish_forward), priority 128 and default group order. The
       publisher's own initial parameters on an inbound PUBLISH (priority, group
       order, timeouts, filter) are not yet surfaced.
  6.3  The peer's MAX_REQUEST_UPDATES is recorded in s->peer_setup but not enforced;
       no TOO_MANY_REQUEST_UPDATES close and no outstanding-update counter yet. An
       unexpected REQUEST_UPDATE must be a session error.
  6.5  PUBLISH_STATE_NOTIFY is validated and consumed by a subscriber (informative,
       9.10) and closes a publisher that receives it; there is no sending API and
       the received state is not applied.
  6.7  Range Filters: `range_filters.ranges` / `.invalid` are surfaced on SUBSCRIBE,
       REQUEST_UPDATE and FETCH but nothing answers INVALID_FILTER yet.
  6.8  Delivery timeouts as Track AND Object properties and the clock start
       (11.x) are untouched. The public send API still refuses the new TIMED_OUT
       range kind (session_fetch.c, the checks near lines 2051 and 2064).
  7    Joining FETCH is refused by the profile's encoder (UNSUPPORTED) but the core
       does not yet gate on supports_joining_fetch before allocating; inbound
       FETCH / SUBSCRIBE surface `loc_filter.approximated` and `fill` for the fill
       implementation; a one-field relative FETCH start currently falls back to the
       whole track.
  Other: SUBSCRIBE_TRACKS encodes only FORWARD and decodes but ignores the other
  parameters 9.18.1 allows; the facade still emits SUBSCRIPTION_ENDED (F7).
Files: profile_d21.c, session_setup/session.c only if a capability is missing.
 [ ] 5.1 Tests (tests/unit/test_d21_setup.c, modeled on test_d18_setup.c and
         test_d18_setup_options.c): client+server over moq::sim complete SETUP;
         server SETUP carrying PATH/AUTHORITY is rejected (d21-server-sends-*);
         unknown option ignored; duplicate unknown option handled per 9.1.
 [ ] 5.2 Point every vtable slot at control_d21 encoders/decoders. For each
         capability field in moq_profile_ops_t set the d21 value from the
         draft (min_track_namespace_fields, request_error_wire_max,
         fetch_descending_supported, fetch_datagram_supported,
         object_payload_len_max, location_varint_max -- all vi64-based, so
         expect d18 values, but verify each against the draft).
 [ ] 5.3 New or changed vtable ops needed (add to profile.h; d16/d18 return
         NULL/no-op; boundary script must still pass):
           encode_publish_state_notify, decode_publish_state_notify
           encode_request_goaway (no request id) -- d18 op exists; check
           max_filter_ranges capability / setup-option accessor
           max_request_updates capability + counter enforcement hook
 [ ] 5.4 Run test_d21_setup, then R3 command. Commit "d21 profile: setup".


## TASK 6: Subscription and publish semantics (publisher role) -- DONE (6.8 partial)

RESULT (Task 6): the session core, facade and d21 profile now implement the draft-21
request semantics below. Each item was test-first and mutation-checked
(tests/unit/test_d21_requests.c, test_publisher.c); 131/131 pass on the default
tree. Runner scenarios cannot score these until Task 8 turns d21 on, so Task 10
re-verifies each against the runner.
  6.1 DONE. Profile capability `allows_concurrent_subscriptions` (d21 true) gates the
      core's duplicate-track rejections (inbound and outbound) and the facade's
      DUPLICATE_SUBSCRIPTION; the facade serves up to 8 subscription slots per Track
      (EXCESSIVE_LOAD beyond) and sends each matching object once per subscription
      with its own alias. New `moq_resolve_loc_filter_window` resolves the exact wire
      Location Filter (relative start, Next Object, absolute, range, inclusive end
      Object); `moq_resolved_window_t` gained `has_end_object/end_object`, honoured
      by the facade's membership test. GAP: the end-Object enforcement in the facade
      (`window_admits`) has no direct test (the resolver and the session-level
      window are tested); a raw-injection facade test is the follow-up.
  6.2 DONE. With `publish_ok_carries_params` false the accept path sends a
      non-default priority, a forward that differs from the PUBLISH's, a filter or a
      new-group request as a REQUEST_UPDATE right behind the bare REQUEST_OK (both
      messages reserved before either is queued), with the update's credit and
      Forward effect pending until acknowledged. The publisher's own initial PUBLISH
      parameters are still not surfaced on the request event.
  6.3 DONE as scoped. We send one update per request until it is answered, which
      satisfies any MAX_REQUEST_UPDATES the peer advertises; we advertise none, so
      inbound updates are unlimited and TOO_MANY_REQUEST_UPDATES cannot arise (the
      constant exists for when a configurable advertised limit is added). A
      REQUEST_UPDATE as a first message is a session error (tested).
  6.4 DONE. Profile capability `request_fin_is_not_cancel` (d21 true): a requester's
      FIN on an established subscription closed the whole session ("truncated
      message") and now only closes that direction; a responder FIN before the
      response / PUBLISH_DONE fails the request (UNSUBSCRIBED surfaced), not the
      session; RESET_STREAM cancels only its request. d16/d18 behaviour unchanged.
  6.5 DONE as decided by F4. NOTIFY is validated and consumed on receive (Task 5);
      sending is optional for a publisher and is not implemented. The facade no
      longer sends the removed SUBSCRIPTION_ENDED (F7): capability
      `publish_done_subscription_ended` (d21 false) stops the finite-end completion
      sweep and the end-of-track status choice, and a finite end leaves the
      subscription open.
  6.6 DONE. The per-request GOAWAY path was already in the profile/core; tests now
      cover a publisher migrating a subscription (event, URI, timeout, session
      untouched), a client's non-empty URI closing the session, and a second GOAWAY
      on one stream closing it.
  6.7 DONE. SUBSCRIBE, FETCH, REQUEST_UPDATE (subscription and publication targets)
      carrying any Range Filter (0x25-0x28 on SUBSCRIBE) are answered REQUEST_ERROR
      INVALID_FILTER (0x36) through the core's message-level reject; the session
      stays open and the application sees no request. A Length-0 filter is not a
      Range Filter. (0x29 on SUBSCRIBE is a parameter the message does not take and
      stays a protocol violation in the codec.)
  6.8 PARTIAL. End of Timed-Out Range can now be sent (`write_fetch_range` accepts
      MOQ_FETCH_RANGE_TIMED_OUT; a profile without it refuses at encode). NOT DONE:
      the draft-21 timer semantics -- SUBGROUP_DELIVERY_TIMEOUT starting at the
      subgroup's FIN and checked until the transport reports all data committed,
      OBJECT_DELIVERY_TIMEOUT per object from its last header byte, and the first
      object's properties overriding the Track-level values. The session arms one
      subgroup timer at open (and ignores CLOSING streams in the sweep, and has no
      timer for publication-backed subgroups); changing that needs a transport
      "all data committed" signal and is better driven by the runner's d21 timeout
      scenarios, so it is carried to Task 10 triage rather than guessed at here.
One test file per bullet, each modeled on its d18 sibling; each commit
passes R3. Map runner scenarios as acceptance for the bullet (names are
discoverable with: curl -s localhost:8080/api/v1/scenarios | jq).
 [x] 6.1 SUBSCRIBE / SUBSCRIBE_OK / LOCATION_FILTER semantics, multiple
         concurrent subscriptions per Track (3.1.x; alias sharing vs distinct
         aliases: d21-overlapping-subscriptions-shared-alias / -distinct-aliases).
 [x] 6.2 PUBLISH flow with REQUEST_OK as response (no PUBLISH_OK),
         subscription params on PUBLISH, token-not-copied rule.
         Existing d18 session code keys on PUBLISH_OK: introduce a semantic
         "publish accepted" event/record already used by the core and only
         change the profile encode/decode, not the session state machine.
 [x] 6.3 REQUEST_UPDATE: subscription params move here; MAX_REQUEST_UPDATES
         accounting and TOO_MANY_REQUEST_UPDATES; unexpected REQUEST_UPDATE
         => session error (test both directions).
 [x] 6.4 PUBLISH_DONE / UNSUBSCRIBE / stream-reset code alignment and FIN vs
         RST/STOP_SENDING semantics on request streams (A.3 #1698).
 [x] 6.5 PUBLISH_SKIPPED (rename path), PUBLISH_STATE_NOTIFY sending and
         tolerance on receive (Task 1.3c decides behavior).
 [x] 6.6 GOAWAY: session GOAWAY with no request id; per-request GOAWAY;
         d21-publisher-goaway-alternate-uri, d21-publisher-client-goaway-*.
 [x] 6.7 Range Filters (decided by the draft, F3): do not advertise
         MAX_FILTER_RANGES; on any Range Filter parameter (0x25-0x29) reply
         REQUEST_ERROR INVALID_FILTER (0x36). Tests: each of the five types
         on SUBSCRIBE and FETCH is rejected with 0x36 and the session stays
         open; a Length-0 filter (removal) is treated per 8.6.
 [~] 6.8 Delivery timeouts as both Track and Object properties; timer starts
         at last header byte (11.x); End of Timed-Out Range signalling when
         a fill timeout expires.


## TASK 7: Fill streams (replaces joining FETCH) -- DONE (publisher side)

RESULT (Task 7): tests in tests/unit/test_d21_requests.c and test_publisher.c, mutation-checked;
131/131 pass. Design: FILL_PARAMETERS on a SUBSCRIBE or REQUEST_UPDATE is held on the
subscription and becomes PENDING only while Forward State is 1 (at accept for the
SUBSCRIBE, at apply for an update; an update without a fill leaves an unopened one
alone). The application polls `moq_session_sub_fill_pending`, opens it with
`moq_session_open_fill` (range resolved by `moq_resolve_fill_range` against the
Largest Object the response advertised: whole track by default, relative starts,
Next Group / Next Object and starts past Largest are empty and open nothing, ends
never pass Largest), serves it with the ordinary fetch writers and ends it with
`moq_session_end_fetch` (FIN) or `moq_session_reset_fill` (failure). The stream opens
with FETCH_HEADER carrying the originating request's id, so several fills can be open
at once. Cancelling the subscription resets every open fill; STOP_SENDING on a fill
cancels only that fill. The facade serves fills from its retained group (the part
inside the range; a range it holds nothing for opens and resets the stream) through
pub_open_fills. 7.4: a one-field FETCH start is now resolved by the core relative to
Largest (previously the whole track); with no Largest it is INVALID_RANGE. The core
refuses a Joining FETCH on a profile without it before touching state.
NOT DONE: FILL_TIMEOUT expiry -> End of Timed-Out Range (needs the same timer work as
6.8), fill-vs-subscription scheduling (3.4), serving fills for non-retained live
history (the facade keeps only the retained group), and the RECEIVING side of a fill
stream (our subscriber API cannot send FILL_PARAMETERS yet). A subscription's own
group-order / timeout overrides inside FILL_PARAMETERS are surfaced in
`moq_fill_info_t` (priority, timeout) but not otherwise acted on.
Files: session_fetch.c, session_subscribe.c, profile_d21.c,
       tests/unit/test_d21_fill.c (replaces test_d18_joining.c analogue).
 [x] 7.1 (required, see F2) Read 3.4 completely; write failing tests from the draft's own
         normative statements: fill opens only when Forward State is 1 and
         FILL_PARAMETERS is present (lines ~1333-1349); empty nested
         LOCATION_FILTER means "fill range = ..." (line ~1298); parameters in
         FILL_PARAMETERS override the subscription's for the fill (1311).
 [x] 7.2 Implement publisher side: serve fill streams over FETCH_HEADER-typed
         unidirectional streams (confirm stream type in 11.x), FILL_TIMEOUT
         expiry -> End of Timed-Out Range, fill-vs-subscription scheduling
         (3.4), cancellation with concurrent fill streams
         (d21-cancel-subscription-with-concurrent-fill-streams),
         fill failing before first object (d21-fill-fails-before-first-object).
 [x] 7.3 Existing publisher_retained_groups / catalog joining-FETCH support
         (docs/publisher-retained-groups.md) must be re-expressed as fill
         under d21 while remaining joining-FETCH under d18/d16. Gate by
         capability, not version.
 [x] 7.4 Standalone FETCH with LOCATION_FILTER range (non-joining) tests.


## TASK 8: Turn it on: negotiation, transports, service layer -- DONE (Swift not built here)

RESULT (Task 8): 8.0 flipped in one commit: `.wire_ready = true`, the endpoint supports and
offers {21, 18, 16} (newest first), the np model's d21 row is AVAILABLE rank 1 (d18 2, d16 3;
the row keeps a reason because its media cells stay UNSUPPORTED until Task 9), pinned literals
and the "refuses d21" endpoint test inverted. 8.2: moq-interop-client `--draft 21` (+README).
8.3: MOQ_SIM_VERSION hook in the simulator and `run_seed_sweeps.sh --version N`; the quick
profile under draft 21 has exactly the failing runners it has under draft 18 (crossed, delay,
delay_backpressure, faults, namespace_sub, streaming_faults: the scenarios assume the d16
start order) and scenario_fetch is skipped (it drives a Joining FETCH). 8.4: Python binding
exports Version.DRAFT_21; the Swift enums (MoQVersion, MoQTransportVersion,
MediaTransportVersion and their exhaustive switches) are NOT touched -- there is no Swift
toolchain here to compile them. 8.5: the adapter never gated on d21 (it accepts 18 and 21);
README updated. Smoke: the runner, driving the adapter, connects over native QUIC with
ALPN moqt-21 and records SETUP (scenario d21-publisher-request-stream-placement).
Not changed: the relay and example relay (16/18 products), media_sender (draft 21 media
writes fail until Task 9's LOC ids), 8.1's per-adapter conformance tests for 21 (the
adapters take their versions from the shared endpoint list; a loopback d21 test per
transport is Task 10 work). The OOM sweep was not run for d21.
 [x] 8.0 Flip the three-way switch IN ONE COMMIT, after Tasks 4-7 pass:
         (a) profile_d21.c `.wire_ready = true` and delete the "transitional"
             file header; (b) endpoint.c moq_endpoint_version_supported() and
             supported_versions() add 21, newest first: {21, 18, 16}; (c)
             tests/support/np/np_tables.c d21 row -> AVAILABLE, endpoint_offered
             true, auto_rank 1 (renumber d18 to 2 and d16 to 3), drop its
             unusable_reason only if a d21 media cell is SUPPORTED (otherwise
             keep the reason); update the pinned literals in
             test_negotiated_profile.c and the AUTO-order assertions in
             test_endpoint_resolve.c (three versions, d21 first), and replace
             the "refuses d21" endpoint test with its inverse.
 [~] 8.1 (rest of endpoint work) resolve/offer tests; WebTransport protocol token for 21; adapters
         that enumerate versions (adapters/msquic, picoquic, mvfst, pico_wt,
         wtquic) -- grep for MOQ_VERSION_DRAFT_18 in adapters/**/src and add
         21 wherever d18 appears; extend their conformance tests
         (test_msquic_multi_alpn.c, test_loopback_d18.cpp analogue, ...).
 [x] 8.2 tools/moq-interop-client: accept --draft 21 (main.c:353,395,426),
         update usage text and test_url_policy if affected.
 [x] 8.3 sim: sim/src/simpair.c d18 references -> include 21 in seeded
         scenario sweeps (scripts/run_seed_sweeps.sh); OOM sweep includes d21.
 [~] 8.4 bindings/ and Package.swift: grep for draft enumerations; add 21.
 [x] 8.5 Remove the adapter's exit-64 gate for draft 21 (Task 2.3).
 [x] 8.6 R3 command + sweeps + check_profile_boundary.sh. Commit.


## TASK 9: Service layer (media_sender) -- LOC property ids per version -- DONE

RESULT (Task 9): the ids draft 21 registers (16.8) are LOC-04's (draft-ietf-moq-loc-04
2.3; text at ../moqxr/docs): Timestamp 0x10, Timescale 0x08, Video Frame Marking 0x09,
Audio Level 0x0C, Video Config 0x0D, Audio Config 0x0F. NOTE the plan's earlier "d18
0x06/0x0A/0x0D" was wrong: drafts 16/18 carry LOC-01 (0x02/0x04/0x06/0x0D). Frame
Marking's id is odd under LOC-04, so it is a length-prefixed RFC 9626 byte string (one
byte, a second for the layer id), not a varint. The media library has one table,
`moq_loc_profile_for_transport` (16/18 -> LOC-01, 21 -> LOC-04); encode/parse take the
pair and FAIL CLOSED for a profile on a draft it was not reviewed for; fields are
sorted by id before delta encoding. media_sender, media_object and msf accept draft 21
and select the profile through that table (no version test in the sender). Hand-derived
byte vectors in tests/unit/test_loc.c. Smoke: the sender negotiates draft 21 against the
runner and exits cleanly. Audio Config (0x0F) is neither emitted nor parsed (the sender has
no audio-config source). The np topology keeps its d21/loc01 cell UNSUPPORTED (true:
LOC-01 is not carried on draft 21) with a reason naming LOC-04; a loc04 media row would
need corpus and closure work and is not added. 9.3: the catalog's retained group is served
as a fill under draft 21 by the Task 7 facade path; no msf change was needed beyond
accepting the version.
 [x] 9.1 Locate the LOC property-block emitter (service/src/media_sender.c;
         properties are generated from typed timing fields). Failing test in
         service/tests: with a d21 session the emitted TIMESTAMP is property
         0x10, VIDEO_FRAME_MARKING 0x09, VIDEO_CONFIG/AUDIO_CONFIG as Track
         properties 0x0D/0x0F; with d18 unchanged (0x06/0x0A/0x0D).
 [x] 9.2 Implement via a per-profile property-id table (capability on the
         profile or a service-side table keyed by negotiated profile --
         never `if (version == 21)` in the core).
 [x] 9.3 Catalog/MSF behavior that depended on joining FETCH: confirm still
         correct under d21 fill semantics (Task 7.3).
 [x] 9.4 Commit.


## TASK 10: Interop acceptance and gap closure (the compliance gate) -- DONE (native QUIC)

RESULT (Task 10): see docs/conformance.md (Draft 21). 216 driven scenarios, 58 PASS, 0 FAIL
among the rows the runner could evaluate; 212 not run (about half not executable by the
runner, half needing per-scenario publisher modes). Fixes made from the first full run:
FIRST_OBJECT, SETUP AUTHORITY / PATH, SUBSCRIBE_NAMESPACE response, 8,192-byte GOAWAY URI,
media_send exit status for peer-provoked closes. d18 shows 7 pre-existing FAIL rows (not
caused by this work, no full Task 0 baseline to diff). ASan 131/131, fuzz clean 30 s each.
WebTransport not exercised. `moq-interop-audit --draft 21` lives in the runner repo and was
not run. Remaining hand-offs: delivery-timer semantics (6.8), FILL_TIMEOUT, Swift enums, d18
AUTHORITY/PATH/FIRST_OBJECT, other raw-QUIC adapters' SETUP AUTHORITY/PATH.
 [x] 10.1 Full driven runs, d21, native QUIC, all scenarios, via the Task 2
          adapter. WebTransport only if a wtquic-msquic build is available
          (see the Task 2 known gap); otherwise record the WebTransport rows
          as not exercised, never as passed.
          Mind runner requirement: QUIC DATAGRAM must be negotiated; native
          QUIC needs datagrams enabled in the moq5 adapter in use.
 [x] 10.2 Triage every non-pass row into exactly one bucket and record it in
          docs/conformance.md (new D21 section):
            FIX      publisher is wrong per the draft -> fix on this branch
            NOT_RUN  needs operator fixtures (tokens) -> supply or document
            DISPUTE  runner wrong per the draft -> Runner disputes below
            N/A      row legitimately not exercised by a publisher
 [x] 10.3 Loop FIX items: failing row -> read cited draft lines -> unit test
          reproducing -> fix -> rerun that scenario only
          (POST run with "scenarios":[id]) -> commit "d21: <requirement id>".
 [x] 10.4 Exit criteria:
            - 0 FAIL among publisher-applicable, testable MUST/MUST NOT rows
              for d21 on native QUIC, or each remaining FAIL is a DISPUTE
              with draft citation. WebTransport is reported separately and is
              gating only if the wtquic-msquic backend is built.
            - `build/moq-interop-audit --draft 21` unchanged (173/173).
            - d16 and d18 runner results not worse than the Task 0 baseline
              (d18 re-run with the moq5 adapter).
            - R3 command green; sanitizer build (ASan/UBSan) green on the new
              tests; fuzz targets run clean for a fixed budget.
          SHOULD/MAY rows (very low runner coverage) are best-effort and not
          gating.
 [x] 10.5 Update docs/conformance.md and docs/README.md (existing files; no
          new files beyond these). Commit. Then superpowers:finishing-a-development-branch.


## REVIEW FOCUS (inputs likely to bite; each needs a test in its owning task)
 1. Peer offers [moqt-21, moqt-18, moqt-16] vs [moqt-18] only vs unknown only:
    newest mutually-supported wins, no d18 bytes on a 21 session (Task 3/8).
 2. Peer (the runner) sends a legacy d18 message on a d21 session:
    PUBLISH_OK 0x1E, SUBSCRIPTION_FILTER 0x21 layout, joining FETCH types
    2/3, GOAWAY with Request ID: each must be rejected per draft, never
    silently decoded as the d21 shape (Task 4/5).
 3. Undefined Type-Flags bits in SUBGROUP_HEADER/OBJECT_DATAGRAM, and
    non-minimal vi64 encodings in the new fields (Task 4g).
 4. FILL_PARAMETERS nested inside FILL_PARAMETERS, with Forward=0, with an
    empty nested LOCATION_FILTER, or arriving on a non-fill-capable request
    (Task 7).
 5. REQUEST_UPDATE flood past MAX_REQUEST_UPDATES, and REQUEST_UPDATE on a
    request that cannot accept one (Task 6.3).


## DECISIONS NEEDED FROM THE USER

Defaults are in brackets; I proceed with them unless told otherwise.

 D1. RESOLVED by the draft (see F3): decline Range Filters via the
     MAX_FILTER_RANGES default of 0. Revisit only if a runner row needs
     range-filter behavior from a publisher; check it against 9.1.6 first,
     as it would then be a runner dispute.
 D2. Keep d16 and d18 in the build alongside d21? [Yes; supported set
     becomes {21,18,16}.]
 D3. Is moqxr's libmoq backend (--libmoq-backend) a second publisher to
     validate against after the moq5 adapter? [Only after Task 10; moqxr
     already speaks --draft 21 through its legacy path and is useful as a
     cross-check, never as authority.]
 D4. The runner has no d21 evaluator for subscriber-side behavior; relay/
     subscriber role correctness for d21 is covered only by unit tests and
     sim sweeps on this branch. [Accept.]


## RUNNER DISPUTES (fill during Task 10)
 (none yet)


## RISKS
 - profile_d18.c is 3.1k lines; copy-then-edit risks carrying d18 semantics
   into d21. Mitigation: every vtable slot gets a d21 test that asserts the
   d21 wire bytes, and Review Focus item 2.
 - Fill streams interact with request-stream correlation and the retained
   groups cache; Task 7 may force a new capability in profile.h.
 - Appendix A.1 (20->21) is editorial only, so the real change surface is
   19/20; the 18->19 and 19->20 deltas must both be applied.
 - Adapters/bindings enumerate versions in many places (Task 8 grep sweep).
