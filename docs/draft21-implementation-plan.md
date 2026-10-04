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


## TASK 0: Branch, baseline, and the instrument
Files: none modified (build outputs only).
Produces: a recorded baseline so later regressions and gains are attributable.

 [ ] 0.1 Branch exists: `git branch --show-current` -> feature/draft21
         (already created; plan file is the first change on it).
 [ ] 0.2 Build and run the existing moq5 suites, record counts.
         cmake --preset <existing preset> && cmake --build build -j4
         ctest --test-dir build -j4 --output-on-failure
         Expected: all PASS (this is the R3 regression command for all tasks).
 [ ] 0.3 Build the runner (its build/bin is currently empty).
         cd ../moq-contribution-interop-runner
         cmake -S . -B build && cmake --build build -j4
         ctest --test-dir build -j2 --timeout 600 --output-on-failure
         build/moq-interop-audit --draft 21        # expect 173 of 173 bound
 [ ] 0.4 Make a cert (runner README quick start) in ../moq-contribution-interop-runner/work/
         and start the runner on 127.0.0.1:8080 with publisher ports 4443-4452.
 [ ] 0.5 Baseline the d18 path to prove the harness works end to end with a
         d18-capable publisher (Task 2 builds the adapter; until then use
         moqxr per docs/interop-notes.md: MOQXR_BIN=... draft 18, WebTransport).
         Save: results/baseline-d18-moqxr.json  (scratch dir, not committed).
 [ ] 0.6 Commit nothing yet; the first commit is Task 1.


## TASK 1: Spec read-through and wire reference (no code)
Files: Create docs/draft21-wire-reference.md. Modify this plan (delta table).

 [ ] 1.1 Read draft sections 3 (model), 6 (sessions), 8 (wire building
         blocks), 9 (control messages), 10 (track properties), 11 (data
         streams), 13 (errors/grease) in the checked-in text and diff against
         docs/draft18-wire-reference.md and core/include/moq/control_d18.h.
         Extend the delta table above with anything missed (expect: Track
         Namespace / Location Filter / Range Filter shared structures in
         section 8, request-stream FIN vs RST semantics from 18->19,
         Authorization Token compression section move).
 [ ] 1.2 Write docs/draft21-wire-reference.md: for each message, the exact
         field order, types, and the section number, copied from the draft
         (not from d18). Include worked byte examples for LOCATION_FILTER,
         FILL_PARAMETERS (nested params), one Range Filter, GOAWAY, and
         PUBLISH_STATE_NOTIFY. These become the unit-test vectors.
 [ ] 1.3 Decide, with the draft in hand, the four open design questions and
         record each answer in the file with a section citation:
           a) What does a publisher that does not support fill do with a
              FILL_PARAMETERS subscription? (3.4, 9.20.16)
           b) What does a publisher advertise for MAX_FILTER_RANGES by
              default, and which error does it send for a Range Filter
              beyond that? (9.1.6, 3.3.2)
           c) When must PUBLISH_STATE_NOTIFY be sent/accepted? (9.10)
           d) Which Track Properties are "known" to a relay vs publisher?
              (10; only the publisher side matters for acceptance)
 [ ] 1.4 Commit: "Add draft-21 wire reference and delta notes"


## TASK 2: Interop adapter for the moq5 publisher (the compliance gate)
Build this BEFORE the codec so every later task has a pass/fail signal.

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

 [ ] 2.1 Read adapters/moqxr/run.sh and examples/harness/adapter.sh fully;
         copy the request validation block (jq) verbatim in structure.
 [ ] 2.2 Add `--draft N` to media_send.c, mapped to the version offer
         (struct moq_version_offer_t, policy PINNED) exactly as
         tools/moq-interop-client/main.c:426 parse_draft does (strict parse,
         reject junk). Test: run `media_send ... --draft 18` against
         moqxr-less runner d18 observed run; connection reaches SETUP.
 [ ] 2.3 Write run.sh: accepts draft 18 now, draft 21 once Task 3 lands
         (gate: `[[ $draft == 18 || $draft == 21 ]]` -- ship with 21 refused
         via exit 64 until Task 3, so a missing profile is not misreported
         as a protocol failure).
 [ ] 2.4 Contract test, no network: capture-stub test modeled on
         ../moq-contribution-interop-runner/tests/e2e/moqxr-adapter-contract.sh.
 [ ] 2.5 Run d18 reference scenario through the runner in driven mode:
         POST /api/v1/runs {"draft":18,"transport":"native-quic",
           "mode":"driven","scenarios":["subscribe-to-publisher-track"], ...}
         Expected: state complete, scenario row pass. Record in baseline.
 [ ] 2.6 Commit: "Add interop-runner adapter for the moq5 publisher"


## TASK 3: Version registry and empty d21 profile
Files: session.h, profile.h, profile_d16.c (lookup), core/CMakeLists.txt,
       moq_alpn.h, endpoint.c, test_alpn.c, test_session_version.c.
Produces: MOQ_VERSION_DRAFT_21, moq_d21_profile_ops(), ALPN "moqt-21".

 [ ] 3.1 Failing tests first.
     test_alpn.c EXPECT[] gains { MOQ_VERSION_DRAFT_21, "moqt-21", 7 }.
     test_session_version.c: for v in {16,18,21}
         ASSERT(moq_profile_lookup(v) != NULL);
         ASSERT(moq_profile_lookup(v)->version == v);
     Run: ctest -R "alpn|session_version" --output-on-failure   -> FAIL
 [ ] 3.2 Add `MOQ_VERSION_DRAFT_21 = 21` to moq_version_t with a comment in
         the existing style. Add the ALPN row and a
         static_assert(sizeof("moqt-21") - 1u <= 255u, ...).
 [ ] 3.3 Create profile_d21.c by COPYING profile_d18.c, set .version, rename
         symbols d18->d21, add moq_d21_profile_ops() to profile.h, add the
         case to moq_profile_lookup. Wire codec calls still point at d18
         functions for now (temporary; Task 4 replaces them). Add to
         core/CMakeLists.txt in BOTH source lists (lines 25 and 84).
 [ ] 3.4 endpoint.c: moq_endpoint_version_supported() and
         supported_versions() add 21, newest first: {21, 18, 16}. Update the
         comment that says "Both profiles". Update tests that assert the
         AUTO offer list (service/tests/test_negotiated_profile_offer.c,
         test_endpoint_resolve.c).
 [ ] 3.5 Run: ctest (R3 command)  and scripts/check_profile_boundary.sh
         Expected: PASS. NOTE: at this point a peer offering moqt-21 would
         negotiate a profile that speaks d18 bytes. Guard: do NOT enable 21
         in supported_versions() until Task 8; commit with 21 registered but
         not offered, and a TODO-free comment saying why.
 [ ] 3.6 Commit: "Register draft-21 version, ALPN and profile skeleton"


## TASK 4: d21 control codec (TDD, one message family per commit)
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
 4g  Data plane: SUBGROUP_HEADER and OBJECT_DATAGRAM Type-Flags bitfield
     (undefined bit set => PROTOCOL_VIOLATION, 11.x), End of Timed-Out
     Range 0x20C, object properties, Mandatory Track Property restriction
     (re-check against 10.x: d18 rejected 0x4000-0x7FFF in object props).
 Each: run `ctest -R control_d21`; commit "d21 codec: <family>".
 Fuzz: extend fuzz/ with a control_d21 target mirroring the d18 one and run
 scripts/run_fuzzers.sh briefly (new decode paths are untrusted input).


## TASK 5: Replace temporary d18 hooks in profile_d21.c; setup + request layer
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


## TASK 6: Subscription and publish semantics (publisher role)
One test file per bullet, each modeled on its d18 sibling; each commit
passes R3. Map runner scenarios as acceptance for the bullet (names are
discoverable with: curl -s localhost:8080/api/v1/scenarios | jq).
 [ ] 6.1 SUBSCRIBE / SUBSCRIBE_OK / LOCATION_FILTER semantics, multiple
         concurrent subscriptions per Track (3.1.x; alias sharing vs distinct
         aliases: d21-overlapping-subscriptions-shared-alias / -distinct-aliases).
 [ ] 6.2 PUBLISH flow with REQUEST_OK as response (no PUBLISH_OK),
         subscription params on PUBLISH, token-not-copied rule.
         Existing d18 session code keys on PUBLISH_OK: introduce a semantic
         "publish accepted" event/record already used by the core and only
         change the profile encode/decode, not the session state machine.
 [ ] 6.3 REQUEST_UPDATE: subscription params move here; MAX_REQUEST_UPDATES
         accounting and TOO_MANY_REQUEST_UPDATES; unexpected REQUEST_UPDATE
         => session error (test both directions).
 [ ] 6.4 PUBLISH_DONE / UNSUBSCRIBE / stream-reset code alignment and FIN vs
         RST/STOP_SENDING semantics on request streams (A.3 #1698).
 [ ] 6.5 PUBLISH_SKIPPED (rename path), PUBLISH_STATE_NOTIFY sending and
         tolerance on receive (Task 1.3c decides behavior).
 [ ] 6.6 GOAWAY: session GOAWAY with no request id; per-request GOAWAY;
         d21-publisher-goaway-alternate-uri, d21-publisher-client-goaway-*.
 [ ] 6.7 Range Filters: either implement (OBJECTID/SUBGROUP/PRIORITY/
         OBJECT_PROPERTY/TRACK_PROPERTY) or advertise MAX_FILTER_RANGES = 0
         per Task 1.3b; if 0, a Range Filter on the wire gets the error the
         draft names. Implement-vs-decline is a user decision if the draft
         permits both (see "Decisions needed").
 [ ] 6.8 Delivery timeouts as both Track and Object properties; timer starts
         at last header byte (11.x); End of Timed-Out Range signalling when
         a fill timeout expires.


## TASK 7: Fill streams (replaces joining FETCH)
Files: session_fetch.c, session_subscribe.c, profile_d21.c,
       tests/unit/test_d21_fill.c (replaces test_d18_joining.c analogue).
 [ ] 7.1 Read 3.4 completely; write failing tests from the draft's own
         normative statements: fill opens only when Forward State is 1 and
         FILL_PARAMETERS is present (lines ~1333-1349); empty nested
         LOCATION_FILTER means "fill range = ..." (line ~1298); parameters in
         FILL_PARAMETERS override the subscription's for the fill (1311).
 [ ] 7.2 Implement publisher side: serve fill streams over FETCH_HEADER-typed
         unidirectional streams (confirm stream type in 11.x), FILL_TIMEOUT
         expiry -> End of Timed-Out Range, fill-vs-subscription scheduling
         (3.4), cancellation with concurrent fill streams
         (d21-cancel-subscription-with-concurrent-fill-streams),
         fill failing before first object (d21-fill-fails-before-first-object).
 [ ] 7.3 Existing publisher_retained_groups / catalog joining-FETCH support
         (docs/publisher-retained-groups.md) must be re-expressed as fill
         under d21 while remaining joining-FETCH under d18/d16. Gate by
         capability, not version.
 [ ] 7.4 Standalone FETCH with LOCATION_FILTER range (non-joining) tests.


## TASK 8: Turn it on: negotiation, transports, service layer
 [ ] 8.1 endpoint.c: enable 21 in supported_versions() (newest first), update
         resolve/offer tests; WebTransport protocol token for 21; adapters
         that enumerate versions (adapters/msquic, picoquic, mvfst, pico_wt,
         wtquic) -- grep for MOQ_VERSION_DRAFT_18 in adapters/**/src and add
         21 wherever d18 appears; extend their conformance tests
         (test_msquic_multi_alpn.c, test_loopback_d18.cpp analogue, ...).
 [ ] 8.2 tools/moq-interop-client: accept --draft 21 (main.c:353,395,426),
         update usage text and test_url_policy if affected.
 [ ] 8.3 sim: sim/src/simpair.c d18 references -> include 21 in seeded
         scenario sweeps (scripts/run_seed_sweeps.sh); OOM sweep includes d21.
 [ ] 8.4 bindings/ and Package.swift: grep for draft enumerations; add 21.
 [ ] 8.5 Remove the adapter's exit-64 gate for draft 21 (Task 2.3).
 [ ] 8.6 R3 command + sweeps + check_profile_boundary.sh. Commit.


## TASK 9: Service layer (media_sender) -- LOC property ids per version
 [ ] 9.1 Locate the LOC property-block emitter (service/src/media_sender.c;
         properties are generated from typed timing fields). Failing test in
         service/tests: with a d21 session the emitted TIMESTAMP is property
         0x10, VIDEO_FRAME_MARKING 0x09, VIDEO_CONFIG/AUDIO_CONFIG as Track
         properties 0x0D/0x0F; with d18 unchanged (0x06/0x0A/0x0D).
 [ ] 9.2 Implement via a per-profile property-id table (capability on the
         profile or a service-side table keyed by negotiated profile --
         never `if (version == 21)` in the core).
 [ ] 9.3 Catalog/MSF behavior that depended on joining FETCH: confirm still
         correct under d21 fill semantics (Task 7.3).
 [ ] 9.4 Commit.


## TASK 10: Interop acceptance and gap closure (the compliance gate)
 [ ] 10.1 Full driven runs, d21, both transports (the runner supports both):
          native-quic and webtransport, all scenarios, via the Task 2 adapter.
          Mind runner requirement: QUIC DATAGRAM must be negotiated; native
          QUIC needs datagrams enabled in the moq5 adapter in use.
 [ ] 10.2 Triage every non-pass row into exactly one bucket and record it in
          docs/conformance.md (new D21 section):
            FIX      publisher is wrong per the draft -> fix on this branch
            NOT_RUN  needs operator fixtures (tokens) -> supply or document
            DISPUTE  runner wrong per the draft -> Runner disputes below
            N/A      row legitimately not exercised by a publisher
 [ ] 10.3 Loop FIX items: failing row -> read cited draft lines -> unit test
          reproducing -> fix -> rerun that scenario only
          (POST run with "scenarios":[id]) -> commit "d21: <requirement id>".
 [ ] 10.4 Exit criteria:
            - 0 FAIL among publisher-applicable, testable MUST/MUST NOT rows
              for d21 on native QUIC and WebTransport, or each remaining FAIL
              is a DISPUTE with draft citation.
            - `build/moq-interop-audit --draft 21` unchanged (173/173).
            - d16 and d18 runner results not worse than the Task 0 baseline
              (d18 re-run with the moq5 adapter).
            - R3 command green; sanitizer build (ASan/UBSan) green on the new
              tests; fuzz targets run clean for a fixed budget.
          SHOULD/MAY rows (very low runner coverage) are best-effort and not
          gating.
 [ ] 10.5 Update docs/conformance.md and docs/README.md (existing files; no
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

 D1. Range Filters (6.7): implement all five, or advertise
     MAX_FILTER_RANGES=0 and decline? [Decline first; implement only if the
     runner's range-filter rows cannot otherwise be scored.]
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
