# Scenario ownership map

Which registered test owns each behavioural claim, and — for claims whose
original carrier has been retired — which tests inherited it.

`check_test_inventory.sh` reads both tables mechanically:

- nothing in RETIRED may be registered again, and its build target must be
  gone unless the row says `retained` — which it may only say because another
  registered test runs that same executable;
- every successor named in RETIRED must still be a registered CTest;
- every test named in REGISTERED OWNERS must still be registered, so a claim
  cannot lose its last owner silently.

## Retired scenarios

| retired scenario | build target | claim it carried | successor owners |
|---|---|---|---|
| `relay_loopback_diag` | removed | a lane binding drains to zero connections before the relay stops, and a frozen drain can name whether the transport terminal or the application acknowledgment is missing | `relay_lane_lifecycle` `relay_pump_branches` `relay_lane_boundary` |
| `relay_loopback` | retained | the full 35-scenario real-QUIC sweep: forwarding on both drafts, lane placement, drain-to-zero over real transport | `relay_loopback_same_d18` `relay_loopback_cross_d18` `relay_lane_lifecycle` `relay_pump_branches` `relay_mixed_draft` |
| `relay_terminal_ack` | removed | an admission-refused orphan is drained, acknowledged and reclaimed by the retirement pass alone, and an acknowledgment attempted before the terminal transfers is refused | `relay_lane_lifecycle` `relay_lane_boundary` `relay_pump_branches` |

### Why each retirement holds

**`relay_loopback_diag`** was a same-source twin of `relay_loopback` linked
against the managed adapter's testing gate. It classified a stall rather than
pinning a behaviour, so it could only ever report; it never failed for a reason
another test could not state.

**`relay_loopback`** (the unselected registration) ran every scenario in the
binary in one 143-second process. The executable is retained: the two thin
selectors run individual scenarios out of it through the same functions with
the same arguments, and `run_lanes_case` ends by stopping both peers,
`moqr_drain_to_count(relay, 0, 200)` and asserting `conn_count == 0` — so the
physical drain-to-zero claim still runs, under a 60-second bound, on real
transport. What the monolith added beyond that was repetition.

**`relay_terminal_ack`** carried two scenarios and neither is now unique:

- *orphan reclaimed by the retirement pass alone* is
  `relay_lane_lifecycle::t_orphan_refused_then_reclaimed`, sans-I/O and with
  the arm decision visible between stages;
- *poll-then-ack ordering is load-bearing* is
  `relay_lane_lifecycle::t_orphan_terminal_before_first_pump`, which pins that
  an acknowledgment attempted before that pass's own poll strands the child;
- *drain to zero, classified* is the retired diag's claim, owned as above;
- the adapter seam it read (`moq_msq_test_lane_reapable`) is exercised
  LibMoQ-side by `test_msquic_terminal_ack.c` and
  `test_msquic_reap_fairness.c`, which own the acknowledgment contract at its
  source.

## Registered owners

Named here so the checker fails if a claim's last owner disappears.

| claim | owner |
|---|---|
| lane/child reclamation, orphan retirement, poll-then-ack ordering | `relay_lane_lifecycle` |
| every production nonzero-return branch of the lane pumps | `relay_pump_branches` |
| fail-closed operands of the retirement pass | `relay_lane_boundary` |
| real-transport forwarding and drain-to-zero, same-lane and cross-lane | `relay_loopback_same_d18` `relay_loopback_cross_d18` |
| cross-draft terminal translation on decoded peer wire | `relay_mixed_draft` |
| the retirement pass is wired into every pump | `relay_reap_wiring` |
| the bounded HTTP request grammar and the decided content-negotiation table, the finite response table, and the rule that no error response carries a metrics body | `relay_http_parse` |
| the bounded validating JSON writer: complete-UTF-8 validation with whole-document refusal, the declared escaping, exact-fit and one-byte-short capacity, bounded-array acquisition, and the counting writer that measures a bound | `relay_admin_json` |
| the serve's stdout sink: the frozen pre-refactor text baseline byte-for-byte, JSON events checked through an independent parser with exact values, key parity with the text rows, the latching JSON stream under scripted short writes and flush failures, the nullable monotonic clock with baseline and last-accepted-sample rules, and the once-only diagnostics | `relay_log_sink` |
| both serve paths emit every readiness, operating-point, attribution and stop record through the serve log, initialise it before the ceiling gate, route the ceiling prose to its prose stream, finalize it last before the ordinary return, keep it a function local, and (verify/measure) refuse a JSON serve between the config load and the preflight | `relay_log_sink_wiring` |
| the WebTransport object's key boundary and Origin authorization config: a key matches on complete decoded bytes at the root and inside the object, every recognized key and the root object appear at most once, closed profile and policy vocabularies compare on length and bytes, embedded NULs are refused in keys, values, credential paths and Origin entries, the allowlist's count/length/total limits and byte-exact duplicate rule hold, entries are owned by the config and outlive the document, cross-field rules are decided after the whole object is read so field order cannot change the verdict, and no refusal echoes the rejected value | `relay_dual_listener` |
| what the WebTransport transport is handed at create: the real sized initializer runs with the full struct size on the object create receives, and the captured configuration pins allocator, server perspective, address, credentials, path, ordered subprotocols, request capacity, streaming flag, the single profile, this listener's own lane and connection share, pump and context identity, and an Origin list that travels with the allowlist policy and is NULL under every other one, with the transport's own result returned unmapped | `relay_wt_cfg` |
| what counts as the verify seam's single-lane refusal: the specific diagnostic AND its exit status together, matched as a whole fixed line, so an unrelated failure from the same tool, the right message at the wrong status, and a successful capacity report that merely describes a term "at lanes > 1" are each not that refusal | `relay_verify_refusal_selftest` |
| build capability is one decision for every command: a binary compiled without WebTransport support refuses a configured `webtransport` object from both `capacity` and `serve`, with exit 2 and nothing published on stdout, while supported raw configurations still succeed at one and several lanes, ordinary semantic errors still refuse as themselves, and `--help`/`--version` are answered without reading a configuration | `relay_coord_seam` |
| when the WebTransport create-boundary test target exists at all: generated only with tests enabled AND the facade available, never in a tests-disabled default build, while the production step stays in the command either way | `relay_wt_cfg_target_gating` |
| every relay executable names each static archive once on its generated link line, so the linker never reports a duplicate library, under both the Makefile and Ninja generators | `relay_link_hygiene` |
| the link gate's own verdicts: a quoted archive path (as CMake writes one containing a space) is examined and not skipped, an unparsable command and a response-file argument fail rather than pass, an unsupported generator is refused, and CMake-generated redundant/clean link commands are judged correctly for both supported generators | `relay_link_hygiene_selftest` |
| the same link-gate cases under the system shell, so the gate keeps working on the interpreter floor (macOS ships Bash 3.2 as `/bin/bash`) that no other lane covers for these scripts | `relay_link_hygiene_selftest_system_shell` |
| the verify and measure binaries refuse a JSON-logging serve by name with nothing on stdout and exit 2, and their capacity subcommand is unaffected | `relay_serve_log_refusal` |
| the admin response state machine: generation identity, exact-once release tokens, reserve/render/commit, partial reads and writes, read/epoch/write/retention deadlines, cancellation before and during a response, and fixed client capacity | `relay_admin_sm` |
| the admin tier public contracts: no cast-away const, the retirement phase vocabulary, the post-release boundary for bank releases, and the absence of retired mechanisms in the documented API | `relay_admin_contracts` |
| the admin tier driven by the real broker: concurrent joiners share one collecting serial, a post-freeze request opens the next, two ready generations never cross-deliver, and every broker token is released exactly once | `relay_admin_broker` |
| the admin listener boundary over real loopback sockets: the finite response table on the wire, one owner thread, the one bounded refusal slot, no lane wake after terminality, and an all-or-nothing start | `relay_admin_listen` |
| the emergency halt in owned child processes: both fatal branches terminate with the halt status against a full stderr, a full stdout, and a writable sink, and the writable sink receives the report | `relay_admin_halt` |
| the listener's structural contracts that no runtime state can reach: one broker held by reference, the terminality gate on lane wakes, the bank gate before a generation is frozen, and cancellation owned by the admin thread | `relay_listener_contracts` |
| every fallible logical-clock operation is checked where it is made, so a refused arm or advance is a named failure at the boundary rather than a later timeout | `relay_clock_call_sites` |
| the request thread's lifetime is proved by a mutex handoff: its release publication is its last access, the wait is on its own condition, and it is relinquished by a release-qualified detach rather than an unbounded join | `relay_request_lifetime` |
| the lifetime authority itself: the real checker driven against thirteen single-mutation copies of the real source, each proved to have applied exactly once, and the pristine source accepted | `relay_request_lifetime_selftest` |
| seeded determinism and per-step invariants over the admin tier against an independent model: statuses stay inside the finite table, a 200 carries the client's own serial, bank transitions are legal, and the release ledger balances | `relay_admin_seeded` |
| namespace advertisement to a live prefix subscriber, both arrival orders, same-shard and cross-shard | `relay_ns_propagation` |

## Boundary fixture

The three thin real-transport cells (`relay_loopback_same_d18`,
`relay_loopback_cross_d18`, `relay_multiversion`) read LibMoQ's committed
test-only certificate and key:

```
adapters/msquic/tests/test_only_loopback_cert.pem
adapters/msquic/tests/test_only_loopback_key.pem
```

Nothing is minted at run time. The retired `relay_gen_certs` fixture shelled
out to `openssl req`, which made the physical cells depend on the host's
tooling and on the clock for a credential no test inspects.
`check_test_inventory.sh` refuses the return of `openssl`/`MOQ_OPENSSL` or
`relay_gen_certs`, and requires both committed paths to exist and to be named
by the relay CMake.

## Lane labels

| label | meaning |
|---|---|
| `sansio` | deterministic, no transport; the merge lane |
| `seeded` | the committed seed set |
| `boundary` | thin, bounded real-transport reachability |
| `tooling` | scripts and their self-tests |

There is no soak or qualification lane. A scenario that needs repetition or
wall-time to say anything has no owner and is not registered.
