# Transport bridge tests

These files build one `test_transport_bridge` executable, registered as `transport_bridge`.
`../test_transport_bridge.c` keeps the original ordered runner. Shared
fixture state has one definition in `support.c`.

| Behavior | File |
| --- | --- |
| Owner/slot admission and refusal | `admission.c` |
| Refused allocations and retained ownership | `allocation_failure.c` |
| Suspended ingress, resets and tick resumption | `backpressure.c` |
| Creation, handshake, pending input and close | `basic.c` |
| Descriptor validation and negative controls | `descriptor_checks.c` |
| Drain retention and payload conservation | `drain_capacity.c` |
| Independent stream progress and ordering | `head_of_line.c` |
| Held-input replay, expiry and cancellation | `held_input.c` |
| Local FIN awaiting commitment | `local_fin.c` |
| Namespace response FIN/RESET and retained ownership | `namespace_subscribe.c` |
| Namespace teardown and blocked retry | `namespace_teardown.c` |
| Priority across closed subgroups | `priority.c` |
| Publication ownership, finish and deferred finalization | `publish.c` |
| Peer and local request cancellation | `request_cancellation.c` |
| Budget entry, exit and service bounds | `service_budget.c` |
| Control reservation and minimum stream-map capacity | `stream_capacity.c` |
| Uni-stream classes, FIFO and control reservation | `stream_placement.c` |
| Shared fixtures and exact output/owner checks | `support.c` |
| Monotonic terminal observations | `terminal_facts.c` |
| Track-status ownership and retirement | `track_status.c` |

`bridge_test_internal.h` declares shared fixture types, state and helper functions;
`test_cases.h` declares the cases called by the runner.
