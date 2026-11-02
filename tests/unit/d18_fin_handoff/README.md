# Request-stream FIN handoff tests

These files build one `test_d18_fin_handoff` executable, registered as `d18_fin_handoff`.
`../test_d18_fin_handoff.c` keeps the original ordered runner. Shared
fixture state has one definition in `support.c`.

| Behavior | File |
| --- | --- |
| Owner/slot admission and refusal | `admission.c` |
| Descriptor validation and negative controls | `descriptor_checks.c` |
| Standalone FETCH and cancellation | `fetch.c` |
| Pending joining FETCH ownership | `fetch_joining.c` |
| GOAWAY and retained request FIN | `goaway.c` |
| Namespace rejection and durable terminals | `namespace_rejection.c` |
| Namespace teardown and blocked retry | `namespace_teardown.c` |
| Publication ownership, finish and deferred finalization | `publish.c` |
| Namespace publication, withdrawal and teardown | `publish_namespace.c` |
| Precommit rejection and current-call FIN | `request_rejection.c` |
| Subscription controls, unsubscribe and completion | `subscribe.c` |
| Namespace-subscription ownership and retry | `subscribe_namespace.c` |
| Track-discovery request ownership | `subscribe_tracks.c` |
| Failed subscription UPDATE and closing timers | `subscription_update.c` |
| Shared fixtures and exact output/owner checks | `support.c` |
| Track-status ownership and retirement | `track_status.c` |

`fin_handoff_test_internal.h` declares shared fixture types, state and helper functions;
`test_cases.h` declares the cases called by the runner.
