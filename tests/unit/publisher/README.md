# Publisher tests

These files build one `test_publisher` executable, registered as `publisher`.
`../test_publisher.c` keeps the ordered runner and its inline cases.

| Behavior | File |
| --- | --- |
| Creation, basic subscription, writing and priority | `basic.c` |
| Track admission, removal, reuse and reset | `track_lifecycle.c` |
| Immediate subscription decisions | `subscribe_callback.c` |
| Deferred subscription authorization | `authorization.c` |
| Tick-driven callbacks | `tick.c` |
| Subscription updates and callback sizing | `subscription_update.c` |
| Namespace admission, reference counts and terminal callbacks | `namespace.c` |
| Publisher-initiated publication and restart | `publish.c` |
| Multiple destinations and forward transitions | `fanout.c` |
| Manual event forwarding | `event_forwarding.c` |
| Location filters and delivery-window changes | `filter_window.c` |
| Completion declarations and terminal ordering | `completion.c` |
| Monotonic group evidence | `monotonic_groups.c` |
| Streaming object lifecycle and write options | `streaming.c` |
| Subgroup headers and first-object signaling | `subgroups.c` |
| Retained group identity, membership and FETCH | `group_history.c` |
| Shared fixtures and the suite failure counter | `support.c` |

`publisher_test_internal.h` declares shared fixtures; `test_cases.h` declares
cases called by the runner. File-local fixtures stay with their cases.
