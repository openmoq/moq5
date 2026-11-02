# Shard runtime

| Responsibility | File |
| --- | --- |
| Configuration, lifecycle and the ordered stepper | `shards.c` |
| Placement and canonical keys | `placement.c` |
| Candidate journal and reconciliation | `journal.c` |
| Namespace control mailboxes | `mailbox.c` |
| Demand channels, routing and staged terminals | `demand.c` |
| Object data transfer and stream progress | `data.c` |
| Owner and requester fill retrieval | `fill.c` |
| Statistics, journal dumps and private inspection | `observability.c` |

`shards_internal.h` owns shared private state. Owner-specific internal headers
declare cross-file helpers. Existing inspection and fault-injection flags keep
their separate scopes.
