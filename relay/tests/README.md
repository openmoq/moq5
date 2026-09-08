# Relay Test Ownership

This directory owns tests of the reusable relay implementation:

- deterministic routing, retention, capacity, and trace behavior;
- session binding and lifecycle over the in-memory simulator;
- shard scheduling, channels, placement, and concurrent stepping;
- pure observation rendering and its golden outputs;
- deterministic scheduler models, seed ledgers, and source contracts.

`tools/moq5-relay/tests/` continues to own command parsing, configuration,
signals, process lifecycle, the administration listener, and physical managed
transport integration. Cross-boundary tests remain there when their subject is
the executable composition, even when they consume private test objects from
this directory. In particular, the managed scheduler explorer and scripted
seed campaigns are tool tests; their transport-independent models and vectors
live here.

The benchmark binaries are owned by `relay/bench/`. Scripts that launch,
aggregate, or compare benchmark runs remain tool-owned because they encode
operator workflow rather than relay behavior.

There is currently no relay-specific fuzz target. A future target that drives
the reusable engine belongs under `relay/`; a target for CLI parsing or process
composition belongs with the tool.
