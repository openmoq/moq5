# LibMoQ Documentation

- [Architecture](architecture.md) — Sans-I/O model, call categories, actions/events, implemented protocol surface, module layout
- [Memory](memory.md) — Allocator contract, borrow/owned rules, scratch arena, receive budget, rcbuf ownership, FFI guidance
- [Integration](integration.md) — Adapter loop, control/data byte feeding, action/event polling, cleanup obligations, backpressure retry
- [Simulation](simulation.md) — SimPair, seeds, traces, OOM sweep, deterministic testing
- [API Boundaries](api-boundaries.md) — Header tiers, naming policy, examples policy
- [Publisher Retained Catalog Groups](publisher-retained-groups.md) — Origin-local cache support for catalog Joining FETCH, and what it does not guarantee through relays
- [Conformance](conformance.md) — Per-draft status, including draft 21 and its interop-runner results
- [Draft 21 plan](draft21-implementation-plan.md) — How draft 21 was added, task by task, with results and open items
- [Draft 21 wire reference](draft21-wire-reference.md) — Draft-21 wire facts, ambiguities and worked byte examples
