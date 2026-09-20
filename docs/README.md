# LibMoQ Documentation

- [Architecture](architecture.md) — Sans-I/O model, call categories, actions/events, implemented protocol surface, module layout
- [Memory](memory.md) — Allocator contract, borrow/owned rules, scratch arena, receive budget, rcbuf ownership, FFI guidance
- [Integration](integration.md) — Adapter loop, control/data byte feeding, action/event polling, cleanup obligations, backpressure retry
- [Simulation](simulation.md) — SimPair, seeds, traces, OOM sweep, deterministic testing
- [API Boundaries](api-boundaries.md) — Header tiers, naming policy, examples policy
- [Publisher Retained Catalog Groups](publisher-retained-groups.md) — Origin-local cache support for catalog Joining FETCH, and what it does not guarantee through relays

- [CAT4MoQ support design](cat4moq-design.md) — Current gaps, C4M-01 and current-relay compatibility, ownership and enforcement boundaries
- [CAT4MoQ client implementation plan](superpowers/plans/2026-09-20-cat4moq-client.md) — Binary credentials, SETUP, managed APIs and MoQXR interoperability
- [CAT4MoQ relay implementation plan](superpowers/plans/2026-09-20-cat4moq-relay.md) — Optional verifier, action checks, revalidation and relay-owned credentials
