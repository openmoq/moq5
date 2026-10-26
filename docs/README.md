# LibMoQ Documentation

- [Architecture](architecture.md) — Sans-I/O model, actions/events, wire profiles and their capabilities, request streams, fills, scheduling, transport binding, concurrent subscriptions, module layout
- [Conformance](conformance.md) — Per-draft status for drafts 16, 18 and 22, known limitations, interoperability evidence
- [MoQ Cluster setup](../relay/README.md#cluster-routing) — Opt-in draft-22 session and relay configuration, peer policy and capacity planning
- [MoQ Cluster design](moq-cluster-01-design.md) — Routing, metadata ownership, failover and validation; [pinned draft-01 text](draft-lcurley-moq-cluster-01.txt)
- [Draft-22 wire reference](draft22-wire-reference.md) — Every draft-22 message, parameter, setup option, stream header and error registry, with the readings taken
- [Draft-18 wire reference](draft18-wire-reference.md) — The vi64 integer encoding
- [Adapters](adapters.md) — Adapter contract, per-adapter matrix, transport binding, reliable resets, porting checklist
- [Transport integration guide](transport-integration-guide.md) — Choosing and wiring a transport backend, version and ALPN selection
- [Receive admission](receive-admission.md) — Holding refused inbound data
- [Memory](memory.md) — Allocator contract, borrow/owned rules, scratch arena, receive budget, rcbuf ownership, FFI guidance
- [Integration](integration.md) — Adapter loop, control/data byte feeding, action/event polling, cleanup obligations, backpressure retry
- [Integration patterns](integration-patterns.md) — Service-tier and media patterns
- [Media player](media-player.md) — Decode-order media for an application's decoders: outputs, selection and timelines, how a track ends, bounds
- [Simulation](simulation.md) — SimPair, seeds, traces, OOM sweep, deterministic testing
- [API Boundaries](api-boundaries.md) — Header tiers, naming policy, examples policy
- [Publisher Retained Catalog Groups](publisher-retained-groups.md) — Origin-local cache support for catalog Joining FETCH, and what it does not guarantee through relays
