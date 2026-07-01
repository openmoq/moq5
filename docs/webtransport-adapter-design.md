# WebTransport Adapter Design Note

> **Status (historical design note).** This captures the original
> rationale for the proxygen WebTransport adapter. The adapter now exists
> as an experimental, installable C++ component that binds directly to
> `proxygen::WebTransport` and does **not** depend on moxygen, in two
> modes: a C++ **attach** adapter (`adapter-proxygen-wt`) and a **managed
> client facade** (`adapter-proxygen-wt-managed`,
> `moq_proxygen_wt_managed_t`) that drives the WT CONNECT and owns the
> thread + session. proxygen WT publish and receive are byte-exact in the
> moqx E2E matrix; it is the explicit-select service backend
> (`MOQ_TRANSPORT_BACKEND_PROXYGEN`), client-only. The moqx/moxygen
> migration discussion below is motivating context, not a description of
> current dependencies or a committed roadmap. For the current
> consumer-facing state see
> [transport-integration-guide.md](transport-integration-guide.md) §10.

Design note for a proxygen WebTransport adapter for libmoq. This is
a sibling to the raw mvfst adapter (`adapters/mvfst/`), not a layer
on top of it.

## Problem

The raw mvfst adapter connects `quic::QuicSocket` directly to
`moq_session_t`. This is the right path for native QUIC applications.

moqx/moxygen enters through `proxygen::WebTransport`, not raw
`quic::QuicSocket`. moxygen's `MoQSession` already owns a
`proxygen::WebTransport` handle and uses its stream/datagram APIs.
Migrating moqx to libmoq requires a WebTransport attach adapter
that bridges `proxygen::WebTransport` to `moq_session_t` — without
rewriting moxygen's transport ownership.

## Layering

```
Raw path (adapters/mvfst):
  mvfst QuicSocket ──→ mvfst adapter ──→ moq_session_t

WebTransport path (adapters/webtransport):
  mvfst QuicSocket ──→ proxygen WebTransport ──→ WT adapter ──→ moq_session_t
```

These are mutually exclusive per connection. Do not attach both
raw mvfst callbacks and WebTransport callbacks to the same socket.

The inner bridge (`bridge.hpp/cpp`) and `quic_endpoint` interface
are reusable: the WebTransport adapter provides a different
`quic_endpoint` implementation that speaks `proxygen::WebTransport`
instead of `quic::QuicSocket`.

## Proposed API shape

C++ attach adapter first. proxygen types are C++ — there is no
useful C projection.

```cpp
// adapters/webtransport/include/moq/webtransport.hpp (draft)

namespace moq::wt {

class adapter {
public:
    struct config {};

    adapter(config cfg,
            moq_session_t *session,
            proxygen::WebTransport *wt);
    ~adapter();

    moq_result_t service(uint64_t now_us);
    bool is_fatal() const noexcept;
    uint64_t fatal_code() const noexcept;

private:
    struct impl;
    std::unique_ptr<impl> impl_;
};

} // namespace moq::wt
```

Originally attach-only: no C header, no managed mode. (Superseded — a
managed C facade `moq_proxygen_wt_managed.h` has since landed; see the
status note at the top.)

## API questions before implementation

These must be answered by reading proxygen WebTransport headers
before writing adapter code:

1. **Bidi streams.** How does `proxygen::WebTransport` expose
   `createBidirectionalStream`? What handle type? Read/write
   split or unified? How is the control bidi opened?

2. **Uni streams.** `createUnidirectionalStream` — same questions.
   How are peer-opened uni streams delivered?

3. **Datagrams.** `sendDatagram` API shape. Read path. Buffer
   ownership.

4. **Write backpressure.** Does `StreamWriteHandle::writeStreamData`
   return a result indicating WOULD_BLOCK / queue full? Or is it
   callback-driven (onStreamWriteReady)?

5. **Read buffer lifetime.** Does `StreamData` own the buffer?
   Is zero-copy feasible or does the adapter need to copy?

6. **Stream handle lifetime.** When does a `StreamWriteHandle` /
   `StreamReadHandle` become invalid? After FIN? After reset?
   After the WebTransport session closes?

7. **Connection close.** How is close signaled? Callback on
   `WebTransportHandler`? Error code propagation?

8. **Thread confinement.** Is `proxygen::WebTransport` confined
   to an EventBase thread like `quic::QuicSocket`?

## Test strategy

1. **Sans-I/O inner bridge.** The existing `bridge.hpp/cpp` is
   transport-agnostic via `quic_endpoint`. A WebTransport endpoint
   (`wt_endpoint`) implements the same interface. BridgePair-style
   deterministic tests work with `wt_endpoint` fake endpoints.

2. **Fake WebTransport endpoint.** Mock `proxygen::WebTransport`
   for unit tests. Record outbound operations, deliver inbound
   data. Same pattern as `paired_endpoint` in the mvfst harness.

3. **Real loopback.** Only after the API surface is understood.
   Requires proxygen server + client in a test binary.

4. **Reuse scenarios.** Setup handshake, subscribe + object,
   multi-object subgroup, datagram, reset propagation — same
   as BridgePair/MvfstPair but through WebTransport.

## moqx migration reality

The WebTransport adapter only solves transport-to-session attachment.
It does **not** solve the relay object model gap:

- moxygen `MoQSession` owns control message parsing, subscribe
  state machines, and track routing. libmoq `moq_session_t` owns
  all of this internally.
- moxygen types (`FullTrackName`, `TrackConsumer`, `Publisher`,
  `Subscriber`, `SubscribeID`, `TrackAlias`) do not map 1:1 to
  libmoq's opaque handles and event-driven API.
- A separate shim/mapping layer is needed between the moqx relay
  logic and libmoq's session events/actions.

Migration order:
1. WebTransport adapter (transport attachment)
2. Session event/action mapping shim (relay logic bridge)
3. Incremental relay feature migration

## Non-goals

- **Do not replace the raw mvfst adapter.** It remains the clean
  native-QUIC path for C and C++ applications.
- **Do not make libmoq core depend on proxygen.** The WebTransport
  adapter is optional, like the mvfst adapter.
- **Do not expose proxygen types in C APIs.** The adapter is
  C++ only.
- **Do not stack WebTransport on top of the raw mvfst adapter.**
  They are siblings with different endpoint implementations.
- **Do not generalize prematurely.** The adapter targets proxygen
  WebTransport specifically. A generic WebTransport interface
  (supporting other implementations) is a separate design decision.

## Implementation slices (draft)

1. **Slice 1:** `wt_endpoint` implementing `quic_endpoint` via
   `proxygen::WebTransport`. Compile against proxygen headers.
   BridgePair-style tests with fake WebTransport.

2. **Slice 2:** `moq::wt::adapter` attach class. Installs
   `WebTransportHandler` callbacks, routes inbound streams/datagrams
   to bridge, services outbound actions through `wt_endpoint`.

3. **Slice 3:** Real loopback test with proxygen H3 server +
   client. Setup + subscribe + object delivery.

4. **Slice 4:** moqx integration — adapt moxygen's transport
   ownership to use `moq::wt::adapter` instead of `MoQSession`'s
   built-in control parsing.
