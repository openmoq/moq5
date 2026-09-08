# Simple Relay

A raw-QUIC relay in about 200 lines of C, using the same relay libraries as
`moq5-relay`. The complete application is [main.c](main.c), including argument
checking, connection retirement, signal handling and shutdown. There is no
forwarding implementation hidden in an example helper.

The application links `moq::relay` and `moq::adapter-msquic-managed`. The SDK
owns routing, retention and session binding. MsQuic owns transport and its lane
thread. The application connects those two pieces and honors the SDK wake mask.

## Build

On a POSIX host, from the repository root with an installed MsQuic development
package:

```sh
cmake -S . -B build/simple-relay \
    -DMOQ_BUILD_RELAY_CORE=ON -DMOQ_BUILD_RELAY_RUNTIME=ON \
    -DMOQ_BUILD_RELAY_TOOL=OFF -DMOQ_BUILD_ADAPTER_MSQUIC=ON \
    -DMOQ_BUILD_MSQUIC_MANAGED=ON -DMOQ_BUILD_EXAMPLES=ON \
    -DMOQ_WARNINGS_AS_ERRORS=ON -Dmsquic_DIR=/path/to/msquic/cmake
cmake --build build/simple-relay --target moq_simple_relay
```

Or build this directory independently against an installed SDK:

```sh
cmake -S examples/relay/simple-relay -B build/simple-consumer \
    -Dlibmoq_DIR=/path/to/prefix/lib/cmake/libmoq \
    -Dmsquic_DIR=/path/to/msquic/cmake -DMOQ_WARNINGS_AS_ERRORS=ON
cmake --build build/simple-consumer
```

The SDK installation must include the `relay` and `adapter-msquic-managed`
components. The example does not enable missing adapters automatically. Its
source and standalone CMake file also install alongside the relay SDK docs.

## Run

```sh
build/simple-relay/examples/relay/simple-relay/moq_simple_relay server.pem server-key.pem 4433
```

The listener binds **127.0.0.1 only**, accepting drafts 18 and 16 in that server
preference order. The certificate and private key must be valid PEM files.
Clients must trust the certificate and verify the server identity. Port `0`
requests an ephemeral port; the program prints the bound port after successful
creation. SIGINT or SIGTERM stops the transport and joins its lane before
destroying relay state. Unexpected transport/pump failures exit nonzero.

This is an embedding example, not a public relay service. It has one lane, a
16-connection cap, the SDK's default bounded capacities and **no authorization
policy**. Do not expose it to untrusted peers by merely changing the bind address.
Use `moq5-relay` for configurable listeners, authorization and admin/metrics.
There is no WebTransport listener, media catalog special case, or graceful
drain command here; process shutdown can cancel in-flight work.

## Ownership

Only the lane callback accesses sessions or steps the relay. Connection tags
live in the adapter's user slot rather than a pointer-keyed map. A detached or
refused connection is never reattached. Its remaining events are cleaned before
terminal acknowledgment; a terminal that has not arrived keeps its slot until a
later callback. After acknowledgment, no borrowed connection handle escapes.

The runtime owns its core/binding; their getters return borrowed pointers. The
main thread calls `stop()` outside callbacks, then destroys transport and relay
in that order. The 100 ms main-thread wait bounds signal response only: forwarding
is driven by adapter activity and the SDK's explicit continuation requests.

## Tests

`simple_relay_lifecycle` drives this exact application through a substituted
transport with real SDK/session state and caller-supplied time, without sockets
or sleeps. `simple_relay_loopback` is a bounded real-transport test: all four
draft pairings, exact binary payloads, subgroup FIN, cumulative connections
beyond the listener cap, and SIGTERM shutdown. Its peers bypass certificate
verification for the committed test certificate; it is **not** TLS trust,
browser, interop-runner, load or performance evidence. The SDK package test
builds the example from relocated installed sources and runs `--help`.

`examples/local-media/relay` remains a separate lower-level media demonstration;
its upstream catalog/FETCH behavior is not replaced by this example.
