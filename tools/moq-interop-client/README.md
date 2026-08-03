# moq-interop-client

MoQ Transport interop test runner. Connects to a relay and runs
named test cases, reporting results in TAP format.

## Role: protocol conformance, not a consumer example

This is a low-level **protocol/conformance** tool. It uses the service-tier
endpoint (`moq_endpoint_t`) only for transport selection and version
negotiation, then drives the sans-I/O core (`moq_session_*`) directly through
the endpoint's `post()` executor so it can exercise and assert exact wire-level
behavior across implementations. It is **not** an example of how to integrate
libmoq into an application, and it does **not** use the high-level media
surface (`moq_media_receiver_t` / `moq_media_sender_t`).

If you are building a media consumer (player, encoder, plugin), start with
`examples/service/` instead — that is the supported high-level API. A
*service-level* interop client, should we need one, belongs in a separate
tool/example so this conformance runner stays focused on protocol behavior.

## Current status

All six interop tests implemented.

## Build (local)

Build trees live under `build/` (see the top-level `CMakePresets.json`):

```
cmake -B build/interop \
    -DMOQ_BUILD_ADAPTER_PICOQUIC=ON \
    -DMOQ_BUILD_PQ_THREADED=ON \
    -DMOQ_BUILD_INTEROP_CLIENT=ON \
    -DMOQ_PICOQUIC_SOURCE_DIR=/path/to/picoquic \
    -DMOQ_PICOTLS_PREFIX=/path/to/picotls/build
cmake --build build/interop --target moq-interop-client
```

## Build (Docker)

Build context must be the parent directory containing `libmoq/`,
`picoquic/`, and `picotls/` as siblings:

```
cd /path/to/parent
docker build -f libmoq/tools/moq-interop-client/Dockerfile -t moq-interop-client .
```


## Usage

```
# Run one named case:
moq-interop-client --relay moqt://relay.example.com:4443 --test setup-only

# Run the whole suite (no --test / TESTCASE): one TAP line per case.
moq-interop-client --relay moqt://relay.example.com:4443

# Pin the offered draft to 16 or 18 (default: auto-negotiate, offer all):
moq-interop-client --relay moqt://relay:4443 --draft 18

# With TLS verification disabled (for test relays with self-signed certs):
moq-interop-client --relay moqt://localhost:4443 --test setup-only --tls-disable-verify

# Via environment (for CI/Docker):
RELAY_URL=moqt://relay:4443 TESTCASE=setup-only MOQT_DRAFT=16 TLS_DISABLE_VERIFY=1 moq-interop-client
```

CLI flags override environment variables. With no test named (neither `--test`
nor `TESTCASE`) the client runs every case and exits 0 as long as it ran;
per-case pass/fail is in the TAP. `--draft` / `MOQT_DRAFT` must be exactly `16`
or `18`.

## Backend selection (testing)

`--backend NAME` (or `MOQT_BACKEND`; the flag wins over the environment) pins
the transport backend so the same suite can exercise libmoq's own adapters. It
is a testing knob, not part of the interop-runner interface, and defaults to
`auto` (the picoquic family). Each backend is transport-restricted; the client
cross-checks the name against the URL scheme and rejects a mismatch up front:

| `--backend` | Transport | Notes |
|---|---|---|
| `auto` | raw QUIC + WebTransport | picoquic family (default) |
| `picoquic` | raw QUIC + WebTransport | |
| `msquic` | raw QUIC only | exact-version — pin `--draft 16`/`18` (a single ALPN) |
| `mvfst` | raw QUIC only | exact-version; when compiled in |
| `proxygen` | WebTransport only | when compiled in |
| `wtquic-msquic` | WebTransport only | WebTransport over MsQuic (cross-platform) |
| `wtquic-network` | WebTransport only | **experimental** — WebTransport over Apple Network.framework; unreliable against eager third-party relays |

Bare `wtquic` is **not** accepted (it names no upstream transport). A backend
that is not compiled into the build is rejected at connect with
`MOQ_ERR_UNSUPPORTED` — the resolver never silently falls back to picoquic.

The TAP stream names the exercised backend, WebTransport profile, and draft:
`# request: backend=… wt-profile=… draft=…` up front (visible even when the
connection fails), and `# negotiated: draft-N over TRANSPORT (backend: …)` once
setup completes.

### WebTransport profile (`--wt-profile`)

`--wt-profile NAME` (or `MOQT_WT_PROFILE`; the flag wins) selects the
WebTransport-over-HTTP/3 wire dialect for the **wtquic-msquic** backend. The two
dialects are mutually exclusive and never auto-negotiated:

| `--wt-profile` | extended CONNECT `:protocol` | peers |
|---|---|---|
| `current` (default) | `webtransport-h3` (current WebTransport-H3 draft) | e.g. imquic |
| `d13-14` | `webtransport` (drafts 13/14 + max-sessions signal) | proxygen/moxygen/moqx and the picoquic h3zero family |

It is **rejected** for any other backend (`picoquic` has a fixed WebTransport
dialect; raw-QUIC `msquic`/`mvfst` have no WebTransport) rather than silently
ignored. In the TAP `# request:` identity, those backends report their truthful
dialect — `native` for a fixed-dialect WebTransport backend, `n/a` for raw QUIC —
never a `current`/`d13-14` they did not speak.

```
# Direct MsQuic, raw QUIC, draft 18:
moq-interop-client --backend msquic --draft 18 --relay moqt://relay:4443 --test setup-only

# WebTransport over MsQuic against a proxygen/moxygen/moqx-style relay:
moq-interop-client --backend wtquic-msquic --wt-profile d13-14 --relay https://relay:4443/ --test setup-only
```

To build a native matrix binary that has every backend compiled in, enable the
adapters explicitly (each requires its dependency): `MOQ_BUILD_PQ_THREADED`,
`MOQ_BUILD_PICO_WT_MANAGED`, `MOQ_BUILD_MSQUIC_MANAGED`,
`MOQ_BUILD_WTQUIC_MSQUIC_MANAGED`, `MOQ_BUILD_WTQUIC_NETWORK_MANAGED`, alongside
`MOQ_BUILD_SERVICE` and `MOQ_BUILD_INTEROP_CLIENT`.

### Docker

```
# CLI args:
docker run --rm moq-interop-client \
    --relay moqt://relay:4443 --test setup-only --tls-disable-verify

# Environment:
docker run --rm \
    -e RELAY_URL=moqt://relay:4443 \
    -e TESTCASE=setup-only \
    -e TLS_DISABLE_VERIFY=1 \
    moq-interop-client
```

## URL format

- `moqt://host:port` — raw QUIC (ALPN `moqt-16` / `moqt-18`, negotiated)
- `moqt://[::1]:port` — IPv6 literal
- `https://host:port/path` — WebTransport

## TAP output

Results go to stdout in [TAP version 14](https://testanything.org/)
format. All logs go to stderr.

Pass:
```
TAP version 14
1..1
ok 1 - setup handshake
  ---
  duration_ms: 42
  message: "MoQ setup complete"
  ...
```

Fail:
```
TAP version 14
1..1
not ok 1 - setup handshake
  ---
  duration_ms: 3000
  message: "timeout"
  ...
```

Unknown test:
```
TAP version 14
1..1
ok 1 - unknown-test # SKIP unknown test case
```

## Exit codes

| Code | Meaning |
|------|---------|
| 0 | Named test passed; or run-all completed (per-case pass/fail is in the TAP) |
| 1 | Named test failed, run-all ran no case, or bad arguments |
| 127 | Unsupported or unknown test case |

## Supported tests

| Test | Status |
|------|--------|
| `setup-only` | Implemented |
| `announce-only` | Implemented |
| `publish-namespace-done` | Implemented |
| `subscribe-error` | Implemented |
| `announce-subscribe` | Implemented |
| `subscribe-before-announce` | Implemented |
