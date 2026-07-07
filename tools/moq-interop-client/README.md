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
