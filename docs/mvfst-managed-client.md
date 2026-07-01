# mvfst Managed Client

Optional managed-mode C API for connecting to MoQ relays via the
mvfst QUIC transport. Requires `MOQ_BUILD_ADAPTER_MVFST=ON`.

## Overview

The managed client owns the network thread, EventBase, mvfst
QuicClientTransport, moq_session_t, and the adapter bridge. The
application drives protocol logic inside an `on_pump` callback
and uses `wake()`/`wait()` for cross-thread signaling.

```
App thread                   Managed network thread
─────────                    ──────────────────────
                             create session
                             create transport
                             start QUIC handshake
                             attach adapter

  wake() ──────────────────→ EventBase.loopOnce()
                             adapter.service(now)
                             on_pump(session)
  wait() ◄──────────────────  signal_activity()

  stop() ──────────────────→ teardown adapter
                             close transport
                             destroy session
  destroy()
```

## API

```c
#include <moq/mvfst.h>

moq_mvfst_managed_cfg_t cfg;
moq_mvfst_managed_cfg_init(&cfg);
cfg.perspective  = MOQ_PERSPECTIVE_CLIENT;
cfg.host         = "relay.example.com";  /* hostname or numeric IP */
cfg.port         = 4433;
cfg.insecure_skip_verify = true;  /* or: cfg.cert_path = "/path/to/ca.pem"; */
cfg.on_pump      = my_pump;
cfg.user_ctx     = &my_state;
cfg.send_request_capacity = true;
cfg.initial_request_capacity = 16;

moq_mvfst_managed_t *m;
moq_mvfst_managed_create(&cfg, &m);

while (!done) {
    moq_mvfst_managed_wake(m);
    moq_result_t rc = moq_mvfst_managed_wait(m, 500000);
    if (rc == MOQ_ERR_CLOSED) break;
    /* process app-side results */
}

moq_mvfst_managed_destroy(m);  /* calls stop() internally */
```

## Session access

`moq_mvfst_managed_session()` returns non-NULL **only when called
from the managed network thread** (inside `on_pump`). Calling it
from the app thread always returns NULL. All `moq_session_*` calls
must happen inside `on_pump`.

## Pump callback

```c
int my_pump(moq_mvfst_managed_t *m, uint64_t now_us, void *ctx) {
    moq_session_t *s = moq_mvfst_managed_session(m);
    if (!s) return 0;

    /* subscribe, poll events, write objects, etc. */

    return 0;  /* 0 = continue, nonzero = request shutdown */
}
```

The pump runs after `adapter.service(now)`, so inbound data is
already processed and outbound actions are drained before the
callback fires.

## Lifecycle

| Function | Thread | Notes |
|----------|--------|-------|
| `create` | app | Spawns thread, blocks until init succeeds/fails |
| `stop` | app | Joins thread, idempotent. Returns `MOQ_ERR_INVAL` if called from pump |
| `destroy` | app | Calls `stop()` internally. `destroy(NULL)` is no-op |
| `wake` | any | Thread-safe, coalesced |
| `wait` | app | Returns `MOQ_OK` (activity), `MOQ_DONE` (timeout), `MOQ_ERR_CLOSED` |
| `session` | pump | Returns NULL from wrong thread or after stop |
| `is_fatal` | any | Atomic read |
| `fatal_code` | any | Atomic read |

## QUIC transport tuning

| Field | Default | Description |
|-------|---------|-------------|
| `max_num_ptos` | 0 (mvfst default: 7) | Max consecutive PTOs before connection close |
| `initial_rtt_us` | 0 (mvfst default: 50000) | Initial RTT estimate in microseconds |

Set `max_num_ptos = 2` and `initial_rtt_us = 10000` for fast
connection failure detection in tests.

## Lifecycle-only mode

If `host` is NULL or empty, no transport or adapter is created.
The pump loop runs with a session-only EventBase-free loop,
useful for lifecycle tests that don't need a server.

## TLS verification

Three mutually exclusive modes:

| Mode | Config | Checks |
|------|--------|--------|
| Insecure | `insecure_skip_verify = true` | None (testing only) |
| Custom CA | `cert_path = "/path/to/ca.pem"` | Chain + host/IP identity |
| System default | neither set | System trust store + host/IP identity |

Both secure modes verify:
1. **Chain trust** — the server certificate chains to a trusted CA
   (PEM file or system trust store).
2. **Host identity** — the server certificate contains an IP SAN
   or DNS SAN matching `cfg.host`.

`insecure_skip_verify` and `cert_path` are mutually exclusive;
`create()` returns `MOQ_ERR_INVAL` if both are set.

```c
/* Local testing — accept any cert */
cfg.insecure_skip_verify = true;

/* Dev/staging — trust a specific CA and verify host identity */
cfg.cert_path = "/etc/moq/relay-ca.pem";

/* Production — system trust store + host identity */
/* (leave both insecure_skip_verify and cert_path unset) */
```

`create()` returns `MOQ_ERR_INVAL` if `cert_path` is set but the
file is not readable.

## Building

The mvfst adapter is optional and does not affect normal libmoq
builds:

```sh
cmake -B build -DMOQ_BUILD_ADAPTER_MVFST=ON
cmake --build build
```

The mvfst adapter is consumed via CMake components:

```cmake
find_package(libmoq REQUIRED COMPONENTS adapter-mvfst)
target_link_libraries(app PRIVATE moq::adapter-mvfst)
```

The public API is C-compatible, but CMake consumers need a C++
toolchain available to link the mvfst adapter. The component
config enables CXX automatically if needed.

The adapter is not included in the default `libmoq.pc` to avoid
pulling C++/mvfst dependencies for C-only consumers.

Run the example subscriber:

```sh
./build/examples/mvfst/moq_mvfst_subscriber \
    --insecure 127.0.0.1 4433 live/cam1 video
```

## Current limitations

- **No WebTransport.** Direct QUIC only.
- **ALPN is `moqt-16`.** Not configurable.
- **Bidi STOP_SENDING ignored.** Uni-stream STOP_SENDING is
  handled; bidi STOP_SENDING is pending a core session API.
- **DNS resolution is blocking.** Hostname lookup runs on the
  managed network thread during `create()`. Unresolvable hosts
  cause `create()` to return `MOQ_ERR_INTERNAL`.
