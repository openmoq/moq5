# mvfst Managed Server

Optional managed-mode C API for accepting MoQ connections via
mvfst QUIC transport. Requires `MOQ_BUILD_ADAPTER_MVFST=ON`.

## Overview

The managed server owns a QuicServer listener on one worker
EventBase. Each accepted QUIC connection gets its own
moq_session_t and adapter bridge. The application iterates
connections via `next_conn()` inside `on_pump` and uses
`wake()`/`wait()` for cross-thread signaling.

All session and connection calls must happen inside `on_pump`.

```
App thread                   Managed network thread
─────────                    ──────────────────────
                             create QuicServer
                             bind + listen
                             start worker EventBase

  wake() ──────────────────→ EventBase.loopOnce()
                             accept new connections
                             adapter.service(now) per conn
                             on_pump(managed, now, ctx)
  wait() ◄──────────────────  signal_activity()

  stop() ──────────────────→ close all connections
                             shutdown QuicServer
                             destroy sessions
  destroy()
```

## API

```c
#include <moq/mvfst.h>

moq_mvfst_managed_cfg_t cfg;
moq_mvfst_managed_cfg_init(&cfg);
cfg.perspective  = MOQ_PERSPECTIVE_SERVER;
cfg.port         = 0;       /* 0 = ephemeral; use local_port() after create */
cfg.cert_path    = "/path/to/server.pem";
cfg.key_path     = "/path/to/server-key.pem";
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

## Pump callback

```c
int my_pump(moq_mvfst_managed_t *m, uint64_t now_us, void *ctx) {
    moq_mvfst_conn_t *conn = NULL;
    while ((conn = moq_mvfst_managed_next_conn(m, conn)) != NULL) {
        moq_session_t *s = moq_mvfst_conn_session(conn);
        /* poll events, accept subscribes, write objects */
    }
    return 0;  /* 0 = continue, nonzero = request shutdown */
}
```

The pump runs after `adapter.service(now)` for each connection,
so inbound data is already processed and outbound actions are
drained before the callback fires.

## Connection iteration

| Function | Thread | Notes |
|----------|--------|-------|
| `next_conn` | pump | Returns NULL when done; pass prev=NULL to start |
| `conn_session` | pump | Session for one connection |
| `conn_close` | pump | Marks close; removal deferred until after pump returns |
| `conn_count` | any | Atomic; safe from app thread |

Note: conn handles are valid only for the current `on_pump` call.

## Server config

| Field | Requirement | Notes |
|-------|-------------|-------|
| `perspective` | `SERVER` | |
| `port` | 0..65535 | 0 = ephemeral |
| `host` | optional | Bind address; NULL = 0.0.0.0 |
| `cert_path` | required | PEM server certificate |
| `key_path` | required | PEM private key |
| `insecure_skip_verify` | must be false | Rejected for server |

## Lifecycle

| Function | Thread | Notes |
|----------|--------|-------|
| `create` | app | Spawns thread, blocks until init succeeds/fails |
| `stop` | app | Joins thread, idempotent. Returns `MOQ_ERR_INVAL` if called from pump |
| `destroy` | app | Calls `stop()` internally. `destroy(NULL)` is no-op |
| `wake` | any | Thread-safe, coalesced |
| `wait` | app | Returns `MOQ_OK` (activity), `MOQ_DONE` (timeout), `MOQ_ERR_CLOSED` |
| `session` | -- | Returns NULL for server (use `next_conn` + `conn_session`) |
| `local_port` | any | Bound port after `create`; useful when bind port is 0 |
| `is_fatal` | any | Atomic read |
| `fatal_code` | any | Atomic read |

## Connection close/error lifecycle

Connections that are closed (by the app via `conn_close`, by
the peer, or by a transport/protocol error) are removed from
`next_conn()` iteration after the current `on_pump` returns.

The managed server does **not** expose a per-connection close
reason API. To observe close causes, poll session events inside
`on_pump` before the connection disappears:

```c
moq_event_t ev[16]; size_t ne;
moq_session_poll_events_ex(s, ev, 16, sizeof(moq_event_t), &ne);
for (size_t i = 0; i < ne; i++) {
    if (ev[i].kind == MOQ_EVENT_SESSION_CLOSED)
        handle_close(ev[i].u.closed.code);
    moq_event_cleanup(&ev[i]);
}
```

Once a connection vanishes from iteration, its handle and
session pointer are invalid. Applications needing transport-level
close callbacks should use the C++ attach API (`moq/mvfst.hpp`).

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

Run the example server:

```sh
./build/examples/mvfst/moq_mvfst_server \
    --cert server.pem --key server-key.pem --port 4433
```

## Current limitations

- **Single worker EventBase** (all connections on one thread).
- **Not a production relay.** Ergonomic managed wrapper for tests
  and simple servers.
- **ALPN is `moqt-16`.** Not configurable.
- **Bidi STOP_SENDING ignored.** Uni-stream STOP_SENDING is
  handled; bidi STOP_SENDING is pending a core session API.
