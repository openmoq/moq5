# Threaded Picoquic Adapter Helper — Design

## Problem

Every picoquic integration repeats ~80 lines of transport boilerplate
(picoquic context → session → adapter → facade → start → event loop →
teardown) and a `loop_callback` with the
`service → tick → service → [app work] → service` sandwich.

Applications that need the network on a background thread (media
demuxers, GUI apps, non-C language bindings) must additionally manage
thread creation, cross-thread signaling, shutdown ordering, and fatal
propagation — with the same bugs each time.

Picoquic already provides `picoquic_start_network_thread` /
`picoquic_wake_up_network_thread` /
`picoquic_delete_network_thread` for exactly this scenario. The
helper builds on that API rather than rolling its own threading.

## Why picoquic's network-thread API

`picoquic_start_network_thread` spawns a thread running
`picoquic_packet_loop_v3`. It provides:

- **Wake-up pipe** (Unix) / **event** (Windows):
  `picoquic_wake_up_network_thread` writes to a pipe fd. The
  network thread's poll/select returns, and the loop callback
  fires with `picoquic_packet_loop_wake_up`. All picoquic/session/
  adapter calls inside that callback execute on the network thread
  — no lock needed.

- **Clean shutdown**: `picoquic_delete_network_thread` sets
  `thread_should_close`, closes the wake-up pipe (which faults the
  poll), joins the thread, and frees the thread context.

- **Custom thread creation**: `picoquic_start_custom_network_thread`
  accepts create/delete/setname function pointers for game engines
  or language runtimes that can't use raw pthreads.

## Thread-Safety Model

The adapter contract (`picoquic.h:17`) states:
> Thread safety: none. All calls must be serialized on the same
> thread that drives picoquic (the callback thread).

This helper enforces that rule. All `moq_session_*`, `moq_pq_*`,
`moq_pub_*`, and `moq_sub_*` calls happen exclusively on the
network thread, inside the `on_pump` callback:

1. **`loop_callback`** (fires on `after_receive`, `after_send`):
   calls `moq_pq_service`, then `on_pump`, then `moq_pq_service`
   again, then `mark_activity`.

2. **`wake_up` callback** (fires on `picoquic_packet_loop_wake_up`):
   clears `wake_pending` under mutex, then same sequence —
   `service → on_pump → service → mark_activity`.

The app thread NEVER directly calls session/adapter/facade APIs.
It communicates via application-level queues:

```
App thread                        Network thread
──────────                        ──────────────
enqueue work (app queue)
moq_pq_threaded_wake(t) ────────→ pipe write → poll wakes
                                   loop_callback(wake_up):
                                   ├── clear wake_pending
                                   ├── moq_pq_service
                                   ├── on_pump():
                                   │   dequeue work
                                   │   call moq_pub_write / sub_poll
                                   ├── moq_pq_service
                                   └── mark_activity → condvar signal
moq_pq_threaded_wait(t) returns ◄─╯
read results from app queue
```

### What the helper does NOT provide

- No mutex-protected `with_session` / `with_lock` API. Wrapping
  picoquic callbacks under a lock conflicts with picoquic's internal
  timing and re-entrance assumptions.
- No direct app-thread access to session state. The `on_pump`
  callback is the single point of session interaction.

## Proposed API

```c
typedef struct moq_pq_threaded moq_pq_threaded_t;

/* -- Configuration ------------------------------------------------ */

typedef struct moq_pq_threaded_cfg {
    uint32_t           struct_size;
    const moq_alloc_t *alloc;           /* required */

    /* TLS */
    const char        *cert_path;       /* server: required; client: NULL */
    const char        *key_path;        /* server: required; client: NULL */

    /* Network */
    moq_perspective_t  perspective;     /* CLIENT or SERVER */
    const char        *host;            /* client: remote host; server: ignored */
    int                port;            /* server: listen; client: remote */

    /* Session tuning. These override defaults in moq_session_cfg_t.
     * The helper owns perspective and alloc; everything else is
     * configurable here. Zero/false = use default. */
    bool               send_request_capacity;      /* default false */
    uint64_t           initial_request_capacity;   /* default 64; inert unless send_request_capacity */
    uint32_t           max_actions;                /* default 64 */
    uint32_t           max_events;                 /* default 16 */
    uint32_t           max_data_streams;            /* default 64 */
    uint32_t           max_subscriptions;           /* default 64 */
    uint32_t           send_buffer_size;            /* default 4096 */
    uint32_t           recv_buffer_size;            /* default 4096 */

    /* TLS verification. If true, the helper calls
     * picoquic_set_null_verifier — suitable for demos and tests
     * only. If false (default), the picoquic context uses the
     * system's default TLS verification, which requires a valid
     * CA chain. For custom verification, use configure_quic. */
    bool               insecure_skip_verify;

    /* Optional: called during _create after picoquic_create but
     * before any connections or the network thread starts. The
     * app may configure TLS settings, certificate verification,
     * token stores, or any other picoquic_quic_t options.
     * Return 0 to continue, nonzero to abort _create.
     * May be NULL. */
    int              (*configure_quic)(picoquic_quic_t *quic, void *ctx);
    void              *configure_quic_ctx;

    /* App pump callback. Called on the network thread during
     * loop_callback between moq_pq_service calls. The app ticks
     * its facade, publishes objects, polls received objects, and
     * processes queued work here.
     *
     * All session/adapter/facade calls are safe inside on_pump.
     * For server mode, on_pump is NOT called until the first
     * inbound connection has been accepted and the session/adapter
     * are ready — the app can safely call _session() / _conn()
     * from inside on_pump without null checks.
     *
     * Return 0 to continue. Nonzero requests clean loop
     * termination: the helper sets pump_exit = true, calls
     * mark_activity, and returns
     * PICOQUIC_NO_ERROR_TERMINATE_PACKET_LOOP. The thread
     * exits cleanly. _stop then returns MOQ_OK (unless fatal
     * was also set). */
    int              (*on_pump)(moq_pq_threaded_t *t,
                                uint64_t now_us, void *ctx);
    void              *on_pump_ctx;

    /* Optional: called on network thread after the
     * service → on_pump → service cycle. Signal-only — do NOT call
     * session/adapter/facade APIs. Use to flag the app thread.
     * May be NULL; the helper calls mark_activity() regardless. */
    void             (*on_activity)(moq_pq_threaded_t *t, void *ctx);
    void              *on_activity_ctx;
} moq_pq_threaded_cfg_t;

void moq_pq_threaded_cfg_init(moq_pq_threaded_cfg_t *cfg);
```

### Lifecycle

```c
moq_result_t moq_pq_threaded_create(const moq_pq_threaded_cfg_t *cfg,
                                     moq_pq_threaded_t **out);
moq_result_t moq_pq_threaded_stop(moq_pq_threaded_t *t);
void         moq_pq_threaded_destroy(moq_pq_threaded_t *t);
```

See "Client Create Sequence" and "Server Mode Lifecycle" below
for mode-specific behavior.

### Accessors

```c
/* Returns the session, or NULL if server mode and no connection
 * has been accepted yet. Reads t->session under mutex. */
moq_session_t *moq_pq_threaded_session(moq_pq_threaded_t *t);

/* Returns the adapter connection, or NULL if server mode and no
 * connection has been accepted yet. Reads t->conn under mutex. */
moq_pq_conn_t *moq_pq_threaded_conn(moq_pq_threaded_t *t);

/* Fatal state. Reads under mutex. */
bool     moq_pq_threaded_is_fatal(const moq_pq_threaded_t *t);
uint64_t moq_pq_threaded_fatal_code(const moq_pq_threaded_t *t);
```

### Cross-thread signaling

```c
moq_result_t moq_pq_threaded_wake(moq_pq_threaded_t *t);
moq_result_t moq_pq_threaded_wait(moq_pq_threaded_t *t,
                                    uint64_t timeout_us);
```

See "Wake Semantics" and "Activity Semantics" below.

## Client Create Sequence

`_create` for CLIENT mode performs these steps in order. If any
step fails, all prior steps are rolled back and `_create` returns
an error. No thread is running; `_stop` / `_destroy` are not
needed.

```
_create(cfg) for CLIENT:
  1. Validate cfg (alloc, host, port, on_pump required)
  2. Allocate moq_pq_threaded_t, init mutex/condvar
  3. picoquic_create(1, NULL, NULL, ..., client_callback, t)
         → t->quic
  4. If cfg->insecure_skip_verify:
         picoquic_set_null_verifier(t->quic)
  5. If cfg->configure_quic:
         rc = cfg->configure_quic(t->quic, cfg->configure_quic_ctx)
         if rc != 0 → rollback
  6. picoquic_create_client_cnx(t->quic, addr, ..., ALPN,
         client_callback, t)
         → t->cnx
     (This already calls picoquic_start_client_cnx internally.
      Do NOT call picoquic_start_client_cnx again.)
  7. Build session_cfg from threaded cfg:
         scfg.alloc = cfg->alloc
         scfg.perspective = MOQ_PERSPECTIVE_CLIENT
         scfg.send_request_capacity = cfg->send_request_capacity
         scfg.initial_request_capacity = cfg->initial_request_capacity
         scfg.max_actions = cfg->max_actions       /* 0 = default */
         scfg.max_events = cfg->max_events
         scfg.max_data_streams = cfg->max_data_streams
         scfg.max_subscriptions = cfg->max_subscriptions
         scfg.send_buffer_size = cfg->send_buffer_size
         scfg.recv_buffer_size = cfg->recv_buffer_size
     moq_session_create(&scfg, now) → t->session
  8. moq_pq_conn_create(session, cnx, alloc) → t->conn
  9. moq_session_start(t->session, now)
 10. moq_pq_service(t->conn, now)
         (Queues initial CLIENT_SETUP into picoquic streams)
 11. picoquic_start_network_thread(t->quic, param,
         loop_callback, t) → t->thread_ctx
 12. Return MOQ_OK
```

## Server Mode Lifecycle

v0 supports exactly one active server connection.

```
_create(cfg) for SERVER:
  1. Validate cfg (alloc, cert_path, key_path, port, on_pump required)
  2. Allocate moq_pq_threaded_t, init mutex/condvar
  3. picoquic_create(8, cert, key, ..., server_callback, t)
         → t->quic
  4. If cfg->insecure_skip_verify:
         picoquic_set_null_verifier(t->quic)
  5. If cfg->configure_quic:
         rc = cfg->configure_quic(t->quic, cfg->configure_quic_ctx)
         if rc != 0 → rollback
  6. t->session = NULL, t->conn = NULL
  7. picoquic_start_network_thread(t->quic, param,
         loop_callback, t) → t->thread_ctx
  8. Return MOQ_OK

First inbound connection (picoquic_callback_ready on network thread):
  server_callback:
    moq_session_create(session_cfg) → s
    moq_pq_conn_create(s, cnx, alloc) → c
    lock(t->mutex)
    t->session = s
    t->conn = c
    unlock(t->mutex)
    on_pump now fires each loop iteration

Subsequent inbound connections:
  server_callback returns -1 — one connection only.

_stop():
  picoquic_delete_network_thread → thread exits
  t->session and t->conn may still be NULL

_destroy():
  if (t->conn) moq_pq_conn_destroy(t->conn)
  if (t->session) moq_session_destroy(t->session)
  picoquic_free(t->quic)
  mutex/cond destroy
  free(t)
```

## Thread-Safe State

All shared state in `moq_pq_threaded_t` is protected by one mutex.
No volatile or C11 atomics in v0.

Fields protected by `t->mutex`:
- `t->thread_ctx` — written by `_create`, read by `_wake`,
  nulled by `_stop`. Both `_wake` and `_stop` can run from any
  app thread, so all access is mutex-protected. `_stop` takes
  a local copy under the mutex before calling
  `picoquic_delete_network_thread` outside the lock (see Stop
  Semantics).
- `t->session` — written by network thread (server mode lazy
  create), read by accessors from any thread.
- `t->conn` — same.
- `t->fatal`, `t->fatal_code` — written by network thread on
  fatal, read by accessors / `_wait` / `_stop`.
- `t->stopped` — written by `_stop` (app thread), read by
  `_wait` and network thread.
- `t->activity_pending` — written by `mark_activity` (network
  thread) and `_stop` (app thread), consumed by `_wait`.
- `t->wake_pending` — written by `_wake` (any thread), cleared
  by network thread on `wake_up` callback.
- `t->pump_exit` — written by network thread when on_pump returns
  nonzero, read by `_stop`.

Fields NOT shared (network-thread-only):
- `t->quic`, `t->cnx` — only touched during create and by
  picoquic's internal thread.
- Adapter/session mutation — only via `on_pump` on network thread.

Fields written once during create, read-only after:
- Config copies (callbacks, tuning fields).

Accessor pattern:

```
moq_session_t *moq_pq_threaded_session(moq_pq_threaded_t *t):
    lock(t->mutex)
    moq_session_t *s = t->session
    unlock(t->mutex)
    return s
```

## Wake Semantics

`_wake` uses a helper-side `wake_pending` flag to avoid redundant
pipe writes:

```
_wake(t):
    lock(t->mutex)
    if t->stopped || t->thread_ctx == NULL:
        unlock → return MOQ_ERR_CLOSED
    if t->wake_pending:
        unlock → return MOQ_OK        /* already pending */
    t->wake_pending = true
    t->wake_in_flight++
    picoquic_network_thread_ctx_t *ctx = t->thread_ctx
    unlock(t->mutex)
    rc = picoquic_wake_up_network_thread(ctx)
    lock(t->mutex)
    t->wake_in_flight--
    if t->wake_in_flight == 0:
        cond_broadcast(t->condvar)
    if rc != 0:
        t->wake_pending = false
        unlock → return MOQ_ERR_INTERNAL
    unlock → return MOQ_OK
```

**Concurrent `_wake` / `_stop`**: `_wake` increments
`wake_in_flight` under the mutex before calling
`picoquic_wake_up_network_thread` outside the lock, and
decrements it (with broadcast) under the mutex afterward.
`_stop` sets `stopped = true` and nulls `thread_ctx` under
the mutex, then waits for `wake_in_flight == 0` before
unlocking and calling `picoquic_delete_network_thread`. This
guarantees no in-flight wake call can touch a freed context.

The network thread clears `wake_pending` when it processes the
wake-up:

```
loop_callback(wake_up):
    lock(t->mutex)
    t->wake_pending = false
    unlock(t->mutex)
    /* normal cycle: service → on_pump → service → mark_activity */
```

Multiple `_wake` calls between processing coalesce: the second
call sees `wake_pending == true` and returns `MOQ_OK` without
writing to the pipe again. No pipe byte accumulation.

## Activity Semantics

The helper defines one internal function, `mark_activity()`:

```
mark_activity(t):
    lock(t->mutex)
    t->activity_pending = true
    cond_broadcast(t->condvar)
    unlock(t->mutex)
    if (t->on_activity)
        t->on_activity(t, t->on_activity_ctx)
```

`mark_activity` is called by the helper's `loop_callback` at the
end of each cycle, AFTER the `service → on_pump → service`
sequence completes. It is NOT wired to the adapter's
`after_callback` — the adapter's `after_callback` is left NULL.
The helper controls exactly when activity is signaled.

Call sites (all on network thread):
- `loop_callback(after_receive)`: service → on_pump → service → mark_activity
- `loop_callback(after_send)`: service → on_pump → service → mark_activity
- `loop_callback(wake_up)`: clear wake_pending → service → on_pump → service → mark_activity
- Fatal detected in loop_callback: set fatal under mutex → mark_activity
- on_pump returns nonzero: set pump_exit under mutex → mark_activity

`_wait` consumes the flag:

```
_wait(t, timeout_us):
    lock(t->mutex)
    if t->stopped || t->fatal || t->pump_exit:
        unlock → return MOQ_ERR_CLOSED
    if t->activity_pending:
        t->activity_pending = false
        unlock → return MOQ_OK
    cond_timedwait(t->condvar, t->mutex, timeout_us)
    if t->activity_pending:
        t->activity_pending = false
        unlock → return MOQ_OK
    if t->stopped || t->fatal || t->pump_exit:
        unlock → return MOQ_ERR_CLOSED
    unlock → return MOQ_DONE
```

## on_pump Termination

When `on_pump` returns nonzero:

1. The helper sets `t->pump_exit = true` under mutex.
2. Calls `mark_activity()` — wakes any `_wait` caller.
3. Returns `PICOQUIC_NO_ERROR_TERMINATE_PACKET_LOOP` from
   `loop_callback`.
4. The network thread exits `picoquic_packet_loop_v3` cleanly.
5. Any `_wait` caller sees `pump_exit` and returns
   `MOQ_ERR_CLOSED`.
6. `_stop` observes the already-exited thread (join returns
   immediately). Returns `MOQ_OK` unless `fatal` was also set
   (in which case `MOQ_ERR_CLOSED`).

The distinction between `pump_exit` and `fatal`: `pump_exit` is a
clean app-requested termination. `fatal` is an adapter/protocol
error. Both cause `_wait` to return `MOQ_ERR_CLOSED` — the app
cannot distinguish them via `_wait`, but can check `_is_fatal()`
to tell them apart.

## Stop Semantics

```
_stop(t):
    if t == NULL: return MOQ_ERR_INVAL

    lock(t->mutex)
    if t->network_thread_id == current_thread_id:
        unlock → return MOQ_ERR_WRONG_STATE
    if t->thread_ctx == NULL:
        bool was_fatal = t->fatal
        unlock → return was_fatal ? MOQ_ERR_CLOSED : MOQ_OK
    picoquic_network_thread_ctx_t *ctx = t->thread_ctx
    t->thread_ctx = NULL
    t->stopped = true
    t->activity_pending = true
    cond_broadcast(t->condvar)
    while t->wake_in_flight > 0:     /* wait for in-flight wakes */
        cond_wait(t->condvar, t->mutex)
    unlock(t->mutex)

    picoquic_delete_network_thread(ctx)

    lock(t->mutex)
    bool was_fatal = t->fatal       /* re-read after join */
    unlock(t->mutex)
    return was_fatal ? MOQ_ERR_CLOSED : MOQ_OK
```

`_stop` sets `stopped = true` and nulls `thread_ctx` under the
mutex, then waits for `wake_in_flight == 0` before unlocking.
This ensures no in-flight `_wake` call can touch a freed thread
context. After join, `fatal` is re-read because the network
thread may have set it during shutdown.

### Network-thread detection

The helper stores the network thread's ID (via `pthread_self()`
from the first `loop_callback` invocation) in
`t->network_thread_id`. `_stop` compares the caller's thread ID
under the mutex. If they match, it returns `MOQ_ERR_WRONG_STATE`
without touching `picoquic_delete_network_thread`.

This is a best-effort detection, not a hard guarantee — thread ID
reuse after join is theoretically possible but harmless here
because `_stop` is only unsafe from the network thread while it is
running.

## Ownership and Lifetime

```
moq_pq_threaded_t owns:
├── picoquic_quic_t
├── picoquic_network_thread_ctx_t* (mutex-protected; NULL after _stop)
├── moq_session_t* (mutex-protected; NULL in server mode before first connection)
├── moq_pq_conn_t* (mutex-protected; NULL in server mode before first connection)
├── pthread_mutex_t + pthread_cond_t
├── mutex-protected flags: activity_pending, wake_pending, stopped,
│   fatal, fatal_code, pump_exit
├── network_thread_id (mutex-protected)
└── copy of moq_pq_threaded_cfg_t tuning fields (read-only after create)
```

The caller owns:
- Any facade created on the session. Must be created from `on_pump`
  (network thread) and destroyed after `_stop` / before `_destroy`.
- Cert/key file paths (must remain valid until `_create` returns).
- The `on_pump`, `on_activity`, and `configure_quic` function
  pointers and their ctx.
- Any application-level queues used for cross-thread communication.

## Callback and Reentrancy Policy

| Callback | Thread | May call session/adapter/facade APIs? |
|----------|--------|---------------------------------------|
| `on_pump` | Network | Yes — the intended call site |
| `on_activity` | Network | No — signal-only |
| `configure_quic` | Caller of _create | No session/adapter exists yet; picoquic_quic_t config only |

- `on_pump` fires between `moq_pq_service` calls. The app ticks
  its facade, publishes objects, polls received objects, and
  processes queued work here.
- `on_activity` fires after the full cycle completes, after
  `mark_activity` sets the flag and broadcasts. It must only
  set flags or signal the app thread. No mutation APIs.
- The app thread must NOT call any session/adapter/facade API.

## Shutdown Sequence

```
1. App calls moq_pq_threaded_stop(t)
   ├── lock: detect not-network-thread (thread ID check)
   ├── lock: copy thread_ctx, null t->thread_ctx,
   │         set stopped = true, activity_pending = true,
   │         cond_broadcast (wakes _wait callers)
   ├── unlock
   └── picoquic_delete_network_thread(local_ctx):
       sets thread_should_close = 1
       closes wake-up pipe → poll faults → thread exits loop
       joins thread (blocks)
       frees thread context

2. App destroys its facade (moq_pub_destroy / moq_sub_destroy)
   (Safe: network thread is joined, no concurrent access)

3. App calls moq_pq_threaded_destroy(t)
   ├── moq_pq_conn_destroy (if non-NULL)
   ├── moq_session_destroy (if non-NULL)
   ├── picoquic_free
   ├── mutex/cond destroy
   └── free(t)
```

### Fatal shutdown

1. `moq_pq_service` returns < 0 in `loop_callback`.
2. Helper sets `fatal = true`, `fatal_code` under mutex.
3. `mark_activity()` — wakes any `_wait` caller.
4. `loop_callback` returns `PICOQUIC_NO_ERROR_TERMINATE_PACKET_LOOP`.
5. Network thread exits; `picoquic_delete_network_thread` join
   returns immediately.
6. App calls `_stop` (idempotent, returns `MOQ_ERR_CLOSED`) →
   destroys facade → `_destroy`.

## Error and Fatal Propagation

| API | On error |
|-----|----------|
| `_create` | Returns `MOQ_ERR_*`. No thread running; no `_stop` / `_destroy` needed. |
| `_stop` | `MOQ_OK` on clean exit. `MOQ_ERR_CLOSED` if fatal. `MOQ_ERR_WRONG_STATE` from network thread. |
| `_wait` | `MOQ_OK` on activity. `MOQ_DONE` on timeout. `MOQ_ERR_CLOSED` if fatal/stopped/pump_exit. |
| `_wake` | `MOQ_OK` on success (including coalesced). `MOQ_ERR_CLOSED` if stopped. `MOQ_ERR_INTERNAL` on pipe failure. |
| `_is_fatal` / `_fatal_code` | Reads under mutex. Safe from any thread. |
| `_session` / `_conn` | Reads under mutex. Returns NULL if not yet available (server mode). |

## Typical Usage: Subscriber App

```c
typedef struct {
    moq_subscriber_t *sub;
    moq_sub_track_t  *track;
    spsc_queue_t      inbox;   /* lock-free SPSC queue */
} my_app_t;

/* Runs on network thread */
int my_pump(moq_pq_threaded_t *t, uint64_t now, void *ctx) {
    my_app_t *app = ctx;
    moq_session_t *s = moq_pq_threaded_session(t);

    if (!app->sub) {
        moq_sub_cfg_t sc; moq_sub_cfg_init(&sc);
        sc.callbacks.ctx = app;
        sc.callbacks.on_subscribed = on_sub;
        moq_sub_create(s, moq_alloc_default(), &sc, &app->sub);
        moq_sub_track_cfg_t tc; moq_sub_track_cfg_init(&tc);
        /* ... configure track ... */
        moq_sub_subscribe(app->sub, &tc, now, &app->track);
    }

    moq_sub_tick(app->sub, now);

    moq_sub_object_t obj;
    while (moq_sub_poll_object(app->sub, &obj) == MOQ_OK)
        spsc_push(&app->inbox, obj);

    return 0;
}

/* App thread */
int main() {
    my_app_t app = {0};
    spsc_init(&app.inbox);

    moq_pq_threaded_cfg_t cfg;
    moq_pq_threaded_cfg_init(&cfg);
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.host = "localhost"; cfg.port = 4443;
    cfg.insecure_skip_verify = true;  /* demo only */
    cfg.on_pump = my_pump; cfg.on_pump_ctx = &app;

    moq_pq_threaded_t *t;
    moq_pq_threaded_create(&cfg, &t);

    while (!moq_pq_threaded_is_fatal(t)) {
        moq_result_t wr = moq_pq_threaded_wait(t, 100000);
        if (wr == MOQ_ERR_CLOSED) break;

        moq_sub_object_t obj;
        while (spsc_pop(&app.inbox, &obj)) {
            /* process object on app thread */
            moq_sub_object_cleanup(&obj);
        }
    }

    moq_pq_threaded_stop(t);
    moq_sub_destroy(app.sub);
    moq_pq_threaded_destroy(t);
}
```

## Test Plan

| # | Case | Method |
|---|------|--------|
| 1 | Clean lifecycle | Loopback: server + client wrappers, publish objects via on_pump, verify receipt via inbox queue |
| 2 | Stop while waiting | App thread in _wait, second thread calls _stop; verify _wait returns promptly with MOQ_ERR_CLOSED |
| 3 | Fatal detection | Configure tiny max_actions to force adapter fatal; verify _wait returns MOQ_ERR_CLOSED, _is_fatal true |
| 4 | Wake coalescing | Rapid _wake calls; verify second returns MOQ_OK without pipe write, no pipe exhaustion |
| 5 | Stop without wait | Create → stop → destroy without calling _wait |
| 6 | Double stop | Call stop twice; second returns MOQ_OK |
| 7 | on_pump creates facade | Verify facade creation in first on_pump call (session is valid, non-NULL) |
| 8 | on_pump nonzero return | on_pump returns 1; verify _wait returns MOQ_ERR_CLOSED, _is_fatal false |
| 9 | Null/invalid args | All public APIs handle NULL gracefully |
| 10 | Server: no client | Create server → stop → destroy; _session() returns NULL throughout |
| 11 | Facade destroy ordering | Destroy facade after _stop, before _destroy; verify no use-after-free |
| 12 | Stop from on_pump | on_pump calls _stop; verify MOQ_ERR_WRONG_STATE returned |
| 13 | configure_quic callback | Verify callback fires with valid quic context during _create |
| 14 | configure_quic failure | configure_quic returns nonzero; _create fails, no thread running |

Tests 1-4 use loopback (two wrappers in one process). Tests 5-8
and 9-14 can use a single wrapper. Test 3 requires constrained
session config.

## Non-Goals

- **Not core.** `moq-core` remains sans-I/O.
- **Not playback.** `moq-playback` is sans-I/O. Apps compose
  playback + threaded adapter at the application level.
- **Not a replacement for direct adapter use.** The existing
  `moq_pq_conn_t` + manual event loop pattern remains the primary
  integration path.
- **Not a generic thread pool.** One network thread per wrapper.
- **Not multi-connection server.** v0 supports exactly one active
  server connection. Multi-connection requires per-connection state
  management and is a future extension.

## Build Gate Recommendation

Gate behind `MOQ_BUILD_PQ_THREADED=OFF`.

Requires `MOQ_BUILD_ADAPTER_PICOQUIC=ON`. The helper links
`moq-adapter-picoquic` and depends on `picoquic_start_network_thread`
from picoquic's sockloop. No additional external dependencies beyond
what the adapter already requires.

Defaulting OFF avoids surprising pthread link requirements for
embedded targets that enable the adapter for single-threaded use.
