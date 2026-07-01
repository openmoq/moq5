# moq-adapter-picoquic

Bridges moq-core sessions to real QUIC transport via picoquic.

## moq_pq_conn_t (direct adapter)

The primary integration path. The application owns the picoquic
event loop (`picoquic_packet_loop`) and drives the adapter via
`moq_pq_callback` and `moq_pq_service`. Single-threaded, no
hidden state.

See `examples/picoquic/publisher.c` and `subscriber.c` for usage.

## moq_pq_threaded_t (threaded helper)

Optional convenience for applications that need picoquic on a
background thread. Wraps picoquic's `picoquic_start_network_thread`
with MoQ session/adapter lifecycle management.

Build: `cmake -DMOQ_BUILD_PQ_THREADED=ON -DMOQ_BUILD_ADAPTER_PICOQUIC=ON`

Link: `moq::adapter-picoquic-threaded`

### Threading model

```
App thread                        Network thread
──────────                        ──────────────
                                  picoquic event loop
                                  ├── moq_pq_service
enqueue work (app queue)          ├── on_pump():
moq_pq_threaded_wake(t)  ──────→ │   dequeue + process work
                                  │   call moq_pub_write / sub_poll
                                  ├── moq_pq_service
moq_pq_threaded_wait(t) ◄─────── └── mark_activity → condvar
read results from app queue
```

**The network thread is the only place to call session, adapter,
or facade APIs.** The app thread communicates via application-level
queues and uses `_wake` / `_wait` for signaling.

### Callback rules

| Callback | Thread | May call moq APIs? |
|----------|--------|--------------------|
| `on_pump` | Network | Yes |
| `on_activity` | Network | No — signal only |
| `configure_quic` | Caller of `_create` | picoquic_quic_t config only |

- `on_pump` runs between `moq_pq_service` calls. Create facades,
  tick them, publish objects, poll received objects here.
- `on_activity` fires after each pump cycle. Set a flag or signal
  a condvar to wake the app thread. No mutation APIs.
- Neither callback may call `moq_pq_threaded_stop`.

### Client example

```c
typedef struct {
    moq_subscriber_t *sub;
    spsc_queue_t      inbox;
} my_app_t;

int my_pump(moq_pq_threaded_t *t, uint64_t now, void *ctx) {
    my_app_t *app = ctx;
    if (!app->sub) {
        moq_sub_cfg_t sc; moq_sub_cfg_init(&sc);
        moq_sub_create(moq_pq_threaded_session(t), ...);
        moq_sub_subscribe(app->sub, ...);
    }
    moq_sub_tick(app->sub, now);
    moq_sub_object_t obj;
    while (moq_sub_poll_object(app->sub, &obj) == MOQ_OK)
        spsc_push(&app->inbox, obj);
    return 0;
}

int main() {
    my_app_t app = {0};

    moq_pq_threaded_cfg_t cfg;
    moq_pq_threaded_cfg_init(&cfg);
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.host = "relay.example.com";
    cfg.port = 4443;
    /* TLS certificate verification is ON by default (system CA chain). Do NOT
     * disable it against a real relay. For a LOCAL/self-signed test relay only,
     * you may set cfg.insecure_skip_verify = true (accepts any cert). */
    cfg.on_pump = my_pump;
    cfg.on_pump_ctx = &app;

    moq_pq_threaded_t *t;
    moq_pq_threaded_create(&cfg, &t);

    while (!moq_pq_threaded_is_fatal(t)) {
        moq_result_t r = moq_pq_threaded_wait(t, 100000);
        if (r == MOQ_ERR_CLOSED) break;
        moq_sub_object_t obj;
        while (spsc_pop(&app.inbox, &obj)) {
            /* process on app thread */
            moq_sub_object_cleanup(&obj);
        }
    }

    moq_pq_threaded_stop(t);
    moq_sub_destroy(app.sub);
    moq_pq_threaded_destroy(t);
}
```

### Server example

```c
int my_server_pump(moq_pq_threaded_t *t, uint64_t now, void *ctx) {
    my_server_t *app = ctx;
    if (!app->pub) {
        moq_pub_cfg_t pc; moq_pub_cfg_init(&pc);
        pc.accept_mode = MOQ_PUB_ACCEPT_ALL;
        moq_pub_create(moq_pq_threaded_session(t), ...);
        moq_pub_add_track(app->pub, ...);
    }
    moq_pub_tick(app->pub, now);
    /* publish objects from app queue */
    return 0;
}

int main() {
    moq_pq_threaded_cfg_t cfg;
    moq_pq_threaded_cfg_init(&cfg);
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.cert_path = "cert.pem";
    cfg.key_path = "key.pem";
    cfg.port = 4443;
    cfg.on_pump = my_server_pump;
    cfg.on_pump_ctx = &app;

    moq_pq_threaded_t *t;
    moq_pq_threaded_create(&cfg, &t);

    /* _session() returns NULL until a client connects */
    /* on_pump is not called until then */

    while (!moq_pq_threaded_is_fatal(t)) {
        moq_result_t r = moq_pq_threaded_wait(t, 100000);
        if (r == MOQ_ERR_CLOSED) break;
    }

    moq_pq_threaded_stop(t);
    moq_pub_destroy(app.pub);
    moq_pq_threaded_destroy(t);
}
```

### Server mode details

- Session and adapter are created lazily on the network thread
  when the first client connection fires `picoquic_callback_ready`.
- `_session()` and `_conn()` return NULL until that happens.
- `on_pump` is not called until session/adapter exist.
- v0 supports one active server connection. Additional connections
  are ignored by the helper without replacing the existing session.
- `_stop()` before any client connects is safe and clean.

### Lifecycle summary

```
_create()    → allocates wrapper, picoquic context, starts thread
_wake()      → wakes network thread to run on_pump
_wait()      → blocks app thread until activity/timeout/stop
_stop()      → joins thread (idempotent, must not be from on_pump)
_destroy()   → frees adapter, session, picoquic, wrapper
```

Shutdown order: `stop` → destroy facade → `destroy`.

### Build integration

```cmake
find_package(libmoq REQUIRED COMPONENTS adapter-picoquic-threaded)
target_link_libraries(myapp PRIVATE moq::adapter-picoquic-threaded)
```

The threaded target transitively links `moq::adapter-picoquic`
and `Threads::Threads`.
