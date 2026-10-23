# moq-adapter-picoquic

Bridges moq-core sessions to real QUIC transport via picoquic.

## Stream scheduling

PicoQUIC and PicoWT schedule data streams per drafts 16/18 §7.2 and draft 22
§5.1.2: subscriber priority,
then publisher priority, then group in the request's group order (MOQT 7.1),
then lowest subgroup ID. Streams of different requests that tie on all of
these share bandwidth round-robin. Applications choose the priorities: the
adapters do not infer them from a media codec or track name.

The adapter ranks queued streams and uses even native priority values for
round-robin service among equal ranks. Control traffic uses priority zero;
data ranks start at two, one per distinct key. PicoQUIC's eight-bit priority
space permits 127 data ranks. Additional distinct keys share the least urgent
native level (254). This preserves a monotone mapping but makes those ranks
ties at the transport; it does not block admission of a more urgent stream.
Equal keys consume one rank regardless of stream count.
The queue's byte budget controls admission: a write is refused once queued
bytes reach the cap, and accepted whole below it, so the bridge's
best-priority write gets in first when capacity returns.
Standalone zero-byte FIN markers each consume one unit of that budget until
drained or discarded; adding FIN to an existing data chunk needs no extra unit.
This bounds accepted markers separately from their zero payload size.

The core emits each stream's priority before its first write, so it survives
subgroup retirement and a blocked transport open/write. A subscriber priority
update re-ranks ordinary streams of that request, including data already queued.
Draft-22 fills retain their independently resolved child parameters.
A FETCH stream retains the priority of its held objects until those bytes
drain from the adapter queue. Objects with the same publisher priority may
be queued together; a different priority waits at the bridge, so it cannot
demote or promote earlier objects. The initial FETCH preamble can take the
first object's priority. No peer acknowledgment is required for this change.
Draft-22 fills carry their parent identity and current object's group through
the bridge; crossing a group waits for the earlier class to drain too.
Within a group, a fill precedes subscription data. Between groups the parent
order applies, except that a fill with the opposite order yields to
subscription data.

Overlapping fills can make these pairwise recommendations cyclic. The bridge
collapses each cyclic component to a shared rank, preserving every precedence
edge between components. Equal ranks take turns among continuously eligible
streams; higher priorities or transport blockage can still delay them. This
is a policy for conflicting recommendations, not a claim to satisfy all edges
of a cycle. The heap retains full logical ranks; the transport key saturates
its position at 65535 before native rank compression. Already transmitted
bytes cannot be reordered.

Each dirty parent snapshot uses linear scratch and quadratic work in its
admitted streams, without recursion. Same-class writes do not rebuild it;
data pumps share a limit of 256 attempts per service call before yielding
remaining work.
Separate request-stream ranks and per-object datagram
priority are not implemented. No congestion-latency improvement is implied
without measurement.

The adapter queue holds 128 KiB by default (PicoQUIC: `send_queue_cap_bytes`).
The bridge retains the rest, up to 1 MiB in total (at least 128 KiB in the
bridge), and hands it over in priority order. When a stream's queued bytes
drain, the adapter reports it (`MOQ_TRANSPORT_CAP_STREAM_DRAINED`), so the
bridge sets the priority again on the next write.

The bridge priority callback is optional and size-gated. Other adapters retain
their existing scheduling behavior when they do not implement it. Without
the drained notice, per-write reprioritization remains best effort; the bridge
does not wait for a callback that the endpoint cannot provide.

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
enqueue work (app queue)          ├── on_lane_pump():
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
| `on_lane_pump` | Network | Yes |
| `on_activity` | Network | No — signal only |
| `configure_quic` | Caller of `_create` | picoquic_quic_t config only |

- `on_lane_pump` runs between `moq_pq_service` calls. Create facades,
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

int my_pump(moq_pq_threaded_t *t, moq_pq_threaded_lane_t *lane,
            uint64_t now, void *ctx) {
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
    moq_pq_threaded_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.host = "relay.example.com";
    cfg.port = 4443;
    /* TLS certificate verification is ON by default. OpenSSL builds use the
     * system CA chain; mbedTLS-only embedded builds require an explicit CA file
     * via the sized cfg.ca_file field or the service ca_file field. Do NOT
     * disable verification against a real relay. For a LOCAL/self-signed test
     * relay only, you may set cfg.insecure_skip_verify = true (accepts any
     * cert). */
    cfg.on_lane_pump = my_pump;
    cfg.on_lane_pump_ctx = &app;

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
int my_server_pump(moq_pq_threaded_t *t, moq_pq_threaded_lane_t *lane,
            uint64_t now, void *ctx) {
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
    cfg.on_lane_pump = my_server_pump;
    cfg.on_lane_pump_ctx = &app;

    moq_pq_threaded_t *t;
    moq_pq_threaded_create(&cfg, &t);

    /* _session() returns NULL until a client connects */
    /* on_lane_pump is not called until then */

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

- Each client's session and adapter are created lazily on the network thread
  when its connection becomes ready.
- `_session()` and `_conn()` return NULL until that happens.
- `on_lane_pump` is not called until session/adapter exist.
- Up to `cfg.max_connections` clients are accepted concurrently (0 selects
  the default of 1024); connections beyond the cap are rejected. Iterate them
  with `moq_pq_threaded_lane_next_conn()`. The legacy `_session()` and `_conn()`
  accessors return the first live connection.
- `_stop()` before any client connects is safe and clean.

### Lifecycle summary

```
_create()    → allocates wrapper, picoquic context, starts thread
_wake()      → wakes network thread to run on_lane_pump
_wait()      → blocks app thread until activity/timeout/stop
_stop()      → joins thread (idempotent, must not be from on_lane_pump)
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

## Provider source baseline

The pinned development picoquic source is
70b5fd5aafdd2d6c349ff783202ece9d778bcaf2 (release `v2026.10.22.70b5fd5`)
with the patches `scripts/setup_picoquic_deps.sh` applies, built against
picotls 7f0d38c1c54c920d56b4e13ceb1c2dfa2b1325e1. Other source trees given
through `MOQ_PICOQUIC_SOURCE_DIR` are used as they are: no version is
checked, and no Git metadata is required.

This release advertises and accepts both the legacy RESET_STREAM_AT transport
parameter (`0x17f7586d2cb571`) and the draft-11 parameter (`0x1d`). That support
comes from upstream, not a LibMoQ patch. It does not change the MoQ draft offer
or enable reliable reset when the peer advertises neither parameter. The
remaining development patches and their removal conditions are documented in
[`patches/picoquic/README.md`](../../patches/picoquic/README.md); the unmodified
release does not yet supply all contracts required below.

On that baseline picoquic-core needs nothing from picoquic-log, so libmoq
links the provider's targets with picoquic's own dependency edges only
(picoquic-log on picoquic-core, picohttp-core and the test library on
picoquic-log). Automatic accommodation of the older core/log cycle (for
example 55b473e2, whose core calls `picoquic_set_qlog` in the log library)
is retired, and there is no configure-time capability check. Consumers of
such a source tree MAY fail to link: its static archives still build with
the symbol unresolved, and another dependency may or may not supply
picoquic-log.

## Provider close-reason ownership

The adapter requires picoquic to copy accepted `picoquic_close_ex` reasons
before returning and retain them through connection deletion callbacks.
`scripts/setup_picoquic_deps.sh` applies patch 0005 to the pinned provider.
External provider packages must carry the equivalent implementation and
advertise `PICOQUIC_CLOSE_REASON_COPIED=1` in their matching public header.
The adapter rejects headers without this contract. Rebuild private-header
consumers and provider binaries together after applying the patch.

## Provider terminal observation

`scripts/setup_picoquic_deps.sh` also applies patch 0006, which advertises
`PICOQUIC_STREAM_TERMINAL_CONTRACT_VERSION` 1: the exact STOP_SENDING error
(`picoquic_get_remote_stop_error`) and an opt-in callback once every byte and
the FIN of a local unidirectional stream are acknowledged
(`picoquic_enable_stream_send_commit`). With it the raw and WebTransport
adapters report commitment to the bridge (`MOQ_TRANSPORT_CAP_COMMIT_REPORT`).
A reset accepted on the stream wins over any later ACK. A provider header
without the macro builds the adapters without commit reports. The patches
apply in the order declared by `patches/picoquic/series`; use
`scripts/setup_picoquic_deps.sh` to prepare the complete provider input.

## Provider terminal cancellation

The raw and WebTransport adapters require
`PICOQUIC_TERMINAL_STREAM_ALREADY_CLOSED=1` in the provider's matching public
header. Patch 0008 supplies this contract for the pinned source. An external
provider must carry the equivalent implementation, not only define the macro.
Rebuild the provider and adapters together; an installed provider's headers and
libraries must describe the same implementation.

Reset and STOP operations distinguish a terminal direction from an invalid
stream ID before changing stream state or its application context. A terminal
direction returns `PICOQUIC_ERROR_STREAM_ALREADY_CLOSED`, which the adapter
treats as an OK no-op: no extra frame, callback or stream creation. Unknown,
out-of-range and wrong-direction IDs remain errors. A FIN that has been sent
but not acknowledged is still resettable; a FIN acknowledged through its final
byte is terminal. STOP ends only the receive direction of a bidirectional
stream, never its independent send direction.

This cancellation contract is required whether or not terminal observation is
enabled. Omitting patch 0006 is supported for drafts 16 and 18, but does not
remove the close-reason or terminal-cancellation requirements.
