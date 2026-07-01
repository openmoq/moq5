# Integration Guide

This document shows how to wire LibMoQ into a transport adapter. The
library is sans-I/O: it never touches sockets. Your adapter code bridges
between the QUIC stack and the session state machine.

## Adapter Loop

A complete adapter loop has six responsibilities:

1. Feed incoming control channel bytes to the session.
2. Feed incoming data stream bytes to the session.
3. Poll and send outbound actions (control bytes, data bytes, resets).
4. Poll and handle application events.
5. Clean up owned resources on polled actions and events.
6. Advance virtual time periodically.

```c
#include <moq/moq.h>

void adapter_loop(moq_session_t *session, quic_conn_t *conn)
{
    uint64_t now = get_time_us();

    /* 1. Feed incoming control bytes. */
    uint8_t recv_buf[4096];
    size_t recv_len = quic_recv_control(conn, recv_buf, sizeof(recv_buf));
    if (recv_len > 0)
        moq_session_on_control_bytes(session, recv_buf, recv_len, now);

    /* 2. Feed incoming data stream bytes. */
    moq_stream_ref_t ref;
    uint8_t data_buf[4096];
    size_t data_len;
    bool fin;
    while (quic_recv_data(conn, &ref, data_buf, &data_len, &fin)) {
        moq_result_t rc = moq_session_on_data_bytes(
            session, ref, data_buf, data_len, fin, now);
        if (rc == MOQ_ERR_WOULD_BLOCK)
            break; /* drain actions/events and retry */
    }

    /* 3. Send outbound actions. */
    moq_action_t acts[16];
    size_t na = moq_session_poll_actions(session, acts, 16);
    for (size_t i = 0; i < na; i++) {
        switch (acts[i].kind) {
        case MOQ_ACTION_SEND_CONTROL:
            quic_send_control(conn,
                acts[i].u.send_control.data,
                acts[i].u.send_control.len);
            break;
        case MOQ_ACTION_SEND_DATA:
            quic_send_data(conn,
                acts[i].u.send_data.stream_ref,
                acts[i].u.send_data.header,
                acts[i].u.send_data.header_len,
                acts[i].u.send_data.payload,
                acts[i].u.send_data.fin);
            break;
        case MOQ_ACTION_RESET_DATA:
            quic_reset_stream(conn,
                acts[i].u.reset_data.stream_ref,
                acts[i].u.reset_data.error_code);
            break;
        case MOQ_ACTION_STOP_DATA:
            quic_stop_sending(conn,
                acts[i].u.stop_data.stream_ref,
                acts[i].u.stop_data.error_code);
            break;
        case MOQ_ACTION_CLOSE_SESSION:
            quic_close(conn, acts[i].u.close_session.code);
            break;
        }
        moq_action_cleanup(&acts[i]); /* release owned payload refs */
    }

    /* 4. Handle application events. */
    moq_event_t evts[16];
    size_t ne = moq_session_poll_events(session, evts, 16);
    for (size_t i = 0; i < ne; i++) {
        switch (evts[i].kind) {
        case MOQ_EVENT_SETUP_COMPLETE:
            on_setup_complete(&evts[i].u.setup_complete);
            break;
        case MOQ_EVENT_SESSION_CLOSED:
            on_session_closed(&evts[i].u.closed);
            break;
        case MOQ_EVENT_SUBSCRIBE_REQUEST:
            on_subscribe_request(session, &evts[i].u.subscribe_request);
            break;
        case MOQ_EVENT_SUBSCRIBE_OK:
            on_subscribe_ok(&evts[i].u.subscribe_ok);
            break;
        case MOQ_EVENT_SUBSCRIBE_ERROR:
            on_subscribe_error(&evts[i].u.subscribe_error);
            break;
        case MOQ_EVENT_REQUEST_READY:
            retry_blocked_requests();
            break;
        case MOQ_EVENT_OBJECT_RECEIVED:
            on_object(session, &evts[i].u.object_received);
            break;
        case MOQ_EVENT_NAMESPACE_PUBLISHED:
            on_namespace(&evts[i].u.namespace_published);
            break;
        case MOQ_EVENT_NAMESPACE_DONE:
            on_namespace_done(&evts[i].u.namespace_done);
            break;
        case MOQ_EVENT_NAMESPACE_REJECTED:
            on_namespace_rejected(&evts[i].u.namespace_rejected);
            break;
        case MOQ_EVENT_NAMESPACE_ACCEPTED:
            on_namespace_accepted(&evts[i].u.namespace_accepted);
            break;
        case MOQ_EVENT_NAMESPACE_CANCELLED:
            on_namespace_cancelled(&evts[i].u.namespace_cancelled);
            break;
        default:
            break; /* ignore unknown future event kinds */
        }
        moq_event_cleanup(&evts[i]); /* release owned payload/properties refs */
    }

    /* 5. Check deadline and advance time. */
    uint64_t deadline = moq_session_next_deadline_us(session);
    if (deadline != UINT64_MAX && now >= deadline)
        moq_session_tick(session, now);
}
```

The adapter should also arm a transport-layer timer for the next
deadline returned by `moq_session_next_deadline_us()`. When that
timer fires, call `moq_session_tick()` with the current time.
`UINT64_MAX` means no deadline is pending.

## Owned Resource Cleanup

Some polled records carry owned `moq_rcbuf_t` references:

- **SEND_DATA actions**: `payload` field (may be NULL for header-only).
  Call `moq_action_cleanup` after processing.
- **OBJECT_RECEIVED events**: `payload` and `properties` fields.
  Call `moq_event_cleanup` after processing.

Both cleanup functions are idempotent and safe to call on any action/event
kind. The recommended pattern: call cleanup unconditionally after
processing every polled record.

## Data Stream Identity

Data streams use `moq_stream_ref_t`, an opaque 64-bit value:

- **Outgoing**: the session mints stream_refs in SEND_DATA actions. The
  adapter maps each ref to a QUIC unidirectional stream.
- **Incoming**: the adapter assigns stream_refs when receiving new QUIC
  streams. The session uses these refs to track per-stream parse state.

Stream_refs must be session-unique and not recycled by the adapter.

## Retry After Backpressure

Several operations can return backpressure errors:

| Error | Meaning | Retry strategy |
|-------|---------|----------------|
| `MOQ_ERR_WOULD_BLOCK` | Action or event queue full | Drain outputs, then call again |
| `MOQ_ERR_BUFFER` | Send or receive buffer full | Drain outputs or feed fewer bytes |
| `MOQ_ERR_REQUEST_BLOCKED` | No request capacity from peer | Wait for `MOQ_EVENT_REQUEST_READY` |

For `MOQ_ERR_WOULD_BLOCK` on `on_data_bytes`, retry with:
`moq_session_on_data_bytes(s, stream_ref, NULL, 0, false, now_us)`

For `MOQ_ERR_WOULD_BLOCK` and `MOQ_ERR_BUFFER` on incoming control
messages, the message stays buffered internally. Call
`moq_session_process_pending(s, now)` after draining outputs to retry.

For `MOQ_ERR_REQUEST_BLOCKED`, the session automatically notifies the
peer that requests are blocked. When the peer grants more request
capacity, the session emits `MOQ_EVENT_REQUEST_READY`.

## Peer STOP_SENDING

When the adapter receives a QUIC STOP_SENDING on a data stream the
session opened, call `moq_session_on_data_stop`. The session queues
a RESET_DATA action for that stream (the adapter must send RESET_STREAM).

## Session Creation

```c
moq_session_cfg_t cfg;
/* Use the sized initializer whenever you set fields past alloc/perspective:
 * the pointer-only moq_session_cfg_init() stamps only the v0 prefix, so any
 * later field (request capacity, caps, version, timeouts, ...) would be ignored. */
moq_session_cfg_init_sized(&cfg, sizeof(cfg), &my_allocator, MOQ_PERSPECTIVE_CLIENT);
cfg.send_request_capacity = true;
cfg.initial_request_capacity = 100;

moq_session_t *session = NULL;
moq_result_t rc = moq_session_create(&cfg, now_us, &session);
if (rc < 0) { /* handle error */ }

/* Client: call start to initiate setup handshake. */
rc = moq_session_start(session, now_us);

/* Server: ready to receive peer setup immediately after create. */
```

## Cleanup

```c
moq_session_destroy(session);
/* All internal state freed via the configured allocator. */
```

`destroy(NULL)` is a no-op. Destroying a session that is not in the
`CLOSED` state is an unclean shutdown but does not leak memory.
