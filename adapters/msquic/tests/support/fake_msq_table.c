#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "fake_msq_table.h"

/* Fail fast (always on, never compiled out) on any input the fixed-size
 * fake buffers cannot represent. A caller feeding an out-of-range receive
 * is a test bug; aborting is preferable to a silent out-of-bounds access. */
#define FAKE_MSQ_REQUIRE(cond) do { \
    if (!(cond)) { \
        fprintf(stderr, "fake_msq_table: %s:%d: requirement failed: %s\n", \
                __FILE__, __LINE__, #cond); \
        abort(); \
    } \
} while (0)

static fake_msq_stream_t *stream_of(HQUIC h)
{
    return (fake_msq_stream_t *)h;
}

/* Read a handle's discriminator without aliasing it as either struct: both
 * tagged objects begin with the uint32_t tag, so the first bytes of the
 * object representation carry it. */
static uint32_t kind_of(HQUIC h)
{
    uint32_t kind = 0;

    memcpy(&kind, h, sizeof(kind));
    return kind;
}

static QUIC_STATUS QUIC_API f_stream_open(HQUIC conn,
                                          QUIC_STREAM_OPEN_FLAGS flags,
                                          QUIC_STREAM_CALLBACK_HANDLER cb,
                                          void *ctx, HQUIC *out)
{
    fake_msq_t *f = (fake_msq_t *)conn;

    if (f->stream_count >= FAKE_MSQ_MAX_STREAMS)
        return QUIC_STATUS_OUT_OF_MEMORY;
    fake_msq_stream_t *st = &f->streams[f->stream_count++];
    memset(st, 0, sizeof(*st));
    st->kind = FAKE_MSQ_KIND_STREAM;
    st->owner = f;
    st->in_use = true;
    st->cb = cb;
    st->ctx = ctx;
    st->uni = (flags & QUIC_STREAM_OPEN_FLAG_UNIDIRECTIONAL) != 0;
    st->receive_enabled = true;
    *out = (HQUIC)st;
    return QUIC_STATUS_SUCCESS;
}

static QUIC_STATUS QUIC_API f_stream_start(HQUIC h,
                                           QUIC_STREAM_START_FLAGS flags)
{
    fake_msq_stream_t *st = stream_of(h);
    fake_msq_t *f = st->owner;

    if (f->stream_start_fails > 0) {
        f->stream_start_fails--;
        return QUIC_STATUS_ABORTED; /* refused before any state moves */
    }
    st->started = true;
    st->start_flags = flags;
    /* real QUIC numbering from the fake's own counters, matching what
     * a correct adapter must have computed */
    if (f->force_next_id != UINT64_MAX) {
        st->id = f->force_next_id;
        f->force_next_id = UINT64_MAX;
    } else if (st->uni) {
        st->id = (f->is_client ? 2u : 3u) + (f->uni_count << 2);
    } else {
        st->id = (f->is_client ? 0u : 1u) + (f->bidi_count << 2);
    }
    if (st->uni)
        f->uni_count++;
    else
        f->bidi_count++;
    return QUIC_STATUS_PENDING; /* always queued, like the real thing */
}

static QUIC_STATUS QUIC_API f_stream_send(HQUIC h,
                                          const QUIC_BUFFER *bufs,
                                          uint32_t count,
                                          QUIC_SEND_FLAGS flags,
                                          void *client_ctx)
{
    fake_msq_stream_t *st = stream_of(h);
    fake_msq_t *f = st->owner;

    if (f->stream_send_fails > 0) {
        f->stream_send_fails--;
        return QUIC_STATUS_ABORTED;
    }
    if (f->send_count >= FAKE_MSQ_MAX_SENDS)
        return QUIC_STATUS_OUT_OF_MEMORY;
    fake_msq_send_t *s = &f->sends[f->send_count++];
    memset(s, 0, sizeof(*s));
    s->stream = st;
    s->flags = flags;
    s->client_ctx = client_ctx;
    s->buf_count = count;
    if (count > 0) {
        s->data = bufs[0].Buffer;
        s->len = bufs[0].Length;
        for (uint32_t i = 0; i < count; i++) {
            s->total += bufs[i].Length;
            if (s->bytes_len + bufs[i].Length <= sizeof(s->bytes)) {
                memcpy(s->bytes + s->bytes_len, bufs[i].Buffer,
                       bufs[i].Length);
                s->bytes_len += bufs[i].Length;
            }
        }
    }
    return QUIC_STATUS_SUCCESS;
}

static QUIC_STATUS QUIC_API f_stream_shutdown(
    HQUIC h, QUIC_STREAM_SHUTDOWN_FLAGS flags, QUIC_UINT62 code)
{
    fake_msq_stream_t *st = stream_of(h);

    st->shutdown_calls++;
    st->last_shutdown_flags = flags;
    st->last_shutdown_code = code;
    /* record in order; announce the first call that no longer fits rather
     * than overwriting an earlier entry */
    if (st->shutdown_log_len < FAKE_MSQ_SHUTDOWN_LOG) {
        st->shutdown_flags_log[st->shutdown_log_len] = (uint32_t)flags;
        st->shutdown_codes_log[st->shutdown_log_len] = code;
        st->shutdown_log_len++;
    } else {
        st->shutdown_log_overflow = true;
    }
    return QUIC_STATUS_SUCCESS;
}

static void QUIC_API f_stream_close(HQUIC h)
{
    stream_of(h)->closed = true;
}

static QUIC_STATUS QUIC_API f_stream_receive_set_enabled(HQUIC h,
                                                         BOOLEAN enabled)
{
    fake_msq_stream_t *st = stream_of(h);
    fake_msq_t *f = st->owner;

    if (f->recv_set_enabled_fails > 0) {
        f->recv_set_enabled_fails--;
        return QUIC_STATUS_ABORTED;
    }
    st->receive_set_enabled += enabled ? 1 : -1;
    st->receive_enabled = enabled;
    /* re-enabling arms redelivery of any held tail; the test drives the
     * actual re-indication via fake_msq_redeliver_held (kept explicit so
     * the callback is never re-entered from inside this downcall). */
    if (enabled)
        st->recv_disabled = false;
    return QUIC_STATUS_SUCCESS;
}

static QUIC_STATUS QUIC_API f_datagram_send(HQUIC conn,
                                            const QUIC_BUFFER *bufs,
                                            uint32_t count,
                                            QUIC_SEND_FLAGS flags,
                                            void *client_ctx)
{
    fake_msq_t *f = (fake_msq_t *)conn;

    (void)flags;
    if (f->dgram_send_fails > 0) {
        f->dgram_send_fails--;
        return QUIC_STATUS_ABORTED;
    }
    if (f->dgram_count >= FAKE_MSQ_MAX_DGRAMS)
        return QUIC_STATUS_OUT_OF_MEMORY;
    fake_msq_dgram_t *d = &f->dgrams[f->dgram_count++];
    memset(d, 0, sizeof(*d));
    d->client_ctx = client_ctx;
    if (count > 0) {
        d->data = bufs[0].Buffer;
        d->len = bufs[0].Length;
    }
    return QUIC_STATUS_SUCCESS;
}

static void QUIC_API f_conn_shutdown(HQUIC conn,
                                     QUIC_CONNECTION_SHUTDOWN_FLAGS flags,
                                     QUIC_UINT62 code)
{
    fake_msq_t *f = (fake_msq_t *)conn;

    (void)flags;
    atomic_fetch_add(&f->conn_shutdowns, 1);
    f->last_conn_shutdown_code = code;
    if (f->on_conn_shutdown != NULL)
        f->on_conn_shutdown(f, f->on_conn_shutdown_ctx);
}

static void QUIC_API f_conn_close(HQUIC conn)
{
    fake_msq_t *f = (fake_msq_t *)conn;

    atomic_fetch_add(&f->conn_closes, 1);
}

/* Mirror of the adapter's laundering: SetCallbackHandler carries the handler
 * as void*, and ISO C forbids a direct object<->function pointer cast. */
static QUIC_STREAM_CALLBACK_HANDLER f_ptr_to_stream_cb(void *cb)
{
    union { void *obj; QUIC_STREAM_CALLBACK_HANDLER fn; } u = { .obj = cb };
    return u.fn;
}

static QUIC_CONNECTION_CALLBACK_HANDLER f_ptr_to_conn_cb(void *cb)
{
    union { void *obj; QUIC_CONNECTION_CALLBACK_HANDLER fn; } u = { .obj = cb };
    return u.fn;
}

static void QUIC_API f_set_callback_handler(HQUIC h, void *cb, void *ctx)
{
    if (kind_of(h) == FAKE_MSQ_KIND_CONN) {
        fake_msq_t *f = (fake_msq_t *)h;

        f->conn_cb = f_ptr_to_conn_cb(cb);
        f->conn_ctx = ctx;
        return;
    }
    FAKE_MSQ_REQUIRE(kind_of(h) == FAKE_MSQ_KIND_STREAM);
    fake_msq_stream_t *st = stream_of(h);

    st->cb = f_ptr_to_stream_cb(cb);
    st->ctx = ctx;
}

static QUIC_STATUS QUIC_API f_conn_set_configuration(HQUIC conn,
                                                     HQUIC configuration)
{
    fake_msq_t *f = (fake_msq_t *)conn;

    (void)configuration;
    atomic_fetch_add(&f->conn_set_configs, 1);
    return QUIC_STATUS_SUCCESS;
}

static QUIC_STATUS QUIC_API f_get_param(HQUIC h, uint32_t param,
                                        uint32_t *len, void *buf)
{
    if (param == QUIC_PARAM_STREAM_ID && *len >= sizeof(uint64_t)) {
        *(uint64_t *)buf = stream_of(h)->id;
        *len = sizeof(uint64_t);
        return QUIC_STATUS_SUCCESS;
    }
    return QUIC_STATUS_NOT_SUPPORTED;
}

void fake_msq_init(fake_msq_t *f, bool is_client)
{
    memset(f, 0, sizeof(*f));
    f->kind = FAKE_MSQ_KIND_CONN;
    atomic_init(&f->conn_shutdowns, 0);
    atomic_init(&f->conn_closes, 0);
    atomic_init(&f->conn_set_configs, 0);
    f->is_client = is_client;
    f->force_next_id = UINT64_MAX;
    f->api.StreamOpen = f_stream_open;
    f->api.StreamStart = f_stream_start;
    f->api.StreamSend = f_stream_send;
    f->api.StreamShutdown = f_stream_shutdown;
    f->api.StreamClose = f_stream_close;
    f->api.StreamReceiveSetEnabled = f_stream_receive_set_enabled;
    f->api.ConnectionShutdown = f_conn_shutdown;
    f->api.ConnectionClose = f_conn_close;
    f->api.ConnectionSetConfiguration = f_conn_set_configuration;
    f->api.DatagramSend = f_datagram_send;
    f->api.SetCallbackHandler = f_set_callback_handler;
    f->api.GetParam = f_get_param;
}

const QUIC_API_TABLE *fake_msq_table(fake_msq_t *f)
{
    return &f->api;
}

HQUIC fake_msq_conn_handle(fake_msq_t *f)
{
    return (HQUIC)f;
}

bool fake_msq_conn_cb_installed(const fake_msq_t *f)
{
    return f->conn_cb != NULL;
}

QUIC_STATUS fake_msq_deliver_conn_event(fake_msq_t *f,
                                        QUIC_CONNECTION_EVENT *ev)
{
    FAKE_MSQ_REQUIRE(f->conn_cb != NULL);
    return f->conn_cb(fake_msq_conn_handle(f), f->conn_ctx, ev);
}

fake_msq_stream_t *fake_msq_stream_at(fake_msq_t *f, int index)
{
    if (index < 0 || index >= f->stream_count)
        return NULL;
    return &f->streams[index];
}

fake_msq_stream_t *fake_msq_peer_stream(fake_msq_t *f, uint64_t id,
                                        bool uni)
{
    if (f->stream_count >= FAKE_MSQ_MAX_STREAMS)
        return NULL;
    fake_msq_stream_t *st = &f->streams[f->stream_count++];
    memset(st, 0, sizeof(*st));
    st->kind = FAKE_MSQ_KIND_STREAM;
    st->owner = f;
    st->in_use = true;
    st->id = id;
    st->uni = uni;
    st->receive_enabled = true;
    return st;
}

void fake_msq_deliver_start_complete(fake_msq_stream_t *st,
                                     QUIC_STATUS status)
{
    QUIC_STREAM_EVENT ev;

    memset(&ev, 0, sizeof(ev));
    ev.Type = QUIC_STREAM_EVENT_START_COMPLETE;
    ev.START_COMPLETE.Status = status;
    ev.START_COMPLETE.ID = st->id;
    ev.START_COMPLETE.PeerAccepted = TRUE;
    st->cb((HQUIC)st, st->ctx, &ev);
}

bool fake_msq_deliver_send_complete(fake_msq_t *f, bool canceled)
{
    for (int i = 0; i < f->send_count; i++) {
        fake_msq_send_t *s = &f->sends[i];

        if (s->completed)
            continue;
        s->completed = true;
        QUIC_STREAM_EVENT ev;
        memset(&ev, 0, sizeof(ev));
        ev.Type = QUIC_STREAM_EVENT_SEND_COMPLETE;
        ev.SEND_COMPLETE.Canceled = canceled ? TRUE : FALSE;
        ev.SEND_COMPLETE.ClientContext = s->client_ctx;
        s->stream->cb((HQUIC)s->stream, s->stream->ctx, &ev);
        return true;
    }
    return false;
}

#define FAKE_MSQ_RECV_BUFS 16

/* Deliver a RECEIVE event and honor the partial-acceptance the callback
 * reports via RECEIVE.TotalBufferLength (bytes consumed). Whatever the
 * app did not consume is stashed for redelivery and the stream's delivery
 * is marked disabled — exactly how MsQuic holds unconsumed bytes and
 * stops indicating RECEIVE until StreamReceiveSetEnabled(TRUE). */
static void deliver_core(fake_msq_stream_t *st,
                         const uint8_t *const *bufs, const size_t *lens,
                         uint32_t count, bool fin)
{
    QUIC_BUFFER arr[FAKE_MSQ_RECV_BUFS];
    uint8_t flat[FAKE_MSQ_HELD_MAX];
    uint64_t total = 0;
    QUIC_STREAM_EVENT ev;

    /* Bounds are validated up front and every access below is provably in
     * range: too many buffers, a length that would not fit a QUIC_BUFFER
     * (size_t -> uint32_t), a length larger than the flatten buffer, or a
     * cumulative total exceeding FAKE_MSQ_HELD_MAX all abort here rather
     * than read/write out of bounds. */
    FAKE_MSQ_REQUIRE(count <= FAKE_MSQ_RECV_BUFS);
    for (uint32_t i = 0; i < count; i++) {
        size_t len = lens[i];

        FAKE_MSQ_REQUIRE(len <= UINT32_MAX);         /* fits QUIC_BUFFER.Length */
        FAKE_MSQ_REQUIRE(len <= sizeof(flat));       /* single buffer fits flat */
        /* total <= sizeof(flat) - len proves total + len <= sizeof(flat)
         * with no unsigned overflow (len <= sizeof(flat) above) */
        FAKE_MSQ_REQUIRE(total <= (uint64_t)sizeof(flat) - len);
        arr[i].Buffer = (uint8_t *)(uintptr_t)bufs[i];
        arr[i].Length = (uint32_t)len;
        if (len > 0) {
            FAKE_MSQ_REQUIRE(bufs[i] != NULL);
            memcpy(flat + total, bufs[i], len);
        }
        total += len;
    }
    /* invariant established by the loop: total <= sizeof(flat) */

    memset(&ev, 0, sizeof(ev));
    ev.Type = QUIC_STREAM_EVENT_RECEIVE;
    ev.RECEIVE.TotalBufferLength = total;
    ev.RECEIVE.Buffers = count > 0 ? arr : NULL;
    ev.RECEIVE.BufferCount = count;
    ev.RECEIVE.Flags = fin ? QUIC_RECEIVE_FLAG_FIN : 0;
    st->cb((HQUIC)st, st->ctx, &ev);

    uint64_t consumed = ev.RECEIVE.TotalBufferLength;
    FAKE_MSQ_REQUIRE(consumed <= total); /* cannot consume more than indicated */
    if (consumed < total) {
        uint64_t held = total - consumed;

        /* held <= total <= sizeof(flat) == sizeof(st->held): both the read
         * (flat + consumed, held bytes) and the write (st->held) are in
         * range */
        FAKE_MSQ_REQUIRE(held <= sizeof(st->held));
        st->held_len = (uint32_t)held;
        memcpy(st->held, flat + consumed, st->held_len);
        st->held_fin = fin;
        st->recv_disabled = true;
    }
}

void fake_msq_deliver_receive(fake_msq_stream_t *st, const uint8_t *data,
                              size_t len, bool fin)
{
    const uint8_t *bufs[1] = { data };
    size_t lens[1] = { len };

    deliver_core(st, bufs, lens, len > 0 ? 1u : 0u, fin);
}

void fake_msq_deliver_receive_multi(fake_msq_stream_t *st,
                                    const uint8_t *const *bufs,
                                    const size_t *lens, uint32_t count,
                                    bool fin)
{
    deliver_core(st, bufs, lens, count, fin);
}

bool fake_msq_stream_recv_disabled(fake_msq_stream_t *st)
{
    return st->recv_disabled;
}

bool fake_msq_redeliver_held(fake_msq_stream_t *st)
{
    if (st->recv_disabled || st->held_len == 0)
        return false;

    uint8_t tmp[FAKE_MSQ_HELD_MAX];
    uint32_t len = st->held_len;
    bool fin = st->held_fin;

    memcpy(tmp, st->held, len);
    st->held_len = 0;
    st->held_fin = false;

    const uint8_t *bufs[1] = { tmp };
    size_t lens[1] = { len };
    deliver_core(st, bufs, lens, 1, fin);
    return true;
}

int fake_msq_pending_sends(fake_msq_t *f)
{
    int n = 0;

    for (int i = 0; i < f->send_count; i++)
        if (!f->sends[i].completed)
            n++;
    return n;
}

fake_msq_dgram_t *fake_msq_dgram_at(fake_msq_t *f, int index)
{
    if (index < 0 || index >= f->dgram_count)
        return NULL;
    return &f->dgrams[index];
}

void *fake_msq_next_dgram_ctx(fake_msq_t *f)
{
    for (int i = 0; i < f->dgram_count; i++)
        if (!f->dgrams[i].completed) {
            f->dgrams[i].completed = true;
            return f->dgrams[i].client_ctx;
        }
    return NULL;
}

int fake_msq_pending_dgrams(fake_msq_t *f)
{
    int n = 0;

    for (int i = 0; i < f->dgram_count; i++)
        if (!f->dgrams[i].completed)
            n++;
    return n;
}
