/*
 * moq_picoquic.c — Picoquic adapter using the shared transport bridge.
 *
 * Routes picoquic callback events to moq_transport_bridge_t inbound
 * handlers. Outbound actions are dispatched by the bridge through the
 * picoquic endpoint ops (picoquic_endpoint.c).
 *
 * The bridge owns: stream mapping, pending state (outbound + inbound),
 * tombstones, close/drain semantics, and service ordering.
 * This file owns: picoquic callback routing, public API surface.
 */

#include <moq/picoquic.h>
#include <moq/transport_bridge.h>
#include "picoquic_endpoint.h"
#include <stdlib.h>
#include <string.h>

struct moq_pq_conn {
    moq_session_t              *session;
    picoquic_cnx_t             *cnx;
    moq_alloc_t                 alloc;
    void                       *user_ctx;
    void                      (*after_callback)(moq_pq_conn_t *conn,
                                                 void *user_ctx);

    moq_transport_bridge_t     *bridge;
    moq_transport_endpoint_ops_t endpoint_ops;
    pq_endpoint_ctx_t           endpoint_ctx;

    /* Bidi-control profiles (draft-16): the control stream is the FIRST
     * client-initiated bidirectional stream, identified by position/content
     * (MoQT §6.1) rather than a fixed QUIC stream ID. Latched on first sight
     * (UINT64_MAX = not yet seen) so peers that carry control on a non-zero
     * bidi stream interoperate. */
    uint64_t                    control_bidi_id;

    /* Per-stream receive backpressure state (see the block comment below the
     * struct). NULL/0 until the first inbound data stream is seen. */
    struct pq_rx_stream        *rx;
    size_t                      rx_count;
    size_t                      rx_cap;
};

/* -- Per-stream receive backpressure --------------------------------- *
 * Raw picoquic auto-grows each stream's receive window as bytes are consumed
 * (MAX_STREAM_DATA doubles once the app passes half the window), so a stalled
 * application -- the bridge returning MOQ_ERR_WOULD_BLOCK and retaining the
 * input -- does NOT stop the peer: the advertised window keeps advancing and
 * the sender keeps filling the connection. This gives the ADAPTER byte-level
 * receive flow control, so an application stall becomes real QUIC backpressure:
 *
 *   - On first sight of ANY inbound stream the adapter puts it under picoquic
 *     application-controlled flow control (picoquic_set_app_flow_control),
 *     which freezes that stream's window; the adapter then owns every advance.
 *   - The window is kept `budget` bytes ahead of the app's forward progress,
 *     where budget is the local initial_max_stream_data_* this endpoint
 *     advertised for the stream's category and initiator -- so the adapter
 *     never grants MORE than the operator's configured window. A zero budget is
 *     honored as zero (a bare FIN is admitted, payload is not); unreadable
 *     transport parameters fail closed rather than fabricate credit. While a
 *     stream has retained bridge work the window is NOT advanced
 *     (picoquic_open_flow_control is withheld), so the peer stalls once it
 *     exhausts the frozen window: durable backpressure.
 *   - picoquic delivers bytes already inside the advertised window even after
 *     the bridge first blocks -- consumed_offset advances before the data
 *     callback and the callback cannot decline delivery. Those bytes are
 *     retained bounded per stream (the bound is the advertised window; the peer
 *     cannot exceed it without a flow-control violation) and replayed IN ORDER
 *     after service() clears that stream's pending state, re-pausing if the
 *     replay itself blocks. Nothing is dropped, duplicated, or reordered.
 *
 * CONTROL streams get the same per-stream mechanism (independent control: a
 * control stall pauses only the control stream, never a media stream, and vice
 * versa). Routing kind is preserved: a paused control stream replays through
 * on_peer_control_bytes.
 *
 * Credit grants are BATCHED (quantum = budget/2): picoquic_open_flow_control's
 * expected_data_size both re-anchors the stream window (consumed + expected --
 * self-limiting) and blindly increments the connection-wide MAX_DATA by the
 * same amount (frames.c picoquic_format_max_data_frame). Granting on every
 * callback would therefore inflate connection credit unboundedly; granting at
 * most once per budget/2 of application progress bounds the ADAPTER-INDUCED
 * connection MAX_DATA increments to <= 2x the app's forward progress (+ the
 * initial window). That bound covers only this adapter's contribution:
 * picoquic's own automatic MAX_DATA growth (delivery-anchored,
 * 2*offset_received > maxdata_local) remains active and is not bounded by the
 * adapter. Connection-level credit is NOT directly application-controlled:
 * picoquic's only conn-level control (picoquic_set_max_data_control) disables
 * picoquic_open_flow_control outright (mutually exclusive by the
 * max_data_limit guard) -- raw attach callers must NOT enable it (see the
 * public header). TOTAL connection arrest is therefore DERIVATIVE: once every
 * stream's frozen window is exhausted, delivery stops, which stops both the
 * automatic advance (anchored to offset_received) and the adapter's bounded
 * increments.
 *
 * Grants are issued only while the connection is READY:
 * picoquic_open_flow_control silently no-ops in any other state, so a grant
 * attempted earlier is retained as OWED (granted is not advanced) and issued
 * by the post-service sweep once the connection is READY.
 */

/* Bridge routing kind for an inbound stream (preserved across pause/replay). */
typedef enum pq_rx_kind {
    PQ_RX_UNI = 0,     /* peer unidirectional data (d18 also classifies its uni
                          control internally on this path) */
    PQ_RX_BIDI,        /* request bidi */
    PQ_RX_CONTROL      /* d16 bidi control stream */
} pq_rx_kind_t;

typedef struct pq_rx_stream {
    uint64_t stream_id;
    uint8_t *buf;         /* bytes delivered while paused, awaiting in-order replay */
    size_t   buf_len;
    size_t   buf_cap;
    uint64_t delivered;   /* cumulative callback bytes == picoquic consumed_offset */
    uint64_t blocked;     /* bytes of the chunk the session last WOULD_BLOCKed on */
    uint64_t budget;      /* window headroom kept ahead of the app's forward progress */
    uint64_t granted;     /* absolute window last advertised (delivered + expected) */
    uint8_t  kind;        /* pq_rx_kind_t: replay routing */
    bool     app_fc;      /* picoquic_set_app_flow_control(cnx, id, 1) issued */
    bool     paused;      /* has retained/blocked work; MUST NOT feed the bridge */
    bool     buf_fin;     /* a peer FIN follows the retained bytes */
    bool     fin_blocked; /* the FIN rode the chunk that blocked (bridge retains
                             it as fin_retained; the stream is over on drain) */
    bool     active;
} pq_rx_stream_t;

/* -- Helpers -------------------------------------------------------- */

static uint64_t pq_now_us(picoquic_cnx_t *cnx)
{
    return picoquic_get_quic_time(picoquic_get_quic_ctx(cnx));
}

/* Find the tracked receive state for stream_id, or NULL. */
static pq_rx_stream_t *pq_rx_find(moq_pq_conn_t *c, uint64_t stream_id)
{
    for (size_t i = 0; i < c->rx_count; i++)
        if (c->rx[i].active && c->rx[i].stream_id == stream_id)
            return &c->rx[i];
    return NULL;
}

/* The local receive window this endpoint advertised for `stream_id`'s category
 * -- the exact amount the adapter is allowed to keep outstanding. Selected by
 * stream category AND initiator (RFC 9000 §18.2): a peer unidirectional stream
 * uses initial_max_stream_data_uni; a bidirectional stream uses
 * initial_max_stream_data_bidi_local when WE initiated it (the peer sends on
 * our locally-created stream) and _bidi_remote when the PEER initiated it.
 *
 * Zero is a REAL budget (the peer may close the stream but send no payload) --
 * it is never replaced with a fabricated default. Returns false when the
 * transport parameters cannot be read at all: the accounting basis is
 * unavailable and the caller must fail closed. */
static bool pq_rx_budget_for(moq_pq_conn_t *c, uint64_t stream_id,
                             uint64_t *out)
{
    const picoquic_tp_t *tp = picoquic_get_transport_parameters(c->cnx, 1);
    if (tp == NULL) return false;
    if (!PICOQUIC_IS_BIDIR_STREAM_ID(stream_id)) {
        *out = tp->initial_max_stream_data_uni;
    } else {
        bool local_initiated =
            (PICOQUIC_IS_CLIENT_STREAM_ID(stream_id) != 0) ==
            (picoquic_is_client(c->cnx) != 0);
        *out = local_initiated ? tp->initial_max_stream_data_bidi_local
                               : tp->initial_max_stream_data_bidi_remote;
    }
    return true;
}

/* Find-or-create tracked state for an inbound stream. Returns NULL on
 * allocation failure OR when the budget basis is unreadable (the caller treats
 * both as fatal). */
static pq_rx_stream_t *pq_rx_get(moq_pq_conn_t *c, uint64_t stream_id,
                                 pq_rx_kind_t kind)
{
    pq_rx_stream_t *st = pq_rx_find(c, stream_id);
    if (st != NULL) return st;

    /* Resolve the budget BEFORE acquiring a slot, so a fail-closed return
     * never leaves a half-initialized table entry behind. */
    uint64_t budget = 0;
    if (!pq_rx_budget_for(c, stream_id, &budget))
        return NULL;            /* TPs unreadable: fail closed */

    /* Reuse a retired slot before growing. */
    for (size_t i = 0; i < c->rx_count; i++)
        if (!c->rx[i].active) { st = &c->rx[i]; break; }

    if (st == NULL) {
        if (c->rx_count == c->rx_cap) {
            size_t ncap = c->rx_cap ? c->rx_cap * 2 : 8;
            /* Checked growth: reject a table so large the byte math would wrap
             * (unreachable for real stream counts; still never overflow). */
            if (ncap <= c->rx_cap || ncap > SIZE_MAX / sizeof(*c->rx))
                return NULL;
            size_t oldb = c->rx_cap * sizeof(*c->rx);
            pq_rx_stream_t *n = (pq_rx_stream_t *)c->alloc.realloc(
                c->rx, oldb, ncap * sizeof(*c->rx), c->alloc.ctx);
            if (n == NULL) return NULL;
            c->rx = n;
            c->rx_cap = ncap;
        }
        st = &c->rx[c->rx_count++];
    }

    memset(st, 0, sizeof(*st));
    st->stream_id = stream_id;
    st->kind = (uint8_t)kind;
    st->budget = budget;
    st->granted = st->budget;   /* the frozen initial window is `budget` from 0 */
    st->active = true;
    return st;
}

/* Retire a stream's receive state (reset/stop/FIN-complete/close/terminal): free
 * its retained buffer and free the slot for reuse. Exactly-once: idempotent on
 * an unknown/already-retired stream. */
static void pq_rx_drop(moq_pq_conn_t *c, uint64_t stream_id)
{
    pq_rx_stream_t *st = pq_rx_find(c, stream_id);
    if (st == NULL) return;
    if (st->buf != NULL)
        c->alloc.free(st->buf, st->buf_cap, c->alloc.ctx);
    memset(st, 0, sizeof(*st));   /* active = false */
}

static void pq_rx_free_all(moq_pq_conn_t *c)
{
    for (size_t i = 0; i < c->rx_count; i++)
        if (c->rx[i].buf != NULL)
            c->alloc.free(c->rx[i].buf, c->rx[i].buf_cap, c->alloc.ctx);
    if (c->rx != NULL)
        c->alloc.free(c->rx, c->rx_cap * sizeof(*c->rx), c->alloc.ctx);
    c->rx = NULL;
    c->rx_count = 0;
    c->rx_cap = 0;
}

/* Append `len` bytes (and an optional trailing FIN) to a paused stream's
 * retention buffer. Bound: the UNPROCESSED total for the stream -- the chunk
 * blocked in the session plus everything already retained plus this chunk --
 * can never exceed the advertised window (the peer cannot send past it without
 * a flow-control violation, which picoquic rejects), so exceeding it is an
 * impossible transport state -> false (fatal). All arithmetic is
 * overflow-checked. false is also returned on allocation failure. */
static bool pq_rx_retain(moq_pq_conn_t *c, pq_rx_stream_t *st,
                         const uint8_t *bytes, size_t len, bool fin)
{
    if (len > 0) {
        /* blocked + buf_len + len <= budget, phrased without wrap: each partial
         * sum is checked before it is used. */
        uint64_t held = st->blocked + (uint64_t)st->buf_len;
        if (held < st->blocked)                    /* wrapped: impossible state */
            return false;
        if ((uint64_t)len > st->budget || held > st->budget - (uint64_t)len)
            return false;   /* peer overran the advertised window: impossible */
        size_t need = st->buf_len + len;           /* <= budget: no size_t wrap
                                                    * (budget <= varint max and
                                                    * both fit the window) */
        if (need < st->buf_len)
            return false;                          /* paranoia: wrapped */
        if (need > st->buf_cap) {
            size_t ncap = st->buf_cap ? st->buf_cap : 512;
            while (ncap < need) {
                if (ncap > SIZE_MAX / 2) { ncap = need; break; }
                ncap *= 2;
            }
            if ((uint64_t)ncap > st->budget) ncap = need;
            uint8_t *n = (uint8_t *)c->alloc.realloc(
                st->buf, st->buf_cap, ncap, c->alloc.ctx);
            if (n == NULL) return false;
            st->buf = n;
            st->buf_cap = ncap;
        }
        memcpy(st->buf + st->buf_len, bytes, len);
        st->buf_len = need;
    }
    if (fin) st->buf_fin = true;
    return true;
}

/* Feed inbound stream bytes to the bridge, preserving the stream's routing
 * kind (a paused d16 control stream replays through the control path). */
static moq_result_t pq_feed_bridge(moq_pq_conn_t *c, pq_rx_stream_t *st,
                                   const uint8_t *bytes, size_t len, bool fin,
                                   uint64_t now)
{
    switch ((pq_rx_kind_t)st->kind) {
    case PQ_RX_CONTROL:
        return moq_transport_bridge_on_peer_control_bytes(
            c->bridge, st->stream_id, bytes, len, fin, now);
    case PQ_RX_BIDI:
        return moq_transport_bridge_on_peer_bidi_bytes(
            c->bridge, st->stream_id, bytes, len, fin, now);
    case PQ_RX_UNI:
    default:
        return moq_transport_bridge_on_peer_uni_bytes(
            c->bridge, st->stream_id, bytes, len, fin, now);
    }
}

/* Advance the peer's receive window to keep at most `budget` bytes ahead of the
 * app's forward progress. held = bytes the app has NOT yet processed (retained
 * adapter-side + the chunk blocked in the session). While held >= budget no
 * credit is granted -> the window stays frozen and the peer stalls. Anchoring on
 * picoquic's consumed_offset (== st->delivered) means we only ever replenish the
 * credit the application actually released.
 *
 * Grants are BATCHED: at most one grant per budget/2 of window advance. Each
 * picoquic_open_flow_control call blindly increments the connection-wide
 * MAX_DATA by its argument (in addition to re-anchoring the stream window), so
 * per-callback grants would inflate connection credit without bound; batching
 * bounds the connection advance to <= 2x the stream's forward progress.
 *
 * Returns 0, or -1 if picoquic rejected the grant -- the claimed flow-control
 * ownership no longer holds, which the caller must treat as fatal. Bookkeeping
 * (granted) is published only on success. */
static int pq_rx_refresh_credit(moq_pq_conn_t *c, pq_rx_stream_t *st)
{
    if (!st->app_fc) return 0;
    uint64_t held = (uint64_t)st->buf_len + st->blocked;
    if (held < st->blocked || held >= st->budget)
        return 0;                                   /* no headroom: frozen */
    uint64_t headroom = st->budget - held;
    uint64_t target = st->delivered + headroom;     /* processed + budget */
    if (target < st->delivered) return 0;           /* wrap guard */
    uint64_t quantum = st->budget / 2;
    if (quantum == 0) quantum = 1;
    if (st->granted + quantum < st->granted ||      /* wrap guard */
        target < st->granted + quantum)
        return 0;                                   /* batch: not enough owed */
    /* picoquic_open_flow_control SILENTLY no-ops unless the connection is
     * exactly READY (sender.c: cnx_state == ready && max_data_limit == 0), so a
     * pre-ready grant would be recorded but never sent. Leave the grant OWED
     * (granted unchanged -- target keeps exceeding it) and let the post-service
     * sweep issue it once the connection is READY. */
    if (picoquic_get_cnx_state(c->cnx) != picoquic_state_ready)
        return 0;
    if (picoquic_open_flow_control(c->cnx, st->stream_id, headroom) != 0)
        return -1;
    st->granted = target;
    return 0;
}

/* One inbound stream callback (uni data, request bidi, or d16 bidi control --
 * the kind preserves routing). Returns 0, or -1 after signalling a fatal
 * transport error (OOM / impossible transport state / rejected flow-control
 * ownership). */
static int pq_rx_on_data(moq_pq_conn_t *c, uint64_t sid, pq_rx_kind_t kind,
                         const uint8_t *bytes, size_t len, bool fin,
                         uint64_t now)
{
    pq_rx_stream_t *st = pq_rx_get(c, sid, kind);
    if (st == NULL) {                        /* untrackable / TPs unreadable */
        moq_transport_bridge_on_transport_error(c->bridge, 0, now);
        return -1;
    }
    if (len > 0 && st->budget == 0) {
        /* A REAL zero budget admits a bare FIN but no payload: nonzero bytes
         * are past the advertised window -- an impossible transport state. */
        moq_transport_bridge_on_transport_error(c->bridge, 0, now);
        return -1;
    }
    if (!st->app_fc) {                       /* first sight: take the window */
        if (picoquic_set_app_flow_control(c->cnx, sid, 1) != 0) {
            /* The window is NOT ours: the backpressure invariant cannot hold.
             * Fatal, and app_fc stays false (no bookkeeping published). */
            moq_transport_bridge_on_transport_error(c->bridge, 0, now);
            return -1;
        }
        st->app_fc = true;
    }
    st->delivered += len;                    /* == picoquic consumed_offset */

    if (st->paused) {
        /* The bridge retains prior input and MUST NOT be given more until
         * service() clears it. Retain these bytes bounded and replay in order. */
        if (!pq_rx_retain(c, st, bytes, len, fin)) {
            moq_transport_bridge_on_transport_error(c->bridge, 0, now);
            return -1;
        }
        return 0;
    }

    moq_result_t rc = pq_feed_bridge(c, st, bytes, len, fin, now);
    if (rc == MOQ_ERR_WOULD_BLOCK) {
        /* The session buffered this chunk internally and is blocked draining it;
         * a trailing FIN (if any) is retained by the bridge and ends the stream
         * once the drain delivers it. Freeze here. */
        st->paused = true;
        st->blocked = len;
        if (fin) st->fin_blocked = true;
        return 0;
    }
    if (rc < 0)                              /* bridge already went fatal */
        return -1;
    if (fin) {                               /* stream fully consumed: retire it */
        pq_rx_drop(c, sid);
        return 0;
    }
    if (pq_rx_refresh_credit(c, st) != 0) {  /* app made progress: advance window */
        moq_transport_bridge_on_transport_error(c->bridge, 0, now);
        return -1;
    }
    return 0;
}

/* After service() drained retained bridge work: for each paused stream whose
 * pending state cleared, replay any bytes retained while it was paused and, if
 * that drains too, resume the window. Called on the picoquic loop thread. */
static void pq_rx_after_service(moq_pq_conn_t *c, uint64_t now)
{
    for (size_t i = 0; i < c->rx_count; i++) {
        pq_rx_stream_t *st = &c->rx[i];
        if (!st->active) continue;
        if (!st->paused) {
            /* Owed-grant sweep: a grant deferred while the connection was not
             * yet READY (or batched past a quantum) is issued here, on the
             * first service after it becomes eligible -- it must not strand
             * waiting for further progress events on the stream. */
            if (pq_rx_refresh_credit(c, st) != 0) {
                moq_transport_bridge_on_transport_error(c->bridge, 0, now);
                return;
            }
            continue;
        }
        if (moq_transport_bridge_stream_has_pending(c->bridge, st->stream_id))
            continue;                         /* still blocked: stay frozen */

        st->blocked = 0;                      /* the blocked chunk was processed */

        if (st->fin_blocked) {
            /* The FIN rode the chunk that blocked: the bridge retained it and
             * has now delivered it with the drain -- the stream is over. Retire
             * (never re-grant credit on a finished stream). */
            pq_rx_drop(c, st->stream_id);
            continue;
        }

        if (st->buf_len > 0 || st->buf_fin) {
            uint8_t *buf = st->buf; size_t blen = st->buf_len;
            bool bfin = st->buf_fin; size_t bcap = st->buf_cap;
            /* Detach the retained buffer first: the bridge does not hold it past
             * the call, and a re-block must re-track by byte count, not re-own
             * this allocation. */
            st->buf = NULL; st->buf_len = 0; st->buf_cap = 0; st->buf_fin = false;
            moq_result_t rc = pq_feed_bridge(c, st, buf, blen, bfin, now);
            if (buf != NULL) c->alloc.free(buf, bcap, c->alloc.ctx);
            if (rc == MOQ_ERR_WOULD_BLOCK) {
                st->blocked = blen;           /* session took it, still draining */
                if (bfin) st->fin_blocked = true;  /* fin retained by the bridge */
                continue;                     /* remain paused */
            }
            if (rc < 0) {                     /* bridge went fatal */
                moq_transport_bridge_on_transport_error(c->bridge, 0, now);
                return;
            }
            if (bfin) { pq_rx_drop(c, st->stream_id); continue; }  /* stream done */
        }

        st->paused = false;
        if (pq_rx_refresh_credit(c, st) != 0) {   /* released credit: advance */
            moq_transport_bridge_on_transport_error(c->bridge, 0, now);
            return;
        }
    }
}

/* -- Public API ----------------------------------------------------- */

/* Frozen v0 layout size: the prefix present in every released version of this
 * struct, ending just before the first appended field (user_ctx). The pointer-
 * only initializer cannot know the caller's storage size, so it touches only
 * this prefix -- safe for an old binary whose moq_pq_conn_cfg_t was exactly
 * this size. */
#define MOQ_PQ_CONN_CFG_V0_SIZE (offsetof(moq_pq_conn_cfg_t, user_ctx))

void moq_pq_conn_cfg_init(moq_pq_conn_cfg_t *cfg)
{
    if (!cfg) return;
    /* Clear and stamp ONLY the v0 prefix: writing sizeof(*cfg) here would
     * overflow an old caller that allocated the smaller v0 struct. Appended
     * fields stay disabled (struct_size == v0 size); callers that want them
     * use moq_pq_conn_cfg_init_sized(). */
    memset(cfg, 0, MOQ_PQ_CONN_CFG_V0_SIZE);
    cfg->struct_size = (uint32_t)MOQ_PQ_CONN_CFG_V0_SIZE;
}

void moq_pq_conn_cfg_init_sized(moq_pq_conn_cfg_t *cfg, size_t cfg_size)
{
    if (!cfg) return;
    /* Clear exactly what the caller allocated, never more than this library's
     * struct knows about. A caller older than the library passes a smaller
     * cfg_size (we clear/stamp that prefix); a caller newer than the library
     * passes a larger one (we clamp to our sizeof and leave its extra fields
     * to its own initializer). */
    size_t n = cfg_size < sizeof(*cfg) ? cfg_size : sizeof(*cfg);
    if (n < sizeof(cfg->struct_size)) return;  /* too small to even stamp */
    memset(cfg, 0, n);
    cfg->struct_size = (uint32_t)n;
}

int moq_pq_conn_create(const moq_pq_conn_cfg_t *cfg,
                         moq_pq_conn_t **out)
{
    if (!cfg || !out) return -1;
    *out = NULL;

    size_t min_size = offsetof(moq_pq_conn_cfg_t, alloc)
                    + sizeof(cfg->alloc);
    if (cfg->struct_size < min_size) return -1;
    if (!cfg->session || !cfg->cnx || !cfg->alloc) return -1;
    if (!cfg->alloc->alloc || !cfg->alloc->free || !cfg->alloc->realloc)
        return -1;

    moq_pq_conn_t *c = (moq_pq_conn_t *)cfg->alloc->alloc(
        sizeof(moq_pq_conn_t), cfg->alloc->ctx);
    if (!c) return -1;
    memset(c, 0, sizeof(*c));

    c->session = cfg->session;
    c->cnx = cfg->cnx;
    c->alloc = *cfg->alloc;
    c->control_bidi_id = UINT64_MAX;   /* no control stream latched yet */
    if (cfg->struct_size >= offsetof(moq_pq_conn_cfg_t, user_ctx) +
        sizeof(cfg->user_ctx))
        c->user_ctx = cfg->user_ctx;
    if (cfg->struct_size >= offsetof(moq_pq_conn_cfg_t, after_callback) +
        sizeof(cfg->after_callback))
        c->after_callback = cfg->after_callback;

    /* Initialize endpoint ops and bridge. */
    if (pq_endpoint_init(&c->endpoint_ops, &c->endpoint_ctx, cfg->cnx,
                         &c->alloc) != 0) {
        c->alloc.free(c, sizeof(*c), c->alloc.ctx);
        return -1;
    }

    moq_transport_bridge_cfg_t bcfg;
    moq_transport_bridge_cfg_init(&bcfg, cfg->alloc);
    moq_result_t brc = moq_transport_bridge_create(
        &bcfg, cfg->session, &c->endpoint_ops, &c->endpoint_ctx,
        &c->bridge);
    if (brc < 0) {
        pq_endpoint_cleanup(&c->endpoint_ctx);
        c->alloc.free(c, sizeof(*c), c->alloc.ctx);
        return -1;
    }

    picoquic_set_callback(cfg->cnx, moq_pq_callback, c);

    *out = c;
    return 0;
}

void moq_pq_conn_destroy(moq_pq_conn_t *conn)
{
    if (!conn) return;
    picoquic_set_callback(conn->cnx, NULL, NULL);
    pq_rx_free_all(conn);
    moq_transport_bridge_destroy(conn->bridge);
    pq_endpoint_cleanup(&conn->endpoint_ctx);
    moq_alloc_t alloc = conn->alloc;
    alloc.free(conn, sizeof(moq_pq_conn_t), alloc.ctx);
}

moq_session_t *moq_pq_conn_session(moq_pq_conn_t *conn)
{
    return conn ? conn->session : NULL;
}

void *moq_pq_conn_user_ctx(const moq_pq_conn_t *conn)
{
    return conn ? conn->user_ctx : NULL;
}

void moq_pq_conn_set_user_ctx(moq_pq_conn_t *conn, void *user_ctx)
{
    if (conn) conn->user_ctx = user_ctx;
}

bool moq_pq_conn_is_fatal(const moq_pq_conn_t *conn)
{
    return conn ? moq_transport_bridge_is_fatal(conn->bridge) : false;
}

uint64_t moq_pq_conn_fatal_code(const moq_pq_conn_t *conn)
{
    return conn ? moq_transport_bridge_fatal_code(conn->bridge) : 0;
}

bool moq_pq_conn_is_closed(const moq_pq_conn_t *conn)
{
    return conn ? moq_transport_bridge_is_closed(conn->bridge) : false;
}

uint64_t moq_pq_conn_close_code(const moq_pq_conn_t *conn)
{
    return conn ? moq_transport_bridge_close_code(conn->bridge) : 0;
}

/* -- Test accessors ------------------------------------------------- */

size_t moq_pq_conn_active_stream_count(const moq_pq_conn_t *c)
{
    return c ? moq_transport_bridge_stream_count(c->bridge) : 0;
}

size_t moq_pq_conn_tombstone_count(const moq_pq_conn_t *c)
{
    return c ? moq_transport_bridge_tombstone_count(c->bridge) : 0;
}

void moq_pq_conn_get_send_stats(const moq_pq_conn_t *c,
                                moq_pq_send_stats_t *out)
{
    if (!out) return;
    if (!c) { memset(out, 0, sizeof(*out)); return; }
    pq_endpoint_get_stats(&c->endpoint_ctx, out);
}

/* -- Inbound: picoquic callback → bridge ---------------------------- */

int moq_pq_callback(picoquic_cnx_t *cnx,
                      uint64_t stream_id,
                      uint8_t *bytes, size_t length,
                      picoquic_call_back_event_t event,
                      void *callback_ctx,
                      void *stream_ctx)
{
    moq_pq_conn_t *c = (moq_pq_conn_t *)callback_ctx;
    if (!c) return 0;

    /* Pull send: picoquic is ready to put bytes on the wire for `stream_id`.
     * `bytes` is the provide context, `length` the largest buffer available.
     * Serviced even while terminal so picoquic always gets a buffer response
     * (the queue is drained/empty by then, so it reneges). */
    if (event == picoquic_callback_prepare_to_send) {
        pq_endpoint_on_prepare_to_send(&c->endpoint_ctx, stream_id,
                                       bytes, length);
        if (c->after_callback)
            c->after_callback(c, c->user_ctx);
        return 0;
    }

    if (moq_transport_bridge_is_terminal(c->bridge)) {
        /* No processing on a terminal adapter, but the hook contract is
         * state-independent: it fires on ALL callback exits. */
        if (c->after_callback)
            c->after_callback(c, c->user_ctx);
        return 0;
    }
    uint64_t now = pq_now_us(cnx);
    (void)stream_ctx;

    switch (event) {
    case picoquic_callback_stream_data:
    case picoquic_callback_stream_fin: {
        bool fin = (event == picoquic_callback_stream_fin);
        bool is_bidi = PICOQUIC_IS_BIDIR_STREAM_ID(stream_id);

        if (is_bidi) {
            /* Control routing is profile-dependent. Bidi-control profiles
             * (draft-16) carry control on the FIRST client-initiated
             * bidirectional stream (MoQT §6.1: "The first stream opened is a
             * client-initiated bidirectional control stream ... which begins
             * with CLIENT_SETUP" -- identified by position, not a fixed QUIC
             * stream ID). We latch that stream on first sight and route it to
             * on_peer_control_bytes in both directions; every other bidi
             * stream is a request stream. This matches stream 0 for a peer
             * that opens control first (the common case) while also accepting
             * peers that reserve stream 0 and carry control on a higher bidi
             * stream (e.g. Apple's Network.framework). Uni-control-pair
             * profiles (draft-18) classify control among unidirectional
             * streams, so every bidi stream is a request stream. */
            bool uni_control = moq_transport_bridge_uses_uni_control(c->bridge);
            if (!uni_control && c->control_bidi_id == UINT64_MAX &&
                PICOQUIC_IS_CLIENT_STREAM_ID(stream_id)) {
                c->control_bidi_id = stream_id;
            }
            bool is_control = !uni_control &&
                stream_id == c->control_bidi_id;

            if (is_control) {
                /* The d16 bidi control stream gets the SAME per-stream receive
                 * flow control as every other stream (independent control: a
                 * blocked media stream never pauses control, and a blocked
                 * control stream pauses ONLY itself). Its routing kind is
                 * preserved through pause/replay. */
                if (pq_rx_on_data(c, stream_id, PQ_RX_CONTROL,
                                  bytes, length, fin, now) != 0)
                    break;      /* fatal already signalled; fall to the epilogue */
                break;
            }
        }
        /* Data-carrying stream (uni data, or a peer request bidi). Byte-level
         * receive backpressure lives here: a uni-control-pair control stream
         * (draft-18) rides the uni path -- the bridge classifies it internally,
         * and its per-stream window keeps it isolated from any blocked media
         * stream. */
        if (pq_rx_on_data(c, stream_id, is_bidi ? PQ_RX_BIDI : PQ_RX_UNI,
                          bytes, length, fin, now) != 0)
            break;      /* fatal already signalled; fall to the epilogue */
        break;
    }

    case picoquic_callback_stream_reset: {
        uint64_t error_code = picoquic_get_remote_stream_error(
            cnx, stream_id);
        /* Abandon any retained/paused receive state: the stream is gone. */
        pq_rx_drop(c, stream_id);
        moq_transport_bridge_on_peer_stream_reset(
            c->bridge, stream_id, error_code, now);
        break;
    }

    case picoquic_callback_stop_sending: {
        uint64_t error_code = picoquic_get_remote_stream_error(
            cnx, stream_id);
        moq_transport_bridge_on_peer_stop_sending(
            c->bridge, stream_id, error_code, now);
        break;
    }

    case picoquic_callback_datagram:
        moq_transport_bridge_on_peer_datagram(
            c->bridge, bytes, length, now);
        break;

    case picoquic_callback_close:
        moq_transport_bridge_on_transport_close(c->bridge, 0, now);
        break;

    case picoquic_callback_application_close: {
        uint64_t app_err = picoquic_get_application_error(cnx);
        moq_transport_bridge_on_transport_close(c->bridge, app_err, now);
        break;
    }

    default:
        break;
    }

    if (c->after_callback)
        c->after_callback(c, c->user_ctx);
    return 0;
}

/* -- Service -------------------------------------------------------- */

int moq_pq_service(moq_pq_conn_t *conn, uint64_t now_us)
{
    if (!conn) return -1;   /* no connection: no hook to fire */

    moq_result_t rc;
    if (moq_transport_bridge_is_fatal(conn->bridge)) {
        /* Already-terminal exits still run the common epilogue: the hook
         * contract is state-independent ("fires on all paths including
         * fatal"), for service exactly as for the picoquic callback. */
        rc = MOQ_ERR_INTERNAL;
    } else if (moq_transport_bridge_is_closed(conn->bridge)) {
        rc = MOQ_OK;
    } else {
        rc = moq_transport_bridge_service(conn->bridge, now_us);

        /* Retained bridge work may have just cleared: replay any bytes buffered
         * while a stream was paused and advance the receive window for streams
         * whose pending state is gone. Skipped once terminal (no window to
         * manage). */
        if (rc >= 0 && !moq_transport_bridge_is_terminal(conn->bridge)) {
            pq_rx_after_service(conn, now_us);
            /* The replay/credit step can itself go fatal (rejected grant,
             * failed replay); success-after-fatal must never be reported. */
            if (moq_transport_bridge_is_fatal(conn->bridge))
                rc = MOQ_ERR_INTERNAL;
        }
    }

    if (conn->after_callback)
        conn->after_callback(conn, conn->user_ctx);

    return rc < 0 ? -1 : 0;
}
