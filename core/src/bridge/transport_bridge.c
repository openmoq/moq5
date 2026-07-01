/*
 * transport_bridge.c — Shared transport bridge implementation.
 *
 * Used by the picoquic, mvfst, and proxygen WebTransport adapters.
 * Owns action dispatch, stream mapping, outbound/inbound retry
 * queues, tombstones, FIN/reset/stop lifecycle, close/fatal
 * semantics, and service ordering.
 */

#include "transport_bridge_internal.h"
#include <moq/session.h>
#include <moq/rcbuf.h>
#include <string.h>

static void bridge_discard_if_dropped(moq_transport_bridge_t *b,
                                      bridge_stream_entry_t *e);

/* -- Config --------------------------------------------------------- */

void moq_transport_bridge_cfg_init(moq_transport_bridge_cfg_t *cfg,
                                    const moq_alloc_t *alloc)
{
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = sizeof(moq_transport_bridge_cfg_t);
    cfg->alloc = alloc;
}

/* -- Helpers: alloc wrappers ---------------------------------------- */

static void *bridge_alloc(const moq_alloc_t *a, size_t size)
{
    return a->alloc(size, a->ctx);
}

static void bridge_free(const moq_alloc_t *a, void *ptr, size_t size)
{
    a->free(ptr, size, a->ctx);
}

/*
 * Validate endpoint result for non-datagram ops. Only OK, WOULD_BLOCK,
 * and ERROR are valid. DROPPED, TOO_LARGE, and unknown values are
 * treated as fatal (a misbehaving endpoint).
 */
static moq_transport_result_t sanitize_stream_result(
    moq_transport_result_t r)
{
    switch (r) {
    case MOQ_TRANSPORT_OK:
    case MOQ_TRANSPORT_WOULD_BLOCK:
    case MOQ_TRANSPORT_ERROR:
        return r;
    default:
        return MOQ_TRANSPORT_ERROR;
    }
}

/* -- Create / Destroy ----------------------------------------------- */

#define HAS_FIELD(ops, field) \
    ((ops)->struct_size >= offsetof(moq_transport_endpoint_ops_t, field) + \
                           sizeof((ops)->field))

static bool ops_valid(const moq_transport_endpoint_ops_t *ops)
{
    if (!ops) return false;
    /* All required ops must be within the declared struct_size and
     * non-NULL. Never read a field outside struct_size. */
    if (!HAS_FIELD(ops, close_transport)) return false;
    if (!ops->open_uni)        return false;
    if (!ops->open_bidi)       return false;
    if (!ops->write)           return false;
    if (!ops->reset_stream)    return false;
    if (!ops->stop_sending)    return false;
    if (!ops->close_transport) return false;
    return true;
}

moq_result_t moq_transport_bridge_create(
    const moq_transport_bridge_cfg_t   *cfg,
    moq_session_t                      *session,
    const moq_transport_endpoint_ops_t *ops,
    void                               *endpoint_ctx,
    moq_transport_bridge_t            **out)
{
    if (!out) return MOQ_ERR_INVAL;
    *out = NULL;
    if (!cfg || !session || !ops) return MOQ_ERR_INVAL;
    if (cfg->struct_size < sizeof(moq_transport_bridge_cfg_t))
        return MOQ_ERR_INVAL;
    if (!cfg->alloc || !cfg->alloc->alloc || !cfg->alloc->free ||
        !cfg->alloc->realloc)
        return MOQ_ERR_INVAL;
    if (!ops_valid(ops)) return MOQ_ERR_INVAL;

    const moq_alloc_t *a = cfg->alloc;
    uint32_t max_s = cfg->max_streams    ? cfg->max_streams    : BRIDGE_DEFAULT_MAX_STREAMS;
    uint32_t max_p = cfg->max_pending    ? cfg->max_pending    : BRIDGE_DEFAULT_MAX_PENDING;
    uint32_t max_t = cfg->max_tombstones ? cfg->max_tombstones : BRIDGE_DEFAULT_MAX_TOMBSTONES;

    moq_transport_bridge_t *b = bridge_alloc(a, sizeof(*b));
    if (!b) return MOQ_ERR_NOMEM;
    memset(b, 0, sizeof(*b));

    b->alloc        = *a;
    b->session      = session;
    b->ops          = ops;
    b->endpoint_ctx = endpoint_ctx;
    b->is_client    = (moq_session_perspective(session) == MOQ_PERSPECTIVE_CLIENT);
    b->max_streams    = max_s;
    b->max_pending    = max_p;
    b->max_tombstones = max_t;
    b->next_inbound_ref = BRIDGE_INBOUND_REF_BASE;
    b->control_stream_id      = UINT64_MAX;
    b->peer_control_stream_id = UINT64_MAX;

    b->control_mode = moq_session_uses_uni_control(session)
        ? BRIDGE_CONTROL_UNI_PAIR : BRIDGE_CONTROL_BIDI;
    b->local_ctrl_uni_stream_id = UINT64_MAX;
    b->peer_ctrl_uni_stream_id  = UINT64_MAX;

    b->streams = bridge_alloc(a, max_s * sizeof(bridge_stream_entry_t));
    if (!b->streams) goto fail;
    memset(b->streams, 0, max_s * sizeof(bridge_stream_entry_t));

    b->pending = bridge_alloc(a, max_p * sizeof(bridge_pending_item_t));
    if (!b->pending) goto fail;
    memset(b->pending, 0, max_p * sizeof(bridge_pending_item_t));

    b->tombstones = bridge_alloc(a, max_t * sizeof(uint64_t));
    if (!b->tombstones) goto fail;
    memset(b->tombstones, 0, max_t * sizeof(uint64_t));

    *out = b;
    return MOQ_OK;

fail:
    if (b->tombstones) bridge_free(a, b->tombstones, max_t * sizeof(uint64_t));
    if (b->pending)    bridge_free(a, b->pending, max_p * sizeof(bridge_pending_item_t));
    if (b->streams)    bridge_free(a, b->streams, max_s * sizeof(bridge_stream_entry_t));
    bridge_free(a, b, sizeof(*b));
    return MOQ_ERR_NOMEM;
}

void moq_transport_bridge_destroy(moq_transport_bridge_t *b)
{
    if (!b) return;

    bridge_cleanup_all_pending(b);

    const moq_alloc_t *a = &b->alloc;
    bridge_free(a, b->tombstones, b->max_tombstones * sizeof(uint64_t));
    bridge_free(a, b->pending, b->max_pending * sizeof(bridge_pending_item_t));
    bridge_free(a, b->streams, b->max_streams * sizeof(bridge_stream_entry_t));
    bridge_free(a, b, sizeof(*b));
}

/* -- Stream map ----------------------------------------------------- */

bridge_stream_entry_t *bridge_alloc_stream(moq_transport_bridge_t *b)
{
    for (uint32_t i = 0; i < b->max_streams; i++) {
        if (!b->streams[i].active) {
            memset(&b->streams[i], 0, sizeof(bridge_stream_entry_t));
            b->streams[i].active = true;
            return &b->streams[i];
        }
    }
    return NULL;
}

bridge_stream_entry_t *bridge_find_by_ref(
    moq_transport_bridge_t *b, moq_stream_ref_t ref)
{
    for (uint32_t i = 0; i < b->max_streams; i++) {
        if (b->streams[i].active && b->streams[i].ref._v == ref._v)
            return &b->streams[i];
    }
    return NULL;
}

bridge_stream_entry_t *bridge_find_by_id(
    moq_transport_bridge_t *b, uint64_t transport_id)
{
    for (uint32_t i = 0; i < b->max_streams; i++) {
        if (b->streams[i].active && b->streams[i].transport_id == transport_id)
            return &b->streams[i];
    }
    return NULL;
}

void bridge_deactivate_stream(bridge_stream_entry_t *e)
{
    memset(e, 0, sizeof(*e));
}

moq_stream_ref_t bridge_assign_inbound_ref(
    moq_transport_bridge_t *b, uint64_t transport_id,
    bridge_stream_kind_t kind)
{
    bridge_stream_entry_t *existing = bridge_find_by_id(b, transport_id);
    if (existing) return existing->ref;

    bridge_stream_entry_t *e = bridge_alloc_stream(b);
    if (!e) {
        bridge_set_fatal(b, 0x1);
        return moq_stream_ref_from_u64(0);
    }

    uint64_t rv = b->next_inbound_ref++;
    e->ref = moq_stream_ref_from_u64(rv);
    e->transport_id = transport_id;
    e->kind = kind;
    e->origin = BRIDGE_ORIGIN_PEER;
    return e->ref;
}

/* -- Stream close lifecycle ----------------------------------------- */

void bridge_mark_local_close(moq_transport_bridge_t *b, moq_stream_ref_t ref)
{
    bridge_stream_entry_t *e = bridge_find_by_ref(b, ref);
    if (e) e->local_send_closed = true;
}

void bridge_mark_peer_close(moq_transport_bridge_t *b, moq_stream_ref_t ref)
{
    bridge_stream_entry_t *e = bridge_find_by_ref(b, ref);
    if (e) e->peer_send_closed = true;
}

void bridge_retire_or_tombstone(moq_transport_bridge_t *b, moq_stream_ref_t ref)
{
    bridge_stream_entry_t *e = bridge_find_by_ref(b, ref);
    if (!e) return;

    if (e->kind == BRIDGE_STREAM_UNI) {
        bridge_deactivate_stream(e);
        return;
    }

    if (e->peer_send_closed && e->local_send_closed) {
        bridge_deactivate_stream(e);
        return;
    }

    if (e->local_send_closed && !e->peer_send_closed) {
        if (moq_session_has_transport_stream(b->session, e->ref)) {
            return;
        }
        uint64_t tid = e->transport_id;
        bridge_deactivate_stream(e);

        if (b->tombstone_count >= b->max_tombstones) {
            for (uint32_t i = 1; i < b->max_tombstones; i++)
                b->tombstones[i - 1] = b->tombstones[i];
            b->tombstone_count--;
        }
        b->tombstones[b->tombstone_count++] = tid;
    }
}

bool bridge_is_tombstoned(const moq_transport_bridge_t *b, uint64_t transport_id)
{
    for (uint32_t i = 0; i < b->tombstone_count; i++) {
        if (b->tombstones[i] == transport_id)
            return true;
    }
    return false;
}

void bridge_remove_tombstone(moq_transport_bridge_t *b, uint64_t transport_id)
{
    for (uint32_t i = 0; i < b->tombstone_count; i++) {
        if (b->tombstones[i] == transport_id) {
            for (uint32_t j = i + 1; j < b->tombstone_count; j++)
                b->tombstones[j - 1] = b->tombstones[j];
            b->tombstone_count--;
            return;
        }
    }
}

/* -- Terminal cleanup ------------------------------------------------ */

static void bridge_clear_all_state(moq_transport_bridge_t *b)
{
    bridge_cleanup_all_pending(b);
    b->pending_control = false;
    b->pending_control_fin = false;
    for (uint32_t i = 0; i < b->max_streams; i++) {
        if (b->streams[i].active)
            bridge_deactivate_stream(&b->streams[i]);
    }
    b->tombstone_count = 0;
}

/* -- Fatal ---------------------------------------------------------- */

void bridge_set_fatal(moq_transport_bridge_t *b, uint64_t code)
{
    if (b->fatal || b->closed) return;
    b->fatal = true;
    b->fatal_code = code;
    bridge_clear_all_state(b);
}

/* Clear local uni control-channel state when that channel's FIN completes.
 * Keyed by transport stream id so both the immediate send path and the
 * pending-write retry path converge here. */
static void bridge_on_local_ctrl_uni_fin(moq_transport_bridge_t *b,
                                          uint64_t stream_id)
{
    if (b->local_ctrl_uni_open &&
        stream_id == b->local_ctrl_uni_stream_id)
        b->local_ctrl_uni_open = false;
}

/* -- Inbound peer unidirectional stream routing (decision only) ----- */

bridge_uni_route_t bridge_route_peer_uni(
    moq_transport_bridge_t *b, moq_uni_class_t cls, uint64_t stream_id)
{
    switch (cls) {
    case MOQ_UNI_CLASS_DATA:
        return BRIDGE_UNI_ROUTE_DATA;
    case MOQ_UNI_CLASS_PADDING:
        return BRIDGE_UNI_ROUTE_HANDLED;   /* receiver discards padding */
    case MOQ_UNI_CLASS_NEED_MORE:
        return BRIDGE_UNI_ROUTE_NEED_MORE;
    case MOQ_UNI_CLASS_CONTROL:
        if (!b->peer_ctrl_uni_open) {
            b->peer_ctrl_uni_open = true;
            b->peer_ctrl_uni_stream_id = stream_id;
            return BRIDGE_UNI_ROUTE_HANDLED;
        }
        if (b->peer_ctrl_uni_stream_id == stream_id)
            return BRIDGE_UNI_ROUTE_HANDLED;   /* same channel, more bytes */
        return BRIDGE_UNI_ROUTE_FATAL;         /* second, distinct channel */
    case MOQ_UNI_CLASS_UNKNOWN:
    default:
        return BRIDGE_UNI_ROUTE_FATAL;
    }
}

/* -- Pending item lifecycle ----------------------------------------- */

void bridge_pending_take_action(bridge_pending_item_t *item, moq_action_t *act)
{
    if (item->owns_action)
        moq_action_cleanup(&item->act);
    item->act = *act;
    item->owns_action = true;
    memset(act, 0, sizeof(*act));
}

void bridge_cleanup_pending_item(const moq_alloc_t *alloc,
                                  bridge_pending_item_t *item)
{
    if (item->owns_action) {
        moq_action_cleanup(&item->act);
        item->owns_action = false;
    }
    if (item->data) {
        alloc->free(item->data, item->data_len, alloc->ctx);
        item->data = NULL;
        item->data_len = 0;
    }
}

void bridge_cleanup_all_pending(moq_transport_bridge_t *b)
{
    for (uint32_t i = 0; i < b->pending_count; i++)
        bridge_cleanup_pending_item(&b->alloc, &b->pending[i]);
    b->pending_count = 0;
}

bool bridge_stream_has_inbound_pending(const bridge_stream_entry_t *e)
{
    return e->pending_retry || e->pending_fin ||
           e->pending_reset || e->pending_stop;
}

/* -- State queries -------------------------------------------------- */

bool moq_transport_bridge_is_fatal(const moq_transport_bridge_t *b)
{
    return b ? b->fatal : false;
}

bool moq_transport_bridge_uses_uni_control(const moq_transport_bridge_t *b)
{
    return b ? (b->control_mode == BRIDGE_CONTROL_UNI_PAIR) : false;
}

uint64_t moq_transport_bridge_fatal_code(const moq_transport_bridge_t *b)
{
    return b ? b->fatal_code : 0;
}

bool moq_transport_bridge_is_closed(const moq_transport_bridge_t *b)
{
    return b ? b->closed : false;
}

uint64_t moq_transport_bridge_close_code(const moq_transport_bridge_t *b)
{
    return b ? b->close_code : 0;
}

bool moq_transport_bridge_is_terminal(const moq_transport_bridge_t *b)
{
    return b ? (b->fatal || b->closed) : false;
}

bool moq_transport_bridge_has_pending(const moq_transport_bridge_t *b)
{
    if (!b) return false;
    if (b->pending_count > 0) return true;
    if (b->pending_control) return true;
    if (b->needs_close) return true;
    for (uint32_t i = 0; i < b->max_streams; i++) {
        if (b->streams[i].active && bridge_stream_has_inbound_pending(&b->streams[i]))
            return true;
    }
    return false;
}

bool moq_transport_bridge_stream_has_pending(
    const moq_transport_bridge_t *b, uint64_t stream_id)
{
    if (!b) return false;
    if (b->pending_control && b->pending_control_stream_id == stream_id)
        return true;
    for (uint32_t i = 0; i < b->max_streams; i++) {
        if (b->streams[i].active && b->streams[i].transport_id == stream_id)
            return bridge_stream_has_inbound_pending(&b->streams[i]);
    }
    return false;
}

size_t moq_transport_bridge_stream_count(const moq_transport_bridge_t *b)
{
    if (!b) return 0;
    size_t n = 0;
    for (uint32_t i = 0; i < b->max_streams; i++)
        if (b->streams[i].active) n++;
    return n;
}

size_t moq_transport_bridge_tombstone_count(const moq_transport_bridge_t *b)
{
    return b ? b->tombstone_count : 0;
}

moq_stream_ref_t moq_transport_bridge_find_ref(
    const moq_transport_bridge_t *b, uint64_t stream_id)
{
    if (!b) return moq_stream_ref_from_u64(0);
    for (uint32_t i = 0; i < b->max_streams; i++) {
        if (b->streams[i].active && b->streams[i].transport_id == stream_id)
            return b->streams[i].ref;
    }
    return moq_stream_ref_from_u64(0);
}

/* -- Outbound: action dispatch -------------------------------------- */

static bool bridge_enqueue_pending(moq_transport_bridge_t *b,
                                    const bridge_pending_item_t *item)
{
    if (b->pending_count >= b->max_pending) return false;
    b->pending[b->pending_count++] = *item;
    return true;
}

static moq_result_t dispatch_send_control(moq_transport_bridge_t *b,
                                           moq_action_t *act)
{
    const moq_send_control_action_t *sc = &act->u.send_control;

    if (!b->control_open) {
        if (b->is_client) {
            uint64_t bidi_id = 0;
            moq_transport_result_t r = sanitize_stream_result(
                b->ops->open_bidi(b->endpoint_ctx, &bidi_id));
            if (r == MOQ_TRANSPORT_WOULD_BLOCK) {
                bridge_pending_item_t p;
                memset(&p, 0, sizeof(p));
                p.kind = PENDING_OPEN_CONTROL;
                p.data = bridge_alloc(&b->alloc, sc->len);
                if (!p.data) { bridge_set_fatal(b, 0x1); return MOQ_ERR_NOMEM; }
                memcpy(p.data, sc->data, sc->len);
                p.data_len = sc->len;
                moq_action_cleanup(act);
                if (!bridge_enqueue_pending(b, &p)) {
                    b->alloc.free(p.data, p.data_len, b->alloc.ctx);
                    bridge_set_fatal(b, 0x1);
                    return MOQ_ERR_INTERNAL;
                }
                return MOQ_OK;
            }
            if (r == MOQ_TRANSPORT_ERROR) {
                moq_action_cleanup(act);
                bridge_set_fatal(b, 0x1);
                return MOQ_ERR_INTERNAL;
            }
            b->control_stream_id = bidi_id;
        } else {
            if (b->peer_control_stream_id == UINT64_MAX) {
                bridge_pending_item_t p;
                memset(&p, 0, sizeof(p));
                p.kind = PENDING_OPEN_CONTROL;
                p.data = bridge_alloc(&b->alloc, sc->len);
                if (!p.data) { bridge_set_fatal(b, 0x1); return MOQ_ERR_NOMEM; }
                memcpy(p.data, sc->data, sc->len);
                p.data_len = sc->len;
                moq_action_cleanup(act);
                if (!bridge_enqueue_pending(b, &p)) {
                    b->alloc.free(p.data, p.data_len, b->alloc.ctx);
                    bridge_set_fatal(b, 0x1);
                    return MOQ_ERR_INTERNAL;
                }
                return MOQ_OK;
            }
            b->control_stream_id = b->peer_control_stream_id;
        }
        b->control_open = true;
    }

    moq_transport_result_t wr = sanitize_stream_result(
        b->ops->write(b->endpoint_ctx, b->control_stream_id,
                       sc->data, sc->len, false));

    if (wr == MOQ_TRANSPORT_WOULD_BLOCK) {
        bridge_pending_item_t p;
        memset(&p, 0, sizeof(p));
        p.kind = PENDING_COPIED_WRITE;
        p.stream_id = b->control_stream_id;
        p.data = bridge_alloc(&b->alloc, sc->len);
        if (!p.data) { bridge_set_fatal(b, 0x1); return MOQ_ERR_NOMEM; }
        memcpy(p.data, sc->data, sc->len);
        p.data_len = sc->len;
        moq_action_cleanup(act);
        if (!bridge_enqueue_pending(b, &p)) {
            b->alloc.free(p.data, p.data_len, b->alloc.ctx);
            bridge_set_fatal(b, 0x1);
            return MOQ_ERR_INTERNAL;
        }
        return MOQ_OK;
    }

    moq_action_cleanup(act);
    if (wr == MOQ_TRANSPORT_ERROR) {
        bridge_set_fatal(b, 0x1);
        return MOQ_ERR_INTERNAL;
    }
    return MOQ_OK;
}

static moq_result_t dispatch_close_session(moq_transport_bridge_t *b,
                                            moq_action_t *act)
{
    const moq_close_session_action_t *cs = &act->u.close_session;
    uint64_t code = cs->code;
    const uint8_t *reason = cs->reason.data;
    size_t reason_len = cs->reason.len;

    moq_transport_result_t r = sanitize_stream_result(
        b->ops->close_transport(b->endpoint_ctx, code, reason, reason_len));

    if (r == MOQ_TRANSPORT_WOULD_BLOCK) {
        bridge_pending_item_t p;
        memset(&p, 0, sizeof(p));
        p.kind = PENDING_CLOSE_TRANSPORT;
        p.error_code = code;
        if (reason && reason_len > 0) {
            p.data = bridge_alloc(&b->alloc, reason_len);
            if (!p.data) {
                moq_action_cleanup(act);
                bridge_set_fatal(b, 0x1);
                return MOQ_ERR_NOMEM;
            }
            memcpy(p.data, reason, reason_len);
            p.data_len = reason_len;
        }
        moq_action_cleanup(act);
        if (!bridge_enqueue_pending(b, &p)) {
            if (p.data) b->alloc.free(p.data, p.data_len, b->alloc.ctx);
            bridge_set_fatal(b, 0x1);
            return MOQ_ERR_INTERNAL;
        }
        return MOQ_OK;
    }

    moq_action_cleanup(act);
    if (r == MOQ_TRANSPORT_ERROR) {
        bridge_set_fatal(b, code);
        return MOQ_ERR_INTERNAL;
    }
    b->closed = true;
    b->close_code = code;
    bridge_clear_all_state(b);
    return MOQ_OK;
}

/* -- SEND_DATA: two-phase header+payload ---------------------------- */

/*
 * Try to send a SEND_DATA action's header and payload.
 *
 * The pending item tracks progress:
 *   PENDING_HEADER_PAYLOAD — header not yet sent
 *   PENDING_PAYLOAD_ONLY   — header sent, payload remaining
 *
 * On success: item is cleaned up, stream deactivated on FIN.
 * On WOULD_BLOCK: item is enqueued in pending (caller must not free).
 * On error: item is cleaned up, bridge goes fatal.
 *
 * Returns MOQ_OK or error.
 */
static moq_result_t bridge_try_send_data(moq_transport_bridge_t *b,
                                          bridge_pending_item_t *p)
{
    moq_send_data_action_t *sd = &p->act.u.send_data;
    uint64_t sid = p->stream_id;

    /* Phase 1: send header (if any) */
    if (p->kind == PENDING_HEADER_PAYLOAD && sd->header_len > 0) {
        size_t pay_len = sd->payload ? moq_rcbuf_len(sd->payload) : 0;
        bool fin_on_hdr = (pay_len == 0) && sd->fin;

        moq_transport_result_t wr = sanitize_stream_result(
            b->ops->write(b->endpoint_ctx, sid,
                           sd->header, sd->header_len, fin_on_hdr));

        if (wr == MOQ_TRANSPORT_WOULD_BLOCK) {
            if (!bridge_enqueue_pending(b, p)) {
                bridge_cleanup_pending_item(&b->alloc, p);
                bridge_set_fatal(b, 0x1);
                return MOQ_ERR_INTERNAL;
            }
            memset(p, 0, sizeof(*p));
            return MOQ_OK;
        }
        if (wr == MOQ_TRANSPORT_ERROR) {
            bridge_cleanup_pending_item(&b->alloc, p);
            bridge_set_fatal(b, 0x1);
            return MOQ_ERR_INTERNAL;
        }

        if (fin_on_hdr) {
            bridge_mark_local_close(b, p->stream_ref);
            bridge_retire_or_tombstone(b, p->stream_ref);
            bridge_cleanup_pending_item(&b->alloc, p);
            return MOQ_OK;
        }

        p->kind = PENDING_PAYLOAD_ONLY;
    }

    /* Phase 2: send payload */
    if (sd->payload) {
        moq_transport_result_t wr;
        bool has_write_payload =
            (b->ops->capabilities & MOQ_TRANSPORT_CAP_WRITE_PAYLOAD) &&
            HAS_FIELD(b->ops, write_payload) &&
            b->ops->write_payload;

        if (has_write_payload) {
            wr = sanitize_stream_result(
                b->ops->write_payload(b->endpoint_ctx, sid,
                                       sd->payload, sd->fin));
        } else {
            wr = sanitize_stream_result(
                b->ops->write(b->endpoint_ctx, sid,
                               moq_rcbuf_data(sd->payload),
                               moq_rcbuf_len(sd->payload), sd->fin));
        }

        if (wr == MOQ_TRANSPORT_WOULD_BLOCK) {
            if (!bridge_enqueue_pending(b, p)) {
                bridge_cleanup_pending_item(&b->alloc, p);
                bridge_set_fatal(b, 0x1);
                return MOQ_ERR_INTERNAL;
            }
            memset(p, 0, sizeof(*p));
            return MOQ_OK;
        }
        if (wr == MOQ_TRANSPORT_ERROR) {
            bridge_cleanup_pending_item(&b->alloc, p);
            bridge_set_fatal(b, 0x1);
            return MOQ_ERR_INTERNAL;
        }
    } else if (sd->header_len == 0 && sd->fin) {
        /* Empty FIN-only (close subgroup) */
        moq_transport_result_t wr = sanitize_stream_result(
            b->ops->write(b->endpoint_ctx, sid, NULL, 0, true));

        if (wr == MOQ_TRANSPORT_WOULD_BLOCK) {
            if (!bridge_enqueue_pending(b, p)) {
                bridge_cleanup_pending_item(&b->alloc, p);
                bridge_set_fatal(b, 0x1);
                return MOQ_ERR_INTERNAL;
            }
            memset(p, 0, sizeof(*p));
            return MOQ_OK;
        }
        if (wr == MOQ_TRANSPORT_ERROR) {
            bridge_cleanup_pending_item(&b->alloc, p);
            bridge_set_fatal(b, 0x1);
            return MOQ_ERR_INTERNAL;
        }
    }

    /* Success — stream done if FIN */
    if (sd->fin) {
        bridge_mark_local_close(b, p->stream_ref);
        bridge_retire_or_tombstone(b, p->stream_ref);
    }
    bridge_cleanup_pending_item(&b->alloc, p);
    return MOQ_OK;
}

/* -- SEND_DATA dispatch --------------------------------------------- */

/*
 * Two-phase send: header via write(), payload via write_payload()
 * (or write() fallback). If the stream is new, open_uni first.
 *
 * On WOULD_BLOCK at any stage, the action is retained in the pending
 * queue with ownership transferred. Retry resumes from the point of
 * failure (open, header, or payload).
 */
static moq_result_t dispatch_send_data(moq_transport_bridge_t *b,
                                        moq_action_t *act)
{
    moq_send_data_action_t *sd = &act->u.send_data;
    moq_stream_ref_t ref = sd->stream_ref;

    bridge_stream_entry_t *e = bridge_find_by_ref(b, ref);
    if (!e) {
        /* Reserve a stream-map slot BEFORE opening transport stream.
         * If the map is full, this is fatal (resource exhaustion). */
        e = bridge_alloc_stream(b);
        if (!e) {
            moq_action_cleanup(act);
            bridge_set_fatal(b, 0x1);
            return MOQ_ERR_INTERNAL;
        }

        uint64_t uni_id = 0;
        moq_transport_result_t r = sanitize_stream_result(
            b->ops->open_uni(b->endpoint_ctx, &uni_id));

        if (r == MOQ_TRANSPORT_WOULD_BLOCK) {
            bridge_deactivate_stream(e);
            bridge_pending_item_t p;
            memset(&p, 0, sizeof(p));
            p.kind = PENDING_OPEN_UNI_DATA;
            p.stream_ref = ref;
            bridge_pending_take_action(&p, act);
            if (!bridge_enqueue_pending(b, &p)) {
                bridge_cleanup_pending_item(&b->alloc, &p);
                bridge_set_fatal(b, 0x1);
                return MOQ_ERR_INTERNAL;
            }
            return MOQ_OK;
        }
        if (r == MOQ_TRANSPORT_ERROR) {
            bridge_deactivate_stream(e);
            moq_action_cleanup(act);
            bridge_set_fatal(b, 0x1);
            return MOQ_ERR_INTERNAL;
        }
        e->ref = ref;
        e->transport_id = uni_id;
        e->kind = BRIDGE_STREAM_UNI;
        e->origin = BRIDGE_ORIGIN_LOCAL;
    }

    /* Enqueue as header+payload pending item — try_send_data does the
     * actual two-phase write and handles partial progress. */
    bridge_pending_item_t p;
    memset(&p, 0, sizeof(p));
    p.kind = PENDING_HEADER_PAYLOAD;
    p.stream_ref = ref;
    p.stream_id = e->transport_id;
    bridge_pending_take_action(&p, act);

    moq_result_t rc = bridge_try_send_data(b, &p);
    if (rc < 0) return rc;

    /* If item was not fully consumed, it's already in pending queue */
    return MOQ_OK;
}

/* -- RESET_DATA / STOP_DATA dispatch -------------------------------- */

static moq_result_t dispatch_reset_data(moq_transport_bridge_t *b,
                                         moq_action_t *act)
{
    /* RESET_DATA targets a unidirectional data stream; RESET_BIDI_STREAM the
     * local send half of a request bidi. Both reset a stream by ref with an
     * error code, so they share this dispatcher (reading the right field). */
    moq_stream_ref_t ref = (act->kind == MOQ_ACTION_RESET_BIDI_STREAM)
        ? act->u.reset_bidi_stream.stream_ref : act->u.reset_data.stream_ref;
    uint64_t error_code = (act->kind == MOQ_ACTION_RESET_BIDI_STREAM)
        ? act->u.reset_bidi_stream.error_code : act->u.reset_data.error_code;
    moq_action_cleanup(act);

    bridge_stream_entry_t *e = bridge_find_by_ref(b, ref);
    if (!e) return MOQ_OK;

    uint64_t sid = e->transport_id;
    bridge_deactivate_stream(e);

    moq_transport_result_t r = sanitize_stream_result(
        b->ops->reset_stream(b->endpoint_ctx, sid, error_code));

    if (r == MOQ_TRANSPORT_WOULD_BLOCK) {
        bridge_pending_item_t p;
        memset(&p, 0, sizeof(p));
        p.kind = PENDING_RESET_STREAM;
        p.stream_id = sid;
        p.error_code = error_code;
        if (!bridge_enqueue_pending(b, &p)) {
            bridge_set_fatal(b, 0x1);
            return MOQ_ERR_INTERNAL;
        }
        return MOQ_OK;
    }
    if (r == MOQ_TRANSPORT_ERROR) {
        bridge_set_fatal(b, 0x1);
        return MOQ_ERR_INTERNAL;
    }
    return MOQ_OK;
}

static moq_result_t dispatch_stop_data(moq_transport_bridge_t *b,
                                        moq_action_t *act)
{
    /* STOP_DATA targets a unidirectional data stream; STOP_BIDI_STREAM the
     * peer's send half of a request bidi. Both send STOP_SENDING by ref with an
     * error code, so they share this dispatcher (reading the right field). */
    moq_stream_ref_t ref = (act->kind == MOQ_ACTION_STOP_BIDI_STREAM)
        ? act->u.stop_bidi_stream.stream_ref : act->u.stop_data.stream_ref;
    uint64_t error_code = (act->kind == MOQ_ACTION_STOP_BIDI_STREAM)
        ? act->u.stop_bidi_stream.error_code : act->u.stop_data.error_code;
    moq_action_cleanup(act);

    bridge_stream_entry_t *e = bridge_find_by_ref(b, ref);
    if (!e) return MOQ_OK;

    moq_transport_result_t r = sanitize_stream_result(
        b->ops->stop_sending(b->endpoint_ctx, e->transport_id, error_code));

    if (r == MOQ_TRANSPORT_WOULD_BLOCK) {
        bridge_pending_item_t p;
        memset(&p, 0, sizeof(p));
        p.kind = PENDING_STOP_SENDING;
        p.stream_id = e->transport_id;
        p.error_code = error_code;
        if (!bridge_enqueue_pending(b, &p)) {
            bridge_set_fatal(b, 0x1);
            return MOQ_ERR_INTERNAL;
        }
        return MOQ_OK;
    }
    if (r == MOQ_TRANSPORT_ERROR) {
        bridge_set_fatal(b, 0x1);
        return MOQ_ERR_INTERNAL;
    }
    return MOQ_OK;
}

/* -- Bidi stream dispatch ------------------------------------------- */

static moq_result_t dispatch_open_bidi(moq_transport_bridge_t *b,
                                        moq_action_t *act)
{
    moq_open_bidi_stream_action_t *ob = &act->u.open_bidi_stream;
    moq_stream_ref_t ref = ob->stream_ref;
    const uint8_t *data = ob->data;
    size_t len = ob->len;
    bool fin = ob->fin;

    bridge_stream_entry_t *e = bridge_alloc_stream(b);
    if (!e) {
        moq_action_cleanup(act);
        bridge_set_fatal(b, 0x1);
        return MOQ_ERR_INTERNAL;
    }

    uint64_t bidi_id = 0;
    moq_transport_result_t r = sanitize_stream_result(
        b->ops->open_bidi(b->endpoint_ctx, &bidi_id));

    if (r == MOQ_TRANSPORT_WOULD_BLOCK) {
        bridge_deactivate_stream(e);
        bridge_pending_item_t p;
        memset(&p, 0, sizeof(p));
        p.kind = PENDING_OPEN_BIDI_DATA;
        p.stream_ref = ref;
        p.fin = fin;
        if (data && len > 0) {
            p.data = bridge_alloc(&b->alloc, len);
            if (!p.data) {
                moq_action_cleanup(act);
                bridge_set_fatal(b, 0x1);
                return MOQ_ERR_NOMEM;
            }
            memcpy(p.data, data, len);
            p.data_len = len;
        }
        moq_action_cleanup(act);
        if (!bridge_enqueue_pending(b, &p)) {
            if (p.data) b->alloc.free(p.data, p.data_len, b->alloc.ctx);
            bridge_set_fatal(b, 0x1);
            return MOQ_ERR_INTERNAL;
        }
        return MOQ_OK;
    }
    if (r == MOQ_TRANSPORT_ERROR) {
        bridge_deactivate_stream(e);
        moq_action_cleanup(act);
        bridge_set_fatal(b, 0x1);
        return MOQ_ERR_INTERNAL;
    }

    e->ref = ref;
    e->transport_id = bidi_id;
    e->kind = BRIDGE_STREAM_BIDI;
    e->origin = BRIDGE_ORIGIN_LOCAL;

    if (data && len > 0) {
        moq_transport_result_t wr = sanitize_stream_result(
            b->ops->write(b->endpoint_ctx, bidi_id, data, len, fin));
        if (wr == MOQ_TRANSPORT_WOULD_BLOCK) {
            bridge_pending_item_t p;
            memset(&p, 0, sizeof(p));
            p.kind = PENDING_COPIED_WRITE;
            p.stream_id = bidi_id;
            p.stream_ref = ref;
            p.fin = fin;
            p.data = bridge_alloc(&b->alloc, len);
            if (!p.data) {
                moq_action_cleanup(act);
                bridge_set_fatal(b, 0x1);
                return MOQ_ERR_NOMEM;
            }
            memcpy(p.data, data, len);
            p.data_len = len;
            moq_action_cleanup(act);
            if (!bridge_enqueue_pending(b, &p)) {
                b->alloc.free(p.data, p.data_len, b->alloc.ctx);
                bridge_set_fatal(b, 0x1);
                return MOQ_ERR_INTERNAL;
            }
            return MOQ_OK;
        }
        if (wr == MOQ_TRANSPORT_ERROR) {
            moq_action_cleanup(act);
            bridge_set_fatal(b, 0x1);
            return MOQ_ERR_INTERNAL;
        }
        /* Data + FIN written together: retire the local send half. */
        if (fin) {
            bridge_mark_local_close(b, ref);
            bridge_retire_or_tombstone(b, ref);
        }
    } else if (fin) {
        /* Open + immediate FIN with no data: close the send half. */
        moq_transport_result_t wr = sanitize_stream_result(
            b->ops->write(b->endpoint_ctx, bidi_id, NULL, 0, true));
        if (wr == MOQ_TRANSPORT_WOULD_BLOCK) {
            bridge_pending_item_t p;
            memset(&p, 0, sizeof(p));
            p.kind = PENDING_CLOSE_BIDI_FIN;
            p.stream_id = bidi_id;
            p.stream_ref = ref;
            moq_action_cleanup(act);
            if (!bridge_enqueue_pending(b, &p)) {
                bridge_set_fatal(b, 0x1);
                return MOQ_ERR_INTERNAL;
            }
            return MOQ_OK;
        }
        if (wr == MOQ_TRANSPORT_ERROR) {
            moq_action_cleanup(act);
            bridge_set_fatal(b, 0x1);
            return MOQ_ERR_INTERNAL;
        }
        bridge_mark_local_close(b, ref);
        bridge_retire_or_tombstone(b, ref);
    }

    moq_action_cleanup(act);
    return MOQ_OK;
}

static moq_result_t dispatch_send_bidi(moq_transport_bridge_t *b,
                                        moq_action_t *act)
{
    moq_send_bidi_stream_action_t *sb = &act->u.send_bidi_stream;
    moq_stream_ref_t ref = sb->stream_ref;
    const uint8_t *data = sb->data;
    size_t len = sb->len;
    bool fin = sb->fin;

    bridge_stream_entry_t *e = bridge_find_by_ref(b, ref);
    if (!e) {
        moq_action_cleanup(act);
        return MOQ_OK;
    }

    moq_transport_result_t wr = sanitize_stream_result(
        b->ops->write(b->endpoint_ctx, e->transport_id, data, len, fin));

    if (wr == MOQ_TRANSPORT_WOULD_BLOCK) {
        bridge_pending_item_t p;
        memset(&p, 0, sizeof(p));
        p.kind = PENDING_COPIED_WRITE;
        p.stream_id = e->transport_id;
        p.stream_ref = ref;
        p.fin = fin;
        if (data && len > 0) {
            p.data = bridge_alloc(&b->alloc, len);
            if (!p.data) {
                moq_action_cleanup(act);
                bridge_set_fatal(b, 0x1);
                return MOQ_ERR_NOMEM;
            }
            memcpy(p.data, data, len);
            p.data_len = len;
        }
        moq_action_cleanup(act);
        if (!bridge_enqueue_pending(b, &p)) {
            if (p.data) b->alloc.free(p.data, p.data_len, b->alloc.ctx);
            bridge_set_fatal(b, 0x1);
            return MOQ_ERR_INTERNAL;
        }
        return MOQ_OK;
    }

    if (wr == MOQ_TRANSPORT_ERROR) {
        moq_action_cleanup(act);
        bridge_set_fatal(b, 0x1);
        return MOQ_ERR_INTERNAL;
    }

    if (fin) {
        bridge_mark_local_close(b, ref);
        bridge_retire_or_tombstone(b, ref);
    }
    moq_action_cleanup(act);
    return MOQ_OK;
}

static moq_result_t dispatch_close_bidi(moq_transport_bridge_t *b,
                                         moq_action_t *act)
{
    moq_stream_ref_t ref = act->u.close_bidi_stream.stream_ref;
    moq_action_cleanup(act);

    bridge_stream_entry_t *e = bridge_find_by_ref(b, ref);
    if (!e) return MOQ_OK;

    uint64_t sid = e->transport_id;
    moq_transport_result_t wr = sanitize_stream_result(
        b->ops->write(b->endpoint_ctx, sid, NULL, 0, true));

    if (wr == MOQ_TRANSPORT_WOULD_BLOCK) {
        bridge_pending_item_t p;
        memset(&p, 0, sizeof(p));
        p.kind = PENDING_CLOSE_BIDI_FIN;
        p.stream_ref = ref;
        p.stream_id = sid;
        if (!bridge_enqueue_pending(b, &p)) {
            bridge_set_fatal(b, 0x1);
            return MOQ_ERR_INTERNAL;
        }
        return MOQ_OK;
    }

    if (wr == MOQ_TRANSPORT_ERROR) {
        bridge_set_fatal(b, 0x1);
        return MOQ_ERR_INTERNAL;
    }

    bridge_mark_local_close(b, ref);
    bridge_retire_or_tombstone(b, ref);
    return MOQ_OK;
}

/* -- Unidirectional control channel (uni-control-pair profiles) ----- */

static moq_result_t dispatch_open_uni_control(moq_transport_bridge_t *b,
                                               moq_action_t *act)
{
    moq_open_uni_control_action_t *oc = &act->u.open_uni_control;
    moq_stream_ref_t ref = oc->stream_ref;
    const uint8_t *data = oc->data;
    size_t len = oc->len;

    /* A profile must open exactly one local uni control channel; a second
     * OPEN_UNI_CONTROL would orphan the first. Treat as an internal error
     * rather than silently replacing the recorded channel. */
    if (b->local_ctrl_uni_open) {
        moq_action_cleanup(act);
        bridge_set_fatal(b, 0x1);
        return MOQ_ERR_INTERNAL;
    }

    bridge_stream_entry_t *e = bridge_alloc_stream(b);
    if (!e) {
        moq_action_cleanup(act);
        bridge_set_fatal(b, 0x1);
        return MOQ_ERR_INTERNAL;
    }

    uint64_t uni_id = 0;
    moq_transport_result_t r = sanitize_stream_result(
        b->ops->open_uni(b->endpoint_ctx, &uni_id));

    if (r == MOQ_TRANSPORT_WOULD_BLOCK) {
        bridge_deactivate_stream(e);
        bridge_pending_item_t p;
        memset(&p, 0, sizeof(p));
        p.kind = PENDING_OPEN_UNI_CONTROL;
        p.stream_ref = ref;
        if (data && len > 0) {
            p.data = bridge_alloc(&b->alloc, len);
            if (!p.data) {
                moq_action_cleanup(act);
                bridge_set_fatal(b, 0x1);
                return MOQ_ERR_NOMEM;
            }
            memcpy(p.data, data, len);
            p.data_len = len;
        }
        moq_action_cleanup(act);
        if (!bridge_enqueue_pending(b, &p)) {
            if (p.data) b->alloc.free(p.data, p.data_len, b->alloc.ctx);
            bridge_set_fatal(b, 0x1);
            return MOQ_ERR_INTERNAL;
        }
        return MOQ_OK;
    }
    if (r == MOQ_TRANSPORT_ERROR) {
        bridge_deactivate_stream(e);
        moq_action_cleanup(act);
        bridge_set_fatal(b, 0x1);
        return MOQ_ERR_INTERNAL;
    }

    e->ref = ref;
    e->transport_id = uni_id;
    e->kind = BRIDGE_STREAM_UNI;
    e->origin = BRIDGE_ORIGIN_LOCAL;
    /* Mark the local control stream as control so a peer RESET/STOP_SENDING of
     * it is treated as a fatal control-channel teardown, not an unknown-data
     * no-op. */
    e->uni_disp = BRIDGE_UNI_DISP_CONTROL;

    b->local_ctrl_uni_open = true;
    b->local_ctrl_uni_stream_id = uni_id;
    b->local_ctrl_uni_ref = ref;

    if (data && len > 0) {
        moq_transport_result_t wr = sanitize_stream_result(
            b->ops->write(b->endpoint_ctx, uni_id, data, len, false));
        if (wr == MOQ_TRANSPORT_WOULD_BLOCK) {
            bridge_pending_item_t p;
            memset(&p, 0, sizeof(p));
            p.kind = PENDING_COPIED_WRITE;
            p.stream_id = uni_id;
            p.stream_ref = ref;
            p.data = bridge_alloc(&b->alloc, len);
            if (!p.data) {
                moq_action_cleanup(act);
                bridge_set_fatal(b, 0x1);
                return MOQ_ERR_NOMEM;
            }
            memcpy(p.data, data, len);
            p.data_len = len;
            moq_action_cleanup(act);
            if (!bridge_enqueue_pending(b, &p)) {
                b->alloc.free(p.data, p.data_len, b->alloc.ctx);
                bridge_set_fatal(b, 0x1);
                return MOQ_ERR_INTERNAL;
            }
            return MOQ_OK;
        }
        if (wr == MOQ_TRANSPORT_ERROR) {
            moq_action_cleanup(act);
            bridge_set_fatal(b, 0x1);
            return MOQ_ERR_INTERNAL;
        }
    }

    moq_action_cleanup(act);
    return MOQ_OK;
}

static moq_result_t dispatch_send_uni_control(moq_transport_bridge_t *b,
                                               moq_action_t *act)
{
    moq_send_uni_control_action_t *sc = &act->u.send_uni_control;
    moq_stream_ref_t ref = sc->stream_ref;
    const uint8_t *data = sc->data;
    size_t len = sc->len;
    bool fin = sc->fin;

    bridge_stream_entry_t *e = bridge_find_by_ref(b, ref);
    if (!e) {
        moq_action_cleanup(act);
        return MOQ_OK;
    }

    moq_transport_result_t wr = sanitize_stream_result(
        b->ops->write(b->endpoint_ctx, e->transport_id, data, len, fin));

    if (wr == MOQ_TRANSPORT_WOULD_BLOCK) {
        bridge_pending_item_t p;
        memset(&p, 0, sizeof(p));
        p.kind = PENDING_COPIED_WRITE;
        p.stream_id = e->transport_id;
        p.stream_ref = ref;
        p.fin = fin;
        if (data && len > 0) {
            p.data = bridge_alloc(&b->alloc, len);
            if (!p.data) {
                moq_action_cleanup(act);
                bridge_set_fatal(b, 0x1);
                return MOQ_ERR_NOMEM;
            }
            memcpy(p.data, data, len);
            p.data_len = len;
        }
        moq_action_cleanup(act);
        if (!bridge_enqueue_pending(b, &p)) {
            if (p.data) b->alloc.free(p.data, p.data_len, b->alloc.ctx);
            bridge_set_fatal(b, 0x1);
            return MOQ_ERR_INTERNAL;
        }
        return MOQ_OK;
    }

    if (wr == MOQ_TRANSPORT_ERROR) {
        moq_action_cleanup(act);
        bridge_set_fatal(b, 0x1);
        return MOQ_ERR_INTERNAL;
    }

    if (fin) {
        bridge_on_local_ctrl_uni_fin(b, e->transport_id);
        bridge_deactivate_stream(e);
    }
    moq_action_cleanup(act);
    return MOQ_OK;
}

/* ------------------------------------------------------------------- */

static moq_result_t bridge_process_action(moq_transport_bridge_t *b,
                                           moq_action_t *act)
{
    switch (act->kind) {
    case MOQ_ACTION_SEND_CONTROL:
        return dispatch_send_control(b, act);

    case MOQ_ACTION_OPEN_UNI_CONTROL:
        return dispatch_open_uni_control(b, act);

    case MOQ_ACTION_SEND_UNI_CONTROL:
        return dispatch_send_uni_control(b, act);

    case MOQ_ACTION_CLOSE_SESSION:
        return dispatch_close_session(b, act);

    case MOQ_ACTION_SEND_DATA:
        return dispatch_send_data(b, act);

    case MOQ_ACTION_RESET_DATA:
        return dispatch_reset_data(b, act);

    case MOQ_ACTION_STOP_DATA:
        return dispatch_stop_data(b, act);

    case MOQ_ACTION_RESET_BIDI_STREAM:
        return dispatch_reset_data(b, act);

    case MOQ_ACTION_STOP_BIDI_STREAM:
        return dispatch_stop_data(b, act);

    case MOQ_ACTION_OPEN_BIDI_STREAM:
        return dispatch_open_bidi(b, act);

    case MOQ_ACTION_SEND_BIDI_STREAM:
        return dispatch_send_bidi(b, act);

    case MOQ_ACTION_CLOSE_BIDI_STREAM:
        return dispatch_close_bidi(b, act);

    case MOQ_ACTION_SEND_DATAGRAM: {
        const moq_send_datagram_action_t *dg = &act->u.send_datagram;
        if (!(b->ops->capabilities & MOQ_TRANSPORT_CAP_DATAGRAM) ||
            !b->ops->send_datagram) {
            moq_action_cleanup(act);
            return MOQ_OK;  /* silently drop — no datagram support */
        }
        moq_transport_result_t r = b->ops->send_datagram(
            b->endpoint_ctx, dg->data, dg->len);
        moq_action_cleanup(act);
        switch (r) {
        case MOQ_TRANSPORT_OK:
        case MOQ_TRANSPORT_WOULD_BLOCK:
        case MOQ_TRANSPORT_DROPPED:
        case MOQ_TRANSPORT_TOO_LARGE:
            return MOQ_OK;  /* non-fatal, no retry */
        case MOQ_TRANSPORT_ERROR:
        default:
            bridge_set_fatal(b, 0x1);
            return MOQ_ERR_INTERNAL;
        }
    }

    default:
        moq_action_cleanup(act);
        bridge_set_fatal(b, 0x1);
        return MOQ_ERR_INTERNAL;
    }
}

static moq_result_t bridge_drain_actions(moq_transport_bridge_t *b)
{
    moq_action_t act;
    while (moq_session_poll_actions(b->session, &act, 1) > 0) {
        moq_result_t rc = bridge_process_action(b, &act);
        if (rc < 0) return rc;
        if (b->pending_count > 0) break;
        if (b->fatal || b->closed) break;
    }
    return MOQ_OK;
}

/* -- Outbound: retry pending ---------------------------------------- */

static moq_result_t bridge_retry_outbound_pending(moq_transport_bridge_t *b)
{
    if (b->pending_count == 0) return MOQ_OK;

    uint32_t old_count = b->pending_count;
    bridge_pending_item_t *old = b->pending;

    b->pending = bridge_alloc(&b->alloc,
        b->max_pending * sizeof(bridge_pending_item_t));
    if (!b->pending) {
        b->pending = old;
        bridge_set_fatal(b, 0x1);
        return MOQ_ERR_NOMEM;
    }
    memset(b->pending, 0, b->max_pending * sizeof(bridge_pending_item_t));
    b->pending_count = 0;

    moq_result_t rc = MOQ_OK;
    uint32_t i = 0;

    for (; i < old_count; i++) {
        bridge_pending_item_t *p = &old[i];

        switch (p->kind) {
        case PENDING_OPEN_CONTROL: {
            if (!b->control_open) {
                if (b->is_client) {
                    uint64_t bidi_id = 0;
                    moq_transport_result_t r = sanitize_stream_result(
                        b->ops->open_bidi(b->endpoint_ctx, &bidi_id));
                    if (r == MOQ_TRANSPORT_WOULD_BLOCK) goto stop;
                    if (r == MOQ_TRANSPORT_ERROR) {
                        bridge_set_fatal(b, 0x1);
                        rc = MOQ_ERR_INTERNAL;
                        goto cleanup;
                    }
                    b->control_stream_id = bidi_id;
                } else {
                    if (b->peer_control_stream_id == UINT64_MAX) goto stop;
                    b->control_stream_id = b->peer_control_stream_id;
                }
                b->control_open = true;
            }
            moq_transport_result_t wr = sanitize_stream_result(
                b->ops->write(b->endpoint_ctx, b->control_stream_id,
                               p->data, p->data_len, false));
            if (wr == MOQ_TRANSPORT_WOULD_BLOCK) goto stop;
            if (wr == MOQ_TRANSPORT_ERROR) {
                bridge_set_fatal(b, 0x1);
                rc = MOQ_ERR_INTERNAL;
                goto cleanup;
            }
            bridge_cleanup_pending_item(&b->alloc, p);
            break;
        }

        case PENDING_COPIED_WRITE: {
            moq_transport_result_t wr = sanitize_stream_result(
                b->ops->write(b->endpoint_ctx, p->stream_id,
                               p->data, p->data_len, p->fin));
            if (wr == MOQ_TRANSPORT_WOULD_BLOCK) goto stop;
            if (wr == MOQ_TRANSPORT_ERROR) {
                bridge_set_fatal(b, 0x1);
                rc = MOQ_ERR_INTERNAL;
                goto cleanup;
            }
            if (p->fin && p->stream_ref._v != 0) {
                /* If this was the local uni control channel, clear its state
                 * (mirrors the immediate dispatch_send_uni_control path). */
                bridge_on_local_ctrl_uni_fin(b, p->stream_id);
                bridge_mark_local_close(b, p->stream_ref);
                bridge_retire_or_tombstone(b, p->stream_ref);
            }
            bridge_cleanup_pending_item(&b->alloc, p);
            break;
        }

        case PENDING_CLOSE_TRANSPORT: {
            uint64_t close_code = p->error_code;
            moq_transport_result_t r = sanitize_stream_result(
                b->ops->close_transport(b->endpoint_ctx, close_code,
                                         p->data, p->data_len));
            if (r == MOQ_TRANSPORT_WOULD_BLOCK) goto stop;
            bridge_cleanup_pending_item(&b->alloc, p);
            if (r == MOQ_TRANSPORT_ERROR) {
                bridge_set_fatal(b, close_code);
                rc = MOQ_ERR_INTERNAL;
                goto cleanup;
            }
            b->closed = true;
            b->close_code = close_code;
            for (uint32_t j = i + 1; j < old_count; j++)
                bridge_cleanup_pending_item(&b->alloc, &old[j]);
            bridge_free(&b->alloc, old,
                        b->max_pending * sizeof(bridge_pending_item_t));
            bridge_clear_all_state(b);
            return MOQ_OK;
        }

        case PENDING_RESET_STREAM: {
            moq_transport_result_t r = sanitize_stream_result(
                b->ops->reset_stream(b->endpoint_ctx, p->stream_id,
                                      p->error_code));
            if (r == MOQ_TRANSPORT_WOULD_BLOCK) goto stop;
            if (r == MOQ_TRANSPORT_ERROR) {
                bridge_set_fatal(b, 0x1);
                rc = MOQ_ERR_INTERNAL;
                goto cleanup;
            }
            bridge_cleanup_pending_item(&b->alloc, p);
            break;
        }

        case PENDING_STOP_SENDING: {
            moq_transport_result_t r = sanitize_stream_result(
                b->ops->stop_sending(b->endpoint_ctx, p->stream_id,
                                      p->error_code));
            if (r == MOQ_TRANSPORT_WOULD_BLOCK) goto stop;
            if (r == MOQ_TRANSPORT_ERROR) {
                bridge_set_fatal(b, 0x1);
                rc = MOQ_ERR_INTERNAL;
                goto cleanup;
            }
            bridge_cleanup_pending_item(&b->alloc, p);
            break;
        }

        case PENDING_OPEN_UNI_DATA: {
            /* Reserve stream-map slot before opening transport stream */
            bridge_stream_entry_t *e = bridge_alloc_stream(b);
            if (!e) {
                bridge_set_fatal(b, 0x1);
                rc = MOQ_ERR_INTERNAL;
                goto cleanup;
            }

            uint64_t uni_id = 0;
            moq_transport_result_t r = sanitize_stream_result(
                b->ops->open_uni(b->endpoint_ctx, &uni_id));
            if (r == MOQ_TRANSPORT_WOULD_BLOCK) {
                bridge_deactivate_stream(e);
                goto stop;
            }
            if (r == MOQ_TRANSPORT_ERROR) {
                bridge_deactivate_stream(e);
                bridge_set_fatal(b, 0x1);
                rc = MOQ_ERR_INTERNAL;
                goto cleanup;
            }
            e->ref = p->stream_ref;
            e->transport_id = uni_id;
            e->kind = BRIDGE_STREAM_UNI;
            e->origin = BRIDGE_ORIGIN_LOCAL;
            p->kind = PENDING_HEADER_PAYLOAD;
            p->stream_id = uni_id;
        }
        /* fall through - reuse this item to send header+payload */

        case PENDING_HEADER_PAYLOAD:
        case PENDING_PAYLOAD_ONLY: {
            moq_result_t trc = bridge_try_send_data(b, p);
            if (trc < 0) {
                rc = trc;
                goto cleanup;
            }
            /* bridge_try_send_data either consumed the item (zeroed p)
             * or moved it into the new pending queue on WOULD_BLOCK.
             * Either way, old[i] is now zeroed — skip it in stop. */
            if (b->pending_count > 0) {
                i++;  /* old[i] already handled — start stop from i+1 */
                goto stop;
            }
            break;
        }

        case PENDING_OPEN_UNI_CONTROL: {
            bridge_stream_entry_t *e = bridge_alloc_stream(b);
            if (!e) {
                bridge_set_fatal(b, 0x1);
                rc = MOQ_ERR_INTERNAL;
                goto cleanup;
            }
            uint64_t uni_id = 0;
            moq_transport_result_t r = sanitize_stream_result(
                b->ops->open_uni(b->endpoint_ctx, &uni_id));
            if (r == MOQ_TRANSPORT_WOULD_BLOCK) {
                bridge_deactivate_stream(e);
                goto stop;
            }
            if (r == MOQ_TRANSPORT_ERROR) {
                bridge_deactivate_stream(e);
                bridge_set_fatal(b, 0x1);
                rc = MOQ_ERR_INTERNAL;
                goto cleanup;
            }
            e->ref = p->stream_ref;
            e->transport_id = uni_id;
            e->kind = BRIDGE_STREAM_UNI;
            e->origin = BRIDGE_ORIGIN_LOCAL;
            /* See the immediate OPEN_UNI_CONTROL path: mark control so a peer
             * RESET/STOP_SENDING is a fatal control-channel teardown. */
            e->uni_disp = BRIDGE_UNI_DISP_CONTROL;
            b->local_ctrl_uni_open = true;
            b->local_ctrl_uni_stream_id = uni_id;
            b->local_ctrl_uni_ref = p->stream_ref;
            if (p->data && p->data_len > 0) {
                moq_transport_result_t wr = sanitize_stream_result(
                    b->ops->write(b->endpoint_ctx, uni_id,
                                   p->data, p->data_len, false));
                if (wr == MOQ_TRANSPORT_WOULD_BLOCK) {
                    p->kind = PENDING_COPIED_WRITE;
                    p->stream_id = uni_id;
                    goto stop;
                }
                if (wr == MOQ_TRANSPORT_ERROR) {
                    bridge_set_fatal(b, 0x1);
                    rc = MOQ_ERR_INTERNAL;
                    goto cleanup;
                }
            }
            bridge_cleanup_pending_item(&b->alloc, p);
            break;
        }

        case PENDING_OPEN_BIDI_DATA: {
            bridge_stream_entry_t *e = bridge_alloc_stream(b);
            if (!e) {
                bridge_set_fatal(b, 0x1);
                rc = MOQ_ERR_INTERNAL;
                goto cleanup;
            }
            uint64_t bidi_id = 0;
            moq_transport_result_t r = sanitize_stream_result(
                b->ops->open_bidi(b->endpoint_ctx, &bidi_id));
            if (r == MOQ_TRANSPORT_WOULD_BLOCK) {
                bridge_deactivate_stream(e);
                goto stop;
            }
            if (r == MOQ_TRANSPORT_ERROR) {
                bridge_deactivate_stream(e);
                bridge_set_fatal(b, 0x1);
                rc = MOQ_ERR_INTERNAL;
                goto cleanup;
            }
            e->ref = p->stream_ref;
            e->transport_id = bidi_id;
            e->kind = BRIDGE_STREAM_BIDI;
            e->origin = BRIDGE_ORIGIN_LOCAL;
            if (p->data && p->data_len > 0) {
                moq_transport_result_t wr = sanitize_stream_result(
                    b->ops->write(b->endpoint_ctx, bidi_id,
                                   p->data, p->data_len, p->fin));
                if (wr == MOQ_TRANSPORT_WOULD_BLOCK) {
                    p->kind = PENDING_COPIED_WRITE;
                    p->stream_id = bidi_id;
                    goto stop;
                }
                if (wr == MOQ_TRANSPORT_ERROR) {
                    bridge_set_fatal(b, 0x1);
                    rc = MOQ_ERR_INTERNAL;
                    goto cleanup;
                }
                if (p->fin) {
                    bridge_mark_local_close(b, p->stream_ref);
                    bridge_retire_or_tombstone(b, p->stream_ref);
                }
            } else if (p->fin) {
                moq_transport_result_t wr = sanitize_stream_result(
                    b->ops->write(b->endpoint_ctx, bidi_id, NULL, 0, true));
                if (wr == MOQ_TRANSPORT_WOULD_BLOCK) {
                    p->kind = PENDING_CLOSE_BIDI_FIN;
                    p->stream_id = bidi_id;
                    goto stop;
                }
                if (wr == MOQ_TRANSPORT_ERROR) {
                    bridge_set_fatal(b, 0x1);
                    rc = MOQ_ERR_INTERNAL;
                    goto cleanup;
                }
                bridge_mark_local_close(b, p->stream_ref);
                bridge_retire_or_tombstone(b, p->stream_ref);
            }
            bridge_cleanup_pending_item(&b->alloc, p);
            break;
        }

        case PENDING_CLOSE_BIDI_FIN: {
            moq_transport_result_t wr = sanitize_stream_result(
                b->ops->write(b->endpoint_ctx, p->stream_id,
                               NULL, 0, true));
            if (wr == MOQ_TRANSPORT_WOULD_BLOCK) goto stop;
            if (wr == MOQ_TRANSPORT_ERROR) {
                bridge_set_fatal(b, 0x1);
                rc = MOQ_ERR_INTERNAL;
                goto cleanup;
            }
            bridge_mark_local_close(b, p->stream_ref);
            bridge_retire_or_tombstone(b, p->stream_ref);
            bridge_cleanup_pending_item(&b->alloc, p);
            break;
        }

        default:
            bridge_cleanup_pending_item(&b->alloc, p);
            break;
        }
    }
    goto done;

stop:
    for (; i < old_count; i++) {
        if (!bridge_enqueue_pending(b, &old[i])) {
            bridge_cleanup_pending_item(&b->alloc, &old[i]);
        }
        memset(&old[i], 0, sizeof(old[i]));
    }
    goto done;

cleanup:
    for (; i < old_count; i++)
        bridge_cleanup_pending_item(&b->alloc, &old[i]);

done:
    bridge_free(&b->alloc, old, b->max_pending * sizeof(bridge_pending_item_t));
    return rc;
}

/* -- Inbound: retry ------------------------------------------------- */

static bool bridge_retry_inbound_pending(moq_transport_bridge_t *b,
                                          uint64_t now_us)
{
    bool progress = false;

    if (b->pending_control) {
        moq_result_t rc = moq_session_process_pending(b->session, now_us);
        if (rc != MOQ_ERR_WOULD_BLOCK) {
            b->pending_control = false;
            progress = true;
            if (rc < 0) {
                bridge_set_fatal(b, 0x1);
                return progress;
            }
            if (b->pending_control_fin) {
                b->pending_control_fin = false;
                /* Close code must match the immediate (non-blocked) control-FIN
                 * path so it does not depend on whether processing blocked:
                 * uni-control-pair treats a control-stream FIN as a protocol
                 * violation (0x3); bidi-control keeps its existing code (0x1). */
                uint64_t fin_code =
                    (b->control_mode == BRIDGE_CONTROL_UNI_PAIR) ? 0x3 : 0x1;
                moq_session_on_transport_close(b->session, fin_code, now_us);
                b->needs_close = true;
                b->needs_close_code = fin_code;
            }
        }
    }

    for (uint32_t i = 0; i < b->max_streams && !b->fatal; i++) {
        bridge_stream_entry_t *e = &b->streams[i];
        if (!e->active) continue;

        if (e->pending_retry) {
            moq_result_t rc;
            if (e->kind == BRIDGE_STREAM_BIDI)
                rc = moq_session_on_bidi_stream_bytes(
                    b->session, e->ref, NULL, 0, false, now_us);
            else
                rc = moq_session_on_data_bytes(
                    b->session, e->ref, NULL, 0, false, now_us);

            if (rc != MOQ_ERR_WOULD_BLOCK) {
                e->pending_retry = false;
                progress = true;
                if (rc < 0) {
                    bridge_set_fatal(b, 0x1);
                    return progress;
                }
                /* If the drain dropped the stream (binding gone) and no FIN is
                 * pending, discard further inbound bytes rather than reopen a
                 * fresh rx entry on later bytes. */
                if (!e->pending_fin && !e->fin_retained)
                    bridge_discard_if_dropped(b, e);
                if (e->pending_fin && !e->fin_retained) {
                    moq_result_t frc;
                    if (e->kind == BRIDGE_STREAM_BIDI)
                        frc = moq_session_on_bidi_stream_bytes(
                            b->session, e->ref, NULL, 0, true, now_us);
                    else
                        frc = moq_session_on_data_bytes(
                            b->session, e->ref, NULL, 0, true, now_us);
                    if (frc == MOQ_ERR_WOULD_BLOCK) {
                        /* FIN delivery blocked — keep stream pending */
                        e->pending_retry = true;
                        e->fin_retained = true;
                        continue;
                    }
                    if (frc < 0)
                        bridge_set_fatal(b, 0x1);
                }
                if (e->fin_retained || e->pending_fin) {
                    e->peer_send_closed = true;
                    bridge_retire_or_tombstone(b, e->ref);
                }
                e->pending_fin = false;
                e->fin_retained = false;
            }
        }

        if (e->pending_reset && e->active) {
            moq_result_t rc;
            if (e->kind == BRIDGE_STREAM_BIDI)
                rc = moq_session_on_bidi_stream_reset(
                    b->session, e->ref, e->pending_reset_code, now_us);
            else
                rc = moq_session_on_data_reset(
                    b->session, e->ref, e->pending_reset_code, now_us);
            if (rc != MOQ_ERR_WOULD_BLOCK) {
                e->pending_reset = false;
                progress = true;
                if (rc < 0)
                    bridge_set_fatal(b, 0x1);
                else
                    bridge_deactivate_stream(e);
            }
        }

        if (e->pending_stop && e->active) {
            /* A peer STOP_SENDING on a request bidi is the D18 cancellation
             * signal and is distinct from a RESET_STREAM: route it through
             * the dedicated bidi-stop input, never on_bidi_stream_reset. */
            moq_result_t rc = (e->kind == BRIDGE_STREAM_BIDI)
                ? moq_session_on_bidi_stream_stop(
                      b->session, e->ref, e->pending_stop_code, now_us)
                : moq_session_on_data_stop(
                      b->session, e->ref, e->pending_stop_code, now_us);
            if (rc != MOQ_ERR_WOULD_BLOCK) {
                e->pending_stop = false;
                progress = true;
                if (rc < 0 && rc != MOQ_ERR_WOULD_BLOCK)
                    bridge_set_fatal(b, 0x1);
            }
        }
    }

    return progress;
}

/* -- Service -------------------------------------------------------- */

moq_result_t moq_transport_bridge_service(
    moq_transport_bridge_t *b, uint64_t now_us)
{
    if (!b) return MOQ_ERR_INVAL;
    if (b->fatal) return MOQ_ERR_INTERNAL;
    if (b->closed) return MOQ_OK;

    for (;;) {
        /* Deferred close has priority — runs even if writes are blocked.
         * Session has already been notified; this closes the transport. */
        if (!b->fatal && !b->closed && b->needs_close) {
            b->needs_close = false;
            moq_transport_result_t cr = sanitize_stream_result(
                b->ops->close_transport(b->endpoint_ctx,
                                         b->needs_close_code, NULL, 0));
            if (cr == MOQ_TRANSPORT_WOULD_BLOCK) {
                bridge_cleanup_all_pending(b);
                bridge_pending_item_t p;
                memset(&p, 0, sizeof(p));
                p.kind = PENDING_CLOSE_TRANSPORT;
                p.error_code = b->needs_close_code;
                if (!bridge_enqueue_pending(b, &p)) {
                    bridge_set_fatal(b, b->needs_close_code);
                    return MOQ_ERR_INTERNAL;
                }
            } else if (cr == MOQ_TRANSPORT_ERROR) {
                bridge_set_fatal(b, b->needs_close_code);
                return MOQ_ERR_INTERNAL;
            } else {
                b->closed = true;
                b->close_code = b->needs_close_code;
                bridge_clear_all_state(b);
                break;
            }
        }

        /* Step 1: retry outbound pending FIFO */
        if (b->pending_count > 0) {
            moq_result_t rc = bridge_retry_outbound_pending(b);
            if (rc < 0) return rc;
            if (b->pending_count > 0)
                break;  /* still blocked — do not tick or drain */
        }

        /* Step 2: drain new actions (only if pending queue empty) */
        if (b->pending_count == 0 && !b->fatal && !b->closed) {
            moq_result_t rc = bridge_drain_actions(b);
            if (rc < 0) return rc;
        }

        /* Step 3: retry inbound pending */
        if (!b->fatal && !b->closed) {
            bool inbound_progress = bridge_retry_inbound_pending(b, now_us);
            if (inbound_progress && !b->fatal && !b->closed)
                continue;
        }

        /* Step 4: tick deadlines — only if outbound is clear */
        if (!b->fatal && !b->closed && b->pending_count == 0) {
            uint64_t dl = moq_session_next_deadline_us(b->session);
            if (dl != UINT64_MAX && dl <= now_us) {
                moq_result_t trc = moq_session_tick(b->session, now_us);
                if (trc < 0 && trc != MOQ_ERR_WOULD_BLOCK) {
                    bridge_set_fatal(b, 0x1);
                    return MOQ_ERR_INTERNAL;
                }
                if (!b->fatal && !b->closed && b->pending_count == 0)
                    continue;
            }
        }

        break;
    }

    return b->fatal ? MOQ_ERR_INTERNAL : MOQ_OK;
}

/* -- Inbound: control bytes ----------------------------------------- */

moq_result_t moq_transport_bridge_on_peer_control_bytes(
    moq_transport_bridge_t *b, uint64_t stream_id,
    const uint8_t *data, size_t len, bool fin, uint64_t now_us)
{
    if (!b || b->fatal || b->closed) return MOQ_ERR_CLOSED;

    if (b->pending_control) {
        if (len > 0) {
            bridge_set_fatal(b, 0x3);
            return MOQ_ERR_PROTO;
        }
        if (fin) b->pending_control_fin = true;
        return MOQ_ERR_WOULD_BLOCK;
    }

    if (!b->peer_control_open) {
        b->peer_control_open = true;
        b->peer_control_stream_id = stream_id;
    } else if (b->peer_control_stream_id != stream_id) {
        bridge_set_fatal(b, 0x3);
        return MOQ_ERR_PROTO;
    }

    moq_result_t rc = moq_session_on_control_bytes(
        b->session, data, len, now_us);

    if (rc == MOQ_ERR_WOULD_BLOCK) {
        b->pending_control = true;
        b->pending_control_stream_id = stream_id;
        if (fin) b->pending_control_fin = true;
        return MOQ_ERR_WOULD_BLOCK;
    }

    if (rc < 0) {
        bridge_set_fatal(b, 0x1);
        return rc;
    }

    if (fin) {
        moq_session_on_transport_close(b->session, 0x1, now_us);
        b->needs_close = true;
        b->needs_close_code = 0x1;
    }

    return MOQ_OK;
}

/* -- Inbound: uni-control-pair classification (UNI_PAIR mode) ------- */

/* Feed bytes from the peer's unidirectional control channel into the session
 * control path. On WOULD_BLOCK the session has retained the bytes in its
 * control buffer; we record pending-control state so moq_transport_bridge_service
 * retries via moq_session_process_pending (shared with the bidi-control path) --
 * the adapter must not re-deliver. A control-channel FIN at the transport layer
 * terminates the session (draft-18: closing a control stream is a violation). */
static moq_result_t bridge_feed_peer_control(
    moq_transport_bridge_t *b, uint64_t stream_id,
    const uint8_t *data, size_t len, bool fin, uint64_t now_us)
{
    moq_result_t rc = MOQ_OK;
    if (len > 0)
        rc = moq_session_on_control_bytes(b->session, data, len, now_us);

    if (rc == MOQ_ERR_WOULD_BLOCK) {
        b->pending_control = true;
        b->pending_control_stream_id = stream_id;
        if (fin) b->pending_control_fin = true;
        return MOQ_ERR_WOULD_BLOCK;
    }
    if (rc < 0) {
        bridge_set_fatal(b, 0x1);
        return rc;
    }
    if (fin) {
        moq_session_on_transport_close(b->session, 0x3, now_us);
        b->needs_close = true;
        b->needs_close_code = 0x3;
    }
    return MOQ_OK;
}

/*
 * After feeding inbound bytes to the session for a peer uni data stream, the
 * session may have *dropped* the stream: it issues STOP_DATA when the bound
 * subscription went away (e.g. after SUBSCRIBE_DONE) or when rx capacity is
 * exhausted, freeing its rx entry without recording the stream as finished.
 * Once that happens the session no longer holds the stream, so replaying any
 * further inbound bytes would have it open a *fresh* rx entry and parse
 * mid-stream bytes as a leading stream type -> "unknown data stream type"
 * (0x3). Mark the bridge entry to discard the remainder instead.
 *
 * This is unambiguous on a non-FIN delivery: the session only records a stream
 * finished together with a FIN (pending_fin), so a freed entry after a non-FIN
 * feed is always a drop, never a completion -- it never masks a legitimate
 * "data after FIN" violation (which the session still reports as a hard error).
 */
static void bridge_discard_if_dropped(moq_transport_bridge_t *b,
                                      bridge_stream_entry_t *e)
{
    if (e && e->active && e->kind != BRIDGE_STREAM_BIDI &&
        e->uni_disp != BRIDGE_UNI_DISP_DISCARD &&
        !moq_session_has_transport_stream(b->session, e->ref))
        e->uni_disp = BRIDGE_UNI_DISP_DISCARD;
}

/* Feed bytes from a peer unidirectional data stream into the session data path
 * (same WOULD_BLOCK / FIN handling as the default uni path). */
static moq_result_t bridge_feed_peer_data(
    moq_transport_bridge_t *b, bridge_stream_entry_t *e, moq_stream_ref_t ref,
    const uint8_t *data, size_t len, bool fin, uint64_t now_us)
{
    if (e->uni_disp == BRIDGE_UNI_DISP_DISCARD) {
        /* Session already dropped this stream; swallow until FIN. */
        if (fin) bridge_deactivate_stream(e);
        return MOQ_OK;
    }
    moq_result_t rc = moq_session_on_data_bytes(b->session, ref, data, len,
                                                fin, now_us);
    if (rc == MOQ_ERR_WOULD_BLOCK) {
        if (moq_session_has_transport_stream(b->session, ref)) {
            /* Session retained the bytes; the service tick drains them. */
            e->pending_retry = true;
            if (fin) e->fin_retained = true;
            return MOQ_ERR_WOULD_BLOCK;
        }
        /* Refused without registering a transport stream (rx/action capacity
         * exhausted): discard the remainder rather than misparse later bytes.
         * Legitimate backpressure refusal -- never fatal. */
        e->uni_disp = BRIDGE_UNI_DISP_DISCARD;
        if (fin) bridge_deactivate_stream(e);
        return MOQ_OK;
    }
    if (rc < 0) {
        bridge_set_fatal(b, 0x1);
        return rc;
    }
    if (fin) {
        bridge_deactivate_stream(e);
        return MOQ_OK;
    }
    bridge_discard_if_dropped(b, e);
    return MOQ_OK;
}

/*
 * Handle an inbound peer unidirectional stream in uni-control-pair mode:
 * classify by leading stream type (buffering leading bytes until possible),
 * then route. Once classified, the disposition is fixed for the stream.
 */
static moq_result_t bridge_uni_pair_inbound(
    moq_transport_bridge_t *b, uint64_t stream_id,
    const uint8_t *data, size_t len, bool fin, uint64_t now_us)
{
    /* While the session is draining buffered control bytes, defer all further
     * inbound (the adapter retries) so nothing is consumed out of order. The
     * shared retry in moq_transport_bridge_service clears this. */
    if (b->pending_control) {
        if (fin && stream_id == b->pending_control_stream_id)
            b->pending_control_fin = true;
        return MOQ_ERR_WOULD_BLOCK;
    }

    bridge_stream_entry_t *e = bridge_find_by_id(b, stream_id);
    if (!e && len == 0 && !fin) return MOQ_OK;

    moq_stream_ref_t ref;
    if (!e) {
        ref = bridge_assign_inbound_ref(b, stream_id, BRIDGE_STREAM_UNI);
        if (ref._v == 0) return MOQ_ERR_INTERNAL;
        e = bridge_find_by_id(b, stream_id);
    } else {
        ref = e->ref;
    }

    /* Already classified: route directly. */
    if (e->uni_disp == BRIDGE_UNI_DISP_CONTROL)
        return bridge_feed_peer_control(b, stream_id, data, len, fin, now_us);
    if (e->uni_disp == BRIDGE_UNI_DISP_DATA)
        return bridge_feed_peer_data(b, e, ref, data, len, fin, now_us);
    if (e->uni_disp == BRIDGE_UNI_DISP_DISCARD) {
        if (fin) bridge_deactivate_stream(e);
        return MOQ_OK;
    }

    /* PENDING: classify using retained prefix + this chunk (peek up to 9). */
    uint8_t tmp[sizeof(e->classify_buf)];
    size_t tn = e->classify_len;
    memcpy(tmp, e->classify_buf, tn);
    size_t take = len < sizeof(tmp) - tn ? len : sizeof(tmp) - tn;
    if (take > 0) memcpy(tmp + tn, data, take);
    tn += take;

    moq_uni_class_t cls = moq_session_classify_peer_uni(b->session, tmp, tn);
    bridge_uni_route_t route = bridge_route_peer_uni(b, cls, stream_id);

    if (route == BRIDGE_UNI_ROUTE_NEED_MORE) {
        if (fin) {            /* stream ended before its type was complete */
            bridge_set_fatal(b, 0x3);
            return MOQ_ERR_PROTO;
        }
        /* On NEED_MORE the total is < a complete stream-type vi64 (< 9 bytes),
         * so the whole chunk fits in the retained buffer. */
        if (len > 0) memcpy(e->classify_buf + e->classify_len, data, len);
        e->classify_len = (uint8_t)(e->classify_len + len);
        return MOQ_OK;
    }
    if (route == BRIDGE_UNI_ROUTE_FATAL) {
        bridge_set_fatal(b, 0x3);    /* unknown type or second control channel */
        return MOQ_ERR_PROTO;
    }

    /* Classified: flush any retained prefix, then this chunk. */
    uint8_t prefix[sizeof(e->classify_buf)];
    size_t plen = e->classify_len;
    if (plen > 0) memcpy(prefix, e->classify_buf, plen);
    e->classify_len = 0;

    if (cls == MOQ_UNI_CLASS_CONTROL) {
        e->uni_disp = BRIDGE_UNI_DISP_CONTROL;
        /* Feed the retained prefix then the current chunk. Both retain their
         * bytes in the session control buffer on WOULD_BLOCK (no loss); a real
         * error (other than WOULD_BLOCK) has already set the bridge fatal. */
        moq_result_t r1 = bridge_feed_peer_control(b, stream_id, prefix, plen,
                                                   false, now_us);
        if (r1 < 0 && r1 != MOQ_ERR_WOULD_BLOCK)
            return r1;
        moq_result_t r2 = bridge_feed_peer_control(b, stream_id, data, len,
                                                   fin, now_us);
        if (r1 == MOQ_ERR_WOULD_BLOCK || r2 == MOQ_ERR_WOULD_BLOCK)
            return MOQ_ERR_WOULD_BLOCK;
        return r2;
    }
    if (cls == MOQ_UNI_CLASS_PADDING) {
        e->uni_disp = BRIDGE_UNI_DISP_DISCARD;
        if (fin) bridge_deactivate_stream(e);
        return MOQ_OK;
    }

    /* DATA */
    e->uni_disp = BRIDGE_UNI_DISP_DATA;
    if (plen > 0) {
        moq_result_t rc = bridge_feed_peer_data(b, e, ref, prefix, plen,
                                                false, now_us);
        if (rc < 0) return rc;
        e = bridge_find_by_ref(b, ref);
        if (!e) return MOQ_OK;
    }
    return bridge_feed_peer_data(b, e, ref, data, len, fin, now_us);
}

/* -- Inbound: uni data streams -------------------------------------- */

moq_result_t moq_transport_bridge_on_peer_uni_bytes(
    moq_transport_bridge_t *b, uint64_t stream_id,
    const uint8_t *data, size_t len, bool fin, uint64_t now_us)
{
    if (!b || b->fatal || b->closed) return MOQ_ERR_CLOSED;

    bridge_stream_entry_t *e = bridge_find_by_id(b, stream_id);

    if (e && e->uni_disp == BRIDGE_UNI_DISP_DISCARD) {
        /* Session dropped this stream (its subscription went away, or rx
         * capacity was exhausted) and is stopping it. Swallow remaining inbound
         * bytes until FIN -- replaying them would be misparsed as a fresh
         * stream. Legitimate, not a protocol violation. */
        if (fin) bridge_deactivate_stream(e);
        return MOQ_OK;
    }

    if (e && e->pending_retry) {
        /* The session retained earlier bytes for this stream and is awaiting a
         * drain (the service-tick retry). The transport does not re-deliver
         * stream bytes, so additional bytes must be APPENDED to the session's
         * retained input here. A stream is only pending_retry via the data path,
         * so this is always a data stream (control backpressure uses a separate
         * flag). If the session has since dropped the stream, discard the rest
         * rather than reopen a fresh rx entry on mid-stream bytes. */
        if (!moq_session_has_transport_stream(b->session, e->ref)) {
            e->pending_retry = false;
            e->pending_fin = false;
            e->fin_retained = false;
            e->uni_disp = BRIDGE_UNI_DISP_DISCARD;
            if (fin) bridge_deactivate_stream(e);
            return MOQ_OK;
        }
        moq_result_t rc = moq_session_on_data_bytes(
            b->session, e->ref, data, len, fin, now_us);
        if (rc == MOQ_ERR_WOULD_BLOCK) {
            if (fin) e->fin_retained = true;
            return MOQ_ERR_WOULD_BLOCK;
        }
        if (rc < 0) { bridge_set_fatal(b, 0x1); return rc; }
        e->pending_retry = false;
        e->pending_fin = false;
        e->fin_retained = false;
        if (fin) { bridge_deactivate_stream(e); return MOQ_OK; }
        bridge_discard_if_dropped(b, e);
        return MOQ_OK;
    }

    /* Uni-control-pair profiles classify peer unidirectional streams (control
     * vs data vs padding) before routing; the default (bidi-control) mode
     * treats every peer uni stream as data. */
    if (b->control_mode == BRIDGE_CONTROL_UNI_PAIR)
        return bridge_uni_pair_inbound(b, stream_id, data, len, fin, now_us);

    /* Don't allocate a stream entry for empty non-FIN on unknown stream */
    if (!e && len == 0 && !fin) return MOQ_OK;

    moq_stream_ref_t ref;
    if (!e) {
        ref = bridge_assign_inbound_ref(b, stream_id, BRIDGE_STREAM_UNI);
        if (ref._v == 0) return MOQ_ERR_INTERNAL;
        e = bridge_find_by_id(b, stream_id);
    } else {
        ref = e->ref;
    }

    moq_result_t rc = moq_session_on_data_bytes(
        b->session, ref, data, len, fin, now_us);

    if (rc == MOQ_ERR_WOULD_BLOCK) {
        if (moq_session_has_transport_stream(b->session, ref)) {
            /* Legitimate inbound backpressure: the session retained the bytes.
             * Let the service tick drain them; never fatal. */
            e->pending_retry = true;
            if (fin) e->fin_retained = true;
            return MOQ_ERR_WOULD_BLOCK;
        }
        /* The session WOULD_BLOCKed before registering a transport stream (rx
         * table / action queue full) and retained nothing. Replaying later
         * bytes would misparse; discard the remainder until FIN. Not fatal. */
        e->uni_disp = BRIDGE_UNI_DISP_DISCARD;
        if (fin) bridge_deactivate_stream(e);
        return MOQ_OK;
    }

    if (rc < 0) {
        bridge_set_fatal(b, 0x1);
        return rc;
    }

    if (fin) {
        bridge_deactivate_stream(e);
        return MOQ_OK;
    }

    bridge_discard_if_dropped(b, e);
    return MOQ_OK;
}

moq_result_t moq_transport_bridge_on_peer_uni_rcbuf(
    moq_transport_bridge_t *b, uint64_t stream_id,
    moq_rcbuf_t *data, bool fin, uint64_t now_us)
{
    if (!b || b->fatal || b->closed) return MOQ_ERR_CLOSED;

    bridge_stream_entry_t *e = bridge_find_by_id(b, stream_id);

    if (e && e->uni_disp == BRIDGE_UNI_DISP_DISCARD) {
        /* See on_peer_uni_bytes: session dropped this stream; swallow until
         * FIN rather than misparse replayed bytes as a fresh stream. */
        if (fin) bridge_deactivate_stream(e);
        return MOQ_OK;
    }

    if (e && e->pending_retry) {
        /* See on_peer_uni_bytes: append additional bytes to the session's
         * retained input; the transport does not re-deliver, and a pending_retry
         * stream is always a data stream. If the session has since dropped the
         * stream, discard the rest instead of reopening a fresh rx entry. */
        if (!moq_session_has_transport_stream(b->session, e->ref)) {
            e->pending_retry = false;
            e->pending_fin = false;
            e->fin_retained = false;
            e->uni_disp = BRIDGE_UNI_DISP_DISCARD;
            if (fin) bridge_deactivate_stream(e);
            return MOQ_OK;
        }
        moq_result_t rc = moq_session_on_data_rcbuf(
            b->session, e->ref, data, fin, now_us);
        if (rc == MOQ_ERR_WOULD_BLOCK) {
            if (fin) e->fin_retained = true;
            return MOQ_ERR_WOULD_BLOCK;
        }
        if (rc < 0) { bridge_set_fatal(b, 0x1); return rc; }
        e->pending_retry = false;
        e->pending_fin = false;
        e->fin_retained = false;
        if (fin) { bridge_deactivate_stream(e); return MOQ_OK; }
        bridge_discard_if_dropped(b, e);
        return MOQ_OK;
    }

    /* Uni-control-pair profiles need to classify the stream by its leading
     * bytes; route through the byte path (zero-copy for these streams is a
     * later refinement). The default bidi-control mode keeps the rcbuf path. */
    if (b->control_mode == BRIDGE_CONTROL_UNI_PAIR) {
        const uint8_t *bytes = data ? moq_rcbuf_data(data) : NULL;
        size_t blen = data ? moq_rcbuf_len(data) : 0;
        return bridge_uni_pair_inbound(b, stream_id, bytes, blen, fin, now_us);
    }

    size_t data_len = data ? moq_rcbuf_len(data) : 0;
    if (!e && data_len == 0 && !fin) return MOQ_OK;

    moq_stream_ref_t ref;
    if (!e) {
        ref = bridge_assign_inbound_ref(b, stream_id, BRIDGE_STREAM_UNI);
        if (ref._v == 0) return MOQ_ERR_INTERNAL;
        e = bridge_find_by_id(b, stream_id);
    } else {
        ref = e->ref;
    }

    moq_result_t rc = moq_session_on_data_rcbuf(
        b->session, ref, data, fin, now_us);

    if (rc == MOQ_ERR_WOULD_BLOCK) {
        if (moq_session_has_transport_stream(b->session, ref)) {
            /* Legitimate inbound backpressure: the session retained the bytes. */
            e->pending_retry = true;
            if (fin) e->fin_retained = true;
            return MOQ_ERR_WOULD_BLOCK;
        }
        /* Refused before registering a transport stream; discard the remainder
         * until FIN rather than misparse later bytes. Not fatal. */
        e->uni_disp = BRIDGE_UNI_DISP_DISCARD;
        if (fin) bridge_deactivate_stream(e);
        return MOQ_OK;
    }

    if (rc < 0) {
        bridge_set_fatal(b, 0x1);
        return rc;
    }

    if (fin) {
        bridge_deactivate_stream(e);
        return MOQ_OK;
    }

    bridge_discard_if_dropped(b, e);
    return MOQ_OK;
}

moq_result_t moq_transport_bridge_on_peer_bidi_bytes(
    moq_transport_bridge_t *b, uint64_t stream_id,
    const uint8_t *data, size_t len, bool fin, uint64_t now_us)
{
    if (!b || b->fatal || b->closed) return MOQ_ERR_CLOSED;

    if (bridge_is_tombstoned(b, stream_id)) {
        if (len > 0) {
            bridge_set_fatal(b, 0x3);
            return MOQ_ERR_PROTO;
        }
        if (fin) bridge_remove_tombstone(b, stream_id);
        return MOQ_OK;
    }

    bridge_stream_entry_t *e = bridge_find_by_id(b, stream_id);

    if (e && e->pending_retry) {
        if (len > 0) {
            bridge_set_fatal(b, 0x3);
            return MOQ_ERR_PROTO;
        }
        if (fin) e->pending_fin = true;
        return MOQ_ERR_WOULD_BLOCK;
    }

    if (!e && len == 0 && !fin) return MOQ_OK;

    moq_stream_ref_t ref;
    if (!e) {
        ref = bridge_assign_inbound_ref(b, stream_id, BRIDGE_STREAM_BIDI);
        if (ref._v == 0) return MOQ_ERR_INTERNAL;
        e = bridge_find_by_id(b, stream_id);
    } else {
        ref = e->ref;
    }

    moq_result_t rc = moq_session_on_bidi_stream_bytes(
        b->session, ref, data, len, fin, now_us);

    if (rc == MOQ_ERR_WOULD_BLOCK) {
        if (!moq_session_has_transport_stream(b->session, ref)) {
            bridge_set_fatal(b, 0x1);
            return MOQ_ERR_INTERNAL;
        }
        e->pending_retry = true;
        if (fin) e->fin_retained = true;
        return MOQ_ERR_WOULD_BLOCK;
    }

    if (rc < 0) {
        bridge_set_fatal(b, 0x1);
        return rc;
    }

    if (fin) {
        e->peer_send_closed = true;
        bridge_retire_or_tombstone(b, ref);
    }

    return MOQ_OK;
}

moq_result_t moq_transport_bridge_on_peer_stream_reset(
    moq_transport_bridge_t *b, uint64_t stream_id,
    uint64_t error_code, uint64_t now_us)
{
    if (!b || b->fatal || b->closed) return MOQ_ERR_CLOSED;

    if (bridge_is_tombstoned(b, stream_id)) {
        bridge_remove_tombstone(b, stream_id);
        return MOQ_OK;
    }

    bridge_stream_entry_t *e = bridge_find_by_id(b, stream_id);
    if (!e) return MOQ_OK;

    /* Uni-control-pair mode: the control channel lives for the session, so
     * a peer RESET of its unidirectional control stream terminates the
     * session -- it must not be swallowed as an unknown data-stream reset.
     * Mirrors the control-channel FIN handling in bridge_feed_peer_control. */
    if (e->kind == BRIDGE_STREAM_UNI &&
        e->uni_disp == BRIDGE_UNI_DISP_CONTROL) {
        moq_session_on_transport_close(b->session, 0x3, now_us);
        b->needs_close = true;
        b->needs_close_code = 0x3;
        return MOQ_OK;
    }

    moq_result_t rc;
    if (e->kind == BRIDGE_STREAM_BIDI)
        rc = moq_session_on_bidi_stream_reset(
            b->session, e->ref, error_code, now_us);
    else
        rc = moq_session_on_data_reset(
            b->session, e->ref, error_code, now_us);

    if (rc == MOQ_ERR_WOULD_BLOCK) {
        e->pending_reset = true;
        e->pending_reset_code = error_code;
        return MOQ_ERR_WOULD_BLOCK;
    }

    if (rc < 0) {
        bridge_set_fatal(b, 0x1);
        return rc;
    }

    bridge_deactivate_stream(e);
    return MOQ_OK;
}

moq_result_t moq_transport_bridge_on_peer_stop_sending(
    moq_transport_bridge_t *b, uint64_t stream_id,
    uint64_t error_code, uint64_t now_us)
{
    if (!b || b->fatal || b->closed) return MOQ_ERR_CLOSED;

    bridge_stream_entry_t *e = bridge_find_by_id(b, stream_id);
    if (!e) return MOQ_OK;

    /* Uni-control-pair mode: the local-origin control stream carries our control
     * output for the session's lifetime, so a peer STOP_SENDING of it refuses
     * the control channel and is a fatal teardown -- it must not be swallowed by
     * the generic local-origin uni data-stop path (moq_session_on_data_stop
     * returns MOQ_OK for a non-subgroup ref, leaving the session established).
     * Mirrors the peer-control RESET handling in on_peer_stream_reset. */
    if (e->kind == BRIDGE_STREAM_UNI &&
        e->uni_disp == BRIDGE_UNI_DISP_CONTROL) {
        moq_session_on_transport_close(b->session, 0x3, now_us);
        b->needs_close = true;
        b->needs_close_code = 0x3;
        return MOQ_OK;
    }

    /* STOP_SENDING asks us to stop sending on the targeted stream's local send
     * half. A bidi has a local send half regardless of which peer opened it
     * (request bidis carry our requests/responses); a uni has one only when we
     * opened it. A peer STOP_SENDING on a bidi is the D18 request cancellation
     * signal and is delivered through the dedicated bidi-stop input, which is
     * distinct from a RESET_STREAM and MUST NOT route through reset. */
    moq_result_t rc;
    if (e->kind == BRIDGE_STREAM_BIDI) {
        rc = moq_session_on_bidi_stream_stop(
            b->session, e->ref, error_code, now_us);
    } else if (e->origin == BRIDGE_ORIGIN_LOCAL) {
        rc = moq_session_on_data_stop(
            b->session, e->ref, error_code, now_us);
    } else {
        /* Peer-origin uni: we have no send half to stop. */
        return MOQ_OK;
    }

    if (rc == MOQ_ERR_WOULD_BLOCK) {
        e->pending_stop = true;
        e->pending_stop_code = error_code;
        return MOQ_ERR_WOULD_BLOCK;
    }

    if (rc < 0) {
        bridge_set_fatal(b, 0x1);
        return rc;
    }

    return MOQ_OK;
}

moq_result_t moq_transport_bridge_on_peer_datagram(
    moq_transport_bridge_t *b,
    const uint8_t *data, size_t len, uint64_t now_us)
{
    if (!b || b->fatal || b->closed) return MOQ_ERR_CLOSED;

    moq_result_t rc = moq_session_on_datagram(
        b->session, data, len, now_us);

    /* Datagrams are lossy — WOULD_BLOCK means silently dropped */
    if (rc == MOQ_ERR_WOULD_BLOCK) return MOQ_OK;

    if (rc < 0) {
        bridge_set_fatal(b, 0x1);
        return rc;
    }

    return MOQ_OK;
}

moq_result_t moq_transport_bridge_on_transport_close(
    moq_transport_bridge_t *b, uint64_t code, uint64_t now_us)
{
    if (!b) return MOQ_ERR_INVAL;
    if (b->fatal || b->closed) return MOQ_OK;
    b->closed = true;
    b->close_code = code;
    bridge_clear_all_state(b);
    moq_session_on_transport_close(b->session, code, now_us);
    return MOQ_OK;
}

moq_result_t moq_transport_bridge_on_transport_error(
    moq_transport_bridge_t *b, uint64_t code, uint64_t now_us)
{
    if (!b) return MOQ_ERR_INVAL;
    if (b->fatal || b->closed) return MOQ_OK;
    bridge_set_fatal(b, code);
    moq_session_on_transport_close(b->session, code, now_us);
    return MOQ_OK;
}
