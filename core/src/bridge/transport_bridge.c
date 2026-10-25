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

#ifdef MOQ_SESSION_SWEEP_TESTING
uint64_t bridge_work_expiry_slots;
uint64_t bridge_work_expiry_items;
uint64_t bridge_work_reset_slots;
uint64_t bridge_work_query_slots;
uint64_t bridge_work_terminal_slots;
#endif

static void bridge_discard_if_dropped(moq_transport_bridge_t *b,
                                      bridge_stream_entry_t *e);

/* -- Config --------------------------------------------------------- */

void moq_transport_bridge_cfg_init(moq_transport_bridge_cfg_t *cfg,
                                    const moq_alloc_t *alloc)
{
    if (!cfg) return;
    memset(cfg, 0, MOQ_TRANSPORT_BRIDGE_CFG_V0_SIZE);
    cfg->struct_size = (uint32_t)MOQ_TRANSPORT_BRIDGE_CFG_V0_SIZE;
    cfg->alloc = alloc;
}

void moq_transport_bridge_cfg_init_sized(moq_transport_bridge_cfg_t *cfg,
                                         size_t cfg_size,
                                         const moq_alloc_t *alloc)
{
    if (!cfg) return;
    size_t n = cfg_size < sizeof(*cfg) ? cfg_size : sizeof(*cfg);
    if (n < MOQ_TRANSPORT_BRIDGE_CFG_V0_SIZE) return;
    memset(cfg, 0, n);
    cfg->struct_size = (uint32_t)n;
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

static bool bridge_array_fits(size_t count, size_t element_size)
{
    return count <= SIZE_MAX / element_size;
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

/* -- Dstream index -------------------------------------------------- */

static uint32_t bridge_index_home(uint64_t id, uint32_t mask)
{
    return (uint32_t)((id * 0x9E3779B97F4A7C15ULL) >> 32) & mask;
}

static uint32_t bridge_index_find(const bridge_index_entry_t *t, uint32_t mask,
                                  uint64_t id)
{
    for (uint32_t i = bridge_index_home(id, mask);; i = (i + 1) & mask) {
        if (t[i].slot == BRIDGE_INDEX_EMPTY) return BRIDGE_INDEX_EMPTY;
        if (t[i].id == id) return t[i].slot;
    }
}

static void bridge_index_insert(bridge_index_entry_t *t, uint32_t mask,
                                uint64_t id, uint32_t slot)
{
    uint32_t i = bridge_index_home(id, mask);
    while (t[i].slot != BRIDGE_INDEX_EMPTY) i = (i + 1) & mask;
    t[i].id = id;
    t[i].slot = slot;
}

/* Later entries of the probe run move back into the gap when their home
 * allows it, so lookups never stop early. */
static void bridge_index_remove(bridge_index_entry_t *t, uint32_t mask,
                                uint64_t id)
{
    uint32_t i = bridge_index_home(id, mask);
    for (;; i = (i + 1) & mask) {
        if (t[i].slot == BRIDGE_INDEX_EMPTY) return;
        if (t[i].id == id) break;
    }
    for (uint32_t j = (i + 1) & mask; t[j].slot != BRIDGE_INDEX_EMPTY;
         j = (j + 1) & mask) {
        uint32_t home = bridge_index_home(t[j].id, mask);
        if (((j - home) & mask) >= ((j - i) & mask)) {
            t[i] = t[j];
            i = j;
        }
    }
    t[i].slot = BRIDGE_INDEX_EMPTY;
}

static void bridge_dindex_free(moq_transport_bridge_t *b)
{
    size_t bytes = ((size_t)b->dindex_mask + 1) * sizeof(bridge_index_entry_t);
    if (b->dref_index) bridge_free(&b->alloc, b->dref_index, bytes);
    if (b->dsid_index) bridge_free(&b->alloc, b->dsid_index, bytes);
    if (b->downer_index) bridge_free(&b->alloc, b->downer_index, bytes);
    b->dref_index = b->dsid_index = b->downer_index = NULL;
}

/* Rebuild the indexes from the current table, sized for `cap` dstreams at
 * most half full. False on allocation failure, leaving them untouched. */
static bool bridge_dindex_build(moq_transport_bridge_t *b, uint32_t cap)
{
    uint64_t n = 1;
    while (n < 2 * (uint64_t)cap) n <<= 1;
    if (n > (uint64_t)1 << 31 ||
        n > SIZE_MAX / sizeof(bridge_index_entry_t)) return false;
    size_t bytes = (size_t)n * sizeof(bridge_index_entry_t);
    bridge_index_entry_t *r = bridge_alloc(&b->alloc, bytes);
    bridge_index_entry_t *t = bridge_alloc(&b->alloc, bytes);
    bridge_index_entry_t *o = bridge_alloc(&b->alloc, bytes);
    if (!r || !t || !o) {
        if (r) bridge_free(&b->alloc, r, bytes);
        if (t) bridge_free(&b->alloc, t, bytes);
        if (o) bridge_free(&b->alloc, o, bytes);
        return false;
    }
    memset(r, 0xff, bytes);
    memset(t, 0xff, bytes);
    memset(o, 0xff, bytes);
    bridge_dindex_free(b);
    b->dref_index = r;
    b->dsid_index = t;
    b->downer_index = o;
    b->dindex_mask = (uint32_t)(n - 1);
    for (uint32_t i = 0; i < b->dstream_cap; i++) {
        const bridge_dstream_t *d = &b->dstreams[i];
        if (!d->active) continue;
        bridge_index_insert(r, b->dindex_mask, d->ref._v, i);
        if (d->has_sid) bridge_index_insert(t, b->dindex_mask, d->sid, i);
        if (d->owner_prev == BRIDGE_INDEX_EMPTY)
            bridge_index_insert(o, b->dindex_mask, d->key.owner, i);
    }
    return true;
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
    if (cfg->struct_size < MOQ_TRANSPORT_BRIDGE_CFG_V0_SIZE)
        return MOQ_ERR_INVAL;
    if (!cfg->alloc || !cfg->alloc->alloc || !cfg->alloc->free ||
        !cfg->alloc->realloc)
        return MOQ_ERR_INVAL;
    if (!ops_valid(ops)) return MOQ_ERR_INVAL;
    if (!(ops->capabilities & MOQ_TRANSPORT_CAP_HOLD_INPUT))
        return MOQ_ERR_UNSUPPORTED;

    const moq_alloc_t *a = cfg->alloc;
    uint32_t max_s = cfg->max_streams    ? cfg->max_streams    : BRIDGE_DEFAULT_MAX_STREAMS;
    uint32_t max_p = cfg->max_pending    ? cfg->max_pending    : BRIDGE_DEFAULT_MAX_PENDING;
    uint32_t max_t = cfg->max_tombstones ? cfg->max_tombstones : BRIDGE_DEFAULT_MAX_TOMBSTONES;

    /* A unidirectional control pair (drafts 18 and 22) keeps one stream-map
     * entry per control stream for the whole session, so a map of fewer than
     * two entries could never complete setup. Refused before anything is
     * allocated or any endpoint call is made. */
    if (max_s < 2 && moq_session_uses_uni_control(session))
        return MOQ_ERR_INVAL;
    if (!bridge_array_fits(max_s, sizeof(bridge_stream_entry_t)) ||
        !bridge_array_fits(max_s, sizeof(bridge_dstream_t)) ||
        !bridge_array_fits(max_s, sizeof(moq_stream_order_node_t)) ||
        !bridge_array_fits(max_p, sizeof(bridge_pending_item_t)) ||
        !bridge_array_fits(max_p, sizeof(bridge_hold_t)) ||
        !bridge_array_fits(max_t, sizeof(uint64_t)))
        return MOQ_ERR_NOMEM;

    moq_result_t rc = MOQ_ERR_NOMEM;
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

    b->dstreams = bridge_alloc(a, max_s * sizeof(bridge_dstream_t));
    if (!b->dstreams) goto fail;
    memset(b->dstreams, 0, max_s * sizeof(bridge_dstream_t));
    b->dstream_cap = max_s;
    b->pass_seq = 1;
    for (uint32_t i = 0; i < max_s; i++) b->dstreams[i].next_free = i + 1;
    if (!bridge_dindex_build(b, max_s)) goto fail;
    for (int h = 0; h < 2; h++) {
        b->dheap[h] = bridge_alloc(a, max_s * sizeof(uint32_t));
        if (!b->dheap[h]) goto fail;
    }
    b->dskip = bridge_alloc(a, max_s * sizeof(uint32_t));
    if (!b->dskip) goto fail;
    b->order_nodes = bridge_alloc(a, max_s * sizeof(*b->order_nodes));
    if (!b->order_nodes) goto fail;
    b->order_slots = bridge_alloc(a, max_s * sizeof(*b->order_slots));
    if (!b->order_slots) goto fail;
    b->max_retained_bytes = MOQ_TRANSPORT_BRIDGE_RETAINED_DEFAULT;
    if (cfg->struct_size >= offsetof(moq_transport_bridge_cfg_t,
                                     max_retained_bytes) +
                            sizeof(cfg->max_retained_bytes) &&
        cfg->max_retained_bytes)
        b->max_retained_bytes = cfg->max_retained_bytes;

    /* Held-stream records, sized from max_pending but separate from the
     * outbound pending queue. */
    if (ops->capabilities & MOQ_TRANSPORT_CAP_HOLD_BIDI_INPUT) {
        b->holds = bridge_alloc(a, max_p * sizeof(bridge_hold_t));
        if (!b->holds) goto fail;
        memset(b->holds, 0, max_p * sizeof(bridge_hold_t));
        b->max_holds = max_p;
    }

    /* One transport owner per session, registered after every fallible
     * allocation: a refused registration frees only this bridge's storage
     * and leaves the session and its existing owner untouched. */
    if (moq_session_register_transport_owner(session, b) != MOQ_OK) {
        rc = MOQ_ERR_WRONG_STATE;
        goto fail;
    }
    /* Every data stream is keyed from here on. A single-entry action queue
     * cannot hold a key beside its SEND_DATA: those streams go out with
     * each SEND_DATA's own priority, without the group order. */
    (void)moq_session_set_data_priority_updates(session, true);

    *out = b;
    return MOQ_OK;

fail:
    if (b->holds)      bridge_free(a, b->holds, max_p * sizeof(bridge_hold_t));
    bridge_dindex_free(b);
    for (int h = 0; h < 2; h++)
        if (b->dheap[h]) bridge_free(a, b->dheap[h], max_s * sizeof(uint32_t));
    if (b->dskip) bridge_free(a, b->dskip, max_s * sizeof(uint32_t));
    if (b->order_nodes) bridge_free(a, b->order_nodes, max_s * sizeof(*b->order_nodes));
    if (b->order_slots) bridge_free(a, b->order_slots, max_s * sizeof(*b->order_slots));
    if (b->dstreams)   bridge_free(a, b->dstreams, max_s * sizeof(bridge_dstream_t));
    if (b->tombstones) bridge_free(a, b->tombstones, max_t * sizeof(uint64_t));
    if (b->pending)    bridge_free(a, b->pending, max_p * sizeof(bridge_pending_item_t));
    if (b->streams)    bridge_free(a, b->streams, max_s * sizeof(bridge_stream_entry_t));
    bridge_free(a, b, sizeof(*b));
    return rc;
}

static void bridge_dstreams_clear(moq_transport_bridge_t *b);

void moq_transport_bridge_destroy(moq_transport_bridge_t *b)
{
    if (!b) return;

    /* The session outlives its bridge (see moq_transport_bridge_create), so
     * releasing the exclusive registration here is a valid access. */
    moq_session_unregister_transport_owner(b->session, b);
    (void)moq_session_set_data_priority_updates(b->session, false);
    bridge_cleanup_all_pending(b);
    bridge_dstreams_clear(b);

    const moq_alloc_t *a = &b->alloc;
    if (b->holds) bridge_free(a, b->holds, b->max_holds * sizeof(bridge_hold_t));
    bridge_dindex_free(b);
    for (int h = 0; h < 2; h++)
        bridge_free(a, b->dheap[h], b->dstream_cap * sizeof(uint32_t));
    bridge_free(a, b->dskip, b->dstream_cap * sizeof(uint32_t));
    bridge_free(a, b->order_nodes, b->dstream_cap * sizeof(*b->order_nodes));
    bridge_free(a, b->order_slots, b->dstream_cap * sizeof(*b->order_slots));
    bridge_free(a, b->dstreams, b->dstream_cap * sizeof(bridge_dstream_t));
    bridge_free(a, b->tombstones, b->max_tombstones * sizeof(uint64_t));
    bridge_free(a, b->pending, b->max_pending * sizeof(bridge_pending_item_t));
    bridge_free(a, b->streams, b->max_streams * sizeof(bridge_stream_entry_t));
    bridge_free(a, b, sizeof(*b));
}

/* -- Stream map ----------------------------------------------------- */

static bool bridge_stream_map_full(const moq_transport_bridge_t *b)
{
    return b->streams_active == b->max_streams;
}

bridge_stream_entry_t *bridge_alloc_stream(moq_transport_bridge_t *b)
{
    for (uint32_t i = 0; i < b->max_streams; i++) {
        if (!b->streams[i].active) {
            memset(&b->streams[i], 0, sizeof(bridge_stream_entry_t));
            b->streams[i].active = true;
            b->streams_active++;
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

const bridge_stream_entry_t *bridge_find_by_id_const(
    const moq_transport_bridge_t *b, uint64_t transport_id)
{
    for (uint32_t i = 0; i < b->max_streams; i++) {
        if (b->streams[i].active && b->streams[i].transport_id == transport_id)
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

void bridge_deactivate_stream(moq_transport_bridge_t *b,
                              bridge_stream_entry_t *e)
{
    b->streams_active -= e->active;
    memset(e, 0, sizeof(*e));
}

static void bridge_note_aborting_terminal(moq_transport_bridge_t *b,
                                           bridge_stream_entry_t *e)
{
    if (e && e->active && e->kind == BRIDGE_STREAM_BIDI && e->aborting) {
        e->transport_final = true;
        b->local_retire_due = true;
    }
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
    /* An abort's drain identity and mapping end together in service, never
     * while an older pending close/reset still borrows this entry. */
    if (e->aborting) return;

    if (e->kind == BRIDGE_STREAM_UNI) {
        bridge_deactivate_stream(b, e);
        return;
    }

    if (e->peer_send_closed && e->local_send_closed) {
        bridge_deactivate_stream(b, e);
        return;
    }

    if (e->local_send_closed && !e->peer_send_closed) {
        /* A bidi we opened whose send half a peer STOP_SENDING closed: the
         * peer's half still carries the response, so the mapping must survive
         * until its own terminal. moq_session_has_transport_stream() also
         * recognises a live draft-18 request owner, but that is the wrong
         * question here: a live local-origin request owner does not by itself
         * mean its peer terminal is still owed. Retirement therefore keys on the
         * STOP fact (peer_stop_received) and the narrower
         * moq_session_stream_awaits_peer_terminal() predicate. An ordinary FIN
         * keeps the existing behaviour. */
        if (e->kind == BRIDGE_STREAM_BIDI &&
            e->origin == BRIDGE_ORIGIN_LOCAL &&
            e->peer_stop_received) {
            return;
        }
        if (moq_session_stream_awaits_peer_terminal(b->session, e->ref)) {
            return;
        }
        uint64_t tid = e->transport_id;
        bridge_deactivate_stream(b, e);

        if (b->tombstone_count >= b->max_tombstones) {
            for (uint32_t i = 1; i < b->max_tombstones; i++)
                b->tombstones[i - 1] = b->tombstones[i];
            b->tombstone_count--;
        }
        b->tombstones[b->tombstone_count++] = tid;
    }
}

/* A pending item that resets (or aborts) this stream, named by its
 * transport id: the reset is owed whatever happens to the mapping. */
static bool bridge_pending_resets_stream(const moq_transport_bridge_t *b,
                                         const bridge_stream_entry_t *e)
{
    for (uint32_t i = 0; i < b->pending_count; i++) {
        const bridge_pending_item_t *p = &b->pending[i];
        if ((p->kind == PENDING_RESET_STREAM || p->kind == PENDING_ABORT_STREAM) &&
            p->has_stream_id && p->stream_id == e->transport_id)
            return true;
    }
    return false;
}

/* Any pending item for this stream, by ref or transport id: an existing
 * output or close obligation that keeps its own order. */
static bool bridge_pending_touches_stream(const moq_transport_bridge_t *b,
                                          const bridge_stream_entry_t *e)
{
    for (uint32_t i = 0; i < b->pending_count; i++) {
        const bridge_pending_item_t *p = &b->pending[i];
        if ((p->stream_ref._v != 0 && p->stream_ref._v == e->ref._v) ||
            (p->has_stream_id && p->stream_id == e->transport_id) ||
            (p->canon_ref != 0 && p->canon_ref == e->ref._v) ||
            (p->canon_have_sid && p->canon_sid == e->transport_id))
            return true;
    }
    return false;
}

/* The session accepted the peer's RESET of this stream. A uni (receive-only
 * here) is over. A bidi's RESET ends only the PEER's sending direction: our
 * own send half is independent (RFC 9000 §3.5). If it is still open, the
 * mapping stays for it: output or a close the session already queued keeps
 * its order, and once the request has no owner left the service pass cancels
 * the half (bridge_settle_bidi_cancel). Nothing more is read from the peer's
 * direction, so its inbound retry state is dropped. */
static void bridge_peer_reset_accepted(moq_transport_bridge_t *b,
                                       bridge_stream_entry_t *e)
{
    if (e->kind != BRIDGE_STREAM_BIDI) {
        bridge_deactivate_stream(b, e);
        return;
    }
    e->peer_send_closed = true;
    e->peer_reset_accepted = true;
    e->pending_retry = false;
    e->pending_fin = false;
    e->fin_retained = false;
    /* Our half's reset is already owed and retained by transport id: that
     * obligation outlives the mapping, which retires now. */
    if (!e->local_send_closed && bridge_pending_resets_stream(b, e)) {
        bridge_deactivate_stream(b, e);
        return;
    }
    bridge_retire_or_tombstone(b, e->ref);
    if (e->active && !e->local_send_closed) b->bidi_cancel_due = true;
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

static size_t bridge_send_data_bytes(const moq_send_data_action_t *sd)
{
    return sd->header_len + (sd->payload ? moq_rcbuf_len(sd->payload) : 0);
}

/* -- Ready heaps: binary min-heaps of slots by (hkey, served, slot). -- */

static bool bridge_dheap_less(const moq_transport_bridge_t *b, uint32_t x,
                              uint32_t y)
{
    const bridge_dstream_t *a = &b->dstreams[x], *c = &b->dstreams[y];
    if ((a->hkey >> 16) != (c->hkey >> 16))
        return (a->hkey >> 16) < (c->hkey >> 16);
    /* Preserve the full logical rank here even when the endpoint's 16-bit
     * position field has saturated. Transport quantization is separate. */
    if (a->pos != c->pos) return a->pos < c->pos;
    if (a->served != c->served) return a->served < c->served;
    return x < y;
}

static void bridge_dheap_put(moq_transport_bridge_t *b, uint8_t h, uint32_t i,
                             uint32_t slot)
{
    b->dheap[h][i] = slot;
    b->dstreams[slot].heap_at = i;
}

static void bridge_dheap_up(moq_transport_bridge_t *b, uint8_t h, uint32_t i)
{
    uint32_t slot = b->dheap[h][i];
    while (i > 0) {
        uint32_t parent = (i - 1) / 2;
        if (!bridge_dheap_less(b, slot, b->dheap[h][parent])) break;
        bridge_dheap_put(b, h, i, b->dheap[h][parent]);
        i = parent;
    }
    bridge_dheap_put(b, h, i, slot);
}

static void bridge_dheap_down(moq_transport_bridge_t *b, uint8_t h, uint32_t i)
{
    uint32_t n = b->dheap_len[h], slot = b->dheap[h][i];
    for (;;) {
        uint32_t c = 2 * i + 1;
        if (c >= n) break;
        if (c + 1 < n && bridge_dheap_less(b, b->dheap[h][c + 1], b->dheap[h][c]))
            c++;
        if (!bridge_dheap_less(b, b->dheap[h][c], slot)) break;
        bridge_dheap_put(b, h, i, b->dheap[h][c]);
        i = c;
    }
    bridge_dheap_put(b, h, i, slot);
}

static void bridge_dheap_remove(moq_transport_bridge_t *b, bridge_dstream_t *d)
{
    uint8_t h = d->heap_id;
    uint32_t i = d->heap_at, last = b->dheap[h][--b->dheap_len[h]];
    d->heap_at = BRIDGE_INDEX_EMPTY;
    if (i == b->dheap_len[h]) return;
    bridge_dheap_put(b, h, i, last);
    bridge_dheap_up(b, h, i);
    bridge_dheap_down(b, h, b->dstreams[last].heap_at);
}

/* -- Owner lists: every stream of one owner, for its order and its
 * owner-wide updates. */

static uint32_t bridge_owner_first(const moq_transport_bridge_t *b,
                                   uint64_t owner)
{
    return bridge_index_find(b->downer_index, b->dindex_mask, owner);
}

static void bridge_owner_link(moq_transport_bridge_t *b, bridge_dstream_t *d)
{
    uint32_t slot = (uint32_t)(d - b->dstreams);
    uint32_t first = bridge_owner_first(b, d->key.owner);
    d->owner_prev = BRIDGE_INDEX_EMPTY;
    d->owner_next = first;
    if (first != BRIDGE_INDEX_EMPTY) {
        b->dstreams[first].owner_prev = slot;
        bridge_index_remove(b->downer_index, b->dindex_mask, d->key.owner);
    }
    bridge_index_insert(b->downer_index, b->dindex_mask, d->key.owner, slot);
}

static void bridge_owner_unlink(moq_transport_bridge_t *b, bridge_dstream_t *d)
{
    if (d->owner_prev != BRIDGE_INDEX_EMPTY) {
        b->dstreams[d->owner_prev].owner_next = d->owner_next;
    } else {
        bridge_index_remove(b->downer_index, b->dindex_mask, d->key.owner);
        if (d->owner_next != BRIDGE_INDEX_EMPTY)
            bridge_index_insert(b->downer_index, b->dindex_mask, d->key.owner,
                                d->owner_next);
    }
    if (d->owner_next != BRIDGE_INDEX_EMPTY)
        b->dstreams[d->owner_next].owner_prev = d->owner_prev;
    d->owner_prev = d->owner_next = BRIDGE_INDEX_EMPTY;
}

/* Drop `d`'s queued actions and release its queue. */
static void bridge_dstream_release(moq_transport_bridge_t *b,
                                   bridge_dstream_t *d)
{
    if (d->heap_at != BRIDGE_INDEX_EMPTY) bridge_dheap_remove(b, d);
    if (d->owner_next != BRIDGE_INDEX_EMPTY)
        b->dstreams[d->owner_next].order_dirty = true;
    if (d->owner_prev != BRIDGE_INDEX_EMPTY)
        b->dstreams[d->owner_prev].order_dirty = true;
    b->order_dirty = true;
    bridge_owner_unlink(b, d);
    for (; d->q_len > 0; d->q_len--) {
        moq_action_t *a = &d->q[d->q_head].action;
        b->dq_bytes -= bridge_send_data_bytes(&a->u.send_data);
        b->dq_items--;
        if (d->q[d->q_head].meta.deadline_us != UINT64_MAX) b->dq_timed--;
        moq_action_cleanup(a);
        d->q_head = (d->q_head + 1) % d->q_cap;
    }
    if (d->q) bridge_free(&b->alloc, d->q, d->q_cap * sizeof(*d->q));
    bridge_index_remove(b->dref_index, b->dindex_mask, d->ref._v);
    if (d->has_sid)
        bridge_index_remove(b->dsid_index, b->dindex_mask, d->sid);
    uint32_t slot = (uint32_t)(d - b->dstreams);
    if (d->has_reset) b->dreset_count--;
    memset(d, 0, sizeof(*d));
    d->next_free = b->dstream_free;
    b->dstream_free = slot;
}

static void bridge_dstreams_clear(moq_transport_bridge_t *b)
{
    for (uint32_t i = 0; i < b->dstream_cap; i++)
        if (b->dstreams[i].active)
            bridge_dstream_release(b, &b->dstreams[i]);
}

static void bridge_clear_all_state(moq_transport_bridge_t *b)
{
    bridge_cleanup_all_pending(b);
    b->pending_control = false;
    b->pending_control_fin = false;
    for (uint32_t i = 0; i < b->max_streams; i++) {
        if (b->streams[i].active)
            bridge_deactivate_stream(b, &b->streams[i]);
    }
    bridge_dstreams_clear(b);
    b->tombstone_count = 0;
    if (b->holds) {
        memset(b->holds, 0, b->max_holds * sizeof(bridge_hold_t));
        moq_session_set_owner_deadline(b->session, b, false, 0);
    }
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

/* One policy for every failed retention; defined with the queue below. */
static moq_result_t bridge_retention_failed(moq_transport_bridge_t *b,
                                            bridge_pending_item_t *p);

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

static bridge_dstream_t *bridge_dstream_find(moq_transport_bridge_t *b,
                                             moq_stream_ref_t ref);
static void bridge_dstream_requeue(moq_transport_bridge_t *b,
                                   bridge_dstream_t *d);
static bridge_dstream_t *bridge_pump_pick(const moq_transport_bridge_t *b);
static moq_result_t bridge_pump_data(moq_transport_bridge_t *b,
                                     bool *progress);

/* Keep the open count, and each data stream's count of retained writes, as
 * `p` enters (+1) or leaves (-1). */
static void bridge_pending_counts(moq_transport_bridge_t *b,
                                  const bridge_pending_item_t *p, int delta)
{
    if (p->kind == PENDING_OPEN_UNI_DATA)
        b->pending_opens += (uint32_t)delta;
    else if (p->kind != PENDING_HEADER_PAYLOAD && p->kind != PENDING_PAYLOAD_ONLY)
        return;
    bridge_dstream_t *d = bridge_dstream_find(b, p->stream_ref);
    if (!d) return;
    d->retained += (uint32_t)delta;
    if (d->retained == 0 || (delta > 0 && d->retained == 1))
        bridge_dstream_requeue(b, d);
}

void bridge_cleanup_all_pending(moq_transport_bridge_t *b)
{
    for (uint32_t i = 0; i < b->pending_count; i++) {
        bridge_pending_counts(b, &b->pending[i], -1);
        bridge_cleanup_pending_item(&b->alloc, &b->pending[i]);
    }
    b->pending_count = 0;
}

static bool bridge_enqueue_pending(moq_transport_bridge_t *b,
                                    const bridge_pending_item_t *item);

/* Record a KNOWN transport stream id on a pending item. Id 0 is legal, so the
 * knowledge is a separate bit and never inferred from the value. */
static inline void bridge_item_set_sid(bridge_pending_item_t *p, uint64_t sid)
{
    p->stream_id = sid;
    p->has_stream_id = true;
}

/* Ordering-domain predicate; defined with the domain helpers below. */
static bool bridge_domain_blocked(moq_transport_bridge_t *b,
                                   moq_stream_ref_t ref, uint64_t sid,
                                   bool have_sid);

typedef enum {
    BRIDGE_DOM_STREAM,
    BRIDGE_DOM_CONN_CTRL,
    BRIDGE_DOM_CONN_TERM,
} bridge_domain_t;

static bridge_domain_t bridge_pending_domain(const bridge_pending_item_t *p)
{
    switch (p->kind) {
    case PENDING_OPEN_UNI_DATA:
    case PENDING_HEADER_PAYLOAD:
    case PENDING_PAYLOAD_ONLY:
    case PENDING_OPEN_BIDI_DATA:
    case PENDING_CLOSE_BIDI_FIN:
    case PENDING_RESET_STREAM:
    case PENDING_STOP_SENDING:
    case PENDING_ABORT_STREAM:
        return BRIDGE_DOM_STREAM;
    case PENDING_COPIED_WRITE:
        /* Data writes carry a ref; the control-channel write does not. */
        return p->stream_ref._v != 0 ? BRIDGE_DOM_STREAM
                                     : BRIDGE_DOM_CONN_CTRL;
    case PENDING_CLOSE_TRANSPORT:
        return BRIDGE_DOM_CONN_TERM;
    case PENDING_OPEN_CONTROL:
    case PENDING_OPEN_UNI_CONTROL:
    case PENDING_COPIED_DATAGRAM:
    default:
        return BRIDGE_DOM_CONN_CTRL;
    }
}


/* Queued output that a STOP_SENDING on this stream has made obsolete. Items
 * carrying the stream's ref are matched by ref; the control-channel writes
 * that carry none are matched by transport id only when they name this stream,
 * so an unrelated item can never be selected. */
static bool bridge_pending_is_bidi_output(const bridge_pending_item_t *p,
                                          const bridge_stream_entry_t *e)
{
    if (p->kind != PENDING_COPIED_WRITE &&
        p->kind != PENDING_CLOSE_BIDI_FIN)
        return false;

    if (p->stream_ref._v != 0)
        return p->stream_ref._v == e->ref._v;
    return p->stream_id == e->transport_id;
}

/* Drop that output in place, freeing what each item owns while preserving the
 * relative order of everything else in the FIFO. */
static void bridge_drop_pending_bidi_output(moq_transport_bridge_t *b,
                                            const bridge_stream_entry_t *e)
{
    uint32_t dst = 0;
    for (uint32_t src = 0; src < b->pending_count; src++) {
        bridge_pending_item_t *p = &b->pending[src];
        if (bridge_pending_is_bidi_output(p, e)) {
            bridge_pending_counts(b, p, -1);
            bridge_cleanup_pending_item(&b->alloc, p);
            memset(p, 0, sizeof(*p));
            continue;
        }
        if (dst != src) {
            b->pending[dst] = *p;
            memset(p, 0, sizeof(*p));
        }
        dst++;
    }
    b->pending_count = dst;
}

static bool bridge_send_data_obsolete(moq_transport_bridge_t *b,
                                      const moq_send_data_action_t *data,
                                      bool subgroup_header_pending)
{
    bridge_stream_entry_t *e = bridge_find_by_ref(b, data->stream_ref);
    bool stopped = e && e->origin == BRIDGE_ORIGIN_LOCAL &&
                   e->kind == BRIDGE_STREAM_UNI && e->peer_stop_received;
    bool terminal = (e && e->data_terminal_received) ||
        bridge_session_has_data_terminal(b->session, data->stream_ref);
    if (!stopped && !terminal) return false;
    /* A timeout still owes reliable delivery of an unsent subgroup header.
     * Peer STOP suppresses even that write at callback receipt. */
    if (!stopped && subgroup_header_pending && (!e || e->bytes_sent == 0))
        return false;
    return true;
}

static bool bridge_data_output_obsolete(moq_transport_bridge_t *b,
                                         const bridge_pending_item_t *p)
{
    if (p->kind != PENDING_OPEN_UNI_DATA && p->kind != PENDING_HEADER_PAYLOAD &&
        p->kind != PENDING_PAYLOAD_ONLY) return false;
    return bridge_send_data_obsolete(b, &p->act.u.send_data,
                                    p->meta.subgroup_header &&
                                    p->kind != PENDING_PAYLOAD_ONLY);
}

static void bridge_prioritize_pending_resets(moq_transport_bridge_t *b)
{
    /* A terminal reset must not wait forever behind an unrelated blocked
     * stream. Keep every earlier same-stream or control operation ahead of
     * it; only independent data-stream work may be overtaken. */
    for (uint32_t i = 0; i < b->pending_count; i++) {
        bridge_pending_item_t reset = b->pending[i];
        if (reset.kind != PENDING_RESET_STREAM || !reset.uni_reset ||
            reset.stream_ref._v == 0) continue;
        bridge_stream_entry_t *e = bridge_find_by_ref(b, reset.stream_ref);
        if (!e || !e->fin_reset_pending) continue;
        uint32_t at = 0;
        for (uint32_t j = 0; j < i; j++) {
            const bridge_pending_item_t *p = &b->pending[j];
            if (p->dom != BRIDGE_DOM_STREAM ||
                (p->canon_ref && p->canon_ref == reset.canon_ref) ||
                (p->canon_have_sid && reset.canon_have_sid &&
                 p->canon_sid == reset.canon_sid)) at = j + 1;
        }
        for (uint32_t j = i; j > at; j--) b->pending[j] = b->pending[j - 1];
        b->pending[at] = reset;
    }
}

static void bridge_prune_terminal_output(moq_transport_bridge_t *b)
{
    for (uint32_t i = 0; i < b->max_streams; i++) {
#ifdef MOQ_SESSION_SWEEP_TESTING
        bridge_work_terminal_slots++;
#endif
        bridge_stream_entry_t *e = &b->streams[i];
        if (e->active && e->kind == BRIDGE_STREAM_UNI &&
            e->origin == BRIDGE_ORIGIN_LOCAL &&
            bridge_session_has_data_terminal(b->session, e->ref))
            e->data_terminal_received = true;
    }
    uint32_t w = 0;
    for (uint32_t r = 0; r < b->pending_count; r++) {
        bridge_pending_item_t *p = &b->pending[r];
        if (bridge_data_output_obsolete(b, p)) {
            bridge_pending_counts(b, p, -1);
            bridge_cleanup_pending_item(&b->alloc, p);
            memset(p, 0, sizeof(*p));
            continue;
        }
        if (w != r) {
            b->pending[w] = *p;
            memset(p, 0, sizeof(*p));
        }
        w++;
    }
    b->pending_count = w;
    /* The scheduler owns retained DATA too. Drop obsolete bytes there at
     * the same boundary, while preserving the stream's reset/key lifetime. */
    for (uint32_t i = 0; i < b->dstream_cap; i++) {
#ifdef MOQ_SESSION_SWEEP_TESTING
        bridge_work_terminal_slots++;
#endif
        bridge_dstream_t *d = &b->dstreams[i];
        if (!d->active || d->q_len == 0) continue;
        uint32_t kept = 0;
        for (uint32_t j = 0; j < d->q_len; j++) {
            bridge_data_item_t *item = &d->q[(d->q_head + j) % d->q_cap];
            if (bridge_send_data_obsolete(b, &item->action.u.send_data,
                                         item->meta.subgroup_header)) {
                b->dq_bytes -= bridge_send_data_bytes(&item->action.u.send_data);
                b->dq_items--;
                if (item->meta.deadline_us != UINT64_MAX) b->dq_timed--;
                moq_action_cleanup(&item->action);
                continue;
            }
            if (kept != j) d->q[(d->q_head + kept) % d->q_cap] = *item;
            kept++;
        }
        if (kept != d->q_len) {
            d->q_len = kept;
            bridge_dstream_requeue(b, d);
        }
    }
    bridge_prioritize_pending_resets(b);
}

/* RFC 9000 §3.5: a STOP_SENDING asks us to stop sending, and we answer with
 * RESET_STREAM on that half. Queue it as a pending operation rather than
 * calling the endpoint re-entrantly from its own receive callback, so it obeys
 * the ordinary WOULD_BLOCK retry and fatal rules. At most one reset is queued
 * per stream however many STOP indications arrive: `peer_stop_received` records
 * that the answer is owed, independently of local_send_closed, which an
 * ordinary FIN may already have set. Called for both stream origins. */
static moq_result_t bridge_stop_bidi_send_half(moq_transport_bridge_t *b,
                                                bridge_stream_entry_t *e,
                                                uint64_t error_code)
{
    if (e->peer_stop_received) return MOQ_OK;
    /* We already cancelled this half after the request ended: that reset
     * (owed or sent) answers the STOP too. */
    if (e->local_cancel_queued) {
        e->peer_stop_received = true;
        return MOQ_OK;
    }

    bridge_drop_pending_bidi_output(b, e);
    e->peer_stop_received = true;
    e->local_send_closed = true;

    bridge_pending_item_t p;
    memset(&p, 0, sizeof(p));
    p.kind = PENDING_RESET_STREAM;
    bridge_item_set_sid(&p, e->transport_id);
    p.error_code = error_code;
    if (!bridge_enqueue_pending(b, &p))
        return bridge_retention_failed(b, &p);
    return MOQ_OK;
}

bool bridge_stream_has_inbound_pending(const bridge_stream_entry_t *e)
{
    return e->pending_retry || e->pending_fin ||
           e->pending_reset || e->pending_stop || e->pending_admission;
}

/* -- Held peer streams ------------------------------------------------ */

static bridge_hold_t *bridge_hold_find(const moq_transport_bridge_t *b,
                                       uint64_t transport_id)
{
    if (!b->holds) return NULL;
    for (uint32_t i = 0; i < b->max_holds; i++)
        if (b->holds[i].state != BRIDGE_HOLD_FREE &&
            b->holds[i].transport_id == transport_id)
            return &b->holds[i];
    return NULL;
}

/* A RECORDED hold of this class (mapped: uni admission; unmapped: request
 * bidi without a stream mapping) is waiting: a new arrival of the same class
 * queues behind it. */
static bool bridge_hold_class_waiting(const moq_transport_bridge_t *b,
                                      bool mapped)
{
    if (!b->holds) return false;
    for (uint32_t i = 0; i < b->max_holds; i++)
        if (b->holds[i].state == BRIDGE_HOLD_RECORDED &&
            b->holds[i].mapped == mapped)
            return true;
    return false;
}

/* Share the owner deadline between admission holds and retained objects.
 * Reset/cancellation debts retry on service; they have no expired timer. */
static void bridge_publish_deadline(moq_transport_bridge_t *b)
{
    bool has = false;
    uint64_t d = UINT64_MAX;
    for (uint32_t i = 0; i < b->max_holds; i++) {
        const bridge_hold_t *h = &b->holds[i];
        if (h->state != BRIDGE_HOLD_RECORDED) continue;
        uint64_t due = h->first_refusal_us > UINT64_MAX - BRIDGE_HOLD_AGE_US
            ? UINT64_MAX : h->first_refusal_us + BRIDGE_HOLD_AGE_US;
        if (!has || due < d) d = due;
        has = true;
    }
    /* Only a queued item with a deadline can bring it earlier. */
    for (uint32_t i = 0; b->dq_timed && i < b->dstream_cap; ++i) {
        const bridge_dstream_t *stream = &b->dstreams[i];
        ++b->deadline_slots;
        if (!stream->active || stream->has_reset) continue;
        for (uint32_t j = 0; j < stream->q_len; ++j) {
            uint64_t due = stream->q[(stream->q_head + j) % stream->q_cap].meta.deadline_us;
            if (due < d) d = due;
        }
    }
    for (uint32_t i = 0; i < b->pending_count; ++i) {
        const bridge_pending_item_t *p = &b->pending[i];
        if (p->kind == PENDING_OPEN_UNI_DATA ||
            p->kind == PENDING_HEADER_PAYLOAD || p->kind == PENDING_PAYLOAD_ONLY)
            if (p->meta.deadline_us < d) d = p->meta.deadline_us;
    }
    has = has || d != UINT64_MAX;
    moq_session_set_owner_deadline(b->session, b, has, d);
}

/* Record a newly refused stream. NULL when every record is in use. */
static bridge_hold_t *bridge_hold_record(moq_transport_bridge_t *b,
                                         uint64_t transport_id,
                                         bridge_stream_kind_t kind,
                                         bool mapped, size_t len,
                                         uint64_t now_us)
{
    for (uint32_t i = 0; i < b->max_holds; i++) {
        bridge_hold_t *h = &b->holds[i];
        if (h->state != BRIDGE_HOLD_FREE) continue;
        memset(h, 0, sizeof(*h));
        h->state = BRIDGE_HOLD_RECORDED;
        h->kind = (uint8_t)kind;
        h->mapped = mapped;
        h->held_payload = len > 0;
        h->transport_id = transport_id;
        h->first_refusal_us = now_us;
        h->seq = ++b->hold_seq;
        bridge_publish_deadline(b);
        return h;
    }
    return NULL;
}

static void bridge_hold_release(moq_transport_bridge_t *b, bridge_hold_t *h)
{
    if (!h) return;
    memset(h, 0, sizeof(*h));
    bridge_publish_deadline(b);
}

/* Stream mappings still owed to the control channel on a uni-pair profile:
 * our own control stream until it has been mapped once (keyed on its stream
 * id, which its FIN does not clear), and the peer's until it is identified.
 * Each control stream keeps its mapping for the whole session (draft-22
 * section 6.3, draft-18 section 3.3). */
static uint32_t bridge_control_reserve(const moq_transport_bridge_t *b)
{
    if (b->control_mode != BRIDGE_CONTROL_UNI_PAIR) return 0;
    return (b->local_ctrl_uni_stream_id == UINT64_MAX ? 1u : 0u) +
           (b->peer_ctrl_uni_open ? 0u : 1u);
}

/* Free stream mappings a new held-capable peer stream (a request stream, or
 * a classified data stream) may take: only those beyond the control reserve,
 * so holding and admission never take an entry a control stream needs. */
static bool bridge_can_map_peer_stream(const moq_transport_bridge_t *b)
{
    uint32_t free_entries = 0;
    for (uint32_t i = 0; i < b->max_streams; i++)
        if (!b->streams[i].active) free_entries++;
    return free_entries > bridge_control_reserve(b);
}

/* The stream error code a held stream is cancelled with at the resource
 * limit (draft-22 section 15.5, draft-18 13.5, draft-16 12.1: the endpoint
 * MUST cancel a stream). Drafts 18 and 22 define EXCESSIVE_LOAD (0x9, "the
 * endpoint is overloaded and is resetting this stream"; draft-18 section
 * 3.3.3, draft-22 section 12.5). Draft-16 defines no such code; its
 * CANCELLED (0x1) covers a cancellation the receiver requested with
 * STOP_SENDING (draft-16 section 10.4.3). */
static uint64_t bridge_cancel_code(const moq_transport_bridge_t *b)
{
    return moq_session_version(b->session) == MOQ_VERSION_DRAFT_16
        ? MOQ_STREAM_ERROR_CANCELLED : MOQ_STREAM_ERROR_EXCESSIVE_LOAD;
}

/* A hold leaves RECORDED for CANCELLING: every half it still has open is
 * cancelled with the profile's code (bridge_cancel_code; draft-22 section
 * 15.5: an endpoint MUST cancel a stream after reaching a resource limit). */
static void bridge_hold_cancel(moq_transport_bridge_t *b, bridge_hold_t *h,
                               bool receive_terminal)
{
    h->state = BRIDGE_HOLD_CANCELLING;
    h->offered = false;
    h->stop_owed = !receive_terminal;
    h->reset_owed = h->kind == BRIDGE_STREAM_BIDI;
    bridge_publish_deadline(b);
}

/* Perform the owed halves of a CANCELLING hold in its own slot. MOQ_OK when
 * the record was settled and freed or is still waiting on a WOULD_BLOCK;
 * MOQ_ERR_INTERNAL when the endpoint failed (fatal). */
static moq_result_t bridge_hold_drive_cancel(moq_transport_bridge_t *b,
                                             bridge_hold_t *h, bool *progress)
{
    const moq_transport_endpoint_ops_t *ops = b->ops;
    uint64_t id = h->transport_id;
    uint64_t code = bridge_cancel_code(b);
    if (h->stop_owed && h->reset_owed && HAS_FIELD(ops, abort_stream) &&
        ops->abort_stream) {
        moq_transport_result_t r = sanitize_stream_result(
            ops->abort_stream(b->endpoint_ctx, id, code));
        if (r == MOQ_TRANSPORT_WOULD_BLOCK) return MOQ_OK;
        if (r != MOQ_TRANSPORT_OK) {
            bridge_set_fatal(b, 0x1);
            return MOQ_ERR_INTERNAL;
        }
        h->stop_owed = h->reset_owed = false;
        *progress = true;
    }
    if (h->reset_owed) {
        moq_transport_result_t r = sanitize_stream_result(
            ops->reset_stream(b->endpoint_ctx, id, code));
        if (r == MOQ_TRANSPORT_WOULD_BLOCK) return MOQ_OK;
        if (r != MOQ_TRANSPORT_OK) {
            bridge_set_fatal(b, 0x1);
            return MOQ_ERR_INTERNAL;
        }
        h->reset_owed = false;
        *progress = true;
    }
    if (h->stop_owed) {
        moq_transport_result_t r = sanitize_stream_result(
            ops->stop_sending(b->endpoint_ctx, id, code));
        if (r == MOQ_TRANSPORT_WOULD_BLOCK) return MOQ_OK;
        if (r != MOQ_TRANSPORT_OK) {
            bridge_set_fatal(b, 0x1);
            return MOQ_ERR_INTERNAL;
        }
        h->stop_owed = false;
        *progress = true;
    }
    /* Both halves settled: the stream's bridge state goes with the record;
     * the adapter dropped the held chunk when it took the cancellation. */
    if (h->mapped) {
        bridge_stream_entry_t *e = bridge_find_by_id(b, id);
        if (e) bridge_deactivate_stream(b, e);
    }
    bridge_hold_release(b, h);
    return MOQ_OK;
}

/* Hold maintenance, run on every service pass before any deadline is
 * ticked: expire holds that reached the hold age, drive owed cancellations,
 * and offer readiness to the oldest waiting hold of each class whose
 * resource is free. Sets *progress when anything moved. */
static moq_result_t bridge_hold_maintain(moq_transport_bridge_t *b,
                                         uint64_t now_us, bool *progress)
{
    if (!b->holds) return MOQ_OK;
    for (uint32_t i = 0; i < b->max_holds; i++) {
        bridge_hold_t *h = &b->holds[i];
        if (h->state != BRIDGE_HOLD_RECORDED) continue;
        if (h->first_refusal_us <= UINT64_MAX - BRIDGE_HOLD_AGE_US &&
            now_us >= h->first_refusal_us + BRIDGE_HOLD_AGE_US) {
            bridge_hold_cancel(b, h, false);
            *progress = true;
        }
    }
    for (uint32_t i = 0; i < b->max_holds && !b->fatal; i++) {
        bridge_hold_t *h = &b->holds[i];
        if (h->state != BRIDGE_HOLD_CANCELLING) continue;
        moq_result_t rc = bridge_hold_drive_cancel(b, h, progress);
        if (rc < 0) return rc;
    }
    for (int mapped = 0; mapped < 2; mapped++) {
        bridge_hold_t *oldest = NULL;
        bool outstanding = false;
        for (uint32_t i = 0; i < b->max_holds; i++) {
            bridge_hold_t *h = &b->holds[i];
            if (h->state != BRIDGE_HOLD_RECORDED || h->mapped != (mapped != 0))
                continue;
            if (h->offered) { outstanding = true; break; }
            if (!oldest || h->seq < oldest->seq) oldest = h;
        }
        if (outstanding || !oldest) continue;
        bool ready = mapped ? moq_session_can_admit_data_stream(b->session)
                            : bridge_can_map_peer_stream(b);
        if (!ready) continue;
        oldest->offered = true;
        if (mapped) {
            bridge_stream_entry_t *e = bridge_find_by_id(b, oldest->transport_id);
            if (e) e->pending_admission = false;
        }
        *progress = true;
    }
    bridge_publish_deadline(b);
    return MOQ_OK;
}

/* A new peer uni data stream the session cannot admit: the adapter keeps the
 * exact chunk (MOQ_ERR_INPUT_NOT_CONSUMED). With held-stream records the
 * stream also takes one -- or, every record being in use, is refused for the
 * adapter to cancel (MOQ_BRIDGE_INGRESS_REFUSE_CANCEL), its bridge state
 * (including any classified prefix) released at once. */
static moq_result_t bridge_hold_admission(moq_transport_bridge_t *b,
                                          bridge_stream_entry_t *e,
                                          size_t len, uint64_t now_us)
{
    if (b->holds) {
        bridge_hold_t *h = bridge_hold_find(b, e->transport_id);
        if (!h) {
            h = bridge_hold_record(b, e->transport_id, BRIDGE_STREAM_UNI,
                                   true, len, now_us);
            if (!h) {
                bridge_deactivate_stream(b, e);
                return MOQ_BRIDGE_INGRESS_REFUSE_CANCEL;
            }
        } else {
            h->offered = false;      /* readiness was not a reservation */
            h->mapped = true;        /* it has a stream entry now; age kept */
            if (len > 0) h->held_payload = true;
        }
    }
    e->pending_admission = true;
    e->held_input = true;
    if (len > 0) e->held_payload = true;
    return MOQ_ERR_INPUT_NOT_CONSUMED;
}

/* Admission preflight for a NEW peer uni data stream (one the session does
 * not own yet): when the session has no receive entry to admit it -- or,
 * with held-stream records, when older held streams are waiting or the
 * stream has not been offered yet -- refuse the whole chunk, bytes and FIN,
 * without feeding it. Returns 0 when the chunk may be fed, else the result
 * to return (MOQ_ERR_INPUT_NOT_CONSUMED or MOQ_BRIDGE_INGRESS_REFUSE_CANCEL).
 * Streams the session already owns, and terminal sessions (the query reports
 * admissible so the input reaches the terminal path) are never refused. */
static moq_result_t bridge_admission_refused(moq_transport_bridge_t *b,
                                             bridge_stream_entry_t *e,
                                             moq_stream_ref_t ref, size_t len,
                                             uint64_t now_us)
{
    /* The adapter still owes a chunk that carried bytes: an empty delivery
     * (a FIN-only notification) cannot overtake it, ready or not. */
    if (e->held_input && e->held_payload && len == 0)
        return MOQ_ERR_INPUT_NOT_CONSUMED;
    if (moq_session_has_transport_stream(b->session, ref))
        return 0;
    if (b->holds) {
        bridge_hold_t *h = bridge_hold_find(b, e->transport_id);
        if (h && h->state == BRIDGE_HOLD_CANCELLING)
            return MOQ_ERR_INPUT_NOT_CONSUMED;
        bool queued = h ? !h->offered : bridge_hold_class_waiting(b, true);
        if (!queued && moq_session_can_admit_data_stream(b->session)) {
            e->pending_admission = false;
            return 0;
        }
        return bridge_hold_admission(b, e, len, now_us);
    }
    if (moq_session_can_admit_data_stream(b->session)) {
        e->pending_admission = false;
        return 0;
    }
    return bridge_hold_admission(b, e, len, now_us);
}

/* A NEW peer uni data stream on a bidi-control profile (draft-16: every
 * peer uni stream is data) that finds no free stream mapping, with
 * held-stream records: its exact chunk is held without a mapping (zero bytes
 * and no FIN consumed), offered oldest first when a mapping frees, refused
 * for the adapter to cancel when every record is in use, and cancelled when
 * it reaches the hold age. Returns true with *out set when it handled the
 * delivery; false when the stream is mapped now or may be mapped. */
static bool bridge_hold_unmapped_uni(moq_transport_bridge_t *b,
                                     uint64_t stream_id, size_t len,
                                     uint64_t now_us, moq_result_t *out)
{
    if (!b->holds || b->control_mode != BRIDGE_CONTROL_BIDI) return false;
    bridge_hold_t *h = bridge_hold_find(b, stream_id);
    if (h && h->state == BRIDGE_HOLD_CANCELLING) {
        *out = MOQ_ERR_INPUT_NOT_CONSUMED;
        return true;
    }
    bool queued = h ? !h->offered : bridge_hold_class_waiting(b, false);
    if (h && h->held_payload && len == 0) queued = true;
    if (!queued && bridge_can_map_peer_stream(b)) return false;
    if (h) {
        h->offered = false;
        if (len > 0) h->held_payload = true;
        *out = MOQ_ERR_INPUT_NOT_CONSUMED;
        return true;
    }
    *out = bridge_hold_record(b, stream_id, BRIDGE_STREAM_UNI, false, len,
                              now_us)
        ? MOQ_ERR_INPUT_NOT_CONSUMED : MOQ_BRIDGE_INGRESS_REFUSE_CANCEL;
    return true;
}

/* A NEW peer uni stream on a uni-pair profile (drafts 18 and 22), with
 * held-stream records, is placed by the class of its first chunk before it
 * takes a stream mapping:
 *   - a complete data type that finds no mapping free beyond the control
 *     reserve, or finds an older stream held without a mapping, is held the
 *     same way as on a bidi-control profile (zero bytes and no FIN consumed,
 *     offered oldest first, refused for the adapter to cancel when every
 *     record is in use, cancelled at the hold age);
 *   - a complete padding type that finds no mapping free beyond the reserve
 *     is refused for the adapter to cancel; it is never held (draft-22 and
 *     draft-18 section 11.5.1: a padding stream may be cancelled at any time);
 *   - a control type, an incomplete type and an unknown type are left to the
 *     classifying path below. A stream that may be the control stream is
 *     never held or cancelled here (draft-22 section 6.3, draft-18 section
 *     3.3: a control stream is never closed during the session).
 * Each cancellation is the stream-local response to a resource limit
 * (draft-22 section 15.5, draft-18 section 13.5), whatever the setup state.
 * Returns true with *out set when it handled the delivery; false when the
 * stream takes the classifying path now. */
static bool bridge_uni_pair_place_unmapped(moq_transport_bridge_t *b,
                                           uint64_t stream_id,
                                           const uint8_t *data, size_t len,
                                           uint64_t now_us, moq_result_t *out)
{
    if (!b->holds || b->control_mode != BRIDGE_CONTROL_UNI_PAIR) return false;
    bridge_hold_t *h = bridge_hold_find(b, stream_id);
    if (h) {
        /* Only a complete data type is ever held here, and the adapter
         * redelivers that exact chunk, so it is not classified again. */
        if (h->state == BRIDGE_HOLD_CANCELLING) {
            *out = MOQ_ERR_INPUT_NOT_CONSUMED;
            return true;
        }
        bool queued = !h->offered || (h->held_payload && len == 0);
        if (!queued && bridge_can_map_peer_stream(b)) return false;
        h->offered = false;
        if (len > 0) h->held_payload = true;
        *out = MOQ_ERR_INPUT_NOT_CONSUMED;
        return true;
    }
    uint8_t tmp[9];
    size_t tn = len < sizeof(tmp) ? len : sizeof(tmp);
    if (tn > 0) memcpy(tmp, data, tn);
    moq_uni_class_t cls = moq_session_classify_peer_uni(b->session, tmp, tn);
    if (cls == MOQ_UNI_CLASS_PADDING) {
        if (bridge_can_map_peer_stream(b)) return false;
        *out = MOQ_BRIDGE_INGRESS_REFUSE_CANCEL;
        return true;
    }
    if (cls != MOQ_UNI_CLASS_DATA) return false;
    if (!bridge_hold_class_waiting(b, false) && bridge_can_map_peer_stream(b))
        return false;
    *out = bridge_hold_record(b, stream_id, BRIDGE_STREAM_UNI, false, len,
                              now_us)
        ? MOQ_ERR_INPUT_NOT_CONSUMED : MOQ_BRIDGE_INGRESS_REFUSE_CANCEL;
    return true;
}

/* The session took the stream's input (consumed, or retained under its own
 * backpressure): the adapter's held-chunk obligation, if any, is discharged,
 * and with it the stream's hold record -- the stream mapping, if it
 * continues, is not acceptance's concern. */
static void bridge_admission_accepted(moq_transport_bridge_t *b,
                                      bridge_stream_entry_t *e)
{
    e->held_input = false;
    e->held_payload = false;
    e->pending_admission = false;
    bridge_hold_t *h = bridge_hold_find(b, e->transport_id);
    if (h && h->state == BRIDGE_HOLD_RECORDED) bridge_hold_release(b, h);
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
    if (b->pending_count > 0 || b->dq_items > 0) return true;
    if (b->pending_control) return true;
    if (b->needs_close) return true;
    /* Only a held deferred reset can answer here: with none held, the stream
     * table is not read. */
    for (uint32_t i = 0; b->dreset_count && i < b->dstream_cap; ++i) {
#ifdef MOQ_SESSION_SWEEP_TESTING
        bridge_work_query_slots++;
#endif
        if (b->dstreams[i].active && b->dstreams[i].has_reset) return true;
    }
    for (uint32_t i = 0; i < b->max_streams; i++) {
        if (b->streams[i].active &&
            (b->streams[i].reset_source_held ||
             bridge_stream_has_inbound_pending(&b->streams[i])))
            return true;
    }
    for (uint32_t i = 0; b->holds && i < b->max_holds; i++) {
        if (b->holds[i].state == BRIDGE_HOLD_CANCELLING ||
            (b->holds[i].state == BRIDGE_HOLD_RECORDED && !b->holds[i].offered))
            return true;
    }
    return false;
}

bool moq_transport_bridge_has_outbound_pending(const moq_transport_bridge_t *b)
{
    if (!b) return false;
    /* Only state that transport write-readiness can unblock: the outbound
     * action FIFO (writes the transport could not accept) and a deferred
     * transport close (a CONNECTION_CLOSE still to send). Inbound retry state
     * -- pending_control and the per-stream pending_retry/reset/stop/fin --
     * is deliberately excluded: it is unblocked by the app draining events
     * (freeing the session's receive queue), NOT by send capacity. Arming
     * connection-write-ready on inbound pending would spin, since connections
     * are almost always write-ready. */
    if (b->pending_count > 0 || b->needs_close || bridge_pump_pick(b))
        return true;
    /* Pressure can leave a refused reset in its original core owner instead
     * of the pending FIFO. Transport readiness must still retry it. */
    for (uint32_t i = 0; i < b->max_streams; ++i)
        if (b->streams[i].active && b->streams[i].reset_source_held)
            return true;
    /* An owed cancellation of a held stream is send-side work. */
    for (uint32_t i = 0; b->holds && i < b->max_holds; i++)
        if (b->holds[i].state == BRIDGE_HOLD_CANCELLING)
            return true;
    return false;
}

/* Readiness, not identity: while the session drains buffered control input
 * on a uni-pair profile, bridge_uni_pair_inbound refuses every peer uni
 * chunk except the control stream's, and this answers "not ready yet" for
 * any stream such a refusal could concern: a mapped peer uni stream, or an
 * id the bridge has no mapping for. A mapped bidi stream or a stream this
 * side opened is never refused there and is excluded. An unmapped id is
 * answered conservatively -- the bridge cannot tell a refused peer uni
 * stream from any other unknown id -- so the answer neither identifies a
 * peer uni stream nor says that anything is retained: an adapter asks only
 * about streams whose receive records it polls, and the input stays its
 * own. */
static bool bridge_uni_deferred(const moq_transport_bridge_t *b,
                                uint64_t stream_id,
                                const bridge_stream_entry_t *e)
{
    return b->pending_control && b->control_mode == BRIDGE_CONTROL_UNI_PAIR &&
           stream_id != b->pending_control_stream_id &&
           (!e || (e->kind == BRIDGE_STREAM_UNI &&
                   e->origin == BRIDGE_ORIGIN_PEER));
}

bool moq_transport_bridge_stream_has_pending(
    const moq_transport_bridge_t *b, uint64_t stream_id)
{
    if (!b) return false;
    if (b->pending_control && b->pending_control_stream_id == stream_id)
        return true;
    const bridge_stream_entry_t *e = bridge_find_by_id_const(b, stream_id);
    {
        const bridge_hold_t *h = bridge_hold_find(b, stream_id);
        if (h) {
            if (h->state == BRIDGE_HOLD_CANCELLING || !h->offered) return true;
            return h->kind == BRIDGE_STREAM_UNI &&
                   bridge_uni_deferred(b, stream_id, e);
        }
    }
    if (e && bridge_stream_has_inbound_pending(e)) return true;
    return bridge_uni_deferred(b, stream_id, e);
}

uint64_t moq_transport_bridge_event_progress_token(
    const moq_transport_bridge_t *b)
{
    if (!b || !b->session) return 0;
    return moq_session_event_progress_token(b->session);
}

bool moq_transport_bridge_has_events(const moq_transport_bridge_t *b)
{
    if (!b || !b->session) return false;
    return moq_session_has_events(b->session);
}

bool moq_transport_bridge_terminal_facts(const moq_transport_bridge_t *b,
                                          bool *out_observed)
{
    /* Both facts come from the session in one call so an adapter cannot sample
     * them at two different instants. A bridge with no session has neither. */
    bool enqueued = b != NULL && b->session != NULL &&
                    moq_session_terminal_enqueued(b->session);
    if (out_observed)
        *out_observed = b != NULL && b->session != NULL &&
                        moq_session_terminal_observed(b->session);
    return enqueued;
}

bool moq_transport_bridge_stream_is_control(const moq_transport_bridge_t *b,
                                            uint64_t stream_id)
{
    if (!b || stream_id == UINT64_MAX) return false;
    if (b->control_open && b->control_stream_id == stream_id) return true;
    if (b->peer_control_open && b->peer_control_stream_id == stream_id) return true;
    if (b->pending_control && b->pending_control_stream_id == stream_id) return true;
    const bridge_stream_entry_t *e = bridge_find_by_id_const(b, stream_id);
    return e && e->kind == BRIDGE_STREAM_UNI &&
           e->uni_disp == BRIDGE_UNI_DISP_CONTROL;
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

/*
 * Retaining an item the bridge already owns cannot fail by capacity: a retry
 * pass moves every survivor out of the queue before re-enqueuing at most the
 * same number back into an array of the same size, and the drain never polls
 * without a free slot. If it fails anyway, the queue's own accounting is
 * wrong, and the item in hand is real work with nowhere to live.
 *
 * That is an invariant breach, not a recoverable condition, so it is never
 * absorbed: ownership is released exactly once here, the bridge is marked
 * fatal with its own code, and the caller stops and surfaces the error rather
 * than continuing as though the work had been retained.
 */
static moq_result_t bridge_retention_failed(moq_transport_bridge_t *b,
                                             bridge_pending_item_t *p)
{
    bridge_cleanup_pending_item(&b->alloc, p);
    memset(p, 0, sizeof(*p));
    bridge_set_fatal(b, 0x5);
    return MOQ_ERR_INTERNAL;
}

#ifdef MOQ_BRIDGE_TEST_INJECT
/*
 * Lets a fixture reach the retention-failure branch, which capacity counting
 * otherwise makes unreachable. Test builds only: no symbol, no storage in a
 * default build.
 */
static unsigned g_retain_fail_after;
static bool      g_retain_fail_armed;

void moq_bridge_test_fail_retain_after(unsigned n)
{
    g_retain_fail_after = n;
    g_retain_fail_armed = true;
}

#endif

static bool bridge_enqueue_pending(moq_transport_bridge_t *b,
                                    const bridge_pending_item_t *item)
{
#ifdef MOQ_BRIDGE_TEST_INJECT
    if (g_retain_fail_armed) {
        if (g_retain_fail_after == 0) {
            g_retain_fail_armed = false;
            return false;
        }
        g_retain_fail_after--;
    }
#endif
    if (b->pending_count >= b->max_pending) return false;

    /*
     * Normalise the item's ordering identity here, at the only place items
     * enter the queue, so the conflict scan never consults the stream map.
     * Both names are kept whenever either resolves; a zero/unknown side
     * survives only while no mapping exists (a pre-open item whose entry was
     * deactivated before it was retained). A pair that contradicts the map is
     * refused rather than silently resolved to one side.
     */
    bridge_pending_item_t norm = *item;
    norm.dom = (uint8_t)bridge_pending_domain(&norm);
    norm.canon_ref = norm.stream_ref._v;
    norm.canon_sid = norm.stream_id;
    norm.canon_have_sid = norm.has_stream_id;

    if (norm.canon_ref != 0 && !norm.canon_have_sid) {
        const bridge_stream_entry_t *e = bridge_find_by_ref(b, norm.stream_ref);
        if (e != NULL && e->active) {
            norm.canon_sid = e->transport_id;
            norm.canon_have_sid = true;
        }
    } else if (norm.canon_ref == 0 && norm.canon_have_sid) {
        const bridge_stream_entry_t *e = bridge_find_by_id(b, norm.canon_sid);
        if (e != NULL && e->active)
            norm.canon_ref = e->ref._v;
    } else if (norm.canon_ref != 0 && norm.canon_have_sid) {
        const bridge_stream_entry_t *e = bridge_find_by_ref(b, norm.stream_ref);
        if (e != NULL && e->active && e->transport_id != norm.canon_sid)
            return false;
    }

    b->pending[b->pending_count++] = norm;
    bridge_pending_counts(b, &norm, +1);
    return true;
}

/* Control bytes on a unidirectional control-channel pair go out on the local
 * uni control stream. Until that stream is open they wait as a pending
 * control item behind the open that will create it. */
static moq_result_t send_control_on_local_uni(moq_transport_bridge_t *b,
                                              moq_action_t *act)
{
    const moq_send_control_action_t *sc = &act->u.send_control;

    if (!b->local_ctrl_uni_open) {
        bridge_pending_item_t p;
        memset(&p, 0, sizeof(p));
        p.kind = PENDING_OPEN_CONTROL;
        p.data = bridge_alloc(&b->alloc, sc->len);
        if (!p.data) { bridge_set_fatal(b, 0x1); return MOQ_ERR_NOMEM; }
        memcpy(p.data, sc->data, sc->len);
        p.data_len = sc->len;
        moq_action_cleanup(act);
        if (!bridge_enqueue_pending(b, &p))
            return bridge_retention_failed(b, &p);
        return MOQ_OK;
    }

    moq_transport_result_t wr = sanitize_stream_result(
        b->ops->write(b->endpoint_ctx, b->local_ctrl_uni_stream_id,
                       sc->data, sc->len, false));
    if (wr == MOQ_TRANSPORT_WOULD_BLOCK) {
        bridge_pending_item_t p;
        memset(&p, 0, sizeof(p));
        p.kind = PENDING_COPIED_WRITE;
        bridge_item_set_sid(&p, b->local_ctrl_uni_stream_id);
        p.stream_ref = b->local_ctrl_uni_ref;
        p.data = bridge_alloc(&b->alloc, sc->len);
        if (!p.data) { bridge_set_fatal(b, 0x1); return MOQ_ERR_NOMEM; }
        memcpy(p.data, sc->data, sc->len);
        p.data_len = sc->len;
        moq_action_cleanup(act);
        if (!bridge_enqueue_pending(b, &p))
            return bridge_retention_failed(b, &p);
        return MOQ_OK;
    }
    moq_action_cleanup(act);
    if (wr == MOQ_TRANSPORT_ERROR) {
        bridge_set_fatal(b, 0x1);
        return MOQ_ERR_INTERNAL;
    }
    return MOQ_OK;
}

static moq_result_t dispatch_send_control(moq_transport_bridge_t *b,
                                           moq_action_t *act)
{
    const moq_send_control_action_t *sc = &act->u.send_control;

    if (b->control_mode == BRIDGE_CONTROL_UNI_PAIR)
        return send_control_on_local_uni(b, act);

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
                if (!bridge_enqueue_pending(b, &p))
                    return bridge_retention_failed(b, &p);
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
                if (!bridge_enqueue_pending(b, &p))
                    return bridge_retention_failed(b, &p);
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
        bridge_item_set_sid(&p, b->control_stream_id);
        p.data = bridge_alloc(&b->alloc, sc->len);
        if (!p.data) { bridge_set_fatal(b, 0x1); return MOQ_ERR_NOMEM; }
        memcpy(p.data, sc->data, sc->len);
        p.data_len = sc->len;
        moq_action_cleanup(act);
        if (!bridge_enqueue_pending(b, &p))
            return bridge_retention_failed(b, &p);
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
        if (!bridge_enqueue_pending(b, &p))
            return bridge_retention_failed(b, &p);
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

/* The FIN of a local unidirectional data stream has been written. An
 * adapter that reports "all data committed" keeps the mapping until that
 * report (or a RESET) so the session's FIN-started timer can still reach the
 * stream; otherwise the stream is retired here. */
static void bridge_local_data_fin_done(moq_transport_bridge_t *b, moq_stream_ref_t ref)
{
    bridge_mark_local_close(b, ref);
    bridge_stream_entry_t *e = bridge_find_by_ref(b, ref);
    if (e && e->kind == BRIDGE_STREAM_UNI && e->origin == BRIDGE_ORIGIN_LOCAL &&
        (b->ops->capabilities & MOQ_TRANSPORT_CAP_COMMIT_REPORT)) {
        e->awaiting_commit = true;
        return;
    }
    bridge_retire_or_tombstone(b, ref);
}

/* -- Data-stream scheduling (MOQT 7.2) ----------------------------------- */

#define BRIDGE_KEY_POS_MAX 0xFFFFu

/* The endpoint key (see set_stream_priority). */
static uint32_t bridge_key(uint8_t subscriber_priority,
                           uint8_t publisher_priority, uint32_t pos)
{
    uint32_t p = pos < BRIDGE_KEY_POS_MAX ? pos + 1 : BRIDGE_KEY_POS_MAX;
    return (uint32_t)subscriber_priority << 24 |
           (uint32_t)publisher_priority << 16 | p;
}

static bool bridge_drained_cap(const moq_transport_bridge_t *b)
{
    return (b->ops->capabilities & MOQ_TRANSPORT_CAP_STREAM_DRAINED) != 0;
}

static bridge_dstream_t *bridge_dstream_find(moq_transport_bridge_t *b,
                                             moq_stream_ref_t ref)
{
    uint32_t slot = bridge_index_find(b->dref_index, b->dindex_mask, ref._v);
    return slot == BRIDGE_INDEX_EMPTY ? NULL : &b->dstreams[slot];
}

/* A marker without an object location continues the last fill class. For
 * ordinary streams, a later owner-wide update supersedes queued snapshots;
 * a fill retains its independently resolved child parameters. */
static moq_action_meta_t bridge_dstream_item_meta(const bridge_dstream_t *d,
                                                 const bridge_data_item_t *item)
{
    moq_action_meta_t meta = d->meta;
    if (item) {
        if (item->meta.mode != MOQ_SCHED_MODE_NONE &&
            (item->meta.mode != MOQ_SCHED_MODE_FILL || item->meta.has_location))
            meta = item->meta;
        const moq_action_t *a = &item->action;
        if (a->detail_size >= offsetof(moq_send_data_action_t,
                                       scheduling_priority) +
                              sizeof(a->u.send_data.scheduling_priority) &&
            a->u.send_data.scheduling_priority &&
            (meta.mode != MOQ_SCHED_MODE_FILL || item->meta.has_location))
            meta.publisher_priority = (uint8_t)a->u.send_data.scheduling_priority;
    }
    if (meta.mode != MOQ_SCHED_MODE_FILL)
        meta.subscriber_priority = d->key.subscriber_priority;
    return meta;
}

static bool bridge_same_class(const moq_action_meta_t *x,
                               const moq_action_meta_t *y)
{
    return x->owner == y->owner && x->mode == y->mode &&
        x->subscriber_priority == y->subscriber_priority &&
        x->publisher_priority == y->publisher_priority &&
        x->has_location == y->has_location &&
        (!x->has_location ||
         (x->group_id == y->group_id && x->subgroup_id == y->subgroup_id &&
          x->group_order == y->group_order &&
          x->fill_group_order == y->fill_group_order));
}

/* Keep `d` in its ready heap while it has something to hand over (data, or
 * a deferred reset) and no write retained: a stream the endpoint refused
 * waits alone, and the others carry on. */
static void bridge_dstream_requeue(moq_transport_bridge_t *b,
                                   bridge_dstream_t *d)
{
    const moq_action_meta_t head = bridge_dstream_item_meta(d,
        d->q_len ? &d->q[d->q_head] : NULL);
    bool ready = (d->q_len > 0 || d->has_reset) && d->retained == 0 &&
                 !d->pump_skipped && !d->dispatching;
    /* A FETCH changes publisher class; a fill can also change its group.
     * Hold just that stream until its earlier class drains. Preamble-only
     * bytes acquire the first object's class. */
    if (ready && d->q_len > 0 && bridge_drained_cap(b) && d->keyed &&
        !d->prefix_only && !bridge_same_class(&head, &d->meta))
        ready = false;
    const bool active = d->keyed || d->retained || d->q_len || d->has_reset ||
                        d->dispatching;
    const moq_action_meta_t rep = d->retained || d->dispatching ||
        (bridge_drained_cap(b) && d->keyed && !d->prefix_only)
            ? d->meta : head;
    if (active != d->order_active || !bridge_same_class(&rep, &d->order_meta)) {
        d->order_meta = rep;
        d->order_active = active;
        d->order_dirty = b->order_dirty = true;
    }
    uint8_t h = d->has_sid ? 0 : 1;
    if (d->heap_at != BRIDGE_INDEX_EMPTY && (!ready || d->heap_id != h))
        bridge_dheap_remove(b, d);
    if (!ready) return;
    d->hkey = bridge_key(head.subscriber_priority, head.publisher_priority, d->pos);
    if (d->heap_at == BRIDGE_INDEX_EMPTY) {
        d->heap_id = h;
        bridge_dheap_put(b, h, b->dheap_len[h]++, (uint32_t)(d - b->dstreams));
    }
    bridge_dheap_up(b, h, d->heap_at);
    bridge_dheap_down(b, h, d->heap_at);
}

static void bridge_dstream_set_sid(moq_transport_bridge_t *b,
                                   bridge_dstream_t *d, uint64_t sid)
{
    if (d->has_sid && d->sid == sid) return;
    if (d->has_sid)
        bridge_index_remove(b->dsid_index, b->dindex_mask, d->sid);
    d->has_sid = true;
    d->sid = sid;
    bridge_index_insert(b->dsid_index, b->dindex_mask, sid,
                        (uint32_t)(d - b->dstreams));
    bridge_dstream_requeue(b, d);
}

/* Hand the endpoint `d`'s key when it changed, and keep its ready-heap
 * place. With the drained capability only while the endpoint holds the
 * stream: an idle stream gets its key at its next write, so the endpoint
 * never keeps state for it meanwhile. */
static moq_result_t bridge_dstream_apply(moq_transport_bridge_t *b,
                                         bridge_dstream_t *d)
{
    bridge_dstream_requeue(b, d);
    if (!d->has_sid || !HAS_FIELD(b->ops, set_stream_priority) ||
        !b->ops->set_stream_priority)
        return MOQ_OK;
    if (bridge_drained_cap(b) && !d->keyed) return MOQ_OK;
    uint32_t key = bridge_key(d->meta.subscriber_priority,
                              d->meta.publisher_priority, d->pos);
    if (d->keyed && key == d->applied) return MOQ_OK;
    if (b->ops->set_stream_priority(b->endpoint_ctx, d->sid, key) !=
        MOQ_TRANSPORT_OK) {
        bridge_set_fatal(b, 0x1);
        return MOQ_ERR_INTERNAL;
    }
    d->applied = key;
    d->keyed = true;
    return MOQ_OK;
}

/* Rebuild only dirty parent snapshots. Held endpoint bytes remain vertices
 * after leaving the ready heap. The ranker uses O(n) scratch/O(n^2) work for
 * one parent's n representatives; served history survives every rebuild.
 * All logical ranks are installed before any further stream is selected.
 * Native rank compression remains the endpoint's documented limit. */
static moq_result_t bridge_dstreams_reorder(moq_transport_bridge_t *b)
{
    if (!b->order_dirty) return MOQ_OK;
    b->order_dirty = false;
    for (uint32_t slot = 0; slot < b->dstream_cap; ++slot) {
        bridge_dstream_t *d = &b->dstreams[slot];
        if (!d->active || !d->order_dirty) continue;
        const uint32_t first = bridge_owner_first(b, d->key.owner);
        uint32_t count = 0;
        for (uint32_t i = first; i != BRIDGE_INDEX_EMPTY;
             i = b->dstreams[i].owner_next) {
            bridge_dstream_t *e = &b->dstreams[i];
            e->order_dirty = false;
            e->pos = 0;
            if (!e->order_active) continue;
            b->order_slots[count] = i;
            b->order_nodes[count++].meta = e->order_meta;
        }
        moq_stream_order_rank(b->order_nodes, count);
        for (uint32_t i = 0; i < count; ++i)
            b->dstreams[b->order_slots[i]].pos = b->order_nodes[i].rank;
        for (uint32_t i = first; i != BRIDGE_INDEX_EMPTY;
             i = b->dstreams[i].owner_next) {
            moq_result_t rc = bridge_dstream_apply(b, &b->dstreams[i]);
            if (rc < 0) return rc;
        }
    }
    /* Several keys changed together, so repair the whole heap, not a
     * sequence of single-key fixes that assumes all other keys are stable. */
    for (uint8_t h = 0; h < 2; ++h)
        for (uint32_t i = b->dheap_len[h] / 2; i > 0; --i)
            bridge_dheap_down(b, h, i - 1);
    return MOQ_OK;
}

/* `d` leaves the priority order; whatever it still queues is dropped. A
 * failure has already cleared every stream, `d` among them. */
static moq_result_t bridge_dstream_free(moq_transport_bridge_t *b,
                                        bridge_dstream_t *d)
{
    bridge_dstream_release(b, d);
    return bridge_dstreams_reorder(b);
}

/* FIN handed over: the stream leaves the order once the endpoint drains it,
 * or now without the drained capability. A deferred reset keeps it until
 * the reset goes out. */
static moq_result_t bridge_dstream_fin(moq_transport_bridge_t *b,
                                       moq_stream_ref_t ref)
{
    bridge_dstream_t *d = bridge_dstream_find(b, ref);
    if (!d) return MOQ_OK;
    if (!bridge_drained_cap(b) && !d->has_reset)
        return bridge_dstream_free(b, d);
    d->fin_handed = true;
    return MOQ_OK;
}

void moq_transport_bridge_on_stream_drained(moq_transport_bridge_t *b,
                                            uint64_t stream_id)
{
    if (!b || b->fatal || b->closed) return;
    uint32_t slot = bridge_index_find(b->dsid_index, b->dindex_mask,
                                      stream_id);
    if (slot == BRIDGE_INDEX_EMPTY) return;
    bridge_dstream_t *d = &b->dstreams[slot];
    if (d->fin_handed && !d->has_reset)
        (void)bridge_dstream_free(b, d);   /* a failure latches fatal */
    else {
        d->keyed = false;
        bridge_dstream_requeue(b, d);
        (void)bridge_dstreams_reorder(b);
    }
}

/* Double the dstream table and its heaps. False (fatal latched) on
 * allocation failure, leaving them untouched. */
static bool bridge_dstreams_grow(moq_transport_bridge_t *b)
{
    uint32_t cap = b->dstream_cap;
    if (cap > UINT32_MAX / 2 ||
        !bridge_array_fits(cap, 2 * sizeof(bridge_dstream_t)) ||
        !bridge_array_fits(cap, 2 * sizeof(moq_stream_order_node_t))) {
        bridge_set_fatal(b, 0x1);
        return false;
    }
    bridge_dstream_t *n = bridge_alloc(&b->alloc, 2 * (size_t)cap * sizeof(*n));
    uint32_t *h0 = n ? bridge_alloc(&b->alloc, 2 * (size_t)cap * sizeof(*h0))
                     : NULL;
    uint32_t *h1 = h0 ? bridge_alloc(&b->alloc, 2 * (size_t)cap * sizeof(*h1))
                      : NULL;
    uint32_t *sk = h1 ? bridge_alloc(&b->alloc, 2 * (size_t)cap * sizeof(*sk))
                      : NULL;
    moq_stream_order_node_t *nodes = sk
        ? bridge_alloc(&b->alloc, 2 * (size_t)cap * sizeof(*nodes)) : NULL;
    uint32_t *slots = nodes
        ? bridge_alloc(&b->alloc, 2 * (size_t)cap * sizeof(*slots)) : NULL;
    if (!slots || !bridge_dindex_build(b, 2 * cap)) {
        if (n) bridge_free(&b->alloc, n, 2 * (size_t)cap * sizeof(*n));
        if (h0) bridge_free(&b->alloc, h0, 2 * (size_t)cap * sizeof(*h0));
        if (h1) bridge_free(&b->alloc, h1, 2 * (size_t)cap * sizeof(*h1));
        if (sk) bridge_free(&b->alloc, sk, 2 * (size_t)cap * sizeof(*sk));
        if (nodes) bridge_free(&b->alloc, nodes, 2 * (size_t)cap * sizeof(*nodes));
        if (slots) bridge_free(&b->alloc, slots, 2 * (size_t)cap * sizeof(*slots));
        bridge_set_fatal(b, 0x1);
        return false;
    }
    bridge_free(&b->alloc, b->dskip, cap * sizeof(*sk));
    b->dskip = sk;
    bridge_free(&b->alloc, b->order_nodes, cap * sizeof(*nodes));
    bridge_free(&b->alloc, b->order_slots, cap * sizeof(*slots));
    b->order_nodes = nodes;
    b->order_slots = slots;
    uint32_t *nh[2] = { h0, h1 };
    for (int h = 0; h < 2; h++) {
        memcpy(nh[h], b->dheap[h], b->dheap_len[h] * sizeof(*h0));
        bridge_free(&b->alloc, b->dheap[h], cap * sizeof(*h0));
        b->dheap[h] = nh[h];
    }
    memcpy(n, b->dstreams, cap * sizeof(*n));
    memset(n + cap, 0, cap * sizeof(*n));
    for (uint32_t i = cap; i < 2 * cap; i++) n[i].next_free = i + 1;
    bridge_free(&b->alloc, b->dstreams, cap * sizeof(*n));
    b->dstreams = n;
    b->dstream_cap = 2 * cap;
    return true;
}

/* A queue for `ref`; the table doubles rather than leave a stream unkeyed.
 * NULL (fatal latched) on allocation failure. */
static bridge_dstream_t *bridge_dstream_new(moq_transport_bridge_t *b,
                                            moq_stream_ref_t ref)
{
    if (b->dstream_free == b->dstream_cap && !bridge_dstreams_grow(b))
        return NULL;
    uint32_t slot = b->dstream_free;
    bridge_dstream_t *d = &b->dstreams[slot];
    b->dstream_free = d->next_free;
    memset(d, 0, sizeof(*d));
    d->active = true;
    d->ref = ref;
    d->heap_at = d->owner_prev = d->owner_next = BRIDGE_INDEX_EMPTY;
    bridge_index_insert(b->dref_index, b->dindex_mask, ref._v, slot);
    return d;
}

/* SET_DATA_PRIORITY: every keyed stream gets a queue slot. */
static moq_result_t dispatch_set_data_priority(moq_transport_bridge_t *b,
                                               moq_action_t *act)
{
    const moq_set_data_priority_action_t sp = act->u.set_data_priority;
    moq_action_cleanup(act);

    if (sp.stream_ref._v == 0) {
        for (uint32_t i = bridge_owner_first(b, sp.priority.owner);
             i != BRIDGE_INDEX_EMPTY; i = b->dstreams[i].owner_next) {
            bridge_dstream_t *d = &b->dstreams[i];
            if (d->meta.mode == MOQ_SCHED_MODE_FILL) continue;
            d->key.subscriber_priority = sp.priority.subscriber_priority;
            d->meta.subscriber_priority = sp.priority.subscriber_priority;
            bridge_dstream_requeue(b, d);
        }
        return bridge_dstreams_reorder(b);
    }
    bridge_dstream_t *d = bridge_dstream_find(b, sp.stream_ref);
    if (d) {
        if (d->owner_next != BRIDGE_INDEX_EMPTY)
            b->dstreams[d->owner_next].order_dirty = true;
        if (d->owner_prev != BRIDGE_INDEX_EMPTY)
            b->dstreams[d->owner_prev].order_dirty = true;
        bridge_owner_unlink(b, d);
    } else {
        d = bridge_dstream_new(b, sp.stream_ref);
        if (!d) return MOQ_ERR_NOMEM;
        d->pub = sp.priority.publisher_priority;
    }
    d->key = sp.priority;
    d->meta.owner = sp.priority.owner;
    d->meta.subscriber_priority = sp.priority.subscriber_priority;
    d->meta.publisher_priority = sp.priority.publisher_priority;
    d->meta.group_id = sp.priority.group_id;
    d->meta.subgroup_id = sp.priority.subgroup_id;
    d->meta.group_order = sp.priority.descending
        ? MOQ_GROUP_ORDER_DESCENDING : MOQ_GROUP_ORDER_ASCENDING;
    d->meta.mode = MOQ_SCHED_MODE_SUBGROUP; /* replaced by the data snapshot */
    d->meta.has_location = true;
    d->order_dirty = b->order_dirty = true;
    bridge_owner_link(b, d);
    bridge_dstream_requeue(b, d);
    return MOQ_OK;
}

#define BRIDGE_DQ_INITIAL_CAP 8u

/* Queue a SEND_DATA on its stream (ownership moves). Out of memory is fatal:
 * the action cannot be kept in order any other way. */
static moq_result_t bridge_dstream_push(moq_transport_bridge_t *b,
                                        bridge_dstream_t *d,
                                        moq_action_t *act,
                                        const moq_action_meta_t *meta)
{
    if (d->q_len == d->q_cap) {
        uint32_t ncap = d->q_cap ? d->q_cap * 2 : BRIDGE_DQ_INITIAL_CAP;
        if (d->q_cap > UINT32_MAX / 2 ||
            !bridge_array_fits(ncap, sizeof(*d->q))) {
            moq_action_cleanup(act);
            bridge_set_fatal(b, 0x1);
            return MOQ_ERR_NOMEM;
        }
        bridge_data_item_t *nq = bridge_alloc(&b->alloc, ncap * sizeof(*nq));
        if (!nq) {
            moq_action_cleanup(act);
            bridge_set_fatal(b, 0x1);
            return MOQ_ERR_NOMEM;
        }
        for (uint32_t i = 0; i < d->q_len; i++)
            nq[i] = d->q[(d->q_head + i) % d->q_cap];
        if (d->q) bridge_free(&b->alloc, d->q, d->q_cap * sizeof(*d->q));
        d->q = nq;
        d->q_cap = ncap;
        d->q_head = 0;
    }
    bridge_data_item_t *item = &d->q[(d->q_head + d->q_len) % d->q_cap];
    item->action = *act;
    item->meta = *meta;
    d->q_len++;
    b->dq_bytes += bridge_send_data_bytes(&act->u.send_data);
    b->dq_items++;
    if (meta->deadline_us != UINT64_MAX) b->dq_timed++;
    if (d->q_len == 1) bridge_dstream_requeue(b, d);
    return MOQ_OK;
}

/* Take `d`'s oldest queued action (ownership moves to *out). */
static void bridge_dstream_pop(moq_transport_bridge_t *b, bridge_dstream_t *d,
                               bridge_data_item_t *out)
{
    *out = d->q[d->q_head];
    d->q_head = (d->q_head + 1) % d->q_cap;
    d->q_len--;
    b->dq_bytes -= bridge_send_data_bytes(&out->action.u.send_data);
    b->dq_items--;
    if (out->meta.deadline_us != UINT64_MAX) b->dq_timed--;
}

/* Put `act` back as `d`'s oldest queued action (ownership moves back). */
static void bridge_dstream_unpop(moq_transport_bridge_t *b, bridge_dstream_t *d,
                                 const bridge_data_item_t *item)
{
    d->q_head = (d->q_head + d->q_cap - 1) % d->q_cap;
    d->q[d->q_head] = *item;
    d->q_len++;
    b->dq_bytes += bridge_send_data_bytes(&item->action.u.send_data);
    b->dq_items++;
    if (item->meta.deadline_us != UINT64_MAX) b->dq_timed++;
}

/* Drop all but `d`'s oldest `keep` queued actions. */
static void bridge_dstream_truncate(moq_transport_bridge_t *b,
                                    bridge_dstream_t *d, uint32_t keep)
{
    for (uint32_t i = keep; i < d->q_len; i++) {
        bridge_data_item_t *item = &d->q[(d->q_head + i) % d->q_cap];
        moq_action_t *a = &item->action;
        b->dq_bytes -= bridge_send_data_bytes(&a->u.send_data);
        b->dq_items--;
        if (item->meta.deadline_us != UINT64_MAX) b->dq_timed--;
        moq_action_cleanup(a);
    }
    d->q_len = keep;
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
 * `e` is the stream's entry when the caller already holds it, else NULL; the
 * bytes the endpoint accepts are counted on it (bytes_sent).
 *
 * Returns MOQ_OK or error.
 */
static void bridge_count_sent(moq_transport_bridge_t *b,
                              const bridge_pending_item_t *p,
                              bridge_stream_entry_t **e, uint64_t n)
{
    if (*e == NULL) *e = bridge_find_by_ref(b, p->stream_ref);
    if (*e != NULL) {
        bridge_dstream_t *d = bridge_dstream_find(b, p->stream_ref);
        if (d && n > 0) {
            /* The FETCH preamble has no object's publisher priority. The
             * first object's priority may also govern that earlier prefix;
             * subsequent objects must preserve already handed object bytes. */
            d->prefix_only = (*e)->bytes_sent == 0 &&
                p->kind == PENDING_HEADER_PAYLOAD &&
                bridge_session_is_fetch_header(b->session, &p->act.u.send_data);
            d->keyed = true;
        }
        if ((*e)->bytes_sent == 0 && p->kind == PENDING_HEADER_PAYLOAD)
            (*e)->subgroup_header_len =
                bridge_session_subgroup_header_size(b->session, &p->act.u.send_data);
        (*e)->bytes_sent += n;
        /* A pending retry can have entered the heap while its old header
         * was drained. Accepted payload bytes restore the held class now. */
        if (d) bridge_dstream_requeue(b, d);
    }
}

/* The endpoint refused `p`'s next write. The pump's write takes its action
 * back when none of it went out (`started` false); anything else is
 * retained. */
static moq_result_t bridge_send_refused(moq_transport_bridge_t *b,
                                        bridge_pending_item_t *p, bool started)
{
    if (b->pump_hand_back) b->pump_refused = true;
    if (b->pump_hand_back && !started) {
        b->pump_handed_back = true;
        return MOQ_OK;
    }
    if (!bridge_enqueue_pending(b, p))
        return bridge_retention_failed(b, p);
    memset(p, 0, sizeof(*p));
    return MOQ_OK;
}

static moq_result_t bridge_try_send_data_impl(moq_transport_bridge_t *b,
                                             bridge_pending_item_t *p,
                                             bridge_stream_entry_t *e)
{
    if (bridge_data_output_obsolete(b, p)) {
        bridge_cleanup_pending_item(&b->alloc, p);
        return MOQ_OK;
    }
    moq_send_data_action_t *sd = &p->act.u.send_data;
    uint64_t sid = p->stream_id;
    bool started = p->kind != PENDING_HEADER_PAYLOAD;

    /* Every SEND_DATA shape belongs to the stream's ordering domain. Check
     * before selecting a phase: a payload-only continuation or bare FIN must
     * not bypass an older retained header/payload action on the same stream. */
    if (bridge_domain_blocked(b, p->stream_ref, sid, p->has_stream_id)) {
        if (!bridge_enqueue_pending(b, p))
            return bridge_retention_failed(b, p);
        memset(p, 0, sizeof(*p));
        return MOQ_OK;
    }

    bridge_dstream_t *d = bridge_dstream_find(b, p->stream_ref);
    if (d) bridge_dstream_set_sid(b, d, sid);
    if (HAS_FIELD(b->ops, set_stream_priority) && b->ops->set_stream_priority) {
        bool has_priority = p->act.detail_size >=
            offsetof(moq_send_data_action_t, scheduling_priority) +
                sizeof(sd->scheduling_priority);
        uint8_t sub_prio = 128, pub_prio = d ? d->pub : 128;
        if (has_priority && sd->scheduling_priority) {
            sub_prio = (uint8_t)(sd->scheduling_priority >> 8);
            pub_prio = (uint8_t)sd->scheduling_priority;
        }
        /* The object about to go out sets the publisher priority (it can
         * change along a FETCH response). */
        uint32_t key = bridge_key(sub_prio, pub_prio, 0);
        bool call = true;
        if (d) {
            key = bridge_key(d->meta.subscriber_priority,
                              d->meta.publisher_priority, d->pos);
            /* Without the drained capability the endpoint may have dropped
             * the key with its emptied queue: key every write. */
            call = !bridge_drained_cap(b) || !d->keyed || key != d->applied;
        }
        if (call && b->ops->set_stream_priority(b->endpoint_ctx, sid, key) !=
                        MOQ_TRANSPORT_OK) {
            bridge_cleanup_pending_item(&b->alloc, p);
            bridge_set_fatal(b, 0x1);
            return MOQ_ERR_INTERNAL;
        }
        if (d) {
            d->applied = key;
            d->keyed = true;
        }
    }

    /* Phase 1: send header (if any) */
    if (p->kind == PENDING_HEADER_PAYLOAD && sd->header_len > 0) {
        size_t pay_len = sd->payload ? moq_rcbuf_len(sd->payload) : 0;
        bool fin_on_hdr = (pay_len == 0) && sd->fin;

        moq_transport_result_t wr = sanitize_stream_result(
            b->ops->write(b->endpoint_ctx, sid,
                          sd->header, sd->header_len, fin_on_hdr));

        if (wr == MOQ_TRANSPORT_WOULD_BLOCK)
            return bridge_send_refused(b, p, started);
        if (wr == MOQ_TRANSPORT_ERROR) {
            bridge_cleanup_pending_item(&b->alloc, p);
            bridge_set_fatal(b, 0x1);
            return MOQ_ERR_INTERNAL;
        }
        bridge_count_sent(b, p, &e, sd->header_len);
        started = true;

        if (fin_on_hdr) {
            moq_stream_ref_t ref = p->stream_ref;
            bridge_local_data_fin_done(b, ref);
            bridge_cleanup_pending_item(&b->alloc, p);
            return bridge_dstream_fin(b, ref);
        }

        p->kind = PENDING_PAYLOAD_ONLY;
    }

    /* Phase 2: send payload */
    if (bridge_data_output_obsolete(b, p)) {
        bridge_cleanup_pending_item(&b->alloc, p);
        return MOQ_OK;
    }
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

        if (wr == MOQ_TRANSPORT_WOULD_BLOCK)
            return bridge_send_refused(b, p, started);
        if (wr == MOQ_TRANSPORT_ERROR) {
            bridge_cleanup_pending_item(&b->alloc, p);
            bridge_set_fatal(b, 0x1);
            return MOQ_ERR_INTERNAL;
        }
        bridge_count_sent(b, p, &e, moq_rcbuf_len(sd->payload));
    } else if (sd->header_len == 0 && sd->fin) {
        /* Empty FIN-only (close subgroup) */
        moq_transport_result_t wr = sanitize_stream_result(
            b->ops->write(b->endpoint_ctx, sid, NULL, 0, true));

        if (wr == MOQ_TRANSPORT_WOULD_BLOCK)
            return bridge_send_refused(b, p, started);
        if (wr == MOQ_TRANSPORT_ERROR) {
            bridge_cleanup_pending_item(&b->alloc, p);
            bridge_set_fatal(b, 0x1);
            return MOQ_ERR_INTERNAL;
        }
    }

    /* Success — stream done if FIN */
    bool fin = sd->fin;
    moq_stream_ref_t ref = p->stream_ref;
    if (fin)
        bridge_local_data_fin_done(b, ref);
    bridge_cleanup_pending_item(&b->alloc, p);
    return fin ? bridge_dstream_fin(b, ref) : MOQ_OK;
}

/* A retry is an in-flight representative too: an earlier header might have
 * drained since refusal. Keep that action's class in the graph until its
 * payload is handed over, instead of exposing the next queued object. */
static moq_result_t bridge_try_send_data(moq_transport_bridge_t *b,
                                         bridge_pending_item_t *p,
                                         bridge_stream_entry_t *e)
{
    const moq_stream_ref_t ref = p->stream_ref;
    bridge_dstream_t *d = bridge_dstream_find(b, ref);
    if (!d || bridge_domain_blocked(b, ref, p->stream_id, p->has_stream_id))
        return bridge_try_send_data_impl(b, p, e);
    const bridge_data_item_t item = { .action = p->act, .meta = p->meta };
    d->meta = bridge_dstream_item_meta(d, &item);
    d->pub = d->meta.publisher_priority;
    d->dispatching = true;
    bridge_dstream_set_sid(b, d, p->stream_id);
    bridge_dstream_requeue(b, d);
    moq_result_t rc = bridge_dstreams_reorder(b);
    if (rc < 0) {
        bridge_cleanup_pending_item(&b->alloc, p);
        return rc;
    }
    rc = bridge_try_send_data_impl(b, p, e);
    d = bridge_dstream_find(b, ref);
    if (d) {
        d->dispatching = false;
        bridge_dstream_requeue(b, d);
    }
    return rc;
}

/* -- Ordering domains ------------------------------------------------
 *
 * poll_actions is destructive: the bridge owns an action the moment it
 * polls one, so it cannot look at an action's ordering domain and then
 * decline to take it. What it can do is RETAIN it. These helpers decide,
 * after the poll and before any endpoint side effect, whether an action
 * must be retained behind older pending work.
 *
 * A domain is a canonical transport stream. Items name it inconsistently:
 * some carry a stream_ref, some only the transport stream id, some
 * neither. `stream_ref._v == 0` is the established "no ref" sentinel (a
 * ref of 0 is refused at the public entry points), so it must never be
 * treated as a stream that other refless items share. Where one side
 * names a ref and the other an id, the stream map supplies the missing
 * half.
 *
 * Three classes:
 *   STREAM     conflicts only with older work for the same stream;
 *   CONN_CTRL  the control backlog -- connection-wide order, so it
 *              blocks later ordinary work;
 *   CONN_TERM  transport close -- keeps top priority and is never
 *              displaced.
 * Anything unrecognised is treated as connection-wide, so a new kind
 * fails closed instead of silently bypassing.
 */
/*
 * A candidate names a stream by ref, by transport id, or by both. Candidate
 * identity is never completed from the stream map: pending identity is
 * normalised exactly once, at the enqueue boundary, and that is the only
 * place it happens. The shapes the dispatchers deliberately provide are a
 * ref alone (a stream about to be opened), or a ref together with an
 * explicit id.
 *
 * A candidate that names neither is unresolvable. Connection-level and
 * terminal work reaches the classifier through its own explicit domain, not
 * by having no identity, so an unresolvable stream candidate is a caller
 * bug. Rather than let it compare equal to nothing and bypass every queued
 * item, it fails closed: the bridge is marked fatal with its own code and
 * the candidate is reported blocked.
 */
static bool bridge_candidate_unresolvable(moq_transport_bridge_t *b,
                                           moq_stream_ref_t ref, bool have_sid)
{
    if (ref._v != 0 || have_sid)
        return false;
    bridge_set_fatal(b, 0x4);
    return true;
}

/*
 * Is a candidate action for this stream blocked by older pending work?
 *
 * Scans the pending FIFO, which is structurally bounded by max_pending
 * (BRIDGE_DEFAULT_MAX_PENDING, 64) because bridge_enqueue_pending refuses
 * past it -- so this is O(max_pending) and independent of the number of
 * connections or subscribers. Retry passes see only the survivors already
 * re-enqueued ahead of the item being retried, so the same predicate
 * serves both drain and retry without special-casing.
 */
#ifdef MOQ_BRIDGE_SCAN_COUNTERS
/* Test-only operation counters for the conflict scan. Not present in any
 * default build: no symbol, no storage, nothing to export. */
unsigned long long moq_bridge_scan_items;
unsigned long long moq_bridge_scan_lookups;
/* Largest number of pending slots a single drained action has ever consumed.
 * The capacity guard's one-slot margin is only sound while this is 1. */
uint32_t moq_bridge_max_retain_delta;
#define MOQ_SCAN_TICK(x) ((x)++)
#else
#define MOQ_SCAN_TICK(x) ((void)0)
#endif

static bool bridge_domain_blocked(moq_transport_bridge_t *b,
                                  moq_stream_ref_t ref, uint64_t sid,
                                  bool have_sid)
{
    uint64_t cref = ref._v;
    uint64_t csid = sid;
    bool chave = have_sid;

    MOQ_SCAN_TICK(moq_bridge_scan_lookups);
    /* Fail closed on a shape no dispatcher produces. */
    if (bridge_candidate_unresolvable(b, ref, have_sid))
        return true;

    for (uint32_t i = 0; i < b->pending_count; i++) {
        const bridge_pending_item_t *p = &b->pending[i];

        /* Counted, and deliberately NOT paired with a lookup tick: the whole
         * point of normalising at enqueue is that no map lookup happens in
         * here, and the counters are what prove it. */
        MOQ_SCAN_TICK(moq_bridge_scan_items);

        /* Connection-wide backlog and a pending transport close both order
         * ahead of ordinary stream work. */
        if ((bridge_domain_t)p->dom != BRIDGE_DOM_STREAM)
            return true;

        /* Field comparisons only. A zero ref names nothing, so two refless
         * items are not thereby the same stream. */
        if (cref != 0 && p->canon_ref != 0 && cref == p->canon_ref)
            return true;
        if (chave && p->canon_have_sid && csid == p->canon_sid)
            return true;
    }
    return false;
}

#ifdef MOQ_BRIDGE_TEST_INJECT
/*
 * Test-only seams. Absent from every default build: no symbol, no storage.
 * They exist so a fixture can build valid pending depths and ask the real
 * classifier its decision, rather than inferring it from side effects.
 *
 * inject goes through bridge_enqueue_pending, so the item is normalised by the
 * production canonicalisation boundary and the structural maximum applies --
 * an injected depth is a real depth.
 */
bool moq_bridge_test_inject_pending_ex(moq_transport_bridge_t *b, int kind,
                                       uint64_t ref, uint64_t sid,
                                       bool have_sid)
{
    bridge_pending_item_t p;

    if (b == NULL) return false;
    memset(&p, 0, sizeof(p));
    p.kind = (bridge_pending_kind_t)kind;
    p.stream_ref = moq_stream_ref_from_u64(ref);
    if (have_sid)
        bridge_item_set_sid(&p, sid);
    return bridge_enqueue_pending(b, &p);
}

bool moq_bridge_test_inject_pending(moq_transport_bridge_t *b, int kind,
                                    uint64_t ref, uint64_t sid)
{
    /* Legacy shape: an id of 0 means "no id" only for callers that pass it
     * that way. Explicit callers use the _ex form. */
    return moq_bridge_test_inject_pending_ex(b, kind, ref, sid, sid != 0);
}

bool moq_bridge_test_domain_blocked(moq_transport_bridge_t *b, uint64_t ref,
                                    uint64_t sid, bool have_sid)
{
    return bridge_domain_blocked(b, moq_stream_ref_from_u64(ref), sid,
                                  have_sid);
}

/* The ref the map currently gives this transport id, or 0 if none. Lets a
 * fixture name a live stream the way a dispatcher would. */
uint64_t moq_bridge_test_ref_for_id(moq_transport_bridge_t *b, uint64_t sid)
{
    const bridge_stream_entry_t *e = b ? bridge_find_by_id(b, sid) : NULL;
    return (e != NULL && e->active) ? e->ref._v : 0u;
}

uint32_t moq_bridge_test_pending_depth(const moq_transport_bridge_t *b)
{
    return b != NULL ? b->pending_count : 0u;
}

int moq_bridge_test_pending_domain(const moq_transport_bridge_t *b,
                                   uint32_t idx)
{
    if (b == NULL || idx >= b->pending_count) return -1;
    return (int)b->pending[idx].dom;
}

void moq_bridge_test_pending_identity(const moq_transport_bridge_t *b,
                                      uint32_t idx, uint64_t *out_ref,
                                      uint64_t *out_sid, bool *out_have_sid)
{
    if (b == NULL || idx >= b->pending_count) return;
    if (out_ref) *out_ref = b->pending[idx].canon_ref;
    if (out_sid) *out_sid = b->pending[idx].canon_sid;
    if (out_have_sid) *out_have_sid = b->pending[idx].canon_have_sid;
}
#endif /* MOQ_BRIDGE_TEST_INJECT */

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
                                        moq_action_t *act,
                                        const moq_action_meta_t *meta)
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
        moq_transport_result_t r =
            bridge_domain_blocked(b, ref, 0, false)
                ? MOQ_TRANSPORT_WOULD_BLOCK
                : sanitize_stream_result(
                      b->ops->open_uni(b->endpoint_ctx, &uni_id));

        if (r == MOQ_TRANSPORT_WOULD_BLOCK) {
            bridge_deactivate_stream(b, e);
            bridge_pending_item_t p;
            memset(&p, 0, sizeof(p));
            p.kind = PENDING_OPEN_UNI_DATA;
            p.stream_ref = ref;
            p.meta = *meta;
            bridge_pending_take_action(&p, act);
            bridge_dstream_t *d = bridge_dstream_find(b, ref);
            if (d) {
                const bridge_data_item_t item = { .action = p.act, .meta = p.meta };
                d->meta = bridge_dstream_item_meta(d, &item);
                d->pub = d->meta.publisher_priority;
            }
            if (!bridge_enqueue_pending(b, &p))
                return bridge_retention_failed(b, &p);
            return MOQ_OK;
        }
        if (r == MOQ_TRANSPORT_ERROR) {
            bridge_deactivate_stream(b, e);
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
    p.meta = *meta;
    bridge_item_set_sid(&p, e->transport_id);
    bridge_pending_take_action(&p, act);

    moq_result_t rc = bridge_try_send_data(b, &p, e);
    if (rc < 0) return rc;
    if (b->pump_handed_back) *act = p.act;   /* the pump takes it back */

    /* If item was not fully consumed, it's already in pending queue */
    return MOQ_OK;
}

/* -- RESET_DATA / STOP_DATA dispatch -------------------------------- */

/*
 * Reset a local unidirectional data stream, keeping its first `reliable`
 * bytes when the endpoint offers reset_stream_at and this connection can use
 * it (RESET_STREAM_AT). Otherwise -- no capability, a reliable size of 0, or
 * the endpoint answering DROPPED -- a plain RESET_STREAM, which the protocol
 * is designed to work with.
 */
static moq_transport_result_t bridge_reset_uni(moq_transport_bridge_t *b,
                                               uint64_t sid, uint64_t error_code,
                                               uint64_t reliable)
{
    if (reliable > 0 &&
        (b->ops->capabilities & MOQ_TRANSPORT_CAP_RELIABLE_RESET) &&
        HAS_FIELD(b->ops, reset_stream_at) && b->ops->reset_stream_at) {
        moq_transport_result_t r =
            b->ops->reset_stream_at(b->endpoint_ctx, sid, error_code, reliable);
        if (r != MOQ_TRANSPORT_DROPPED)
            return sanitize_stream_result(r);
    }
    return sanitize_stream_result(
        b->ops->reset_stream(b->endpoint_ctx, sid, error_code));
}

/* Bytes the retained SEND_DATA work of one stream will hand the endpoint:
 * writes are all-or-nothing, so a retained header or payload goes out whole
 * ahead of anything queued behind it on that stream. */
static uint64_t bridge_pending_stream_bytes(const moq_transport_bridge_t *b,
                                            moq_stream_ref_t ref, uint64_t sid,
                                            bool have_sid)
{
    uint64_t n = 0;
    for (uint32_t i = 0; i < b->pending_count; i++) {
        const bridge_pending_item_t *p = &b->pending[i];
        bool same = (ref._v != 0 && p->canon_ref == ref._v) ||
                    (have_sid && p->canon_have_sid && p->canon_sid == sid);
        if (!same) continue;
        const moq_send_data_action_t *sd = &p->act.u.send_data;
        uint64_t pay = sd->payload ? moq_rcbuf_len(sd->payload) : 0;
        if (p->kind == PENDING_OPEN_UNI_DATA || p->kind == PENDING_HEADER_PAYLOAD)
            n += sd->header_len + pay;
        else if (p->kind == PENDING_PAYLOAD_ONLY)
            n += pay;
    }
    return n;
}

/* Reset of a queued stream. What it queues beyond the reset's reliable size
 * (writes are whole) is dropped now; a prefix that size still needs keeps the
 * stream queued, and the reset follows it in the stream's turn (*deferred).
 * Otherwise the stream leaves the order and the reset goes out now. */
static moq_result_t bridge_dstream_reset(moq_transport_bridge_t *b,
                                         moq_stream_ref_t ref,
                                         uint64_t error_code, uint64_t reliable,
                                         bool terminal,
                                         bool *deferred)
{
    *deferred = false;
    bridge_dstream_t *d = bridge_dstream_find(b, ref);
    if (!d) return MOQ_OK;
    if (d->has_reset) {
        *deferred = d->q_len > 0;
        return *deferred ? MOQ_OK : bridge_dstream_free(b, d);
    }
    const bridge_stream_entry_t *e = bridge_find_by_ref(b, ref);
    uint64_t handed = e ? e->bytes_sent : 0;
    if (d->q_len > 0 && reliable > handed)
        handed += bridge_pending_stream_bytes(b, ref, e ? e->transport_id : 0,
                                              e != NULL);
    uint32_t keep = 0;
    for (; keep < d->q_len && reliable > handed; keep++)
        handed += bridge_send_data_bytes(
            &d->q[(d->q_head + keep) % d->q_cap].action.u.send_data);
    if (keep == 0) return bridge_dstream_free(b, d);
    bridge_dstream_truncate(b, d, keep);
    d->has_reset = true;
    b->dreset_count++;
    d->reset_terminal = terminal;
    d->reset_code = error_code;
    d->reset_reliable = reliable;
    *deferred = true;
    return MOQ_OK;
}

static moq_result_t dispatch_reset_data(moq_transport_bridge_t *b,
                                         moq_action_t *act,
                                         const moq_action_meta_t *meta)
{
    /* RESET_DATA targets a unidirectional data stream; RESET_BIDI_STREAM the
     * local send half of a request bidi. Both reset a stream by ref with an
     * error code, so they share this dispatcher (reading the right field) --
     * but their lifecycle differs, see below. */
    const bool is_bidi = (act->kind == MOQ_ACTION_RESET_BIDI_STREAM);
    bool terminal = !is_bidi && meta && meta->outgoing_terminal;
    moq_stream_ref_t ref = is_bidi
        ? act->u.reset_bidi_stream.stream_ref : act->u.reset_data.stream_ref;
    bridge_dstream_t *d = is_bidi ? NULL : bridge_dstream_find(b, ref);
    const uint64_t owner = d ? d->key.owner : (meta ? meta->owner : 0);
    uint64_t error_code = is_bidi
        ? act->u.reset_bidi_stream.error_code : act->u.reset_data.error_code;
    /* The reliable size the session asks for, read only where the action's
     * detail carries it; an older producer's reset is a plain one. */
    uint64_t reliable = 0;
    if (!is_bidi &&
        act->detail_size >= offsetof(moq_reset_data_action_t, reliable_size) +
                            sizeof(act->u.reset_data.reliable_size))
        reliable = act->u.reset_data.reliable_size;
    moq_action_cleanup(act);

    if (!is_bidi) {
        /* A reset retained behind its reliable prefix owns the first cause,
         * even if the core slot has since reaped and another reset arrives. */
        const bridge_dstream_t *d = bridge_dstream_find(b, ref);
        if (d && d->has_reset) {
            error_code = d->reset_code;
            reliable = d->reset_reliable;
            terminal = d->reset_terminal;
        }
        bool deferred;
        moq_result_t frc = bridge_dstream_reset(b, ref, error_code, reliable, terminal,
                                                &deferred);
        if (frc < 0 || deferred) return frc;
    }

    bridge_stream_entry_t *e = bridge_find_by_ref(b, ref);
    if (!e) {
        /* The stream has no mapping yet. If its open is still retained
         * (an open_uni that returned WOULD_BLOCK), the reset waits behind it
         * and resolves the stream once that work has run: the stream header
         * goes out before the reset, which then keeps it. A stream with no
         * retained work was never opened and has nothing to reset. */
        if (is_bidi) return MOQ_OK;
        uint64_t queued = bridge_pending_stream_bytes(b, ref, 0, false);
        if (queued == 0) return MOQ_OK;
        bridge_pending_item_t p;
        memset(&p, 0, sizeof(p));
        p.kind = PENDING_RESET_STREAM;
        p.stream_ref = ref;
        p.meta.owner = owner;
        p.error_code = error_code;
        p.uni_reset = true;
        p.meta.outgoing_terminal = terminal;
        p.reliable_size = reliable < queued ? reliable : queued;
        if (!bridge_enqueue_pending(b, &p))
            return bridge_retention_failed(b, &p);
        return MOQ_OK;
    }

    uint64_t sid = e->transport_id;
    if (!is_bidi && terminal) e->data_terminal_received = true;
    if (!is_bidi && e->fin_reset_pending) return MOQ_OK;
    /* Our send half was already reset in answer to the peer's STOP_SENDING:
     * one reset per half, never a second. */
    if (is_bidi && (e->peer_stop_received || e->local_cancel_queued)) {
        bridge_retire_or_tombstone(b, ref);
        return MOQ_OK;
    }
    const bool retain_reset_mapping = !is_bidi &&
        ((e->awaiting_commit && e->subgroup_header_len != 0) ||
         e->data_terminal_received || e->peer_stop_received ||
         bridge_session_has_data_terminal(b->session, ref));

    /* A RESET_STREAM closes only OUR sending direction (RFC 9000 §3.5). For a
     * request bidi the peer's sending direction stays independently capable of
     * FIN/RESET, so we must NOT destroy the mapping here. We mark our send half
     * closed only after the endpoint accepts the reset, and let
     * bridge_retire_or_tombstone() keep the dual-id mapping alive while the peer
     * terminal is still owed (moq_session_stream_awaits_peer_terminal(), e.g. a
     * NORMAL drain reference). The peer's later RESET then reaches
     * moq_session_on_bidi_stream_reset(), which releases the exact drain
     * reference and retires the mapping exactly once.
     *
     * A subgroup FIN awaiting commit stays mapped through a blocked reset.
     * Other unidirectional RESET_DATA lifetimes are unchanged. */
    /* A reliable size never exceeds what the stream is handed before the
     * reset: the bytes the endpoint accepted plus any SEND_DATA still
     * retained for it, which goes out first (per-stream order). */
    if (!is_bidi) {
        uint64_t handed = e->bytes_sent +
                          bridge_pending_stream_bytes(b, ref, sid, true);
        if (reliable > handed) reliable = handed;
        if (retain_reset_mapping) {
            e->awaiting_commit = false;
            e->fin_reset_pending = true;
        } else {
            bridge_deactivate_stream(b, e);
        }
    }

    moq_transport_result_t r =
        /* sid came from the live entry above, so it is known */
        bridge_domain_blocked(b, ref, sid, true)
            ? MOQ_TRANSPORT_WOULD_BLOCK
        : is_bidi
            ? sanitize_stream_result(
                  b->ops->reset_stream(b->endpoint_ctx, sid, error_code))
            : bridge_reset_uni(b, sid, error_code, reliable);

    if (r == MOQ_TRANSPORT_WOULD_BLOCK) {
        bridge_pending_item_t p;
        memset(&p, 0, sizeof(p));
        p.kind = PENDING_RESET_STREAM;
        bridge_item_set_sid(&p, sid);
        p.meta.owner = owner;
        p.error_code = error_code;
        p.uni_reset = !is_bidi;
        p.meta.outgoing_terminal = terminal;
        p.reliable_size = reliable;
        /* Defer the bidi local-close/retire decision until the endpoint has
         * actually accepted the reset -- never before. Carry the ref so the
         * retry applies it exactly once against the still-live mapping. A
         * retained subgroup FIN uses the same accepted-reset boundary. */
        p.stream_ref = ref;
        if (!bridge_enqueue_pending(b, &p))
            return bridge_retention_failed(b, &p);
        return MOQ_OK;
    }
    if (r == MOQ_TRANSPORT_ERROR) {
        bridge_set_fatal(b, 0x1);
        return MOQ_ERR_INTERNAL;
    }
    if (is_bidi || retain_reset_mapping) {
        bridge_mark_local_close(b, ref);
        bridge_retire_or_tombstone(b, ref);
    }
    return MOQ_OK;
}

/* -- Local receive retirement ---------------------------------------- *
 * A peer data stream the session stopped is released once the endpoint has
 * accepted that STOP and the adapter reports the transport finished with the
 * stream: after a local receive abort some transports never surface the
 * peer's FIN or RESET, so without this the mapping and the session's stopped
 * owner would wait forever. The release claims no FIN, no reset and no
 * delivery. It is bounded by the stream map itself: two bits per entry. */

/* A peer-origin unidirectional data stream: not control, not aborting. */
static bool bridge_is_peer_data_receive(const bridge_stream_entry_t *e)
{
    return e->active && e->kind == BRIDGE_STREAM_UNI &&
           e->origin == BRIDGE_ORIGIN_PEER &&
           e->uni_disp != BRIDGE_UNI_DISP_CONTROL && !e->aborting;
}

/* The endpoint accepted the session's STOP_DATA for this stream. */
static void bridge_note_data_stop_accepted(moq_transport_bridge_t *b,
                                           bridge_stream_entry_t *e)
{
    if (!bridge_is_peer_data_receive(e)) return;
    e->local_stop_accepted = true;
    if (e->transport_final) b->local_retire_due = true;
}

/* Inbound obligations this bridge still owes the session for the stream: the
 * real FIN or RESET (and any bytes before it) go first and settle the stream
 * themselves; an admission-held stream was never seen by the session. */
static bool bridge_inbound_owed(const bridge_stream_entry_t *e)
{
    return e->pending_retry || e->pending_fin || e->fin_retained ||
           e->pending_reset || e->pending_stop || e->held_input ||
           e->held_payload || e->pending_admission;
}

/* Our request-cancellation policy after the request has ended: when the
 * peer's accepted RESET left the request with no owner at the session and our
 * own send half is still open with no output or close of its own pending,
 * cancel that half too -- a reset with CANCELLED, queued through the ordinary
 * pending machinery (never issued from the endpoint's callback). Draft-18
 * §3.3.2 and draft-22 §6.4.2.3: a request is cancelled by abruptly
 * terminating any direction still open. This is that policy, not a QUIC rule
 * that one direction's RESET resets the other. A FIN or close already owed
 * keeps its order; a live owner (a retained response, deferred teardown) is
 * waited for. A cancellation needs one retention slot; with none free it stays
 * due (the stream keeps its mapping and the pass yields), exactly like an
 * action the drain step does not poll at saturation, and is queued once the
 * retained work ahead of it drains. Sets *queued when a reset was queued. */
static moq_result_t bridge_settle_bidi_cancel(moq_transport_bridge_t *b,
                                              bool *queued)
{
    b->bidi_cancel_due = false;
    for (uint32_t i = 0; i < b->max_streams; i++) {
        bridge_stream_entry_t *e = &b->streams[i];
        if (!e->active || e->kind != BRIDGE_STREAM_BIDI || !e->peer_reset_accepted ||
            e->local_send_closed || e->local_cancel_queued || e->peer_stop_received ||
            e->aborting)
            continue;
        if (bridge_pending_touches_stream(b, e) ||
            !bridge_session_bidi_owner_ended(b->session, e->ref)) {
            b->bidi_cancel_due = true;     /* decided once they settle */
            continue;
        }
        if (b->pending_count >= b->max_pending) {
            b->bidi_cancel_due = true;     /* backpressure, not a failure */
            break;
        }
        bridge_pending_item_t p;
        memset(&p, 0, sizeof(p));
        p.kind = PENDING_RESET_STREAM;
        bridge_item_set_sid(&p, e->transport_id);
        p.stream_ref = e->ref;            /* the retry closes our half here */
        p.error_code = MOQ_STREAM_ERROR_CANCELLED;
        if (!bridge_enqueue_pending(b, &p))
            return bridge_retention_failed(b, &p);
        e->local_cancel_queued = true;
        *queued = true;
    }
    return MOQ_OK;
}

/* Service-pass step: release stopped data owners or aborted request drain
 * identities once their terminal and remaining obligations permit it. */
static moq_result_t bridge_settle_local_retire(moq_transport_bridge_t *b,
                                               uint64_t now_us,
                                               bool *released)
{
    b->local_retire_due = false;
    for (uint32_t i = 0; i < b->max_streams; i++) {
        bridge_stream_entry_t *e = &b->streams[i];
        bool aborted_bidi = e->active && e->kind == BRIDGE_STREAM_BIDI &&
                            e->aborting;
        if (!e->transport_final) continue;
        if (!aborted_bidi &&
            (!bridge_is_peer_data_receive(e) || !e->local_stop_accepted))
            continue;
        if (aborted_bidi ? bridge_pending_touches_stream(b, e)
                         : bridge_inbound_owed(e)) {
            b->local_retire_due = true;   /* recheck once they settle */
            continue;
        }
        moq_result_t rc = aborted_bidi
            ? bridge_session_on_bidi_retired(b->session, e->ref, now_us)
            : bridge_session_on_data_retired(b->session, e->ref, now_us);
        if (rc == MOQ_OK || rc == MOQ_ERR_STALE_HANDLE) {
            bridge_deactivate_stream(b, e);
            *released = true;
        } else if (rc == MOQ_SESSION_SUSPENDED || rc == MOQ_ERR_WOULD_BLOCK) {
            b->local_retire_due = true;
            return rc == MOQ_SESSION_SUSPENDED ? rc : MOQ_OK;
        } else if (rc == MOQ_ERR_CLOSED) {
            return MOQ_OK;
        } else if (rc == MOQ_ERR_WRONG_STATE && aborted_bidi) {
            b->local_retire_due = true;   /* live owner or queued action */
        } else if (rc != MOQ_ERR_WRONG_STATE) {
            /* WRONG_STATE: a live owner the report must not release; it is
             * left as it is. Anything else is an invariant break. */
            bridge_set_fatal(b, 0x1);
            return rc;
        }
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
    bool data_stop = act->kind == MOQ_ACTION_STOP_DATA;
    moq_action_cleanup(act);

    bridge_stream_entry_t *e = bridge_find_by_ref(b, ref);
    if (!e) return MOQ_OK;

    moq_transport_result_t r =
        bridge_domain_blocked(b, e->ref, e->transport_id, true)
            ? MOQ_TRANSPORT_WOULD_BLOCK
            : sanitize_stream_result(
                  b->ops->stop_sending(b->endpoint_ctx, e->transport_id,
                                        error_code));

    if (r == MOQ_TRANSPORT_WOULD_BLOCK) {
        bridge_pending_item_t p;
        memset(&p, 0, sizeof(p));
        p.kind = PENDING_STOP_SENDING;
        bridge_item_set_sid(&p, e->transport_id);
        p.error_code = error_code;
        p.data_stop = data_stop;
        if (!bridge_enqueue_pending(b, &p))
            return bridge_retention_failed(b, &p);
        return MOQ_OK;
    }
    if (r == MOQ_TRANSPORT_ERROR) {
        bridge_set_fatal(b, 0x1);
        return MOQ_ERR_INTERNAL;
    }
    if (data_stop) bridge_note_data_stop_accepted(b, e);
    return MOQ_OK;
}

/*
 * Whole-request teardown: ONE transaction aborting both halves with one
 * code. Keeps the discard mapping until service settles its terminal, using
 * either the endpoint's native abort_stream, or the sequential reset+stop
 * fallback with EXPLICIT pending state: a WOULD_BLOCK before the RESET was
 * accepted retries the whole abort; a WOULD_BLOCK after it retries only
 * the remaining STOP half. A runtime failure after partial application
 * is fatal — never rolled back.
 */
static moq_result_t bridge_abort_apply(moq_transport_bridge_t *b,
                                       uint64_t sid, uint64_t error_code,
                                       bool reset_done);

static moq_result_t dispatch_abort_bidi(moq_transport_bridge_t *b,
                                        moq_action_t *act)
{
    moq_stream_ref_t ref = act->u.abort_bidi_stream.stream_ref;
    uint64_t error_code = act->u.abort_bidi_stream.error_code;
    moq_action_cleanup(act);

    bridge_stream_entry_t *e = bridge_find_by_ref(b, ref);
    if (!e) return MOQ_OK;

    uint64_t sid = e->transport_id;
    /*
     * KEEP the entry, marked aborting: its ref mapping stays live so late
     * peer bytes are discarded (not handed a fresh inbound ref), and it
     * is retired by service after a genuine FIN / RESET / stream terminal
     * and completion of outbound obligations. Clear inbound pending state
     * after preserving any real terminal — we are tearing the stream down. */
    bool peer_terminal = e->peer_send_closed || e->peer_reset_accepted ||
                         e->pending_fin || e->fin_retained || e->pending_reset;
    e->aborting = true;
    e->pending_retry = false;
    e->pending_fin = false;
    e->fin_retained = false;
    e->pending_reset = false;
    e->pending_stop = false;
    /* A peer half already ended before its abort action was dispatched needs
     * no second terminal. A retained FIN/RESET is genuine too, but is settled
     * only as the local abort's drain identity, never delivered to the core. */
    if (peer_terminal) bridge_note_aborting_terminal(b, e);
    return bridge_abort_apply(b, sid, error_code, false);
}

static moq_result_t bridge_abort_apply(moq_transport_bridge_t *b,
                                       uint64_t sid, uint64_t error_code,
                                       bool reset_done)
{
    if (HAS_FIELD(b->ops, abort_stream) && b->ops->abort_stream != NULL &&
        !reset_done) {
        moq_transport_result_t r = sanitize_stream_result(
            b->ops->abort_stream(b->endpoint_ctx, sid, error_code));
        if (r == MOQ_TRANSPORT_WOULD_BLOCK) {
            bridge_pending_item_t p;
            memset(&p, 0, sizeof(p));
            p.kind = PENDING_ABORT_STREAM;
            bridge_item_set_sid(&p, sid);
            p.error_code = error_code;
            if (!bridge_enqueue_pending(b, &p))
                return bridge_retention_failed(b, &p);
            return MOQ_OK;
        }
        if (r == MOQ_TRANSPORT_ERROR) {
            bridge_set_fatal(b, 0x1);
            return MOQ_ERR_INTERNAL;
        }
        return MOQ_OK;
    }

    /* sequential fallback */
    if (!reset_done) {
        moq_transport_result_t r = sanitize_stream_result(
            b->ops->reset_stream(b->endpoint_ctx, sid, error_code));
        if (r == MOQ_TRANSPORT_WOULD_BLOCK) {
            bridge_pending_item_t p;
            memset(&p, 0, sizeof(p));
            p.kind = PENDING_ABORT_STREAM; /* nothing applied yet */
            bridge_item_set_sid(&p, sid);
            p.error_code = error_code;
            if (!bridge_enqueue_pending(b, &p))
                return bridge_retention_failed(b, &p);
            return MOQ_OK;
        }
        if (r == MOQ_TRANSPORT_ERROR) {
            bridge_set_fatal(b, 0x1);
            return MOQ_ERR_INTERNAL;
        }
    }
    {
        moq_transport_result_t r = sanitize_stream_result(
            b->ops->stop_sending(b->endpoint_ctx, sid, error_code));
        if (r == MOQ_TRANSPORT_WOULD_BLOCK) {
            /* the RESET half was accepted: retry ONLY the STOP half */
            bridge_pending_item_t p;
            memset(&p, 0, sizeof(p));
            p.kind = PENDING_STOP_SENDING;
            bridge_item_set_sid(&p, sid);
            p.error_code = error_code;
            if (!bridge_enqueue_pending(b, &p))
                return bridge_retention_failed(b, &p);
            return MOQ_OK;
        }
        if (r == MOQ_TRANSPORT_ERROR) {
            /* partial application (RESET landed, STOP failed): fatal */
            bridge_set_fatal(b, 0x1);
            return MOQ_ERR_INTERNAL;
        }
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
    moq_transport_result_t r =
        bridge_domain_blocked(b, ref, 0, false)
            ? MOQ_TRANSPORT_WOULD_BLOCK
            : sanitize_stream_result(
                  b->ops->open_bidi(b->endpoint_ctx, &bidi_id));

    if (r == MOQ_TRANSPORT_WOULD_BLOCK) {
        bridge_deactivate_stream(b, e);
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
        if (!bridge_enqueue_pending(b, &p))
            return bridge_retention_failed(b, &p);
        return MOQ_OK;
    }
    if (r == MOQ_TRANSPORT_ERROR) {
        bridge_deactivate_stream(b, e);
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
            bridge_item_set_sid(&p, bidi_id);
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
            if (!bridge_enqueue_pending(b, &p))
                return bridge_retention_failed(b, &p);
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
            bridge_item_set_sid(&p, bidi_id);
            p.stream_ref = ref;
            moq_action_cleanup(act);
            if (!bridge_enqueue_pending(b, &p))
                return bridge_retention_failed(b, &p);
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
    /* A peer STOP_SENDING reset this send half, so anything the session still
     * wants to write on it is obsolete. An ordinary FIN is left alone: that
     * path's existing behaviour is not this correction's business. */
    if (e->peer_stop_received) {
        moq_action_cleanup(act);
        return MOQ_OK;
    }

    moq_transport_result_t wr = sanitize_stream_result(
        b->ops->write(b->endpoint_ctx, e->transport_id, data, len, fin));

    if (wr == MOQ_TRANSPORT_WOULD_BLOCK) {
        bridge_pending_item_t p;
        memset(&p, 0, sizeof(p));
        p.kind = PENDING_COPIED_WRITE;
        bridge_item_set_sid(&p, e->transport_id);
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
        if (!bridge_enqueue_pending(b, &p))
            return bridge_retention_failed(b, &p);
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
    /* A peer STOP_SENDING reset this send half: the FIN is obsolete, not owed. */
    if (e->peer_stop_received) return MOQ_OK;

    uint64_t sid = e->transport_id;
    moq_transport_result_t wr = sanitize_stream_result(
        b->ops->write(b->endpoint_ctx, sid, NULL, 0, true));

    if (wr == MOQ_TRANSPORT_WOULD_BLOCK) {
        bridge_pending_item_t p;
        memset(&p, 0, sizeof(p));
        p.kind = PENDING_CLOSE_BIDI_FIN;
        p.stream_ref = ref;
        bridge_item_set_sid(&p, sid);
        if (!bridge_enqueue_pending(b, &p))
            return bridge_retention_failed(b, &p);
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
        bridge_deactivate_stream(b, e);
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
        if (!bridge_enqueue_pending(b, &p))
            return bridge_retention_failed(b, &p);
        return MOQ_OK;
    }
    if (r == MOQ_TRANSPORT_ERROR) {
        bridge_deactivate_stream(b, e);
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
            bridge_item_set_sid(&p, uni_id);
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
            if (!bridge_enqueue_pending(b, &p))
                return bridge_retention_failed(b, &p);
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
        bridge_item_set_sid(&p, e->transport_id);
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
        if (!bridge_enqueue_pending(b, &p))
            return bridge_retention_failed(b, &p);
        return MOQ_OK;
    }

    if (wr == MOQ_TRANSPORT_ERROR) {
        moq_action_cleanup(act);
        bridge_set_fatal(b, 0x1);
        return MOQ_ERR_INTERNAL;
    }

    if (fin) {
        bridge_on_local_ctrl_uni_fin(b, e->transport_id);
        bridge_deactivate_stream(b, e);
    }
    moq_action_cleanup(act);
    return MOQ_OK;
}

/* ------------------------------------------------------------------- */

/* Expiry follows the action into bridge storage. Keep only the actual stream
 * prefix and retain the reset on the stream, so a full pending array cannot
 * make the timeout lose its owner or prevent it from releasing stale data. */
static moq_result_t bridge_expire_data(moq_transport_bridge_t *b,
                                       moq_stream_ref_t ref,
                                       const moq_action_meta_t *expired)
{
    const moq_action_meta_t meta = *expired;
    bridge_dstream_t *d = bridge_dstream_find(b, ref);
    bridge_stream_entry_t *e = bridge_find_by_ref(b, ref);
    moq_reset_data_action_t reset = { .stream_ref = ref,
        .reliable_size = e ? e->subgroup_header_len : 0 };
    bool pending_reset = false;
    for (uint32_t i = 0; i < b->pending_count; ++i) {
        const bridge_pending_item_t *p = &b->pending[i];
        if (p->stream_ref._v != ref._v) continue;
        if (p->kind == PENDING_RESET_STREAM && p->uni_reset)
            pending_reset = true;
        if (p->kind == PENDING_OPEN_UNI_DATA || p->kind == PENDING_HEADER_PAYLOAD)
            if (p->meta.subgroup_header)
                reset.reliable_size = p->act.u.send_data.header_len;
    }
    if (!d && !pending_reset) {
        d = bridge_dstream_new(b, ref);
        if (!d) return MOQ_ERR_NOMEM;
        /* Unkeyed fallback writes predate this timeout owner. Transfer their
         * existing contributions before removing expired payloads; a retained
         * stream prefix must still block reset until its retry succeeds. */
        for (uint32_t i = 0; i < b->pending_count; ++i) {
            const bridge_pending_item_t *p = &b->pending[i];
            if (p->stream_ref._v == ref._v &&
                (p->kind == PENDING_OPEN_UNI_DATA ||
                 p->kind == PENDING_HEADER_PAYLOAD ||
                 p->kind == PENDING_PAYLOAD_ONLY)) ++d->retained;
        }
        d->key.owner = meta.owner;
        d->key.subscriber_priority = meta.subscriber_priority;
        d->key.publisher_priority = meta.publisher_priority;
        d->meta = meta;
        bridge_owner_link(b, d);
        if (e) bridge_dstream_set_sid(b, d, e->transport_id);
    }
    if (d) {
        uint32_t keep = 0;
        if (d->q_len && d->q[d->q_head].meta.subgroup_header) {
            reset.reliable_size = d->q[d->q_head].action.u.send_data.header_len;
            keep = 1;
        }
        bridge_dstream_truncate(b, d, keep);
    }
    bool terminal = false;
    const bool reset_ready = bridge_session_take_data_timeout(
        b->session, ref, b->now_us, &reset, &terminal);
    if (e && terminal) e->data_terminal_received = true;
    uint32_t dst = 0;
    for (uint32_t src = 0; src < b->pending_count; ++src) {
        bridge_pending_item_t *p = &b->pending[src];
        const bool data = p->kind == PENDING_OPEN_UNI_DATA ||
                          p->kind == PENDING_HEADER_PAYLOAD ||
                          p->kind == PENDING_PAYLOAD_ONLY;
        if (data && p->stream_ref._v == ref._v &&
            !(p->meta.subgroup_header && p->kind != PENDING_PAYLOAD_ONLY)) {
            bridge_pending_counts(b, p, -1);
            bridge_cleanup_pending_item(&b->alloc, p);
            memset(p, 0, sizeof(*p));
            continue;
        }
        if (dst != src) {
            b->pending[dst] = *p;
            memset(p, 0, sizeof(*p));
        }
        ++dst;
    }
    b->pending_count = dst;
    if (d) {
        if (reset_ready && !pending_reset && !d->has_reset) {
            d->has_reset = true;
            b->dreset_count++;
            d->reset_terminal = terminal;
            d->reset_code = reset.error_code;
            d->reset_reliable = reset.reliable_size;
        }
        bridge_dstream_requeue(b, d);
    }
    return MOQ_OK;
}

static bool bridge_data_expired(const moq_transport_bridge_t *b,
                                 const moq_action_meta_t *meta)
{
    return meta->deadline_us != UINT64_MAX && meta->deadline_us <= b->now_us;
}

/* A deferred reset has its own stream slot. Refusal keeps that slot rather
 * than consuming pending capacity needed to finish unrelated streams. */
static moq_result_t bridge_try_deferred_reset(moq_transport_bridge_t *b,
                                              bridge_dstream_t *d, bool *accepted)
{
    *accepted = false;
    if (d->reset_pass == b->pass_seq) return MOQ_OK;
    bridge_stream_entry_t *e = bridge_find_by_ref(b, d->ref);
    if (!e) {
        *accepted = true; /* no transport stream was opened */
        return bridge_dstream_free(b, d);
    }
    if (d->reset_terminal) e->data_terminal_received = true;
    uint64_t reliable = d->reset_reliable;
    /* STOP suppresses even an unsent prefix. Preserve the first reset cause,
     * but do not wait for bytes the peer has asked us to stop sending. */
    if (e->peer_stop_received && reliable > e->bytes_sent)
        reliable = e->bytes_sent;
    if (e->bytes_sent < reliable ||
        bridge_domain_blocked(b, d->ref, e->transport_id, true)) return MOQ_OK;
    e->awaiting_commit = false;
    e->fin_reset_pending = true;
    d->reset_pass = b->pass_seq;
    moq_transport_result_t r = bridge_reset_uni(b, e->transport_id,
                                                d->reset_code, reliable);
    if (r == MOQ_TRANSPORT_WOULD_BLOCK) return MOQ_OK;
    if (r == MOQ_TRANSPORT_ERROR) {
        bridge_set_fatal(b, 0x1);
        return MOQ_ERR_INTERNAL;
    }
    const moq_stream_ref_t ref = d->ref;
    bridge_mark_local_close(b, ref);
    bridge_retire_or_tombstone(b, ref);
    *accepted = true;
    return bridge_dstream_free(b, d);
}

static moq_result_t bridge_expire_retained_data(moq_transport_bridge_t *b)
{
    /* Only a queued item with a deadline can expire: with none queued, the
     * stream table is not read; once the last one is gone, the walk stops. */
    for (uint32_t i = 0; b->dq_timed && i < b->dstream_cap; ++i) {
#ifdef MOQ_SESSION_SWEEP_TESTING
        bridge_work_expiry_slots++;
#endif
        bridge_dstream_t *d = &b->dstreams[i];
        if (!d->active) continue;
        for (uint32_t j = 0; j < d->q_len; ++j) {
#ifdef MOQ_SESSION_SWEEP_TESTING
            bridge_work_expiry_items++;
#endif
            const bridge_data_item_t *item = &d->q[(d->q_head + j) % d->q_cap];
            if (!bridge_data_expired(b, &item->meta)) continue;
            moq_result_t rc = bridge_expire_data(b, d->ref, &item->meta);
            if (rc < 0) return rc;
            break;
        }
    }
    for (uint32_t i = 0; i < b->pending_count;) {
        const bridge_pending_item_t *p = &b->pending[i];
        if ((p->kind == PENDING_OPEN_UNI_DATA || p->kind == PENDING_HEADER_PAYLOAD ||
             p->kind == PENDING_PAYLOAD_ONLY) && bridge_data_expired(b, &p->meta)) {
            moq_result_t rc = bridge_expire_data(b, p->stream_ref, &p->meta);
            if (rc < 0) return rc;
            i = 0; /* compaction may have removed earlier items of this stream */
        } else {
            ++i;
        }
    }
    /* With no deferred reset held, the stream table is not read. With any,
     * the walk reads every slot in turn, reset-bearing or not, until the
     * last one is settled or the pass's budget is spent. */
    for (uint32_t i = 0;
         b->dreset_count && i < b->dstream_cap && b->pump_budget; ++i) {
#ifdef MOQ_SESSION_SWEEP_TESTING
        bridge_work_reset_slots++;
#endif
        bridge_dstream_t *d = &b->dstreams[i];
        if (!d->active || !d->has_reset || d->q_len || d->retained) continue;
        bool accepted;
        --b->pump_budget;
        moq_result_t rc = bridge_try_deferred_reset(b, d, &accepted);
        if (rc < 0) return rc;
    }
    bridge_publish_deadline(b);
    return MOQ_OK;
}

static moq_result_t bridge_process_action(moq_transport_bridge_t *b,
                                           moq_action_t *act,
                                           const moq_action_meta_t *meta)
{
    switch (act->kind) {
    case MOQ_ACTION_SEND_CONTROL:
        return dispatch_send_control(b, act);

    case MOQ_ACTION_OPEN_UNI_CONTROL:
        return dispatch_open_uni_control(b, act);

    case MOQ_ACTION_SEND_UNI_CONTROL:
        return dispatch_send_uni_control(b, act);

    case MOQ_ACTION_CLOSE_SESSION: {
        /* Data the session queued ahead of the close goes first, as far as
         * the endpoint takes it. */
        bool moved;
        moq_result_t rc = bridge_pump_data(b, &moved);
        if (rc < 0) {
            moq_action_cleanup(act);
            return rc;
        }
        return dispatch_close_session(b, act);
    }

    case MOQ_ACTION_SEND_DATA: {
        if (bridge_data_expired(b, meta)) {
            moq_result_t rc = bridge_expire_data(b, act->u.send_data.stream_ref, meta);
            moq_action_cleanup(act);
            return rc;
        }
        bridge_dstream_t *d = bridge_dstream_find(b,
                                  act->u.send_data.stream_ref);
        if (!d && meta->mode == MOQ_SCHED_MODE_FILL) {
            d = bridge_dstream_new(b, act->u.send_data.stream_ref);
            if (!d) {
                moq_action_cleanup(act);
                return MOQ_ERR_NOMEM;
            }
            d->meta = *meta;
            d->key.owner = meta->owner;
            d->key.subscriber_priority = meta->subscriber_priority;
            d->key.publisher_priority = meta->publisher_priority;
            d->key.group_id = meta->group_id;
            d->key.descending = meta->group_order == MOQ_GROUP_ORDER_DESCENDING;
            d->pub = meta->publisher_priority;
            bridge_owner_link(b, d);
        }
        return d ? bridge_dstream_push(b, d, act, meta)
                 : dispatch_send_data(b, act, meta);
    }

    case MOQ_ACTION_SET_DATA_PRIORITY:
        return dispatch_set_data_priority(b, act);

    case MOQ_ACTION_RESET_DATA:
        return dispatch_reset_data(b, act, meta);

    case MOQ_ACTION_STOP_DATA:
        return dispatch_stop_data(b, act);

    case MOQ_ACTION_RESET_BIDI_STREAM:
        return dispatch_reset_data(b, act, meta);

    case MOQ_ACTION_STOP_BIDI_STREAM:
        return dispatch_stop_data(b, act);

    case MOQ_ACTION_ABORT_BIDI_STREAM:
        return dispatch_abort_bidi(b, act);

    case MOQ_ACTION_OPEN_BIDI_STREAM:
        return dispatch_open_bidi(b, act);

    case MOQ_ACTION_SEND_BIDI_STREAM:
        return dispatch_send_bidi(b, act);

    case MOQ_ACTION_CLOSE_BIDI_STREAM:
        return dispatch_close_bidi(b, act);

    case MOQ_ACTION_SEND_DATAGRAM: {
        if (bridge_data_expired(b, meta)) {
            moq_action_cleanup(act);
            return MOQ_OK;
        }
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

/* Once the required prefix has been handed over, pending data/FIN for this
 * exact ref is obsolete. Preserve every unrelated item and its FIFO order. */
static void bridge_drop_pending_data(moq_transport_bridge_t *b,
                                      const bridge_stream_entry_t *e)
{
    uint32_t dst = 0;
    for (uint32_t src = 0; src < b->pending_count; ++src) {
        bridge_pending_item_t *p = &b->pending[src];
        const bool data = p->kind == PENDING_OPEN_UNI_DATA ||
                          p->kind == PENDING_HEADER_PAYLOAD ||
                          p->kind == PENDING_PAYLOAD_ONLY;
        const bool same = p->canon_ref == e->ref._v ||
            (p->canon_have_sid && p->canon_sid == e->transport_id);
        if (data && same) {
            bridge_pending_counts(b, p, -1);
            bridge_cleanup_pending_item(&b->alloc, p);
            memset(p, 0, sizeof(*p));
            continue;
        }
        if (dst != src) {
            b->pending[dst] = *p;
            memset(p, 0, sizeof(*p));
        }
        dst++;
    }
    b->pending_count = dst;
}

/* The core retains the reset through refusal, so this path needs no spare
 * pending slot or byte capacity. It can overtake only data actions and only
 * after its reliable prefix is already handed to the endpoint. */
static moq_result_t bridge_service_data_reset(moq_transport_bridge_t *b,
                                               bool *offered, bool *accepted)
{
    *offered = *accepted = false;
    moq_reset_data_action_t reset;
    if (!bridge_session_offer_data_reset(b->session, &reset)) return MOQ_OK;
    bridge_stream_entry_t *e = bridge_find_by_ref(b, reset.stream_ref);
    if (!e || e->kind != BRIDGE_STREAM_UNI || e->origin != BRIDGE_ORIGIN_LOCAL ||
        bridge_stream_is_session_control(b, e->transport_id) ||
        e->bytes_sent < reset.reliable_size)
        return MOQ_OK;
    /* Ordinary dispatch already reserves a pending slot. Use source retention
     * only to escape pressure, then keep that owner through reset refusal. */
    if (b->dq_bytes < b->max_retained_bytes &&
        b->pending_count < b->max_pending && !e->reset_source_held)
        return MOQ_OK;
    bridge_dstream_t *d = bridge_dstream_find(b, reset.stream_ref);
    if (d && d->has_reset) return MOQ_OK; /* older deferred reset owns its cause */
    if (!bridge_session_claim_data_reset(b->session, &reset)) return MOQ_OK;
    *offered = true;
    e->reset_source_held = true;
    bridge_session_discard_reset_data(b->session, reset.stream_ref);
    bridge_drop_pending_data(b, e);
    if (d) {
        moq_result_t rc = bridge_dstream_free(b, d);
        if (rc < 0) return rc;
    }
    /* A FIN's later commitment must not retire a refused reset's mapping. */
    if (e->awaiting_commit) {
        e->awaiting_commit = false;
        e->fin_reset_pending = true;
    }
    if (bridge_domain_blocked(b, reset.stream_ref, e->transport_id, true))
        return MOQ_OK;
    const moq_transport_result_t r = bridge_reset_uni(b, e->transport_id,
        reset.error_code, reset.reliable_size);
    if (r == MOQ_TRANSPORT_WOULD_BLOCK) return MOQ_OK;
    if (r == MOQ_TRANSPORT_ERROR ||
        !bridge_session_commit_data_reset(b->session, &reset)) {
        bridge_set_fatal(b, 0x1);
        return MOQ_ERR_INTERNAL;
    }
    bridge_mark_local_close(b, reset.stream_ref);
    bridge_retire_or_tombstone(b, reset.stream_ref);
    *accepted = true;
    return MOQ_OK;
}

/* Drain at most BRIDGE_DRAIN_BUDGET actions. Ordinary dispatchers retain an
 * action behind older work in its stream domain, so each destructive poll
 * requires a spare pending slot and retained-byte capacity. A pressured
 * cancellation instead stays core-owned until accepted and can release its
 * own obsolete data without consuming either resource. */
static bool bridge_done_ready(void *ctx, uint64_t owner)
{
    moq_transport_bridge_t *b = ctx;
    for (uint32_t i = bridge_owner_first(b, owner); i != BRIDGE_INDEX_EMPTY;
         i = b->dstreams[i].owner_next) {
        const bridge_dstream_t *d = &b->dstreams[i];
        if (!d->fin_handed || d->has_reset) {
            b->done_waiting = true;
            return false;
        }
    }
    for (uint32_t i = 0; i < b->pending_count; ++i) {
        const bridge_pending_item_t *p = &b->pending[i];
        if (p->meta.owner == owner &&
            (p->kind == PENDING_OPEN_UNI_DATA ||
             p->kind == PENDING_HEADER_PAYLOAD ||
             p->kind == PENDING_PAYLOAD_ONLY ||
             (p->kind == PENDING_RESET_STREAM && p->uni_reset))) {
            b->done_waiting = true;
            return false;
        }
    }
    return true;
}

static moq_result_t bridge_drain_actions(moq_transport_bridge_t *b)
{
    moq_action_t act;
    moq_action_meta_t meta;
    uint32_t drained = 0;
    b->done_waiting = false;
    bool reset_blocked = false;
    while (drained < BRIDGE_DRAIN_BUDGET) {
        moq_result_t rc;
        if (!reset_blocked) {
            bool offered, accepted;
            rc = bridge_service_data_reset(b, &offered, &accepted);
            if (rc < 0) return rc;
            if (offered) {
                if (accepted) {
                    drained++;
                    if (b->fatal || b->closed) break;
                    continue;
                }
                reset_blocked = true;
            }
        }
        if (b->pending_count >= b->max_pending ||
            b->dq_bytes >= b->max_retained_bytes)
            break;
        /* Try a refused reset once per pass. Data preceding its barrier may
         * now fit after the purge; the reset and following control stay put. */
        if (reset_blocked) {
            if (!bridge_session_poll_leading_data(b->session, &act, &meta)) break;
        } else if (!bridge_session_poll_ready_action(b->session, &act, &meta,
                                                     bridge_done_ready, b)) {
            break;
        }
        uint32_t before = b->pending_count;
        rc = bridge_process_action(b, &act, &meta);
        if (rc < 0) return rc;
#ifdef MOQ_BRIDGE_SCAN_COUNTERS
        if (b->pending_count > before) {
            uint32_t delta = b->pending_count - before;
            if (delta > moq_bridge_max_retain_delta)
                moq_bridge_max_retain_delta = delta;
        }
#else
        (void)before;
#endif
        drained++;
        if (b->fatal || b->closed) break;
    }
    return MOQ_OK;
}

/* The ready stream with the best key. A blocked open -- no stream-map
 * entry is one too -- holds back only the streams still to open: the open
 * ones are what return the credit. */
static bridge_dstream_t *bridge_pump_pick(const moq_transport_bridge_t *b)
{
    bool open_blocked = b->pending_opens > 0 || bridge_stream_map_full(b);
    uint32_t best = b->dheap_len[0] ? b->dheap[0][0] : BRIDGE_INDEX_EMPTY;
    if (!open_blocked && b->dheap_len[1]) {
        uint32_t c = b->dheap[1][0];
        if (best == BRIDGE_INDEX_EMPTY || bridge_dheap_less(b, c, best))
            best = c;
    }
    return best == BRIDGE_INDEX_EMPTY ? NULL : &b->dstreams[best];
}

/* What `d` hands over next adds no stream bytes: a FIN-only write, or a
 * reset waiting behind the prefix it has already handed over. */
static bool bridge_dstream_next_empty(const bridge_dstream_t *d)
{
    return d->q_len == 0 ||
           bridge_send_data_bytes(&d->q[d->q_head].action.u.send_data) == 0;
}

/* The ready stream with the best key whose next hand-off adds no bytes, for
 * an endpoint whose shared budget is spent (MOQ_TRANSPORT_CAP_SHARED_ADMISSION):
 * an open stream's FIN-only write, which the endpoint may still take, or a
 * reset, which may give capacity back. A stream still to open needs bytes,
 * unless all it holds is a reset. */
static bridge_dstream_t *bridge_pump_pick_empty(const moq_transport_bridge_t *b)
{
    uint32_t best = BRIDGE_INDEX_EMPTY;
    for (uint32_t h = 0; h < 2; h++)
        for (uint32_t i = 0; i < b->dheap_len[h]; i++) {
            const uint32_t s = b->dheap[h][i];
            const bridge_dstream_t *d = &b->dstreams[s];
            if (h == 1 ? d->q_len != 0 : !bridge_dstream_next_empty(d))
                continue;
            if (best == BRIDGE_INDEX_EMPTY || bridge_dheap_less(b, s, best))
                best = s;
        }
    return best == BRIDGE_INDEX_EMPTY ? NULL : &b->dstreams[best];
}

/* Hand queued data to the endpoint, best key first (equal keys take turns).
 * A write the endpoint refuses before any byte went out goes back to its
 * stream, which sits out the rest of this pass: one stream's refusal never
 * holds back another, and nothing waits in the pending FIFO out of priority
 * order. Each step needs at most one pending slot. Sets *progress if
 * anything went.
 *
 * An endpoint with MOQ_TRANSPORT_CAP_SHARED_ADMISSION refuses a write only
 * when its connection-wide budget is spent, which no other stream's write
 * can change: from that refusal on, only work that adds no bytes is offered,
 * until a reset it accepts may have given capacity back. */
static moq_result_t bridge_pump_data(moq_transport_bridge_t *b,
                                     bool *progress)
{
    *progress = false;
    uint32_t nskip = 0;
    moq_result_t rc = MOQ_OK;
    const bool shared = (b->ops->capabilities &
                         MOQ_TRANSPORT_CAP_SHARED_ADMISSION) != 0;
    bool spent = false;
    /* A bounded number of head transitions bounds rank rebuilds in a pump.
     * The remaining ready heap is visible to outbound_ready for continuation. */
    while (!b->fatal && !b->closed && b->pending_count < b->max_pending &&
           b->pump_budget > 0) {
        rc = bridge_dstreams_reorder(b);
        if (rc < 0) break;
        bridge_dstream_t *d = spent ? bridge_pump_pick_empty(b)
                                    : bridge_pump_pick(b);
        if (!d) break;
        b->pump_budget--;
        moq_stream_ref_t ref = d->ref;
        if (d->q_len == 0) {
            bool accepted;
            rc = bridge_try_deferred_reset(b, d, &accepted);
            if (rc < 0) break;
            if (accepted) {
                *progress = true;
                spent = false;
            } else {
                d->pump_skipped = true;
                if (d->heap_at != BRIDGE_INDEX_EMPTY) bridge_dheap_remove(b, d);
                b->dskip[nskip++] = (uint32_t)(d - b->dstreams);
            }
            continue;
        }
        bridge_data_item_t item;
        bridge_dstream_pop(b, d, &item);
        b->pump_hand_back = true;
        b->pump_handed_back = false;
        b->pump_refused = false;
        rc = dispatch_send_data(b, &item.action, &item.meta);
        b->pump_hand_back = false;
        if (rc < 0) break;
        if (shared && b->pump_refused) spent = true;
        if (b->pump_handed_back) {
            bridge_dstream_unpop(b, d, &item);
            d->pump_skipped = true;
            if (d->heap_at != BRIDGE_INDEX_EMPTY) bridge_dheap_remove(b, d);
            b->dskip[nskip++] = (uint32_t)(d - b->dstreams);
            continue;
        }
        *progress = true;
        /* The write may have ended the stream. */
        d = bridge_dstream_find(b, ref);
        if (d) {
            d->served = ++b->serve_seq;
            bridge_dstream_requeue(b, d);
        }
    }
    for (uint32_t i = 0; i < nskip; i++) {
        bridge_dstream_t *d = &b->dstreams[b->dskip[i]];
        d->pump_skipped = false;
        if (d->active) bridge_dstream_requeue(b, d);
    }
    return rc < 0 ? rc : bridge_dstreams_reorder(b);
}

/* -- Outbound: retry pending ---------------------------------------- */

static void bridge_pending_dispatch_done(moq_transport_bridge_t *b,
                                          moq_stream_ref_t ref)
{
    bridge_dstream_t *d = bridge_dstream_find(b, ref);
    if (d) {
        d->dispatching = false;
        bridge_dstream_requeue(b, d);
    }
}

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
    uint32_t first_counted = 0;
    moq_stream_ref_t dispatch_ref = moq_stream_ref_from_u64(0);

    for (; i < old_count; i++) {
        bridge_pending_item_t *p = &old[i];
        /* Moving storage does not finish a write. Keep the detached tail
         * counted, and transfer only this item's class to dispatch before
         * removing its old contribution. Its kind can change during retry. */
        if (p->kind == PENDING_OPEN_UNI_DATA ||
            p->kind == PENDING_HEADER_PAYLOAD || p->kind == PENDING_PAYLOAD_ONLY) {
            dispatch_ref = p->stream_ref;
            bridge_dstream_t *d = bridge_dstream_find(b, dispatch_ref);
            if (d) d->dispatching = true;
        }
        bridge_pending_counts(b, p, -1);
        first_counted = i + 1;
        if (bridge_data_output_obsolete(b, p)) {
            bridge_cleanup_pending_item(&b->alloc, p);
            bridge_pending_dispatch_done(b, dispatch_ref);
            dispatch_ref = moq_stream_ref_from_u64(0);
            continue;
        }

        switch (p->kind) {
        case PENDING_OPEN_CONTROL: {
            if (b->control_mode == BRIDGE_CONTROL_UNI_PAIR) {
                /* Waiting for the local uni control stream, which an earlier
                 * pending open creates; retry the write once it exists. */
                if (!b->local_ctrl_uni_open) goto stop;
                moq_transport_result_t uw = sanitize_stream_result(
                    b->ops->write(b->endpoint_ctx, b->local_ctrl_uni_stream_id,
                                   p->data, p->data_len, false));
                if (uw == MOQ_TRANSPORT_WOULD_BLOCK) goto stop;
                if (uw == MOQ_TRANSPORT_ERROR) {
                    bridge_set_fatal(b, 0x1);
                    rc = MOQ_ERR_INTERNAL;
                    goto cleanup;
                }
                bridge_cleanup_pending_item(&b->alloc, p);
                break;
            }
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
            /* A stream whose send half a peer STOP reset while this write
             * waited: the bytes are obsolete, so drop the item. */
            bridge_stream_entry_t *pe = p->stream_ref._v != 0
                ? bridge_find_by_ref(b, p->stream_ref) : NULL;
            if (pe && pe->kind == BRIDGE_STREAM_BIDI &&
                pe->peer_stop_received) {
                bridge_cleanup_pending_item(&b->alloc, p);
                break;
            }
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
            for (uint32_t j = i + 1; j < old_count; j++) {
                bridge_pending_counts(b, &old[j], -1);
                bridge_cleanup_pending_item(&b->alloc, &old[j]);
            }
            bridge_free(&b->alloc, old,
                        b->max_pending * sizeof(bridge_pending_item_t));
            bridge_clear_all_state(b);
            return MOQ_OK;
        }

        case PENDING_RESET_STREAM: {
            if (p->uni_reset && !p->has_stream_id) {
                /* Retained before its stream existed: the open and every
                 * write ahead of it have run (FIFO), so resolve the stream
                 * now and retire its mapping, as an immediate reset does. */
                bridge_stream_entry_t *ue = bridge_find_by_ref(b, p->stream_ref);
                if (!ue) {
                    bridge_cleanup_pending_item(&b->alloc, p);
                    break;
                }
                if (p->reliable_size > ue->bytes_sent)
                    p->reliable_size = ue->bytes_sent;
                bridge_item_set_sid(p, ue->transport_id);
                if (p->meta.outgoing_terminal) ue->data_terminal_received = true;
                if ((ue->awaiting_commit && ue->subgroup_header_len != 0) ||
                    ue->data_terminal_received || ue->peer_stop_received) {
                    ue->awaiting_commit = false;
                    ue->fin_reset_pending = true;
                } else {
                    p->stream_ref = moq_stream_ref_from_u64(0);
                    bridge_deactivate_stream(b, ue);
                }
            }
            moq_transport_result_t r = p->uni_reset
                ? bridge_reset_uni(b, p->stream_id, p->error_code,
                                   p->reliable_size)
                : sanitize_stream_result(
                      b->ops->reset_stream(b->endpoint_ctx, p->stream_id,
                                           p->error_code));
            if (r == MOQ_TRANSPORT_WOULD_BLOCK) goto stop;
            if (r == MOQ_TRANSPORT_ERROR) {
                bridge_set_fatal(b, 0x1);
                rc = MOQ_ERR_INTERNAL;
                goto cleanup;
            }
            /* Deferred bidi lifecycle: only now that the endpoint accepted the
             * reset do we close our send half and retire-or-keep the mapping,
             * so a peer terminal that arrived while the reset was blocked has
             * not been dropped. A retained subgroup FIN also carries its ref
             * until this accepted-reset boundary. */
            if (p->stream_ref._v) {
                bridge_mark_local_close(b, p->stream_ref);
                bridge_retire_or_tombstone(b, p->stream_ref);
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
            if (p->data_stop) {
                bridge_stream_entry_t *se = bridge_find_by_id(b, p->stream_id);
                if (se) bridge_note_data_stop_accepted(b, se);
            }
            bridge_cleanup_pending_item(&b->alloc, p);
            break;
        }

        case PENDING_ABORT_STREAM: {
            /* whole abort not yet applied (native op, or fallback whose
             * RESET half has not been accepted) */
            if (HAS_FIELD(b->ops, abort_stream) &&
                b->ops->abort_stream != NULL) {
                moq_transport_result_t r = sanitize_stream_result(
                    b->ops->abort_stream(b->endpoint_ctx, p->stream_id,
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
            moq_transport_result_t r = sanitize_stream_result(
                b->ops->reset_stream(b->endpoint_ctx, p->stream_id,
                                     p->error_code));
            if (r == MOQ_TRANSPORT_WOULD_BLOCK) goto stop;
            if (r == MOQ_TRANSPORT_ERROR) {
                bridge_set_fatal(b, 0x1);
                rc = MOQ_ERR_INTERNAL;
                goto cleanup;
            }
            /* RESET accepted: only the STOP half remains. Morph the
             * item so a later WOULD_BLOCK never re-sends the RESET. */
            p->kind = PENDING_STOP_SENDING;
            r = sanitize_stream_result(
                b->ops->stop_sending(b->endpoint_ctx, p->stream_id,
                                     p->error_code));
            if (r == MOQ_TRANSPORT_WOULD_BLOCK) goto stop;
            if (r == MOQ_TRANSPORT_ERROR) {
                bridge_set_fatal(b, 0x1); /* partial: fatal, no rollback */
                rc = MOQ_ERR_INTERNAL;
                goto cleanup;
            }
            bridge_cleanup_pending_item(&b->alloc, p);
            break;
        }

        case PENDING_OPEN_UNI_DATA: {
            /* An earlier queued action for this ref may have opened the
             * stream since this item was retained. Reuse that mapping. */
            bridge_stream_entry_t *e = bridge_find_by_ref(b, p->stream_ref);
            if (!e) {
                /* Reserve stream-map slot before opening transport stream. */
                e = bridge_alloc_stream(b);
                if (!e) {
                    bridge_set_fatal(b, 0x1);
                    rc = MOQ_ERR_INTERNAL;
                    goto cleanup;
                }
                uint64_t uni_id = 0;
                moq_transport_result_t r = sanitize_stream_result(
                    b->ops->open_uni(b->endpoint_ctx, &uni_id));
                if (r == MOQ_TRANSPORT_WOULD_BLOCK) {
                    bridge_deactivate_stream(b, e);
                    goto stop;
                }
                if (r == MOQ_TRANSPORT_ERROR) {
                    bridge_deactivate_stream(b, e);
                    bridge_set_fatal(b, 0x1);
                    rc = MOQ_ERR_INTERNAL;
                    goto cleanup;
                }
                e->ref = p->stream_ref;
                e->transport_id = uni_id;
                e->kind = BRIDGE_STREAM_UNI;
                e->origin = BRIDGE_ORIGIN_LOCAL;
            }
            p->kind = PENDING_HEADER_PAYLOAD;
            bridge_item_set_sid(p, e->transport_id);
        }
        /* fall through - reuse this item to send header+payload */

        case PENDING_HEADER_PAYLOAD:
        case PENDING_PAYLOAD_ONLY: {
            moq_result_t trc = bridge_try_send_data(b, p, NULL);
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
                bridge_deactivate_stream(b, e);
                goto stop;
            }
            if (r == MOQ_TRANSPORT_ERROR) {
                bridge_deactivate_stream(b, e);
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
                    bridge_item_set_sid(p, uni_id);
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
                bridge_deactivate_stream(b, e);
                goto stop;
            }
            if (r == MOQ_TRANSPORT_ERROR) {
                bridge_deactivate_stream(b, e);
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
                    bridge_item_set_sid(p, bidi_id);
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
                    bridge_item_set_sid(p, bidi_id);
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
            bridge_stream_entry_t *pe = bridge_find_by_ref(b, p->stream_ref);
            if (pe && pe->peer_stop_received) {
                bridge_cleanup_pending_item(&b->alloc, p);
                break;
            }
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
        bridge_pending_dispatch_done(b, dispatch_ref);
        dispatch_ref = moq_stream_ref_from_u64(0);
    }
    goto done;

stop:
    /*
     * The two queue-full policies, stated because they differ on purpose.
     *
     * DRAIN side: unreachable. The drain never polls unless a slot is free and
     * one action consumes at most one slot, so a drain-side enqueue cannot
     * fail; the dispatchers' fatal branches remain as assertions of that.
     *
     * RETRY side: cannot overflow by counting. This pass moved every survivor
     * out of `b->pending` (count reset to 0) and re-enqueues at most the same
     * old_count items into an array of the same max_pending, so the queue can
     * only return to a depth it already held. If it fails regardless, the
     * accounting is wrong: the pass stops and surfaces an invariant breach
     * instead of dropping an owned item and reporting success.
     */
    for (; i < old_count; i++) {
        if (!bridge_enqueue_pending(b, &old[i])) {
            if (i >= first_counted)
                bridge_pending_counts(b, &old[i], -1);
            rc = bridge_retention_failed(b, &old[i]);
            i++;
            goto cleanup;
        }
        /* Enqueue first: a future old item must never disappear from the
         * graph between its detached and live queue ownership. */
        if (i >= first_counted)
            bridge_pending_counts(b, &old[i], -1);
        memset(&old[i], 0, sizeof(old[i]));
    }
    goto done;

cleanup:
    for (; i < old_count; i++) {
        if (i >= first_counted)
            bridge_pending_counts(b, &old[i], -1);
        bridge_cleanup_pending_item(&b->alloc, &old[i]);
    }

done:
    bridge_pending_dispatch_done(b, dispatch_ref);
    bridge_free(&b->alloc, old, b->max_pending * sizeof(bridge_pending_item_t));
    return rc;
}

/* -- Inbound: retry ------------------------------------------------- */

/*
 * Outcome of one inbound-retry pass.
 *
 * `progress` and `rc` are separate because they answer different questions and
 * a single value cannot carry both: a pass can make durable progress AND then
 * suspend, and a suspension is neither progress nor an error. Collapsing them
 * would force suspension to masquerade as one of MOQ_OK, MOQ_ERR_WOULD_BLOCK
 * (which the caller reads as retained inbound backpressure) or a fatal.
 */
typedef struct {
    bool         progress;   /* at least one durable advance this pass */
    moq_result_t rc;         /* MOQ_OK, or MOQ_SESSION_SUSPENDED */
} bridge_inbound_outcome_t;

static moq_result_t bridge_stop_local_data(moq_transport_bridge_t *b,
                                            bridge_stream_entry_t *e,
                                            uint64_t code, uint64_t now_us)
{
    const bridge_dstream_t *d = bridge_dstream_find(b, e->ref);
    if (d && d->has_reset) {
        /* STOP cancels further writes, but does not replace a reset already
         * retained by the scheduler with a later core fallback reset. */
        e->peer_stop_received = true;
        bridge_prune_terminal_output(b);
        return MOQ_OK;
    }
    if (e->fin_reset_pending) return MOQ_OK;
    if (e->subgroup_header_len != 0) {
        moq_result_t rc = bridge_session_on_subgroup_stop(
            b->session, e->ref, code, now_us, e->subgroup_header_len);
        /* WOULD_BLOCK may retain a core terminal debt. Keep the first STOP
         * through output pressure; a later ACK must not erase that fact. */
        if (rc == MOQ_OK || rc == MOQ_ERR_WOULD_BLOCK)
            e->peer_stop_received = true;
        return rc;
    }
    return moq_session_on_data_stop(b->session, e->ref, code, now_us);
}

static bridge_inbound_outcome_t bridge_retry_inbound_pending(
    moq_transport_bridge_t *b, uint64_t now_us)
{
    bool progress = false;

    if (b->pending_control) {
        moq_result_t rc = moq_session_process_pending(b->session, now_us);
        /* Tested before both the WOULD_BLOCK and the negative branches: this is
         * neither retained backpressure nor an error, and every field below
         * must keep the value it had so the retry survives to the next pass. */
        if (rc == MOQ_SESSION_SUSPENDED)
            return (bridge_inbound_outcome_t){ progress, MOQ_SESSION_SUSPENDED };
        if (rc != MOQ_ERR_WOULD_BLOCK) {
            b->pending_control = false;
            progress = true;
            if (rc < 0) {
                bridge_set_fatal(b, 0x1);
                return (bridge_inbound_outcome_t){ progress, MOQ_OK };
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

        /* A stream refused at admission holds nothing here: when the session
         * can admit again, clear the readiness mark so the adapter redelivers
         * its held chunk (admission is re-checked on that delivery). The
         * obligation itself (held_input) ends only with that chunk's
         * acceptance, a peer RESET or teardown. */
        if (e->pending_admission) {
            if (!b->holds && moq_session_can_admit_data_stream(b->session)) {
                e->pending_admission = false;
                progress = true;
            }
            continue;
        }

        if (e->pending_retry) {
            moq_result_t rc;
            if (e->kind == BRIDGE_STREAM_BIDI)
                rc = moq_session_on_bidi_stream_bytes(
                    b->session, e->ref, NULL, 0, false, now_us);
            else
                rc = moq_session_on_data_bytes(
                    b->session, e->ref, NULL, 0, false, now_us);

            /* Leaves pending_retry set and the scan stopped: a spent budget
             * makes every later owner suspend too, so continuing would only
             * repeat the work. */
            if (rc == MOQ_SESSION_SUSPENDED)
                return (bridge_inbound_outcome_t){ progress, MOQ_SESSION_SUSPENDED };
            if (rc != MOQ_ERR_WOULD_BLOCK) {
                e->pending_retry = false;
                progress = true;
                if (rc < 0) {
                    bridge_set_fatal(b, 0x1);
                    return (bridge_inbound_outcome_t){ progress, MOQ_OK };
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
            /* pending_reset and its code stay intact, and the entry stays
             * active: the reset has not been delivered. */
            if (rc == MOQ_SESSION_SUSPENDED)
                return (bridge_inbound_outcome_t){ progress, MOQ_SESSION_SUSPENDED };
            if (rc != MOQ_ERR_WOULD_BLOCK) {
                e->pending_reset = false;
                progress = true;
                if (rc < 0)
                    bridge_set_fatal(b, 0x1);
                else
                    bridge_peer_reset_accepted(b, e);
            }
        }

        if (e->pending_stop && e->active && e->peer_reset_accepted) {
            /* The peer's RESET ended the request first: the STOP's answer
             * (our half's reset) is already queued, and the request is not
             * cancelled a second time. */
            e->pending_stop = false;
            progress = true;
            bridge_retire_or_tombstone(b, e->ref);
            continue;
        }

        if (e->pending_stop && e->active) {
            /* A peer STOP_SENDING on a request bidi is the D18 cancellation
             * signal and is distinct from a RESET_STREAM: route it through
             * the dedicated bidi-stop input, never on_bidi_stream_reset. */
            moq_result_t rc = (e->kind == BRIDGE_STREAM_BIDI)
                ? moq_session_on_bidi_stream_stop(
                      b->session, e->ref, e->pending_stop_code, now_us)
                : bridge_stop_local_data(b, e, e->pending_stop_code, now_us);
            /* pending_stop and its code stay intact. */
            if (rc == MOQ_SESSION_SUSPENDED)
                return (bridge_inbound_outcome_t){ progress, MOQ_SESSION_SUSPENDED };
            if (rc != MOQ_ERR_WOULD_BLOCK) {
                e->pending_stop = false;
                progress = true;
                if (rc < 0 && rc != MOQ_ERR_WOULD_BLOCK)
                    bridge_set_fatal(b, 0x1);
            }
        }
    }

    return (bridge_inbound_outcome_t){ progress, MOQ_OK };
}

/* -- Service -------------------------------------------------------- */

/* The service pass itself. Kept separate from the public entry point so the
 * budgeted-advance context is entered and left exactly once, no matter which of
 * this function's several returns is taken. */
static moq_result_t bridge_service_pass(
    moq_transport_bridge_t *b, uint64_t now_us, bool *suspended)
{
    *suspended = false;
    b->now_us = now_us;
    b->pump_budget = BRIDGE_DRAIN_BUDGET;
    ++b->pass_seq; /* every dstream's reset is untried in this pass */
    for (;;) {
        /* Outgoing expiry/debt is independent of unrelated blocked provider
         * output. This path advances only when outgoing work is actually due. */
        if (!b->fatal && !b->closed) {
            moq_result_t orc = bridge_session_service_outgoing(b->session, now_us);
            if (orc == MOQ_SESSION_SUSPENDED) {
                *suspended = true;
                break;
            }
            if (orc < 0) return orc;
            if (bridge_session_take_terminal_changes(b->session))
                bridge_prune_terminal_output(b);
            else
                bridge_prioritize_pending_resets(b);
        }
        /* Deferred close has priority — runs even if writes are blocked.
         * Session has already been notified; this closes the transport. */
        if (!b->fatal && !b->closed && b->needs_close) {
            b->needs_close = false;
            moq_transport_result_t cr = sanitize_stream_result(
                b->ops->close_transport(b->endpoint_ctx,
                                         b->needs_close_code, NULL, 0));
            if (cr == MOQ_TRANSPORT_WOULD_BLOCK) {
                bridge_cleanup_all_pending(b);
                bridge_dstreams_clear(b);
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

        /* Held streams: expire, cancel and offer before any deadline is
         * ticked, so a due hold age never reaches step 4 unchanged. */
        if (!b->fatal && !b->closed && b->holds) {
            bool moved = false;
            moq_result_t hrc = bridge_hold_maintain(b, now_us, &moved);
            if (hrc < 0) return hrc;
        }

        /* Retained object deadlines apply before any retry, including when
         * pending output is full. The supplied clock does not advance while
         * endpoint operations in this pass run. */
        if (!b->fatal && !b->closed) {
            moq_result_t rc = bridge_expire_retained_data(b);
            if (rc < 0) return rc;
        }

        /* Step 1: retry outbound pending FIFO */
        if (b->pending_count > 0) {
            moq_result_t rc = bridge_retry_outbound_pending(b);
            if (rc < 0) return rc;
            /* Old pending work is retried first, every pass. It no longer
             * ends the pass: an action for an unrelated stream may still be
             * drained, and a same-stream action is retained by the domain
             * gate rather than by stopping here. */
        }

        /* Step 2: drain new actions. Per-stream ordering is the gate's job. */
        if (!b->fatal && !b->closed) {
            moq_result_t rc = bridge_drain_actions(b);
            if (rc < 0) return rc;
        }

        /* Step 2b: hand queued data to the endpoint in priority order; what
         * it frees may let more actions drain. */
        if (!b->fatal && !b->closed) {
            bool was_full = b->dq_bytes >= b->max_retained_bytes;
            bool moved;
            moq_result_t rc = bridge_pump_data(b, &moved);
            if (rc < 0) return rc;
            if (moved && (b->done_waiting ||
                (was_full && b->dq_bytes < b->max_retained_bytes)))
                continue; /* a handed FIN/reset can release owner DONE */
        }

        /* Step 3: retry inbound pending */
        if (!b->fatal && !b->closed) {
            bridge_inbound_outcome_t in = bridge_retry_inbound_pending(b, now_us);
            /* Suspension is neither progress nor an error: it ends the pass
             * with every pending flag left intact for the continuation. The
             * flag is what distinguishes this exit from a drained pass, which
             * leaves by the same break. */
            if (in.rc == MOQ_SESSION_SUSPENDED) {
                *suspended = true;
                break;
            }
            if (in.progress && !b->fatal && !b->closed)
                continue;
        }

        /* Step 3b: settle locally-stopped data and locally-aborted request
         * identities after their genuine terminal, outside endpoint ops. */
        if (!b->fatal && !b->closed && b->local_retire_due) {
            bool released = false;
            moq_result_t lrc = bridge_settle_local_retire(b, now_us, &released);
            if (lrc == MOQ_SESSION_SUSPENDED) {
                *suspended = true;
                break;
            }
            if (lrc < 0) return lrc;
            if (released && !b->fatal && !b->closed) continue;
        }

        /* Step 3c: our own cancellation of a request bidi the peer's RESET
         * ended. */
        if (!b->fatal && !b->closed && b->bidi_cancel_due) {
            bool queued = false;
            moq_result_t crc = bridge_settle_bidi_cancel(b, &queued);
            if (crc < 0) return crc;
            if (queued && !b->fatal && !b->closed) continue;
        }

        /* Step 4: tick deadlines — only if outbound is clear */
        if (!b->fatal && !b->closed && b->pending_count == 0) {
            uint64_t dl = moq_session_next_deadline_us(b->session);
            if (dl != UINT64_MAX && dl <= now_us) {
                moq_result_t trc = moq_session_tick(b->session, now_us);
                /* A suspended tick is not fatal. The deadline stays due, so
                 * the obligation survives as session state and a later pass
                 * runs it exactly once. */
                if (trc == MOQ_SESSION_SUSPENDED) {
                    *suspended = true;
                    break;
                }
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

moq_result_t moq_transport_bridge_service(
    moq_transport_bridge_t *b, uint64_t now_us)
{
    if (!b) return MOQ_ERR_INVAL;
    if (b->fatal) return MOQ_ERR_INTERNAL;
    if (b->closed) return MOQ_OK;

    /* No budget context: the wired session entry points take the unlimited
     * advance, which drives the sweep to completion and cannot suspend. A
     * UINT32_MAX context would still be a FINITE budget and would route them
     * through the budgeted path, so this entry would gain a suspension it has
     * no way to report. */
    bool suspended = false;
    moq_result_t rc = bridge_service_pass(b, now_us, &suspended);
    bridge_publish_deadline(b);
    return rc;
}

moq_result_t moq_transport_bridge_service_budgeted(
    moq_transport_bridge_t *b, uint64_t now_us, uint32_t sweep_budget,
    moq_bridge_budgeted_result_t *out)
{
    if (!out) return MOQ_ERR_INVAL;
    memset(out, 0, sizeof(*out));
    if (!b) return MOQ_ERR_INVAL;
    if (b->fatal) return MOQ_ERR_INTERNAL;
    if (b->closed) return MOQ_OK;

    /* Paired by construction: one enter, one leave, whichever return the pass
     * takes. Leaving the context is what keeps a later ordinary session call
     * from inheriting the budget and observing the suspension sentinel. */
    session_budget_enter(b->session, sweep_budget);
    bool suspended = false;
    moq_result_t rc = bridge_service_pass(b, now_us, &suspended);
    /* Read before leaving: leave zeroes the remaining budget. */
    bridge_publish_deadline(b);
    out->sweep_spent = sweep_budget - session_budget_remaining(b->session);
    out->suspended = suspended;
    session_budget_leave(b->session);
    return rc;
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

/* Accepted input can retire its session owner (for example, after a pending
 * terminal drains). Do not reopen a parser on a later tail. Admission refusals
 * never reach this helper: their bytes and FIN remain caller-owned. Padding
 * and already-discarded streams retain their separate protocol discard paths. */
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
        if (fin) bridge_deactivate_stream(b, e);
        return MOQ_OK;
    }
    moq_result_t ar = bridge_admission_refused(b, e, ref, len, now_us);
    if (ar < 0) return ar;
    moq_result_t rc = moq_session_on_data_bytes(b->session, ref, data, len,
                                                fin, now_us);
    bridge_admission_accepted(b, e);
    if (rc == MOQ_ERR_WOULD_BLOCK) {
        if (moq_session_has_transport_stream(b->session, ref)) {
            /* Session retained the bytes; the service tick drains them. */
            e->pending_retry = true;
            if (fin) e->fin_retained = true;
            return MOQ_ERR_WOULD_BLOCK;
        }
        return bridge_hold_admission(b, e, len, now_us);
    }
    if (rc < 0) {
        bridge_set_fatal(b, 0x1);
        return rc;
    }
    if (fin) {
        bridge_deactivate_stream(b, e);
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
    /* While the session is draining buffered control bytes, this bridge takes
     * no further peer uni input until the shared retry in
     * moq_transport_bridge_service clears it. The control stream's own input
     * is the session's retained control input (WOULD_BLOCK; its FIN is
     * recorded). Any other stream's chunk is refused whole and stays the
     * adapter's (MOQ_ERR_INPUT_NOT_CONSUMED: zero bytes, no FIN taken, any
     * classification prefix taken on an earlier call stays the bridge's);
     * the stream reports pending until the control input drains
     * (bridge_uni_deferred), and its redelivery is classified and validated
     * as if it had just arrived. */
    if (b->pending_control) {
        if (stream_id == b->pending_control_stream_id) {
            if (fin) b->pending_control_fin = true;
            return MOQ_ERR_WOULD_BLOCK;
        }
        if (len == 0 && !fin) return MOQ_OK;     /* nothing to take or keep */
        return MOQ_ERR_INPUT_NOT_CONSUMED;
    }

    bridge_stream_entry_t *e = bridge_find_by_id(b, stream_id);
    if (!e && len == 0 && !fin) return MOQ_OK;

    moq_result_t placed_rc;
    if (!e && bridge_uni_pair_place_unmapped(b, stream_id, data, len, now_us,
                                             &placed_rc))
        return placed_rc;

    moq_stream_ref_t ref;
    if (!e) {
        ref = bridge_assign_inbound_ref(b, stream_id, BRIDGE_STREAM_UNI);
        if (ref._v == 0) return MOQ_ERR_INTERNAL;
        e = bridge_find_by_id(b, stream_id);
    } else {
        ref = e->ref;
    }

    /* The adapter still owes a refused chunk that carried bytes: an empty
     * (FIN-only) delivery cannot stand in for it -- refuse it without
     * classifying, whether or not the session is ready. A redelivery with
     * bytes (or the retry of a refused zero-byte FIN) re-enters
     * classification with the retained prefix and is re-checked below. */
    if (e->held_input && e->held_payload && len == 0)
        return MOQ_ERR_INPUT_NOT_CONSUMED;

    /* Already classified: route directly. */
    if (e->uni_disp == BRIDGE_UNI_DISP_CONTROL)
        return bridge_feed_peer_control(b, stream_id, data, len, fin, now_us);
    if (e->uni_disp == BRIDGE_UNI_DISP_DATA)
        return bridge_feed_peer_data(b, e, ref, data, len, fin, now_us);
    if (e->uni_disp == BRIDGE_UNI_DISP_DISCARD) {
        if (fin) bridge_deactivate_stream(b, e);
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
         * so the whole chunk fits in the retained buffer.
         *
         * No age applies to an incomplete stream type: such a stream keeps
         * its mapping until its type completes, a peer RESET, or teardown.
         *
         * Temporary known limitation, not a conformance result: an
         * incomplete type takes any free mapping, an entry the control
         * reserve keeps included, because it may be the control stream and
         * has no other lossless destination yet. A peer that parks streams
         * here consumes stream mappings, after which a new stream that
         * cannot be held -- an incomplete type, or its control stream --
         * meets a full map, which is fatal. Timeouts are left to the
         * implementation (draft-22 section 15.6); the session's own timeouts
         * are unchanged and nothing else bounds the wait. */
        if (len > 0) memcpy(e->classify_buf + e->classify_len, data, len);
        e->classify_len = (uint8_t)(e->classify_len + len);
        return MOQ_OK;
    }
    if (route == BRIDGE_UNI_ROUTE_FATAL) {
        bridge_set_fatal(b, 0x3);    /* unknown type or second control channel */
        return MOQ_ERR_PROTO;
    }

    /* A data stream the session cannot admit yet: refuse this chunk whole
     * (nothing of it fed) and keep the classification prefix bridge-owned;
     * the redelivered chunk re-enters classification with that same prefix.
     * Control and padding streams are never gated here. */
    if (cls == MOQ_UNI_CLASS_DATA) {
        moq_result_t ar = bridge_admission_refused(b, e, ref, len, now_us);
        if (ar < 0) return ar;
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
        if (fin) bridge_deactivate_stream(b, e);
        return MOQ_OK;
    }

    /* DATA */
    e->uni_disp = BRIDGE_UNI_DISP_DATA;
    if (plen > 0) {
        moq_result_t rc = bridge_feed_peer_data(b, e, ref, prefix, plen,
                                                false, now_us);
        if (rc == MOQ_ERR_INPUT_NOT_CONSUMED) {
            /* The old prefix remains ours; none of this caller chunk was
             * delivered. Retry classification with both in their old order. */
            memcpy(e->classify_buf, prefix, plen);
            e->classify_len = (uint8_t)plen;
            e->uni_disp = BRIDGE_UNI_DISP_PENDING;
            e->held_payload = len > 0;
            return bridge_hold_admission(b, e, len, now_us);
        }
        if (rc < 0 && rc != MOQ_ERR_WOULD_BLOCK) return rc;
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
        if (fin) bridge_deactivate_stream(b, e);
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
            if (fin) bridge_deactivate_stream(b, e);
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
        if (fin) { bridge_deactivate_stream(b, e); return MOQ_OK; }
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

    moq_result_t held_rc;
    if (!e && bridge_hold_unmapped_uni(b, stream_id, len, now_us, &held_rc))
        return held_rc;

    moq_stream_ref_t ref;
    if (!e) {
        ref = bridge_assign_inbound_ref(b, stream_id, BRIDGE_STREAM_UNI);
        if (ref._v == 0) return MOQ_ERR_INTERNAL;
        e = bridge_find_by_id(b, stream_id);
    } else {
        ref = e->ref;
    }

    moq_result_t ar = bridge_admission_refused(b, e, ref, len, now_us);
    if (ar < 0) return ar;

    moq_result_t rc = moq_session_on_data_bytes(
        b->session, ref, data, len, fin, now_us);
    bridge_admission_accepted(b, e);

    if (rc == MOQ_ERR_WOULD_BLOCK) {
        if (moq_session_has_transport_stream(b->session, ref)) {
            /* Legitimate inbound backpressure: the session retained the bytes.
             * Let the service tick drain them; never fatal. */
            e->pending_retry = true;
            if (fin) e->fin_retained = true;
            return MOQ_ERR_WOULD_BLOCK;
        }
        return bridge_hold_admission(b, e, len, now_us);
    }

    if (rc < 0) {
        bridge_set_fatal(b, 0x1);
        return rc;
    }

    if (fin) {
        bridge_deactivate_stream(b, e);
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
        if (fin) bridge_deactivate_stream(b, e);
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
            if (fin) bridge_deactivate_stream(b, e);
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
        if (fin) { bridge_deactivate_stream(b, e); return MOQ_OK; }
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

    moq_result_t held_rc;
    if (!e && bridge_hold_unmapped_uni(b, stream_id, data_len, now_us,
                                       &held_rc))
        return held_rc;

    moq_stream_ref_t ref;
    if (!e) {
        ref = bridge_assign_inbound_ref(b, stream_id, BRIDGE_STREAM_UNI);
        if (ref._v == 0) return MOQ_ERR_INTERNAL;
        e = bridge_find_by_id(b, stream_id);
    } else {
        ref = e->ref;
    }

    moq_result_t ar = bridge_admission_refused(b, e, ref, data_len, now_us);
    if (ar < 0) return ar;

    moq_result_t rc = moq_session_on_data_rcbuf(
        b->session, ref, data, fin, now_us);
    bridge_admission_accepted(b, e);

    if (rc == MOQ_ERR_WOULD_BLOCK) {
        if (moq_session_has_transport_stream(b->session, ref)) {
            /* Legitimate inbound backpressure: the session retained the bytes. */
            e->pending_retry = true;
            if (fin) e->fin_retained = true;
            return MOQ_ERR_WOULD_BLOCK;
        }
        return bridge_hold_admission(b, e, data_len, now_us);
    }

    if (rc < 0) {
        bridge_set_fatal(b, 0x1);
        return rc;
    }

    if (fin) {
        bridge_deactivate_stream(b, e);
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

    if (e && e->aborting) {
        /* Discarding after a whole-stream abort: absorb late bytes
         * silently, latch the peer's FIN for service — never a fresh ref,
         * never an advancing session call from an endpoint callback. */
        if (fin) bridge_note_aborting_terminal(b, e);
        return MOQ_OK;
    }

    /* The peer's half ended with a RESET the session accepted; the mapping
     * is kept for our own send half only. Nothing more can arrive on the
     * peer's half, and nothing is parsed as a new request. */
    if (e && e->peer_reset_accepted) return MOQ_OK;

    if (e && e->pending_retry) {
        if (len > 0) {
            bridge_set_fatal(b, 0x3);
            return MOQ_ERR_PROTO;
        }
        if (fin) e->pending_fin = true;
        return MOQ_ERR_WOULD_BLOCK;
    }

    if (!e && len == 0 && !fin) return MOQ_OK;

    /* A new peer request stream with held-stream records: it is mapped only
     * when no older held request stream is waiting (or it is the one offered)
     * and a mapping is free beyond the control reserve. Otherwise its exact
     * chunk is held -- or, every record in use, refused for the adapter to
     * cancel. */
    const bool hold_bidi = b->holds && b->control_mode == BRIDGE_CONTROL_UNI_PAIR;
    bridge_hold_t *held = (!e && hold_bidi) ? bridge_hold_find(b, stream_id) : NULL;
    if (!e && hold_bidi) {
        if (held && held->state == BRIDGE_HOLD_CANCELLING)
            return MOQ_ERR_INPUT_NOT_CONSUMED;
        bool queued = held ? !held->offered
                           : bridge_hold_class_waiting(b, false);
        /* The FIN cannot overtake the held bytes. */
        if (held && held->held_payload && len == 0) queued = true;
        if (queued || !bridge_can_map_peer_stream(b)) {
            if (held) {
                held->offered = false;
                if (len > 0) held->held_payload = true;
                return MOQ_ERR_INPUT_NOT_CONSUMED;
            }
            if (!bridge_hold_record(b, stream_id, BRIDGE_STREAM_BIDI, false,
                                    len, now_us))
                return MOQ_BRIDGE_INGRESS_REFUSE_CANCEL;
            return MOQ_ERR_INPUT_NOT_CONSUMED;
        }
    }

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
    /* The session took the chunk (or failed below): the hold is over. */
    if (held) bridge_hold_release(b, held);

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

    /* A held stream: the peer's RESET settles only the receive half. A held
     * uni stream has nothing more owed; a held request stream still has our
     * own half, which is reset (cancellation in the record's own slot). The
     * adapter drops the chunk it was holding either way. */
    {
        bridge_hold_t *h = bridge_hold_find(b, stream_id);
        if (h) {
            bridge_stream_entry_t *he = bridge_find_by_id(b, stream_id);
            if (h->state == BRIDGE_HOLD_RECORDED &&
                h->kind == BRIDGE_STREAM_BIDI) {
                bridge_hold_cancel(b, h, true);
                return MOQ_OK;
            }
            h->stop_owed = false;
            if (!h->reset_owed) {
                if (he) bridge_deactivate_stream(b, he);
                bridge_hold_release(b, h);
            }
            return MOQ_OK;
        }
    }

    bridge_stream_entry_t *e = bridge_find_by_id(b, stream_id);
    if (!e) return MOQ_OK;

    if (e->aborting) {
        bridge_note_aborting_terminal(b, e);
        return MOQ_OK;
    }

    /* The peer's half already ended with an accepted RESET; this one repeats
     * it. The mapping is held for our own send half only. */
    if (e->peer_reset_accepted) return MOQ_OK;

    /* Refused at admission (ready to retry or not): the session never saw
     * the stream, so there is nothing to reset there; retire the mapping (the
     * adapter drops the chunk it was holding). */
    if (e->held_input) { bridge_deactivate_stream(b, e); return MOQ_OK; }

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

    /* A reset still owed from an earlier refusal keeps its first code: a
     * repeat delivers that cause, never its own. */
    uint64_t cause = e->pending_reset ? e->pending_reset_code : error_code;
    moq_result_t rc;
    if (e->kind == BRIDGE_STREAM_BIDI)
        rc = moq_session_on_bidi_stream_reset(
            b->session, e->ref, cause, now_us);
    else
        rc = moq_session_on_data_reset(
            b->session, e->ref, cause, now_us);

    if (rc == MOQ_ERR_WOULD_BLOCK) {
        /* The first peer code is the stream's cause; a repeat while it is
         * still owed does not overwrite it. */
        if (!e->pending_reset) {
            e->pending_reset = true;
            e->pending_reset_code = error_code;
        }
        return MOQ_ERR_WOULD_BLOCK;
    }

    if (rc < 0) {
        bridge_set_fatal(b, 0x1);
        return rc;
    }

    e->pending_reset = false;            /* delivered once, here */
    bridge_peer_reset_accepted(b, e);
    return MOQ_OK;
}

moq_result_t moq_transport_bridge_on_peer_stream_terminal(
    moq_transport_bridge_t *b, uint64_t stream_id, uint64_t now_us)
{
    (void)now_us;
    if (!b || b->fatal || b->closed) return MOQ_ERR_CLOSED;
    /* Latch only: an endpoint op may still have this entry borrowed. */
    bridge_stream_entry_t *e = bridge_find_by_id(b, stream_id);
    bridge_note_aborting_terminal(b, e);
    /* A peer data stream: keep the fact that the transport is done with it.
     * It is acted on only by a service pass (never from inside this call, so
     * a report made from within an endpoint op cannot reach the frame that
     * called the op), and only once the session's STOP of the stream was
     * accepted: the report alone never releases a live owner. An unknown
     * stream allocates nothing. Any other stream: harmless no-op. */
    if (e && bridge_is_peer_data_receive(e) && !e->transport_final) {
        e->transport_final = true;
        if (e->local_stop_accepted) b->local_retire_due = true;
    }
    return MOQ_OK;
}

moq_result_t moq_transport_bridge_on_local_stream_committed(
    moq_transport_bridge_t *b, uint64_t stream_id, uint64_t now_us)
{
    if (!b || b->fatal || b->closed) return MOQ_ERR_CLOSED;
    bridge_stream_entry_t *e = bridge_find_by_id(b, stream_id);
    if (!e || !e->awaiting_commit || e->peer_stop_received ||
        e->data_terminal_received ||
        bridge_session_has_data_terminal(b->session, e->ref)) return MOQ_OK;
    moq_stream_ref_t ref = e->ref;
    moq_result_t rc = moq_session_on_data_committed(b->session, ref, now_us);
    if (rc != MOQ_OK && rc != MOQ_ERR_CLOSED) return rc;
    /* Resolve session ownership after its advancing input. An already-queued
     * reset can outlive the subgroup slot and still needs this mapping until
     * the endpoint accepts it; the clock alone does not establish ownership. */
    if (!bridge_session_has_queued_data_reset(b->session, ref) &&
        !bridge_session_has_data_terminal(b->session, ref))
        bridge_deactivate_stream(b, e);
    return MOQ_OK;
}

moq_result_t moq_transport_bridge_on_peer_stop_sending(
    moq_transport_bridge_t *b, uint64_t stream_id,
    uint64_t error_code, uint64_t now_us)
{
    if (!b || b->fatal || b->closed) return MOQ_ERR_CLOSED;

    bridge_stream_entry_t *e = bridge_find_by_id(b, stream_id);
    if (!e) return MOQ_OK;

    if (e->aborting) {
        /* STOP_SENDING only asks us to stop our SEND half; the peer may
         * keep sending on their half. Do NOT retire — keep discarding
         * (mapping retained) until FIN / RESET / full-stream terminal. */
        return MOQ_OK;
    }

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
     * opened it. Either way the answer is a RESET of that half (RFC 9000 §3.5),
     * queued for service rather than issued re-entrantly from this callback.
     *
     * What differs is the OTHER direction. On a bidi WE opened, the peer's send
     * half still carries the response: draft-18 §3.3.2 has a rejecting endpoint
     * send REQUEST_ERROR and FIN, which can only arrive if the receive half and
     * its request owner survive. Cancelling the whole request here would free
     * that owner and make the response parse as a fresh inbound request.
     *
     * On a bidi the PEER opened, the STOP is the requester cancelling the
     * request it made, so it is delivered to the session through the dedicated
     * bidi-stop input -- distinct from a RESET_STREAM, and never routed
     * through reset. */
    moq_result_t rc;
    if (e->kind == BRIDGE_STREAM_BIDI) {
        rc = bridge_stop_bidi_send_half(b, e, error_code);
        if (rc < 0) return rc;

        if (e->origin == BRIDGE_ORIGIN_LOCAL) {
            bridge_retire_or_tombstone(b, e->ref);
            return MOQ_OK;
        }
        /* The peer's RESET already ended this request at the session: its
         * STOP only answers our send half (reset above), and the request is
         * not cancelled a second time. */
        if (e->peer_reset_accepted) {
            bridge_retire_or_tombstone(b, e->ref);
            return MOQ_OK;
        }
        rc = moq_session_on_bidi_stream_stop(
            b->session, e->ref, error_code, now_us);
    } else if (e->origin == BRIDGE_ORIGIN_LOCAL) {
        const bridge_dstream_t *d = bridge_dstream_find(b, e->ref);
        if (d && d->has_reset)
            return bridge_stop_local_data(b, e, error_code, now_us);
        if (e->fin_reset_pending || e->peer_stop_received)
            return e->pending_stop ? MOQ_ERR_WOULD_BLOCK : MOQ_OK;
        if (e->data_terminal_received || e->subgroup_header_len != 0 ||
            bridge_session_is_outgoing_subgroup(b->session, e->ref)) {
            e->peer_stop_received = true;
            e->pending_stop_code = error_code;
            bridge_prune_terminal_output(b);
        }
        rc = bridge_stop_local_data(b, e, error_code, now_us);
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
    /* A bridge-local failure can become fatal before the transport reports
     * its terminal. The fatal latch tears down bridge state, but it does not
     * close the session; preserve that first cause and still deliver the
     * session terminal when the transport catches up. */
    if (b->fatal) {
        moq_session_on_transport_close(b->session, b->fatal_code, now_us);
        return MOQ_OK;
    }
    if (b->closed) return MOQ_OK;
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
    if (b->fatal) {
        moq_session_on_transport_close(b->session, b->fatal_code, now_us);
        return MOQ_OK;
    }
    if (b->closed) return MOQ_OK;
    bridge_set_fatal(b, code);
    moq_session_on_transport_close(b->session, code, now_us);
    return MOQ_OK;
}
