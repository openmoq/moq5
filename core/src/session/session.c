#include "session_internal.h"

/* Bounded backoff applied to the *reported* next deadline when a subgroup
 * delivery-timeout reset cannot be enqueued because the action queue is full.
 * The pending reset is retained (the raw per-subgroup deadline stays expired);
 * this only defers when timer drivers next poll so a backpressured peer cannot
 * drive a zero-sleep retry loop. A few milliseconds keeps reset latency low
 * once the queue drains while bounding wakeups to roughly the rate a managed
 * pump already uses for its idle tick. */
#define MOQ_SG_RETRY_BACKOFF_US 5000

/* -- Advancing-call bookkeeping ------------------------------------ */

static void session_refresh_idle(moq_session_t *s, uint64_t now_us)
{
    if (s->idle_timeout_us > 0 && s->state != MOQ_SESS_CLOSED)
        s->idle_deadline_us = deadline_add(now_us, s->idle_timeout_us);
}

/* True once the armed idle deadline has elapsed. Side-effect-free: callers
 * close via close_with_error so the result/contract stays uniform. Disabled
 * (idle_timeout_us == 0 -> idle_deadline_us == UINT64_MAX) and not-yet-expired
 * sessions report false. The idle timeout is authoritative once it fires:
 * checked before inbound activity refreshes the deadline so a peer that has
 * already gone silent past it cannot revive an expired session by sending a
 * byte just before the timer driver delivers the tick. Must be read after
 * session_begin_advance() has stamped last_now_us. */
static bool session_idle_expired(const moq_session_t *s)
{
    return s->idle_deadline_us != UINT64_MAX &&
           s->last_now_us >= s->idle_deadline_us;
}

void session_begin_advance(moq_session_t *s, uint64_t now_us)
{
    s->last_now_us = now_us;
    s->borrow_epoch++;

    if (s->action_head == s->action_tail) {
        s->send_len = 0;
        sg_reap_terminal(s);
    }

#ifndef NDEBUG
    if (s->output_scratch_len > 0)
        memset(s->output_scratch, 0xCD, s->output_scratch_len);
#endif
    s->output_scratch_len = 0;

    /* The event borrow arena holds spans referenced by still-queued
     * events; it can only recycle once the queue has drained (same
     * idiom as send_buf above, which waits for the action queue). */
    if (s->event_head == s->event_tail) {
#ifndef NDEBUG
        if (s->event_scratch_len > 0)
            memset(s->event_scratch, 0xCD, s->event_scratch_len);
#endif
        s->event_scratch_len = 0;
    }

    /* Retry any deferred PUBLISH_FINISHED whose Stream Count is now satisfied but
     * whose event could not be queued when its last data stream FIN'd. No-op
     * unless a subscriber-role PUBLISH_DONE is currently being gated. */
    pub_reap_deferred_dones(s);
}

/* -- Action resource cleanup --------------------------------------- */

void decref_queued_data_payloads(moq_session_t *s)
{
    for (size_t i = s->action_head; i < s->action_tail; i++) {
        moq_action_t *a = &s->actions[i % s->action_cap];
        if (a->kind == MOQ_ACTION_SEND_DATA && a->u.send_data.payload) {
            moq_rcbuf_decref(a->u.send_data.payload);
            a->u.send_data.payload = NULL;
        }
    }
}

void decref_queued_event_payloads(moq_session_t *s)
{
    for (size_t i = s->event_head; i < s->event_tail; i++) {
        moq_event_t *e = &s->events[i % s->event_cap];
        if (e->kind == MOQ_EVENT_OBJECT_RECEIVED) {
            if (e->u.object_received.payload) {
                size_t plen = moq_rcbuf_len(e->u.object_received.payload);
                if (plen <= s->recv_payload_bytes)
                    s->recv_payload_bytes -= plen;
                else
                    s->recv_payload_bytes = 0;
                moq_rcbuf_decref(e->u.object_received.payload);
                e->u.object_received.payload = NULL;
            }
            if (e->u.object_received.properties) {
                size_t elen = moq_rcbuf_len(e->u.object_received.properties);
                if (elen <= s->recv_payload_bytes)
                    s->recv_payload_bytes -= elen;
                else
                    s->recv_payload_bytes = 0;
                moq_rcbuf_decref(e->u.object_received.properties);
                e->u.object_received.properties = NULL;
            }
        } else if (e->kind == MOQ_EVENT_FETCH_OBJECT) {
            if (e->u.fetch_object.payload) {
                size_t plen = moq_rcbuf_len(e->u.fetch_object.payload);
                if (plen <= s->recv_payload_bytes)
                    s->recv_payload_bytes -= plen;
                else
                    s->recv_payload_bytes = 0;
                moq_rcbuf_decref(e->u.fetch_object.payload);
                e->u.fetch_object.payload = NULL;
            }
            if (e->u.fetch_object.properties) {
                size_t elen = moq_rcbuf_len(e->u.fetch_object.properties);
                if (elen <= s->recv_payload_bytes)
                    s->recv_payload_bytes -= elen;
                else
                    s->recv_payload_bytes = 0;
                moq_rcbuf_decref(e->u.fetch_object.properties);
                e->u.fetch_object.properties = NULL;
            }
        } else if (e->kind == MOQ_EVENT_OBJECT_CHUNK) {
            if (e->u.object_chunk.chunk) {
                moq_rcbuf_decref(e->u.object_chunk.chunk);
                e->u.object_chunk.chunk = NULL;
            }
            if (e->u.object_chunk.properties) {
                size_t elen = moq_rcbuf_len(e->u.object_chunk.properties);
                if (elen <= s->recv_payload_bytes)
                    s->recv_payload_bytes -= elen;
                else
                    s->recv_payload_bytes = 0;
                moq_rcbuf_decref(e->u.object_chunk.properties);
                e->u.object_chunk.properties = NULL;
            }
        }
    }
}

void free_rx_stream_bufs(moq_session_t *s)
{
    for (size_t i = 0; i < s->rx_cap; i++) {
        moq_rx_stream_t *rx = &s->rx_streams[i];
        if (rx->active) {
            moq_index_remove(s->idx_rx_by_ref, s->idx_rx_mask,
                              rx->stream_ref._v);
            if (rx->payload_rcbuf) {
                /* Payload assembled into an rcbuf but not yet emitted;
                 * payload_buf points into it, so decref instead of freeing. */
                moq_rcbuf_decref(rx->payload_rcbuf);
                if ((size_t)rx->payload_expected <= s->recv_payload_bytes)
                    s->recv_payload_bytes -= (size_t)rx->payload_expected;
                else
                    s->recv_payload_bytes = 0;
                rx->payload_rcbuf = NULL;
                rx->payload_buf = NULL;
            }
            if (rx->cur_extensions) {
                size_t ext_len = moq_rcbuf_len(rx->cur_extensions);
                if (ext_len <= s->recv_payload_bytes)
                    s->recv_payload_bytes -= ext_len;
                else
                    s->recv_payload_bytes = 0;
                moq_rcbuf_decref(rx->cur_extensions);
                rx->cur_extensions = NULL;
            }
            if (rx->pending_chunk) {
                moq_rcbuf_decref(rx->pending_chunk);
                rx->pending_chunk = NULL;
            }
            if (rx->input_buf) {
                if (rx->input_cap <= s->recv_input_bytes)
                    s->recv_input_bytes -= rx->input_cap;
                else
                    s->recv_input_bytes = 0;
                s->alloc.free(rx->input_buf, rx->input_cap, s->alloc.ctx);
                rx->input_buf = NULL;
            }
        }
        rx->active = false;
    }
}

/* -- Hash index helpers -------------------------------------------- */

static uint64_t idx_hash(uint64_t key) {
    key ^= key >> 33;
    key *= 0xFF51AFD7ED558CCDULL;
    key ^= key >> 33;
    key *= 0xC4CEB9FE1A85EC53ULL;
    key ^= key >> 33;
    return key;
}

int moq_index_find(const moq_index_entry_t *tbl, size_t mask, uint64_t key)
{
    size_t i = (size_t)(idx_hash(key) & mask);
    for (;;) {
        if (tbl[i].slot < 0) return -1;
        if (tbl[i].key == key) return tbl[i].slot;
        i = (i + 1) & mask;
    }
}

void moq_index_insert(moq_index_entry_t *tbl, size_t mask,
                       uint64_t key, int slot)
{
    size_t i = (size_t)(idx_hash(key) & mask);
    while (tbl[i].slot >= 0) {
        i = (i + 1) & mask;
    }
    tbl[i].key = key;
    tbl[i].slot = (int32_t)slot;
}

void moq_index_remove(moq_index_entry_t *tbl, size_t mask, uint64_t key)
{
    size_t i = (size_t)(idx_hash(key) & mask);
    for (;;) {
        if (tbl[i].slot < 0) return;
        if (tbl[i].key == key) break;
        i = (i + 1) & mask;
    }
    tbl[i].slot = -1;
    /* Backshift to maintain probe chains. */
    size_t j = i;
    for (;;) {
        j = (j + 1) & mask;
        if (tbl[j].slot < 0) break;
        size_t home = (size_t)(idx_hash(tbl[j].key) & mask);
        /* Check if j is displaced past its home position. */
        bool displaced = (j != home) &&
            ((j > i && (home <= i || home > j)) ||
             (j < i && (home <= i && home > j)));
        if (displaced) {
            tbl[i] = tbl[j];
            tbl[j].slot = -1;
            i = j;
        }
    }
}

/* -- Request registry helpers -------------------------------------- */

moq_request_endpoint_t request_registry_find_by_id(
    const moq_session_t *s, uint64_t request_id)
{
    int32_t packed = moq_index_find(s->idx_req_by_rid, s->idx_req_mask,
                                     request_id);
    if (packed < 0) return req_endpoint_none();
    moq_request_endpoint_t ep;
    memset(&ep, 0, sizeof(ep));
    ep.kind = req_kind(packed);
    ep.slot = req_slot(packed);
    ep.has_request_id = true;
    ep.request_id = request_id;
    return ep;
}

void request_registry_insert_by_id(
    moq_session_t *s, uint64_t request_id, moq_request_endpoint_t ep)
{
    moq_index_insert(s->idx_req_by_rid, s->idx_req_mask,
                     request_id, req_pack(ep.kind, ep.slot));
}

void request_registry_remove_by_id(
    moq_session_t *s, uint64_t request_id)
{
    moq_request_endpoint_t ep = request_registry_find_by_id(s, request_id);
    if (ep.kind != MOQ_REQ_NONE)
        s->profile->release_request(s, &ep);
    moq_index_remove(s->idx_req_by_rid, s->idx_req_mask, request_id);
}

/* Stream-ref-keyed registry. Pure secondary-index operations over
 * idx_req_by_streamref (see the header note). remove_by_streamref does not
 * release or clear the request pool slot and does not touch the request-id
 * index; the two indexes are independent keys onto the same pool, and a
 * terminal request path must remove every key it populated before the slot is
 * reused. Used by stream-correlated request profiles; draft-16 never calls
 * these. */
moq_request_endpoint_t request_registry_find_by_streamref(
    const moq_session_t *s, moq_stream_ref_t stream_ref)
{
    int32_t packed = moq_index_find(s->idx_req_by_streamref,
                                     s->idx_req_streamref_mask, stream_ref._v);
    if (packed < 0) return req_endpoint_none();
    moq_request_endpoint_t ep;
    memset(&ep, 0, sizeof(ep));
    ep.kind = req_kind(packed);
    ep.slot = req_slot(packed);
    ep.has_stream_ref = true;
    ep.stream_ref = stream_ref;
    return ep;
}

void request_registry_insert_by_streamref(
    moq_session_t *s, moq_stream_ref_t stream_ref, moq_request_endpoint_t ep)
{
    moq_index_insert(s->idx_req_by_streamref, s->idx_req_streamref_mask,
                     stream_ref._v, req_pack(ep.kind, ep.slot));
}

void request_registry_remove_by_streamref(
    moq_session_t *s, moq_stream_ref_t stream_ref)
{
    moq_index_remove(s->idx_req_by_streamref, s->idx_req_streamref_mask,
                     stream_ref._v);
}

/* -- Profile capability accessors ----------------------------------- *
 * The core consults these instead of testing a draft version. NULL hooks fall
 * back to behavior identical to draft-16. */

bool moq_session_uses_request_streams(const moq_session_t *s)
{
    return s->profile->uses_request_streams;
}

bool moq_session_uses_uni_control(const moq_session_t *s)
{
    /* Explicit topology capability (independent of whether a uni-stream
     * classifier is installed). Draft-16 sets this false. */
    return s->profile->uses_uni_control_channel;
}

moq_uni_class_t moq_session_classify_peer_uni(const moq_session_t *s,
                                              const uint8_t *data, size_t len)
{
    /* Profiles without a unidirectional control channel (draft-16) leave the
     * hook NULL; every peer unidirectional stream is data. */
    if (!s->profile->classify_uni_stream)
        return MOQ_UNI_CLASS_DATA;
    return s->profile->classify_uni_stream(data, len);
}

moq_result_t moq_session_validate_inbound_request_stream(
    moq_session_t *s, moq_stream_ref_t ref, uint64_t msg_type,
    uint64_t wire_request_id, moq_request_endpoint_t *out)
{
    /* Profiles that do not open a bidi stream per request (draft-16) leave the
     * hook NULL; this path is never taken for them. */
    if (!s->profile->validate_inbound_request_stream) {
        if (out) *out = req_endpoint_none();
        return MOQ_ERR_INVAL;
    }
    return s->profile->validate_inbound_request_stream(s, ref, msg_type,
                                                       wire_request_id, out);
}

/* -- Queue helpers ------------------------------------------------- */

bool action_queue_full(const moq_session_t *s)
{
    return (s->action_tail - s->action_head) >= s->action_cap;
}

size_t moq_session_action_capacity(const moq_session_t *s)
{
    if (!s) return 0;
    size_t used = s->action_tail - s->action_head;
    return used >= s->action_cap ? 0 : s->action_cap - used;
}

moq_result_t push_action(moq_session_t *s, const moq_action_t *a)
{
    if (action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
    s->actions[s->action_tail % s->action_cap] = *a;
    s->action_tail++;
    session_refresh_idle(s, s->last_now_us);
    return MOQ_OK;
}

bool event_queue_full(const moq_session_t *s)
{
    return (s->event_tail - s->event_head) >= s->event_cap;
}

moq_result_t push_event(moq_session_t *s, const moq_event_t *e)
{
    if (event_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
    s->events[s->event_tail % s->event_cap] = *e;
    s->event_tail++;
    return MOQ_OK;
}

/* -- Output scratch ------------------------------------------------ */

uint8_t *scratch_alloc(moq_session_t *s, size_t len)
{
    if (len == 0) return s->output_scratch + s->output_scratch_len;
    if (len > s->output_scratch_cap - s->output_scratch_len) return NULL;
    uint8_t *p = s->output_scratch + s->output_scratch_len;
    s->output_scratch_len += len;
    return p;
}

void *scratch_alloc_aligned(moq_session_t *s, size_t len, size_t align)
{
    if (align == 0) align = 1;
    if ((align & (align - 1)) != 0) return NULL;
    size_t cur = s->output_scratch_len;
    size_t aligned = (cur + align - 1) & ~(align - 1);
    if (aligned < cur) return NULL;
    if (aligned > s->output_scratch_cap) return NULL;
    size_t remaining = s->output_scratch_cap - aligned;
    if (len > remaining) return NULL;
    s->output_scratch_len = aligned + len;
    return s->output_scratch + aligned;
}

uint8_t *scratch_copy(moq_session_t *s, const uint8_t *data, size_t len)
{
    if (len == 0) return NULL;
    uint8_t *p = scratch_alloc(s, len);
    if (!p) return NULL;
    memcpy(p, data, len);
    return p;
}

/* -- Event borrow arena --------------------------------------------
 * Same allocators against the event arena. Anything copied here is
 * referenced by a queued moq_event_t and must survive later advancing
 * calls in the same batch; the arena recycles in session_begin_advance
 * once the event queue is empty. Exhaustion returns NULL, failing the
 * producing path exactly like transient scratch exhaustion would. */

uint8_t *event_scratch_alloc(moq_session_t *s, size_t len)
{
    if (len == 0) return s->event_scratch + s->event_scratch_len;
    if (len > s->event_scratch_cap - s->event_scratch_len) return NULL;
    uint8_t *p = s->event_scratch + s->event_scratch_len;
    s->event_scratch_len += len;
    return p;
}

void *event_scratch_alloc_aligned(moq_session_t *s, size_t len, size_t align)
{
    if (align == 0) align = 1;
    if ((align & (align - 1)) != 0) return NULL;
    size_t cur = s->event_scratch_len;
    size_t aligned = (cur + align - 1) & ~(align - 1);
    if (aligned < cur) return NULL;
    if (aligned > s->event_scratch_cap) return NULL;
    size_t remaining = s->event_scratch_cap - aligned;
    if (len > remaining) return NULL;
    s->event_scratch_len = aligned + len;
    return s->event_scratch + aligned;
}

uint8_t *event_scratch_copy(moq_session_t *s, const uint8_t *data, size_t len)
{
    if (len == 0) return NULL;
    uint8_t *p = event_scratch_alloc(s, len);
    if (!p) return NULL;
    memcpy(p, data, len);
    return p;
}

/* Predicate: would an `align`-aligned array of `count * elem_size` bytes,
 * followed by `tail_bytes` of unaligned data, BOTH fit in the event scratch
 * arena starting at the current event_scratch_len? Uses the SAME alignment math
 * as event_scratch_alloc_aligned(), so a preflight with this helper exactly
 * matches what the subsequent aligned array allocation + manual tail memcpy will
 * consume -- including the alignment padding the bare `array+tail <= cap-len`
 * checks missed (a padding-sized tail overflow). Pure: never mutates scratch.
 * Rejects the count*elem_size multiplication overflow and computes the capacity
 * test by subtraction so no addition can wrap. */
bool event_scratch_fits_aligned(const moq_session_t *s, size_t count,
                                size_t elem_size, size_t tail_bytes,
                                size_t align)
{
    if (align == 0) align = 1;
    if ((align & (align - 1)) != 0) return false;
    if (elem_size != 0 && count > SIZE_MAX / elem_size) return false;
    size_t array_bytes = count * elem_size;
    size_t cur = s->event_scratch_len;
    size_t aligned = (cur + align - 1) & ~(align - 1);
    if (aligned < cur) return false;                 /* alignment overflow */
    if (aligned > s->event_scratch_cap) return false;
    size_t remaining = s->event_scratch_cap - aligned;
    if (array_bytes > remaining) return false;
    remaining -= array_bytes;
    if (tail_bytes > remaining) return false;
    return true;
}

/* -- Send helper --------------------------------------------------- */

moq_result_t queue_send_control(moq_session_t *s,
                                 const uint8_t *data, size_t len)
{
    if (len == 0) return MOQ_OK;
    if (action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
    if (len > s->send_cap - s->send_len) return MOQ_ERR_BUFFER;
    memcpy(s->send_buf + s->send_len, data, len);

    moq_action_t a;
    memset(&a, 0, sizeof(a));
    a.kind = MOQ_ACTION_SEND_CONTROL;
    a.detail_size = (uint32_t)sizeof(moq_send_control_action_t);
    a.borrow_epoch = s->borrow_epoch;
    a.u.send_control.data = s->send_buf + s->send_len;
    a.u.send_control.len  = len;
    moq_result_t rc = push_action(s, &a);
    if (rc < 0) return rc;

    s->send_len += len;
    return MOQ_OK;
}

moq_result_t queue_send_bidi(moq_session_t *s, moq_stream_ref_t ref,
                              const uint8_t *data, size_t len, bool fin)
{
    /* Once a per-request GOAWAY has been sent on this request bidi the request is
     * migrating (§10.4): the peer treats the GOAWAY as terminal and closes on any
     * trailing bytes, so no further control message (SUBSCRIBE_OK / FETCH_OK /
     * TRACK_STATUS_OK / PUBLISH_DONE / REQUEST_UPDATE / NAMESPACE / PUBLISH_BLOCKED
     * / …) may be written here. Data streams are unaffected -- they are separate
     * uni streams, not this action. The GOAWAY itself is queued before its marker
     * is set, so it is never blocked by this guard. */
    if (request_goaway_already_sent(s, ref)) return MOQ_ERR_WRONG_STATE;
    if (action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
    /* Distinguish a message that can never fit (permanent failure) from a
     * temporary shortfall in the remaining buffer (retryable): the latter must
     * not be a hard error, or the dispatch frees the request slot and the
     * required response is lost. */
    if (len > s->send_cap) return MOQ_ERR_BUFFER;
    if (len > s->send_cap - s->send_len) return MOQ_ERR_WOULD_BLOCK;
    memcpy(s->send_buf + s->send_len, data, len);

    moq_action_t a;
    memset(&a, 0, sizeof(a));
    a.kind = MOQ_ACTION_SEND_BIDI_STREAM;
    a.detail_size = (uint32_t)sizeof(moq_send_bidi_stream_action_t);
    a.borrow_epoch = s->borrow_epoch;
    a.u.send_bidi_stream.stream_ref = ref;
    a.u.send_bidi_stream.data = s->send_buf + s->send_len;
    a.u.send_bidi_stream.len  = len;
    a.u.send_bidi_stream.fin  = fin;
    moq_result_t rc = push_action(s, &a);
    if (rc < 0) return rc;

    s->send_len += len;
    return MOQ_OK;
}

moq_result_t reject_apply_redirect(moq_session_t *s,
                                   moq_request_error_encode_args_t *args,
                                   const moq_redirect_target_t *rd,
                                   bool namespace_scoped)
{
    bool is_redirect = args->error_code == MOQ_REQUEST_ERROR_REDIRECT;
    bool tail_set = rd && (rd->connect_uri.len > 0 ||
                           rd->track_namespace.count > 0 ||
                           rd->track_name.len > 0);
    if (!is_redirect) {
        /* A redirect target only travels with the REDIRECT error code. */
        return tail_set ? MOQ_ERR_INVAL : MOQ_OK;
    }
    /* REDIRECT is a draft-18 REQUEST_ERROR tail on the request bidi. */
    if (!moq_session_uses_request_streams(s)) return MOQ_ERR_INVAL;
    if (s->perspective == MOQ_PERSPECTIVE_CLIENT && rd && rd->connect_uri.len > 0)
        return MOQ_ERR_INVAL;                 /* client must send a zero-length URI */
    if (namespace_scoped && rd && rd->track_name.len > 0)
        return MOQ_ERR_INVAL;                 /* namespace-scoped: empty track name */
    /* Validate the (public, caller-supplied) redirect target before it is read by
     * the encoder's size pass: bounded namespace count, non-NULL spans, and
     * non-empty namespace fields (draft-18 §1.4.2: 0..32 fields, each non-empty). */
    if (rd) {
        if (rd->connect_uri.len > 0 && !rd->connect_uri.data) return MOQ_ERR_INVAL;
        if (rd->track_name.len > 0 && !rd->track_name.data) return MOQ_ERR_INVAL;
        if (rd->track_namespace.count > 32) return MOQ_ERR_INVAL;
        if (rd->track_namespace.count > 0 && !rd->track_namespace.parts)
            return MOQ_ERR_INVAL;
        for (size_t i = 0; i < rd->track_namespace.count; i++)
            if (rd->track_namespace.parts[i].len == 0 ||
                !rd->track_namespace.parts[i].data)
                return MOQ_ERR_INVAL;
    }
    /* error_code == REDIRECT always emits the tail (an all-empty Redirect is valid). */
    args->has_redirect = true;
    if (rd) {
        args->connect_uri = rd->connect_uri.data;
        args->connect_uri_len = rd->connect_uri.len;
        args->redirect_namespace = rd->track_namespace;
        args->redirect_track_name = rd->track_name.data;
        args->redirect_track_name_len = rd->track_name.len;
    }
    return MOQ_OK;
}

moq_result_t queue_request_error_bidi(moq_session_t *s, moq_stream_ref_t ref,
                                      const moq_request_error_encode_args_t *args)
{
    size_t scratch_saved = s->output_scratch_len;
    /* Upper bound: envelope header + error_code + retry + reason span + redirect
     * (uri span + namespace tuple + track-name span), with vi64 slack per field. */
    size_t bound = 48 + args->reason_len;
    if (args->has_redirect) {
        bound += args->connect_uri_len + args->redirect_track_name_len + 16;
        for (size_t i = 0; i < args->redirect_namespace.count; i++)
            bound += args->redirect_namespace.parts[i].len + 9;
    }
    uint8_t *buf = (uint8_t *)scratch_alloc_aligned(s, bound, 1);
    if (!buf) return MOQ_ERR_BUFFER;   /* message larger than the scratch arena */
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, bound);
    moq_result_t rc = s->profile->encode_request_error(s, &w, args);
    if (rc < 0) { s->output_scratch_len = scratch_saved; return rc; }
    /* queue_send_bidi copies into send_buf and gives BUFFER (never fits send_cap)
     * vs WOULD_BLOCK (transient remaining shortfall); restore scratch either way. */
    rc = queue_send_bidi(s, ref, buf, moq_buf_writer_offset(&w), true);
    s->output_scratch_len = scratch_saved;
    return rc;
}

void moq_request_goaway_cfg_init(moq_request_goaway_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = sizeof(moq_request_goaway_cfg_t);
}

moq_result_t session_core_send_request_goaway(
    moq_session_t *s, moq_request_family_t family, int slot,
    const uint8_t *uri, size_t uri_len, uint64_t timeout_ms)
{
    /* Draft-16 has no per-request GOAWAY (the encode op is NULL); the library
     * signals an unsupported draft-18 operation with MOQ_ERR_INVAL. */
    if (!s->profile->encode_request_goaway) return MOQ_ERR_INVAL;
    /* A client cannot instruct the server to connect elsewhere (§10.4). */
    if (s->perspective == MOQ_PERSPECTIVE_CLIENT && uri_len > 0)
        return MOQ_ERR_INVAL;
    /* Prevalidate the URI up front: a NULL or over-cap URI must be rejected
     * before it can drive an absurd `32 + uri_len` scratch allocation (the
     * encoder re-checks, but allocation happens first). 8192 = §10.4 cap. */
    if (uri_len > 0 && !uri) return MOQ_ERR_INVAL;
    if (uri_len > 8192) return MOQ_ERR_INVAL;

    /* Family × state eligibility (§10.4): the request must be live and the local
     * send half still open. Resolve the request bidi + the one-per-stream marker.
     * Per §10.4 the GOAWAY "does not impact subscription state", so the entry stays
     * live -- the app keeps producing on the old request until the peer tears the
     * old stream down (handled by request_goaway_free_on_teardown). */
    moq_stream_ref_t ref;
    bool *sent;
    bool eligible;
    switch (family) {
    case MOQ_REQUEST_FAMILY_SUBSCRIBE:
        eligible = s->subs[slot].state == MOQ_SUB_ESTABLISHED;
        ref = s->subs[slot].request_stream_ref;
        sent = &s->subs[slot].goaway_sent; break;
    case MOQ_REQUEST_FAMILY_FETCH:
        eligible = s->fetches[slot].state == MOQ_FETCH_ACCEPTED;
        ref = s->fetches[slot].request_stream_ref;
        sent = &s->fetches[slot].goaway_sent; break;
    case MOQ_REQUEST_FAMILY_PUBLISH:
        eligible = s->publishes[slot].state == MOQ_PUB_ESTABLISHED;
        ref = s->publishes[slot].request_stream_ref;
        sent = &s->publishes[slot].goaway_sent; break;
    case MOQ_REQUEST_FAMILY_ANNOUNCEMENT:
        eligible = s->announcements[slot].state == MOQ_ANN_ESTABLISHED;
        ref = s->announcements[slot].request_stream_ref;
        sent = &s->announcements[slot].goaway_sent; break;
    case MOQ_REQUEST_FAMILY_NS_SUB:
        eligible = s->ns_subs[slot].state == MOQ_NS_SUB_ESTABLISHED;
        ref = s->ns_subs[slot].stream_ref;
        sent = &s->ns_subs[slot].goaway_sent; break;
    case MOQ_REQUEST_FAMILY_SUBSCRIBE_TRACKS:
        eligible = s->track_subs[slot].state == MOQ_TRACK_SUB_ESTABLISHED;
        ref = s->track_subs[slot].request_stream_ref;
        sent = &s->track_subs[slot].goaway_sent; break;
    case MOQ_REQUEST_FAMILY_TRACK_STATUS:
        /* The requester opened the bidi with FIN; only the responder's send half
         * is still open, so only it may migrate (before answering). */
        eligible = s->track_statuses[slot].state == MOQ_TS_PENDING_PUBLISHER;
        ref = s->track_statuses[slot].request_stream_ref;
        sent = &s->track_statuses[slot].goaway_sent; break;
    default:
        return MOQ_ERR_INVAL;
    }
    if (!eligible || ref._v == 0) return MOQ_ERR_WRONG_STATE;
    if (*sent) return MOQ_ERR_WRONG_STATE;          /* one GOAWAY per request stream */

    /* Reserve-before-mutate: the action slot, then a sized scratch encode + a
     * queue_send_bidi with fin=false (our send half stays open for the deferred
     * timeout-reset; data streams stay alive). On success we only mark goaway_sent
     * -- the entry stays live so old-session output continues; the peer's
     * spec-mandated old-stream teardown is absorbed later. */
    if (action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
    size_t scratch_saved = s->output_scratch_len;
    uint8_t *buf = (uint8_t *)scratch_alloc_aligned(s, 32 + uri_len, 1);
    if (!buf) return MOQ_ERR_BUFFER;
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, 32 + uri_len);
    moq_result_t rc = s->profile->encode_request_goaway(s, &w, uri, uri_len,
                                                        timeout_ms);
    if (rc < 0) { s->output_scratch_len = scratch_saved; return rc; }
    rc = queue_send_bidi(s, ref, buf, moq_buf_writer_offset(&w), false);
    s->output_scratch_len = scratch_saved;
    if (rc < 0) return rc;
    *sent = true;
    return MOQ_OK;
}

/* Queue a graceful FIN of our send half on a request bidi (CLOSE_BIDI_STREAM).
 * Stream-correlated profiles use this to close the requester half after a
 * terminal response so the peer can retire the request bidi; it carries no data.
 * Returns MOQ_ERR_WOULD_BLOCK if the action queue is full (retryable). */
moq_result_t queue_close_bidi(moq_session_t *s, moq_stream_ref_t ref)
{
    if (action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
    moq_action_t a;
    memset(&a, 0, sizeof(a));
    a.kind = MOQ_ACTION_CLOSE_BIDI_STREAM;
    a.detail_size = (uint32_t)sizeof(moq_close_bidi_stream_action_t);
    a.borrow_epoch = s->borrow_epoch;
    a.u.close_bidi_stream.stream_ref = ref;
    return push_action(s, &a);
}

moq_result_t queue_open_uni_control(moq_session_t *s, moq_stream_ref_t ref,
                                    const uint8_t *data, size_t len)
{
    if (len == 0) return MOQ_OK;
    if (action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
    if (len > s->send_cap - s->send_len) return MOQ_ERR_BUFFER;
    memcpy(s->send_buf + s->send_len, data, len);

    moq_action_t a;
    memset(&a, 0, sizeof(a));
    a.kind = MOQ_ACTION_OPEN_UNI_CONTROL;
    a.detail_size = (uint32_t)sizeof(moq_open_uni_control_action_t);
    a.borrow_epoch = s->borrow_epoch;
    a.u.open_uni_control.stream_ref = ref;
    a.u.open_uni_control.data = s->send_buf + s->send_len;
    a.u.open_uni_control.len  = len;
    moq_result_t rc = push_action(s, &a);
    if (rc < 0) return rc;

    s->send_len += len;
    return MOQ_OK;
}

/*
 * Close the session with an error code. Internal helper.
 *
 * reason must point to static storage (string literal) or NULL.
 * The pointer is stored directly in the closed event without copy.
 * Do not pass heap-allocated or stack-allocated reason strings.
 */
moq_result_t close_with_error(moq_session_t *s,
                               uint64_t code, const char *reason)
{
    s->state = MOQ_SESS_CLOSED;
    s->goaway_deadline_us = UINT64_MAX;
    s->subgroup_deadline_us = UINT64_MAX;
    s->subgroup_retry_deadline_us = UINT64_MAX;
    s->idle_deadline_us = UINT64_MAX;
    decref_queued_data_payloads(s);
    decref_queued_event_payloads(s);
    free_rx_stream_bufs(s);
    s->action_head = s->action_tail = 0;
    s->send_len = 0;
    s->event_head = s->event_tail = 0;

    /* Invalidate all subgroup entries. */
    for (size_t i = 0; i < s->sg_cap; i++)
        if (s->subgroups[i].state != MOQ_SG_FREE)
            sg_free_entry(i, s->subgroups);

    moq_action_t a;
    memset(&a, 0, sizeof(a));
    a.kind = MOQ_ACTION_CLOSE_SESSION;
    a.detail_size = (uint32_t)sizeof(moq_close_session_action_t);
    a.borrow_epoch = s->borrow_epoch;
    a.u.close_session.code = code;
    if (reason) {
        a.u.close_session.reason.data = (const uint8_t *)reason;
        a.u.close_session.reason.len  = strlen(reason);
    }
    push_action(s, &a);

    moq_event_t e;
    memset(&e, 0, sizeof(e));
    e.kind = MOQ_EVENT_SESSION_CLOSED;
    e.detail_size = (uint32_t)sizeof(moq_session_closed_event_t);
    e.borrow_epoch = s->borrow_epoch;
    e.u.closed.code = code;
    if (reason) {
        e.u.closed.reason.data = (const uint8_t *)reason;
        e.u.closed.reason.len  = strlen(reason);
    }
    push_event(s, &e);
    return MOQ_OK;
}

/* -- Input handlers ------------------------------------------------ */

static moq_result_t handle_control_bytes(moq_session_t *s,
                                          const uint8_t *buf, size_t len)
{
    if (s->state == MOQ_SESS_CLOSED) return MOQ_ERR_CLOSED;
    if (len > 0 && !buf) return MOQ_ERR_INVAL;

    if (len > 0) {
        if (len > s->recv_cap - s->recv_len) return MOQ_ERR_BUFFER;
        memcpy(s->recv_buf + s->recv_len, buf, len);
        s->recv_len += len;
    }

    if (s->recv_len == 0) return MOQ_OK;

    size_t consumed = 0;
    moq_result_t rc = s->profile->process_control_data(
        s, s->recv_buf, s->recv_len, &consumed);

    if (consumed > 0 && consumed < s->recv_len)
        memmove(s->recv_buf, s->recv_buf + consumed,
                s->recv_len - consumed);
    s->recv_len -= consumed;

    return rc;
}

static moq_result_t handle_tick(moq_session_t *s)
{
    if (s->state == MOQ_SESS_CLOSED) return MOQ_ERR_CLOSED;

    if (session_idle_expired(s))
        return close_with_error(s, MOQ_CLOSE_IDLE_TIMEOUT,
                                "idle timeout");

    if (s->goaway_deadline_us != UINT64_MAX &&
        s->last_now_us >= s->goaway_deadline_us) {
        s->goaway_deadline_us = UINT64_MAX;
        return close_with_error(s, MOQ_CLOSE_GOAWAY_TIMEOUT,
                                "GOAWAY drain timeout");
    }

    /* Subgroup delivery timeout: reset expired subgroups. */
    if (s->subgroup_deadline_us != UINT64_MAX &&
        s->last_now_us >= s->subgroup_deadline_us) {
        for (size_t i = 0; i < s->sg_cap; i++) {
            moq_sg_entry_t *sg = &s->subgroups[i];
            if (sg->state == MOQ_SG_FREE) continue;
            if (sg->delivery_deadline_us == UINT64_MAX) continue;
            if (s->last_now_us < sg->delivery_deadline_us) continue;
            if (sg->state == MOQ_SG_CLOSING ||
                sg->state == MOQ_SG_RESETTING) {
                sg->delivery_deadline_us = UINT64_MAX;
                continue;
            }
            if (action_queue_full(s)) {
                sg_recompute_deadline(s);
                /* The reset is still pending (the per-subgroup deadline
                 * stays expired), but the action queue is full. Defer the
                 * *reported* next deadline by a bounded backoff so timer
                 * drivers sleep until then instead of busy-spinning a
                 * zero-length wait on the already-expired deadline. */
                s->subgroup_retry_deadline_us =
                    deadline_add(s->last_now_us, MOQ_SG_RETRY_BACKOFF_US);
                return MOQ_ERR_WOULD_BLOCK;
            }
            moq_action_t a;
            memset(&a, 0, sizeof(a));
            a.kind = MOQ_ACTION_RESET_DATA;
            a.detail_size = (uint32_t)sizeof(moq_reset_data_action_t);
            a.borrow_epoch = s->borrow_epoch;
            a.u.reset_data.stream_ref = sg->stream_ref;
            a.u.reset_data.error_code = MOQ_RESET_DELIVERY_TIMEOUT;
            moq_result_t arc = push_action(s, &a);
            if (arc < 0) {
                sg_recompute_deadline(s);
                s->subgroup_retry_deadline_us =
                    deadline_add(s->last_now_us, MOQ_SG_RETRY_BACKOFF_US);
                return arc;
            }
            sg->state = MOQ_SG_RESETTING;
            sg->streaming_payload_len = 0;
            sg->streaming_bytes_written = 0;
            sg->delivery_deadline_us = UINT64_MAX;
        }
        sg_recompute_deadline(s);
    }

    /* All due subgroup resets were enqueued (or none were due): no deferred
     * reset remains, so drop any backoff armed by a prior WOULD_BLOCK. */
    s->subgroup_retry_deadline_us = UINT64_MAX;
    return MOQ_OK;
}

/* -- Datagrams held for an unestablished alias (reordering buffer) ---- */

static void staged_slot_free(moq_session_t *s, size_t i)
{
    moq_staged_datagram_t *e = &s->staged_dg[i];
    if (!e->in_use) return;
    if (e->bytes) s->alloc.free(e->bytes, e->len, s->alloc.ctx);
    if (e->len <= s->staged_bytes) s->staged_bytes -= e->len;
    else                           s->staged_bytes = 0;
    *e = (moq_staged_datagram_t){0};
    if (s->staged_count) s->staged_count--;
}

/* Free all held datagrams but keep the ring array (discard, not destroy). */
static void staged_clear(moq_session_t *s)
{
    if (!s->staged_dg) return;
    for (size_t i = 0; i < s->staged_cap; i++) staged_slot_free(s, i);
}

/* Free only held datagrams whose alias is NOT established. An established
 * alias's held data is still deliverable (merely stuck under event-queue
 * pressure) and must not be dropped just because no subscription is pending. */
static void staged_clear_unresolved(moq_session_t *s)
{
    if (!s->staged_dg) return;
    for (size_t i = 0; i < s->staged_cap; i++) {
        if (s->staged_dg[i].in_use &&
            sub_find_by_alias_subscriber(s, s->staged_dg[i].alias) < 0)
            staged_slot_free(s, i);
    }
}

/* Free held datagrams and the ring array itself (session teardown). */
static void free_staged_datagrams(moq_session_t *s)
{
    if (!s->staged_dg) return;
    staged_clear(s);
    s->alloc.free(s->staged_dg, s->staged_cap * sizeof(*s->staged_dg),
                  s->alloc.ctx);
    s->staged_dg = NULL;
}

/* Evict the lowest-seq held datagram (drop-oldest), counted. */
static void staged_evict_oldest(moq_session_t *s)
{
    size_t best = s->staged_cap;
    uint64_t best_seq = UINT64_MAX;
    for (size_t i = 0; i < s->staged_cap; i++) {
        if (s->staged_dg[i].in_use && s->staged_dg[i].seq < best_seq) {
            best = i;
            best_seq = s->staged_dg[i].seq;
        }
    }
    if (best < s->staged_cap) {
        staged_slot_free(s, best);
        s->staged_dropped++;
    }
}

/* Copy a datagram for later replay once its alias is established. Bounded by
 * slot count + a byte budget that shares the receive buffer; on overflow or
 * allocation failure the data is DROPPED (counted) -- the peer should not be
 * sending data for an unestablished alias, and the spec permits dropping it,
 * so this never fails the receive path with NOMEM. */
static void stage_datagram(moq_session_t *s, uint64_t alias,
                           const uint8_t *buf, size_t len)
{
    if (s->staged_cap == 0) { s->staged_dropped++; return; }
    if (len > s->staged_bytes_cap) { s->staged_dropped++; return; }
    if (!s->staged_dg) {
        size_t sz = s->staged_cap * sizeof(*s->staged_dg);
        s->staged_dg = (moq_staged_datagram_t *)s->alloc.alloc(sz, s->alloc.ctx);
        if (!s->staged_dg) { s->staged_dropped++; return; }
        memset(s->staged_dg, 0, sz);
    }
    while (s->staged_count > 0 && s->staged_bytes + len > s->staged_bytes_cap)
        staged_evict_oldest(s);
    size_t slot = s->staged_cap;
    for (size_t i = 0; i < s->staged_cap; i++)
        if (!s->staged_dg[i].in_use) { slot = i; break; }
    if (slot == s->staged_cap) { staged_evict_oldest(s);
        for (size_t i = 0; i < s->staged_cap; i++)
            if (!s->staged_dg[i].in_use) { slot = i; break; }
    }
    if (slot == s->staged_cap) { s->staged_dropped++; return; }
    uint8_t *copy = NULL;
    if (len > 0) {
        copy = (uint8_t *)s->alloc.alloc(len, s->alloc.ctx);
        if (!copy) { s->staged_dropped++; return; }
        memcpy(copy, buf, len);
    }
    s->staged_dg[slot] = (moq_staged_datagram_t){
        .in_use = true, .seq = s->staged_next_seq++,
        .alias = alias, .bytes = copy, .len = len };
    s->staged_count++;
    s->staged_bytes += len;
}

/* Decode + deliver one datagram. When allow_stage is set and the datagram
 * parses cleanly but its alias is not yet established (and a forwarding
 * subscription is pending), it is held for control/data reordering instead
 * of dropped. Replay passes allow_stage=false. */
static moq_result_t deliver_datagram(moq_session_t *s,
                                     const uint8_t *buf, size_t len,
                                     bool allow_stage)
{
    if (s->state == MOQ_SESS_CLOSED) return MOQ_ERR_CLOSED;
    if (!session_is_active(s)) return MOQ_ERR_WRONG_STATE;

    /* Zero-init: a padding/ignored datagram returns MOQ_DONE without
     * touching unknown_alias, so it must default false (never staged). */
    moq_decoded_object_datagram_t decoded = {0};
    moq_result_t rc = s->profile->decode_object_datagram(s, buf, len, &decoded);
    if (rc == MOQ_ERR_PROTO)
        return close_with_error(s, 0x3, "malformed object datagram");
    if (rc > 0) {
        if (allow_stage && decoded.unknown_alias &&
            session_has_forwarding_pending_subscriber(s))
            stage_datagram(s, decoded.track_alias, buf, len);
        return MOQ_OK;
    }
    if (rc < 0) return rc;
    if (s->state == MOQ_SESS_CLOSED) return MOQ_OK;

    return session_core_on_object_datagram(s, &decoded,
        decoded.payload, decoded.payload_len,
        decoded.properties, decoded.properties_len);
}

static moq_result_t handle_datagram(moq_session_t *s,
                                     const uint8_t *buf, size_t len)
{
    return deliver_datagram(s, buf, len, true);
}

/* Deliver held datagrams whose alias is now established, oldest first.
 * `only_alias` (when not UINT64_MAX) restricts to one alias. Stops on
 * event-queue backpressure, leaving the rest held for a later retry.
 *
 * Oldest-first is preserved by a min-seq scan of the whole fixed array per
 * delivered datagram, so this is O(cap^2). That is intentional and safe ONLY
 * because cap is tiny (MOQ_STAGED_DG_SLOTS = 16); do not raise the cap without
 * replacing this min-scan with an indexed/heap/ordered structure. */
static void staged_replay(moq_session_t *s, uint64_t only_alias)
{
    if (!s->staged_dg) return;
    for (;;) {
        /* Select the lowest-seq deliverable slot (oldest-first). */
        size_t best = s->staged_cap;
        uint64_t best_seq = UINT64_MAX;
        for (size_t i = 0; i < s->staged_cap; i++) {
            moq_staged_datagram_t *e = &s->staged_dg[i];
            if (!e->in_use) continue;
            if (only_alias != UINT64_MAX && e->alias != only_alias) continue;
            if (sub_find_by_alias_subscriber(s, e->alias) < 0) continue;
            if (e->seq < best_seq) { best = i; best_seq = e->seq; }
        }
        if (best == s->staged_cap) break;
        moq_result_t rc = deliver_datagram(s, s->staged_dg[best].bytes,
                                           s->staged_dg[best].len, false);
        if (rc == MOQ_ERR_WOULD_BLOCK) break;   /* no capacity; retry later */
        staged_slot_free(s, best);
    }
}

void session_release_staged_for_alias(moq_session_t *s, uint64_t alias)
{
    staged_replay(s, alias);
    session_resume_deferred_for_alias(s, alias);
}

void session_replay_staged(moq_session_t *s)
{
    if (s->staged_count) staged_replay(s, UINT64_MAX);
    session_retry_resumed_deferred(s);
}

void session_discard_staged_if_no_pending(moq_session_t *s)
{
    if (session_has_forwarding_pending_subscriber(s)) return;
    /* Drop only data for aliases that can never resolve now; data for an
     * already-established alias stays (it is just awaiting event capacity). */
    staged_clear_unresolved(s);
    session_discard_deferred_streams(s);
}

static moq_result_t session_step(moq_session_t *s, const moq_input_t *input)
{
    if (!s || !input) return MOQ_ERR_INVAL;

    session_begin_advance(s, input->now_us);

    switch (input->kind) {
    case MOQ_INPUT_START:
        return handle_start(s);
    case MOQ_INPUT_CONTROL_BYTES:
        if (session_idle_expired(s))
            return close_with_error(s, MOQ_CLOSE_IDLE_TIMEOUT,
                                    "idle timeout");
        if (input->u.control_bytes.len > 0)
            session_refresh_idle(s, input->now_us);
        return handle_control_bytes(s, input->u.control_bytes.buf,
                                    input->u.control_bytes.len);
    case MOQ_INPUT_TICK:
        return handle_tick(s);
    case MOQ_INPUT_DATAGRAM:
        if (session_idle_expired(s))
            return close_with_error(s, MOQ_CLOSE_IDLE_TIMEOUT,
                                    "idle timeout");
        session_refresh_idle(s, input->now_us);
        return handle_datagram(s, input->u.datagram.buf,
                               input->u.datagram.len);
    default:
        return MOQ_ERR_INVAL;
    }
}

/* -- Public API ---------------------------------------------------- */

/* Frozen v0 prefix: struct_size + alloc + perspective. A pointer-only init can
 * only safely touch these (it cannot know the caller's struct size), and
 * moq_session_create() requires at least this much. Must never shrink. */
#define MOQ_SESSION_CFG_V0_SIZE \
    (offsetof(moq_session_cfg_t, send_request_capacity))

void moq_session_cfg_init(moq_session_cfg_t *cfg,
                           const moq_alloc_t *alloc,
                           moq_perspective_t perspective)
{
    if (!cfg) return;
    /* Pointer-only: clear/stamp ONLY the frozen v0 prefix so an old caller's
     * smaller struct is never written past. Fields beyond the prefix are left
     * as-is and ignored by create (struct_size == V0); a caller that sets them
     * must use moq_session_cfg_init_sized(). */
    memset(cfg, 0, MOQ_SESSION_CFG_V0_SIZE);
    cfg->struct_size = (uint32_t)MOQ_SESSION_CFG_V0_SIZE;
    cfg->alloc = alloc;
    cfg->perspective = perspective;
}

void moq_session_cfg_init_sized(moq_session_cfg_t *cfg, size_t cfg_size,
                                 const moq_alloc_t *alloc,
                                 moq_perspective_t perspective)
{
    if (!cfg) return;
    /* Clear exactly what the caller allocated, never more than this library's
     * struct knows about (matches moq_pub_cfg_init_sized). */
    size_t n = cfg_size < sizeof(*cfg) ? cfg_size : sizeof(*cfg);
    if (n < sizeof(cfg->struct_size)) return;   /* too small to even stamp */
    memset(cfg, 0, n);
    cfg->struct_size = (uint32_t)n;
    if (n >= offsetof(moq_session_cfg_t, alloc) + sizeof(cfg->alloc))
        cfg->alloc = alloc;
    if (n >= offsetof(moq_session_cfg_t, perspective) + sizeof(cfg->perspective))
        cfg->perspective = perspective;
}

void moq_action_cleanup(moq_action_t *action)
{
    if (!action) return;
    if (action->kind == MOQ_ACTION_SEND_DATA && action->u.send_data.payload) {
        moq_rcbuf_decref(action->u.send_data.payload);
        action->u.send_data.payload = NULL;
    }
}

void moq_event_cleanup(moq_event_t *event)
{
    if (!event) return;
    if (event->kind == MOQ_EVENT_OBJECT_RECEIVED) {
        if (event->u.object_received.payload) {
            moq_rcbuf_decref(event->u.object_received.payload);
            event->u.object_received.payload = NULL;
        }
        if (event->u.object_received.properties) {
            moq_rcbuf_decref(event->u.object_received.properties);
            event->u.object_received.properties = NULL;
        }
    } else if (event->kind == MOQ_EVENT_FETCH_OBJECT) {
        if (event->u.fetch_object.payload) {
            moq_rcbuf_decref(event->u.fetch_object.payload);
            event->u.fetch_object.payload = NULL;
        }
        if (event->u.fetch_object.properties) {
            moq_rcbuf_decref(event->u.fetch_object.properties);
            event->u.fetch_object.properties = NULL;
        }
    } else if (event->kind == MOQ_EVENT_OBJECT_CHUNK) {
        if (event->u.object_chunk.chunk) {
            moq_rcbuf_decref(event->u.object_chunk.chunk);
            event->u.object_chunk.chunk = NULL;
        }
        if (event->u.object_chunk.properties) {
            moq_rcbuf_decref(event->u.object_chunk.properties);
            event->u.object_chunk.properties = NULL;
        }
    }
}

static size_t cfg_read_u32(const moq_session_cfg_t *cfg, size_t field_offset,
                            size_t field_size)
{
    if (cfg->struct_size >= field_offset + field_size)
        return *(const uint32_t *)((const char *)cfg + field_offset);
    return 0;
}

moq_result_t moq_session_create(const moq_session_cfg_t *cfg,
                                 uint64_t now_us,
                                 moq_session_t **out)
{
    if (out) *out = NULL;
    if (!cfg || !out) return MOQ_ERR_INVAL;
    if (cfg->struct_size < offsetof(moq_session_cfg_t, send_request_capacity))
        return MOQ_ERR_INVAL;
    if (!cfg->alloc || !cfg->alloc->alloc || !cfg->alloc->realloc ||
        !cfg->alloc->free)
        return MOQ_ERR_INVAL;
    if (cfg->perspective != MOQ_PERSPECTIVE_CLIENT &&
        cfg->perspective != MOQ_PERSPECTIVE_SERVER)
        return MOQ_ERR_INVAL;

    /* Validate request capacity config (values consumed by profile init). */
    {
        bool send_request_capacity = false;
        uint64_t initial_request_capacity = 0;
        if (cfg->struct_size >=
            offsetof(moq_session_cfg_t, send_request_capacity) +
            sizeof(cfg->send_request_capacity))
            send_request_capacity = cfg->send_request_capacity;
        if (send_request_capacity) {
            if (cfg->struct_size <
                offsetof(moq_session_cfg_t, initial_request_capacity) +
                sizeof(cfg->initial_request_capacity))
                return MOQ_ERR_INVAL;
            initial_request_capacity = cfg->initial_request_capacity;
        }
        if (send_request_capacity && initial_request_capacity > MOQ_QUIC_VARINT_MAX)
            return MOQ_ERR_INVAL;
    }

    bool send_auth_token_cache_size = false;
    uint64_t auth_token_cache_size = 0;
    if (cfg->struct_size >=
        offsetof(moq_session_cfg_t, send_auth_token_cache_size) +
        sizeof(cfg->send_auth_token_cache_size))
        send_auth_token_cache_size = cfg->send_auth_token_cache_size;
    if (send_auth_token_cache_size) {
        if (cfg->struct_size >=
            offsetof(moq_session_cfg_t, auth_token_cache_size) +
            sizeof(cfg->auth_token_cache_size))
            auth_token_cache_size = cfg->auth_token_cache_size;
    }

    size_t action_cap = cfg_read_u32(cfg,
        offsetof(moq_session_cfg_t, max_actions), sizeof(cfg->max_actions));
    size_t event_cap = cfg_read_u32(cfg,
        offsetof(moq_session_cfg_t, max_events), sizeof(cfg->max_events));
    size_t send_cap = cfg_read_u32(cfg,
        offsetof(moq_session_cfg_t, send_buffer_size), sizeof(cfg->send_buffer_size));
    size_t recv_cap = cfg_read_u32(cfg,
        offsetof(moq_session_cfg_t, recv_buffer_size), sizeof(cfg->recv_buffer_size));
    size_t sub_cap = cfg_read_u32(cfg,
        offsetof(moq_session_cfg_t, max_subscriptions), sizeof(cfg->max_subscriptions));
    size_t scratch_cap = cfg_read_u32(cfg,
        offsetof(moq_session_cfg_t, output_scratch_size), sizeof(cfg->output_scratch_size));

    size_t sg_cap = cfg_read_u32(cfg,
        offsetof(moq_session_cfg_t, max_open_subgroups), sizeof(cfg->max_open_subgroups));
    size_t rx_cap = cfg_read_u32(cfg,
        offsetof(moq_session_cfg_t, max_data_streams), sizeof(cfg->max_data_streams));
    size_t max_obj_payload = cfg_read_u32(cfg,
        offsetof(moq_session_cfg_t, max_object_payload_size), sizeof(cfg->max_object_payload_size));
    size_t max_recv_buf = cfg_read_u32(cfg,
        offsetof(moq_session_cfg_t, max_receive_buffer_bytes), sizeof(cfg->max_receive_buffer_bytes));
    size_t ann_cap = cfg_read_u32(cfg,
        offsetof(moq_session_cfg_t, max_announcements), sizeof(cfg->max_announcements));

    if (!action_cap)  action_cap  = MOQ_DEFAULT_MAX_ACTIONS;
    if (!event_cap)   event_cap   = MOQ_DEFAULT_MAX_EVENTS;
    if (sub_cap > 0xFFFF) return MOQ_ERR_INVAL; /* handle slot is 16-bit */
    if (!send_cap)    send_cap    = MOQ_DEFAULT_SEND_BUF;
    if (!recv_cap)    recv_cap    = MOQ_DEFAULT_RECV_BUF;
    if (!sub_cap)     sub_cap     = MOQ_DEFAULT_MAX_SUBS;
    if (!scratch_cap) scratch_cap = MOQ_DEFAULT_OUTPUT_SCRATCH;
    if (!sg_cap)      sg_cap      = MOQ_DEFAULT_MAX_SUBGROUPS;
    if (sg_cap > 0xFFFF) return MOQ_ERR_INVAL;
    if (!rx_cap)      rx_cap      = MOQ_DEFAULT_MAX_DATA_STREAMS;
    if (!max_obj_payload)  max_obj_payload  = MOQ_DEFAULT_MAX_OBJ_PAYLOAD;
    if (!max_recv_buf) max_recv_buf = MOQ_DEFAULT_MAX_RECV_BUF;
    if (!ann_cap)     ann_cap     = MOQ_DEFAULT_MAX_ANNOUNCEMENTS;
    if (ann_cap > 0xFFFF) return MOQ_ERR_INVAL;

    size_t fetch_cap = cfg_read_u32(cfg,
        offsetof(moq_session_cfg_t, max_fetches),
        sizeof(cfg->max_fetches));
    if (!fetch_cap)   fetch_cap   = MOQ_DEFAULT_MAX_FETCHES;
    if (fetch_cap > 0xFFFF) return MOQ_ERR_INVAL;

    size_t pub_cap = cfg_read_u32(cfg,
        offsetof(moq_session_cfg_t, max_publishes),
        sizeof(cfg->max_publishes));
    if (!pub_cap)     pub_cap     = MOQ_DEFAULT_MAX_PUBLISHES;
    if (pub_cap > 0xFFFF) return MOQ_ERR_INVAL;

    size_t ts_cap = cfg_read_u32(cfg,
        offsetof(moq_session_cfg_t, max_track_statuses),
        sizeof(cfg->max_track_statuses));
    if (!ts_cap)      ts_cap      = MOQ_DEFAULT_MAX_TRACK_STATUSES;
    if (ts_cap > 0xFFFF) return MOQ_ERR_INVAL;

    size_t ns_sub_cap = cfg_read_u32(cfg,
        offsetof(moq_session_cfg_t, max_namespace_subscriptions),
        sizeof(cfg->max_namespace_subscriptions));
    if (!ns_sub_cap)  ns_sub_cap  = MOQ_DEFAULT_MAX_NS_SUBS;
    if (ns_sub_cap > 0xFFFF) return MOQ_ERR_INVAL;
    size_t ns_sub_recv_cap = recv_cap < 4096 ? recv_cap : 4096;

    size_t track_sub_cap = cfg_read_u32(cfg,
        offsetof(moq_session_cfg_t, max_track_subscriptions),
        sizeof(cfg->max_track_subscriptions));
    if (!track_sub_cap)  track_sub_cap = MOQ_DEFAULT_MAX_TRACK_SUBS;
    if (track_sub_cap > 0xFFFF) return MOQ_ERR_INVAL;

    moq_version_t version = MOQ_VERSION_DRAFT_16;
    if (cfg->struct_size >= offsetof(moq_session_cfg_t, version) +
        sizeof(cfg->version)) {
        if (cfg->version != 0)
            version = cfg->version;
    }
    const moq_profile_ops_t *profile = moq_profile_lookup(version);
    if (!profile) return MOQ_ERR_INVAL;

#define ALIGN_UP(x, a) (((x) + (a) - 1) & ~((a) - 1))

    const size_t act_align    = _Alignof(moq_action_t);
    const size_t evt_align    = _Alignof(moq_event_t);
    const size_t sub_align    = _Alignof(moq_sub_entry_t);
    const size_t ann_align    = _Alignof(moq_ann_entry_t);
    const size_t fetch_align  = _Alignof(moq_fetch_entry_t);
    const size_t pub_align    = _Alignof(moq_pub_entry_t);
    const size_t ts_align     = _Alignof(moq_ts_entry_t);
    const size_t sg_align     = _Alignof(moq_sg_entry_t);
    const size_t rx_align     = _Alignof(moq_rx_stream_t);
    const size_t ns_sub_align = _Alignof(moq_ns_sub_entry_t);
    const size_t track_sub_align = _Alignof(moq_track_sub_entry_t);

    size_t off_actions = ALIGN_UP(sizeof(moq_session_t), act_align);
    size_t act_bytes   = action_cap * sizeof(moq_action_t);
    size_t off_events  = ALIGN_UP(off_actions + act_bytes, evt_align);
    size_t evt_bytes   = event_cap * sizeof(moq_event_t);
    size_t off_send    = off_events + evt_bytes;
    size_t off_recv    = off_send + send_cap;
    size_t off_subs    = ALIGN_UP(off_recv + recv_cap, sub_align);
    size_t sub_bytes   = sub_cap * sizeof(moq_sub_entry_t);
    size_t off_ann     = ALIGN_UP(off_subs + sub_bytes, ann_align);
    size_t ann_bytes   = ann_cap * sizeof(moq_ann_entry_t);
    size_t off_fetch   = ALIGN_UP(off_ann + ann_bytes, fetch_align);
    size_t fetch_bytes = fetch_cap * sizeof(moq_fetch_entry_t);
    size_t off_pub     = ALIGN_UP(off_fetch + fetch_bytes, pub_align);
    size_t pub_bytes   = pub_cap * sizeof(moq_pub_entry_t);
    size_t off_ts      = ALIGN_UP(off_pub + pub_bytes, ts_align);
    size_t ts_bytes    = ts_cap * sizeof(moq_ts_entry_t);
    size_t off_sg      = ALIGN_UP(off_ts + ts_bytes, sg_align);
    size_t sg_bytes    = sg_cap * sizeof(moq_sg_entry_t);
    size_t off_rx      = ALIGN_UP(off_sg + sg_bytes, rx_align);
    size_t rx_bytes    = rx_cap * sizeof(moq_rx_stream_t);
    size_t off_rx_fin  = ALIGN_UP(off_rx + rx_bytes, _Alignof(uint64_t));
    size_t rx_fin_cap  = rx_cap < 16 ? 16 : rx_cap;
    size_t rx_fin_bytes = rx_fin_cap * sizeof(uint64_t);
    size_t off_unsub_tomb = ALIGN_UP(off_rx_fin + rx_fin_bytes,
                                      _Alignof(uint64_t));
    size_t unsub_tomb_cap = sub_cap + pub_cap;
    size_t unsub_tomb_bytes = unsub_tomb_cap * sizeof(uint64_t);
    /* Bounded grace cache of locally-cancelled FETCH request IDs (see the field
     * comment in moq_session_t). The fetch slot is freed on cancel; this absorbs
     * a late response for the cancelled id without reoccupying the pool. */
    size_t off_fetch_cancel_tomb = ALIGN_UP(off_unsub_tomb + unsub_tomb_bytes,
                                            _Alignof(uint64_t));
    size_t fetch_cancel_tomb_cap = fetch_cap;
    size_t fetch_cancel_tomb_bytes = fetch_cancel_tomb_cap * sizeof(uint64_t);
    /* Drain ring of request-bidi stream_refs locally cancelled while their
     * response was still possibly in flight (stream-correlated profiles). Late
     * response bytes on these refs are discarded until FIN/reset instead of
     * being mistaken for a new inbound request. Draft-16 leaves it empty. */
    size_t off_drain_ref = ALIGN_UP(off_fetch_cancel_tomb + fetch_cancel_tomb_bytes,
                                     _Alignof(uint64_t));
    /* Drain refs absorb late in-flight responses on locally-cancelled request
     * bidis: subscriptions (unsubscribe), fetches (fetch-cancel),
     * announcements (publish_namespace_done / cancel_namespace), namespace
     * subscriptions (a locally-terminated ns_sub request bidi), track-status
     * requests (a locally-terminated accept/reject response bidi),
     * track-subscriptions (a locally-terminated SUBSCRIBE_TRACKS reject/teardown
     * response bidi), and publishes (a locally-terminated PUBLISH reject /
     * PUBLISH_DONE response bidi). */
    size_t drain_ref_cap = sub_cap + fetch_cap + ann_cap + ns_sub_cap + ts_cap +
                           track_sub_cap + pub_cap;
    size_t drain_ref_bytes = drain_ref_cap * sizeof(uint64_t);
    /* Parallel per-drain-ref reason byte (NORMAL absorb vs GOAWAY-strict). */
    size_t off_drain_reason = off_drain_ref + drain_ref_bytes;
    size_t drain_reason_bytes = drain_ref_cap * sizeof(uint8_t);
    size_t idx_sub_cap = moq_index_cap_for(sub_cap * 2 + ann_cap + fetch_cap + pub_cap + ts_cap + ns_sub_cap + track_sub_cap);
    size_t idx_rx_cap  = moq_index_cap_for(rx_cap);
    size_t idx_ns_cap  = moq_index_cap_for(ns_sub_cap);
    const size_t idx_align = _Alignof(moq_index_entry_t);
    size_t off_idx_sub = ALIGN_UP(off_drain_reason + drain_reason_bytes, idx_align);
    size_t idx_sub_bytes = idx_sub_cap * sizeof(moq_index_entry_t);
    size_t off_idx_rx  = ALIGN_UP(off_idx_sub + idx_sub_bytes, idx_align);
    size_t idx_rx_bytes = idx_rx_cap * sizeof(moq_index_entry_t);
    size_t off_ns_sub  = ALIGN_UP(off_idx_rx + idx_rx_bytes, ns_sub_align);
    size_t ns_sub_bytes = ns_sub_cap * sizeof(moq_ns_sub_entry_t);
    size_t off_idx_ns  = ALIGN_UP(off_ns_sub + ns_sub_bytes, idx_align);
    size_t idx_ns_bytes = idx_ns_cap * sizeof(moq_index_entry_t);
    /* Stream-ref request index: sized like idx_req_by_rid (holds at most the
     * same set of requests, one stream_ref each). Draft-16 leaves it empty. */
    size_t idx_reqref_cap = idx_sub_cap;
    size_t off_idx_reqref = ALIGN_UP(off_idx_ns + idx_ns_bytes, idx_align);
    size_t idx_reqref_bytes = idx_reqref_cap * sizeof(moq_index_entry_t);
    /* Subscriber-role established subscription alias -> slot index (replaces the
     * linear s->subs scan in sub_find_by_alias_subscriber on the receive path). */
    size_t idx_sub_alias_cap = moq_index_cap_for(sub_cap);
    size_t off_idx_sub_alias = ALIGN_UP(off_idx_reqref + idx_reqref_bytes, idx_align);
    size_t idx_sub_alias_bytes = idx_sub_alias_cap * sizeof(moq_index_entry_t);
    /* The per-stream receive buffers that follow (ns_recv and the *_req_recv
     * groups) are sized as `count * ns_sub_recv_cap` byte products WITHOUT an
     * explicit division-overflow check, unlike the struct-array segments above.
     * That is safe by construction: every entry count is capped at <= 0xFFFF and
     * ns_sub_recv_cap is <= 4096 (both bounded above), so each product is
     * <= ~256 MiB and cannot overflow size_t on any target. The monotonic-order
     * checks in the cross-check block below also catch any additive wrap. */
    size_t off_ns_recv = off_idx_sub_alias + idx_sub_alias_bytes;
    size_t ns_recv_bytes = ns_sub_cap * ns_sub_recv_cap;
    /* Per-subscription request-stream receive buffers (stream-correlated
     * profiles parse request/response control messages off the request bidi
     * here). Same per-stream cap as namespace-sub buffers. */
    size_t sub_req_recv_cap = ns_sub_recv_cap;
    size_t off_sub_req_recv = off_ns_recv + ns_recv_bytes;
    size_t sub_req_recv_bytes = sub_cap * sub_req_recv_cap;
    /* Per-fetch request-bidi RESPONSE receive buffers (FETCH_OK/REQUEST_ERROR
     * on stream-correlated profiles). Same per-stream cap as the sub buffers;
     * separate from the FETCH data-uni path. */
    size_t fetch_req_recv_cap = ns_sub_recv_cap;
    size_t off_fetch_req_recv = off_sub_req_recv + sub_req_recv_bytes;
    size_t fetch_req_recv_bytes = fetch_cap * fetch_req_recv_cap;
    /* Per-announcement request-bidi receive buffers (stream-correlated profiles
     * parse the PUBLISH_NAMESPACE response/REQUEST_UPDATE off the announce bidi
     * here). Same per-stream cap as the sub buffers. */
    size_t ann_req_recv_cap = ns_sub_recv_cap;
    size_t off_ann_req_recv = off_fetch_req_recv + fetch_req_recv_bytes;
    size_t ann_req_recv_bytes = ann_cap * ann_req_recv_cap;
    /* Per-track-status request-bidi receive buffers (stream-correlated profiles
     * buffer the TRACK_STATUS_OK / REQUEST_ERROR response off the request bidi
     * here). Same per-stream cap as the sub buffers. */
    size_t ts_req_recv_cap = ns_sub_recv_cap;
    size_t off_ts_req_recv = off_ann_req_recv + ann_req_recv_bytes;
    size_t ts_req_recv_bytes = ts_cap * ts_req_recv_cap;
    /* SUBSCRIBE_TRACKS pool (draft-18 only): entry array + per-entry request-bidi
     * receive buffers (REQUEST_OK / REQUEST_ERROR / PUBLISH_BLOCKED stream). */
    size_t off_track_sub = ALIGN_UP(off_ts_req_recv + ts_req_recv_bytes,
                                    track_sub_align);
    size_t track_sub_bytes = track_sub_cap * sizeof(moq_track_sub_entry_t);
    size_t track_sub_recv_cap = ns_sub_recv_cap;
    size_t off_track_sub_recv = off_track_sub + track_sub_bytes;
    size_t track_sub_recv_bytes = track_sub_cap * track_sub_recv_cap;
    /* Per-publish request-bidi receive buffers (stream-correlated profiles buffer
     * the PUBLISH bidi's response/lifecycle messages here). Same per-stream cap
     * as the sub buffers. */
    size_t pub_req_recv_cap = ns_sub_recv_cap;
    size_t off_pub_req_recv = off_track_sub_recv + track_sub_recv_bytes;
    size_t pub_req_recv_bytes = pub_cap * pub_req_recv_cap;
    size_t off_scratch = ALIGN_UP(off_pub_req_recv + pub_req_recv_bytes,
                                    _Alignof(max_align_t));
    /* Event borrow arena: same budget as the transient scratch. */
    size_t off_evscratch = ALIGN_UP(off_scratch + scratch_cap,
                                    _Alignof(max_align_t));
    size_t prof_align  = profile->state_align > 0 ? profile->state_align : 1;
    size_t off_profile = ALIGN_UP(off_evscratch + scratch_cap, prof_align);
    size_t total       = off_profile + profile->state_size;

#undef ALIGN_UP

    /* Layout-discipline gate: reject any capacity or offset that would overflow
     * or break monotonicity BEFORE allocating. Struct-array segments verify the
     * `count * elem_size` multiplication (bytes/elem == cap); every segment
     * verifies its offset does not run backwards (an additive wrap); the total is
     * capped at 64 MiB. RULE: any NEW arena segment added above MUST also be
     * added here (its order check, plus a mult check if it is a struct array) --
     * otherwise the layout discipline is incomplete and a bad cap could silently
     * under-size a region. */
    if (act_bytes / sizeof(moq_action_t) != action_cap ||
        evt_bytes / sizeof(moq_event_t)  != event_cap  ||
        sub_bytes / sizeof(moq_sub_entry_t) != sub_cap ||
        ann_bytes / sizeof(moq_ann_entry_t) != ann_cap ||
        fetch_bytes / sizeof(moq_fetch_entry_t) != fetch_cap ||
        pub_bytes / sizeof(moq_pub_entry_t) != pub_cap ||
        ts_bytes  / sizeof(moq_ts_entry_t)  != ts_cap  ||
        sg_bytes  / sizeof(moq_sg_entry_t)  != sg_cap  ||
        rx_bytes  / sizeof(moq_rx_stream_t) != rx_cap  ||
        rx_fin_bytes / sizeof(uint64_t)    != rx_fin_cap ||
        unsub_tomb_bytes / sizeof(uint64_t) != unsub_tomb_cap ||
        fetch_cancel_tomb_bytes / sizeof(uint64_t) != fetch_cancel_tomb_cap ||
        drain_ref_bytes / sizeof(uint64_t) != drain_ref_cap ||
        off_fetch_cancel_tomb < off_unsub_tomb ||
        off_drain_ref < off_fetch_cancel_tomb ||
        ns_sub_bytes / sizeof(moq_ns_sub_entry_t) != ns_sub_cap ||
        track_sub_bytes / sizeof(moq_track_sub_entry_t) != track_sub_cap ||
        idx_reqref_bytes / sizeof(moq_index_entry_t) != idx_reqref_cap ||
        idx_sub_alias_bytes / sizeof(moq_index_entry_t) != idx_sub_alias_cap ||
        off_events < off_actions || off_send < off_events ||
        off_recv < off_send || off_subs < off_recv + recv_cap ||
        off_ann < off_subs || off_fetch < off_ann || off_pub < off_fetch ||
        off_ts < off_pub ||
        off_sg < off_pub ||
        off_rx < off_sg ||
        off_rx_fin < off_rx || off_ns_sub < off_idx_rx ||
        off_idx_reqref < off_idx_ns ||
        off_idx_sub_alias < off_idx_reqref ||
        off_ns_recv < off_idx_sub_alias ||
        off_sub_req_recv < off_ns_recv ||
        off_fetch_req_recv < off_sub_req_recv ||
        off_ann_req_recv < off_fetch_req_recv ||
        off_ts_req_recv < off_ann_req_recv ||
        off_track_sub < off_ts_req_recv ||
        off_track_sub_recv < off_track_sub ||
        off_pub_req_recv < off_track_sub_recv ||
        off_scratch < off_pub_req_recv ||
        off_evscratch < off_scratch ||
        off_profile < off_evscratch ||
        total < off_profile ||
        total > (size_t)64 * 1024 * 1024)
        return MOQ_ERR_INVAL;

    uint8_t *mem = (uint8_t *)cfg->alloc->alloc(total, cfg->alloc->ctx);
    if (!mem) return MOQ_ERR_NOMEM;

    memset(mem, 0, total);
    moq_session_t *s = (moq_session_t *)mem;
    s->alloc = *cfg->alloc;
    s->alloc_size = total;
    s->perspective = cfg->perspective;
    s->state = MOQ_SESS_IDLE;
    s->last_now_us = now_us;
    s->borrow_epoch = 1;

    s->actions    = (moq_action_t *)(mem + off_actions);
    s->action_cap = action_cap;
    s->events     = (moq_event_t *)(mem + off_events);
    s->event_cap  = event_cap;
    s->send_buf   = mem + off_send;
    s->send_cap   = send_cap;
    s->recv_buf   = mem + off_recv;
    s->recv_cap   = recv_cap;
    s->subs       = (moq_sub_entry_t *)(mem + off_subs);
    s->sub_cap    = sub_cap;
    s->announcements = (moq_ann_entry_t *)(mem + off_ann);
    s->ann_cap    = ann_cap;
    s->fetches    = (moq_fetch_entry_t *)(mem + off_fetch);
    s->fetch_cap  = fetch_cap;
    s->publishes  = (moq_pub_entry_t *)(mem + off_pub);
    s->pub_cap    = pub_cap;
    s->track_statuses = (moq_ts_entry_t *)(mem + off_ts);
    s->ts_cap     = ts_cap;
    s->subgroups  = (moq_sg_entry_t *)(mem + off_sg);
    s->sg_cap     = sg_cap;
    s->next_stream_ref = 1;
    s->rx_streams      = (moq_rx_stream_t *)(mem + off_rx);
    s->rx_cap          = rx_cap;
    s->rx_finished     = (uint64_t *)(mem + off_rx_fin);
    s->rx_fin_cap      = rx_fin_cap;
    s->max_obj_payload = max_obj_payload;
    s->max_recv_buf = max_recv_buf;
    /* Reordering buffer for data that arrives before its SUBSCRIBE_OK: a
     * small fixed slot ring (lazily allocated on first use) whose held bytes
     * share the receive-buffer budget. */
    s->staged_cap        = MOQ_STAGED_DG_SLOTS;
    s->staged_bytes_cap  = max_recv_buf;
    s->unsub_tombstones   = (uint64_t *)(mem + off_unsub_tomb);
    s->unsub_tomb_cap     = unsub_tomb_cap;
    s->fetch_cancel_tombs    = (uint64_t *)(mem + off_fetch_cancel_tomb);
    s->fetch_cancel_tomb_cap = fetch_cancel_tomb_cap;
    s->drain_refs         = (uint64_t *)(mem + off_drain_ref);
    s->drain_ref_reasons  = (uint8_t *)(mem + off_drain_reason);
    s->drain_ref_cap      = drain_ref_cap;
    s->idx_req_by_rid     = (moq_index_entry_t *)(mem + off_idx_sub);
    s->idx_req_mask       = idx_sub_cap - 1;
    s->idx_rx_by_ref      = (moq_index_entry_t *)(mem + off_idx_rx);
    s->idx_rx_mask        = idx_rx_cap - 1;
    for (size_t i = 0; i < idx_sub_cap; i++)
        s->idx_req_by_rid[i].slot = -1;
    for (size_t i = 0; i < idx_rx_cap; i++)
        s->idx_rx_by_ref[i].slot = -1;
    s->ns_subs        = (moq_ns_sub_entry_t *)(mem + off_ns_sub);
    s->ns_sub_cap     = ns_sub_cap;
    s->idx_ns_by_ref  = (moq_index_entry_t *)(mem + off_idx_ns);
    s->idx_ns_mask    = idx_ns_cap - 1;
    for (size_t i = 0; i < idx_ns_cap; i++)
        s->idx_ns_by_ref[i].slot = -1;
    s->idx_req_by_streamref     = (moq_index_entry_t *)(mem + off_idx_reqref);
    s->idx_req_streamref_mask   = idx_reqref_cap - 1;
    for (size_t i = 0; i < idx_reqref_cap; i++)
        s->idx_req_by_streamref[i].slot = -1;
    s->idx_sub_by_alias   = (moq_index_entry_t *)(mem + off_idx_sub_alias);
    s->idx_sub_alias_mask = idx_sub_alias_cap - 1;
    for (size_t i = 0; i < idx_sub_alias_cap; i++)
        s->idx_sub_by_alias[i].slot = -1;
    for (size_t i = 0; i < ns_sub_cap; i++) {
        s->ns_subs[i].recv_buf = mem + off_ns_recv + i * ns_sub_recv_cap;
        s->ns_subs[i].recv_cap = ns_sub_recv_cap;
    }
    for (size_t i = 0; i < sub_cap; i++) {
        s->subs[i].req_recv_buf = mem + off_sub_req_recv + i * sub_req_recv_cap;
        s->subs[i].req_recv_cap = sub_req_recv_cap;
    }
    for (size_t i = 0; i < fetch_cap; i++) {
        s->fetches[i].req_recv_buf =
            mem + off_fetch_req_recv + i * fetch_req_recv_cap;
        s->fetches[i].req_recv_cap = fetch_req_recv_cap;
    }
    for (size_t i = 0; i < ann_cap; i++) {
        s->announcements[i].req_recv_buf =
            mem + off_ann_req_recv + i * ann_req_recv_cap;
        s->announcements[i].req_recv_cap = ann_req_recv_cap;
    }
    for (size_t i = 0; i < ts_cap; i++) {
        s->track_statuses[i].req_recv_buf =
            mem + off_ts_req_recv + i * ts_req_recv_cap;
        s->track_statuses[i].req_recv_cap = ts_req_recv_cap;
    }
    s->track_subs     = (moq_track_sub_entry_t *)(mem + off_track_sub);
    s->track_sub_cap  = track_sub_cap;
    for (size_t i = 0; i < track_sub_cap; i++) {
        s->track_subs[i].req_recv_buf =
            mem + off_track_sub_recv + i * track_sub_recv_cap;
        s->track_subs[i].req_recv_cap = track_sub_recv_cap;
    }
    for (size_t i = 0; i < pub_cap; i++) {
        s->publishes[i].req_recv_buf =
            mem + off_pub_req_recv + i * pub_req_recv_cap;
        s->publishes[i].req_recv_cap = pub_req_recv_cap;
    }
    s->output_scratch     = mem + off_scratch;
    s->output_scratch_cap = scratch_cap;
    s->event_scratch      = mem + off_evscratch;
    s->event_scratch_cap  = scratch_cap;
    s->profile            = profile;
    s->profile_state      = mem + off_profile;
    profile->init_in_place(s->profile_state, cfg);

    if (cfg->struct_size >= offsetof(moq_session_cfg_t, streaming_objects) +
        sizeof(cfg->streaming_objects))
        s->streaming_objects = cfg->streaming_objects;

    s->goaway_deadline_us = UINT64_MAX;
    s->subgroup_deadline_us = UINT64_MAX;
    s->subgroup_retry_deadline_us = UINT64_MAX;
    s->idle_deadline_us = UINT64_MAX;
    if (cfg->struct_size >= offsetof(moq_session_cfg_t, goaway_timeout_us) +
        sizeof(cfg->goaway_timeout_us))
        s->goaway_timeout_us = cfg->goaway_timeout_us;
    if (cfg->struct_size >= offsetof(moq_session_cfg_t, idle_timeout_us) +
        sizeof(cfg->idle_timeout_us))
        s->idle_timeout_us = cfg->idle_timeout_us;
    /* Arm the idle deadline from creation time so a session that is created
     * and then left silent (e.g. a server session whose peer never sends the
     * setup) still advertises a finite deadline and idle-closes on tick. A
     * disabled timeout (0) leaves idle_deadline_us at UINT64_MAX. */
    if (s->idle_timeout_us > 0)
        s->idle_deadline_us = deadline_add(now_us, s->idle_timeout_us);

    {
        uintptr_t addr = (uintptr_t)(void *)s;
        uint64_t h = (uint64_t)addr;
        h ^= h >> 16;
        h ^= h >> 32;
        h *= 0x9E3779B97F4A7C15ULL;
        h ^= now_us;
        h ^= (uint64_t)cfg->perspective << 8;
        h ^= h >> 33;
        s->session_tag = (uint16_t)((h & 0x7FFF) | 1);
    }

    s->send_auth_token_cache_size = send_auth_token_cache_size;
    s->auth_token_cache_size = auth_token_cache_size;
    {
        size_t cache_max = (size_t)auth_token_cache_size;
        size_t cache_entries;
        if (cache_max > 0) {
            cache_entries = cache_max / 16;
            if (cache_entries > 4096) cache_entries = 4096;
            if (cache_entries == 0) cache_entries = 1;
        } else {
            cache_entries = 1;
        }
        moq_result_t crc = moq_token_cache_init(&s->peer_token_cache,
            &s->alloc, cache_max, cache_entries);
        if (crc < 0) {
            /* The profile was already initialized in place (above), so its
             * destroy hook must run before the arena is freed -- otherwise a
             * profile that acquires resources in init_in_place would leak on
             * this late create-failure path (the normal teardown in
             * moq_session_destroy calls it, but that never runs here). */
            profile->destroy(s->profile_state);
            s->alloc.free(s, total, s->alloc.ctx);
            return crc;
        }
    }

    *out = s;
    return MOQ_OK;
}

void moq_session_destroy(moq_session_t *s)
{
    if (!s) return;
    if (s->profile && s->profile_state)
        s->profile->destroy(s->profile_state);
    decref_queued_data_payloads(s);
    decref_queued_event_payloads(s);
    free_rx_stream_bufs(s);
    free_staged_datagrams(s);
    moq_token_cache_free(&s->peer_token_cache);
    for (size_t i = 0; i < s->sub_cap; i++) {
        if (s->subs[i].track_id_buf)
            s->alloc.free(s->subs[i].track_id_buf, s->subs[i].track_id_len,
                          s->alloc.ctx);
    }
    for (size_t i = 0; i < s->ann_cap; i++) {
        if (s->announcements[i].ns_id_buf)
            s->alloc.free(s->announcements[i].ns_id_buf,
                          s->announcements[i].ns_id_len,
                          s->alloc.ctx);
    }
    /* A buffered or released-but-unaccepted Joining FETCH (§10.12.2) owns deep-copied
     * auth-token values until the fetch entry is freed; release them on teardown. */
    for (size_t i = 0; i < s->fetch_cap; i++) {
        if (s->fetches[i].join_token_count > 0)
            process_auth_tokens_free_staging(s, s->fetches[i].join_tokens,
                s->fetches[i].join_token_staged, s->fetches[i].join_token_count);
    }
    ns_sub_destroy_all(s);
    track_sub_destroy_all(s);
    moq_alloc_t alloc = s->alloc;
    alloc.free(s, s->alloc_size, alloc.ctx);
}

moq_session_state_t moq_session_state(const moq_session_t *s)
{
    return s ? s->state : MOQ_SESS_CLOSED;
}

moq_perspective_t moq_session_perspective(const moq_session_t *s)
{
    return s ? s->perspective : (moq_perspective_t)0;
}

moq_result_t moq_session_start(moq_session_t *s, uint64_t now_us)
{
    if (!s) return MOQ_ERR_INVAL;
    moq_input_t input;
    memset(&input, 0, sizeof(input));
    input.kind = MOQ_INPUT_START;
    input.now_us = now_us;
    return session_step(s, &input);
}

moq_result_t moq_session_on_control_bytes(moq_session_t *s,
                                           const uint8_t *buf,
                                           size_t len,
                                           uint64_t now_us)
{
    if (!s) return MOQ_ERR_INVAL;
    moq_input_t input;
    memset(&input, 0, sizeof(input));
    input.kind = MOQ_INPUT_CONTROL_BYTES;
    input.now_us = now_us;
    input.u.control_bytes.buf = buf;
    input.u.control_bytes.len = len;
    return session_step(s, &input);
}

moq_result_t moq_session_process_pending(moq_session_t *s, uint64_t now_us)
{
    return moq_session_on_control_bytes(s, NULL, 0, now_us);
}

/* Draft-neutral inbound-GOAWAY handling shared by all profiles: copy the
 * (already-decoded) New Session URI, enter DRAINING, arm the drain deadline, and
 * surface MOQ_EVENT_GOAWAY. The profile owns the wire decode and the pre-decode
 * active/duplicate checks; this owns the state transition and the event. A
 * server receiving a non-empty URI is a protocol violation (§10.4). */
moq_result_t session_core_on_goaway(moq_session_t *s,
                                    const uint8_t *uri, size_t uri_len)
{
    if (s->perspective == MOQ_PERSPECTIVE_SERVER && uri_len > 0)
        return close_with_error(s, 0x3, "GOAWAY with URI to server");

    if (event_queue_full(s))
        return MOQ_ERR_WOULD_BLOCK;

    uint8_t *uri_copy = NULL;
    if (uri_len > 0) {
        uri_copy = event_scratch_copy(s, uri, uri_len);
        if (!uri_copy) {
            if (s->event_scratch_len == 0)
                return close_with_error(s, 0x1,
                    "event scratch permanently too small for GOAWAY URI");
            return MOQ_ERR_WOULD_BLOCK;
        }
    }

    s->goaway_received = true;
    if (s->state == MOQ_SESS_ESTABLISHED)
        s->state = MOQ_SESS_DRAINING;
    if (s->goaway_timeout_us > 0 && s->goaway_deadline_us == UINT64_MAX)
        s->goaway_deadline_us = deadline_add(s->last_now_us, s->goaway_timeout_us);

    moq_event_t e;
    memset(&e, 0, sizeof(e));
    e.kind = MOQ_EVENT_GOAWAY;
    e.detail_size = (uint32_t)sizeof(moq_goaway_event_t);
    e.borrow_epoch = s->borrow_epoch;
    if (uri_copy) {
        e.u.goaway.new_session_uri.data = uri_copy;
        e.u.goaway.new_session_uri.len = uri_len;
    }
    return push_event(s, &e);
}

moq_result_t session_core_emit_request_redirect(
    moq_session_t *s, moq_request_family_t family, uint64_t handle_opaque,
    const moq_decoded_redirect_t *rd, uint64_t error_code,
    bool can_retry, uint64_t retry_after_ms,
    const uint8_t *reason, size_t reason_len)
{
    if (event_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;

    size_t scratch_saved = s->event_scratch_len;
    moq_bytes_t ev_uri = {0}, ev_name = {0}, ev_reason = {0};
    moq_namespace_t ev_ns = {0};

    if (rd->connect_uri_len > 0) {
        ev_uri.data = event_scratch_copy(s, rd->connect_uri, rd->connect_uri_len);
        ev_uri.len = rd->connect_uri_len;
        if (!ev_uri.data) goto nomem;
    }
    if (rd->track_namespace.count > 0) {
        moq_bytes_t *parts = (moq_bytes_t *)event_scratch_alloc_aligned(
            s, rd->track_namespace.count * sizeof(moq_bytes_t),
            _Alignof(moq_bytes_t));
        if (!parts) goto nomem;
        for (size_t i = 0; i < rd->track_namespace.count; i++) {
            size_t plen = rd->track_namespace.parts[i].len;
            uint8_t *pd = plen ? event_scratch_copy(s,
                rd->track_namespace.parts[i].data, plen) : NULL;
            if (plen && !pd) goto nomem;
            parts[i].data = pd;
            parts[i].len = plen;
        }
        ev_ns.parts = parts;
        ev_ns.count = rd->track_namespace.count;
    }
    if (rd->track_name_len > 0) {
        ev_name.data = event_scratch_copy(s, rd->track_name, rd->track_name_len);
        ev_name.len = rd->track_name_len;
        if (!ev_name.data) goto nomem;
    }
    if (reason_len > 0) {
        ev_reason.data = event_scratch_copy(s, reason, reason_len);
        ev_reason.len = reason_len;
        if (!ev_reason.data) goto nomem;
    }

    moq_event_t e;
    memset(&e, 0, sizeof(e));
    e.kind = MOQ_EVENT_REQUEST_REDIRECT;
    e.detail_size = (uint32_t)sizeof(moq_request_redirect_event_t);
    e.borrow_epoch = s->borrow_epoch;
    e.u.request_redirect.family = family;
    e.u.request_redirect.handle.raw = handle_opaque;
    e.u.request_redirect.error_code = (moq_request_error_t)error_code;
    e.u.request_redirect.can_retry = can_retry;
    e.u.request_redirect.retry_after_ms = retry_after_ms;
    e.u.request_redirect.connect_uri = ev_uri;
    e.u.request_redirect.track_namespace = ev_ns;
    e.u.request_redirect.track_name = ev_name;
    e.u.request_redirect.reason = ev_reason;

    moq_result_t rc = push_event(s, &e);
    if (rc < 0) {
        s->event_scratch_len = scratch_saved;
        return rc;
    }
    return MOQ_OK;

nomem:
    s->event_scratch_len = scratch_saved;
    if (scratch_saved == 0)
        return close_with_error(s, 0x1,
            "event scratch permanently too small for REDIRECT");
    return MOQ_ERR_BUFFER;
}

moq_result_t moq_session_goaway(moq_session_t *s,
                                 const uint8_t *uri,
                                 size_t uri_len,
                                 uint64_t now_us)
{
    if (!s) return MOQ_ERR_INVAL;
    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_WRONG_STATE;
    if (s->goaway_sent) return MOQ_ERR_WRONG_STATE;
    if (s->perspective == MOQ_PERSPECTIVE_CLIENT && uri_len > 0)
        return MOQ_ERR_INVAL;

    uint8_t buf[8224];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    moq_goaway_encode_args_t ga = { .uri = uri, .uri_len = uri_len };
    moq_result_t rc = s->profile->encode_goaway(s, &w, &ga);
    if (rc < 0) return rc;

    rc = queue_send_control(s, buf, moq_buf_writer_offset(&w));
    if (rc < 0) return rc;

    s->goaway_sent = true;
    if (s->state == MOQ_SESS_ESTABLISHED)
        s->state = MOQ_SESS_DRAINING;
    if (s->goaway_timeout_us > 0 && s->goaway_deadline_us == UINT64_MAX)
        s->goaway_deadline_us = deadline_add(now_us, s->goaway_timeout_us);
    return MOQ_OK;
}

moq_result_t moq_session_tick(moq_session_t *s, uint64_t now_us)
{
    if (!s) return MOQ_ERR_INVAL;
    moq_input_t input;
    memset(&input, 0, sizeof(input));
    input.kind = MOQ_INPUT_TICK;
    input.now_us = now_us;
    return session_step(s, &input);
}

bool moq_session_borrow_valid(const moq_session_t *s, uint64_t borrow_epoch)
{
    if (!s) return false;
    return s->borrow_epoch == borrow_epoch;
}

moq_result_t moq_session_on_datagram(moq_session_t *s,
                                      const uint8_t *data, size_t len,
                                      uint64_t now_us)
{
    if (!s) return MOQ_ERR_INVAL;
    if (len > 0 && !data) return MOQ_ERR_INVAL;
    moq_input_t input;
    memset(&input, 0, sizeof(input));
    input.kind = MOQ_INPUT_DATAGRAM;
    input.now_us = now_us;
    input.u.datagram.buf = data;
    input.u.datagram.len = len;
    return session_step(s, &input);
}

moq_result_t moq_session_on_transport_close(moq_session_t *s,
                                              uint64_t code,
                                              uint64_t now_us)
{
    if (!s) return MOQ_ERR_INVAL;
    if (s->state == MOQ_SESS_CLOSED) return MOQ_OK;

    session_begin_advance(s, now_us);

    s->state = MOQ_SESS_CLOSED;
    s->goaway_deadline_us = UINT64_MAX;
    s->subgroup_deadline_us = UINT64_MAX;
    s->subgroup_retry_deadline_us = UINT64_MAX;
    s->idle_deadline_us = UINT64_MAX;
    decref_queued_data_payloads(s);
    decref_queued_event_payloads(s);
    free_rx_stream_bufs(s);
    s->action_head = s->action_tail = 0;
    s->send_len = 0;
    s->event_head = s->event_tail = 0;

    for (size_t i = 0; i < s->sg_cap; i++)
        if (s->subgroups[i].state != MOQ_SG_FREE)
            sg_free_entry(i, s->subgroups);

    moq_event_t e;
    memset(&e, 0, sizeof(e));
    e.kind = MOQ_EVENT_SESSION_CLOSED;
    e.detail_size = (uint32_t)sizeof(moq_session_closed_event_t);
    e.borrow_epoch = s->borrow_epoch;
    e.u.closed.code = code;
    push_event(s, &e);
    return MOQ_OK;
}

moq_result_t moq_session_on_data_bytes(moq_session_t *s,
                                       moq_stream_ref_t stream_ref,
                                       const uint8_t *buf,
                                       size_t len,
                                       bool fin,
                                       uint64_t now_us)
{
    if (!s) return MOQ_ERR_INVAL;
    if (len > 0 && !buf) return MOQ_ERR_INVAL;
    session_begin_advance(s, now_us);
    if (session_idle_expired(s))
        return close_with_error(s, MOQ_CLOSE_IDLE_TIMEOUT, "idle timeout");
    session_refresh_idle(s, now_us);
    return handle_data_bytes(s, stream_ref, buf, len, fin);
}

moq_result_t moq_session_on_data_rcbuf(moq_session_t *s,
                                       moq_stream_ref_t stream_ref,
                                       moq_rcbuf_t *data,
                                       bool fin,
                                       uint64_t now_us)
{
    if (!s) return MOQ_ERR_INVAL;
    session_begin_advance(s, now_us);
    if (session_idle_expired(s))
        return close_with_error(s, MOQ_CLOSE_IDLE_TIMEOUT, "idle timeout");
    session_refresh_idle(s, now_us);
    return handle_data_bytes_rcbuf(s, stream_ref, data, fin);
}

moq_result_t moq_session_on_data_reset(moq_session_t *s,
                                        moq_stream_ref_t stream_ref,
                                        uint64_t error_code,
                                        uint64_t now_us)
{
    if (!s) return MOQ_ERR_INVAL;
    (void)error_code;
    session_begin_advance(s, now_us);
    if (session_idle_expired(s))
        return close_with_error(s, MOQ_CLOSE_IDLE_TIMEOUT, "idle timeout");
    session_refresh_idle(s, now_us);
    return handle_data_reset(s, stream_ref);
}

moq_result_t moq_session_on_bidi_stream_bytes(moq_session_t *s,
                                               moq_stream_ref_t stream_ref,
                                               const uint8_t *buf,
                                               size_t len,
                                               bool fin,
                                               uint64_t now_us)
{
    if (!s) return MOQ_ERR_INVAL;
    if (len > 0 && !buf) return MOQ_ERR_INVAL;
    session_begin_advance(s, now_us);
    if (session_idle_expired(s))
        return close_with_error(s, MOQ_CLOSE_IDLE_TIMEOUT, "idle timeout");
    session_refresh_idle(s, now_us);
    return handle_bidi_stream_bytes(s, stream_ref, buf, len, fin);
}

moq_result_t moq_session_on_bidi_stream_reset(moq_session_t *s,
                                               moq_stream_ref_t stream_ref,
                                               uint64_t error_code,
                                               uint64_t now_us)
{
    if (!s) return MOQ_ERR_INVAL;
    (void)error_code;
    session_begin_advance(s, now_us);
    if (session_idle_expired(s))
        return close_with_error(s, MOQ_CLOSE_IDLE_TIMEOUT, "idle timeout");
    session_refresh_idle(s, now_us);
    return handle_bidi_stream_reset(s, stream_ref);
}

moq_result_t moq_session_on_bidi_stream_stop(moq_session_t *s,
                                              moq_stream_ref_t stream_ref,
                                              uint64_t error_code,
                                              uint64_t now_us)
{
    if (!s) return MOQ_ERR_INVAL;
    (void)error_code;
    session_begin_advance(s, now_us);
    if (session_idle_expired(s))
        return close_with_error(s, MOQ_CLOSE_IDLE_TIMEOUT, "idle timeout");
    session_refresh_idle(s, now_us);
    return handle_bidi_stream_stop(s, stream_ref);
}

bool moq_session_has_transport_stream(const moq_session_t *s, moq_stream_ref_t ref)
{
    if (!s) return false;
    if (moq_index_find(s->idx_rx_by_ref, s->idx_rx_mask, ref._v) >= 0)
        return true;
    if (moq_index_find(s->idx_ns_by_ref, s->idx_ns_mask, ref._v) >= 0)
        return true;
    return false;
}

/* -- Poll + observation -------------------------------------------- */

moq_result_t moq_session_poll_actions_ex(moq_session_t *s,
                                          void *out, size_t cap,
                                          size_t element_size,
                                          size_t *out_count)
{
    if (out_count) *out_count = 0;
    if (!s || !out || !out_count) return MOQ_ERR_INVAL;
    if (cap == 0) return MOQ_OK;

    const size_t prefix = offsetof(moq_action_t, u);
    if (element_size < prefix) return MOQ_ERR_INVAL;

    const size_t lib_size = sizeof(moq_action_t);
    const size_t copy = lib_size < element_size ? lib_size : element_size;
    const size_t send_data_needed = prefix + sizeof(moq_send_data_action_t);

    size_t n = 0;
    uint8_t *dst = (uint8_t *)out;

    while (n < cap && s->action_head < s->action_tail) {
        const moq_action_t *head = &s->actions[s->action_head % s->action_cap];

        if (head->kind == MOQ_ACTION_SEND_DATA &&
            element_size < send_data_needed)
            break;

        memset(dst, 0, element_size);
        memcpy(dst, head, copy);
        uint64_t epoch = s->borrow_epoch;
        memcpy(dst + offsetof(moq_action_t, borrow_epoch), &epoch, sizeof(epoch));
        s->action_head++;
        dst += element_size;
        n++;
    }

    if (n == 0 && s->action_head < s->action_tail)
        return MOQ_ERR_ABI_MISMATCH;

    *out_count = n;
    return MOQ_OK;
}

static size_t event_min_poll_size(uint32_t kind)
{
    const size_t prefix = offsetof(moq_event_t, u);
    switch (kind) {
    case MOQ_EVENT_OBJECT_RECEIVED:
        return prefix + sizeof(moq_object_received_event_t);
    case MOQ_EVENT_FETCH_OBJECT:
        return prefix + sizeof(moq_fetch_object_event_t);
    case MOQ_EVENT_OBJECT_CHUNK:
        return prefix + sizeof(moq_object_chunk_event_t);
    default:
        return prefix;
    }
}

moq_result_t moq_session_poll_events_ex(moq_session_t *s,
                                         void *out, size_t cap,
                                         size_t element_size,
                                         size_t *out_count)
{
    if (out_count) *out_count = 0;
    if (!s || !out || !out_count) return MOQ_ERR_INVAL;
    if (cap == 0) return MOQ_OK;

    const size_t prefix = offsetof(moq_event_t, u);
    if (element_size < prefix) return MOQ_ERR_INVAL;

    const size_t lib_size = sizeof(moq_event_t);
    const size_t copy = lib_size < element_size ? lib_size : element_size;

    size_t n = 0;
    uint8_t *dst = (uint8_t *)out;

    while (n < cap && s->event_head < s->event_tail) {
        const moq_event_t *head = &s->events[s->event_head % s->event_cap];

        if (element_size < event_min_poll_size(head->kind))
            break;

        /* Release session budget when owned refs transfer to caller. */
        if (head->kind == MOQ_EVENT_OBJECT_RECEIVED) {
            if (head->u.object_received.payload) {
                size_t plen = moq_rcbuf_len(head->u.object_received.payload);
                if (plen <= s->recv_payload_bytes)
                    s->recv_payload_bytes -= plen;
                else
                    s->recv_payload_bytes = 0;
            }
            if (head->u.object_received.properties) {
                size_t elen = moq_rcbuf_len(head->u.object_received.properties);
                if (elen <= s->recv_payload_bytes)
                    s->recv_payload_bytes -= elen;
                else
                    s->recv_payload_bytes = 0;
            }
        } else if (head->kind == MOQ_EVENT_FETCH_OBJECT) {
            if (head->u.fetch_object.payload) {
                size_t plen = moq_rcbuf_len(head->u.fetch_object.payload);
                if (plen <= s->recv_payload_bytes)
                    s->recv_payload_bytes -= plen;
                else
                    s->recv_payload_bytes = 0;
            }
            if (head->u.fetch_object.properties) {
                size_t elen = moq_rcbuf_len(head->u.fetch_object.properties);
                if (elen <= s->recv_payload_bytes)
                    s->recv_payload_bytes -= elen;
                else
                    s->recv_payload_bytes = 0;
            }
        } else if (head->kind == MOQ_EVENT_OBJECT_CHUNK) {
            if (head->u.object_chunk.properties) {
                size_t elen = moq_rcbuf_len(head->u.object_chunk.properties);
                if (elen <= s->recv_payload_bytes)
                    s->recv_payload_bytes -= elen;
                else
                    s->recv_payload_bytes = 0;
            }
        }

        memset(dst, 0, element_size);
        memcpy(dst, head, copy);
        uint64_t epoch = s->borrow_epoch;
        memcpy(dst + offsetof(moq_event_t, borrow_epoch), &epoch, sizeof(epoch));
        s->event_head++;
        dst += element_size;
        n++;
    }

    if (n == 0 && s->event_head < s->event_tail)
        return MOQ_ERR_ABI_MISMATCH;

    /* Draining events freed event-queue capacity: retry delivering any data
     * held for a now-established alias. Without this a tiny event queue could
     * strand held data, since replay otherwise only runs on SUBSCRIBE_OK or
     * fresh network input. Called unconditionally (not gated on staged
     * datagrams): a deferred subgroup/uni stream can be stranded with
     * staged_count == 0. Newly delivered objects surface on the next poll. */
    if (n > 0)
        session_replay_staged(s);

    /* Same freed-capacity rule for deferred early-arrival request bidis
     * (§3.3, buffered before establishment): their establishment-time refeed
     * can WOULD_BLOCK on a full event queue (e.g. SETUP_COMPLETE in a
     * max_events=1 queue), and there is no bridge retry for those bytes (they
     * were accepted) nor any guaranteed further peer activity on the stream.
     * Newly surfaced request events appear on the next poll. A hard dispatch
     * error closes the session inside the refeed; its CLOSED event/action
     * surface through the normal queues. */
    if (n > 0 && s->request_refeed_pending && session_is_active(s))
        (void)request_streams_refeed_deferred(s);

    *out_count = n;
    return MOQ_OK;
}

/* -- Request capacity ---------------------------------------------- */

uint64_t moq_session_peer_request_capacity(const moq_session_t *s)
{
    if (!s) return 0;
    return s->profile->peer_request_capacity(s);
}

uint64_t moq_session_local_request_capacity(const moq_session_t *s)
{
    if (!s) return 0;
    return s->profile->local_request_capacity(s);
}

moq_result_t moq_session_grant_request_capacity(moq_session_t *s,
                                                  uint64_t new_capacity,
                                                  uint64_t now_us)
{
    if (!s) return MOQ_ERR_INVAL;
    return s->profile->grant_capacity(s, new_capacity, now_us);
}

/* D16 compatibility aliases — call through semantic API. */

uint64_t moq_session_peer_max_request_id(const moq_session_t *s)
{
    return moq_session_peer_request_capacity(s);
}

uint64_t moq_session_local_max_request_id(const moq_session_t *s)
{
    return moq_session_local_request_capacity(s);
}

moq_result_t moq_session_update_max_request_id(moq_session_t *s,
                                                uint64_t max_request_id,
                                                uint64_t now_us)
{
    return moq_session_grant_request_capacity(s, max_request_id, now_us);
}

uint64_t moq_session_peer_auth_token_cache_size(const moq_session_t *s)
{
    if (!s) return 0;
    return s->peer_setup.has_max_auth_token_cache_size ?
        s->peer_setup.max_auth_token_cache_size : 0;
}

/* Earliest subgroup deadline to *report* to a timer driver.
 *
 * Expired subgroups (deadline <= now) need a delivery-timeout reset. When that
 * reset has already been deferred by a full action queue, a retry backoff is
 * armed (subgroup_retry_deadline_us) and we report it for the expired work so
 * drivers back off instead of spinning a zero-length wait. Subgroups whose
 * deadline is still in the future are always reported on their own schedule --
 * the backoff for expired work must never mask a nearer un-expired deadline.
 * If no subgroup is currently expired, any armed retry field is stale (the
 * blocked subgroup was reset/freed elsewhere) and is ignored. */
static uint64_t reported_subgroup_deadline(const moq_session_t *s)
{
    uint64_t earliest_future = UINT64_MAX;
    uint64_t earliest_expired = UINT64_MAX;
    for (size_t i = 0; i < s->sg_cap; i++) {
        const moq_sg_entry_t *sg = &s->subgroups[i];
        if (sg->state == MOQ_SG_FREE) continue;
        if (sg->delivery_deadline_us == UINT64_MAX) continue;
        if (sg->delivery_deadline_us <= s->last_now_us) {
            if (sg->delivery_deadline_us < earliest_expired)
                earliest_expired = sg->delivery_deadline_us;
        } else if (sg->delivery_deadline_us < earliest_future) {
            earliest_future = sg->delivery_deadline_us;
        }
    }

    uint64_t d = earliest_future;
    if (earliest_expired != UINT64_MAX) {
        /* Expired reset work pending: defer to the retry backoff if one is
         * armed, otherwise report the expired deadline so the driver ticks
         * promptly to enqueue the reset. */
        uint64_t due = (s->subgroup_retry_deadline_us != UINT64_MAX)
                           ? s->subgroup_retry_deadline_us
                           : earliest_expired;
        if (due < d) d = due;
    }
    return d;
}

uint64_t moq_session_next_deadline_us(const moq_session_t *s)
{
    if (!s) return UINT64_MAX;
    uint64_t d = s->goaway_deadline_us;
    uint64_t sg = reported_subgroup_deadline(s);
    if (sg < d) d = sg;
    if (s->idle_deadline_us < d) d = s->idle_deadline_us;
    return d;
}
