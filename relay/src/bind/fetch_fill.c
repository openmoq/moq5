#include "bind_internal.h"
#include "fetch_fill_internal.h"
#include "fetch_internal.h"
#include "fetch_joining_internal.h"
#include "metadata_internal.h"
#include "request_answers_internal.h"
#include "subscription_update_internal.h"

void bind_fill_request(moqr_bind_t *b, b_conn_t *cn,
                       const moq_fill_request_event_t *fr,
                       uint64_t now_us);

void bind_fill_terminal(moqr_bind_t *b, b_conn_t *cn, moq_fill_t fill);

uint64_t bind_wait_deadline(const moqr_bind_t *b, uint64_t processed_at_us,
                            bool has_fill_timeout, uint64_t fill_timeout_ms);


/* -- fill fetch streams --------------------------------------------------------- *
 * A subscription that asked for a fill is owed a fill fetch stream: the
 * FETCH response for the fill range, opened by the publisher. The relay
 * answers it from the same retrieval cursor that serves a FETCH over that
 * range, written through the session's fill family. The cursor serves cached
 * Objects and retrieves missing runs upstream, within the fill's deadline:
 * its processing time plus the FILL_TIMEOUT it carried (capped at the relay's
 * maximum) or, absent one, the relay's own wait. Past the deadline each run
 * is written as End of Timed-Out Range at its last Location and the fill
 * continues; a timeout of zero therefore serves what is held and marks the
 * rest at once. The stream ends normally after the range end. Descending
 * fills visit one group window at a time, newest first, without reversing
 * Object order inside a group.
 *
 * The same holds when the log of the shard serving the fill holds nothing of
 * the range when the request arrives: the Largest Object the range is bounded
 * by can be known (an upstream's or another shard's acceptance carries it)
 * before any Object of the range is retained here. The stream is opened with
 * its header at once and the cursor retrieves that missing range too. If no
 * source is available, it retries until the deadline; unserved windows are
 * then marked Timed-Out, followed by FIN.
 * Ending it without a marker would claim the range holds no Objects
 * (Section 3.2.2 of draft-ietf-moq-transport-22), and resetting it would
 * give up before the wait Section 9.20.5 allows.
 *
 * A multi-shard runtime may install a fill source (moqr_bind_set_fill_source)
 * for the part of a range this shard's log cannot hold because another shard
 * owns the track and this shard's feed of it started later. That part is
 * read through the source with the same item contract and the same deadline
 * handling, and whatever follows it is served from the log as above.
 *
 * An upstream failure leaves the Object state unknown and resets the fill
 * with UNKNOWN_OBJECT_STATUS; local admission or retrieval failures use
 * INTERNAL_ERROR. Neither failure touches the subscription. */

/* Both standalone FETCH and subscription fills use the same retrieval
 * outcomes: an upstream failure leaves Object status unknown; local failures
 * are this relay's own. */
moqr_reset_cause_t
bind_retrieve_error_cause(const moqr_retrieve_item_t *it)
{
    switch (it->error_kind) {
    case MOQR_RETRIEVE_RESPONSE:
    case MOQR_RETRIEVE_RESET:
    case MOQR_RETRIEVE_SOURCE_LOST:
    case MOQR_RETRIEVE_MALFORMED:
        return MOQR_RESET_UNKNOWN_OBJECT_STATUS;
    default:
        return MOQR_RESET_INTERNAL_ERROR;
    }
}


uint64_t
bind_retrieve_reset_code(const moqr_bind_t *b, const b_conn_t *cn,
                         moqr_reset_cause_t cause)
{
    uint64_t code = BIND_RESET_INTERNAL;
    (void)moqr_reset_encode(moqr_bind_conn_version(b, cn->session), cause, &code);
    return code;
}


static uint64_t
bind_fill_fail_code(const moqr_bind_t *b, const b_conn_t *cn)
{
    return bind_retrieve_reset_code(b, cn, MOQR_RESET_INTERNAL_ERROR);
}


/* Fail a fill on the wire. The reset is an obligation: a WOULD_BLOCK holds it
 * in the refusal table (B_ABORT_FILL; the poll that delivered the fill
 * request guaranteed a free entry) and the refusal pump retries it until it
 * commits or the child is gone. */
static void
bind_fill_fail(moqr_bind_t *b, b_conn_t *cn, moq_fill_t fill, uint64_t now_us)
{
    b_refusal_t want = {
        .used = true,
        .kind = B_ABORT_FILL,
        .handle = fill._opaque,
        .error_code = bind_fill_fail_code(b, cn),
    };
    moq_result_t rc = bind_refusal_submit(cn, &want, now_us);
    if (rc == MOQ_OK) {
        FWD_DBG(b, refusals_sent);
        return;
    }
    if (rc != MOQ_ERR_WOULD_BLOCK) {
        if (!fwd_result_is_gone(rc)) {
            b->session_errors++;
        }
        return;
    }
    FWD_DBG(b, refusals_blocked);
    bind_terminal_hold(b, cn, &want);
}


/* Release a tracked fill: retire its cursor and free the slot. */
void
bind_fill_release(moqr_bind_t *b, b_fetch_t *bf)
{
    moqr_bind_retrieve_close(b, &bf->retrieval);
    if (moqr_fetch_is_valid(bf->cfetch)) {
        (void)moqr_core_fetch_close(b->core, bf->cfetch);
    }
    if (bf->fill_src_open) {
        b->fill_src.close(b->fill_src.ctx, bf->fill_src_handle);
    }
    if (bf->sa_remote) {
        b->sa_src.close(b->sa_src.ctx, bf->sa_handle);
    }
    memset(bf, 0, sizeof(*bf));
}


/* The instant a fill (or a FETCH) stops waiting for upstream: its processing
 * time plus the wait the request selects (a carried FILL_TIMEOUT shortened to
 * the relay's maximum, else the relay's default), saturating. */
uint64_t
bind_wait_deadline(const moqr_bind_t *b, uint64_t processed_at_us,
                   bool has_fill_timeout, uint64_t fill_timeout_ms)
{
    uint64_t wait_us = b->fill_wait_default_us;
    if (has_fill_timeout) {
        wait_us = fill_timeout_ms > b->fill_wait_max_us / 1000u
                      ? b->fill_wait_max_us
                      : fill_timeout_ms * 1000u;
    }
    return processed_at_us > UINT64_MAX - wait_us ? UINT64_MAX
                                                  : processed_at_us + wait_us;
}


static uint64_t
bind_fill_deadline(const moqr_bind_t *b, const moq_fill_request_event_t *fr)
{
    return bind_wait_deadline(b, fr->processed_at_us, fr->has_fill_timeout,
                              fr->fill_timeout_ms);
}


/* Claim the external source or initialize the shared local retrieval cursor.
 * NOT_HELD is only the external source's transient open refusal. An empty
 * local cache is handled by the shared cursor, not a cache-only timeout. */
typedef enum bind_fill_open {
    BIND_FILL_OPENED,
    BIND_FILL_NOT_HELD,
    BIND_FILL_FAILED,
} bind_fill_open_t;


static bind_fill_open_t
bind_fill_cursor_open(moqr_bind_t *b, b_conn_t *cn, b_fetch_t *bf,
                      uint64_t now_us)
{
    moqr_track_t track = MOQR_TRACK_INVALID;
    uint64_t track_gen = 0;
    if (moqr_core_sub_track(b->core, bf->fill_rsub, &track, &track_gen) !=
            MOQR_OK ||
        !moqr_track_eq(track, bf->fill_track) || track_gen != bf->fill_track_gen) {
        return BIND_FILL_FAILED;
    }
    if (b->has_fill_src && !bf->fill_src_used) {
        /* The source claims the part of the range this shard's log cannot
         * hold, if any; the log serves whatever follows it. */
        moqr_bind_fill_query_t q;
        memset(&q, 0, sizeof(q));
        q.struct_size = (uint32_t)sizeof(q);
        q.track = track;
        q.track_gen = track_gen;
        q.start_group = bf->fill_start_group;
        q.start_object = bf->fill_start_object;
        q.end_group = bf->fill_end_group;
        q.end_object = bf->fill_end_object;
        /* Descending fills visit one group at a time. A source's historical
         * prefix must precede the local suffix WITHIN that group. */
        q.group_order = MOQR_GROUP_ORDER_ASCENDING;
        q.subscriber_priority = bf->fill_priority;
        q.deadline_us = bf->fill_deadline_us;
        moqr_bind_fill_claim_t claim;
        memset(&claim, 0, sizeof(claim));
        moqr_result_t sr = b->fill_src.open(b->fill_src.ctx, &q, &claim);
        if (sr == MOQR_OK) {
            moqr_retrieve_location_t end = retrieve_loc(claim.end_group,
                                                         claim.end_object);
            if (retrieve_loc_cmp(end, retrieve_loc(q.start_group, q.start_object)) < 0 ||
                retrieve_loc_cmp(end, retrieve_loc(q.end_group, q.end_object)) > 0) {
                b->fill_src.close(b->fill_src.ctx, claim.handle);
                return BIND_FILL_FAILED;
            }
            bf->fill_src_open = true;
            bf->fill_src_handle = claim.handle;
            bf->fill_src_end_group = claim.end_group;
            bf->fill_src_end_object = claim.end_object;
            return BIND_FILL_OPENED;
        }
        if (sr == MOQR_ERR_WOULD_BLOCK) {
            return BIND_FILL_NOT_HELD;
        }
        if (sr != MOQR_DONE) return BIND_FILL_FAILED;
        bf->fill_src_used = true;   /* not the source's: the log's alone */
    }
    moqr_retrieve_request_t req;
    memset(&req, 0, sizeof(req));
    req.binding = cn->binding;
    req.parent = bf->fill_rsub;
    req.track = bf->fill_track;
    req.track_gen = bf->fill_track_gen;
    req.source = conn_cookie(b, cn);
    req.demand = bf->fill_rsub._opaque;
    req.child = bf->sfill._opaque;
    req.start = retrieve_loc(bf->fill_start_group, bf->fill_start_object);
    req.end = retrieve_loc(bf->fill_end_group, bf->fill_end_object);
    req.group_order = MOQR_GROUP_ORDER_ASCENDING;
    req.subscriber_priority = bf->fill_priority;
    req.deadline_us = bf->fill_deadline_us;
    (void)now_us;
    return moqr_bind_retrieve_init(b, &bf->retrieval, &req) == MOQR_OK
               ? BIND_FILL_OPENED : BIND_FILL_FAILED;
}


/* Whether the fill has its shared local cursor or a fill-source claim. */
static bool
bind_fill_has_reader(const b_fetch_t *bf)
{
    return bf->fill_src_open || bf->retrieval.live;
}


static moqr_result_t
bind_fill_peek(moqr_bind_t *b, b_fetch_t *bf, uint64_t now_us,
               moqr_fetch_item_t *it)
{
    if (!bf->fill_src_open) return MOQR_ERR_WRONG_STATE;
    memset(it, 0, sizeof(*it));
    return b->fill_src.peek(b->fill_src.ctx, bf->fill_src_handle, now_us, it);
}


static moqr_result_t
bind_fill_commit(moqr_bind_t *b, b_fetch_t *bf)
{
    if (bf->fill_src_open) {
        return b->fill_src.commit(b->fill_src.ctx, bf->fill_src_handle);
    }
    return moqr_bind_retrieve_commit(b, &bf->retrieval);
}


/* The fill source has served its claimed part. The claim ends; a range that
 * continues past it is the log's from the Location after the claimed end.
 * Returns true when the whole current window is done. */
static bool
bind_fill_source_done(moqr_bind_t *b, b_fetch_t *bf)
{
    b->fill_src.close(b->fill_src.ctx, bf->fill_src_handle);
    bf->fill_src_open = false;
    bf->fill_src_used = true;
    bf->response_done = false;
    uint64_t eg = bf->fill_src_end_group;
    uint64_t eo = bf->fill_src_end_object;
    if (eg > bf->fill_end_group ||
        (eg == bf->fill_end_group &&
         (eo >= bf->fill_end_object || eo == UINT64_MAX))) {
        return true;
    }
    if (eo == UINT64_MAX) {
        bf->fill_start_group = eg + 1u;
        bf->fill_start_object = 0;
    } else {
        bf->fill_start_group = eg;
        bf->fill_start_object = eo + 1u;
    }
    bf->response_next = retrieve_loc(bf->fill_start_group, bf->fill_start_object);
    return false;
}


/* The current group window is covered. Descending orders groups only: each
 * reader and source claim within one window still runs in Object order. */
static void
bind_fill_window_done(moqr_bind_t *b, b_fetch_t *bf)
{
    moqr_bind_retrieve_close(b, &bf->retrieval);
    moqr_retrieve_location_t next;
    moqr_retrieve_location_t end = retrieve_loc(bf->fill_end_group,
                                                 bf->fill_end_object);
    if (moqr_retrieve_after(bf->response_start, bf->response_end, bf->response_order, end, &next)) {
        moqr_retrieve_location_t window = moqr_retrieve_window_end(
            bf->response_end, bf->response_order, next.group);
        bf->fill_start_group = next.group;
        bf->fill_start_object = next.object;
        bf->fill_end_group = window.group;
        bf->fill_end_object = window.object;
        bf->fill_src_used = false;
        bf->response_next = next;
        bf->response_done = false;
    } else {
        bf->response_ne_pending = false;
        bf->state = BF_NEEDS_FIN;
    }
}


/* Unmarked runs are non-existent by default. Retain their end only when a
 * subsequent marked run needs to distinguish the two, as for standalone
 * FETCH. A following Object or final FIN needs no explicit marker. */
static moq_result_t
bind_fill_write_range(b_conn_t *cn, b_fetch_t *bf,
                      moq_fetch_range_kind_t kind,
                      moqr_retrieve_location_t end, uint64_t now_us)
{
    if (kind == MOQ_FETCH_RANGE_NON_EXISTENT) return MOQ_OK;
    if (bf->response_ne_pending) {
        moq_result_t rc = moq_session_write_fill_range(cn->session, bf->sfill,
            MOQ_FETCH_RANGE_NON_EXISTENT, bf->response_ne_end.group,
            bf->response_ne_end.object, now_us);
        if (rc != MOQ_OK) return rc;
        bf->response_ne_pending = false;
    }
    return moq_session_write_fill_range(cn->session, bf->sfill, kind,
                                         end.group, end.object, now_us);
}


void
bind_fill_request(moqr_bind_t *b, b_conn_t *cn,
                  const moq_fill_request_event_t *fr, uint64_t now_us)
{
    moqr_sub_t rsub = MOQR_SUB_INVALID;
    bool tracked = false;
    /* The fill's parent: a SUBSCRIBE's subscription, or a publication the
     * relay opened for track discovery. */
    uint64_t parent = fr->sub._opaque != 0 ? fr->sub._opaque : fr->pub._opaque;
    for (uint32_t i = 0; parent != 0 && i < b->n_dsubs; i++) {
        if (cn->dsubs[i].used && cn->dsubs[i].sub_raw == parent) {
            rsub = cn->dsubs[i].rsub;
            tracked = true;
            break;
        }
    }
    int fi = -1;
    for (uint32_t i = 0; tracked && i < b->n_fetches; i++) {
        if (!cn->fetches[i].used) {
            fi = (int)i;
            break;
        }
    }
    if (fi < 0) {
        bind_fill_fail(b, cn, fr->fill, now_us);
        return;
    }
    /* The fill range is inclusive and already bounded by Largest Object. */
    b_fetch_t *bf = &cn->fetches[fi];
    memset(bf, 0, sizeof(*bf));
    bf->fill = true;
    bf->state = BF_FILL_NEEDS_OPEN;
    bf->sfill = fr->fill;
    bf->cfetch = MOQR_FETCH_INVALID;
    bf->fill_deadline_us = bind_fill_deadline(b, fr);
    bf->fill_rsub = rsub;
    if (moqr_core_sub_track(b->core, rsub, &bf->fill_track,
                             &bf->fill_track_gen) != MOQR_OK) {
        memset(bf, 0, sizeof(*bf));
        bind_fill_fail(b, cn, fr->fill, now_us);
        return;
    }
    bf->fill_start_group = fr->start_group;
    bf->fill_start_object = fr->start_object;
    bf->fill_end_group = fr->end_group;
    bf->fill_end_object = fr->end_object;
    bf->fill_priority = fr->subscriber_priority;
    bf->response_start = retrieve_loc(fr->start_group, fr->start_object);
    bf->response_end = retrieve_loc(fr->end_group, fr->end_object);
    bf->response_order = (moqr_group_order_t)fr->group_order;
    bf->response_next = moqr_retrieve_first(bf->response_start, bf->response_end, bf->response_order);
    moqr_retrieve_location_t window = moqr_retrieve_window_end(
        bf->response_end, bf->response_order, bf->response_next.group);
    bf->fill_start_group = bf->response_next.group;
    bf->fill_start_object = bf->response_next.object;
    bf->fill_end_group = window.group;
    bf->fill_end_object = window.object;
    if (bind_fill_cursor_open(b, cn, bf, now_us) == BIND_FILL_FAILED) {
        memset(bf, 0, sizeof(*bf));
        bind_fill_fail(b, cn, fr->fill, now_us);
        return;
    }
    bind_fetch_mark_used(cn, bf);
}


/* The child ended on the session side (its FIN or RESET committed, the peer
 * stopped it, or its subscription went away): nothing more is owed on it. */
void
bind_fill_terminal(moqr_bind_t *b, b_conn_t *cn, moq_fill_t fill)
{
    for (uint32_t i = 0; i < b->n_fetches; i++) {
        b_fetch_t *bf = &cn->fetches[i];
        if (bf->used && bf->fill && bf->sfill._opaque == fill._opaque) {
            bind_fill_release(b, bf);
        }
    }
    for (uint32_t i = 0; i < b->n_refusals; i++) {
        b_refusal_t *r = &cn->refusals[i];
        if (r->used && r->kind == B_ABORT_FILL && r->handle == fill._opaque) {
            memset(r, 0, sizeof(*r));
            FWD_DBG(b, refusals_dropped);
        }
    }
}


/* The same writer consumes the local shared cursor and the built-in shard's
 * hidden typed adjunct. External public-only sources retain their documented
 * cache-item interpretation below. ERROR is always a child RESET, never END. */
static bool
bind_fill_retrieve_step(moqr_bind_t *b, b_conn_t *cn, b_fetch_t *bf,
                        uint64_t now_us)
{
    moqr_retrieve_item_t it;
    moqr_result_t prc = bf->fill_src_open
        ? b->retrieve_source_peek(b->fill_src.ctx, bf->fill_src_handle, now_us, &it)
        : moqr_bind_retrieve_peek(b, &bf->retrieval, now_us, &it);
    if (prc == MOQR_ERR_WOULD_BLOCK) return false;
    if (prc != MOQR_OK) {
        bf->state = BF_FILL_NEEDS_ABORT;
        return true;
    }
    if (it.kind == MOQR_RETRIEVE_ERROR) {
        bf->fill_abort_code = bind_retrieve_reset_code(b, cn,
            bind_retrieve_error_cause(&it));
        bf->fill_abort_code_set = true;
        bf->state = BF_FILL_NEEDS_ABORT;
        return true;
    }
    if (it.kind == MOQR_RETRIEVE_END) {
        moqr_retrieve_location_t end = bf->fill_src_open
            ? retrieve_loc(bf->fill_src_end_group, bf->fill_src_end_object)
            : retrieve_loc(bf->fill_end_group, bf->fill_end_object);
        if (!bf->response_done || retrieve_loc_cmp(it.end, end) != 0) {
            bf->state = BF_FILL_NEEDS_ABORT;
            return true;
        }
        if (bf->fill_src_open && !bind_fill_source_done(b, bf)) return true;
        bind_fill_window_done(b, bf);
        return true;
    }
    moqr_retrieve_location_t start = retrieve_loc(bf->fill_start_group,
                                                   bf->fill_start_object);
    moqr_retrieve_location_t end = bf->fill_src_open
        ? retrieve_loc(bf->fill_src_end_group, bf->fill_src_end_object)
        : retrieve_loc(bf->fill_end_group, bf->fill_end_object);
    if (bf->response_done || retrieve_loc_cmp(it.start, bf->response_next) != 0 ||
        retrieve_loc_cmp(it.start, start) != 0 ||
        retrieve_loc_cmp(it.end, start) < 0 || retrieve_loc_cmp(it.end, end) > 0) {
        bf->state = BF_FILL_NEEDS_ABORT;
        return true;
    }
    moq_result_t rc;
    if (it.kind == MOQR_RETRIEVE_GAP) {
        rc = bind_fill_write_range(cn, bf, it.gap_kind, it.end, now_us);
    } else if (it.kind == MOQR_RETRIEVE_OBJECT) {
        if (retrieve_loc_cmp(it.end, start) != 0 ||
            it.object.group_id != it.end.group ||
            it.object.object_id != it.end.object) {
            bf->state = BF_FILL_NEEDS_ABORT;
            return true;
        }
        moq_fetch_object_cfg_t oc;
        moq_fetch_object_cfg_init(&oc);
        oc.group_id = it.object.group_id;
        oc.subgroup_id = it.object.subgroup_id;
        oc.object_id = it.object.object_id;
        oc.publisher_priority = it.object.publisher_priority;
        oc.datagram = moq_session_supports_fetch_datagram(cn->session)
                          ? it.object.datagram_pref : false;
        oc.payload = (moq_rcbuf_t *)(void *)it.object.payload;
        moq_rcbuf_t *props_own = NULL;
        rc = bind_props_out(b, cn, it.object.properties, it.object.properties_codec,
                            &oc.properties, &props_own);
        if (rc == MOQ_OK)
            rc = moq_session_write_fill_object(cn->session, bf->sfill, &oc, now_us);
        if (props_own != NULL) moq_rcbuf_decref(props_own);
    } else {
        bf->state = BF_FILL_NEEDS_ABORT;
        return true;
    }
    if (rc == MOQ_ERR_WOULD_BLOCK) return false;
    if (rc != MOQ_OK) {
        bf->state = BF_FILL_NEEDS_ABORT;
        return true;
    }
    if (it.kind == MOQR_RETRIEVE_OBJECT) {
        bf->response_ne_pending = false;
        b->deliveries_written++;
    } else if (it.gap_kind == MOQ_FETCH_RANGE_NON_EXISTENT) {
        bf->response_ne_pending = true;
        bf->response_ne_end = it.end;
    }
    if (bind_fill_commit(b, bf) != MOQR_OK) {
        bf->state = BF_FILL_NEEDS_ABORT;
        return true;
    }
    moqr_retrieve_location_t next;
    if (moqr_retrieve_after(start, end, MOQR_GROUP_ORDER_ASCENDING, it.end, &next)) {
        bf->response_next = next;
        bf->fill_start_group = next.group;
        bf->fill_start_object = next.object;
    } else {
        bf->response_done = true;
    }
    return true;
}


/* Drive one fill as far as the session allows; false means HOLD (retry next
 * pump, no state advanced). A fill never terminates the connection: anything
 * that goes wrong mid-stream is answered on its own stream with a RESET. */
bool
bind_fill_step(moqr_bind_t *b, b_conn_t *cn, b_fetch_t *bf, uint64_t now_us)
{
    moq_result_t rc;
    switch (bf->state) {
    case BF_FILL_NEEDS_OPEN:
        rc = moq_session_open_fill(cn->session, bf->sfill, now_us);
        if (rc == MOQ_ERR_WOULD_BLOCK) {
            return false;
        }
        if (rc != MOQ_OK) {
            if (!fwd_result_is_gone(rc)) {
                b->session_errors++;
            }
            bind_fill_release(b, bf);
            return true;
        }
        bf->state = BF_STREAMING;
        return true;
    case BF_STREAMING: {
        bind_fill_open_t opened = bind_fill_has_reader(bf) ? BIND_FILL_OPENED
            : bind_fill_cursor_open(b, cn, bf, now_us);
        if (opened == BIND_FILL_FAILED) {
            bf->state = BF_FILL_NEEDS_ABORT;
            return true;
        }
        if (opened == BIND_FILL_NOT_HELD) {
            /* The source cannot claim this window yet. Wait only until
             * the original deadline, then classify this window and move
             * to the next group in the fill's delivery order. */
            if (now_us < bf->fill_deadline_us) {
                return false;
            }
            rc = bind_fill_write_range(cn, bf, MOQ_FETCH_RANGE_TIMED_OUT,
                retrieve_loc(bf->fill_end_group, bf->fill_end_object), now_us);
            if (rc == MOQ_ERR_WOULD_BLOCK) {
                return false;
            }
            if (rc == MOQ_OK) bind_fill_window_done(b, bf);
            else bf->state = BF_FILL_NEEDS_ABORT;
            return true;
        }
        if (!bf->fill_src_open || b->retrieve_source_peek != NULL) {
            return bind_fill_retrieve_step(b, cn, bf, now_us);
        }
        moqr_fetch_item_t it;
        moqr_result_t prc = bind_fill_peek(b, bf, now_us, &it);
        if (prc == MOQR_ERR_WOULD_BLOCK) {
            /* pin budget momentarily full, or the source's part not here
             * yet */
            return false;
        }
        if (prc != MOQR_OK) {
            bf->state = BF_FILL_NEEDS_ABORT;   /* range lost mid-fill */
            return true;
        }
        if (it.kind == MOQR_FETCH_ITEM_END) {
            if (bf->fill_src_open && !bind_fill_source_done(b, bf)) {
                return true;   /* the rest of the range is the log's */
            }
            bind_fill_window_done(b, bf);
            return true;
        }
        if (it.kind == MOQR_FETCH_ITEM_GAP ||
            it.kind == MOQR_FETCH_ITEM_PENDING) {
            /* Locations the log does not hold, or an Object still arriving.
             * Upstream may still deliver them, so the cursor waits (the
             * re-peek recomputes the run as objects arrive, and serves the
             * Object once it is whole) until the fill's deadline; then the
             * run is one End of Timed-Out Range at its last Location. */
            if (now_us < bf->fill_deadline_us) {
                return false;
            }
            rc = bind_fill_write_range(cn, bf, MOQ_FETCH_RANGE_TIMED_OUT,
                retrieve_loc(it.rec.group_id, it.rec.object_id), now_us);
            if (rc == MOQ_ERR_WOULD_BLOCK) {
                return false;
            }
            if (rc != MOQ_OK) {
                bf->state = BF_FILL_NEEDS_ABORT;
                return true;
            }
            if (bind_fill_commit(b, bf) != MOQR_OK) bf->state = BF_FILL_NEEDS_ABORT;
            return true;
        }
        if (it.kind == MOQR_FETCH_ITEM_UNKNOWN ||
            it.kind == MOQR_FETCH_ITEM_MARKER) {
            /* Locations the relay has no evidence about (a run, or the
             * evicted prefix: through the last object of the group before
             * the first retained one) and no source to wait on: marked at
             * once, Unknown while the fill's wait budget lasts and Timed-Out
             * once it is spent -- at once for a FILL_TIMEOUT of 0. The fill
             * profile's Location is a vi64, so UINT64_MAX is a group's last
             * object id. */
            uint64_t mg = it.rec.group_id, mo = it.rec.object_id;
            if (it.kind == MOQR_FETCH_ITEM_MARKER) {
                if (it.marker_group == 0) {
                    if (bind_fill_commit(b, bf) != MOQR_OK) {
                        bf->state = BF_FILL_NEEDS_ABORT;
                    }
                    return true;
                }
                mg = it.marker_group - 1u;
                mo = UINT64_MAX;
            }
            rc = bind_fill_write_range(cn, bf,
                bind_fill_unserved_kind(now_us >= bf->fill_deadline_us),
                retrieve_loc(mg, mo), now_us);
            if (rc == MOQ_ERR_WOULD_BLOCK) {
                return false;
            }
            if (rc != MOQ_OK) {
                bf->state = BF_FILL_NEEDS_ABORT;
                return true;
            }
            if (bind_fill_commit(b, bf) != MOQR_OK) bf->state = BF_FILL_NEEDS_ABORT;
            return true;
        }
        moq_fetch_object_cfg_t oc;
        moq_fetch_object_cfg_init(&oc);
        oc.group_id = it.rec.group_id;
        oc.subgroup_id = it.rec.subgroup_id;
        oc.object_id = it.rec.object_id;
        oc.publisher_priority = it.rec.publisher_priority;
        oc.datagram = moq_session_supports_fetch_datagram(cn->session)
                          ? it.rec.datagram_pref
                          : false;
        oc.payload = (moq_rcbuf_t *)(void *)it.rec.payload;
        moq_rcbuf_t *props_own = NULL;
        rc = bind_props_out(b, cn, it.rec.properties, it.rec.properties_codec,
                            &oc.properties, &props_own);
        if (rc == MOQ_OK)
            rc = moq_session_write_fill_object(cn->session, bf->sfill, &oc, now_us);
        if (props_own != NULL) moq_rcbuf_decref(props_own);
        if (rc == MOQ_ERR_WOULD_BLOCK) {
            return false;   /* HOLD: not committed; the re-peek re-emits it */
        }
        if (rc != MOQ_OK) {
            bf->state = BF_FILL_NEEDS_ABORT;
            return true;
        }
        bf->response_ne_pending = false;
        if (bind_fill_commit(b, bf) != MOQR_OK) bf->state = BF_FILL_NEEDS_ABORT;
        b->deliveries_written++;
        return true;
    }
    case BF_NEEDS_FIN:
        rc = moq_session_end_fill(cn->session, bf->sfill, now_us);
        if (rc == MOQ_ERR_WOULD_BLOCK) {
            return false;
        }
        if (rc != MOQ_OK && !fwd_result_is_gone(rc)) {
            b->session_errors++;
        }
        bind_fill_release(b, bf);
        return true;
    case BF_FILL_NEEDS_ABORT:
        rc = moq_session_abort_fill(cn->session, bf->sfill,
                                    bf->fill_abort_code_set
                                        ? bf->fill_abort_code
                                        : bind_fill_fail_code(b, cn),
                                    now_us);
        if (rc == MOQ_ERR_WOULD_BLOCK) {
            return false;
        }
        if (rc != MOQ_OK && !fwd_result_is_gone(rc)) {
            b->session_errors++;
        }
        bind_fill_release(b, bf);
        return true;
    default:
        b->session_errors++;   /* a FETCH state on a fill entry: a bug */
        bind_fill_release(b, bf);
        return true;
    }
}
