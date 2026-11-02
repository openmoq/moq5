#include "bind_internal.h"
#include "fetch_internal.h"
#include "authorization_internal.h"
#include "fetch_fill_internal.h"
#include "fetch_joining_internal.h"
#include "metadata_internal.h"
#include "request_answers_internal.h"

/* The only way a fetch slot becomes used: it also keeps fetch_hw above the
 * slot, so the per-pump walks bounded by it still reach it. */
void
bind_fetch_mark_used(b_conn_t *cn, b_fetch_t *bf)
{
    uint32_t slot = (uint32_t)(bf - cn->fetches);
    bf->used = true;
    if (slot >= cn->fetch_hw) {
        cn->fetch_hw = slot + 1u;
    }
}

void bind_standalone_request(moqr_bind_t *b, b_conn_t *cn,
                             const moq_fetch_request_event_t *fq,
                             const moqr_fetch_req_t *freq,
                             const moq_fetch_request_options_t *fo,
                             const moqr_track_t *join_track,
                             uint64_t now_us);

bool bind_standalone_cancelled(moqr_bind_t *b, b_conn_t *cn,
                               moq_fetch_t fetch);

const moq_fetch_request_options_t *bind_fetch_options(const moq_event_t *ev);

void bind_fetch_range(const moq_fetch_request_options_t *fo,
                      moqr_fetch_req_t *freq);


int
retrieve_loc_cmp(moqr_retrieve_location_t a, moqr_retrieve_location_t z)
{
    if (a.group != z.group) return a.group < z.group ? -1 : 1;
    return a.object == z.object ? 0 : (a.object < z.object ? -1 : 1);
}


moqr_retrieve_location_t
retrieve_loc(uint64_t group, uint64_t object)
{
    return (moqr_retrieve_location_t){ .group = group, .object = object };
}


/* Never carry the raw FETCH end into another group. Zero encodes the whole
 * end group, including its UINT64_MAX object. These conversions are lossless. */
moqr_retrieve_location_t
retrieve_inclusive_end(uint64_t group, uint64_t raw_object)
{
    return retrieve_loc(group, raw_object == 0 ? UINT64_MAX : raw_object - 1u);
}


static uint64_t
retrieve_raw_end(moqr_retrieve_location_t end)
{
    return end.object == UINT64_MAX ? 0 : end.object + 1u;
}


static moqr_retrieve_location_t
retrieve_loc_min(moqr_retrieve_location_t a, moqr_retrieve_location_t z)
{
    return retrieve_loc_cmp(a, z) < 0 ? a : z;
}


/* Only called for a Location strictly above the current (nonnegative) next. */
static moqr_retrieve_location_t
retrieve_previous(moqr_retrieve_location_t at)
{
    return at.object == 0 ? retrieve_loc(at.group - 1u, UINT64_MAX)
                          : retrieve_loc(at.group, at.object - 1u);
}


/* A fill's parent subscription is still the live, accepted one for the
 * track generation; a standalone FETCH's track is still that generation. */
static bool
retrieve_parent_current(moqr_bind_t *b, const moqr_retrieve_request_t *r)
{
    if (r->standalone) {
        uint64_t gen = 0;
        return moqr_core_track_current_generation(b->core, r->track, &gen) ==
                   MOQR_OK && gen == r->track_gen;
    }
    moqr_track_t track = MOQR_TRACK_INVALID;
    uint64_t gen = 0;
    moqr_sub_state_t state;
    return moqr_core_sub_state(b->core, r->parent, &state) == MOQR_OK &&
           state == MOQR_SUB_ACTIVE &&
           moqr_core_sub_track(b->core, r->parent, &track, &gen) == MOQR_OK &&
           moqr_track_eq(track, r->track) && gen == r->track_gen;
}


typedef struct b_retrieve_source {
    b_conn_t *conn;
    moqr_retrieve_parent_t parent;
    const b_usub_t *subscription;
} b_retrieve_source_t;


static bool
retrieve_source(moqr_bind_t *b, const moqr_retrieve_request_t *r,
                 bool pinned, uint64_t cookie, moqr_binding_t binding,
                 moqr_retrieve_parent_t parent, b_retrieve_source_t *out)
{
    memset(out, 0, sizeof(*out));
    for (uint32_t i = 0; i < b->max_conns; i++) {
        b_conn_t *cn = &b->conns[i];
        if (!cn->used || cn->closed || cn->session == NULL ||
            (pinned && (conn_cookie(b, cn) != cookie ||
                        !moqr_binding_eq(cn->binding, binding)))) continue;
        for (uint32_t j = 0; j < b->n_usubs; j++) {
            b_usub_t *us = &cn->usubs[j];
            if (us->used && moqr_track_eq(us->track, r->track) &&
                us->track_gen == r->track_gen &&
                (!pinned || (!parent.publish &&
                             us->ssub._opaque == parent.handle.sub._opaque))) {
                out->conn = cn;
                out->parent.handle.sub = us->ssub;
                out->subscription = us;
                return true;
            }
        }
        for (uint32_t j = 0; j < b->n_pubs; j++) {
            const b_pub_t *pub = &cn->pubs[j];
            if (pub->used && moqr_track_eq(pub->track, r->track) &&
                pub->track_gen == r->track_gen &&
                (!pinned || (parent.publish &&
                             pub->pub_raw == parent.handle.pub._opaque))) {
                out->conn = cn;
                out->parent.publish = true;
                out->parent.handle.pub = (moq_publication_t){ ._opaque = pub->pub_raw };
                return true;
            }
        }
    }
    return false;
}


bool
retrieve_fwd_current(moqr_bind_t *b, const b_fwd_t *t)
{
    b_retrieve_source_t source;
    return retrieve_parent_current(b, &t->retrieval) &&
           retrieve_source(b, &t->retrieval, true, t->u_cookie, t->u_binding,
                            t->source_parent, &source);
}


void
retrieve_fwd_expire(b_fwd_t *t, uint64_t now_us)
{
    if (t->consumer == FW_CONSUMER_FILL && t->end == FW_END_NONE &&
        now_us >= t->retrieval.deadline_us) {
        fwd_end_up(t, FW_END_TIMEOUT);
    }
}


/* Clone visible bytes, not a possibly much larger rcbuf root/slice. Charge
 * the exact allocations (including both rcbuf headers) before retaining them.
 * Legacy Joining FETCH keeps its existing event-reference ownership policy. */
static moqr_result_t
retrieve_object_bytes(const moq_rcbuf_t *payload, const moq_rcbuf_t *properties,
                       uint64_t *out)
{
    size_t payload_bytes = 0, property_bytes = 0;
    if ((payload != NULL &&
         moq_rcbuf_allocation_size(moq_rcbuf_len(payload), &payload_bytes) !=
             MOQ_OK) ||
        (properties != NULL &&
         moq_rcbuf_allocation_size(moq_rcbuf_len(properties), &property_bytes) !=
             MOQ_OK)) {
        return MOQR_ERR_CAPACITY;
    }
    const uint64_t bytes = (uint64_t)payload_bytes + (uint64_t)property_bytes;
    if (bytes < (uint64_t)payload_bytes) {
        return MOQR_ERR_CAPACITY;
    }
    *out = bytes;
    return MOQR_OK;
}


moqr_result_t
retrieve_copy_object(moqr_bind_t *b, b_fwd_t *t, b_fwd_item_t *it,
                     const moq_fetch_object_event_t *o)
{
    uint64_t bytes;
    moqr_result_t rc = retrieve_object_bytes(o->payload, o->properties, &bytes);
    if (rc != MOQR_OK) return rc;
    if (!fwd_charge(b, t, bytes)) return MOQR_ERR_CAPACITY;
    it->charged = bytes;
    if ((o->payload != NULL &&
         moq_rcbuf_clone(&b->alloc, o->payload, &it->payload) != MOQ_OK) ||
        (o->properties != NULL &&
         moq_rcbuf_clone(&b->alloc, o->properties, &it->properties) != MOQ_OK)) {
        fwd_item_release(b, t, it);
        return MOQR_ERR_NOMEM;
    }
    return MOQR_OK;
}


static b_fwd_t *
retrieve_collector(moqr_bind_t *b, const moqr_retrieve_cursor_t *c)
{
    if (c->collector_slot == 0 || c->collector_slot > b->n_fwd) return NULL;
    b_fwd_t *t = &b->fwd[c->collector_slot - 1u];
    return t->used && t->consumer == FW_CONSUMER_FILL &&
           t->generation == c->collector_generation ? t : NULL;
}


static void
retrieve_detach_collector(moqr_bind_t *b, moqr_retrieve_cursor_t *c)
{
    b_fwd_t *t = retrieve_collector(b, c);
    if (t != NULL) {
        t->d = FW_D_TERMINAL;
        fwd_end_up(t, FW_END_CANCELLED);
        if (t->u == FW_U_TERMINAL) {
            fwd_release(b, t);
        } else {
            b->budget_pending = true; /* one pump for newly owed cancellation */
        }
    }
    c->collector_slot = 0;
    c->collector_generation = 0;
}


/* No authorization call here: the still-live parent/grant authorizes this
 * child. The request is narrowed to this frozen hole, not a synthetic inbound
 * FETCH and not an unconditional pull of a track's history. */
static moqr_result_t
retrieve_admit(moqr_bind_t *b, moqr_retrieve_cursor_t *c)
{
    b_retrieve_source_t source;
    if (!retrieve_source(b, &c->request, c->source_bound,
                          c->source_cookie, c->source_binding,
                          c->source_parent, &source)) {
        return c->source_bound ? MOQR_ERR_STALE_HANDLE : MOQR_ERR_WOULD_BLOCK;
    }
    moq_bytes_t parts[32], name;
    moqr_ns_t ns;
    if (source.subscription != NULL && source.subscription->has_key) {
        const b_usub_t *us = source.subscription;
        ns = (moqr_ns_t){ .parts = us->parts, .count = us->ns_count };
        name = us->name;
    } else {
        /* An accepted PUBLISH, or an upstream subscription that could not keep
         * its own copy of the key, names the full retained-track identity
         * through the core. Copy that core-owned key now; never keep those
         * borrowed views in a transaction or infer source validity from a
         * live connection alone. */
        moqr_result_t rc = moqr_core_track_key(b->core, c->request.track,
            c->request.track_gen, parts, 32, &ns, &name);
        if (rc != MOQR_OK) return rc;
    }
    size_t key_len = name.len;
    if (ns.count > 32 || key_len > BF_FWD_KEY_MAX ||
        (name.len != 0 && name.data == NULL)) return MOQR_ERR_CAPACITY;
    for (size_t i = 0; i < ns.count; i++) {
        if (ns.parts[i].len == 0 || ns.parts[i].data == NULL ||
            ns.parts[i].len > BF_FWD_KEY_MAX - key_len) return MOQR_ERR_CAPACITY;
        key_len += ns.parts[i].len;
    }
    b_fwd_t *t = NULL;
    for (uint32_t i = 0; i < b->n_fwd; i++) {
        if (!b->fwd[i].used) { t = &b->fwd[i]; break; }
    }
    if (t == NULL || b->fwd_generation == UINT64_MAX) return MOQR_ERR_CAPACITY;
    if (key_len > b->fwd_bytes_cap - b->fwd_bytes_used) return MOQR_ERR_CAPACITY;
    memset(t, 0, sizeof(*t));
    t->used = true;
    t->consumer = FW_CONSUMER_FILL;
    t->generation = ++b->fwd_generation;
#ifdef MOQR_BIND_TESTING
    b->dbg_fwd.live++;
#endif
    if (!fwd_charge(b, t, key_len)) {
        fwd_release(b, t);
        return MOQR_ERR_CAPACITY;
    }
    if (key_len != 0) {
        t->key = b->alloc.alloc(key_len, b->alloc.ctx);
        if (t->key == NULL) {
            fwd_release(b, t);
            return MOQR_ERR_NOMEM;
        }
    }
    t->key_len = key_len;
    t->ns_count = (uint32_t)ns.count;
    size_t off = 0;
    for (uint32_t i = 0; i < t->ns_count; i++) {
        memcpy(t->key + off, ns.parts[i].data, ns.parts[i].len);
        t->parts[i] = (moq_bytes_t){ t->key + off, ns.parts[i].len };
        off += ns.parts[i].len;
    }
    if (name.len != 0) memcpy(t->key + off, name.data, name.len);
    t->name = (moq_bytes_t){ t->key != NULL ? t->key + off : NULL, name.len };
    t->retrieval = c->request;
    t->d_binding = c->request.binding;
    t->d_cookie = c->request.source;
    t->u_cookie = conn_cookie(b, source.conn);
    t->u_binding = source.conn->binding;
    t->source_parent = source.parent;
    t->track = c->request.track;
    t->track_gen = c->request.track_gen;
    t->start_group = c->next.group;
    t->start_object = c->next.object;
    t->end_group = c->span_end.group;
    t->end_object = retrieve_raw_end(c->span_end);
    t->has_prio = true;
    t->prio = c->request.subscriber_priority;
    t->u = FW_U_UNSENT;
    t->d = FW_D_PENDING;
    c->collector_slot = (uint32_t)(t - b->fwd) + 1u;
    c->collector_generation = t->generation;
    c->source_bound = true;
    c->source_cookie = t->u_cookie;
    c->source_binding = t->u_binding;
    c->source_parent = t->source_parent;
    /* Local fill replay runs after bind_fwd_pump. Signal its existing bounded
     * self-wake rather than leave an UNSENT request asleep until its deadline. */
    b->budget_pending = true;
    FWD_DBG(b, admitted);
    return MOQR_OK;
}


static void
retrieve_error(moqr_retrieve_cursor_t *c, moqr_retrieve_error_kind_t kind,
                moqr_result_t result)
{
    memset(&c->item, 0, sizeof(c->item));
    c->item.kind = MOQR_RETRIEVE_ERROR;
    c->item.error_kind = kind;
    c->item.local_result = result;
    c->item.start = c->next;
    c->item.end = c->request.end;
    c->pending = true;
}


static void
retrieve_fwd_error(moqr_retrieve_cursor_t *c, const b_fwd_t *t)
{
    moqr_retrieve_error_kind_t kind = t->failure;
    if (kind == 0) {
        switch (t->end) {
        case FW_END_ERROR: kind = MOQR_RETRIEVE_RESPONSE; break;
        case FW_END_RESET: kind = MOQR_RETRIEVE_RESET; break;
        case FW_END_LOST: kind = MOQR_RETRIEVE_SOURCE_LOST; break;
        case FW_END_MISMATCH: kind = MOQR_RETRIEVE_MALFORMED; break;
        default: kind = MOQR_RETRIEVE_LOCAL; break;
        }
    }
    retrieve_error(c, kind, t->local_result);
    c->item.session_result = t->session_result;
    c->item.error_code = t->end == FW_END_RESET ? t->reset_code : t->err_code;
    c->item.cause = t->reset_cause;
    c->item.data_stream = t->reset_data_stream;
    c->item.stop_sending = t->reset_stop_sending;
    c->item.can_retry = t->err_can_retry;
    c->item.retry_after_ms = t->err_retry_after_ms;
    c->item.reason = (moq_bytes_t){ t->reason, t->reason_len };
}


static void
retrieve_gap(moqr_retrieve_cursor_t *c, moqr_retrieve_location_t end,
              moq_fetch_range_kind_t kind)
{
    memset(&c->item, 0, sizeof(c->item));
    c->item.kind = MOQR_RETRIEVE_GAP;
    c->item.start = c->next;
    c->item.end = end;
    c->item.gap_kind = kind;
    c->pending = true;
}


static void
retrieve_object(moqr_retrieve_cursor_t *c, const moqr_record_view_t *object)
{
    memset(&c->item, 0, sizeof(c->item));
    c->item.kind = MOQR_RETRIEVE_OBJECT;
    c->item.start = c->next;
    c->item.end = retrieve_loc(object->group_id, object->object_id);
    c->item.object = *object;
    c->pending = true;
}


static void
retrieve_cached_release(moqr_bind_t *b, moqr_retrieve_cursor_t *c)
{
    if (c->cached_slot != 0 && c->cached_slot <= b->n_fwd) {
        b_retrieve_hold_t *hold = &b->cached_holds[c->cached_slot - 1u];
        if (hold->generation != 0 && hold->generation == c->cached_generation) {
            moq_rcbuf_decref(hold->payload);
            moq_rcbuf_decref(hold->properties);
            b->fwd_bytes_used -= hold->charged;
            memset(hold, 0, sizeof(*hold));
        }
    }
    c->cached_slot = 0;
    c->cached_generation = 0;
}


static moqr_result_t
retrieve_cached_copy(moqr_bind_t *b, moqr_retrieve_cursor_t *c,
                     moqr_record_view_t *object)
{
    if (c->cached_slot != 0) return MOQR_ERR_WRONG_STATE;
    uint64_t bytes;
    moqr_result_t rc = retrieve_object_bytes(object->payload, object->properties, &bytes);
    if (rc != MOQR_OK) return rc;
    b_retrieve_hold_t *hold = NULL;
    for (uint32_t i = 0; i < b->n_fwd; i++) {
        if (b->cached_holds[i].generation == 0) {
            hold = &b->cached_holds[i];
            break;
        }
    }
    if (hold == NULL || b->cached_generation == UINT64_MAX ||
        !fwd_bytes_charge(b, &hold->charged, bytes)) return MOQR_ERR_CAPACITY;
    hold->generation = ++b->cached_generation;
    c->cached_slot = (uint32_t)(hold - b->cached_holds) + 1u;
    c->cached_generation = hold->generation;
    if ((object->payload != NULL &&
         moq_rcbuf_clone(&b->alloc, object->payload, &hold->payload) != MOQ_OK) ||
        (object->properties != NULL &&
         moq_rcbuf_clone(&b->alloc, object->properties, &hold->properties) != MOQ_OK)) {
        retrieve_cached_release(b, c);
        return MOQR_ERR_NOMEM;
    }
    object->payload = hold->payload;
    object->properties = hold->properties;
    return MOQR_OK;
}


static void
retrieve_cached_object(moqr_bind_t *b, moqr_retrieve_cursor_t *c)
{
    moqr_record_view_t object = c->cache_item.rec;
    if (object.payload != NULL || object.properties != NULL) {
        moqr_result_t rc = retrieve_cached_copy(b, c, &object);
        if (rc != MOQR_OK) {
            retrieve_error(c, MOQR_RETRIEVE_LOCAL, rc);
            return;
        }
    }
    retrieve_object(c, &object);
}


/* The part of the request the cursor's next Location belongs to: the whole
 * range in ascending order, the next Location's group in descending order. */
static moqr_retrieve_location_t
retrieve_window(const moqr_retrieve_cursor_t *c)
{
    return moqr_retrieve_window_end(c->request.end, c->request.group_order,
                                    c->next.group);
}


/* A fill's cache reader is its own core FETCH; a standalone FETCH's is its
 * admission, whose view is only reset (dropping a pin) and never closed. */
static void
retrieve_cache_close(moqr_bind_t *b, moqr_retrieve_cursor_t *c)
{
    if (c->request.standalone) {
        if (moqr_fetch_is_valid(c->cache)) {
            moqr_retrieve_location_t w = retrieve_window(c);
            (void)moqr_core_internal_retrieve_view(b->core, c->request.owner,
                                                   c->next.group, c->next.object,
                                                   w.group, w.object);
        }
    } else if (moqr_fetch_is_valid(c->cache)) {
        (void)moqr_core_fetch_close(b->core, c->cache);
    }
    c->cache = MOQR_FETCH_INVALID;
    c->cache_peeked = false;
    c->cache_absent = false;
    memset(&c->cache_item, 0, sizeof(c->cache_item));
}


static moqr_result_t
retrieve_cache_open(moqr_bind_t *b, moqr_retrieve_cursor_t *c, uint64_t now_us)
{
    if (c->request.standalone) {
        moqr_retrieve_location_t w = retrieve_window(c);
        moqr_result_t vrc = moqr_core_internal_retrieve_view(
            b->core, c->request.owner, c->next.group, c->next.object,
            w.group, w.object);
        if (vrc != MOQR_OK) return vrc;
        c->cache = c->request.owner;
        c->cache_end = w;
        return MOQR_OK;
    }
    moq_bytes_t parts[32], name;
    moqr_ns_t ns;
    moqr_result_t rc = moqr_core_track_key(b->core, c->request.track,
                                           c->request.track_gen, parts, 32,
                                           &ns, &name);
    if (rc != MOQR_OK) return rc;
    moqr_fetch_req_t req;
    moqr_fetch_req_init(&req);
    req.ns = ns;
    req.name = name;
    req.start_group = c->next.group;
    req.start_object = c->next.object;
    req.end_group = c->request.end.group;
    req.end_object = retrieve_raw_end(c->request.end);
    req.group_order = MOQR_GROUP_ORDER_ASCENDING;
    req.subscriber_priority = c->request.subscriber_priority;
    req.cookie = c->request.child;
    req.report_gaps = true;
    req.report_unknown = true;
    req.report_pending = true;
    moqr_fetch_plan_t plan;
    rc = moqr_core_fetch_open(b->core, c->request.binding, &req, now_us,
                              &c->cache, &plan);
    if (rc != MOQR_OK) return rc;
    if (plan.admit != MOQR_FETCH_ACCEPT) {
        c->cache = MOQR_FETCH_INVALID;
        if (plan.error_code != MOQ_REQUEST_ERROR_DOES_NOT_EXIST &&
            plan.error_code != MOQ_REQUEST_ERROR_INVALID_RANGE) {
            return MOQR_ERR_INTERNAL;
        }
        c->cache_absent = true;
        return MOQR_OK;
    }
    c->cache_end = retrieve_inclusive_end(plan.end_group, plan.end_object);
    if (retrieve_loc_cmp(c->cache_end, c->next) < 0 ||
        retrieve_loc_cmp(c->cache_end, c->request.end) > 0) {
        return MOQR_ERR_INTERNAL;
    }
    return MOQR_OK;
}


/* A cache frontier is either a pinned object, a known non-existent prefix,
 * or an exact unresolved run. A peeked core gap stays frozen until consumed;
 * re-peeking it while an origin is collecting would overwrite its commit end. */
typedef enum retrieve_cache_kind {
    RETRIEVE_CACHE_OBJECT, RETRIEVE_CACHE_MISSING, RETRIEVE_CACHE_ABSENT,
    RETRIEVE_CACHE_PENDING,
} retrieve_cache_kind_t;


static moqr_result_t
retrieve_cache_front(moqr_bind_t *b, moqr_retrieve_cursor_t *c, uint64_t now_us,
                      retrieve_cache_kind_t *kind, moqr_retrieve_location_t *end)
{
    if (!moqr_fetch_is_valid(c->cache) && !c->cache_absent) {
        moqr_result_t rc = retrieve_cache_open(b, c, now_us);
        if (rc != MOQR_OK) return rc;
    }
    if (c->cache_absent) {
        *kind = RETRIEVE_CACHE_MISSING;
        *end = retrieve_window(c);
        return MOQR_OK;
    }
    if (!c->cache_peeked) {
        moqr_result_t rc = moqr_core_fetch_peek(b->core, c->cache, now_us,
                                                &c->cache_item);
        if (rc != MOQR_OK) return rc;
        c->cache_peeked = true;
    }
    const moqr_fetch_item_t *it = &c->cache_item;
    *end = retrieve_loc(it->rec.group_id, it->rec.object_id);
    switch (it->kind) {
    case MOQR_FETCH_ITEM_OBJECT:
    case MOQR_FETCH_ITEM_PENDING:
        if (retrieve_loc_cmp(*end, c->next) > 0) {
            *end = retrieve_previous(*end);
            *kind = RETRIEVE_CACHE_ABSENT;
        } else if (it->kind == MOQR_FETCH_ITEM_OBJECT) {
            *kind = RETRIEVE_CACHE_OBJECT;
        } else {
            /* An Object still arriving here: a standalone FETCH waits for
             * it to complete (draft-22 3.2); a fill retrieves it. */
            *kind = c->request.standalone ? RETRIEVE_CACHE_PENDING
                                          : RETRIEVE_CACHE_MISSING;
        }
        break;
    case MOQR_FETCH_ITEM_MARKER:
        if (it->marker_group == 0) return MOQR_ERR_INTERNAL;
        *end = retrieve_loc(it->marker_group - 1u, UINT64_MAX);
        *kind = RETRIEVE_CACHE_MISSING;
        break;
    case MOQR_FETCH_ITEM_GAP:
    case MOQR_FETCH_ITEM_UNKNOWN:
        *kind = RETRIEVE_CACHE_MISSING;
        break;
    case MOQR_FETCH_ITEM_END:
        *end = c->cache_end;
        *kind = RETRIEVE_CACHE_ABSENT;
        break;
    default:
        return MOQR_ERR_INTERNAL;
    }
    if (retrieve_loc_cmp(*end, c->next) < 0 ||
        retrieve_loc_cmp(*end, retrieve_window(c)) > 0) return MOQR_ERR_INTERNAL;
    return MOQR_OK;
}


moqr_result_t
moqr_bind_retrieve_init(moqr_bind_t *b, moqr_retrieve_cursor_t *c,
                        const moqr_retrieve_request_t *r)
{
    if (b == NULL || c == NULL || r == NULL ||
        retrieve_loc_cmp(r->start, r->end) > 0) return MOQR_ERR_INVAL;
    if (c->live) return MOQR_ERR_WRONG_STATE;
    if (r->group_order != MOQR_GROUP_ORDER_ASCENDING &&
        !(r->standalone && r->group_order == MOQR_GROUP_ORDER_DESCENDING)) {
        return MOQR_ERR_UNSUPPORTED;
    }
    if (r->standalone && !moqr_fetch_is_valid(r->owner)) return MOQR_ERR_INVAL;
    if (!retrieve_parent_current(b, r)) return MOQR_ERR_STALE_HANDLE;
    memset(c, 0, sizeof(*c));
    c->request = *r;
    c->next = moqr_retrieve_first(r->start, r->end, r->group_order);
    c->cache = MOQR_FETCH_INVALID;
    c->live = true;
    return MOQR_OK;
}


/* Once origin work ends, convert its stream into explicit coverage. FIN
 * proves the trailing default gap ONLY through the accepted FETCH_OK end.
 * A shortened OK leaves an UNKNOWN tail, never invented non-existence. On
 * timeout, complete received items (even before OK) remain usable; only the
 * unproven tail is TIMED_OUT. No hard failure enters this path. */
static void
retrieve_collection_front(moqr_retrieve_cursor_t *c, const b_fwd_t *t)
{
    if (t != NULL && t->next_write < t->n_items) {
        const b_fwd_item_t *it = &t->items[t->next_write];
        moqr_retrieve_location_t end = retrieve_loc(it->group, it->object);
        if (it->is_gap) {
            retrieve_gap(c, end, (moq_fetch_range_kind_t)it->range_kind);
        } else if (retrieve_loc_cmp(end, c->next) > 0) {
            retrieve_gap(c, retrieve_previous(end), MOQ_FETCH_RANGE_NON_EXISTENT);
        } else {
            moqr_record_view_t object;
            memset(&object, 0, sizeof(object));
            object.group_id = it->group;
            object.subgroup_id = it->subgroup;
            object.object_id = it->object;
            object.publisher_priority = it->publisher_priority;
            object.datagram_pref = it->datagram;
            object.status = MOQR_OBJ_NORMAL;
            object.payload = it->payload;
            object.properties = it->properties;
            object.properties_codec = it->properties_codec;
            retrieve_object(c, &object);
        }
        return;
    }
    if (t != NULL && t->end == FW_END_COMPLETE) {
        moqr_retrieve_location_t end = retrieve_inclusive_end(t->ok_end_group,
                                                               t->ok_end_object);
        if (retrieve_loc_cmp(c->next, end) <= 0) {
            retrieve_gap(c, end, MOQ_FETCH_RANGE_NON_EXISTENT);
        } else {
            retrieve_gap(c, c->span_end, MOQ_FETCH_RANGE_UNKNOWN);
        }
    } else {
        retrieve_gap(c, c->span_end, MOQ_FETCH_RANGE_TIMED_OUT);
    }
}


moqr_result_t
moqr_bind_retrieve_peek(moqr_bind_t *b, moqr_retrieve_cursor_t *c,
                        uint64_t now_us, moqr_retrieve_item_t *out)
{
    if (b == NULL || c == NULL || out == NULL || !c->live) return MOQR_ERR_INVAL;
    if (c->pending && c->item.kind == MOQR_RETRIEVE_ERROR) {
        *out = c->item;
        return MOQR_OK;
    }
    b_fwd_t *t = retrieve_collector(b, c);
    if (t != NULL && t->end != FW_END_NONE && t->end != FW_END_COMPLETE &&
        t->end != FW_END_TIMEOUT) {
        /* Preserve the actual latched terminal, even if the parent/source
         * disappears later in the same pump. */
        retrieve_fwd_error(c, t);
        *out = c->item;
        return MOQR_OK;
    }
    if (!retrieve_parent_current(b, &c->request)) {
        retrieve_error(c, MOQR_RETRIEVE_SOURCE_LOST, MOQR_ERR_STALE_HANDLE);
    } else if (c->source_bound) {
        b_retrieve_source_t source;
        if (!retrieve_source(b, &c->request, true, c->source_cookie,
                              c->source_binding, c->source_parent, &source)) {
            retrieve_error(c, MOQR_RETRIEVE_SOURCE_LOST, MOQR_ERR_STALE_HANDLE);
        }
    }
    if ((!c->pending || c->item.kind != MOQR_RETRIEVE_ERROR) && t == NULL &&
        c->collector_slot != 0) {
        retrieve_error(c, MOQR_RETRIEVE_SOURCE_LOST, MOQR_ERR_STALE_HANDLE);
    }
    if (c->pending) {
        if (c->item.kind == MOQR_RETRIEVE_ERROR && t != NULL &&
            t->end == FW_END_NONE) {
            fwd_end_up(t, FW_END_LOST);
            b->budget_pending = true;
        }
        *out = c->item;
        return MOQR_OK;
    }
    if (c->done) {
        memset(&c->item, 0, sizeof(c->item));
        c->item.kind = MOQR_RETRIEVE_END;
        c->item.start = c->request.start;
        c->item.end = c->request.end;
        c->pending = true;
        *out = c->item;
        return MOQR_OK;
    }
    if (!c->span_active) {
        c->waiting_origin = false;
        retrieve_cache_kind_t kind;
        moqr_retrieve_location_t end;
        moqr_result_t rc = retrieve_cache_front(b, c, now_us, &kind, &end);
        if (rc == MOQR_ERR_WOULD_BLOCK) return rc;
        if (rc != MOQR_OK) {
            retrieve_error(c, MOQR_RETRIEVE_LOCAL, rc);
        } else if (kind == RETRIEVE_CACHE_OBJECT) {
            retrieve_cached_object(b, c);
        } else if (kind == RETRIEVE_CACHE_ABSENT) {
            retrieve_gap(c, end, MOQ_FETCH_RANGE_NON_EXISTENT);
        } else if (kind == RETRIEVE_CACHE_PENDING) {
            if (now_us < c->request.deadline_us) {
                /* Re-read on the next pump: it completes, or is abandoned. */
                retrieve_cache_close(b, c);
                c->waiting_origin = true;
                return MOQR_ERR_WOULD_BLOCK;
            }
            retrieve_gap(c, end, MOQ_FETCH_RANGE_TIMED_OUT);
        } else {
            c->span_active = true;
            c->span_end = end;
            c->span_refreshed = false;
        }
        if (c->pending) { *out = c->item; return MOQR_OK; }
    }
    t = retrieve_collector(b, c);
    if (t == NULL && c->collector_slot != 0) {
        retrieve_error(c, MOQR_RETRIEVE_SOURCE_LOST, MOQR_ERR_STALE_HANDLE);
    } else if (t == NULL && now_us < c->request.deadline_us) {
        moqr_result_t rc = retrieve_admit(b, c);
        if (rc == MOQR_ERR_WOULD_BLOCK && c->request.standalone) {
            /* No connection this relay could retrieve the run from: its
             * state cannot be determined (draft-22 3.2.2), which a FETCH
             * marks at once rather than wait for a source to appear. */
            retrieve_gap(c, c->span_end, MOQ_FETCH_RANGE_UNKNOWN);
            *out = c->item;
            return MOQR_OK;
        }
        if (rc == MOQR_ERR_WOULD_BLOCK) {
            /* No origin exists yet. No transaction has frozen this miss;
             * retire its cache peek so a newly complete local object can be
             * served on the next pump instead of waiting out the timer. */
            retrieve_cache_close(b, c);
            c->span_active = false;
            c->waiting_origin = true;
            return rc;
        }
        if (rc == MOQR_OK) return MOQR_ERR_WOULD_BLOCK;
        retrieve_error(c, rc == MOQR_ERR_STALE_HANDLE ? MOQR_RETRIEVE_SOURCE_LOST
                                                      : MOQR_RETRIEVE_LOCAL, rc);
    }
    if (c->pending) { *out = c->item; return MOQR_OK; }
    if (t != NULL) {
        /* Live coverage can commit before this FETCH delivers its copy. */
        while (t->next_write < t->n_items) {
            b_fwd_item_t *it = &t->items[t->next_write];
            if (retrieve_loc_cmp(retrieve_loc(it->group, it->object),
                                  c->next) >= 0) break;
            fwd_item_release(b, t, it);
            t->next_write++;
        }
        uint8_t before = t->u;
        retrieve_fwd_expire(t, now_us);
        if (before != FW_U_CANCELLING && t->u == FW_U_CANCELLING) {
            b->budget_pending = true;
        }
        if (t->end == FW_END_NONE) {
            /* No item is held here. Reopen the unresolved cache view at the
             * committed frontier without changing the admitted FETCH span. */
            retrieve_cache_close(b, c);
            retrieve_cache_kind_t kind;
            moqr_retrieve_location_t end;
            moqr_result_t rc = retrieve_cache_front(b, c, now_us, &kind, &end);
            if (rc == MOQR_ERR_WOULD_BLOCK) return rc;
            if (rc != MOQR_OK) {
                retrieve_error(c, MOQR_RETRIEVE_LOCAL, rc);
            } else if (kind == RETRIEVE_CACHE_OBJECT) {
                retrieve_cached_object(b, c);
            } else if (kind == RETRIEVE_CACHE_ABSENT) {
                retrieve_gap(c, retrieve_loc_min(end, c->span_end),
                              MOQ_FETCH_RANGE_NON_EXISTENT);
            } else {
                return MOQR_ERR_WOULD_BLOCK;
            }
            *out = c->item;
            return MOQR_OK;
        }
        if (t->end != FW_END_COMPLETE && t->end != FW_END_TIMEOUT) {
            retrieve_fwd_error(c, t);
            *out = c->item;
            return MOQR_OK;
        }
    }
    retrieve_collection_front(c, t);
    if (c->item.kind == MOQR_RETRIEVE_OBJECT) {
        /* The collector owns these bytes. Rebase the cache on the next peek,
         * without pinning a redundant cached object or retaining an old gap. */
        retrieve_cache_close(b, c);
    } else {
        /* Reconcile newly available cache data, without changing the admitted
         * span. Retire (not re-peek/commit) the old frozen core gap before
         * opening the same remaining coverage afresh. No extra core slot. */
        if (!c->span_refreshed) {
            retrieve_cache_close(b, c);
            c->span_refreshed = true;
        }
        retrieve_cache_kind_t kind;
        moqr_retrieve_location_t end;
        moqr_result_t rc = retrieve_cache_front(b, c, now_us, &kind, &end);
        if (rc == MOQR_ERR_WOULD_BLOCK) {
            c->pending = false;
            return rc;
        }
        if (rc != MOQR_OK) {
            retrieve_error(c, MOQR_RETRIEVE_LOCAL, rc);
        } else if (kind == RETRIEVE_CACHE_OBJECT &&
                   c->item.kind != MOQR_RETRIEVE_OBJECT) {
            retrieve_cached_object(b, c);
        } else if (c->item.kind == MOQR_RETRIEVE_GAP) {
            moqr_retrieve_location_t clipped = retrieve_loc_min(end, c->item.end);
            retrieve_gap(c, clipped, kind == RETRIEVE_CACHE_ABSENT
                                         ? MOQ_FETCH_RANGE_NON_EXISTENT
                                         : c->item.gap_kind);
        }
    }
    *out = c->item;
    return MOQR_OK;
}


/* Commit the frozen core item only after all of its coverage is accounted
 * for. Clipping a collector gap around a newly cached island must not skip
 * the rest of either item. END closes the capped cache plan, then reopens
 * the remaining original range on the next peek. */
static moqr_result_t
retrieve_cache_commit_through(moqr_bind_t *b, moqr_retrieve_cursor_t *c,
                              moqr_retrieve_location_t end)
{
    if (!c->cache_peeked) return MOQR_OK;
    moqr_retrieve_location_t held = retrieve_loc(c->cache_item.rec.group_id,
                                                 c->cache_item.rec.object_id);
    if (c->cache_item.kind == MOQR_FETCH_ITEM_MARKER) {
        held = retrieve_loc(c->cache_item.marker_group - 1u, UINT64_MAX);
    } else if (c->cache_item.kind == MOQR_FETCH_ITEM_END) {
        held = c->cache_end;
    }
    if (retrieve_loc_cmp(held, end) > 0) return MOQR_OK;
    if (c->cache_item.kind == MOQR_FETCH_ITEM_END) {
        retrieve_cache_close(b, c);
        return MOQR_OK;
    }
    moqr_result_t rc = moqr_core_fetch_commit(b->core, c->cache);
    if (rc == MOQR_OK) {
        c->cache_peeked = false;
        memset(&c->cache_item, 0, sizeof(c->cache_item));
        if (retrieve_loc_cmp(c->cache_end, end) <= 0) retrieve_cache_close(b, c);
    }
    return rc;
}


moqr_result_t
moqr_bind_retrieve_commit(moqr_bind_t *b, moqr_retrieve_cursor_t *c)
{
    if (b == NULL || c == NULL || !c->live || !c->pending) return MOQR_ERR_INVAL;
    if (c->item.kind == MOQR_RETRIEVE_ERROR) return MOQR_ERR_WRONG_STATE;
    if (c->item.kind == MOQR_RETRIEVE_END) return MOQR_OK;
    moqr_retrieve_location_t end = c->item.end;
    b_fwd_t *t = retrieve_collector(b, c);
    moqr_result_t rc = retrieve_cache_commit_through(b, c, end);
    if (rc != MOQR_OK) {
        retrieve_error(c, MOQR_RETRIEVE_LOCAL, rc);
        return rc;
    }
    if (t != NULL && t->next_write < t->n_items) {
        b_fwd_item_t *it = &t->items[t->next_write];
        if (retrieve_loc_cmp(retrieve_loc(it->group, it->object), end) <= 0) {
            fwd_item_release(b, t, it);
            t->next_write++;
        }
    }
    retrieve_cached_release(b, c);
    c->pending = false;
    if (c->span_active && retrieve_loc_cmp(c->span_end, end) <= 0) {
        retrieve_detach_collector(b, c);
        c->span_active = false;
        c->span_refreshed = false;
        c->cache_absent = false;
    }
    moqr_retrieve_location_t next;
    if (moqr_retrieve_after(c->request.start, c->request.end,
                            c->request.group_order, end, &next)) {
        c->next = next;
    } else {
        c->done = true;
    }
    memset(&c->item, 0, sizeof(c->item));
    return MOQR_OK;
}


void
moqr_bind_retrieve_close(moqr_bind_t *b, moqr_retrieve_cursor_t *c)
{
    if (b == NULL || c == NULL || !c->live) return;
    retrieve_detach_collector(b, c);
    retrieve_cache_close(b, c);
    retrieve_cached_release(b, c);
    memset(c, 0, sizeof(*c));
}


void
moqr_bind_retrieve_forget(moqr_retrieve_cursor_t *c)
{
    if (c != NULL) memset(c, 0, sizeof(*c));
}


uint64_t
moqr_bind_retrieve_next_deadline(const moqr_retrieve_cursor_t *c)
{
    /* An admitted collector's timer is bind-owned, including zero-credit
     * waits. Only the no-origin wait needs a cursor-owned timer here. */
    return c != NULL && c->live && !c->pending && !c->done && c->waiting_origin
               ? c->request.deadline_us : UINT64_MAX;
}


/* -- standalone FETCH --------------------------------------------------------- *
 * A standalone FETCH, on every draft, is admitted once, by this core's
 * retrieval owner or by the shard that owns the track, and answered with
 * FETCH_OK at once: its End Location is known at admission. Its range is then
 * covered in the requested group order through the shared retrieval cursor:
 * Objects this relay holds are served from its cache, and every run it does
 * not hold is retrieved with an upstream FETCH for exactly that run (draft-22
 * 3.2.3), all against the request's one total wait budget (9.20.5). A run
 * nothing can serve by then is marked Timed-Out (Unknown on a draft without
 * that marker) and the response continues; a FILL_TIMEOUT of 0 therefore
 * serves what is held and marks the rest at once. An upstream
 * FETCH that fails resets this response with UNKNOWN_OBJECT_STATUS (3.2.2),
 * and any local failure resets it with INTERNAL_ERROR: both are
 * request-local, never a session close or a FIN over missing Objects. The
 * subscriber's cancellation retires the request and its upstream work. */

void
bind_standalone_abort(moqr_bind_t *b, const b_conn_t *cn, b_fetch_t *bf,
                      moqr_reset_cause_t cause)
{
    bf->sa_abort_code = bind_retrieve_reset_code(b, cn, cause);
    bf->state = BF_FETCH_NEEDS_ABORT;
}


static b_fetch_t *
bind_fetch_free_slot(moqr_bind_t *b, b_conn_t *cn)
{
    for (uint32_t i = 0; i < b->n_fetches; i++) {
        if (!cn->fetches[i].used) return &cn->fetches[i];
    }
    return NULL;
}


/* Admission. The FETCH is already authorized; its range form is resolved by
 * whichever core owns the track. A Joining FETCH answered this way (see
 * fwd_admit) passes the retained identity its subscription was answered for
 * as join_track: a name that now resolves to another retained identity, here
 * or at the shard that owns the track, is refused, never served from that
 * one. A request refused for room is refused as capacity, as a join's
 * forwarded form is. */
void
bind_standalone_request(moqr_bind_t *b, b_conn_t *cn,
                        const moq_fetch_request_event_t *fq,
                        const moqr_fetch_req_t *freq,
                        const moq_fetch_request_options_t *fo,
                        const moqr_track_t *join_track, uint64_t now_us)
{
    b_fetch_t *bf = bind_fetch_free_slot(b, cn);
    if (bf == NULL) {
        bind_reject_admission(b, cn, MOQR_AUTH_FETCH, fq->fetch._opaque,
                              MOQR_ERR_CAPACITY, now_us);
        return;
    }
    const uint64_t deadline = bind_wait_deadline(
        b, now_us, fo != NULL && (fo->flags & MOQ_FETCH_REQUEST_HAS_FILL_TIMEOUT) != 0,
        fo != NULL ? fo->fill_timeout_ms : 0);
    const moqr_group_order_t order =
        fq->group_order == MOQ_GROUP_ORDER_DESCENDING
            ? MOQR_GROUP_ORDER_DESCENDING : MOQR_GROUP_ORDER_ASCENDING;
    moqr_fetch_req_t req = *freq;
    req.group_order = order;
    moqr_fetch_t owner = MOQR_FETCH_INVALID;
    moqr_core_retrieve_plan_t plan;
    moqr_result_t rc = moqr_core_internal_retrieve_open(b->core, cn->binding, &req,
                                                        now_us, &owner, &plan);
    if (rc != MOQR_OK) {
        /* The FETCH pool is full (or the request cannot be planned): this
         * request alone is refused, a full pool as capacity (with its retry
         * hint), as a join's forwarded form is. */
        if (rc == MOQR_ERR_CAPACITY) {
            bind_reject_admission(b, cn, MOQR_AUTH_FETCH, fq->fetch._opaque,
                                  MOQR_ERR_CAPACITY, now_us);
            return;
        }
        bind_fetch_refuse(b, cn, fq->fetch, MOQ_REQUEST_ERROR_INTERNAL_ERROR, now_us);
        return;
    }
    if (plan.wire.admit != MOQR_FETCH_ACCEPT) {
        if (plan.remote_cookie != 0 && b->has_sa_src) {
            moqr_retrieve_standalone_query_t q;
            memset(&q, 0, sizeof(q));
            q.remote_cookie = plan.remote_cookie;
            if (b->cluster_info.enabled) {
                moqr_cluster_route_t selected;
                if (moqr_core_cluster_route(b->core, cn->binding, req.ns, &selected, sizeof(selected)) != MOQ_OK) {
                    bind_fetch_refuse(b, cn, fq->fetch, MOQ_REQUEST_ERROR_DOES_NOT_EXIST, now_us);
                    return;
                }
                q.cluster_selected = true;
                q.cluster_source_advertisement = selected.advertisement;
                q.cluster_requester_hop_id = cn->cluster_peer_hop;
                q.cluster_requester_correlation_id = cn->cluster_correlation;
            }
            q.ns = req.ns;
            q.name = req.name;
            q.start_group = req.start_group;
            q.start_object = req.start_object;
            q.end_group = req.end_group;
            q.end_object = req.end_object;
            q.range_form = req.range_form;
            q.relative_start = req.relative_start;
            q.group_order = order;
            q.subscriber_priority = req.subscriber_priority;
            q.deadline_us = deadline;
            q.join = join_track != NULL;
            if (join_track != NULL) {
                q.join_track = *join_track;
            }
            uint64_t handle = 0;
            moqr_result_t orc = b->sa_src.open(b->sa_src.ctx, &q, &handle);
            if (orc == MOQR_OK) {
                memset(bf, 0, sizeof(*bf));
                bind_fetch_mark_used(cn, bf);
                bf->standalone = true;
                bf->sa_remote = true;
                bf->sa_handle = handle;
                bf->response_order = order;
                bf->state = BF_FETCH_NEEDS_PLAN;
                bf->sfetch = fq->fetch;
                bf->fill_deadline_us = deadline;
                return;
            }
            if (orc == MOQR_ERR_CAPACITY) {
                /* No retrieval slot here to route it through. */
                bind_reject_admission(b, cn, MOQR_AUTH_FETCH, fq->fetch._opaque,
                                      MOQR_ERR_CAPACITY, now_us);
                return;
            }
            plan.wire.error_code = join_track != NULL && orc == MOQR_ERR_STALE_HANDLE
                                       ? MOQ_REQUEST_ERROR_INVALID_JOINING_REQUEST_ID
                                       : MOQ_REQUEST_ERROR_INTERNAL_ERROR;
        }
        bind_fetch_refuse(b, cn, fq->fetch, plan.wire.error_code, now_us);
        return;
    }
    if (join_track != NULL && !moqr_track_eq(plan.track, *join_track)) {
        (void)moqr_core_fetch_close(b->core, owner);
        bind_fetch_refuse(b, cn, fq->fetch,
                          MOQ_REQUEST_ERROR_INVALID_JOINING_REQUEST_ID, now_us);
        return;
    }
    memset(bf, 0, sizeof(*bf));
    bf->standalone = true;
    bf->state = BF_NEEDS_OK;
    bf->sfetch = fq->fetch;
    bf->cfetch = owner;
    bf->eot = plan.wire.end_of_track;
    bf->end_group = plan.wire.end_group;
    bf->end_object = plan.wire.end_object;
    bf->fill_deadline_us = deadline;
    bf->response_order = order;
    bf->response_start = retrieve_loc(plan.start_group, plan.start_object);
    bf->response_end = retrieve_loc(plan.end_group, plan.end_object);
    bf->response_next = moqr_retrieve_first(bf->response_start, bf->response_end, order);
    moqr_retrieve_request_t r;
    memset(&r, 0, sizeof(r));
    r.binding = cn->binding;
    r.standalone = true;
    r.owner = owner;
    r.track = plan.track;
    r.track_gen = plan.track_gen;
    r.source = conn_cookie(b, cn);
    r.child = fq->fetch._opaque;
    r.start = bf->response_start;
    r.end = bf->response_end;
    r.group_order = order;
    r.subscriber_priority = req.subscriber_priority;
    r.deadline_us = deadline;
    if (moqr_bind_retrieve_init(b, &bf->retrieval, &r) != MOQR_OK) {
        (void)moqr_core_fetch_close(b->core, owner);
        memset(bf, 0, sizeof(*bf));
        bind_fetch_refuse(b, cn, fq->fetch, MOQ_REQUEST_ERROR_INTERNAL_ERROR, now_us);
        return;
    }
    bind_fetch_mark_used(cn, bf);
}


/* The subscriber cancelled (draft-22 3.2.4): nothing more is owed on the
 * request, and its cursor's release cancels whatever it asked upstream. */
bool
bind_standalone_cancelled(moqr_bind_t *b, b_conn_t *cn, moq_fetch_t fetch)
{
    for (uint32_t i = 0; i < b->n_fetches; i++) {
        b_fetch_t *bf = &cn->fetches[i];
        if (bf->used && bf->standalone && bf->sfetch._opaque == fetch._opaque) {
            bind_fill_release(b, bf);
            return true;
        }
    }
    return false;
}


/* An End of Range marker on a FETCH stream. A run ending with its group goes
 * through the profile seam that knows the draft's last Object ID. */
static moq_result_t
bind_fetch_write_range(b_conn_t *cn, moq_fetch_t fetch,
                       moq_fetch_range_kind_t kind, moqr_retrieve_location_t end,
                       uint64_t now_us)
{
    if (end.object == UINT64_MAX && end.group != UINT64_MAX) {
        return moq_session_write_fetch_range_before_group(cn->session, fetch, kind,
                                                          end.group + 1u, now_us);
    }
    return moq_session_write_fetch_range(cn->session, fetch, kind, end.group,
                                         end.object, now_us);
}


static moqr_result_t
bind_standalone_peek(moqr_bind_t *b, b_fetch_t *bf, uint64_t now_us,
                     moqr_retrieve_item_t *it)
{
    if (bf->sa_remote) {
        return b->sa_src.peek(b->sa_src.ctx, bf->sa_handle, now_us, it);
    }
    return moqr_bind_retrieve_peek(b, &bf->retrieval, now_us, it);
}


static moqr_result_t
bind_standalone_commit(moqr_bind_t *b, b_fetch_t *bf)
{
    if (bf->sa_remote) return b->sa_src.commit(b->sa_src.ctx, bf->sa_handle);
    return moqr_bind_retrieve_commit(b, &bf->retrieval);
}


/* One item of an accepted standalone FETCH. Every item must start exactly
 * where the previous one ended in the request's order and stay inside its
 * group's part of the range; anything else resets the response rather than
 * write a stream that misstates what exists. WOULD_BLOCK consumes nothing. */
static bool
bind_standalone_item(moqr_bind_t *b, b_conn_t *cn, b_fetch_t *bf, uint64_t now_us)
{
    moqr_retrieve_item_t it;
    moqr_result_t prc = bind_standalone_peek(b, bf, now_us, &it);
    if (prc == MOQR_ERR_WOULD_BLOCK) return false;
    if (prc != MOQR_OK) {
        bind_standalone_abort(b, cn, bf, MOQR_RESET_INTERNAL_ERROR);
        return true;
    }
    if (it.kind == MOQR_RETRIEVE_ERROR) {
        bind_standalone_abort(b, cn, bf, bind_retrieve_error_cause(&it));
        return true;
    }
    if (it.kind == MOQR_RETRIEVE_END) {
        if (!bf->response_done) {
            bind_standalone_abort(b, cn, bf, MOQR_RESET_INTERNAL_ERROR);
            return true;
        }
        /* A run known not to exist at the end of the range is what FIN says. */
        bf->response_ne_pending = false;
        bf->state = BF_NEEDS_FIN;
        return true;
    }
    moqr_retrieve_location_t window = moqr_retrieve_window_end(
        bf->response_end, bf->response_order, bf->response_next.group);
    if (bf->response_done || retrieve_loc_cmp(it.start, bf->response_next) != 0 ||
        retrieve_loc_cmp(it.end, it.start) < 0 ||
        retrieve_loc_cmp(it.end, window) > 0) {
        bind_standalone_abort(b, cn, bf, MOQR_RESET_INTERNAL_ERROR);
        return true;
    }
    moq_result_t rc;
    if (it.kind == MOQR_RETRIEVE_OBJECT) {
        if (retrieve_loc_cmp(it.start, it.end) != 0 ||
            it.object.group_id != it.end.group ||
            it.object.object_id != it.end.object) {
            bind_standalone_abort(b, cn, bf, MOQR_RESET_INTERNAL_ERROR);
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
            rc = moq_session_write_fetch_object(cn->session, bf->sfetch, &oc, now_us);
        if (props_own != NULL) moq_rcbuf_decref(props_own);
    } else if (it.kind == MOQR_RETRIEVE_GAP) {
        if (it.gap_kind == MOQ_FETCH_RANGE_NON_EXISTENT) {
            /* Unmarked, a gap in a FETCH response says these Objects do not
             * exist (3.2.2). Held back in case a marked run follows. */
            rc = MOQ_OK;
        } else {
            if (bf->response_ne_pending) {
                /* Split the known non-existent run from the marked one. */
                rc = bind_fetch_write_range(cn, bf->sfetch,
                                            MOQ_FETCH_RANGE_NON_EXISTENT,
                                            bf->response_ne_end, now_us);
                if (rc == MOQ_ERR_WOULD_BLOCK) return false;
                if (rc != MOQ_OK) {
                    if (fwd_result_is_gone(rc)) bind_fill_release(b, bf);
                    else bind_standalone_abort(b, cn, bf, MOQR_RESET_INTERNAL_ERROR);
                    return true;
                }
                bf->response_ne_pending = false;
            }
            /* Draft-16 and draft-18 have no End of Timed-Out Range. A run
             * whose status was not confirmed upstream within the wait budget
             * is of unknown status, which a FETCH response there reports as
             * a range of unknown Objects while it goes on serving the known
             * ones (draft-16 9.16.3, draft-18 10.12.3). */
            moq_fetch_range_kind_t kind = it.gap_kind;
            if (kind == MOQ_FETCH_RANGE_TIMED_OUT && cn->wire_draft != B_DRAFT_22) {
                kind = MOQ_FETCH_RANGE_UNKNOWN;
            }
            rc = bind_fetch_write_range(cn, bf->sfetch, kind, it.end, now_us);
        }
    } else {
        bind_standalone_abort(b, cn, bf, MOQR_RESET_INTERNAL_ERROR);
        return true;
    }
    if (rc == MOQ_ERR_WOULD_BLOCK) return false;
    if (rc != MOQ_OK) {
        if (fwd_result_is_gone(rc)) bind_fill_release(b, bf);
        else bind_standalone_abort(b, cn, bf, MOQR_RESET_INTERNAL_ERROR);
        return true;
    }
    if (it.kind == MOQR_RETRIEVE_OBJECT) {
        bf->response_ne_pending = false;
        b->deliveries_written++;
    } else if (it.gap_kind == MOQ_FETCH_RANGE_NON_EXISTENT) {
        bf->response_ne_pending = true;
        bf->response_ne_end = it.end;
    }
    if (bind_standalone_commit(b, bf) != MOQR_OK) {
        bind_standalone_abort(b, cn, bf, MOQR_RESET_INTERNAL_ERROR);
        return true;
    }
    moqr_retrieve_location_t next;
    if (moqr_retrieve_after(bf->response_start, bf->response_end, bf->response_order, it.end, &next)) {
        bf->response_next = next;
    } else {
        bf->response_done = true;
    }
    return true;
}


/* Drive one standalone FETCH as far as the session allows; false means HOLD
 * (retry next pump, nothing advanced). */
static bool
bind_standalone_step(moqr_bind_t *b, b_conn_t *cn, b_fetch_t *bf, uint64_t now_us)
{
    moq_result_t rc;
    switch (bf->state) {
    case BF_FETCH_NEEDS_PLAN: {
        moqr_retrieve_standalone_plan_t plan;
        memset(&plan, 0, sizeof(plan));
        moqr_result_t prc = b->sa_src.plan(b->sa_src.ctx, bf->sa_handle, &plan);
        if (prc == MOQR_ERR_WOULD_BLOCK) return false;
        if (prc != MOQR_OK || plan.wire.admit != MOQR_FETCH_ACCEPT ||
            retrieve_loc_cmp(plan.start, plan.end) > 0) {
            moq_fetch_t fetch = bf->sfetch;
            /* No room at the owner (no retrieval slot, or its FETCH pool
             * full) is refused as capacity, as a full pool here is. */
            bool room = prc == MOQR_ERR_CAPACITY ||
                        (prc == MOQR_OK && plan.wire.admit == MOQR_FETCH_REJECT &&
                         plan.capacity);
            uint64_t code = prc == MOQR_OK && plan.wire.admit == MOQR_FETCH_REJECT
                                ? plan.wire.error_code
                                : MOQ_REQUEST_ERROR_INTERNAL_ERROR;
            bind_fill_release(b, bf);
            if (room) {
                bind_reject_admission(b, cn, MOQR_AUTH_FETCH, fetch._opaque,
                                      MOQR_ERR_CAPACITY, now_us);
            } else {
                bind_fetch_refuse(b, cn, fetch, code, now_us);
            }
            return true;
        }
        bf->eot = plan.wire.end_of_track;
        bf->end_group = plan.wire.end_group;
        bf->end_object = plan.wire.end_object;
        bf->response_start = plan.start;
        bf->response_end = plan.end;
        bf->response_next = moqr_retrieve_first(plan.start, plan.end, bf->response_order);
        bf->state = BF_NEEDS_OK;
        return true;
    }
    case BF_NEEDS_OK: {
        /* The track's properties are stated (§7.7): a local track's from its
         * reply pin, one another shard owns from the snapshot its owner took
         * at admission. A set this session cannot carry refuses the fetch
         * before FETCH_OK. */
        moqr_bind_meta_ref_t pin = {0};
        b_props_tmp_t ptmp = { NULL, 0 };
        moq_bytes_t pout = { NULL, 0 };
        bool stated;
        if (bf->sa_remote) {
            moqr_retrieve_standalone_plan_t sp;
            memset(&sp, 0, sizeof(sp));
            moqr_bind_meta_ref_t snap = {0};
            if (b->sa_src.plan(b->sa_src.ctx, bf->sa_handle, &sp) == MOQR_OK) {
                snap.state = sp.meta_state;
                snap.wire_version = sp.meta_wire_version;
                snap.bytes = sp.meta_bytes;
            }
            stated = bind_meta_out(b, &snap, moq_session_version(cn->session), &pout,
                                   &ptmp);
        } else {
            stated = bind_fetch_ok_pin(b, cn, bf->cfetch, &pin, &pout, &ptmp);
        }
        if (!stated) {
            moqr_bind_meta_release(b, &pin);
            moq_fetch_t fetch = bf->sfetch;
            bind_fill_release(b, bf);
            bind_fetch_refuse(b, cn, fetch, MOQ_REQUEST_ERROR_INTERNAL_ERROR, now_us);
            return true;
        }
        moq_accept_fetch_cfg_t ac;
        moq_accept_fetch_cfg_init(&ac);
        ac.end_of_track = bf->eot;
        ac.end_group = bf->end_group;
        ac.end_object = bf->end_object;
        ac.track_properties = pout;
        rc = moq_session_accept_fetch(cn->session, bf->sfetch, &ac, now_us);
        bind_track_props_release(b, &ptmp);
        moqr_bind_meta_release(b, &pin);
        if (rc == MOQ_ERR_WOULD_BLOCK) return false;
        if (rc == MOQ_OK) {
            bf->state = BF_STREAMING;
            return true;
        }
        if (fwd_result_is_gone(rc)) {
            bind_fill_release(b, bf);
            return true;
        }
        /* Nothing reached the wire: the request is still pending, so it is
         * refused instead. */
        moq_fetch_t fetch = bf->sfetch;
        b->session_errors++;
        bind_fill_release(b, bf);
        bind_fetch_refuse(b, cn, fetch, MOQ_REQUEST_ERROR_INTERNAL_ERROR, now_us);
        return true;
    }
    case BF_STREAMING:
        return bind_standalone_item(b, cn, bf, now_us);
    case BF_NEEDS_FIN:
        rc = moq_session_end_fetch(cn->session, bf->sfetch, now_us);
        if (rc == MOQ_ERR_WOULD_BLOCK) return false;
        if (rc == MOQ_OK || fwd_result_is_gone(rc)) {
            bind_fill_release(b, bf);
        } else {
            bind_standalone_abort(b, cn, bf, MOQR_RESET_INTERNAL_ERROR);
        }
        return true;
    case BF_FETCH_NEEDS_ABORT:
        rc = moq_session_abort_fetch(cn->session, bf->sfetch, bf->sa_abort_code,
                                     now_us);
        if (rc == MOQ_ERR_WOULD_BLOCK) return false;
        if (rc != MOQ_OK && !fwd_result_is_gone(rc)) b->session_errors++;
        bind_fill_release(b, bf);
        return true;
    default:
        b->session_errors++;   /* a fill state on a FETCH entry: a bug */
        bind_standalone_abort(b, cn, bf, MOQR_RESET_INTERNAL_ERROR);
        return true;
    }
}


/* The request's options (moq_fetch_request_options_t), when the event and
 * the record both reach the fields the binding reads; NULL otherwise (a
 * draft whose FETCH carries none). */
const moq_fetch_request_options_t *
bind_fetch_options(const moq_event_t *ev)
{
    const moq_fetch_request_event_t *fq = &ev->u.fetch_request;
    if (ev->detail_size < offsetof(moq_fetch_request_event_t, options) +
                              sizeof(fq->options) ||
        fq->options == NULL ||
        fq->options->struct_size < MOQ_FETCH_REQUEST_OPTIONS_V0_SIZE) {
        return NULL;
    }
    return fq->options;
}


/* A FETCH the session surfaced with its range unresolved (every one with a
 * Location Filter, moq_session_set_defer_fetch_ranges at attach) onto the
 * core's range form: no filter starts at {0, 0}; RELATIVE_GROUP and
 * NEXT_OBJECT start relative to the Largest Object; those and ABSOLUTE_START
 * end at it. The bounded forms are explicit, and so is the event's range. */
void
bind_fetch_range(const moq_fetch_request_options_t *fo, moqr_fetch_req_t *freq)
{
    if (fo == NULL || fo->struct_size <
            offsetof(moq_fetch_request_options_t, range_unresolved) +
                sizeof(fo->range_unresolved) || !fo->range_unresolved) {
        return;
    }
    const moq_location_filter_cfg_t *loc = &fo->location;
    switch (loc->kind) {
    case MOQ_LOCATION_FILTER_NONE:
        freq->range_form = MOQR_FETCH_END_AT_LARGEST;
        break;
    case MOQ_LOCATION_FILTER_RELATIVE_GROUP:
        freq->range_form =
            MOQR_FETCH_START_RELATIVE | MOQR_FETCH_END_AT_LARGEST;
        freq->relative_start = loc->relative_group_offset;
        break;
    case MOQ_LOCATION_FILTER_NEXT_OBJECT:
        freq->range_form =
            MOQR_FETCH_START_NEXT_OBJECT | MOQR_FETCH_END_AT_LARGEST;
        break;
    case MOQ_LOCATION_FILTER_ABSOLUTE_START:
        freq->range_form = MOQR_FETCH_END_AT_LARGEST;
        break;
    default:
        break;   /* GROUP_END / OBJECT_END: explicit */
    }
}


void
bind_fetch_pump(moqr_bind_t *b, b_conn_t *cn, uint64_t now_us)
{
    for (uint32_t fi = 0; fi < cn->fetch_hw; fi++) {
        b_fetch_t *bf = &cn->fetches[fi];
        if (!bf->used) {
            continue;
        }
        if (bf->fill) {
            uint32_t guard = 0;
            for (; guard < 512 && bf->used; guard++) {
                if (!bind_fill_step(b, cn, bf, now_us)) {
                    break;
                }
            }
            if (guard == 512 && bf->used) b->budget_pending = true;
            continue;
        }
        if (bf->standalone) {
            for (uint32_t guard = 0; guard < 512 && bf->used; guard++) {
                if (!bind_standalone_step(b, cn, bf, now_us)) {
                    break;
                }
            }
            continue;
        }
        /* Every fetch the binding admits is a fill or a standalone FETCH. */
    }
    /* Slots released at the top end no longer bound the walks. */
    while (cn->fetch_hw > 0 && !cn->fetches[cn->fetch_hw - 1u].used) {
        cn->fetch_hw--;
    }
}
