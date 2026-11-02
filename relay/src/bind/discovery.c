#include "bind_internal.h"
#include "discovery_internal.h"
#include "authorization_internal.h"
#include "fetch_joining_internal.h"
#include "intents_internal.h"
#include "metadata_internal.h"
#include "subscription_update_internal.h"

/* The pool every held answer fits in at once: at most one update is owed per
 * request, and the requests that can be updated are bounded by the core's
 * namespace subscriptions, track subscriptions and announced namespaces;
 * each track subscription may also owe its own first answer. */
uint32_t
bind_nsans_cap(const moqr_core_limits_t *lim)
{
    uint64_t n = 3u * (uint64_t)lim->max_ns_subs + (uint64_t)lim->max_ns_nodes;
    return n > UINT32_MAX ? UINT32_MAX : (uint32_t)n;
}

void bind_nsans_conn_gone(moqr_bind_t *b, const b_conn_t *cn);

void bind_dgrant_conn_gone(moqr_bind_t *b, const b_conn_t *cn);

bool bind_cookie_is_track_sub(uint64_t cookie);

bool bind_cookie_is_publication(uint64_t cookie);

void bind_tracks_reject(moqr_bind_t *b, b_conn_t *cn, uint64_t handle,
                        uint64_t err, uint64_t now_us);

static void bind_tracks_answer_held(moqr_bind_t *b, b_conn_t *cn, b_nsans_t *e,
                                    uint64_t now_us);

void bind_tracks_admit(moqr_bind_t *b, b_conn_t *cn, uint64_t handle,
                       moqr_ns_t prefix, bool forward,
                       bool include_properties, uint32_t group_order,
                       const moqr_auth_token_t *tokens,
                       size_t token_count, uint64_t now_us);

moqr_result_t bind_ann_withdraw(moqr_bind_t *b, b_conn_t *cn,
                                uint64_t raw, uint64_t now_us);

void
ann_clear_slot(moqr_bind_t *b, b_conn_t *cn, uint32_t slot)
{
    if (slot >= b->n_anns || cn->anns[slot].used == false) {
        return;
    }
    if (cn->anns[slot].bytes != NULL) {
        b->alloc.free(cn->anns[slot].bytes, cn->anns[slot].bytes_len,
                      b->alloc.ctx);
    }
    memset(&cn->anns[slot], 0, sizeof(cn->anns[slot]));
}


void bind_cluster_adverts_clear(moqr_bind_t *b, b_conn_t *cn, uint64_t handle);


void
ann_clear_all(moqr_bind_t *b, b_conn_t *cn)
{
    for (uint32_t i = 0; i < b->n_anns; i++) {
        ann_clear_slot(b, cn, i);
    }
}


int
ann_find(const moqr_bind_t *b, const b_conn_t *cn, uint64_t raw)
{
    for (uint32_t i = 0; i < b->n_anns; i++) {
        if (cn->anns[i].used && cn->anns[i].ann_raw == raw) {
            return (int)i;
        }
    }
    return -1;
}


/* Withdraw this connection's announce `raw` from the core and drop its
 * binding copy. MOQR_OK also when nothing is left to withdraw; WOULD_BLOCK
 * when the withdrawal's fan-out must wait for intent room (nothing changed). */
moqr_result_t
bind_ann_withdraw(moqr_bind_t *b, b_conn_t *cn, uint64_t raw, uint64_t now_us)
{
    int ann_slot = ann_find(b, cn, raw);
    if (ann_slot < 0) {
        return MOQR_OK;
    }
    moqr_result_t urc;
    BIND_CALL_RETRY(urc, moqr_core_unannounce(b->core, cn->binding,
                                              cn->anns[ann_slot].ns));
    if (urc == MOQR_ERR_WOULD_BLOCK) {
        return urc;
    }
    if (urc == MOQR_OK || urc == MOQR_ERR_STALE_HANDLE ||
        urc == MOQR_ERR_WRONG_STATE) {
        if (cn->anns[ann_slot].cluster_grant.gen != 0)
            moqr_core_grant_abort(b->core, cn->anns[ann_slot].cluster_grant);
        ann_clear_slot(b, cn, (uint32_t)ann_slot);
    } else {
        b->session_errors++;
    }
    return MOQR_OK;
}


int
ann_store(moqr_bind_t *b, b_conn_t *cn, uint64_t raw, moqr_ns_t ns)
{
    /* draft-18 Section 2.4.1 admits 0..32 fields, so a ROOT namespace (count 0)
     * is a legitimate announce and must be storable; draft-16's 1..32 floor is
     * enforced by the session profile before the event reaches here. Only a
     * NON-EMPTY namespace needs a parts array. */
    if (raw == 0 || ns.count > 32 || (ns.count > 0 && ns.parts == NULL) ||
        ann_find(b, cn, raw) >= 0) {
        return -1;
    }
    int slot = -1;
    for (uint32_t i = 0; i < b->n_anns; i++) {
        if (!cn->anns[i].used) {
            slot = (int)i;
            break;
        }
    }
    if (slot < 0) {
        return -1;
    }
    size_t total = 0;
    for (size_t i = 0; i < ns.count; i++) {
        /* Every PRESENT Track Namespace Field must carry at least one byte
         * (draft-18 and draft-16 Section 2.4.1). Zero fields is a legal ROOT
         * namespace; a zero-LENGTH field is not, whether or not its pointer is
         * non-NULL. */
        if (ns.parts[i].len == 0 || ns.parts[i].data == NULL) {
            return -1;
        }
        if (SIZE_MAX - total < ns.parts[i].len) {
            return -1;
        }
        total += ns.parts[i].len;
    }
    uint8_t *bytes = NULL;
    if (total > 0) {
        bytes = b->alloc.alloc(total, b->alloc.ctx);
        if (bytes == NULL) {
            return -1;
        }
    }
    b_ann_t *a = &cn->anns[slot];
    memset(a, 0, sizeof(*a));
    a->used = true;
    a->ann_raw = raw;
    a->bytes = bytes;
    a->bytes_len = total;
    a->ns.parts = a->parts;
    a->ns.count = ns.count;
    size_t off = 0;
    for (size_t i = 0; i < ns.count; i++) {
        /* Validated above: every present field has len > 0 and non-NULL data. */
        a->parts[i].len = ns.parts[i].len;
        memcpy(bytes + off, ns.parts[i].data, ns.parts[i].len);
        a->parts[i].data = bytes + off;
        off += ns.parts[i].len;
    }
    return slot;
}


bool
ns_equal(moqr_ns_t a, moqr_ns_t b)
{
    if (a.count != b.count) {
        return false;
    }
    for (size_t i = 0; i < a.count; i++) {
        if (a.parts[i].len != b.parts[i].len ||
            memcmp(a.parts[i].data, b.parts[i].data, a.parts[i].len) != 0) {
            return false;
        }
    }
    return true;
}


void
bind_cluster_adverts_clear(moqr_bind_t *b, b_conn_t *cn, uint64_t handle)
{
    if (cn->cluster_adverts == NULL) return;
    for (uint32_t i = 0; i < b->n_anns; i++) {
        b_cadv_t *a = &cn->cluster_adverts[i];
        if (!a->ann.used || (handle != 0 && a->ann.ann_raw != handle)) continue;
        if (a->ann.bytes != NULL)
            b->alloc.free(a->ann.bytes, a->ann.bytes_len, b->alloc.ctx);
        memset(a, 0, sizeof(*a));
    }
}


static moqr_result_t
bind_cluster_announce_current(moqr_bind_t *b, b_conn_t *cn, moqr_ns_t ns, uint64_t raw)
{
    int slot = ann_find(b, cn, raw);
    if (slot >= 0 && cn->anns[slot].discovered)
        return moqr_result_legacy(moqr_core_announce_cluster(b->core, cn->binding, ns, raw,
                                          &cn->anns[slot].cluster, false));
    moq_cluster_metadata_t metadata = {0};
    moq_result_t rc = moq_session_namespace_cluster(cn->session,
        (moq_announcement_t){raw}, &metadata, sizeof(metadata));
    if (rc != MOQ_OK && cn->cluster_negotiated) return MOQR_ERR_WRONG_STATE;
    return moqr_result_legacy(moqr_core_announce_cluster(b->core, cn->binding, ns, raw,
        cn->cluster_negotiated ? &metadata : NULL, false));
}


static bool
conn_has_other_announce(const moqr_bind_t *b, const b_conn_t *cn,
                        uint64_t raw, moqr_ns_t ns)
{
    for (uint32_t i = 0; i < b->n_anns; i++) {
        const b_ann_t *ann = &cn->anns[i];
        if (ann->used && ann->ann_raw != raw && ns_equal(ann->ns, ns)) {
            return true;
        }
    }
    return false;
}


/* The core has one source slot for an exact namespace. Drafts 16 Section 8.3
 * and 18 Section 9.3 permit establishment time to select a publisher when an
 * implementation cannot retain every source. This binding's deterministic
 * policy is newest-wins across connections: retire the incumbent generation,
 * including every track it sourced, then install the new one. A duplicate on
 * the same connection remains a request error and never displaces itself. */
moqr_result_t
bind_announce(moqr_bind_t *b, b_conn_t *cn, moqr_ns_t ns, uint64_t raw,
              uint64_t now_us)
{
    moqr_result_t rc;
    if (b->cluster_info.enabled) {
        BIND_CALL_RETRY(rc, bind_cluster_announce_current(b, cn, ns, raw));
        return rc;
    }
    BIND_CALL_RETRY(rc, moqr_core_announce_ex(b->core, cn->binding, ns, raw));
    if (rc != MOQR_ERR_WRONG_STATE ||
        conn_has_other_announce(b, cn, raw, ns)) {
        return rc;
    }

    BIND_CALL_RETRY(rc, moqr_core_force_withdraw(
                            b->core, ns, MOQ_REQUEST_ERROR_UNINTERESTED,
                            now_us));
    if (rc != MOQR_OK) {
        return rc;
    }
    BIND_CALL_RETRY(rc, moqr_core_announce_ex(b->core, cn->binding, ns, raw));
    return rc;
}

#ifdef MOQR_BIND_TESTING


/* Drive the PRODUCTION ann_store validator on one live conn slot.
 *
 * A malformed present Track Namespace Field cannot arrive through a real
 * session -- the parser rejects it first -- so this is the only way to exercise
 * bind's defense-in-depth arm without copying the predicate into the test.
 * Returns true when ann_store ACCEPTED the namespace; an accepted slot is
 * released again so the probe leaves no announcement state behind. */
bool
moqr_bind_debug_ann_store_probe(moqr_bind_t *b, uint32_t slot, uint64_t raw,
                                moqr_ns_t ns)
{
    if (b == NULL || slot >= b->max_conns || !b->conns[slot].used) {
        return false;
    }
    int r = ann_store(b, &b->conns[slot], raw, ns);
    if (r < 0) {
        return false;
    }
    ann_clear_slot(b, &b->conns[slot], (uint32_t)r);
    return true;
}

#endif

uint8_t
bind_nsu_family(uint64_t handle)
{
    switch (moq_handle_pool_tag(handle)) {
    case MOQ_HANDLE_POOL_ANNOUNCEMENT:     return B_NSU_ANN;
    case MOQ_HANDLE_POOL_NAMESPACE_SUB:    return B_NSU_NS;
    case MOQ_HANDLE_POOL_SUBSCRIBE_TRACKS: return B_NSU_TRACKS;
    default:                               return 0;
    }
}


bool
bind_cookie_is_publication(uint64_t cookie)
{
    return moq_handle_pool_tag(cookie) == MOQ_HANDLE_POOL_PUBLICATION;
}


bool
bind_cookie_is_track_sub(uint64_t cookie)
{
    return moq_handle_pool_tag(cookie) == MOQ_HANDLE_POOL_SUBSCRIBE_TRACKS;
}


static moq_result_t
bind_nsu_owed(b_conn_t *cn, uint8_t family, uint64_t handle,
              moq_namespace_update_record_t *rec)
{
    memset(rec, 0, sizeof(*rec));
    switch (family) {
    case B_NSU_ANN:
        return moq_session_namespace_owed_update(
            cn->session, (moq_announcement_t){ ._opaque = handle }, rec,
            sizeof(*rec));
    case B_NSU_NS:
        return moq_session_ns_sub_owed_update(
            cn->session, (moq_ns_sub_handle_t){ ._opaque = handle }, rec,
            sizeof(*rec));
    case B_NSU_TRACKS:
        return moq_session_subscribe_tracks_owed_update(
            cn->session, (moq_track_sub_handle_t){ ._opaque = handle }, rec,
            sizeof(*rec));
    default:
        return MOQ_ERR_INVAL;
    }
}


static moq_result_t
bind_nsu_answer(b_conn_t *cn, uint8_t family, uint64_t handle, uint64_t id,
                bool reject, uint64_t error_code, uint64_t now_us)
{
    if (!reject) {
        moq_accept_subscription_update_cfg_t a;
        moq_accept_subscription_update_cfg_init(&a);
        a.update_request_id = id;
        switch (family) {
        case B_NSU_ANN:
            return moq_session_accept_namespace_update(
                cn->session, (moq_announcement_t){ ._opaque = handle }, &a,
                now_us);
        case B_NSU_NS:
            return moq_session_accept_ns_sub_update(
                cn->session, (moq_ns_sub_handle_t){ ._opaque = handle }, &a,
                now_us);
        case B_NSU_TRACKS:
            return moq_session_accept_subscribe_tracks_update(
                cn->session, (moq_track_sub_handle_t){ ._opaque = handle }, &a,
                now_us);
        default:
            return MOQ_ERR_INVAL;
        }
    }
    moq_reject_subscription_update_cfg_t r;
    moq_reject_subscription_update_cfg_init(&r);
    r.update_request_id = id;
    r.error_code = (moq_request_error_t)error_code;
    r.reason = bind_subscribe_error_reason((moq_request_error_t)error_code);
    switch (family) {
    case B_NSU_ANN:
        return moq_session_reject_namespace_update(
            cn->session, (moq_announcement_t){ ._opaque = handle }, &r, now_us);
    case B_NSU_NS:
        return moq_session_reject_ns_sub_update(
            cn->session, (moq_ns_sub_handle_t){ ._opaque = handle }, &r, now_us);
    case B_NSU_TRACKS:
        return moq_session_reject_subscribe_tracks_update(
            cn->session, (moq_track_sub_handle_t){ ._opaque = handle }, &r,
            now_us);
    default:
        return MOQ_ERR_INVAL;
    }
}


void
bind_tokens_to_relay(const moq_resolved_token_t *in, size_t n,
                     moqr_auth_token_t *out)
{
    for (size_t i = 0; i < n; i++) {
        out[i].token_type = in[i].token_type;
        out[i].token_value = in[i].token_value;
    }
}


static void
bind_tokens_to_session(const moqr_auth_token_t *in, size_t n,
                       moq_resolved_token_t *out)
{
    for (size_t i = 0; i < n; i++) {
        out[i].token_type = in[i].token_type;
        out[i].token_value = in[i].token_value;
    }
}


/* What one update leaves in effect. The prefix and token views borrow the
 * session record (until the next advancing session call), the binding's
 * announcement copy, or core storage (until the next mutating core call). */
typedef struct b_nsu_view {
    moqr_ns_t            ns;
    moq_bytes_t          parts[32];
    moq_resolved_token_t tokens[MOQR_AUTH_MAX_TOKENS];
    size_t               token_count;
    bool                 prefix;     /* the update selects a new prefix   */
    bool                 forward_selected;
    bool                 forward;
    bool                 auth;       /* the update carries tokens         */
} b_nsu_view_t;


static moqr_result_t
bind_nsu_view(moqr_bind_t *b, b_conn_t *cn, uint8_t family, uint64_t handle,
              const moq_namespace_update_record_t *rec, b_nsu_view_t *v)
{
    memset(v, 0, sizeof(*v));
    v->prefix = (rec->flags & MOQ_NAMESPACE_UPDATE_PREFIX) != 0;
    v->forward_selected = (rec->flags & MOQ_NAMESPACE_UPDATE_FORWARD) != 0;
    v->forward = rec->forward;
    v->auth = (rec->flags & MOQ_NAMESPACE_UPDATE_AUTHORIZATION) != 0;
    if (v->prefix) {
        if (rec->track_namespace_prefix.count > 32) {
            return MOQR_ERR_INVAL;
        }
        for (size_t i = 0; i < rec->track_namespace_prefix.count; i++) {
            v->parts[i] = rec->track_namespace_prefix.parts[i];
        }
        v->ns = (moqr_ns_t){ .parts = v->parts,
                             .count = rec->track_namespace_prefix.count };
    } else if (family == B_NSU_ANN) {
        int slot = ann_find(b, cn, handle);
        if (slot < 0) {
            return MOQR_ERR_STALE_HANDLE;
        }
        v->ns = cn->anns[slot].ns;
    } else {
        moqr_result_t rc = moqr_core_subscription_prefix(
            b->core, cn->binding, handle, family == B_NSU_TRACKS, v->parts, 32,
            &v->ns);
        if (rc != MOQR_OK) {
            return rc;
        }
    }
    if (v->auth) {
        if (rec->token_count > MOQR_AUTH_MAX_TOKENS ||
            (rec->token_count > 0 && rec->tokens == NULL)) {
            return MOQR_ERR_INVAL;
        }
        for (size_t i = 0; i < rec->token_count; i++) {
            v->tokens[i] = rec->tokens[i];
        }
        v->token_count = rec->token_count;
    } else {
        const moqr_auth_token_t *t = NULL;
        size_t n = 0;
        if (moqr_core_request_tokens_get(b->core, cn->binding, handle, &t,
                                         &n) == MOQR_OK &&
            n <= MOQR_AUTH_MAX_TOKENS) {
            bind_tokens_to_session(t, n, v->tokens);
            v->token_count = n;
        }
    }
    return MOQR_OK;
}


static moqr_auth_action_t
bind_nsu_action(uint8_t family)
{
    return family == B_NSU_ANN ? MOQR_AUTH_PUBLISH_NAMESPACE
                               : MOQR_AUTH_SUBSCRIBE_NAMESPACE;
}


/* Retire what a namespace request held in the relay: its route or interest,
 * its announcement grant and its retained tokens. */
void
bind_nsu_retire(moqr_bind_t *b, b_conn_t *cn, uint8_t family, uint64_t handle,
                uint64_t now_us)
{
    uint64_t bcookie = (uint64_t)(cn - b->conns) + 1u;
    moqr_result_t rc;
    switch (family) {
    case B_NSU_ANN: {
        int slot = ann_find(b, cn, handle);
        if (slot >= 0) {
            BIND_CALL_RETRY(rc, moqr_core_unannounce(b->core, cn->binding,
                                                     cn->anns[slot].ns));
            if (rc == MOQR_OK || rc == MOQR_ERR_STALE_HANDLE ||
                rc == MOQR_ERR_WRONG_STATE) {
                ann_clear_slot(b, cn, (uint32_t)slot);
            } else {
                b->session_errors++;
            }
        }
        moqr_core_grant_release(b->core, bcookie, handle);
        break;
    }
    case B_NSU_NS:
        (void)moqr_core_ns_unsubscribe(b->core, cn->binding, handle);
        break;
    case B_NSU_TRACKS:
        (void)moqr_core_tracks_unsubscribe(b->core, cn->binding, handle);
        break;
    default:
        return;
    }
    moqr_core_request_tokens_clear(b->core, cn->binding, handle);
}


b_nsans_t *
bind_nsans_lookup(moqr_bind_t *b, const b_conn_t *cn, uint64_t handle,
                  bool initial)
{
    uint32_t ci = (uint32_t)(cn - b->conns);
    for (uint32_t i = 0; i < b->nsans_cap; i++) {
        if (b->nsans[i].used && b->nsans[i].conn == ci &&
            b->nsans[i].handle == handle && b->nsans[i].initial == initial) {
            return &b->nsans[i];
        }
    }
    return NULL;
}


/* The held answer to an update of this request. */
b_nsans_t *
bind_nsans_find(moqr_bind_t *b, const b_conn_t *cn, uint64_t handle)
{
    return bind_nsans_lookup(b, cn, handle, false);
}


void bind_nsans_drop(moqr_bind_t *b, b_nsans_t *e);


/* The request has ended: nothing held for it is owed any more. */
void
bind_nsans_forget(moqr_bind_t *b, const b_conn_t *cn, uint64_t handle)
{
    for (int k = 0; k < 2; k++) {
        b_nsans_t *e = bind_nsans_lookup(b, cn, handle, k != 0);
        if (e != NULL) {
            bind_nsans_drop(b, e);
        }
    }
}


void
bind_nsans_drop(moqr_bind_t *b, b_nsans_t *e)
{
    if (e->gres.gen != 0) {
        moqr_core_grant_abort(b->core, e->gres);
    }
    memset(e, 0, sizeof(*e));
    b->nsans_live--;
}


void
bind_nsans_conn_gone(moqr_bind_t *b, const b_conn_t *cn)
{
    uint32_t ci = (uint32_t)(cn - b->conns);
    for (uint32_t i = 0; i < b->nsans_cap && b->nsans_live > 0; i++) {
        if (b->nsans[i].used && b->nsans[i].conn == ci) {
            bind_nsans_drop(b, &b->nsans[i]);
        }
    }
}


/* Hold an answer. A pool sized for every owed update cannot be full; if it
 * is, the connection is closed rather than leave an update unanswered. */
b_nsans_t *
bind_nsans_hold(moqr_bind_t *b, b_conn_t *cn, uint8_t family, uint64_t handle,
                uint64_t id, uint64_t now_us)
{
    b_nsans_t *e = bind_nsans_find(b, cn, handle);
    if (e == NULL) {
        for (uint32_t i = 0; i < b->nsans_cap; i++) {
            if (!b->nsans[i].used) {
                e = &b->nsans[i];
                memset(e, 0, sizeof(*e));
                e->used = true;
                b->nsans_live++;
                break;
            }
        }
    }
    if (e == NULL) {
        b->session_errors++;
        (void)moq_session_close(cn->session, BIND_SESSION_INTERNAL_ERROR,
                                "namespace update answer", now_us);
        conn_detach(b, cn, now_us);
        return NULL;
    }
    e->family = family;
    e->conn = (uint32_t)(cn - b->conns);
    e->handle = handle;
    e->update_id = id;
    return e;
}


/* Hold a track subscription's first answer, sent on a later pump. */
void
bind_tracks_hold_answer(moqr_bind_t *b, b_conn_t *cn, uint64_t handle,
                        bool reject, uint64_t err, uint64_t now_us)
{
    b_nsans_t *e = bind_nsans_lookup(b, cn, handle, true);
    if (e == NULL) {
        for (uint32_t i = 0; i < b->nsans_cap; i++) {
            if (!b->nsans[i].used) {
                e = &b->nsans[i];
                memset(e, 0, sizeof(*e));
                e->used = true;
                b->nsans_live++;
                break;
            }
        }
    }
    if (e == NULL) {
        b->session_errors++;
        (void)moq_session_close(cn->session, BIND_SESSION_INTERNAL_ERROR,
                                "track subscription answer", now_us);
        conn_detach(b, cn, now_us);
        return;
    }
    e->initial = true;
    e->family = B_NSU_TRACKS;
    e->conn = (uint32_t)(cn - b->conns);
    e->handle = handle;
    e->reject = reject;
    e->error_code = err;
}


/* Refuse an update: the request ends, and the relay stops serving it now. */
static void
bind_nsu_reject(moqr_bind_t *b, b_conn_t *cn, uint8_t family, uint64_t handle,
                uint64_t id, uint64_t err, uint64_t now_us)
{
    if (err == 0) {
        err = MOQ_REQUEST_ERROR_UNAUTHORIZED;
    }
    b_nsans_t *held = bind_nsans_find(b, cn, handle);
    if (held != NULL) {
        bind_nsans_drop(b, held);
    }
    bind_nsu_retire(b, cn, family, handle, now_us);
    moq_result_t rc = bind_nsu_answer(cn, family, handle, id, true, err, now_us);
    if (rc == MOQ_ERR_WOULD_BLOCK) {
        b_nsans_t *e = bind_nsans_hold(b, cn, family, handle, id, now_us);
        if (e != NULL) {
            e->reject = true;
            e->error_code = err;
        }
        return;
    }
    if (rc != MOQ_OK && !fwd_result_is_gone(rc)) {
        b->session_errors++;
    }
}


/* True while the ordered ring still holds a namespace notification for this
 * namespace subscription: it names a suffix of the prefix in effect, so the
 * acceptance of a new prefix must not overtake it. */
static bool
bind_ring_holds_ns(const moqr_bind_t *b, uint64_t bcookie, uint64_t handle)
{
    for (uint32_t i = 0; i < b->pending_count; i++) {
        const moqr_intent_t *it =
            &b->pending[(b->pending_head + i) % b->pending_cap];
        if ((it->kind == MOQR_INTENT_NS_FOUND ||
             it->kind == MOQR_INTENT_NS_GONE) &&
            it->binding_cookie == bcookie && it->cookie == handle) {
            return true;
        }
    }
    return false;
}


/* One attempt to accept a held update. true when the entry is settled
 * (answered, refused, or moot); false while it must still be held. */
static moqr_result_t
bind_cluster_update_current(moqr_bind_t *b, b_conn_t *cn, uint64_t handle)
{
    int slot = ann_find(b, cn, handle);
    if (slot < 0) return MOQR_ERR_STALE_HANDLE;
    moq_namespace_update_record_t rec;
    if (moq_session_namespace_owed_update(cn->session,
            (moq_announcement_t){handle}, &rec, sizeof(rec)) != MOQ_OK)
        return MOQR_ERR_STALE_HANDLE;
    return moqr_result_legacy(moqr_core_announce_cluster(b->core, cn->binding,
        cn->anns[slot].ns, handle, &rec.cluster, true));
}


static bool
bind_nsu_commit(moqr_bind_t *b, b_conn_t *cn, b_nsans_t *e, uint64_t now_us)
{
    uint8_t family = e->family;
    uint64_t handle = e->handle;
    uint64_t id = e->update_id;
    uint64_t bcookie = (uint64_t)(cn - b->conns) + 1u;
    if (e->reject) {
        moq_result_t rc = bind_nsu_answer(cn, family, handle, id, true,
                                          e->error_code, now_us);
        if (rc == MOQ_ERR_WOULD_BLOCK) {
            return false;
        }
        if (rc != MOQ_OK && !fwd_result_is_gone(rc)) {
            b->session_errors++;
        }
        bind_nsans_drop(b, e);
        return true;
    }
    moq_namespace_update_record_t rec;
    b_nsu_view_t v;
    if (bind_nsu_owed(cn, family, handle, &rec) != MOQ_OK ||
        rec.update_request_id != id ||
        bind_nsu_view(b, cn, family, handle, &rec, &v) != MOQR_OK) {
        bind_nsans_drop(b, e);   /* nothing is owed any more */
        return true;
    }
    /* The new prefix outlives the acceptance, which ends the record's
     * borrow: keep a copy of its bytes. */
    uint8_t prefix_bytes[MOQ_FULL_TRACK_NAME_MAX];
    if (v.prefix) {
        size_t off = 0;
        for (size_t i = 0; i < v.ns.count; i++) {
            if (v.parts[i].len > sizeof(prefix_bytes) - off) {
                bind_nsans_drop(b, e);
                bind_nsu_reject(b, cn, family, handle, id,
                                MOQ_REQUEST_ERROR_EXCESSIVE_LOAD, now_us);
                return true;
            }
            if (v.parts[i].len > 0) {
                memcpy(prefix_bytes + off, v.parts[i].data, v.parts[i].len);
            }
            v.parts[i].data = prefix_bytes + off;
            off += v.parts[i].len;
        }
    }
    uint32_t tflags = 0;
    if (family == B_NSU_TRACKS) {
        tflags = (v.prefix ? MOQR_TRACKS_UPDATE_PREFIX : 0u) |
                 (v.forward_selected ? MOQR_TRACKS_UPDATE_FORWARD : 0u) |
                 (v.auth ? MOQR_TRACKS_UPDATE_TOKENS : 0u);
    }
    /* Stage the change before answering: everything it needs is held from
     * here, so the acceptance on the wire is always followed. */
    moqr_ns_update_prep_t nprep;
    moqr_tracks_update_prep_t tprep;
    memset(&nprep, 0, sizeof(nprep));
    memset(&tprep, 0, sizeof(tprep));
    moqr_result_t crc = MOQR_OK;
    if (family == B_NSU_NS && v.prefix) {
        BIND_CALL_RETRY(crc, moqr_core_ns_update_prepare(b->core, cn->binding,
                                                         handle, v.ns, &nprep));
    } else if (family == B_NSU_TRACKS && tflags != 0) {
        crc = moqr_core_tracks_update_prepare(b->core, cn->binding, handle,
                                              tflags, v.ns, v.forward, &tprep);
    }
    if (crc == MOQR_ERR_WOULD_BLOCK) {
        return false;
    }
    if (crc != MOQR_OK) {
        bind_nsans_drop(b, e);
        bind_nsu_reject(b, cn, family, handle, id,
                        crc == MOQR_ERR_CAPACITY || crc == MOQR_ERR_NOMEM
                            ? MOQ_REQUEST_ERROR_EXCESSIVE_LOAD
                            : MOQ_REQUEST_ERROR_INTERNAL_ERROR,
                        now_us);
        return true;
    }
    if (family == B_NSU_NS && v.prefix && bind_ring_holds_ns(b, bcookie, handle)) {
        moqr_core_ns_update_abort(b->core, &nprep);
        return false;   /* an old-prefix notification goes first */
    }
    if (family == B_NSU_ANN && b->cluster_info.enabled && !e->cluster_applied) {
        BIND_CALL_RETRY(crc, bind_cluster_update_current(b, cn, handle));
        if (crc == MOQR_ERR_WOULD_BLOCK) return false;
        if (crc != MOQR_OK) {
            bind_nsans_drop(b, e);
            bind_nsu_reject(b, cn, family, handle, id,
                crc == MOQR_ERR_NOMEM || crc == MOQR_ERR_CAPACITY
                    ? MOQ_REQUEST_ERROR_EXCESSIVE_LOAD : MOQ_REQUEST_ERROR_INTERNAL_ERROR, now_us);
            return true;
        }
        e->cluster_applied = true;
    }
    moq_result_t arc = bind_nsu_answer(cn, family, handle, id, false, 0, now_us);
    if (arc != MOQ_OK) {
        moqr_core_ns_update_abort(b->core, &nprep);
        moqr_core_tracks_update_abort(b->core, &tprep);
        if (arc == MOQ_ERR_WOULD_BLOCK) {
            return false;
        }
        if (!fwd_result_is_gone(arc)) {
            b->session_errors++;
        }
        bind_nsans_drop(b, e);
        return true;
    }
    /* Accepted: apply the change it acknowledged. */
    moqr_grant_res_t gres = e->gres;
    e->gres.gen = 0;
    bind_nsans_drop(b, e);
    moqr_result_t prc = MOQR_OK;
    switch (family) {
    case B_NSU_ANN:
        if (gres.gen != 0) {
            moqr_core_grant_release(b->core, bcookie, handle);
            prc = moqr_core_grant_commit(b->core, gres, 0);
        }
        break;
    case B_NSU_NS:
        if (nprep.staged) {
            prc = moqr_core_ns_update_commit(b->core, &nprep);
        }
        break;
    case B_NSU_TRACKS:
        if (tprep.staged) {
            prc = moqr_core_tracks_update_commit(b->core, &tprep);
        }
        break;
    default:
        break;
    }
    if (prc != MOQR_OK) {
        /* The acceptance is on the wire and the relay could not follow it:
         * the connection cannot be told the truth, so it ends. */
        b->session_errors++;
        (void)moq_session_close(cn->session, BIND_SESSION_INTERNAL_ERROR,
                                "namespace update failed", now_us);
        conn_detach(b, cn, now_us);
    }
    return true;
}


/* The update is authorized (`lease` is an announcement's revalidation
 * lease): take the tokens it carries and any grant, then accept it. */
static void
bind_nsu_decide(moqr_bind_t *b, b_conn_t *cn, uint8_t family, uint64_t handle,
                uint64_t id, uint64_t lease, uint64_t now_us)
{
    moq_namespace_update_record_t rec;
    b_nsu_view_t v;
    if (bind_nsu_owed(cn, family, handle, &rec) != MOQ_OK ||
        rec.update_request_id != id) {
        return;   /* nothing is owed any more */
    }
    if (bind_nsu_view(b, cn, family, handle, &rec, &v) != MOQR_OK) {
        bind_nsu_reject(b, cn, family, handle, id,
                        MOQ_REQUEST_ERROR_INTERNAL_ERROR, now_us);
        return;
    }
    moqr_grant_res_t gres;
    memset(&gres, 0, sizeof(gres));
    if (family == B_NSU_ANN && lease > 0) {
        moqr_park_req_t gpr;
        bind_park_init(&gpr, cn, b, MOQR_AUTH_PUBLISH_NAMESPACE, v.ns,
                       (moq_bytes_t){ NULL, 0 }, handle);
        if (bind_grant_reserve(b, &gpr, v.tokens, v.token_count, lease, now_us,
                               &gres) != MOQR_OK) {
            bind_nsu_reject(b, cn, family, handle, id,
                            MOQ_REQUEST_ERROR_UNAUTHORIZED, now_us);
            return;
        }
    }
    if (v.auth) {
        moqr_auth_token_t mt[MOQR_AUTH_MAX_TOKENS];
        bind_tokens_to_relay(v.tokens, v.token_count, mt);
        if (moqr_core_request_tokens_set(b->core, cn->binding, handle,
                                         v.token_count > 0 ? mt : NULL,
                                         v.token_count) != MOQR_OK) {
            if (gres.gen != 0) moqr_core_grant_abort(b->core, gres);
            bind_nsu_reject(b, cn, family, handle, id,
                            MOQ_REQUEST_ERROR_EXCESSIVE_LOAD, now_us);
            return;
        }
    }
    b_nsans_t *e = bind_nsans_hold(b, cn, family, handle, id, now_us);
    if (e == NULL) {
        if (gres.gen != 0) moqr_core_grant_abort(b->core, gres);
        return;
    }
    e->reject = false;
    e->gres = gres;
    (void)bind_nsu_commit(b, cn, e, now_us);
}


/* Authorize from `stage` on: REQUEST_UPDATE first, then the original
 * namespace action. A DEFER parks the stage that deferred. */
void
bind_nsu_authorize(moqr_bind_t *b, b_conn_t *cn, uint8_t family,
                   uint64_t handle, uint64_t id, moqr_auth_action_t stage,
                   uint64_t now_us)
{
    moq_namespace_update_record_t rec;
    b_nsu_view_t v;
    if (bind_nsu_owed(cn, family, handle, &rec) != MOQ_OK ||
        rec.update_request_id != id) {
        return;
    }
    if (bind_nsu_view(b, cn, family, handle, &rec, &v) != MOQR_OK) {
        bind_nsu_reject(b, cn, family, handle, id,
                        MOQ_REQUEST_ERROR_INTERNAL_ERROR, now_us);
        return;
    }
    uint64_t lease = 0;
    for (;;) {
        uint64_t err = 0, ticket = 0;
        bind_auth_result_t ar = bind_authorize(
            b, cn, stage, v.ns, (moq_bytes_t){ NULL, 0 }, v.tokens,
            v.token_count, now_us, &err, &ticket,
            stage == MOQR_AUTH_PUBLISH_NAMESPACE ? &lease : NULL);
        if (ar == BIND_AUTH_DEFER) {
            moqr_park_req_t pr;
            bind_park_init(&pr, cn, b, stage, v.ns, (moq_bytes_t){ NULL, 0 },
                           handle);
            pr.sub_filter_type = BIND_PARK_NS_UPDATE;
            pr.sub_start_group = id;
            if (bind_park(b, &pr, v.tokens, v.token_count, ticket, now_us) ==
                MOQR_OK) {
                return;   /* the update stays owed until the decision */
            }
            ar = BIND_AUTH_FAIL_CLOSED;
            err = MOQ_REQUEST_ERROR_UNAUTHORIZED;
        }
        if (ar != BIND_AUTH_ALLOW) {
            bind_nsu_reject(b, cn, family, handle, id, err, now_us);
            return;
        }
        if (stage == MOQR_AUTH_REQUEST_UPDATE) {
            stage = bind_nsu_action(family);
            continue;
        }
        break;
    }
    bind_nsu_decide(b, cn, family, handle, id, lease, now_us);
}


/* A deferred stage of an update's authorization was decided. */
void
bind_nsu_resolved(moqr_bind_t *b, b_conn_t *cn, const moqr_park_req_t *v,
                  const moqr_auth_verdict_t *verdict, uint64_t now_us)
{
    uint8_t family = bind_nsu_family(v->session_cookie);
    uint64_t id = v->sub_start_group;
    if (family == 0) {
        return;
    }
    if (verdict->decision != MOQR_AUTH_ALLOW) {
        bind_nsu_reject(b, cn, family, v->session_cookie, id,
                        verdict->error_code != 0
                            ? verdict->error_code
                            : MOQ_REQUEST_ERROR_UNAUTHORIZED,
                        now_us);
        return;
    }
    if (v->action == MOQR_AUTH_REQUEST_UPDATE) {
        bind_nsu_authorize(b, cn, family, v->session_cookie, id,
                           bind_nsu_action(family), now_us);
        return;
    }
    bind_nsu_decide(b, cn, family, v->session_cookie, id,
                    verdict->revalidate_after_us, now_us);
}


/* Retry every held answer, once per pump. */
void
bind_nsu_pump(moqr_bind_t *b, uint64_t now_us)
{
    for (uint32_t i = 0; i < b->nsans_cap && b->nsans_live > 0; i++) {
        b_nsans_t *e = &b->nsans[i];
        if (!e->used) {
            continue;
        }
        b_conn_t *cn = &b->conns[e->conn];
        if (!cn->used || cn->closed || cn->session == NULL) {
            bind_nsans_drop(b, e);
            continue;
        }
        if (cn->events_pending) continue;
        if (e->initial) {
            bind_tracks_answer_held(b, cn, e, now_us);
            continue;
        }
        (void)bind_nsu_commit(b, cn, e, now_us);
    }
}


/* -- Track discovery: SUBSCRIBE_TRACKS admission ---------------------------- */

/* Refuse a track subscription. An answer the session cannot take yet is
 * held and sent on a later pump. */
void
bind_tracks_reject(moqr_bind_t *b, b_conn_t *cn, uint64_t handle, uint64_t err,
                   uint64_t now_us)
{
    moq_reject_subscribe_tracks_cfg_t r;
    moq_reject_subscribe_tracks_cfg_init(&r);
    r.error_code = (moq_request_error_t)err;
    moq_result_t rc = moq_session_reject_subscribe_tracks(
        cn->session, (moq_track_sub_handle_t){ ._opaque = handle }, &r, now_us);
    if (rc == MOQ_ERR_WOULD_BLOCK) {
        bind_tracks_hold_answer(b, cn, handle, true, err, now_us);
    } else if (rc != MOQ_OK && !fwd_result_is_gone(rc)) {
        b->session_errors++;
    }
}


/* Accept a registered track subscription. Its interest stays held (nothing
 * is offered for it) until the acceptance is out; one the session cannot
 * take yet is held and sent on a later pump. False when it was not
 * accepted and the interest is gone. */
static bool
bind_tracks_accept(moqr_bind_t *b, b_conn_t *cn, uint64_t handle,
                   uint64_t now_us)
{
    moq_accept_subscribe_tracks_cfg_t a;
    moq_accept_subscribe_tracks_cfg_init(&a);
    moq_result_t rc = moq_session_accept_subscribe_tracks(
        cn->session, (moq_track_sub_handle_t){ ._opaque = handle }, &a, now_us);
    if (rc == MOQ_OK) {
        (void)moqr_core_tracks_answered(b->core, cn->binding, handle);
        return true;
    }
    if (rc == MOQ_ERR_WOULD_BLOCK) {
        bind_tracks_hold_answer(b, cn, handle, false, 0, now_us);
        return true;
    }
    /* Not accepted: no interest may outlive it. */
    (void)moqr_core_tracks_unsubscribe(b->core, cn->binding, handle);
    moqr_core_request_tokens_clear(b->core, cn->binding, handle);
    if (!fwd_result_is_gone(rc)) {
        b->session_errors++;
    }
    return false;
}


/* Retry a held first answer. */
static void
bind_tracks_answer_held(moqr_bind_t *b, b_conn_t *cn, b_nsans_t *e,
                        uint64_t now_us)
{
    uint64_t handle = e->handle;
    bool reject = e->reject;
    uint64_t err = e->error_code;
    bind_nsans_drop(b, e);   /* re-held below if still blocked */
    if (reject) {
        bind_tracks_reject(b, cn, handle, err, now_us);
    } else {
        (void)bind_tracks_accept(b, cn, handle, now_us);
    }
}


/* Admit an authorized track subscription: retain its tokens (each
 * discovered track is authorized with them), register its interest, then
 * accept it. Discovery offers matching tracks once the acceptance is out. */
void
bind_tracks_admit(moqr_bind_t *b, b_conn_t *cn, uint64_t handle, moqr_ns_t prefix,
                  bool forward, bool include_properties, uint32_t group_order,
                  const moqr_auth_token_t *tokens, size_t token_count,
                  uint64_t now_us)
{
    if (moqr_core_request_tokens_set(b->core, cn->binding, handle, tokens,
                                     token_count) != MOQR_OK) {
        bind_tracks_reject(b, cn, handle, MOQ_REQUEST_ERROR_EXCESSIVE_LOAD,
                           now_us);
        return;
    }
    moqr_tracks_req_t rq;
    moqr_tracks_req_init_sized(&rq, sizeof(rq));
    rq.prefix = prefix;
    rq.cookie = handle;
    rq.forward = forward;
    rq.include_properties = include_properties;
    rq.group_order = (moqr_group_order_t)group_order;
    rq.held = true;
    moqr_result_t rc = moqr_core_tracks_subscribe(b->core, cn->binding, &rq);
    if (rc != MOQR_OK) {
        moqr_core_request_tokens_clear(b->core, cn->binding, handle);
        bind_tracks_reject(b, cn, handle,
                           rc == MOQR_ERR_CAPACITY
                               ? MOQ_REQUEST_ERROR_EXCESSIVE_LOAD
                               : MOQ_REQUEST_ERROR_INTERNAL_ERROR,
                           now_us);
        return;
    }
    (void)bind_tracks_accept(b, cn, handle, now_us);
}


/* -- Track discovery: offering matching tracks ------------------------------ */

/* Hold a deferred ALLOW for its offer. False when there is no room. */
static bool
bind_dgrant_put(moqr_bind_t *b, b_conn_t *cn, uint64_t tcookie, uint64_t track,
                uint64_t track_gen, uint32_t input_gen, moqr_grant_res_t gres)
{
    for (uint32_t i = 0; i < BIND_DGRANTS; i++) {
        b_dgrant_t *g = &b->dgrants[i];
        if (!g->used) {
            g->used = true;
            g->conn = (uint32_t)(cn - b->conns);
            g->tcookie = tcookie;
            g->track = track;
            g->track_gen = track_gen;
            g->input_gen = input_gen;
            g->gres = gres;
            return true;
        }
    }
    return false;
}


/* The connection is gone: its held ALLOWs with it (the core retires their
 * reservations with the binding). */
void
bind_dgrant_conn_gone(moqr_bind_t *b, const b_conn_t *cn)
{
    uint32_t ci = (uint32_t)(cn - b->conns);
    for (uint32_t i = 0; i < BIND_DGRANTS; i++) {
        if (b->dgrants[i].used && b->dgrants[i].conn == ci) {
            memset(&b->dgrants[i], 0, sizeof(b->dgrants[i]));
        }
    }
}


/* Take the held ALLOW for this offer, if any. Held decisions made under the
 * track subscription's earlier inputs, or for an earlier instance of the
 * track, no longer authorize anything: their reservations are released. */
static bool
bind_dgrant_take(moqr_bind_t *b, b_conn_t *cn, const moqr_discovery_t *d,
                 moqr_grant_res_t *gres)
{
    bool found = false;
    uint32_t ci = (uint32_t)(cn - b->conns);
    for (uint32_t i = 0; i < BIND_DGRANTS; i++) {
        b_dgrant_t *g = &b->dgrants[i];
        if (!g->used || g->conn != ci || g->tcookie != d->cookie) {
            continue;
        }
        bool same_track = g->track == d->track._opaque;
        if (g->input_gen == d->input_gen && same_track &&
            g->track_gen == d->track_gen && !found) {
            *gres = g->gres;
            found = true;
        } else if (g->input_gen == d->input_gen && !same_track) {
            continue;   /* another track's decision, still current */
        } else if (g->gres.gen != 0) {
            moqr_core_grant_abort(b->core, g->gres);
        }
        memset(g, 0, sizeof(*g));
    }
    return found;
}


#define BIND_DISCOVERY_PER_PUMP 32u


/* Offer the binding's unoffered matches, each as a PUBLISH whose
 * subscription the relay then serves like any other: authorized for the
 * track with the track subscription's tokens (never copied into the
 * PUBLISH), carrying the subscription's FORWARD, GROUP_ORDER and, when asked
 * for, the track's properties. A track that cannot get a subscription is
 * reported with PUBLISH_SKIPPED and is not offered again for that source
 * (§3.6.3); one the subscriber is not authorized for is not offered until
 * its tokens change. */
void
bind_discovery_pump(moqr_bind_t *b, b_conn_t *cn, uint64_t now_us)
{
    for (uint32_t n = 0; n < BIND_DISCOVERY_PER_PUMP; n++) {
        if (!cn->used || cn->closed) {
            return;
        }
        moqr_discovery_t d;
        if (moqr_core_next_discovery(b->core, cn->binding, &d, sizeof(d)) != MOQR_OK) {
            return;
        }
        moq_resolved_token_t toks[MOQR_AUTH_MAX_TOKENS];
        size_t ntok = 0;
        const moqr_auth_token_t *rt = NULL;
        if (moqr_core_request_tokens_get(b->core, cn->binding, d.cookie, &rt,
                                         &ntok) != MOQR_OK ||
            ntok > MOQR_AUTH_MAX_TOKENS) {
            ntok = 0;
        }
        bind_tokens_to_session(rt, ntok, toks);
        uint64_t err = 0, ticket = 0, lease = 0;
        /* A deferred ALLOW is offered under the grant reserved when it
         * arrived; any other offer is authorized now, with the track
         * subscription's current tokens. */
        moqr_grant_res_t gres;
        memset(&gres, 0, sizeof(gres));
        bool held = d.authorized && bind_dgrant_take(b, cn, &d, &gres);
        bind_auth_result_t ar = held
            ? BIND_AUTH_ALLOW
            : bind_authorize(b, cn, MOQR_AUTH_SUBSCRIBE, d.ns, d.name, toks,
                             ntok, now_us, &err, &ticket, &lease);
        if (ar == BIND_AUTH_DEFER) {
            /* Hold the offer until the verifier decides; its ticket is
             * resolved through moqr_bind_auth_resolve, or retired with the
             * connection. */
            moqr_park_req_t pr;
            bind_park_init(&pr, cn, b, MOQR_AUTH_SUBSCRIBE, d.ns, d.name,
                           d.cookie);
            pr.sub_filter_type = BIND_PARK_DISCOVERY;
            pr.sub_start_group = d.track._opaque;
            pr.sub_start_object = d.track_gen;
            pr.sub_end_group_delta = d.input_gen;
            moqr_track_t track = d.track;
            uint64_t gen = d.track_gen;
            uint64_t tcookie = d.cookie;
            uint32_t igen = d.input_gen;
            bool parked = bind_park(b, &pr, toks, ntok, ticket, now_us) ==
                          MOQR_OK;
            (void)moqr_core_discovery_done(b->core, cn->binding, tcookie,
                                           track, gen, igen,
                                           parked ? MOQR_DISCOVERY_PENDING
                                                  : MOQR_DISCOVERY_DECLINED);
            continue;
        }
        if (ar == BIND_AUTH_ALLOW && !held && lease > 0) {
            /* A time-limited ALLOW is kept only as a revalidation grant,
             * reserved before the offer; without one nothing is offered,
             * as a SUBSCRIBE without one is refused. */
            moqr_park_req_t gpr;
            bind_park_init(&gpr, cn, b, MOQR_AUTH_SUBSCRIBE, d.ns, d.name,
                           d.cookie);
            if (bind_grant_reserve(b, &gpr, toks, ntok, lease, now_us,
                                   &gres) != MOQR_OK) {
                ar = BIND_AUTH_FAIL_CLOSED;
            }
        }
        if (ar != BIND_AUTH_ALLOW) {
            (void)moqr_core_discovery_done(b->core, cn->binding, d.cookie,
                                           d.track, d.track_gen, d.input_gen,
                                           MOQR_DISCOVERY_DECLINED);
            continue;
        }
        moq_result_t prc = MOQ_ERR_REQUEST_BLOCKED;
        moq_publication_t pub = { 0 };
        int slot = dsub_free_slot(b, cn);
        /* The relay's subscription is reserved before the offer goes on the
         * wire: a track the relay has no subscription for is SKIPPED, never
         * offered and then finished for an internal error (§3.6.3). */
        bool reserved = slot >= 0 &&
                        moqr_core_internal_sub_reserve(b->core) == MOQR_OK;
        if (reserved) {
            /* Asked for, the track's properties are all stated or the offer
             * is not made (a relay includes all of them, d16 §2.5, d18 §2.5,
             * d22 §7.7): a set this draft cannot carry, or a required record
             * that is not available, is reported like a track without a
             * subscription. An unmanaged track's set is EMPTY. */
            moqr_bind_meta_ref_t props;
            memset(&props, 0, sizeof(props));
            b_props_tmp_t ptmp = { NULL, 0 };
            moq_bytes_t pout = { NULL, 0 };
            bool statable = true;
            if (d.include_properties) {
                bind_meta_reply_pin(b, d.track, &props);
                statable = bind_meta_out(b, &props, moq_session_version(cn->session), &pout,
                                         &ptmp);
            }
            moq_publish_cfg_t pc;
            moq_publish_cfg_init_sized(&pc, sizeof(pc));
            pc.track_namespace = (moq_namespace_t){ .parts = d.ns.parts,
                                                    .count = d.ns.count };
            /* A PUBLISH names the track's Largest Object once objects exist:
             * the session takes it from this track history. */
            if (d.has_largest) {
                (void)moq_session_note_object_published(
                    cn->session, &pc.track_namespace, d.name, d.largest_group,
                    d.largest_object);
            }
            pc.track_name = d.name;
            pc.has_forward = true;
            pc.forward = d.forward;
            if (d.group_order != MOQR_GROUP_ORDER_DEFAULT) {
                pc.has_group_order = true;
                pc.group_order = (moq_group_order_t)d.group_order;
            }
            pc.track_properties = pout;
            if (statable) {
                prc = moq_session_publish(cn->session, &pc, now_us, &pub);
            } else {
                FWD_DBG(b, publish_withheld);
            }
            bind_track_props_release(b, &ptmp);
            moqr_bind_meta_release(b, &props);
        }
        if (reserved && prc != MOQ_OK) {
            moqr_core_internal_sub_release_reserve(b->core);
        }
        if (prc == MOQ_ERR_WOULD_BLOCK) {
            /* The same match is offered again next pump: a held ALLOW stays
             * held; a decision made now is made again then. */
            FWD_DBG(b, discovery_retried);
            if (gres.gen != 0 &&
                !(held && bind_dgrant_put(b, cn, d.cookie, d.track._opaque,
                                          d.track_gen, d.input_gen, gres))) {
                moqr_core_grant_abort(b->core, gres);
            } else if (held && gres.gen == 0) {
                (void)bind_dgrant_put(b, cn, d.cookie, d.track._opaque,
                                      d.track_gen, d.input_gen, gres);
            }
            return;
        }
        if (prc != MOQ_OK && gres.gen != 0) {
            moqr_core_grant_abort(b->core, gres);
            memset(&gres, 0, sizeof(gres));
        }
        if (prc == MOQ_OK) {
            /* The relay's subscription starts paused: nothing is sent before
             * the subscriber's PUBLISH_OK, which sets its Forward. */
            moqr_subscribe_req_t rq;
            moqr_subscribe_req_init(&rq);
            rq.ns = d.ns;
            rq.name = d.name;
            rq.group_order = d.group_order;
            rq.cookie = pub._opaque;
            rq.forward = false;
            rq.range_end_holds = bind_range_end_holds(cn);
            rq.reserved = true;
            memset(&cn->dsubs[slot], 0, sizeof(cn->dsubs[slot]));
            cn->dsubs[slot].used = true;
            cn->dsubs[slot].publication = true;
            cn->dsubs[slot].sub_raw = pub._opaque;
            moqr_track_t track = d.track;
            uint64_t gen = d.track_gen;
            uint64_t tcookie = d.cookie;
            uint32_t igen = d.input_gen;
            moqr_sub_t rs;
            moqr_result_t src;
            BIND_CALL_RETRY(src, moqr_core_subscribe(b->core, cn->binding, &rq,
                                                     &rs));
            if (src != MOQR_OK) {
                moqr_core_internal_sub_release_reserve(b->core);
                if (gres.gen != 0) {
                    moqr_core_grant_abort(b->core, gres);
                }
            }
            if (!cn->used || cn->closed) {
                return;
            }
            if (src == MOQR_OK) {
                cn->dsubs[slot].rsub = rs;
                bind_grant_commit_sub(b, gres, rs, now_us);
            } else {
                memset(&cn->dsubs[slot], 0, sizeof(cn->dsubs[slot]));
                moqr_pd_desc_t pd = bind_local_pd(MOQR_PD_INTERNAL_ERROR);
                uint64_t wire = 0;
                moq_finish_publish_cfg_t fc;
                moq_finish_publish_cfg_init(&fc);
                if (moqr_pd_desc_emit(pd, moqr_bind_conn_version(b, cn->session),
                                      &wire) == MOQR_OK) {
                    fc.status_code = wire;
                }
                if (moq_session_finish_publish(cn->session, pub, &fc, now_us) !=
                    MOQ_OK) {
                    b->session_errors++;
                }
            }
            (void)moqr_core_discovery_done(b->core, cn->binding, tcookie, track,
                                           gen, igen, MOQR_DISCOVERY_OFFERED);
            continue;
        }
        /* No subscription for this track: say so on the track
         * subscription's stream, relative to its prefix. */
        size_t skip = d.prefix_count <= d.ns.count ? d.prefix_count : d.ns.count;
        moq_namespace_t suffix = { .parts = d.ns.parts + skip,
                                   .count = d.ns.count - skip };
        moq_result_t src = moq_session_send_publish_blocked(
            cn->session, (moq_track_sub_handle_t){ ._opaque = d.cookie },
            &suffix, d.name, now_us);
        if (src == MOQ_ERR_WOULD_BLOCK) {
            FWD_DBG(b, discovery_retried);
            return;   /* the match is reported again next pump */
        }
        if (src != MOQ_OK && !fwd_result_is_gone(src)) {
            b->session_errors++;
        }
        (void)moqr_core_discovery_done(b->core, cn->binding, d.cookie, d.track,
                                       d.track_gen, d.input_gen,
                                       MOQR_DISCOVERY_SKIPPED);
    }
}


/* A deferred discovery decision arrived. It counts only for the track
 * subscription's inputs it was asked under (a token update since then makes
 * the core refuse it as stale). An ALLOW is held for its offer with its
 * revalidation grant; a lease the relay cannot keep declines the track. */
void
bind_discovery_resolved(moqr_bind_t *b, b_conn_t *cn, const moqr_park_req_t *v,
                        const moqr_auth_verdict_t *verdict, uint64_t now_us)
{
    moqr_track_t track = { ._opaque = v->sub_start_group };
    uint64_t track_gen = v->sub_start_object;
    uint32_t input_gen = (uint32_t)v->sub_end_group_delta;
    uint32_t outcome = MOQR_DISCOVERY_DECLINED;
    moqr_grant_res_t gres;
    memset(&gres, 0, sizeof(gres));
    if (verdict->decision == MOQR_AUTH_ALLOW) {
        outcome = MOQR_DISCOVERY_ALLOWED;
        if (verdict->revalidate_after_us > 0 &&
            moqr_core_grant_reserve(b->core, v, verdict->revalidate_after_us,
                                    now_us, &gres) != MOQR_OK) {
            outcome = MOQR_DISCOVERY_DECLINED;
        }
    }
    moqr_result_t rc = moqr_core_discovery_done(b->core, cn->binding,
                                                v->session_cookie, track,
                                                track_gen, input_gen, outcome);
    if (outcome != MOQR_DISCOVERY_ALLOWED) {
        return;
    }
    if (rc != MOQR_OK ||
        !bind_dgrant_put(b, cn, v->session_cookie, track._opaque, track_gen,
                         input_gen, gres)) {
        if (gres.gen != 0) {
            moqr_core_grant_abort(b->core, gres);
        }
    }
}


/* The subscriber answered a PUBLISH discovery sent: PUBLISH_OK starts the
 * relay subscription with the Forward the subscriber chose; PUBLISH_ERROR
 * retires it. */
void
bind_publication_answered(moqr_bind_t *b, b_conn_t *cn, uint64_t pub_raw,
                          bool ok, bool forward, uint64_t now_us)
{
    b_dsub_t *ds = dsub_find(b, cn, pub_raw);
    if (ds == NULL || !ds->publication) {
        return;
    }
    if (!ok) {
        bind_dsub_retire(b, cn, ds, now_us);
        return;
    }
    moqr_result_t frc = moqr_core_sub_set_forward(b->core, ds->rsub, forward,
                                                  now_us);
    if (frc != MOQR_OK && frc != MOQR_ERR_STALE_HANDLE) {
        b->session_errors++;
    }
}


/* Resolve negotiated peer state once, then originate root discovery after
 * SETUP authorization has been handled. Neither operation opens a transport. */
void bind_cluster_sync(moqr_bind_t *b, b_conn_t *cn, uint64_t now_us, bool discover)
{
    if (!b->cluster_info.enabled || moq_session_state(cn->session) != MOQ_SESS_ESTABLISHED)
        return;
    moq_cluster_info_t info;
    if (moq_session_cluster_info(cn->session, &info, sizeof(info)) != MOQ_OK)
        return;
    if (!cn->cluster_peer_ready) {
        uint64_t cost = cn->cluster_cost_override ? cn->cluster_cost : info.peer_relay_cost;
        moqr_cluster_peer_cfg_t peer;
        moqr_cluster_peer_cfg_init(&peer);
        peer.negotiated = info.negotiated;
        peer.peer_hop_id = info.peer_hop_id;
        peer.effective_relay_cost = cost;
        peer.correlation_id = cn->cluster_correlation;
        if (moqr_core_binding_set_cluster(b->core, cn->binding, &peer) != MOQ_OK) {
            b->session_errors++;
            (void)moq_session_close(cn->session, BIND_SESSION_INTERNAL_ERROR,
                                    "cluster peer configuration failed", now_us);
            conn_detach(b, cn, now_us);
            return;
        }
        cn->cluster_negotiated = info.negotiated;
        cn->cluster_peer_hop = info.peer_hop_id;
        cn->cluster_peer_ready = true;
    }
    if (discover && cn->cluster_negotiated && cn->cluster_discovery._opaque == 0) {
        moq_subscribe_namespace_cfg_t cfg;
        moq_subscribe_namespace_cfg_init(&cfg);
        cfg.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
        moq_result_t rc =
            moq_session_subscribe_namespace(cn->session, &cfg, now_us, &cn->cluster_discovery);
        if (rc != MOQ_OK && rc != MOQ_ERR_WOULD_BLOCK && rc != MOQ_ERR_REQUEST_BLOCKED) {
            b->session_errors++;
            (void)moq_session_close(cn->session, BIND_SESSION_INTERNAL_ERROR,
                                    "cluster discovery failed", now_us);
            conn_detach(b, cn, now_us);
        }
    }
}


void bind_cluster_discovery_end(moqr_bind_t *b, b_conn_t *cn, uint64_t handle,
                                uint64_t now_us)
{
    for (uint32_t i = 0; i < b->n_anns; i++) {
        b_ann_t *a = &cn->anns[i];
        if (a->used && a->discovered && a->discovery._opaque == handle)
            (void)bind_ann_withdraw(b, cn, a->ann_raw, now_us);
    }
    cn->cluster_discovery_closed = true;
    cn->cluster_discovery_retiring = false;
    for (uint32_t i = 0; i < b->n_anns; i++) {
        const b_ann_t *a = &cn->anns[i];
        if (a->used && a->discovered && a->discovery._opaque == handle)
            cn->cluster_discovery_retiring = true;
    }
    /* Keep the terminal handle: rediscovery is a host reconnect policy,
     * not a tight loop against a peer that rejected discovery. */
}


void bind_cluster_discovery_cancel(moqr_bind_t *b, b_conn_t *cn, moq_ns_sub_handle_t handle,
                                   uint64_t now_us)
{
    moq_result_t rc = moq_session_cancel_namespace_sub(cn->session, handle, now_us);
    cn->cluster_discovery_cancel_owed = rc == MOQ_ERR_WOULD_BLOCK;
    bind_cluster_discovery_end(b, cn, handle._opaque, now_us);
}


/* The latest candidate lives in an eager per-slot path buffer. Updating
 * metadata cannot allocate or replace the engine's old route until admitted. */
void bind_cluster_apply_ann(moqr_bind_t *b, b_conn_t *cn, uint32_t slot, uint64_t now_us)
{
    b_ann_t *a = &cn->anns[slot];
    if (!a->used || !a->discovered || !a->cluster_pending)
        return;
    moqr_result_t rc;
    BIND_CALL_RETRY(rc, moqr_result_legacy(moqr_core_announce_cluster(b->core, cn->binding, a->ns, a->ann_raw,
                                                   &a->cluster, !a->cluster_initial)));
    if (rc == MOQR_ERR_WOULD_BLOCK)
        return;
    if (rc == MOQR_OK) {
        a->cluster_pending = false;
        a->cluster_initial = false;
        if (a->cluster_grant.gen != 0) {
            moqr_grant_res_t grant = a->cluster_grant;
            a->cluster_grant.gen = 0;
            if (moqr_core_grant_commit(b->core, grant, 0) != MOQR_OK)
                bind_cluster_discovery_cancel(b, cn, a->discovery, now_us);
        }
        return;
    }
    bool loop = false;
    for (size_t i = 0; i < a->cluster.hop_count; i++)
        if (b->cluster_info.local_hop_id != 0 &&
            a->cluster.hop_ids[i] == b->cluster_info.local_hop_id)
            loop = true;
    if (loop) {
        /* A looping replacement invalidates this route, while unrelated
         * advertisements on its discovery stream remain usable. */
        a->cluster_pending = false;
        (void)bind_ann_withdraw(b, cn, a->ann_raw, now_us);
    } else {
        bind_cluster_discovery_cancel(b, cn, a->discovery, now_us);
    }
}


void bind_cluster_found(moqr_bind_t *b, b_conn_t *cn,
                        const moq_namespace_found_event_t *found, uint64_t now_us)
{
    if (cn->cluster_discovery_closed || !cn->cluster_negotiated ||
        found->handle._opaque != cn->cluster_discovery._opaque)
        return;
    const moq_ns_sub_handle_t discovery = found->handle;
    moqr_ns_t ns = {found->track_namespace_suffix.parts, found->track_namespace_suffix.count};
    if (found->cluster.hop_count == 0 || found->cluster.hop_ids == NULL ||
        found->cluster.hop_count > b->cluster_info.max_hops) {
        bind_cluster_discovery_cancel(b, cn, discovery, now_us);
        return;
    }
    int slot = -1;
    for (uint32_t i = 0; i < b->n_anns; i++) {
        b_ann_t *a = &cn->anns[i];
        if (a->used && a->discovered && a->discovery._opaque == discovery._opaque &&
            ns_equal(a->ns, ns)) {
            slot = (int)i;
            break;
        }
    }
    if (slot < 0) {
        uint64_t err = 0, ticket = 0, lease = 0;
        bind_auth_result_t auth =
            bind_authorize(b, cn, MOQR_AUTH_PUBLISH_NAMESPACE, ns, (moq_bytes_t){0}, NULL, 0,
                           now_us, &err, &ticket, &lease);
        if (auth != BIND_AUTH_ALLOW || b->cluster_next_discovery == UINT64_C(0x0fffffffffffffff)) {
            /* A response advertisement has no per-advertisement rejection.
             * DENY/DEFER ends discovery without installing unauthorized routes. */
            bind_cluster_discovery_cancel(b, cn, discovery, now_us);
            return;
        }
        uint64_t raw = UINT64_C(0xf000000000000000) | ++b->cluster_next_discovery;
        slot = ann_store(b, cn, raw, ns);
        if (slot < 0) {
            bind_cluster_discovery_cancel(b, cn, discovery, now_us);
            return;
        }
        b_ann_t *a = &cn->anns[slot];
        a->discovered = true;
        a->discovery = discovery;
        a->cluster_initial = true;
        moqr_park_req_t request;
        bind_park_init(&request, cn, b, MOQR_AUTH_PUBLISH_NAMESPACE, a->ns, (moq_bytes_t){0}, raw);
        if (bind_grant_reserve(b, &request, NULL, 0, lease, now_us, &a->cluster_grant) != MOQR_OK) {
            ann_clear_slot(b, cn, (uint32_t)slot);
            bind_cluster_discovery_cancel(b, cn, discovery, now_us);
            return;
        }
    }
    /* A metadata update changes neither the authorized namespace nor the
     * publisher lifetime. Its existing grant continues tick-based renewal;
     * it must not need a second grant slot for an unchanged authorization. */
    b_ann_t *a = &cn->anns[slot];
    uint64_t *path = cn->cluster_paths + (size_t)slot * b->cluster_info.max_hops;
    memcpy(path, found->cluster.hop_ids, found->cluster.hop_count * sizeof(uint64_t));
    a->cluster = found->cluster;
    a->cluster.hop_ids = path;
    a->cluster_pending = true;
    bind_cluster_apply_ann(b, cn, (uint32_t)slot, now_us);
}


/* Send the route currently eligible for this recipient. Durable advertisement
 * identity is recorded separately so a blocked origin change resumes after
 * its withdrawal and never repeats DONE or splices publishers. */
moq_result_t bind_cluster_namespace(moqr_bind_t *b, b_conn_t *cn, const moqr_intent_t *it,
                                    uint64_t now_us)
{
    moqr_ns_t ns = {it->ns_parts, it->ns_count};
    moq_namespace_t suffix = {it->ns_parts + it->value, it->ns_count - (size_t)it->value};
    moq_ns_sub_handle_t handle = {it->cookie};
    b_cadv_t *old = NULL, *free_slot = NULL;
    for (uint32_t i = 0; i < b->n_anns; i++) {
        b_cadv_t *a = &cn->cluster_adverts[i];
        if (!a->ann.used) {
            if (free_slot == NULL)
                free_slot = a;
        } else if (a->ann.ann_raw == it->cookie && ns_equal(a->ann.ns, ns))
            old = a;
    }
    moqr_cluster_route_t route;
    moq_result_t selected = moqr_core_cluster_route(b->core, cn->binding, ns, &route, sizeof(route));
    if (selected != MOQ_OK && selected != MOQ_DONE)
        return MOQ_ERR_INVAL;
    bool gone = selected == MOQ_DONE || route.prefix_count != ns.count;
    bool changed =
        old != NULL && !gone &&
        (old->origin != route.origin_id ||
         (route.origin_id == 0 && !moqr_cluster_advertisement_id_eq(old->source_advertisement, route.advertisement)));
    if (old != NULL && (gone || changed)) {
        moq_result_t rc = moq_session_send_namespace_done(cn->session, handle, &suffix, now_us);
        if (rc != MOQ_OK)
            return rc;
        if (old->ann.bytes != NULL)
            b->alloc.free(old->ann.bytes, old->ann.bytes_len, b->alloc.ctx);
        memset(old, 0, sizeof(*old));
        free_slot = old;
        old = NULL;
    }
    if (gone || (old != NULL && !cn->cluster_negotiated))
        return MOQ_OK;
    if (route.metadata.hop_count > b->cluster_info.max_hops || route.metadata.hop_ids == NULL)
        return MOQ_ERR_INVAL;
    memcpy(b->cluster_hops, route.metadata.hop_ids, route.metadata.hop_count * sizeof(uint64_t));
    b->cluster_hops[route.metadata.hop_count] = b->cluster_info.local_hop_id;
    moq_cluster_metadata_t metadata = route.metadata;
    metadata.hop_ids = b->cluster_hops;
    metadata.hop_count++;
    b_cadv_t candidate;
    memset(&candidate, 0, sizeof(candidate));
    if (old == NULL) {
        if (free_slot == NULL)
            return MOQ_ERR_NOMEM;
        size_t size = 0;
        for (size_t i = 0; i < ns.count; i++)
            size += ns.parts[i].len;
        if (size > BIND_ANN_BYTES_MAX)
            return MOQ_ERR_INVAL;
        if (size != 0) {
            candidate.ann.bytes = b->alloc.alloc(size, b->alloc.ctx);
            if (candidate.ann.bytes == NULL)
                return MOQ_ERR_NOMEM;
        }
        candidate.ann.bytes_len = size;
        candidate.ann.ns.count = ns.count;
        size_t at = 0;
        for (size_t i = 0; i < ns.count; i++) {
            candidate.ann.parts[i] = (moq_bytes_t){candidate.ann.bytes + at, ns.parts[i].len};
            memcpy(candidate.ann.bytes + at, ns.parts[i].data, ns.parts[i].len);
            at += ns.parts[i].len;
        }
    }
    moq_result_t rc = cn->cluster_negotiated
                          ? moq_session_send_namespace_cluster(cn->session, handle, &suffix,
                                                               &metadata, sizeof(metadata), now_us)
                          : moq_session_send_namespace(cn->session, handle, &suffix, now_us);
    if (rc != MOQ_OK) {
        if (candidate.ann.bytes != NULL)
            b->alloc.free(candidate.ann.bytes, candidate.ann.bytes_len, b->alloc.ctx);
        return rc;
    }
    if (old == NULL) {
        *free_slot = candidate;
        old = free_slot;
        old->ann.ns.parts = old->ann.parts;
        old->ann.used = true;
        old->ann.ann_raw = it->cookie;
    }
    old->origin = route.origin_id;
    old->source_advertisement = route.advertisement;
    return MOQ_OK;
}
