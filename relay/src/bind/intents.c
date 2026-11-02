#include "bind_internal.h"
#include "intents_internal.h"
#include "authorization_internal.h"
#include "delivery_internal.h"
#include "discovery_internal.h"
#include "fetch_joining_internal.h"
#include "metadata_internal.h"
#include "request_answers_internal.h"
#include "subscription_update_internal.h"
#include "testing_internal.h"
#include "upstream_internal.h"

bool bind_execute_intents(moqr_bind_t *b, uint64_t now_us);


/* Scalar-safe intent kinds: payload is fully self-contained, so a copy can be
 * parked in the pending ring and retried across pumps. Kinds that read the
 * borrowed ns_parts/name views (UPSTREAM_SUBSCRIBE / NS_FOUND / NS_GONE) are
 * deliberately absent — those byte spans borrow core-interned storage and are
 * valid only until the next mutating core call (relay.h:102), so they can
 * never be parked by a shallow copy. NS_FOUND / NS_GONE reach the same ring
 * through bind_defer_push_nsu, which deep-copies the suffix into the slot's
 * payload sidecar first; UPSTREAM_SUBSCRIBE remains consume-in-pump. */
static bool
bind_intent_deferrable(moqr_intent_kind_t kind)
{
    switch (kind) {
    case MOQR_INTENT_ACCEPT_SUB:
    case MOQR_INTENT_REJECT_SUB:
    case MOQR_INTENT_SUB_DONE:
    case MOQR_INTENT_ACCEPT_PUBLISH:
    case MOQR_INTENT_REJECT_PUBLISH:
    case MOQR_INTENT_TRACK_STATUS_OK:
    case MOQR_INTENT_TRACK_STATUS_ERROR:
        return true;
    default:
        return false;
    }
}
#define BIND_TRY_FROM_RING  1u   /* a retry of an already-parked slot      */

#define BIND_TRY_FORCE_PARK 2u   /* something is blocked: queue, never send */


bool bind_try_intent(moqr_bind_t *b, const moqr_intent_t *it,
                     uint64_t now_us, uint8_t mode,
                     bool *parked_out, moqr_intent_t *defer);


/* Deep-copy one namespace update's suffix into the retained ring. The intent's
 * ns_parts borrow core-interned storage that dies at the next mutating core
 * call, so nothing borrowed may escape this call.
 * Returns true if the update is now owned by the bind and will be retried.
 * false means held state is exhausted — the caller must fail closed rather
 * than drop a MUST-required update. */
static bool
bind_nsu_store(moqr_bind_t *b, uint32_t slot, moqr_intent_t *parked)
{
#ifdef MOQR_BIND_TESTING
    if (atomic_load(&bind_dbg_fail_nsu_stores) > 0 &&
        atomic_fetch_sub(&bind_dbg_fail_nsu_stores, 1) == 1) {
        return false;   /* injected: byte allocation / slot exhaustion */
    }
#endif
    /* The sidecar owns the SUFFIX the subscriber is owed -- the same span the
     * healthy send passes (ns_parts + value). The prefix bytes are never sent
     * and must not be copied or left semantically live in the parked intent,
     * but the prefix/full CARDINALITY is preserved (see below). */
    if (parked->value > parked->ns_count) {
        return false;
    }
    uint32_t off = b->cluster_info.enabled ? 0u : parked->value;
    uint32_t suffix_count = parked->ns_count - off;
    if (suffix_count > 32) {
        return false;
    }
    size_t total = 0;
    for (uint32_t i = 0; i < suffix_count; i++) {
        const moq_bytes_t *src = &parked->ns_parts[off + i];
        if (src->len > 0 && src->data == NULL) {
            return false;
        }
        if (SIZE_MAX - total < src->len) {
            return false;
        }
        total += src->len;
    }
    /* Defense in depth: never allocate more than the per-slot bound the
     * capacity description charges for. */
    if (total > BIND_ANN_BYTES_MAX) {
        return false;
    }
    uint8_t *bytes = NULL;
    if (total > 0) {
        bytes = b->alloc.alloc(total, b->alloc.ctx);
        if (bytes == NULL) {
            return false;
        }
    }
    b_nsu_t *u = &b->nsu[slot];
    if (u->used) {
        /* A live payload here means the ring handed out an occupied slot.
         * Overwriting it would silently drop an update already owed, so the
         * store is refused and the caller fails closed. The existing slot is
         * left exactly as it was. */
        if (bytes != NULL) {
            b->alloc.free(bytes, total, b->alloc.ctx);
        }
        return false;
    }
    u->used = true;   /* occupancy is explicit: a valid suffix may own 0 bytes */
    u->bytes = bytes;
    u->bytes_len = total;
    /* Only the SUFFIX is bind-owned, but ns_count and value keep their meaning:
     * ns_count is the FULL announced Track Namespace cardinality and value is
     * the subscribed prefix cardinality, exactly as the fresh intent carried
     * them. That distinction is load-bearing -- a true root announcement is
     * (ns_count 0, value 0), while a non-root namespace equal to its prefix is
     * (ns_count N, value N) and merely has an empty wire suffix -- so it must
     * survive retention or a replayed exact-prefix update would be
     * indistinguishable from a root one.
     *
     * The owned suffix descriptors therefore stay at their ORIGINAL indices
     * [value, ns_count); the prefix descriptors [0, value) are cleared so no
     * borrowed pointer survives the pump, and their bytes are never copied. */
    size_t o = 0;
    for (uint32_t i = 0; i < suffix_count; i++) {
        moq_bytes_t src = parked->ns_parts[off + i];
        parked->ns_parts[off + i].len = src.len;
        if (src.len == 0) {
            parked->ns_parts[off + i].data = NULL;   /* empty, not aliased */
            continue;
        }
        memcpy(bytes + o, src.data, src.len);
        parked->ns_parts[off + i].data = bytes + o;
        o += src.len;
    }
    for (uint32_t i = 0; i < off; i++) {
        parked->ns_parts[i].data = NULL;   /* no borrowed prefix left live */
        parked->ns_parts[i].len = 0;
    }
    b->nsu_live++;
    b->nsu_retained++;
    if (b->nsu_live > b->nsu_high_water) {
        b->nsu_high_water = b->nsu_live;
    }
    return true;
}


/* Release one sidecar slot's payload (delivered, purged, or torn down). */
static void
bind_nsu_release(moqr_bind_t *b, uint32_t slot)
{
    b_nsu_t *u = &b->nsu[slot];
    if (!u->used) {
        return;
    }
    if (u->bytes != NULL) {
        b->alloc.free(u->bytes, u->bytes_len, b->alloc.ctx);
    }
    b->nsu_live--;
    memset(u, 0, sizeof(*u));
}


/* THE ordered-ring room predicate. Both insertion kinds go through it, so an
 * injected "full" and a genuinely full ring take the identical branch. */
static bool
bind_ordered_room(moqr_bind_t *b, bool is_nsu)
{
#ifdef MOQR_BIND_TESTING
    if (atomic_load(&bind_dbg_ring_full_n) > 0 &&
        (atomic_load(&bind_dbg_ring_full_nsu) != 0) == is_nsu &&
        atomic_fetch_sub(&bind_dbg_ring_full_n, 1) == 1) {
        return false;
    }
#endif
    (void)is_nsu;
    return b->pending_count < b->pending_cap;
}


/* Append to the ordered ring. PRECONDITION: room was already admitted for this
 * insertion by exactly one bind_ordered_room() call. Admission is separate from
 * the append so that one insertion answers to one decision: a namespace
 * admitted as a namespace must never also be judged as a scalar. */
static void
bind_pending_append(moqr_bind_t *b, const moqr_intent_t *it)
{
    b->pending[(b->pending_head + b->pending_count) % b->pending_cap] = *it;
    b->pending_count++;
    if (b->pending_count > b->pending_high_water) {
        b->pending_high_water = b->pending_count;
    }
}


/* Scalar admission + append. Namespace insertions do NOT come through here:
 * they admit themselves as namespaces in bind_defer_push_nsu. */
static bool
bind_pending_push(moqr_bind_t *b, const moqr_intent_t *it)
{
    if (!bind_ordered_room(b, false)) {
        return false;
    }
    bind_pending_append(b, it);
    return true;
}


/* false => the intent could NOT join the ordered FIFO. Namespace updates now
 * share this ring, so "a scalar terminal always fits" is no longer a safe
 * assumption: a caller that ignores the failure would consume mandatory
 * ordered output while its target connection stays live. */
static bool
bind_defer_push(moqr_bind_t *b, const moqr_intent_t *it,
                 moqr_bind_meta_ref_t *pin)
{
    if (!bind_intent_deferrable(it->kind)) {
        b->pending_nonscalar_blocked++;
        return true;   /* contract violation, counted; not a ring overflow */
    }
    uint32_t slot = (b->pending_head + b->pending_count) % b->pending_cap;
    if (!bind_pending_push(b, it)) {
        b->session_errors++;
        return false;
    }
    if (pin != NULL && (it->kind == MOQR_INTENT_ACCEPT_SUB ||
                        it->kind == MOQR_INTENT_TRACK_STATUS_OK)) {
        b->pending_metadata[slot] = *pin;
        memset(pin, 0, sizeof(*pin));
    }
    return true;
}

/* Wire registries are distinct namespaces; the bind never lets one value
 * serve two of them.
 * - Data-stream reset CANCELLED: every draft assigns 0x1 — the stream was
 *   cancelled because the publisher ended the subscription, and PUBLISH_DONE
 *   carries the more detailed status.
 * - PUBLISH_DONE GOING_AWAY: every draft assigns 0x4 — the publisher issued
 *   a GOAWAY.
 * - REQUEST_ERROR INTERNAL_ERROR: every draft assigns 0x0 — the relay's own
 *   upstream request was terminated by a signal it cannot act on. */
#define BIND_RESET_CANCELLED   0x1u

#define BIND_DONE_GOING_AWAY   0x4u



/* The ordered ring is full for this intent: the connection it targets can no
 * longer be given output in order, so close it rather than continue a live
 * session that has lost mandatory ordered output. Other connections' retained
 * entries are untouched. */
static void
bind_ordered_overflow_close(moqr_bind_t *b, const moqr_intent_t *it,
                            uint64_t now_us)
{
    b_conn_t *cn = conn_by_cookie(b, it->binding_cookie);
    if (cn == NULL || cn->closed) {
        return;
    }
    b->ordered_failed_closed++;
    (void)moq_session_close(cn->session, BIND_SESSION_INTERNAL_ERROR,
                            "ordered output ring exhausted", now_us);
    conn_detach(b, cn, now_us);
}


/* Park a namespace update in the SAME ordered ring as every scalar terminal,
 * with its borrowed suffix deep-copied into the slot's payload sidecar. One
 * queue is what makes ordering exact: an update can neither overtake nor be
 * overtaken by control output queued around it.
 * false => the ring or the copy is exhausted; the caller must fail closed. */
static bool
bind_defer_push_nsu(moqr_bind_t *b, const moqr_intent_t *it)
{
    if (!bind_ordered_room(b, true)) {
        return false;
    }
    uint32_t slot = (b->pending_head + b->pending_count) % b->pending_cap;
    moqr_intent_t parked = *it;
    if (!bind_nsu_store(b, slot, &parked)) {
        return false;
    }
    bind_pending_append(b, &parked);   /* admitted above, as a namespace */
    return true;
}


/* Flush the core's outward intents to their sessions, durably. The ENTIRE core
 * ring is drained every pump: borrowed-payload views cannot survive to the next
 * pump (relay.h:102), so UPSTREAM_SUBSCRIBE is executed inline the moment it is
 * polled. A blocked NS_FOUND / NS_GONE instead parks in the SAME ordered ring
 * with its suffix deep-copied, so draft-18 Section 6.2's MUST survives without
 * letting it overtake or be overtaken. Once anything is blocked, every
 * subsequent parkable output queues behind it (preserving order). Returns true
 * if nothing is parked (caller may deliver), false otherwise (caller holds
 * delivery so no object overtakes a parked terminal). */
bool
bind_execute_intents(moqr_bind_t *b, uint64_t now_us)
{
    if (b->intent_drain_active) {
#ifdef MOQR_BIND_TESTING
        b->dbg_intent_retry_suppressed++;
#endif
        return false;
    }
    b->intent_drain_active = true;

    /* 1. Retry parked scalars first, in FIFO order. Stop retrying at the first
     *    that still blocks (nothing behind it may overtake it), but fall
     *    through to step 2 so borrowed intents newly in the ring are still
     *    consumed this pump. */
    bool blocked = false;
    while (b->pending_count > 0) {
        moqr_intent_t defer;
        moqr_intent_t current = b->pending[b->pending_head];
        moqr_bind_meta_ref_t pin = {0};
        (void)moqr_bind_meta_retain(b, &b->pending_metadata[b->pending_head],
                                  &pin);
        uint32_t epoch = b->pending_purge_epoch;
        bool done = bind_try_pinned(b, &current, now_us, BIND_TRY_FROM_RING,
                                    NULL, &defer, &pin);
        moqr_bind_meta_release(b, &pin);
        if (b->pending_purge_epoch != epoch) {
            /* The retry detached its own connection: purge already released
             * this slot and compacted the ring, so the head is now somebody
             * else's entry. Popping here would silently discard it. */
            continue;
        }
        if (!done) {
            b->pending[b->pending_head] = defer;
            if (defer.kind != MOQR_INTENT_ACCEPT_SUB &&
                defer.kind != MOQR_INTENT_TRACK_STATUS_OK) {
                moqr_bind_meta_release(b, &b->pending_metadata[b->pending_head]);
            }
            blocked = true;
            break;
        }
        bind_nsu_release(b, b->pending_head);   /* no-op unless it held bytes */
        moqr_bind_meta_release(b, &b->pending_metadata[b->pending_head]);
        b->pending_head = (b->pending_head + 1u) % b->pending_cap;
        b->pending_count--;
    }
    blocked = blocked || b->pending_count > 0;
    /* 2. Drain the whole ring. Consume-in-pump borrowed intents
     *    (UPSTREAM_SUBSCRIBE) execute inline and are never parked; NS_FOUND /
     *    NS_GONE are borrowed too but DO park, after their suffix is
     *    deep-copied into the slot's sidecar. Both those and scalar intents
     *    park once anything is blocked (order), else execute and park only if
     *    they themselves block. */
    size_t n;
    while ((n = moqr_core_poll_intents(b->core, b->pump_intents,
                                       BIND_PUMP_INTENT_BATCH)) > 0) {
        for (size_t i = 0; i < n; i++) {
            moqr_intent_t *it = &b->pump_intents[i];
            bool is_nsu = it->kind == MOQR_INTENT_NS_FOUND ||
                          it->kind == MOQR_INTENT_NS_GONE;
            if (!bind_intent_deferrable(it->kind) && !is_nsu) {
                /* Router-only / consume-in-pump kinds (UPSTREAM_SUBSCRIBE):
                 * they own no per-session output order to preserve. */
                moqr_intent_t tw;
                (void)bind_try_intent(b, it, now_us, BIND_TRY_FRESH, NULL,
                                      &tw);
                continue;
            }
            moqr_bind_meta_ref_t pin = {0};
            bind_meta_prepare_reply(b, it, &pin);
            if (blocked) {
                /* Anything ordered joins the one FIFO with NO inline send:
                 * an attempt here could reach the wire ahead of what is
                 * already parked. */
                if (is_nsu) {
                    moqr_intent_t tw;
                    (void)bind_try_intent(b, it, now_us,
                                          BIND_TRY_FORCE_PARK, NULL, &tw);
                } else if (!bind_defer_push(b, it, &pin)) {
                    bind_ordered_overflow_close(b, it, now_us);
                }
                moqr_bind_meta_release(b, &pin);
                continue;
            }
            moqr_intent_t defer;
            bool parked = false;
            if (!bind_try_pinned(b, it, now_us, BIND_TRY_FRESH, &parked,
                                 &defer, &pin)) {
                if (!bind_defer_push(b, &defer, &pin)) {
                    bind_ordered_overflow_close(b, &defer, now_us);
                }
                blocked = true;
            } else if (parked) {
                /* A namespace update just joined the FIFO: everything polled
                 * after it must queue behind it, not race it. */
                blocked = true;
            }
            moqr_bind_meta_release(b, &pin);
        }
    }
    b->intent_drain_active = false;
    return !blocked;
}


/* Drop parked outputs that have become moot, keeping every other entry in
 * order: all of a connection's (ns_sub_only false), or only the NAMESPACE
 * updates for one of its namespace subscriptions (ns_sub_only, ns_cookie). */
void
bind_pending_purge(moqr_bind_t *b, uint64_t binding_cookie, bool ns_sub_only,
                   uint64_t ns_cookie)
{
    uint32_t kept = 0;
    uint32_t scanned = b->pending_count;
    for (uint32_t i = 0; i < scanned; i++) {
        uint32_t from = (b->pending_head + i) % b->pending_cap;
        moqr_intent_t *src = &b->pending[from];
        bool moot = src->binding_cookie == binding_cookie &&
                    (!ns_sub_only ||
                     ((src->kind == MOQR_INTENT_NS_FOUND ||
                       src->kind == MOQR_INTENT_NS_GONE) &&
                      src->cookie == ns_cookie));
        if (moot) {
            bind_nsu_release(b, from);   /* frees any retained namespace bytes */
            moqr_bind_meta_release(b, &b->pending_metadata[from]);
            continue;
        }
        uint32_t to = (b->pending_head + kept) % b->pending_cap;
        if (to != from) {
            /* The payload sidecar is keyed by slot, so it moves with the
             * entry and the parked view is re-pointed at its new home. */
            /* The sidecar bookkeeping moves with the entry; the byte
             * allocation itself does not, so the parked part pointers stay
             * valid without fixup. */
            bind_nsu_release(b, to);
            b->nsu[to] = b->nsu[from];
            memset(&b->nsu[from], 0, sizeof(b->nsu[from]));
            moqr_bind_meta_release(b, &b->pending_metadata[to]);
            b->pending_metadata[to] = b->pending_metadata[from];
            memset(&b->pending_metadata[from], 0,
                   sizeof(b->pending_metadata[from]));
            b->pending[to] = *src;
        }
        kept++;
    }
    if (kept != scanned) {
        b->pending_purge_epoch++;   /* the ring was rewritten under any caller */
    }
    b->pending_count = kept;
}


/* Drop any parked outputs targeting a connection that is tearing down: its
 * session is about to go away, so the writes are moot. Called at close, before
 * the slot (and its binding cookie) can be reused by a new connection. */
void
bind_pending_purge_conn(moqr_bind_t *b, uint64_t binding_cookie)
{
    bind_pending_purge(b, binding_cookie, false, 0);
}


bool
bind_retry_may_drain_intents(moqr_bind_t *b)
{
    if (b->intent_drain_active) {
#ifdef MOQR_BIND_TESTING
        b->dbg_intent_retry_suppressed++;
#endif
        return false;
    }
    return true;
}


b_dsub_t *dsub_find(const moqr_bind_t *b, b_conn_t *cn, uint64_t sub_raw);

int dsub_free_slot(const moqr_bind_t *b, const b_conn_t *cn);


/* -- intent execution --------------------------------------------------------- */

b_dsub_t *
dsub_find(const moqr_bind_t *b, b_conn_t *cn, uint64_t sub_raw)
{
    for (uint32_t i = 0; i < b->n_dsubs; i++) {
        if (cn->dsubs[i].used && cn->dsubs[i].sub_raw == sub_raw) {
            return &cn->dsubs[i];
        }
    }
    return NULL;
}


/* Whether the session has already ended this downstream request on its own
 * (an update it refused automatically ends it with UPDATE_FAILED and names
 * it in no event). Asked of the request itself -- the parent, by its exact
 * session handle -- through the non-advancing owed-update observer with no
 * outputs: STALE_HANDLE is the session's word that the request is over
 * (freed, or its terminal held behind resets it owes). OK (an answer owed)
 * and WRONG_STATE (none owed) are both a live request. A stream's stale
 * handle is never evidence: a single subgroup can end on its own (a delivery
 * timeout, the peer's STOP_SENDING) while its request lives on. */
bool
bind_request_ended_by_session(const b_conn_t *cn, const b_dsub_t *ds)
{
    moq_result_t rc = ds->publication
        ? moq_session_publication_owed_update_sized(
              cn->session, (moq_publication_t){ ._opaque = ds->sub_raw },
              NULL, NULL, NULL, 0)
        : moq_session_subscription_owed_update_sized(
              cn->session, (moq_subscription_t){ ._opaque = ds->sub_raw },
              NULL, NULL, NULL, 0);
    return rc == MOQ_ERR_STALE_HANDLE;
}


uint64_t
bind_stream_count_next(uint64_t count, uint8_t wire_draft)
{
    const uint64_t unknown = wire_draft == B_DRAFT_22
                                 ? UINT64_MAX : (UINT64_C(1) << 62) - 1;
    return count < unknown ? count + 1 : unknown;
}


int
dsub_free_slot(const moqr_bind_t *b, const b_conn_t *cn)
{
    for (uint32_t i = 0; i < b->n_dsubs; i++) {
        if (!cn->dsubs[i].used) {
            return (int)i;
        }
    }
    return -1;
}


static int
usub_free_slot(const moqr_bind_t *b, const b_conn_t *cn)
{
    for (uint32_t i = 0; i < b->n_usubs; i++) {
        if (!cn->usubs[i].used) {
            return (int)i;
        }
    }
    return -1;
}


static int
pub_free_slot(const moqr_bind_t *b, const b_conn_t *cn)
{
    for (uint32_t i = 0; i < b->n_pubs; i++) {
        if (!cn->pubs[i].used) {
            return (int)i;
        }
    }
    return -1;
}


/* The ACK did not commit. Retire only its core subscription and preserve the
 * request-local refusal in the same FIFO if the session cannot take it yet.
 * Converting to REJECT_SUB also lets the caller release the old ACK pin. */
static bool
bind_refuse_unaccepted_sub(moqr_bind_t *b, b_conn_t *cn,
                           const moqr_intent_t *it, bool capacity,
                           uint64_t now_us, moqr_intent_t *defer)
{
    (void)moqr_core_unsubscribe(b->core, it->sub, now_us);
    moq_subscription_t ssub = { it->cookie };
    moq_reject_subscribe_cfg_t cfg;
    if (capacity) {
        /* The tracking table is a bounded pool: a capacity refusal. */
        bind_reject_subscribe_cfg_init(&cfg, bind_capacity_error(cn));
        cfg.can_retry = true;
        cfg.retry_after_ms = bind_capacity_retry_ms(it->cookie);
    } else {
        bind_reject_subscribe_cfg_init(&cfg, MOQ_REQUEST_ERROR_INTERNAL_ERROR);
    }
    moq_result_t rc = moq_session_reject_subscribe(cn->session, ssub, &cfg, now_us);
    if (rc == MOQ_ERR_WOULD_BLOCK) {
        *defer = *it;
        defer->kind = MOQR_INTENT_REJECT_SUB;
        defer->error_code = capacity ? MOQR_REQUEST_ERROR_CAPACITY
                                     : MOQ_REQUEST_ERROR_INTERNAL_ERROR;
        return false;
    }
    if (rc != MOQ_OK && !fwd_result_is_gone(rc)) b->session_errors++;
    return true;
}


bool
bind_try_intent(moqr_bind_t *b, const moqr_intent_t *it, uint64_t now_us,
                uint8_t mode, bool *parked_out, moqr_intent_t *defer)
{
    /* Reserved-cookie intents belong to the shard manager's pseudo-bindings,
     * not a real session: hand them to the router. Without a router installed
     * (the single-shard default) this is skipped and the reserved cookie is
     * moot exactly as an unknown cookie is below. */
    if (b->router != NULL && it->binding_cookie >= b->router_cookie_base) {
        if (b->router(b->router_ctx, it, now_us)) {
            return true; /* the router consumed it */
        }
        if (bind_intent_deferrable(it->kind)) {
            *defer = *it; /* retry next pump, like a WOULD_BLOCKed write */
            return false;
        }
        return true; /* declined borrowed-view kind: moot, as an unrouted cookie */
    }
    b_conn_t *cn = conn_by_cookie(b, it->binding_cookie);
    if (cn == NULL || cn->closed) {
        return true; /* moot: the target session is gone or tearing down */
    }
    if (cn->events_pending && bind_intent_deferrable(it->kind)) {
        *defer = *it;
        return false;
    }
    switch (it->kind) {
    case MOQR_INTENT_ACCEPT_SUB: {
        if (bind_cookie_is_publication(it->cookie)) {
            /* A discovery publication: the subscriber's PUBLISH_OK was the
             * acceptance, so nothing goes on the wire. */
            b_dsub_t *pds = dsub_find(b, cn, it->cookie);
            if (pds == NULL || !pds->publication) {
                (void)moqr_core_unsubscribe(b->core, it->sub, now_us);
            } else {
                pds->rsub = it->sub;
            }
            return true;
        }
        moq_subscription_t ssub = { it->cookie };
        const moqr_bind_meta_ref_t *pin = b->metadata_executing;
        /* Reserve the tracking slot BEFORE accepting on the wire: a sub
         * the binding cannot track must be refused, never orphaned. */
        int slot = dsub_free_slot(b, cn);
        b_props_tmp_t ptmp = { NULL, 0 };
        moq_bytes_t pout = { NULL, 0 };
        if (slot < 0 ||
            !bind_meta_out(b, pin, moq_session_version(cn->session), &pout, &ptmp)) {
            /* An ownership/representation limit, not an unsupported request
             * type (D22 Section 12.3). */
            return bind_refuse_unaccepted_sub(b, cn, it, slot < 0, now_us,
                                              defer);
        }
        moq_accept_subscribe_cfg_t cfg;
        moq_accept_subscribe_cfg_init(&cfg);
        cfg.track_properties = pout;
        cfg.has_largest = it->has_largest;
        cfg.largest_group = it->largest_group;
        cfg.largest_object = it->largest_object;
        moq_result_t arc =
            moq_session_accept_subscribe(cn->session, ssub, &cfg, now_us);
        bind_track_props_release(b, &ptmp);
        if (arc == MOQ_ERR_WOULD_BLOCK) {
            *defer = *it; /* slot NOT committed; the retry re-reserves */
            return false;
        }
        if (arc == MOQ_ERR_BUFFER) {
            /* Encoding/preflight could not fit the ACK, without committing
             * the session request. Do not abandon it, or classify unrelated
             * stale/closed/invalid failures as metadata capacity. */
            return bind_refuse_unaccepted_sub(b, cn, it, false, now_us, defer);
        }
        if (arc != MOQ_OK) {
            (void)moqr_core_unsubscribe(b->core, it->sub, now_us);
            b->session_errors++;
            return true;
        }
        memset(&cn->dsubs[slot], 0, sizeof(cn->dsubs[slot]));
        cn->dsubs[slot].used = true;
        cn->dsubs[slot].sub_raw = it->cookie;
        cn->dsubs[slot].rsub = it->sub;
        uint64_t source_gen = 0;
        cn->dsubs[slot].has_ident =
            moqr_core_sub_track(b->core, it->sub, &cn->dsubs[slot].ident_track,
                                &source_gen) == MOQR_OK;
        return true;
    }
    case MOQR_INTENT_REJECT_SUB: {
        if (bind_cookie_is_publication(it->cookie)) {
            /* The track a discovery publication offered has no source any
             * more: the publication ends as its track did. */
            moqr_intent_t done = *it;
            done.kind = MOQR_INTENT_SUB_DONE;
            done.pd = bind_local_pd(MOQR_PD_TRACK_ENDED);
            return bind_try_intent(b, &done, now_us, mode, parked_out, defer);
        }
        moq_subscription_t ssub = { it->cookie };
        moq_reject_subscribe_cfg_t cfg;
        if (it->error_code == MOQR_REQUEST_ERROR_CAPACITY) {
            bind_reject_subscribe_cfg_init(&cfg, bind_capacity_error(cn));
            cfg.can_retry = true;
            cfg.retry_after_ms = bind_capacity_retry_ms(it->cookie);
        } else {
            bind_reject_subscribe_cfg_init(&cfg, it->error_code);
        }
        if (moq_session_reject_subscribe(cn->session, ssub, &cfg, now_us) ==
            MOQ_ERR_WOULD_BLOCK) {
            *defer = *it;
            return false;
        }
        return true;
    }
    case MOQR_INTENT_SUB_DONE: {
        /* An update answer owed on the subscription goes first (the session
         * refuses the terminal while one is owed). A refusal given now is
         * itself the subscription's terminal (UPDATE_FAILED), which makes
         * this one moot. */
        b_upd_give_t settle = bind_update_answer_settle(b, cn, it->cookie,
                                                        now_us);
        if (settle == B_UPD_HELD) {
            *defer = *it;
            return false;
        }
        if (settle == B_UPD_ENDED) {
            return true;
        }
        b_dsub_t *ds = dsub_find(b, cn, it->cookie);
        if (ds == NULL) {
            /* Only a successful ACCEPT creates this owner. An unaccepted
             * or already-retired request has no binding-owned completion;
             * never send a guessed zero count for an ownerless intent. */
            return true;
        }
        if (bind_request_ended_by_session(cn, ds)) {
            /* The session already ended this request (and owes or sent its
             * own terminal): retire the binding's side only. */
            bind_dsub_retire(b, cn, ds, now_us);
            return true;
        }
        /* Close any open subgroup streams for this sub, THEN send the
         * terminal. A stream still open here never received its close: the
         * core delivers a subgroup's FIN (or upstream reset) as a seal notice
         * before the subscription's terminal, and that close frees the slot.
         * So nothing proves the subgroup was delivered whole -- its upstream
         * may never have been sealed (the done wait expired) or the
         * subscription ended early -- and it is reset, never FINed (draft-18
         * section 11.4.3, draft-22 section 11.3.2: a sender that closes the
         * stream before delivering all of its objects MUST reset it). Defer
         * the whole SUB_DONE if any write blocks: tracking is left intact so
         * the retry re-closes (already-closed slots are cleared, so it is
         * idempotent) and re-sends DONE. Downstream tracking is torn down
         * only once the terminal is actually on the wire. */
        for (uint32_t i = 0; i < b->n_sgs; i++) {
            if (!cn->sgs[i].used || cn->sgs[i].sub_raw != it->cookie) {
                continue;
            }
            if (cn->sgs[i].stopped) {
                memset(&cn->sgs[i], 0, sizeof(cn->sgs[i]));
                bind_dl_rearm_on_slot_release(b, cn);
                continue;
            }
            moq_result_t crc;
#ifdef MOQR_BIND_TESTING
            if (atomic_load(&bind_dbg_block_sg_resets) > 0) {
                /* Injected WOULD_BLOCK before the session is called: the
                 * reset is genuinely unsent. */
                (void)atomic_fetch_sub(&bind_dbg_block_sg_resets, 1);
                *defer = *it;
                return false;
            }
#endif
            if (cn->sgs[i].reset_owed) {
                crc = moq_session_reset_subgroup(cn->session, cn->sgs[i].h,
                                                 cn->sgs[i].reset_wire, now_us);
            } else {
                /* Reset with the reset registry's CANCELLED (draft-18 section
                 * 3.3.3, draft-22 section 12.5: the stream was cancelled, and
                 * for a subscription PUBLISH_DONE carries the detailed status)
                 * -- the SUB_DONE's status code lives in the PUBLISH_DONE
                 * registry and never doubles as a reset code. This is also the
                 * only terminal a begun object can take: close_subgroup
                 * returns WRONG_STATE on a streaming subgroup. */
#ifdef MOQR_BIND_TESTING
                if (atomic_load(&bind_dbg_fail_sg_resets) > 0 &&
                    atomic_fetch_sub(&bind_dbg_fail_sg_resets, 1) == 1) {
                    /* Injected hard failure: the session is NOT called, so
                     * the production fail-closed path runs unchanged. */
                    crc = MOQ_ERR_WRONG_STATE;
                } else
#endif
                crc = moq_session_reset_subgroup(cn->session, cn->sgs[i].h,
                                                 BIND_RESET_CANCELLED,
                                                 now_us);
            }
            if (crc == MOQ_ERR_WOULD_BLOCK) {
                *defer = *it;
                return false;
            }
            /* Every one of these resets is owed: a refusal leaves the stream
             * without its terminal, so fail closed rather than strand it and
             * release the subscription. The one tolerated result is
             * STALE_HANDLE, which the session returns only when it already
             * owns this stream's termination: either the entry was freed
             * (its generation moved on: the stream ended and was reaped), or
             * the same generation is no longer OPEN or STREAMING -- CLOSING
             * (its FIN is queued), RESETTING (a reset is queued, e.g. the
             * session's delivery timeout or its answer to the peer's
             * STOP_SENDING) or RESET_OWED (a teardown's reset the session
             * itself issues once its action queue has room). The binding is
             * not told of those session-side terminations, so its slot can
             * outlive them; there is nothing left for it to send. */
            if (crc != MOQ_OK && crc != MOQ_ERR_STALE_HANDLE) {
                b->session_errors++;
                (void)moq_session_close(cn->session,
                                        BIND_SESSION_INTERNAL_ERROR,
                                        "sub_done reset", now_us);
                conn_detach(b, cn, now_us);
                return true;
            }
            cn->sgs[i].used = false;
            cn->sgs[i].obj_begun = false;
            cn->sgs[i].reset_owed = false;
            bind_dl_rearm_on_slot_release(b, cn);   /* freed slot: unblock a waiting delivery */
        }
        moq_subscription_t ssub = { it->cookie };
        moq_done_subscribe_cfg_t cfg;
        moq_done_subscribe_cfg_init(&cfg);
        cfg.stream_count = ds->streams_opened;
        /* The retained terminal is encoded for the draft this subscriber
         * reads — the single translation point for the PUBLISH_DONE domain. */
        uint64_t pd_wire = 0;
        if (moqr_pd_desc_emit(it->pd, moqr_bind_conn_version(b, cn->session),
                              &pd_wire) != MOQR_OK) {
            (void)moq_session_close(cn->session, BIND_SESSION_INTERNAL_ERROR,
                                    "untranslatable terminal", now_us);
            conn_detach(b, cn, now_us);
            return false;
        }
        cfg.status_code = pd_wire;   /* 64-bit: an extension may exceed 32 bits */
        moq_result_t drc;
#ifdef MOQR_BIND_TESTING
        int fail_kind = atomic_load(&bind_dbg_fail_sub_done_kind);
        bool fail_match = fail_kind == 0 ||
                          (fail_kind == 1 && !ds->publication) ||
                          (fail_kind == 2 && ds->publication);
        if (atomic_load(&bind_dbg_block_sub_dones) > 0) {
            /* Injected block: the session is NOT called, so the terminal is
             * genuinely unsent and the retry must produce it. */
            (void)atomic_fetch_sub(&bind_dbg_block_sub_dones, 1);
            drc = MOQ_ERR_WOULD_BLOCK;
        } else if (fail_match && atomic_load(&bind_dbg_fail_sub_dones) > 0 &&
                   atomic_fetch_sub(&bind_dbg_fail_sub_dones, 1) == 1) {
            /* Injected hard refusal: nothing queued, nothing sent. */
            drc = MOQ_ERR_WRONG_STATE;
        } else
#endif
        if (ds->publication) {
            moq_finish_publish_cfg_t fc;
            moq_finish_publish_cfg_init(&fc);
            fc.status_code = pd_wire;
            fc.stream_count = ds->streams_opened;
            drc = moq_session_finish_publish(
                cn->session, (moq_publication_t){ ._opaque = it->cookie }, &fc,
                now_us);
        } else {
            drc = moq_session_done_subscribe(cn->session, ssub, &cfg, now_us);
        }
        if (drc == MOQ_ERR_WOULD_BLOCK) {
            /* No room yet (action queue, encode space or drain slot): the
             * session queued nothing. Retry with tracking intact. */
            *defer = *it;
            return false;
        }
        if (drc == MOQ_ERR_STALE_HANDLE) {
            /* The session already ended this request itself, without telling
             * the binding (an update it refused on its own ends the request
             * with UPDATE_FAILED; no event names it). Its terminal -- and any
             * resets it still owes before that -- are the session's to send,
             * so the binding only retires its own owner, already validated
             * above. Stale is not proof of delivery. */
            bind_dsub_release(b, cn, ds);
            return true;
        }
        if (drc != MOQ_OK) {
            /* A hard refusal also queued nothing (WRONG_STATE, BUFFER: the
             * encoded terminal can never fit, INVAL, ...), and no retry can
             * deliver it. Releasing the owner here would strand the request
             * with no terminal, so end it the way every unrecoverable binding
             * fault does: close the session. */
            b->session_errors++;
            (void)moq_session_close(cn->session, BIND_SESSION_INTERNAL_ERROR,
                                    "terminal refused", now_us);
            conn_detach(b, cn, now_us);
            return true;
        }
        /* Accepted: the terminal is queued, so the owner is released. */
        bind_dsub_release(b, cn, ds);
        return true;
    }
    case MOQR_INTENT_UPSTREAM_SUBSCRIBE: {
        /* Borrowed-payload (ns_parts/name); NOT deferrable — a blocked
         * subscribe fails closed to upstream_error, whose downstream REJECT_SUB
         * IS durable, so the requester still gets a terminal. Making the
         * upstream subscribe itself retryable is a later refinement. */
        int slot = usub_free_slot(b, cn);
        if (slot < 0) {
            /* The upstream table is a bounded pool: the parked downstream
             * requests get a capacity refusal. */
            (void)moqr_core_upstream_error(
                b->core, it->track, it->track_gen,
                MOQR_REQUEST_ERROR_CAPACITY, now_us);
            return true;
        }
        moq_subscribe_cfg_t cfg;
        moq_subscribe_cfg_init(&cfg);
        cfg.track_namespace = (moq_namespace_t){ .parts = it->ns_parts,
                                                 .count = it->ns_count };
        cfg.track_name = it->name;
        cfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
        moq_subscription_t ssub;
        if (moq_session_subscribe(cn->session, &cfg, now_us, &ssub) ==
            MOQ_OK) {
            usub_release(b, &cn->usubs[slot]);   /* a clean slot */
            cn->usubs[slot].used = true;
            cn->usubs[slot].track = it->track;
            cn->usubs[slot].track_gen = it->track_gen;
            cn->usubs[slot].ssub = ssub;
            usub_store_key(b, &cn->usubs[slot], it->ns_parts, it->ns_count,
                           it->name);
        } else {
            (void)moqr_core_upstream_error(
                b->core, it->track, it->track_gen,
                MOQ_REQUEST_ERROR_INTERNAL_ERROR, now_us);
        }
        return true;
    }
    case MOQR_INTENT_UPSTREAM_UNSUBSCRIBE:
        /* Retire only this source generation. A replacement can already own
         * the same retained track when this queued intent reaches the bind. */
        for (uint32_t i = 0; i < b->n_usubs; i++) {
            b_usub_t *us = &cn->usubs[i];
            if (!us->used || !moqr_track_eq(us->track, it->track) ||
                us->track_gen != it->track_gen) continue;
            if (bind_cancel_settled(b, cn, bind_cancel_upstream_sub(cn, us->ssub, now_us),
                                    "obsolete source cancellation failed", now_us)) {
                usub_release(b, us);
            } else {
                us->cancel_owed = true;
                cn->retry_pending |= B_RETRY_CANCEL;
            }
        }
        for (uint32_t i = 0; i < b->n_pubs; i++) {
            b_pub_t *bp = &cn->pubs[i];
            if (!bp->used || !moqr_track_eq(bp->track, it->track) ||
                bp->track_gen != it->track_gen) continue;
            moq_publication_t pub = {bp->pub_raw};
            uint64_t code = bp->cancel_code;
            if (!bp->cancel_owed)
                (void)moqr_reset_encode(moqr_bind_conn_version(b, cn->session),
                                        MOQR_RESET_CANCELLED, &code);
            b_meta_publish_t *mp = bind_meta_pub_find(b, cn, pub._opaque);
            if (mp != NULL) bind_meta_pub_release(b, mp);
            if (bind_cancel_settled(b, cn, bind_cancel_upstream_pub(cn, pub, code, now_us),
                                    "obsolete source cancellation failed", now_us)) {
                memset(bp, 0, sizeof(*bp));
            } else {
                bp->cancel_owed = true;
                bp->cancel_code = code;
                cn->retry_pending |= B_RETRY_CANCEL;
            }
        }
        return true;
    case MOQR_INTENT_ACCEPT_PUBLISH: {
        moq_publication_t pub = { it->cookie };
        /* Reserve tracking BEFORE accepting: an untracked publish would
         * silently drop every object it pushes. On refusal, unwind the
         * core's track (upstream lost) and reject on the wire (durable, as
         * with ACCEPT_SUB's no-slot path). */
        int slot = pub_free_slot(b, cn);
        if (slot < 0) {
            (void)moqr_core_upstream_lost(b->core, it->track, it->track_gen);
            moq_reject_publish_cfg_t rj;
            moq_reject_publish_cfg_init(&rj);
            rj.error_code = bind_capacity_error(cn);   /* bounded pool */
            rj.can_retry = true;
            rj.retry_after_ms = bind_capacity_retry_ms(it->cookie);
            if (moq_session_reject_publish(cn->session, pub, &rj, now_us) ==
                MOQ_ERR_WOULD_BLOCK) {
                *defer = *it;
                defer->kind = MOQR_INTENT_REJECT_PUBLISH;
                defer->error_code = MOQR_REQUEST_ERROR_CAPACITY;
                return false;
            }
            return true;
        }
        moq_accept_publish_cfg_t cfg;
        moq_accept_publish_cfg_init(&cfg);
        moq_result_t prc =
            moq_session_accept_publish(cn->session, pub, &cfg, now_us);
        if (prc == MOQ_ERR_WOULD_BLOCK) {
            *defer = *it;
            return false;
        }
        if (prc != MOQ_OK) {
            (void)moqr_core_upstream_lost(b->core, it->track, it->track_gen);
            b->session_errors++;
            return true;
        }
        cn->pubs[slot].used = true;
        cn->pubs[slot].pub_raw = it->cookie;
        cn->pubs[slot].track = it->track;
        cn->pubs[slot].track_gen = it->track_gen;
        return true;
    }
    case MOQR_INTENT_REJECT_PUBLISH: {
        moq_publication_t pub = { it->cookie };
        moq_reject_publish_cfg_t cfg;
        moq_reject_publish_cfg_init(&cfg);
        if (it->error_code == MOQR_REQUEST_ERROR_CAPACITY) {
            cfg.error_code = bind_capacity_error(cn);
            cfg.can_retry = true;
            cfg.retry_after_ms = bind_capacity_retry_ms(it->cookie);
        } else {
            cfg.error_code = it->error_code;
        }
        if (moq_session_reject_publish(cn->session, pub, &cfg, now_us) ==
            MOQ_ERR_WOULD_BLOCK) {
            *defer = *it;
            return false;
        }
        return true;
    }
    case MOQR_INTENT_NS_FOUND:
    case MOQR_INTENT_NS_GONE: {
        /* Borrowed-payload (ns_parts), so the intent itself can never be
         * parked. draft-18 Section 6.2 owes this NAMESPACE to the matching
         * prefix subscriber as a MUST, so a blocked write is retained by
         * deep copy (bind_nsu_retain) and replayed, never dropped. */
        moq_ns_sub_handle_t h = { it->cookie };
        bool is_gone = it->kind == MOQR_INTENT_NS_GONE;
        moqr_ns_t suffix_ns = {
            .parts = it->ns_parts + it->value,
            .count = it->ns_count - (size_t)it->value,
        };
        /* The ROOT namespace -- zero ANNOUNCED fields -- has no representation
         * on a peer whose negotiated draft requires at least one, so sending it
         * would put an invalid namespace on that wire. The test is the announced
         * cardinality, not an empty suffix: a non-root namespace equal to its
         * prefix also yields a zero-length suffix and is unaffected. Ask the
         * CAPABILITY, never a draft number, and drop the update before the send
         * and before any retain/owed accounting, so it is never queued,
         * retained or counted as owed. FOUND and GONE are suppressed
         * symmetrically, so the peer is never left holding half a pair; its own
         * namespace subscription, including a legal empty prefix, is
         * untouched. */
        if (it->ns_count == 0 &&
            !moq_session_supports_zero_field_track_namespace(cn->session)) {
            return true;   /* consumed: nothing sent, nothing owed */
        }
        /* No NAMESPACE before the subscription's REQUEST_OK: while its
         * acceptance is held, the update waits in order like a blocked one. */
        if (mode != BIND_TRY_FORCE_PARK &&
            !bind_ns_sub_accept_held(b, cn, it->cookie)) {
            moq_namespace_t suffix = { .parts = suffix_ns.parts,
                                       .count = suffix_ns.count };
            moq_result_t rc = b->cluster_info.enabled
                ? bind_cluster_namespace(b, cn, it, now_us)
                : is_gone
                    ? moq_session_send_namespace_done(cn->session, h, &suffix,
                                                      now_us)
                    : moq_session_send_namespace(cn->session, h, &suffix,
                                                 now_us);
            if (rc == MOQ_OK) {
                return true;   /* healthy: nothing allocated, nothing parked */
            }
            if (rc == MOQ_ERR_STALE_HANDLE) {
                /* The session has retired the subscription (the peer reset
                 * or stopped its stream; the session reports no event for
                 * that). Nothing is owed to it: drop it from the core so it
                 * stops taking updates and a namespace-subscription slot,
                 * and drop its other parked updates. A purge that removes
                 * this very entry from the ring is noticed by the ring
                 * retry through the purge epoch. */
                (void)moqr_core_ns_unsubscribe(b->core, cn->binding,
                                               it->cookie);
                bind_pending_purge(b, (uint64_t)(cn - b->conns) + 1u, true,
                                   it->cookie);
                return true;
            }
            if (rc != MOQ_ERR_WOULD_BLOCK) {
                /* A hard send failure loses an update the spec says MUST be
                 * delivered. Counting it is telemetry, not enforcement: the
                 * subscriber that can no longer be told the truth is closed
                 * on the same lifecycle-safe path capacity exhaustion uses. */
                b->session_errors++;
                b->nsu_failed_closed++;
                (void)moq_session_close(cn->session,
                                        BIND_SESSION_INTERNAL_ERROR,
                                        "namespace update failed", now_us);
                conn_detach(b, cn, now_us);
                return true;
            }
        }
        if (mode == BIND_TRY_FROM_RING) {
            /* Already parked, and its payload already bind-owned: keep the
             * slot exactly where it is so nothing overtakes it. */
            *defer = *it;
            return false;
        }
        if (!bind_defer_push_nsu(b, it)) {
            /* Held state exhausted. Failing closed on the subscriber that can
             * no longer be told the truth is the only honest option: silently
             * continuing would drop a MUST. Other connections' parked output
             * is untouched. */
            b->nsu_failed_closed++;
            (void)moq_session_close(cn->session, BIND_SESSION_INTERNAL_ERROR,
                                    "namespace update undeliverable", now_us);
            conn_detach(b, cn, now_us);
            return true;
        }
        /* Parked in the one FIFO: the executor must now treat the bind as
         * blocked so nothing polled after this can be attempted ahead of it. */
        if (parked_out != NULL) {
            *parked_out = true;
        }
        return true;
    }
    case MOQR_INTENT_TRACK_STATUS_OK: {
        moq_track_status_handle_t h = { it->cookie };
        const moqr_bind_meta_ref_t *pin = b->metadata_executing;
        /* Draft-16's answer (a REQUEST_OK) has no field for them at all. */
        const bool stated = moq_session_version(cn->session) != MOQ_VERSION_DRAFT_16;
        b_props_tmp_t ptmp = { NULL, 0 };
        moq_bytes_t pout = { NULL, 0 };
        if (stated &&
            !bind_meta_out(b, pin, moq_session_version(cn->session), &pout, &ptmp)) {
            /* The track's properties cannot be stated on this session, so
             * the status is refused rather than answered without them, as an
             * ACK would be (§7.7). */
            moqr_intent_t refuse = *it;
            refuse.kind = MOQR_INTENT_TRACK_STATUS_ERROR;
            refuse.error_code = MOQ_REQUEST_ERROR_INTERNAL_ERROR;
            return bind_try_intent(b, &refuse, now_us, mode, parked_out, defer);
        }
        moq_accept_track_status_cfg_t cfg;
        moq_accept_track_status_cfg_init(&cfg);
        cfg.has_largest = it->has_largest;
        cfg.largest_group = it->largest_group;
        cfg.largest_object = it->largest_object;
        if (stated) cfg.track_properties = pout;
        moq_result_t trc =
            moq_session_accept_track_status(cn->session, h, &cfg, now_us);
        bind_track_props_release(b, &ptmp);
        if (trc == MOQ_ERR_WOULD_BLOCK) {
            *defer = *it;
            return false;
        }
        if (trc != MOQ_OK) {
            b->session_errors++;
        }
        return true;
    }
    case MOQR_INTENT_TRACK_STATUS_ERROR: {
        moq_track_status_handle_t h = { it->cookie };
        moq_reject_track_status_cfg_t cfg;
        moq_reject_track_status_cfg_init(&cfg);
        cfg.error_code = it->error_code;
        if (moq_session_reject_track_status(cn->session, h, &cfg, now_us) ==
            MOQ_ERR_WOULD_BLOCK) {
            *defer = *it;
            return false;
        }
        return true;
    }
    default:
        return true;
    }
}
