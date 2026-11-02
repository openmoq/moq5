#include "bind_internal.h"
#include "upstream_internal.h"
#include "delivery_internal.h"
#include "fetch_internal.h"
#include "fetch_fill_internal.h"
#include "fetch_joining_internal.h"
#include "intents_internal.h"
#include "metadata_internal.h"
#include "request_answers_internal.h"
#include "testing_internal.h"

void bind_dup_owners_release(moqr_bind_t *b, const b_conn_t *cn);

void usub_store_key(moqr_bind_t *b, b_usub_t *us, const moq_bytes_t *parts,
                    uint32_t ns_count, moq_bytes_t name);

void usub_release(moqr_bind_t *b, b_usub_t *us);

void usub_release_all(moqr_bind_t *b, b_conn_t *cn);


/* The PUBLISH_DONE status an ACTIVE upstream subscription ends with. The relay
 * neither re-issues a request nor follows a Redirect, so what it owes its
 * downstream subscribers is a truthful terminal for its OWN state:
 *
 *   GOAWAY   -> GOING_AWAY: the upstream really is departing (0x4 in both
 *               registries, d16 Section 13.4.3 / d18 Section 15.10.3);
 *   REDIRECT -> TRACK_ENDED: nobody is departing. This relay will not serve
 *               the track from the named location, so the track has ended as
 *               far as these subscribers are concerned (0x2 in both). Saying
 *               GOING_AWAY here would assert a departure that is not
 *               happening. */
static moqr_pd_status_t
bind_upstream_end_pd(bind_upstream_end_t how)
{
    switch (how) {
    case BIND_UP_END_GOAWAY:
        return MOQR_PD_GOING_AWAY;
    case BIND_UP_END_REDIRECT:
        return MOQR_PD_TRACK_ENDED;
    }
    return MOQR_PD_INTERNAL_ERROR;
}


/* The REQUEST_ERROR a PENDING upstream's parked subscribers are rejected with.
 *
 * TIMEOUT (0x2) is not available to either cause: no timeout elapsed, and
 * draft-18 Section 10.6 defines TIMEOUT as an implementation timeout the relay
 * did not hit. REDIRECT (0x34) is not available either — it is meaningful only
 * with the Redirect structure of Section 10.6.1, which this relay does not
 * carry, and draft-16 does not assign it at all. draft-18's GOING_AWAY (0x6)
 * would be precise for the GOAWAY cause, but draft-16 assigns no GOING_AWAY in
 * this registry, and REQUEST_ERROR codes travel to downstream subscribers as a
 * plain number with no per-target translation — so a d18-only code here would
 * reach a draft-16 subscriber as an unassigned number.
 *
 * The generic INTERNAL_ERROR (0x0) is assigned identically in both registries
 * and states nothing false. */
static uint32_t
bind_upstream_end_reqerr(bind_upstream_end_t how)
{
    switch (how) {
    case BIND_UP_END_GOAWAY:
    case BIND_UP_END_REDIRECT:
        return BIND_REQERR_INTERNAL;
    }
    return BIND_REQERR_INTERNAL;
}


/* A relay-originated request was terminated by the upstream. The relay does NOT
 * follow redirect URIs or migrate requests to a new session (a later routing
 * feature), so it treats the request as terminal and notifies downstreams
 * rather than hanging. The only request the relay originates is the upstream
 * SUBSCRIBE, so only that family maps to tracked state: an ACTIVE upstream
 * terminates its downstream subscribers with a wire SUBSCRIBE_DONE
 * (source_done); a PENDING upstream (terminated before its OK) rejects its
 * parked downstream subscribers (upstream_error). Any other family, or a
 * handle that matches no live upstream subscription, is a no-op — these are
 * terminal responses to the relay's OWN requests, not peer requests owed a
 * wire reply.
 *
 * The relay implements no reissue or retry schedule, so no answer it produces
 * here is retryable, whatever retry interval the upstream offered. */
void
bind_upstream_terminated(moqr_bind_t *b, b_conn_t *cn,
                         moq_request_family_t family, uint64_t handle_raw,
                         bind_upstream_end_t how, uint64_t now_us)
{
    if (family != MOQ_REQUEST_FAMILY_SUBSCRIBE) {
        return; /* the relay originates no other upstream request family */
    }
    for (uint32_t i = 0; i < b->n_usubs; i++) {
        if (!cn->usubs[i].used || cn->usubs[i].ssub._opaque != handle_raw) {
            continue;
        }
        moqr_result_t rc;
        BIND_CALL_RETRY(rc, moqr_core_source_done(
            b->core, cn->usubs[i].track, cn->usubs[i].track_gen,
            bind_local_pd(bind_upstream_end_pd(how)), now_us));
        if (rc == MOQR_ERR_WRONG_STATE) {
            /* PENDING upstream (terminal before OK): reject the parked
             * downstream subscribers instead of terminating active ones. */
            BIND_CALL_RETRY(rc, moqr_core_upstream_error(
                b->core, cn->usubs[i].track, cn->usubs[i].track_gen,
                bind_upstream_end_reqerr(how), now_us));
        }
        if (rc != MOQR_OK && rc != MOQR_ERR_STALE_HANDLE) {
            b->session_errors++;
        }
        usub_release(b, &cn->usubs[i]);
    }
}

#ifdef MOQR_BIND_TESTING


/* Drive the production upstream-termination dispatch for one connection.
 * draft-18 makes two of the four cause/state combinations unreachable over the
 * wire — a REQUEST_ERROR cannot answer a request that already had its OK, and
 * a per-request GOAWAY is eligible only on an ESTABLISHED request — so those
 * two rows are pinned here, against the same function the event handler calls,
 * rather than left unpinned. */
/* The first live upstream subscription this bind holds, as {conn slot, the
 * handle on that connection's session} — the two coordinates the termination
 * dispatch matches on. */
void
moqr_bind_debug_fwd_stats(const moqr_bind_t *b, moqr_bind_fwd_debug_t *out)
{
    if (out == NULL) {
        return;
    }
    memset(out, 0, sizeof(*out));
    if (b == NULL) {
        return;
    }
    *out = b->dbg_fwd;
    out->bytes_used = b->fwd_bytes_used;
    out->bytes_cap = b->fwd_bytes_cap;
    for (uint32_t ci = 0; ci < b->max_conns; ci++) {
        const b_conn_t *cn = &b->conns[ci];
        if (!cn->used || cn->refusals == NULL) {
            continue;
        }
        for (uint32_t i = 0; i < b->n_refusals; i++) {
            if (cn->refusals[i].used &&
                bind_kind_is_accept(cn->refusals[i].kind)) {
                out->accepts_pending++;
            } else if (cn->refusals[i].used) {
                out->refusals_pending++;
            }
        }
    }
}


bool
moqr_bind_debug_sg_handle(const moqr_bind_t *b, const moq_session_t *session,
                          uint64_t group, uint64_t subgroup,
                          moq_subgroup_handle_t *out)
{
    if (b == NULL || session == NULL || out == NULL) {
        return false;
    }
    for (uint32_t ci = 0; ci < b->max_conns; ci++) {
        const b_conn_t *cn = &b->conns[ci];
        if (!cn->used || cn->session != session) {
            continue;
        }
        for (uint32_t i = 0; i < b->n_sgs; i++) {
            if (cn->sgs[i].used && cn->sgs[i].group == group &&
                cn->sgs[i].subgroup == subgroup) {
                *out = cn->sgs[i].h;
                return true;
            }
        }
    }
    return false;
}


bool
moqr_bind_debug_first_usub(const moqr_bind_t *b, uint32_t *slot,
                           uint64_t *handle_raw)
{
    if (b == NULL || slot == NULL || handle_raw == NULL) {
        return false;
    }
    for (uint32_t ci = 0; ci < b->max_conns; ci++) {
        if (!b->conns[ci].used) {
            continue;
        }
        for (uint32_t i = 0; i < b->n_usubs; i++) {
            if (b->conns[ci].usubs[i].used) {
                *slot = ci;
                *handle_raw = b->conns[ci].usubs[i].ssub._opaque;
                return true;
            }
        }
    }
    return false;
}


void
moqr_bind_debug_upstream_terminated(moqr_bind_t *b, uint32_t slot,
                                    uint32_t family, uint64_t handle_raw,
                                    int redirect, uint64_t now_us)
{
    if (b == NULL || slot >= b->max_conns || !b->conns[slot].used) {
        return;
    }
    bind_upstream_terminated(b, &b->conns[slot], (moq_request_family_t)family,
                             handle_raw,
                             redirect ? BIND_UP_END_REDIRECT
                                      : BIND_UP_END_GOAWAY,
                             now_us);
}

#endif


/* Map an inbound data event's sub/pub handle to the relay track it feeds (the
 * relay's upstream subscription or a peer-initiated publish). MOQR_TRACK_INVALID
 * when it targets neither — i.e. not relay ingest. */
moqr_track_t
bind_ingest_track(moqr_bind_t *b, b_conn_t *cn, moq_subscription_t sub,
                  moq_publication_t pub)
{
    /* A source whose cancellation is owed delivers nothing more. */
    if (moq_subscription_is_valid(sub)) {
        for (uint32_t i = 0; i < b->n_usubs; i++) {
            if (cn->usubs[i].used && cn->usubs[i].ssub._opaque == sub._opaque) {
                uint64_t current;
                return !cn->usubs[i].cancel_owed &&
                    moqr_core_track_current_generation(b->core, cn->usubs[i].track, &current) == MOQR_OK &&
                    current == cn->usubs[i].track_gen ? cn->usubs[i].track : MOQR_TRACK_INVALID;
            }
        }
    } else if (moq_publication_is_valid(pub)) {
        for (uint32_t i = 0; i < b->n_pubs; i++) {
            if (cn->pubs[i].used && cn->pubs[i].pub_raw == pub._opaque) {
                uint64_t current;
                return !cn->pubs[i].cancel_owed &&
                    moqr_core_track_current_generation(b->core, cn->pubs[i].track, &current) == MOQR_OK &&
                    current == cn->pubs[i].track_gen ? cn->pubs[i].track : MOQR_TRACK_INVALID;
            }
        }
    }
    return MOQR_TRACK_INVALID;
}


/* A request terminal can arrive after a source was replaced while its
 * cancellation waited for output credit. The retained track handle alone
 * does not authorize effects on its new source. An intentionally idled
 * source still owns cleanup of its old retained log and cached FETCHes. */
bool
bind_terminal_owns_track(moqr_bind_t *b, moqr_track_t track, uint64_t source_gen)
{
    uint64_t current = 0;
    return (moqr_core_track_current_generation(b->core, track, &current) == MOQR_OK &&
            current == source_gen) ||
           moqr_core_internal_source_idle(b->core, track, source_gen);
}


static void bind_track_malformed(moqr_bind_t *b, b_conn_t *cn,
                                 moq_subscription_t sub, moq_publication_t pub,
                                 uint64_t now_us);

void bind_fetches_malformed(moqr_bind_t *b, moqr_track_t track,
                            uint64_t now_us);

void bind_reset_track_streams(moqr_bind_t *b, moqr_track_t track,
                              uint64_t now_us);

bool bind_cancel_settled(moqr_bind_t *b, b_conn_t *cn, moq_result_t rc,
                         const char *what, uint64_t now_us);

moq_result_t bind_cancel_upstream_sub(b_conn_t *cn, moq_subscription_t ssub,
                                      uint64_t now_us);

moq_result_t bind_cancel_upstream_pub(b_conn_t *cn, moq_publication_t pub,
                                      uint64_t code, uint64_t now_us);


/* An object the core refused to ingest. A duplicate of a retained Object
 * that differs in Delivery Mode, Subgroup ID, Priority, status or Payload
 * makes the Track malformed (draft-22 §7.1, §12.1); an identical one is the
 * ordinary duplicate a relay drops (§7.3: "SHOULD attempt to deduplicate").
 * Any other refusal leaves a hole in the subgroup the object belonged to, so
 * the subgroup is not passed on as complete (§7.4, §11.3.2). */
uint32_t
bind_ingest_refused(moqr_bind_t *b, b_conn_t *cn, moqr_track_t track,
                    const moqr_log_append_desc_t *d, moqr_result_t rc,
                    moq_subscription_t sub, moq_publication_t pub,
                    uint64_t now_us)
{
    /* Identity is the Location (Group, Object) across every subgroup and the
     * datagram list (§7.1: "identified by the Object's Full Track Name, Group
     * ID and Object ID"), and the comparison runs before any refusal is
     * treated as a plain drop, whatever the log refused it for. */
    (void)rc;
    uint32_t dup = moqr_core_internal_duplicate(b->core, track, d);
    switch (dup) {
    case MOQR_LOG_DUP_IDENTICAL:
    case MOQR_LOG_DUP_UNVERIFIED:
    case MOQR_LOG_DUP_UPDATE:
        /* A copy the relay drops (§7.3: "SHOULD attempt to deduplicate"), or
         * the permitted existing-to-missing update (§7.1 item 1), which the
         * relay MAY ignore: it keeps what it retains. */
        b->ingest_duplicates++;
        return dup;
    case MOQR_LOG_DUP_PENDING:
        return dup;          /* the caller compares the copy's chunks */
    case MOQR_LOG_DUP_DIFFERS:
        b->ingest_refusals++;
        bind_track_malformed(b, cn, sub, pub, now_us);
        return dup;
    default:
        break;
    }
    b->ingest_refusals++;
    if (!d->datagram_pref) {
        (void)moqr_core_internal_note_gap(b->core, track, d->group_id, d->subgroup_id);
    }
    return dup;
}


uint32_t
bind_conn_slot(const moqr_bind_t *b, const b_conn_t *cn)
{
    return (uint32_t)(cn - b->conns);
}


/* The owner comparing the copy a chunk belongs to, or NULL. */
b_dup_owner_t *
bind_dup_owner_find(moqr_bind_t *b, const b_conn_t *cn, uint64_t src,
                    const moq_object_chunk_event_t *oc)
{
    for (uint32_t i = 0; i < BIND_DUP_OWNERS; i++) {
        b_dup_owner_t *o = &b->dup_owners[i];
        if (o->used && o->conn == bind_conn_slot(b, cn) && o->src == src &&
            o->group == oc->group_id && o->subgroup == oc->subgroup_id &&
            o->object == oc->object_id) {
            return o;
        }
    }
    return NULL;
}


/* Compare one chunk of a copy. The first byte that differs from the retained
 * Object's is conclusive: whatever the rest of the copy holds, its payload
 * differs, so the Track is malformed at once (§7.1, §12.1) and the peer need
 * not finish the copy. The owner then stays only to consume the copy's later
 * chunks, which change nothing more. On the copy's terminal the owner is
 * released: every byte equal through a NORMAL end is an identical copy,
 * dropped; a copy that ended early (RESET or STOP) with no differing byte is
 * dropped too. The retained record is never touched. */
void
bind_dup_owner_chunk(moqr_bind_t *b, b_conn_t *cn, b_dup_owner_t *o,
                     const moq_object_chunk_event_t *oc, uint64_t now_us)
{
    if (oc->chunk != NULL && moq_rcbuf_len(oc->chunk) > 0 && !o->differs) {
        size_t n = moq_rcbuf_len(oc->chunk);
        if (moqr_core_internal_duplicate_range(b->core, o->track, o->group,
                                               o->subgroup, o->object, o->offset,
                                               moq_rcbuf_data(oc->chunk), n) ==
            MOQR_LOG_RANGE_DIFFERS) {
            o->differs = true;
            b->ingest_refusals++;
            bind_track_malformed(b, cn, oc->sub, oc->pub, now_us);
        }
        o->offset += n;
    }
    if (!oc->end) {
        return;
    }
    o->used = false;
    if (!o->differs) {
        b->ingest_duplicates++;
    }
}


/* A connection or Track goes away: its copies' owners with it. */
void
bind_dup_owners_release(moqr_bind_t *b, const b_conn_t *cn)
{
    for (uint32_t i = 0; i < BIND_DUP_OWNERS; i++) {
        if (b->dup_owners[i].used && b->dup_owners[i].conn == bind_conn_slot(b, cn)) {
            b->dup_owners[i].used = false;
        }
    }
}


/* Retain an owned copy of the track key an upstream subscription was issued
 * for, so a later forwarded fetch can name it without consulting the core.
 * Bounded by the full-track-name cap; a copy that cannot be made leaves the
 * subscription usable for live delivery but not as a forwarding source. */
void
usub_store_key(moqr_bind_t *b, b_usub_t *us, const moq_bytes_t *parts,
               uint32_t ns_count, moq_bytes_t name)
{
    us->has_key = false;
    us->key = NULL;
    us->key_len = 0;
    if (ns_count > 32) {
        return;
    }
    size_t total = name.len;
    for (uint32_t i = 0; i < ns_count; i++) {
        if (parts[i].len == 0 || parts[i].data == NULL ||
            BF_FWD_KEY_MAX - total < parts[i].len) {
            return;
        }
        total += parts[i].len;
    }
    if (total > BF_FWD_KEY_MAX || (name.len > 0 && name.data == NULL)) {
        return;
    }
    uint8_t *bytes = NULL;
    if (total > 0) {
        bytes = b->alloc.alloc(total, b->alloc.ctx);
        if (bytes == NULL) {
            return;
        }
    }
    size_t off = 0;
    for (uint32_t i = 0; i < ns_count; i++) {
        memcpy(bytes + off, parts[i].data, parts[i].len);
        us->parts[i] = (moq_bytes_t){ bytes + off, parts[i].len };
        off += parts[i].len;
    }
    if (name.len > 0) {
        memcpy(bytes + off, name.data, name.len);
    }
    us->name = (moq_bytes_t){ bytes + off, name.len };
    us->ns_count = ns_count;
    us->key = bytes;
    us->key_len = total;
    us->has_key = true;
}


void
usub_release(moqr_bind_t *b, b_usub_t *us)
{
    if (us->key != NULL) {
        b->alloc.free(us->key, us->key_len, b->alloc.ctx);
    }
    memset(us, 0, sizeof(*us));
}


void
usub_release_all(moqr_bind_t *b, b_conn_t *cn)
{
    if (cn->usubs == NULL) {
        return;
    }
    for (uint32_t i = 0; i < b->n_usubs; i++) {
        if (cn->usubs[i].key != NULL || cn->usubs[i].used) {
            usub_release(b, &cn->usubs[i]);
        }
    }
}


/* Every downstream subgroup stream still open for a subscription to `track`
 * is reset with MALFORMED_TRACK, before the subscriptions end: the Track is
 * malformed, so nothing more is sent on them, and a reset stream counts
 * toward the PUBLISH_DONE stream count the subscriber waits for. A reset the
 * session cannot take now stays owed on the stream (b_sg_t.reset_owed): its
 * close is that reset whenever it happens, never a FIN. */
void
bind_reset_track_streams(moqr_bind_t *b, moqr_track_t track, uint64_t now_us)
{
    for (uint32_t ci = 0; ci < b->max_conns; ci++) {
        b_conn_t *cn = &b->conns[ci];
        if (!cn->used || cn->closed) continue;
        uint64_t wire = MOQ_STREAM_ERROR_MALFORMED_TRACK;
        (void)moqr_reset_encode(moqr_bind_conn_version(b, cn->session),
                                MOQR_RESET_MALFORMED_TRACK, &wire);
        for (uint32_t di = 0; di < b->n_dsubs; di++) {
            const b_dsub_t *ds = &cn->dsubs[di];
            moqr_track_t dt;
            uint64_t dg = 0;
            if (!ds->used ||
                moqr_core_sub_track(b->core, ds->rsub, &dt, &dg) != MOQR_OK ||
                !moqr_track_eq(dt, track)) {
                continue;
            }
            for (uint32_t i = 0; i < b->n_sgs; i++) {
                if (!cn->sgs[i].used || cn->sgs[i].sub_raw != ds->sub_raw) continue;
                if (cn->sgs[i].stopped) continue;
                if (cn->events_pending) {
                    cn->sgs[i].reset_owed = true;
                    cn->sgs[i].reset_wire = wire;
                    continue;
                }
                moq_result_t rrc = moq_session_reset_subgroup(cn->session,
                                                              cn->sgs[i].h, wire,
                                                              now_us);
                if (rrc == MOQ_ERR_WOULD_BLOCK) {
                    /* Owed: the stream's next close is this reset. */
                    cn->sgs[i].reset_owed = true;
                    cn->sgs[i].reset_wire = wire;
                    continue;
                }
                cn->sgs[i].used = false;
                cn->sgs[i].obj_begun = false;
                cn->sgs[i].reset_owed = false;
                bind_dl_rearm_on_slot_release(b, cn);
            }
        }
    }
}


/* Every FETCH this relay serves for `track` from its log ends with
 * MALFORMED_TRACK (draft-22 §12.1: a relay "MUST ... reset any fetch streams
 * with Status Code MALFORMED_TRACK"): one already answered is reset, one not
 * yet answered is refused with the same code. */
void
bind_fetches_malformed(moqr_bind_t *b, moqr_track_t track, uint64_t now_us)
{
    /* Forwarded Joining FETCHes of the Track, collected or still collecting:
     * the collection is not replayed; the upstream request, if live, is
     * cancelled; the downstream FETCH is refused or reset with
     * MALFORMED_TRACK by the forwarding pump, which retries under pressure.
     * A private fill collection stops collecting; its fill is ended below. */
    for (uint32_t i = 0; i < b->n_fwd; i++) {
        b_fwd_t *t = &b->fwd[i];
        if (!t->used || !moqr_track_eq(t->track, track)) continue;
        if (t->consumer == FW_CONSUMER_JOIN && t->d != FW_D_TERMINAL &&
            !t->malformed) {
            b_conn_t *dcn = fwd_conn(b, t->d_cookie, t->d_binding);
            uint64_t wire = MOQ_STREAM_ERROR_MALFORMED_TRACK;
            if (dcn != NULL) {
                (void)moqr_reset_encode(moqr_bind_conn_version(b, dcn->session),
                                        MOQR_RESET_MALFORMED_TRACK, &wire);
            }
            t->malformed = true;
            t->malformed_wire = wire;
        }
        if (t->u == FW_U_PENDING || t->u == FW_U_UNSENT) {
            fwd_end_up(t, FW_END_ERROR);
            t->err_code = MOQ_REQUEST_ERROR_MALFORMED_TRACK;
        }
    }
    for (uint32_t ci = 0; ci < b->max_conns; ci++) {
        b_conn_t *cn = &b->conns[ci];
        if (!cn->used || cn->closed) continue;
        for (uint32_t fi = 0; fi < b->n_fetches; fi++) {
            b_fetch_t *bf = &cn->fetches[fi];
            if (!bf->used || bf->sa_remote) continue;
            if (bf->fill) {
                /* A fill of a subscription to the Track ends with
                 * MALFORMED_TRACK now, before the subscription's own
                 * terminal could end it as its child; a refusal keeps the
                 * abort owed with that code. */
                if (!moqr_track_eq(bf->fill_track, track)) continue;
                uint64_t code = MOQ_STREAM_ERROR_MALFORMED_TRACK;
                (void)moqr_reset_encode(moqr_bind_conn_version(b, cn->session),
                                        MOQR_RESET_MALFORMED_TRACK, &code);
                bf->fill_abort_code = code;
                bf->fill_abort_code_set = true;
                moq_result_t rc = moq_session_abort_fill(cn->session, bf->sfill,
                                                         code, now_us);
                if (rc == MOQ_ERR_WOULD_BLOCK) {
                    bf->state = BF_FILL_NEEDS_ABORT;
                } else {
                    if (rc != MOQ_OK && !fwd_result_is_gone(rc)) b->session_errors++;
                    bind_fill_release(b, bf);
                }
                continue;
            }
            moqr_track_t ft;
            uint64_t fgen = 0;
            if (moqr_core_internal_fetch_track(b->core, bf->cfetch, &ft, &fgen) !=
                    MOQR_OK ||
                !moqr_track_eq(ft, track)) {
                continue;
            }
            if (bf->state == BF_NEEDS_OK || bf->state == BF_FETCH_NEEDS_PLAN) {
                moq_fetch_t sf = bf->sfetch;
                if (bf->standalone) {
                    bind_fill_release(b, bf);
                } else {
                    (void)moqr_core_fetch_close(b->core, bf->cfetch);
                    memset(bf, 0, sizeof(*bf));
                }
                bind_fetch_refuse(b, cn, sf, MOQ_REQUEST_ERROR_MALFORMED_TRACK,
                                  now_us);
                continue;
            }
            /* Answered: the reset is owed and driven by the fetch pump. */
            bind_standalone_abort(b, cn, bf, MOQR_RESET_MALFORMED_TRACK);
        }
    }
}


/* The Track an upstream subscription or publication carries is malformed
 * (draft-22 §12.1): downstream subscriptions end with PUBLISH_DONE
 * MALFORMED_TRACK and served fetch streams are reset with it; upstream, the
 * relay as subscriber "MUST cancel any corresponding subscription or
 * fetches for that Track from that publisher". The triggering object was
 * never cached. */
static void
bind_track_malformed(moqr_bind_t *b, b_conn_t *cn, moq_subscription_t sub,
                     moq_publication_t pub, uint64_t now_us)
{
    moqr_track_t track = MOQR_TRACK_INVALID;
    uint64_t gen = 0;
    b_usub_t *us = NULL;
    b_pub_t *bp = NULL;
    if (moq_subscription_is_valid(sub)) {
        for (uint32_t i = 0; i < b->n_usubs; i++) {
            if (cn->usubs[i].used && cn->usubs[i].ssub._opaque == sub._opaque) {
                us = &cn->usubs[i];
                track = us->track;
                gen = us->track_gen;
                break;
            }
        }
    } else if (moq_publication_is_valid(pub)) {
        for (uint32_t i = 0; i < b->n_pubs; i++) {
            if (cn->pubs[i].used && cn->pubs[i].pub_raw == pub._opaque) {
                bp = &cn->pubs[i];
                track = bp->track;
                gen = bp->track_gen;
                break;
            }
        }
    }
    if (!moqr_track_is_valid(track)) {
        return;
    }
    b->malformed_tracks++;
    bind_reset_track_streams(b, track, now_us);
    moqr_result_t rc;
    BIND_CALL_RETRY(rc, moqr_core_source_done(
        b->core, track, gen, bind_local_pd(MOQR_PD_MALFORMED_TRACK), now_us));
    if (rc != MOQR_OK && rc != MOQR_ERR_STALE_HANDLE &&
        rc != MOQR_ERR_WRONG_STATE) {
        b->session_errors++;
    }
    bind_fetches_malformed(b, track, now_us);
    /* The relay as subscriber "MUST cancel any corresponding subscription"
     * (draft-22 §12.1): an obligation, not best effort. A cancellation the
     * session refuses for now stays owed on the source's entry and is
     * retried every pump (bind_retry_owed_cancels) until it commits or the
     * request is gone. */
    if (us != NULL) {
        if (bind_cancel_settled(b, cn, bind_cancel_upstream_sub(cn, us->ssub, now_us),
                                "malformed Track cancellation failed", now_us)) {
            usub_release(b, us);
        } else {
            us->cancel_owed = true;
            cn->retry_pending |= B_RETRY_CANCEL;
        }
    } else if (bp != NULL) {
        uint64_t code = MOQ_STREAM_ERROR_MALFORMED_TRACK;
        (void)moqr_reset_encode(moqr_bind_conn_version(b, cn->session),
                                MOQR_RESET_MALFORMED_TRACK, &code);
        b_meta_publish_t *mp = bind_meta_pub_find(b, cn, pub._opaque);
        if (mp != NULL) bind_meta_pub_release(b, mp);
        if (bind_cancel_settled(b, cn, bind_cancel_upstream_pub(cn, pub, code, now_us),
                                "malformed Track cancellation failed", now_us)) {
            bp->used = false;
        } else {
            bp->cancel_owed = true;
            bp->cancel_code = code;
            cn->retry_pending |= B_RETRY_CANCEL;
        }
    }
}


/* Whether an upstream cancellation attempt settled its obligation. The
 * session refuses it for now with WOULD_BLOCK -- a full action queue, or a
 * send buffer still holding earlier output, which the transport releases --
 * and the cancellation stays owed. It is settled when it committed or the
 * request is already gone (ended, cancelled, its session closed). Any other
 * result is a local failure that can never make the cancellation: the
 * upstream session is closed with INTERNAL_ERROR so the request cannot go on
 * with no owner, which settles it too. */
bool
bind_cancel_settled(moqr_bind_t *b, b_conn_t *cn, moq_result_t rc, const char *what,
                    uint64_t now_us)
{
    if (rc == MOQ_ERR_WOULD_BLOCK) return false;
    if (rc != MOQ_OK && !fwd_result_is_gone(rc)) {
        b->session_errors++;
        (void)moq_session_close(cn->session, BIND_SESSION_INTERNAL_ERROR, what, now_us);
    }
    return true;
}


moq_result_t
bind_cancel_upstream_sub(b_conn_t *cn, moq_subscription_t ssub, uint64_t now_us)
{
#ifdef MOQR_BIND_TESTING
    if (atomic_load(&bind_dbg_block_upstream_cancels) > 0) {
        (void)atomic_fetch_sub(&bind_dbg_block_upstream_cancels, 1);
        return MOQ_ERR_WOULD_BLOCK;   /* injected: the session is not called */
    }
    if (atomic_load(&bind_dbg_fail_upstream_cancels) > 0) {
        (void)atomic_fetch_sub(&bind_dbg_fail_upstream_cancels, 1);
        return MOQ_ERR_BUFFER;        /* injected: the session is not called */
    }
#endif
    return moq_session_unsubscribe(cn->session, ssub, now_us);
}


moq_result_t
bind_cancel_upstream_pub(b_conn_t *cn, moq_publication_t pub, uint64_t code,
                         uint64_t now_us)
{
#ifdef MOQR_BIND_TESTING
    if (atomic_load(&bind_dbg_block_upstream_cancels) > 0) {
        (void)atomic_fetch_sub(&bind_dbg_block_upstream_cancels, 1);
        return MOQ_ERR_WOULD_BLOCK;   /* injected: the session is not called */
    }
#endif
    return moq_session_cancel_publication(cn->session, pub, code, now_us);
}


/* Retry the upstream cancellations owed by malformed Tracks, each settled
 * as bind_cancel_settled decides. */
void
bind_retry_owed_cancels(moqr_bind_t *b, b_conn_t *cn, uint64_t now_us)
{
    if (!(cn->retry_pending & B_RETRY_CANCEL)) {
        return;   /* nothing was marked owed since a walk last found none */
    }
    for (uint32_t i = 0; i < b->n_usubs; i++) {
        b_usub_t *us = &cn->usubs[i];
        if (!us->used || !us->cancel_owed) continue;
        if (bind_cancel_settled(b, cn, bind_cancel_upstream_sub(cn, us->ssub, now_us),
                                "malformed Track cancellation failed", now_us)) {
            usub_release(b, us);
        }
    }
    for (uint32_t i = 0; i < b->n_pubs; i++) {
        b_pub_t *bp = &cn->pubs[i];
        if (!bp->used || !bp->cancel_owed) continue;
        moq_publication_t pub = { bp->pub_raw };
        if (bind_cancel_settled(b, cn,
                                bind_cancel_upstream_pub(cn, pub, bp->cancel_code, now_us),
                                "malformed Track cancellation failed", now_us)) {
            memset(bp, 0, sizeof(*bp));
        }
    }
    /* Re-read rather than tracked through the walks: a settlement above may
     * mark another entry owed, possibly one already passed. */
    bool owed = false;
    for (uint32_t i = 0; !owed && i < b->n_usubs; i++) {
        owed = cn->usubs[i].used && cn->usubs[i].cancel_owed;
    }
    for (uint32_t i = 0; !owed && i < b->n_pubs; i++) {
        owed = cn->pubs[i].used && cn->pubs[i].cancel_owed;
    }
    cn->retry_pending = (uint8_t)((cn->retry_pending & ~B_RETRY_CANCEL) |
                                  (owed ? B_RETRY_CANCEL : 0u));
}
