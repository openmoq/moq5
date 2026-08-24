/*
 * Deterministic exact-work oracle for the advancing-call preamble.
 *
 * CONTRACT UNDER TEST: the work one advancing call performs is bounded by the
 * ACTIVE / READY / DEFERRED owner set, not by configured pool capacity. Hold
 * the live owner set and its state fixed, vary a capacity that set does not
 * occupy, and the measured work image must not move -- on EVERY capacity axis
 * the preamble touches, not just the two pools the sweep names.
 *
 * No wall clock appears anywhere. Every number is an exact integer read from
 * the gated probe counters (session_internal.h), which are observational: each
 * is a plain increment at one loop-body entry or one charged transition, and
 * no control decision reads them.
 *
 * THREE independent assertion families, deliberately separable:
 *
 *   OUTPUT/STATE -- the exact normalized event and action records a phase
 *                   produced, in order, with complete payloads, plus a declared
 *                   owner/subgroup/RX/cursor state view. Expected images are
 *                   built from fixture declarations made BEFORE the call.
 *   MODEL        -- charged-transition and pending-owner counts, declared per
 *                   case and absolute.
 *   COMPLEXITY   -- the configured-capacity probe counters must be IDENTICAL
 *                   across every row of every capacity axis, per phase.
 *
 * Compiling with EXACT_WORK_NO_COMPLEXITY drops ONLY the complexity family;
 * every output/state/model assertion must still pass, which is what proves the
 * counters observe rather than decide.
 *
 * PHASES. A case is a sequence of phases (initial call, capacity recovery,
 * idempotent repeat, post-free reuse). Each phase carries its own work, output
 * and state image and is compared across capacities SEPARATELY, so a scan that
 * moved out of a refused call and into its recovery cannot cancel in a total.
 *
 * NOT installed; requires moq-core-test-internals.
 */
#include <moq/moq.h>
#include <moq/control_d18.h>
#include "test_support.h"
#include "test_session_support.h"
#include "../../core/src/session/session_internal.h"
#include "../../core/src/session/session_transport.h"
#include "../support/sweep_arm.h"
#include "../support/ownership_graph.h"
#include "../support/occupancy_audit.h"
#include <stdio.h>
#include <string.h>

/* ================= work image ===================================== */

/*
 * CONFIGURED-POOL scans, one named field per loop site -- a cleanup scan never
 * folds into the bound-stream counter, or a correction that moved work from
 * one site to another would cancel.
 *
 * FOURTEEN configured-pool sites: the full transitive inventory reachable from
 * session_advance_sweep(), plus the three fetch-pool scans of the buffered-join
 * cleanup route, which turns out NOT to be preamble work at all (see
 * run_join_lifecycle) and is measured on its own route.
 *
 * The staged ring is counted separately and is NOT one of the fourteen: its
 * slot count is a compile-time constant, not a configured capacity. It is
 * carried so the day it becomes configurable it is already measured.
 *
 * Loops in the same reachable set that are bounded by neither a pool nor the
 * ring are counted separately below and excluded from the comparator, so their
 * classification is measured rather than asserted.
 */
typedef struct {
    /* sg_cap */
    uint64_t sg_reap, sg_deadline, close_sg;
    /* pub_cap */
    uint64_t pub_reap, retry_pub;
    /* sub_cap */
    uint64_t sub_reap, retry_sub, fwd_pending;
    /* rx_cap */
    uint64_t rx_stop, close_rx, deferred_rx;
    /* fetch_cap */
    uint64_t join, join_resolve, join_reject;
    /* staged slots (a compile-time constant, not configurable -- carried so a
     * future configurable staging pool is measured the day it appears) */
    uint64_t staged;
    /* charged transitions and owner-model counts */
    uint64_t sg_charged, rx_charged, pub_pending, sub_pending;
    /* NON-capacity loop sites: one field per site, exactly as the capacity
     * scans get one field per site */
    uint64_t queued_action, queued_event, tomb, auth_staging;
    uint64_t index_find, index_rm_search, index_rm_backshift;
    /* invocation counts, separate from the probe-step counts above */
    uint64_t index_find_calls, index_rm_calls;
    /* control loops: bounded by active work */
    uint64_t catchup, rx_rescan;
} work_t;

static void work_read(work_t *w)
{
    w->sg_reap        = session_work_sg_reap_probes;
    w->sg_deadline    = session_work_sg_deadline_probes;
    w->close_sg       = session_work_close_sg_probes;
    w->pub_reap       = session_work_pub_reap_probes;
    w->retry_pub      = session_work_retry_pub_probes;
    w->sub_reap       = session_work_sub_reap_probes;
    w->retry_sub      = session_work_retry_sub_probes;
    w->fwd_pending    = session_work_fwd_pending_probes;
    w->rx_stop        = session_work_rx_probes;
    w->close_rx       = session_work_close_rx_probes;
    w->deferred_rx    = session_work_deferred_rx_probes;
    w->join           = session_work_join_probes;
    w->join_resolve   = session_work_join_resolve_probes;
    w->join_reject    = session_work_join_reject_probes;
    w->staged         = session_work_staged_probes;
    w->queued_action  = session_work_queued_action;
    w->queued_event   = session_work_queued_event;
    w->tomb           = session_work_tomb_probes;
    w->auth_staging   = session_work_auth_staging;
    w->index_find     = session_work_index_find;
    w->index_find_calls = session_work_index_find_calls;
    w->index_rm_calls   = session_work_index_rm_calls;
    w->index_rm_search = session_work_index_rm_search;
    w->index_rm_backshift = session_work_index_rm_backshift;
    w->catchup        = session_work_catchup_passes;
    w->rx_rescan      = session_work_rx_rescan_passes;
    w->sg_charged     = session_work_sg_reap_charged;
    w->rx_charged     = session_work_rx_charged;
    w->pub_pending    = session_work_pub_reap_pending;
    w->sub_pending    = session_work_sub_reap_pending;
}

/* Explicit per-field subtraction. A cast of the struct to uint64_t* and
 * pointer arithmetic across members is not a representation the C standard
 * guarantees, so every field is named. */
static void work_delta(const work_t *a, const work_t *b, work_t *o)
{
    o->sg_reap        = b->sg_reap        - a->sg_reap;
    o->sg_deadline    = b->sg_deadline    - a->sg_deadline;
    o->close_sg       = b->close_sg       - a->close_sg;
    o->pub_reap       = b->pub_reap       - a->pub_reap;
    o->retry_pub      = b->retry_pub      - a->retry_pub;
    o->sub_reap       = b->sub_reap       - a->sub_reap;
    o->retry_sub      = b->retry_sub      - a->retry_sub;
    o->fwd_pending    = b->fwd_pending    - a->fwd_pending;
    o->rx_stop        = b->rx_stop        - a->rx_stop;
    o->close_rx       = b->close_rx       - a->close_rx;
    o->deferred_rx    = b->deferred_rx    - a->deferred_rx;
    o->join           = b->join           - a->join;
    o->join_resolve   = b->join_resolve   - a->join_resolve;
    o->join_reject    = b->join_reject    - a->join_reject;
    o->staged         = b->staged         - a->staged;
    o->queued_action  = b->queued_action  - a->queued_action;
    o->queued_event   = b->queued_event   - a->queued_event;
    o->tomb           = b->tomb           - a->tomb;
    o->auth_staging   = b->auth_staging   - a->auth_staging;
    o->index_find     = b->index_find     - a->index_find;
    o->index_find_calls = b->index_find_calls - a->index_find_calls;
    o->index_rm_calls   = b->index_rm_calls   - a->index_rm_calls;
    o->index_rm_search    = b->index_rm_search    - a->index_rm_search;
    o->index_rm_backshift = b->index_rm_backshift - a->index_rm_backshift;
    o->catchup        = b->catchup        - a->catchup;
    o->rx_rescan      = b->rx_rescan      - a->rx_rescan;
    o->sg_charged     = b->sg_charged     - a->sg_charged;
    o->rx_charged     = b->rx_charged     - a->rx_charged;
    o->pub_pending    = b->pub_pending    - a->pub_pending;
    o->sub_pending    = b->sub_pending    - a->sub_pending;
}

static uint64_t work_capacity_total(const work_t *w)
{
    return w->sg_reap + w->sg_deadline + w->close_sg +
           w->pub_reap + w->retry_pub +
           w->sub_reap + w->retry_sub + w->fwd_pending +
           w->rx_stop + w->close_rx + w->deferred_rx +
           w->join + w->join_resolve + w->join_reject + w->staged;
}

/*
 * Renders EVERY differing configured-capacity field into `buf` and returns it,
 * or NULL when the two images agree. Reporting only the first divergence would
 * let a later site's scaling hide behind an earlier one, which is exactly the
 * masking this test exists to prevent.
 *
 * The classified non-capacity fields are deliberately not compared here; they
 * are printed on every row instead.
 */
static const char *work_capacity_diff(const work_t *a, const work_t *b,
                                      char *buf, size_t cap)
{
    size_t off = 0;
    int any = 0;
#define CMPF(f)                                                                  do { if (a->f != b->f) {                                                         int k = snprintf(buf + off, cap - off, "%s%s(%llu->%llu)",                                    any ? "," : "", #f, (unsigned long long)a->f,                                (unsigned long long)b->f);                                  if (k > 0 && (size_t)k < cap - off) off += (size_t)k;                        any = 1;                                                                 } } while (0)
    CMPF(sg_reap); CMPF(sg_deadline); CMPF(close_sg);
    CMPF(pub_reap); CMPF(retry_pub);
    CMPF(sub_reap); CMPF(retry_sub); CMPF(fwd_pending);
    CMPF(rx_stop); CMPF(close_rx); CMPF(deferred_rx);
    CMPF(join); CMPF(join_resolve); CMPF(join_reject); CMPF(staged);
    /* The three index loops are NOT configured-capacity scans, but their
     * tables are sized from capacity, so a correction that made index work
     * follow the table rather than the live edge set would show up here. They
     * are exactly constant across every capacity row today. */
    /*
     * `index_rm_backshift` is DELIBERATELY EXCLUDED. Open-addressing backshift
     * length is a function of the table mask and the keys' home positions: with
     * an identical declared operation set and an identical live edge set, a
     * differently sized table clusters the same keys differently, so it is not
     * a valid cross-capacity equality member. It keeps its absolute model bound
     * ([present_rm, present_rm * IDX_CHAIN_MAX]), its producer-coverage row and
     * its printing; the exact invocation counts (index_find_calls /
     * index_rm_calls) carry the capacity-independent fact instead.
     */
    CMPF(index_find); CMPF(index_rm_search);
#undef CMPF
    return any ? buf : NULL;
}

static void work_print(const char *tag, const work_t *w)
{
    printf("    %-26s sg=%llu sgdl=%llu clsg=%llu pub=%llu rtypub=%llu "
           "sub=%llu rtysub=%llu fwd=%llu rx=%llu clrx=%llu defrx=%llu "
           "join=%llu jres=%llu jrej=%llu stg=%llu | CAPWORK=%llu | "
           "sgrp=%llu rxstop=%llu "
           "pubpd=%llu subpd=%llu | [qa=%llu qe=%llu tomb=%llu auth=%llu "
           "idxfc=%llu idxf=%llu idxrc=%llu idxrs=%llu idxrb=%llu "
           "cat=%llu rsc=%llu]\n",
           tag,
           (unsigned long long)w->sg_reap, (unsigned long long)w->sg_deadline,
           (unsigned long long)w->close_sg, (unsigned long long)w->pub_reap,
           (unsigned long long)w->retry_pub, (unsigned long long)w->sub_reap,
           (unsigned long long)w->retry_sub, (unsigned long long)w->fwd_pending,
           (unsigned long long)w->rx_stop, (unsigned long long)w->close_rx,
           (unsigned long long)w->deferred_rx, (unsigned long long)w->join,
           (unsigned long long)w->join_resolve,
           (unsigned long long)w->join_reject,
           (unsigned long long)w->staged,
           (unsigned long long)work_capacity_total(w),
           (unsigned long long)w->sg_charged, (unsigned long long)w->rx_charged,
           (unsigned long long)w->pub_pending, (unsigned long long)w->sub_pending,
           (unsigned long long)w->queued_action, (unsigned long long)w->queued_event,
           (unsigned long long)w->tomb, (unsigned long long)w->auth_staging,
           (unsigned long long)w->index_find_calls,
           (unsigned long long)w->index_find,
           (unsigned long long)w->index_rm_calls,
           (unsigned long long)w->index_rm_search,
           (unsigned long long)w->index_rm_backshift,
           (unsigned long long)w->catchup, (unsigned long long)w->rx_rescan);
}

/* ================= occupancy topology oracle ====================== */

/*
 * The pools' intrusive occupancy lists are what every advancing-preamble scan
 * walks, so their TOPOLOGY is part of what a measured call must conserve.
 *
 * The semantic owner images below deliberately mask the three container fields
 * (occ_next / occ_prev / occ_linked): a neighbour leaving the list rewrites
 * this owner's links without touching anything the owner means. That mask is
 * only safe because the COMPLETE topology -- head validity, bounded traversal,
 * single reachability in ascending slot order, prev/next reciprocity, and
 * allocated-iff-linked in both directions -- is checked by the bounded auditor
 * at the SAME lifecycle checkpoints. A membership flag is not its replacement.
 *
 * Auditing here rather than only in the standalone occupancy test is what
 * catches a TRANSIENT break: a chain corrupted by one production call and
 * repaired by the next is invisible to any check that runs only at the end.
 */
static uint64_t g_occ_audits;   /* dynamic audit count, reported at the end */

static int occ_audit_named(const moq_session_t *s, const char *what)
{
    const char *pool = "";
    size_t slot = 0;
    int failures = 0;
    g_occ_audits++;
    occ_fault_t f = occ_check_all(s, &pool, &slot);
    if (f != OCC_OK) {
        printf("      TOPOLOGY FAULT (%s): pool=%s slot=%zu -- %s\n",
               what, pool, slot, occ_fault_name(f));
        MOQ_TEST_CHECK(f == OCC_OK);
    }
    return failures;
}

/* ================= symbolic identity ============================== */

/*
 * Owners live at different physical slots in the placement matrix, so their
 * packed handles differ. Work and outputs are compared under symbolic renaming:
 * a handle is normalized to the fixture's declared symbol, never to its slot
 * bits. Differing slot bits must not authorize differing work.
 */
#define SYM_MAX 8
typedef struct {
    size_t   n;
    uint64_t raw[SYM_MAX];
    uint32_t sym[SYM_MAX];
} symtab_t;

static void sym_reset(symtab_t *t) { t->n = 0; }

static void sym_bind(symtab_t *t, uint64_t raw, uint32_t sym)
{
    if (t->n < SYM_MAX) { t->raw[t->n] = raw; t->sym[t->n] = sym; t->n++; }
}

#define SYM_UNKNOWN 0xFFFFFFFFu
static uint32_t sym_of(const symtab_t *t, uint64_t raw)
{
    if (raw == 0) return 0;
    for (size_t i = 0; i < t->n; i++)
        if (t->raw[i] == raw) return t->sym[i];
    return SYM_UNKNOWN;
}

/* ================= normalized output image ======================== */

#define REC_MAX      16
#define REC_REASON   64

typedef struct {
    int      is_action;      /* 0 = event, 1 = action */
    uint32_t kind;
    uint32_t owner_sym;      /* symbolic owner identity, not a raw handle */
    uint64_t code;           /* status_code / error_code / close code */
    uint64_t stream_count;
    uint64_t stream_ref;
    uint64_t aux;            /* SEND_DATA: owned payload length, 0 when none;
                              * SEND_BIDI_STREAM: 1 + decoded retry interval */
    uint64_t msg_type;       /* SEND_BIDI_STREAM: the decoded control msg type */
    int      oversize;       /* byte payload exceeded the record's capacity:
                              * the record is INCOMPARABLE, never equal-prefix */
    uint32_t reason_len;
    uint8_t  reason[REC_REASON];  /* reason text, or SEND_DATA header bytes */
} rec_t;

typedef struct { size_t n; int overflow; rec_t r[REC_MAX]; } out_t;

static void out_reset(out_t *o) { o->n = 0; o->overflow = 0; }

static rec_t *out_push(out_t *o)
{
    if (o->n >= REC_MAX) { o->overflow = 1; return NULL; }
    rec_t *r = &o->r[o->n++];
    memset(r, 0, sizeof(*r));
    return r;
}

/*
 * FAIL CLOSED on oversize. Recording the true length while comparing only the
 * first REC_REASON bytes would let two different tails pass on an equal prefix,
 * so an over-cap payload marks the record INCOMPARABLE instead.
 */
static void rec_set_reason(rec_t *r, const uint8_t *d, size_t len)
{
    if (!r) return;
    r->reason_len = (uint32_t)len;
    if (len > REC_REASON) { r->oversize = 1; len = REC_REASON; }
    if (d && len) memcpy(r->reason, d, len);
}

/*
 * Decode ONE complete draft-18 control message out of a request-bidi send and
 * normalize it. A partial envelope, a trailing byte, or a body that does not
 * decode all make the record INCOMPARABLE rather than comparing on whatever
 * happened to parse.
 */
static void rec_decode_bidi_message(rec_t *r, const uint8_t *d, size_t len)
{
    moq_buf_reader_t rd;
    moq_control_envelope_t env;
    moq_buf_reader_init(&rd, d, len);
    if (moq_d18_decode_envelope(&rd, &env) < 0) { r->oversize = 1; return; }
    if (moq_buf_reader_remaining(&rd) != 0)     { r->oversize = 1; return; }
    r->msg_type = env.msg_type;
    if (env.msg_type != MOQ_D18_REQUEST_ERROR)  { r->oversize = 1; return; }
    moq_d18_request_error_t err;
    if (moq_d18_decode_request_error(env.payload, env.payload_len, &err) < 0) {
        r->oversize = 1; return;
    }
    r->code = err.error_code;
    r->aux  = 1u + err.retry_interval;    /* 1-based: 0 means "not decoded" */
    rec_set_reason(r, err.reason.data, err.reason.len);
}

/* The declared image of one such terminal send. */
static void exp_bidi_request_error(out_t *o, uint32_t sym, uint64_t stream_ref,
                                   uint64_t error_code, uint64_t retry,
                                   const char *reason, bool fin)
{
    rec_t *r = out_push(o);
    if (!r) return;
    r->is_action    = 1;
    r->kind         = MOQ_ACTION_SEND_BIDI_STREAM;
    r->owner_sym    = sym;
    r->stream_ref   = stream_ref;
    r->stream_count = fin ? 1u : 0u;
    r->msg_type     = MOQ_D18_REQUEST_ERROR;
    r->code         = error_code;
    r->aux          = 1u + retry;
    if (reason) rec_set_reason(r, (const uint8_t *)reason, strlen(reason));
}

/*
 * Order across INDEPENDENT request bidis is deliberately NOT contractual: the
 * rejection loop scans the fetch pool, so the physical order follows slot
 * placement, which the placement matrix varies on purpose. The comparison is
 * therefore an exact MULTISET keyed by the symbolic owner -- every ref, FIN
 * bit, wire field and multiplicity still load-bearing, only the sequence
 * between distinct streams released.
 */
static void out_sort_by_sym(out_t *o)
{
    for (size_t i = 1; i < o->n; i++)
        for (size_t j = i; j > 0 && o->r[j - 1].owner_sym > o->r[j].owner_sym;
             j--) {
            rec_t t = o->r[j - 1]; o->r[j - 1] = o->r[j]; o->r[j] = t;
        }
}

/*
 * Drain every event, deep-copying the fields that carry meaning. Spans are
 * borrowed only until the next advancing call, so the copy happens here.
 * An unmodelled event kind is recorded with kind alone AND flagged, so it can
 * never silently compare equal to a modelled one.
 */
static void out_capture_events(moq_session_t *s, const symtab_t *t, out_t *o)
{
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) == 1) {
        rec_t *r = out_push(o);
        if (r) {
            r->is_action = 0;
            r->kind = (uint32_t)ev.kind;
            switch (ev.kind) {
            case MOQ_EVENT_PUBLISH_FINISHED:
                r->owner_sym    = sym_of(t, ev.u.publish_finished.pub._opaque);
                r->code         = ev.u.publish_finished.status_code;
                r->stream_count = ev.u.publish_finished.stream_count;
                rec_set_reason(r, ev.u.publish_finished.reason.data,
                               ev.u.publish_finished.reason.len);
                break;
            case MOQ_EVENT_SUBSCRIBE_DONE:
                r->owner_sym    = sym_of(t, ev.u.subscribe_done.sub._opaque);
                r->code         = ev.u.subscribe_done.status_code;
                r->stream_count = ev.u.subscribe_done.stream_count;
                rec_set_reason(r, ev.u.subscribe_done.reason.data,
                               ev.u.subscribe_done.reason.len);
                break;
            case MOQ_EVENT_SETUP_COMPLETE:
                /* The negotiated perspectives are the meaning here; recording
                 * them keeps a mis-negotiated pair from passing as setup. */
                r->code         = (uint64_t)ev.u.setup_complete.local_perspective;
                r->stream_count = (uint64_t)ev.u.setup_complete.peer_perspective;
                r->aux          = 1u + (uint64_t)ev.u.setup_complete.token_count;
                break;
            case MOQ_EVENT_UNSUBSCRIBED:
                r->owner_sym = sym_of(t, ev.u.unsubscribed.sub._opaque);
                break;
            case MOQ_EVENT_SESSION_CLOSED:
                r->code = ev.u.closed.code;
                rec_set_reason(r, ev.u.closed.reason.data, ev.u.closed.reason.len);
                break;
            default:
                r->owner_sym = SYM_UNKNOWN;   /* unmodelled: cannot match a model */
                break;
            }
        }
        moq_event_cleanup(&ev);
    }
}

static void out_capture_actions(moq_session_t *s, const symtab_t *t, out_t *o)
{
    (void)t;
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) == 1) {
        rec_t *r = out_push(o);
        if (r) {
            r->is_action = 1;
            r->kind = (uint32_t)a.kind;
            switch (a.kind) {
            case MOQ_ACTION_STOP_DATA:
                r->stream_ref = a.u.stop_data.stream_ref._v;
                r->code       = a.u.stop_data.error_code;
                break;
            case MOQ_ACTION_RESET_DATA:
                r->stream_ref = a.u.reset_data.stream_ref._v;
                r->code       = a.u.reset_data.error_code;
                break;
            case MOQ_ACTION_CLOSE_SESSION:
                r->code = a.u.close_session.code;
                rec_set_reason(r, a.u.close_session.reason.data,
                               a.u.close_session.reason.len);
                break;
            case MOQ_ACTION_SEND_DATA:
                r->stream_ref   = a.u.send_data.stream_ref._v;
                r->stream_count = a.u.send_data.fin ? 1u : 0u;  /* FIN flag */
                r->code         = a.u.send_data.header_len;
                r->aux          = a.u.send_data.payload
                                  ? moq_rcbuf_len(a.u.send_data.payload) + 1u : 0u;
                rec_set_reason(r, a.u.send_data.header, a.u.send_data.header_len);
                break;
            case MOQ_ACTION_SEND_BIDI_STREAM:
                /*
                 * Normalized to its DECODED wire semantics, not to opaque
                 * bytes: the control message type, the error code, the retry
                 * interval and the reason are each their own comparable field.
                 * Any envelope that fails to decode, or that leaves trailing
                 * bytes, marks the record INCOMPARABLE -- it can never pass by
                 * accident against a declared one.
                 */
                r->stream_ref   = a.u.send_bidi_stream.stream_ref._v;
                r->owner_sym    = sym_of(t, r->stream_ref);
                r->stream_count = a.u.send_bidi_stream.fin ? 1u : 0u;
                rec_decode_bidi_message(r, a.u.send_bidi_stream.data,
                                        a.u.send_bidi_stream.len);
                break;
            case MOQ_ACTION_SEND_CONTROL:
                r->stream_ref = 0;   /* modelled by kind only */
                break;
            default:
                r->owner_sym = SYM_UNKNOWN;
                break;
            }
        }
        moq_action_cleanup(&a);
    }
}

/*
 * NON-DESTRUCTIVE snapshot of both queues, read straight out of the rings.
 * A phase whose whole point is that a queue is FULL cannot capture its output
 * by polling: draining is what the next phase must find still in place.
 */
static void out_snapshot(moq_session_t *s, const symtab_t *t, out_t *o)
{
    for (size_t i = s->event_head; i < s->event_tail; i++) {
        const moq_event_t *ev = &s->events[i % s->event_cap];
        rec_t *r = out_push(o);
        if (!r) return;
        r->is_action = 0;
        r->kind = (uint32_t)ev->kind;
        switch (ev->kind) {
        case MOQ_EVENT_PUBLISH_FINISHED:
            r->owner_sym    = sym_of(t, ev->u.publish_finished.pub._opaque);
            r->code         = ev->u.publish_finished.status_code;
            r->stream_count = ev->u.publish_finished.stream_count;
            rec_set_reason(r, ev->u.publish_finished.reason.data,
                           ev->u.publish_finished.reason.len);
            break;
        case MOQ_EVENT_SUBSCRIBE_DONE:
            r->owner_sym    = sym_of(t, ev->u.subscribe_done.sub._opaque);
            r->code         = ev->u.subscribe_done.status_code;
            r->stream_count = ev->u.subscribe_done.stream_count;
            rec_set_reason(r, ev->u.subscribe_done.reason.data,
                           ev->u.subscribe_done.reason.len);
            break;
        case MOQ_EVENT_PUBLISH_UNSUBSCRIBED:
            r->owner_sym = sym_of(t, ev->u.publish_unsubscribed.pub._opaque);
            break;
        case MOQ_EVENT_SESSION_CLOSED:
            r->code = ev->u.closed.code;
            rec_set_reason(r, ev->u.closed.reason.data, ev->u.closed.reason.len);
            break;
        default:
            r->owner_sym = SYM_UNKNOWN;
            break;
        }
    }
    for (size_t i = s->action_head; i < s->action_tail; i++) {
        const moq_action_t *a = &s->actions[i % s->action_cap];
        rec_t *r = out_push(o);
        if (!r) return;
        r->is_action = 1;
        r->kind = (uint32_t)a->kind;
        if (a->kind == MOQ_ACTION_STOP_DATA) {
            r->stream_ref = a->u.stop_data.stream_ref._v;
            r->code = a->u.stop_data.error_code;
        }
    }
}

/* Declaration helpers: build the EXPECTED image before the call runs. */
static void exp_event(out_t *o, uint32_t kind, uint32_t sym, uint64_t code,
                      uint64_t sc, const char *reason)
{
    rec_t *r = out_push(o);
    if (!r) return;
    r->is_action = 0; r->kind = kind; r->owner_sym = sym;
    r->code = code; r->stream_count = sc;
    if (reason) rec_set_reason(r, (const uint8_t *)reason, strlen(reason));
}

static void exp_action(out_t *o, uint32_t kind, uint64_t stream_ref,
                       uint64_t code)
{
    rec_t *r = out_push(o);
    if (!r) return;
    r->is_action = 1; r->kind = kind; r->stream_ref = stream_ref; r->code = code;
}

static void exp_action_reason(out_t *o, uint32_t kind, uint64_t code,
                              const char *reason)
{
    rec_t *r = out_push(o);
    if (!r) return;
    r->is_action = 1; r->kind = kind; r->code = code;
    if (reason) rec_set_reason(r, (const uint8_t *)reason, strlen(reason));
}

static const char *rec_diff(const rec_t *a, const rec_t *b)
{
    if (a->oversize || b->oversize) return "oversize_incomparable";
    if (a->is_action != b->is_action) return "is_action";
    if (a->kind != b->kind) return "kind";
    if (a->owner_sym != b->owner_sym) return "owner_sym";
    if (a->code != b->code) return "code";
    if (a->stream_count != b->stream_count) return "stream_count";
    if (a->stream_ref != b->stream_ref) return "stream_ref";
    if (a->aux != b->aux) return "aux";
    if (a->msg_type != b->msg_type) return "msg_type";
    if (a->reason_len != b->reason_len) return "reason_len";
    if (memcmp(a->reason, b->reason, sizeof(a->reason)) != 0) return "reason_bytes";
    return NULL;
}

static void out_print(const char *tag, const out_t *o)
{
    printf("      %s n=%zu%s", tag, o->n, o->overflow ? " OVERFLOW" : "");
    for (size_t i = 0; i < o->n; i++) {
        const rec_t *r = &o->r[i];
        printf(" | %s k=%u sym=%u code=%llu sc=%llu ref=%llu aux=%llu mt=%llu "
               "rl=%u%s",
               r->is_action ? "A" : "E", r->kind, r->owner_sym,
               (unsigned long long)r->code, (unsigned long long)r->stream_count,
               (unsigned long long)r->stream_ref, (unsigned long long)r->aux,
               (unsigned long long)r->msg_type, r->reason_len,
               r->oversize ? " INCOMPARABLE" : "");
    }
    printf("\n");
}

/* Compares length, order and every field; names the first divergence. */
static int out_check(const out_t *want, const out_t *got, const char *where)
{
    int failures = 0;
    MOQ_TEST_CHECK(!got->overflow);
    MOQ_TEST_CHECK(!want->overflow);
    if (want->n != got->n) {
        printf("      OUTPUT MISMATCH (%s): expected %zu records, observed %zu\n",
               where, want->n, got->n);
        out_print("expected:", want);
        out_print("observed:", got);
        MOQ_TEST_CHECK(want->n == got->n);
        return failures;
    }
    for (size_t i = 0; i < want->n; i++) {
        const char *f = rec_diff(&want->r[i], &got->r[i]);
        if (f) {
            printf("      OUTPUT MISMATCH (%s): record %zu differs in '%s'\n",
                   where, i, f);
            out_print("expected:", want);
            out_print("observed:", got);
            MOQ_TEST_CHECK(f == NULL);
            return failures;
        }
    }
    return failures;
}

/* ================= declared state view ============================ */

/*
 * A named-value view of exactly the owner/subgroup/RX/cursor state a case
 * declares. Expected values are added by the fixture BEFORE the call; observed
 * values are read after it. Comparison is by name, and a mismatch names the
 * field -- so a wrong generation or a stale binding is a legible failure, not a
 * count that happens to agree.
 */
#define ST_MAX 384
#define ST_KEY  28
typedef struct { char k[ST_KEY]; uint64_t v; } stkv_t;
typedef struct { size_t n; int overflow; stkv_t kv[ST_MAX]; } state_t;

static void st_reset(state_t *v) { v->n = 0; v->overflow = 0; }

/* Keys are composed as "<prefix>.<field>", so every owner a case declares gets
 * its own inventory and two owners can never share one set of names. */
static void st_addp(state_t *v, const char *pfx, const char *k, uint64_t val)
{
    if (v->n >= ST_MAX) { v->overflow = 1; return; }
    /* Compose wide, then truncate EXPLICITLY to the stored key width -- the
     * same composition st_set() looks up with. */
    char wide[ST_KEY * 2];
    snprintf(wide, sizeof(wide), "%s.%s", pfx, k);
    memcpy(v->kv[v->n].k, wide, ST_KEY - 1u);
    v->kv[v->n].k[ST_KEY - 1u] = '\0';
    v->kv[v->n].v = val;
    v->n++;
}

/* Overwrite an already-declared field BY NAME. Positional patching of the kv
 * array breaks silently the moment the inventory grows a field. */
static void st_set(state_t *v, const char *pfx, const char *k, uint64_t val)
{
    /* Compose wide, then truncate EXPLICITLY to the stored key width, so the
     * lookup key is byte-identical to what st_addp() recorded. */
    char wide[ST_KEY * 2];
    char full[ST_KEY];
    snprintf(wide, sizeof(wide), "%s.%s", pfx, k);
    memcpy(full, wide, ST_KEY - 1u);
    full[ST_KEY - 1u] = '\0';
    for (size_t i = 0; i < v->n; i++)
        if (strcmp(v->kv[i].k, full) == 0) { v->kv[i].v = val; return; }
    v->overflow = 1;   /* declaring a field that does not exist is a failure */
}

static void st_add(state_t *v, const char *k, uint64_t val)
{
    if (v->n >= ST_MAX) { v->overflow = 1; return; }
    snprintf(v->kv[v->n].k, ST_KEY, "%s", k);
    v->kv[v->n].v = val;
    v->n++;
}

static void st_print(const char *tag, const state_t *v)
{
    printf("      %s", tag);
    for (size_t i = 0; i < v->n; i++)
        printf(" %s=%llu", v->kv[i].k, (unsigned long long)v->kv[i].v);
    printf("%s\n", v->overflow ? "  OVERFLOW" : "");
}

static int st_check(const state_t *want, const state_t *got, const char *where)
{
    int failures = 0;
    MOQ_TEST_CHECK(!want->overflow);
    MOQ_TEST_CHECK(!got->overflow);
    if (want->n != got->n) {
        printf("      STATE MISMATCH (%s): expected %zu fields, observed %zu\n",
               where, want->n, got->n);
        st_print("expected:", want);
        st_print("observed:", got);
        MOQ_TEST_CHECK(want->n == got->n);
        return failures;
    }
    for (size_t i = 0; i < want->n; i++) {
        if (strcmp(want->kv[i].k, got->kv[i].k) != 0 ||
            want->kv[i].v != got->kv[i].v) {
            printf("      STATE MISMATCH (%s): field '%s' expected %llu, "
                   "observed '%s' = %llu\n", where, want->kv[i].k,
                   (unsigned long long)want->kv[i].v, got->kv[i].k,
                   (unsigned long long)got->kv[i].v);
            st_print("expected:", want);
            st_print("observed:", got);
            MOQ_TEST_CHECK(0 == 1);
            return failures;
        }
    }
    return failures;
}

/* ================= seeded operation schedule ====================== */

/*
 * The setup operations a case performs are a LIST, shuffled by seed and
 * executed in that order. Only commuting operations enter the shuffle;
 * dependency-bearing ones stay in explicit ordered groups outside it. Slot
 * placement is separately seeded.
 */
#define OPS_MAX 12
typedef enum {
    OP_ARM_PUB = 1, OP_ARM_SUB, OP_ARM_LIVE_PUB, OP_ARM_LIVE_SUB,
    OP_ARM_FUTURE_PUB, OP_ARM_FUTURE_SUB, OP_BIND_RX, OP_ARM_CLOSING_SG,
    OP_ARM_RESETTING_SG, OP_ARM_DEFERRED_RX
} op_kind_t;

static const char *op_name(op_kind_t k)
{
    switch (k) {
    case OP_ARM_PUB:         return "arm_pub";
    case OP_ARM_SUB:         return "arm_sub";
    case OP_ARM_LIVE_PUB:    return "live_pub";
    case OP_ARM_LIVE_SUB:    return "live_sub";
    case OP_ARM_FUTURE_PUB:  return "future_pub";
    case OP_ARM_FUTURE_SUB:  return "future_sub";
    case OP_BIND_RX:         return "bind_rx";
    case OP_ARM_CLOSING_SG:  return "closing_sg";
    case OP_ARM_RESETTING_SG:return "resetting_sg";
    case OP_ARM_DEFERRED_RX: return "deferred_rx";
    }
    return "?";
}

typedef struct {
    op_kind_t kind;
    size_t    a;      /* pool slot */
    size_t    b;      /* auxiliary (rx slot / stream ref index) */
} op_t;

typedef struct {
    uint32_t seed;
    size_t   n;
    op_t     op[OPS_MAX];
    char     text[320];
} sched_t;

static uint32_t lcg_next(uint32_t *st)
{
    *st = (*st) * 1664525u + 1013904223u;
    return (*st) >> 8;
}

static void sched_reset(sched_t *sc, uint32_t seed)
{
    memset(sc, 0, sizeof(*sc));
    sc->seed = seed;
}

static void sched_add(sched_t *sc, op_kind_t k, size_t a, size_t b)
{
    if (sc->n >= OPS_MAX) return;
    sc->op[sc->n].kind = k; sc->op[sc->n].a = a; sc->op[sc->n].b = b; sc->n++;
}

/* Shuffle the commuting operation list by seed and render it for the log. */
static void sched_finalize(sched_t *sc)
{
    uint32_t st = sc->seed * 2654435761u + 12345u;
    for (size_t i = sc->n; i > 1; i--) {
        size_t j = (size_t)(lcg_next(&st) % (uint32_t)i);
        op_t t = sc->op[i - 1]; sc->op[i - 1] = sc->op[j]; sc->op[j] = t;
    }
    int off = snprintf(sc->text, sizeof(sc->text), "seed=%u ops[", sc->seed);
    for (size_t i = 0; i < sc->n && off > 0 && (size_t)off < sizeof(sc->text); i++)
        off += snprintf(sc->text + off, sizeof(sc->text) - (size_t)off,
                        "%s%s@%zu", i ? "," : "", op_name(sc->op[i].kind),
                        sc->op[i].a);
    if (off > 0 && (size_t)off < sizeof(sc->text))
        snprintf(sc->text + off, sizeof(sc->text) - (size_t)off, "]");
}

/* A seeded permutation of a slot domain, used for placement inside a pool. */
static void seeded_perm(uint32_t seed, size_t domain, size_t *out)
{
    uint32_t st = seed * 2246822519u + 7u;
    for (size_t i = 0; i < domain; i++) out[i] = i;
    for (size_t i = domain; i > 1; i--) {
        size_t j = (size_t)(lcg_next(&st) % (uint32_t)i);
        size_t t = out[i - 1]; out[i - 1] = out[j]; out[j] = t;
    }
}

/* ================= fixture ======================================== */

/*
 * Non-default fixture values throughout. Every field the terminal event
 * carries is distinct and non-zero, so a normalizer that dropped one, or a
 * finalize that surfaced a default, is visible.
 */
#define PUB_STATUS   0x27ull
#define PUB_SC       5ull
#define PUB_REASON   "exact-work-publication-reason"
#define SUB_STATUS   0x39ull
#define SUB_SC       3ull
#define SUB_REASON   "exact-work-subscription-reason"
#define RX_REF_BASE  0x7100ull   /* distinct, non-zero stream refs */

/*
 * Declared request identities. Every driven owner carries a REAL
 * request-registry edge, so the index counters measure genuine lookups and
 * removals rather than the probe cost of removing an absent key 0.
 */
#define REQ_PUB0     0x41ull
#define REQ_PUB1     0x43ull
#define REQ_SUB0     0x45ull
#define REQ_SUB1     0x47ull
#define REQ_SUB2     0x49ull

/* Symbols for owners, stable across every physical placement. */
enum { SYM_PUB0 = 11, SYM_PUB1 = 12, SYM_SUB0 = 21, SYM_SUB1 = 22, SYM_SUB2 = 23 };

typedef struct {
    uint32_t sub_cap, pub_cap, sg_cap, rx_cap, fetch_cap;
    uint32_t max_actions, max_events, scratch;
    moq_version_t version;
} caps_t;

static void caps_default(caps_t *c)
{
    c->sub_cap = 64; c->pub_cap = 16; c->sg_cap = 32; c->rx_cap = 32;
    c->fetch_cap = 16; c->max_actions = 64; c->max_events = 16;
    c->scratch = 65536; c->version = MOQ_VERSION_DRAFT_16;
}

static moq_session_t *make_session_side(const caps_t *c, moq_alloc_t *alloc,
                                       moq_perspective_t persp)
{
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg),
                               alloc ? alloc : moq_alloc_default(), persp);
    cfg.max_subscriptions   = c->sub_cap;
    cfg.max_publishes       = c->pub_cap;
    cfg.max_open_subgroups  = c->sg_cap;
    cfg.max_data_streams    = c->rx_cap;
    cfg.max_fetches         = c->fetch_cap;
    cfg.max_actions         = c->max_actions;
    cfg.max_events          = c->max_events;
    cfg.output_scratch_size = c->scratch;
    cfg.version             = c->version;
    moq_session_t *s = NULL;
    if (moq_session_create(&cfg, 0, &s) < 0) return NULL;
    return s;
}

static moq_session_t *make_session(const caps_t *c)
{
    return make_session_side(c, NULL, MOQ_PERSPECTIVE_CLIENT);
}

/*
 * ONE queued action, classified exactly. The caller declares the kind it
 * expects; anything else -- including a stray action of a kind this fixture
 * never models -- is a NAMED failure, never a silently discarded record.
 * The bytes are deep-copied here because they are borrowed only until the next
 * advancing call, and an over-cap payload FAILS rather than truncating.
 */
#define SETUP_BYTES_MAX 512

static int capture_one_action(moq_session_t *s, uint32_t want_kind,
                              uint8_t *buf, size_t cap, size_t *out_len,
                              const char *what)
{
    int failures = 0;
    moq_action_t a;
    size_t n = 0;
    *out_len = 0;
    while (moq_session_poll_actions(s, &a, 1) == 1) {
        n++;
        if (n == 1) {
            if (a.kind != want_kind) {
                printf("      SETUP ACTION MISMATCH (%s): expected kind %u, "
                       "observed %u\n", what, want_kind, (unsigned)a.kind);
                MOQ_TEST_CHECK(a.kind == want_kind);
            } else {
                const uint8_t *d = NULL;
                size_t len = 0;
                if (want_kind == MOQ_ACTION_OPEN_UNI_CONTROL) {
                    d = a.u.open_uni_control.data;
                    len = a.u.open_uni_control.len;
                } else if (want_kind == MOQ_ACTION_SEND_CONTROL) {
                    d = a.u.send_control.data;
                    len = a.u.send_control.len;
                }
                if (len == 0 || len > cap || d == NULL) {
                    printf("      SETUP ACTION MISMATCH (%s): payload len %zu "
                           "is empty, absent or over the %zu-byte cap\n",
                           what, len, cap);
                    MOQ_TEST_CHECK(len > 0 && len <= cap && d != NULL);
                } else {
                    memcpy(buf, d, len);
                    *out_len = len;
                }
            }
        } else {
            printf("      SETUP ACTION MISMATCH (%s): unexpected extra action "
                   "kind %u at position %zu\n", what, (unsigned)a.kind, n);
        }
        moq_action_cleanup(&a);
    }
    if (n != 1) {
        printf("      SETUP ACTION MISMATCH (%s): expected exactly 1 action, "
               "observed %zu\n", what, n);
        MOQ_TEST_CHECK(n == 1);
    }
    return failures;
}

/* Exactly one fully normalized SETUP_COMPLETE and nothing else. */
static int expect_setup_complete(moq_session_t *s, moq_perspective_t local,
                                 moq_perspective_t peer, const char *what)
{
    int failures = 0;
    symtab_t nosym; sym_reset(&nosym);
    out_t want, got; out_reset(&want); out_reset(&got);
    out_capture_events(s, &nosym, &got);
    rec_t *r = out_push(&want);
    MOQ_TEST_CHECK(r != NULL);
    if (r) {
        r->kind         = MOQ_EVENT_SETUP_COMPLETE;
        r->code         = (uint64_t)local;
        r->stream_count = (uint64_t)peer;
        r->aux          = 1u;          /* 1 + zero resolved setup tokens */
    }
    failures += out_check(&want, &got, what);
    return failures;
}

/* Both queues empty, compared as an image rather than as a count. */
static int expect_quiescent(moq_session_t *s, const char *what)
{
    symtab_t nosym; sym_reset(&nosym);
    out_t want, got; out_reset(&want); out_reset(&got);
    out_capture_actions(s, &nosym, &got);
    out_capture_events(s, &nosym, &got);
    return out_check(&want, &got, what);
}

/*
 * A REAL established draft-18 pair at the fixture's declared capacities, with
 * every step of the handshake classified.
 *
 * Draft-18 gives each side its OWN unidirectional control channel and carries
 * its setup inline on the open, so BOTH sides are started and the server's
 * setup exists before it has seen the client's. Nothing here is pumped
 * blindly: each start owes exactly one OPEN_UNI_CONTROL and no other action,
 * each ingress must return MOQ_OK, each side owes exactly one SETUP_COMPLETE,
 * and both queues must then be empty.
 */
static int establish_pair_d18(const caps_t *c, moq_alloc_t *alloc,
                              moq_session_t **cl_out, moq_session_t **sv_out)
{
    int failures = 0;
    moq_session_t *cl = make_session_side(c, alloc, MOQ_PERSPECTIVE_CLIENT);
    moq_session_t *sv = make_session_side(c, alloc, MOQ_PERSPECTIVE_SERVER);
    MOQ_TEST_CHECK(cl != NULL && sv != NULL);
    if (!cl || !sv) {
        if (cl) moq_session_destroy(cl);
        if (sv) moq_session_destroy(sv);
        *cl_out = NULL; *sv_out = NULL;
        return failures;
    }

    uint8_t sv_setup[SETUP_BYTES_MAX], cl_setup[SETUP_BYTES_MAX];
    size_t  sv_len = 0, cl_len = 0;

    /* Owner-empty starting point for BOTH sessions, before either is even
     * started: heads and sentinels, with no owner of any kind allocated. */
    failures += occ_audit_named(cl, "d18_pair.client_empty");
    failures += occ_audit_named(sv, "d18_pair.server_empty");

    MOQ_TEST_CHECK_EQ_INT((int)moq_session_start(sv, 0), (int)MOQ_OK);
    failures += capture_one_action(sv, MOQ_ACTION_OPEN_UNI_CONTROL, sv_setup,
                                   sizeof(sv_setup), &sv_len, "setup.server");
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_start(cl, 0), (int)MOQ_OK);
    failures += capture_one_action(cl, MOQ_ACTION_OPEN_UNI_CONTROL, cl_setup,
                                   sizeof(cl_setup), &cl_len, "setup.client");

    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_control_bytes(sv, cl_setup, cl_len, 0),
        (int)MOQ_OK);
    failures += expect_setup_complete(sv, MOQ_PERSPECTIVE_SERVER,
                                      MOQ_PERSPECTIVE_CLIENT, "setup.server");
    failures += expect_quiescent(sv, "setup.server_quiescent");

    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_control_bytes(cl, sv_setup, sv_len, 0),
        (int)MOQ_OK);
    failures += expect_setup_complete(cl, MOQ_PERSPECTIVE_CLIENT,
                                      MOQ_PERSPECTIVE_SERVER, "setup.client");
    failures += expect_quiescent(cl, "setup.client_quiescent");
    failures += expect_quiescent(sv, "setup.server_final");

    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(cl),
                          (int)MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv),
                          (int)MOQ_SESS_ESTABLISHED);
    /* Established and quiescent: SETUP allocates no request owner, so both
     * sides must still be owner-empty with intact heads. */
    failures += occ_audit_named(cl, "d18_pair.client_established_empty");
    failures += occ_audit_named(sv, "d18_pair.server_established_empty");

    *cl_out = cl; *sv_out = sv;
    return failures;
}

/* Every white-box slot is bounds-checked against the live pool before use. */
static int slot_ok(size_t slot, size_t cap, const char *what, int *failures)
{
    if (slot < cap) return 1;
    printf("      BOUNDS: %s slot %zu >= cap %zu\n", what, slot, cap);
    (*failures)++;
    return 0;
}

static uint8_t *dup_reason(moq_session_t *s, const char *txt, size_t *out_len)
{
    size_t len = strlen(txt);
    uint8_t *p = (uint8_t *)s->alloc.alloc(len, s->alloc.ctx);
    if (p) memcpy(p, txt, len);
    *out_len = p ? len : 0;
    return p;
}

/* Install the by-id registry edge a real request owner holds. */
static void register_pub(moq_session_t *s, size_t slot, uint64_t req_id)
{
    /* Idempotent: re-arming a slot must not leave a second edge on the same
     * key, which one removal would not clear. */
    request_registry_remove_by_id(s, req_id);
    s->publishes[slot].request_id = req_id;
    moq_request_endpoint_t ep;
    memset(&ep, 0, sizeof(ep));
    ep.kind = MOQ_REQ_PUBLISH;
    ep.slot = (int)slot;
    ep.has_request_id = true;
    ep.request_id = req_id;
    request_registry_insert_by_id(s, req_id, ep);
}

static void register_sub(moq_session_t *s, size_t slot, uint64_t req_id)
{
    request_registry_remove_by_id(s, req_id);
    s->subs[slot].request_id = req_id;
    moq_request_endpoint_t ep;
    memset(&ep, 0, sizeof(ep));
    ep.kind = MOQ_REQ_SUBSCRIPTION;
    ep.slot = (int)slot;
    ep.has_request_id = true;
    ep.request_id = req_id;
    request_registry_insert_by_id(s, req_id, ep);
}

/*
 * A ready deferred publication carrying non-default terminal values.
 *
 * ROLE-VALID: the deferred PUBLISH_FINISHED path is the SUBSCRIBER-role
 * publication's (pub_finalize_done's own contract), so that is the role armed.
 * Role 0 is MOQ_PUB_FREE's placeholder and is not a lifecycle any owner has.
 */
static void arm_ready_pub(moq_session_t *s, size_t slot, uint64_t req_id)
{
    /* Production allocation links the slot into the pool's occupancy
     * list, which is what the preamble scans walk. A fixture that armed
     * an owner without it would be arming a state no session can hold. */
    pub_occ_link(s, slot);
    moq_pub_entry_t *pe = &s->publishes[slot];
    pe->state  = MOQ_PUB_ESTABLISHED;
    pe->role   = MOQ_PUB_ROLE_SUBSCRIBER;
    pe->generation |= 1u;   /* the allocators' own rule: live == odd */
    pe->handle = (moq_publication_t){
        moq_handle_pack(MOQ_HANDLE_POOL_PUBLICATION, s->session_tag,
                        pe->generation, (uint32_t)slot) };
    register_pub(s, slot, req_id);
    pe->done_pending           = true;
    pe->done_expired           = true;
    pe->done_status_code       = PUB_STATUS;
    pe->done_stream_count      = PUB_SC;
    pe->processed_stream_count = 0;
    pe->done_deadline_us       = 0;
    if (!pe->done_reason_buf)
        pe->done_reason_buf = dup_reason(s, PUB_REASON, &pe->done_reason_len);
}

/* The deferred SUBSCRIBE_DONE path is the SUBSCRIBER-role subscription's. */
static void arm_ready_sub(moq_session_t *s, size_t slot, uint64_t req_id)
{
    /* Production allocation links the slot into the pool's occupancy
     * list, which is what the preamble scans walk. A fixture that armed
     * an owner without it would be arming a state no session can hold. */
    sub_occ_link(s, slot);
    moq_sub_entry_t *se = &s->subs[slot];
    se->state  = MOQ_SUB_ESTABLISHED;
    se->role   = MOQ_SUB_ROLE_SUBSCRIBER;
    se->generation |= 1u;   /* the allocators' own rule: live == odd */
    se->handle = (moq_subscription_t){
        moq_handle_pack(MOQ_HANDLE_POOL_SUBSCRIPTION, s->session_tag,
                        se->generation, (uint32_t)slot) };
    register_sub(s, slot, req_id);
    se->done_pending           = true;
    se->done_expired           = true;
    se->done_status_code       = SUB_STATUS;
    se->done_stream_count      = SUB_SC;
    se->processed_stream_count = 0;
    se->done_deadline_us       = 0;
    if (!se->done_reason_buf)
        se->done_reason_buf = dup_reason(s, SUB_REASON, &se->done_reason_len);
}

/* Occupied, valid, but with no terminal pending. */
static void arm_live_pub(moq_session_t *s, size_t slot, uint64_t req_id)
{
    /* Production allocation links the slot into the pool's occupancy
     * list, which is what the preamble scans walk. A fixture that armed
     * an owner without it would be arming a state no session can hold. */
    pub_occ_link(s, slot);
    moq_pub_entry_t *pe = &s->publishes[slot];
    pe->state  = MOQ_PUB_ESTABLISHED;
    pe->role   = MOQ_PUB_ROLE_SUBSCRIBER;
    pe->generation |= 1u;   /* the allocators' own rule: live == odd */
    pe->handle = (moq_publication_t){
        moq_handle_pack(MOQ_HANDLE_POOL_PUBLICATION, s->session_tag,
                        pe->generation, (uint32_t)slot) };
    register_pub(s, slot, req_id);
    pe->done_pending = false;
}

static void arm_live_sub(moq_session_t *s, size_t slot, uint64_t req_id)
{
    /* Production allocation links the slot into the pool's occupancy
     * list, which is what the preamble scans walk. A fixture that armed
     * an owner without it would be arming a state no session can hold. */
    sub_occ_link(s, slot);
    moq_sub_entry_t *se = &s->subs[slot];
    se->state  = MOQ_SUB_ESTABLISHED;
    se->role   = MOQ_SUB_ROLE_SUBSCRIBER;
    se->generation |= 1u;   /* the allocators' own rule: live == odd */
    se->handle = (moq_subscription_t){
        moq_handle_pack(MOQ_HANDLE_POOL_SUBSCRIPTION, s->session_tag,
                        se->generation, (uint32_t)slot) };
    register_sub(s, slot, req_id);
    se->done_pending = false;
}

/* Deferred but NOT runnable: count unsatisfied and deadline in the future. */
static void arm_future_pub(moq_session_t *s, size_t slot, uint64_t now,
                           uint64_t req_id)
{
    arm_live_pub(s, slot, req_id);
    moq_pub_entry_t *pe = &s->publishes[slot];
    pe->done_pending           = true;
    pe->done_expired           = false;
    pe->done_stream_count      = PUB_SC;
    pe->processed_stream_count = 0;
    pe->done_deadline_us       = now + 1000000ull;
}

static void arm_future_sub(moq_session_t *s, size_t slot, uint64_t now,
                           uint64_t req_id)
{
    arm_live_sub(s, slot, req_id);
    moq_sub_entry_t *se = &s->subs[slot];
    se->done_pending           = true;
    se->done_expired           = false;
    se->done_stream_count      = SUB_SC;
    se->processed_stream_count = 0;
    se->done_deadline_us       = now + 1000000ull;
}

/* Bind a live rx stream to an owner, with a distinct non-zero stream ref so
 * the STOP action's payload discriminates which stream was stopped. */
/*
 * A live rx stream, indexed exactly as production indexes one. Without the
 * idx_rx_by_ref edge its removal would walk an absent key, which is precisely
 * the fake-index shape this fixture must not have.
 */
static void bind_rx_pub(moq_session_t *s, size_t rx_slot, moq_publication_t pub,
                        uint64_t ref)
{
    /* Production allocation links the slot; the sweeps walk that list. */
    rx_occ_link(s, rx_slot);
    moq_rx_stream_t *rx = &s->rx_streams[rx_slot];
    rx->active     = true;
    rx->pub_handle = pub;
    rx->sub        = MOQ_SUBSCRIPTION_INVALID;
    rx->stream_ref = moq_stream_ref_from_u64(ref);
    moq_index_insert(s->idx_rx_by_ref, s->idx_rx_mask, ref, (int)rx_slot);
}

/* An rx stream parked awaiting an unresolved alias: what
 * session_discard_deferred_streams() looks for. */
static void bind_rx_deferred(moq_session_t *s, size_t rx_slot, uint64_t ref)
{
    /* Production allocation links the slot; the sweeps walk that list. */
    rx_occ_link(s, rx_slot);
    moq_rx_stream_t *rx = &s->rx_streams[rx_slot];
    rx->active      = true;
    rx->sub         = MOQ_SUBSCRIPTION_INVALID;
    rx->pub_handle  = MOQ_PUBLICATION_INVALID;
    rx->stream_ref  = moq_stream_ref_from_u64(ref);
    rx->parse_state = MOQ_RX_DEFERRED_ALIAS;
    moq_index_insert(s->idx_rx_by_ref, s->idx_rx_mask, ref, (int)rx_slot);
}

static void fill_action_queue(moq_session_t *s)
{
    while (!action_queue_full(s)) {
        moq_action_t a;
        memset(&a, 0, sizeof(a));
        a.kind = MOQ_ACTION_STOP_DATA;
        a.detail_size = (uint32_t)sizeof(moq_stop_data_action_t);
        a.borrow_epoch = s->borrow_epoch;
        a.u.stop_data.stream_ref = moq_stream_ref_from_u64(0xB000);
        if (push_action(s, &a) < 0) break;
    }
}

static void fill_event_queue(moq_session_t *s)
{
    while (!event_queue_full(s)) {
        moq_event_t e;
        memset(&e, 0, sizeof(e));
        e.kind = MOQ_EVENT_PUBLISH_UNSUBSCRIBED;
        e.detail_size = (uint32_t)sizeof(moq_publish_unsubscribed_event_t);
        e.borrow_epoch = s->borrow_epoch;
        if (push_event(s, &e) < 0) break;
    }
}

/* ================= capacity axes ================================== */

/*
 * FOUR orthogonal axes. Each varies ONE configured capacity and pins the rest,
 * so a correction that fixed only the sub/pub reaps would still be caught by
 * the subgroup, receive-stream and fetch axes. No Cartesian product: each axis
 * carries the same fixed live model and its own exact relational oracle.
 */
typedef struct { const char *axis; caps_t c; char tag[48]; } row_t;

#define ROWS_MAX 20

static size_t build_rows(row_t *rows)
{
    static const uint32_t SUBS[3] = { 8u, 64u, 512u };
    static const uint32_t PUBS[3] = { 4u, 16u, 256u };
    static const uint32_t SGS[3]  = { 8u, 64u, 512u };
    static const uint32_t RXS[3]  = { 8u, 64u, 512u };
    static const uint32_t FTS[3]  = { 4u, 16u, 256u };
    size_t n = 0;
    for (size_t i = 0; i < 3; i++)
        for (size_t j = 0; j < 3; j++) {
            caps_default(&rows[n].c);
            rows[n].c.sub_cap = SUBS[i];
            rows[n].c.pub_cap = PUBS[j];
            rows[n].axis = "sub/pub";
            snprintf(rows[n].tag, sizeof(rows[n].tag), "subs=%-4u pubs=%-4u",
                     SUBS[i], PUBS[j]);
            n++;
        }
    for (size_t i = 0; i < 3; i++) {
        caps_default(&rows[n].c);
        rows[n].c.sg_cap = SGS[i];
        rows[n].axis = "sg";
        snprintf(rows[n].tag, sizeof(rows[n].tag), "sg=%-4u", SGS[i]);
        n++;
    }
    for (size_t i = 0; i < 3; i++) {
        caps_default(&rows[n].c);
        rows[n].c.rx_cap = RXS[i];
        rows[n].axis = "rx";
        snprintf(rows[n].tag, sizeof(rows[n].tag), "rx=%-4u", RXS[i]);
        n++;
    }
    for (size_t i = 0; i < 3; i++) {
        caps_default(&rows[n].c);
        rows[n].c.fetch_cap = FTS[i];
        rows[n].axis = "fetch";
        snprintf(rows[n].tag, sizeof(rows[n].tag), "fetch=%-4u", FTS[i]);
        n++;
    }
    return n;   /* 18 */
}

/* The smallest value on each axis: slot placement must be expressible at every
 * row of that axis, so the live owner set stays identical across the axis. */
#define MIN_SUB 8u
#define MIN_PUB 4u
#define MIN_SG  8u
#define MIN_RX  8u

/* ================= phases ========================================= */

/* ================= the declared WORK MODEL ======================== */

/*
 * The absolute half of the work image. Configured-capacity counters stay
 * RELATIONAL (the comparator) so no post-fix constant is baked in; everything
 * else -- charged transitions, deferred owners encountered, every non-capacity
 * loop site and both control loops -- is DECLARED per phase from the fixture's
 * live set and checked exactly.
 *
 * Derivation rules, applied by every case:
 *   catchup           = one per advancing call in the phase (a fresh sweep);
 *                       an inherited cursor would add one.
 *   rx_rescan         = per owner that enters STOP_STREAMS: 1, plus 1 more if
 *                       that owner actually stopped a stream (the scan rescans
 *                       to zero after a successful stop).
 *   index_find        = one per moq_index_find call the declared owners imply.
 *   index_rm_search   = one per moq_index_remove ATTEMPT, present or absent.
 *   index_rm_backshift= at least one iteration per PRESENT-key removal; an
 *                       absent-key removal never enters the loop at all.
 *   queued_action/_event = the declared queue occupancy at a close.
 *   tomb/auth_staging = 0 unless the case declares the work.
 */
typedef struct {
    int      declared;
    uint64_t sg_charged, rx_charged, pub_pending, sub_pending;
    uint64_t queued_action, queued_event, tomb, auth_staging;
    uint64_t staged;          /* fixed-size ring: an exact declared visit count */
    /*
     * Index loops are bounded by the DECLARED live edge/operation set, not by
     * capacity: `idx_ops` is the number of registry/index operations the
     * phase's declared owners imply, and each loop must walk at least one cell
     * per operation and at most IDX_CHAIN_MAX. Placement inside the table is
     * not this test's contract; the capacity comparator separately proves the
     * counts do not follow the table size.
     */
    /*
     * Three DISTINCT operation sets, because they are not the same set:
     *   idx_find_ops    - every moq_index_find call: a by-id registry lookup
     *                     (request_registry_remove_by_id does one before its
     *                     removal, present or not) and an rx-by-ref lookup;
     *   idx_remove_ops  - every moq_index_remove ATTEMPT, present or absent;
     *   idx_present_rm  - only the attempts whose key is PRESENT. An absent
     *                     removal searches and returns without ever entering
     *                     the backshift loop, so it cannot bound backshift.
     */
    uint64_t idx_find_ops;
    uint64_t idx_remove_ops;
    uint64_t idx_present_rm;
    uint64_t catchup, rx_rescan;
} model_t;

#define IDX_CHAIN_MAX 8u

static void model_reset(model_t *m) { memset(m, 0, sizeof(*m)); m->declared = 1; }

/*
 * Core diff: returns the name of the first member that violates the declared
 * model, or NULL. Split out so the self-check can probe it quietly.
 * `undeclared` is reported as its own named failure rather than passing.
 */
static const char *model_diff(const model_t *w, const work_t *g,
                              char *buf, size_t cap)
{
    if (!w->declared) { snprintf(buf, cap, "MODEL_UNDECLARED"); return buf; }
#define MD(f)                                                                \
    do { if (w->f != g->f) {                                                 \
        snprintf(buf, cap, "%s (expected %llu, observed %llu)", #f,          \
                 (unsigned long long)w->f, (unsigned long long)g->f);        \
        return buf;                                                          \
    } } while (0)
    MD(sg_charged); MD(rx_charged); MD(pub_pending); MD(sub_pending);
    MD(queued_action); MD(queued_event); MD(tomb); MD(auth_staging);
    MD(catchup); MD(rx_rescan);
    /*
     * The declared operation set is EXACT against the invocation counters:
     * `idx_find_ops` is how many times moq_index_find is called and
     * `idx_remove_ops` how many times moq_index_remove is ATTEMPTED. Probe
     * STEPS stay bounded below, because hash placement is not this test's
     * contract -- but the call count is, and it is checked by equality.
     */
    if (w->idx_find_ops != g->index_find_calls) {
        snprintf(buf, cap, "index_find_calls (expected %llu, observed %llu)",
                 (unsigned long long)w->idx_find_ops,
                 (unsigned long long)g->index_find_calls);
        return buf;
    }
    if (w->idx_remove_ops != g->index_rm_calls) {
        snprintf(buf, cap, "index_rm_calls (expected %llu, observed %llu)",
                 (unsigned long long)w->idx_remove_ops,
                 (unsigned long long)g->index_rm_calls);
        return buf;
    }
    /* The staged ring's slot count is a compile-time constant, so its visit
     * count is an ABSOLUTE declaration, not merely a cross-capacity relation. */
    MD(staged);
#undef MD
#define MB(f, lo, hi)                                                        \
    do { if (g->f < (lo) || g->f > (hi)) {                                   \
        snprintf(buf, cap, "%s = %llu, declared bound [%llu,%llu] from the "  \
                 "live edge set", #f, (unsigned long long)g->f,              \
                 (unsigned long long)(lo), (unsigned long long)(hi));        \
        return buf;                                                          \
    } } while (0)
    MB(index_find, w->idx_find_ops, w->idx_find_ops * IDX_CHAIN_MAX);
    MB(index_rm_search, w->idx_remove_ops, w->idx_remove_ops * IDX_CHAIN_MAX);
    /* Only a PRESENT key's removal enters the backshift loop -- at least once,
     * to inspect the cell after the hole; a displaced chain relocates. */
    MB(index_rm_backshift, w->idx_present_rm,
       w->idx_present_rm * IDX_CHAIN_MAX);
#undef MB
    return NULL;
}

static int model_check(const model_t *w, const work_t *g, const char *where)
{
    int failures = 0;
    char buf[192];
    const char *d = model_diff(w, g, buf, sizeof(buf));
    if (d) {
        printf("      MODEL MISMATCH (%s): %s\n", where, d);
        MOQ_TEST_CHECK(d == NULL);
    }
    return failures;
}

#define PHASE_MAX 4
typedef struct {
    const char *name;
    work_t  w;
    out_t   got;
    state_t st;
} phase_t;

/* ================= cases ========================================== */

enum {
    C_IDLE = 0, C_OCCUPIED, C_FUTURE, C_READY_PUB, C_READY_SUB, C_READY_MULTI,
    C_BLOCKED_RETRY, C_EXACT_ONCE, C_RX_STOP, C_FREE_REUSE, C_SCRATCH_CLOSE,
    C_FWD_PENDING, C_DEFERRED_DISCARD,
    C_STAGED_DISCARD, C_COUNT
};

static const char *case_name(int c)
{
    switch (c) {
    case C_IDLE:          return "idle_empty";
    case C_OCCUPIED:      return "occupied_no_terminal";
    case C_FUTURE:        return "future_deferred";
    case C_READY_PUB:     return "ready_pub";
    case C_READY_SUB:     return "ready_sub";
    case C_READY_MULTI:   return "ready_both_multi";
    case C_BLOCKED_RETRY: return "blocked_then_retry";
    case C_EXACT_ONCE:    return "exact_once";
    case C_RX_STOP:       return "rx_bound_stop";
    case C_FREE_REUSE:    return "free_then_slot_reuse";
    case C_SCRATCH_CLOSE: return "finalize_scratch_close";
    case C_FWD_PENDING:   return "fwd_pending_retains";
    case C_DEFERRED_DISCARD: return "deferred_stream_discard";
    case C_STAGED_DISCARD:return "staged_selective_discard";
    }
    return "?";
}

/* Per-case capacity overrides. Constant across every row of every axis, so
 * they can never explain a cross-capacity difference. */
static void case_caps(int case_id, caps_t *c)
{
    if (case_id == C_BLOCKED_RETRY)
        c->max_actions = 2;    /* the refused phase's filler set stays declarable */
    if (case_id == C_SCRATCH_CLOSE)
        c->scratch = 16;       /* below the reason: a PERMANENT shortfall */
}

typedef struct {
    size_t p0, p1;             /* publication slots */
    size_t u0, u1, u2;         /* subscription slots */
    size_t g0, g1;             /* subgroup slots */
    size_t r0, r1;             /* rx slots */
} place_t;

static int place_build(place_t *pl, uint32_t seed, const caps_t *c, int *failures)
{
    size_t pp[MIN_PUB], up[MIN_SUB], gp[MIN_SG], rp[MIN_RX];
    seeded_perm(seed, MIN_PUB, pp);
    seeded_perm(seed + 1u, MIN_SUB, up);
    seeded_perm(seed + 2u, MIN_SG, gp);
    seeded_perm(seed + 3u, MIN_RX, rp);
    pl->p0 = pp[0]; pl->p1 = pp[1];
    pl->u0 = up[0]; pl->u1 = up[1]; pl->u2 = up[2];
    pl->g0 = gp[0]; pl->g1 = gp[1];
    pl->r0 = rp[0]; pl->r1 = rp[1];
    return slot_ok(pl->p0, c->pub_cap, "pub", failures) &&
           slot_ok(pl->p1, c->pub_cap, "pub", failures) &&
           slot_ok(pl->u0, c->sub_cap, "sub", failures) &&
           slot_ok(pl->u1, c->sub_cap, "sub", failures) &&
           slot_ok(pl->u2, c->sub_cap, "sub", failures) &&
           slot_ok(pl->g0, c->sg_cap, "sg", failures) &&
           slot_ok(pl->g1, c->sg_cap, "sg", failures) &&
           slot_ok(pl->r0, c->rx_cap, "rx", failures) &&
           slot_ok(pl->r1, c->rx_cap, "rx", failures);
}

/* Execute one scheduled setup operation. Handles are predictable before the
 * op runs (a fresh entry's live generation is 1), so a binding op does not
 * depend on its owner's op having run first -- which is what lets the whole
 * commuting set be shuffled. */
static void op_apply(moq_session_t *s, const op_t *o, uint64_t now)
{
    switch (o->kind) {
    case OP_ARM_PUB:          arm_ready_pub(s, o->a, o->b); break;
    case OP_ARM_SUB:          arm_ready_sub(s, o->a, o->b); break;
    case OP_ARM_LIVE_PUB:     arm_live_pub(s, o->a, o->b); break;
    case OP_ARM_LIVE_SUB:     arm_live_sub(s, o->a, o->b); break;
    case OP_ARM_FUTURE_PUB:   arm_future_pub(s, o->a, now, o->b); break;
    case OP_ARM_FUTURE_SUB:   arm_future_sub(s, o->a, now, o->b); break;
    case OP_ARM_CLOSING_SG:   sweep_arm_closing_subgroup(s, o->a); break;
    case OP_ARM_RESETTING_SG:
        sg_occ_link(s, o->a);
        s->subgroups[o->a].state = MOQ_SG_RESETTING;
        s->subgroups[o->a].delivery_deadline_us = UINT64_MAX;
        break;
    case OP_BIND_RX: {
        /* Predictable without the owner's op having run: a fresh pool entry's
         * live generation is 1, which is what keeps the whole set commuting. */
        moq_publication_t pub = { moq_handle_pack(MOQ_HANDLE_POOL_PUBLICATION,
                                    s->session_tag, 1u, (uint32_t)o->b) };
        bind_rx_pub(s, o->a, pub, RX_REF_BASE + o->a);
        break;
    }
    case OP_ARM_DEFERRED_RX:  bind_rx_deferred(s, o->a, RX_REF_BASE + o->a); break;
    }
}

static uint64_t pub_handle_at(moq_session_t *s, size_t slot)
{
    return moq_handle_pack(MOQ_HANDLE_POOL_PUBLICATION, s->session_tag,
                           s->publishes[slot].generation, (uint32_t)slot);
}

static uint64_t sub_handle_at(moq_session_t *s, size_t slot)
{
    return moq_handle_pack(MOQ_HANDLE_POOL_SUBSCRIPTION, s->session_tag,
                           s->subs[slot].generation, (uint32_t)slot);
}

static void st_pub_retired(state_t *v, const char *pfx, moq_session_t *s,
                           size_t slot)
{
    if (slot >= s->pub_cap) { st_addp(v, pfx, "OUT_OF_RANGE", 1); return; }
    const moq_pub_entry_t *e = &s->publishes[slot];
    st_addp(v, pfx, "state",      (uint64_t)e->state);
    st_addp(v, pfx, "gen",        e->generation);
    st_addp(v, pfx, "dn_pend",    e->done_pending ? 1u : 0u);
    st_addp(v, pfx, "dn_exp",     e->done_expired ? 1u : 0u);
    st_addp(v, pfx, "dn_status",  e->done_status_code);
    st_addp(v, pfx, "dn_count",   e->done_stream_count);
    st_addp(v, pfx, "dn_proc",    e->processed_stream_count);
    st_addp(v, pfx, "dn_deadline", e->done_deadline_us);
    st_addp(v, pfx, "dn_rlen",    (uint64_t)e->done_reason_len);
    st_addp(v, pfx, "dn_rbuf",    e->done_reason_buf ? 1u : 0u);
}

static void st_sub_retired(state_t *v, const char *pfx, moq_session_t *s,
                           size_t slot)
{
    if (slot >= s->sub_cap) { st_addp(v, pfx, "OUT_OF_RANGE", 1); return; }
    const moq_sub_entry_t *e = &s->subs[slot];
    st_addp(v, pfx, "state",      (uint64_t)e->state);
    st_addp(v, pfx, "gen",        e->generation);
    st_addp(v, pfx, "dn_pend",    e->done_pending ? 1u : 0u);
    st_addp(v, pfx, "dn_exp",     e->done_expired ? 1u : 0u);
    st_addp(v, pfx, "dn_status",  e->done_status_code);
    st_addp(v, pfx, "dn_count",   e->done_stream_count);
    st_addp(v, pfx, "dn_proc",    e->processed_stream_count);
    st_addp(v, pfx, "dn_deadline", e->done_deadline_us);
    st_addp(v, pfx, "dn_rlen",    (uint64_t)e->done_reason_len);
    st_addp(v, pfx, "dn_rbuf",    e->done_reason_buf ? 1u : 0u);
}

static void st_sg_retired(state_t *v, const char *pfx, moq_session_t *s,
                          size_t slot)
{
    if (slot >= s->sg_cap) { st_addp(v, pfx, "OUT_OF_RANGE", 1); return; }
    const moq_sg_entry_t *e = &s->subgroups[slot];
    st_addp(v, pfx, "state",    (uint64_t)e->state);
    st_addp(v, pfx, "gen",      e->generation);
    st_addp(v, pfx, "sub",      e->sub._opaque);
    st_addp(v, pfx, "pub",      e->pub._opaque);
    st_addp(v, pfx, "deadline", e->delivery_deadline_us);
    st_addp(v, pfx, "spay_len", e->streaming_payload_len);
    st_addp(v, pfx, "swritten", e->streaming_bytes_written);
}

static void st_fetch_retired(state_t *v, const char *pfx, moq_session_t *s,
                             size_t slot)
{
    if (slot >= s->fetch_cap) { st_addp(v, pfx, "OUT_OF_RANGE", 1); return; }
    const moq_fetch_entry_t *e = &s->fetches[slot];
    st_addp(v, pfx, "state",   (uint64_t)e->state);
    st_addp(v, pfx, "gen",     e->generation);
    st_addp(v, pfx, "tok_n",   (uint64_t)e->join_token_count);
    st_addp(v, pfx, "tok0",    e->join_tokens[0].token_value.data ? 1u : 0u);
    st_addp(v, pfx, "tok1",    e->join_tokens[1].token_value.data ? 1u : 0u);
}

/*
 * Complete, PREFIXED, bounds-safe owner inventories. Every owner a case
 * declares gets its own inventory under its own symbolic prefix, captured
 * before and after every phase, so a case with several owners cannot check one
 * and ignore the rest.
 */
/*
 * The retained deferred reason's BYTES, packed into four words and compared
 * exactly. Length alone plus a pointer-presence flag would let an in-place
 * byte change pass while the owner is still retained. Over-long input marks
 * the slot OUT_OF_RANGE rather than silently comparing a prefix.
 */
#define REASON_WORDS 4
static void st_reason_bytes(state_t *v, const char *pfx, const uint8_t *buf,
                            size_t len)
{
    if (len > REASON_WORDS * 8u) { st_addp(v, pfx, "dn_rOVERSIZE", 1); return; }
    for (size_t w = 0; w < REASON_WORDS; w++) {
        uint64_t acc = 0;
        for (size_t b = 0; b < 8; b++) {
            size_t idx = w * 8 + b;
            uint64_t byte = (buf && idx < len) ? buf[idx] : 0u;
            acc |= byte << (8 * b);
        }
        char k[8]; snprintf(k, sizeof(k), "dn_r%zu", w);
        st_addp(v, pfx, k, acc);
    }
}

static void st_expect_reason_bytes(state_t *v, const char *pfx, const char *txt)
{
    st_reason_bytes(v, pfx, (const uint8_t *)txt, txt ? strlen(txt) : 0);
}

/*
 * The inventory SHAPE follows the entry's own state: a FREE entry emits the
 * retired contract subset, a live one the full inventory. That is not adopting
 * state -- the expectation independently declares which shape it expects, and
 * st_check() compares field NAMES, so an entry that should have been retired
 * and was not fails on the very first name.
 */
static void st_pub(state_t *v, const char *pfx, moq_session_t *s, size_t slot)
{
    if (slot >= s->pub_cap) { st_addp(v, pfx, "OUT_OF_RANGE", 1); return; }
    if (s->publishes[slot].state == MOQ_PUB_FREE) {
        st_pub_retired(v, pfx, s, slot); return;
    }
    const moq_pub_entry_t *e = &s->publishes[slot];
    st_addp(v, pfx, "state",      (uint64_t)e->state);
    st_addp(v, pfx, "role",       (uint64_t)e->role);
    st_addp(v, pfx, "gen",        e->generation);
    st_addp(v, pfx, "handle",     e->handle._opaque);
    st_addp(v, pfx, "req_id",     e->request_id);
    st_addp(v, pfx, "req_ref",    e->request_stream_ref._v);
    st_addp(v, pfx, "alias",      e->track_alias);
    st_addp(v, pfx, "send_ok",    e->send_allowed ? 1u : 0u);
    st_addp(v, pfx, "dn_pend",    e->done_pending ? 1u : 0u);
    st_addp(v, pfx, "dn_exp",     e->done_expired ? 1u : 0u);
    st_addp(v, pfx, "dn_status",  e->done_status_code);
    st_addp(v, pfx, "dn_count",   e->done_stream_count);
    st_addp(v, pfx, "dn_proc",    e->processed_stream_count);
    st_addp(v, pfx, "dn_deadline", e->done_deadline_us);
    st_addp(v, pfx, "dn_rlen",    (uint64_t)e->done_reason_len);
    st_addp(v, pfx, "dn_rbuf",    e->done_reason_buf ? 1u : 0u);
    st_reason_bytes(v, pfx, e->done_reason_buf, e->done_reason_len);
}

static void st_sub(state_t *v, const char *pfx, moq_session_t *s, size_t slot)
{
    if (slot >= s->sub_cap) { st_addp(v, pfx, "OUT_OF_RANGE", 1); return; }
    if (s->subs[slot].state == MOQ_SUB_FREE) {
        st_sub_retired(v, pfx, s, slot); return;
    }
    const moq_sub_entry_t *e = &s->subs[slot];
    st_addp(v, pfx, "state",      (uint64_t)e->state);
    st_addp(v, pfx, "role",       (uint64_t)e->role);
    st_addp(v, pfx, "gen",        e->generation);
    st_addp(v, pfx, "handle",     e->handle._opaque);
    st_addp(v, pfx, "req_id",     e->request_id);
    st_addp(v, pfx, "req_ref",    e->request_stream_ref._v);
    st_addp(v, pfx, "alias",      e->track_alias);
    st_addp(v, pfx, "forward",    e->forward ? 1u : 0u);
    st_addp(v, pfx, "dn_pend",    e->done_pending ? 1u : 0u);
    st_addp(v, pfx, "dn_exp",     e->done_expired ? 1u : 0u);
    st_addp(v, pfx, "dn_status",  e->done_status_code);
    st_addp(v, pfx, "dn_count",   e->done_stream_count);
    st_addp(v, pfx, "dn_proc",    e->processed_stream_count);
    st_addp(v, pfx, "dn_deadline", e->done_deadline_us);
    st_addp(v, pfx, "dn_rlen",    (uint64_t)e->done_reason_len);
    st_addp(v, pfx, "dn_rbuf",    e->done_reason_buf ? 1u : 0u);
    st_reason_bytes(v, pfx, e->done_reason_buf, e->done_reason_len);
}

static void st_rx(state_t *v, const char *pfx, moq_session_t *s, size_t slot)
{
    if (slot >= s->rx_cap) { st_addp(v, pfx, "OUT_OF_RANGE", 1); return; }
    const moq_rx_stream_t *rx = &s->rx_streams[slot];
    st_addp(v, pfx, "active",  rx->active ? 1u : 0u);
    st_addp(v, pfx, "ref",     rx->stream_ref._v);
    st_addp(v, pfx, "sub",     rx->sub._opaque);
    st_addp(v, pfx, "pub",     rx->pub_handle._opaque);
    st_addp(v, pfx, "parse",   (uint64_t)rx->parse_state);
    st_addp(v, pfx, "in_len",  (uint64_t)rx->input_len);
    st_addp(v, pfx, "pay_buf", rx->payload_rcbuf ? 1u : 0u);
}

static void st_sg(state_t *v, const char *pfx, moq_session_t *s, size_t slot)
{
    if (slot >= s->sg_cap) { st_addp(v, pfx, "OUT_OF_RANGE", 1); return; }
    if (s->subgroups[slot].state == MOQ_SG_FREE) {
        st_sg_retired(v, pfx, s, slot); return;
    }
    const moq_sg_entry_t *e = &s->subgroups[slot];
    st_addp(v, pfx, "state",    (uint64_t)e->state);
    st_addp(v, pfx, "gen",      e->generation);
    st_addp(v, pfx, "ref",      e->stream_ref._v);
    st_addp(v, pfx, "sub",      e->sub._opaque);
    st_addp(v, pfx, "pub",      e->pub._opaque);
    st_addp(v, pfx, "deadline", e->delivery_deadline_us);
    st_addp(v, pfx, "spay_len", e->streaming_payload_len);
    st_addp(v, pfx, "swritten", e->streaming_bytes_written);
}

/* The complete persisted sweep cursor -- every field a resume reads, not just
 * the four the previous inventory sampled. */
static void st_observe_cursor(state_t *v, moq_session_t *s)
{
    st_add(v, "sweep.active",  s->sweep_active ? 1u : 0u);
    st_add(v, "sweep.stage",   (uint64_t)s->sweep_stage);
    st_add(v, "sweep.phase",   (uint64_t)s->sweep_phase);
    st_add(v, "sweep.slot",    (uint64_t)s->sweep_slot);
    st_add(v, "sweep.rx_pos",  (uint64_t)s->sweep_rx_pos);
    st_add(v, "sweep.rx_found", s->sweep_rx_found ? 1u : 0u);
    st_add(v, "sweep.reaped",  s->sweep_reaped_subgroup ? 1u : 0u);
    st_add(v, "sweep.now",     s->sweep_now_us);
    st_add(v, "sess.state",    (uint64_t)s->state);
}

/*
 * A RETIRED owner's contract, and only its contract.
 *
 * The private residue a free path happens to leave -- a stale subscription
 * handle and request id, a stale subgroup stream ref -- is deliberately NOT
 * pinned: doing so would turn a future cleanup improvement into a test
 * failure. What a retired owner owes is FREE state, an advanced generation,
 * cleared deferred-terminal state, no resolvable handle and no surviving graph
 * edge -- the last two asserted separately by check_retired(). Identity bytes
 * the free path may or may not scrub are deliberately OUT of the contract.
 */


static void st_expect_pub_free(state_t *v, const char *pfx, uint32_t live_gen)
{
    st_addp(v, pfx, "state",      (uint64_t)MOQ_PUB_FREE);
    st_addp(v, pfx, "gen",        live_gen + 1u);
    st_addp(v, pfx, "dn_pend",    0);
    st_addp(v, pfx, "dn_exp",     0);
    st_addp(v, pfx, "dn_status",  0);
    st_addp(v, pfx, "dn_count",   0);
    st_addp(v, pfx, "dn_proc",    0);
    st_addp(v, pfx, "dn_deadline", 0);
    st_addp(v, pfx, "dn_rlen",    0);
    st_addp(v, pfx, "dn_rbuf",    0);
}

static void st_expect_sub_free(state_t *v, const char *pfx, uint32_t live_gen)
{
    st_addp(v, pfx, "state",      (uint64_t)MOQ_SUB_FREE);
    st_addp(v, pfx, "gen",        live_gen + 1u);
    st_addp(v, pfx, "dn_pend",    0);
    st_addp(v, pfx, "dn_exp",     0);
    st_addp(v, pfx, "dn_status",  0);
    st_addp(v, pfx, "dn_count",   0);
    st_addp(v, pfx, "dn_proc",    0);
    st_addp(v, pfx, "dn_deadline", 0);
    st_addp(v, pfx, "dn_rlen",    0);
    st_addp(v, pfx, "dn_rbuf",    0);
}

/*
 * The EXACT declared edge set of a LIVE owner, asserted BEFORE its measured
 * phase. check_retired() alone proves only that keys vanish during the free --
 * a wrong endpoint kind or a wrong target could exist for the whole live phase
 * and disappear by key anyway. og_check_owner_edges() additionally proves that
 * nothing else anywhere points at this owner.
 */
static int check_live_edge(moq_session_t *s, int kind, size_t slot,
                           og_domain_t domain, uint64_t key, const char *what)
{
    int failures = 0;
    og_graph_t g;
    og_capture(s, &g);
    failures += og_check_integrity(&g, what);
    failures += og_check_edge(&g, domain, key, kind, (int)slot, what);
    {
        og_edge_spec_t want = { domain, key };
        failures += og_check_owner_edges(&g, kind, (int)slot, &want, 1, what);
    }
    return failures;
}

/*
 * The rest of a retirement, which no field comparison can express: the handle
 * no longer resolves, the registry holds no key for it, and nothing anywhere
 * still points at the slot.
 */
static int check_retired(moq_session_t *s, int kind, size_t slot,
                         uint64_t handle, uint64_t req_id, const char *what)
{
    int failures = 0;
    og_graph_t g;
    og_capture(s, &g);
    failures += og_check_integrity(&g, what);
    failures += og_check_no_edge(&g, OG_DOM_REQ_RID, req_id, what);
    failures += og_check_owner_unreferenced(&g, kind, (int)slot, what);
    if (kind == MOQ_REQ_PUBLISH) {
        moq_publication_t h = { handle };
        MOQ_TEST_CHECK(pub_resolve_handle(s, h) < 0);
    } else if (kind == MOQ_REQ_SUBSCRIPTION) {
        moq_subscription_t h = { handle };
        MOQ_TEST_CHECK(sub_resolve_handle(s, h) < 0);
    } else if (kind == MOQ_REQ_FETCH) {
        moq_fetch_t h = { handle };
        MOQ_TEST_CHECK(fetch_resolve_handle(s, h) < 0);
    }
    return failures;
}

/*
 * The EXACT drain multiset: precisely the declared refs, once each, and nothing
 * else. A count alone would pass on the wrong ref; a membership test alone
 * would pass on a duplicate.
 */
static int check_drain_set(moq_session_t *s, const uint64_t *refs, size_t n,
                           const char *what)
{
    int failures = 0;
    if (s->drain_ref_count != n) {
        printf("      DRAIN MISMATCH (%s): expected %zu refs, observed %zu\n",
               what, n, s->drain_ref_count);
        MOQ_TEST_CHECK(s->drain_ref_count == n);
        return failures;
    }
    for (size_t i = 0; i < n; i++) {
        size_t seen = 0;
        for (size_t d = 0; d < s->drain_ref_count; d++)
            if (s->drain_refs[d] == refs[i]) seen++;
        if (seen != 1) {
            printf("      DRAIN MISMATCH (%s): ref %llu present %zu times, "
                   "expected exactly once\n", what,
                   (unsigned long long)refs[i], seen);
            MOQ_TEST_CHECK(seen == 1);
        }
    }
    return failures;
}

/*
 * Retirement of a STREAM-CORRELATED owner: everything check_retired() proves,
 * plus the absence of its request-stream key. A by-id-only check would pass on
 * a draft-18 owner that never held a by-id key at all.
 */
static int check_retired_streamref(moq_session_t *s, int kind, size_t slot,
                                   uint64_t handle, uint64_t req_id,
                                   uint64_t req_ref, const char *what)
{
    int failures = check_retired(s, kind, slot, handle, req_id, what);
    og_graph_t g;
    og_capture(s, &g);
    failures += og_check_no_edge(&g, OG_DOM_REQ_STREAMREF, req_ref, what);
    return failures;
}

/* A live deferred owner, declared from the fixture's own arming values. */
static void st_expect_pub_ready(state_t *v, const char *pfx, uint32_t gen,
                                uint64_t handle, uint64_t req_id, int expired)
{
    st_addp(v, pfx, "state",      (uint64_t)MOQ_PUB_ESTABLISHED);
    st_addp(v, pfx, "role",       (uint64_t)MOQ_PUB_ROLE_SUBSCRIBER);
    st_addp(v, pfx, "gen",        gen);
    st_addp(v, pfx, "handle",     handle);
    st_addp(v, pfx, "req_id",     req_id);
    st_addp(v, pfx, "req_ref",    0);
    st_addp(v, pfx, "alias",      0);
    st_addp(v, pfx, "send_ok",    0);
    st_addp(v, pfx, "dn_pend",    1);
    st_addp(v, pfx, "dn_exp",     (uint64_t)(expired ? 1 : 0));
    st_addp(v, pfx, "dn_status",  PUB_STATUS);
    st_addp(v, pfx, "dn_count",   PUB_SC);
    st_addp(v, pfx, "dn_proc",    0);
    st_addp(v, pfx, "dn_deadline", 0);
    st_addp(v, pfx, "dn_rlen",    (uint64_t)strlen(PUB_REASON));
    st_addp(v, pfx, "dn_rbuf",    1);
    st_expect_reason_bytes(v, pfx, PUB_REASON);
}

static void st_expect_sub_ready(state_t *v, const char *pfx, uint32_t gen,
                                uint64_t handle, uint64_t req_id, int expired)
{
    st_addp(v, pfx, "state",      (uint64_t)MOQ_SUB_ESTABLISHED);
    st_addp(v, pfx, "role",       (uint64_t)MOQ_SUB_ROLE_SUBSCRIBER);
    st_addp(v, pfx, "gen",        gen);
    st_addp(v, pfx, "handle",     handle);
    st_addp(v, pfx, "req_id",     req_id);
    st_addp(v, pfx, "req_ref",    0);
    st_addp(v, pfx, "alias",      0);
    st_addp(v, pfx, "forward",    0);
    st_addp(v, pfx, "dn_pend",    1);
    st_addp(v, pfx, "dn_exp",     (uint64_t)(expired ? 1 : 0));
    st_addp(v, pfx, "dn_status",  SUB_STATUS);
    st_addp(v, pfx, "dn_count",   SUB_SC);
    st_addp(v, pfx, "dn_proc",    0);
    st_addp(v, pfx, "dn_deadline", 0);
    st_addp(v, pfx, "dn_rlen",    (uint64_t)strlen(SUB_REASON));
    st_addp(v, pfx, "dn_rbuf",    1);
    st_expect_reason_bytes(v, pfx, SUB_REASON);
}

static void st_expect_rx_free(state_t *v, const char *pfx)
{
    st_addp(v, pfx, "active",  0);
    st_addp(v, pfx, "ref",     0);
    st_addp(v, pfx, "sub",     0);
    st_addp(v, pfx, "pub",     0);
    st_addp(v, pfx, "parse",   0);
    st_addp(v, pfx, "in_len",  0);
    st_addp(v, pfx, "pay_buf", 0);
}

static void st_expect_rx_bound_pub(state_t *v, const char *pfx, uint64_t ref,
                                   uint64_t pub_handle, uint64_t parse)
{
    st_addp(v, pfx, "active",  1);
    st_addp(v, pfx, "ref",     ref);
    st_addp(v, pfx, "sub",     0);
    st_addp(v, pfx, "pub",     pub_handle);
    st_addp(v, pfx, "parse",   parse);
    st_addp(v, pfx, "in_len",  0);
    st_addp(v, pfx, "pay_buf", 0);
}

/* A retired subgroup owes FREE state, an advanced generation, cleared
 * ownership bindings and a disarmed deadline. Whether the free path also
 * scrubs the stream ref is deliberately NOT pinned here. */
/* An armed-but-not-yet-reaped subgroup: state, generation and ref retained. */
static void st_expect_sg_armed(state_t *v, const char *pfx, uint64_t state,
                               uint32_t gen, uint64_t ref, uint64_t sub)
{
    st_addp(v, pfx, "state",    state);
    st_addp(v, pfx, "gen",      gen);
    st_addp(v, pfx, "ref",      ref);
    st_addp(v, pfx, "sub",      sub);
    st_addp(v, pfx, "pub",      0);
    st_addp(v, pfx, "deadline", UINT64_MAX);
    st_addp(v, pfx, "spay_len", 0);
    st_addp(v, pfx, "swritten", 0);
}

/* A retired subgroup owes FREE state, an advanced generation, cleared
 * ownership bindings and a disarmed deadline. Whether the free path also
 * scrubs the stream ref is not a contract this prerequisite pins. */

static void st_expect_sg_free(state_t *v, const char *pfx, uint32_t live_gen)
{
    st_addp(v, pfx, "state",    (uint64_t)MOQ_SG_FREE);
    st_addp(v, pfx, "gen",      live_gen + 1u);
    st_addp(v, pfx, "sub",      0);
    st_addp(v, pfx, "pub",      0);
    st_addp(v, pfx, "deadline", UINT64_MAX);
    st_addp(v, pfx, "spay_len", 0);
    st_addp(v, pfx, "swritten", 0);
}

/* A live, occupied, non-deferred owner. */
static void st_expect_pub_live(state_t *v, const char *pfx, uint32_t gen,
                               uint64_t handle, uint64_t req_id)
{
    st_addp(v, pfx, "state",  (uint64_t)MOQ_PUB_ESTABLISHED);
    st_addp(v, pfx, "role",   (uint64_t)MOQ_PUB_ROLE_SUBSCRIBER);
    st_addp(v, pfx, "gen",    gen);
    st_addp(v, pfx, "handle", handle);
    st_addp(v, pfx, "req_id", req_id);
    st_addp(v, pfx, "req_ref", 0);
    st_addp(v, pfx, "alias",  0);
    st_addp(v, pfx, "send_ok", 0);
    st_addp(v, pfx, "dn_pend", 0);
    st_addp(v, pfx, "dn_exp",  0);
    st_addp(v, pfx, "dn_status", 0);
    st_addp(v, pfx, "dn_count", 0);
    st_addp(v, pfx, "dn_proc", 0);
    st_addp(v, pfx, "dn_deadline", 0);
    st_addp(v, pfx, "dn_rlen", 0);
    st_addp(v, pfx, "dn_rbuf", 0);
    st_expect_reason_bytes(v, pfx, NULL);
}


static void st_expect_sub_live(state_t *v, const char *pfx, uint32_t gen,
                               uint64_t handle, uint64_t req_id)
{
    st_addp(v, pfx, "state",  (uint64_t)MOQ_SUB_ESTABLISHED);
    st_addp(v, pfx, "role",   (uint64_t)MOQ_SUB_ROLE_SUBSCRIBER);
    st_addp(v, pfx, "gen",    gen);
    st_addp(v, pfx, "handle", handle);
    st_addp(v, pfx, "req_id", req_id);
    st_addp(v, pfx, "req_ref", 0);
    st_addp(v, pfx, "alias",  0);
    st_addp(v, pfx, "forward", 0);
    st_addp(v, pfx, "dn_pend", 0);
    st_addp(v, pfx, "dn_exp",  0);
    st_addp(v, pfx, "dn_status", 0);
    st_addp(v, pfx, "dn_count", 0);
    st_addp(v, pfx, "dn_proc", 0);
    st_addp(v, pfx, "dn_deadline", 0);
    st_addp(v, pfx, "dn_rlen", 0);
    st_addp(v, pfx, "dn_rbuf", 0);
    st_expect_reason_bytes(v, pfx, NULL);
}

/* Deferred but not runnable: count unsatisfied, deadline in the future. */
static void st_expect_pub_future(state_t *v, const char *pfx, uint32_t gen,
                                 uint64_t handle, uint64_t req_id,
                                 uint64_t deadline)
{
    st_expect_pub_live(v, pfx, gen, handle, req_id);
    st_set(v, pfx, "dn_pend", 1);
    st_set(v, pfx, "dn_count", PUB_SC);
    st_set(v, pfx, "dn_deadline", deadline);
}

static void st_expect_sub_future(state_t *v, const char *pfx, uint32_t gen,
                                 uint64_t handle, uint64_t req_id,
                                 uint64_t deadline)
{
    st_expect_sub_live(v, pfx, gen, handle, req_id);
    st_set(v, pfx, "dn_pend", 1);
    st_set(v, pfx, "dn_count", SUB_SC);
    st_set(v, pfx, "dn_deadline", deadline);
}

/* A forwarding subscriber-role subscription still PENDING: what suppresses the
 * discard of retained staged/deferred work. */
static void st_expect_sub_fwd_pending(state_t *v, const char *pfx, uint32_t gen,
                                      uint64_t handle, uint64_t req_id)
{
    st_addp(v, pfx, "state",  (uint64_t)MOQ_SUB_PENDING_SUBSCRIBER);
    st_addp(v, pfx, "role",   (uint64_t)MOQ_SUB_ROLE_SUBSCRIBER);
    st_addp(v, pfx, "gen",    gen);
    st_addp(v, pfx, "handle", handle);
    st_addp(v, pfx, "req_id", req_id);
    st_addp(v, pfx, "req_ref", 0);
    st_addp(v, pfx, "alias",  0);
    st_addp(v, pfx, "forward", 1);
    st_addp(v, pfx, "dn_pend", 0);
    st_addp(v, pfx, "dn_exp",  0);
    st_addp(v, pfx, "dn_status", 0);
    st_addp(v, pfx, "dn_count", 0);
    st_addp(v, pfx, "dn_proc", 0);
    st_addp(v, pfx, "dn_deadline", 0);
    st_addp(v, pfx, "dn_rlen", 0);
    st_addp(v, pfx, "dn_rbuf", 0);
    st_expect_reason_bytes(v, pfx, NULL);
}

/* The declared live image of arm_pending_publisher_sub(). */
static void st_expect_sub_pending_publisher(state_t *v, const char *pfx,
                                            uint32_t gen, uint64_t handle,
                                            uint64_t req_id, uint64_t ref)
{
    st_addp(v, pfx, "state",  (uint64_t)MOQ_SUB_PENDING_PUBLISHER);
    st_addp(v, pfx, "role",   (uint64_t)MOQ_SUB_ROLE_PUBLISHER);
    st_addp(v, pfx, "gen",    gen);
    st_addp(v, pfx, "handle", handle);
    st_addp(v, pfx, "req_id", req_id);
    st_addp(v, pfx, "req_ref", ref);
    st_addp(v, pfx, "alias",  0);
    st_addp(v, pfx, "forward", 0);
    st_addp(v, pfx, "dn_pend", 0);
    st_addp(v, pfx, "dn_exp",  0);
    st_addp(v, pfx, "dn_status", 0);
    st_addp(v, pfx, "dn_count", 0);
    st_addp(v, pfx, "dn_proc", 0);
    st_addp(v, pfx, "dn_deadline", 0);
    st_addp(v, pfx, "dn_rlen", 0);
    st_addp(v, pfx, "dn_rbuf", 0);
    st_expect_reason_bytes(v, pfx, NULL);
}

/* The declared live image of arm_forwarding_pending_sub_d18(). */
static void st_expect_sub_fwd_pending_d18(state_t *v, const char *pfx,
                                          uint32_t gen, uint64_t handle,
                                          uint64_t req_id, uint64_t ref)
{
    st_expect_sub_fwd_pending(v, pfx, gen, handle, req_id);
    st_set(v, pfx, "req_ref", ref);
}

/* One staged-datagram record, byte-exact: its whole 8-byte payload is packed
 * into a single word, so an in-place corruption is visible. */
static void st_staged(state_t *v, const char *pfx, moq_session_t *s, size_t i)
{
    if (!s->staged_dg || i >= s->staged_cap) {
        st_addp(v, pfx, "OUT_OF_RANGE", 1); return;
    }
    const moq_staged_datagram_t *e = &s->staged_dg[i];
    st_addp(v, pfx, "in_use", e->in_use ? 1u : 0u);
    st_addp(v, pfx, "alias",  e->alias);
    if (e->in_use) st_addp(v, pfx, "seq", e->seq);
    st_addp(v, pfx, "len",    (uint64_t)e->len);
    /* A non-zero length with no buffer is INCOMPARABLE, never a zero digest:
     * the field would otherwise read the same as an all-zero payload. */
    if (e->len > 0 && !e->bytes) { st_addp(v, pfx, "bytes_NOBUF", 1); return; }
    uint64_t w = 0;
    for (size_t b = 0; b < 8 && b < e->len; b++)
        w |= (uint64_t)e->bytes[b] << (8 * b);
    st_addp(v, pfx, "bytes", w);
}

/* Non-default join parameters, so a wrong one cannot pass as a default. */
#define JOIN_FETCH_TYPE 3u
#define JOIN_PRIORITY   0x5Bu
#define JOIN_ORDER      MOQ_GROUP_ORDER_DESCENDING

/*
 * ABSOLUTE Joining-FETCH inventory. Every field the fixture declares when it
 * arms the owner is read back and compared, so a wrong role, a wrong join
 * parameter, a re-keyed request identity or a single altered token byte is a
 * named failure -- not merely an owner whose graph edge happens to be right.
 */
static void st_token_bytes(state_t *v, const char *pfx, const char *what,
                           const uint8_t *buf, size_t len)
{
    /* Bounded and FAIL-CLOSED: an over-cap value is incomparable, never an
     * equal-prefix pass. */
    if (len > 16u) { char k[ST_KEY + 16];
                     snprintf(k, sizeof(k), "%s_OVERSIZE", what);
                     st_addp(v, pfx, k, 1); return; }
    for (size_t w = 0; w < 2; w++) {
        uint64_t acc = 0;
        for (size_t b = 0; b < 8; b++) {
            size_t idx = w * 8 + b;
            uint64_t byte = (buf && idx < len) ? buf[idx] : 0u;
            acc |= byte << (8 * b);
        }
        char k[ST_KEY + 16]; snprintf(k, sizeof(k), "%s%zu", what, w);
        st_addp(v, pfx, k, acc);
    }
}

static void st_fetch(state_t *v, const char *pfx, moq_session_t *s, size_t slot)
{
    if (slot >= s->fetch_cap) { st_addp(v, pfx, "OUT_OF_RANGE", 1); return; }
    if (s->fetches[slot].state == MOQ_FETCH_FREE) {
        st_fetch_retired(v, pfx, s, slot); return;
    }
    const moq_fetch_entry_t *e = &s->fetches[slot];
    st_addp(v, pfx, "state",   (uint64_t)e->state);
    st_addp(v, pfx, "role",    (uint64_t)e->role);
    st_addp(v, pfx, "gen",     e->generation);
    st_addp(v, pfx, "slot",    (uint64_t)slot);
    st_addp(v, pfx, "handle",  e->handle._opaque);
    st_addp(v, pfx, "req_id",  e->request_id);
    st_addp(v, pfx, "req_ref", e->request_stream_ref._v);
    st_addp(v, pfx, "req_fin", e->req_recv_fin ? 1u : 0u);
    st_addp(v, pfx, "hoff_fin", e->handoff_fin_pending ? 1u : 0u);
    st_addp(v, pfx, "dstart",  e->data_stream_started ? 1u : 0u);
    st_addp(v, pfx, "dfin",    e->data_stream_fin ? 1u : 0u);
    st_addp(v, pfx, "j_type",  (uint64_t)e->join_fetch_type);
    st_addp(v, pfx, "j_prio",  (uint64_t)e->join_subscriber_priority);
    st_addp(v, pfx, "j_order", (uint64_t)e->join_group_order);
    st_addp(v, pfx, "j_id",    e->join_request_id);
    st_addp(v, pfx, "j_start", e->join_start);
    st_addp(v, pfx, "tok_n",   (uint64_t)e->join_token_count);
    for (size_t i = 0; i < 2; i++) {
        char k[ST_KEY];
        snprintf(k, sizeof(k), "t%zu_type", i);
        st_addp(v, pfx, k, e->join_tokens[i].token_type);
        snprintf(k, sizeof(k), "t%zu_len", i);
        st_addp(v, pfx, k, (uint64_t)e->join_tokens[i].token_value.len);
        snprintf(k, sizeof(k), "t%zu_stg", i);
        st_addp(v, pfx, k, e->join_token_staged[i] ? 1u : 0u);
        snprintf(k, sizeof(k), "t%zu_b", i);
        st_token_bytes(v, pfx, k, e->join_tokens[i].token_value.data,
                       e->join_tokens[i].token_value.len);
    }
}

/*
 * The declared LIVE image of a join armed by arm_pending_join(), built from
 * the fixture's own arming values -- never read back from the owner.
 */
static void st_expect_fetch_join(state_t *v, const char *pfx, uint32_t gen,
                                 uint64_t handle, size_t slot, uint64_t req_id,
                                 uint64_t req_ref, uint64_t sub_req_id,
                                 uint64_t start)
{
    st_addp(v, pfx, "state",   (uint64_t)MOQ_FETCH_PENDING_JOIN);
    st_addp(v, pfx, "role",    (uint64_t)MOQ_FETCH_ROLE_PUBLISHER);
    st_addp(v, pfx, "gen",     gen);
    st_addp(v, pfx, "slot",    (uint64_t)slot);
    st_addp(v, pfx, "handle",  handle);
    st_addp(v, pfx, "req_id",  req_id);
    st_addp(v, pfx, "req_ref", req_ref);
    st_addp(v, pfx, "req_fin", 0);
    st_addp(v, pfx, "hoff_fin", 0);
    st_addp(v, pfx, "dstart",  0);
    st_addp(v, pfx, "dfin",    0);
    st_addp(v, pfx, "j_type",  JOIN_FETCH_TYPE);
    st_addp(v, pfx, "j_prio",  JOIN_PRIORITY);
    st_addp(v, pfx, "j_order", (uint64_t)JOIN_ORDER);
    st_addp(v, pfx, "j_id",    sub_req_id);
    st_addp(v, pfx, "j_start", start);
    st_addp(v, pfx, "tok_n",   2);
    for (size_t i = 0; i < 2; i++) {
        char k[ST_KEY];
        uint8_t bytes[16];
        size_t len = 8 + i;
        memset(bytes, (int)(0xA0 + i), sizeof(bytes));
        snprintf(k, sizeof(k), "t%zu_type", i);
        st_addp(v, pfx, k, (uint64_t)(0x10 + i));
        snprintf(k, sizeof(k), "t%zu_len", i);
        st_addp(v, pfx, k, (uint64_t)len);
        snprintf(k, sizeof(k), "t%zu_stg", i);
        st_addp(v, pfx, k, 1);
        snprintf(k, sizeof(k), "t%zu_b", i);
        st_token_bytes(v, pfx, k, bytes, len);
    }
}

static void st_expect_fetch_free(state_t *v, const char *pfx, uint32_t live_gen)
{
    st_addp(v, pfx, "state",   (uint64_t)MOQ_FETCH_FREE);
    st_addp(v, pfx, "gen",     live_gen + 1u);
    st_addp(v, pfx, "tok_n",   0);
    st_addp(v, pfx, "tok0",    0);   /* staged token storage released */
    st_addp(v, pfx, "tok1",    0);
}

/*
 * The CLOSE path's rx contract, which is narrower than a free: the entry is
 * DEACTIVATED and its buffers released, but it is not reset -- free_rx_stream_bufs
 * deliberately leaves the identity bytes alone. Pinning that residue would make
 * a future zeroing improvement a test failure, so it is left out of the
 * contract exactly as the retired-owner rule requires.
 */
static void st_rx_released(state_t *v, const char *pfx, moq_session_t *s,
                           size_t slot)
{
    if (slot >= s->rx_cap) { st_addp(v, pfx, "OUT_OF_RANGE", 1); return; }
    const moq_rx_stream_t *rx = &s->rx_streams[slot];
    st_addp(v, pfx, "active",  rx->active ? 1u : 0u);
    st_addp(v, pfx, "in_len",  (uint64_t)rx->input_len);
    st_addp(v, pfx, "pay_buf", rx->payload_buf ? 1u : 0u);
}

static void st_expect_rx_released(state_t *v, const char *pfx)
{
    st_addp(v, pfx, "active",  0);
    st_addp(v, pfx, "in_len",  0);
    st_addp(v, pfx, "pay_buf", 0);
}

static void st_expect_cursor_retired(state_t *v, uint64_t sess_state,
                                     uint64_t sweep_now)
{
    st_add(v, "sweep.active",  0);
    st_add(v, "sweep.stage",   (uint64_t)MOQ_SWEEP_STAGE_IDLE);
    st_add(v, "sweep.phase",   (uint64_t)MOQ_SWEEP_PHASE_SELECT);
    st_add(v, "sweep.slot",    0);
    st_add(v, "sweep.rx_pos",  0);
    st_add(v, "sweep.rx_found", 0);
    st_add(v, "sweep.reaped",  0);
    st_add(v, "sweep.now",     sweep_now);
    st_add(v, "sess.state",    sess_state);
}

/* ================= the case runner ================================ */

static void sort2(size_t *a, size_t *b)
{
    if (*a > *b) { size_t t = *a; *a = *b; *b = t; }
}

static void sort3(size_t *a, size_t *b, size_t *c)
{
    sort2(a, b); sort2(b, c); sort2(a, b);
}

/* Arm a faithful buffered Joining FETCH against a subscription's request id,
 * carrying real staged token storage so its retirement also exercises the
 * auth-staging free loop.
 *
 * Every identity below belongs to a SYMBOL, not to a slot. The placement matrix
 * moves the physical slot while holding the symbolic owner set fixed, so a
 * slot-derived request id or stream ref would make each row declare a different
 * owner and let a coherent setup error authorize itself. */
#define REF_SUB0      0x9001ull
#define REF_SUB1      0x9002ull
#define REF_SUB2      0x9003ull
#define REQ_JOIN0     0x61ull
#define REQ_JOIN1     0x63ull
#define REQ_JOIN2     0x65ull
#define REQ_JOIN3     0x67ull
#define REF_JOIN0     0x9100ull
#define REF_JOIN1     0x9101ull
#define REF_JOIN2     0x9102ull
#define REF_JOIN3     0x9103ull
#define JOIN_START0   0x901ull
#define JOIN_START1   0x902ull
#define JOIN_START2   0x903ull
#define JOIN_START3   0x904ull
/* Symbols for the joins, alongside SYM_SUB0/1/2 above. */
enum { SYM_J0 = 31, SYM_J1 = 32, SYM_J2 = 33, SYM_J3 = 34 };

/*
 * A buffered Joining FETCH in its DRAFT-18 shape, which is the only shape
 * production can create: the buffering branch is reachable only when the
 * request arrived on a request bidi (has_stream_ref), so the committed entry
 * carries the PUBLISHER role, a non-zero request stream ref, and exactly ONE
 * registry edge -- keyed by STREAM REF, never by id (session_fetch.c:250-279).
 */
static void arm_pending_join(moq_session_t *s, size_t slot, uint64_t sub_req_id,
                             uint64_t req_id, uint64_t req_ref, uint64_t start)
{
    /* Production allocation links the slot into the pool's occupancy
     * list, which is what the preamble scans walk. A fixture that armed
     * an owner without it would be arming a state no session can hold. */
    fetch_occ_link(s, slot);
    moq_fetch_entry_t *fe = &s->fetches[slot];
    fe->state           = MOQ_FETCH_PENDING_JOIN;
    fe->role            = MOQ_FETCH_ROLE_PUBLISHER;
    fe->generation     |= 1u;
    fe->handle          = (moq_fetch_t){
        moq_handle_pack(MOQ_HANDLE_POOL_FETCH, s->session_tag,
                        fe->generation, (uint32_t)slot) };
    /* Identity comes from the caller's DECLARATION, not from the physical
     * slot: the placement matrix must hold the symbolic owner fixed while the
     * slot moves, which a slot-derived id or ref would silently break. */
    fe->request_id         = req_id;
    fe->request_stream_ref = moq_stream_ref_from_u64(req_ref);
    {
        moq_request_endpoint_t ep;
        memset(&ep, 0, sizeof(ep));
        ep.kind = MOQ_REQ_FETCH;
        ep.slot = (int)slot;
        ep.has_request_id = true;
        ep.request_id = fe->request_id;
        ep.has_stream_ref = true;
        ep.stream_ref = fe->request_stream_ref;
        request_registry_insert_by_streamref(s, fe->request_stream_ref, ep);
    }
    fe->join_request_id          = sub_req_id;
    fe->join_start               = start;
    fe->join_fetch_type          = (uint8_t)JOIN_FETCH_TYPE;
    fe->join_subscriber_priority = (uint8_t)JOIN_PRIORITY;
    fe->join_group_order         = JOIN_ORDER;
    fe->join_token_count = 2;
    for (size_t i = 0; i < 2; i++) {
        size_t len = 8 + i;
        uint8_t *v = (uint8_t *)s->alloc.alloc(len, s->alloc.ctx);
        if (!v) { fe->join_token_count = i; break; }
        memset(v, (int)(0xA0 + i), len);
        fe->join_tokens[i].token_type = 0x10 + i;
        fe->join_tokens[i].token_value.data = v;
        fe->join_tokens[i].token_value.len  = len;
        fe->join_token_staged[i] = true;
    }
}

/*
 * Allocate and populate the staged-datagram ring. Its slot count is a
 * compile-time constant (MOQ_STAGED_DG_SLOTS), NOT a user-configured capacity,
 * so it is not a matrix axis -- but its scan is real work and is modelled.
 */
static void arm_staged(moq_session_t *s, uint64_t alias, size_t slot,
                       uint8_t fill)
{
    if (!s->staged_dg) {
        s->staged_dg = (moq_staged_datagram_t *)s->alloc.alloc(
            s->staged_cap * sizeof(*s->staged_dg), s->alloc.ctx);
        if (!s->staged_dg) return;
        memset(s->staged_dg, 0, s->staged_cap * sizeof(*s->staged_dg));
    }
    if (slot >= s->staged_cap) return;
    moq_staged_datagram_t *e = &s->staged_dg[slot];
    e->in_use = true;
    e->alias  = alias;
    e->seq    = slot;
    e->len    = 8;
    e->bytes  = (uint8_t *)s->alloc.alloc(e->len, s->alloc.ctx);
    if (e->bytes) memset(e->bytes, fill, e->len);
    else          e->len = 0;
    s->staged_count++;
    s->staged_bytes += e->len;
}

/* An ESTABLISHED subscriber-role subscription owning a track alias, indexed as
 * production indexes it -- so staged data for that alias is RETAINED. */
static void arm_alias_owner(moq_session_t *s, size_t slot, uint64_t alias,
                            uint64_t req_id)
{
    arm_live_sub(s, slot, req_id);
    s->subs[slot].track_alias = alias;
    /* sub_alias_index_insert() is static to session_subscribe.c; this is the
     * same single index operation it performs. */
    moq_index_insert(s->idx_sub_by_alias, s->idx_sub_alias_mask, alias,
                     (int)slot);
}

/* A live (non-terminal) subgroup and a live rx entry, so a close-time cleanup
 * has something to clean rather than only being counted empty. */
static void arm_live_subgroup(moq_session_t *s, size_t slot, uint64_t ref,
                              moq_publication_t owner)
{
    /* Production allocation links the slot; the sweeps walk that list. */
    sg_occ_link(s, slot);
    moq_sg_entry_t *e = &s->subgroups[slot];
    e->state       = MOQ_SG_OPEN;
    e->generation |= 1u;
    e->stream_ref  = moq_stream_ref_from_u64(ref);
    e->delivery_deadline_us = UINT64_MAX;
    /* The DECLARED live publication, not a fabricated slot-0 handle: an
     * expectation may not be authorized by an owner that does not exist. */
    e->pub = owner;
}

/* A forwarding subscriber-role subscription that is PENDING: exactly what
 * session_has_forwarding_pending_subscriber() must find, so the discard of
 * retained staged/deferred work is suppressed. */
static void arm_forwarding_pending_sub(moq_session_t *s, size_t slot,
                                       uint64_t req_id)
{
    /* Production allocation links the slot; the sweeps walk that list. */
    sub_occ_link(s, slot);
    moq_sub_entry_t *e = &s->subs[slot];
    e->state       = MOQ_SUB_PENDING_SUBSCRIBER;
    e->role        = MOQ_SUB_ROLE_SUBSCRIBER;
    e->generation |= 1u;
    e->handle      = (moq_subscription_t){
        moq_handle_pack(MOQ_HANDLE_POOL_SUBSCRIPTION, s->session_tag,
                        e->generation, (uint32_t)slot) };
    e->forward     = true;
    e->done_pending = false;
    register_sub(s, slot, req_id);
}

/* Install the stream-ref registry edge a draft-18 request owner holds. */
static void register_sub_streamref(moq_session_t *s, size_t slot,
                                   uint64_t req_id, uint64_t ref)
{
    moq_sub_entry_t *e = &s->subs[slot];
    e->request_id         = req_id;
    e->request_stream_ref = moq_stream_ref_from_u64(ref);
    moq_request_endpoint_t ep;
    memset(&ep, 0, sizeof(ep));
    ep.kind = MOQ_REQ_SUBSCRIPTION;
    ep.slot = (int)slot;
    ep.has_request_id = true;
    ep.request_id     = req_id;
    ep.has_stream_ref = true;
    ep.stream_ref     = e->request_stream_ref;
    request_registry_insert_by_streamref(s, e->request_stream_ref, ep);
}

/*
 * The draft-18 responder subscription a peer's SUBSCRIBE creates and has not
 * yet been answered: PUBLISHER role, PENDING_PUBLISHER, keyed by its request
 * stream ref alone (responses correlate by stream, so there is deliberately no
 * by-id edge). It carries NO deferred terminal -- done_pending is installed
 * only for a SUBSCRIBER-role owner, and forging one here would be a state
 * production cannot reach.
 */
static void arm_pending_publisher_sub(moq_session_t *s, size_t slot,
                                      uint64_t req_id, uint64_t ref)
{
    /* Production allocation links the slot; the sweeps walk that list. */
    sub_occ_link(s, slot);
    moq_sub_entry_t *e = &s->subs[slot];
    e->state       = MOQ_SUB_PENDING_PUBLISHER;
    e->role        = MOQ_SUB_ROLE_PUBLISHER;
    e->generation |= 1u;
    e->handle      = (moq_subscription_t){
        moq_handle_pack(MOQ_HANDLE_POOL_SUBSCRIPTION, s->session_tag,
                        e->generation, (uint32_t)slot) };
    e->done_pending = false;
    register_sub_streamref(s, slot, req_id, ref);
}

/* A forwarding-pending subscriber in the SAME draft-18 shape: stream-ref
 * keyed, so the whole fixture graph belongs to one profile. */
static void arm_forwarding_pending_sub_d18(moq_session_t *s, size_t slot,
                                           uint64_t req_id, uint64_t ref)
{
    /* Production allocation links the slot; the sweeps walk that list. */
    sub_occ_link(s, slot);
    moq_sub_entry_t *e = &s->subs[slot];
    e->state       = MOQ_SUB_PENDING_SUBSCRIBER;
    e->role        = MOQ_SUB_ROLE_SUBSCRIBER;
    e->generation |= 1u;
    e->handle      = (moq_subscription_t){
        moq_handle_pack(MOQ_HANDLE_POOL_SUBSCRIPTION, s->session_tag,
                        e->generation, (uint32_t)slot) };
    e->forward     = true;
    e->done_pending = false;
    register_sub_streamref(s, slot, req_id, ref);
}

/*
 * Run one case at one capacity row. Every phase records its own work, output,
 * state and MODEL image; expected images are declared before each measured
 * call and checked immediately after it.
 */
static int run_case(int case_id, const caps_t *caps_in, uint32_t seed,
                    phase_t *ph, size_t *nph, const char **sched_text)
{
    static char sched_buf[320];
    int failures = 0;
    const uint64_t now = 1000000ull;
    caps_t c = *caps_in;
    case_caps(case_id, &c);
    *nph = 0;

    moq_session_t *s = make_session(&c);
    MOQ_TEST_CHECK(s != NULL);
    if (!s) return failures;
    /* Owner-empty starting point: heads and sentinels before any arm. */
    failures += occ_audit_named(s, "run_case.empty");

    place_t pl;
    if (!place_build(&pl, seed, &c, &failures)) { moq_session_destroy(s); return failures; }

    symtab_t sym; sym_reset(&sym);
    sched_t sc; sched_reset(&sc, seed);

    /* ---- the commuting setup operations for this case ---- */
    switch (case_id) {
    case C_IDLE:
        break;
    case C_OCCUPIED:
        sched_add(&sc, OP_ARM_LIVE_PUB, pl.p0, REQ_PUB0);
        sched_add(&sc, OP_ARM_LIVE_PUB, pl.p1, REQ_PUB1);
        sched_add(&sc, OP_ARM_LIVE_SUB, pl.u0, REQ_SUB0);
        sched_add(&sc, OP_ARM_LIVE_SUB, pl.u1, REQ_SUB1);
        sched_add(&sc, OP_ARM_LIVE_SUB, pl.u2, REQ_SUB2);
        break;
    case C_FUTURE:
        sched_add(&sc, OP_ARM_FUTURE_PUB, pl.p0, REQ_PUB0);
        sched_add(&sc, OP_ARM_FUTURE_SUB, pl.u0, REQ_SUB0);
        break;
    case C_READY_PUB:   sched_add(&sc, OP_ARM_PUB, pl.p0, REQ_PUB0); break;
    case C_READY_SUB:   sched_add(&sc, OP_ARM_SUB, pl.u0, REQ_SUB0); break;
    case C_READY_MULTI:
        sched_add(&sc, OP_ARM_PUB, pl.p0, REQ_PUB0);
        sched_add(&sc, OP_ARM_PUB, pl.p1, REQ_PUB1);
        sched_add(&sc, OP_ARM_SUB, pl.u0, REQ_SUB0);
        sched_add(&sc, OP_ARM_SUB, pl.u1, REQ_SUB1);
        sched_add(&sc, OP_ARM_SUB, pl.u2, REQ_SUB2);
        break;
    case C_BLOCKED_RETRY:
        sched_add(&sc, OP_ARM_PUB, pl.p0, REQ_PUB0);
        sched_add(&sc, OP_BIND_RX, pl.r0, pl.p0);
        break;
    case C_EXACT_ONCE:
    case C_FREE_REUSE:
        sched_add(&sc, OP_ARM_PUB, pl.p0, REQ_PUB0);
        sched_add(&sc, OP_ARM_SUB, pl.u0, REQ_SUB0);
        break;
    case C_RX_STOP:
        sched_add(&sc, OP_ARM_PUB, pl.p0, REQ_PUB0);
        sched_add(&sc, OP_BIND_RX, pl.r0, pl.p0);
        sched_add(&sc, OP_BIND_RX, pl.r1, pl.p0);
        break;
    case C_SCRATCH_CLOSE:
        sched_add(&sc, OP_ARM_PUB, pl.p0, REQ_PUB0);
        break;
    case C_FWD_PENDING:
    case C_DEFERRED_DISCARD:
        sched_add(&sc, OP_ARM_SUB, pl.u0, REQ_SUB0);
        sched_add(&sc, OP_ARM_DEFERRED_RX, pl.r0, 0);
        break;
    case C_STAGED_DISCARD:
        sched_add(&sc, OP_ARM_SUB, pl.u0, REQ_SUB0);
        break;
    default: break;
    }
    sched_finalize(&sc);
    for (size_t i = 0; i < sc.n; i++) op_apply(s, &sc.op[i], now);
    snprintf(sched_buf, sizeof(sched_buf), "%s", sc.text);
    *sched_text = sched_buf;

    /* ---- ordered (non-commuting) setup ---- */
    if (case_id == C_FWD_PENDING) {
        /* Two forwarding-pending subscribers survive the finalize, so the
         * retained deferred stream must NOT be discarded. */
        arm_forwarding_pending_sub(s, pl.u1, REQ_SUB1);
        arm_forwarding_pending_sub(s, pl.u2, REQ_SUB2);
    }
    if (case_id == C_SCRATCH_CLOSE) {
        arm_live_subgroup(s, pl.g0, 0xC0DE, s->publishes[pl.p0].handle);
        bind_rx_pub(s, pl.r0, s->publishes[pl.p0].handle, RX_REF_BASE + pl.r0);
        /*
         * A TERMINAL subgroup beside the live one, so the reap really reaps and
         * then recomputes the aggregate deadline over the SURVIVOR -- the
         * recompute has nothing to walk when the pool empties itself.
         */
        sweep_arm_closing_subgroup(s, pl.g1);
        /*
         * A second rx stream bound to NOTHING, so the sweep's owner-bound stop
         * leaves it and the CLOSE is what cleans it. Without a stream that
         * survives to the close, the close-time rx cleanup has nothing to walk.
         */
        bind_rx_deferred(s, pl.r1, RX_REF_BASE + pl.r1);
    }
    if (case_id == C_BLOCKED_RETRY)
        fill_action_queue(s);

    if (case_id == C_STAGED_DISCARD) {
        /* One staged entry whose alias an ESTABLISHED subscriber owns (must be
         * RETAINED) and one whose alias nothing owns (must be DISCARDED), so
         * the scan's selectivity is proven, not just its cost. */
        arm_alias_owner(s, pl.u1, 0x777ull, REQ_SUB1);
        arm_staged(s, 0x777ull, 0, 0xAA);   /* resolvable  -> retained */
        arm_staged(s, 0x888ull, 1, 0xBB);   /* unresolved  -> discarded */
        MOQ_TEST_CHECK(s->staged_dg != NULL);
        MOQ_TEST_CHECK_EQ_U64((uint64_t)s->staged_count, 2u);
    }
    sym_bind(&sym, pub_handle_at(s, pl.p0), SYM_PUB0);
    sym_bind(&sym, pub_handle_at(s, pl.p1), SYM_PUB1);
    sym_bind(&sym, sub_handle_at(s, pl.u0), SYM_SUB0);
    sym_bind(&sym, sub_handle_at(s, pl.u1), SYM_SUB1);
    sym_bind(&sym, sub_handle_at(s, pl.u2), SYM_SUB2);
    uint32_t pub0_gen = s->publishes[pl.p0].generation;
    uint32_t pub1_gen = s->publishes[pl.p1].generation;
    uint32_t sub0_gen = s->subs[pl.u0].generation;
    uint32_t sub1_gen = s->subs[pl.u1].generation;
    uint32_t sub2_gen = s->subs[pl.u2].generation;
    uint64_t pub0_h = pub_handle_at(s, pl.p0);
    uint64_t sub0_h = sub_handle_at(s, pl.u0);
    uint64_t sub1_h = sub_handle_at(s, pl.u1);
    uint64_t sub2_h = sub_handle_at(s, pl.u2);
    uint32_t sg0_gen = s->subgroups[pl.g0].generation;
    uint32_t sg1_gen = s->subgroups[pl.g1].generation;
    uint64_t pub1_h = pub_handle_at(s, pl.p1);

    /* Every arm is complete: the pools' occupancy topology must already be
     * whole before any baseline is accepted from this fixture. */
    {
        char tag[80];
        snprintf(tag, sizeof(tag), "%s/armed", case_name(case_id));
        failures += occ_audit_named(s, tag);
    }

    /*
     * LIVE TOPOLOGY, asserted before the measured call. Every owner this case
     * declares must hold exactly the edge its lifecycle owns -- right domain,
     * right endpoint kind, right target, and nothing else pointing at it.
     */
    switch (case_id) {
    case C_READY_PUB: case C_RX_STOP: case C_BLOCKED_RETRY:
    case C_SCRATCH_CLOSE:
        failures += check_live_edge(s, MOQ_REQ_PUBLISH, pl.p0,
                                    OG_DOM_REQ_RID, REQ_PUB0, "live.P0");
        break;
    case C_READY_SUB:
        failures += check_live_edge(s, MOQ_REQ_SUBSCRIPTION, pl.u0,
                                    OG_DOM_REQ_RID, REQ_SUB0, "live.U0");
        break;
    case C_EXACT_ONCE: case C_FREE_REUSE:
        failures += check_live_edge(s, MOQ_REQ_PUBLISH, pl.p0,
                                    OG_DOM_REQ_RID, REQ_PUB0, "live.P0");
        failures += check_live_edge(s, MOQ_REQ_SUBSCRIPTION, pl.u0,
                                    OG_DOM_REQ_RID, REQ_SUB0, "live.U0");
        break;
    case C_READY_MULTI:
        failures += check_live_edge(s, MOQ_REQ_PUBLISH, pl.p0,
                                    OG_DOM_REQ_RID, REQ_PUB0, "live.P0");
        failures += check_live_edge(s, MOQ_REQ_PUBLISH, pl.p1,
                                    OG_DOM_REQ_RID, REQ_PUB1, "live.P1");
        failures += check_live_edge(s, MOQ_REQ_SUBSCRIPTION, pl.u0,
                                    OG_DOM_REQ_RID, REQ_SUB0, "live.U0");
        failures += check_live_edge(s, MOQ_REQ_SUBSCRIPTION, pl.u1,
                                    OG_DOM_REQ_RID, REQ_SUB1, "live.U1");
        failures += check_live_edge(s, MOQ_REQ_SUBSCRIPTION, pl.u2,
                                    OG_DOM_REQ_RID, REQ_SUB2, "live.U2");
        break;
    case C_FWD_PENDING:
        failures += check_live_edge(s, MOQ_REQ_SUBSCRIPTION, pl.u0,
                                    OG_DOM_REQ_RID, REQ_SUB0, "live.U0");
        failures += check_live_edge(s, MOQ_REQ_SUBSCRIPTION, pl.u1,
                                    OG_DOM_REQ_RID, REQ_SUB1, "live.U1");
        failures += check_live_edge(s, MOQ_REQ_SUBSCRIPTION, pl.u2,
                                    OG_DOM_REQ_RID, REQ_SUB2, "live.U2");
        break;
    case C_STAGED_DISCARD:
        failures += check_live_edge(s, MOQ_REQ_SUBSCRIPTION, pl.u0,
                                    OG_DOM_REQ_RID, REQ_SUB0, "live.U0");
        failures += check_live_edge(s, MOQ_REQ_SUBSCRIPTION, pl.u1,
                                    OG_DOM_REQ_RID, REQ_SUB1, "live.U1");
        break;
    default: break;
    }

    /*
     * Staged-ring classification, asserted per case: every model EXCEPT the
     * staged one leaves the ring unallocated, which is why their staged scan
     * cannot run. The staged case allocates it deliberately and models the
     * scan's work.
     */
    if (case_id == C_STAGED_DISCARD) MOQ_TEST_CHECK(s->staged_dg != NULL);
    else                             MOQ_TEST_CHECK(s->staged_dg == NULL);

#define PHASE(NAME, CALL, WANT_OUT, WANT_ST, WANT_MODEL, OBS_ST)          \
    do {                                                                  \
        phase_t *p = &ph[(*nph)++];                                       \
        p->name = (NAME);                                                 \
        out_reset(&p->got); st_reset(&p->st);                             \
        work_t before, after;                                             \
        work_read(&before);                                               \
        CALL;                                                             \
        work_read(&after);                                                \
        work_delta(&before, &after, &p->w);                               \
        char occ_tag[80];                                                 \
        snprintf(occ_tag, sizeof(occ_tag), "%s/%s.post_call",             \
                 case_name(case_id), (NAME));                             \
        failures += occ_audit_named(s, occ_tag);                          \
        /* State FIRST: polling runs staged replay and deferred refeed, so a  \
         * capture taken before the observation would mutate what is being    \
         * asserted. */                                                       \
        OBS_ST;                                                           \
        out_capture_events(s, &sym, &p->got);                             \
        out_capture_actions(s, &sym, &p->got);                            \
        snprintf(occ_tag, sizeof(occ_tag), "%s/%s.post_observe",           \
                 case_name(case_id), (NAME));                             \
        failures += occ_audit_named(s, occ_tag);                          \
        failures += out_check((WANT_OUT), &p->got, (NAME));               \
        failures += st_check((WANT_ST), &p->st, (NAME));                  \
        failures += model_check((WANT_MODEL), &p->w, (NAME));             \
    } while (0)

    out_t want; state_t wst; model_t wm;

    switch (case_id) {
    case C_IDLE: {
        out_reset(&want); st_reset(&wst); model_reset(&wm);
        wm.catchup = 1;
        st_expect_cursor_retired(&wst, (uint64_t)MOQ_SESS_IDLE, now);
        PHASE("initial", session_begin_advance(s, now), &want, &wst, &wm,
              do { st_observe_cursor(&p->st, s); } while (0));
        break;
    }
    case C_OCCUPIED: {
        out_reset(&want); st_reset(&wst); model_reset(&wm);
        wm.catchup = 1;
        /* All FIVE occupied owners are inventoried, not just the cursor. */
        st_expect_pub_live(&wst, "P0", pub0_gen, pub_handle_at(s, pl.p0), REQ_PUB0);
        st_expect_pub_live(&wst, "P1", pub1_gen, pub_handle_at(s, pl.p1), REQ_PUB1);
        st_expect_sub_live(&wst, "U0", sub0_gen, sub_handle_at(s, pl.u0), REQ_SUB0);
        st_expect_sub_live(&wst, "U1", sub1_gen, sub_handle_at(s, pl.u1), REQ_SUB1);
        st_expect_sub_live(&wst, "U2", sub2_gen, sub_handle_at(s, pl.u2), REQ_SUB2);
        st_expect_cursor_retired(&wst, (uint64_t)MOQ_SESS_IDLE, now);
        PHASE("initial", session_begin_advance(s, now), &want, &wst, &wm,
              do { st_pub(&p->st, "P0", s, pl.p0); st_pub(&p->st, "P1", s, pl.p1);
                   st_sub(&p->st, "U0", s, pl.u0); st_sub(&p->st, "U1", s, pl.u1);
                   st_sub(&p->st, "U2", s, pl.u2);
                   st_observe_cursor(&p->st, s); } while (0));
        break;
    }
    case C_FUTURE: {
        out_reset(&want); st_reset(&wst); model_reset(&wm);
        wm.catchup = 1; wm.pub_pending = 1; wm.sub_pending = 1;
        st_expect_pub_future(&wst, "P0", pub0_gen, pub_handle_at(s, pl.p0),
                             REQ_PUB0, now + 1000000ull);
        st_expect_sub_future(&wst, "U0", sub0_gen, sub_handle_at(s, pl.u0),
                             REQ_SUB0, now + 1000000ull);
        st_expect_cursor_retired(&wst, (uint64_t)MOQ_SESS_IDLE, now);
        PHASE("initial", session_begin_advance(s, now), &want, &wst, &wm,
              do { st_pub(&p->st, "P0", s, pl.p0); st_sub(&p->st, "U0", s, pl.u0);
                   st_observe_cursor(&p->st, s); } while (0));
        break;
    }
    case C_READY_PUB: {
        out_reset(&want); st_reset(&wst); model_reset(&wm);
        exp_event(&want, MOQ_EVENT_PUBLISH_FINISHED, SYM_PUB0, PUB_STATUS,
                  PUB_SC, PUB_REASON);
        wm.catchup = 1; wm.rx_rescan = 1; wm.pub_pending = 1;
        wm.idx_find_ops = 1; wm.idx_remove_ops = 1; wm.idx_present_rm = 1;
        st_expect_pub_free(&wst, "P0", pub0_gen);
        st_expect_cursor_retired(&wst, (uint64_t)MOQ_SESS_IDLE, now);
        PHASE("initial", session_begin_advance(s, now), &want, &wst, &wm,
              do { st_pub(&p->st, "P0", s, pl.p0);
                   st_observe_cursor(&p->st, s); } while (0));
        failures += check_retired(s, MOQ_REQ_PUBLISH, pl.p0, pub0_h, REQ_PUB0,
                                  "ready_pub");
        break;
    }
    case C_READY_SUB: {
        out_reset(&want); st_reset(&wst); model_reset(&wm);
        exp_event(&want, MOQ_EVENT_SUBSCRIBE_DONE, SYM_SUB0, SUB_STATUS,
                  SUB_SC, SUB_REASON);
        wm.catchup = 1; wm.rx_rescan = 1; wm.sub_pending = 1;
        /* Subscriber-role free: ONE by-id lookup, then TWO removals -- the
         * by-id key (present) and the ESTABLISHED subscriber's alias key
         * (absent here, but attempted unconditionally). */
        wm.idx_find_ops = 1; wm.idx_remove_ops = 2; wm.idx_present_rm = 1;
        st_expect_sub_free(&wst, "U0", sub0_gen);
        st_expect_cursor_retired(&wst, (uint64_t)MOQ_SESS_IDLE, now);
        PHASE("initial", session_begin_advance(s, now), &want, &wst, &wm,
              do { st_sub(&p->st, "U0", s, pl.u0);
                   st_observe_cursor(&p->st, s); } while (0));
        failures += check_retired(s, MOQ_REQ_SUBSCRIPTION, pl.u0, sub0_h,
                                  REQ_SUB0, "ready_sub");
        break;
    }
    case C_READY_MULTI: {
        size_t pa = pl.p0, pb = pl.p1; sort2(&pa, &pb);
        size_t ua = pl.u0, ub = pl.u1, uc = pl.u2; sort3(&ua, &ub, &uc);
        out_reset(&want); st_reset(&wst); model_reset(&wm);
        exp_event(&want, MOQ_EVENT_PUBLISH_FINISHED,
                  pa == pl.p0 ? SYM_PUB0 : SYM_PUB1, PUB_STATUS, PUB_SC, PUB_REASON);
        exp_event(&want, MOQ_EVENT_PUBLISH_FINISHED,
                  pb == pl.p0 ? SYM_PUB0 : SYM_PUB1, PUB_STATUS, PUB_SC, PUB_REASON);
        exp_event(&want, MOQ_EVENT_SUBSCRIBE_DONE,
                  ua == pl.u0 ? SYM_SUB0 : (ua == pl.u1 ? SYM_SUB1 : SYM_SUB2),
                  SUB_STATUS, SUB_SC, SUB_REASON);
        exp_event(&want, MOQ_EVENT_SUBSCRIBE_DONE,
                  ub == pl.u0 ? SYM_SUB0 : (ub == pl.u1 ? SYM_SUB1 : SYM_SUB2),
                  SUB_STATUS, SUB_SC, SUB_REASON);
        exp_event(&want, MOQ_EVENT_SUBSCRIBE_DONE,
                  uc == pl.u0 ? SYM_SUB0 : (uc == pl.u1 ? SYM_SUB1 : SYM_SUB2),
                  SUB_STATUS, SUB_SC, SUB_REASON);
        wm.catchup = 1; wm.rx_rescan = 5; wm.pub_pending = 2; wm.sub_pending = 3;
        /* Two publications (1 removal each) and three subscriptions (2 each:
         * by-id present, alias attempted), one by-id lookup per owner. */
        wm.idx_find_ops = 5; wm.idx_remove_ops = 8; wm.idx_present_rm = 5;
        st_expect_pub_free(&wst, "P0", pub0_gen);
        st_expect_pub_free(&wst, "P1", pub1_gen);
        st_expect_sub_free(&wst, "U0", sub0_gen);
        st_expect_sub_free(&wst, "U1", sub1_gen);
        st_expect_sub_free(&wst, "U2", sub2_gen);
        st_expect_cursor_retired(&wst, (uint64_t)MOQ_SESS_IDLE, now);
        PHASE("initial", session_begin_advance(s, now), &want, &wst, &wm,
              do { st_pub(&p->st, "P0", s, pl.p0); st_pub(&p->st, "P1", s, pl.p1);
                   st_sub(&p->st, "U0", s, pl.u0); st_sub(&p->st, "U1", s, pl.u1);
                   st_sub(&p->st, "U2", s, pl.u2);
                   st_observe_cursor(&p->st, s); } while (0));
        failures += check_retired(s, MOQ_REQ_PUBLISH, pl.p0, pub0_h, REQ_PUB0, "multi.P0");
        failures += check_retired(s, MOQ_REQ_PUBLISH, pl.p1, pub1_h, REQ_PUB1, "multi.P1");
        failures += check_retired(s, MOQ_REQ_SUBSCRIPTION, pl.u0, sub0_h, REQ_SUB0, "multi.U0");
        failures += check_retired(s, MOQ_REQ_SUBSCRIPTION, pl.u1, sub1_h, REQ_SUB1, "multi.U1");
        failures += check_retired(s, MOQ_REQ_SUBSCRIPTION, pl.u2, sub2_h, REQ_SUB2, "multi.U2");
        break;
    }
    case C_BLOCKED_RETRY: {
        out_reset(&want); st_reset(&wst); model_reset(&wm);
        for (uint32_t i = 0; i < c.max_actions; i++)
            exp_action(&want, MOQ_ACTION_STOP_DATA, 0xB000ull, 0);
        wm.catchup = 1; wm.rx_rescan = 1; wm.pub_pending = 1; wm.rx_charged = 1;
        st_expect_pub_ready(&wst, "P0", pub0_gen, pub0_h, REQ_PUB0, 1);
        st_expect_rx_bound_pub(&wst, "R0", RX_REF_BASE + pl.r0, pub0_h,
                               (uint64_t)MOQ_RX_NEED_STOP);
        st_expect_cursor_retired(&wst, (uint64_t)MOQ_SESS_IDLE, now);
        PHASE("refused", session_begin_advance(s, now), &want, &wst, &wm,
              do { st_pub(&p->st, "P0", s, pl.p0);
                   st_rx(&p->st, "R0", s, pl.r0);
                   st_observe_cursor(&p->st, s); } while (0));

        out_reset(&want); st_reset(&wst); model_reset(&wm);
        exp_event(&want, MOQ_EVENT_PUBLISH_FINISHED, SYM_PUB0, PUB_STATUS,
                  PUB_SC, PUB_REASON);
        exp_action(&want, MOQ_ACTION_STOP_DATA, RX_REF_BASE + pl.r0, 0);
        wm.catchup = 1; wm.rx_rescan = 2; wm.pub_pending = 1; wm.rx_charged = 1;
        wm.idx_find_ops = 1; wm.idx_remove_ops = 2; wm.idx_present_rm = 2;
        st_expect_pub_free(&wst, "P0", pub0_gen);
        st_expect_rx_free(&wst, "R0");
        st_expect_cursor_retired(&wst, (uint64_t)MOQ_SESS_IDLE, now);
        PHASE("recovery", session_begin_advance(s, now), &want, &wst, &wm,
              do { st_pub(&p->st, "P0", s, pl.p0);
                   st_rx(&p->st, "R0", s, pl.r0);
                   st_observe_cursor(&p->st, s); } while (0));
        failures += check_retired(s, MOQ_REQ_PUBLISH, pl.p0, pub0_h, REQ_PUB0,
                                  "blocked.recovery");
        break;
    }
    case C_EXACT_ONCE: {
        out_reset(&want); st_reset(&wst); model_reset(&wm);
        exp_event(&want, MOQ_EVENT_PUBLISH_FINISHED, SYM_PUB0, PUB_STATUS,
                  PUB_SC, PUB_REASON);
        exp_event(&want, MOQ_EVENT_SUBSCRIBE_DONE, SYM_SUB0, SUB_STATUS,
                  SUB_SC, SUB_REASON);
        wm.catchup = 1; wm.rx_rescan = 2; wm.pub_pending = 1; wm.sub_pending = 1;
        wm.idx_find_ops = 2; wm.idx_remove_ops = 3; wm.idx_present_rm = 2;
        st_expect_pub_free(&wst, "P0", pub0_gen);
        st_expect_sub_free(&wst, "U0", sub0_gen);
        st_expect_cursor_retired(&wst, (uint64_t)MOQ_SESS_IDLE, now);
        PHASE("complete", session_begin_advance(s, now), &want, &wst, &wm,
              do { st_pub(&p->st, "P0", s, pl.p0); st_sub(&p->st, "U0", s, pl.u0);
                   st_observe_cursor(&p->st, s); } while (0));
        failures += check_retired(s, MOQ_REQ_PUBLISH, pl.p0, pub0_h, REQ_PUB0,
                                  "exact_once.P0");
        failures += check_retired(s, MOQ_REQ_SUBSCRIPTION, pl.u0, sub0_h,
                                  REQ_SUB0, "exact_once.U0");

        for (int rep = 0; rep < 2; rep++) {
            out_reset(&want); st_reset(&wst); model_reset(&wm);
            wm.catchup = 1;
            st_expect_pub_free(&wst, "P0", pub0_gen);
            st_expect_sub_free(&wst, "U0", sub0_gen);
            st_expect_cursor_retired(&wst, (uint64_t)MOQ_SESS_IDLE, now);
            PHASE(rep ? "idempotent_3" : "idempotent_2",
                  session_begin_advance(s, now), &want, &wst, &wm,
                  do { st_pub(&p->st, "P0", s, pl.p0);
                       st_sub(&p->st, "U0", s, pl.u0);
                       st_observe_cursor(&p->st, s); } while (0));
        }
        break;
    }
    case C_RX_STOP: {
        size_t ra = pl.r0, rb = pl.r1; sort2(&ra, &rb);
        out_reset(&want); st_reset(&wst); model_reset(&wm);
        exp_event(&want, MOQ_EVENT_PUBLISH_FINISHED, SYM_PUB0, PUB_STATUS,
                  PUB_SC, PUB_REASON);
        exp_action(&want, MOQ_ACTION_STOP_DATA, RX_REF_BASE + ra, 0);
        exp_action(&want, MOQ_ACTION_STOP_DATA, RX_REF_BASE + rb, 0);
        wm.catchup = 1; wm.rx_rescan = 2; wm.pub_pending = 1; wm.rx_charged = 2;
        wm.idx_find_ops = 1; wm.idx_remove_ops = 3; wm.idx_present_rm = 3;
        st_expect_pub_free(&wst, "P0", pub0_gen);
        st_expect_rx_free(&wst, "R0");
        st_expect_rx_free(&wst, "R1");        /* BOTH rx entries, not just one */
        st_expect_cursor_retired(&wst, (uint64_t)MOQ_SESS_IDLE, now);
        PHASE("initial", session_begin_advance(s, now), &want, &wst, &wm,
              do { st_pub(&p->st, "P0", s, pl.p0);
                   st_rx(&p->st, "R0", s, pl.r0);
                   st_rx(&p->st, "R1", s, pl.r1);
                   st_observe_cursor(&p->st, s); } while (0));
        failures += check_retired(s, MOQ_REQ_PUBLISH, pl.p0, pub0_h, REQ_PUB0,
                                  "rx_bound_stop");
        break;
    }
    case C_FREE_REUSE: {
        out_reset(&want); st_reset(&wst); model_reset(&wm);
        exp_event(&want, MOQ_EVENT_PUBLISH_FINISHED, SYM_PUB0, PUB_STATUS,
                  PUB_SC, PUB_REASON);
        exp_event(&want, MOQ_EVENT_SUBSCRIBE_DONE, SYM_SUB0, SUB_STATUS,
                  SUB_SC, SUB_REASON);
        wm.catchup = 1; wm.rx_rescan = 2; wm.pub_pending = 1; wm.sub_pending = 1;
        wm.idx_find_ops = 2; wm.idx_remove_ops = 3; wm.idx_present_rm = 2;
        st_expect_pub_free(&wst, "P0", pub0_gen);
        st_expect_sub_free(&wst, "U0", sub0_gen);
        st_expect_cursor_retired(&wst, (uint64_t)MOQ_SESS_IDLE, now);
        PHASE("complete", session_begin_advance(s, now), &want, &wst, &wm,
              do { st_pub(&p->st, "P0", s, pl.p0); st_sub(&p->st, "U0", s, pl.u0);
                   st_observe_cursor(&p->st, s); } while (0));
        failures += check_retired(s, MOQ_REQ_PUBLISH, pl.p0, pub0_h, REQ_PUB0,
                                  "free_reuse.P0");
        failures += check_retired(s, MOQ_REQ_SUBSCRIPTION, pl.u0, sub0_h,
                                  REQ_SUB0, "free_reuse.U0");

        arm_live_pub(s, pl.p0, REQ_PUB0);
        arm_live_sub(s, pl.u0, REQ_SUB0);
        uint32_t pub_gen2 = s->publishes[pl.p0].generation;
        uint32_t sub_gen2 = s->subs[pl.u0].generation;
        uint64_t sub_h2   = sub_handle_at(s, pl.u0);
        uint64_t pub_h2   = pub_handle_at(s, pl.p0);
        MOQ_TEST_CHECK(pub_gen2 != pub0_gen);
        MOQ_TEST_CHECK(sub_gen2 != sub0_gen);
        /* SAFE REUSE: the retired handles must not address the replacement. */
        { moq_publication_t old_p = { pub0_h };
          moq_subscription_t old_u = { sub0_h };
          MOQ_TEST_CHECK(pub_resolve_handle(s, old_p) < 0);
          MOQ_TEST_CHECK(sub_resolve_handle(s, old_u) < 0); }
        out_reset(&want); st_reset(&wst); model_reset(&wm);
        wm.catchup = 1;
        st_expect_pub_live(&wst, "P0", pub_gen2, pub_handle_at(s, pl.p0), REQ_PUB0);
        st_expect_sub_live(&wst, "U0", sub_gen2, sub_handle_at(s, pl.u0), REQ_SUB0);
        st_expect_cursor_retired(&wst, (uint64_t)MOQ_SESS_IDLE, now);
        PHASE("reuse_live_silent", session_begin_advance(s, now), &want, &wst,
              &wm,
              do { st_pub(&p->st, "P0", s, pl.p0); st_sub(&p->st, "U0", s, pl.u0);
                   st_observe_cursor(&p->st, s); } while (0));

        sym_reset(&sym);
        sym_bind(&sym, pub_handle_at(s, pl.p0), SYM_PUB1);
        sym_bind(&sym, sub_handle_at(s, pl.u0), SYM_SUB1);
        arm_ready_pub(s, pl.p0, REQ_PUB0);
        arm_ready_sub(s, pl.u0, REQ_SUB0);
        out_reset(&want); st_reset(&wst); model_reset(&wm);
        exp_event(&want, MOQ_EVENT_PUBLISH_FINISHED, SYM_PUB1, PUB_STATUS,
                  PUB_SC, PUB_REASON);
        exp_event(&want, MOQ_EVENT_SUBSCRIBE_DONE, SYM_SUB1, SUB_STATUS,
                  SUB_SC, SUB_REASON);
        wm.catchup = 1; wm.rx_rescan = 2; wm.pub_pending = 1; wm.sub_pending = 1;
        wm.idx_find_ops = 2; wm.idx_remove_ops = 3; wm.idx_present_rm = 2;
        st_expect_pub_free(&wst, "P0", pub_gen2);
        st_expect_sub_free(&wst, "U0", sub_gen2);
        st_expect_cursor_retired(&wst, (uint64_t)MOQ_SESS_IDLE, now);
        PHASE("reuse_ready", session_begin_advance(s, now), &want, &wst, &wm,
              do { st_pub(&p->st, "P0", s, pl.p0); st_sub(&p->st, "U0", s, pl.u0);
                   st_observe_cursor(&p->st, s); } while (0));
        failures += check_retired(s, MOQ_REQ_PUBLISH, pl.p0, pub_h2, REQ_PUB0,
                                  "reuse.P0");
        failures += check_retired(s, MOQ_REQ_SUBSCRIPTION, pl.u0, sub_h2,
                                  REQ_SUB0, "reuse.U0");
        break;
    }
    case C_SCRATCH_CLOSE: {
        /* A live subgroup and a live rx entry, so the close-time cleanup has
         * real work rather than being counted only empty. */
        MOQ_TEST_CHECK(s->publishes[pl.p0].done_reason_len > s->event_scratch_cap);
        MOQ_TEST_CHECK(s->subgroups[pl.g0].state == MOQ_SG_OPEN);
        MOQ_TEST_CHECK(s->subgroups[pl.g1].state == MOQ_SG_CLOSING);
        MOQ_TEST_CHECK(s->rx_streams[pl.r0].active);
        MOQ_TEST_CHECK(s->rx_streams[pl.r1].active);
        /* The EXACT declared binding, asserted BEFORE the cleanup: a subgroup
         * bound to some other publication would otherwise be invisible, since
         * the free path clears the handle either way. */
        MOQ_TEST_CHECK_EQ_HEX(s->subgroups[pl.g0].pub._opaque, pub0_h);
        MOQ_TEST_CHECK_EQ_HEX(s->rx_streams[pl.r0].pub_handle._opaque, pub0_h);
        out_reset(&want); st_reset(&wst); model_reset(&wm);
        exp_event(&want, MOQ_EVENT_SESSION_CLOSED, 0, 0x1ull, 0,
                  "event scratch permanently too small");
        exp_action_reason(&want, MOQ_ACTION_CLOSE_SESSION, 0x1ull,
                          "event scratch permanently too small");
        /* The bound rx entry is really stopped before the finalize fails, so
         * the model carries its charged transition, its rescan pass, its
         * registry removal, and the queued STOP the close then discards. */
        wm.catchup = 1; wm.rx_rescan = 2; wm.pub_pending = 1; wm.rx_charged = 1;
        /* The terminal subgroup is reaped (one charged transition) before the
         * finalize fails; the close then frees the survivor and the unbound
         * stream, so TWO registry keys are removed, both present. */
        wm.sg_charged = 1;
        wm.idx_remove_ops = 2; wm.idx_present_rm = 2; wm.queued_action = 1;
        st_expect_sg_free(&wst, "G0", sg0_gen);  /* cleaned exactly */
        st_expect_sg_free(&wst, "G1", sg1_gen);  /* reaped before the close */
        st_expect_rx_free(&wst, "R0");               /* cleaned exactly */
        st_expect_rx_released(&wst, "R1");           /* released by the close */
        st_expect_cursor_retired(&wst, (uint64_t)MOQ_SESS_CLOSED, now);
        PHASE("scratch_close", session_begin_advance(s, now), &want, &wst, &wm,
              do { st_sg(&p->st, "G0", s, pl.g0); st_sg(&p->st, "G1", s, pl.g1);
                   st_rx(&p->st, "R0", s, pl.r0);
                   st_rx_released(&p->st, "R1", s, pl.r1);
                   st_observe_cursor(&p->st, s); } while (0));
        break;
    }
    case C_FWD_PENDING: {
        /* Finalizing U0 must NOT discard the retained deferred stream, because
         * two forwarding-pending subscribers still remain. */
        out_reset(&want); st_reset(&wst); model_reset(&wm);
        exp_event(&want, MOQ_EVENT_SUBSCRIBE_DONE, SYM_SUB0, SUB_STATUS,
                  SUB_SC, SUB_REASON);
        wm.catchup = 1; wm.rx_rescan = 1; wm.sub_pending = 1;
        wm.idx_find_ops = 1; wm.idx_remove_ops = 2; wm.idx_present_rm = 1;
        st_expect_sub_free(&wst, "U0", sub0_gen);
        st_expect_sub_fwd_pending(&wst, "U1", sub1_gen, sub_handle_at(s, pl.u1), REQ_SUB1);
        st_expect_sub_fwd_pending(&wst, "U2", sub2_gen, sub_handle_at(s, pl.u2), REQ_SUB2);
        /* RETAINED: the deferred stream survives, untouched. */
        st_addp(&wst, "R0", "active", 1);
        st_addp(&wst, "R0", "ref", RX_REF_BASE + pl.r0);
        st_addp(&wst, "R0", "sub", 0);
        st_addp(&wst, "R0", "pub", 0);
        st_addp(&wst, "R0", "parse", (uint64_t)MOQ_RX_DEFERRED_ALIAS);
        st_addp(&wst, "R0", "in_len", 0);
        st_addp(&wst, "R0", "pay_buf", 0);
        st_expect_cursor_retired(&wst, (uint64_t)MOQ_SESS_IDLE, now);
        PHASE("finalize_other", session_begin_advance(s, now), &want, &wst, &wm,
              do { st_sub(&p->st, "U0", s, pl.u0); st_sub(&p->st, "U1", s, pl.u1);
                   st_sub(&p->st, "U2", s, pl.u2); st_rx(&p->st, "R0", s, pl.r0);
                   st_observe_cursor(&p->st, s); } while (0));
        failures += check_retired(s, MOQ_REQ_SUBSCRIPTION, pl.u0, sub0_h,
                                  REQ_SUB0, "fwd_pending.U0");
        break;
    }
    case C_DEFERRED_DISCARD: {
        /*
         * The MIRROR of fwd_pending_retains: with NO forwarding-pending
         * subscriber left, the same retained deferred stream can never be
         * matched, so freeing the subscription discards it -- one STOP_DATA and
         * the entry gone. Without this the discard scan has nothing to walk.
         */
        out_reset(&want); st_reset(&wst); model_reset(&wm);
        exp_event(&want, MOQ_EVENT_SUBSCRIBE_DONE, SYM_SUB0, SUB_STATUS,
                  SUB_SC, SUB_REASON);
        exp_action(&want, MOQ_ACTION_STOP_DATA, RX_REF_BASE + pl.r0, 0);
        wm.catchup = 1; wm.rx_rescan = 1; wm.sub_pending = 1;
        /* The subscription free's by-id lookup; its by-id (present) and alias
         * (absent) removals; and the discarded stream's own key removal. */
        wm.idx_find_ops = 1; wm.idx_remove_ops = 3; wm.idx_present_rm = 2;
        st_expect_sub_free(&wst, "U0", sub0_gen);
        st_expect_rx_free(&wst, "R0");
        st_expect_cursor_retired(&wst, (uint64_t)MOQ_SESS_IDLE, now);
        PHASE("discard", session_begin_advance(s, now), &want, &wst, &wm,
              do { st_sub(&p->st, "U0", s, pl.u0); st_rx(&p->st, "R0", s, pl.r0);
                   st_observe_cursor(&p->st, s); } while (0));
        failures += check_retired(s, MOQ_REQ_SUBSCRIPTION, pl.u0, sub0_h,
                                  REQ_SUB0, "deferred_discard.U0");
        break;
    }
    case C_STAGED_DISCARD: {
        out_reset(&want); st_reset(&wst); model_reset(&wm);
        exp_event(&want, MOQ_EVENT_SUBSCRIBE_DONE, SYM_SUB0, SUB_STATUS,
                  SUB_SC, SUB_REASON);
        wm.catchup = 1; wm.rx_rescan = 1; wm.sub_pending = 1;
        /* The subscription free's by-id lookup, plus one alias lookup per
         * staged entry the discard scan resolves. */
        wm.idx_find_ops = 3; wm.idx_remove_ops = 2; wm.idx_present_rm = 1;
        wm.staged = 16;            /* the fixed ring is visited exactly once */
        st_expect_sub_free(&wst, "U0", sub0_gen);
        st_expect_sub_live(&wst, "U1", sub1_gen, sub_handle_at(s, pl.u1),
                           REQ_SUB1);
        st_set(&wst, "U1", "alias", 0x777ull);
        /*
         * BYTE-EXACT conservation of the retained record, plus the retirement
         * of the unresolved one and the aggregate byte budget -- a count and
         * two in_use bits would not have shown a corrupted payload.
         */
        st_add(&wst, "staged_count", 1);
        st_add(&wst, "staged_bytes", 8);
        st_add(&wst, "s0.in_use", 1);
        st_add(&wst, "s0.alias", 0x777ull);
        st_add(&wst, "s0.seq",   0);
        st_add(&wst, "s0.len",   8);
        st_add(&wst, "s0.bytes", 0xAAAAAAAAAAAAAAAAull);   /* exact payload */
        st_add(&wst, "s1.in_use", 0);
        st_add(&wst, "s1.alias",  0);
        st_add(&wst, "s1.len",    0);
        st_add(&wst, "s1.bytes",  0);
        /* The established alias still resolves to U1; the unresolved one does
         * not resolve at all -- checked through the PRODUCTION lookup. */
        st_add(&wst, "lookup_777", (uint64_t)(int64_t)pl.u1);
        st_add(&wst, "lookup_888", (uint64_t)(int64_t)-1);
        st_expect_cursor_retired(&wst, (uint64_t)MOQ_SESS_IDLE, now);
        PHASE("initial", session_begin_advance(s, now), &want, &wst, &wm,
              do { st_sub(&p->st, "U0", s, pl.u0); st_sub(&p->st, "U1", s, pl.u1);
                   st_add(&p->st, "staged_count", (uint64_t)s->staged_count);
                   st_add(&p->st, "staged_bytes", (uint64_t)s->staged_bytes);
                   st_staged(&p->st, "s0", s, 0);
                   st_staged(&p->st, "s1", s, 1);
                   st_add(&p->st, "lookup_777",
                          (uint64_t)(int64_t)sub_find_by_alias_subscriber(s, 0x777ull));
                   st_add(&p->st, "lookup_888",
                          (uint64_t)(int64_t)sub_find_by_alias_subscriber(s, 0x888ull));
                   st_observe_cursor(&p->st, s); } while (0));
        /* Every retirement in this fixture carries the retirement oracle,
         * this one included. */
        failures += check_retired(s, MOQ_REQ_SUBSCRIPTION, pl.u0, sub0_h,
                                  REQ_SUB0, "staged.U0");
        /* U1 keeps its own edge through the phase. */
        failures += check_live_edge(s, MOQ_REQ_SUBSCRIPTION, pl.u1,
                                    OG_DOM_REQ_RID, REQ_SUB1, "staged.U1");
        break;
    }
    default: break;
    }
#undef PHASE

    moq_session_destroy(s);
    return failures;
}

/* ================= close with readiness still pending ============= */

/*
 * The blocked advance is now a MEASURED, NAMED PHASE, not an unmeasured
 * precondition. Its blockers are captured NON-DESTRUCTIVELY, so the event
 * queue is still full when the close phase runs -- which is what keeps the
 * deferred terminals demonstrably pending AT the close transition.
 */
static int run_close_pending(const caps_t *caps_in, uint32_t seed,
                             phase_t *ph, size_t *nph, const char **sched_text)
{
    static char sched_buf[320];
    int failures = 0;
    const uint64_t now = 1000000ull;
    caps_t c = *caps_in;
    *nph = 0;

    moq_session_t *s = make_session(&c);
    MOQ_TEST_CHECK(s != NULL);
    if (!s) return failures;
    /* Owner-empty starting point: heads and sentinels before any arm. */
    failures += occ_audit_named(s, "close_with_pending.empty");
    place_t pl;
    if (!place_build(&pl, seed, &c, &failures)) { moq_session_destroy(s); return failures; }

    sched_t sc; sched_reset(&sc, seed);
    sched_add(&sc, OP_ARM_PUB, pl.p0, REQ_PUB0);
    sched_add(&sc, OP_ARM_SUB, pl.u0, REQ_SUB0);
    sched_finalize(&sc);
    for (size_t i = 0; i < sc.n; i++) op_apply(s, &sc.op[i], now);
    snprintf(sched_buf, sizeof(sched_buf), "%s", sc.text);
    *sched_text = sched_buf;

    symtab_t sym; sym_reset(&sym);
    sym_bind(&sym, pub_handle_at(s, pl.p0), SYM_PUB0);
    sym_bind(&sym, sub_handle_at(s, pl.u0), SYM_SUB0);
    uint32_t pub0_gen = s->publishes[pl.p0].generation;
    uint32_t sub0_gen = s->subs[pl.u0].generation;
    uint64_t pub0_h = pub_handle_at(s, pl.p0);
    uint64_t sub0_h = sub_handle_at(s, pl.u0);

    /* Declared blockers: exactly max_events PUBLISH_UNSUBSCRIBED records. */
    fill_event_queue(s);
    MOQ_TEST_CHECK(event_queue_full(s));
    MOQ_TEST_CHECK_EQ_U64((uint64_t)(s->event_tail - s->event_head),
                          (uint64_t)c.max_events);

    out_t want; state_t wst; model_t wm;

#define PHASE(NAME, CALL, WANT_OUT, WANT_ST, WANT_MODEL, CAPTURE, OBS_ST)  \
    do {                                                                   \
        phase_t *p = &ph[(*nph)++];                                        \
        p->name = (NAME);                                                  \
        out_reset(&p->got); st_reset(&p->st);                              \
        work_t before, after;                                              \
        work_read(&before);                                                \
        failures += occ_audit_named(s, "close_with_pending.pre_phase"); \
        CALL;                                                              \
        work_read(&after);                                                 \
        failures += occ_audit_named(s, "close_with_pending.post_call"); \
        work_delta(&before, &after, &p->w);                                \
        OBS_ST;                                                            \
        CAPTURE;                                                           \
        failures += out_check((WANT_OUT), &p->got, (NAME));                \
        failures += st_check((WANT_ST), &p->st, (NAME));                   \
        failures += model_check((WANT_MODEL), &p->w, (NAME));              \
    } while (0)

    /* PHASE 1 -- the blocked advance, measured. Both terminals are runnable and
     * neither can finalize; the blockers are conserved exactly. */
    out_reset(&want); st_reset(&wst); model_reset(&wm);
    for (uint32_t i = 0; i < c.max_events; i++)
        exp_event(&want, MOQ_EVENT_PUBLISH_UNSUBSCRIBED, 0, 0, 0, NULL);
    wm.catchup = 1; wm.rx_rescan = 2; wm.pub_pending = 1; wm.sub_pending = 1;
    st_expect_pub_ready(&wst, "P0", pub0_gen, pub0_h, REQ_PUB0, 1);
    st_expect_sub_ready(&wst, "U0", sub0_gen, sub0_h, REQ_SUB0, 1);
    st_add(&wst, "events_queued", c.max_events);
    st_expect_cursor_retired(&wst, (uint64_t)MOQ_SESS_IDLE, now);
    PHASE("blocked", session_begin_advance(s, now), &want, &wst, &wm,
          out_snapshot(s, &sym, &p->got),
          do { st_pub(&p->st, "P0", s, pl.p0); st_sub(&p->st, "U0", s, pl.u0);
               st_add(&p->st, "events_queued",
                      (uint64_t)(s->event_tail - s->event_head));
               st_observe_cursor(&p->st, s); } while (0));

    /* The blockers are still in place, so readiness is pending AT the close. */
    MOQ_TEST_CHECK(event_queue_full(s));
    MOQ_TEST_CHECK(s->publishes[pl.p0].done_pending);
    MOQ_TEST_CHECK(s->subs[pl.u0].done_pending);

    /* PHASE 2 -- the close itself. Every queued blocker is discarded and
     * exactly the terminal pair is surfaced. */
    out_reset(&want); st_reset(&wst); model_reset(&wm);
    exp_event(&want, MOQ_EVENT_SESSION_CLOSED, 0, 0x5ull, 0, "exact-work-close");
    exp_action_reason(&want, MOQ_ACTION_CLOSE_SESSION, 0x5ull, "exact-work-close");
    wm.catchup = 1; wm.rx_rescan = 2; wm.pub_pending = 1; wm.sub_pending = 1;
    wm.queued_event = c.max_events;   /* the discarded blockers, visited once */
    st_expect_pub_ready(&wst, "P0", pub0_gen, pub0_h, REQ_PUB0, 1);
    st_expect_sub_ready(&wst, "U0", sub0_gen, sub0_h, REQ_SUB0, 1);
    /* Observed BEFORE the drain: the close leaves exactly its own terminal
     * event queued, every blocker having been discarded. */
    st_add(&wst, "events_queued", 1);
    st_expect_cursor_retired(&wst, (uint64_t)MOQ_SESS_CLOSED, now);
    PHASE("close", MOQ_TEST_CHECK(moq_session_close(s, 0x5, "exact-work-close",
                                                    now) == MOQ_OK),
          &want, &wst, &wm,
          do { out_capture_events(s, &sym, &p->got);
               out_capture_actions(s, &sym, &p->got); } while (0),
          do { st_pub(&p->st, "P0", s, pl.p0); st_sub(&p->st, "U0", s, pl.u0);
               st_add(&p->st, "events_queued",
                      (uint64_t)(s->event_tail - s->event_head));
               st_observe_cursor(&p->st, s); } while (0));

    /* PHASE 3 -- no resurrection, and still capacity-invariant. */
    out_reset(&want); st_reset(&wst); model_reset(&wm);
    wm.catchup = 1;
    st_expect_pub_ready(&wst, "P0", pub0_gen, pub0_h, REQ_PUB0, 1);
    st_expect_sub_ready(&wst, "U0", sub0_gen, sub0_h, REQ_SUB0, 1);
    st_add(&wst, "events_queued", 0);
    st_expect_cursor_retired(&wst, (uint64_t)MOQ_SESS_CLOSED, now);
    PHASE("post_close", session_begin_advance(s, now), &want, &wst, &wm,
          do { out_capture_events(s, &sym, &p->got);
               out_capture_actions(s, &sym, &p->got); } while (0),
          do { st_pub(&p->st, "P0", s, pl.p0); st_sub(&p->st, "U0", s, pl.u0);
               st_add(&p->st, "events_queued",
                      (uint64_t)(s->event_tail - s->event_head));
               st_observe_cursor(&p->st, s); } while (0));
#undef PHASE
    moq_session_destroy(s);
    return failures;
}

/* ================= real subgroup close / reset lifecycle ========== */

/*
 * The subgroup terminal transitions driven through the PUBLIC API on a real
 * established session -- moq_session_close_subgroup() and
 * moq_session_reset_subgroup() -- rather than by writing MOQ_SG_CLOSING and
 * MOQ_SG_RESETTING into the pool. Their own outgoing actions are classified
 * exactly, and only then is the reap measured.
 *
 * The action queue is deliberately left non-empty while both transitions are
 * armed: REAP_SUBGROUPS is gated on a drained action queue, so the subgroups
 * survive to the measured call instead of being reaped by the transition's own
 * preamble. Draining is what makes the measured phase the reap.
 */
static int run_subgroup_lifecycle(const caps_t *caps_in, uint32_t seed,
                                  phase_t *ph, size_t *nph,
                                  const char **sched_text)
{
    static char sched_buf[64];
    int failures = 0;
    const uint64_t now = 1000000ull;
    const caps_t c = *caps_in;
    *nph = 0;
    snprintf(sched_buf, sizeof(sched_buf), "seed=%u (public lifecycle)", seed);
    *sched_text = sched_buf;

    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    moq_session_cfg_t extra = MOQ_SESSION_CFG_INIT;
    extra.max_subscriptions  = c.sub_cap;
    extra.max_publishes      = c.pub_cap;
    extra.max_open_subgroups = c.sg_cap;
    extra.max_data_streams   = c.rx_cap;
    extra.max_fetches        = c.fetch_cap;
    extra.max_actions        = c.max_actions;
    extra.max_events         = c.max_events;
    extra.output_scratch_size = c.scratch;

    moq_session_t *cl = NULL, *sv = NULL;
    establish_pair(&alloc, 10, 10, &cl, &sv, &extra, &extra);
    MOQ_TEST_CHECK(cl != NULL && sv != NULL);
    if (!cl || !sv) return failures;
    /* ESTABLISHED but OWNER-EMPTY: setup allocates no request owner, so both
     * sides must start from intact heads and sentinels. */
    failures += occ_audit_named(cl, "subgroup_close_reset.client_empty");
    failures += occ_audit_named(sv, "subgroup_close_reset.server_empty");

    /* Real subscription: subscribe on the client, accept on the server. */
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
    moq_namespace_t ns = { ns_parts, 1 };
    moq_subscribe_cfg_t sub_cfg;
    moq_subscribe_cfg_init(&sub_cfg);
    sub_cfg.track_namespace = ns;
    sub_cfg.track_name = MOQ_BYTES_LITERAL("t");
    moq_subscription_t sub_h;
    MOQ_TEST_CHECK(moq_session_subscribe(cl, &sub_cfg, now, &sub_h) == MOQ_OK);
    pump_actions_to_peer(cl, sv, now);

    /* The "real established pair" precondition is CLASSIFIED, not assumed: the
     * server's first event must be exactly one SUBSCRIBE_REQUEST carrying a
     * valid subscription handle, and nothing else may be queued behind it. */
    moq_event_t ev;
    MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
    MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_SUBSCRIBE_REQUEST);
    moq_subscription_t sv_sub = MOQ_SUBSCRIPTION_INVALID;
    if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST)
        sv_sub = ev.u.subscribe_request.sub;
    moq_event_cleanup(&ev);
    MOQ_TEST_CHECK(moq_subscription_is_valid(sv_sub));
    MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 0);

    moq_accept_subscribe_cfg_t acc;
    moq_accept_subscribe_cfg_init(&acc);
    MOQ_TEST_CHECK(moq_session_accept_subscribe(sv, sv_sub, &acc, now) == MOQ_OK);
    pump_actions_to_peer(sv, cl, now);
    /* The client sees exactly one SUBSCRIBE_OK for the handle it created. */
    MOQ_TEST_CHECK(moq_session_poll_events(cl, &ev, 1) == 1);
    MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_SUBSCRIBE_OK);
    if (ev.kind == MOQ_EVENT_SUBSCRIBE_OK)
        MOQ_TEST_CHECK(moq_subscription_eq(ev.u.subscribe_ok.sub, sub_h));
    moq_event_cleanup(&ev);
    MOQ_TEST_CHECK(moq_session_poll_events(cl, &ev, 1) == 0);
    MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 0);
    moq_action_t act;
    while (moq_session_poll_actions(sv, &act, 1) == 1) moq_action_cleanup(&act);

    /* Two real subgroups on the publisher side. */
    moq_subgroup_cfg_t sg_cfg;
    moq_subgroup_cfg_init(&sg_cfg);
    /*
     * The track alias is the subscription's identity and is captured BEFORE
     * either open, so a coherent open-path mutation to the live alias and the
     * emitted headers cannot authorize its own expectation.
     */
    const size_t sv_sub_slot = moq_handle_slot(sv_sub._opaque);
    /* A product result outside the pool is a BOUNDED diagnostic, never an
     * out-of-range read: slot_ok() reports it and the case stops here. */
    if (!slot_ok(sv_sub_slot, sv->sub_cap, "sub", &failures)) {
        moq_session_destroy(cl); moq_session_destroy(sv); return failures;
    }
    const uint64_t sub_alias = sv->subs[sv_sub_slot].track_alias;

    /* DISTINCT non-default values in every field, so the two opens cannot
     * exchange identities and still compare equal. */
    const uint64_t GA_GROUP = 7,  GA_SUBGROUP = 1;  const uint8_t GA_PRIO = 129;
    const uint64_t GB_GROUP = 11, GB_SUBGROUP = 2;  const uint8_t GB_PRIO = 200;
    sg_cfg.group_id = GA_GROUP; sg_cfg.subgroup_id = GA_SUBGROUP;
    sg_cfg.publisher_priority = GA_PRIO;
    moq_subgroup_handle_t sg_a, sg_b;
    MOQ_TEST_CHECK(moq_session_open_subgroup(sv, sv_sub, &sg_cfg, now, &sg_a)
                   == MOQ_OK);
    sg_cfg.group_id = GB_GROUP; sg_cfg.subgroup_id = GB_SUBGROUP;
    sg_cfg.publisher_priority = GB_PRIO;
    MOQ_TEST_CHECK(moq_session_open_subgroup(sv, sv_sub, &sg_cfg, now, &sg_b)
                   == MOQ_OK);
    size_t sa = moq_handle_slot(sg_a._opaque), sb = moq_handle_slot(sg_b._opaque);
    if (!slot_ok(sa, sv->sg_cap, "sg", &failures) ||
        !slot_ok(sb, sv->sg_cap, "sg", &failures)) {
        moq_session_destroy(cl); moq_session_destroy(sv); return failures;
    }
    /*
     * Both halves of the fixture are now fully armed -- the real subscription
     * on each side and the two real subgroups on the server. The phase audits
     * that follow cover the measured server; this one keeps the CLIENT half
     * from being assumed valid merely because nothing measures it.
     */
    failures += occ_audit_named(cl, "subgroup_close_reset.client_armed");
    failures += occ_audit_named(sv, "subgroup_close_reset.server_armed");

    /*
     * The two stream refs are captured BEFORE the terminal calls and are the
     * EXPECTATION from here on. Reading them back afterwards would let a
     * coherent owner-plus-output mutation authorize itself.
     */
    const uint64_t ref_a = sv->subgroups[sa].stream_ref._v;
    const uint64_t ref_b = sv->subgroups[sb].stream_ref._v;
    /* Re-asserted after both opens: the live alias must still be the one the
     * expected headers were built from. */
    MOQ_TEST_CHECK_EQ_U64(sv->subs[sv_sub_slot].track_alias, sub_alias);
    const uint32_t gen_a = sv->subgroups[sa].generation;
    const uint32_t gen_b = sv->subgroups[sb].generation;
    MOQ_TEST_CHECK(ref_a != 0 && ref_b != 0 && ref_a != ref_b);

    /* The opens' own header writes, classified rather than blind-drained. */
    {
        symtab_t nosym; sym_reset(&nosym);
        out_t opens; out_reset(&opens);
        out_capture_actions(sv, &nosym, &opens);
        /*
         * The expected header bytes are ENCODED INDEPENDENTLY from the declared
         * configuration through the session's own profile encoder -- not copied
         * from the observation. An arbitrary or malformed header on the right
         * stream no longer satisfies the precondition. The track alias is the
         * subscription's identity, captured before the opens.
         */
        uint8_t hbuf_a[64], hbuf_b[64];
        moq_buf_writer_t hw;
        moq_subgroup_header_encode_args_t ha, hb;
        memset(&ha, 0, sizeof(ha)); memset(&hb, 0, sizeof(hb));
        ha.track_alias = sub_alias; ha.group_id = GA_GROUP;
        ha.subgroup_id = GA_SUBGROUP; ha.publisher_priority = GA_PRIO;
        hb.track_alias = sub_alias; hb.group_id = GB_GROUP;
        hb.subgroup_id = GB_SUBGROUP; hb.publisher_priority = GB_PRIO;
        moq_buf_writer_init(&hw, hbuf_a, sizeof(hbuf_a));
        MOQ_TEST_CHECK(sv->profile->encode_subgroup_header(sv, &hw, &ha) >= 0);
        size_t hlen_a = moq_buf_writer_offset(&hw);
        moq_buf_writer_init(&hw, hbuf_b, sizeof(hbuf_b));
        MOQ_TEST_CHECK(sv->profile->encode_subgroup_header(sv, &hw, &hb) >= 0);
        size_t hlen_b = moq_buf_writer_offset(&hw);
        /* The two encodings must differ, or the case could not tell them apart. */
        MOQ_TEST_CHECK(hlen_a != hlen_b ||
                       memcmp(hbuf_a, hbuf_b, hlen_a) != 0);

        out_t wo; out_reset(&wo);
        rec_t *r0 = out_push(&wo);
        if (r0) { r0->is_action = 1; r0->kind = MOQ_ACTION_SEND_DATA;
                  r0->stream_ref = ref_a; r0->code = hlen_a; r0->aux = 0;
                  rec_set_reason(r0, hbuf_a, hlen_a); }
        rec_t *r1 = out_push(&wo);
        if (r1) { r1->is_action = 1; r1->kind = MOQ_ACTION_SEND_DATA;
                  r1->stream_ref = ref_b; r1->code = hlen_b; r1->aux = 0;
                  rec_set_reason(r1, hbuf_b, hlen_b); }
        failures += out_check(&wo, &opens, "subgroup_opens");
        MOQ_TEST_CHECK_EQ_SIZE(opens.n, 2u);
    }
    MOQ_TEST_CHECK(sv->action_head == sv->action_tail);

    /* The opens' own header writes are drained here, so the queue the two
     * terminal transitions leave behind is exactly their own two actions. */
    while (moq_session_poll_actions(sv, &act, 1) == 1) moq_action_cleanup(&act);
    MOQ_TEST_CHECK(sv->action_head == sv->action_tail);

    symtab_t sym; sym_reset(&sym);
    out_t want; state_t wst; model_t wm;

#define PHASE(NAME, CALL, WANT_OUT, WANT_ST, WANT_MODEL, OBS_ST)          \
    do {                                                                  \
        phase_t *p = &ph[(*nph)++];                                       \
        p->name = (NAME);                                                 \
        out_reset(&p->got); st_reset(&p->st);                             \
        work_t before, after;                                             \
        work_read(&before);                                               \
        failures += occ_audit_named(sv, "subgroup_close_reset.pre_phase"); \
        CALL;                                                             \
        work_read(&after);                                                \
        failures += occ_audit_named(sv, "subgroup_close_reset.post_call"); \
        work_delta(&before, &after, &p->w);                               \
        OBS_ST;                                                           \
        out_capture_events(sv, &sym, &p->got);                            \
        out_capture_actions(sv, &sym, &p->got);                           \
        failures += out_check((WANT_OUT), &p->got, (NAME));               \
        failures += st_check((WANT_ST), &p->st, (NAME));                  \
        failures += model_check((WANT_MODEL), &p->w, (NAME));             \
    } while (0)

    /*
     * The two terminal transitions, driven publicly. Their own actions are NOT
     * drained here, so the queue stays non-empty and REAP_SUBGROUPS cannot run
     * inside either call's preamble -- proven by the state view: both entries
     * are still CLOSING/RESETTING afterwards.
     */
    {
        phase_t *p = &ph[(*nph)++];
        p->name = "arm_close_reset";
        out_reset(&p->got); st_reset(&p->st);
        work_t before, after;
        work_read(&before);
    failures += occ_audit_named(sv, "subgroup_close_reset.pre_phase");
        MOQ_TEST_CHECK(moq_session_close_subgroup(sv, sg_a, now) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_reset_subgroup(sv, sg_b, 0x4, now) == MOQ_OK);
        work_read(&after);
        failures += occ_audit_named(sv, "subgroup_close_reset.post_call");
        work_delta(&before, &after, &p->w);
        /* Actions deliberately NOT captured: the queue must stay non-empty. */
        st_sg(&p->st, "GA", sv, sa);
        st_sg(&p->st, "GB", sv, sb);
        st_add(&p->st, "actions_queued",
               (uint64_t)(sv->action_tail - sv->action_head));
        st_reset(&wst);
        st_expect_sg_armed(&wst, "GA", (uint64_t)MOQ_SG_CLOSING, gen_a, ref_a,
                           sv_sub._opaque);
        st_expect_sg_armed(&wst, "GB", (uint64_t)MOQ_SG_RESETTING, gen_b, ref_b,
                           sv_sub._opaque);
        st_add(&wst, "actions_queued", 2);
        failures += st_check(&wst, &p->st, "arm_close_reset");
        model_reset(&wm);
        wm.catchup = 2;    /* two public advancing calls, one sweep apiece */
        failures += model_check(&wm, &p->w, "arm_close_reset");
    }

    /* Classify the two transitions' own outgoing actions exactly. */
    {
        out_t got; out_reset(&got);
        out_capture_actions(sv, &sym, &got);
        out_reset(&want);
        rec_t *r = out_push(&want);
        if (r) { r->is_action = 1; r->kind = MOQ_ACTION_SEND_DATA;
                 r->stream_ref = ref_a;    /* captured BEFORE the transition */
                 r->stream_count = 1;      /* the close's FIN */
                 r->code = 0;              /* empty header */
                 r->aux = 0; }             /* and NO owned payload */
        exp_action(&want, MOQ_ACTION_RESET_DATA, ref_b, 0x4ull);
        failures += out_check(&want, &got, "transition_actions");
    }

    /* Measured: with the queue drained, the reap runs. */
    out_reset(&want); st_reset(&wst); model_reset(&wm);
    wm.catchup = 1; wm.sg_charged = 2;
    st_expect_sg_free(&wst, "GA", gen_a);
    st_expect_sg_free(&wst, "GB", gen_b);
    st_add(&wst, "actions_queued", 0);
    PHASE("reap", session_begin_advance(sv, now), &want, &wst, &wm,
          do { st_sg(&p->st, "GA", sv, sa); st_sg(&p->st, "GB", sv, sb);
               st_add(&p->st, "actions_queued",
                      (uint64_t)(sv->action_tail - sv->action_head)); } while (0));

    /* Idempotent: nothing is reaped twice. */
    out_reset(&want); st_reset(&wst); model_reset(&wm);
    wm.catchup = 1;
    st_expect_sg_free(&wst, "GA", gen_a);
    st_expect_sg_free(&wst, "GB", gen_b);
    st_add(&wst, "actions_queued", 0);
    PHASE("reap_idempotent", session_begin_advance(sv, now), &want, &wst, &wm,
          do { st_sg(&p->st, "GA", sv, sa); st_sg(&p->st, "GB", sv, sb);
               st_add(&p->st, "actions_queued",
                      (uint64_t)(sv->action_tail - sv->action_head)); } while (0));
#undef PHASE

    moq_session_destroy(cl);
    moq_session_destroy(sv);
    return failures;
}

/* ================= matrix driver ================================== */

typedef int (*runner_fn)(const caps_t *, uint32_t, phase_t *, size_t *,
                         const char **);

typedef struct { int case_id; runner_fn fn; const char *name; } entry_t;

static int run_case_entry(const caps_t *c, uint32_t seed, phase_t *ph,
                          size_t *nph, const char **txt);

/* Thread the case id through a file-local so the generic driver can call both
 * runners through one signature. */
static int g_case_id;

static int run_case_entry(const caps_t *c, uint32_t seed, phase_t *ph,
                          size_t *nph, const char **txt)
{
    return run_case(g_case_id, c, seed, ph, nph, txt);
}

/*
 * PRODUCER COVERAGE. Every configured-capacity site must perform nonzero
 * declared work in at least one fixture. Removing a site's increment therefore
 * fails this assertion in BOTH binaries -- it is not merely a change in RED
 * prose, which is what the earlier "M2-M5 kill" claim wrongly rested on.
 */
static work_t g_producer_seen;

static void producer_note(const work_t *w)
{
#define ACC(f) if (w->f > g_producer_seen.f) g_producer_seen.f = w->f
    ACC(sg_reap); ACC(sg_deadline); ACC(close_sg);
    ACC(pub_reap); ACC(retry_pub);
    ACC(sub_reap); ACC(retry_sub); ACC(fwd_pending);
    ACC(rx_stop); ACC(close_rx); ACC(deferred_rx);
    ACC(join); ACC(join_resolve); ACC(join_reject); ACC(staged);
    ACC(sg_charged); ACC(rx_charged); ACC(pub_pending); ACC(sub_pending);
    ACC(queued_action); ACC(queued_event); ACC(tomb); ACC(auth_staging);
    ACC(index_find); ACC(index_rm_search); ACC(index_rm_backshift);
    ACC(index_find_calls); ACC(index_rm_calls);
    ACC(catchup); ACC(rx_rescan);
#undef ACC
}

static int producer_check(void)
{
    int failures = 0;
    struct { const char *name; uint64_t v; int must_be_zero; } rows[] = {
        { "sg_reap",        g_producer_seen.sg_reap,        0 },
        { "sg_deadline",    g_producer_seen.sg_deadline,    0 },
        { "close_sg",       g_producer_seen.close_sg,       0 },
        { "pub_reap",       g_producer_seen.pub_reap,       0 },
        { "retry_pub",      g_producer_seen.retry_pub,      0 },
        { "sub_reap",       g_producer_seen.sub_reap,       0 },
        { "retry_sub",      g_producer_seen.retry_sub,      0 },
        { "fwd_pending",    g_producer_seen.fwd_pending,    0 },
        { "rx_stop",        g_producer_seen.rx_stop,        0 },
        { "close_rx",       g_producer_seen.close_rx,       0 },
        { "deferred_rx",    g_producer_seen.deferred_rx,    0 },
        { "join",           g_producer_seen.join,           0 },
        { "join_resolve",   g_producer_seen.join_resolve,   0 },
        { "join_reject",    g_producer_seen.join_reject,    0 },
        { "sg_charged",     g_producer_seen.sg_charged,     0 },
        { "rx_charged",     g_producer_seen.rx_charged,     0 },
        { "pub_pending",    g_producer_seen.pub_pending,    0 },
        { "sub_pending",    g_producer_seen.sub_pending,    0 },
        { "queued_action",  g_producer_seen.queued_action,  0 },
        { "queued_event",   g_producer_seen.queued_event,   0 },
        { "auth_staging",   g_producer_seen.auth_staging,   0 },
        { "index_find",     g_producer_seen.index_find,     0 },
        { "index_rm_search", g_producer_seen.index_rm_search, 0 },
        { "catchup",        g_producer_seen.catchup,        0 },
        { "rx_rescan",      g_producer_seen.rx_rescan,      0 },
        { "staged",         g_producer_seen.staged,         0 },
        { "tomb",           g_producer_seen.tomb,           0 },
        { "index_rm_backshift", g_producer_seen.index_rm_backshift, 0 },
        { "index_find_calls",   g_producer_seen.index_find_calls,   0 },
        { "index_rm_calls",     g_producer_seen.index_rm_calls,     0 },
    };
    printf("PRODUCER COVERAGE\n");
    for (size_t i = 0; i < sizeof(rows) / sizeof(rows[0]); i++) {
        printf("  %-18s max=%llu%s\n", rows[i].name,
               (unsigned long long)rows[i].v,
               rows[i].must_be_zero ? "  (classified unreachable)" : "");
        if (rows[i].must_be_zero) {
            MOQ_TEST_CHECK_EQ_U64(rows[i].v, 0u);
        } else if (rows[i].v == 0) {
            printf("    UNEXERCISED: no fixture performs work at this site\n");
            MOQ_TEST_CHECK(rows[i].v != 0);
        }
    }
    return failures;
}

/*
 * MODEL SELF-CHECK. Parallel to the comparator probe: an identical declared
 * model passes, a missing declaration fails, and changing each absolute member
 * one at a time is detected AND named. Without this, deleting an MCHK() line
 * leaves the controls green while producer coverage still reports the counter
 * fired.
 */
static int model_selfcheck(void)
{
    int failures = 0;
    work_t g; memset(&g, 0, sizeof(g));
    g.sg_charged = 2; g.rx_charged = 3; g.pub_pending = 5; g.sub_pending = 7;
    g.queued_action = 11; g.queued_event = 13; g.tomb = 17; g.auth_staging = 19;
    g.catchup = 23; g.rx_rescan = 29;
    g.index_find = 4; g.index_rm_search = 4; g.index_rm_backshift = 4;
    g.index_find_calls = 4; g.index_rm_calls = 4;

    model_t m; model_reset(&m);
    m.sg_charged = 2; m.rx_charged = 3; m.pub_pending = 5; m.sub_pending = 7;
    m.queued_action = 11; m.queued_event = 13; m.tomb = 17; m.auth_staging = 19;
    m.catchup = 23; m.rx_rescan = 29;
    m.idx_find_ops = 4; m.idx_remove_ops = 4; m.idx_present_rm = 4;
    m.staged = 16; g.staged = 16;
    /* Capacity members differ deliberately: the model must ignore them. */
    g.sg_reap = 512; g.pub_reap = 256; g.sub_reap = 1024;
    {
        char b[192];
        MOQ_TEST_CHECK(model_diff(&m, &g, b, sizeof(b)) == NULL);
    }

    /* A phase with no declared model must fail. */
    {
        model_t undeclared; memset(&undeclared, 0, sizeof(undeclared));
        char b[192];
        const char *d = model_diff(&undeclared, &g, b, sizeof(b));
        MOQ_TEST_CHECK(d != NULL && strstr(d, "MODEL_UNDECLARED") != NULL);
    }

    struct { const char *name; size_t off; } members[] = {
        { "sg_charged",    offsetof(work_t, sg_charged) },
        { "rx_charged",    offsetof(work_t, rx_charged) },
        { "pub_pending",   offsetof(work_t, pub_pending) },
        { "sub_pending",   offsetof(work_t, sub_pending) },
        { "queued_action", offsetof(work_t, queued_action) },
        { "queued_event",  offsetof(work_t, queued_event) },
        { "tomb",          offsetof(work_t, tomb) },
        { "auth_staging",  offsetof(work_t, auth_staging) },
        { "catchup",       offsetof(work_t, catchup) },
        { "rx_rescan",     offsetof(work_t, rx_rescan) },
        { "index_find",         offsetof(work_t, index_find) },
        { "index_rm_search",    offsetof(work_t, index_rm_search) },
        { "index_rm_backshift", offsetof(work_t, index_rm_backshift) },
        { "index_find_calls",   offsetof(work_t, index_find_calls) },
        { "index_rm_calls",     offsetof(work_t, index_rm_calls) },
        { "staged",             offsetof(work_t, staged) },
    };
    const size_t n = sizeof(members) / sizeof(members[0]);
    MOQ_TEST_CHECK_EQ_SIZE(n, 16u);   /* 13 exact + 3 bounded index loops */
    for (size_t i = 0; i < n; i++) {
        work_t c = g;
        uint64_t *f = (uint64_t *)((char *)&c + members[i].off);
        *f += 100;                       /* past the bound for the index loops */
        char b[192];
        const char *d = model_diff(&m, &c, b, sizeof(b));
        if (!d || !strstr(d, members[i].name)) {
            printf("  MODEL SELF-CHECK FAILURE: a change to '%s' was %s\n",
                   members[i].name, d ? "misnamed" : "not detected");
            printf("    reported: %s\n", d ? d : "(nothing)");
            MOQ_TEST_CHECK(d != NULL && strstr(d, members[i].name) != NULL);
        }
    }
    printf("MODEL SELF-CHECK: %zu members, each detected and named\n", n);
    return failures;
}

/*
 * COMPARATOR SELF-CHECK. Hand-built images, no session involved: identical
 * images must agree, and changing each configured-capacity member one at a
 * time must be detected AND named. This covers every member the comparator
 * claims to compare, so a member silently dropped from it is a test failure.
 */
static int comparator_selfcheck(void)
{
    int failures = 0;
    char buf[256];
    work_t a; memset(&a, 0, sizeof(a));
    a.sg_reap = 3; a.sg_deadline = 5; a.close_sg = 7;
    a.pub_reap = 11; a.retry_pub = 13;
    a.sub_reap = 17; a.retry_sub = 19; a.fwd_pending = 23;
    a.rx_stop = 29; a.close_rx = 31; a.deferred_rx = 37;
    a.join = 41; a.join_resolve = 61; a.join_reject = 67; a.staged = 43;
    a.index_find = 47; a.index_rm_search = 53; a.index_rm_backshift = 59;
    /* Members OUTSIDE the comparator differ deliberately: it must ignore them. */
    work_t b = a;
    b.queued_action = 99; b.queued_event = 98; b.tomb = 97; b.auth_staging = 96;
    b.catchup = 92; b.rx_rescan = 91;
    b.sg_charged = 90; b.rx_charged = 89; b.pub_pending = 88; b.sub_pending = 87;
    b.index_find_calls = 86; b.index_rm_calls = 85;
    MOQ_TEST_CHECK(work_capacity_diff(&a, &b, buf, sizeof(buf)) == NULL);

    struct { const char *name; size_t off; } members[] = {
        { "sg_reap",     offsetof(work_t, sg_reap) },
        { "sg_deadline", offsetof(work_t, sg_deadline) },
        { "close_sg",    offsetof(work_t, close_sg) },
        { "pub_reap",    offsetof(work_t, pub_reap) },
        { "retry_pub",   offsetof(work_t, retry_pub) },
        { "sub_reap",    offsetof(work_t, sub_reap) },
        { "retry_sub",   offsetof(work_t, retry_sub) },
        { "fwd_pending", offsetof(work_t, fwd_pending) },
        { "rx_stop",     offsetof(work_t, rx_stop) },
        { "close_rx",    offsetof(work_t, close_rx) },
        { "deferred_rx", offsetof(work_t, deferred_rx) },
        { "join",         offsetof(work_t, join) },
        { "join_resolve", offsetof(work_t, join_resolve) },
        { "join_reject",  offsetof(work_t, join_reject) },
        { "staged",      offsetof(work_t, staged) },
        { "index_find",         offsetof(work_t, index_find) },
        { "index_rm_search",    offsetof(work_t, index_rm_search) },
    };
    const size_t n = sizeof(members) / sizeof(members[0]);
    /* 14 configured-POOL scan sites + the fixed staged ring + the 2 index
     * loops that ARE cross-capacity equality members. The staged ring's slot
     * count is a compile-time constant, not a configured capacity, and is
     * counted separately for that reason. */
    MOQ_TEST_CHECK_EQ_SIZE(n, 17u);
    for (size_t i = 0; i < n; i++) {
        work_t c = a;
        uint64_t *f = (uint64_t *)((char *)&c + members[i].off);
        *f += 1;
        const char *d = work_capacity_diff(&a, &c, buf, sizeof(buf));
        if (!d || !strstr(d, members[i].name)) {
            printf("  COMPARATOR SELF-CHECK FAILURE: a change to '%s' was %s\n",
                   members[i].name, d ? "misnamed" : "not detected");
            printf("    reported: %s\n", d ? d : "(no difference)");
            MOQ_TEST_CHECK(d != NULL && strstr(d, members[i].name) != NULL);
        }
    }
    /*
     * And the exclusion is DELIBERATE, not an omission: changing
     * index_rm_backshift alone must be IGNORED, while every member above is
     * still detected. Without this a silently dropped member would look exactly
     * like a ruled-out one.
     */
    {
        work_t c = a;
        c.index_rm_backshift += 7;
        const char *d = work_capacity_diff(&a, &c, buf, sizeof(buf));
        if (d != NULL) {
            printf("  COMPARATOR SELF-CHECK FAILURE: 'index_rm_backshift' is a "
                   "ruled-out member but was reported: %s\n", d);
            MOQ_TEST_CHECK(d == NULL);
        }
    }
    printf("COMPARATOR SELF-CHECK: %zu members, each detected and named; "
           "index_rm_backshift deliberately ignored\n", n);
    return failures;
}

static int drive(const char *name, runner_fn fn, uint32_t seed)
{
    int failures = 0;
    row_t rows[ROWS_MAX];
    size_t nrows = build_rows(rows);

    phase_t ph[ROWS_MAX][PHASE_MAX];
    size_t  nph[ROWS_MAX];
    const char *txt = "";

    printf("CASE %-24s seed=%u\n", name, seed);
    for (size_t i = 0; i < nrows; i++) {
        failures += fn(&rows[i].c, seed, ph[i], &nph[i], &txt);
        printf("  [%s] %-18s %s\n", rows[i].axis, rows[i].tag, txt);
        for (size_t p = 0; p < nph[i]; p++) {
            char tag[64];
            snprintf(tag, sizeof(tag), "%s", ph[i][p].name);
            work_print(tag, &ph[i][p].w);
            producer_note(&ph[i][p].w);
        }
    }

#ifndef EXACT_WORK_NO_COMPLEXITY
    /*
     * COMPLEXITY, per axis and PER PHASE. Each axis has its own baseline (its
     * first row), so a comparison never crosses two axes; each phase is
     * compared on its own, so a scan moving out of a refused call and into its
     * recovery cannot cancel in a total.
     */
    for (size_t i = 0; i < nrows; i++) {
        size_t base = 0;
        while (base < i && strcmp(rows[base].axis, rows[i].axis) != 0) base++;
        if (base == i) continue;                 /* this row IS its axis baseline */
        if (nph[base] != nph[i]) {
            printf("  PHASE-COUNT MISMATCH %s vs %s\n", rows[base].tag, rows[i].tag);
            MOQ_TEST_CHECK(nph[base] == nph[i]);
            continue;
        }
        for (size_t p = 0; p < nph[i]; p++) {
            char dbuf[256];
            const char *f = work_capacity_diff(&ph[base][p].w, &ph[i][p].w,
                                               dbuf, sizeof(dbuf));
            if (f) {
                printf("  COMPLEXITY FAILURE case=%s axis=%s phase=%s fields=%s "
                       "%s\n", name, rows[i].axis, ph[i][p].name, f, txt);
                printf("    the live owner set is identical in both rows; work "
                       "scaled with configured capacity\n");
                printf("    baseline %s\n", rows[base].tag);
                work_print("baseline", &ph[base][p].w);
                printf("    scaled   %s\n", rows[i].tag);
                work_print("scaled", &ph[i][p].w);
                MOQ_TEST_CHECK(!"preamble work follows the live owner set");
            }
        }
    }
#endif
    return failures;
}

/* ================= placement matrix =============================== */

/*
 * The same symbolic model at LOW / MIDDLE / cap-1 slots in all four pools.
 * "High slot" is capacity-relative here, which the fixed low-slot capacity
 * matrix deliberately is not: drawing every slot from the smallest pool keeps
 * the live set identical across capacities, but slot 7 is not a high slot in a
 * 512-entry pool.
 *
 * Work must be EQUAL and outcomes EQUIVALENT under symbolic renaming, so a
 * correction that skipped a tail entry, or charged work proportional to a
 * physical slot index, fails here.
 */
#define PLACE_RX_REF 0x5A5Aull

typedef enum { PLACE_LOW = 0, PLACE_MID, PLACE_HIGH, PLACE_COUNT } place_kind_t;

static const char *place_name(place_kind_t k)
{
    return k == PLACE_LOW ? "low" : (k == PLACE_MID ? "middle" : "cap-1");
}

static size_t place_slot(place_kind_t k, uint32_t cap)
{
    switch (k) {
    case PLACE_LOW:  return 0;
    case PLACE_MID:  return cap / 2u;
    default:         return cap - 1u;
    }
}

/*
 * Second placement model: an EXPIRED owner whose bound stream's STOP cannot be
 * queued. Its lookup path early-exits at the owner, so a scan proportional to a
 * physical slot index -- rather than to the live set -- shows up here as
 * differing work between low, middle and cap-1.
 */
static int run_placement_blocked(place_kind_t k, work_t *w_out, out_t *o_out)
{
    int failures = 0;
    const uint64_t now = 1000000ull;
    caps_t c; caps_default(&c);
    c.max_actions = 2;
    moq_session_t *s = make_session(&c);
    MOQ_TEST_CHECK(s != NULL);
    if (!s) return failures;
    /* Owner-empty starting point: heads and sentinels before any arm. */
    failures += occ_audit_named(s, "placement_blocked.empty");

    size_t ps = place_slot(k, c.pub_cap), rs = place_slot(k, c.rx_cap);
    if (!slot_ok(ps, c.pub_cap, "pub", &failures) ||
        !slot_ok(rs, c.rx_cap, "rx", &failures)) {
        moq_session_destroy(s); return failures;
    }
    arm_ready_pub(s, ps, REQ_PUB0);
    bind_rx_pub(s, rs, s->publishes[ps].handle, PLACE_RX_REF);
    fill_action_queue(s);
    MOQ_TEST_CHECK(action_queue_full(s));

    symtab_t sym; sym_reset(&sym);
    sym_bind(&sym, pub_handle_at(s, ps), SYM_PUB0);
    uint32_t gen = s->publishes[ps].generation;
    uint64_t h   = pub_handle_at(s, ps);

    work_t before, after;
    work_read(&before);
    failures += occ_audit_named(s, "placement_blocked.pre_call");
    session_begin_advance(s, now);
    work_read(&after);
    failures += occ_audit_named(s, "placement_blocked.post_call");
    work_delta(&before, &after, w_out);

    /* IMMEDIATE post-call snapshot: polling runs replay/refeed, so state and
     * topology are taken before any drain. */
    state_t got, wst; st_reset(&got); st_reset(&wst);
    st_pub(&got, "P0", s, ps);
    st_rx(&got, "R0", s, rs);
    st_expect_pub_ready(&wst, "P0", gen, h, REQ_PUB0, 1);
    st_expect_rx_bound_pub(&wst, "R0", PLACE_RX_REF, h,
                           (uint64_t)MOQ_RX_NEED_STOP);
    failures += st_check(&wst, &got, place_name(k));
    failures += check_live_edge(s, MOQ_REQ_PUBLISH, ps, OG_DOM_REQ_RID,
                                REQ_PUB0, place_name(k));

    out_reset(o_out);
    out_capture_events(s, &sym, o_out);
    out_capture_actions(s, &sym, o_out);
    out_t want; out_reset(&want);
    for (uint32_t i = 0; i < c.max_actions; i++)
        exp_action(&want, MOQ_ACTION_STOP_DATA, 0xB000ull, 0);
    failures += out_check(&want, o_out, place_name(k));

    model_t wm; model_reset(&wm);
    wm.catchup = 1; wm.rx_rescan = 1; wm.pub_pending = 1; wm.rx_charged = 1;
    failures += model_check(&wm, w_out, place_name(k));

    moq_session_destroy(s);
    return failures;
}

static int run_placement(place_kind_t k, work_t *w_out, out_t *o_out)
{
    int failures = 0;
    const uint64_t now = 1000000ull;
    caps_t c; caps_default(&c);
    moq_session_t *s = make_session(&c);
    MOQ_TEST_CHECK(s != NULL);
    if (!s) return failures;
    /* Owner-empty starting point: heads and sentinels before any arm. */
    failures += occ_audit_named(s, "placement_matrix.empty");

    size_t ps = place_slot(k, c.pub_cap), us = place_slot(k, c.sub_cap);
    size_t gs = place_slot(k, c.sg_cap),  rs = place_slot(k, c.rx_cap);
    if (!slot_ok(ps, c.pub_cap, "pub", &failures) ||
        !slot_ok(us, c.sub_cap, "sub", &failures) ||
        !slot_ok(gs, c.sg_cap, "sg", &failures)  ||
        !slot_ok(rs, c.rx_cap, "rx", &failures)) {
        moq_session_destroy(s); return failures;
    }

    arm_ready_pub(s, ps, REQ_PUB0);
    arm_ready_sub(s, us, REQ_SUB0);
    sweep_arm_closing_subgroup(s, gs);
    bind_rx_pub(s, rs, s->publishes[ps].handle, PLACE_RX_REF);

    symtab_t sym; sym_reset(&sym);
    sym_bind(&sym, pub_handle_at(s, ps), SYM_PUB0);
    sym_bind(&sym, sub_handle_at(s, us), SYM_SUB0);
    uint32_t pub_gen = s->publishes[ps].generation;
    uint32_t sub_gen = s->subs[us].generation;
    uint32_t sg_gen  = s->subgroups[gs].generation;
    uint64_t pub_h   = pub_handle_at(s, ps);
    uint64_t sub_h   = sub_handle_at(s, us);

    work_t before, after;
    work_read(&before);
    failures += occ_audit_named(s, "placement_matrix.pre_call");
    session_begin_advance(s, now);
    work_read(&after);
    failures += occ_audit_named(s, "placement_matrix.post_call");
    work_delta(&before, &after, w_out);

    /* Absolute outcome, so an "equal but both wrong" pair cannot pass. */
    /* IMMEDIATE post-call snapshot, before any drain. */
    /* FULL symbolically-normalized retirement, not four booleans. */
    state_t got, wst; st_reset(&got); st_reset(&wst);
    st_pub(&got, "P0", s, ps);
    st_sub(&got, "U0", s, us);
    st_sg(&got, "G0", s, gs);
    st_rx(&got, "R0", s, rs);
    st_expect_pub_free(&wst, "P0", pub_gen);
    st_expect_sub_free(&wst, "U0", sub_gen);
    st_expect_sg_free(&wst, "G0", sg_gen);
    st_expect_rx_free(&wst, "R0");
    failures += st_check(&wst, &got, place_name(k));
    failures += check_retired(s, MOQ_REQ_PUBLISH, ps, pub_h, REQ_PUB0,
                              place_name(k));
    failures += check_retired(s, MOQ_REQ_SUBSCRIPTION, us, sub_h, REQ_SUB0,
                              place_name(k));

    model_t wm; model_reset(&wm);
    /* Two owners enter STOP_STREAMS; the publication stops its bound stream
     * (1 + 1 rescan), the subscription has none (1). */
    wm.catchup = 1; wm.rx_rescan = 3; wm.pub_pending = 1; wm.sub_pending = 1;
    wm.rx_charged = 1; wm.sg_charged = 1;
    wm.idx_find_ops = 2; wm.idx_remove_ops = 4; wm.idx_present_rm = 3;
    failures += model_check(&wm, w_out, place_name(k));

    out_reset(o_out);
    out_capture_events(s, &sym, o_out);
    out_capture_actions(s, &sym, o_out);
    out_t want; out_reset(&want);
    exp_event(&want, MOQ_EVENT_PUBLISH_FINISHED, SYM_PUB0, PUB_STATUS, PUB_SC,
              PUB_REASON);
    exp_event(&want, MOQ_EVENT_SUBSCRIBE_DONE, SYM_SUB0, SUB_STATUS, SUB_SC,
              SUB_REASON);
    exp_action(&want, MOQ_ACTION_STOP_DATA, PLACE_RX_REF, 0);
    failures += out_check(&want, o_out, place_name(k));

    moq_session_destroy(s);
    return failures;
}

/*
 * WHOLE-RECORD conservation for an owner the measured call must not touch at
 * all.
 *
 * The field-level inventory below is an ABSOLUTE, declaration-built image and
 * stays -- it is what proves the fixture armed the owner it meant to, and it
 * gives readable failures. But it is a PROJECTION: any field it does not name
 * can be corrupted after one production call and repaired before the next, and
 * no projection closes that class. So once the absolute arm check has passed,
 * the complete object representation of each conserved owner is snapshotted
 * and re-compared after every call.
 *
 * This is CONSERVATION, not an adopted expectation: these owners are unrelated
 * to the teardown and owe it exactly zero mutation, so "identical to what it
 * was" is the whole contract and adopting it cannot launder a wrong shape --
 * the absolute check already fixed the shape.
 *
 * The three spans the entry can OWN are asserted empty at capture and at every
 * comparison, so a byte-identical record can never hide mutated bytes behind a
 * retained pointer.
 */
typedef struct {
    int     valid;
    size_t  slot;
    uint8_t bytes[sizeof(moq_sub_entry_t)];
} sub_snap_t;

static int sub_snap_spans_empty(moq_session_t *s, size_t slot, const char *who,
                                const char *where)
{
    int failures = 0;
    const moq_sub_entry_t *e = &s->subs[slot];
    if (e->req_recv_len != 0 || e->track_id_len != 0 ||
        e->done_reason_len != 0) {
        printf("      CONSERVATION FAIL-CLOSED (%s/%s): owned spans must be "
               "empty for a byte-exact record compare -- req_recv_len=%zu "
               "track_id_len=%zu done_reason_len=%zu\n", where, who,
               e->req_recv_len, e->track_id_len, e->done_reason_len);
        MOQ_TEST_CHECK(e->req_recv_len == 0 && e->track_id_len == 0 &&
                       e->done_reason_len == 0);
    }
    return failures;
}

/*
 * The entry's intrusive occupancy links are CONTAINER metadata, not owner
 * state: a neighbour leaving the list rewrites this owner's `occ_prev` without
 * touching anything the owner means. They are masked out of the byte image, so
 * the record stays whole in every semantic byte while the list remains free to
 * re-link around it.
 *
 * What replaces them is NOT the membership flag asserted below -- that alone
 * could not see a broken head, chain or reciprocity. The COMPLETE intrusive
 * topology is checked by occ_audit_named() at the same lifecycle checkpoints
 * this image is compared at, including after every advancing call and after
 * every late terminal.
 */
static void sub_snap_mask_links(uint8_t *bytes)
{
    memset(bytes + offsetof(moq_sub_entry_t, occ_next), 0,
           sizeof(((moq_sub_entry_t *)0)->occ_next));
    memset(bytes + offsetof(moq_sub_entry_t, occ_prev), 0,
           sizeof(((moq_sub_entry_t *)0)->occ_prev));
    memset(bytes + offsetof(moq_sub_entry_t, occ_linked), 0,
           sizeof(((moq_sub_entry_t *)0)->occ_linked));
}

static int sub_snap_capture(moq_session_t *s, size_t slot, sub_snap_t *snap,
                            const char *who)
{
    int failures = 0;
    snap->valid = 0;
    snap->slot  = slot;
    if (!slot_ok(slot, s->sub_cap, "sub", &failures)) return failures;
    failures += sub_snap_spans_empty(s, slot, who, "capture");
    /* A conserved owner must be a LIVE list member, and stay one. */
    MOQ_TEST_CHECK(s->subs[slot].occ_linked);
    memcpy(snap->bytes, &s->subs[slot], sizeof(snap->bytes));
    sub_snap_mask_links(snap->bytes);
    snap->valid = 1;
    return failures;
}

static int sub_snap_check(moq_session_t *s, const sub_snap_t *snap,
                          const char *who, const char *where)
{
    int failures = 0;
    if (!snap->valid) {
        printf("      CONSERVATION (%s/%s): no snapshot was captured\n",
               where, who);
        MOQ_TEST_CHECK(snap->valid);
        return failures;
    }
    if (!slot_ok(snap->slot, s->sub_cap, "sub", &failures)) return failures;
    failures += sub_snap_spans_empty(s, snap->slot, who, where);
    MOQ_TEST_CHECK(s->subs[snap->slot].occ_linked);
    uint8_t live[sizeof(moq_sub_entry_t)];
    memcpy(live, &s->subs[snap->slot], sizeof(live));
    sub_snap_mask_links(live);
    if (memcmp(live, snap->bytes, sizeof(snap->bytes)) != 0) {
        size_t off = 0;
        while (off < sizeof(snap->bytes) && live[off] == snap->bytes[off]) off++;
        printf("      CONSERVATION MISMATCH (%s/%s): slot %zu byte %zu of %zu "
               "changed, 0x%02X -> 0x%02X\n", where, who, snap->slot, off,
               sizeof(snap->bytes), (unsigned)snap->bytes[off],
               (unsigned)live[off]);
        MOQ_TEST_CHECK(!"a conserved owner's record changed");
    }
    return failures;
}

/*
 * The COMPLETE phase postcondition of the join-cleanup route, asserted after
 * the teardown and again after EVERY late terminal. A per-call check that
 * looked only at the drain count and the session state could not see an owner
 * corrupted after one call and repaired before the next, so the whole
 * declared image is re-compared each time.
 */
typedef struct {
    moq_session_t *s;
    size_t   u0, u1, u2;
    size_t   j[3], jf;
    uint64_t h0, h1, h2;
    uint64_t jh[3], jreq[3], jref[3];
    uint64_t jfh;
    uint32_t live_gen;
    sub_snap_t snap_u1, snap_u2;   /* whole-record conservation baselines */
} join_fix_t;

static int join_postcondition(const join_fix_t *f, const uint64_t *remaining,
                              size_t n_remaining, const char *tag)
{
    int failures = 0;
    moq_session_t *s = f->s;

    /* The complete intrusive topology, at the same point the semantic images
     * are compared -- a chain broken by this call and repaired by the next
     * would otherwise never be observed. */
    failures += occ_audit_named(s, tag);

    /* The exact remaining drain multiset, each entry with its declared reason;
     * a consumed ref is absent by construction of the multiset check. */
    failures += check_drain_set(s, remaining, n_remaining, tag);
    for (size_t i = 0; i < n_remaining; i++)
        MOQ_TEST_CHECK(drain_ref_reason(s, moq_stream_ref_from_u64(remaining[i]))
                       == MOQ_DRAIN_NORMAL);
    MOQ_TEST_CHECK(!drain_ref_contains(s, moq_stream_ref_from_u64(REF_SUB0)));
    MOQ_TEST_CHECK(!drain_ref_contains(s, moq_stream_ref_from_u64(REF_SUB1)));
    MOQ_TEST_CHECK(!drain_ref_contains(s, moq_stream_ref_from_u64(REF_SUB2)));

    /* U0 and every J stay fully retired, graph edges included. */
    failures += check_retired_streamref(s, MOQ_REQ_SUBSCRIPTION, f->u0, f->h0,
                                        REQ_SUB0, REF_SUB0, tag);
    for (size_t i = 0; i < 3; i++)
        failures += check_retired_streamref(s, MOQ_REQ_FETCH, f->j[i], f->jh[i],
                                            f->jreq[i], f->jref[i], tag);

    /*
     * U1/U2: the absolute declared inventory (semantic identity, readable
     * failures) AND byte-exact conservation of the whole record (closes the
     * class of every field the inventory does not name).
     */
    failures += sub_snap_check(s, &f->snap_u1, "U1", tag);
    failures += sub_snap_check(s, &f->snap_u2, "U2", tag);
    {
        state_t got, wst; st_reset(&got); st_reset(&wst);
        st_sub(&got, "U1", s, f->u1);
        st_sub(&got, "U2", s, f->u2);
        st_expect_sub_fwd_pending_d18(&wst, "U1", f->live_gen, f->h1, REQ_SUB1,
                                      REF_SUB1);
        st_expect_sub_fwd_pending_d18(&wst, "U2", f->live_gen, f->h2, REQ_SUB2,
                                      REF_SUB2);
        failures += st_check(&wst, &got, tag);
    }
    failures += check_live_edge(s, MOQ_REQ_SUBSCRIPTION, f->u1,
                                OG_DOM_REQ_STREAMREF, REF_SUB1, tag);
    failures += check_live_edge(s, MOQ_REQ_SUBSCRIPTION, f->u2,
                                OG_DOM_REQ_STREAMREF, REF_SUB2, tag);
    /* The FOREIGN join is untouched: it belongs to another subscription. */
    {
        state_t got, wst; st_reset(&got); st_reset(&wst);
        st_fetch(&got, "JF", s, f->jf);
        st_expect_fetch_join(&wst, "JF", f->live_gen, f->jfh, f->jf, REQ_JOIN3,
                             REF_JOIN3, REQ_SUB1, JOIN_START3);
        failures += st_check(&wst, &got, tag);
    }
    failures += check_live_edge(s, MOQ_REQ_FETCH, f->jf,
                                OG_DOM_REQ_STREAMREF, REF_JOIN3, tag);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(s), (int)MOQ_SESS_ESTABLISHED);
    return failures;
}

/*
 * THE BUFFERED-JOIN CLEANUP LIFECYCLE, on its real production route.
 *
 * BOUNDARY, stated rather than implied: this is NOT advancing-preamble work.
 * session_core_discard_pending_joins() runs from sub_free_entry() only when the
 * entry is MOQ_SUB_PENDING_PUBLISHER (session_subscribe.c:164), and a
 * PENDING_PUBLISHER entry can never carry the deferred SUBSCRIBE_DONE the
 * preamble finalizes -- done_pending is installed only for a SUBSCRIBER-role
 * owner. The route that really reaches it is the peer tearing down the request
 * bidi: request_stream_teardown() rejects the buffered joins, then frees the
 * subscription, whose free runs the defensive discard scan. So the join family
 * leaves the capacity matrix and is measured here, through that call.
 *
 * ONE PROFILE for every owner: draft-18, so U0, U1/U2 and every J are keyed by
 * their own request stream ref and hold exactly one edge each. A mixed graph --
 * draft-18 refs beside draft-16 by-id owners -- is a graph no session owns.
 *
 * The route runs THREE fetch-capacity scans, and all three are instrumented:
 * the reject preflight, the reject itself, and the defensive discard.
 */

static int run_join_lifecycle(place_kind_t k, uint32_t fetch_cap,
                              work_t *w_out, out_t *o_out, const char *tag)
{
    int failures = 0;
    const uint64_t now = 2000000ull;
    caps_t c; caps_default(&c);
    c.version   = MOQ_VERSION_DRAFT_18;
    c.fetch_cap = fetch_cap;

    /*
     * A REAL established draft-18 pair. A peer request bidi and a
     * PENDING_PUBLISHER responder owner cannot exist in a session that never
     * completed SETUP, so the containing lifecycle is production-reachable
     * even though the owner shapes are placed white-box.
     */
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    moq_session_t *cl = NULL, *s = NULL;
    failures += establish_pair_d18(&c, &alloc, &cl, &s);
    MOQ_TEST_CHECK(cl != NULL && s != NULL);
    if (!cl || !s) return failures;
    /* establish_pair_d18() already classified every setup record and proved
     * both queues empty; only the fixture's own preconditions remain. */
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(s), (int)MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)s->fetch_cap, (uint64_t)fetch_cap);
    MOQ_TEST_CHECK(s->drain_ref_cap >= 3u);

    size_t u0 = place_slot(k, c.sub_cap);
    size_t u1 = (k == PLACE_HIGH) ? u0 - 1u : u0 + 1u;
    size_t u2 = (k == PLACE_HIGH) ? u0 - 2u : u0 + 2u;
    size_t j[3];
    j[0] = place_slot(k, c.fetch_cap);
    j[1] = (k == PLACE_HIGH) ? j[0] - 1u : j[0] + 1u;
    j[2] = (k == PLACE_HIGH) ? j[0] - 2u : j[0] + 2u;
    /*
     * A FOREIGN buffered join -- bound to U1's request id, not U0's. The
     * teardown must leave it completely alone, and its presence is what gives
     * the defensive discard scan inside sub_free_entry() something to walk:
     * with every matching join already rejected, that scan has nothing left.
     */
    size_t jf = (k == PLACE_HIGH) ? j[0] - 3u : j[0] + 3u;
    if (!slot_ok(u0, c.sub_cap, "sub", &failures) ||
        !slot_ok(u1, c.sub_cap, "sub", &failures) ||
        !slot_ok(u2, c.sub_cap, "sub", &failures) ||
        !slot_ok(j[0], c.fetch_cap, "fetch", &failures) ||
        !slot_ok(j[1], c.fetch_cap, "fetch", &failures) ||
        !slot_ok(j[2], c.fetch_cap, "fetch", &failures) ||
        !slot_ok(jf, c.fetch_cap, "fetch", &failures)) {
        moq_session_destroy(cl); moq_session_destroy(s); return failures;
    }

    /*
     * DECLARED identities, all fixed BEFORE the arm. Nothing below is read back
     * out of an armed owner: each slot is asserted untouched first, which pins
     * the next live generation at 1 (the pool allocators' live == odd rule), and
     * the packed handle then follows from (pool, tag, generation, slot).
     */
    static const uint64_t jreq[3] = { REQ_JOIN0, REQ_JOIN1, REQ_JOIN2 };
    static const uint64_t jref[3] = { REF_JOIN0, REF_JOIN1, REF_JOIN2 };
    static const uint64_t jstart[3] = { JOIN_START0, JOIN_START1, JOIN_START2 };
    static const uint32_t jsym[3] = { SYM_J0, SYM_J1, SYM_J2 };
    const uint32_t live_gen = 1u;
    uint64_t jh[3];
    for (size_t i = 0; i < 3; i++) {
        MOQ_TEST_CHECK_EQ_INT((int)s->fetches[j[i]].state, (int)MOQ_FETCH_FREE);
        MOQ_TEST_CHECK_EQ_U64((uint64_t)s->fetches[j[i]].generation, 0u);
        jh[i] = moq_handle_pack(MOQ_HANDLE_POOL_FETCH, s->session_tag,
                                live_gen, (uint32_t)j[i]);
    }
    MOQ_TEST_CHECK_EQ_INT((int)s->fetches[jf].state, (int)MOQ_FETCH_FREE);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)s->fetches[jf].generation, 0u);
    const uint64_t jfh = moq_handle_pack(MOQ_HANDLE_POOL_FETCH, s->session_tag,
                                         live_gen, (uint32_t)jf);
    MOQ_TEST_CHECK_EQ_INT((int)s->subs[u0].state, (int)MOQ_SUB_FREE);
    MOQ_TEST_CHECK_EQ_INT((int)s->subs[u1].state, (int)MOQ_SUB_FREE);
    MOQ_TEST_CHECK_EQ_INT((int)s->subs[u2].state, (int)MOQ_SUB_FREE);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)s->subs[u0].generation, 0u);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)s->subs[u1].generation, 0u);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)s->subs[u2].generation, 0u);
    const uint64_t h0 = moq_handle_pack(MOQ_HANDLE_POOL_SUBSCRIPTION,
                                        s->session_tag, live_gen, (uint32_t)u0);
    const uint64_t h1 = moq_handle_pack(MOQ_HANDLE_POOL_SUBSCRIPTION,
                                        s->session_tag, live_gen, (uint32_t)u1);
    const uint64_t h2 = moq_handle_pack(MOQ_HANDLE_POOL_SUBSCRIPTION,
                                        s->session_tag, live_gen, (uint32_t)u2);

    arm_pending_publisher_sub(s, u0, REQ_SUB0, REF_SUB0);
    arm_forwarding_pending_sub_d18(s, u1, REQ_SUB1, REF_SUB1);
    arm_forwarding_pending_sub_d18(s, u2, REQ_SUB2, REF_SUB2);
    for (size_t i = 0; i < 3; i++)
        arm_pending_join(s, j[i], REQ_SUB0, jreq[i], jref[i], jstart[i]);
    arm_pending_join(s, jf, REQ_SUB1, REQ_JOIN3, REF_JOIN3, JOIN_START3);

    symtab_t sym; sym_reset(&sym);
    sym_bind(&sym, h0, SYM_SUB0);
    for (size_t i = 0; i < 3; i++) sym_bind(&sym, jref[i], jsym[i]);
    sym_bind(&sym, REF_JOIN3, SYM_J3);

    /* ---- the complete LIVE inventory and graph, before the call ---- */
    {
        state_t got, wst; st_reset(&got); st_reset(&wst);
        st_sub(&got, "U0", s, u0);
        st_sub(&got, "U1", s, u1);
        st_sub(&got, "U2", s, u2);
        for (size_t i = 0; i < 3; i++) {
            char pfx[8]; snprintf(pfx, sizeof(pfx), "J%zu", i);
            st_fetch(&got, pfx, s, j[i]);
        }
        st_expect_sub_pending_publisher(&wst, "U0", live_gen, h0, REQ_SUB0,
                                        REF_SUB0);
        st_expect_sub_fwd_pending_d18(&wst, "U1", live_gen, h1, REQ_SUB1,
                                      REF_SUB1);
        st_expect_sub_fwd_pending_d18(&wst, "U2", live_gen, h2, REQ_SUB2,
                                      REF_SUB2);
        for (size_t i = 0; i < 3; i++) {
            char pfx[8]; snprintf(pfx, sizeof(pfx), "J%zu", i);
            st_expect_fetch_join(&wst, pfx, live_gen, jh[i], j[i], jreq[i],
                                 jref[i], REQ_SUB0, jstart[i]);
        }
        st_fetch(&got, "JF", s, jf);
        st_expect_fetch_join(&wst, "JF", live_gen, jfh, jf, REQ_JOIN3,
                             REF_JOIN3, REQ_SUB1, JOIN_START3);
        failures += st_check(&wst, &got, tag);
    }
    failures += check_live_edge(s, MOQ_REQ_SUBSCRIPTION, u0,
                                OG_DOM_REQ_STREAMREF, REF_SUB0, tag);
    failures += check_live_edge(s, MOQ_REQ_SUBSCRIPTION, u1,
                                OG_DOM_REQ_STREAMREF, REF_SUB1, tag);
    failures += check_live_edge(s, MOQ_REQ_SUBSCRIPTION, u2,
                                OG_DOM_REQ_STREAMREF, REF_SUB2, tag);
    for (size_t i = 0; i < 3; i++)
        failures += check_live_edge(s, MOQ_REQ_FETCH, j[i],
                                    OG_DOM_REQ_STREAMREF, jref[i], tag);
    failures += check_live_edge(s, MOQ_REQ_FETCH, jf,
                                OG_DOM_REQ_STREAMREF, REF_JOIN3, tag);
    /* No drain obligation exists yet: the teardown's three are its own. */
    MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, (size_t)0);

    /*
     * Conservation baselines, taken only NOW -- after the absolute declared
     * inventory above has already fixed U1/U2's semantic shape, so the byte
     * image cannot launder a wrongly armed owner.
     */
    sub_snap_t snap_u1, snap_u2;
    failures += sub_snap_capture(s, u1, &snap_u1, "U1");
    failures += sub_snap_capture(s, u2, &snap_u2, "U2");

    /* ---- the measured production call ---- */
    work_t before, after;
    work_read(&before);
    failures += occ_audit_named(s, "join_lifecycle.pre_teardown");
    moq_result_t rc = request_stream_teardown(
        s, moq_stream_ref_from_u64(REF_SUB0));
    work_read(&after);
    failures += occ_audit_named(s, "join_lifecycle.post_teardown");
    work_delta(&before, &after, w_out);
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(s), (int)MOQ_SESS_ESTABLISHED);

    /* ---- immediate post-call state, observed BEFORE anything is drained ---- */
    {
        state_t got, wst; st_reset(&got); st_reset(&wst);
        st_sub(&got, "U0", s, u0);
        st_sub(&got, "U1", s, u1);   /* the retained owners are inventoried, */
        st_sub(&got, "U2", s, u2);   /* not merely created */
        for (size_t i = 0; i < 3; i++) {
            char pfx[8]; snprintf(pfx, sizeof(pfx), "J%zu", i);
            st_fetch(&got, pfx, s, j[i]);
        }
        st_expect_sub_free(&wst, "U0", live_gen);
        st_expect_sub_fwd_pending_d18(&wst, "U1", live_gen, h1, REQ_SUB1,
                                      REF_SUB1);
        st_expect_sub_fwd_pending_d18(&wst, "U2", live_gen, h2, REQ_SUB2,
                                      REF_SUB2);
        for (size_t i = 0; i < 3; i++) {
            char pfx[8]; snprintf(pfx, sizeof(pfx), "J%zu", i);
            st_expect_fetch_free(&wst, pfx, live_gen);
        }
        failures += st_check(&wst, &got, tag);
    }
    /* Retirement for U0 AND every J, by stream ref as well as by id. */
    failures += check_retired_streamref(s, MOQ_REQ_SUBSCRIPTION, u0, h0,
                                        REQ_SUB0, REF_SUB0, tag);
    for (size_t i = 0; i < 3; i++)
        failures += check_retired_streamref(s, MOQ_REQ_FETCH, j[i], jh[i],
                                            jreq[i], jref[i], tag);
    /* The retained owners keep their edges: conservation, not just survival. */
    failures += check_live_edge(s, MOQ_REQ_SUBSCRIPTION, u1,
                                OG_DOM_REQ_STREAMREF, REF_SUB1, tag);
    failures += check_live_edge(s, MOQ_REQ_SUBSCRIPTION, u2,
                                OG_DOM_REQ_STREAMREF, REF_SUB2, tag);

    /*
     * ---- the DRAIN obligations the rejections installed ----
     * Each rejected join had no peer FIN, so its bidi owes exactly one
     * MOQ_DRAIN_NORMAL ref so the fetcher's later terminal is absorbed rather
     * than read as a stray. The full phase postcondition is asserted here and
     * again after every late terminal.
     */
    join_fix_t fx;
    fx.s = s;
    fx.u0 = u0; fx.u1 = u1; fx.u2 = u2;
    fx.h0 = h0; fx.h1 = h1; fx.h2 = h2;
    fx.live_gen = live_gen;
    for (size_t i = 0; i < 3; i++) {
        fx.j[i] = j[i]; fx.jh[i] = jh[i];
        fx.jreq[i] = jreq[i]; fx.jref[i] = jref[i];
    }
    fx.snap_u1 = snap_u1;
    fx.snap_u2 = snap_u2;
    fx.jf  = jf;
    fx.jfh = jfh;
    failures += join_postcondition(&fx, jref, 3, tag);

    /* ---- the declared output of the measured call ---- */
    out_reset(o_out);
    {
        out_t want, got; out_reset(&want); out_reset(&got);
        out_capture_events(s, &sym, &got);
        exp_event(&want, MOQ_EVENT_UNSUBSCRIBED, SYM_SUB0, 0, 0, NULL);
        failures += out_check(&want, &got, tag);
        for (size_t i = 0; i < got.n; i++) o_out->r[o_out->n++] = got.r[i];
    }
    {
        out_t want, got; out_reset(&want); out_reset(&got);
        out_capture_actions(s, &sym, &got);
        for (size_t i = 0; i < 3; i++)
            exp_bidi_request_error(&want, jsym[i], jref[i],
                                   MOQ_REQUEST_ERROR_INVALID_JOINING_REQUEST_ID,
                                   0, NULL, true);
        out_sort_by_sym(&want);
        out_sort_by_sym(&got);
        failures += out_check(&want, &got, tag);
        for (size_t i = 0; i < got.n; i++) o_out->r[o_out->n++] = got.r[i];
    }

    /*
     * ---- each drain ref releases on its own late peer terminal ----
     * Delivered through the production request-bidi ingress. After EACH
     * delivery the COMPLETE phase postcondition is re-asserted, so a transient
     * owner corruption between two valid production calls is caught at the
     * call that caused it rather than being repaired by the next one.
     */
    for (size_t i = 0; i < 3; i++) {
        char lt[40];
        snprintf(lt, sizeof(lt), "%s.late%zu", tag, i);
        size_t before_n = s->drain_ref_count;
        moq_result_t drc = moq_session_on_bidi_stream_bytes(
            s, moq_stream_ref_from_u64(jref[i]), NULL, 0, true, now);
        MOQ_TEST_CHECK_EQ_INT((int)drc, (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, before_n - 1u);
        {
            /* Exactly the refs not yet consumed, in declaration order. */
            uint64_t rest[3];
            size_t n_rest = 0;
            for (size_t q = i + 1; q < 3; q++) rest[n_rest++] = jref[q];
            failures += join_postcondition(&fx, rest, n_rest, lt);
        }
        out_t want, got; out_reset(&want); out_reset(&got);
        out_capture_events(s, &sym, &got);
        out_capture_actions(s, &sym, &got);
        failures += out_check(&want, &got, lt);
    }
    /* Idempotent summary; it does NOT stand in for the per-call checks. */
    MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, (size_t)0);
    failures += join_postcondition(&fx, NULL, 0, tag);

    model_t wm; model_reset(&wm);
    /*
     * No advancing preamble runs on this route, so there is no catch-up pass
     * and no deferred-owner selection. The declared operation set:
     *   finds   -- the teardown's own by-streamref lookup (1); the
     *              migration-marker lookup queue_send_bidi() performs before
     *              each reject is written (3); and the by-id lookup inside
     *              each of the four frees, 3 joins plus U0 (4).
     *   removes -- each join free removes its by-id key (absent here: a
     *              draft-18 join is keyed by stream ref alone) and its
     *              stream-ref key (present); U0's free does the same, and its
     *              alias key is not attempted at all (PENDING_PUBLISHER is not
     *              the ESTABLISHED subscriber state that clears an alias).
     *              4 owners x 2 attempts = 8, of which 4 are present.
     */
    wm.auth_staging = 6;      /* two staged token values per join */
    wm.idx_find_ops = 8; wm.idx_remove_ops = 8; wm.idx_present_rm = 4;
    failures += model_check(&wm, w_out, tag);

    moq_session_destroy(cl);
    moq_session_destroy(s);
    return failures;
}

static int run_placement_matrix(void)
{
    int failures = 0;
    work_t w[PLACE_COUNT];
    out_t  o[PLACE_COUNT];
    printf("CASE placement_matrix         fixed caps: subs=64 pubs=16 sg=32 rx=32\n");
    for (int k = 0; k < PLACE_COUNT; k++) {
        failures += run_placement((place_kind_t)k, &w[k], &o[k]);
        char tag[40];
        snprintf(tag, sizeof(tag), "%s", place_name((place_kind_t)k));
        work_print(tag, &w[k]);
        out_print("out:", &o[k]);
        producer_note(&w[k]);
    }
    for (int k = 1; k < PLACE_COUNT; k++) {
        /* Outcome equivalence is an OUTPUT assertion and holds unconditionally. */
        failures += out_check(&o[0], &o[k], place_name((place_kind_t)k));
#ifndef EXACT_WORK_NO_COMPLEXITY
        char dbuf[256];
        const char *f = work_capacity_diff(&w[0], &w[k], dbuf, sizeof(dbuf));
        if (f) {
            printf("  PLACEMENT FAILURE: fields %s differ between %s and %s "
                   "for the same symbolic model\n", f, place_name(PLACE_LOW),
                   place_name((place_kind_t)k));
            work_print("low", &w[0]);
            work_print("scaled", &w[k]);
            MOQ_TEST_CHECK(!"work must not depend on an owner's physical slot");
        }
#endif
    }

    /* Second model: the blocked expired owner, same three placements. */
    printf("CASE placement_blocked        fixed caps: pubs=16 rx=32 actions=2\n");
    for (int k = 0; k < PLACE_COUNT; k++) {
        failures += run_placement_blocked((place_kind_t)k, &w[k], &o[k]);
        char tag[40];
        snprintf(tag, sizeof(tag), "%s", place_name((place_kind_t)k));
        work_print(tag, &w[k]);
        producer_note(&w[k]);
    }
    for (int k = 1; k < PLACE_COUNT; k++) {
        failures += out_check(&o[0], &o[k], place_name((place_kind_t)k));
#ifndef EXACT_WORK_NO_COMPLEXITY
        char dbuf[256];
        const char *f = work_capacity_diff(&w[0], &w[k], dbuf, sizeof(dbuf));
        if (f) {
            printf("  PLACEMENT FAILURE (blocked): fields %s differ between %s "
                   "and %s\n", f, place_name(PLACE_LOW),
                   place_name((place_kind_t)k));
            printf("    the retry-deadline recompute stops at the FIRST blocked "
                   "owner, so its probe count follows that owner's physical "
                   "slot index rather than the live set\n");
            work_print("low", &w[0]);
            work_print("scaled", &w[k]);
            MOQ_TEST_CHECK(!"blocked lookup must not follow the slot");
        }
#endif
    }
    /*
     * Third model: the buffered-join cleanup LIFECYCLE. Two independent
     * comparisons over the same runner -- physical placement at a fixed
     * fetch capacity, then fetch capacity at a fixed (low) placement. The
     * second is what carries the fetch-capacity axis now that the join family
     * has left the preamble matrix.
     */
    printf("CASE join_lifecycle           placement, fixed caps: subs=64 "
           "fetch=16 (draft-18)\n");
    for (int k = 0; k < PLACE_COUNT; k++) {
        char tag[40];
        snprintf(tag, sizeof(tag), "place.%s", place_name((place_kind_t)k));
        failures += run_join_lifecycle((place_kind_t)k, 16u, &w[k], &o[k], tag);
        work_print(tag, &w[k]);
        producer_note(&w[k]);
    }
    for (int k = 1; k < PLACE_COUNT; k++) {
        failures += out_check(&o[0], &o[k], place_name((place_kind_t)k));
#ifndef EXACT_WORK_NO_COMPLEXITY
        char dbuf[256];
        const char *f = work_capacity_diff(&w[0], &w[k], dbuf, sizeof(dbuf));
        if (f) {
            printf("  PLACEMENT FAILURE (join lifecycle): fields %s differ "
                   "between %s and %s\n", f, place_name(PLACE_LOW),
                   place_name((place_kind_t)k));
            work_print("low", &w[0]);
            work_print("scaled", &w[k]);
            MOQ_TEST_CHECK(!"cleanup scans must not follow the slot");
        }
#endif
    }

    {
        static const uint32_t fcaps[3] = { 8u, 16u, 256u };
        work_t fw[3]; out_t fo[3];
        printf("CASE join_lifecycle           fetch-capacity axis, fixed low "
               "placement (draft-18)\n");
        for (int i = 0; i < 3; i++) {
            char tag[40];
            snprintf(tag, sizeof(tag), "fetch=%u", (unsigned)fcaps[i]);
            failures += run_join_lifecycle(PLACE_LOW, fcaps[i], &fw[i], &fo[i],
                                           tag);
            work_print(tag, &fw[i]);
            producer_note(&fw[i]);
        }
        for (int i = 1; i < 3; i++) {
            char tag[40];
            snprintf(tag, sizeof(tag), "fetch=%u", (unsigned)fcaps[i]);
            failures += out_check(&fo[0], &fo[i], tag);
#ifndef EXACT_WORK_NO_COMPLEXITY
            char dbuf[256];
            const char *f = work_capacity_diff(&fw[0], &fw[i], dbuf,
                                               sizeof(dbuf));
            if (f) {
                printf("  CAPACITY FAILURE (join lifecycle): fields %s differ "
                       "between fetch=%u and fetch=%u\n", f,
                       (unsigned)fcaps[0], (unsigned)fcaps[i]);
                work_print("fetch-low", &fw[0]);
                work_print("fetch-high", &fw[i]);
                MOQ_TEST_CHECK(!"join cleanup must not follow fetch_cap");
            }
#endif
        }
    }
    return failures;
}

/* ================= late cancelled-FETCH tombstone ================= */

/*
 * The REAL late-cancel path, driven through the real producer.
 *
 * The only fixture state installed by hand is the ONE fact production cannot
 * be made to establish here without a second peer: the pre-existing cancel
 * tombstone a local fetch cancel leaves behind. Everything else -- the rx
 * entry, its NEED_STOP state, and the carrier fields
 * (stop_consumes_cancel_tomb / cancel_tomb_request_id) -- is created by the
 * production branch at session_receive.c:1018-1030, reached by feeding a
 * wire-valid encoded FETCH_HEADER for that request id on a fresh data uni.
 * A refused STOP leaves the entry in MOQ_RX_NEED_STOP, and a later empty
 * ingress on the SAME stream retries through the NEED_STOP branch (:765-766).
 *
 * The header is encoded through the SESSION'S OWN profile vtable, so the case
 * is wire-correct on either draft and cannot drift from the decoder it feeds.
 *
 * The entry never acquires a publication or subscription binding, so this case
 * declares none.
 *
 * This is PRODUCER/LIFECYCLE work, not advancing-preamble work, so it sits
 * OUTSIDE the capacity matrix -- the boundary is stated rather than implied.
 */
#define TOMB_REQ_ID 0x333ull
#define TOMB_REF    0x7C00ull

static int run_tomb_late_cancel(void)
{
    int failures = 0;
    const uint64_t now = 1000000ull;
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    moq_session_cfg_t extra = MOQ_SESSION_CFG_INIT;
    extra.max_actions = 2;
    moq_session_t *cl = NULL, *s = NULL;
    establish_pair(&alloc, 10, 10, &cl, &s, &extra, &extra);
    MOQ_TEST_CHECK(cl != NULL && s != NULL);
    if (!cl || !s) return failures;
    /* ESTABLISHED but OWNER-EMPTY on both sides, before the tombstone arm. */
    failures += occ_audit_named(cl, "tomb_late_cancel.client_empty");
    failures += occ_audit_named(s, "tomb_late_cancel.server_empty");
    caps_t c; caps_default(&c); c.max_actions = 2;
    {
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) == 1) moq_action_cleanup(&a);
        moq_event_t e;
        while (moq_session_poll_events(s, &e, 1) == 1) moq_event_cleanup(&e);
    }

    /* The ONE hand-installed fact: the tombstone a local cancel left behind.
     * The request id must NOT resolve to a fetch -- that is precisely the
     * branch condition the producer keys on. */
    fetch_cancel_tomb_add(s, TOMB_REQ_ID);
    MOQ_TEST_CHECK(fetch_cancel_tomb_contains(s, TOMB_REQ_ID));
    MOQ_TEST_CHECK_EQ_U64((uint64_t)s->fetch_cancel_tomb_count, 1u);
    MOQ_TEST_CHECK(fetch_find_by_request_id(s, TOMB_REQ_ID) < 0);
    /* No rx entry exists for the ref yet: the producer must create it. */
    MOQ_TEST_CHECK(moq_index_find(s->idx_rx_by_ref, s->idx_rx_mask,
                                  TOMB_REF) < 0);
    fill_action_queue(s);

    /* The wire-valid FETCH_HEADER, built by the session's own encoder. */
    uint8_t hdr[32];
    size_t hdr_len = 0;
    {
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, hdr, sizeof(hdr));
        MOQ_TEST_CHECK_EQ_INT(
            (int)s->profile->encode_fetch_header(s, &w, TOMB_REQ_ID),
            (int)MOQ_OK);
        hdr_len = moq_buf_writer_offset(&w);
        MOQ_TEST_CHECK(hdr_len > 0);
    }

    symtab_t sym; sym_reset(&sym);
    printf("CASE tomb_late_cancel         (producer/lifecycle, outside the "
           "capacity matrix)\n");

    /* --- PHASE 1: the real producer runs; its STOP is refused --- */
    work_t b1, a1, w1;
    work_read(&b1);
    failures += occ_audit_named(s, "tomb_late_cancel.pre_call");
    moq_result_t rc1 = moq_session_on_data_bytes(
        s, moq_stream_ref_from_u64(TOMB_REF), hdr, hdr_len, false, now);
    work_read(&a1);
    failures += occ_audit_named(s, "tomb_late_cancel.post_blocked");
    work_delta(&b1, &a1, &w1);
    work_print("blocked", &w1);
    MOQ_TEST_CHECK(rc1 == MOQ_ERR_WOULD_BLOCK);

    /* The rx slot is the PRODUCER'S choice, resolved through the production
     * lookup -- the fixture never picks it. */
    int tslot = moq_index_find(s->idx_rx_by_ref, s->idx_rx_mask, TOMB_REF);
    MOQ_TEST_CHECK(tslot >= 0);
    if (tslot < 0) { moq_session_destroy(cl); moq_session_destroy(s);
                     return failures; }
    {
        state_t got, wst; st_reset(&got); st_reset(&wst);
        st_rx(&got, "R0", s, (size_t)tslot);
        st_add(&got, "carrier_on",  s->rx_streams[tslot].stop_consumes_cancel_tomb ? 1u : 0u);
        st_add(&got, "carrier_id",  s->rx_streams[tslot].cancel_tomb_request_id);
        st_add(&got, "tomb_count",  (uint64_t)s->fetch_cancel_tomb_count);
        st_add(&got, "sess.state",  (uint64_t)moq_session_state(s));
        /* UNBOUND, in NEED_STOP, with the carrier the production branch owes. */
        st_expect_rx_bound_pub(&wst, "R0", TOMB_REF, 0,
                               (uint64_t)MOQ_RX_NEED_STOP);
        st_add(&wst, "carrier_on",  1);
        st_add(&wst, "carrier_id",  TOMB_REQ_ID);
        st_add(&wst, "tomb_count",  1);      /* retained, NOT consumed */
        st_add(&wst, "sess.state",  (uint64_t)MOQ_SESS_ESTABLISHED);
        failures += st_check(&wst, &got, "tomb.blocked");
        model_t wm; model_reset(&wm);
        wm.catchup = 1;
        /* Two lookups, both ABSENT: the rx stream by ref (a new stream), then
         * the request registry by id (the fetch is gone -- that is what sends
         * the branch to the tombstone). */
        wm.idx_find_ops = 2;
        failures += model_check(&wm, &w1, "tomb.blocked");
    }
    MOQ_TEST_CHECK(fetch_cancel_tomb_contains(s, TOMB_REQ_ID));

    /* --- PHASE 2: drain, then the SAME production ingress retries --- */
    {
        out_t drained; out_reset(&drained);
        out_capture_actions(s, &sym, &drained);
        MOQ_TEST_CHECK_EQ_SIZE(drained.n, (size_t)c.max_actions);
    }
    work_t b2, a2, w2;
    work_read(&b2);
    failures += occ_audit_named(s, "tomb_late_cancel.pre_retry");
    moq_result_t rc2 = moq_session_on_data_bytes(
        s, moq_stream_ref_from_u64(TOMB_REF), NULL, 0, false, now);
    work_read(&a2);
    failures += occ_audit_named(s, "tomb_late_cancel.post_retry");
    work_delta(&b2, &a2, &w2);
    work_print("retry", &w2);
    MOQ_TEST_CHECK(rc2 == MOQ_OK);
    {
        state_t got, wst; st_reset(&got); st_reset(&wst);
        st_rx(&got, "R0", s, (size_t)tslot);
        st_add(&got, "tomb_count", (uint64_t)s->fetch_cancel_tomb_count);
        st_add(&got, "idx_key",
               moq_index_find(s->idx_rx_by_ref, s->idx_rx_mask,
                              TOMB_REF) >= 0 ? 1u : 0u);
        st_add(&got, "sess.state", (uint64_t)moq_session_state(s));
        st_expect_rx_free(&wst, "R0");
        st_add(&wst, "tomb_count", 0);      /* consumed exactly once */
        st_add(&wst, "idx_key",    0);      /* index key retired with it */
        st_add(&wst, "sess.state", (uint64_t)MOQ_SESS_ESTABLISHED);
        failures += st_check(&wst, &got, "tomb.retry");
        model_t wm; model_reset(&wm);
        wm.catchup = 1; wm.tomb = 1;
        wm.idx_find_ops = 1;    /* the ingress resolves the stream by ref */
        wm.idx_remove_ops = 1; wm.idx_present_rm = 1;
        failures += model_check(&wm, &w2, "tomb.retry");
        out_t want, got_out; out_reset(&want); out_reset(&got_out);
        exp_action(&want, MOQ_ACTION_STOP_DATA, TOMB_REF, 0);
        out_capture_actions(s, &sym, &got_out);
        failures += out_check(&want, &got_out, "tomb.retry");
    }
    /*
     * The one-time consume is proven from the TERMINAL postcondition, not by
     * feeding the retired ref again: the tombstone is gone, the rx slot is
     * free and unkeyed, no owner was ever bound, exactly one STOP_DATA was
     * emitted (compared above) and nothing else is queued behind it.
     */
    MOQ_TEST_CHECK(!fetch_cancel_tomb_contains(s, TOMB_REQ_ID));
    MOQ_TEST_CHECK_EQ_U64((uint64_t)s->fetch_cancel_tomb_count, 0u);
    MOQ_TEST_CHECK(moq_index_find(s->idx_rx_by_ref, s->idx_rx_mask,
                                  TOMB_REF) < 0);
    MOQ_TEST_CHECK(!s->rx_streams[tslot].active);
    {
        moq_action_t a;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_actions(s, &a, 1), (size_t)0);
        moq_event_t e;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(s, &e, 1), (size_t)0);
    }

    producer_note(&w1); producer_note(&w2);
    moq_session_destroy(cl);
    moq_session_destroy(s);
    return failures;
}

/* ================= colliding index chain ========================== */

/*
 * A REAL colliding chain, at a fixed capacity so the live set is exact.
 * The colliding key is found by measuring the PRODUCTION lookup's own probe
 * count -- the private hash is never duplicated in the test. Retiring the first
 * owner executes the backshift; the survivor must remain reachable through the
 * production lookup, at its own owner, with exact topology.
 */
static int run_index_collision(void)
{
    int failures = 0;
    const uint64_t now = 1000000ull;
    caps_t c; caps_default(&c);
    moq_session_t *s = make_session(&c);
    MOQ_TEST_CHECK(s != NULL);
    if (!s) return failures;
    /* Owner-empty starting point: heads and sentinels before any arm. */
    failures += occ_audit_named(s, "index_chain_collision.empty");

    arm_ready_pub(s, 0, REQ_PUB0);

    uint64_t collide_id = 0;
    for (uint64_t cand = REQ_PUB1; cand < REQ_PUB1 + (1u << 18); cand += 2ull) {
        uint64_t before = session_work_index_find;
        (void)request_registry_find_by_id(s, cand);
        if (session_work_index_find - before == 2u) { collide_id = cand; break; }
    }
    printf("CASE index_chain_collision    key %llu collides with %llu "
           "(idx_req_mask=%zu)\n", (unsigned long long)collide_id,
           (unsigned long long)REQ_PUB0, s->idx_req_mask);
    MOQ_TEST_CHECK(collide_id != 0);
    if (!collide_id) { moq_session_destroy(s); return failures; }

    arm_live_pub(s, 1, collide_id);
    uint32_t p0_gen = s->publishes[0].generation;
    uint32_t p1_gen = s->publishes[1].generation;
    uint64_t p0_h   = pub_handle_at(s, 0);
    uint64_t p1_h   = pub_handle_at(s, 1);

    /* Declared topology BEFORE the retirement: two edges, one per owner. */
    {
        og_graph_t g; og_capture(s, &g);
        failures += og_check_integrity(&g, "collide.before");
        failures += og_check_edge(&g, OG_DOM_REQ_RID, REQ_PUB0,
                                  MOQ_REQ_PUBLISH, 0, "collide.before");
        failures += og_check_edge(&g, OG_DOM_REQ_RID, collide_id,
                                  MOQ_REQ_PUBLISH, 1, "collide.before");
    }
    MOQ_TEST_CHECK(request_registry_find_by_id(s, collide_id).slot == 1);

    symtab_t sym; sym_reset(&sym);
    sym_bind(&sym, p0_h, SYM_PUB0);

    work_t before, after, w;
    work_read(&before);
    failures += occ_audit_named(s, "index_chain_collision.pre_call");
    session_begin_advance(s, now);
    work_read(&after);
    failures += occ_audit_named(s, "index_chain_collision.post_call");
    work_delta(&before, &after, &w);
    work_print("collision", &w);

    state_t got, wst; st_reset(&got); st_reset(&wst);
    st_pub(&got, "P0", s, 0);
    st_pub(&got, "P1", s, 1);
    st_expect_pub_free(&wst, "P0", p0_gen);
    st_expect_pub_live(&wst, "P1", p1_gen, p1_h, collide_id);
    failures += st_check(&wst, &got, "collision");

    out_t want, gotout; out_reset(&want); out_reset(&gotout);
    exp_event(&want, MOQ_EVENT_PUBLISH_FINISHED, SYM_PUB0, PUB_STATUS, PUB_SC,
              PUB_REASON);
    out_capture_events(s, &sym, &gotout);
    out_capture_actions(s, &sym, &gotout);
    failures += out_check(&want, &gotout, "collision");

    model_t wm; model_reset(&wm);
    wm.catchup = 1; wm.rx_rescan = 1; wm.pub_pending = 1;
    wm.idx_find_ops = 1; wm.idx_remove_ops = 1; wm.idx_present_rm = 1;
    failures += model_check(&wm, &w, "collision");

    /* The backshift RAN, and the survivor is still reachable. */
    MOQ_TEST_CHECK(w.index_rm_backshift >= 1u);
    MOQ_TEST_CHECK(request_registry_find_by_id(s, collide_id).slot == 1);
    failures += check_retired(s, MOQ_REQ_PUBLISH, 0, p0_h, REQ_PUB0,
                              "collide.retired");
    {
        og_graph_t g; og_capture(s, &g);
        failures += og_check_integrity(&g, "collide.after");
        failures += og_check_edge(&g, OG_DOM_REQ_RID, collide_id,
                                  MOQ_REQ_PUBLISH, 1, "collide.after");
        failures += og_check_no_edge(&g, OG_DOM_REQ_RID, REQ_PUB0,
                                     "collide.after");
    }
    producer_note(&w);
    moq_session_destroy(s);
    return failures;
}

/* ================= bounded-normalizer guard ====================== */

/*
 * The record's byte payload is bounded. An over-cap value must make the record
 * INCOMPARABLE, never compare equal on a matching prefix.
 */
static int guard_selfcheck(void)
{
    int failures = 0;
    uint8_t big[REC_REASON + 16];
    memset(big, 'x', sizeof(big));
    uint8_t big2[REC_REASON + 16];
    memset(big2, 'x', sizeof(big2));
    big2[REC_REASON + 4] = 'y';        /* differs only PAST the cap */

    rec_t a, b;
    memset(&a, 0, sizeof(a)); memset(&b, 0, sizeof(b));
    rec_set_reason(&a, big,  sizeof(big));
    rec_set_reason(&b, big2, sizeof(big2));
    MOQ_TEST_CHECK(a.oversize && b.oversize);
    {
        const char *d = rec_diff(&a, &b);
        MOQ_TEST_CHECK(d != NULL && strcmp(d, "oversize_incomparable") == 0);
    }
    /* Two records with IDENTICAL over-cap bytes are still incomparable. */
    {
        rec_t c; memset(&c, 0, sizeof(c));
        rec_set_reason(&c, big, sizeof(big));
        const char *d = rec_diff(&a, &c);
        MOQ_TEST_CHECK(d != NULL && strcmp(d, "oversize_incomparable") == 0);
    }
    /* At the cap exactly, records stay comparable. */
    {
        rec_t d1, d2; memset(&d1, 0, sizeof(d1)); memset(&d2, 0, sizeof(d2));
        rec_set_reason(&d1, big, REC_REASON);
        rec_set_reason(&d2, big, REC_REASON);
        MOQ_TEST_CHECK(!d1.oversize && !d2.oversize);
        MOQ_TEST_CHECK(rec_diff(&d1, &d2) == NULL);
    }
    printf("GUARD SELF-CHECK: bounded byte payloads fail closed\n");
    return failures;
}

/* ================= seeded-schedule self-checks ==================== */

/*
 * The seed must reach the OPERATION ORDER, not just slot numbers. Two declared
 * seeds must generate different sequences, and replaying one seed must
 * reproduce its sequence and its placement exactly.
 */
static void build_probe_sched(sched_t *sc, uint32_t seed, const place_t *pl)
{
    sched_reset(sc, seed);
    sched_add(sc, OP_ARM_PUB, pl->p0, 0);
    sched_add(sc, OP_ARM_PUB, pl->p1, 0);
    sched_add(sc, OP_ARM_SUB, pl->u0, 0);
    sched_add(sc, OP_ARM_SUB, pl->u1, 0);
    sched_add(sc, OP_ARM_SUB, pl->u2, 0);
    sched_finalize(sc);
}

static int check_seed_schedules(const uint32_t *seeds, size_t nseeds)
{
    int failures = 0;
    caps_t c; caps_default(&c);
    sched_t a, b, a2;
    place_t pa, pb, pa2;
    int dummy = 0;

    MOQ_TEST_CHECK(nseeds >= 2);
    if (nseeds < 2) return failures;

    place_build(&pa,  seeds[0], &c, &dummy);
    place_build(&pb,  seeds[1], &c, &dummy);
    place_build(&pa2, seeds[0], &c, &dummy);
    build_probe_sched(&a,  seeds[0], &pa);
    build_probe_sched(&b,  seeds[1], &pb);
    build_probe_sched(&a2, seeds[0], &pa2);

    printf("SEEDED SCHEDULES\n  %s\n  %s\n", a.text, b.text);

    /*
     * The seed must reach the ORDER. Comparing kind AND slot would pass on a
     * fixed order with seeded slots -- the exact weakness this check exists to
     * exclude -- so only the KIND SEQUENCE is compared.
     *
     * Two assertions, because a seed PAIR could in principle collide:
     *   (a) the declared seed's kind order differs from the DECLARATION order
     *       (arm_pub, arm_pub, arm_sub, arm_sub, arm_sub), which is a direct
     *       "the shuffle ran" fact independent of any other seed;
     *   (b) two declared seeds reach different kind orders.
     */
    static const op_kind_t DECLARED[5] = {
        OP_ARM_PUB, OP_ARM_PUB, OP_ARM_SUB, OP_ARM_SUB, OP_ARM_SUB };
    int moved_from_declaration = 0;
    MOQ_TEST_CHECK_EQ_SIZE(a.n, 5u);
    for (size_t i = 0; i < a.n && i < 5; i++)
        if (a.op[i].kind != DECLARED[i]) moved_from_declaration = 1;
    MOQ_TEST_CHECK(moved_from_declaration);

    int order_differs = 0;
    for (size_t i = 0; i < a.n && i < b.n; i++)
        if (a.op[i].kind != b.op[i].kind) order_differs = 1;
    MOQ_TEST_CHECK(order_differs);

    /* Replay reproduces the sequence and the placement byte for byte. */
    MOQ_TEST_CHECK(strcmp(a.text, a2.text) == 0);
    MOQ_TEST_CHECK(a.n == a2.n);
    for (size_t i = 0; i < a.n; i++) {
        MOQ_TEST_CHECK(a.op[i].kind == a2.op[i].kind);
        MOQ_TEST_CHECK(a.op[i].a == a2.op[i].a);
    }
    MOQ_TEST_CHECK(pa.p0 == pa2.p0 && pa.u0 == pa2.u0 &&
                   pa.g0 == pa2.g0 && pa.r0 == pa2.r0);
    return failures;
}

/* ================= main =========================================== */

int main(void)
{
    int failures = 0;
    static const uint32_t SEEDS[3] = { 0x5eedu, 0xa11ceu, 0x0u };
    const size_t nseeds = sizeof(SEEDS) / sizeof(SEEDS[0]);

    printf("== exact-work matrix: %d white-box cases + close-pending + "
           "placement, %zu seeds, 18 capacity rows on 4 orthogonal axes ==\n",
           C_COUNT, nseeds);

    failures += comparator_selfcheck();
    failures += model_selfcheck();
    failures += guard_selfcheck();
    failures += check_seed_schedules(SEEDS, nseeds);

    for (size_t k = 0; k < nseeds; k++) {
        for (int ci = 0; ci < C_COUNT; ci++) {
            g_case_id = ci;
            failures += drive(case_name(ci), run_case_entry, SEEDS[k]);
        }
        failures += drive("close_with_pending", run_close_pending, SEEDS[k]);
        failures += drive("subgroup_close_reset", run_subgroup_lifecycle,
                          SEEDS[k]);
    }

    failures += run_placement_matrix();
    failures += run_index_collision();
    failures += run_tomb_late_cancel();
    failures += producer_check();

    /*
     * The staged scan and the tombstone scan are both REACHABLE and both have
     * active cases. Their counters are required NON-ZERO by producer coverage;
     * these totals report the work those cases performed.
     */
    printf("TOPOLOGY AUDITS: %llu executed (every pool, at every checkpoint)\n",
           (unsigned long long)g_occ_audits);
    printf("ACTIVE-SCAN TOTALS staged=%llu tomb=%llu (both must be > 0; the "
           "tomb case is a standalone producer/lifecycle case)\n",
           (unsigned long long)g_producer_seen.staged,
           (unsigned long long)g_producer_seen.tomb);
    MOQ_TEST_CHECK(g_producer_seen.staged > 0u);
    MOQ_TEST_CHECK(g_producer_seen.tomb > 0u);

    if (failures) {
        printf("FAILURES: %d\n", failures);
        return 1;
    }
    MOQ_TEST_PASS("session_exact_work");
    return 0;
}
