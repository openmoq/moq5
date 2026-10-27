/*
 * Media-sender pacing attribution.
 *
 * Drives the PRODUCTION sender_hook (one hook call == one sender_drain pass)
 * against a real SimPair peer that subscribes on the real wire, and expresses
 * pacing purely in pump/drive units -- no wall clock, no sleeps, no localhost
 * timing, and no second implementation of the drain.
 *
 * Every object carries a unique payload sentinel, every expected identity is
 * declared BEFORE the observation that checks it, and the peer-side trace is a
 * bounds-checked deep copy of the borrowed payload taken at poll time.
 *
 * What the trace can establish: that the service sender hands an accepted
 * object to the session in the SAME drive that follows its write, in exact FIFO
 * identity, and that a retained head resumes on a later drive without loss,
 * duplication, or reordering. What it cannot establish: backend wake latency
 * (the hook is driven directly here) or peer-side playback behaviour.
 */
#include <moq/media_sender.h>
#include <moq/rcbuf.h>
#include <moq/sim.h>
#include <moq/session.h>
#include <moq/publisher.h>   /* MOQ_PUB_DONE_TRACK_ENDED */
#include <moq/msf.h>         /* MOQ_MSF_CATALOG_TRACK_NAME */
#include "test_support.h"
#include "test_alloc_support.h"
/* Expected handles/aliases require layout, not the private action helpers. */
#include "../../core/src/session/session_internal.h"

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#ifdef MOQ_TEST_HAVE_MPEG2TS
#include "ts_stream.h"
#endif

static int failures = 0;

/* Forward decl: the per-scenario verdict also enforces the final
 * unconsumed-event invariant (see report(&fix, ) below). */
typedef struct fix fix_t;
static int fix_unconsumed(fix_t *fix, const char *name);
static void expect_controls(fix_t *fix, const char *label);

/* Per-scenario verdict. MOQ_TEST_PASS only prints while the GLOBAL counter is
 * zero, which would silence every control that runs after a failing
 * row -- so each scenario reports its own outcome from its own delta.
 *
 * It ALSO closes the single final invariant, which covers EVERY peer
 * observation class the collector records -- subgroup RESET, subgroup FIN,
 * status objects, SUBSCRIBE_DONE, SUBSCRIBE_OK and PUBLISH_REQUEST: each one
 * must have been claimed by a declared inventory. SUBSCRIBE_OK is compared
 * against the inventory subscribe_track() declared as it created each
 * subscription, so a row cannot pass by never comparing its controls. A scenario that declares none of a class therefore
 * fails on any observation of it, so nothing can be silently retained. */
static void report(fix_t *fix, const char *name, int before)
{
    failures += fix_unconsumed(fix, name);
    int n = failures - before;
    if (n == 0) printf("PASS: %s\n", name);
    else        printf("FAIL: %s (%d diagnostics)\n", name, n);
}

/* Test seams (media_sender.c, MOQ_MEDIA_SENDER_TESTING). */
moq_media_sender_t *moq_media_sender_test_new_cfg(
    const moq_media_sender_cfg_t *cfg);
void moq_media_sender_test_pump(moq_media_sender_t *s,
                                moq_session_t *session, uint64_t now_us);
void moq_media_sender_test_free(moq_media_sender_t *s);
uint32_t moq_media_sender_test_reset_attempts(const moq_media_sender_t *s);
bool moq_media_sender_test_queue_entry(moq_media_sender_t *s, size_t i,
                                       const moq_media_track_t **track,
                                       const moq_rcbuf_t **payload,
                                       uint64_t *group, bool *starts_group);
bool moq_media_sender_test_emit_head(moq_media_sender_t *s,
                                     moq_rcbuf_t **payload);
void moq_media_sender_test_set_cond_wait_hook(
    int (*fn)(const struct timespec *, void *), void *ctx);
void moq_media_sender_test_drop_track_locked(moq_media_sender_t *s,
                                             moq_media_track_t *t);
bool moq_media_sender_test_has_pending_output(moq_media_sender_t *s);
bool moq_media_sender_test_complete_failed(moq_media_sender_t *s);
bool moq_media_sender_test_timeline_held(moq_media_sender_t *s);
void moq_media_sender_test_set_alloc(const moq_alloc_t *alloc);
void moq_media_sender_test_refuse_catalog_build(moq_media_sender_t *s);

/* -- Trace ------------------------------------------------------------- *
 * One record per object the PEER actually received, in arrival order, tagged
 * with the drive pass that delivered it. */

enum { TR_MAX = 128, TERM_MAX = 32, RESET_MAX = 8, SOK_MAX = 8, STAT_MAX = 8,
       TR_COPY = 16, NTRACK = 2, NPUBX = 2, PUBREQ_MAX = 4,
       NS_MAX = 4,
       /* Every fixture object is exactly this many identical sentinel bytes,
        * so the whole payload is comparable, not just its first byte. */
       PAYLOAD_LEN = 8,
       SENTINEL_MAX = 256 };

/* Which peer events are legal in the current phase, MEDIA OBJECTS INCLUDED.
 *   ARM     - namespace/catalog/subscribe handshake. Objects are FORBIDDEN, so
 *             a stray or early object can never be adopted into a later
 *             scenario's measured baseline.
 *   RELEASE - the one arm that legitimately delivers media: scenario 4's
 *             hold-release subscription, whose SUBSCRIBE_OK arrives while the
 *             previously held FIFO drains. Admits the subscribe control plus
 *             objects and their subgroup terminals, and nothing else.
 *   PACE    - a measured drain: objects and their subgroup terminals only. */
typedef enum { PHASE_ARM = 0, PHASE_RELEASE, PHASE_PACE } phase_t;

/* One observed subgroup terminal, with the identity the public contract
 * exposes plus the object-trace position at which it arrived -- that position
 * is what proves the terminal followed the last object of its subgroup. */
typedef struct {
    int      track;
    uint64_t group;
    uint64_t subgroup;
    bool     end_of_group;
    int      objs_before;   /* object-trace length when this terminal arrived */
    int      seq;           /* GLOBAL collector ordinal across event classes */
    bool     matched;       /* consumed by exactly one declared expectation */
} term_rec_t;

/* Declared subgroup terminal. */
typedef struct {
    int      track;
    uint64_t group;
    uint64_t subgroup;
    bool     end_of_group;
    /* Declared: does this subgroup carry media? A terminal-only subgroup --
     * one opened solely to carry an END_OF_TRACK status object -- legitimately
     * closes with no media object before it, so the non-vacuity check is
     * declared per terminal rather than assumed. */
    bool     carries_media;
} term_exp_t;

/* One observed subgroup RESET. A RESET is a DIFFERENT event class from a FIN:
 * for one subgroup the peer reports exactly one of them, so they are recorded
 * and declared separately and never merged. */
typedef struct {
    int      track;
    uint64_t sub_opaque;   /* WHICH peer handle observed it: two slots of one
                            * track produce two records that must never
                            * authorize each other. Exactly one of the two is
                            * non-zero on any record. */
    uint64_t pub_opaque;
    uint64_t group;
    uint64_t subgroup;
    uint64_t error_code;
    bool     end_of_group;
    int      objs_before;
    int      seq;
    bool     matched;
} reset_rec_t;

typedef struct {
    int      track;
    uint64_t group;
    uint64_t subgroup;
    uint64_t error_code;
    bool     end_of_group;
    /* Declared subscription handle. 0 means "any subscription of this track",
     * which is only legal where the track has exactly ONE subscription -- a
     * fan-out row must declare the handle. */
    uint64_t sub_opaque;
    uint64_t pub_opaque;
} reset_exp_t;

/* One observed STATUS object (a non-NORMAL object, e.g. END_OF_TRACK). These
 * are NOT media, so they are inventoried separately from the payload trace and
 * declared per scenario. */
typedef struct {
    int      track;
    uint64_t group;
    uint64_t object;
    int      status;
    bool     payload_present;   /* must be false for a status object */
    int      seq;
    bool     matched;
} stat_rec_t;

typedef struct {
    int      track;
    uint64_t group;
    uint64_t object;
    int      status;
} stat_exp_t;

/* One observed SUBSCRIBE_OK, recorded in full at poll time. `track_properties`
 * is BORROWED from output scratch, so a bounded prefix is deep-copied and an
 * over-long block is a sticky failure rather than a silent truncation. */
typedef struct {
    uint64_t sub_opaque;
    uint64_t track_alias;
    bool     has_largest;
    uint64_t largest_group;
    uint64_t largest_object;
    bool     has_expires;
    uint64_t expires_ms;
    size_t   props_len;
    bool     props_data_null;
    uint8_t  props[TR_COPY];
    bool     dynamic_groups;
    bool     matched;
} sok_rec_t;

/* Declared SUBSCRIBE_OK. A renewal can carry the previously published largest
 * location; other body fields remain fixture-wide empty/false invariants. */
typedef struct {
    uint64_t sub_opaque;    /* the EXACT handle moq_session_subscribe returned */
    uint64_t track_alias;   /* declared, never adopted from the event */
    bool     has_largest;
    uint64_t largest_group;
    uint64_t largest_object;
} sok_exp_t;


/* One observed PUBLISH_REQUEST, deep-copied: every borrowed byte field is
 * copied under its own bounds/NULL guard, so the record stays valid after
 * moq_event_cleanup and a truncated or absent field can never read past the
 * event.
 *
 * Deep-copied: the track name and the namespace parts. NOT copied: token
 * values and the track-properties block -- both are declared EMPTY, and a
 * non-zero count or length fails the comparator on the scalar alone, so
 * neither pointer is ever dereferenced. */
typedef struct {
    uint64_t pub_opaque;
    size_t   ns_count;
    size_t   ns_len[NS_MAX];
    uint8_t  ns[NS_MAX][TR_COPY];
    size_t   name_len;
    uint8_t  name[TR_COPY];
    uint64_t track_alias;
    bool     forward;
    /* Only the SEMANTIC emptiness of these two is recorded. The public
     * contract fixes neither pointer's representation for an empty span, so
     * asserting NULL-ness would pin an unpromised detail; and because both are
     * declared EMPTY, neither pointer is ever dereferenced. */
    size_t   token_count;
    size_t   props_len;
    bool     dynamic_groups;
    bool     has_largest;
    uint64_t largest_group;
    uint64_t largest_object;
    bool     has_expires;
    uint64_t expires_ms;
    bool     truncated;   /* a copied field exceeded the record's bound */
    bool     matched;
} pubreq_rec_t;

/* Declared PUBLISH_REQUEST: the request image re-derived from the fixture and
 * the product source, never adopted from the event. */
typedef struct {
    const char *name;         /* exact track name */
    bool        accepted;     /* did this fixture answer it? */
    uint64_t    pub_opaque;   /* exact handle, DERIVED pre-ingress */
    uint64_t    track_alias;  /* exact alias, DERIVED pre-ingress */
    bool        has_largest;  /* catalog only: PUBLISH follows the retained group */
} pubreq_exp_t;
typedef struct {
    int      pass;
    int      track;      /* index into fix->sub[]; -1 = not a media track */
    /* WHICH peer slot delivered it. Exactly one is non-zero (enforced by
     * peer_track_of); a fan-out track delivers the SAME object on each of its
     * slots, and those records must never be interchangeable. */
    uint64_t sub_opaque;
    uint64_t pub_opaque;
    uint64_t group;
    uint64_t object;
    int      enq_ord;    /* app enqueue ordinal, resolved via the sentinel */
    size_t   len;
    uint8_t  copy[TR_COPY];
    bool     matched;    /* consumed by a declared multiset comparison */
} tr_rec_t;

typedef struct {
    tr_rec_t recs[TR_MAX];
    int      n;
    /* Sticky flags. Any one of them makes the trace INCOMPARABLE: it is never
     * silently truncated, tolerated, or normalized away. */
    bool     overflow;     /* more records than the trace can hold */
    bool     long_payload; /* a payload exceeded TR_COPY */
    bool     foreign_obj;  /* object on a subscription the fixture never made */
    bool     unexpected;   /* a peer event kind not legal in this phase */
    bool     obj_in_arm;   /* a media object in a phase that forbids objects */
    bool     term_overflow;/* more subgroup terminals than TERM_MAX */
    bool     reset_overflow;/* more subgroup resets than RESET_MAX */
    bool     handle_ambiguous;/* an observation carried both peer handles, or
                               * neither: the XOR the public contract states was
                               * violated, so nothing can be attributed */
    bool     pubreq_overflow; /* more PUBLISH_REQUESTs than PUBREQ_MAX */
    bool     sok_overflow; /* more SUBSCRIBE_OKs than SOK_MAX */
    bool     stat_overflow;/* more status objects than STAT_MAX */
    bool     stat_payload; /* a status object carried a payload */
    bool     long_props;   /* a SUBSCRIBE_OK track-properties block over TR_COPY */
    int      bad_kind;     /* the offending kind, for the diagnostic */
} trace_t;

/* Declared expectation, written down before the observation. */
typedef struct {
    int      track;
    uint64_t group;
    uint64_t object;
    uint8_t  sentinel;
    int      enq_ord;    /* the app enqueue ordinal this object was written at */
} tr_exp_t;

/* Declared object observation for a FAN-OUT track, where the same object is
 * delivered once per peer slot and the two records are only distinguishable by
 * the handle that carried them. Exactly one handle is declared non-zero. */
typedef struct {
    int      track;
    uint64_t sub_opaque;
    uint64_t pub_opaque;
    uint64_t group;
    uint64_t object;
    uint8_t  sentinel;
    int      enq_ord;
} tr_slot_exp_t;

typedef struct fix {
    moq_simpair_t      *sp;
    moq_media_sender_t *s;
    moq_media_track_t  *t[NTRACK];
    moq_subscription_t  sub[NTRACK];
    bool                subscribed[NTRACK];
    /* Publish-mode fan-out: the peer ACCEPTS the sender's PUBLISH for a named
     * track, which installs a SECOND slot on that publisher track beside the
     * peer's own subscription. Only the named track is accepted, so no other
     * publication can start delivering into this fixture's trace. */
    /* Publish-mode capability. A PULL fixture never advertises tracks with
     * PUBLISH, so a PUBLISH_REQUEST there is an UNEXPECTED event, not a
     * silently declined one. */
    bool                publish_mode;
    const char         *accept_pub_name;
    int                 accept_pub_track;
    moq_publication_t   pub_x[NPUBX];
    int                 pub_x_track[NPUBX];
    bool                pub_x_used[NPUBX];
    int                 pub_x_n;
    int                 pub_req_n;      /* PUBLISH_REQUESTs seen */
    int                 pub_req_declined;
    pubreq_rec_t        pubreq[PUBREQ_MAX];
    int                 pubreq_consumed;
    /* Derived from PRE-INGRESS session state, before the sender publishes
     * anything: the exact publication handle the RECEIVER will mint and the
     * exact track alias the PUBLISHER will consume, in the source-declared
     * request order (catalog, then the media tracks in add order). */
    uint64_t            exp_pub_handle[PUBREQ_MAX];
    uint64_t            exp_pub_alias[PUBREQ_MAX];
    int                 exp_pub_n;
    /* Declared SUBSCRIBE_OK inventory, appended by subscribe_track() BEFORE
     * the request is driven, so no row can forget to declare its controls. */
    sok_exp_t           sok_exp[SOK_MAX];
    int                 sok_exp_n;
    int                 sok_consumed;
    trace_t             tr;
    int                 pass;
    /* harness self-check state */
    int                 ready_n;
    int                 joined_n;
    int                 collect_calls;
    bool                accept_ns;
    phase_t             phase;
    int                 enq_n;                    /* next enqueue ordinal */
    term_rec_t          term[TERM_MAX];          /* subgroup terminals, in order */
    int                 term_n;
    stat_rec_t          stat[STAT_MAX];          /* status objects, in order */
    int                 stat_n;
    reset_rec_t         reset[RESET_MAX];        /* subgroup resets, in order */
    int                 reset_n;
    int                 reset_consumed;          /* claimed by a declared set */
    int                 term_consumed;
    int                 stat_consumed;
    /* One GLOBAL ordinal across reset/FIN/status/done, so relative ORDER is
     * compared directly instead of being inferred from separate arrays. */
    int                 seq_next;
    sok_rec_t           sok[SOK_MAX];             /* SUBSCRIBE_OKs, in order */
    int                 sub_ok_n;                 /* exact SUBSCRIBE_OK count */
    uint64_t            done_sub[NTRACK];         /* SUBSCRIBE_DONE handles seen */
    uint64_t            done_status[NTRACK];
    int                 done_seq[NTRACK];
    bool                done_matched[NTRACK];
    int                 done_n;
    int                 done_consumed;            /* claimed by a declared set */
    bool                done_overflow;
    int                 ns_pub_n;                 /* exact NAMESPACE_PUBLISHED count */
    bool                ns_pub_bad;               /* namespace identity/token mismatch */
    int                 enq_ord[SENTINEL_MAX];    /* sentinel -> ordinal, -1 = unused */
} fix_t;

static int fix_unconsumed(fix_t *fix, const char *name)
{
    int bad = 0;
    /* The control contract is closed HERE, for every row, so it cannot be
     * omitted: comparing it at a call site is optional, satisfying it is not. */
    {
        int before_ctl = failures;
        expect_controls(fix, name);
        bad += failures - before_ctl;
        failures = before_ctl;   /* counted once, through `bad` */
    }
    if (fix->sub_ok_n != fix->sok_consumed || fix->tr.sok_overflow) {
        fprintf(stderr, "FAIL %s: %d SUBSCRIBE_OK observed, %d consumed "
                "(overflow=%d)\n", name, fix->sub_ok_n, fix->sok_consumed,
                (int)fix->tr.sok_overflow);
        bad++;
    }
    if (fix->done_n != fix->done_consumed || fix->done_overflow) {
        fprintf(stderr, "FAIL %s: %d SUBSCRIBE_DONE observed, %d declared "
                "(overflow=%d)\n", name, fix->done_n, fix->done_consumed,
                (int)fix->done_overflow);
        bad++;
    }
    if (fix->reset_n != fix->reset_consumed || fix->tr.reset_overflow) {
        fprintf(stderr, "FAIL %s: %d subgroup RESET observed, %d declared "
                "(overflow=%d)\n", name, fix->reset_n, fix->reset_consumed,
                (int)fix->tr.reset_overflow);
        bad++;
    }
    if (fix->term_n != fix->term_consumed || fix->tr.term_overflow) {
        fprintf(stderr, "FAIL %s: %d subgroup FIN observed, %d declared "
                "(overflow=%d)\n", name, fix->term_n, fix->term_consumed,
                (int)fix->tr.term_overflow);
        bad++;
    }
    if (fix->pub_req_n != fix->pubreq_consumed || fix->tr.pubreq_overflow) {
        fprintf(stderr, "FAIL %s: %d PUBLISH_REQUEST observed, %d declared "
                "(overflow=%d)\n", name, fix->pub_req_n, fix->pubreq_consumed,
                (int)fix->tr.pubreq_overflow);
        bad++;
    }
    if (fix->stat_n != fix->stat_consumed || fix->tr.stat_overflow) {
        fprintf(stderr, "FAIL %s: %d status object observed, %d declared "
                "(overflow=%d)\n", name, fix->stat_n, fix->stat_consumed,
                (int)fix->tr.stat_overflow);
        bad++;
    }
    return bad;
}

static void on_ready_cb(void *ctx, moq_media_sender_t *s)
{
    (void)s; ((fix_t *)ctx)->ready_n++;
}

static void on_joined_cb(void *ctx, moq_media_sender_t *s,
                         moq_media_track_t *t, size_t active)
{
    (void)s; (void)t; (void)active; ((fix_t *)ctx)->joined_n++;
}

/* The advertised namespace, set once by the fixture and compared against the
 * peer's NAMESPACE_PUBLISHED identity. */
static moq_bytes_t g_ns[2];

/* Are MEDIA OBJECTS (and their subgroup terminals) legal in this phase? */
static bool objects_legal(phase_t ph)
{
    return ph == PHASE_PACE || ph == PHASE_RELEASE;
}

/* Which non-object peer event kinds are legal, by phase. Declared, closed
 * sets: anything else is a named failure, never a silent cleanup. */
static bool kind_legal(const fix_t *fix, phase_t ph, moq_event_kind_t k)
{
    switch (ph) {
    case PHASE_ARM:
        /* PUBLISH_REQUEST is admitted ONLY by a publish-capable fixture: a
         * pull fixture advertises nothing with PUBLISH, so one arriving there
         * is an unexpected event and must fail by name. */
        return k == MOQ_EVENT_NAMESPACE_PUBLISHED ||
               (fix->publish_mode && k == MOQ_EVENT_PUBLISH_REQUEST) ||
               k == MOQ_EVENT_SUBSCRIBE_OK;
    case PHASE_RELEASE:
        /* The hold-release subscribe: its own SUBSCRIBE_OK, and nothing else
         * on the control side. */
        return k == MOQ_EVENT_SUBSCRIBE_OK;
    case PHASE_PACE:
    default:
        return false;   /* a measured drain owes no control event */
    }
}

/* The SINGLE peer-event sink. Every peer poll in this file goes through here,
 * so no phase can silently discard an event and leave the trace short: the
 * COMPLETE peer event queue is classified against the phase's declared set,
 * OBJECTS INCLUDED. Payload spans are borrowed until cleanup, so they are
 * deep-copied, bounds-checked, at poll time. */

/* Resolve an observation's SUBSCRIPTION handle to the fixture track it belongs
 * to. Returns -1 for a handle this fixture never opened. A fan-out track's
 * second slot is an accepted PUBLICATION, not another subscription (a second
 * subscriber-role subscription to one track is refused by
 * moq_session_subscribe), so publications resolve through pub_track_of. */
static int sub_track_of(const fix_t *fix, uint64_t opaque)
{
    if (opaque == 0) return -1;
    for (int i = 0; i < NTRACK; i++)
        if (fix->subscribed[i] && opaque == fix->sub[i]._opaque) return i;
    return -1;
}

/* Same, for an observation delivered on a publication the peer accepted.
 * Exactly one of the two handles is valid on every such event. */
static int pub_track_of(const fix_t *fix, uint64_t opaque)
{
    if (opaque == 0) return -1;
    for (int i = 0; i < fix->pub_x_n && i < NPUBX; i++)
        if (fix->pub_x_used[i] && opaque == fix->pub_x[i]._opaque)
            return fix->pub_x_track[i];
    return -1;
}

/* Resolve an observation to a fixture track, ENFORCING the public contract
 * that exactly one of the two peer handles is valid (session.h:983). Both
 * zero, both non-zero, or a lone handle this fixture never opened are all
 * named, sticky failures -- never a silent fall-through to the other field. */
static int peer_track_of(fix_t *fix, uint64_t sub_op, uint64_t pub_op)
{
    if ((sub_op != 0) == (pub_op != 0)) {
        fix->tr.handle_ambiguous = true;
        return -1;
    }
    return sub_op ? sub_track_of(fix, sub_op) : pub_track_of(fix, pub_op);
}
static void collect(fix_t *fix, uint64_t now)
{
    moq_session_t *srv = moq_simpair_server(fix->sp);
    moq_event_t ev;
    fix->collect_calls++;
    while (moq_session_poll_events(srv, &ev, 1) == 1) {
        if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) {
            const moq_object_received_event_t *o = &ev.u.object_received;
            int idx = peer_track_of(fix, o->sub._opaque, o->pub._opaque);
            if (!objects_legal(fix->phase)) {
                /* An object in a phase that forbids them. Named, sticky: it is
                 * never quietly folded into a later measured baseline. */
                fix->tr.obj_in_arm = true;
            } else if (o->status != MOQ_OBJECT_NORMAL) {
                /* A STATUS object is not media: inventoried separately so it
                 * can never be compared as a zero-length payload record. */
                if (idx < 0) {
                    fix->tr.foreign_obj = true;
                } else if (fix->stat_n >= STAT_MAX) {
                    fix->tr.stat_overflow = true;
                } else {
                    stat_rec_t *sr = &fix->stat[fix->stat_n++];
                    memset(sr, 0, sizeof(*sr));
                    sr->track = idx;
                    sr->group = o->group_id;
                    sr->object = o->object_id;
                    sr->status = (int)o->status;
                    sr->payload_present = (o->payload != NULL);
                    sr->seq = fix->seq_next++;
                    if (sr->payload_present) fix->tr.stat_payload = true;
                }
            } else if (idx < 0) {
                /* An object on a subscription this fixture never created. */
                fix->tr.foreign_obj = true;
            } else if (fix->tr.n >= TR_MAX) {
                fix->tr.overflow = true;
            } else {
                tr_rec_t *r = &fix->tr.recs[fix->tr.n++];
                memset(r, 0, sizeof(*r));
                r->pass = fix->pass;
                r->track = idx;
                r->sub_opaque = o->sub._opaque;
                r->pub_opaque = o->pub._opaque;
                r->group = o->group_id;
                r->object = o->object_id;
                r->enq_ord = -1;
                r->len = o->payload ? moq_rcbuf_len(o->payload) : 0;
                if (r->len > TR_COPY) {
                    fix->tr.long_payload = true;
                } else if (r->len && moq_rcbuf_data(o->payload)) {
                    memcpy(r->copy, moq_rcbuf_data(o->payload), r->len);
                    /* Resolve the app enqueue ordinal from the declared
                     * sentinel->ordinal mapping recorded at write() time. */
                    r->enq_ord = fix->enq_ord[r->copy[0]];
                }
            }
        } else if (ev.kind == MOQ_EVENT_SUBGROUP_FINISHED) {
            const moq_subgroup_finished_event_t *f = &ev.u.subgroup_finished;
            int idx = peer_track_of(fix, f->sub._opaque, f->pub._opaque);
            if (!objects_legal(fix->phase)) {
                fix->tr.obj_in_arm = true;
            } else if (idx < 0) {
                fix->tr.foreign_obj = true;
            } else if (fix->term_n >= TERM_MAX) {
                fix->tr.term_overflow = true;
            } else {
                term_rec_t *t = &fix->term[fix->term_n++];
                memset(t, 0, sizeof(*t));
                t->track = idx;
                t->group = f->group_id;
                t->subgroup = f->subgroup_id;
                t->end_of_group = f->end_of_group;
                t->objs_before = fix->tr.n;
                t->seq = fix->seq_next++;
            }
        } else if (ev.kind == MOQ_EVENT_SUBGROUP_RESET) {
            const moq_subgroup_reset_event_t *f = &ev.u.subgroup_reset;
            int idx = peer_track_of(fix, f->sub._opaque, f->pub._opaque);
            if (!objects_legal(fix->phase)) {
                fix->tr.obj_in_arm = true;
            } else if (idx < 0) {
                fix->tr.foreign_obj = true;
            } else if (fix->reset_n >= RESET_MAX) {
                fix->tr.reset_overflow = true;
            } else {
                reset_rec_t *r = &fix->reset[fix->reset_n++];
                memset(r, 0, sizeof(*r));
                r->track = idx;
                r->group = f->group_id;
                r->subgroup = f->subgroup_id;
                r->error_code = f->error_code;
                r->end_of_group = f->end_of_group;
                r->sub_opaque = f->sub._opaque;
                r->pub_opaque = f->pub._opaque;
                r->objs_before = fix->tr.n;
                r->seq = fix->seq_next++;
            }
        } else if (ev.kind == MOQ_EVENT_SUBSCRIBE_DONE) {
            /* Removing/completing a track ends the peer's subscription. Not a
             * media event: inventoried exactly, never waved through. */
            const moq_subscribe_done_event_t *d = &ev.u.subscribe_done;
            if (fix->done_n >= NTRACK) fix->done_overflow = true;
            else {
                fix->done_sub[fix->done_n] = d->sub._opaque;
                fix->done_status[fix->done_n] = d->status_code;
                fix->done_seq[fix->done_n] = fix->seq_next++;
                fix->done_n++;
            }
        } else if (ev.kind == MOQ_EVENT_SUBSCRIBE_OK) {
            const moq_subscribe_ok_event_t *k = &ev.u.subscribe_ok;
            fix->sub_ok_n++;
            if (!kind_legal(fix, fix->phase, ev.kind)) {
                fix->tr.unexpected = true;
                fix->tr.bad_kind = (int)ev.kind;
            }
            if (fix->sub_ok_n > SOK_MAX) {
                fix->tr.sok_overflow = true;
            } else {
                sok_rec_t *r = &fix->sok[fix->sub_ok_n - 1];
                memset(r, 0, sizeof(*r));
                r->sub_opaque = k->sub._opaque;
                r->track_alias = k->track_alias;
                r->has_largest = k->has_largest;
                r->largest_group = k->largest_group;
                r->largest_object = k->largest_object;
                r->has_expires = k->has_expires;
                r->expires_ms = k->expires_ms;
                r->props_len = k->track_properties.len;
                r->props_data_null = (k->track_properties.data == NULL);
                r->dynamic_groups = k->dynamic_groups;
                if (r->props_len > TR_COPY)
                    fix->tr.long_props = true;
                else if (r->props_len && k->track_properties.data)
                    memcpy(r->props, k->track_properties.data, r->props_len);
            }
        } else if (ev.kind == MOQ_EVENT_PUBLISH_REQUEST &&
                   kind_legal(fix, fix->phase, ev.kind)) {
            const moq_publish_request_event_t *pr = &ev.u.publish_request;
            fix->pub_req_n++;
            if (fix->pub_req_n > PUBREQ_MAX) {
                fix->tr.pubreq_overflow = true;
            } else {
                /* Deep copy: the event's byte fields are borrowed from output
                 * scratch and die at moq_event_cleanup. */
                pubreq_rec_t *q = &fix->pubreq[fix->pub_req_n - 1];
                memset(q, 0, sizeof(*q));
                q->pub_opaque = pr->pub._opaque;
                q->track_alias = pr->track_alias;
                q->forward = pr->forward;
                q->token_count = pr->token_count;
                q->props_len = pr->track_properties.len;
                q->dynamic_groups = pr->dynamic_groups;
                q->has_largest = pr->has_largest;
                q->largest_group = pr->largest_group;
                q->largest_object = pr->largest_object;
                q->has_expires = pr->has_expires;
                q->expires_ms = pr->expires_ms;
                q->name_len = pr->track_name.len;
                if (q->name_len > TR_COPY) q->truncated = true;
                else if (q->name_len && pr->track_name.data)
                    memcpy(q->name, pr->track_name.data, q->name_len);
                else if (q->name_len) q->truncated = true;  /* len without data */
                q->ns_count = pr->track_namespace.count;
                if (q->ns_count > NS_MAX || pr->track_namespace.parts == NULL) {
                    q->truncated = true;
                    q->ns_count = 0;
                } else {
                    for (size_t pi = 0; pi < q->ns_count; pi++) {
                        const moq_bytes_t *b = &pr->track_namespace.parts[pi];
                        q->ns_len[pi] = b->len;
                        if (b->len > TR_COPY || (b->len && !b->data))
                            q->truncated = true;
                        else if (b->len)
                            memcpy(q->ns[pi], b->data, b->len);
                    }
                }
            }
            int want = -1;
            if (fix->accept_pub_name) {
                size_t wl = strlen(fix->accept_pub_name);
                if (pr->track_name.len == wl && pr->track_name.data &&
                    memcmp(pr->track_name.data, fix->accept_pub_name, wl) == 0)
                    want = fix->accept_pub_track;
            }
            if (want >= 0 && fix->pub_x_n < NPUBX) {
                moq_accept_publish_cfg_t ac;
                moq_accept_publish_cfg_init(&ac);
                if (moq_session_accept_publish(srv, pr->pub, &ac, now) == MOQ_OK) {
                    int slot = fix->pub_x_n++;
                    fix->pub_x[slot] = pr->pub;
                    fix->pub_x_track[slot] = want;
                    fix->pub_x_used[slot] = true;
                } else {
                    fix->tr.unexpected = true;
                }
            } else {
                fix->pub_req_declined++;   /* left unanswered, delivers nothing */
            }
        } else if (ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED &&
                   kind_legal(fix, fix->phase, ev.kind)) {
            const moq_namespace_published_event_t *n = &ev.u.namespace_published;
            fix->ns_pub_n++;
            /* Declared identity: exactly the two parts the fixture advertised. */
            /* Declared empty auth-token inventory, in the public empty
             * representation the fixture advertised. */
            if (n->token_count != 0 || n->tokens != NULL)
                fix->ns_pub_bad = true;
            if (n->track_namespace.count != 2 || n->track_namespace.parts == NULL) {
                fix->ns_pub_bad = true;
            } else {
                for (int i = 0; i < 2; i++) {
                    const moq_bytes_t *got = &n->track_namespace.parts[i];
                    if (got->len != g_ns[i].len || got->data == NULL ||
                        memcmp(got->data, g_ns[i].data, got->len) != 0)
                        fix->ns_pub_bad = true;
                }
            }
            if (fix->accept_ns) {
                moq_accept_namespace_cfg_t ac;
                moq_accept_namespace_cfg_init(&ac);
                (void)moq_session_accept_namespace(srv, n->ann, &ac, now);
            }
        } else {
            fix->tr.unexpected = true;
            fix->tr.bad_kind = (int)ev.kind;
        }
        moq_event_cleanup(&ev);
    }
}

/* One DRIVE: exactly one production sender_hook (== one drain pass), then the
 * peer delivery it enables, run to real quiescence, then the trace collection.
 * Quiescence is asserted, so a step budget overrun can never be mistaken for
 * "nothing more to send". */
static void drive(fix_t *fix, uint64_t now)
{
    fix->pass++;
    moq_media_sender_test_pump(fix->s, moq_simpair_client(fix->sp), now);
    MOQ_TEST_CHECK(moq_simpair_run_until_quiescent(fix->sp, 64, NULL) == MOQ_OK);
    collect(fix, now);
}

/* Any sticky flag makes the WHOLE trace incomparable, named. */
static bool trace_sticky(fix_t *fix, const char *label)
{
    if (!(fix->tr.overflow || fix->tr.long_payload || fix->tr.foreign_obj ||
          fix->tr.unexpected || fix->tr.obj_in_arm || fix->tr.term_overflow ||
          fix->tr.sok_overflow || fix->tr.long_props ||
          fix->tr.stat_overflow || fix->tr.stat_payload ||
          fix->tr.reset_overflow || fix->done_overflow ||
          fix->tr.handle_ambiguous || fix->tr.pubreq_overflow))
        return false;
    fprintf(stderr, "FAIL %s: trace INCOMPARABLE (overflow=%d long=%d "
            "foreign_obj=%d unexpected=%d obj_in_forbidding_phase=%d "
            "term_overflow=%d sok_overflow=%d long_props=%d "
            "stat_overflow=%d stat_payload=%d bad_kind=%d)\n",
            label, (int)fix->tr.overflow, (int)fix->tr.long_payload,
            (int)fix->tr.foreign_obj, (int)fix->tr.unexpected,
            (int)fix->tr.obj_in_arm, (int)fix->tr.term_overflow,
            (int)fix->tr.sok_overflow, (int)fix->tr.long_props,
            (int)fix->tr.stat_overflow, (int)fix->tr.stat_payload,
            fix->tr.bad_kind);
    if (fix->tr.handle_ambiguous)
        fprintf(stderr, "FAIL %s: an observation did not carry EXACTLY ONE "
                "peer handle (sub XOR pub)\n", label);
    if (fix->tr.pubreq_overflow)
        fprintf(stderr, "FAIL %s: PUBLISH_REQUEST overflow\n", label);
    if (fix->tr.reset_overflow)
        fprintf(stderr, "FAIL %s: subgroup RESET overflow\n", label);
    if (fix->done_overflow)
        fprintf(stderr, "FAIL %s: more SUBSCRIBE_DONE events than tracks\n",
                label);
    failures++;
    return true;
}

/* Compare the observed subgroup terminals against a declared inventory.
 *
 * Multiplicity and identity are EXACT: every declared terminal must be matched
 * by exactly one observation on (track, group, subgroup id, end_of_group), and
 * no observation may be left over. Matching is by multiset, not by arrival
 * order, because the order in which two tracks' terminals reach the peer is a
 * property of the simulated delivery interleave, not of the sender's contract.
 *
 * The contract that IS asserted per terminal is the public one: a subgroup
 * terminal arrives strictly AFTER the final OBJECT_RECEIVED of that subgroup.
 * That is checked against the object trace by position -- every object of the
 * terminal's (track, group) must lie before it, and none after. */
static void expect_terminals(fix_t *fix, const term_exp_t *exp, int n,
                             const char *label)
{
    if (trace_sticky(fix, label)) return;
    if (fix->term_n != n) {
        fprintf(stderr, "FAIL %s: %d subgroup terminals, expected %d\n",
                label, fix->term_n, n);
        failures++;
        /* Deliberately NOT an early return: identity and ordering are still
         * checked, so a reordered or duplicated terminal is diagnosed by name
         * rather than hidden behind the count. */
    }
    for (int i = 0; i < fix->term_n; i++) fix->term[i].matched = false;
    for (int e = 0; e < n; e++) {
        int hit = -1;
        for (int i = 0; i < fix->term_n; i++) {
            term_rec_t *t = &fix->term[i];
            if (t->matched) continue;
            if (t->track == exp[e].track && t->group == exp[e].group &&
                t->subgroup == exp[e].subgroup &&
                t->end_of_group == exp[e].end_of_group) { hit = i; break; }
        }
        if (hit < 0) {
            fprintf(stderr, "FAIL %s: no terminal matches declared "
                    "track=%d g=%llu sg=%llu eog=%d\n", label, exp[e].track,
                    (unsigned long long)exp[e].group,
                    (unsigned long long)exp[e].subgroup,
                    (int)exp[e].end_of_group);
            failures++;
            continue;
        }
        fix->term[hit].matched = true;
        /* Ordering and carriage, counted for THIS terminal's own
         * (track, group) -- never from the global trace length, which would
         * pass on any unrelated earlier object. */
        int before = fix->term[hit].objs_before;
        int mine_before = 0, mine_after = 0;
        for (int r = 0; r < fix->tr.n; r++) {
            const tr_rec_t *o = &fix->tr.recs[r];
            if (o->track != exp[e].track || o->group != exp[e].group) continue;
            if (r < before) mine_before++;
            else            mine_after++;
        }
        if (mine_after != 0) {
            fprintf(stderr, "FAIL %s: %d object(s) of track=%d g=%llu arrived "
                    "AFTER its subgroup terminal\n", label, mine_after,
                    exp[e].track, (unsigned long long)exp[e].group);
            failures++;
        }
        if (exp[e].carries_media && mine_before == 0) {
            fprintf(stderr, "FAIL %s: carries_media terminal track=%d g=%llu "
                    "had NO media object of its own subgroup before it\n",
                    label, exp[e].track, (unsigned long long)exp[e].group);
            failures++;
        }
        if (!exp[e].carries_media && (mine_before != 0 || mine_after != 0)) {
            fprintf(stderr, "FAIL %s: terminal-only terminal track=%d g=%llu "
                    "carried %d media object(s)\n", label, exp[e].track,
                    (unsigned long long)exp[e].group, mine_before + mine_after);
            failures++;
        }
    }
    fix->term_consumed = fix->term_n;
    for (int i = 0; i < fix->term_n; i++)
        if (!fix->term[i].matched) {
            fprintf(stderr, "FAIL %s: undeclared terminal track=%d g=%llu "
                    "sg=%llu eog=%d\n", label, fix->term[i].track,
                    (unsigned long long)fix->term[i].group,
                    (unsigned long long)fix->term[i].subgroup,
                    (int)fix->term[i].end_of_group);
            failures++;
        }
}

/* Exact status-object inventory: multiplicity and identity, matched one-to-one
 * against a declared set, with no leftover observation. A status object must
 * carry no payload. */
static void expect_status(fix_t *fix, const stat_exp_t *exp, int n,
                          const char *label)
{
    if (trace_sticky(fix, label)) return;
    if (fix->stat_n != n) {
        fprintf(stderr, "FAIL %s: %d status objects, expected %d\n", label,
                fix->stat_n, n);
        failures++;
    }
    for (int i = 0; i < fix->stat_n && i < STAT_MAX; i++)
        fix->stat[i].matched = false;
    for (int e = 0; e < n; e++) {
        int hit = -1;
        for (int i = 0; i < fix->stat_n && i < STAT_MAX; i++) {
            stat_rec_t *r = &fix->stat[i];
            if (r->matched) continue;
            if (r->track == exp[e].track && r->group == exp[e].group &&
                r->object == exp[e].object && r->status == exp[e].status) {
                hit = i; break;
            }
        }
        if (hit < 0) {
            fprintf(stderr, "FAIL %s: no status object matches declared "
                    "track=%d g=%llu o=%llu status=%d\n", label, exp[e].track,
                    (unsigned long long)exp[e].group,
                    (unsigned long long)exp[e].object, exp[e].status);
            failures++;
            continue;
        }
        fix->stat[hit].matched = true;
        if (fix->stat[hit].payload_present) {
            fprintf(stderr, "FAIL %s: status object track=%d g=%llu carried a "
                    "payload\n", label, exp[e].track,
                    (unsigned long long)exp[e].group);
            failures++;
        }
    }
    fix->stat_consumed = fix->stat_n;
    for (int i = 0; i < fix->stat_n && i < STAT_MAX; i++)
        if (!fix->stat[i].matched) {
            fprintf(stderr, "FAIL %s: undeclared status object track=%d "
                    "g=%llu o=%llu status=%d\n", label, fix->stat[i].track,
                    (unsigned long long)fix->stat[i].group,
                    (unsigned long long)fix->stat[i].object,
                    fix->stat[i].status);
            failures++;
        }
}


/* Exact declared PUBLISH_REQUEST inventory. Every field is re-derived from the
 * fixture and the product source, never adopted from the event:
 *
 *   namespace   - the two parts this fixture advertised (g_ns);
 *   name        - "v" / "a" for the media tracks, and the DEFAULT catalog name
 *                 (MOQ_MSF_CATALOG_TRACK_NAME, media_sender.c:3409) since the
 *                 fixture configures none;
 *   forward     - true: moq_pub_publish_cfg_init sets has_forward/forward
 *                 (publisher.c) and the sender leaves the cfg at its defaults
 *                 (media_sender.c:1712);
 *   tokens      - none: the sender passes no auth tokens;
 *   properties  - EMPTY: the sender sets track_properties only on the ADD_TRACK
 *                 cfg, and moq_pub_publish_track forwards the PUBLISH cfg's
 *                 (empty) block (publisher.c:3592). monotonic_groups is
 *                 facade-local and never reaches the wire;
 *   dyn groups  - false: no DYNAMIC_GROUPS property is ever set;
 *   largest     - absent: nothing has been published when these requests are
 *                 issued (they ride track registration, before any object);
 *   expires     - absent: the sender configures no expiry.
 *
 * Handles and aliases are session-assigned but NOT unknowable: both are
 * derived from pre-ingress source state in fix_init_q2 and declared here as
 * ABSOLUTE values. The pairwise-distinct and non-zero checks below are kept as
 * secondary conservation checks, not as the oracle. */
static void expect_pub_requests(fix_t *fix, const pubreq_exp_t *exp, int n,
                                const char *label)
{
    if (trace_sticky(fix, label)) return;
    if (fix->pub_req_n != n) {
        fprintf(stderr, "FAIL %s: %d PUBLISH_REQUEST, expected %d\n", label,
                fix->pub_req_n, n);
        failures++;
    }
    int have = fix->pub_req_n < PUBREQ_MAX ? fix->pub_req_n : PUBREQ_MAX;
    for (int i = 0; i < have; i++) fix->pubreq[i].matched = false;
    for (int e = 0; e < n; e++) {
        size_t wl = strlen(exp[e].name);
        int hit = -1;
        for (int i = 0; i < have; i++) {
            pubreq_rec_t *q = &fix->pubreq[i];
            if (q->matched || q->truncated) continue;
            if (q->name_len != wl || memcmp(q->name, exp[e].name, wl) != 0)
                continue;
            hit = i; break;
        }
        if (hit < 0) {
            fprintf(stderr, "FAIL %s: no PUBLISH_REQUEST for declared track "
                    "'%s'\n", label, exp[e].name);
            failures++;
            continue;
        }
        pubreq_rec_t *q = &fix->pubreq[hit];
        q->matched = true;
        /* The whole declared image, field by field. */
        if (q->ns_count != 2) {
            fprintf(stderr, "FAIL %s['%s']: namespace has %zu parts, "
                    "expected 2\n", label, exp[e].name, q->ns_count);
            failures++;
        } else {
            for (int pi = 0; pi < 2; pi++)
                if (q->ns_len[pi] != g_ns[pi].len ||
                    memcmp(q->ns[pi], g_ns[pi].data, g_ns[pi].len) != 0) {
                    fprintf(stderr, "FAIL %s['%s']: namespace part %d differs "
                            "from the advertised identity\n", label,
                            exp[e].name, pi);
                    failures++;
                }
        }
        MOQ_TEST_CHECK(q->forward);
        MOQ_TEST_CHECK_EQ_SIZE(q->token_count, 0);
        MOQ_TEST_CHECK_EQ_SIZE(q->props_len, 0);
        MOQ_TEST_CHECK(!q->dynamic_groups);
        MOQ_TEST_CHECK(q->has_largest == exp[e].has_largest);
        MOQ_TEST_CHECK_EQ_U64(q->largest_group, 0);
        MOQ_TEST_CHECK_EQ_U64(q->largest_object, 0);
        MOQ_TEST_CHECK(!q->has_expires);
        MOQ_TEST_CHECK_EQ_U64(q->expires_ms, 0);
        /* ABSOLUTE identity, derived before ingress. */
        if (q->pub_opaque != exp[e].pub_opaque) {
            fprintf(stderr, "FAIL %s['%s']: publication handle %llu, expected "
                    "the derived %llu (pool=%u tag=%u gen=%u slot=%u vs "
                    "pool=%u tag=%u gen=%u slot=%u)\n", label, exp[e].name,
                    (unsigned long long)q->pub_opaque,
                    (unsigned long long)exp[e].pub_opaque,
                    moq_handle_pool_tag(q->pub_opaque),
                    moq_handle_session_tag(q->pub_opaque),
                    moq_handle_generation(q->pub_opaque),
                    moq_handle_slot(q->pub_opaque),
                    moq_handle_pool_tag(exp[e].pub_opaque),
                    moq_handle_session_tag(exp[e].pub_opaque),
                    moq_handle_generation(exp[e].pub_opaque),
                    moq_handle_slot(exp[e].pub_opaque));
            failures++;
        }
        if (q->track_alias != exp[e].track_alias) {
            fprintf(stderr, "FAIL %s['%s']: track alias %llu, expected the "
                    "derived %llu\n", label, exp[e].name,
                    (unsigned long long)q->track_alias,
                    (unsigned long long)exp[e].track_alias);
            failures++;
        }
        MOQ_TEST_CHECK(q->pub_opaque != 0);
        MOQ_TEST_CHECK(q->track_alias != 0);
        /* Declared answer: an accepted request is exactly one of the
         * fixture's accepted publications, a declined one is none of them.
         * The declared handle above is what identifies the request; this
         * table only says whether the fixture answered it. */
        bool accepted = false;
        for (int k = 0; k < fix->pub_x_n && k < NPUBX; k++)
            if (fix->pub_x_used[k] &&
                fix->pub_x[k]._opaque == q->pub_opaque) accepted = true;
        if (accepted != exp[e].accepted) {
            fprintf(stderr, "FAIL %s['%s']: request was %s, declared %s\n",
                    label, exp[e].name, accepted ? "ACCEPTED" : "declined",
                    exp[e].accepted ? "ACCEPTED" : "declined");
            failures++;
        }
    }
    for (int i = 0; i < have; i++) {
        if (fix->pubreq[i].truncated) {
            fprintf(stderr, "FAIL %s: PUBLISH_REQUEST %d carried a field the "
                    "record could not copy safely\n", label, i);
            failures++;
        }
        if (!fix->pubreq[i].matched) {
            /* Print ONLY what was copied. name_len is the length the event
             * declared, which may exceed the record's bounded copy -- passing
             * it as a precision would read past name[TR_COPY]. A truncated
             * record prints no bytes at all. */
            const pubreq_rec_t *q = &fix->pubreq[i];
            size_t shown = q->name_len < (size_t)TR_COPY ? q->name_len
                                                         : (size_t)TR_COPY;
            if (q->truncated)
                fprintf(stderr, "FAIL %s: undeclared PUBLISH_REQUEST whose "
                        "name (declared len %zu) could not be copied safely\n",
                        label, q->name_len);
            else
                fprintf(stderr, "FAIL %s: undeclared PUBLISH_REQUEST for track "
                        "prefix '%.*s' (declared len %zu)\n", label,
                        (int)shown, (const char *)q->name, q->name_len);
            failures++;
        }
    }
    /* Identity: session-assigned handles and aliases are pairwise distinct, so
     * two requests can never authorize each other. */
    for (int i = 0; i < have; i++)
        for (int j = i + 1; j < have; j++) {
            if (fix->pubreq[i].pub_opaque == fix->pubreq[j].pub_opaque) {
                fprintf(stderr, "FAIL %s: PUBLISH_REQUESTs %d and %d share a "
                        "publication handle\n", label, i, j);
                failures++;
            }
            if (fix->pubreq[i].track_alias == fix->pubreq[j].track_alias) {
                fprintf(stderr, "FAIL %s: PUBLISH_REQUESTs %d and %d share a "
                        "track alias\n", label, i, j);
                failures++;
            }
        }
    fix->pubreq_consumed = fix->pub_req_n;
}

/* Exact subgroup-RESET inventory: multiplicity, identity, error code and
 * end_of_group bit, matched one-to-one with no leftover. Consuming it is what
 * satisfies the final unconsumed-event invariant in report(). */
static void expect_resets(fix_t *fix, const reset_exp_t *exp, int n,
                          const char *label)
{
    if (trace_sticky(fix, label)) return;
    if (fix->reset_n != n) {
        fprintf(stderr, "FAIL %s: %d subgroup RESET, expected %d\n", label,
                fix->reset_n, n);
        failures++;
    }
    for (int i = 0; i < fix->reset_n && i < RESET_MAX; i++)
        fix->reset[i].matched = false;
    for (int e = 0; e < n; e++) {
        int hit = -1;
        for (int i = 0; i < fix->reset_n && i < RESET_MAX; i++) {
            reset_rec_t *r = &fix->reset[i];
            if (r->matched) continue;
            if (r->track == exp[e].track && r->group == exp[e].group &&
                r->subgroup == exp[e].subgroup &&
                r->error_code == exp[e].error_code &&
                r->end_of_group == exp[e].end_of_group &&
                (exp[e].sub_opaque == 0 ||
                 r->sub_opaque == exp[e].sub_opaque) &&
                (exp[e].pub_opaque == 0 ||
                 r->pub_opaque == exp[e].pub_opaque)) { hit = i; break; }
        }
        if (hit < 0) {
            fprintf(stderr, "FAIL %s: no RESET matches declared track=%d "
                    "sub=%llu g=%llu sg=%llu code=0x%llx eog=%d\n", label,
                    exp[e].track, (unsigned long long)exp[e].sub_opaque,
                    (unsigned long long)exp[e].group,
                    (unsigned long long)exp[e].subgroup,
                    (unsigned long long)exp[e].error_code,
                    (int)exp[e].end_of_group);
            failures++;
            continue;
        }
        fix->reset[hit].matched = true;
    }
    for (int i = 0; i < fix->reset_n && i < RESET_MAX; i++)
        if (!fix->reset[i].matched) {
            fprintf(stderr, "FAIL %s: undeclared RESET track=%d sub=%llu "
                    "g=%llu sg=%llu code=0x%llx\n", label, fix->reset[i].track,
                    (unsigned long long)fix->reset[i].sub_opaque,
                    (unsigned long long)fix->reset[i].group,
                    (unsigned long long)fix->reset[i].subgroup,
                    (unsigned long long)fix->reset[i].error_code);
            failures++;
        }
    fix->reset_consumed = fix->reset_n;
}

/* Declared SUBSCRIBE_DONE. */
typedef struct { uint64_t sub_opaque; uint64_t status_code; } done_exp_t;

/* Exact SUBSCRIBE_DONE inventory: multiset match on (handle, status_code) with
 * exact multiplicity and no leftover observation. Status is DECLARED, never
 * read back from the event. Consuming the inventory is what satisfies the
 * final unconsumed-event invariant in report(&fix, ). */
static void expect_done(fix_t *fix, const done_exp_t *exp, int n,
                        const char *label)
{
    if (fix->done_overflow) {
        fprintf(stderr, "FAIL %s: SUBSCRIBE_DONE overflow\n", label);
        failures++;
        return;
    }
    if (fix->done_n != n) {
        fprintf(stderr, "FAIL %s: %d SUBSCRIBE_DONE, expected %d\n", label,
                fix->done_n, n);
        failures++;
    }
    for (int i = 0; i < fix->done_n && i < NTRACK; i++)
        fix->done_matched[i] = false;
    for (int e = 0; e < n; e++) {
        int hit = -1;
        for (int i = 0; i < fix->done_n && i < NTRACK; i++)
            if (!fix->done_matched[i] &&
                fix->done_sub[i] == exp[e].sub_opaque &&
                fix->done_status[i] == exp[e].status_code) { hit = i; break; }
        if (hit < 0) {
            fprintf(stderr, "FAIL %s: no SUBSCRIBE_DONE matches declared "
                    "sub=%llu status=%llu\n", label,
                    (unsigned long long)exp[e].sub_opaque,
                    (unsigned long long)exp[e].status_code);
            failures++;
            continue;
        }
        fix->done_matched[hit] = true;
    }
    for (int i = 0; i < fix->done_n && i < NTRACK; i++)
        if (!fix->done_matched[i]) {
            fprintf(stderr, "FAIL %s: undeclared SUBSCRIBE_DONE sub=%llu "
                    "status=%llu\n", label,
                    (unsigned long long)fix->done_sub[i],
                    (unsigned long long)fix->done_status[i]);
            failures++;
        }
    fix->done_consumed = fix->done_n;
}

/* Exact control-event inventory: one namespace advertisement with the declared
 * identity and empty token inventory, and one SUBSCRIBE_OK per subscription the
 * fixture made -- matched on the EXACT handle it was given plus the declared
 * track alias, with the fixture-wide body invariants enforced. */
/* The whole CONTROL contract for a row, compared against the inventory
 * subscribe_track() declared as it created each subscription: exactly one
 * clean NAMESPACE_PUBLISHED, and exactly one SUBSCRIBE_OK per declared
 * subscription carrying its exact handle, its exact derived alias, and the
 * body invariants these tracks hold. Consuming it is what the final report
 * requires, so a row cannot pass by simply never comparing. */
static void expect_controls(fix_t *fix, const char *label)
{
    const sok_exp_t *exp = fix->sok_exp;
    const int n = fix->sok_exp_n;
    fix->sok_consumed = fix->sub_ok_n;
    /* Deliberately does NOT gate on the object-trace stickies: control events
     * are collected independently of the object trace, and one row spoils that
     * trace on purpose as a harness self-check. Control-class overflow is
     * reported by the final invariant instead. */
    if (fix->ns_pub_n != 1 || fix->ns_pub_bad) {
        fprintf(stderr, "FAIL %s: namespace_published=%d (bad_identity_or_"
                "tokens=%d, expected exactly 1 and clean)\n", label,
                fix->ns_pub_n, (int)fix->ns_pub_bad);
        failures++;
    }
    if (fix->sub_ok_n != n) {
        fprintf(stderr, "FAIL %s: subscribe_ok=%d, expected %d\n", label,
                fix->sub_ok_n, n);
        failures++;
        /* Not an early return: identity and body are still compared, so a
         * wrong-bodied response is diagnosed rather than hidden by the count. */
    }
    int have = fix->sub_ok_n < SOK_MAX ? fix->sub_ok_n : SOK_MAX;
    for (int i = 0; i < have; i++) fix->sok[i].matched = false;
    for (int e = 0; e < n; e++) {
        int hit = -1;
        for (int i = 0; i < have; i++)
            if (!fix->sok[i].matched &&
                fix->sok[i].sub_opaque == exp[e].sub_opaque) { hit = i; break; }
        if (hit < 0) {
            fprintf(stderr, "FAIL %s: no SUBSCRIBE_OK for the declared "
                    "subscription handle %llu\n", label,
                    (unsigned long long)exp[e].sub_opaque);
            failures++;
            continue;
        }
        sok_rec_t *r = &fix->sok[hit];
        r->matched = true;
        if (r->track_alias != exp[e].track_alias ||
            r->has_largest != exp[e].has_largest ||
            r->largest_group != exp[e].largest_group ||
            r->largest_object != exp[e].largest_object ||
            r->has_expires || r->expires_ms != 0 || r->props_len != 0 ||
            r->dynamic_groups) {
            fprintf(stderr, "FAIL %s: SUBSCRIBE_OK sub=%llu alias=%llu "
                    "has_largest=%d largest=%llu/%llu has_expires=%d "
                    "expires=%llu props_len=%zu dynamic_groups=%d; expected "
                    "alias=%llu has_largest=%d largest=%llu/%llu, other fields empty\n", label,
                    (unsigned long long)r->sub_opaque,
                    (unsigned long long)r->track_alias, (int)r->has_largest,
                    (unsigned long long)r->largest_group,
                    (unsigned long long)r->largest_object,
                    (int)r->has_expires, (unsigned long long)r->expires_ms,
                    r->props_len, (int)r->dynamic_groups,
                    (unsigned long long)exp[e].track_alias,
                    (int)exp[e].has_largest,
                    (unsigned long long)exp[e].largest_group,
                    (unsigned long long)exp[e].largest_object);
            failures++;
        }
    }
    for (int i = 0; i < have; i++)
        if (!fix->sok[i].matched) {
            fprintf(stderr, "FAIL %s: undeclared SUBSCRIBE_OK sub=%llu "
                    "alias=%llu\n", label,
                    (unsigned long long)fix->sok[i].sub_opaque,
                    (unsigned long long)fix->sok[i].track_alias);
            failures++;
        }
}

/* Compare the trace suffix starting at `from` against a declared expectation,
 * in order, on identity, app enqueue ordinal, payload LENGTH and every payload
 * BYTE. Any sticky trace flag fails first, by name. */

/* Order-INDEPENDENT comparison of a window of object observations against a
 * declared multiset, keyed by the exact peer handle. Delivery order across two
 * slots is not a promise this fixture may rely on, but multiplicity and
 * identity are: each declared record must be claimed by exactly one
 * observation, and no observation may be left over. */
static void expect_slots_from(fix_t *fix, int from, const tr_slot_exp_t *exp,
                              int n, const char *label)
{
    if (trace_sticky(fix, label)) return;
    if (fix->tr.n - from != n) {
        fprintf(stderr, "FAIL %s: trace has %d records after %d, expected %d\n",
                label, fix->tr.n - from, from, n);
        failures++;
        return;
    }
    for (int i = 0; i < n; i++) fix->tr.recs[from + i].matched = false;
    for (int e = 0; e < n; e++) {
        /* A declaration must name exactly ONE handle, matching the public XOR;
         * a zero/zero or both-nonzero declaration would be unfalsifiable. */
        if ((exp[e].sub_opaque != 0) == (exp[e].pub_opaque != 0)) {
            fprintf(stderr, "FAIL %s[%d]: declaration does not name exactly "
                    "one peer handle\n", label, e);
            failures++;
            continue;
        }
        int hit = -1;
        for (int i = 0; i < n; i++) {
            const tr_rec_t *r = &fix->tr.recs[from + i];
            if (r->matched) continue;
            if (r->track != exp[e].track || r->group != exp[e].group ||
                r->object != exp[e].object || r->enq_ord != exp[e].enq_ord ||
                r->len != (size_t)PAYLOAD_LEN ||
                r->sub_opaque != exp[e].sub_opaque ||
                r->pub_opaque != exp[e].pub_opaque) continue;
            bool bytes_ok = true;
            for (int b = 0; b < PAYLOAD_LEN; b++)
                if (r->copy[b] != exp[e].sentinel) { bytes_ok = false; break; }
            if (!bytes_ok) continue;
            hit = i; break;
        }
        if (hit < 0) {
            fprintf(stderr, "FAIL %s: no observation matches declared track=%d "
                    "sub=%llu pub=%llu g=%llu o=%llu enq=%d byte=0x%02x x%d\n",
                    label, exp[e].track,
                    (unsigned long long)exp[e].sub_opaque,
                    (unsigned long long)exp[e].pub_opaque,
                    (unsigned long long)exp[e].group,
                    (unsigned long long)exp[e].object, exp[e].enq_ord,
                    exp[e].sentinel, PAYLOAD_LEN);
            failures++;
            continue;
        }
        fix->tr.recs[from + hit].matched = true;
    }
    for (int i = 0; i < n; i++) {
        const tr_rec_t *r = &fix->tr.recs[from + i];
        if (r->matched) continue;
        fprintf(stderr, "FAIL %s: undeclared observation track=%d sub=%llu "
                "pub=%llu g=%llu o=%llu enq=%d len=%zu\n", label, r->track,
                (unsigned long long)r->sub_opaque,
                (unsigned long long)r->pub_opaque,
                (unsigned long long)r->group, (unsigned long long)r->object,
                r->enq_ord, r->len);
        failures++;
    }
}

static void expect_from(fix_t *fix, int from, const tr_exp_t *exp, int n,
                        const char *label)
{
    if (trace_sticky(fix, label)) return;
    if (fix->tr.n - from != n) {
        fprintf(stderr, "FAIL %s: trace has %d records after %d, expected %d\n",
                label, fix->tr.n - from, from, n);
        failures++;
        return;
    }
    for (int i = 0; i < n; i++) {
        const tr_rec_t *r = &fix->tr.recs[from + i];
        const tr_exp_t *e = &exp[i];
        bool bad = r->track != e->track || r->group != e->group ||
                   r->object != e->object || r->enq_ord != e->enq_ord ||
                   r->len != (size_t)PAYLOAD_LEN;
        if (!bad) {
            /* Byte-exact: every byte of the declared payload, not just the
             * first. The whole object is the sentinel repeated. */
            for (int b = 0; b < PAYLOAD_LEN; b++)
                if (r->copy[b] != e->sentinel) { bad = true; break; }
        }
        if (bad) {
            fprintf(stderr,
                    "FAIL %s[%d]: got track=%d g=%llu o=%llu enq=%d len=%zu "
                    "bytes=", label, i, r->track, (unsigned long long)r->group,
                    (unsigned long long)r->object, r->enq_ord, r->len);
            for (size_t b = 0; b < r->len && b < TR_COPY; b++)
                fprintf(stderr, "%02x", r->copy[b]);
            fprintf(stderr, ", expected track=%d g=%llu o=%llu enq=%d len=%d "
                    "byte=0x%02x x%d\n", e->track,
                    (unsigned long long)e->group, (unsigned long long)e->object,
                    e->enq_ord, PAYLOAD_LEN, e->sentinel, PAYLOAD_LEN);
            failures++;
        }
    }
}

/* Conservation: written == sent + queued + policy drops, each category named,
 * PLUS the exact queued byte count. Every fixture object is PAYLOAD_LEN bytes
 * with no caller-supplied properties, so the byte budget is exactly
 * PAYLOAD_LEN * objects_queued and is declared literally by the caller. */
static void expect_conserved(moq_media_sender_t *s, uint64_t written,
                             uint64_t sent, uint64_t queued, uint64_t dropped,
                             uint64_t bytes_queued, const char *label)
{
    moq_media_sender_stats_t st;
    memset(&st, 0, sizeof(st));
    MOQ_TEST_CHECK(moq_media_sender_get_stats(s, &st, sizeof(st)) == MOQ_OK);
    if (st.objects_written != written || st.objects_sent != sent ||
        st.objects_queued != queued || st.objects_dropped != dropped ||
        st.bytes_queued != bytes_queued ||
        st.objects_written != st.objects_sent + st.objects_queued +
                              st.objects_dropped) {
        fprintf(stderr,
                "FAIL %s: written=%llu sent=%llu queued=%llu dropped=%llu "
                "bytes_queued=%llu (expected %llu/%llu/%llu/%llu/%llu)\n", label,
                (unsigned long long)st.objects_written,
                (unsigned long long)st.objects_sent,
                (unsigned long long)st.objects_queued,
                (unsigned long long)st.objects_dropped,
                (unsigned long long)st.bytes_queued,
                (unsigned long long)written, (unsigned long long)sent,
                (unsigned long long)queued, (unsigned long long)dropped,
                (unsigned long long)bytes_queued);
        failures++;
    }
}


/* -- Fixture ----------------------------------------------------------- */

/* Wire record of the publisher's CANCELLED (0x1) subgroup resets: every
 * RESET_DATA action the client (the media sender's session) emits with that
 * code. A subscriber that cancelled has deleted its subscription state, so
 * only the wire shows that the publisher reset the cancelled subscription's
 * open subgroup. */
static int g_cancel_resets;
/* These fixture media objects have nonempty payload/properties. A non-FIN
 * SEND_DATA with header bytes but no payload is their subgroup header. */
static int g_subgroup_headers;

static void trace_cancel_resets(void *ctx, const moq_sim_trace_record_t *r)
{
    (void)ctx;
    if (r->kind == MOQ_SIM_TRACE_ACTION && r->from == MOQ_PERSPECTIVE_CLIENT &&
        r->action_kind == MOQ_ACTION_RESET_DATA && r->code == 0x1)
        g_cancel_resets++;
    if (r->kind == MOQ_SIM_TRACE_ACTION && r->from == MOQ_PERSPECTIVE_CLIENT &&
        r->action_kind == MOQ_ACTION_SEND_DATA && r->bytes.len > 0 &&
        r->count == 0 &&
        r->struct_size >= offsetof(moq_sim_trace_record_t, fin) + sizeof(r->fin) &&
        !r->fin)
        g_subgroup_headers++;
}

static moq_simpair_t *make_pair(moq_alloc_t *alloc, uint32_t max_actions,
                                moq_version_t ver)
{
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = alloc; cfg.seed = 42; cfg.initial_now_us = 1000;
    cfg.trace_fn = trace_cancel_resets;
    g_cancel_resets = 0;
    g_subgroup_headers = 0;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 32;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = 32;
    cfg.version = ver;
    cfg.max_actions = max_actions;   /* 0 = library default */
    moq_simpair_t *sp = NULL;
    if (moq_simpair_create(&cfg, &sp) != MOQ_OK) return NULL;
    moq_simpair_start(sp);
    (void)moq_simpair_run_until_quiescent(sp, 64, NULL);
    moq_event_t ev;
    while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
        moq_event_cleanup(&ev);
    while (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1)
        moq_event_cleanup(&ev);
    return sp;
}

static moq_media_track_t *add_track(moq_media_sender_t *s, const char *name,
                                    moq_media_type_t mt)
{
    moq_media_track_cfg_t tc;
    moq_media_track_cfg_init(&tc);
    tc.name = (moq_bytes_t){ (const uint8_t *)name, strlen(name) };
    tc.media_type = mt;
    tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
    tc.codec = mt == MOQ_MEDIA_TYPE_VIDEO
        ? (moq_bytes_t){ (const uint8_t *)"av01", 4 }
        : (moq_bytes_t){ (const uint8_t *)"opus", 4 };
    tc.bitrate = 1500000;
    tc.is_live = true;
    if (mt == MOQ_MEDIA_TYPE_AUDIO) {
        /* MSF 5.2.28/5.2.29: an audio track requires samplerate + channelConfig. */
        tc.samplerate = 48000;
        tc.channel_config = (moq_bytes_t){ (const uint8_t *)"2", 1 };
    }
    moq_media_track_t *t = NULL;
    (void)moq_media_sender_add_track(s, &tc, &t);
    return t;
}

/* Build a pull-mode (advertise + answer SUBSCRIBE) sender with two tracks,
 * driven to ready against a peer that accepts the namespace. Demand is then
 * per-track and entirely the peer's choice. */
/* Existing pacing controls use draft-16 explicitly. Demand-recovery cases
 * pass their own version to exercise both draft-16 and draft-18. */
#define PACING_VERSION MOQ_VERSION_DRAFT_16

/* `queue_objects` 0 keeps the library default; a nonzero value sets
 * queue_max_objects, i.e. the ACTIVE post-ready queue object bound, which is
 * what refusals are measured against here (the physical allocation is larger
 * and is an implementation detail). When it is set, the policy becomes
 * RETURN_WOULD_BLOCK so a full queue refuses cleanly instead of evicting. */
static bool fix_init_q2(fix_t *fix, moq_alloc_t *alloc, uint32_t max_actions,
                        bool drop_without_demand, uint64_t now,
                        uint32_t queue_objects, bool keep_drop_policy,
                        bool publish_tracks, const char *accept_pub_name,
                        int accept_pub_track, uint64_t version,
                        moq_media_send_backpressure_t drop_policy)
{
    memset(fix, 0, sizeof(*fix));
    fix->publish_mode = publish_tracks;
    fix->accept_pub_name = accept_pub_name;
    fix->accept_pub_track = accept_pub_track;
    for (int i = 0; i < SENTINEL_MAX; i++) fix->enq_ord[i] = -1;
    fix->phase = PHASE_ARM;
    fix->sp = make_pair(alloc, max_actions, version);
    if (!fix->sp) return false;
    moq_session_t *cl = moq_simpair_client(fix->sp);
    moq_session_t *srv = moq_simpair_server(fix->sp);

    g_ns[0] = MOQ_BYTES_LITERAL("svc");
    g_ns[1] = MOQ_BYTES_LITERAL("pace");

    moq_media_sender_cfg_t cfg;
    moq_media_sender_cfg_init_live_sized(&cfg, sizeof(cfg));
    cfg.namespace_ = (moq_namespace_t){ g_ns, 2 };
    cfg.publish_tracks = publish_tracks;        /* pull: demand == SUBSCRIBE */
    cfg.drop_without_demand = drop_without_demand;
    cfg.catalog_refresh_interval_us = UINT64_MAX;  /* no periodic republish */
    if (queue_objects) {
        cfg.queue_max_objects = queue_objects;
        /* Default for a bounded fixture: refuse cleanly. keep_drop_policy
         * leaves the live preset in place so a full queue EVICTS instead. */
        if (!keep_drop_policy)
            cfg.backpressure = MOQ_MEDIA_SEND_BP_RETURN_WOULD_BLOCK;
        else if (drop_policy != MOQ_MEDIA_SEND_BP_UNSET)
            cfg.backpressure = drop_policy;
    }
    moq_media_sender_callbacks_init_sized(&cfg.callbacks, sizeof(cfg.callbacks));
    cfg.callbacks.ctx = fix;
    cfg.callbacks.on_ready = on_ready_cb;
    cfg.callbacks.on_subscriber_joined = on_joined_cb;
    fix->s = moq_media_sender_test_new_cfg(&cfg);
    if (!fix->s) return false;

    fix->t[0] = add_track(fix->s, "v", MOQ_MEDIA_TYPE_VIDEO);
    fix->t[1] = add_track(fix->s, "a", MOQ_MEDIA_TYPE_AUDIO);
    if (!fix->t[0] || !fix->t[1]) return false;

    if (publish_tracks) {
        /* PRE-INGRESS derivation, taken before the sender has published
         * anything, so every expected value comes from source state rather
         * than from the events under test:
         *
         *   ALIAS  - the publisher consumes one per successful
         *            moq_session_publish, and the sender issues them in a
         *            fixed source order: the catalog first
         *            (media_sender.c:2413), then the frozen media snapshot in
         *            add order, "v" then "a" (:2437);
         *   HANDLE - the receiver mints
         *            moq_handle_pack(PUBLICATION, session_tag,
         *                            generation | 1, slot) over the first free
         *            publication slot (session_publish.c:664), and each
         *            committed request occupies its slot, so the three
         *            requests take the first three free slots in arrival
         *            order. */
        uint64_t a0 = cl->profile->next_track_alias(cl);
        /* One request per published track: the catalog plus this fixture's
         * NTRACK media tracks. */
        const int want_pub = 1 + NTRACK;
        int found = 0;
        for (size_t k = 0; k < srv->pub_cap && found < want_pub; k++) {
            if (srv->publishes[k].state != MOQ_PUB_FREE) continue;
            uint32_t gen = srv->publishes[k].generation | 1u;
            fix->exp_pub_handle[found] = moq_handle_pack(
                MOQ_HANDLE_POOL_PUBLICATION, srv->session_tag, gen,
                (uint32_t)k);
            fix->exp_pub_alias[found] = a0 + (uint64_t)found;
            /* A structurally invalid pack would silently compare equal to a
             * zero handle, so the derivation itself is checked. */
            MOQ_TEST_CHECK(fix->exp_pub_handle[found] != 0);
            found++;
        }
        fix->exp_pub_n = found;
        MOQ_TEST_CHECK_EQ_INT(fix->exp_pub_n, want_pub);
    }

    fix->accept_ns = true;
    for (int c = 0; c < 16 && !moq_media_sender_is_ready(fix->s); c++)
        drive(fix, now);
    (void)cl; (void)srv;
    /* Harness self-check: the optional on_ready callback MUST have fired, so a
     * silently-unwired callbacks bundle cannot make readiness vacuous. */
    MOQ_TEST_CHECK(moq_media_sender_is_ready(fix->s));
    MOQ_TEST_CHECK_EQ_INT(fix->ready_n, 1);
    /* Exactly one namespace advertisement, carrying the declared identity. */
    MOQ_TEST_CHECK_EQ_INT(fix->ns_pub_n, 1);
    MOQ_TEST_CHECK(!fix->ns_pub_bad);
    MOQ_TEST_CHECK_EQ_INT(fix->sub_ok_n, 0);   /* nothing subscribed yet */
    return moq_media_sender_is_ready(fix->s) && fix->ready_n == 1 &&
           fix->ns_pub_n == 1 && !fix->ns_pub_bad;
}

static bool fix_init_q(fix_t *fix, moq_alloc_t *alloc, uint32_t max_actions,
                       bool drop_without_demand, uint64_t now,
                       uint32_t queue_objects, bool keep_drop_policy)
{
    return fix_init_q2(fix, alloc, max_actions, drop_without_demand, now,
                       queue_objects, keep_drop_policy, false, NULL, -1,
                       PACING_VERSION, MOQ_MEDIA_SEND_BP_UNSET);
}

/* Publish-mode variant: the sender PUSHES its tracks, and the peer accepts the
 * publication of exactly one named track, so that track carries TWO active
 * facade slots -- its subscription and its publication. */
static bool fix_init_pub_act(fix_t *fix, moq_alloc_t *alloc, uint64_t now,
                             uint32_t queue_objects, uint32_t max_actions,
                             const char *accept_pub_name, int accept_pub_track)
{
    return fix_init_q2(fix, alloc, max_actions, false, now, queue_objects,
                       true, true, accept_pub_name, accept_pub_track,
                       PACING_VERSION, MOQ_MEDIA_SEND_BP_UNSET);
}

static bool fix_init(fix_t *fix, moq_alloc_t *alloc, uint32_t max_actions,
                     bool drop_without_demand, uint64_t now)
{
    return fix_init_q(fix, alloc, max_actions, drop_without_demand, now, 0,
                      false);
}

/* Bounded active queue that keeps the LIVE drop policy, so a full queue EVICTS
 * (capacity policy) instead of refusing. */
static bool fix_init_drop(fix_t *fix, moq_alloc_t *alloc, uint64_t now,
                          uint32_t queue_objects)
{
    return fix_init_q(fix, alloc, 0, false, now, queue_objects, true);
}

/* Same, with a constrained session action ring so real shared capacity can
 * refuse the reset through the production facade path. */
static bool fix_init_drop_act(fix_t *fix, moq_alloc_t *alloc, uint64_t now,
                              uint32_t queue_objects, uint32_t max_actions)
{
    return fix_init_q(fix, alloc, max_actions, false, now, queue_objects, true);
}

/* Same, with an explicit drop policy. */
static bool fix_init_policy_act(fix_t *fix, moq_alloc_t *alloc, uint64_t now,
                                uint32_t queue_objects, uint32_t max_actions,
                                moq_media_send_backpressure_t policy,
                                moq_version_t version)
{
    return fix_init_q2(fix, alloc, max_actions, false, now, queue_objects,
                       true, false, NULL, -1, version, policy);
}

/* The peer subscribes to one media track; returns once SUBSCRIBE_OK arrived
 * and the sender reports demand for it. */
static bool subscribe_track(fix_t *fix, int idx, const char *name, uint64_t now)
{
    moq_session_t *cl = moq_simpair_client(fix->sp);
    moq_session_t *srv = moq_simpair_server(fix->sp);
    moq_subscribe_cfg_t sc; moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ g_ns, 2 };
    sc.track_name = (moq_bytes_t){ (const uint8_t *)name, strlen(name) };
    sc.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    sc.has_forward = true; sc.forward = true;
    moq_subscription_t sub; memset(&sub, 0, sizeof(sub));
    if (moq_session_subscribe(srv, &sc, now, &sub) != MOQ_OK) return false;
    /* Publish the handle BEFORE driving so any object this subscription
     * delivers is attributed, never dropped from the trace. */
    fix->sub[idx] = sub;
    fix->subscribed[idx] = true;
    /* Declare this subscription's SUBSCRIBE_OK BEFORE driving: the exact
     * handle subscribe returned, and the exact alias the PUBLISHER session
     * will consume next, read here from pre-ingress source state. Declaring it
     * in the helper is what closes the omission class -- a row cannot forget
     * to declare a control it created. */
    if (fix->sok_exp_n < SOK_MAX) {
        fix->sok_exp[fix->sok_exp_n].sub_opaque = sub._opaque;
        fix->sok_exp[fix->sok_exp_n].track_alias =
            cl->profile->next_track_alias(cl);
        fix->sok_exp_n++;
    } else {
        MOQ_TEST_CHECK(false);   /* declaration table too small */
    }
    int want = fix->sub_ok_n + 1;
    for (int i = 0; i < 16 && fix->sub_ok_n < want; i++)
        drive(fix, now);
    /* EXACTLY one SUBSCRIBE_OK per subscription: a duplicate is a failure,
     * not a tolerated extra. */
    bool ok = fix->sub_ok_n == want;
    fix->subscribed[idx] = ok;
    (void)cl;
    MOQ_TEST_CHECK(ok);
    MOQ_TEST_CHECK(moq_media_sender_track_has_subscriber(fix->s, fix->t[idx]));
    return ok;
}


static void fix_free(fix_t *fix)
{
    if (fix->s) moq_media_sender_test_free(fix->s);
    if (fix->sp) {
        moq_event_t ev;
        while (moq_session_poll_events(moq_simpair_client(fix->sp), &ev, 1) == 1)
            moq_event_cleanup(&ev);
        while (moq_session_poll_events(moq_simpair_server(fix->sp), &ev, 1) == 1)
            moq_event_cleanup(&ev);
        moq_simpair_destroy(fix->sp);
    }
}

/* Phase transitions are explicit: PACE narrows the legal peer-event set to
 * media objects only, so a stray control event during a measured drain fails
 * by name instead of being cleaned away. */
static void set_phase(fix_t *fix, phase_t ph) { fix->phase = ph; }

/* Write one object whose whole payload is a unique repeated sentinel, and
 * record the APP ENQUEUE ORDINAL for that sentinel on success. The trace
 * resolves each received object back to its ordinal through this mapping, so
 * enqueue order is compared, not inferred from array position. */
static moq_result_t write_obj(fix_t *fix, int idx, uint8_t sentinel, bool sync,
                              bool starts_group, uint64_t pts)
{
    /* PREFLIGHT, before any allocation or product call: a reused sentinel
     * would make the ordinal mapping ambiguous. Refuse it deterministically
     * with no product write, no ordinal advance and no queue mutation. */
    if (fix->enq_ord[sentinel] != -1)
        return MOQ_ERR_WRONG_STATE;
    uint8_t d[PAYLOAD_LEN];
    memset(d, sentinel, sizeof(d));
    moq_rcbuf_t *b = NULL;
    if (moq_rcbuf_create(moq_alloc_default(), d, sizeof(d), &b) != MOQ_OK)
        return MOQ_ERR_NOMEM;
    moq_media_send_object_t o; memset(&o, 0, sizeof(o));
    o.struct_size = sizeof(o);
    o.payload = b;
    o.is_sync = sync;
    o.starts_group = starts_group;
    o.presentation_time_us = pts;
    moq_result_t rc = moq_media_sender_write(fix->s, fix->t[idx], &o);
    if (rc != MOQ_OK) {
        moq_rcbuf_decref(b);   /* no transfer on failure */
        return rc;
    }
    fix->enq_ord[sentinel] = fix->enq_n++;
    return rc;
}

/* ===================================================================== *
 *  Scenario 1 -- one-at-a-time demanded control.
 *  Each accepted object is followed by exactly ONE production drive. Proves
 *  the object leaves in that same drive, with exact FIFO identity and exact
 *  conservation at every boundary.
 * ===================================================================== */
static void test_one_at_a_time(void)
{
    int before = failures;
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    fix_t fix;
    uint64_t now = 1000;
    if (!fix_init(&fix, &alloc, 0, false, now)) { failures++; goto out; }
    if (!subscribe_track(&fix, 0, "v", now)) { failures++; goto out; }
    set_phase(&fix, PHASE_PACE);

    /* One sync-anchored group per object keeps the identity trivially
     * declarable: group i, object 0, sentinel 0x40+i. */
    for (int i = 0; i < 4; i++) {
        int from = fix.tr.n;
        const tr_exp_t exp[1] = {
            { 0, (uint64_t)i, 0, (uint8_t)(0x40 + i), i } };
        MOQ_TEST_CHECK(write_obj(&fix, 0, (uint8_t)(0x40 + i), true, true,
                                 1000u * (uint64_t)(i + 1)) == MOQ_OK);
        /* Written, not yet drained: it is queued and nothing was dropped. */
        expect_conserved(fix.s, (uint64_t)i + 1, (uint64_t)i, 1, 0, PAYLOAD_LEN,
                         "s1-after-write");
        /* PERMANENT fixture contract: reusing a live sentinel is refused
         * BEFORE any product call -- deterministic error, no ordinal advance,
         * and not one byte of sender state moved. */
        {
            int ord_before = fix.enq_n;
            moq_result_t dup = write_obj(&fix, 0, (uint8_t)(0x40 + i), true,
                                         true, 999999);
            MOQ_TEST_CHECK(dup == MOQ_ERR_WRONG_STATE);
            MOQ_TEST_CHECK_EQ_INT(fix.enq_n, ord_before);
            expect_conserved(fix.s, (uint64_t)i + 1, (uint64_t)i, 1, 0,
                             PAYLOAD_LEN, "s1-dup-sentinel-no-mutation");
        }
        int pass_before = fix.pass;
        drive(&fix, now);
        /* Exactly one drive was needed, and it emitted exactly this object. */
        MOQ_TEST_CHECK_EQ_INT(fix.pass, pass_before + 1);
        expect_from(&fix, from, exp, 1, "s1");
        MOQ_TEST_CHECK_EQ_INT(fix.tr.recs[from].pass, fix.pass);
        expect_conserved(fix.s, (uint64_t)i + 1, (uint64_t)i + 1, 0, 0, 0,
                         "s1-after-drive");
    }
    /* v0 emits one subgroup per group, and a group's subgroup FINs when the
     * NEXT group opens -- so four one-object groups close exactly three
     * subgroups, the fourth still being open. Declared before the check. */
    {
        const term_exp_t terms[3] = {
            { 0, 0, 0, true, true }, { 0, 1, 0, true, true }, { 0, 2, 0, true, true } };
        expect_terminals(&fix, terms, 3, "s1-terminals");
    }
    /* Controls are declared by subscribe_track() and compared by the final
     * report; no per-row call is needed or possible to forget. */
    /* Harness self-check: the trace collector really ran and really saw data. */
    MOQ_TEST_CHECK_EQ_INT(fix.tr.n, 4);
    MOQ_TEST_CHECK(fix.collect_calls >= 4);
    MOQ_TEST_CHECK_EQ_INT(fix.joined_n, 1);

    /* PERMANENT classifier self-check, through the LIVE collector -- not a
     * source-only helper. Every comparison above is done, so the trace may now
     * be deliberately spoiled: write one more object, put the fixture back into
     * ordinary ARM, and drive. The object arrives in a phase that FORBIDS
     * objects, so the collector must raise its named sticky failure and refuse
     * to adopt it into the trace. */
    {
        int recs_before = fix.tr.n;
        int terms_before = fix.term_n;
        /* Opening a new group makes this drive carry BOTH an object and the
         * PRIOR group's subgroup terminal, so the object-side and
         * terminal-side gates are exercised independently. */
        MOQ_TEST_CHECK(write_obj(&fix, 0, 0x4f, true, true, 90000) == MOQ_OK);
        set_phase(&fix, PHASE_ARM);
        drive(&fix, now);
        MOQ_TEST_CHECK(fix.tr.obj_in_arm);                 /* rejected, by name */
        MOQ_TEST_CHECK_EQ_INT(fix.tr.n, recs_before);      /* object not adopted */
        MOQ_TEST_CHECK_EQ_INT(fix.term_n, terms_before);   /* terminal not adopted */
        set_phase(&fix, PHASE_PACE);
    }

out:
    fix_free(&fix);
    MOQ_TEST_CHECK(as.balance == 0);
    report(&fix, "pacing_one_at_a_time", before);
}

/* ===================================================================== *
 *  Scenario 2 -- burst demanded control.
 *  A GOP-shaped burst (one sync anchor + five deltas in one group) enqueued
 *  before any drive must drain as far as capacity allows in a SINGLE drive.
 * ===================================================================== */
static void test_burst(void)
{
    int before = failures;
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    fix_t fix;
    uint64_t now = 1000;
    if (!fix_init(&fix, &alloc, 0, false, now)) { failures++; goto out; }
    if (!subscribe_track(&fix, 0, "v", now)) { failures++; goto out; }
    set_phase(&fix, PHASE_PACE);

    enum { GOP = 6 };
    const tr_exp_t exp[GOP] = {
        { 0, 0, 0, 0x50, 0 }, { 0, 0, 1, 0x51, 1 }, { 0, 0, 2, 0x52, 2 },
        { 0, 0, 3, 0x53, 3 }, { 0, 0, 4, 0x54, 4 }, { 0, 0, 5, 0x55, 5 },
    };
    for (int i = 0; i < GOP; i++)
        MOQ_TEST_CHECK(write_obj(&fix, 0, (uint8_t)(0x50 + i), i == 0, i == 0,
                                 1000u * (uint64_t)(i + 1)) == MOQ_OK);
    expect_conserved(fix.s, GOP, 0, GOP, 0, GOP * PAYLOAD_LEN, "s2-queued");

    int from = fix.tr.n;
    drive(&fix, now);
    expect_from(&fix, from, exp, GOP, "s2-burst");
    expect_conserved(fix.s, GOP, GOP, 0, 0, 0, "s2-drained");
    /* All six left in ONE drive: every record carries the same pass. */
    for (int i = 0; i < GOP; i++)
        MOQ_TEST_CHECK_EQ_INT(fix.tr.recs[from + i].pass, fix.pass);

    /* A further drive with no new media emits nothing (no duplication). */
    int from2 = fix.tr.n;
    drive(&fix, now);
    expect_from(&fix, from2, exp, 0, "s2-idle-drive");
    expect_conserved(fix.s, GOP, GOP, 0, 0, 0, "s2-idle");
    /* One group only: its subgroup is still open, so NO terminal exists. */
    expect_terminals(&fix, NULL, 0, "s2-terminals");
    {
    }

    /* Close that six-object group by opening the next one. Its terminal must
     * then follow the LAST of the six objects, not merely the first -- the
     * multi-object case the ordering assertion exists for. */
    {
        const tr_exp_t nxt[1] = { { 0, 1, 0, 0x56, GOP } };
        int from3 = fix.tr.n;
        MOQ_TEST_CHECK(write_obj(&fix, 0, 0x56, true, true, 7000) == MOQ_OK);
        drive(&fix, now);
        expect_from(&fix, from3, nxt, 1, "s2-next-group");
        expect_conserved(fix.s, GOP + 1, GOP + 1, 0, 0, 0, "s2-next-group-stats");
        const term_exp_t terms[1] = { { 0, 0, 0, true, true } };
        expect_terminals(&fix, terms, 1, "s2-terminal-after-six");
        /* Non-vacuity of the ordering rule: the terminal really did arrive
         * after SIX objects of its own subgroup, not after one. */
        MOQ_TEST_CHECK_EQ_INT(fix.term_n, 1);
        if (fix.term_n == 1)
            MOQ_TEST_CHECK_EQ_INT(fix.term[0].objs_before, GOP);
    }

out:
    fix_free(&fix);
    MOQ_TEST_CHECK(as.balance == 0);
    report(&fix, "pacing_burst", before);
}

/* ===================================================================== *
 *  Scenario 3 -- alternating demanded tracks.
 *  Both tracks have demand. Proves the drain follows write order exactly and
 *  does NOT batch per track.
 * ===================================================================== */
static void test_alternating(void)
{
    int before = failures;
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    fix_t fix;
    uint64_t now = 1000;
    if (!fix_init(&fix, &alloc, 0, false, now)) { failures++; goto out; }
    if (!subscribe_track(&fix, 0, "v", now)) { failures++; goto out; }
    if (!subscribe_track(&fix, 1, "a", now)) { failures++; goto out; }
    set_phase(&fix, PHASE_PACE);
    MOQ_TEST_CHECK_EQ_INT(fix.joined_n, 2);
    /* The two subscription handles must be distinct, or the per-track
     * attribution in collect() would be degenerate. */
    MOQ_TEST_CHECK(fix.sub[0]._opaque != fix.sub[1]._opaque);

    /* v0 a0 v1 a1 v2 a2 -- each object opens its own group on its own track. */
    const tr_exp_t exp[6] = {
        { 0, 0, 0, 0x60, 0 }, { 1, 0, 0, 0x61, 1 },
        { 0, 1, 0, 0x62, 2 }, { 1, 1, 0, 0x63, 3 },
        { 0, 2, 0, 0x64, 4 }, { 1, 2, 0, 0x65, 5 },
    };
    for (int i = 0; i < 6; i++)
        MOQ_TEST_CHECK(write_obj(&fix, i & 1, (uint8_t)(0x60 + i), true, true,
                                 1000u * (uint64_t)(i + 1)) == MOQ_OK);
    expect_conserved(fix.s, 6, 0, 6, 0, 6 * PAYLOAD_LEN, "s3-queued");

    int from = fix.tr.n;
    drive(&fix, now);
    expect_from(&fix, from, exp, 6, "s3-alternating");
    expect_conserved(fix.s, 6, 6, 0, 0, 0, "s3-drained");
    /* Non-vacuity: the observed order really does alternate tracks, so a
     * per-track batching regression cannot pass by accident. */
    for (int i = 0; i < 6; i++)
        MOQ_TEST_CHECK_EQ_INT(fix.tr.recs[from + i].track, i & 1);
    /* Each track wrote three one-object groups, so each closes its first two:
     * four terminals in total, derived from the geometry, not read back. */
    {
        const term_exp_t terms[4] = {
            { 0, 0, 0, true, true }, { 0, 1, 0, true, true },
            { 1, 0, 0, true, true }, { 1, 1, 0, true, true } };
        expect_terminals(&fix, terms, 4, "s3-terminals");
    }
    /* Controls are declared by subscribe_track() and compared by the final
     * report; no per-row call is needed or possible to forget. */

out:
    fix_free(&fix);
    MOQ_TEST_CHECK(as.balance == 0);
    report(&fix, "pacing_alternating", before);
}

/* ===================================================================== *
 *  Scenario 4 -- undemanded-head discriminator.
 *  An UNDEMANDED track sits at the FIFO head with a DEMANDED track behind it.
 *  This pins what the product actually does; it changes no semantics.
 * ===================================================================== */
static void test_isolation(bool drop_without_demand)
{
    int before = failures;
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    fix_t fix;
    uint64_t now = 1000;
    if (!fix_init(&fix, &alloc, 0, drop_without_demand, now)) {
        failures++; goto out;
    }
    /* ONLY audio is subscribed: video has no demand at all. */
    if (!subscribe_track(&fix, 1, "a", now)) { failures++; goto out; }
    set_phase(&fix, PHASE_PACE);
    MOQ_TEST_CHECK(!moq_media_sender_track_has_subscriber(fix.s, fix.t[0]));
    MOQ_TEST_CHECK(moq_media_sender_track_has_subscriber(fix.s, fix.t[1]));

    /* INTERLEAVED, two per track, so the rows prove PER-TRACK order and not
     * merely that one skipped head was tolerated:
     *   ordinal 0  video g0   (undemanded)
     *   ordinal 1  audio g0   (demanded)
     *   ordinal 2  video g1   (undemanded)
     *   ordinal 3  audio g1   (demanded)  */
    MOQ_TEST_CHECK(write_obj(&fix, 0, 0x70, true, true, 1000) == MOQ_OK);
    MOQ_TEST_CHECK(write_obj(&fix, 1, 0x71, true, true, 2000) == MOQ_OK);
    MOQ_TEST_CHECK(write_obj(&fix, 0, 0x72, true, true, 3000) == MOQ_OK);
    MOQ_TEST_CHECK(write_obj(&fix, 1, 0x73, true, true, 4000) == MOQ_OK);
    expect_conserved(fix.s, 4, 0, 4, 0, 4 * PAYLOAD_LEN, "iso-queued");
    /* Non-vacuity: the fixture really did queue four objects across two
     * tracks, so an empty fixture cannot masquerade as the intended outcome. */
    MOQ_TEST_CHECK_EQ_INT(fix.enq_n, 4);

    /* Declared audio identity/order -- ordinals 1 then 3, group 0 then 1. */
    const tr_exp_t audio[2] = { { 1, 0, 0, 0x71, 1 }, { 1, 1, 0, 0x73, 3 } };
    /* Declared video identity/order once its demand arrives. */
    const tr_exp_t video[2] = { { 0, 0, 0, 0x70, 0 }, { 0, 1, 0, 0x72, 2 } };

    if (!drop_without_demand) {
        /* ---- REQUIRED CONTRACT: the default hold is PER TRACK -----------
         * ONE production hook must deliver the demanded audio in exact order
         * while retaining every undemanded video object exactly. */
        int from = fix.tr.n;
        int pass_before = fix.pass;
        drive(&fix, now);
        MOQ_TEST_CHECK_EQ_INT(fix.pass, pass_before + 1);   /* exactly one hook */
        expect_from(&fix, from, audio, 2, "iso-hold-one-hook-audio");
        expect_conserved(fix.s, 4, 2, 2, 0, 2 * PAYLOAD_LEN,
                         "iso-hold-one-hook-stats");
        /* Both audio objects left in that single hook. */
        if (fix.tr.n - from == 2) {
            MOQ_TEST_CHECK_EQ_INT(fix.tr.recs[from].pass, fix.pass);
            MOQ_TEST_CHECK_EQ_INT(fix.tr.recs[from + 1].pass, fix.pass);
        }

        /* A second hook with video STILL undemanded: emit nothing, mutate
         * nothing, and terminate inside the drive budget. */
        int from2 = fix.tr.n;
        drive(&fix, now);
        expect_from(&fix, from2, audio, 0, "iso-hold-second-hook-idle");
        expect_conserved(fix.s, 4, 2, 2, 0, 2 * PAYLOAD_LEN,
                         "iso-hold-second-hook-stats");

        /* Video demand arrives: the retained video releases EXACTLY once, in
         * video-track order, with exact final stats. */
        int from3 = fix.tr.n;
        set_phase(&fix, PHASE_RELEASE);
        if (!subscribe_track(&fix, 0, "v", now)) { failures++; goto out; }
        set_phase(&fix, PHASE_PACE);
        drive(&fix, now);   /* one further hook: no duplication, no new work */
        expect_from(&fix, from3, video, 2, "iso-hold-release-video");
        expect_conserved(fix.s, 4, 4, 0, 0, 0, "iso-hold-release-stats");
        /* Complete terminal inventory: each track wrote two one-object
         * groups, so each closes its first. */
        {
            const term_exp_t terms[2] = { { 1, 0, 0, true, true }, { 0, 0, 0, true, true } };
            expect_terminals(&fix, terms, 2, "iso-hold-terminals");
        }
        /* Controls are declared by subscribe_track() and compared by the final
     * report; no per-row call is needed or possible to forget. */
    } else {
        /* ---- REQUIRED CONTRACT: drop_without_demand completes in the
         * SAME hook ------------------------------------------------------
         * One hook drops all queued video media AND continues to deliver the
         * demanded audio. No unrelated later wake may be required. */
        int from = fix.tr.n;
        int pass_before = fix.pass;
        drive(&fix, now);
        MOQ_TEST_CHECK_EQ_INT(fix.pass, pass_before + 1);   /* exactly one hook */
        expect_from(&fix, from, audio, 2, "iso-drop-one-hook-audio");
        /* Exact drop count and bytes: both video objects dropped, nothing of
         * theirs left queued, and no video object on the wire. */
        expect_conserved(fix.s, 4, 2, 0, 2, 0, "iso-drop-one-hook-stats");
        {   /* Exact drop accounting: two video objects discarded, both sync
             * points, and TWO distinct groups discarded. */
            moq_media_sender_stats_t st; memset(&st, 0, sizeof(st));
            MOQ_TEST_CHECK(moq_media_sender_get_stats(fix.s, &st, sizeof(st))
                           == MOQ_OK);
            MOQ_TEST_CHECK_EQ_U64(st.objects_dropped, 2);
            MOQ_TEST_CHECK_EQ_U64(st.keyframes_dropped, 2);
            /* TWO distinct video groups had media discarded, so
             * groups_dropped is 2 -- the field counts distinct (track, group)
             * values with at least one discard, not per-track passes.
             * groups_abandoned counts open wire subgroups actually driven
             * through RESET, of which there are none here. */
            MOQ_TEST_CHECK_EQ_U64(st.groups_dropped, 2);
            MOQ_TEST_CHECK_EQ_U64(st.groups_abandoned, 0);
        }
        for (int i = from; i < fix.tr.n; i++)
            MOQ_TEST_CHECK_EQ_INT(fix.tr.recs[i].track, 1);

        /* A following idle hook produces no duplicate output and no new work. */
        int from2 = fix.tr.n;
        drive(&fix, now);
        expect_from(&fix, from2, audio, 0, "iso-drop-idle-hook");
        expect_conserved(fix.s, 4, 2, 0, 2, 0, "iso-drop-idle-stats");
        {
            const term_exp_t terms[1] = { { 1, 0, 0, true, true } };
            expect_terminals(&fix, terms, 1, "iso-drop-terminals");
        }
        {
        }
    }

out:
    fix_free(&fix);
    MOQ_TEST_CHECK(as.balance == 0);
    report(&fix, drop_without_demand ? "pacing_isolation_drop"
                              : "pacing_isolation_hold", before);
}

/* Demand returning after a discarded group head must not renumber its tail
 * as object zero. Exercise separate drain passes, two discarded groups, a
 * sync sample that does not start a group, another demanded track, and END
 * both before and after recovery. All observations come from the real peer. */
static void test_demand_resumes_at_group_start(uint64_t version, bool end_skipped)
{
    int before = failures;
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    fix_t fix;
    const uint64_t now = 1000;
    const char *name = version == MOQ_VERSION_DRAFT_16
        ? (end_skipped ? "pacing_d16_end_skipped_group" : "pacing_d16_resume_group")
        : (end_skipped ? "pacing_d18_end_skipped_group" : "pacing_d18_resume_group");
    if (!fix_init_q2(&fix, &alloc, 0, true, now, 0, false,
                     false, NULL, -1, version, MOQ_MEDIA_SEND_BP_UNSET)) {
        failures++; goto out;
    }
    if (!subscribe_track(&fix, 1, "a", now)) { failures++; goto out; }
    set_phase(&fix, PHASE_PACE);
    MOQ_TEST_CHECK(write_obj(&fix, 0, 0x50, true, true, 1000) == MOQ_OK);
    MOQ_TEST_CHECK(write_obj(&fix, 1, 0x51, true, true, 1000) == MOQ_OK);
    drive(&fix, now);
    const tr_exp_t audio_head[] = { { 1, 0, 0, 0x51, 1 } };
    expect_from(&fix, 0, audio_head, 1, "resume-audio-head");
    expect_conserved(fix.s, 2, 1, 0, 1, 0, "resume-first-drop");

    MOQ_TEST_CHECK(write_obj(&fix, 0, 0x52, true, true, 2000) == MOQ_OK);
    drive(&fix, now);
    MOQ_TEST_CHECK(write_obj(&fix, 0, 0x53, false, false, 2100) == MOQ_OK);
    drive(&fix, now);
    expect_conserved(fix.s, 4, 1, 0, 3, 0, "resume-second-group-drop");

    set_phase(&fix, PHASE_ARM);
    if (!subscribe_track(&fix, 0, "v", now)) { failures++; goto out; }
    set_phase(&fix, PHASE_PACE);
    MOQ_TEST_CHECK(write_obj(&fix, 0, 0x54, true, false, 2200) == MOQ_OK);
    MOQ_TEST_CHECK(write_obj(&fix, 1, 0x55, true, false, 2200) == MOQ_OK);
    int from = fix.tr.n;
    drive(&fix, now);
    const tr_exp_t audio_tail[] = { { 1, 0, 1, 0x55, 5 } };
    expect_from(&fix, from, audio_tail, 1, "resume-skip-tail-audio-unaffected");
    expect_conserved(fix.s, 6, 2, 0, 4, 0, "resume-skip-tail-stats");
    from = fix.tr.n;
    drive(&fix, now);
    expect_from(&fix, from, NULL, 0, "resume-idle-no-duplicate");
    expect_conserved(fix.s, 6, 2, 0, 4, 0, "resume-idle-stats");

    if (!end_skipped) {
        MOQ_TEST_CHECK(write_obj(&fix, 0, 0x56, true, true, 3000) == MOQ_OK);
        MOQ_TEST_CHECK(write_obj(&fix, 0, 0x57, false, false, 3100) == MOQ_OK);
        from = fix.tr.n;
        drive(&fix, now);
        const tr_exp_t recovered[] = {
            { 0, 2, 0, 0x56, 6 }, { 0, 2, 1, 0x57, 7 }
        };
        expect_from(&fix, from, recovered, 2, "resume-exact-fresh-group");
        expect_conserved(fix.s, 8, 4, 0, 4, 0, "resume-recovered-stats");
    }
    MOQ_TEST_CHECK(moq_media_sender_end_track(fix.s, fix.t[0]) == MOQ_OK);
    from = fix.tr.n;
    drive(&fix, now);
    expect_from(&fix, from, NULL, 0, "resume-terminal-no-media");
    const stat_exp_t terminal[] = {
        { 0, end_skipped ? 0 : 3, 0, MOQ_OBJECT_END_OF_TRACK }
    };
    expect_status(&fix, terminal, 1, "resume-terminal-status");
    if (end_skipped) {
        const term_exp_t terms[] = { { 0, 0, 0, false, false } };
        expect_terminals(&fix, terms, 1, "resume-skipped-terminal");
    } else {
        const term_exp_t terms[] = {
            { 0, 2, 0, true, true }, { 0, 3, 0, false, false }
        };
        expect_terminals(&fix, terms, 2, "resume-recovered-terminal");
    }
    moq_media_sender_stats_t st;
    memset(&st, 0, sizeof(st));
    MOQ_TEST_CHECK(moq_media_sender_get_stats(fix.s, &st, sizeof(st)) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(st.groups_dropped, 2);
    MOQ_TEST_CHECK_EQ_U64(st.keyframes_dropped, 3);
    MOQ_TEST_CHECK_EQ_U64(st.groups_abandoned, 0);
    expect_conserved(fix.s, end_skipped ? 6 : 8, end_skipped ? 2 : 4,
                     0, 4, 0, "resume-final-stats");
out:
    fix_free(&fix);
    MOQ_TEST_CHECK(as.balance == 0);
    report(&fix, name, before);
}

static void test_demand_loss_after_emission(uint64_t version)
{
    int before = failures;
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    fix_t fix;
    const uint64_t now = 1000;
    const char *name = version == MOQ_VERSION_DRAFT_16
        ? "pacing_d16_demand_loss_after_emission"
        : "pacing_d18_demand_loss_after_emission";
    if (!fix_init_q2(&fix, &alloc, 0, true, now, 0, false,
                     false, NULL, -1, version, MOQ_MEDIA_SEND_BP_UNSET)) {
        failures++; goto out;
    }
    if (!subscribe_track(&fix, 0, "v", now)) { failures++; goto out; }
    set_phase(&fix, PHASE_PACE);
    MOQ_TEST_CHECK(write_obj(&fix, 0, 0x60, true, true, 1000) == MOQ_OK);
    drive(&fix, now);
    const tr_exp_t head[] = { { 0, 0, 0, 0x60, 0 } };
    expect_from(&fix, 0, head, 1, "demand-loss-head");
    int cancel_resets = g_cancel_resets;
    MOQ_TEST_CHECK(moq_session_unsubscribe(moq_simpair_server(fix.sp),
                                          fix.sub[0], now) == MOQ_OK);
    for (int i = 0; i < 8 &&
         moq_media_sender_track_has_subscriber(fix.s, fix.t[0]); i++)
        drive(&fix, now);
    MOQ_TEST_CHECK(!moq_media_sender_track_has_subscriber(fix.s, fix.t[0]));
    /* Both versions reset the departing subscription's open subgroup with
     * CANCELLED on the wire. */
    MOQ_TEST_CHECK_EQ_INT(g_cancel_resets - cancel_resets, 1);
    MOQ_TEST_CHECK(write_obj(&fix, 0, 0x61, false, false, 1100) == MOQ_OK);
    drive(&fix, now);
    expect_conserved(fix.s, 2, 1, 0, 1, 0, "demand-loss-drop");
    MOQ_TEST_CHECK_EQ_U64(moq_media_sender_test_reset_attempts(fix.s), 1);

    /* Before issuing the renewed SUBSCRIBE, declare the already published
     * largest location (group 0, object 0). */
    fix.sok_exp[fix.sok_exp_n].has_largest = true;
    set_phase(&fix, PHASE_ARM);
    if (!subscribe_track(&fix, 0, "v", now)) { failures++; goto out; }
    set_phase(&fix, PHASE_PACE);
    MOQ_TEST_CHECK(write_obj(&fix, 0, 0x62, false, false, 1200) == MOQ_OK);
    MOQ_TEST_CHECK(write_obj(&fix, 0, 0x63, true, true, 2000) == MOQ_OK);
    MOQ_TEST_CHECK(write_obj(&fix, 0, 0x64, false, false, 2100) == MOQ_OK);
    int from = fix.tr.n;
    drive(&fix, now);
    const tr_exp_t recovered[] = {
        { 0, 1, 0, 0x63, 3 }, { 0, 1, 1, 0x64, 4 }
    };
    expect_from(&fix, from, recovered, 2, "demand-loss-no-location-reuse");
    expect_conserved(fix.s, 5, 3, 0, 2, 0, "demand-loss-recovered");
    MOQ_TEST_CHECK(moq_media_sender_end_track(fix.s, fix.t[0]) == MOQ_OK);
    drive(&fix, now);
    const stat_exp_t terminal[] = { { 0, 2, 0, MOQ_OBJECT_END_OF_TRACK } };
    const term_exp_t terms[] = {
        { 0, 1, 0, true, true }, { 0, 2, 0, false, false }
    };
    expect_status(&fix, terminal, 1, "demand-loss-terminal");
    expect_terminals(&fix, terms, 2, "demand-loss-fin");
    /* That reset (asserted on the wire above) surfaces no SUBGROUP_RESET:
     * the subscriber deleted the subscription state when it cancelled,
     * stopped its streams and discards what still arrives for them. The
     * service discharges its local abandonment once, with no new reset. */
    expect_resets(&fix, NULL, 0, "demand-loss-departure");
    moq_media_sender_stats_t st;
    memset(&st, 0, sizeof(st));
    MOQ_TEST_CHECK(moq_media_sender_get_stats(fix.s, &st, sizeof(st)) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(st.groups_dropped, 1);
    MOQ_TEST_CHECK_EQ_U64(st.keyframes_dropped, 0);
    MOQ_TEST_CHECK_EQ_U64(st.groups_abandoned, 0);
    MOQ_TEST_CHECK_EQ_U64(moq_media_sender_test_reset_attempts(fix.s), 1);
    expect_conserved(fix.s, 5, 3, 0, 2, 0, "demand-loss-final");
out:
    fix_free(&fix);
    MOQ_TEST_CHECK(as.balance == 0);
    report(&fix, name, before);
}

/* ===================================================================== *
 *  Scenario 5 -- capacity/refusal recovery.
 *  A REAL session refusal at the head: the peer session's action ring is small,
 *  so moq_pub_write_object_ex returns MOQ_ERR_WOULD_BLOCK mid-burst. Proves the
 *  retained head resumes on later DRIVES alone -- no new media, no unrelated
 *  ingress -- with no loss, duplication, or reordering.
 * ===================================================================== */
static void test_refusal_recovery(void)
{
    int before = failures;
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    fix_t fix;
    uint64_t now = 1000;
    /* Small action ring on BOTH sessions: enough for the handshake/subscribe
     * exchanges (each runs to quiescence) but not for a whole burst in one
     * drain pass. */
    if (!fix_init(&fix, &alloc, 4, false, now)) { failures++; goto out; }
    if (!subscribe_track(&fix, 0, "v", now)) { failures++; goto out; }
    set_phase(&fix, PHASE_PACE);

    enum { N = 8 };
    tr_exp_t exp[N];
    for (int i = 0; i < N; i++) {
        exp[i].track = 0; exp[i].group = (uint64_t)i; exp[i].object = 0;
        exp[i].sentinel = (uint8_t)(0x80 + i);
        exp[i].enq_ord = i;
    }
    for (int i = 0; i < N; i++)
        MOQ_TEST_CHECK(write_obj(&fix, 0, (uint8_t)(0x80 + i), true, true,
                                 1000u * (uint64_t)(i + 1)) == MOQ_OK);
    expect_conserved(fix.s, N, 0, N, 0, N * PAYLOAD_LEN, "s5-queued");

    int from = fix.tr.n;
    /* First drive: the head refusal must leave work behind. */
    drive(&fix, now);
    moq_media_sender_stats_t st; memset(&st, 0, sizeof(st));
    MOQ_TEST_CHECK(moq_media_sender_get_stats(fix.s, &st, sizeof(st)) == MOQ_OK);
    /* Non-vacuity: the constrained ring really did refuse mid-burst. If this
     * fires the fixture no longer creates the state it claims to. */
    MOQ_TEST_CHECK(st.objects_sent > 0);
    MOQ_TEST_CHECK(st.objects_sent < N);
    MOQ_TEST_CHECK(st.objects_queued == (uint64_t)N - st.objects_sent);
    MOQ_TEST_CHECK_EQ_U64(st.objects_dropped, 0);
    uint64_t first_pass_sent = st.objects_sent;

    /* Recovery on drives alone. Bounded loop, then assert completion -- a
     * budget overrun fails on state, never on elapsed time. */
    int drives = 1;
    for (; drives < 32; drives++) {
        memset(&st, 0, sizeof(st));
        MOQ_TEST_CHECK(moq_media_sender_get_stats(fix.s, &st, sizeof(st))
                       == MOQ_OK);
        if (st.objects_queued == 0) break;
        drive(&fix, now);
    }
    expect_conserved(fix.s, N, N, 0, 0, 0, "s5-recovered");
    /* Exact FIFO identity across the whole multi-drive arc: no duplication,
     * no reordering, nothing lost at the refusal boundary. */
    expect_from(&fix, from, exp, N, "s5-arc");
    /* EXACT geometry of this arm, asserted literally rather than loosely.
     * With max_actions = 4, one object per group and no delivery inside a
     * drain pass, a new-group emission costs more session actions than the
     * ring can hold twice over, so exactly ONE object leaves per drive and the
     * eight-object burst takes exactly eight drives. These numbers are a
     * property of THIS arm (they would move if the per-object action cost of a
     * new-group emission changed); they are deterministic -- fixed seed, no
     * timing -- and each is killed by a restored mutant. */
    MOQ_TEST_CHECK_EQ_U64(first_pass_sent, 1);
    MOQ_TEST_CHECK_EQ_INT(drives, N);
    {
        int p0 = fix.tr.recs[from].pass;
        for (int i = 0; i < N; i++) {
            int in_pass = 0;
            for (int k = 0; k < N; k++)
                if (fix.tr.recs[from + k].pass == p0 + i) in_pass++;
            MOQ_TEST_CHECK_EQ_INT(in_pass, 1);
        }
        MOQ_TEST_CHECK_EQ_INT(fix.tr.recs[from + N - 1].pass, p0 + N - 1);
    }
    /* Eight one-object groups: exactly seven closed subgroups, groups 0..6. */
    {
        term_exp_t terms[N - 1];
        for (int i = 0; i < N - 1; i++) {
            terms[i].track = 0; terms[i].group = (uint64_t)i;
            terms[i].subgroup = 0; terms[i].end_of_group = true;
            terms[i].carries_media = true;
        }
        expect_terminals(&fix, terms, N - 1, "s5-terminals");
    }
    {
    }

out:
    fix_free(&fix);
    MOQ_TEST_CHECK(as.balance == 0);
    report(&fix, "pacing_refusal_recovery", before);
}

/* ===================================================================== *
 *  Refusal AFTER a skipped track.
 *  An undemanded track sits at the head and is skipped; the first eligible
 *  DEMANDED write then genuinely returns MOQ_ERR_WOULD_BLOCK because the peer
 *  session's action ring is small. Proves the refused demanded entry AND the
 *  held undemanded entries stay exact, and that later hooks recover with no
 *  loss, duplication, cross-track inversion, or busy-spin.
 * ===================================================================== */
static void test_refusal_after_skip(void)
{
    int before = failures;
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    fix_t fix;
    uint64_t now = 1000;
    enum { NA = 4 };            /* demanded audio objects */
    /* Same constrained arm as the signed refusal case: 1 object per hook. */
    if (!fix_init(&fix, &alloc, 4, false, now)) { failures++; goto out; }
    if (!subscribe_track(&fix, 1, "a", now)) { failures++; goto out; }
    set_phase(&fix, PHASE_PACE);
    MOQ_TEST_CHECK(!moq_media_sender_track_has_subscriber(fix.s, fix.t[0]));

    /* Head = undemanded video; behind it four demanded audio objects. */
    MOQ_TEST_CHECK(write_obj(&fix, 0, 0xa0, true, true, 1000) == MOQ_OK);
    tr_exp_t audio[NA];
    for (int i = 0; i < NA; i++) {
        MOQ_TEST_CHECK(write_obj(&fix, 1, (uint8_t)(0xa1 + i), true, true,
                                 2000u + 1000u * (uint64_t)i) == MOQ_OK);
        audio[i].track = 1; audio[i].group = (uint64_t)i; audio[i].object = 0;
        audio[i].sentinel = (uint8_t)(0xa1 + i); audio[i].enq_ord = i + 1;
    }
    expect_conserved(fix.s, NA + 1, 0, NA + 1, 0, (NA + 1) * PAYLOAD_LEN,
                     "skip-refuse-queued");
    MOQ_TEST_CHECK_EQ_INT(fix.enq_n, NA + 1);   /* non-vacuity */

    /* Hook 1: the undemanded head is skipped, ONE audio object goes out, and
     * the next audio write is genuinely refused -- so everything else stays
     * queued, the held video object included. */
    int from = fix.tr.n;
    drive(&fix, now);
    expect_conserved(fix.s, NA + 1, 1, NA, 0, NA * PAYLOAD_LEN,
                     "skip-refuse-first-hook");

    /* Recovery on hooks alone: no new media, no unrelated ingress. Bounded
     * budget, and the exact hook count is asserted afterwards so a spin or an
     * early give-up both fail on state rather than on elapsed time. */
    int hooks = 1;
    for (; hooks < 32; hooks++) {
        moq_media_sender_stats_t st; memset(&st, 0, sizeof(st));
        MOQ_TEST_CHECK(moq_media_sender_get_stats(fix.s, &st, sizeof(st))
                       == MOQ_OK);
        if (st.objects_sent >= NA) break;
        drive(&fix, now);
    }
    MOQ_TEST_CHECK_EQ_INT(hooks, NA);           /* one object per hook, no spin */
    /* The whole audio arc, exact identity and order, no duplication. */
    expect_from(&fix, from, audio, NA, "skip-refuse-audio-arc");
    /* The undemanded video object is STILL held, byte and identity exact. */
    expect_conserved(fix.s, NA + 1, NA, 1, 0, PAYLOAD_LEN,
                     "skip-refuse-video-still-held");
    /* No cross-track inversion: not one video record in the audio arc. */
    for (int i = from; i < fix.tr.n; i++)
        MOQ_TEST_CHECK_EQ_INT(fix.tr.recs[i].track, 1);

    /* Video demand arrives: the held object releases exactly once. */
    int from2 = fix.tr.n;
    const tr_exp_t video[1] = { { 0, 0, 0, 0xa0, 0 } };
    set_phase(&fix, PHASE_RELEASE);
    if (!subscribe_track(&fix, 0, "v", now)) { failures++; goto out; }
    set_phase(&fix, PHASE_PACE);
    drive(&fix, now);
    expect_from(&fix, from2, video, 1, "skip-refuse-video-release");
    expect_conserved(fix.s, NA + 1, NA + 1, 0, 0, 0, "skip-refuse-final");
    {   /* Audio wrote four one-object groups -> three closed; video wrote one
         * group, still open. */
        const term_exp_t terms[NA - 1] = {
            { 1, 0, 0, true, true }, { 1, 1, 0, true, true }, { 1, 2, 0, true, true } };
        expect_terminals(&fix, terms, NA - 1, "skip-refuse-terminals");
    }
    {
    }

out:
    fix_free(&fix);
    MOQ_TEST_CHECK(as.balance == 0);
    report(&fix, "pacing_refusal_after_skip", before);
}

/* ===================================================================== *
 *  End-marker safety.
 *  A later END_OF_TRACK sits BEHIND held media of the SAME track. Any skip or
 *  rotation must not let that terminal overtake its own track's held media:
 *  ending the publisher track first makes the held object unwritable
 *  (WRONG_STATE at the drain, hence fatal), so the held object would be lost.
 *  The END-safety assertions below are the permanent pin for that.
 * ===================================================================== */
static void test_end_marker_order(void)
{
    int before = failures;
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    fix_t fix;
    uint64_t now = 1000;
    if (!fix_init(&fix, &alloc, 0, false, now)) { failures++; goto out; }
    if (!subscribe_track(&fix, 1, "a", now)) { failures++; goto out; }
    set_phase(&fix, PHASE_PACE);

    /* [ video v0 (undemanded) ][ END_OF_TRACK(video) ][ audio a0 (demanded) ] */
    MOQ_TEST_CHECK(write_obj(&fix, 0, 0xb0, true, true, 1000) == MOQ_OK);
    MOQ_TEST_CHECK(moq_media_sender_end_track(fix.s, fix.t[0]) == MOQ_OK);
    MOQ_TEST_CHECK(write_obj(&fix, 1, 0xb1, true, true, 2000) == MOQ_OK);
    /* The terminal marker carries no payload, so the byte budget counts the
     * two media objects only. */
    /* The terminal marker occupies a physical ring slot but is NOT queued
     * media, so the public counters see two objects and their bytes only. */
    expect_conserved(fix.s, 2, 0, 2, 0, 2 * PAYLOAD_LEN, "endmark-queued");

    /* Contract: one hook delivers the demanded audio and holds the video
     * object AND its terminal. */
    const tr_exp_t audio[1] = { { 1, 0, 0, 0xb1, 1 } };
    int from = fix.tr.n;
    drive(&fix, now);
    expect_from(&fix, from, audio, 1, "endmark-audio-one-hook");
    MOQ_TEST_CHECK(!moq_media_sender_is_fatal(fix.s));

    /* PIN: video demand arrives and the HELD video object must still be
     * deliverable -- i.e. its END_OF_TRACK never overtook it. A terminal that
     * ran first would have ended the publisher track, so the held object could
     * no longer be written and the drain would turn the sender fatal. */
    int from2 = fix.tr.n;
    const tr_exp_t video[1] = { { 0, 0, 0, 0xb0, 0 } };
    set_phase(&fix, PHASE_RELEASE);
    if (!subscribe_track(&fix, 0, "v", now)) { failures++; goto out; }
    set_phase(&fix, PHASE_PACE);
    drive(&fix, now);
    expect_from(&fix, from2, video, 1, "endmark-video-not-overtaken");
    MOQ_TEST_CHECK(!moq_media_sender_is_fatal(fix.s));
    MOQ_TEST_CHECK_EQ_U64(moq_media_sender_fatal_code(fix.s), 0);
    expect_conserved(fix.s, 2, 2, 0, 0, 0, "endmark-final");
    {   /* Exactly one END_OF_TRACK status object, on the video track. */
        const stat_exp_t st[1] = { { 0, 1, 0, MOQ_OBJECT_END_OF_TRACK } };
        expect_status(&fix, st, 1, "endmark-status");
        /* Video's media group closes, and the FRESH subgroup carrying the
         * terminal closes behind it. */
        const term_exp_t terms[2] = { { 0, 0, 0, true, true }, { 0, 1, 0, false, false } };
        expect_terminals(&fix, terms, 2, "endmark-terminals");
    }

out:
    fix_free(&fix);
    MOQ_TEST_CHECK(as.balance == 0);
    report(&fix, "pacing_end_marker_order", before);
}

/* ===================================================================== *
 *  Terminal stats population, separately attributable.
 *  `objects_queued` belongs to the same public counter population as
 *  objects_written / objects_sent / objects_dropped / bytes_queued -- all of
 *  which are about MEDIA accepted by write(). A private zero-byte
 *  END_OF_TRACK work item must therefore not appear in it. The PHYSICAL ring
 *  slot it occupies must still count, which is observable as a refusal when
 *  the bounded ring is full.
 * ===================================================================== */
static void test_terminal_stats(void)
{
    int before = failures;
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    fix_t fix;
    uint64_t now = 1000;
    enum { RING = 3 };
    /* Bounded active queue + RETURN_WOULD_BLOCK so a full queue refuses
     * cleanly. */
    if (!fix_init_q(&fix, &alloc, 0, false, now, RING, false)) { failures++; goto out; }
    if (!subscribe_track(&fix, 0, "v", now)) { failures++; goto out; }
    if (!subscribe_track(&fix, 1, "a", now)) { failures++; goto out; }
    set_phase(&fix, PHASE_PACE);

    /* Two media objects: one physical slot each. */
    MOQ_TEST_CHECK(write_obj(&fix, 0, 0xc0, true, true, 1000) == MOQ_OK);
    MOQ_TEST_CHECK(write_obj(&fix, 1, 0xc1, true, true, 2000) == MOQ_OK);
    expect_conserved(fix.s, 2, 0, 2, 0, 2 * PAYLOAD_LEN, "termstat-two-media");

    /* The terminal marker takes the THIRD physical slot but is not media: the
     * public queued count and byte budget must both be unchanged. */
    MOQ_TEST_CHECK(moq_media_sender_end_track(fix.s, fix.t[0]) == MOQ_OK);
    expect_conserved(fix.s, 2, 0, 2, 0, 2 * PAYLOAD_LEN,
                     "termstat-marker-not-queued-media");

    /* The marker still occupies a slot against the ACTIVE QUEUE object bound:
     * that bound is now 3/3, so the next media write is refused. This is what
     * keeps the marker from simply dropping out of the accounting. */
    MOQ_TEST_CHECK(write_obj(&fix, 1, 0xc2, true, true, 3000) == MOQ_ERR_WOULD_BLOCK);
    expect_conserved(fix.s, 2, 0, 2, 0, 2 * PAYLOAD_LEN,
                     "termstat-after-refusal");

    /* Drain: both media objects emitted, the terminal consumed, nothing left. */
    const tr_exp_t objs[2] = { { 0, 0, 0, 0xc0, 0 }, { 1, 0, 0, 0xc1, 1 } };
    int from = fix.tr.n;
    drive(&fix, now);
    expect_from(&fix, from, objs, 2, "termstat-drained");
    expect_conserved(fix.s, 2, 2, 0, 0, 0, "termstat-final");
    {   /* Exactly one END_OF_TRACK status object, on the video track. */
        const stat_exp_t st[1] = { { 0, 1, 0, MOQ_OBJECT_END_OF_TRACK } };
        expect_status(&fix, st, 1, "termstat-status");
        const term_exp_t terms[2] = { { 0, 0, 0, true, true }, { 0, 1, 0, false, false } };
        expect_terminals(&fix, terms, 2, "termstat-terminals");
    }
    /* Space is available again once the ring drained. */
    MOQ_TEST_CHECK(write_obj(&fix, 1, 0xc3, true, true, 4000) == MOQ_OK);
    expect_conserved(fix.s, 3, 2, 1, 0, PAYLOAD_LEN, "termstat-reuse");

out:
    fix_free(&fix);
    MOQ_TEST_CHECK(as.balance == 0);
    report(&fix, "pacing_terminal_stats", before);
}

/* ===================================================================== *
 *  A real refusal stops the WHOLE hook.
 *  Lack of demand is track-local and skippable; MOQ_ERR_WOULD_BLOCK from a
 *  facade/session operation is SHARED output-capacity backpressure, so nothing
 *  further may be probed in that invocation.
 *
 *  The discriminator is an END_OF_TRACK marker for an UNDEMANDED track sitting
 *  behind the refused entry: moq_pub_end_track() treats "no subscriber" as
 *  local terminal success, so it needs no wire capacity and WOULD be processed
 *  by a scan that kept going after the refusal -- ending that track behind the
 *  application's back. Under the contract it is not touched, which is
 *  observable because the track can still be subscribed afterwards.
 * ===================================================================== */
static void test_refusal_stops_hook(void)
{
    int before = failures;
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    fix_t fix;
    uint64_t now = 1000;
    enum { NA = 4, RING = 5 };
    /* Bounded active queue so the slot the hook frees BEFORE its refusal is
     * observable: a write refused at the bound must succeed once that slot is
     * released. */
    if (!fix_init_q(&fix, &alloc, 4, false, now, RING, false)) { failures++; goto out; }
    if (!subscribe_track(&fix, 1, "a", now)) { failures++; goto out; }
    set_phase(&fix, PHASE_PACE);
    MOQ_TEST_CHECK(!moq_media_sender_track_has_subscriber(fix.s, fix.t[0]));

    /* Four demanded audio objects, then an END_OF_TRACK for the UNDEMANDED
     * video track. No video media is queued, so no per-track hold protects
     * that marker -- only the refusal contract does. */
    tr_exp_t audio[NA];
    for (int i = 0; i < NA; i++) {
        MOQ_TEST_CHECK(write_obj(&fix, 1, (uint8_t)(0xd0 + i), true, true,
                                 1000u + 1000u * (uint64_t)i) == MOQ_OK);
        audio[i].track = 1; audio[i].group = (uint64_t)i; audio[i].object = 0;
        audio[i].sentinel = (uint8_t)(0xd0 + i); audio[i].enq_ord = i;
    }
    MOQ_TEST_CHECK(moq_media_sender_end_track(fix.s, fix.t[0]) == MOQ_OK);
    expect_conserved(fix.s, NA, 0, NA, 0, NA * PAYLOAD_LEN, "stophook-queued");
    MOQ_TEST_CHECK_EQ_INT(fix.enq_n, NA);   /* non-vacuity */
    /* The ACTIVE QUEUE object bound is now reached (four media + the marker),
     * so a further write is refused. (The physical allocation is larger; the
     * bound that refuses here is queue_max_objects.) */
    MOQ_TEST_CHECK(write_obj(&fix, 1, 0xdf, true, true, 9000) ==
                   MOQ_ERR_WOULD_BLOCK);

    /* Hook 1: one audio object out, the next refused by shared capacity. */
    int from = fix.tr.n;
    drive(&fix, now);
    expect_conserved(fix.s, NA, 1, NA - 1, 0, (NA - 1) * PAYLOAD_LEN,
                     "stophook-first-hook");
    /* The slot freed BEFORE the refusal must be usable: the same write that
     * was refused above now succeeds. */
    MOQ_TEST_CHECK(write_obj(&fix, 1, 0xdf, true, true, 9000) == MOQ_OK);
    /* Nothing beyond the refusal was touched: the video track was NOT ended,
     * so it can still be subscribed. A scan that kept probing after the
     * refusal would have consumed its terminal here. */
    set_phase(&fix, PHASE_RELEASE);
    if (!subscribe_track(&fix, 0, "v", now)) { failures++; goto out; }
    set_phase(&fix, PHASE_PACE);

    /* Recovery on hooks alone, bounded, with the exact count asserted after. */
    int hooks = 1;
    for (; hooks < 32; hooks++) {
        moq_media_sender_stats_t st; memset(&st, 0, sizeof(st));
        MOQ_TEST_CHECK(moq_media_sender_get_stats(fix.s, &st, sizeof(st))
                       == MOQ_OK);
        if (st.objects_queued == 0) break;
        drive(&fix, now);
    }
    MOQ_TEST_CHECK(hooks < 32);              /* no spin, no give-up */
    /* objects_queued counts MEDIA, so reaching zero does not mean the ring is
     * empty: the terminal marker still occupies its physical slot. One more
     * hook flushes it. */
    drive(&fix, now);
    {   /* Five audio objects now: the four originals plus the one admitted
         * into the slot the hook freed before refusing. */
        tr_exp_t arc[NA + 1];
        for (int i = 0; i < NA; i++) arc[i] = audio[i];
        arc[NA].track = 1; arc[NA].group = (uint64_t)NA; arc[NA].object = 0;
        arc[NA].sentinel = 0xdf; arc[NA].enq_ord = NA;
        expect_from(&fix, from, arc, NA + 1, "stophook-audio-arc");
    }
    expect_conserved(fix.s, NA + 1, NA + 1, 0, 0, 0, "stophook-final");
    {   /* Three closed audio subgroups, plus the fresh subgroup the facade
         * opens and FINs to carry the video END_OF_TRACK status object -- that
         * one is not a group end, so its end_of_group bit is false. */
        const term_exp_t terms[NA + 1] = {
            { 1, 0, 0, true, true }, { 1, 1, 0, true, true }, { 1, 2, 0, true, true },
            { 1, 3, 0, true, true }, { 0, 0, 0, false, false } };
        expect_terminals(&fix, terms, NA + 1, "stophook-terminals");
    }
    {
        const stat_exp_t st1[1] = { { 0, 0, 0, MOQ_OBJECT_END_OF_TRACK } };
        expect_status(&fix, st1, 1, "stophook-status");
    }

out:
    fix_free(&fix);
    MOQ_TEST_CHECK(as.balance == 0);
    report(&fix, "pacing_refusal_stops_hook", before);
}

/* ===================================================================== *
 *  Dropped-group counting, separately attributable.
 *  `groups_dropped` counts DISTINCT (track, group) values from which at least
 *  one queued media object was discarded. Several objects of ONE group must
 *  therefore count once, and two distinct groups must count twice -- neither is
 *  "once per track per drain pass".
 * ===================================================================== */
static void test_drop_group_count(void)
{
    int before = failures;
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    fix_t fix;
    uint64_t now = 1000;
    if (!fix_init(&fix, &alloc, 0, true /* drop_without_demand */, now)) {
        failures++; goto out;
    }
    /* Audio demanded, video not: the video media is what gets dropped. */
    if (!subscribe_track(&fix, 1, "a", now)) { failures++; goto out; }
    set_phase(&fix, PHASE_PACE);
    MOQ_TEST_CHECK(!moq_media_sender_track_has_subscriber(fix.s, fix.t[0]));

    /* ONE video group carrying THREE objects: one sync anchor + two deltas. */
    MOQ_TEST_CHECK(write_obj(&fix, 0, 0xe0, true, true, 1000) == MOQ_OK);
    MOQ_TEST_CHECK(write_obj(&fix, 0, 0xe1, false, false, 2000) == MOQ_OK);
    MOQ_TEST_CHECK(write_obj(&fix, 0, 0xe2, false, false, 3000) == MOQ_OK);
    /* One demanded audio object so the pass has real work behind the drop. */
    MOQ_TEST_CHECK(write_obj(&fix, 1, 0xe3, true, true, 4000) == MOQ_OK);
    expect_conserved(fix.s, 4, 0, 4, 0, 4 * PAYLOAD_LEN, "dropcnt-queued");
    MOQ_TEST_CHECK_EQ_INT(fix.enq_n, 4);   /* non-vacuity */

    const tr_exp_t audio[1] = { { 1, 0, 0, 0xe3, 3 } };
    int from = fix.tr.n;
    drive(&fix, now);
    expect_from(&fix, from, audio, 1, "dropcnt-audio");
    expect_conserved(fix.s, 4, 1, 0, 3, 0, "dropcnt-stats");
    {   /* THREE objects of ONE group: one distinct group, one keyframe. */
        moq_media_sender_stats_t st; memset(&st, 0, sizeof(st));
        MOQ_TEST_CHECK(moq_media_sender_get_stats(fix.s, &st, sizeof(st))
                       == MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(st.objects_dropped, 3);
        MOQ_TEST_CHECK_EQ_U64(st.keyframes_dropped, 1);
        MOQ_TEST_CHECK_EQ_U64(st.groups_dropped, 1);
        MOQ_TEST_CHECK_EQ_U64(st.groups_abandoned, 0);
    }

    /* Now TWO distinct video groups, one object each, dropped in ONE pass:
     * groups_dropped must advance by exactly two. */
    MOQ_TEST_CHECK(write_obj(&fix, 0, 0xe4, true, true, 5000) == MOQ_OK);
    MOQ_TEST_CHECK(write_obj(&fix, 0, 0xe5, true, true, 6000) == MOQ_OK);
    MOQ_TEST_CHECK(write_obj(&fix, 1, 0xe6, true, true, 7000) == MOQ_OK);
    const tr_exp_t audio2[1] = { { 1, 1, 0, 0xe6, 6 } };
    int from2 = fix.tr.n;
    drive(&fix, now);
    expect_from(&fix, from2, audio2, 1, "dropcnt-audio2");
    expect_conserved(fix.s, 7, 2, 0, 5, 0, "dropcnt-stats2");
    {
        moq_media_sender_stats_t st; memset(&st, 0, sizeof(st));
        MOQ_TEST_CHECK(moq_media_sender_get_stats(fix.s, &st, sizeof(st))
                       == MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(st.objects_dropped, 5);
        MOQ_TEST_CHECK_EQ_U64(st.keyframes_dropped, 3);
        MOQ_TEST_CHECK_EQ_U64(st.groups_dropped, 3);   /* 1 + 2 */
        MOQ_TEST_CHECK_EQ_U64(st.groups_abandoned, 0);
    }
    {   /* Audio wrote two one-object groups: its first subgroup closed. */
        const term_exp_t terms[1] = { { 1, 0, 0, true, true } };
        expect_terminals(&fix, terms, 1, "dropcnt-terminals");
    }

out:
    fix_free(&fix);
    MOQ_TEST_CHECK(as.balance == 0);
    report(&fix, "pacing_drop_group_count", before);
}

/* ===================================================================== *
 *  end_track() honours the ACTIVE QUEUE bound.
 *  The public contract is MOQ_ERR_WOULD_BLOCK "when the send queue is
 *  momentarily full". The terminal marker is a zero-byte entry, so only the
 *  object bound applies -- but it is the CONFIGURED bound, not the larger
 *  physical allocation, and a refusal must not latch any end state.
 * ===================================================================== */
static void test_end_track_bound(void)
{
    int before = failures;
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    fix_t fix;
    uint64_t now = 1000;
    enum { QCAP = 3 };
    if (!fix_init_q(&fix, &alloc, 0, false, now, QCAP, false)) { failures++; goto out; }
    if (!subscribe_track(&fix, 0, "v", now)) { failures++; goto out; }
    if (!subscribe_track(&fix, 1, "a", now)) { failures++; goto out; }
    set_phase(&fix, PHASE_PACE);

    /* Fill the ACTIVE queue object bound: two media + one terminal marker. */
    MOQ_TEST_CHECK(write_obj(&fix, 0, 0xf0, true, true, 1000) == MOQ_OK);
    MOQ_TEST_CHECK(write_obj(&fix, 1, 0xf1, true, true, 2000) == MOQ_OK);
    MOQ_TEST_CHECK(moq_media_sender_end_track(fix.s, fix.t[0]) == MOQ_OK);
    expect_conserved(fix.s, 2, 0, 2, 0, 2 * PAYLOAD_LEN, "endbound-full");
    /* Media is refused at the bound. */
    MOQ_TEST_CHECK(write_obj(&fix, 1, 0xf2, true, true, 3000) ==
                   MOQ_ERR_WOULD_BLOCK);

    /* A SECOND track's terminal must be refused by the same bound -- terminal
     * work does not get to bypass queue_max_objects -- and the refusal must
     * latch nothing: the track is still writable afterwards. */
    MOQ_TEST_CHECK(moq_media_sender_end_track(fix.s, fix.t[1]) ==
                   MOQ_ERR_WOULD_BLOCK);
    expect_conserved(fix.s, 2, 0, 2, 0, 2 * PAYLOAD_LEN, "endbound-refused");

    /* Drain one slot's worth, then retry: the terminal is accepted exactly
     * once and completes. */
    const tr_exp_t objs[2] = { { 0, 0, 0, 0xf0, 0 }, { 1, 0, 0, 0xf1, 1 } };
    int from = fix.tr.n;
    drive(&fix, now);
    expect_from(&fix, from, objs, 2, "endbound-drained");
    MOQ_TEST_CHECK(moq_media_sender_end_track(fix.s, fix.t[1]) == MOQ_OK);
    /* Idempotent second call, still exactly one terminal. */
    MOQ_TEST_CHECK(moq_media_sender_end_track(fix.s, fix.t[1]) == MOQ_OK);
    drive(&fix, now);
    {
        const stat_exp_t st1[2] = { { 0, 1, 0, MOQ_OBJECT_END_OF_TRACK },
                                    { 1, 1, 0, MOQ_OBJECT_END_OF_TRACK } };
        expect_status(&fix, st1, 2, "endbound-status");
        /* Each track: its media group closes, then the fresh terminal one. */
        const term_exp_t terms[4] = { { 0, 0, 0, true, true }, { 0, 1, 0, false, false },
                                      { 1, 0, 0, true, true }, { 1, 1, 0, false, false } };
        expect_terminals(&fix, terms, 4, "endbound-terminals");
    }
    expect_conserved(fix.s, 2, 2, 0, 0, 0, "endbound-final");

out:
    fix_free(&fix);
    MOQ_TEST_CHECK(as.balance == 0);
    report(&fix, "pacing_end_track_bound", before);
}

/* ===================================================================== *
 *  Lifecycle removal / completion is counted.
 *  The public conservation identity is unconditional, so queued media
 *  discarded because its track was REMOVED or the sender was COMPLETED must be
 *  counted like any other discard. The private terminal marker is never
 *  counted, and this path arms no wire RESET -- removed-track teardown runs
 *  before the drain and owns its terminal semantics.
 *
 *  Two public arms: moq_media_sender_remove_track() and
 *  moq_media_sender_complete(). complete() is a graceful end, not a removal:
 *  accepted media is sent, every track then ends after its own media, and
 *  only then are the tracks removed -- so nothing is discarded, and its
 *  expectations are declared separately.
 * ===================================================================== */
static void test_removal_accounting(bool via_complete)
{
    int before = failures;
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    fix_t fix;
    uint64_t now = 1000;
    if (!fix_init(&fix, &alloc, 0, false, now)) { failures++; goto out; }
    /* BOTH tracks demanded, so nothing here can be confused with a no-demand
     * drop: every discard below is a lifecycle discard. */
    if (!subscribe_track(&fix, 0, "v", now)) { failures++; goto out; }
    if (!subscribe_track(&fix, 1, "a", now)) { failures++; goto out; }
    set_phase(&fix, PHASE_PACE);
    MOQ_TEST_CHECK(moq_media_sender_track_has_subscriber(fix.s, fix.t[0]));
    MOQ_TEST_CHECK(moq_media_sender_track_has_subscriber(fix.s, fix.t[1]));

    /* Video: TWO groups -- g0 with a sync anchor plus a delta, then g1 with a
     * sync anchor. Audio: one object. Nothing is driven yet, so no wire
     * subgroup is open for either track. */
    MOQ_TEST_CHECK(write_obj(&fix, 0, 0x20, true,  true,  1000) == MOQ_OK);
    MOQ_TEST_CHECK(write_obj(&fix, 0, 0x21, false, false, 2000) == MOQ_OK);
    MOQ_TEST_CHECK(write_obj(&fix, 0, 0x22, true,  true,  3000) == MOQ_OK);
    MOQ_TEST_CHECK(write_obj(&fix, 1, 0x23, true,  true,  4000) == MOQ_OK);
    expect_conserved(fix.s, 4, 0, 4, 0, 4 * PAYLOAD_LEN, "rm-queued");
    MOQ_TEST_CHECK_EQ_INT(fix.enq_n, 4);   /* non-vacuity */

    /* Queue a terminal marker for the video track BEFORE tearing it down, so
     * the removal path has a private zero-byte entry to discard. It must be
     * released without ever being counted as media. */
    MOQ_TEST_CHECK(moq_media_sender_end_track(fix.s, fix.t[0]) == MOQ_OK);
    expect_conserved(fix.s, 4, 0, 4, 0, 4 * PAYLOAD_LEN, "rm-queued-with-marker");

    if (via_complete)
        MOQ_TEST_CHECK(moq_media_sender_complete(fix.s) == MOQ_OK);
    else
        MOQ_TEST_CHECK(moq_media_sender_remove_track(fix.s, fix.t[0]) == MOQ_OK);

    int from = fix.tr.n;
    drive(&fix, now);
    /* Completion is staged over hooks: the media and each track's end, then
     * the finalize, then each removed track's teardown. */
    for (int i = 0; via_complete && i < 4; i++) drive(&fix, now);

    /* Removal's END_OF_TRACK opens a real stream. The first hook queues its
     * header, SimPair observes admission, and a later hook can commit DONE.
     * Keep the same time and collect all output for the exact checks below. */
    for (unsigned i = 0; !via_complete && i < 16 && fix.done_n == 0; i++)
        drive(&fix, now);

    if (!via_complete) {
        /* The demanded AUDIO track is unaffected: its object still leaves. */
        const tr_exp_t audio[1] = { { 1, 0, 0, 0x23, 3 } };
        expect_from(&fix, from, audio, 1, "rm-audio-unaffected");
        /* Video's three queued objects are all counted as discards, from TWO
         * distinct groups, and conservation holds: 4 == 1 + 0 + 3. */
        expect_conserved(fix.s, 4, 1, 0, 3, 0, "rm-stats");
        moq_media_sender_stats_t st; memset(&st, 0, sizeof(st));
        MOQ_TEST_CHECK(moq_media_sender_get_stats(fix.s, &st, sizeof(st)) == MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(st.objects_dropped, 3);
        MOQ_TEST_CHECK_EQ_U64(st.keyframes_dropped, 2);
        MOQ_TEST_CHECK_EQ_U64(st.groups_dropped, 2);
        /* No wire subgroup was ever open for video, and this path arms no
         * RESET, so nothing was abandoned. */
        MOQ_TEST_CHECK_EQ_U64(st.groups_abandoned, 0);
        /* Exactly the removed track's subscription ended, with the declared
         * status -- not whatever the event happened to carry. */
        const done_exp_t dexp[1] = {
            { fix.sub[0]._opaque, MOQ_PUB_DONE_TRACK_ENDED } };
        expect_done(&fix, dexp, 1, "rm-done");
        /* Video emitted no media, so its terminal opens and closes a single
         * subgroup at group 0; audio's one group is still open. */
        const stat_exp_t sx[1] = { { 0, 0, 0, MOQ_OBJECT_END_OF_TRACK } };
        expect_status(&fix, sx, 1, "rm-status");
        const term_exp_t terms[1] = { { 0, 0, 0, false, false } };
        expect_terminals(&fix, terms, 1, "rm-terminals");
    } else {
        /* complete() keeps the accepted media: all four objects leave in
         * enqueue order, nothing is discarded, and conservation holds:
         * 4 == 4 + 0 + 0. */
        const tr_exp_t media[4] = { { 0, 0, 0, 0x20, 0 }, { 0, 0, 1, 0x21, 1 },
                                    { 0, 1, 0, 0x22, 2 }, { 1, 0, 0, 0x23, 3 } };
        expect_from(&fix, from, media, 4, "complete-media-sent");
        expect_conserved(fix.s, 4, 4, 0, 0, 0, "complete-stats");
        moq_media_sender_stats_t st; memset(&st, 0, sizeof(st));
        MOQ_TEST_CHECK(moq_media_sender_get_stats(fix.s, &st, sizeof(st)) == MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(st.objects_dropped, 0);
        MOQ_TEST_CHECK_EQ_U64(st.keyframes_dropped, 0);
        MOQ_TEST_CHECK_EQ_U64(st.groups_dropped, 0);
        MOQ_TEST_CHECK_EQ_U64(st.groups_abandoned, 0);
        /* Each track ends after its own media: video by its end_track
         * marker at {its last group + 1, 0}, audio by the end complete()
         * owes it. */
        const stat_exp_t sx[2] = { { 0, 2, 0, MOQ_OBJECT_END_OF_TRACK },
                                   { 1, 1, 0, MOQ_OBJECT_END_OF_TRACK } };
        expect_status(&fix, sx, 2, "complete-status");
        /* Every media subgroup closes cleanly; each end has a terminal-only
         * subgroup of its own. */
        const term_exp_t terms[5] = {
            { 0, 0, 0, true, true }, { 0, 1, 0, true, true }, { 0, 2, 0, false, false },
            { 1, 0, 0, true, true }, { 1, 1, 0, false, false } };
        expect_terminals(&fix, terms, 5, "complete-terminals");
        /* The removal that follows ends both subscriptions, exactly once
         * each, with the declared status. */
        const done_exp_t dexp[2] = {
            { fix.sub[0]._opaque, MOQ_PUB_DONE_TRACK_ENDED },
            { fix.sub[1]._opaque, MOQ_PUB_DONE_TRACK_ENDED } };
        expect_done(&fix, dexp, 2, "complete-done");
    }

    /* A further hook adds no media and mutates no counter. */
    int from2 = fix.tr.n;
    drive(&fix, now);
    expect_from(&fix, from2, NULL, 0, "rm-idle");

out:
    fix_free(&fix);
    MOQ_TEST_CHECK(as.balance == 0);
    report(&fix, via_complete ? "pacing_completion_accounting"
                        : "pacing_removal_accounting", before);
}

/* ===================================================================== *
 *  Distinct-group accounting is CUMULATIVE, not per drain pass.
 *  Two objects of the SAME group, dropped in SEPARATE hooks, are two object
 *  discards but ONE dropped group. Both are queued before the first hook; a
 *  shared-capacity refusal on the demanded track stops that hook between
 *  them, so the second is discarded only by the next hook.
 * ===================================================================== */
static void test_drop_group_across_passes(void)
{
    int before = failures;
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    fix_t fix;
    uint64_t now = 1000;
    /* drop_without_demand, and the constrained action ring that lets one
     * new-group emission out per hook. */
    if (!fix_init(&fix, &alloc, 4, true /* drop_without_demand */, now)) {
        failures++; goto out;
    }
    /* Audio demanded so the pass has real work; video undemanded so its media
     * is what gets discarded. */
    if (!subscribe_track(&fix, 1, "a", now)) { failures++; goto out; }
    set_phase(&fix, PHASE_PACE);
    MOQ_TEST_CHECK(!moq_media_sender_track_has_subscriber(fix.s, fix.t[0]));

    /* Queue order: audio g0, video g0 anchor, audio g1, video g0 delta. */
    MOQ_TEST_CHECK(write_obj(&fix, 1, 0x30, true, true, 1000) == MOQ_OK);
    MOQ_TEST_CHECK(write_obj(&fix, 0, 0x31, true, true, 1500) == MOQ_OK);
    MOQ_TEST_CHECK(write_obj(&fix, 1, 0x32, true, true, 2000) == MOQ_OK);
    MOQ_TEST_CHECK(write_obj(&fix, 0, 0x33, false, false, 2500) == MOQ_OK);

    /* PASS 1: audio g0 leaves, the video anchor is discarded, audio g1 is
     * refused by shared capacity, and the video delta stays queued. */
    const tr_exp_t audio1[1] = { { 1, 0, 0, 0x30, 0 } };
    int from = fix.tr.n;
    int pass1 = fix.pass;
    drive(&fix, now);
    MOQ_TEST_CHECK_EQ_INT(fix.pass, pass1 + 1);
    expect_from(&fix, from, audio1, 1, "xpass-audio1");
    expect_conserved(fix.s, 4, 1, 2, 1, 2 * PAYLOAD_LEN, "xpass-stats1");
    {
        moq_media_sender_stats_t st; memset(&st, 0, sizeof(st));
        MOQ_TEST_CHECK(moq_media_sender_get_stats(fix.s, &st, sizeof(st)) == MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(st.objects_dropped, 1);
        MOQ_TEST_CHECK_EQ_U64(st.keyframes_dropped, 1);
        MOQ_TEST_CHECK_EQ_U64(st.groups_dropped, 1);
    }

    /* PASS 2, a SEPARATE hook: audio g1 leaves and the queued video delta of
     * the SAME group 0 is discarded. */
    const tr_exp_t audio2[1] = { { 1, 1, 0, 0x32, 2 } };
    int from2 = fix.tr.n;
    int pass2 = fix.pass;
    drive(&fix, now);
    MOQ_TEST_CHECK_EQ_INT(fix.pass, pass2 + 1);   /* a distinct, later hook */
    expect_from(&fix, from2, audio2, 1, "xpass-audio2");
    expect_conserved(fix.s, 4, 2, 0, 2, 0, "xpass-stats2");
    {
        moq_media_sender_stats_t st; memset(&st, 0, sizeof(st));
        MOQ_TEST_CHECK(moq_media_sender_get_stats(fix.s, &st, sizeof(st)) == MOQ_OK);
        /* TWO object discards ... */
        MOQ_TEST_CHECK_EQ_U64(st.objects_dropped, 2);
        MOQ_TEST_CHECK_EQ_U64(st.keyframes_dropped, 1);  /* only the anchor */
        /* ... but ONE dropped group: the tuple (video, group 0) is the same
         * across the two passes, and the cumulative counter must not
         * re-count it. */
        MOQ_TEST_CHECK_EQ_U64(st.groups_dropped, 1);
        MOQ_TEST_CHECK_EQ_U64(st.groups_abandoned, 0);
    }
    {   /* Audio's first of two groups closed. */
        const term_exp_t terms[1] = { { 1, 0, 0, true, true } };
        expect_terminals(&fix, terms, 1, "xpass-terminals");
    }

out:
    fix_free(&fix);
    MOQ_TEST_CHECK(as.balance == 0);
    report(&fix, "pacing_drop_group_across_passes", before);
}

/* ===================================================================== *
 *  Capacity-policy eviction uses the SAME accounting.
 *  A DROP policy evicting a whole group to make room must produce exactly the
 *  same byte/object/keyframe/distinct-group facts as a no-demand or lifecycle
 *  discard -- no separate bookkeeping.
 * ===================================================================== */
static void test_evict_accounting(void)
{
    int before = failures;
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    fix_t fix;
    uint64_t now = 1000;
    enum { QCAP = 2 };
    /* Bounded active queue with the LIVE drop policy, and nothing driven, so
     * the third write must evict to make room. */
    if (!fix_init_drop(&fix, &alloc, now, QCAP)) { failures++; goto out; }
    if (!subscribe_track(&fix, 0, "v", now)) { failures++; goto out; }
    set_phase(&fix, PHASE_PACE);

    MOQ_TEST_CHECK(write_obj(&fix, 0, 0x40, true, true, 1000) == MOQ_OK);
    MOQ_TEST_CHECK(write_obj(&fix, 0, 0x41, true, true, 2000) == MOQ_OK);
    expect_conserved(fix.s, 2, 0, 2, 0, 2 * PAYLOAD_LEN, "evict-full");

    /* Third fresh anchor: the oldest group (video g0) is evicted. */
    MOQ_TEST_CHECK(write_obj(&fix, 0, 0x42, true, true, 3000) == MOQ_OK);
    expect_conserved(fix.s, 3, 0, 2, 1, 2 * PAYLOAD_LEN, "evict-one");
    {
        moq_media_sender_stats_t st; memset(&st, 0, sizeof(st));
        MOQ_TEST_CHECK(moq_media_sender_get_stats(fix.s, &st, sizeof(st)) == MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(st.objects_dropped, 1);
        MOQ_TEST_CHECK_EQ_U64(st.keyframes_dropped, 1);
        MOQ_TEST_CHECK_EQ_U64(st.groups_dropped, 1);
        MOQ_TEST_CHECK_EQ_U64(st.groups_abandoned, 0);
    }

    /* A fourth evicts video g1: a DISTINCT group, so the group count advances
     * by exactly one again. */
    MOQ_TEST_CHECK(write_obj(&fix, 0, 0x43, true, true, 4000) == MOQ_OK);
    expect_conserved(fix.s, 4, 0, 2, 2, 2 * PAYLOAD_LEN, "evict-two");
    {
        moq_media_sender_stats_t st; memset(&st, 0, sizeof(st));
        MOQ_TEST_CHECK(moq_media_sender_get_stats(fix.s, &st, sizeof(st)) == MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(st.objects_dropped, 2);
        MOQ_TEST_CHECK_EQ_U64(st.keyframes_dropped, 2);
        MOQ_TEST_CHECK_EQ_U64(st.groups_dropped, 2);
        MOQ_TEST_CHECK_EQ_U64(st.groups_abandoned, 0);
    }

    /* The two survivors drain in order, byte and ordinal exact. */
    const tr_exp_t rest[2] = { { 0, 2, 0, 0x42, 2 }, { 0, 3, 0, 0x43, 3 } };
    int from = fix.tr.n;
    drive(&fix, now);
    expect_from(&fix, from, rest, 2, "evict-drained");
    expect_conserved(fix.s, 4, 2, 0, 2, 0, "evict-final");
    {   /* The first survivor's group closed when the second opened. */
        const term_exp_t terms[1] = { { 0, 2, 0, true, true } };
        expect_terminals(&fix, terms, 1, "evict-terminals");
    }

out:
    fix_free(&fix);
    MOQ_TEST_CHECK(as.balance == 0);
    report(&fix, "pacing_evict_accounting", before);
}

/* ===================================================================== *
 *  Eviction / reset interaction. Shared geometry for both rows.
 *
 *  Builds the cross-path state: a DEMANDED video track
 *  whose sync anchor has already been EMITTED (so the group and its wire
 *  subgroup are open, and no anchor of that group is queued any more), then a
 *  delta of that same group queued and EVICTED by capacity pressure from the
 *  other track -- which also arms the service's pending_reset and closes the
 *  group for writes, so a SECOND delta of it is refused.
 *
 *  Returns false and reports if any step of the geometry did not actually
 *  happen, so neither row can pass or fail vacuously.
 * ===================================================================== */
typedef struct {
    uint64_t dropped, keyframes, groups, abandoned;
} drop_snap_t;

static void drop_snap(moq_media_sender_t *s, drop_snap_t *out)
{
    moq_media_sender_stats_t st; memset(&st, 0, sizeof(st));
    memset(out, 0, sizeof(*out));
    if (moq_media_sender_get_stats(s, &st, sizeof(st)) != MOQ_OK) return;
    out->dropped = st.objects_dropped;
    out->keyframes = st.keyframes_dropped;
    out->groups = st.groups_dropped;
    out->abandoned = st.groups_abandoned;
}

static bool evict_geometry(fix_t *fix, uint64_t now)
{
    if (!subscribe_track(fix, 0, "v", now)) return false;
    if (!subscribe_track(fix, 1, "a", now)) return false;
    set_phase(fix, PHASE_PACE);

    /* 1. Emit video group 0's sync anchor through a real drive. starts_group
     *    with is_sync and NOT ends_group, so the group -- and its wire
     *    subgroup -- stay open with no anchor left in the queue. */
    const tr_exp_t anchor[1] = { { 0, 0, 0, 0x50, 0 } };
    int from = fix->tr.n;
    if (write_obj(fix, 0, 0x50, true, true, 1000) != MOQ_OK) return false;
    drive(fix, now);
    expect_from(fix, from, anchor, 1, "evx-anchor-emitted");
    expect_conserved(fix->s, 1, 1, 0, 0, 0, "evx-anchor-stats");
    if (fix->tr.n - from != 1) return false;

    /* 2. A video DELTA of that same still-open group: no anchor of it is
     *    queued any more, which is exactly what makes it evictable. */
    if (write_obj(fix, 0, 0x51, false, false, 2000) != MOQ_OK) return false;
    expect_conserved(fix->s, 2, 1, 1, 0, PAYLOAD_LEN, "evx-delta-queued");

    /* 3. Pressure from the OTHER track: one audio object fills the bound, and
     *    a second forces the capacity path to evict the video delta. */
    if (write_obj(fix, 1, 0x52, true, true, 3000) != MOQ_OK) return false;
    {
        drop_snap_t pre; drop_snap(fix->s, &pre);
        if (pre.dropped != 0) return false;            /* nothing dropped yet */
    }
    if (write_obj(fix, 1, 0x53, true, true, 4000) != MOQ_OK) return false;

    drop_snap_t after_ev; drop_snap(fix->s, &after_ev);
    /* The eviction REALLY happened and took exactly the video delta: one
     * object, no keyframe (it is a delta), one group. */
    MOQ_TEST_CHECK_EQ_U64(after_ev.dropped, 1);
    MOQ_TEST_CHECK_EQ_U64(after_ev.keyframes, 0);
    MOQ_TEST_CHECK_EQ_U64(after_ev.groups, 1);
    if (after_ev.dropped != 1) return false;
    expect_conserved(fix->s, 4, 1, 2, 1, 2 * PAYLOAD_LEN, "evx-after-eviction");

    /* 4. Drain the audio pressure so step 5 needs no further eviction. The
     *    WIRE subgroup video group 0 was emitted on is driven through RESET by
     *    the post-pass sweep, which needs no queued video entry to hang off.
     *    The obligation is therefore DISCHARGED here, not carried untouched
     *    into step 5. */
    const tr_exp_t audio[2] = { { 1, 0, 0, 0x52, 2 }, { 1, 1, 0, 0x53, 3 } };
    int froma = fix->tr.n;
    drive(fix, now);
    expect_from(fix, froma, audio, 2, "evx-audio-drained");
    expect_conserved(fix->s, 4, 3, 0, 1, 0, "evx-audio-stats");
    if (fix->tr.n - froma != 2) return false;

    /* 5. Group 0 lost queued media, so a SECOND video delta of it is
     *    refused, taking nothing. */
    if (write_obj(fix, 0, 0x54, false, false, 5000) != MOQ_ERR_WOULD_BLOCK)
        return false;
    expect_conserved(fix->s, 4, 3, 0, 1, 0, "evx-delta2-refused");
    return true;
}

/* Same shape as evict_geometry, but on a constrained action ring: the drive
 * that would discharge the abandon obligation is itself refused by real shared
 * capacity, so the obligation is left armed and RETRYABLE. Counts are not
 * asserted here -- the row that uses it asserts the obligation state. */
static bool evict_geometry_noflush(fix_t *fix, uint64_t now)
{
    if (!subscribe_track(fix, 0, "v", now)) return false;
    if (!subscribe_track(fix, 1, "a", now)) return false;
    set_phase(fix, PHASE_PACE);

    /* Anchor emitted, group and wire subgroup left open. */
    int from = fix->tr.n;
    if (write_obj(fix, 0, 0x60, true, true, 1000) != MOQ_OK) return false;
    for (int i = 0; i < 6 && fix->tr.n == from; i++) drive(fix, now);
    if (fix->tr.n - from != 1) return false;
    if (fix->tr.recs[from].track != 0 || fix->tr.recs[from].group != 0)
        return false;

    /* Delta of that same open group, then pressure that evicts it. */
    if (write_obj(fix, 0, 0x61, false, false, 2000) != MOQ_OK) return false;
    if (write_obj(fix, 1, 0x62, true, true, 3000) != MOQ_OK) return false;
    drop_snap_t pre; drop_snap(fix->s, &pre);
    if (pre.dropped != 0) return false;
    if (write_obj(fix, 1, 0x63, true, true, 4000) != MOQ_OK) return false;
    drop_snap_t post; drop_snap(fix->s, &post);
    /* The eviction really happened and armed the obligation. */
    MOQ_TEST_CHECK_EQ_U64(post.dropped, 1);
    MOQ_TEST_CHECK_EQ_U64(post.groups, 1);
    MOQ_TEST_CHECK_EQ_U64(post.abandoned, 0);
    if (post.dropped != 1) return false;

    /* ONE hook: the media pass exhausts the constrained action ring, so the
     * abandon attempt that follows is refused by shared capacity. Proven, not
     * assumed: media is still queued afterwards, which is what a shared
     * capacity refusal looks like -- the reset was not skipped for want of a
     * publisher track. */
    drive(fix, now);
    {
        moq_media_sender_stats_t st; memset(&st, 0, sizeof(st));
        if (moq_media_sender_get_stats(fix->s, &st, sizeof(st)) != MOQ_OK)
            return false;
        MOQ_TEST_CHECK(st.objects_queued > 0);
        if (st.objects_queued == 0) return false;
    }
    return true;
}

/* ===================================================================== *
 *  An open Group that lost queued media is closed for writes.
 *
 *  Video group 0's sync anchor is emitted, so the group and its wire subgroup
 *  are open with no anchor of it left in the queue; a delta of it is then
 *  evicted by pressure from the other track, and the wire subgroup is reset.
 *  A further delta for that group can no longer be delivered as a
 *  continuation of it: it is refused with WOULD_BLOCK and no ownership taken,
 *  no Location of group 0 is emitted a second time, and the next sync point
 *  opens group 1 at object 0.
 * ===================================================================== */
static void test_evicted_open_group_closed(void)
{
    int before = failures;
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    fix_t fix;
    uint64_t now = 1000;
    enum { QCAP = 2 };
    if (!fix_init_drop(&fix, &alloc, now, QCAP)) { failures++; goto out; }
    if (!subscribe_track(&fix, 0, "v", now)) { failures++; goto out; }
    if (!subscribe_track(&fix, 1, "a", now)) { failures++; goto out; }
    set_phase(&fix, PHASE_PACE);

    /* 1. The anchor of video group 0 is emitted; the group stays open. */
    const tr_exp_t anchor[1] = { { 0, 0, 0, 0x80, 0 } };
    int from = fix.tr.n;
    MOQ_TEST_CHECK(write_obj(&fix, 0, 0x80, true, true, 1000) == MOQ_OK);
    drive(&fix, now);
    expect_from(&fix, from, anchor, 1, "evopen-anchor");

    /* 2-3. A delta of that group, evicted by audio pressure. */
    MOQ_TEST_CHECK(write_obj(&fix, 0, 0x81, false, false, 2000) == MOQ_OK);
    MOQ_TEST_CHECK(write_obj(&fix, 1, 0x82, true, true, 3000) == MOQ_OK);
    MOQ_TEST_CHECK(write_obj(&fix, 1, 0x83, true, true, 4000) == MOQ_OK);
    {
        drop_snap_t st; drop_snap(fix.s, &st);
        MOQ_TEST_CHECK_EQ_U64(st.dropped, 1);
        MOQ_TEST_CHECK_EQ_U64(st.keyframes, 0);
    }

    /* 4. Audio drains and the abandoned video subgroup is reset. */
    const tr_exp_t audio[2] = { { 1, 0, 0, 0x82, 2 }, { 1, 1, 0, 0x83, 3 } };
    int froma = fix.tr.n;
    drive(&fix, now);
    expect_from(&fix, froma, audio, 2, "evopen-audio");
    MOQ_TEST_CHECK_EQ_INT(fix.reset_n, 1);

    /* 5. Another delta for group 0 is refused, with no ownership taken. */
    MOQ_TEST_CHECK(write_obj(&fix, 0, 0x84, false, false, 5000) ==
                   MOQ_ERR_WOULD_BLOCK);
    int fromd = fix.tr.n;
    drive(&fix, now);
    MOQ_TEST_CHECK_EQ_INT(fix.tr.n - fromd, 0);
    MOQ_TEST_CHECK(!moq_media_sender_is_fatal(fix.s));

    /* 6. The next sync point opens group 1 at object 0. */
    const tr_exp_t next[1] = { { 0, 1, 0, 0x85, 4 } };
    int fromn = fix.tr.n;
    MOQ_TEST_CHECK(write_obj(&fix, 0, 0x85, true, false, 6000) == MOQ_OK);
    drive(&fix, now);
    expect_from(&fix, fromn, next, 1, "evopen-next-group");

    /* No Location of video group 0 appeared twice. */
    {
        int g0 = 0;
        for (int i = 0; i < fix.tr.n; i++)
            if (fix.tr.recs[i].track == 0 && fix.tr.recs[i].group == 0) g0++;
        MOQ_TEST_CHECK_EQ_INT(g0, 1);
    }
    expect_conserved(fix.s, 5, 4, 0, 1, 0, "evopen-final");
    {
        const reset_exp_t rexp[1] = {
            { 0, 0, 0, 0x10, true, .sub_opaque = 0, .pub_opaque = 0 } };
        expect_resets(&fix, rexp, 1, "evopen-reset");
        const term_exp_t terms[1] = { { 1, 0, 0, true, true } };
        expect_terminals(&fix, terms, 1, "evopen-terminals");
    }

out:
    fix_free(&fix);
    MOQ_TEST_CHECK(as.balance == 0);
    report(&fix, "pacing_evicted_open_group_closed", before);
}

/* ===================================================================== *
 *  A continuation never evicts the Group it continues.
 *
 *  Video group 0's anchor is emitted and a delta of it is queued; audio fills
 *  the bound. A second delta of video group 0 needs room, but the only video
 *  entry is the queued tail of the very Group it extends: evicting it would
 *  leave the incoming delta joining a Group that lost media. The write is
 *  refused with WOULD_BLOCK and nothing is evicted; the queued delta then
 *  drains at its own Location.
 * ===================================================================== */
static void test_continuation_keeps_own_group(void)
{
    int before = failures;
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    fix_t fix;
    uint64_t now = 1000;
    enum { QCAP = 2 };
    if (!fix_init_drop(&fix, &alloc, now, QCAP)) { failures++; goto out; }
    if (!subscribe_track(&fix, 0, "v", now)) { failures++; goto out; }
    if (!subscribe_track(&fix, 1, "a", now)) { failures++; goto out; }
    set_phase(&fix, PHASE_PACE);

    const tr_exp_t anchor[1] = { { 0, 0, 0, 0x88, 0 } };
    int from = fix.tr.n;
    MOQ_TEST_CHECK(write_obj(&fix, 0, 0x88, true, true, 1000) == MOQ_OK);
    drive(&fix, now);
    expect_from(&fix, from, anchor, 1, "keep-anchor");

    MOQ_TEST_CHECK(write_obj(&fix, 0, 0x89, false, false, 2000) == MOQ_OK);
    MOQ_TEST_CHECK(write_obj(&fix, 1, 0x8a, true, true, 3000) == MOQ_OK);
    MOQ_TEST_CHECK(write_obj(&fix, 0, 0x8b, false, false, 4000) ==
                   MOQ_ERR_WOULD_BLOCK);
    expect_conserved(fix.s, 3, 1, 2, 0, 2 * PAYLOAD_LEN, "keep-refused");

    const tr_exp_t rest[2] = { { 0, 0, 1, 0x89, 1 }, { 1, 0, 0, 0x8a, 2 } };
    int from2 = fix.tr.n;
    drive(&fix, now);
    expect_from(&fix, from2, rest, 2, "keep-drained");
    expect_conserved(fix.s, 3, 3, 0, 0, 0, "keep-final");
    MOQ_TEST_CHECK_EQ_INT(fix.reset_n, 0);

out:
    fix_free(&fix);
    MOQ_TEST_CHECK(as.balance == 0);
    report(&fix, "pacing_continuation_keeps_own_group", before);
}

/* Cross-path cumulative group identity: two discards of the same (video,
 * group 0) by two different paths -- the no-demand drop in one hook, then
 * lifecycle removal -- are two object discards but ONE group. A shared-capacity
 * refusal on the demanded track stops the first hook between them. */
static void test_evict_crosspath_group(void)
{
    int before = failures;
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    fix_t fix;
    uint64_t now = 1000;
    if (!fix_init(&fix, &alloc, 4, true /* drop_without_demand */, now)) {
        failures++; goto out;
    }
    if (!subscribe_track(&fix, 1, "a", now)) { failures++; goto out; }
    set_phase(&fix, PHASE_PACE);

    MOQ_TEST_CHECK(write_obj(&fix, 1, 0x90, true, true, 1000) == MOQ_OK);
    MOQ_TEST_CHECK(write_obj(&fix, 0, 0x91, true, true, 1500) == MOQ_OK);
    MOQ_TEST_CHECK(write_obj(&fix, 1, 0x92, true, true, 2000) == MOQ_OK);
    MOQ_TEST_CHECK(write_obj(&fix, 0, 0x93, false, false, 2500) == MOQ_OK);

    /* Hook 1: the video anchor is discarded for want of demand; the delta of
     * the same group stays queued behind the refused audio object. */
    drive(&fix, now);
    {
        drop_snap_t st; drop_snap(fix.s, &st);
        MOQ_TEST_CHECK_EQ_U64(st.dropped, 1);
        MOQ_TEST_CHECK_EQ_U64(st.groups, 1);
    }
    expect_conserved(fix.s, 4, 1, 2, 1, 2 * PAYLOAD_LEN, "evx-a-first");

    /* Discard the delta through an INDEPENDENT public path. */
    MOQ_TEST_CHECK(moq_media_sender_remove_track(fix.s, fix.t[0]) == MOQ_OK);
    drive(&fix, now);

    drop_snap_t st; drop_snap(fix.s, &st);
    MOQ_TEST_CHECK_EQ_U64(st.dropped, 2);
    MOQ_TEST_CHECK_EQ_U64(st.keyframes, 1);   /* the anchor */
    MOQ_TEST_CHECK_EQ_U64(st.groups, 1);
    expect_conserved(fix.s, 4, 2, 0, 2, 0, "evx-a-final");
    {
        const tr_exp_t audio[2] = { { 1, 0, 0, 0x90, 0 }, { 1, 1, 0, 0x92, 2 } };
        expect_from(&fix, 0, audio, 2, "evx-a-audio");
        const term_exp_t terms[1] = { { 1, 0, 0, true, true } };
        expect_terminals(&fix, terms, 1, "evx-a-terminals");
        /* The removed video track never had a subscriber and never opened a
         * subgroup: its removal sends no SUBSCRIBE_DONE, no RESET and no
         * END_OF_TRACK status object. */
        expect_done(&fix, NULL, 0, "evx-a-done");
        expect_resets(&fix, NULL, 0, "evx-a-reset");
        expect_status(&fix, NULL, 0, "evx-a-status");
    }

out:
    fix_free(&fix);
    MOQ_TEST_CHECK(as.balance == 0);
    report(&fix, "pacing_evict_crosspath_group", before);
}

/* A Group abandoned for want of demand stays abandoned when demand arrives
 * before its queued tail drains: the video anchor is dropped with no
 * subscriber, its delta waits behind a shared-capacity refusal, then video is
 * subscribed. The delta is dropped, never emitted as Object 0 of its Group,
 * and the next keyframe opens Group 1. */
static void test_no_demand_suffix_dropped(void)
{
    int before = failures;
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    fix_t fix;
    uint64_t now = 1000;
    if (!fix_init(&fix, &alloc, 4, true /* drop_without_demand */, now)) {
        failures++; goto out;
    }
    if (!subscribe_track(&fix, 1, "a", now)) { failures++; goto out; }
    set_phase(&fix, PHASE_PACE);

    /* Audio Groups ahead of and behind the video Group, so every pass stops
     * on shared capacity before the video delta. */
    MOQ_TEST_CHECK(write_obj(&fix, 1, 0xa0, true, true, 1000) == MOQ_OK);
    MOQ_TEST_CHECK(write_obj(&fix, 0, 0xa1, true, true, 1500) == MOQ_OK);
    for (int i = 0; i < 6; i++)
        MOQ_TEST_CHECK(write_obj(&fix, 1, (uint8_t)(0xb0 + i), true, true,
                                 2000 + (uint64_t)i) == MOQ_OK);
    MOQ_TEST_CHECK(write_obj(&fix, 0, 0xa3, false, false, 2500) == MOQ_OK);
    drive(&fix, now);

    /* Demand arrives between passes, while the delta is still queued. */
    set_phase(&fix, PHASE_RELEASE);
    if (!subscribe_track(&fix, 0, "v", now)) { failures++; goto out; }
    for (int i = 0; i < 8; i++) drive(&fix, now);
    /* The next video keyframe opens Group 1. */
    MOQ_TEST_CHECK(write_obj(&fix, 0, 0xa4, true, true, 3000) == MOQ_OK);
    drive(&fix, now);
    {
        tr_exp_t all[8];
        for (int i = 0; i < 7; i++)
            all[i] = (tr_exp_t){ 1, (uint64_t)i, 0,
                                 (uint8_t)(i == 0 ? 0xa0 : 0xb0 + i - 1),
                                 i == 0 ? 0 : i + 1 };
        all[7] = (tr_exp_t){ 0, 1, 0, 0xa4, 9 };
        expect_from(&fix, 0, all, 8, "nds-trace");
        /* Each audio Group finished when the next opened; no video subgroup
         * ever opened for Group 0, so nothing was reset. */
        term_exp_t terms[6];
        for (int i = 0; i < 6; i++)
            terms[i] = (term_exp_t){ 1, (uint64_t)i, 0, true, true };
        expect_terminals(&fix, terms, 6, "nds-terminals");
        expect_resets(&fix, NULL, 0, "nds-reset");
    }
    drop_snap_t st; drop_snap(fix.s, &st);
    MOQ_TEST_CHECK_EQ_U64(st.dropped, 2);     /* the anchor, then its delta */
    MOQ_TEST_CHECK_EQ_U64(st.groups, 1);

out:
    fix_free(&fix);
    MOQ_TEST_CHECK(as.balance == 0);
    report(&fix, "pacing_no_demand_suffix_dropped", before);
}

/* Eviction: the armed RESET must survive removal ordering. */
static void test_evict_reset_survives_removal(void)
{
    int before = failures;
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    fix_t fix;
    uint64_t now = 1000;
    enum { QCAP = 2 };
    if (!fix_init_drop(&fix, &alloc, now, QCAP)) { failures++; goto out; }
    if (!evict_geometry(&fix, now)) { failures++; goto out; }
    /* REMOVAL-INDEPENDENT flush: the
     * eviction armed the abandon obligation while video's subgroup was open,
     * and video has NO queued entry for a drain pass to hang it off -- yet the
     * geometry's own production hook must already have discharged it. Asserted
     * BEFORE any removal call. */
    MOQ_TEST_CHECK_EQ_INT(fix.reset_n, 1);
    {
        drop_snap_t st0; drop_snap(fix.s, &st0);
        MOQ_TEST_CHECK_EQ_U64(st0.abandoned, 1);
    }

    MOQ_TEST_CHECK(moq_media_sender_remove_track(fix.s, fix.t[0]) == MOQ_OK);
    drive(&fix, now);
    drive(&fix, now);   /* a further drive: no duplicate reset/finish/terminal */

    /* Exactly one RESET for the abandoned video subgroup, with the service's
     * declared code, and NO finish for that same subgroup. */
    {
        const reset_exp_t rexp[1] = {
            { 0, 0, 0, 0x10, true, .sub_opaque = 0, .pub_opaque = 0 } };
        expect_resets(&fix, rexp, 1, "evx-b-reset");
    }
    {   /* TWO finishes, and neither is video g0: audio's first group closed
         * when its second opened, and moq_pub_end_track() correctly opens a
         * FRESH video subgroup g1 for the terminal and closes that. */
        const term_exp_t terms[2] = { { 1, 0, 0, true, true },
                                      { 0, 1, 0, false, false } };
        expect_terminals(&fix, terms, 2, "evx-b-terminals");
    }
    {   /* Exactly one video END_OF_TRACK status object, on that fresh
         * subgroup, carrying no payload. */
        const stat_exp_t sx[1] = { { 0, 1, 0, MOQ_OBJECT_END_OF_TRACK } };
        expect_status(&fix, sx, 1, "evx-b-status");
    }
    {   /* The track's terminal control is exact. */
        const done_exp_t dexp[1] = {
            { fix.sub[0]._opaque, MOQ_PUB_DONE_TRACK_ENDED } };
        expect_done(&fix, dexp, 1, "evx-b-done");
    }
    /* ORDER, compared through the ONE global collector ordinal rather than
     * inferred from separate array positions: the reset precedes the fresh
     * video status object, the fresh subgroup's FIN, and the done. */
    if (fix.reset_n == 1 && fix.stat_n == 1 && fix.done_n == 1) {
        int rseq = fix.reset[0].seq;
        MOQ_TEST_CHECK(rseq < fix.stat[0].seq);
        MOQ_TEST_CHECK(rseq < fix.done_seq[0]);
        int fresh = -1;
        for (int i = 0; i < fix.term_n; i++)
            if (fix.term[i].track == 0 && fix.term[i].group == 1)
                fresh = fix.term[i].seq;
        MOQ_TEST_CHECK(fresh >= 0);
        if (fresh >= 0) MOQ_TEST_CHECK(rseq < fresh);
    }
    drop_snap_t st; drop_snap(fix.s, &st);
    MOQ_TEST_CHECK_EQ_U64(st.abandoned, 1);   /* exactly once */
    MOQ_TEST_CHECK_EQ_U64(st.dropped, 1);
    MOQ_TEST_CHECK_EQ_U64(st.groups, 1);
    /* The other demanded track was unaffected (asserted in the geometry). */
    expect_conserved(fix.s, 4, 3, 0, 1, 0, "evx-b-final");

out:
    fix_free(&fix);
    MOQ_TEST_CHECK(as.balance == 0);
    report(&fix, "pacing_evict_reset_survives_removal", before);
}

/* ===================================================================== *
 *  The abandon obligation is RETRYABLE.
 *  The RESET itself is refused by real shared capacity before any teardown.
 *  While blocked: no g0 FIN, and the owner/queue/stats are preserved except
 *  the discards already committed. After capacity returns: exactly one reset
 *  with code 0x10, teardown only after it, exact fresh terminal output, and no
 *  duplicate on a further hook.
 * ===================================================================== */
static void test_reset_retry_blocked(void)
{
    int before = failures;
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    fix_t fix;
    uint64_t now = 1000;
    enum { QCAP = 2 };
    /* max_actions = 4 is the signed one-object-per-hook arm: the session's
     * action ring is what refuses the reset, through the real facade path. */
    if (!fix_init_drop_act(&fix, &alloc, now, QCAP, 4)) { failures++; goto out; }
    if (!evict_geometry_noflush(&fix, now)) { failures++; goto out; }

    /* The obligation is armed and has NOT been discharged: the hook that would
     * have flushed it was refused by shared capacity -- and that hook made
     * EXACTLY ONE attempt, not a futile second one from the post-pass sweep
     * with no capacity-progress point in between. */
    MOQ_TEST_CHECK_EQ_INT(fix.reset_n, 0);
    MOQ_TEST_CHECK_EQ_U64(moq_media_sender_test_reset_attempts(fix.s), 1);
    {
        drop_snap_t st0; drop_snap(fix.s, &st0);
        MOQ_TEST_CHECK_EQ_U64(st0.abandoned, 0);   /* nothing counted */
    }
    /* No FIN for the abandoned video subgroup while blocked. */
    for (int i = 0; i < fix.term_n; i++)
        MOQ_TEST_CHECK(!(fix.term[i].track == 0 && fix.term[i].group == 0));

    /* Ask for teardown while still blocked: it must NOT overtake the reset. */
    MOQ_TEST_CHECK(moq_media_sender_remove_track(fix.s, fix.t[0]) == MOQ_OK);
    drive(&fix, now);

    /* Capacity returns across the following hooks; the obligation is retried
     * and completes exactly once, before the terminal. */
    for (int i = 0; i < 8 && fix.reset_n == 0; i++) drive(&fix, now);
    drive(&fix, now);
    drive(&fix, now);   /* further hooks: no duplicate reset/finish/terminal */

    {
        const reset_exp_t rexp[1] = {
            { 0, 0, 0, 0x10, true, .sub_opaque = 0, .pub_opaque = 0 } };
        expect_resets(&fix, rexp, 1, "retry-reset");
        const stat_exp_t sx[1] = { { 0, 1, 0, MOQ_OBJECT_END_OF_TRACK } };
        expect_status(&fix, sx, 1, "retry-status");
        const done_exp_t dexp[1] = {
            { fix.sub[0]._opaque, MOQ_PUB_DONE_TRACK_ENDED } };
        expect_done(&fix, dexp, 1, "retry-done");
    }
    {   /* No FIN for video g0; audio's first group and the fresh terminal
         * subgroup are the only finishes. */
        const term_exp_t terms[2] = { { 1, 0, 0, true, true },
                                      { 0, 1, 0, false, false } };
        expect_terminals(&fix, terms, 2, "retry-terminals");
    }
    /* Teardown strictly AFTER the reset, on the one global ordinal. */
    if (fix.reset_n == 1 && fix.stat_n == 1 && fix.done_n == 1) {
        MOQ_TEST_CHECK(fix.reset[0].seq < fix.stat[0].seq);
        MOQ_TEST_CHECK(fix.reset[0].seq < fix.done_seq[0]);
    }
    {
        drop_snap_t st; drop_snap(fix.s, &st);
        MOQ_TEST_CHECK_EQ_U64(st.abandoned, 1);   /* exactly once */
    }

out:
    fix_free(&fix);
    MOQ_TEST_CHECK(as.balance == 0);
    report(&fix, "pacing_reset_retry_blocked", before);
}

/* ===================================================================== *
 *  The TERMINAL branch discharges the obligation through the one transition,
 *  independent of the cached demand answer.
 *
 *  Facade semantics, which decide this row's oracle: once the
 *  subscriber has left, moq_pub_reset_group() finds no active slot with an open
 *  subgroup and returns MOQ_OK having emitted nothing, and
 *  the departed peer observes neither the RESET nor the END_OF_TRACK. The
 *  observable is therefore the SERVICE-side transition -- the obligation
 *  discharged once WITHOUT counting another reset -- not a wire sequence.
 *  No seam was invented to manufacture one.
 * ===================================================================== */
static void test_terminal_flushes_reset(void)
{
    int before = failures;
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    fix_t fix;
    uint64_t now = 1000;
    enum { QCAP = 4 };
    if (!fix_init_drop(&fix, &alloc, now, QCAP)) { failures++; goto out; }
    if (!subscribe_track(&fix, 0, "v", now)) { failures++; goto out; }
    if (!subscribe_track(&fix, 1, "a", now)) { failures++; goto out; }
    set_phase(&fix, PHASE_PACE);

    /* Emit video group 0's anchor, leaving the group and wire subgroup open. */
    const tr_exp_t anchor[1] = { { 0, 0, 0, 0x70, 0 } };
    int from = fix.tr.n;
    MOQ_TEST_CHECK(write_obj(&fix, 0, 0x70, true, true, 1000) == MOQ_OK);
    drive(&fix, now);
    expect_from(&fix, from, anchor, 1, "tflush-anchor");

    /* The peer leaves the video track, and the sender observes it. */
    int cancel_resets = g_cancel_resets;
    MOQ_TEST_CHECK(moq_session_unsubscribe(moq_simpair_server(fix.sp),
                                           fix.sub[0], now) == MOQ_OK);
    for (int i = 0; i < 8 &&
         moq_media_sender_track_has_subscriber(fix.s, fix.t[0]); i++)
        drive(&fix, now);
    MOQ_TEST_CHECK(!moq_media_sender_track_has_subscriber(fix.s, fix.t[0]));
    /* The departure closes video g0/sg0 on the wire with the publisher's
     * CANCELLED reset. */
    MOQ_TEST_CHECK_EQ_INT(g_cancel_resets - cancel_resets, 1);
    /* Nothing armed yet, so nothing has been attempted. */
    MOQ_TEST_CHECK_EQ_U64(moq_media_sender_test_reset_attempts(fix.s), 0);

    /* APP THREAD ONLY from here, so no hook -- and therefore no general sweep
     * -- can run between arming the obligation and queueing the terminal. */
    /* TWO deltas of the open video group, so evicting that whole group frees
     * two slots and leaves room for the terminal marker without any hook. */
    MOQ_TEST_CHECK(write_obj(&fix, 0, 0x71, false, false, 2000) == MOQ_OK);
    MOQ_TEST_CHECK(write_obj(&fix, 0, 0x74, false, false, 2500) == MOQ_OK);
    MOQ_TEST_CHECK(write_obj(&fix, 1, 0x72, true, true, 3000) == MOQ_OK);
    MOQ_TEST_CHECK(write_obj(&fix, 1, 0x73, true, true, 4000) == MOQ_OK);
    MOQ_TEST_CHECK(write_obj(&fix, 1, 0x75, true, true, 4500) == MOQ_OK);
    {   /* The eviction really took the undemanded video delta and armed the
         * abandon obligation while the wire subgroup was still open. */
        drop_snap_t st; drop_snap(fix.s, &st);
        MOQ_TEST_CHECK_EQ_U64(st.dropped, 2);   /* both video deltas */
        MOQ_TEST_CHECK_EQ_U64(st.groups, 1);    /* one group */
        MOQ_TEST_CHECK_EQ_U64(st.abandoned, 0);
        MOQ_TEST_CHECK_EQ_U64(moq_media_sender_test_reset_attempts(fix.s), 0);
    }
    MOQ_TEST_CHECK(moq_media_sender_end_track(fix.s, fix.t[0]) == MOQ_OK);

    /* ONE hook. The terminal branch is reached for video (no video media is
     * held, so no hold latch retains it) and MUST discharge the obligation
     * before end_track can close the same subgroup. */
    drive(&fix, now);
    {
        drop_snap_t st; drop_snap(fix.s, &st);
        MOQ_TEST_CHECK_EQ_U64(st.abandoned, 0);   /* local completion only */
    }
    /* The row started from a declared zero, so the count is exact: that hook
     * made exactly ONE attempt. */
    MOQ_TEST_CHECK_EQ_U64(moq_media_sender_test_reset_attempts(fix.s), 1);
    drive(&fix, now);
    {   /* Exactly once: no further hook repeats the transition, and no
         * further hook even attempts it. */
        drop_snap_t st; drop_snap(fix.s, &st);
        MOQ_TEST_CHECK_EQ_U64(st.abandoned, 0);
        MOQ_TEST_CHECK_EQ_U64(moq_media_sender_test_reset_attempts(fix.s), 1);
    }
    /* The departed video peer observes nothing; audio is unaffected. */
    const tr_exp_t audio[3] = { { 1, 0, 0, 0x72, 3 }, { 1, 1, 0, 0x73, 4 },
                                { 1, 2, 0, 0x75, 5 } };
    expect_from(&fix, from + 1, audio, 3, "tflush-audio");
    {
        const term_exp_t terms[2] = { { 1, 0, 0, true, true },
                                      { 1, 1, 0, true, true } };
        expect_terminals(&fix, terms, 2, "tflush-terminals");
        /* The peer's departure is what closed video g0/sg0 on the wire: the
         * session resets the open subgroup with code 0x1 while the
         * subscription is torn down (asserted on the wire above), long before
         * the service arms its obligation, so the later service RESET (0x10)
         * reaches no active slot and emits nothing. The departed peer
         * surfaces no SUBGROUP_RESET for it: it stopped the stream when it
         * deleted the subscription state (draft-16 §9.15) and discards what
         * still arrives for it (draft-16 §10.1). */
        expect_resets(&fix, NULL, 0, "tflush-resets");
    }

out:
    fix_free(&fix);
    MOQ_TEST_CHECK(as.balance == 0);
    report(&fix, "pacing_terminal_flushes_reset", before);
}

/* ===================================================================== *
 *  ONE reset attempt per hook, by EITHER reach path.
 *
 *  The obligation is armed by capacity eviction on a single-slot track. How
 *  the drain reaches it is what the parameter selects, and NEITHER arm is a
 *  refusal -- the refused case needs two slots and lives in
 *  pacing_reset_partial_fanout:
 *
 *    drain_to == 1: the drain gets as far as the track's own queued entry, so
 *                   the QUEUE PATH attempts the obligation and, with a single
 *                   slot and room in the ring, COMPLETES it in that hook;
 *    drain_to == 2: an earlier entry is refused first, so the drain never
 *                   inspects the track's entry and the post-pass no-entry
 *                   SWEEP is what attempts it -- still refused here.
 *
 *  Either way the hook makes exactly one attempt, and the obligation is
 *  discharged exactly once overall.
 * ===================================================================== */
static void test_reset_attempt_once_per_hook(int drain_to)
{
    int before = failures;
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    fix_t fix;
    uint64_t now = 1000;
    enum { QCAP = 8, ACTS = 4, FILL = 7 };
    if (!fix_init_drop_act(&fix, &alloc, now, QCAP, ACTS)) { failures++; goto out; }
    if (!subscribe_track(&fix, 0, "v", now)) { failures++; goto out; }
    if (!subscribe_track(&fix, 1, "a", now)) { failures++; goto out; }
    set_phase(&fix, PHASE_PACE);

    /* Video group 0's anchor emitted; group and wire subgroup left open. */
    int from = fix.tr.n;
    MOQ_TEST_CHECK(write_obj(&fix, 0, 0x80, true, true, 1000) == MOQ_OK);
    for (int i = 0; i < 8 && fix.tr.n == from; i++) drive(&fix, now);
    MOQ_TEST_CHECK_EQ_INT(fix.tr.n - from, 1);

    /* A delta of that same open group -- anchorless, hence evictable. */
    MOQ_TEST_CHECK(write_obj(&fix, 0, 0x81, false, false, 2000) == MOQ_OK);
    /* Audio anchors fill the bound; one more forces the capacity path to take
     * the video delta and arm the abandon obligation. */
    for (int i = 0; i < FILL; i++)
        MOQ_TEST_CHECK(write_obj(&fix, 1, (uint8_t)(0x90 + i), true, true,
                                 3000 + (uint64_t)i * 100) == MOQ_OK);
    {
        drop_snap_t pre; drop_snap(fix.s, &pre);
        MOQ_TEST_CHECK_EQ_U64(pre.dropped, 0);
    }
    MOQ_TEST_CHECK(write_obj(&fix, 1, 0xA0, true, true, 5000) == MOQ_OK);
    {
        drop_snap_t post; drop_snap(fix.s, &post);
        MOQ_TEST_CHECK_EQ_U64(post.dropped, 1);      /* the video delta */
        MOQ_TEST_CHECK_EQ_U64(post.groups, 1);
        MOQ_TEST_CHECK_EQ_U64(post.abandoned, 0);    /* not discharged yet */
    }

    /* Drain the audio pressure down to a SINGLE queued object, so the next
     * hook's media pass exhausts the constrained action ring without ever
     * refusing an object -- the drain therefore reaches what follows. The
     * obligation stays refused throughout: every hook's one action slot is
     * taken by that hook's audio object. */
    for (int i = 0; i < 40; i++) {
        moq_media_sender_stats_t st; memset(&st, 0, sizeof(st));
        MOQ_TEST_CHECK(moq_media_sender_get_stats(fix.s, &st, sizeof(st)) == MOQ_OK);
        if (st.objects_queued <= (uint64_t)drain_to) break;
        drive(&fix, now);
    }
    MOQ_TEST_CHECK_EQ_INT(fix.reset_n, 0);
    /* At least one refused attempt already happened; the exact count depends
     * on how many hooks the drain loop above needed, so only the PER-HOOK
     * delta measured below is asserted exactly. */
    MOQ_TEST_CHECK(moq_media_sender_test_reset_attempts(fix.s) >= 1);

    /* Now give the blocked track a queued entry of its own, BEHIND that last
     * audio object, so the next hook reaches the obligation through the QUEUE
     * PATH rather than through the no-entry sweep. */
    MOQ_TEST_CHECK(write_obj(&fix, 0, 0x82, true, true, 6000) == MOQ_OK);
    {
        uint32_t a0 = moq_media_sender_test_reset_attempts(fix.s);
        drive(&fix, now);
        /* EXACTLY one attempt in that hook, whichever site reached the
         * obligation -- the queue path when the drain gets as far as the
         * video entry (drain_to == 1), the post-pass sweep when an earlier
         * entry was refused first (drain_to == 2). */
        MOQ_TEST_CHECK_EQ_U64(moq_media_sender_test_reset_attempts(fix.s),
                              (uint64_t)a0 + 1u);
        MOQ_TEST_CHECK_EQ_INT(fix.reset_n, drain_to == 1 ? 1 : 0);
    }

    /* Capacity returns: the obligation completes exactly once. */
    for (int i = 0; i < 24 && fix.reset_n == 0; i++) drive(&fix, now);
    for (int i = 0; i < 16; i++) drive(&fix, now);
    {
        const reset_exp_t rexp[1] = {
            { 0, 0, 0, 0x10, true, .sub_opaque = 0, .pub_opaque = 0 } };
        expect_resets(&fix, rexp, 1, "attempt-reset");
        drop_snap_t st; drop_snap(fix.s, &st);
        MOQ_TEST_CHECK_EQ_U64(st.abandoned, 1);
    }
    {   /* Each audio object is its own group and FINs when the NEXT group
         * opens, so groups 0..FILL-1 finish; audio's last group and video's
         * later group stay open (no terminal is requested here), and video's
         * abandoned group 0 RESETs rather than finishing. */
        term_exp_t terms[FILL];
        for (int i = 0; i < FILL; i++) {
            terms[i].track = 1; terms[i].group = (uint64_t)i;
            terms[i].subgroup = 0; terms[i].end_of_group = true;
            terms[i].carries_media = true;
        }
        expect_terminals(&fix, terms, FILL, "attempt-terminals");
    }

out:
    fix_free(&fix);
    MOQ_TEST_CHECK(as.balance == 0);
    report(&fix, drain_to == 1 ? "pacing_reset_attempt_queue_path"
                              : "pacing_reset_attempt_sweep_path", before);
}

/* ===================================================================== *
 *  PARTIAL queue-path refusal across a FAN-OUT track.
 *
 *  Two active facade slots on ONE publisher track: the peer's SUBSCRIBE and
 *  the peer's ACCEPTED PUBLISH (publish-mode push). One armed abandon
 *  obligation therefore owes TWO wire RESETs, and moq_pub_reset_group()
 *  retires them slot by slot -- so a bounded action ring can refuse the
 *  SECOND one. That is the state the per-pass attempt latch exists for: the
 *  queue path attempted the obligation and was refused, and the post-pass
 *  sweep must not retry the same track with no capacity-progress point in
 *  between.
 *
 *  Why two SUBSCRIBEs cannot be used instead: moq_session_subscribe() refuses
 *  a second subscriber-role subscription to the same full track name on one
 *  session with MOQ_ERR_INVAL (sub_is_duplicate_track,
 *  session_subscribe.c:545+), so a one-session pair cannot produce two
 *  subscription slots. The accepted publication is the public second slot.
 * ===================================================================== */
static void test_reset_partial_fanout(bool stale_remaining)
{
    int before = failures;
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    fix_t fix;
    uint64_t now = 1000;
    enum { QCAP = 8, ACTS = 4, FILL = 7 };
    if (!fix_init_pub_act(&fix, &alloc, now, QCAP, ACTS, "v", 0)) {
        failures++; goto out;
    }
    if (!subscribe_track(&fix, 0, "v", now)) { failures++; goto out; }
    if (!subscribe_track(&fix, 1, "a", now)) { failures++; goto out; }

    /* Declared publish-mode inventory, compared as an exact multiset against
     * the request image re-derived from the fixture and the product source:
     * the sender pushed all three of its tracks and this fixture accepted
     * EXACTLY the video one, so only that track fans out. */
    {
        /* Source-declared request ORDER decides which derived handle/alias
         * belongs to which track: the media snapshot in add order (v, a), then
         * the catalog, whose PUBLISH is sent after its retained group exists. */
        const pubreq_exp_t preq[3] = {
            { "v", true,  fix.exp_pub_handle[0], fix.exp_pub_alias[0],
              .has_largest = false },
            { "a", false, fix.exp_pub_handle[1], fix.exp_pub_alias[1],
              .has_largest = false },
            { MOQ_MSF_CATALOG_TRACK_NAME, false,
              fix.exp_pub_handle[2], fix.exp_pub_alias[2], true },
        };
        expect_pub_requests(&fix, preq, 3, "fanout-publish-requests");
    }
    MOQ_TEST_CHECK_EQ_INT(fix.pub_x_n, 1);
    MOQ_TEST_CHECK_EQ_INT(fix.pub_req_declined, 2);
    if (fix.pub_x_n != 1) { failures++; goto out; }
    const uint64_t vid_sub = fix.sub[0]._opaque;
    const uint64_t vid_pub = fix.pub_x[0]._opaque;
    MOQ_TEST_CHECK(vid_sub != 0 && vid_pub != 0);
    /* Two DISTINCT peer slots: if the handles collided, every per-handle
     * inventory below would be vacuous. */
    MOQ_TEST_CHECK(vid_sub != vid_pub);

    set_phase(&fix, PHASE_PACE);

    /* The anchor is delivered on BOTH slots -- the fan-out is real, not
     * assumed -- and leaves TWO open wire subgroups for group 0. Declared as
     * an exact multiset BEFORE the write: one observation on each exact peer
     * handle, same object identity, same enqueue ordinal, byte-exact payload,
     * in either delivery order. */
    int from = fix.tr.n;
    int anchor_ord = fix.enq_n;
    const tr_slot_exp_t anchor[2] = {
        { 0, vid_sub, 0, 0, 0, 0x80, anchor_ord },
        { 0, 0, vid_pub, 0, 0, 0x80, anchor_ord },
    };
    MOQ_TEST_CHECK(write_obj(&fix, 0, 0x80, true, true, 1000) == MOQ_OK);
    for (int i = 0; i < 12 && fix.tr.n - from < 2; i++) drive(&fix, now);
    expect_slots_from(&fix, from, anchor, 2, "fanout-anchor");

    /* Anchorless delta of that open group, then pressure that evicts it and
     * arms ONE abandon obligation for the track. Every enqueue ordinal is
     * captured at write time for the whole-row inventory below. */
    MOQ_TEST_CHECK(write_obj(&fix, 0, 0x81, false, false, 2000) == MOQ_OK);
    int aud_ord[FILL + 1];
    for (int i = 0; i < FILL; i++) {
        aud_ord[i] = fix.enq_n;
        MOQ_TEST_CHECK(write_obj(&fix, 1, (uint8_t)(0x90 + i), true, true,
                                 3000 + (uint64_t)i * 100) == MOQ_OK);
    }
    aud_ord[FILL] = fix.enq_n;
    MOQ_TEST_CHECK(write_obj(&fix, 1, 0xA0, true, true, 5000) == MOQ_OK);
    {
        drop_snap_t d; drop_snap(fix.s, &d);
        MOQ_TEST_CHECK_EQ_U64(d.dropped, 1);
        MOQ_TEST_CHECK_EQ_U64(d.groups, 1);
        MOQ_TEST_CHECK_EQ_U64(d.abandoned, 0);
    }

    /* Drain the audio pressure to a single queued object so the next hook's
     * drain reaches the video entry rather than stopping short of it. */
    for (int i = 0; i < 40; i++) {
        moq_media_sender_stats_t st; memset(&st, 0, sizeof(st));
        MOQ_TEST_CHECK(moq_media_sender_get_stats(fix.s, &st, sizeof(st)) == MOQ_OK);
        if (st.objects_queued <= 1) break;
        drive(&fix, now);
    }
    MOQ_TEST_CHECK_EQ_INT(fix.reset_n, 0);   /* nothing retired yet */
    int vid2_ord = fix.enq_n;
    MOQ_TEST_CHECK(write_obj(&fix, 0, 0x82, true, true, 6000) == MOQ_OK);

    /* THE MEASURED HOOK: the queue path reaches the obligation, retires the
     * first slot and is refused on the second. */
    {
        uint32_t a0 = moq_media_sender_test_reset_attempts(fix.s);
        drive(&fix, now);
        /* Exactly ONE service reset attempt: the sweep may not retry a track
         * the queue path already attempted in this same pass. */
        MOQ_TEST_CHECK_EQ_U64(moq_media_sender_test_reset_attempts(fix.s),
                              (uint64_t)a0 + 1u);
        /* Exactly the partial peer inventory that fit: one RESET, on one of
         * the two declared video handles, for the abandoned subgroup. */
        MOQ_TEST_CHECK_EQ_INT(fix.reset_n, 1);
        if (fix.reset_n == 1) {
            const reset_rec_t *r = &fix.reset[0];
            MOQ_TEST_CHECK_EQ_INT(r->track, 0);
            MOQ_TEST_CHECK_EQ_U64(r->group, 0);
            MOQ_TEST_CHECK_EQ_U64(r->subgroup, 0);
            MOQ_TEST_CHECK_EQ_U64(r->error_code, 0x10);
            MOQ_TEST_CHECK(r->end_of_group);
            MOQ_TEST_CHECK((r->sub_opaque == vid_sub && r->pub_opaque == 0) ||
                           (r->pub_opaque == vid_pub && r->sub_opaque == 0));
        }
        /* The obligation is RETAINED, not counted: the abandonment is one
         * service transition and it has not completed. */
        drop_snap_t d; drop_snap(fix.s, &d);
        MOQ_TEST_CHECK_EQ_U64(d.abandoned, 0);
        /* Neither abandoned subgroup may be turned into a clean FIN. */
        for (int i = 0; i < fix.term_n; i++)
            MOQ_TEST_CHECK(!(fix.term[i].track == 0 && fix.term[i].group == 0));
    }

    if (stale_remaining) {
        /* Retire the remaining subgroup directly through the session before
         * the next sender hook. This is a test-only driver of the real API,
         * using the existing layout access to identify the one open g0 slot.
         * The facade's next reset then sees a stale handle, with NO new reset
         * effect. The service must remember its first successful slot across
         * WOULD_BLOCK rather than counting only the last call's effects. */
        moq_session_t *session = moq_simpair_client(fix.sp);
        moq_subgroup_handle_t subgroup = MOQ_SUBGROUP_INVALID;
        size_t open = 0;
        for (size_t i = 0; i < session->sg_cap; i++) {
            const moq_sg_entry_t *sg = &session->subgroups[i];
            if (sg->state != MOQ_SG_OPEN || sg->group_id != 0) continue;
            subgroup._opaque = moq_handle_pack(MOQ_HANDLE_POOL_SUBGROUP,
                session->session_tag, sg->generation, (uint32_t)i);
            open++;
        }
        MOQ_TEST_CHECK_EQ_U64(open, 1);
        uint32_t a0 = moq_media_sender_test_reset_attempts(fix.s);
        MOQ_TEST_CHECK(moq_session_reset_subgroup(session, subgroup, 0x10, now)
                       == MOQ_OK);
        MOQ_TEST_CHECK(moq_simpair_run_until_quiescent(fix.sp, 64, NULL) == MOQ_OK);
        collect(&fix, now);   /* delivery only: no sender hook */
        MOQ_TEST_CHECK_EQ_INT(fix.reset_n, 2);
        MOQ_TEST_CHECK_EQ_U64(moq_media_sender_test_reset_attempts(fix.s), a0);
        drop_snap_t d; drop_snap(fix.s, &d);
        MOQ_TEST_CHECK_EQ_U64(d.abandoned, 0);   /* still not completed */
    }

    /* Capacity returns: the remaining slot is retired on a later hook, the
     * service abandonment is counted exactly once, and no hook duplicates it. */
    for (int i = 0; i < 40 && fix.reset_n < 2; i++) drive(&fix, now);
    for (int i = 0; i < 16; i++) drive(&fix, now);
    {
        drop_snap_t d; drop_snap(fix.s, &d);
        MOQ_TEST_CHECK_EQ_U64(d.abandoned, 1);
        moq_media_sender_track_stats_t ts = {0};
        MOQ_TEST_CHECK(moq_media_sender_get_track_stats(fix.s, fix.t[0], &ts,
                                                       sizeof(ts)) == MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(ts.groups_abandoned, 1);
    }
    {   /* Exactly one RESET per peer slot, matched by exact handle so the two
         * otherwise-identical records cannot authorize each other. */
        const reset_exp_t rexp[2] = {
            { 0, 0, 0, 0x10, true, vid_sub, 0 },
            { 0, 0, 0, 0x10, true, 0, vid_pub },
        };
        expect_resets(&fix, rexp, 2, "fanout-resets");
    }
    {   /* OUTPUT CONSERVATION for the whole row, as an exact multiset from the
         * original boundary: every observation the peer received, on its exact
         * peer handle, with exact identity, enqueue ordinal and payload bytes,
         * and nothing else. The evicted delta 0x81 is declared by its ABSENCE
         * -- twelve records, no leftovers, no duplicates.
         *
         *   video g0/o0 0x80 : once per video slot (the anchor, fanned out);
         *   audio g0..g7/o0  : 0x90..0x96 then 0xA0, once each on the audio
         *                      subscription (its publication was declined);
         *   video g1/o0 0x82 : once per video slot -- group 1, because group 0
         *                      was abandoned and RESET, never continued. */
        tr_slot_exp_t all[2 + FILL + 1 + 2];
        int m = 0;
        all[m++] = (tr_slot_exp_t){ 0, vid_sub, 0, 0, 0, 0x80, anchor_ord };
        all[m++] = (tr_slot_exp_t){ 0, 0, vid_pub, 0, 0, 0x80, anchor_ord };
        for (int i = 0; i < FILL + 1; i++) {
            uint8_t sent = (i < FILL) ? (uint8_t)(0x90 + i) : (uint8_t)0xA0;
            all[m++] = (tr_slot_exp_t){ 1, fix.sub[1]._opaque, 0,
                                        (uint64_t)i, 0, sent, aud_ord[i] };
        }
        all[m++] = (tr_slot_exp_t){ 0, vid_sub, 0, 1, 0, 0x82, vid2_ord };
        all[m++] = (tr_slot_exp_t){ 0, 0, vid_pub, 1, 0, 0x82, vid2_ord };
        expect_slots_from(&fix, from, all, m, "fanout-media");
    }
    {   /* Audio is subscription-only (its publication was declined), and each
         * of its objects is its own group finishing when the next one opens:
         * groups 0..FILL-1. Audio's last group and video's later group stay
         * open, and video's abandoned group 0 RESETs on both slots rather than
         * finishing on either. */
        term_exp_t terms[FILL];
        for (int i = 0; i < FILL; i++) {
            terms[i].track = 1; terms[i].group = (uint64_t)i;
            terms[i].subgroup = 0; terms[i].end_of_group = true;
            terms[i].carries_media = true;
        }
        expect_terminals(&fix, terms, FILL, "fanout-terminals");
    }

out:
    fix_free(&fix);
    MOQ_TEST_CHECK(as.balance == 0);
    report(&fix, stale_remaining ? "pacing_reset_partial_fanout_stale_remainder"
                                 : "pacing_reset_partial_fanout", before);
}

/* ===================================================================== *
 *  A write that waits for room re-checks its Group after the wait.
 *
 *  BLOCK_TIMEOUT releases the sender lock while it waits, so a drop can close
 *  the Group the waiting object continues. The object is then refused with
 *  WOULD_BLOCK, never queued into the closed Group. Driven white-box: the
 *  cond-wait seam runs the drop where the wait would be.
 * ===================================================================== */
typedef struct closed_ctx {
    moq_media_sender_t *s;
    moq_media_track_t  *t;
    int                 calls;
} closed_ctx_t;

static int close_group_during_wait(const struct timespec *abs, void *ctx)
{
    (void)abs;
    closed_ctx_t *c = (closed_ctx_t *)ctx;
    if (c->calls++ == 0)
        moq_media_sender_test_drop_track_locked(c->s, c->t);
    return 0;
}

static moq_media_track_t *wr_add_loc(moq_media_sender_t *s, const char *name)
{
    moq_media_track_cfg_t tc;
    moq_media_track_cfg_init_sized(&tc, sizeof(tc));
    tc.name = (moq_bytes_t){ (const uint8_t *)name, strlen(name) };
    tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
    tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
    tc.codec = (moq_bytes_t){ (const uint8_t *)"av01", 4 };
    tc.bitrate = 1000000;
    moq_media_track_t *t = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_add_track(s, &tc, &t),
                          (int)MOQ_OK);
    return t;
}

static moq_result_t wr_write(moq_media_sender_t *s, moq_media_track_t *t,
                             bool sync)
{
    uint8_t d[PAYLOAD_LEN];
    memset(d, 0x5A, sizeof(d));
    moq_rcbuf_t *b = NULL;
    moq_rcbuf_create(moq_alloc_default(), d, sizeof(d), &b);
    moq_media_send_object_t o; memset(&o, 0, sizeof(o));
    o.struct_size = sizeof(o);
    o.payload = b;
    o.is_sync = sync;
    o.starts_group = sync;
    moq_result_t rc = moq_media_sender_write(s, t, &o);
    if (rc != MOQ_OK) moq_rcbuf_decref(b);
    return rc;
}

static void test_wait_recheck(void)
{
    int before = failures;
    moq_media_sender_cfg_t cfg;
    moq_media_sender_cfg_init_lossless_sized(&cfg, sizeof(cfg));
    g_ns[0] = MOQ_BYTES_LITERAL("svc");
    g_ns[1] = MOQ_BYTES_LITERAL("wait");
    cfg.namespace_ = (moq_namespace_t){ g_ns, 2 };
    cfg.pre_ready_max_objects = 2;
    cfg.block_timeout_us = 200000;
    /* No session is ever latched here: pin the legacy companion, whose
     * Group IDs count from 0. */
    cfg.companion_profile = MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01;
    moq_media_sender_t *s = moq_media_sender_test_new_cfg(&cfg);
    MOQ_TEST_CHECK(s != NULL);
    if (!s) return;
    moq_media_track_t *a = wr_add_loc(s, "a");
    moq_media_track_t *b = wr_add_loc(s, "b");
    /* Group 0 of `a`: its anchor is sent, a delta of it stays queued. */
    MOQ_TEST_CHECK_EQ_INT((int)wr_write(s, a, true), (int)MOQ_OK);
    moq_rcbuf_t *p = NULL;
    MOQ_TEST_CHECK(moq_media_sender_test_emit_head(s, &p));
    if (p) moq_rcbuf_decref(p);
    MOQ_TEST_CHECK_EQ_INT((int)wr_write(s, a, false), (int)MOQ_OK);
    /* `b` fills the bound. */
    MOQ_TEST_CHECK_EQ_INT((int)wr_write(s, b, true), (int)MOQ_OK);
    /* Another delta of `a` waits; meanwhile `a`'s queued delta is dropped,
     * which closes Group 0. */
    closed_ctx_t c = { s, a, 0 };
    moq_media_sender_test_set_cond_wait_hook(close_group_during_wait, &c);
    moq_result_t rc = wr_write(s, a, false);
    moq_media_sender_test_set_cond_wait_hook(NULL, NULL);
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK(c.calls >= 1);
    const moq_media_track_t *qt = NULL;
    for (size_t i = 0;
         moq_media_sender_test_queue_entry(s, i, &qt, NULL, NULL, NULL); i++)
        MOQ_TEST_CHECK(qt != a);
    /* The next sync point opens Group 1. */
    MOQ_TEST_CHECK_EQ_INT((int)wr_write(s, a, true), (int)MOQ_OK);
    uint64_t grp = 0;
    bool starts = false, seen = false;
    for (size_t i = 0;
         moq_media_sender_test_queue_entry(s, i, &qt, NULL, &grp, &starts); i++)
        if (qt == a) {
            MOQ_TEST_CHECK(grp == 1 && starts);
            seen = true;
        }
    MOQ_TEST_CHECK(seen);
    moq_media_sender_test_free(s);
    int n = failures - before;
    if (n == 0) printf("PASS: wait_recheck_closed_group\n");
    else        printf("FAIL: wait_recheck_closed_group (%d diagnostics)\n", n);
}


/* ===================================================================== *
 *  Graceful completion with a generated SAP timeline.
 *  complete() owes the "v.sap" sibling an end as well as its media track:
 *  the timeline ends only after every record its media produced has gone
 *  out, and the terminal (isComplete) catalog is cut only after both ends.
 *  A timeline without a subscriber keeps its records for a joiner and holds
 *  the completion with them, as retained media does.
 *
 *  Its own small rig, not fix_t: the peer subscribes to the catalog, the
 *  media track and (optionally) its timeline, and every delivery is logged
 *  in one arrival order, so ordering across tracks is compared directly.
 * ===================================================================== */
enum { TLC_V = 0, TLC_SAP = 1, TLC_CAT = 2, TLC_A = 3, TLC_NSUB = 4, TLC_LOG = 64,
       TLC_COPY = 192 };

typedef enum { TLC_OBJ, TLC_STATUS, TLC_DONE } tlc_kind_t;

typedef struct {
    tlc_kind_t kind;
    int        track;          /* TLC_V / TLC_SAP / TLC_CAT / TLC_A, -1 = foreign */
    uint64_t   group, object;
    int        status;
    int        pass;           /* the drive that delivered it */
    bool       cat_complete;   /* a catalog object that parsed isComplete */
    bool       cat_bad;        /* a catalog object that did not parse */
    size_t     len;
    uint8_t    copy[TLC_COPY];
    bool       truncated;
} tlc_rec_t;

typedef struct {
    moq_simpair_t      *sp;
    moq_media_sender_t *s;
    moq_media_track_t  *v;
    moq_media_track_t  *a;
    moq_subscription_t  sub[TLC_NSUB];
    int                 sub_ok;
    tlc_rec_t           log[TLC_LOG];
    int                 n;
    bool                overflow, unexpected;
    int                 pass;
} tlc_t;

static int tlc_track_of(const tlc_t *x, uint64_t sub_opaque)
{
    for (int i = 0; i < TLC_NSUB; i++)
        if (x->sub[i]._opaque && x->sub[i]._opaque == sub_opaque) return i;
    return -1;
}

static tlc_rec_t *tlc_push(tlc_t *x, tlc_kind_t k, int track)
{
    if (x->n >= TLC_LOG) { x->overflow = true; return NULL; }
    tlc_rec_t *r = &x->log[x->n++];
    memset(r, 0, sizeof(*r));
    r->kind = k;
    r->track = track;
    r->pass = x->pass;
    return r;
}

static void tlc_collect(tlc_t *x, uint64_t now)
{
    moq_session_t *srv = moq_simpair_server(x->sp);
    moq_event_t ev;
    while (moq_session_poll_events(srv, &ev, 1) == 1) {
        if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) {
            const moq_object_received_event_t *o = &ev.u.object_received;
            int idx = tlc_track_of(x, o->sub._opaque);
            tlc_rec_t *r = tlc_push(x, o->status == MOQ_OBJECT_NORMAL
                                           ? TLC_OBJ : TLC_STATUS, idx);
            if (r) {
                r->group = o->group_id;
                r->object = o->object_id;
                r->status = (int)o->status;
                r->len = o->payload ? moq_rcbuf_len(o->payload) : 0;
                if (r->len > TLC_COPY) r->truncated = true;
                else if (r->len) memcpy(r->copy, moq_rcbuf_data(o->payload), r->len);
                if (idx == TLC_CAT && o->status == MOQ_OBJECT_NORMAL) {
                    moq_msf_catalog_t cat;
                    memset(&cat, 0, sizeof(cat));
                    moq_bytes_t j = { moq_rcbuf_data(o->payload),
                                      moq_rcbuf_len(o->payload) };
                    if (moq_msf_catalog_parse(moq_alloc_default(), j, &cat) == MOQ_OK) {
                        r->cat_complete = cat.is_complete;
                        moq_msf_catalog_cleanup(moq_alloc_default(), &cat);
                    } else {
                        r->cat_bad = true;
                    }
                }
            }
        } else if (ev.kind == MOQ_EVENT_SUBSCRIBE_DONE) {
            tlc_rec_t *r = tlc_push(x, TLC_DONE,
                                    tlc_track_of(x, ev.u.subscribe_done.sub._opaque));
            if (r) r->status = (int)ev.u.subscribe_done.status_code;
        } else if (ev.kind == MOQ_EVENT_SUBSCRIBE_OK) {
            x->sub_ok++;
        } else if (ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED) {
            moq_accept_namespace_cfg_t ac;
            moq_accept_namespace_cfg_init(&ac);
            (void)moq_session_accept_namespace(srv, ev.u.namespace_published.ann,
                                               &ac, now);
        } else if (ev.kind != MOQ_EVENT_SUBGROUP_FINISHED) {
            x->unexpected = true;
        }
        moq_event_cleanup(&ev);
    }
}

/* make_pair with a peer event queue deep enough for a whole pump, so every
 * delivery surfaces in the pump that issued it and arrival passes order the
 * sender's issues across streams. */
static moq_simpair_t *tlc_pair(moq_alloc_t *alloc, uint32_t max_actions)
{
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = alloc; cfg.seed = 42; cfg.initial_now_us = 1000;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 32;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = 32;
    cfg.version = PACING_VERSION;
    cfg.max_actions = max_actions;   /* 0 = library default */
    cfg.max_events = 256;
    moq_simpair_t *sp = NULL;
    if (moq_simpair_create(&cfg, &sp) != MOQ_OK) return NULL;
    moq_simpair_start(sp);
    (void)moq_simpair_run_until_quiescent(sp, 64, NULL);
    moq_event_t ev;
    while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
        moq_event_cleanup(&ev);
    while (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1)
        moq_event_cleanup(&ev);
    return sp;
}

static void tlc_drive(tlc_t *x, uint64_t now)
{
    x->pass++;
    moq_media_sender_test_pump(x->s, moq_simpair_client(x->sp), now);
    MOQ_TEST_CHECK(moq_simpair_run_until_quiescent(x->sp, 64, NULL) == MOQ_OK);
    tlc_collect(x, now);
}

static bool tlc_subscribe(tlc_t *x, int idx, const char *name, uint64_t now)
{
    moq_subscribe_cfg_t sc; moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ g_ns, 2 };
    sc.track_name = (moq_bytes_t){ (const uint8_t *)name, strlen(name) };
    sc.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    sc.has_forward = true; sc.forward = true;
    if (moq_session_subscribe(moq_simpair_server(x->sp), &sc, now,
                              &x->sub[idx]) != MOQ_OK)
        return false;
    int want = x->sub_ok + 1;
    for (int i = 0; i < 16 && x->sub_ok < want; i++) tlc_drive(x, now);
    return x->sub_ok == want;
}

static moq_result_t tlc_write_to(tlc_t *x, moq_media_track_t *t,
                                 uint8_t sentinel, bool starts, bool has_sap,
                                 moq_sap_type_t sap, uint64_t pts_us)
{
    uint8_t d[PAYLOAD_LEN];
    memset(d, sentinel, sizeof(d));
    moq_rcbuf_t *b = NULL;
    if (moq_rcbuf_create(moq_alloc_default(), d, sizeof(d), &b) != MOQ_OK)
        return MOQ_ERR_NOMEM;
    moq_media_send_object_t o; memset(&o, 0, sizeof(o));
    o.struct_size = sizeof(o);
    o.payload = b;
    o.is_sync = starts;
    o.starts_group = starts;
    o.presentation_time_us = pts_us;
    o.has_sap_type = has_sap;
    o.sap_type = sap;
    moq_result_t rc = moq_media_sender_write(x->s, t, &o);
    if (rc != MOQ_OK) moq_rcbuf_decref(b);
    return rc;
}

static moq_result_t tlc_write(tlc_t *x, uint8_t sentinel, bool starts,
                              moq_sap_type_t sap, uint64_t pts_us)
{
    return tlc_write_to(x, x->v, sentinel, starts, true, sap, pts_us);
}

/* Index of the first/last record matching (kind, track), or -1. */
static int tlc_find(const tlc_t *x, tlc_kind_t k, int track, bool last)
{
    int at = -1;
    for (int i = 0; i < x->n; i++)
        if (x->log[i].kind == k && x->log[i].track == track) {
            at = i;
            if (!last) break;
        }
    return at;
}

static int tlc_count(const tlc_t *x, tlc_kind_t k, int track)
{
    int c = 0;
    for (int i = 0; i < x->n; i++)
        c += x->log[i].kind == k && x->log[i].track == track;
    return c;
}

static int tlc_first_complete_catalog(const tlc_t *x)
{
    for (int i = 0; i < x->n; i++)
        if (x->log[i].kind == TLC_OBJ && x->log[i].track == TLC_CAT &&
            x->log[i].cat_complete)
            return i;
    return -1;
}

/* Every SAP record the media produced, as the last (independent) timeline
 * Object carries them: CMSF §3.6.1 [{"l":[group,object],"data":[sap,ept]}],
 * ept the presentation time rounded to ms. */
static const char TLC_SAP_ALL[] =
    "[{\"l\":[0,0],\"data\":[1,0]},{\"l\":[1,0],\"data\":[1,33]},"
    "{\"l\":[1,1],\"data\":[3,50]},{\"l\":[2,0],\"data\":[1,67]}]";

/* Which subscription arrives only after the hold is observed:
 *   TL_DEMANDED  - none: "v" and "v.sap" are both subscribed before complete();
 *   TL_JOINER    - "v.sap": its records are kept and the completion held;
 *   MEDIA_JOINER - "v": the media is retained, so it has produced no records
 *                  yet, and the subscribed timeline must not end before it;
 *   OTHER_HELD   - "a", a second track whose one Object is retained: "v"
 *                  and its timeline complete in full while "a" holds the
 *                  finalization, and the timeline's outstanding work is
 *                  reported as work on every pump until its end is sent.
 * `max_actions` bounds the session's action ring, so the media, the timeline
 * and both ends contend for room across several pumps. */
typedef enum { TL_DEMANDED, TL_JOINER, MEDIA_JOINER, OTHER_HELD } tlc_mode_t;

static void test_complete_timeline(tlc_mode_t mode, uint32_t max_actions,
                                   bool mt)
{
    int before = failures;
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    tlc_t x;
    memset(&x, 0, sizeof(x));
    uint64_t now = 1000;
    char label[96];
    const char *tl_name = mt ? "v.timeline" : "v.sap";
    snprintf(label, sizeof(label), "pacing_complete_%s_%s_act%u",
             mt ? "media_timeline" : "timeline",
             mode == TL_DEMANDED ? "demanded"
             : mode == TL_JOINER ? "joiner"
             : mode == MEDIA_JOINER ? "media_joiner" : "other_held",
             (unsigned)max_actions);

    x.sp = tlc_pair(&alloc, max_actions);
    if (!x.sp) { failures++; goto out; }
    g_ns[0] = MOQ_BYTES_LITERAL("svc");
    g_ns[1] = MOQ_BYTES_LITERAL("tl");
    moq_media_sender_cfg_t cfg;
    moq_media_sender_cfg_init_live_sized(&cfg, sizeof(cfg));
    cfg.namespace_ = (moq_namespace_t){ g_ns, 2 };
    cfg.catalog_refresh_interval_us = UINT64_MAX;
    x.s = moq_media_sender_test_new_cfg(&cfg);
    if (!x.s) { failures++; goto out; }
    moq_media_track_cfg_t tc;
    moq_media_track_cfg_init(&tc);
    tc.name = MOQ_BYTES_LITERAL("v");
    tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
    tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
    tc.codec = MOQ_BYTES_LITERAL("av01");
    tc.bitrate = 1500000;
    tc.timescale = 1000000;
    tc.is_live = true;
    tc.emit_sap_timeline = !mt;
    tc.sap_timeline_history_groups = 8;   /* nothing evicted here */
    tc.emit_media_timeline = mt;
    tc.media_timeline_history_groups = 1; /* each group's record evicts the last */
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_add_track(x.s, &tc, &x.v),
                          (int)MOQ_OK);
    if (!x.v) { failures++; goto out; }
    if (mode == OTHER_HELD) {
        x.a = add_track(x.s, "a", MOQ_MEDIA_TYPE_AUDIO);
        if (!x.a) { failures++; goto out; }
    }
    for (int c = 0; c < 16 && !moq_media_sender_is_ready(x.s); c++)
        tlc_drive(&x, now);
    MOQ_TEST_CHECK(moq_media_sender_is_ready(x.s));
    MOQ_TEST_CHECK(tlc_subscribe(&x, TLC_CAT, MOQ_MSF_CATALOG_TRACK_NAME, now));
    if (mode != MEDIA_JOINER)
        MOQ_TEST_CHECK(tlc_subscribe(&x, TLC_V, "v", now));
    if (mode != TL_JOINER)
        MOQ_TEST_CHECK(tlc_subscribe(&x, TLC_SAP, tl_name, now));
    /* Nothing delivered yet: the catalog subscription starts after the
     * catalog already published, and no media has been written. */
    MOQ_TEST_CHECK_EQ_INT(x.n, 0);
    MOQ_TEST_CHECK_EQ_INT(tlc_first_complete_catalog(&x), -1);

    /* Three media groups, the middle one with a mid-group SAP 3: four SAP
     * records. Written and completed with no pump in between, so the whole
     * backlog and every end meet the bounded ring together. */
    MOQ_TEST_CHECK(tlc_write(&x, 0x60, true,  MOQ_SAP_TYPE_1, 0)     == MOQ_OK);
    MOQ_TEST_CHECK(tlc_write(&x, 0x61, true,  MOQ_SAP_TYPE_1, 33000) == MOQ_OK);
    MOQ_TEST_CHECK(tlc_write(&x, 0x62, false, MOQ_SAP_TYPE_3, 50000) == MOQ_OK);
    MOQ_TEST_CHECK(tlc_write(&x, 0x63, true,  MOQ_SAP_TYPE_1, 66600) == MOQ_OK);
    if (mode == OTHER_HELD)
        MOQ_TEST_CHECK(tlc_write_to(&x, x.a, 0x70, true, false, MOQ_SAP_NONE,
                                    10000) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_complete(x.s), (int)MOQ_OK);
    int from_pass = x.pass;

    /* Drive until the terminal catalog arrives, or a generous bound. With
     * "a" held, work is reported on every pump while one of v's ends is
     * still to arrive. (An end is on the wire before the facade's end_track
     * has also sent its done, so work can outlast the arrival by a pump;
     * the hold is checked idle below.) */
    for (int d = 0; d < 64 && tlc_first_complete_catalog(&x) < 0; d++) {
        tlc_drive(&x, now);
        if (mode == OTHER_HELD) {
            bool ends_out = tlc_count(&x, TLC_STATUS, TLC_V) == 1 &&
                            tlc_count(&x, TLC_STATUS, TLC_SAP) == 1;
            bool pending = moq_media_sender_test_has_pending_output(x.s);
            MOQ_TEST_CHECK(pending || ends_out);
            if (!pending && !ends_out)
                fprintf(stderr, "  pump %d: pending=%d ends_out=%d\n",
                        x.pass, (int)pending, (int)ends_out);
        }
    }

    if (mode == TL_JOINER) {
        /* Held: the media and its end went out, the timeline kept all four
         * records for a joiner, and nothing finalized -- no terminal
         * catalog, no SUBSCRIBE_DONE, work still reported pending. */
        MOQ_TEST_CHECK_EQ_INT(tlc_count(&x, TLC_OBJ, TLC_V), 4);
        MOQ_TEST_CHECK_EQ_INT(tlc_count(&x, TLC_STATUS, TLC_V), 1);
        MOQ_TEST_CHECK_EQ_INT(tlc_first_complete_catalog(&x), -1);
        MOQ_TEST_CHECK_EQ_INT(tlc_count(&x, TLC_DONE, TLC_V), 0);
        MOQ_TEST_CHECK(!moq_media_sender_test_has_pending_output(x.s));
        {   /* A completion held for a joiner has not failed. */
            moq_media_sender_stats_t st;
            MOQ_TEST_CHECK(moq_media_sender_get_stats(x.s, &st, sizeof(st)) == MOQ_OK);
            MOQ_TEST_CHECK_EQ_INT((int)st.completion_error, (int)MOQ_OK);
        }
        int held_at = x.n;
        /* A joiner subscribes: it is owed those records, then the end. */
        MOQ_TEST_CHECK(tlc_subscribe(&x, TLC_SAP, tl_name, now));
        MOQ_TEST_CHECK(moq_media_sender_test_has_pending_output(x.s));
        for (int d = 0; d < 64 && tlc_first_complete_catalog(&x) < 0; d++)
            tlc_drive(&x, now);
        /* Nothing about the timeline was delivered before the join. */
        for (int i = 0; i < held_at; i++)
            MOQ_TEST_CHECK(x.log[i].track != TLC_SAP);
    } else if (mode == MEDIA_JOINER) {
        /* Held: the media is retained for a joiner with its end behind it,
         * so no record exists yet and the subscribed timeline neither emits
         * nor ends; nothing finalizes and no work is reported. */
        MOQ_TEST_CHECK_EQ_INT(x.n, 0);
        MOQ_TEST_CHECK(!moq_media_sender_test_has_pending_output(x.s));
        MOQ_TEST_CHECK(tlc_subscribe(&x, TLC_V, "v", now));
        MOQ_TEST_CHECK(moq_media_sender_test_has_pending_output(x.s));
        for (int d = 0; d < 64 && tlc_first_complete_catalog(&x) < 0; d++)
            tlc_drive(&x, now);
    } else if (mode == OTHER_HELD) {
        /* v and its timeline are complete and ended; "a" holds the
         * finalization with its retained Object. A joiner releases it. */
        MOQ_TEST_CHECK_EQ_INT(tlc_first_complete_catalog(&x), -1);
        MOQ_TEST_CHECK_EQ_INT(tlc_count(&x, TLC_STATUS, TLC_V), 1);
        MOQ_TEST_CHECK_EQ_INT(tlc_count(&x, TLC_STATUS, TLC_SAP), 1);
        MOQ_TEST_CHECK_EQ_INT(tlc_count(&x, TLC_OBJ, TLC_A), 0);
        MOQ_TEST_CHECK(!moq_media_sender_test_has_pending_output(x.s));
        MOQ_TEST_CHECK(tlc_subscribe(&x, TLC_A, "a", now));
        MOQ_TEST_CHECK(moq_media_sender_test_has_pending_output(x.s));
        for (int d = 0; d < 64 && tlc_first_complete_catalog(&x) < 0; d++)
            tlc_drive(&x, now);
        MOQ_TEST_CHECK_EQ_INT(tlc_count(&x, TLC_OBJ, TLC_A), 1);
        MOQ_TEST_CHECK_EQ_INT(tlc_count(&x, TLC_STATUS, TLC_A), 1);
        int a_end = tlc_find(&x, TLC_STATUS, TLC_A, false);
        int a_obj = tlc_find(&x, TLC_OBJ, TLC_A, false);
        MOQ_TEST_CHECK(a_end > a_obj && a_obj >= 0);
        int t = tlc_first_complete_catalog(&x);
        MOQ_TEST_CHECK(t >= 0 && a_end >= 0 && x.log[t].pass >= x.log[a_end].pass);
    }

    /* The inventory is taken once the rig is idle, the teardown's done
     * included. */
    MOQ_TEST_CHECK(tlc_first_complete_catalog(&x) >= 0);
    for (int i = 0; i < 4; i++) tlc_drive(&x, now);

    /* Media: exactly the four Objects, then one END_OF_TRACK after them. */
    MOQ_TEST_CHECK_EQ_INT(tlc_count(&x, TLC_OBJ, TLC_V), 4);
    MOQ_TEST_CHECK_EQ_INT(tlc_count(&x, TLC_STATUS, TLC_V), 1);
    int v_last = tlc_find(&x, TLC_OBJ, TLC_V, true);
    int v_end = tlc_find(&x, TLC_STATUS, TLC_V, false);
    MOQ_TEST_CHECK(v_end > v_last && v_last >= 0);
    if (v_end >= 0)
        MOQ_TEST_CHECK_EQ_INT(x.log[v_end].status, MOQ_OBJECT_END_OF_TRACK);

    /* Timeline: one Object per media group's boundary, strictly increasing
     * Locations; the last carries every record still retained; then one
     * END_OF_TRACK. The SAP history keeps all four records. The media
     * timeline keeps one group's record, so a boundary whose record was
     * evicted before it could go out is skipped, and every Object carries
     * exactly its own group's §7.1.1 record. */
    int sap_n = tlc_count(&x, TLC_OBJ, TLC_SAP);
    if (mt) MOQ_TEST_CHECK(sap_n >= 1 && sap_n <= 3);
    else    MOQ_TEST_CHECK(sap_n >= 3 && sap_n <= 4);
    uint64_t pg = 0, po = 0;
    bool first = true;
    for (int i = 0; i < x.n; i++) {
        const tlc_rec_t *r = &x.log[i];
        if (r->kind != TLC_OBJ || r->track != TLC_SAP) continue;
        if (!first)
            MOQ_TEST_CHECK(r->group > pg || (r->group == pg && r->object > po));
        first = false; pg = r->group; po = r->object;
        if (mt) {
            static const uint64_t media_ms[3] = { 0, 33, 66 };
            moq_msf_media_timeline_record_t rec[2];
            size_t nrec = 0;
            MOQ_TEST_CHECK(!r->truncated);
            MOQ_TEST_CHECK(moq_msf_media_timeline_decode(
                (moq_bytes_t){ r->copy, r->len }, rec, 2, &nrec) == MOQ_OK);
            MOQ_TEST_CHECK_EQ_U64(nrec, 1);
            MOQ_TEST_CHECK_EQ_U64(r->object, 0);
            if (nrec == 1 && r->group < 3) {
                MOQ_TEST_CHECK_EQ_U64(rec[0].group, r->group);
                MOQ_TEST_CHECK_EQ_U64(rec[0].object, 0);
                MOQ_TEST_CHECK_EQ_U64(rec[0].media_time_ms, media_ms[r->group]);
            }
        }
    }
    int sap_last = tlc_find(&x, TLC_OBJ, TLC_SAP, true);
    if (sap_last >= 0 && mt) {
        MOQ_TEST_CHECK_EQ_U64(x.log[sap_last].group, 2);
    } else if (sap_last >= 0) {
        const tlc_rec_t *r = &x.log[sap_last];
        MOQ_TEST_CHECK_EQ_U64(r->group, 2);
        MOQ_TEST_CHECK_EQ_U64(r->object, 0);
        MOQ_TEST_CHECK(!r->truncated && r->len == sizeof(TLC_SAP_ALL) - 1 &&
                       memcmp(r->copy, TLC_SAP_ALL, r->len) == 0);
        if (r->len != sizeof(TLC_SAP_ALL) - 1 || memcmp(r->copy, TLC_SAP_ALL, r->len))
            fprintf(stderr, "  last timeline object: %.*s\n", (int)r->len,
                    (const char *)r->copy);
    }
    MOQ_TEST_CHECK_EQ_INT(tlc_count(&x, TLC_STATUS, TLC_SAP), 1);
    int sap_end = tlc_find(&x, TLC_STATUS, TLC_SAP, false);
    MOQ_TEST_CHECK(sap_end > sap_last && sap_last >= 0);
    if (sap_end >= 0)
        MOQ_TEST_CHECK_EQ_INT(x.log[sap_end].status, MOQ_OBJECT_END_OF_TRACK);

    /* The terminal catalog is cut no earlier than the pump that sent the later
     * of the two ends. Within one pump the streams' arrival order is the
     * transport's (it schedules by priority), so pumps are compared. Exactly
     * one terminal catalog, and every catalog Object parsed. */
    int term = tlc_first_complete_catalog(&x);
    MOQ_TEST_CHECK(term >= 0 && v_end >= 0 && sap_end >= 0);
    if (term >= 0 && v_end >= 0 && sap_end >= 0) {
        MOQ_TEST_CHECK(x.log[term].pass >= x.log[v_end].pass);
        MOQ_TEST_CHECK(x.log[term].pass >= x.log[sap_end].pass);
    }
    int ncomplete = 0;
    for (int i = 0; i < x.n; i++) {
        ncomplete += x.log[i].kind == TLC_OBJ && x.log[i].cat_complete;
        MOQ_TEST_CHECK(!x.log[i].cat_bad);
    }
    MOQ_TEST_CHECK_EQ_INT(ncomplete, 1);

    /* Each subscription ends once, TRACK_ENDED, no earlier than its track's
     * END_OF_TRACK (the facade's end owes it); the catalog subscription
     * itself stays. */
    MOQ_TEST_CHECK_EQ_INT(tlc_count(&x, TLC_DONE, TLC_V), 1);
    MOQ_TEST_CHECK_EQ_INT(tlc_count(&x, TLC_DONE, TLC_SAP), 1);
    MOQ_TEST_CHECK_EQ_INT(tlc_count(&x, TLC_DONE, TLC_CAT), 0);
    MOQ_TEST_CHECK_EQ_INT(tlc_count(&x, TLC_DONE, TLC_A), mode == OTHER_HELD);
    for (int i = 0; i < x.n; i++)
        if (x.log[i].kind == TLC_DONE) {
            MOQ_TEST_CHECK_EQ_INT(x.log[i].status, (int)MOQ_PUB_DONE_TRACK_ENDED);
            int e = tlc_find(&x, TLC_STATUS, x.log[i].track, false);
            MOQ_TEST_CHECK(e >= 0 && x.log[i].pass >= x.log[e].pass);
        }
    MOQ_TEST_CHECK(!moq_media_sender_test_has_pending_output(x.s));

    /* Non-vacuity of the pressure: under a bounded ring the completion took
     * several pumps; the media and the timeline did not all fit in the first. */
    if (max_actions) {
        int in_first = 0;
        for (int i = 0; i < x.n; i++)
            in_first += x.log[i].pass == from_pass + 1 &&
                        (x.log[i].track == TLC_V || x.log[i].track == TLC_SAP);
        MOQ_TEST_CHECK(in_first < 4 + sap_n + 2);
    }
    MOQ_TEST_CHECK(!x.overflow && !x.unexpected);
    for (int i = 0; i < x.n; i++) MOQ_TEST_CHECK(x.log[i].track >= 0);
    {
        moq_media_sender_stats_t st;
        MOQ_TEST_CHECK(moq_media_sender_get_stats(x.s, &st, sizeof(st)) == MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)st.completion_error, (int)MOQ_OK);
    }
    if (failures != before)
        for (int i = 0; i < x.n; i++)
            fprintf(stderr, "   [%d] pass=%d kind=%d track=%d %llu:%llu st=%d cc=%d len=%zu\n", i,
                    x.log[i].pass, (int)x.log[i].kind, x.log[i].track,
                    (unsigned long long)x.log[i].group, (unsigned long long)x.log[i].object,
                    x.log[i].status, (int)x.log[i].cat_complete, x.log[i].len);

out:
    if (x.s) moq_media_sender_test_free(x.s);
    if (x.sp) {
        moq_event_t ev;
        while (moq_session_poll_events(moq_simpair_client(x.sp), &ev, 1) == 1)
            moq_event_cleanup(&ev);
        while (moq_session_poll_events(moq_simpair_server(x.sp), &ev, 1) == 1)
            moq_event_cleanup(&ev);
        moq_simpair_destroy(x.sp);
    }
    MOQ_TEST_CHECK(as.balance == 0);
    int nf = failures - before;
    if (nf == 0) printf("PASS: %s\n", label);
    else         printf("FAIL: %s (%d diagnostics)\n", label, nf);
}

#ifdef MOQ_TEST_HAVE_MPEG2TS
/* A sender allocator that refuses every request while armed. */
static bool g_tsf_refuse;
static void *tsf_alloc(size_t n, void *ctx) { (void)ctx; return g_tsf_refuse ? NULL : malloc(n); }
static void *tsf_realloc(void *p, size_t o, size_t n, void *ctx)
{
    (void)o; (void)ctx;
    return g_tsf_refuse ? NULL : realloc(p, n);
}
static void tsf_free(void *p, size_t n, void *ctx) { (void)n; (void)ctx; free(p); }
static const moq_alloc_t g_tsf_alloc = { NULL, tsf_alloc, tsf_realloc, tsf_free };

/* A failed completion on one track does not hide another track's timeline
 * output. A byte-stream MPEG2TS track's completion fails (its source's last
 * Object cannot be copied): no end and no terminal catalog follow. The video
 * track's media went out while its SAP timeline had no subscriber, so its
 * records are kept for a joiner -- not work. A subscriber then joins the
 * timeline over a bounded action ring: until every record has gone out, a
 * write refused for room included, there is work; then none. `video_ended`:
 * video was ended (end_track) before complete(), so once caught up its
 * timeline would be owed its completion end -- which a failed completion
 * no longer sends, and so is no work either. */
static void test_failed_ts_timeline(uint32_t max_actions, bool video_ended)
{
    int before = failures;
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    tlc_t x;
    memset(&x, 0, sizeof(x));
    uint64_t now = 1000;
    char label[96];
    snprintf(label, sizeof(label), "pacing_failed_ts_timeline%s_act%u",
             video_ended ? "_video_ended" : "", (unsigned)max_actions);
    bool held_seen = false;

    x.sp = tlc_pair(&alloc, max_actions);
    if (!x.sp) { failures++; goto out; }
    g_ns[0] = MOQ_BYTES_LITERAL("svc");
    g_ns[1] = MOQ_BYTES_LITERAL("tsf");
    moq_media_sender_cfg_t cfg;
    moq_media_sender_cfg_init_live_sized(&cfg, sizeof(cfg));
    cfg.namespace_ = (moq_namespace_t){ g_ns, 2 };
    cfg.catalog_refresh_interval_us = UINT64_MAX;
    moq_media_sender_test_set_alloc(&g_tsf_alloc);
    x.s = moq_media_sender_test_new_cfg(&cfg);
    moq_media_sender_test_set_alloc(NULL);
    if (!x.s) { failures++; goto out; }
    /* The TS track first: its completion step runs before video's end. */
    moq_msf_mpeg2ts_t rec;
    moq_msf_mpeg2ts_init(&rec);
    rec.has_mode = true;
    rec.mode = MOQ_MSF_MPEG2TS_MODE_UNMODIFIED_PROGRAM;
    rec.has_packet_size = true;
    rec.packet_size = 188;
    moq_msf_track_ext_t ext = { .kind = MOQ_MSF_EXT_MPEG2TS, .u = { .mpeg2ts = &rec } };
    moq_media_track_cfg_t tc;
    moq_media_track_cfg_init_sized(&tc, sizeof(tc));
    tc.name = MOQ_BYTES_LITERAL("ts");
    tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
    tc.packaging = MOQ_MEDIA_PACKAGING_MPEG2TS;
    tc.bitrate = 3000000;
    tc.extensions = &ext;
    tc.extension_count = 1;
    tc.mpeg2ts_object_packets = 3;
    moq_media_track_t *ts = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_add_track(x.s, &tc, &ts), (int)MOQ_OK);
    moq_media_track_cfg_init(&tc);
    tc.name = MOQ_BYTES_LITERAL("v");
    tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
    tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
    tc.codec = MOQ_BYTES_LITERAL("av01");
    tc.bitrate = 1500000;
    tc.timescale = 1000000;
    tc.is_live = true;
    tc.emit_sap_timeline = true;
    tc.sap_timeline_history_groups = 8;
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_add_track(x.s, &tc, &x.v), (int)MOQ_OK);
    if (!ts || !x.v) { failures++; goto out; }
    for (int c = 0; c < 16 && !moq_media_sender_is_ready(x.s); c++) tlc_drive(&x, now);
    MOQ_TEST_CHECK(moq_media_sender_is_ready(x.s));
    MOQ_TEST_CHECK(tlc_subscribe(&x, TLC_CAT, MOQ_MSF_CATALOG_TRACK_NAME, now));
    MOQ_TEST_CHECK(tlc_subscribe(&x, TLC_V, "v", now));

    /* Video's media goes out; its timeline, with no subscriber, keeps the
     * four records. */
    MOQ_TEST_CHECK(tlc_write(&x, 0x60, true,  MOQ_SAP_TYPE_1, 0)     == MOQ_OK);
    MOQ_TEST_CHECK(tlc_write(&x, 0x61, true,  MOQ_SAP_TYPE_1, 33000) == MOQ_OK);
    MOQ_TEST_CHECK(tlc_write(&x, 0x62, false, MOQ_SAP_TYPE_3, 50000) == MOQ_OK);
    MOQ_TEST_CHECK(tlc_write(&x, 0x63, true,  MOQ_SAP_TYPE_1, 66600) == MOQ_OK);
    if (video_ended)
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_end_track(x.s, x.v), (int)MOQ_OK);
    for (int d = 0; d < 32 && (tlc_count(&x, TLC_OBJ, TLC_V) < 4 ||
                               tlc_count(&x, TLC_STATUS, TLC_V) < (video_ended ? 1 : 0));
         d++)
        tlc_drive(&x, now);
    MOQ_TEST_CHECK_EQ_INT(tlc_count(&x, TLC_OBJ, TLC_V), 4);
    MOQ_TEST_CHECK_EQ_INT(tlc_count(&x, TLC_STATUS, TLC_V), video_ended ? 1 : 0);
    /* Four TS packets: one whole three-packet Object, and one packet the
     * packetizer still holds until its source is finished. */
    {
        ts_gen_t g;
        ts_gen_init(&g, 188, 1, false);
        static uint8_t gop[16 * 188];
        size_t n = ts_gen_gop(&g, gop, 5);
        MOQ_TEST_CHECK(n >= 4 * 188);
        n = 4 * 188;
        moq_media_send_mpeg2ts_t in;
        memset(&in, 0, sizeof(in));
        in.struct_size = sizeof(in);
        in.data = (moq_bytes_t){ gop, n };
        size_t consumed = 0;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_write_mpeg2ts(x.s, ts, &in, &consumed),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(consumed, n);
    }
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_complete(x.s), (int)MOQ_OK);

    /* The pump that finishes the TS source cannot copy its last Object. */
    g_tsf_refuse = true;
    tlc_drive(&x, now);
    g_tsf_refuse = false;
    MOQ_TEST_CHECK(moq_media_sender_test_complete_failed(x.s));
    MOQ_TEST_CHECK(!moq_media_sender_is_fatal(x.s));
    {
        moq_media_sender_stats_t st;
        MOQ_TEST_CHECK(moq_media_sender_get_stats(x.s, &st, sizeof(st)) == MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)st.completion_error, (int)MOQ_ERR_NOMEM);
    }
    for (int d = 0; d < 8; d++) tlc_drive(&x, now);
    /* Kept records with no subscriber, a failed completion, retained TS
     * media: nothing is work. */
    MOQ_TEST_CHECK(!moq_media_sender_test_has_pending_output(x.s));
    int quiet_at = x.n;

    /* A subscriber joins the timeline: its records are output. */
    moq_subscribe_cfg_t sc; moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ g_ns, 2 };
    sc.track_name = MOQ_BYTES_LITERAL("v.sap");
    sc.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    sc.has_forward = true; sc.forward = true;
    int ok0 = x.sub_ok;
    MOQ_TEST_CHECK(moq_session_subscribe(moq_simpair_server(x.sp), &sc, now,
                                         &x.sub[TLC_SAP]) == MOQ_OK);
    bool all_out = false;
    for (int d = 0; d < 64 && !all_out; d++) {
        tlc_drive(&x, now);
        int last = tlc_find(&x, TLC_OBJ, TLC_SAP, true);
        all_out = last >= 0 && x.log[last].group == 2 &&
                  x.log[last].len == sizeof(TLC_SAP_ALL) - 1 &&
                  memcmp(x.log[last].copy, TLC_SAP_ALL, x.log[last].len) == 0;
        bool held = moq_media_sender_test_timeline_held(x.s);
        held_seen |= held;
        /* Once the subscription is established, until the last record is
         * out, there is work. */
        if (!all_out && x.sub_ok > ok0)
            MOQ_TEST_CHECK(moq_media_sender_test_has_pending_output(x.s));
        if (held) MOQ_TEST_CHECK(moq_media_sender_test_has_pending_output(x.s));
    }
    MOQ_TEST_CHECK(all_out);
    /* These rings refuse a timeline write mid-way (non-vacuity). */
    if (max_actions >= 2 && max_actions <= 8) MOQ_TEST_CHECK(held_seen);
    for (int d = 0; d < 8; d++) tlc_drive(&x, now);
    MOQ_TEST_CHECK(!moq_media_sender_test_timeline_held(x.s));
    MOQ_TEST_CHECK(!moq_media_sender_test_has_pending_output(x.s));
    /* No completion end and no terminal catalog: the completion failed. */
    MOQ_TEST_CHECK_EQ_INT(tlc_count(&x, TLC_STATUS, TLC_V), video_ended ? 1 : 0);
    MOQ_TEST_CHECK_EQ_INT(tlc_count(&x, TLC_STATUS, TLC_SAP), 0);
    MOQ_TEST_CHECK_EQ_INT(tlc_first_complete_catalog(&x), -1);
    for (int i = 0; i < quiet_at; i++) MOQ_TEST_CHECK(x.log[i].track != TLC_SAP);
    MOQ_TEST_CHECK(!x.overflow && !x.unexpected);

out:
    g_tsf_refuse = false;
    if (x.s) moq_media_sender_test_free(x.s);
    if (x.sp) {
        moq_event_t ev;
        while (moq_session_poll_events(moq_simpair_client(x.sp), &ev, 1) == 1)
            moq_event_cleanup(&ev);
        while (moq_session_poll_events(moq_simpair_server(x.sp), &ev, 1) == 1)
            moq_event_cleanup(&ev);
        moq_simpair_destroy(x.sp);
    }
    MOQ_TEST_CHECK(as.balance == 0);
    int nf = failures - before;
    if (nf == 0) printf("PASS: %s (held %s)\n", label, held_seen ? "seen" : "not seen");
    else         printf("FAIL: %s (%d diagnostics)\n", label, nf);
}
#endif

/* The terminal catalog itself cannot be built. Video and audio ended
 * (end_track) and their media and ends went out, so complete() owes no
 * further end: the next pump finalizes and stages the terminal generation,
 * whose build's first allocation is refused (a one-shot seam at the build:
 * a pump-wide refusal would stop the removed tracks' teardown first, which
 * retries quietly). `mode`: 0 that failure; 1 the healthy control (the
 * terminal catalog commits); 2 the healthy commit, then a periodic catalog
 * refresh whose build is refused the same way -- a failure after the
 * completion finished, which is not the completion's. */
static void test_terminal_catalog_failure(int mode)
{
    static const char *const names[] = { "fails", "commits", "commits_then_refresh_fails" };
    int before = failures;
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    tlc_t x;
    memset(&x, 0, sizeof(x));
    uint64_t now = 1000;
    char label[96];
    snprintf(label, sizeof(label), "pacing_terminal_catalog_%s", names[mode]);

    x.sp = tlc_pair(&alloc, 0);
    if (!x.sp) { failures++; goto out; }
    g_ns[0] = MOQ_BYTES_LITERAL("svc");
    g_ns[1] = MOQ_BYTES_LITERAL("tcf");
    moq_media_sender_cfg_t cfg;
    moq_media_sender_cfg_init_live_sized(&cfg, sizeof(cfg));
    cfg.namespace_ = (moq_namespace_t){ g_ns, 2 };
    cfg.catalog_refresh_interval_us = mode == 2 ? 2000000 : UINT64_MAX;
    x.s = moq_media_sender_test_new_cfg(&cfg);
    if (!x.s) { failures++; goto out; }
    moq_media_track_cfg_t tc;
    moq_media_track_cfg_init(&tc);
    tc.name = MOQ_BYTES_LITERAL("v");
    tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
    tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
    tc.codec = MOQ_BYTES_LITERAL("av01");
    tc.bitrate = 1500000;
    tc.is_live = true;
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_add_track(x.s, &tc, &x.v), (int)MOQ_OK);
    x.a = add_track(x.s, "a", MOQ_MEDIA_TYPE_AUDIO);
    if (!x.v || !x.a) { failures++; goto out; }
    for (int c = 0; c < 16 && !moq_media_sender_is_ready(x.s); c++) tlc_drive(&x, now);
    MOQ_TEST_CHECK(tlc_subscribe(&x, TLC_CAT, MOQ_MSF_CATALOG_TRACK_NAME, now));
    MOQ_TEST_CHECK(tlc_subscribe(&x, TLC_V, "v", now));
    MOQ_TEST_CHECK(tlc_subscribe(&x, TLC_A, "a", now));
    MOQ_TEST_CHECK(tlc_write(&x, 0x60, true, MOQ_SAP_TYPE_1, 0) == MOQ_OK);
    MOQ_TEST_CHECK(tlc_write(&x, 0x61, true, MOQ_SAP_TYPE_1, 33000) == MOQ_OK);
    MOQ_TEST_CHECK(tlc_write_to(&x, x.a, 0x70, true, false, MOQ_SAP_NONE, 10000) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_end_track(x.s, x.v), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_end_track(x.s, x.a), (int)MOQ_OK);
    for (int d = 0; d < 32 && (tlc_count(&x, TLC_STATUS, TLC_V) < 1 ||
                               tlc_count(&x, TLC_STATUS, TLC_A) < 1); d++)
        tlc_drive(&x, now);
    MOQ_TEST_CHECK_EQ_INT(tlc_count(&x, TLC_OBJ, TLC_V), 2);
    MOQ_TEST_CHECK_EQ_INT(tlc_count(&x, TLC_OBJ, TLC_A), 1);
    MOQ_TEST_CHECK_EQ_INT(tlc_count(&x, TLC_STATUS, TLC_V), 1);
    MOQ_TEST_CHECK_EQ_INT(tlc_count(&x, TLC_STATUS, TLC_A), 1);
    MOQ_TEST_CHECK(!moq_media_sender_test_has_pending_output(x.s));

    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_complete(x.s), (int)MOQ_OK);
    if (mode == 0) moq_media_sender_test_refuse_catalog_build(x.s);
    for (int d = 0; d < 8; d++) tlc_drive(&x, now);

    moq_media_sender_stats_t st;
    MOQ_TEST_CHECK(moq_media_sender_get_stats(x.s, &st, sizeof(st)) == MOQ_OK);
    if (mode == 0) {
        /* The existing policy: an unbuildable catalog is the encode fatal.
         * No terminal catalog; the completion's exact failure, kept. */
        MOQ_TEST_CHECK(moq_media_sender_is_fatal(x.s));
        MOQ_TEST_CHECK_EQ_U64(moq_media_sender_fatal_code(x.s),
                              MOQ_MEDIA_SENDER_FATAL_CATALOG_ENCODE);
        MOQ_TEST_CHECK_EQ_INT(tlc_first_complete_catalog(&x), -1);
        MOQ_TEST_CHECK_EQ_INT((int)st.completion_error, (int)MOQ_ERR_NOMEM);
        MOQ_TEST_CHECK_EQ_INT((int)st.last_error, (int)MOQ_ERR_NOMEM);
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_complete(x.s), (int)MOQ_ERR_CLOSED);
        moq_media_sender_stats_t st2;
        MOQ_TEST_CHECK(moq_media_sender_get_stats(x.s, &st2, sizeof(st2)) == MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)st2.completion_error, (int)MOQ_ERR_NOMEM);
    } else {
        MOQ_TEST_CHECK(!moq_media_sender_is_fatal(x.s));
        MOQ_TEST_CHECK(tlc_first_complete_catalog(&x) >= 0);
        MOQ_TEST_CHECK_EQ_INT((int)st.completion_error, (int)MOQ_OK);
    }
    if (mode == 2) {
        /* After the commit: a refresh is due, and its build is refused. */
        int catalogs = tlc_count(&x, TLC_OBJ, TLC_CAT);
        now += 3000000;
        moq_media_sender_test_refuse_catalog_build(x.s);
        tlc_drive(&x, now);
        for (int d = 0; d < 4; d++) tlc_drive(&x, now);
        /* The refresh did fail (non-vacuity): the existing encode fatal. */
        MOQ_TEST_CHECK(moq_media_sender_is_fatal(x.s));
        MOQ_TEST_CHECK_EQ_U64(moq_media_sender_fatal_code(x.s),
                              MOQ_MEDIA_SENDER_FATAL_CATALOG_ENCODE);
        MOQ_TEST_CHECK_EQ_INT(tlc_count(&x, TLC_OBJ, TLC_CAT), catalogs);
        moq_media_sender_stats_t st3;
        MOQ_TEST_CHECK(moq_media_sender_get_stats(x.s, &st3, sizeof(st3)) == MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)st3.completion_error, (int)MOQ_OK);
    }
    MOQ_TEST_CHECK(!x.overflow && !x.unexpected);

out:
    if (x.s) moq_media_sender_test_free(x.s);
    if (x.sp) {
        moq_event_t ev;
        while (moq_session_poll_events(moq_simpair_client(x.sp), &ev, 1) == 1)
            moq_event_cleanup(&ev);
        while (moq_session_poll_events(moq_simpair_server(x.sp), &ev, 1) == 1)
            moq_event_cleanup(&ev);
        moq_simpair_destroy(x.sp);
    }
    MOQ_TEST_CHECK(as.balance == 0);
    int nf = failures - before;
    if (nf == 0) printf("PASS: %s\n", label);
    else         printf("FAIL: %s (%d diagnostics)\n", label, nf);
}

/* ===================================================================== *
 *  A drop evicts the group whose first object the facade still holds.
 *
 *  The constrained ring refuses the first object of group 1 with
 *  MOQ_ERR_WOULD_BLOCK, so the facade retains that write for a retry. Before
 *  the retry, two newer keyframe groups overflow the queue and the drop policy
 *  evicts group 1. media_sender.h: a drop policy evicts whole groups and
 *  abandons the open subgroup on the wire if it was mid-send; it never makes
 *  the sender fatal. draft-18 11.4.3: a subgroup closed before all of its
 *  objects were delivered MUST be reset, never closed with a FIN.
 * ===================================================================== */
static void test_drop_blocked_group(moq_media_send_backpressure_t policy,
                                    moq_version_t version, bool header_only,
                                    const char *name)
{
    int before = failures;
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    fix_t fix;
    uint64_t now = 1000;
    enum { QCAP = 2 };
    /* Four actions allow g0's header + two object actions + FIN, but no g1
     * header. Five also open g1, then refuse its two-action object write. */
    uint32_t acts = header_only ? 5u : 4u;
    if (!fix_init_policy_act(&fix, &alloc, now, QCAP, acts, policy, version)) {
        failures++; goto out;
    }
    if (!subscribe_track(&fix, 0, "v", now)) { failures++; goto out; }
    set_phase(&fix, PHASE_PACE);

    int from = fix.tr.n;
    int headers_before = g_subgroup_headers;
    MOQ_TEST_CHECK(write_obj(&fix, 0, 0xd0, true, true, 1000) == MOQ_OK);
    MOQ_TEST_CHECK(write_obj(&fix, 0, 0xd1, true, true, 2000) == MOQ_OK);
    drive(&fix, now);
    /* Group 0 went out; group 1 was refused by the session, not dropped. */
    expect_conserved(fix.s, 2, 1, 1, 0, PAYLOAD_LEN, "blkdrop-g1-refused");
    MOQ_TEST_CHECK_EQ_INT(fix.tr.n - from, 1);
    MOQ_TEST_CHECK_EQ_INT(g_subgroup_headers - headers_before,
                          header_only ? 2 : 1);
    MOQ_TEST_CHECK_EQ_INT(fix.reset_n, 0);
    MOQ_TEST_CHECK(!moq_media_sender_is_fatal(fix.s));

    /* Only the first control object has reached the peer. Capture its group
     * before eviction or any observation of the target group's reset. MSF on
     * draft-22 seeds groups from an epoch base; this checks relative identity,
     * not that wall-clock seed. Legacy drafts retain the absolute-zero oracle.
     * Group numbers below are offsets from this one stable anchor. */
    if (fix.tr.n - from != 1) goto out;
    uint64_t group_base = version == MOQ_VERSION_DRAFT_22
        ? fix.tr.recs[from].group : 0;
    MOQ_TEST_CHECK(group_base <= MOQ_QUIC_VARINT_MAX - 4u);
    if (group_base > MOQ_QUIC_VARINT_MAX - 4u) goto out;
    {
        const tr_exp_t anchor[1] = { { 0, group_base, 0, 0xd0, 0 } };
        expect_from(&fix, from, anchor, 1, "blkdrop-base-anchor");
    }

    /* Two newer keyframe groups overflow the bound of two: the oldest group,
     * group 1, is evicted whole and the newest sync point is kept. */
    MOQ_TEST_CHECK(write_obj(&fix, 0, 0xd2, true, true, 3000) == MOQ_OK);
    MOQ_TEST_CHECK(write_obj(&fix, 0, 0xd3, true, true, 4000) == MOQ_OK);
    {
        drop_snap_t d; drop_snap(fix.s, &d);
        MOQ_TEST_CHECK_EQ_U64(d.dropped, 1);
        MOQ_TEST_CHECK_EQ_U64(d.keyframes, 1);
        MOQ_TEST_CHECK_EQ_U64(d.groups, 1);
    }
    expect_conserved(fix.s, 4, 1, 2, 1, 2 * PAYLOAD_LEN, "blkdrop-g1-evicted");

    /* Capacity returns over later hooks; a few more prove nothing repeats. */
    for (int i = 0; i < 16; i++) {
        moq_media_sender_stats_t st; memset(&st, 0, sizeof(st));
        MOQ_TEST_CHECK(moq_media_sender_get_stats(fix.s, &st, sizeof(st))
                       == MOQ_OK);
        if (st.objects_queued == 0 || moq_media_sender_is_fatal(fix.s)) break;
        drive(&fix, now);
    }
    for (int i = 0; i < 4; i++) drive(&fix, now);

    /* The drop killed neither the sender nor the session. */
    MOQ_TEST_CHECK(!moq_media_sender_is_fatal(fix.s));
    MOQ_TEST_CHECK_EQ_U64(moq_media_sender_fatal_code(fix.s), 0);
    MOQ_TEST_CHECK(moq_session_state(moq_simpair_client(fix.sp)) ==
                   MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK(moq_session_state(moq_simpair_server(fix.sp)) ==
                   MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK(moq_media_sender_track_has_subscriber(fix.s, fix.t[0]));
    expect_conserved(fix.s, 4, 3, 0, 1, 0, "blkdrop-drained");

    /* The publication keeps going: a later group is accepted and delivered. */
    MOQ_TEST_CHECK(write_obj(&fix, 0, 0xd4, true, true, 5000) == MOQ_OK);
    for (int i = 0; i < 8; i++) {
        moq_media_sender_stats_t st; memset(&st, 0, sizeof(st));
        MOQ_TEST_CHECK(moq_media_sender_get_stats(fix.s, &st, sizeof(st))
                       == MOQ_OK);
        if (st.objects_queued == 0 || moq_media_sender_is_fatal(fix.s)) break;
        drive(&fix, now);
    }
    MOQ_TEST_CHECK(!moq_media_sender_is_fatal(fix.s));
    expect_conserved(fix.s, 5, 4, 0, 1, 0, "blkdrop-later-group");

    /* Every delivered object, exactly, in order: group 1 never appears, and
     * the groups after it keep their own ids. */
    {
        const tr_exp_t exp[4] = { { 0, group_base, 0, 0xd0, 0 },
                                  { 0, group_base + 2, 0, 0xd2, 2 },
                                  { 0, group_base + 3, 0, 0xd3, 3 },
                                  { 0, group_base + 4, 0, 0xd4, 4 } };
        expect_from(&fix, from, exp, 4, "blkdrop-media");
    }
    /* Complete groups 0, 2 and 3 FIN; group 4 is still open. Group 1 never
     * FINs: its only object was dropped before it was delivered. */
    {
        const term_exp_t terms[3] = { { 0, group_base, 0, true, true },
                                      { 0, group_base + 2, 0, true, true },
                                      { 0, group_base + 3, 0, true, true } };
        expect_terminals(&fix, terms, 3, "blkdrop-terminals");
    }
    /* An opened header-only subgroup must reset. In this library's unopened
     * local-operation case there is no stream to reset, so expect none. */
    {
        const reset_exp_t rexp[1] = {
            { 0, group_base + 1, 0, 0x10, true, .sub_opaque = 0, .pub_opaque = 0 } };
        expect_resets(&fix, rexp, header_only ? 1 : 0, "blkdrop-resets");
        moq_media_sender_stats_t st = {0};
        moq_media_sender_track_stats_t ts = {0};
        MOQ_TEST_CHECK(moq_media_sender_get_stats(fix.s, &st, sizeof(st)) == MOQ_OK);
        MOQ_TEST_CHECK(moq_media_sender_get_track_stats(fix.s, fix.t[0], &ts,
                                                       sizeof(ts)) == MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(st.groups_abandoned, header_only ? 1u : 0u);
        MOQ_TEST_CHECK_EQ_U64(ts.groups_abandoned, header_only ? 1u : 0u);
    }

out:
    fix_free(&fix);
    MOQ_TEST_CHECK(as.balance == 0);
    report(&fix, name, before);
}

/* Same, when the facade holds a write in the MIDDLE of group 1: its first
 * object was delivered, so the evicted tail leaves a subgroup on the wire that
 * draft-18 11.4.3 requires to be reset, not FINed truncated. */
static void test_drop_blocked_tail(moq_media_send_backpressure_t policy,
                                   moq_version_t version,
                                   const char *name)
{
    int before = failures;
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    fix_t fix;
    uint64_t now = 1000;
    enum { QCAP = 2, ACTS = 4 };
    if (!fix_init_policy_act(&fix, &alloc, now, QCAP, ACTS, policy, version)) {
        failures++; goto out;
    }
    if (!subscribe_track(&fix, 0, "v", now)) { failures++; goto out; }
    set_phase(&fix, PHASE_PACE);

    int from = fix.tr.n;
    MOQ_TEST_CHECK(write_obj(&fix, 0, 0xe8, true, true, 1000) == MOQ_OK);
    drive(&fix, now);
    /* Anchor to the first delivered control BEFORE writing the target group,
     * never to that group's delivered object or RESET. As above, draft-22's
     * absolute MSF wall-clock seed is outside this pacing proof. */
    MOQ_TEST_CHECK_EQ_INT(fix.tr.n - from, 1);
    if (fix.tr.n - from != 1) goto out;
    uint64_t group_base = version == MOQ_VERSION_DRAFT_22
        ? fix.tr.recs[from].group : 0;
    MOQ_TEST_CHECK(group_base <= MOQ_QUIC_VARINT_MAX - 3u);
    if (group_base > MOQ_QUIC_VARINT_MAX - 3u) goto out;
    {
        const tr_exp_t anchor[1] = { { 0, group_base, 0, 0xe8, 0 } };
        expect_from(&fix, from, anchor, 1, "blktail-base-anchor");
    }
    MOQ_TEST_CHECK(write_obj(&fix, 0, 0xe9, true, true, 2000) == MOQ_OK);
    MOQ_TEST_CHECK(write_obj(&fix, 0, 0xea, false, false, 2500) == MOQ_OK);
    drive(&fix, now);
    /* Group 1's first object went out; its second was refused, not dropped. */
    expect_conserved(fix.s, 3, 2, 1, 0, PAYLOAD_LEN, "blktail-g1o1-refused");
    MOQ_TEST_CHECK_EQ_INT(fix.tr.n - from, 2);
    MOQ_TEST_CHECK_EQ_INT(fix.reset_n, 0);
    MOQ_TEST_CHECK(!moq_media_sender_is_fatal(fix.s));

    /* Two newer keyframe groups overflow the bound: group 1's queued tail is
     * the oldest group and is evicted whole. */
    MOQ_TEST_CHECK(write_obj(&fix, 0, 0xeb, true, true, 3000) == MOQ_OK);
    MOQ_TEST_CHECK(write_obj(&fix, 0, 0xec, true, true, 4000) == MOQ_OK);
    {
        drop_snap_t d; drop_snap(fix.s, &d);
        MOQ_TEST_CHECK_EQ_U64(d.dropped, 1);
        MOQ_TEST_CHECK_EQ_U64(d.keyframes, 0);
        MOQ_TEST_CHECK_EQ_U64(d.groups, 1);
    }
    expect_conserved(fix.s, 5, 2, 2, 1, 2 * PAYLOAD_LEN, "blktail-evicted");

    for (int i = 0; i < 16; i++) {
        moq_media_sender_stats_t st; memset(&st, 0, sizeof(st));
        MOQ_TEST_CHECK(moq_media_sender_get_stats(fix.s, &st, sizeof(st))
                       == MOQ_OK);
        if (st.objects_queued == 0 || moq_media_sender_is_fatal(fix.s)) break;
        drive(&fix, now);
    }
    for (int i = 0; i < 4; i++) drive(&fix, now);

    MOQ_TEST_CHECK(!moq_media_sender_is_fatal(fix.s));
    MOQ_TEST_CHECK_EQ_U64(moq_media_sender_fatal_code(fix.s), 0);
    MOQ_TEST_CHECK(moq_session_state(moq_simpair_client(fix.sp)) ==
                   MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK(moq_session_state(moq_simpair_server(fix.sp)) ==
                   MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK(moq_media_sender_track_has_subscriber(fix.s, fix.t[0]));
    expect_conserved(fix.s, 5, 4, 0, 1, 0, "blktail-drained");
    {
        const tr_exp_t exp[4] = { { 0, group_base, 0, 0xe8, 0 },
                                  { 0, group_base + 1, 0, 0xe9, 1 },
                                  { 0, group_base + 2, 0, 0xeb, 3 },
                                  { 0, group_base + 3, 0, 0xec, 4 } };
        expect_from(&fix, from, exp, 4, "blktail-media");
    }
    /* Group 1 is reset exactly once and never FINs; complete groups 0 and 2
     * FIN; group 3 is still open. */
    {
        const reset_exp_t rexp[1] = {
            { 0, group_base + 1, 0, 0x10, true, .sub_opaque = 0, .pub_opaque = 0 } };
        expect_resets(&fix, rexp, 1, "blktail-reset");
        const term_exp_t terms[2] = { { 0, group_base, 0, true, true },
                                      { 0, group_base + 2, 0, true, true } };
        expect_terminals(&fix, terms, 2, "blktail-terminals");
    }
    {
        drop_snap_t d; drop_snap(fix.s, &d);
        MOQ_TEST_CHECK_EQ_U64(d.abandoned, 1);
        moq_media_sender_track_stats_t ts = {0};
        MOQ_TEST_CHECK(moq_media_sender_get_track_stats(fix.s, fix.t[0], &ts,
                                                       sizeof(ts)) == MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(ts.groups_abandoned, 1);
    }

out:
    fix_free(&fix);
    MOQ_TEST_CHECK(as.balance == 0);
    report(&fix, name, before);
}


/* Direct public removal BEFORE any eviction must release the facade's held
 * write before END_OF_TRACK. Shapes: unopened, header-only, mid-group tail.
 * This ownership gap predates PR63; it is not attributed to its contributor. */
static void test_remove_blocked_write(moq_version_t version, int shape)
{
    int before = failures;
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    fix_t fix;
    const uint64_t now = 1000;
    char name[80];
    snprintf(name, sizeof(name), "pacing_remove_blocked_d%u_shape%d",
             (unsigned)version, shape);
    if (!fix_init_policy_act(&fix, &alloc, now, 2, shape == 1 ? 5 : 4,
                             MOQ_MEDIA_SEND_BP_DROP_GROUP, version)) {
        failures++; goto out;
    }
    if (!subscribe_track(&fix, 0, "v", now)) { failures++; goto out; }
    set_phase(&fix, PHASE_PACE);
    int from = fix.tr.n;
    int headers = g_subgroup_headers;
    MOQ_TEST_CHECK(write_obj(&fix, 0, 0xf0, true, true, 1000) == MOQ_OK);
    if (shape == 2) drive(&fix, now);
    MOQ_TEST_CHECK(write_obj(&fix, 0, 0xf1, true, true, 2000) == MOQ_OK);
    if (shape == 2)
        MOQ_TEST_CHECK(write_obj(&fix, 0, 0xf2, false, false, 2500) == MOQ_OK);
    drive(&fix, now);
    int sent = shape == 2 ? 2 : 1;
    expect_conserved(fix.s, (uint64_t)sent + 1, (uint64_t)sent, 1, 0,
                     PAYLOAD_LEN, "remove-held-before");
    MOQ_TEST_CHECK_EQ_INT(fix.tr.n - from, sent);
    MOQ_TEST_CHECK_EQ_INT(g_subgroup_headers - headers, shape == 0 ? 1 : 2);
    MOQ_TEST_CHECK_EQ_INT(fix.reset_n, 0);
    MOQ_TEST_CHECK_EQ_INT(fix.done_n, 0);
    MOQ_TEST_CHECK_EQ_U64(moq_media_sender_test_reset_attempts(fix.s), 0);
    /* Require the expected queue state before removal. */
    if (failures != before) goto out;
    uint64_t base = version == MOQ_VERSION_DRAFT_22 ? fix.tr.recs[from].group : 0;
    MOQ_TEST_CHECK(base <= MOQ_QUIC_VARINT_MAX - 2u);
    if (base > MOQ_QUIC_VARINT_MAX - 2u) goto out;
    {
        const tr_exp_t media[2] = { {0, base, 0, 0xf0, 0},
                                    {0, base + 1, 0, 0xf1, 1} };
        expect_from(&fix, from, media, sent, "remove-held-media-before");
    }
    if (failures != before) goto out;
    MOQ_TEST_CHECK(moq_media_sender_remove_track(fix.s, fix.t[0]) == MOQ_OK);
    moq_media_track_cfg_t tc;
    moq_media_track_cfg_init_sized(&tc, sizeof(tc));
    tc.name = MOQ_BYTES_LITERAL("v");
    tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
    tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
    tc.codec = MOQ_BYTES_LITERAL("av01");
    tc.bitrate = 1500000;
    tc.is_live = true;
    moq_media_track_t *replacement = NULL;
    MOQ_TEST_CHECK(moq_media_sender_add_track(fix.s, &tc, &replacement)
                   == MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK(replacement == NULL);
    for (int i = 0; i < 24; i++) drive(&fix, now);
    moq_result_t replace_rc = moq_media_sender_add_track(fix.s, &tc, &replacement);
    moq_media_sender_stats_t st = {0};
    MOQ_TEST_CHECK(moq_media_sender_get_stats(fix.s, &st, sizeof(st)) == MOQ_OK);
    fprintf(stderr, "PR63-REMOVE d%u shape%d done=%d status=%d replacement=%d fatal=%d queued=%llu dropped=%llu resets=%d abandoned=%llu\n",
            (unsigned)version, shape, fix.done_n, fix.stat_n,
            replace_rc == MOQ_OK && replacement != NULL,
            moq_media_sender_is_fatal(fix.s), (unsigned long long)st.objects_queued,
            (unsigned long long)st.objects_dropped, fix.reset_n,
            (unsigned long long)st.groups_abandoned);
    MOQ_TEST_CHECK(replace_rc == MOQ_OK && replacement != NULL);
    MOQ_TEST_CHECK(!moq_media_sender_is_fatal(fix.s));
    expect_conserved(fix.s, (uint64_t)sent + 1, (uint64_t)sent, 0, 1, 0,
                     "remove-held-after");
    MOQ_TEST_CHECK_EQ_U64(st.groups_abandoned, shape == 0 ? 0u : 1u);
    {
        const tr_exp_t media[2] = { {0, base, 0, 0xf0, 0},
                                    {0, base + 1, 0, 0xf1, 1} };
        expect_from(&fix, from, media, sent, "remove-held-media-after");
        const reset_exp_t reset[1] = {
            {0, base + 1, 0, 0x10, true, .sub_opaque = 0, .pub_opaque = 0} };
        expect_resets(&fix, reset, shape == 0 ? 0 : 1, "remove-held-reset");
        const stat_exp_t terminal[1] = { {0, base + 2, 0, MOQ_OBJECT_END_OF_TRACK} };
        expect_status(&fix, terminal, 1, "remove-held-terminal");
        const term_exp_t fins[2] = { {0, base, 0, true, true},
                                     {0, base + 2, 0, false, false} };
        expect_terminals(&fix, fins, 2, "remove-held-fin");
        const done_exp_t done[1] = { {fix.sub[0]._opaque, MOQ_PUB_DONE_TRACK_ENDED} };
        expect_done(&fix, done, 1, "remove-held-done");
    }
    if (shape != 0 && fix.reset_n == 1 && fix.stat_n == 1 && fix.done_n == 1) {
        MOQ_TEST_CHECK(fix.reset[0].seq < fix.stat[0].seq);
        MOQ_TEST_CHECK(fix.reset[0].seq < fix.done_seq[0]);
    }
out:
    fix_free(&fix);
    MOQ_TEST_CHECK(as.balance == 0);
    report(&fix, name, before);
}


int main(void)
{
    test_one_at_a_time();
    test_burst();
    test_alternating();
    test_isolation(false);
    test_isolation(true);
    test_demand_resumes_at_group_start(MOQ_VERSION_DRAFT_16, false);
    test_demand_resumes_at_group_start(MOQ_VERSION_DRAFT_16, true);
    test_demand_resumes_at_group_start(MOQ_VERSION_DRAFT_18, false);
    test_demand_resumes_at_group_start(MOQ_VERSION_DRAFT_18, true);
    test_demand_loss_after_emission(MOQ_VERSION_DRAFT_16);
    test_demand_loss_after_emission(MOQ_VERSION_DRAFT_18);
    test_refusal_recovery();
    test_refusal_after_skip();
    test_end_marker_order();
    test_terminal_stats();
    test_refusal_stops_hook();
    test_drop_group_count();
    test_end_track_bound();
    test_removal_accounting(false);
    test_removal_accounting(true);
    test_drop_group_across_passes();
    test_evict_accounting();
    test_evicted_open_group_closed();
    test_continuation_keeps_own_group();
    test_wait_recheck();
    test_evict_crosspath_group();
    test_no_demand_suffix_dropped();
    test_evict_reset_survives_removal();
    test_reset_retry_blocked();
    test_terminal_flushes_reset();
    test_reset_attempt_once_per_hook(1);
    test_reset_attempt_once_per_hook(2);
    test_reset_partial_fanout(false);
    test_reset_partial_fanout(true);
    /* Every mode, unbounded and across bounded action rings: each ring size
     * refuses a different write first. */
    static const uint32_t rings[] = { 0, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 };
    for (size_t r = 0; r < sizeof(rings) / sizeof(rings[0]); r++)
        for (int m = TL_DEMANDED; m <= OTHER_HELD; m++) {
            test_complete_timeline((tlc_mode_t)m, rings[r], false);
            if (m == TL_DEMANDED)
                test_complete_timeline((tlc_mode_t)m, rings[r], true);
        }
    for (int m = 0; m < 3; m++) test_terminal_catalog_failure(m);
#ifdef MOQ_TEST_HAVE_MPEG2TS
    for (size_t r = 0; r < sizeof(rings) / sizeof(rings[0]); r++)
        for (int e = 0; e < 2; e++) test_failed_ts_timeline(rings[r], e != 0);
#endif
    test_drop_blocked_group(MOQ_MEDIA_SEND_BP_DROP_TO_KEYFRAME,
                            PACING_VERSION, false,
                            "pacing_drop_blocked_group_to_keyframe");
    test_drop_blocked_group(MOQ_MEDIA_SEND_BP_DROP_GROUP,
                            PACING_VERSION, false,
                            "pacing_drop_blocked_group_drop_group");
    test_drop_blocked_tail(MOQ_MEDIA_SEND_BP_DROP_TO_KEYFRAME,
                           PACING_VERSION,
                           "pacing_drop_blocked_tail_to_keyframe");
    test_drop_blocked_tail(MOQ_MEDIA_SEND_BP_DROP_GROUP,
                           PACING_VERSION,
                           "pacing_drop_blocked_tail_drop_group");
    static const moq_version_t blocked_versions[] = {
        MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18, MOQ_VERSION_DRAFT_22 };
    static const moq_media_send_backpressure_t policies[] = {
        MOQ_MEDIA_SEND_BP_DROP_TO_KEYFRAME, MOQ_MEDIA_SEND_BP_DROP_GROUP };
    for (size_t v = 0; v < sizeof(blocked_versions) / sizeof(blocked_versions[0]); v++)
        for (size_t p = 0; p < sizeof(policies) / sizeof(policies[0]); p++) {
            char name[96];
            snprintf(name, sizeof(name), "pacing_drop_header_only_d%u_policy%u",
                     (unsigned)blocked_versions[v], (unsigned)policies[p]);
            test_drop_blocked_group(policies[p], blocked_versions[v], true, name);
            if (blocked_versions[v] == PACING_VERSION) continue;
            snprintf(name, sizeof(name), "pacing_drop_unopened_d%u_policy%u",
                     (unsigned)blocked_versions[v], (unsigned)policies[p]);
            test_drop_blocked_group(policies[p], blocked_versions[v], false, name);
            snprintf(name, sizeof(name), "pacing_drop_tail_d%u_policy%u",
                     (unsigned)blocked_versions[v], (unsigned)policies[p]);
            test_drop_blocked_tail(policies[p], blocked_versions[v], name);
        }
    for (size_t v = 0; v < sizeof(blocked_versions) / sizeof(blocked_versions[0]); v++)
        for (int shape = 0; shape < 3; shape++)
            test_remove_blocked_write(blocked_versions[v], shape);
    if (failures) {
        fprintf(stderr, "FAILURES: %d\n", failures);
        return 1;
    }
    return 0;
}
