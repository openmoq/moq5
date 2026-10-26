/*
 * CONTRACT: endpoint drain accounts for work already accepted by attached
 * services, and permanent completion is ordered per MSF-01 §11.3.
 *
 * The Android finish() contract composes the public sequence
 *
 *     moq_media_sender_complete(sender)
 *     moq_endpoint_drain(endpoint, timeout)
 *
 * and requires the MSF-01 §11.3 terminal catalog (isComplete:true, empty
 * tracks) to have reached the peer. complete() only RECORDS the work and wakes
 * the endpoint; the terminal generation is built by a later sender hook pass.
 * This file pins that drain() drives that recorded work to the core rather than
 * reporting success on the transport's view alone, and that the ordering rules
 * around it hold.
 *
 * Historically (pre-fix, for the record only): drain() consulted only the
 * facade's transport view and could return MOQ_OK while accepted service work
 * was still outstanding; catalog mutations accepted before the peer accepted
 * the namespace were not flushed locally; and permanent completion could
 * publish isComplete:true before Track Ended. Those are the defects the rows
 * below were written against and now permanently guard.
 *
 * The contract is NOT completion-specific: ordinary queued work is covered on
 * the same terms -- a demanded media object accepted by write(), and the
 * zero-byte END_OF_TRACK marker accepted by end_track(), both physically queued
 * in the sender's ring.
 *
 * Composition under test:
 *
 *   - ONE bare endpoint per row, carrying a scripted facade probe installed
 *     BEFORE sender setup. The probe reports a chosen drain state and COUNTS
 *     drain-state/wake/wait calls. It models a managed context that has
 *     recorded a wake but has not yet run its next cycle: it never services
 *     anything spontaneously, and in the serviced mode it runs EXACTLY ONE
 *     recorded wake, from the wait() path only.
 *   - the sender is created through the PUBLIC moq_media_sender_attach(ep, ...),
 *     so the production sender_hook is installed on that endpoint and every
 *     public operation's wake lands on THIS endpoint.
 *   - service progress is driven only by moq_endpoint_test_pump_cycle(), the
 *     real ep_pump_cycle (posted-task drain + state refresh + attached hooks).
 *     The sender's own pump seam is never called.
 *   - the peer is a real SimPair session subscribing the catalog track and (in
 *     the media rows) the media track. Catalog generations are decoded with the
 *     real moq_msf_catalog_parse, and delta generations are applied with the
 *     real moq_msf_catalog_apply_delta; every peer event is recorded into an
 *     ordered per-phase inventory and compared field by field.
 *
 * No socket, no sleep, no wall clock, no thread race. Where a catalog
 * timestamp would otherwise make an outcome clock-dependent, the sender's
 * MSF §5.1.2 generatedAt sequence is SCRIPTED per sender instead.
 *
 * Lifecycle (an attached child makes moq_endpoint_stop() return
 * MOQ_ERR_WRONG_STATE, endpoint.h:289-300 / endpoint.c:1842-1852):
 *
 *     act -> drain(endpoint) -> [contract assertion] -> [isolated recovery]
 *     -> destroy(sender) -> stop(endpoint) [+ idempotent] -> destroy(endpoint)
 *
 * Draft-16 by declared scope, matching test_media_sender_pacing.c: the rows
 * here pin drain and completion ordering, not profile coverage. The sender
 * encodes LOC in the profile the session observed, so draft-18 is exercised by
 * the negotiated-profile rows rather than being unable to carry the block.
 */
#include <pthread.h>
#include <moq/media_sender.h>
#include <moq/endpoint.h>
#include <moq/rcbuf.h>
#include <moq/sim.h>
#include <moq/session.h>
#include <moq/msf.h>
#include "test_session_support.h"

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <stdio.h>

static int failures = 0;

/* Test seams (endpoint.c, MOQ_MEDIA_SENDER_TESTING). The sender is built and
 * torn down entirely through the PUBLIC attach/destroy API. */
moq_endpoint_t *moq_endpoint_test_make_bare_alloc(const moq_alloc_t *alloc);
void moq_endpoint_test_pump_cycle(moq_endpoint_t *ep, moq_session_t *session,
                                  uint64_t now_us);
size_t       moq_endpoint_test_probe_size(void);
moq_result_t moq_endpoint_test_install_probe(moq_endpoint_t *ep, void *storage,
                                             size_t size, int report);
unsigned moq_endpoint_test_probe_drain_calls(const void *probe);
unsigned moq_endpoint_test_probe_wakes(const void *probe);
unsigned moq_endpoint_test_probe_waits(const void *probe);
unsigned moq_endpoint_test_probe_services(const void *probe);
void     moq_endpoint_test_probe_set_service(void *probe, void (*fn)(void *),
                                             void *ctx);
void     moq_endpoint_test_probe_arm_on_probe_once(void *probe,
                                                   void (*fn)(void *),
                                                   void *ctx, unsigned at_call);
void     moq_endpoint_test_remove_probe(moq_endpoint_t *ep);

/* media_sender.c test seams (MOQ_MEDIA_SENDER_TESTING): used ONLY to refuse a
 * staged catalog install once, and to read the catalog_dirty flag. Neither
 * changes what drain() does. */
void moq_media_sender_test_block_retained_install_once(moq_media_sender_t *s);
bool moq_media_sender_test_catalog_dirty(const moq_media_sender_t *s);
bool moq_media_sender_test_pending_send_work(const moq_media_sender_t *s);
void moq_media_sender_test_fail_next_media_write_once(moq_media_sender_t *s);
void moq_media_sender_test_after_republish_once(moq_media_sender_t *s,
                                                void (*fn)(void *), void *ctx);
void moq_media_sender_test_block_end_track_once(moq_media_sender_t *s);
void moq_media_sender_test_script_generated_at(moq_media_sender_t *s,
                                               uint64_t first,
                                               uint64_t rest);
void moq_media_sender_test_before_initial_build_once(moq_media_sender_t *s,
                                                     void (*fn)(void *),
                                                     void *ctx);
void moq_media_sender_test_block_flush_reset_once(moq_media_sender_t *s);
void moq_media_sender_test_block_remove_track_once(moq_media_sender_t *s);
void moq_media_sender_test_block_live_catalog_once(moq_media_sender_t *s);
bool moq_media_sender_test_live_catalog_sent(const moq_media_sender_t *s);
uint32_t moq_media_sender_test_reset_attempts(const moq_media_sender_t *s);
uint32_t moq_media_sender_test_remove_attempts(const moq_media_sender_t *s);
int      moq_media_sender_test_send_drain_status(const moq_media_sender_t *s);
uint32_t moq_media_sender_test_live_catalog_attempts(
    const moq_media_sender_t *s);
bool moq_media_sender_test_track_pending_reset(const moq_media_sender_t *s,
                                               const moq_media_track_t *t);
bool moq_media_sender_test_track_pub_ended(const moq_media_sender_t *s,
                                           const moq_media_track_t *t);
bool moq_media_sender_test_track_teardown_done(const moq_media_sender_t *s,
                                               const moq_media_track_t *t);
size_t   moq_media_sender_test_pending_obj_count(const moq_media_sender_t *s);
uint64_t moq_media_sender_test_catalog_group(const moq_media_sender_t *s);
unsigned moq_media_sender_test_retained_installs(const moq_media_sender_t *s);

#define PROBE_DRAIN_DONE 1   /* EP_DRAIN_DONE: transport idle (endpoint.c) */

enum { CAT_MAX = 16, CAT_BYTES = 8192, OBS_MAX = 128, MED_MAX = 16 };

/* Declared phases. Every peer event is recorded with the phase that was active
 * when it was collected, and each phase's inventory is then compared in order,
 * field by field -- not merely screened by kind. */
typedef enum {
    PH_SETUP = 0,      /* namespace advertisement + readiness */
    PH_SUB,            /* subscription establishment */
    PH_LIVE,           /* ordinary traffic before the operation under test */
    PH_ACT,            /* the public operation .. the contract assertion point */
    PH_RECOVER,        /* strictly separated: ONE serviced endpoint cycle */
    PH_TEARDOWN,       /* sender destroy / endpoint stop */
    PH_COUNT
} phase_t;

static const char *phase_name(phase_t p)
{
    switch (p) {
    case PH_SETUP:    return "setup";
    case PH_SUB:      return "subscribe";
    case PH_LIVE:     return "live";
    case PH_ACT:      return "act";
    case PH_RECOVER:  return "recover";
    case PH_TEARDOWN: return "teardown";
    default:          return "?";
    }
}

static const char *kind_name(moq_event_kind_t k)
{
    switch (k) {
    case MOQ_EVENT_NAMESPACE_PUBLISHED: return "NAMESPACE_PUBLISHED";
    case MOQ_EVENT_SUBSCRIBE_OK:        return "SUBSCRIBE_OK";
    case MOQ_EVENT_OBJECT_RECEIVED:     return "OBJECT_RECEIVED";
    case MOQ_EVENT_SUBGROUP_FINISHED:   return "SUBGROUP_FINISHED";
    case MOQ_EVENT_SUBGROUP_RESET:      return "SUBGROUP_RESET";
    case MOQ_EVENT_PUBLISH_REQUEST:     return "PUBLISH_REQUEST";
    case MOQ_EVENT_FETCH_OK:            return "FETCH_OK";
    case MOQ_EVENT_FETCH_OBJECT:        return "FETCH_OBJECT";
    case MOQ_EVENT_FETCH_COMPLETE:      return "FETCH_COMPLETE";
    case MOQ_EVENT_SUBSCRIBE_DONE:      return "SUBSCRIBE_DONE";
    case MOQ_EVENT_NAMESPACE_DONE:      return "NAMESPACE_DONE";
    default:                            return "OTHER";
    }
}

/* One recorded peer observation, in arrival order. */
typedef struct {
    phase_t          phase;
    moq_event_kind_t kind;
    uint64_t         sub;        /* subscription handle (0 = n/a) */
    uint64_t         fetch;      /* fetch handle (0 = n/a) */
    uint64_t         pub;        /* publication handle (0 = n/a) */
    uint64_t         ann;        /* announcement handle (0 = n/a) */
    uint64_t         group, subgroup, object;
    uint8_t          status;
    uint8_t          prio;       /* publisher priority */
    bool             eog;
    bool             datagram;
    bool             has_payload;
    bool             has_props;  /* object property block present */
    /* SUBSCRIBE_OK body / SUBSCRIBE_DONE body / NAMESPACE_PUBLISHED tokens. */
    uint64_t         x, y;       /* alias | status_code, largest_grp | streams */
    uint64_t         largest_obj, expires_ms;
    bool             has_largest, has_expires, dyn;
    size_t           reason_len; /* SUBSCRIBE_DONE reason / SUB_OK props */
    size_t           token_count;
} obs_t;

/* An expected observation. Every field present in the recorded form is
 * compared; there is no "kind only" mode. */
typedef obs_t exp_t;   /* one shape: every recorded field is compared */

/* One catalog generation as the PEER received it, verbatim. */
typedef struct {
    uint64_t group, object;
    size_t   len;
    uint8_t  buf[CAT_BYTES];
    /* Decoded by the real MSF parser. */
    bool     parsed;
    bool     is_complete;
    size_t   track_count;
    bool     is_delta;
    size_t   delta_ops, delta_add, delta_remove;
    /* MSF §5.1.2 generatedAt, exactly as the decoded generation carries it. */
    bool     has_generated_at;
    uint64_t generated_at;
    phase_t  phase;
} cat_rec_t;

/* One media object as the PEER received it. */
typedef struct {
    uint64_t group, object;
    uint8_t  status;
    size_t   len;
    uint8_t  buf[256];
    phase_t  phase;
} med_rec_t;

typedef struct fix {
    moq_simpair_t      *sp;
    moq_endpoint_t     *ep;
    moq_media_sender_t *s;
    moq_media_track_t  *media;      /* the one live media track (arms 1,3,4) */
    moq_alloc_t         alloc;
    /* Caller-owned, non-allocating probe storage: the endpoint carrying it
     * has no hidden allocation, so the counting allocator's balance covers
     * the whole fixture. */
    union { unsigned long long a; void *p; unsigned char b[128]; } probe_mem;
    void               *probe;
    unsigned            cycles;     /* production pump cycles (ours + drain's) */
    unsigned            serviced_cycles;  /* cycles driven from drain's wait */
    phase_t             phase;
    /* Subscriptions. */
    moq_subscription_t  cat_sub, med_sub, tl_sub;
    bool                cat_subscribed, med_subscribed, tl_subscribed;
    /* Generated-timeline objects, verbatim. */
    med_rec_t           tl_rec[MED_MAX];
    int                 tl_n;
    /* The retained catalog as a Joining FETCH delivered it (MSF-01 §5), plus
     * the effective track-name set rebuilt from base + deltas. */
    cat_rec_t           fet[CAT_MAX];
    int                 fet_n;
    int                 fetch_ok_n, fetch_complete_n;
    /* The EFFECTIVE catalog: an owning moq_msf_catalog_t maintained by the
     * real product delta-application entry point, never a hand-rolled name
     * set. `eff_valid` says a base generation was adopted; `eff_fail` is
     * sticky and makes every identity question INCOMPARABLE. */
    moq_msf_catalog_t   eff_cat;
    bool                eff_valid;
    bool                eff_fail;
    int                 sub_ok_n;
    moq_subscribe_ok_event_t sub_ok;      /* the catalog subscription's */
    size_t              sub_ok_props_len;
    moq_subscribe_ok_event_t med_sub_ok;  /* the media subscription's */
    size_t              med_sub_ok_props_len;
    uint64_t            ann_handle;       /* from NAMESPACE_PUBLISHED */
    /* Inventories. */
    obs_t               obs[OBS_MAX];
    int                 obs_n;
    cat_rec_t           cat[CAT_MAX];
    int                 cat_n;
    med_rec_t           med[MED_MAX];
    int                 med_n;
    /* Sticky incomparability. */
    bool                overflow, long_payload, foreign_obj, parse_fail;
    bool                ns_bad, accept_bad, obs_overflow;
    /* Phase ledger: a phase is ENTERED when set_phase() selects it and
     * DECLARED when expect_phase() consumes its inventory. Teardown fails if
     * any entered phase was never declared, and a second declaration of the
     * same phase is itself a failure -- so no phase can be silently skipped
     * or double-counted. */
    bool                phase_entered[PH_COUNT];
    bool                phase_declared[PH_COUNT];
    bool                withhold_accept;  /* the peer never accepts the ns */
    uint32_t            max_actions;      /* 0 = library default */
    moq_result_t        late_add_rc;      /* the barrier mutation's result */
    bool                drop_without_demand;
    moq_result_t        post_drain_rc;
    moq_fetch_t         fetch;
    bool                publish_tracks;   /* cfg.publish_tracks */
    int                 pub_req_n;        /* peer PUBLISH requests accepted */
    uint64_t            pub_handle;
    int                 ready_n;
    int                 ns_pub_n;
    uint64_t            now;
} fix_t;

static void probe_service(void *ctx);   /* the scripted managed context */

static moq_bytes_t g_ns[2];
static const char  MEDIA_TRACK_NAME[] = "a";
static const uint8_t MEDIA_PAYLOAD[] = { 0xA5, 0x5A, 0x11, 0x22, 0x33 };

static void on_ready_cb(void *ctx, moq_media_sender_t *s)
{
    (void)s; ((fix_t *)ctx)->ready_n++;
}

/* A phase is entered EXACTLY once. Re-entering one is a failure, so a fixture
 * cannot fold two windows into one inventory. */
/* Adopt one decoded generation into the effective catalog. Ownership of *cat
 * is TAKEN: on every path it is either moved into fix->eff_cat or released
 * here. A base generation replaces the effective catalog outright; a delta is
 * applied through the product's own moq_msf_catalog_apply_delta(), so the test
 * never reimplements the delta algorithm it is meant to be checking. */
static void eff_apply(fix_t *fix, moq_msf_catalog_t *cat)
{
    if (cat->delta_update == NULL) {              /* an independent base */
        if (fix->eff_valid) moq_msf_catalog_cleanup(&fix->alloc, &fix->eff_cat);
        fix->eff_cat = *cat;
        memset(cat, 0, sizeof(*cat));             /* ownership moved */
        fix->eff_valid = true;
        return;
    }
    if (!fix->eff_valid) {                        /* a delta with no base */
        fix->eff_fail = true;
        moq_msf_catalog_cleanup(&fix->alloc, cat);
        return;
    }
    moq_msf_catalog_t out;
    memset(&out, 0, sizeof(out));
    if (moq_msf_catalog_apply_delta(&fix->alloc, &fix->eff_cat, cat, &out)
            == MOQ_OK) {
        moq_msf_catalog_cleanup(&fix->alloc, &fix->eff_cat);
        fix->eff_cat = out;
    } else {
        fix->eff_fail = true;
    }
    moq_msf_catalog_cleanup(&fix->alloc, cat);
}

/* Track identity is read ONLY from the effective catalog the product built. */
static bool eff_has(const fix_t *fix, const char *name)
{
    size_t n = strlen(name);
    if (!fix->eff_valid || fix->eff_fail) return false;
    for (size_t i = 0; i < fix->eff_cat.track_count; i++)
        if (fix->eff_cat.tracks[i].name.len == n &&
            memcmp(fix->eff_cat.tracks[i].name.data, name, n) == 0) return true;
    return false;
}

static void set_phase(fix_t *fix, phase_t ph)
{
    if (fix->phase_entered[ph]) {
        fprintf(stderr, "FAIL: phase %s entered twice\n", phase_name(ph));
        failures++;
    }
    fix->phase = ph;
    fix->phase_entered[ph] = true;
}

static obs_t *obs_push(fix_t *fix, moq_event_kind_t k)
{
    if (fix->obs_n >= OBS_MAX) { fix->obs_overflow = true; return NULL; }
    obs_t *o = &fix->obs[fix->obs_n++];
    memset(o, 0, sizeof(*o));
    o->phase = fix->phase;
    o->kind = k;
    return o;
}

/* Collect every peer event into the ordered inventory; decode catalog
 * generations with the real parser and capture media objects verbatim. */
static void collect(fix_t *fix)
{
    moq_session_t *srv = moq_simpair_server(fix->sp);
    moq_event_t ev;
    while (moq_session_poll_events(srv, &ev, 1) == 1) {
        obs_t *o = obs_push(fix, ev.kind);
        if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) {
            const moq_object_received_event_t *e = &ev.u.object_received;
            size_t len = e->payload ? moq_rcbuf_len(e->payload) : 0;
            if (o) {
                o->sub = e->sub._opaque;
                o->pub = e->pub._opaque;
                o->group = e->group_id;
                o->subgroup = e->subgroup_id;
                o->object = e->object_id;
                o->status = e->status;
                o->prio = e->publisher_priority;
                o->eog = e->end_of_group;
                o->datagram = e->datagram;
                o->has_payload = (e->payload != NULL);
                o->has_props = (e->properties != NULL);
            }
            /* The catalog reaches the peer either on its SUBSCRIBE or, with
             * publish_tracks, on the PUBLICATION the peer accepted. */
            bool is_cat = (fix->cat_subscribed &&
                           e->sub._opaque == fix->cat_sub._opaque) ||
                          (fix->pub_handle != 0 &&
                           e->pub._opaque == fix->pub_handle);
            bool is_med = fix->med_subscribed &&
                          e->sub._opaque == fix->med_sub._opaque;
            bool is_tl  = fix->tl_subscribed &&
                          e->sub._opaque == fix->tl_sub._opaque;
            if (is_tl) {
                if (fix->tl_n >= MED_MAX) {
                    fix->overflow = true;
                } else if (len > sizeof(fix->tl_rec[0].buf)) {
                    fix->long_payload = true;
                } else {
                    med_rec_t *m = &fix->tl_rec[fix->tl_n++];
                    memset(m, 0, sizeof(*m));
                    m->group = e->group_id;
                    m->object = e->object_id;
                    m->status = e->status;
                    m->len = len;
                    m->phase = fix->phase;
                    if (len) memcpy(m->buf, moq_rcbuf_data(e->payload), len);
                }
            } else if (!is_cat && !is_med) {
                fix->foreign_obj = true;
            } else if (is_cat) {
                if (e->status != MOQ_OBJECT_NORMAL) {
                    fix->foreign_obj = true;   /* the catalog emits no status */
                } else if (fix->cat_n >= CAT_MAX) {
                    fix->overflow = true;
                } else if (len == 0 || len > CAT_BYTES) {
                    fix->long_payload = true;
                } else {
                    cat_rec_t *c = &fix->cat[fix->cat_n++];
                    memset(c, 0, sizeof(*c));
                    c->group = e->group_id;
                    c->object = e->object_id;
                    c->len = len;
                    c->phase = fix->phase;
                    memcpy(c->buf, moq_rcbuf_data(e->payload), len);
                    /* Decode with the REAL parser: the oracle classifies the
                     * peer's bytes, not the sender's internal state. */
                    moq_msf_catalog_t cat;
                    memset(&cat, 0, sizeof(cat));
                    moq_bytes_t json = { c->buf, c->len };
                    if (moq_msf_catalog_parse(&fix->alloc, json, &cat)
                            == MOQ_OK) {
                        c->parsed = true;
                        c->is_complete = cat.is_complete;
                        c->track_count = cat.track_count;
                        c->is_delta = cat.delta_update != NULL;
                        c->has_generated_at = cat.has_generated_at;
                        c->generated_at = cat.generated_at;
                        c->delta_ops = cat.delta_update_count;
                        for (size_t d = 0; d < cat.delta_update_count; d++) {
                            const moq_msf_delta_op_t *op = &cat.delta_update[d];
                            if (op->op == MOQ_MSF_DELTA_OP_ADD)
                                c->delta_add += op->track_count;
                            else if (op->op == MOQ_MSF_DELTA_OP_REMOVE)
                                c->delta_remove += op->track_count;
                        }
                        moq_msf_catalog_cleanup(&fix->alloc, &cat);
                    } else {
                        fix->parse_fail = true;
                    }
                }
            } else {                       /* media subscription */
                if (fix->med_n >= MED_MAX) {
                    fix->overflow = true;
                } else if (len > sizeof(fix->med[0].buf)) {
                    fix->long_payload = true;
                } else {
                    med_rec_t *m = &fix->med[fix->med_n++];
                    memset(m, 0, sizeof(*m));
                    m->group = e->group_id;
                    m->object = e->object_id;
                    m->status = e->status;
                    m->len = len;
                    m->phase = fix->phase;
                    if (len) memcpy(m->buf, moq_rcbuf_data(e->payload), len);
                }
            }
        } else if (ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED) {
            /* The peer accepts the advertisement; that acceptance is what
             * makes the sender ready (media_sender.c:2585-2592). Identity is
             * checked against the namespace the fixture advertised, and the
             * accept RESULT is checked -- a silently failed accept would
             * otherwise look like a healthy setup. */
            const moq_namespace_published_event_t *n = &ev.u.namespace_published;
            fix->ns_pub_n++;
            fix->ann_handle = n->ann._opaque;
            if (o) { o->ann = n->ann._opaque; o->token_count = n->token_count; }
            if (n->track_namespace.count != 2 || !n->track_namespace.parts) {
                fix->ns_bad = true;
            } else {
                for (int i = 0; i < 2; i++) {
                    const moq_bytes_t *got = &n->track_namespace.parts[i];
                    if (got->len != g_ns[i].len || !got->data ||
                        memcmp(got->data, g_ns[i].data, got->len) != 0)
                        fix->ns_bad = true;
                }
            }
            if (!fix->withhold_accept) {
                moq_accept_namespace_cfg_t ac;
                moq_accept_namespace_cfg_init(&ac);
                if (moq_session_accept_namespace(srv, n->ann, &ac, fix->now)
                        != MOQ_OK)
                    fix->accept_bad = true;
            }
        } else if (ev.kind == MOQ_EVENT_SUBSCRIBE_OK) {
            const moq_subscribe_ok_event_t *ok = &ev.u.subscribe_ok;
            fix->sub_ok_n++;
            if (o) {
                o->sub = ok->sub._opaque;
                o->x = ok->track_alias;
                o->y = ok->largest_group;
                o->largest_obj = ok->largest_object;
                o->has_largest = ok->has_largest;
                o->has_expires = ok->has_expires;
                o->expires_ms = ok->expires_ms;
                o->dyn = ok->dynamic_groups;
                /* track_properties is BORROWED from output scratch: its
                 * LENGTH is recorded here, before cleanup, and the span is
                 * never retained. Non-empty property bytes would belong to
                 * the deep catalog/payload oracles, not to this inventory. */
                o->reason_len = ok->track_properties.len;
            }
            if (fix->cat_subscribed &&
                ok->sub._opaque == fix->cat_sub._opaque) {
                fix->sub_ok = *ok;
                fix->sub_ok_props_len = ok->track_properties.len;
                fix->sub_ok.track_properties = (moq_bytes_t){ NULL, 0 };
            } else if (fix->med_subscribed &&
                       ok->sub._opaque == fix->med_sub._opaque) {
                fix->med_sub_ok = *ok;
                fix->med_sub_ok_props_len = ok->track_properties.len;
                fix->med_sub_ok.track_properties = (moq_bytes_t){ NULL, 0 };
            }
        } else if (ev.kind == MOQ_EVENT_SUBGROUP_FINISHED) {
            const moq_subgroup_finished_event_t *f = &ev.u.subgroup_finished;
            if (o) {
                o->sub = f->sub._opaque;
                o->pub = f->pub._opaque;
                o->group = f->group_id;
                o->subgroup = f->subgroup_id;
                o->eog = f->end_of_group;
            }
        } else if (ev.kind == MOQ_EVENT_SUBSCRIBE_DONE) {
            const moq_subscribe_done_event_t *d = &ev.u.subscribe_done;
            if (o) {
                o->sub = d->sub._opaque;
                o->x = d->status_code;
                o->y = d->stream_count;
                o->reason_len = d->reason.len;
            }
        } else if (ev.kind == MOQ_EVENT_PUBLISH_REQUEST) {
            /* publish_tracks: the sender PUBLISHes its catalog track; the peer
             * accepts, which is what creates the demand term 8 needs. */
            const moq_publish_request_event_t *pr = &ev.u.publish_request;
            fix->pub_req_n++;
            fix->pub_handle = pr->pub._opaque;
            if (o) { o->pub = pr->pub._opaque; o->x = pr->track_alias; }
            moq_accept_publish_cfg_t apc;
            moq_accept_publish_cfg_init(&apc);
            if (moq_session_accept_publish(srv, pr->pub, &apc, fix->now) != MOQ_OK)
                fix->accept_bad = true;
        } else if (ev.kind == MOQ_EVENT_FETCH_OK) {
            const moq_fetch_ok_event_t *f = &ev.u.fetch_ok;
            if (o) {
                o->fetch = f->fetch._opaque;
                o->y = f->end_group;
                o->largest_obj = f->end_object;
                o->eog = f->end_of_track;
                o->reason_len = f->track_properties.len;
            }
            fix->fetch_ok_n++;
        } else if (ev.kind == MOQ_EVENT_FETCH_OBJECT) {
            const moq_fetch_object_event_t *f = &ev.u.fetch_object;
            size_t len = f->payload ? moq_rcbuf_len(f->payload) : 0;
            if (o) {
                o->fetch = f->fetch._opaque;
                o->group = f->group_id;
                o->subgroup = f->subgroup_id;
                o->object = f->object_id;
                o->prio = f->publisher_priority;
                o->datagram = f->datagram;
                o->has_payload = (f->payload != NULL);
                o->has_props = (f->properties != NULL);
            }
            /* The RETAINED catalog, decoded with the real parser. A live
             * OBJECT_RECEIVED is never accepted as a substitute for this. */
            if (fix->fet_n >= CAT_MAX) {
                fix->overflow = true;
            } else if (len == 0 || len > CAT_BYTES) {
                fix->long_payload = true;
            } else {
                cat_rec_t *c = &fix->fet[fix->fet_n++];
                memset(c, 0, sizeof(*c));
                c->group = f->group_id;
                c->object = f->object_id;
                c->len = len;
                c->phase = fix->phase;
                memcpy(c->buf, moq_rcbuf_data(f->payload), len);
                moq_msf_catalog_t cat;
                memset(&cat, 0, sizeof(cat));
                moq_bytes_t json = { c->buf, c->len };
                if (moq_msf_catalog_parse(&fix->alloc, json, &cat) == MOQ_OK) {
                    c->parsed = true;
                    c->is_complete = cat.is_complete;
                    c->track_count = cat.track_count;
                    c->is_delta = cat.delta_update != NULL;
                    c->has_generated_at = cat.has_generated_at;
                    c->generated_at = cat.generated_at;
                    c->delta_ops = cat.delta_update_count;
                    for (size_t d = 0; d < cat.delta_update_count; d++) {
                        const moq_msf_delta_op_t *op = &cat.delta_update[d];
                        if (op->op == MOQ_MSF_DELTA_OP_ADD)
                            c->delta_add += op->track_count;
                        else if (op->op == MOQ_MSF_DELTA_OP_REMOVE)
                            c->delta_remove += op->track_count;
                    }
                    /* Apply this generation through the PRODUCT's own delta
                     * entry point; eff_apply consumes `cat` on every path. */
                    eff_apply(fix, &cat);
                } else {
                    fix->parse_fail = true;
                }
            }
        } else if (ev.kind == MOQ_EVENT_FETCH_COMPLETE) {
            if (o) o->fetch = ev.u.fetch_complete.fetch._opaque;
            fix->fetch_complete_n++;
        } else if (ev.kind == MOQ_EVENT_SUBGROUP_RESET) {
            const moq_subgroup_reset_event_t *r = &ev.u.subgroup_reset;
            if (o) {
                o->sub = r->sub._opaque;
                o->pub = r->pub._opaque;
                o->group = r->group_id;
                o->subgroup = r->subgroup_id;
                o->eog = r->end_of_group;
                o->x = r->error_code;
            }
        } else if (ev.kind == MOQ_EVENT_NAMESPACE_DONE) {
            if (o) o->ann = ev.u.namespace_done.ann._opaque;
        }
        moq_event_cleanup(&ev);
    }
}

/* Compare one phase's COMPLETE inventory, in order, field by field. */
static void expect_phase(fix_t *fix, const char *name, phase_t ph,
                         const exp_t *exp, int n)
{
    if (fix->phase_declared[ph]) {
        fprintf(stderr, "FAIL %s: phase %s declared twice\n",
                name, phase_name(ph));
        failures++;
    }
    if (!fix->phase_entered[ph]) {
        fprintf(stderr, "FAIL %s: phase %s declared but never entered\n",
                name, phase_name(ph));
        failures++;
    }
    fix->phase_declared[ph] = true;
    int idx[OBS_MAX], m = 0;
    for (int i = 0; i < fix->obs_n; i++)
        if (fix->obs[i].phase == ph) idx[m++] = i;
    if (m != n) {
        fprintf(stderr, "FAIL %s: phase %s inventory has %d observations, "
                "declared %d\n", name, phase_name(ph), m, n);
        for (int i = 0; i < m; i++)
            fprintf(stderr, "      [%d] %s sub=%llu grp=%llu obj=%llu st=%u\n",
                    i, kind_name(fix->obs[idx[i]].kind),
                    (unsigned long long)fix->obs[idx[i]].sub,
                    (unsigned long long)fix->obs[idx[i]].group,
                    (unsigned long long)fix->obs[idx[i]].object,
                    fix->obs[idx[i]].status);
        failures++;
        return;
    }
    for (int i = 0; i < n; i++) {
        const obs_t *g = &fix->obs[idx[i]];
        const exp_t *e = &exp[i];
        bool ok = g->kind == e->kind && g->sub == e->sub && g->pub == e->pub &&
                  g->fetch == e->fetch &&
                  g->ann == e->ann && g->group == e->group &&
                  g->subgroup == e->subgroup && g->object == e->object &&
                  g->status == e->status && g->prio == e->prio &&
                  g->eog == e->eog && g->datagram == e->datagram &&
                  g->has_payload == e->has_payload &&
                  g->has_props == e->has_props &&
                  g->x == e->x && g->y == e->y &&
                  g->largest_obj == e->largest_obj &&
                  g->expires_ms == e->expires_ms &&
                  g->has_largest == e->has_largest &&
                  g->has_expires == e->has_expires && g->dyn == e->dyn &&
                  g->reason_len == e->reason_len &&
                  g->token_count == e->token_count;
        if (!ok) {
            fprintf(stderr,
                "FAIL %s: phase %s observation %d mismatch\n"
                "      got      %s sub=%llu pub=%llu ann=%llu grp=%llu "
                "sg=%llu obj=%llu st=%u prio=%u eog=%d dg=%d pay=%d prop=%d "
                "x=%llu y=%llu lobj=%llu exp=%llu hl=%d he=%d dyn=%d "
                "rlen=%zu tok=%zu\n"
                "      declared %s sub=%llu pub=%llu ann=%llu grp=%llu "
                "sg=%llu obj=%llu st=%u prio=%u eog=%d dg=%d pay=%d prop=%d "
                "x=%llu y=%llu lobj=%llu exp=%llu hl=%d he=%d dyn=%d "
                "rlen=%zu tok=%zu\n",
                name, phase_name(ph), i,
                kind_name(g->kind), (unsigned long long)g->sub,
                (unsigned long long)g->pub,
                (unsigned long long)g->ann, (unsigned long long)g->group,
                (unsigned long long)g->subgroup, (unsigned long long)g->object,
                g->status, g->prio, g->eog, g->datagram, g->has_payload,
                g->has_props, (unsigned long long)g->x,
                (unsigned long long)g->y, (unsigned long long)g->largest_obj,
                (unsigned long long)g->expires_ms, g->has_largest,
                g->has_expires, g->dyn, g->reason_len, g->token_count,
                kind_name(e->kind), (unsigned long long)e->sub,
                (unsigned long long)e->pub,
                (unsigned long long)e->ann, (unsigned long long)e->group,
                (unsigned long long)e->subgroup, (unsigned long long)e->object,
                e->status, e->prio, e->eog, e->datagram, e->has_payload,
                e->has_props, (unsigned long long)e->x,
                (unsigned long long)e->y, (unsigned long long)e->largest_obj,
                (unsigned long long)e->expires_ms, e->has_largest,
                e->has_expires, e->dyn, e->reason_len, e->token_count);
            failures++;
        }
    }
}

/* One CYCLE: exactly one production endpoint pump cycle (which runs the
 * attached sender hook), then peer delivery to real quiescence, then
 * collection. Quiescence is asserted so a step-budget overrun can never be
 * read as "nothing more to send". */
static void cycle(fix_t *fix, uint64_t now)
{
    fix->cycles++;
    moq_endpoint_test_pump_cycle(fix->ep, moq_simpair_client(fix->sp), now);
    MOQ_TEST_CHECK(moq_simpair_run_until_quiescent(fix->sp, 64, NULL) == MOQ_OK);
    collect(fix);
}

/* The scripted managed context. Installed on the probe, it is called ONLY from
 * the probe's wait path, once per recorded-but-unserviced wake -- i.e. only
 * after product drain() has observed pending hook work and asked the facade for
 * progress. It runs exactly what a real backend cycle runs: one production
 * endpoint pump cycle, then deterministic peer delivery, then collection. */
static void probe_service(void *ctx)
{
    fix_t *fix = (fix_t *)ctx;
    fix->serviced_cycles++;
    cycle(fix, fix->now);
}

/* Deliver + collect WITHOUT any pump cycle: exactly what the production
 * sequence gives the operation before drain(). */
static void settle_no_cycle(fix_t *fix)
{
    MOQ_TEST_CHECK(moq_simpair_run_until_quiescent(fix->sp, 64, NULL) == MOQ_OK);
    collect(fix);
}

static bool sticky(fix_t *fix, const char *label)
{
    if (!(fix->overflow || fix->long_payload || fix->foreign_obj ||
          fix->parse_fail || fix->ns_bad || fix->accept_bad ||
          fix->obs_overflow || fix->eff_fail))
        return false;
    fprintf(stderr, "FAIL %s: peer trace INCOMPARABLE (overflow=%d "
            "long_payload=%d foreign_obj=%d parse_fail=%d ns_bad=%d "
            "accept_bad=%d obs_overflow=%d eff_fail=%d)\n",
            label, fix->overflow, fix->long_payload, fix->foreign_obj,
            fix->parse_fail, fix->ns_bad, fix->accept_bad, fix->obs_overflow,
            fix->eff_fail);
    failures++;
    return true;
}

/* max_actions == 0 uses the library default. A SMALL ring is the real service
 * pacing geometry (test_media_sender_pacing.c:2066-2069): the handshake and
 * subscribe exchanges still converge because each runs to quiescence, but a
 * burst -- or a generated-timeline emission -- can be refused mid-hook through
 * the production facade, with no test seam involved. */
static moq_simpair_t *make_pair_actions(moq_alloc_t *alloc, uint32_t max_actions)
{
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = alloc; cfg.seed = 7; cfg.initial_now_us = 1000;
    cfg.max_actions = max_actions;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 32;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = 32;
    cfg.version = MOQ_VERSION_DRAFT_16;
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

static moq_simpair_t *make_pair(moq_alloc_t *alloc)
{
    return make_pair_actions(alloc, 0);
}

/* Reach a legitimate ready state with ZERO media tracks, on one endpoint that
 * already carries the scripted probe, with the sender attached through the
 * public API. Media tracks (if any) are added LIVE afterwards, so an arm's
 * geometry is what the wire shows rather than what a helper name claims. */
static bool fix_init_actions2(fix_t *fix, moq_alloc_t *alloc, uint64_t now,
                              uint32_t max_actions, bool drop_no_demand)
{
    memset(fix, 0, sizeof(*fix));
    fix->max_actions = max_actions;
    fix->drop_without_demand = drop_no_demand;
    fix->alloc = *alloc;
    fix->now = now;
    set_phase(fix, PH_SETUP);
    fix->sp = make_pair_actions(alloc, fix->max_actions);
    if (!fix->sp) return false;

    /* The endpoint is owned by the SAME counting allocator as everything else,
     * so a leaked endpoint is visible in the balance. */
    fix->ep = moq_endpoint_test_make_bare_alloc(alloc);
    if (!fix->ep) return false;
    /* Probe FIRST: installed before any sender exists, and it refuses to
     * overwrite an existing facade. */
    MOQ_TEST_CHECK(moq_endpoint_test_probe_size() <= sizeof(fix->probe_mem));
    if (moq_endpoint_test_install_probe(fix->ep, &fix->probe_mem,
                                        sizeof(fix->probe_mem),
                                        PROBE_DRAIN_DONE) != MOQ_OK)
        return false;
    fix->probe = &fix->probe_mem;
    /* The scripted managed context. It can only ever run from the probe's wait
     * path, one recorded wake at a time -- never before drain asks for
     * progress, and never more than one recorded wake per wait. */
    moq_endpoint_test_probe_set_service(fix->probe, probe_service, fix);
    /* A second install must be refused, not silently overwrite/leak. */
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_endpoint_test_install_probe(fix->ep, &fix->probe_mem,
                                             sizeof(fix->probe_mem),
                                             PROBE_DRAIN_DONE),
        (int)MOQ_ERR_WRONG_STATE);

    g_ns[0] = MOQ_BYTES_LITERAL("svc");
    g_ns[1] = MOQ_BYTES_LITERAL("done");

    moq_media_sender_cfg_t cfg;
    moq_media_sender_cfg_init_live_sized(&cfg, sizeof(cfg));
    cfg.namespace_ = (moq_namespace_t){ g_ns, 2 };
    /* No periodic republish: a refresh generation would confound the
     * "did a terminal catalog appear" oracle. */
    cfg.catalog_refresh_interval_us = UINT64_MAX;
    cfg.drop_without_demand = fix->drop_without_demand;
    cfg.publish_tracks = fix->publish_tracks;
    moq_media_sender_callbacks_init_sized(&cfg.callbacks, sizeof(cfg.callbacks));
    cfg.callbacks.ctx = fix;
    cfg.callbacks.on_ready = on_ready_cb;
    /* PUBLIC attach: installs the production hook on this endpoint and makes
     * every public operation's wake land here. */
    if (moq_media_sender_attach(fix->ep, &cfg, &fix->s) != MOQ_OK) return false;

    for (int c = 0; c < 16 && !moq_media_sender_is_ready(fix->s); c++)
        cycle(fix, now);
    /* Readiness is proven by the production callback, not adopted from
     * sender state alone. */
    MOQ_TEST_CHECK(moq_media_sender_is_ready(fix->s));
    MOQ_TEST_CHECK_EQ_INT(fix->ready_n, 1);
    MOQ_TEST_CHECK_EQ_INT(fix->ns_pub_n, 1);
    MOQ_TEST_CHECK(!moq_media_sender_is_closed(fix->s));
    /* Complete setup inventory: exactly one advertisement, on the handle the
     * fixture then accepted and later compares NAMESPACE_DONE against. */
    {
        exp_t e[1];
        memset(e, 0, sizeof(e));
        e[0].kind = MOQ_EVENT_NAMESPACE_PUBLISHED;
        e[0].ann = fix->ann_handle;
        expect_phase(fix, "setup", PH_SETUP, e, 1);
    }
    MOQ_TEST_CHECK(fix->ann_handle != 0);
    return moq_media_sender_is_ready(fix->s) && fix->ready_n == 1;
}

static bool fix_init_actions(fix_t *fix, moq_alloc_t *alloc, uint64_t now,
                             uint32_t max_actions)
{
    return fix_init_actions2(fix, alloc, now, max_actions, false);
}

static bool fix_init(fix_t *fix, moq_alloc_t *alloc, uint64_t now)
{
    return fix_init_actions2(fix, alloc, now, 0, false);
}

static bool subscribe_track_filter(fix_t *fix, uint64_t now, const char *name,
                                   size_t name_len, uint32_t filter,
                                   moq_subscription_t *out)
{
    moq_session_t *srv = moq_simpair_server(fix->sp);
    moq_subscribe_cfg_t sc; moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ g_ns, 2 };
    sc.track_name = (moq_bytes_t){ (const uint8_t *)name, name_len };
    /* ABSOLUTE_START from the first object. NOTE, measured: this does NOT
     * replay the generation the sender retained BEFORE the subscription
     * existed -- neither this filter nor LARGEST_OBJECT does. The peer's
     * inventory starts empty and fills from the sender's next publish. */
    sc.filter = filter;
    sc.start_group = 0;
    sc.start_object = 0;
    sc.has_forward = true; sc.forward = true;
    memset(out, 0, sizeof(*out));
    return moq_session_subscribe(srv, &sc, now, out) == MOQ_OK;
}

static bool subscribe_track(fix_t *fix, uint64_t now, const char *name,
                            size_t name_len, moq_subscription_t *out)
{
    return subscribe_track_filter(fix, now, name, name_len,
                                  MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT, out);
}

/* The peer subscribes the catalog track so every generation lands on the wire
 * and is decoded. Requires an accepted subscription. */
/* exp_grp/exp_obj < 0 means "whatever the sender has retained" -- used by the
 * pre-acceptance rows, whose whole subject is how many generations exist. */
static bool subscribe_catalog_largest(fix_t *fix, uint64_t now,
                                      int64_t exp_grp, int64_t exp_obj)
{
    set_phase(fix, PH_SUB);
    if (!subscribe_track_filter(fix, now, MOQ_MSF_CATALOG_TRACK_NAME,
                                strlen(MOQ_MSF_CATALOG_TRACK_NAME),
                                MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START,
                                &fix->cat_sub))
        return false;
    fix->cat_subscribed = true;
    for (int i = 0; i < 16 && fix->sub_ok_n == 0; i++) cycle(fix, now);
    /* Exactly one SUBSCRIBE_OK, on the declared handle, with the complete body
     * the event exposes compared against the declared response for a freshly
     * accepted catalog subscription with nothing yet delivered on it. */
    MOQ_TEST_CHECK_EQ_INT(fix->sub_ok_n, 1);
    MOQ_TEST_CHECK_EQ_INT((int)fix->sub_ok.sub._opaque,
                          (int)fix->cat_sub._opaque);
    MOQ_TEST_CHECK_EQ_INT((int)fix->sub_ok.track_alias, 1);
    MOQ_TEST_CHECK(fix->sub_ok.has_largest);
    if (exp_grp >= 0) {
        MOQ_TEST_CHECK_EQ_INT((int)fix->sub_ok.largest_group, (int)exp_grp);
        MOQ_TEST_CHECK_EQ_INT((int)fix->sub_ok.largest_object, (int)exp_obj);
    }
    MOQ_TEST_CHECK(!fix->sub_ok.has_expires);
    MOQ_TEST_CHECK_EQ_INT((int)fix->sub_ok.expires_ms, 0);
    MOQ_TEST_CHECK_EQ_INT((int)fix->sub_ok_props_len, 0);
    MOQ_TEST_CHECK(!fix->sub_ok.dynamic_groups);
    {
        exp_t e[1];
        memset(e, 0, sizeof(e));
        e[0].kind = MOQ_EVENT_SUBSCRIBE_OK;
        e[0].sub = fix->cat_sub._opaque;
        e[0].x = 1;                        /* track_alias */
        e[0].has_largest = true;
        e[0].y = fix->sub_ok.largest_group;        /* measured, then compared */
        e[0].largest_obj = fix->sub_ok.largest_object;
        expect_phase(fix, "subscribe", PH_SUB, e, 1);
    }
    set_phase(fix, PH_LIVE);
    return fix->sub_ok_n == 1;
}

static bool subscribe_catalog(fix_t *fix, uint64_t now)
{
    return subscribe_catalog_largest(fix, now, 0, 0);
}

/* Post-ready add of EXACTLY ONE live media track: publishes a new independent
 * generation plus a delta the subscribed peer receives live. That ordinary
 * traffic is what makes the terminal-vs-ordinary distinction observable. */
static bool add_one_media_track_live(fix_t *fix, uint64_t now)
{
    moq_media_track_cfg_t tc;
    moq_media_track_cfg_init(&tc);
    tc.name = (moq_bytes_t){ (const uint8_t *)MEDIA_TRACK_NAME, 1 };
    tc.media_type = MOQ_MEDIA_TYPE_AUDIO;
    tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
    tc.codec = MOQ_BYTES_LITERAL("opus");
    tc.bitrate = 64000;                                      /* non-default */
    tc.is_live = true;
    tc.samplerate = 48000;                                   /* MSF 5.2.28 */
    tc.channel_config = MOQ_BYTES_LITERAL("2"); /* MSF 5.2.29 */
    if (moq_media_sender_add_track(fix->s, &tc, &fix->media) != MOQ_OK)
        return false;
    int before = fix->cat_n;
    for (int i = 0; i < 8 && fix->cat_n == before; i++) cycle(fix, now);
    return fix->cat_n > before;
}

/* The media-track population the peer's decoded catalog stream describes over
 * records [0, upto): the newest independent NON-terminal generation's track
 * list, then every delta after it. Geometry is read off the wire. */
static int effective_tracks(const fix_t *fix, int upto)
{
    int base = -1;
    for (int i = 0; i < upto; i++)
        if (fix->cat[i].parsed && !fix->cat[i].is_delta &&
            !fix->cat[i].is_complete)
            base = i;
    if (base < 0) return -1;
    long n = (long)fix->cat[base].track_count;
    for (int i = base + 1; i < upto; i++)
        if (fix->cat[i].parsed && fix->cat[i].is_delta)
            n += (long)fix->cat[i].delta_add - (long)fix->cat[i].delta_remove;
    return (int)n;
}

static int terminal_count(const fix_t *fix)
{
    int n = 0;
    for (int i = 0; i < fix->cat_n; i++)
        if (fix->cat[i].parsed && fix->cat[i].is_complete) n++;
    return n;
}

/* -- THE ONE TERMINAL ORACLE, applied to every completion arm and control ----
 * base_n is the peer's decoded inventory size immediately before complete().
 * prev_group is the greatest group the peer can already account for: the last
 * observed generation's group, or -- when nothing was delivered -- the retained
 * largest group SUBSCRIBE_OK declared. The new-group requirement therefore
 * applies in EVERY arm, with no have_base escape. */
static void terminal_oracle(fix_t *fix, const char *name,
                            int base_n, uint64_t prev_group)
{
    (void)name;
    MOQ_TEST_CHECK_EQ_INT(terminal_count(fix), 1);
    MOQ_TEST_CHECK_EQ_INT(fix->cat_n, base_n + 1);
    int ti = -1;
    for (int i = 0; i < fix->cat_n; i++)
        if (fix->cat[i].parsed && fix->cat[i].is_complete) ti = i;
    if (ti < 0) return;                      /* already reported above */
    const cat_rec_t *t = &fix->cat[ti];
    MOQ_TEST_CHECK_EQ_INT((int)t->track_count, 0);   /* empty track array */
    MOQ_TEST_CHECK(!t->is_delta);                    /* independent */
    MOQ_TEST_CHECK_EQ_INT((int)t->object, 0);        /* object id 0 */
    MOQ_TEST_CHECK_EQ_INT(ti, fix->cat_n - 1);       /* last, nothing after */
    MOQ_TEST_CHECK(ti >= base_n);                    /* newly emitted */
    MOQ_TEST_CHECK(t->group > prev_group);           /* a NEW group */
}

/* The greatest group the peer can already account for before the operation. */
static uint64_t prev_group_of(const fix_t *fix)
{
    if (fix->cat_n > 0) return fix->cat[fix->cat_n - 1].group;
    return fix->sub_ok.largest_group;   /* declared retained largest (0) */
}

/* Canonical teardown for a borrowed endpoint, after the assertion point.
 * Proves stop succeeds, is idempotent, and manufactures nothing. Sender APIs
 * are never called after destroy. */
static void teardown(fix_t *fix, const char *name, int cat_n_at_assert,
                     int med_n_at_assert, const exp_t *exp, int n)
{
    set_phase(fix, PH_TEARDOWN);
    /* Attached child blocks stop -- the correction to the task's literal
     * complete -> drain -> stop wording. */
    MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_stop(fix->ep),
                          (int)MOQ_ERR_WRONG_STATE);
    moq_media_sender_destroy(fix->s);
    fix->s = NULL;
    fix->media = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_stop(fix->ep), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_stop(fix->ep), (int)MOQ_OK); /* idem */
    settle_no_cycle(fix);
    /* Teardown fabricates nothing. */
    MOQ_TEST_CHECK_EQ_INT(fix->cat_n, cat_n_at_assert);
    MOQ_TEST_CHECK_EQ_INT(fix->med_n, med_n_at_assert);
    expect_phase(fix, name, PH_TEARDOWN, exp, n);
    /* Ledger: every phase this fixture entered was declared exactly once. */
    for (int ph = 0; ph < PH_COUNT; ph++)
        if (fix->phase_entered[ph] && !fix->phase_declared[ph]) {
            fprintf(stderr, "FAIL %s: phase %s was entered but never "
                    "declared\n", name, phase_name((phase_t)ph));
            failures++;
        }
    (void)sticky(fix, name);
}

static void fix_free(fix_t *fix)
{
    if (fix->eff_valid) {
        moq_msf_catalog_cleanup(&fix->alloc, &fix->eff_cat);
        fix->eff_valid = false;
    }
    if (fix->s) { moq_media_sender_destroy(fix->s); fix->s = NULL; }
    if (fix->ep) {
        (void)moq_endpoint_stop(fix->ep);
        moq_endpoint_test_remove_probe(fix->ep);
        moq_endpoint_destroy(fix->ep);
        fix->ep = NULL;
    }
    if (fix->sp) {
        moq_event_t ev;
        while (moq_session_poll_events(moq_simpair_client(fix->sp), &ev, 1) == 1)
            moq_event_cleanup(&ev);
        while (moq_session_poll_events(moq_simpair_server(fix->sp), &ev, 1) == 1)
            moq_event_cleanup(&ev);
        moq_simpair_destroy(fix->sp);
    }
    memset(fix, 0, sizeof(*fix));
}

/* Report an arm's drain result and its separately counted idempotence phase. */
static void report(const char *name, int drain_diags, int idem_diags,
                   bool have_idem)
{
    if (drain_diags == 0) printf("PASS: %s\n", name);
    else                  printf("FAIL: %s (%d diagnostics)\n", name, drain_diags);
    if (have_idem)
        printf("      idempotence: %s (%d diagnostics)\n",
               idem_diags == 0 ? "no duplicate output" : "BROKEN", idem_diags);
}

/* drain() drove the flush itself: it observed pending hook work, asked the
 * facade for progress, and the scripted managed context answered exactly the
 * recorded wakes. Asserted per arm so a GREEN that merely inherited an earlier
 * pump cannot pass. */
static void assert_drain_drove(fix_t *fix, unsigned services0, unsigned cycles0,
                               unsigned dcalls0)
{
    MOQ_TEST_CHECK(moq_endpoint_test_probe_services(fix->probe) > services0);
    MOQ_TEST_CHECK(fix->cycles > cycles0);
    /* ORDERING: the transport was consulted EXACTLY TWICE -- the entry probe
     * that rejects unsupported/wrong-thread callers, then once on the final
     * iteration after no hook reported owed work. A drain that consulted the
     * backend while a service still held output would show more. */
    MOQ_TEST_CHECK_EQ_INT(
        (int)(moq_endpoint_test_probe_drain_calls(fix->probe) - dcalls0), 2);
}

/* A drain with nothing owed returns without a gratuitous pump or wait. */
static void assert_drain_is_noop(fix_t *fix, const char *name)
{
    unsigned w0 = moq_endpoint_test_probe_wakes(fix->probe);
    unsigned t0 = moq_endpoint_test_probe_waits(fix->probe);
    unsigned s0 = moq_endpoint_test_probe_services(fix->probe);
    unsigned c0 = fix->cycles;
    MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_drain(fix->ep, 1000000), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)(moq_endpoint_test_probe_wakes(fix->probe) - w0), 0);
    MOQ_TEST_CHECK_EQ_INT((int)(moq_endpoint_test_probe_waits(fix->probe) - t0), 0);
    MOQ_TEST_CHECK_EQ_INT((int)(moq_endpoint_test_probe_services(fix->probe) - s0), 0);
    MOQ_TEST_CHECK_EQ_INT((int)(fix->cycles - c0), 0);
    (void)name;
}

/* ==================================================================== *
 * ARM 1 -- complete(), exactly one live media track.
 * drain() must itself drive the terminal catalog to the peer.
 * ==================================================================== */
static void arm_complete_one_track(moq_alloc_t *alloc)
{
    const char *NAME = "complete_drain_one_track";
    int before = failures;
    fix_t fix;
    uint64_t now = 1000;
    if (!fix_init(&fix, alloc, now)) {
        fprintf(stderr, "FAIL %s: fixture setup\n", NAME);
        failures++; fix_free(&fix); return;
    }
    if (!subscribe_catalog(&fix, now)) {
        fprintf(stderr, "FAIL %s: catalog subscribe\n", NAME);
        failures++; fix_free(&fix); return;
    }
    MOQ_TEST_CHECK(add_one_media_track_live(&fix, now));
    if (sticky(&fix, NAME)) { fix_free(&fix); return; }

    int base_n = fix.cat_n;
    MOQ_TEST_CHECK(base_n >= 1);
    uint64_t prev_group = prev_group_of(&fix);
    /* Geometry from the DECODED wire: exactly one media track at completion. */
    MOQ_TEST_CHECK_EQ_INT(effective_tracks(&fix, base_n), 1);
    MOQ_TEST_CHECK_EQ_INT(terminal_count(&fix), 0);
    {   /* the complete live inventory: base generation, then the delta */
        exp_t e[2];
        memset(e, 0, sizeof(e));
        e[0].kind = MOQ_EVENT_OBJECT_RECEIVED; e[0].sub = fix.cat_sub._opaque;
        e[0].group = 1; e[0].object = 0; e[0].eog = true; e[0].has_payload = true;
        e[1].kind = MOQ_EVENT_OBJECT_RECEIVED; e[1].sub = fix.cat_sub._opaque;
        e[1].group = 1; e[1].object = 1; e[1].eog = true; e[1].has_payload = true;
        expect_phase(&fix, NAME, PH_LIVE, e, 2);
    }

    /* --- the production sequence, with NO intervening pump cycle ------- */
    set_phase(&fix, PH_ACT);
    unsigned wakes0 = moq_endpoint_test_probe_wakes(fix.probe);
    unsigned cycles0 = fix.cycles;
    unsigned services0 = moq_endpoint_test_probe_services(fix.probe);
    unsigned dcalls0 = moq_endpoint_test_probe_drain_calls(fix.probe);
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_complete(fix.s), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_complete(fix.s), (int)MOQ_OK);
    MOQ_TEST_CHECK(moq_endpoint_test_probe_wakes(fix.probe) > wakes0);
    MOQ_TEST_CHECK_EQ_INT((int)(fix.cycles - cycles0), 0);   /* no pre-pump */

    settle_no_cycle(&fix);
    MOQ_TEST_CHECK_EQ_INT(fix.cat_n, base_n);   /* still nothing new */

    /* drain() drives it: the hook reports owed work, drain asks the facade for
     * progress, the scripted managed context runs one production cycle. */
    MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_drain(fix.ep, 1000000), (int)MOQ_OK);
    assert_drain_drove(&fix, services0, cycles0, dcalls0);
    if (sticky(&fix, NAME)) { fix_free(&fix); return; }

    /* ---- THE CONTRACT: no separate recovery call ---------------------- */
    terminal_oracle(&fix, NAME, base_n, prev_group);
    MOQ_TEST_CHECK(!moq_media_sender_is_fatal(fix.s));
    MOQ_TEST_CHECK(!moq_media_sender_is_closed(fix.s));
    MOQ_TEST_CHECK(!moq_endpoint_is_fatal(fix.ep));
    MOQ_TEST_CHECK(!moq_endpoint_is_closed(fix.ep));
    {   /* the group-1 subgroup terminal, then the terminal generation */
        exp_t e[2];
        memset(e, 0, sizeof(e));
        e[0].kind = MOQ_EVENT_SUBGROUP_FINISHED; e[0].sub = fix.cat_sub._opaque;
        e[0].group = 1; e[0].eog = true;
        e[1].kind = MOQ_EVENT_OBJECT_RECEIVED; e[1].sub = fix.cat_sub._opaque;
        e[1].group = 2; e[1].object = 0; e[1].eog = true; e[1].has_payload = true;
        expect_phase(&fix, NAME, PH_ACT, e, 2);
    }
    /* Nothing is owed now: a second drain is a no-op. */
    assert_drain_is_noop(&fix, NAME);
    int drained = failures - before;

    /* ---- separately counted idempotence ------------------------------- */
    int ibefore = failures;
    set_phase(&fix, PH_RECOVER);
    for (int i = 0; i < 3; i++) cycle(&fix, now);
    terminal_oracle(&fix, NAME, base_n, prev_group);
    expect_phase(&fix, NAME, PH_RECOVER, NULL, 0);
    int idem = failures - ibefore;

    teardown(&fix, NAME, fix.cat_n, fix.med_n, NULL, 0);
    report(NAME, drained, idem, true);
    fix_free(&fix);
}

/* ==================================================================== *
 * ARM 2 -- complete(), zero media tracks.
 * ==================================================================== */
static void arm_complete_zero_tracks(moq_alloc_t *alloc)
{
    const char *NAME = "complete_drain_zero_tracks";
    int before = failures;
    fix_t fix;
    uint64_t now = 1000;
    if (!fix_init(&fix, alloc, now)) {
        fprintf(stderr, "SKIP-CONSTRAINT %s: ready unreachable with zero "
                "media tracks through the public setup\n", NAME);
        fix_free(&fix);
        printf("CONSTRAINT: %s\n", NAME);
        return;
    }
    if (!subscribe_catalog(&fix, now)) {
        fprintf(stderr, "FAIL %s: catalog subscribe\n", NAME);
        failures++; fix_free(&fix); return;
    }
    if (sticky(&fix, NAME)) { fix_free(&fix); return; }

    int base_n = fix.cat_n;
    /* Zero media tracks were ever added and nothing was published, so the
     * peer's pre-complete inventory is empty. Liveness of the subscription --
     * what makes this arm non-vacuous -- is the SUBSCRIBE_OK checked above,
     * whose declared retained largest group is what the new-group requirement
     * is measured against. */
    MOQ_TEST_CHECK_EQ_INT(fix.cat_n, 0);
    MOQ_TEST_CHECK_EQ_INT(terminal_count(&fix), 0);
    MOQ_TEST_CHECK_EQ_INT(fix.sub_ok_n, 1);
    uint64_t prev_group = prev_group_of(&fix);
    MOQ_TEST_CHECK_EQ_INT((int)prev_group, 0);
    expect_phase(&fix, NAME, PH_LIVE, NULL, 0);

    set_phase(&fix, PH_ACT);
    unsigned wakes0 = moq_endpoint_test_probe_wakes(fix.probe);
    unsigned cycles0 = fix.cycles;
    unsigned services0 = moq_endpoint_test_probe_services(fix.probe);
    unsigned dcalls0 = moq_endpoint_test_probe_drain_calls(fix.probe);
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_complete(fix.s), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_complete(fix.s), (int)MOQ_OK);
    MOQ_TEST_CHECK(moq_endpoint_test_probe_wakes(fix.probe) > wakes0);
    MOQ_TEST_CHECK_EQ_INT((int)(fix.cycles - cycles0), 0);

    settle_no_cycle(&fix);
    MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_drain(fix.ep, 1000000), (int)MOQ_OK);
    assert_drain_drove(&fix, services0, cycles0, dcalls0);
    if (sticky(&fix, NAME)) { fix_free(&fix); return; }

    terminal_oracle(&fix, NAME, base_n, prev_group);
    /* The zero-track terminal lists no tracks, read off the wire. */
    if (fix.cat_n > 0)
        MOQ_TEST_CHECK_EQ_INT((int)fix.cat[fix.cat_n - 1].track_count, 0);
    MOQ_TEST_CHECK(!moq_media_sender_is_fatal(fix.s));
    MOQ_TEST_CHECK(!moq_media_sender_is_closed(fix.s));
    MOQ_TEST_CHECK(!moq_endpoint_is_fatal(fix.ep));
    MOQ_TEST_CHECK(!moq_endpoint_is_closed(fix.ep));
    {   /* exactly the terminal generation, nothing else */
        exp_t e[1];
        memset(e, 0, sizeof(e));
        e[0].kind = MOQ_EVENT_OBJECT_RECEIVED; e[0].sub = fix.cat_sub._opaque;
        e[0].group = 1; e[0].object = 0; e[0].eog = true; e[0].has_payload = true;
        expect_phase(&fix, NAME, PH_ACT, e, 1);
    }
    assert_drain_is_noop(&fix, NAME);
    int drained = failures - before;

    int ibefore = failures;
    set_phase(&fix, PH_RECOVER);
    for (int i = 0; i < 3; i++) cycle(&fix, now);
    terminal_oracle(&fix, NAME, base_n, prev_group);
    expect_phase(&fix, NAME, PH_RECOVER, NULL, 0);
    int idem = failures - ibefore;

    teardown(&fix, NAME, fix.cat_n, fix.med_n, NULL, 0);
    report(NAME, drained, idem, true);
    fix_free(&fix);
}

/* Bring one live media track to a state where the peer subscribes it and the
 * catalog traffic has settled. Leaves the fixture in PH_LIVE. */
static bool media_arm_setup(fix_t *fix, moq_alloc_t *alloc, uint64_t now,
                            const char *NAME)
{
    if (!fix_init(fix, alloc, now)) {
        fprintf(stderr, "FAIL %s: fixture setup\n", NAME);
        failures++; return false;
    }
    if (!subscribe_catalog(fix, now)) {
        fprintf(stderr, "FAIL %s: catalog subscribe\n", NAME);
        failures++; return false;
    }
    if (!add_one_media_track_live(fix, now)) {
        fprintf(stderr, "FAIL %s: live media track add\n", NAME);
        failures++; return false;
    }
    if (!subscribe_track(fix, now, MEDIA_TRACK_NAME, 1, &fix->med_sub)) {
        fprintf(stderr, "FAIL %s: media subscribe\n", NAME);
        failures++; return false;
    }
    fix->med_subscribed = true;
    for (int i = 0; i < 16 && fix->sub_ok_n < 2; i++) cycle(fix, now);
    MOQ_TEST_CHECK_EQ_INT(fix->sub_ok_n, 2);   /* catalog + media, both live */
    MOQ_TEST_CHECK_EQ_INT(fix->med_n, 0);      /* nothing published yet */
    /* Demand is real: the sender sees a subscriber on the media track. */
    MOQ_TEST_CHECK(moq_media_sender_track_has_subscriber(fix->s, fix->media));
    MOQ_TEST_CHECK(moq_media_sender_has_media_subscriber(fix->s));
    /* The media subscription's COMPLETE exposed body, declared like the
     * catalog's: a freshly accepted subscription with nothing delivered. */
    MOQ_TEST_CHECK_EQ_INT((int)fix->med_sub_ok.sub._opaque,
                          (int)fix->med_sub._opaque);
    MOQ_TEST_CHECK_EQ_INT((int)fix->med_sub_ok.track_alias, 2);
    MOQ_TEST_CHECK(!fix->med_sub_ok.has_largest);   /* nothing published yet */
    MOQ_TEST_CHECK_EQ_INT((int)fix->med_sub_ok.largest_group, 0);
    MOQ_TEST_CHECK_EQ_INT((int)fix->med_sub_ok.largest_object, 0);
    MOQ_TEST_CHECK(!fix->med_sub_ok.has_expires);
    MOQ_TEST_CHECK_EQ_INT((int)fix->med_sub_ok.expires_ms, 0);
    MOQ_TEST_CHECK_EQ_INT((int)fix->med_sub_ok_props_len, 0);
    MOQ_TEST_CHECK(!fix->med_sub_ok.dynamic_groups);
    {   /* the complete live inventory: the catalog base generation, the delta
         * that added this track, then the media subscription's own OK */
        exp_t e[3];
        memset(e, 0, sizeof(e));
        e[0].kind = MOQ_EVENT_OBJECT_RECEIVED; e[0].sub = fix->cat_sub._opaque;
        e[0].group = 1; e[0].object = 0; e[0].eog = true; e[0].has_payload = true;
        e[1].kind = MOQ_EVENT_OBJECT_RECEIVED; e[1].sub = fix->cat_sub._opaque;
        e[1].group = 1; e[1].object = 1; e[1].eog = true; e[1].has_payload = true;
        e[2].kind = MOQ_EVENT_SUBSCRIBE_OK; e[2].sub = fix->med_sub._opaque;
        e[2].x = 2;                        /* the media track's alias */
        /* Measured and pinned: unlike the catalog subscription, the media
         * subscription's OK carries NO largest -- the track has published
         * nothing, so there is no retained largest object to report. */
        e[2].has_largest = false;
        expect_phase(fix, NAME, PH_LIVE, e, 3);
    }
    return !sticky(fix, NAME);
}

/* ==================================================================== *
 * ARM 3 -- an ordinary DEMANDED media object, queued by public write().
 * The same drain contract, with no completion involved.
 * ==================================================================== */
static void arm_queued_media_object(moq_alloc_t *alloc)
{
    const char *NAME = "queued_media_object_drain";
    int before = failures;
    fix_t fix;
    uint64_t now = 1000;
    if (!media_arm_setup(&fix, alloc, now, NAME)) { fix_free(&fix); return; }

    set_phase(&fix, PH_ACT);
    unsigned wakes0 = moq_endpoint_test_probe_wakes(fix.probe);
    unsigned cycles0 = fix.cycles;
    unsigned services0 = moq_endpoint_test_probe_services(fix.probe);
    unsigned dcalls0 = moq_endpoint_test_probe_drain_calls(fix.probe);

    moq_rcbuf_t *payload = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_rcbuf_create(&fix.alloc, MEDIA_PAYLOAD,
                                                sizeof(MEDIA_PAYLOAD),
                                                &payload),
                          (int)MOQ_OK);
    MOQ_TEST_CHECK(payload != NULL);
    moq_media_send_object_t so;
    memset(&so, 0, sizeof(so));
    so.struct_size = sizeof(so);
    so.payload = payload;
    so.is_sync = true;          /* sync anchor: the queue is sync-anchored */
    so.starts_group = true;
    so.presentation_time_us = 5000;
    /* On MOQ_OK the payload reference is TRANSFERRED to the send queue
     * (media_sender.h: "no ownership taken" only on the failure returns), so
     * the fixture must not decref it here. */
    moq_result_t wrc = moq_media_sender_write(fix.s, fix.media, &so);
    MOQ_TEST_CHECK_EQ_INT((int)wrc, (int)MOQ_OK);
    if (wrc != MOQ_OK) moq_rcbuf_decref(payload);

    /* write() woke this endpoint and ran no hook of its own. */
    MOQ_TEST_CHECK(moq_endpoint_test_probe_wakes(fix.probe) > wakes0);
    MOQ_TEST_CHECK_EQ_INT((int)(fix.cycles - cycles0), 0);

    settle_no_cycle(&fix);
    MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_drain(fix.ep, 1000000), (int)MOQ_OK);
    assert_drain_drove(&fix, services0, cycles0, dcalls0);
    if (sticky(&fix, NAME)) { fix_free(&fix); return; }

    /* THE CONTRACT: drain() reported the flush, so the accepted object has
     * reached the peer -- byte for byte, exactly once. */
    MOQ_TEST_CHECK_EQ_INT(fix.med_n, 1);
    if (fix.med_n == 1) {
        MOQ_TEST_CHECK_EQ_INT((int)fix.med[0].status, (int)MOQ_OBJECT_NORMAL);
        MOQ_TEST_CHECK_EQ_INT((int)fix.med[0].len, (int)sizeof(MEDIA_PAYLOAD));
        MOQ_TEST_CHECK(fix.med[0].len == sizeof(MEDIA_PAYLOAD) &&
                       memcmp(fix.med[0].buf, MEDIA_PAYLOAD,
                              sizeof(MEDIA_PAYLOAD)) == 0);
    }
    {   /* exactly the one media object, on the media subscription */
        exp_t e[1];
        memset(e, 0, sizeof(e));
        e[0].kind = MOQ_EVENT_OBJECT_RECEIVED; e[0].sub = fix.med_sub._opaque;
        e[0].group = 0; e[0].object = 0; e[0].eog = true; e[0].has_payload = true;
        /* Measured: a RAW/LOC media object carries the service-generated
         * property block and the default publisher priority; the catalog
         * generations carry neither. The property BYTES belong to the LOC
         * codec's own tests -- this inventory pins only their presence. */
        e[0].prio = 128; e[0].has_props = true;
        expect_phase(&fix, NAME, PH_ACT, e, 1);
    }
    assert_drain_is_noop(&fix, NAME);
    int drained = failures - before;

    int ibefore = failures;
    set_phase(&fix, PH_RECOVER);
    for (int i = 0; i < 3; i++) cycle(&fix, now);
    MOQ_TEST_CHECK_EQ_INT(fix.med_n, 1);       /* exactly once, no duplicate */
    expect_phase(&fix, NAME, PH_RECOVER, NULL, 0);
    int idem = failures - ibefore;

    teardown(&fix, NAME, fix.cat_n, fix.med_n, NULL, 0);
    report(NAME, drained, idem, true);
    fix_free(&fix);
}

/* ==================================================================== *
 * ARM 4 -- the zero-byte END_OF_TRACK marker, queued by public end_track().
 * It carries no payload at all, so a predicate that looked only at queued
 * BYTES would miss it.
 * ==================================================================== */
static void arm_queued_end_of_track(moq_alloc_t *alloc)
{
    const char *NAME = "queued_end_of_track_drain";
    int before = failures;
    fix_t fix;
    uint64_t now = 1000;
    if (!media_arm_setup(&fix, alloc, now, NAME)) { fix_free(&fix); return; }

    set_phase(&fix, PH_ACT);
    unsigned wakes0 = moq_endpoint_test_probe_wakes(fix.probe);
    unsigned cycles0 = fix.cycles;
    unsigned services0 = moq_endpoint_test_probe_services(fix.probe);
    unsigned dcalls0 = moq_endpoint_test_probe_drain_calls(fix.probe);
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_end_track(fix.s, fix.media),
                          (int)MOQ_OK);
    /* Idempotent per the header contract. */
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_end_track(fix.s, fix.media),
                          (int)MOQ_OK);
    MOQ_TEST_CHECK(moq_endpoint_test_probe_wakes(fix.probe) > wakes0);
    MOQ_TEST_CHECK_EQ_INT((int)(fix.cycles - cycles0), 0);

    settle_no_cycle(&fix);
    MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_drain(fix.ep, 1000000), (int)MOQ_OK);
    assert_drain_drove(&fix, services0, cycles0, dcalls0);
    if (sticky(&fix, NAME)) { fix_free(&fix); return; }

    MOQ_TEST_CHECK_EQ_INT(fix.med_n, 1);
    if (fix.med_n == 1) {
        MOQ_TEST_CHECK_EQ_INT((int)fix.med[0].status,
                              (int)MOQ_OBJECT_END_OF_TRACK);
        MOQ_TEST_CHECK_EQ_INT((int)fix.med[0].len, 0);   /* zero-byte marker */
    }
    {   /* the zero-byte END_OF_TRACK marker, then its subgroup terminal */
        exp_t e[2];
        memset(e, 0, sizeof(e));
        e[0].kind = MOQ_EVENT_OBJECT_RECEIVED; e[0].sub = fix.med_sub._opaque;
        /* Measured: the END_OF_TRACK marker's subgroup does NOT carry the
         * END_OF_GROUP header bit, unlike the catalog generations. */
        e[0].group = 0; e[0].object = 0; e[0].eog = false;
        e[0].status = MOQ_OBJECT_END_OF_TRACK; e[0].has_payload = false;
        e[0].prio = 128;                  /* a status object carries no props */
        e[1].kind = MOQ_EVENT_SUBGROUP_FINISHED; e[1].sub = fix.med_sub._opaque;
        e[1].group = 0; e[1].eog = false;
        expect_phase(&fix, NAME, PH_ACT, e, 2);
    }
    assert_drain_is_noop(&fix, NAME);
    int drained = failures - before;

    int ibefore = failures;
    set_phase(&fix, PH_RECOVER);
    for (int i = 0; i < 3; i++) cycle(&fix, now);
    MOQ_TEST_CHECK_EQ_INT(fix.med_n, 1);       /* exactly once, no duplicate */
    expect_phase(&fix, NAME, PH_RECOVER, NULL, 0);
    int idem = failures - ibefore;

    teardown(&fix, NAME, fix.cat_n, fix.med_n, NULL, 0);
    report(NAME, drained, idem, true);
    fix_free(&fix);
}

/* ==================================================================== *
 * CONTROL -- the INITIAL-SETUP term, isolated.
 * drain() is called immediately after attach, before any cycle has run, so
 * the ONLY owed output is the initial catalog + namespace advertisement.
 * drain() must drive that handoff: afterwards the peer has seen exactly one
 * advertisement. Without the term drain would return at once with nothing
 * advertised.
 * ==================================================================== */
static void control_initial_setup_is_owed(moq_alloc_t *alloc)
{
    const char *NAME = "control_initial_setup_is_owed";
    int before = failures;
    fix_t fix;
    uint64_t now = 1000;
    memset(&fix, 0, sizeof(fix));
    fix.alloc = *alloc;
    fix.now = now;
    set_phase(&fix, PH_SETUP);
    fix.sp = make_pair(alloc);
    MOQ_TEST_CHECK(fix.sp != NULL);
    if (!fix.sp) { failures++; fix_free(&fix); return; }
    fix.ep = moq_endpoint_test_make_bare_alloc(alloc);
    MOQ_TEST_CHECK(fix.ep != NULL);
    if (!fix.ep) { failures++; fix_free(&fix); return; }
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_endpoint_test_install_probe(fix.ep, &fix.probe_mem,
                                             sizeof(fix.probe_mem),
                                             PROBE_DRAIN_DONE), (int)MOQ_OK);
    fix.probe = &fix.probe_mem;
    moq_endpoint_test_probe_set_service(fix.probe, probe_service, &fix);

    g_ns[0] = MOQ_BYTES_LITERAL("svc");
    g_ns[1] = MOQ_BYTES_LITERAL("done");
    moq_media_sender_cfg_t cfg;
    moq_media_sender_cfg_init_live_sized(&cfg, sizeof(cfg));
    cfg.namespace_ = (moq_namespace_t){ g_ns, 2 };
    cfg.catalog_refresh_interval_us = UINT64_MAX;
    moq_media_sender_callbacks_init_sized(&cfg.callbacks, sizeof(cfg.callbacks));
    cfg.callbacks.ctx = &fix;
    cfg.callbacks.on_ready = on_ready_cb;
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_attach(fix.ep, &cfg, &fix.s),
                          (int)MOQ_OK);
    if (!fix.s) { failures++; fix_free(&fix); return; }

    /* NOT ONE cycle has run: the initial catalog is owed and nothing else. */
    MOQ_TEST_CHECK(moq_media_sender_test_pending_send_work(fix.s));
    MOQ_TEST_CHECK_EQ_INT((int)fix.cycles, 0);
    MOQ_TEST_CHECK_EQ_INT(fix.ns_pub_n, 0);
    MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_drain(fix.ep, 1000000), (int)MOQ_OK);
    /* drain drove the setup handoff, and the peer saw the advertisement. */
    MOQ_TEST_CHECK(fix.cycles > 0);
    MOQ_TEST_CHECK_EQ_INT(fix.ns_pub_n, 1);
    MOQ_TEST_CHECK(!moq_media_sender_test_pending_send_work(fix.s));
    {
        exp_t e[1];
        memset(e, 0, sizeof(e));
        e[0].kind = MOQ_EVENT_NAMESPACE_PUBLISHED;
        e[0].ann = fix.ann_handle;
        expect_phase(&fix, NAME, PH_SETUP, e, 1);
    }

    teardown(&fix, NAME, fix.cat_n, fix.med_n, NULL, 0);
    report(NAME, failures - before, 0, false);
    fix_free(&fix);
}

/* ==================================================================== *
 * CONTROL -- `ready` is not ITSELF a pending obligation.
 * The initial catalog and namespace advertisement have been handed to the core,
 * but the peer deliberately never accepts the namespace, so the sender stays
 * not-ready forever. In this ZERO-OUTPUT state -- nothing queued, nothing dirty
 * -- no local output is owed, so drain() must return immediately. This kills an
 * implementation that reports pending merely because !ready, which would turn a
 * local flush into a peer-acceptance barrier.
 *
 * Scope, stated precisely: this row says readiness ALONE is not an obligation.
 * It does NOT say a protocol gate can never hold drain -- accepted output that
 * cannot reach the core because readiness or track demand has not opened is a
 * real obligation and correctly holds drain pending. That complementary fact is
 * the control_no_demand_queue_holds_drain row.
 * ==================================================================== */
static void ready_alone_is_not_pending_control(moq_alloc_t *alloc)
{
    const char *NAME = "control_ready_alone_is_not_pending";
    int before = failures;
    fix_t fix;
    uint64_t now = 1000;
    memset(&fix, 0, sizeof(fix));
    fix.alloc = *alloc;
    fix.now = now;
    fix.withhold_accept = true;          /* the peer never accepts */
    set_phase(&fix, PH_SETUP);
    fix.sp = make_pair(alloc);
    MOQ_TEST_CHECK(fix.sp != NULL);
    if (!fix.sp) { fix_free(&fix); failures++; return; }
    fix.ep = moq_endpoint_test_make_bare_alloc(alloc);
    MOQ_TEST_CHECK(fix.ep != NULL);
    if (!fix.ep) { fix_free(&fix); failures++; return; }
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_endpoint_test_install_probe(fix.ep, &fix.probe_mem,
                                             sizeof(fix.probe_mem),
                                             PROBE_DRAIN_DONE),
        (int)MOQ_OK);
    fix.probe = &fix.probe_mem;
    moq_endpoint_test_probe_set_service(fix.probe, probe_service, &fix);

    g_ns[0] = MOQ_BYTES_LITERAL("svc");
    g_ns[1] = MOQ_BYTES_LITERAL("done");
    moq_media_sender_cfg_t cfg;
    moq_media_sender_cfg_init_live_sized(&cfg, sizeof(cfg));
    cfg.namespace_ = (moq_namespace_t){ g_ns, 2 };
    cfg.catalog_refresh_interval_us = UINT64_MAX;
    moq_media_sender_callbacks_init_sized(&cfg.callbacks, sizeof(cfg.callbacks));
    cfg.callbacks.ctx = &fix;
    cfg.callbacks.on_ready = on_ready_cb;
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_attach(fix.ep, &cfg, &fix.s),
                          (int)MOQ_OK);
    if (!fix.s) { fix_free(&fix); failures++; return; }

    /* Enough cycles for the initial catalog + advertisement to be handed to
     * the core; the peer sees the advertisement and deliberately ignores it. */
    for (int i = 0; i < 8; i++) cycle(&fix, now);
    MOQ_TEST_CHECK_EQ_INT(fix.ns_pub_n, 1);        /* it really was advertised */
    MOQ_TEST_CHECK(!moq_media_sender_is_ready(fix.s));   /* still not ready */
    MOQ_TEST_CHECK_EQ_INT(fix.ready_n, 0);

    set_phase(&fix, PH_ACT);
    /* No queued media, no catalog mutation: nothing LOCAL is owed. */
    assert_drain_is_noop(&fix, NAME);
    MOQ_TEST_CHECK(!moq_media_sender_is_ready(fix.s));   /* unchanged by drain */
    expect_phase(&fix, NAME, PH_ACT, NULL, 0);
    {
        exp_t e[1];
        memset(e, 0, sizeof(e));
        e[0].kind = MOQ_EVENT_NAMESPACE_PUBLISHED;
        e[0].ann = fix.ann_handle;
        expect_phase(&fix, NAME, PH_SETUP, e, 1);
    }

    teardown(&fix, NAME, fix.cat_n, fix.med_n, NULL, 0);
    report(NAME, failures - before, 0, false);
    fix_free(&fix);
}

/* ==================================================================== *
 * ROW -- a STAGED generation whose install is refused.
 * The first hook clears catalog_dirty and stages the generation, then the
 * retained install is refused, so the obligation lives ONLY in
 * pending_obj_count. drain() must keep going and finish it exactly once.
 * This kills a predicate that checks catalog_dirty but omits the staged state.
 * ==================================================================== */
static void arm_staged_generation_refusal(moq_alloc_t *alloc)
{
    const char *NAME = "staged_generation_refusal_drain";
    int before = failures;
    fix_t fix;
    uint64_t now = 1000;
    if (!fix_init(&fix, alloc, now)) {
        fprintf(stderr, "FAIL %s: fixture setup\n", NAME);
        failures++; fix_free(&fix); return;
    }
    if (!subscribe_catalog(&fix, now)) {
        fprintf(stderr, "FAIL %s: catalog subscribe\n", NAME);
        failures++; fix_free(&fix); return;
    }
    MOQ_TEST_CHECK(add_one_media_track_live(&fix, now));
    if (sticky(&fix, NAME)) { fix_free(&fix); return; }
    int base_n = fix.cat_n;
    uint64_t prev_group = prev_group_of(&fix);
    {
        exp_t e[2];
        memset(e, 0, sizeof(e));
        e[0].kind = MOQ_EVENT_OBJECT_RECEIVED; e[0].sub = fix.cat_sub._opaque;
        e[0].group = 1; e[0].object = 0; e[0].eog = true; e[0].has_payload = true;
        e[1].kind = MOQ_EVENT_OBJECT_RECEIVED; e[1].sub = fix.cat_sub._opaque;
        e[1].group = 1; e[1].object = 1; e[1].eog = true; e[1].has_payload = true;
        expect_phase(&fix, NAME, PH_LIVE, e, 2);
    }

    set_phase(&fix, PH_ACT);
    unsigned cycles0 = fix.cycles;
    unsigned services0 = moq_endpoint_test_probe_services(fix.probe);
    /* Refuse the retained install of the NEXT staged generation, once. */
    moq_media_sender_test_block_retained_install_once(fix.s);
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_complete(fix.s), (int)MOQ_OK);
    MOQ_TEST_CHECK(moq_media_sender_test_catalog_dirty(fix.s));

    settle_no_cycle(&fix);
    MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_drain(fix.ep, 1000000), (int)MOQ_OK);
    /* More than one serviced cycle was needed: the first staged and was
     * refused, a later one completed the install. */
    MOQ_TEST_CHECK(moq_endpoint_test_probe_services(fix.probe) - services0 >= 2);
    MOQ_TEST_CHECK(fix.cycles - cycles0 >= 2);
    MOQ_TEST_CHECK(!moq_media_sender_test_catalog_dirty(fix.s));
    if (sticky(&fix, NAME)) { fix_free(&fix); return; }
    terminal_oracle(&fix, NAME, base_n, prev_group);
    {   /* the same output as the unrefused arm, once the retry lands */
        exp_t e[2];
        memset(e, 0, sizeof(e));
        e[0].kind = MOQ_EVENT_SUBGROUP_FINISHED; e[0].sub = fix.cat_sub._opaque;
        e[0].group = 1; e[0].eog = true;
        e[1].kind = MOQ_EVENT_OBJECT_RECEIVED; e[1].sub = fix.cat_sub._opaque;
        e[1].group = 2; e[1].object = 0; e[1].eog = true; e[1].has_payload = true;
        expect_phase(&fix, NAME, PH_ACT, e, 2);
    }
    assert_drain_is_noop(&fix, NAME);
    int drained = failures - before;

    int ibefore = failures;
    set_phase(&fix, PH_RECOVER);
    for (int i = 0; i < 3; i++) cycle(&fix, now);
    terminal_oracle(&fix, NAME, base_n, prev_group);
    expect_phase(&fix, NAME, PH_RECOVER, NULL, 0);
    int idem = failures - ibefore;

    teardown(&fix, NAME, fix.cat_n, fix.med_n, NULL, 0);
    report(NAME, drained, idem, true);
    fix_free(&fix);
}

/* ==================================================================== *
 * PREDICATE CONTROL -- the cached answer, state by state.
 * Asserts the scalar the hook callback returns at each point of a real
 * sequence, so an omitted term is visible directly and not only through a
 * downstream drain. Covers the terms this fixture can reach in isolation:
 * settled(false), physical ring, catalog_dirty, and the staged generation.
 * ==================================================================== */
static void control_pending_predicate_states(moq_alloc_t *alloc)
{
    const char *NAME = "control_pending_predicate_states";
    int before = failures;
    fix_t fix;
    uint64_t now = 1000;
    if (!media_arm_setup(&fix, alloc, now, NAME)) { fix_free(&fix); return; }

    set_phase(&fix, PH_ACT);
    /* settled: nothing owed */
    MOQ_TEST_CHECK(!moq_media_sender_test_pending_send_work(fix.s));

    /* physical ring: one queued media object */
    moq_rcbuf_t *payload = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_rcbuf_create(&fix.alloc, MEDIA_PAYLOAD,
                                                sizeof(MEDIA_PAYLOAD),
                                                &payload), (int)MOQ_OK);
    moq_media_send_object_t so;
    memset(&so, 0, sizeof(so));
    so.struct_size = sizeof(so);
    so.payload = payload;
    so.is_sync = true;
    so.starts_group = true;
    so.presentation_time_us = 5000;
    moq_result_t wrc = moq_media_sender_write(fix.s, fix.media, &so);
    MOQ_TEST_CHECK_EQ_INT((int)wrc, (int)MOQ_OK);
    if (wrc != MOQ_OK) moq_rcbuf_decref(payload);
    MOQ_TEST_CHECK(moq_media_sender_test_pending_send_work(fix.s));
    MOQ_TEST_CHECK(!moq_media_sender_test_catalog_dirty(fix.s));  /* ring only */
    cycle(&fix, now);
    MOQ_TEST_CHECK(!moq_media_sender_test_pending_send_work(fix.s));

    /* catalog_dirty: a completion mutation, not yet staged */
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_complete(fix.s), (int)MOQ_OK);
    MOQ_TEST_CHECK(moq_media_sender_test_catalog_dirty(fix.s));
    MOQ_TEST_CHECK(moq_media_sender_test_pending_send_work(fix.s));

    /* staged generation: dirty is CLEARED by the stage, the obligation now
     * lives only in the staged state, and the answer must still be true. */
    moq_media_sender_test_block_retained_install_once(fix.s);
    cycle(&fix, now);
    MOQ_TEST_CHECK(!moq_media_sender_test_catalog_dirty(fix.s));
    MOQ_TEST_CHECK(moq_media_sender_test_pending_send_work(fix.s));

    /* and it settles to false once drain finishes it */
    MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_drain(fix.ep, 1000000), (int)MOQ_OK);
    MOQ_TEST_CHECK(!moq_media_sender_test_pending_send_work(fix.s));
    MOQ_TEST_CHECK_EQ_INT(terminal_count(&fix), 1);
    /* complete() also ends the media track, so the drain carries the whole
     * MSF §11.3 sequence: the queued object, its subgroup terminal, the
     * END_OF_TRACK marker and ITS terminal, the subscription DONE, then the
     * catalog group terminal and the terminal generation. That makes this
     * control the one place where the removed-track teardown term is
     * exercised end to end. */
    MOQ_TEST_CHECK_EQ_INT(fix.med_n, 2);
    {
        exp_t e[7];
        memset(e, 0, sizeof(e));
        e[0].kind = MOQ_EVENT_OBJECT_RECEIVED; e[0].sub = fix.med_sub._opaque;
        e[0].group = 0; e[0].object = 0; e[0].eog = true; e[0].has_payload = true;
        e[0].prio = 128; e[0].has_props = true;
        e[1].kind = MOQ_EVENT_SUBGROUP_FINISHED; e[1].sub = fix.med_sub._opaque;
        e[1].group = 0; e[1].eog = true;
        e[2].kind = MOQ_EVENT_OBJECT_RECEIVED; e[2].sub = fix.med_sub._opaque;
        e[2].group = 1; e[2].object = 0; e[2].eog = false;
        e[2].status = MOQ_OBJECT_END_OF_TRACK; e[2].prio = 128;
        e[3].kind = MOQ_EVENT_SUBGROUP_FINISHED; e[3].sub = fix.med_sub._opaque;
        e[3].group = 1; e[3].eog = false;
        e[4].kind = MOQ_EVENT_SUBSCRIBE_DONE; e[4].sub = fix.med_sub._opaque;
        e[4].x = 2;   /* status: track ended */
        e[4].y = 2;   /* stream count: the media subgroup and the terminal */
        e[5].kind = MOQ_EVENT_SUBGROUP_FINISHED; e[5].sub = fix.cat_sub._opaque;
        e[5].group = 1; e[5].eog = true;
        e[6].kind = MOQ_EVENT_OBJECT_RECEIVED; e[6].sub = fix.cat_sub._opaque;
        e[6].group = 2; e[6].object = 0; e[6].eog = true; e[6].has_payload = true;
        expect_phase(&fix, NAME, PH_ACT, e, 7);
    }

    teardown(&fix, NAME, fix.cat_n, fix.med_n, NULL, 0);
    report(NAME, failures - before, 0, false);
    fix_free(&fix);
}

/* Write one sync-anchored object with the fixture's declared payload. */
static moq_result_t write_one_sap(fix_t *fix, uint64_t pts, bool declare_sap)
{
    moq_rcbuf_t *payload = NULL;
    if (moq_rcbuf_create(&fix->alloc, MEDIA_PAYLOAD, sizeof(MEDIA_PAYLOAD),
                         &payload) != MOQ_OK)
        return MOQ_ERR_NOMEM;
    moq_media_send_object_t so;
    memset(&so, 0, sizeof(so));
    so.struct_size = sizeof(so);
    so.payload = payload;
    so.is_sync = true;
    so.starts_group = true;
    so.presentation_time_us = pts;
    if (declare_sap) { so.has_sap_type = true; so.sap_type = MOQ_SAP_TYPE_1; }
    moq_result_t rc = moq_media_sender_write(fix->s, fix->media, &so);
    if (rc != MOQ_OK) moq_rcbuf_decref(payload);   /* no transfer on failure */
    return rc;
}

static moq_result_t write_one(fix_t *fix, uint64_t pts)
{
    return write_one_sap(fix, pts, false);
}

/* ==================================================================== *
 * REFERENCE -- the finite publication tail, in both orders.
 *
 * The same tail is queued before the first drive in both rows: three
 * DEMANDED media objects with distinct payloads, then the track's
 * END_OF_TRACK. complete() removes every track, and a removed track's queued
 * media is discarded by design (test_media_sender_pacing.c's completion
 * accounting row pins that), so:
 *
 *   - complete first, then drain: the LOSS reference. The terminal catalog
 *     arrives; the queued media does not, and is counted as dropped.
 *   - drain, complete, drain: PRESERVATION. The first drain carries every
 *     accepted object and the track's terminal to the peer before completion
 *     is requested; the second carries the terminal catalog.
 *
 * Both are pinned as what they are -- neither is an expected-success row for
 * the other's order -- with the exact payloads, the track and catalog
 * terminals, every peer event, the sender's own accounting, and no duplicate
 * after further cycles.
 * ==================================================================== */
static const uint8_t TAIL_PAYLOAD[3][4] = {
    { 0x10, 0x11, 0x12, 0x13 },
    { 0x20, 0x21, 0x22, 0x23 },
    { 0x30, 0x31, 0x32, 0x33 },
};

static moq_result_t write_tail(fix_t *fix, int i)
{
    moq_rcbuf_t *payload = NULL;
    if (moq_rcbuf_create(&fix->alloc, TAIL_PAYLOAD[i], sizeof(TAIL_PAYLOAD[i]),
                         &payload) != MOQ_OK)
        return MOQ_ERR_NOMEM;
    moq_media_send_object_t so;
    memset(&so, 0, sizeof(so));
    so.struct_size = sizeof(so);
    so.payload = payload;
    so.is_sync = true;
    so.starts_group = true;
    so.presentation_time_us = 5000u + (uint64_t)i * 1000u;
    moq_result_t rc = moq_media_sender_write(fix->s, fix->media, &so);
    if (rc != MOQ_OK) moq_rcbuf_decref(payload);   /* no transfer on failure */
    return rc;
}

static void tail_stats(fix_t *fix, moq_media_sender_stats_t *st)
{
    memset(st, 0, sizeof(*st));
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_media_sender_get_stats(fix->s, st, sizeof(*st)), (int)MOQ_OK);
}

static void finite_tail_row(moq_alloc_t *alloc, bool drain_first)
{
    const char *NAME = drain_first ? "finite_tail_drain_complete_drain"
                                   : "finite_tail_complete_first_loses_tail";
    int before = failures;
    fix_t fix;
    uint64_t now = 1000;
    if (!media_arm_setup(&fix, alloc, now, NAME)) { fix_free(&fix); return; }

    set_phase(&fix, PH_ACT);
    unsigned cycles0 = fix.cycles;
    for (int i = 0; i < 3; i++)
        MOQ_TEST_CHECK_EQ_INT((int)write_tail(&fix, i), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_end_track(fix.s, fix.media),
                          (int)MOQ_OK);
    /* The whole tail is queued; nothing has been driven. */
    MOQ_TEST_CHECK_EQ_INT((int)(fix.cycles - cycles0), 0);
    settle_no_cycle(&fix);
    MOQ_TEST_CHECK_EQ_INT(fix.med_n, 0);
    int base_n = fix.cat_n;
    uint64_t prev_group = prev_group_of(&fix);
    MOQ_TEST_CHECK_EQ_INT(terminal_count(&fix), 0);

    if (drain_first) {
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_drain(fix.ep, 1000000),
                              (int)MOQ_OK);
        /* THE FIRST BARRIER: every accepted object and the track's terminal
         * reached the peer, and no completion exists yet. */
        MOQ_TEST_CHECK_EQ_INT(fix.med_n, 4);
        MOQ_TEST_CHECK_EQ_INT(terminal_count(&fix), 0);
        MOQ_TEST_CHECK_EQ_INT(fix.cat_n, base_n);
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_complete(fix.s), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_drain(fix.ep, 1000000),
                              (int)MOQ_OK);
    } else {
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_complete(fix.s), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_drain(fix.ep, 1000000),
                              (int)MOQ_OK);
    }
    if (sticky(&fix, NAME)) { fix_free(&fix); return; }

    terminal_oracle(&fix, NAME, base_n, prev_group);
    MOQ_TEST_CHECK(!moq_media_sender_is_fatal(fix.s));
    MOQ_TEST_CHECK(!moq_endpoint_is_fatal(fix.ep));
    {   /* The sender's own accounting: every written object is either sent
         * or dropped, and nothing is left queued. */
        moq_media_sender_stats_t st;
        tail_stats(&fix, &st);
        MOQ_TEST_CHECK_EQ_INT((int)st.objects_written, 3);
        MOQ_TEST_CHECK_EQ_INT((int)st.objects_queued, 0);
        MOQ_TEST_CHECK_EQ_INT((int)(st.objects_sent + st.objects_dropped), 3);
        MOQ_TEST_CHECK_EQ_INT((int)st.objects_sent, drain_first ? 3 : 0);
        MOQ_TEST_CHECK_EQ_INT((int)st.objects_dropped, drain_first ? 0 : 3);
        MOQ_TEST_CHECK_EQ_INT((int)st.groups_dropped, drain_first ? 0 : 3);
        MOQ_TEST_CHECK_EQ_INT((int)st.keyframes_dropped, drain_first ? 0 : 3);
        MOQ_TEST_CHECK_EQ_INT((int)st.groups_abandoned, 0);
    }
    /* The media the peer received, byte for byte: all three objects in
     * order and then the track's terminal -- or, completing first, the
     * terminal alone. */
    int objects = drain_first ? 3 : 0;
    MOQ_TEST_CHECK_EQ_INT(fix.med_n, objects + 1);
    for (int i = 0; i < objects && i < fix.med_n; i++) {
        MOQ_TEST_CHECK_EQ_INT((int)fix.med[i].status, (int)MOQ_OBJECT_NORMAL);
        MOQ_TEST_CHECK_EQ_INT((int)fix.med[i].group, i);
        MOQ_TEST_CHECK(fix.med[i].len == sizeof(TAIL_PAYLOAD[i]) &&
                       memcmp(fix.med[i].buf, TAIL_PAYLOAD[i],
                              sizeof(TAIL_PAYLOAD[i])) == 0);
    }
    if (fix.med_n == objects + 1) {
        MOQ_TEST_CHECK_EQ_INT((int)fix.med[objects].status,
                              (int)MOQ_OBJECT_END_OF_TRACK);
        MOQ_TEST_CHECK_EQ_INT((int)fix.med[objects].group, objects);
        MOQ_TEST_CHECK_EQ_INT((int)fix.med[objects].len, 0);
    }
    {   /* Every peer event, in order: the media groups (preserving order
         * only), the track's terminal and its subscription's end, then the
         * catalog's group close and its terminal generation. */
        exp_t e[11];
        memset(e, 0, sizeof(e));
        int n = 0;
        for (int i = 0; i < objects; i++) {
            e[n].kind = MOQ_EVENT_OBJECT_RECEIVED; e[n].sub = fix.med_sub._opaque;
            e[n].group = (uint64_t)i; e[n].object = 0; e[n].eog = true;
            e[n].has_payload = true; e[n].prio = 128; e[n].has_props = true;
            n++;
            e[n].kind = MOQ_EVENT_SUBGROUP_FINISHED; e[n].sub = fix.med_sub._opaque;
            e[n].group = (uint64_t)i; e[n].eog = true;
            n++;
        }
        e[n].kind = MOQ_EVENT_OBJECT_RECEIVED; e[n].sub = fix.med_sub._opaque;
        e[n].group = (uint64_t)objects; e[n].object = 0; e[n].eog = false;
        e[n].status = MOQ_OBJECT_END_OF_TRACK; e[n].prio = 128;
        n++;
        e[n].kind = MOQ_EVENT_SUBGROUP_FINISHED; e[n].sub = fix.med_sub._opaque;
        e[n].group = (uint64_t)objects; e[n].eog = false;
        n++;
        e[n].kind = MOQ_EVENT_SUBSCRIBE_DONE; e[n].sub = fix.med_sub._opaque;
        e[n].x = 2; e[n].y = (uint64_t)(objects + 1);   /* one stream per group */
        n++;
        e[n].kind = MOQ_EVENT_SUBGROUP_FINISHED; e[n].sub = fix.cat_sub._opaque;
        e[n].group = 1; e[n].eog = true;
        n++;
        e[n].kind = MOQ_EVENT_OBJECT_RECEIVED; e[n].sub = fix.cat_sub._opaque;
        e[n].group = 2; e[n].object = 0; e[n].eog = true; e[n].has_payload = true;
        n++;
        expect_phase(&fix, NAME, PH_ACT, e, n);
    }
    int drained = failures - before;

    /* ---- separately counted idempotence: nothing more, ever ----------- */
    int ibefore = failures;
    set_phase(&fix, PH_RECOVER);
    for (int i = 0; i < 3; i++) cycle(&fix, now);
    MOQ_TEST_CHECK_EQ_INT(fix.med_n, objects + 1);
    terminal_oracle(&fix, NAME, base_n, prev_group);
    expect_phase(&fix, NAME, PH_RECOVER, NULL, 0);
    int idem = failures - ibefore;

    teardown(&fix, NAME, fix.cat_n, fix.med_n, NULL, 0);
    report(NAME, drained, idem, true);
    fix_free(&fix);
}

/* ==================================================================== *
 * CONTROL -- the PHYSICAL RING term, isolated.
 * An object written to a track with NO subscriber is HELD in the ring (the
 * default no-demand policy holds rather than drops), so a hook runs to
 * completion with the ring still occupied and no other term true. The cached
 * answer must stay true and drain must NOT report a flush. This is the state
 * that makes the ring term load-bearing: the eager setter alone would have
 * been cleared by the recompute at the end of that hook.
 * ==================================================================== */
static void control_no_demand_queue_holds_drain(moq_alloc_t *alloc)
{
    const char *NAME = "control_no_demand_queue_holds_drain";
    int before = failures;
    fix_t fix;
    uint64_t now = 1000;
    if (!fix_init(&fix, alloc, now)) {
        fprintf(stderr, "FAIL %s: fixture setup\n", NAME);
        failures++; fix_free(&fix); return;
    }
    if (!subscribe_catalog(&fix, now)) {
        fprintf(stderr, "FAIL %s: catalog subscribe\n", NAME);
        failures++; fix_free(&fix); return;
    }
    MOQ_TEST_CHECK(add_one_media_track_live(&fix, now));
    /* Deliberately NO media subscription: the track has no demand. */
    MOQ_TEST_CHECK(!moq_media_sender_track_has_subscriber(fix.s, fix.media));
    if (sticky(&fix, NAME)) { fix_free(&fix); return; }
    {   /* the catalog base generation and the delta that added the track */
        exp_t e[2];
        memset(e, 0, sizeof(e));
        e[0].kind = MOQ_EVENT_OBJECT_RECEIVED; e[0].sub = fix.cat_sub._opaque;
        e[0].group = 1; e[0].object = 0; e[0].eog = true; e[0].has_payload = true;
        e[1].kind = MOQ_EVENT_OBJECT_RECEIVED; e[1].sub = fix.cat_sub._opaque;
        e[1].group = 1; e[1].object = 1; e[1].eog = true; e[1].has_payload = true;
        expect_phase(&fix, NAME, PH_LIVE, e, 2);
    }

    set_phase(&fix, PH_ACT);
    MOQ_TEST_CHECK_EQ_INT((int)write_one(&fix, 5000), (int)MOQ_OK);
    MOQ_TEST_CHECK(moq_media_sender_test_pending_send_work(fix.s));
    /* A full production cycle runs and the object is still queued: the hook's
     * own recompute -- not the setter -- is what keeps the answer true. */
    cycle(&fix, now);
    MOQ_TEST_CHECK(moq_media_sender_test_pending_send_work(fix.s));
    MOQ_TEST_CHECK(!moq_media_sender_test_catalog_dirty(fix.s));
    {
        moq_media_sender_stats_t st; memset(&st, 0, sizeof(st));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_media_sender_get_stats(fix.s, &st, sizeof(st)), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)st.objects_written, 1);
        MOQ_TEST_CHECK_EQ_INT((int)st.objects_sent, 0);   /* held, not sent */
        MOQ_TEST_CHECK_EQ_INT((int)st.objects_queued, 1);
        MOQ_TEST_CHECK_EQ_INT((int)st.objects_dropped, 0);
    }
    /* drain() must NOT claim a flush while that object is owed. */
    MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_drain(fix.ep, 20000), (int)MOQ_DONE);
    MOQ_TEST_CHECK_EQ_INT(fix.med_n, 0);
    MOQ_TEST_CHECK(moq_media_sender_test_pending_send_work(fix.s));
    expect_phase(&fix, NAME, PH_ACT, NULL, 0);

    teardown(&fix, NAME, fix.cat_n, fix.med_n, NULL, 0);
    report(NAME, failures - before, 0, false);
    fix_free(&fix);
}

/* The one-shot fired from inside the transport snapshot: models the managed
 * context accepting new send work exactly while drain is taking the backend's
 * answer. Callers are told to quiesce their OWN writers, so this exercises the
 * managed-side window the final recheck exists for. */
static void late_work_on_probe(void *ctx)
{
    fix_t *fix = (fix_t *)ctx;
    (void)write_one(fix, 9000);
}

/* ==================================================================== *
 * MECHANISM SELF-CHECK -- the final recheck, under a DELIBERATELY VIOLATED
 * caller precondition.
 *
 * endpoint.h tells callers to quiesce concurrent send-side mutations; this row
 * violates that on purpose by calling the public write() from inside the fake
 * backend snapshot. It is therefore NOT evidence about the documented
 * contract, and it does NOT show that concurrent writers are safe. It exists
 * only to prove the recheck mechanism is wired: without it drain returns OK
 * with that object unsent. A managed-side interleaving whose work was accepted
 * BEFORE drain began is covered by the ordinary pending path, not here.
 * New send work appears during the backend snapshot. Without the recheck
 * drain would return MOQ_OK with that object unsent.
 * ==================================================================== */
static void control_final_recheck_catches_late_work(moq_alloc_t *alloc)
{
    const char *NAME = "selfcheck_final_recheck_mechanism";
    int before = failures;
    fix_t fix;
    uint64_t now = 1000;
    if (!media_arm_setup(&fix, alloc, now, NAME)) { fix_free(&fix); return; }

    set_phase(&fix, PH_ACT);
    /* Nothing is owed yet, so drain reaches the backend probe immediately --
     * and the probe injects one object at exactly that instant. */
    MOQ_TEST_CHECK(!moq_media_sender_test_pending_send_work(fix.s));
    /* Land the injection in the IN-LOOP probe, not the entry probe: drain
     * takes one entry probe to reject unsupported/wrong-thread callers before
     * the loop, and work appearing there is caught by the ordinary pending
     * path. The recheck is what covers the in-loop snapshot. */
    unsigned at = moq_endpoint_test_probe_drain_calls(fix.probe) + 2;
    moq_endpoint_test_probe_arm_on_probe_once(fix.probe, late_work_on_probe,
                                              &fix, at);
    MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_drain(fix.ep, 1000000), (int)MOQ_OK);
    /* The late object was flushed, not lost. */
    MOQ_TEST_CHECK_EQ_INT(fix.med_n, 1);
    MOQ_TEST_CHECK(!moq_media_sender_test_pending_send_work(fix.s));
    {
        exp_t e[1];
        memset(e, 0, sizeof(e));
        e[0].kind = MOQ_EVENT_OBJECT_RECEIVED; e[0].sub = fix.med_sub._opaque;
        e[0].group = 0; e[0].object = 0; e[0].eog = true; e[0].has_payload = true;
        e[0].prio = 128; e[0].has_props = true;
        expect_phase(&fix, NAME, PH_ACT, e, 1);
    }

    teardown(&fix, NAME, fix.cat_n, fix.med_n, NULL, 0);
    report(NAME, failures - before, 0, false);
    fix_free(&fix);
}

/* ==================================================================== *
 * ROW -- an attachment-local FATAL must not become drain success.
 * One accepted object; the production media write returns a hard negative
 * once, so the SENDER terminalizes while the ENDPOINT stays established and
 * transport-idle. drain() must report that the flush did not complete.
 * ==================================================================== */
static void arm_attachment_fatal_is_not_drain_success(moq_alloc_t *alloc)
{
    const char *NAME = "attachment_fatal_drain";
    int before = failures;
    fix_t fix;
    uint64_t now = 1000;
    if (!media_arm_setup(&fix, alloc, now, NAME)) { fix_free(&fix); return; }

    set_phase(&fix, PH_ACT);
    MOQ_TEST_CHECK_EQ_INT((int)write_one(&fix, 5000), (int)MOQ_OK);
    MOQ_TEST_CHECK(moq_media_sender_test_pending_send_work(fix.s));
    moq_media_sender_test_fail_next_media_write_once(fix.s);

    /* The sender terminalizes on the accepted item; the endpoint does not. */
    moq_result_t drc = moq_endpoint_drain(fix.ep, 1000000);
    MOQ_TEST_CHECK(moq_media_sender_is_fatal(fix.s));
    MOQ_TEST_CHECK(!moq_endpoint_is_fatal(fix.ep));
    MOQ_TEST_CHECK(!moq_endpoint_is_closed(fix.ep));
    MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_state(fix.ep),
                          (int)MOQ_ENDPOINT_ESTABLISHED);
    /* The accepted object never reached the peer... */
    MOQ_TEST_CHECK_EQ_INT(fix.med_n, 0);
    /* ...so drain must NOT claim the flush completed. */
    MOQ_TEST_CHECK_EQ_INT((int)drc, (int)MOQ_ERR_CLOSED);
    /* And it stays that way: a repeat is the same answer, not a late OK. */
    MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_drain(fix.ep, 20000),
                          (int)MOQ_ERR_CLOSED);
    expect_phase(&fix, NAME, PH_ACT, NULL, 0);

    teardown(&fix, NAME, fix.cat_n, fix.med_n, NULL, 0);
    report(NAME, failures - before, 0, false);
    fix_free(&fix);
}

/* ==================================================================== *
 * ROWS -- the two GENERATED TIMELINE families, independently.
 * A media write appends a record to the generated sibling; the timeline's own
 * subscription arrives while the session action ring is tight, so the emission
 * is refused through the PRODUCTION facade and the hook settles with the lead
 * cursor behind. The media FIFO is empty and the catalog is clean at that
 * point, so the timeline term is the only thing owed. drain() must retry and
 * emit exactly one decoded timeline object.
 *
 * `media_tl` selects the family: false = SAP event timeline "<name>.sap",
 * true = MSF §7 media timeline "<name>.timeline". Each row must be killed only
 * by its OWN term's omission.
 * ==================================================================== */
static void timeline_row(moq_alloc_t *alloc, bool media_tl)
{
    const char *NAME = media_tl ? "media_timeline_drain" : "sap_timeline_drain";
    int before = failures;
    fix_t fix;
    uint64_t now = 1000;
    /* The real service pacing geometry: a small ring on both sessions. */
    if (!fix_init_actions(&fix, alloc, now, 2)) {
        fprintf(stderr, "FAIL %s: fixture setup\n", NAME);
        failures++; fix_free(&fix); return;
    }
    if (!subscribe_catalog(&fix, now)) {
        fprintf(stderr, "FAIL %s: catalog subscribe\n", NAME);
        failures++; fix_free(&fix); return;
    }
    /* A live media track that generates the timeline sibling. */
    {
        moq_media_track_cfg_t tc;
        moq_media_track_cfg_init(&tc);
        tc.name = (moq_bytes_t){ (const uint8_t *)MEDIA_TRACK_NAME, 1 };
        tc.media_type = MOQ_MEDIA_TYPE_AUDIO;
        tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
        tc.codec = MOQ_BYTES_LITERAL("opus");
        tc.bitrate = 64000;
        tc.is_live = true;
        tc.samplerate = 48000;
        tc.channel_config = MOQ_BYTES_LITERAL("2");
        if (media_tl) tc.emit_media_timeline = true;
        else          tc.emit_sap_timeline = true;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_media_sender_add_track(fix.s, &tc, &fix.media), (int)MOQ_OK);
        int b = fix.cat_n;
        for (int i = 0; i < 8 && fix.cat_n == b; i++) cycle(&fix, now);
        MOQ_TEST_CHECK(fix.cat_n > b);
    }
    if (!subscribe_track(&fix, now, MEDIA_TRACK_NAME, 1, &fix.med_sub)) {
        fprintf(stderr, "FAIL %s: media subscribe\n", NAME);
        failures++; fix_free(&fix); return;
    }
    fix.med_subscribed = true;
    for (int i = 0; i < 16 && fix.sub_ok_n < 2; i++) cycle(&fix, now);
    MOQ_TEST_CHECK_EQ_INT(fix.sub_ok_n, 2);
    if (sticky(&fix, NAME)) { fix_free(&fix); return; }

    /* One media object: it delivers AND appends the timeline record. The
     * timeline track has no subscriber yet, so nothing of it is owed. */
    MOQ_TEST_CHECK_EQ_INT((int)write_one_sap(&fix, 5000, !media_tl), (int)MOQ_OK);
    for (int i = 0; i < 6 && fix.med_n == 0; i++) cycle(&fix, now);
    MOQ_TEST_CHECK_EQ_INT(fix.med_n, 1);
    MOQ_TEST_CHECK_EQ_INT(fix.tl_n, 0);
    MOQ_TEST_CHECK(!moq_media_sender_test_pending_send_work(fix.s));
    {   /* catalog base + delta, the media OK, then the media object */
        exp_t e[4];
        memset(e, 0, sizeof(e));
        e[0].kind = MOQ_EVENT_OBJECT_RECEIVED; e[0].sub = fix.cat_sub._opaque;
        e[0].group = 1; e[0].object = 0; e[0].eog = true; e[0].has_payload = true;
        e[1].kind = MOQ_EVENT_OBJECT_RECEIVED; e[1].sub = fix.cat_sub._opaque;
        e[1].group = 1; e[1].object = 1; e[1].eog = true; e[1].has_payload = true;
        e[2].kind = MOQ_EVENT_SUBSCRIBE_OK; e[2].sub = fix.med_sub._opaque;
        e[2].x = 2;
        e[3].kind = MOQ_EVENT_OBJECT_RECEIVED; e[3].sub = fix.med_sub._opaque;
        e[3].group = 0; e[3].object = 0; e[3].eog = true; e[3].has_payload = true;
        e[3].prio = 128; e[3].has_props = true;
        expect_phase(&fix, NAME, PH_LIVE, e, 4);
    }

    set_phase(&fix, PH_ACT);
    /* Now subscribe the generated timeline track. */
    {
        char tname[16];
        size_t nl = 0;
        tname[nl++] = MEDIA_TRACK_NAME[0];
        const char *suf = media_tl ? ".timeline" : ".sap";
        for (const char *c = suf; *c; c++) tname[nl++] = *c;
        if (!subscribe_track(&fix, now, tname, nl, &fix.tl_sub)) {
            fprintf(stderr, "FAIL %s: timeline subscribe\n", NAME);
            failures++; fix_free(&fix); return;
        }
    }
    fix.tl_subscribed = true;
    for (int i = 0; i < 16 && fix.sub_ok_n < 3; i++) cycle(&fix, now);
    MOQ_TEST_CHECK_EQ_INT(fix.sub_ok_n, 3);
    if (sticky(&fix, NAME)) { fix_free(&fix); return; }

    /* THE ISOLATED STATE: the media FIFO is empty, the catalog is clean, and
     * the only thing owed is this timeline's uncaught lead cursor. */
    MOQ_TEST_CHECK_EQ_INT(fix.tl_n, 0);
    MOQ_TEST_CHECK(!moq_media_sender_test_catalog_dirty(fix.s));
    {
        moq_media_sender_stats_t mst; memset(&mst, 0, sizeof(mst));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_media_sender_get_stats(fix.s, &mst, sizeof(mst)), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)mst.objects_queued, 0);   /* FIFO empty */
    }
    MOQ_TEST_CHECK(moq_media_sender_test_pending_send_work(fix.s));

    /* drain() must retry the refused emission and complete it. */
    MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_drain(fix.ep, 1000000), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(fix.tl_n, 1);
    /* Exactly once: further cycles add nothing. */
    for (int i = 0; i < 3; i++) cycle(&fix, now);
    MOQ_TEST_CHECK_EQ_INT(fix.tl_n, 1);

    /* Decode the emitted object with the family's own real codec. */
    if (fix.tl_n == 1) {
        MOQ_TEST_CHECK_EQ_INT((int)fix.tl_rec[0].object, 0);
        MOQ_TEST_CHECK_EQ_INT((int)fix.tl_rec[0].status, (int)MOQ_OBJECT_NORMAL);
        if (media_tl) {
            moq_msf_media_timeline_record_t recs[4];
            size_t n = 0;
            moq_bytes_t json = { fix.tl_rec[0].buf, fix.tl_rec[0].len };
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_msf_media_timeline_decode(json, recs, 4, &n), (int)MOQ_OK);
            MOQ_TEST_CHECK_EQ_INT((int)n, 1);
            if (n == 1) {
                /* The record the media write created, at its wire Location. */
                MOQ_TEST_CHECK_EQ_INT((int)recs[0].group, 0);
                MOQ_TEST_CHECK_EQ_INT((int)recs[0].object, 0);
                MOQ_TEST_CHECK_EQ_INT((int)recs[0].media_time_ms, 5);  /* 5000us */
            }
        } else {
            /* The SAP event timeline has no public decoder; compare against
             * the exact §3.6.1 document the fixture's own declared values
             * produce -- one record, Location [0,0], SAP type 1, EPT 5 ms. */
            static const char want[] =
                "[{\"l\":[0,0],\"data\":[1,5]}]";
            MOQ_TEST_CHECK_EQ_INT((int)fix.tl_rec[0].len,
                                  (int)(sizeof(want) - 1));
            MOQ_TEST_CHECK(fix.tl_rec[0].len == sizeof(want) - 1 &&
                           memcmp(fix.tl_rec[0].buf, want, sizeof(want) - 1) == 0);
        }
    }

    {   /* the timeline subscription's OK, then its single emitted object */
        exp_t e[2];
        memset(e, 0, sizeof(e));
        e[0].kind = MOQ_EVENT_SUBSCRIBE_OK; e[0].sub = fix.tl_sub._opaque;
        e[0].x = 3;                          /* the timeline track's alias */
        e[1].kind = MOQ_EVENT_OBJECT_RECEIVED; e[1].sub = fix.tl_sub._opaque;
        e[1].group = 0; e[1].object = 0; e[1].eog = true; e[1].has_payload = true;
        /* A generated timeline object carries the default publisher priority
         * and NO property block (unlike a RAW/LOC media object). */
        e[1].prio = 128;
        expect_phase(&fix, NAME, PH_ACT, e, 2);
    }
    teardown(&fix, NAME, fix.cat_n, fix.med_n, NULL, 0);
    report(NAME, failures - before, 0, false);
    fix_free(&fix);
}

/* Fired ONCE inside the hook, after the republish decision and before the
 * settled recompute: a real public add_track() lands in exactly that window. */
/* An APPLICATION THREAD's add_track() landing inside a pump-side window.
 *
 * A public add_track() takes the endpoint mutex for its terminal check, and a
 * pump cycle runs its hooks holding that mutex, so the call cannot be made
 * from inside the window itself -- nor could a real application thread make
 * it there. What a real application thread CAN do is pass the terminal check
 * before the pump takes the mutex, then register while the window is open
 * (registration needs only the sender's own mutex, which the window has
 * released). That is modelled exactly: the thread parks at the sender's
 * add-track gap hook, after its state checks and before registration, and the
 * window releases it and waits for its registration to finish. */
void moq_media_sender_test_set_add_track_gap_hook(void (*fn)(void *), void *ctx);

enum { LATE_IDLE, LATE_PARKED, LATE_RELEASED, LATE_DONE };
typedef struct {
    fix_t          *fix;
    pthread_mutex_t mu;
    pthread_cond_t  cv;
    int             state;
    moq_result_t    rc;
    pthread_t       th;
} late_app_t;

static void late_app_gap(void *ctx)
{
    late_app_t *a = (late_app_t *)ctx;
    pthread_mutex_lock(&a->mu);
    a->state = LATE_PARKED;
    pthread_cond_broadcast(&a->cv);
    while (a->state < LATE_RELEASED) pthread_cond_wait(&a->cv, &a->mu);
    pthread_mutex_unlock(&a->mu);
}

static void *late_app_thread(void *ctx)
{
    late_app_t *a = (late_app_t *)ctx;
    moq_media_track_cfg_t tc;
    moq_media_track_cfg_init(&tc);
    tc.name = MOQ_BYTES_LITERAL("late");
    tc.media_type = MOQ_MEDIA_TYPE_AUDIO;
    tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
    tc.codec = MOQ_BYTES_LITERAL("opus");
    tc.bitrate = 32000;
    tc.is_live = true;
    tc.samplerate = 48000;
    tc.channel_config = MOQ_BYTES_LITERAL("2");
    moq_media_track_t *t = NULL;
    moq_result_t rc = moq_media_sender_add_track(a->fix->s, &tc, &t);
    pthread_mutex_lock(&a->mu);
    a->rc = rc;
    a->state = LATE_DONE;
    pthread_cond_broadcast(&a->cv);
    pthread_mutex_unlock(&a->mu);
    return NULL;
}

/* Start the application thread and wait until it is parked past its terminal
 * check. False if it could not be started or finished without parking. */
static bool late_app_park(late_app_t *a, fix_t *fix)
{
    memset(a, 0, sizeof(*a));
    a->fix = fix;
    a->rc = MOQ_ERR_INTERNAL;
    pthread_mutex_init(&a->mu, NULL);
    pthread_cond_init(&a->cv, NULL);
    moq_media_sender_test_set_add_track_gap_hook(late_app_gap, a);
    if (pthread_create(&a->th, NULL, late_app_thread, a) != 0) {
        moq_media_sender_test_set_add_track_gap_hook(NULL, NULL);
        a->state = LATE_DONE;
        return false;
    }
    pthread_mutex_lock(&a->mu);
    while (a->state == LATE_IDLE) pthread_cond_wait(&a->cv, &a->mu);
    bool parked = (a->state == LATE_PARKED);
    pthread_mutex_unlock(&a->mu);
    return parked;
}

/* The window: release the parked thread and wait for its add_track() to
 * return, so the registration lands inside this window. */
static void late_app_window(void *ctx)
{
    late_app_t *a = (late_app_t *)ctx;
    pthread_mutex_lock(&a->mu);
    if (a->state == LATE_PARKED) {
        a->state = LATE_RELEASED;
        pthread_cond_broadcast(&a->cv);
    }
    while (a->state < LATE_DONE) pthread_cond_wait(&a->cv, &a->mu);
    a->fix->late_add_rc = a->rc;
    pthread_mutex_unlock(&a->mu);
}

/* Join the thread (releasing it first if the window never opened, so a
 * failing row cannot hang) and remove the gap hook. */
static void late_app_finish(late_app_t *a)
{
    pthread_mutex_lock(&a->mu);
    if (a->state == LATE_PARKED) {
        a->state = LATE_RELEASED;
        pthread_cond_broadcast(&a->cv);
    }
    pthread_mutex_unlock(&a->mu);
    if (a->th) pthread_join(a->th, NULL);
    moq_media_sender_test_set_add_track_gap_hook(NULL, NULL);
    pthread_cond_destroy(&a->cv);
    pthread_mutex_destroy(&a->mu);
}

/* ==================================================================== *
 * ROW -- catalog_dirty across the republish/recompute window.
 * A public add_track() is accepted AFTER the hook decided what to republish
 * and BEFORE its settled recompute. The eager setter marks the cache pending;
 * the recompute then runs and must NOT clobber it. Without the catalog_dirty
 * term the recompute computes idle, drain returns at once, and the accepted
 * track never reaches the catalog on the wire.
 * ==================================================================== */
static void arm_dirty_across_recompute(moq_alloc_t *alloc)
{
    const char *NAME = "catalog_dirty_across_recompute";
    int before = failures;
    fix_t fix;
    uint64_t now = 1000;
    if (!fix_init(&fix, alloc, now)) {
        fprintf(stderr, "FAIL %s: fixture setup\n", NAME);
        failures++; fix_free(&fix); return;
    }
    if (!subscribe_catalog(&fix, now)) {
        fprintf(stderr, "FAIL %s: catalog subscribe\n", NAME);
        failures++; fix_free(&fix); return;
    }
    MOQ_TEST_CHECK(add_one_media_track_live(&fix, now));
    if (sticky(&fix, NAME)) { fix_free(&fix); return; }
    int base_n = fix.cat_n;
    MOQ_TEST_CHECK_EQ_INT(effective_tracks(&fix, base_n), 1);
    {
        exp_t e[2];
        memset(e, 0, sizeof(e));
        e[0].kind = MOQ_EVENT_OBJECT_RECEIVED; e[0].sub = fix.cat_sub._opaque;
        e[0].group = 1; e[0].object = 0; e[0].eog = true; e[0].has_payload = true;
        e[1].kind = MOQ_EVENT_OBJECT_RECEIVED; e[1].sub = fix.cat_sub._opaque;
        e[1].group = 1; e[1].object = 1; e[1].eog = true; e[1].has_payload = true;
        expect_phase(&fix, NAME, PH_LIVE, e, 2);
    }

    set_phase(&fix, PH_ACT);
    /* Park the application thread past its terminal check, arm the barrier,
     * then drive ONE cycle: the mutation lands inside it. */
    late_app_t late;
    MOQ_TEST_CHECK(late_app_park(&late, &fix));
    moq_media_sender_test_after_republish_once(fix.s, late_app_window, &late);
    cycle(&fix, now);
    late_app_finish(&late);
    MOQ_TEST_CHECK_EQ_INT((int)fix.late_add_rc, (int)MOQ_OK);
    /* The mutation was accepted inside that window and is still owed. */
    MOQ_TEST_CHECK(moq_media_sender_test_catalog_dirty(fix.s));
    MOQ_TEST_CHECK(moq_media_sender_test_pending_send_work(fix.s));

    MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_drain(fix.ep, 1000000), (int)MOQ_OK);
    /* The accepted track reached the catalog on the wire, exactly once: one
     * new generation carrying the independent base plus the add delta. */
    MOQ_TEST_CHECK_EQ_INT(fix.cat_n, base_n + 2);
    MOQ_TEST_CHECK_EQ_INT(effective_tracks(&fix, fix.cat_n), 2);
    for (int i = 0; i < 3; i++) cycle(&fix, now);
    MOQ_TEST_CHECK_EQ_INT(fix.cat_n, base_n + 2);
    {
        exp_t e[3];
        memset(e, 0, sizeof(e));
        e[0].kind = MOQ_EVENT_SUBGROUP_FINISHED; e[0].sub = fix.cat_sub._opaque;
        e[0].group = 1; e[0].eog = true;
        e[1].kind = MOQ_EVENT_OBJECT_RECEIVED; e[1].sub = fix.cat_sub._opaque;
        e[1].group = 2; e[1].object = 0; e[1].eog = true; e[1].has_payload = true;
        e[2].kind = MOQ_EVENT_OBJECT_RECEIVED; e[2].sub = fix.cat_sub._opaque;
        e[2].group = 2; e[2].object = 1; e[2].eog = true; e[2].has_payload = true;
        expect_phase(&fix, NAME, PH_ACT, e, 3);
    }

    teardown(&fix, NAME, fix.cat_n, fix.med_n, NULL, 0);
    report(NAME, failures - before, 0, false);
    fix_free(&fix);
}

/* ==================================================================== *
 * ROW -- removed-track TEARDOWN, isolated.
 * The catalog has NO subscriber, so its republish needs no wire action and
 * completes silently (catalog_dirty clears, nothing is staged). The media
 * track DOES have a subscriber, so its removal owes a reliable END_OF_TRACK
 * through a deliberately tight action ring, which refuses it. The teardown
 * term is then the only thing owed, and drain must finish it.
 * ==================================================================== */
static void arm_removed_track_teardown(moq_alloc_t *alloc)
{
    const char *NAME = "removed_track_teardown_drain";
    int before = failures;
    fix_t fix;
    uint64_t now = 1000;
    if (!fix_init(&fix, alloc, now)) {
        fprintf(stderr, "FAIL %s: fixture setup\n", NAME);
        failures++; fix_free(&fix); return;
    }
    set_phase(&fix, PH_LIVE);
    /* A live media track, added post-ready. The catalog is deliberately NOT
     * subscribed, so no catalog generation reaches the peer at all. */
    {
        moq_media_track_cfg_t tc;
        moq_media_track_cfg_init(&tc);
        tc.name = (moq_bytes_t){ (const uint8_t *)MEDIA_TRACK_NAME, 1 };
        tc.media_type = MOQ_MEDIA_TYPE_AUDIO;
        tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
        tc.codec = MOQ_BYTES_LITERAL("opus");
        tc.bitrate = 64000;
        tc.is_live = true;
        tc.samplerate = 48000;
        tc.channel_config = MOQ_BYTES_LITERAL("2");
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_media_sender_add_track(fix.s, &tc, &fix.media), (int)MOQ_OK);
    }
    for (int i = 0; i < 8; i++) cycle(&fix, now);
    if (!subscribe_track(&fix, now, MEDIA_TRACK_NAME, 1, &fix.med_sub)) {
        fprintf(stderr, "FAIL %s: media subscribe\n", NAME);
        failures++; fix_free(&fix); return;
    }
    fix.med_subscribed = true;
    for (int i = 0; i < 16 && fix.sub_ok_n < 1; i++) cycle(&fix, now);
    MOQ_TEST_CHECK_EQ_INT(fix.sub_ok_n, 1);
    MOQ_TEST_CHECK(moq_media_sender_track_has_subscriber(fix.s, fix.media));
    MOQ_TEST_CHECK_EQ_INT(fix.cat_n, 0);   /* the catalog is unsubscribed */
    if (sticky(&fix, NAME)) { fix_free(&fix); return; }
    {
        exp_t e[1];
        memset(e, 0, sizeof(e));
        e[0].kind = MOQ_EVENT_SUBSCRIBE_OK; e[0].sub = fix.med_sub._opaque;
        e[0].x = 1;   /* the only subscription in this row */
        expect_phase(&fix, NAME, PH_LIVE, e, 1);
    }

    set_phase(&fix, PH_ACT);
    /* Refuse the teardown's END_OF_TRACK exactly once, at the production call
     * boundary. Measured first: with the constrained action ring the teardown
     * either completes inside the same hook (ring 4+) or the fixture stalls
     * permanently (ring 2), so that geometry cannot isolate this window. */
    moq_media_sender_test_block_end_track_once(fix.s);
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_remove_track(fix.s, fix.media),
                          (int)MOQ_OK);
    fix.media = NULL;                       /* the handle is inert after remove */
    /* ONE cycle: the catalog republish completes (no demand -> no wire action)
     * while the removal's END_OF_TRACK is refused by the tight ring. */
    cycle(&fix, now);
    MOQ_TEST_CHECK(!moq_media_sender_test_catalog_dirty(fix.s));
    MOQ_TEST_CHECK_EQ_INT(fix.med_n, 0);          /* not yet emitted */
    MOQ_TEST_CHECK(moq_media_sender_test_pending_send_work(fix.s));

    MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_drain(fix.ep, 1000000), (int)MOQ_OK);
    /* The reliable terminal reached the subscriber, exactly once. */
    MOQ_TEST_CHECK_EQ_INT(fix.med_n, 1);
    if (fix.med_n == 1) {
        MOQ_TEST_CHECK_EQ_INT((int)fix.med[0].status,
                              (int)MOQ_OBJECT_END_OF_TRACK);
        MOQ_TEST_CHECK_EQ_INT((int)fix.med[0].len, 0);
    }
    for (int i = 0; i < 3; i++) cycle(&fix, now);
    MOQ_TEST_CHECK_EQ_INT(fix.med_n, 1);

    {   /* the reliable terminal, then its subgroup terminal and the DONE */
        exp_t e[3];
        memset(e, 0, sizeof(e));
        e[0].kind = MOQ_EVENT_OBJECT_RECEIVED; e[0].sub = fix.med_sub._opaque;
        e[0].group = 0; e[0].object = 0; e[0].eog = false;
        e[0].status = MOQ_OBJECT_END_OF_TRACK; e[0].prio = 128;
        e[1].kind = MOQ_EVENT_SUBGROUP_FINISHED; e[1].sub = fix.med_sub._opaque;
        e[1].group = 0; e[1].eog = false;
        e[2].kind = MOQ_EVENT_SUBSCRIBE_DONE; e[2].sub = fix.med_sub._opaque;
        e[2].x = 2; e[2].y = 1;
        expect_phase(&fix, NAME, PH_ACT, e, 3);
    }
    teardown(&fix, NAME, fix.cat_n, fix.med_n, NULL, 0);
    report(NAME, failures - before, 0, false);
    fix_free(&fix);
}

/* ==================================================================== *
 * ROW -- pending_reset, isolated.
 * With drop_without_demand the sender drops queued media for a track whose
 * subscriber left, and an OPEN wire subgroup that just lost its tail must be
 * RESET rather than truncated. The reset flush is refused once at its
 * production call boundary, so the hook settles with the obligation armed, an
 * empty FIFO and a clean catalog. drain must complete it exactly once.
 * ==================================================================== */
static void arm_pending_reset(moq_alloc_t *alloc)
{
    const char *NAME = "pending_reset_drain";
    int before = failures;
    fix_t fix;
    uint64_t now = 1000;
    if (!fix_init_actions2(&fix, alloc, now, 0, true)) {
        fprintf(stderr, "FAIL %s: fixture setup\n", NAME);
        failures++; fix_free(&fix); return;
    }
    set_phase(&fix, PH_LIVE);
    {
        moq_media_track_cfg_t tc;
        moq_media_track_cfg_init(&tc);
        tc.name = (moq_bytes_t){ (const uint8_t *)MEDIA_TRACK_NAME, 1 };
        tc.media_type = MOQ_MEDIA_TYPE_AUDIO;
        tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
        tc.codec = MOQ_BYTES_LITERAL("opus");
        tc.bitrate = 64000;
        tc.is_live = true;
        tc.samplerate = 48000;
        tc.channel_config = MOQ_BYTES_LITERAL("2");
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_media_sender_add_track(fix.s, &tc, &fix.media), (int)MOQ_OK);
    }
    for (int i = 0; i < 8; i++) cycle(&fix, now);
    if (!subscribe_track(&fix, now, MEDIA_TRACK_NAME, 1, &fix.med_sub)) {
        fprintf(stderr, "FAIL %s: media subscribe\n", NAME);
        failures++; fix_free(&fix); return;
    }
    fix.med_subscribed = true;
    for (int i = 0; i < 16 && fix.sub_ok_n < 1; i++) cycle(&fix, now);
    MOQ_TEST_CHECK_EQ_INT(fix.sub_ok_n, 1);
    /* One object opens the wire subgroup. */
    MOQ_TEST_CHECK_EQ_INT((int)write_one(&fix, 5000), (int)MOQ_OK);
    for (int i = 0; i < 6 && fix.med_n == 0; i++) cycle(&fix, now);
    MOQ_TEST_CHECK_EQ_INT(fix.med_n, 1);
    if (sticky(&fix, NAME)) { fix_free(&fix); return; }
    {   /* the subscription's OK, then the one delivered object */
        exp_t e[2];
        memset(e, 0, sizeof(e));
        e[0].kind = MOQ_EVENT_SUBSCRIBE_OK; e[0].sub = fix.med_sub._opaque;
        e[0].x = 1;
        e[1].kind = MOQ_EVENT_OBJECT_RECEIVED; e[1].sub = fix.med_sub._opaque;
        e[1].group = 0; e[1].object = 0; e[1].eog = true; e[1].has_payload = true;
        e[1].prio = 128; e[1].has_props = true;
        expect_phase(&fix, NAME, PH_LIVE, e, 2);
    }

    set_phase(&fix, PH_ACT);
    /* The subscriber leaves, then a second object is written: with no demand
     * the drop path discards it and arms the wire RESET for the open group. */
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_unsubscribe(moq_simpair_server(fix.sp), fix.med_sub, now),
        (int)MOQ_OK);
    for (int i = 0; i < 6; i++) cycle(&fix, now);
    MOQ_TEST_CHECK(!moq_media_sender_track_has_subscriber(fix.s, fix.media));
    moq_media_sender_test_block_flush_reset_once(fix.s);
    MOQ_TEST_CHECK_EQ_INT((int)write_one(&fix, 6000), (int)MOQ_OK);
    cycle(&fix, now);
    /* THE ISOLATED STATE: the reset was attempted and refused, the FIFO is
     * empty (the dropped object is gone) and the catalog is clean. */
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_test_reset_attempts(fix.s), 1);
    MOQ_TEST_CHECK(!moq_media_sender_test_catalog_dirty(fix.s));
    MOQ_TEST_CHECK(moq_media_sender_test_pending_send_work(fix.s));
    {
        moq_media_sender_stats_t mst; memset(&mst, 0, sizeof(mst));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_media_sender_get_stats(fix.s, &mst, sizeof(mst)), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)mst.objects_queued, 0);
        MOQ_TEST_CHECK_EQ_INT((int)mst.objects_dropped, 1);
        MOQ_TEST_CHECK_EQ_INT((int)mst.groups_abandoned, 0);   /* not yet */
    }

    MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_drain(fix.ep, 1000000), (int)MOQ_OK);
    /* drain retried it, exactly once, and the abandon is now accounted. */
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_test_reset_attempts(fix.s), 2);
    {
        moq_media_sender_stats_t mst; memset(&mst, 0, sizeof(mst));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_media_sender_get_stats(fix.s, &mst, sizeof(mst)), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)mst.groups_abandoned, 1);
    }
    for (int i = 0; i < 3; i++) cycle(&fix, now);
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_test_reset_attempts(fix.s), 2);
    {   /* the abandoned subgroup, reported to the departed subscriber's
         * stream with the service's abandon code */
        exp_t e[1];
        memset(e, 0, sizeof(e));
        e[0].kind = MOQ_EVENT_SUBGROUP_RESET; e[0].sub = fix.med_sub._opaque;
        /* Measured: the peer surfaces the RESET_STREAM application error the
         * transport carried (1), not the service's internal abandon code. */
        e[0].group = 0; e[0].eog = true; e[0].x = 1;
        expect_phase(&fix, NAME, PH_ACT, e, 1);
    }

    teardown(&fix, NAME, fix.cat_n, fix.med_n, NULL, 0);
    report(NAME, failures - before, 0, false);
    fix_free(&fix);
}

/* ==================================================================== *
 * CONTRACT -- mutations accepted AFTER the initial retained catalog is built
 * but BEFORE the peer accepts the namespace are flushed LOCALLY.
 *
 * `ready` is peer acceptance. In this window it is false, yet the accepted
 * mutation is purely local work: add_track()/remove_track() record it
 * unconditionally, and the hook's track maintenance and catalog republish are
 * gated on the initial catalog having been installed -- not on the peer.
 * (Before the fix both were gated on `ready`, and the accepted mutation was
 * never handed to the core at the drain boundary.)
 *
 * The assertions that matter are taken AT THE DRAIN RETURN BOUNDARY, with
 * acceptance still withheld: a later repair cannot make an earlier successful
 * drain truthful. Neither row uses a later unrelated mutation to make the
 * accepted one observable.
 *
 * The retained-state oracle is the one MSF-01 §5 mandates: SUBSCRIBE plus a
 * Joining FETCH at offset 0. A live catalog subscription is never accepted as
 * a substitute -- the effective track set below is rebuilt ONLY from
 * FETCH_OBJECT payloads, through the real MSF parser and delta application.
 * ==================================================================== */
static void pre_acceptance_row(moq_alloc_t *alloc, bool removing)
{
    const char *NAME = removing ? "pre_acceptance_remove"
                                : "pre_acceptance_add";
    int before = failures;
    fix_t fix;
    uint64_t now = 1000;
    memset(&fix, 0, sizeof(fix));
    fix.alloc = *alloc;
    fix.now = now;
    fix.withhold_accept = true;          /* the peer holds its acceptance */
    set_phase(&fix, PH_SETUP);
    fix.sp = make_pair(alloc);
    MOQ_TEST_CHECK(fix.sp != NULL);
    if (!fix.sp) { failures++; fix_free(&fix); return; }
    fix.ep = moq_endpoint_test_make_bare_alloc(alloc);
    MOQ_TEST_CHECK(fix.ep != NULL);
    if (!fix.ep) { failures++; fix_free(&fix); return; }
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_endpoint_test_install_probe(fix.ep, &fix.probe_mem,
                                             sizeof(fix.probe_mem),
                                             PROBE_DRAIN_DONE), (int)MOQ_OK);
    fix.probe = &fix.probe_mem;
    moq_endpoint_test_probe_set_service(fix.probe, probe_service, &fix);
    g_ns[0] = MOQ_BYTES_LITERAL("svc");
    g_ns[1] = MOQ_BYTES_LITERAL("done");
    moq_media_sender_cfg_t cfg;
    moq_media_sender_cfg_init_live_sized(&cfg, sizeof(cfg));
    cfg.namespace_ = (moq_namespace_t){ g_ns, 2 };
    cfg.catalog_refresh_interval_us = UINT64_MAX;
    moq_media_sender_callbacks_init_sized(&cfg.callbacks, sizeof(cfg.callbacks));
    cfg.callbacks.ctx = &fix;
    cfg.callbacks.on_ready = on_ready_cb;
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_attach(fix.ep, &cfg, &fix.s),
                          (int)MOQ_OK);
    if (!fix.s) { failures++; fix_free(&fix); return; }

    moq_media_track_cfg_t tc;
    moq_media_track_cfg_init(&tc);
    tc.media_type = MOQ_MEDIA_TYPE_AUDIO;
    tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
    tc.codec = MOQ_BYTES_LITERAL("opus");
    tc.bitrate = 64000;
    tc.is_live = true;
    tc.samplerate = 48000;
    tc.channel_config = MOQ_BYTES_LITERAL("2");

    moq_media_track_t *seeded = NULL;
    if (removing) {
        /* A tuple that IS in the initial retained catalog. */
        tc.name = MOQ_BYTES_LITERAL("seed");
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_media_sender_add_track(fix.s, &tc, &seeded), (int)MOQ_OK);
    }
    /* Build the initial catalog + advertisement; the peer never accepts. */
    for (int i = 0; i < 8; i++) cycle(&fix, now);
    MOQ_TEST_CHECK_EQ_INT(fix.ns_pub_n, 1);
    MOQ_TEST_CHECK(!moq_media_sender_is_ready(fix.s));
    /* The initial retained catalog IS materialized: generation 0, one install. */
    uint64_t grp0 = moq_media_sender_test_catalog_group(fix.s);
    unsigned inst0 = moq_media_sender_test_retained_installs(fix.s);
    MOQ_TEST_CHECK_EQ_INT((int)grp0, 0);
    MOQ_TEST_CHECK_EQ_INT((int)inst0, 1);
    {
        exp_t e[1];
        memset(e, 0, sizeof(e));
        e[0].kind = MOQ_EVENT_NAMESPACE_PUBLISHED;
        e[0].ann = fix.ann_handle;
        expect_phase(&fix, NAME, PH_SETUP, e, 1);
    }

    set_phase(&fix, PH_ACT);
    if (removing) {
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_media_sender_remove_track(fix.s, seeded), (int)MOQ_OK);
    } else {
        tc.name = MOQ_BYTES_LITERAL("late");
        moq_media_track_t *t = NULL;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_media_sender_add_track(fix.s, &tc, &t), (int)MOQ_OK);
    }

    /* ---- THE DRAIN BOUNDARY ------------------------------------------ *
     * The mutation was ACCEPTED, so drain must complete: the contract is a
     * LOCAL flush and explicitly does not wait for peer acceptance. And at the
     * instant it returns, the accepted mutation must already be in locally
     * owned publisher/retained state -- a repair that only happens when the
     * peer later accepts cannot make this return truthful. */
    MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_drain(fix.ep, 1000000), (int)MOQ_OK);
    MOQ_TEST_CHECK(!moq_media_sender_is_ready(fix.s));   /* still withheld */
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_test_catalog_group(fix.s),
                          (int)(grp0 + 1));              /* advanced once */
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_test_retained_installs(fix.s),
                          (int)(inst0 + 1));             /* installed once */
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_media_sender_test_pending_obj_count(fix.s), 0);  /* none staged */
    expect_phase(&fix, NAME, PH_ACT, NULL, 0);   /* nothing on the wire yet */

    /* ---- acceptance, with NO further mutation ------------------------ */
    {
        moq_accept_namespace_cfg_t ac;
        moq_accept_namespace_cfg_init(&ac);
        moq_announcement_t ann; memset(&ann, 0, sizeof(ann));
        ann._opaque = fix.ann_handle;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_accept_namespace(moq_simpair_server(fix.sp), ann,
                                              &ac, now), (int)MOQ_OK);
    }
    fix.withhold_accept = false;
    for (int i = 0; i < 8 && !moq_media_sender_is_ready(fix.s); i++)
        cycle(&fix, now);
    MOQ_TEST_CHECK(moq_media_sender_is_ready(fix.s));

    /* ---- the MSF-01 §5 retained oracle: SUBSCRIBE + Joining FETCH(0) -- */
    set_phase(&fix, PH_RECOVER);
    {
        moq_session_t *srv = moq_simpair_server(fix.sp);
        moq_subscribe_cfg_t sc; moq_subscribe_cfg_init(&sc);
        sc.track_namespace = (moq_namespace_t){ g_ns, 2 };
        sc.track_name = (moq_bytes_t){
            (const uint8_t *)MOQ_MSF_CATALOG_TRACK_NAME,
            strlen(MOQ_MSF_CATALOG_TRACK_NAME) };
        /* draft-16 §9.16.2 requires the joined subscription to have negotiated
         * the Largest Object filter. */
        sc.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
        sc.has_forward = true; sc.forward = true;
        memset(&fix.cat_sub, 0, sizeof(fix.cat_sub));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_subscribe(srv, &sc, now, &fix.cat_sub), (int)MOQ_OK);
        fix.cat_subscribed = true;
        for (int i = 0; i < 16 && fix.sub_ok_n < 1; i++) cycle(&fix, now);
        MOQ_TEST_CHECK_EQ_INT(fix.sub_ok_n, 1);

        moq_fetch_cfg_t fc; moq_fetch_cfg_init(&fc);
        fc.is_joining = true;
        /* MSF-01 §5 requires a Joining FETCH with OFFSET 0, which under the
         * MOQT Joining-FETCH definition is a RELATIVE join with Joining Start
         * 0 -- the same form the production MSF path issues
         * (service/tests/test_media_sender.c:520-529). An Absolute join from
         * group 0 is a different request and is not the §5 oracle. */
        fc.joining_relative = true;
        fc.joining_sub = fix.cat_sub;
        fc.joining_start = 0;
        memset(&fix.fetch, 0, sizeof(fix.fetch));
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(srv, &fc, now, &fix.fetch),
                              (int)MOQ_OK);
        for (int i = 0; i < 24 && fix.fetch_complete_n == 0; i++)
            cycle(&fix, now);
    }
    if (sticky(&fix, NAME)) { fix_free(&fix); return; }
    /* Exactly one FETCH_OK, at least one FETCH_OBJECT, exactly one COMPLETE. */
    MOQ_TEST_CHECK_EQ_INT(fix.fetch_ok_n, 1);
    MOQ_TEST_CHECK(fix.fet_n >= 1);
    MOQ_TEST_CHECK_EQ_INT(fix.fetch_complete_n, 1);
    /* The retained result is what the fetch delivered, never a live object. */
    MOQ_TEST_CHECK_EQ_INT(fix.cat_n, 0);

    {   /* The retained-oracle inventory, DECLARED from the contract rather
         * than read back from what happened. After either accepted mutation
         * has been locally committed, the retained generation is group 1
         * carrying objects 0 and 1, so: Largest is exactly (1,1); the Joining
         * FETCH's exclusive end is exactly (1,2); the delivered objects are
         * exactly (1,0) then (1,1); then one COMPLETE. Both rows declare the
         * same shape because the contract is the same -- a row whose product
         * behaviour differs is RED here, which is the intent. */
        exp_t e[5];
        memset(e, 0, sizeof(e));
        e[0].kind = MOQ_EVENT_SUBSCRIBE_OK; e[0].sub = fix.cat_sub._opaque;
        e[0].x = 1; e[0].has_largest = true;
        e[0].y = 1;                             /* Largest.Group   */
        e[0].largest_obj = 1;                   /* Largest.Object  */
        e[1].kind = MOQ_EVENT_FETCH_OK; e[1].fetch = fix.fetch._opaque;
        e[1].y = 1;                             /* end_group       */
        e[1].largest_obj = 2;                   /* exclusive end   */
        for (int k = 0; k < 2; k++) {
            e[2 + k].kind = MOQ_EVENT_FETCH_OBJECT;
            e[2 + k].fetch = fix.fetch._opaque;
            e[2 + k].group = 1;
            e[2 + k].object = (uint64_t)k;
            e[2 + k].has_payload = true;
        }
        e[4].kind = MOQ_EVENT_FETCH_COMPLETE; e[4].fetch = fix.fetch._opaque;
        expect_phase(&fix, NAME, PH_RECOVER, e, 5);
    }
    /* THE CONTRACT: the accepted mutation is in the RETAINED catalog. */
    if (removing) MOQ_TEST_CHECK(!eff_has(&fix, "seed"));
    else          MOQ_TEST_CHECK(eff_has(&fix, "late"));

    teardown(&fix, NAME, fix.cat_n, fix.med_n, NULL, 0);
    report(NAME, failures - before, 0, false);
    fix_free(&fix);
}

/* ==================================================================== *
 * CONTRACT -- a mutation the INITIAL catalog already represents costs no
 * generation, and the dedup is decided on CONTENT, never on the timestamp.
 *
 * A track added before the first hook is registered by the initial pass and is
 * therefore already carried by generation 0, but the accepted add legitimately
 * records catalog work (that recording must not wait for the peer). The
 * republish must resolve to the same catalog and DEDUP.
 *
 * The hazard this row exists to pin: the no-op dedup compares catalog BYTES,
 * and MSF-01 §5.1.2 generatedAt is a WALL-CLOCK field. A probe built with a
 * fresh stamp differs from a committed catalog built a millisecond earlier even
 * when the track set is identical, and that difference alone would publish an
 * unnecessary generation. Waiting for a real millisecond rollover makes that a
 * flaky oracle, so the sender's stamp sequence is SCRIPTED per sender: the
 * initial build takes STAMP_A and every later request takes a distinct STAMP_B.
 * With the contract held the row is decided in one run.
 *
 * The retained generation is then read back through the MSF-01 §5 oracle and
 * required to carry STAMP_A on the wire -- the private scalar alone is not the
 * proof.
 * ==================================================================== */
static void initial_generation_dedup_row(moq_alloc_t *alloc)
{
    const char *NAME = "initial_generation_dedups_on_content";
    const uint64_t STAMP_A = 1700000000123ull;   /* the initial build */
    const uint64_t STAMP_B = 1700000000456ull;   /* every later request */
    int before = failures;
    fix_t fix;
    uint64_t now = 1000;
    memset(&fix, 0, sizeof(fix));
    fix.alloc = *alloc;
    fix.now = now;
    set_phase(&fix, PH_SETUP);
    fix.sp = make_pair(alloc);
    MOQ_TEST_CHECK(fix.sp != NULL);
    if (!fix.sp) { failures++; fix_free(&fix); return; }
    fix.ep = moq_endpoint_test_make_bare_alloc(alloc);
    MOQ_TEST_CHECK(fix.ep != NULL);
    if (!fix.ep) { failures++; fix_free(&fix); return; }
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_endpoint_test_install_probe(fix.ep, &fix.probe_mem,
                                             sizeof(fix.probe_mem),
                                             PROBE_DRAIN_DONE), (int)MOQ_OK);
    fix.probe = &fix.probe_mem;
    moq_endpoint_test_probe_set_service(fix.probe, probe_service, &fix);
    g_ns[0] = MOQ_BYTES_LITERAL("svc");
    g_ns[1] = MOQ_BYTES_LITERAL("done");
    moq_media_sender_cfg_t cfg;
    moq_media_sender_cfg_init_live_sized(&cfg, sizeof(cfg));
    cfg.namespace_ = (moq_namespace_t){ g_ns, 2 };
    cfg.catalog_refresh_interval_us = UINT64_MAX;   /* no periodic generation */
    moq_media_sender_callbacks_init_sized(&cfg.callbacks, sizeof(cfg.callbacks));
    cfg.callbacks.ctx = &fix;
    cfg.callbacks.on_ready = on_ready_cb;
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_attach(fix.ep, &cfg, &fix.s),
                          (int)MOQ_OK);
    if (!fix.s) { failures++; fix_free(&fix); return; }

    /* (1) A real public add BEFORE the first hook: generation 0 will carry it,
     * and the accepted mutation records catalog work all the same. */
    {
        moq_media_track_cfg_t tc;
        moq_media_track_cfg_init(&tc);
        tc.name = MOQ_BYTES_LITERAL("seed");
        tc.media_type = MOQ_MEDIA_TYPE_AUDIO;
        tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
        tc.codec = MOQ_BYTES_LITERAL("opus");
        tc.bitrate = 64000;
        tc.is_live = true;
        tc.samplerate = 48000;
        tc.channel_config = MOQ_BYTES_LITERAL("2");
        moq_media_track_t *seeded = NULL;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_media_sender_add_track(fix.s, &tc, &seeded), (int)MOQ_OK);
    }
    /* The accepted mutation IS recorded -- this row is not "nothing happened". */
    MOQ_TEST_CHECK(moq_media_sender_test_catalog_dirty(fix.s));
    MOQ_TEST_CHECK(moq_media_sender_test_pending_send_work(fix.s));

    /* (2) Script the stamps, before any build has run. */
    moq_media_sender_test_script_generated_at(fix.s, STAMP_A, STAMP_B);

    /* (3) Drive the real production hook path to readiness. */
    for (int i = 0; i < 16 && !moq_media_sender_is_ready(fix.s); i++)
        cycle(&fix, now);
    MOQ_TEST_CHECK(moq_media_sender_is_ready(fix.s));
    MOQ_TEST_CHECK_EQ_INT(fix.ready_n, 1);
    MOQ_TEST_CHECK_EQ_INT(fix.ns_pub_n, 1);
    {
        exp_t e[1];
        memset(e, 0, sizeof(e));
        e[0].kind = MOQ_EVENT_NAMESPACE_PUBLISHED;
        e[0].ann = fix.ann_handle;
        expect_phase(&fix, NAME, PH_SETUP, e, 1);
    }

    /* (4) NO extra generation: still generation 0, one retained install, and
     * the recorded work is settled -- nothing dirty, nothing staged. */
    set_phase(&fix, PH_ACT);
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_test_catalog_group(fix.s), 0);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_media_sender_test_retained_installs(fix.s), 1);
    MOQ_TEST_CHECK(!moq_media_sender_test_catalog_dirty(fix.s));
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_media_sender_test_pending_obj_count(fix.s), 0);
    MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_drain(fix.ep, 1000000), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_test_catalog_group(fix.s), 0);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_media_sender_test_retained_installs(fix.s), 1);
    expect_phase(&fix, NAME, PH_ACT, NULL, 0);   /* nothing published */

    /* (5) Read the retained generation back off the wire (MSF-01 §5) and
     * require it to be generation 0, object 0, carrying STAMP_A. */
    set_phase(&fix, PH_RECOVER);
    {
        moq_session_t *srv = moq_simpair_server(fix.sp);
        moq_subscribe_cfg_t sc; moq_subscribe_cfg_init(&sc);
        sc.track_namespace = (moq_namespace_t){ g_ns, 2 };
        sc.track_name = (moq_bytes_t){
            (const uint8_t *)MOQ_MSF_CATALOG_TRACK_NAME,
            strlen(MOQ_MSF_CATALOG_TRACK_NAME) };
        sc.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
        sc.has_forward = true; sc.forward = true;
        memset(&fix.cat_sub, 0, sizeof(fix.cat_sub));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_subscribe(srv, &sc, now, &fix.cat_sub), (int)MOQ_OK);
        fix.cat_subscribed = true;
        for (int i = 0; i < 16 && fix.sub_ok_n < 1; i++) cycle(&fix, now);
        MOQ_TEST_CHECK_EQ_INT(fix.sub_ok_n, 1);

        moq_fetch_cfg_t fc; moq_fetch_cfg_init(&fc);
        fc.is_joining = true;
        fc.joining_relative = true;
        fc.joining_sub = fix.cat_sub;
        fc.joining_start = 0;
        memset(&fix.fetch, 0, sizeof(fix.fetch));
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(srv, &fc, now, &fix.fetch),
                              (int)MOQ_OK);
        for (int i = 0; i < 24 && fix.fetch_complete_n == 0; i++)
            cycle(&fix, now);
    }
    if (sticky(&fix, NAME)) { fix_free(&fix); return; }
    MOQ_TEST_CHECK_EQ_INT(fix.fetch_ok_n, 1);
    MOQ_TEST_CHECK_EQ_INT(fix.fetch_complete_n, 1);
    MOQ_TEST_CHECK_EQ_INT(fix.cat_n, 0);        /* no live object substituted */
    /* Exactly ONE retained object: generation 0 was never superseded. */
    MOQ_TEST_CHECK_EQ_INT(fix.fet_n, 1);
    if (fix.fet_n == 1) {
        const cat_rec_t *r = &fix.fet[0];
        MOQ_TEST_CHECK(r->parsed);
        MOQ_TEST_CHECK(!r->is_delta);
        MOQ_TEST_CHECK(!r->is_complete);
        MOQ_TEST_CHECK_EQ_INT((int)r->track_count, 1);   /* "seed" */
        /* THE CONTENT-VS-TIMESTAMP FACT, read off the wire: the retained
         * generation is the one built with STAMP_A. If the dedup had compared
         * against a freshly stamped probe, a second generation carrying
         * STAMP_B would have been published instead. */
        MOQ_TEST_CHECK(r->has_generated_at);
        MOQ_TEST_CHECK_EQ_U64(r->generated_at, STAMP_A);
    }
    {   /* the declared retained inventory: Largest (0,0), exclusive end (0,1) */
        exp_t e[4];
        memset(e, 0, sizeof(e));
        e[0].kind = MOQ_EVENT_SUBSCRIBE_OK; e[0].sub = fix.cat_sub._opaque;
        e[0].x = 1; e[0].has_largest = true;
        e[0].y = 0; e[0].largest_obj = 0;
        e[1].kind = MOQ_EVENT_FETCH_OK; e[1].fetch = fix.fetch._opaque;
        e[1].y = 0; e[1].largest_obj = 1;
        e[2].kind = MOQ_EVENT_FETCH_OBJECT; e[2].fetch = fix.fetch._opaque;
        e[2].group = 0; e[2].object = 0; e[2].has_payload = true;
        e[3].kind = MOQ_EVENT_FETCH_COMPLETE; e[3].fetch = fix.fetch._opaque;
        expect_phase(&fix, NAME, PH_RECOVER, e, 4);
    }
    MOQ_TEST_CHECK(eff_has(&fix, "seed"));

    teardown(&fix, NAME, fix.cat_n, fix.med_n, NULL, 0);
    report(NAME, failures - before, 0, false);
    fix_free(&fix);
}

/* Fired ONCE inside the hook, AFTER the initial track-registration snapshot
 * has been walked and BEFORE the initial catalog is built and installed as the
 * retained group: a real public add_track() lands in exactly that window. */
/* ==================================================================== *
 * CONTRACT -- the INITIAL-BUILD mutation window.
 *
 * A mutation accepted after the initial registration snapshot has been walked
 * but before the initial retained catalog is committed belongs to no
 * generation yet: it is not registered with the publisher, and the catalog it
 * would have dirtied does not exist. It must still be committed into a later
 * local generation. This row is what stops the earlier pre-acceptance rows
 * being satisfied by a dirty condition that only fires once the catalog has
 * been materialized -- such a rule leaves exactly this window open, and this
 * row fails under it.
 *
 * Acceptance is WITHHELD throughout the assertion: drain() is a LOCAL flush,
 * so it must commit the accepted add into locally owned publisher/retained
 * state without any peer round trip. The retained oracle afterwards is the
 * same MSF-01 §5 SUBSCRIBE + Relative Joining FETCH(0), with NO further
 * mutation between the drain and the fetch.
 * ==================================================================== */
static void initial_build_race_row(moq_alloc_t *alloc)
{
    const char *NAME = "initial_build_race";
    int before = failures;
    fix_t fix;
    uint64_t now = 1000;
    memset(&fix, 0, sizeof(fix));
    fix.alloc = *alloc;
    fix.now = now;
    fix.late_add_rc = MOQ_ERR_INTERNAL;   /* proven set by the barrier below */
    fix.withhold_accept = true;           /* the peer holds its acceptance */
    set_phase(&fix, PH_SETUP);
    fix.sp = make_pair(alloc);
    MOQ_TEST_CHECK(fix.sp != NULL);
    if (!fix.sp) { failures++; fix_free(&fix); return; }
    fix.ep = moq_endpoint_test_make_bare_alloc(alloc);
    MOQ_TEST_CHECK(fix.ep != NULL);
    if (!fix.ep) { failures++; fix_free(&fix); return; }
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_endpoint_test_install_probe(fix.ep, &fix.probe_mem,
                                             sizeof(fix.probe_mem),
                                             PROBE_DRAIN_DONE), (int)MOQ_OK);
    fix.probe = &fix.probe_mem;
    moq_endpoint_test_probe_set_service(fix.probe, probe_service, &fix);
    g_ns[0] = MOQ_BYTES_LITERAL("svc");
    g_ns[1] = MOQ_BYTES_LITERAL("done");
    moq_media_sender_cfg_t cfg;
    moq_media_sender_cfg_init_live_sized(&cfg, sizeof(cfg));
    cfg.namespace_ = (moq_namespace_t){ g_ns, 2 };
    cfg.catalog_refresh_interval_us = UINT64_MAX;
    moq_media_sender_callbacks_init_sized(&cfg.callbacks, sizeof(cfg.callbacks));
    cfg.callbacks.ctx = &fix;
    cfg.callbacks.on_ready = on_ready_cb;
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_attach(fix.ep, &cfg, &fix.s),
                          (int)MOQ_OK);
    if (!fix.s) { failures++; fix_free(&fix); return; }

    /* One track present BEFORE the first pump, so the initial catalog is
     * non-empty and the raced add is distinguishable from it. */
    {
        moq_media_track_cfg_t tc;
        moq_media_track_cfg_init(&tc);
        tc.name = MOQ_BYTES_LITERAL("seed");
        tc.media_type = MOQ_MEDIA_TYPE_AUDIO;
        tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
        tc.codec = MOQ_BYTES_LITERAL("opus");
        tc.bitrate = 64000;
        tc.is_live = true;
        tc.samplerate = 48000;
        tc.channel_config = MOQ_BYTES_LITERAL("2");
        moq_media_track_t *seeded = NULL;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_media_sender_add_track(fix.s, &tc, &seeded), (int)MOQ_OK);
    }
    /* Arm the barrier BEFORE the first pump: the window it names occurs
     * exactly once, on the hook that first registers the publisher tracks. */
    late_app_t late;
    MOQ_TEST_CHECK(late_app_park(&late, &fix));
    moq_media_sender_test_before_initial_build_once(fix.s, late_app_window,
                                                    &late);
    for (int i = 0; i < 8; i++) cycle(&fix, now);
    late_app_finish(&late);

    /* The barrier fired and the public add was ACCEPTED inside the window. */
    MOQ_TEST_CHECK_EQ_INT((int)fix.late_add_rc, (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(fix.ns_pub_n, 1);
    MOQ_TEST_CHECK(!moq_media_sender_is_ready(fix.s));   /* still withheld */
    {
        exp_t e[1];
        memset(e, 0, sizeof(e));
        e[0].kind = MOQ_EVENT_NAMESPACE_PUBLISHED;
        e[0].ann = fix.ann_handle;
        expect_phase(&fix, NAME, PH_SETUP, e, 1);
    }

    /* ---- THE DRAIN BOUNDARY, acceptance still withheld ---------------- */
    set_phase(&fix, PH_ACT);
    MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_drain(fix.ep, 1000000), (int)MOQ_OK);
    MOQ_TEST_CHECK(!moq_media_sender_is_ready(fix.s));
    /* The accepted add is committed EXACTLY ONCE into local retained state:
     * generation 1, installed once on top of the initial generation 0. */
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_test_catalog_group(fix.s), 1);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_media_sender_test_retained_installs(fix.s), 2);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_media_sender_test_pending_obj_count(fix.s), 0);
    MOQ_TEST_CHECK(!moq_media_sender_test_catalog_dirty(fix.s));
    expect_phase(&fix, NAME, PH_ACT, NULL, 0);   /* nothing on the wire yet */

    /* ---- acceptance, with NO further mutation ------------------------- */
    {
        moq_accept_namespace_cfg_t ac;
        moq_accept_namespace_cfg_init(&ac);
        moq_announcement_t ann; memset(&ann, 0, sizeof(ann));
        ann._opaque = fix.ann_handle;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_accept_namespace(moq_simpair_server(fix.sp), ann,
                                              &ac, now), (int)MOQ_OK);
    }
    fix.withhold_accept = false;
    for (int i = 0; i < 8 && !moq_media_sender_is_ready(fix.s); i++)
        cycle(&fix, now);
    MOQ_TEST_CHECK(moq_media_sender_is_ready(fix.s));

    /* ---- the MSF-01 §5 retained oracle: SUBSCRIBE + Joining FETCH(0) --- */
    set_phase(&fix, PH_RECOVER);
    {
        moq_session_t *srv = moq_simpair_server(fix.sp);
        moq_subscribe_cfg_t sc; moq_subscribe_cfg_init(&sc);
        sc.track_namespace = (moq_namespace_t){ g_ns, 2 };
        sc.track_name = (moq_bytes_t){
            (const uint8_t *)MOQ_MSF_CATALOG_TRACK_NAME,
            strlen(MOQ_MSF_CATALOG_TRACK_NAME) };
        sc.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
        sc.has_forward = true; sc.forward = true;
        memset(&fix.cat_sub, 0, sizeof(fix.cat_sub));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_subscribe(srv, &sc, now, &fix.cat_sub), (int)MOQ_OK);
        fix.cat_subscribed = true;
        for (int i = 0; i < 16 && fix.sub_ok_n < 1; i++) cycle(&fix, now);
        MOQ_TEST_CHECK_EQ_INT(fix.sub_ok_n, 1);

        moq_fetch_cfg_t fc; moq_fetch_cfg_init(&fc);
        fc.is_joining = true;
        fc.joining_relative = true;            /* MSF-01 §5: offset 0 */
        fc.joining_sub = fix.cat_sub;
        fc.joining_start = 0;
        memset(&fix.fetch, 0, sizeof(fix.fetch));
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(srv, &fc, now, &fix.fetch),
                              (int)MOQ_OK);
        for (int i = 0; i < 24 && fix.fetch_complete_n == 0; i++)
            cycle(&fix, now);
    }
    if (sticky(&fix, NAME)) { fix_free(&fix); return; }
    MOQ_TEST_CHECK_EQ_INT(fix.fetch_ok_n, 1);
    MOQ_TEST_CHECK(fix.fet_n >= 1);
    MOQ_TEST_CHECK_EQ_INT(fix.fetch_complete_n, 1);
    MOQ_TEST_CHECK_EQ_INT(fix.cat_n, 0);    /* no live object substituted */
    {   /* the same DECLARED contract as the pre-acceptance rows */
        exp_t e[5];
        memset(e, 0, sizeof(e));
        e[0].kind = MOQ_EVENT_SUBSCRIBE_OK; e[0].sub = fix.cat_sub._opaque;
        e[0].x = 1; e[0].has_largest = true;
        e[0].y = 1; e[0].largest_obj = 1;
        e[1].kind = MOQ_EVENT_FETCH_OK; e[1].fetch = fix.fetch._opaque;
        e[1].y = 1; e[1].largest_obj = 2;
        for (int k = 0; k < 2; k++) {
            e[2 + k].kind = MOQ_EVENT_FETCH_OBJECT;
            e[2 + k].fetch = fix.fetch._opaque;
            e[2 + k].group = 1;
            e[2 + k].object = (uint64_t)k;
            e[2 + k].has_payload = true;
        }
        e[4].kind = MOQ_EVENT_FETCH_COMPLETE; e[4].fetch = fix.fetch._opaque;
        expect_phase(&fix, NAME, PH_RECOVER, e, 5);
    }
    /* THE CONTRACT: BOTH tracks are in the retained catalog. */
    MOQ_TEST_CHECK(eff_has(&fix, "seed"));
    MOQ_TEST_CHECK(eff_has(&fix, "late"));

    teardown(&fix, NAME, fix.cat_n, fix.med_n, NULL, 0);
    report(NAME, failures - before, 0, false);
    fix_free(&fix);
}

/* Shared LIVE setup for the completion rows: a ready sender, a subscribed
 * catalog, one live media track with a subscriber, and one delivered object so
 * the track is genuinely ACTIVE with an open wire subgroup. Returns false and
 * reports its own failure if any step does not converge. */
static bool completion_live_setup(fix_t *fix, moq_alloc_t *alloc, uint64_t now,
                                  const char *name, bool drop_no_demand)
{
    if (!fix_init_actions2(fix, alloc, now, 0, drop_no_demand)) {
        fprintf(stderr, "FAIL %s: fixture setup\n", name);
        failures++; return false;
    }
    if (!subscribe_catalog(fix, now)) {
        fprintf(stderr, "FAIL %s: catalog subscribe\n", name);
        failures++; return false;
    }
    if (!add_one_media_track_live(fix, now)) {
        fprintf(stderr, "FAIL %s: live media track add\n", name);
        failures++; return false;
    }
    if (!subscribe_track(fix, now, MEDIA_TRACK_NAME, 1, &fix->med_sub)) {
        fprintf(stderr, "FAIL %s: media subscribe\n", name);
        failures++; return false;
    }
    fix->med_subscribed = true;
    for (int i = 0; i < 16 && fix->sub_ok_n < 2; i++) cycle(fix, now);
    MOQ_TEST_CHECK_EQ_INT(fix->sub_ok_n, 2);
    MOQ_TEST_CHECK_EQ_INT((int)write_one(fix, 5000), (int)MOQ_OK);
    for (int i = 0; i < 6 && fix->med_n == 0; i++) cycle(fix, now);
    MOQ_TEST_CHECK_EQ_INT(fix->med_n, 1);
    if (sticky(fix, name)) return false;
    {   /* catalog base + delta, the media OK, then the media object */
        exp_t e[4];
        memset(e, 0, sizeof(e));
        e[0].kind = MOQ_EVENT_OBJECT_RECEIVED; e[0].sub = fix->cat_sub._opaque;
        e[0].group = 1; e[0].object = 0; e[0].eog = true; e[0].has_payload = true;
        e[1].kind = MOQ_EVENT_OBJECT_RECEIVED; e[1].sub = fix->cat_sub._opaque;
        e[1].group = 1; e[1].object = 1; e[1].eog = true; e[1].has_payload = true;
        e[2].kind = MOQ_EVENT_SUBSCRIBE_OK; e[2].sub = fix->med_sub._opaque;
        e[2].x = 2;
        e[3].kind = MOQ_EVENT_OBJECT_RECEIVED; e[3].sub = fix->med_sub._opaque;
        e[3].group = 0; e[3].object = 0; e[3].eog = true; e[3].has_payload = true;
        e[3].prio = 128; e[3].has_props = true;
        expect_phase(fix, name, PH_LIVE, e, 4);
    }
    return true;
}

/* ==================================================================== *
 * CONTRACT -- MSF-01 §11.3 completion ORDERING, Track Ended prerequisite.
 * §11.3 orders permanent completion: (1) Track Ended for every active track,
 * then (2) the independent isComplete:true, empty-tracks catalog. This row
 * refuses the reliable END_OF_TRACK at its production call boundary and
 * requires that the terminal generation is NOT advanced while it is owed.
 *
 * The boundary oracle is STATE plus the decoded wire: the catalog group and
 * retained-install count must be unchanged, and no isComplete:true generation
 * may have reached the peer. Receive ordering across independent streams is
 * NOT used as a proof of send ordering.
 * ==================================================================== */
static void complete_waits_for_track_ended_row(moq_alloc_t *alloc)
{
    const char *NAME = "complete_waits_for_track_ended";
    int before = failures;
    fix_t fix;
    uint64_t now = 1000;
    memset(&fix, 0, sizeof(fix));
    if (!completion_live_setup(&fix, alloc, now, NAME, false)) {
        fix_free(&fix); return;
    }

    set_phase(&fix, PH_ACT);
    uint64_t grp0 = moq_media_sender_test_catalog_group(fix.s);
    unsigned inst0 = moq_media_sender_test_retained_installs(fix.s);
    int cat0 = fix.cat_n;
    MOQ_TEST_CHECK(!moq_media_sender_test_track_pub_ended(fix.s, fix.media));
    moq_media_sender_test_block_end_track_once(fix.s);
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_complete(fix.s), (int)MOQ_OK);
    cycle(&fix, now);

    /* ---- THE REFUSED BOUNDARY: step 1 is still owed ------------------- */
    MOQ_TEST_CHECK(!moq_media_sender_test_track_pub_ended(fix.s, fix.media));
    MOQ_TEST_CHECK(!moq_media_sender_test_track_teardown_done(fix.s, fix.media));
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_test_catalog_group(fix.s),
                          (int)grp0);                 /* not advanced */
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_test_retained_installs(fix.s),
                          (int)inst0);                /* nothing installed */
    MOQ_TEST_CHECK_EQ_INT(terminal_count(&fix), 0);   /* nothing on the wire */
    MOQ_TEST_CHECK_EQ_INT(fix.cat_n, cat0);
    MOQ_TEST_CHECK(moq_media_sender_test_pending_send_work(fix.s));

    /* ---- recovery: the prerequisite once, then ONE terminal catalog --- */
    MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_drain(fix.ep, 1000000), (int)MOQ_OK);
    MOQ_TEST_CHECK(moq_media_sender_test_track_pub_ended(fix.s, fix.media));
    MOQ_TEST_CHECK_EQ_INT(terminal_count(&fix), 1);
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_test_catalog_group(fix.s),
                          (int)(grp0 + 1));
    MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_drain(fix.ep, 200000), (int)MOQ_OK);
    for (int i = 0; i < 3; i++) cycle(&fix, now);
    MOQ_TEST_CHECK_EQ_INT(terminal_count(&fix), 1);
    {   /* The CORRECTED delivery order. The row's contract oracle is still the
         * STATE at the refused boundary above -- receive order across
         * independent streams does not by itself prove send order -- but the
         * observable order now matches §11.3: the media track's Track Ended
         * outcome is complete (terminal object, subgroup close, SUBSCRIBE_DONE)
         * BEFORE the terminal catalog generation appears on the catalog
         * stream. Before the fix this inventory was the other way round. */
        exp_t e[6];
        memset(e, 0, sizeof(e));
        e[0].kind = MOQ_EVENT_SUBGROUP_FINISHED; e[0].sub = fix.med_sub._opaque;
        e[0].group = 0; e[0].eog = true;
        e[1].kind = MOQ_EVENT_OBJECT_RECEIVED; e[1].sub = fix.med_sub._opaque;
        e[1].group = 1; e[1].object = 0; e[1].eog = false;
        e[1].status = MOQ_OBJECT_END_OF_TRACK; e[1].prio = 128;
        e[2].kind = MOQ_EVENT_SUBGROUP_FINISHED; e[2].sub = fix.med_sub._opaque;
        e[2].group = 1; e[2].eog = false;
        e[3].kind = MOQ_EVENT_SUBSCRIBE_DONE; e[3].sub = fix.med_sub._opaque;
        e[3].x = 2; e[3].y = 2;
        e[4].kind = MOQ_EVENT_SUBGROUP_FINISHED; e[4].sub = fix.cat_sub._opaque;
        e[4].group = 1; e[4].eog = true;
        e[5].kind = MOQ_EVENT_OBJECT_RECEIVED; e[5].sub = fix.cat_sub._opaque;
        e[5].group = 2; e[5].object = 0; e[5].eog = true; e[5].has_payload = true;
        expect_phase(&fix, NAME, PH_ACT, e, 6);
    }

    teardown(&fix, NAME, fix.cat_n, fix.med_n, NULL, 0);
    report(NAME, failures - before, 0, false);
    fix_free(&fix);
}

/* ==================================================================== *
 * CONTRACT -- MSF-01 §11.3 completion ORDERING, owed wire RESET prerequisite.
 *
 * Built on the geometry the pending_reset_drain row already proves. The
 * obligation is CREATED and PROVEN before completion is requested, and every
 * refusal is measured by an EXACT attempt count rather than inferred from a
 * generic pending bit -- an earlier revision of this row armed the seam
 * without ever dropping a second object, so `pending_reset` was never armed
 * and the row proved nothing.
 *
 * Order: drop-without-demand on, anchor object delivered, subscriber leaves,
 * a later object is dropped and the first reset attempt is refused ->
 * pending_reset genuinely retained; then the refusal is re-armed and
 * complete() is called, so the completion boundary attempts the same reset and
 * is refused again while the terminal generation must NOT advance.
 * ==================================================================== */
static void complete_waits_for_reset_row(moq_alloc_t *alloc)
{
    const char *NAME = "complete_waits_for_reset";
    int before = failures;
    fix_t fix;
    uint64_t now = 1000;
    memset(&fix, 0, sizeof(fix));
    /* drop_without_demand ON. Setup capacity is NOT weakened: the action ring
     * keeps the library default, so nothing but the one-shot seam refuses. */
    if (!completion_live_setup(&fix, alloc, now, NAME, true)) {
        fix_free(&fix); return;
    }

    set_phase(&fix, PH_ACT);
    uint64_t grp0 = moq_media_sender_test_catalog_group(fix.s);
    unsigned inst0 = moq_media_sender_test_retained_installs(fix.s);
    int cat0 = fix.cat_n;
    uint32_t r0 = moq_media_sender_test_reset_attempts(fix.s);
    MOQ_TEST_CHECK_EQ_INT((int)r0, 0);
    MOQ_TEST_CHECK(!moq_media_sender_test_track_pending_reset(fix.s, fix.media));

    /* (1) The subscriber leaves; demand is PROVEN gone. */
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_unsubscribe(moq_simpair_server(fix.sp),
                                     fix.med_sub, now), (int)MOQ_OK);
    for (int i = 0; i < 6; i++) cycle(&fix, now);
    MOQ_TEST_CHECK(!moq_media_sender_track_has_subscriber(fix.s, fix.media));

    /* (2) A later object is queued with no demand: the drop path discards it
     * and arms the wire RESET for the open group. The first flush attempt is
     * refused, so the obligation is RETAINED. */
    moq_media_sender_test_block_flush_reset_once(fix.s);
    MOQ_TEST_CHECK_EQ_INT((int)write_one(&fix, 6000), (int)MOQ_OK);
    cycle(&fix, now);
    /* (3) The obligation is real: exactly one attempt was made and refused,
     * pending_reset is retained, and the FIFO is empty (the object is gone). */
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_test_reset_attempts(fix.s),
                          (int)(r0 + 1));
    MOQ_TEST_CHECK(moq_media_sender_test_track_pending_reset(fix.s, fix.media));
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_media_sender_test_pending_obj_count(fix.s), 0);
    MOQ_TEST_CHECK(!moq_media_sender_test_catalog_dirty(fix.s));
    MOQ_TEST_CHECK(moq_media_sender_test_pending_send_work(fix.s));
    /* Nothing terminal can have happened yet. */
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_test_catalog_group(fix.s),
                          (int)grp0);
    MOQ_TEST_CHECK_EQ_INT(terminal_count(&fix), 0);

    /* (4) Re-arm and request permanent completion. The completion boundary
     * must attempt the owed reset again -- and be refused again. */
    moq_media_sender_test_block_flush_reset_once(fix.s);
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_complete(fix.s), (int)MOQ_OK);
    cycle(&fix, now);

    /* ---- THE REFUSED BOUNDARY: §11.3 step 1 is still owed ------------- */
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_test_reset_attempts(fix.s),
                          (int)(r0 + 2));   /* the boundary DID try again */
    MOQ_TEST_CHECK(moq_media_sender_test_track_pending_reset(fix.s, fix.media));
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_test_catalog_group(fix.s),
                          (int)grp0);                 /* not advanced */
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_test_retained_installs(fix.s),
                          (int)inst0);                /* nothing installed */
    MOQ_TEST_CHECK_EQ_INT(terminal_count(&fix), 0);   /* nothing on the wire */
    MOQ_TEST_CHECK_EQ_INT(fix.cat_n, cat0);
    MOQ_TEST_CHECK(moq_media_sender_test_pending_send_work(fix.s));

    /* ---- recovery: the reset EXACTLY once more, then ONE terminal ----- */
    MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_drain(fix.ep, 1000000), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_test_reset_attempts(fix.s),
                          (int)(r0 + 3));
    MOQ_TEST_CHECK(!moq_media_sender_test_track_pending_reset(fix.s, fix.media));
    MOQ_TEST_CHECK_EQ_INT(terminal_count(&fix), 1);
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_test_catalog_group(fix.s),
                          (int)(grp0 + 1));
    MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_drain(fix.ep, 200000), (int)MOQ_OK);
    for (int i = 0; i < 3; i++) cycle(&fix, now);
    MOQ_TEST_CHECK_EQ_INT(terminal_count(&fix), 1);
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_test_reset_attempts(fix.s),
                          (int)(r0 + 3));   /* no extra attempt */
    {   /* Recorded AS MEASURED; the row's contract oracle is the boundary
         * state above, not this delivery order. */
        exp_t e[3];
        memset(e, 0, sizeof(e));
        e[0].kind = MOQ_EVENT_SUBGROUP_RESET; e[0].sub = fix.med_sub._opaque;
        e[0].group = 0; e[0].eog = true; e[0].x = 1;
        e[1].kind = MOQ_EVENT_SUBGROUP_FINISHED; e[1].sub = fix.cat_sub._opaque;
        e[1].group = 1; e[1].eog = true;
        e[2].kind = MOQ_EVENT_OBJECT_RECEIVED; e[2].sub = fix.cat_sub._opaque;
        e[2].group = 2; e[2].object = 0; e[2].eog = true; e[2].has_payload = true;
        expect_phase(&fix, NAME, PH_ACT, e, 3);
    }

    teardown(&fix, NAME, fix.cat_n, fix.med_n, NULL, 0);
    report(NAME, failures - before, 0, false);
    fix_free(&fix);
}

/* ==================================================================== *
 * CONTROL -- drain retries a refused publisher-track REMOVAL.
 *
 * Deliberately NOT an MSF-01 §11.3 ordering row. §11.3 requires Track Ended
 * before the terminal catalog; moq_pub_end_track() itself completes the active
 * subscriptions with Track Ended (core/include/moq/publisher.h:514-544), so
 * once it has succeeded the later retryable moq_pub_remove_track() is LOCAL
 * registry cleanup and the spec makes it no further wire-order prerequisite.
 * The earlier classification of this row as a third ordering step is WITHDRAWN.
 *
 * What it still proves is a drain-cleanup obligation: with the removal refused
 * AFTER the end succeeded, the sender must stay PENDING, drain must retry the
 * removal exactly once and return MOQ_OK, and the final state must carry one
 * terminal catalog and one Track Ended outcome, no duplicate, and no remaining
 * local teardown. Whether the terminal catalog has already been staged at that
 * intermediate point is deliberately NOT asserted -- either staging policy is
 * permitted here.
 * ==================================================================== */
static void removal_cleanup_control(moq_alloc_t *alloc)
{
    const char *NAME = "control_drain_retries_track_removal";
    int before = failures;
    fix_t fix;
    uint64_t now = 1000;
    memset(&fix, 0, sizeof(fix));
    if (!completion_live_setup(&fix, alloc, now, NAME, false)) {
        fix_free(&fix); return;
    }

    set_phase(&fix, PH_ACT);
    uint64_t grp0 = moq_media_sender_test_catalog_group(fix.s);
    uint32_t rm0 = moq_media_sender_test_remove_attempts(fix.s);
    MOQ_TEST_CHECK_EQ_INT((int)rm0, 0);
    /* The removal shares the teardown walk step with the track end, so the
     * constrained-ring geometry cannot isolate this window (ring 4+ completes
     * both in one hook, ring 2 stalls the fixture). Hence the one-shot seam. */
    moq_media_sender_test_block_remove_track_once(fix.s);
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_complete(fix.s), (int)MOQ_OK);
    cycle(&fix, now);

    /* ---- the refusal FIRED, and it fired AFTER the end succeeded ------ */
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_test_remove_attempts(fix.s),
                          (int)(rm0 + 1));
    MOQ_TEST_CHECK(moq_media_sender_test_track_pub_ended(fix.s, fix.media));
    MOQ_TEST_CHECK(!moq_media_sender_test_track_teardown_done(fix.s, fix.media));
    /* Catalog staging policy at this intermediate point is UNCONSTRAINED. */
    MOQ_TEST_CHECK(moq_media_sender_test_pending_send_work(fix.s));
    MOQ_TEST_CHECK_EQ_INT(moq_media_sender_test_send_drain_status(fix.s),
                          1 /* MOQ_EP_SEND_DRAIN_PENDING */);

    /* ---- drain retries the removal EXACTLY once and completes --------- */
    MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_drain(fix.ep, 1000000), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_test_remove_attempts(fix.s),
                          (int)(rm0 + 2));
    MOQ_TEST_CHECK(moq_media_sender_test_track_teardown_done(fix.s, fix.media));
    MOQ_TEST_CHECK(!moq_media_sender_test_pending_send_work(fix.s));
    MOQ_TEST_CHECK_EQ_INT(terminal_count(&fix), 1);
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_test_catalog_group(fix.s),
                          (int)(grp0 + 1));
    /* A second drain adds nothing: no duplicate terminal, no extra attempt. */
    MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_drain(fix.ep, 200000), (int)MOQ_OK);
    for (int i = 0; i < 3; i++) cycle(&fix, now);
    MOQ_TEST_CHECK_EQ_INT(terminal_count(&fix), 1);
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_test_remove_attempts(fix.s),
                          (int)(rm0 + 2));
    {   /* One Track Ended outcome on the media stream, one terminal catalog
         * generation on the catalog stream, and nothing else. */
        exp_t e[6];
        memset(e, 0, sizeof(e));
        e[0].kind = MOQ_EVENT_SUBGROUP_FINISHED; e[0].sub = fix.med_sub._opaque;
        e[0].group = 0; e[0].eog = true;
        e[1].kind = MOQ_EVENT_OBJECT_RECEIVED; e[1].sub = fix.med_sub._opaque;
        e[1].group = 1; e[1].object = 0; e[1].eog = false;
        e[1].status = MOQ_OBJECT_END_OF_TRACK; e[1].prio = 128;
        e[2].kind = MOQ_EVENT_SUBGROUP_FINISHED; e[2].sub = fix.med_sub._opaque;
        e[2].group = 1; e[2].eog = false;
        e[3].kind = MOQ_EVENT_SUBGROUP_FINISHED; e[3].sub = fix.cat_sub._opaque;
        e[3].group = 1; e[3].eog = true;
        e[4].kind = MOQ_EVENT_OBJECT_RECEIVED; e[4].sub = fix.cat_sub._opaque;
        e[4].group = 2; e[4].object = 0; e[4].eog = true; e[4].has_payload = true;
        e[5].kind = MOQ_EVENT_SUBSCRIBE_DONE; e[5].sub = fix.med_sub._opaque;
        e[5].x = 2; e[5].y = 2;
        expect_phase(&fix, NAME, PH_ACT, e, 6);
    }

    teardown(&fix, NAME, fix.cat_n, fix.med_n, NULL, 0);
    report(NAME, failures - before, 0, false);
    fix_free(&fix);
}

/* ==================================================================== *
 * ROW -- the publish-initiated initial catalog object (term 8), isolated.
 * With publish_tracks the sender PUBLISHes its catalog track; once the peer
 * accepts, the retained initial catalog must also be written LIVE so a relay
 * caches it without a FETCH. That write is refused once at its production call
 * boundary, so the hook settles with live_catalog_sent false, no ring, no dirty
 * or staged catalog work and no other owed term. drain() must retry it and
 * emit the retained catalog identity exactly once.
 * ==================================================================== */
static void arm_publish_initiated_catalog(moq_alloc_t *alloc)
{
    const char *NAME = "publish_initiated_catalog_drain";
    int before = failures;
    fix_t fix;
    uint64_t now = 1000;
    memset(&fix, 0, sizeof(fix));
    fix.alloc = *alloc;
    fix.now = now;
    fix.publish_tracks = true;
    set_phase(&fix, PH_SETUP);
    fix.sp = make_pair(alloc);
    MOQ_TEST_CHECK(fix.sp != NULL);
    if (!fix.sp) { failures++; fix_free(&fix); return; }
    fix.ep = moq_endpoint_test_make_bare_alloc(alloc);
    MOQ_TEST_CHECK(fix.ep != NULL);
    if (!fix.ep) { failures++; fix_free(&fix); return; }
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_endpoint_test_install_probe(fix.ep, &fix.probe_mem,
                                             sizeof(fix.probe_mem),
                                             PROBE_DRAIN_DONE), (int)MOQ_OK);
    fix.probe = &fix.probe_mem;
    moq_endpoint_test_probe_set_service(fix.probe, probe_service, &fix);
    g_ns[0] = MOQ_BYTES_LITERAL("svc");
    g_ns[1] = MOQ_BYTES_LITERAL("done");
    moq_media_sender_cfg_t cfg;
    moq_media_sender_cfg_init_live_sized(&cfg, sizeof(cfg));
    cfg.namespace_ = (moq_namespace_t){ g_ns, 2 };
    cfg.catalog_refresh_interval_us = UINT64_MAX;
    cfg.publish_tracks = true;
    moq_media_sender_callbacks_init_sized(&cfg.callbacks, sizeof(cfg.callbacks));
    cfg.callbacks.ctx = &fix;
    cfg.callbacks.on_ready = on_ready_cb;
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_attach(fix.ep, &cfg, &fix.s),
                          (int)MOQ_OK);
    if (!fix.s) { failures++; fix_free(&fix); return; }

    /* Drive until the peer has accepted the catalog PUBLISH -- that acceptance
     * is what creates the demand the live write needs. */
    for (int i = 0; i < 10 && fix.pub_req_n == 0; i++) cycle(&fix, now);
    MOQ_TEST_CHECK_EQ_INT(fix.pub_req_n, 1);
    MOQ_TEST_CHECK(!moq_media_sender_test_live_catalog_sent(fix.s));
    {
        exp_t e[2];
        memset(e, 0, sizeof(e));
        e[0].kind = MOQ_EVENT_NAMESPACE_PUBLISHED; e[0].ann = fix.ann_handle;
        e[1].kind = MOQ_EVENT_PUBLISH_REQUEST;
        e[1].pub = fix.pub_handle; e[1].x = 1;
        expect_phase(&fix, NAME, PH_SETUP, e, 2);
    }

    set_phase(&fix, PH_ACT);
    /* The live-catalog write boundary has an EXACT attempt counter, so the
     * refusal is OBSERVED rather than inferred: a generic pending bit can be
     * true for another reason during PUBLISH establishment, and that would
     * make this row pass without the one-shot ever firing. */
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_media_sender_test_live_catalog_attempts(fix.s), 0);
    /* Refuse exactly the first live catalog write. The one-shot fires on the
     * first hook whose demand gate passes -- which is a cycle or two after the
     * peer's acceptance is processed -- so drive until the attempt has
     * actually been made and refused. */
    moq_media_sender_test_block_live_catalog_once(fix.s);
    for (int i = 0; i < 8; i++) {
        cycle(&fix, now);
        if (moq_media_sender_test_live_catalog_attempts(fix.s) >= 1) break;
    }

    /* ---- THE ISOLATED STATE ------------------------------------------ */
    /* EXACTLY one attempt was made, and it was refused. */
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_media_sender_test_live_catalog_attempts(fix.s), 1);
    MOQ_TEST_CHECK(!moq_media_sender_test_live_catalog_sent(fix.s));
    MOQ_TEST_CHECK(!moq_media_sender_test_catalog_dirty(fix.s));
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_media_sender_test_pending_obj_count(fix.s), 0);
    {
        moq_media_sender_stats_t mst; memset(&mst, 0, sizeof(mst));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_media_sender_get_stats(fix.s, &mst, sizeof(mst)), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)mst.objects_queued, 0);
    }
    MOQ_TEST_CHECK(moq_media_sender_test_pending_send_work(fix.s));
    MOQ_TEST_CHECK_EQ_INT(fix.cat_n, 0);        /* nothing delivered yet */

    /* drain() must retry it and deliver the retained catalog identity once. */
    MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_drain(fix.ep, 1000000), (int)MOQ_OK);
    /* drain's successful retry is the SECOND attempt at the same boundary. */
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_media_sender_test_live_catalog_attempts(fix.s), 2);
    MOQ_TEST_CHECK(moq_media_sender_test_live_catalog_sent(fix.s));
    MOQ_TEST_CHECK_EQ_INT(fix.cat_n, 1);
    if (fix.cat_n == 1) {
        MOQ_TEST_CHECK_EQ_INT((int)fix.cat[0].group, 0);
        MOQ_TEST_CHECK_EQ_INT((int)fix.cat[0].object, 0);
        MOQ_TEST_CHECK(fix.cat[0].parsed && !fix.cat[0].is_delta &&
                       !fix.cat[0].is_complete);
    }
    for (int i = 0; i < 3; i++) cycle(&fix, now);
    MOQ_TEST_CHECK_EQ_INT(fix.cat_n, 1);        /* exactly once */
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_media_sender_test_live_catalog_attempts(fix.s), 2);
    {   /* exactly the retained initial catalog, delivered on the publication */
        exp_t e[1];
        memset(e, 0, sizeof(e));
        e[0].kind = MOQ_EVENT_OBJECT_RECEIVED;
        e[0].pub = fix.pub_handle;
        e[0].group = 0; e[0].object = 0; e[0].eog = true; e[0].has_payload = true;
        expect_phase(&fix, NAME, PH_ACT, e, 1);
    }

    teardown(&fix, NAME, fix.cat_n, fix.med_n, NULL, 0);
    report(NAME, failures - before, 0, false);
    fix_free(&fix);
}


#ifdef MOQ_RAW_INIT_RETAINED_ROW
#include "raw_init_oracle.h"
/* ==================================================================== *
 * CONTRACT -- an init-bearing RAW/LOC track's record is RETAINED through the
 * MSF-01 initRef / root Initialization Data List, on the late add itself and
 * after a later unrelated mutation, and every live generation the peer saw
 * carries the shape the ruled producer policy declares: an init-bearing add
 * is one independent generation; a no-record add is base plus one delta.
 *
 * The retained oracle is the MSF-01 §5 one: the Joining FETCH at offset 0,
 * rebuilt from FETCH_OBJECT payloads through the real parser and the
 * product's own delta application. Live generations are classified from the
 * peer's decoded bytes. Fixture records are OPAQUE test bytes; the ASC is the
 * derived AAC-LC 48 kHz stereo AudioSpecificConfig (0x11 0x90).
 * ==================================================================== */
static const uint8_t RAW_INIT_VIDEO_RECORD[] = {
    0x01, 0x64, 0x00, 0x1f, 0xff, 0xe1, 0x00, 0x04, 0x67, 0x64, 0x00, 0x1f,
    0x01, 0x00, 0x04, 0x68, 0xee, 0x3c, 0x80
};
static const uint8_t RAW_INIT_AUDIO_ASC[] = { 0x11, 0x90 };

static bool raw_init_add(fix_t *fix, const char *name, bool audio,
                         const uint8_t *init, size_t init_len)
{
    moq_media_track_cfg_t tc;
    moq_media_track_cfg_init(&tc);
    tc.name = (moq_bytes_t){ (const uint8_t *)name, strlen(name) };
    tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
    tc.is_live = true;
    if (audio) {
        tc.media_type = MOQ_MEDIA_TYPE_AUDIO;
        tc.codec = MOQ_BYTES_LITERAL("mp4a.40.2");
        tc.bitrate = 128000;
        tc.samplerate = 48000;
        tc.channel_config = MOQ_BYTES_LITERAL("2");
    } else {
        tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
        tc.codec = MOQ_BYTES_LITERAL("avc1.64001f");
        tc.bitrate = 1500000;
        tc.width = 640; tc.height = 480;
    }
    if (init_len > 0) tc.init_data = (moq_bytes_t){ init, init_len };
    moq_media_track_t *t = NULL;
    return moq_media_sender_add_track(fix->s, &tc, &t) == MOQ_OK;
}

/* One accepted mutation, flushed and delivered: drain, then cycle until the
 * peer holds `want` catalog records or the budget ends. */
static void raw_init_flush(fix_t *fix, uint64_t now, int want)
{
    MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_drain(fix->ep, 1000000), (int)MOQ_OK);
    for (int i = 0; i < 8 && fix->cat_n < want; i++) cycle(fix, now);
    settle_no_cycle(fix);
}

/* Re-decode one observed live generation and judge it through the oracle:
 * an independent generation of exactly `tracks` tracks whose records are the
 * declared ones. A record that was never received, or did not decode, is a
 * NAMED diagnostic and is never read as metadata. */
static void raw_init_expect_live(const fix_t *fix, int i, const char *name,
                                 bool with_late)
{
    const size_t tracks = with_late ? 2u : 1u;
    if (i >= fix->cat_n) {
        fprintf(stderr, "FAIL %s: live record %d was never received (%d held)\n",
                name, i, fix->cat_n);
        failures++;
        return;
    }
    if (!fix->cat[i].parsed) {
        fprintf(stderr, "FAIL %s: live record %d did not decode when received\n",
                name, i);
        failures++;
        return;
    }
    moq_msf_catalog_t c;
    memset(&c, 0, sizeof(c));
    moq_bytes_t json = { fix->cat[i].buf, fix->cat[i].len };
    if (moq_msf_catalog_parse(&fix->alloc, json, &c) != MOQ_OK) {
        fprintf(stderr, "FAIL %s: live record %d does not parse\n", name, i);
        failures++;
        return;
    }
    MOQ_TEST_CHECK(c.delta_update == NULL);
    MOQ_TEST_CHECK_EQ_U64(c.track_count, tracks);
    MOQ_TEST_CHECK_EQ_U64(c.init_data_count, tracks);
    MOQ_TEST_CHECK(raw_init_list_is_inline(&c));
    MOQ_TEST_CHECK(raw_init_no_legacy_field(&c));
    MOQ_TEST_CHECK(raw_init_list_is_exactly_used(&c));
    MOQ_TEST_CHECK_EQ_INT((int)raw_init_resolve(&c, "v", RAW_INIT_VIDEO_RECORD,
                                                sizeof(RAW_INIT_VIDEO_RECORD)),
                          (int)RAW_INIT_OK);
    if (with_late)
        MOQ_TEST_CHECK_EQ_INT((int)raw_init_resolve(&c, "late", RAW_INIT_AUDIO_ASC,
                                                    sizeof(RAW_INIT_AUDIO_ASC)),
                              (int)RAW_INIT_OK);
    moq_msf_catalog_cleanup(&fix->alloc, &c);
}

static void raw_init_retained_row(moq_alloc_t *alloc)
{
    const char *NAME = "raw_init_retained_records";
    int before = failures;
    fix_t fix;
    uint64_t now = 1000;
    if (!fix_init(&fix, alloc, now)) {
        fprintf(stderr, "FAIL %s: fixture setup\n", NAME);
        failures++; fix_free(&fix); return;
    }
    /* The peer subscribes before any media track exists, so every later
     * generation lands on the wire and is decoded -- with the Largest Object
     * filter, which the Joining FETCH below requires of the subscription it
     * joins (draft-16 section 9.16.2). */
    set_phase(&fix, PH_SUB);
    if (!subscribe_track_filter(&fix, now, MOQ_MSF_CATALOG_TRACK_NAME,
                                strlen(MOQ_MSF_CATALOG_TRACK_NAME),
                                MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT,
                                &fix.cat_sub)) {
        fprintf(stderr, "FAIL %s: catalog subscribe\n", NAME);
        failures++; fix_free(&fix); return;
    }
    fix.cat_subscribed = true;
    for (int i = 0; i < 16 && fix.sub_ok_n == 0; i++) cycle(&fix, now);
    MOQ_TEST_CHECK_EQ_INT(fix.sub_ok_n, 1);
    {   /* DECLARED, not read back: the zero-track initial catalog is
         * generation 0, object 0, so the retained Largest is exactly (0,0). */
        exp_t e[1];
        memset(e, 0, sizeof(e));
        e[0].kind = MOQ_EVENT_SUBSCRIBE_OK;
        e[0].sub = fix.cat_sub._opaque;
        e[0].x = 1;
        e[0].has_largest = true;
        e[0].y = 0;
        e[0].largest_obj = 0;
        expect_phase(&fix, NAME, PH_SUB, e, 1);
    }
    set_phase(&fix, PH_LIVE);

    /* ---- LIVE: the first init-bearing RAW track --------------------- */
    MOQ_TEST_CHECK(raw_init_add(&fix, "v", false, RAW_INIT_VIDEO_RECORD,
                                sizeof(RAW_INIT_VIDEO_RECORD)));
    raw_init_flush(&fix, now, 1);
    if (sticky(&fix, NAME)) { fix_free(&fix); return; }
    {   /* declared: ONE independent generation, group 1 */
        exp_t e[1];
        memset(e, 0, sizeof(e));
        e[0].kind = MOQ_EVENT_OBJECT_RECEIVED; e[0].sub = fix.cat_sub._opaque;
        e[0].group = 1; e[0].object = 0; e[0].eog = true; e[0].has_payload = true;
        expect_phase(&fix, NAME, PH_LIVE, e, 1);
    }
    MOQ_TEST_CHECK_EQ_INT(fix.cat_n, 1);
    raw_init_expect_live(&fix, 0, NAME, false);

    /* ---- ACT: the late init-bearing add, then an unrelated add ------- */
    set_phase(&fix, PH_ACT);
    MOQ_TEST_CHECK(raw_init_add(&fix, "late", true, RAW_INIT_AUDIO_ASC,
                                sizeof(RAW_INIT_AUDIO_ASC)));
    raw_init_flush(&fix, now, 2);
    MOQ_TEST_CHECK(raw_init_add(&fix, "plain", false, NULL, 0));
    raw_init_flush(&fix, now, 4);
    if (sticky(&fix, NAME)) { fix_free(&fix); return; }
    {   /* declared: group 1's subgroup terminal, then the late add as ONE
         * independent generation (group 2), its subgroup terminal, then the
         * no-record add as its base plus ONE delta (group 3). */
        exp_t e[5];
        memset(e, 0, sizeof(e));
        for (int k = 0; k < 5; k++) e[k].sub = fix.cat_sub._opaque;
        e[0].kind = MOQ_EVENT_SUBGROUP_FINISHED; e[0].group = 1; e[0].eog = true;
        e[1].kind = MOQ_EVENT_OBJECT_RECEIVED; e[1].group = 2; e[1].object = 0;
        e[1].eog = true; e[1].has_payload = true;
        e[2].kind = MOQ_EVENT_SUBGROUP_FINISHED; e[2].group = 2; e[2].eog = true;
        e[3].kind = MOQ_EVENT_OBJECT_RECEIVED; e[3].group = 3; e[3].object = 0;
        e[3].eog = true; e[3].has_payload = true;
        e[4].kind = MOQ_EVENT_OBJECT_RECEIVED; e[4].group = 3; e[4].object = 1;
        e[4].eog = true; e[4].has_payload = true;
        expect_phase(&fix, NAME, PH_ACT, e, 5);
    }
    MOQ_TEST_CHECK_EQ_INT(fix.cat_n, 4);
    if (fix.cat_n == 4) {
        raw_init_expect_live(&fix, 1, NAME, true);        /* group 2: independent */
        raw_init_expect_live(&fix, 2, NAME, true);        /* group 3 base */
        MOQ_TEST_CHECK(fix.cat[3].parsed && fix.cat[3].is_delta);
        MOQ_TEST_CHECK_EQ_U64(fix.cat[3].delta_ops, 1);
        MOQ_TEST_CHECK_EQ_U64(fix.cat[3].delta_add, 1);
        MOQ_TEST_CHECK_EQ_U64(fix.cat[3].delta_remove, 0);
    }

    /* ---- RECOVER: the MSF-01 §5 retained oracle, Joining FETCH(0) ---- */
    /* A Joining FETCH names a FRESH Largest Object subscription, as the
     * accepted retained rows do: the live subscription above has already
     * delivered and joining it is refused (FETCH_ERROR), and one session
     * holds one subscription per track, so the live one is released first
     * (a local unsubscribe; the peer observes nothing for it). The retained
     * generation is then what THIS join delivers, never a live object. */
    set_phase(&fix, PH_RECOVER);
    moq_subscription_t join_sub;
    memset(&join_sub, 0, sizeof(join_sub));
    {
        moq_session_t *srv = moq_simpair_server(fix.sp);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_unsubscribe(srv, fix.cat_sub, now),
                              (int)MOQ_OK);
        for (int i = 0; i < 6; i++) cycle(&fix, now);
        fix.cat_subscribed = false;   /* anything more on it is foreign */
        MOQ_TEST_CHECK(subscribe_track_filter(&fix, now, MOQ_MSF_CATALOG_TRACK_NAME,
                                              strlen(MOQ_MSF_CATALOG_TRACK_NAME),
                                              MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT,
                                              &join_sub));
        for (int i = 0; i < 16 && fix.sub_ok_n < 2; i++) cycle(&fix, now);
        MOQ_TEST_CHECK_EQ_INT(fix.sub_ok_n, 2);

        moq_fetch_cfg_t fc; moq_fetch_cfg_init(&fc);
        fc.is_joining = true;
        fc.joining_relative = true;
        fc.joining_sub = join_sub;
        fc.joining_start = 0;
        memset(&fix.fetch, 0, sizeof(fix.fetch));
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(srv, &fc, now, &fix.fetch),
                              (int)MOQ_OK);
        for (int i = 0; i < 24 && fix.fetch_complete_n == 0; i++)
            cycle(&fix, now);
    }
    if (sticky(&fix, NAME)) { fix_free(&fix); return; }
    MOQ_TEST_CHECK_EQ_INT(fix.fetch_ok_n, 1);
    MOQ_TEST_CHECK_EQ_INT(fix.fetch_complete_n, 1);
    MOQ_TEST_CHECK_EQ_INT(fix.fet_n, 2);
    {   /* declared: the released subscription's open group-3 subgroup is
         * RESET to it (the RESET_STREAM application error the transport
         * carried, 1, as the accepted rows measure); the join's OK with the
         * retained largest (3,1); the retained generation -- group 3, objects
         * 0 and 1 -- then COMPLETE */
        exp_t e[6];
        memset(e, 0, sizeof(e));
        e[0].kind = MOQ_EVENT_SUBGROUP_RESET; e[0].sub = fix.cat_sub._opaque;
        e[0].group = 3; e[0].eog = true; e[0].x = 1;
        e[1].kind = MOQ_EVENT_SUBSCRIBE_OK; e[1].sub = join_sub._opaque;
        e[1].x = 2; e[1].has_largest = true; e[1].y = 3; e[1].largest_obj = 1;
        e[2].kind = MOQ_EVENT_FETCH_OK; e[2].fetch = fix.fetch._opaque;
        e[2].y = 3; e[2].largest_obj = 2;
        for (int k = 0; k < 2; k++) {
            e[3 + k].kind = MOQ_EVENT_FETCH_OBJECT;
            e[3 + k].fetch = fix.fetch._opaque;
            e[3 + k].group = 3; e[3 + k].object = (uint64_t)k;
            e[3 + k].has_payload = true;
        }
        e[5].kind = MOQ_EVENT_FETCH_COMPLETE; e[5].fetch = fix.fetch._opaque;
        expect_phase(&fix, NAME, PH_RECOVER, e, 6);
    }
    /* THE CONTRACT, on the effective catalog the product's own apply built
     * from what the FETCH delivered. */
    MOQ_TEST_CHECK(fix.eff_valid && !fix.eff_fail);
    if (fix.eff_valid && !fix.eff_fail) {
        const moq_msf_catalog_t *c = &fix.eff_cat;
        MOQ_TEST_CHECK_EQ_U64(c->track_count, 3);
        MOQ_TEST_CHECK_EQ_U64(c->init_data_count, 2);
        MOQ_TEST_CHECK(raw_init_list_is_inline(c));
        MOQ_TEST_CHECK(raw_init_no_legacy_field(c));
        MOQ_TEST_CHECK(raw_init_list_is_exactly_used(c));
        MOQ_TEST_CHECK_EQ_INT((int)raw_init_resolve(c, "v", RAW_INIT_VIDEO_RECORD,
                                                    sizeof(RAW_INIT_VIDEO_RECORD)),
                              (int)RAW_INIT_OK);
        MOQ_TEST_CHECK_EQ_INT((int)raw_init_resolve(c, "late", RAW_INIT_AUDIO_ASC,
                                                    sizeof(RAW_INIT_AUDIO_ASC)),
                              (int)RAW_INIT_OK);
        MOQ_TEST_CHECK(!raw_init_same_ref(raw_init_track(c, "v"),
                                          raw_init_track(c, "late")));
        const moq_msf_track_t *p = raw_init_track(c, "plain");
        MOQ_TEST_CHECK(p != NULL);
        if (p) MOQ_TEST_CHECK(!p->has_init_ref && !p->has_init_data);
    }

    teardown(&fix, NAME, fix.cat_n, fix.med_n, NULL, 0);
    report(NAME, failures - before, 0, false);
    fix_free(&fix);
}
#endif /* MOQ_RAW_INIT_RETAINED_ROW */


#ifdef MOQ_RAW_INIT_IDENTITY_ROW
#include "raw_init_oracle.h"
/* ==================================================================== *
 * CONTRACT -- a surviving track's initRef is STABLE across committed
 * generations, through the real pump: v1 and v2 share one entry; v1 (the
 * introducer) is removed while an init-bearing track is added; a later
 * no-record add follows. Every live generation and the retained Joining
 * FETCH(0) effective catalog must carry v2 under the reference it was
 * declared with, resolving to the same exact bytes. Records are OPAQUE.
 * ==================================================================== */
static const uint8_t RAW_ID_VIDEO_RECORD[] = {
    0x01, 0x64, 0x00, 0x1f, 0xff, 0xe1, 0x00, 0x04, 0x67, 0x64, 0x00, 0x1f,
    0x01, 0x00, 0x04, 0x68, 0xee, 0x3c, 0x80
};
static const uint8_t RAW_ID_AUDIO_ASC[] = { 0x11, 0x90 };

static bool raw_id_add(fix_t *fix, const char *name, bool audio,
                       const uint8_t *init, size_t init_len,
                       moq_media_track_t **out)
{
    moq_media_track_cfg_t tc;
    moq_media_track_cfg_init(&tc);
    tc.name = (moq_bytes_t){ (const uint8_t *)name, strlen(name) };
    tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
    tc.is_live = true;
    if (audio) {
        tc.media_type = MOQ_MEDIA_TYPE_AUDIO;
        tc.codec = MOQ_BYTES_LITERAL("mp4a.40.2");
        tc.bitrate = 128000;
        tc.samplerate = 48000;
        tc.channel_config = MOQ_BYTES_LITERAL("2");
    } else {
        tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
        tc.codec = MOQ_BYTES_LITERAL("avc1.64001f");
        tc.bitrate = 1500000;
        tc.width = 640; tc.height = 480;
    }
    if (init_len > 0) tc.init_data = (moq_bytes_t){ init, init_len };
    moq_media_track_t *t = NULL;
    bool ok = moq_media_sender_add_track(fix->s, &tc, &t) == MOQ_OK;
    if (out) *out = t;
    return ok;
}

static void raw_id_flush(fix_t *fix, uint64_t now, int want)
{
    MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_drain(fix->ep, 1000000), (int)MOQ_OK);
    for (int i = 0; i < 8 && fix->cat_n < want; i++) cycle(fix, now);
    settle_no_cycle(fix);
}

/* Parse live record i; on true the caller owns *c. */
static bool raw_id_live(const fix_t *fix, int i, const char *name,
                        moq_msf_catalog_t *c)
{
    if (i >= fix->cat_n) {
        fprintf(stderr, "FAIL %s: live record %d was never received (%d held)\n",
                name, i, fix->cat_n);
        failures++;
        return false;
    }
    memset(c, 0, sizeof(*c));
    moq_bytes_t json = { fix->cat[i].buf, fix->cat[i].len };
    if (moq_msf_catalog_parse(&fix->alloc, json, c) != MOQ_OK) {
        fprintf(stderr, "FAIL %s: live record %d does not parse\n", name, i);
        failures++;
        return false;
    }
    return true;
}

static void raw_init_identity_row(moq_alloc_t *alloc)
{
    const char *NAME = "raw_init_identity_retained";
    int before = failures;
    fix_t fix;
    uint64_t now = 1000;
    if (!fix_init(&fix, alloc, now)) {
        fprintf(stderr, "FAIL %s: fixture setup\n", NAME);
        failures++; fix_free(&fix); return;
    }
    set_phase(&fix, PH_SUB);
    if (!subscribe_track_filter(&fix, now, MOQ_MSF_CATALOG_TRACK_NAME,
                                strlen(MOQ_MSF_CATALOG_TRACK_NAME),
                                MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT,
                                &fix.cat_sub)) {
        fprintf(stderr, "FAIL %s: catalog subscribe\n", NAME);
        failures++; fix_free(&fix); return;
    }
    fix.cat_subscribed = true;
    for (int i = 0; i < 16 && fix.sub_ok_n == 0; i++) cycle(&fix, now);
    MOQ_TEST_CHECK_EQ_INT(fix.sub_ok_n, 1);
    {
        exp_t e[1];
        memset(e, 0, sizeof(e));
        e[0].kind = MOQ_EVENT_SUBSCRIBE_OK; e[0].sub = fix.cat_sub._opaque;
        e[0].x = 1; e[0].has_largest = true; e[0].y = 0; e[0].largest_obj = 0;
        expect_phase(&fix, NAME, PH_SUB, e, 1);
    }
    set_phase(&fix, PH_LIVE);

    /* ---- LIVE: v1 and v2, identical records, ONE entry --------------- */
    moq_media_track_t *v1 = NULL;
    MOQ_TEST_CHECK(raw_id_add(&fix, "v1", false, RAW_ID_VIDEO_RECORD,
                              sizeof(RAW_ID_VIDEO_RECORD), &v1));
    MOQ_TEST_CHECK(raw_id_add(&fix, "v2", false, RAW_ID_VIDEO_RECORD,
                              sizeof(RAW_ID_VIDEO_RECORD), NULL));
    raw_id_flush(&fix, now, 1);
    if (sticky(&fix, NAME)) { fix_free(&fix); return; }
    {   /* declared: both init-bearing adds coalesce into ONE independent
         * generation, group 1 */
        exp_t e[1];
        memset(e, 0, sizeof(e));
        e[0].kind = MOQ_EVENT_OBJECT_RECEIVED; e[0].sub = fix.cat_sub._opaque;
        e[0].group = 1; e[0].object = 0; e[0].eog = true; e[0].has_payload = true;
        expect_phase(&fix, NAME, PH_LIVE, e, 1);
    }
    raw_init_ref_t declared;
    memset(&declared, 0, sizeof(declared));
    {
        moq_msf_catalog_t c;
        if (raw_id_live(&fix, 0, NAME, &c)) {
            MOQ_TEST_CHECK(c.delta_update == NULL);
            MOQ_TEST_CHECK_EQ_U64(c.track_count, 2);
            MOQ_TEST_CHECK_EQ_U64(c.init_data_count, 1);
            MOQ_TEST_CHECK(raw_init_same_ref(raw_init_track(&c, "v1"),
                                             raw_init_track(&c, "v2")));
            declared = raw_init_ref_of(&c, "v2");
            moq_msf_catalog_cleanup(&fix.alloc, &c);
        }
    }
    MOQ_TEST_CHECK(declared.ok);

    /* ---- ACT: remove the introducer + add a record; then a bare add --- */
    set_phase(&fix, PH_ACT);
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_remove_track(fix.s, v1), (int)MOQ_OK);
    MOQ_TEST_CHECK(raw_id_add(&fix, "a", true, RAW_ID_AUDIO_ASC,
                              sizeof(RAW_ID_AUDIO_ASC), NULL));
    raw_id_flush(&fix, now, 2);
    MOQ_TEST_CHECK(raw_id_add(&fix, "plain", false, NULL, 0, NULL));
    raw_id_flush(&fix, now, 4);
    if (sticky(&fix, NAME)) { fix_free(&fix); return; }
    {   /* declared: g1's subgroup terminal; the remove+record add as ONE
         * independent generation (g2); its terminal; the bare add as base
         * plus ONE delta (g3) */
        exp_t e[5];
        memset(e, 0, sizeof(e));
        for (int k = 0; k < 5; k++) e[k].sub = fix.cat_sub._opaque;
        e[0].kind = MOQ_EVENT_SUBGROUP_FINISHED; e[0].group = 1; e[0].eog = true;
        e[1].kind = MOQ_EVENT_OBJECT_RECEIVED; e[1].group = 2; e[1].object = 0;
        e[1].eog = true; e[1].has_payload = true;
        e[2].kind = MOQ_EVENT_SUBGROUP_FINISHED; e[2].group = 2; e[2].eog = true;
        e[3].kind = MOQ_EVENT_OBJECT_RECEIVED; e[3].group = 3; e[3].object = 0;
        e[3].eog = true; e[3].has_payload = true;
        e[4].kind = MOQ_EVENT_OBJECT_RECEIVED; e[4].group = 3; e[4].object = 1;
        e[4].eog = true; e[4].has_payload = true;
        expect_phase(&fix, NAME, PH_ACT, e, 5);
    }
    /* Each declared independent live generation -- g2, and g3's base --
     * re-declares v2 under its ORIGINAL reference, resolving to the same
     * bytes; v1 is gone. A missing record is a named diagnostic. */
    for (int i = 1; i <= 2; i++) {
        moq_msf_catalog_t c;
        if (!raw_id_live(&fix, i, NAME, &c)) continue;
        MOQ_TEST_CHECK(c.delta_update == NULL);
        MOQ_TEST_CHECK_EQ_U64(c.track_count, 2);            /* v2, a */
        MOQ_TEST_CHECK(raw_init_track(&c, "v1") == NULL);
        MOQ_TEST_CHECK_EQ_U64(c.init_data_count, 2);
        MOQ_TEST_CHECK(raw_init_list_is_exactly_used(&c));
        MOQ_TEST_CHECK_EQ_INT((int)raw_init_resolve(&c, "v2", RAW_ID_VIDEO_RECORD,
                                                    sizeof(RAW_ID_VIDEO_RECORD)),
                              (int)RAW_INIT_OK);
        MOQ_TEST_CHECK_EQ_INT((int)raw_init_resolve(&c, "a", RAW_ID_AUDIO_ASC,
                                                    sizeof(RAW_ID_AUDIO_ASC)),
                              (int)RAW_INIT_OK);
        MOQ_TEST_CHECK(raw_init_ref_eq(declared, raw_init_ref_of(&c, "v2")));
        moq_msf_catalog_cleanup(&fix.alloc, &c);
    }

    /* ---- RECOVER: the retained oracle ------------------------------- */
    set_phase(&fix, PH_RECOVER);
    moq_subscription_t join_sub;
    memset(&join_sub, 0, sizeof(join_sub));
    {
        moq_session_t *srv = moq_simpair_server(fix.sp);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_unsubscribe(srv, fix.cat_sub, now),
                              (int)MOQ_OK);
        for (int i = 0; i < 6; i++) cycle(&fix, now);
        fix.cat_subscribed = false;
        MOQ_TEST_CHECK(subscribe_track_filter(&fix, now, MOQ_MSF_CATALOG_TRACK_NAME,
                                              strlen(MOQ_MSF_CATALOG_TRACK_NAME),
                                              MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT,
                                              &join_sub));
        for (int i = 0; i < 16 && fix.sub_ok_n < 2; i++) cycle(&fix, now);
        MOQ_TEST_CHECK_EQ_INT(fix.sub_ok_n, 2);
        moq_fetch_cfg_t fc; moq_fetch_cfg_init(&fc);
        fc.is_joining = true;
        fc.joining_relative = true;
        fc.joining_sub = join_sub;
        fc.joining_start = 0;
        memset(&fix.fetch, 0, sizeof(fix.fetch));
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(srv, &fc, now, &fix.fetch),
                              (int)MOQ_OK);
        for (int i = 0; i < 24 && fix.fetch_complete_n == 0; i++)
            cycle(&fix, now);
    }
    if (sticky(&fix, NAME)) { fix_free(&fix); return; }
    MOQ_TEST_CHECK_EQ_INT(fix.fetch_ok_n, 1);
    MOQ_TEST_CHECK_EQ_INT(fix.fetch_complete_n, 1);
    MOQ_TEST_CHECK_EQ_INT(fix.fet_n, 2);
    {
        exp_t e[6];
        memset(e, 0, sizeof(e));
        e[0].kind = MOQ_EVENT_SUBGROUP_RESET; e[0].sub = fix.cat_sub._opaque;
        e[0].group = 3; e[0].eog = true; e[0].x = 1;
        e[1].kind = MOQ_EVENT_SUBSCRIBE_OK; e[1].sub = join_sub._opaque;
        e[1].x = 2; e[1].has_largest = true; e[1].y = 3; e[1].largest_obj = 1;
        e[2].kind = MOQ_EVENT_FETCH_OK; e[2].fetch = fix.fetch._opaque;
        e[2].y = 3; e[2].largest_obj = 2;
        for (int k = 0; k < 2; k++) {
            e[3 + k].kind = MOQ_EVENT_FETCH_OBJECT;
            e[3 + k].fetch = fix.fetch._opaque;
            e[3 + k].group = 3; e[3 + k].object = (uint64_t)k;
            e[3 + k].has_payload = true;
        }
        e[5].kind = MOQ_EVENT_FETCH_COMPLETE; e[5].fetch = fix.fetch._opaque;
        expect_phase(&fix, NAME, PH_RECOVER, e, 6);
    }
    MOQ_TEST_CHECK(fix.eff_valid && !fix.eff_fail);
    if (fix.eff_valid && !fix.eff_fail) {
        const moq_msf_catalog_t *c = &fix.eff_cat;
        MOQ_TEST_CHECK_EQ_U64(c->track_count, 3);           /* v2, a, plain */
        MOQ_TEST_CHECK(raw_init_track(c, "v1") == NULL);
        MOQ_TEST_CHECK_EQ_U64(c->init_data_count, 2);
        MOQ_TEST_CHECK(raw_init_list_is_exactly_used(c));
        MOQ_TEST_CHECK_EQ_INT((int)raw_init_resolve(c, "v2", RAW_ID_VIDEO_RECORD,
                                                    sizeof(RAW_ID_VIDEO_RECORD)),
                              (int)RAW_INIT_OK);
        MOQ_TEST_CHECK_EQ_INT((int)raw_init_resolve(c, "a", RAW_ID_AUDIO_ASC,
                                                    sizeof(RAW_ID_AUDIO_ASC)),
                              (int)RAW_INIT_OK);
        MOQ_TEST_CHECK(raw_init_ref_eq(declared, raw_init_ref_of(c, "v2")));
        const moq_msf_track_t *p = raw_init_track(c, "plain");
        MOQ_TEST_CHECK(p != NULL);
        if (p) MOQ_TEST_CHECK(!p->has_init_ref && !p->has_init_data);
    }

    teardown(&fix, NAME, fix.cat_n, fix.med_n, NULL, 0);
    report(NAME, failures - before, 0, false);
    fix_free(&fix);
}

/* Whether a removed track has fully torn down (media_sender.c test seam). */
bool moq_media_sender_test_track_teardown_done(const moq_media_sender_t *s,
                                               const moq_media_track_t *t);

static const uint8_t RAW_ID_VIDEO_RECORD_2[] = {
    0x01, 0x4d, 0x40, 0x1e, 0xff, 0xe1, 0x00, 0x04, 0x67, 0x4d, 0x40, 0x1e,
    0x01, 0x00, 0x04, 0x68, 0xee, 0x3c, 0x80
};

/* ==================================================================== *
 * CONTRACT -- public name reuse after ACTUAL teardown: v1 and v2 share an
 * entry; v1 is removed and torn down by the real pump; a replacement named
 * v1 with DIFFERENT bytes is added. The surviving v2 keeps its identity and
 * data; the replacement gets its own entry; the two identities differ, and
 * every generation and the retained catalog resolve both byte-exact. No
 * teardown flag is forged: readiness of the name is read from the seam and
 * proven by the public add itself succeeding.
 * ==================================================================== */
static void raw_init_reuse_row(moq_alloc_t *alloc)
{
    const char *NAME = "raw_init_identity_name_reuse";
    int before = failures;
    fix_t fix;
    uint64_t now = 1000;
    if (!fix_init(&fix, alloc, now)) {
        fprintf(stderr, "FAIL %s: fixture setup\n", NAME);
        failures++; fix_free(&fix); return;
    }
    set_phase(&fix, PH_SUB);
    if (!subscribe_track_filter(&fix, now, MOQ_MSF_CATALOG_TRACK_NAME,
                                strlen(MOQ_MSF_CATALOG_TRACK_NAME),
                                MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT,
                                &fix.cat_sub)) {
        fprintf(stderr, "FAIL %s: catalog subscribe\n", NAME);
        failures++; fix_free(&fix); return;
    }
    fix.cat_subscribed = true;
    for (int i = 0; i < 16 && fix.sub_ok_n == 0; i++) cycle(&fix, now);
    MOQ_TEST_CHECK_EQ_INT(fix.sub_ok_n, 1);
    {
        exp_t e[1];
        memset(e, 0, sizeof(e));
        e[0].kind = MOQ_EVENT_SUBSCRIBE_OK; e[0].sub = fix.cat_sub._opaque;
        e[0].x = 1; e[0].has_largest = true; e[0].y = 0; e[0].largest_obj = 0;
        expect_phase(&fix, NAME, PH_SUB, e, 1);
    }
    set_phase(&fix, PH_LIVE);

    moq_media_track_t *v1 = NULL;
    MOQ_TEST_CHECK(raw_id_add(&fix, "v1", false, RAW_ID_VIDEO_RECORD,
                              sizeof(RAW_ID_VIDEO_RECORD), &v1));
    MOQ_TEST_CHECK(raw_id_add(&fix, "v2", false, RAW_ID_VIDEO_RECORD,
                              sizeof(RAW_ID_VIDEO_RECORD), NULL));
    raw_id_flush(&fix, now, 1);
    if (sticky(&fix, NAME)) { fix_free(&fix); return; }
    {
        exp_t e[1];
        memset(e, 0, sizeof(e));
        e[0].kind = MOQ_EVENT_OBJECT_RECEIVED; e[0].sub = fix.cat_sub._opaque;
        e[0].group = 1; e[0].object = 0; e[0].eog = true; e[0].has_payload = true;
        expect_phase(&fix, NAME, PH_LIVE, e, 1);
    }
    raw_init_ref_t declared;
    memset(&declared, 0, sizeof(declared));
    {
        moq_msf_catalog_t c;
        if (raw_id_live(&fix, 0, NAME, &c)) {
            MOQ_TEST_CHECK_EQ_U64(c.init_data_count, 1);
            MOQ_TEST_CHECK(raw_init_same_ref(raw_init_track(&c, "v1"),
                                             raw_init_track(&c, "v2")));
            declared = raw_init_ref_of(&c, "v2");
            moq_msf_catalog_cleanup(&fix.alloc, &c);
        }
    }
    MOQ_TEST_CHECK(declared.ok);

    /* ---- ACT: remove v1, let the real pump tear it down, re-add v1 ---- */
    set_phase(&fix, PH_ACT);
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_remove_track(fix.s, v1), (int)MOQ_OK);
    raw_id_flush(&fix, now, 3);                   /* g2: base + REMOVE delta */
    for (int i = 0; i < 32 && !moq_media_sender_test_track_teardown_done(fix.s, v1); i++)
        cycle(&fix, now);
    MOQ_TEST_CHECK(moq_media_sender_test_track_teardown_done(fix.s, v1));
    /* The public add is what proves the name is free again. */
    MOQ_TEST_CHECK(raw_id_add(&fix, "v1", false, RAW_ID_VIDEO_RECORD_2,
                              sizeof(RAW_ID_VIDEO_RECORD_2), NULL));
    raw_id_flush(&fix, now, 4);                   /* g3: independent */
    if (sticky(&fix, NAME)) { fix_free(&fix); return; }
    {   /* declared: g1's terminal; the removal as base + one REMOVE delta
         * (g2); its terminal; the record-bearing replacement as ONE
         * independent generation (g3) */
        exp_t e[5];
        memset(e, 0, sizeof(e));
        for (int k = 0; k < 5; k++) e[k].sub = fix.cat_sub._opaque;
        e[0].kind = MOQ_EVENT_SUBGROUP_FINISHED; e[0].group = 1; e[0].eog = true;
        e[1].kind = MOQ_EVENT_OBJECT_RECEIVED; e[1].group = 2; e[1].object = 0;
        e[1].eog = true; e[1].has_payload = true;
        e[2].kind = MOQ_EVENT_OBJECT_RECEIVED; e[2].group = 2; e[2].object = 1;
        e[2].eog = true; e[2].has_payload = true;
        e[3].kind = MOQ_EVENT_SUBGROUP_FINISHED; e[3].group = 2; e[3].eog = true;
        e[4].kind = MOQ_EVENT_OBJECT_RECEIVED; e[4].group = 3; e[4].object = 0;
        e[4].eog = true; e[4].has_payload = true;
        expect_phase(&fix, NAME, PH_ACT, e, 5);
    }
    {   /* g3: v2 under its declared identity with the OLD bytes; the new v1
         * under a DIFFERENT identity with the NEW bytes; two entries. */
        moq_msf_catalog_t c;
        if (raw_id_live(&fix, 3, NAME, &c)) {
            MOQ_TEST_CHECK(c.delta_update == NULL);
            MOQ_TEST_CHECK_EQ_U64(c.track_count, 2);
            MOQ_TEST_CHECK_EQ_U64(c.init_data_count, 2);
            MOQ_TEST_CHECK(raw_init_list_is_exactly_used(&c));
            MOQ_TEST_CHECK(raw_init_ref_eq(declared, raw_init_ref_of(&c, "v2")));
            MOQ_TEST_CHECK(!raw_init_same_ref(raw_init_track(&c, "v1"),
                                              raw_init_track(&c, "v2")));
            MOQ_TEST_CHECK_EQ_INT((int)raw_init_resolve(&c, "v2", RAW_ID_VIDEO_RECORD,
                                                        sizeof(RAW_ID_VIDEO_RECORD)),
                                  (int)RAW_INIT_OK);
            MOQ_TEST_CHECK_EQ_INT((int)raw_init_resolve(&c, "v1", RAW_ID_VIDEO_RECORD_2,
                                                        sizeof(RAW_ID_VIDEO_RECORD_2)),
                                  (int)RAW_INIT_OK);
            moq_msf_catalog_cleanup(&fix.alloc, &c);
        }
    }

    /* ---- RECOVER: the retained catalog ------------------------------ */
    set_phase(&fix, PH_RECOVER);
    moq_subscription_t join_sub;
    memset(&join_sub, 0, sizeof(join_sub));
    {
        moq_session_t *srv = moq_simpair_server(fix.sp);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_unsubscribe(srv, fix.cat_sub, now),
                              (int)MOQ_OK);
        for (int i = 0; i < 6; i++) cycle(&fix, now);
        fix.cat_subscribed = false;
        MOQ_TEST_CHECK(subscribe_track_filter(&fix, now, MOQ_MSF_CATALOG_TRACK_NAME,
                                              strlen(MOQ_MSF_CATALOG_TRACK_NAME),
                                              MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT,
                                              &join_sub));
        for (int i = 0; i < 16 && fix.sub_ok_n < 2; i++) cycle(&fix, now);
        MOQ_TEST_CHECK_EQ_INT(fix.sub_ok_n, 2);
        moq_fetch_cfg_t fc; moq_fetch_cfg_init(&fc);
        fc.is_joining = true;
        fc.joining_relative = true;
        fc.joining_sub = join_sub;
        fc.joining_start = 0;
        memset(&fix.fetch, 0, sizeof(fix.fetch));
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(srv, &fc, now, &fix.fetch),
                              (int)MOQ_OK);
        for (int i = 0; i < 24 && fix.fetch_complete_n == 0; i++)
            cycle(&fix, now);
    }
    if (sticky(&fix, NAME)) { fix_free(&fix); return; }
    MOQ_TEST_CHECK_EQ_INT(fix.fetch_ok_n, 1);
    MOQ_TEST_CHECK_EQ_INT(fix.fetch_complete_n, 1);
    MOQ_TEST_CHECK_EQ_INT(fix.fet_n, 1);
    {   /* declared: the released sub's g3 reset; the join's OK, largest
         * (3,0); FETCH_OK end 3 exclusive 1; the one retained object; done */
        exp_t e[5];
        memset(e, 0, sizeof(e));
        e[0].kind = MOQ_EVENT_SUBGROUP_RESET; e[0].sub = fix.cat_sub._opaque;
        e[0].group = 3; e[0].eog = true; e[0].x = 1;
        e[1].kind = MOQ_EVENT_SUBSCRIBE_OK; e[1].sub = join_sub._opaque;
        e[1].x = 2; e[1].has_largest = true; e[1].y = 3; e[1].largest_obj = 0;
        e[2].kind = MOQ_EVENT_FETCH_OK; e[2].fetch = fix.fetch._opaque;
        e[2].y = 3; e[2].largest_obj = 1;
        e[3].kind = MOQ_EVENT_FETCH_OBJECT; e[3].fetch = fix.fetch._opaque;
        e[3].group = 3; e[3].object = 0; e[3].has_payload = true;
        e[4].kind = MOQ_EVENT_FETCH_COMPLETE; e[4].fetch = fix.fetch._opaque;
        expect_phase(&fix, NAME, PH_RECOVER, e, 5);
    }
    MOQ_TEST_CHECK(fix.eff_valid && !fix.eff_fail);
    if (fix.eff_valid && !fix.eff_fail) {
        const moq_msf_catalog_t *c = &fix.eff_cat;
        MOQ_TEST_CHECK_EQ_U64(c->track_count, 2);
        MOQ_TEST_CHECK_EQ_U64(c->init_data_count, 2);
        MOQ_TEST_CHECK(raw_init_list_is_exactly_used(c));
        MOQ_TEST_CHECK(raw_init_ref_eq(declared, raw_init_ref_of(c, "v2")));
        MOQ_TEST_CHECK(!raw_init_same_ref(raw_init_track(c, "v1"), raw_init_track(c, "v2")));
        MOQ_TEST_CHECK_EQ_INT((int)raw_init_resolve(c, "v2", RAW_ID_VIDEO_RECORD,
                                                    sizeof(RAW_ID_VIDEO_RECORD)),
                              (int)RAW_INIT_OK);
        MOQ_TEST_CHECK_EQ_INT((int)raw_init_resolve(c, "v1", RAW_ID_VIDEO_RECORD_2,
                                                    sizeof(RAW_ID_VIDEO_RECORD_2)),
                              (int)RAW_INIT_OK);
    }

    teardown(&fix, NAME, fix.cat_n, fix.med_n, NULL, 0);
    report(NAME, failures - before, 0, false);
    fix_free(&fix);
}
#endif /* MOQ_RAW_INIT_IDENTITY_ROW */

int main(void)
{
    /* Counting allocator: it owns the SimPair, the endpoint and every MSF
     * decode, and the probe allocates nothing, so the balance covers the whole
     * fixture. macOS ASan ships no LeakSanitizer, so balance is proven here
     * rather than assumed. */
    test_alloc_state_t st = { 0 };
    moq_alloc_t alloc = test_allocator(&st);

    arm_complete_one_track(&alloc);
    MOQ_TEST_CHECK_EQ_INT((int)st.balance, 0);
    arm_complete_zero_tracks(&alloc);
    MOQ_TEST_CHECK_EQ_INT((int)st.balance, 0);
    arm_queued_media_object(&alloc);
    MOQ_TEST_CHECK_EQ_INT((int)st.balance, 0);
    arm_queued_end_of_track(&alloc);
    MOQ_TEST_CHECK_EQ_INT((int)st.balance, 0);
    arm_staged_generation_refusal(&alloc);
    MOQ_TEST_CHECK_EQ_INT((int)st.balance, 0);
    control_initial_setup_is_owed(&alloc);
    MOQ_TEST_CHECK_EQ_INT((int)st.balance, 0);
    ready_alone_is_not_pending_control(&alloc);
    MOQ_TEST_CHECK_EQ_INT((int)st.balance, 0);
    control_pending_predicate_states(&alloc);
    MOQ_TEST_CHECK_EQ_INT((int)st.balance, 0);
    control_no_demand_queue_holds_drain(&alloc);
    MOQ_TEST_CHECK_EQ_INT((int)st.balance, 0);
    control_final_recheck_catches_late_work(&alloc);
    MOQ_TEST_CHECK_EQ_INT((int)st.balance, 0);
    arm_attachment_fatal_is_not_drain_success(&alloc);
    MOQ_TEST_CHECK_EQ_INT((int)st.balance, 0);
    timeline_row(&alloc, false);
    MOQ_TEST_CHECK_EQ_INT((int)st.balance, 0);
    timeline_row(&alloc, true);
    MOQ_TEST_CHECK_EQ_INT((int)st.balance, 0);
    arm_dirty_across_recompute(&alloc);
    MOQ_TEST_CHECK_EQ_INT((int)st.balance, 0);
    arm_removed_track_teardown(&alloc);
    MOQ_TEST_CHECK_EQ_INT((int)st.balance, 0);
    arm_pending_reset(&alloc);
    MOQ_TEST_CHECK_EQ_INT((int)st.balance, 0);
    pre_acceptance_row(&alloc, false);
    MOQ_TEST_CHECK_EQ_INT((int)st.balance, 0);
    pre_acceptance_row(&alloc, true);
    MOQ_TEST_CHECK_EQ_INT((int)st.balance, 0);
    initial_build_race_row(&alloc);
    MOQ_TEST_CHECK_EQ_INT((int)st.balance, 0);
    initial_generation_dedup_row(&alloc);
    MOQ_TEST_CHECK_EQ_INT((int)st.balance, 0);
    complete_waits_for_track_ended_row(&alloc);
    MOQ_TEST_CHECK_EQ_INT((int)st.balance, 0);
    complete_waits_for_reset_row(&alloc);
#ifdef MOQ_RAW_INIT_RETAINED_ROW
    raw_init_retained_row(&alloc);
#endif
#ifdef MOQ_RAW_INIT_IDENTITY_ROW
    raw_init_identity_row(&alloc);
    raw_init_reuse_row(&alloc);
#endif
    MOQ_TEST_CHECK_EQ_INT((int)st.balance, 0);
    removal_cleanup_control(&alloc);
    MOQ_TEST_CHECK_EQ_INT((int)st.balance, 0);
    finite_tail_row(&alloc, false);
    MOQ_TEST_CHECK_EQ_INT((int)st.balance, 0);
    finite_tail_row(&alloc, true);
    MOQ_TEST_CHECK_EQ_INT((int)st.balance, 0);
    arm_publish_initiated_catalog(&alloc);
    MOQ_TEST_CHECK_EQ_INT((int)st.balance, 0);
    if (failures) {
        printf("FAILURES: %d\n", failures);
        return 1;
    }
    MOQ_TEST_PASS("media_sender_completion_drain");
    return 0;
}
