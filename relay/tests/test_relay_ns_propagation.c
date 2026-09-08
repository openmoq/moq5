/*
 * Namespace advertisement propagation through the real relay: a K=2 SimPair
 * rig where each client session attaches to its own shard's production
 * binding (moqr_shards_bind) and the cross-shard control plane runs under the
 * deterministic round barrier.
 *
 * The contract under test is the publisher-side MUST of draft-18 Section 6.2
 * ("a relay that has received an authorized PUBLISH_NAMESPACE ... MUST send a
 * NAMESPACE message to any subscriber that has sent SUBSCRIBE_NAMESPACE for
 * that namespace, or a prefix of that namespace") and its draft-16 spelling
 * (same paragraph, forwarding a PUBLISH_NAMESPACE instead of a NAMESPACE).
 * The clause is not conditioned on arrival order, so a namespace subscription
 * that is already live when the announcement arrives is owed the same single
 * advertisement as one that arrives after it.
 *
 * Both orders are exercised on both shard topologies, so a propagation path
 * that only resolves announcements already present at subscribe time is
 * separated from one that also delivers later ones.
 */

#include <moq/relay/moqr_shards.h>

#include <moq/relay/moqr_bind.h>

#include <moq/moq.h>
#include <moq/sim.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../../tests/unit/test_support.h"

/* counting allocator (single-threaded: the deterministic runner) */
typedef struct ca {
    moq_alloc_t vt;
    long        allocs, frees, live;
} ca_t;

static void *ca_a(size_t n, void *c)
{
    ca_t *a = c;
    void *p = malloc(n);
    if (p != NULL) {
        a->allocs++;
        a->live += (long)n;
    }
    return p;
}
static void *ca_r(void *p, size_t o, size_t n, void *c)
{
    ca_t *a = c;
    void *q = realloc(p, n);
    if (q != NULL) {
        a->live += (long)n - (long)o;
    }
    return q;
}
static void ca_f(void *p, size_t n, void *c)
{
    ca_t *a = c;
    if (p != NULL) {
        a->frees++;
        a->live -= (long)n;
    }
    free(p);
}
static void ca_init(ca_t *a)
{
    memset(a, 0, sizeof(*a));
    a->vt.ctx = a;
    a->vt.alloc = ca_a;
    a->vt.realloc = ca_r;
    a->vt.free = ca_f;
}

static moq_bytes_t
B(const char *s)
{
    return (moq_bytes_t){ .data = (const uint8_t *)s, .len = strlen(s) };
}

/* -- the rig ------------------------------------------------------------------ */

#define NP_MAX_CONNS 4

/* Bounds are deadlock guards, not tolerances: every driver loop must reach
 * its terminating condition well inside them, and reaching one is reported as
 * a fixture failure. */
#define NP_STEP_BOUND 64
#define NP_WAKE_BOUND 64

/* An ORDERED record of one shard's steps inside a window, taken from the
 * production return masks. Cardinality is the point: an OR aggregate cannot
 * tell one self-continuation from several, and "several, but finite" is
 * exactly the failure a one-shot continuation must exclude. */
#define NP_WATCH_MAX 16

typedef struct np_watch {
    int      shard;             /* watched shard; -1 disables recording  */
    int      steps;             /* watched-shard steps, in this window   */
    uint64_t mask[NP_WATCH_MAX];/* each step's returned wake set, in order */
    bool     overflow;          /* more steps than the record holds      */
    int      transport_wakes;   /* cycles armed by the watched shard's
                                 * OWN transport (must be 0 in the window) */
    int      push_wakes;        /* times another shard's returned mask
                                 * named the watched shard                */
} np_watch_t;

static void
np_watch_init(np_watch_t *w, int shard)
{
    memset(w, 0, sizeof(*w));
    w->shard = shard;
}

/* Self-bit returns, and any bit the watched shard named that was not itself. */
static int
np_watch_self_returns(const np_watch_t *w)
{
    int n = 0;
    for (int i = 0; i < w->steps && i < NP_WATCH_MAX; i++) {
        if ((w->mask[i] >> (unsigned)w->shard) & 1u) {
            n++;
        }
    }
    return n;
}

static uint64_t
np_watch_foreign_bits(const np_watch_t *w)
{
    uint64_t bits = 0;
    for (int i = 0; i < w->steps && i < NP_WATCH_MAX; i++) {
        bits |= w->mask[i] & ~(1ull << (unsigned)w->shard);
    }
    return bits;
}

typedef struct npconn {
    bool           used;
    uint16_t       shard;
    moq_simpair_t *sp;
    moq_session_t *peer;    /* client side: the test's endpoint */
    moq_session_t *rsess;   /* server side: the relay's session */
} npconn_t;

typedef struct nprig {
    ca_t          *alloc;
    moqr_shards_t *s;
    npconn_t       conns[NP_MAX_CONNS];
    uint64_t       now;
    uint16_t       shards;
    bool           earned;   /* drive shards only from earned wakes */
    int            failures;
    /* A driver failure is NOT a protocol verdict. Once the rig itself has
     * failed — a transport step refused, a shard step refused, a bound
     * exhausted — the case stops scoring `ns_found` and reports the named
     * fixture failure instead, so a broken fixture can never be read as the
     * missing advertisement it is meant to detect. */
    const char    *driver_fail;
} nprig_t;

#define NP_CHECK(rig, expr)                                         \
    do {                                                            \
        if (!(expr)) {                                              \
            printf("FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
            (rig)->failures++;                                      \
        }                                                           \
    } while (0)

/* Latch the FIRST named driver failure; later ones are its consequences. */
static void
np_driver_fail(nprig_t *r, const char *what)
{
    if (r->driver_fail == NULL) {
        r->driver_fail = what;
        printf("FIXTURE FAILURE: %s\n", what);
        r->failures++;
    }
}

#define NP_DRIVER(rig, what, expr)          \
    do {                                    \
        if (!(expr)) {                      \
            np_driver_fail((rig), (what));  \
        }                                   \
    } while (0)

static moqr_result_t
nprig_create_ex(nprig_t *r, ca_t *a, uint16_t shards, bool earned)
{
    memset(r, 0, sizeof(*r));
    r->alloc = a;
    r->now = 1;
    r->shards = shards;
    r->earned = earned;
    moqr_shards_cfg_t cfg;
    moqr_shards_cfg_init_sized(&cfg, sizeof(cfg), &a->vt);
    cfg.shards = shards;
    cfg.admit_remote_demand = true;
    cfg.core_cfg.log_budget.max_groups = 4;
    cfg.core_cfg.log_budget.max_bytes = 1u << 20;
    cfg.core_cfg.linger_us = 500;
    cfg.live_visibility = earned;
    return moqr_shards_create(&cfg, &r->s);
}

static moqr_result_t
nprig_create(nprig_t *r, ca_t *a, uint16_t shards)
{
    return nprig_create_ex(r, a, shards, false);
}

static void
nprig_destroy(nprig_t *r)
{
    moqr_shards_destroy(r->s);
    for (int i = 0; i < NP_MAX_CONNS; i++) {
        if (r->conns[i].used) {
            moq_simpair_destroy(r->conns[i].sp);
            r->conns[i].used = false;
        }
    }
}

static npconn_t *
nprig_connect(nprig_t *r, uint16_t shard, moq_version_t version)
{
    int slot = -1;
    for (int i = 0; i < NP_MAX_CONNS; i++) {
        if (!r->conns[i].used) {
            slot = i;
            break;
        }
    }
    if (slot < 0) {
        return NULL;
    }
    npconn_t *cn = &r->conns[slot];
    memset(cn, 0, sizeof(*cn));
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = &r->alloc->vt;
    cfg.seed = 0xC7A + (uint64_t)slot;
    cfg.version = version;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 1024;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = 1024;
    if (moq_simpair_create(&cfg, &cn->sp) != MOQ_OK) {
        return NULL;
    }
    cn->peer = moq_simpair_client(cn->sp);
    cn->rsess = moq_simpair_server(cn->sp);
    if (moq_simpair_start(cn->sp) != MOQ_OK ||
        moqr_bind_conn_open(moqr_shards_bind(r->s, shard), cn->rsess,
                            version) != MOQR_OK) {
        moq_simpair_destroy(cn->sp);
        return NULL;
    }
    cn->shard = shard;
    cn->used = true;
    return cn;
}

/* One deterministic cycle: transport steps for every connection, then ONE
 * round of the multi-shard runtime (both binds pump inside it, plus the
 * cross-shard phases under the round barrier). Every driver result is
 * checked; nothing is discarded. */
static void
nprig_cycle(nprig_t *r)
{
    if (r->driver_fail != NULL) {
        return;
    }
    r->now += 1000;
    for (int i = 0; i < NP_MAX_CONNS; i++) {
        npconn_t *cn = &r->conns[i];
        if (!cn->used) {
            continue;
        }
        NP_DRIVER(r, "simpair advance refused",
                  moq_simpair_advance_to(cn->sp, r->now) == MOQ_OK);
        size_t steps = 0;
        /* WOULD_BLOCK here means the bound was reached with work still
         * moving — the pair never reached quiescence, which is a fixture
         * failure and not a protocol outcome. */
        NP_DRIVER(r, "simpair did not quiesce within its step bound",
                  moq_simpair_run_until_quiescent(cn->sp, NP_STEP_BOUND,
                                                  &steps) == MOQ_OK);
    }
    NP_DRIVER(r, "shards round step refused",
              moqr_shards_step(r->s, r->now) == MOQR_OK);
}

/* One cycle of the wake-driven runtime: a shard runs only when it EARNS a
 * turn — its own transport actually delivered work toward the relay-side
 * session, or another shard's push named it in the returned wake set.
 * Nothing is stepped speculatively, so a shard that is owed cross-shard state
 * but is never woken simply never runs, exactly as an idle lane thread would
 * not.
 *
 * `w`, when non-NULL, records the watched shard's steps in order, along with
 * why it was reached, so a case can assert the exact cardinality of the
 * continuation a transition requested. */
static void
nprig_cycle_earned_ex(nprig_t *r, np_watch_t *w)
{
    if (r->driver_fail != NULL) {
        return;
    }
    r->now += 1000;
    uint64_t pending = 0;
    for (int i = 0; i < NP_MAX_CONNS; i++) {
        npconn_t *cn = &r->conns[i];
        if (!cn->used) {
            continue;
        }
        NP_DRIVER(r, "simpair advance refused",
                  moq_simpair_advance_to(cn->sp, r->now) == MOQ_OK);
        /* Quiescence by direction: the relay's lane is armed only by work
         * handled TOWARD the relay session, which is the edge a managed
         * adapter turns into a pump. Traffic that only moves toward the
         * client arms nothing. The loop must REACH quiescence inside its
         * bound; exhausting it is a fixture failure, never a verdict. */
        bool quiesced = false;
        for (int st = 0; st < NP_STEP_BOUND; st++) {
            size_t to_srv = 0, to_cli = 0;
            if (moq_simpair_step_directional(cn->sp, &to_srv, &to_cli) !=
                MOQ_OK) {
                np_driver_fail(r, "simpair directional step refused");
                break;
            }
            if (to_srv > 0) {
                if (w != NULL && cn->shard == (uint16_t)w->shard &&
                    ((pending >> cn->shard) & 1u) == 0) {
                    w->transport_wakes++;
                }
                pending |= 1ull << cn->shard;
            }
            if (to_srv == 0 && to_cli == 0) {
                quiesced = true;
                break;
            }
        }
        NP_DRIVER(r, "simpair did not quiesce within its directional bound",
                  quiesced || r->driver_fail != NULL);
    }
    /* Drain the earned wake set. Redundant wakes are legal, so the bound only
     * guards a runaway — but leaving it with work still pending is a fixture
     * failure, so the drain must be ASSERTED empty, never assumed. */
    int guard = 0;
    for (; pending != 0 && guard < NP_WAKE_BOUND; guard++) {
        uint16_t sh = 0;
        while (((pending >> sh) & 1u) == 0) {
            sh++;
        }
        pending &= ~(1ull << sh);
        uint64_t woke = 0;
        NP_DRIVER(r, "shard step refused",
                  moqr_shards_step_shard(r->s, sh, r->now, &woke) == MOQR_OK);
        if (w != NULL && w->shard >= 0) {
            if (sh == (uint16_t)w->shard) {
                if (w->steps < NP_WATCH_MAX) {
                    w->mask[w->steps] = woke;
                } else {
                    w->overflow = true;
                }
                w->steps++;
            } else if ((woke >> (unsigned)w->shard) & 1u) {
                /* Another shard's step named the watched shard: the real
                 * cross-shard push, observed through the production mask. */
                w->push_wakes++;
            }
        }
        pending |= woke;
    }
    NP_DRIVER(r, "earned wake set did not drain within its bound",
              pending == 0);
}

static void
nprig_cycle_earned(nprig_t *r)
{
    nprig_cycle_earned_ex(r, NULL);
}

static void
nprig_pump(nprig_t *r, int cycles)
{
    for (int i = 0; i < cycles; i++) {
        if (r->earned) {
            nprig_cycle_earned(r);
        } else {
            nprig_cycle(r);
        }
    }
}

/* -- peer observation ---------------------------------------------------------- */

#define NP_MAX_SEEN 4

typedef struct nps {
    int      ns_sub_ok;        /* SUBSCRIBE_NAMESPACE acceptances (exactly 1) */
    int      ns_rejected;      /* SUBSCRIBE_NAMESPACE refusals   (exactly 0) */
    int      session_closed;   /* session terminals              (exactly 0) */
    int      unexpected;       /* any other error/terminal event (exactly 0) */
    int      ns_found;
    int      ns_gone;
    /* Every advertised suffix, in arrival order: the field-exactness oracle. */
    int      seen;
    uint32_t seen_count[NP_MAX_SEEN];
    char     seen_parts[NP_MAX_SEEN][2][16];
    uint32_t seen_lens[NP_MAX_SEEN][2];
    uint64_t seen_handle[NP_MAX_SEEN];
    /* Publisher side. */
    int      announce_ok;      /* PUBLISH_NAMESPACE acceptances  (exactly 1) */
    int      announce_errors;  /* PUBLISH_NAMESPACE refusals     (exactly 0) */
} nps_t;

static void
nps_record(nps_t *ps, const moq_namespace_found_event_t *ev)
{
    if (ps->seen >= NP_MAX_SEEN) {
        ps->seen++;
        return;
    }
    int i = ps->seen++;
    ps->seen_handle[i] = ev->handle._opaque;
    uint32_t n = (uint32_t)ev->track_namespace_suffix.count;
    ps->seen_count[i] = n;
    for (uint32_t p = 0; p < n && p < 2; p++) {
        uint32_t len = ev->track_namespace_suffix.parts[p].len;
        if (len > 15) {
            len = 15;
        }
        memcpy(ps->seen_parts[i][p], ev->track_namespace_suffix.parts[p].data,
               len);
        ps->seen_parts[i][p][len] = '\0';
        ps->seen_lens[i][p] = ev->track_namespace_suffix.parts[p].len;
    }
}

static void
nps_drain(nprig_t *r, npconn_t *cn, nps_t *ps)
{
    (void)r;
    moq_event_t evs[16];
    size_t n;
    while ((n = moq_session_poll_events(cn->peer, evs, 16)) > 0) {
        for (size_t e = 0; e < n; e++) {
            moq_event_t *ev = &evs[e];
            switch (ev->kind) {
            case MOQ_EVENT_NS_SUB_OK:
                ps->ns_sub_ok++;
                break;
            case MOQ_EVENT_NS_SUB_ERROR:
                ps->ns_rejected++;
                break;
            case MOQ_EVENT_NAMESPACE_FOUND:
                ps->ns_found++;
                nps_record(ps, &ev->u.namespace_found);
                break;
            case MOQ_EVENT_NAMESPACE_GONE:
                ps->ns_gone++;
                break;
            /* Anything that could explain a missing advertisement by killing
             * the session or the request must be counted, not ignored. */
            case MOQ_EVENT_SESSION_CLOSED:
                ps->session_closed++;
                break;
            case MOQ_EVENT_GOAWAY:
            case MOQ_EVENT_NAMESPACE_CANCELLED:
            case MOQ_EVENT_SUBSCRIBE_ERROR:
                ps->unexpected++;
                break;
            case MOQ_EVENT_NAMESPACE_ACCEPTED:
                ps->announce_ok++;
                break;
            case MOQ_EVENT_NAMESPACE_REJECTED:
                ps->announce_errors++;
                break;
            default:
                break;
            }
            moq_event_cleanup(ev);
        }
    }
}

/* -- client-side operations ----------------------------------------------------- */

static moq_ns_sub_handle_t
np_subscribe_namespace(nprig_t *r, npconn_t *sub, moq_bytes_t *pfx,
                       uint32_t count)
{
    moq_subscribe_namespace_cfg_t nscfg;
    memset(&nscfg, 0, sizeof(nscfg));
    moq_subscribe_namespace_cfg_init(&nscfg);
    nscfg.track_namespace_prefix =
        (moq_namespace_t){ .parts = pfx, .count = count };
    /* Portable across drafts: draft-18's SUBSCRIBE_NAMESPACE carries no
     * interest field and the profile requires NAMESPACE_STATE exactly;
     * draft-16 accepts it too. */
    nscfg.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
    moq_ns_sub_handle_t nsh = { 0 };
    NP_CHECK(r, moq_session_subscribe_namespace(sub->peer, &nscfg, r->now,
                                                &nsh) == MOQ_OK);
    return nsh;
}

static void
np_announce(nprig_t *r, npconn_t *pub, moq_bytes_t *nsp, uint32_t count)
{
    moq_publish_namespace_cfg_t pcfg;
    memset(&pcfg, 0, sizeof(pcfg));
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = count };
    moq_announcement_t ann;
    NP_CHECK(r, moq_session_publish_namespace(pub->peer, &pcfg, r->now,
                                              &ann) == MOQ_OK);
}

/* The single advertised namespace every case uses, and the empty prefix that
 * matches it (the shape a subscriber asking for "everything" sends). */
static const char *const NS_A = "moq-test";
static const char *const NS_B = "interop";

/* Exactly one advertisement, carrying the subscriber's own handle and the
 * full namespace as the suffix beyond an empty prefix. */
static int
np_expect_one(nprig_t *r, nps_t *ps, uint64_t want_handle)
{
    int failures = 0;
    MOQ_TEST_CHECK_EQ_INT(ps->ns_found, 1);
    MOQ_TEST_CHECK_EQ_INT(ps->seen, 1);
    if (ps->seen != 1) {
        r->failures += failures;
        return failures;
    }
    MOQ_TEST_CHECK_EQ_U64(ps->seen_handle[0], want_handle);
    MOQ_TEST_CHECK_EQ_U64(ps->seen_count[0], 2);
    if (ps->seen_count[0] == 2) {
        MOQ_TEST_CHECK_EQ_U64(ps->seen_lens[0][0], strlen(NS_A));
        MOQ_TEST_CHECK_EQ_U64(ps->seen_lens[0][1], strlen(NS_B));
        MOQ_TEST_CHECK(strcmp(ps->seen_parts[0][0], NS_A) == 0);
        MOQ_TEST_CHECK(strcmp(ps->seen_parts[0][1], NS_B) == 0);
    }
    return failures;
}

/* -- the scenarios --------------------------------------------------------------- *
 *
 * `late` selects the ordering: true  = SUBSCRIBE_NAMESPACE first, announcement
 * second (the clause's temporally-unqualified case); false = announcement
 * first (the control that already resolves at subscribe time).
 * `split` places the publisher on a different shard from the subscriber. */
static int
np_case_ex(moq_version_t version, bool late, bool split, uint16_t shards,
           bool earned, const char *label)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    nprig_t r;
    if (nprig_create_ex(&r, &a, shards, earned) != MOQR_OK) {
        printf("FAIL: %s: rig create\n", label);
        return 1;
    }
    uint16_t sub_shard = 0;
    uint16_t pub_shard = split ? (uint16_t)(shards - 1) : (uint16_t)0;

    npconn_t *sub = nprig_connect(&r, sub_shard, version);
    npconn_t *pub = nprig_connect(&r, pub_shard, version);
    NP_CHECK(&r, sub != NULL && pub != NULL);
    if (sub == NULL || pub == NULL) {
        nprig_destroy(&r);
        MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
        return failures + 1;
    }
    nps_t sps, pps;
    memset(&sps, 0, sizeof(sps));
    memset(&pps, 0, sizeof(pps));
    nprig_pump(&r, 4);   /* handshakes */

    moq_bytes_t nsp[2] = { B(NS_A), B(NS_B) };
    moq_ns_sub_handle_t nsh = { 0 };
    /* The mirror-install transition's own wake accounting, for the earned
     * cases: an ordered record of the subscriber shard's steps across the
     * announcement window, and a second one across the quiescent tail. */
    np_watch_t ann, tail;
    np_watch_init(&ann, earned ? (int)sub_shard : -1);
    np_watch_init(&tail, earned ? (int)sub_shard : -1);

    if (late) {
        /* Guard: the namespace subscription is accepted FIRST, exactly once,
         * and nothing has been advertised yet. */
        nsh = np_subscribe_namespace(&r, sub, NULL, 0);
        nprig_pump(&r, 6);
        nps_drain(&r, sub, &sps);
        MOQ_TEST_CHECK_EQ_INT(sps.ns_sub_ok, 1);
        MOQ_TEST_CHECK_EQ_INT(sps.ns_rejected, 0);
        MOQ_TEST_CHECK_EQ_INT(sps.ns_found, 0);

        /* Guard: the announcement is accepted LATER, exactly once. The
         * earned cases record shard 0's wake accounting across it. */
        np_announce(&r, pub, nsp, 2);
        if (earned) {
            for (int c = 0; c < 12; c++) {
                nprig_cycle_earned_ex(&r, &ann);
            }
        } else {
            nprig_pump(&r, 12);
        }
        nps_drain(&r, pub, &pps);
        MOQ_TEST_CHECK_EQ_INT(pps.announce_ok, 1);
        MOQ_TEST_CHECK_EQ_INT(pps.announce_errors, 0);
    } else {
        np_announce(&r, pub, nsp, 2);
        nprig_pump(&r, 12);
        nps_drain(&r, pub, &pps);
        MOQ_TEST_CHECK_EQ_INT(pps.announce_ok, 1);
        MOQ_TEST_CHECK_EQ_INT(pps.announce_errors, 0);

        nsh = np_subscribe_namespace(&r, sub, NULL, 0);
        nprig_pump(&r, 12);
        nps_drain(&r, sub, &sps);
        MOQ_TEST_CHECK_EQ_INT(sps.ns_sub_ok, 1);
        MOQ_TEST_CHECK_EQ_INT(sps.ns_rejected, 0);
    }

    nps_drain(&r, sub, &sps);

    /* Both sessions must still be healthy through the target assertion: a
     * terminal or a cancelled request would explain a missing advertisement
     * without the relay ever having owed one. These are FIXTURE conditions,
     * not verdicts — if any of them fails, the case reports the named fixture
     * failure and scores no protocol result at all, so an unhealthy rig can
     * never be mistaken for the defect it exists to detect. */
    if (sps.session_closed != 0 || pps.session_closed != 0) {
        np_driver_fail(&r, "a session terminated before the verdict");
    } else if (sps.unexpected != 0 || pps.unexpected != 0) {
        np_driver_fail(&r, "an unexpected terminal or error event arrived");
    } else if (sps.ns_sub_ok != 1) {
        np_driver_fail(&r, "the namespace subscription was not accepted "
                           "exactly once");
    } else if (pps.announce_ok != 1 || pps.announce_errors != 0) {
        np_driver_fail(&r, "the announcement was not accepted exactly once");
    }
    if (r.driver_fail != NULL) {
        printf("FAIL: %s: fixture failure (%s) — no protocol verdict\n", label,
               r.driver_fail);
        failures += r.failures;
        nprig_destroy(&r);
        /* The leak oracle runs on the fail-closed path too: a rig that
         * reports a fixture failure must still be a rig that balances. */
        MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
        return failures;
    }

    if (sps.ns_found != 1) {
        printf("  [%s] ns_found=%d ns_gone=%d ns_sub_ok=%d closed=%d\n", label,
               sps.ns_found, sps.ns_gone, sps.ns_sub_ok, sps.session_closed);
    }
    failures += np_expect_one(&r, &sps, nsh._opaque);

    if (earned && late) {
        /* The transition itself, observed through the production return
         * masks as an ORDERED sequence — cardinality, not presence. */
        if (ann.overflow || ann.steps != 2) {
            printf("  [%s] watch: steps=%d transport=%d push=%d overflow=%d\n",
                   label, ann.steps, ann.transport_wakes, ann.push_wakes,
                   (int)ann.overflow);
        }
        /* (1) the subscriber shard was never armed by its own transport in
         *     this window — it had nothing to send or receive; */
        MOQ_TEST_CHECK_EQ_INT(ann.transport_wakes, 0);
        /* (2) another shard's returned mask named it: the real cross-shard
         *     push is the only thing that reached it; */
        MOQ_TEST_CHECK(ann.push_wakes >= 1);
        /* (3) it stepped exactly twice — the mirror install and the drain
         *     that install owed, and nothing more; */
        MOQ_TEST_CHECK(!ann.overflow);
        MOQ_TEST_CHECK_EQ_INT(ann.steps, 2);
        if (ann.steps == 2) {
            /* (4) the install step returned exactly its own bit; */
            MOQ_TEST_CHECK_EQ_U64(ann.mask[0], 1ull << sub_shard);
            /* (5) the owed drain returned exactly nothing. */
            MOQ_TEST_CHECK_EQ_U64(ann.mask[1], 0ull);
        }
        /* (6) exactly ONE self-continuation across the window, and no other
         *     bit was ever returned by this shard. */
        MOQ_TEST_CHECK_EQ_INT(np_watch_self_returns(&ann), 1);
        MOQ_TEST_CHECK_EQ_U64(np_watch_foreign_bits(&ann), 0ull);
    }

    /* Quiescence: further earned rounds carry no duplicate, no withdrawal,
     * and — the anti-spin property — request no further continuation. */
    for (int c = 0; c < 8; c++) {
        if (earned) {
            nprig_cycle_earned_ex(&r, &tail);
        } else {
            nprig_cycle(&r);
        }
    }
    nps_drain(&r, sub, &sps);
    MOQ_TEST_CHECK_EQ_INT(sps.ns_found, 1);
    MOQ_TEST_CHECK_EQ_INT(sps.ns_gone, 0);
    MOQ_TEST_CHECK_EQ_INT(sps.session_closed, 0);
    if (earned && late) {
        /* (7) the tail performs NO subscriber step at all: the continuation
         *     was one-shot, so nothing is left to wake it. */
        MOQ_TEST_CHECK_EQ_INT(tail.steps, 0);
        MOQ_TEST_CHECK_EQ_INT(tail.transport_wakes, 0);
    }
    NP_CHECK(&r, r.driver_fail == NULL);

    failures += r.failures;
    nprig_destroy(&r);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    if (failures == 0) {
        printf("PASS: %s\n", label);
    }
    return failures;
}

static int
np_case(moq_version_t version, bool late, bool split, uint16_t shards,
        const char *label)
{
    return np_case_ex(version, late, split, shards, false, label);
}

/* A prefix that shares no first part with the announced namespace must earn
 * no advertisement at all, on either topology. */
static int
np_case_nonmatching(moq_version_t version, bool split)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    nprig_t r;
    if (nprig_create(&r, &a, 2) != MOQR_OK) {
        printf("FAIL: nonmatching: rig create\n");
        return 1;
    }
    npconn_t *sub = nprig_connect(&r, 0, version);
    npconn_t *pub = nprig_connect(&r, split ? 1 : 0, version);
    NP_CHECK(&r, sub != NULL && pub != NULL);
    if (sub == NULL || pub == NULL) {
        nprig_destroy(&r);
        MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
        return failures + 1;
    }
    nps_t sps;
    memset(&sps, 0, sizeof(sps));
    nprig_pump(&r, 4);

    moq_bytes_t other[1] = { B("elsewhere") };
    (void)np_subscribe_namespace(&r, sub, other, 1);
    nprig_pump(&r, 6);
    nps_drain(&r, sub, &sps);
    MOQ_TEST_CHECK_EQ_INT(sps.ns_sub_ok, 1);
    MOQ_TEST_CHECK_EQ_INT(sps.ns_rejected, 0);

    moq_bytes_t nsp[2] = { B(NS_A), B(NS_B) };
    np_announce(&r, pub, nsp, 2);
    nprig_pump(&r, 12);
    nps_drain(&r, sub, &sps);
    /* Silence must come from the prefix, not from a dead session or a
     * refused request. */
    MOQ_TEST_CHECK_EQ_INT(sps.session_closed, 0);
    MOQ_TEST_CHECK_EQ_INT(sps.unexpected, 0);
    MOQ_TEST_CHECK_EQ_INT(sps.ns_sub_ok, 1);
    NP_CHECK(&r, r.driver_fail == NULL);
    MOQ_TEST_CHECK_EQ_INT(sps.ns_found, 0);

    failures += r.failures;
    nprig_destroy(&r);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    if (failures == 0) {
        printf("PASS: ns_nonmatching_prefix%s\n", split ? "_split" : "");
    }
    return failures;
}

/* How one carrier step is classified. The terminal is carried only by a
 * SUCCESSFUL step that finds nothing left to move: SimPair defines every
 * negative result as an error and defines no result meaning "the closed pair
 * carried it", so a refusal is a driver failure and never a terminal outcome.
 * The real loop below routes every step through this, and
 * np_case_carry_classifier feeds it synthetic results through the same path. */
typedef enum np_carry {
    NP_CARRY_PROGRESS = 0,   /* OK, work moved: keep stepping        */
    NP_CARRY_DONE,           /* OK, nothing left: the carrier is done */
    NP_CARRY_REFUSED         /* non-OK: a driver failure, never done  */
} np_carry_t;

static np_carry_t
np_classify_carry(moq_result_t rc, size_t delivered)
{
    if (rc != MOQ_OK) {
        return NP_CARRY_REFUSED;
    }
    return (delivered == 0) ? NP_CARRY_DONE : NP_CARRY_PROGRESS;
}

/* Every negative SimPair result the API can return must classify as REFUSED,
 * so no error can be mistaken for a carried terminal. Load-bearing on the
 * SAME function the carrier loop calls, not a copy of its logic. */
static int
np_case_carry_classifier(void)
{
    int failures = 0;
    static const moq_result_t errs[] = {
        MOQ_ERR_NOMEM,          MOQ_ERR_INVAL,        MOQ_ERR_PROTO,
        MOQ_ERR_CLOSED,         MOQ_ERR_WRONG_STATE,  MOQ_ERR_STALE_HANDLE,
        MOQ_ERR_WRONG_SESSION,  MOQ_ERR_WOULD_BLOCK,  MOQ_ERR_BUFFER,
        MOQ_ERR_REQUEST_BLOCKED, MOQ_ERR_ABI_MISMATCH, MOQ_ERR_GOAWAY,
        MOQ_ERR_INTERRUPTED,    MOQ_ERR_UNSUPPORTED,
    };
    for (size_t i = 0; i < sizeof(errs) / sizeof(errs[0]); i++) {
        /* Neither delivery count may rescue an error. */
        MOQ_TEST_CHECK(np_classify_carry(errs[i], 0) == NP_CARRY_REFUSED);
        MOQ_TEST_CHECK(np_classify_carry(errs[i], 7) == NP_CARRY_REFUSED);
    }
    /* And success classifies by progress alone. */
    MOQ_TEST_CHECK(np_classify_carry(MOQ_OK, 0) == NP_CARRY_DONE);
    MOQ_TEST_CHECK(np_classify_carry(MOQ_OK, 1) == NP_CARRY_PROGRESS);
    if (failures == 0) {
        printf("PASS: ns_carry_classifier\n");
    }
    return failures;
}

/* The session-terminal guard must be live, not dead accounting: close the
 * subscriber's session and require the counter the other cases assert to be
 * zero to actually observe it. Paired with the fixture-failure path, this is
 * what makes "no session terminated before the verdict" a real guard. */
static int
np_case_terminal_observed(moq_version_t version)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    nprig_t r;
    if (nprig_create(&r, &a, 1) != MOQR_OK) {
        printf("FAIL: terminal_observed: rig create\n");
        return 1;
    }
    npconn_t *sub = nprig_connect(&r, 0, version);
    NP_CHECK(&r, sub != NULL);
    if (sub == NULL) {
        nprig_destroy(&r);
        MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
        return failures + 1;
    }
    nps_t sps;
    memset(&sps, 0, sizeof(sps));
    nprig_pump(&r, 4);
    nps_drain(&r, sub, &sps);
    MOQ_TEST_CHECK_EQ_INT(sps.session_closed, 0);

    /* The relay side closes; the client observes the terminal. The carrier
     * loop is held to the same discipline as the rest of the rig: every
     * result checked, and the terminating condition asserted rather than
     * fallen out of. */
    NP_DRIVER(&r, "relay-side close refused",
              moq_session_close(sub->rsess, 0, "test", r.now) == MOQ_OK);
    bool carried = false;
    for (int st = 0; st < NP_STEP_BOUND; st++) {
        size_t d = 0;
        /* Sequenced deliberately: the step writes `d`, so reading it in the
         * same call's argument list would be unsequenced against that write
         * and a conforming compiler could classify a progress step as DONE. */
        moq_result_t rc = moq_simpair_step(sub->sp, &d);
        np_carry_t k = np_classify_carry(rc, d);
        if (k == NP_CARRY_REFUSED) {
            np_driver_fail(&r, "terminal carrier step refused");
            break;
        }
        if (k == NP_CARRY_DONE) {
            carried = true;
            break;
        }
    }
    NP_DRIVER(&r, "terminal did not carry within the step bound", carried);
    nps_drain(&r, sub, &sps);
    NP_CHECK(&r, r.driver_fail == NULL);
    MOQ_TEST_CHECK_EQ_INT(sps.session_closed, 1);

    failures += r.failures;
    nprig_destroy(&r);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    if (failures == 0) {
        printf("PASS: ns_session_terminal_observed\n");
    }
    return failures;
}

int
main(void)
{
    int failures = 0;

    /* Same shard, announcement already present at subscribe time. */
    failures += np_case(MOQ_VERSION_DRAFT_18, false, false, 1,
                        "ns_existing_same_shard_d18");
    failures += np_case(MOQ_VERSION_DRAFT_16, false, false, 1,
                        "ns_existing_same_shard_d16");
    /* Same shard, announcement arrives after the subscription. */
    failures += np_case(MOQ_VERSION_DRAFT_18, true, false, 1,
                        "ns_late_same_shard_d18");
    failures += np_case(MOQ_VERSION_DRAFT_16, true, false, 1,
                        "ns_late_same_shard_d16");
    /* Distinct shards, announcement already present at subscribe time. */
    failures += np_case(MOQ_VERSION_DRAFT_18, false, true, 2,
                        "ns_existing_cross_shard_d18");
    failures += np_case(MOQ_VERSION_DRAFT_16, false, true, 2,
                        "ns_existing_cross_shard_d16");
    /* Distinct shards, announcement arrives after the subscription: the
     * shape the interop subscriber exercises. */
    failures += np_case(MOQ_VERSION_DRAFT_18, true, true, 2,
                        "ns_late_cross_shard_d18");
    failures += np_case(MOQ_VERSION_DRAFT_16, true, true, 2,
                        "ns_late_cross_shard_d16");

    /* Distinct shards, late announcement, and a runtime that runs a shard
     * only on earned work — the shape where the subscriber's own lane has
     * nothing else to wake it. */
    failures += np_case_ex(MOQ_VERSION_DRAFT_18, true, true, 2, true,
                           "ns_late_cross_shard_earned_d18");
    failures += np_case_ex(MOQ_VERSION_DRAFT_16, true, true, 2, true,
                           "ns_late_cross_shard_earned_d16");
    failures += np_case_ex(MOQ_VERSION_DRAFT_18, false, true, 2, true,
                           "ns_existing_cross_shard_earned_d18");

    failures += np_case_carry_classifier();
    failures += np_case_terminal_observed(MOQ_VERSION_DRAFT_18);

    failures += np_case_nonmatching(MOQ_VERSION_DRAFT_18, false);
    failures += np_case_nonmatching(MOQ_VERSION_DRAFT_18, true);

    if (failures != 0) {
        printf("FAILED: %d\n", failures);
        return 1;
    }
    printf("OK\n");
    return 0;
}
