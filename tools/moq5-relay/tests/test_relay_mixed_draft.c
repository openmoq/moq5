/*
 * Mixed-draft forwarding, decoded off the downstream wire.
 *
 * One listener accepts draft-16 and draft-18 at once and routes between those
 * connections, and the two drafts disagree inside the same registry. A terminal
 * forwarded verbatim across that boundary therefore states something the sender
 * never said: PUBLISH_DONE trades EXPIRED and TOO_FAR_BEHIND between 0x5 and
 * 0x6, and the data-stream reset registry moves UNKNOWN_OBJECT_STATUS while
 * adding codes draft-16 never assigned.
 *
 * Every expectation here is the byte the downstream peer actually decoded from
 * its own session, not a value read back out of the relay.
 */

#include <moq/relay/moqr_bind.h>
#ifdef MOQR_BIND_TESTING
#include <moq/relay/moqr_bind_test.h>
#endif
#include "../cli/conn_reap.h"
#include <moq/relay/moqr_shards.h>

#include <moq/msquic_managed.h>
#include <moq/rcbuf.h>
#include <moq/session.h>

#include "support/fake_msq_managed.h"
#include "support/msq_test_seams.h"

#include "seedx_shuttle.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* The production single-lane pump and its serve context, built by the shared
 * test TU so this rig drives the real relay callback rather than a copy. */
extern void *moqr_test_mk_serve_ctx(moqr_bind_t *bind, moqr_core_t *core,
                                    moqr_trace_t *trace);
extern moq_msquic_lane_pump_fn moqr_test_single_pump(void);

static int g_failures;

#define T_CHECK(expr)                                                     \
    do {                                                                  \
        if (!(expr)) {                                                    \
            printf("FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr);       \
            g_failures++;                                                 \
        }                                                                 \
    } while (0)

#define D16 MOQ_VERSION_DRAFT_16
#define D18 MOQ_VERSION_DRAFT_18

/* Stream-through forwarding (the production CLI configuration): the reset arms
 * need a downstream object that is already BEGUN when the upstream stream dies,
 * because a never-begun object owes no downstream terminal. */
static bool g_rig_streaming = false;

static const uint8_t NS0[] = "live";
static const uint8_t NS1[] = "alpha";

typedef struct rig {
    fake_mgd_t                 fake;
    moqr_core_t               *core;
    moqr_bind_t               *bind;
    moqr_trace_t              *trace;
    void                      *ctx;
    moq_msquic_managed_t      *m;
    moq_msquic_managed_lane_t *lane;
    uint64_t                   now;
} rig_t;

static const char *
alpn_of(moq_version_t v)
{
    return v == D16 ? "moqt-16" : "moqt-18";
}

/* A listener offering BOTH drafts: each accepted connection negotiates its own
 * version from its ALPN, which is what makes a mixed topology real rather than
 * simulated. */
static bool
mixed_rig_up_auth(rig_t *r, moqr_authorize_fn authorize)
{
    memset(r, 0, sizeof(*r));
    fake_mgd_init(&r->fake);
    moq_msq_test_api_override = fake_mgd_table(&r->fake);
    moq_msq_test_no_doorbell = true;

    moqr_core_relay_cfg_t core_cfg;

    moqr_core_relay_cfg_init_sized(&core_cfg, sizeof(core_cfg),
                                   moq_alloc_default());
    if (moqr_trace_create(moq_alloc_default(), 256, &r->trace) != MOQR_OK) {
        return false;
    }
    core_cfg.trace = r->trace;
    core_cfg.authorize = authorize;
    if (moqr_core_create(&core_cfg, &r->core) != MOQR_OK) {
        return false;
    }
    moqr_bind_cfg_t bcfg;

    moqr_bind_cfg_init_sized(&bcfg, sizeof(bcfg), moq_alloc_default());
    bcfg.core = r->core;
    if (moqr_bind_create(&bcfg, &r->bind) != MOQR_OK) {
        return false;
    }
    r->ctx = moqr_test_mk_serve_ctx(r->bind, r->core, r->trace);

    static moq_version_t vers[2];
    vers[0] = D16;
    vers[1] = D18;

    moq_msquic_managed_cfg_t scfg;

    moq_msquic_managed_cfg_init_sized(&scfg, sizeof(scfg));
    scfg.alloc = moq_alloc_default();
    scfg.perspective = MOQ_PERSPECTIVE_SERVER;
    scfg.host = NULL;
    scfg.port = 0;
    scfg.cert_path = "unused-by-the-fake-cert.pem";
    scfg.key_path = "unused-by-the-fake-key.pem";
    scfg.send_request_capacity = true;
    scfg.initial_request_capacity = 1024;
    scfg.version = 0;              /* no exact version: the list decides */
    scfg.streaming_objects = g_rig_streaming;
    scfg.versions = vers;
    scfg.version_count = 2;
    scfg.lane_count = 1;
    scfg.max_connections = 4;
    scfg.on_lane_pump = moqr_test_single_pump();
    scfg.on_lane_pump_user = r->ctx;
    if (moq_msquic_managed_create(&scfg, &r->m) != MOQ_OK) {
        return false;
    }
    r->lane = moq_msquic_managed_lane(r->m, 0);
    return r->lane != NULL;
}

static bool
mixed_rig_up(rig_t *r)
{
    return mixed_rig_up_auth(r, NULL);
}

static void
rig_down(rig_t *r)
{
    if (r->m != NULL) {
        (void)moq_msquic_managed_stop(r->m);
        moq_msquic_managed_destroy(r->m);
    }
    free(r->ctx);
    if (r->bind != NULL) {
        moqr_bind_destroy(r->bind);
    }
    if (r->core != NULL) {
        moqr_core_destroy(r->core);
    }
    if (r->trace != NULL) {
        moqr_trace_destroy(r->trace);
    }
    moq_msq_test_api_override = NULL;
    moq_msq_test_no_doorbell = false;
}

static void
pump_rounds(rig_t *r, shx_driver_t **ds, int nds, int max_rounds)
{
    for (int round = 0; round < max_rounds; round++) {
        bool moved = false;

        for (int i = 0; i < nds; i++) {
            moved |= shx_round(ds[i]);
        }
        r->now += 1000;
        (void)moq_msquic_lane_wake(r->lane);
        moq_msq_test_step_t did = moq_msq_test_lane_step(r->lane);

        if (did == MOQ_MSQ_TEST_STEP_PUMPED ||
            did == MOQ_MSQ_TEST_STEP_TICKED) {
            moved = true;
        }
        for (int i = 0; i < nds; i++) {
            moved |= shx_round(ds[i]);
        }
        if (!moved && round > 0) {
            return;
        }
    }
}

static bool
establish(rig_t *r, shx_driver_t *d)
{
    shx_driver_t *ds[1] = { d };

    for (int i = 0; i < 16; i++) {
        pump_rounds(r, ds, 1, 4);
        if (shx_ev_count(d, MOQ_EVENT_SETUP_COMPLETE) >= 1) {
            return true;
        }
    }
    return false;
}

/* Drive the publisher's side of the relay's upstream subscription. */
static void
pub_accept_and_open(shx_driver_t *pub, moq_subgroup_handle_t *sgh,
                    bool *sg_open)
{
    if (!pub->got_subscribe) {
        return;
    }
    pub->got_subscribe = false;

    moq_accept_subscribe_cfg_t acfg;

    moq_accept_subscribe_cfg_init(&acfg);
    pub->now += 1000;
    (void)moq_session_accept_subscribe(pub->sess, pub->subscribe_handle, &acfg,
                                       pub->now);

    moq_subgroup_cfg_t sgc;

    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = 1;
    sgc.publisher_priority = 100;
    pub->now += 1000;
    if (moq_session_open_subgroup(pub->sess, pub->subscribe_handle, &sgc,
                                  pub->now, sgh) == MOQ_OK) {
        *sg_open = true;
    }
}

static bool
pub_push(shx_driver_t *pub, moq_subgroup_handle_t sgh, uint64_t oid)
{
    uint8_t body[16];
    moq_rcbuf_t *pl = NULL;

    memset(body, (int)(0x50 + oid), sizeof(body));
    if (moq_rcbuf_create(moq_alloc_default(), body, sizeof(body), &pl) < 0) {
        return false;
    }
    pub->now += 1000;
    moq_result_t rc = moq_session_write_object(pub->sess, sgh, oid, pl,
                                               pub->now);

    moq_rcbuf_decref(pl);
    return rc == MOQ_OK;
}

/*
 * One publisher on `pub_ver`, one subscriber on `sub_ver`, on the SAME
 * listener. The publisher ends its upstream subscription with `up_status` in
 * its own registry; the subscriber must decode `want_status` in the other.
 */
static int
run_pd_forward_held(const char *label, moq_version_t pub_ver,
                    moq_version_t sub_ver, uint64_t up_status,
                    uint64_t want_status, int hold_writes)
{
    int before = g_failures;
    rig_t r;
    shx_driver_t pub, sub;

    T_CHECK(mixed_rig_up(&r));
    if (r.m == NULL) {
        rig_down(&r);
        return g_failures - before;
    }
    T_CHECK(shx_driver_open(&pub, &r.fake, pub_ver, alpn_of(pub_ver)));
    T_CHECK(shx_driver_open(&sub, &r.fake, sub_ver, alpn_of(sub_ver)));
    T_CHECK(establish(&r, &pub));
    T_CHECK(establish(&r, &sub));

    /* Each connection really did negotiate its own draft. */
    T_CHECK(moqr_bind_conn_version(r.bind, NULL) == 0);

    shx_driver_t *ds[2] = { &pub, &sub };
    moq_bytes_t nsp[2] = { { NS0, sizeof(NS0) - 1 },
                           { NS1, sizeof(NS1) - 1 } };
    moq_publish_namespace_cfg_t pcfg;

    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };

    moq_announcement_t ann;

    T_CHECK(moq_session_publish_namespace(pub.sess, &pcfg, pub.now, &ann) ==
            MOQ_OK);
    pump_rounds(&r, ds, 2, 8);

    moq_subscribe_cfg_t sc;

    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    sc.track_name = (moq_bytes_t){ (const uint8_t *)"t", 1 };
    sc.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;

    moq_subscription_t sh;

    T_CHECK(moq_session_subscribe(sub.sess, &sc, sub.now, &sh) == MOQ_OK);

    moq_subgroup_handle_t sgh;
    bool sg_open = false;

    for (int i = 0; i < 24 && !sg_open; i++) {
        pump_rounds(&r, ds, 2, 2);
        pub_accept_and_open(&pub, &sgh, &sg_open);
    }
    T_CHECK(sg_open);
    T_CHECK(pub_push(&pub, sgh, 0));
    pump_rounds(&r, ds, 2, 10);
    T_CHECK(shx_ev_count(&sub, MOQ_EVENT_OBJECT_RECEIVED) == 1);

    /* Close the subgroup first: a subscription with an open subgroup refuses
     * DONE (WRONG_STATE), exactly as a real publisher must finish its streams
     * before ending the subscription. */
    pub.now += 1000;
    T_CHECK(moq_session_close_subgroup(pub.sess, sgh, pub.now) == MOQ_OK);
    pump_rounds(&r, ds, 2, 8);

    /* Hold the downstream terminal write across `hold_writes` pumps. The
     * descriptor was captured with the UPSTREAM generation before the hold, so
     * a retry that re-derived the origin from the emitting connection would
     * translate against the wrong registry and decode a different byte. */
    if (hold_writes > 0) {
        moqr_bind_debug_block_sub_done(hold_writes);
    }

    /* The publisher ends the upstream subscription with its own draft's
     * number for the meaning it intends. */
    moq_done_subscribe_cfg_t dcfg;

    moq_done_subscribe_cfg_init(&dcfg);
    dcfg.status_code = up_status;
    pub.now += 1000;
    T_CHECK(moq_session_done_subscribe(pub.sess, pub.subscribe_handle, &dcfg,
                                       pub.now) == MOQ_OK);
    pump_rounds(&r, ds, 2, 16);

    if (hold_writes > 0) {
        /* The injected blocks were all consumed, so the terminal that landed
         * came from a retry and not from the first attempt. */
        T_CHECK(moqr_bind_debug_sub_done_blocks_left() == 0);
        moqr_bind_debug_block_sub_done(0);
    }
    T_CHECK(shx_ev_count(&sub, MOQ_EVENT_SUBSCRIBE_DONE) == 1);
    printf("ORACLE %s up=0x%llx decoded=0x%llx want=0x%llx\n", label,
           (unsigned long long)up_status,
           (unsigned long long)sub.last_done_status,
           (unsigned long long)want_status);
    T_CHECK(sub.done_status_count == 1);
    T_CHECK(sub.last_done_status == want_status);

    /* exactly once */
    pump_rounds(&r, ds, 2, 8);
    T_CHECK(shx_ev_count(&sub, MOQ_EVENT_SUBSCRIBE_DONE) == 1);

    shx_driver_close(&pub);
    shx_driver_close(&sub);
    rig_down(&r);
    if (g_failures == before) {
        printf("PASS: %s\n", label);
    }
    return g_failures - before;
}

static int
run_pd_forward(const char *label, moq_version_t pub_ver, moq_version_t sub_ver,
               uint64_t up_status, uint64_t want_status)
{
    return run_pd_forward_held(label, pub_ver, sub_ver, up_status, want_status,
                               0);
}

/*
 * The upstream RESETs a begun subgroup with `up_code` in its own registry; the
 * downstream subscriber on the other draft must decode `want_code` in its.
 */
static int
run_reset_forward(const char *label, moq_version_t pub_ver,
                  moq_version_t sub_ver, uint64_t up_code, uint64_t want_code)
{
    int before = g_failures;
    rig_t r;
    shx_driver_t pub, sub;

    g_rig_streaming = true;
    T_CHECK(mixed_rig_up(&r));
    g_rig_streaming = false;
    if (r.m == NULL) {
        rig_down(&r);
        return g_failures - before;
    }
    T_CHECK(shx_driver_open(&pub, &r.fake, pub_ver, alpn_of(pub_ver)));
    T_CHECK(shx_driver_open(&sub, &r.fake, sub_ver, alpn_of(sub_ver)));
    T_CHECK(establish(&r, &pub));
    T_CHECK(establish(&r, &sub));

    shx_driver_t *ds[2] = { &pub, &sub };
    moq_bytes_t nsp[2] = { { NS0, sizeof(NS0) - 1 },
                           { NS1, sizeof(NS1) - 1 } };
    moq_publish_namespace_cfg_t pcfg;

    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };

    moq_announcement_t ann;

    T_CHECK(moq_session_publish_namespace(pub.sess, &pcfg, pub.now, &ann) ==
            MOQ_OK);
    pump_rounds(&r, ds, 2, 8);

    moq_subscribe_cfg_t sc;

    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    sc.track_name = (moq_bytes_t){ (const uint8_t *)"t", 1 };
    sc.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;

    moq_subscription_t sh;

    T_CHECK(moq_session_subscribe(sub.sess, &sc, sub.now, &sh) == MOQ_OK);

    moq_subgroup_handle_t sgh;
    bool sg_open = false;

    for (int i = 0; i < 24 && !sg_open; i++) {
        pump_rounds(&r, ds, 2, 2);
        pub_accept_and_open(&pub, &sgh, &sg_open);
    }
    T_CHECK(sg_open);
    T_CHECK(pub_push(&pub, sgh, 0));
    pump_rounds(&r, ds, 2, 10);
    T_CHECK(shx_ev_count(&sub, MOQ_EVENT_OBJECT_RECEIVED) == 1);

    /* Begin object 1 and send half of it: stream-through forwarding has now
     * BEGUN the downstream object, so the reset below owes a downstream
     * terminal. */
    pub.now += 1000;
    T_CHECK(moq_session_begin_object(pub.sess, sgh, 1, 32, pub.now) == MOQ_OK);

    uint8_t half[16];
    moq_rcbuf_t *pl = NULL;

    memset(half, 0x61, sizeof(half));
    T_CHECK(moq_rcbuf_create(moq_alloc_default(), half, sizeof(half), &pl) >= 0);
    pub.now += 1000;
    T_CHECK(moq_session_write_object_data(pub.sess, sgh, pl, pub.now) ==
            MOQ_OK);
    moq_rcbuf_decref(pl);
    pump_rounds(&r, ds, 2, 10);

    /* The upstream stream dies abnormally with its own draft's number. */
    pub.now += 1000;
    T_CHECK(moq_session_reset_subgroup(pub.sess, sgh, up_code, pub.now) ==
            MOQ_OK);
    pump_rounds(&r, ds, 2, 16);

    printf("ORACLE %s up=0x%llx decoded=0x%llx want=0x%llx count=%d\n", label,
           (unsigned long long)up_code,
           (unsigned long long)sub.last_sg_reset_code,
           (unsigned long long)want_code, sub.sg_reset_count);
    T_CHECK(sub.sg_reset_count == 1);
    T_CHECK(sub.last_sg_reset_code == want_code);

    shx_driver_close(&pub);
    shx_driver_close(&sub);
    rig_down(&r);
    if (g_failures == before) {
        printf("PASS: %s\n", label);
    }
    return g_failures - before;
}

/* -- relay-authored terminals ---------------------------------------------- *
 *
 * Two different wire domains, deliberately. An INITIAL subscribe denial is a
 * REQUEST_ERROR — draft-16 Section 9.8 / draft-18 Section 10.4 — and must never
 * manufacture a PUBLISH_DONE. A grant that was ALLOWED and later revoked on
 * revalidation ends an ACTIVE subscription, which is a PUBLISH_DONE
 * (draft-16 Section 13.4.3 / draft-18 Section 15.10.3). Only the second may
 * carry a relay-authored extension number. */

static moqr_auth_decision_t g_reval_decision = MOQR_AUTH_ALLOW;
static uint64_t             g_reval_lease = 0;
static uint64_t             g_deny_code = 0;
/* set the REQUEST_ERROR scalar but tag NO terminal: the core must not
 * launder the number into a PUBLISH_DONE extension */
static uint64_t             g_request_error_only = 0;

static void
reval_hook(void *ctx, const moqr_auth_request_t *req,
           moqr_auth_verdict_t *out)
{
    (void)req;
    (void)ctx;
    memset(out, 0, sizeof(*out));
    if (req->action != MOQR_AUTH_SUBSCRIBE) {
        /* Only the SUBSCRIBE grant is under test. Denying the announce grant
         * instead would revoke the namespace and end the track with
         * force_withdraw's TRACK_ENDED — a different path in a different
         * domain, which is exactly what this arm must not measure. */
        out->decision = MOQR_AUTH_ALLOW;
        return;
    }
    out->decision = g_reval_decision;
    out->revalidate_after_us = g_reval_lease;
    /* The request-level denial code and the revocation TERMINAL are different
     * domains. The terminal is stated explicitly as a tagged descriptor; the
     * scalar below stays REQUEST_ERROR-domain and is never read as a status. */
    out->error_code = g_request_error_only != 0 ? g_request_error_only
                                                : MOQR_AUTH_REASON_POLICY;
    out->reason = MOQR_AUTH_REASON_POLICY;
    if (g_deny_code != 0) {
        moqr_pd_desc_t t;

        if (moqr_pd_desc_extension(g_deny_code, &t) == MOQR_OK) {
            out->revoke_terminal = t;
        }
        /* a refused extension leaves the terminal NONE: the core falls back to
         * the truthful UNAUTHORIZED */
    }
}

/* Bring a publisher and a subscriber up on the given drafts and leave an
 * ACTIVE subscription in place. */
static bool
ext_stage(rig_t *r, shx_driver_t *pub, shx_driver_t *sub, moq_version_t sub_ver,
          moq_subscription_t *sh)
{
    if (!shx_driver_open(pub, &r->fake, D16, alpn_of(D16)) ||
        !shx_driver_open(sub, &r->fake, sub_ver, alpn_of(sub_ver))) {
        return false;
    }
    if (!establish(r, pub) || !establish(r, sub)) {
        return false;
    }
    shx_driver_t *ds[2] = { pub, sub };
    moq_bytes_t nsp[2] = { { NS0, sizeof(NS0) - 1 },
                           { NS1, sizeof(NS1) - 1 } };
    moq_publish_namespace_cfg_t pcfg;

    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };

    moq_announcement_t ann;

    if (moq_session_publish_namespace(pub->sess, &pcfg, pub->now, &ann) !=
        MOQ_OK) {
        return false;
    }
    pump_rounds(r, ds, 2, 8);

    moq_subscribe_cfg_t sc;

    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    sc.track_name = (moq_bytes_t){ (const uint8_t *)"t", 1 };
    sc.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    if (moq_session_subscribe(sub->sess, &sc, sub->now, sh) != MOQ_OK) {
        return false;
    }
    moq_subgroup_handle_t sgh;
    bool sg_open = false;

    for (int i = 0; i < 24 && !sg_open; i++) {
        pump_rounds(r, ds, 2, 2);
        pub_accept_and_open(pub, &sgh, &sg_open);
    }
    if (!sg_open) {
        return false;
    }
    (void)pub_push(pub, sgh, 0);
    pump_rounds(r, ds, 2, 10);
    return shx_ev_count(sub, MOQ_EVENT_OBJECT_RECEIVED) == 1;
}

/* NEGATIVE CONTROL: an initial denial is a REQUEST_ERROR, so no PUBLISH_DONE
 * is manufactured for it at all. */
static int
run_initial_deny_control(const char *label, moq_version_t sub_ver)
{
    int before = g_failures;
    rig_t r;
    shx_driver_t pub, sub;

    g_reval_decision = MOQR_AUTH_DENY;
    g_reval_lease = 0;
    g_deny_code = 0x7u;
    T_CHECK(mixed_rig_up_auth(&r, reval_hook));
    if (r.m == NULL) {
        rig_down(&r);
        return g_failures - before;
    }
    moq_subscription_t sh;

    (void)ext_stage(&r, &pub, &sub, sub_ver, &sh);
    shx_driver_t *ds[2] = { &pub, &sub };

    pump_rounds(&r, ds, 2, 24);
    printf("ORACLE %s done_count=%d err_count=%d\n", label,
           sub.done_status_count,
           shx_ev_count(&sub, MOQ_EVENT_SUBSCRIBE_ERROR));
    /* The denial rode the REQUEST_ERROR domain: no status was invented. */
    T_CHECK(sub.done_status_count == 0);
    T_CHECK(shx_ev_count(&sub, MOQ_EVENT_SUBSCRIBE_ERROR) >= 1);

    shx_driver_close(&pub);
    shx_driver_close(&sub);
    rig_down(&r);
    g_reval_decision = MOQR_AUTH_ALLOW;
    g_deny_code = 0;
    if (g_failures == before) {
        printf("PASS: %s\n", label);
    }
    return g_failures - before;
}

/* ALLOW with a lease, then a due revalidation that DENIES: the ACTIVE
 * subscription is revoked with a relay-authored PUBLISH_DONE. */
static int
run_reval_extension(const char *label, moq_version_t sub_ver,
                    uint64_t deny_code, uint64_t want_status)
{
    int before = g_failures;
    rig_t r;
    shx_driver_t pub, sub;

    g_reval_decision = MOQR_AUTH_ALLOW;
    /* An immediately-due lease: the managed pump reads the monotonic clock
     * directly (there is no clock seam in the adapter), so the recheck is made
     * due by construction rather than by waiting. Rechecks simply re-ALLOW
     * until the decision flips. */
    g_reval_lease = 1;
    g_deny_code = 0;
    T_CHECK(mixed_rig_up_auth(&r, reval_hook));
    if (r.m == NULL) {
        rig_down(&r);
        return g_failures - before;
    }
    moq_subscription_t sh;

    T_CHECK(ext_stage(&r, &pub, &sub, sub_ver, &sh));
    shx_driver_t *ds[2] = { &pub, &sub };

    /* The lease comes due and the recheck denies with the policy code. */
    g_reval_decision = MOQR_AUTH_DENY;
    g_deny_code = deny_code;
    for (int i = 0; i < 60 && sub.done_status_count == 0; i++) {
        pump_rounds(&r, ds, 2, 4);
    }

    printf("ORACLE %s deny=0x%llx decoded=0x%llx want=0x%llx count=%d\n",
           label, (unsigned long long)deny_code,
           (unsigned long long)sub.last_done_status,
           (unsigned long long)want_status, sub.done_status_count);
    T_CHECK(sub.done_status_count == 1);
    T_CHECK(sub.last_done_status == want_status);

    shx_driver_close(&pub);
    shx_driver_close(&sub);
    rig_down(&r);
    g_reval_decision = MOQR_AUTH_ALLOW;
    g_reval_lease = 0;
    g_deny_code = 0;
    if (g_failures == before) {
        printf("PASS: %s\n", label);
    }
    return g_failures - before;
}

static const uint8_t NS2[] = "beta";

/* A block count no run can exhaust: the hold ends only when the test clears
 * it, so a terminal owed across another event really is still owed. */
#define HOLD_UNTIL_RELEASED (1 << 20)

/* Announce `ns`, subscribe to its one track, and move a single object, leaving
 * the publisher's subgroup open. Returns false if any leg fails to complete. */
static bool
stage_track(rig_t *r, shx_driver_t **ds, int nds, shx_driver_t *pub,
            shx_driver_t *sub, moq_bytes_t *nsp, moq_subgroup_handle_t *sgh)
{
    moq_publish_namespace_cfg_t pcfg;

    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };

    moq_announcement_t ann;

    if (moq_session_publish_namespace(pub->sess, &pcfg, pub->now, &ann) !=
        MOQ_OK) {
        return false;
    }
    pump_rounds(r, ds, nds, 8);

    moq_subscribe_cfg_t sc;

    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    sc.track_name = (moq_bytes_t){ (const uint8_t *)"t", 1 };
    sc.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;

    moq_subscription_t sh;

    if (moq_session_subscribe(sub->sess, &sc, sub->now, &sh) != MOQ_OK) {
        return false;
    }
    bool sg_open = false;

    for (int i = 0; i < 24 && !sg_open; i++) {
        pump_rounds(r, ds, nds, 2);
        pub_accept_and_open(pub, sgh, &sg_open);
    }
    if (!sg_open || !pub_push(pub, *sgh, 0)) {
        return false;
    }
    pump_rounds(r, ds, nds, 10);
    return shx_ev_count(sub, MOQ_EVENT_OBJECT_RECEIVED) == 1;
}

/* End the upstream subscription with `up_status`, closing the subgroup first
 * (a subscription with an open subgroup refuses DONE). */
static void
end_upstream(rig_t *r, shx_driver_t **ds, int nds, shx_driver_t *pub,
             moq_subgroup_handle_t sgh, uint64_t up_status)
{
    pub->now += 1000;
    T_CHECK(moq_session_close_subgroup(pub->sess, sgh, pub->now) == MOQ_OK);
    pump_rounds(r, ds, nds, 8);

    moq_done_subscribe_cfg_t dcfg;

    moq_done_subscribe_cfg_init(&dcfg);
    dcfg.status_code = up_status;
    pub->now += 1000;
    T_CHECK(moq_session_done_subscribe(pub->sess, pub->subscribe_handle, &dcfg,
                                       pub->now) == MOQ_OK);
    pump_rounds(r, ds, nds, 12);
}

/*
 * REPLACEMENT: the terminal is retained while a DIFFERENT-draft publisher
 * takes the namespace over, opening a new upstream generation. The retained
 * descriptor carries the origin captured with the generation that produced it,
 * so the byte that lands must still be translated from `ver_a` — never from
 * the replacement's draft, and never from the emitting connection's.
 */
static int
run_pd_replacement(const char *label, moq_version_t ver_a, moq_version_t ver_b,
                   moq_version_t sub_ver, uint64_t up_status,
                   uint64_t want_status)
{
    int before = g_failures;
    rig_t r;
    shx_driver_t pub_a, pub_b, sub;

    T_CHECK(mixed_rig_up(&r));
    if (r.m == NULL) {
        rig_down(&r);
        return g_failures - before;
    }
    T_CHECK(shx_driver_open(&pub_a, &r.fake, ver_a, alpn_of(ver_a)));
    T_CHECK(shx_driver_open(&sub, &r.fake, sub_ver, alpn_of(sub_ver)));
    T_CHECK(shx_driver_open(&pub_b, &r.fake, ver_b, alpn_of(ver_b)));
    T_CHECK(establish(&r, &pub_a));
    T_CHECK(establish(&r, &sub));
    T_CHECK(establish(&r, &pub_b));

    shx_driver_t *ds[3] = { &pub_a, &sub, &pub_b };
    moq_bytes_t nsp[2] = { { NS0, sizeof(NS0) - 1 },
                           { NS1, sizeof(NS1) - 1 } };
    moq_subgroup_handle_t sgh;

    T_CHECK(stage_track(&r, ds, 3, &pub_a, &sub, nsp, &sgh));

    /* Hold the terminal until released, so the replacement lands while the
     * previous generation's terminal is still owed. */
    moqr_bind_debug_block_sub_done(HOLD_UNTIL_RELEASED);
    end_upstream(&r, ds, 3, &pub_a, sgh, up_status);
    T_CHECK(sub.done_status_count == 0);   /* genuinely held */

    /* The replacement announces the SAME namespace on the other draft: a new
     * upstream generation while the previous generation's terminal is owed. */
    moq_publish_namespace_cfg_t pcfg;

    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };

    moq_announcement_t ann_b;

    T_CHECK(moq_session_publish_namespace(pub_b.sess, &pcfg, pub_b.now,
                                          &ann_b) == MOQ_OK);
    pump_rounds(&r, ds, 3, 12);
    T_CHECK(sub.done_status_count == 0);   /* still held, across the takeover */

    moqr_bind_debug_block_sub_done(0);
    for (int i = 0; i < 24 && sub.done_status_count == 0; i++) {
        pump_rounds(&r, ds, 3, 4);
    }

    printf("ORACLE %s up=0x%llx decoded=0x%llx want=0x%llx count=%d\n", label,
           (unsigned long long)up_status,
           (unsigned long long)sub.last_done_status,
           (unsigned long long)want_status, sub.done_status_count);
    T_CHECK(sub.done_status_count == 1);
    T_CHECK(sub.last_done_status == want_status);

    pump_rounds(&r, ds, 3, 8);
    T_CHECK(sub.done_status_count == 1);   /* exactly once */

    shx_driver_close(&pub_a);
    shx_driver_close(&pub_b);
    shx_driver_close(&sub);
    rig_down(&r);
    if (g_failures == before) {
        printf("PASS: %s\n", label);
    }
    return g_failures - before;
}

/*
 * MIXED GENERATION: two tracks, two publishers on DIFFERENT drafts, two
 * subscribers on the SAME draft. Both terminals are held and drained together
 * with the identical upstream number. Origin is per-record, so the same number
 * must decode differently for each subscriber.
 */
static int
run_pd_mixed_generation(const char *label, moq_version_t sub_ver,
                        uint64_t up_status, uint64_t want_from_d16,
                        uint64_t want_from_d18)
{
    int before = g_failures;
    rig_t r;
    shx_driver_t pub16, pub18, sub_a, sub_b;

    T_CHECK(mixed_rig_up(&r));
    if (r.m == NULL) {
        rig_down(&r);
        return g_failures - before;
    }
    T_CHECK(shx_driver_open(&pub16, &r.fake, D16, alpn_of(D16)));
    T_CHECK(shx_driver_open(&pub18, &r.fake, D18, alpn_of(D18)));
    T_CHECK(shx_driver_open(&sub_a, &r.fake, sub_ver, alpn_of(sub_ver)));
    T_CHECK(shx_driver_open(&sub_b, &r.fake, sub_ver, alpn_of(sub_ver)));
    T_CHECK(establish(&r, &pub16));
    T_CHECK(establish(&r, &pub18));
    T_CHECK(establish(&r, &sub_a));
    T_CHECK(establish(&r, &sub_b));

    shx_driver_t *ds[4] = { &pub16, &pub18, &sub_a, &sub_b };
    moq_bytes_t ns_a[2] = { { NS0, sizeof(NS0) - 1 },
                            { NS1, sizeof(NS1) - 1 } };
    moq_bytes_t ns_b[2] = { { NS0, sizeof(NS0) - 1 },
                            { NS2, sizeof(NS2) - 1 } };
    moq_subgroup_handle_t sg_a, sg_b;

    T_CHECK(stage_track(&r, ds, 4, &pub16, &sub_a, ns_a, &sg_a));
    T_CHECK(stage_track(&r, ds, 4, &pub18, &sub_b, ns_b, &sg_b));

    /* Both terminals owed at once, neither on the wire. */
    moqr_bind_debug_block_sub_done(HOLD_UNTIL_RELEASED);
    end_upstream(&r, ds, 4, &pub16, sg_a, up_status);
    end_upstream(&r, ds, 4, &pub18, sg_b, up_status);
    T_CHECK(sub_a.done_status_count == 0);
    T_CHECK(sub_b.done_status_count == 0);

    moqr_bind_debug_block_sub_done(0);
    for (int i = 0; i < 32 &&
                    (sub_a.done_status_count == 0 ||
                     sub_b.done_status_count == 0); i++) {
        pump_rounds(&r, ds, 4, 4);
    }

    printf("ORACLE %s up=0x%llx from_d16=0x%llx (want 0x%llx) "
           "from_d18=0x%llx (want 0x%llx)\n", label,
           (unsigned long long)up_status,
           (unsigned long long)sub_a.last_done_status,
           (unsigned long long)want_from_d16,
           (unsigned long long)sub_b.last_done_status,
           (unsigned long long)want_from_d18);
    T_CHECK(sub_a.done_status_count == 1);
    T_CHECK(sub_b.done_status_count == 1);
    T_CHECK(sub_a.last_done_status == want_from_d16);
    T_CHECK(sub_b.last_done_status == want_from_d18);

    shx_driver_close(&pub16);
    shx_driver_close(&pub18);
    shx_driver_close(&sub_a);
    shx_driver_close(&sub_b);
    rig_down(&r);
    if (g_failures == before) {
        printf("PASS: %s\n", label);
    }
    return g_failures - before;
}

/* -- upstream migration signals (draft-18 Section 10.4 / 10.6) -------------- *
 *
 * The relay originates one upstream request: the SUBSCRIBE it makes on a
 * downstream subscriber's behalf. That request can be terminated by the
 * upstream in two different ways, and they do NOT mean the same thing:
 *
 *   GOAWAY on the request stream (Section 10.4) says the upstream is going
 *   away and the request should be re-issued elsewhere;
 *   REQUEST_ERROR REDIRECT (Section 10.6.1) says this endpoint cannot serve
 *   the request, and carries a Redirect structure naming where it could be.
 *
 * This relay follows neither: it does not re-issue, retarget, or forward a
 * Redirect. What it owes downstream is therefore a TRUTHFUL terminal for its
 * own state, never a restatement of the upstream's intent it cannot act on.
 */

typedef enum up_signal {
    UP_SIG_GOAWAY = 0,
    UP_SIG_REDIRECT,
} up_signal_t;

/* Terminate the relay's upstream SUBSCRIBE the way `sig` says. `accepted`
 * selects the relay-side state: an ACTIVE upstream (already OK'd) or a PENDING
 * one (terminated before its OK). */
static moq_result_t
pub_signal_rc(shx_driver_t *pub, up_signal_t sig)
{
    pub->now += 1000;
    if (sig == UP_SIG_GOAWAY) {
        moq_request_goaway_cfg_t g;

        moq_request_goaway_cfg_init(&g);
        return moq_session_request_goaway_subscribe(
                   pub->sess, pub->subscribe_handle, &g, pub->now);
    }

    /* REDIRECT rides a REQUEST_ERROR and carries a Redirect structure. The
     * upstream offers a retry interval; the relay must not pass that on. */
    moq_reject_subscribe_cfg_t rj;

    moq_reject_subscribe_cfg_init(&rj);
    rj.error_code = MOQ_REQUEST_ERROR_REDIRECT;
    rj.can_retry = true;
    rj.retry_after_ms = 5000;
    rj.redirect.connect_uri = (moq_bytes_t){ NULL, 0 };
    rj.redirect.track_namespace = (moq_namespace_t){ NULL, 0 };
    rj.redirect.track_name = (moq_bytes_t){ NULL, 0 };
    return moq_session_reject_subscribe(pub->sess, pub->subscribe_handle, &rj,
                                        pub->now);
}

/*
 * One publisher, one subscriber, possibly on different drafts. The publisher
 * either accepts the relay's upstream SUBSCRIBE and then signals (ACTIVE), or
 * signals before accepting (PENDING). The subscriber's decoded terminal is the
 * oracle.
 *
 * `want_done` >= 0 expects a PUBLISH_DONE with that status; `want_error` >= 0
 * expects a REQUEST_ERROR with that code. Exactly one of them applies.
 */
static int
run_migration_case(const char *label, up_signal_t sig, bool accepted,
                   moq_version_t pub_ver, moq_version_t sub_ver,
                   int64_t want_done, int64_t want_error)
{
    int before = g_failures;
    rig_t r;
    shx_driver_t pub, sub;

    T_CHECK(mixed_rig_up(&r));
    if (r.m == NULL) {
        rig_down(&r);
        return g_failures - before;
    }
    T_CHECK(shx_driver_open(&pub, &r.fake, pub_ver, alpn_of(pub_ver)));
    T_CHECK(shx_driver_open(&sub, &r.fake, sub_ver, alpn_of(sub_ver)));
    T_CHECK(establish(&r, &pub));
    T_CHECK(establish(&r, &sub));

    shx_driver_t *ds[2] = { &pub, &sub };
    moq_bytes_t nsp[2] = { { NS0, sizeof(NS0) - 1 },
                           { NS1, sizeof(NS1) - 1 } };
    moq_publish_namespace_cfg_t pcfg;

    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };

    moq_announcement_t ann;

    T_CHECK(moq_session_publish_namespace(pub.sess, &pcfg, pub.now, &ann) ==
            MOQ_OK);
    pump_rounds(&r, ds, 2, 8);

    moq_subscribe_cfg_t sc;

    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    sc.track_name = (moq_bytes_t){ (const uint8_t *)"t", 1 };
    sc.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;

    moq_subscription_t sh;

    T_CHECK(moq_session_subscribe(sub.sess, &sc, sub.now, &sh) == MOQ_OK);

    /* Wait for the relay's upstream SUBSCRIBE to reach the publisher. */
    for (int i = 0; i < 24 && !pub.got_subscribe; i++) {
        pump_rounds(&r, ds, 2, 2);
    }
    T_CHECK(pub.got_subscribe);
    pub.got_subscribe = false;

    moq_subgroup_handle_t sgh;

    if (accepted) {
        moq_accept_subscribe_cfg_t acfg;

        moq_accept_subscribe_cfg_init(&acfg);
        pub.now += 1000;
        T_CHECK(moq_session_accept_subscribe(pub.sess, pub.subscribe_handle,
                                             &acfg, pub.now) == MOQ_OK);
        /* An object makes the subscription genuinely ACTIVE downstream. */
        moq_subgroup_cfg_t sgc;

        moq_subgroup_cfg_init(&sgc);
        sgc.group_id = 1;
        sgc.publisher_priority = 100;
        pub.now += 1000;
        T_CHECK(moq_session_open_subgroup(pub.sess, pub.subscribe_handle, &sgc,
                                          pub.now, &sgh) == MOQ_OK);
        T_CHECK(pub_push(&pub, sgh, 0));
        pump_rounds(&r, ds, 2, 10);
        T_CHECK(shx_ev_count(&sub, MOQ_EVENT_OBJECT_RECEIVED) == 1);
        pub.now += 1000;
        T_CHECK(moq_session_close_subgroup(pub.sess, sgh, pub.now) == MOQ_OK);
        pump_rounds(&r, ds, 2, 8);
    }

    T_CHECK(pub_signal_rc(&pub, sig) == MOQ_OK);
    for (int i = 0; i < 40 && sub.done_status_count == 0 &&
                    sub.sub_error_count == 0; i++) {
        pump_rounds(&r, ds, 2, 4);
    }

    printf("ORACLE %s done{n=%d code=0x%llx} error{n=%d code=0x%llx "
           "retry=%d after=%llu}\n", label, sub.done_status_count,
           (unsigned long long)sub.last_done_status, sub.sub_error_count,
           (unsigned long long)sub.last_sub_error,
           (int)sub.last_sub_error_can_retry,
           (unsigned long long)sub.last_sub_error_retry_ms);

    if (want_done >= 0) {
        T_CHECK(sub.done_status_count == 1);
        T_CHECK(sub.last_done_status == (uint64_t)want_done);
        T_CHECK(sub.sub_error_count == 0);   /* not a request error */
    } else {
        T_CHECK(sub.sub_error_count == 1);
        T_CHECK(sub.last_sub_error == (uint64_t)want_error);
        T_CHECK(sub.done_status_count == 0); /* no terminal was invented */
        /* The relay implements no reissue policy: it must never hand back a
         * retryable answer, whatever the upstream offered. */
        T_CHECK(!sub.last_sub_error_can_retry);
        T_CHECK(sub.last_sub_error_retry_ms == 0);
    }

    /* exactly once */
    pump_rounds(&r, ds, 2, 8);
    T_CHECK(sub.done_status_count + sub.sub_error_count == 1);

    shx_driver_close(&pub);
    shx_driver_close(&sub);
    rig_down(&r);
    if (g_failures == before) {
        printf("PASS: %s\n", label);
    }
    return g_failures - before;
}

/* Stage the relay's upstream SUBSCRIBE to ACTIVE (`accepted`) or leave it
 * PENDING, returning with both peers live and the upstream untouched. */
static bool
mig_stage(rig_t *r, shx_driver_t *pub, shx_driver_t *sub, moq_version_t pub_ver,
          moq_version_t sub_ver, bool accepted)
{
    if (!shx_driver_open(pub, &r->fake, pub_ver, alpn_of(pub_ver)) ||
        !shx_driver_open(sub, &r->fake, sub_ver, alpn_of(sub_ver)) ||
        !establish(r, pub) || !establish(r, sub)) {
        return false;
    }
    shx_driver_t *ds[2] = { pub, sub };
    moq_bytes_t nsp[2] = { { NS0, sizeof(NS0) - 1 },
                           { NS1, sizeof(NS1) - 1 } };
    moq_publish_namespace_cfg_t pcfg;

    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };

    moq_announcement_t ann;

    if (moq_session_publish_namespace(pub->sess, &pcfg, pub->now, &ann) !=
        MOQ_OK) {
        return false;
    }
    pump_rounds(r, ds, 2, 8);

    moq_subscribe_cfg_t sc;

    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    sc.track_name = (moq_bytes_t){ (const uint8_t *)"t", 1 };
    sc.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;

    moq_subscription_t sh;

    if (moq_session_subscribe(sub->sess, &sc, sub->now, &sh) != MOQ_OK) {
        return false;
    }
    for (int i = 0; i < 24 && !pub->got_subscribe; i++) {
        pump_rounds(r, ds, 2, 2);
    }
    if (!pub->got_subscribe) {
        return false;
    }
    pub->got_subscribe = false;
    if (!accepted) {
        return true;
    }
    moq_accept_subscribe_cfg_t acfg;

    moq_accept_subscribe_cfg_init(&acfg);
    pub->now += 1000;
    if (moq_session_accept_subscribe(pub->sess, pub->subscribe_handle, &acfg,
                                     pub->now) != MOQ_OK) {
        return false;
    }
    pump_rounds(r, ds, 2, 8);
    return true;
}

/*
 * The per-request GOAWAY is a draft-18 addition: draft-16 Section 9.4 has only
 * a session-scoped GOAWAY, and its message carries no Request ID. A relay on a
 * draft-16 upstream therefore never sees this signal at all, which is the
 * reason a draft-16 fallback exists for it rather than a translation.
 */
static int
run_migration_unreachable(const char *label)
{
    int before = g_failures;
    rig_t r;
    shx_driver_t pub, sub;

    T_CHECK(mixed_rig_up(&r));
    if (r.m == NULL) {
        rig_down(&r);
        return g_failures - before;
    }
    T_CHECK(mig_stage(&r, &pub, &sub, D16, D16, true));

    /* draft-16 cannot express it: the library refuses the operation rather
     * than inventing a message the profile does not define. */
    moq_request_goaway_cfg_t g;

    moq_request_goaway_cfg_init(&g);
    pub.now += 1000;
    moq_result_t rc16 = moq_session_request_goaway_subscribe(
        pub.sess, pub.subscribe_handle, &g, pub.now);

    printf("ORACLE %s d16_request_goaway=%d\n", label, (int)rc16);
    T_CHECK(rc16 == MOQ_ERR_INVAL);

    shx_driver_t *ds[2] = { &pub, &sub };

    pump_rounds(&r, ds, 2, 8);
    /* A refused signal is not a terminal: nothing was said downstream. */
    T_CHECK(sub.done_status_count == 0);
    T_CHECK(sub.sub_error_count == 0);

    shx_driver_close(&pub);
    shx_driver_close(&sub);
    rig_down(&r);
    if (g_failures == before) {
        printf("PASS: %s\n", label);
    }
    return g_failures - before;
}

/*
 * The two combinations draft-18 places out of reach of the wire — a
 * REQUEST_ERROR cannot answer a request that already had its SUBSCRIBE_OK
 * (Section 10.6: exactly one answer per request), and a per-request GOAWAY is
 * eligible only on an ESTABLISHED request (Section 10.4) — driven through the
 * same production dispatch the event handler calls, with the terminal still
 * decoded off the downstream peer's own session.
 *
 * `accepted` selects the relay-side state, and therefore the cause under test:
 * ACTIVE is probed with REDIRECT, PENDING with GOAWAY.
 */
static int
run_migration_probe(const char *label, moq_version_t sub_ver, bool accepted,
                    int64_t want_done, int64_t want_error)
{
    int before = g_failures;
    rig_t r;
    shx_driver_t pub, sub;

    T_CHECK(mixed_rig_up(&r));
    if (r.m == NULL) {
        rig_down(&r);
        return g_failures - before;
    }
    T_CHECK(mig_stage(&r, &pub, &sub, D18, sub_ver, accepted));

    uint32_t slot = 0;
    uint64_t handle = 0;

    T_CHECK(moqr_bind_debug_first_usub(r.bind, &slot, &handle));

    shx_driver_t *ds[2] = { &pub, &sub };

    r.now += 1000;
    moqr_bind_debug_upstream_terminated(r.bind, slot,
                                        MOQ_REQUEST_FAMILY_SUBSCRIBE, handle,
                                        accepted ? 1 : 0, r.now);
    for (int i = 0; i < 40 && sub.done_status_count == 0 &&
                    sub.sub_error_count == 0; i++) {
        pump_rounds(&r, ds, 2, 4);
    }

    printf("ORACLE %s done{n=%d code=0x%llx} error{n=%d code=0x%llx "
           "retry=%d after=%llu}\n", label, sub.done_status_count,
           (unsigned long long)sub.last_done_status, sub.sub_error_count,
           (unsigned long long)sub.last_sub_error,
           (int)sub.last_sub_error_can_retry,
           (unsigned long long)sub.last_sub_error_retry_ms);

    if (want_done >= 0) {
        T_CHECK(sub.done_status_count == 1);
        T_CHECK(sub.last_done_status == (uint64_t)want_done);
        T_CHECK(sub.sub_error_count == 0);
    } else {
        T_CHECK(sub.sub_error_count == 1);
        T_CHECK(sub.last_sub_error == (uint64_t)want_error);
        T_CHECK(sub.done_status_count == 0);
        T_CHECK(!sub.last_sub_error_can_retry);
        T_CHECK(sub.last_sub_error_retry_ms == 0);
    }
    pump_rounds(&r, ds, 2, 8);
    T_CHECK(sub.done_status_count + sub.sub_error_count == 1);

    shx_driver_close(&pub);
    shx_driver_close(&sub);
    rig_down(&r);
    if (g_failures == before) {
        printf("PASS: %s\n", label);
    }
    return g_failures - before;
}

/*
 * Fail-closed: a family the relay never originates, and a handle matching no
 * live upstream subscription, must both be no-ops. These are terminal answers
 * to the relay's OWN requests — there is no peer owed a wire reply — so an
 * unrecognised one must leave every subscription and every demand untouched.
 */
static int
run_migration_probe_guards(const char *label)
{
    int before = g_failures;
    rig_t r;
    shx_driver_t pub, sub;

    T_CHECK(mixed_rig_up(&r));
    if (r.m == NULL) {
        rig_down(&r);
        return g_failures - before;
    }
    T_CHECK(mig_stage(&r, &pub, &sub, D18, D18, true));

    uint32_t slot = 0;
    uint64_t handle = 0;

    T_CHECK(moqr_bind_debug_first_usub(r.bind, &slot, &handle));

    shx_driver_t *ds[2] = { &pub, &sub };
    moqr_bind_stats_t st0;

    moqr_bind_get_stats(r.bind, &st0);

    r.now += 1000;
    /* right handle, a family the relay never originates */
    moqr_bind_debug_upstream_terminated(r.bind, slot,
                                        MOQ_REQUEST_FAMILY_FETCH, handle, 0,
                                        r.now);
    /* right family, a handle no upstream subscription owns */
    moqr_bind_debug_upstream_terminated(r.bind, slot,
                                        MOQ_REQUEST_FAMILY_SUBSCRIBE,
                                        handle ^ 0xDEADBEEFull, 1, r.now);
    /* a connection slot that is not in use */
    moqr_bind_debug_upstream_terminated(r.bind, 0xFFFFu,
                                        MOQ_REQUEST_FAMILY_SUBSCRIBE, handle,
                                        0, r.now);
    pump_rounds(&r, ds, 2, 12);

    moqr_bind_stats_t st1;

    moqr_bind_get_stats(r.bind, &st1);

    printf("ORACLE %s done=%d error=%d session_errors=%llu->%llu\n", label,
           sub.done_status_count, sub.sub_error_count,
           (unsigned long long)st0.session_errors,
           (unsigned long long)st1.session_errors);
    T_CHECK(sub.done_status_count == 0);
    T_CHECK(sub.sub_error_count == 0);
    T_CHECK(st1.session_errors == st0.session_errors);

    /* The upstream subscription is still there and still usable: the real
     * signal on the real coordinates still terminates it exactly once. */
    uint32_t slot2 = 0;
    uint64_t handle2 = 0;

    T_CHECK(moqr_bind_debug_first_usub(r.bind, &slot2, &handle2));
    T_CHECK(slot2 == slot);
    T_CHECK(handle2 == handle);

    r.now += 1000;
    moqr_bind_debug_upstream_terminated(r.bind, slot,
                                        MOQ_REQUEST_FAMILY_SUBSCRIBE, handle,
                                        0, r.now);
    for (int i = 0; i < 40 && sub.done_status_count == 0; i++) {
        pump_rounds(&r, ds, 2, 4);
    }
    T_CHECK(sub.done_status_count == 1);
    T_CHECK(sub.last_done_status == 0x4u);   /* GOAWAY -> GOING_AWAY */

    shx_driver_close(&pub);
    shx_driver_close(&sub);
    rig_down(&r);
    if (g_failures == before) {
        printf("PASS: %s\n", label);
    }
    return g_failures - before;
}

/* -- request-error width (draft-16 Section 13.4.2 / draft-18 Section 15.10.2) --
 *
 * A REQUEST_ERROR code is a QUIC varint, not a 32-bit number. draft-18 accepts
 * the full 64-bit range; draft-16's encoder tops out at the QUIC varint
 * maximum. A relay that stores the code in 32 bits does not fail loudly — C
 * narrows silently — it forwards a DIFFERENT, still-plausible code.
 *
 * WIDE's low 32 bits are 0x7, which is unassigned in both registries, so a
 * truncating relay produces a legal-looking value rather than an obvious zero.
 * That is the point: only the exact comparison catches it.
 */
#define REQERR_WIDE   UINT64_C(0x100000007)
/* Above draft-16's QUIC varint ceiling, so its encoder must refuse it. */
#define REQERR_UNREP  UINT64_MAX

/*
 * The upstream answers the relay's own SUBSCRIBE with a REQUEST_ERROR. The
 * relay resolves that into its parked downstream subscribers, and the value
 * the downstream peer decodes must be the one the upstream sent, bit for bit.
 */
static int
run_upstream_error_width(const char *label, moq_version_t pub_ver,
                         moq_version_t sub_ver, uint64_t up_code,
                         bool expect_delivered, uint64_t want_code)
{
    int before = g_failures;
    rig_t r;
    shx_driver_t pub, sub;

    T_CHECK(mixed_rig_up(&r));
    if (r.m == NULL) {
        rig_down(&r);
        return g_failures - before;
    }
    T_CHECK(mig_stage(&r, &pub, &sub, pub_ver, sub_ver, false));

    shx_driver_t *ds[2] = { &pub, &sub };
    moqr_bind_stats_t st0;

    moqr_bind_get_stats(r.bind, &st0);

    /* The upstream refuses the relay's SUBSCRIBE with the wide code. */
    moq_reject_subscribe_cfg_t rj;

    moq_reject_subscribe_cfg_init(&rj);
    rj.error_code = up_code;
    pub.now += 1000;
    T_CHECK(moq_session_reject_subscribe(pub.sess, pub.subscribe_handle, &rj,
                                         pub.now) == MOQ_OK);

    for (int i = 0; i < 40 && sub.sub_error_count == 0; i++) {
        pump_rounds(&r, ds, 2, 4);
    }

    moqr_bind_stats_t st1;

    moqr_bind_get_stats(r.bind, &st1);
    printf("ORACLE %s up=0x%llx decoded{n=%d code=0x%llx} want=0x%llx\n", label,
           (unsigned long long)up_code, sub.sub_error_count,
           (unsigned long long)sub.last_sub_error,
           (unsigned long long)want_code);

    if (expect_delivered) {
        T_CHECK(sub.sub_error_count == 1);
        /* The whole 64-bit value, not its low half. */
        T_CHECK(sub.last_sub_error == want_code);
        T_CHECK(sub.done_status_count == 0);
    } else {
        /* draft-16 cannot express it: the session refuses the code rather than
         * altering it, so nothing false reaches the subscriber. */
        T_CHECK(sub.sub_error_count == 0);
        T_CHECK(sub.done_status_count == 0);
        /* ...and the refusal is not paid for elsewhere: no session was closed
         * and no connection was torn down on the way out. */
        T_CHECK(st1.session_errors == st0.session_errors);
        /* the upstream subscription is still the relay's to resolve */
        uint32_t uslot = 0;
        uint64_t uhandle = 0;

        T_CHECK(moqr_bind_debug_first_usub(r.bind, &uslot, &uhandle));
    }

    shx_driver_close(&pub);
    shx_driver_close(&sub);
    rig_down(&r);
    if (g_failures == before) {
        printf("PASS: %s\n", label);
    }
    return g_failures - before;
}

/*
 * The relay AUTHORS the request error: an initial SUBSCRIBE denial carries the
 * verdict's own code straight to the downstream session. Nothing normalizes it
 * on the way in, so this is where the relay's own carrier width decides what
 * the subscriber reads.
 *
 * `expect_delivered == false` is the fail-closed arm: a code the target draft's
 * encoder cannot express must be refused, never replaced by a different one,
 * and the refusal must not cost the connection or the upstream subscription.
 */
static int
run_authored_reqerr(const char *label, moq_version_t sub_ver, uint64_t code,
                    bool expect_delivered, uint64_t want)
{
    int before = g_failures;
    rig_t r;
    shx_driver_t pub, sub;

    g_reval_decision = MOQR_AUTH_DENY;
    g_reval_lease = 0;
    g_deny_code = 0;
    g_request_error_only = code;
    T_CHECK(mixed_rig_up_auth(&r, reval_hook));
    if (r.m == NULL) {
        rig_down(&r);
        g_request_error_only = 0;
        g_reval_decision = MOQR_AUTH_ALLOW;
        return g_failures - before;
    }
    moq_subscription_t sh;

    (void)ext_stage(&r, &pub, &sub, sub_ver, &sh);
    shx_driver_t *ds[2] = { &pub, &sub };
    moqr_bind_stats_t st0;

    moqr_bind_get_stats(r.bind, &st0);
    pump_rounds(&r, ds, 2, 24);

    moqr_bind_stats_t st1;

    moqr_bind_get_stats(r.bind, &st1);
    printf("ORACLE %s authored=0x%llx decoded{n=%d code=0x%llx}\n", label,
           (unsigned long long)code, sub.sub_error_count,
           (unsigned long long)sub.last_sub_error);

    if (expect_delivered) {
        T_CHECK(sub.sub_error_count >= 1);
        T_CHECK(sub.last_sub_error == want);
    } else {
        /* Refused, not altered: the subscriber is told nothing false. */
        T_CHECK(sub.last_sub_error != code);
        T_CHECK(sub.last_sub_error == 0);
    }
    /* No collateral either way: the denial is a request answer, never a
     * PUBLISH_DONE, and it costs no session. */
    T_CHECK(sub.done_status_count == 0);
    T_CHECK(st1.session_errors == st0.session_errors);

    shx_driver_close(&pub);
    shx_driver_close(&sub);
    rig_down(&r);
    g_request_error_only = 0;
    g_reval_decision = MOQR_AUTH_ALLOW;
    if (g_failures == before) {
        printf("PASS: %s\n", label);
    }
    return g_failures - before;
}

int
main(void)
{
    /* The shuttle's frame scanner is used by the reset arms below; reference it
     * so the shared header does not warn in arms that only read decoded
     * events. */
    (void)(&shx_scan);

    /* The swap, both directions: the same MEANING keeps its meaning. */
    (void)run_pd_forward("pd_expired_d16_to_d18", D16, D18, 0x5u, 0x6u);
    (void)run_pd_forward("pd_expired_d18_to_d16", D18, D16, 0x6u, 0x5u);
    (void)run_pd_forward("pd_toofar_d16_to_d18", D16, D18, 0x6u, 0x5u);
    (void)run_pd_forward("pd_toofar_d18_to_d16", D18, D16, 0x5u, 0x6u);

    /* Codes both drafts agree on are unchanged by the hop. */
    (void)run_pd_forward("pd_track_ended_d16_to_d18", D16, D18, 0x2u, 0x2u);

    /* Same draft: byte-verbatim, including a code this relay never heard of. */
    (void)run_pd_forward("pd_unknown_same_draft_d18", D18, D18, 0x4242u,
                         0x4242u);
    (void)run_pd_forward("pd_unknown_same_draft_d16", D16, D16, 0x4242u,
                         0x4242u);

    /* Cross draft, unrecognized: the number means something else there, so it
     * fails closed to the target domain's INTERNAL_ERROR. */
    (void)run_pd_forward("pd_unknown_cross_d18_to_d16", D18, D16, 0x4242u,
                         0x0u);
    (void)run_pd_forward("pd_unknown_cross_d16_to_d18", D16, D18, 0x4242u,
                         0x0u);
    /* draft-18's EXCESSIVE_LOAD has no draft-16 counterpart. */
    (void)run_pd_forward("pd_excessive_load_d18_to_d16", D18, D16, 0x9u, 0x0u);

    /* Reset relocation: draft-16 numbers UNKNOWN_OBJECT_STATUS 0x4, which is
     * GOING_AWAY in draft-18 — the collision a verbatim hop would create. */
    (void)run_reset_forward("rs_unknown_obj_d16_to_d18", D16, D18, 0x4u, 0x6u);
    (void)run_reset_forward("rs_unknown_obj_d18_to_d16", D18, D16, 0x6u, 0x4u);

    /* draft-18-only causes have no draft-16 number: they degrade to the
     * truthful generic CANCELLED, never borrowing 0x4. */
    (void)run_reset_forward("rs_going_away_d18_to_d16", D18, D16, 0x4u, 0x1u);
    (void)run_reset_forward("rs_too_far_behind_d18_to_d16", D18, D16, 0x5u,
                            0x1u);

    /* Codes both drafts agree on, and same-draft verbatim including an
     * unknown extension. */
    (void)run_reset_forward("rs_cancelled_d16_to_d18", D16, D18, 0x1u, 0x1u);
    (void)run_reset_forward("rs_unknown_same_draft", D18, D18, 0x4242u,
                            0x4242u);
    (void)run_reset_forward("rs_unknown_cross_draft", D18, D16, 0x4242u, 0x0u);

    /* Negative control: an initial denial stays in the REQUEST_ERROR domain. */
    (void)run_initial_deny_control("auth_initial_deny_is_request_error", D18);

    /* A relay-authored extension reaches BOTH drafts unchanged when the number
     * means nothing in either; a registered or GREASE number is refused as an
     * extension and degrades to the truthful UNAUTHORIZED. */
    (void)run_reval_extension("ext_0x7_to_d16", D16, 0x7u, 0x7u);
    (void)run_reval_extension("ext_0x7_to_d18", D18, 0x7u, 0x7u);
    (void)run_reval_extension("ext_registered_0x9_to_d18", D18, 0x9u, 0x1u);
    (void)run_reval_extension("ext_registered_0x5_to_d16", D16, 0x5u, 0x1u);
    (void)run_reval_extension("ext_grease_to_d18", D18, 0x9Du, 0x1u);

    /* The width the tagged terminal unlocks: a 64-bit relay-authored extension
     * now reaches both drafts exactly, which the REQUEST_ERROR scalar could
     * never have carried. */
    (void)run_reval_extension("ext_64bit_to_d16", D16, UINT64_C(0x100000000),
                              UINT64_C(0x100000000));
    (void)run_reval_extension("ext_64bit_to_d18", D18, UINT64_C(0x100000000),
                              UINT64_C(0x100000000));

    /* No numeric laundering: a request error of 0x7 with NO tagged terminal
     * must end the subscription as UNAUTHORIZED, never as PD extension 0x7. */
    g_request_error_only = 0x7u;
    (void)run_reval_extension("no_laundering_request_error_0x7", D18, 0u, 0x1u);
    g_request_error_only = 0;

    /* A 64-bit terminal survives emission unnarrowed. The status field on the
     * wire is 64-bit, so a forwarded upstream code above UINT32_MAX must reach
     * the peer intact; a same-draft hop isolates the width from any mapping. */
    (void)run_pd_forward("pd_64bit_same_draft_d18", D18, D18,
                         UINT64_C(0x100000000), UINT64_C(0x100000000));
    (void)run_pd_forward("pd_64bit_same_draft_d16", D16, D16,
                         UINT64_C(0x100000000), UINT64_C(0x100000000));

    /* Same-draft unknown reset on BOTH drafts, not one representative. */
    (void)run_reset_forward("rs_unknown_same_draft_d16", D16, D16, 0x4242u,
                            0x4242u);
    (void)run_reset_forward("rs_unknown_same_draft_d18", D18, D18, 0x4242u,
                            0x4242u);
    /* and the reverse cross-draft direction for the unknown case */
    (void)run_reset_forward("rs_unknown_cross_d16_to_d18", D16, D18, 0x4242u,
                            0x0u);

    /* A downstream terminal write that blocks is retained and retried. The
     * descriptor and its origin profile are fixed when the upstream terminal
     * arrives, so the byte that finally lands must be the same one an unheld
     * hop produces — once, never twice. */
    (void)run_pd_forward_held("pd_held_swap_d16_to_d18", D16, D18, 0x5u, 0x6u,
                              1);
    (void)run_pd_forward_held("pd_held_swap_d18_to_d16", D18, D16, 0x5u, 0x6u,
                              1);
    (void)run_pd_forward_held("pd_held_twice_swap_d16_to_d18", D16, D18, 0x5u,
                              0x6u, 2);
    /* the 64-bit terminal survives the hold unnarrowed */
    (void)run_pd_forward_held("pd_held_64bit_same_draft_d18", D18, D18,
                              UINT64_C(0x100000000), UINT64_C(0x100000000), 1);
    (void)run_pd_forward_held("pd_held_64bit_same_draft_d16", D16, D16,
                              UINT64_C(0x100000000), UINT64_C(0x100000000), 2);
    /* an unknown cross-draft terminal still fails closed after a hold */
    (void)run_pd_forward_held("pd_held_unknown_cross_d18_to_d16", D18, D16,
                              0x4242u, 0x0u, 1);

    /* A retained terminal keeps the origin of the generation that produced it,
     * even when a different-draft publisher takes the namespace over before it
     * reaches the wire. */
    (void)run_pd_replacement("pd_replacement_origin_d16_gen_d18_repl", D16, D18,
                             D18, 0x5u, 0x6u);
    (void)run_pd_replacement("pd_replacement_origin_d18_gen_d16_repl", D18, D16,
                             D16, 0x5u, 0x6u);

    /* Origin is per record, not per relay: one upstream number, two drafts of
     * origin, two different bytes decoded by same-draft subscribers. */
    (void)run_pd_mixed_generation("pd_mixed_generation_to_d18", D18, 0x5u, 0x6u,
                                  0x5u);
    (void)run_pd_mixed_generation("pd_mixed_generation_to_d16", D16, 0x5u, 0x5u,
                                  0x6u);

    /* GOAWAY on a request stream: the upstream really is going away, and
     * PUBLISH_DONE GOING_AWAY is 0x4 in both registries. */
    (void)run_migration_case("mig_active_goaway_d18", UP_SIG_GOAWAY, true,
                             D18, D18, 0x4, -1);
    (void)run_migration_case("mig_active_goaway_cross", UP_SIG_GOAWAY, true,
                             D18, D16, 0x4, -1);

    /* PENDING REDIRECT: the upstream never OK'd, so the parked downstream
     * subscribers get a REQUEST_ERROR. No timeout elapsed, so TIMEOUT (0x2)
     * would be a false statement; the relay carries no Redirect structure, so
     * REDIRECT (0x34) would be false too — and draft-16 assigns it nothing.
     * Generic INTERNAL_ERROR (0x0), identical in both registries, is truthful.
     * The upstream offered a 5-second retry interval; the relay has no reissue
     * schedule, so the subscriber must not be told to retry. */
    (void)run_migration_case("mig_pending_redirect_d18", UP_SIG_REDIRECT,
                             false, D18, D18, -1, 0x0);
    (void)run_migration_case("mig_pending_redirect_cross", UP_SIG_REDIRECT,
                             false, D18, D16, -1, 0x0);

    /* The two rows the drafts place out of reach of the wire, pinned against
     * the same production dispatch the event handler calls. */
    (void)run_migration_unreachable("mig_reach_goaway_needs_d18");
    (void)run_migration_probe("mig_probe_active_redirect_d18", D18, true,
                              0x2, -1);
    (void)run_migration_probe("mig_probe_active_redirect_cross", D16, true,
                              0x2, -1);
    (void)run_migration_probe("mig_probe_pending_goaway_d18", D18, false,
                              -1, 0x0);
    (void)run_migration_probe("mig_probe_pending_goaway_d16", D16, false,
                              -1, 0x0);
    (void)run_migration_probe_guards("mig_probe_fail_closed");

    /* A REQUEST_ERROR is a varint. draft-18 carries the full 64-bit value to
     * the peer; draft-16 refuses one its encoder cannot express rather than
     * silently sending a different code. */
    (void)run_upstream_error_width("reqerr_control_small", D18, D18,
                                   0x11u, true, 0x11u);
    /* draft-16 hands an unregistered code to the application verbatim, so this
     * is the hop on which a wide value is actually visible end to end. A relay
     * that stored it in 32 bits would forward 0x7 here — a legal-looking code
     * that says something the upstream never said. */
    (void)run_upstream_error_width("reqerr_width_d16_to_d16_exact", D16, D16,
                                   REQERR_WIDE, true, REQERR_WIDE);
    /* draft-18 Section 15's unknown-code rule is applied by the SOURCE session:
     * an unregistered code is surfaced as INTERNAL_ERROR before the relay ever
     * sees it. Recorded so the normalization is not mistaken for relay loss. */
    (void)run_upstream_error_width("reqerr_d18_source_normalizes", D18, D16,
                                   REQERR_WIDE, true, 0x0u);
    /* Relay-authored: the verdict's own code reaches the subscriber, full
     * width, with nothing normalizing it on the way in. */
    (void)run_authored_reqerr("reqerr_authored_wide_d16", D16, REQERR_WIDE,
                              true, REQERR_WIDE);
    /* A draft-18 RECEIVER applies Section 15's unknown-code rule to what it
     * reads, so the same authored code surfaces there as INTERNAL_ERROR. The
     * relay still emitted all 64 bits; the normalization is the peer's. */
    (void)run_authored_reqerr("reqerr_authored_wide_d18_receiver_normalizes",
                              D18, REQERR_WIDE, true, 0x0u);
    /* Above draft-16's varint ceiling: refused, never replaced. */
    (void)run_authored_reqerr("reqerr_authored_unrepresentable_d16", D16,
                              REQERR_UNREP, false, 0);

    return g_failures != 0;
}
