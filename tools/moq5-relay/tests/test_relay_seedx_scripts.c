/*
 * The exact scripted arms — the mandatory proofs the seeded explorer's random
 * visitation may supplement but never replace. Six scripts, tracked
 * independently: d16 withdraw, d16 revoke, d18 withdraw, d18 revoke, d18
 * reject-without-processing, and the directed credit script.
 *
 * Everything runs sans-I/O over production code: the REAL relay lane pump
 * (cli/main.c compiled in with main renamed away) driving a REAL binding and
 * core, over the managed facade on the fake MsQuic table; each peer is a bare
 * sans-I/O session behind a REAL transport bridge over the shared fake
 * endpoint, connected by the byte shuttle. The load-bearing oracle for every
 * withdrawal is the downstream subscriber's RESPONSE STREAM, read as wire
 * frames from the tap: NAMESPACE (0x8) first, exactly one NAMESPACE_DONE
 * (0xE) after it with the exact suffix, none before, and no further NAMESPACE
 * until a fresh publication. Driver events are secondary evidence only.
 *
 * Spec authority (verified byte-for-byte, hashes recorded):
 *   /Users/jekyll/Projects/MoQ/Spec/draft-ietf-moq-transport-16.txt
 *     sha256 2174e50090f20801df4d21e16b9ec21abe593e6ba2a84e43142aabdeb47b2c18
 *     §6.1/6.2, §9.21 NAMESPACE 0x8, §9.22 PUBLISH_NAMESPACE_DONE 0x9,
 *     §9.23 NAMESPACE_DONE 0xE, §9.24 PUBLISH_NAMESPACE_CANCEL 0xC
 *   /Users/jekyll/Projects/MoQ/Spec/draft-ietf-moq-transport-18.txt
 *     sha256 9e6b32cb7797c151e9e127374c1291af3ed546b2d453cd5bbb15946977eeeeb6
 *     §6.2 withdrawal = cancelling the request, §3.3.2 reset/STOP_SENDING,
 *     §3.3.2 rejection-without-processing = REQUEST_ERROR + FIN,
 *     §10.16 NAMESPACE 0x8, §10.17 NAMESPACE_DONE 0xE
 */

#include <moq/relay/moqr_bind.h>
#include <moq/relay/moqr_shards.h>
#if defined(MOQR_RELAY_INSPECT) || defined(MOQR_BIND_TESTING)
#include <moq/relay/moqr_shards_test.h>
#endif
#include "../cli/conn_reap.h"

#include <moq/msquic_managed.h>
#include <moq/rcbuf.h>
#include <moq/session.h>

#include "support/fake_msq_managed.h"
#include "support/msq_test_seams.h"

#include "seedx_shuttle.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* the real production callbacks and context builders (cli/main.c compiled in
 * with main renamed away — the same recipe as test_relay_pump_branches) */
extern moq_msquic_lane_pump_fn moqr_test_single_pump(void);
extern moq_msquic_lane_pump_fn moqr_test_lanes_pump(void);
extern void *moqr_test_mk_serve_ctx(moqr_bind_t *bind, moqr_core_t *core,
                                    moqr_trace_t *trace);
extern void *moqr_test_mk_lanes_ctx(moqr_shards_t *shards, uint32_t lanes);

#include "seedx/seedx_ledger.h"

static int g_failures;

#define T_CHECK(expr)                                                     \
    do {                                                                  \
        if (!(expr)) {                                                    \
            printf("FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr);       \
            g_failures++;                                                 \
        }                                                                 \
    } while (0)

#define FRAME_NAMESPACE      0x8u
#define FRAME_REQUEST_ERROR  0x5u
#define FRAME_PUB_NS_DONE    0x9u
#define FRAME_PUB_NS_CANCEL  0xCu
#define FRAME_NAMESPACE_DONE 0xEu

/* -- rig --------------------------------------------------------------------- */

typedef struct rig {
    fake_mgd_t            fake;
    moqr_core_t          *core;
    moqr_bind_t          *bind;
    moqr_trace_t         *trace;
    void                 *ctx;
    moq_msquic_managed_t *m;
    moq_msquic_managed_lane_t *lane;
    uint64_t              now;
} rig_t;

/* Stream-through ingest/forwarding (the production CLI configuration) for
 * the arms that need a mid-flight downstream object; whole-object elsewhere. */
static bool g_rig_streaming = false;

static bool
rig_up(rig_t *r, moq_version_t version, moqr_authorize_fn authorize)
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
    scfg.version = version;
    scfg.streaming_objects = g_rig_streaming;
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

/* -- drivers ------------------------------------------------------------------ */

static const char *
alpn_of(moq_version_t v)
{
    return v == MOQ_VERSION_DRAFT_16 ? "moqt-16" : "moqt-18";
}

/* rounds of shuttle + lane pump until nothing moves (bounded) */
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

        if (did == MOQ_MSQ_TEST_STEP_PUMPED || did == MOQ_MSQ_TEST_STEP_TICKED) {
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

/* -- wire oracle -------------------------------------------------------------- *
 * The downstream subscriber's response-stream tap, decoded as frames. For d16
 * the response stream is the control stream; for d18 it is the response half
 * of the subscriber's SUBSCRIBE_NAMESPACE request bidi. */

typedef struct ns_oracle {
    int first_ns_idx;     /* frame index of the first NAMESPACE           */
    int first_done_idx;   /* frame index of the first NAMESPACE_DONE      */
    int ns_count;
    int done_count;
    int ns_after_done;    /* NAMESPACE frames after the first DONE        */
    bool suffix_ok;       /* the DONE's suffix matches expectation        */
} ns_oracle_t;

/* expected suffix: one field, `field`/`field_len` (the announced namespace
 * minus the subscribed prefix) */
static void
ns_oracle_scan(const shx_tap_t *tap, const uint8_t *field, size_t field_len,
               ns_oracle_t *o)
{
    shx_frame_t fr[64];
    int n = shx_scan(tap, fr, 64);

    memset(o, 0, sizeof(*o));
    o->first_ns_idx = -1;
    o->first_done_idx = -1;
    for (int i = 0; i < n; i++) {
        if (fr[i].type == FRAME_NAMESPACE) {
            o->ns_count++;
            if (o->first_ns_idx < 0) {
                o->first_ns_idx = i;
            }
            if (o->first_done_idx >= 0) {
                o->ns_after_done++;
            }
        } else if (fr[i].type == FRAME_NAMESPACE_DONE) {
            o->done_count++;
            if (o->first_done_idx < 0) {
                o->first_done_idx = i;
                /* payload: tuple count varint, then per-field len + bytes */
                uint64_t cnt = 0, flen = 0;
                size_t off = shx_varint(fr[i].payload, fr[i].len, &cnt);
                size_t l2 = off > 0
                                ? shx_varint(fr[i].payload + off,
                                             fr[i].len - off, &flen)
                                : 0;

                o->suffix_ok = off > 0 && l2 > 0 && cnt == 1 &&
                               flen == field_len &&
                               fr[i].len >= off + l2 + field_len &&
                               memcmp(fr[i].payload + off + l2, field,
                                      field_len) == 0;
            }
        }
    }
}

/* find the subscriber's response tap: d16 = its control stream; d18 = the
 * first driver-opened bidi that carries a NAMESPACE frame */
static const shx_tap_t *
response_tap(const shx_driver_t *sub)
{
    for (int i = 0; i < SHX_MAX_STREAMS; i++) {
        const shx_tap_t *t = &sub->tap[i];

        if (!t->used) {
            continue;
        }
        shx_frame_t fr[64];
        int n = shx_scan(t, fr, 64);

        for (int k = 0; k < n; k++) {
            if (fr[k].type == FRAME_NAMESPACE ||
                fr[k].type == FRAME_NAMESPACE_DONE) {
                return t;
            }
        }
    }
    return NULL;
}

/* -- shared script skeleton --------------------------------------------------- *
 * Establish a publisher and a downstream SUBSCRIBE_NAMESPACE subscriber,
 * announce ns {"live","alpha"} under subscribed prefix {"live"}, prove the
 * NAMESPACE reached the subscriber, then run `withdraw`, then assert the
 * 0163 oracle: NAMESPACE first, exactly one NAMESPACE_DONE with suffix
 * "alpha", none before, and no further NAMESPACE until a fresh publication
 * (which the script then performs and proves). */

typedef void (*withdraw_fn)(rig_t *r, shx_driver_t *pub,
                            moq_announcement_t ann);

/* the draft-specific upstream wire fact each script must additionally prove */
typedef enum {
    UP_D16_PUB_DONE,      /* driver emitted PUBLISH_NAMESPACE_DONE 0x9     */
    UP_D16_RELAY_CANCEL,  /* relay emitted PUBLISH_NAMESPACE_CANCEL 0xC    */
    UP_D18_DRIVER_CANCEL, /* driver cancelled its request stream (reset/
                           * stop op recorded at its endpoint), and NO d16
                           * control message was used                      */
    UP_D18_RELAY_CANCEL,  /* relay cancelled the request stream: the pub
                           * child's stream shutdown records carry the
                           * abort flags + error code, and NO 0xC frame    */
} upstream_kind_t;

static const uint8_t NS0[] = "live";
static const uint8_t NS1[] = "alpha";

static int count_type_in_ep_writes(const shx_driver_t *d, uint64_t stream_id,
                                   uint64_t type);

static int
count_type_in_child_tap(const shx_driver_t *d, uint64_t type)
{
    int c = 0;

    for (int i = 0; i < SHX_MAX_STREAMS; i++) {
        if (!d->tap[i].used) {
            continue;
        }
        shx_frame_t fr[64];
        int n = shx_scan(&d->tap[i], fr, 64);

        for (int k = 0; k < n; k++) {
            if (fr[k].type == type) {
                c++;
            }
        }
    }
    return c;
}

static int
count_ep_aborts(const shx_driver_t *d)
{
    int c = 0;

    for (size_t i = 0; i < d->ep.count; i++) {
        if (d->ep.ops[i].kind == FAKE_OP_RESET ||
            d->ep.ops[i].kind == FAKE_OP_STOP) {
            c++;
        }
    }
    return c;
}

static int
count_child_stream_aborts(const shx_driver_t *d)
{
    const fake_msq_t *f = &d->child->fake;
    int c = 0;

    for (int i = 0; i < f->stream_count; i++) {
        c += f->streams[i].shutdown_calls;
    }
    return c;
}

static void
assert_upstream(const shx_driver_t *pub, upstream_kind_t kind)
{
    switch (kind) {
    case UP_D16_PUB_DONE:
        /* the driver's own control-stream writes carry exactly one 0x9 */
        T_CHECK(count_type_in_ep_writes(pub, pub->ctrl_bidi_id,
                                        FRAME_PUB_NS_DONE) == 1);
        break;
    case UP_D16_RELAY_CANCEL:
        /* the relay's emission to the publisher: exactly one 0xC frame */
        T_CHECK(count_type_in_child_tap(pub, FRAME_PUB_NS_CANCEL) == 1);
        break;
    case UP_D18_DRIVER_CANCEL:
        /* request-stream cancellation, never a d16 control message */
        T_CHECK(count_ep_aborts(pub) >= 1);
        T_CHECK(count_type_in_ep_writes(pub, pub->ctrl_bidi_id,
                                        FRAME_PUB_NS_DONE) == 0);
        break;
    case UP_D18_RELAY_CANCEL:
        /* the relay aborts the request stream: the child's shutdown
         * records carry it; and never a 0xC control frame */
        T_CHECK(count_child_stream_aborts(pub) >= 1);
        T_CHECK(count_type_in_child_tap(pub, FRAME_PUB_NS_CANCEL) == 0);
        break;
    }
}

static int
run_withdraw_script(const char *label, moq_version_t version,
                    moqr_authorize_fn authorize, withdraw_fn withdraw,
                    upstream_kind_t upstream, bool fragment)
{
    int before = g_failures;
    rig_t r;
    shx_driver_t pub, sub;

    T_CHECK(rig_up(&r, version, authorize));
    if (r.m == NULL) {
        rig_down(&r);
        return g_failures - before;
    }
    T_CHECK(shx_driver_open(&pub, &r.fake, version, alpn_of(version)));
    T_CHECK(shx_driver_open(&sub, &r.fake, version, alpn_of(version)));
    pub.fragment = fragment;
    T_CHECK(establish(&r, &pub));
    T_CHECK(establish(&r, &sub));

    shx_driver_t *ds[2] = { &pub, &sub };

    /* downstream namespace interest */
    moq_bytes_t pfx[1] = { { NS0, sizeof(NS0) - 1 } };
    moq_subscribe_namespace_cfg_t nscfg;

    moq_subscribe_namespace_cfg_init(&nscfg);
    nscfg.track_namespace_prefix =
        (moq_namespace_t){ .parts = pfx, .count = 1 };
    nscfg.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
    moq_ns_sub_handle_t nsh;

    T_CHECK(moq_session_subscribe_namespace(sub.sess, &nscfg, sub.now,
                                            &nsh) == MOQ_OK);
    pump_rounds(&r, ds, 2, 8);

    /* announce */
    moq_bytes_t nsp[2] = { { NS0, sizeof(NS0) - 1 }, { NS1, sizeof(NS1) - 1 } };
    moq_publish_namespace_cfg_t pcfg;

    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    moq_announcement_t ann;

    T_CHECK(moq_session_publish_namespace(pub.sess, &pcfg, pub.now, &ann) ==
            MOQ_OK);
    pump_rounds(&r, ds, 2, 12);

    T_CHECK(shx_ev_count(&sub, MOQ_EVENT_NAMESPACE_FOUND) == 1);
    const shx_tap_t *tap = response_tap(&sub);

    T_CHECK(tap != NULL);
    ns_oracle_t o;

    if (tap != NULL) {
        ns_oracle_scan(tap, NS1, sizeof(NS1) - 1, &o);
        T_CHECK(o.ns_count == 1);
        T_CHECK(o.done_count == 0); /* no DONE may precede its NAMESPACE */
    }

    /* the withdrawal under test */
    withdraw(&r, &pub, ann);
    pump_rounds(&r, ds, 2, 12);
    assert_upstream(&pub, upstream);

    T_CHECK(tap != NULL);
    if (tap != NULL) {
        ns_oracle_scan(tap, NS1, sizeof(NS1) - 1, &o);
        T_CHECK(o.ns_count == 1);           /* no new NAMESPACE            */
        T_CHECK(o.done_count == 1);         /* exactly one NAMESPACE_DONE  */
        T_CHECK(o.first_ns_idx >= 0 && o.first_done_idx > o.first_ns_idx);
        T_CHECK(o.ns_after_done == 0);      /* nothing until a fresh pub   */
        T_CHECK(o.suffix_ok);               /* the exact suffix, "alpha"   */
    }
    T_CHECK(shx_ev_count(&sub, MOQ_EVENT_NAMESPACE_GONE) == 1);

    /* fresh publication: the NAMESPACE may (and must) appear again */
    moq_announcement_t ann2;

    T_CHECK(moq_session_publish_namespace(pub.sess, &pcfg, pub.now, &ann2) ==
            MOQ_OK);
    pump_rounds(&r, ds, 2, 12);
    if (tap != NULL) {
        ns_oracle_scan(tap, NS1, sizeof(NS1) - 1, &o);
        T_CHECK(o.ns_count == 2);
        T_CHECK(o.done_count == 1);
    }

    /* a SECOND withdraw + third publication: repeated request-stream
     * teardown must leave the publisher session healthy and the routing
     * fresh (a re-announce cycle is not a one-shot capability) */
    withdraw(&r, &pub, ann2);
    pump_rounds(&r, ds, 2, 12);
    moq_announcement_t ann3;

    T_CHECK(moq_session_publish_namespace(pub.sess, &pcfg, pub.now, &ann3) ==
            MOQ_OK);
    pump_rounds(&r, ds, 2, 12);
    if (tap != NULL) {
        ns_oracle_scan(tap, NS1, sizeof(NS1) - 1, &o);
        T_CHECK(o.ns_count == 3);
        T_CHECK(o.done_count == 2);
        T_CHECK(shx_ev_count(&pub, MOQ_EVENT_SESSION_CLOSED) == 0);
    }

    shx_driver_close(&pub);
    shx_driver_close(&sub);
    rig_down(&r);
    if (g_failures == before) {
        printf("PASS: %s\n", label);
    }
    return g_failures - before;
}

/* -- withdrawal variants ------------------------------------------------------ */

/* publisher-side withdrawal: the session API; the profile puts it on the wire
 * as PUBLISH_NAMESPACE_DONE 0x9 (d16) or the request-stream cancel (d18) */
static void
withdraw_publisher(rig_t *r, shx_driver_t *pub, moq_announcement_t ann)
{
    (void)r;
    pub->now += 1000;
    T_CHECK(moq_session_publish_namespace_done(pub->sess, ann, pub->now) ==
            MOQ_OK);
}

/* relay-side revocation: the production force-withdraw primitive */
static void
withdraw_relay(rig_t *r, shx_driver_t *pub, moq_announcement_t ann)
{
    (void)pub;
    (void)ann;
    moq_bytes_t nsp[2] = { { NS0, sizeof(NS0) - 1 }, { NS1, sizeof(NS1) - 1 } };
    moqr_ns_t ns = { nsp, 2 };

    r->now += 1000;
    T_CHECK(moqr_core_force_withdraw(r->core, ns, 0x10u, r->now) == MOQR_OK);
}

/* -- upstream wire checks ------------------------------------------------------ */

/* reassemble the driver's own writes on a stream and count frames of `type`
 * (e.g. the d16 PUBLISH_NAMESPACE_DONE the driver emitted). */
static int
count_type_in_ep_writes(const shx_driver_t *d, uint64_t stream_id,
                        uint64_t type)
{
    /* reassemble the driver's writes on the stream, then scan frames */
    shx_tap_t tmp;

    memset(&tmp, 0, sizeof(tmp));
    tmp.used = true;
    tmp.id = stream_id;
    for (size_t i = 0; i < d->ep.count; i++) {
        const fake_op_t *op = &d->ep.ops[i];

        if ((op->kind == FAKE_OP_WRITE || op->kind == FAKE_OP_OPEN_BIDI ||
             op->kind == FAKE_OP_OPEN_UNI) &&
            op->stream_id == stream_id &&
            tmp.len + op->data_len <= SHX_TAP_CAP) {
            memcpy(tmp.bytes + tmp.len, op->data, op->data_len);
            tmp.len += op->data_len;
        }
    }
    shx_frame_t fr[64];
    int n = shx_scan(&tmp, fr, 64);
    int c = 0;

    for (int i = 0; i < n; i++) {
        if (fr[i].type == type) {
            c++;
        }
    }
    return c;
}

/* -- the directed credit script ---------------------------------------------- *
 * K=2 shards over 2 lanes, admitted cross-shard demand, demand-channel entry
 * capacity 2 (a config constant). The requester lane is STALLED while the
 * publisher pushes 3 objects: 2 accepted into the channel (full), the 3rd
 * held by the owner; one requester step consumes, returns credit, and MUST
 * wake the producer lane (the OPEN-cell fact); the held object then flows.
 * The reference ledger's identities are asserted after every transition, and
 * the model runs on configuration arithmetic — production is the check side
 * (channel-bytes zero/nonzero, delivered counts, the wake bit), never the
 * model's source. */

#define CREDIT_CAP 2u

typedef struct credit_pub_state {
    moq_subscription_t    up_sub;
    moq_subgroup_handle_t sgh;
    bool                  sub_seen;
    bool                  sg_open;
} credit_pub_state_t;

static void
credit_pub_drive(shx_driver_t *pub, credit_pub_state_t *st)
{
    if (!st->sub_seen && pub->got_subscribe) {
        st->sub_seen = true;
        st->up_sub = pub->subscribe_handle;
        moq_accept_subscribe_cfg_t acfg;

        moq_accept_subscribe_cfg_init(&acfg);
        pub->now += 1000;
        T_CHECK(moq_session_accept_subscribe(pub->sess, st->up_sub, &acfg,
                                             pub->now) == MOQ_OK);
        moq_subgroup_cfg_t sgc;

        moq_subgroup_cfg_init(&sgc);
        sgc.group_id = 1;
        sgc.publisher_priority = 100;
        pub->now += 1000;
        T_CHECK(moq_session_open_subgroup(pub->sess, st->up_sub, &sgc,
                                          pub->now, &st->sgh) == MOQ_OK);
        st->sg_open = true;
    }
}

static bool
credit_push_one(shx_driver_t *pub, credit_pub_state_t *st, uint64_t oid)
{
    uint8_t body[32];
    moq_rcbuf_t *pl = NULL;

    memset(body, (int)(0xA0 + oid), sizeof(body));
    if (moq_rcbuf_create(moq_alloc_default(), body, sizeof(body), &pl) < 0) {
        return false;
    }
    pub->now += 1000;
    moq_result_t rc =
        moq_session_write_object(pub->sess, st->sgh, oid, pl, pub->now);

    moq_rcbuf_decref(pl);
    return rc == MOQ_OK;
}

static int
run_credit_script(void)
{
    int before = g_failures;
    rig_t r;
    shx_driver_t pub, sub;
    moqr_shards_t *shards = NULL;
    sx_pair_t pair; /* the reference ledger for the owner->requester pair */

    memset(&pair, 0, sizeof(pair));
    pair.capacity = CREDIT_CAP;

    /* K=2 rig with the production multi-lane pump */
    memset(&r, 0, sizeof(r));
    fake_mgd_init(&r.fake);
    moq_msq_test_api_override = fake_mgd_table(&r.fake);
    moq_msq_test_no_doorbell = true;

    moqr_shards_cfg_t shcfg;

    moqr_shards_cfg_init_sized(&shcfg, sizeof(shcfg), moq_alloc_default());
    shcfg.shards = 2;
    shcfg.admit_remote_demand = true;
    shcfg.live_visibility = true; /* per-shard stepping, no shared round */
    shcfg.demand_channel_entries = CREDIT_CAP;
    T_CHECK(moqr_shards_create(&shcfg, &shards) == MOQR_OK);
    r.ctx = moqr_test_mk_lanes_ctx(shards, 2);

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
    scfg.version = MOQ_VERSION_DRAFT_18;
    scfg.streaming_objects = true; /* the production forwarding ingest */
    scfg.lane_count = 2;
    scfg.max_connections = 4;
    scfg.on_lane_pump = moqr_test_lanes_pump();
    scfg.on_lane_pump_user = r.ctx;
    T_CHECK(moq_msquic_managed_create(&scfg, &r.m) == MOQ_OK);
    if (r.m == NULL) {
        moqr_shards_destroy(shards);
        moq_msq_test_api_override = NULL;
        moq_msq_test_no_doorbell = false;
        return g_failures - before;
    }
    moq_msquic_managed_lane_t *lane0 = moq_msquic_managed_lane(r.m, 0);
    moq_msquic_managed_lane_t *lane1 = moq_msquic_managed_lane(r.m, 1);

    /* pub -> lane 0 (round robin), sub -> lane 1 */
    T_CHECK(shx_driver_open(&pub, &r.fake, MOQ_VERSION_DRAFT_18, "moqt-18"));
    T_CHECK(shx_driver_open(&sub, &r.fake, MOQ_VERSION_DRAFT_18, "moqt-18"));

#define STEP0() do { (void)moq_msquic_lane_wake(lane0);                   \
                     (void)moq_msq_test_lane_step(lane0); } while (0)
#define STEP1() do { (void)moq_msquic_lane_wake(lane1);                   \
                     (void)moq_msq_test_lane_step(lane1); } while (0)
#define ROUNDS(n)                                                         \
    do {                                                                  \
        for (int r2 = 0; r2 < (n); r2++) {                                \
            (void)shx_round(&pub); (void)shx_round(&sub);                 \
            STEP0(); STEP1();                                             \
            (void)shx_round(&pub); (void)shx_round(&sub);                 \
        }                                                                 \
    } while (0)
#define ROUNDS_LANE0_ONLY(n)                                              \
    do {                                                                  \
        for (int r2 = 0; r2 < (n); r2++) {                                \
            (void)shx_round(&pub); STEP0(); (void)shx_round(&pub);        \
        }                                                                 \
    } while (0)

    for (int i = 0; i < 24 &&
                    (shx_ev_count(&pub, MOQ_EVENT_SETUP_COMPLETE) < 1 ||
                     shx_ev_count(&sub, MOQ_EVENT_SETUP_COMPLETE) < 1);
         i++) {
        ROUNDS(2);
    }
    T_CHECK(shx_ev_count(&pub, MOQ_EVENT_SETUP_COMPLETE) == 1);
    T_CHECK(shx_ev_count(&sub, MOQ_EVENT_SETUP_COMPLETE) == 1);

    /* announce on shard 0's namespace, subscribe cross-shard from lane 1 */
    moq_bytes_t nsp[2] = { { NS0, sizeof(NS0) - 1 },
                           { NS1, sizeof(NS1) - 1 } };
    moq_publish_namespace_cfg_t pcfg;

    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    moq_announcement_t ann;

    T_CHECK(moq_session_publish_namespace(pub.sess, &pcfg, pub.now, &ann) ==
            MOQ_OK);
    ROUNDS(8);

    moq_subscribe_cfg_t sc;

    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    sc.track_name = (moq_bytes_t){ (const uint8_t *)"t", 1 };
    sc.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    moq_subscription_t sh;

    T_CHECK(moq_session_subscribe(sub.sess, &sc, sub.now, &sh) == MOQ_OK);

    credit_pub_state_t ps;

    memset(&ps, 0, sizeof(ps));
    for (int i = 0; i < 24 && !ps.sg_open; i++) {
        ROUNDS(2);
        credit_pub_drive(&pub, &ps);
    }
    T_CHECK(ps.sg_open);
    /* settle the acceptance round-trip (owner ACK over the channel, the
     * requester's SUBSCRIBE_OK to the subscriber) BEFORE the stall begins */
    ROUNDS(10);
    T_CHECK(shx_ev_count(&sub, MOQ_EVENT_SUBSCRIBE_OK) == 1);

    /* STALL the requester lane, then push capacity + 1 objects. Each push is
     * one offer; the ledger decides accepted vs held from configuration
     * arithmetic and is checked against production after every transition. */
    T_CHECK(credit_push_one(&pub, &ps, 0));
    T_CHECK(sx_pair_offer(&pair));               /* accepted, occ 1 */
    T_CHECK(sx_pair_check(&pair) == 0);
    ROUNDS_LANE0_ONLY(4);

    T_CHECK(credit_push_one(&pub, &ps, 1));
    T_CHECK(sx_pair_offer(&pair));               /* accepted, occ 2 = cap */
    T_CHECK(sx_pair_check(&pair) == 0);
    ROUNDS_LANE0_ONLY(4);

    T_CHECK(!sx_pair_offer(&pair));              /* the 3rd offer is HELD */
    T_CHECK(credit_push_one(&pub, &ps, 2));
    T_CHECK(sx_pair_check(&pair) == 0 && pair.held_current == 1);
    ROUNDS_LANE0_ONLY(4);

    /* production check side: the channel holds data, nothing delivered yet,
     * and the producer lane is quiescent */
    T_CHECK(moqr_shards_debug_demand_channel_bytes(shards, 0, 1) > 0);
    T_CHECK(shx_ev_count(&sub, MOQ_EVENT_OBJECT_RECEIVED) == 0);

    moq_msq_test_lane_row_t l0;

    (void)moq_msq_test_lane_snapshot(lane0, &l0, NULL, 0);
    T_CHECK(!l0.ext_wake && !l0.wake_pending && !l0.pump_pending);

    /* ONE requester step consumes the channel and MUST wake the producer
     * lane: the credit wake, read from lane 0's pending bits BEFORE lane 0
     * runs again. */
    (void)moq_msquic_lane_wake(lane1);
    (void)moq_msq_test_lane_step(lane1);
    (void)shx_round(&sub);
    while (sx_pair_consume(&pair)) {             /* the drain empties it */
        T_CHECK(sx_pair_check(&pair) == 0);
    }
    (void)moq_msq_test_lane_snapshot(lane0, &l0, NULL, 0);
    T_CHECK(l0.ext_wake || l0.wake_pending || l0.pump_pending);

    /* the held offer retries into the freed credit and flows through */
    T_CHECK(sx_pair_retry(&pair));
    T_CHECK(sx_pair_check(&pair) == 0 && pair.held_current == 0 &&
            pair.accepted_total == 3);
    ROUNDS(10);
    while (sx_pair_consume(&pair)) {
        T_CHECK(sx_pair_check(&pair) == 0);
    }
    T_CHECK(shx_ev_count(&sub, MOQ_EVENT_OBJECT_RECEIVED) == 3);
    T_CHECK(pair.consumed_total == 3 && pair.occupancy == 0);
    T_CHECK(moqr_shards_debug_demand_channel_bytes(shards, 0, 1) == 0);

#undef STEP0
#undef STEP1
#undef ROUNDS
#undef ROUNDS_LANE0_ONLY

    shx_driver_close(&pub);
    shx_driver_close(&sub);
    (void)moq_msquic_managed_stop(r.m);
    moq_msquic_managed_destroy(r.m);
    free(r.ctx);
    moqr_shards_destroy(shards);
    moq_msq_test_api_override = NULL;
    moq_msq_test_no_doorbell = false;
    if (g_failures == before) {
        printf("PASS: seedx_credit\n");
    }
    return g_failures - before;
}

/* deny PUBLISH_NAMESPACE at the relay without any application processing:
 * d18 Section 3.3.2 — REQUEST_ERROR then FIN, never a stream abort */
static void
deny_announce_authz(void *ctx, const moqr_auth_request_t *req,
                    moqr_auth_verdict_t *out)
{
    (void)ctx;
    memset(out, 0, sizeof(*out));
    out->decision = req->action == MOQR_AUTH_PUBLISH_NAMESPACE
                        ? MOQR_AUTH_DENY
                        : MOQR_AUTH_ALLOW;
    out->error_code = 0x10u;
    out->reason = MOQR_AUTH_REASON_POLICY;
}


/* -- subscribe / push / cancel: cancellation precedence ----------------- *
 * One delivered object proves the pipe, then the subscriber CANCELS. The
 * acknowledged cancellation must (a) propagate upstream -- the publisher
 * observes its subscription torn down -- and (b) end deliveries: pushes
 * after the settle never reach the subscriber. */
static int
run_cancel_script(void)
{
    int before = g_failures;
    rig_t r;
    shx_driver_t pub, sub;
    moqr_shards_t *shards = NULL;

    memset(&r, 0, sizeof(r));
    fake_mgd_init(&r.fake);
    moq_msq_test_api_override = fake_mgd_table(&r.fake);
    moq_msq_test_no_doorbell = true;

    moqr_shards_cfg_t shcfg;

    moqr_shards_cfg_init_sized(&shcfg, sizeof(shcfg), moq_alloc_default());
    shcfg.shards = 2;
    shcfg.admit_remote_demand = true;
    shcfg.live_visibility = true;
    shcfg.demand_channel_entries = 8; /* roomy: credit never interferes */
    T_CHECK(moqr_shards_create(&shcfg, &shards) == MOQR_OK);
    r.ctx = moqr_test_mk_lanes_ctx(shards, 2);

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
    scfg.version = MOQ_VERSION_DRAFT_18;
    scfg.lane_count = 2;
    scfg.max_connections = 4;
    scfg.on_lane_pump = moqr_test_lanes_pump();
    scfg.on_lane_pump_user = r.ctx;
    T_CHECK(moq_msquic_managed_create(&scfg, &r.m) == MOQ_OK);
    if (r.m == NULL) {
        moqr_shards_destroy(shards);
        moq_msq_test_api_override = NULL;
        moq_msq_test_no_doorbell = false;
        return g_failures - before;
    }
    moq_msquic_managed_lane_t *lane0 = moq_msquic_managed_lane(r.m, 0);
    moq_msquic_managed_lane_t *lane1 = moq_msquic_managed_lane(r.m, 1);

    T_CHECK(shx_driver_open(&pub, &r.fake, MOQ_VERSION_DRAFT_18, "moqt-18"));
    T_CHECK(shx_driver_open(&sub, &r.fake, MOQ_VERSION_DRAFT_18, "moqt-18"));

#define CROUNDS(n)                                                        \
    do {                                                                  \
        for (int r2 = 0; r2 < (n); r2++) {                                \
            (void)shx_round(&pub); (void)shx_round(&sub);                 \
            (void)moq_msquic_lane_wake(lane0);                            \
            (void)moq_msq_test_lane_step(lane0);                          \
            (void)moq_msquic_lane_wake(lane1);                            \
            (void)moq_msq_test_lane_step(lane1);                          \
            (void)shx_round(&pub); (void)shx_round(&sub);                 \
        }                                                                 \
    } while (0)

    for (int i = 0; i < 24 &&
                    (shx_ev_count(&pub, MOQ_EVENT_SETUP_COMPLETE) < 1 ||
                     shx_ev_count(&sub, MOQ_EVENT_SETUP_COMPLETE) < 1);
         i++) {
        CROUNDS(2);
    }
    T_CHECK(shx_ev_count(&pub, MOQ_EVENT_SETUP_COMPLETE) == 1);
    T_CHECK(shx_ev_count(&sub, MOQ_EVENT_SETUP_COMPLETE) == 1);

    moq_bytes_t nsp[2] = { { NS0, sizeof(NS0) - 1 },
                           { NS1, sizeof(NS1) - 1 } };
    moq_publish_namespace_cfg_t pcfg;

    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    moq_announcement_t ann;

    T_CHECK(moq_session_publish_namespace(pub.sess, &pcfg, pub.now, &ann) ==
            MOQ_OK);
    CROUNDS(8);

    moq_subscribe_cfg_t sc;

    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    sc.track_name = (moq_bytes_t){ (const uint8_t *)"t", 1 };
    sc.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    moq_subscription_t sh;

    T_CHECK(moq_session_subscribe(sub.sess, &sc, sub.now, &sh) == MOQ_OK);

    credit_pub_state_t ps;

    memset(&ps, 0, sizeof(ps));
    for (int i = 0; i < 24 && !ps.sg_open; i++) {
        CROUNDS(2);
        credit_pub_drive(&pub, &ps);
    }
    T_CHECK(ps.sg_open);
    CROUNDS(10);
    T_CHECK(shx_ev_count(&sub, MOQ_EVENT_SUBSCRIBE_OK) == 1);

    /* one delivered object proves the pipe end-to-end */
    T_CHECK(credit_push_one(&pub, &ps, 0));
    CROUNDS(10);
    T_CHECK(shx_ev_count(&sub, MOQ_EVENT_OBJECT_RECEIVED) == 1);

    /* the CANCEL, settled */
    T_CHECK(moq_session_unsubscribe(sub.sess, sh, sub.now) == MOQ_OK);
    CROUNDS(12);

    /* precedence, upstream half: the relay tears the upstream subscription
     * down -- the publisher observes it. */
    T_CHECK(shx_ev_count(&pub, MOQ_EVENT_UNSUBSCRIBED) == 1);

    /* precedence, downstream half: pushes after the acknowledged cancel
     * never reach the subscriber (the push itself may legitimately be
     * refused upstream -- only the subscriber count is the invariant) */
    (void)credit_push_one(&pub, &ps, 1);
    (void)credit_push_one(&pub, &ps, 2);
    CROUNDS(12);
    T_CHECK(shx_ev_count(&sub, MOQ_EVENT_OBJECT_RECEIVED) == 1);

#undef CROUNDS

    shx_driver_close(&pub);
    shx_driver_close(&sub);
    (void)moq_msquic_managed_stop(r.m);
    moq_msquic_managed_destroy(r.m);
    free(r.ctx);
    moqr_shards_destroy(shards);
    moq_msq_test_api_override = NULL;
    moq_msq_test_no_doorbell = false;
    if (g_failures == before) {
        printf("PASS: seedx_cancel\n");
    }
    return g_failures - before;
}

static int
run_d18_reject_script(void)
{
    int before = g_failures;
    rig_t r;
    shx_driver_t pub, sub;

    T_CHECK(rig_up(&r, MOQ_VERSION_DRAFT_18, deny_announce_authz));
    if (r.m == NULL) {
        rig_down(&r);
        return g_failures - before;
    }
    T_CHECK(shx_driver_open(&pub, &r.fake, MOQ_VERSION_DRAFT_18,
                            alpn_of(MOQ_VERSION_DRAFT_18)));
    T_CHECK(shx_driver_open(&sub, &r.fake, MOQ_VERSION_DRAFT_18,
                            alpn_of(MOQ_VERSION_DRAFT_18)));
    T_CHECK(establish(&r, &pub));
    T_CHECK(establish(&r, &sub));

    shx_driver_t *ds[2] = { &pub, &sub };
    moq_bytes_t pfx[1] = { { NS0, sizeof(NS0) - 1 } };
    moq_subscribe_namespace_cfg_t nscfg;

    moq_subscribe_namespace_cfg_init(&nscfg);
    nscfg.track_namespace_prefix =
        (moq_namespace_t){ .parts = pfx, .count = 1 };
    nscfg.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
    moq_ns_sub_handle_t nsh;

    T_CHECK(moq_session_subscribe_namespace(sub.sess, &nscfg, sub.now, &nsh) ==
            MOQ_OK);
    pump_rounds(&r, ds, 2, 8);

    moq_bytes_t nsp[2] = { { NS0, sizeof(NS0) - 1 }, { NS1, sizeof(NS1) - 1 } };
    moq_publish_namespace_cfg_t pcfg;

    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    moq_announcement_t ann;

    T_CHECK(moq_session_publish_namespace(pub.sess, &pcfg, pub.now, &ann) ==
            MOQ_OK);
    pump_rounds(&r, ds, 2, 12);

    /* the driver observes the rejection */
    T_CHECK(shx_ev_count(&pub, MOQ_EVENT_NAMESPACE_REJECTED) == 1);
    /* wire: REQUEST_ERROR (0x5) on the request stream, then FIN — and NO
     * stream abort: rejection-without-processing is FIN-shaped, distinct
     * from processed cancellation */
    int err_frames = count_type_in_child_tap(&pub, FRAME_REQUEST_ERROR);
    bool fin_on_err_stream = false;

    for (int i = 0; i < SHX_MAX_STREAMS; i++) {
        const shx_tap_t *t = &pub.tap[i];

        if (!t->used) {
            continue;
        }
        shx_frame_t fr[16];
        int n = shx_scan(t, fr, 16);

        for (int k = 0; k < n; k++) {
            if (fr[k].type == FRAME_REQUEST_ERROR && t->fin_seen) {
                fin_on_err_stream = true;
            }
        }
    }
    T_CHECK(err_frames == 1);
    T_CHECK(fin_on_err_stream);
    T_CHECK(count_child_stream_aborts(&pub) == 0);
    /* downstream: the denied namespace never reaches the subscriber */
    T_CHECK(shx_ev_count(&sub, MOQ_EVENT_NAMESPACE_FOUND) == 0);
    T_CHECK(count_type_in_child_tap(&sub, FRAME_NAMESPACE) == 0);

    shx_driver_close(&pub);
    shx_driver_close(&sub);
    rig_down(&r);
    if (g_failures == before) {
        printf("PASS: seedx_d18_reject\n");
    }
    return g_failures - before;
}


/* -- multipublisher succession (draft-16 SS8.3 / draft-18 SS9.3) --------- *
 * The compliance surface, recorded as five explicit observations plus ONE
 * exact expectation: the relay must handle the same Track from multiple
 * publishers — an implementation constrained to fewer publishers may
 * reject or cancel the extras, but silent acceptance followed by permanent
 * non-use is not an adequate capacity policy, and standing downstream
 * demand must not strand when the source ends while another publisher of
 * the namespace is live. Runs the core policy through the single binding
 * rig; the per-draft wire shape is the announce/withdraw encoding already
 * pinned by the withdrawal scripts.
 */
static void
multipub_drive_pub(shx_driver_t *pub, moq_subgroup_handle_t *sgh,
                   bool *sg_open, uint32_t group)
{
    if (pub->got_subscribe) {
        pub->got_subscribe = false;
        moq_accept_subscribe_cfg_t acfg;

        moq_accept_subscribe_cfg_init(&acfg);
        pub->now += 1000;
        (void)moq_session_accept_subscribe(pub->sess, pub->subscribe_handle,
                                           &acfg, pub->now);
        moq_subgroup_cfg_t sgc;

        moq_subgroup_cfg_init(&sgc);
        sgc.group_id = group;
        sgc.publisher_priority = 100;
        pub->now += 1000;
        if (moq_session_open_subgroup(pub->sess, pub->subscribe_handle,
                                      &sgc, pub->now, sgh) == MOQ_OK) {
            *sg_open = true;
        }
    }
}

static bool
multipub_push(shx_driver_t *pub, moq_subgroup_handle_t sgh, uint64_t oid)
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

static int
run_multipub_script(const char *label, moq_version_t version)
{
    int before = g_failures;
    rig_t r;
    shx_driver_t pa, pb, sub;

    T_CHECK(rig_up(&r, version, NULL));
    if (r.m == NULL) {
        rig_down(&r);
        return g_failures - before;
    }
    T_CHECK(shx_driver_open(&pa, &r.fake, version, alpn_of(version)));
    T_CHECK(shx_driver_open(&pb, &r.fake, version, alpn_of(version)));
    T_CHECK(shx_driver_open(&sub, &r.fake, version, alpn_of(version)));
    T_CHECK(establish(&r, &pa));
    T_CHECK(establish(&r, &pb));
    T_CHECK(establish(&r, &sub));

    shx_driver_t *ds[3] = { &pa, &pb, &sub };
    moq_bytes_t nsp[2] = { { NS0, sizeof(NS0) - 1 },
                           { NS1, sizeof(NS1) - 1 } };
    moq_publish_namespace_cfg_t pcfg;

    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    moq_announcement_t ann_a, ann_b;

    T_CHECK(moq_session_publish_namespace(pa.sess, &pcfg, pa.now, &ann_a) ==
            MOQ_OK);
    pump_rounds(&r, ds, 3, 8);

    /* AUDIT 1: a SECOND publisher announces the SAME namespace while the
     * first holds it — accepted or explicitly rejected? */
    T_CHECK(moq_session_publish_namespace(pb.sess, &pcfg, pb.now, &ann_b) ==
            MOQ_OK);
    pump_rounds(&r, ds, 3, 8);
    printf("AUDIT[%s] 1: concurrent second announce: accepted=%d "
           "rejected=%d cancelled=%d\n", label,
           shx_ev_count(&pb, 11 /* NAMESPACE_ACCEPTED */),
           shx_ev_count(&pb, MOQ_EVENT_NAMESPACE_REJECTED),
           shx_ev_count(&pb, MOQ_EVENT_NAMESPACE_CANCELLED));

    /* establish the downstream track against the standing winner (A) */
    moq_subscribe_cfg_t sc;

    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    sc.track_name = (moq_bytes_t){ (const uint8_t *)"t", 1 };
    sc.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    moq_subscription_t sh;

    T_CHECK(moq_session_subscribe(sub.sess, &sc, sub.now, &sh) == MOQ_OK);
    moq_subgroup_handle_t sgh_a;
    bool sg_a = false;

    for (int i = 0; i < 24 && !sg_a; i++) {
        pump_rounds(&r, ds, 3, 2);
        multipub_drive_pub(&pa, &sgh_a, &sg_a, 1);
    }
    T_CHECK(sg_a);
    T_CHECK(multipub_push(&pa, sgh_a, 0));
    pump_rounds(&r, ds, 3, 10);
    T_CHECK(shx_ev_count(&sub, MOQ_EVENT_OBJECT_RECEIVED) == 1);

    /* AUDIT 2: A withdraws its namespace — is A's established upstream
     * Track subscription still live (does a further push deliver)? */
    pa.now += 1000;
    T_CHECK(moq_session_publish_namespace_done(pa.sess, ann_a, pa.now) ==
            MOQ_OK);
    pump_rounds(&r, ds, 3, 10);
    bool a_push_ok = multipub_push(&pa, sgh_a, 1);

    pump_rounds(&r, ds, 3, 10);
    printf("AUDIT[%s] 2: post-withdraw push by A: write=%d delivered=%d "
           "(upstream sub live=%s)\n", label, a_push_ok,
           shx_ev_count(&sub, MOQ_EVENT_OBJECT_RECEIVED),
           a_push_ok && shx_ev_count(&sub, MOQ_EVENT_OBJECT_RECEIVED) == 2
               ? "yes" : "no");

    /* the withdrawn namespace is free again: B re-announces and must now
     * hold it — a LIVE alternative publisher exists from here on */
    pb.now += 1000;
    T_CHECK(moq_session_publish_namespace(pb.sess, &pcfg, pb.now, &ann_b) ==
            MOQ_OK);
    pump_rounds(&r, ds, 3, 8);
    printf("AUDIT[%s] 2b: B re-announce after A withdrew: accepted=%d "
           "rejected=%d\n", label,
           shx_ev_count(&pb, 11 /* NAMESPACE_ACCEPTED */),
           shx_ev_count(&pb, MOQ_EVENT_NAMESPACE_REJECTED));

    /* AUDIT 3: B is (still) announced and a matching established
     * downstream subscriber exists — does ANY request reach B, and does B
     * receive any explicit implementation-constraint response otherwise? */
    printf("AUDIT[%s] 3: requests to B while demand stands: subscribe=%d "
           "(explicit constraint response to B: rejected=%d "
           "cancelled=%d)\n", label, pb.got_subscribe ? 1 : 0,
           shx_ev_count(&pb, MOQ_EVENT_NAMESPACE_REJECTED),
           shx_ev_count(&pb, MOQ_EVENT_NAMESPACE_CANCELLED));

    /* AUDIT 4: A goes transport-terminal — is the standing downstream
     * demand retargeted to B without any nudge, or terminated, or left
     * silent? */
    int done_before = shx_ev_count(&sub, MOQ_EVENT_SUBSCRIBE_DONE);

    fake_mgd_deliver_peer_close(pa.child, 0);
    fake_mgd_deliver_shutdown_complete(pa.child);
    pa.child = NULL; /* the shuttle must not touch the closing conn */
    shx_driver_t *ds2[2] = { &pb, &sub };

    pump_rounds(&r, ds2, 2, 12);
    int sub_done = shx_ev_count(&sub, MOQ_EVENT_SUBSCRIBE_DONE) -
                   done_before;

    printf("AUDIT[%s] 4: after A terminal: retarget-to-B subscribe=%d, "
           "downstream SUBSCRIBE_DONE=%d, downstream silent=%s\n", label,
           pb.got_subscribe ? 1 : 0, sub_done,
           (!pb.got_subscribe && sub_done == 0) ? "YES (stranded)" : "no");

    /* With another publisher of the namespace live, the source's terminal
     * must RETARGET the standing demand: exactly one fresh upstream
     * SUBSCRIBE reaches B, the established subscriber sees no duplicate
     * SUBSCRIBE_OK and no terminal, and data RESUMES from B. */
    T_CHECK(pb.got_subscribe);
    T_CHECK(sub_done == 0);
    T_CHECK(shx_ev_count(&sub, MOQ_EVENT_SUBSCRIBE_OK) == 1);
    int objs_before_b = shx_ev_count(&sub, MOQ_EVENT_OBJECT_RECEIVED);
    moq_subgroup_handle_t sgh_b;
    bool sg_b = false;

    for (int i = 0; i < 24 && !sg_b; i++) {
        multipub_drive_pub(&pb, &sgh_b, &sg_b, 2);
        pump_rounds(&r, ds2, 2, 2);
    }
    T_CHECK(sg_b);
    T_CHECK(multipub_push(&pb, sgh_b, 0));
    pump_rounds(&r, ds2, 2, 10);
    T_CHECK(shx_ev_count(&sub, MOQ_EVENT_OBJECT_RECEIVED) ==
            objs_before_b + 1);
    /* and the transport path never doubles into a late terminal */
    pump_rounds(&r, ds2, 2, 6);
    T_CHECK(shx_ev_count(&sub, MOQ_EVENT_SUBSCRIBE_DONE) == done_before);

    if (pa.child != NULL) {
        shx_driver_close(&pa);
    }
    shx_driver_close(&pb);
    shx_driver_close(&sub);
    rig_down(&r);
    if (g_failures == before) {
        printf("PASS: %s\n", label);
    }
    return g_failures - before;
}

/* the NO-ALTERNATE arm: the source's transport terminal with no other
 * announced publisher must terminate every live subscriber explicitly —
 * exactly one SUBSCRIBE_DONE each, never silence, never a duplicate. */
static int
run_multipub_noalt_script(const char *label, moq_version_t version)
{
    int before = g_failures;
    rig_t r;
    shx_driver_t pa, sub;

    T_CHECK(rig_up(&r, version, NULL));
    if (r.m == NULL) {
        rig_down(&r);
        return g_failures - before;
    }
    T_CHECK(shx_driver_open(&pa, &r.fake, version, alpn_of(version)));
    T_CHECK(shx_driver_open(&sub, &r.fake, version, alpn_of(version)));
    T_CHECK(establish(&r, &pa));
    T_CHECK(establish(&r, &sub));

    shx_driver_t *ds[2] = { &pa, &sub };
    moq_bytes_t nsp[2] = { { NS0, sizeof(NS0) - 1 },
                           { NS1, sizeof(NS1) - 1 } };
    moq_publish_namespace_cfg_t pcfg;

    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    moq_announcement_t ann_a;

    T_CHECK(moq_session_publish_namespace(pa.sess, &pcfg, pa.now, &ann_a) ==
            MOQ_OK);
    pump_rounds(&r, ds, 2, 8);

    moq_subscribe_cfg_t sc;

    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    sc.track_name = (moq_bytes_t){ (const uint8_t *)"t", 1 };
    sc.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    moq_subscription_t sh;

    T_CHECK(moq_session_subscribe(sub.sess, &sc, sub.now, &sh) == MOQ_OK);
    moq_subgroup_handle_t sgh_a;
    bool sg_a = false;

    for (int i = 0; i < 24 && !sg_a; i++) {
        pump_rounds(&r, ds, 2, 2);
        multipub_drive_pub(&pa, &sgh_a, &sg_a, 1);
    }
    T_CHECK(sg_a);
    T_CHECK(multipub_push(&pa, sgh_a, 0));
    pump_rounds(&r, ds, 2, 10);
    T_CHECK(shx_ev_count(&sub, MOQ_EVENT_OBJECT_RECEIVED) == 1);

    fake_mgd_deliver_peer_close(pa.child, 0);
    fake_mgd_deliver_shutdown_complete(pa.child);
    pa.child = NULL;
    shx_driver_t *ds1[1] = { &sub };

    pump_rounds(&r, ds1, 1, 12);
    T_CHECK(shx_ev_count(&sub, MOQ_EVENT_SUBSCRIBE_DONE) == 1);
    /* ORACLE <label>_publish_done_status: the decoded PUBLISH_DONE status
     * for a source lost with no alternate is TRACK_ENDED (0x2) — "the track
     * is no longer being published" (d16 §13.4.3 / d18 §15.10.3). 0x6 is
     * TOO_FAR_BEHIND (d16) / EXPIRED (d18): a different meaning per draft
     * and never this one. */
    printf("ORACLE %s_publish_done_status decoded=0x%llx\n", label,
           (unsigned long long)sub.last_done_status);
    T_CHECK(sub.done_status_count == 1);
    T_CHECK(sub.last_done_status == 0x2);
    /* exactly once: further pumping never doubles the terminal */
    pump_rounds(&r, ds1, 1, 8);
    T_CHECK(shx_ev_count(&sub, MOQ_EVENT_SUBSCRIBE_DONE) == 1);

    shx_driver_close(&sub);
    rig_down(&r);
    if (g_failures == before) {
        printf("PASS: %s\n", label);
    }
    return g_failures - before;
}

/* The no-alternate terminal with a BEGUN downstream subgroup: stream-through
 * forwarding has opened (and started) the downstream object when the source
 * dies. The two wire terminals live in DIFFERENT registries and must be
 * pinned independently: the begun subgroup is RESET with the data-stream
 * reset code CANCELLED (0x1) — "the publisher ended the subscription, in
 * which case PUBLISH_DONE will have a more detailed status code" (d16
 * §10.4.3 / d18 §3.3.3) — while the SUBSCRIBE_DONE itself carries the
 * PUBLISH_DONE status TRACK_ENDED (0x2). */
static int
run_multipub_noalt_begun_script(const char *label, moq_version_t version)
{
    int before = g_failures;
    rig_t r;
    shx_driver_t pa, sub;

    g_rig_streaming = true;
    bool up = rig_up(&r, version, NULL);

    g_rig_streaming = false;
    T_CHECK(up);
    if (r.m == NULL) {
        rig_down(&r);
        return g_failures - before;
    }
    T_CHECK(shx_driver_open(&pa, &r.fake, version, alpn_of(version)));
    T_CHECK(shx_driver_open(&sub, &r.fake, version, alpn_of(version)));
    T_CHECK(establish(&r, &pa));
    T_CHECK(establish(&r, &sub));

    shx_driver_t *ds[2] = { &pa, &sub };
    moq_bytes_t nsp[2] = { { NS0, sizeof(NS0) - 1 },
                           { NS1, sizeof(NS1) - 1 } };
    moq_publish_namespace_cfg_t pcfg;

    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    moq_announcement_t ann_a;

    T_CHECK(moq_session_publish_namespace(pa.sess, &pcfg, pa.now, &ann_a) ==
            MOQ_OK);
    pump_rounds(&r, ds, 2, 8);

    moq_subscribe_cfg_t sc;

    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    sc.track_name = (moq_bytes_t){ (const uint8_t *)"t", 1 };
    sc.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    moq_subscription_t sh;

    T_CHECK(moq_session_subscribe(sub.sess, &sc, sub.now, &sh) == MOQ_OK);
    moq_subgroup_handle_t sgh_a;
    bool sg_a = false;

    for (int i = 0; i < 24 && !sg_a; i++) {
        pump_rounds(&r, ds, 2, 2);
        multipub_drive_pub(&pa, &sgh_a, &sg_a, 1);
    }
    T_CHECK(sg_a);
    T_CHECK(multipub_push(&pa, sgh_a, 0));
    pump_rounds(&r, ds, 2, 10);
    T_CHECK(shx_ev_count(&sub, MOQ_EVENT_OBJECT_RECEIVED) == 1);

    /* Begin object 1 (32 declared) and send only half: the relay's
     * stream-through forwarding begins the downstream object, which is
     * then mid-flight when the source dies. */
    pa.now += 1000;
    T_CHECK(moq_session_begin_object(pa.sess, sgh_a, 1, 32, pa.now) ==
            MOQ_OK);
    uint8_t half[16];
    moq_rcbuf_t *pl = NULL;

    memset(half, 0x61, sizeof(half));
    T_CHECK(moq_rcbuf_create(moq_alloc_default(), half, sizeof(half), &pl) >=
            0);
    pa.now += 1000;
    T_CHECK(moq_session_write_object_data(pa.sess, sgh_a, pl, pa.now) ==
            MOQ_OK);
    moq_rcbuf_decref(pl);
    pump_rounds(&r, ds, 2, 10);

    fake_mgd_deliver_peer_close(pa.child, 0);
    fake_mgd_deliver_shutdown_complete(pa.child);
    pa.child = NULL;
    shx_driver_t *ds1[1] = { &sub };

    pump_rounds(&r, ds1, 1, 12);
    T_CHECK(shx_ev_count(&sub, MOQ_EVENT_SUBSCRIBE_DONE) == 1);
    /* ORACLE <label>_publish_done_status: PUBLISH_DONE TRACK_ENDED (0x2). */
    printf("ORACLE %s_publish_done_status decoded=0x%llx\n", label,
           (unsigned long long)sub.last_done_status);
    T_CHECK(sub.done_status_count == 1);
    T_CHECK(sub.last_done_status == 0x2);
    /* ORACLE <label>_subgroup_reset_code: the begun downstream subgroup is
     * reset with CANCELLED (0x1) — its own registry, never the PUBLISH_DONE
     * status by numeric coincidence. */
    printf("ORACLE %s_subgroup_reset_code decoded=0x%llx count=%d\n", label,
           (unsigned long long)sub.last_sg_reset_code, sub.sg_reset_count);
    T_CHECK(sub.sg_reset_count == 1);
    T_CHECK(sub.last_sg_reset_code == 0x1);
    pump_rounds(&r, ds1, 1, 8);
    T_CHECK(shx_ev_count(&sub, MOQ_EVENT_SUBSCRIBE_DONE) == 1);

    shx_driver_close(&sub);
    rig_down(&r);
    if (g_failures == before) {
        printf("PASS: %s\n", label);
    }
    return g_failures - before;
}

/* Range completion: a bounded (ABSOLUTE_RANGE) subscription that consumes
 * its whole range retires with the PUBLISH_DONE status SUBSCRIPTION_ENDED
 * (0x3) — "the publisher reached the end of an associated subscription
 * filter" (d16 §13.4.3 / d18 §15.10.3). Status 0x0 is INTERNAL_ERROR in
 * that registry: a clean completion must never report it. */
static int
run_range_complete_script(const char *label, moq_version_t version)
{
    int before = g_failures;
    rig_t r;
    shx_driver_t pa, sub;

    T_CHECK(rig_up(&r, version, NULL));
    if (r.m == NULL) {
        rig_down(&r);
        return g_failures - before;
    }
    T_CHECK(shx_driver_open(&pa, &r.fake, version, alpn_of(version)));
    T_CHECK(shx_driver_open(&sub, &r.fake, version, alpn_of(version)));
    T_CHECK(establish(&r, &pa));
    T_CHECK(establish(&r, &sub));

    shx_driver_t *ds[2] = { &pa, &sub };
    moq_bytes_t nsp[2] = { { NS0, sizeof(NS0) - 1 },
                           { NS1, sizeof(NS1) - 1 } };
    moq_publish_namespace_cfg_t pcfg;

    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    moq_announcement_t ann_a;

    T_CHECK(moq_session_publish_namespace(pa.sess, &pcfg, pa.now, &ann_a) ==
            MOQ_OK);
    pump_rounds(&r, ds, 2, 8);

    moq_subscribe_cfg_t sc;

    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    sc.track_name = (moq_bytes_t){ (const uint8_t *)"t", 1 };
    sc.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE;
    sc.start_group = 1;
    sc.start_object = 0;
    sc.end_group = 1;   /* exactly group 1 */
    moq_subscription_t sh;

    T_CHECK(moq_session_subscribe(sub.sess, &sc, sub.now, &sh) == MOQ_OK);
    moq_subgroup_handle_t sgh_a;
    bool sg_a = false;

    for (int i = 0; i < 24 && !sg_a; i++) {
        pump_rounds(&r, ds, 2, 2);
        multipub_drive_pub(&pa, &sgh_a, &sg_a, 1);
    }
    T_CHECK(sg_a);
    T_CHECK(multipub_push(&pa, sgh_a, 0));
    pa.now += 1000;
    T_CHECK(moq_session_close_subgroup(pa.sess, sgh_a, pa.now) == MOQ_OK);
    /* Group 2 proves the range boundary: the bounded sub must complete at
     * the end of group 1, never receive group 2. */
    moq_subgroup_cfg_t sgc2;

    moq_subgroup_cfg_init(&sgc2);
    sgc2.group_id = 2;
    sgc2.publisher_priority = 100;
    moq_subgroup_handle_t sgh_b;

    pa.now += 1000;
    T_CHECK(moq_session_open_subgroup(pa.sess, pa.subscribe_handle, &sgc2,
                                      pa.now, &sgh_b) == MOQ_OK);
    T_CHECK(multipub_push(&pa, sgh_b, 0));
    pump_rounds(&r, ds, 2, 16);
    T_CHECK(shx_ev_count(&sub, MOQ_EVENT_OBJECT_RECEIVED) == 1);
    T_CHECK(shx_ev_count(&sub, MOQ_EVENT_SUBSCRIBE_DONE) == 1);
    /* ORACLE <label>_range_done_status: SUBSCRIPTION_ENDED (0x3). */
    printf("ORACLE %s_range_done_status decoded=0x%llx\n", label,
           (unsigned long long)sub.last_done_status);
    T_CHECK(sub.done_status_count == 1);
    T_CHECK(sub.last_done_status == 0x3);

    shx_driver_close(&pa);
    shx_driver_close(&sub);
    rig_down(&r);
    if (g_failures == before) {
        printf("PASS: %s\n", label);
    }
    return g_failures - before;
}

int moqr_multipub_main(void);
int moqr_readmission_main(void);

int
main(int argc, char **argv)
{
    if (argc >= 2 && strcmp(argv[1], "--multipub") == 0) {
        return moqr_multipub_main();
    }
    if (argc >= 2 && strcmp(argv[1], "--readmission") == 0) {
        return moqr_readmission_main();
    }
    (void)argv;
    /* the six mandatory scripts, tracked independently; the remaining four
     * report as OPEN until implemented in this phase */
    (void)run_withdraw_script("seedx_d16_withdraw", MOQ_VERSION_DRAFT_16,
                              NULL, withdraw_publisher, UP_D16_PUB_DONE,
                              false);
    (void)run_withdraw_script("seedx_d16_revoke", MOQ_VERSION_DRAFT_16,
                              NULL, withdraw_relay, UP_D16_RELAY_CANCEL,
                              false);
    (void)run_withdraw_script("seedx_d18_withdraw", MOQ_VERSION_DRAFT_18,
                              NULL, withdraw_publisher, UP_D18_DRIVER_CANCEL,
                              true);
    (void)run_withdraw_script("seedx_d18_revoke", MOQ_VERSION_DRAFT_18,
                              NULL, withdraw_relay, UP_D18_RELAY_CANCEL,
                              false);
    (void)run_d18_reject_script();
    (void)run_credit_script();
    (void)run_cancel_script();
    return g_failures;
}


/* -- post-revocation re-admission (the 0171-D family) -------------------- *
 * A source force-withdraw/revocation may terminate the old generation, but
 * a later ACCEPTED re-announcement plus a fresh downstream subscription
 * must create a fresh upstream demand and carry data — no stale manager,
 * mirror, demand, pump-sub, generation or terminal may suppress the new
 * generation. Same-publisher and replacement-publisher shapes, single- and
 * cross-shard, through a third generation. */
static int
run_readmission_script(const char *label, bool cross, bool replacement,
                       int generations)
{
    int before = g_failures;
    rig_t r;
    shx_driver_t p1, p2, sub;
    moqr_shards_t *shards = NULL;

    memset(&r, 0, sizeof(r));
    fake_mgd_init(&r.fake);
    moq_msq_test_api_override = fake_mgd_table(&r.fake);
    moq_msq_test_no_doorbell = true;

    moqr_shards_cfg_t shcfg;

    moqr_shards_cfg_init_sized(&shcfg, sizeof(shcfg), moq_alloc_default());
    shcfg.shards = 2;
    shcfg.admit_remote_demand = true;
    shcfg.live_visibility = true;
    shcfg.demand_channel_entries = 8;
    T_CHECK(moqr_shards_create(&shcfg, &shards) == MOQR_OK);
    r.ctx = moqr_test_mk_lanes_ctx(shards, 2);

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
    scfg.version = MOQ_VERSION_DRAFT_18;
    scfg.lane_count = 2;
    scfg.max_connections = 4;
    scfg.on_lane_pump = moqr_test_lanes_pump();
    scfg.on_lane_pump_user = r.ctx;
    T_CHECK(moq_msquic_managed_create(&scfg, &r.m) == MOQ_OK);
    if (r.m == NULL) {
        moqr_shards_destroy(shards);
        moq_msq_test_api_override = NULL;
        moq_msq_test_no_doorbell = false;
        return g_failures - before;
    }
    moq_msquic_managed_lane_t *lane0 = moq_msquic_managed_lane(r.m, 0);
    moq_msquic_managed_lane_t *lane1 = moq_msquic_managed_lane(r.m, 1);

    /* the round-robin puts driver 1 on lane 0 and driver 2 on lane 1: the
     * publishers take lane 0 (= owner shard of ns0); the subscriber takes
     * lane 1 for the cross-shard shape or lane 0 for single-shard */
    T_CHECK(shx_driver_open(&p1, &r.fake, MOQ_VERSION_DRAFT_18, "moqt-18"));
    if (cross) {
        T_CHECK(shx_driver_open(&sub, &r.fake, MOQ_VERSION_DRAFT_18,
                                "moqt-18"));
        T_CHECK(shx_driver_open(&p2, &r.fake, MOQ_VERSION_DRAFT_18,
                                "moqt-18"));
    } else {
        T_CHECK(shx_driver_open(&p2, &r.fake, MOQ_VERSION_DRAFT_18,
                                "moqt-18"));
        T_CHECK(shx_driver_open(&sub, &r.fake, MOQ_VERSION_DRAFT_18,
                                "moqt-18"));
    }

#define RROUNDS(n)                                                        \
    do {                                                                  \
        for (int r2 = 0; r2 < (n); r2++) {                                \
            (void)shx_round(&p1); (void)shx_round(&p2);                   \
            (void)shx_round(&sub);                                        \
            (void)moq_msquic_lane_wake(lane0);                            \
            (void)moq_msq_test_lane_step(lane0);                          \
            (void)moq_msquic_lane_wake(lane1);                            \
            (void)moq_msq_test_lane_step(lane1);                          \
            (void)shx_round(&p1); (void)shx_round(&p2);                   \
            (void)shx_round(&sub);                                        \
        }                                                                 \
    } while (0)

    for (int i = 0; i < 24 &&
                    (shx_ev_count(&p1, MOQ_EVENT_SETUP_COMPLETE) < 1 ||
                     shx_ev_count(&p2, MOQ_EVENT_SETUP_COMPLETE) < 1 ||
                     shx_ev_count(&sub, MOQ_EVENT_SETUP_COMPLETE) < 1);
         i++) {
        RROUNDS(2);
    }
    T_CHECK(shx_ev_count(&sub, MOQ_EVENT_SETUP_COMPLETE) == 1);

    moq_bytes_t nsp[1] = { { (const uint8_t *)"sched-ns-3", 10 } };
    moq_publish_namespace_cfg_t pcfg;

    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 1 };
    moqr_ns_t rns = { .parts = nsp, .count = 1 };

    int objs = 0;
    uint32_t group = 1;

    for (int gen = 0; gen < generations; gen++) {
        shx_driver_t *pub = (replacement && gen > 0) ? &p2 : &p1;
        moq_announcement_t ann;

        pub->now += 1000;
        T_CHECK(moq_session_publish_namespace(pub->sess, &pcfg, pub->now,
                                              &ann) == MOQ_OK);
        RROUNDS(8);

        /* fresh downstream subscription for this generation */
        moq_subscribe_cfg_t sc;

        moq_subscribe_cfg_init(&sc);
        sc.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 1 };
        sc.track_name = (moq_bytes_t){ (const uint8_t *)"t", 1 };
        sc.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
        moq_subscription_t sh;
        int ok_before = shx_ev_count(&sub, MOQ_EVENT_SUBSCRIBE_OK);

        sub.now += 1000;
        T_CHECK(moq_session_subscribe(sub.sess, &sc, sub.now, &sh) ==
                MOQ_OK);
        moq_subgroup_handle_t sgh;
        bool sg = false;

        for (int i = 0; i < 40 && !sg; i++) {
            RROUNDS(2);
            multipub_drive_pub(pub, &sgh, &sg, group);
        }
        RROUNDS(10); /* settle the acceptance round-trip downstream */
        /* generation gen: the fresh subscription must be ACCEPTED... */
        T_CHECK(sg);
        T_CHECK(shx_ev_count(&sub, MOQ_EVENT_SUBSCRIBE_OK) == ok_before + 1);
        /* ...and must CARRY DATA */
        if (sg) {
            T_CHECK(multipub_push(pub, sgh, 0));
            RROUNDS(10);
            objs++;
            T_CHECK(shx_ev_count(&sub, MOQ_EVENT_OBJECT_RECEIVED) >= objs);
        }
        group++;
        /* revoke this generation (the last one stays live) */
        if (gen + 1 < generations) {
            T_CHECK(moqr_core_force_withdraw(moqr_shards_core(shards, 0),
                                             rns, 0x10u, pub->now) ==
                    MOQR_OK);
            RROUNDS(10);
        }
    }
#undef RROUNDS

    shx_driver_close(&p1);
    shx_driver_close(&p2);
    shx_driver_close(&sub);
    (void)moq_msquic_managed_stop(r.m);
    moq_msquic_managed_destroy(r.m);
    free(r.ctx);
    moqr_shards_destroy(shards);
    moq_msq_test_api_override = NULL;
    moq_msq_test_no_doorbell = false;
    if (g_failures == before) {
        printf("PASS: %s\n", label);
    }
    return g_failures - before;
}

int
moqr_readmission_main(void)
{
    (void)run_readmission_script("readm_same_cross_g2", true, false, 2);
    (void)run_readmission_script("readm_repl_cross_g2", true, true, 2);
    (void)run_readmission_script("readm_same_cross_g3", true, false, 3);
    (void)run_readmission_script("readm_same_single_g2", false, false, 2);
    (void)run_readmission_script("readm_repl_single_g2", false, true, 2);
    return g_failures;
}

/* --multipub: the SS8.3/SS9.3 succession audit, a separate qualification
 * entry (expected RED while the compliance gap stands) */
int
moqr_multipub_main(void)
{
    (void)run_multipub_script("multipub_d16", MOQ_VERSION_DRAFT_16);
    (void)run_multipub_script("multipub_d18", MOQ_VERSION_DRAFT_18);
    (void)run_multipub_noalt_script("multipub_noalt_d16",
                                    MOQ_VERSION_DRAFT_16);
    (void)run_multipub_noalt_script("multipub_noalt_d18",
                                    MOQ_VERSION_DRAFT_18);
    (void)run_multipub_noalt_begun_script("multipub_noalt_begun_d16",
                                          MOQ_VERSION_DRAFT_16);
    (void)run_multipub_noalt_begun_script("multipub_noalt_begun_d18",
                                          MOQ_VERSION_DRAFT_18);
    (void)run_range_complete_script("range_complete_d16",
                                    MOQ_VERSION_DRAFT_16);
    (void)run_range_complete_script("range_complete_d18",
                                    MOQ_VERSION_DRAFT_18);
    return g_failures;
}
