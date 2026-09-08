/*
 * The irreducible boundary: one relay, two transports, one runtime.
 *
 * Everything else about the dual listener is proven deterministically -- the
 * config, the shard plan, the wiring order. What no sans-I/O test can show is
 * that an object published over one transport actually reaches a subscriber
 * that arrived over the other. That is this test, over real localhost QUIC,
 * driving the SHIPPING lane pumps of both facades against ONE shard runtime.
 *
 * Two directions, because the interesting failure is asymmetric: a relay could
 * plausibly forward raw->WebTransport while dropping WebTransport->raw.
 */

#include "../cli/config.h"
#include <moq/relay/moqr_shards.h>

#include <moq/msquic_managed.h>
#include <moq/rcbuf.h>
#include <moq/session.h>
#include <moq/wtquic_msquic_managed.h>

#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

extern moq_msquic_lane_pump_fn moqr_test_lanes_pump(void);
extern moq_wtquic_msquic_lane_pump_fn moqr_test_wt_lanes_pump(void);
extern void *moqr_test_mk_lanes_ctx(moqr_shards_t *shards, uint32_t lanes);
extern void moqr_test_lanes_ctx_plan_dual(void *ctx, uint32_t raw, uint32_t wt);
extern void moqr_test_lanes_ctx_set_raw(void *ctx, moq_msquic_managed_t *raw);
extern uint64_t moqr_test_lanes_ctx_pump_turns(void *ctx, uint32_t lane);
extern uint64_t moqr_test_lanes_ctx_wake_pushes(void *ctx, uint32_t lane);

static int g_failures;
#define CHECK(x) do { if (!(x)) { \
    fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #x); \
    g_failures++; } } while (0)

#define NS_PART_0 "dual"
#define TRACK_NAME "t"
static const uint8_t kPayload[] = "cross-transport";

/* -- the client peers ------------------------------------------------------
 * Both transports carry the same moq_session_t, so the protocol side is one
 * body; only creation and pumping differ. */

typedef struct peer {
    moq_session_t *sess;
    bool           publisher;
    /* publisher */
    bool           announced;
    bool           accepted;
    bool           opened;
    bool           wrote;
    moq_subgroup_handle_t sg;
    moq_subscription_t    up_sub;
    /* subscriber */
    bool           subscribed;
    atomic_int     received;
} peer_t;

static void
peer_drive(peer_t *p, uint64_t now_us)
{
    if (p->sess == NULL ||
        moq_session_state(p->sess) != MOQ_SESS_ESTABLISHED) {
        return;
    }
    moq_bytes_t nsp[1] = { MOQ_BYTES_LITERAL(NS_PART_0) };
    if (p->publisher && !p->announced) {
        moq_publish_namespace_cfg_t pc;
        memset(&pc, 0, sizeof(pc));
        moq_publish_namespace_cfg_init(&pc);
        pc.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 1 };
        moq_announcement_t ann;
        if (moq_session_publish_namespace(p->sess, &pc, now_us, &ann) ==
            MOQ_OK) {
            p->announced = true;
        }
    }
    if (!p->publisher && !p->subscribed) {
        moq_subscribe_cfg_t sc;
        memset(&sc, 0, sizeof(sc));
        moq_subscribe_cfg_init(&sc);
        sc.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 1 };
        sc.track_name = MOQ_BYTES_LITERAL(TRACK_NAME);
        sc.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
        moq_subscription_t sh;
        if (moq_session_subscribe(p->sess, &sc, now_us, &sh) == MOQ_OK) {
            p->subscribed = true;
        }
    }

    moq_event_t evs[8];
    size_t n;
    while ((n = moq_session_poll_events(p->sess, evs, 8)) > 0) {
        for (size_t e = 0; e < n; e++) {
            if (evs[e].kind == MOQ_EVENT_SUBSCRIBE_REQUEST && p->publisher) {
                moq_accept_subscribe_cfg_t ac;
                memset(&ac, 0, sizeof(ac));
                moq_accept_subscribe_cfg_init(&ac);
                if (moq_session_accept_subscribe(
                        p->sess, evs[e].u.subscribe_request.sub, &ac,
                        now_us) == MOQ_OK) {
                    p->up_sub = evs[e].u.subscribe_request.sub;
                    p->accepted = true;
                }
            } else if (evs[e].kind == MOQ_EVENT_SUBSCRIBE_ERROR &&
                       !p->publisher) {
                /* The relay refuses until it has a route for the namespace,
                 * so a refusal is a "not yet", not a verdict: clear the latch
                 * and let the next pump ask again. */
                p->subscribed = false;
            } else if (evs[e].kind == MOQ_EVENT_OBJECT_RECEIVED &&
                       !p->publisher) {
                atomic_fetch_add(&p->received, 1);
            }
            moq_event_cleanup(&evs[e]);
        }
    }

    if (p->publisher && p->accepted && !p->wrote) {
        if (!p->opened) {
            moq_subgroup_cfg_t sgc;
            memset(&sgc, 0, sizeof(sgc));
            moq_subgroup_cfg_init(&sgc);
            sgc.group_id = 1;
            sgc.publisher_priority = 100;
            if (moq_session_open_subgroup(p->sess, p->up_sub, &sgc, now_us,
                                          &p->sg) == MOQ_OK) {
                p->opened = true;
            }
        }
        if (p->opened) {
            moq_rcbuf_t *pl = NULL;
            if (moq_rcbuf_create(moq_alloc_default(), kPayload,
                                 sizeof(kPayload) - 1, &pl) == MOQ_OK &&
                moq_session_write_object(p->sess, p->sg, 0, pl, now_us) ==
                    MOQ_OK) {
                p->wrote = true;
            }
            if (pl != NULL) {
                moq_rcbuf_decref(pl);
            }
        }
    }
}

static int
raw_client_pump(moq_msquic_managed_t *m, moq_msquic_managed_lane_t *lane,
                uint64_t now_us, void *vctx)
{
    (void)lane;
    peer_t *p = vctx;
    p->sess = moq_msquic_managed_session(m);
    peer_drive(p, now_us);
    return 0;
}

static int
wt_client_pump(moq_wtquic_msquic_managed_t *m,
               moq_wtquic_msquic_managed_lane_t *lane, uint64_t now_us,
               void *vctx)
{
    (void)lane;
    peer_t *p = vctx;
    p->sess = moq_wtquic_msquic_managed_session(m);
    peer_drive(p, now_us);
    return 0;
}

/* -- the scenario ---------------------------------------------------------- */

/* raw_publishes: raw publisher -> WebTransport subscriber, else the reverse. */
static int
run_cross(const char *cert, const char *key, bool raw_publishes)
{
    int before = g_failures;
    /* Compose the runtime exactly as `serve` does, from a real dual config:
     * the budgets, the cross-shard pools and the per-shard clamp all come from
     * the production builder rather than from anything invented here. */
    /* The ports here only satisfy the schema: both listeners below bind an
     * ephemeral port, so nothing in this test depends on a fixed one. */
    static const char *const kCfg =
        "{\"listener\":{\"host\":\"127.0.0.1\",\"port\":4433,"
        "\"cert\":\"c\",\"key\":\"k\",\"lanes\":1,\"versions\":[18]},"
        "\"webtransport\":{\"host\":\"127.0.0.1\",\"port\":4443,"
        "\"cert\":\"c\",\"key\":\"k\",\"lanes\":1,\"versions\":[18]}}";
    moqr_cli_config_t ccfg;
    char cerr[192] = { 0 };
    CHECK(moqr_cli_config_parse(kCfg, strlen(kCfg), &ccfg, cerr,
                                sizeof(cerr)) == MOQR_OK);
    moqr_cli_shard_plan_t plan;
    CHECK(moqr_cli_shard_plan(&ccfg, &plan, cerr, sizeof(cerr)) == MOQR_OK);
    CHECK(plan.total_shards == 2);
    CHECK(plan.raw_count == 1);
    CHECK(plan.wt_count == 1);

    moqr_shards_cfg_t scfg;
    moqr_shards_t *shards = NULL;
    uint32_t serve_max_conns = 0;
    CHECK(moqr_cli_serve_compose(&ccfg, moq_alloc_default(), &scfg,
                                 &serve_max_conns) == MOQR_OK);
    scfg.live_visibility = true;   /* lanes step independently, no barrier */
    CHECK(scfg.shards == 2);
    CHECK(moqr_shards_create(&scfg, &shards) == MOQR_OK);
    if (shards == NULL) {
        return g_failures - before;
    }
    void *ctx = moqr_test_mk_lanes_ctx(shards, 2);
    CHECK(ctx != NULL);
    if (ctx == NULL) {
        moqr_shards_destroy(shards);
        return g_failures - before;
    }
    moqr_test_lanes_ctx_plan_dual(ctx, 1, 1);

    /* the relay's raw listener */
    moq_msquic_managed_cfg_t rc;
    moq_msquic_managed_cfg_init_sized(&rc, sizeof(rc));
    rc.alloc = moq_alloc_default();
    rc.perspective = MOQ_PERSPECTIVE_SERVER;
    rc.host = "127.0.0.1";
    rc.port = 0;                   /* ephemeral: no fixed port to collide */
    rc.cert_path = cert;
    rc.key_path = key;
    rc.version = MOQ_VERSION_DRAFT_18;   /* match the WebTransport default */
    rc.lane_count = 1;
    rc.max_connections = serve_max_conns;
    rc.streaming_objects = false;
    rc.on_lane_pump = moqr_test_lanes_pump();
    rc.on_lane_pump_user = ctx;
    moq_msquic_managed_t *relay_raw = NULL;
    CHECK(moq_msquic_managed_create(&rc, &relay_raw) == MOQ_OK);

    /* the relay's WebTransport listener, on the SAME runtime */
    moq_wtquic_msquic_managed_cfg_t wc;
    moq_wtquic_msquic_managed_cfg_init_sized(&wc, sizeof(wc));
    wc.alloc = moq_alloc_default();
    wc.perspective = MOQ_PERSPECTIVE_SERVER;
    wc.host = "127.0.0.1";
    wc.port = 0;
    wc.cert_path = cert;
    wc.key_path = key;
    wc.lane_count = 1;
    wc.max_connections = serve_max_conns;
    wc.wt_path = "/moq";
    wc.streaming_objects = false;
    wc.on_lane_pump = moqr_test_wt_lanes_pump();
    wc.on_lane_pump_user = ctx;
    moq_wtquic_msquic_managed_t *relay_wt = NULL;
    CHECK(moq_wtquic_msquic_managed_create(&wc, &relay_wt) == MOQ_OK);
    if (relay_raw == NULL || relay_wt == NULL) {
        if (relay_wt != NULL) {
            (void)moq_wtquic_msquic_managed_stop(relay_wt);
            moq_wtquic_msquic_managed_destroy(relay_wt);
        }
        if (relay_raw != NULL) {
            (void)moq_msquic_managed_stop(relay_raw);
            moq_msquic_managed_destroy(relay_raw);
        }
        free(ctx);
        moqr_shards_destroy(shards);
        return g_failures - before;
    }
    moqr_test_lanes_ctx_set_raw(ctx, relay_raw);

    const uint16_t raw_port = moq_msquic_managed_port(relay_raw);
    const uint16_t wt_port = moq_wtquic_msquic_managed_port(relay_wt);
    CHECK(raw_port != 0);
    CHECK(wt_port != 0);

    /* the two clients, one on each transport */
    peer_t raw_peer, wt_peer;
    memset(&raw_peer, 0, sizeof(raw_peer));
    memset(&wt_peer, 0, sizeof(wt_peer));
    raw_peer.publisher = raw_publishes;
    wt_peer.publisher = !raw_publishes;

    moq_msquic_managed_cfg_t crc;
    moq_msquic_managed_cfg_init_sized(&crc, sizeof(crc));
    crc.alloc = moq_alloc_default();
    crc.perspective = MOQ_PERSPECTIVE_CLIENT;
    crc.host = "127.0.0.1";
    crc.port = raw_port;
    crc.version = MOQ_VERSION_DRAFT_18;
    crc.insecure_skip_verify = true;
    crc.streaming_objects = false;
    crc.on_lane_pump = raw_client_pump;
    crc.on_lane_pump_user = &raw_peer;
    moq_msquic_managed_t *craw = NULL;
    CHECK(moq_msquic_managed_create(&crc, &craw) == MOQ_OK);

    moq_wtquic_msquic_managed_cfg_t cwc;
    moq_wtquic_msquic_managed_cfg_init_sized(&cwc, sizeof(cwc));
    cwc.alloc = moq_alloc_default();
    cwc.perspective = MOQ_PERSPECTIVE_CLIENT;
    cwc.host = "127.0.0.1";
    cwc.port = wt_port;
    cwc.insecure_skip_verify = true;
    cwc.wt_path = "/moq";
    cwc.streaming_objects = false;
    cwc.on_lane_pump = wt_client_pump;
    cwc.on_lane_pump_user = &wt_peer;
    moq_wtquic_msquic_managed_t *cwt = NULL;
    CHECK(moq_wtquic_msquic_managed_create(&cwc, &cwt) == MOQ_OK);

    /* Drive until the subscriber has the object. Bounded by attempts, and the
     * loop exits on the event rather than on elapsed time. */
    peer_t *sub = raw_publishes ? &wt_peer : &raw_peer;
    bool delivered = false;
    for (int i = 0; i < 600 && !delivered; i++) {
        if (craw != NULL) {
            (void)moq_msquic_managed_wake(craw);
            (void)moq_msquic_managed_wait(craw, 10 * 1000);
        }
        if (cwt != NULL) {
            (void)moq_wtquic_msquic_managed_wake(cwt);
            (void)moq_wtquic_msquic_managed_wait(cwt, 10 * 1000);
        }
        (void)moq_msquic_managed_wake(relay_raw);
        (void)moq_wtquic_msquic_managed_wake(relay_wt);
        delivered = atomic_load(&sub->received) > 0;
    }
    if (!delivered) {
        /* Say WHERE it stopped: a silent "not delivered" cannot distinguish a
         * handshake that never completed from a relay that never forwarded. */
        fprintf(stderr,
                "  raw peer: sess=%d announced=%d subscribed=%d accepted=%d "
                "opened=%d wrote=%d received=%d\n",
                raw_peer.sess != NULL, raw_peer.announced, raw_peer.subscribed,
                raw_peer.accepted, raw_peer.opened, raw_peer.wrote,
                atomic_load(&raw_peer.received));
        fprintf(stderr,
                "  wt  peer: sess=%d announced=%d subscribed=%d accepted=%d "
                "opened=%d wrote=%d received=%d\n",
                wt_peer.sess != NULL, wt_peer.announced, wt_peer.subscribed,
                wt_peer.accepted, wt_peer.opened, wt_peer.wrote,
                atomic_load(&wt_peer.received));
        fprintf(stderr,
                "  shard 0 (raw): turns=%llu cross-shard wakes=%llu\n",
                (unsigned long long)moqr_test_lanes_ctx_pump_turns(ctx, 0),
                (unsigned long long)moqr_test_lanes_ctx_wake_pushes(ctx, 0));
        fprintf(stderr,
                "  shard 1 (wt) : turns=%llu cross-shard wakes=%llu\n",
                (unsigned long long)moqr_test_lanes_ctx_pump_turns(ctx, 1),
                (unsigned long long)moqr_test_lanes_ctx_wake_pushes(ctx, 1));
    }
    CHECK(delivered);

    if (cwt != NULL) {
        (void)moq_wtquic_msquic_managed_stop(cwt);
        moq_wtquic_msquic_managed_destroy(cwt);
    }
    if (craw != NULL) {
        (void)moq_msquic_managed_stop(craw);
        moq_msquic_managed_destroy(craw);
    }
    (void)moq_wtquic_msquic_managed_stop(relay_wt);
    (void)moq_msquic_managed_stop(relay_raw);
    moq_wtquic_msquic_managed_destroy(relay_wt);
    moq_msquic_managed_destroy(relay_raw);
    free(ctx);
    moqr_shards_destroy(shards);
    return g_failures - before;
}

int
main(int argc, char **argv)
{
    if (argc < 4) {
        fprintf(stderr, "usage: %s <cert> <key> <raw_to_wt|wt_to_raw>\n",
                argv[0]);
        return 2;
    }
    const bool raw_publishes = strcmp(argv[3], "raw_to_wt") == 0;
    if (!raw_publishes && strcmp(argv[3], "wt_to_raw") != 0) {
        fprintf(stderr, "unknown scenario: %s\n", argv[3]);
        return 2;
    }
    (void)run_cross(argv[1], argv[2], raw_publishes);
    if (g_failures == 0) {
        printf("PASS: relay_dual_loopback %s\n", argv[3]);
        return 0;
    }
    return 1;
}
