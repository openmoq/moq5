/* Single-endpoint draft negotiation.
 *
 * MOQT performs version negotiation with ALPN on a QUIC connection
 * (draft-ietf-moq-transport-18 Section 3.1): the client offers its supported
 * MOQT ALPNs in preference order and the server selects one. Which entry a
 * multi-ALPN SERVER prefers is MsQuic behaviour, not a MOQT normative rule --
 * it is pinned by adapters/msquic/tests/test_msquic_multi_alpn.c
 * (t_raw_multi_offer_preference), and this test only requires that BOTH
 * offered drafts are servable from one endpoint. A relay that
 * offers a single ALPN can therefore serve only the peers that speak that one
 * draft; peers on the other draft are refused at the TLS handshake with the
 * no_application_protocol alert (RFC 9001 Section 8.1, QUIC error 0x178).
 *
 * This pins the capability: ONE listener endpoint accepts both a draft-18 and
 * a draft-16 client, and each connection is bound to the exact draft that was
 * negotiated for it -- never a permissive cross-draft decode.
 */

#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>

#include <moq/msquic_managed.h>

#include "config.h"
#include "conn_reap.h"
#include "versions.h"
#include <moq/session.h>

#include <moq/relay/moqr_shards.h>
#if defined(MOQR_RELAY_INSPECT) || defined(MOQR_BIND_TESTING)
#include <moq/relay/moqr_shards_test.h>
#endif

static int g_failures;

#define MV_CHECK(cond)                                                        \
    do {                                                                      \
        if (!(cond)) {                                                        \
            fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);   \
            g_failures++;                                                     \
        }                                                                     \
    } while (0)



/* The relay side of the endpoint: the SAME production machinery the loopback
 * fixture drives -- moqr_bind_conn_open() per accepted connection and
 * moqr_shards_step_shard() per lane turn. No idle/substitute pump, so a control
 * request from either client really enters relay bind and core. */
#define MV_CONN_OPENED MOQR_CONN_OPENED
#define MV_CONN_DEAD   MOQR_CONN_DEAD

typedef struct mv_relay_ctx {
    moqr_shards_t *shards;
    uint32_t       lanes;
} mv_relay_ctx_t;

static int
mv_relay_pump(moq_msquic_managed_t *m, moq_msquic_managed_lane_t *lane,
              uint64_t now_us, void *vctx)
{
    mv_relay_ctx_t *ctx = vctx;
    uint32_t shard = moq_msquic_lane_index(lane);
    if (shard >= moqr_shards_count(ctx->shards)) {
        return 0;
    }
    moqr_bind_t *bind = moqr_shards_bind(ctx->shards, (uint16_t)shard);
    for (moq_msquic_managed_conn_t *conn = moq_msquic_lane_next_conn(lane, NULL);
         conn != NULL; conn = moq_msquic_lane_next_conn(lane, conn)) {
        void *tag = moq_msquic_managed_conn_user(conn);
        if (tag == MV_CONN_DEAD) {
            continue;
        }
        moq_session_t *s = moq_msquic_managed_conn_session(conn);
        if (s == NULL) {
            continue;
        }
        if (tag == NULL) {
            if (moqr_bind_conn_open(bind, s,
                                    moq_msquic_managed_conn_negotiated_version(conn)) == MOQR_OK) {
                moq_msquic_managed_conn_set_user(conn, MV_CONN_OPENED);
            } else {
                moq_msquic_managed_conn_set_user(conn, MV_CONN_DEAD);
                moq_msquic_managed_conn_close(conn, 0);
            }
        }
    }
    uint64_t mask = 0;
    moqr_result_t rc =
        moqr_shards_step_shard(ctx->shards, (uint16_t)shard, now_us, &mask);
    for (uint32_t d = 0; mask != 0 && d < ctx->lanes; d++) {
        if (mask & (1ull << d)) {
            (void)moq_msquic_lane_wake(moq_msquic_managed_lane(m, d));
        }
    }
    if (rc != MOQR_OK) {
        return 1;
    }
    /* A binding this step detached must be retired now: nothing else wakes
     * this lane on its behalf. */
    return moqr_relay_reap_pass(bind, lane, NULL) ? 0 : 1;
}

/* One client: announce a distinct namespace once ESTABLISHED, then count the
 * relay's NAMESPACE_ACCEPTED for it. */
typedef struct mv_client_ctx {
    const char  *ns0;
    const char  *ns1;
    atomic_int   established;
    atomic_int   requested;
    atomic_int   accepted;
    atomic_int   errors;
    atomic_int   last_rc;
} mv_client_ctx_t;

static int
mv_client_pump(moq_msquic_managed_t *m, moq_msquic_managed_lane_t *lane,
               uint64_t now_us, void *vctx)
{
    mv_client_ctx_t *ctx = vctx;
    (void)lane;
    moq_session_t *s = moq_msquic_managed_session(m);
    if (s == NULL || moq_session_state(s) != MOQ_SESS_ESTABLISHED) {
        return 0;
    }
    atomic_store(&ctx->established, 1);
    if (atomic_load(&ctx->requested) == 0) {
        moq_bytes_t nsp[2];
        nsp[0] = (moq_bytes_t){ (const uint8_t *)ctx->ns0, (uint32_t)strlen(ctx->ns0) };
        nsp[1] = (moq_bytes_t){ (const uint8_t *)ctx->ns1, (uint32_t)strlen(ctx->ns1) };
        moq_publish_namespace_cfg_t pcfg;
        memset(&pcfg, 0, sizeof(pcfg));
        moq_publish_namespace_cfg_init(&pcfg);
        pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
        moq_announcement_t ann;
        moq_result_t prc = moq_session_publish_namespace(s, &pcfg, now_us, &ann);
        if (prc == MOQ_OK) {
            atomic_store(&ctx->requested, 1);
        } else if (prc != MOQ_ERR_WOULD_BLOCK) {
            atomic_store(&ctx->last_rc, (int)prc);
        }
        return 0;
    }
    moq_event_t evs[8];
    size_t n;
    while ((n = moq_session_poll_events(s, evs, 8)) > 0) {
        for (size_t e = 0; e < n; e++) {
            if (evs[e].kind == MOQ_EVENT_NAMESPACE_ACCEPTED) {
                atomic_fetch_add(&ctx->accepted, 1);
            } else if (evs[e].kind == MOQ_EVENT_NAMESPACE_CANCELLED) {
                atomic_fetch_add(&ctx->errors, 1);
            }
            moq_event_cleanup(&evs[e]);
        }
    }
    return 0;
}

/* Like mk_client(), but with a control pump that performs the namespace
 * exchange for this connection. */
static moq_msquic_managed_t *
mk_client_ctl(moq_version_t version, uint16_t port, mv_client_ctx_t *ctx)
{
    moq_msquic_managed_cfg_t cfg;
    moq_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.host = "127.0.0.1";
    cfg.port = port;
    cfg.insecure_skip_verify = true;
    cfg.version = version;
    cfg.on_lane_pump = mv_client_pump;
    cfg.on_lane_pump_user = ctx;
    moq_msquic_managed_t *t = NULL;
    if (moq_msquic_managed_create(&cfg, &t) != MOQ_OK) {
        return NULL;
    }
    return t;
}

/* One endpoint per lane count, configured exactly the way production does it:
 * parse a relay config, then hand it to the SAME moqr_cli_apply_versions() both
 * serve compositions call. Nothing here reimplements the mapping, so updating
 * only one serve path would still be caught by the mechanical call-site check
 * that accompanies this test. */
static int
run_endpoint(const char *cert, const char *key, uint32_t lanes)
{
    moqr_cli_config_t rcfg;
    char cerr[256];
    const char *json =
        "{\"listener\":{\"port\":4433,\"versions\":[18,16]}}";
    MV_CHECK(moqr_cli_config_parse(json, strlen(json), &rcfg, cerr,
                                   sizeof(cerr)) == MOQR_OK);
    MV_CHECK(rcfg.version_count == 2);

    moq_msquic_managed_cfg_t scfg;
    moq_msquic_managed_cfg_init_sized(&scfg, sizeof(scfg));
    scfg.alloc = moq_alloc_default();
    scfg.perspective = MOQ_PERSPECTIVE_SERVER;
    scfg.host = "127.0.0.1";
    scfg.port = 0;   /* ephemeral: the listener port is not what is under test */
    scfg.cert_path = cert;
    scfg.key_path = key;
    /* The same request-capacity grant both production serve paths set; without
     * it a peer's first control request is REQUEST_BLOCKED. */
    scfg.send_request_capacity = true;
    scfg.initial_request_capacity = 1024;
    scfg.streaming_objects = true;
    moqr_cli_apply_versions(&rcfg, &scfg);   /* the production mapping */
    MV_CHECK(scfg.version == 0);
    MV_CHECK(scfg.version_count == 2);
    MV_CHECK(scfg.versions != NULL);
    if (scfg.versions != NULL && scfg.version_count == 2) {
        MV_CHECK(scfg.versions[0] == MOQ_VERSION_DRAFT_18);
        MV_CHECK(scfg.versions[1] == MOQ_VERSION_DRAFT_16);
    }
    scfg.lane_count = lanes;
    scfg.max_connections = 4;

    /* A real relay behind the listener: one shard per lane, driven by the
     * production bind/shards pump. */
    moqr_shards_cfg_t shcfg;
    moqr_shards_cfg_init_sized(&shcfg, sizeof(shcfg), moq_alloc_default());
    shcfg.shards = (uint16_t)lanes;
    moqr_shards_t *shards = NULL;
    MV_CHECK(moqr_shards_create(&shcfg, &shards) == MOQR_OK);
    if (shards == NULL) {
        return 1;
    }
    mv_relay_ctx_t rctx = { shards, lanes };
    scfg.on_lane_pump = mv_relay_pump;
    scfg.on_lane_pump_user = &rctx;

    moq_msquic_managed_t *server = NULL;
    MV_CHECK(moq_msquic_managed_create(&scfg, &server) == MOQ_OK);
    if (server == NULL) {
        /* A listener that cannot start is a NAMED failure, and it must not
         * strand the shards this function already created. */
        moqr_shards_destroy(shards);
        return 1;
    }
    uint16_t port = moq_msquic_managed_port(server);
    MV_CHECK(port != 0);

    mv_client_ctx_t k18 = { "mv18", "cam", 0, 0, 0, 0, 0 };
    mv_client_ctx_t k16 = { "mv16", "cam", 0, 0, 0, 0, 0 };
    moq_msquic_managed_t *c18 = mk_client_ctl(MOQ_VERSION_DRAFT_18, port, &k18);
    moq_msquic_managed_t *c16 = mk_client_ctl(MOQ_VERSION_DRAFT_16, port, &k16);
    MV_CHECK(c18 != NULL);
    MV_CHECK(c16 != NULL);
    if (c18 == NULL || c16 == NULL) {
        /* Report and unwind: every later step dereferences both clients, so a
         * failed creation must end this arm rather than crash in it. */
        for (int i = 0; i < 2; i++) {
            moq_msquic_managed_t *c = (i == 0) ? c18 : c16;
            if (c != NULL) {
                (void)moq_msquic_managed_stop(c);
                moq_msquic_managed_destroy(c);
            }
        }
        (void)moq_msquic_managed_stop(server);
        moq_msquic_managed_destroy(server);
        moqr_shards_destroy(shards);
        return 1;
    }

    /* Bounded wait for both connections to land AND for each client's
     * PUBLISH_NAMESPACE to be answered by the relay. */
    for (int waited = 0; waited < 300; waited++) {   /* bounded, never a hang */
        if (moq_msquic_managed_conn_count(server) >= 2 &&
            atomic_load(&k18.accepted) >= 1 && atomic_load(&k16.accepted) >= 1) {
            break;
        }
        (void)moq_msquic_managed_wait(server, 50 * 1000);
    }

    size_t accepted = moq_msquic_managed_conn_count(server);
    bool f18 = (c18 != NULL) && moq_msquic_managed_is_fatal(c18);
    bool f16 = (c16 != NULL) && moq_msquic_managed_is_fatal(c16);
    fprintf(stderr,
            "endpoint(lanes=%u): accepted=%zu d18_fatal=%d(code=%llu) "
            "d16_fatal=%d(code=%llu)\n",
            lanes, accepted, (int)f18,
            (unsigned long long)(c18 ? moq_msquic_managed_fatal_code(c18) : 0),
            (int)f16,
            (unsigned long long)(c16 ? moq_msquic_managed_fatal_code(c16) : 0));

    /* The draft-18 client is the side the current single-ALPN listener
     * already serves; it must keep working. */
    MV_CHECK(!f18);
    /* The draft-16 client is the capability under test: on a single endpoint
     * that supports both drafts it must NOT be refused at the handshake
     * (RFC 9001 Section 8.1 would otherwise give 0x178). */
    MV_CHECK(!f16);
    MV_CHECK(accepted == 2);
    /* Each accepted connection is pinned to the draft ALPN selected for it,
     * and that selection is immutable for the connection's life. */
    MV_CHECK(moq_msquic_managed_negotiated_version(c18) ==
             MOQ_VERSION_DRAFT_18);
    MV_CHECK(moq_msquic_managed_negotiated_version(c16) ==
             MOQ_VERSION_DRAFT_16);

    /* A real control exchange reached relay bind/core on BOTH exact profiles:
     * each client's PUBLISH_NAMESPACE was authorized and answered exactly once,
     * with no cancellation. */
    fprintf(stderr,
            "  control(lanes=%u): d18 est=%d req=%d acc=%d err=%d rc=%d | "
            "d16 est=%d req=%d acc=%d err=%d rc=%d\n",
            lanes, atomic_load(&k18.established), atomic_load(&k18.requested),
            atomic_load(&k18.accepted), atomic_load(&k18.errors),
            atomic_load(&k18.last_rc),
            atomic_load(&k16.established), atomic_load(&k16.requested),
            atomic_load(&k16.accepted), atomic_load(&k16.errors),
            atomic_load(&k16.last_rc));
    MV_CHECK(atomic_load(&k18.established) == 1);
    MV_CHECK(atomic_load(&k16.established) == 1);
    MV_CHECK(atomic_load(&k18.accepted) == 1);
    MV_CHECK(atomic_load(&k16.accepted) == 1);
    MV_CHECK(atomic_load(&k18.errors) == 0);
    MV_CHECK(atomic_load(&k16.errors) == 0);

    for (int i = 0; i < 2; i++) {
        moq_msquic_managed_t *c = (i == 0) ? c18 : c16;
        if (c != NULL) {
            (void)moq_msquic_managed_stop(c);
            moq_msquic_managed_destroy(c);
        }
    }
    /* QUIESCENCE BEFORE DESTRUCTION. Destroying the shards would reclaim
     * anything still held, so the drain has to be asserted while the relay is
     * still alive: the two control exchanges must leave nothing pending. */
    for (int waited = 0; waited < 200; waited++) {
        if (moq_msquic_managed_conn_count(server) == 0) {
            break;
        }
        (void)moq_msquic_managed_wait(server, 50 * 1000);
    }
    size_t left = moq_msquic_managed_conn_count(server);
    bool srv_fatal = moq_msquic_managed_is_fatal(server);
    uint32_t pend_dem = 0, own_slots = 0, req_open = 0, chan = 0;
    for (uint32_t sh = 0; sh < lanes; sh++) {
        pend_dem += moqr_shards_debug_pending_demand(shards, (uint16_t)sh);
        own_slots += moqr_shards_debug_owner_progress_slots(shards,
                                                            (uint16_t)sh);
        req_open += moqr_shards_debug_requester_open_objects(shards,
                                                             (uint16_t)sh);
    }
    for (uint32_t i = 0; i < lanes; i++) {
        for (uint32_t d = 0; d < lanes; d++) {
            chan += moqr_shards_debug_demand_channel_pending(shards,
                                                             (uint16_t)i,
                                                             (uint16_t)d);
        }
    }
    uint64_t errs = 0;
    for (uint32_t sh = 0; sh < lanes; sh++) {
        moqr_bind_stats_t bst;
        moqr_bind_get_stats(moqr_shards_bind(shards, (uint16_t)sh), &bst);
        errs += bst.session_errors + bst.nsu_failed_closed +
                bst.ordered_failed_closed;
    }
    fprintf(stderr,
            "  drain(lanes=%u): conns=%zu fatal=%d pending_demand=%u "
            "owner_slots=%u req_open=%u chan=%u errs=%llu\n",
            lanes, left, (int)srv_fatal, pend_dem, own_slots, req_open, chan,
            (unsigned long long)errs);
    MV_CHECK(left == 0);          /* every relay-side connection is gone     */
    MV_CHECK(!srv_fatal);
    MV_CHECK(pend_dem == 0);      /* no demand left recorded                 */
    MV_CHECK(own_slots == 0);     /* no owner progress slot held             */
    MV_CHECK(req_open == 0);      /* no requester open object held           */
    MV_CHECK(chan == 0);          /* every directed channel drained          */
    MV_CHECK(errs == 0);          /* the control exchanges added no errors   */

    (void)moq_msquic_managed_stop(server);
    moq_msquic_managed_destroy(server);
    moqr_shards_destroy(shards);

    return 0;
}


int
main(int argc, char **argv)
{
    if (argc < 3) {
        fprintf(stderr, "usage: %s <cert.pem> <key.pem>\n", argv[0]);
        return 2;
    }
    /* lanes=1 and lanes=2 are the two production serve compositions, so a
     * change that reaches only one of them fails here. */
    (void)run_endpoint(argv[1], argv[2], 1);
    (void)run_endpoint(argv[1], argv[2], 2);
    if (g_failures == 0) {
        printf("PASS: one endpoint served moqt-18 and moqt-16 (lanes 1 and 2)\n");
    }
    return g_failures;
}
