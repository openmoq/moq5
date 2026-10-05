/*
 * A REAL moq_media_receiver_t behind the production relay binding, with a
 * real origin: the publisher facade on the origin connection retains the
 * MSF catalog generation {0: catalog JSON}; the receiver (driven through
 * its test pump, no network) subscribes the catalog track LargestObject
 * through the relay, issues its Relative Joining FETCH(0), and must reach
 * TRACK_ADDED and CATALOG_READY from the forwarded response -- exactly one
 * fetch, no fatal, and no further track event afterwards (no catalog
 * mutation or refresh). Both drafts.
 *
 * The scripted NOT_SUPPORTED control (test_media_receiver_fetch_catalog_rejected)
 * stays as the receiver's view of a relay that rejects joining fetches; this
 * row is the receiver's view of one that forwards them.
 */
#include <moq/media_receiver.h>
#include <moq/msf.h>
#include <moq/sim.h>
#include <moq/session.h>
#include <moq/publisher.h>
#include <moq/relay/relay.h>
#include <moq/relay/moqr_bind.h>
#include "test_support.h"

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <stdio.h>

static int failures = 0;

/* -- Test seam (media_receiver.c, MOQ_MEDIA_RECEIVER_TESTING) ----------- */
moq_media_receiver_t *moq_media_receiver_test_new_cfg(
    const moq_media_receiver_cfg_t *cfg);
void moq_media_receiver_test_free(moq_media_receiver_t *r);
void moq_media_receiver_test_pump(moq_media_receiver_t *r,
                                  moq_session_t *session, uint64_t now_us);
bool moq_media_receiver_test_catalog_fetch_pending(const moq_media_receiver_t *r);

/* Minimal MSF catalog declaring one live LOC video track. */
static const char CATALOG_JSON[] =
    "{\"version\":\"1\",\"tracks\":["
    "{\"name\":\"video\",\"packaging\":\"loc\",\"isLive\":true,"
    "\"role\":\"video\",\"codec\":\"avc1.42e01e\"}]}";

typedef struct pair {
    moq_simpair_t *sp;
    moq_session_t *client;   /* the test's endpoint (origin / receiver) */
    moq_session_t *server;   /* the relay's session                     */
} pair_t;

static bool
pair_open(pair_t *p, moqr_bind_t *bind, moq_version_t version, uint64_t seed)
{
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = moq_alloc_default();
    cfg.seed = seed;
    cfg.version = version;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 1024;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = 1024;
    if (moq_simpair_create(&cfg, &p->sp) != MOQ_OK) return false;
    p->client = moq_simpair_client(p->sp);
    p->server = moq_simpair_server(p->sp);
    if (moq_simpair_start(p->sp) != MOQ_OK) return false;
    return moqr_bind_conn_open(bind, p->server, version) == MOQR_OK;
}

static int
run(moq_version_t version)
{
    const char *lbl = version == MOQ_VERSION_DRAFT_16 ? "v16" : "v18";
    int before = failures;
    const moq_alloc_t *alloc = moq_alloc_default();

    moqr_core_relay_cfg_t ccfg;
    moqr_core_relay_cfg_init_sized(&ccfg, sizeof(ccfg), alloc);
    ccfg.log_budget.max_groups = 8;
    ccfg.log_budget.max_bytes = 1 << 20;
    ccfg.linger_us = 500;
    moqr_core_t *core = NULL;
    MOQ_TEST_CHECK(moqr_core_create(&ccfg, &core) == MOQR_OK);
    moqr_bind_cfg_t bcfg;
    moqr_bind_cfg_init_sized(&bcfg, sizeof(bcfg), alloc);
    bcfg.core = core;
    bcfg.max_conns = 4;
    moqr_bind_t *bind = NULL;
    MOQ_TEST_CHECK(moqr_bind_create(&bcfg, &bind) == MOQR_OK);

    pair_t origin, down;
    memset(&origin, 0, sizeof(origin));
    memset(&down, 0, sizeof(down));
    MOQ_TEST_CHECK(pair_open(&origin, bind, version, 0xB001u));
    MOQ_TEST_CHECK(pair_open(&down, bind, version, 0xB002u));

    uint64_t now = 1;
    moq_publisher_t *pub = NULL;
    moq_media_receiver_t *r = NULL;
#define CYCLE()                                                           \
    do {                                                                  \
        now += 1000;                                                      \
        size_t steps_ = 0;                                                \
        (void)moq_simpair_advance_to(origin.sp, now);                     \
        (void)moq_simpair_run_until_quiescent(origin.sp, 64, &steps_);    \
        (void)moq_simpair_advance_to(down.sp, now);                       \
        (void)moq_simpair_run_until_quiescent(down.sp, 64, &steps_);      \
        (void)moqr_bind_pump(bind, now);                                  \
        if (pub) (void)moq_pub_tick(pub, now);                            \
        if (r) moq_media_receiver_test_pump(r, down.client, now);         \
    } while (0)
    for (int i = 0; i < 8; i++) CYCLE();
    MOQ_TEST_CHECK(moq_session_state(origin.client) == MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK(moq_session_state(down.client) == MOQ_SESS_ESTABLISHED);

    /* Origin: namespace svc/demo, catalog track with the retained generation. */
    moq_bytes_t nsp[2] = { MOQ_BYTES_LITERAL("svc"), MOQ_BYTES_LITERAL("demo") };
    moq_namespace_t ns = { .parts = nsp, .count = 2 };
    moq_pub_cfg_t pcfg;
    moq_pub_cfg_init_sized(&pcfg, sizeof(pcfg));
    pcfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    MOQ_TEST_CHECK(moq_pub_create(origin.client, alloc, &pcfg, &pub) == MOQ_OK);
    moq_pub_track_cfg_t tcfg;
    moq_pub_track_cfg_init_sized(&tcfg, sizeof(tcfg));
    tcfg.track_namespace = ns;
    tcfg.track_name = MOQ_BYTES_LITERAL("catalog");
    tcfg.advertise_namespace = true;
    moq_pub_track_t *track = NULL;
    MOQ_TEST_CHECK(pub && moq_pub_add_track(pub, &tcfg, now, &track) == MOQ_OK);
    moq_rcbuf_t *cat = NULL;
    MOQ_TEST_CHECK(moq_rcbuf_create(alloc, (const uint8_t *)CATALOG_JSON,
                                    sizeof(CATALOG_JSON) - 1, &cat) == MOQ_OK);
    moq_pub_retained_object_t objs[1] = {
        { .object_id = 0, .payload = cat, .properties = NULL, .end_of_group = true },
    };
    moq_pub_retained_group_cfg_t rg;
    moq_pub_retained_group_cfg_init(&rg);
    rg.group_id = 0;
    rg.objects = objs;
    rg.object_count = 1;
    MOQ_TEST_CHECK(pub && track && moq_pub_set_retained_group(pub, track, &rg) == MOQ_OK);
    if (cat) moq_rcbuf_decref(cat);
    for (int i = 0; i < 8 && !(pub && track && moq_pub_namespace_accepted(pub, track)); i++) CYCLE();
    MOQ_TEST_CHECK(pub && track && moq_pub_namespace_accepted(pub, track));

    /* The real receiver on the downstream connection: discover only. */
    moq_media_receiver_cfg_t rcfg;
    moq_media_receiver_cfg_init_live(&rcfg);
    rcfg.namespace_.parts = nsp;
    rcfg.namespace_.count = 2;
    rcfg.auto_subscribe = false;
    rcfg.time_mode = MOQ_MEDIA_TIME_RAW;
    rcfg.overflow.policy = MOQ_MEDIA_OVERFLOW_DROP_GROUP;
    rcfg.overflow.max_objects = 64;
    rcfg.overflow.max_bytes = 1u << 20;
    r = moq_media_receiver_test_new_cfg(&rcfg);
    MOQ_TEST_CHECK(r != NULL);

    bool fetch_pending_seen = false, fetch_cleared = false;
    int track_added = 0, catalog_ready = 0, other_events = 0, after_ready = 0;
    int ready_cycle = -1, fetch_seen_cycle = -1, fetch_cleared_cycle = -1;
    for (int cycle = 0; cycle < 80 && r; cycle++) {
        CYCLE();
        bool pending = moq_media_receiver_test_catalog_fetch_pending(r);
        if (pending && !fetch_pending_seen) {
            fetch_pending_seen = true;
            fetch_seen_cycle = cycle;
        }
        if (fetch_pending_seen && !pending && !fetch_cleared) {
            fetch_cleared = true;
            fetch_cleared_cycle = cycle;
        }
        moq_media_track_event_t te;
        while (moq_media_receiver_poll_track(r, &te, sizeof(te)) == MOQ_OK) {
            if (catalog_ready > 0) after_ready++;
            if (te.kind == MOQ_MEDIA_TRACK_ADDED) track_added++;
            else if (te.kind == MOQ_MEDIA_CATALOG_READY) { catalog_ready++; ready_cycle = cycle; }
            else other_events++;
        }
        (void)moq_session_process_pending(down.client, now);
        if (moq_media_receiver_is_fatal(r)) {
            fprintf(stderr, "receiver FATAL: code=0x%llx\n",
                    (unsigned long long)moq_media_receiver_fatal_code(r));
            break;
        }
    }
    bool fatal = r ? moq_media_receiver_is_fatal(r) : true;
    fprintf(stderr,
            "RESULT %s: track_added=%d catalog_ready=%d(cycle %d) other=%d after_ready=%d "
            "fetch_pending_seen=%d(cycle %d) fetch_cleared=%d(cycle %d) fatal=%d\n",
            lbl, track_added, catalog_ready, ready_cycle, other_events, after_ready,
            fetch_pending_seen, fetch_seen_cycle, fetch_cleared, fetch_cleared_cycle, fatal);
    MOQ_TEST_CHECK(fetch_pending_seen);          /* the Joining FETCH was issued */
    MOQ_TEST_CHECK(fetch_cleared);               /* and answered (terminal)      */
    MOQ_TEST_CHECK(!fatal);
    MOQ_TEST_CHECK(track_added == 1);
    MOQ_TEST_CHECK(catalog_ready == 1);
    MOQ_TEST_CHECK(other_events == 0);
    MOQ_TEST_CHECK(after_ready == 0);            /* no later mutation / refresh  */
    MOQ_TEST_CHECK(r && !moq_media_receiver_test_catalog_fetch_pending(r));
    MOQ_TEST_CHECK(moq_session_state(down.client) == MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK(moq_session_state(origin.client) == MOQ_SESS_ESTABLISHED);
    /* The forwarded catalog never entered the relay's live log. */
    {
        char dump[4096];
        size_t dn = 0;
        if (moqr_core_route_dump_text(core, dump, sizeof(dump), &dn) == MOQR_OK &&
            dn < sizeof(dump)) {
            dump[dn] = '\0';
            const char *c = strstr(dump, "\"catalog\"");
            const char *log = c ? strstr(c, "log:") : NULL;
            MOQ_TEST_CHECK(log != NULL && strstr(log, "records=0") != NULL);
        } else {
            MOQ_TEST_CHECK(0);
        }
    }
#undef CYCLE
    if (r) moq_media_receiver_test_free(r);
    if (pub) moq_pub_destroy(pub);
    moqr_bind_destroy(bind);
    moqr_core_destroy(core);
    if (origin.sp) moq_simpair_destroy(origin.sp);
    if (down.sp) moq_simpair_destroy(down.sp);
    if (failures == before) {
        printf("PASS: media_receiver_relay_bootstrap %s\n", lbl);
    }
    return failures - before;
}

int main(void)
{
    (void)run(MOQ_VERSION_DRAFT_16);
    (void)run(MOQ_VERSION_DRAFT_18);
    if (failures == 0)
        MOQ_TEST_PASS("media_receiver_relay_bootstrap");
    return failures ? 1 : 0;
}
