/*
 * MOQT 7.2 scheduling over the REAL picoquic transport in the deterministic
 * simulator (tls_api): genuine QUIC packets, the picoquic adapter with its
 * default send queue, a rate-limited link so the publisher really has more
 * queued than it can send, and virtual time. What is checked is what the
 * subscriber RECEIVES: the order in which groups complete.
 *
 * Each case queues every group at once on the publisher, then runs the
 * network until the subscriber has everything:
 *   - group order: the subscriber's, else the publisher's preference, else
 *     ascending (draft-16 and draft-18);
 *   - subscriber priority first across subscriptions, whatever was
 *     published first;
 *   - a subscriber priority update reorders what is still to be sent;
 *   - each opened stream gets its own QUIC stream id.
 */

#include <moq/moq.h>
#include <moq/picoquic.h>

#include "picoquic_endpoint.h"

#include <picoquictest_internal.h>
#include <picoquic_utils.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int failures = 0;
static const char *scenario = "scheduling";
#define CHECK(cond) do { \
    if (!(cond)) { \
        fprintf(stderr, "FAIL[%s]: %s:%d: %s\n", scenario, __FILE__, \
                __LINE__, #cond); \
        failures++; \
    } } while (0)

#define ASC  MOQ_GROUP_ORDER_ASCENDING
#define DESC MOQ_GROUP_ORDER_DESCENDING
#define NONE MOQ_GROUP_ORDER_DEFAULT

#define OBJ_SIZE     16384u   /* bytes per object */
#define OBJS_PER_GRP 3
#define LINK_PS_PER_BYTE 800000u   /* 10 Mbps */

/* -- sim pair (as in test_pq_sim_flow_control.c) ------------------------- */

typedef struct {
    picoquic_test_tls_api_ctx_t *test_ctx;
    uint64_t now;
    uint64_t loss;
    moq_version_t version;
    moq_session_t *client_session;
    moq_session_t *server_session;
    moq_pq_conn_t *client_conn;
    moq_pq_conn_t *server_conn;
    int            server_create_failed;
} pq_sim_t;

static int sim_create_session(moq_version_t version, moq_perspective_t persp,
                              moq_session_t **out)
{
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), persp);
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 16;
    cfg.version = version;
    cfg.max_events = 256;
    if (persp == MOQ_PERSPECTIVE_SERVER) {
        cfg.send_buffer_size = 1u << 20;
        cfg.max_actions = 1024;
    }
    return moq_session_create(&cfg, 0, out) == MOQ_OK ? 0 : -1;
}

static int sim_attach_conn(moq_session_t *session, picoquic_cnx_t *cnx,
                           moq_pq_conn_t **out)
{
    moq_pq_conn_cfg_t cfg;
    moq_pq_conn_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.session = session;
    cfg.cnx = cnx;
    cfg.alloc = moq_alloc_default();
    return moq_pq_conn_create(&cfg, out);
}

static int sim_server_cb(picoquic_cnx_t *cnx, uint64_t stream_id,
                         uint8_t *bytes, size_t length,
                         picoquic_call_back_event_t event, void *callback_ctx,
                         void *stream_ctx)
{
    pq_sim_t *s = (pq_sim_t *)callback_ctx;
    (void)stream_id; (void)bytes; (void)length; (void)stream_ctx;
    if (event != picoquic_callback_almost_ready &&
        event != picoquic_callback_ready)
        return 0;
    if (s->server_conn) return 0;
    if (sim_create_session(s->version, MOQ_PERSPECTIVE_SERVER,
                           &s->server_session) != 0 ||
        sim_attach_conn(s->server_session, cnx, &s->server_conn) != 0) {
        s->server_create_failed = 1;
        return -1;
    }
    return 0;
}

static int sim_round(pq_sim_t *s, uint64_t time_limit, int *was_active)
{
    if (s->client_conn) moq_pq_service(s->client_conn, s->now);
    if (s->server_conn) moq_pq_service(s->server_conn, s->now);
    int wa = 0;
    int rc = tls_api_one_sim_round(s->test_ctx, &s->now, time_limit, &wa);
    if (s->client_conn) moq_pq_service(s->client_conn, s->now);
    if (s->server_conn) moq_pq_service(s->server_conn, s->now);
    if (was_active) *was_active = wa;
    return rc;
}

static void sim_pump(pq_sim_t *s, uint64_t ms)
{
    uint64_t limit = s->now + ms * 1000;
    int inactive = 0;
    for (int i = 0; i < 200000; i++) {
        if (s->now > limit) return;
        int wa = 0;
        if (sim_round(s, limit, &wa) != 0) return;
        if (!wa) { if (++inactive > 10) return; }
        else inactive = 0;
    }
}

static int sim_setup(pq_sim_t *s, uint8_t cid_byte, moq_version_t version)
{
    memset(s, 0, sizeof(*s));
    s->version = version;
    picoquic_solution_dir = PICOQUIC_SOURCE_DIR;
    picoquic_connection_id_t cid = {
        {0x6d, 0x71, 0x5c, cid_byte, 0, 0, 0, 0}, 8};
    if (tls_api_init_ctx_ex(&s->test_ctx, PICOQUIC_INTERNAL_TEST_VERSION_1,
                            PICOQUIC_TEST_SNI, PICOQUIC_TEST_ALPN, &s->now,
                            NULL, NULL, 0, 1, 0, &cid) != 0)
        return -1;
    picoquic_set_default_idle_timeout(s->test_ctx->qclient, 30000);
    picoquic_set_default_idle_timeout(s->test_ctx->qserver, 30000);
    /* The publisher's direction is the bottleneck. */
    s->test_ctx->s_to_c_link->picosec_per_byte = LINK_PS_PER_BYTE;

    if (sim_create_session(version, MOQ_PERSPECTIVE_CLIENT,
                           &s->client_session) != 0)
        return -1;
    if (sim_attach_conn(s->client_session, s->test_ctx->cnx_client,
                        &s->client_conn) != 0)
        return -1;
    picoquic_set_default_callback(s->test_ctx->qserver, sim_server_cb, s);
    if (picoquic_start_client_cnx(s->test_ctx->cnx_client) != 0) return -1;
    if (tls_api_connection_loop(s->test_ctx, &s->loss, 0, &s->now) != 0)
        return -1;
    if (s->server_create_failed || !s->server_conn) return -1;

    if (moq_session_start(s->client_session, s->now) < 0) return -1;
    if (version == MOQ_VERSION_DRAFT_18 &&
        moq_session_start(s->server_session, s->now) < 0)
        return -1;
    sim_pump(s, 500);
    moq_event_t ev;
    while (moq_session_poll_events(s->client_session, &ev, 1) > 0)
        moq_event_cleanup(&ev);
    while (moq_session_poll_events(s->server_session, &ev, 1) > 0)
        moq_event_cleanup(&ev);
    return moq_session_state(s->client_session) == MOQ_SESS_ESTABLISHED &&
           moq_session_state(s->server_session) == MOQ_SESS_ESTABLISHED
        ? 0 : -1;
}

static void sim_cleanup(pq_sim_t *s)
{
    if (s->client_conn) moq_pq_conn_destroy(s->client_conn);
    if (s->server_conn) moq_pq_conn_destroy(s->server_conn);
    if (s->client_session) moq_session_destroy(s->client_session);
    if (s->server_session) moq_session_destroy(s->server_session);
    if (s->test_ctx) tls_api_delete_ctx(s->test_ctx);
    memset(s, 0, sizeof(*s));
}

/* -- subscriptions and data ---------------------------------------------- */

typedef struct {
    moq_subscription_t pub;   /* the publisher's handle */
    moq_subscription_t sub;   /* the subscriber's handle */
} track_t;

static int subscribe(pq_sim_t *s, const char *name, int sub_prio,
                     moq_group_order_t order, moq_group_order_t pref,
                     track_t *out)
{
    moq_bytes_t part = { (const uint8_t *)"live", 4 };
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ &part, 1 };
    sc.track_name = (moq_bytes_t){ (const uint8_t *)name, strlen(name) };
    sc.has_subscriber_priority = sub_prio >= 0;
    sc.subscriber_priority = (uint8_t)(sub_prio >= 0 ? sub_prio : 0);
    sc.group_order = order;
    if (moq_session_subscribe(s->client_session, &sc, s->now, &out->sub) < 0)
        return -1;
    sim_pump(s, 200);
    bool got = false;
    moq_event_t ev;
    while (moq_session_poll_events(s->server_session, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
            out->pub = ev.u.subscribe_request.sub;
            got = true;
        }
        moq_event_cleanup(&ev);
    }
    if (!got) return -1;
    uint8_t props[16];
    size_t props_len = 0;
    if (pref != NONE &&
        moq_session_track_properties_add_group_order(s->server_session, NULL,
            0, pref, props, sizeof(props), &props_len) != MOQ_OK)
        return -1;
    moq_accept_subscribe_cfg_t ac;
    moq_accept_subscribe_cfg_init(&ac);
    ac.track_properties = (moq_bytes_t){ props, props_len };
    if (moq_session_accept_subscribe(s->server_session, out->pub, &ac,
                                     s->now) < 0)
        return -1;
    sim_pump(s, 200);
    bool ok = false;
    while (moq_session_poll_events(s->client_session, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_OK) ok = true;
        moq_event_cleanup(&ev);
    }
    return ok ? 0 : -1;
}

/* Open (group, subgroup 0) and write its objects, each payload starting with
 * (track, group); FIN after the last. */
static int publish_group(pq_sim_t *s, const track_t *t, uint8_t track,
                         uint64_t group)
{
    moq_subgroup_cfg_t cfg;
    moq_subgroup_cfg_init(&cfg);
    cfg.group_id = group;
    cfg.publisher_priority = 128;
    moq_subgroup_handle_t sg;
    if (moq_session_open_subgroup(s->server_session, t->pub, &cfg, s->now,
                                  &sg) < 0)
        return -1;
    static uint8_t payload[OBJ_SIZE];
    for (int o = 0; o < OBJS_PER_GRP; o++) {
        memset(payload, 0, sizeof(payload));
        payload[0] = track;
        payload[1] = (uint8_t)group;
        moq_rcbuf_t *buf = NULL;
        if (moq_rcbuf_create(moq_alloc_default(), payload, sizeof(payload),
                             &buf) < 0)
            return -1;
        moq_result_t rc = moq_session_write_object(s->server_session, sg,
                                                   (uint64_t)o, buf, s->now);
        moq_rcbuf_decref(buf);
        if (rc < 0) return -1;
    }
    return moq_session_close_subgroup(s->server_session, sg, s->now) < 0
        ? -1 : 0;
}

/* Groups in the order their last object reached the subscriber, as
 * track << 8 | group. Runs the network until `want` groups completed or the
 * virtual budget is spent. `hook` (may be NULL) runs once, after the first
 * completed group. */
typedef void (*hook_fn)(pq_sim_t *s, void *ctx);

static size_t receive_groups(pq_sim_t *s, uint32_t *out, size_t want,
                             hook_fn hook, void *ctx)
{
    static int count[1u << 16];   /* objects per track << 8 | group */
    memset(count, 0, sizeof(count));
    size_t n = 0;
    uint64_t deadline = s->now + 60000000;
    while (n < want && s->now < deadline) {
        sim_pump(s, 20);
        moq_event_t ev;
        while (moq_session_poll_events(s->client_session, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED && ev.u.object_received.payload &&
                moq_rcbuf_len(ev.u.object_received.payload) >= 2) {
                const uint8_t *d = moq_rcbuf_data(ev.u.object_received.payload);
                uint32_t key = (uint32_t)d[0] << 8 | d[1];
                if (++count[key] == OBJS_PER_GRP && n < want) {
                    out[n++] = key;
                    if (n == 1 && hook) hook(s, ctx);
                }
            }
            moq_event_cleanup(&ev);
        }
    }
    return n;
}

static void expect_groups(const uint32_t *got, size_t n, const uint32_t *want,
                          size_t m, const char *what, moq_version_t ver)
{
    bool same = n == m;
    for (size_t i = 0; same && i < m; i++) same = got[i] == want[i];
    if (!same) {
        fprintf(stderr, "%s (d%d): got", what,
                ver == MOQ_VERSION_DRAFT_18 ? 18 : 16);
        for (size_t i = 0; i < n; i++) fprintf(stderr, " %u/%u",
                                               got[i] >> 8, got[i] & 0xff);
        fprintf(stderr, " want");
        for (size_t i = 0; i < m; i++) fprintf(stderr, " %u/%u",
                                               want[i] >> 8, want[i] & 0xff);
        fprintf(stderr, "\n");
    }
    CHECK(same);
}

static const moq_version_t k_versions[] = {
    MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18,
};
static uint8_t g_cid = 1;

/* -- cases --------------------------------------------------------------- */

/* Groups 1..4 queued at once complete in the effective group order. */
static void check_group_order(moq_version_t ver, moq_group_order_t order,
                              moq_group_order_t pref, bool want_desc)
{
    pq_sim_t s;
    CHECK(sim_setup(&s, g_cid++, ver) == 0);
    track_t t;
    CHECK(subscribe(&s, "v", -1, order, pref, &t) == 0);
    for (uint64_t g = 1; g <= 4; g++) CHECK(publish_group(&s, &t, 1, g) == 0);
    uint32_t got[8];
    size_t n = receive_groups(&s, got, 4, NULL, NULL);
    uint32_t want[4];
    for (int i = 0; i < 4; i++)
        want[i] = 1u << 8 | (uint32_t)(want_desc ? 4 - i : 1 + i);
    expect_groups(got, n, want, 4, "group order", ver);
    sim_cleanup(&s);
}

/* Subscriber priority decides between subscriptions, not arrival: track 2
 * (priority 20) is published first, track 1 (priority 10) completes first. */
static void check_subscriber_priority(moq_version_t ver)
{
    pq_sim_t s;
    CHECK(sim_setup(&s, g_cid++, ver) == 0);
    track_t a, b;
    CHECK(subscribe(&s, "a", 10, NONE, NONE, &a) == 0);
    CHECK(subscribe(&s, "b", 20, NONE, NONE, &b) == 0);
    for (uint64_t g = 1; g <= 2; g++) CHECK(publish_group(&s, &b, 2, g) == 0);
    for (uint64_t g = 1; g <= 2; g++) CHECK(publish_group(&s, &a, 1, g) == 0);
    uint32_t got[8];
    size_t n = receive_groups(&s, got, 4, NULL, NULL);
    static const uint32_t want[] = { 0x101, 0x102, 0x201, 0x202 };
    expect_groups(got, n, want, 4, "subscriber priority", ver);
    sim_cleanup(&s);
}

/* Once the first group arrives, track 2 is raised above track 1: its
 * remaining groups complete before track 1's. */
static void raise_b(pq_sim_t *s, void *ctx)
{
    const track_t *b = ctx;
    moq_subscription_update_cfg_t uc;
    moq_subscription_update_cfg_init(&uc);
    uc.has_subscriber_priority = true;
    uc.subscriber_priority = 5;
    CHECK(moq_session_update_subscription(s->client_session, b->sub, &uc,
                                          s->now) == MOQ_OK);
}

static void check_priority_update(moq_version_t ver)
{
    pq_sim_t s;
    CHECK(sim_setup(&s, g_cid++, ver) == 0);
    track_t a, b;
    CHECK(subscribe(&s, "a", 10, NONE, NONE, &a) == 0);
    CHECK(subscribe(&s, "b", 20, NONE, NONE, &b) == 0);
    for (uint64_t g = 1; g <= 4; g++) CHECK(publish_group(&s, &a, 1, g) == 0);
    for (uint64_t g = 1; g <= 4; g++) CHECK(publish_group(&s, &b, 2, g) == 0);
    uint32_t got[16];
    size_t n = receive_groups(&s, got, 8, raise_b, &b);
    CHECK(n == 8);
    /* The first group is track 1's (it led before the update); after the
     * update, all of track 2 completes before the rest of track 1, allowing
     * one track 1 group already in flight. */
    CHECK(n > 0 && got[0] >> 8 == 1);
    size_t last_b = 0, first_a_after = SIZE_MAX;
    for (size_t i = 0; i < n; i++)
        if (got[i] >> 8 == 2) last_b = i;
    for (size_t i = 2; i < n; i++)
        if (got[i] >> 8 == 1) { first_a_after = i; break; }
    if (!(first_a_after == SIZE_MAX || last_b < first_a_after)) {
        fprintf(stderr, "priority update (d%d): got",
                ver == MOQ_VERSION_DRAFT_18 ? 18 : 16);
        for (size_t i = 0; i < n; i++)
            fprintf(stderr, " %u/%u", got[i] >> 8, got[i] & 0xff);
        fprintf(stderr, "\n");
    }
    CHECK(first_a_after == SIZE_MAX || last_b < first_a_after);
    sim_cleanup(&s);
}

/* Opening a stream must take its id even before any byte is queued: two
 * opens in a row, or an open after a write refused by a full queue, must not
 * return the same QUIC stream. */
static void check_open_ids(moq_version_t ver)
{
    pq_sim_t s;
    CHECK(sim_setup(&s, g_cid++, ver) == 0);
    moq_transport_endpoint_ops_t ops = MOQ_TRANSPORT_ENDPOINT_OPS_INIT;
    pq_endpoint_ctx_t ep;
    CHECK(pq_endpoint_init(&ops, &ep, s.test_ctx->cnx_server,
                           moq_alloc_default(), 1) == 0);
    static const uint8_t bytes[2] = { 1, 2 };
    uint64_t a = 0, b = 0, c = 0, d = 0, e = 0;
    CHECK(ops.open_uni(&ep, &a) == MOQ_TRANSPORT_OK);
    CHECK(ops.open_uni(&ep, &b) == MOQ_TRANSPORT_OK);
    CHECK(a != b);
    CHECK(ops.write(&ep, a, bytes, sizeof(bytes), false) == MOQ_TRANSPORT_OK);
    CHECK(ops.write(&ep, b, bytes, sizeof(bytes), false) ==
          MOQ_TRANSPORT_WOULD_BLOCK);
    CHECK(ops.open_uni(&ep, &c) == MOQ_TRANSPORT_OK);
    CHECK(c != a && c != b);
    CHECK(ops.open_bidi(&ep, &d) == MOQ_TRANSPORT_OK);
    CHECK(ops.open_bidi(&ep, &e) == MOQ_TRANSPORT_OK);
    CHECK(d != e);
    pq_endpoint_cleanup(&ep);
    sim_cleanup(&s);
}

int main(void)
{
    for (size_t v = 0; v < 2; v++) {
        moq_version_t ver = k_versions[v];
        scenario = "group_order";
        check_group_order(ver, DESC, ASC, true);
        check_group_order(ver, ASC, DESC, false);
        check_group_order(ver, NONE, DESC, true);
        check_group_order(ver, NONE, NONE, false);
        scenario = "subscriber_priority";
        check_subscriber_priority(ver);
        scenario = "priority_update";
        check_priority_update(ver);
        scenario = "open_ids";
        check_open_ids(ver);
    }
    if (failures) {
        fprintf(stderr, "test_pq_sim_scheduling: %d failure(s)\n", failures);
        return 1;
    }
    printf("test_pq_sim_scheduling: all passed\n");
    return 0;
}
