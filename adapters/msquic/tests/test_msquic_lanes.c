/*
 * Decisive lane-partition pin for the managed MsQuic facade.
 *
 * A server with lane_count = 2 places accepted connections via
 * choose_lane: the first two accepted go to lane 0, the third to
 * lane 1. Three clients connect, each SUBSCRIBEs; the server publishes
 * one object per subscription from inside the OWNING lane's pump. The
 * pin proves the lane ownership contract:
 *
 *   - lane 0 owns two connections, lane 1 owns one;
 *   - a lane pump iterates ONLY its own lane's connections (every
 *     connection a pump sees reports that lane via conn_lane), and sees
 *     ALL of them in one pass (lane 0's pump observes two at once);
 *   - a lane pump is never re-entered concurrently for the same lane
 *     (a per-lane depth counter never exceeds one);
 *   - a terminal connection stays visible through the lane pump that
 *     observes its SESSION_CLOSED before it is reaped (all three
 *     closes are observed).
 *
 * Confinement: every moq_session_* call lives inside on_lane_pump. The
 * main thread only waits and asserts recorded observations after the
 * managed stop/destroy quiescence barrier.
 */

#if defined(__linux__) && !defined(_POSIX_C_SOURCE)
#define _POSIX_C_SOURCE 200809L
#endif

#include <pthread.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <moq/msquic_managed.h>
#include <moq/rcbuf.h>

static int failures = 0;

#define CHECK(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)

enum { NCLIENTS = 3, OBJECT_SIZE = 64 };

static void payload_fill(uint8_t *dst, size_t size, uint8_t tag)
{
    for (size_t i = 0; i < size; i++)
        dst[i] = (uint8_t)(i * 131u + tag + 47u);
}

/* --- per-connection server state (lives on the conn user slot) ------- */

struct conn_state {
    bool have_sub;
    bool published;
    bool closed_seen;
    moq_subscription_t sub;
};

/* --- server-wide observations --------------------------------------- */

struct server_obs {
    /* choose_lane placement counter (accepts may be concurrent) */
    atomic_uint accept_seq;
    /* per-lane reentry depth (must never exceed 1) */
    atomic_int lane_depth[2];
    atomic_bool reentry_seen;
    atomic_bool wrong_lane_seen;
    /* max connections a single pass of each lane observed */
    int max_seen[2];        /* written only by that lane's doorbell */
    /* SESSION_CLOSED observations, total across lanes */
    atomic_int closed_seen;
    atomic_int published_total;
    atomic_int errors;
    struct conn_state cstate[NCLIENTS + 2];
    atomic_uint cstate_next;
};

static uint32_t choose_lane(moq_msquic_managed_t *m,
                            const moq_msquic_accept_info_t *info,
                            void *user)
{
    struct server_obs *sv = user;

    (void)m;
    (void)info;
    /* first two accepted -> lane 0, the rest -> lane 1 */
    return atomic_fetch_add(&sv->accept_seq, 1) < 2 ? 0u : 1u;
}

static int server_lane_pump(moq_msquic_managed_t *m,
                            moq_msquic_managed_lane_t *lane,
                            uint64_t now, void *user)
{
    struct server_obs *sv = user;
    uint32_t li = moq_msquic_lane_index(lane);

    (void)m;
    if (li > 1) {
        atomic_store(&sv->wrong_lane_seen, true);
        return 0;
    }
    int depth = atomic_fetch_add(&sv->lane_depth[li], 1) + 1;
    if (depth > 1)
        atomic_store(&sv->reentry_seen, true);

    int seen = 0;
    for (moq_msquic_managed_conn_t *c = moq_msquic_lane_next_conn(lane, NULL);
         c != NULL; c = moq_msquic_lane_next_conn(lane, c)) {
        seen++;
        /* every connection a lane pump iterates must belong to it */
        if (moq_msquic_managed_conn_lane(c) != lane)
            atomic_store(&sv->wrong_lane_seen, true);

        moq_session_t *s = moq_msquic_managed_conn_session(c);
        if (s == NULL)
            continue;

        struct conn_state *cs = moq_msquic_managed_conn_user(c);
        if (cs == NULL) {
            unsigned slot = atomic_fetch_add(&sv->cstate_next, 1);
            if (slot < NCLIENTS + 2) {
                cs = &sv->cstate[slot];
                moq_msquic_managed_conn_set_user(c, cs);
            }
        }

        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST && cs != NULL &&
                !cs->have_sub) {
                cs->sub = ev.u.subscribe_request.sub;
                cs->have_sub = true;
            } else if (ev.kind == MOQ_EVENT_SESSION_CLOSED && cs != NULL &&
                       !cs->closed_seen) {
                cs->closed_seen = true;
                atomic_fetch_add(&sv->closed_seen, 1);
            }
            moq_event_cleanup(&ev);
        }
        if (cs != NULL && cs->have_sub && !cs->published) {
            moq_accept_subscribe_cfg_t ac;
            moq_subgroup_cfg_t sgc;
            moq_subgroup_handle_t sg;

            moq_accept_subscribe_cfg_init(&ac);
            if (moq_session_accept_subscribe(s, cs->sub, &ac, now) < 0) {
                atomic_fetch_add(&sv->errors, 1);
                continue;
            }
            moq_subgroup_cfg_init(&sgc);
            sgc.group_id = 0;
            sgc.publisher_priority = 200;
            if (moq_session_open_subgroup(s, cs->sub, &sgc, now, &sg) < 0)
                continue; /* retry a later pump */
            uint8_t *bytes = malloc(OBJECT_SIZE);
            moq_rcbuf_t *buf = NULL;

            if (bytes != NULL) {
                payload_fill(bytes, OBJECT_SIZE, (uint8_t)li);
                if (moq_rcbuf_create(moq_alloc_default(), bytes,
                                     OBJECT_SIZE, &buf) == 0) {
                    (void)moq_session_write_object(s, sg, 0, buf, now);
                    moq_rcbuf_decref(buf);
                }
                free(bytes);
            }
            (void)moq_session_close_subgroup(s, sg, now);
            cs->published = true;
            atomic_fetch_add(&sv->published_total, 1);
        }
    }
    if (seen > sv->max_seen[li])
        sv->max_seen[li] = seen; /* only this lane's doorbell writes [li] */

    atomic_fetch_sub(&sv->lane_depth[li], 1);
    return 0;
}

/* --- client ---------------------------------------------------------- */

struct client_state {
    int received;
    bool subscribed;
    bool close_sent;
    int errors;
};

static int client_lane_pump(moq_msquic_managed_t *m,
                            moq_msquic_managed_lane_t *lane,
                            uint64_t now, void *user)
{
    struct client_state *cl = user;
    moq_session_t *s = moq_msquic_managed_session(m);

    (void)lane;
    if (s == NULL)
        return 0;

    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SETUP_COMPLETE && !cl->subscribed) {
            static const moq_bytes_t ns[] = {
                { (const uint8_t *)"lane", 4 },
            };
            moq_subscribe_cfg_t sc;
            moq_subscription_t sub;

            moq_subscribe_cfg_init(&sc);
            sc.track_namespace.parts = (moq_bytes_t *)ns;
            sc.track_namespace.count = 1;
            sc.track_name = (moq_bytes_t){ (const uint8_t *)"t", 1 };
            sc.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
            cl->subscribed = true;
            if (moq_session_subscribe(s, &sc, now, &sub) < 0)
                cl->errors++;
        } else if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) {
            if (ev.u.object_received.payload != NULL &&
                moq_rcbuf_len(ev.u.object_received.payload) == OBJECT_SIZE)
                cl->received++;
            else
                cl->errors++;
        }
        moq_event_cleanup(&ev);
    }
    /* once the object arrived, close cleanly so the server observes a
     * SESSION_CLOSED terminal before reap */
    if (cl->received >= 1 && !cl->close_sent) {
        cl->close_sent = true;
        (void)moq_session_close(s, 0, NULL, now);
    }
    return 0;
}

/* --- pin 1: decisive lane partition ---------------------------------- */

static void test_partition(const char *cert, const char *key)
{
    int before = failures;
    struct server_obs sv;
    memset(&sv, 0, sizeof(sv));

    moq_msquic_managed_cfg_t cfg;
    moq_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.host = "127.0.0.1";
    cfg.port = 0;
    cfg.cert_path = cert;
    cfg.key_path = key;
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 16;
    cfg.max_events = 64;
    cfg.max_connections = NCLIENTS;
    cfg.lane_count = 2;
    cfg.choose_lane = choose_lane;
    cfg.choose_lane_user = &sv;
    cfg.on_lane_pump = server_lane_pump;
    cfg.on_lane_pump_user = &sv;

    moq_msquic_managed_t *srv = NULL;
    CHECK(moq_msquic_managed_create(&cfg, &srv) == MOQ_OK);
    if (srv == NULL)
        return;
    CHECK(moq_msquic_managed_lane_count(srv) == 2);
    uint16_t port = moq_msquic_managed_port(srv);
    CHECK(port != 0);

    struct client_state cst[NCLIENTS];
    moq_msquic_managed_t *cli[NCLIENTS] = { 0 };
    memset(cst, 0, sizeof(cst));

    for (int i = 0; i < NCLIENTS; i++) {
        moq_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
        cfg.alloc = moq_alloc_default();
        cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
        cfg.host = "127.0.0.1";
        cfg.port = port;
        cfg.insecure_skip_verify = true;
        cfg.send_request_capacity = true;
        cfg.initial_request_capacity = 16;
        cfg.max_events = 64;
        cfg.on_lane_pump = client_lane_pump;
        cfg.on_lane_pump_user = &cst[i];
        CHECK(moq_msquic_managed_create(&cfg, &cli[i]) == MOQ_OK);
    }

    /* drive: wake the server + every client until all objects landed
     * and all closes were observed, or a bounded deadline elapses */
    for (int iter = 0; iter < 6000; iter++) {
        int recv_total = 0;
        for (int i = 0; i < NCLIENTS; i++)
            recv_total += cst[i].received;
        if (recv_total >= NCLIENTS &&
            atomic_load(&sv.closed_seen) >= NCLIENTS)
            break;
        (void)moq_msquic_managed_wake(srv);
        for (int i = 0; i < NCLIENTS; i++)
            (void)moq_msquic_managed_wake(cli[i]);
        struct timespec ts = { 0, 3 * 1000 * 1000 };
        nanosleep(&ts, NULL);
    }

    for (int i = 0; i < NCLIENTS; i++) {
        if (cli[i] != NULL) {
            (void)moq_msquic_managed_stop(cli[i]);
            moq_msquic_managed_destroy(cli[i]);
        }
    }
    (void)moq_msquic_managed_stop(srv);
    moq_msquic_managed_destroy(srv);

    /* --- assertions (after the quiescence barrier) --- */
    int recv_total = 0;
    for (int i = 0; i < NCLIENTS; i++) {
        recv_total += cst[i].received;
        CHECK(cst[i].errors == 0);
    }
    CHECK(recv_total == NCLIENTS);            /* every client got its object */
    CHECK(sv.max_seen[0] == 2);               /* lane 0 saw BOTH its conns */
    CHECK(sv.max_seen[1] == 1);               /* lane 1 saw only its one */
    CHECK(!atomic_load(&sv.wrong_lane_seen)); /* no cross-lane iteration */
    CHECK(!atomic_load(&sv.reentry_seen));    /* no concurrent lane reentry */
    CHECK(atomic_load(&sv.closed_seen) == NCLIENTS); /* terminal before reap */
    CHECK(atomic_load(&sv.published_total) == NCLIENTS);
    CHECK(atomic_load(&sv.errors) == 0);

    if (failures == before)
        printf("PASS: lane_partition\n");
}

/* --- pin 2: an out-of-range choose_lane REFUSES (never clamps) ------- */

struct refuse_obs {
    atomic_int pump_saw_conn;   /* a server lane pump ever saw a conn */
};

static uint32_t choose_lane_oob(moq_msquic_managed_t *m,
                                const moq_msquic_accept_info_t *info,
                                void *user)
{
    (void)m;
    (void)info;
    (void)user;
    return 99u; /* out of range for lane_count = 2 */
}

static int refuse_server_pump(moq_msquic_managed_t *m,
                              moq_msquic_managed_lane_t *lane,
                              uint64_t now, void *user)
{
    struct refuse_obs *ro = user;

    (void)m;
    (void)now;
    if (moq_msquic_lane_next_conn(lane, NULL) != NULL)
        atomic_store(&ro->pump_saw_conn, 1);
    return 0;
}

static void test_invalid_choose_lane_refuses(const char *cert,
                                             const char *key)
{
    int before = failures;
    struct refuse_obs ro;
    memset(&ro, 0, sizeof(ro));

    moq_msquic_managed_cfg_t cfg;
    moq_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.host = "127.0.0.1";
    cfg.port = 0;
    cfg.cert_path = cert;
    cfg.key_path = key;
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 16;
    cfg.max_events = 64;
    cfg.max_connections = 2;
    cfg.lane_count = 2;
    cfg.choose_lane = choose_lane_oob;
    cfg.choose_lane_user = &ro;
    cfg.on_lane_pump = refuse_server_pump;
    cfg.on_lane_pump_user = &ro;

    moq_msquic_managed_t *srv = NULL;
    CHECK(moq_msquic_managed_create(&cfg, &srv) == MOQ_OK);
    if (srv == NULL)
        return;
    uint16_t port = moq_msquic_managed_port(srv);
    CHECK(port != 0);

    struct client_state cs;
    moq_msquic_managed_t *cli = NULL;
    memset(&cs, 0, sizeof(cs));
    moq_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.host = "127.0.0.1";
    cfg.port = port;
    cfg.insecure_skip_verify = true;
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 16;
    cfg.max_events = 64;
    cfg.on_lane_pump = client_lane_pump;
    cfg.on_lane_pump_user = &cs;
    CHECK(moq_msquic_managed_create(&cfg, &cli) == MOQ_OK);

    /* the server refuses the connection at accept (out-of-range lane),
     * so the client's connect fails: drive until it goes fatal. */
    for (int iter = 0; iter < 2000; iter++) {
        (void)moq_msquic_managed_wake(cli);
        if (moq_msquic_managed_is_fatal(cli))
            break;
        struct timespec ts = { 0, 3 * 1000 * 1000 };
        nanosleep(&ts, NULL);
    }

    /* read every observation before the destroy quiescence frees state */
    bool cli_fatal = moq_msquic_managed_is_fatal(cli);
    size_t srv_conns = moq_msquic_managed_conn_count(srv);
    bool saw_conn = atomic_load(&ro.pump_saw_conn) != 0;

    if (cli != NULL) {
        (void)moq_msquic_managed_stop(cli);
        moq_msquic_managed_destroy(cli);
    }
    (void)moq_msquic_managed_stop(srv);
    moq_msquic_managed_destroy(srv);

    CHECK(srv_conns == 0);       /* never admitted (no clamp to lane 1) */
    CHECK(!saw_conn);            /* no lane pump ever saw a connection */
    CHECK(cli_fatal);            /* the client's connect was rejected */
    CHECK(!cs.subscribed);       /* never reached SETUP */

    if (failures == before)
        printf("PASS: invalid_choose_lane_refuses\n");
}

/* --- pin 3: lane_wake targets one lane; managed_wake wakes all ------- */

struct wake_obs {
    atomic_int pumps[2];        /* pump invocations per lane */
};

static int count_server_pump(moq_msquic_managed_t *m,
                             moq_msquic_managed_lane_t *lane,
                             uint64_t now, void *user)
{
    struct wake_obs *wo = user;
    uint32_t li = moq_msquic_lane_index(lane);

    (void)m;
    (void)now;
    if (li < 2)
        atomic_fetch_add(&wo->pumps[li], 1);
    return 0;
}

/* Poll until pumps[idx] exceeds `base`, or a bounded deadline elapses. */
static bool wait_pump_advanced(struct wake_obs *wo, int idx, int base)
{
    for (int iter = 0; iter < 500; iter++) {
        if (atomic_load(&wo->pumps[idx]) > base)
            return true;
        struct timespec ts = { 0, 1 * 1000 * 1000 };
        nanosleep(&ts, NULL);
    }
    return false;
}

static void test_lane_wake_selectivity(const char *cert, const char *key)
{
    int before = failures;
    struct wake_obs wo;
    memset(&wo, 0, sizeof(wo));

    moq_msquic_managed_cfg_t cfg;
    moq_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.host = "127.0.0.1";
    cfg.port = 0;
    cfg.cert_path = cert;
    cfg.key_path = key;
    cfg.max_events = 64;
    cfg.max_connections = 2;
    cfg.lane_count = 2;
    cfg.on_lane_pump = count_server_pump;
    cfg.on_lane_pump_user = &wo;

    moq_msquic_managed_t *srv = NULL;
    CHECK(moq_msquic_managed_create(&cfg, &srv) == MOQ_OK);
    if (srv == NULL)
        return;
    moq_msquic_managed_lane_t *l0 = moq_msquic_managed_lane(srv, 0);
    moq_msquic_managed_lane_t *l1 = moq_msquic_managed_lane(srv, 1);
    CHECK(l0 != NULL && l1 != NULL);

    /* no clients and no transport: an idle lane pumps ONLY when woken, so
     * the counters are a crisp record of which wakes reached which lane */
    struct timespec settle = { 0, 20 * 1000 * 1000 };
    nanosleep(&settle, NULL);

    /* target lane 0 only */
    int b0 = atomic_load(&wo.pumps[0]);
    int b1 = atomic_load(&wo.pumps[1]);
    CHECK(moq_msquic_lane_wake(l0) == MOQ_OK);
    CHECK(wait_pump_advanced(&wo, 0, b0));         /* lane 0 ran */
    CHECK(atomic_load(&wo.pumps[1]) == b1);        /* lane 1 did NOT */

    /* managed_wake wakes every lane */
    int m0 = atomic_load(&wo.pumps[0]);
    int m1 = atomic_load(&wo.pumps[1]);
    CHECK(moq_msquic_managed_wake(srv) == MOQ_OK);
    CHECK(wait_pump_advanced(&wo, 0, m0));         /* lane 0 ran */
    CHECK(wait_pump_advanced(&wo, 1, m1));         /* lane 1 ran too */

    (void)moq_msquic_managed_stop(srv);
    moq_msquic_managed_destroy(srv);

    if (failures == before)
        printf("PASS: lane_wake_selectivity\n");
}

/* --- pin 4: a cross-lane wake from a callback is GUARANTEED ---------- */

struct xlane_obs {
    atomic_int l1_pumps;        /* lane 1 pump invocations */
    atomic_bool l1_holding;     /* lane 1 pump is mid-pump (contended) */
    atomic_bool l0_did_wake;    /* lane 0's callback issued the wake */
    atomic_int wake_rc;         /* rc returned by lane_wake(lane1) */
    moq_msquic_managed_lane_t *lane1;
};

static int xlane_pump(moq_msquic_managed_t *m,
                      moq_msquic_managed_lane_t *lane, uint64_t now,
                      void *user)
{
    struct xlane_obs *xo = user;
    uint32_t li = moq_msquic_lane_index(lane);

    (void)m;
    (void)now;
    if (li == 1) {
        int n = atomic_fetch_add(&xo->l1_pumps, 1) + 1;

        if (n == 1) {
            /* hold lane 1 inside its pump (its mutex is held, so a wake
             * targeting it is contended) until lane 0's callback has
             * issued the cross-lane wake. Bounded so the test cannot
             * hang. */
            atomic_store(&xo->l1_holding, true);
            for (int i = 0; i < 3000 && !atomic_load(&xo->l0_did_wake);
                 i++) {
                struct timespec ts = { 0, 1 * 1000 * 1000 };
                nanosleep(&ts, NULL);
            }
            atomic_store(&xo->l1_holding, false);
        }
    } else if (li == 0) {
        if (atomic_load(&xo->l1_holding) &&
            !atomic_load(&xo->l0_did_wake)) {
            /* wake lane 1 from inside lane 0's callback while lane 1 is
             * mid-pump — the cross-lane, contended path */
            int rc = (int)moq_msquic_lane_wake(xo->lane1);

            atomic_store(&xo->wake_rc, rc);
            atomic_store(&xo->l0_did_wake, true);
        }
    }
    return 0;
}

static void test_cross_lane_wake_delivered(const char *cert,
                                           const char *key)
{
    int before = failures;
    struct xlane_obs xo;
    memset(&xo, 0, sizeof(xo));
    atomic_store(&xo.wake_rc, 0x7fffffff); /* sentinel: not yet called */

    moq_msquic_managed_cfg_t cfg;
    moq_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.host = "127.0.0.1";
    cfg.port = 0;
    cfg.cert_path = cert;
    cfg.key_path = key;
    cfg.max_events = 64;
    cfg.max_connections = 2;
    cfg.lane_count = 2;
    cfg.on_lane_pump = xlane_pump;
    cfg.on_lane_pump_user = &xo;

    moq_msquic_managed_t *srv = NULL;
    CHECK(moq_msquic_managed_create(&cfg, &srv) == MOQ_OK);
    if (srv == NULL)
        return;
    moq_msquic_managed_lane_t *l0 = moq_msquic_managed_lane(srv, 0);
    xo.lane1 = moq_msquic_managed_lane(srv, 1);
    CHECK(l0 != NULL && xo.lane1 != NULL);

    /* 1. wake lane 1 so it enters its held pump */
    CHECK(moq_msquic_lane_wake(xo.lane1) == MOQ_OK);
    bool held = false;
    for (int i = 0; i < 2000; i++) {
        if (atomic_load(&xo.l1_holding)) {
            held = true;
            break;
        }
        struct timespec ts = { 0, 1 * 1000 * 1000 };
        nanosleep(&ts, NULL);
    }
    CHECK(held);

    /* 2. wake lane 0 so its pump issues the contended cross-lane wake */
    CHECK(moq_msquic_lane_wake(l0) == MOQ_OK);

    /* 3. lane 1 MUST pump again because the wake was recorded, not
     * dropped — this is the whole point of the guarantee */
    bool repumped = false;
    for (int i = 0; i < 4000; i++) {
        if (atomic_load(&xo.l1_pumps) >= 2) {
            repumped = true;
            break;
        }
        struct timespec ts = { 0, 1 * 1000 * 1000 };
        nanosleep(&ts, NULL);
    }
    int rc = atomic_load(&xo.wake_rc);

    (void)moq_msquic_managed_stop(srv);
    moq_msquic_managed_destroy(srv);

    CHECK(rc == MOQ_OK);   /* the cross-lane wake returned OK ... */
    CHECK(repumped);       /* ... AND actually scheduled lane 1 again */

    if (failures == before)
        printf("PASS: cross_lane_wake_delivered\n");
}

/* --- pin 5: cross-lane wake is bounded even with a far deadline ------ *
 * The target lane carries a real, far MoQ session deadline (a subgroup
 * opened under a 30 s delivery timeout). A cross-lane wake to it must be
 * delivered within the doorbell's idle cap, NOT deferred to that far
 * deadline — the exact case where an unbounded deadline-branch wait would
 * lose the wake until the session timer fires. */

struct dl_obs {
    atomic_int l1_pumps;
    atomic_bool l1_have_sub;
    atomic_bool l1_updated;      /* server saw SUBSCRIBE_UPDATED */
    atomic_bool l1_sg_open;      /* subgroup opened: far deadline pending */
    _Atomic uint64_t l1_deadline; /* verified session deadline (in-pump) */
    atomic_bool l0_did_wake;
    atomic_int wake_rc;
    moq_msquic_managed_lane_t *lane1;
    moq_subscription_t sub;      /* server sub; only lane 1's pump touches it */
};

struct dl_client {
    moq_subscription_t sub;
    bool subscribed;
    bool updated;
};

static uint32_t choose_lane_one(moq_msquic_managed_t *m,
                                const moq_msquic_accept_info_t *info,
                                void *user)
{
    (void)m;
    (void)info;
    (void)user;
    return 1u; /* the single connection lives on lane 1 */
}

static int dl_client_pump(moq_msquic_managed_t *m,
                          moq_msquic_managed_lane_t *lane, uint64_t now,
                          void *user)
{
    struct dl_client *c = user;
    moq_session_t *s = moq_msquic_managed_session(m);

    (void)lane;
    if (s == NULL)
        return 0;
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SETUP_COMPLETE && !c->subscribed) {
            static const moq_bytes_t ns[] = {
                { (const uint8_t *)"lane", 4 },
            };
            moq_subscribe_cfg_t sc;

            moq_subscribe_cfg_init(&sc);
            sc.track_namespace.parts = (moq_bytes_t *)ns;
            sc.track_namespace.count = 1;
            sc.track_name = (moq_bytes_t){ (const uint8_t *)"t", 1 };
            sc.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
            if (moq_session_subscribe(s, &sc, now, &c->sub) == 0)
                c->subscribed = true;
        } else if (ev.kind == MOQ_EVENT_SUBSCRIBE_OK && !c->updated) {
            /* ask for a far delivery timeout: the server's subscription
             * picks it up, so its next subgroup carries that deadline */
            moq_subscription_update_cfg_t uc;

            moq_subscription_update_cfg_init(&uc);
            uc.has_delivery_timeout = true;
            uc.delivery_timeout_us = 30ull * 1000000ull; /* 30 s */
            if (moq_session_update_subscription(s, c->sub, &uc, now) == 0)
                c->updated = true;
        }
        moq_event_cleanup(&ev);
    }
    return 0;
}

static int dl_server_pump(moq_msquic_managed_t *m,
                          moq_msquic_managed_lane_t *lane, uint64_t now,
                          void *user)
{
    struct dl_obs *o = user;
    uint32_t li = moq_msquic_lane_index(lane);

    (void)m;
    if (li == 0) {
        if (atomic_load(&o->l1_sg_open) && !atomic_load(&o->l0_did_wake)) {
            int rc = (int)moq_msquic_lane_wake(o->lane1);

            atomic_store(&o->wake_rc, rc);
            atomic_store(&o->l0_did_wake, true);
        }
        return 0;
    }

    atomic_fetch_add(&o->l1_pumps, 1);
    for (moq_msquic_managed_conn_t *c = moq_msquic_lane_next_conn(lane, NULL);
         c != NULL; c = moq_msquic_lane_next_conn(lane, c)) {
        moq_session_t *s = moq_msquic_managed_conn_session(c);

        if (s == NULL)
            continue;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST &&
                !atomic_load(&o->l1_have_sub)) {
                moq_accept_subscribe_cfg_t ac;

                o->sub = ev.u.subscribe_request.sub;
                moq_accept_subscribe_cfg_init(&ac);
                (void)moq_session_accept_subscribe(s, o->sub, &ac, now);
                atomic_store(&o->l1_have_sub, true);
            } else if (ev.kind == MOQ_EVENT_SUBSCRIBE_UPDATED) {
                atomic_store(&o->l1_updated, true);
            }
            moq_event_cleanup(&ev);
        }
        /* once the delivery timeout is in force, open a subgroup and leave
         * it open — that gives the session a lasting far deadline */
        if (atomic_load(&o->l1_updated) && !atomic_load(&o->l1_sg_open)) {
            moq_subgroup_cfg_t sgc;
            moq_subgroup_handle_t sg;

            moq_subgroup_cfg_init(&sgc);
            sgc.group_id = 0;
            sgc.publisher_priority = 200;
            if (moq_session_open_subgroup(s, o->sub, &sgc, now, &sg) == 0) {
                atomic_store(&o->l1_deadline,
                             moq_session_next_deadline_us(s));
                atomic_store(&o->l1_sg_open, true);
            }
        }
    }
    return 0;
}

static void test_cross_lane_wake_deadline_bounded(const char *cert,
                                                  const char *key)
{
    int before = failures;
    struct dl_obs o;
    struct dl_client dc;
    memset(&o, 0, sizeof(o));
    memset(&dc, 0, sizeof(dc));
    atomic_store(&o.wake_rc, 0x7fffffff);

    moq_msquic_managed_cfg_t cfg;
    moq_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.host = "127.0.0.1";
    cfg.port = 0;
    cfg.cert_path = cert;
    cfg.key_path = key;
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 16;
    cfg.max_events = 64;
    cfg.max_connections = 2;
    cfg.lane_count = 2;
    cfg.choose_lane = choose_lane_one; /* the conn lands on lane 1 */
    cfg.idle_timeout_ms = 30000; /* keep the transport alive through the test */
    cfg.on_lane_pump = dl_server_pump;
    cfg.on_lane_pump_user = &o;

    moq_msquic_managed_t *srv = NULL;
    CHECK(moq_msquic_managed_create(&cfg, &srv) == MOQ_OK);
    if (srv == NULL)
        return;
    uint16_t port = moq_msquic_managed_port(srv);
    /* lane 0 stays empty (it only issues the cross-lane wake); the client
     * connection is placed on lane 1 by choose_lane_one */
    moq_msquic_managed_lane_t *l0 = moq_msquic_managed_lane(srv, 0);
    o.lane1 = moq_msquic_managed_lane(srv, 1);
    CHECK(l0 != NULL && o.lane1 != NULL);

    moq_msquic_managed_t *cli = NULL;
    moq_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.host = "127.0.0.1";
    cfg.port = port;
    cfg.insecure_skip_verify = true;
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 16;
    cfg.max_events = 64;
    cfg.idle_timeout_ms = 30000;
    cfg.on_lane_pump = dl_client_pump;
    cfg.on_lane_pump_user = &dc;
    CHECK(moq_msquic_managed_create(&cfg, &cli) == MOQ_OK);

    /* drive until the server has opened the subgroup (far deadline live) */
    for (int i = 0; i < 4000 && !atomic_load(&o.l1_sg_open); i++) {
        (void)moq_msquic_managed_wake(srv);
        (void)moq_msquic_managed_wake(cli);
        struct timespec ts = { 0, 3 * 1000 * 1000 };
        nanosleep(&ts, NULL);
    }
    CHECK(atomic_load(&o.l1_sg_open));
    uint64_t dl = atomic_load(&o.l1_deadline);
    CHECK(dl != UINT64_MAX); /* the deadline is real and finite */

    /* let lane 1 quiesce: stop driving and wait for its pump count to
     * settle, so the doorbell is genuinely idle-sleeping on the far
     * deadline when the cross-lane wake arrives */
    int last = atomic_load(&o.l1_pumps);
    for (int stable = 0; stable < 40;) {
        struct timespec ts = { 0, 10 * 1000 * 1000 };
        nanosleep(&ts, NULL);
        int cur = atomic_load(&o.l1_pumps);
        if (cur == last) {
            stable++;
        } else {
            stable = 0;
            last = cur;
        }
    }
    int b1 = atomic_load(&o.l1_pumps);

    /* cross-lane wake from lane 0's callback to lane 1 (idle, far
     * deadline). Waking lane 0 only schedules its doorbell; wait until it
     * has actually issued the cross-lane wake before timing delivery, so
     * lane 0's own scheduling latency is not charged to the bound. */
    (void)moq_msquic_lane_wake(l0);
    for (int i = 0; i < 3000 && !atomic_load(&o.l0_did_wake); i++) {
        struct timespec ts = { 0, 1 * 1000 * 1000 };
        nanosleep(&ts, NULL);
    }
    int rc = atomic_load(&o.wake_rc);

    /* from the moment the wake was issued, lane 1 must pump well within
     * the far deadline — with the bounded idle wait it lands within the
     * cap; an unbounded deadline wait would defer it ~30 s */
    bool repumped = false;
    for (int i = 0; i < 2000; i++) {
        if (atomic_load(&o.l1_pumps) > b1) {
            repumped = true;
            break;
        }
        struct timespec ts = { 0, 1 * 1000 * 1000 };
        nanosleep(&ts, NULL);
    }

    (void)moq_msquic_managed_stop(cli);
    moq_msquic_managed_destroy(cli);
    (void)moq_msquic_managed_stop(srv);
    moq_msquic_managed_destroy(srv);

    CHECK(dl != UINT64_MAX);
    CHECK(rc == MOQ_OK);
    CHECK(repumped); /* delivered within the cap despite a ~30 s deadline */

    if (failures == before)
        printf("PASS: cross_lane_wake_deadline_bounded\n");
}

/* --- pin 6: on_activity is OUTSIDE the pump window (confinement) ----- *
 * on_activity runs on the lane's doorbell thread with mgd_tls_lane still
 * set and the sweep snapshot still live, but OUTSIDE the pump window. The
 * per-connection lane API must refuse it (unlike a same-lane lane_wake,
 * which must still arm cleanly). */

struct act_obs {
    /* Published by the pump (doorbell thread) and read by on_activity,
     * which also fires on the MsQuic LISTENER thread outside lane->mu, so
     * these are shared cross-thread: atomic, lane before conn (release), and
     * acquired together in on_activity. */
    _Atomic(moq_msquic_managed_lane_t *) lane;   /* published in-pump */
    _Atomic(moq_msquic_managed_conn_t *) conn;   /* published in-pump */
    /* pump-thread-only observer state (never read off the doorbell) */
    bool have_sub;
    moq_subscription_t sub;
    bool published;
    /* on_activity observations */
    atomic_bool act_ran;
    atomic_bool act_next_conn_null;
    atomic_bool act_conn_session_null;
    atomic_int  act_wake_rc;
};

static int act_server_pump(moq_msquic_managed_t *m,
                           moq_msquic_managed_lane_t *lane, uint64_t now,
                           void *user)
{
    struct act_obs *o = user;

    (void)m;
    /* lane is available first; conn is published (release) after, so a
     * reader that acquire-loads a non-NULL conn also sees lane. */
    atomic_store_explicit(&o->lane, lane, memory_order_release);
    for (moq_msquic_managed_conn_t *c = moq_msquic_lane_next_conn(lane, NULL);
         c != NULL; c = moq_msquic_lane_next_conn(lane, c)) {
        if (atomic_load_explicit(&o->conn, memory_order_acquire) == NULL)
            atomic_store_explicit(&o->conn, c, memory_order_release);
        moq_session_t *s = moq_msquic_managed_conn_session(c);
        if (s == NULL)
            continue;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST && !o->have_sub) {
                o->sub = ev.u.subscribe_request.sub;
                o->have_sub = true;
            }
            moq_event_cleanup(&ev);
        }
        if (o->have_sub && !o->published) {
            moq_accept_subscribe_cfg_t ac;
            moq_subgroup_cfg_t sgc;
            moq_subgroup_handle_t sg;

            moq_accept_subscribe_cfg_init(&ac);
            if (moq_session_accept_subscribe(s, o->sub, &ac, now) < 0)
                continue;
            moq_subgroup_cfg_init(&sgc);
            sgc.group_id = 0;
            sgc.publisher_priority = 200;
            if (moq_session_open_subgroup(s, o->sub, &sgc, now, &sg) < 0)
                continue;
            uint8_t *bytes = malloc(OBJECT_SIZE);
            moq_rcbuf_t *buf = NULL;

            if (bytes != NULL) {
                payload_fill(bytes, OBJECT_SIZE, 0);
                if (moq_rcbuf_create(moq_alloc_default(), bytes,
                                     OBJECT_SIZE, &buf) == 0) {
                    (void)moq_session_write_object(s, sg, 0, buf, now);
                    moq_rcbuf_decref(buf);
                }
                free(bytes);
            }
            (void)moq_session_close_subgroup(s, sg, now);
            o->published = true;
        }
    }
    return 0;
}

static void act_server_activity(moq_msquic_managed_t *m, void *ctx)
{
    struct act_obs *o = ctx;

    (void)m;
    /* acquire conn and lane ONCE into locals (conn's release publishes lane
     * too), validate, then use the locals consistently. */
    moq_msquic_managed_conn_t *conn =
        atomic_load_explicit(&o->conn, memory_order_acquire);
    moq_msquic_managed_lane_t *lane =
        atomic_load_explicit(&o->lane, memory_order_acquire);

    if (atomic_load(&o->act_ran) || conn == NULL || lane == NULL)
        return;
    /* OUTSIDE the pump window: iteration and every conn accessor refuse */
    atomic_store(&o->act_next_conn_null,
                 moq_msquic_lane_next_conn(lane, NULL) == NULL);
    atomic_store(&o->act_conn_session_null,
                 moq_msquic_managed_conn_session(conn) == NULL);
    /* a stray close from on_activity must be a no-op (does not tear down
     * the conn — proved by the client still receiving its object) */
    moq_msquic_managed_conn_close(conn, 7);
    /* but a same-lane lane_wake from on_activity must still arm cleanly
     * (this is why mgd_tls_lane must stay set here — no deadlock) */
    atomic_store(&o->act_wake_rc, (int)moq_msquic_lane_wake(lane));
    atomic_store(&o->act_ran, true);
}

static void test_on_activity_confined(const char *cert, const char *key)
{
    int before = failures;
    struct act_obs o;
    memset(&o, 0, sizeof(o));   /* the non-atomic pump-thread fields */
    atomic_init(&o.lane, NULL);
    atomic_init(&o.conn, NULL);
    atomic_init(&o.act_ran, false);
    atomic_init(&o.act_next_conn_null, false);
    atomic_init(&o.act_conn_session_null, false);
    atomic_init(&o.act_wake_rc, 0x7fffffff);

    moq_msquic_managed_cfg_t cfg;
    moq_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.host = "127.0.0.1";
    cfg.port = 0;
    cfg.cert_path = cert;
    cfg.key_path = key;
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 16;
    cfg.max_events = 64;
    cfg.max_connections = 1;
    cfg.lane_count = 1;
    cfg.on_lane_pump = act_server_pump;
    cfg.on_lane_pump_user = &o;
    cfg.on_activity = act_server_activity;
    cfg.on_activity_ctx = &o;

    moq_msquic_managed_t *srv = NULL;
    CHECK(moq_msquic_managed_create(&cfg, &srv) == MOQ_OK);
    if (srv == NULL)
        return;
    uint16_t port = moq_msquic_managed_port(srv);

    struct client_state cst;
    moq_msquic_managed_t *cli = NULL;
    memset(&cst, 0, sizeof(cst));
    moq_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.host = "127.0.0.1";
    cfg.port = port;
    cfg.insecure_skip_verify = true;
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 16;
    cfg.max_events = 64;
    cfg.on_lane_pump = client_lane_pump;
    cfg.on_lane_pump_user = &cst;
    CHECK(moq_msquic_managed_create(&cfg, &cli) == MOQ_OK);

    for (int iter = 0; iter < 6000; iter++) {
        if (cst.received >= 1 && atomic_load(&o.act_ran))
            break;
        (void)moq_msquic_managed_wake(srv);
        (void)moq_msquic_managed_wake(cli);
        struct timespec ts = { 0, 3 * 1000 * 1000 };
        nanosleep(&ts, NULL);
    }

    bool ran = atomic_load(&o.act_ran);
    bool nc = atomic_load(&o.act_next_conn_null);
    bool cs = atomic_load(&o.act_conn_session_null);
    int wrc = atomic_load(&o.act_wake_rc);
    int recv = cst.received;

    if (cli != NULL) {
        (void)moq_msquic_managed_stop(cli);
        moq_msquic_managed_destroy(cli);
    }
    (void)moq_msquic_managed_stop(srv);
    moq_msquic_managed_destroy(srv);

    CHECK(ran);            /* on_activity actually ran the checks */
    CHECK(nc);             /* lane_next_conn refused from on_activity */
    CHECK(cs);             /* conn_session refused from on_activity */
    CHECK(wrc == MOQ_OK);  /* same-lane lane_wake from on_activity OK */
    CHECK(recv >= 1);      /* conn_close from on_activity was a no-op */

    if (failures == before)
        printf("PASS: on_activity_confined\n");
}

/* --- harness --------------------------------------------------------- */

int main(int argc, char **argv)
{
    if (argc < 3) {
        fprintf(stderr, "usage: %s <cert.pem> <key.pem>\n", argv[0]);
        return 2;
    }
    const char *cert = argv[1], *key = argv[2];

    /* one library reference for the whole process: each sub-pin opens and
     * closes managed facades, and letting MsQuic's global refcount bounce
     * to zero between them re-initializes its datapath each time — churn
     * that has shown rare transient connect failures. Never released;
     * reachable until exit. */
    static const QUIC_API_TABLE *lib_pin;

    if (QUIC_FAILED(MsQuicOpen2(&lib_pin))) {
        fprintf(stderr, "MsQuicOpen2 failed\n");
        return 2;
    }

    test_partition(cert, key);
    test_invalid_choose_lane_refuses(cert, key);
    test_lane_wake_selectivity(cert, key);
    test_cross_lane_wake_delivered(cert, key);
    test_cross_lane_wake_deadline_bounded(cert, key);
    test_on_activity_confined(cert, key);

    if (failures == 0)
        printf("PASS: msquic_lanes\n");
    else
        fprintf(stderr, "FAIL: msquic_lanes (%d)\n", failures);
    return failures == 0 ? 0 : 1;
}
