/*
 * Real-MsQuic loopback proof of INBOUND receive backpressure recovery.
 *
 * A managed server subscribes and STALLS its receiver (after SUBSCRIBE_OK
 * it stops polling session events, so received objects back the bridge up
 * and the adapter arrests delivery by accepting zero bytes). A managed
 * client publishes a fixed set of 4096-byte objects, more than the
 * adapter's ~1 MiB in-flight send budget can hold at once, so the sender
 * is backpressured while the receiver is stalled. On release the whole
 * set is delivered.
 *
 * What this proves (claims kept narrow on purpose):
 *   - a stalled receiver eventually backpressures the sender: the
 *     publisher's transport-accepted bytes (lane flush_bytes) plateau
 *     below the full workload while the SESSION accepts every object;
 *   - while stalled, the receiving APPLICATION observes no object
 *     delivery (it is not polling);
 *   - on release the ENTIRE workload is delivered BYTE-EXACT and
 *     EXACTLY ONCE, with no loss and no duplication.
 *
 * What this does NOT claim: it does not measure peer QUIC stream flow-
 * control credit at the wire. The production receive window (16 MiB)
 * exceeds this workload, and flush_bytes / objects_drained are adapter-
 * and application-level signals, not transport credit counters. The
 * exact receive-arrest mechanics (partial accept, hold, resume,
 * multi-buffer stop-and-hold, resume-failure fatal) are proven
 * deterministically in the unit rail (test_msquic_unit.c). This test is
 * the end-to-end recovery confirmation over real MsQuic.
 */

#if defined(__linux__) && !defined(_POSIX_C_SOURCE)
#define _POSIX_C_SOURCE 200809L
#endif

#include <pthread.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <moq/msquic_managed.h>
#include <moq/rcbuf.h>

static int failures = 0;

#define CHECK(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)

enum { OBJECT_SIZE = 4096 };
enum { N_OBJECTS = 384 };                 /* > the ~1 MiB / ~256-object
                                             in-flight send budget */
enum { WORKLOAD_BYTES = N_OBJECTS * OBJECT_SIZE };
enum { SENDER_MAX_ACTIONS = 1024 };       /* session queue never the limiter */

static void payload_fill(uint8_t *dst, size_t size, uint64_t idx)
{
    for (size_t i = 0; i < 8 && i < size; i++)
        dst[i] = (uint8_t)(idx >> (8 * i));
    for (size_t j = 8; j < size; j++)
        dst[j] = (uint8_t)(idx * 131u + j * 7u + 47u);
}

/* --- server: subscribe, stall the receiver, then release ----------- */

struct server_side {
    pthread_mutex_t mu;
    moq_msquic_managed_t *m;
    int setup;
    bool subscribed;
    bool sub_ok;
    bool release;
    int drained;                 /* objects the application polled */
    int object_errors;           /* byte / id mismatches */
    int dup_objects;
    uint8_t seen[N_OBJECTS];
};

static int server_pump(moq_msquic_managed_t *m,
                       moq_msquic_managed_lane_t *lane, uint64_t now,
                       void *ctx)
{
    struct server_side *sv = ctx;
    moq_msquic_managed_conn_t *c = moq_msquic_lane_next_conn(lane, NULL);
    moq_session_t *s = c ? moq_msquic_managed_conn_session(c) : NULL;

    (void)m;
    pthread_mutex_lock(&sv->mu);
    bool may_drain = !sv->sub_ok || sv->release;
    if (s != NULL && may_drain) {
        moq_event_t ev;
        bool stop = false;
        while (!stop && moq_session_poll_events(s, &ev, 1) > 0) {
            switch (ev.kind) {
            case MOQ_EVENT_SETUP_COMPLETE: sv->setup++; break;
            case MOQ_EVENT_SUBSCRIBE_OK:
                sv->sub_ok = true;
                /* while stalled, stop polling right here so any inbound
                 * objects that arrived in the same batch stay queued in
                 * the session — the receiver truly delivers nothing until
                 * release, so the stall is exact (no transition leak) */
                if (!sv->release)
                    stop = true;
                break;
            case MOQ_EVENT_OBJECT_RECEIVED: {
                const moq_rcbuf_t *p = ev.u.object_received.payload;
                uint64_t oid = ev.u.object_received.object_id;

                if (p == NULL || moq_rcbuf_len(p) != OBJECT_SIZE) {
                    sv->object_errors++;
                } else {
                    uint8_t want[OBJECT_SIZE];
                    payload_fill(want, OBJECT_SIZE, oid);
                    if (memcmp(moq_rcbuf_data(p), want, OBJECT_SIZE) != 0)
                        sv->object_errors++;
                }
                if (oid < (uint64_t)N_OBJECTS && sv->seen[oid]++ != 0)
                    sv->dup_objects++;
                sv->drained++;
                break;
            }
            default: break;
            }
            moq_event_cleanup(&ev);
        }
    }
    if (s != NULL && sv->setup > 0 && !sv->subscribed) {
        static const moq_bytes_t ns[] = { { (const uint8_t *)"msq", 3 } };
        moq_subscribe_cfg_t sc;
        moq_subscription_t sub;
        moq_subscribe_cfg_init(&sc);
        sc.track_namespace.parts = (moq_bytes_t *)ns;
        sc.track_namespace.count = 1;
        sc.track_name = (moq_bytes_t){ (const uint8_t *)"track", 5 };
        sc.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        sv->subscribed = true;
        (void)moq_session_subscribe(s, &sc, now, &sub);
    }
    pthread_mutex_unlock(&sv->mu);
    return 0;
}

/* --- client: accept + publish the whole set ------------------------ */

struct client_side {
    pthread_mutex_t mu;
    moq_msquic_managed_t *m;
    int setup;
    moq_subscription_t sub;
    bool have_sub, accepted, subgroup_open;
    moq_subgroup_handle_t sg;
    uint64_t published;
    int publish_errors;
    uint64_t pumps;              /* completed pump generations */
};

static int client_pump(moq_msquic_managed_t *m,
                       moq_msquic_managed_lane_t *lane, uint64_t now,
                       void *ctx)
{
    struct client_side *cl = ctx;
    moq_session_t *s = moq_msquic_managed_session(m);

    (void)lane;
    pthread_mutex_lock(&cl->mu);
    cl->pumps++;
    moq_event_t ev;
    while (s != NULL && moq_session_poll_events(s, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SETUP_COMPLETE) cl->setup++;
        else if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
            cl->sub = ev.u.subscribe_request.sub;
            cl->have_sub = true;
        }
        moq_event_cleanup(&ev);
    }
    if (s != NULL && cl->have_sub && !cl->accepted) {
        moq_accept_subscribe_cfg_t ac;
        cl->accepted = true;
        moq_accept_subscribe_cfg_init(&ac);
        if (moq_session_accept_subscribe(s, cl->sub, &ac, now) < 0)
            cl->publish_errors++;
    }
    if (s != NULL && cl->accepted && !cl->subgroup_open) {
        moq_subgroup_cfg_t sgc;
        moq_subgroup_cfg_init(&sgc);
        sgc.group_id = 0;
        sgc.publisher_priority = 200;
        if (moq_session_open_subgroup(s, cl->sub, &sgc, now, &cl->sg) < 0)
            cl->publish_errors++;
        else
            cl->subgroup_open = true;
    }
    while (s != NULL && cl->subgroup_open && cl->published < N_OBJECTS) {
        uint8_t *bytes = malloc(OBJECT_SIZE);
        moq_rcbuf_t *buf = NULL;
        if (bytes == NULL) { cl->publish_errors++; break; }
        payload_fill(bytes, OBJECT_SIZE, cl->published);
        int rc = moq_rcbuf_create(moq_alloc_default(), bytes, OBJECT_SIZE,
                                  &buf);
        free(bytes);
        if (rc < 0) { cl->publish_errors++; break; }
        moq_result_t wr =
            moq_session_write_object(s, cl->sg, cl->published, buf, now);
        moq_rcbuf_decref(buf);
        if (wr == MOQ_OK) cl->published++;
        else break;
    }
    pthread_mutex_unlock(&cl->mu);
    return 0;
}

static uint64_t client_flush_bytes(struct client_side *cl)
{
    moq_msquic_managed_lane_t *lane = moq_msquic_managed_lane(cl->m, 0);
    moq_msquic_lane_stats_t st;
    if (lane == NULL ||
        moq_msquic_lane_get_stats(lane, &st, sizeof(st)) != MOQ_OK)
        return UINT64_MAX;
    return st.flush_bytes;
}

static uint64_t client_published(struct client_side *cl)
{
    pthread_mutex_lock(&cl->mu);
    uint64_t p = cl->published;
    pthread_mutex_unlock(&cl->mu);
    return p;
}

static uint64_t client_pumps(struct client_side *cl)
{
    pthread_mutex_lock(&cl->mu);
    uint64_t p = cl->pumps;
    pthread_mutex_unlock(&cl->mu);
    return p;
}

static int server_drained(struct server_side *sv)
{
    pthread_mutex_lock(&sv->mu);
    int d = sv->drained;
    pthread_mutex_unlock(&sv->mu);
    return d;
}

static void kick(struct server_side *sv, struct client_side *cl)
{
    (void)moq_msquic_managed_wake(sv->m);
    (void)moq_msquic_managed_wake(cl->m);
    (void)moq_msquic_managed_wait(cl->m, 5 * 1000);
    (void)moq_msquic_managed_wait(sv->m, 5 * 1000);
}

static void cfg_side(moq_msquic_managed_cfg_t *cfg, moq_perspective_t persp,
                     uint16_t port, const char *cert, const char *key,
                     moq_msquic_lane_pump_fn pump, void *user,
                     uint32_t max_actions)
{
    moq_msquic_managed_cfg_init_sized(cfg, sizeof(*cfg));
    cfg->alloc = moq_alloc_default();
    cfg->perspective = persp;
    cfg->host = "127.0.0.1";
    cfg->port = port;
    cfg->cert_path = cert;
    cfg->key_path = key;
    cfg->insecure_skip_verify = (persp == MOQ_PERSPECTIVE_CLIENT);
    cfg->on_lane_pump = pump;
    cfg->on_lane_pump_user = user;
    cfg->send_request_capacity = true;
    cfg->initial_request_capacity = 16;
    cfg->max_events = 64;
    cfg->max_actions = max_actions;
}

static void t_stalled_receiver_recovers(const char *cert, const char *key)
{
    int before = failures;
    struct server_side sv;
    struct client_side cl;

    memset(&sv, 0, sizeof(sv));
    memset(&cl, 0, sizeof(cl));
    pthread_mutex_init(&sv.mu, NULL);
    pthread_mutex_init(&cl.mu, NULL);

    moq_msquic_managed_cfg_t scfg;
    cfg_side(&scfg, MOQ_PERSPECTIVE_SERVER, 0, cert, key, server_pump, &sv,
             0);
    scfg.max_events = 4; /* small receive queue: the receiver pauses early */
    CHECK(moq_msquic_managed_create(&scfg, &sv.m) == MOQ_OK);
    if (sv.m == NULL)
        return;
    uint16_t port = moq_msquic_managed_port(sv.m);
    CHECK(port != 0);

    moq_msquic_managed_cfg_t ccfg;
    cfg_side(&ccfg, MOQ_PERSPECTIVE_CLIENT, port, cert, key, client_pump,
             &cl, SENDER_MAX_ACTIONS);
    CHECK(moq_msquic_managed_create(&ccfg, &cl.m) == MOQ_OK);
    if (cl.m == NULL) {
        (void)moq_msquic_managed_stop(sv.m);
        moq_msquic_managed_destroy(sv.m);
        return;
    }

    /* Phase 1: publish the whole set into the session while the receiver
     * is stalled. The session accepts everything; the transport does not. */
    bool all_written = false;
    for (int i = 0; i < 6000 && !all_written; i++) {
        kick(&sv, &cl);
        all_written = client_published(&cl) >= N_OBJECTS;
    }
    CHECK(all_written);

    /* Keep the receiver stalled and wait for the publisher's transport-accepted
     * bytes to reach a STABLE, NONZERO plateau strictly below the workload: the
     * transport admitted a bounded prefix (the ~1 MiB send budget) and does NOT
     * advance while the receiver holds the rest off, even though the session
     * accepted every object. The plateau is defined as N consecutive COMPLETED
     * pump generations with NO advance in flush_bytes; ANY advance resets the
     * stable count. This waits for genuine stability instead of comparing
     * timing-sensitive point samples (which could catch flush_bytes mid-climb).
     * Pump-generation-counted, never sleep-based. */
    enum { PLATEAU_STABLE_GENS = 40, PLATEAU_FLOOR = 512 * 1024 };
    uint64_t plateau = client_flush_bytes(&cl);
    uint64_t last_pumps = client_pumps(&cl);
    int stable_gens = 0;
    for (int i = 0; i < 200000 && stable_gens < PLATEAU_STABLE_GENS; i++) {
        kick(&sv, &cl);
        uint64_t pumps = client_pumps(&cl);
        if (pumps == last_pumps)
            continue;                    /* no completed pump generation yet */
        last_pumps = pumps;
        uint64_t f = client_flush_bytes(&cl);
        if (f != plateau) {              /* transport advanced: not yet stable */
            plateau = f;
            stable_gens = 0;
        } else {
            stable_gens++;
        }
    }
    int drained_stall = server_drained(&sv);

    CHECK(stable_gens >= PLATEAU_STABLE_GENS);    /* a real plateau formed */
    CHECK(plateau != UINT64_MAX);
    CHECK(plateau >= (uint64_t)PLATEAU_FLOOR);    /* nonzero, a real prefix, not
                                                    a trivial few objects */
    CHECK(plateau < (uint64_t)WORKLOAD_BYTES);    /* below the workload */
    CHECK(drained_stall == 0);                    /* app delivered nothing */

    printf("STALLED: published=%llu, transport flush_bytes plateau=%llu "
           "(>= %d, < workload=%d) stable across %d pump-gens, app drained=%d\n",
           (unsigned long long)client_published(&cl),
           (unsigned long long)plateau, PLATEAU_FLOOR, WORKLOAD_BYTES,
           PLATEAU_STABLE_GENS, drained_stall);

    /* Phase 2: release -> the entire workload is delivered byte-exact and
     * exactly once */
    pthread_mutex_lock(&sv.mu);
    sv.release = true;
    pthread_mutex_unlock(&sv.mu);
    bool full = false;
    for (int i = 0; i < 8000 && !full; i++) {
        kick(&sv, &cl);
        full = server_drained(&sv) >= N_OBJECTS;
    }
    CHECK(full);

    bool cl_fatal = moq_msquic_managed_is_fatal(cl.m);
    bool sv_fatal = moq_msquic_managed_is_fatal(sv.m);
    (void)moq_msquic_managed_stop(cl.m);
    (void)moq_msquic_managed_stop(sv.m);
    moq_msquic_managed_destroy(cl.m);
    moq_msquic_managed_destroy(sv.m);

    CHECK(cl.setup == 1 && sv.setup == 1);
    CHECK(cl.publish_errors == 0);
    CHECK(cl.published == N_OBJECTS);
    CHECK(sv.drained == N_OBJECTS);   /* all delivered */
    CHECK(sv.dup_objects == 0);       /* exactly once */
    CHECK(sv.object_errors == 0);     /* byte-exact */
    int missing = 0;
    for (int i = 0; i < N_OBJECTS; i++)
        if (sv.seen[i] != 1)
            missing++;
    CHECK(missing == 0);              /* no loss */
    CHECK(!cl_fatal && !sv_fatal);

    printf("RELEASED: delivered=%d/%d byte-exact exactly-once (missing=%d "
           "dup=%d)\n",
           sv.drained, N_OBJECTS, missing, sv.dup_objects);

    pthread_mutex_destroy(&sv.mu);
    pthread_mutex_destroy(&cl.mu);
    if (failures == before)
        printf("PASS: stalled_receiver_recovers\n");
}

/* --- over-window continuous drain: real receive-credit crossing ------------- */

/* One subgroup stream carrying more than BOTH configured receive windows
 * (16 MiB stream, 32 MiB connection): 768 x 64 KiB = 48 MiB. A continuously
 * draining receiver with a small event queue forces repeated adapter
 * pause/resume so the peer's stream/connection flow-control windows must be
 * advanced past their initial ceilings for the transfer to complete at all.
 *
 * Scope: this case drives BOTH facades every iteration (big_kick wakes each),
 * so it proves the transport RECEIVE-CREDIT path recovers — the windows are
 * advanced and the full 48 MiB is delivered byte-exact. It does NOT prove
 * autonomous managed progress: the explicit per-iteration wakes stand in for
 * any lost scheduler re-drive. Autonomous delivery with no external receiver
 * wake is proven separately by autonomous_bounded_drain below. */
enum { BIG_OBJ = 64u * 1024u };
enum { BIG_N = 768 };                          /* 48 MiB total on one subgroup */
enum { BIG_STREAM_WIN = 16u * 1024u * 1024u }; /* StreamRecvWindowDefault */
enum { BIG_CONN_WIN = 32u * 1024u * 1024u };   /* ConnFlowControlWindow */

struct big_server {
    pthread_mutex_t mu;
    moq_msquic_managed_t *m;
    int setup;
    bool subscribed;
    int drained;
    int object_errors, dup_objects;
    uint64_t bytes_drained;
    uint8_t *seen;   /* BIG_N flags */
    uint8_t *want;   /* BIG_OBJ scratch for the byte-exact check */
};

static int big_server_pump(moq_msquic_managed_t *m,
                           moq_msquic_managed_lane_t *lane, uint64_t now,
                           void *ctx)
{
    struct big_server *sv = ctx;
    moq_msquic_managed_conn_t *c = moq_msquic_lane_next_conn(lane, NULL);
    moq_session_t *s = c ? moq_msquic_managed_conn_session(c) : NULL;
    (void)m;
    pthread_mutex_lock(&sv->mu);
    /* Continuous drain: poll (and consume) every pump. The small event queue
     * — not any test stall — is what forces the adapter to pause and resume. */
    moq_event_t ev;
    while (s != NULL && moq_session_poll_events(s, &ev, 1) > 0) {
        switch (ev.kind) {
        case MOQ_EVENT_SETUP_COMPLETE: sv->setup++; break;
        case MOQ_EVENT_OBJECT_RECEIVED: {
            const moq_rcbuf_t *p = ev.u.object_received.payload;
            uint64_t oid = ev.u.object_received.object_id;
            if (p == NULL || moq_rcbuf_len(p) != BIG_OBJ) {
                sv->object_errors++;
            } else {
                payload_fill(sv->want, BIG_OBJ, oid);
                if (memcmp(moq_rcbuf_data(p), sv->want, BIG_OBJ) != 0)
                    sv->object_errors++;
                else
                    sv->bytes_drained += BIG_OBJ;
            }
            if (oid < (uint64_t)BIG_N && sv->seen[oid]++ != 0)
                sv->dup_objects++;
            sv->drained++;
            break;
        }
        default: break;
        }
        moq_event_cleanup(&ev);
    }
    if (s != NULL && sv->setup > 0 && !sv->subscribed) {
        static const moq_bytes_t ns[] = { { (const uint8_t *)"msq", 3 } };
        moq_subscribe_cfg_t sc;
        moq_subscription_t sub;
        moq_subscribe_cfg_init(&sc);
        sc.track_namespace.parts = (moq_bytes_t *)ns;
        sc.track_namespace.count = 1;
        sc.track_name = (moq_bytes_t){ (const uint8_t *)"track", 5 };
        sc.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        sv->subscribed = true;
        (void)moq_session_subscribe(s, &sc, now, &sub);
    }
    pthread_mutex_unlock(&sv->mu);
    return 0;
}

struct big_client {
    pthread_mutex_t mu;
    moq_msquic_managed_t *m;
    int setup;
    moq_subscription_t sub;
    bool have_sub, accepted, subgroup_open;
    moq_subgroup_handle_t sg;
    uint64_t published;
    int publish_errors;
};

static int big_client_pump(moq_msquic_managed_t *m,
                           moq_msquic_managed_lane_t *lane, uint64_t now,
                           void *ctx)
{
    struct big_client *cl = ctx;
    moq_session_t *s = moq_msquic_managed_session(m);
    (void)lane;
    pthread_mutex_lock(&cl->mu);
    moq_event_t ev;
    while (s != NULL && moq_session_poll_events(s, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SETUP_COMPLETE) cl->setup++;
        else if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
            cl->sub = ev.u.subscribe_request.sub;
            cl->have_sub = true;
        }
        moq_event_cleanup(&ev);
    }
    if (s != NULL && cl->have_sub && !cl->accepted) {
        moq_accept_subscribe_cfg_t ac;
        cl->accepted = true;
        moq_accept_subscribe_cfg_init(&ac);
        if (moq_session_accept_subscribe(s, cl->sub, &ac, now) < 0)
            cl->publish_errors++;
    }
    if (s != NULL && cl->accepted && !cl->subgroup_open) {
        moq_subgroup_cfg_t sgc;
        moq_subgroup_cfg_init(&sgc);
        sgc.group_id = 0;
        sgc.publisher_priority = 200;
        if (moq_session_open_subgroup(s, cl->sub, &sgc, now, &cl->sg) < 0)
            cl->publish_errors++;
        else
            cl->subgroup_open = true;
    }
    /* One subgroup stream: write until the transport backs up (WOULD_BLOCK),
     * which only clears as the receiver drains and advances its window. */
    while (s != NULL && cl->subgroup_open && cl->published < (uint64_t)BIG_N) {
        uint8_t *bytes = malloc(BIG_OBJ);
        moq_rcbuf_t *buf = NULL;
        if (bytes == NULL) { cl->publish_errors++; break; }
        payload_fill(bytes, BIG_OBJ, cl->published);
        int rc = moq_rcbuf_create(moq_alloc_default(), bytes, BIG_OBJ, &buf);
        free(bytes);
        if (rc < 0) { cl->publish_errors++; break; }
        moq_result_t wr =
            moq_session_write_object(s, cl->sg, cl->published, buf, now);
        moq_rcbuf_decref(buf);
        if (wr == MOQ_OK) cl->published++;
        else break;
    }
    pthread_mutex_unlock(&cl->mu);
    return 0;
}

static int big_drained(struct big_server *sv)
{
    pthread_mutex_lock(&sv->mu);
    int d = sv->drained;
    pthread_mutex_unlock(&sv->mu);
    return d;
}

static void big_kick(struct big_server *sv, struct big_client *cl)
{
    (void)moq_msquic_managed_wake(sv->m);
    (void)moq_msquic_managed_wake(cl->m);
    (void)moq_msquic_managed_wait(cl->m, 5 * 1000);
    (void)moq_msquic_managed_wait(sv->m, 5 * 1000);
}

static void t_over_window_continuous_drain(const char *cert, const char *key)
{
    int before = failures;
    struct big_server sv;
    struct big_client cl;
    memset(&sv, 0, sizeof(sv));
    memset(&cl, 0, sizeof(cl));
    pthread_mutex_init(&sv.mu, NULL);
    pthread_mutex_init(&cl.mu, NULL);
    sv.seen = calloc(BIG_N, 1);
    sv.want = malloc(BIG_OBJ);
    CHECK(sv.seen != NULL && sv.want != NULL);
    if (!sv.seen || !sv.want) { free(sv.seen); free(sv.want); return; }

    moq_msquic_managed_cfg_t scfg;
    cfg_side(&scfg, MOQ_PERSPECTIVE_SERVER, 0, cert, key, big_server_pump, &sv, 0);
    scfg.max_events = 4;   /* small receive queue -> repeated pause/resume */
    CHECK(moq_msquic_managed_create(&scfg, &sv.m) == MOQ_OK);
    if (sv.m == NULL) { free(sv.seen); free(sv.want); return; }
    uint16_t port = moq_msquic_managed_port(sv.m);
    CHECK(port != 0);

    moq_msquic_managed_cfg_t ccfg;
    cfg_side(&ccfg, MOQ_PERSPECTIVE_CLIENT, port, cert, key, big_client_pump,
             &cl, SENDER_MAX_ACTIONS);
    CHECK(moq_msquic_managed_create(&ccfg, &cl.m) == MOQ_OK);
    if (cl.m == NULL) {
        (void)moq_msquic_managed_stop(sv.m);
        moq_msquic_managed_destroy(sv.m);
        free(sv.seen); free(sv.want);
        return;
    }

    /* Drive to completion. 48 MiB on one stream cannot arrive without the
     * peer's stream (16 MiB) AND connection (32 MiB) windows being advanced
     * well past their initial ceilings, which only happens as the draining
     * receiver reads and the adapter resumes. Pump-generation bounded. */
    bool full = false;
    for (int i = 0; i < 200000 && !full; i++) {
        big_kick(&sv, &cl);
        full = big_drained(&sv) >= BIG_N;
    }
    CHECK(full);

    bool cl_fatal = moq_msquic_managed_is_fatal(cl.m);
    bool sv_fatal = moq_msquic_managed_is_fatal(sv.m);
    (void)moq_msquic_managed_stop(cl.m);
    (void)moq_msquic_managed_stop(sv.m);
    moq_msquic_managed_destroy(cl.m);
    moq_msquic_managed_destroy(sv.m);

    CHECK(cl.publish_errors == 0);
    CHECK(cl.published == (uint64_t)BIG_N);
    CHECK(sv.drained == BIG_N);                 /* all delivered */
    CHECK(sv.dup_objects == 0);                 /* exactly once */
    CHECK(sv.object_errors == 0);               /* byte-exact */
    CHECK(sv.bytes_drained == (uint64_t)BIG_N * BIG_OBJ);
    /* Progress crossed BOTH initial windows (else the transfer would stall). */
    CHECK(sv.bytes_drained > (uint64_t)BIG_STREAM_WIN);
    CHECK(sv.bytes_drained > (uint64_t)BIG_CONN_WIN);
    int missing = 0;
    for (int i = 0; i < BIG_N; i++)
        if (sv.seen[i] != 1) missing++;
    CHECK(missing == 0);                        /* no loss */
    CHECK(!cl_fatal && !sv_fatal);              /* no fatal state */

    printf("OVER-WINDOW: delivered=%d/%d (%llu bytes) byte-exact exactly-once, "
           "crossed stream=%d/conn=%d windows (missing=%d dup=%d)\n",
           sv.drained, BIG_N, (unsigned long long)sv.bytes_drained,
           BIG_STREAM_WIN, BIG_CONN_WIN, missing, sv.dup_objects);

    free(sv.seen);
    free(sv.want);
    pthread_mutex_destroy(&sv.mu);
    pthread_mutex_destroy(&cl.mu);
    if (failures == before)
        printf("PASS: over_window_continuous_drain\n");
}

/* --- autonomous bounded-poll delivery: the managed re-drive regression ------
 *
 * The exact shape of the original MsQuic gauntlet stall: ONE subscriber, ONE
 * subgroup, 1024 x 256-byte objects, DEFAULT session event capacity, and a
 * receiver that polls a BOUNDED batch (<= 16) per pump and is NEVER explicitly
 * woken after setup. Autonomous managed progress alone must deliver every
 * object.
 *
 * The doorbell runs one on_lane_pump, then a post-pump service pass that can
 * refill the session event queue from inbound the bridge buffered while the
 * queue was full. That refill produces app-consumable events but does not
 * itself arm another pump, and — the publisher having finished — no fresh
 * transport RECEIVE arrives to arm one either. So without an explicit re-drive
 * the transfer stalls on a 16-event boundary: the publisher completes, the
 * receiver stays established and nonfatal, and the buffered remainder never
 * reaches the application. This test requires the scheduler to sustain that
 * re-drive to 1024/1024 with no external receiver wake.
 *
 * Unlike over_window above, NOTHING here wakes the receiver during delivery —
 * that is the whole point: over_window's explicit big_kick() drives both
 * facades every iteration and so proves transport credit recovery while
 * MASKING a lost autonomous re-drive; this case proves the re-drive itself.
 */
enum { AUTO_OBJ = 256 };
enum { AUTO_N = 1024 };
enum { AUTO_POLL = 16 };   /* bounded per-pump event batch (never drain-loop) */

struct auto_server {
    pthread_mutex_t mu;
    moq_msquic_managed_t *m;
    int setup;
    bool subscribed;
    int drained, object_errors, dup_objects;
    uint8_t seen[AUTO_N];
};

static int auto_server_pump(moq_msquic_managed_t *m,
                            moq_msquic_managed_lane_t *lane, uint64_t now,
                            void *ctx)
{
    struct auto_server *sv = ctx;
    moq_msquic_managed_conn_t *c = moq_msquic_lane_next_conn(lane, NULL);
    moq_session_t *s = c ? moq_msquic_managed_conn_session(c) : NULL;

    (void)m;
    pthread_mutex_lock(&sv->mu);
    if (s != NULL) {
        /* BOUNDED poll: exactly one batch of up to AUTO_POLL events, never a
         * drain-until-empty loop — a dropped re-pump is only visible when the
         * app leaves events behind, as a real bounded consumer does. */
        moq_event_t ev[AUTO_POLL];
        size_t ne = 0;
        (void)moq_session_poll_events_ex(s, ev, AUTO_POLL, sizeof(ev[0]), &ne);
        for (size_t i = 0; i < ne; i++) {
            switch (ev[i].kind) {
            case MOQ_EVENT_SETUP_COMPLETE: sv->setup++; break;
            case MOQ_EVENT_OBJECT_RECEIVED: {
                const moq_rcbuf_t *p = ev[i].u.object_received.payload;
                uint64_t oid = ev[i].u.object_received.object_id;

                if (p == NULL || moq_rcbuf_len(p) != AUTO_OBJ) {
                    sv->object_errors++;
                } else {
                    uint8_t want[AUTO_OBJ];
                    payload_fill(want, AUTO_OBJ, oid);
                    if (memcmp(moq_rcbuf_data(p), want, AUTO_OBJ) != 0)
                        sv->object_errors++;
                }
                if (oid < (uint64_t)AUTO_N && sv->seen[oid]++ != 0)
                    sv->dup_objects++;
                sv->drained++;
                break;
            }
            default: break;
            }
            moq_event_cleanup(&ev[i]);
        }
        if (sv->setup > 0 && !sv->subscribed) {
            static const moq_bytes_t ns[] = { { (const uint8_t *)"msq", 3 } };
            moq_subscribe_cfg_t sc;
            moq_subscription_t sub;
            moq_subscribe_cfg_init(&sc);
            sc.track_namespace.parts = (moq_bytes_t *)ns;
            sc.track_namespace.count = 1;
            sc.track_name = (moq_bytes_t){ (const uint8_t *)"track", 5 };
            sc.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
            sv->subscribed = true;
            (void)moq_session_subscribe(s, &sc, now, &sub);
        }
    }
    pthread_mutex_unlock(&sv->mu);
    return 0;
}

struct auto_client {
    pthread_mutex_t mu;
    moq_msquic_managed_t *m;
    int setup;
    moq_subscription_t sub;
    bool have_sub, accepted, subgroup_open;
    moq_subgroup_handle_t sg;
    uint64_t published;
    int publish_errors;
};

static int auto_client_pump(moq_msquic_managed_t *m,
                            moq_msquic_managed_lane_t *lane, uint64_t now,
                            void *ctx)
{
    struct auto_client *cl = ctx;
    moq_session_t *s = moq_msquic_managed_session(m);

    (void)lane;
    pthread_mutex_lock(&cl->mu);
    moq_event_t ev;
    while (s != NULL && moq_session_poll_events(s, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SETUP_COMPLETE) cl->setup++;
        else if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
            cl->sub = ev.u.subscribe_request.sub;
            cl->have_sub = true;
        }
        moq_event_cleanup(&ev);
    }
    if (s != NULL && cl->have_sub && !cl->accepted) {
        moq_accept_subscribe_cfg_t ac;
        cl->accepted = true;
        moq_accept_subscribe_cfg_init(&ac);
        if (moq_session_accept_subscribe(s, cl->sub, &ac, now) < 0)
            cl->publish_errors++;
    }
    if (s != NULL && cl->accepted && !cl->subgroup_open) {
        moq_subgroup_cfg_t sgc;
        moq_subgroup_cfg_init(&sgc);
        sgc.group_id = 0;
        sgc.publisher_priority = 200;
        if (moq_session_open_subgroup(s, cl->sub, &sgc, now, &cl->sg) < 0)
            cl->publish_errors++;
        else
            cl->subgroup_open = true;
    }
    while (s != NULL && cl->subgroup_open && cl->published < (uint64_t)AUTO_N) {
        uint8_t bytes[AUTO_OBJ];
        moq_rcbuf_t *buf = NULL;
        payload_fill(bytes, AUTO_OBJ, cl->published);
        if (moq_rcbuf_create(moq_alloc_default(), bytes, AUTO_OBJ, &buf) < 0) {
            cl->publish_errors++;
            break;
        }
        moq_result_t wr =
            moq_session_write_object(s, cl->sg, cl->published, buf, now);
        moq_rcbuf_decref(buf);
        if (wr == MOQ_OK) cl->published++;
        else break;
    }
    pthread_mutex_unlock(&cl->mu);
    return 0;
}

static int auto_drained(struct auto_server *sv)
{
    pthread_mutex_lock(&sv->mu);
    int d = sv->drained;
    pthread_mutex_unlock(&sv->mu);
    return d;
}

static uint64_t auto_published(struct auto_client *cl)
{
    pthread_mutex_lock(&cl->mu);
    uint64_t p = cl->published;
    pthread_mutex_unlock(&cl->mu);
    return p;
}

static bool auto_established(struct auto_server *sv, struct auto_client *cl)
{
    pthread_mutex_lock(&sv->mu);
    bool s = sv->setup > 0 && sv->subscribed;
    pthread_mutex_unlock(&sv->mu);
    pthread_mutex_lock(&cl->mu);
    bool c = cl->have_sub;
    pthread_mutex_unlock(&cl->mu);
    return s && c;
}

static void t_autonomous_bounded_drain(const char *cert, const char *key,
                                       uint32_t recv_cap)
{
    int before = failures;
    struct auto_server sv;
    struct auto_client cl;

    memset(&sv, 0, sizeof(sv));
    memset(&cl, 0, sizeof(cl));
    pthread_mutex_init(&sv.mu, NULL);
    pthread_mutex_init(&cl.mu, NULL);

    moq_msquic_managed_cfg_t scfg;
    cfg_side(&scfg, MOQ_PERSPECTIVE_SERVER, 0, cert, key, auto_server_pump, &sv,
             0);
    /* recv_cap == 0 selects the session default event capacity (the gauntlet
     * shape); a large cap re-fills the queue less often but must still reach
     * 1024/1024 autonomously — the re-drive settles either way. */
    scfg.max_events = recv_cap;
    CHECK(moq_msquic_managed_create(&scfg, &sv.m) == MOQ_OK);
    if (sv.m == NULL) return;
    uint16_t port = moq_msquic_managed_port(sv.m);
    CHECK(port != 0);

    moq_msquic_managed_cfg_t ccfg;
    cfg_side(&ccfg, MOQ_PERSPECTIVE_CLIENT, port, cert, key, auto_client_pump,
             &cl, SENDER_MAX_ACTIONS);
    CHECK(moq_msquic_managed_create(&ccfg, &cl.m) == MOQ_OK);
    if (cl.m == NULL) {
        (void)moq_msquic_managed_stop(sv.m);
        moq_msquic_managed_destroy(sv.m);
        return;
    }

    /* Phase A — SETUP ONLY: drive the handshake and the subscribe handshake.
     * Waking the receiver is allowed HERE; it stops before any object is
     * published, so no delivery is charged to an explicit receiver wake. */
    bool ready = false;
    for (int i = 0; i < 40000 && !ready; i++) {
        (void)moq_msquic_managed_wake(sv.m);
        (void)moq_msquic_managed_wake(cl.m);
        (void)moq_msquic_managed_wait(cl.m, 2 * 1000);
        (void)moq_msquic_managed_wait(sv.m, 2 * 1000);
        ready = auto_established(&sv, &cl);
    }
    CHECK(ready);

    /* Phase B — publish + AUTONOMOUS receive. Wake ONLY the publisher; the
     * receiver is NEVER woken again. Its every pump is a transport-driven
     * autonomous one — precisely what the managed scheduler must sustain.
     * Plateau-bounded: a stall fails fast (no progress for a run of waits)
     * instead of spinning to the iteration cap. */
    bool full = false;
    int last = -1, stagnant = 0;
    for (int i = 0; i < 200000 && !full; i++) {
        (void)moq_msquic_managed_wake(cl.m);          /* publisher only */
        (void)moq_msquic_managed_wait(cl.m, 2 * 1000);
        (void)moq_msquic_managed_wait(sv.m, 2 * 1000);  /* WAIT, never WAKE */
        int d = auto_drained(&sv);
        full = d >= AUTO_N;
        if (d == last) {
            if (++stagnant > 500) break;   /* ~1s of no autonomous progress */
        } else {
            last = d;
            stagnant = 0;
        }
    }

    int drained = auto_drained(&sv);
    uint64_t published = auto_published(&cl);
    bool sv_fatal = moq_msquic_managed_is_fatal(sv.m);
    bool cl_fatal = moq_msquic_managed_is_fatal(cl.m);

    if (!full) {
        /* Causality of the stall, via PUBLIC discriminators only (no seam):
         * the transfer is not dead, it is un-pumped. The publisher completed,
         * the receiver is established and nonfatal, delivery stopped on a
         * 16-event boundary, and explicit receiver wakes advance it one
         * bounded batch at a time to completion. On the fixed scheduler this
         * branch is never taken — autonomous progress already reached AUTO_N. */
        fprintf(stderr,
                "AUTONOMOUS STALL: drained=%d/%d (=%d*%d + %d) published=%llu "
                "established=%d sv_fatal=%d\n",
                drained, AUTO_N, drained / AUTO_POLL, AUTO_POLL,
                drained % AUTO_POLL, (unsigned long long)published,
                auto_established(&sv, &cl), sv_fatal);
        CHECK(published == AUTO_N);            /* publisher done: data buffered */
        CHECK(auto_established(&sv, &cl));      /* receiver still established */
        CHECK(!sv_fatal);                      /* receiver alive, just idle */
        CHECK(drained < AUTO_N);               /* stalled below the workload */

        int d0 = drained;
        (void)moq_msquic_managed_wake(sv.m);   /* ONE explicit receiver wake */
        (void)moq_msquic_managed_wait(sv.m, 50 * 1000);
        int d1 = auto_drained(&sv);
        CHECK(d1 > d0);                        /* a wake advances the transfer */

        bool rec = false;                      /* repeated wakes complete it */
        for (int i = 0; i < 200000 && !rec; i++) {
            (void)moq_msquic_managed_wake(sv.m);
            (void)moq_msquic_managed_wait(sv.m, 5 * 1000);
            rec = auto_drained(&sv) >= AUTO_N;
        }
        CHECK(rec);
        drained = auto_drained(&sv);
    }

    (void)moq_msquic_managed_stop(cl.m);
    (void)moq_msquic_managed_stop(sv.m);
    moq_msquic_managed_destroy(cl.m);
    moq_msquic_managed_destroy(sv.m);

    /* The regression proper: autonomous progress alone delivered everything. */
    CHECK(full);
    CHECK(published == AUTO_N);
    CHECK(cl.publish_errors == 0);
    CHECK(drained == AUTO_N);            /* all delivered */
    CHECK(sv.dup_objects == 0);          /* exactly once */
    CHECK(sv.object_errors == 0);        /* byte-exact */
    int missing = 0;
    for (int i = 0; i < AUTO_N; i++)
        if (sv.seen[i] != 1)
            missing++;
    CHECK(missing == 0);                 /* no loss */
    CHECK(!cl_fatal && !sv_fatal);       /* no fatal state */

    printf("AUTONOMOUS[cap=%u]: delivered=%d/%d (published=%llu) byte-exact "
           "exactly-once, no external receiver wake (missing=%d dup=%d)\n",
           recv_cap, drained, AUTO_N, (unsigned long long)published, missing,
           sv.dup_objects);

    pthread_mutex_destroy(&sv.mu);
    pthread_mutex_destroy(&cl.mu);
    if (failures == before)
        printf("PASS: autonomous_bounded_drain[cap=%u]\n", recv_cap);
}

/* --- anti-spin: a non-draining receiver must not busy-loop the doorbell -----
 *
 * The re-drive is gated on the event-progress token: a receiver that frees no
 * event capacity advances the token zero times, so no pump is re-armed. This
 * proves the bound directly — with the publisher quiet (no new transport events)
 * and NO explicit receiver wake, a receiver that drains nothing must let its
 * doorbell go idle: its pump-sweep count barely moves. An unconditional re-arm
 * would spin here (a service pass every idle tick), which this test catches. */

struct nospin_server {
    pthread_mutex_t mu;
    moq_msquic_managed_t *m;
    int setup;
    bool subscribed;
    bool frozen;   /* once subscribed, never poll again (drain nothing) */
};

static int nospin_server_pump(moq_msquic_managed_t *m,
                              moq_msquic_managed_lane_t *lane, uint64_t now,
                              void *ctx)
{
    struct nospin_server *sv = ctx;
    moq_msquic_managed_conn_t *c = moq_msquic_lane_next_conn(lane, NULL);
    moq_session_t *s = c ? moq_msquic_managed_conn_session(c) : NULL;

    (void)m;
    pthread_mutex_lock(&sv->mu);
    if (s != NULL && !sv->frozen) {
        moq_event_t ev[AUTO_POLL];
        size_t ne = 0;
        (void)moq_session_poll_events_ex(s, ev, AUTO_POLL, sizeof(ev[0]), &ne);
        for (size_t i = 0; i < ne; i++) {
            if (ev[i].kind == MOQ_EVENT_SETUP_COMPLETE) sv->setup++;
            moq_event_cleanup(&ev[i]);
        }
        if (sv->setup > 0 && !sv->subscribed) {
            static const moq_bytes_t ns[] = { { (const uint8_t *)"msq", 3 } };
            moq_subscribe_cfg_t sc;
            moq_subscription_t sub;
            moq_subscribe_cfg_init(&sc);
            sc.track_namespace.parts = (moq_bytes_t *)ns;
            sc.track_namespace.count = 1;
            sc.track_name = (moq_bytes_t){ (const uint8_t *)"track", 5 };
            sc.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
            sv->subscribed = true;
            (void)moq_session_subscribe(s, &sc, now, &sub);
            sv->frozen = true;   /* stop draining: objects will queue and stall */
        }
    }
    pthread_mutex_unlock(&sv->mu);
    return 0;
}

static uint64_t lane_pump_sweeps(moq_msquic_managed_t *m)
{
    moq_msquic_managed_lane_t *lane = moq_msquic_managed_lane(m, 0);
    moq_msquic_lane_stats_t st;
    if (lane == NULL ||
        moq_msquic_lane_get_stats(lane, &st, sizeof(st)) != MOQ_OK)
        return UINT64_MAX;
    return st.pump_sweeps;
}

static void t_nondraining_no_spin(const char *cert, const char *key)
{
    int before = failures;
    struct nospin_server sv;
    struct auto_client cl;

    memset(&sv, 0, sizeof(sv));
    memset(&cl, 0, sizeof(cl));
    pthread_mutex_init(&sv.mu, NULL);
    pthread_mutex_init(&cl.mu, NULL);

    moq_msquic_managed_cfg_t scfg;
    cfg_side(&scfg, MOQ_PERSPECTIVE_SERVER, 0, cert, key, nospin_server_pump,
             &sv, 0);
    scfg.max_events = 0;
    CHECK(moq_msquic_managed_create(&scfg, &sv.m) == MOQ_OK);
    if (sv.m == NULL) return;
    uint16_t port = moq_msquic_managed_port(sv.m);
    CHECK(port != 0);

    moq_msquic_managed_cfg_t ccfg;
    cfg_side(&ccfg, MOQ_PERSPECTIVE_CLIENT, port, cert, key, auto_client_pump,
             &cl, SENDER_MAX_ACTIONS);
    CHECK(moq_msquic_managed_create(&ccfg, &cl.m) == MOQ_OK);
    if (cl.m == NULL) {
        (void)moq_msquic_managed_stop(sv.m);
        moq_msquic_managed_destroy(sv.m);
        return;
    }

    /* Drive setup + a publish burst. The receiver subscribes then freezes; its
     * event queue fills and the adapter arrests. */
    bool primed = false;
    for (int i = 0; i < 40000 && !primed; i++) {
        (void)moq_msquic_managed_wake(sv.m);
        (void)moq_msquic_managed_wake(cl.m);
        (void)moq_msquic_managed_wait(cl.m, 2 * 1000);
        (void)moq_msquic_managed_wait(sv.m, 2 * 1000);
        pthread_mutex_lock(&sv.mu);
        bool sub = sv.subscribed;
        pthread_mutex_unlock(&sv.mu);
        primed = sub && auto_published(&cl) >= AUTO_N;
    }
    CHECK(primed);

    /* QUIET WINDOW: no wakes to either side — pure waits on the frozen
     * receiver. A correctly gated re-drive idles here (the token never advances
     * because the app drains nothing), so pump-sweeps barely move. */
    uint64_t sweeps0 = lane_pump_sweeps(sv.m);
    for (int i = 0; i < 400; i++)
        (void)moq_msquic_managed_wait(sv.m, 5 * 1000);   /* ~2s of quiet */
    uint64_t sweeps1 = lane_pump_sweeps(sv.m);

    bool sv_fatal = moq_msquic_managed_is_fatal(sv.m);
    (void)moq_msquic_managed_stop(cl.m);
    (void)moq_msquic_managed_stop(sv.m);
    moq_msquic_managed_destroy(cl.m);
    moq_msquic_managed_destroy(sv.m);

    CHECK(sweeps0 != UINT64_MAX && sweeps1 != UINT64_MAX);
    /* Bounded: idle doorbell does a handful of sweeps at most across ~2s; a spin
     * would be thousands. The gate is what makes this hold. */
    uint64_t delta = sweeps1 - sweeps0;
    CHECK(delta < 100);
    CHECK(!sv_fatal);

    printf("NO-SPIN: non-draining receiver quiet-window pump-sweeps delta=%llu "
           "(bound<100)\n", (unsigned long long)delta);

    pthread_mutex_destroy(&sv.mu);
    pthread_mutex_destroy(&cl.mu);
    if (failures == before)
        printf("PASS: nondraining_no_spin\n");
}

int main(int argc, char **argv)
{
    if (argc < 3) {
        fprintf(stderr, "usage: %s <cert.pem> <key.pem>\n", argv[0]);
        return 2;
    }
    static const QUIC_API_TABLE *lib_pin;
    if (QUIC_FAILED(MsQuicOpen2(&lib_pin))) {
        fprintf(stderr, "MsQuicOpen2 failed\n");
        return 2;
    }

    t_stalled_receiver_recovers(argv[1], argv[2]);
    t_over_window_continuous_drain(argv[1], argv[2]);
    t_autonomous_bounded_drain(argv[1], argv[2], 0);      /* default capacity */
    t_autonomous_bounded_drain(argv[1], argv[2], 8192);   /* large capacity */
    t_nondraining_no_spin(argv[1], argv[2]);

    if (failures == 0)
        printf("PASS: msquic_recv_loopback\n");
    else
        fprintf(stderr, "FAIL: msquic_recv_loopback (%d)\n", failures);
    return failures;
}
