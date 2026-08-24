/*
 * Real-MsQuic APPLICATION-EVENT HOLD / RELEASE.
 *
 * A managed server subscribes and then stops polling object events. A
 * managed client publishes a fixed set of 4096-byte objects over real
 * MsQuic. While the receiver's application polling is held, the transfer
 * must survive: the publisher puts the whole workload out, real bytes
 * reach the transport, the receiver's own pump keeps running on transport
 * activity alone -- and the application receives NOTHING. On release the
 * entire workload arrives.
 *
 * What this proves, and only this:
 *   - a receiver that holds its application polling does not break the
 *     connection, lose data, or go fatal -- neither side does;
 *   - while held, the receiving APPLICATION observes no object delivery,
 *     which is armed BEFORE the first object is published so it cannot be
 *     a race between the control and data streams;
 *   - the receiver's pump advances during the hold WITHOUT any application
 *     wake, so it is real transport activity driving it;
 *   - on release the ENTIRE workload is delivered BYTE-EXACT, IN ORDER and
 *     EXACTLY ONCE, with no loss and no duplication.
 *
 * What this deliberately does NOT claim:
 *   - it does not claim sender backpressure. This workload is far inside
 *     the 16 MiB receive window, so MsQuic can and does accept all of it;
 *     `flush_bytes` is used only as a MONOTONIC transport-progress marker,
 *     never as evidence that the sender was blocked. It also counts MoQ
 *     framing, so it is not comparable to the payload total at all;
 *   - it does not claim native receive pause/resume or adapter arrest. The
 *     arrest mechanics -- partial accept, hold, resume, multi-buffer
 *     stop-and-hold, resume-failure fatal, and capacity invariance across
 *     receive-queue ceilings -- are owned deterministically by
 *     test_msquic_unit.c;
 *   - it does not claim receive-credit renewal. That is the over-window
 *     qualification cell's one unique fact.
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

enum { OBJECT_SIZE = 4096 };
enum { N_OBJECTS = 384 };                 /* > the ~1 MiB / ~256-object
                                             in-flight send budget */
enum { WORKLOAD_BYTES = N_OBJECTS * OBJECT_SIZE };
enum { SENDER_MAX_ACTIONS = 1024 };       /* session queue never the limiter */

/*
 * Every phase of this cell waits on a real elapsed-time allowance, never a
 * kick count. kick() waits at most 5 ms per side and returns early whenever
 * either side has activity, so a fixed number of iterations is not a time
 * bound at all: on a fast machine it can be consumed long before the work
 * it is waiting for completes, and the guard becomes the verdict rather
 * than a backstop. 10 s is three orders of magnitude above the ~0.03 s a
 * healthy run takes, and far inside the registration's 120 s CTest cap, so
 * it leaves room for sanitizer and loaded-CI variance. One allowance is
 * shared by all three phases: they wait on different facts, but they all
 * want the same "this cannot still be healthy" backstop.
 */
enum { PHASE_TIMEOUT_US = 10 * 1000 * 1000 };

/*
 * Monotonic elapsed microseconds since `since`. Returns UINT64_MAX if the
 * clock cannot be read, which the caller treats as expiry -- fail closed,
 * never loop forever on a clock that stopped answering.
 */
static uint64_t monotonic_elapsed_us(const struct timespec *since)
{
    struct timespec now;
    uint64_t secs;
    long nsec;

    if (clock_gettime(CLOCK_MONOTONIC, &now) != 0)
        return UINT64_MAX;
    if (now.tv_sec < since->tv_sec ||
        (now.tv_sec == since->tv_sec && now.tv_nsec < since->tv_nsec))
        return UINT64_MAX; /* a backward monotonic sample is exactly the
                            * condition this helper exists to fail closed
                            * on: report expiry, never keep waiting */
    secs = (uint64_t)(now.tv_sec - since->tv_sec);
    nsec = now.tv_nsec - since->tv_nsec;
    if (nsec < 0) {
        nsec += 1000000000L; /* secs >= 1 here, so the borrow is safe */
        secs--;
    }
    if (secs > (UINT64_MAX - 1000000u) / 1000000u)
        return UINT64_MAX;
    return secs * 1000000u + (uint64_t)(nsec / 1000);
}

/*
 * Start a phase's allowance. Returns false if CLOCK_MONOTONIC cannot be
 * read at all, which the caller reports rather than waiting blind.
 */
static bool phase_start(struct timespec *start)
{
    return clock_gettime(CLOCK_MONOTONIC, start) == 0;
}

static bool phase_expired(const struct timespec *start)
{
    return monotonic_elapsed_us(start) >= PHASE_TIMEOUT_US;
}

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
    uint64_t next_expected;      /* arrival order across the release */
    int order_errors;
    uint64_t pumps;              /* completed server pump generations */
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
    sv->pumps++;
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
                if (oid != sv->next_expected)
                    sv->order_errors++;
                sv->next_expected = oid + 1;
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
    bool gate_open;              /* set only once the receiver holds */
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
    if (s != NULL && cl->accepted && cl->gate_open && !cl->subgroup_open) {
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


static uint64_t server_pumps(struct server_side *sv)
{
    uint64_t p;

    pthread_mutex_lock(&sv->mu);
    p = sv->pumps;
    pthread_mutex_unlock(&sv->mu);
    return p;
}

static bool server_holding(struct server_side *sv)
{
    bool h;

    pthread_mutex_lock(&sv->mu);
    h = sv->sub_ok && !sv->release;
    pthread_mutex_unlock(&sv->mu);
    return h;
}

static bool client_accepted(struct client_side *cl)
{
    bool a;

    pthread_mutex_lock(&cl->mu);
    a = cl->accepted;
    pthread_mutex_unlock(&cl->mu);
    return a;
}

static void client_open_gate(struct client_side *cl)
{
    pthread_mutex_lock(&cl->mu);
    cl->gate_open = true;
    pthread_mutex_unlock(&cl->mu);
}

/* Drive the CLIENT only. Waiting on the server is allowed -- what must not
 * happen during the held phase is an application wake of the server, since
 * the point is that real transport activity advances its pump on its own. */
static void kick_client_only(struct server_side *sv, struct client_side *cl)
{
    (void)moq_msquic_managed_wake(cl->m);
    (void)moq_msquic_managed_wait(cl->m, 5 * 1000);
    (void)moq_msquic_managed_wait(sv->m, 5 * 1000);
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

    /* Phase 1 -- ARM THE HOLD, then open the publication gate.
     *
     * The publisher may not open its subgroup or write a single object
     * until the receiver has observed SUBSCRIBE_OK and stopped polling
     * objects. Without that gate the two streams race: object data can
     * reach the receiver's session and be polled before the control
     * response arms the hold, and "the application received nothing"
     * becomes a coin flip rather than a contract. */
    struct timespec arm_start;
    bool armed = false;
    bool arm_clock_ok = phase_start(&arm_start);

    CHECK(arm_clock_ok);
    while (arm_clock_ok) {
        kick(&sv, &cl);
        armed = server_holding(&sv) && client_accepted(&cl);
        if (armed || phase_expired(&arm_start))
            break;
    }
    CHECK(armed);
    /* the hold is armed BEFORE any object exists */
    CHECK(client_published(&cl) == 0);
    CHECK(server_drained(&sv) == 0);

    uint64_t server_pumps_armed = server_pumps(&sv);

    client_open_gate(&cl);

    /* Phase 2 -- HELD. Drive the CLIENT only: the receiver is never woken
     * by the application from here, so any advance of its pump generation
     * is real transport activity arriving.
     *
     * ONE monotonic held-ready predicate, three facts, all of them things
     * that once true stay true:
     *
     *   - the publisher accepted the whole workload;
     *   - a substantial number of bytes really reached the transport;
     *   - the server's pump count is above the PRE-GATE armed baseline.
     *
     * The third is the causal one: the server is never explicitly woken
     * after the gate opens, so a pump count above the baseline captured
     * before it can only have come from arriving transport activity. That
     * is what makes "the application delivered nothing" a statement about
     * the application rather than about a receiver that never ran.
     *
     * Nothing here samples the pump count mid-phase and then demands a
     * FURTHER generation: once the receiver has consumed all the transport
     * work currently available, no later generation is owed, and no
     * allowance of any length can conjure one. The bound is a fail-closed
     * elapsed-time backstop, never the verdict. */
    enum { PROGRESS_FLOOR = 512 * 1024 };
    struct timespec held_start;
    bool held_clock_ok = phase_start(&held_start);
    bool held_ready = false;
    uint64_t flushed = 0;

    CHECK(held_clock_ok);
    while (held_clock_ok) {
        kick_client_only(&sv, &cl);
        flushed = client_flush_bytes(&cl);
        held_ready = client_published(&cl) >= N_OBJECTS &&
                     flushed != UINT64_MAX &&
                     flushed >= (uint64_t)PROGRESS_FLOOR &&
                     server_pumps(&sv) > server_pumps_armed;
        if (held_ready || phase_expired(&held_start))
            break;
    }
    uint64_t server_pumps_held = server_pumps(&sv);

    CHECK(held_ready);
    CHECK(client_published(&cl) == N_OBJECTS);  /* publisher put it all out */
    CHECK(flushed >= (uint64_t)PROGRESS_FLOOR); /* real transport progress */
    CHECK(server_pumps_held > server_pumps_armed); /* advanced, unwoken */

    /* the contract: with its polling held, the application received
     * nothing, and neither side went fatal doing it */
    int drained_held = server_drained(&sv);

    CHECK(drained_held == 0);
    CHECK(!moq_msquic_managed_is_fatal(cl.m));
    CHECK(!moq_msquic_managed_is_fatal(sv.m));

    printf("HELD: published=%llu, transport flush_bytes=%llu (>= %d), "
           "receiver pumped %llu -> %llu with no application wake, "
           "app drained=%d\n",
           (unsigned long long)client_published(&cl),
           (unsigned long long)flushed, PROGRESS_FLOOR,
           (unsigned long long)server_pumps_armed,
           (unsigned long long)server_pumps_held, drained_held);

    /* Phase 3 -- RELEASE. Both sides are driven now, and the entire
     * workload must arrive byte-exact, in order, exactly once. */
    pthread_mutex_lock(&sv.mu);
    sv.release = true;
    pthread_mutex_unlock(&sv.mu);
    struct timespec rel_start;
    bool rel_clock_ok = phase_start(&rel_start);
    bool full = false;

    CHECK(rel_clock_ok);
    while (rel_clock_ok) {
        kick(&sv, &cl);
        full = server_drained(&sv) >= N_OBJECTS;
        if (full || phase_expired(&rel_start))
            break;
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
    CHECK(sv.order_errors == 0);      /* and in the order published */
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
 * any lost scheduler re-drive. That the doorbell re-drives a draining
 * receiver by itself, and stops when nothing drains, is proven
 * deterministically over a real session event backlog in
 * test_msquic_no_spin.c. */
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

int main(int argc, char **argv)
{
    /*
     * Two cells live in this binary and they belong to DIFFERENT lanes, so
     * each CTest registration names the one it wants. A CTest label applies
     * to the registered command, not to a C function, so running both from
     * one command would put the qualification cell inside the correctness
     * gate no matter how it was labelled.
     *
     *   stall       -- the retained application-event hold/release cell
     *   over_window -- the real-QUIC credit-renewal qualification cell
     *   (absent)    -- documented direct-run mode: both, for a developer
     *                  running the binary by hand. Both CTest registrations
     *                  always pass a selector, so this can never affect
     *                  their classification.
     *
     * An unrecognized selector fails closed rather than silently running
     * nothing.
     */
    if (argc < 3) {
        fprintf(stderr,
                "usage: %s <cert.pem> <key.pem> [stall|over_window]\n",
                argv[0]);
        return 2;
    }
    const char *sel = argc > 3 ? argv[3] : NULL;
    bool run_stall = sel == NULL || strcmp(sel, "stall") == 0;
    bool run_over_window = sel == NULL || strcmp(sel, "over_window") == 0;

    if (!run_stall && !run_over_window) {
        fprintf(stderr, "unknown case selector '%s' "
                        "(expected stall|over_window)\n", sel);
        return 2;
    }

    static const QUIC_API_TABLE *lib_pin;
    if (QUIC_FAILED(MsQuicOpen2(&lib_pin))) {
        fprintf(stderr, "MsQuicOpen2 failed\n");
        return 2;
    }

    if (run_stall)
        t_stalled_receiver_recovers(argv[1], argv[2]);
    if (run_over_window)
        t_over_window_continuous_drain(argv[1], argv[2]);

    if (failures == 0)
        printf("PASS: msquic_recv_loopback\n");
    else
        fprintf(stderr, "FAIL: msquic_recv_loopback (%d)\n", failures);
    return failures;
}
