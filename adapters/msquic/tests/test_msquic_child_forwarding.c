/*
 * cfg.streaming_objects must reach the CHILD SESSION the managed facade
 * builds, and it must reach it through the production child path.
 *
 * The forwarding site is mgd_make_child()'s
 *
 *     if (m->cfg.streaming_objects)
 *         scfg.streaming_objects = true;
 *
 * so a rig that constructs its own subscriber session cannot pin it: the
 * child here is created by the facade (lanes-only constructor + the
 * production child path) and bound to a fake connection, and the whole MoQ
 * flow -- SETUP, SUBSCRIBE, one published object -- is relayed between that
 * child and a raw peer session over the fake table.
 *
 * BOTH arms run, on two independent facades: with the flag set the object
 * must arrive as OBJECT_CHUNK slices and never as a whole object; with it
 * clear, exactly one whole OBJECT_RECEIVED and no chunk at all. The
 * flag-clear arm is also what makes a prefix-copy defect visible -- a
 * derivation that adopted an appended field from a prefix-sized caller
 * would turn that arm into chunks.
 *
 * Confinement is respected: every moq_session_* call on the child happens
 * inside its on_lane_pump. The main thread only relays wire bytes between
 * the two fakes while the lane is parked at its pre-wait barrier, so no
 * doorbell can be running concurrently.
 */

#if defined(__linux__) && !defined(_POSIX_C_SOURCE)
#define _POSIX_C_SOURCE 200809L
#endif

#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

#include "msq_child_pair.h"

static int failures = 0;

#define CHECK(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)

/* MOQ_MSQUIC_TESTING-only seams. */
extern void (*moq_msq_test_prewait)(moq_msquic_managed_lane_t *lane);
extern moq_result_t moq_msq_test_managed_create_lanes_only_api(
    const moq_msquic_managed_cfg_t *cfg, const QUIC_API_TABLE *api,
    moq_msquic_managed_t **out);
extern moq_result_t moq_msq_test_lane_inject_live_child(
    moq_msquic_managed_lane_t *lane, HQUIC connection,
    moq_msquic_managed_conn_t **out);
extern moq_msquic_conn_t *moq_msq_test_managed_conn_adapter(
    moq_msquic_managed_conn_t *conn);

/* The raw peer's op recorder copies at most 256 bytes per write
 * (fake_endpoint.h), so the object stays inside one recorded write and the
 * relay carries it whole. */
enum { OBJ_LEN = 192 };

/* --- what the child's pump observes ------------------------------------ */

static struct {
    pthread_mutex_t mu;
    bool subscribed;
    int setup;
    int sub_ok;
    int objects;        /* whole-object OBJECT_RECEIVED */
    uint64_t object_bytes;
    int object_errors;
    int chunk_events;   /* OBJECT_CHUNK */
    uint64_t chunk_bytes;
    int chunk_errors;
    int chunk_objects;  /* completed by end + terminal NORMAL */
} g_sub = { .mu = PTHREAD_MUTEX_INITIALIZER };

static void sub_reset(void)
{
    pthread_mutex_lock(&g_sub.mu);
    g_sub.subscribed = false;
    g_sub.setup = 0;
    g_sub.sub_ok = 0;
    g_sub.objects = 0;
    g_sub.object_bytes = 0;
    g_sub.object_errors = 0;
    g_sub.chunk_events = 0;
    g_sub.chunk_bytes = 0;
    g_sub.chunk_errors = 0;
    g_sub.chunk_objects = 0;
    pthread_mutex_unlock(&g_sub.mu);
}

static int child_pump(moq_msquic_managed_t *m,
                      moq_msquic_managed_lane_t *lane, uint64_t now_us,
                      void *user)
{
    moq_session_t *s = moq_msquic_managed_session(m);
    moq_event_t ev;

    (void)lane;
    (void)user;
    if (s == NULL)
        return 0;
    pthread_mutex_lock(&g_sub.mu);
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        switch (ev.kind) {
        case MOQ_EVENT_SETUP_COMPLETE:
            g_sub.setup++;
            break;
        case MOQ_EVENT_SUBSCRIBE_OK:
            g_sub.sub_ok++;
            break;
        case MOQ_EVENT_OBJECT_RECEIVED: {
            const moq_rcbuf_t *p = ev.u.object_received.payload;

            g_sub.objects++;
            if (p == NULL || moq_rcbuf_len(p) != OBJ_LEN) {
                g_sub.object_errors++;
            } else {
                uint8_t want[OBJ_LEN];

                payload_fill(want, OBJ_LEN);
                if (memcmp(moq_rcbuf_data(p), want, OBJ_LEN) != 0)
                    g_sub.object_errors++;
                g_sub.object_bytes += OBJ_LEN;
            }
            break;
        }
        case MOQ_EVENT_OBJECT_CHUNK: {
            const moq_object_chunk_event_t *oc = &ev.u.object_chunk;

            g_sub.chunk_events++;
            if (oc->chunk != NULL) {
                size_t len = moq_rcbuf_len(oc->chunk);
                const uint8_t *d = moq_rcbuf_data(oc->chunk);

                for (size_t i = 0; i < len; i++)
                    if (d[i] !=
                        (uint8_t)((g_sub.chunk_bytes + i) * 131u + 47u)) {
                        g_sub.chunk_errors++;
                        break;
                    }
                g_sub.chunk_bytes += len;
            }
            if (oc->end && oc->terminal == MOQ_OBJECT_TERMINAL_NORMAL)
                g_sub.chunk_objects++;
            break;
        }
        default:
            break;
        }
        moq_event_cleanup(&ev);
    }
    if (g_sub.setup > 0 && !g_sub.subscribed) {
        static const moq_bytes_t ns[] = { { (const uint8_t *)"live", 4 } };
        moq_subscribe_cfg_t sc;
        moq_subscription_t sub;

        moq_subscribe_cfg_init(&sc);
        sc.track_namespace.parts = (moq_bytes_t *)ns;
        sc.track_namespace.count = 1;
        sc.track_name = (moq_bytes_t){ (const uint8_t *)"video", 5 };
        sc.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
        g_sub.subscribed =
            moq_session_subscribe(s, &sc, now_us, &sub) == MOQ_OK;
    }
    pthread_mutex_unlock(&g_sub.mu);
    return 0;
}

/* Drive generations until the peer has an accepted subscription, then
 * publish one object and drive until it is fully surfaced. The round bound
 * is a hang guard; the verdict is the tally. */
static void run_flow(struct pair *p, bool streaming)
{
    enum { MAX_STEPS = 40 };
    moq_subscription_t server_sub = MOQ_SUBSCRIPTION_INVALID;
    bool accepted = false, published = false;
    int steps = 0;

    while (steps++ < MAX_STEPS) {
        moq_event_t ev;

        if (!step(p))
            break;
        while (moq_session_poll_events(p->peer, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST)
                server_sub = ev.u.subscribe_request.sub;
            moq_event_cleanup(&ev);
        }
        if (!accepted && moq_subscription_is_valid(server_sub)) {
            moq_accept_subscribe_cfg_t acc;

            moq_accept_subscribe_cfg_init(&acc);
            accepted =
                moq_session_accept_subscribe(p->peer, server_sub, &acc, 0) ==
                MOQ_OK;
            continue;
        }
        if (accepted && !published) {
            moq_subgroup_cfg_t sgc;
            moq_subgroup_handle_t sg;
            uint8_t bytes[OBJ_LEN];
            moq_rcbuf_t *buf = NULL;

            moq_subgroup_cfg_init(&sgc);
            sgc.group_id = 0;
            sgc.subgroup_id = 0;
            sgc.publisher_priority = 200;
            if (moq_session_open_subgroup(p->peer, server_sub, &sgc, 0,
                                          &sg) != MOQ_OK)
                break;
            payload_fill(bytes, OBJ_LEN);
            if (moq_rcbuf_create(moq_alloc_default(), bytes, OBJ_LEN,
                                 &buf) < 0)
                break;
            (void)moq_session_write_object(p->peer, sg, 0, buf, 0);
            moq_rcbuf_decref(buf);
            (void)moq_session_close_subgroup(p->peer, sg, 0);
            published = true;
            continue;
        }
        if (published) {
            pthread_mutex_lock(&g_sub.mu);
            bool done = streaming ? g_sub.chunk_objects >= 1
                                  : g_sub.objects >= 1;
            pthread_mutex_unlock(&g_sub.mu);
            if (done) {
                /* one more generation so any trailing event is surfaced */
                (void)step(p);
                break;
            }
        }
    }
    CHECK(accepted);
    CHECK(published);
}

/* --- the two arms ------------------------------------------------------ */

static void t_streaming_flag_set(void)
{
    int before = failures;
    struct pair p;

    sub_reset();
    if (!pair_up(&p, true, sizeof(moq_msquic_managed_cfg_t),
                 child_pump)) {
        CHECK(0 && "pair up (flag set)");
        pair_down(&p);
        return;
    }
    run_flow(&p, true);

    pthread_mutex_lock(&g_sub.mu);
    CHECK(g_sub.setup == 1);
    CHECK(g_sub.sub_ok == 1);
    /* the flag reached the child session: slices, never a whole object */
    CHECK(g_sub.chunk_events >= 1);
    CHECK(g_sub.chunk_bytes == OBJ_LEN);
    CHECK(g_sub.chunk_errors == 0);
    CHECK(g_sub.chunk_objects == 1);
    CHECK(g_sub.objects == 0);
    pthread_mutex_unlock(&g_sub.mu);

    pair_down(&p);
    if (failures == before)
        printf("PASS: streaming_flag_set\n");
}

static void t_streaming_flag_clear(void)
{
    int before = failures;
    struct pair p;

    sub_reset();
    if (!pair_up(&p, false, sizeof(moq_msquic_managed_cfg_t),
                 child_pump)) {
        CHECK(0 && "pair up (flag clear)");
        pair_down(&p);
        return;
    }
    run_flow(&p, false);

    pthread_mutex_lock(&g_sub.mu);
    CHECK(g_sub.setup == 1);
    CHECK(g_sub.sub_ok == 1);
    /* the default: exactly one whole object, and no chunk at all */
    CHECK(g_sub.objects == 1);
    CHECK(g_sub.object_bytes == OBJ_LEN);
    CHECK(g_sub.object_errors == 0);
    CHECK(g_sub.chunk_events == 0);
    pthread_mutex_unlock(&g_sub.mu);

    pair_down(&p);
    if (failures == before)
        printf("PASS: streaming_flag_clear\n");
}

/*
 * A prefix-sized caller cannot turn the flag on. struct_size stops before
 * the appended block, so even though cfg.streaming_objects is written here
 * the facade must default it to false and the child must render whole
 * objects. This is the arm a prefix-copy defect shows up in: a derivation
 * that adopted an appended field from a short cfg would deliver chunks.
 */
static void t_streaming_prefix_sized_defaults_clear(void)
{
    int before = failures;
    struct pair p;

    sub_reset();
    if (!pair_up(&p, true, offsetof(moq_msquic_managed_cfg_t, version),
                 child_pump)) {
        CHECK(0 && "pair up (prefix sized)");
        pair_down(&p);
        return;
    }
    CHECK(moq_msquic_managed_negotiated_version(p.m) ==
          MOQ_VERSION_DRAFT_16);
    run_flow(&p, false);

    pthread_mutex_lock(&g_sub.mu);
    CHECK(g_sub.setup == 1);
    CHECK(g_sub.sub_ok == 1);
    CHECK(g_sub.objects == 1);
    CHECK(g_sub.object_bytes == OBJ_LEN);
    CHECK(g_sub.object_errors == 0);
    CHECK(g_sub.chunk_events == 0);
    pthread_mutex_unlock(&g_sub.mu);

    pair_down(&p);
    if (failures == before)
        printf("PASS: streaming_prefix_sized_defaults_clear\n");
}

int main(void)
{
    setvbuf(stdout, NULL, _IOLBF, 0);
    moq_msq_test_prewait = prewait_hook;

    t_streaming_flag_set();
    t_streaming_flag_clear();
    t_streaming_prefix_sized_defaults_clear();

    if (failures == 0)
        printf("PASS: msquic_child_forwarding\n");
    else
        fprintf(stderr, "FAIL: msquic_child_forwarding (%d)\n", failures);
    return failures;
}
