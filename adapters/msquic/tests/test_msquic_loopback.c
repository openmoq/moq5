/*
 * Managed MoQ-over-MsQuic loopback: a managed server (port 0) and a
 * managed client (insecure loopback verification) run one full MoQ
 * flow over real MsQuic — SETUP both sides, SUBSCRIBE, one object
 * larger than the transport's 64 KiB send-budget floor (idle oversized
 * sends must make forward progress through the managed facade),
 * byte-exact payload verification, then a clean client close observed
 * as terminal-not-fatal on both sides.
 *
 * Confinement: every moq_session_* call lives inside on_lane_pump. The main
 * thread only waits (wake/wait or bounded polls of the terminal
 * accessors) and asserts recorded observations.
 *
 * Also covered, kept deliberately small: a client connect to a dead
 * port goes fatal within its configured idle window, with no setup.
 */

#if defined(__linux__) && !defined(_POSIX_C_SOURCE)
#define _POSIX_C_SOURCE 200809L
#endif

#include <pthread.h>
#include <stddef.h>
#include <stdint.h>
#include <time.h>
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

enum { OBJECT_SIZE = 96 * 1024 }; /* > the 64 KiB budget floor */

static void payload_fill(uint8_t *dst, size_t size)
{
    for (size_t i = 0; i < size; i++)
        dst[i] = (uint8_t)(i * 131u + 47u);
}

struct side {
    pthread_mutex_t mu;
    moq_msquic_managed_t *m;
    int setup;
    int sub_ok;
    int session_closed;
    int objects;
    uint64_t object_bytes;
    int object_errors;
    int publish_errors;
    bool subscribed;
    bool published;
    bool close_sent;
    bool hold_open;            /* never close from the pump */
    moq_subscription_t sub;
    bool have_sub;
    /* callback-context pins */
    bool accessors_probed;     /* terminal accessors ran inside on_lane_pump */
    int stop_in_pump_rc;       /* stop() from on_lane_pump result */
    bool stop_in_pump_ran;
    int stop_in_activity_rc;   /* stop() from on_activity result */
    bool stop_in_activity_ran;
    /* moq_msquic_managed_session() pump-window gating pins */
    bool session_in_pump;      /* non-NULL observed inside the client pump */
    bool session_in_activity;  /* non-NULL from on_activity (must stay false) */
    bool session_on_server;    /* non-NULL on a server facade (must stay false) */
    /* streaming-objects receive (cfg.streaming_objects = true) */
    int chunk_events;          /* OBJECT_CHUNK events seen */
    uint64_t chunk_bytes;      /* payload bytes across chunks */
    int chunk_errors;          /* byte-pattern mismatches */
    int chunk_objects;         /* objects completed by end + terminal NORMAL */
};

static void observe_object(struct side *sd, const moq_event_t *ev)
{
    const moq_rcbuf_t *payload = ev->u.object_received.payload;

    if (payload == NULL) {
        sd->object_errors++;
        return;
    }
    size_t len = moq_rcbuf_len(payload);
    const uint8_t *data = moq_rcbuf_data(payload);

    sd->object_bytes += len;
    if (len != OBJECT_SIZE) {
        sd->object_errors++;
        return;
    }
    for (size_t i = 0; i < len; i++)
        if (data[i] != (uint8_t)(i * 131u + 47u)) {
            sd->object_errors++;
            return;
        }
}

static int client_pump(moq_msquic_managed_t *m,
                       moq_msquic_managed_lane_t *lane, uint64_t now,
                       void *ctx)
{
    struct side *sd = ctx;
    /* a client's single session is the client lane/session path */
    moq_session_t *s = moq_msquic_managed_session(m);
    moq_event_t ev;

    (void)lane;

    pthread_mutex_lock(&sd->mu);
    if (s != NULL)
        sd->session_in_pump = true; /* accessor valid inside the lane pump */
    /* the documented contract: on_lane_pump may call any facade accessor,
     * and a stop() attempt from here is refused, not deadlocked */
    if (!sd->accessors_probed) {
        sd->accessors_probed = true;
        (void)moq_msquic_managed_is_fatal(m);
        (void)moq_msquic_managed_is_closed(m);
        (void)moq_msquic_managed_fatal_code(m);
        (void)moq_msquic_managed_close_code(m);
    }
    if (!sd->stop_in_pump_ran) {
        sd->stop_in_pump_ran = true;
        sd->stop_in_pump_rc = (int)moq_msquic_managed_stop(m);
    }
    while (s != NULL && moq_session_poll_events(s, &ev, 1) > 0) {
        switch (ev.kind) {
        case MOQ_EVENT_SETUP_COMPLETE:
            sd->setup++;
            break;
        case MOQ_EVENT_SUBSCRIBE_OK:
            sd->sub_ok++;
            break;
        case MOQ_EVENT_OBJECT_RECEIVED:
            observe_object(sd, &ev);
            sd->objects++;
            break;
        case MOQ_EVENT_OBJECT_CHUNK: {
            /* streaming receive (cfg.streaming_objects): verify each slice
             * continues the payload_fill pattern at the running offset */
            const moq_object_chunk_event_t *oc = &ev.u.object_chunk;

            sd->chunk_events++;
            if (oc->chunk != NULL) {
                size_t len = moq_rcbuf_len(oc->chunk);
                const uint8_t *data = moq_rcbuf_data(oc->chunk);

                for (size_t i = 0; i < len; i++)
                    if (data[i] !=
                        (uint8_t)((sd->chunk_bytes + i) * 131u + 47u)) {
                        sd->chunk_errors++;
                        break;
                    }
                sd->chunk_bytes += len;
            }
            if (oc->end && oc->terminal == MOQ_OBJECT_TERMINAL_NORMAL)
                sd->chunk_objects++;
            break;
        }
        case MOQ_EVENT_SESSION_CLOSED:
            sd->session_closed++;
            break;
        default:
            break;
        }
        moq_event_cleanup(&ev);
    }
    if (s != NULL && sd->setup > 0 && !sd->subscribed) {
        static const moq_bytes_t ns[] = {
            { (const uint8_t *)"msq", 3 },
        };
        moq_subscribe_cfg_t sc;
        moq_subscription_t sub;

        moq_subscribe_cfg_init(&sc);
        sc.track_namespace.parts = (moq_bytes_t *)ns;
        sc.track_namespace.count = 1;
        sc.track_name = (moq_bytes_t){ (const uint8_t *)"track", 5 };
        sc.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        sd->subscribed = true;
        if (moq_session_subscribe(s, &sc, now, &sub) < 0)
            sd->publish_errors++;
    }
    if (s != NULL && (sd->objects >= 1 || sd->chunk_objects >= 1) &&
        !sd->close_sent && !sd->hold_open) {
        sd->close_sent = true;
        (void)moq_session_close(s, 0, NULL, now);
    }
    pthread_mutex_unlock(&sd->mu);
    return 0;
}

static int server_pump(moq_msquic_managed_t *m,
                       moq_msquic_managed_lane_t *lane, uint64_t now,
                       void *ctx)
{
    struct side *sd = ctx;
    /* a server drives its accepted session through lane iteration; this
     * scenario runs exactly one connection */
    moq_msquic_managed_conn_t *c = moq_msquic_lane_next_conn(lane, NULL);
    moq_session_t *s = c != NULL ? moq_msquic_managed_conn_session(c)
                                 : NULL;
    moq_event_t ev;

    pthread_mutex_lock(&sd->mu);
    /* the client-only accessor must refuse on a server facade, even from
     * inside its own lane pump */
    if (moq_msquic_managed_session(m) != NULL)
        sd->session_on_server = true;
    while (s != NULL && moq_session_poll_events(s, &ev, 1) > 0) {
        switch (ev.kind) {
        case MOQ_EVENT_SETUP_COMPLETE:
            sd->setup++;
            break;
        case MOQ_EVENT_SUBSCRIBE_REQUEST:
            sd->sub = ev.u.subscribe_request.sub;
            sd->have_sub = true;
            break;
        case MOQ_EVENT_SESSION_CLOSED:
            sd->session_closed++;
            break;
        default:
            break;
        }
        moq_event_cleanup(&ev);
    }
    if (s != NULL && sd->have_sub && !sd->published) {
        sd->published = true;

        moq_accept_subscribe_cfg_t ac;
        moq_accept_subscribe_cfg_init(&ac);
        if (moq_session_accept_subscribe(s, sd->sub, &ac, now) < 0)
            sd->publish_errors++;

        moq_subgroup_cfg_t sgc;
        moq_subgroup_handle_t sg;
        moq_subgroup_cfg_init(&sgc);
        sgc.group_id = 0;
        sgc.publisher_priority = 200;
        if (moq_session_open_subgroup(s, sd->sub, &sgc, now, &sg) < 0) {
            sd->publish_errors++;
        } else {
            uint8_t *bytes = malloc(OBJECT_SIZE);
            moq_rcbuf_t *buf = NULL;

            if (bytes != NULL)
                payload_fill(bytes, OBJECT_SIZE);
            if (bytes == NULL ||
                moq_rcbuf_create(moq_alloc_default(), bytes,
                                 OBJECT_SIZE, &buf) < 0) {
                sd->publish_errors++;
            } else {
                if (moq_session_write_object(s, sg, 0, buf, now) < 0)
                    sd->publish_errors++;
                moq_rcbuf_decref(buf);
                if (moq_session_close_subgroup(s, sg, now) < 0)
                    sd->publish_errors++;
            }
            free(bytes);
        }
    }
    pthread_mutex_unlock(&sd->mu);
    return 0;
}

static void stop_probe_activity(moq_msquic_managed_t *m, void *ctx)
{
    struct side *sd = ctx;

    if (!sd->stop_in_activity_ran) {
        sd->stop_in_activity_ran = true;
        sd->stop_in_activity_rc = (int)moq_msquic_managed_stop(m);
    }
    /* the session accessor is pump-window-only: from on_activity (same
     * thread, but outside the pump) it must refuse */
    if (moq_msquic_managed_session(m) != NULL)
        sd->session_in_activity = true;
}

static void side_init(struct side *sd)
{
    memset(sd, 0, sizeof(*sd));
    pthread_mutex_init(&sd->mu, NULL);
}

static void side_destroy(struct side *sd)
{
    pthread_mutex_destroy(&sd->mu);
}

/* Bounded wait for a terminal state on a managed handle. */
static bool wait_terminal(moq_msquic_managed_t *m, int max_ms)
{
    for (int i = 0; i < max_ms / 10; i++) {
        if (moq_msquic_managed_is_closed(m) ||
            moq_msquic_managed_is_fatal(m))
            return true;
        (void)moq_msquic_managed_wait(m, 10 * 1000);
    }
    return moq_msquic_managed_is_closed(m) ||
           moq_msquic_managed_is_fatal(m);
}

static void t_pubsub_oversized_clean_close(const char *cert,
                                           const char *key)
{
    int before = failures;
    struct side sv, cl;

    side_init(&sv);
    side_init(&cl);

    moq_msquic_managed_cfg_t scfg;
    moq_msquic_managed_cfg_init_sized(&scfg, sizeof(scfg));
    scfg.alloc = moq_alloc_default();
    scfg.perspective = MOQ_PERSPECTIVE_SERVER;
    scfg.host = "127.0.0.1";
    scfg.port = 0; /* ephemeral */
    scfg.cert_path = cert;
    scfg.key_path = key;
    scfg.on_lane_pump = server_pump;
    scfg.on_lane_pump_user = &sv;
    scfg.send_request_capacity = true;
    scfg.initial_request_capacity = 16;
    scfg.max_events = 64;
    CHECK(moq_msquic_managed_create(&scfg, &sv.m) == MOQ_OK);
    if (sv.m == NULL)
        return;
    uint16_t port = moq_msquic_managed_port(sv.m);
    CHECK(port != 0);

    moq_msquic_managed_cfg_t ccfg;
    moq_msquic_managed_cfg_init_sized(&ccfg, sizeof(ccfg));
    ccfg.alloc = moq_alloc_default();
    ccfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    ccfg.host = "127.0.0.1";
    ccfg.port = port;
    ccfg.insecure_skip_verify = true;
    ccfg.on_lane_pump = client_pump;
    ccfg.on_lane_pump_user = &cl;
    ccfg.on_activity = stop_probe_activity;
    ccfg.on_activity_ctx = &cl;
    ccfg.send_request_capacity = true;
    ccfg.initial_request_capacity = 16;
    ccfg.max_events = 64;
    CHECK(moq_msquic_managed_create(&ccfg, &cl.m) == MOQ_OK);
    if (cl.m == NULL) {
        (void)moq_msquic_managed_stop(sv.m);
        moq_msquic_managed_destroy(sv.m);
        return;
    }
    /* pump-window gating: the client's single connection already exists, but
     * this thread is outside any lane pump, so the accessor must refuse */
    CHECK(moq_msquic_managed_session(cl.m) == NULL);

    /* the flow drives itself through the pumps; the main thread only
     * waits for both terminals */
    CHECK(wait_terminal(cl.m, 30000));
    CHECK(wait_terminal(sv.m, 30000));
    CHECK(moq_msquic_managed_session(cl.m) == NULL); /* still outside a pump */

    CHECK(moq_msquic_managed_is_closed(cl.m));
    CHECK(!moq_msquic_managed_is_fatal(cl.m));
    CHECK(moq_msquic_managed_is_closed(sv.m));
    CHECK(!moq_msquic_managed_is_fatal(sv.m));

    CHECK(moq_msquic_managed_stop(cl.m) == MOQ_OK);
    CHECK(moq_msquic_managed_stop(sv.m) == MOQ_OK);
    /* stop is idempotent */
    CHECK(moq_msquic_managed_stop(cl.m) == MOQ_OK);
    moq_msquic_managed_destroy(cl.m);
    moq_msquic_managed_destroy(sv.m);

    /* observations, read after teardown (no pumps can run) */
    CHECK(cl.setup == 1);
    CHECK(sv.setup == 1);
    CHECK(cl.sub_ok == 1);
    CHECK(cl.objects == 1);
    CHECK(cl.object_bytes == OBJECT_SIZE);
    CHECK(cl.object_errors == 0);
    CHECK(sv.publish_errors == 0);
    CHECK(cl.session_closed >= 1);
    CHECK(sv.session_closed >= 1);
    CHECK(cl.accessors_probed);
    CHECK(cl.stop_in_pump_ran);
    CHECK(cl.stop_in_pump_rc == (int)MOQ_ERR_WRONG_STATE);
    CHECK(cl.stop_in_activity_ran);
    CHECK(cl.stop_in_activity_rc == (int)MOQ_ERR_WRONG_STATE);
    /* session accessor gating: valid only inside the client's lane pump */
    CHECK(cl.session_in_pump);
    CHECK(!cl.session_in_activity);
    CHECK(!sv.session_on_server);
    /* default receive path is whole-object: no chunk events without
     * streaming_objects */
    CHECK(cl.chunk_events == 0);

    side_destroy(&sv);
    side_destroy(&cl);
    if (failures == before)
        printf("PASS: pubsub_oversized_clean_close\n");
}

/* streaming_objects forwards into the child session: a subscriber configured
 * with it receives the published object as OBJECT_CHUNK slices (byte-exact
 * across chunks, terminal NORMAL) and never as a whole-object event. */
static void t_streaming_objects(const char *cert, const char *key)
{
    int before = failures;
    struct side sv, cl;

    side_init(&sv);
    side_init(&cl);

    moq_msquic_managed_cfg_t scfg;
    moq_msquic_managed_cfg_init_sized(&scfg, sizeof(scfg));
    scfg.alloc = moq_alloc_default();
    scfg.perspective = MOQ_PERSPECTIVE_SERVER;
    scfg.host = "127.0.0.1";
    scfg.port = 0; /* ephemeral */
    scfg.cert_path = cert;
    scfg.key_path = key;
    scfg.on_lane_pump = server_pump;
    scfg.on_lane_pump_user = &sv;
    scfg.send_request_capacity = true;
    scfg.initial_request_capacity = 16;
    scfg.max_events = 64;
    CHECK(moq_msquic_managed_create(&scfg, &sv.m) == MOQ_OK);
    if (sv.m == NULL)
        return;
    uint16_t port = moq_msquic_managed_port(sv.m);
    CHECK(port != 0);

    moq_msquic_managed_cfg_t ccfg;
    moq_msquic_managed_cfg_init_sized(&ccfg, sizeof(ccfg));
    ccfg.alloc = moq_alloc_default();
    ccfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    ccfg.host = "127.0.0.1";
    ccfg.port = port;
    ccfg.insecure_skip_verify = true;
    ccfg.on_lane_pump = client_pump;
    ccfg.on_lane_pump_user = &cl;
    ccfg.send_request_capacity = true;
    ccfg.initial_request_capacity = 16;
    ccfg.max_events = 64;
    ccfg.streaming_objects = true; /* receive as OBJECT_CHUNK slices */
    CHECK(moq_msquic_managed_create(&ccfg, &cl.m) == MOQ_OK);
    if (cl.m == NULL) {
        (void)moq_msquic_managed_stop(sv.m);
        moq_msquic_managed_destroy(sv.m);
        return;
    }

    CHECK(wait_terminal(cl.m, 30000));
    CHECK(wait_terminal(sv.m, 30000));

    CHECK(moq_msquic_managed_is_closed(cl.m));
    CHECK(!moq_msquic_managed_is_fatal(cl.m));
    CHECK(moq_msquic_managed_stop(cl.m) == MOQ_OK);
    CHECK(moq_msquic_managed_stop(sv.m) == MOQ_OK);
    moq_msquic_managed_destroy(cl.m);
    moq_msquic_managed_destroy(sv.m);

    /* observations, read after teardown */
    CHECK(cl.setup == 1);
    CHECK(sv.setup == 1);
    CHECK(cl.sub_ok == 1);
    CHECK(cl.objects == 0);            /* never a whole-object event */
    CHECK(cl.chunk_events >= 1);       /* the object arrived as slices */
    CHECK(cl.chunk_bytes == OBJECT_SIZE);
    CHECK(cl.chunk_errors == 0);       /* byte-exact across chunks */
    CHECK(cl.chunk_objects == 1);      /* one object, terminal NORMAL */
    CHECK(sv.publish_errors == 0);
    CHECK(cl.session_closed >= 1);
    CHECK(sv.session_closed >= 1);

    side_destroy(&sv);
    side_destroy(&cl);
    if (failures == before)
        printf("PASS: streaming_objects\n");
}

/* Wake-contract state: the pump publishes (and later closes) ONLY
 * when the main thread sets the flags — so delivery of the object and
 * the clean close prove the wake-driven pump path runs on_lane_pump AND
 * drains what the pump queued AND feeds a close recorded during the
 * pump. No transport event can carry this flow: the connection is
 * idle until the wake. */
struct wake_srv {
    pthread_mutex_t mu;
    moq_msquic_managed_t *m;
    moq_subscription_t sub;
    bool have_sub;
    bool accepted;
    bool publish_now;   /* set by main, then wake() */
    bool close_now;     /* set by main, then wake() */
    bool published;
    bool close_sent;
    int pumps;
};

static int wake_server_pump(moq_msquic_managed_t *m,
                            moq_msquic_managed_lane_t *lane, uint64_t now,
                            void *ctx)
{
    struct wake_srv *sv = ctx;
    moq_msquic_managed_conn_t *c = moq_msquic_lane_next_conn(lane, NULL);
    moq_session_t *s = c != NULL ? moq_msquic_managed_conn_session(c)
                                 : NULL;
    moq_event_t ev;

    (void)m;
    pthread_mutex_lock(&sv->mu);
    sv->pumps++;
    while (s != NULL && moq_session_poll_events(s, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
            sv->sub = ev.u.subscribe_request.sub;
            sv->have_sub = true;
        }
        moq_event_cleanup(&ev);
    }
    if (s != NULL && sv->have_sub && !sv->accepted) {
        moq_accept_subscribe_cfg_t ac;

        sv->accepted = true;
        moq_accept_subscribe_cfg_init(&ac);
        (void)moq_session_accept_subscribe(s, sv->sub, &ac, now);
    }
    if (s != NULL && sv->accepted && sv->publish_now && !sv->published) {
        sv->published = true;

        moq_subgroup_cfg_t sgc;
        moq_subgroup_handle_t sg;
        moq_subgroup_cfg_init(&sgc);
        sgc.group_id = 0;
        sgc.publisher_priority = 200;
        if (moq_session_open_subgroup(s, sv->sub, &sgc, now, &sg) == 0) {
            uint8_t *bytes = malloc(OBJECT_SIZE);
            moq_rcbuf_t *buf = NULL;

            if (bytes != NULL) {
                payload_fill(bytes, OBJECT_SIZE);
                if (moq_rcbuf_create(moq_alloc_default(), bytes,
                                     OBJECT_SIZE, &buf) == 0) {
                    (void)moq_session_write_object(s, sg, 0, buf, now);
                    moq_rcbuf_decref(buf);
                }
                free(bytes);
            }
            (void)moq_session_close_subgroup(s, sg, now);
        }
    }
    if (s != NULL && sv->close_now && !sv->close_sent) {
        sv->close_sent = true;
        (void)moq_session_close(s, 0, NULL, now);
    }
    pthread_mutex_unlock(&sv->mu);
    return 0;
}

/* Server-side state for multi-connection scenarios: one slot per
 * accepted session, keyed by session pointer, driven entirely from
 * inside on_lane_pump via lane iteration. */
struct msrv {
    pthread_mutex_t mu;
    moq_msquic_managed_t *m;
    struct {
        moq_subscription_t sub;
        bool have_sub;
        bool published;
    } slots[4];
    int next_slot;
    int published;
    int session_closed;
    size_t first_closed_conn_count;
};

static int multi_server_pump(moq_msquic_managed_t *m,
                             moq_msquic_managed_lane_t *lane, uint64_t now,
                             void *ctx)
{
    struct msrv *sv = ctx;

    (void)m;
    pthread_mutex_lock(&sv->mu);
    for (moq_msquic_managed_conn_t *conn =
             moq_msquic_lane_next_conn(lane, NULL);
         conn != NULL; conn = moq_msquic_lane_next_conn(lane, conn)) {
        moq_session_t *s = moq_msquic_managed_conn_session(conn);
        /* per-connection state rides the conn's user slot: pointer
         * values (session or conn) are NOT stable identities across a
         * reclaimed child and its successor */
        int slot = (int)(intptr_t)moq_msquic_managed_conn_user(conn) - 1;

        if (slot < 0) {
            if (sv->next_slot >= 4)
                continue;
            slot = sv->next_slot++;
            moq_msquic_managed_conn_set_user(
                conn, (void *)(intptr_t)(slot + 1));
        }

        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
                sv->slots[slot].sub = ev.u.subscribe_request.sub;
                sv->slots[slot].have_sub = true;
            } else if (ev.kind == MOQ_EVENT_SESSION_CLOSED) {
                sv->session_closed++;
                if (sv->first_closed_conn_count == 0)
                    sv->first_closed_conn_count =
                        moq_msquic_managed_conn_count(m);
            }
            moq_event_cleanup(&ev);
        }
        if (sv->slots[slot].have_sub && !sv->slots[slot].published) {
            sv->slots[slot].published = true;
            sv->published++;

            moq_accept_subscribe_cfg_t ac;
            moq_accept_subscribe_cfg_init(&ac);
            (void)moq_session_accept_subscribe(s, sv->slots[slot].sub,
                                               &ac, now);

            moq_subgroup_cfg_t sgc;
            moq_subgroup_handle_t sg;
            moq_subgroup_cfg_init(&sgc);
            sgc.group_id = 0;
            sgc.publisher_priority = 200;
            if (moq_session_open_subgroup(s, sv->slots[slot].sub, &sgc,
                                          now, &sg) == 0) {
                uint8_t *bytes = malloc(OBJECT_SIZE);
                moq_rcbuf_t *buf = NULL;

                if (bytes != NULL) {
                    payload_fill(bytes, OBJECT_SIZE);
                    if (moq_rcbuf_create(moq_alloc_default(), bytes,
                                         OBJECT_SIZE, &buf) == 0) {
                        (void)moq_session_write_object(s, sg, 0, buf,
                                                       now);
                        moq_rcbuf_decref(buf);
                    }
                    free(bytes);
                }
                (void)moq_session_close_subgroup(s, sg, now);
            }
        }
    }
    pthread_mutex_unlock(&sv->mu);
    return 0;
}

static moq_msquic_managed_t *make_multi_server(struct msrv *sv,
                                               const char *cert,
                                               const char *key,
                                               uint32_t max_conns)
{
    moq_msquic_managed_cfg_t cfg;

    memset(sv, 0, sizeof(*sv));
    pthread_mutex_init(&sv->mu, NULL);
    moq_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.host = "127.0.0.1";
    cfg.port = 0;
    cfg.cert_path = cert;
    cfg.key_path = key;
    cfg.on_lane_pump = multi_server_pump;
    cfg.on_lane_pump_user = sv;
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 16;
    cfg.max_events = 64;
    cfg.max_connections = max_conns;
    if (moq_msquic_managed_create(&cfg, &sv->m) != MOQ_OK)
        return NULL;
    return sv->m;
}

static moq_msquic_managed_t *make_client(struct side *sd, uint16_t port,
                                         bool hold_open)

{
    moq_msquic_managed_cfg_t cfg;

    side_init(sd);
    sd->hold_open = hold_open;
    moq_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.host = "127.0.0.1";
    cfg.port = port;
    cfg.insecure_skip_verify = true;
    cfg.on_lane_pump = client_pump;
    cfg.on_lane_pump_user = sd;
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 16;
    cfg.max_events = 64;
    if (moq_msquic_managed_create(&cfg, &sd->m) != MOQ_OK)
        return NULL;
    return sd->m;
}

static void t_wake_pump_contract(const char *cert, const char *key)
{
    int before = failures;
    struct wake_srv sv;
    struct side cl;

    memset(&sv, 0, sizeof(sv));
    pthread_mutex_init(&sv.mu, NULL);

    moq_msquic_managed_cfg_t cfg;
    moq_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.host = "127.0.0.1";
    cfg.port = 0;
    cfg.cert_path = cert;
    cfg.key_path = key;
    cfg.on_lane_pump = wake_server_pump;
    cfg.on_lane_pump_user = &sv;
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 16;
    cfg.max_events = 64;
    CHECK(moq_msquic_managed_create(&cfg, &sv.m) == MOQ_OK);
    if (sv.m == NULL)
        return;

    CHECK(make_client(&cl, moq_msquic_managed_port(sv.m), true) != NULL);
    if (cl.m == NULL)
        return;
    /* client is up and subscribed; the connection then goes IDLE — the
     * server pump saw the request but holds publication */
    for (int i = 0; i < 3000; i++) {
        pthread_mutex_lock(&sv.mu);
        bool acc = sv.accepted;
        pthread_mutex_unlock(&sv.mu);
        if (acc)
            break;
        (void)moq_msquic_managed_wait(cl.m, 10 * 1000);
    }

    /* a bare wake with nothing to do still runs on_lane_pump */
    pthread_mutex_lock(&sv.mu);
    int pumps_before = sv.pumps;
    pthread_mutex_unlock(&sv.mu);
    CHECK(moq_msquic_managed_wake(sv.m) == MOQ_OK);
    for (int i = 0; i < 3000; i++) {
        pthread_mutex_lock(&sv.mu);
        bool ran = sv.pumps > pumps_before;
        pthread_mutex_unlock(&sv.mu);
        if (ran)
            break;
        struct timespec ts = { 0, 10 * 1000 * 1000 };
        nanosleep(&ts, NULL);
    }
    pthread_mutex_lock(&sv.mu);
    CHECK(sv.pumps > pumps_before);
    pthread_mutex_unlock(&sv.mu);

    /* publication rides ONLY the wake path: flag + wake -> the pump
     * queues the object and the wake pass must drain it */
    pthread_mutex_lock(&sv.mu);
    sv.publish_now = true;
    pthread_mutex_unlock(&sv.mu);
    CHECK(moq_msquic_managed_wake(sv.m) == MOQ_OK);
    for (int i = 0; i < 3000; i++) {
        pthread_mutex_lock(&cl.mu);
        bool got = cl.objects >= 1;
        pthread_mutex_unlock(&cl.mu);
        if (got)
            break;
        (void)moq_msquic_managed_wait(cl.m, 10 * 1000);
    }
    pthread_mutex_lock(&cl.mu);
    CHECK(cl.objects == 1);
    CHECK(cl.object_errors == 0);
    pthread_mutex_unlock(&cl.mu);

    /* a close recorded during the wake-driven pump is fed and delivered */
    pthread_mutex_lock(&sv.mu);
    sv.close_now = true;
    pthread_mutex_unlock(&sv.mu);
    CHECK(moq_msquic_managed_wake(sv.m) == MOQ_OK);
    CHECK(wait_terminal(cl.m, 30000));
    CHECK(moq_msquic_managed_is_closed(cl.m));
    CHECK(!moq_msquic_managed_is_fatal(cl.m));

    CHECK(moq_msquic_managed_stop(cl.m) == MOQ_OK);
    CHECK(moq_msquic_managed_stop(sv.m) == MOQ_OK);
    moq_msquic_managed_destroy(cl.m);
    moq_msquic_managed_destroy(sv.m);

    side_destroy(&cl);
    pthread_mutex_destroy(&sv.mu);
    if (failures == before)
        printf("PASS: wake_pump_contract\n");
}

/* Bounded wait for a live-connection count on the server. */
static bool wait_conn_count(moq_msquic_managed_t *m, size_t want,
                            int max_ms)
{
    for (int i = 0; i < max_ms / 10; i++) {
        if (moq_msquic_managed_conn_count(m) == want)
            return true;
        struct timespec ts = { 0, 10 * 1000 * 1000 };
        nanosleep(&ts, NULL);
    }
    return moq_msquic_managed_conn_count(m) == want;
}

static bool wait_server_closed_before_reap(struct msrv *sv,
                                           size_t expected_count,
                                           int max_ms)
{
    for (int i = 0; i < max_ms / 10; i++) {
        pthread_mutex_lock(&sv->mu);
        bool observed = sv->session_closed > 0;
        size_t count = sv->first_closed_conn_count;
        pthread_mutex_unlock(&sv->mu);

        if (observed)
            return count == expected_count;
        if (moq_msquic_managed_conn_count(sv->m) < expected_count)
            return false;
        (void)moq_msquic_managed_wait(sv->m, 10 * 1000);
    }

    pthread_mutex_lock(&sv->mu);
    bool observed = sv->session_closed > 0;
    size_t count = sv->first_closed_conn_count;
    pthread_mutex_unlock(&sv->mu);
    return observed && count == expected_count;
}

/* Two clients against one server (cap 2): both complete the full
 * subscribe/object/clean-close flow on their own sessions, and the
 * server's accepted connections drain back to zero after their
 * terminals. */
static void t_multi_client(const char *cert, const char *key)
{
    int before = failures;
    struct msrv sv;
    struct side c1, c2;

    CHECK(make_multi_server(&sv, cert, key, 2) != NULL);
    if (sv.m == NULL)
        return;
    uint16_t port = moq_msquic_managed_port(sv.m);

    CHECK(make_client(&c1, port, true) != NULL);
    CHECK(make_client(&c2, port, true) != NULL); /* c2 holds open */
    if (c1.m == NULL || c2.m == NULL)
        return;

    for (int i = 0; i < 3000; i++) {
        pthread_mutex_lock(&c1.mu);
        pthread_mutex_lock(&c2.mu);
        bool both = c1.objects >= 1 && c2.objects >= 1;
        pthread_mutex_unlock(&c2.mu);
        pthread_mutex_unlock(&c1.mu);
        if (both && moq_msquic_managed_conn_count(sv.m) == 2)
            break;
        (void)moq_msquic_managed_wait(c1.m, 10 * 1000);
        (void)moq_msquic_managed_wait(c2.m, 10 * 1000);
    }
    CHECK(moq_msquic_managed_conn_count(sv.m) == 2);

    /* the first connection closes and is reclaimed while the second
     * stays live: the SERVER facade must not look terminal — its
     * listener and another connection are still serving */
    pthread_mutex_lock(&c1.mu);
    c1.hold_open = false;
    pthread_mutex_unlock(&c1.mu);
    (void)moq_msquic_managed_wake(c1.m);
    CHECK(wait_terminal(c1.m, 30000));
    CHECK(moq_msquic_managed_is_closed(c1.m));
    CHECK(!moq_msquic_managed_is_fatal(c1.m));
    CHECK(wait_server_closed_before_reap(&sv, 2, 30000));
    CHECK(wait_conn_count(sv.m, 1, 30000));
    /* drain queued activity, then the wait must time out — NOT report
     * the facade closed — while a connection and the listener live */
    {
        moq_result_t wrc;

        do {
            wrc = moq_msquic_managed_wait(sv.m, 200 * 1000);
        } while (wrc == MOQ_OK);
        CHECK(wrc == MOQ_DONE);
    }
    CHECK(moq_msquic_managed_wake(sv.m) == MOQ_OK);

    /* release the second client and let it finish cleanly */
    pthread_mutex_lock(&c2.mu);
    c2.hold_open = false;
    pthread_mutex_unlock(&c2.mu);
    (void)moq_msquic_managed_wake(c2.m);
    CHECK(wait_terminal(c2.m, 30000));
    CHECK(moq_msquic_managed_is_closed(c2.m));
    CHECK(!moq_msquic_managed_is_fatal(c2.m));
    /* both children reclaimed after their terminals */
    CHECK(wait_conn_count(sv.m, 0, 30000));

    CHECK(moq_msquic_managed_stop(c1.m) == MOQ_OK);
    CHECK(moq_msquic_managed_stop(c2.m) == MOQ_OK);
    CHECK(moq_msquic_managed_stop(sv.m) == MOQ_OK);
    moq_msquic_managed_destroy(c1.m);
    moq_msquic_managed_destroy(c2.m);
    moq_msquic_managed_destroy(sv.m);

    CHECK(c1.setup == 1);
    CHECK(c2.setup == 1);
    CHECK(c1.objects == 1);
    CHECK(c2.objects == 1);
    CHECK(c1.object_errors == 0);
    CHECK(c2.object_errors == 0);
    CHECK(sv.published == 2);

    side_destroy(&c1);
    side_destroy(&c2);
    pthread_mutex_destroy(&sv.mu);
    if (failures == before)
        printf("PASS: multi_client\n");
}

/* Cap 1: while the first client is active (holding its session open),
 * a second connect is refused at the listener — bounded fatal, no
 * setup — and the first stays healthy and finishes cleanly. */
static void t_cap_refusal(const char *cert, const char *key)
{
    int before = failures;
    struct msrv sv;
    struct side c1, c2;

    CHECK(make_multi_server(&sv, cert, key, 1) != NULL);
    if (sv.m == NULL)
        return;
    uint16_t port = moq_msquic_managed_port(sv.m);

    CHECK(make_client(&c1, port, true) != NULL); /* holds open */
    if (c1.m == NULL)
        return;
    /* first client fully up (it received its object and idles) */
    for (int i = 0; i < 3000; i++) {
        pthread_mutex_lock(&c1.mu);
        bool got = c1.objects >= 1;
        pthread_mutex_unlock(&c1.mu);
        if (got)
            break;
        (void)moq_msquic_managed_wait(c1.m, 10 * 1000);
    }

    CHECK(make_client(&c2, port, false) != NULL);
    if (c2.m == NULL)
        return;
    CHECK(wait_terminal(c2.m, 30000));
    CHECK(moq_msquic_managed_is_fatal(c2.m));
    CHECK(c2.setup == 0);

    /* the first client is still healthy: close it from its pump */
    pthread_mutex_lock(&c1.mu);
    c1.hold_open = false;
    pthread_mutex_unlock(&c1.mu);
    (void)moq_msquic_managed_wake(c1.m);
    CHECK(wait_terminal(c1.m, 30000));
    CHECK(moq_msquic_managed_is_closed(c1.m));
    CHECK(!moq_msquic_managed_is_fatal(c1.m));

    CHECK(moq_msquic_managed_stop(c1.m) == MOQ_OK);
    CHECK(moq_msquic_managed_stop(c2.m) == MOQ_OK);
    CHECK(moq_msquic_managed_stop(sv.m) == MOQ_OK);
    moq_msquic_managed_destroy(c1.m);
    moq_msquic_managed_destroy(c2.m);
    moq_msquic_managed_destroy(sv.m);

    CHECK(c1.setup == 1);
    CHECK(c1.objects == 1);
    CHECK(c1.object_errors == 0);

    side_destroy(&c1);
    side_destroy(&c2);
    pthread_mutex_destroy(&sv.mu);
    if (failures == before)
        printf("PASS: cap_refusal\n");
}

/* Cap 1, sequential reuse: after the first connection closes and its
 * child is reclaimed, the freed slot admits a second client — the
 * accept capacity really is live connections, not a one-shot. */
static void t_sequential_reuse(const char *cert, const char *key)
{
    int before = failures;
    struct msrv sv;
    struct side c1, c2;

    CHECK(make_multi_server(&sv, cert, key, 1) != NULL);
    if (sv.m == NULL)
        return;
    uint16_t port = moq_msquic_managed_port(sv.m);

    CHECK(make_client(&c1, port, false) != NULL);
    if (c1.m == NULL)
        return;
    CHECK(wait_terminal(c1.m, 30000));
    CHECK(moq_msquic_managed_is_closed(c1.m));
    CHECK(moq_msquic_managed_stop(c1.m) == MOQ_OK);
    moq_msquic_managed_destroy(c1.m);
    /* the child must be reclaimed before the next accept can fit */
    CHECK(wait_conn_count(sv.m, 0, 30000));

    CHECK(make_client(&c2, port, false) != NULL);
    if (c2.m == NULL)
        return;
    CHECK(wait_terminal(c2.m, 30000));
    CHECK(moq_msquic_managed_is_closed(c2.m));
    CHECK(!moq_msquic_managed_is_fatal(c2.m));

    CHECK(moq_msquic_managed_stop(c2.m) == MOQ_OK);
    CHECK(moq_msquic_managed_stop(sv.m) == MOQ_OK);
    moq_msquic_managed_destroy(c2.m);
    moq_msquic_managed_destroy(sv.m);

    CHECK(c1.objects == 1);
    CHECK(c2.objects == 1);
    CHECK(c1.object_errors == 0 && c2.object_errors == 0);
    CHECK(sv.published == 2);

    side_destroy(&c1);
    side_destroy(&c2);
    pthread_mutex_destroy(&sv.mu);
    if (failures == before)
        printf("PASS: sequential_reuse\n");
}

/* Server stop with two ACTIVE clients: stop() drains deterministically
 * (no hang), the children's pending sends/datagrams all complete or
 * cancel, and the peers observe an orderly close — not a fatal. */
static void t_stop_with_active(const char *cert, const char *key)
{
    int before = failures;
    struct msrv sv;
    struct side c1, c2;

    CHECK(make_multi_server(&sv, cert, key, 2) != NULL);
    if (sv.m == NULL)
        return;
    uint16_t port = moq_msquic_managed_port(sv.m);

    CHECK(make_client(&c1, port, true) != NULL); /* both hold open */
    CHECK(make_client(&c2, port, true) != NULL);
    if (c1.m == NULL || c2.m == NULL)
        return;
    for (int i = 0; i < 3000; i++) {
        pthread_mutex_lock(&c1.mu);
        pthread_mutex_lock(&c2.mu);
        bool both = c1.objects >= 1 && c2.objects >= 1;
        pthread_mutex_unlock(&c2.mu);
        pthread_mutex_unlock(&c1.mu);
        if (both)
            break;
        (void)moq_msquic_managed_wait(c1.m, 10 * 1000);
    }
    CHECK(moq_msquic_managed_conn_count(sv.m) == 2);

    /* stop with both connections live: bounded, drains, orderly */
    CHECK(moq_msquic_managed_stop(sv.m) == MOQ_OK);
    CHECK(moq_msquic_managed_conn_count(sv.m) == 0);
    CHECK(moq_msquic_managed_pending_sends(sv.m) == 0);
    CHECK(moq_msquic_managed_pending_datagrams(sv.m) == 0);
    moq_msquic_managed_destroy(sv.m);

    CHECK(wait_terminal(c1.m, 30000));
    CHECK(wait_terminal(c2.m, 30000));
    CHECK(moq_msquic_managed_is_closed(c1.m));
    CHECK(!moq_msquic_managed_is_fatal(c1.m));
    CHECK(moq_msquic_managed_is_closed(c2.m));
    CHECK(!moq_msquic_managed_is_fatal(c2.m));
    CHECK(moq_msquic_managed_pending_sends(c1.m) == 0);
    CHECK(moq_msquic_managed_pending_sends(c2.m) == 0);

    CHECK(moq_msquic_managed_stop(c1.m) == MOQ_OK);
    CHECK(moq_msquic_managed_stop(c2.m) == MOQ_OK);
    moq_msquic_managed_destroy(c1.m);
    moq_msquic_managed_destroy(c2.m);

    CHECK(c1.setup == 1 && c2.setup == 1);
    CHECK(c1.object_errors == 0 && c2.object_errors == 0);

    side_destroy(&c1);
    side_destroy(&c2);
    pthread_mutex_destroy(&sv.mu);
    if (failures == before)
        printf("PASS: stop_with_active\n");
}

/* Drain: refuse new connections while the existing one runs to its own
 * clean conclusion. */
static void t_drain(const char *cert, const char *key)
{
    int before = failures;
    struct msrv sv;
    struct side c1, c2;

    CHECK(make_multi_server(&sv, cert, key, 2) != NULL);
    if (sv.m == NULL)
        return;
    uint16_t port = moq_msquic_managed_port(sv.m);

    CHECK(make_client(&c1, port, true) != NULL);
    if (c1.m == NULL)
        return;
    for (int i = 0; i < 3000; i++) {
        pthread_mutex_lock(&c1.mu);
        bool got = c1.objects >= 1;
        pthread_mutex_unlock(&c1.mu);
        if (got)
            break;
        (void)moq_msquic_managed_wait(c1.m, 10 * 1000);
    }

    moq_msquic_managed_drain(sv.m);
    CHECK(make_client(&c2, port, false) != NULL);
    if (c2.m == NULL)
        return;
    CHECK(wait_terminal(c2.m, 30000));
    CHECK(moq_msquic_managed_is_fatal(c2.m)); /* refused */
    CHECK(c2.setup == 0);

    /* the existing connection still finishes cleanly */
    pthread_mutex_lock(&c1.mu);
    c1.hold_open = false;
    pthread_mutex_unlock(&c1.mu);
    (void)moq_msquic_managed_wake(c1.m);
    CHECK(wait_terminal(c1.m, 30000));
    CHECK(moq_msquic_managed_is_closed(c1.m));
    CHECK(wait_conn_count(sv.m, 0, 30000));

    CHECK(moq_msquic_managed_stop(c1.m) == MOQ_OK);
    CHECK(moq_msquic_managed_stop(c2.m) == MOQ_OK);
    CHECK(moq_msquic_managed_stop(sv.m) == MOQ_OK);
    moq_msquic_managed_destroy(c1.m);
    moq_msquic_managed_destroy(c2.m);
    moq_msquic_managed_destroy(sv.m);

    side_destroy(&c1);
    side_destroy(&c2);
    pthread_mutex_destroy(&sv.mu);
    if (failures == before)
        printf("PASS: drain\n");
}

/* An old binary's frozen-prefix config (initialized with the v0 size,
 * no appended fields) must still create a working client that defaults
 * to draft-16 — the append-only ABI contract, proven on the wire. */
static void t_v0_prefix_cfg_defaults(const char *cert, const char *key)
{
    int before = failures;
    struct side sv, cl;

    side_init(&sv);
    side_init(&cl);

    moq_msquic_managed_cfg_t scfg;
    moq_msquic_managed_cfg_init_sized(&scfg, sizeof(scfg));
    scfg.alloc = moq_alloc_default();
    scfg.perspective = MOQ_PERSPECTIVE_SERVER;
    scfg.host = "127.0.0.1";
    scfg.port = 0;
    scfg.cert_path = cert;
    scfg.key_path = key;
    scfg.on_lane_pump = server_pump;
    scfg.on_lane_pump_user = &sv;
    scfg.send_request_capacity = true;
    scfg.initial_request_capacity = 16;
    scfg.max_events = 64;
    CHECK(moq_msquic_managed_create(&scfg, &sv.m) == MOQ_OK);
    if (sv.m == NULL)
        return;

    moq_msquic_managed_cfg_t ccfg;
    memset(&ccfg, 0xff, sizeof(ccfg)); /* poison the appended tail */
    moq_msquic_managed_cfg_init_sized(
        &ccfg, offsetof(moq_msquic_managed_cfg_t, version));
    ccfg.alloc = moq_alloc_default();
    ccfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    ccfg.host = "127.0.0.1";
    ccfg.port = moq_msquic_managed_port(sv.m);
    ccfg.insecure_skip_verify = true;
    ccfg.on_lane_pump = client_pump;
    ccfg.on_lane_pump_user = &cl;
    ccfg.send_request_capacity = true;
    ccfg.initial_request_capacity = 16;
    ccfg.max_events = 64;
    CHECK(moq_msquic_managed_create(&ccfg, &cl.m) == MOQ_OK);
    if (cl.m == NULL) {
        (void)moq_msquic_managed_stop(sv.m);
        moq_msquic_managed_destroy(sv.m);
        return;
    }

    CHECK(wait_terminal(cl.m, 30000));
    CHECK(wait_terminal(sv.m, 30000));
    CHECK(moq_msquic_managed_is_closed(cl.m));
    CHECK(!moq_msquic_managed_is_fatal(cl.m));

    CHECK(moq_msquic_managed_stop(cl.m) == MOQ_OK);
    CHECK(moq_msquic_managed_stop(sv.m) == MOQ_OK);
    moq_msquic_managed_destroy(cl.m);
    moq_msquic_managed_destroy(sv.m);

    /* the draft-16 default flow completed end to end */
    CHECK(cl.setup == 1);
    CHECK(cl.objects == 1);
    CHECK(cl.object_errors == 0);

    side_destroy(&sv);
    side_destroy(&cl);
    if (failures == before)
        printf("PASS: v0_prefix_cfg_defaults\n");
}

static int idle_pump(moq_msquic_managed_t *m,
                     moq_msquic_managed_lane_t *lane, uint64_t now,
                     void *ctx)
{
    (void)m;
    (void)lane;
    (void)now;
    (void)ctx;
    return 0;
}

/* A connect to a dead port fails within the configured idle window:
 * fatal terminal, no setup, and stop/destroy stay clean. */
static void t_connect_failure_fatal(void)
{
    int before = failures;
    moq_msquic_managed_t *m = NULL;

    moq_msquic_managed_cfg_t cfg;
    moq_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.host = "127.0.0.1";
    cfg.port = 1; /* nothing listens there */
    cfg.insecure_skip_verify = true;
    cfg.idle_timeout_ms = 2000;
    cfg.on_lane_pump = idle_pump;
    CHECK(moq_msquic_managed_create(&cfg, &m) == MOQ_OK);
    if (m == NULL)
        return;

    CHECK(wait_terminal(m, 20000));
    CHECK(moq_msquic_managed_is_fatal(m));
    CHECK(!moq_msquic_managed_is_closed(m));
    CHECK(moq_msquic_managed_stop(m) == MOQ_OK);
    moq_msquic_managed_destroy(m);

    if (failures == before)
        printf("PASS: connect_failure_fatal\n");
}

int main(int argc, char **argv)
{
    if (argc < 3) {
        fprintf(stderr, "usage: %s <cert.pem> <key.pem>\n", argv[0]);
        return 2;
    }

    /* one library reference for the whole process: managed facades
     * open/close MsQuic per instance, and letting the global refcount
     * bounce to zero between scenarios re-initializes MsQuic's
     * datapath each time — churn that has shown rare transient
     * connect failures. Never released; reachable until exit. */
    static const QUIC_API_TABLE *lib_pin;

    if (QUIC_FAILED(MsQuicOpen2(&lib_pin))) {
        fprintf(stderr, "MsQuicOpen2 failed\n");
        return 2;
    }

    t_pubsub_oversized_clean_close(argv[1], argv[2]);
    t_streaming_objects(argv[1], argv[2]);
    t_v0_prefix_cfg_defaults(argv[1], argv[2]);
    t_multi_client(argv[1], argv[2]);
    t_cap_refusal(argv[1], argv[2]);
    t_sequential_reuse(argv[1], argv[2]);
    t_stop_with_active(argv[1], argv[2]);
    t_drain(argv[1], argv[2]);
    t_wake_pump_contract(argv[1], argv[2]);
    t_connect_failure_fatal();

    if (failures == 0)
        printf("PASS: msquic_loopback\n");
    else
        fprintf(stderr, "FAIL: msquic_loopback (%d)\n", failures);
    return failures;
}
