/*
 * Relay-egress fanout comparison: ONE managed server publishing an
 * independent object stream to each of N subscriber connections, over
 * the raw MoQ-over-QUIC managed paths (msquic_managed, pq_threaded).
 *
 * This measures the axis the single-connection A/B (bench_quic_compare)
 * cannot: how aggregate egress scales with connection count. The two
 * facades differ exactly where it matters — pq_threaded services every
 * connection from ONE network thread by design, and msquic_managed
 * spreads connections across MsQuic's per-core workers and partitions
 * them across `lane_count` lanes, each with its own mutex + doorbell
 * (one lane = a single serializing domain; --lanes fans across them).
 * The N-sweep makes both ceilings visible.
 *
 * Per subscriber: SETUP -> SUBSCRIBE -> N objects on one subgroup
 * stream, deterministic payloads verified on arrival (index prefix and
 * length on every object). The measurement window opens when every
 * subscriber has its warmup objects (at SUBSCRIBE_OK when warmup is 0)
 * and closes when the last subscriber completes; aggregate = total
 * measured objects / window. Startup skew is inside the window, so the
 * number is conservative.
 *
 * Output: one JSON line per run:
 *   {"backend":..., "conns":N, "objects_per_sub":..., "aggregate_obj_s":...,
 *    "aggregate_mbps":..., "errors":...}
 */

#if defined(__linux__) && !defined(_POSIX_C_SOURCE)
#define _POSIX_C_SOURCE 200809L
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <moq/moq.h>
#include <moq/msquic_managed.h>
#include <moq/picoquic_threaded.h>
#ifdef MOQ_BENCH_HAVE_MVFST
#include <moq/mvfst.h>
#endif

#define FAN_MAX_CONNS 128
#define FAN_BURST 64
#define FAN_TIMEOUT_SECS 120

struct fan_opts {
    const char *backend;    /* "msquic_managed" | "pq_threaded" |
                               "mvfst_managed" */
    const char *cert;
    const char *key;
    uint64_t conns;
    uint64_t objects;       /* per subscriber, warmup included */
    uint64_t object_size;   /* >= 8 */
    uint64_t warmup;        /* per subscriber */
    int port;               /* pq_threaded listen port */
    uint32_t lanes;         /* msquic_managed server lanes (0/1 = single;
                               >1 fans connections across lane doorbells
                               round-robin). Ignored by other backends. */
};

static uint64_t fan_now_us(void)
{
    struct timespec ts;

    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000u + (uint64_t)ts.tv_nsec / 1000u;
}

static void payload_fill(uint8_t *dst, size_t size, uint64_t idx)
{
    for (size_t i = 0; i < 8 && i < size; i++)
        dst[i] = (uint8_t)(idx >> (8 * i));
    for (size_t i = 8; i < size; i++)
        dst[i] = (uint8_t)(idx * 131u + i * 7u + 47u);
}

/* --- server side: one publisher per accepted connection ----------------- */

struct fan_pub {
    moq_session_t *s;       /* identity key (stable while the conn lives) */
    moq_subscription_t sub;
    moq_subgroup_handle_t sg;
    bool have_sub;
    bool sg_open;
    bool sg_closed;
    uint64_t published;
};

struct fan_server {
    const struct fan_opts *o;
    struct fan_pub pubs[FAN_MAX_CONNS];
    size_t pub_count;
    uint64_t errors;
};

static struct fan_pub *fan_pub_for(struct fan_server *sv, moq_session_t *s)
{
    for (size_t i = 0; i < sv->pub_count; i++)
        if (sv->pubs[i].s == s)
            return &sv->pubs[i];
    if (sv->pub_count >= FAN_MAX_CONNS)
        return NULL;
    struct fan_pub *p = &sv->pubs[sv->pub_count++];

    memset(p, 0, sizeof(*p));
    p->s = s;
    return p;
}

/* One pump pass for one connection's session: accept its subscribe,
 * then publish its stream up to the per-subscriber target. */
static void fan_serve_session(struct fan_server *sv, moq_session_t *s,
                              uint64_t now)
{
    struct fan_pub *p = fan_pub_for(sv, s);
    moq_event_t ev;

    if (p == NULL) {
        sv->errors++;
        return;
    }
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST && !p->have_sub) {
            moq_accept_subscribe_cfg_t ac;

            moq_accept_subscribe_cfg_init(&ac);
            if (moq_session_accept_subscribe(
                    s, ev.u.subscribe_request.sub, &ac, now) < 0) {
                sv->errors++;
            } else {
                p->sub = ev.u.subscribe_request.sub;
                p->have_sub = true;
            }
        }
        moq_event_cleanup(&ev);
    }
    if (!p->have_sub)
        return;
    if (!p->sg_open) {
        moq_subgroup_cfg_t sgc;

        moq_subgroup_cfg_init(&sgc);
        sgc.group_id = 0;
        sgc.publisher_priority = 200;
        if (moq_session_open_subgroup(s, p->sub, &sgc, now, &p->sg) < 0)
            return; /* pressure: retry on a later pump */
        p->sg_open = true;
    }
    for (uint64_t burst = 0;
         burst < FAN_BURST && p->published < sv->o->objects; burst++) {
        uint8_t *bytes = malloc(sv->o->object_size);
        moq_rcbuf_t *buf = NULL;

        if (bytes == NULL) {
            sv->errors++;
            return;
        }
        payload_fill(bytes, sv->o->object_size, p->published);
        int rc = moq_rcbuf_create(moq_alloc_default(), bytes,
                                  sv->o->object_size, &buf);
        free(bytes);
        if (rc < 0) {
            sv->errors++;
            return;
        }
        if (moq_session_write_object(s, p->sg, p->published, buf,
                                     now) < 0) {
            moq_rcbuf_decref(buf);
            return; /* pressure: retry on a later pump */
        }
        moq_rcbuf_decref(buf);
        p->published++;
    }
    if (p->published == sv->o->objects && !p->sg_closed) {
        if (moq_session_close_subgroup(s, p->sg, now) == 0)
            p->sg_closed = true;
    }
}

/* --- subscriber side (one per client facade) ----------------------------- */

struct fan_sub {
    const struct fan_opts *o;
    uint64_t received;
    uint64_t errors;
    uint64_t t_warm;        /* SUBSCRIBE_OK, or warmup-th arrival */
    uint64_t t_end;         /* time of the last arrival */
    bool sub_sent;
    bool done;
};

static void fan_sub_event(struct fan_sub *b, moq_session_t *s,
                          const moq_event_t *ev, uint64_t now)
{
    switch (ev->kind) {
    case MOQ_EVENT_SETUP_COMPLETE:
        if (!b->sub_sent) {
            static const moq_bytes_t ns[] = {
                { (const uint8_t *)"fan", 3 },
            };
            moq_subscribe_cfg_t sc;
            moq_subscription_t sub;

            moq_subscribe_cfg_init(&sc);
            sc.track_namespace.parts = (moq_bytes_t *)ns;
            sc.track_namespace.count = 1;
            sc.track_name =
                (moq_bytes_t){ (const uint8_t *)"egress", 6 };
            sc.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
            b->sub_sent = true;
            if (moq_session_subscribe(s, &sc, now, &sub) < 0)
                b->errors++;
        }
        break;
    case MOQ_EVENT_SUBSCRIBE_OK:
        if (b->o->warmup == 0 && b->t_warm == 0)
            b->t_warm = fan_now_us();
        break;
    case MOQ_EVENT_OBJECT_RECEIVED: {
        const moq_rcbuf_t *pl = ev->u.object_received.payload;

        if (pl == NULL || moq_rcbuf_len(pl) != b->o->object_size) {
            b->errors++;
            break;
        }
        const uint8_t *data = moq_rcbuf_data(pl);
        uint64_t idx = 0;

        for (size_t i = 0; i < 8; i++)
            idx |= (uint64_t)data[i] << (8 * i);
        if (idx != b->received) /* in-order on one subgroup stream */
            b->errors++;
        b->received++;
        if (b->o->warmup == 0 && b->t_warm == 0)
            b->t_warm = fan_now_us();
        else if (b->received == b->o->warmup)
            b->t_warm = fan_now_us();
        if (b->received == b->o->objects) {
            b->t_end = fan_now_us();
            b->done = true;
        }
        break;
    }
    default:
        break;
    }
}

/* --- msquic runners ------------------------------------------------------- */

static int msq_srv_pump(moq_msquic_managed_t *m,
                        moq_msquic_managed_lane_t *lane, uint64_t now,
                        void *ctx)
{
    struct fan_server *sv = ctx;

    (void)m;
    /* one lane pump services every connection the lane owns in a single
     * pass — across lanes the doorbells run concurrently, so fanout
     * scales past a single lane lock */
    for (moq_msquic_managed_conn_t *c = moq_msquic_lane_next_conn(lane, NULL);
         c != NULL; c = moq_msquic_lane_next_conn(lane, c)) {
        moq_session_t *s = moq_msquic_managed_conn_session(c);

        if (s != NULL)
            fan_serve_session(sv, s, now);
    }
    return 0;
}

static int msq_cli_pump(moq_msquic_managed_t *m,
                        moq_msquic_managed_lane_t *lane, uint64_t now,
                        void *ctx)
{
    struct fan_sub *b = ctx;
    moq_session_t *s = moq_msquic_managed_session(m);
    moq_event_t ev;

    (void)lane;
    if (s == NULL)
        return 0;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        fan_sub_event(b, s, &ev, now);
        moq_event_cleanup(&ev);
    }
    return 0;
}

/* --- pq runners ------------------------------------------------------------ */

static int pq_srv_pump(moq_pq_threaded_t *t, moq_pq_threaded_lane_t *lane,
                        uint64_t now, void *ctx)
{
    struct fan_server *sv = ctx;

    (void)t;
    for (moq_pq_threaded_conn_t *c = moq_pq_threaded_lane_next_conn(lane, NULL);
         c != NULL; c = moq_pq_threaded_lane_next_conn(lane, c)) {
        moq_session_t *s = moq_pq_threaded_conn_session(c);

        if (s != NULL)
            fan_serve_session(sv, s, now);
    }
    return 0;
}

static int pq_cli_pump(moq_pq_threaded_t *t, moq_pq_threaded_lane_t *lane,
                        uint64_t now, void *ctx)
{
    struct fan_sub *b = ctx;
    moq_session_t *s = moq_pq_threaded_session(t);
    moq_event_t ev;

    (void)lane;
    if (s == NULL)
        return 0;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        fan_sub_event(b, s, &ev, now);
        moq_event_cleanup(&ev);
    }
    return 0;
}

/* --- run ------------------------------------------------------------------- */

static int fan_report(const struct fan_opts *o, struct fan_sub *subs,
                      uint64_t server_errors)
{
    uint64_t t_warm_latest = 0, t_end_latest = 0, errors = server_errors;
    uint64_t measured = 0;

    for (uint64_t i = 0; i < o->conns; i++) {
        struct fan_sub *b = &subs[i];

        errors += b->errors;
        if (!b->done) {
            fprintf(stderr, "%s: sub %llu incomplete (%llu/%llu)\n",
                    o->backend, (unsigned long long)i,
                    (unsigned long long)b->received,
                    (unsigned long long)o->objects);
            return -1;
        }
        if (b->t_warm > t_warm_latest)
            t_warm_latest = b->t_warm;
        if (b->t_end > t_end_latest)
            t_end_latest = b->t_end;
        measured += o->objects - o->warmup;
    }
    if (errors != 0 || t_end_latest <= t_warm_latest) {
        fprintf(stderr, "%s: errors=%llu\n", o->backend,
                (unsigned long long)errors);
        return -1;
    }
    double secs = (double)(t_end_latest - t_warm_latest) / 1e6;
    double ops = (double)measured / secs;

    printf("{\"backend\":\"%s\",\"conns\":%llu,"
           "\"objects_per_sub\":%llu,\"object_size\":%llu,"
           "\"aggregate_obj_s\":%.1f,\"aggregate_mbps\":%.1f,"
           "\"errors\":0}\n",
           o->backend, (unsigned long long)o->conns,
           (unsigned long long)o->objects,
           (unsigned long long)o->object_size, ops,
           ops * (double)o->object_size * 8.0 / 1e6);
    return 0;
}

static int run_msq(const struct fan_opts *o)
{
    int rc = -1;
    struct fan_server sv = { .o = o };
    struct fan_sub subs[FAN_MAX_CONNS];
    moq_msquic_managed_t *srv = NULL;
    moq_msquic_managed_t *clis[FAN_MAX_CONNS] = { 0 };
    moq_msquic_managed_cfg_t cfg;
    uint64_t deadline =
        fan_now_us() + (uint64_t)FAN_TIMEOUT_SECS * 1000000u;

    memset(subs, 0, sizeof(subs));
    moq_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.host = "127.0.0.1";
    cfg.port = 0;
    cfg.cert_path = o->cert;
    cfg.key_path = o->key;
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 10;
    cfg.max_events = 512;
    cfg.max_connections = (uint32_t)o->conns;
    if (o->lanes > 1)
        cfg.lane_count = o->lanes;
    cfg.on_lane_pump = msq_srv_pump;
    cfg.on_lane_pump_user = &sv;
    if (moq_msquic_managed_create(&cfg, &srv) != MOQ_OK)
        goto out;

    for (uint64_t i = 0; i < o->conns; i++) {
        subs[i].o = o;
        moq_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
        cfg.alloc = moq_alloc_default();
        cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
        cfg.host = "127.0.0.1";
        cfg.port = moq_msquic_managed_port(srv);
        cfg.insecure_skip_verify = true;
        cfg.send_request_capacity = true;
        cfg.initial_request_capacity = 10;
        cfg.max_events = 512;
        cfg.on_lane_pump = msq_cli_pump;
        cfg.on_lane_pump_user = &subs[i];
        if (moq_msquic_managed_create(&cfg, &clis[i]) != MOQ_OK)
            goto out;
    }

    for (;;) {
        bool all = true;

        for (uint64_t i = 0; i < o->conns; i++)
            if (!subs[i].done) {
                all = false;
                /* watchdog tick for stragglers; progress itself is
                 * transport-event driven on both sides */
                (void)moq_msquic_managed_wait(clis[i], 2000);
                break;
            }
        if (all)
            break;
        if (fan_now_us() > deadline) {
            fprintf(stderr, "msquic_managed: timed out\n");
            goto out_stop;
        }
        (void)moq_msquic_managed_wake(srv);
    }
    rc = 0;

out_stop:
    for (uint64_t i = 0; i < o->conns; i++)
        if (clis[i] != NULL)
            (void)moq_msquic_managed_stop(clis[i]);
    (void)moq_msquic_managed_stop(srv);
    if (rc == 0)
        rc = fan_report(o, subs, sv.errors);
out:
    for (uint64_t i = 0; i < o->conns; i++)
        if (clis[i] != NULL) {
            (void)moq_msquic_managed_stop(clis[i]);
            moq_msquic_managed_destroy(clis[i]);
        }
    if (srv != NULL) {
        (void)moq_msquic_managed_stop(srv);
        moq_msquic_managed_destroy(srv);
    }
    return rc;
}

static int run_pq(const struct fan_opts *o)
{
    int rc = -1;
    struct fan_server sv = { .o = o };
    struct fan_sub subs[FAN_MAX_CONNS];
    moq_pq_threaded_t *srv = NULL;
    moq_pq_threaded_t *clis[FAN_MAX_CONNS] = { 0 };
    moq_pq_threaded_cfg_t cfg;
    uint64_t deadline =
        fan_now_us() + (uint64_t)FAN_TIMEOUT_SECS * 1000000u;

    memset(subs, 0, sizeof(subs));
    moq_pq_threaded_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.cert_path = o->cert;
    cfg.key_path = o->key;
    cfg.port = o->port;
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 10;
    cfg.max_events = 512;
    cfg.max_connections = (uint32_t)o->conns;
    cfg.on_lane_pump = pq_srv_pump;
    cfg.on_lane_pump_ctx = &sv;
    if (moq_pq_threaded_create(&cfg, &srv) != MOQ_OK)
        goto out;
    for (int i = 0; i < 3; i++)
        (void)moq_pq_threaded_wait(srv, 100000);

    for (uint64_t i = 0; i < o->conns; i++) {
        subs[i].o = o;
        moq_pq_threaded_cfg_init_sized(&cfg, sizeof(cfg));
        cfg.alloc = moq_alloc_default();
        cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
        cfg.host = "127.0.0.1";
        cfg.port = o->port;
        cfg.insecure_skip_verify = true;
        cfg.send_request_capacity = true;
        cfg.initial_request_capacity = 10;
        cfg.max_events = 512;
        cfg.on_lane_pump = pq_cli_pump;
        cfg.on_lane_pump_ctx = &subs[i];
        if (moq_pq_threaded_create(&cfg, &clis[i]) != MOQ_OK)
            goto out;
    }

    for (;;) {
        bool all = true;

        for (uint64_t i = 0; i < o->conns; i++)
            if (!subs[i].done) {
                all = false;
                (void)moq_pq_threaded_wait(clis[i], 2000);
                break;
            }
        if (all)
            break;
        if (fan_now_us() > deadline) {
            fprintf(stderr, "pq_threaded: timed out\n");
            goto out_stop;
        }
        (void)moq_pq_threaded_wake(srv);
    }
    rc = 0;

out_stop:
    for (uint64_t i = 0; i < o->conns; i++)
        if (clis[i] != NULL)
            (void)moq_pq_threaded_stop(clis[i]);
    (void)moq_pq_threaded_stop(srv);
    if (rc == 0)
        rc = fan_report(o, subs, sv.errors);
out:
    for (uint64_t i = 0; i < o->conns; i++)
        if (clis[i] != NULL) {
            (void)moq_pq_threaded_stop(clis[i]);
            moq_pq_threaded_destroy(clis[i]);
        }
    if (srv != NULL) {
        (void)moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
    }
    return rc;
}

#ifdef MOQ_BENCH_HAVE_MVFST
/* --- mvfst runners ---------------------------------------------------------- */

static int mvfst_srv_pump(moq_mvfst_managed_t *m,
                          moq_mvfst_managed_lane_t *lane, uint64_t now,
                          void *ctx)
{
    struct fan_server *sv = ctx;

    (void)m;
    for (moq_mvfst_conn_t *c = moq_mvfst_lane_next_conn(lane, NULL);
         c != NULL; c = moq_mvfst_lane_next_conn(lane, c)) {
        moq_session_t *s = moq_mvfst_conn_session(c);

        if (s != NULL)
            fan_serve_session(sv, s, now);
    }
    return 0;
}

static int mvfst_cli_pump(moq_mvfst_managed_t *m,
                          moq_mvfst_managed_lane_t *lane, uint64_t now,
                          void *ctx)
{
    struct fan_sub *b = ctx;
    moq_session_t *s = moq_mvfst_managed_session(m);
    moq_event_t ev;

    (void)lane;

    if (s == NULL)
        return 0;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        fan_sub_event(b, s, &ev, now);
        moq_event_cleanup(&ev);
    }
    return 0;
}

static int run_mvfst(const struct fan_opts *o)
{
    int rc = -1;
    struct fan_server sv = { .o = o };
    struct fan_sub subs[FAN_MAX_CONNS];
    moq_mvfst_managed_t *srv = NULL;
    moq_mvfst_managed_t *clis[FAN_MAX_CONNS] = { 0 };
    moq_mvfst_managed_cfg_t cfg;
    uint64_t deadline =
        fan_now_us() + (uint64_t)FAN_TIMEOUT_SECS * 1000000u;

    memset(subs, 0, sizeof(subs));
    moq_mvfst_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.host = "127.0.0.1";
    cfg.port = 0;
    cfg.cert_path = o->cert;
    cfg.key_path = o->key;
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 10;
    cfg.max_events = 512;
    cfg.max_connections = o->conns;
    cfg.on_lane_pump = mvfst_srv_pump;
    cfg.user_ctx = &sv;
    if (moq_mvfst_managed_create(&cfg, &srv) != MOQ_OK)
        goto out;

    for (uint64_t i = 0; i < o->conns; i++) {
        subs[i].o = o;
        moq_mvfst_managed_cfg_init_sized(&cfg, sizeof(cfg));
        cfg.alloc = moq_alloc_default();
        cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
        cfg.host = "127.0.0.1";
        cfg.port = moq_mvfst_managed_local_port(srv);
        cfg.insecure_skip_verify = true;
        cfg.send_request_capacity = true;
        cfg.initial_request_capacity = 10;
        cfg.max_events = 512;
        cfg.on_lane_pump = mvfst_cli_pump;
        cfg.user_ctx = &subs[i];
        if (moq_mvfst_managed_create(&cfg, &clis[i]) != MOQ_OK)
            goto out;
    }

    for (;;) {
        bool all = true;

        for (uint64_t i = 0; i < o->conns; i++)
            if (!subs[i].done) {
                all = false;
                (void)moq_mvfst_managed_wait(clis[i], 2000);
                break;
            }
        if (all)
            break;
        if (fan_now_us() > deadline) {
            fprintf(stderr, "mvfst_managed: timed out\n");
            goto out_stop;
        }
        (void)moq_mvfst_managed_wake(srv);
    }
    rc = 0;

out_stop:
    for (uint64_t i = 0; i < o->conns; i++)
        if (clis[i] != NULL)
            (void)moq_mvfst_managed_stop(clis[i]);
    (void)moq_mvfst_managed_stop(srv);
    if (rc == 0)
        rc = fan_report(o, subs, sv.errors);
out:
    for (uint64_t i = 0; i < o->conns; i++)
        if (clis[i] != NULL) {
            (void)moq_mvfst_managed_stop(clis[i]);
            moq_mvfst_managed_destroy(clis[i]);
        }
    if (srv != NULL) {
        (void)moq_mvfst_managed_stop(srv);
        moq_mvfst_managed_destroy(srv);
    }
    return rc;
}
#endif /* MOQ_BENCH_HAVE_MVFST */

static void usage(const char *argv0)
{
    fprintf(stderr,
        "usage: %s --backend msquic_managed|pq_threaded|mvfst_managed\n"
        "          --cert <cert.pem> --key <key.pem>\n"
        "          [--conns N] [--objects N] [--object-size N]\n"
        "          [--warmup N] [--port N] [--lanes N]\n"
        "\n"
        "One managed server publishes an independent stream of\n"
        "`objects` objects to each of `conns` subscriber connections\n"
        "(each on its own managed client facade). Reports the\n"
        "aggregate delivery rate over the window from the last\n"
        "subscriber's warmup (or SUBSCRIBE_OK for --warmup 0) to\n"
        "the last subscriber's completion.\n",
        argv0);
}

int main(int argc, char **argv)
{
    struct fan_opts o = {
        .backend = NULL,
        .conns = 8,
        .objects = 2000,
        .object_size = 1200,
        .warmup = 100,
        .port = 14767,
    };

    for (int i = 1; i < argc; i++) {
        const char *a = argv[i];

        if (strcmp(a, "--backend") == 0 && i + 1 < argc)
            o.backend = argv[++i];
        else if (strcmp(a, "--cert") == 0 && i + 1 < argc)
            o.cert = argv[++i];
        else if (strcmp(a, "--key") == 0 && i + 1 < argc)
            o.key = argv[++i];
        else if (strcmp(a, "--conns") == 0 && i + 1 < argc)
            o.conns = strtoull(argv[++i], NULL, 10);
        else if (strcmp(a, "--objects") == 0 && i + 1 < argc)
            o.objects = strtoull(argv[++i], NULL, 10);
        else if (strcmp(a, "--object-size") == 0 && i + 1 < argc)
            o.object_size = strtoull(argv[++i], NULL, 10);
        else if (strcmp(a, "--warmup") == 0 && i + 1 < argc)
            o.warmup = strtoull(argv[++i], NULL, 10);
        else if (strcmp(a, "--port") == 0 && i + 1 < argc)
            o.port = atoi(argv[++i]);
        else if (strcmp(a, "--lanes") == 0 && i + 1 < argc)
            o.lanes = (uint32_t)strtoul(argv[++i], NULL, 10);
        else {
            usage(argv[0]);
            return 2;
        }
    }
    if (o.backend == NULL || o.cert == NULL || o.key == NULL ||
        o.conns == 0 || o.conns > FAN_MAX_CONNS ||
        o.object_size < 8 || o.warmup >= o.objects ||
        o.port <= 0 || o.port > 65535) {
        usage(argv[0]);
        return 2;
    }
    if (strcmp(o.backend, "msquic_managed") == 0)
        return run_msq(&o) == 0 ? 0 : 1;
    if (strcmp(o.backend, "pq_threaded") == 0)
        return run_pq(&o) == 0 ? 0 : 1;
#ifdef MOQ_BENCH_HAVE_MVFST
    if (strcmp(o.backend, "mvfst_managed") == 0)
        return run_mvfst(&o) == 0 ? 0 : 1;
#endif
    usage(argv[0]);
    return 2;
}
