/*
 * Idle managed-loopback discriminator for the automatic catalog refresh.
 *
 * A media_sender publishes over a REAL managed pico/pq_threaded endpoint (its
 * own network thread) to a push-only server that records catalog groups. After
 * the initial catalog is published and accepted (demand present), the test does
 * NO media writes and NO explicit application wake, then waits real time. The
 * catalog group can only advance if the managed adapter WAKES ITSELF at the
 * refresh deadline (the app_deadline fold) — so observing several distinct
 * catalog groups accrue while idle proves the backend-scheduled idle wake, which
 * a manual test_pump could not. Uses the shipping moq::service.
 */
#include <moq/media_sender.h>
#include <moq/endpoint.h>
#include <moq/picoquic_threaded.h>
#include <moq/session.h>
#include <moq/msf.h>
#include "test_support.h"

#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

static int failures = 0;

/* -- Minimal push-only server that records catalog groups ------------- */

#define CAP_MAX 256
typedef struct {
    pthread_mutex_t   mu;
    bool              pub_cat_ok;
    moq_publication_t pub_cat;
    int               n;
    uint64_t          groups[CAP_MAX];
} push_srv_t;

static push_srv_t g_ps;

static int psrv_pump(moq_pq_threaded_t *t, moq_pq_threaded_lane_t *lane,
                     uint64_t now_us, void *ctx)
{
    (void)lane;
    push_srv_t *ps = (push_srv_t *)ctx;
    moq_session_t *session = moq_pq_threaded_session(t);
    if (!session || moq_session_state(session) != MOQ_SESS_ESTABLISHED)
        return 0;
    moq_event_t ev;
    while (moq_session_poll_events(session, &ev, 1) == 1) {
        if (ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED) {
            moq_accept_namespace_cfg_t acc;
            moq_accept_namespace_cfg_init(&acc);
            moq_session_accept_namespace(session,
                ev.u.namespace_published.ann, &acc, now_us);
        } else if (ev.kind == MOQ_EVENT_PUBLISH_REQUEST) {
            const moq_publish_request_event_t *pr = &ev.u.publish_request;
            const bool is_catalog =
                pr->track_name.len == strlen(MOQ_MSF_CATALOG_TRACK_NAME) &&
                memcmp(pr->track_name.data, MOQ_MSF_CATALOG_TRACK_NAME,
                       pr->track_name.len) == 0;
            const moq_publication_t cand = pr->pub;
            moq_accept_publish_cfg_t acc;
            moq_accept_publish_cfg_init(&acc);
            if (moq_session_accept_publish(session, cand, &acc,
                                           now_us) == MOQ_OK && is_catalog) {
                pthread_mutex_lock(&ps->mu);
                ps->pub_cat = cand;
                ps->pub_cat_ok = true;
                pthread_mutex_unlock(&ps->mu);
            }
        } else if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) {
            const moq_object_received_event_t *o = &ev.u.object_received;
            pthread_mutex_lock(&ps->mu);
            if (ps->pub_cat_ok && moq_publication_eq(o->pub, ps->pub_cat) &&
                ps->n < CAP_MAX)
                ps->groups[ps->n++] = o->group_id;
            pthread_mutex_unlock(&ps->mu);
        }
        moq_event_cleanup(&ev);
    }
    return 0;
}

static moq_pq_threaded_t *psrv_start(const char *cert, const char *key,
                                     int *out_port)
{
    int base = 20600 + (int)(getpid() % 991);
    for (int attempt = 0; attempt < 8; attempt++) {
        int port = base + attempt * 13;
        moq_pq_threaded_cfg_t cfg;
        moq_pq_threaded_cfg_init_sized(&cfg, sizeof(cfg));
        cfg.alloc = moq_alloc_default();
        cfg.perspective = MOQ_PERSPECTIVE_SERVER;
        cfg.cert_path = cert;
        cfg.key_path = key;
        cfg.port = port;
        cfg.send_request_capacity = true;
        cfg.initial_request_capacity = 16;
        cfg.on_lane_pump = psrv_pump;
        cfg.on_lane_pump_ctx = &g_ps;
        moq_pq_threaded_t *t = NULL;
        if (moq_pq_threaded_create(&cfg, &t) == MOQ_OK) {
            *out_port = port;
            return t;
        }
    }
    return NULL;
}

static int distinct_groups(void)
{
    pthread_mutex_lock(&g_ps.mu);
    int d = 0;
    for (int i = 0; i < g_ps.n; i++) {
        bool seen = false;
        for (int j = 0; j < i; j++)
            if (g_ps.groups[j] == g_ps.groups[i]) { seen = true; break; }
        if (!seen) d++;
    }
    pthread_mutex_unlock(&g_ps.mu);
    return d;
}

static bool wait_ready(moq_media_sender_t *s, int ticks)
{
    for (int i = 0; i < ticks; i++) {
        if (moq_media_sender_is_ready(s)) return true;
        usleep(20000);
    }
    return false;
}

int main(int argc, char **argv)
{
    const char *cert = NULL, *key = NULL;
    for (int i = 1; i + 1 < argc; i++) {
        if (strcmp(argv[i], "--cert") == 0) cert = argv[++i];
        else if (strcmp(argv[i], "--key") == 0) key = argv[++i];
    }
    if (!cert || !key) { fprintf(stderr, "need --cert/--key\n"); return 1; }

    memset(&g_ps, 0, sizeof(g_ps));
    pthread_mutex_init(&g_ps.mu, NULL);
    int port = 0;
    moq_pq_threaded_t *srv = psrv_start(cert, key, &port);
    MOQ_TEST_CHECK(srv != NULL);
    if (!srv) return 1;

    char url[64];
    snprintf(url, sizeof(url), "moqt://127.0.0.1:%d", port);
    moq_endpoint_cfg_t ec;
    moq_endpoint_cfg_init(&ec);
    ec.url = (moq_bytes_t){ (const uint8_t *)url, strlen(url) };
    ec.insecure_skip_verify = true;

    moq_bytes_t parts[2] = { MOQ_BYTES_LITERAL("svc"),
                             MOQ_BYTES_LITERAL("demo") };
    moq_media_sender_cfg_t cfg;
    moq_media_sender_cfg_init_live_sized(&cfg, sizeof(cfg));
    cfg.namespace_ = (moq_namespace_t){ parts, 2 };
    cfg.endpoint = &ec;
    cfg.publish_tracks = true;
    /* Short interval so an idle window spans many refreshes. */
    cfg.catalog_refresh_interval_us = 100000;   /* 100 ms */
    moq_media_sender_t *s = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_create(&cfg, &s), (int)MOQ_OK);

    moq_media_track_t *v = NULL;
    moq_media_track_cfg_t tc;
    moq_media_track_cfg_init(&tc);
    tc.name = (moq_bytes_t){ (const uint8_t *)"v", 1 };
    tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
    tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
    tc.codec = (moq_bytes_t){ (const uint8_t *)"av01", 4 };
    tc.bitrate = 1500000;
    tc.is_live = true;
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_add_track(s, &tc, &v),
                          (int)MOQ_OK);

    MOQ_TEST_CHECK(wait_ready(s, 400));   /* namespace + catalog PUBLISH accepted */

    /* Let the initial catalog (group 0) land. */
    for (int i = 0; i < 100 && distinct_groups() < 1; i++) usleep(20000);
    int base_groups = distinct_groups();
    MOQ_TEST_CHECK(base_groups >= 1);

    /* IDLE WINDOW: no media writes, no add/remove, no application pump. The only
     * thing that can advance the catalog group is the adapter waking itself at
     * the refresh deadline. Wait ~1.5 s (≈15 intervals). */
    int target = base_groups + 4;
    int seen = base_groups;
    for (int i = 0; i < 75 && seen < target; i++) {   /* up to 1.5 s */
        usleep(20000);
        seen = distinct_groups();
    }

    /* The refresh fired repeatedly from idle adapter-scheduled wakes. */
    MOQ_TEST_CHECK(seen >= target);
    printf("[idle] base=%d after=%d (interval 100ms, ~1.5s idle)\n",
           base_groups, seen);

    moq_media_sender_destroy(s);
    moq_pq_threaded_stop(srv);
    moq_pq_threaded_destroy(srv);
    pthread_mutex_destroy(&g_ps.mu);

    MOQ_TEST_PASS(seen >= target ? "idle_refresh_managed_loopback"
                                 : "idle_refresh_managed_loopback_FAILED");
    printf("%s: %d failures\n", failures ? "FAIL" : "PASS", failures);
    return failures ? 1 : 0;
}
