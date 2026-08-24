/*
 * Push-mode catalog-republish CURSOR test (dedicated internals-linked
 * target). Narrowly scoped loopback: a push-only consumer (accepts every
 * PUBLISH, never subscribes) and the private MOQ_MEDIA_SENDER_TESTING fault
 * seam forcing one WOULD_BLOCK before object 1 of a delta generation. Kept
 * OUT of test_media_sender so the production network suite links the
 * shipping moq::service; only this binary links the test internals.
 */
#include <moq/media_sender.h>
#include <moq/endpoint.h>
#include <moq/session.h>
#include <moq/picoquic_threaded.h>
#include <moq/msf.h>
#include <moq/rcbuf.h>
#include "test_support.h"

#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

static int failures = 0;

/* Private fault seams (media_sender.c, MOQ_MEDIA_SENDER_TESTING). */
void moq_media_sender_test_block_republish_before(moq_media_sender_t *s,
                                                  uint64_t object_id);
bool moq_media_sender_test_block_republish_state(moq_media_sender_t *s,
                                                 size_t *cursor,
                                                 size_t *count,
                                                 bool *retained_set);
void moq_media_sender_test_republish_live(moq_media_sender_t *s,
                                          size_t *cursor, size_t *count,
                                          unsigned *retained_installs);

/* -- Minimal push-only server ---------------------------------------- */

#define CAP_MAX 64
typedef struct {
    pthread_mutex_t   mu;
    atomic_bool       ns_accepted;
    bool              pub_cat_ok;
    moq_publication_t pub_cat;
    int               n;
    struct { uint64_t group, object, seq; } objs[CAP_MAX];
    uint64_t          seq;
} push_srv_t;

static push_srv_t g_ps;

static int psrv_pump(moq_pq_threaded_t *t, moq_pq_threaded_lane_t *lane,
                     uint64_t now_us, void *ctx)
{
    (void)lane;
    push_srv_t *ps = (push_srv_t *)ctx;
    moq_session_t *session = moq_pq_threaded_session(t);
    if (!session) return 0;
    if (moq_session_state(session) != MOQ_SESS_ESTABLISHED) return 0;

    moq_event_t ev;
    while (moq_session_poll_events(session, &ev, 1) == 1) {
        if (ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED) {
            moq_accept_namespace_cfg_t acc;
            moq_accept_namespace_cfg_init(&acc);
            moq_session_accept_namespace(session,
                ev.u.namespace_published.ann, &acc, now_us);
            atomic_store(&ps->ns_accepted, true);
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
                ps->n < CAP_MAX) {
                ps->objs[ps->n].group = o->group_id;
                ps->objs[ps->n].object = o->object_id;
                ps->objs[ps->n].seq = ps->seq++;
                ps->n++;
            }
            pthread_mutex_unlock(&ps->mu);
        }
        moq_event_cleanup(&ev);
    }
    return 0;
}

static moq_pq_threaded_t *psrv_start(const char *cert, const char *key,
                                     int *out_port)
{
    /* Port range disjoint from the other service tests. */
    int base = 19900 + (int)(getpid() % 997);
    for (int attempt = 0; attempt < 8; attempt++) {
        int port = base + attempt * 17;
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

static bool wait_ready(moq_media_sender_t *s, int ticks)
{
    for (int i = 0; i < ticks; i++) {
        if (moq_media_sender_is_ready(s)) return true;
        usleep(20000);
    }
    return moq_media_sender_is_ready(s);
}

static int count_groups(void)
{
    pthread_mutex_lock(&g_ps.mu);
    int n = 0;
    for (int i = 0; i < g_ps.n; i++) {
        bool seen = false;
        for (int j = 0; j < i; j++)
            if (g_ps.objs[j].group == g_ps.objs[i].group) { seen = true; break; }
        if (!seen) n++;
    }
    pthread_mutex_unlock(&g_ps.mu);
    return n;
}

static int wait_gens(int want, int ticks)
{
    for (int i = 0; i < ticks; i++) {
        if (count_groups() >= want) return count_groups();
        usleep(20000);
    }
    return count_groups();
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
    moq_media_sender_t *s = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_create(&cfg, &s),
                          (int)MOQ_OK);
    moq_media_track_t *v = NULL;
    {
        moq_media_track_cfg_t tc;
        moq_media_track_cfg_init(&tc);
        tc.name = (moq_bytes_t){ (const uint8_t *)"v", 1 };
        tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
        tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
        tc.codec = (moq_bytes_t){ (const uint8_t *)"av01", 4 };
        tc.bitrate = 1500000;
        tc.is_live = true;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_media_sender_add_track(s, &tc, &v), (int)MOQ_OK);
    }
    MOQ_TEST_CHECK(wait_ready(s, 400));
    MOQ_TEST_CHECK(wait_gens(1, 400) >= 1);

    /* Arm the one-shot fault, then trigger the ADD generation (independent
     * base object 0 + deltaUpdate object 1). The handle stays in scope: its
     * REMOVE later forces generation 3 as a real barrier. */
    moq_media_sender_test_block_republish_before(s, 1);
    moq_media_track_t *a3 = NULL;
    {
        moq_media_track_cfg_t tc;
        moq_media_track_cfg_init(&tc);
        tc.name = (moq_bytes_t){ (const uint8_t *)"a3", 2 };
        tc.media_type = MOQ_MEDIA_TYPE_AUDIO;
        tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
        tc.codec = (moq_bytes_t){ (const uint8_t *)"opus", 4 };
        tc.bitrate = 128000;
        tc.samplerate = 48000;
        tc.channel_config = (moq_bytes_t){ (const uint8_t *)"2", 1 };
        tc.is_live = true;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_media_sender_add_track(s, &tc, &a3), (int)MOQ_OK);
    }

    /* The fault fires on the pump's first republish pass: object 0 written,
     * cursor parked at 1, generation uncommitted, retained not installed. */
    size_t cur = 0, cnt = 0; bool retained = true;
    bool fired = false;
    for (int i = 0; i < 200 && !fired; i++) {
        fired = moq_media_sender_test_block_republish_state(
            s, &cur, &cnt, &retained);
        if (!fired) usleep(20000);
    }
    MOQ_TEST_CHECK(fired);
    MOQ_TEST_CHECK_EQ_INT((int)cur, 1);
    MOQ_TEST_CHECK(cnt >= 2);
    MOQ_TEST_CHECK(!retained);

    /* The pump's natural retry completes the generation: object 0 exactly
     * once, object 1 exactly once, 0 strictly before 1. */
    MOQ_TEST_CHECK(wait_gens(2, 400) >= 2);
    uint64_t gen1 = 0;
    {
        bool done = false;
        for (int i = 0; i < 200 && !done; i++) {
            pthread_mutex_lock(&g_ps.mu);
            uint64_t gmax = 0;
            for (int j = 0; j < g_ps.n; j++)
                if (g_ps.objs[j].group > gmax) gmax = g_ps.objs[j].group;
            int n0 = 0, n1 = 0;
            uint64_t seq0 = 0, seq1 = 0;
            for (int j = 0; j < g_ps.n; j++)
                if (g_ps.objs[j].group == gmax) {
                    if (g_ps.objs[j].object == 0) { n0++; seq0 = g_ps.objs[j].seq; }
                    if (g_ps.objs[j].object == 1) { n1++; seq1 = g_ps.objs[j].seq; }
                }
            MOQ_TEST_CHECK(n0 <= 1);
            MOQ_TEST_CHECK(n1 <= 1);
            done = (gmax > 0 && n0 == 1 && n1 == 1);
            if (done) { MOQ_TEST_CHECK(seq0 < seq1); gen1 = gmax; }
            pthread_mutex_unlock(&g_ps.mu);
            if (!done) usleep(20000);
        }
        MOQ_TEST_CHECK(done);
    }

    /* Monotonic-mode legality: the sender declares monotonic_groups on the
     * catalog track, and this generation carried MULTIPLE objects (base 0 +
     * delta 1) in ONE group, resumed across a mid-generation retry -- the
     * A-enforcement must not have rejected the same-group continuation
     * (which would fatal the sender or strand the generation). */
    MOQ_TEST_CHECK(!moq_media_sender_is_fatal(s));

    /* FINAL-STATE proof: generation 2 actually committed -- pending count
     * AND cursor cleared, and the retained-group installs are exactly one
     * per generation (gen 0's initial install + this generation's). */
    {
        size_t lcur = 99, lcnt = 99; unsigned installs = 0;
        bool committed = false;
        for (int i = 0; i < 200 && !committed; i++) {
            moq_media_sender_test_republish_live(s, &lcur, &lcnt, &installs);
            committed = (lcnt == 0 && installs >= 2);
            if (!committed) usleep(20000);
        }
        MOQ_TEST_CHECK(committed);
        MOQ_TEST_CHECK_EQ_INT((int)lcnt, 0);
        MOQ_TEST_CHECK_EQ_INT((int)lcur, 0);
        MOQ_TEST_CHECK_EQ_INT((int)installs, 2);   /* exactly once per gen */
    }

    /* Stability across a REAL third-generation barrier: removing a3 forces
     * generation 3; once it is observed and committed, gen1's wire counts
     * must be unchanged -- nothing re-emitted after commit. */
    {
        pthread_mutex_lock(&g_ps.mu);
        int before0 = 0, before1 = 0;
        for (int j = 0; j < g_ps.n; j++)
            if (g_ps.objs[j].group == gen1) {
                if (g_ps.objs[j].object == 0) before0++;
                if (g_ps.objs[j].object == 1) before1++;
            }
        pthread_mutex_unlock(&g_ps.mu);
        MOQ_TEST_CHECK(before0 == 1 && before1 == 1);
    }
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_remove_track(s, a3),
                          (int)MOQ_OK);
    MOQ_TEST_CHECK(wait_gens(3, 400) >= 3);   /* the barrier is REAL now */
    {
        size_t lcur = 99, lcnt = 99; unsigned installs = 0;
        bool committed = false;
        for (int i = 0; i < 200 && !committed; i++) {
            moq_media_sender_test_republish_live(s, &lcur, &lcnt, &installs);
            committed = (lcnt == 0 && lcur == 0 && installs >= 3);
            if (!committed) usleep(20000);
        }
        MOQ_TEST_CHECK(committed);
        MOQ_TEST_CHECK_EQ_INT((int)installs, 3);   /* one per generation */
    }
    {
        pthread_mutex_lock(&g_ps.mu);
        int after0 = 0, after1 = 0;
        for (int j = 0; j < g_ps.n; j++)
            if (g_ps.objs[j].group == gen1) {
                if (g_ps.objs[j].object == 0) after0++;
                if (g_ps.objs[j].object == 1) after1++;
            }
        pthread_mutex_unlock(&g_ps.mu);
        MOQ_TEST_CHECK(after0 == 1 && after1 == 1);
    }

    moq_media_sender_destroy(s);
    moq_pq_threaded_stop(srv);
    moq_pq_threaded_destroy(srv);
    MOQ_TEST_PASS("push_catalog_republish_cursor_would_block");
    printf("%s: %d failures\n", failures ? "FAIL" : "PASS", failures);
    return failures ? 1 : 0;
}
