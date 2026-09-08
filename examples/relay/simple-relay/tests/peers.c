/* Real transport boundary only. Exact object contents, no performance claim. */
#define _POSIX_C_SOURCE 200809L
#define _DARWIN_C_SOURCE 1
#define _DEFAULT_SOURCE 1
#include <moq/msquic_managed.h>
#include <moq/rcbuf.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

typedef struct {
    bool publisher, started;
    atomic_bool announced, failed, finished;
    unsigned received;
} peer_t;

static const uint8_t payload[] = {0, 1, 2, 0xff, 'r', 'e', 'l', 'a', 'y'};
static uint64_t monotonic_us(void)
{
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) abort();
    return (uint64_t)ts.tv_sec * 1000000 + (uint64_t)ts.tv_nsec / 1000;
}

static int on_pump(moq_msquic_managed_t *m, moq_msquic_managed_lane_t *lane,
                   uint64_t now, void *user)
{
    peer_t *p = user;
    (void)lane;
    moq_session_t *s = moq_msquic_managed_session(m);
    if (s == NULL) return 0;
    if (!p->started && moq_session_state(s) == MOQ_SESS_ESTABLISHED) {
        moq_bytes_t part = MOQ_BYTES_LITERAL("simple-relay-test");
        moq_namespace_t ns = {.parts = &part, .count = 1};
        moq_result_t rc;
        if (p->publisher) {
            moq_publish_namespace_cfg_t cfg;
            moq_publish_namespace_cfg_init(&cfg);
            cfg.track_namespace = ns;
            moq_announcement_t ann;
            rc = moq_session_publish_namespace(s, &cfg, now, &ann);
        } else {
            moq_subscribe_cfg_t cfg;
            moq_subscribe_cfg_init(&cfg);
            cfg.track_namespace = ns;
            cfg.track_name = MOQ_BYTES_LITERAL("bytes");
            cfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
            moq_subscription_t sub;
            rc = moq_session_subscribe(s, &cfg, now, &sub);
        }
        p->started = true;
        if (rc != MOQ_OK) {
            fprintf(stderr, "peer %s initial request: %d\n", p->publisher ? "pub" : "sub", (int)rc);
            atomic_store(&p->failed, true);
        }
    }
    moq_event_t ev[16];
    size_t n;
    while ((n = moq_session_poll_events(s, ev, 16)) != 0) {
        for (size_t i = 0; i < n; ++i) {
            if (ev[i].kind == MOQ_EVENT_NAMESPACE_ACCEPTED) {
                atomic_store(&p->announced, true);
            } else if (ev[i].kind == MOQ_EVENT_SUBSCRIBE_REQUEST && p->publisher) {
                moq_accept_subscribe_cfg_t accept;
                moq_accept_subscribe_cfg_init(&accept);
                moq_subgroup_cfg_t cfg;
                moq_subgroup_cfg_init(&cfg);
                cfg.group_id = 7;
                moq_subgroup_handle_t sg;
                moq_rcbuf_t *buf = NULL;
                moq_subscription_t sub = ev[i].u.subscribe_request.sub;
                bool ok = moq_session_accept_subscribe(s, sub, &accept, now) == MOQ_OK &&
                          moq_session_open_subgroup(s, sub, &cfg, now, &sg) == MOQ_OK &&
                          moq_rcbuf_create(moq_alloc_default(), payload, sizeof(payload), &buf) == MOQ_OK;
                for (uint64_t j = 0; ok && j < 3; ++j) {
                    ok = moq_session_write_object(s, sg, j, buf, now) == MOQ_OK;
                }
                if (ok) ok = moq_session_close_subgroup(s, sg, now) == MOQ_OK;
                moq_rcbuf_decref(buf);
                if (!ok) {
                    fputs("publisher accept/write failed\n", stderr);
                    atomic_store(&p->failed, true);
                }
            } else if (ev[i].kind == MOQ_EVENT_OBJECT_RECEIVED && !p->publisher) {
                const moq_object_received_event_t *ob = &ev[i].u.object_received;
                if (ob->group_id != 7 || ob->object_id != p->received ||
                    ob->datagram || ob->payload == NULL ||
                    moq_rcbuf_len(ob->payload) != sizeof(payload) ||
                    memcmp(moq_rcbuf_data(ob->payload), payload, sizeof(payload)) != 0) {
                    atomic_store(&p->failed, true);
                }
                ++p->received;
            } else if (ev[i].kind == MOQ_EVENT_SUBGROUP_FINISHED && !p->publisher) {
                if (p->received != 3 || ev[i].u.subgroup_finished.group_id != 7) {
                    atomic_store(&p->failed, true);
                }
                atomic_store(&p->finished, true);
            } else if (ev[i].kind == MOQ_EVENT_SESSION_CLOSED ||
                       ev[i].kind == MOQ_EVENT_SUBSCRIBE_ERROR ||
                       ev[i].kind == MOQ_EVENT_NAMESPACE_REJECTED ||
                       ev[i].kind == MOQ_EVENT_NAMESPACE_CANCELLED) {
                fprintf(stderr, "peer %s rejected/closed event %u\n",
                        p->publisher ? "pub" : "sub", (unsigned)ev[i].kind);
                if (ev[i].kind == MOQ_EVENT_SESSION_CLOSED) {
                    fprintf(stderr, "close code=%llu reason=%.*s\n",
                            (unsigned long long)ev[i].u.closed.code,
                            (int)ev[i].u.closed.reason.len,
                            ev[i].u.closed.reason.data ? (const char *)ev[i].u.closed.reason.data : "");
                }
                atomic_store(&p->failed, true);
            }
            moq_event_cleanup(&ev[i]);
        }
    }
    return 0;
}

static bool await_flag(moq_msquic_managed_t *m, peer_t *p, atomic_bool *flag,
                       uint64_t deadline)
{
    while (!atomic_load(flag) && !atomic_load(&p->failed)) {
        uint64_t now = monotonic_us();
        if (now >= deadline) { fputs("peer deadline\n", stderr); return false; }
        uint64_t remaining = deadline - now;
        /* Boundary watchdog only; session work is performed by adapter wakes. */
        moq_result_t rc = moq_msquic_managed_wait(m, remaining < 100000 ? remaining : 100000);
        if (rc != MOQ_OK && rc != MOQ_DONE) {
            fprintf(stderr, "peer wait: %d fatal=%d code=%llu\n", (int)rc,
                    (int)moq_msquic_managed_is_fatal(m),
                    (unsigned long long)moq_msquic_managed_fatal_code(m));
            return false;
        }
    }
    return !atomic_load(&p->failed);
}

static moq_result_t connect_peer(peer_t *p, uint16_t port, moq_version_t version,
                                 moq_msquic_managed_t **out)
{
    moq_msquic_managed_cfg_t cfg;
    moq_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.host = "127.0.0.1";
    cfg.port = port;
    cfg.version = version;
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 1024;
    /* Test-only committed self-signed certificate; not an authentication test. */
    cfg.insecure_skip_verify = true;
    cfg.on_lane_pump = on_pump;
    cfg.on_lane_pump_user = p;
    return moq_msquic_managed_create(&cfg, out);
}

int main(int argc, char **argv)
{
    if (argc != 4) return 2;
    char *end;
    unsigned long port = strtoul(argv[1], &end, 10);
    if (*end || !port || port > 65535) return 2;
    moq_version_t versions[2];
    for (int i = 0; i < 2; ++i) {
        if (!strcmp(argv[i + 2], "16")) versions[i] = MOQ_VERSION_DRAFT_16;
        else if (!strcmp(argv[i + 2], "18")) versions[i] = MOQ_VERSION_DRAFT_18;
        else return 2;
    }
    peer_t pub = {.publisher = true}, sub = {0};
    atomic_init(&pub.announced, false); atomic_init(&sub.announced, false);
    atomic_init(&pub.failed, false); atomic_init(&sub.failed, false);
    atomic_init(&pub.finished, false); atomic_init(&sub.finished, false);
    moq_msquic_managed_t *p = NULL, *s = NULL;
    uint64_t deadline = monotonic_us() + 15000000;
    bool ok = connect_peer(&pub, (uint16_t)port, versions[0], &p) == MOQ_OK;
    if (ok) ok = await_flag(p, &pub, &pub.announced, deadline);
    if (ok) ok = connect_peer(&sub, (uint16_t)port, versions[1], &s) == MOQ_OK;
    if (ok) ok = await_flag(s, &sub, &sub.finished, deadline);
    if (s) {
        if (moq_msquic_managed_stop(s) != MOQ_OK) ok = false;
        moq_msquic_managed_destroy(s);
    }
    if (p) {
        if (moq_msquic_managed_stop(p) != MOQ_OK) ok = false;
        moq_msquic_managed_destroy(p);
    }
    /* Pump-owned plain counters can only be read after both lanes join. */
    ok = ok && !atomic_load(&pub.failed) && !atomic_load(&sub.failed) && sub.received == 3;
    printf("%s: %s -> %s, %u/3 exact objects and subgroup FIN\n",
           ok ? "PASS" : "FAIL", argv[2], argv[3], sub.received);
    return ok ? 0 : 1;
}
