/* Client sessions back service attachments until managed destroy, even after
 * stop or terminal reclamation. Existing test seams synthesize child state;
 * real construction, pump, reap, stop and destroy own all lifetime decisions.
 * No socket, credential, worker thread, sleep or transport timing is used. */
#include "msquic_internal.h"
#include "support/fake_msq_table.h"
#include "support/msq_test_seams.h"
#include <moq/msquic_managed.h>
#include <moq/session.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

extern bool moq_msq_test_no_doorbell;
extern moq_result_t moq_msq_test_managed_create_lanes_only_api(
    const moq_msquic_managed_cfg_t *, const QUIC_API_TABLE *,
    moq_msquic_managed_t **);
extern bool moq_msq_test_lane_inject_terminal_child(
    moq_msquic_managed_lane_t *, uint64_t);
extern bool moq_msq_test_lane_inject_idle_child(moq_msquic_managed_lane_t *);

static int failures;
static const char *arm;
#define CHECK(e) do { if (!(e)) { \
    fprintf(stderr, "FAIL[%s]: %s:%d: %s\n", arm, __FILE__, __LINE__, #e); \
    failures++; } } while (0)

struct fixture {
    moq_alloc_t alloc;
    void *session;
    size_t live;
    unsigned session_frees;
    unsigned pumps;
    unsigned closed_events;
    bool server;
    bool acknowledge;
};

static void *a_alloc(size_t n, void *ctx)
{
    struct fixture *f = ctx;
    void *p = malloc(n);
    if (p != NULL) f->live++;
    return p;
}

static void a_free(void *p, size_t n, void *ctx)
{
    struct fixture *f = ctx;
    (void)n;
    if (p == NULL) return;
    if (p == f->session) f->session_frees++;
    CHECK(f->live > 0);
    f->live--;
    free(p);
}

static void *a_realloc(void *p, size_t old, size_t n, void *ctx)
{
    struct fixture *f = ctx;
    (void)old;
    if (p == NULL) return a_alloc(n, ctx);
    if (n == 0) { a_free(p, old, ctx); return NULL; }
    CHECK(p != f->session);
    return realloc(p, n);
}

static int pump(moq_msquic_managed_t *m, moq_msquic_managed_lane_t *lane,
                uint64_t now_us, void *ctx)
{
    struct fixture *f = ctx;
    (void)now_us;
    f->pumps++;
    moq_msquic_managed_conn_t *c = moq_msquic_lane_next_conn(lane, NULL);
    if (c == NULL) return 0;
    CHECK(moq_msquic_lane_next_conn(lane, c) == NULL);
    moq_session_t *s = moq_msquic_managed_conn_session(c);
    CHECK(s != NULL);
    if (s == NULL) return 1;
    if (f->session == NULL) f->session = s;
    CHECK(f->session == s);
    CHECK(f->session_frees == 0);
    if (!f->server) CHECK(moq_msquic_managed_session(m) == s);
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SESSION_CLOSED) f->closed_events++;
        moq_event_cleanup(&ev);
    }
    if (f->server && f->acknowledge)
        CHECK(moq_msquic_managed_conn_ack_terminal(c) == MOQ_OK);
    return 0;
}

static void run(moq_version_t version, bool server, bool terminal,
                bool acknowledge)
{
    char label[96];
    (void)snprintf(label, sizeof(label), "d%d-%s-%s%s", (int)version,
                   server ? "server" : "client", terminal ? "reap" : "stop",
                   acknowledge ? "-ack" : "");
    arm = label;
    int before = failures;
    struct fixture f = {0};
    f.server = server;
    f.acknowledge = acknowledge;
    f.alloc = (moq_alloc_t){.alloc = a_alloc, .realloc = a_realloc,
                           .free = a_free, .ctx = &f};
    fake_msq_t *fake = calloc(1, sizeof(*fake));
    CHECK(fake != NULL);
    if (fake == NULL) return;
    fake_msq_init(fake, !server);
    moq_msquic_managed_cfg_t cfg;
    moq_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = &f.alloc;
    cfg.perspective = server ? MOQ_PERSPECTIVE_SERVER : MOQ_PERSPECTIVE_CLIENT;
    cfg.version = version;
    cfg.on_lane_pump = pump;
    cfg.on_lane_pump_user = &f;
    moq_msquic_managed_t *m = NULL;
    CHECK(moq_msq_test_managed_create_lanes_only_api(&cfg,
              fake_msq_table(fake), &m) == MOQ_OK);
    if (m == NULL) { free(fake); CHECK(f.live == 0); return; }
    moq_msquic_managed_lane_t *lane = moq_msquic_managed_lane(m, 0);
    CHECK(lane != NULL);
    bool injected = terminal
        ? moq_msq_test_lane_inject_terminal_child(lane, 0x11)
        : moq_msq_test_lane_inject_idle_child(lane);
    CHECK(injected);
    if (!injected) goto done;
    CHECK(moq_msquic_lane_wake(lane) == MOQ_OK);
    CHECK(moq_msq_test_lane_step(lane) == MOQ_MSQ_TEST_STEP_PUMPED);
    CHECK(f.pumps == 1);
    CHECK(f.session != NULL);
    CHECK(f.closed_events == (terminal ? 1u : 0u));
    unsigned freed_after_step = server && terminal && acknowledge ? 1u : 0u;
    CHECK(f.session_frees == freed_after_step);
    moq_msq_test_lane_row_t lr;
    size_t linked = moq_msq_test_lane_snapshot(lane, &lr, NULL, 0);
    CHECK(linked == ((terminal && (!server || acknowledge)) ? 0u : 1u));
    CHECK(lr.conn_count == linked);
    CHECK(moq_msquic_managed_session(m) == NULL); /* remains pump-scoped */
    CHECK(moq_msquic_managed_stop(m) == MOQ_OK);
    CHECK(f.session_frees == (server ? 1u : 0u));
    CHECK(moq_msquic_managed_stop(m) == MOQ_OK);
    CHECK(f.session_frees == (server ? 1u : 0u));
    CHECK(moq_msq_test_lane_snapshot(lane, &lr, NULL, 0) == 0);
done:
    moq_msquic_managed_destroy(m);
    if (injected) CHECK(f.session_frees == 1);
    CHECK(f.live == 0);
    free(fake);
    if (failures == before) printf("PASS[%s]\n", label);
}

int main(void)
{
    moq_msq_test_no_doorbell = true;
    const moq_version_t versions[] = {MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18};
    for (size_t i = 0; i < sizeof(versions) / sizeof(versions[0]); i++) {
        run(versions[i], false, false, false);
        run(versions[i], false, true, false);
        run(versions[i], true, false, false);
        run(versions[i], true, true, false);
        run(versions[i], true, true, true);
    }
    moq_msq_test_no_doorbell = false;
    return failures ? 1 : 0;
}
