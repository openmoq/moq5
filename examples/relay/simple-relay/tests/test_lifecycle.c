/* Real example + relay/session libraries, substituted transport only. No I/O. */
#define _POSIX_C_SOURCE 200809L
#define _DARWIN_C_SOURCE 1
#define _DEFAULT_SOURCE 1
#include <moq/relay/moqr_shards.h>
#include <moq/msquic_managed.h>
#include <stdlib.h>
static moqr_result_t step(moqr_shards_t *, uint16_t, uint64_t, uint64_t *);
#define moqr_shards_step_shard step
#define main example_main
#include "../main.c"
#undef main
#undef moqr_shards_step_shard

static unsigned checks, failures;
#define CHECK(x) do { ++checks; if (!(x)) { ++failures; \
    fprintf(stderr, "FAIL %d: %s\n", __LINE__, #x); } } while (0)

struct moq_msquic_managed { int unused; };
struct moq_msquic_managed_lane { unsigned index; };
struct moq_msquic_managed_conn {
    moq_session_t *session;
    void *tag;
    moq_version_t version;
    unsigned closes, acks;
};
static struct moq_msquic_managed fake_transport;
static struct moq_msquic_managed_lane fake_lane;
static struct moq_msquic_managed_conn child;
static moq_msquic_managed_cfg_t captured;
static bool visible, create_fail, wait_fail;
static moq_result_t ack_result, wake_result;
static unsigned creates, stops, destroys, wakes;
static uint64_t injected_wakes;
static bool step_fail;

static moqr_result_t step(moqr_shards_t *r, uint16_t i, uint64_t now, uint64_t *w)
{
    if (step_fail) return MOQR_ERR_NOMEM;
    moqr_result_t rc = moqr_shards_step_shard(r, i, now, w);
    *w |= injected_wakes;
    return rc;
}
void moq_msquic_managed_cfg_init_sized(moq_msquic_managed_cfg_t *cfg, size_t n)
{
    CHECK(n == sizeof(*cfg));
    memset(cfg, 0, n);
    cfg->struct_size = (uint32_t)n;
}
moq_result_t moq_msquic_managed_create(const moq_msquic_managed_cfg_t *cfg,
                                     moq_msquic_managed_t **out)
{
    ++creates;
    captured = *cfg;
    CHECK(cfg->alloc != NULL && cfg->perspective == MOQ_PERSPECTIVE_SERVER);
    CHECK(!strcmp(cfg->host, "127.0.0.1") && cfg->port == 0);
    CHECK(!strcmp(cfg->cert_path, "cert") && !strcmp(cfg->key_path, "key"));
    CHECK(cfg->version == 0 && cfg->version_count == 2);
    CHECK(cfg->versions[0] == MOQ_VERSION_DRAFT_18 && cfg->versions[1] == MOQ_VERSION_DRAFT_16);
    CHECK(cfg->lane_count == 1 && cfg->max_connections == 16);
    CHECK(cfg->on_lane_pump == pump && cfg->send_request_capacity);
    CHECK(cfg->initial_request_capacity == 1024);
    CHECK(!cfg->insecure_skip_verify);
    *out = create_fail ? NULL : &fake_transport;
    return create_fail ? MOQ_ERR_INTERNAL : MOQ_OK;
}
moq_result_t moq_msquic_managed_stop(moq_msquic_managed_t *m)
{
    CHECK(m == &fake_transport && destroys == 0);
    CHECK(moqr_shards_bind(((app_t *)captured.on_lane_pump_user)->relay, 0) != NULL);
    ++stops;
    return MOQ_OK;
}
void moq_msquic_managed_destroy(moq_msquic_managed_t *m)
{
    CHECK(m == &fake_transport && stops == 1);
    ++destroys;
}
moq_result_t moq_msquic_managed_wait(moq_msquic_managed_t *m, uint64_t us)
{
    CHECK(m == &fake_transport && us == 100000);
    CHECK(captured.on_lane_pump(m, &fake_lane, 123, captured.on_lane_pump_user) == 0);
    interrupted = 1;
    return wait_fail ? MOQ_ERR_INTERNAL : MOQ_OK;
}
uint16_t moq_msquic_managed_port(const moq_msquic_managed_t *m)
{ CHECK(m == &fake_transport); return 12345; }
uint32_t moq_msquic_lane_index(const moq_msquic_managed_lane_t *lane)
{ return lane->index; }
moq_msquic_managed_conn_t *moq_msquic_lane_next_conn(moq_msquic_managed_lane_t *lane,
                                                   moq_msquic_managed_conn_t *prev)
{ CHECK(lane == &fake_lane); return visible && prev == NULL ? &child : NULL; }
moq_session_t *moq_msquic_managed_conn_session(moq_msquic_managed_conn_t *c)
{ return c->session; }
void *moq_msquic_managed_conn_user(const moq_msquic_managed_conn_t *c)
{ return c->tag; }
void moq_msquic_managed_conn_set_user(moq_msquic_managed_conn_t *c, void *p)
{ c->tag = p; }
moq_version_t moq_msquic_managed_conn_negotiated_version(const moq_msquic_managed_conn_t *c)
{ return c->version; }
void moq_msquic_managed_conn_close(moq_msquic_managed_conn_t *c, uint64_t code)
{ CHECK(code == 0); ++c->closes; }
moq_result_t moq_msquic_managed_conn_ack_terminal(moq_msquic_managed_conn_t *c)
{
    CHECK(c->tag == &retired_tag);
    moq_event_t ev;
    size_t n = moq_session_poll_events(c->session, &ev, 1);
    CHECK(n == 0); /* Acknowledgment cannot precede draining detached events. */
    if (n) moq_event_cleanup(&ev);
    ++c->acks;
    return ack_result;
}
moq_result_t moq_msquic_lane_wake(moq_msquic_managed_lane_t *lane)
{ CHECK(lane == &fake_lane); ++wakes; return wake_result; }

static void pump_cases(moq_version_t version)
{
    app_t app = {0};
    atomic_init(&app.failed, false);
    moqr_shards_cfg_t cfg;
    moqr_shards_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default());
    cfg.live_visibility = true;
    CHECK(moqr_shards_create(&cfg, &app.relay) == MOQR_OK);
    moq_session_cfg_t sc;
    moq_session_cfg_init_sized(&sc, sizeof(sc), moq_alloc_default(), MOQ_PERSPECTIVE_SERVER);
    sc.version = version;
    memset(&child, 0, sizeof(child));
    CHECK(moq_session_create(&sc, 1, &child.session) == MOQ_OK);
    child.version = version;
    visible = true;
    ack_result = MOQ_ERR_WRONG_STATE;
    wake_result = MOQ_OK;
    moqr_bind_t *bind = moqr_shards_bind(app.relay, 0);
    CHECK(pump(&fake_transport, &fake_lane, 2, &app) == 0);
    CHECK(child.tag == &attached_tag && child.acks == 0 && child.closes == 0);
    CHECK(moqr_bind_conn_version(bind, child.session) == version);
    CHECK(pump(&fake_transport, &fake_lane, 3, &app) == 0); /* No duplicate attach. */
    CHECK(child.closes == 0);
    CHECK(moqr_bind_conn_close(bind, child.session) == MOQR_OK);
    CHECK(pump(&fake_transport, &fake_lane, 4, &app) == 0);
    CHECK(child.tag == &retired_tag && child.acks == 1);
    CHECK(!moqr_bind_conn_is_open(bind, child.session));
    CHECK(moq_session_on_transport_close(child.session, 0, 5) == MOQ_OK);
    ack_result = MOQ_OK;
    CHECK(pump(&fake_transport, &fake_lane, 6, &app) == 0);
    CHECK(child.acks == 2 && !moqr_bind_conn_is_open(bind, child.session));
    ack_result = MOQ_ERR_INTERNAL;
    CHECK(pump(&fake_transport, &fake_lane, 7, &app) == 1);
    CHECK(atomic_load(&app.failed));
    moq_session_destroy(child.session);
    CHECK(moq_session_create(&sc, 8, &child.session) == MOQ_OK);
    child.tag = NULL; /* A successor using the same handle address. */
    child.version = (moq_version_t)999;
    ack_result = MOQ_ERR_WRONG_STATE;
    CHECK(pump(&fake_transport, &fake_lane, 9, &app) == 0);
    CHECK(child.tag == &retired_tag && child.closes == 1);
    CHECK(pump(&fake_transport, &fake_lane, 10, &app) == 0);
    CHECK(child.closes == 1); /* A refused child is never attached again. */
    moq_session_destroy(child.session);
    visible = false;
    wakes = 0;
    injected_wakes = 1;
    CHECK(pump(&fake_transport, &fake_lane, 11, &app) == 0 && wakes == 1);
    wake_result = MOQ_ERR_CLOSED;
    CHECK(pump(&fake_transport, &fake_lane, 12, &app) == 1);
    injected_wakes = 2;
    CHECK(pump(&fake_transport, &fake_lane, 13, &app) == 1);
    injected_wakes = 0;
    step_fail = true;
    CHECK(pump(&fake_transport, &fake_lane, 14, &app) == 1);
    step_fail = false;
    fake_lane.index = 1;
    CHECK(pump(&fake_transport, &fake_lane, 15, &app) == 1);
    fake_lane.index = 0;
    moqr_shards_destroy(app.relay);
}

int main(void)
{
    const char *invalid[] = {"", "-1", "+1", " 1", "1x", "65536", "999999999999999"};
    for (size_t i = 0; i < sizeof(invalid) / sizeof(invalid[0]); ++i) {
        uint16_t port = 17;
        CHECK(!parse_port(invalid[i], &port) && port == 17);
    }
    uint16_t port;
    CHECK(parse_port("0", &port) && port == 0);
    CHECK(parse_port("65535", &port) && port == 65535);
    pump_cases(MOQ_VERSION_DRAFT_16);
    pump_cases(MOQ_VERSION_DRAFT_18);
    for (unsigned mode = 0; mode < 3; ++mode) {
        char *argv[] = {"relay", "cert", "key", "0", NULL};
        creates = stops = destroys = 0;
        create_fail = mode == 1;
        wait_fail = mode == 2;
        interrupted = 0;
        CHECK(example_main(4, argv) == (mode ? 1 : 0));
        CHECK(creates == 1 && stops == (create_fail ? 0u : 1u));
        CHECK(destroys == stops);
    }
    printf("simple relay lifecycle: %u checks, %u failures\n", checks, failures);
    return failures ? 1 : 0;
}
