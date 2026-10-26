/*
 * A bridge pass stops at its drain budget: the session can still hold
 * actions while the bridge holds nothing outbound and the endpoint accepted
 * every write. moq_session_has_pending_output reports that work, which the
 * bridge's own observer (write readiness) does not.
 *
 * Pinned, through two real bridges over fake endpoints, on drafts 16, 18
 * and 22: a publisher with one accepted subscription queues more subgroups
 * (open, object, FIN: three actions each) than one pass drains; one server
 * service pass leaves moq_transport_bridge_has_outbound_pending false and
 * moq_session_has_pending_output true; further passes drain it to false.
 */
#include <moq/moq.h>
#include <moq/transport_bridge.h>
#include "test_support.h"

#include <stdbool.h>

static int failures = 0;

#include "../support/bridge_wire_pair.h"
#include "test_binding_support.h"

enum { SUBGROUPS = 200 };   /* well past one pass's 256 actions */

/* The pair, as bridge_wire_pair's pair_open, with room for more actions
 * and open subgroups than one pass drains, and the draft-22 binding and
 * server start that pair_open does not do. */
/* Everything created so far, in reverse; safe on a partly opened pair. */
static void close_partial(rl_pair_t *p)
{
    if (p->cb) moq_transport_bridge_destroy(p->cb);
    if (p->sb) moq_transport_bridge_destroy(p->sb);
    if (p->c) moq_session_destroy(p->c);
    if (p->s) moq_session_destroy(p->s);
    memset(p, 0, sizeof(*p));
}

static bool open_roomy_pair(rl_pair_t *p, moq_version_t ver)
{
    memset(p, 0, sizeof(*p));
    p->ver = ver;
    p->now = 1000;
    p->client_control_sid = UINT64_MAX;
    p->server_control_uni = UINT64_MAX;
    p->calloc = test_allocator(&p->cas);
    p->salloc = test_allocator(&p->sas);
    moq_session_cfg_t cc, sc;
    moq_session_cfg_init_sized(&cc, sizeof(cc), &p->calloc, MOQ_PERSPECTIVE_CLIENT);
    moq_session_cfg_init_sized(&sc, sizeof(sc), &p->salloc, MOQ_PERSPECTIVE_SERVER);
    cc.version = sc.version = ver;
    cc.send_request_capacity = sc.send_request_capacity = true;
    cc.initial_request_capacity = sc.initial_request_capacity = 64;
    sc.max_actions = 2048;
    sc.max_open_subgroups = 512;
    if (moq_session_create(&cc, p->now, &p->c) < 0) return false;
    if (moq_session_create(&sc, p->now, &p->s) < 0) return false;
    fake_endpoint_init(&p->cep, 1000, 2000);
    fake_endpoint_init(&p->sep, 3000, 4000);
    p->cep.vtable.capabilities |= MOQ_TRANSPORT_CAP_HOLD_INPUT;
    p->sep.vtable.capabilities |= MOQ_TRANSPORT_CAP_HOLD_INPUT;
    moq_transport_bridge_cfg_t bc, sbc;
    moq_transport_bridge_cfg_init(&bc, &p->calloc);
    moq_transport_bridge_cfg_init(&sbc, &p->salloc);
    if (moq_transport_bridge_create(&bc, p->c, &p->cep.vtable, &p->cep, &p->cb) < 0) return false;
    if (moq_transport_bridge_create(&sbc, p->s, &p->sep.vtable, &p->sep, &p->sb) < 0) return false;
    if (test_bind_if_required(p->c) != MOQ_OK || test_bind_if_required(p->s) != MOQ_OK) return false;
    if (moq_session_start(p->c, p->now) < 0) return false;
    if (moq_session_uses_uni_control(p->s) && moq_session_start(p->s, p->now) < 0) return false;
    shuttle(p);
    obs_t ob;
    memset(&ob, 0, sizeof(ob));
    drain(p->c, &ob);
    drain(p->s, &ob);
    return moq_session_state(p->c) == MOQ_SESS_ESTABLISHED &&
           moq_session_state(p->s) == MOQ_SESS_ESTABLISHED;
}

static bool accepted_subscription(rl_pair_t *p, moq_subscription_t *ssub)
{
    moq_bytes_t nsp[1] = { MOQ_BYTES_LITERAL("live") };
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ nsp, 1 };
    sc.track_name = MOQ_BYTES_LITERAL("t");
    sc.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    moq_subscription_t csub;
    if (moq_session_subscribe(p->c, &sc, p->now, &csub) != MOQ_OK) return false;
    shuttle(p);
    moq_event_t ev;
    *ssub = MOQ_SUBSCRIPTION_INVALID;
    while (moq_session_poll_events(p->s, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) *ssub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
    }
    if (!moq_subscription_is_valid(*ssub)) return false;
    moq_accept_subscribe_cfg_t ac;
    moq_accept_subscribe_cfg_init(&ac);
    if (moq_session_accept_subscribe(p->s, *ssub, &ac, p->now) != MOQ_OK) return false;
    shuttle(p);
    obs_t ob;
    memset(&ob, 0, sizeof(ob));
    drain(p->c, &ob);
    return true;
}

static void run(moq_version_t ver)
{
    int before = failures;
    rl_pair_t p;
    moq_subscription_t ssub = MOQ_SUBSCRIPTION_INVALID;
    /* Preparation is a prerequisite, not evidence: a refusal fails the row
     * and unwinds what was created, never reaching the predicate checks. */
    if (!open_roomy_pair(&p, ver)) {
        MOQ_TEST_CHECK(!"pair opened");
        close_partial(&p);
        goto report;
    }
    if (!accepted_subscription(&p, &ssub)) {
        MOQ_TEST_CHECK(!"subscription accepted");
        pair_close(&p);
        goto report;
    }
    MOQ_TEST_CHECK(!moq_session_has_pending_output(p.s));

    static const uint8_t bytes[] = { 1, 2, 3, 4 };
    int queued = 0;
    for (int g = 0; g < SUBGROUPS; g++) {
        moq_subgroup_cfg_t cfg;
        moq_subgroup_cfg_init(&cfg);
        cfg.group_id = (uint64_t)g;
        moq_subgroup_handle_t sg;
        if (moq_session_open_subgroup(p.s, ssub, &cfg, p.now, &sg) != MOQ_OK) break;
        moq_rcbuf_t *b = NULL;
        MOQ_TEST_CHECK_EQ_INT(moq_rcbuf_create(moq_alloc_default(), bytes, sizeof(bytes), &b),
                              MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(moq_session_write_object(p.s, sg, 0, b, p.now), MOQ_OK);
        moq_rcbuf_decref(b);
        MOQ_TEST_CHECK_EQ_INT(moq_session_close_subgroup(p.s, sg, p.now), MOQ_OK);
        queued++;
    }
    MOQ_TEST_CHECK_EQ_INT(queued, SUBGROUPS);
    MOQ_TEST_CHECK(2048 - moq_session_action_capacity(p.s) > 256);  /* > one pass queued */
    MOQ_TEST_CHECK(moq_session_has_pending_output(p.s));

    /* The endpoint accepts every write; only its recorder is bounded, so
     * clear it per pass (nothing here is delivered to the client). */
    p.now += 10;
    MOQ_TEST_CHECK_EQ_INT(moq_transport_bridge_service(p.sb, p.now), MOQ_OK);
    fake_endpoint_clear_ops(&p.sep);
    /* The budget stopped the pass with session output still queued. */
    MOQ_TEST_CHECK(!moq_transport_bridge_has_outbound_pending(p.sb));
    MOQ_TEST_CHECK(moq_session_has_pending_output(p.s));

    for (int i = 0; i < 8 && moq_session_has_pending_output(p.s); i++) {
        p.now += 10;
        MOQ_TEST_CHECK_EQ_INT(moq_transport_bridge_service(p.sb, p.now), MOQ_OK);
        fake_endpoint_clear_ops(&p.sep);
    }
    MOQ_TEST_CHECK(!moq_session_has_pending_output(p.s));
    MOQ_TEST_CHECK(!moq_transport_bridge_has_outbound_pending(p.sb));

    pair_close(&p);
report:
    printf("%s: bridge budget pending output draft=%llu\n",
           failures == before ? "PASS" : "FAIL", (unsigned long long)ver);
}

int main(void)
{
    run(MOQ_VERSION_DRAFT_16);
    run(MOQ_VERSION_DRAFT_18);
    run(MOQ_VERSION_DRAFT_22);
    return failures ? 1 : 0;
}
