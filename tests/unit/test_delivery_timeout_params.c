/*
 * Delivery timeouts on every message each draft allows them in, as the
 * session lets an application set them:
 *
 *   - draft-16 9.2.2.2: one DELIVERY TIMEOUT parameter that "MAY appear in a
 *     PUBLISH_OK, SUBSCRIBE, or REQUEST_UPDATE message" and "MUST contain a
 *     value greater than 0"; 11.1: the publisher's as a Track Extension.
 *   - draft-18 10.2.3 and 10.2.4: SUBGROUP_ and OBJECT_DELIVERY_TIMEOUT
 *     "MAY appear in a PUBLISH_OK, SUBSCRIBE, or REQUEST_UPDATE message";
 *     12.1 and 12.2: the publisher's as Track Properties; 8: for each type
 *     the smaller non-zero of the two sides applies.
 *   - draft-22: a PUBLISH is answered with a REQUEST_OK that carries none of
 *     the subscriber's choices; they follow in a REQUEST_UPDATE.
 *
 * A timeout the subscriber asks for is checked where draft-18 8 makes it
 * bite: an object older than its OBJECT_DELIVERY_TIMEOUT is never handed to
 * the transport and its stream is reset with DELIVERY_TIMEOUT.
 */
#include <moq/moq.h>
#include <moq/sim.h>
#include "test_support.h"
#include "../../core/src/session/session_internal.h"
#include <string.h>

static int failures;

static const uint64_t MS = 1000;

static const moq_version_t k_versions[] = {
    MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18, MOQ_VERSION_DRAFT_22,
};
#define N_VERSIONS (sizeof(k_versions) / sizeof(k_versions[0]))

static int draft_of(moq_version_t v)
{
    return v == MOQ_VERSION_DRAFT_16 ? 16 : v == MOQ_VERSION_DRAFT_18 ? 18 : 22;
}

static void drain_events(moq_session_t *s)
{
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0) moq_event_cleanup(&ev);
}

static void drain_actions(moq_session_t *s)
{
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
}

static bool take(moq_session_t *s, moq_event_kind_t kind, moq_event_t *out)
{
    moq_event_t ev;
    bool found = false;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        if (!found && ev.kind == kind) { *out = ev; found = true; continue; }
        moq_event_cleanup(&ev);
    }
    return found;
}

/* The client subscribes or accepts; the server publishes, except for a
 * PUBLISH, which the client sends. */
typedef struct fx {
    moq_simpair_t *sp;
    moq_session_t *c;
    moq_session_t *sv;
} fx_t;

static bool fx_open(fx_t *fx, moq_version_t v)
{
    memset(fx, 0, sizeof(*fx));
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = moq_alloc_default();
    cfg.version = v;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 16;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = 16;
    if (moq_simpair_create(&cfg, &fx->sp) < 0) return false;
    fx->c = moq_simpair_client(fx->sp);
    fx->sv = moq_simpair_server(fx->sp);
    if (moq_simpair_start(fx->sp) < 0) return false;
    moq_simpair_run_until_quiescent(fx->sp, 16, NULL);
    drain_events(fx->c);
    drain_events(fx->sv);
    return moq_session_state(fx->c) == MOQ_SESS_ESTABLISHED;
}

static void fx_close(fx_t *fx)
{
    if (fx->sp) moq_simpair_destroy(fx->sp);
    memset(fx, 0, sizeof(*fx));
}

static void run(fx_t *fx)
{
    moq_simpair_run_until_quiescent(fx->sp, 16, NULL);
}

static moq_bytes_t g_part;   /* "live", set by main */

static moq_rcbuf_t *payload(void)
{
    static const uint8_t body[] = "object";
    moq_rcbuf_t *b = NULL;
    moq_rcbuf_create(moq_alloc_default(), body, sizeof(body), &b);
    return b;
}

/* `writer` writes one object on `sg` at `at`; by `at` + 51 ms the 50 ms
 * object timeout has run out. The object never goes out, and its stream is
 * reset with DELIVERY_TIMEOUT. */
static void expect_object_times_out(moq_session_t *writer,
                                    moq_subgroup_handle_t sg, uint64_t at,
                                    const char *what, moq_version_t v)
{
    moq_stream_ref_t ref = moq_stream_ref_from_u64(0);
    moq_action_t a;
    while (moq_session_poll_actions(writer, &a, 1) > 0) {
        if (a.kind == MOQ_ACTION_SEND_DATA && ref._v == 0)
            ref = a.u.send_data.stream_ref;
        moq_action_cleanup(&a);
    }
    moq_rcbuf_t *p = payload();
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_write_object(writer, sg, 0, p, at),
                          MOQ_OK);
    moq_rcbuf_decref(p);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_tick(writer, at + 51 * MS), MOQ_OK);
    int sent = 0, resets = 0;
    while (moq_session_poll_actions(writer, &a, 1) > 0) {
        if (a.kind == MOQ_ACTION_SEND_DATA && a.u.send_data.payload) sent++;
        if (a.kind == MOQ_ACTION_RESET_DATA &&
            a.u.reset_data.stream_ref._v == ref._v &&
            a.u.reset_data.error_code == MOQ_RESET_DELIVERY_TIMEOUT)
            resets++;
        moq_action_cleanup(&a);
    }
    if (sent != 0 || resets != 1)
        fprintf(stderr, "%s (d%d): %d sent, %d timeout resets\n", what,
                draft_of(v), sent, resets);
    MOQ_TEST_CHECK_EQ_INT(sent, 0);
    MOQ_TEST_CHECK_EQ_INT(resets, 1);
}

/* The subscriber's timeouts for draft `v`: 50 ms per object, and on drafts
 * with two types 70 ms per subgroup. Draft-16's one value stands for both. */
static void dt_select(moq_version_t v, uint32_t *flags, uint64_t *object_ms,
                      uint64_t *subgroup_ms)
{
    *object_ms = 50;
    if (v == MOQ_VERSION_DRAFT_16) {
        *flags = 1;
        *subgroup_ms = 0;
    } else {
        *flags = 3;
        *subgroup_ms = 70;
    }
}

/* What the publisher holds: draft-16 reads its one value into both types. */
static void expect_dt(bool has_object, uint64_t object_ms, bool has_subgroup,
                      uint64_t subgroup_ms, moq_version_t v, uint64_t want_object,
                      uint64_t want_subgroup)
{
    MOQ_TEST_CHECK(has_object);
    MOQ_TEST_CHECK_EQ_U64(object_ms, want_object);
    MOQ_TEST_CHECK(has_subgroup);
    MOQ_TEST_CHECK_EQ_U64(subgroup_ms,
                          v == MOQ_VERSION_DRAFT_16 ? want_object : want_subgroup);
}

static bool subscribe_accept(fx_t *fx, const moq_subscribe_cfg_t *cfg,
                             const uint8_t *props, size_t props_len,
                             moq_subscription_t *pub_side,
                             moq_subscription_t *sub_side)
{
    if (moq_session_subscribe(fx->c, cfg, moq_simpair_now_us(fx->sp),
                              sub_side) < 0)
        return false;
    run(fx);
    moq_event_t ev;
    if (!take(fx->sv, MOQ_EVENT_SUBSCRIBE_REQUEST, &ev)) return false;
    *pub_side = ev.u.subscribe_request.sub;
    moq_event_cleanup(&ev);
    moq_accept_subscribe_cfg_t ac;
    moq_accept_subscribe_cfg_init(&ac);
    ac.track_properties = (moq_bytes_t){ props, props_len };
    if (moq_session_accept_subscribe(fx->sv, *pub_side, &ac,
                                     moq_simpair_now_us(fx->sp)) < 0)
        return false;
    run(fx);
    drain_events(fx->c);
    drain_events(fx->sv);
    drain_actions(fx->sv);
    return true;
}

static void subscribe_cfg(moq_subscribe_cfg_t *cfg, const char *name)
{
    moq_subscribe_cfg_init(cfg);
    cfg->track_namespace = (moq_namespace_t){ &g_part, 1 };
    cfg->track_name = (moq_bytes_t){ (const uint8_t *)name, strlen(name) };
}

static moq_subgroup_handle_t open_sg(moq_session_t *s, moq_subscription_t sub,
                                     uint64_t now)
{
    moq_subgroup_cfg_t cfg;
    moq_subgroup_cfg_init(&cfg);
    moq_subgroup_handle_t sg = MOQ_SUBGROUP_INVALID;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_subgroup(s, sub, &cfg, now, &sg),
                          MOQ_OK);
    return sg;
}

/* [DT-P1] SUBSCRIBE carries the subscriber's timeouts on every draft; the
 * publisher holds them and enforces the object one. */
static void test_subscribe(moq_version_t v)
{
    fx_t fx;
    if (!fx_open(&fx, v)) { MOQ_TEST_CHECK(false); fx_close(&fx); return; }
    moq_subscribe_cfg_t cfg;
    subscribe_cfg(&cfg, "v");
    dt_select(v, &cfg.delivery_timeout_flags, &cfg.object_delivery_timeout_ms,
              &cfg.subgroup_delivery_timeout_ms);
    moq_subscription_t ssub, csub;
    MOQ_TEST_CHECK(subscribe_accept(&fx, &cfg, NULL, 0, &ssub, &csub));
    int slot = sub_resolve_handle(fx.sv, ssub);
    MOQ_TEST_CHECK(slot >= 0);
    if (slot >= 0) {
        const moq_sub_entry_t *e = &fx.sv->subs[slot];
        expect_dt(e->dt_sub_has_object, e->dt_sub_object_ms,
                  e->dt_sub_has_subgroup, e->dt_sub_subgroup_ms, v, 50, 70);
    }
    uint64_t now = moq_simpair_now_us(fx.sp) + MS;
    moq_subgroup_handle_t sg = open_sg(fx.sv, ssub, now);
    expect_object_times_out(fx.sv, sg, now, "SUBSCRIBE", v);
    fx_close(&fx);
    MOQ_TEST_PASS("SUBSCRIBE carries the subscriber's delivery timeouts");
}

static bool publish(fx_t *fx, moq_publication_t *pub, moq_publication_t *sub_side)
{
    moq_publish_cfg_t cfg;
    moq_publish_cfg_init(&cfg);
    cfg.track_namespace = (moq_namespace_t){ &g_part, 1 };
    cfg.track_name = MOQ_BYTES_LITERAL("p");
    if (moq_session_publish(fx->c, &cfg, moq_simpair_now_us(fx->sp), pub) < 0)
        return false;
    run(fx);
    moq_event_t ev;
    if (!take(fx->sv, MOQ_EVENT_PUBLISH_REQUEST, &ev)) return false;
    *sub_side = ev.u.publish_request.pub;
    moq_event_cleanup(&ev);
    return true;
}

/* [DT-P2] The PUBLISH_OK of drafts 16 and 18 carries them; the publisher
 * reports them on its PUBLISH_OK event, holds them and enforces the object
 * one. */
static void test_publish_ok(moq_version_t v)
{
    fx_t fx;
    if (!fx_open(&fx, v)) { MOQ_TEST_CHECK(false); fx_close(&fx); return; }
    moq_publication_t pub, sph;
    MOQ_TEST_CHECK(publish(&fx, &pub, &sph));
    moq_accept_publish_cfg_t ac;
    moq_accept_publish_cfg_init_sized(&ac, sizeof(ac));
    dt_select(v, &ac.delivery_timeout_flags, &ac.object_delivery_timeout_ms,
              &ac.subgroup_delivery_timeout_ms);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_publish(fx.sv, sph, &ac,
                              moq_simpair_now_us(fx.sp)), MOQ_OK);
    run(&fx);
    moq_event_t ev;
    bool got = take(fx.c, MOQ_EVENT_PUBLISH_OK, &ev);
    MOQ_TEST_CHECK(got);
    if (got) {
        MOQ_TEST_CHECK(ev.u.publish_ok.has_delivery_timeout);
        MOQ_TEST_CHECK_EQ_U64(ev.u.publish_ok.delivery_timeout_ms, 50);
        moq_event_cleanup(&ev);
    }
    int slot = pub_resolve_handle(fx.c, pub);
    MOQ_TEST_CHECK(slot >= 0);
    if (slot >= 0) {
        const moq_pub_entry_t *e = &fx.c->publishes[slot];
        expect_dt(e->dt_sub_has_object, e->dt_sub_object_ms,
                  e->dt_sub_has_subgroup, e->dt_sub_subgroup_ms, v, 50, 70);
    }
    drain_events(fx.sv);
    drain_actions(fx.c);
    uint64_t now = moq_simpair_now_us(fx.sp) + MS;
    moq_subgroup_cfg_t sgc;
    moq_subgroup_cfg_init(&sgc);
    moq_subgroup_handle_t sg = MOQ_SUBGROUP_INVALID;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_pub_subgroup(fx.c, pub, &sgc,
                                                             now, &sg), MOQ_OK);
    expect_object_times_out(fx.c, sg, now, "PUBLISH_OK", v);
    fx_close(&fx);
    MOQ_TEST_PASS("PUBLISH_OK carries the subscriber's delivery timeouts");
}

/* [DT-P3] Draft-22's REQUEST_OK carries no subscriber choice: an accept that
 * selects timeouts is refused before anything is sent, and the timeouts go
 * in the REQUEST_UPDATE that follows. */
static void test_publish_ok_d22(void)
{
    moq_version_t v = MOQ_VERSION_DRAFT_22;
    fx_t fx;
    if (!fx_open(&fx, v)) { MOQ_TEST_CHECK(false); fx_close(&fx); return; }
    moq_publication_t pub, sph;
    MOQ_TEST_CHECK(publish(&fx, &pub, &sph));
    moq_accept_publish_cfg_t ac;
    moq_accept_publish_cfg_init_sized(&ac, sizeof(ac));
    dt_select(v, &ac.delivery_timeout_flags, &ac.object_delivery_timeout_ms,
              &ac.subgroup_delivery_timeout_ms);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_publish(fx.sv, sph, &ac,
                              moq_simpair_now_us(fx.sp)), MOQ_ERR_INVAL);
    moq_action_t a;
    MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_actions(fx.sv, &a, 1), 0);
    moq_accept_publish_cfg_init_sized(&ac, sizeof(ac));
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_publish(fx.sv, sph, &ac,
                              moq_simpair_now_us(fx.sp)), MOQ_OK);
    run(&fx);
    drain_events(fx.c);
    moq_publication_update_cfg_t u;
    moq_publication_update_cfg_init_sized(&u, sizeof(u));
    dt_select(v, &u.delivery_timeout_flags, &u.object_delivery_timeout_ms,
              &u.subgroup_delivery_timeout_ms);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_publication(fx.sv, sph, &u,
                              moq_simpair_now_us(fx.sp)), MOQ_OK);
    run(&fx);
    int slot = pub_resolve_handle(fx.c, pub);
    MOQ_TEST_CHECK(slot >= 0);
    if (slot >= 0) {
        const moq_pub_entry_t *e = &fx.c->publishes[slot];
        expect_dt(e->dt_sub_has_object, e->dt_sub_object_ms,
                  e->dt_sub_has_subgroup, e->dt_sub_subgroup_ms, v, 50, 70);
    }
    fx_close(&fx);
    MOQ_TEST_PASS("draft-22 takes the PUBLISH timeouts in a REQUEST_UPDATE");
}

/* [DT-P4] REQUEST_UPDATE carries each type independently on drafts 18 and
 * 22, and the one DELIVERY_TIMEOUT on draft-16; an omitted type keeps its
 * value. */
static void test_update(moq_version_t v)
{
    fx_t fx;
    if (!fx_open(&fx, v)) { MOQ_TEST_CHECK(false); fx_close(&fx); return; }
    moq_subscribe_cfg_t cfg;
    subscribe_cfg(&cfg, "u");
    moq_subscription_t ssub, csub;
    MOQ_TEST_CHECK(subscribe_accept(&fx, &cfg, NULL, 0, &ssub, &csub));
    int slot = sub_resolve_handle(fx.sv, ssub);
    MOQ_TEST_CHECK(slot >= 0);
    if (slot < 0) { fx_close(&fx); return; }

    moq_subscription_update_cfg_t u;
    moq_subscription_update_cfg_init_sized(&u, sizeof(u));
    u.delivery_timeout_flags = MOQ_SUBSCRIPTION_UPDATE_SUBGROUP_DELIVERY_TIMEOUT;
    u.subgroup_delivery_timeout_ms = 70;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_subscription(fx.c, csub, &u,
                              moq_simpair_now_us(fx.sp)), MOQ_OK);
    run(&fx);
    const moq_sub_entry_t *e = &fx.sv->subs[slot];
    if (v == MOQ_VERSION_DRAFT_16) {
        expect_dt(e->dt_sub_has_object, e->dt_sub_object_ms,
                  e->dt_sub_has_subgroup, e->dt_sub_subgroup_ms, v, 70, 70);
    } else {
        MOQ_TEST_CHECK(!e->dt_sub_has_object);
        MOQ_TEST_CHECK(e->dt_sub_has_subgroup);
        MOQ_TEST_CHECK_EQ_U64(e->dt_sub_subgroup_ms, 70);
    }
    drain_events(fx.sv);
    drain_events(fx.c);

    moq_subscription_update_cfg_init_sized(&u, sizeof(u));
    u.delivery_timeout_flags = MOQ_SUBSCRIPTION_UPDATE_OBJECT_DELIVERY_TIMEOUT;
    u.object_delivery_timeout_ms = 50;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_subscription(fx.c, csub, &u,
                              moq_simpair_now_us(fx.sp)), MOQ_OK);
    run(&fx);
    e = &fx.sv->subs[slot];
    expect_dt(e->dt_sub_has_object, e->dt_sub_object_ms,
              e->dt_sub_has_subgroup, e->dt_sub_subgroup_ms, v, 50, 70);
    drain_events(fx.sv);
    drain_actions(fx.sv);
    uint64_t now = moq_simpair_now_us(fx.sp) + MS;
    moq_subgroup_handle_t sg = open_sg(fx.sv, ssub, now);
    expect_object_times_out(fx.sv, sg, now, "REQUEST_UPDATE", v);
    fx_close(&fx);
    MOQ_TEST_PASS("REQUEST_UPDATE carries the subscriber's delivery timeouts");
}

/* [DT-P5] Draft-16's DELIVERY TIMEOUT is one value greater than 0
 * (9.2.2.2): a selection of 0 or of two different values is refused before
 * anything is sent, on each message. */
static void test_d16_single_value(void)
{
    fx_t fx;
    if (!fx_open(&fx, MOQ_VERSION_DRAFT_16)) {
        MOQ_TEST_CHECK(false); fx_close(&fx); return;
    }
    moq_publication_t pub, sph;
    MOQ_TEST_CHECK(publish(&fx, &pub, &sph));
    moq_accept_publish_cfg_t ac;
    moq_accept_publish_cfg_init_sized(&ac, sizeof(ac));
    ac.delivery_timeout_flags = MOQ_ACCEPT_PUBLISH_OBJECT_DELIVERY_TIMEOUT;
    ac.object_delivery_timeout_ms = 0;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_publish(fx.sv, sph, &ac,
                              moq_simpair_now_us(fx.sp)), MOQ_ERR_INVAL);
    ac.delivery_timeout_flags = MOQ_ACCEPT_PUBLISH_OBJECT_DELIVERY_TIMEOUT |
                                MOQ_ACCEPT_PUBLISH_SUBGROUP_DELIVERY_TIMEOUT;
    ac.object_delivery_timeout_ms = 50;
    ac.subgroup_delivery_timeout_ms = 60;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_publish(fx.sv, sph, &ac,
                              moq_simpair_now_us(fx.sp)), MOQ_ERR_INVAL);
    moq_action_t a;
    MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_actions(fx.sv, &a, 1), 0);
    ac.subgroup_delivery_timeout_ms = 50;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_publish(fx.sv, sph, &ac,
                              moq_simpair_now_us(fx.sp)), MOQ_OK);
    fx_close(&fx);
    MOQ_TEST_PASS("draft-16's single delivery timeout is never 0 nor two values");
}

/* [DT-P6] The publisher's defaults as Track Properties: one property per
 * type on drafts 18 and 22 (12.1, 12.2), draft-16's one Track Extension
 * (11.1). The same value already present is kept, another is refused. */
static void expect_props(moq_session_t *s, const uint8_t *in, size_t in_len,
                         bool ho, uint64_t o, bool hs, uint64_t sg,
                         moq_result_t want, const uint8_t *bytes, size_t n)
{
    uint8_t out[64];
    size_t len = 0;
    moq_result_t rc = moq_session_track_properties_add_delivery_timeouts(
        s, in, in_len, ho, o, hs, sg, out, sizeof(out), &len);
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)want);
    if (rc == MOQ_OK) {
        MOQ_TEST_CHECK_EQ_SIZE(len, n);
        MOQ_TEST_CHECK(len == n && memcmp(out, bytes, n) == 0);
    }
}

static void test_track_properties(moq_version_t v)
{
    fx_t fx;
    if (!fx_open(&fx, v)) { MOQ_TEST_CHECK(false); fx_close(&fx); return; }
    moq_session_t *s = fx.sv;
    static const uint8_t obj20[] = { 0x02, 20 };
    static const uint8_t obj21[] = { 0x02, 21 };
    static const uint8_t sg30[] = { 0x06, 30 };
    static const uint8_t both[] = { 0x02, 20, 0x04, 30 };   /* delta types */
    expect_props(s, NULL, 0, false, 0, false, 0, MOQ_ERR_INVAL, NULL, 0);
    expect_props(s, NULL, 0, true, 20, false, 0, MOQ_OK, obj20, 2);
    expect_props(s, obj20, 2, true, 20, false, 0, MOQ_OK, obj20, 2);
    expect_props(s, obj21, 2, true, 20, false, 0, MOQ_ERR_INVAL, NULL, 0);
    if (v == MOQ_VERSION_DRAFT_16) {
        expect_props(s, NULL, 0, true, 20, true, 20, MOQ_OK, obj20, 2);
        expect_props(s, NULL, 0, true, 20, true, 30, MOQ_ERR_INVAL, NULL, 0);
        expect_props(s, NULL, 0, true, 0, false, 0, MOQ_ERR_INVAL, NULL, 0);
    } else {
        expect_props(s, NULL, 0, false, 0, true, 30, MOQ_OK, sg30, 2);
        expect_props(s, NULL, 0, true, 20, true, 30, MOQ_OK, both, 4);
        expect_props(s, obj20, 2, true, 20, true, 30, MOQ_OK, both, 4);
    }

    /* A publisher default alone (the subscriber asks for none) is enforced. */
    uint8_t props[16];
    size_t props_len = 0;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_track_properties_add_delivery_timeouts(
        s, NULL, 0, true, 50, false, 0, props, sizeof(props), &props_len),
        MOQ_OK);
    moq_subscribe_cfg_t cfg;
    subscribe_cfg(&cfg, "t");
    moq_subscription_t ssub, csub;
    MOQ_TEST_CHECK(subscribe_accept(&fx, &cfg, props, props_len, &ssub, &csub));
    uint64_t now = moq_simpair_now_us(fx.sp) + MS;
    moq_subgroup_handle_t sg = open_sg(fx.sv, ssub, now);
    expect_object_times_out(fx.sv, sg, now, "Track Property", v);
    fx_close(&fx);
    MOQ_TEST_PASS("the publisher's delivery timeouts as Track Properties");
}

int main(void)
{
    g_part = MOQ_BYTES_LITERAL("live");
    for (size_t i = 0; i < N_VERSIONS; i++) {
        test_subscribe(k_versions[i]);
        test_update(k_versions[i]);
        test_track_properties(k_versions[i]);
    }
    test_publish_ok(MOQ_VERSION_DRAFT_16);
    test_publish_ok(MOQ_VERSION_DRAFT_18);
    test_publish_ok_d22();
    test_d16_single_value();
    if (failures) {
        fprintf(stderr, "test_delivery_timeout_params: %d failure(s)\n", failures);
        return 1;
    }
    printf("test_delivery_timeout_params: all passed\n");
    return 0;
}
