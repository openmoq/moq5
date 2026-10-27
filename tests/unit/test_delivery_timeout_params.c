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
#include "test_session_support.h"
#include "accept_publish_historical.h"
#include "../../core/src/session/session_internal.h"
#include <stdlib.h>
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

static bool fx_open_wait(fx_t *fx, moq_version_t v, uint64_t wait_us)
{
    memset(fx, 0, sizeof(*fx));
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = moq_alloc_default();
    cfg.version = v;
    cfg.done_wait_timeout_us = wait_us;
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

static bool fx_open(fx_t *fx, moq_version_t v)
{
    return fx_open_wait(fx, v, 0);
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
static moq_result_t build_props(moq_session_t *s, const uint8_t *in, size_t in_len,
    const moq_track_delivery_timeouts_cfg_t *cfg, uint8_t *out, size_t cap, size_t *len)
{
    /* Building properties is passive even on failure, unlike session UPDATE. */
    uint64_t now = s->last_now_us, epoch = s->borrow_epoch, ref = s->next_stream_ref;
    size_t ah = s->action_head, at = s->action_tail, eh = s->event_head,
           et = s->event_tail, bytes = s->send_len;
    size_t state_size = s->profile->state_size;
    void *before = malloc(state_size ? state_size : 1);
    if (!before) { MOQ_TEST_CHECK(false); return MOQ_ERR_NOMEM; }
    memcpy(before, s->profile_state, state_size);
    moq_result_t rc = moq_session_track_properties_add_delivery_timeouts(s,
        in, in_len, cfg, out, cap, len);
    MOQ_TEST_CHECK(s->last_now_us == now && s->borrow_epoch == epoch && s->next_stream_ref == ref);
    MOQ_TEST_CHECK(s->action_head == ah && s->action_tail == at &&
        s->event_head == eh && s->event_tail == et && s->send_len == bytes);
    MOQ_TEST_CHECK(memcmp(before, s->profile_state, state_size) == 0);
    free(before);
    return rc;
}

static void expect_props(moq_session_t *s, const uint8_t *in, size_t in_len,
                         bool ho, uint64_t o, bool hs, uint64_t sg,
                         moq_result_t want, const uint8_t *bytes, size_t n)
{
    uint8_t out[64];
    size_t len = 0;
    moq_track_delivery_timeouts_cfg_t cfg;
    moq_track_delivery_timeouts_cfg_init(&cfg);
    cfg.has_object_delivery_timeout = ho;
    cfg.object_delivery_timeout_ms = o;
    cfg.has_subgroup_delivery_timeout = hs;
    cfg.subgroup_delivery_timeout_ms = sg;
    moq_result_t rc = build_props(
        s, in, in_len, &cfg, out, sizeof(out), &len);
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)want);
    if (rc == MOQ_OK) {
        MOQ_TEST_CHECK_EQ_SIZE(len, n);
        MOQ_TEST_CHECK(len == n && memcmp(out, bytes, n) == 0);
    }
}

static void test_builder_initializer(void)
{
    const size_t current = sizeof(moq_track_delivery_timeouts_cfg_t);
    for (size_t size = 0; size <= current + 8; ++size) {
        unsigned char *storage = malloc(size + 16);
        if (!storage) { MOQ_TEST_CHECK(false); return; }
        memset(storage, 0xA5, size + 16);
        moq_track_delivery_timeouts_cfg_init_sized((moq_track_delivery_timeouts_cfg_t *)storage, size);
        size_t written = size < current ? size : current;
        if (written < sizeof(uint32_t)) written = 0;
        if (written) {
            uint32_t stamped;
            memcpy(&stamped, storage, sizeof(stamped));
            MOQ_TEST_CHECK_EQ_SIZE(stamped, written);
            for (size_t i = sizeof(stamped); i < written; ++i) MOQ_TEST_CHECK(storage[i] == 0);
        }
        for (size_t i = written; i < size + 16; ++i) MOQ_TEST_CHECK(storage[i] == 0xA5);
        free(storage);
    }
    moq_track_delivery_timeouts_cfg_init_sized(NULL, current);
    moq_track_delivery_timeouts_cfg_t cfg;
    moq_track_delivery_timeouts_cfg_init(&cfg);
    MOQ_TEST_CHECK_EQ_SIZE(cfg.struct_size, sizeof(cfg));
    MOQ_TEST_CHECK(!cfg.has_object_delivery_timeout && !cfg.has_subgroup_delivery_timeout);
    MOQ_TEST_CHECK(cfg.object_delivery_timeout_ms == 0 && cfg.subgroup_delivery_timeout_ms == 0);
}

static void test_builder_sized_inputs(moq_session_t *s)
{
    moq_track_delivery_timeouts_cfg_t cfg;
    moq_track_delivery_timeouts_cfg_init(&cfg);
    cfg.has_object_delivery_timeout = true;
    cfg.object_delivery_timeout_ms = 50;
    cfg.subgroup_delivery_timeout_ms = UINT64_MAX; /* unselected, ignored on d16 too */
    uint8_t out[64]; size_t len = 0;
    /* Every short actual allocation stops before the frozen last-v0 field. */
    for (size_t size = sizeof(uint32_t); size <= MOQ_TRACK_DELIVERY_TIMEOUTS_CFG_V0_SIZE; ++size) {
        cfg.struct_size = (uint32_t)size;
        void *input = malloc(size);
        if (!input) { MOQ_TEST_CHECK(false); return; }
        memcpy(input, &cfg, size);
        moq_result_t want = size < MOQ_TRACK_DELIVERY_TIMEOUTS_CFG_V0_SIZE ? MOQ_ERR_INVAL : MOQ_OK;
        MOQ_TEST_CHECK_EQ_INT((int)build_props(s, NULL, 0, input, out, sizeof(out), &len), want);
        MOQ_TEST_CHECK(memcmp(input, &cfg, size) == 0);
        if (want == MOQ_OK) {
            const uint8_t expected[] = {2, 50};
            MOQ_TEST_CHECK(len == sizeof(expected) && memcmp(out, expected, sizeof(expected)) == 0);
        }
        free(input);
    }
    struct { moq_track_delivery_timeouts_cfg_t cfg; uint8_t future[16]; } future;
    memset(&future, 0xA5, sizeof(future));
    moq_track_delivery_timeouts_cfg_init_sized(&future.cfg, sizeof(future));
    MOQ_TEST_CHECK_EQ_SIZE(future.cfg.struct_size, sizeof(future.cfg));
    for (size_t i = 0; i < sizeof(future.future); ++i) MOQ_TEST_CHECK(future.future[i] == 0xA5);
    future.cfg = cfg;
    future.cfg.struct_size = (uint32_t)sizeof(future);
    unsigned char before[sizeof(future)];
    memcpy(before, &future, sizeof(future));
    MOQ_TEST_CHECK_EQ_INT((int)build_props(s, NULL, 0, &future.cfg, out, sizeof(out), &len), MOQ_OK);
    MOQ_TEST_CHECK(memcmp(before, &future, sizeof(future)) == 0);
    MOQ_TEST_CHECK(len == 2 && out[0] == 2 && out[1] == 50);

    cfg.struct_size = (uint32_t)sizeof(cfg);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_track_properties_add_delivery_timeouts(
        NULL, NULL, 0, &cfg, out, sizeof(out), &len), MOQ_ERR_INVAL);
    MOQ_TEST_CHECK_EQ_INT((int)build_props(s, NULL, 0, NULL, out, sizeof(out), &len), MOQ_ERR_INVAL);
    MOQ_TEST_CHECK_EQ_INT((int)build_props(s, NULL, 1, &cfg, out, sizeof(out), &len), MOQ_ERR_INVAL);
    MOQ_TEST_CHECK_EQ_INT((int)build_props(s, NULL, 0, &cfg, NULL, 0, &len), MOQ_ERR_INVAL);
    MOQ_TEST_CHECK_EQ_INT((int)build_props(s, NULL, 0, &cfg, out, sizeof(out), NULL), MOQ_ERR_INVAL);
}

static void test_builder_values(moq_session_t *s, moq_version_t v)
{
    const uint64_t values[] = {0, 1, MOQ_QUIC_VARINT_MAX, MOQ_QUIC_VARINT_MAX + 1, UINT64_MAX};
    for (unsigned which = 0; which < 3; ++which) {
        for (size_t i = 0; i < sizeof(values) / sizeof(values[0]); ++i) {
            moq_track_delivery_timeouts_cfg_t cfg;
            moq_track_delivery_timeouts_cfg_init(&cfg);
            cfg.has_object_delivery_timeout = which != 1;
            cfg.has_subgroup_delivery_timeout = which != 0;
            cfg.object_delivery_timeout_ms = cfg.has_object_delivery_timeout ? values[i] : UINT64_MAX;
            cfg.subgroup_delivery_timeout_ms = cfg.has_subgroup_delivery_timeout ? values[i] : UINT64_MAX;
            uint8_t out[64]; size_t len = 0;
            moq_result_t want = v == MOQ_VERSION_DRAFT_16 &&
                (values[i] == 0 || values[i] > MOQ_QUIC_VARINT_MAX) ? MOQ_ERR_INVAL : MOQ_OK;
            moq_result_t rc = build_props(s, NULL, 0, &cfg, out, sizeof(out), &len);
            MOQ_TEST_CHECK_EQ_INT((int)rc, (int)want);
            if (rc == MOQ_OK) {
                MOQ_TEST_CHECK(len <= sizeof(out));
                if (len > sizeof(out)) continue;
                moq_dt_scan_t scan;
                MOQ_TEST_CHECK_EQ_INT((int)session_scan_dt_props(s, out, len, true, &scan), MOQ_OK);
                MOQ_TEST_CHECK(scan.has_object == (which != 1 || v == MOQ_VERSION_DRAFT_16));
                MOQ_TEST_CHECK(scan.has_subgroup == (which != 0 || v == MOQ_VERSION_DRAFT_16));
                if (scan.has_object) MOQ_TEST_CHECK_EQ_U64(scan.object_ms, values[i]);
                if (scan.has_subgroup) MOQ_TEST_CHECK_EQ_U64(scan.subgroup_ms, values[i]);
            }
        }
    }
    expect_props(s, NULL, 0, false, UINT64_MAX, false, UINT64_MAX, MOQ_ERR_INVAL, NULL, 0);
    expect_props(s, NULL, 0, true, 20, true, 30,
        v == MOQ_VERSION_DRAFT_16 ? MOQ_ERR_INVAL : MOQ_OK,
        (const uint8_t[]){2, 20, 4, 30}, 4);
    expect_props(s, NULL, 0, false, UINT64_MAX, true, 30, MOQ_OK,
        v == MOQ_VERSION_DRAFT_16 ? (const uint8_t[]){2, 30} : (const uint8_t[]){6, 30}, 2);
}

/* Existing allocation/refusal fixture, no extra harness or session mutation. */
static void test_builder_resources(moq_version_t v)
{
    fail_alloc_state_t memory = {0};
    moq_alloc_t alloc = fail_allocator(&memory);
    moq_session_cfg_t sc;
    moq_session_cfg_init_sized(&sc, sizeof(sc), &alloc, MOQ_PERSPECTIVE_SERVER);
    sc.version = v;
    moq_session_t *s = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_create(&sc, 0, &s), MOQ_OK);
    if (!s) return;
    int64_t baseline = memory.balance;
    moq_track_delivery_timeouts_cfg_t cfg;
    moq_track_delivery_timeouts_cfg_init(&cfg);
    cfg.has_object_delivery_timeout = cfg.has_subgroup_delivery_timeout = true;
    cfg.object_delivery_timeout_ms = cfg.subgroup_delivery_timeout_ms = 20;
    uint8_t out[64]; size_t len = 0;
    int calls = memory.call_count;
    memory.fail_at = calls + 1;
    MOQ_TEST_CHECK_EQ_INT((int)build_props(s, NULL, 0, &cfg, out, sizeof(out), &len),
        v == MOQ_VERSION_DRAFT_16 ? MOQ_OK : MOQ_ERR_NOMEM);
    MOQ_TEST_CHECK_EQ_INT(memory.call_count, calls + (v == MOQ_VERSION_DRAFT_16 ? 0 : 1));
    MOQ_TEST_CHECK(memory.balance == baseline);
    memory.fail_at = 0;
    /* First merge failure, second merge conflict, BUFFER, and success must
     * all release the two-value scratch allocation. */
    const uint8_t first_conflict[] = {2, 21};
    const uint8_t second_conflict[] = {6, 21};
    const uint8_t malformed[] = {2};
    struct { const uint8_t *in; size_t len; moq_result_t want; } rows[] = {
        {first_conflict, sizeof(first_conflict), MOQ_ERR_INVAL},
        {malformed, sizeof(malformed), MOQ_ERR_INVAL},
        {NULL, 0, MOQ_OK},
    };
    for (size_t i = 0; i < sizeof(rows) / sizeof(rows[0]); ++i) {
        calls = memory.call_count;
        MOQ_TEST_CHECK_EQ_INT((int)build_props(s, rows[i].in, rows[i].len, &cfg,
            out, sizeof(out), &len), rows[i].want);
        MOQ_TEST_CHECK_EQ_INT(memory.call_count, calls + (v == MOQ_VERSION_DRAFT_16 ? 0 : 1));
        MOQ_TEST_CHECK(memory.balance == baseline);
    }
    if (v != MOQ_VERSION_DRAFT_16) {
        MOQ_TEST_CHECK_EQ_INT((int)build_props(s, second_conflict, sizeof(second_conflict),
            &cfg, out, sizeof(out), &len), MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(memory.balance == baseline);
        calls = memory.call_count;
        uint8_t dummy = 0;
        MOQ_TEST_CHECK_EQ_INT((int)build_props(s, &dummy, SIZE_MAX - 15,
            &cfg, out, sizeof(out), &len), MOQ_ERR_INVAL);
        MOQ_TEST_CHECK_EQ_INT(memory.call_count, calls); /* overflow before read/allocation */
    }
    size_t need = v == MOQ_VERSION_DRAFT_16 ? 2 : 4;
    for (size_t cap = 0; cap <= need; ++cap) {
        memset(out, 0xA5, sizeof(out));
        MOQ_TEST_CHECK_EQ_INT((int)build_props(s, NULL, 0, &cfg, out, cap, &len),
            cap < need ? MOQ_ERR_BUFFER : MOQ_OK);
        for (size_t i = cap; i < sizeof(out); ++i) MOQ_TEST_CHECK(out[i] == 0xA5);
        MOQ_TEST_CHECK(memory.balance == baseline);
        if (cap == need) MOQ_TEST_CHECK_EQ_SIZE(len, need);
    }
    moq_session_destroy(s);
    MOQ_TEST_CHECK_EQ_U64(memory.balance, 0);
}

static void test_track_properties(moq_version_t v)
{
    fx_t fx;
    if (!fx_open(&fx, v)) { MOQ_TEST_CHECK(false); fx_close(&fx); return; }
    moq_session_t *s = fx.sv;
    test_builder_sized_inputs(s);
    test_builder_values(s, v);
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
        const uint8_t duplicate[] = {2, 20, 0, 20};
        const uint8_t immutable[] = {0x0B, 4, 2, 20, 4, 30};
        const uint8_t mixed_duplicate[] = {2, 20, 9, 2, 2, 20};
        const uint8_t immutable_duplicate[] = {0x0B, 4, 2, 20, 0, 20};
        const uint8_t immutable_conflict[] = {0x0B, 2, 2, 21};
        expect_props(s, duplicate, sizeof(duplicate), true, 20, false, 0, MOQ_ERR_INVAL, NULL, 0);
        expect_props(s, immutable, sizeof(immutable), true, 20, true, 30, MOQ_OK,
                     immutable, sizeof(immutable));
        expect_props(s, mixed_duplicate, sizeof(mixed_duplicate), true, 20, false, 0, MOQ_ERR_INVAL, NULL, 0);
        expect_props(s, immutable_duplicate, sizeof(immutable_duplicate), true, 20, false, 0, MOQ_ERR_INVAL, NULL, 0);
        expect_props(s, immutable_conflict, sizeof(immutable_conflict), true, 20, false, 0, MOQ_ERR_INVAL, NULL, 0);
    }

    /* A publisher default alone (the subscriber asks for none) is enforced. */
    uint8_t props[16];
    size_t props_len = 0;
    moq_track_delivery_timeouts_cfg_t publisher_timeouts;
    moq_track_delivery_timeouts_cfg_init(&publisher_timeouts);
    publisher_timeouts.has_object_delivery_timeout = true;
    publisher_timeouts.object_delivery_timeout_ms = 50;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_track_properties_add_delivery_timeouts(
        s, NULL, 0, &publisher_timeouts, props, sizeof(props), &props_len),
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

/* A draft-16 timeout is one wire value, so either public selector must
 * commit the same complete state at BOTH endpoints. The requester uses this
 * state to bound terminal waits; retaining an older alias is observable. */
static void expect_local_timeout(bool ho, uint64_t obj, bool hs, uint64_t sg,
                                 uint64_t want, const char *row)
{
    fprintf(stderr, "PR62-LOCAL %s object=%d/%llu subgroup=%d/%llu want=%llu\n",
            row, ho, (unsigned long long)obj, hs, (unsigned long long)sg,
            (unsigned long long)want);
    MOQ_TEST_CHECK(ho);
    MOQ_TEST_CHECK_EQ_U64(obj, want);
    MOQ_TEST_CHECK(hs);
    MOQ_TEST_CHECK_EQ_U64(sg, want);
}

static void test_d16_requester_state(bool publication, bool first_object)
{
    fx_t fx;
    if (!fx_open(&fx, MOQ_VERSION_DRAFT_16)) {
        MOQ_TEST_CHECK(false); fx_close(&fx); return;
    }
    moq_subscription_t local_sub = MOQ_SUBSCRIPTION_INVALID;
    moq_subscription_t peer_sub = MOQ_SUBSCRIPTION_INVALID;
    moq_publication_t local_pub = MOQ_PUBLICATION_INVALID;
    moq_publication_t peer_pub = MOQ_PUBLICATION_INVALID;
    moq_session_t *owner = publication ? fx.sv : fx.c;
    if (publication) {
        if (!publish(&fx, &peer_pub, &local_pub)) {
            MOQ_TEST_CHECK(false); fx_close(&fx); return;
        }
        moq_accept_publish_cfg_t ac;
        moq_accept_publish_cfg_init_sized(&ac, sizeof(ac));
        ac.delivery_timeout_flags = first_object
            ? MOQ_ACCEPT_PUBLISH_OBJECT_DELIVERY_TIMEOUT
            : MOQ_ACCEPT_PUBLISH_SUBGROUP_DELIVERY_TIMEOUT;
        ac.object_delivery_timeout_ms = first_object ? 60000 : 0;
        ac.subgroup_delivery_timeout_ms = first_object ? 0 : 60000;
        MOQ_TEST_CHECK(moq_session_accept_publish(owner, local_pub, &ac,
                           moq_simpair_now_us(fx.sp)) == MOQ_OK);
    } else {
        moq_subscribe_cfg_t cfg;
        subscribe_cfg(&cfg, "local-timeout");
        cfg.delivery_timeout_flags = first_object
            ? MOQ_SUBSCRIBE_OBJECT_DELIVERY_TIMEOUT
            : MOQ_SUBSCRIBE_SUBGROUP_DELIVERY_TIMEOUT;
        cfg.object_delivery_timeout_ms = first_object ? 60000 : 0;
        cfg.subgroup_delivery_timeout_ms = first_object ? 0 : 60000;
        if (!subscribe_accept(&fx, &cfg, NULL, 0, &peer_sub, &local_sub)) {
            MOQ_TEST_CHECK(false); fx_close(&fx); return;
        }
    }
    run(&fx);
    drain_events(fx.c); drain_events(fx.sv);
    for (unsigned step = 0; step < 3; ++step) {
        uint64_t want = step == 0 ? 60000 : step == 1 ? 50 : 1000;
        if (step != 0) {
            bool use_object = step == 1 ? !first_object : first_object;
            if (publication) {
                moq_publication_update_cfg_t u;
                moq_publication_update_cfg_init_sized(&u, sizeof(u));
                u.delivery_timeout_flags = use_object
                    ? MOQ_SUBSCRIPTION_UPDATE_OBJECT_DELIVERY_TIMEOUT
                    : MOQ_SUBSCRIPTION_UPDATE_SUBGROUP_DELIVERY_TIMEOUT;
                u.object_delivery_timeout_ms = use_object ? want : 0;
                u.subgroup_delivery_timeout_ms = use_object ? 0 : want;
                MOQ_TEST_CHECK(moq_session_update_publication(owner, local_pub,
                                   &u, moq_simpair_now_us(fx.sp)) == MOQ_OK);
            } else {
                moq_subscription_update_cfg_t u;
                moq_subscription_update_cfg_init_sized(&u, sizeof(u));
                u.delivery_timeout_flags = use_object
                    ? MOQ_SUBSCRIPTION_UPDATE_OBJECT_DELIVERY_TIMEOUT
                    : MOQ_SUBSCRIPTION_UPDATE_SUBGROUP_DELIVERY_TIMEOUT;
                u.object_delivery_timeout_ms = use_object ? want : 0;
                u.subgroup_delivery_timeout_ms = use_object ? 0 : want;
                MOQ_TEST_CHECK(moq_session_update_subscription(owner, local_sub,
                                   &u, moq_simpair_now_us(fx.sp)) == MOQ_OK);
            }
            run(&fx);
            drain_events(fx.c); drain_events(fx.sv);
        }
        char row[64];
        (void)snprintf(row, sizeof(row), "%s-%s-step%u",
                       publication ? "publish" : "subscribe",
                       first_object ? "object" : "subgroup", step);
        if (publication) {
            int ls = pub_resolve_handle(owner, local_pub);
            int ps = pub_resolve_handle(fx.c, peer_pub);
            MOQ_TEST_CHECK(ls >= 0 && ps >= 0);
            if (ls >= 0 && ps >= 0) {
                moq_pub_entry_t *l = &owner->publishes[ls];
                moq_pub_entry_t *r = &fx.c->publishes[ps];
                expect_local_timeout(l->dt_sub_has_object, l->dt_sub_object_ms,
                    l->dt_sub_has_subgroup, l->dt_sub_subgroup_ms, want, row);
                expect_dt(r->dt_sub_has_object, r->dt_sub_object_ms,
                    r->dt_sub_has_subgroup, r->dt_sub_subgroup_ms,
                    MOQ_VERSION_DRAFT_16, want, want);
                MOQ_TEST_CHECK(!l->update_pending);
            }
        } else {
            int ls = sub_resolve_handle(owner, local_sub);
            int ps = sub_resolve_handle(fx.sv, peer_sub);
            MOQ_TEST_CHECK(ls >= 0 && ps >= 0);
            if (ls >= 0 && ps >= 0) {
                moq_sub_entry_t *l = &owner->subs[ls];
                moq_sub_entry_t *r = &fx.sv->subs[ps];
                expect_local_timeout(l->dt_sub_has_object, l->dt_sub_object_ms,
                    l->dt_sub_has_subgroup, l->dt_sub_subgroup_ms, want, row);
                expect_dt(r->dt_sub_has_object, r->dt_sub_object_ms,
                    r->dt_sub_has_subgroup, r->dt_sub_subgroup_ms,
                    MOQ_VERSION_DRAFT_16, want, want);
                MOQ_TEST_CHECK(!l->update_pending);
            }
        }
    }
    fx_close(&fx);
}

/* A sent data stream whose header never reaches the receiver is exactly
 * why PUBLISH_DONE has a recovery timeout. After an ACKed decrease the stale
 * alias must not retain the subscription for the original sixty seconds. */
static void test_d16_decreased_terminal_wait(void)
{
    fx_t fx;
    if (!fx_open_wait(&fx, MOQ_VERSION_DRAFT_16, MS)) {
        MOQ_TEST_CHECK(false); fx_close(&fx); return;
    }
    moq_subscribe_cfg_t cfg;
    subscribe_cfg(&cfg, "terminal-timeout");
    cfg.delivery_timeout_flags = MOQ_SUBSCRIBE_OBJECT_DELIVERY_TIMEOUT;
    cfg.object_delivery_timeout_ms = 60000;
    moq_subscription_t remote = MOQ_SUBSCRIPTION_INVALID;
    moq_subscription_t local = MOQ_SUBSCRIPTION_INVALID;
    if (!subscribe_accept(&fx, &cfg, NULL, 0, &remote, &local)) {
        MOQ_TEST_CHECK(false); fx_close(&fx); return;
    }
    moq_subscription_update_cfg_t u;
    moq_subscription_update_cfg_init_sized(&u, sizeof(u));
    u.delivery_timeout_flags = MOQ_SUBSCRIPTION_UPDATE_SUBGROUP_DELIVERY_TIMEOUT;
    u.subgroup_delivery_timeout_ms = 50;
    MOQ_TEST_CHECK(moq_session_update_subscription(fx.c, local, &u,
                       moq_simpair_now_us(fx.sp)) == MOQ_OK);
    run(&fx);
    drain_events(fx.c); drain_events(fx.sv);
    int ls = sub_resolve_handle(fx.c, local);
    MOQ_TEST_CHECK(ls >= 0);
    if (ls < 0) { fx_close(&fx); return; }
    MOQ_TEST_CHECK(!fx.c->subs[ls].update_pending);

    uint64_t now = moq_simpair_now_us(fx.sp);
    moq_subgroup_handle_t sg = open_sg(fx.sv, remote, now);
    MOQ_TEST_CHECK(moq_session_close_subgroup(fx.sv, sg, now) == MOQ_OK);
    /* Consume/drop exactly the simulated transport's data actions. The data
     * stream exists at the sender but its header is lost before reception. */
    drain_actions(fx.sv);
    moq_done_subscribe_cfg_t dc;
    moq_done_subscribe_cfg_init(&dc);
    dc.stream_count = 1;
    MOQ_TEST_CHECK(moq_session_done_subscribe(fx.sv, remote, &dc, now) == MOQ_OK);
    run(&fx);
    MOQ_TEST_CHECK(fx.c->subs[ls].done_pending);
    moq_event_t ev;
    bool early = take(fx.c, MOQ_EVENT_SUBSCRIBE_DONE, &ev);
    MOQ_TEST_CHECK(!early);
    if (early) moq_event_cleanup(&ev);
    uint64_t deadline = fx.c->subs[ls].done_deadline_us;
    MOQ_TEST_CHECK(moq_session_tick(fx.c,
                       moq_simpair_now_us(fx.sp) + 51 * MS) == MOQ_OK);
    bool got = take(fx.c, MOQ_EVENT_SUBSCRIBE_DONE, &ev);
    fprintf(stderr, "PR62-TERMINAL deadline_delta=%llu recovered=%d\n",
            (unsigned long long)(deadline - now), got);
    MOQ_TEST_CHECK(got);
    if (got) moq_event_cleanup(&ev);
    fx_close(&fx);
}

/* Pressure stays local: these resource-free fillers are discarded, never
 * delivered to the simulated peer as protocol input. */
static bool fill_actions(moq_session_t *s)
{
    moq_action_t a = {0};
    a.kind = MOQ_ACTION_SEND_CONTROL;
    a.detail_size = (uint32_t)sizeof(moq_send_control_action_t);
    for (size_t i = 0; i < s->action_cap && !action_queue_full(s); ++i)
        if (push_action(s, &a) != MOQ_OK) return false;
    return action_queue_full(s);
}

static moq_result_t update_timeout(fx_t *fx, bool publication,
    moq_subscription_t sub, moq_publication_t pub, uint32_t flags,
    uint64_t object_ms, uint64_t subgroup_ms, bool generic, uint64_t now)
{
    if (publication) {
        moq_publication_update_cfg_t u;
        moq_publication_update_cfg_init_sized(&u, sizeof(u));
        u.delivery_timeout_flags = flags;
        u.object_delivery_timeout_ms = object_ms;
        u.subgroup_delivery_timeout_ms = subgroup_ms;
        u.has_delivery_timeout = generic;
        u.delivery_timeout_us = 1000;
        return moq_session_update_publication(fx->sv, pub, &u, now);
    }
    moq_subscription_update_cfg_t u;
    moq_subscription_update_cfg_init_sized(&u, sizeof(u));
    u.delivery_timeout_flags = flags;
    u.object_delivery_timeout_ms = object_ms;
    u.subgroup_delivery_timeout_ms = subgroup_ms;
    u.has_delivery_timeout = generic;
    u.delivery_timeout_us = 1000;
    return moq_session_update_subscription(fx->c, sub, &u, now);
}

/* Complement the parent's complete wire round-trips with exact refusal and
 * ACK admission boundaries. The private ACK hook is the codec's commit hook;
 * REQUEST_ERROR below is decoded from real wire bytes through the public API. */
static void test_d16_update_transaction(bool publication)
{
    fx_t fx;
    if (!fx_open(&fx, MOQ_VERSION_DRAFT_16)) {
        MOQ_TEST_CHECK(false); fx_close(&fx); return;
    }
    moq_subscription_t sub = MOQ_SUBSCRIPTION_INVALID, remote_sub;
    moq_publication_t pub = MOQ_PUBLICATION_INVALID, remote_pub;
    moq_session_t *s = publication ? fx.sv : fx.c;
    if (publication) {
        if (!publish(&fx, &remote_pub, &pub)) {
            MOQ_TEST_CHECK(false); fx_close(&fx); return;
        }
        moq_accept_publish_cfg_t ac;
        moq_accept_publish_cfg_init_sized(&ac, sizeof(ac));
        ac.delivery_timeout_flags = MOQ_ACCEPT_PUBLISH_SUBGROUP_DELIVERY_TIMEOUT;
        ac.subgroup_delivery_timeout_ms = 60000;
        MOQ_TEST_CHECK(fill_actions(s));
        MOQ_TEST_CHECK(moq_session_accept_publish(s, pub, &ac,
                           moq_simpair_now_us(fx.sp)) == MOQ_ERR_WOULD_BLOCK);
        int pending = pub_resolve_handle(s, pub);
        MOQ_TEST_CHECK(pending >= 0);
        if (pending < 0) { fx_close(&fx); return; }
        MOQ_TEST_CHECK(s->publishes[pending].state == MOQ_PUB_PENDING_SUBSCRIBER);
        MOQ_TEST_CHECK(!s->publishes[pending].dt_sub_has_object);
        MOQ_TEST_CHECK(!s->publishes[pending].dt_sub_has_subgroup);
        drain_actions(s);
        MOQ_TEST_CHECK(moq_session_accept_publish(s, pub, &ac,
                           moq_simpair_now_us(fx.sp)) == MOQ_OK);
        run(&fx);
        drain_events(fx.c); drain_events(fx.sv);
    } else {
        moq_subscribe_cfg_t cfg;
        subscribe_cfg(&cfg, "transaction");
        cfg.delivery_timeout_flags = MOQ_SUBSCRIBE_OBJECT_DELIVERY_TIMEOUT;
        cfg.object_delivery_timeout_ms = 60000;
        if (!subscribe_accept(&fx, &cfg, NULL, 0, &remote_sub, &sub)) {
            MOQ_TEST_CHECK(false); fx_close(&fx); return;
        }
    }
    int slot = publication ? pub_resolve_handle(s, pub) : sub_resolve_handle(s, sub);
    MOQ_TEST_CHECK(slot >= 0);
    if (slot < 0) { fx_close(&fx); return; }
    bool *pending = publication ? &s->publishes[slot].update_pending
                                : &s->subs[slot].update_pending;
    bool *ho = publication ? &s->publishes[slot].dt_sub_has_object
                           : &s->subs[slot].dt_sub_has_object;
    bool *hs = publication ? &s->publishes[slot].dt_sub_has_subgroup
                           : &s->subs[slot].dt_sub_has_subgroup;
    uint64_t *obj = publication ? &s->publishes[slot].dt_sub_object_ms
                                : &s->subs[slot].dt_sub_object_ms;
    uint64_t *sg = publication ? &s->publishes[slot].dt_sub_subgroup_ms
                               : &s->subs[slot].dt_sub_subgroup_ms;
    bool *up_ho = publication ? &s->publishes[slot].dt_upd_has_object
                              : &s->subs[slot].dt_upd_has_object;
    bool *up_hs = publication ? &s->publishes[slot].dt_upd_has_subgroup
                              : &s->subs[slot].dt_upd_has_subgroup;
    const uint32_t both = MOQ_SUBSCRIPTION_UPDATE_OBJECT_DELIVERY_TIMEOUT |
                          MOQ_SUBSCRIPTION_UPDATE_SUBGROUP_DELIVERY_TIMEOUT;
    const uint32_t one = MOQ_SUBSCRIPTION_UPDATE_SUBGROUP_DELIVERY_TIMEOUT;
    uint64_t now = moq_simpair_now_us(fx.sp);
    const struct { uint32_t flags; uint64_t obj, sg; bool generic; } bad[] = {
        { one, 0, 0, false }, { both, 50, 60, false },
        { one, 0, UINT64_C(1) << 62, false }, { one, 0, 50, true },
    };
    for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); ++i) {
        size_t tail = s->action_tail;
        uint64_t at = s->last_now_us;
        MOQ_TEST_CHECK(update_timeout(&fx, publication, sub, pub, bad[i].flags,
            bad[i].obj, bad[i].sg, bad[i].generic, now + MS) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK_EQ_SIZE(s->action_tail, tail);
        MOQ_TEST_CHECK_EQ_U64(s->last_now_us, at);
        MOQ_TEST_CHECK(!*pending && !*up_ho && !*up_hs);
        expect_dt(*ho, *obj, *hs, *sg, MOQ_VERSION_DRAFT_16, 60000, 60000);
    }
    MOQ_TEST_CHECK(fill_actions(s));
    MOQ_TEST_CHECK(update_timeout(&fx, publication, sub, pub, one, 0, 50,
                                  false, now) == MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK(!*pending && !*up_ho && !*up_hs);
    expect_dt(*ho, *obj, *hs, *sg, MOQ_VERSION_DRAFT_16, 60000, 60000);
    drain_actions(s);
    MOQ_TEST_CHECK(update_timeout(&fx, publication, sub, pub, one, 0, 50,
                                  false, now) == MOQ_OK);
    MOQ_TEST_CHECK(*pending && *up_ho && *up_hs);
    MOQ_TEST_CHECK_EQ_U64(publication ? s->publishes[slot].dt_upd_object_ms
                                     : s->subs[slot].dt_upd_object_ms, 50);
    MOQ_TEST_CHECK_EQ_U64(publication ? s->publishes[slot].dt_upd_subgroup_ms
                                     : s->subs[slot].dt_upd_subgroup_ms, 50);
    expect_dt(*ho, *obj, *hs, *sg, MOQ_VERSION_DRAFT_16, 60000, 60000);
    drain_actions(s);   /* ACK commit is driven explicitly in this unit case. */
    uint64_t ack_id = publication ? s->publishes[slot].update_request_id
                                  : s->subs[slot].update_request_id;
    moq_event_t filler = {0};
    filler.kind = MOQ_EVENT_SETUP_COMPLETE;
    filler.detail_size = (uint32_t)sizeof(moq_setup_complete_event_t);
    for (size_t i = 0; i < s->event_cap && !event_queue_full(s); ++i)
        MOQ_TEST_CHECK(push_event(s, &filler) == MOQ_OK);
    MOQ_TEST_CHECK(event_queue_full(s));
    for (unsigned i = 0; i < 2; ++i) {
        moq_result_t rc = publication
            ? session_core_on_publish_update_ok(s, slot, false, 0, 0, false, 0)
            : session_core_on_subscribe_update_ok(s, slot, false, 0, 0, false, 0);
        MOQ_TEST_CHECK(rc == MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK(*pending && *up_ho && *up_hs);
        expect_dt(*ho, *obj, *hs, *sg, MOQ_VERSION_DRAFT_16, 60000, 60000);
    }
    drain_events(s);
    moq_result_t rc = publication
        ? session_core_on_publish_update_ok(s, slot, false, 0, 0, false, 0)
        : session_core_on_subscribe_update_ok(s, slot, false, 0, 0, false, 0);
    MOQ_TEST_CHECK(rc == MOQ_OK);
    request_registry_remove_by_id(s, ack_id);   /* the d16 codec's post-ACK step */
    MOQ_TEST_CHECK(!*pending && !*up_ho && !*up_hs);
    expect_dt(*ho, *obj, *hs, *sg, MOQ_VERSION_DRAFT_16, 50, 50);
    unsigned acks = 0;
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        if (ev.kind == (publication ? MOQ_EVENT_PUBLICATION_UPDATE_OK
                                    : MOQ_EVENT_SUBSCRIPTION_UPDATE_OK)) ++acks;
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK_EQ_INT((int)acks, 1);

    /* A refused subsequent update must not install its canonical snapshot. */
    MOQ_TEST_CHECK(update_timeout(&fx, publication, sub, pub,
        MOQ_SUBSCRIPTION_UPDATE_OBJECT_DELIVERY_TIMEOUT, 1000, 0, false, now) == MOQ_OK);
    uint64_t failed_id = publication ? s->publishes[slot].update_request_id
                                     : s->subs[slot].update_request_id;
    drain_actions(s);
    uint8_t wire[64];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, wire, sizeof(wire));
    moq_request_error_encode_args_t err = {
        .request_id = failed_id, .error_code = MOQ_REQUEST_ERROR_NOT_SUPPORTED,
    };
    MOQ_TEST_CHECK(s->profile->encode_request_error(s, &w, &err) == MOQ_OK);
    MOQ_TEST_CHECK(moq_session_on_control_bytes(s, wire,
        moq_buf_writer_offset(&w), now) == MOQ_OK);
    MOQ_TEST_CHECK(!*pending && !*up_ho && !*up_hs);
    MOQ_TEST_CHECK(publication ? s->publishes[slot].update_failed
                               : s->subs[slot].update_failed);
    expect_dt(*ho, *obj, *hs, *sg, MOQ_VERSION_DRAFT_16, 50, 50);
    fx_close(&fx);
}

/* Each input is an actual short allocation. Poison only the block under
 * review; its torn form must be absent, its complete form must be consulted.
 * All older blocks keep their existing PR gates and zero/default behavior. */
static void test_accept_short_reader(moq_version_t v)
{
    const size_t ends[] = {
        MOQ_ACCEPT_PUBLISH_CFG_V0_SIZE,
        offsetof(moq_accept_publish_cfg_t, new_group_request) + sizeof(uint64_t),
        offsetof(moq_accept_publish_cfg_t, end_group) + sizeof(uint64_t),
        offsetof(moq_accept_publish_cfg_t, forward) + sizeof(bool),
        offsetof(moq_accept_publish_cfg_t, has_expires) + sizeof(bool),
        offsetof(moq_accept_publish_cfg_t, subgroup_delivery_timeout_ms) + sizeof(uint64_t),
    };
    for (size_t block = 0; block < sizeof(ends) / sizeof(ends[0]); ++block) {
        size_t first = block ? ends[block - 1] : sizeof(uint32_t);
        for (size_t size = first; size <= ends[block]; ++size) {
            bool complete = size == ends[block];
            fx_t fx;
            if (!fx_open(&fx, v)) { MOQ_TEST_CHECK(false); fx_close(&fx); return; }
            moq_publication_t pub, local;
            if (!publish(&fx, &pub, &local)) {
                MOQ_TEST_CHECK(false); fx_close(&fx); return;
            }
            moq_accept_publish_cfg_t cfg;
            moq_accept_publish_cfg_init_sized(&cfg, sizeof(cfg));
            cfg.struct_size = (uint32_t)size;
            if (block == 1) { cfg.has_new_group_request = true; cfg.new_group_request = 1; }
            if (block == 2) cfg.filter = (moq_subscribe_filter_t)UINT32_MAX;
            if (block == 3) { cfg.has_forward = true; cfg.forward = false; }
            if (block == 4) { cfg.has_expires = true; cfg.expires_ms = 1; }
            if (block == 5) { cfg.delivery_timeout_flags = UINT32_MAX; cfg.reserved = 1; }
            void *short_cfg = malloc(size);
            MOQ_TEST_CHECK(short_cfg != NULL);
            if (!short_cfg) { fx_close(&fx); return; }
            memcpy(short_cfg, &cfg, size);
            moq_result_t want = MOQ_OK;
            if (block == 0 && !complete) want = MOQ_ERR_INVAL;
            if (complete) {
                if (block == 1 || block == 2 || block == 5) want = MOQ_ERR_INVAL;
                if (block == 3 && v == MOQ_VERSION_DRAFT_22) want = MOQ_ERR_INVAL;
                if (block == 4 && v != MOQ_VERSION_DRAFT_22) want = MOQ_ERR_UNSUPPORTED;
            }
            uint64_t at = fx.sv->last_now_us;
            size_t tail = fx.sv->action_tail;
            moq_result_t rc = moq_session_accept_publish(fx.sv, local, short_cfg,
                moq_simpair_now_us(fx.sp) + MS);
            MOQ_TEST_CHECK_EQ_INT((int)rc, (int)want);
            MOQ_TEST_CHECK(memcmp(short_cfg, &cfg, size) == 0);
            if (want < 0) {
                /* Dynamic-group permission is checked against the owner
                 * after advancing, as in the PR. Input-only failures are
                 * rejected before advancing. */
                if (block != 1) MOQ_TEST_CHECK_EQ_U64(fx.sv->last_now_us, at);
                MOQ_TEST_CHECK_EQ_SIZE(fx.sv->action_tail, tail);
            } else {
                int slot = pub_resolve_handle(fx.sv, local);
                MOQ_TEST_CHECK(slot >= 0);
                if (slot >= 0) {
                    MOQ_TEST_CHECK(fx.sv->publishes[slot].send_allowed ==
                                   !(block == 3 && complete));
                    MOQ_TEST_CHECK(!fx.sv->publishes[slot].dt_sub_has_object);
                    MOQ_TEST_CHECK(!fx.sv->publishes[slot].dt_sub_has_subgroup);
                }
            }
            free(short_cfg);
            fx_close(&fx);
        }
    }
}

static void test_accept_historical(moq_version_t version)
{
    for (unsigned variant = 0; variant < 3; ++variant) {
        for (unsigned exact = 0; exact < 2; ++exact) {
            fx_t fx;
            if (!fx_open(&fx, version)) { MOQ_TEST_CHECK(false); fx_close(&fx); return; }
            moq_publication_t pub, local;
            if (!publish(&fx, &pub, &local)) { MOQ_TEST_CHECK(false); fx_close(&fx); return; }
            historical_accept_observation_t old;
            int rc = historical_accept_call(fx.sv, local._opaque,
                moq_simpair_now_us(fx.sp), variant, exact != 0, &old);
            moq_result_t want = variant == 1 && version == MOQ_VERSION_DRAFT_22 ? MOQ_ERR_INVAL
                : variant == 2 && version != MOQ_VERSION_DRAFT_22 ? MOQ_ERR_UNSUPPORTED : MOQ_OK;
            MOQ_TEST_CHECK_EQ_INT(rc, (int)want);
            MOQ_TEST_CHECK(old.canary_ok && old.unchanged);
            MOQ_TEST_CHECK_EQ_SIZE(old.floor, MOQ_ACCEPT_PUBLISH_CFG_V0_SIZE);
            MOQ_TEST_CHECK_EQ_SIZE(old.expiry_end,
                offsetof(moq_accept_publish_cfg_t, has_expires) + sizeof(bool));
            MOQ_TEST_CHECK(old.size > old.expiry_end); /* real old sizeof padding */
            MOQ_TEST_CHECK(old.size <= offsetof(moq_accept_publish_cfg_t, delivery_timeout_flags));
            fx_close(&fx);
        }
    }
}

static void test_d22_update_precedence(bool publication)
{
    /* Whole, torn and actual-old allocations; generic zero must take the
     * same conflict/unsupported precedence as a nonzero generic timeout. */
    const struct { bool generic; uint64_t generic_us, typed_ms; uint32_t flags;
        unsigned extent; moq_result_t want; } rows[] = {
        {true, 0, 50, 1, 0, MOQ_ERR_INVAL},
        {true, 1000, 0, 1, 0, MOQ_ERR_INVAL},
        {true, 1000, 50, 3, 0, MOQ_ERR_INVAL},
        {true, 0, 0, 0, 0, MOQ_ERR_UNSUPPORTED},
        {true, 1000, 0, 0, 0, MOQ_ERR_UNSUPPORTED},
        {true, 0, 50, 1, 1, MOQ_ERR_UNSUPPORTED},
        {true, 1000, 50, 1, 1, MOQ_ERR_UNSUPPORTED},
        {true, 0, 50, 1, 2, MOQ_ERR_UNSUPPORTED},
        {true, 1000, 50, 1, 2, MOQ_ERR_UNSUPPORTED},
        {true, 1000, 50, 4, 0, MOQ_ERR_INVAL},
        {true, 1000, 50, 4, 1, MOQ_ERR_UNSUPPORTED},
        {true, 0, 50, 1, 3, MOQ_ERR_INVAL},
        {true, 0, 50, 1, 4, MOQ_ERR_UNSUPPORTED},
        {false, 0, 0, 1, 0, MOQ_OK},
        {false, 0, 50, 2, 0, MOQ_OK},
    };
    for (size_t i = 0; i < sizeof(rows) / sizeof(rows[0]); ++i) {
        fx_t fx;
        if (!fx_open(&fx, MOQ_VERSION_DRAFT_22)) { MOQ_TEST_CHECK(false); fx_close(&fx); return; }
        moq_subscription_t sub = MOQ_SUBSCRIPTION_INVALID, remote_sub = MOQ_SUBSCRIPTION_INVALID;
        moq_publication_t pub = MOQ_PUBLICATION_INVALID, remote_pub = MOQ_PUBLICATION_INVALID;
        moq_session_t *s = publication ? fx.sv : fx.c;
        if (publication) {
            if (!publish(&fx, &remote_pub, &pub)) { MOQ_TEST_CHECK(false); fx_close(&fx); return; }
            moq_accept_publish_cfg_t ac;
            moq_accept_publish_cfg_init_sized(&ac, sizeof(ac));
            MOQ_TEST_CHECK(moq_session_accept_publish(s, pub, &ac, moq_simpair_now_us(fx.sp)) == MOQ_OK);
            run(&fx); drain_events(fx.c); drain_events(fx.sv);
        } else {
            moq_subscribe_cfg_t cfg;
            subscribe_cfg(&cfg, "precedence");
            if (!subscribe_accept(&fx, &cfg, NULL, 0, &remote_sub, &sub)) {
                MOQ_TEST_CHECK(false); fx_close(&fx); return;
            }
        }
        moq_subscription_update_cfg_t su;
        moq_publication_update_cfg_t pu;
        moq_subscription_update_cfg_init_sized(&su, sizeof(su));
        moq_publication_update_cfg_init_sized(&pu, sizeof(pu));
        su.has_delivery_timeout = pu.has_delivery_timeout = rows[i].generic;
        su.delivery_timeout_us = pu.delivery_timeout_us = rows[i].generic_us;
        su.delivery_timeout_flags = pu.delivery_timeout_flags = rows[i].flags;
        su.reserved = pu.reserved = rows[i].extent >= 3 ? 1 : 0;
        su.object_delivery_timeout_ms = pu.object_delivery_timeout_ms = rows[i].typed_ms;
        su.subgroup_delivery_timeout_ms = pu.subgroup_delivery_timeout_ms = rows[i].typed_ms;
        size_t size = publication ? sizeof(pu) : sizeof(su);
        if (rows[i].extent == 1 || rows[i].extent == 4) size = publication
            ? offsetof(moq_publication_update_cfg_t, subgroup_delivery_timeout_ms) + sizeof(uint64_t) - 1
            : offsetof(moq_subscription_update_cfg_t, subgroup_delivery_timeout_ms) + sizeof(uint64_t) - 1;
        if (rows[i].extent == 2) size = publication
            ? offsetof(moq_publication_update_cfg_t, auth_tokens)
            : offsetof(moq_subscription_update_cfg_t, auth_tokens);
        su.struct_size = pu.struct_size = (uint32_t)size;
        void *input = malloc(size);
        size_t state_size = s->profile->state_size;
        void *profile_before = malloc(state_size ? state_size : 1);
        if (!input || !profile_before) {
            MOQ_TEST_CHECK(false); free(input); free(profile_before); fx_close(&fx); return;
        }
        memcpy(input, publication ? (void *)&pu : (void *)&su, size);
        memcpy(profile_before, s->profile_state, state_size);
        uint64_t at = s->last_now_us, epoch = s->borrow_epoch, ref = s->next_stream_ref;
        size_t head = s->action_head, tail = s->action_tail, bytes = s->send_len;
        moq_result_t rc = publication
            ? moq_session_update_publication(s, pub, input, at + MS)
            : moq_session_update_subscription(s, sub, input, at + MS);
        MOQ_TEST_CHECK_EQ_INT((int)rc, (int)rows[i].want);
        MOQ_TEST_CHECK(memcmp(input, publication ? (void *)&pu : (void *)&su, size) == 0);
        if (rows[i].want < 0) {
            MOQ_TEST_CHECK(s->last_now_us == at && s->borrow_epoch == epoch);
            MOQ_TEST_CHECK(s->next_stream_ref == ref && s->action_head == head &&
                           s->action_tail == tail && s->send_len == bytes);
            MOQ_TEST_CHECK(memcmp(profile_before, s->profile_state, state_size) == 0);
            int slot = publication ? pub_resolve_handle(s, pub) : sub_resolve_handle(s, sub);
            MOQ_TEST_CHECK(slot >= 0);
            if (slot >= 0) MOQ_TEST_CHECK(publication
                ? !s->publishes[slot].update_pending && s->publishes[slot].update_request_id == 0
                : !s->subs[slot].update_pending && s->subs[slot].update_request_id == 0);
        } else {
            int slot = publication ? pub_resolve_handle(s, pub) : sub_resolve_handle(s, sub);
            MOQ_TEST_CHECK(slot >= 0);
            if (slot >= 0) {
                MOQ_TEST_CHECK(publication ? s->publishes[slot].update_pending : s->subs[slot].update_pending);
                MOQ_TEST_CHECK(publication ? !s->publishes[slot].dt_sub_has_object &&
                    !s->publishes[slot].dt_sub_has_subgroup : !s->subs[slot].dt_sub_has_object &&
                    !s->subs[slot].dt_sub_has_subgroup);
            }
            run(&fx);
            for (unsigned peer = 0; peer < 2; ++peer) {
                moq_session_t *endpoint = peer ? (publication ? fx.c : fx.sv) : s;
                int target = publication ? pub_resolve_handle(endpoint, peer ? remote_pub : pub)
                    : sub_resolve_handle(endpoint, peer ? remote_sub : sub);
                MOQ_TEST_CHECK(target >= 0);
                if (target < 0) continue;
                bool has_object = publication ? endpoint->publishes[target].dt_sub_has_object
                    : endpoint->subs[target].dt_sub_has_object;
                bool has_subgroup = publication ? endpoint->publishes[target].dt_sub_has_subgroup
                    : endpoint->subs[target].dt_sub_has_subgroup;
                MOQ_TEST_CHECK(has_object == ((rows[i].flags & 1) != 0));
                MOQ_TEST_CHECK(has_subgroup == ((rows[i].flags & 2) != 0));
                if (has_object) MOQ_TEST_CHECK_EQ_U64(publication
                    ? endpoint->publishes[target].dt_sub_object_ms : endpoint->subs[target].dt_sub_object_ms,
                    rows[i].typed_ms);
                if (has_subgroup) MOQ_TEST_CHECK_EQ_U64(publication
                    ? endpoint->publishes[target].dt_sub_subgroup_ms : endpoint->subs[target].dt_sub_subgroup_ms,
                    rows[i].typed_ms);
            }
        }
        free(profile_before); free(input); fx_close(&fx);
    }
}

int main(void)
{
    g_part = MOQ_BYTES_LITERAL("live");
    test_builder_initializer();
    for (size_t i = 0; i < N_VERSIONS; i++) {
        test_subscribe(k_versions[i]);
        test_update(k_versions[i]);
        test_track_properties(k_versions[i]);
        test_builder_resources(k_versions[i]);
        test_accept_short_reader(k_versions[i]);
        test_accept_historical(k_versions[i]);
    }
    test_publish_ok(MOQ_VERSION_DRAFT_16);
    test_publish_ok(MOQ_VERSION_DRAFT_18);
    test_publish_ok_d22();
    test_d16_single_value();
    test_d16_requester_state(false, true);
    test_d16_requester_state(false, false);
    test_d16_requester_state(true, true);
    test_d16_requester_state(true, false);
    test_d16_decreased_terminal_wait();
    test_d16_update_transaction(false);
    test_d16_update_transaction(true);
    test_d22_update_precedence(false);
    test_d22_update_precedence(true);
    if (failures) {
        fprintf(stderr, "test_delivery_timeout_params: %d failure(s)\n", failures);
        return 1;
    }
    printf("test_delivery_timeout_params: all passed\n");
    return 0;
}
