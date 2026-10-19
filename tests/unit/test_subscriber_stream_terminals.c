/*
 * Subscriber facade stream terminals (moq_sub_cfg_t.surface_stream_terminals,
 * moq_sub_poll_stream_terminal), on drafts 16, 18 and 22.
 *
 * Invariants pinned here:
 *   - every subgroup stream terminal of an active track yields exactly one
 *     record, never coalesced: FIN with END_OF_GROUP, plain FIN and RESET,
 *     each with the last Object the stream carried (completely, for RESET)
 *     and the reset's code;
 *   - a record is pollable only after every object queued before it;
 *   - a full lane holds the tick at WOULD_BLOCK and loses nothing;
 *   - with the option off, or outside a caller's struct_size, nothing is
 *     queued and release behaves as before;
 *   - on every draft a track whose record is still queued is not released.
 */
#include <moq/subscriber.h>
#include <moq/sim.h>
#include <moq/publisher.h>
#include "test_session_support.h"
#include <stdio.h>
#include <string.h>

static int failures = 0;

typedef struct {
    test_alloc_state_t as;
    moq_alloc_t        alloc;
    moq_simpair_t     *sp;
    moq_publisher_t   *pub;
    moq_pub_track_t   *ptrack;
    moq_subscriber_t  *sub;
    moq_sub_track_t   *track;
} fx_t;

static void fx_setup(fx_t *f, moq_version_t ver, bool lane,
                     int cfg_generation, uint32_t max_terminals)
{
    memset(f, 0, sizeof(*f));
    f->alloc = test_allocator(&f->as);
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = &f->alloc; cfg.seed = 42; cfg.initial_now_us = 1000;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 16;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = 16;
    cfg.version = ver;
    moq_simpair_create(&cfg, &f->sp);
    moq_simpair_start(f->sp);
    moq_simpair_run_until_quiescent(f->sp, 8, NULL);
    moq_event_t ev;
    if (moq_session_poll_events(moq_simpair_client(f->sp), &ev, 1) == 1)
        moq_event_cleanup(&ev);
    if (moq_session_poll_events(moq_simpair_server(f->sp), &ev, 1) == 1)
        moq_event_cleanup(&ev);

    moq_pub_cfg_t pcfg; moq_pub_cfg_init_sized(&pcfg, sizeof(pcfg));
    pcfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    moq_pub_create(moq_simpair_server(f->sp), &f->alloc, &pcfg, &f->pub);
    moq_pub_track_cfg_t ptcfg; moq_pub_track_cfg_init(&ptcfg);
    static moq_bytes_t ns[] = { { (const uint8_t *)"test", 4 } };
    ptcfg.track_namespace.parts = ns;
    ptcfg.track_namespace.count = 1;
    ptcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_pub_add_track(f->pub, &ptcfg, moq_simpair_now_us(f->sp), &f->ptrack);

    moq_sub_cfg_t scfg;
    if (cfg_generation == 1) {
        moq_sub_cfg_init(&scfg);
    } else {
        moq_sub_cfg_init_sized(&scfg, sizeof(scfg));
        scfg.surface_stream_terminals = lane;
        scfg.max_stream_terminals = max_terminals;
    }
    if (cfg_generation == 2) {
        /* A caller whose struct ends at max_fill_items: the enabled lane
         * outside its size is never read. */
        scfg.struct_size = (uint32_t)(offsetof(moq_sub_cfg_t, max_fill_items) +
                                      sizeof(uint32_t));
        scfg.surface_stream_terminals = true;
    }
    MOQ_TEST_CHECK(moq_sub_create(moq_simpair_client(f->sp), &f->alloc,
                                  &scfg, &f->sub) == MOQ_OK);

    moq_sub_track_cfg_t tcfg; moq_sub_track_cfg_init(&tcfg);
    tcfg.track_namespace.parts = ns;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    MOQ_TEST_CHECK(moq_sub_subscribe(f->sub, &tcfg, moq_simpair_now_us(f->sp),
                                     &f->track) == MOQ_OK);
    moq_simpair_run_until_quiescent(f->sp, 8, NULL);
    moq_pub_tick(f->pub, moq_simpair_now_us(f->sp));
    moq_simpair_run_until_quiescent(f->sp, 8, NULL);
    moq_sub_tick(f->sub, moq_simpair_now_us(f->sp));
    MOQ_TEST_CHECK(moq_sub_track_is_active(f->track));
}

static void fx_teardown(fx_t *f)
{
    moq_sub_destroy(f->sub);
    moq_pub_destroy(f->pub);
    moq_event_t ev;
    while (moq_session_poll_events(moq_simpair_server(f->sp), &ev, 1) == 1)
        moq_event_cleanup(&ev);
    while (moq_session_poll_events(moq_simpair_client(f->sp), &ev, 1) == 1)
        moq_event_cleanup(&ev);
    moq_simpair_destroy(f->sp);
    MOQ_TEST_CHECK(f->as.balance == 0);
}

static void write_obj(fx_t *f, uint64_t g, uint64_t o, bool eog)
{
    moq_rcbuf_t *pay = NULL;
    uint8_t b[4] = { (uint8_t)g, (uint8_t)o, 0xAB, 0xCD };
    moq_rcbuf_create(&f->alloc, b, sizeof(b), &pay);
    moq_pub_object_cfg_t oc; moq_pub_object_cfg_init_sized(&oc, sizeof(oc));
    oc.group_id = g;
    oc.object_id = o;
    oc.payload = pay;
    oc.end_of_group = eog;
    MOQ_TEST_CHECK(moq_pub_write_object_ex(f->pub, f->ptrack, &oc,
                                           moq_simpair_now_us(f->sp)) == MOQ_OK);
    moq_rcbuf_decref(pay);
}

static void deliver(fx_t *f)
{
    moq_simpair_run_until_quiescent(f->sp, 16, NULL);
}

static bool poll_obj(fx_t *f, uint64_t g, uint64_t o)
{
    moq_sub_object_t obj;
    if (moq_sub_poll_object(f->sub, &obj) != MOQ_OK) return false;
    bool ok = obj.track == f->track && obj.group_id == g && obj.object_id == o;
    moq_sub_object_cleanup(&obj);
    return ok;
}

static const char *pname(moq_version_t v)
{
    return v == MOQ_VERSION_DRAFT_22 ? "d22"
         : v == MOQ_VERSION_DRAFT_18 ? "d18" : "d16";
}

/* Poll the next terminal into a poisoned record. */
static moq_result_t poll_st(fx_t *f, moq_sub_stream_terminal_t *st)
{
    memset(st, 0xEE, sizeof(*st));
    return moq_sub_poll_stream_terminal(f->sub, st, sizeof(*st));
}

static void drain_objects(fx_t *f)
{
    moq_sub_object_t obj;
    while (moq_sub_poll_object(f->sub, &obj) == MOQ_OK)
        moq_sub_object_cleanup(&obj);
}

static void end_group(fx_t *f)
{
    MOQ_TEST_CHECK(moq_pub_end_group(f->pub, f->ptrack,
                                     moq_simpair_now_us(f->sp)) == MOQ_OK);
}

/* A FIN with END_OF_GROUP and a plain FIN, each behind its objects. */
static void test_fin_records(moq_version_t ver)
{
    int before = failures;
    fx_t f;
    fx_setup(&f, ver, true, 0, 0);

    write_obj(&f, 0, 0, true);
    write_obj(&f, 0, 2, true);
    end_group(&f);
    write_obj(&f, 1, 0, false);
    write_obj(&f, 1, 1, false);
    end_group(&f);
    deliver(&f);
    MOQ_TEST_CHECK(moq_sub_tick(f.sub, moq_simpair_now_us(f.sp)) == MOQ_OK);

    moq_sub_stream_terminal_t st;
    MOQ_TEST_CHECK(poll_st(&f, &st) == MOQ_DONE);
    MOQ_TEST_CHECK(poll_obj(&f, 0, 0));
    MOQ_TEST_CHECK(poll_st(&f, &st) == MOQ_DONE);
    MOQ_TEST_CHECK(poll_obj(&f, 0, 2));
    MOQ_TEST_CHECK(poll_st(&f, &st) == MOQ_OK);
    MOQ_TEST_CHECK(st.struct_size == sizeof(st));
    MOQ_TEST_CHECK(st.kind == MOQ_SUB_STREAM_FIN_END_OF_GROUP);
    MOQ_TEST_CHECK(st.track == f.track);
    MOQ_TEST_CHECK_EQ_U64(st.group_id, 0);
    MOQ_TEST_CHECK_EQ_U64(st.subgroup_id, 0);
    MOQ_TEST_CHECK(st.has_last_object && !st.has_cut_object);
    MOQ_TEST_CHECK_EQ_U64(st.last_object_id, 2);
    MOQ_TEST_CHECK(poll_st(&f, &st) == MOQ_DONE);
    MOQ_TEST_CHECK(poll_obj(&f, 1, 0));
    MOQ_TEST_CHECK(poll_obj(&f, 1, 1));
    MOQ_TEST_CHECK(poll_st(&f, &st) == MOQ_OK);
    MOQ_TEST_CHECK(st.kind == MOQ_SUB_STREAM_FIN);
    MOQ_TEST_CHECK_EQ_U64(st.group_id, 1);
    MOQ_TEST_CHECK(st.has_last_object);
    MOQ_TEST_CHECK_EQ_U64(st.last_object_id, 1);
    MOQ_TEST_CHECK(poll_st(&f, &st) == MOQ_DONE);

    MOQ_TEST_CHECK(moq_sub_poll_stream_terminal(NULL, &st, sizeof(st)) ==
                   MOQ_ERR_INVAL);
    MOQ_TEST_CHECK(moq_sub_poll_stream_terminal(f.sub, NULL, sizeof(st)) ==
                   MOQ_ERR_INVAL);

    fx_teardown(&f);
    if (failures == before)
        printf("PASS: stream_terminal_fin_records_%s\n", pname(ver));
}

/* Two subgroups reset with their objects delivered: two records, in
 * order, each with its own last Object and code. */
static void test_reset_records(moq_version_t ver)
{
    int before = failures;
    fx_t f;
    fx_setup(&f, ver, true, 0, 0);

    write_obj(&f, 3, 0, false);
    write_obj(&f, 3, 1, false);
    MOQ_TEST_CHECK(moq_pub_reset_group(f.pub, f.ptrack, 0x10,
                                       moq_simpair_now_us(f.sp)) == MOQ_OK);
    write_obj(&f, 4, 0, false);
    MOQ_TEST_CHECK(moq_pub_reset_group(f.pub, f.ptrack, 0x11,
                                       moq_simpair_now_us(f.sp)) == MOQ_OK);
    deliver(&f);
    moq_sub_tick(f.sub, moq_simpair_now_us(f.sp));
    drain_objects(&f);

    moq_sub_stream_terminal_t st;
    MOQ_TEST_CHECK(poll_st(&f, &st) == MOQ_OK);
    MOQ_TEST_CHECK(st.kind == MOQ_SUB_STREAM_RESET);
    MOQ_TEST_CHECK_EQ_U64(st.group_id, 3);
    MOQ_TEST_CHECK_EQ_U64(st.error_code, 0x10);
    MOQ_TEST_CHECK(st.has_last_object && !st.has_cut_object);
    MOQ_TEST_CHECK_EQ_U64(st.last_object_id, 1);
    MOQ_TEST_CHECK(poll_st(&f, &st) == MOQ_OK);
    MOQ_TEST_CHECK(st.kind == MOQ_SUB_STREAM_RESET);
    MOQ_TEST_CHECK_EQ_U64(st.group_id, 4);
    MOQ_TEST_CHECK_EQ_U64(st.error_code, 0x11);
    MOQ_TEST_CHECK_EQ_U64(st.last_object_id, 0);
    MOQ_TEST_CHECK(poll_st(&f, &st) == MOQ_DONE);

    fx_teardown(&f);
    if (failures == before)
        printf("PASS: stream_terminal_reset_records_%s\n", pname(ver));
}

/* A one-record lane: the second terminal is held, the tick reports
 * WOULD_BLOCK, and polling lets it in, in order. */
static void test_full_lane(moq_version_t ver)
{
    int before = failures;
    fx_t f;
    fx_setup(&f, ver, true, 0, 1);

    for (uint64_t g = 0; g < 2; g++) {
        write_obj(&f, g, 0, false);
        end_group(&f);
    }
    deliver(&f);
    MOQ_TEST_CHECK(moq_sub_tick(f.sub, moq_simpair_now_us(f.sp)) ==
                   MOQ_ERR_WOULD_BLOCK);
    drain_objects(&f);
    MOQ_TEST_CHECK(moq_sub_tick(f.sub, moq_simpair_now_us(f.sp)) ==
                   MOQ_ERR_WOULD_BLOCK);
    moq_sub_stream_terminal_t st;
    MOQ_TEST_CHECK(poll_st(&f, &st) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(st.group_id, 0);
    MOQ_TEST_CHECK(poll_st(&f, &st) == MOQ_DONE);
    MOQ_TEST_CHECK(moq_sub_tick(f.sub, moq_simpair_now_us(f.sp)) == MOQ_OK);
    MOQ_TEST_CHECK(poll_st(&f, &st) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(st.group_id, 1);
    MOQ_TEST_CHECK(poll_st(&f, &st) == MOQ_DONE);

    fx_teardown(&f);
    if (failures == before)
        printf("PASS: stream_terminal_full_lane_%s\n", pname(ver));
}

/* The option off, through a v0 cfg, or outside the caller's struct_size:
 * nothing is queued, and a finished track releases at once. */
static void test_option_off(moq_version_t ver, int cfg_generation)
{
    int before = failures;
    fx_t f;
    fx_setup(&f, ver, false, cfg_generation, 0);

    write_obj(&f, 0, 0, true);
    end_group(&f);
    deliver(&f);
    MOQ_TEST_CHECK(moq_sub_tick(f.sub, moq_simpair_now_us(f.sp)) == MOQ_OK);
    drain_objects(&f);
    moq_sub_stream_terminal_t st;
    MOQ_TEST_CHECK(poll_st(&f, &st) == MOQ_DONE);
    MOQ_TEST_CHECK(moq_sub_unsubscribe(f.sub, f.track,
                                       moq_simpair_now_us(f.sp)) == MOQ_OK);
    deliver(&f);
    moq_sub_tick(f.sub, moq_simpair_now_us(f.sp));
    MOQ_TEST_CHECK(moq_sub_release_track(f.sub, f.track) == MOQ_OK);

    fx_teardown(&f);
    if (failures == before)
        printf("PASS: stream_terminal_option_off_%d_%s\n", cfg_generation,
               pname(ver));
}

/* A short record is refused without dequeuing. */
static void test_short_poll(moq_version_t ver)
{
    int before = failures;
    fx_t f;
    fx_setup(&f, ver, true, 0, 0);
    write_obj(&f, 0, 0, false);
    end_group(&f);
    deliver(&f);
    moq_sub_tick(f.sub, moq_simpair_now_us(f.sp));
    drain_objects(&f);
    moq_sub_stream_terminal_t st;
    MOQ_TEST_CHECK(moq_sub_poll_stream_terminal(f.sub, &st,
                       MOQ_SUB_STREAM_TERMINAL_V0_SIZE - 1) ==
                   MOQ_ERR_ABI_MISMATCH);
    MOQ_TEST_CHECK(poll_st(&f, &st) == MOQ_OK);
    MOQ_TEST_CHECK(st.kind == MOQ_SUB_STREAM_FIN);
    fx_teardown(&f);
    if (failures == before)
        printf("PASS: stream_terminal_short_poll_%s\n", pname(ver));
}

/* On every draft, a queued record holds a finished track's slot until it
 * is polled. */
static void test_release_with_queued_terminal(moq_version_t ver)
{
    int before = failures;
    fx_t f;
    fx_setup(&f, ver, true, 0, 0);

    write_obj(&f, 0, 0, true);
    end_group(&f);
    deliver(&f);
    MOQ_TEST_CHECK(moq_sub_tick(f.sub, moq_simpair_now_us(f.sp)) == MOQ_OK);
    drain_objects(&f);
    MOQ_TEST_CHECK(moq_sub_unsubscribe(f.sub, f.track,
                                       moq_simpair_now_us(f.sp)) == MOQ_OK);
    deliver(&f);
    moq_sub_tick(f.sub, moq_simpair_now_us(f.sp));

    MOQ_TEST_CHECK(moq_sub_release_track(f.sub, f.track) == MOQ_ERR_WRONG_STATE);
    moq_sub_stream_terminal_t st;
    MOQ_TEST_CHECK(poll_st(&f, &st) == MOQ_OK);
    MOQ_TEST_CHECK(st.track == f.track);
    MOQ_TEST_CHECK(moq_sub_release_track(f.sub, f.track) == MOQ_OK);

    fx_teardown(&f);
    if (failures == before)
        printf("PASS: stream_terminal_release_%s\n", pname(ver));
}

/* Streaming mode: a reset that cuts Object 1's payload short closes the
 * stream through that object's terminal chunk; the record follows the chunk
 * and names the cut Object. Raw sessions, so half a payload can be fed. */
static void test_cut_object_record(void)
{
    int before = failures;
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    moq_session_cfg_t cx = MOQ_SESSION_CFG_INIT;
    cx.streaming_objects = true;
    moq_session_t *c = NULL, *sv = NULL;
    establish_pair(&alloc, 10, 10, &c, &sv, &cx, NULL);

    moq_sub_cfg_t scfg;
    moq_sub_cfg_init_sized(&scfg, sizeof(scfg));
    scfg.streaming_objects = true;
    scfg.surface_stream_terminals = true;
    moq_subscriber_t *sub = NULL;
    MOQ_TEST_CHECK(moq_sub_create(c, &alloc, &scfg, &sub) == MOQ_OK);
    static moq_bytes_t ns[] = { { (const uint8_t *)"live", 4 } };
    moq_sub_track_cfg_t tcfg;
    moq_sub_track_cfg_init(&tcfg);
    tcfg.track_namespace.parts = ns;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_sub_track_t *track = NULL;
    MOQ_TEST_CHECK(moq_sub_subscribe(sub, &tcfg, 0, &track) == MOQ_OK);
    pump_actions_to_peer(c, sv, 0);
    moq_event_t ev;
    MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
    MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
    moq_subscription_t ssub = ev.u.subscribe_request.sub;
    moq_accept_subscribe_cfg_t acc;
    moq_accept_subscribe_cfg_init(&acc);
    MOQ_TEST_CHECK(moq_session_accept_subscribe(sv, ssub, &acc, 0) == MOQ_OK);
    pump_actions_to_peer(sv, c, 0);
    MOQ_TEST_CHECK(moq_sub_tick(sub, 0) == MOQ_OK);
    MOQ_TEST_CHECK(moq_sub_track_is_active(track));

    moq_subgroup_cfg_t sc;
    moq_subgroup_cfg_init(&sc);
    sc.group_id = 2;
    moq_subgroup_handle_t sg;
    MOQ_TEST_CHECK(moq_session_open_subgroup(sv, ssub, &sc, 0, &sg) == MOQ_OK);
    for (uint64_t id = 0; id < 2; id++) {
        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t *)"abcd", 4, &p);
        MOQ_TEST_CHECK(moq_session_write_object(sv, sg, id, p, 0) == MOQ_OK);
        moq_rcbuf_decref(p);
    }
    /* Everything but the last two payload bytes of Object 1. */
    moq_stream_ref_t ref = moq_stream_ref_from_u64(0xC1);
    moq_action_t a[16];
    size_t n = 0, got;
    while (n < 16 && (got = moq_session_poll_actions(sv, a + n, 16 - n)) > 0)
        n += got;
    size_t last = n;
    for (size_t i = 0; i < n; i++)
        if (a[i].kind == MOQ_ACTION_SEND_DATA) last = i;
    for (size_t i = 0; i < n; i++) {
        if (a[i].kind == MOQ_ACTION_SEND_DATA) {
            const moq_action_t *d = &a[i];
            if (d->u.send_data.header_len > 0)
                moq_session_on_data_bytes(c, ref, d->u.send_data.header,
                                          d->u.send_data.header_len, false, 0);
            if (d->u.send_data.payload) {
                size_t len = moq_rcbuf_len(d->u.send_data.payload);
                if (i == last) len -= 2;
                moq_session_on_data_bytes(c, ref,
                    moq_rcbuf_data(d->u.send_data.payload), len, false, 0);
            }
        }
        moq_action_cleanup(&a[i]);
    }
    MOQ_TEST_CHECK(moq_session_reset_subgroup(sv, sg, 0, 0) == MOQ_OK);
    while ((n = moq_session_poll_actions(sv, a, 16)) > 0)
        for (size_t i = 0; i < n; i++) moq_action_cleanup(&a[i]);
    MOQ_TEST_CHECK(moq_session_on_data_reset(c, ref, 0x21, 0) == MOQ_OK);
    MOQ_TEST_CHECK(moq_sub_tick(sub, 0) == MOQ_OK);

    /* Held until the cut Object's terminal chunk is polled. */
    moq_sub_stream_terminal_t st;
    moq_sub_chunk_t ck;
    bool saw_cut = false;
    for (;;) {
        MOQ_TEST_CHECK(moq_sub_poll_stream_terminal(sub, &st, sizeof(st)) ==
                       MOQ_DONE);
        if (moq_sub_poll_chunk(sub, &ck) != MOQ_OK) break;
        if (ck.end && ck.terminal == MOQ_OBJECT_TERMINAL_RESET)
            saw_cut = ck.object_id == 1;
        moq_sub_chunk_cleanup(&ck);
        if (saw_cut) break;
    }
    MOQ_TEST_CHECK(saw_cut);
    memset(&st, 0xEE, sizeof(st));
    MOQ_TEST_CHECK(moq_sub_poll_stream_terminal(sub, &st, sizeof(st)) == MOQ_OK);
    MOQ_TEST_CHECK(st.kind == MOQ_SUB_STREAM_RESET);
    MOQ_TEST_CHECK(st.track == track);
    MOQ_TEST_CHECK_EQ_U64(st.group_id, 2);
    MOQ_TEST_CHECK_EQ_U64(st.error_code, 0x21);
    MOQ_TEST_CHECK(st.has_cut_object && !st.has_last_object);
    MOQ_TEST_CHECK_EQ_U64(st.cut_object_id, 1);
    MOQ_TEST_CHECK(moq_sub_poll_stream_terminal(sub, &st, sizeof(st)) == MOQ_DONE);

    moq_sub_destroy(sub);
    moq_session_destroy(c);
    moq_session_destroy(sv);
    MOQ_TEST_CHECK(as.balance == 0);
    if (failures == before)
        printf("PASS: stream_terminal_cut_object\n");
}

int main(void)
{
    const moq_version_t vers[] = { MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18,
                                   MOQ_VERSION_DRAFT_22 };
    for (size_t vi = 0; vi < 3; vi++) {
        moq_version_t ver = vers[vi];
        test_fin_records(ver);
        test_reset_records(ver);
        test_full_lane(ver);
        test_option_off(ver, 0);
        test_option_off(ver, 1);
        test_option_off(ver, 2);
        test_short_poll(ver);
        test_release_with_queued_terminal(ver);
    }
    test_cut_object_record();
    if (failures == 0) {
        printf("PASS: all subscriber stream-terminal tests\n");
        return 0;
    }
    printf("FAIL: %d subscriber stream-terminal test failures\n", failures);
    return 1;
}
