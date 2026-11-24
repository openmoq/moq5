/*
 * Subscriber facade Group ends (moq_sub_cfg_t.surface_group_ends,
 * moq_sub_poll_group_end), on drafts 16, 18 and 22.
 *
 * Invariants pinned here:
 *   - a subgroup whose header carried END_OF_GROUP and that ends with a FIN
 *     yields exactly one Group end, naming its track, group and subgroup;
 *   - on draft 22 the same subgroup also yields a FIN_END_OF_GROUP stream
 *     terminal naming the Group's largest Object, and a FIN without the bit
 *     yields a plain FIN terminal and no Group end;
 *   - that Group end is pollable only after every object queued before it
 *     has been polled;
 *   - a FIN without END_OF_GROUP and a reset subgroup yield none;
 *   - with the option off (including a v0-sized cfg) nothing is queued;
 *   - a full Group-end queue holds the tick at WOULD_BLOCK and loses nothing;
 *   - on draft 22 a finished track whose Group end is still queued is not
 *     released (its slot could otherwise be reused under the queued record);
 *     drafts 16 and 18 release it as before.
 */
#include <moq/subscriber.h>
#include <moq/sim.h>
#include <moq/publisher.h>
#include "test_session_support.h"
#include <stdio.h>
#include <string.h>

#include "subscriber_group_end_layout.h"

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

static void fx_setup_lanes(fx_t *f, moq_version_t ver, bool group_ends,
                           bool stream_terminals, int cfg_generation,
                           uint32_t max_objects)
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
        scfg.surface_group_ends = group_ends;
        scfg.surface_stream_terminals = stream_terminals;
    }
    if (cfg_generation == 2) {
        /* Main's full pre-TS sizeof, with poisoned old tail padding and an
         * enabled new field outside that size. Neither may opt into TS. */
        scfg.struct_size = sizeof(sub_cfg_before_ts_t);
        size_t end = offsetof(sub_cfg_before_ts_t, on_update_ok) +
                     sizeof(((sub_cfg_before_ts_t *)0)->on_update_ok);
        memset((unsigned char *)&scfg + end, 0xA5,
               sizeof(sub_cfg_before_ts_t) - end);
        scfg.surface_group_ends = true;
    }
    scfg.max_objects = max_objects;
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

static void fx_setup(fx_t *f, moq_version_t ver, bool group_ends,
                     int cfg_generation, uint32_t max_objects)
{
    fx_setup_lanes(f, ver, group_ends, false, cfg_generation, max_objects);
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

/* The Group end follows the subgroup's objects and waits for them. */
static void test_fin_with_end_of_group(moq_version_t ver)
{
    int before = failures;
    fx_t f;
    fx_setup(&f, ver, true, false, 0);

    write_obj(&f, 0, 0, true);
    write_obj(&f, 0, 1, true);
    MOQ_TEST_CHECK(moq_pub_end_group(f.pub, f.ptrack,
                                     moq_simpair_now_us(f.sp)) == MOQ_OK);
    deliver(&f);
    MOQ_TEST_CHECK(moq_sub_tick(f.sub, moq_simpair_now_us(f.sp)) == MOQ_OK);

    moq_sub_group_end_t ge;
    MOQ_TEST_CHECK(moq_sub_poll_group_end(f.sub, &ge) == MOQ_DONE);
    MOQ_TEST_CHECK(poll_obj(&f, 0, 0));
    MOQ_TEST_CHECK(moq_sub_poll_group_end(f.sub, &ge) == MOQ_DONE);
    MOQ_TEST_CHECK(poll_obj(&f, 0, 1));
    memset(&ge, 0xEE, sizeof(ge));
    MOQ_TEST_CHECK(moq_sub_poll_group_end(f.sub, &ge) == MOQ_OK);
    MOQ_TEST_CHECK(ge.track == f.track);
    MOQ_TEST_CHECK_EQ_U64(ge.group_id, 0);
    MOQ_TEST_CHECK_EQ_U64(ge.subgroup_id, 0);
    MOQ_TEST_CHECK(moq_sub_poll_group_end(f.sub, &ge) == MOQ_DONE);

    /* A second Group on its own subgroup: the end names that Group. */
    write_obj(&f, 1, 0, true);
    MOQ_TEST_CHECK(moq_pub_end_group(f.pub, f.ptrack,
                                     moq_simpair_now_us(f.sp)) == MOQ_OK);
    deliver(&f);
    moq_sub_tick(f.sub, moq_simpair_now_us(f.sp));
    MOQ_TEST_CHECK(poll_obj(&f, 1, 0));
    MOQ_TEST_CHECK(moq_sub_poll_group_end(f.sub, &ge) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(ge.group_id, 1);
    MOQ_TEST_CHECK(moq_sub_poll_group_end(f.sub, &ge) == MOQ_DONE);

    MOQ_TEST_CHECK(moq_sub_poll_group_end(NULL, &ge) == MOQ_ERR_INVAL);
    MOQ_TEST_CHECK(moq_sub_poll_group_end(f.sub, NULL) == MOQ_ERR_INVAL);

    fx_teardown(&f);
    if (failures == before)
        printf("PASS: group_end_fin_with_end_of_group_%s\n", pname(ver));
}

/* A FIN on a subgroup without END_OF_GROUP, and a reset subgroup, say
 * nothing about where the Group ends. */
static void test_no_end_without_bit_or_on_reset(moq_version_t ver)
{
    int before = failures;
    fx_t f;
    fx_setup(&f, ver, true, false, 0);

    write_obj(&f, 0, 0, false);
    MOQ_TEST_CHECK(moq_pub_end_group(f.pub, f.ptrack,
                                     moq_simpair_now_us(f.sp)) == MOQ_OK);
    write_obj(&f, 1, 0, true);
    MOQ_TEST_CHECK(moq_pub_reset_group(f.pub, f.ptrack, 0x10,
                                       moq_simpair_now_us(f.sp)) == MOQ_OK);
    deliver(&f);
    moq_sub_tick(f.sub, moq_simpair_now_us(f.sp));
    MOQ_TEST_CHECK(poll_obj(&f, 0, 0));
    moq_sub_object_t obj;
    while (moq_sub_poll_object(f.sub, &obj) == MOQ_OK)
        moq_sub_object_cleanup(&obj);

    moq_sub_stats_t st;
    MOQ_TEST_CHECK(moq_sub_get_stats(f.sub, &st, sizeof(st)) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(st.subgroup_finished, 1);
    MOQ_TEST_CHECK_EQ_U64(st.subgroup_resets, 1);
    moq_sub_group_end_t ge;
    MOQ_TEST_CHECK(moq_sub_poll_group_end(f.sub, &ge) == MOQ_DONE);

    fx_teardown(&f);
    if (failures == before)
        printf("PASS: group_end_none_without_bit_or_on_reset_%s\n", pname(ver));
}

/* The option off -- explicitly, or through a v0-sized cfg -- queues
 * nothing, and the tick behaves exactly as before. */
static void test_option_off(moq_version_t ver, int cfg_generation)
{
    int before = failures;
    fx_t f;
    fx_setup(&f, ver, false, cfg_generation, 0);

    for (uint64_t g = 0; g < 3; g++) {
        write_obj(&f, g, 0, true);
        MOQ_TEST_CHECK(moq_pub_end_group(f.pub, f.ptrack,
                                         moq_simpair_now_us(f.sp)) == MOQ_OK);
    }
    deliver(&f);
    MOQ_TEST_CHECK(moq_sub_tick(f.sub, moq_simpair_now_us(f.sp)) == MOQ_OK);
    for (uint64_t g = 0; g < 3; g++)
        MOQ_TEST_CHECK(poll_obj(&f, g, 0));
    moq_sub_group_end_t ge;
    MOQ_TEST_CHECK(moq_sub_poll_group_end(f.sub, &ge) == MOQ_DONE);
    moq_sub_stats_t st;
    MOQ_TEST_CHECK(moq_sub_get_stats(f.sub, &st, sizeof(st)) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(st.subgroup_finished, 3);

    fx_teardown(&f);
    if (failures == before)
        printf("PASS: group_end_option_off%s_%s\n",
               cfg_generation == 2 ? "_pre_ts_padding" :
               cfg_generation == 1 ? "_v0_cfg" : "",
               pname(ver));
}

/* The Group-end queue is bounded by max_objects. A Group end that finds it
 * full is held, the tick reports WOULD_BLOCK, and the next tick after a poll
 * queues it in order. */
static void test_queue_full_pending(moq_version_t ver)
{
    int before = failures;
    fx_t f;
    fx_setup(&f, ver, true, false, 2);

    for (uint64_t g = 0; g < 2; g++) {
        write_obj(&f, g, 0, true);
        MOQ_TEST_CHECK(moq_pub_end_group(f.pub, f.ptrack,
                                         moq_simpair_now_us(f.sp)) == MOQ_OK);
        deliver(&f);
        MOQ_TEST_CHECK(moq_sub_tick(f.sub, moq_simpair_now_us(f.sp)) == MOQ_OK);
        MOQ_TEST_CHECK(poll_obj(&f, g, 0));
    }
    write_obj(&f, 2, 0, true);
    MOQ_TEST_CHECK(moq_pub_end_group(f.pub, f.ptrack,
                                     moq_simpair_now_us(f.sp)) == MOQ_OK);
    deliver(&f);
    MOQ_TEST_CHECK(moq_sub_tick(f.sub, moq_simpair_now_us(f.sp)) ==
                   MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK(poll_obj(&f, 2, 0));
    MOQ_TEST_CHECK(moq_sub_tick(f.sub, moq_simpair_now_us(f.sp)) ==
                   MOQ_ERR_WOULD_BLOCK);

    moq_sub_group_end_t ge;
    MOQ_TEST_CHECK(moq_sub_poll_group_end(f.sub, &ge) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(ge.group_id, 0);
    MOQ_TEST_CHECK(moq_sub_tick(f.sub, moq_simpair_now_us(f.sp)) == MOQ_OK);
    MOQ_TEST_CHECK(moq_sub_poll_group_end(f.sub, &ge) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(ge.group_id, 1);
    MOQ_TEST_CHECK(moq_sub_poll_group_end(f.sub, &ge) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(ge.group_id, 2);
    MOQ_TEST_CHECK(moq_sub_poll_group_end(f.sub, &ge) == MOQ_DONE);

    fx_teardown(&f);
    if (failures == before)
        printf("PASS: group_end_queue_full_pending_%s\n", pname(ver));
}

/* A queued Group end holds a finished track's facade slot on draft 22 until
 * it is polled; drafts 16 and 18 keep their release behavior. */
static void test_release_with_queued_group_end(moq_version_t ver)
{
    int before = failures;
    fx_t f;
    fx_setup(&f, ver, true, false, 0);

    write_obj(&f, 0, 0, true);
    MOQ_TEST_CHECK(moq_pub_end_group(f.pub, f.ptrack,
                                     moq_simpair_now_us(f.sp)) == MOQ_OK);
    deliver(&f);
    MOQ_TEST_CHECK(moq_sub_tick(f.sub, moq_simpair_now_us(f.sp)) == MOQ_OK);
    MOQ_TEST_CHECK(poll_obj(&f, 0, 0));

    MOQ_TEST_CHECK(moq_sub_unsubscribe(f.sub, f.track,
                                       moq_simpair_now_us(f.sp)) == MOQ_OK);
    deliver(&f);
    moq_sub_tick(f.sub, moq_simpair_now_us(f.sp));

    moq_sub_group_end_t ge;
    if (ver == MOQ_VERSION_DRAFT_22) {
        MOQ_TEST_CHECK(moq_sub_release_track(f.sub, f.track) ==
                       MOQ_ERR_WRONG_STATE);
        MOQ_TEST_CHECK(moq_sub_poll_group_end(f.sub, &ge) == MOQ_OK);
        MOQ_TEST_CHECK(ge.track == f.track);
        MOQ_TEST_CHECK_EQ_U64(ge.group_id, 0);
        MOQ_TEST_CHECK(moq_sub_release_track(f.sub, f.track) == MOQ_OK);
    } else {
        /* Not checked on these drafts: the caller polls first, as the
         * header says; the queued record is left for it. */
        MOQ_TEST_CHECK(moq_sub_release_track(f.sub, f.track) == MOQ_OK);
        MOQ_TEST_CHECK(moq_sub_poll_group_end(f.sub, &ge) == MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(ge.group_id, 0);
    }
    MOQ_TEST_CHECK(moq_sub_poll_group_end(f.sub, &ge) == MOQ_DONE);

    fx_teardown(&f);
    if (failures == before)
        printf("PASS: group_end_release_%s\n", pname(ver));
}

/* Draft 22 decodes the subgroup header with draft 18's decoder, END_OF_GROUP
 * being bit 0x08 in both, and finishes the subgroup through the shared
 * receive path. A subgroup that carried the bit and ends with a FIN yields,
 * behind its objects, a FIN_END_OF_GROUP stream terminal naming the Group's
 * largest Object and a Group end; one without the bit yields a plain FIN
 * terminal and no Group end. */
static void test_group_end_d22(void)
{
    int before = failures;
    fx_t f;
    fx_setup_lanes(&f, MOQ_VERSION_DRAFT_22, true, true, 0, 0);

    write_obj(&f, 0, 0, true);
    write_obj(&f, 0, 3, true);
    MOQ_TEST_CHECK(moq_pub_end_group(f.pub, f.ptrack,
                                     moq_simpair_now_us(f.sp)) == MOQ_OK);
    write_obj(&f, 1, 0, false);
    write_obj(&f, 1, 1, false);
    MOQ_TEST_CHECK(moq_pub_end_group(f.pub, f.ptrack,
                                     moq_simpair_now_us(f.sp)) == MOQ_OK);
    deliver(&f);
    MOQ_TEST_CHECK(moq_sub_tick(f.sub, moq_simpair_now_us(f.sp)) == MOQ_OK);

    moq_sub_stream_terminal_t st;
    moq_sub_group_end_t ge;
    MOQ_TEST_CHECK(moq_sub_poll_stream_terminal(f.sub, &st, sizeof(st)) == MOQ_DONE);
    MOQ_TEST_CHECK(moq_sub_poll_group_end(f.sub, &ge) == MOQ_DONE);
    MOQ_TEST_CHECK(poll_obj(&f, 0, 0));
    MOQ_TEST_CHECK(poll_obj(&f, 0, 3));
    memset(&st, 0xEE, sizeof(st));
    MOQ_TEST_CHECK(moq_sub_poll_stream_terminal(f.sub, &st, sizeof(st)) == MOQ_OK);
    MOQ_TEST_CHECK(st.struct_size == sizeof(st));
    MOQ_TEST_CHECK(st.kind == MOQ_SUB_STREAM_FIN_END_OF_GROUP);
    MOQ_TEST_CHECK(st.track == f.track);
    MOQ_TEST_CHECK_EQ_U64(st.group_id, 0);
    MOQ_TEST_CHECK_EQ_U64(st.subgroup_id, 0);
    MOQ_TEST_CHECK(st.has_last_object && !st.has_cut_object);
    MOQ_TEST_CHECK_EQ_U64(st.last_object_id, 3);
    memset(&ge, 0xEE, sizeof(ge));
    MOQ_TEST_CHECK(moq_sub_poll_group_end(f.sub, &ge) == MOQ_OK);
    MOQ_TEST_CHECK(ge.track == f.track);
    MOQ_TEST_CHECK_EQ_U64(ge.group_id, 0);
    MOQ_TEST_CHECK_EQ_U64(ge.subgroup_id, 0);
    MOQ_TEST_CHECK(moq_sub_poll_group_end(f.sub, &ge) == MOQ_DONE);

    MOQ_TEST_CHECK(poll_obj(&f, 1, 0));
    MOQ_TEST_CHECK(poll_obj(&f, 1, 1));
    memset(&st, 0xEE, sizeof(st));
    MOQ_TEST_CHECK(moq_sub_poll_stream_terminal(f.sub, &st, sizeof(st)) == MOQ_OK);
    MOQ_TEST_CHECK(st.kind == MOQ_SUB_STREAM_FIN);
    MOQ_TEST_CHECK_EQ_U64(st.group_id, 1);
    MOQ_TEST_CHECK(st.has_last_object);
    MOQ_TEST_CHECK_EQ_U64(st.last_object_id, 1);
    MOQ_TEST_CHECK(moq_sub_poll_stream_terminal(f.sub, &st, sizeof(st)) == MOQ_DONE);
    MOQ_TEST_CHECK(moq_sub_poll_group_end(f.sub, &ge) == MOQ_DONE);

    moq_sub_stats_t stats;
    MOQ_TEST_CHECK(moq_sub_get_stats(f.sub, &stats, sizeof(stats)) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(stats.subgroup_finished, 2);

    fx_teardown(&f);
    if (failures == before)
        printf("PASS: sub_group_end_d22\n");
}

int main(void)
{
    test_group_end_d22();
    for (int vi = 0; vi < 2; vi++) {
        moq_version_t ver = vi ? MOQ_VERSION_DRAFT_18 : MOQ_VERSION_DRAFT_16;
        test_fin_with_end_of_group(ver);
        test_no_end_without_bit_or_on_reset(ver);
        test_option_off(ver, false);
        test_option_off(ver, true);
        test_option_off(ver, 2);
        test_queue_full_pending(ver);
        test_release_with_queued_group_end(ver);
    }
    test_release_with_queued_group_end(MOQ_VERSION_DRAFT_22);
    if (failures == 0) {
        printf("PASS: all subscriber group-end tests\n");
        return 0;
    }
    printf("FAIL: %d subscriber group-end test failures\n", failures);
    return 1;
}
