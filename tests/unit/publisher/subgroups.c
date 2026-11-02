#include "test_cases.h"

/* The subgroup header is fixed when a subgroup opens: its END_OF_GROUP bit
 * and whether its Objects carry properties (drafts 16 and 18, subgroup header
 * Type). An Object that cannot be written under the header a destination
 * already sent for its Group is refused at admission -- before the operation
 * is committed -- so nothing is left pending and the track keeps working:
 * the same Object written with the subgroup's header values goes through. */
extern unsigned long moq_pub_test_commit_count;

/* Received objects of groups 0..1, objects 0..5, per destination: [0] the
 * client's subscription, [1] the publication it accepted. */
typedef struct { unsigned n[2][6][2]; } recv_tally_t;

static void tally_objects(moq_simpair_t *sp, recv_tally_t *t)
{
    moq_event_t evts[16]; size_t ne;
    while ((ne = moq_session_poll_events(moq_simpair_client(sp), evts, 16)) > 0)
        for (size_t i = 0; i < ne; i++) {
            if (evts[i].kind == MOQ_EVENT_OBJECT_RECEIVED) {
                const moq_object_received_event_t *o = &evts[i].u.object_received;
                int dest = moq_publication_is_valid(o->pub) ? 1 : 0;
                if (o->group_id < 2 && o->object_id < 6)
                    t->n[o->group_id][o->object_id][dest]++;
            }
            moq_event_cleanup(&evts[i]);
        }
}

static void subgroup_header_case(uint64_t version, const char *label)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    fanout_setup_ver(&as, &alloc, &sp, &pub, &track, true, 0, version);
    recv_tally_t t;
    memset(&t, 0, sizeof(t));

    uint8_t d[] = { 0x70, 0x61, 0x79 };
    moq_rcbuf_t *pl = NULL;
    moq_rcbuf_create(&alloc, d, sizeof(d), &pl);
    uint8_t pd[] = { 0x02, 0x00 };          /* one even-type property */
    moq_rcbuf_t *pr = NULL;
    moq_rcbuf_create(&alloc, pd, sizeof(pd), &pr);

    /* Object 0 opens Group 0's subgroup: no END_OF_GROUP, no properties. */
    moq_pub_object_cfg_t obj;
    moq_pub_object_cfg_init_sized(&obj, sizeof(obj));
    obj.group_id = 0; obj.object_id = 0; obj.payload = pl;
    MOQ_TEST_CHECK(moq_pub_write_object_ex(pub, track, &obj,
        moq_simpair_now_us(sp)) == MOQ_OK);

    /* Object 1 asking for END_OF_GROUP: refused, nothing committed. */
    unsigned long commits = moq_pub_test_commit_count;
    moq_pub_object_cfg_init_sized(&obj, sizeof(obj));
    obj.group_id = 0; obj.object_id = 1; obj.payload = pl;
    obj.end_of_group = true;
    MOQ_TEST_CHECK(moq_pub_write_object_ex(pub, track, &obj,
        moq_simpair_now_us(sp)) == MOQ_ERR_WRONG_STATE);
    MOQ_TEST_CHECK_EQ_U64(moq_pub_test_commit_count, commits);
    /* The track is not stuck. */
    obj.end_of_group = false;
    MOQ_TEST_CHECK(moq_pub_write_object_ex(pub, track, &obj,
        moq_simpair_now_us(sp)) == MOQ_OK);

    /* Object 2 with properties, into a subgroup opened without them. */
    commits = moq_pub_test_commit_count;
    moq_pub_object_cfg_init_sized(&obj, sizeof(obj));
    obj.group_id = 0; obj.object_id = 2; obj.payload = pl;
    obj.properties = pr;
    MOQ_TEST_CHECK(moq_pub_write_object_ex(pub, track, &obj,
        moq_simpair_now_us(sp)) == MOQ_ERR_WRONG_STATE);
    MOQ_TEST_CHECK_EQ_U64(moq_pub_test_commit_count, commits);
    obj.properties = NULL;
    MOQ_TEST_CHECK(moq_pub_write_object_ex(pub, track, &obj,
        moq_simpair_now_us(sp)) == MOQ_OK);

    /* The streaming bracket follows the same rule. */
    commits = moq_pub_test_commit_count;
    moq_pub_begin_object_cfg_t bcfg;
    moq_pub_begin_object_cfg_init_sized(&bcfg, sizeof(bcfg));
    bcfg.group_id = 0; bcfg.object_id = 3; bcfg.payload_length = sizeof(d);
    bcfg.properties = pr;
    MOQ_TEST_CHECK(moq_pub_begin_object(pub, track, &bcfg,
        moq_simpair_now_us(sp)) == MOQ_ERR_WRONG_STATE);
    MOQ_TEST_CHECK_EQ_U64(moq_pub_test_commit_count, commits);
    bcfg.properties = NULL;
    MOQ_TEST_CHECK(moq_pub_begin_object(pub, track, &bcfg,
        moq_simpair_now_us(sp)) == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_write_data(pub, track, pl,
        moq_simpair_now_us(sp)) == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_end_object(pub, track,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    tally_objects(sp, &t);

    /* A new Group opens a new subgroup with the header its first Object
     * asks for: END_OF_GROUP and properties. An object without properties
     * fits that header too (written with an empty list), as does a streamed
     * one. */
    moq_pub_object_cfg_init_sized(&obj, sizeof(obj));
    obj.group_id = 1; obj.object_id = 0; obj.payload = pl;
    obj.end_of_group = true; obj.properties = pr;
    MOQ_TEST_CHECK(moq_pub_write_object_ex(pub, track, &obj,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_pub_object_cfg_init_sized(&obj, sizeof(obj));
    obj.group_id = 1; obj.object_id = 1; obj.payload = pl;
    obj.end_of_group = true; obj.properties = NULL;
    MOQ_TEST_CHECK(moq_pub_write_object_ex(pub, track, &obj,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_pub_begin_object_cfg_init_sized(&bcfg, sizeof(bcfg));
    bcfg.group_id = 1; bcfg.object_id = 2; bcfg.payload_length = sizeof(d);
    MOQ_TEST_CHECK(moq_pub_begin_object(pub, track, &bcfg,
        moq_simpair_now_us(sp)) == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_write_data(pub, track, pl,
        moq_simpair_now_us(sp)) == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_end_object(pub, track,
        moq_simpair_now_us(sp)) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    tally_objects(sp, &t);

    /* Each destination received every written object exactly once, and
     * nothing for the refused attempts. */
    static const uint64_t want[][2] = { {0,0},{0,1},{0,2},{0,3},{1,0},{1,1},{1,2} };
    for (size_t i = 0; i < sizeof(want) / sizeof(want[0]); i++)
        for (int dest = 0; dest < 2; dest++)
            MOQ_TEST_CHECK_EQ_U64(t.n[want[i][0]][want[i][1]][dest], 1);
    for (int dest = 0; dest < 2; dest++) {
        MOQ_TEST_CHECK_EQ_U64(t.n[0][4][dest], 0);
        MOQ_TEST_CHECK_EQ_U64(t.n[1][3][dest], 0);
    }
    MOQ_TEST_CHECK(end_track_virtual(sp, pub, track) == MOQ_OK);

    moq_rcbuf_decref(pl);
    moq_rcbuf_decref(pr);
    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS(label);
}

void test_subgroup_header_mismatch_admission(void)
{
    subgroup_header_case(0, "subgroup header mismatch refused at admission");
    subgroup_header_case(MOQ_VERSION_DRAFT_18,
                         "subgroup header mismatch refused at admission d18");
}

/* -- FIRST_OBJECT on facade-opened subgroups ----------------------------- *
 * The facade is the Original Publisher: a destination stream that begins
 * with the first object the track published in that group's subgroup
 * carries FIRST_OBJECT (draft-18 and draft-22 §2.2); a stream that begins
 * later -- a subscriber that arrived mid-group -- does not, and neither do
 * the stream's later objects. Draft-16 has no such bit. */
static void collect_first_flags(moq_simpair_t *sp, uint64_t g, bool *flags,
                                int *n)
{
    moq_event_t ev;
    while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1) {
        if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED &&
            ev.u.object_received.group_id == g && *n < 4)
            flags[(*n)++] = ev.u.object_received.first_object;
        moq_event_cleanup(&ev);
    }
}

void test_facade_first_object(moq_version_t ver)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
    moq_publisher_t *pub; moq_pub_track_t *track;
    windows_setup(&as, &alloc, &sp, ver, &pub, &track);
    const bool has_bit = ver != MOQ_VERSION_DRAFT_16;

    /* Group 0 starts before anyone subscribes. */
    MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 0, 0, 0xF0)
                   == MOQ_OK);
    windows_subscribe(sp, pub, MOQ_SUBSCRIBE_FILTER_NONE, 0, 0, 0);
    MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 0, 1, 0xF1)
                   == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    bool flags[4] = { true, true, true, true };
    int n = 0;
    collect_first_flags(sp, 0, flags, &n);
    MOQ_TEST_CHECK_EQ_INT(n, 1);
    MOQ_TEST_CHECK(!flags[0]);      /* joined mid-group: not the first */

    /* Group 1 starts with the subscriber present. */
    MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 1, 0, 0xF2)
                   == MOQ_OK);
    MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 1, 1, 0xF3)
                   == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    n = 0;
    collect_first_flags(sp, 1, flags, &n);
    MOQ_TEST_CHECK_EQ_INT(n, 2);
    MOQ_TEST_CHECK(flags[0] == has_bit);
    MOQ_TEST_CHECK(!flags[1]);

    /* Groups need not increase with time (draft-22 §2.3.1): group 3 starts,
     * then group 2, which nothing published before, starts too. Each first
     * object opens its subgroup with FIRST_OBJECT; a later object of group 2
     * does not. */
    MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 3, 0, 0xF4) == MOQ_OK);
    MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 2, 0, 0xF5) == MOQ_OK);
    MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 2, 1, 0xF6) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    bool f3[4] = { true, true, true, true }, f2[4] = { true, true, true, true };
    int n3 = 0, n2 = 0;
    {
        moq_event_t ev;
        while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) {
                uint64_t g = ev.u.object_received.group_id;
                if (g == 3 && n3 < 4) f3[n3++] = ev.u.object_received.first_object;
                if (g == 2 && n2 < 4) f2[n2++] = ev.u.object_received.first_object;
            }
            moq_event_cleanup(&ev);
        }
    }
    MOQ_TEST_CHECK_EQ_INT(n3, 1);
    MOQ_TEST_CHECK(f3[0] == has_bit);
    MOQ_TEST_CHECK_EQ_INT(n2, 2);
    MOQ_TEST_CHECK(f2[0] == has_bit);
    MOQ_TEST_CHECK(!f2[1]);

    /* Group 50, below seventeen later group starts but never published,
     * starts its subgroup and carries FIRST_OBJECT; so does the new group
     * 117. */
    for (uint64_t g = 100; g <= 116; g++)
        MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, g, 0, 0xE0) == MOQ_OK);
    MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 50, 0, 0xE1) == MOQ_OK);
    MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 117, 0, 0xE2) == MOQ_OK);
    {
        int seen50 = 0, seen117 = 0, seen116 = 0;
        moq_event_t ev;
        for (int round = 0; round < 16; round++) {
        moq_simpair_run_until_quiescent(sp, 32, NULL);
        while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) {
                uint64_t g = ev.u.object_received.group_id;
                bool f = ev.u.object_received.first_object;
                if (g == 50) { seen50++; MOQ_TEST_CHECK(f == has_bit); }
                if (g == 116) { seen116++; MOQ_TEST_CHECK(f == has_bit); }
                if (g == 117) { seen117++; MOQ_TEST_CHECK(f == has_bit); }
            }
            moq_event_cleanup(&ev);
        }
        }
        MOQ_TEST_CHECK(seen50 == 1 && seen116 == 1 && seen117 == 1);
    }

    /* Records now hold groups 103..116, 50 and 117; groups 0..3 and
     * 100..102 were displaced and sealed. Every group's subgroup ended when
     * the track moved on, so a later Object in a group is a revisit. */
    int ended = 0;
    bool f = false;
    uint64_t sgid = UINT64_MAX;

    /* A displaced group takes nothing more. */
    MOQ_TEST_CHECK_EQ_INT((int)declare_write_stream(pub, track, &alloc, sp,
                              100, 1, 0xE3), (int)MOQ_ERR_WRONG_STATE);
    MOQ_TEST_CHECK_EQ_INT(fo_seen(sp, 100, 1, NULL, NULL, &ended), 0);

    /* A recorded group revisited opens the next subgroup, which the stream
     * begins (FIRST_OBJECT on the drafts with the bit). */
    MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 116, 1, 0xE4)
                   == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(fo_seen(sp, 116, 1, &f, &sgid, &ended), 1);
    MOQ_TEST_CHECK(f == has_bit);
    MOQ_TEST_CHECK_EQ_U64(sgid, 1);
    MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 50, 1, 0xE7)
                   == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(fo_seen(sp, 50, 1, &f, &sgid, &ended), 1);
    MOQ_TEST_CHECK(f == has_bit);
    MOQ_TEST_CHECK_EQ_U64(sgid, 1);

    /* A revisit must be above every Object already assigned in the group. */
    MOQ_TEST_CHECK_EQ_INT((int)declare_write_stream(pub, track, &alloc, sp,
                              116, 1, 0xE5), (int)MOQ_ERR_WRONG_STATE);
    MOQ_TEST_CHECK_EQ_INT(fo_seen(sp, 116, 1, NULL, NULL, &ended), 0);
    MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 116, 2, 0xE6)
                   == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(fo_seen(sp, 116, 2, &f, &sgid, &ended), 1);
    MOQ_TEST_CHECK(f == has_bit);
    MOQ_TEST_CHECK_EQ_U64(sgid, 2);

    /* Seven revisits per group (Subgroup IDs 0..7): groups 117 and 50
     * alternate, so each write leaves the other group. */
    for (uint64_t k = 1; k <= 7; k++) {
        if (k >= 2) {
            MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 50, k,
                                                0xE8) == MOQ_OK);
            MOQ_TEST_CHECK_EQ_INT(fo_seen(sp, 50, k, &f, &sgid, &ended), 1);
            MOQ_TEST_CHECK(f == has_bit);
            MOQ_TEST_CHECK_EQ_U64(sgid, k);
        }
        MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 117, k,
                                            0xE9) == MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(fo_seen(sp, 117, k, &f, &sgid, &ended), 1);
        MOQ_TEST_CHECK(f == has_bit);
        MOQ_TEST_CHECK_EQ_U64(sgid, k);
    }
    /* An eighth revisit is refused, payload left to the caller. */
    {
        uint8_t d[2] = { 0xEA, 0x15 };
        moq_rcbuf_t *pl = NULL;
        moq_rcbuf_create(&alloc, d, sizeof(d), &pl);
        MOQ_TEST_CHECK_EQ_INT((int)moq_pub_write_object(pub, track, 50, 8, pl,
                                  moq_simpair_now_us(sp)), (int)MOQ_ERR_NOMEM);
        MOQ_TEST_CHECK_EQ_INT((int)moq_rcbuf_refcount(pl), 1);
        moq_rcbuf_decref(pl);
        MOQ_TEST_CHECK_EQ_INT(fo_seen(sp, 50, 8, NULL, NULL, &ended), 0);
    }

    /* Groups 116, 50 and 117 are revisited. Twelve more (500..511) take the
     * revisited count to fifteen; each new group and its filler (600..611)
     * displace the least recently admitted group never revisited. */
    for (uint64_t i = 0; i < 12; i++) {
        MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 500 + i, 0,
                                            0xEB) == MOQ_OK);
        MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 600 + i, 0,
                                            0xEC) == MOQ_OK);
        MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 500 + i, 1,
                                            0xED) == MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(fo_seen(sp, 500 + i, 1, &f, &sgid, &ended), 1);
        MOQ_TEST_CHECK(f == has_bit);
        MOQ_TEST_CHECK_EQ_U64(sgid, 1);
    }
    /* A sixteenth revisited group is refused on every path, before any
     * effect: nothing is sent and no object is left open. Group 520's own
     * write displaced the last filler; the revisit of 116 needs no record. */
    MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 520, 0, 0xEE)
                   == MOQ_OK);
    MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 116, 3, 0xEF)
                   == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(fo_seen(sp, 116, 3, &f, &sgid, &ended), 1);
    MOQ_TEST_CHECK_EQ_U64(sgid, 3);
    MOQ_TEST_CHECK_EQ_INT((int)declare_write_stream(pub, track, &alloc, sp,
                              520, 1, 0xF7), (int)MOQ_ERR_NOMEM);
    {
        moq_pub_begin_object_cfg_t bcfg;
        moq_pub_begin_object_cfg_init_sized(&bcfg, sizeof(bcfg));
        bcfg.group_id = 520; bcfg.object_id = 1; bcfg.payload_length = 2;
        MOQ_TEST_CHECK_EQ_INT((int)moq_pub_begin_object(pub, track, &bcfg,
                                  moq_simpair_now_us(sp)), (int)MOQ_ERR_NOMEM);
        moq_rcbuf_t *pd = NULL;
        uint8_t two[2] = { 1, 2 };
        moq_rcbuf_create(&alloc, two, sizeof(two), &pd);
        MOQ_TEST_CHECK_EQ_INT((int)moq_pub_write_data(pub, track, pd,
                                  moq_simpair_now_us(sp)), (int)MOQ_ERR_WRONG_STATE);
        moq_rcbuf_decref(pd);
    }
    MOQ_TEST_CHECK_EQ_INT(fo_seen(sp, 520, 1, NULL, NULL, &ended), 0);

    /* A revisited group keeps its record however far the track moved on;
     * a group displaced long ago is still sealed. */
    MOQ_TEST_CHECK(declare_write_stream(pub, track, &alloc, sp, 500, 2, 0xF8)
                   == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(fo_seen(sp, 500, 2, &f, &sgid, &ended), 1);
    MOQ_TEST_CHECK(f == has_bit);
    MOQ_TEST_CHECK_EQ_U64(sgid, 2);
    MOQ_TEST_CHECK_EQ_INT((int)declare_write_stream(pub, track, &alloc, sp,
                              103, 1, 0xF9), (int)MOQ_ERR_WRONG_STATE);
    MOQ_TEST_CHECK_EQ_INT(fo_seen(sp, 103, 1, NULL, NULL, &ended), 0);

    /* The subscriber never saw a malformed track: the subscription is still
     * established. */
    MOQ_TEST_CHECK_EQ_INT(ended, 0);

    moq_pub_destroy(pub);
    drain_all(sp);
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS(ver == MOQ_VERSION_DRAFT_22 ? "facade_first_object_d22"
                  : ver == MOQ_VERSION_DRAFT_18 ? "facade_first_object_d18"
                  : "facade_first_object_d16");
}
