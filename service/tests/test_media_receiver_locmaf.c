/*
 * LOCMAF tracks (draft-einarsson-moq-locmaf-01) through the receiver's live
 * intake, queue and poll paths, on drafts 16, 18 and 22, fed Objects
 * directly (no wire). Each Object is a real encoder's output for a canonical
 * source chunk, so a rebuilt Object equals its source byte for byte.
 *
 * Pinned:
 * - a LOCMAF track's description carries packaging LOCMAF and its CMAF
 *   Header record with the trex facts; its Objects reach the application
 *   as canonical CMAF chunks: packaging CMAF, the chunk as the fragment,
 *   the payload empty, with a loss report;
 * - every live Object taken in is counted exactly once: rebuilt,
 *   undecodable, expired or late;
 * - a delta the decoder cannot continue (a mid-group join, an Object after
 *   a gap, a reset stream, a datagram, a second subgroup, a delta after a
 *   rawBoxes Object) is dropped, a loss reported on the next delivered
 *   Object, until a full header recovers the Group or the next Group
 *   starts; a rawBoxes Object ends the loss for delivery only (section 9.3);
 *   a duplicate is dropped without loss;
 * - Groups decode independently, up to two open at once: an earlier Group's
 *   tail after the next Group began is still delivered in order; a reset
 *   Group's slot is reclaimed without eviction; a third Group evicts the
 *   lowest open one (or is refused when it is lower than both), counted,
 *   its later Objects late; a closed Group never reopens, through the
 *   closed record and its floor;
 * - a disable resets the decoders, so a Group resumes at its next full
 *   header; an Object of an earlier subscription is dropped before decode;
 * - an unsupported or missing locmafVersion, property-carried init and
 *   encryptionScheme leave the track without a delivery path, and
 *   subscribe_track refuses it; an in-band init carried in rawBoxes is
 *   refused;
 * - on draft 22 the Object's LOC properties still surface; an MSF-02
 *   advisory update keeps the chain, and a critical one replaces the handle,
 *   whose decoder then rebuilds an earlier Group with the new Header's
 *   facts (the documented limitation);
 * - every catalog revision and companion delivers the same chunks;
 * - an allocation failure while starting the stage fails the start with
 *   nothing leaked, and one while rebuilding loses that Object only.
 */
#include <moq/media_receiver.h>
#include <moq/subscriber.h>
#define MOQ_MEDIA_RECEIVER_TESTING 1
#include "../src/media_receiver.c"

static int failures;

#include "locmaf_receiver_fixture.h"

#define ROW(name) do { lmx_row = (name); } while (0)

static moq_version_t g_draft = MOQ_VERSION_DRAFT_16;

static const char *draft_name(moq_version_t v)
{
    return v == MOQ_VERSION_DRAFT_16 ? "d16" : v == MOQ_VERSION_DRAFT_18 ? "d18" : "d22";
}

/* -- Delivery ------------------------------------------------------------- */

static void test_f1_exact(void)
{
    ROW("f1_exact");
    lmx_t f;
    if (!lmx_open(&f, g_draft)) { lmx_close(&f); return; }
    /* Fixture F1: audio, one sample, clear, a full header. */
    static const uint8_t obj[] = { 0x02, 0x04, 0x0A, 0x00, 0x0E, 0x01,
                                   0xDE, 0xAD, 0xBE, 0xEF };
    static const uint8_t canon[] = {
        0x00,0x00,0x00,0x5C, 'm','o','o','f',
        0x00,0x00,0x00,0x10, 'm','f','h','d', 0,0,0,0, 0,0,0,0,
        0x00,0x00,0x00,0x44, 't','r','a','f',
        0x00,0x00,0x00,0x14, 't','f','h','d', 0x00,0x02,0x00,0x10, 0,0,0,1, 0,0,0,4,
        0x00,0x00,0x00,0x14, 't','f','d','t', 0x01,0,0,0, 0,0,0,0, 0,0,0,0,
        0x00,0x00,0x00,0x14, 't','r','u','n', 0,0,0,1, 0,0,0,1, 0,0,0,0x64,
        0x00,0x00,0x00,0x0C, 'm','d','a','t', 0xDE,0xAD,0xBE,0xEF };
    lmx_obj_t o;
    memset(&o, 0, sizeof(o));
    memcpy(o.wire, obj, sizeof(obj));
    o.wire_len = sizeof(obj);
    memcpy(o.canon, canon, sizeof(canon));
    o.canon_len = sizeof(canon);
    lmx_push(&f, 0, 0, &o);
    lmx_poll(&f);
    MOQ_TEST_CHECK(f.got_count == 1);
    MOQ_TEST_CHECK(lmx_rebuilt(&f, 0, 0, &o));
    MOQ_TEST_CHECK(f.got_count == 1 && f.got[0].has_loss_report && !f.got[0].gap);
    MOQ_TEST_CHECK(f.got[0].origin == MOQ_MEDIA_DELIVERY_LIVE);
    moq_media_receiver_stats_t st = lmx_stats(&f);
    MOQ_TEST_CHECK(st.locmaf_objects_rebuilt == 1 && st.parse_drops == 0);
    MOQ_TEST_CHECK(lmx_balanced(&f, 0));
    /* The description: the trex facts the decoder uses. */
    const moq_cmaf_init_info_t *ci = f.t->desc.cmaf;
    MOQ_TEST_CHECK(ci && ci->struct_size >= MOQ_CMAF_INIT_INFO_SIZE_THROUGH_CONSTANT_IV &&
                   ci->trak_count == 1 && ci->has_trex &&
                   ci->trex_default_sample_duration == 1024 &&
                   ci->trex_default_sample_flags == 0x02000000u);
    const moq_msf_locmaf_t *rec =
        moq_msf_ext_locmaf(f.t->desc.extensions, f.t->desc.extension_count);
    MOQ_TEST_CHECK(rec && rec->has_version && rec->version.len == 3 &&
                   memcmp(rec->version.data, "0.3", 3) == 0);
    lmx_close(&f);
}

static void test_sequence(void)
{
    ROW("sequence");
    lmx_t f;
    if (!lmx_open(&f, g_draft)) { lmx_close(&f); return; }
    lmx_obj_t g0[6], g1[4];
    lmx_group(&f, 0, 6, g0);
    lmx_group(&f, 1, 4, g1);
    for (uint64_t o = 0; o < 6; o++) lmx_push(&f, 0, o, &g0[o]);
    for (uint64_t o = 0; o < 4; o++) lmx_push(&f, 1, o, &g1[o]);
    lmx_poll(&f);
    MOQ_TEST_CHECK(f.got_count == 10);
    for (uint64_t o = 0; o < 6; o++) MOQ_TEST_CHECK(lmx_rebuilt(&f, 0, o, &g0[o]));
    for (uint64_t o = 0; o < 4; o++) MOQ_TEST_CHECK(lmx_rebuilt(&f, 1, o, &g1[o]));
    for (size_t i = 0; i < f.got_count; i++) MOQ_TEST_CHECK(!f.got[i].gap);
    MOQ_TEST_CHECK(lmx_in_order(&f, 0) && lmx_in_order(&f, 1));
    /* The deltas are smaller than the chunks they rebuild. */
    MOQ_TEST_CHECK(g0[1].wire_len < g0[1].canon_len);
    MOQ_TEST_CHECK(lmx_stats(&f).locmaf_objects_rebuilt == 10);
    MOQ_TEST_CHECK(lmx_balanced(&f, 0));
    lmx_close(&f);
}

/* Joined mid-group with no fill: deltas are dropped until the next group. */
static void test_mid_group_join(void)
{
    ROW("mid_group_join");
    lmx_t f;
    if (!lmx_open(&f, g_draft)) { lmx_close(&f); return; }
    lmx_obj_t g0[6], g1[2];
    lmx_group(&f, 0, 6, g0);
    lmx_group(&f, 1, 2, g1);
    for (uint64_t o = 3; o < 6; o++) lmx_push(&f, 0, o, &g0[o]);
    for (uint64_t o = 0; o < 2; o++) lmx_push(&f, 1, o, &g1[o]);
    lmx_poll(&f);
    MOQ_TEST_CHECK(lmx_count(&f, 0) == 0 && lmx_count(&f, 1) == 2);
    MOQ_TEST_CHECK(lmx_rebuilt(&f, 1, 0, &g1[0]) && lmx_rebuilt(&f, 1, 1, &g1[1]));
    const lmx_rec_t *first = lmx_find(&f, 1, 0);
    MOQ_TEST_CHECK(first && first->gap && first->gap_group == 0 && first->gap_object == 3);
    moq_media_receiver_stats_t st = lmx_stats(&f);
    MOQ_TEST_CHECK(st.locmaf_objects_undecodable == 3 && st.locmaf_objects_rebuilt == 2 &&
                   st.locmaf_objects_expired == 0);
    MOQ_TEST_CHECK(receiver_lm_slot_state(((receiver_locmaf_t *)f.t->lm)->stage, 0) ==
                   LM_SLOT_BARRIER);
    MOQ_TEST_CHECK(lmx_balanced(&f, 0));
    lmx_close(&f);
}

/* A gap in Object IDs: the chain is lost until a full header. */
static void test_gap(void)
{
    ROW("gap");
    lmx_t f;
    if (!lmx_open(&f, g_draft)) { lmx_close(&f); return; }
    lmx_obj_t g0[7];
    lmx_group(&f, 0, 5, g0);
    lmx_make(&f, 0, 5, LMX_FULL, &g0[5]);
    lmx_make(&f, 0, 6, LMX_AUTO, &g0[6]);
    lmx_push(&f, 0, 0, &g0[0]);
    lmx_push(&f, 0, 1, &g0[1]);
    lmx_push(&f, 0, 3, &g0[3]);    /* (0, 2) never arrives */
    lmx_push(&f, 0, 4, &g0[4]);
    lmx_push(&f, 0, 5, &g0[5]);    /* a full header recovers */
    lmx_push(&f, 0, 6, &g0[6]);
    lmx_poll(&f);
    MOQ_TEST_CHECK(lmx_count(&f, 0) == 4);
    MOQ_TEST_CHECK(lmx_rebuilt(&f, 0, 0, &g0[0]) && lmx_rebuilt(&f, 0, 1, &g0[1]));
    MOQ_TEST_CHECK(lmx_rebuilt(&f, 0, 5, &g0[5]) && lmx_rebuilt(&f, 0, 6, &g0[6]));
    const lmx_rec_t *r5 = lmx_find(&f, 0, 5), *r6 = lmx_find(&f, 0, 6);
    MOQ_TEST_CHECK(r5 && r5->gap && r5->gap_group == 0 && r5->gap_object == 2);
    MOQ_TEST_CHECK(r6 && !r6->gap);
    MOQ_TEST_CHECK(lmx_stats(&f).locmaf_objects_undecodable == 2);
    MOQ_TEST_CHECK(lmx_balanced(&f, 0));
    lmx_close(&f);
}

/* A duplicate is dropped before decode, without loss. */
static void test_duplicate(void)
{
    ROW("duplicate");
    lmx_t f;
    if (!lmx_open(&f, g_draft)) { lmx_close(&f); return; }
    lmx_obj_t g0[3];
    lmx_group(&f, 0, 3, g0);
    lmx_push(&f, 0, 0, &g0[0]);
    lmx_push(&f, 0, 1, &g0[1]);
    lmx_push(&f, 0, 1, &g0[1]);
    lmx_push(&f, 0, 0, &g0[0]);
    lmx_push(&f, 0, 2, &g0[2]);
    lmx_poll(&f);
    MOQ_TEST_CHECK(lmx_count(&f, 0) == 3);
    MOQ_TEST_CHECK(lmx_rebuilt(&f, 0, 2, &g0[2]));
    const lmx_rec_t *r2 = lmx_find(&f, 0, 2);
    MOQ_TEST_CHECK(r2 && !r2->gap);
    moq_media_receiver_stats_t st = lmx_stats(&f);
    MOQ_TEST_CHECK(st.locmaf_objects_undecodable == 2 && st.locmaf_objects_rebuilt == 3);
    MOQ_TEST_CHECK(lmx_balanced(&f, 0));
    lmx_close(&f);
}

/* -- Open Groups ------------------------------------------------------------- */

static void test_interleave_tail_after_next_head(void)
{
    ROW("interleave_tail_after_next_head");
    lmx_t f;
    if (!lmx_open(&f, g_draft)) { lmx_close(&f); return; }
    lmx_obj_t a[6], b[6];
    lmx_group(&f, 0, 6, a);
    lmx_group(&f, 1, 6, b);
    lmx_push(&f, 0, 0, &a[0]);
    lmx_push(&f, 1, 0, &b[0]);
    for (uint64_t o = 1; o < 6; o++) lmx_push(&f, 0, o, &a[o]);
    for (uint64_t o = 1; o < 6; o++) lmx_push(&f, 1, o, &b[o]);
    lmx_poll(&f);
    MOQ_TEST_CHECK(f.got_count == 12);
    for (uint64_t o = 0; o < 6; o++)
        MOQ_TEST_CHECK(lmx_rebuilt(&f, 0, o, &a[o]) && lmx_rebuilt(&f, 1, o, &b[o]));
    for (size_t i = 0; i < f.got_count; i++) MOQ_TEST_CHECK(!f.got[i].gap);
    MOQ_TEST_CHECK(lmx_in_order(&f, 0) && lmx_in_order(&f, 1));
    MOQ_TEST_CHECK(lmx_stats(&f).locmaf_groups_evicted == 0);
    MOQ_TEST_CHECK(lmx_balanced(&f, 0));
    lmx_close(&f);
}

static void test_interleave_alternating(void)
{
    ROW("interleave_alternating");
    lmx_t f;
    if (!lmx_open(&f, g_draft)) { lmx_close(&f); return; }
    lmx_obj_t a[5], b[5];
    lmx_group(&f, 4, 5, a);
    lmx_group(&f, 5, 5, b);
    for (uint64_t o = 0; o < 5; o++) {
        lmx_push(&f, 4, o, &a[o]);
        lmx_push(&f, 5, o, &b[o]);
    }
    lmx_poll(&f);
    MOQ_TEST_CHECK(f.got_count == 10);
    for (uint64_t o = 0; o < 5; o++)
        MOQ_TEST_CHECK(lmx_rebuilt(&f, 4, o, &a[o]) && lmx_rebuilt(&f, 5, o, &b[o]));
    MOQ_TEST_CHECK(lmx_balanced(&f, 0));
    lmx_close(&f);
}

static void test_reset_one_group_other_active(void)
{
    ROW("reset_one_group_other_active");
    lmx_t f;
    if (!lmx_open(&f, g_draft)) { lmx_close(&f); return; }
    lmx_obj_t a[6], b[4], c[1];
    lmx_group(&f, 0, 4, a);
    lmx_make(&f, 0, 4, LMX_FULL, &a[4]);
    lmx_make(&f, 0, 5, LMX_AUTO, &a[5]);
    lmx_group(&f, 1, 4, b);
    lmx_group(&f, 2, 1, c);
    for (uint64_t o = 0; o < 3; o++) lmx_push(&f, 0, o, &a[o]);
    for (uint64_t o = 0; o < 3; o++) lmx_push(&f, 1, o, &b[o]);
    lmx_terminal(&f, MOQ_SUB_STREAM_RESET, 0, 0);
    receiver_lm_t *st = ((receiver_locmaf_t *)f.t->lm)->stage;
    MOQ_TEST_CHECK(receiver_lm_slot_state(st, 0) == LM_SLOT_BARRIER);
    lmx_push(&f, 0, 3, &a[3]);      /* dropped: no reference after the reset */
    lmx_push(&f, 0, 4, &a[4]);      /* a full header recovers the Group */
    lmx_push(&f, 1, 3, &b[3]);
    MOQ_TEST_CHECK(receiver_lm_slot_state(st, 0) == LM_SLOT_RUNNING);
    lmx_terminal(&f, MOQ_SUB_STREAM_RESET, 0, 0);
    lmx_push(&f, 2, 0, &c[0]);      /* takes the reset Group's slot */
    lmx_push(&f, 0, 5, &a[5]);      /* late: the Group closed */
    lmx_poll(&f);
    MOQ_TEST_CHECK(lmx_count(&f, 0) == 4 && lmx_count(&f, 1) == 4 && lmx_count(&f, 2) == 1);
    MOQ_TEST_CHECK(lmx_rebuilt(&f, 0, 4, &a[4]) && lmx_rebuilt(&f, 2, 0, &c[0]));
    for (uint64_t o = 0; o < 4; o++) MOQ_TEST_CHECK(lmx_rebuilt(&f, 1, o, &b[o]));
    moq_media_receiver_stats_t s = lmx_stats(&f);
    MOQ_TEST_CHECK(s.locmaf_resets == 2 && s.locmaf_groups_evicted == 0);
    MOQ_TEST_CHECK(s.locmaf_objects_late == 1 && s.locmaf_objects_undecodable == 1);
    MOQ_TEST_CHECK(lmx_balanced(&f, 0));
    lmx_close(&f);
}

static void test_fin_one_group_other_active(void)
{
    ROW("fin_one_group_other_active");
    lmx_t f;
    if (!lmx_open(&f, g_draft)) { lmx_close(&f); return; }
    lmx_obj_t a[3], b[3];
    lmx_group(&f, 0, 3, a);
    lmx_group(&f, 1, 3, b);
    lmx_push(&f, 0, 0, &a[0]);
    lmx_push(&f, 0, 1, &a[1]);
    lmx_push(&f, 1, 0, &b[0]);
    lmx_terminal(&f, MOQ_SUB_STREAM_FIN_END_OF_GROUP, 0, 0);
    receiver_lm_t *st = ((receiver_locmaf_t *)f.t->lm)->stage;
    MOQ_TEST_CHECK(receiver_lm_slot_state(st, 0) == LM_SLOT_FREE);
    lmx_push(&f, 1, 1, &b[1]);
    lmx_push(&f, 0, 2, &a[2]);      /* late */
    lmx_push(&f, 1, 2, &b[2]);
    lmx_terminal(&f, MOQ_SUB_STREAM_FIN, 1, 1);
    MOQ_TEST_CHECK(receiver_lm_slot_state(st, 1) == LM_SLOT_FREE);
    lmx_poll(&f);
    MOQ_TEST_CHECK(lmx_count(&f, 0) == 2 && lmx_count(&f, 1) == 3);
    moq_media_receiver_stats_t s = lmx_stats(&f);
    MOQ_TEST_CHECK(s.locmaf_objects_late == 1 && s.locmaf_groups_evicted == 0);
    MOQ_TEST_CHECK(lmx_balanced(&f, 0));
    lmx_close(&f);
}

static void test_eog_status_retires(void)
{
    ROW("eog_status_retires");
    lmx_t f;
    if (!lmx_open(&f, g_draft)) { lmx_close(&f); return; }
    lmx_obj_t a[4];
    lmx_group(&f, 0, 4, a);
    for (uint64_t o = 0; o < 3; o++) lmx_push(&f, 0, o, &a[o]);
    lmx_status(&f, 0, 3, MOQ_OBJECT_END_OF_GROUP);
    receiver_lm_t *st = ((receiver_locmaf_t *)f.t->lm)->stage;
    MOQ_TEST_CHECK(receiver_lm_slot_state(st, 0) == LM_SLOT_FREE);
    lmx_push(&f, 0, 3, &a[3]);      /* late */
    lmx_poll(&f);
    MOQ_TEST_CHECK(lmx_count(&f, 0) == 3);
    size_t statuses = 0;
    for (size_t i = 0; i < f.got_count; i++) statuses += f.got[i].status;
    MOQ_TEST_CHECK(statuses == 1 && f.got_count == 4 && f.got[3].status);
    MOQ_TEST_CHECK(lmx_stats(&f).locmaf_objects_late == 1);
    MOQ_TEST_CHECK(lmx_balanced(&f, 0));
    lmx_close(&f);
}

static void test_capacity_evicts_lowest(void)
{
    ROW("capacity_evicts_lowest");
    lmx_t f;
    if (!lmx_open(&f, g_draft)) { lmx_close(&f); return; }
    lmx_obj_t a[5], b[4], c[3];
    lmx_group(&f, 0, 5, a);
    lmx_group(&f, 1, 4, b);
    lmx_group(&f, 2, 3, c);
    lmx_push(&f, 0, 0, &a[0]);
    lmx_push(&f, 0, 1, &a[1]);
    lmx_push(&f, 1, 0, &b[0]);
    lmx_push(&f, 1, 1, &b[1]);
    lmx_push(&f, 2, 0, &c[0]);      /* evicts Group 0 */
    for (uint64_t o = 2; o < 5; o++) lmx_push(&f, 0, o, &a[o]);
    lmx_push(&f, 1, 2, &b[2]);
    lmx_push(&f, 1, 3, &b[3]);
    lmx_push(&f, 2, 1, &c[1]);
    lmx_push(&f, 2, 2, &c[2]);
    lmx_poll(&f);
    MOQ_TEST_CHECK(lmx_count(&f, 0) == 2 && lmx_count(&f, 1) == 4 && lmx_count(&f, 2) == 3);
    moq_media_receiver_stats_t s = lmx_stats(&f);
    MOQ_TEST_CHECK(s.locmaf_groups_evicted == 1 && s.locmaf_objects_late == 3);
    /* The late Objects are a loss on the next delivered Object. */
    const lmx_rec_t *n = lmx_find(&f, 1, 2);
    MOQ_TEST_CHECK(n && n->gap && n->gap_group == 0 && n->gap_object == 2);
    MOQ_TEST_CHECK(lmx_balanced(&f, 0));
    lmx_close(&f);
}

static void test_capacity_refuses_older(void)
{
    ROW("capacity_refuses_older");
    lmx_t f;
    if (!lmx_open(&f, g_draft)) { lmx_close(&f); return; }
    lmx_obj_t a[3], b[2], c[2];
    lmx_group(&f, 0, 3, a);
    lmx_group(&f, 1, 2, b);
    lmx_group(&f, 2, 2, c);
    lmx_push(&f, 1, 0, &b[0]);
    lmx_push(&f, 2, 0, &c[0]);
    for (uint64_t o = 0; o < 3; o++) lmx_push(&f, 0, o, &a[o]);
    lmx_push(&f, 1, 1, &b[1]);
    lmx_push(&f, 2, 1, &c[1]);
    lmx_poll(&f);
    MOQ_TEST_CHECK(lmx_count(&f, 0) == 0 && lmx_count(&f, 1) == 2 && lmx_count(&f, 2) == 2);
    moq_media_receiver_stats_t s = lmx_stats(&f);
    MOQ_TEST_CHECK(s.locmaf_groups_evicted == 1 && s.locmaf_objects_late == 3);
    MOQ_TEST_CHECK(lmx_balanced(&f, 0));
    lmx_close(&f);
}

static void test_closed_record_floor(void)
{
    ROW("closed_record_floor");
    lmx_t f;
    if (!lmx_open(&f, g_draft)) { lmx_close(&f); return; }
    lmx_obj_t first[2], one;
    lmx_group(&f, 0, 2, first);
    lmx_push(&f, 0, 0, &first[0]);
    lmx_terminal(&f, MOQ_SUB_STREAM_FIN, 0, 0);
    for (uint64_t g = 1; g < 9; g++) {
        lmx_make(&f, g, 0, LMX_AUTO, &one);
        lmx_push(&f, g, 0, &one);
        lmx_terminal(&f, MOQ_SUB_STREAM_FIN, g, g);
    }
    /* Group 0 left the ring of eight; the floor still closes it. */
    lmx_push(&f, 0, 1, &first[1]);
    lmx_poll(&f);
    MOQ_TEST_CHECK(lmx_count(&f, 0) == 1);
    MOQ_TEST_CHECK(lmx_stats(&f).locmaf_objects_late == 1);
    MOQ_TEST_CHECK(lmx_balanced(&f, 0));
    lmx_close(&f);
}

static void test_barrier_per_group(void)
{
    ROW("barrier_per_group");
    lmx_t f;
    if (!lmx_open(&f, g_draft)) { lmx_close(&f); return; }
    lmx_obj_t a[4], b[4];
    lmx_group(&f, 0, 4, a);
    lmx_group(&f, 1, 4, b);
    lmx_push(&f, 0, 0, &a[0]);
    lmx_push(&f, 1, 0, &b[0]);
    lmx_push(&f, 0, 2, &a[2]);      /* gap in Group 0 */
    lmx_push(&f, 1, 1, &b[1]);
    lmx_push(&f, 0, 3, &a[3]);
    lmx_push(&f, 1, 2, &b[2]);
    lmx_push(&f, 1, 3, &b[3]);
    lmx_poll(&f);
    MOQ_TEST_CHECK(lmx_count(&f, 0) == 1 && lmx_count(&f, 1) == 4);
    for (uint64_t o = 0; o < 4; o++) MOQ_TEST_CHECK(lmx_rebuilt(&f, 1, o, &b[o]));
    MOQ_TEST_CHECK(lmx_balanced(&f, 0));
    lmx_close(&f);
}

/* -- rawBoxes (section 9) ------------------------------------------------------ */

static void test_raw_then_delta_live(void)
{
    ROW("raw_then_delta_live");
    lmx_t f;
    if (!lmx_open(&f, g_draft)) { lmx_close(&f); return; }
    lmx_obj_t a[4], raw, b4, b5;
    lmx_group(&f, 0, 4, a);          /* a[3] is a delta against (0, 2) */
    moq_locmaf_encoder_reset(&f.enc);
    lmx_obj_t skip[2];
    lmx_group(&f, 0, 2, skip);
    lmx_make(&f, 0, 2, LMX_RAW, &raw);
    lmx_make(&f, 0, 4, LMX_FULL, &b4);
    lmx_make(&f, 0, 5, LMX_AUTO, &b5);
    lmx_push(&f, 0, 0, &a[0]);
    lmx_push(&f, 0, 1, &a[1]);
    lmx_push(&f, 0, 2, &raw);
    receiver_lm_t *st = ((receiver_locmaf_t *)f.t->lm)->stage;
    MOQ_TEST_CHECK(receiver_lm_slot_state(st, 0) == LM_SLOT_RAW);
    lmx_push(&f, 0, 3, &a[3]);       /* refused: a delta after rawBoxes */
    MOQ_TEST_CHECK(receiver_lm_slot_state(st, 0) == LM_SLOT_BARRIER);
    lmx_push(&f, 0, 4, &b4);
    lmx_push(&f, 0, 5, &b5);
    lmx_poll(&f);
    MOQ_TEST_CHECK(lmx_count(&f, 0) == 5);
    MOQ_TEST_CHECK(lmx_rebuilt(&f, 0, 2, &raw));   /* the boxes, verbatim */
    MOQ_TEST_CHECK(lmx_rebuilt(&f, 0, 4, &b4) && lmx_rebuilt(&f, 0, 5, &b5));
    const lmx_rec_t *r4 = lmx_find(&f, 0, 4);
    MOQ_TEST_CHECK(r4 && r4->gap && r4->gap_object == 3);
    moq_media_receiver_stats_t s = lmx_stats(&f);
    MOQ_TEST_CHECK(s.locmaf_objects_undecodable == 1 && s.parse_drops == 1);
    MOQ_TEST_CHECK(lmx_balanced(&f, 0));
    lmx_close(&f);
}

static void test_raw_ends_barrier_for_delivery_only(void)
{
    ROW("raw_ends_barrier_for_delivery_only");
    lmx_t f;
    if (!lmx_open(&f, g_draft)) { lmx_close(&f); return; }
    lmx_obj_t a[5], raw, full;
    lmx_group(&f, 0, 5, a);          /* a[4] is a delta against (0, 3) */
    moq_locmaf_encoder_reset(&f.enc);
    lmx_make(&f, 0, 3, LMX_RAW, &raw);
    lmx_make(&f, 0, 5, LMX_FULL, &full);
    lmx_push(&f, 0, 0, &a[0]);
    lmx_push(&f, 0, 1, &a[1]);
    lmx_push(&f, 0, 3, &raw);        /* after a gap: delivered */
    lmx_push(&f, 0, 4, &a[4]);       /* refused */
    lmx_push(&f, 0, 5, &full);       /* recovers */
    lmx_poll(&f);
    MOQ_TEST_CHECK(lmx_count(&f, 0) == 4);
    MOQ_TEST_CHECK(lmx_rebuilt(&f, 0, 3, &raw) && lmx_rebuilt(&f, 0, 5, &full));
    MOQ_TEST_CHECK(!lmx_find(&f, 0, 4));
    const lmx_rec_t *r3 = lmx_find(&f, 0, 3);
    MOQ_TEST_CHECK(r3 && r3->gap && r3->gap_object == 2);
    MOQ_TEST_CHECK(lmx_balanced(&f, 0));
    lmx_close(&f);
}

/* rawBoxes carrying a CMAF Header in band, and a malformed Object. */
static void test_raw_init_refused(void)
{
    ROW("raw_init_refused");
    lmx_t f;
    if (!lmx_open(&f, g_draft)) { lmx_close(&f); return; }
    lmx_obj_t bad, ok[2];
    /* vi64(4), then the boxes: an ftyp and a moov. */
    uint8_t init[1 + 512];
    init[0] = 0x04;
    size_t init_len = 1 + lmx_init(init + 1);
    lmx_make(&f, 0, 1, LMX_BAD, &bad);
    lmx_group(&f, 1, 2, ok);
    f.pushed++;
    moq_media_receiver_test_push_object_ex(f.r, f.t, 0, 0, 0, 0x80, MOQ_OBJECT_NORMAL,
        false, false, init, init_len, NULL, 0);
    lmx_push(&f, 0, 1, &bad);
    lmx_push(&f, 1, 0, &ok[0]);
    lmx_push(&f, 1, 1, &ok[1]);
    lmx_poll(&f);
    MOQ_TEST_CHECK(lmx_count(&f, 0) == 0 && lmx_count(&f, 1) == 2);
    moq_media_receiver_stats_t s = lmx_stats(&f);
    MOQ_TEST_CHECK(s.locmaf_objects_undecodable == 2 && s.parse_drops == 2);
    MOQ_TEST_CHECK(lmx_balanced(&f, 0));
    lmx_close(&f);
}

/* -- Delivery rules a publisher breaks (section 3) ----------------------------- */

static void test_datagram(void)
{
    ROW("datagram");
    lmx_t f;
    if (!lmx_open(&f, g_draft)) { lmx_close(&f); return; }
    lmx_obj_t a[4], b[1];
    lmx_group(&f, 0, 4, a);
    lmx_group(&f, 1, 1, b);
    lmx_push(&f, 0, 0, &a[0]);
    lmx_push(&f, 0, 1, &a[1]);
    lmx_push_ex(&f, 0, 0, 2, &a[2], true, NULL, 0);
    lmx_push(&f, 0, 3, &a[3]);
    lmx_push(&f, 1, 0, &b[0]);
    lmx_poll(&f);
    MOQ_TEST_CHECK(lmx_count(&f, 0) == 2 && lmx_count(&f, 1) == 1);
    moq_media_receiver_stats_t s = lmx_stats(&f);
    MOQ_TEST_CHECK(s.locmaf_resets == 1 && s.locmaf_objects_undecodable == 2);
    MOQ_TEST_CHECK(lmx_balanced(&f, 0));
    lmx_close(&f);
}

static void test_second_subgroup(void)
{
    ROW("second_subgroup");
    lmx_t f;
    if (!lmx_open(&f, g_draft)) { lmx_close(&f); return; }
    lmx_obj_t a[3];
    lmx_group(&f, 0, 3, a);
    lmx_push_ex(&f, 0, 0, 0, &a[0], false, NULL, 0);
    lmx_push_ex(&f, 0, 7, 1, &a[1], false, NULL, 0);
    lmx_push_ex(&f, 0, 0, 2, &a[2], false, NULL, 0);
    lmx_poll(&f);
    MOQ_TEST_CHECK(lmx_count(&f, 0) == 1);
    moq_media_receiver_stats_t s = lmx_stats(&f);
    MOQ_TEST_CHECK(s.locmaf_resets == 1 && s.locmaf_objects_undecodable == 2);
    MOQ_TEST_CHECK(lmx_balanced(&f, 0));
    lmx_close(&f);
}

/* -- Selection and generations ------------------------------------------------- */

static void test_disable_resume(void)
{
    ROW("disable_resume");
    lmx_t f;
    if (!lmx_open(&f, g_draft)) { lmx_close(&f); return; }
    lmx_obj_t a[5];
    lmx_group(&f, 0, 4, a);
    lmx_make(&f, 0, 4, LMX_FULL, &a[4]);
    lmx_push(&f, 0, 0, &a[0]);
    lmx_push(&f, 0, 1, &a[1]);
    lmx_poll(&f);
    MOQ_TEST_CHECK(moq_media_receiver_unsubscribe_track(f.r, f.t) == MOQ_OK);
    moq_media_receiver_test_push_object_ex(f.r, f.t, 0, 0, 2, 0x80, MOQ_OBJECT_NORMAL,
        false, false, a[2].wire, a[2].wire_len, NULL, 0);   /* dropped unread */
    MOQ_TEST_CHECK(moq_media_receiver_subscribe_track(f.r, f.t, NULL) == MOQ_OK);
    lmx_push(&f, 0, 3, &a[3]);       /* the decoder was reset */
    lmx_push(&f, 0, 4, &a[4]);
    lmx_poll(&f);
    MOQ_TEST_CHECK(lmx_count(&f, 0) == 3);
    MOQ_TEST_CHECK(lmx_rebuilt(&f, 0, 4, &a[4]));
    moq_media_receiver_stats_t s = lmx_stats(&f);
    MOQ_TEST_CHECK(s.locmaf_resets == 1 && s.locmaf_objects_undecodable == 1);
    MOQ_TEST_CHECK(lmx_balanced(&f, 0));
    lmx_close(&f);
}

/* An Object of an earlier subscription is dropped before decode (drafts 16
 * and 18: draft 22 reads the facade generation's Track Properties). */
static void test_old_generation(void)
{
    if (g_draft == MOQ_VERSION_DRAFT_22) return;
    ROW("old_generation");
    lmx_t f;
    if (!lmx_open(&f, g_draft)) { lmx_close(&f); return; }
    static char gen_a, gen_b;
    lmx_obj_t a[2];
    lmx_group(&f, 0, 2, a);
    f.t->sub_track = (moq_sub_track_t *)(void *)&gen_a;
    lmx_push(&f, 0, 0, &a[0]);
    f.t->sub_track = (moq_sub_track_t *)(void *)&gen_b;
    moq_sub_object_t so;
    memset(&so, 0, sizeof(so));
    so.track = (moq_sub_track_t *)(void *)&gen_a;
    so.group_id = 0;
    so.object_id = 1;
    so.status = MOQ_OBJECT_NORMAL;
    MOQ_TEST_CHECK(moq_rcbuf_create(&f.alloc, a[1].wire, a[1].wire_len, &so.payload) == MOQ_OK);
    f.pushed++;
    receiver_on_locmaf_object(f.r, f.t, &so, 0);
    f.t->sub_track = NULL;
    lmx_poll(&f);
    MOQ_TEST_CHECK(lmx_count(&f, 0) == 1);
    MOQ_TEST_CHECK(lmx_stats(&f).locmaf_objects_undecodable == 1);
    MOQ_TEST_CHECK(lmx_balanced(&f, 0));
    lmx_close(&f);
}

/* -- Delivery paths ------------------------------------------------------------ */

static bool open_path(const char *version, const char *extra, bool *has_path,
                      moq_result_t *subscribe_rc)
{
    lmx_t f;
    memset(&f, 0, sizeof(f));
    f.alloc = test_allocator(&f.state);
    moq_media_receiver_cfg_t cfg;
    moq_media_receiver_cfg_init_live_sized(&cfg, sizeof(cfg));
    static moq_bytes_t ns[] = { { (const uint8_t *)"lm", 2 } };
    cfg.namespace_ = (moq_namespace_t){ ns, 1 };
    cfg.companion_profile = g_draft == MOQ_VERSION_DRAFT_22
        ? MOQ_MEDIA_COMPANION_D22_MSF01_LOC05_PUBLIC
        : MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01;
    moq_media_receiver_test_set_alloc(&f.alloc);
    f.r = moq_media_receiver_test_new_cfg(&cfg);
    moq_media_receiver_test_set_alloc(NULL);
    if (!f.r) return false;
    moq_media_receiver_test_latch_session_version(f.r, g_draft);
    char json[4096];
    lmx_catalog(json, sizeof(json), version, extra);
    moq_media_receiver_test_ingest(f.r, 0, 0, json, strlen(json));
    moq_media_track_event_t ev;
    while (moq_media_receiver_poll_track(f.r, &ev, sizeof(ev)) == MOQ_OK)
        if (ev.kind == MOQ_MEDIA_TRACK_ADDED) f.t = ev.track;
    bool ok = f.t != NULL;
    if (ok) {
        *has_path = f.t->has_media_path;
        *subscribe_rc = moq_media_receiver_subscribe_track(f.r, f.t, NULL);
    }
    lmx_close(&f);
    return ok;
}

static void test_no_delivery_path(void)
{
    ROW("no_delivery_path");
    bool path = false;
    moq_result_t rc = MOQ_OK;
    /* The control: a supported version has a path. */
    MOQ_TEST_CHECK(open_path("draft-01", ",\"locmafVersion\":\"0.3\"", &path, &rc));
    MOQ_TEST_CHECK(path && rc == MOQ_OK);
    static const struct { const char *version, *extra; } rows[] = {
        { "draft-01", ",\"locmafVersion\":\"0.4\"" },
        { "draft-02", ",\"locmafVersion\":\"0.4\"" },
        { "draft-01", "" },
        { "draft-01", ",\"locmafVersion\":3" },
        { "draft-01", ",\"locmafVersion\":\"0.3\",\"encryptionScheme\":\"moq-secure-objects\"" },
        { "draft-02", ",\"locmafVersion\":\"0.3\",\"encryptionScheme\":\"moq-secure-objects\"" },
    };
    for (size_t i = 0; i < sizeof(rows) / sizeof(rows[0]); i++) {
        path = true;
        rc = MOQ_OK;
        MOQ_TEST_CHECK(open_path(rows[i].version, rows[i].extra, &path, &rc));
        if (path || rc != MOQ_ERR_UNSUPPORTED)
            fprintf(stderr, "  row %zu: path %d rc %d\n", i, (int)path, (int)rc);
        MOQ_TEST_CHECK(!path && rc == MOQ_ERR_UNSUPPORTED);
    }
}

/* MSF-02 init carried in a track property: no path for a CMAF Header. */
static void test_property_init(void)
{
    ROW("property_init");
    lmx_t f;
    memset(&f, 0, sizeof(f));
    f.alloc = test_allocator(&f.state);
    moq_media_receiver_cfg_t cfg;
    moq_media_receiver_cfg_init_live_sized(&cfg, sizeof(cfg));
    static moq_bytes_t ns[] = { { (const uint8_t *)"lm", 2 } };
    cfg.namespace_ = (moq_namespace_t){ ns, 1 };
    cfg.companion_profile = MOQ_MEDIA_COMPANION_D22_MSF01_LOC05_PUBLIC;
    moq_media_receiver_test_set_alloc(&f.alloc);
    f.r = moq_media_receiver_test_new_cfg(&cfg);
    moq_media_receiver_test_set_alloc(NULL);
    if (!f.r) return;
    moq_media_receiver_test_latch_session_version(f.r, MOQ_VERSION_DRAFT_22);
    static const char json[] =
        "{\"version\":\"draft-02\",\"initDataList\":[{\"id\":\"i\","
        "\"type\":\"track-property\",\"data\":\"0D\"}],\"tracks\":[{\"name\":\"v\","
        "\"packaging\":\"locmaf\",\"isLive\":true,\"role\":\"video\","
        "\"codec\":\"avc1.64001f\",\"initRef\":\"i\",\"locmafVersion\":\"0.3\"}]}";
    moq_media_receiver_test_ingest(f.r, 0, 0, json, strlen(json));
    moq_media_track_event_t ev;
    while (moq_media_receiver_poll_track(f.r, &ev, sizeof(ev)) == MOQ_OK)
        if (ev.kind == MOQ_MEDIA_TRACK_ADDED) f.t = ev.track;
    MOQ_TEST_CHECK(f.t && !f.t->has_media_path);
    if (f.t)
        MOQ_TEST_CHECK(moq_media_receiver_subscribe_track(f.r, f.t, NULL) ==
                       MOQ_ERR_UNSUPPORTED);
    lmx_close(&f);
}

/* Draft 22: the Object's LOC properties surface on the rebuilt chunk. */
static void test_d22_properties(void)
{
    if (g_draft != MOQ_VERSION_DRAFT_22) return;
    ROW("d22_properties");
    lmx_t f;
    if (!lmx_open(&f, g_draft)) { lmx_close(&f); return; }
    moq_loc_metadata_t loc;
    moq_loc_metadata_init_sized(&loc, sizeof(loc));
    loc.present = MOQ_LOC_METADATA_HAS_TIMESTAMP;
    loc.timestamp = 1700000000000000ull;
    moq_rcbuf_t *props = NULL;
    MOQ_TEST_CHECK(moq_loc_encode_metadata_ex(&f.alloc, MOQ_VERSION_DRAFT_22,
        MOQ_LOC_PROFILE_05, &loc, NULL, &props) == MOQ_OK);
    lmx_obj_t a[2];
    lmx_group(&f, 0, 2, a);
    if (props) {
        lmx_push_ex(&f, 0, 0, 0, &a[0], false, moq_rcbuf_data(props), moq_rcbuf_len(props));
        lmx_push(&f, 0, 1, &a[1]);
        moq_rcbuf_decref(props);
    }
    lmx_poll(&f);
    MOQ_TEST_CHECK(lmx_rebuilt(&f, 0, 0, &a[0]) && lmx_rebuilt(&f, 0, 1, &a[1]));
    const lmx_rec_t *r0 = lmx_find(&f, 0, 0), *r1 = lmx_find(&f, 0, 1);
    MOQ_TEST_CHECK(r0 && r0->has_time && r0->time_us == 1700000000000000ull);
    MOQ_TEST_CHECK(r1 && !r1->has_time);
    MOQ_TEST_CHECK(lmx_balanced(&f, 0));
    lmx_close(&f);
}

/* -- Revisions and companions (the catalog x companion matrix, LIVE) ----------- */

static void test_matrix(void)
{
    static const struct {
        const char *name;
        moq_version_t v;
        moq_media_companion_profile_t c;
    } rows[] = {
        { "legacy-16", MOQ_VERSION_DRAFT_16, MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01 },
        { "legacy-18", MOQ_VERSION_DRAFT_18, MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01 },
        { "loc04-22", MOQ_VERSION_DRAFT_22, MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC },
        { "loc05-22", MOQ_VERSION_DRAFT_22, MOQ_MEDIA_COMPANION_D22_MSF01_LOC05_PUBLIC },
        { "auto-16", MOQ_VERSION_DRAFT_16, MOQ_MEDIA_COMPANION_AUTO },
        { "auto-18", MOQ_VERSION_DRAFT_18, MOQ_MEDIA_COMPANION_AUTO },
        { "auto-22", MOQ_VERSION_DRAFT_22, MOQ_MEDIA_COMPANION_AUTO },
    };
    static const char *const versions[] = { "draft-01", "draft-02" };
    for (size_t vi = 0; vi < 2; vi++) {
        for (size_t i = 0; i < sizeof(rows) / sizeof(rows[0]); i++) {
            char name[64];
            snprintf(name, sizeof(name), "matrix %s %s", versions[vi], rows[i].name);
            ROW(name);
            lmx_t f;
            if (!lmx_open_full(&f, rows[i].v, rows[i].c, versions[vi],
                               ",\"locmafVersion\":\"0.3\"", false, false)) {
                fprintf(stderr, "  %s: no path\n", name);
                failures++;
                lmx_close(&f);
                continue;
            }
            lmx_obj_t a[4];
            lmx_group(&f, 3, 4, a);
            for (uint64_t o = 0; o < 4; o++) lmx_push(&f, 3, o, &a[o]);
            lmx_poll(&f);
            for (uint64_t o = 0; o < 4; o++) MOQ_TEST_CHECK(lmx_rebuilt(&f, 3, o, &a[o]));
            MOQ_TEST_CHECK(lmx_balanced(&f, 0));
            lmx_close(&f);
        }
    }
}

/* MSF-02 streamMapping=object on a locmaf track is ignored: no ordering
 * stage, and a second subgroup still resets. */
static void test_stream_mapping_ignored(void)
{
    ROW("stream_mapping_ignored");
    lmx_t f;
    if (!lmx_open_full(&f, g_draft, g_draft == MOQ_VERSION_DRAFT_22
                           ? MOQ_MEDIA_COMPANION_D22_MSF01_LOC05_PUBLIC
                           : MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01,
                       "draft-02",
                       ",\"locmafVersion\":\"0.3\",\"packagingMetadata\":\"streamMapping=object\"",
                       false, false)) { lmx_close(&f); return; }
    MOQ_TEST_CHECK(!f.t->ordered);
    lmx_obj_t a[2];
    lmx_group(&f, 0, 2, a);
    lmx_push_ex(&f, 0, 0, 0, &a[0], false, NULL, 0);
    lmx_push_ex(&f, 0, 1, 1, &a[1], false, NULL, 0);
    lmx_poll(&f);
    MOQ_TEST_CHECK(lmx_count(&f, 0) == 1 && lmx_stats(&f).locmaf_resets == 1);
    MOQ_TEST_CHECK(lmx_balanced(&f, 0));
    lmx_close(&f);
}

/* -- MSF-02 updates ------------------------------------------------------------- */

static void test_advisory_update_keeps_chain(void)
{
    ROW("advisory_update_keeps_chain");
    lmx_t f;
    if (!lmx_open_full(&f, g_draft, g_draft == MOQ_VERSION_DRAFT_22
                           ? MOQ_MEDIA_COMPANION_D22_MSF01_LOC05_PUBLIC
                           : MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01,
                       "draft-02", ",\"locmafVersion\":\"0.3\",\"bitrate\":1000",
                       false, false)) { lmx_close(&f); return; }
    lmx_obj_t a[4];
    lmx_group(&f, 0, 4, a);
    lmx_push(&f, 0, 0, &a[0]);
    lmx_push(&f, 0, 1, &a[1]);
    static const char upd[] =
        "{\"generatedAt\":9,\"deltaUpdate\":[{\"op\":\"update\",\"tracks\":["
        "{\"parentName\":\"v\",\"bitrate\":2000}]}]}";
    moq_media_receiver_test_ingest(f.r, 0, 1, upd, strlen(upd));
    bool updated = false, removed = false;
    moq_media_track_event_t ev;
    while (moq_media_receiver_poll_track(f.r, &ev, sizeof(ev)) == MOQ_OK) {
        if (ev.kind == MOQ_MEDIA_TRACK_UPDATED && ev.track == f.t) updated = true;
        if (ev.kind == MOQ_MEDIA_TRACK_REMOVED) removed = true;
    }
    MOQ_TEST_CHECK(updated && !removed);
    lmx_push(&f, 0, 2, &a[2]);
    lmx_push(&f, 0, 3, &a[3]);
    lmx_poll(&f);
    for (uint64_t o = 0; o < 4; o++) MOQ_TEST_CHECK(lmx_rebuilt(&f, 0, o, &a[o]));
    MOQ_TEST_CHECK(lmx_balanced(&f, 0));
    lmx_close(&f);
}

/* A critical change replaces the handle; its decoder rebuilds an Object of
 * a Group encoded under the earlier Header with the new Header's facts: a
 * LOCMAF Object carries no configuration identity. A field the Object left
 * to the trex default is the new default now, and the canonical chunk,
 * which omits trex-equal defaults, has the same bytes: nothing in the
 * delivered Object tells the application. */
static void test_stale_group_after_replacement(void)
{
    ROW("stale_group_after_replacement");
    lmx_t f;
    if (!lmx_open_full(&f, g_draft, g_draft == MOQ_VERSION_DRAFT_22
                           ? MOQ_MEDIA_COMPANION_D22_MSF01_LOC05_PUBLIC
                           : MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01,
                       "draft-02", ",\"locmafVersion\":\"0.3\"", false, false)) {
        lmx_close(&f);
        return;
    }
    lmx_obj_t old[1];
    lmx_group(&f, 7, 1, old);        /* its duration is the trex default, 1024 */
    /* A new independent catalog whose Header's trex duration is 2048. */
    static const uint32_t trex4[4] = { 1, 2048, 0, 0x02000000u };
    uint8_t init[512];
    char b64[800], json[4096];
    lmx_b64(init, moq_test_build_cmaf_init_trex(init, 1, 48000, trex4, 1, true), b64);
    snprintf(json, sizeof(json),
             "{\"version\":\"draft-02\",\"initDataList\":[{\"id\":\"i\","
             "\"type\":\"inline\",\"data\":\"%s\"}],\"tracks\":[{\"name\":\"v\","
             "\"packaging\":\"locmaf\",\"isLive\":true,\"role\":\"video\","
             "\"codec\":\"avc1.64001f\",\"initRef\":\"i\",\"locmafVersion\":\"0.3\"}]}",
             b64);
    moq_media_receiver_test_ingest(f.r, 1, 0, json, strlen(json));
    moq_media_track_t *succ = NULL;
    moq_media_track_event_t ev;
    while (moq_media_receiver_poll_track(f.r, &ev, sizeof(ev)) == MOQ_OK)
        if (ev.kind == MOQ_MEDIA_TRACK_ADDED && ev.replaces == f.t) succ = ev.track;
    MOQ_TEST_CHECK(succ != NULL && succ != f.t);
    if (succ) {
        MOQ_TEST_CHECK(succ->desc.cmaf &&
                       succ->desc.cmaf->trex_default_sample_duration == 2048);
        MOQ_TEST_CHECK(receiver_lm_start(f.r, succ));
        moq_media_receiver_test_push_object_ex(f.r, succ, 7, 7, 0, 0x80,
            MOQ_OBJECT_NORMAL, false, false, old[0].wire, old[0].wire_len, NULL, 0);
        moq_media_object_t o;
        bool got = false;
        while (moq_media_receiver_poll_object(f.r, &o, sizeof(o)) == MOQ_OK) {
            if (o.track == succ) {
                got = true;
                MOQ_TEST_CHECK(o.fragment.len == old[0].canon_len &&
                               memcmp(o.fragment.data, old[0].canon, o.fragment.len) == 0);
            }
            lmx_cleanup(&f, &o);
        }
        MOQ_TEST_CHECK(got);
        MOQ_TEST_CHECK(f.t->desc.cmaf->trex_default_sample_duration == 1024);
        receiver_lm_free(f.r, succ);
    }
    lmx_close(&f);
}

/* -- Allocation failure ---------------------------------------------------------- */

typedef struct {
    test_alloc_state_t base;
    uint64_t calls, fail_at;
} fail_state_t;

static void *fail_alloc(size_t n, void *ctx)
{
    fail_state_t *s = (fail_state_t *)ctx;
    if (++s->calls == s->fail_at) return NULL;
    return test_alloc(n, &s->base);
}

static void fail_free(void *p, size_t n, void *ctx)
{
    test_free(p, n, &((fail_state_t *)ctx)->base);
}

static void *fail_realloc(void *p, size_t o, size_t n, void *ctx)
{
    (void)ctx;
    return test_realloc(p, o, n, NULL);
}

static void test_oom_start(void)
{
    ROW("oom_start");
    lmx_t f;
    if (!lmx_open(&f, g_draft)) { lmx_close(&f); return; }
    receiver_lm_free(f.r, f.t);
    moq_alloc_t keep = f.r->alloc;
    bool started = false;
    for (uint64_t k = 1; k < 16 && !started; k++) {
        fail_state_t fs;
        memset(&fs, 0, sizeof(fs));
        fs.fail_at = k;
        moq_alloc_t a = { &fs, fail_alloc, fail_realloc, fail_free };
        moq_media_receiver_test_set_receiver_alloc(f.r, &a);
        started = receiver_lm_start(f.r, f.t);
        if (!started) {
            MOQ_TEST_CHECK(f.t->lm == NULL && fs.base.balance == 0);
        } else {
            MOQ_TEST_CHECK(k == 5);   /* glue, stage, two slots' storage, then none */
            receiver_lm_free(f.r, f.t);
            MOQ_TEST_CHECK(fs.base.balance == 0);
        }
        moq_media_receiver_test_set_receiver_alloc(f.r, &keep);
    }
    MOQ_TEST_CHECK(started);
    MOQ_TEST_CHECK(receiver_lm_start(f.r, f.t));
    lmx_close(&f);
}

/* An allocation failure while rebuilding loses that Object alone: the
 * decoder advanced, so the next delta still decodes. */
static void test_oom_rebuild(void)
{
    ROW("oom_rebuild");
    lmx_t f;
    if (!lmx_open(&f, g_draft)) { lmx_close(&f); return; }
    lmx_obj_t a[4];
    lmx_group(&f, 0, 4, a);
    lmx_push(&f, 0, 0, &a[0]);
    /* The stage copied the receiver's allocator at its start: make its next
     * allocation, the rebuilt buffer, fail. */
    receiver_locmaf_t *lm = (receiver_locmaf_t *)f.t->lm;
    fail_state_t fs;
    memset(&fs, 0, sizeof(fs));
    fs.fail_at = 1;
    moq_alloc_t a_fail = { &fs, fail_alloc, fail_realloc, fail_free };
    moq_alloc_t *stage_alloc = (moq_alloc_t *)(void *)lm->stage;   /* first member */
    moq_alloc_t keep = *stage_alloc;
    *stage_alloc = a_fail;
    lmx_push(&f, 0, 1, &a[1]);
    *stage_alloc = keep;
    MOQ_TEST_CHECK(fs.calls >= 1 && fs.base.balance == 0);
    lmx_push(&f, 0, 2, &a[2]);
    lmx_push(&f, 0, 3, &a[3]);
    lmx_poll(&f);
    MOQ_TEST_CHECK(lmx_count(&f, 0) == 3 && !lmx_find(&f, 0, 1));
    MOQ_TEST_CHECK(lmx_rebuilt(&f, 0, 2, &a[2]) && lmx_rebuilt(&f, 0, 3, &a[3]));
    const lmx_rec_t *r2 = lmx_find(&f, 0, 2);
    MOQ_TEST_CHECK(r2 && r2->gap && r2->gap_object == 1);
    MOQ_TEST_CHECK(lmx_balanced(&f, 0));
    lmx_close(&f);
}

/* -- Stats extent ------------------------------------------------------------------ */

static void test_stats_extent(void)
{
    ROW("stats_extent");
    lmx_t f;
    if (!lmx_open(&f, g_draft)) { lmx_close(&f); return; }
    lmx_obj_t a[1];
    lmx_group(&f, 0, 1, a);
    lmx_push(&f, 0, 0, &a[0]);
    const size_t at = offsetof(moq_media_receiver_stats_t, locmaf_objects_rebuilt);
    for (size_t size = at - 8; size <= sizeof(moq_media_receiver_stats_t) + 8; size++) {
        union { moq_media_receiver_stats_t s; uint8_t raw[sizeof(moq_media_receiver_stats_t) + 16]; } u;
        memset(u.raw, 0xA5, sizeof(u.raw));
        MOQ_TEST_CHECK(moq_media_receiver_get_stats(f.r, &u.s, size) == MOQ_OK);
        size_t want = size >= MOQ_MEDIA_RECEIVER_STATS_SIZE_THROUGH_LOCMAF
                      ? sizeof(moq_media_receiver_stats_t)
                      : size > at ? at : size;
        MOQ_TEST_CHECK(u.s.struct_size == want);
        bool clean = true;
        for (size_t i = want; i < sizeof(u.raw); i++) clean &= u.raw[i] == 0xA5;
        MOQ_TEST_CHECK(clean);
        if (want == sizeof(moq_media_receiver_stats_t))
            MOQ_TEST_CHECK(u.s.locmaf_objects_rebuilt == 1);
    }
    lmx_close(&f);
}

int main(void)
{
    static const moq_version_t drafts[] = {
        MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18, MOQ_VERSION_DRAFT_22 };
    for (size_t d = 0; d < 3; d++) {
        g_draft = drafts[d];
        int before = failures;
        test_f1_exact();
        test_sequence();
        test_mid_group_join();
        test_gap();
        test_duplicate();
        test_interleave_tail_after_next_head();
        test_interleave_alternating();
        test_reset_one_group_other_active();
        test_fin_one_group_other_active();
        test_eog_status_retires();
        test_capacity_evicts_lowest();
        test_capacity_refuses_older();
        test_closed_record_floor();
        test_barrier_per_group();
        test_raw_then_delta_live();
        test_raw_ends_barrier_for_delivery_only();
        test_raw_init_refused();
        test_datagram();
        test_second_subgroup();
        test_disable_resume();
        test_old_generation();
        test_no_delivery_path();
        test_d22_properties();
        test_stream_mapping_ignored();
        test_advisory_update_keeps_chain();
        test_stale_group_after_replacement();
        test_oom_start();
        test_oom_rebuild();
        test_stats_extent();
        printf("%s: %s\n", draft_name(g_draft), failures == before ? "PASS" : "FAIL");
    }
    test_property_init();
    test_matrix();
    printf("%s: %d failures\n", failures ? "FAIL" : "PASS", failures);
    return failures ? 1 : 0;
}
