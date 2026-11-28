/*
 * Fills of a LOCMAF track (draft-einarsson-moq-locmaf-01) through the
 * receiver's fill intake and its live intake, fed the facade's fill items
 * and Objects directly (no wire). The fill intake does not depend on the
 * draft; the receiver here uses the legacy companion, whose parse needs no
 * subscriber for Track Properties (the end-to-end draft-22 path runs in the
 * simulated-session test). Each case runs with the stream-terminal lane off
 * and on.
 *
 * Pinned:
 * - a fill's Objects decode in the fill's own decoder and are delivered as
 *   FILL, canonical CMAF, never through the live decoder; a fill of a
 *   LOCMAF track gets its delivery domain with the lane off; a fill Object
 *   with no free domain is dropped, counted;
 * - a live join by a delta in the subscription's first Group holds the
 *   deltas until a fill of the same subscription holds the chain they
 *   continue, from a full header, then hands it on: FILL (g, < o) precede
 *   LIVE (g, >= o), whichever arrives first; held Objects a fill already
 *   delivered are dropped without loss;
 * - no hand-off after a reset or a gap on the live side, for a fill of
 *   another subscription, for a later Group, from a fill whose chain broke,
 *   or from a fill whose last anchor was a rawBoxes Object; a full header
 *   on the fill restores it;
 * - held Objects expire after 300 ms, beyond 64, when the fill ends, and
 *   when the live Group resets; an END_OF_TRACK waits behind them;
 * - a rawBoxes Object then a delta on a fill: the delta is refused there;
 * - the join Group bootstraps while the next Group runs; a history fill of
 *   earlier Groups decodes in its own decoder;
 * - a standalone fetch's Objects decode in a decoder of the fetch's own and
 *   are delivered as FETCH, canonical CMAF; the decoder goes when the fetch
 *   ends, or with the receiver while the fetch is open;
 * - every Object taken in is counted once.
 */
#include <moq/media_receiver.h>
#include <moq/subscriber.h>
#define MOQ_MEDIA_RECEIVER_TESTING 1
#include "../src/media_receiver.c"

static int failures;

#include "locmaf_receiver_fixture.h"

#define ROW(name) do { lmx_row = (name); } while (0)

static bool g_lane;
static char gen_live;
#define SUB_LIVE ((moq_sub_track_t *)(void *)&gen_live)

static moq_sub_fill_t tok(uint64_t n) { return (moq_sub_fill_t){ n }; }

/* A fixture whose subscription asked for a fill, its facade generation
 * SUB_LIVE. */
static bool fx_open(lmx_t *f)
{
    if (!lmx_open_full(f, MOQ_VERSION_DRAFT_18, MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01,
                       "draft-01", ",\"locmafVersion\":\"0.3\"", true, g_lane))
        return false;
    f->t->sub_track = SUB_LIVE;
    return true;
}

static void fx_close(lmx_t *f)
{
    if (f->t) f->t->sub_track = NULL;
    lmx_close(f);
}

static void feed(lmx_t *f, moq_sub_fill_item_t *it, uint64_t now_us)
{
    it->struct_size = sizeof(*it);
    receiver_take_fill_item(f->r, it, now_us);
}

static void created(lmx_t *f, uint64_t token)
{
    moq_sub_fill_item_t it;
    memset(&it, 0, sizeof(it));
    it.kind = MOQ_SUB_FILL_CREATED;
    it.request = tok(token);
    it.track = SUB_LIVE;
    it.initiating_request_id = 100 + token;
    feed(f, &it, 0);
}

static uint64_t g_fill_taken;

static void fill(lmx_t *f, uint64_t token, uint64_t g, uint64_t o,
                 const lmx_obj_t *obj, uint64_t now_us)
{
    moq_sub_fill_item_t it;
    memset(&it, 0, sizeof(it));
    it.kind = MOQ_SUB_FILL_OBJECT;
    it.request = tok(token);
    it.track = SUB_LIVE;
    it.initiating_request_id = 100 + token;
    it.u.object.group_id = g;
    it.u.object.subgroup_id = g;
    it.u.object.object_id = o;
    MOQ_TEST_CHECK(moq_rcbuf_create(&f->r->alloc, obj->wire, obj->wire_len,
                                    &it.u.object.payload) == MOQ_OK);
    g_fill_taken++;
    feed(f, &it, now_us);
}

static void fill_end(lmx_t *f, uint64_t token, uint64_t now_us)
{
    moq_sub_fill_item_t it;
    memset(&it, 0, sizeof(it));
    it.kind = MOQ_SUB_FILL_TERMINAL;
    it.request = tok(token);
    it.track = SUB_LIVE;
    it.initiating_request_id = 100 + token;
    it.u.terminal.reason = MOQ_FILL_COMPLETE;
    feed(f, &it, now_us);
}

static receiver_lm_t *stage_of(lmx_t *f)
{
    return f->t->lm ? ((receiver_locmaf_t *)f->t->lm)->stage : NULL;
}

/* The record of (g, o) from `origin`, or NULL. */
static const lmx_rec_t *find_from(const lmx_t *f, uint64_t g, uint64_t o,
                                  uint32_t origin)
{
    for (size_t i = 0; i < f->got_count; i++)
        if (!f->got[i].status && f->got[i].group == g && f->got[i].object == o &&
            f->got[i].origin == origin)
            return &f->got[i];
    return NULL;
}

static bool rebuilt_from(const lmx_t *f, uint64_t g, uint64_t o, uint32_t origin,
                         const lmx_obj_t *obj)
{
    const lmx_rec_t *r = find_from(f, g, o, origin);
    bool ok = r && r->packaging == MOQ_MEDIA_PACKAGING_CMAF && r->payload_len == 0 &&
              r->frag_len == obj->canon_len &&
              memcmp(r->frag, obj->canon, obj->canon_len) == 0;
    if (!ok)
        fprintf(stderr, "  %s: (%llu, %llu) origin %u not rebuilt\n", lmx_row,
                (unsigned long long)g, (unsigned long long)o, (unsigned)origin);
    return ok;
}

/* The position of (g, o, origin) in delivery order, or SIZE_MAX. */
static size_t pos_of(const lmx_t *f, uint64_t g, uint64_t o, uint32_t origin)
{
    for (size_t i = 0; i < f->got_count; i++)
        if (!f->got[i].status && f->got[i].group == g && f->got[i].object == o &&
            f->got[i].origin == origin)
            return i;
    return SIZE_MAX;
}

#define LIVE MOQ_MEDIA_DELIVERY_LIVE
#define FILL MOQ_MEDIA_DELIVERY_FILL
#define FETCH MOQ_MEDIA_DELIVERY_FETCH

/* A standalone fetch of the track, its facade request FETCH_REQ. */
static char g_fetch_req;
#define FETCH_REQ ((moq_sub_fetch_req_t *)(void *)&g_fetch_req)

/* The track's fetch record, as the executor opens it once its FETCH is sent. */
static bool fetch_open(lmx_t *f)
{
    pthread_mutex_lock(&f->r->mu);
    bool ok = receiver_fetch_open_locked(f->r, f->t, FETCH_REQ, 200, 0);
    pthread_mutex_unlock(&f->r->mu);
    return ok;
}

/* One facade item of the fetch: (g, o) when `obj`, else `kind` alone. */
static void fetch_item(lmx_t *f, moq_sub_fetch_item_kind_t kind, uint64_t g,
                       uint64_t o, const lmx_obj_t *obj)
{
    moq_sub_fetch_item_t it;
    memset(&it, 0, sizeof(it));
    it.kind = kind;
    it.request = FETCH_REQ;
    if (obj) {
        it.u.object.group_id = g;
        it.u.object.object_id = o;
        MOQ_TEST_CHECK(moq_rcbuf_create(&f->r->alloc, obj->wire, obj->wire_len,
                                        &it.u.object.payload) == MOQ_OK);
    }
    receiver_take_fetch_item(f->r, &it);
}

/* The decoders the track's fetch records hold. */
static unsigned fetch_decoders(lmx_t *f)
{
    unsigned n = 0;
    pthread_mutex_lock(&f->r->mu);
    for (size_t i = RECEIVER_FILL_DOMS; i < RECEIVER_DOMS; i++)
        if (f->r->fill_doms[i].lm) n++;
    pthread_mutex_unlock(&f->r->mu);
    return n;
}

/* -- The hand-off -------------------------------------------------------------- */

static void test_handoff(bool live_first)
{
    ROW(live_first ? "handoff_bootstrap_ok live first" : "handoff_bootstrap_ok fill first");
    lmx_t f;
    g_fill_taken = 0;
    if (!fx_open(&f)) { fx_close(&f); return; }
    lmx_obj_t a[6];
    lmx_group(&f, 0, 6, a);
    created(&f, 1);
    if (live_first) {
        for (uint64_t o = 3; o < 6; o++) lmx_push(&f, 0, o, &a[o]);
        MOQ_TEST_CHECK(receiver_lm_slot_state(stage_of(&f), 0) == LM_SLOT_BOOTSTRAP);
        MOQ_TEST_CHECK(f.r->obj_tail == f.r->obj_head);   /* held */
        for (uint64_t o = 0; o < 3; o++) fill(&f, 1, 0, o, &a[o], 1000);
    } else {
        for (uint64_t o = 0; o < 3; o++) fill(&f, 1, 0, o, &a[o], 1000);
        for (uint64_t o = 3; o < 6; o++) lmx_push(&f, 0, o, &a[o]);
    }
    lmx_poll(&f);
    MOQ_TEST_CHECK(f.got_count == 6);
    for (uint64_t o = 0; o < 3; o++) MOQ_TEST_CHECK(rebuilt_from(&f, 0, o, FILL, &a[o]));
    for (uint64_t o = 3; o < 6; o++) MOQ_TEST_CHECK(rebuilt_from(&f, 0, o, LIVE, &a[o]));
    MOQ_TEST_CHECK(pos_of(&f, 0, 2, FILL) < pos_of(&f, 0, 3, LIVE));
    MOQ_TEST_CHECK(receiver_lm_slot_state(stage_of(&f), 0) == LM_SLOT_RUNNING);
    const lmx_rec_t *l3 = find_from(&f, 0, 3, LIVE);
    MOQ_TEST_CHECK(l3 && !l3->gap);
    MOQ_TEST_CHECK(lmx_stats(&f).locmaf_objects_expired == 0);
    MOQ_TEST_CHECK(lmx_balanced(&f, g_fill_taken));
    fx_close(&f);
}

static void test_handoff_refused_after_reset(void)
{
    ROW("handoff_refused_after_reset");
    lmx_t f;
    g_fill_taken = 0;
    if (!fx_open(&f)) { fx_close(&f); return; }
    lmx_obj_t a[8];
    lmx_group(&f, 0, 8, a);
    created(&f, 1);
    for (uint64_t o = 0; o < 3; o++) fill(&f, 1, 0, o, &a[o], 0);
    lmx_push(&f, 0, 3, &a[3]);       /* handed off */
    lmx_terminal(&f, MOQ_SUB_STREAM_RESET, 0, 0);
    for (uint64_t o = 3; o < 5; o++) fill(&f, 1, 0, o, &a[o], 0);
    lmx_push(&f, 0, 5, &a[5]);       /* the fill sits right at (0, 5) */
    lmx_push(&f, 0, 6, &a[6]);
    lmx_poll(&f);
    MOQ_TEST_CHECK(find_from(&f, 0, 3, LIVE) && !find_from(&f, 0, 5, LIVE) &&
                   !find_from(&f, 0, 6, LIVE));
    MOQ_TEST_CHECK(receiver_lm_slot_state(stage_of(&f), 0) == LM_SLOT_BARRIER);
    MOQ_TEST_CHECK(lmx_balanced(&f, g_fill_taken));
    fx_close(&f);
}

static void test_handoff_refused_after_gap(void)
{
    ROW("handoff_refused_after_gap");
    lmx_t f;
    g_fill_taken = 0;
    if (!fx_open(&f)) { fx_close(&f); return; }
    lmx_obj_t a[8];
    lmx_group(&f, 0, 6, a);
    lmx_make(&f, 0, 6, LMX_FULL, &a[6]);
    lmx_make(&f, 0, 7, LMX_AUTO, &a[7]);
    created(&f, 1);
    lmx_push(&f, 0, 2, &a[2]);       /* held */
    lmx_push(&f, 0, 4, &a[4]);       /* a gap among the held deltas */
    for (uint64_t o = 0; o < 4; o++) fill(&f, 1, 0, o, &a[o], 0);
    lmx_push(&f, 0, 5, &a[5]);
    lmx_push(&f, 0, 6, &a[6]);       /* a full header recovers */
    lmx_push(&f, 0, 7, &a[7]);
    lmx_poll(&f);
    MOQ_TEST_CHECK(!find_from(&f, 0, 2, LIVE) && !find_from(&f, 0, 4, LIVE) &&
                   !find_from(&f, 0, 5, LIVE));
    MOQ_TEST_CHECK(rebuilt_from(&f, 0, 6, LIVE, &a[6]) && rebuilt_from(&f, 0, 7, LIVE, &a[7]));
    for (uint64_t o = 0; o < 4; o++) MOQ_TEST_CHECK(rebuilt_from(&f, 0, o, FILL, &a[o]));
    MOQ_TEST_CHECK(lmx_balanced(&f, g_fill_taken));
    fx_close(&f);
}

static void test_handoff_refused_other_generation(void)
{
    ROW("handoff_refused_other_generation");
    lmx_t f;
    g_fill_taken = 0;
    if (!fx_open(&f)) { fx_close(&f); return; }
    lmx_obj_t a[5];
    lmx_group(&f, 0, 5, a);
    created(&f, 1);
    for (uint64_t o = 0; o < 3; o++) fill(&f, 1, 0, o, &a[o], 0);
    /* A new subscription generation (a live-to-VOD re-subscription): the
     * earlier fill belongs to the previous one. */
    MOQ_TEST_CHECK(receiver_lm_start(f.r, f.t));
    receiver_lm_note_fill(f.t, false);
    lmx_push(&f, 0, 3, &a[3]);
    lmx_push(&f, 0, 4, &a[4]);
    lmx_poll(&f);
    MOQ_TEST_CHECK(!find_from(&f, 0, 3, LIVE) && !find_from(&f, 0, 4, LIVE));
    moq_media_receiver_stats_t s = lmx_stats(&f);
    MOQ_TEST_CHECK(s.locmaf_objects_rebuilt == 3);
    MOQ_TEST_CHECK(lmx_balanced(&f, g_fill_taken));
    fx_close(&f);
}

static void test_handoff_refused_later_group(void)
{
    ROW("handoff_refused_later_group");
    lmx_t f;
    g_fill_taken = 0;
    if (!fx_open(&f)) { fx_close(&f); return; }
    lmx_obj_t a[2], b[4];
    lmx_group(&f, 0, 2, a);
    lmx_group(&f, 1, 4, b);
    created(&f, 1);
    lmx_push(&f, 0, 0, &a[0]);       /* the join Group */
    lmx_push(&f, 0, 1, &a[1]);
    for (uint64_t o = 0; o < 2; o++) fill(&f, 1, 1, o, &b[o], 0);
    lmx_push(&f, 1, 2, &b[2]);       /* a later Group joined by a delta */
    MOQ_TEST_CHECK(receiver_lm_slot_state(stage_of(&f), 1) == LM_SLOT_BARRIER);
    lmx_push(&f, 1, 3, &b[3]);
    lmx_poll(&f);
    MOQ_TEST_CHECK(!find_from(&f, 1, 2, LIVE) && !find_from(&f, 1, 3, LIVE));
    MOQ_TEST_CHECK(lmx_balanced(&f, g_fill_taken));
    fx_close(&f);
}

static void test_handoff_refused_fill_broken(void)
{
    ROW("handoff_refused_fill_broken");
    lmx_t f;
    g_fill_taken = 0;
    if (!fx_open(&f)) { fx_close(&f); return; }
    lmx_obj_t a[5];
    lmx_group(&f, 0, 5, a);
    created(&f, 1);
    fill(&f, 1, 0, 0, &a[0], 0);
    fill(&f, 1, 0, 2, &a[2], 0);     /* the fill's own gap */
    lmx_push(&f, 0, 3, &a[3]);       /* held: the fill cannot continue it */
    MOQ_TEST_CHECK(receiver_lm_slot_state(stage_of(&f), 0) == LM_SLOT_BOOTSTRAP);
    moq_media_receiver_test_timers(f.r, 400000);   /* the hold expires */
    lmx_poll(&f);
    MOQ_TEST_CHECK(!find_from(&f, 0, 3, LIVE));
    moq_media_receiver_stats_t s = lmx_stats(&f);
    MOQ_TEST_CHECK(s.locmaf_objects_expired == 1);
    MOQ_TEST_CHECK(receiver_lm_slot_state(stage_of(&f), 0) == LM_SLOT_BARRIER);
    MOQ_TEST_CHECK(lmx_balanced(&f, g_fill_taken));
    fx_close(&f);
}

static void test_handoff_overlap_fill_ahead(void)
{
    ROW("handoff_overlap_fill_ahead");
    lmx_t f;
    g_fill_taken = 0;
    if (!fx_open(&f)) { fx_close(&f); return; }
    lmx_obj_t a[8];
    lmx_group(&f, 0, 8, a);
    created(&f, 1);
    for (uint64_t o = 0; o < 6; o++) fill(&f, 1, 0, o, &a[o], 0);
    for (uint64_t o = 3; o < 8; o++) lmx_push(&f, 0, o, &a[o]);
    lmx_poll(&f);
    for (uint64_t o = 0; o < 6; o++) MOQ_TEST_CHECK(rebuilt_from(&f, 0, o, FILL, &a[o]));
    for (uint64_t o = 3; o < 6; o++) MOQ_TEST_CHECK(!find_from(&f, 0, o, LIVE));
    MOQ_TEST_CHECK(rebuilt_from(&f, 0, 6, LIVE, &a[6]) && rebuilt_from(&f, 0, 7, LIVE, &a[7]));
    const lmx_rec_t *l6 = find_from(&f, 0, 6, LIVE);
    MOQ_TEST_CHECK(l6 && !l6->gap);   /* no loss: the fill delivered 3..5 */
    moq_media_receiver_stats_t s = lmx_stats(&f);
    MOQ_TEST_CHECK(s.locmaf_objects_undecodable == 3 && s.locmaf_objects_expired == 0);
    MOQ_TEST_CHECK(lmx_balanced(&f, g_fill_taken));
    fx_close(&f);
}

static void test_handoff_refused_after_fill_raw(void)
{
    ROW("handoff_refused_after_fill_raw");
    lmx_t f;
    g_fill_taken = 0;
    if (!fx_open(&f)) { fx_close(&f); return; }
    lmx_obj_t a[4], raw;
    lmx_group(&f, 0, 4, a);          /* a[3] continues (0, 2) */
    moq_locmaf_encoder_reset(&f.enc);
    lmx_obj_t skip[2];
    lmx_group(&f, 0, 2, skip);
    lmx_make(&f, 0, 2, LMX_RAW, &raw);
    created(&f, 1);
    fill(&f, 1, 0, 0, &a[0], 0);
    fill(&f, 1, 0, 1, &a[1], 0);
    fill(&f, 1, 0, 2, &raw, 0);
    lmx_push(&f, 0, 3, &a[3]);       /* held; the fill holds no reference */
    MOQ_TEST_CHECK(receiver_lm_slot_state(stage_of(&f), 0) == LM_SLOT_BOOTSTRAP);
    fill(&f, 1, 0, 3, &a[3], 0);     /* refused on the fill */
    moq_media_receiver_test_timers(f.r, 400000);
    lmx_poll(&f);
    MOQ_TEST_CHECK(!find_from(&f, 0, 3, LIVE) && !find_from(&f, 0, 3, FILL));
    MOQ_TEST_CHECK(rebuilt_from(&f, 0, 2, FILL, &raw));
    moq_media_receiver_stats_t s = lmx_stats(&f);
    MOQ_TEST_CHECK(s.locmaf_objects_expired == 1 && s.parse_drops >= 1);
    MOQ_TEST_CHECK(lmx_balanced(&f, g_fill_taken));
    fx_close(&f);
}

static void test_handoff_after_fill_raw_then_full(void)
{
    ROW("handoff_after_fill_raw_then_full");
    lmx_t f;
    g_fill_taken = 0;
    if (!fx_open(&f)) { fx_close(&f); return; }
    lmx_obj_t f0, raw1, full2, d3, d4;
    lmx_make(&f, 0, 0, LMX_AUTO, &f0);
    lmx_make(&f, 0, 1, LMX_RAW, &raw1);
    lmx_make(&f, 0, 2, LMX_AUTO, &full2);   /* full: rawBoxes reset the chain */
    lmx_make(&f, 0, 3, LMX_AUTO, &d3);
    lmx_make(&f, 0, 4, LMX_AUTO, &d4);
    created(&f, 1);
    fill(&f, 1, 0, 0, &f0, 0);
    fill(&f, 1, 0, 1, &raw1, 0);
    fill(&f, 1, 0, 2, &full2, 0);
    fill(&f, 1, 0, 3, &d3, 0);
    lmx_push(&f, 0, 4, &d4);         /* handed off from the anchor at 2 */
    lmx_poll(&f);
    MOQ_TEST_CHECK(rebuilt_from(&f, 0, 4, LIVE, &d4));
    MOQ_TEST_CHECK(receiver_lm_slot_state(stage_of(&f), 0) == LM_SLOT_RUNNING);
    MOQ_TEST_CHECK(lmx_balanced(&f, g_fill_taken));
    fx_close(&f);
}

static void test_raw_then_delta_fill(void)
{
    ROW("raw_then_delta_fill");
    lmx_t f;
    g_fill_taken = 0;
    if (!fx_open(&f)) { fx_close(&f); return; }
    lmx_obj_t a[4], raw, b4, b5;
    lmx_group(&f, 0, 4, a);
    moq_locmaf_encoder_reset(&f.enc);
    lmx_obj_t skip[2];
    lmx_group(&f, 0, 2, skip);
    lmx_make(&f, 0, 2, LMX_RAW, &raw);
    lmx_make(&f, 0, 4, LMX_FULL, &b4);
    lmx_make(&f, 0, 5, LMX_AUTO, &b5);
    created(&f, 1);
    fill(&f, 1, 0, 0, &a[0], 0);
    fill(&f, 1, 0, 1, &a[1], 0);
    fill(&f, 1, 0, 2, &raw, 0);
    fill(&f, 1, 0, 3, &a[3], 0);     /* refused */
    fill(&f, 1, 0, 4, &b4, 0);
    fill(&f, 1, 0, 5, &b5, 0);
    lmx_poll(&f);
    MOQ_TEST_CHECK(f.got_count == 5 && !find_from(&f, 0, 3, FILL));
    MOQ_TEST_CHECK(rebuilt_from(&f, 0, 4, FILL, &b4) && rebuilt_from(&f, 0, 5, FILL, &b5));
    const lmx_rec_t *r4 = find_from(&f, 0, 4, FILL);
    MOQ_TEST_CHECK(r4 && r4->has_loss_report && r4->gap && r4->gap_object == 3);
    MOQ_TEST_CHECK(lmx_balanced(&f, g_fill_taken));
    fx_close(&f);
}

/* -- Hold limits ----------------------------------------------------------------- */

static void test_hold_deadline(void)
{
    ROW("hold_deadline");
    lmx_t f;
    g_fill_taken = 0;
    if (!fx_open(&f)) { fx_close(&f); return; }
    lmx_obj_t a[5];
    lmx_group(&f, 0, 5, a);
    created(&f, 1);
    moq_media_receiver_test_push_object_ex(f.r, f.t, 0, 0, 3, 0x80, MOQ_OBJECT_NORMAL,
        false, false, a[3].wire, a[3].wire_len, NULL, 0);
    f.pushed++;
    MOQ_TEST_CHECK(receiver_lm_deadline(stage_of(&f)) == RECEIVER_LM_HOLD_US);
    moq_media_receiver_test_timers(f.r, RECEIVER_LM_HOLD_US - 1);
    MOQ_TEST_CHECK(receiver_lm_holds(stage_of(&f)));
    MOQ_TEST_CHECK(moq_media_receiver_test_next_deadline_us(f.r) == RECEIVER_LM_HOLD_US);
    moq_media_receiver_test_timers(f.r, RECEIVER_LM_HOLD_US);
    MOQ_TEST_CHECK(!receiver_lm_holds(stage_of(&f)));
    MOQ_TEST_CHECK(lmx_stats(&f).locmaf_objects_expired == 1);
    MOQ_TEST_CHECK(moq_media_receiver_test_next_deadline_us(f.r) == UINT64_MAX);
    MOQ_TEST_CHECK(lmx_balanced(&f, g_fill_taken));
    fx_close(&f);
}

static void test_hold_overflow(void)
{
    ROW("hold_overflow");
    lmx_t f;
    g_fill_taken = 0;
    if (!fx_open(&f)) { fx_close(&f); return; }
    created(&f, 1);
    static lmx_obj_t objs[RECEIVER_LM_HOLD + 2];
    lmx_group(&f, 0, RECEIVER_LM_HOLD + 2, objs);
    if (!g_lane) {
        for (uint64_t i = 1; i <= RECEIVER_LM_HOLD + 1; i++) lmx_push(&f, 0, i, &objs[i]);
        lmx_poll(&f);
        MOQ_TEST_CHECK(f.got_count == 0);
        moq_media_receiver_stats_t s = lmx_stats(&f);
        MOQ_TEST_CHECK(s.locmaf_objects_expired == RECEIVER_LM_HOLD + 1);
        MOQ_TEST_CHECK(receiver_lm_slot_state(stage_of(&f), 0) == LM_SLOT_BARRIER);
        MOQ_TEST_CHECK(lmx_balanced(&f, g_fill_taken));
        fx_close(&f);
        return;
    }
    /* With the lane, each held Object keeps a lane slot: intake waits once
     * the lane is reserved, before the hold overflows, and no Object is
     * lost to it. The hold's deadline then returns the slots. */
    uint64_t taken = 0;
    for (uint64_t i = 1; i <= RECEIVER_LM_HOLD + 1; i++) {
        pthread_mutex_lock(&f.r->mu);
        bool room = st_room_locked(f.r);
        pthread_mutex_unlock(&f.r->mu);
        if (!room) break;
        moq_media_receiver_test_push_object_ex(f.r, f.t, 0, 0, i, 0x80, MOQ_OBJECT_NORMAL,
            false, false, objs[i].wire, objs[i].wire_len, NULL, 0);
        taken++;
    }
    f.pushed = taken;
    MOQ_TEST_CHECK(taken > 0 && taken < RECEIVER_LM_HOLD + 1);
    MOQ_TEST_CHECK(lmx_stats(&f).locmaf_objects_expired == 0);
    moq_media_receiver_test_timers(f.r, RECEIVER_LM_HOLD_US);
    MOQ_TEST_CHECK(lmx_stats(&f).locmaf_objects_expired == taken);
    pthread_mutex_lock(&f.r->mu);
    MOQ_TEST_CHECK(st_room_locked(f.r) && f.r->st_reserved == 0);
    pthread_mutex_unlock(&f.r->mu);
    MOQ_TEST_CHECK(lmx_balanced(&f, g_fill_taken));
    fx_close(&f);
}

/* The fill ends before it reaches the held Objects: they expire at once. */
static void test_fill_ends_early(void)
{
    ROW("fill_ends_early");
    lmx_t f;
    g_fill_taken = 0;
    if (!fx_open(&f)) { fx_close(&f); return; }
    lmx_obj_t a[6];
    lmx_group(&f, 0, 6, a);
    created(&f, 1);
    lmx_push(&f, 0, 4, &a[4]);
    lmx_push(&f, 0, 5, &a[5]);
    fill(&f, 1, 0, 0, &a[0], 0);
    fill(&f, 1, 0, 1, &a[1], 0);
    fill_end(&f, 1, 0);
    MOQ_TEST_CHECK(receiver_lm_holds(stage_of(&f)));
    moq_media_receiver_test_timers(f.r, 1);
    MOQ_TEST_CHECK(!receiver_lm_holds(stage_of(&f)));
    lmx_poll(&f);
    MOQ_TEST_CHECK(!find_from(&f, 0, 4, LIVE) && !find_from(&f, 0, 5, LIVE));
    MOQ_TEST_CHECK(lmx_stats(&f).locmaf_objects_expired == 2);
    MOQ_TEST_CHECK(lmx_balanced(&f, g_fill_taken));
    fx_close(&f);
}

/* No fill was asked for: a mid-group delta is dropped at once, never
 * held. */
static void test_no_fill(void)
{
    ROW("no_fill");
    lmx_t f;
    g_fill_taken = 0;
    if (!fx_open(&f)) { fx_close(&f); return; }
    receiver_lm_note_fill(f.t, false);
    lmx_obj_t a[3];
    lmx_group(&f, 0, 3, a);
    lmx_push(&f, 0, 2, &a[2]);
    MOQ_TEST_CHECK(!receiver_lm_holds(stage_of(&f)));
    moq_media_receiver_stats_t s = lmx_stats(&f);
    MOQ_TEST_CHECK(s.locmaf_objects_expired == 0 && s.locmaf_objects_undecodable == 1);
    MOQ_TEST_CHECK(lmx_balanced(&f, g_fill_taken));
    fx_close(&f);
}

static void test_eot_behind_held(void)
{
    ROW("eot_behind_held");
    lmx_t f;
    g_fill_taken = 0;
    if (!fx_open(&f)) { fx_close(&f); return; }
    lmx_obj_t a[4];
    lmx_group(&f, 0, 4, a);
    created(&f, 1);
    lmx_push(&f, 0, 2, &a[2]);
    lmx_push(&f, 0, 3, &a[3]);
    lmx_status(&f, 0, 4, MOQ_OBJECT_END_OF_TRACK);
    MOQ_TEST_CHECK(!f.t->ended);     /* parked behind the held Objects */
    fill(&f, 1, 0, 0, &a[0], 0);
    fill(&f, 1, 0, 1, &a[1], 0);     /* hands off, then the end */
    lmx_poll(&f);
    MOQ_TEST_CHECK(rebuilt_from(&f, 0, 2, LIVE, &a[2]) && rebuilt_from(&f, 0, 3, LIVE, &a[3]));
    MOQ_TEST_CHECK(f.t->ended);
    MOQ_TEST_CHECK(lmx_balanced(&f, g_fill_taken));
    fx_close(&f);
}

/* The join Group bootstraps while the next Group opens and runs. */
static void test_bootstrap_with_interleave(void)
{
    ROW("bootstrap_with_interleave");
    lmx_t f;
    g_fill_taken = 0;
    if (!fx_open(&f)) { fx_close(&f); return; }
    lmx_obj_t a[5], b[3];
    lmx_group(&f, 0, 5, a);
    lmx_group(&f, 1, 3, b);
    created(&f, 1);
    lmx_push(&f, 0, 3, &a[3]);
    lmx_push(&f, 1, 0, &b[0]);
    lmx_push(&f, 0, 4, &a[4]);
    lmx_push(&f, 1, 1, &b[1]);
    MOQ_TEST_CHECK(receiver_lm_slot_state(stage_of(&f), 0) == LM_SLOT_BOOTSTRAP);
    MOQ_TEST_CHECK(receiver_lm_slot_state(stage_of(&f), 1) == LM_SLOT_RUNNING);
    for (uint64_t o = 0; o < 3; o++) fill(&f, 1, 0, o, &a[o], 0);
    lmx_push(&f, 1, 2, &b[2]);
    lmx_poll(&f);
    for (uint64_t o = 3; o < 5; o++) MOQ_TEST_CHECK(rebuilt_from(&f, 0, o, LIVE, &a[o]));
    for (uint64_t o = 0; o < 3; o++) MOQ_TEST_CHECK(rebuilt_from(&f, 1, o, LIVE, &b[o]));
    MOQ_TEST_CHECK(lmx_balanced(&f, g_fill_taken));
    fx_close(&f);
}

/* A history fill of earlier Groups beside live: its own decoder, and the
 * live chain is untouched. */
static void test_history_fill(void)
{
    ROW("history_fill");
    lmx_t f;
    g_fill_taken = 0;
    if (!fx_open(&f)) { fx_close(&f); return; }
    lmx_obj_t h0[3], h1[3], l[3];
    lmx_group(&f, 1, 3, h0);
    lmx_group(&f, 2, 3, h1);
    lmx_group(&f, 5, 3, l);
    created(&f, 2);
    lmx_push(&f, 5, 0, &l[0]);
    for (uint64_t o = 0; o < 3; o++) fill(&f, 2, 1, o, &h0[o], 0);
    lmx_push(&f, 5, 1, &l[1]);
    for (uint64_t o = 0; o < 3; o++) fill(&f, 2, 2, o, &h1[o], 0);
    lmx_push(&f, 5, 2, &l[2]);
    lmx_poll(&f);
    for (uint64_t o = 0; o < 3; o++) {
        MOQ_TEST_CHECK(rebuilt_from(&f, 1, o, FILL, &h0[o]));
        MOQ_TEST_CHECK(rebuilt_from(&f, 2, o, FILL, &h1[o]));
        MOQ_TEST_CHECK(rebuilt_from(&f, 5, o, LIVE, &l[o]));
    }
    MOQ_TEST_CHECK(lmx_balanced(&f, g_fill_taken));
    fx_close(&f);
}

/* Every fill record taken: a fill Object without one is dropped, counted,
 * and never decoded on the live decoder. */
static void test_no_free_domain(void)
{
    ROW("no_free_domain");
    lmx_t f;
    g_fill_taken = 0;
    if (!fx_open(&f)) { fx_close(&f); return; }
    pthread_mutex_lock(&f.r->mu);
    for (size_t i = 0; i < RECEIVER_FILL_DOMS; i++)
        if (f.r->fill_doms[i].state == DOM_FREE) f.r->fill_doms[i].state = DOM_READY;
    pthread_mutex_unlock(&f.r->mu);
    lmx_obj_t a[1];
    lmx_group(&f, 0, 1, a);
    created(&f, 9);
    fill(&f, 9, 0, 0, &a[0], 0);
    lmx_poll(&f);
    MOQ_TEST_CHECK(f.got_count == 0);
    MOQ_TEST_CHECK(lmx_stats(&f).locmaf_objects_undecodable == 1);
    pthread_mutex_lock(&f.r->mu);
    for (size_t i = 0; i < RECEIVER_FILL_DOMS; i++)
        if (f.r->fill_doms[i].state == DOM_READY && !f.r->fill_doms[i].track)
            f.r->fill_doms[i].state = DOM_FREE;
    pthread_mutex_unlock(&f.r->mu);
    MOQ_TEST_CHECK(lmx_balanced(&f, g_fill_taken));
    fx_close(&f);
}

/* The fill's decoder is created at its first Object; when that fails, its
 * Objects are dropped and the fill still ends normally. */
static void test_fill_decoder_oom(void)
{
    ROW("fill_decoder_oom");
    lmx_t f;
    g_fill_taken = 0;
    if (!fx_open(&f)) { fx_close(&f); return; }
    lmx_obj_t a[2];
    lmx_group(&f, 0, 2, a);
    created(&f, 1);
    int dom = -1;
    pthread_mutex_lock(&f.r->mu);
    dom = dom_find_locked(f.r, tok(1));
    if (dom >= 0) f.r->fill_doms[dom].lm_failed = true;
    pthread_mutex_unlock(&f.r->mu);
    MOQ_TEST_CHECK(dom >= 0 && f.r->fill_doms[dom].locmaf);
    fill(&f, 1, 0, 0, &a[0], 0);
    fill(&f, 1, 0, 1, &a[1], 0);
    fill_end(&f, 1, 0);
    lmx_poll(&f);
    MOQ_TEST_CHECK(f.got_count == 0);
    MOQ_TEST_CHECK(lmx_stats(&f).locmaf_objects_undecodable == 2);
    bool terminal = false;
    moq_media_track_event_t ev;
    while (moq_media_receiver_poll_track(f.r, &ev, sizeof(ev)) == MOQ_OK)
        if (ev.kind == MOQ_MEDIA_TRACK_FILL_TERMINAL) terminal = true;
    MOQ_TEST_CHECK(terminal);
    MOQ_TEST_CHECK(lmx_balanced(&f, g_fill_taken));
    fx_close(&f);
}

/* -- A standalone fetch ------------------------------------------------------- */

/* A fetch of a whole Group: each Object decodes in the fetch's own decoder
 * (a delta against the full header before it) and is delivered as FETCH,
 * its canonical chunk; the decoder goes with the fetch's end, and the
 * terminal follows the Objects. */
static void test_fetch_decodes(void)
{
    ROW("fetch_decodes");
    lmx_t f;
    if (!fx_open(&f)) { fx_close(&f); return; }
    lmx_obj_t a[4];
    lmx_group(&f, 2, 4, a);
    MOQ_TEST_CHECK(fetch_open(&f));
    for (uint64_t o = 0; o < 4; o++) fetch_item(&f, MOQ_SUB_FETCH_OBJECT, 2, o, &a[o]);
    MOQ_TEST_CHECK(fetch_decoders(&f) == 1);
    fetch_item(&f, MOQ_SUB_FETCH_COMPLETE, 0, 0, NULL);
    MOQ_TEST_CHECK(fetch_decoders(&f) == 0);
    lmx_poll(&f);
    MOQ_TEST_CHECK(f.got_count == 4);
    for (uint64_t o = 0; o < 4; o++) MOQ_TEST_CHECK(rebuilt_from(&f, 2, o, FETCH, &a[o]));
    MOQ_TEST_CHECK(lmx_in_order(&f, 2));
    MOQ_TEST_CHECK(lmx_stats(&f).locmaf_objects_rebuilt == 4);
    MOQ_TEST_CHECK(lmx_balanced(&f, 4));
    bool terminal = false;
    moq_media_track_event_t ev;
    while (moq_media_receiver_poll_track(f.r, &ev, sizeof(ev)) == MOQ_OK)
        if (ev.kind == MOQ_MEDIA_TRACK_FETCH_TERMINAL)
            terminal = ev.fill_reason == MOQ_MEDIA_FETCH_COMPLETE &&
                       ev.initiating_request_id == 200;
    MOQ_TEST_CHECK(terminal);
    fx_close(&f);
}

/* A receiver freed while a fetch is open frees the fetch's decoder (the
 * ledger balances at close). */
static void test_fetch_open_at_free(void)
{
    ROW("fetch_open_at_free");
    lmx_t f;
    if (!fx_open(&f)) { fx_close(&f); return; }
    lmx_obj_t a[1];
    lmx_group(&f, 0, 1, a);
    MOQ_TEST_CHECK(fetch_open(&f));
    fetch_item(&f, MOQ_SUB_FETCH_OBJECT, 0, 0, &a[0]);
    MOQ_TEST_CHECK(fetch_decoders(&f) == 1);
    fx_close(&f);
}

int main(void)
{
    for (int lane = 0; lane < 2; lane++) {
        g_lane = lane != 0;
        int before = failures;
        test_handoff(false);
        test_handoff(true);
        test_handoff_refused_after_reset();
        test_handoff_refused_after_gap();
        test_handoff_refused_other_generation();
        test_handoff_refused_later_group();
        test_handoff_refused_fill_broken();
        test_handoff_overlap_fill_ahead();
        test_handoff_refused_after_fill_raw();
        test_handoff_after_fill_raw_then_full();
        test_raw_then_delta_fill();
        test_hold_deadline();
        test_hold_overflow();
        test_fill_ends_early();
        test_no_fill();
        test_eot_behind_held();
        test_bootstrap_with_interleave();
        test_history_fill();
        test_no_free_domain();
        test_fill_decoder_oom();
        test_fetch_decodes();
        test_fetch_open_at_free();
        printf("lane %s: %s\n", g_lane ? "on" : "off", failures == before ? "PASS" : "FAIL");
    }
    printf("%s: %d failures\n", failures ? "FAIL" : "PASS", failures);
    return failures ? 1 : 0;
}
