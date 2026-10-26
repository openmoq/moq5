/*
 * ABI rows for the playback public interface, against the production
 * library: frozen sizes and layouts, the sized initializers, the sized
 * polls and the command-cleanup contract.
 *
 * The same source is also built with -fshort-enums (playback_abi_short_enums)
 * where the compiler supports it: the layout pins below must hold there too,
 * and that binary still drives the normally built library end to end.
 */
#include <moq/playback.h>
#include <moq/rcbuf.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int failures = 0;

#define CHECK(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)

/* -- Layout pins ------------------------------------------------------ */

_Static_assert(sizeof(moq_playback_media_type_t) == 4 &&
               sizeof(moq_playback_packaging_t) == 4 &&
               sizeof(moq_playback_cmd_kind_t) == 4 &&
               sizeof(moq_playback_reset_reason_t) == 4 &&
               sizeof(moq_playback_event_kind_t) == 4 &&
               sizeof(moq_playback_drop_reason_t) == 4 &&
               sizeof(moq_playback_feedback_kind_t) == 4,
               "kinds are fixed-width");

_Static_assert(MOQ_PLAYBACK_MEDIA_VIDEO == 1 && MOQ_PLAYBACK_MEDIA_AUDIO == 2 &&
               MOQ_PLAYBACK_PACKAGING_RAW == 1 &&
               MOQ_PLAYBACK_PACKAGING_CMAF == 2 &&
               MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO == 1 &&
               MOQ_PLAYBACK_CMD_DECODE_VIDEO == 2 &&
               MOQ_PLAYBACK_CMD_DECODE_CMAF == 3 &&
               MOQ_PLAYBACK_CMD_RESET == 4 &&
               MOQ_PLAYBACK_CMD_CONFIGURE_AUDIO == 5 &&
               MOQ_PLAYBACK_CMD_DECODE_AUDIO == 6 &&
               MOQ_PLAYBACK_RESET_GAP == 1 &&
               MOQ_PLAYBACK_RESET_DECODE_ERROR == 2 &&
               MOQ_PLAYBACK_RESET_TRACK_SWITCH == 3 &&
               MOQ_PLAYBACK_RESET_CONFIG_CHANGE == 4 &&
               MOQ_PLAYBACK_EVENT_GAP_DETECTED == 1 &&
               MOQ_PLAYBACK_EVENT_KEYFRAME_WAITING == 2 &&
               MOQ_PLAYBACK_EVENT_SKIP_FORWARD == 3 &&
               MOQ_PLAYBACK_EVENT_OBJECT_DROPPED == 4 &&
               MOQ_PLAYBACK_EVENT_TRACK_ENDED == 5 &&
               MOQ_PLAYBACK_EVENT_BACKLOG_SHED == 6 &&
               MOQ_PLAYBACK_EVENT_PARTIAL_GROUP_ABANDONED == 7 &&
               MOQ_PLAYBACK_EVENT_GAP_CONTINUED == 8 &&
               MOQ_PLAYBACK_EVENT_KNOWLEDGE_FORGOTTEN == 9 &&
               MOQ_PLAYBACK_GAP_CAUSE_UNKNOWN == 1 &&
               MOQ_PLAYBACK_GAP_CAUSE_STREAM_RESET == 2 &&
               MOQ_PLAYBACK_GAP_CAUSE_RECEIVER_DROP == 3 &&
               MOQ_PLAYBACK_GAP_DISCONTINUITY == 1 &&
               MOQ_PLAYBACK_GAP_CONTINUE == 2 &&
               MOQ_PLAYBACK_STREAM_FIN_END_OF_GROUP == 1 &&
               MOQ_PLAYBACK_STREAM_FIN == 2 &&
               MOQ_PLAYBACK_STREAM_RESET == 3 &&
               MOQ_PLAYBACK_STREAM_RECEIVER_DROP == 4 &&
               MOQ_PLAYBACK_STREAM_END_OF_TRACK == 5 &&
               MOQ_PLAYBACK_DROP_MALFORMED_LOC == 1 &&
               MOQ_PLAYBACK_DROP_MALFORMED_CMAF == 2 &&
               MOQ_PLAYBACK_DROP_MISSING_TIMESTAMP == 3 &&
               MOQ_PLAYBACK_DROP_NON_MONOTONIC_DTS == 4 &&
               MOQ_PLAYBACK_DROP_UNSUPPORTED_MULTI_SAMPLE == 5 &&
               MOQ_PLAYBACK_DROP_STALE == 6 &&
               MOQ_PLAYBACK_DROP_KEYFRAME_WAIT == 7 &&
               MOQ_PLAYBACK_DROP_UNSUPPORTED_MULTI_CHUNK == 8 &&
               MOQ_PLAYBACK_FEEDBACK_QUEUE_PRESSURE == 1 &&
               MOQ_PLAYBACK_FEEDBACK_DECODE_ERROR == 2,
               "kind and reason values are never renumbered");

#if UINTPTR_MAX == UINT64_MAX
/* LP64 layout. A change here is an ABI change and must be deliberate. */
_Static_assert(MOQ_PLAYBACK_CFG_V0_SIZE == 56 &&
               offsetof(moq_playback_cfg_t, max_samples_per_object) == 56 &&
               offsetof(moq_playback_cfg_t, ignore_eog_bit) == 60 &&
               offsetof(moq_playback_cfg_t, infer_end_of_group_from_next_group) == 61 &&
               offsetof(moq_playback_cfg_t, max_known_ranges) == 64 &&
               offsetof(moq_playback_cfg_t, max_held_commands) == 68 &&
               sizeof(moq_playback_cfg_t) == 72, "cfg layout");
/* A command hold is a frozen 64-bit value handle. */
_Static_assert(sizeof(moq_playback_command_hold_t) == 8 &&
               offsetof(moq_playback_command_hold_t, _opaque) == 0, "command hold");
_Static_assert(offsetof(moq_playback_track_cfg_t, media_type) == 4 &&
               offsetof(moq_playback_track_cfg_t, packaging) == 8 &&
               offsetof(moq_playback_track_cfg_t, codec) == 16 &&
               offsetof(moq_playback_track_cfg_t, init_data) == 32 &&
               offsetof(moq_playback_track_cfg_t, target_latency_us) == 72 &&
               offsetof(moq_playback_track_cfg_t, is_live) == 80 &&
               MOQ_PLAYBACK_TRACK_CFG_V0_SIZE == 81 &&
               offsetof(moq_playback_track_cfg_t, transport_version) == 88 &&
               offsetof(moq_playback_track_cfg_t, track_properties) == 96 &&
               offsetof(moq_playback_track_cfg_t, gap_policy) == 112 &&
               sizeof(moq_playback_track_cfg_t) == 120, "track cfg layout");
_Static_assert(offsetof(moq_playback_stream_terminal_t, group_id) == 8 &&
               offsetof(moq_playback_stream_terminal_t, object_id) == 24 &&
               offsetof(moq_playback_stream_terminal_t, error_code) == 40 &&
               offsetof(moq_playback_stream_terminal_t, has_last_object) == 48 &&
               offsetof(moq_playback_stream_terminal_t, last_object_id) == 56 &&
               MOQ_PLAYBACK_STREAM_TERMINAL_V0_SIZE == 72 &&
               sizeof(moq_playback_stream_terminal_t) == 72,
               "stream terminal layout");
/* The appended event members fit the v0 union: the record keeps its size. */
_Static_assert(offsetof(moq_playback_event_t, u.gap_detected.object_id) == 24 &&
               offsetof(moq_playback_event_t, u.gap_detected.cause) == 32 &&
               offsetof(moq_playback_event_t, u.gap_continued.hole_duration_us) == 24 &&
               offsetof(moq_playback_event_t, u.knowledge_forgotten.last_group_id) == 24 &&
               offsetof(moq_playback_event_t, u.gap_skipped_in_group.from_object_id) == 24 &&
               offsetof(moq_playback_event_t, u.gap_skipped_in_group.to_object_id) == 32,
               "appended event members");
_Static_assert(MOQ_PLAYBACK_OBJECT_V0_SIZE == 64 &&
               offsetof(moq_playback_object_t, track) == 8 &&
               offsetof(moq_playback_object_t, status) == 41 &&
               offsetof(moq_playback_object_t, payload) == 48 &&
               sizeof(moq_playback_object_t) == 64, "object layout");
_Static_assert(MOQ_PLAYBACK_CMD_V0_SIZE == 104 &&
               offsetof(moq_playback_cmd_t, kind) == 4 &&
               offsetof(moq_playback_cmd_t, track) == 8 &&
               offsetof(moq_playback_cmd_t, u) == 16 &&
               sizeof(moq_playback_cmd_t) == 104, "command layout");
_Static_assert(MOQ_PLAYBACK_EVENT_V0_SIZE == 40 &&
               offsetof(moq_playback_event_t, u) == 16 &&
               sizeof(moq_playback_event_t) == 40, "event layout");
_Static_assert(MOQ_PLAYBACK_PARSED_INPUT_V0_SIZE == 68 &&
               offsetof(moq_playback_parsed_input_t, track) == 8 &&
               offsetof(moq_playback_parsed_input_t, publisher_priority) == 40 &&
               offsetof(moq_playback_parsed_input_t, parsed) == 48 &&
               offsetof(moq_playback_parsed_input_t, payload) == 56 &&
               offsetof(moq_playback_parsed_input_t, timescale) == 64 &&
               sizeof(moq_playback_parsed_input_t) == 72, "parsed input layout");
_Static_assert(MOQ_PLAYBACK_FEEDBACK_V0_SIZE == 32 &&
               offsetof(moq_playback_feedback_t, u) == 16 &&
               sizeof(moq_playback_feedback_t) == 32, "feedback layout");
#endif

/* -- Allocator -------------------------------------------------------- */

typedef struct { int64_t balance; } bal_t;
static bal_t g_bal;
static void *b_alloc(size_t n, void *c)
{
    bal_t *b = (bal_t *)c;
    void *p = n ? malloc(n) : NULL;
    if (p) b->balance++;
    return p;
}
static void b_free(void *p, size_t n, void *c)
{
    bal_t *b = (bal_t *)c;
    (void)n;
    if (p) b->balance--;
    free(p);
}
static moq_alloc_t g_alloc = { &g_bal, b_alloc, NULL, b_free };

/* -- Helpers ---------------------------------------------------------- */

static moq_playback_t *new_pb(void)
{
    moq_playback_cfg_t cfg;
    moq_playback_cfg_init(&cfg);
    moq_playback_t *pb = NULL;
    CHECK(moq_playback_create(&g_alloc, &cfg, &pb) == MOQ_OK);
    return pb;
}

static moq_playback_track_t video_track(moq_playback_t *pb)
{
    moq_playback_track_cfg_t tc;
    moq_playback_track_cfg_init(&tc);
    tc.media_type = MOQ_PLAYBACK_MEDIA_VIDEO;
    tc.packaging = MOQ_PLAYBACK_PACKAGING_RAW;
    moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
    CHECK(moq_playback_add_track(pb, &tc, &t) == MOQ_OK);
    return t;
}

/* Push one independent draft-16 video object; returns its payload with one
 * extra ref held by the caller. The LOC-01 properties are written by hand
 * (timestamp key 0x02 as a two-byte QUIC varint, then the frame-marking key
 * at delta 0x02 with only the I bit), so this file passes no enum-typed
 * argument across the library boundary and stays valid under
 * -fshort-enums. */
static moq_rcbuf_t *push_kf(moq_playback_t *pb, moq_playback_track_t t,
                            uint64_t g, uint64_t o, bool eog)
{
    uint64_t ts = 1000 + g * 100 + o;
    const uint8_t loc[5] = { 0x02, (uint8_t)(0x40u | (ts >> 8)),
                             (uint8_t)ts, 0x02, 0x20 };
    moq_rcbuf_t *props = NULL;
    CHECK(moq_rcbuf_create(&g_alloc, loc, sizeof(loc), &props) == MOQ_OK);
    static const uint8_t data[2] = { 9, 9 };
    moq_rcbuf_t *payload = NULL;
    CHECK(moq_rcbuf_create(&g_alloc, data, sizeof(data), &payload) == MOQ_OK);
    moq_playback_object_t obj;
    moq_playback_object_init(&obj);
    obj.track = t;
    obj.group_id = g;
    obj.object_id = o;
    obj.end_of_group = eog;
    obj.payload = payload;
    obj.properties = props;
    CHECK(moq_playback_push_object(pb, &obj, 0) == MOQ_OK);
    moq_rcbuf_decref(props);
    return payload;
}

/* -- Sized polls ------------------------------------------------------ */

/* A poll into a record smaller than the v0 size is refused without
 * dequeuing or transferring a ref; a retry with a full record returns the
 * same command, stamped with the bytes written. */
static void test_short_poll_refused_without_dequeue(void)
{
    moq_playback_t *pb = new_pb();
    moq_playback_track_t t = video_track(pb);
    moq_rcbuf_t *payload = push_kf(pb, t, 1, 0, false);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);

    moq_playback_cmd_t c;
    memset(&c, 0xAB, sizeof(c));
    CHECK(moq_playback_poll_command(pb, &c, MOQ_PLAYBACK_CMD_V0_SIZE - 1) ==
          MOQ_ERR_ABI_MISMATCH);
    CHECK(c.struct_size == 0xABABABABu);   /* nothing written */
    CHECK(moq_playback_poll_command(pb, &c, 0) == MOQ_ERR_ABI_MISMATCH);

    CHECK(moq_playback_poll_command(pb, &c, sizeof(c)) == MOQ_OK);
    CHECK(c.struct_size == sizeof(c));
    CHECK(c.kind == MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO);
    moq_playback_cmd_cleanup(&c);

    uint32_t before = moq_rcbuf_refcount(payload);
    CHECK(moq_playback_poll_command(pb, &c, 8) == MOQ_ERR_ABI_MISMATCH);
    CHECK(moq_rcbuf_refcount(payload) == before);
    CHECK(moq_playback_poll_command(pb, &c, sizeof(c)) == MOQ_OK);
    CHECK(c.kind == MOQ_PLAYBACK_CMD_DECODE_VIDEO);
    CHECK(c.u.decode_video.payload == payload);
    CHECK(c.u.decode_video.object_id == 0);
    moq_playback_cmd_cleanup(&c);
    CHECK(moq_playback_poll_command(pb, &c, sizeof(c)) == MOQ_DONE);

    /* A short record is refused even when the queue is empty. */
    CHECK(moq_playback_poll_command(pb, &c, 4) == MOQ_ERR_ABI_MISMATCH);

    moq_rcbuf_decref(payload);
    moq_playback_destroy(pb);
}

/* A record larger than the library's receives exactly the library's bytes:
 * the stamp says how many, and the bytes past it are untouched. */
static void test_long_poll_stamps_written_size(void)
{
    moq_playback_t *pb = new_pb();
    moq_playback_track_t t = video_track(pb);
    moq_rcbuf_t *payload = push_kf(pb, t, 1, 0, false);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);

    struct { moq_playback_cmd_t c; uint8_t tail[16]; } big;
    memset(&big, 0xCD, sizeof(big));
    CHECK(moq_playback_poll_command(pb, &big.c, sizeof(big)) == MOQ_OK);
    CHECK(big.c.struct_size == sizeof(moq_playback_cmd_t));
    CHECK(big.tail[0] == 0xCD && big.tail[15] == 0xCD);
    moq_playback_cmd_cleanup(&big.c);

    /* Drain the decode, then produce one event: an object below the cursor
     * that the engine never held is reported STALE. */
    moq_playback_cmd_t c;
    while (moq_playback_poll_command(pb, &c, sizeof(c)) == MOQ_OK)
        moq_playback_cmd_cleanup(&c);
    moq_rcbuf_t *late = push_kf(pb, t, 0, 0, false);

    struct { moq_playback_event_t e; uint8_t tail[16]; } bige;
    memset(&bige, 0xCD, sizeof(bige));
    moq_playback_event_t e;
    CHECK(moq_playback_poll_event(pb, &e, MOQ_PLAYBACK_EVENT_V0_SIZE - 1) ==
          MOQ_ERR_ABI_MISMATCH);
    CHECK(moq_playback_poll_event(pb, &bige.e, sizeof(bige)) == MOQ_OK);
    CHECK(bige.e.struct_size == sizeof(moq_playback_event_t));
    CHECK(bige.e.kind == MOQ_PLAYBACK_EVENT_OBJECT_DROPPED);
    CHECK(bige.e.u.object_dropped.reason == MOQ_PLAYBACK_DROP_STALE);
    CHECK(bige.tail[0] == 0xCD && bige.tail[15] == 0xCD);
    CHECK(moq_playback_poll_event(pb, &bige.e, sizeof(bige)) == MOQ_DONE);

    moq_rcbuf_decref(late);
    moq_rcbuf_decref(payload);
    moq_playback_destroy(pb);
}

/* An event poll into a short record leaves the event queued. */
static void test_short_event_poll_keeps_event(void)
{
    moq_playback_t *pb = new_pb();
    moq_playback_track_t t = video_track(pb);
    moq_rcbuf_t *payload = push_kf(pb, t, 1, 0, false);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    moq_playback_cmd_t c;
    while (moq_playback_poll_command(pb, &c, sizeof(c)) == MOQ_OK)
        moq_playback_cmd_cleanup(&c);

    /* Group 0 object 0 lies below the cursor and was never held. */
    moq_rcbuf_t *late = push_kf(pb, t, 0, 0, false);
    moq_playback_event_t e;
    CHECK(moq_playback_poll_event(pb, &e, 8) == MOQ_ERR_ABI_MISMATCH);
    CHECK(moq_playback_poll_event(pb, &e, sizeof(e)) == MOQ_OK);
    CHECK(e.struct_size == sizeof(e));
    CHECK(e.kind == MOQ_PLAYBACK_EVENT_OBJECT_DROPPED);
    CHECK(e.u.object_dropped.reason == MOQ_PLAYBACK_DROP_STALE);

    moq_rcbuf_decref(late);
    moq_rcbuf_decref(payload);
    moq_playback_destroy(pb);
}

/* -- Cleanup contract ------------------------------------------------- */

/* A polled command outlives its engine: cleanup after destroy releases its
 * refs, and a second cleanup is a no-op. */
static void test_cleanup_after_destroy(void)
{
    int64_t base = g_bal.balance;
    moq_playback_t *pb = new_pb();
    moq_playback_track_t t = video_track(pb);
    moq_rcbuf_t *payload = push_kf(pb, t, 1, 0, false);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);

    moq_playback_cmd_t cfg_cmd, dec_cmd;
    CHECK(moq_playback_poll_command(pb, &cfg_cmd, sizeof(cfg_cmd)) == MOQ_OK);
    CHECK(moq_playback_poll_command(pb, &dec_cmd, sizeof(dec_cmd)) == MOQ_OK);
    moq_playback_destroy(pb);

    CHECK(moq_rcbuf_refcount(payload) == 2);
    moq_playback_cmd_cleanup(&dec_cmd);
    CHECK(moq_rcbuf_refcount(payload) == 1);
    moq_playback_cmd_cleanup(&dec_cmd);
    CHECK(dec_cmd.u.decode_video.payload == NULL);
    moq_playback_cmd_cleanup(&cfg_cmd);
    moq_playback_cmd_cleanup(NULL);

    moq_rcbuf_decref(payload);
    CHECK(g_bal.balance == base);
}

/* Cleanup reads only what the stamp covers: a record whose struct_size is
 * below the v0 size was never produced by a poll and is left untouched. */
static void test_cleanup_ignores_unstamped_record(void)
{
    static const uint8_t d[1] = { 1 };
    moq_rcbuf_t *buf = NULL;
    CHECK(moq_rcbuf_create(&g_alloc, d, 1, &buf) == MOQ_OK);
    moq_playback_cmd_t c;
    memset(&c, 0, sizeof(c));
    c.kind = MOQ_PLAYBACK_CMD_DECODE_VIDEO;
    c.u.decode_video.payload = buf;
    c.struct_size = (uint32_t)(MOQ_PLAYBACK_CMD_V0_SIZE - 1);
    moq_playback_cmd_cleanup(&c);
    CHECK(c.u.decode_video.payload == buf);
    CHECK(moq_rcbuf_refcount(buf) == 1);

    /* A kind that owns nothing is a no-op under any stamp. */
    c.struct_size = sizeof(c);
    c.kind = MOQ_PLAYBACK_CMD_RESET;
    moq_playback_cmd_cleanup(&c);
    CHECK(moq_rcbuf_refcount(buf) == 1);
    moq_rcbuf_decref(buf);
}

/* -- Sized initializers ----------------------------------------------- */

static void test_init_sized_writes_only_the_prefix(void)
{
    struct { moq_playback_cfg_t c; uint8_t guard[8]; } g;
    memset(&g, 0x5A, sizeof(g));
    moq_playback_cfg_init_sized(&g.c, MOQ_PLAYBACK_CFG_V0_SIZE);
    CHECK(g.c.struct_size == MOQ_PLAYBACK_CFG_V0_SIZE);
    CHECK(g.c.max_tracks == 4 && g.c.max_track_config_bytes == 4096);
    /* max_samples_per_object lies past the prefix: untouched. */
    unsigned char raw[4];
    memcpy(raw, (unsigned char *)&g.c +
           offsetof(moq_playback_cfg_t, max_samples_per_object), 4);
    CHECK(raw[0] == 0x5A && raw[3] == 0x5A);

    memset(&g, 0x5A, sizeof(g));
    moq_playback_cfg_init_sized(&g.c, sizeof(g));   /* larger than ours */
    CHECK(g.c.struct_size == sizeof(moq_playback_cfg_t));
    CHECK(g.c.max_samples_per_object == 16);
    CHECK(g.guard[0] == 0x5A && g.guard[7] == 0x5A);

    memset(&g, 0x5A, sizeof(g));
    moq_playback_cfg_init_sized(&g.c, 3);
    CHECK(((unsigned char *)&g.c)[0] == 0x5A);
    moq_playback_cfg_init_sized(NULL, sizeof(g.c));

    moq_playback_track_cfg_t tc;
    memset(&tc, 0x5A, sizeof(tc));
    moq_playback_track_cfg_init_sized(&tc, MOQ_PLAYBACK_TRACK_CFG_V0_SIZE);
    CHECK(tc.struct_size == MOQ_PLAYBACK_TRACK_CFG_V0_SIZE);
    CHECK(tc.transport_version == 0x5A5A5A5Au);   /* not covered */
    moq_playback_track_cfg_init(&tc);
    CHECK(tc.struct_size == sizeof(tc));
    CHECK(tc.transport_version == MOQ_VERSION_DRAFT_16);
    CHECK(tc.track_properties.len == 0 && tc.track_properties.data == NULL);

    moq_playback_object_t o;
    memset(&o, 0x5A, sizeof(o));
    moq_playback_object_init(&o);
    CHECK(o.struct_size == sizeof(o) && o.payload == NULL);

    moq_playback_feedback_t f;
    memset(&f, 0x5A, sizeof(f));
    moq_playback_feedback_init(&f);
    CHECK(f.struct_size == sizeof(f) && f.kind == 0);

    moq_playback_parsed_input_t pi;
    memset(&pi, 0x5A, sizeof(pi));
    moq_playback_parsed_input_init(&pi);
    CHECK(pi.struct_size == sizeof(pi) && pi.parsed == NULL &&
          pi.payload == NULL);
    memset(&pi, 0x5A, sizeof(pi));
    moq_playback_parsed_input_init_sized(&pi, 3);
    CHECK(((unsigned char *)&pi)[0] == 0x5A);
}

/* -- Prefix-safe readers ---------------------------------------------- */

/* A cfg whose struct_size ends partway through an appended field gets that
 * field's default, whatever its bytes hold: here ignore_eog_bit reads as
 * false, so an end-of-group bit still closes the group. */
static void test_cfg_reader_ignores_uncovered_tail(void)
{
    moq_playback_cfg_t cfg;
    moq_playback_cfg_init(&cfg);
    cfg.ignore_eog_bit = true;
    cfg.struct_size = (uint32_t)offsetof(moq_playback_cfg_t, ignore_eog_bit);
    moq_playback_t *pb = NULL;
    CHECK(moq_playback_create(&g_alloc, &cfg, &pb) == MOQ_OK);
    moq_playback_track_t t = video_track(pb);
    moq_rcbuf_t *a = push_kf(pb, t, 1, 0, true);
    moq_rcbuf_t *b = push_kf(pb, t, 2, 0, false);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    size_t decodes = 0;
    moq_playback_cmd_t c;
    while (moq_playback_poll_command(pb, &c, sizeof(c)) == MOQ_OK) {
        if (c.kind == MOQ_PLAYBACK_CMD_DECODE_VIDEO) decodes++;
        moq_playback_cmd_cleanup(&c);
    }
    CHECK(decodes == 2);
    moq_rcbuf_decref(a);
    moq_rcbuf_decref(b);
    moq_playback_destroy(pb);

    cfg.struct_size = (uint32_t)(MOQ_PLAYBACK_CFG_V0_SIZE - 1);
    CHECK(moq_playback_create(&g_alloc, &cfg, &pb) == MOQ_ERR_INVAL);
}

/* A caller compiled against the released layout, which ends after
 * infer_end_of_group_from_next_group, passes a struct_size of that
 * layout's sizeof (where max_known_ranges begins): a heap block of exactly
 * that size, its padding after the last bool poisoned. create takes the
 * defaults of both appended fields: held polls stay disabled. That is a
 * different caller from a current one passing a short prefix that stops
 * before max_held_commands. */
static void test_released_cfg_boundary(void)
{
    size_t released = offsetof(moq_playback_cfg_t, max_known_ranges);
    moq_playback_cfg_t full;
    moq_playback_cfg_init(&full);
    unsigned char *block = (unsigned char *)malloc(released);
    CHECK(block != NULL);
    if (!block) return;
    memcpy(block, &full, released);
    size_t after_bools = offsetof(moq_playback_cfg_t, infer_end_of_group_from_next_group) +
                         sizeof(bool);
    memset(block + after_bools, 0xA5, released - after_bools);
    uint32_t size = (uint32_t)released;
    memcpy(block, &size, sizeof(size));
    moq_playback_t *pb = NULL;
    CHECK(moq_playback_create(&g_alloc, (const moq_playback_cfg_t *)(void *)block, &pb) ==
          MOQ_OK);
    moq_playback_cmd_t c;
    moq_playback_command_hold_t h;
    CHECK(moq_playback_poll_command_held(pb, &c, sizeof(c), &h) == MOQ_ERR_UNSUPPORTED);
    moq_playback_destroy(pb);
    free(block);
}

/* A caller compiled against the v0 layouts passes objects that end at the
 * v0 floor: heap blocks of exactly that size here, so a read past the end
 * is a sanitizer error. create and add_track accept them and take the
 * defaults of every appended field (draft 16 for the track). */
static void test_v0_sized_storage_accepted(void)
{
    moq_playback_cfg_t full;
    moq_playback_cfg_init(&full);
    full.struct_size = (uint32_t)MOQ_PLAYBACK_CFG_V0_SIZE;
    unsigned char *cfg_v0 = malloc(MOQ_PLAYBACK_CFG_V0_SIZE);
    memcpy(cfg_v0, &full, MOQ_PLAYBACK_CFG_V0_SIZE);
    moq_playback_t *pb = NULL;
    CHECK(moq_playback_create(&g_alloc, (const moq_playback_cfg_t *)cfg_v0,
                              &pb) == MOQ_OK);
    free(cfg_v0);

    moq_playback_track_cfg_t tfull;
    moq_playback_track_cfg_init(&tfull);
    tfull.media_type = MOQ_PLAYBACK_MEDIA_VIDEO;
    tfull.packaging = MOQ_PLAYBACK_PACKAGING_RAW;
    tfull.struct_size = (uint32_t)MOQ_PLAYBACK_TRACK_CFG_V0_SIZE;
    unsigned char *tc_v0 = malloc(MOQ_PLAYBACK_TRACK_CFG_V0_SIZE);
    memcpy(tc_v0, &tfull, MOQ_PLAYBACK_TRACK_CFG_V0_SIZE);
    moq_playback_track_t t = MOQ_PLAYBACK_TRACK_INVALID;
    CHECK(moq_playback_add_track(pb, (const moq_playback_track_cfg_t *)tc_v0,
                                 &t) == MOQ_OK);
    free(tc_v0);

    /* Draft-16 LOC parses: the default for an old caller. */
    moq_rcbuf_t *payload = push_kf(pb, t, 1, 0, false);
    CHECK(moq_playback_tick(pb, 0) == MOQ_OK);
    size_t decodes = 0;
    moq_playback_cmd_t c;
    while (moq_playback_poll_command(pb, &c, sizeof(c)) == MOQ_OK) {
        if (c.kind == MOQ_PLAYBACK_CMD_DECODE_VIDEO) decodes++;
        moq_playback_cmd_cleanup(&c);
    }
    CHECK(decodes == 1);
    moq_rcbuf_decref(payload);
    moq_playback_destroy(pb);
}

/* Inputs below their v0 floor are refused. */
static void test_inputs_below_floor_refused(void)
{
    moq_playback_t *pb = new_pb();
    moq_playback_track_t t = video_track(pb);

    moq_playback_object_t o;
    moq_playback_object_init(&o);
    o.track = t;
    o.status = MOQ_OBJECT_END_OF_GROUP;
    o.struct_size = (uint32_t)(MOQ_PLAYBACK_OBJECT_V0_SIZE - 1);
    CHECK(moq_playback_push_object(pb, &o, 0) == MOQ_ERR_INVAL);
    o.struct_size = (uint32_t)MOQ_PLAYBACK_OBJECT_V0_SIZE;
    CHECK(moq_playback_push_object(pb, &o, 0) == MOQ_OK);

    moq_playback_feedback_t f;
    moq_playback_feedback_init(&f);
    f.kind = MOQ_PLAYBACK_FEEDBACK_QUEUE_PRESSURE;
    f.track = t;
    f.struct_size = (uint32_t)(MOQ_PLAYBACK_FEEDBACK_V0_SIZE - 1);
    CHECK(moq_playback_handle_feedback(pb, &f, 0) == MOQ_ERR_INVAL);
    f.struct_size = (uint32_t)MOQ_PLAYBACK_FEEDBACK_V0_SIZE;
    CHECK(moq_playback_handle_feedback(pb, &f, 0) == MOQ_OK);

    moq_playback_track_cfg_t tc;
    moq_playback_track_cfg_init(&tc);
    tc.media_type = MOQ_PLAYBACK_MEDIA_AUDIO;
    tc.packaging = MOQ_PLAYBACK_PACKAGING_RAW;
    tc.struct_size = (uint32_t)(MOQ_PLAYBACK_TRACK_CFG_V0_SIZE - 1);
    moq_playback_track_t t2;
    CHECK(moq_playback_add_track(pb, &tc, &t2) == MOQ_ERR_INVAL);

    moq_playback_destroy(pb);
}

int main(void)
{
    test_short_poll_refused_without_dequeue();
    test_long_poll_stamps_written_size();
    test_short_event_poll_keeps_event();
    test_cleanup_after_destroy();
    test_cleanup_ignores_unstamped_record();
    test_init_sized_writes_only_the_prefix();
    test_cfg_reader_ignores_uncovered_tail();
    test_released_cfg_boundary();
    test_inputs_below_floor_refused();
    test_v0_sized_storage_accepted();

    CHECK(g_bal.balance == 0);
    if (failures) {
        fprintf(stderr, "%d failure(s)\n", failures);
        return 1;
    }
    printf("playback abi: all passed\n");
    return 0;
}
