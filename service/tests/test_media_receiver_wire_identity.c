/*
 * Wire identity through moq_media_receiver_poll_object.
 *
 * A polled media object carries the identity the wire gave it -- group,
 * subgroup, object and the session-delivered publisher priority -- appended
 * after the frozen v0 prefix, at full width, with zero a valid id. Subgroup
 * presence is a transport fact: a subgroup-stream object has one, as the
 * SUBGROUP_HEADER resolved it; a datagram-forwarded object has none, and only
 * has_subgroup says so (its subgroup_id is zero, which is also a real id).
 *
 * Older callers are judged against a frozen copy of the v0 layout, not a
 * formula: one polls with the frozen minimum prefix, one with the complete
 * old struct sizeof (on ARMv7 those differ by the old trailing padding), and
 * each gets exactly what it asked for, its stamp says so, nothing past its
 * buffer is written, and cleanup stays inside the stamp. A stamp that does
 * not reach the end of has_subgroup is not an identity record.
 *
 * The oracle is the wire: a REAL receiver, driven through its real pump hook
 * over a moq-sim pair, is fed hand-encoded draft-16 subgroup streams and one
 * object datagram whose header fields are written here. Every expectation is
 * the tuple that was encoded, never a value read back from the new fields.
 * Two tracks repeat the same numeric ids, groups arrive out of order, one
 * stream carries ids above UINT32_MAX with distinct group, subgroup and
 * object values, one uses the first-object subgroup mode, and every id is
 * apart from the parsed timestamps (a LOC object without properties has
 * capture absent and presentation time 0). Arrival order and payload
 * ownership are unchanged.
 *
 * The scenario is draft-16 wire; draft-18 delivers the same OBJECT_RECEIVED
 * metadata but is not measured here.
 */
#include <moq/media_receiver.h>
#include <moq/msf.h>
#include <moq/sim.h>
#include <moq/session.h>
#include <moq/control.h>
#include <moq/codec.h>
#include "test_support.h"

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <stdio.h>

static int failures = 0;

/* -- Test seams (media_receiver.c, MOQ_MEDIA_RECEIVER_TESTING) ----------- */
moq_media_receiver_t *moq_media_receiver_test_new_cfg(const moq_media_receiver_cfg_t *cfg);
void moq_media_receiver_test_destroy_with_session(moq_media_receiver_t *r, moq_session_t *session, uint64_t now_us);
void moq_media_receiver_test_pump(moq_media_receiver_t *r, moq_session_t *session, uint64_t now_us);

/* -- The frozen v0 layout, copied field for field, as the authority ------ */
typedef struct old_media_object_v0 {
    uint32_t              struct_size;
    moq_media_track_t    *track;
    uint32_t              config_generation;
    moq_media_packaging_t packaging;
    moq_object_status_t   status;
    bool                  end_of_group;
    bool                  datagram;
    bool                  keyframe;
    bool                  has_capture_time;
    uint64_t              capture_time_us;
    uint64_t              decode_time_us;
    int64_t               composition_offset_us;
    uint64_t              presentation_time_us;
    moq_bytes_t           payload;
    moq_bytes_t           fragment;
    size_t                mdat_offset;
    size_t                mdat_len;
    size_t                sample_count;
    const moq_cmaf_sample_t *samples;
    moq_rcbuf_t          *payload_ref;
    moq_rcbuf_t          *properties_ref;
    moq_cmaf_sample_t    *samples_owned;
} old_media_object_v0_t;

#define OBJECT_V0_SIZE (offsetof(old_media_object_v0_t, samples_owned) + sizeof(moq_cmaf_sample_t *))
#define OBJECT_IDENTITY_END (offsetof(moq_media_object_t, has_subgroup) + sizeof(bool))
#define SAME_OFFSET(f) (offsetof(old_media_object_v0_t, f) == offsetof(moq_media_object_t, f))

_Static_assert(SAME_OFFSET(struct_size) && SAME_OFFSET(track) && SAME_OFFSET(status) && SAME_OFFSET(has_capture_time) &&
               SAME_OFFSET(presentation_time_us) && SAME_OFFSET(payload) && SAME_OFFSET(fragment) &&
               SAME_OFFSET(sample_count) && SAME_OFFSET(samples) && SAME_OFFSET(payload_ref) &&
               SAME_OFFSET(properties_ref) && SAME_OFFSET(samples_owned),
               "the v0 prefix is laid out exactly as the frozen copy");
_Static_assert(OBJECT_V0_SIZE == offsetof(moq_media_object_t, samples_owned) + sizeof(moq_cmaf_sample_t *),
               "the frozen minimum is the end of samples_owned");
/* The tail begins where the complete old struct ENDED (its sizeof, padding
 * included), never inside that padding: 152 on LP64, 104 on ARMv7 (a 100-byte
 * prefix in a 104-byte struct), 100 on i686. */
_Static_assert(offsetof(moq_media_object_t, group_id) == sizeof(old_media_object_v0_t),
               "the identity tail must start at the old sizeof");
_Static_assert(sizeof(((moq_media_object_t *)0)->group_id) == 8 &&
               sizeof(((moq_media_object_t *)0)->subgroup_id) == 8 &&
               sizeof(((moq_media_object_t *)0)->object_id) == 8 &&
               sizeof(((moq_media_object_t *)0)->publisher_priority) == 1 &&
               sizeof(((moq_media_object_t *)0)->has_subgroup) == 1, "full-width ids, byte priority and flag");
_Static_assert(offsetof(moq_media_object_t, subgroup_id) == offsetof(moq_media_object_t, group_id) + 8 &&
               offsetof(moq_media_object_t, object_id) == offsetof(moq_media_object_t, group_id) + 16 &&
               offsetof(moq_media_object_t, publisher_priority) == offsetof(moq_media_object_t, group_id) + 24 &&
               offsetof(moq_media_object_t, has_subgroup) == offsetof(moq_media_object_t, group_id) + 25,
               "the tail is packed in declaration order with no holes");
_Static_assert(OBJECT_IDENTITY_END <= sizeof(moq_media_object_t), "the record ends inside the struct");

/* -- Scripted-peer byte builders (draft-16) ------------------------------ */
static size_t put_varint(uint8_t *buf, uint64_t v)
{
    if (v < 0x40) { buf[0] = (uint8_t)v; return 1; }
    if (v < 0x4000) { buf[0] = (uint8_t)(0x40 | (v >> 8)); buf[1] = (uint8_t)(v & 0xff); return 2; }
    if (v < 0x40000000ull) {
        buf[0] = (uint8_t)(0x80 | (v >> 24)); buf[1] = (uint8_t)((v >> 16) & 0xff);
        buf[2] = (uint8_t)((v >> 8) & 0xff);  buf[3] = (uint8_t)(v & 0xff);
        return 4;
    }
    buf[0] = (uint8_t)(0xc0 | (v >> 56));
    for (int i = 1; i < 8; i++) buf[i] = (uint8_t)((v >> (8 * (7 - i))) & 0xff);
    return 8;
}

static size_t build_subscribe_ok(uint8_t *out, size_t out_cap, uint64_t request_id, uint64_t track_alias)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, out, out_cap);
    if (moq_d16_encode_subscribe_ok(&w, request_id, track_alias, NULL, 0, NULL, 0) < 0) return 0;
    return moq_buf_writer_offset(&w);
}

/* One wire object and the tuple it encodes. SUBGROUP_HEADER types used:
 *   0x14  subgroup id PRESENT, priority byte present;
 *   0x12  subgroup id FIRST_OBJ (the first object's id), priority present;
 *   0x30  subgroup id mode ZERO, DEFAULT_PRIORITY (the session delivers 128).
 * Objects on a stream are { object id delta, payload length, payload }; the
 * first delta is the id itself. The datagram is type 0x00 (object id and
 * priority present, no extensions). */
typedef struct {
    const char *track;      /* "cam-a" (alias 2) or "cam-b" (alias 3) */
    uint64_t    group;
    bool        has_subgroup;
    uint64_t    subgroup;
    uint64_t    object;
    uint8_t     priority;
    const char *payload;
} wire_t;

#define HI(v) (0x100000000ull + (v))     /* above UINT32_MAX */

/* Intake order == poll order. Ids repeat across the two tracks, group 5
 * lands after group 9, one stream is entirely above UINT32_MAX with distinct
 * group, subgroup and object, and no id equals the parsed timestamps (0). */
static const wire_t k_wire[] = {
    { "cam-b", 9, true,  4, 3, 17,  "b:g9:s4:o3" },                 /* stream 301, 0x14 */
    { "cam-b", 9, true,  4, 4, 17,  "b:g9:s4:o4" },
    { "cam-b", 9, true,  4, 5, 17,  "b:g9:s4:o5" },
    { "cam-a", 9, true,  0, 3, 128, "a:g9:s0:o3" },                 /* stream 302, 0x30 */
    { "cam-a", 9, true,  0, 4, 128, "a:g9:s0:o4" },
    { "cam-a", 7, false, 0, 5, 200, "a:g7:dg:o5" },                 /* datagram, 0x00 */
    { "cam-b", 5, true,  0, 0, 17,  "b:g5:s0:o0" },                 /* stream 303, 0x14 */
    { "cam-b", 5, true,  0, 1, 17,  "b:g5:s0:o1" },
    { "cam-a", HI(3), true, HI(5), HI(7),  9, "a:hi:o7" },          /* stream 304, 0x14 */
    { "cam-a", HI(3), true, HI(5), HI(10), 9, "a:hi:o10" },
    { "cam-b", 11, true, 6, 6, 33,  "b:g11:first:o6" },             /* stream 305, 0x12 */
    { "cam-b", 11, true, 6, 8, 33,  "b:g11:first:o8" },
};
#define K_OBJECTS (sizeof(k_wire) / sizeof(k_wire[0]))

static size_t subgroup_stream(uint8_t *buf, uint8_t type, uint64_t alias, uint64_t group, bool with_subgroup,
                              uint64_t subgroup, bool with_priority, uint8_t priority,
                              const wire_t *objects, size_t n)
{
    size_t o = 0;
    buf[o++] = type;
    o += put_varint(buf + o, alias);
    o += put_varint(buf + o, group);
    if (with_subgroup) o += put_varint(buf + o, subgroup);
    if (with_priority) buf[o++] = priority;
    uint64_t prev = 0;
    for (size_t k = 0; k < n; k++) {
        uint64_t delta = k == 0 ? objects[k].object : objects[k].object - prev - 1;
        o += put_varint(buf + o, delta);
        size_t len = strlen(objects[k].payload);
        o += put_varint(buf + o, len);
        memcpy(buf + o, objects[k].payload, len);
        o += len;
        prev = objects[k].object;
    }
    return o;
}

static size_t object_datagram(uint8_t *buf, uint64_t alias, const wire_t *w)
{
    size_t o = 0;
    buf[o++] = 0x00;
    o += put_varint(buf + o, alias);
    o += put_varint(buf + o, w->group);
    o += put_varint(buf + o, w->object);
    buf[o++] = w->priority;
    size_t len = strlen(w->payload);
    memcpy(buf + o, w->payload, len);
    return o + len;
}

/* -- request-id learning: decode the receiver's outbound SUBSCRIBEs by name -- */
typedef struct {
    bool have_catalog, have_a, have_b;
    uint64_t catalog_rid, a_rid, b_rid;
    int subscribe_seen;
} learned_t;

static void drain_and_learn(moq_session_t *client, learned_t *l)
{
    moq_action_t acts[16];
    size_t n;
    while ((n = moq_session_poll_actions(client, acts, 16)) > 0) {
        for (size_t i = 0; i < n; i++) {
            if (acts[i].kind != MOQ_ACTION_SEND_CONTROL) continue;
            moq_control_envelope_t env;
            moq_buf_reader_t r;
            moq_buf_reader_init(&r, acts[i].u.send_control.data, acts[i].u.send_control.len);
            if (moq_control_decode_envelope(&r, &env) < 0 || env.msg_type != MOQ_D16_SUBSCRIBE) continue;
            moq_bytes_t ns_parts[8];
            moq_kvp_entry_t params[16];
            moq_d16_subscribe_t s;
            memset(&s, 0, sizeof(s));
            s.params = params;
            s.params_cap = 16;
            if (moq_d16_decode_subscribe(env.payload, env.payload_len, ns_parts, 8, &s) < 0) continue;
            l->subscribe_seen++;
            if (s.track_name.len == 5 && memcmp(s.track_name.data, "cam-a", 5) == 0) { l->have_a = true; l->a_rid = s.request_id; }
            else if (s.track_name.len == 5 && memcmp(s.track_name.data, "cam-b", 5) == 0) { l->have_b = true; l->b_rid = s.request_id; }
            else if (!l->have_catalog) { l->have_catalog = true; l->catalog_rid = s.request_id; }
        }
    }
}

static const char CATALOG_JSON[] =
    "{\"version\":1,\"tracks\":["
    "{\"name\":\"cam-a\",\"packaging\":\"loc\",\"isLive\":true,\"role\":\"video\",\"codec\":\"avc1.42e01e\"},"
    "{\"name\":\"cam-b\",\"packaging\":\"loc\",\"isLive\":true,\"role\":\"video\",\"codec\":\"avc1.42e01e\"}]}";

static const char *track_name_of(moq_media_track_t *cam_a, moq_media_track_t *cam_b, const moq_media_track_t *t)
{
    return t == cam_a ? "cam-a" : t == cam_b ? "cam-b" : "?";
}

static bool feed_stream(moq_session_t *client, uint64_t ref, const uint8_t *buf, size_t n, uint64_t now)
{
    moq_result_t rc = moq_session_on_data_bytes(client, moq_stream_ref_from_u64(ref), buf, n, true, now);
    if (rc < 0) fprintf(stderr, "stream %llu rejected: rc=%d\n", (unsigned long long)ref, (int)rc);
    MOQ_TEST_CHECK(rc >= 0);
    return rc >= 0;
}

static bool payload_is(const moq_media_object_t *o, const char *want)
{
    return o->payload.len == strlen(want) && memcmp(o->payload.data, want, o->payload.len) == 0;
}

/* Poll with `size` into a sentinel-filled buffer. On success the stamp is
 * exactly min(size, the library's struct) and nothing past `size` moved,
 * before and after cleanup. Returns whether an object was consumed. */
static bool poll_old_caller(moq_media_receiver_t *r, size_t size, const wire_t *w, moq_media_track_t *want_track)
{
    union { moq_media_object_t obj; unsigned char raw[sizeof(moq_media_object_t) + 32]; } u;
    memset(u.raw, 0xA5, sizeof u.raw);
    moq_result_t rc = moq_media_receiver_poll_object(r, &u.obj, size);
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
    if (rc != MOQ_OK) return false;                 /* a poisoned object is never read or cleaned */
    size_t want_stamp = size < sizeof(moq_media_object_t) ? size : sizeof(moq_media_object_t);
    MOQ_TEST_CHECK_EQ_SIZE((size_t)u.obj.struct_size, want_stamp);
    MOQ_TEST_CHECK(u.obj.struct_size < OBJECT_IDENTITY_END);   /* not an identity record */
    bool untouched = true;
    for (size_t i = size; i < sizeof u.raw; i++) untouched = untouched && u.raw[i] == 0xA5;
    MOQ_TEST_CHECK(untouched);
    MOQ_TEST_CHECK(u.obj.track == want_track);
    MOQ_TEST_CHECK(payload_is(&u.obj, w->payload));
    MOQ_TEST_CHECK(u.obj.datagram == !w->has_subgroup);
    moq_media_object_cleanup(&u.obj);
    moq_media_object_cleanup(&u.obj);               /* idempotent */
    untouched = true;
    for (size_t i = size; i < sizeof u.raw; i++) untouched = untouched && u.raw[i] == 0xA5;
    MOQ_TEST_CHECK(untouched);
    return true;
}

int main(void)
{
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = moq_alloc_default();
    cfg.seed = 7;
    cfg.initial_now_us = 1000;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = 16;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 16;
    moq_simpair_t *sp = NULL;
    MOQ_TEST_CHECK(moq_simpair_create(&cfg, &sp) == MOQ_OK && sp != NULL);
    if (!sp) return 1;
    moq_simpair_start(sp);
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    moq_session_t *client = moq_simpair_client(sp);
    moq_session_t *server = moq_simpair_server(sp);
    MOQ_TEST_CHECK(moq_session_state(client) == MOQ_SESS_ESTABLISHED);
    { moq_event_t ev;
      while (moq_session_poll_events(client, &ev, 1) == 1) moq_event_cleanup(&ev);
      while (moq_session_poll_events(server, &ev, 1) == 1) moq_event_cleanup(&ev); }

    moq_bytes_t ns_parts[2] = { MOQ_BYTES_LITERAL("svc"), MOQ_BYTES_LITERAL("demo") };
    moq_media_receiver_cfg_t rcfg;
    moq_media_receiver_cfg_init_live(&rcfg);
    rcfg.namespace_.parts = ns_parts;
    rcfg.namespace_.count = 2;
    rcfg.auto_subscribe = true;
    rcfg.time_mode = MOQ_MEDIA_TIME_RAW;
    rcfg.overflow.policy = MOQ_MEDIA_OVERFLOW_DROP_GROUP;
    rcfg.overflow.max_objects = 64;
    rcfg.overflow.max_bytes = 1u << 20;
    moq_media_receiver_t *r = moq_media_receiver_test_new_cfg(&rcfg);
    MOQ_TEST_CHECK(r != NULL);
    if (!r) { moq_simpair_destroy(sp); return 1; }

    uint64_t now = moq_simpair_now_us(sp);
    learned_t learned;
    memset(&learned, 0, sizeof(learned));
    uint8_t ctrl[256], buf[512];
    bool cat_ok = false, cat_data = false, a_ok = false, b_ok = false, media_fed = false, catalog_ready = false;
    moq_media_track_t *cam_a = NULL, *cam_b = NULL;

    for (int cycle = 0; cycle < 60 && !media_fed; cycle++) {
        now += 1000;
        moq_media_receiver_test_pump(r, client, now);
        drain_and_learn(client, &learned);
        if (learned.have_catalog && !cat_ok) {
            size_t n = build_subscribe_ok(ctrl, sizeof(ctrl), learned.catalog_rid, 0);
            MOQ_TEST_CHECK(n > 0 && moq_session_on_control_bytes(client, ctrl, n, now) >= 0);
            cat_ok = true;
        }
        if (cat_ok && !cat_data) {
            size_t o = 0;
            buf[o++] = 0x30;
            o += put_varint(buf + o, 0);
            o += put_varint(buf + o, 0);
            o += put_varint(buf + o, 0);
            size_t jlen = sizeof(CATALOG_JSON) - 1;
            o += put_varint(buf + o, jlen);
            memcpy(buf + o, CATALOG_JSON, jlen);
            o += jlen;
            (void)feed_stream(client, 101, buf, o, now);
            cat_data = true;
        }
        if (catalog_ready && learned.have_a && !a_ok) {
            size_t n = build_subscribe_ok(ctrl, sizeof(ctrl), learned.a_rid, 2);
            MOQ_TEST_CHECK(n > 0 && moq_session_on_control_bytes(client, ctrl, n, now) >= 0);
            a_ok = true;
        }
        if (catalog_ready && learned.have_b && !b_ok) {
            size_t n = build_subscribe_ok(ctrl, sizeof(ctrl), learned.b_rid, 3);
            MOQ_TEST_CHECK(n > 0 && moq_session_on_control_bytes(client, ctrl, n, now) >= 0);
            b_ok = true;
        }
        if (a_ok && b_ok) {
            /* Each delivery is pumped into the receiver before the next, so the
             * session's bounded event queue never refuses one; intake order is
             * the feed order. */
            (void)moq_session_process_pending(client, now);
            bool fed = true;
#define FEED(ref, ...) do { \
        if (fed) fed = feed_stream(client, (ref), buf, subgroup_stream(buf, __VA_ARGS__), now); \
        moq_media_receiver_test_pump(r, client, now); \
    } while (0)
            FEED(301, 0x14, 3, 9, true, 4, true, 17, &k_wire[0], 3);
            FEED(302, 0x30, 2, 9, false, 0, false, 0, &k_wire[3], 2);
            if (fed) {
                size_t n = object_datagram(buf, 2, &k_wire[5]);
                moq_result_t rc = moq_session_on_datagram(client, buf, n, now);
                if (rc < 0) fprintf(stderr, "datagram rejected: rc=%d\n", (int)rc);
                MOQ_TEST_CHECK(rc >= 0);
                fed = rc >= 0;
                moq_media_receiver_test_pump(r, client, now);
            }
            FEED(303, 0x14, 3, 5, true, 0, true, 17, &k_wire[6], 2);
            FEED(304, 0x14, 2, HI(3), true, HI(5), true, 9, &k_wire[8], 2);
            /* FIRST_OBJ mode: no subgroup field; the session resolves it to 6, the first object's id. */
            FEED(305, 0x12, 3, 11, false, 0, true, 33, &k_wire[10], 2);
#undef FEED
            media_fed = fed;
            if (!fed) break;
        }
        moq_media_track_event_t te;
        while (moq_media_receiver_poll_track(r, &te, sizeof(te)) == MOQ_OK) {
            if (te.kind == MOQ_MEDIA_TRACK_ADDED && te.desc) {
                if (te.desc->name.len == 5 && memcmp(te.desc->name.data, "cam-a", 5) == 0) cam_a = te.track;
                if (te.desc->name.len == 5 && memcmp(te.desc->name.data, "cam-b", 5) == 0) cam_b = te.track;
            } else if (te.kind == MOQ_MEDIA_CATALOG_READY) {
                catalog_ready = true;
            }
        }
        (void)moq_session_process_pending(client, now);
        if (moq_media_receiver_is_fatal(r)) break;
    }
    for (int cycle = 0; cycle < 8; cycle++) {
        now += 1000;
        moq_media_receiver_test_pump(r, client, now);
        drain_and_learn(client, &learned);
    }
    bool ready = media_fed && cam_a && cam_b && !moq_media_receiver_is_fatal(r);
    MOQ_TEST_CHECK(ready);
    MOQ_TEST_CHECK_EQ_INT(learned.subscribe_seen, 3);

    size_t next = 0;
    if (ready) {
        /* 1. The frozen minimum prefix: the first wire object. */
        if (poll_old_caller(r, OBJECT_V0_SIZE, &k_wire[0], cam_b)) next++;
        /* 2. The complete old struct (its sizeof, padding included): the second. */
        if (poll_old_caller(r, sizeof(old_media_object_v0_t), &k_wire[1], cam_b)) next++;
        /* 3. Shorter than the minimum is refused and consumes nothing. */
        {
            moq_media_object_t o;
            MOQ_TEST_CHECK_EQ_INT((int)moq_media_receiver_poll_object(r, &o, OBJECT_V0_SIZE - 1), (int)MOQ_ERR_INVAL);
        }
        /* 4. The identity tail, against the encoded tuples, in intake order. */
        for (size_t k = next; k < K_OBJECTS; k++) {
            const wire_t *w = &k_wire[k];
            moq_media_object_t o;
            memset(&o, 0xA5, sizeof o);
            moq_result_t rc = moq_media_receiver_poll_object(r, &o, sizeof o);
            MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
            if (rc != MOQ_OK) break;                    /* nothing owned; a poisoned object is not touched */
            MOQ_TEST_CHECK(o.struct_size >= OBJECT_IDENTITY_END);
            const char *name = track_name_of(cam_a, cam_b, o.track);
            fprintf(stderr, "OBJECT %zu: %s g=%llu sg=%d/%llu o=%llu p=%u pts=%llu %.*s\n", k, name,
                    (unsigned long long)o.group_id, (int)o.has_subgroup, (unsigned long long)o.subgroup_id,
                    (unsigned long long)o.object_id, (unsigned)o.publisher_priority,
                    (unsigned long long)o.presentation_time_us, (int)o.payload.len, (const char *)o.payload.data);
            MOQ_TEST_CHECK(strcmp(name, w->track) == 0);
            MOQ_TEST_CHECK_EQ_U64(o.group_id, w->group);
            MOQ_TEST_CHECK(o.has_subgroup == w->has_subgroup);
            MOQ_TEST_CHECK(o.datagram == !w->has_subgroup);
            MOQ_TEST_CHECK_EQ_U64(o.subgroup_id, w->has_subgroup ? w->subgroup : 0);
            MOQ_TEST_CHECK_EQ_U64(o.object_id, w->object);
            MOQ_TEST_CHECK_EQ_INT((int)o.publisher_priority, (int)w->priority);
            MOQ_TEST_CHECK(payload_is(&o, w->payload));
            MOQ_TEST_CHECK(o.has_capture_time == false);
            MOQ_TEST_CHECK_EQ_U64(o.presentation_time_us, 0);   /* ids are wire identity, not the parsed timeline */
            moq_media_object_cleanup(&o);
            next++;
        }
        MOQ_TEST_CHECK_EQ_SIZE(next, (size_t)K_OBJECTS);
        {
            /* Drained. A receiver without an endpoint reads as closed once empty, so
             * the answer is CLOSED rather than DONE; nothing more is delivered. */
            moq_media_object_t o;
            MOQ_TEST_CHECK_EQ_INT((int)moq_media_receiver_poll_object(r, &o, sizeof o), (int)MOQ_ERR_CLOSED);
        }
        moq_media_receiver_stats_t st;
        memset(&st, 0, sizeof st);
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_receiver_get_stats(r, &st, sizeof st), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(st.objects_received, K_OBJECTS);
        MOQ_TEST_CHECK_EQ_U64(st.objects_dropped, 0);
        MOQ_TEST_CHECK_EQ_U64(st.parse_drops, 0);
    }

    /* Every object still queued is released by the production teardown. */
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    now += 1000;
    moq_media_receiver_test_destroy_with_session(r, client, now);
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    { moq_event_t ev;
      while (moq_session_poll_events(client, &ev, 1) == 1) moq_event_cleanup(&ev);
      while (moq_session_poll_events(server, &ev, 1) == 1) moq_event_cleanup(&ev); }
    moq_simpair_destroy(sp);
    if (failures == 0) MOQ_TEST_PASS("media_receiver_wire_identity");
    return failures ? 1 : 0;
}
