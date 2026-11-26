/*
 * The fixture of the receiver's LOCMAF tests (white-box: included after
 * "../src/media_receiver.c"). A sessionless receiver adopts a catalog with
 * one "locmaf" track whose CMAF Header has a trex, and its live stage is
 * started as the subscription's issue starts it. Objects are produced by a
 * real encoder from canonical source chunks, so a rebuilt Object must equal
 * its source byte for byte.
 */
#ifndef LOCMAF_RECEIVER_FIXTURE_H
#define LOCMAF_RECEIVER_FIXTURE_H

#include <moq/locmaf.h>
#include <moq/msf.h>
#include "media_builders.h"
#include "test_support.h"
#include "test_alloc_support.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define LMX_MAX_OBJ   256u
#define LMX_MAX_GROUP 16u
#define LMX_MAX_POLL  256u

/* One Object of a group, with the bytes its rebuild must give. */
typedef struct lmx_obj {
    uint8_t  wire[LMX_MAX_OBJ];
    size_t   wire_len;
    uint8_t  canon[LMX_MAX_OBJ];
    size_t   canon_len;
} lmx_obj_t;

/* What the application polled. */
typedef struct lmx_rec {
    uint64_t group, object;
    uint32_t packaging, origin;
    bool     status;
    bool     has_loss_report, gap;
    uint64_t gap_group, gap_object;
    size_t   payload_len;
    uint8_t  frag[LMX_MAX_OBJ];
    size_t   frag_len;
    bool     has_time;
    uint64_t time_us;
} lmx_rec_t;

typedef struct lmx {
    test_alloc_state_t   state;
    moq_alloc_t          alloc;
    moq_media_receiver_t *r;
    moq_media_track_t   *t;
    moq_locmaf_track_t   facts;
    moq_locmaf_encoder_t enc;
    void                *enc_mem;
    size_t               enc_size;
    uint64_t             pushed;       /* NORMAL live Objects taken in */
    lmx_rec_t            got[LMX_MAX_POLL];
    size_t               got_count;
} lmx_t;

static const char *lmx_row = "";

static void lmx_b64(const uint8_t *in, size_t n, char *out)
{
    static const char tab[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    size_t o = 0;
    for (size_t i = 0; i < n; i += 3) {
        uint32_t v = (uint32_t)in[i] << 16;
        if (i + 1 < n) v |= (uint32_t)in[i + 1] << 8;
        if (i + 2 < n) v |= in[i + 2];
        out[o++] = tab[(v >> 18) & 63];
        out[o++] = tab[(v >> 12) & 63];
        out[o++] = i + 1 < n ? tab[(v >> 6) & 63] : '=';
        out[o++] = i + 2 < n ? tab[v & 63] : '=';
    }
    out[o] = '\0';
}

/* The CMAF Header every fixture track carries: track_ID 1, timescale
 * 48000, trex sdi 1 / duration 1024 / size 0 / flags 0x02000000. */
static inline size_t lmx_init(uint8_t *out)
{
    static const uint32_t trex4[4] = { 1, 1024, 0, 0x02000000u };
    return moq_test_build_cmaf_init_trex(out, 1, 48000, trex4, 1, true);
}

/* A catalog of revision `version` ("draft-01" or "draft-02") with one
 * locmaf track; `extra` is spliced into the track object. */
static inline void lmx_catalog(char *out, size_t cap, const char *version,
                        const char *extra)
{
    uint8_t init[512];
    char b64[800];
    lmx_b64(init, lmx_init(init), b64);
    snprintf(out, cap,
             "{\"version\":\"%s\",\"initDataList\":[{\"id\":\"i\","
             "\"type\":\"inline\",\"data\":\"%s\"}],\"tracks\":[{\"name\":\"v\","
             "\"packaging\":\"locmaf\",\"isLive\":true,\"role\":\"video\","
             "\"codec\":\"avc1.64001f\",\"initRef\":\"i\"%s}]}",
             version, b64, extra);
}

static inline bool lmx_enc_open(lmx_t *f)
{
    moq_locmaf_encoder_cfg_t cfg;
    moq_locmaf_encoder_cfg_init(&cfg);
    cfg.track = &f->facts;
    if (moq_locmaf_encoder_storage_size(&cfg, &f->enc_size) != MOQ_OK) return false;
    f->enc_mem = malloc(f->enc_size);
    if (!f->enc_mem) return false;
    cfg.storage = f->enc_mem;
    cfg.storage_size = f->enc_size;
    return moq_locmaf_encoder_init(&f->enc, sizeof(f->enc), &cfg) == MOQ_OK;
}

/* A receiver on `v` with `companion`, its catalog adopted and the track's
 * live stage started; `fill` says whether the subscription asked for a
 * fill. False (and the fixture closed) on failure. */
static inline bool lmx_open_full(lmx_t *f, moq_version_t v,
                          moq_media_companion_profile_t companion,
                          const char *version, const char *extra, bool fill,
                          bool lane)
{
    memset(f, 0, sizeof(*f));
    f->alloc = test_allocator(&f->state);
    moq_media_receiver_cfg_t cfg;
    moq_media_receiver_cfg_init_live_sized(&cfg, sizeof(cfg));
    static moq_bytes_t ns[] = { { (const uint8_t *)"lm", 2 } };
    cfg.namespace_ = (moq_namespace_t){ ns, 1 };
    cfg.auto_subscribe = true;
    cfg.companion_profile = companion;
    cfg.overflow.policy = MOQ_MEDIA_OVERFLOW_DROP_GROUP;
    cfg.overflow.max_objects = 512;
    cfg.surface_stream_terminals = lane;
    moq_media_receiver_test_set_alloc(&f->alloc);
    f->r = moq_media_receiver_test_new_cfg(&cfg);
    moq_media_receiver_test_set_alloc(NULL);
    MOQ_TEST_CHECK(f->r != NULL);
    if (!f->r) return false;
    moq_media_receiver_test_latch_session_version(f->r, v);
    char json[4096];
    lmx_catalog(json, sizeof(json), version, extra);
    moq_media_receiver_test_ingest(f->r, 0, 0, json, strlen(json));
    moq_media_track_event_t ev;
    while (moq_media_receiver_poll_track(f->r, &ev, sizeof(ev)) == MOQ_OK)
        if (ev.kind == MOQ_MEDIA_TRACK_ADDED) f->t = ev.track;
    MOQ_TEST_CHECK(f->t != NULL);
    if (!f->t) return false;
    MOQ_TEST_CHECK(f->t->desc.info.packaging == MOQ_MEDIA_PACKAGING_LOCMAF);
    MOQ_TEST_CHECK(f->t->desc.cmaf != NULL);
    if (!f->t->has_media_path || !f->t->desc.cmaf) return false;
    MOQ_TEST_CHECK(moq_locmaf_track_from_cmaf(f->t->desc.cmaf, &f->facts,
                                              sizeof(f->facts)) == MOQ_OK);
    MOQ_TEST_CHECK(receiver_lm_start(f->r, f->t));
    receiver_lm_note_fill(f->t, fill);
    return lmx_enc_open(f);
}

static inline bool lmx_open(lmx_t *f, moq_version_t v)
{
    moq_media_companion_profile_t c = v == MOQ_VERSION_DRAFT_22
        ? MOQ_MEDIA_COMPANION_D22_MSF01_LOC05_PUBLIC
        : MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01;
    return lmx_open_full(f, v, c, "draft-01", ",\"locmafVersion\":\"0.3\"", false,
                         false);
}

/* Release a polled Object. Its sample table was allocated by the
 * receiver's allocator, which here is the test's: it goes back there, so
 * the balance stays exact. */
static inline void lmx_cleanup(lmx_t *f, moq_media_object_t *o)
{
    if (o->samples_owned) {
        f->alloc.free(o->samples_owned, o->sample_count * sizeof(moq_cmaf_sample_t),
                      f->alloc.ctx);
        o->samples_owned = NULL;
    }
    moq_media_object_cleanup(o);
}

static inline void lmx_close(lmx_t *f)
{
    if (f->r) {
        moq_media_object_t o;
        while (moq_media_receiver_poll_object(f->r, &o, sizeof(o)) == MOQ_OK)
            lmx_cleanup(f, &o);
        moq_media_track_event_t ev;
        while (moq_media_receiver_poll_track(f->r, &ev, sizeof(ev)) == MOQ_OK) {}
        moq_media_receiver_test_free(f->r);
        f->r = NULL;
    }
    free(f->enc_mem);
    f->enc_mem = NULL;
    MOQ_TEST_CHECK(f->state.balance == 0);
}

/* The canonical source chunk of (g, o): one sample, a size and flags that
 * vary with o, and the decode time of a continuous group. */
static inline size_t lmx_source(const lmx_t *f, uint64_t g, uint64_t o, uint8_t *out,
                         size_t cap)
{
    uint8_t payload[16];
    uint32_t size = 4u + (uint32_t)(o % 5u);
    for (uint32_t i = 0; i < size; i++) payload[i] = (uint8_t)(g * 31u + o * 7u + i);
    moq_locmaf_sample_t s;
    memset(&s, 0, sizeof(s));
    s.duration = 1024;
    s.size = size;
    s.flags = o == 0 ? 0x02000000u : 0x01010000u;
    moq_locmaf_chunk_t c;
    memset(&c, 0, sizeof(c));
    c.struct_size = sizeof(c);
    c.kind = MOQ_LOCMAF_OBJECT_FULL;
    c.base_media_decode_time = (g * 1000u + o) * 1024u;
    c.sample_description_index = 1;
    c.sample_count = 1;
    c.samples = &s;
    c.mdat_payload = (moq_bytes_t){ payload, size };
    size_t len = 0;
    MOQ_TEST_CHECK(moq_locmaf_write_cmaf(&f->facts, &c, out, cap, &len) == MOQ_OK);
    return len;
}

enum { LMX_AUTO = 0, LMX_FULL, LMX_RAW, LMX_BAD };

/* Encode (g, o) as `kind`: AUTO lets the encoder choose (a full header at
 * a new group or after a reset, else a delta), FULL re-anchors, RAW is a
 * rawBoxes Object of the canonical chunk, BAD a malformed Object. */
static inline void lmx_make(lmx_t *f, uint64_t g, uint64_t o, int kind, lmx_obj_t *out)
{
    memset(out, 0, sizeof(*out));
    out->canon_len = lmx_source(f, g, o, out->canon, sizeof(out->canon));
    moq_bytes_t src = { out->canon, out->canon_len };
    moq_result_t rc;
    if (kind == LMX_BAD) {
        static const uint8_t bad[] = { 0x05, 0x00 };   /* an unknown element */
        memcpy(out->wire, bad, sizeof(bad));
        out->wire_len = sizeof(bad);
        return;
    }
    if (kind == LMX_FULL) moq_locmaf_encoder_reset(&f->enc);
    if (kind == LMX_RAW)
        rc = moq_locmaf_encode_raw_boxes(&f->enc, g, o, src, out->wire,
                                         sizeof(out->wire), &out->wire_len, NULL);
    else
        rc = moq_locmaf_encode_chunk(&f->enc, g, o, src, out->wire,
                                     sizeof(out->wire), &out->wire_len, NULL);
    MOQ_TEST_CHECK(rc == MOQ_OK);
    moq_locmaf_encoder_commit(&f->enc);
}

/* A whole group's Objects 0..n-1, each encoded after its predecessor. */
static inline void lmx_group(lmx_t *f, uint64_t g, size_t n, lmx_obj_t *objs)
{
    for (size_t o = 0; o < n; o++) lmx_make(f, g, o, LMX_AUTO, &objs[o]);
}

static inline void lmx_push_ex(lmx_t *f, uint64_t g, uint64_t sub, uint64_t o,
                        const lmx_obj_t *obj, bool datagram,
                        const uint8_t *props, size_t plen)
{
    f->pushed++;
    moq_media_receiver_test_push_object_ex(f->r, f->t, g, sub, o, 0x80,
        MOQ_OBJECT_NORMAL, false, datagram, obj->wire, obj->wire_len, props, plen);
}

/* (g, o) on the group's subgroup g. */
static inline void lmx_push(lmx_t *f, uint64_t g, uint64_t o, const lmx_obj_t *obj)
{
    lmx_push_ex(f, g, g, o, obj, false, NULL, 0);
}

static inline void lmx_status(lmx_t *f, uint64_t g, uint64_t o, moq_object_status_t st)
{
    moq_media_receiver_test_push_object_ex(f->r, f->t, g, g, o, 0x80, st, false,
                                           false, NULL, 0, NULL, 0);
}

static inline void lmx_terminal(lmx_t *f, uint32_t kind, uint64_t g, uint64_t sub)
{
    moq_sub_stream_terminal_t st;
    memset(&st, 0, sizeof(st));
    st.struct_size = sizeof(st);
    st.kind = kind;
    st.track = f->t->sub_track;
    st.group_id = g;
    st.subgroup_id = sub;
    receiver_lm_terminal_now(f->r, f->t, &st);
}

/* Poll everything queued into f->got (appending). */
static inline void lmx_poll(lmx_t *f)
{
    moq_media_object_t o;
    while (moq_media_receiver_poll_object(f->r, &o, sizeof(o)) == MOQ_OK) {
        if (f->got_count < LMX_MAX_POLL) {
            lmx_rec_t *g = &f->got[f->got_count++];
            memset(g, 0, sizeof(*g));
            g->group = o.group_id;
            g->object = o.object_id;
            g->packaging = o.packaging;
            g->origin = o.delivery_origin;
            g->status = o.status != MOQ_OBJECT_NORMAL;
            g->has_loss_report = o.has_loss_report;
            g->gap = o.gap;
            g->gap_group = o.gap_group;
            g->gap_object = o.gap_object;
            g->payload_len = o.payload.len;
            g->frag_len = o.fragment.len < LMX_MAX_OBJ ? o.fragment.len : LMX_MAX_OBJ;
            if (o.fragment.len) memcpy(g->frag, o.fragment.data, g->frag_len);
            g->has_time = o.metadata.has_time_us;
            g->time_us = o.metadata.time_us;
        }
        lmx_cleanup(f, &o);
    }
}

static inline moq_media_receiver_stats_t lmx_stats(lmx_t *f)
{
    moq_media_receiver_stats_t st;
    memset(&st, 0, sizeof(st));
    MOQ_TEST_CHECK(moq_media_receiver_get_stats(f->r, &st, sizeof(st)) == MOQ_OK);
    return st;
}

/* Every live Object taken in is counted exactly once. */
static inline bool lmx_balanced(lmx_t *f, uint64_t fill_counted)
{
    moq_media_receiver_stats_t st = lmx_stats(f);
    uint64_t sum = st.locmaf_objects_rebuilt + st.locmaf_objects_undecodable +
                   st.locmaf_objects_expired + st.locmaf_objects_late;
    if (sum != f->pushed + fill_counted)
        fprintf(stderr, "  %s: counted %llu (rebuilt %llu, undecodable %llu, "
                "expired %llu, late %llu), taken in %llu\n", lmx_row,
                (unsigned long long)sum,
                (unsigned long long)st.locmaf_objects_rebuilt,
                (unsigned long long)st.locmaf_objects_undecodable,
                (unsigned long long)st.locmaf_objects_expired,
                (unsigned long long)st.locmaf_objects_late,
                (unsigned long long)(f->pushed + fill_counted));
    return sum == f->pushed + fill_counted;
}

/* The polled record of (g, o), or NULL. */
static inline const lmx_rec_t *lmx_find(const lmx_t *f, uint64_t g, uint64_t o)
{
    for (size_t i = 0; i < f->got_count; i++)
        if (!f->got[i].status && f->got[i].group == g && f->got[i].object == o)
            return &f->got[i];
    return NULL;
}

/* (g, o) was delivered as its canonical chunk. */
static inline bool lmx_rebuilt(const lmx_t *f, uint64_t g, uint64_t o, const lmx_obj_t *obj)
{
    const lmx_rec_t *r = lmx_find(f, g, o);
    if (!r) {
        fprintf(stderr, "  %s: (%llu, %llu) not delivered\n", lmx_row,
                (unsigned long long)g, (unsigned long long)o);
        return false;
    }
    bool ok = r->packaging == MOQ_MEDIA_PACKAGING_CMAF && r->payload_len == 0 &&
              r->frag_len == obj->canon_len &&
              memcmp(r->frag, obj->canon, obj->canon_len) == 0;
    if (!ok)
        fprintf(stderr, "  %s: (%llu, %llu) packaging %u payload %zu frag %zu/%zu\n",
                lmx_row, (unsigned long long)g, (unsigned long long)o,
                (unsigned)r->packaging, r->payload_len, r->frag_len, obj->canon_len);
    return ok;
}

/* Objects of group g were delivered in Object order. */
static inline bool lmx_in_order(const lmx_t *f, uint64_t g)
{
    bool have = false;
    uint64_t last = 0;
    for (size_t i = 0; i < f->got_count; i++) {
        if (f->got[i].status || f->got[i].group != g) continue;
        if (have && f->got[i].object <= last) return false;
        have = true;
        last = f->got[i].object;
    }
    return true;
}

static inline size_t lmx_count(const lmx_t *f, uint64_t g)
{
    size_t n = 0;
    for (size_t i = 0; i < f->got_count; i++)
        if (!f->got[i].status && f->got[i].group == g) n++;
    return n;
}

#endif /* LOCMAF_RECEIVER_FIXTURE_H */
