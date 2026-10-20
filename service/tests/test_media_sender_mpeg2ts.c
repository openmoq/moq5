/*
 * The media sender's MPEG-2 TS packaging (draft-gregoire-moq-msfts), driven
 * white-box with no network: add_track's TS rules and the catalog entry it
 * authors, the track cfg size contract, write_mpeg2ts (consumption, chunking,
 * sync errors, held Objects, end_object, the one-writer rule, per-program
 * filtering, continuation discard), app-Object mode, end_track, and the
 * per-track stats. The send queue is read through a test seam; nothing
 * leaves the process.
 *
 * In a build without moq::mpeg2ts the TS entry points are checked to return
 * MOQ_ERR_UNSUPPORTED and nothing else runs.
 */
#include <moq/endpoint.h>
#include <moq/media_sender.h>
#include <moq/msf.h>
#include <moq/rcbuf.h>
#ifdef MOQ_TEST_HAVE_SIM
#include <moq/sim.h>
#endif
#include "test_support.h"
#include "ts_stream.h"

#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int failures = 0;

/* Test seams (media_sender.c, MOQ_MEDIA_SENDER_TESTING). */
moq_media_sender_t *moq_media_sender_test_new_cfg(
    const moq_media_sender_cfg_t *cfg);
void moq_media_sender_test_free(moq_media_sender_t *s);
moq_result_t moq_media_sender_test_build_catalog(moq_media_sender_t *s,
                                                 moq_rcbuf_t **out);
bool moq_media_sender_test_queue_entry(moq_media_sender_t *s, size_t i,
                                       const moq_media_track_t **track,
                                       const moq_rcbuf_t **payload,
                                       uint64_t *group, bool *starts_group);
bool moq_media_sender_test_emit_head(moq_media_sender_t *s,
                                     moq_rcbuf_t **payload);
size_t moq_media_sender_test_track_count(moq_media_sender_t *s);
void moq_media_sender_test_set_wait_entry_hook(void (*fn)(void *), void *ctx);
void moq_media_sender_test_set_alloc(const moq_alloc_t *alloc);
void moq_media_sender_test_set_ep(moq_media_sender_t *s, moq_endpoint_t *ep);
moq_endpoint_t *moq_endpoint_test_make_bare(void);
void moq_endpoint_test_free_bare(moq_endpoint_t *ep);
void moq_media_sender_test_drop_track_locked(moq_media_sender_t *s,
                                             moq_media_track_t *t);
void moq_media_sender_test_set_ready(moq_media_sender_t *s);
void moq_media_sender_test_mark_registered(moq_media_sender_t *s);
void moq_media_sender_test_mark_published(moq_media_sender_t *s);
void moq_media_sender_test_set_clock_ms(moq_media_sender_t *s,
                                        bool set, uint64_t ms);
size_t moq_media_sender_test_stage(moq_media_sender_t *s,
                                   moq_rcbuf_t **objs, size_t cap);
void moq_media_sender_test_pump(moq_media_sender_t *s, moq_session_t *session,
                                uint64_t now_us);
void moq_media_sender_test_set_ts_yield_hook(void (*fn)(void *), void *ctx);


static moq_bytes_t g_ns_parts[2] = { { (const uint8_t *)"svc", 3 },
                                     { (const uint8_t *)"ts", 2 } };

static moq_media_sender_t *make_sender(moq_media_send_backpressure_t bp,
                                       uint32_t pre_ready_objects)
{
    moq_media_sender_cfg_t cfg;
    moq_media_sender_cfg_init_live_sized(&cfg, sizeof(cfg));
    cfg.namespace_ = (moq_namespace_t){ g_ns_parts, 2 };
    cfg.backpressure = bp;
    cfg.block_timeout_us = 200000;
    cfg.pre_ready_max_objects = pre_ready_objects;
    /* No session is ever latched here: pin the legacy companion, whose
     * Group IDs count from 0. */
    cfg.companion_profile = MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01;
    return moq_media_sender_test_new_cfg(&cfg);
}

static void ts_rec(moq_msf_mpeg2ts_t *r, moq_msf_mpeg2ts_mode_t mode,
                   uint32_t packet_size)
{
    moq_msf_mpeg2ts_init(r);
    r->has_mode = true;
    r->mode = mode;
    r->has_packet_size = true;
    r->packet_size = packet_size;
}

typedef struct ts_track_opts {
    const char              *name;
    moq_media_packaging_t    packaging;
    const moq_msf_track_ext_t *ext;
    size_t                   ext_count;
    bool                     app_objects;
    uint32_t                 object_packets;
    bool                     forward_before_ra;
    moq_bytes_t              init;
    bool                     sap_timeline;
    bool                     media_timeline;
    moq_media_type_t         media_type;
} ts_track_opts_t;

static moq_result_t add_track(moq_media_sender_t *s, const ts_track_opts_t *o,
                              moq_media_track_t **out)
{
    moq_media_track_cfg_t tc;
    moq_media_track_cfg_init_sized(&tc, sizeof(tc));
    tc.name = (moq_bytes_t){ (const uint8_t *)o->name, strlen(o->name) };
    tc.media_type = o->media_type ? o->media_type : MOQ_MEDIA_TYPE_VIDEO;
    tc.packaging = o->packaging ? o->packaging : MOQ_MEDIA_PACKAGING_MPEG2TS;
    tc.bitrate = 3000000;
    tc.init_data = o->init;
    tc.extensions = o->ext;
    tc.extension_count = o->ext_count;
    tc.mpeg2ts_app_objects = o->app_objects;
    tc.mpeg2ts_object_packets = o->object_packets;
    tc.mpeg2ts_forward_before_random_access = o->forward_before_ra;
    tc.emit_sap_timeline = o->sap_timeline;
    tc.emit_media_timeline = o->media_timeline;
    if (tc.packaging != MOQ_MEDIA_PACKAGING_MPEG2TS)
        tc.codec = (moq_bytes_t){ (const uint8_t *)"avc1.64001f", 11 };
    *out = NULL;
    return moq_media_sender_add_track(s, &tc, out);
}


#ifdef MOQ_TEST_HAVE_MPEG2TS

static size_t queue_len(moq_media_sender_t *s)
{
    size_t n = 0;
    while (moq_media_sender_test_queue_entry(s, n, NULL, NULL, NULL, NULL)) n++;
    return n;
}

/* -- add_track: the TS rules and the extension list ---------------------- */

static void test_add_track_rules(void)
{
    int before = failures;
    moq_media_sender_t *s = make_sender(MOQ_MEDIA_SEND_BP_RETURN_WOULD_BLOCK, 0);
    MOQ_TEST_CHECK(s != NULL);
    if (!s) return;
    moq_msf_mpeg2ts_t rec;
    ts_rec(&rec, MOQ_MSF_MPEG2TS_MODE_UNMODIFIED_PROGRAM, 188);
    moq_msf_track_ext_t ext = { .kind = MOQ_MSF_EXT_MPEG2TS,
                                .u = { .mpeg2ts = &rec } };
    moq_media_track_t *t = NULL;
    ts_track_opts_t o;

    /* An MPEG2TS track needs its record; a record needs an MPEG2TS track. */
    memset(&o, 0, sizeof(o)); o.name = "a";
    MOQ_TEST_CHECK_EQ_INT((int)add_track(s, &o, &t), (int)MOQ_ERR_INVAL);
    memset(&o, 0, sizeof(o)); o.name = "a";
    o.packaging = MOQ_MEDIA_PACKAGING_CMAF; o.ext = &ext; o.ext_count = 1;
    MOQ_TEST_CHECK_EQ_INT((int)add_track(s, &o, &t), (int)MOQ_ERR_INVAL);
    /* A count without an array; NONE and unknown kinds; a kind twice; a NULL
     * record; a record below its first published size. */
    memset(&o, 0, sizeof(o)); o.name = "a"; o.ext_count = 1;
    MOQ_TEST_CHECK_EQ_INT((int)add_track(s, &o, &t), (int)MOQ_ERR_INVAL);
    moq_msf_track_ext_t bad[2] = { ext, ext };
    bad[0].kind = MOQ_MSF_EXT_NONE;
    memset(&o, 0, sizeof(o)); o.name = "a"; o.ext = bad; o.ext_count = 1;
    MOQ_TEST_CHECK_EQ_INT((int)add_track(s, &o, &t), (int)MOQ_ERR_INVAL);
    bad[0].kind = (moq_msf_ext_kind_t)7;
    MOQ_TEST_CHECK_EQ_INT((int)add_track(s, &o, &t), (int)MOQ_ERR_INVAL);
    bad[0] = ext;
    o.ext_count = 2;
    MOQ_TEST_CHECK_EQ_INT((int)add_track(s, &o, &t), (int)MOQ_ERR_INVAL);
    bad[0].u.mpeg2ts = NULL; o.ext_count = 1;
    MOQ_TEST_CHECK_EQ_INT((int)add_track(s, &o, &t), (int)MOQ_ERR_INVAL);
    moq_msf_mpeg2ts_t small = rec;
    small.struct_size = 8;
    bad[0].u.mpeg2ts = &small;
    MOQ_TEST_CHECK_EQ_INT((int)add_track(s, &o, &t), (int)MOQ_ERR_INVAL);

    /* Records the subscriber's check or the service refuses. */
    moq_msf_mpeg2ts_t r2;
    moq_msf_track_ext_t e2 = { .kind = MOQ_MSF_EXT_MPEG2TS, .u = { .mpeg2ts = &r2 } };
    memset(&o, 0, sizeof(o)); o.name = "a"; o.ext = &e2; o.ext_count = 1;
    ts_rec(&r2, MOQ_MSF_MPEG2TS_MODE_UNMODIFIED_PROGRAM, 204);
    MOQ_TEST_CHECK_EQ_INT((int)add_track(s, &o, &t), (int)MOQ_ERR_INVAL);
    ts_rec(&r2, MOQ_MSF_MPEG2TS_MODE_ES_PACKETS, 188);
    r2.has_es_pid = true; r2.es_pid = 0x101;
    MOQ_TEST_CHECK_EQ_INT((int)add_track(s, &o, &t), (int)MOQ_ERR_INVAL);
    ts_rec(&r2, MOQ_MSF_MPEG2TS_MODE_PER_PROGRAM, 188);   /* no program */
    MOQ_TEST_CHECK_EQ_INT((int)add_track(s, &o, &t), (int)MOQ_ERR_INVAL);
    moq_msf_mpeg2ts_init(&r2);                            /* no mode */
    r2.has_packet_size = true; r2.packet_size = 188;
    MOQ_TEST_CHECK_EQ_INT((int)add_track(s, &o, &t), (int)MOQ_ERR_INVAL);
    /* mpeg2tsRandomAccess is the service's to declare in byte-stream mode. */
    ts_rec(&r2, MOQ_MSF_MPEG2TS_MODE_UNMODIFIED_PROGRAM, 188);
    r2.has_random_access = true; r2.random_access = false;
    MOQ_TEST_CHECK_EQ_INT((int)add_track(s, &o, &t), (int)MOQ_ERR_INVAL);
    r2.random_access = true; o.forward_before_ra = true;
    MOQ_TEST_CHECK_EQ_INT((int)add_track(s, &o, &t), (int)MOQ_ERR_INVAL);
    o.forward_before_ra = false;
    /* Generated timelines need per-Object times TS keeps in the packets. */
    ts_rec(&r2, MOQ_MSF_MPEG2TS_MODE_UNMODIFIED_PROGRAM, 188);
    o.sap_timeline = true;
    MOQ_TEST_CHECK_EQ_INT((int)add_track(s, &o, &t), (int)MOQ_ERR_INVAL);
    o.sap_timeline = false; o.media_timeline = true;
    MOQ_TEST_CHECK_EQ_INT((int)add_track(s, &o, &t), (int)MOQ_ERR_INVAL);
    o.media_timeline = false;
    /* Initialization data must be whole source packets. */
    uint8_t junk[187];
    memset(junk, 0x47, sizeof(junk));
    o.init = (moq_bytes_t){ junk, sizeof(junk) };
    MOQ_TEST_CHECK_EQ_INT((int)add_track(s, &o, &t), (int)MOQ_ERR_INVAL);
    o.init = (moq_bytes_t){ NULL, 0 };
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_test_track_count(s), 0);

    /* No codec and, for audio, no samplerate/channel_config: a multiplex
     * has no inherent codec. */
    o.media_type = MOQ_MEDIA_TYPE_AUDIO;
    MOQ_TEST_CHECK_EQ_INT((int)add_track(s, &o, &t), (int)MOQ_OK);
    MOQ_TEST_CHECK(t != NULL);
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_test_track_count(s), 1);

    moq_media_sender_test_free(s);
    if (failures == before) printf("PASS: ts_add_track_rules\n");
}

/* -- The catalog entry, and the deep copy behind it ---------------------- */

static const moq_msf_track_t *find_track(const moq_msf_catalog_t *c,
                                         const char *name)
{
    for (size_t i = 0; i < c->track_count; i++)
        if (c->tracks[i].name.len == strlen(name) &&
            memcmp(c->tracks[i].name.data, name, strlen(name)) == 0)
            return &c->tracks[i];
    return NULL;
}

static void test_catalog_entry(void)
{
    int before = failures;
    moq_media_sender_t *s = make_sender(MOQ_MEDIA_SEND_BP_RETURN_WOULD_BLOCK, 0);
    if (!s) { failures++; return; }

    ts_gen_t g;
    ts_gen_init(&g, 188, 1, false);
    uint8_t psi[188 * 2];
    size_t psi_len = ts_gen_psi(&g, psi);

    /* The caller's record, SI PIDs and names live in heap buffers that are
     * scribbled and freed right after add_track. */
    moq_msf_mpeg2ts_t *rec = malloc(sizeof(*rec));
    uint32_t *pids = malloc(2 * sizeof(uint32_t));
    char *mode_name = malloc(32);
    ts_rec(rec, MOQ_MSF_MPEG2TS_MODE_UNMODIFIED_PROGRAM, 188);
    rec->has_program_number = true; rec->program_number = 1;
    rec->has_pcr_pid = true; rec->pcr_pid = TS_VIDEO_PID;
    rec->has_mux_rate = true; rec->mux_rate = 4000000;
    pids[0] = 0x11; pids[1] = 0x12;
    rec->has_si_pids = true; rec->si_pids = pids; rec->si_pid_count = 2;
    memcpy(mode_name, "unmodified-program", 18);
    rec->mode_name = (moq_bytes_t){ (const uint8_t *)mode_name, 18 };
    moq_msf_track_ext_t *ext = malloc(sizeof(*ext));
    ext->kind = MOQ_MSF_EXT_MPEG2TS;
    ext->u.mpeg2ts = rec;

    ts_track_opts_t o;
    memset(&o, 0, sizeof(o));
    o.name = "prog"; o.ext = ext; o.ext_count = 1;
    o.init = (moq_bytes_t){ psi, psi_len };
    moq_media_track_t *t = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)add_track(s, &o, &t), (int)MOQ_OK);

    /* Forwarding before the first random access point: not declared. */
    moq_msf_mpeg2ts_t rec2;
    ts_rec(&rec2, MOQ_MSF_MPEG2TS_MODE_UNMODIFIED_PROGRAM, 188);
    moq_msf_track_ext_t ext2 = { .kind = MOQ_MSF_EXT_MPEG2TS,
                                 .u = { .mpeg2ts = &rec2 } };
    memset(&o, 0, sizeof(o));
    o.name = "fwd"; o.ext = &ext2; o.ext_count = 1; o.forward_before_ra = true;
    MOQ_TEST_CHECK_EQ_INT((int)add_track(s, &o, &t), (int)MOQ_OK);
    /* An unmodified multiplex without a reference program names no stream
     * to declare it on: absent. */
    moq_msf_mpeg2ts_t rec3;
    ts_rec(&rec3, MOQ_MSF_MPEG2TS_MODE_UNMODIFIED_MULTIPLEX, 188);
    moq_msf_track_ext_t ext3 = { .kind = MOQ_MSF_EXT_MPEG2TS,
                                 .u = { .mpeg2ts = &rec3 } };
    memset(&o, 0, sizeof(o));
    o.name = "mux"; o.ext = &ext3; o.ext_count = 1;
    MOQ_TEST_CHECK_EQ_INT((int)add_track(s, &o, &t), (int)MOQ_OK);

    memset(rec, 0xA5, sizeof(*rec));
    memset(pids, 0xA5, 2 * sizeof(uint32_t));
    memset(mode_name, 'X', 32);
    memset(ext, 0xA5, sizeof(*ext));
    free(rec); free(pids); free(mode_name); free(ext);

    moq_rcbuf_t *json = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_test_build_catalog(s, &json),
                          (int)MOQ_OK);
    moq_msf_catalog_t cat;
    memset(&cat, 0, sizeof(cat));
    cat.struct_size = sizeof(cat);
    moq_result_t prc = json ? moq_msf_catalog_parse(
        moq_alloc_default(),
        (moq_bytes_t){ moq_rcbuf_data(json), moq_rcbuf_len(json) }, &cat)
        : MOQ_ERR_INVAL;
    MOQ_TEST_CHECK_EQ_INT((int)prc, (int)MOQ_OK);
    if (prc == MOQ_OK) {
        const moq_msf_track_t *pt = find_track(&cat, "prog");
        MOQ_TEST_CHECK(pt != NULL);
        if (pt) {
            const moq_msf_mpeg2ts_t *m = moq_msf_track_mpeg2ts(pt);
            MOQ_TEST_CHECK(pt->packaging.len == 7 &&
                           memcmp(pt->packaging.data, "mpeg2ts", 7) == 0);
            MOQ_TEST_CHECK(pt->has_mime_type && pt->mime_type.len == 10 &&
                           memcmp(pt->mime_type.data, "video/mp2t", 10) == 0);
            MOQ_TEST_CHECK(!pt->has_codec);
            MOQ_TEST_CHECK(!pt->has_timescale);
            MOQ_TEST_CHECK(m != NULL);
            if (m) {
                MOQ_TEST_CHECK(m->has_mode &&
                               m->mode == MOQ_MSF_MPEG2TS_MODE_UNMODIFIED_PROGRAM);
                MOQ_TEST_CHECK(m->has_packet_size && m->packet_size == 188);
                MOQ_TEST_CHECK(m->has_program_number && m->program_number == 1);
                MOQ_TEST_CHECK(m->has_pcr_pid && m->pcr_pid == TS_VIDEO_PID);
                MOQ_TEST_CHECK(m->has_mux_rate && m->mux_rate == 4000000);
                MOQ_TEST_CHECK(m->has_si_pids && m->si_pid_count == 2 &&
                               m->si_pids[0] == 0x11 && m->si_pids[1] == 0x12);
                MOQ_TEST_CHECK(m->has_random_access && m->random_access);
            }
            MOQ_TEST_CHECK(moq_msf_track_mpeg2ts_check(pt, NULL) == MOQ_OK);
            /* The PSI travels as an "inline" initDataList entry. */
            moq_rcbuf_t *pk = NULL;
            MOQ_TEST_CHECK_EQ_INT((int)moq_msf_track_mpeg2ts_init_data(
                                      moq_alloc_default(), &cat, pt, &pk, NULL),
                                  (int)MOQ_OK);
            MOQ_TEST_CHECK(pk && moq_rcbuf_len(pk) == psi_len &&
                           memcmp(moq_rcbuf_data(pk), psi, psi_len) == 0);
            if (pk) moq_rcbuf_decref(pk);
        }
        const moq_msf_track_t *ft = find_track(&cat, "fwd");
        const moq_msf_mpeg2ts_t *fm = ft ? moq_msf_track_mpeg2ts(ft) : NULL;
        MOQ_TEST_CHECK(fm && fm->has_random_access && !fm->random_access);
        const moq_msf_track_t *mt = find_track(&cat, "mux");
        const moq_msf_mpeg2ts_t *mm = mt ? moq_msf_track_mpeg2ts(mt) : NULL;
        MOQ_TEST_CHECK(mm && !mm->has_random_access);
        moq_msf_catalog_cleanup(moq_alloc_default(), &cat);
    }
    if (json) moq_rcbuf_decref(json);
    moq_media_sender_test_free(s);
    if (failures == before) printf("PASS: ts_catalog_entry\n");
}

/* -- Late TS initialization must keep the independent catalog shape -------
 * An independent catalog carries a TS track's PSI in the root initDataList,
 * referenced by initRef. A deltaUpdate ADD cannot carry that shape, so a
 * generation adding an init-bearing TS track is a full independent catalog;
 * otherwise a later independent catalog would change the track's
 * representation, which a receiver's immutable-tuple check rejects. Legacy
 * companion rows; the draft-22 companion (every init-bearing track by
 * reference) is a control. */

static moq_media_sender_t *make_d22_sender(void)
{
    moq_media_sender_cfg_t cfg;
    moq_media_sender_cfg_init_live_sized(&cfg, sizeof(cfg));
    cfg.namespace_ = (moq_namespace_t){ g_ns_parts, 2 };
    cfg.backpressure = MOQ_MEDIA_SEND_BP_RETURN_WOULD_BLOCK;
    cfg.companion_profile = MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC;
    return moq_media_sender_test_new_cfg(&cfg);
}

static void check_catalog_metadata(const moq_msf_track_t *t, int group,
                                    int alt, moq_bytes_t label)
{
    MOQ_TEST_CHECK(t != NULL);
    if (!t) return;
    MOQ_TEST_CHECK(t->has_render_group && t->render_group == group);
    MOQ_TEST_CHECK(t->has_alt_group && t->alt_group == alt);
    MOQ_TEST_CHECK(t->has_label && t->label.len == label.len);
    if (t->has_label && t->label.len == label.len && label.len)
        MOQ_TEST_CHECK(memcmp(t->label.data, label.data, label.len) == 0);
    const moq_msf_mpeg2ts_t *r = moq_msf_track_mpeg2ts(t);
    MOQ_TEST_CHECK(r && r->has_packet_size && r->packet_size == 188);
    MOQ_TEST_CHECK(moq_msf_track_mpeg2ts_check(t, NULL) == MOQ_OK);
}

static void test_catalog_metadata(void)
{
    int before = failures;
    moq_media_sender_t *s = make_sender(MOQ_MEDIA_SEND_BP_RETURN_WOULD_BLOCK, 0);
    MOQ_TEST_CHECK(s != NULL);
    if (!s) return;
    static const uint8_t expected[] = { 'B', 0, 0xf0, 0x90, 0x80, 0x80 };
    uint8_t borrowed[sizeof(expected)];
    memcpy(borrowed, expected, sizeof(expected));
    moq_msf_mpeg2ts_t rec;
    ts_rec(&rec, MOQ_MSF_MPEG2TS_MODE_UNMODIFIED_PROGRAM, 188);
    moq_msf_track_ext_t ext = {
        .kind = MOQ_MSF_EXT_MPEG2TS, .u = { .mpeg2ts = &rec }
    };
    moq_media_track_cfg_t tc;
    moq_media_track_cfg_init(&tc);
    tc.name = (moq_bytes_t){ (const uint8_t *)"base", 4 };
    tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
    tc.packaging = MOQ_MEDIA_PACKAGING_MPEG2TS;
    tc.bitrate = 3000000;
    tc.extensions = &ext;
    tc.extension_count = 1;
    tc.mpeg2ts_app_objects = true;
    tc.has_render_group = true;
    tc.render_group = -2;
    tc.has_alt_group = true;
    tc.alt_group = 9;
    tc.has_label = true;
    tc.label = (moq_bytes_t){ borrowed, sizeof(borrowed) };
    moq_media_track_t *track = NULL;
    moq_result_t rc = moq_media_sender_add_track(s, &tc, &track);
    MOQ_TEST_CHECK(rc == MOQ_OK);
    if (rc != MOQ_OK) { moq_media_sender_test_free(s); return; }
    memset(borrowed, 'X', sizeof(borrowed));
    moq_media_sender_test_set_ready(s);
    moq_media_sender_test_mark_registered(s);
    moq_media_sender_test_mark_published(s);
    tc.name = (moq_bytes_t){ (const uint8_t *)"late", 4 };
    tc.render_group = 0;
    tc.alt_group = 10;
    tc.label = (moq_bytes_t){ NULL, 0 };
    rc = moq_media_sender_add_track(s, &tc, &track);
    MOQ_TEST_CHECK(rc == MOQ_OK);
    memset(&rec, 0xa5, sizeof(rec));
    memset(&ext, 0xa5, sizeof(ext));
    moq_media_sender_test_mark_registered(s);
    moq_rcbuf_t *objs[3] = { NULL, NULL, NULL };
    size_t n = moq_media_sender_test_stage(s, objs, 3);
    MOQ_TEST_CHECK(n == 2 && objs[0] && objs[1]);
    const moq_alloc_t *al = moq_alloc_default();
    moq_msf_catalog_t base = {0}, delta = {0}, merged = {0};
    if (n == 2 && objs[0] && objs[1]) {
        moq_result_t br = moq_msf_catalog_parse(al,
            (moq_bytes_t){ moq_rcbuf_data(objs[0]), moq_rcbuf_len(objs[0]) }, &base);
        moq_result_t dr = moq_msf_catalog_parse(al,
            (moq_bytes_t){ moq_rcbuf_data(objs[1]), moq_rcbuf_len(objs[1]) }, &delta);
        MOQ_TEST_CHECK(br == MOQ_OK && dr == MOQ_OK);
        const moq_bytes_t label = { expected, sizeof(expected) };
        const moq_bytes_t empty = { NULL, 0 };
        if (br == MOQ_OK) {
            MOQ_TEST_CHECK(base.track_count == 1);
            check_catalog_metadata(find_track(&base, "base"), -2, 9, label);
        }
        if (dr == MOQ_OK) {
            MOQ_TEST_CHECK(delta.delta_update_count == 1);
            if (delta.delta_update_count == 1) {
                const moq_msf_delta_op_t *op = &delta.delta_update[0];
                MOQ_TEST_CHECK(op->op == MOQ_MSF_DELTA_OP_ADD && op->track_count == 1);
                if (op->track_count == 1)
                    check_catalog_metadata(&op->tracks[0], 0, 10, empty);
            }
        }
        if (br == MOQ_OK && dr == MOQ_OK) {
            rc = moq_msf_catalog_apply_delta(al, &base, &delta, &merged);
            MOQ_TEST_CHECK(rc == MOQ_OK);
            if (rc == MOQ_OK) {
                MOQ_TEST_CHECK(merged.track_count == 2);
                check_catalog_metadata(find_track(&merged, "base"), -2, 9, label);
                check_catalog_metadata(find_track(&merged, "late"), 0, 10, empty);
            }
        }
    }
    moq_msf_catalog_cleanup(al, &merged);
    moq_msf_catalog_cleanup(al, &delta);
    moq_msf_catalog_cleanup(al, &base);
    for (size_t i = 0; i < 3; i++) moq_rcbuf_decref(objs[i]);
    moq_media_sender_test_free(s);
    if (failures == before) printf("PASS: ts_catalog_metadata_base_add\n");
}

static void test_catalog_late_add(bool raw, bool with_init, bool d22)
{
    int before = failures;
    const bool full = !raw && with_init;
    const char *name = d22 ? "ts_late_add_init_full_d22"
                     : raw ? "ts_late_add_raw_legacy"
                           : with_init ? "ts_late_add_init_full"
                                       : "ts_late_add_no_init_delta";
    moq_media_sender_t *s = d22 ? make_d22_sender()
                                : make_sender(MOQ_MEDIA_SEND_BP_RETURN_WOULD_BLOCK, 0);
    MOQ_TEST_CHECK(s != NULL);
    if (!s) return;
    moq_rcbuf_t *objs[3] = { NULL, NULL, NULL };
    moq_rcbuf_t *fresh = NULL, *decoded = NULL;
    moq_msf_catalog_t cat = {0}, refresh = {0};
    cat.struct_size = sizeof(cat);
    refresh.struct_size = sizeof(refresh);
    const moq_alloc_t *al = moq_alloc_default();
    moq_media_track_t *track = NULL;
    moq_result_t rc;
    moq_media_sender_test_set_clock_ms(s, true, 1000);

    ts_track_opts_t o = { .name = "base", .packaging = MOQ_MEDIA_PACKAGING_RAW };
    rc = add_track(s, &o, &track);
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
    if (rc != MOQ_OK) goto cleanup;
    moq_media_sender_test_set_ready(s);
    moq_media_sender_test_mark_registered(s);
    moq_media_sender_test_mark_published(s);
    size_t n = moq_media_sender_test_stage(s, objs, 3);
    MOQ_TEST_CHECK_EQ_SIZE(n, 0); /* baseline really committed */
    if (n != 0) goto cleanup;

    ts_gen_t g;
    ts_gen_init(&g, 188, 1, false);
    uint8_t psi[188 * 2];
    size_t psi_len = ts_gen_psi(&g, psi);
    MOQ_TEST_CHECK_EQ_SIZE(psi_len, sizeof(psi));
    if (psi_len != sizeof(psi)) goto cleanup;
    const uint8_t raw_init[] = { 0x01, 0x42, 0xe0, 0x1e };
    moq_msf_mpeg2ts_t rec;
    ts_rec(&rec, MOQ_MSF_MPEG2TS_MODE_UNMODIFIED_PROGRAM, 188);
    moq_msf_track_ext_t ext = {
        .kind = MOQ_MSF_EXT_MPEG2TS, .u = { .mpeg2ts = &rec }
    };
    memset(&o, 0, sizeof(o));
    o.name = "late";
    if (raw) o.packaging = MOQ_MEDIA_PACKAGING_RAW;
    else { o.ext = &ext; o.ext_count = 1; }
    if (with_init)
        o.init = raw ? (moq_bytes_t){ raw_init, sizeof(raw_init) }
                     : (moq_bytes_t){ psi, psi_len };
    rc = add_track(s, &o, &track);
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
    if (rc != MOQ_OK) goto cleanup;
    moq_media_sender_test_mark_registered(s);
    moq_media_sender_test_set_clock_ms(s, true, 2000);
    n = moq_media_sender_test_stage(s, objs, 3);
    const size_t expected = full ? 1 : 2;
    if (n != expected) {
        fprintf(stderr, "FAIL: %s: expected %zu staged catalog objects, got %zu\n",
                name, expected, n);
        failures++;
        goto cleanup;
    }

    /* A full generation is object 0; the legacy controls are base + ADD. */
    moq_rcbuf_t *wire = objs[n - 1];
    MOQ_TEST_CHECK(wire != NULL);
    if (!wire) goto cleanup;
    rc = moq_msf_catalog_parse(al,
        (moq_bytes_t){ moq_rcbuf_data(wire), moq_rcbuf_len(wire) }, &cat);
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
    if (rc != MOQ_OK) goto cleanup;
    const moq_msf_track_t *late = NULL;
    if (full) {
        MOQ_TEST_CHECK(cat.has_generated_at && cat.generated_at == 2000);
        MOQ_TEST_CHECK(cat.delta_update == NULL);
        MOQ_TEST_CHECK_EQ_SIZE(cat.track_count, 2);
        MOQ_TEST_CHECK(find_track(&cat, "base") != NULL);
        late = find_track(&cat, "late");
    } else {
        MOQ_TEST_CHECK(!cat.has_generated_at);
        MOQ_TEST_CHECK(cat.delta_update != NULL && cat.delta_update_count == 1);
        if (!cat.delta_update || cat.delta_update_count != 1) goto cleanup;
        MOQ_TEST_CHECK(cat.delta_update[0].op == MOQ_MSF_DELTA_OP_ADD);
        MOQ_TEST_CHECK_EQ_SIZE(cat.delta_update[0].track_count, 1);
        if (cat.delta_update[0].track_count != 1) goto cleanup;
        late = &cat.delta_update[0].tracks[0];
        MOQ_TEST_CHECK(late->name.len == 4 &&
                       memcmp(late->name.data, "late", 4) == 0);
    }
    MOQ_TEST_CHECK(late != NULL);
    if (!late) goto cleanup;
    MOQ_TEST_CHECK(late->has_init_ref == full);
    MOQ_TEST_CHECK(late->has_init_data == (raw && with_init));
    MOQ_TEST_CHECK_EQ_SIZE(cat.init_data_count, full ? 1 : 0);
    if (!full) {
        if (raw) {
            rc = moq_msf_decode_init_data(al, late->init_data, &decoded);
            MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
            MOQ_TEST_CHECK(decoded && moq_rcbuf_len(decoded) == sizeof(raw_init) &&
                           memcmp(moq_rcbuf_data(decoded), raw_init,
                                  sizeof(raw_init)) == 0);
        } else {
            MOQ_TEST_CHECK(moq_msf_track_mpeg2ts_check(late, NULL) == MOQ_OK);
        }
        goto cleanup;
    }
    rc = moq_msf_track_mpeg2ts_init_data(al, &cat, late, &decoded, NULL);
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
    MOQ_TEST_CHECK(decoded && moq_rcbuf_len(decoded) == psi_len &&
                   memcmp(moq_rcbuf_data(decoded), psi, psi_len) == 0);
    if (decoded) { moq_rcbuf_decref(decoded); decoded = NULL; }

    /* Model commitment, then build the later independent refresh document.
     * This target has sender internals only, not a receiver ingest seam. */
    moq_media_sender_test_mark_published(s);
    moq_media_sender_test_set_clock_ms(s, true, 3000);
    rc = moq_media_sender_test_build_catalog(s, &fresh);
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
    if (rc != MOQ_OK || !fresh) goto cleanup;
    rc = moq_msf_catalog_parse(al,
        (moq_bytes_t){ moq_rcbuf_data(fresh), moq_rcbuf_len(fresh) }, &refresh);
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
    if (rc != MOQ_OK) goto cleanup;
    MOQ_TEST_CHECK(refresh.has_generated_at && refresh.generated_at == 3000);
    const moq_msf_track_t *again = find_track(&refresh, "late");
    MOQ_TEST_CHECK(refresh.delta_update == NULL && again != NULL);
    if (!again) goto cleanup;
    MOQ_TEST_CHECK(again->has_init_ref && !again->has_init_data);
    MOQ_TEST_CHECK(again->init_ref.len == late->init_ref.len &&
                   again->init_ref.len > 0 &&
                   memcmp(again->init_ref.data, late->init_ref.data,
                          again->init_ref.len) == 0);
    rc = moq_msf_track_mpeg2ts_init_data(al, &refresh, again, &decoded, NULL);
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
    MOQ_TEST_CHECK(decoded && moq_rcbuf_len(decoded) == psi_len &&
                   memcmp(moq_rcbuf_data(decoded), psi, psi_len) == 0);
cleanup:
    if (decoded) moq_rcbuf_decref(decoded);
    moq_msf_catalog_cleanup(al, &refresh);
    moq_msf_catalog_cleanup(al, &cat);
    if (fresh) moq_rcbuf_decref(fresh);
    for (size_t i = 0; i < 3; i++)
        if (objs[i]) moq_rcbuf_decref(objs[i]);
    moq_media_sender_test_free(s);
    if (failures == before) printf("PASS: %s\n", name);
}

/* -- The track cfg size contract ------------------------------------------ */

static moq_result_t write_ts(moq_media_sender_t *s, moq_media_track_t *t,
                             const uint8_t *d, size_t n, bool end_object,
                             size_t *consumed);

static void test_track_cfg_abi(void)
{
    int before = failures;
    /* Storage with room past the struct, so writes beyond a given size show. */
    union {
        moq_media_track_cfg_t c;
        uint8_t b[sizeof(moq_media_track_cfg_t) + 16];
    } u;
    const size_t live_end =
        offsetof(moq_media_track_cfg_t, is_live) + sizeof(bool);

    /* The exported one-argument symbol clears and stamps the layout through
     * alt_group rounded up to the struct's alignment -- the historical
     * allocation, 232 octets on LP64 -- and nothing past it. */
    const size_t al = _Alignof(moq_media_track_cfg_t);
    const size_t hist = (offsetof(moq_media_track_cfg_t, alt_group) +
                         sizeof(int) + al - 1) & ~(al - 1);
    if (sizeof(void *) == 8) MOQ_TEST_CHECK_EQ_U64(hist, 232);
    memset(&u, 0xA5, sizeof(u));
    (moq_media_track_cfg_init)(&u.c);
    MOQ_TEST_CHECK_EQ_U64(u.c.struct_size, hist);
    MOQ_TEST_CHECK(u.c.is_live && !u.c.has_alt_group && u.c.alt_group == 0 &&
                   !u.c.has_track_duration && !u.c.emit_media_timeline);
    bool canary = true;
    for (size_t i = hist; i < sizeof(u.b); i++)
        if (u.b[i] != 0xA5) canary = false;
    MOQ_TEST_CHECK(canary);
    /* Through a function pointer too. */
    void (*fp)(moq_media_track_cfg_t *) = moq_media_track_cfg_init;
    memset(&u, 0xA5, sizeof(u));
    fp(&u.c);
    MOQ_TEST_CHECK_EQ_U64(u.c.struct_size, hist);

    /* The macro passes the caller's sizeof: the whole struct. */
    memset(&u, 0xA5, sizeof(u));
    moq_media_track_cfg_init(&u.c);
    MOQ_TEST_CHECK_EQ_U64(u.c.struct_size, sizeof(u.c));
    MOQ_TEST_CHECK(u.c.is_live && u.c.extensions == NULL &&
                   u.c.extension_count == 0 && !u.c.mpeg2ts_app_objects &&
                   u.c.mpeg2ts_object_packets == 0);

    /* Every prefix: below struct_size nothing is written; otherwise
     * min(size, sizeof) is cleared and stamped, is_live set only when the
     * prefix covers it, and no byte past it is touched. */
    for (size_t n = 0; n <= sizeof(u.b); n++) {
        memset(&u, 0xA5, sizeof(u));
        moq_media_track_cfg_init_sized(&u.c, n);
        size_t w = n < sizeof(u.c) ? n : sizeof(u.c);
        bool ok = true;
        if (n < sizeof(uint32_t)) {
            for (size_t i = 0; i < sizeof(u.b); i++)
                if (u.b[i] != 0xA5) ok = false;
        } else {
            ok = u.c.struct_size == w;
            for (size_t i = sizeof(uint32_t); i < sizeof(u.b); i++) {
                uint8_t want = 0xA5;
                if (i < w)
                    want = (i == offsetof(moq_media_track_cfg_t, is_live) &&
                            w >= live_end) ? 1 : 0;
                if (u.b[i] != want) ok = false;
            }
        }
        if (!ok) {
            printf("FAIL: track cfg init_sized prefix %zu\n", n);
            failures++;
        }
    }

    /* The add_track floor: the layout through sap_timeline_history_groups. */
    MOQ_TEST_CHECK_EQ_U64(MOQ_MEDIA_TRACK_CFG_V0_SIZE,
                          offsetof(moq_media_track_cfg_t, has_track_duration));

    /* add_track reads an appended field only when struct_size covers all of
     * it. Each boundary is pinned from the field's own offset and size, so
     * the cases hold on every ABI: one octet short leaves the field out,
     * the exact end reads it. */
    moq_media_sender_t *s = make_sender(MOQ_MEDIA_SEND_BP_RETURN_WOULD_BLOCK, 0);
    if (!s) { failures++; return; }
    moq_msf_mpeg2ts_t rec;
    ts_rec(&rec, MOQ_MSF_MPEG2TS_MODE_UNMODIFIED_PROGRAM, 188);
    moq_msf_track_ext_t ext = { .kind = MOQ_MSF_EXT_MPEG2TS,
                                .u = { .mpeg2ts = &rec } };
#define TCFG_END(f) (offsetof(moq_media_track_cfg_t, f) + \
                     sizeof(((moq_media_track_cfg_t *)0)->f))
    moq_media_track_cfg_t tc;
    moq_media_track_t *t = NULL;
    static const char *const names[] = { "e0", "e1", "a0", "a1", "p0", "p1" };
    for (int i = 0; i < 6; i++) {
        moq_media_track_cfg_init_sized(&tc, sizeof(tc));
        tc.name = (moq_bytes_t){ (const uint8_t *)names[i], 2 };
        tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
        tc.packaging = MOQ_MEDIA_PACKAGING_MPEG2TS;
        tc.bitrate = 1000000;
        tc.extensions = &ext;
        tc.extension_count = 1;
        tc.mpeg2ts_app_objects = i < 4;
        tc.mpeg2ts_object_packets = 1;
        size_t end = i < 2 ? TCFG_END(extension_count)
                   : i < 4 ? TCFG_END(mpeg2ts_app_objects)
                           : TCFG_END(mpeg2ts_object_packets);
        tc.struct_size = (uint32_t)(i % 2 == 0 ? end - 1 : end);
        t = NULL;
        moq_result_t rc = moq_media_sender_add_track(s, &tc, &t);
        if (i == 0) {
            /* The extension pair cut short: no record, so the MPEG2TS track
             * is refused. */
            MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_ERR_INVAL);
            continue;
        }
        MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
        if (!t) continue;
        static uint8_t gop[16 * 188];
        ts_gen_t g;
        ts_gen_init(&g, 188, 1, false);
        size_t glen = ts_gen_gop(&g, gop, 2);   /* PAT PMT RAP v v A */
        size_t c = 0;
        moq_result_t wr = write_ts(s, t, gop, glen, false, &c);
        if (i == 1 || i == 2) {
            /* mpeg2ts_app_objects not read: byte-stream mode. */
            MOQ_TEST_CHECK_EQ_INT((int)wr, (int)MOQ_OK);
        } else if (i == 3) {
            /* mpeg2ts_app_objects read: app-Object mode refuses bytes. */
            MOQ_TEST_CHECK_EQ_INT((int)wr, (int)MOQ_ERR_WRONG_STATE);
        } else {
            /* Byte stream; mpeg2ts_object_packets 1 is read only at its
             * exact end, else the default (64) holds the whole GOP in one
             * unfinished Object. */
            MOQ_TEST_CHECK_EQ_INT((int)wr, (int)MOQ_OK);
            moq_media_sender_track_stats_t st;
            MOQ_TEST_CHECK(moq_media_sender_get_track_stats(s, t, &st,
                                                            sizeof(st)) ==
                           MOQ_OK);
            if (i == 4) MOQ_TEST_CHECK_EQ_U64(st.mpeg2ts_objects, 0);
            else        MOQ_TEST_CHECK(st.mpeg2ts_objects > 0);
        }
    }
#undef TCFG_END
    /* A v0-sized LOC cfg is still accepted; one octet less is not. */
    moq_media_track_cfg_t old;
    moq_media_track_cfg_init_sized(&old, MOQ_MEDIA_TRACK_CFG_V0_SIZE);
    old.name = (moq_bytes_t){ (const uint8_t *)"l", 1 };
    old.media_type = MOQ_MEDIA_TYPE_VIDEO;
    old.packaging = MOQ_MEDIA_PACKAGING_RAW;
    old.codec = (moq_bytes_t){ (const uint8_t *)"av01", 4 };
    old.bitrate = 1000000;
    old.struct_size = (uint32_t)(MOQ_MEDIA_TRACK_CFG_V0_SIZE - 1);
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_add_track(s, &old, &t),
                          (int)MOQ_ERR_INVAL);
    old.struct_size = (uint32_t)MOQ_MEDIA_TRACK_CFG_V0_SIZE;
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_add_track(s, &old, &t),
                          (int)MOQ_OK);
    moq_media_sender_test_free(s);
    if (failures == before) printf("PASS: ts_track_cfg_abi\n");
}

/* -- write_mpeg2ts --------------------------------------------------------- */

#define GOPS      3u
#define VIDEO_CONT 5u
#define STREAM_MAX (GOPS * (6u + VIDEO_CONT) * 192u)

/* A stream of GOPS GOPs; returns its length. */
static size_t make_stream(uint8_t *buf, uint32_t ps, bool two_programs)
{
    ts_gen_t g;
    ts_gen_init(&g, ps, 1, two_programs);
    size_t n = 0;
    for (unsigned i = 0; i < GOPS; i++) n += ts_gen_gop(&g, buf + n, VIDEO_CONT);
    return n;
}

static moq_media_track_t *add_bytes_track(moq_media_sender_t *s,
                                          moq_msf_mpeg2ts_t *rec,
                                          uint32_t object_packets)
{
    moq_msf_track_ext_t ext = { .kind = MOQ_MSF_EXT_MPEG2TS,
                                .u = { .mpeg2ts = rec } };
    ts_track_opts_t o;
    memset(&o, 0, sizeof(o));
    o.name = "ts"; o.ext = &ext; o.ext_count = 1;
    o.object_packets = object_packets;
    moq_media_track_t *t = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)add_track(s, &o, &t), (int)MOQ_OK);
    return t;
}

static moq_result_t write_ts(moq_media_sender_t *s, moq_media_track_t *t,
                             const uint8_t *d, size_t n, bool end_object,
                             size_t *consumed)
{
    moq_media_send_mpeg2ts_t in;
    memset(&in, 0, sizeof(in));
    in.struct_size = sizeof(in);
    in.data = (moq_bytes_t){ d, n };
    in.end_object = end_object;
    return moq_media_sender_write_mpeg2ts(s, t, &in, consumed);
}

/* The queued Objects, concatenated, with their Group starts. */
typedef struct obj_log {
    uint8_t  bytes[STREAM_MAX + 4 * 192];
    size_t   len;
    size_t   count;
    size_t   off[256];
    uint64_t group[256];
    bool     starts[256];
} obj_log_t;

static void log_queue(moq_media_sender_t *s, obj_log_t *lg)
{
    const moq_rcbuf_t *p = NULL;
    uint64_t g = 0;
    bool st = false;
    for (size_t i = 0; moq_media_sender_test_queue_entry(s, i, NULL, &p, &g, &st);
         i++) {
        if (!p) continue;                    /* the END_OF_TRACK marker */
        size_t n = moq_rcbuf_len(p);
        if (lg->len + n > sizeof(lg->bytes) || lg->count == 256) {
            failures++;
            return;
        }
        lg->off[lg->count] = lg->len;
        lg->group[lg->count] = g;
        lg->starts[lg->count] = st;
        lg->count++;
        memcpy(lg->bytes + lg->len, moq_rcbuf_data(p), n);
        lg->len += n;
    }
}

/* The same stream in one call and in one-byte calls: the same Objects. */
static void test_chunking(void)
{
    int before = failures;
    static uint8_t stream[STREAM_MAX];
    size_t len = make_stream(stream, 188, false);
    static obj_log_t whole, bytes;
    memset(&whole, 0, sizeof(whole));
    memset(&bytes, 0, sizeof(bytes));

    for (int pass = 0; pass < 2; pass++) {
        moq_media_sender_t *s = make_sender(MOQ_MEDIA_SEND_BP_RETURN_WOULD_BLOCK, 0);
        if (!s) { failures++; return; }
        moq_msf_mpeg2ts_t rec;
        ts_rec(&rec, MOQ_MSF_MPEG2TS_MODE_UNMODIFIED_PROGRAM, 188);
        moq_media_track_t *t = add_bytes_track(s, &rec, 4);
        size_t consumed = 0;
        if (pass == 0) {
            MOQ_TEST_CHECK_EQ_INT((int)write_ts(s, t, stream, len, false,
                                                &consumed), (int)MOQ_OK);
            MOQ_TEST_CHECK_EQ_U64(consumed, len);
        } else {
            for (size_t i = 0; i < len; i++) {
                MOQ_TEST_CHECK_EQ_INT((int)write_ts(s, t, stream + i, 1, false,
                                                    &consumed), (int)MOQ_OK);
                MOQ_TEST_CHECK_EQ_U64(consumed, 1);
            }
        }
        log_queue(s, pass == 0 ? &whole : &bytes);
        moq_media_sender_test_free(s);
    }
    MOQ_TEST_CHECK(whole.count > GOPS);
    MOQ_TEST_CHECK_EQ_U64(whole.count, bytes.count);
    MOQ_TEST_CHECK_EQ_U64(whole.len, bytes.len);
    MOQ_TEST_CHECK(memcmp(whole.bytes, bytes.bytes, whole.len) == 0);
    unsigned groups = 0;
    for (size_t i = 0; i < whole.count && i < bytes.count; i++) {
        MOQ_TEST_CHECK(whole.off[i] == bytes.off[i] &&
                       whole.group[i] == bytes.group[i] &&
                       whole.starts[i] == bytes.starts[i]);
        if (whole.starts[i]) {
            groups++;
            /* Every Group opens with the PAT (PID 0) of its lead-in. */
            const uint8_t *p = whole.bytes + whole.off[i];
            MOQ_TEST_CHECK(p[0] == 0x47 && (p[1] & 0x1F) == 0 && p[2] == 0);
        }
    }
    MOQ_TEST_CHECK_EQ_INT((int)groups, (int)GOPS);
    if (failures == before) printf("PASS: ts_write_chunking\n");
}

/* RETURN_WOULD_BLOCK: a short write -- MOQ_OK, *consumed covering the packet
 * that completed the Object that could not be queued; that Object is held and
 * queued first by the next call; a call that can make no progress returns
 * MOQ_ERR_WOULD_BLOCK and changes nothing; nothing is lost or duplicated. */
static void test_consumed_and_parked(void)
{
    int before = failures;
    static uint8_t stream[STREAM_MAX];
    size_t len = make_stream(stream, 188, false);
    /* Pre-ready: the pre-ready bound (2 Objects) applies. */
    moq_media_sender_t *s = make_sender(MOQ_MEDIA_SEND_BP_RETURN_WOULD_BLOCK, 2);
    if (!s) { failures++; return; }
    moq_msf_mpeg2ts_t rec;
    ts_rec(&rec, MOQ_MSF_MPEG2TS_MODE_UNMODIFIED_PROGRAM, 188);
    /* Three packets per Object: the PAT/PMT lead-in fits beside the random
     * access point, so no packet is discarded before it. */
    moq_media_track_t *t = add_bytes_track(s, &rec, 3);

    static uint8_t out[STREAM_MAX + 4 * 188];
    size_t out_len = 0, off = 0;
    int short_writes = 0;
    for (int iter = 0; iter < 200 && off <= len; iter++) {
        size_t consumed = 99;
        moq_result_t rc = write_ts(s, t, stream + off, len - off, false,
                                   &consumed);
        MOQ_TEST_CHECK(consumed <= len - off);
        MOQ_TEST_CHECK(consumed % 188 == 0);   /* whole packets here */
        off += consumed;
        moq_media_sender_track_stats_t st;
        MOQ_TEST_CHECK(moq_media_sender_get_track_stats(s, t, &st,
                                                        sizeof(st)) == MOQ_OK);
        if (rc == MOQ_ERR_WOULD_BLOCK) {
            /* No progress: nothing taken, nothing changed. */
            MOQ_TEST_CHECK_EQ_U64(consumed, 0);
            MOQ_TEST_CHECK(st.mpeg2ts_objects_parked >= 1);
        }
        if (rc == MOQ_ERR_WOULD_BLOCK || st.mpeg2ts_objects_parked > 0) {
            /* A short write holds Objects back and still reports MOQ_OK. */
            if (rc == MOQ_OK) short_writes++;
            MOQ_TEST_CHECK(st.mpeg2ts_objects_parked >= 1 &&
                           st.mpeg2ts_objects_parked <= 2);
            MOQ_TEST_CHECK_EQ_U64(queue_len(s), 2);
            /* While the queue stays full a call makes no progress:
             * MOQ_ERR_WOULD_BLOCK, nothing taken, the queue and the held
             * Objects as they were. */
            size_t c0 = 99;
            MOQ_TEST_CHECK_EQ_INT((int)write_ts(s, t, stream + off, len - off,
                                                false, &c0),
                                  (int)MOQ_ERR_WOULD_BLOCK);
            MOQ_TEST_CHECK_EQ_U64(c0, 0);
            moq_media_sender_track_stats_t q;
            MOQ_TEST_CHECK(moq_media_sender_get_track_stats(s, t, &q,
                                                            sizeof(q)) == MOQ_OK);
            MOQ_TEST_CHECK_EQ_U64(q.mpeg2ts_objects_parked,
                                  st.mpeg2ts_objects_parked);
            MOQ_TEST_CHECK_EQ_U64(q.mpeg2ts_packets_in, st.mpeg2ts_packets_in);
            MOQ_TEST_CHECK_EQ_U64(queue_len(s), 2);
            /* The queue frees: the head leaves as if sent. */
            moq_rcbuf_t *p = NULL;
            MOQ_TEST_CHECK(moq_media_sender_test_emit_head(s, &p));
            if (p) {
                memcpy(out + out_len, moq_rcbuf_data(p), moq_rcbuf_len(p));
                out_len += moq_rcbuf_len(p);
                moq_rcbuf_decref(p);
            }
            continue;
        }
        MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
        if (off == len) break;
    }
    MOQ_TEST_CHECK_EQ_U64(off, len);
    MOQ_TEST_CHECK(short_writes > 0);
    moq_media_sender_track_stats_t pst;
    size_t c = 99;
    /* Close the last Object and take everything left: empty calls until
     * nothing is held. */
    moq_result_t rc = MOQ_ERR_WOULD_BLOCK;
    for (int i = 0; i < 8 && rc == MOQ_ERR_WOULD_BLOCK; i++) {
        moq_rcbuf_t *p = NULL;
        if (moq_media_sender_test_emit_head(s, &p) && p) {
            memcpy(out + out_len, moq_rcbuf_data(p), moq_rcbuf_len(p));
            out_len += moq_rcbuf_len(p);
            moq_rcbuf_decref(p);
        }
        rc = write_ts(s, t, NULL, 0, true, &c);
    }
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
    for (int i = 0; i < 8; i++) {
        MOQ_TEST_CHECK(moq_media_sender_get_track_stats(s, t, &pst,
                                                        sizeof(pst)) == MOQ_OK);
        if (pst.mpeg2ts_objects_parked == 0) break;
        moq_rcbuf_t *p = NULL;
        if (moq_media_sender_test_emit_head(s, &p) && p) {
            memcpy(out + out_len, moq_rcbuf_data(p), moq_rcbuf_len(p));
            out_len += moq_rcbuf_len(p);
            moq_rcbuf_decref(p);
        }
        rc = write_ts(s, t, NULL, 0, false, &c);
        MOQ_TEST_CHECK(rc == MOQ_OK || rc == MOQ_ERR_WOULD_BLOCK);
    }
    moq_rcbuf_t *p = NULL;
    while (moq_media_sender_test_emit_head(s, &p)) {
        memcpy(out + out_len, moq_rcbuf_data(p), moq_rcbuf_len(p));
        out_len += moq_rcbuf_len(p);
        moq_rcbuf_decref(p);
    }
    /* Every packet arrived once, in order (the stream starts at a random
     * access point's lead-in, so nothing is discarded before it). */
    MOQ_TEST_CHECK_EQ_U64(out_len, len);
    MOQ_TEST_CHECK(out_len == len && memcmp(out, stream, len) == 0);
    moq_media_sender_track_stats_t st;
    MOQ_TEST_CHECK(moq_media_sender_get_track_stats(s, t, &st, sizeof(st)) ==
                   MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(st.mpeg2ts_objects_parked, 0);
    MOQ_TEST_CHECK_EQ_U64(st.objects_written, st.objects_sent);
    MOQ_TEST_CHECK_EQ_U64(st.mpeg2ts_packets_in, len / 188);
    MOQ_TEST_CHECK_EQ_U64(st.mpeg2ts_groups, GOPS);
    moq_media_sender_test_free(s);
    if (failures == before) printf("PASS: ts_consumed_and_parked\n");
}

/* A missing sync byte stops the call before that packet's bytes in it, and
 * changes nothing else -- whether the packet starts in this call or in a
 * previous one. */
static void test_sync_error(void)
{
    int before = failures;
    static uint8_t stream[STREAM_MAX];
    size_t len = make_stream(stream, 188, false);
    (void)len;
    moq_media_sender_t *s = make_sender(MOQ_MEDIA_SEND_BP_RETURN_WOULD_BLOCK, 0);
    if (!s) { failures++; return; }
    moq_msf_mpeg2ts_t rec;
    ts_rec(&rec, MOQ_MSF_MPEG2TS_MODE_UNMODIFIED_PROGRAM, 188);
    moq_media_track_t *t = add_bytes_track(s, &rec, 4);

    stream[3 * 188] = 0x00;                 /* packet 3 has no sync byte */
    size_t c = 99;
    MOQ_TEST_CHECK_EQ_INT((int)write_ts(s, t, stream, 6 * 188, false, &c),
                          (int)MOQ_ERR_PROTO);
    MOQ_TEST_CHECK_EQ_U64(c, 3 * 188);
    moq_media_sender_track_stats_t st;
    MOQ_TEST_CHECK(moq_media_sender_get_track_stats(s, t, &st, sizeof(st)) ==
                   MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(st.mpeg2ts_packets_in, 3);
    /* Repeating it refuses the same packet again, taking nothing. */
    MOQ_TEST_CHECK_EQ_INT((int)write_ts(s, t, stream + 3 * 188, 188, false, &c),
                          (int)MOQ_ERR_PROTO);
    MOQ_TEST_CHECK_EQ_U64(c, 0);
    /* Part of a bad packet is refused too, before any of it is kept. */
    MOQ_TEST_CHECK_EQ_INT((int)write_ts(s, t, stream + 3 * 188, 50, false, &c),
                          (int)MOQ_ERR_PROTO);
    MOQ_TEST_CHECK_EQ_U64(c, 0);
    stream[3 * 188] = 0x47;
    /* A packet that starts in a previous call: its sync byte is judged as it
     * arrives. 100 octets of a good packet are kept... */
    MOQ_TEST_CHECK_EQ_INT((int)write_ts(s, t, stream + 3 * 188, 100, false, &c),
                          (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(c, 100);
    /* ...its other 88 complete it, and the next packet is bad. */
    uint8_t two[2 * 188];
    memcpy(two, stream + 3 * 188, 2 * 188);
    two[188] = 0x12;
    MOQ_TEST_CHECK_EQ_INT((int)write_ts(s, t, two + 100, 2 * 188 - 100, false,
                                        &c), (int)MOQ_ERR_PROTO);
    MOQ_TEST_CHECK_EQ_U64(c, 88);
    MOQ_TEST_CHECK(moq_media_sender_get_track_stats(s, t, &st, sizeof(st)) ==
                   MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(st.mpeg2ts_packets_in, 4);
    moq_media_sender_test_free(s);
    if (failures == before) printf("PASS: ts_sync_error\n");
}

/* end_object makes a call's last packet close an Object, and the PAT/PMT run
 * that precedes a random access point stays with it in the next Group. */
static void test_end_object(void)
{
    int before = failures;
    ts_gen_t g;
    ts_gen_init(&g, 188, 1, false);
    static uint8_t gop1[16 * 188], gop2[16 * 188];
    size_t n1 = ts_gen_gop(&g, gop1, 2);   /* PAT PMT RAP v v A */
    size_t n2 = ts_gen_gop(&g, gop2, 2);
    moq_media_sender_t *s = make_sender(MOQ_MEDIA_SEND_BP_RETURN_WOULD_BLOCK, 0);
    if (!s) { failures++; return; }
    moq_msf_mpeg2ts_t rec;
    ts_rec(&rec, MOQ_MSF_MPEG2TS_MODE_UNMODIFIED_PROGRAM, 188);
    moq_media_track_t *t = add_bytes_track(s, &rec, 64);
    size_t c = 0;
    /* GOP 1, then the next GOP's PAT and PMT, as one write with end_object. */
    static uint8_t first[32 * 188];
    memcpy(first, gop1, n1);
    memcpy(first + n1, gop2, 2 * 188);
    MOQ_TEST_CHECK_EQ_INT((int)write_ts(s, t, first, n1 + 2 * 188, true, &c),
                          (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(queue_len(s), 1);
    const moq_rcbuf_t *p = NULL;
    uint64_t grp = 9;
    bool st = false;
    MOQ_TEST_CHECK(moq_media_sender_test_queue_entry(s, 0, NULL, &p, &grp, &st));
    MOQ_TEST_CHECK(p && moq_rcbuf_len(p) == n1 &&
                   memcmp(moq_rcbuf_data(p), gop1, n1) == 0);
    MOQ_TEST_CHECK(grp == 0 && st);
    MOQ_TEST_CHECK_EQ_INT((int)write_ts(s, t, gop2 + 2 * 188, n2 - 2 * 188,
                                        true, &c), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(queue_len(s), 2);
    MOQ_TEST_CHECK(moq_media_sender_test_queue_entry(s, 1, NULL, &p, &grp, &st));
    MOQ_TEST_CHECK(p && moq_rcbuf_len(p) == n2 &&
                   memcmp(moq_rcbuf_data(p), gop2, n2) == 0);
    MOQ_TEST_CHECK(grp == 1 && st);
    moq_media_sender_test_free(s);
    if (failures == before) printf("PASS: ts_end_object\n");
}

/* One writer per track: a second write_mpeg2ts (or end_track) while one is
 * in progress is refused, and takes nothing. */
typedef struct busy_ctx {
    moq_media_sender_t *s;
    moq_media_track_t  *t;
    const uint8_t      *d;
    size_t              n;
    atomic_bool         waiting;
    moq_result_t        rc;
    size_t              consumed;
} busy_ctx_t;

static void busy_wait_entry(void *ctx)
{
    atomic_store(&((busy_ctx_t *)ctx)->waiting, true);
}

static void *busy_writer(void *p)
{
    busy_ctx_t *b = (busy_ctx_t *)p;
    b->rc = write_ts(b->s, b->t, b->d, b->n, true, &b->consumed);
    return NULL;
}

static void test_one_writer(void)
{
    int before = failures;
    static uint8_t stream[STREAM_MAX];
    size_t len = make_stream(stream, 188, false);
    moq_media_sender_t *s = make_sender(MOQ_MEDIA_SEND_BP_BLOCK_TIMEOUT, 1);
    if (!s) { failures++; return; }
    moq_msf_mpeg2ts_t rec;
    ts_rec(&rec, MOQ_MSF_MPEG2TS_MODE_UNMODIFIED_PROGRAM, 188);
    moq_media_track_t *t = add_bytes_track(s, &rec, 2);
    busy_ctx_t b;
    memset(&b, 0, sizeof(b));
    b.s = s; b.t = t; b.d = stream; b.n = len;
    moq_media_sender_test_set_wait_entry_hook(busy_wait_entry, &b);
    pthread_t th;
    pthread_create(&th, NULL, busy_writer, &b);
    for (int i = 0; i < 2000 && !atomic_load(&b.waiting); i++) usleep(1000);
    MOQ_TEST_CHECK(atomic_load(&b.waiting));
    size_t c = 99;
    MOQ_TEST_CHECK_EQ_INT((int)write_ts(s, t, stream, 188, false, &c),
                          (int)MOQ_ERR_WRONG_STATE);
    MOQ_TEST_CHECK_EQ_U64(c, 0);
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_end_track(s, t),
                          (int)MOQ_ERR_WRONG_STATE);
    pthread_join(th, NULL);
    moq_media_sender_test_set_wait_entry_hook(NULL, NULL);
    /* BLOCK_TIMEOUT never drops: the Object is held and the call is a short
     * write reporting exactly what it took. */
    MOQ_TEST_CHECK_EQ_INT((int)b.rc, (int)MOQ_OK);
    MOQ_TEST_CHECK(b.consumed > 0 && b.consumed < len);
    moq_media_sender_track_stats_t st;
    MOQ_TEST_CHECK(moq_media_sender_get_track_stats(s, t, &st, sizeof(st)) ==
                   MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(st.objects_dropped, 0);
    MOQ_TEST_CHECK(st.mpeg2ts_objects_parked >= 1);
    moq_media_sender_test_free(s);
    if (failures == before) printf("PASS: ts_one_writer\n");
}

/* write() is the app-Object mode; the two modes do not mix. */
static void test_app_objects(void)
{
    int before = failures;
    ts_gen_t g;
    ts_gen_init(&g, 188, 1, false);
    static uint8_t gop[16 * 188];
    size_t n = ts_gen_gop(&g, gop, 2);
    moq_media_sender_t *s = make_sender(MOQ_MEDIA_SEND_BP_RETURN_WOULD_BLOCK, 0);
    if (!s) { failures++; return; }

    moq_msf_mpeg2ts_t rec;
    ts_rec(&rec, MOQ_MSF_MPEG2TS_MODE_UNMODIFIED_PROGRAM, 188);
    rec.has_random_access = true; rec.random_access = true;
    moq_msf_track_ext_t ext = { .kind = MOQ_MSF_EXT_MPEG2TS,
                                .u = { .mpeg2ts = &rec } };
    ts_track_opts_t o;
    memset(&o, 0, sizeof(o));
    o.name = "app"; o.ext = &ext; o.ext_count = 1; o.app_objects = true;
    moq_media_track_t *app = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)add_track(s, &o, &app), (int)MOQ_OK);
    moq_msf_mpeg2ts_t rec2;
    ts_rec(&rec2, MOQ_MSF_MPEG2TS_MODE_UNMODIFIED_PROGRAM, 188);
    moq_media_track_t *bytes = add_bytes_track(s, &rec2, 4);
    /* An app-Object track that declares no random access. */
    moq_msf_mpeg2ts_t rec3;
    ts_rec(&rec3, MOQ_MSF_MPEG2TS_MODE_UNMODIFIED_PROGRAM, 188);
    moq_msf_track_ext_t ext3 = { .kind = MOQ_MSF_EXT_MPEG2TS,
                                 .u = { .mpeg2ts = &rec3 } };
    memset(&o, 0, sizeof(o));
    o.name = "app2"; o.ext = &ext3; o.ext_count = 1; o.app_objects = true;
    moq_media_track_t *app2 = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)add_track(s, &o, &app2), (int)MOQ_OK);

    size_t c = 99;
    MOQ_TEST_CHECK_EQ_INT((int)write_ts(s, app, gop, n, false, &c),
                          (int)MOQ_ERR_WRONG_STATE);
    MOQ_TEST_CHECK_EQ_U64(c, 0);

    moq_media_send_object_t ob;
    memset(&ob, 0, sizeof(ob));
    ob.struct_size = sizeof(ob);
    moq_rcbuf_t *b = NULL;
    /* Not whole packets, on either track. */
    moq_rcbuf_create(moq_alloc_default(), gop, 187, &b);
    ob.payload = b; ob.is_sync = true; ob.starts_group = true;
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_write(s, app, &ob),
                          (int)MOQ_ERR_PROTO);
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_write(s, app2, &ob),
                          (int)MOQ_ERR_PROTO);
    moq_rcbuf_decref(b);
    /* A bad sync byte. */
    uint8_t badsync[188];
    memcpy(badsync, gop, sizeof(badsync));
    badsync[0] = 0x00;
    moq_rcbuf_create(moq_alloc_default(), badsync, sizeof(badsync), &b);
    ob.payload = b;
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_write(s, app2, &ob),
                          (int)MOQ_ERR_PROTO);
    moq_rcbuf_decref(b);
    /* A Group start without a random access point (the PAT/PMT alone). */
    moq_rcbuf_create(moq_alloc_default(), gop, 2 * 188, &b);
    ob.payload = b;
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_write(s, app, &ob),
                          (int)MOQ_ERR_PROTO);
    moq_rcbuf_decref(b);
    /* The GOP: accepted, ownership taken. */
    moq_rcbuf_create(moq_alloc_default(), gop, n, &b);
    ob.payload = b;
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_write(s, app, &ob), (int)MOQ_OK);
    /* A byte-stream track takes no write(). */
    moq_rcbuf_create(moq_alloc_default(), gop, n, &b);
    ob.payload = b;
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_write(s, bytes, &ob),
                          (int)MOQ_ERR_WRONG_STATE);
    moq_rcbuf_decref(b);
    MOQ_TEST_CHECK_EQ_U64(queue_len(s), 1);
    moq_media_sender_test_free(s);
    if (failures == before) printf("PASS: ts_app_objects\n");
}

/* end_track ends the source: held Objects, then the packetizer's last ones,
 * then END_OF_TRACK; a partial packet is discarded and counted. */
static void test_end_track(void)
{
    int before = failures;
    static uint8_t stream[STREAM_MAX];
    size_t len = make_stream(stream, 188, false);
    moq_media_sender_t *s = make_sender(MOQ_MEDIA_SEND_BP_RETURN_WOULD_BLOCK, 0);
    if (!s) { failures++; return; }
    moq_msf_mpeg2ts_t rec;
    ts_rec(&rec, MOQ_MSF_MPEG2TS_MODE_UNMODIFIED_PROGRAM, 188);
    moq_media_track_t *t = add_bytes_track(s, &rec, 64);
    size_t c = 0;
    MOQ_TEST_CHECK_EQ_INT((int)write_ts(s, t, stream, len, false, &c),
                          (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)write_ts(s, t, stream, 100, false, &c),
                          (int)MOQ_OK);
    /* GOPs 1 and 2 are out (each closed by the next Group); GOP 3 is still
     * being filled. */
    MOQ_TEST_CHECK_EQ_U64(queue_len(s), GOPS - 1);
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_end_track(s, t), (int)MOQ_OK);
    /* GOP 3, then the END_OF_TRACK marker. */
    MOQ_TEST_CHECK_EQ_U64(queue_len(s), GOPS + 1);
    static obj_log_t lg;
    memset(&lg, 0, sizeof(lg));
    log_queue(s, &lg);
    MOQ_TEST_CHECK_EQ_U64(lg.len, len);
    MOQ_TEST_CHECK(lg.len == len && memcmp(lg.bytes, stream, len) == 0);
    moq_media_sender_track_stats_t st;
    MOQ_TEST_CHECK(moq_media_sender_get_track_stats(s, t, &st, sizeof(st)) ==
                   MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(st.mpeg2ts_partial_bytes_discarded, 100);
    MOQ_TEST_CHECK_EQ_INT((int)write_ts(s, t, stream, 188, false, &c),
                          (int)MOQ_ERR_WRONG_STATE);
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_end_track(s, t), (int)MOQ_OK);
    moq_media_sender_test_free(s);
    if (failures == before) printf("PASS: ts_end_track\n");
}

/* Per-program: the record's program is filtered out of a multiplex. */
static void test_per_program(void)
{
    int before = failures;
    static uint8_t stream[STREAM_MAX + GOPS * 2 * 188];
    ts_gen_t g;
    ts_gen_init(&g, 188, 1, true);
    size_t len = 0;
    for (unsigned i = 0; i < GOPS; i++) len += ts_gen_gop(&g, stream + len, 2);
    moq_media_sender_t *s = make_sender(MOQ_MEDIA_SEND_BP_RETURN_WOULD_BLOCK, 0);
    if (!s) { failures++; return; }
    moq_msf_mpeg2ts_t rec;
    ts_rec(&rec, MOQ_MSF_MPEG2TS_MODE_PER_PROGRAM, 188);
    rec.has_program_number = true; rec.program_number = 1;
    moq_media_track_t *t = add_bytes_track(s, &rec, 64);
    size_t c = 0;
    MOQ_TEST_CHECK_EQ_INT((int)write_ts(s, t, stream, len, true, &c), (int)MOQ_OK);
    static obj_log_t lg;
    memset(&lg, 0, sizeof(lg));
    log_queue(s, &lg);
    MOQ_TEST_CHECK(lg.len > 0 && lg.len < len);
    bool other = false;
    for (size_t off = 0; off + 188 <= lg.len; off += 188) {
        unsigned pid = ((unsigned)(lg.bytes[off + 1] & 0x1F) << 8) |
                       lg.bytes[off + 2];
        if (pid == TS_PMT2_PID || pid == TS_VIDEO2_PID) other = true;
    }
    MOQ_TEST_CHECK(!other);
    moq_media_sender_test_free(s);
    if (failures == before) printf("PASS: ts_per_program\n");
}

/* Under a drop policy, an open Group that loses queued media is closed: the
 * packetizer's later Objects of it are discarded and counted, and the next
 * Group is queued normally. */
static void test_continuation_discard(void)
{
    int before = failures;
    static uint8_t stream[STREAM_MAX];
    size_t len = make_stream(stream, 188, false);
    (void)len;
    size_t gop = (5u + VIDEO_CONT) * 188;
    moq_media_sender_t *s = make_sender(MOQ_MEDIA_SEND_BP_DROP_TO_KEYFRAME, 3);
    if (!s) { failures++; return; }
    moq_msf_mpeg2ts_t rec;
    ts_rec(&rec, MOQ_MSF_MPEG2TS_MODE_UNMODIFIED_PROGRAM, 188);
    moq_media_track_t *t = add_bytes_track(s, &rec, 2);
    ts_track_opts_t o;
    memset(&o, 0, sizeof(o));
    o.name = "loc"; o.packaging = MOQ_MEDIA_PACKAGING_RAW;
    moq_media_track_t *loc = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)add_track(s, &o, &loc), (int)MOQ_OK);

    size_t c = 0, off = 0;
    /* Group 0's first Object is sent and its second is queued... */
    while (queue_len(s) < 2 && off < gop) {
        MOQ_TEST_CHECK_EQ_INT((int)write_ts(s, t, stream + off, 188, false, &c),
                              (int)MOQ_OK);
        off += 188;
    }
    moq_rcbuf_t *p = NULL;
    MOQ_TEST_CHECK(moq_media_sender_test_emit_head(s, &p));
    if (p) moq_rcbuf_decref(p);
    /* ...and LOC keyframes push the queued one out. */
    MOQ_TEST_CHECK_EQ_U64(queue_len(s), 1);
    for (int i = 0; i < 3; i++) {
        moq_media_send_object_t ob;
        memset(&ob, 0, sizeof(ob));
        ob.struct_size = sizeof(ob);
        moq_rcbuf_t *b = NULL;
        moq_rcbuf_create(moq_alloc_default(), stream, 8, &b);
        ob.payload = b; ob.is_sync = true; ob.starts_group = true;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_write(s, loc, &ob),
                              (int)MOQ_OK);
    }
    moq_media_sender_track_stats_t st;
    MOQ_TEST_CHECK(moq_media_sender_get_track_stats(s, t, &st, sizeof(st)) ==
                   MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(st.objects_dropped, 1);
    /* The rest of group 0 is discarded, never queued as a continuation. */
    MOQ_TEST_CHECK_EQ_INT((int)write_ts(s, t, stream + off, gop - off,
                                        false, &c), (int)MOQ_OK);
    MOQ_TEST_CHECK(moq_media_sender_get_track_stats(s, t, &st, sizeof(st)) ==
                   MOQ_OK);
    MOQ_TEST_CHECK(st.mpeg2ts_continuation_objects_discarded >= 1);
    uint64_t written = st.objects_written;
    /* The next Group starts normally, at Group 1 object 0. */
    MOQ_TEST_CHECK_EQ_INT((int)write_ts(s, t, stream + gop, 4 * 188, false, &c),
                          (int)MOQ_OK);
    MOQ_TEST_CHECK(moq_media_sender_get_track_stats(s, t, &st, sizeof(st)) ==
                   MOQ_OK);
    MOQ_TEST_CHECK(st.objects_written > written);
    const moq_media_track_t *qt = NULL;
    uint64_t grp = 0;
    bool starts = false;
    bool found = false;
    for (size_t i = 0;
         moq_media_sender_test_queue_entry(s, i, &qt, NULL, &grp, &starts); i++)
        if (qt == t) {
            MOQ_TEST_CHECK(grp == 1 && starts);
            found = true;
            break;
        }
    MOQ_TEST_CHECK(found);
    moq_media_sender_test_free(s);
    if (failures == before) printf("PASS: ts_continuation_discard\n");
}

/* An allocator that refuses every request while armed. */
static bool g_refuse;
static void *gate_alloc(size_t n, void *ctx)
{
    (void)ctx;
    return g_refuse ? NULL : malloc(n);
}
static void *gate_realloc(void *p, size_t o, size_t n, void *ctx)
{
    (void)o; (void)ctx;
    return g_refuse ? NULL : realloc(p, n);
}
static void gate_free(void *p, size_t n, void *ctx)
{
    (void)n; (void)ctx;
    free(p);
}
static const moq_alloc_t g_gate = { NULL, gate_alloc, gate_realloc, gate_free };

static moq_media_sender_t *make_gated_sender(void)
{
    moq_media_sender_test_set_alloc(&g_gate);
    moq_media_sender_t *s = make_sender(MOQ_MEDIA_SEND_BP_RETURN_WOULD_BLOCK, 64);
    moq_media_sender_test_set_alloc(NULL);
    return s;
}

/* A Group start that cannot be copied loses its whole Group: the call
 * reports MOQ_ERR_NOMEM, the Group's later Objects are discarded rather than
 * queued into the previous service Group, and the next Group opens a new
 * one. */
static void test_lost_group_start(void)
{
    int before = failures;
    static uint8_t stream[STREAM_MAX];
    make_stream(stream, 188, false);
    const size_t gop = (4u + VIDEO_CONT) * 188;   /* PAT PMT RAP v.. A */
    moq_media_sender_t *s = make_gated_sender();
    if (!s) { failures++; return; }
    moq_msf_mpeg2ts_t rec;
    ts_rec(&rec, MOQ_MSF_MPEG2TS_MODE_UNMODIFIED_PROGRAM, 188);
    /* Three packets per Object: each GOP is exactly three Objects. */
    moq_media_track_t *t = add_bytes_track(s, &rec, 3);
    size_t c = 0;
    MOQ_TEST_CHECK_EQ_INT((int)write_ts(s, t, stream, gop, false, &c),
                          (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(queue_len(s), 3);
    /* GOP 2's first Object (PAT PMT RAP) cannot be copied. */
    g_refuse = true;
    MOQ_TEST_CHECK_EQ_INT((int)write_ts(s, t, stream + gop, 3 * 188, false,
                                        &c), (int)MOQ_ERR_NOMEM);
    g_refuse = false;
    MOQ_TEST_CHECK_EQ_U64(c, 3 * 188);
    MOQ_TEST_CHECK_EQ_INT((int)write_ts(s, t, stream + gop + 3 * 188,
                                        gop - 3 * 188, false, &c), (int)MOQ_OK);
    moq_media_sender_track_stats_t st;
    MOQ_TEST_CHECK(moq_media_sender_get_track_stats(s, t, &st, sizeof(st)) ==
                   MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(st.mpeg2ts_continuation_objects_discarded, 2);
    MOQ_TEST_CHECK_EQ_U64(queue_len(s), 3);
    /* The lost start took Group ID 1 with it: GOP 3 opens Group 2, so the
     * hole is visible on the wire. */
    MOQ_TEST_CHECK_EQ_INT((int)write_ts(s, t, stream + 2 * gop, gop, true, &c),
                          (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(queue_len(s), 6);
    uint64_t grp = 9;
    bool starts = false;
    for (size_t i = 0;
         moq_media_sender_test_queue_entry(s, i, NULL, NULL, &grp, &starts); i++)
        MOQ_TEST_CHECK(grp == (i < 3 ? 0u : 2u) && starts == (i % 3 == 0));
    moq_media_sender_test_free(s);
    if (failures == before) printf("PASS: ts_lost_group_start\n");
}

/* The interrupt latch, set once as a write starts to wait for space. */
static moq_endpoint_t *g_int_ep;
static int g_int_once;
static void interrupt_on_wait(void *ctx)
{
    (void)ctx;
    if (g_int_once > 0) {
        g_int_once--;
        moq_endpoint_set_interrupted(g_int_ep, true);
    }
}

/* Append the queue head's payload to `out` and remove it, as if sent. */
static void take_head(moq_media_sender_t *s, uint8_t *out, size_t *out_len)
{
    moq_rcbuf_t *p = NULL;
    MOQ_TEST_CHECK(moq_media_sender_test_emit_head(s, &p));
    if (!p) return;
    memcpy(out + *out_len, moq_rcbuf_data(p), moq_rcbuf_len(p));
    *out_len += moq_rcbuf_len(p);
    moq_rcbuf_decref(p);
}

/* An interrupt while an Object waits for space loses nothing: the call
 * returns MOQ_ERR_INTERRUPTED with the packets it took, the Object is held,
 * and after the latch clears the next call queues it first, so the Group
 * continues with no hole. */
static void test_interrupt_keeps_output(void)
{
    int before = failures;
    static uint8_t stream[STREAM_MAX];
    make_stream(stream, 188, false);
    const size_t gop = (4u + VIDEO_CONT) * 188;   /* three Objects of 3 */
    moq_endpoint_t *ep = moq_endpoint_test_make_bare();
    moq_media_sender_t *s = make_sender(MOQ_MEDIA_SEND_BP_BLOCK_TIMEOUT, 1);
    if (!s || !ep) { failures++; return; }
    moq_media_sender_test_set_ep(s, ep);
    moq_msf_mpeg2ts_t rec;
    ts_rec(&rec, MOQ_MSF_MPEG2TS_MODE_UNMODIFIED_PROGRAM, 188);
    moq_media_track_t *t = add_bytes_track(s, &rec, 3);
    g_int_ep = ep;
    g_int_once = 1;
    moq_media_sender_test_set_wait_entry_hook(interrupt_on_wait, NULL);

    static uint8_t out[STREAM_MAX];
    size_t out_len = 0, c = 0;
    /* Object 0 is queued; Object 1, completed by packet 6, waits and is
     * interrupted. */
    MOQ_TEST_CHECK_EQ_INT((int)write_ts(s, t, stream, gop, false, &c),
                          (int)MOQ_ERR_INTERRUPTED);
    MOQ_TEST_CHECK_EQ_U64(c, 6 * 188);
    moq_media_sender_track_stats_t st;
    MOQ_TEST_CHECK(moq_media_sender_get_track_stats(s, t, &st, sizeof(st)) ==
                   MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(st.mpeg2ts_objects_parked, 1);
    MOQ_TEST_CHECK_EQ_U64(queue_len(s), 1);
    /* While the latch is set nothing is taken. */
    size_t c2 = 99;
    MOQ_TEST_CHECK_EQ_INT((int)write_ts(s, t, stream + c, gop - c, false, &c2),
                          (int)MOQ_ERR_INTERRUPTED);
    MOQ_TEST_CHECK_EQ_U64(c2, 0);
    moq_endpoint_set_interrupted(ep, false);

    /* Resume: Object 1 first, then Object 2, every one in Group 0. */
    size_t off = c;
    take_head(s, out, &out_len);
    for (int i = 0; i < 8 && off < gop; i++) {
        MOQ_TEST_CHECK_EQ_INT((int)write_ts(s, t, stream + off, gop - off,
                                            false, &c), (int)MOQ_OK);
        off += c;
        uint64_t grp = 9;
        bool starts = true;
        MOQ_TEST_CHECK(moq_media_sender_test_queue_entry(s, 0, NULL, NULL,
                                                         &grp, &starts));
        MOQ_TEST_CHECK(grp == 0 && !starts);
        take_head(s, out, &out_len);
    }
    for (int i = 0; i < 4; i++) {
        MOQ_TEST_CHECK(moq_media_sender_get_track_stats(s, t, &st,
                                                        sizeof(st)) == MOQ_OK);
        if (st.mpeg2ts_objects_parked == 0 && queue_len(s) == 0) break;
        MOQ_TEST_CHECK(write_ts(s, t, NULL, 0, false, &c) == MOQ_OK);
        if (queue_len(s) > 0) take_head(s, out, &out_len);
    }
    MOQ_TEST_CHECK_EQ_U64(off, gop);
    MOQ_TEST_CHECK_EQ_U64(out_len, gop);
    MOQ_TEST_CHECK(out_len == gop && memcmp(out, stream, gop) == 0);
    MOQ_TEST_CHECK(moq_media_sender_get_track_stats(s, t, &st, sizeof(st)) ==
                   MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(st.objects_dropped, 0);
    MOQ_TEST_CHECK_EQ_U64(st.mpeg2ts_continuation_objects_discarded, 0);
    moq_media_sender_test_set_wait_entry_hook(NULL, NULL);
    moq_media_sender_test_set_ep(s, NULL);
    moq_media_sender_test_free(s);
    moq_endpoint_test_free_bare(ep);
    if (failures == before) printf("PASS: ts_interrupt_keeps_output\n");
}

/* end_track reports a last Object it could not copy, and is not ended until
 * it is called again. */
static void test_end_track_nomem(void)
{
    int before = failures;
    static uint8_t stream[STREAM_MAX];
    make_stream(stream, 188, false);
    moq_media_sender_t *s = make_gated_sender();
    if (!s) { failures++; return; }
    moq_msf_mpeg2ts_t rec;
    ts_rec(&rec, MOQ_MSF_MPEG2TS_MODE_UNMODIFIED_PROGRAM, 188);
    moq_media_track_t *t = add_bytes_track(s, &rec, 3);
    size_t c = 0;
    MOQ_TEST_CHECK_EQ_INT((int)write_ts(s, t, stream, 4 * 188, false, &c),
                          (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(queue_len(s), 1);
    g_refuse = true;
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_end_track(s, t),
                          (int)MOQ_ERR_NOMEM);
    g_refuse = false;
    /* The lost last Object abandons Group 0: its queued first Object is
     * dropped and counted, and there is no END_OF_TRACK yet. */
    MOQ_TEST_CHECK_EQ_U64(queue_len(s), 0);
    moq_media_sender_track_stats_t st;
    MOQ_TEST_CHECK(moq_media_sender_get_track_stats(s, t, &st, sizeof(st)) ==
                   MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(st.objects_dropped, 1);
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_end_track(s, t), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(queue_len(s), 1);
    moq_media_sender_test_free(s);
    if (failures == before) printf("PASS: ts_end_track_nomem\n");
}

/* A sync error discards the bytes of the bad packet kept from earlier calls
 * and counts them, so the next call starts at a packet boundary. */
static void test_proto_discards_carry(void)
{
    int before = failures;
    static uint8_t stream[STREAM_MAX];
    make_stream(stream, 192, false);
    moq_media_sender_t *s = make_sender(MOQ_MEDIA_SEND_BP_RETURN_WOULD_BLOCK, 0);
    if (!s) { failures++; return; }
    moq_msf_mpeg2ts_t rec;
    ts_rec(&rec, MOQ_MSF_MPEG2TS_MODE_UNMODIFIED_PROGRAM, 192);
    moq_media_track_t *t = add_bytes_track(s, &rec, 4);
    size_t c = 0;
    /* Two octets of a 192-octet packet's prefix... */
    MOQ_TEST_CHECK_EQ_INT((int)write_ts(s, t, stream, 2, false, &c),
                          (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(c, 2);
    /* ...then the rest, without a sync byte. */
    uint8_t bad[192];
    memcpy(bad, stream, 192);
    bad[4] = 0x00;
    MOQ_TEST_CHECK_EQ_INT((int)write_ts(s, t, bad + 2, 190, false, &c),
                          (int)MOQ_ERR_PROTO);
    MOQ_TEST_CHECK_EQ_U64(c, 0);
    moq_media_sender_track_stats_t st;
    MOQ_TEST_CHECK(moq_media_sender_get_track_stats(s, t, &st, sizeof(st)) ==
                   MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(st.mpeg2ts_partial_bytes_discarded, 2);
    /* A whole good packet is taken as one. */
    MOQ_TEST_CHECK_EQ_INT((int)write_ts(s, t, stream, 192, false, &c),
                          (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(c, 192);
    MOQ_TEST_CHECK(moq_media_sender_get_track_stats(s, t, &st, sizeof(st)) ==
                   MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(st.mpeg2ts_packets_in, 1);
    moq_media_sender_test_free(s);
    if (failures == before) printf("PASS: ts_proto_discards_carry\n");
}

/* keyframes_dropped counts dropped Objects that hold a random access point:
 * a Group forwarded before the first one starts without it. */
static void test_keyframe_stats(void)
{
    int before = failures;
    static uint8_t stream[STREAM_MAX];
    make_stream(stream, 188, false);
    const size_t gop = (4u + VIDEO_CONT) * 188;
    moq_media_sender_t *s = make_sender(MOQ_MEDIA_SEND_BP_DROP_TO_KEYFRAME, 64);
    if (!s) { failures++; return; }
    moq_msf_mpeg2ts_t rec;
    ts_rec(&rec, MOQ_MSF_MPEG2TS_MODE_UNMODIFIED_PROGRAM, 188);
    moq_msf_track_ext_t ext = { .kind = MOQ_MSF_EXT_MPEG2TS,
                                .u = { .mpeg2ts = &rec } };
    ts_track_opts_t o;
    memset(&o, 0, sizeof(o));
    o.name = "ts"; o.ext = &ext; o.ext_count = 1;
    o.object_packets = 3;
    o.forward_before_ra = true;
    moq_media_track_t *t = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)add_track(s, &o, &t), (int)MOQ_OK);
    if (!t) { moq_media_sender_test_free(s); failures++; return; }
    /* Join after GOP 1's random access point: Group 0 is v v v | v v A, and
     * Group 1 is PAT PMT RAP | v v v | v v A. */
    size_t c = 0;
    MOQ_TEST_CHECK_EQ_INT((int)write_ts(s, t, stream + 3 * 188,
                                        2 * gop - 3 * 188, true, &c),
                          (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(queue_len(s), 5);
    moq_media_sender_test_drop_track_locked(s, t);
    moq_media_sender_track_stats_t st;
    MOQ_TEST_CHECK(moq_media_sender_get_track_stats(s, t, &st, sizeof(st)) ==
                   MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(st.objects_dropped, 5);
    MOQ_TEST_CHECK_EQ_U64(st.keyframes_dropped, 1);
    moq_media_sender_test_free(s);
    if (failures == before) printf("PASS: ts_keyframe_stats\n");
}

/* The generic per-track counters sum to the sender's own, for LOC, CMAF and
 * TS tracks alike, and the TS counters stay zero on the others. */
static void test_track_stats_sum(void)
{
    int before = failures;
    static uint8_t stream[STREAM_MAX];
    size_t len = make_stream(stream, 188, false);
    moq_media_sender_t *s = make_sender(MOQ_MEDIA_SEND_BP_DROP_GROUP, 4);
    if (!s) { failures++; return; }
    moq_msf_mpeg2ts_t rec;
    ts_rec(&rec, MOQ_MSF_MPEG2TS_MODE_UNMODIFIED_PROGRAM, 188);
    moq_media_track_t *t = add_bytes_track(s, &rec, 4);
    ts_track_opts_t o;
    memset(&o, 0, sizeof(o));
    o.name = "loc"; o.packaging = MOQ_MEDIA_PACKAGING_RAW;
    moq_media_track_t *loc = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)add_track(s, &o, &loc), (int)MOQ_OK);
    size_t c = 0;
    for (int i = 0; i < 6; i++) {
        moq_media_send_object_t ob;
        memset(&ob, 0, sizeof(ob));
        ob.struct_size = sizeof(ob);
        moq_rcbuf_t *b = NULL;
        moq_rcbuf_create(moq_alloc_default(), stream, 16, &b);
        ob.payload = b; ob.is_sync = true; ob.starts_group = true;
        if (moq_media_sender_write(s, loc, &ob) != MOQ_OK) moq_rcbuf_decref(b);
        (void)write_ts(s, t, stream + (size_t)i * 188 * 4, 188 * 4, false, &c);
    }
    moq_rcbuf_t *p = NULL;
    if (moq_media_sender_test_emit_head(s, &p) && p) moq_rcbuf_decref(p);
    moq_media_sender_stats_t all;
    MOQ_TEST_CHECK(moq_media_sender_get_stats(s, &all, sizeof(all)) == MOQ_OK);
    moq_media_sender_track_stats_t a, b2;
    MOQ_TEST_CHECK(moq_media_sender_get_track_stats(s, t, &a, sizeof(a)) == MOQ_OK);
    MOQ_TEST_CHECK(moq_media_sender_get_track_stats(s, loc, &b2, sizeof(b2)) ==
                   MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(a.objects_written + b2.objects_written,
                          all.objects_written);
    MOQ_TEST_CHECK_EQ_U64(a.objects_sent + b2.objects_sent, all.objects_sent);
    MOQ_TEST_CHECK_EQ_U64(a.objects_queued + b2.objects_queued,
                          all.objects_queued);
    MOQ_TEST_CHECK_EQ_U64(a.objects_dropped + b2.objects_dropped,
                          all.objects_dropped);
    MOQ_TEST_CHECK_EQ_U64(a.bytes_queued + b2.bytes_queued, all.bytes_queued);
    MOQ_TEST_CHECK_EQ_U64(a.groups_dropped + b2.groups_dropped,
                          all.groups_dropped);
    MOQ_TEST_CHECK_EQ_U64(a.keyframes_dropped + b2.keyframes_dropped,
                          all.keyframes_dropped);
    MOQ_TEST_CHECK(all.objects_dropped > 0);
    MOQ_TEST_CHECK_EQ_U64(a.objects_written,
                          a.objects_sent + a.objects_queued + a.objects_dropped);
    MOQ_TEST_CHECK_EQ_U64(b2.mpeg2ts_packets_in, 0);
    MOQ_TEST_CHECK(a.mpeg2ts_packets_in > 0);
    /* Size contract: the first published size is required, and a smaller
     * out_size writes nothing past it. */
    MOQ_TEST_CHECK(moq_media_sender_get_track_stats(s, t, &a, 8) ==
                   MOQ_ERR_INVAL);
    MOQ_TEST_CHECK(moq_media_sender_get_track_stats(s, NULL, &a, sizeof(a)) ==
                   MOQ_ERR_INVAL);
    MOQ_TEST_CHECK(moq_media_sender_get_track_stats(
                       s, (const moq_media_track_t *)&all, &a, sizeof(a)) ==
                   MOQ_ERR_INVAL);
    (void)len;
    moq_media_sender_test_free(s);
    if (failures == before) printf("PASS: ts_track_stats_sum\n");
}

#endif /* MOQ_TEST_HAVE_MPEG2TS */

/* Every build has the TS entry points; without moq::mpeg2ts they refuse. */
static void test_build_surface(void)
{
    int before = failures;
    moq_media_sender_t *s = make_sender(MOQ_MEDIA_SEND_BP_RETURN_WOULD_BLOCK, 0);
    if (!s) { failures++; return; }
    moq_msf_mpeg2ts_t rec;
    ts_rec(&rec, MOQ_MSF_MPEG2TS_MODE_UNMODIFIED_PROGRAM, 188);
    moq_msf_track_ext_t ext = { .kind = MOQ_MSF_EXT_MPEG2TS,
                                .u = { .mpeg2ts = &rec } };
    ts_track_opts_t o;
    memset(&o, 0, sizeof(o));
    o.name = "ts"; o.ext = &ext; o.ext_count = 1;
    moq_media_track_t *t = NULL;
    uint8_t pkt[188] = { 0x47 };
    size_t c = 99;
    moq_media_send_mpeg2ts_t in;
    memset(&in, 0, sizeof(in));
    in.struct_size = sizeof(in);
    in.data = (moq_bytes_t){ pkt, sizeof(pkt) };
#ifdef MOQ_TEST_HAVE_MPEG2TS
    MOQ_TEST_CHECK_EQ_INT((int)add_track(s, &o, &t), (int)MOQ_OK);
    in.struct_size = 4;
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_write_mpeg2ts(s, t, &in, &c),
                          (int)MOQ_ERR_INVAL);
    MOQ_TEST_CHECK_EQ_U64(c, 0);
    in.struct_size = sizeof(in);
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_write_mpeg2ts(s, t, &in, NULL),
                          (int)MOQ_ERR_INVAL);
#else
    MOQ_TEST_CHECK_EQ_INT((int)add_track(s, &o, &t), (int)MOQ_ERR_UNSUPPORTED);
    memset(&o, 0, sizeof(o));
    o.name = "loc"; o.packaging = MOQ_MEDIA_PACKAGING_RAW;
    MOQ_TEST_CHECK_EQ_INT((int)add_track(s, &o, &t), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_write_mpeg2ts(s, t, &in, &c),
                          (int)MOQ_ERR_UNSUPPORTED);
    MOQ_TEST_CHECK_EQ_U64(c, 0);
#endif
    moq_media_sender_test_free(s);
    if (failures == before) printf("PASS: ts_build_surface\n");
}

#if defined(MOQ_TEST_HAVE_MPEG2TS) && defined(MOQ_TEST_HAVE_SIM)
/* -- write_mpeg2ts: the companion refusal write() applies ---------------- */

static moq_media_sender_t *make_companion_sender(
    moq_media_companion_profile_t companion)
{
    moq_media_sender_cfg_t cfg;
    moq_media_sender_cfg_init_live_sized(&cfg, sizeof(cfg));
    cfg.namespace_ = (moq_namespace_t){ g_ns_parts, 2 };
    cfg.backpressure = MOQ_MEDIA_SEND_BP_RETURN_WOULD_BLOCK;
    cfg.companion_profile = companion;
    return moq_media_sender_test_new_cfg(&cfg);
}

/* An established session of `version`, its setup events drained. */
static moq_simpair_t *companion_pair(moq_version_t version)
{
    moq_simpair_cfg_t pc = MOQ_SIMPAIR_CFG_INIT;
    pc.alloc = moq_alloc_default();
    pc.version = version;
    pc.client_send_request_capacity = pc.server_send_request_capacity = true;
    pc.client_initial_request_capacity = pc.server_initial_request_capacity = 32;
    moq_simpair_t *sp = NULL;
    if (moq_simpair_create(&pc, &sp) != MOQ_OK) return NULL;
    if (moq_simpair_start(sp) != MOQ_OK ||
        moq_simpair_run_until_quiescent(sp, 16, NULL) != MOQ_OK) {
        moq_simpair_destroy(sp);
        return NULL;
    }
    moq_event_t e;
    while (moq_session_poll_events(moq_simpair_client(sp), &e, 1)) moq_event_cleanup(&e);
    while (moq_session_poll_events(moq_simpair_server(sp), &e, 1)) moq_event_cleanup(&e);
    return sp;
}

/* A CMAF track without init_data: accepted while an AUTO companion is
 * unresolved, and blocking it once it resolves to draft 22. */
static moq_result_t add_no_init_cmaf(moq_media_sender_t *s)
{
    ts_track_opts_t o;
    memset(&o, 0, sizeof(o));
    o.name = "cmaf";
    o.packaging = MOQ_MEDIA_PACKAGING_CMAF;
    moq_media_track_t *t = NULL;
    return add_track(s, &o, &t);
}

/* Rows: 0 explicit legacy on D22, 1 explicit D22 on D18, 2 AUTO on D22
 * blocked by a CMAF track without init_data, 3 explicit D22 before any draft
 * is observed -- each refused before a byte is taken. Controls: 4 legacy on
 * D18, 5 explicit D22 on D22, 6 AUTO on D22, 7 AUTO on D18, 8 unresolved AUTO
 * -- each takes the whole stream. */
static void test_companion_refusal(void)
{
    int before = failures;
    static const struct {
        moq_media_companion_profile_t companion;
        moq_version_t version;       /* 0: no session observed */
        bool blocker;
        bool refused;
    } rows[] = {
        { MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01, MOQ_VERSION_DRAFT_22, false, true },
        { MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC, MOQ_VERSION_DRAFT_18, false, true },
        { MOQ_MEDIA_COMPANION_AUTO, MOQ_VERSION_DRAFT_22, true, true },
        { MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC, 0, false, true },
        { MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01, MOQ_VERSION_DRAFT_18, false, false },
        { MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC, MOQ_VERSION_DRAFT_22, false, false },
        { MOQ_MEDIA_COMPANION_AUTO, MOQ_VERSION_DRAFT_22, false, false },
        { MOQ_MEDIA_COMPANION_AUTO, MOQ_VERSION_DRAFT_18, false, false },
        { MOQ_MEDIA_COMPANION_AUTO, 0, false, false },
    };
    static uint8_t stream[STREAM_MAX];
    size_t len = make_stream(stream, 188, false);
    for (size_t i = 0; i < sizeof(rows) / sizeof(rows[0]); i++) {
        moq_media_sender_t *s = make_companion_sender(rows[i].companion);
        MOQ_TEST_CHECK(s != NULL);
        if (!s) continue;
        moq_msf_mpeg2ts_t rec;
        ts_rec(&rec, MOQ_MSF_MPEG2TS_MODE_UNMODIFIED_PROGRAM, 188);
        moq_media_track_t *t = add_bytes_track(s, &rec, 4);
        if (rows[i].blocker)
            MOQ_TEST_CHECK_EQ_INT((int)add_no_init_cmaf(s), (int)MOQ_OK);
        moq_simpair_t *sp = NULL;
        if (rows[i].version) {
            sp = companion_pair(rows[i].version);
            MOQ_TEST_CHECK(sp != NULL);
            if (sp) moq_media_sender_test_pump(s, moq_simpair_client(sp), 1000);
        }
        if (t && (sp || !rows[i].version)) {
            /* One sync byte: on refusal it is not taken into the carry. */
            size_t consumed = 99;
            moq_result_t rc = write_ts(s, t, stream, 1, false, &consumed);
            MOQ_TEST_CHECK_EQ_INT((int)rc,
                (int)(rows[i].refused ? MOQ_ERR_UNSUPPORTED : MOQ_OK));
            MOQ_TEST_CHECK_EQ_U64(consumed, rows[i].refused ? 0u : 1u);
            if (rows[i].refused) {
                /* The whole stream is refused just the same, and nothing was
                 * kept, held or queued. */
                rc = write_ts(s, t, stream, len, true, &consumed);
                MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_ERR_UNSUPPORTED);
                MOQ_TEST_CHECK_EQ_U64(consumed, 0);
                MOQ_TEST_CHECK_EQ_U64(queue_len(s), 0);
                moq_media_sender_stats_t st;
                MOQ_TEST_CHECK(moq_media_sender_get_stats(s, &st, sizeof(st)) == MOQ_OK &&
                               st.last_error == MOQ_ERR_UNSUPPORTED &&
                               st.objects_written == 0);
                moq_media_sender_track_stats_t ts;
                MOQ_TEST_CHECK(moq_media_sender_get_track_stats(s, t, &ts, sizeof(ts)) == MOQ_OK &&
                               ts.mpeg2ts_objects_parked == 0 &&
                               ts.mpeg2ts_partial_bytes_discarded == 0);
                MOQ_TEST_CHECK(!moq_media_sender_is_fatal(s));
            } else {
                rc = write_ts(s, t, stream + 1, len - 1, false, &consumed);
                MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
                MOQ_TEST_CHECK_EQ_U64(consumed, len - 1);
                MOQ_TEST_CHECK(queue_len(s) > 0);
            }
        }
        moq_media_sender_test_free(s);
        if (sp) moq_simpair_destroy(sp);
    }
    if (failures == before) printf("PASS: ts_write_companion_refusal\n");
}

/* The session latches between Objects of one call -- while the writer has
 * released the sender -- and resolves an AUTO companion to a refusal (draft 22
 * with a CMAF track lacking init_data). The call stops there: no further byte
 * is taken, and the bytes already taken stay reported. */
typedef struct yield_latch {
    moq_media_sender_t *s;
    moq_simpair_t *sp;
    unsigned calls;
} yield_latch_t;

static void latch_in_yield(void *ctx)
{
    yield_latch_t *y = ctx;
    if (y->calls++ == 0)
        moq_media_sender_test_pump(y->s, moq_simpair_client(y->sp), 1000);
}

static void test_companion_refusal_at_yield(void)
{
    int before = failures;
    static uint8_t stream[STREAM_MAX];
    size_t len = make_stream(stream, 188, false);
    moq_media_sender_t *s = make_companion_sender(MOQ_MEDIA_COMPANION_AUTO);
    moq_simpair_t *sp = companion_pair(MOQ_VERSION_DRAFT_22);
    MOQ_TEST_CHECK(s != NULL && sp != NULL);
    if (s && sp) {
        moq_msf_mpeg2ts_t rec;
        ts_rec(&rec, MOQ_MSF_MPEG2TS_MODE_UNMODIFIED_PROGRAM, 188);
        moq_media_track_t *t = add_bytes_track(s, &rec, 2);
        MOQ_TEST_CHECK_EQ_INT((int)add_no_init_cmaf(s), (int)MOQ_OK);
        yield_latch_t y = { s, sp, 0 };
        moq_media_sender_test_set_ts_yield_hook(latch_in_yield, &y);
        size_t consumed = 0;
        moq_result_t rc = t ? write_ts(s, t, stream, len, false, &consumed)
                            : MOQ_ERR_INVAL;
        moq_media_sender_test_set_ts_yield_hook(NULL, NULL);
        MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_ERR_UNSUPPORTED);
        MOQ_TEST_CHECK(y.calls == 1);
        /* Whole packets up to the one that completed the queued Object. */
        MOQ_TEST_CHECK(consumed > 0 && consumed < len && consumed % 188 == 0);
        size_t queued = queue_len(s);
        MOQ_TEST_CHECK(queued > 0);
        size_t again = 99;
        rc = t ? write_ts(s, t, stream + consumed, len - consumed, false, &again)
               : MOQ_ERR_INVAL;
        MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_ERR_UNSUPPORTED);
        MOQ_TEST_CHECK_EQ_U64(again, 0);
        MOQ_TEST_CHECK_EQ_U64(queue_len(s), queued);
        MOQ_TEST_CHECK(!moq_media_sender_is_fatal(s));
    }
    if (s) moq_media_sender_test_free(s);
    if (sp) moq_simpair_destroy(sp);
    if (failures == before) printf("PASS: ts_write_companion_refusal_at_yield\n");
}
#endif /* MOQ_TEST_HAVE_MPEG2TS && MOQ_TEST_HAVE_SIM */

int main(void)
{
    test_build_surface();
#ifdef MOQ_TEST_HAVE_MPEG2TS
    test_add_track_rules();
    test_catalog_entry();
    test_catalog_metadata();
    test_catalog_late_add(false, true, false);
    test_catalog_late_add(false, false, false);
    test_catalog_late_add(true, true, false);
    test_catalog_late_add(false, true, true);
    test_track_cfg_abi();
    test_chunking();
    test_consumed_and_parked();
    test_sync_error();
    test_end_object();
    test_one_writer();
    test_app_objects();
    test_end_track();
    test_per_program();
    test_continuation_discard();
    test_lost_group_start();
    test_end_track_nomem();
    test_interrupt_keeps_output();
    test_proto_discards_carry();
    test_keyframe_stats();
    test_track_stats_sum();
#endif
#if defined(MOQ_TEST_HAVE_MPEG2TS) && defined(MOQ_TEST_HAVE_SIM)
    test_companion_refusal();
    test_companion_refusal_at_yield();
#endif
    if (failures) {
        printf("FAIL: %d sender mpeg2ts failures\n", failures);
        return 1;
    }
    printf("PASS: all sender mpeg2ts tests\n");
    return 0;
}
