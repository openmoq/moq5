/*
 * The track description's decoder observations, through the real catalog
 * ingest of a sessionless receiver:
 *
 *   - codec_config is the decoder extradata for every packaging, and
 *     init_data the resolved init bytes;
 *   - cmaf points at the record parsed from a CMAF header -- clear or CENC
 *     protected -- and is NULL for a LOC track and for a CMAF track whose
 *     header does not parse; the record's codec_config is the description's
 *     own span;
 *   - the record shares the handle's existing string allocation: ingesting a
 *     CMAF track makes exactly as many allocations as the same track
 *     declared LOC, and an allocation refused at any point of the ingest
 *     leaks nothing;
 *   - a description copy borrows the record, and a live-to-VOD conversion
 *     leaves it in place, unchanged;
 *   - the record is charged to the retained-byte cap, inline or by initRef:
 *     a CMAF track costs exactly its LOC twin plus the record, a catalog
 *     over the cap is refused whole, and a removed track keeps its charge.
 *
 * White-box: links moq-service-receiver-test-internals.
 */
#include <moq/media_receiver.h>
#include <moq/msf.h>
#include <moq/rcbuf.h>
#include "test_support.h"
#include "media_builders.h"
#include "media_sender_cmaf_init.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int failures = 0;

/* Test seams (media_receiver.c, MOQ_MEDIA_RECEIVER_TESTING). */
moq_media_receiver_t *moq_media_receiver_test_new_cfg(const moq_media_receiver_cfg_t *cfg);
void moq_media_receiver_test_free(moq_media_receiver_t *r);
void moq_media_receiver_test_set_alloc(const moq_alloc_t *alloc);
void moq_media_receiver_test_set_transport_version(moq_media_receiver_t *r,
                                                   moq_version_t v);
void moq_media_receiver_test_ingest(moq_media_receiver_t *r, uint64_t group,
                                    uint64_t object, const char *json,
                                    size_t len);
bool moq_media_receiver_test_poll(moq_media_receiver_t *r,
                                  moq_media_track_event_kind_t *kind,
                                  moq_media_track_t **track);
void moq_media_receiver_test_set_limits(moq_media_receiver_t *r,
                                        size_t max_stable_handles,
                                        size_t max_cp_snaps,
                                        uint64_t max_retained_bytes);
size_t moq_media_receiver_test_track_count(const moq_media_receiver_t *r);
uint64_t moq_media_receiver_test_catalog_drops(const moq_media_receiver_t *r);
bool moq_media_receiver_test_is_fatal(const moq_media_receiver_t *r);
uint64_t moq_media_receiver_test_fatal_code(const moq_media_receiver_t *r);

/* -- a counting allocator that can refuse one nominated request ---------- */

typedef struct {
    int64_t  live;       /* blocks allocated and not yet freed */
    uint64_t calls;      /* alloc requests seen */
    uint64_t refuse;     /* the request number to refuse; 0 refuses none */
} counting_t;

static void *counting_alloc(size_t size, void *ctx)
{
    counting_t *c = ctx;
    if (++c->calls == c->refuse) return NULL;
    void *p = malloc(size);
    if (p) c->live++;
    return p;
}

static void *counting_realloc(void *ptr, size_t old_size, size_t new_size, void *ctx)
{
    (void)old_size;
    counting_t *c = ctx;
    void *p = realloc(ptr, new_size);
    if (p && !ptr) c->live++;
    return p;
}

static void counting_free(void *ptr, size_t size, void *ctx)
{
    (void)size;
    counting_t *c = ctx;
    if (ptr) { c->live--; free(ptr); }
}

/* -- fixtures --------------------------------------------------------------- */

static moq_media_receiver_t *new_receiver(counting_t *c)
{
    moq_alloc_t alloc = { c, counting_alloc, counting_realloc, counting_free };
    static const moq_bytes_t ns_parts[] = {
        { (const uint8_t *)"svc", 3 }, { (const uint8_t *)"demo", 4 },
    };
    moq_media_receiver_cfg_t cfg;
    moq_media_receiver_cfg_init_live(&cfg);
    cfg.namespace_.parts = ns_parts;
    cfg.namespace_.count = 2;
    cfg.auto_subscribe = false;
    moq_media_receiver_test_set_alloc(&alloc);
    moq_media_receiver_t *r = moq_media_receiver_test_new_cfg(&cfg);
    moq_media_receiver_test_set_alloc(NULL);
    if (r) moq_media_receiver_test_set_transport_version(r, MOQ_VERSION_DRAFT_16);
    return r;
}

/* One video track with the given packaging and codec whose init data is
 * `init` (inline in the initDataList, referenced by initRef). */
static void catalog(char *out, size_t cap, const char *packaging, const char *codec,
                    const uint8_t *init, size_t init_len, bool live)
{
    moq_rcbuf_t *b64 = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_msf_encode_init_data(
        moq_alloc_default(), (moq_bytes_t){ init, init_len }, &b64), (int)MOQ_OK);
    int n = snprintf(out, cap,
        "{\"version\":\"1\",\"generatedAt\":1,"
        "\"tracks\":[{\"name\":\"v\",\"packaging\":\"%s\",\"isLive\":%s,%s"
        "\"role\":\"video\",\"codec\":\"%s\",\"timescale\":90000,\"initRef\":\"i1\"}],"
        "\"initDataList\":[{\"id\":\"i1\",\"type\":\"inline\",\"data\":\"%.*s\"}]}",
        packaging, live ? "true" : "false", live ? "" : "\"trackDuration\":60000,",
        codec, b64 ? (int)moq_rcbuf_len(b64) : 0,
        b64 ? (const char *)moq_rcbuf_data(b64) : "");
    MOQ_TEST_CHECK(n > 0 && (size_t)n < cap);
    moq_rcbuf_decref(b64);
}

/* Ingest a catalog and return the handle of its first TRACK_ADDED. */
static moq_media_track_t *ingest_one(moq_media_receiver_t *r, uint64_t group,
                                     const char *json)
{
    moq_media_receiver_test_ingest(r, group, 0, json, strlen(json));
    moq_media_track_event_kind_t kind;
    moq_media_track_t *track = NULL, *added = NULL;
    while (moq_media_receiver_test_poll(r, &kind, &track))
        if (kind == MOQ_MEDIA_TRACK_ADDED && !added) added = track;
    return added;
}

static bool span_within(moq_bytes_t inner, moq_bytes_t outer)
{
    return inner.len > 0 && inner.data >= outer.data &&
           inner.data + inner.len <= outer.data + outer.len;
}

#define FOURCC(a, b, c, d) (((uint32_t)(a) << 24) | ((uint32_t)(b) << 16) | \
                            ((uint32_t)(c) << 8) | (uint32_t)(d))

/* -- rows --------------------------------------------------------------------- */

/* A clear CMAF track: the parsed record, whose extradata is the common span
 * inside the resolved init segment. */
static void test_cmaf_clear(void)
{
    counting_t c = { 0 };
    moq_media_receiver_t *r = new_receiver(&c);
    MOQ_TEST_CHECK(r != NULL);
    if (!r) return;
    uint8_t init[256];
    size_t init_len = sender_test_cmaf_init(init, 5);
    char json[2048];
    catalog(json, sizeof(json), "cmaf", "avc1.42e01e", init, init_len, true);
    moq_media_track_t *t = ingest_one(r, 0, json);
    const moq_media_track_desc_t *d = moq_media_track_desc_get(t);
    MOQ_TEST_CHECK(d != NULL);
    if (d) {
        MOQ_TEST_CHECK_EQ_INT((int)d->info.packaging, (int)MOQ_MEDIA_PACKAGING_CMAF);
        MOQ_TEST_CHECK(d->init_data.len == init_len &&
                       memcmp(d->init_data.data, init, init_len) == 0);
        const moq_cmaf_init_info_t *rec = d->cmaf;
        MOQ_TEST_CHECK(rec != NULL);
        if (rec) {
            MOQ_TEST_CHECK(rec->struct_size == sizeof(*rec));
            MOQ_TEST_CHECK(rec->sample_entry == FOURCC('a', 'v', 'c', '1'));
            MOQ_TEST_CHECK(rec->codec_kind == MOQ_CMAF_CODEC_AVC);
            MOQ_TEST_CHECK(rec->track_id == 5 && rec->timescale == 90000);
            MOQ_TEST_CHECK(!rec->has_cenc);
            MOQ_TEST_CHECK(d->codec_config.data == rec->codec_config.data &&
                           d->codec_config.len == rec->codec_config.len);
            MOQ_TEST_CHECK(span_within(d->codec_config, d->init_data));
        }
    }
    moq_media_receiver_test_free(r);
    MOQ_TEST_CHECK_EQ_INT((int)c.live, 0);
    MOQ_TEST_PASS("cmaf_record.clear");
}

/* A CENC protected CMAF track: the record carries the original format from
 * frma and the tenc parameters. */
static void test_cmaf_protected(void)
{
    counting_t c = { 0 };
    moq_media_receiver_t *r = new_receiver(&c);
    MOQ_TEST_CHECK(r != NULL);
    if (!r) return;
    const uint8_t avcc[] = { 0x01, 0x64, 0x00, 0x1f };
    uint8_t kid[16];
    for (int i = 0; i < 16; i++) kid[i] = (uint8_t)(0xa0 + i);
    uint8_t init[1024];
    size_t init_len = moq_test_build_cmaf_init_cenc(init, 2, 90000, 640, 360,
        avcc, sizeof(avcc), "cbcs", 1, 8, kid, 0, MOQ_TEST_CENC_OK);
    char json[4096];
    catalog(json, sizeof(json), "cmaf", "avc1.64001f", init, init_len, true);
    const moq_media_track_desc_t *d = moq_media_track_desc_get(ingest_one(r, 0, json));
    const moq_cmaf_init_info_t *rec = d ? d->cmaf : NULL;
    MOQ_TEST_CHECK(rec != NULL);
    if (rec) {
        MOQ_TEST_CHECK(rec->sample_entry == FOURCC('a', 'v', 'c', '1'));
        MOQ_TEST_CHECK(rec->has_cenc && rec->default_is_protected == 1 &&
                       rec->default_per_sample_iv_size == 8);
        MOQ_TEST_CHECK(rec->scheme.len == 4 && memcmp(rec->scheme.data, "cbcs", 4) == 0);
        MOQ_TEST_CHECK(rec->default_kid.len == 16 &&
                       memcmp(rec->default_kid.data, kid, 16) == 0);
        MOQ_TEST_CHECK(span_within(rec->default_kid, d->init_data));
        MOQ_TEST_CHECK(d->codec_config.len == sizeof(avcc) &&
                       d->codec_config.data == rec->codec_config.data);
    }
    moq_media_receiver_test_free(r);
    MOQ_TEST_CHECK_EQ_INT((int)c.live, 0);
    MOQ_TEST_PASS("cmaf_record.protected");
}

/* A LOC track has extradata and no record; a CMAF track whose header does
 * not parse has neither, and is still announced. */
static void test_loc_and_malformed(void)
{
    counting_t c = { 0 };
    moq_media_receiver_t *r = new_receiver(&c);
    MOQ_TEST_CHECK(r != NULL);
    if (!r) return;
    const uint8_t extradata[] = { 0x01, 0x42, 0xc0, 0x1e, 0xff };
    char json[2048];
    catalog(json, sizeof(json), "loc", "avc1.42c01e", extradata, sizeof(extradata), true);
    const moq_media_track_desc_t *d = moq_media_track_desc_get(ingest_one(r, 0, json));
    MOQ_TEST_CHECK(d != NULL);
    if (d) {
        MOQ_TEST_CHECK(d->cmaf == NULL);
        MOQ_TEST_CHECK(d->codec_config.len == sizeof(extradata) &&
                       memcmp(d->codec_config.data, extradata, sizeof(extradata)) == 0);
        MOQ_TEST_CHECK(d->codec_config.data == d->init_data.data &&
                       d->codec_config.len == d->init_data.len);
    }
    moq_media_receiver_test_free(r);
    MOQ_TEST_CHECK_EQ_INT((int)c.live, 0);

    counting_t c2 = { 0 };
    r = new_receiver(&c2);
    MOQ_TEST_CHECK(r != NULL);
    if (!r) return;
    const uint8_t not_moov[] = { 0, 0, 0, 8, 'f', 'r', 'e', 'e' };
    catalog(json, sizeof(json), "cmaf", "avc1.42c01e", not_moov, sizeof(not_moov), true);
    d = moq_media_track_desc_get(ingest_one(r, 0, json));
    MOQ_TEST_CHECK(d != NULL);
    if (d) {
        MOQ_TEST_CHECK(d->name.len == 1 && d->name.data[0] == 'v');
        MOQ_TEST_CHECK(d->cmaf == NULL);
        MOQ_TEST_CHECK(d->codec_config.len == 0 && d->init_data.len == 0);
        MOQ_TEST_CHECK_EQ_INT((int)d->info.packaging, 0);
    }
    moq_media_receiver_test_free(r);
    MOQ_TEST_CHECK_EQ_INT((int)c2.live, 0);
    MOQ_TEST_PASS("cmaf_record.loc_and_malformed");
}

/* The record costs no allocation of its own: ingesting the CMAF track makes
 * exactly as many allocations as the same track and init bytes declared LOC,
 * and refusing any one of them leaks nothing. */
static void test_allocations(void)
{
    uint8_t init[256];
    size_t init_len = sender_test_cmaf_init(init, 1);
    char cmaf_json[2048], loc_json[2048];
    catalog(cmaf_json, sizeof(cmaf_json), "cmaf", "avc1.42e01e", init, init_len, true);
    catalog(loc_json, sizeof(loc_json), "loc", "avc1.42e01e", init, init_len, true);
    uint64_t calls[2] = { 0, 0 };
    const char *const jsons[2] = { cmaf_json, loc_json };
    for (int k = 0; k < 2; k++) {
        counting_t c = { 0 };
        moq_media_receiver_t *r = new_receiver(&c);
        MOQ_TEST_CHECK(r != NULL);
        if (!r) return;
        uint64_t before = c.calls;
        moq_media_track_t *t = ingest_one(r, 0, jsons[k]);
        calls[k] = c.calls - before;
        const moq_media_track_desc_t *d = moq_media_track_desc_get(t);
        MOQ_TEST_CHECK(d != NULL && (d->cmaf != NULL) == (k == 0));
        moq_media_receiver_test_free(r);
        MOQ_TEST_CHECK_EQ_INT((int)c.live, 0);
    }
    printf("ORACLE ingest allocations cmaf=%llu loc=%llu\n",
           (unsigned long long)calls[0], (unsigned long long)calls[1]);
    MOQ_TEST_CHECK(calls[0] > 0 && calls[0] == calls[1]);

    for (uint64_t refuse = 1; refuse <= calls[0]; refuse++) {
        counting_t c = { 0 };
        moq_media_receiver_t *r = new_receiver(&c);
        MOQ_TEST_CHECK(r != NULL);
        if (!r) return;
        c.refuse = c.calls + refuse;
        moq_media_track_t *t = ingest_one(r, 0, cmaf_json);
        const moq_media_track_desc_t *d = moq_media_track_desc_get(t);
        /* Whatever survived the refusal is whole: a record, when present,
         * is complete and its extradata is the description's. */
        if (d && d->cmaf) {
            MOQ_TEST_CHECK(d->cmaf->struct_size == sizeof(*d->cmaf));
            MOQ_TEST_CHECK(d->codec_config.data == d->cmaf->codec_config.data);
        }
        moq_media_receiver_test_free(r);
        if (c.live != 0)
            printf("leak after refusing ingest allocation %llu: %lld\n",
                   (unsigned long long)refuse, (long long)c.live);
        MOQ_TEST_CHECK_EQ_INT((int)c.live, 0);
    }
    MOQ_TEST_PASS("cmaf_record.allocations");
}

/* A description copy borrows the record; a live-to-VOD conversion rewrites
 * the mutable fields and leaves the record in place, unchanged. */
static void test_copy_and_vod(void)
{
    counting_t c = { 0 };
    moq_media_receiver_t *r = new_receiver(&c);
    MOQ_TEST_CHECK(r != NULL);
    if (!r) return;
    uint8_t init[256];
    size_t init_len = sender_test_cmaf_init(init, 3);
    char live[2048], vod[2048];
    catalog(live, sizeof(live), "cmaf", "avc1.42e01e", init, init_len, true);
    catalog(vod, sizeof(vod), "cmaf", "avc1.42e01e", init, init_len, false);
    moq_media_track_t *t = ingest_one(r, 0, live);
    const moq_media_track_desc_t *d = moq_media_track_desc_get(t);
    const moq_cmaf_init_info_t *rec = d ? d->cmaf : NULL;
    MOQ_TEST_CHECK(rec != NULL);
    if (!rec) { moq_media_receiver_test_free(r); return; }
    moq_cmaf_init_info_t snap;
    memcpy(&snap, rec, sizeof(snap));

    moq_media_track_desc_t copy;
    memset(&copy, 0xEE, sizeof(copy));
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_receiver_track_desc_copy(r, t, &copy, sizeof(copy)),
                          (int)MOQ_OK);
    MOQ_TEST_CHECK(copy.cmaf == rec);
    MOQ_TEST_CHECK(copy.codec_config.data == d->codec_config.data);
    /* The copy's floor holds the pointer. */
    union {
        moq_media_track_desc_t desc;
        unsigned char raw[sizeof(moq_media_track_desc_t)];
    } floor_copy;
    memset(floor_copy.raw, 0xEE, sizeof(floor_copy.raw));
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_receiver_track_desc_copy(
        r, t, &floor_copy.desc, MOQ_MEDIA_TRACK_DESC_V0_SIZE), (int)MOQ_OK);
    MOQ_TEST_CHECK(floor_copy.desc.struct_size == MOQ_MEDIA_TRACK_DESC_V0_SIZE);
    MOQ_TEST_CHECK(floor_copy.desc.cmaf == rec);
    for (size_t i = MOQ_MEDIA_TRACK_DESC_V0_SIZE; i < sizeof(floor_copy.raw); i++)
        MOQ_TEST_CHECK(floor_copy.raw[i] == 0xEE);

    moq_media_receiver_test_ingest(r, 1, 0, vod, strlen(vod));
    moq_media_track_event_kind_t kind;
    moq_media_track_t *ev_track = NULL;
    bool updated = false;
    while (moq_media_receiver_test_poll(r, &kind, &ev_track))
        if (kind == MOQ_MEDIA_TRACK_UPDATED && ev_track == t) updated = true;
    MOQ_TEST_CHECK(updated);
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_receiver_track_desc_copy(r, t, &copy, sizeof(copy)),
                          (int)MOQ_OK);
    MOQ_TEST_CHECK(!copy.is_live && copy.has_track_duration);
    MOQ_TEST_CHECK(copy.cmaf == rec && d->cmaf == rec);
    MOQ_TEST_CHECK(memcmp(rec, &snap, sizeof(snap)) == 0);
    moq_media_receiver_test_free(r);
    MOQ_TEST_CHECK_EQ_INT((int)c.live, 0);
    MOQ_TEST_PASS("cmaf_record.copy_and_vod");
}

/* -- the retained-byte charge -------------------------------------------------- */

/* One live video track `name` with the given packaging whose init data is
 * `init`: inline as the track's initData, or in the initDataList and pulled
 * in by initRef. */
static void cap_catalog(char *out, size_t cap, const char *name, const char *packaging,
                        const uint8_t *init, size_t init_len, bool by_ref)
{
    moq_rcbuf_t *b64 = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_msf_encode_init_data(
        moq_alloc_default(), (moq_bytes_t){ init, init_len }, &b64), (int)MOQ_OK);
    int b64_len = b64 ? (int)moq_rcbuf_len(b64) : 0;
    const char *b64_data = b64 ? (const char *)moq_rcbuf_data(b64) : "";
    int n = by_ref
        ? snprintf(out, cap,
              "{\"version\":\"1\",\"generatedAt\":1,"
              "\"tracks\":[{\"name\":\"%s\",\"packaging\":\"%s\",\"isLive\":true,"
              "\"role\":\"video\",\"codec\":\"avc1.42e01e\",\"timescale\":90000,"
              "\"initRef\":\"i1\"}],"
              "\"initDataList\":[{\"id\":\"i1\",\"type\":\"inline\",\"data\":\"%.*s\"}]}",
              name, packaging, b64_len, b64_data)
        : snprintf(out, cap,
              "{\"version\":\"1\",\"generatedAt\":1,"
              "\"tracks\":[{\"name\":\"%s\",\"packaging\":\"%s\",\"isLive\":true,"
              "\"role\":\"video\",\"codec\":\"avc1.42e01e\",\"timescale\":90000,"
              "\"initData\":\"%.*s\"}]}",
              name, packaging, b64_len, b64_data);
    MOQ_TEST_CHECK(n > 0 && (size_t)n < cap);
    moq_rcbuf_decref(b64);
}

/* Ingest a catalog and count the track events it announces; `added` is the
 * handle of the first TRACK_ADDED. */
typedef struct {
    moq_media_track_t *added;
    int                added_count, removed_count;
} ingest_log_t;

static ingest_log_t ingest_counted(moq_media_receiver_t *r, uint64_t group, const char *json)
{
    ingest_log_t log = { NULL, 0, 0 };
    moq_media_receiver_test_ingest(r, group, 0, json, strlen(json));
    moq_media_track_event_kind_t kind;
    moq_media_track_t *track = NULL;
    while (moq_media_receiver_test_poll(r, &kind, &track)) {
        if (kind == MOQ_MEDIA_TRACK_ADDED) {
            if (!log.added) log.added = track;
            log.added_count++;
        } else if (kind == MOQ_MEDIA_TRACK_REMOVED) {
            log.removed_count++;
        }
    }
    return log;
}

/* Whether a fresh receiver with retained-byte cap `cap` adopts `json` as its
 * first catalog. A refusal is the first-catalog fatal with nothing announced
 * or retained; either way nothing leaks. */
static bool first_catalog_fits(const char *json, uint64_t cap)
{
    counting_t c = { 0 };
    moq_media_receiver_t *r = new_receiver(&c);
    MOQ_TEST_CHECK(r != NULL);
    if (!r) return false;
    moq_media_receiver_test_set_limits(r, 1000, 1000, cap);
    ingest_log_t log = ingest_counted(r, 0, json);
    bool fits = !moq_media_receiver_test_is_fatal(r);
    MOQ_TEST_CHECK_EQ_INT(log.added_count, fits ? 1 : 0);
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_receiver_test_track_count(r), fits ? 1 : 0);
    if (!fits)
        MOQ_TEST_CHECK(moq_media_receiver_test_fatal_code(r) ==
                       MOQ_MEDIA_RECEIVER_FATAL_CATALOG_UNUSABLE);
    moq_media_receiver_test_free(r);
    MOQ_TEST_CHECK_EQ_INT((int)c.live, 0);
    return fits;
}

/* The least cap under which `json` is adopted as a first catalog: the charge
 * for its one track, found by bisection between a refusing and an adopting
 * cap. */
static uint64_t first_catalog_charge(const char *json)
{
    uint64_t lo = 0, hi = UINT64_C(1) << 20;
    MOQ_TEST_CHECK(!first_catalog_fits(json, lo));
    MOQ_TEST_CHECK(first_catalog_fits(json, hi));
    while (hi - lo > 1) {
        uint64_t mid = lo + (hi - lo) / 2;
        if (first_catalog_fits(json, mid)) hi = mid;
        else lo = mid;
    }
    return hi;
}

/* The record is charged to the retained-byte cap. A CMAF track's charge is
 * its LOC twin's -- same name, codec and init bytes -- plus the one byte the
 * longer packaging name copies, plus the record: one byte less refuses the
 * first catalog whole. A later catalog replacing the track is preflighted
 * against the record charges of both the retained track and the new one,
 * and dropped whole when they do not fit: the retained track and its record
 * are untouched and nothing is announced. Adopted, the removed track keeps
 * its handle, its record and its charge. Inline initData and an initRef
 * charge alike. */
static void test_retained_charge(void)
{
    uint8_t init[256];
    size_t init_len = sender_test_cmaf_init(init, 1);
    for (int by_ref = 0; by_ref < 2; by_ref++) {
        const char *how = by_ref ? "initRef" : "inline";
        char cmaf_json[2048], loc_json[2048], next[2048], more[2048];
        cap_catalog(cmaf_json, sizeof(cmaf_json), "t0", "cmaf", init, init_len, by_ref);
        cap_catalog(loc_json, sizeof(loc_json), "t0", "loc", init, init_len, by_ref);
        cap_catalog(next, sizeof(next), "t1", "cmaf", init, init_len, by_ref);
        cap_catalog(more, sizeof(more), "t2", "loc", init, init_len, by_ref);

        uint64_t loc = first_catalog_charge(loc_json);
        uint64_t cmaf = loc + (sizeof("cmaf") - sizeof("loc")) + sizeof(moq_cmaf_init_info_t);
        printf("ORACLE %s charge loc=%llu cmaf=%llu record=%zu\n", how,
               (unsigned long long)loc, (unsigned long long)cmaf,
               sizeof(moq_cmaf_init_info_t));
        MOQ_TEST_CHECK(!first_catalog_fits(cmaf_json, cmaf - 1));
        MOQ_TEST_CHECK(first_catalog_fits(cmaf_json, cmaf));

        for (int exact = 0; exact < 2; exact++) {
            counting_t c = { 0 };
            moq_media_receiver_t *r = new_receiver(&c);
            MOQ_TEST_CHECK(r != NULL);
            if (!r) return;
            moq_media_receiver_test_set_limits(r, 1000, 1000, 2 * cmaf - (exact ? 0 : 1));
            ingest_log_t first = ingest_counted(r, 0, cmaf_json);
            const moq_media_track_desc_t *d0 = moq_media_track_desc_get(first.added);
            const moq_cmaf_init_info_t *rec0 = d0 ? d0->cmaf : NULL;
            MOQ_TEST_CHECK(rec0 != NULL);
            if (!rec0) { moq_media_receiver_test_free(r); return; }
            moq_cmaf_init_info_t snap;
            memcpy(&snap, rec0, sizeof(snap));

            uint64_t drops = moq_media_receiver_test_catalog_drops(r);
            ingest_log_t replaced = ingest_counted(r, 1, next);
            MOQ_TEST_CHECK(!moq_media_receiver_test_is_fatal(r));
            MOQ_TEST_CHECK(d0->cmaf == rec0 && memcmp(rec0, &snap, sizeof(snap)) == 0);
            if (!exact) {
                MOQ_TEST_CHECK(moq_media_receiver_test_catalog_drops(r) == drops + 1);
                MOQ_TEST_CHECK_EQ_INT(replaced.added_count, 0);
                MOQ_TEST_CHECK_EQ_INT(replaced.removed_count, 0);
                MOQ_TEST_CHECK_EQ_INT((int)moq_media_receiver_test_track_count(r), 1);
            } else {
                MOQ_TEST_CHECK(moq_media_receiver_test_catalog_drops(r) == drops);
                MOQ_TEST_CHECK_EQ_INT(replaced.added_count, 1);
                MOQ_TEST_CHECK_EQ_INT(replaced.removed_count, 1);
                MOQ_TEST_CHECK_EQ_INT((int)moq_media_receiver_test_track_count(r), 2);
                const moq_media_track_desc_t *d1 = moq_media_track_desc_get(replaced.added);
                MOQ_TEST_CHECK(d1 != NULL && d1->cmaf != NULL && d1->cmaf != rec0);
                /* The cap is spent: t0's charge outlives its removal, so not
                 * even a LOC track fits. */
                ingest_log_t over = ingest_counted(r, 2, more);
                MOQ_TEST_CHECK(moq_media_receiver_test_catalog_drops(r) == drops + 1);
                MOQ_TEST_CHECK_EQ_INT(over.added_count, 0);
                MOQ_TEST_CHECK_EQ_INT((int)moq_media_receiver_test_track_count(r), 2);
                MOQ_TEST_CHECK(d0->cmaf == rec0 && memcmp(rec0, &snap, sizeof(snap)) == 0);
            }
            moq_media_receiver_test_free(r);
            MOQ_TEST_CHECK_EQ_INT((int)c.live, 0);
        }
    }
    MOQ_TEST_PASS("cmaf_record.retained_charge");
}

int main(void)
{
    test_cmaf_clear();
    test_cmaf_protected();
    test_loc_and_malformed();
    test_allocations();
    test_copy_and_vod();
    test_retained_charge();
    printf("%s: %d failures\n", failures ? "FAIL" : "PASS", failures);
    return failures ? 1 : 0;
}
