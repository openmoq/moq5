/*
 * MSF catalog support for the "locmaf" packaging
 * (draft-einarsson-moq-locmaf-01 section 5): the locmafVersion record in
 * both catalog revisions, parse leniency, the subscriber's version check,
 * strict encode (independent and delta tracks), the receive-path re-encode
 * of an applied delta, clone and update merges, and allocation failure.
 */

#include <moq/msf.h>
#include <moq/rcbuf.h>
#ifdef MOQ_TEST_HAVE_LOCMAF
#include <moq/locmaf.h>
#endif

#include <stddef.h>
#include <stdint.h>
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

#define CHECK_RC(expr, want) do { \
    moq_result_t _rc = (expr); \
    if (_rc != (want)) { \
        fprintf(stderr, "FAIL: %s:%d: %s == %d, expected %d\n", \
                __FILE__, __LINE__, #expr, _rc, (int)(want)); \
        failures++; \
    } \
} while (0)

/* -- Counting allocator: every test ends at a zero balance. -------------- */

typedef struct {
    int64_t  bytes;
    int64_t  blocks;
    uint64_t calls;
    uint64_t fail_at;     /* 1-based allocation to refuse; 0 = never */
} count_ctx_t;

static void *c_alloc(size_t n, void *ctx)
{
    count_ctx_t *c = (count_ctx_t *)ctx;
    c->calls++;
    if (c->fail_at && c->calls == c->fail_at) return NULL;
    void *p = malloc(n ? n : 1);
    if (p) { c->bytes += (int64_t)n; c->blocks++; }
    return p;
}

static void c_free(void *p, size_t n, void *ctx)
{
    count_ctx_t *c = (count_ctx_t *)ctx;
    if (!p) return;
    c->bytes -= (int64_t)n;
    c->blocks--;
    free(p);
}

static void *c_realloc(void *p, size_t old_n, size_t new_n, void *ctx)
{
    count_ctx_t *c = (count_ctx_t *)ctx;
    c->calls++;
    if (c->fail_at && c->calls == c->fail_at) return NULL;
    void *q = realloc(p, new_n ? new_n : 1);
    if (q) c->bytes += (int64_t)new_n - (int64_t)old_n;
    return q;
}

static count_ctx_t g_count;
static moq_alloc_t g_alloc = {
    .ctx = &g_count, .alloc = c_alloc, .realloc = c_realloc, .free = c_free,
};

static void check_balance(const char *where)
{
    if (g_count.bytes != 0 || g_count.blocks != 0) {
        fprintf(stderr, "FAIL: %s: allocator balance %lld bytes, %lld blocks\n",
                where, (long long)g_count.bytes, (long long)g_count.blocks);
        failures++;
    }
    g_count.bytes = 0;
    g_count.blocks = 0;
    g_count.calls = 0;
    g_count.fail_at = 0;
}

/* -- Helpers --------------------------------------------------------------- */

static moq_bytes_t lit(const char *s)
{
    return (moq_bytes_t){ (const uint8_t *)s, strlen(s) };
}

static bool bv_eq(moq_bytes_t a, const char *s)
{
    size_t n = strlen(s);
    return a.len == n && (n == 0 || (a.data && memcmp(a.data, s, n) == 0));
}

static bool buf_contains(const moq_rcbuf_t *b, const char *needle)
{
    size_t n = strlen(needle), len = moq_rcbuf_len(b);
    const uint8_t *d = moq_rcbuf_data(b);
    for (size_t i = 0; n <= len && i + n <= len; i++)
        if (memcmp(d + i, needle, n) == 0) return true;
    return false;
}

static moq_result_t parse(const char *json, moq_msf_catalog_t *cat)
{
    return moq_msf_catalog_parse(&g_alloc, lit(json), cat);
}

static const moq_msf_track_t *find_track(const moq_msf_catalog_t *c,
                                         const char *name)
{
    for (size_t i = 0; i < c->track_count; i++)
        if (bv_eq(c->tracks[i].name, name)) return &c->tracks[i];
    return NULL;
}

static moq_msf_locmaf_error_t check_reason(const moq_msf_track_t *t,
                                           moq_result_t *rc_out)
{
    moq_msf_locmaf_error_t r = (moq_msf_locmaf_error_t)99;
    moq_result_t rc = moq_msf_track_locmaf_check(t, &r);
    if (rc_out) *rc_out = rc;
    return r;
}

/* Apply a JSON delta to base through the sized entry point. */
static moq_result_t apply_json(const moq_msf_catalog_t *base, const char *delta,
                               moq_msf_catalog_t *out)
{
    moq_msf_catalog_t d;
    moq_result_t rc = parse(delta, &d);
    if (rc != MOQ_OK) {
        memset(out, 0, sizeof(*out));
        out->struct_size = sizeof(*out);
        return rc;
    }
    rc = moq_msf_catalog_apply_delta_sized(&g_alloc, base, &d, NULL, out,
                                           sizeof(*out));
    moq_msf_catalog_cleanup(&g_alloc, &d);
    return rc;
}

#define LM_TRACK(name, extra) \
    "{\"name\":\"" name "\",\"packaging\":\"locmaf\",\"isLive\":true," \
    "\"role\":\"video\",\"codec\":\"avc1.64001f\",\"initRef\":\"v\"" extra "}"

#define CP_LIST \
    "\"contentProtections\":[{\"refID\":\"cp1\"," \
    "\"defaultKID\":[\"01234567-89ab-cdef-0123-456789abcdef\"]," \
    "\"scheme\":\"cbcs\",\"drmSystem\":{" \
    "\"systemID\":\"edef8ba9-79d6-4ace-a3c8-27dcd51d21ed\"," \
    "\"pssh\":\"AAAAP3Bzc2gAAAAA7e+LqXnWSs6jyAAA\"}}]"

#define INIT_LIST \
    "\"initDataList\":[{\"id\":\"v\",\"type\":\"inline\",\"data\":\"AAAA\"}]"

/* An MSF-01 catalog with a protected locmaf track and a cmaf one. */
static const char k_r01[] =
    "{\"version\":\"draft-01\",\"generatedAt\":5," INIT_LIST "," CP_LIST ","
    "\"tracks\":["
    LM_TRACK("video", ",\"locmafVersion\":\"0.3\",\"contentProtectionRefIDs\":[\"cp1\"]")
    ",{\"name\":\"audio\",\"packaging\":\"cmaf\",\"isLive\":true,"
    "\"role\":\"audio\",\"codec\":\"mp4a.40.2\",\"initRef\":\"v\"}]}";

/* The same tracks in an MSF-02 catalog. */
static const char k_r02[] =
    "{\"version\":\"draft-02\",\"generatedAt\":5," INIT_LIST "," CP_LIST ","
    "\"tracks\":["
    LM_TRACK("video", ",\"locmafVersion\":\"0.3\",\"contentProtectionRefIDs\":[\"cp1\"]")
    ",{\"name\":\"audio\",\"packaging\":\"cmaf\",\"isLive\":true,"
    "\"role\":\"audio\",\"codec\":\"mp4a.40.2\",\"initRef\":\"v\"}]}";

/* -- The record ------------------------------------------------------------ */

static void test_record_init_sized(void)
{
    union { moq_msf_locmaf_t rec; unsigned char raw[sizeof(moq_msf_locmaf_t) + 8]; } u;
    memset(u.raw, 0xAB, sizeof(u.raw));
    moq_msf_locmaf_init_sized(&u.rec, 3);            /* below struct_size */
    CHECK(u.raw[0] == 0xAB);
    moq_msf_locmaf_init_sized(NULL, sizeof(u.rec));  /* no-op */
    moq_msf_locmaf_init_sized(&u.rec, sizeof(u.raw));
    CHECK(u.rec.struct_size == sizeof(moq_msf_locmaf_t));
    CHECK(!u.rec.has_version && u.rec.malformed == 0 && u.rec.version.len == 0);
    for (size_t i = sizeof(moq_msf_locmaf_t); i < sizeof(u.raw); i++)
        CHECK(u.raw[i] == 0xAB);
    memset(u.raw, 0xAB, sizeof(u.raw));
    moq_msf_locmaf_init_sized(&u.rec, MOQ_MSF_LOCMAF_V0_SIZE - 1);
    CHECK(u.rec.struct_size == MOQ_MSF_LOCMAF_V0_SIZE - 1);
    CHECK(MOQ_MSF_LOCMAF_V0_SIZE ==
          offsetof(moq_msf_locmaf_t, malformed) + sizeof(uint32_t));
    CHECK(MOQ_MSF_EXT_LOCMAF == 3u);
    CHECK(strcmp(MOQ_MSF_LOCMAF_VERSION, "0.3") == 0);
#ifdef MOQ_TEST_HAVE_LOCMAF
    /* The catalog's version is the one the codec implements. */
    CHECK(strcmp(MOQ_MSF_LOCMAF_VERSION, MOQ_LOCMAF_VERSION) == 0);
#endif
    check_balance("record_init_sized");
}

/* -- Parse, round trip and both revisions ---------------------------------- */

static void test_parse_round_trip(const char *json, moq_msf_revision_t rev)
{
    moq_msf_catalog_t cat, again;
    CHECK_RC(parse(json, &cat), MOQ_OK);
    CHECK(cat.revision == rev);
    const moq_msf_track_t *v = find_track(&cat, "video");
    const moq_msf_track_t *a = find_track(&cat, "audio");
    CHECK(v && a);
    if (!v || !a) { moq_msf_catalog_cleanup(&g_alloc, &cat); return; }
    const moq_msf_locmaf_t *m = moq_msf_track_locmaf(v);
    CHECK(m && m->struct_size == sizeof(moq_msf_locmaf_t) && m->has_version &&
          bv_eq(m->version, "0.3") && m->malformed == 0);
    CHECK(moq_msf_ext_locmaf(v->extensions, v->extension_count) == m);
    CHECK(moq_msf_track_locmaf(a) == NULL);
    CHECK(v->has_init_ref && bv_eq(v->init_ref, "v"));
    CHECK(v->cp_ref_id_count == 1 && bv_eq(v->cp_ref_ids[0], "cp1"));
    moq_result_t rc;
    CHECK(check_reason(v, &rc) == MOQ_MSF_LOCMAF_OK && rc == MOQ_OK);
    CHECK(check_reason(a, &rc) == MOQ_MSF_LOCMAF_ERR_NOT_LOCMAF && rc == MOQ_ERR_INVAL);

    moq_rcbuf_t *json_out = NULL;
    CHECK_RC(moq_msf_catalog_encode(&g_alloc, &cat, &json_out), MOQ_OK);
    if (json_out) {
        CHECK(buf_contains(json_out, "\"locmafVersion\":\"0.3\""));
        CHECK(buf_contains(json_out, "\"packaging\":\"locmaf\""));
        moq_bytes_t jb = { moq_rcbuf_data(json_out), moq_rcbuf_len(json_out) };
        CHECK_RC(moq_msf_catalog_parse(&g_alloc, jb, &again), MOQ_OK);
        const moq_msf_track_t *v2 = find_track(&again, "video");
        const moq_msf_locmaf_t *m2 = v2 ? moq_msf_track_locmaf(v2) : NULL;
        CHECK(m2 && m2->has_version && bv_eq(m2->version, "0.3"));
        CHECK(again.revision == rev);
        moq_msf_catalog_cleanup(&g_alloc, &again);
        moq_rcbuf_decref(json_out);
    }
    moq_msf_catalog_cleanup(&g_alloc, &cat);
    check_balance("parse_round_trip");
}

/* The same track in a draft-01 and a draft-02 catalog parses to the same
 * record; the MSF-01 extent (the exported legacy parse) records it too; a
 * packagingMetadata key on a locmaf track leaves the record alone. */
static void test_both_revisions(void)
{
    moq_msf_catalog_t c1, c2;
    CHECK_RC(parse(k_r01, &c1), MOQ_OK);
    CHECK_RC(parse(k_r02, &c2), MOQ_OK);
    const moq_msf_locmaf_t *m1 = moq_msf_track_locmaf(find_track(&c1, "video"));
    const moq_msf_locmaf_t *m2 = moq_msf_track_locmaf(find_track(&c2, "video"));
    CHECK(m1 && m2 && m1->has_version == m2->has_version &&
          m1->malformed == m2->malformed && m1->version.len == m2->version.len &&
          memcmp(m1->version.data, m2->version.data, m1->version.len) == 0);
    moq_msf_catalog_cleanup(&g_alloc, &c1);
    moq_msf_catalog_cleanup(&g_alloc, &c2);

    /* The MSF-01 extent. */
    moq_msf_catalog_t legacy;
    CHECK_RC(moq_msf_catalog_parse_sized(&g_alloc, lit(k_r01), &legacy,
                                         MOQ_MSF_CATALOG_SIZE_THROUGH_VERSION_FORM),
             MOQ_OK);
    const moq_msf_locmaf_t *ml = moq_msf_track_locmaf(find_track(&legacy, "video"));
    CHECK(ml && ml->has_version && bv_eq(ml->version, "0.3"));
    moq_msf_catalog_cleanup(&g_alloc, &legacy);

    /* packagingMetadata (MSF-02) on a locmaf track: the supplement holds it,
     * the LOCMAF record is unchanged. */
    static const char pm[] =
        "{\"version\":\"draft-02\"," INIT_LIST ",\"tracks\":["
        LM_TRACK("video", ",\"locmafVersion\":\"0.3\","
                 "\"packagingMetadata\":\"streamMapping=object\"") "]}";
    moq_msf_catalog_t c3;
    CHECK_RC(parse(pm, &c3), MOQ_OK);
    const moq_msf_track_t *t3 = find_track(&c3, "video");
    const moq_msf_locmaf_t *m3 = moq_msf_track_locmaf(t3);
    CHECK(m3 && bv_eq(m3->version, "0.3") && m3->malformed == 0);
    const moq_msf_track_supplement_t *s3 = moq_msf_track_supplement(t3);
    CHECK(s3 && s3->has_packaging_metadata);
    CHECK(t3 && t3->extension_count == 2 &&
          t3->extensions[0].kind == MOQ_MSF_EXT_SUPPLEMENT &&
          t3->extensions[1].kind == MOQ_MSF_EXT_LOCMAF);
    CHECK(check_reason(t3, NULL) == MOQ_MSF_LOCMAF_OK);
    moq_msf_catalog_cleanup(&g_alloc, &c3);
    check_balance("both_revisions");
}

/* -- Leniency and the subscriber's check ----------------------------------- */

static void test_check_and_leniency(void)
{
    static const char json[] =
        "{\"version\":\"draft-01\"," INIT_LIST ",\"tracks\":["
        LM_TRACK("ok", ",\"locmafVersion\":\"0.3\"") ","
        LM_TRACK("number", ",\"locmafVersion\":3") ","
        LM_TRACK("null", ",\"locmafVersion\":null") ","
        LM_TRACK("empty", ",\"locmafVersion\":\"\"") ","
        LM_TRACK("future", ",\"locmafVersion\":\"0.4\"") ","
        LM_TRACK("missing", "") ","
        "{\"name\":\"stray\",\"packaging\":\"cmaf\",\"isLive\":true,"
        "\"role\":\"video\",\"codec\":\"avc1.64001f\",\"locmafVersion\":\"0.3\"}]}";
    moq_msf_catalog_t cat;
    CHECK_RC(parse(json, &cat), MOQ_OK);   /* nothing here fails the parse */
    moq_result_t rc;

    CHECK(check_reason(find_track(&cat, "ok"), &rc) == MOQ_MSF_LOCMAF_OK && rc == MOQ_OK);

    const moq_msf_track_t *num = find_track(&cat, "number");
    const moq_msf_locmaf_t *mn = moq_msf_track_locmaf(num);
    CHECK(mn && mn->has_version && mn->malformed == MOQ_MSF_LOCMAF_F_VERSION &&
          mn->version.len == 0);
    CHECK(check_reason(num, &rc) == MOQ_MSF_LOCMAF_ERR_VERSION_MALFORMED &&
          rc == MOQ_ERR_PROTO);
    const moq_msf_locmaf_t *mz = moq_msf_track_locmaf(find_track(&cat, "null"));
    CHECK(mz && mz->malformed == MOQ_MSF_LOCMAF_F_VERSION);
    CHECK(check_reason(find_track(&cat, "empty"), &rc) ==
              MOQ_MSF_LOCMAF_ERR_VERSION_MALFORMED && rc == MOQ_ERR_PROTO);
    CHECK(check_reason(find_track(&cat, "future"), &rc) ==
              MOQ_MSF_LOCMAF_ERR_VERSION_UNSUPPORTED && rc == MOQ_ERR_UNSUPPORTED);
    const moq_msf_track_t *miss = find_track(&cat, "missing");
    CHECK(moq_msf_track_locmaf(miss) == NULL);
    CHECK(check_reason(miss, &rc) == MOQ_MSF_LOCMAF_ERR_VERSION_MISSING &&
          rc == MOQ_ERR_PROTO);
    /* A version on another packaging is recorded; the check is about
     * LOCMAF tracks only. */
    const moq_msf_track_t *stray = find_track(&cat, "stray");
    CHECK(moq_msf_track_locmaf(stray) != NULL);
    CHECK(check_reason(stray, &rc) == MOQ_MSF_LOCMAF_ERR_NOT_LOCMAF && rc == MOQ_ERR_INVAL);
    CHECK_RC(moq_msf_track_locmaf_check(NULL, NULL), MOQ_ERR_INVAL);
    CHECK_RC(moq_msf_track_locmaf_check(find_track(&cat, "ok"), NULL), MOQ_OK);

    /* The parse is lenient; authoring is not: this catalog does not encode. */
    moq_rcbuf_t *out = NULL;
    CHECK_RC(moq_msf_catalog_encode(&g_alloc, &cat, &out), MOQ_ERR_INVAL);
    CHECK(out == NULL);
    moq_msf_catalog_cleanup(&g_alloc, &cat);

    /* A record below its one extent is refused by the check. */
    moq_msf_locmaf_t small;
    moq_msf_locmaf_init(&small);
    small.has_version = true;
    small.version = lit("0.3");
    small.struct_size = MOQ_MSF_LOCMAF_V0_SIZE - 1;
    moq_msf_track_ext_t ext = { .kind = MOQ_MSF_EXT_LOCMAF, .u = { .locmaf = &small } };
    moq_msf_track_t t;
    memset(&t, 0, sizeof(t));
    t.struct_size = sizeof(t);
    t.name = lit("v");
    t.packaging = lit("locmaf");
    t.extensions = &ext;
    t.extension_count = 1;
    CHECK_RC(moq_msf_track_locmaf_check(&t, NULL), MOQ_ERR_INVAL);
    small.struct_size = MOQ_MSF_LOCMAF_V0_SIZE;
    CHECK_RC(moq_msf_track_locmaf_check(&t, NULL), MOQ_OK);
    check_balance("check_and_leniency");
}

/* -- Strict authoring ------------------------------------------------------ */

typedef struct {
    moq_msf_catalog_t   cat;
    moq_msf_track_t     track;
    moq_msf_track_ext_t ext;
    moq_msf_locmaf_t    rec;
} authored_t;

static void authored_init(authored_t *a, moq_msf_revision_t rev,
                          const char *packaging, bool with_record)
{
    memset(a, 0, sizeof(*a));
    a->cat.struct_size = sizeof(a->cat);
    a->cat.version = MOQ_MSF_VERSION;
    a->cat.revision = rev;
    a->track.struct_size = sizeof(a->track);
    a->track.name = lit("video");
    a->track.packaging = lit(packaging);
    a->track.is_live = true;
    a->track.has_role = true;
    a->track.role = lit("video");
    a->track.has_codec = true;
    a->track.codec = lit("avc1.64001f");
    moq_msf_locmaf_init(&a->rec);
    a->rec.has_version = true;
    a->rec.version = lit(MOQ_MSF_LOCMAF_VERSION);
    a->ext.kind = MOQ_MSF_EXT_LOCMAF;
    a->ext.u.locmaf = &a->rec;
    if (with_record) {
        a->track.extensions = &a->ext;
        a->track.extension_count = 1;
    }
    a->cat.tracks = &a->track;
    a->cat.track_count = 1;
}

static moq_result_t encode_rc(const moq_msf_catalog_t *cat, const char *needle,
                              bool *found)
{
    moq_rcbuf_t *out = NULL;
    moq_result_t rc = moq_msf_catalog_encode(&g_alloc, cat, &out);
    if (found) *found = out && needle && buf_contains(out, needle);
    if (out) moq_rcbuf_decref(out);
    return rc;
}

static void test_strict_encode(void)
{
    static const moq_msf_revision_t revs[] = { MOQ_MSF_REVISION_01,
                                               MOQ_MSF_REVISION_02 };
    for (size_t r = 0; r < 2; r++) {
        authored_t a;
        bool found = false;
        /* Positive: emitted under either revision. */
        authored_init(&a, revs[r], "locmaf", true);
        CHECK_RC(encode_rc(&a.cat, "\"locmafVersion\":\"0.3\"", &found), MOQ_OK);
        CHECK(found);
        /* A locmaf track without its record, or with an absent version. */
        authored_init(&a, revs[r], "locmaf", false);
        CHECK_RC(encode_rc(&a.cat, NULL, NULL), MOQ_ERR_INVAL);
        authored_init(&a, revs[r], "locmaf", true);
        a.rec.has_version = false;
        CHECK_RC(encode_rc(&a.cat, NULL, NULL), MOQ_ERR_INVAL);
        /* Empty, malformed-flagged, or not UTF-8. */
        authored_init(&a, revs[r], "locmaf", true);
        a.rec.version = lit("");
        CHECK_RC(encode_rc(&a.cat, NULL, NULL), MOQ_ERR_INVAL);
        authored_init(&a, revs[r], "locmaf", true);
        a.rec.malformed = MOQ_MSF_LOCMAF_F_VERSION;
        CHECK_RC(encode_rc(&a.cat, NULL, NULL), MOQ_ERR_INVAL);
        authored_init(&a, revs[r], "locmaf", true);
        a.rec.version = (moq_bytes_t){ (const uint8_t *)"\xff\x30", 2 };
        CHECK_RC(encode_rc(&a.cat, NULL, NULL), MOQ_ERR_INVAL);
        /* Caller storage: a record below its extent, or a span without data. */
        authored_init(&a, revs[r], "locmaf", true);
        a.rec.struct_size = MOQ_MSF_LOCMAF_V0_SIZE - 1;
        CHECK_RC(encode_rc(&a.cat, NULL, NULL), MOQ_ERR_INVAL);
        authored_init(&a, revs[r], "locmaf", true);
        a.rec.version = (moq_bytes_t){ NULL, 3 };
        CHECK_RC(encode_rc(&a.cat, NULL, NULL), MOQ_ERR_INVAL);
        /* A version on another packaging. */
        authored_init(&a, revs[r], "cmaf", true);
        CHECK_RC(encode_rc(&a.cat, NULL, NULL), MOQ_ERR_INVAL);
        /* A version this build does not implement is still authorable. */
        authored_init(&a, revs[r], "locmaf", true);
        a.rec.version = lit("0.4");
        CHECK_RC(encode_rc(&a.cat, "\"locmafVersion\":\"0.4\"", &found), MOQ_OK);
        CHECK(found);
    }
    check_balance("strict_encode");
}

/* Strict encode of delta tracks: add, clone and update. */
static void test_delta_authoring(void)
{
    moq_msf_locmaf_t rec;
    moq_msf_locmaf_init(&rec);
    rec.has_version = true;
    rec.version = lit("0.3");
    moq_msf_track_ext_t ext = { .kind = MOQ_MSF_EXT_LOCMAF, .u = { .locmaf = &rec } };

    moq_msf_track_t t;
    moq_msf_delta_op_t op;
    moq_msf_catalog_t d;
#define RESET_DELTA(kind, rev) do { \
        memset(&t, 0, sizeof(t)); \
        t.struct_size = sizeof(t); \
        t.name = lit("video"); \
        memset(&op, 0, sizeof(op)); \
        op.op = (kind); op.tracks = &t; op.track_count = 1; \
        memset(&d, 0, sizeof(d)); \
        d.struct_size = sizeof(d); d.version = MOQ_MSF_VERSION; \
        d.revision = (rev); d.has_generated_at = true; d.generated_at = 9; \
        d.delta_update = &op; d.delta_update_count = 1; \
        rec.has_version = true; rec.version = lit("0.3"); rec.malformed = 0; \
    } while (0)

    for (moq_msf_revision_t rev = MOQ_MSF_REVISION_01; rev <= MOQ_MSF_REVISION_02; rev++) {
        /* add: a whole locmaf track. */
        RESET_DELTA(MOQ_MSF_DELTA_OP_ADD, rev);
        t.has_packaging = true; t.packaging = lit("locmaf");
        t.has_is_live = true; t.is_live = true;
        t.extensions = &ext; t.extension_count = 1;
        bool found = false;
        CHECK_RC(encode_rc(&d, "\"locmafVersion\":\"0.3\"", &found), MOQ_OK);
        CHECK(found);
        t.extensions = NULL; t.extension_count = 0;          /* no version */
        CHECK_RC(encode_rc(&d, NULL, NULL), MOQ_ERR_INVAL);
        t.extensions = &ext; t.extension_count = 1;
        t.packaging = lit("cmaf");                           /* version on cmaf */
        CHECK_RC(encode_rc(&d, NULL, NULL), MOQ_ERR_INVAL);
        t.has_packaging = false;                             /* no packaging */
        CHECK_RC(encode_rc(&d, NULL, NULL), MOQ_ERR_INVAL);
        t.has_packaging = true; t.packaging = lit("locmaf");
        rec.malformed = MOQ_MSF_LOCMAF_F_VERSION;
        CHECK_RC(encode_rc(&d, NULL, NULL), MOQ_ERR_INVAL);

        /* clone: an override's version must be well-formed. */
        RESET_DELTA(MOQ_MSF_DELTA_OP_CLONE, rev);
        t.name = lit("video2");
        t.has_parent_name = true; t.parent_name = lit("video");
        t.extensions = &ext; t.extension_count = 1;
        CHECK_RC(encode_rc(&d, "\"locmafVersion\":\"0.3\"", &found), MOQ_OK);
        CHECK(found);
        rec.version = lit("");
        CHECK_RC(encode_rc(&d, NULL, NULL), MOQ_ERR_INVAL);
        rec.version = lit("0.3");
        rec.has_version = false;
        CHECK_RC(encode_rc(&d, NULL, NULL), MOQ_ERR_INVAL);
        rec.has_version = true;
        t.has_packaging = true; t.packaging = lit("cmaf");   /* and away from it */
        CHECK_RC(encode_rc(&d, NULL, NULL), MOQ_ERR_INVAL);
        t.packaging = lit("locmaf");
        CHECK_RC(encode_rc(&d, NULL, NULL), MOQ_OK);

        if (rev == MOQ_MSF_REVISION_02) {
            /* update: the same rules as a clone. */
            RESET_DELTA(MOQ_MSF_DELTA_OP_UPDATE, rev);
            t.name = lit("");
            t.has_parent_name = true; t.parent_name = lit("video");
            t.extensions = &ext; t.extension_count = 1;
            CHECK_RC(encode_rc(&d, "\"locmafVersion\":\"0.3\"", &found), MOQ_OK);
            CHECK(found);
            rec.malformed = MOQ_MSF_LOCMAF_F_VERSION;
            CHECK_RC(encode_rc(&d, NULL, NULL), MOQ_ERR_INVAL);
        }
    }
#undef RESET_DELTA
    check_balance("delta_authoring");
}

/* -- Applied deltas: merges and the receive-path re-encode ------------------ */

static void test_apply(void)
{
    moq_msf_catalog_t base, eff;
    static const char base_json[] =
        "{\"version\":\"draft-01\"," INIT_LIST ",\"tracks\":["
        LM_TRACK("video", ",\"locmafVersion\":\"0.3\"") ","
        LM_TRACK("bare", "") ","
        LM_TRACK("bad", ",\"locmafVersion\":7") "]}";
    CHECK_RC(parse(base_json, &base), MOQ_OK);

    /* A clone inherits the parent's record; an override replaces it. */
    CHECK_RC(apply_json(&base,
        "{\"deltaUpdate\":[{\"op\":\"clone\",\"tracks\":["
        "{\"parentName\":\"video\",\"name\":\"v2\"},"
        "{\"parentName\":\"video\",\"name\":\"v3\",\"locmafVersion\":\"0.4\"}]}]}",
        &eff), MOQ_OK);
    const moq_msf_locmaf_t *p = moq_msf_track_locmaf(find_track(&eff, "video"));
    const moq_msf_locmaf_t *c2 = moq_msf_track_locmaf(find_track(&eff, "v2"));
    const moq_msf_locmaf_t *c3 = moq_msf_track_locmaf(find_track(&eff, "v3"));
    CHECK(p && bv_eq(p->version, "0.3"));
    CHECK(c2 && bv_eq(c2->version, "0.3"));
    CHECK(c3 && bv_eq(c3->version, "0.4"));
    /* The peer's malformed version survives the receive-path re-encode as
     * null, so the track stays refused. */
    const moq_msf_track_t *bad = find_track(&eff, "bad");
    const moq_msf_locmaf_t *mb = moq_msf_track_locmaf(bad);
    CHECK(mb && mb->has_version && mb->malformed == MOQ_MSF_LOCMAF_F_VERSION);
    CHECK(check_reason(bad, NULL) == MOQ_MSF_LOCMAF_ERR_VERSION_MALFORMED);
    /* A locmaf track without a version also survives, and stays refused. */
    const moq_msf_track_t *bare = find_track(&eff, "bare");
    CHECK(bare && moq_msf_track_locmaf(bare) == NULL &&
          check_reason(bare, NULL) == MOQ_MSF_LOCMAF_ERR_VERSION_MISSING);
    moq_msf_catalog_cleanup(&g_alloc, &eff);

    /* A clone that adds a version to a "locmaf" parent without one. */
    CHECK_RC(apply_json(&base,
        "{\"deltaUpdate\":[{\"op\":\"clone\",\"tracks\":["
        "{\"parentName\":\"bare\",\"name\":\"fixed\",\"locmafVersion\":\"0.3\"}]}]}",
        &eff), MOQ_OK);
    CHECK(check_reason(find_track(&eff, "fixed"), NULL) == MOQ_MSF_LOCMAF_OK);
    moq_msf_catalog_cleanup(&g_alloc, &eff);

    /* A clone that changes packaging to "cmaf" while inheriting a version:
     * the effective track would carry locmafVersion on cmaf. */
    CHECK_RC(apply_json(&base,
        "{\"deltaUpdate\":[{\"op\":\"clone\",\"tracks\":["
        "{\"parentName\":\"video\",\"name\":\"plain\",\"packaging\":\"cmaf\"}]}]}",
        &eff), MOQ_ERR_PROTO);
    moq_msf_catalog_cleanup(&g_alloc, &eff);

    /* An add of a locmaf track. */
    CHECK_RC(apply_json(&base,
        "{\"deltaUpdate\":[{\"op\":\"add\",\"tracks\":["
        LM_TRACK("added", ",\"locmafVersion\":\"0.3\"") "]}]}", &eff), MOQ_OK);
    CHECK(check_reason(find_track(&eff, "added"), NULL) == MOQ_MSF_LOCMAF_OK);
    moq_msf_catalog_cleanup(&g_alloc, &eff);
    moq_msf_catalog_cleanup(&g_alloc, &base);

    /* MSF-02: update in place. */
    static const char base02[] =
        "{\"version\":\"draft-02\"," INIT_LIST ",\"tracks\":["
        LM_TRACK("video", ",\"locmafVersion\":\"0.3\"") ","
        LM_TRACK("bare", "") "]}";
    CHECK_RC(parse(base02, &base), MOQ_OK);
    CHECK_RC(apply_json(&base,
        "{\"generatedAt\":9,\"deltaUpdate\":[{\"op\":\"update\",\"tracks\":["
        "{\"parentName\":\"video\",\"locmafVersion\":\"0.4\"},"
        "{\"parentName\":\"bare\",\"locmafVersion\":\"0.3\"}]}]}", &eff), MOQ_OK);
    const moq_msf_track_t *uv = find_track(&eff, "video");
    const moq_msf_track_t *ub = find_track(&eff, "bare");
    const moq_msf_locmaf_t *uvm = uv ? moq_msf_track_locmaf(uv) : NULL;
    CHECK(uvm && bv_eq(uvm->version, "0.4"));
    CHECK(ub && moq_msf_track_locmaf(ub) &&
          check_reason(ub, NULL) == MOQ_MSF_LOCMAF_OK);
    CHECK(eff.revision == MOQ_MSF_REVISION_02);
    moq_msf_catalog_cleanup(&g_alloc, &eff);
    /* An update that changes packaging away from "locmaf" while inheriting
     * a version. */
    CHECK_RC(apply_json(&base,
        "{\"generatedAt\":9,\"deltaUpdate\":[{\"op\":\"update\",\"tracks\":["
        "{\"parentName\":\"video\",\"packaging\":\"cmaf\"}]}]}", &eff),
        MOQ_ERR_PROTO);
    moq_msf_catalog_cleanup(&g_alloc, &eff);
    /* An unrelated bitrate update keeps the record as it was. */
    CHECK_RC(apply_json(&base,
        "{\"generatedAt\":9,\"deltaUpdate\":[{\"op\":\"update\",\"tracks\":["
        "{\"parentName\":\"video\",\"bitrate\":100000}]}]}", &eff), MOQ_OK);
    const moq_msf_track_t *bv = find_track(&eff, "video");
    const moq_msf_locmaf_t *bvm = bv ? moq_msf_track_locmaf(bv) : NULL;
    CHECK(bv && bv->has_bitrate && bv->bitrate == 100000 && bvm &&
          bv_eq(bvm->version, "0.3"));
    moq_msf_catalog_cleanup(&g_alloc, &eff);
    moq_msf_catalog_cleanup(&g_alloc, &base);
    check_balance("apply");
}

/* -- Allocation failure ---------------------------------------------------- */

/* A failing allocation at each step of a parse, an encode and an apply of
 * a catalog holding locmaf and mpeg2ts tracks: every call either succeeds
 * or returns NOMEM, and nothing leaks. */
static void test_oom(void)
{
    static const char json[] =
        "{\"version\":\"draft-02\"," INIT_LIST ",\"tracks\":["
        LM_TRACK("video", ",\"locmafVersion\":\"0.3\",\"keyframeSpacing\":30") ","
        "{\"name\":\"ts\",\"packaging\":\"mpeg2ts\",\"isLive\":true,"
        "\"mpeg2tsMode\":\"unmodified-program\",\"mpeg2tsPacketSize\":188,"
        "\"mpeg2tsSiPids\":[17,18]}]}";
    static const char delta[] =
        "{\"generatedAt\":9,\"deltaUpdate\":[{\"op\":\"clone\",\"tracks\":["
        "{\"parentName\":\"video\",\"name\":\"v2\",\"locmafVersion\":\"0.4\"}]}]}";
    bool parsed_ok = false, applied_ok = false, done = false;
    for (uint64_t k = 1; k < 1000 && !done; k++) {
        moq_msf_catalog_t cat;
        g_count.calls = 0;
        g_count.fail_at = k;
        moq_result_t rc = parse(json, &cat);
        CHECK(rc == MOQ_OK || rc == MOQ_ERR_NOMEM);
        if (rc == MOQ_OK) {
            parsed_ok = true;
            moq_rcbuf_t *out = NULL;
            moq_result_t erc = moq_msf_catalog_encode(&g_alloc, &cat, &out);
            CHECK(erc == MOQ_OK || erc == MOQ_ERR_NOMEM);
            if (out) moq_rcbuf_decref(out);
            moq_msf_catalog_cleanup(&g_alloc, &cat);
        }
        bool a_clean = g_count.calls < k;   /* the failure was never reached */
        g_count.fail_at = 0;
        CHECK(g_count.bytes == 0 && g_count.blocks == 0);

        /* The apply with the failure at its k-th allocation. */
        moq_msf_catalog_t base, d, eff;
        g_count.calls = 0;
        if (parse(json, &base) != MOQ_OK) { CHECK(false); break; }
        if (parse(delta, &d) != MOQ_OK) {
            CHECK(false);
            moq_msf_catalog_cleanup(&g_alloc, &base);
            break;
        }
        g_count.calls = 0;
        g_count.fail_at = k;
        rc = moq_msf_catalog_apply_delta_sized(&g_alloc, &base, &d, NULL, &eff,
                                               sizeof(eff));
        bool b_clean = g_count.calls < k;
        g_count.fail_at = 0;
        done = a_clean && b_clean;
        CHECK(rc == MOQ_OK || rc == MOQ_ERR_NOMEM);
        if (rc == MOQ_OK) {
            applied_ok = true;
            const moq_msf_locmaf_t *m = moq_msf_track_locmaf(find_track(&eff, "v2"));
            CHECK(m && bv_eq(m->version, "0.4"));
            CHECK(moq_msf_track_mpeg2ts(find_track(&eff, "ts")) != NULL);
            moq_msf_catalog_cleanup(&g_alloc, &eff);
        }
        moq_msf_catalog_cleanup(&g_alloc, &d);
        moq_msf_catalog_cleanup(&g_alloc, &base);
        CHECK(g_count.bytes == 0 && g_count.blocks == 0);
    }
    CHECK(parsed_ok && applied_ok && done);
    check_balance("oom");
}

int main(void)
{
    test_record_init_sized();
    test_parse_round_trip(k_r01, MOQ_MSF_REVISION_01);
    test_parse_round_trip(k_r02, MOQ_MSF_REVISION_02);
    test_both_revisions();
    test_check_and_leniency();
    test_strict_encode();
    test_delta_authoring();
    test_apply();
    test_oom();
    printf("%s: %d failures\n", failures ? "FAIL" : "PASS", failures);
    return failures ? 1 : 0;
}
