/*
 * MSF-02 catalogs (draft-ietf-moq-msf-02) and their coexistence with MSF-01:
 * the revision, the catalog's appended block and its extents, the sized and
 * the MSF-01 entry points, publishTracks, the timeline rules per revision,
 * and the section 5.6 examples.
 */

#include <moq/msf.h>
#include <moq/rcbuf.h>

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

/* Bounded substring search: encoder output is not NUL-terminated. */
static bool buf_contains(const moq_rcbuf_t *b, const char *needle)
{
    size_t n = strlen(needle), len = moq_rcbuf_len(b);
    const uint8_t *d = moq_rcbuf_data(b);
    for (size_t i = 0; n <= len && i + n <= len; i++)
        if (memcmp(d + i, needle, n) == 0) return true;
    return false;
}

/* The offset of needle in b, or SIZE_MAX. */
static size_t buf_find(const moq_rcbuf_t *b, const char *needle)
{
    size_t n = strlen(needle), len = moq_rcbuf_len(b);
    const uint8_t *d = moq_rcbuf_data(b);
    for (size_t i = 0; n <= len && i + n <= len; i++)
        if (memcmp(d + i, needle, n) == 0) return i;
    return SIZE_MAX;
}

static char *read_fixture(const char *name, size_t *out_len)
{
    char path[1024];
    snprintf(path, sizeof(path), "%s/msf02/%s", MOQ_MSF_FIXTURE_DIR, name);
    FILE *fp = fopen(path, "rb");
    if (!fp) {
        fprintf(stderr, "FAIL: cannot open fixture %s\n", path);
        failures++;
        return NULL;
    }
    fseek(fp, 0, SEEK_END);
    long sz = ftell(fp);
    fseek(fp, 0, SEEK_SET);
    if (sz < 0) { fclose(fp); return NULL; }
    char *buf = malloc((size_t)sz + 1);
    if (!buf) { fclose(fp); return NULL; }
    size_t rd = fread(buf, 1, (size_t)sz, fp);
    fclose(fp);
    buf[rd] = '\0';
    if (out_len) *out_len = rd;
    return buf;
}

/* Parse a fixture with the full extent. */
static moq_result_t parse_fixture(const char *name, moq_msf_catalog_t *cat)
{
    size_t n = 0;
    char *json = read_fixture(name, &n);
    if (!json) return MOQ_ERR_INVAL;
    moq_result_t rc = moq_msf_catalog_parse(&g_alloc,
        (moq_bytes_t){ (const uint8_t *)json, n }, cat);
    free(json);
    return rc;
}

static moq_result_t parse_full(const char *json, moq_msf_catalog_t *cat)
{
    return moq_msf_catalog_parse(&g_alloc, lit(json), cat);
}

/* The exported MSF-01 symbol, called by address. */
static moq_result_t (*const frozen_parse)(const moq_alloc_t *, moq_bytes_t,
                                          moq_msf_catalog_t *) =
    &moq_msf_catalog_parse;

static const moq_msf_track_t *find_track(const moq_msf_catalog_t *c,
                                         const char *name)
{
    for (size_t i = 0; i < c->track_count; i++)
        if (bv_eq(c->tracks[i].name, name)) return &c->tracks[i];
    return NULL;
}

/* Storage for a catalog written through an arbitrary extent, with a canary
 * past it. */
#define EXT_BUF 192
typedef union {
    max_align_t       align;
    moq_msf_catalog_t cat;
    uint8_t           bytes[EXT_BUF];
} ext_buf_t;

#define CANARY 0xA5u

static bool canary_intact(const ext_buf_t *b, size_t from)
{
    for (size_t i = from; i < EXT_BUF; i++)
        if (b->bytes[i] != CANARY) return false;
    return true;
}

/* -- Layout ---------------------------------------------------------------- */

_Static_assert(MOQ_MSF_CATALOG_V0_SIZE ==
               offsetof(moq_msf_catalog_t, version_form),
               "the MSF-01 layout ends where version_form starts");
_Static_assert(offsetof(moq_msf_catalog_t, revision) ==
               MOQ_MSF_CATALOG_SIZE_THROUGH_VERSION_FORM + 4,
               "revision lies past the reserved slot after version_form");
_Static_assert(offsetof(moq_msf_catalog_t, publish_tracks) ==
               offsetof(moq_msf_catalog_t, _reserved_publish_tracks) + 4,
               "publish_tracks follows its reserved slot");
_Static_assert(MOQ_MSF_CATALOG_SIZE_THROUGH_PUBLISH_TRACKS <=
               sizeof(moq_msf_catalog_t) &&
               sizeof(moq_msf_catalog_t) -
                   MOQ_MSF_CATALOG_SIZE_THROUGH_PUBLISH_TRACKS < sizeof(uint64_t),
               "the appended block ends the struct");
_Static_assert(MOQ_MSF_REVISION_01 == 0 && MOQ_MSF_REVISION_02 == 1,
               "revision values are pinned");

static void test_layout(void)
{
    if (sizeof(void *) == 8 && sizeof(size_t) == 8) {
        CHECK(MOQ_MSF_CATALOG_V0_SIZE == 112);
        CHECK(MOQ_MSF_CATALOG_SIZE_THROUGH_VERSION_FORM == 116);
        CHECK(offsetof(moq_msf_catalog_t, _reserved_revision) == 116);
        CHECK(offsetof(moq_msf_catalog_t, revision) == 120);
        CHECK(offsetof(moq_msf_catalog_t, _reserved_publish_tracks) == 124);
        CHECK(offsetof(moq_msf_catalog_t, publish_tracks) == 128);
        CHECK(offsetof(moq_msf_catalog_t, publish_track_count) == 136);
        CHECK(sizeof(moq_msf_catalog_t) == 144);
        CHECK(sizeof(moq_msf_validate_cfg_t) == 40);
        CHECK(MOQ_MSF_VALIDATE_CFG_V0_SIZE == 40);
    }
}

/* -- Revision ---------------------------------------------------------------- */

static void test_revision_parse(void)
{
    static const struct {
        const char *version;
        moq_msf_revision_t revision;
        moq_msf_version_form_t form;
        bool frozen_ok;
    } ok[] = {
        { "\"1\"",        MOQ_MSF_REVISION_01, MOQ_MSF_VERSION_FORM_NUMERAL,    true },
        { "1",            MOQ_MSF_REVISION_01, MOQ_MSF_VERSION_FORM_NUMERAL,    true },
        { "\"draft-01\"", MOQ_MSF_REVISION_01, MOQ_MSF_VERSION_FORM_DRAFT_NAME, true },
        { "\"draft-02\"", MOQ_MSF_REVISION_02, MOQ_MSF_VERSION_FORM_DRAFT_NAME, false },
    };
    char json[256];
    for (size_t i = 0; i < sizeof(ok) / sizeof(ok[0]); i++) {
        snprintf(json, sizeof(json), "{\"version\":%s,\"tracks\":[]}",
                 ok[i].version);
        moq_msf_catalog_t cat;
        CHECK_RC(parse_full(json, &cat), MOQ_OK);
        CHECK(cat.struct_size == sizeof(cat));
        CHECK(cat.revision == ok[i].revision);
        CHECK(cat.version == MOQ_MSF_VERSION);
        CHECK(cat.version_form == ok[i].form);
        moq_msf_catalog_cleanup(&g_alloc, &cat);

        moq_msf_catalog_t old;
        moq_result_t rc = frozen_parse(&g_alloc, lit(json), &old);
        CHECK(rc == (ok[i].frozen_ok ? MOQ_OK : MOQ_ERR_PROTO));
        if (rc == MOQ_OK) {
            CHECK(old.struct_size == MOQ_MSF_CATALOG_SIZE_THROUGH_VERSION_FORM);
            CHECK(old.version_form == ok[i].form);
            moq_msf_catalog_cleanup(&g_alloc, &old);
        }
    }
    static const char *const bad[] = {
        "\"draft-03\"", "\"draft-2\"", "\"draft-002\"", "\"Draft-02\"",
        "\"draft-02 \"", "\"2\"", "2", "\"\"", "null", "\"draft-00\"",
    };
    for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); i++) {
        snprintf(json, sizeof(json), "{\"version\":%s,\"tracks\":[]}", bad[i]);
        moq_msf_catalog_t cat;
        CHECK_RC(parse_full(json, &cat), MOQ_ERR_PROTO);
        CHECK_RC(frozen_parse(&g_alloc, lit(json), &cat), MOQ_ERR_PROTO);
    }
    check_balance("revision_parse");
}

/* -- Extents --------------------------------------------------------------- */

enum { X_INVAL, X_R01, X_FULL };

static int extent_class(size_t x)
{
    if (x < MOQ_MSF_CATALOG_V0_SIZE) return X_INVAL;
    if (x < offsetof(moq_msf_catalog_t, revision) + sizeof(moq_msf_revision_t))
        return X_R01;
    if (x < MOQ_MSF_CATALOG_SIZE_THROUGH_PUBLISH_TRACKS) return X_INVAL;
    return X_FULL;
}

static const char k_r01_publish[] =
    "{\"version\":\"1\",\"tracks\":[{\"name\":\"v\",\"packaging\":\"loc\","
    "\"isLive\":true}],\"publishTracks\":[{\"name\":\"log\","
    "\"packaging\":\"moqlog\",\"isLive\":true}]}";

static const char k_r02_simple[] =
    "{\"version\":\"draft-02\",\"generatedAt\":5,\"tracks\":["
    "{\"name\":\"v\",\"packaging\":\"loc\",\"isLive\":true}]}";

static void test_extent_boundaries(void)
{
    size_t lo = MOQ_MSF_CATALOG_V0_SIZE - 1;
    size_t hi = sizeof(moq_msf_catalog_t) + 8;
    for (size_t x = lo; x <= hi; x++) {
        int cls = extent_class(x);
        size_t wrote = x < sizeof(moq_msf_catalog_t) ? x : sizeof(moq_msf_catalog_t);

        /* An MSF-01 catalog with publishTracks. */
        ext_buf_t b;
        memset(b.bytes, CANARY, sizeof(b.bytes));
        moq_result_t rc = moq_msf_catalog_parse_sized(&g_alloc,
            lit(k_r01_publish), &b.cat, x);
        if (cls == X_INVAL) {
            CHECK(rc == MOQ_ERR_INVAL);
            CHECK(canary_intact(&b, 0));          /* nothing written */
        } else {
            CHECK(rc == MOQ_OK);
            CHECK(b.cat.struct_size == wrote);
            CHECK(canary_intact(&b, wrote));
            CHECK(b.cat.track_count == 1);
            if (cls == X_FULL) {
                CHECK(b.cat.revision == MOQ_MSF_REVISION_01);
                CHECK(b.cat.publish_track_count == 1);
                CHECK(b.cat.publish_tracks &&
                      bv_eq(b.cat.publish_tracks[0].name, "log"));
            }
            moq_msf_catalog_cleanup(&g_alloc, &b.cat);
        }

        /* An MSF-02 catalog: refused by the MSF-01 extent. */
        memset(b.bytes, CANARY, sizeof(b.bytes));
        rc = moq_msf_catalog_parse_sized(&g_alloc, lit(k_r02_simple), &b.cat, x);
        if (cls == X_INVAL) {
            CHECK(rc == MOQ_ERR_INVAL);
            CHECK(canary_intact(&b, 0));
        } else if (cls == X_R01) {
            CHECK(rc == MOQ_ERR_PROTO);
            CHECK(canary_intact(&b, wrote));
        } else {
            CHECK(rc == MOQ_OK);
            CHECK(b.cat.struct_size == wrote);
            CHECK(b.cat.revision == MOQ_MSF_REVISION_02);
            CHECK(canary_intact(&b, wrote));
            moq_msf_catalog_cleanup(&g_alloc, &b.cat);
        }
        check_balance("extent_boundaries parse");

        /* apply_delta_sized writes its output through the same extents. */
        moq_msf_catalog_t base, delta;
        CHECK_RC(parse_full(k_r01_publish, &base), MOQ_OK);
        CHECK_RC(parse_full("{\"generatedAt\":9,\"deltaUpdate\":[{\"op\":"
                            "\"add\",\"tracks\":[{\"name\":\"a\","
                            "\"packaging\":\"loc\",\"isLive\":true}]}]}",
                            &delta), MOQ_OK);
        memset(b.bytes, CANARY, sizeof(b.bytes));
        rc = moq_msf_catalog_apply_delta_sized(&g_alloc, &base, &delta, NULL,
                                               &b.cat, x);
        if (cls == X_INVAL) {
            CHECK(rc == MOQ_ERR_INVAL);
            CHECK(canary_intact(&b, 0));
        } else {
            CHECK(rc == MOQ_OK);
            CHECK(b.cat.struct_size == wrote);
            CHECK(canary_intact(&b, wrote));
            CHECK(b.cat.track_count == 2);
            if (cls == X_FULL) CHECK(b.cat.publish_track_count == 1);
            moq_msf_catalog_cleanup(&g_alloc, &b.cat);
        }

        /* An input whose struct_size covers part of the block is refused. */
        if (x > MOQ_MSF_CATALOG_SIZE_THROUGH_VERSION_FORM &&
            x < sizeof(moq_msf_catalog_t)) {
            uint32_t saved = base.struct_size;
            base.struct_size = (uint32_t)x;
            moq_msf_catalog_t out;
            rc = moq_msf_catalog_apply_delta_sized(&g_alloc, &base, &delta,
                                                   NULL, &out, sizeof(out));
            CHECK(rc == (cls == X_INVAL ? MOQ_ERR_INVAL : MOQ_OK));
            if (rc == MOQ_OK) moq_msf_catalog_cleanup(&g_alloc, &out);
            moq_rcbuf_t *enc = NULL;
            rc = moq_msf_catalog_encode(&g_alloc, &base, &enc);
            CHECK(rc == (cls == X_INVAL ? MOQ_ERR_INVAL : MOQ_OK));
            moq_rcbuf_decref(enc);
            base.struct_size = saved;
        }
        moq_msf_catalog_cleanup(&g_alloc, &delta);
        moq_msf_catalog_cleanup(&g_alloc, &base);
        check_balance("extent_boundaries apply");
    }
}

/* -- The MSF-01 symbols ----------------------------------------------------- */

static void test_frozen_symbol(void)
{
    /* The exported parse writes the MSF-01 extent and nothing past it. */
    ext_buf_t b;
    memset(b.bytes, CANARY, sizeof(b.bytes));
    CHECK_RC(frozen_parse(&g_alloc, lit(k_r01_publish), &b.cat), MOQ_OK);
    CHECK(b.cat.struct_size == MOQ_MSF_CATALOG_SIZE_THROUGH_VERSION_FORM);
    CHECK(canary_intact(&b, MOQ_MSF_CATALOG_SIZE_THROUGH_VERSION_FORM));
    CHECK(b.cat.track_count == 1);
    CHECK(b.cat.tracks[0].extension_count == 0);
    moq_msf_catalog_cleanup(&g_alloc, &b.cat);
    CHECK(canary_intact(&b, MOQ_MSF_CATALOG_SIZE_THROUGH_VERSION_FORM));

    moq_msf_catalog_t cat;
    CHECK_RC(frozen_parse(&g_alloc, lit(k_r02_simple), &cat), MOQ_ERR_PROTO);

    /* The MSF-01 extent carries no supplement, whatever keys a track holds:
     * what a caller of that extent reads is what it always read. */
    static const char r01_keys[] =
        "{\"version\":\"1\",\"tracks\":[{\"name\":\"v\",\"packaging\":\"loc\","
        "\"isLive\":true,\"encryptionScheme\":\"moq-secure-objects\","
        "\"keyId\":\"k\",\"keyframeSpacing\":15}]}";
    CHECK_RC(frozen_parse(&g_alloc, lit(r01_keys), &cat), MOQ_OK);
    CHECK(cat.tracks[0].extension_count == 0 && !moq_msf_track_supplement(&cat.tracks[0]));
    moq_msf_catalog_cleanup(&g_alloc, &cat);
    memset(b.bytes, CANARY, sizeof(b.bytes));
    CHECK_RC(moq_msf_catalog_parse_sized(&g_alloc, lit(r01_keys), &b.cat,
                 MOQ_MSF_CATALOG_SIZE_THROUGH_VERSION_FORM), MOQ_OK);
    CHECK(b.cat.tracks[0].extension_count == 0);
    moq_msf_catalog_cleanup(&g_alloc, &b.cat);
    CHECK_RC(parse_full(r01_keys, &cat), MOQ_OK);
    CHECK(moq_msf_track_supplement(&cat.tracks[0]) &&
          moq_msf_track_supplement(&cat.tracks[0])->has_key_id);
    moq_msf_catalog_cleanup(&g_alloc, &cat);

    /* The MSF-01 applies refuse an MSF-02 base rather than downgrade it. */
    moq_msf_catalog_t base, delta, out;
    CHECK_RC(parse_full(k_r02_simple, &base), MOQ_OK);
    CHECK_RC(parse_full("{\"generatedAt\":9,\"deltaUpdate\":[{\"op\":\"add\","
                        "\"tracks\":[{\"name\":\"a\",\"packaging\":\"loc\","
                        "\"isLive\":true}]}]}", &delta), MOQ_OK);
    moq_result_t (*legacy)(const moq_alloc_t *, const moq_msf_catalog_t *,
                           const moq_msf_catalog_t *, moq_msf_catalog_t *) =
        &moq_msf_catalog_apply_delta;
    CHECK_RC(legacy(&g_alloc, &base, &delta, &out), MOQ_ERR_INVAL);
    CHECK_RC(moq_msf_catalog_apply_delta_ex(&g_alloc, &base, &delta, NULL, &out),
             MOQ_ERR_INVAL);
    /* ... and an MSF-01-extent output cannot hold an MSF-02 catalog. */
    CHECK_RC(moq_msf_catalog_apply_delta_sized(&g_alloc, &base, &delta, NULL,
                 &out, MOQ_MSF_CATALOG_SIZE_THROUGH_VERSION_FORM), MOQ_ERR_INVAL);
    CHECK_RC(moq_msf_catalog_apply_delta_sized(&g_alloc, &base, &delta, NULL,
                                               &out, sizeof(out)), MOQ_OK);
    CHECK(out.revision == MOQ_MSF_REVISION_02 && out.track_count == 2);
    moq_msf_catalog_cleanup(&g_alloc, &out);
    moq_msf_catalog_cleanup(&g_alloc, &base);

    /* An MSF-01 base through the legacy apply: the MSF-01 extent out. */
    CHECK_RC(parse_full(k_r01_publish, &base), MOQ_OK);
    memset(b.bytes, CANARY, sizeof(b.bytes));
    CHECK_RC(legacy(&g_alloc, &base, &delta, &b.cat), MOQ_OK);
    CHECK(b.cat.struct_size == MOQ_MSF_CATALOG_SIZE_THROUGH_VERSION_FORM);
    CHECK(canary_intact(&b, MOQ_MSF_CATALOG_SIZE_THROUGH_VERSION_FORM));
    CHECK(b.cat.track_count == 2);
    moq_msf_catalog_cleanup(&g_alloc, &b.cat);
    moq_msf_catalog_cleanup(&g_alloc, &base);
    moq_msf_catalog_cleanup(&g_alloc, &delta);
    check_balance("frozen_symbol");
}

/* -- The root keys a parsed delta carried ---------------------------------- */

static void test_presence_header(void)
{
    moq_msf_catalog_t r02, r01;
    CHECK_RC(parse_full(k_r02_simple, &r02), MOQ_OK);
    CHECK_RC(parse_full(k_r01_publish, &r01), MOQ_OK);

    static const struct { const char *roots; bool r02_ok; } rows[] = {
        { "\"generatedAt\":1,",                 true },
        { "",                                    false },  /* no generatedAt */
        { "\"generatedAt\":\"1\",",             false },  /* not a Number */
        { "\"generatedAt\":1,\"initDataList\":[],",        false },
        { "\"generatedAt\":1,\"contentProtections\":[],",  false },
        { "\"generatedAt\":1,\"publishTracks\":[],",       false },
        { "\"generatedAt\":1,\"publishTracks\":[{\"name\":\"p\","
          "\"packaging\":\"moqlog\",\"isLive\":true}],",   false },
        { "\"generatedAt\":1,\"com.example-key\":3,",      true },
    };
    char json[512];
    for (size_t i = 0; i < sizeof(rows) / sizeof(rows[0]); i++) {
        snprintf(json, sizeof(json),
                 "{%s\"deltaUpdate\":[{\"op\":\"add\",\"tracks\":[{\"name\":"
                 "\"a\",\"packaging\":\"loc\",\"isLive\":true}]}]}",
                 rows[i].roots);
        moq_msf_catalog_t d, out;
        CHECK_RC(parse_full(json, &d), MOQ_OK);
        moq_result_t rc = moq_msf_catalog_apply_delta_sized(&g_alloc, &r02, &d,
                                                            NULL, &out, sizeof(out));
        CHECK(rc == (rows[i].r02_ok ? MOQ_OK : MOQ_ERR_PROTO));
        if (rc == MOQ_OK) moq_msf_catalog_cleanup(&g_alloc, &out);
        /* MSF-01 has no such rule: the same delta applies. */
        rc = moq_msf_catalog_apply_delta_sized(&g_alloc, &r01, &d, NULL, &out,
                                               sizeof(out));
        CHECK(rc == MOQ_OK);
        if (rc == MOQ_OK) moq_msf_catalog_cleanup(&g_alloc, &out);
        moq_msf_catalog_cleanup(&g_alloc, &d);
    }

    /* Keys a delta may never carry are refused by the parse itself. */
    static const char *const never[] = {
        "{\"isComplete\":false,\"generatedAt\":1,\"deltaUpdate\":[{\"op\":"
        "\"remove\",\"tracks\":[{\"name\":\"v\"}]}]}",
        "{\"tracks\":[],\"generatedAt\":1,\"deltaUpdate\":[{\"op\":"
        "\"remove\",\"tracks\":[{\"name\":\"v\"}]}]}",
        "{\"version\":\"draft-02\",\"generatedAt\":1,\"deltaUpdate\":[{\"op\":"
        "\"remove\",\"tracks\":[{\"name\":\"v\"}]}]}",
    };
    for (size_t i = 0; i < sizeof(never) / sizeof(never[0]); i++) {
        moq_msf_catalog_t d;
        CHECK_RC(parse_full(never[i], &d), MOQ_ERR_PROTO);
    }

    /* A caller-built delta is judged by its public fields. */
    moq_msf_track_t at;
    memset(&at, 0, sizeof(at));
    at.struct_size = sizeof(at);
    at.name = lit("a"); at.packaging = lit("loc"); at.is_live = true;
    at.has_packaging = true; at.has_is_live = true;
    moq_msf_delta_op_t op = { MOQ_MSF_DELTA_OP_ADD, &at, 1 };
    moq_msf_catalog_t d;
    memset(&d, 0, sizeof(d));
    d.struct_size = sizeof(d);
    d.delta_update = &op; d.delta_update_count = 1;
    moq_msf_catalog_t out;
    CHECK_RC(moq_msf_catalog_apply_delta_sized(&g_alloc, &r02, &d, NULL, &out,
                                               sizeof(out)), MOQ_ERR_PROTO);
    d.has_generated_at = true; d.generated_at = 7;
    CHECK_RC(moq_msf_catalog_apply_delta_sized(&g_alloc, &r02, &d, NULL, &out,
                                               sizeof(out)), MOQ_OK);
    CHECK(out.has_generated_at && out.generated_at == 7);
    moq_msf_catalog_cleanup(&g_alloc, &out);
    moq_msf_init_data_entry_t ide = { lit("i"), lit("inline"), lit("AAAA") };
    d.init_data_list = &ide; d.init_data_count = 1;
    CHECK_RC(moq_msf_catalog_apply_delta_sized(&g_alloc, &r02, &d, NULL, &out,
                                               sizeof(out)), MOQ_ERR_PROTO);
    d.init_data_list = NULL; d.init_data_count = 0;
    d.is_complete = true;
    CHECK_RC(moq_msf_catalog_apply_delta_sized(&g_alloc, &r02, &d, NULL, &out,
                                               sizeof(out)), MOQ_ERR_PROTO);
    d.is_complete = false;
    moq_msf_track_t pt = at;
    d.publish_tracks = &pt; d.publish_track_count = 1;
    CHECK_RC(moq_msf_catalog_apply_delta_sized(&g_alloc, &r02, &d, NULL, &out,
                                               sizeof(out)), MOQ_ERR_PROTO);
    d.publish_tracks = NULL; d.publish_track_count = 0;
    moq_cmsf_content_protection_t cp;
    memset(&cp, 0, sizeof(cp));
    d.content_protections = &cp; d.content_protection_count = 1;
    CHECK_RC(moq_msf_catalog_apply_delta_sized(&g_alloc, &r02, &d, NULL, &out,
                                               sizeof(out)), MOQ_ERR_PROTO);
    d.content_protections = NULL; d.content_protection_count = 0;
    d.tracks = &pt; d.track_count = 1;
    CHECK_RC(moq_msf_catalog_apply_delta_sized(&g_alloc, &r02, &d, NULL, &out,
                                               sizeof(out)), MOQ_ERR_PROTO);

    moq_msf_catalog_cleanup(&g_alloc, &r02);
    moq_msf_catalog_cleanup(&g_alloc, &r01);
    check_balance("presence_header");
}

/* -- The section 5.6 examples ----------------------------------------------- */

static const char *const k_examples[] = {
    "5_6_1_av_single.json", "5_6_2_simulcast.json", "5_6_3_svc.json",
    "5_6_4_delta_update_add.json", "5_6_5_delta_remove.json",
    "5_6_6_custom_fields.json", "5_6_7_vod.json", "5_6_8_encrypted.json",
    "5_6_9_timelines.json", "5_6_10_template.json",
    "5_6_11_captions_scte35.json", "5_6_12_cea708.json",
    "5_6_13_terminate.json", "5_6_14_variables_template.json",
    "5_6_14_variables_resolved.json", "5_6_15_authorization.json",
    "5_6_16_catalog_refs.json", "5_6_17_publish_tracks.json",
};
#define N_EXAMPLES (sizeof(k_examples) / sizeof(k_examples[0]))

/* Keys these examples carry that this library does not model. */
static const char *const k_unmodelled[] = {
    "\"buffers\"", "\"spatialId\"", "\"temporalId\"", "\"accessibility\"",
    "\"authInfo\"", "\"connectionURI\"", "\"com.example-",
};

static bool same_track_names(const moq_msf_track_t *a, size_t an,
                             const moq_msf_track_t *b, size_t bn)
{
    if (an != bn) return false;
    for (size_t i = 0; i < an; i++) {
        const moq_msf_track_t *x = &a[i], *y = &b[i];
        if (x->name.len != y->name.len ||
            memcmp(x->name.data, y->name.data, x->name.len) != 0 ||
            x->has_namespace != y->has_namespace ||
            x->namespace_.len != y->namespace_.len ||
            (x->has_namespace &&
             memcmp(x->namespace_.data, y->namespace_.data, x->namespace_.len) != 0) ||
            !bv_eq(x->packaging, "") != !bv_eq(y->packaging, "") ||
            x->is_live != y->is_live || x->depends_count != y->depends_count ||
            x->has_bitrate != y->has_bitrate || x->bitrate != y->bitrate ||
            x->has_template != y->has_template ||
            x->has_track_duration != y->has_track_duration)
            return false;
    }
    return true;
}

/* Parse, strict encode, re-parse: modelled fields survive, unmodelled keys
 * do not. `skip` names examples whose vocabulary another test covers. */
static void roundtrip_examples(const char *const *skip, size_t nskip)
{
    for (size_t i = 0; i < N_EXAMPLES; i++) {
        bool skipped = false;
        for (size_t s = 0; s < nskip; s++)
            if (strcmp(k_examples[i], skip[s]) == 0) skipped = true;
        if (skipped) continue;
        moq_msf_catalog_t cat;
        moq_result_t rc = parse_fixture(k_examples[i], &cat);
        if (rc != MOQ_OK) {
            fprintf(stderr, "FAIL: parse %s: %d\n", k_examples[i], rc);
            failures++;
            continue;
        }
        if (!cat.delta_update) CHECK(cat.revision == MOQ_MSF_REVISION_02);
        moq_rcbuf_t *enc = NULL;
        rc = moq_msf_catalog_encode(&g_alloc, &cat, &enc);
        if (rc != MOQ_OK) {
            fprintf(stderr, "FAIL: encode %s: %d\n", k_examples[i], rc);
            failures++;
            moq_msf_catalog_cleanup(&g_alloc, &cat);
            continue;
        }
        if (!cat.delta_update)
            CHECK(buf_contains(enc, "\"version\":\"draft-02\""));
        for (size_t k = 0; k < sizeof(k_unmodelled) / sizeof(k_unmodelled[0]); k++)
            CHECK(!buf_contains(enc, k_unmodelled[k]));
        moq_msf_catalog_t back;
        CHECK_RC(moq_msf_catalog_parse(&g_alloc,
                     (moq_bytes_t){ moq_rcbuf_data(enc), moq_rcbuf_len(enc) },
                     &back), MOQ_OK);
        CHECK(back.revision == cat.revision);
        CHECK(same_track_names(cat.tracks, cat.track_count,
                               back.tracks, back.track_count));
        CHECK(same_track_names(cat.publish_tracks, cat.publish_track_count,
                               back.publish_tracks, back.publish_track_count));
        CHECK(back.is_complete == cat.is_complete);
        CHECK(back.delta_update_count == cat.delta_update_count);
        moq_msf_catalog_cleanup(&g_alloc, &back);
        moq_rcbuf_decref(enc);
        moq_msf_catalog_cleanup(&g_alloc, &cat);
    }
    check_balance("examples_roundtrip");
}

static void test_examples_roundtrip_r02(void)
{
    roundtrip_examples(NULL, 0);
}

/* -- Timeline rules per revision -------------------------------------------- */

static void test_timeline_relaxation(void)
{
    /* MSF-02 timelines need no mimeType and an event timeline needs no
     * depends (7.2, 8.3); MSF-01 required both (7.2, 8.3 of -01). */
    moq_msf_catalog_t base;
    moq_result_t prc = parse_fixture("5_6_11_captions_scte35.json", &base);
    CHECK(prc == MOQ_OK);
    if (prc != MOQ_OK) return;
    CHECK(base.revision == MOQ_MSF_REVISION_02);
    moq_rcbuf_t *enc = NULL;
    CHECK_RC(moq_msf_catalog_encode(&g_alloc, &base, &enc), MOQ_OK);
    moq_rcbuf_decref(enc);

    moq_msf_catalog_t d, out;
    CHECK_RC(parse_full("{\"generatedAt\":1746104606050,\"deltaUpdate\":[{\"op\":"
                        "\"remove\",\"tracks\":[{\"name\":\"audio\"}]}]}", &d),
             MOQ_OK);
    CHECK_RC(moq_msf_catalog_apply_delta_sized(&g_alloc, &base, &d, NULL, &out,
                                               sizeof(out)), MOQ_OK);
    CHECK(out.track_count == 3 && out.revision == MOQ_MSF_REVISION_02);
    CHECK(find_track(&out, "scte35") && !find_track(&out, "audio"));
    moq_msf_catalog_cleanup(&g_alloc, &out);
    moq_msf_catalog_cleanup(&g_alloc, &d);

    /* The same tracks as MSF-01 keep MSF-01's rules: an eventtimeline without
     * mimeType does not encode. */
    base.revision = MOQ_MSF_REVISION_01;
    CHECK_RC(moq_msf_catalog_encode(&g_alloc, &base, &enc), MOQ_ERR_INVAL);
    base.revision = MOQ_MSF_REVISION_02;

    /* A media timeline still needs depends in MSF-02 (7.2). */
    moq_msf_track_t *tl = (moq_msf_track_t *)(uintptr_t)find_track(&base,
                                                                   "video-timeline");
    CHECK(tl != NULL);
    if (tl) {
        moq_bytes_t *saved = tl->depends;
        size_t n = tl->depends_count;
        tl->depends = NULL; tl->depends_count = 0;
        CHECK_RC(moq_msf_catalog_encode(&g_alloc, &base, &enc), MOQ_ERR_INVAL);
        tl->depends = saved; tl->depends_count = n;
    }
    /* eventType stays required on an event timeline in both revisions. */
    moq_msf_track_t *ev = (moq_msf_track_t *)(uintptr_t)find_track(&base, "scte35");
    if (ev) {
        ev->has_event_type = false;
        CHECK_RC(moq_msf_catalog_encode(&g_alloc, &base, &enc), MOQ_ERR_INVAL);
        ev->has_event_type = true;
    }
    moq_msf_catalog_cleanup(&g_alloc, &base);
    check_balance("timeline_relaxation");
}

/* -- Apply ------------------------------------------------------------------- */

static void test_apply_keeps_revision_and_publish(void)
{
    moq_msf_catalog_t base, d, out;
    moq_result_t prc = parse_fixture("5_6_17_publish_tracks.json", &base);
    CHECK(prc == MOQ_OK);
    if (prc != MOQ_OK) return;
    CHECK(base.publish_track_count == 2);
    CHECK_RC(parse_full("{\"generatedAt\":1746104606100,\"deltaUpdate\":[{\"op\":"
                        "\"remove\",\"tracks\":[{\"name\":\"audio\",\"namespace\":"
                        "\"broadcast.example.com/live/stream1\"}]}]}", &d), MOQ_OK);
    CHECK_RC(moq_msf_catalog_apply_delta_sized(&g_alloc, &base, &d, NULL, &out,
                                               sizeof(out)), MOQ_OK);
    CHECK(out.revision == MOQ_MSF_REVISION_02);
    CHECK(out.version_form == MOQ_MSF_VERSION_FORM_DRAFT_NAME);
    CHECK(out.track_count == 1);
    CHECK(out.has_generated_at && out.generated_at == 1746104606100ULL);
    CHECK(out.publish_track_count == 2);
    if (out.publish_track_count == 2) {
        CHECK(bv_eq(out.publish_tracks[0].name, "4"));
        CHECK(bv_eq(out.publish_tracks[1].packaging, "moqlog"));
        CHECK(bv_eq(out.publish_tracks[1].namespace_,
                    "moq-syslog.arpa/logs-v1/%resourceId%"));
    }
    moq_msf_catalog_cleanup(&g_alloc, &out);
    moq_msf_catalog_cleanup(&g_alloc, &d);
    moq_msf_catalog_cleanup(&g_alloc, &base);

    /* MSF-01, with publishTracks (MSF-01 5.1.5) and the draft-name form. */
    CHECK_RC(parse_full("{\"version\":\"draft-01\",\"tracks\":[{\"name\":\"v\","
                        "\"packaging\":\"loc\",\"isLive\":true}],\"publishTracks\":"
                        "[{\"name\":\"m\",\"packaging\":\"moqmetrics\","
                        "\"isLive\":true}]}", &base), MOQ_OK);
    CHECK_RC(parse_full("{\"deltaUpdate\":[{\"op\":\"add\",\"tracks\":[{\"name\":"
                        "\"a\",\"packaging\":\"loc\",\"isLive\":true}]}]}", &d),
             MOQ_OK);
    CHECK_RC(moq_msf_catalog_apply_delta_sized(&g_alloc, &base, &d, NULL, &out,
                                               sizeof(out)), MOQ_OK);
    CHECK(out.revision == MOQ_MSF_REVISION_01);
    CHECK(out.version_form == MOQ_MSF_VERSION_FORM_DRAFT_NAME);
    CHECK(out.track_count == 2 && out.publish_track_count == 1);
    moq_msf_catalog_cleanup(&g_alloc, &out);
    moq_msf_catalog_cleanup(&g_alloc, &d);
    moq_msf_catalog_cleanup(&g_alloc, &base);
    check_balance("apply_keeps_revision_and_publish");
}

static void test_apply_error_code(void)
{
    /* An effective catalog that would not encode is the peer's fault: PROTO
     * from every apply entry point, not INVAL. */
    moq_msf_catalog_t base, d, out;
    CHECK_RC(parse_full("{\"version\":\"1\",\"tracks\":[{\"name\":\"v\","
                        "\"packaging\":\"loc\",\"isLive\":true}]}", &base), MOQ_OK);
    /* An add track without packaging (it parses, 5.6.4 of -01 omits it). */
    CHECK_RC(parse_full("{\"deltaUpdate\":[{\"op\":\"add\",\"tracks\":["
                        "{\"name\":\"a\",\"isLive\":true}]}]}", &d), MOQ_OK);
    CHECK_RC(moq_msf_catalog_apply_delta(&g_alloc, &base, &d, &out), MOQ_ERR_PROTO);
    CHECK_RC(moq_msf_catalog_apply_delta_ex(&g_alloc, &base, &d, NULL, &out),
             MOQ_ERR_PROTO);
    CHECK_RC(moq_msf_catalog_apply_delta_sized(&g_alloc, &base, &d, NULL, &out,
                                               sizeof(out)), MOQ_ERR_PROTO);
    CHECK(out.struct_size == sizeof(out) && out.track_count == 0 && !out._dom);
    moq_msf_catalog_cleanup(&g_alloc, &d);
    /* A clone that leaves a live track with a trackDuration (5.2.35). */
    CHECK_RC(parse_full("{\"deltaUpdate\":[{\"op\":\"clone\",\"tracks\":["
                        "{\"parentName\":\"v\",\"name\":\"w\","
                        "\"trackDuration\":10}]}]}", &d), MOQ_OK);
    CHECK_RC(moq_msf_catalog_apply_delta(&g_alloc, &base, &d, &out), MOQ_ERR_PROTO);
    moq_msf_catalog_cleanup(&g_alloc, &d);
    /* Malformed caller storage stays INVAL. */
    moq_msf_track_t at;
    memset(&at, 0, sizeof(at));
    at.struct_size = sizeof(at);
    at.name = lit("a"); at.packaging = (moq_bytes_t){ NULL, 3 }; at.is_live = true;
    moq_msf_delta_op_t op = { MOQ_MSF_DELTA_OP_ADD, &at, 1 };
    moq_msf_catalog_t cd;
    memset(&cd, 0, sizeof(cd));
    cd.struct_size = sizeof(cd);
    cd.delta_update = &op; cd.delta_update_count = 1;
    CHECK_RC(moq_msf_catalog_apply_delta(&g_alloc, &base, &cd, &out), MOQ_ERR_INVAL);
    moq_msf_catalog_cleanup(&g_alloc, &base);
    check_balance("apply_error_code");
}

/* -- publishTracks ---------------------------------------------------------- */

static void test_publish_tracks(void)
{
    moq_msf_catalog_t cat;
    moq_result_t prc = parse_fixture("5_6_17_publish_tracks.json", &cat);
    CHECK(prc == MOQ_OK);
    if (prc != MOQ_OK || cat.publish_track_count != 2) {
        if (prc == MOQ_OK) moq_msf_catalog_cleanup(&g_alloc, &cat);
        return;
    }
    CHECK(cat.track_count == 2 && cat.publish_track_count == 2);
    const moq_msf_track_t *m = &cat.publish_tracks[0];
    CHECK(bv_eq(m->name, "4") && bv_eq(m->packaging, "moqmetrics"));
    CHECK(m->has_role && bv_eq(m->role, "metrics") && m->is_live);
    CHECK(m->has_namespace &&
          bv_eq(m->namespace_, "metrics.moq.arpa/metrics-v1/%resourceId%"));

    /* publishTracks follow tracks and precede initDataList. */
    moq_msf_init_data_entry_t ide = { lit("i"), lit("inline"), lit("AAAA") };
    cat.init_data_list = &ide; cat.init_data_count = 1;
    moq_rcbuf_t *enc = NULL;
    CHECK_RC(moq_msf_catalog_encode(&g_alloc, &cat, &enc), MOQ_OK);
    size_t t = buf_find(enc, "\"tracks\":["), p = buf_find(enc, "\"publishTracks\":[");
    size_t i = buf_find(enc, "\"initDataList\":[");
    CHECK(t != SIZE_MAX && p != SIZE_MAX && i != SIZE_MAX && t < p && p < i);
    moq_rcbuf_decref(enc);
    cat.init_data_list = NULL; cat.init_data_count = 0;

    /* Full Track Names are unique within publishTracks ... */
    moq_msf_track_t pt[2] = { cat.publish_tracks[0], cat.publish_tracks[0] };
    moq_msf_track_t *saved = cat.publish_tracks;
    cat.publish_tracks = pt;
    CHECK_RC(moq_msf_catalog_encode(&g_alloc, &cat, &enc), MOQ_ERR_INVAL);
    /* ... but a name may be both a track and a publish track (8). */
    pt[1] = cat.tracks[0];
    CHECK_RC(moq_msf_catalog_encode(&g_alloc, &cat, &enc), MOQ_OK);
    moq_rcbuf_decref(enc);
    /* A publish track is a track: the same field rules apply. */
    pt[1].packaging = lit("");
    CHECK_RC(moq_msf_catalog_encode(&g_alloc, &cat, &enc), MOQ_ERR_INVAL);
    cat.publish_tracks = saved;

    /* The MSF-01 extent ignores the key. */
    moq_msf_catalog_t old;
    size_t n = 0;
    char *json = read_fixture("5_6_17_publish_tracks.json", &n);
    if (json) {
        char *v = strstr(json, "\"draft-02\"");
        if (v) memcpy(v, "\"draft-01\"", 10);
        CHECK_RC(frozen_parse(&g_alloc, (moq_bytes_t){ (const uint8_t *)json, n },
                              &old), MOQ_OK);
        CHECK(old.track_count == 2);
        moq_msf_catalog_cleanup(&g_alloc, &old);
        free(json);
    }

    /* A delta carries no publishTracks. */
    moq_msf_catalog_t d;
    CHECK_RC(parse_full("{\"generatedAt\":1,\"deltaUpdate\":[{\"op\":\"remove\","
                        "\"tracks\":[{\"name\":\"v\"}]}]}", &d), MOQ_OK);
    CHECK(d.publish_track_count == 0 && d.publish_tracks == NULL);
    d.publish_tracks = cat.publish_tracks; d.publish_track_count = 1;
    CHECK_RC(moq_msf_catalog_encode(&g_alloc, &d, &enc), MOQ_ERR_INVAL);
    d.publish_tracks = NULL; d.publish_track_count = 0;
    moq_msf_catalog_cleanup(&g_alloc, &d);

    /* A malformed publishTracks is malformed JSON for the catalog. */
    CHECK_RC(parse_full("{\"version\":\"draft-02\",\"tracks\":[],"
                        "\"publishTracks\":{}}", &d), MOQ_ERR_PROTO);
    CHECK_RC(parse_full("{\"version\":\"draft-02\",\"tracks\":[],"
                        "\"publishTracks\":[{\"name\":\"x\"}]}", &d), MOQ_ERR_PROTO);
    moq_msf_catalog_cleanup(&g_alloc, &cat);
    check_balance("publish_tracks");
}


/* -- The track supplement --------------------------------------------------- */

static const moq_msf_track_supplement_t *supp(const moq_msf_track_t *t)
{
    return moq_msf_track_supplement(t);
}

static void test_supplement_init_sized(void)
{
    static const size_t sizes[] = { 0, 3, MOQ_MSF_TRACK_SUPPLEMENT_V0_SIZE,
                                    sizeof(moq_msf_track_supplement_t),
                                    sizeof(moq_msf_track_supplement_t) + 8 };
    for (size_t i = 0; i < sizeof(sizes) / sizeof(sizes[0]); i++) {
        union {
            max_align_t align;
            moq_msf_track_supplement_t s;
            uint8_t bytes[sizeof(moq_msf_track_supplement_t) + 16];
        } u;
        memset(u.bytes, CANARY, sizeof(u.bytes));
        moq_msf_track_supplement_init_sized(&u.s, sizes[i]);
        size_t want = sizes[i] < sizeof(uint32_t) ? 0
                    : (sizes[i] < sizeof(u.s) ? sizes[i] : sizeof(u.s));
        for (size_t k = 0; k < sizeof(u.bytes); k++) {
            if (k < want) {
                if (k >= sizeof(uint32_t)) CHECK(u.bytes[k] == 0);
            } else {
                CHECK(u.bytes[k] == CANARY);
            }
        }
        if (want) CHECK(u.s.struct_size == want);
    }
    moq_msf_track_supplement_init_sized(NULL, 64);   /* no-op */
    moq_msf_track_supplement_t s;
    moq_msf_track_supplement_init(&s);
    CHECK(s.struct_size == sizeof(s) && !s.has_keyframe_spacing && s.malformed == 0);
    CHECK(MOQ_MSF_TRACK_SUPPLEMENT_V0_SIZE ==
          offsetof(moq_msf_track_supplement_t, malformed) + sizeof(uint32_t));
    /* Its entry stays one pointer wide. */
    CHECK(sizeof(moq_msf_track_ext_t) ==
          offsetof(moq_msf_track_ext_t, u) + sizeof(void *));
    CHECK(MOQ_MSF_EXT_SUPPLEMENT == 2u);
}

static void test_depends_forms(void)
{
    moq_msf_catalog_t cat;
    moq_result_t prc = parse_fixture("5_6_9_timelines.json", &cat);
    CHECK(prc == MOQ_OK);
    if (prc != MOQ_OK) return;
    const moq_msf_track_t *h = find_track(&cat, "history");
    const moq_msf_track_t *e = find_track(&cat, "identified-objects");
    CHECK(h && e);
    if (h && e) {
        CHECK(h->depends_count == 0);
        const moq_msf_track_supplement_t *hs = supp(h);
        CHECK(hs && hs->depends_render_group_count == 1 &&
              hs->depends_render_groups[0] == 1 && !hs->has_depends);
        CHECK(e->depends_count == 1 && bv_eq(e->depends[0], "history"));
        CHECK(supp(e) == NULL);
        /* MSF-02 timelines encode without mimeType; a media timeline whose
         * depends are only Numbers still has depends (7.2). */
        moq_rcbuf_t *enc = NULL;
        CHECK_RC(moq_msf_catalog_encode(&g_alloc, &cat, &enc), MOQ_OK);
        CHECK(buf_contains(enc, "\"depends\":[1]"));
        moq_rcbuf_decref(enc);
    }
    const moq_msf_track_t *v = find_track(&cat, "1080p-video");
    CHECK(v && supp(v) && supp(v)->has_accessible_window_ms &&
          supp(v)->accessible_window_ms == 300000);
    moq_msf_catalog_cleanup(&g_alloc, &cat);

    /* Strings and Numbers mixed; Numbers in the supplement, in order. */
    CHECK_RC(parse_full("{\"version\":\"draft-02\",\"tracks\":[{\"name\":\"a\","
                        "\"packaging\":\"loc\",\"isLive\":true,\"renderGroup\":3},"
                        "{\"name\":\"t\",\"packaging\":\"mediatimeline\","
                        "\"isLive\":true,\"depends\":[\"a\",3,-1,\"a\"]}]}", &cat),
             MOQ_OK);
    const moq_msf_track_t *t = find_track(&cat, "t");
    CHECK(t && t->depends_count == 2 && supp(t) &&
          supp(t)->depends_render_group_count == 2 &&
          supp(t)->depends_render_groups[0] == 3 &&
          supp(t)->depends_render_groups[1] == -1);
    moq_msf_catalog_cleanup(&g_alloc, &cat);

    /* A Number is MSF-02 vocabulary: an MSF-01 "depends" holding one stays
     * malformed, in both extents. */
    static const char r01_num[] =
        "{\"version\":\"draft-01\",\"tracks\":[{\"name\":\"t\",\"packaging\":"
        "\"mediatimeline\",\"isLive\":true,\"mimeType\":\"application/json\","
        "\"depends\":[1]}]}";
    CHECK_RC(parse_full(r01_num, &cat), MOQ_ERR_PROTO);
    CHECK_RC(frozen_parse(&g_alloc, lit(r01_num), &cat), MOQ_ERR_PROTO);
    /* An entry that is neither a String nor an integral Number. */
    static const char *const bad[] = { "1.5", "true", "null", "[1]", "{}",
                                       "1e3", "99999999999" };
    char json[256];
    for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); i++) {
        snprintf(json, sizeof(json),
                 "{\"version\":\"draft-02\",\"tracks\":[{\"name\":\"t\","
                 "\"packaging\":\"loc\",\"isLive\":true,\"depends\":[%s]}]}",
                 bad[i]);
        CHECK_RC(parse_full(json, &cat), MOQ_ERR_PROTO);
    }
    check_balance("depends_forms");
}

static moq_msf_catalog_error_t check_of(const char *json, moq_result_t want)
{
    moq_msf_catalog_t cat;
    moq_msf_catalog_error_t why = 99;
    moq_result_t rc = parse_full(json, &cat);
    CHECK(rc == MOQ_OK);
    if (rc != MOQ_OK) return 99;
    CHECK_RC(moq_msf_catalog_check(&g_alloc, &cat, NULL, &why), want);
    moq_msf_catalog_cleanup(&g_alloc, &cat);
    return why;
}

#define TRK(n, extra) "{\"name\":\"" n "\",\"packaging\":\"loc\",\"isLive\":true" extra "}"
#define R02(tracks) "{\"version\":\"draft-02\",\"tracks\":[" tracks "]}"
#define R01(tracks) "{\"version\":\"1\",\"tracks\":[" tracks "]}"

static void test_check_depends(void)
{
    static const char *const ok[] = {
        "5_6_2_simulcast.json", "5_6_3_svc.json", "5_6_9_timelines.json",
        "5_6_11_captions_scte35.json",
    };
    for (size_t i = 0; i < sizeof(ok) / sizeof(ok[0]); i++) {
        moq_msf_catalog_t cat;
        CHECK_RC(parse_fixture(ok[i], &cat), MOQ_OK);
        moq_msf_catalog_error_t why = 99;
        CHECK_RC(moq_msf_catalog_check(&g_alloc, &cat, NULL, &why), MOQ_OK);
        CHECK(why == MOQ_MSF_CATALOG_OK);
        moq_msf_catalog_cleanup(&g_alloc, &cat);
    }
    /* No match; only the track itself; two tracks named alike (in two
     * namespaces); an unknown renderGroup. */
    CHECK(check_of(R02(TRK("a", ",\"depends\":[\"b\"]")), MOQ_ERR_PROTO) ==
          MOQ_MSF_CATALOG_ERR_DEPENDS_UNRESOLVED);
    CHECK(check_of(R02(TRK("a", ",\"depends\":[\"a\"]")), MOQ_ERR_PROTO) ==
          MOQ_MSF_CATALOG_ERR_DEPENDS_UNRESOLVED);
    CHECK(check_of(R02(TRK("a", ",\"depends\":[\"b\"]") ","
                       TRK("b", ",\"namespace\":\"x\"") ","
                       TRK("b", ",\"namespace\":\"y\"")), MOQ_ERR_PROTO) ==
          MOQ_MSF_CATALOG_ERR_DEPENDS_AMBIGUOUS);
    CHECK(check_of(R02(TRK("a", ",\"renderGroup\":1") ","
                       TRK("b", ",\"depends\":[2]")), MOQ_ERR_PROTO) ==
          MOQ_MSF_CATALOG_ERR_DEPENDS_UNRESOLVED);
    /* A Number naming only the declaring track's own renderGroup resolves:
     * 5.2.15 asks for a renderGroup in the catalog, not another track. */
    CHECK(check_of(R02(TRK("a", ",\"renderGroup\":4,\"depends\":[4]")), MOQ_OK) ==
          MOQ_MSF_CATALOG_OK);
    /* A String naming a track elsewhere resolves; so does one naming a
     * track that shares the declaring track's name in another namespace. */
    CHECK(check_of(R02(TRK("a", ",\"namespace\":\"x\",\"depends\":[\"a\"]") ","
                       TRK("a", ",\"namespace\":\"y\"")), MOQ_OK) ==
          MOQ_MSF_CATALOG_OK);
    /* MSF-01 has no such rule. */
    CHECK(check_of(R01(TRK("a", ",\"depends\":[\"zz\"]")), MOQ_OK) ==
          MOQ_MSF_CATALOG_OK);

    /* Authoring refuses what the check refuses: an MSF-02 catalog whose
     * depends do not resolve does not encode; MSF-01 has no such rule. */
    static const char *const dangling[] = {
        R02(TRK("a", ",\"depends\":[\"b\"]")),
        R02(TRK("a", ",\"depends\":[3]")),
        R02(TRK("a", ",\"depends\":[\"b\"]") "," TRK("b", ",\"namespace\":\"x\"")
            "," TRK("b", ",\"namespace\":\"y\"")),
    };
    for (size_t i = 0; i < sizeof(dangling) / sizeof(dangling[0]); i++) {
        moq_msf_catalog_t cat;
        CHECK_RC(parse_full(dangling[i], &cat), MOQ_OK);
        moq_rcbuf_t *enc = NULL;
        moq_result_t rc = moq_msf_catalog_encode(&g_alloc, &cat, &enc);
        if (rc != MOQ_ERR_INVAL) {
            fprintf(stderr, "FAIL: dangling depends row %zu encoded: %d\n", i, rc);
            failures++;
            moq_rcbuf_decref(enc);
        }
        moq_msf_catalog_cleanup(&g_alloc, &cat);
    }
    moq_msf_catalog_t r01;
    CHECK_RC(parse_full(R01(TRK("a", ",\"depends\":[\"b\"]")), &r01), MOQ_OK);
    moq_rcbuf_t *enc = NULL;
    CHECK_RC(moq_msf_catalog_encode(&g_alloc, &r01, &enc), MOQ_OK);
    moq_rcbuf_decref(enc);
    moq_msf_catalog_cleanup(&g_alloc, &r01);
    check_balance("check_depends");
}

static void test_check_reason_values(void)
{
    CHECK(MOQ_MSF_CATALOG_OK == 0);
    CHECK(MOQ_MSF_CATALOG_ERR_DUPLICATE_TRACK == 1);
    CHECK(MOQ_MSF_CATALOG_ERR_DUPLICATE_INIT_ID == 2);
    CHECK(MOQ_MSF_CATALOG_ERR_DUPLICATE_CP_REF == 3);
    CHECK(MOQ_MSF_CATALOG_ERR_DEPENDS_UNRESOLVED == 4);
    CHECK(MOQ_MSF_CATALOG_ERR_DEPENDS_AMBIGUOUS == 5);
    CHECK(MOQ_MSF_CATALOG_ERR_DUPLICATE_PUBLISH_TRACK == 6);

    /* Each reason from its violation, in both revisions where it applies. */
    CHECK(check_of(R02(TRK("a", "") "," TRK("a", "")), MOQ_ERR_PROTO) ==
          MOQ_MSF_CATALOG_ERR_DUPLICATE_TRACK);
    CHECK(check_of(R01(TRK("a", "") "," TRK("a", "")), MOQ_ERR_PROTO) ==
          MOQ_MSF_CATALOG_ERR_DUPLICATE_TRACK);
    CHECK(check_of("{\"version\":\"draft-02\",\"tracks\":[],\"initDataList\":["
                   "{\"id\":\"i\",\"type\":\"inline\",\"data\":\"\"},"
                   "{\"id\":\"i\",\"type\":\"inline\",\"data\":\"\"}]}",
                   MOQ_ERR_PROTO) == MOQ_MSF_CATALOG_ERR_DUPLICATE_INIT_ID);
    CHECK(check_of("{\"version\":\"1\",\"tracks\":[],\"contentProtections\":["
                   "{\"refID\":\"r\",\"defaultKID\":[\"k\"],\"scheme\":\"cenc\","
                   "\"drmSystem\":{\"systemID\":\"s\"}},"
                   "{\"refID\":\"r\",\"defaultKID\":[\"k\"],\"scheme\":\"cenc\","
                   "\"drmSystem\":{\"systemID\":\"s\"}}]}",
                   MOQ_ERR_PROTO) == MOQ_MSF_CATALOG_ERR_DUPLICATE_CP_REF);
    CHECK(check_of("{\"version\":\"draft-02\",\"tracks\":[" TRK("p", "") "],"
                   "\"publishTracks\":[" TRK("p", "") "," TRK("p", "") "]}",
                   MOQ_ERR_PROTO) == MOQ_MSF_CATALOG_ERR_DUPLICATE_PUBLISH_TRACK);
    /* The same name in tracks and publishTracks is fine (MSF-02 8). */
    CHECK(check_of("{\"version\":\"draft-02\",\"tracks\":[" TRK("p", "") "],"
                   "\"publishTracks\":[" TRK("p", "") "]}", MOQ_OK) ==
          MOQ_MSF_CATALOG_OK);

    /* Argument and storage refusals. */
    moq_msf_catalog_t cat, d;
    CHECK_RC(parse_full(R02(TRK("a", "")), &cat), MOQ_OK);
    CHECK_RC(moq_msf_catalog_check(NULL, &cat, NULL, NULL), MOQ_ERR_INVAL);
    CHECK_RC(moq_msf_catalog_check(&g_alloc, NULL, NULL, NULL), MOQ_ERR_INVAL);
    CHECK_RC(moq_msf_catalog_check(&g_alloc, &cat, NULL, NULL), MOQ_OK);
    moq_msf_validate_cfg_t cfg;
    moq_msf_validate_cfg_init(&cfg);
    CHECK_RC(moq_msf_catalog_check(&g_alloc, &cat, &cfg, NULL), MOQ_OK);
    cfg.struct_size = MOQ_MSF_VALIDATE_CFG_V0_SIZE - 1;
    CHECK_RC(moq_msf_catalog_check(&g_alloc, &cat, &cfg, NULL), MOQ_ERR_INVAL);
    moq_msf_validate_cfg_init(&cfg);
    moq_bytes_t empty_field = { (const uint8_t *)"", 0 };
    cfg.catalog_namespace = (moq_namespace_t){ &empty_field, 1 };
    CHECK_RC(moq_msf_catalog_check(&g_alloc, &cat, &cfg, NULL), MOQ_ERR_INVAL);
    uint32_t ss = cat.struct_size;
    cat.struct_size = (uint32_t)(MOQ_MSF_CATALOG_SIZE_THROUGH_PUBLISH_TRACKS - 1);
    CHECK_RC(moq_msf_catalog_check(&g_alloc, &cat, NULL, NULL), MOQ_ERR_INVAL);
    cat.struct_size = ss;
    cat.revision = 7;
    CHECK_RC(moq_msf_catalog_check(&g_alloc, &cat, NULL, NULL), MOQ_ERR_INVAL);
    cat.revision = MOQ_MSF_REVISION_02;
    moq_msf_track_t *tp = cat.tracks;
    cat.tracks = NULL;
    CHECK_RC(moq_msf_catalog_check(&g_alloc, &cat, NULL, NULL), MOQ_ERR_INVAL);
    cat.tracks = tp;
    moq_msf_catalog_cleanup(&g_alloc, &cat);
    CHECK_RC(parse_full("{\"generatedAt\":1,\"deltaUpdate\":[{\"op\":\"remove\","
                        "\"tracks\":[{\"name\":\"a\"}]}]}", &d), MOQ_OK);
    CHECK_RC(moq_msf_catalog_check(&g_alloc, &d, NULL, NULL), MOQ_ERR_INVAL);
    CHECK_RC(moq_msf_catalog_validate_identities(&g_alloc, &d), MOQ_ERR_INVAL);
    moq_msf_catalog_cleanup(&g_alloc, &d);

    /* Allocation failure: every attempt ends at zero balance. */
    CHECK_RC(parse_fixture("5_6_9_timelines.json", &cat), MOQ_OK);
    for (uint64_t fail = 1; fail < 16; fail++) {
        g_count.calls = 0;
        g_count.fail_at = fail;
        moq_result_t rc = moq_msf_catalog_check(&g_alloc, &cat, NULL, NULL);
        CHECK(rc == MOQ_OK || rc == MOQ_ERR_NOMEM);
    }
    g_count.fail_at = 0;
    moq_msf_catalog_cleanup(&g_alloc, &cat);
    check_balance("check_reason_values");
}

static void test_supplement_r01_unknown_keys(void)
{
    /* MSF-01: the MSF-02-only keys are unknown keys: not parsed, no record. */
    moq_msf_catalog_t cat;
    CHECK_RC(parse_full(R01(TRK("a", ",\"keyframeSpacing\":15,"
                               "\"accessibleWindowTime\":5,"
                               "\"packagingMetadata\":\"streamMapping=object\"")),
                        &cat), MOQ_OK);
    CHECK(cat.tracks[0].extension_count == 0 && supp(&cat.tracks[0]) == NULL);
    moq_msf_catalog_cleanup(&g_alloc, &cat);
    /* ... the encryption keys are MSF-01 fields too. */
    CHECK_RC(parse_full(R01(TRK("a", ",\"keyframeSpacing\":15,"
                               "\"encryptionScheme\":\"moq-secure-objects\"")),
                        &cat), MOQ_OK);
    const moq_msf_track_supplement_t *s = supp(&cat.tracks[0]);
    CHECK(s && s->has_encryption_scheme && !s->has_keyframe_spacing);
    moq_rcbuf_t *enc = NULL;
    CHECK_RC(moq_msf_catalog_encode(&g_alloc, &cat, &enc), MOQ_OK);
    CHECK(buf_contains(enc, "\"encryptionScheme\":\"moq-secure-objects\""));
    CHECK(!buf_contains(enc, "keyframeSpacing"));
    moq_rcbuf_decref(enc);
    moq_msf_catalog_cleanup(&g_alloc, &cat);

    /* Authoring: an MSF-02 field in an MSF-01 catalog is refused. */
    moq_msf_track_supplement_t rec;
    moq_msf_track_supplement_init(&rec);
    rec.has_keyframe_spacing = true; rec.keyframe_spacing = 15;
    moq_msf_track_ext_t ext = { .kind = MOQ_MSF_EXT_SUPPLEMENT,
                                .u.supplement = &rec };
    moq_msf_track_t t;
    memset(&t, 0, sizeof(t));
    t.struct_size = sizeof(t);
    t.name = lit("a"); t.packaging = lit("loc"); t.is_live = true;
    t.extensions = &ext; t.extension_count = 1;
    moq_msf_catalog_t c;
    memset(&c, 0, sizeof(c));
    c.struct_size = sizeof(c);
    c.version = MOQ_MSF_VERSION;
    c.tracks = &t; c.track_count = 1;
    CHECK_RC(moq_msf_catalog_encode(&g_alloc, &c, &enc), MOQ_ERR_INVAL);
    c.revision = MOQ_MSF_REVISION_02;
    CHECK_RC(moq_msf_catalog_encode(&g_alloc, &c, &enc), MOQ_OK);
    CHECK(buf_contains(enc, "\"keyframeSpacing\":15"));
    moq_rcbuf_decref(enc);
    /* Malformed bits never author. */
    rec.malformed = MOQ_MSF_SUPPLEMENT_F_NAMESPACE;
    CHECK_RC(moq_msf_catalog_encode(&g_alloc, &c, &enc), MOQ_ERR_INVAL);
    rec.malformed = 0;
    /* The list rules: the record's size, a NULL record, a kind twice. */
    rec.struct_size = MOQ_MSF_TRACK_SUPPLEMENT_V0_SIZE - 1;
    CHECK_RC(moq_msf_catalog_encode(&g_alloc, &c, &enc), MOQ_ERR_INVAL);
    rec.struct_size = MOQ_MSF_TRACK_SUPPLEMENT_V0_SIZE;
    CHECK_RC(moq_msf_catalog_encode(&g_alloc, &c, &enc), MOQ_OK);
    moq_rcbuf_decref(enc);
    rec.struct_size = sizeof(rec);
    ext.u.supplement = NULL;
    CHECK_RC(moq_msf_catalog_encode(&g_alloc, &c, &enc), MOQ_ERR_INVAL);
    ext.u.supplement = &rec;
    moq_msf_track_ext_t two[2] = { ext, ext };
    t.extensions = two; t.extension_count = 2;
    CHECK_RC(moq_msf_catalog_encode(&g_alloc, &c, &enc), MOQ_ERR_INVAL);
    two[1].kind = 9;
    CHECK_RC(moq_msf_catalog_encode(&g_alloc, &c, &enc), MOQ_ERR_INVAL);
    t.extensions = &ext; t.extension_count = 1;
    /* Any packaging carries it. */
    t.packaging = lit("moqlog");
    CHECK_RC(moq_msf_catalog_encode(&g_alloc, &c, &enc), MOQ_OK);
    moq_rcbuf_decref(enc);
    check_balance("supplement_r01_unknown_keys");
}

static void test_encryption_fields_both_revisions(void)
{
    size_t n = 0;
    char *json = read_fixture("5_6_8_encrypted.json", &n);
    if (!json) return;
    for (int r = 0; r < 2; r++) {
        if (r == 1) {
            char *v = strstr(json, "\"draft-02\"");
            if (v) memcpy(v, "\"draft-01\"", 10);
        }
        moq_msf_catalog_t cat;
        CHECK_RC(moq_msf_catalog_parse(&g_alloc,
                     (moq_bytes_t){ (const uint8_t *)json, n }, &cat), MOQ_OK);
        CHECK(cat.revision == (r ? MOQ_MSF_REVISION_01 : MOQ_MSF_REVISION_02));
        for (size_t i = 0; i < cat.track_count; i++) {
            const moq_msf_track_supplement_t *s = supp(&cat.tracks[i]);
            CHECK(s != NULL);
            if (!s) continue;
            CHECK(s->has_encryption_scheme &&
                  bv_eq(s->encryption_scheme, "moq-secure-objects"));
            CHECK(s->has_cipher_suite && bv_eq(s->cipher_suite, "aes-128-gcm-sha256"));
            CHECK(s->has_key_id && bv_eq(s->key_id, "key-2024-q1-premium"));
            CHECK(s->has_track_base_key &&
                  bv_eq(s->track_base_key, "MTIzNDU2Nzg5MDEyMzQ1Ng=="));
            CHECK(!s->has_keyframe_spacing && !s->has_packaging_metadata &&
                  !s->has_accessible_window_ms);
        }
        moq_rcbuf_t *enc = NULL;
        CHECK_RC(moq_msf_catalog_encode(&g_alloc, &cat, &enc), MOQ_OK);
        CHECK(buf_contains(enc, "\"cipherSuite\":\"aes-128-gcm-sha256\""));
        CHECK(buf_contains(enc, "\"trackBaseKey\":\"MTIzNDU2Nzg5MDEyMzQ1Ng==\""));
        moq_msf_catalog_t back;
        CHECK_RC(moq_msf_catalog_parse(&g_alloc,
                     (moq_bytes_t){ moq_rcbuf_data(enc), moq_rcbuf_len(enc) },
                     &back), MOQ_OK);
        CHECK(back.track_count == cat.track_count);
        for (size_t i = 0; i < cat.track_count && i < back.track_count; i++) {
            const moq_msf_track_supplement_t *a = supp(&cat.tracks[i]);
            const moq_msf_track_supplement_t *b = supp(&back.tracks[i]);
            CHECK(a && b && bv_eq(b->key_id, "key-2024-q1-premium") &&
                  a->has_track_base_key == b->has_track_base_key);
        }
        moq_msf_catalog_cleanup(&g_alloc, &back);
        moq_rcbuf_decref(enc);
        moq_msf_catalog_cleanup(&g_alloc, &cat);
    }
    /* Leniency: a non-String value is absent. */
    moq_msf_catalog_t cat;
    CHECK_RC(parse_full(R02(TRK("a", ",\"encryptionScheme\":5,\"keyId\":\"k\"")),
                        &cat), MOQ_OK);
    const moq_msf_track_supplement_t *s = supp(&cat.tracks[0]);
    CHECK(s && !s->has_encryption_scheme && s->has_key_id);
    moq_msf_catalog_cleanup(&g_alloc, &cat);
    free(json);
    check_balance("encryption_fields_both_revisions");
}

static void test_supplement_clone_merge(void)
{
    static const char base_r02[] =
        "{\"version\":\"draft-02\",\"generatedAt\":1,\"tracks\":["
        "{\"name\":\"p\",\"packaging\":\"loc\",\"isLive\":true,\"renderGroup\":2,"
        "\"encryptionScheme\":\"moq-secure-objects\",\"cipherSuite\":\"c\","
        "\"keyId\":\"k1\",\"keyframeSpacing\":30},"
        "{\"name\":\"q\",\"packaging\":\"loc\",\"isLive\":true,"
        "\"depends\":[\"p\",2]}]}";
    moq_msf_catalog_t base, d, out;
    CHECK_RC(parse_full(base_r02, &base), MOQ_OK);
    CHECK_RC(parse_full("{\"generatedAt\":2,\"deltaUpdate\":[{\"op\":\"clone\","
                        "\"tracks\":[{\"parentName\":\"p\",\"name\":\"p2\","
                        "\"keyId\":\"k2\",\"accessibleWindowTime\":9},"
                        "{\"parentName\":\"q\",\"name\":\"q2\","
                        "\"depends\":[]}]}]}", &d), MOQ_OK);
    CHECK(d.revision == MOQ_MSF_REVISION_02);   /* MSF-02 vocabulary */
    const moq_msf_track_supplement_t *ds = supp(&d.delta_update[0].tracks[1]);
    CHECK(ds && ds->has_depends && ds->depends_render_group_count == 0);
    CHECK_RC(moq_msf_catalog_apply_delta_sized(&g_alloc, &base, &d, NULL, &out,
                                               sizeof(out)), MOQ_OK);
    const moq_msf_track_t *p2 = find_track(&out, "p2");
    const moq_msf_track_t *p = find_track(&out, "p");
    const moq_msf_track_t *q2 = find_track(&out, "q2");
    CHECK(p2 && p && q2);
    if (p2 && p && q2) {
        const moq_msf_track_supplement_t *s = supp(p2);
        CHECK(s && bv_eq(s->encryption_scheme, "moq-secure-objects") &&
              bv_eq(s->cipher_suite, "c") && bv_eq(s->key_id, "k2") &&
              s->has_keyframe_spacing && s->keyframe_spacing == 30 &&
              s->has_accessible_window_ms && s->accessible_window_ms == 9 &&
              !s->has_depends);
        CHECK(bv_eq(supp(p)->key_id, "k1") && !supp(p)->has_accessible_window_ms);
        /* "depends":[] on the clone cleared both lists. */
        CHECK(q2->depends_count == 0 && supp(q2) == NULL);
    }
    moq_msf_catalog_cleanup(&g_alloc, &out);
    moq_msf_catalog_cleanup(&g_alloc, &base);

    /* MSF-01: the clone's MSF-02 fields are dropped, its encryption fields
     * kept, and an empty "depends" inherits the parent's (as before). */
    CHECK_RC(parse_full("{\"version\":\"1\",\"tracks\":["
                        "{\"name\":\"p\",\"packaging\":\"loc\",\"isLive\":true,"
                        "\"keyId\":\"k1\"},"
                        "{\"name\":\"q\",\"packaging\":\"loc\",\"isLive\":true,"
                        "\"depends\":[\"p\"]}]}", &base), MOQ_OK);
    CHECK_RC(moq_msf_catalog_apply_delta_sized(&g_alloc, &base, &d, NULL, &out,
                                               sizeof(out)), MOQ_OK);
    p2 = find_track(&out, "p2");
    q2 = find_track(&out, "q2");
    CHECK(p2 && q2);
    if (p2 && q2) {
        CHECK(supp(p2) && bv_eq(supp(p2)->key_id, "k2") &&
              !supp(p2)->has_accessible_window_ms);
        CHECK(q2->depends_count == 1 && bv_eq(q2->depends[0], "p"));
    }
    moq_msf_catalog_cleanup(&g_alloc, &out);
    moq_msf_catalog_cleanup(&g_alloc, &d);

    /* MSF-01: a Number "depends" in a delta is refused. */
    CHECK_RC(parse_full("{\"deltaUpdate\":[{\"op\":\"add\",\"tracks\":["
                        "{\"name\":\"n\",\"packaging\":\"loc\",\"isLive\":true,"
                        "\"depends\":[2]}]}]}", &d), MOQ_OK);
    CHECK_RC(moq_msf_catalog_apply_delta_sized(&g_alloc, &base, &d, NULL, &out,
                                               sizeof(out)), MOQ_ERR_PROTO);
    moq_msf_catalog_cleanup(&g_alloc, &d);
    moq_msf_catalog_cleanup(&g_alloc, &base);

    /* An mpeg2ts record and a supplement on one track merge kind by kind. */
    CHECK_RC(parse_full("{\"version\":\"draft-02\",\"generatedAt\":1,\"tracks\":["
                        "{\"name\":\"ts\",\"packaging\":\"mpeg2ts\",\"isLive\":true,"
                        "\"mpeg2tsMode\":\"per-program\",\"mpeg2tsPacketSize\":188,"
                        "\"keyId\":\"a\"}]}", &base), MOQ_OK);
    CHECK(base.tracks[0].extension_count == 2 &&
          base.tracks[0].extensions[0].kind == MOQ_MSF_EXT_MPEG2TS &&
          base.tracks[0].extensions[1].kind == MOQ_MSF_EXT_SUPPLEMENT);
    CHECK_RC(parse_full("{\"generatedAt\":2,\"deltaUpdate\":[{\"op\":\"clone\","
                        "\"tracks\":[{\"parentName\":\"ts\",\"name\":\"ts2\","
                        "\"mpeg2tsProgramNumber\":3,\"keyId\":\"b\"}]}]}", &d),
             MOQ_OK);
    CHECK_RC(moq_msf_catalog_apply_delta_sized(&g_alloc, &base, &d, NULL, &out,
                                               sizeof(out)), MOQ_OK);
    const moq_msf_track_t *ts2 = find_track(&out, "ts2");
    CHECK(ts2 && moq_msf_track_mpeg2ts(ts2) &&
          moq_msf_track_mpeg2ts(ts2)->has_program_number &&
          moq_msf_track_mpeg2ts(ts2)->packet_size == 188 &&
          supp(ts2) && bv_eq(supp(ts2)->key_id, "b"));
    moq_msf_catalog_cleanup(&g_alloc, &out);
    moq_msf_catalog_cleanup(&g_alloc, &d);
    moq_msf_catalog_cleanup(&g_alloc, &base);
    check_balance("supplement_clone_merge");
}


/* -- "update" and the MSF-02 delta rules ------------------------------------ */

/* apply_json expecting `want`; a success is released here. */
#define CHECK_APPLY(base, delta, want) do { \
    moq_msf_catalog_t _o; \
    moq_result_t _r = apply_json((base), (delta), &_o); \
    if (_r != (want)) { \
        fprintf(stderr, "FAIL: %s:%d: apply == %d, expected %d\n", \
                __FILE__, __LINE__, _r, (int)(want)); \
        failures++; \
    } \
    if (_r == MOQ_OK) moq_msf_catalog_cleanup(&g_alloc, &_o); \
} while (0)

static moq_result_t apply_json(const moq_msf_catalog_t *base, const char *delta,
                               moq_msf_catalog_t *out)
{
    moq_msf_catalog_t d;
    moq_result_t rc = parse_full(delta, &d);
    if (rc != MOQ_OK) {
        memset(out, 0, sizeof(*out));     /* safe to clean up */
        out->struct_size = sizeof(*out);
        return rc;
    }
    rc = moq_msf_catalog_apply_delta_sized(&g_alloc, base, &d, NULL, out,
                                           sizeof(*out));
    moq_msf_catalog_cleanup(&g_alloc, &d);
    return rc;
}

static const moq_msf_track_t *find_ns(const moq_msf_catalog_t *c,
                                      const char *name, const char *ns)
{
    for (size_t i = 0; i < c->track_count; i++) {
        const moq_msf_track_t *t = &c->tracks[i];
        if (!bv_eq(t->name, name)) continue;
        if (ns ? (t->has_namespace && bv_eq(t->namespace_, ns)) : !t->has_namespace)
            return t;
    }
    return NULL;
}

static void test_update_example(void)
{
    moq_msf_catalog_t base, d, eff, eff2;
    moq_result_t prc = parse_fixture("delta_base.json", &base);
    CHECK(prc == MOQ_OK);
    if (prc != MOQ_OK) return;
    prc = parse_fixture("5_6_4_delta_update_add.json", &d);
    CHECK(prc == MOQ_OK);
    if (prc != MOQ_OK) { moq_msf_catalog_cleanup(&g_alloc, &base); return; }
    CHECK(d.revision == MOQ_MSF_REVISION_02);
    CHECK(d.delta_update_count == 3 &&
          d.delta_update[0].op == MOQ_MSF_DELTA_OP_UPDATE);
    const moq_msf_track_t *u = &d.delta_update[0].tracks[0];
    CHECK(u->has_parent_name && bv_eq(u->parent_name, "video-1080") &&
          u->name.len == 0 && u->has_bitrate && u->bitrate == 4000000);
    CHECK_RC(moq_msf_catalog_apply_delta_sized(&g_alloc, &base, &d, NULL, &eff,
                                               sizeof(eff)), MOQ_OK);
    CHECK(eff.track_count == 6 && eff.revision == MOQ_MSF_REVISION_02);
    /* update: the inherited video-1080 in place; the custom one untouched. */
    const moq_msf_track_t *v1080 = find_ns(&eff, "video-1080", NULL);
    const moq_msf_track_t *c1080 = find_ns(&eff, "video-1080", "example.com/custom");
    CHECK(v1080 && v1080->bitrate == 4000000 && v1080->width == 1920 &&
          !v1080->has_parent_name && v1080 == &eff.tracks[1]);
    CHECK(c1080 && c1080->bitrate == 2000000);
    /* add, then clone from the custom parent (its namespace inherited). */
    const moq_msf_track_t *slides = find_ns(&eff, "slides", NULL);
    const moq_msf_track_t *v720 = find_ns(&eff, "video-720", "example.com/custom");
    CHECK(slides && slides->framerate_millis == 15000 && slides == &eff.tracks[4]);
    CHECK(v720 && v720->width == 1280 && v720->height == 720 &&
          v720->bitrate == 600000 && bv_eq(v720->codec, "av01.0.08M.10.0.110.09") &&
          v720 == &eff.tracks[5]);
    CHECK(eff.has_generated_at && eff.generated_at == 1746104606044ULL);
    moq_msf_catalog_cleanup(&g_alloc, &d);

    /* 5.6.5 then removes "video" and "slides". */
    CHECK_RC(parse_fixture("5_6_5_delta_remove.json", &d), MOQ_OK);
    CHECK_RC(moq_msf_catalog_apply_delta_sized(&g_alloc, &eff, &d, NULL, &eff2,
                                               sizeof(eff2)), MOQ_OK);
    CHECK(eff2.track_count == 4 && !find_ns(&eff2, "video", NULL) &&
          !find_ns(&eff2, "slides", NULL) && find_ns(&eff2, "video-720",
                                                     "example.com/custom"));
    moq_msf_catalog_cleanup(&g_alloc, &eff2);
    moq_msf_catalog_cleanup(&g_alloc, &d);
    moq_msf_catalog_cleanup(&g_alloc, &eff);

    /* The update carries MSF-02 vocabulary, so it round-trips as such. */
    CHECK_RC(parse_fixture("5_6_4_delta_update_add.json", &d), MOQ_OK);
    moq_rcbuf_t *enc = NULL;
    CHECK_RC(moq_msf_catalog_encode(&g_alloc, &d, &enc), MOQ_OK);
    CHECK(buf_contains(enc, "{\"op\":\"update\",\"tracks\":[{\"parentName\":"
                            "\"video-1080\",\"bitrate\":4000000}]}"));
    moq_msf_catalog_t back;
    CHECK_RC(moq_msf_catalog_parse(&g_alloc,
                 (moq_bytes_t){ moq_rcbuf_data(enc), moq_rcbuf_len(enc) }, &back),
             MOQ_OK);
    CHECK(back.delta_update_count == 3 &&
          back.delta_update[0].op == MOQ_MSF_DELTA_OP_UPDATE &&
          back.revision == MOQ_MSF_REVISION_02);
    moq_msf_catalog_cleanup(&g_alloc, &back);
    moq_rcbuf_decref(enc);
    /* MSF-01 cannot carry it: the MSF-01 extent refuses the op, an MSF-01
     * base refuses it at apply, and an MSF-01 delta does not encode it. */
    size_t n = 0;
    char *json = read_fixture("5_6_4_delta_update_add.json", &n);
    if (json) {
        CHECK_RC(frozen_parse(&g_alloc, (moq_bytes_t){ (const uint8_t *)json, n },
                              &back), MOQ_ERR_PROTO);
        free(json);
    }
    d.revision = MOQ_MSF_REVISION_01;
    CHECK_RC(moq_msf_catalog_encode(&g_alloc, &d, &enc), MOQ_ERR_INVAL);
    d.revision = MOQ_MSF_REVISION_02;
    moq_msf_catalog_t r01;
    CHECK_RC(parse_full("{\"version\":\"1\",\"tracks\":[{\"name\":\"video-1080\","
                        "\"packaging\":\"loc\",\"isLive\":true}]}", &r01), MOQ_OK);
    moq_msf_catalog_t out;
    CHECK_RC(moq_msf_catalog_apply_delta_sized(&g_alloc, &r01, &d, NULL, &out,
                                               sizeof(out)), MOQ_ERR_PROTO);
    moq_msf_catalog_cleanup(&g_alloc, &r01);
    moq_msf_catalog_cleanup(&g_alloc, &d);
    moq_msf_catalog_cleanup(&g_alloc, &base);
    check_balance("update_example");
}

#define UPD(body) "{\"generatedAt\":9,\"deltaUpdate\":[{\"op\":\"update\"," \
                  "\"tracks\":[" body "]}]}"

static void test_update_rules(void)
{
    static const char base_json[] =
        "{\"version\":\"draft-02\",\"generatedAt\":1,\"tracks\":["
        "{\"name\":\"live\",\"packaging\":\"loc\",\"isLive\":true,"
        "\"keyId\":\"k\",\"depends\":[\"vod\"]},"
        "{\"name\":\"vod\",\"namespace\":\"a/b\",\"packaging\":\"loc\","
        "\"isLive\":false,\"trackDuration\":5,"
        "\"template\":[0,2,[0,0],[1,0],0,0]},"
        "{\"name\":\"plain\",\"packaging\":\"loc\",\"isLive\":true}]}";
    moq_msf_catalog_t base, out;
    CHECK_RC(parse_full(base_json, &base), MOQ_OK);
    static const struct { const char *ops; moq_result_t want; } rows[] = {
        /* the target, by parentName (+ parentNamespace) */
        { UPD("{\"parentName\":\"live\",\"bitrate\":5}"),               MOQ_OK },
        { UPD("{\"parentName\":\"vod\",\"parentNamespace\":\"a/b\","
              "\"label\":\"x\"}"),                                      MOQ_OK },
        { UPD("{\"parentName\":\"vod\",\"label\":\"x\"}"),               MOQ_ERR_PROTO },
        { UPD("{\"parentName\":\"nope\",\"label\":\"x\"}"),              MOQ_ERR_PROTO },
        /* a name or namespace equal to the target's; never another */
        { UPD("{\"parentName\":\"live\",\"name\":\"live\"}"),            MOQ_OK },
        { UPD("{\"parentName\":\"live\",\"name\":\"other\"}"),           MOQ_ERR_PROTO },
        { UPD("{\"parentName\":\"vod\",\"parentNamespace\":\"a/b\","
              "\"namespace\":\"a/b\"}"),                                MOQ_OK },
        { UPD("{\"parentName\":\"vod\",\"parentNamespace\":\"a/b\","
              "\"namespace\":\"a/c\"}"),                                MOQ_ERR_PROTO },
        { UPD("{\"parentName\":\"live\",\"namespace\":\"x\"}"),          MOQ_ERR_PROTO },
        /* isLive never false -> true; true -> false is the 11.4 end */
        { UPD("{\"parentName\":\"vod\",\"parentNamespace\":\"a/b\","
              "\"isLive\":true}"),                                      MOQ_ERR_PROTO },
        { UPD("{\"parentName\":\"live\",\"isLive\":false,"
              "\"trackDuration\":7}"),                                  MOQ_OK },
        /* a present template never changes; one may be added */
        { UPD("{\"parentName\":\"vod\",\"parentNamespace\":\"a/b\","
              "\"template\":[0,2,[0,0],[1,0],0,0]}"),                   MOQ_OK },
        { UPD("{\"parentName\":\"vod\",\"parentNamespace\":\"a/b\","
              "\"template\":[0,3,[0,0],[1,0],0,0]}"),                   MOQ_ERR_PROTO },
        { UPD("{\"parentName\":\"plain\",\"template\":[0,3,[0,0],[1,0],0,0]}"),
                                                                        MOQ_OK },
        /* an update that leaves a live track with a duration */
        { UPD("{\"parentName\":\"plain\",\"trackDuration\":4}"),         MOQ_ERR_PROTO },
        /* the update op never carries packaging-less naming problems */
        { UPD("{\"parentName\":\"live\",\"packaging\":\"cmaf\"}"),       MOQ_OK },
    };
    for (size_t i = 0; i < sizeof(rows) / sizeof(rows[0]); i++) {
        moq_result_t rc = apply_json(&base, rows[i].ops, &out);
        if (rc != rows[i].want) {
            fprintf(stderr, "FAIL: update_rules row %zu: %d, expected %d\n",
                    i, rc, rows[i].want);
            failures++;
        }
        if (rc == MOQ_OK) moq_msf_catalog_cleanup(&g_alloc, &out);
    }

    /* Field by field: the update's fields override; the rest stay. */
    CHECK_RC(apply_json(&base, UPD("{\"parentName\":\"live\",\"bitrate\":5,"
                                   "\"keyframeSpacing\":30,\"depends\":[]}"),
                        &out), MOQ_OK);
    const moq_msf_track_t *t = find_ns(&out, "live", NULL);
    CHECK(t && t->has_bitrate && t->bitrate == 5 && t->is_live &&
          t->depends_count == 0 && supp(t) && bv_eq(supp(t)->key_id, "k") &&
          supp(t)->keyframe_spacing == 30 && t == &out.tracks[0]);
    CHECK(out.track_count == 3);
    moq_msf_catalog_cleanup(&g_alloc, &out);

    /* Ops apply in order: update a track an earlier op added. */
    CHECK_RC(apply_json(&base, "{\"generatedAt\":9,\"deltaUpdate\":["
                        "{\"op\":\"add\",\"tracks\":[{\"name\":\"n\","
                        "\"packaging\":\"loc\",\"isLive\":true}]},"
                        "{\"op\":\"update\",\"tracks\":[{\"parentName\":\"n\","
                        "\"label\":\"later\"}]}]}", &out), MOQ_OK);
    t = find_ns(&out, "n", NULL);
    CHECK(t && t->has_label && bv_eq(t->label, "later"));
    moq_msf_catalog_cleanup(&g_alloc, &out);

    /* An update op needs parentName; a name it carries is a non-empty String. */
    moq_msf_catalog_t d;
    CHECK_RC(parse_full(UPD("{\"name\":\"live\"}"), &d), MOQ_ERR_PROTO);
    CHECK_RC(parse_full(UPD("{\"parentName\":\"live\",\"name\":\"\"}"), &d),
             MOQ_ERR_PROTO);
    CHECK_RC(parse_full(UPD("{\"parentName\":\"live\",\"name\":3}"), &d),
             MOQ_ERR_PROTO);
    /* parentName belongs only to clone and update ops. */
    CHECK_RC(parse_full("{\"generatedAt\":9,\"deltaUpdate\":[{\"op\":\"add\","
                        "\"tracks\":[{\"name\":\"z\",\"parentName\":\"live\"}]}]}",
                        &d), MOQ_ERR_PROTO);
    moq_msf_catalog_cleanup(&g_alloc, &base);
    check_balance("update_rules");
}

static void test_delta_r02_roots(void)
{
    moq_msf_catalog_t base, out;
    CHECK_RC(parse_full(k_r02_simple, &base), MOQ_OK);
    static const char *const refused[] = {
        "{\"deltaUpdate\":[{\"op\":\"remove\",\"tracks\":[{\"name\":\"v\"}]}]}",
        "{\"generatedAt\":\"9\",\"deltaUpdate\":[{\"op\":\"remove\",\"tracks\":"
        "[{\"name\":\"v\"}]}]}",
        "{\"generatedAt\":-9,\"deltaUpdate\":[{\"op\":\"remove\",\"tracks\":"
        "[{\"name\":\"v\"}]}]}",
        "{\"generatedAt\":9,\"initDataList\":[],\"deltaUpdate\":[{\"op\":"
        "\"remove\",\"tracks\":[{\"name\":\"v\"}]}]}",
        "{\"generatedAt\":9,\"publishTracks\":[],\"deltaUpdate\":[{\"op\":"
        "\"remove\",\"tracks\":[{\"name\":\"v\"}]}]}",
        "{\"generatedAt\":9,\"contentProtections\":[],\"deltaUpdate\":[{\"op\":"
        "\"remove\",\"tracks\":[{\"name\":\"v\"}]}]}",
        "{\"generatedAt\":9,\"initDataList\":[{\"id\":\"i\",\"type\":\"inline\","
        "\"data\":\"\"}],\"deltaUpdate\":[{\"op\":\"remove\",\"tracks\":"
        "[{\"name\":\"v\"}]}]}",
    };
    for (size_t i = 0; i < sizeof(refused) / sizeof(refused[0]); i++) {
        moq_result_t rc = apply_json(&base, refused[i], &out);
        if (rc != MOQ_ERR_PROTO) {
            fprintf(stderr, "FAIL: delta_r02_roots row %zu: %d\n", i, rc);
            failures++;
        }
        if (rc == MOQ_OK) moq_msf_catalog_cleanup(&g_alloc, &out);
    }
    CHECK_RC(apply_json(&base, "{\"generatedAt\":9,\"deltaUpdate\":[{\"op\":"
                        "\"remove\",\"tracks\":[{\"name\":\"v\"}]}]}", &out),
             MOQ_OK);
    moq_msf_catalog_cleanup(&g_alloc, &out);
    /* An MSF-02 add track carries packaging and isLive (5.2.4.1). */
    CHECK_APPLY(&base, "{\"generatedAt\":9,\"deltaUpdate\":[{\"op\":"
                "\"add\",\"tracks\":[{\"name\":\"a\",\"packaging\":"
                "\"loc\"}]}]}", MOQ_ERR_PROTO);
    CHECK_APPLY(&base, "{\"generatedAt\":9,\"deltaUpdate\":[{\"op\":"
                "\"add\",\"tracks\":[{\"name\":\"a\",\"isLive\":true}]}]}",
                MOQ_ERR_PROTO);
    moq_msf_catalog_cleanup(&g_alloc, &base);
    /* MSF-01 keeps its leniency for a missing isLive. */
    CHECK_RC(parse_full(k_r01_publish, &base), MOQ_OK);
    CHECK_RC(apply_json(&base, "{\"deltaUpdate\":[{\"op\":\"add\",\"tracks\":"
                        "[{\"name\":\"a\",\"packaging\":\"loc\"}]}]}", &out),
             MOQ_OK);
    moq_msf_catalog_cleanup(&g_alloc, &out);
    moq_msf_catalog_cleanup(&g_alloc, &base);
    check_balance("delta_r02_roots");
}

static void test_authored_delta_paths(void)
{
    moq_msf_catalog_t base;
    CHECK_RC(parse_fixture("delta_base.json", &base), MOQ_OK);
    moq_msf_track_t ut;
    memset(&ut, 0, sizeof(ut));
    ut.struct_size = sizeof(ut);
    ut.has_parent_name = true; ut.parent_name = lit("video-1080");
    ut.has_bitrate = true; ut.bitrate = 123;
    moq_msf_track_t at;
    memset(&at, 0, sizeof(at));
    at.struct_size = sizeof(at);
    at.name = lit("extra"); at.packaging = lit("loc"); at.is_live = true;
    at.has_packaging = true; at.has_is_live = true;
    moq_msf_delta_op_t ops[2] = { { MOQ_MSF_DELTA_OP_UPDATE, &ut, 1 },
                                  { MOQ_MSF_DELTA_OP_ADD, &at, 1 } };
    moq_msf_catalog_t d;
    memset(&d, 0, sizeof(d));
    d.struct_size = sizeof(d);
    d.delta_update = ops; d.delta_update_count = 2;
    d.revision = MOQ_MSF_REVISION_02;

    /* Encode judges by the delta's own revision. */
    moq_rcbuf_t *enc = NULL;
    CHECK_RC(moq_msf_catalog_encode(&g_alloc, &d, &enc), MOQ_ERR_INVAL); /* no generatedAt */
    d.has_generated_at = true; d.generated_at = 77;
    d.revision = MOQ_MSF_REVISION_01;
    CHECK_RC(moq_msf_catalog_encode(&g_alloc, &d, &enc), MOQ_ERR_INVAL); /* update in 01 */
    d.revision = MOQ_MSF_REVISION_02;
    CHECK_RC(moq_msf_catalog_encode(&g_alloc, &d, &enc), MOQ_OK);

    /* Apply judges by the base's, through the public fields. */
    moq_msf_catalog_t out;
    CHECK_RC(moq_msf_catalog_apply_delta_sized(&g_alloc, &base, &d, NULL, &out,
                                               sizeof(out)), MOQ_OK);
    const moq_msf_track_t *v = find_ns(&out, "video-1080", NULL);
    CHECK(v && v->bitrate == 123 && find_ns(&out, "extra", NULL));
    moq_msf_catalog_cleanup(&g_alloc, &out);
    d.has_generated_at = false;
    CHECK_RC(moq_msf_catalog_apply_delta_sized(&g_alloc, &base, &d, NULL, &out,
                                               sizeof(out)), MOQ_ERR_PROTO);
    d.has_generated_at = true;

    /* The authored bytes re-parse to the same delta, which re-applies. */
    moq_msf_catalog_t back, out2;
    moq_result_t brc = enc ? moq_msf_catalog_parse(&g_alloc,
        (moq_bytes_t){ moq_rcbuf_data(enc), moq_rcbuf_len(enc) }, &back)
                           : MOQ_ERR_INVAL;
    CHECK(brc == MOQ_OK);
    if (brc == MOQ_OK) {
        CHECK(back.revision == MOQ_MSF_REVISION_02 && back.has_generated_at &&
              back.generated_at == 77 && back.delta_update_count == 2);
        CHECK_RC(moq_msf_catalog_apply_delta_sized(&g_alloc, &base, &back, NULL,
                                                   &out2, sizeof(out2)), MOQ_OK);
        CHECK(find_ns(&out2, "video-1080", NULL) &&
              find_ns(&out2, "video-1080", NULL)->bitrate == 123);
        moq_msf_catalog_cleanup(&g_alloc, &out2);
        moq_msf_catalog_cleanup(&g_alloc, &back);
    }
    moq_rcbuf_decref(enc);

    /* An update track names no other track than its target. */
    ut.name = lit("renamed");
    CHECK_RC(moq_msf_catalog_apply_delta_sized(&g_alloc, &base, &d, NULL, &out,
                                               sizeof(out)), MOQ_ERR_PROTO);
    ut.name = (moq_bytes_t){ NULL, 0 };
    /* An unknown op kind is caller error. */
    ops[0].op = (moq_msf_delta_op_kind_t)9;
    CHECK_RC(moq_msf_catalog_apply_delta_sized(&g_alloc, &base, &d, NULL, &out,
                                               sizeof(out)), MOQ_ERR_INVAL);
    CHECK_RC(moq_msf_catalog_encode(&g_alloc, &d, &enc), MOQ_ERR_INVAL);
    moq_msf_catalog_cleanup(&g_alloc, &base);
    check_balance("authored_delta_paths");
}

/* MSF-02 5.3: a delta carries deltaUpdate and generatedAt and no other root
 * field. Authoring refuses one rather than drop it, judging a parsed delta
 * by what its JSON held; MSF-01 authoring keeps writing such a delta without
 * them, as it always has. */
static void test_delta_authoring_roots(void)
{
    moq_msf_track_t ut;
    memset(&ut, 0, sizeof(ut));
    ut.struct_size = sizeof(ut);
    ut.has_parent_name = true; ut.parent_name = lit("video");
    ut.has_bitrate = true; ut.bitrate = 9;
    moq_msf_track_t rt;
    memset(&rt, 0, sizeof(rt));
    rt.struct_size = sizeof(rt);
    rt.name = lit("audio");
    moq_msf_delta_op_t ops[2] = { { MOQ_MSF_DELTA_OP_UPDATE, &ut, 1 },
                                  { MOQ_MSF_DELTA_OP_REMOVE, &rt, 1 } };
    moq_msf_catalog_t d;
    memset(&d, 0, sizeof(d));
    d.struct_size = sizeof(d);
    d.delta_update = ops; d.delta_update_count = 2;
    d.has_generated_at = true; d.generated_at = 3;
    d.revision = MOQ_MSF_REVISION_02;
    moq_rcbuf_t *enc = NULL;
    CHECK_RC(moq_msf_catalog_encode(&g_alloc, &d, &enc), MOQ_OK);
    moq_rcbuf_decref(enc);
    enc = NULL;

    moq_msf_init_data_entry_t ide = { lit("i"), lit("inline"), lit("AAAA") };
    moq_cmsf_content_protection_t cp;
    memset(&cp, 0, sizeof(cp));
    d.init_data_list = &ide; d.init_data_count = 1;
    CHECK_RC(moq_msf_catalog_encode(&g_alloc, &d, &enc), MOQ_ERR_INVAL);
    d.init_data_list = NULL; d.init_data_count = 0;
    d.content_protections = &cp; d.content_protection_count = 1;
    CHECK_RC(moq_msf_catalog_encode(&g_alloc, &d, &enc), MOQ_ERR_INVAL);
    d.content_protections = NULL; d.content_protection_count = 0;

    /* MSF-01: the same root data is still left out, not refused. */
    ops[0].op = MOQ_MSF_DELTA_OP_CLONE;
    ut.name = lit("video2");
    d.revision = MOQ_MSF_REVISION_01;
    d.init_data_list = &ide; d.init_data_count = 1;
    d.content_protections = &cp; d.content_protection_count = 1;
    CHECK_RC(moq_msf_catalog_encode(&g_alloc, &d, &enc), MOQ_OK);
    if (enc) {
        CHECK(!buf_contains(enc, "initDataList") &&
              !buf_contains(enc, "contentProtections"));
        moq_rcbuf_decref(enc);
        enc = NULL;
    }

    /* Parsed MSF-02 deltas: a forbidden root key present, even empty, does
     * not author; the same delta without it authors, and its bytes apply
     * where the other's do not. */
    static const char *const roots[] = {
        "\"initDataList\":[],", "\"contentProtections\":[],",
        "\"publishTracks\":[],",
        "\"initDataList\":[{\"id\":\"i\",\"type\":\"inline\",\"data\":\"\"}],",
    };
    moq_msf_catalog_t base;
    CHECK_RC(parse_full("{\"version\":\"draft-02\",\"generatedAt\":1,\"tracks\":["
                        TRK("video", "") "," TRK("audio", "") "]}", &base), MOQ_OK);
    char json[512];
    for (size_t i = 0; i <= sizeof(roots) / sizeof(roots[0]); i++) {
        bool clean = i == sizeof(roots) / sizeof(roots[0]);
        snprintf(json, sizeof(json),
                 "{\"generatedAt\":5,%s\"deltaUpdate\":[{\"op\":\"update\","
                 "\"tracks\":[{\"parentName\":\"video\",\"bitrate\":9}]}]}",
                 clean ? "" : roots[i]);
        moq_msf_catalog_t pd;
        CHECK_RC(parse_full(json, &pd), MOQ_OK);
        CHECK(pd.revision == MOQ_MSF_REVISION_02);
        moq_result_t erc = moq_msf_catalog_encode(&g_alloc, &pd, &enc);
        if (erc != (clean ? MOQ_OK : MOQ_ERR_INVAL)) {
            fprintf(stderr, "FAIL: delta authoring root row %zu: %d\n", i, erc);
            failures++;
        }
        CHECK_APPLY(&base, json, clean ? MOQ_OK : MOQ_ERR_PROTO);
        if (erc == MOQ_OK && enc) {
            moq_msf_catalog_t back, out;
            CHECK_RC(moq_msf_catalog_parse(&g_alloc,
                         (moq_bytes_t){ moq_rcbuf_data(enc), moq_rcbuf_len(enc) },
                         &back), MOQ_OK);
            CHECK_RC(moq_msf_catalog_apply_delta_sized(&g_alloc, &base, &back,
                                                       NULL, &out, sizeof(out)),
                     MOQ_OK);
            CHECK(out.track_count == 2 && out.tracks[0].bitrate == 9);
            moq_msf_catalog_cleanup(&g_alloc, &out);
            moq_msf_catalog_cleanup(&g_alloc, &back);
        }
        moq_rcbuf_decref(enc);
        enc = NULL;
        moq_msf_catalog_cleanup(&g_alloc, &pd);
    }
    moq_msf_catalog_cleanup(&g_alloc, &base);
    check_balance("delta_authoring_roots");
}

static void test_validate_after_all_ops(void)
{
    static const char base_json[] =
        "{\"version\":\"draft-02\",\"generatedAt\":1,\"tracks\":["
        TRK("v", ",\"renderGroup\":1") ","
        "{\"name\":\"t\",\"packaging\":\"mediatimeline\",\"isLive\":true,"
        "\"depends\":[\"v\",1]}]}";
    moq_msf_catalog_t base, out;
    CHECK_RC(parse_full(base_json, &base), MOQ_OK);
    /* Removing what a track depends on leaves an invalid catalog. */
    CHECK_APPLY(&base, "{\"generatedAt\":2,\"deltaUpdate\":[{\"op\":"
                "\"remove\",\"tracks\":[{\"name\":\"v\"}]}]}", MOQ_ERR_PROTO);
    /* ... but the check runs after every op: removed and re-added is fine. */
    CHECK_RC(apply_json(&base, "{\"generatedAt\":2,\"deltaUpdate\":["
                        "{\"op\":\"remove\",\"tracks\":[{\"name\":\"v\"}]},"
                        "{\"op\":\"add\",\"tracks\":[" TRK("v", ",\"renderGroup\":1")
                        "]}]}", &out), MOQ_OK);
    moq_msf_catalog_cleanup(&g_alloc, &out);
    /* A second "v" in another namespace makes "v" ambiguous. */
    CHECK_APPLY(&base, "{\"generatedAt\":2,\"deltaUpdate\":[{\"op\":"
                "\"add\",\"tracks\":[" TRK("v", ",\"namespace\":\"other\"")
                "]}]}", MOQ_ERR_PROTO);
    /* An added track whose depends resolve nowhere. */
    CHECK_APPLY(&base, "{\"generatedAt\":2,\"deltaUpdate\":[{\"op\":"
                "\"add\",\"tracks\":[" TRK("w", ",\"depends\":[7]") "]}]}",
                MOQ_ERR_PROTO);
    moq_msf_catalog_cleanup(&g_alloc, &base);
    check_balance("validate_after_all_ops");
}

static void test_illogical_ops_r02(void)
{
    moq_msf_catalog_t base, out;
    CHECK_RC(parse_fixture("delta_base.json", &base), MOQ_OK);
    static const char *const refused[] = {
        /* add an existing name */
        "{\"generatedAt\":2,\"deltaUpdate\":[{\"op\":\"add\",\"tracks\":["
        TRK("audio", "") "]}]}",
        /* remove an unknown track */
        "{\"generatedAt\":2,\"deltaUpdate\":[{\"op\":\"remove\",\"tracks\":["
        "{\"name\":\"nope\"}]}]}",
        /* remove twice */
        "{\"generatedAt\":2,\"deltaUpdate\":[{\"op\":\"remove\",\"tracks\":["
        "{\"name\":\"audio\"},{\"name\":\"audio\"}]}]}",
        /* update an unknown track */
        "{\"generatedAt\":2,\"deltaUpdate\":[{\"op\":\"update\",\"tracks\":["
        "{\"parentName\":\"nope\",\"bitrate\":1}]}]}",
        /* clone to an existing name */
        "{\"generatedAt\":2,\"deltaUpdate\":[{\"op\":\"clone\",\"tracks\":["
        "{\"parentName\":\"audio\",\"name\":\"video\"}]}]}",
        /* clone from an unknown parent */
        "{\"generatedAt\":2,\"deltaUpdate\":[{\"op\":\"clone\",\"tracks\":["
        "{\"parentName\":\"nope\",\"name\":\"x\"}]}]}",
    };
    for (size_t i = 0; i < sizeof(refused) / sizeof(refused[0]); i++) {
        moq_result_t rc = apply_json(&base, refused[i], &out);
        if (rc != MOQ_ERR_PROTO) {
            fprintf(stderr, "FAIL: illogical_ops_r02 row %zu: %d\n", i, rc);
            failures++;
        }
        if (rc == MOQ_OK) moq_msf_catalog_cleanup(&g_alloc, &out);
        CHECK(out.struct_size == sizeof(out) && out.track_count == 0);
    }
    moq_msf_catalog_cleanup(&g_alloc, &base);
    check_balance("illogical_ops_r02");
}


/* -- Naming and Full Track Name identity ------------------------------------ */

static void test_namespace_split(void)
{
    moq_bytes_t f[40];
    size_t n = 99;
    CHECK_RC(moq_msf_namespace_split(lit("example.com/live/channel1"), f, 40, &n),
             MOQ_OK);
    CHECK(n == 3 && bv_eq(f[0], "example.com") && bv_eq(f[1], "live") &&
          bv_eq(f[2], "channel1"));
    CHECK_RC(moq_msf_namespace_split(lit("one"), f, 40, &n), MOQ_OK);
    CHECK(n == 1 && bv_eq(f[0], "one"));
    /* Probe, then a buffer that is too small: the count is always written. */
    CHECK_RC(moq_msf_namespace_split(lit("a/b/c"), NULL, 0, &n), MOQ_ERR_BUFFER);
    CHECK(n == 3);
    memset(f, 0, sizeof(f));
    CHECK_RC(moq_msf_namespace_split(lit("a/b/c"), f, 2, &n), MOQ_ERR_BUFFER);
    CHECK(n == 3 && bv_eq(f[0], "a") && bv_eq(f[1], "b") && f[2].data == NULL);
    /* 32 fields are allowed, 33 are not (5.2.2). */
    char ns[128];
    size_t k = 0;
    for (int i = 0; i < 33; i++) { ns[k++] = 'x'; ns[k++] = '/'; }
    ns[k - 1] = '\0';                                /* 33 fields */
    CHECK_RC(moq_msf_namespace_split(lit(ns), f, 40, &n), MOQ_ERR_PROTO);
    CHECK(n == 0);
    ns[k - 3] = '\0';                                /* 32 fields */
    CHECK_RC(moq_msf_namespace_split(lit(ns), f, 40, &n), MOQ_OK);
    CHECK(n == 32 && MOQ_MSF_NAMESPACE_MAX_FIELDS == 32u);
    /* Empty, a leading, trailing or doubled '/': an empty field. */
    static const char *const bad[] = { "", "/a", "a/", "a//b", "/", "//" };
    for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); i++) {
        n = 99;
        CHECK_RC(moq_msf_namespace_split(lit(bad[i]), f, 40, &n), MOQ_ERR_PROTO);
        CHECK(n == 0);
    }
    /* Arguments. */
    CHECK_RC(moq_msf_namespace_split(lit("a"), f, 1, NULL), MOQ_ERR_INVAL);
    CHECK_RC(moq_msf_namespace_split(lit("a"), NULL, 1, &n), MOQ_ERR_INVAL);
    CHECK_RC(moq_msf_namespace_split((moq_bytes_t){ NULL, 3 }, f, 1, &n),
             MOQ_ERR_INVAL);
}

/* The bytes of track 0's name in a one-track MSF-02 catalog whose name is
 * the JSON text `name_json`, or PROTO. */
static moq_result_t name_bytes(const char *name_json, uint8_t *out, size_t cap,
                               size_t *out_len)
{
    char json[256];
    snprintf(json, sizeof(json),
             "{\"version\":\"draft-02\",\"tracks\":[{\"name\":%s,"
             "\"packaging\":\"loc\",\"isLive\":true}]}", name_json);
    moq_msf_catalog_t cat;
    moq_result_t rc = parse_full(json, &cat);
    if (rc != MOQ_OK) return rc;
    *out_len = cat.tracks[0].name.len;
    if (*out_len <= cap) memcpy(out, cat.tracks[0].name.data, *out_len);
    moq_msf_catalog_cleanup(&g_alloc, &cat);
    return MOQ_OK;
}

static void test_string_to_bytes_vectors(void)
{
    /* MSF-02 11.2.5, Table 13. */
    static const struct {
        const char *json;
        const char *bytes;      /* NULL: the catalog is invalid */
        size_t len;
    } rows[] = {
        { "\"video\"",        "\x76\x69\x64\x65\x6f",     5 },
        { "\"caf\xc3\xa9\"",  "\x63\x61\x66\xc3\xa9",     5 },
        { "\"caf\\u00e9\"",   "\x63\x61\x66\xc3\xa9",     5 },
        { "\"cafe\\u0301\"",  "\x63\x61\x66\x65\xcc\x81", 6 },
        { "\"Video\"",        "\x56\x69\x64\x65\x6f",     5 },
        { "\"\"",             NULL, 0 },
        { "\"\\uDEAD\"",      NULL, 0 },
    };
    for (size_t i = 0; i < sizeof(rows) / sizeof(rows[0]); i++) {
        uint8_t b[16];
        size_t n = 0;
        moq_result_t rc = name_bytes(rows[i].json, b, sizeof(b), &n);
        if (!rows[i].bytes) {
            CHECK(rc == MOQ_ERR_PROTO);
        } else {
            CHECK(rc == MOQ_OK && n == rows[i].len &&
                  memcmp(b, rows[i].bytes, n) == 0);
        }
    }
    /* "video" and "Video" are different tracks; the two spellings of "café"
     * are the same one. */
    CHECK(check_of(R02(TRK("video", "") "," TRK("Video", "")), MOQ_OK) ==
          MOQ_MSF_CATALOG_OK);
    CHECK(check_of("{\"version\":\"draft-02\",\"tracks\":["
                   TRK("caf\xc3\xa9", "") "," TRK("caf\\u00e9", "") "]}",
                   MOQ_ERR_PROTO) == MOQ_MSF_CATALOG_ERR_DUPLICATE_TRACK);
    /* An empty namespace field is as invalid as an empty name. */
    moq_msf_catalog_t cat;
    CHECK_RC(parse_full(R02(TRK("a", ",\"namespace\":\"x//y\"")), &cat),
             MOQ_ERR_PROTO);
    CHECK_RC(parse_full(R02(TRK("a", ",\"namespace\":\"\"")), &cat),
             MOQ_ERR_PROTO);
    check_balance("string_to_bytes_vectors");
}

static void test_track_matches(void)
{
    moq_msf_catalog_t cat;
    CHECK_RC(parse_full(R02(TRK("in", "") "," TRK("ex", ",\"namespace\":\"a/b\"")),
                        &cat), MOQ_OK);
    const moq_msf_track_t *in = &cat.tracks[0], *ex = &cat.tracks[1];
    moq_bytes_t ab[2] = { lit("a"), lit("b") };
    moq_bytes_t slash[1] = { lit("a/b") };
    moq_namespace_t ns_ab = { ab, 2 }, ns_slash = { slash, 1 }, none = { NULL, 0 };
    /* Explicit: the fields of the string, byte for byte. */
    CHECK(moq_msf_track_matches(ex, none, ns_ab, lit("ex")));
    CHECK(moq_msf_track_matches(ex, ns_slash, ns_ab, lit("ex")));
    CHECK(!moq_msf_track_matches(ex, none, ns_slash, lit("ex")));
    CHECK(!moq_msf_track_matches(ex, none, ns_ab, lit("Ex")));
    /* Inherited: the catalog namespace, when known. A catalog-namespace
     * field holding '/' is one field, not two. */
    CHECK(moq_msf_track_matches(in, ns_ab, ns_ab, lit("in")));
    CHECK(!moq_msf_track_matches(in, none, ns_ab, lit("in")));
    CHECK(!moq_msf_track_matches(in, ns_slash, ns_ab, lit("in")));
    CHECK(moq_msf_track_matches(in, ns_slash, ns_slash, lit("in")));
    /* A name that is not UTF-8 never matches a catalog name (11.2.3); NULL
     * and malformed arguments match nothing. */
    CHECK(!moq_msf_track_matches(in, ns_ab, ns_ab,
                                 (moq_bytes_t){ (const uint8_t *)"\xff", 1 }));
    CHECK(!moq_msf_track_matches(NULL, ns_ab, ns_ab, lit("in")));
    CHECK(!moq_msf_track_matches(in, ns_ab, (moq_namespace_t){ NULL, 2 }, lit("in")));
    moq_msf_catalog_cleanup(&g_alloc, &cat);

    /* MOQT names and fields that are not UTF-8 never match a catalog name
     * (11.2.3), even when the bytes are equal: a lenient MSF-01 parse or a
     * caller can hold such bytes. */
    moq_msf_track_t bad;
    memset(&bad, 0, sizeof(bad));
    bad.struct_size = sizeof(bad);
    bad.name = (moq_bytes_t){ (const uint8_t *)"\xff", 1 };
    CHECK(!moq_msf_track_matches(&bad, ns_ab, ns_ab,
                                 (moq_bytes_t){ (const uint8_t *)"\xff", 1 }));
    bad.name = lit("ok");
    bad.has_namespace = true;
    bad.namespace_ = (moq_bytes_t){ (const uint8_t *)"\xc3/b", 3 };
    moq_bytes_t bad_fields[2] = { { (const uint8_t *)"\xc3", 1 }, lit("b") };
    moq_namespace_t ns_bad = { bad_fields, 2 };
    CHECK(!moq_msf_track_matches(&bad, none, ns_bad, lit("ok")));
    bad.has_namespace = false;                   /* inherited */
    CHECK(!moq_msf_track_matches(&bad, ns_bad, ns_bad, lit("ok")));
    /* ... while valid multi-byte UTF-8 matches byte for byte, explicit or
     * inherited, and a valid inherited field may hold '/'. */
    moq_bytes_t euro[2] = { { (const uint8_t *)"\xe2\x82\xac", 3 }, lit("b") };
    moq_namespace_t ns_euro = { euro, 2 };
    bad.has_namespace = true;
    bad.namespace_ = (moq_bytes_t){ (const uint8_t *)"\xe2\x82\xac/b", 5 };
    CHECK(moq_msf_track_matches(&bad, none, ns_euro, lit("ok")));
    bad.has_namespace = false;
    CHECK(moq_msf_track_matches(&bad, ns_euro, ns_euro, lit("ok")));
    CHECK(moq_msf_track_matches(&bad, ns_slash, ns_slash, lit("ok")));
    CHECK(!moq_msf_track_matches(&bad, ns_slash, ns_ab, lit("ok")));

    /* A span that claims bytes with no buffer matches nothing and is never
     * read: a MOQT field, a MOQT name, a catalog-namespace field. */
    moq_bytes_t null_one[1] = { { NULL, 1 } };
    moq_namespace_t ns_null = { null_one, 1 };
    bad.has_namespace = true;
    bad.namespace_ = lit("a");
    CHECK(!moq_msf_track_matches(&bad, none, ns_null, lit("ok")));
    CHECK(!moq_msf_track_matches(&bad, none, (moq_namespace_t){ ab, 1 },
                                 (moq_bytes_t){ NULL, 2 }));
    bad.has_namespace = false;
    CHECK(!moq_msf_track_matches(&bad, ns_null, ns_null, lit("ok")));
    CHECK(!moq_msf_track_matches(&bad, ns_null, (moq_namespace_t){ ab, 1 },
                                 lit("ok")));
    check_balance("track_matches");
}

static void test_full_name_identity(void)
{
    static const char json[] =
        R02(TRK("a", "") "," TRK("a", ",\"namespace\":\"x/y\""));
    moq_msf_catalog_t cat;
    CHECK_RC(parse_full(json, &cat), MOQ_OK);
    moq_bytes_t xy[2] = { lit("x"), lit("y") };
    moq_bytes_t one[1] = { lit("x/y") };
    moq_msf_validate_cfg_t cfg;
    moq_msf_validate_cfg_init(&cfg);
    moq_msf_catalog_error_t why = 99;
    /* Unknown catalog namespace: an inherited name is its own class. */
    CHECK_RC(moq_msf_catalog_check(&g_alloc, &cat, &cfg, &why), MOQ_OK);
    /* Inherited x/y equals explicit "x/y": one Full Track Name twice. */
    cfg.catalog_namespace = (moq_namespace_t){ xy, 2 };
    CHECK_RC(moq_msf_catalog_check(&g_alloc, &cat, &cfg, &why), MOQ_ERR_PROTO);
    CHECK(why == MOQ_MSF_CATALOG_ERR_DUPLICATE_TRACK);
    /* A one-field catalog namespace "x/y" is not the two fields x, y. */
    cfg.catalog_namespace = (moq_namespace_t){ one, 1 };
    CHECK_RC(moq_msf_catalog_check(&g_alloc, &cat, &cfg, &why), MOQ_OK);
    moq_msf_catalog_cleanup(&g_alloc, &cat);

    /* MSF-01 compares the namespace as written and ignores the catalog's. */
    CHECK_RC(parse_full(R01(TRK("a", "") "," TRK("a", ",\"namespace\":\"x/y\"")),
                        &cat), MOQ_OK);
    cfg.catalog_namespace = (moq_namespace_t){ xy, 2 };
    CHECK_RC(moq_msf_catalog_check(&g_alloc, &cat, &cfg, &why), MOQ_OK);
    moq_msf_catalog_cleanup(&g_alloc, &cat);

    /* apply resolves names the same way: under catalog namespace x/y an
     * explicit "x/y" names the inherited track. */
    CHECK_RC(parse_full(R02(TRK("a", "") "," TRK("b", "")), &cat), MOQ_OK);
    moq_msf_catalog_t d, out;
    CHECK_RC(parse_full("{\"generatedAt\":3,\"deltaUpdate\":[{\"op\":\"remove\","
                        "\"tracks\":[{\"name\":\"a\",\"namespace\":\"x/y\"}]}]}",
                        &d), MOQ_OK);
    CHECK_RC(moq_msf_catalog_apply_delta_sized(&g_alloc, &cat, &d, NULL, &out,
                                               sizeof(out)), MOQ_ERR_PROTO);
    cfg.catalog_namespace = (moq_namespace_t){ xy, 2 };
    CHECK_RC(moq_msf_catalog_apply_delta_sized(&g_alloc, &cat, &d, &cfg, &out,
                                               sizeof(out)), MOQ_OK);
    CHECK(out.track_count == 1 && bv_eq(out.tracks[0].name, "b"));
    moq_msf_catalog_cleanup(&g_alloc, &out);
    moq_msf_catalog_cleanup(&g_alloc, &d);
    /* ... and an add of explicit "x/y"/"b" collides with the inherited b. */
    CHECK_RC(parse_full("{\"generatedAt\":3,\"deltaUpdate\":[{\"op\":\"add\","
                        "\"tracks\":[" TRK("b", ",\"namespace\":\"x/y\"") "]}]}",
                        &d), MOQ_OK);
    CHECK_RC(moq_msf_catalog_apply_delta_sized(&g_alloc, &cat, &d, &cfg, &out,
                                               sizeof(out)), MOQ_ERR_PROTO);
    CHECK_RC(moq_msf_catalog_apply_delta_sized(&g_alloc, &cat, &d, NULL, &out,
                                               sizeof(out)), MOQ_OK);
    moq_msf_catalog_cleanup(&g_alloc, &out);
    moq_msf_catalog_cleanup(&g_alloc, &d);
    moq_msf_catalog_cleanup(&g_alloc, &cat);
    check_balance("full_name_identity");
}

static void test_naming_parse_r02(void)
{
    moq_msf_catalog_t cat, out;
    /* Independent MSF-02 tracks: a namespace is a String of valid fields,
     * names are non-empty, naming strings are UTF-8 (5.2.2, 5.2.3, 11.2). */
    static const char *const bad[] = {
        R02(TRK("a", ",\"namespace\":3")),
        R02(TRK("a", ",\"namespace\":\"/x\"")),
        R02("{\"name\":\"\",\"packaging\":\"loc\",\"isLive\":true}"),
        R02(TRK("\xff", "")),
        R02(TRK("a", ",\"namespace\":\"x/\xc0\xaf\"")),
        R02(TRK("a", ",\"depends\":[\"\xed\xa0\x80\"]")),
        "{\"version\":\"draft-02\",\"tracks\":[],\"publishTracks\":["
        TRK("p", ",\"namespace\":\"a//b\"") "]}",
    };
    for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); i++) {
        moq_result_t rc = parse_full(bad[i], &cat);
        if (rc != MOQ_ERR_PROTO) {
            fprintf(stderr, "FAIL: naming_parse_r02 row %zu: %d\n", i, rc);
            failures++;
            if (rc == MOQ_OK) moq_msf_catalog_cleanup(&g_alloc, &cat);
        }
    }
    /* MSF-01 keeps its leniency: a non-String namespace is absent, bytes are
     * not judged. */
    CHECK_RC(parse_full(R01(TRK("a", ",\"namespace\":3")), &cat), MOQ_OK);
    CHECK(!cat.tracks[0].has_namespace && cat.tracks[0].extension_count == 0);
    moq_msf_catalog_cleanup(&g_alloc, &cat);
    CHECK_RC(parse_full(R01(TRK("\xff", ",\"namespace\":\"a//b\"")), &cat), MOQ_OK);
    moq_msf_catalog_cleanup(&g_alloc, &cat);

    /* Delta op tracks: a non-String naming key is recorded as malformed and
     * judged against the base's revision. */
    moq_msf_catalog_t r02, r01, d;
    CHECK_RC(parse_full("{\"version\":\"draft-02\",\"generatedAt\":1,\"tracks\":["
                        TRK("a", "") "]}", &r02), MOQ_OK);
    CHECK_RC(parse_full(R01(TRK("a", "")), &r01), MOQ_OK);
    CHECK_RC(parse_full("{\"generatedAt\":2,\"deltaUpdate\":[{\"op\":\"remove\","
                        "\"tracks\":[{\"name\":\"a\",\"namespace\":7}]}]}", &d),
             MOQ_OK);
    const moq_msf_track_supplement_t *s = supp(&d.delta_update[0].tracks[0]);
    CHECK(s && s->malformed == MOQ_MSF_SUPPLEMENT_F_NAMESPACE);
    CHECK(!d.delta_update[0].tracks[0].has_namespace);
    CHECK_RC(moq_msf_catalog_apply_delta_sized(&g_alloc, &r02, &d, NULL, &out,
                                               sizeof(out)), MOQ_ERR_PROTO);
    CHECK_RC(moq_msf_catalog_apply_delta_sized(&g_alloc, &r01, &d, NULL, &out,
                                               sizeof(out)), MOQ_OK);
    CHECK(out.track_count == 0);
    moq_msf_catalog_cleanup(&g_alloc, &out);
    moq_rcbuf_t *enc = NULL;
    CHECK_RC(moq_msf_catalog_encode(&g_alloc, &d, &enc), MOQ_ERR_INVAL);
    moq_msf_catalog_cleanup(&g_alloc, &d);
    CHECK_RC(parse_full("{\"generatedAt\":2,\"deltaUpdate\":[{\"op\":\"clone\","
                        "\"tracks\":[{\"parentName\":\"a\",\"parentNamespace\":[],"
                        "\"name\":\"c\"}]}]}", &d), MOQ_OK);
    s = supp(&d.delta_update[0].tracks[0]);
    CHECK(s && s->malformed == MOQ_MSF_SUPPLEMENT_F_PARENT_NAMESPACE);
    CHECK_RC(moq_msf_catalog_apply_delta_sized(&g_alloc, &r02, &d, NULL, &out,
                                               sizeof(out)), MOQ_ERR_PROTO);
    CHECK_RC(moq_msf_catalog_apply_delta_sized(&g_alloc, &r01, &d, NULL, &out,
                                               sizeof(out)), MOQ_OK);
    moq_msf_catalog_cleanup(&g_alloc, &out);
    moq_msf_catalog_cleanup(&g_alloc, &d);
    /* String naming keys of op tracks are judged at apply on an MSF-02 base. */
    static const char *const op_bad[] = {
        "{\"generatedAt\":2,\"deltaUpdate\":[{\"op\":\"add\",\"tracks\":["
        TRK("n", ",\"namespace\":\"a//b\"") "]}]}",
        "{\"generatedAt\":2,\"deltaUpdate\":[{\"op\":\"add\",\"tracks\":["
        TRK("\xff", "") "]}]}",
        "{\"generatedAt\":2,\"deltaUpdate\":[{\"op\":\"clone\",\"tracks\":["
        "{\"parentName\":\"a\",\"parentNamespace\":\"\",\"name\":\"c\"}]}]}",
        "{\"generatedAt\":2,\"deltaUpdate\":[{\"op\":\"update\",\"tracks\":["
        "{\"parentName\":\"a\",\"depends\":[\"\xc3\"]}]}]}",
        "{\"generatedAt\":2,\"deltaUpdate\":[{\"op\":\"remove\",\"tracks\":["
        "{\"name\":\"a\",\"namespace\":\"/\"}]}]}",
    };
    for (size_t i = 0; i < sizeof(op_bad) / sizeof(op_bad[0]); i++) {
        moq_result_t rc = apply_json(&r02, op_bad[i], &out);
        if (rc != MOQ_ERR_PROTO) {
            fprintf(stderr, "FAIL: naming op row %zu: %d\n", i, rc);
            failures++;
            if (rc == MOQ_OK) moq_msf_catalog_cleanup(&g_alloc, &out);
        }
    }
    moq_msf_catalog_cleanup(&g_alloc, &r01);
    moq_msf_catalog_cleanup(&g_alloc, &r02);
    check_balance("naming_parse_r02");
}

static void test_naming_encode_r02(void)
{
    moq_msf_track_t t;
    memset(&t, 0, sizeof(t));
    t.struct_size = sizeof(t);
    t.name = lit("a"); t.packaging = lit("loc"); t.is_live = true;
    moq_msf_catalog_t c;
    memset(&c, 0, sizeof(c));
    c.struct_size = sizeof(c);
    c.version = MOQ_MSF_VERSION;
    c.tracks = &t; c.track_count = 1;
    c.revision = MOQ_MSF_REVISION_02;
    moq_rcbuf_t *enc = NULL;
    CHECK_RC(moq_msf_catalog_encode(&g_alloc, &c, &enc), MOQ_OK);
    moq_rcbuf_decref(enc);
    static const struct { moq_bytes_t name; bool has_ns; moq_bytes_t ns; } bad[] = {
        { { (const uint8_t *)"\xff", 1 }, false, { NULL, 0 } },
        { { (const uint8_t *)"a\0b", 3 }, false, { NULL, 0 } },
        { { (const uint8_t *)"a", 1 }, true, { (const uint8_t *)"x//y", 4 } },
        { { (const uint8_t *)"a", 1 }, true, { (const uint8_t *)"x/\0", 3 } },
        { { (const uint8_t *)"a", 1 }, true, { (const uint8_t *)"", 0 } },
    };
    for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); i++) {
        t.name = bad[i].name;
        t.has_namespace = bad[i].has_ns;
        t.namespace_ = bad[i].ns;
        c.revision = MOQ_MSF_REVISION_02;
        moq_result_t rc = moq_msf_catalog_encode(&g_alloc, &c, &enc);
        if (rc != MOQ_ERR_INVAL) {
            fprintf(stderr, "FAIL: naming_encode_r02 row %zu: %d\n", i, rc);
            failures++;
            moq_rcbuf_decref(enc);
        }
        /* MSF-01 authoring is unchanged: bytes as given. */
        c.revision = MOQ_MSF_REVISION_01;
        if (bad[i].ns.len > 0 || !bad[i].has_ns) {
            CHECK_RC(moq_msf_catalog_encode(&g_alloc, &c, &enc), MOQ_OK);
            moq_rcbuf_decref(enc);
        }
    }
    t.name = lit("a"); t.has_namespace = false;
    /* A depends String follows the same rule. */
    moq_bytes_t dep = { (const uint8_t *)"\xc3", 1 };
    t.depends = &dep; t.depends_count = 1;
    c.revision = MOQ_MSF_REVISION_02;
    CHECK_RC(moq_msf_catalog_encode(&g_alloc, &c, &enc), MOQ_ERR_INVAL);
    t.depends = NULL; t.depends_count = 0;
    /* And the naming keys of an authored MSF-02 delta. */
    moq_msf_track_t ct;
    memset(&ct, 0, sizeof(ct));
    ct.struct_size = sizeof(ct);
    ct.name = lit("c");
    ct.has_parent_name = true; ct.parent_name = lit("a");
    ct.has_parent_namespace = true; ct.parent_namespace = lit("p//q");
    moq_msf_delta_op_t op = { MOQ_MSF_DELTA_OP_CLONE, &ct, 1 };
    moq_msf_catalog_t d;
    memset(&d, 0, sizeof(d));
    d.struct_size = sizeof(d);
    d.delta_update = &op; d.delta_update_count = 1;
    d.has_generated_at = true; d.generated_at = 1;
    d.revision = MOQ_MSF_REVISION_02;
    CHECK_RC(moq_msf_catalog_encode(&g_alloc, &d, &enc), MOQ_ERR_INVAL);
    ct.parent_namespace = lit("p/q");
    CHECK_RC(moq_msf_catalog_encode(&g_alloc, &d, &enc), MOQ_OK);
    moq_rcbuf_decref(enc);
    check_balance("naming_encode_r02");
}

static void test_bom_and_raw_utf8(void)
{
    /* json.h does not take a byte order mark: such a catalog is malformed
     * JSON in both revisions, as 11.2.1 requires of MSF-02 ("MUST NOT begin
     * with a byte order mark"). */
    moq_msf_catalog_t cat;
    CHECK_RC(parse_full("\xef\xbb\xbf{\"version\":\"draft-02\",\"tracks\":[]}",
                        &cat), MOQ_ERR_PROTO);
    CHECK_RC(parse_full("\xef\xbb\xbf{\"version\":\"1\",\"tracks\":[]}", &cat),
             MOQ_ERR_PROTO);
    /* Raw UTF-8 passes through untouched; invalid raw bytes in a naming
     * string fail MSF-02 only. */
    CHECK_RC(parse_full(R02(TRK("\xe2\x82\xac", "")), &cat), MOQ_OK);
    CHECK(cat.tracks[0].name.len == 3 &&
          memcmp(cat.tracks[0].name.data, "\xe2\x82\xac", 3) == 0);
    moq_msf_catalog_cleanup(&g_alloc, &cat);
    CHECK_RC(parse_full(R02(TRK("\xe2\x82", "")), &cat), MOQ_ERR_PROTO);
    CHECK_RC(parse_full(R01(TRK("\xe2\x82", "")), &cat), MOQ_OK);
    moq_msf_catalog_cleanup(&g_alloc, &cat);
    /* A non-naming string is not judged in either revision. */
    CHECK_RC(parse_full(R02(TRK("a", ",\"label\":\"\xff\"")), &cat), MOQ_OK);
    moq_msf_catalog_cleanup(&g_alloc, &cat);
    /* NUL is UTF-8, but authoring never writes it (11.2.1); a parse keeps
     * it. */
    CHECK_RC(parse_full(R02(TRK("a\\u0000b", "")), &cat), MOQ_OK);
    CHECK(cat.tracks[0].name.len == 3);
    moq_rcbuf_t *enc = NULL;
    CHECK_RC(moq_msf_catalog_encode(&g_alloc, &cat, &enc), MOQ_ERR_INVAL);
    moq_msf_catalog_cleanup(&g_alloc, &cat);
    check_balance("bom_and_raw_utf8");
}


/* -- Supplement fields: stream mapping, required fields, switch points ------ */

static moq_msf_stream_mapping_t mapping_of(const char *pkg, const char *extra)
{
    char json[512];
    snprintf(json, sizeof(json),
             "{\"version\":\"draft-02\",\"tracks\":[{\"name\":\"t\","
             "\"packaging\":\"%s\",\"isLive\":true%s}]}", pkg, extra);
    moq_msf_catalog_t cat;
    if (parse_full(json, &cat) != MOQ_OK) return 99;
    moq_msf_stream_mapping_t m = moq_msf_track_stream_mapping(&cat.tracks[0]);
    moq_msf_catalog_cleanup(&g_alloc, &cat);
    return m;
}

static void test_stream_mapping(void)
{
    CHECK(MOQ_MSF_STREAM_MAPPING_GROUP == 0 && MOQ_MSF_STREAM_MAPPING_OBJECT == 1 &&
          MOQ_MSF_STREAM_MAPPING_UNRECOGNIZED == 2);
    CHECK(mapping_of("loc", "") == MOQ_MSF_STREAM_MAPPING_GROUP);
    CHECK(mapping_of("loc", ",\"packagingMetadata\":\"streamMapping=group\"") ==
          MOQ_MSF_STREAM_MAPPING_GROUP);
    CHECK(mapping_of("loc", ",\"packagingMetadata\":\"streamMapping=object\"") ==
          MOQ_MSF_STREAM_MAPPING_OBJECT);
    CHECK(mapping_of("loc", ",\"packagingMetadata\":\"streamMapping=subgroup\"") ==
          MOQ_MSF_STREAM_MAPPING_UNRECOGNIZED);
    /* 5.2.5: the first occurrence of a key is used; other keys are ignored. */
    CHECK(mapping_of("loc", ",\"packagingMetadata\":"
                     "\"streamMapping=object;streamMapping=group\"") ==
          MOQ_MSF_STREAM_MAPPING_OBJECT);
    CHECK(mapping_of("loc", ",\"packagingMetadata\":\"x=1;streamMapping=object\"") ==
          MOQ_MSF_STREAM_MAPPING_OBJECT);
    CHECK(mapping_of("loc", ",\"packagingMetadata\":\"x=1\"") ==
          MOQ_MSF_STREAM_MAPPING_GROUP);
    /* Values are case-sensitive. */
    CHECK(mapping_of("loc", ",\"packagingMetadata\":\"streamMapping=Object\"") ==
          MOQ_MSF_STREAM_MAPPING_UNRECOGNIZED);
    /* The key belongs to "loc": any other packaging defines none. */
    CHECK(mapping_of("mediatimeline", ",\"depends\":[\"t\"],"
                     "\"packagingMetadata\":\"streamMapping=object\"") ==
          MOQ_MSF_STREAM_MAPPING_GROUP);
    CHECK(moq_msf_track_stream_mapping(NULL) == MOQ_MSF_STREAM_MAPPING_GROUP);

    moq_bytes_t v = { NULL, 0 };
    CHECK(moq_msf_packaging_metadata_find(lit("a=1;b=22;a=3"), "a", &v) &&
          bv_eq(v, "1"));
    CHECK(moq_msf_packaging_metadata_find(lit("a=1;b=22;a=3"), "b", &v) &&
          bv_eq(v, "22"));
    CHECK(!moq_msf_packaging_metadata_find(lit("a=1;b=22"), "c", &v));
    CHECK(!moq_msf_packaging_metadata_find(lit("ab=1"), "a", &v));
    CHECK(!moq_msf_packaging_metadata_find(lit("a"), "a", &v));
    CHECK(moq_msf_packaging_metadata_find(lit("junk;a=1"), "a", &v) && bv_eq(v, "1"));
    CHECK(!moq_msf_packaging_metadata_find(lit("a=1"), NULL, &v));
    CHECK(!moq_msf_packaging_metadata_find(lit("a=1"), "a", NULL));
    CHECK(!moq_msf_packaging_metadata_find((moq_bytes_t){ NULL, 3 }, "a", &v));
    check_balance("stream_mapping");
}

/* Strict encode of one MSF-02 track, and whether apply's receive path takes
 * the same track. */
static void required_case(const char *track_json, moq_result_t strict_want)
{
    char json[768];
    snprintf(json, sizeof(json),
             "{\"version\":\"draft-02\",\"generatedAt\":1,\"tracks\":[%s]}",
             track_json);
    moq_msf_catalog_t cat;
    moq_result_t rc = parse_full(json, &cat);
    CHECK(rc == MOQ_OK);
    if (rc != MOQ_OK) return;
    moq_rcbuf_t *enc = NULL;
    rc = moq_msf_catalog_encode(&g_alloc, &cat, &enc);
    if (rc != strict_want) {
        fprintf(stderr, "FAIL: strict encode %s: %d, expected %d\n", track_json,
                rc, strict_want);
        failures++;
    }
    moq_rcbuf_decref(enc);
    /* The receive path is lenient: a peer's catalog still applies. */
    CHECK_APPLY(&cat, "{\"generatedAt\":2,\"deltaUpdate\":[{\"op\":\"add\","
                "\"tracks\":[{\"name\":\"zz\",\"packaging\":\"moqlog\","
                "\"isLive\":true}]}]}", MOQ_OK);
    /* MSF-01 has no such table. */
    cat.revision = MOQ_MSF_REVISION_01;
    rc = moq_msf_catalog_encode(&g_alloc, &cat, &enc);
    CHECK(rc == MOQ_OK || strict_want == MOQ_OK || rc == MOQ_ERR_INVAL);
    moq_rcbuf_decref(enc);
    moq_msf_catalog_cleanup(&g_alloc, &cat);
}

#define LOC(role, f) "{\"name\":\"t\",\"packaging\":\"loc\",\"isLive\":true," \
                     "\"role\":\"" role "\"" f "}"

static void test_required_fields_r02(void)
{
    /* 5.2.4.1, Table 6: every cell missing once. */
    required_case(LOC("audio", ",\"codec\":\"opus\",\"bitrate\":1,"
                      "\"samplerate\":48000,\"channelConfig\":\"2\""), MOQ_OK);
    required_case(LOC("audio", ",\"bitrate\":1,\"samplerate\":48000,"
                      "\"channelConfig\":\"2\""), MOQ_ERR_INVAL);
    required_case(LOC("audio", ",\"codec\":\"opus\",\"samplerate\":48000,"
                      "\"channelConfig\":\"2\""), MOQ_ERR_INVAL);
    required_case(LOC("audio", ",\"codec\":\"opus\",\"bitrate\":1,"
                      "\"channelConfig\":\"2\""), MOQ_ERR_INVAL);
    required_case(LOC("audio", ",\"codec\":\"opus\",\"bitrate\":1,"
                      "\"samplerate\":48000"), MOQ_ERR_INVAL);
    required_case(LOC("video", ",\"codec\":\"av01\",\"bitrate\":1,\"framerate\":30"),
                  MOQ_OK);
    required_case(LOC("video", ",\"bitrate\":1,\"framerate\":30"), MOQ_ERR_INVAL);
    required_case(LOC("video", ",\"codec\":\"av01\",\"framerate\":30"), MOQ_ERR_INVAL);
    required_case(LOC("video", ",\"codec\":\"av01\",\"bitrate\":1"), MOQ_ERR_INVAL);
    required_case(LOC("audiodescription", ",\"codec\":\"opus\",\"samplerate\":1,"
                      "\"channelConfig\":\"2\",\"lang\":\"en\""), MOQ_OK);
    required_case(LOC("audiodescription", ",\"samplerate\":1,"
                      "\"channelConfig\":\"2\",\"lang\":\"en\""), MOQ_ERR_INVAL);
    required_case(LOC("audiodescription", ",\"codec\":\"opus\","
                      "\"channelConfig\":\"2\",\"lang\":\"en\""), MOQ_ERR_INVAL);
    required_case(LOC("audiodescription", ",\"codec\":\"opus\",\"samplerate\":1,"
                      "\"lang\":\"en\""), MOQ_ERR_INVAL);
    required_case(LOC("audiodescription", ",\"codec\":\"opus\",\"samplerate\":1,"
                      "\"channelConfig\":\"2\""), MOQ_ERR_INVAL);
    required_case(LOC("signlanguage", ",\"codec\":\"av01\",\"framerate\":30"), MOQ_OK);
    required_case(LOC("signlanguage", ",\"framerate\":30"), MOQ_ERR_INVAL);
    required_case(LOC("signlanguage", ",\"codec\":\"av01\""), MOQ_ERR_INVAL);
    /* Unlisted pairs have no further required fields. */
    required_case(LOC("caption", ""), MOQ_OK);
    required_case("{\"name\":\"t\",\"packaging\":\"loc\",\"isLive\":true}", MOQ_OK);
    required_case("{\"name\":\"t\",\"packaging\":\"cmaf\",\"isLive\":true,"
                  "\"role\":\"video\"}", MOQ_OK);
    /* eventtimeline/eventtimeline: eventType, which every revision and both
     * paths already require of an event timeline (5.2.6). */
    moq_msf_catalog_t cat;
    CHECK_RC(parse_full(R02("{\"name\":\"e\",\"packaging\":\"eventtimeline\","
                            "\"isLive\":true,\"role\":\"eventtimeline\"}"), &cat),
             MOQ_OK);
    moq_rcbuf_t *enc = NULL;
    CHECK_RC(moq_msf_catalog_encode(&g_alloc, &cat, &enc), MOQ_ERR_INVAL);
    moq_msf_catalog_cleanup(&g_alloc, &cat);
    required_case("{\"name\":\"e\",\"packaging\":\"eventtimeline\",\"isLive\":true,"
                  "\"role\":\"eventtimeline\",\"eventType\":\"x\"}", MOQ_OK);
    /* keyframeSpacing counts Objects: 0 does not author (5.2.46). */
    required_case(LOC("caption", ",\"keyframeSpacing\":0"), MOQ_ERR_INVAL);
    required_case(LOC("caption", ",\"keyframeSpacing\":1"), MOQ_OK);
    /* streamMapping: one of the two modes (4.1.1); packagingMetadata pairs
     * hold no whitespace, '=' or ';' in a key or value (5.2.5). */
    required_case(LOC("caption", ",\"packagingMetadata\":\"streamMapping=object\""),
                  MOQ_OK);
    required_case(LOC("caption", ",\"packagingMetadata\":\"streamMapping=sub\""),
                  MOQ_ERR_INVAL);
    static const char *const pm_bad[] = { "", "a", "a=", "=b", "a=b;", ";a=b",
                                          "a=b=c", "a b=c", "a=\\tb",
                                          "a=b;;c=d" };
    char f[128];
    for (size_t i = 0; i < sizeof(pm_bad) / sizeof(pm_bad[0]); i++) {
        snprintf(f, sizeof(f), ",\"packagingMetadata\":\"%s\"", pm_bad[i]);
        char t[256];
        snprintf(t, sizeof(t), "{\"name\":\"t\",\"packaging\":\"loc\","
                 "\"isLive\":true%s}", f);
        required_case(t, MOQ_ERR_INVAL);
    }
    required_case("{\"name\":\"t\",\"packaging\":\"loc\",\"isLive\":true,"
                  "\"packagingMetadata\":\"x=1;streamMapping=group\"}", MOQ_OK);
    check_balance("required_fields_r02");
}

static void test_alt_group_window(void)
{
    /* 5.2.45: tracks of one alternate group carry the same
     * accessibleWindowTime. */
    required_case(TRK("a", ",\"altGroup\":1,\"accessibleWindowTime\":5") ","
                  TRK("b", ",\"altGroup\":1,\"accessibleWindowTime\":5") ","
                  TRK("c", ",\"altGroup\":2,\"accessibleWindowTime\":9"), MOQ_OK);
    required_case(TRK("a", ",\"altGroup\":1,\"accessibleWindowTime\":5") ","
                  TRK("b", ",\"altGroup\":1,\"accessibleWindowTime\":6"),
                  MOQ_ERR_INVAL);
    required_case(TRK("a", ",\"altGroup\":1,\"accessibleWindowTime\":5") ","
                  TRK("b", ",\"altGroup\":1"), MOQ_ERR_INVAL);
    required_case(TRK("a", ",\"altGroup\":1") "," TRK("b", ",\"altGroup\":1"),
                  MOQ_OK);
    required_case(TRK("a", ",\"accessibleWindowTime\":5") ","
                  TRK("b", ",\"accessibleWindowTime\":6"), MOQ_OK);
    check_balance("alt_group_window");
}

static void test_switch_point(void)
{
    uint64_t g = 99;
    /* 6.3.2: (G * S + N) / T, when it is an integer. */
    CHECK_RC(moq_msf_switch_point_group(2, 0, 15, 30, &g), MOQ_OK);
    CHECK(g == 1);
    CHECK_RC(moq_msf_switch_point_group(1, 0, 15, 30, &g), MOQ_DONE);
    CHECK_RC(moq_msf_switch_point_group(1, 15, 30, 15, &g), MOQ_OK);
    CHECK(g == 3);
    CHECK_RC(moq_msf_switch_point_group(0, 0, 600, 15, &g), MOQ_OK);
    CHECK(g == 0);
    CHECK_RC(moq_msf_switch_point_group(3, 7, 15, 30, &g), MOQ_DONE);
    /* Refused: zero spacing, an Object past the source's interval, overflow,
     * a NULL out. */
    CHECK_RC(moq_msf_switch_point_group(1, 0, 0, 30, &g), MOQ_ERR_INVAL);
    CHECK_RC(moq_msf_switch_point_group(1, 0, 15, 0, &g), MOQ_ERR_INVAL);
    CHECK_RC(moq_msf_switch_point_group(1, 15, 15, 30, &g), MOQ_ERR_INVAL);
    CHECK_RC(moq_msf_switch_point_group(UINT64_MAX / 15 + 1, 0, 15, 30, &g),
             MOQ_ERR_INVAL);
    /* UINT64_MAX is a multiple of 3: G * S lands on it and N = 1 passes it. */
    CHECK_RC(moq_msf_switch_point_group(UINT64_MAX / 3, 1, 3, 3, &g), MOQ_ERR_INVAL);
    CHECK_RC(moq_msf_switch_point_group(UINT64_MAX / 3, 0, 3, 3, &g), MOQ_OK);
    CHECK(g == UINT64_MAX / 3);
    CHECK_RC(moq_msf_switch_point_group(UINT64_MAX / 2, 0, 2, 2, &g), MOQ_OK);
    CHECK(g == UINT64_MAX / 2);
    CHECK_RC(moq_msf_switch_point_group(2, 0, 15, 30, NULL), MOQ_ERR_INVAL);
}

/* -- initDataList property types -------------------------------------------- */

static void test_init_property_type(void)
{
    static const char *const ok13[] = { "0x0D", "0d", "d", "0X0D", "0x000000000000000d" };
    for (size_t i = 0; i < sizeof(ok13) / sizeof(ok13[0]); i++) {
        moq_msf_init_data_entry_t e = { lit("i"), lit("track-property"), lit(ok13[i]) };
        uint64_t t = 0;
        CHECK_RC(moq_msf_init_data_property_type(&e, &t), MOQ_OK);
        CHECK(t == 13);
        e.type = lit("object-property");
        t = 0;
        CHECK_RC(moq_msf_init_data_property_type(&e, &t), MOQ_OK);
        CHECK(t == 13);
    }
    moq_msf_init_data_entry_t e = { lit("i"), lit("track-property"),
                                    lit("0xffffffffffffffff") };
    uint64_t t = 0;
    CHECK_RC(moq_msf_init_data_property_type(&e, &t), MOQ_OK);
    CHECK(t == UINT64_MAX);
    static const char *const bad[] = { "0x", "0xG1", " 0x0D", "0x0D ",
                                       "0x10000000000000000", "", "x0D", "-1",
                                       "0x0x1" };
    for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); i++) {
        e.data = lit(bad[i]);
        t = 7;
        CHECK_RC(moq_msf_init_data_property_type(&e, &t), MOQ_ERR_PROTO);
        CHECK(t == 7);
    }
    e.data = lit("0x0D");
    e.type = lit("inline");
    CHECK_RC(moq_msf_init_data_property_type(&e, &t), MOQ_ERR_INVAL);
    e.type = lit("Track-Property");
    CHECK_RC(moq_msf_init_data_property_type(&e, &t), MOQ_ERR_INVAL);
    e.type = lit("track-property");
    CHECK_RC(moq_msf_init_data_property_type(NULL, &t), MOQ_ERR_INVAL);
    CHECK_RC(moq_msf_init_data_property_type(&e, NULL), MOQ_ERR_INVAL);
    e.data = (moq_bytes_t){ NULL, 2 };
    CHECK_RC(moq_msf_init_data_property_type(&e, &t), MOQ_ERR_INVAL);

    /* Authoring: an MSF-02 initDataList entry has one of the three types,
     * and a property type in hex (5.1.7); MSF-01 is unchanged. */
    static const struct { const char *type, *data; moq_result_t r02; } rows[] = {
        { "inline",          "AAAA", MOQ_OK },
        { "track-property",  "0x0D", MOQ_OK },
        { "object-property", "f",    MOQ_OK },
        { "track-property",  "0xZZ", MOQ_ERR_INVAL },
        { "object-property", "",     MOQ_ERR_INVAL },
        { "other",           "x",    MOQ_ERR_INVAL },
    };
    for (size_t i = 0; i < sizeof(rows) / sizeof(rows[0]); i++) {
        moq_msf_init_data_entry_t ide = { lit("i"), lit(rows[i].type),
                                          lit(rows[i].data) };
        moq_msf_track_t tr;
        memset(&tr, 0, sizeof(tr));
        tr.struct_size = sizeof(tr);
        tr.name = lit("a"); tr.packaging = lit("loc"); tr.is_live = true;
        moq_msf_catalog_t c;
        memset(&c, 0, sizeof(c));
        c.struct_size = sizeof(c);
        c.version = MOQ_MSF_VERSION;
        c.tracks = &tr; c.track_count = 1;
        c.init_data_list = &ide; c.init_data_count = 1;
        c.revision = MOQ_MSF_REVISION_02;
        moq_rcbuf_t *enc = NULL;
        moq_result_t rc = moq_msf_catalog_encode(&g_alloc, &c, &enc);
        if (rc != rows[i].r02) {
            fprintf(stderr, "FAIL: init entry row %zu: %d\n", i, rc);
            failures++;
        }
        moq_rcbuf_decref(enc);
        enc = NULL;
        c.revision = MOQ_MSF_REVISION_01;
        if (rows[i].data[0]) {
            CHECK_RC(moq_msf_catalog_encode(&g_alloc, &c, &enc), MOQ_OK);
            moq_rcbuf_decref(enc);
        }
    }
    check_balance("init_property_type");
}

/* -- Allocation failure ----------------------------------------------------- */

static void test_oom_parse_apply(void)
{
    static const char *const docs[] = { "5_6_17_publish_tracks.json",
                                        "5_6_11_captions_scte35.json" };
    for (size_t k = 0; k < sizeof(docs) / sizeof(docs[0]); k++) {
        size_t n = 0;
        char *json = read_fixture(docs[k], &n);
        if (!json) continue;
        moq_bytes_t j = { (const uint8_t *)json, n };
        for (uint64_t fail = 1; fail < 64; fail++) {
            memset(&g_count, 0, sizeof(g_count));
            g_count.fail_at = fail;
            moq_msf_catalog_t cat;
            moq_result_t rc = moq_msf_catalog_parse(&g_alloc, j, &cat);
            CHECK(rc == MOQ_OK || rc == MOQ_ERR_NOMEM);
            if (rc == MOQ_OK) {
                moq_msf_catalog_t d, out;
                g_count.fail_at = 0;
                CHECK_RC(parse_full("{\"generatedAt\":1,\"deltaUpdate\":[{\"op\":"
                                    "\"add\",\"tracks\":[{\"name\":\"z\","
                                    "\"packaging\":\"loc\",\"isLive\":true}]}]}",
                                    &d), MOQ_OK);
                g_count.calls = 0;
                g_count.fail_at = fail;
                rc = moq_msf_catalog_apply_delta_sized(&g_alloc, &cat, &d, NULL,
                                                       &out, sizeof(out));
                CHECK(rc == MOQ_OK || rc == MOQ_ERR_NOMEM);
                if (rc == MOQ_OK) moq_msf_catalog_cleanup(&g_alloc, &out);
                else CHECK(out.struct_size == sizeof(out) && !out._dom);
                moq_msf_catalog_cleanup(&g_alloc, &d);
                moq_msf_catalog_cleanup(&g_alloc, &cat);
            }
            g_count.fail_at = 0;
            check_balance("oom_parse_apply");
        }
        free(json);
    }
    memset(&g_count, 0, sizeof(g_count));
}

int main(void)
{
    test_layout();
    test_revision_parse();
    test_extent_boundaries();
    test_frozen_symbol();
    test_presence_header();
    test_examples_roundtrip_r02();
    test_timeline_relaxation();
    test_apply_keeps_revision_and_publish();
    test_apply_error_code();
    test_publish_tracks();
    test_supplement_init_sized();
    test_depends_forms();
    test_check_depends();
    test_check_reason_values();
    test_supplement_r01_unknown_keys();
    test_encryption_fields_both_revisions();
    test_supplement_clone_merge();
    test_update_example();
    test_update_rules();
    test_delta_r02_roots();
    test_authored_delta_paths();
    test_delta_authoring_roots();
    test_validate_after_all_ops();
    test_illogical_ops_r02();
    test_namespace_split();
    test_string_to_bytes_vectors();
    test_track_matches();
    test_full_name_identity();
    test_naming_parse_r02();
    test_naming_encode_r02();
    test_bom_and_raw_utf8();
    test_stream_mapping();
    test_required_fields_r02();
    test_alt_group_window();
    test_switch_point();
    test_init_property_type();
    test_oom_parse_apply();

    printf("%s: %d failures\n", failures ? "FAIL" : "PASS", failures);
    return failures ? 1 : 0;
}
