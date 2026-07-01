#include <moq/msf.h>
#include <moq/rcbuf.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>

/* White-box seam: the parser's internal overflow-safe array sizer. Not part of
 * the public API (hidden visibility); declared here so the boundary can be
 * tested directly, which a real 64-bit JSON input cannot reach. */
extern bool moq_msf_checked_array_bytes(size_t count, size_t elem,
                                        size_t *out_bytes);

static int failures = 0;

#define CHECK(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)

#define CHECK_EQ_SIZE(a, b) do { \
    size_t _a = (a), _b = (b); \
    if (_a != _b) { \
        fprintf(stderr, "FAIL: %s:%d: %s == %zu, expected %s == %zu\n", \
                __FILE__, __LINE__, #a, _a, #b, _b); \
        failures++; \
    } \
} while (0)

#define CHECK_EQ_U64(a, b) do { \
    uint64_t _a = (a), _b = (b); \
    if (_a != _b) { \
        fprintf(stderr, "FAIL: %s:%d: %s == %llu, expected %s == %llu\n", \
                __FILE__, __LINE__, #a, (unsigned long long)_a, \
                #b, (unsigned long long)_b); \
        failures++; \
    } \
} while (0)

static moq_bytes_t lit(const char *s)
{
    return (moq_bytes_t){ (const uint8_t *)s, strlen(s) };
}

static bool bv_eq(moq_bytes_t a, const char *s)
{
    size_t len = strlen(s);
    return a.len == len && memcmp(a.data, s, len) == 0;
}

#ifdef MOQ_MSF_FIXTURE_DIR
static char *read_fixture(const char *name, size_t *out_len)
{
    char path[1024];
    snprintf(path, sizeof(path), "%s/%s", MOQ_MSF_FIXTURE_DIR, name);
    FILE *fp = fopen(path, "rb");
    if (!fp) return NULL;
    if (fseek(fp, 0, SEEK_END) != 0) { fclose(fp); return NULL; }
    long sz = ftell(fp);
    if (sz < 0 || fseek(fp, 0, SEEK_SET) != 0) { fclose(fp); return NULL; }
    char *buf = (char *)malloc((size_t)sz + 1);
    if (!buf) { fclose(fp); return NULL; }
    size_t rd = fread(buf, 1, (size_t)sz, fp);
    fclose(fp);
    buf[rd] = '\0';
    if (out_len) *out_len = rd;
    return buf;
}
#endif

static bool contains_bytes(moq_bytes_t hay, const char *needle)
{
    size_t n = strlen(needle);
    if (hay.len < n) return false;
    for (size_t i = 0; i + n <= hay.len; i++)
        if (memcmp(hay.data + i, needle, n) == 0) return true;
    return false;
}

/* -- OOM allocator --------------------------------------------------- */

typedef struct {
    int64_t  balance;
    uint64_t alloc_count;
    uint64_t fail_at;
} oom_state_t;

static void *oom_alloc(size_t sz, void *ctx)
{
    oom_state_t *s = (oom_state_t *)ctx;
    if (sz == 0) return NULL;
    s->alloc_count++;
    if (s->fail_at > 0 && s->alloc_count == s->fail_at)
        return NULL;
    void *p = malloc(sz);
    if (p) s->balance++;
    return p;
}

static void oom_free(void *p, size_t sz, void *ctx)
{
    oom_state_t *s = (oom_state_t *)ctx;
    (void)sz;
    if (p) s->balance--;
    free(p);
}

static moq_alloc_t oom_allocator(oom_state_t *s)
{
    return (moq_alloc_t){ s, oom_alloc, NULL, oom_free };
}

static bool buf_contains(const uint8_t *hay, size_t hlen,
                          const char *needle)
{
    size_t nlen = strlen(needle);
    if (nlen > hlen) return false;
    for (size_t i = 0; i <= hlen - nlen; i++) {
        if (memcmp(hay + i, needle, nlen) == 0) return true;
    }
    return false;
}

/* -- Byte-counting allocator ----------------------------------------- */

typedef struct {
    int64_t balance;
    size_t  bytes_alloc;
    size_t  bytes_free;
} sz_state_t;

static void *sz_alloc_fn(size_t sz, void *ctx)
{
    sz_state_t *s = (sz_state_t *)ctx;
    s->balance++;
    s->bytes_alloc += sz;
    return malloc(sz);
}

static void sz_free_fn(void *p, size_t sz, void *ctx)
{
    sz_state_t *s = (sz_state_t *)ctx;
    if (p) { s->balance--; s->bytes_free += sz; }
    free(p);
}

int main(void)
{
    const moq_alloc_t *alloc = moq_alloc_default();

    /* -- parse minimal catalog --------------------------------------- */
    {
        const char *json =
            "{\"version\":1,\"tracks\":["
            "{\"name\":\"video\",\"packaging\":\"loc\",\"isLive\":true}"
            "]}";
        moq_msf_catalog_t cat;
        moq_result_t rc = moq_msf_catalog_parse(alloc, lit(json), &cat);
        CHECK(rc == MOQ_OK);
        CHECK(cat.version == 1);
        CHECK_EQ_SIZE(cat.track_count, 1);
        CHECK(bv_eq(cat.tracks[0].name, "video"));
        CHECK(bv_eq(cat.tracks[0].packaging, "loc"));
        CHECK(cat.tracks[0].is_live == true);
        CHECK(cat.has_generated_at == false);
        moq_msf_catalog_cleanup(alloc, &cat);
    }

    /* -- parse audio + video ----------------------------------------- */
    {
        const char *json =
            "{\"version\":1,\"generatedAt\":1746104606044,\"tracks\":["
            "{\"name\":\"1080p-video\","
            "\"namespace\":\"conference.example.com/conference123/alice\","
            "\"packaging\":\"loc\",\"isLive\":true,"
            "\"targetLatency\":2000,\"role\":\"video\","
            "\"renderGroup\":1,\"codec\":\"av01.0.08M.10.0.110.09\","
            "\"width\":1920,\"height\":1080,\"framerate\":30,\"bitrate\":1500000},"
            "{\"name\":\"audio\","
            "\"namespace\":\"conference.example.com/conference123/alice\","
            "\"packaging\":\"loc\",\"isLive\":true,"
            "\"targetLatency\":2000,\"role\":\"audio\","
            "\"codec\":\"opus\",\"samplerate\":48000,"
            "\"channelConfig\":\"2\",\"bitrate\":32000,\"renderGroup\":1}"
            "]}";
        moq_msf_catalog_t cat;
        moq_result_t rc = moq_msf_catalog_parse(alloc, lit(json), &cat);
        CHECK(rc == MOQ_OK);
        CHECK_EQ_SIZE(cat.track_count, 2);
        CHECK(cat.has_generated_at == true);
        CHECK_EQ_U64(cat.generated_at, 1746104606044ULL);

        const moq_msf_track_t *v = &cat.tracks[0];
        CHECK(bv_eq(v->name, "1080p-video"));
        CHECK(v->has_role && bv_eq(v->role, "video"));
        CHECK(v->has_codec && bv_eq(v->codec, "av01.0.08M.10.0.110.09"));
        CHECK(v->has_width && v->width == 1920);
        CHECK(v->has_height && v->height == 1080);
        CHECK(v->has_framerate && v->framerate_millis == 30000);
        CHECK(v->has_bitrate && v->bitrate == 1500000);
        CHECK(v->has_render_group && v->render_group == 1);
        CHECK(v->has_target_latency && v->target_latency == 2000);
        CHECK(v->has_namespace);

        const moq_msf_track_t *a = &cat.tracks[1];
        CHECK(bv_eq(a->name, "audio"));
        CHECK(a->has_role && bv_eq(a->role, "audio"));
        CHECK(a->has_codec && bv_eq(a->codec, "opus"));
        CHECK(a->has_samplerate && a->samplerate == 48000);
        CHECK(a->has_channel_config && bv_eq(a->channel_config, "2"));

        moq_msf_catalog_cleanup(alloc, &cat);
    }

    /* -- find_role ---------------------------------------------------- */
    {
        const char *json =
            "{\"version\":1,\"tracks\":["
            "{\"name\":\"v\",\"packaging\":\"loc\",\"isLive\":true,\"role\":\"video\"},"
            "{\"name\":\"a\",\"packaging\":\"loc\",\"isLive\":true,\"role\":\"audio\"}"
            "]}";
        moq_msf_catalog_t cat;
        CHECK(moq_msf_catalog_parse(alloc, lit(json), &cat) == MOQ_OK);

        const moq_msf_track_t *v = moq_msf_catalog_find_role(&cat, "video");
        CHECK(v != NULL);
        CHECK(bv_eq(v->name, "v"));

        const moq_msf_track_t *a = moq_msf_catalog_find_role(&cat, "audio");
        CHECK(a != NULL);
        CHECK(bv_eq(a->name, "a"));

        CHECK(moq_msf_catalog_find_role(&cat, "subtitle") == NULL);

        moq_msf_catalog_cleanup(alloc, &cat);
    }

    /* -- initData borrowed ------------------------------------------- */
    {
        const char *json =
            "{\"version\":1,\"tracks\":["
            "{\"name\":\"v\",\"packaging\":\"loc\",\"isLive\":true,"
            "\"initData\":\"AAAB\"}"
            "]}";
        moq_msf_catalog_t cat;
        CHECK(moq_msf_catalog_parse(alloc, lit(json), &cat) == MOQ_OK);
        CHECK(cat.tracks[0].has_init_data);
        CHECK(bv_eq(cat.tracks[0].init_data, "AAAB"));
        moq_msf_catalog_cleanup(alloc, &cat);
    }

    /* -- unknown fields ignored -------------------------------------- */
    {
        const char *json =
            "{\"version\":1,\"customTop\":\"ignored\",\"tracks\":["
            "{\"name\":\"v\",\"packaging\":\"loc\",\"isLive\":true,"
            "\"futureField\":42,\"anotherUnknown\":{\"nested\":true}}"
            "]}";
        moq_msf_catalog_t cat;
        CHECK(moq_msf_catalog_parse(alloc, lit(json), &cat) == MOQ_OK);
        CHECK_EQ_SIZE(cat.track_count, 1);
        CHECK(bv_eq(cat.tracks[0].name, "v"));
        moq_msf_catalog_cleanup(alloc, &cat);
    }

    /* -- empty tracks array ------------------------------------------ */
    {
        const char *json = "{\"version\":1,\"tracks\":[]}";
        moq_msf_catalog_t cat;
        CHECK(moq_msf_catalog_parse(alloc, lit(json), &cat) == MOQ_OK);
        CHECK_EQ_SIZE(cat.track_count, 0);
        CHECK(cat.tracks == NULL);
        moq_msf_catalog_cleanup(alloc, &cat);
    }

    /* -- missing version -------------------------------------------- */
    {
        const char *json = "{\"tracks\":[{\"name\":\"v\",\"packaging\":\"loc\",\"isLive\":true}]}";
        moq_msf_catalog_t cat;
        CHECK(moq_msf_catalog_parse(alloc, lit(json), &cat) == MOQ_ERR_PROTO);
    }

    /* -- wrong version ---------------------------------------------- */
    {
        const char *json = "{\"version\":2,\"tracks\":[{\"name\":\"v\",\"packaging\":\"loc\",\"isLive\":true}]}";
        moq_msf_catalog_t cat;
        CHECK(moq_msf_catalog_parse(alloc, lit(json), &cat) == MOQ_ERR_PROTO);
    }

    /* -- MSF-01 §5.1.1: version is a String. Parse accepts the bare "1" (the
     * value reserved for the final MSF version); the encoder emits "draft-01"
     * for this draft (asserted in the encode tests below). ------------------ */
    {
        const char *json = "{\"version\":\"1\",\"tracks\":[]}";
        moq_msf_catalog_t cat;
        CHECK(moq_msf_catalog_parse(alloc, lit(json), &cat) == MOQ_OK);
        CHECK(cat.version == MOQ_MSF_VERSION);
        moq_msf_catalog_cleanup(alloc, &cat);
    }

    /* -- Accept (and emit) the draft-XX convention for this draft: "draft-01" */
    {
        const char *json = "{\"version\":\"draft-01\",\"tracks\":[]}";
        moq_msf_catalog_t cat;
        CHECK(moq_msf_catalog_parse(alloc, lit(json), &cat) == MOQ_OK);
        moq_msf_catalog_cleanup(alloc, &cat);
    }

    /* -- numeric 1 accepted for LEGACY (MSF-00) compatibility only ------- */
    {
        const char *json = "{\"version\":1,\"tracks\":[]}";
        moq_msf_catalog_t cat;
        CHECK(moq_msf_catalog_parse(alloc, lit(json), &cat) == MOQ_OK);
        moq_msf_catalog_cleanup(alloc, &cat);
    }

    /* -- reject unsupported / malformed version values ------------------- */
    {
        const char *bad[] = {
            "{\"version\":\"2\",\"tracks\":[]}",        /* other numeric-as-string */
            "{\"version\":\"draft-00\",\"tracks\":[]}", /* other draft */
            "{\"version\":\"draft-99\",\"tracks\":[]}", /* unknown draft */
            "{\"version\":\"draft-1\",\"tracks\":[]}",  /* malformed draft (not draft-01) */
            "{\"version\":\"\",\"tracks\":[]}",         /* empty string */
            "{\"version\":2,\"tracks\":[]}",            /* other numeric */
            "{\"version\":true,\"tracks\":[]}",         /* non-string/non-number */
        };
        for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); i++) {
            moq_msf_catalog_t cat;
            CHECK(moq_msf_catalog_parse(alloc, lit(bad[i]), &cat) == MOQ_ERR_PROTO);
        }
    }

    /* -- missing tracks --------------------------------------------- */
    {
        const char *json = "{\"version\":1}";
        moq_msf_catalog_t cat;
        CHECK(moq_msf_catalog_parse(alloc, lit(json), &cat) == MOQ_ERR_PROTO);
    }

    /* -- malformed JSON --------------------------------------------- */
    {
        const char *json = "{not valid json at all";
        moq_msf_catalog_t cat;
        CHECK(moq_msf_catalog_parse(alloc, lit(json), &cat) == MOQ_ERR_PROTO);
    }

    /* -- track missing required name -------------------------------- */
    {
        const char *json = "{\"version\":1,\"tracks\":[{\"packaging\":\"loc\",\"isLive\":true}]}";
        moq_msf_catalog_t cat;
        CHECK(moq_msf_catalog_parse(alloc, lit(json), &cat) == MOQ_ERR_PROTO);
    }

    /* -- track missing packaging ------------------------------------ */
    {
        const char *json = "{\"version\":1,\"tracks\":[{\"name\":\"v\",\"isLive\":true}]}";
        moq_msf_catalog_t cat;
        CHECK(moq_msf_catalog_parse(alloc, lit(json), &cat) == MOQ_ERR_PROTO);
    }

    /* -- NULL args --------------------------------------------------- */
    {
        moq_msf_catalog_t cat;
        CHECK(moq_msf_catalog_parse(NULL, lit("{}"), &cat) == MOQ_ERR_INVAL);
        CHECK(moq_msf_catalog_parse(alloc, (moq_bytes_t){NULL, 0}, &cat) == MOQ_ERR_INVAL);
        CHECK(moq_msf_catalog_parse(alloc, lit("{}"), NULL) == MOQ_ERR_INVAL);
        moq_msf_catalog_cleanup(alloc, NULL);
        CHECK(moq_msf_catalog_find_role(NULL, "video") == NULL);
    }

    /* -- OOM: fail JSON DOM allocation ------------------------------- */
    {
        const char *json =
            "{\"version\":1,\"tracks\":["
            "{\"name\":\"v\",\"packaging\":\"loc\",\"isLive\":true}"
            "]}";
        oom_state_t oom = { 0, 0, 1 };
        moq_alloc_t fa = oom_allocator(&oom);
        moq_msf_catalog_t cat;
        CHECK(moq_msf_catalog_parse(&fa, lit(json), &cat) == MOQ_ERR_NOMEM);
        CHECK(oom.balance == 0);
    }

    /* -- OOM: fail track array allocation ----------------------------- */
    {
        const char *json =
            "{\"version\":1,\"tracks\":["
            "{\"name\":\"v\",\"packaging\":\"loc\",\"isLive\":true}"
            "]}";
        oom_state_t oom = { 0, 0, 2 };
        moq_alloc_t fa = oom_allocator(&oom);
        moq_msf_catalog_t cat;
        CHECK(moq_msf_catalog_parse(&fa, lit(json), &cat) == MOQ_ERR_NOMEM);
        CHECK(oom.balance == 0);
    }

    /* -- label and lang fields --------------------------------------- */
    {
        const char *json =
            "{\"version\":1,\"tracks\":["
            "{\"name\":\"sub\",\"packaging\":\"loc\",\"isLive\":false,"
            "\"lang\":\"en\",\"label\":\"English\"}"
            "]}";
        moq_msf_catalog_t cat;
        CHECK(moq_msf_catalog_parse(alloc, lit(json), &cat) == MOQ_OK);
        CHECK(cat.tracks[0].has_lang && bv_eq(cat.tracks[0].lang, "en"));
        CHECK(cat.tracks[0].has_label && bv_eq(cat.tracks[0].label, "English"));
        CHECK(cat.tracks[0].is_live == false);
        moq_msf_catalog_cleanup(alloc, &cat);
    }

    /* -- timescale field --------------------------------------------- */
    {
        const char *json =
            "{\"version\":1,\"tracks\":["
            "{\"name\":\"v\",\"packaging\":\"loc\",\"isLive\":true,"
            "\"timescale\":90000}"
            "]}";
        moq_msf_catalog_t cat;
        CHECK(moq_msf_catalog_parse(alloc, lit(json), &cat) == MOQ_OK);
        CHECK(cat.tracks[0].has_timescale);
        CHECK_EQ_U64(cat.tracks[0].timescale, 90000);
        moq_msf_catalog_cleanup(alloc, &cat);
    }

    /* -- negative width rejected --------------------------------------- */
    {
        const char *json =
            "{\"version\":1,\"tracks\":["
            "{\"name\":\"v\",\"packaging\":\"loc\",\"isLive\":true,"
            "\"width\":-1}"
            "]}";
        moq_msf_catalog_t cat;
        CHECK(moq_msf_catalog_parse(alloc, lit(json), &cat) == MOQ_OK);
        CHECK(cat.tracks[0].has_width == false);
        moq_msf_catalog_cleanup(alloc, &cat);
    }

    /* -- fractional width rejected ------------------------------------ */
    {
        const char *json =
            "{\"version\":1,\"tracks\":["
            "{\"name\":\"v\",\"packaging\":\"loc\",\"isLive\":true,"
            "\"width\":12.5}"
            "]}";
        moq_msf_catalog_t cat;
        CHECK(moq_msf_catalog_parse(alloc, lit(json), &cat) == MOQ_OK);
        CHECK(cat.tracks[0].has_width == false);
        moq_msf_catalog_cleanup(alloc, &cat);
    }

    /* -- UINT32_MAX+1 width rejected ---------------------------------- */
    {
        const char *json =
            "{\"version\":1,\"tracks\":["
            "{\"name\":\"v\",\"packaging\":\"loc\",\"isLive\":true,"
            "\"width\":4294967296}"
            "]}";
        moq_msf_catalog_t cat;
        CHECK(moq_msf_catalog_parse(alloc, lit(json), &cat) == MOQ_OK);
        CHECK(cat.tracks[0].has_width == false);
        moq_msf_catalog_cleanup(alloc, &cat);
    }

    /* -- negative generatedAt rejected -------------------------------- */
    {
        const char *json =
            "{\"version\":1,\"generatedAt\":-100,\"tracks\":["
            "{\"name\":\"v\",\"packaging\":\"loc\",\"isLive\":true}"
            "]}";
        moq_msf_catalog_t cat;
        CHECK(moq_msf_catalog_parse(alloc, lit(json), &cat) == MOQ_OK);
        CHECK(cat.has_generated_at == false);
        moq_msf_catalog_cleanup(alloc, &cat);
    }

    /* -- renderGroup out of int range --------------------------------- */
    {
        const char *json =
            "{\"version\":1,\"tracks\":["
            "{\"name\":\"v\",\"packaging\":\"loc\",\"isLive\":true,"
            "\"renderGroup\":99999999999999}"
            "]}";
        moq_msf_catalog_t cat;
        CHECK(moq_msf_catalog_parse(alloc, lit(json), &cat) == MOQ_OK);
        CHECK(cat.tracks[0].has_render_group == false);
        moq_msf_catalog_cleanup(alloc, &cat);
    }

    /* -- generatedAt > UINT64_MAX rejected ----------------------------- */
    {
        const char *json =
            "{\"version\":1,\"generatedAt\":99999999999999999999999,\"tracks\":["
            "{\"name\":\"v\",\"packaging\":\"loc\",\"isLive\":true}"
            "]}";
        moq_msf_catalog_t cat;
        CHECK(moq_msf_catalog_parse(alloc, lit(json), &cat) == MOQ_OK);
        CHECK(cat.has_generated_at == false);
        moq_msf_catalog_cleanup(alloc, &cat);
    }

    /* -- huge framerate rejected -------------------------------------- */
    {
        const char *json =
            "{\"version\":1,\"tracks\":["
            "{\"name\":\"v\",\"packaging\":\"loc\",\"isLive\":true,"
            "\"framerate\":1e308}"
            "]}";
        moq_msf_catalog_t cat;
        CHECK(moq_msf_catalog_parse(alloc, lit(json), &cat) == MOQ_OK);
        CHECK(cat.tracks[0].has_framerate == false);
        moq_msf_catalog_cleanup(alloc, &cat);
    }

    /* -- renderGroup past long max rejected --------------------------- */
    {
        const char *json =
            "{\"version\":1,\"tracks\":["
            "{\"name\":\"v\",\"packaging\":\"loc\",\"isLive\":true,"
            "\"renderGroup\":999999999999999999999}"
            "]}";
        moq_msf_catalog_t cat;
        CHECK(moq_msf_catalog_parse(alloc, lit(json), &cat) == MOQ_OK);
        CHECK(cat.tracks[0].has_render_group == false);
        moq_msf_catalog_cleanup(alloc, &cat);
    }

    /* -- byte-counting OOM: DOM freed with correct size --------------- */
    {
        const char *json =
            "{\"version\":1,\"tracks\":["
            "{\"name\":\"v\",\"packaging\":\"loc\",\"isLive\":true}"
            "]}";

        sz_state_t szs = { 0, 0, 0 };
        moq_alloc_t sza = { &szs, sz_alloc_fn, NULL, sz_free_fn };

        moq_msf_catalog_t cat;
        moq_result_t rc = moq_msf_catalog_parse(&sza, lit(json), &cat);
        CHECK(rc == MOQ_OK);
        CHECK(szs.balance == 2); /* DOM + tracks array */
        CHECK(szs.bytes_alloc > 0);

        moq_msf_catalog_cleanup(&sza, &cat);
        CHECK(szs.balance == 0);
        CHECK(szs.bytes_free == szs.bytes_alloc);
    }

    /* ================================================================ */
    /* Encoder tests                                                    */
    /* ================================================================ */

    /* -- encode empty catalog, parse back ----------------------------- */
    {
        moq_msf_catalog_t cat;
        memset(&cat, 0, sizeof(cat));
        cat.struct_size = sizeof(cat);
        cat.version = MOQ_MSF_VERSION;

        moq_rcbuf_t *out = NULL;
        CHECK(moq_msf_catalog_encode(alloc, &cat, &out) == MOQ_OK);
        CHECK(out != NULL);

        /* MSF-01 §5.1.1 wire form: version is the JSON String "draft-01" (the
         * Internet-Draft "draft-XX" convention for this draft). */
        {
            const char *d = (const char *)moq_rcbuf_data(out);
            size_t n = moq_rcbuf_len(out);
            char tmp[256];
            size_t c = n < sizeof(tmp) - 1 ? n : sizeof(tmp) - 1;
            memcpy(tmp, d, c);
            tmp[c] = '\0';
            CHECK(strstr(tmp, "\"version\":\"draft-01\"") != NULL);
        }

        moq_msf_catalog_t parsed;
        moq_bytes_t json = { moq_rcbuf_data(out), moq_rcbuf_len(out) };
        CHECK(moq_msf_catalog_parse(alloc, json, &parsed) == MOQ_OK);
        CHECK_EQ_SIZE(parsed.track_count, 0);
        moq_msf_catalog_cleanup(alloc, &parsed);
        moq_rcbuf_decref(out);
    }

    /* -- encode minimal one-track, parse back ------------------------- */
    {
        moq_msf_track_t t;
        memset(&t, 0, sizeof(t));
        t.struct_size = sizeof(t);
        t.name = lit("video");
        t.packaging = lit("loc");
        t.is_live = true;

        moq_msf_catalog_t cat;
        memset(&cat, 0, sizeof(cat));
        cat.struct_size = sizeof(cat);
        cat.version = MOQ_MSF_VERSION;
        cat.tracks = &t;
        cat.track_count = 1;

        moq_rcbuf_t *out = NULL;
        CHECK(moq_msf_catalog_encode(alloc, &cat, &out) == MOQ_OK);

        moq_msf_catalog_t parsed;
        moq_bytes_t json = { moq_rcbuf_data(out), moq_rcbuf_len(out) };
        CHECK(moq_msf_catalog_parse(alloc, json, &parsed) == MOQ_OK);
        CHECK_EQ_SIZE(parsed.track_count, 1);
        CHECK(bv_eq(parsed.tracks[0].name, "video"));
        CHECK(bv_eq(parsed.tracks[0].packaging, "loc"));
        CHECK(parsed.tracks[0].is_live == true);
        moq_msf_catalog_cleanup(alloc, &parsed);
        moq_rcbuf_decref(out);
    }

    /* -- encode full audio+video, parse back all fields ---------------- */
    {
        moq_msf_track_t tracks[2];
        memset(tracks, 0, sizeof(tracks));

        tracks[0].struct_size = sizeof(tracks[0]);
        tracks[0].name = lit("hd");
        tracks[0].packaging = lit("loc");
        tracks[0].is_live = true;
        tracks[0].has_namespace = true;
        tracks[0].namespace_ = lit("live/cam1");
        tracks[0].has_role = true;
        tracks[0].role = lit("video");
        tracks[0].has_codec = true;
        tracks[0].codec = lit("avc1.42e01e");
        tracks[0].has_width = true;
        tracks[0].width = 1920;
        tracks[0].has_height = true;
        tracks[0].height = 1080;
        tracks[0].has_framerate = true;
        tracks[0].framerate_millis = 30000;
        tracks[0].has_bitrate = true;
        tracks[0].bitrate = 5000000;
        tracks[0].has_render_group = true;
        tracks[0].render_group = 1;
        tracks[0].has_alt_group = true;
        tracks[0].alt_group = 2;
        tracks[0].has_target_latency = true;
        tracks[0].target_latency = 2000;
        tracks[0].has_timescale = true;
        tracks[0].timescale = 90000;
        tracks[0].has_init_data = true;
        tracks[0].init_data = lit("AAAB");
        tracks[0].has_label = true;
        tracks[0].label = lit("HD Video");

        tracks[1].struct_size = sizeof(tracks[1]);
        tracks[1].name = lit("audio");
        tracks[1].packaging = lit("loc");
        tracks[1].is_live = true;
        tracks[1].has_role = true;
        tracks[1].role = lit("audio");
        tracks[1].has_codec = true;
        tracks[1].codec = lit("opus");
        tracks[1].has_samplerate = true;
        tracks[1].samplerate = 48000;
        tracks[1].has_channel_config = true;
        tracks[1].channel_config = lit("2");
        tracks[1].has_lang = true;
        tracks[1].lang = lit("en");

        moq_msf_catalog_t cat;
        memset(&cat, 0, sizeof(cat));
        cat.struct_size = sizeof(cat);
        cat.version = MOQ_MSF_VERSION;
        cat.tracks = tracks;
        cat.track_count = 2;
        cat.has_generated_at = true;
        cat.generated_at = 1746104606044ULL;

        moq_rcbuf_t *out = NULL;
        CHECK(moq_msf_catalog_encode(alloc, &cat, &out) == MOQ_OK);

        moq_msf_catalog_t p;
        moq_bytes_t json = { moq_rcbuf_data(out), moq_rcbuf_len(out) };
        CHECK(moq_msf_catalog_parse(alloc, json, &p) == MOQ_OK);
        CHECK_EQ_SIZE(p.track_count, 2);
        CHECK_EQ_U64(p.generated_at, 1746104606044ULL);
        CHECK(p.tracks[0].has_width && p.tracks[0].width == 1920);
        CHECK(p.tracks[0].has_height && p.tracks[0].height == 1080);
        CHECK(p.tracks[0].has_framerate && p.tracks[0].framerate_millis == 30000);
        CHECK(p.tracks[0].has_bitrate && p.tracks[0].bitrate == 5000000);
        CHECK(p.tracks[0].has_render_group && p.tracks[0].render_group == 1);
        CHECK(p.tracks[0].has_alt_group && p.tracks[0].alt_group == 2);
        CHECK(p.tracks[0].has_timescale && p.tracks[0].timescale == 90000);
        CHECK(p.tracks[0].has_target_latency && p.tracks[0].target_latency == 2000);
        CHECK(p.tracks[0].has_init_data && bv_eq(p.tracks[0].init_data, "AAAB"));
        CHECK(p.tracks[0].has_label && bv_eq(p.tracks[0].label, "HD Video"));
        CHECK(p.tracks[1].has_samplerate && p.tracks[1].samplerate == 48000);
        CHECK(p.tracks[1].has_channel_config && bv_eq(p.tracks[1].channel_config, "2"));
        CHECK(p.tracks[1].has_lang && bv_eq(p.tracks[1].lang, "en"));
        moq_msf_catalog_cleanup(alloc, &p);
        moq_rcbuf_decref(out);
    }

    /* -- string escaping: quote, backslash, newline -------------------- */
    {
        moq_msf_track_t t;
        memset(&t, 0, sizeof(t));
        t.struct_size = sizeof(t);
        t.name = lit("video");
        t.packaging = lit("loc");
        t.is_live = true;
        t.has_label = true;
        uint8_t tricky[] = {'H', 'e', 'l', '"', '\\', '\n', 'o'};
        t.label = (moq_bytes_t){ tricky, sizeof(tricky) };

        moq_msf_catalog_t cat;
        memset(&cat, 0, sizeof(cat));
        cat.struct_size = sizeof(cat);
        cat.version = MOQ_MSF_VERSION;
        cat.tracks = &t;
        cat.track_count = 1;

        moq_rcbuf_t *out = NULL;
        CHECK(moq_msf_catalog_encode(alloc, &cat, &out) == MOQ_OK);

        moq_msf_catalog_t p;
        moq_bytes_t json = { moq_rcbuf_data(out), moq_rcbuf_len(out) };
        CHECK(moq_msf_catalog_parse(alloc, json, &p) == MOQ_OK);
        CHECK(p.tracks[0].has_label);
        CHECK(p.tracks[0].label.len == sizeof(tricky));
        CHECK(memcmp(p.tracks[0].label.data, tricky, sizeof(tricky)) == 0);
        moq_msf_catalog_cleanup(alloc, &p);
        moq_rcbuf_decref(out);
    }

    /* -- framerate formatting: 30, 29.97, 33.333 ---------------------- */
    {
        moq_msf_track_t tracks[3];
        memset(tracks, 0, sizeof(tracks));
        for (int i = 0; i < 3; i++) {
            tracks[i].struct_size = sizeof(tracks[i]);
            tracks[i].name = lit(i == 0 ? "a" : i == 1 ? "b" : "c");
            tracks[i].packaging = lit("loc");
            tracks[i].is_live = true;
            tracks[i].has_framerate = true;
        }
        tracks[0].framerate_millis = 30000;
        tracks[1].framerate_millis = 29970;
        tracks[2].framerate_millis = 33333;

        moq_msf_catalog_t cat;
        memset(&cat, 0, sizeof(cat));
        cat.struct_size = sizeof(cat);
        cat.version = MOQ_MSF_VERSION;
        cat.tracks = tracks;
        cat.track_count = 3;

        moq_rcbuf_t *out = NULL;
        CHECK(moq_msf_catalog_encode(alloc, &cat, &out) == MOQ_OK);

        /* Check raw JSON for correct framerate formatting. */
        const uint8_t *d = moq_rcbuf_data(out);
        size_t dlen = moq_rcbuf_len(out);
        CHECK(dlen > 0);
        CHECK(buf_contains(d, dlen, "\"framerate\":30,") ||
              buf_contains(d, dlen, "\"framerate\":30}"));
        CHECK(buf_contains(d, dlen, "\"framerate\":29.97"));
        CHECK(buf_contains(d, dlen, "\"framerate\":33.333"));

        moq_msf_catalog_t p;
        moq_bytes_t json = { moq_rcbuf_data(out), moq_rcbuf_len(out) };
        CHECK(moq_msf_catalog_parse(alloc, json, &p) == MOQ_OK);
        CHECK(p.tracks[0].framerate_millis == 30000);
        CHECK(p.tracks[1].framerate_millis == 29970);
        CHECK(p.tracks[2].framerate_millis == 33333);
        moq_msf_catalog_cleanup(alloc, &p);
        moq_rcbuf_decref(out);
    }

    /* -- invalid catalog inputs return INVAL -------------------------- */
    {
        moq_rcbuf_t *out = NULL;
        moq_msf_catalog_t cat;
        memset(&cat, 0, sizeof(cat));
        cat.version = MOQ_MSF_VERSION;

        /* NULL args. */
        CHECK(moq_msf_catalog_encode(NULL, &cat, &out) == MOQ_ERR_INVAL);
        CHECK(moq_msf_catalog_encode(alloc, NULL, &out) == MOQ_ERR_INVAL);
        CHECK(moq_msf_catalog_encode(alloc, &cat, NULL) == MOQ_ERR_INVAL);

        /* Wrong version. */
        cat.version = 99;
        CHECK(moq_msf_catalog_encode(alloc, &cat, &out) == MOQ_ERR_INVAL);
        CHECK(out == NULL);

        /* track_count > 0 but tracks == NULL. */
        cat.version = MOQ_MSF_VERSION;
        cat.track_count = 1;
        cat.tracks = NULL;
        CHECK(moq_msf_catalog_encode(alloc, &cat, &out) == MOQ_ERR_INVAL);

        /* String with data==NULL, len > 0. */
        moq_msf_track_t bad;
        memset(&bad, 0, sizeof(bad));
        bad.struct_size = sizeof(bad);
        bad.name = (moq_bytes_t){ NULL, 5 };
        bad.packaging = lit("loc");
        cat.tracks = &bad;
        cat.track_count = 1;
        CHECK(moq_msf_catalog_encode(alloc, &cat, &out) == MOQ_ERR_INVAL);
    }

    /* -- OOM during staging buffer allocation ------------------------- */
    {
        moq_msf_track_t t;
        memset(&t, 0, sizeof(t));
        t.struct_size = sizeof(t);
        t.name = lit("v");
        t.packaging = lit("loc");
        t.is_live = true;

        moq_msf_catalog_t cat;
        memset(&cat, 0, sizeof(cat));
        cat.struct_size = sizeof(cat);
        cat.version = MOQ_MSF_VERSION;
        cat.tracks = &t;
        cat.track_count = 1;

        /* Fail alloc #1 (staging buffer). */
        oom_state_t oom1 = { 0, 0, 1 };
        moq_alloc_t fa1 = oom_allocator(&oom1);
        moq_rcbuf_t *out = NULL;
        CHECK(moq_msf_catalog_encode(&fa1, &cat, &out) == MOQ_ERR_NOMEM);
        CHECK(out == NULL);
        CHECK(oom1.balance == 0);

        /* Fail alloc #2 (rcbuf after staging succeeds). */
        oom_state_t oom2 = { 0, 0, 2 };
        moq_alloc_t fa2 = oom_allocator(&oom2);
        out = NULL;
        CHECK(moq_msf_catalog_encode(&fa2, &cat, &out) == MOQ_ERR_NOMEM);
        CHECK(out == NULL);
        CHECK(oom2.balance == 0);
    }

    /* -- semantic round-trip: parse fixture, encode, parse again ------- */
    {
        const char *fixture =
            "{\"version\":1,\"generatedAt\":1746104606044,\"tracks\":["
            "{\"name\":\"1080p-video\","
            "\"namespace\":\"conference.example.com/conference123/alice\","
            "\"packaging\":\"loc\",\"isLive\":true,"
            "\"targetLatency\":2000,\"role\":\"video\","
            "\"renderGroup\":1,\"codec\":\"av01.0.08M.10.0.110.09\","
            "\"width\":1920,\"height\":1080,\"framerate\":30,\"bitrate\":1500000},"
            "{\"name\":\"audio\","
            "\"namespace\":\"conference.example.com/conference123/alice\","
            "\"packaging\":\"loc\",\"isLive\":true,"
            "\"targetLatency\":2000,\"role\":\"audio\","
            "\"codec\":\"opus\",\"samplerate\":48000,"
            "\"channelConfig\":\"2\",\"bitrate\":32000,\"renderGroup\":1}"
            "]}";

        moq_msf_catalog_t cat1;
        CHECK(moq_msf_catalog_parse(alloc, lit(fixture), &cat1) == MOQ_OK);

        moq_rcbuf_t *encoded = NULL;
        CHECK(moq_msf_catalog_encode(alloc, &cat1, &encoded) == MOQ_OK);

        moq_msf_catalog_t cat2;
        moq_bytes_t json = { moq_rcbuf_data(encoded), moq_rcbuf_len(encoded) };
        CHECK(moq_msf_catalog_parse(alloc, json, &cat2) == MOQ_OK);

        CHECK_EQ_SIZE(cat2.track_count, cat1.track_count);
        CHECK_EQ_U64(cat2.generated_at, cat1.generated_at);
        for (size_t i = 0; i < cat1.track_count; i++) {
            CHECK(bv_eq(cat2.tracks[i].name,
                (const char *)cat1.tracks[i].name.data) ||
                cat2.tracks[i].name.len == cat1.tracks[i].name.len);
            CHECK(cat2.tracks[i].has_width == cat1.tracks[i].has_width);
            if (cat1.tracks[i].has_width)
                CHECK(cat2.tracks[i].width == cat1.tracks[i].width);
        }

        moq_msf_catalog_cleanup(alloc, &cat2);
        moq_rcbuf_decref(encoded);
        moq_msf_catalog_cleanup(alloc, &cat1);
    }

    /* ================================================================ */
    /* Base64 decode tests                                              */
    /* ================================================================ */

    /* -- RFC 4648 test vectors ---------------------------------------- */
    {
        struct { const char *b64; const char *raw; size_t raw_len; } vecs[] = {
            { "",       "",       0 },
            { "Zg==",  "f",      1 },
            { "Zm8=",  "fo",     2 },
            { "Zm9v",  "foo",    3 },
            { "Zm9vYg==", "foob", 4 },
            { "Zm9vYmE=", "fooba", 5 },
            { "Zm9vYmFy", "foobar", 6 },
        };
        for (size_t i = 0; i < sizeof(vecs) / sizeof(vecs[0]); i++) {
            moq_rcbuf_t *out = NULL;
            moq_result_t rc = moq_msf_decode_init_data(alloc,
                lit(vecs[i].b64), &out);
            CHECK(rc == MOQ_OK);
            CHECK(out != NULL);
            CHECK_EQ_SIZE(moq_rcbuf_len(out), vecs[i].raw_len);
            if (vecs[i].raw_len > 0)
                CHECK(memcmp(moq_rcbuf_data(out), vecs[i].raw,
                              vecs[i].raw_len) == 0);
            moq_rcbuf_decref(out);
        }
    }

    /* -- decode invalid chars ----------------------------------------- */
    {
        moq_rcbuf_t *out = NULL;
        CHECK(moq_msf_decode_init_data(alloc, lit("Z@=="), &out) == MOQ_ERR_PROTO);
        CHECK(out == NULL);
    }

    /* -- decode bad length (not multiple of 4) ------------------------ */
    {
        moq_rcbuf_t *out = NULL;
        CHECK(moq_msf_decode_init_data(alloc, lit("Zg="), &out) == MOQ_ERR_PROTO);
    }

    /* -- decode padding in wrong position ----------------------------- */
    {
        moq_rcbuf_t *out = NULL;
        CHECK(moq_msf_decode_init_data(alloc, lit("Z==="), &out) == MOQ_ERR_PROTO);
    }

    /* -- decode data after padding ------------------------------------ */
    {
        moq_rcbuf_t *out = NULL;
        CHECK(moq_msf_decode_init_data(alloc, lit("Zg==Zg=="), &out) == MOQ_ERR_PROTO);
    }

    /* -- decode NULL args --------------------------------------------- */
    {
        moq_rcbuf_t *out = NULL;
        CHECK(moq_msf_decode_init_data(NULL, lit("Zg=="), &out) == MOQ_ERR_INVAL);
        CHECK(moq_msf_decode_init_data(alloc, lit("Zg=="), NULL) == MOQ_ERR_INVAL);
        moq_bytes_t bad = { NULL, 5 };
        CHECK(moq_msf_decode_init_data(alloc, bad, &out) == MOQ_ERR_INVAL);
    }

    /* -- decode non-canonical padding rejected ------------------------- */
    {
        moq_rcbuf_t *out = NULL;
        /* "Zn==" has non-zero unused bits in the last data char
         * before padding. 'n' = 39 = 0b100111, low 4 bits nonzero. */
        CHECK(moq_msf_decode_init_data(alloc, lit("Zn=="), &out) == MOQ_ERR_PROTO);
        CHECK(out == NULL);
    }

    /* -- decode OOM: fail temp alloc ---------------------------------- */
    {
        oom_state_t oom = { 0, 0, 1 };
        moq_alloc_t fa = oom_allocator(&oom);
        moq_rcbuf_t *out = NULL;
        CHECK(moq_msf_decode_init_data(&fa, lit("Zg=="), &out) == MOQ_ERR_NOMEM);
        CHECK(oom.balance == 0);
    }

    /* -- decode OOM: fail rcbuf alloc --------------------------------- */
    {
        oom_state_t oom = { 0, 0, 2 };
        moq_alloc_t fa = oom_allocator(&oom);
        moq_rcbuf_t *out = NULL;
        CHECK(moq_msf_decode_init_data(&fa, lit("Zg=="), &out) == MOQ_ERR_NOMEM);
        CHECK(oom.balance == 0);
    }

    /* ================================================================ */
    /* Base64 encode tests                                              */
    /* ================================================================ */

    /* -- RFC 4648 encode vectors -------------------------------------- */
    {
        struct { const char *raw; size_t raw_len; const char *b64; } vecs[] = {
            { "",       0, "" },
            { "f",      1, "Zg==" },
            { "fo",     2, "Zm8=" },
            { "foo",    3, "Zm9v" },
            { "foob",   4, "Zm9vYg==" },
            { "fooba",  5, "Zm9vYmE=" },
            { "foobar", 6, "Zm9vYmFy" },
        };
        for (size_t i = 0; i < sizeof(vecs) / sizeof(vecs[0]); i++) {
            moq_rcbuf_t *out = NULL;
            moq_bytes_t in = { (const uint8_t *)vecs[i].raw, vecs[i].raw_len };
            moq_result_t rc = moq_msf_encode_init_data(alloc, in, &out);
            CHECK(rc == MOQ_OK);
            CHECK(out != NULL);
            size_t b64_len = strlen(vecs[i].b64);
            CHECK_EQ_SIZE(moq_rcbuf_len(out), b64_len);
            if (b64_len > 0)
                CHECK(memcmp(moq_rcbuf_data(out), vecs[i].b64, b64_len) == 0);
            moq_rcbuf_decref(out);
        }
    }

    /* -- encode/decode round-trip ------------------------------------- */
    {
        uint8_t raw[] = { 0x01, 0x64, 0x00, 0x1E, 0xFF, 0x00, 0xAB };
        moq_rcbuf_t *encoded = NULL;
        CHECK(moq_msf_encode_init_data(alloc,
            (moq_bytes_t){ raw, sizeof(raw) }, &encoded) == MOQ_OK);

        moq_rcbuf_t *decoded = NULL;
        CHECK(moq_msf_decode_init_data(alloc,
            (moq_bytes_t){ moq_rcbuf_data(encoded), moq_rcbuf_len(encoded) },
            &decoded) == MOQ_OK);
        CHECK_EQ_SIZE(moq_rcbuf_len(decoded), sizeof(raw));
        CHECK(memcmp(moq_rcbuf_data(decoded), raw, sizeof(raw)) == 0);

        moq_rcbuf_decref(decoded);
        moq_rcbuf_decref(encoded);
    }

    /* -- encode OOM: fail temp alloc ---------------------------------- */
    {
        oom_state_t oom = { 0, 0, 1 };
        moq_alloc_t fa = oom_allocator(&oom);
        moq_rcbuf_t *out = NULL;
        CHECK(moq_msf_encode_init_data(&fa,
            (moq_bytes_t){ (const uint8_t *)"foo", 3 }, &out) == MOQ_ERR_NOMEM);
        CHECK(oom.balance == 0);
    }

    /* -- encode OOM: fail rcbuf alloc --------------------------------- */
    {
        oom_state_t oom = { 0, 0, 2 };
        moq_alloc_t fa = oom_allocator(&oom);
        moq_rcbuf_t *out = NULL;
        CHECK(moq_msf_encode_init_data(&fa,
            (moq_bytes_t){ (const uint8_t *)"foo", 3 }, &out) == MOQ_ERR_NOMEM);
        CHECK(oom.balance == 0);
    }

    /* -- catalog track name constant ----------------------------------- */
    {
        CHECK(MOQ_MSF_CATALOG_TRACK_NAME_LEN == 7);
        CHECK(memcmp(MOQ_MSF_CATALOG_TRACK_NAME, "catalog", 7) == 0);
        CHECK(strlen(MOQ_MSF_CATALOG_TRACK_NAME) == MOQ_MSF_CATALOG_TRACK_NAME_LEN);
    }

    /* ================================================================ */
    /* MSF-01 / CMSF: initDataList, initRef, eventType                   */
    /* ================================================================ */

    /* -- parse root initDataList + track initRef --------------------- */
    {
        const char *json =
            "{\"version\":1,\"tracks\":["
            "{\"name\":\"hd\",\"packaging\":\"cmaf\",\"isLive\":true,"
            "\"role\":\"video\",\"altGroup\":1,\"initRef\":\"init-hd\"}"
            "],\"initDataList\":["
            "{\"id\":\"init-hd\",\"type\":\"inline\",\"data\":\"AAAB\"}"
            "]}";
        moq_msf_catalog_t cat;
        CHECK(moq_msf_catalog_parse(alloc, lit(json), &cat) == MOQ_OK);
        CHECK_EQ_SIZE(cat.track_count, 1);
        CHECK(cat.tracks[0].has_init_ref);
        CHECK(bv_eq(cat.tracks[0].init_ref, "init-hd"));
        CHECK(bv_eq(cat.tracks[0].packaging, "cmaf"));
        CHECK(cat.tracks[0].has_alt_group && cat.tracks[0].alt_group == 1);
        CHECK_EQ_SIZE(cat.init_data_count, 1);
        CHECK(bv_eq(cat.init_data_list[0].id, "init-hd"));
        CHECK(bv_eq(cat.init_data_list[0].type, "inline"));
        CHECK(bv_eq(cat.init_data_list[0].data, "AAAB"));

        /* resolution helper */
        const moq_msf_init_data_entry_t *e =
            moq_msf_catalog_find_init_data(&cat, cat.tracks[0].init_ref);
        CHECK(e != NULL && bv_eq(e->data, "AAAB"));
        CHECK(moq_msf_catalog_find_init_data(&cat, lit("nope")) == NULL);

        moq_msf_catalog_cleanup(alloc, &cat);
    }

    /* -- malformed initDataList entry (missing data) -> PROTO --------- */
    {
        const char *json =
            "{\"version\":1,\"tracks\":["
            "{\"name\":\"v\",\"packaging\":\"cmaf\",\"isLive\":true}"
            "],\"initDataList\":[{\"id\":\"x\",\"type\":\"inline\"}]}";
        moq_msf_catalog_t cat;
        CHECK(moq_msf_catalog_parse(alloc, lit(json), &cat) == MOQ_ERR_PROTO);
    }

    /* -- encode initDataList + initRef, then re-parse ---------------- */
    {
        moq_msf_track_t tr;
        memset(&tr, 0, sizeof(tr));
        tr.struct_size = sizeof(tr);
        tr.name = lit("hd");
        tr.packaging = lit("cmaf");
        tr.is_live = true;
        tr.has_alt_group = true; tr.alt_group = 1;
        tr.has_init_ref = true; tr.init_ref = lit("init-hd");

        moq_msf_init_data_entry_t idl;
        idl.id = lit("init-hd");
        idl.type = lit("inline");
        idl.data = lit("AAAB");

        moq_msf_catalog_t cat;
        memset(&cat, 0, sizeof(cat));
        cat.struct_size = sizeof(cat);
        cat.version = MOQ_MSF_VERSION;
        cat.tracks = &tr;
        cat.track_count = 1;
        cat.init_data_list = &idl;
        cat.init_data_count = 1;

        moq_rcbuf_t *enc = NULL;
        CHECK(moq_msf_catalog_encode(alloc, &cat, &enc) == MOQ_OK);
        moq_bytes_t out = { moq_rcbuf_data(enc), moq_rcbuf_len(enc) };

        moq_msf_catalog_t back;
        CHECK(moq_msf_catalog_parse(alloc, out, &back) == MOQ_OK);
        CHECK_EQ_SIZE(back.track_count, 1);
        CHECK(back.tracks[0].has_init_ref &&
              bv_eq(back.tracks[0].init_ref, "init-hd"));
        CHECK_EQ_SIZE(back.init_data_count, 1);
        CHECK(bv_eq(back.init_data_list[0].id, "init-hd"));
        CHECK(bv_eq(back.init_data_list[0].data, "AAAB"));

        moq_msf_catalog_cleanup(alloc, &back);
        moq_rcbuf_decref(enc);
    }

    /* -- CMSF-style round trip: cmaf + altGroup + initRef ------------ */
    {
        const char *json =
            "{\"version\":1,\"tracks\":["
            "{\"name\":\"hd\",\"packaging\":\"cmaf\",\"isLive\":true,"
            "\"role\":\"video\",\"altGroup\":1,\"initRef\":\"init-hd\","
            "\"codec\":\"avc1.640028\",\"width\":1920,\"height\":1080},"
            "{\"name\":\"sd\",\"packaging\":\"cmaf\",\"isLive\":true,"
            "\"role\":\"video\",\"altGroup\":1,\"initRef\":\"init-sd\","
            "\"codec\":\"avc1.64000d\",\"width\":192,\"height\":144}"
            "],\"initDataList\":["
            "{\"id\":\"init-hd\",\"type\":\"inline\",\"data\":\"AAAB\"},"
            "{\"id\":\"init-sd\",\"type\":\"inline\",\"data\":\"AAAC\"}"
            "]}";
        moq_msf_catalog_t c1;
        CHECK(moq_msf_catalog_parse(alloc, lit(json), &c1) == MOQ_OK);

        moq_rcbuf_t *enc = NULL;
        CHECK(moq_msf_catalog_encode(alloc, &c1, &enc) == MOQ_OK);
        moq_bytes_t out = { moq_rcbuf_data(enc), moq_rcbuf_len(enc) };

        moq_msf_catalog_t c2;
        CHECK(moq_msf_catalog_parse(alloc, out, &c2) == MOQ_OK);
        CHECK_EQ_SIZE(c2.track_count, c1.track_count);
        CHECK_EQ_SIZE(c2.init_data_count, c1.init_data_count);
        for (size_t i = 0; i < c2.track_count; i++) {
            CHECK(bv_eq(c2.tracks[i].packaging, "cmaf"));
            CHECK(c2.tracks[i].has_init_ref);
            const moq_msf_init_data_entry_t *e =
                moq_msf_catalog_find_init_data(&c2, c2.tracks[i].init_ref);
            CHECK(e != NULL);
        }

        moq_msf_catalog_cleanup(alloc, &c2);
        moq_rcbuf_decref(enc);
        moq_msf_catalog_cleanup(alloc, &c1);
    }

    /* -- eventtimeline track + eventType: round trip ----------------- *
     * A complete eventtimeline track per MSF §8.2: eventType + mimeType
     * "application/json" + a non-empty depends list. */
    {
        const char *json =
            "{\"version\":1,\"tracks\":["
            "{\"name\":\"sap\",\"packaging\":\"eventtimeline\",\"isLive\":true,"
            "\"eventType\":\"org.ietf.moq.cmsf.sap\","
            "\"mimeType\":\"application/json\",\"depends\":[\"video\"]}"
            "]}";
        moq_msf_catalog_t cat;
        CHECK(moq_msf_catalog_parse(alloc, lit(json), &cat) == MOQ_OK);
        CHECK(bv_eq(cat.tracks[0].packaging, "eventtimeline"));
        CHECK(cat.tracks[0].has_event_type);
        CHECK(bv_eq(cat.tracks[0].event_type, "org.ietf.moq.cmsf.sap"));

        moq_rcbuf_t *enc = NULL;
        CHECK(moq_msf_catalog_encode(alloc, &cat, &enc) == MOQ_OK);
        moq_bytes_t out = { moq_rcbuf_data(enc), moq_rcbuf_len(enc) };
        moq_msf_catalog_t back;
        CHECK(moq_msf_catalog_parse(alloc, out, &back) == MOQ_OK);
        CHECK(back.tracks[0].has_event_type &&
              bv_eq(back.tracks[0].event_type, "org.ietf.moq.cmsf.sap"));

        moq_msf_catalog_cleanup(alloc, &back);
        moq_rcbuf_decref(enc);
        moq_msf_catalog_cleanup(alloc, &cat);
    }

    /* -- eventtimeline track: depends (5.2.14) + mimeType (5.2.19) round trip */
    {
        const char *json =
            "{\"version\":1,\"tracks\":["
            "{\"name\":\"sap\",\"packaging\":\"eventtimeline\",\"isLive\":true,"
            "\"eventType\":\"org.ietf.moq.cmsf.sap\","
            "\"mimeType\":\"application/json\","
            "\"depends\":[\"video\",\"audio\"]}"
            "]}";
        moq_msf_catalog_t cat;
        CHECK(moq_msf_catalog_parse(alloc, lit(json), &cat) == MOQ_OK);
        CHECK(cat.tracks[0].has_mime_type &&
              bv_eq(cat.tracks[0].mime_type, "application/json"));
        CHECK(cat.tracks[0].depends_count == 2);
        CHECK(bv_eq(cat.tracks[0].depends[0], "video"));
        CHECK(bv_eq(cat.tracks[0].depends[1], "audio"));

        moq_rcbuf_t *enc = NULL;
        CHECK(moq_msf_catalog_encode(alloc, &cat, &enc) == MOQ_OK);
        moq_bytes_t out = { moq_rcbuf_data(enc), moq_rcbuf_len(enc) };
        /* Encode emits the canonical "mimeType" key. */
        bool canonical_mime = false;
        for (size_t i = 0; i + 10 <= out.len; i++)
            if (memcmp(out.data + i, "\"mimeType\"", 10) == 0) {
                canonical_mime = true; break;
            }
        CHECK(canonical_mime);

        moq_msf_catalog_t back;
        CHECK(moq_msf_catalog_parse(alloc, out, &back) == MOQ_OK);
        CHECK(back.tracks[0].has_mime_type &&
              bv_eq(back.tracks[0].mime_type, "application/json"));
        CHECK(back.tracks[0].depends_count == 2 &&
              bv_eq(back.tracks[0].depends[0], "video") &&
              bv_eq(back.tracks[0].depends[1], "audio"));

        moq_msf_catalog_cleanup(alloc, &back);
        moq_rcbuf_decref(enc);
        moq_msf_catalog_cleanup(alloc, &cat);
    }

    /* -- mimeType parsed leniently from lowercase "mimetype" -------------- */
    {
        const char *json =
            "{\"version\":1,\"tracks\":["
            "{\"name\":\"sap\",\"packaging\":\"eventtimeline\",\"isLive\":true,"
            "\"eventType\":\"x\",\"mimetype\":\"application/json\"}"
            "]}";
        moq_msf_catalog_t cat;
        CHECK(moq_msf_catalog_parse(alloc, lit(json), &cat) == MOQ_OK);
        CHECK(cat.tracks[0].has_mime_type &&
              bv_eq(cat.tracks[0].mime_type, "application/json"));
        moq_msf_catalog_cleanup(alloc, &cat);
    }

    /* -- eventType validity rule (MSF 5.2.5) on encode --------------- */
    {
        /* eventType on a non-eventtimeline track -> INVAL */
        moq_msf_track_t tr;
        memset(&tr, 0, sizeof(tr));
        tr.struct_size = sizeof(tr);
        tr.name = lit("v"); tr.packaging = lit("cmaf"); tr.is_live = true;
        tr.has_event_type = true; tr.event_type = lit("x");
        moq_msf_catalog_t cat;
        memset(&cat, 0, sizeof(cat));
        cat.struct_size = sizeof(cat);
        cat.version = MOQ_MSF_VERSION;
        cat.tracks = &tr; cat.track_count = 1;
        moq_rcbuf_t *enc = NULL;
        CHECK(moq_msf_catalog_encode(alloc, &cat, &enc) == MOQ_ERR_INVAL);
        CHECK(enc == NULL);

        /* eventtimeline track WITHOUT eventType -> INVAL.
         * Carry valid mimeType + depends so the only defect is the missing
         * eventType (isolates the §5.2.5 rule from the §8.2 checks). */
        moq_bytes_t deps_v = lit("video");
        tr.has_event_type = false;
        tr.packaging = lit("eventtimeline");
        tr.has_mime_type = true; tr.mime_type = lit("application/json");
        tr.depends = &deps_v; tr.depends_count = 1;
        CHECK(moq_msf_catalog_encode(alloc, &cat, &enc) == MOQ_ERR_INVAL);
        CHECK(enc == NULL);
    }

    /* -- MSF §8.2 eventtimeline requirements + span coherence on encode -- */
    {
        moq_bytes_t deps_v = lit("video");
        moq_msf_track_t tr;
        moq_msf_catalog_t cat;
        moq_rcbuf_t *enc;

        /* Build a complete, valid eventtimeline track template, then mutate
         * one field per case to assert each requirement is enforced. */
        #define ET_RESET()                                                  \
            do {                                                            \
                memset(&tr, 0, sizeof(tr));                                 \
                tr.struct_size = sizeof(tr);                                \
                tr.name = lit("sap"); tr.packaging = lit("eventtimeline");  \
                tr.is_live = true;                                          \
                tr.has_event_type = true;                                   \
                tr.event_type = lit("org.ietf.moq.cmsf.sap");               \
                tr.has_mime_type = true;                                    \
                tr.mime_type = lit("application/json");                     \
                tr.depends = &deps_v; tr.depends_count = 1;                 \
                memset(&cat, 0, sizeof(cat));                               \
                cat.struct_size = sizeof(cat);                             \
                cat.version = MOQ_MSF_VERSION;                              \
                cat.tracks = &tr; cat.track_count = 1;                      \
                enc = NULL;                                                 \
            } while (0)

        /* Baseline: the template encodes cleanly. */
        ET_RESET();
        CHECK(moq_msf_catalog_encode(alloc, &cat, &enc) == MOQ_OK);
        moq_rcbuf_decref(enc); enc = NULL;

        /* §8.2: eventtimeline missing mimeType -> INVAL. */
        ET_RESET();
        tr.has_mime_type = false; tr.mime_type = (moq_bytes_t){0};
        CHECK(moq_msf_catalog_encode(alloc, &cat, &enc) == MOQ_ERR_INVAL);
        CHECK(enc == NULL);

        /* §8.2: eventtimeline mimeType != "application/json" -> INVAL. */
        ET_RESET();
        tr.mime_type = lit("text/plain");
        CHECK(moq_msf_catalog_encode(alloc, &cat, &enc) == MOQ_ERR_INVAL);
        CHECK(enc == NULL);

        /* §8.2: eventtimeline with empty depends list -> INVAL. */
        ET_RESET();
        tr.depends = NULL; tr.depends_count = 0;
        CHECK(moq_msf_catalog_encode(alloc, &cat, &enc) == MOQ_ERR_INVAL);
        CHECK(enc == NULL);

        /* Span coherence: depends_count > 0 but depends == NULL -> INVAL,
         * not a crash. */
        ET_RESET();
        tr.depends = NULL; tr.depends_count = 1;
        CHECK(moq_msf_catalog_encode(alloc, &cat, &enc) == MOQ_ERR_INVAL);
        CHECK(enc == NULL);

        /* Span coherence: a malformed depends entry (data==NULL, len>0)
         * -> INVAL. */
        ET_RESET();
        moq_bytes_t bad_dep = { NULL, 3 };
        tr.depends = &bad_dep; tr.depends_count = 1;
        CHECK(moq_msf_catalog_encode(alloc, &cat, &enc) == MOQ_ERR_INVAL);
        CHECK(enc == NULL);

        /* Span coherence: an empty depends entry (len==0) -> INVAL. */
        ET_RESET();
        moq_bytes_t empty_dep = lit("");
        tr.depends = &empty_dep; tr.depends_count = 1;
        CHECK(moq_msf_catalog_encode(alloc, &cat, &enc) == MOQ_ERR_INVAL);
        CHECK(enc == NULL);

        /* Span coherence: a malformed mimeType (data==NULL, len>0) -> INVAL. */
        ET_RESET();
        tr.mime_type = (moq_bytes_t){ NULL, 3 };
        CHECK(moq_msf_catalog_encode(alloc, &cat, &enc) == MOQ_ERR_INVAL);
        CHECK(enc == NULL);

        /* Non-eventtimeline track may carry mimeType + depends as ordinary
         * optional metadata (no §8.2 constraints). */
        ET_RESET();
        tr.packaging = lit("cmaf");
        tr.has_event_type = false; tr.event_type = (moq_bytes_t){0};
        tr.mime_type = lit("video/mp4");
        CHECK(moq_msf_catalog_encode(alloc, &cat, &enc) == MOQ_OK);
        moq_rcbuf_decref(enc); enc = NULL;

        #undef ET_RESET
    }

    /* -- version: MSF-01 string "1" + legacy numeric; unsupported ---- */
    {
        moq_msf_catalog_t cat;
        CHECK(moq_msf_catalog_parse(alloc, lit(
            "{\"version\":\"1\",\"tracks\":["
            "{\"name\":\"v\",\"packaging\":\"loc\",\"isLive\":true}]}"),
            &cat) == MOQ_OK);
        CHECK(cat.version == MOQ_MSF_VERSION);
        moq_msf_catalog_cleanup(alloc, &cat);

        CHECK(moq_msf_catalog_parse(alloc, lit(
            "{\"version\":1,\"tracks\":["
            "{\"name\":\"v\",\"packaging\":\"loc\",\"isLive\":true}]}"),
            &cat) == MOQ_OK);
        moq_msf_catalog_cleanup(alloc, &cat);

        CHECK(moq_msf_catalog_parse(alloc, lit(
            "{\"version\":\"2\",\"tracks\":["
            "{\"name\":\"v\",\"packaging\":\"loc\",\"isLive\":true}]}"),
            &cat) == MOQ_ERR_PROTO);
    }

    /* -- encode emits the MSF-01 string version --------------------- */
    {
        moq_msf_track_t tr;
        memset(&tr, 0, sizeof(tr)); tr.struct_size = sizeof(tr);
        tr.name = lit("v"); tr.packaging = lit("loc"); tr.is_live = true;
        moq_msf_catalog_t cat;
        memset(&cat, 0, sizeof(cat)); cat.struct_size = sizeof(cat);
        cat.version = MOQ_MSF_VERSION; cat.tracks = &tr; cat.track_count = 1;

        moq_rcbuf_t *enc = NULL;
        CHECK(moq_msf_catalog_encode(alloc, &cat, &enc) == MOQ_OK);
        moq_bytes_t out = { moq_rcbuf_data(enc), moq_rcbuf_len(enc) };
        CHECK(contains_bytes(out, "\"version\":\"draft-01\""));
        moq_rcbuf_decref(enc);   /* caller-built catalog: no cleanup */
    }

    /* -- initDataList present but not an array -> PROTO -------------- */
    {
        moq_msf_catalog_t cat;
        CHECK(moq_msf_catalog_parse(alloc, lit(
            "{\"version\":\"1\",\"tracks\":["
            "{\"name\":\"v\",\"packaging\":\"cmaf\",\"isLive\":true}],"
            "\"initDataList\":{}}"), &cat) == MOQ_ERR_PROTO);
        CHECK(moq_msf_catalog_parse(alloc, lit(
            "{\"version\":\"1\",\"tracks\":["
            "{\"name\":\"v\",\"packaging\":\"cmaf\",\"isLive\":true}],"
            "\"initDataList\":\"x\"}"), &cat) == MOQ_ERR_PROTO);
    }

    /* -- contentProtections + nested drmSystem: parse/resolve are lenient,
     * but the public encoder rejects the non-UUID defaultKIDs (asymmetry) -- */
    {
        const char *json =
            "{\"version\":\"1\","
            "\"contentProtections\":[{"
            "\"refID\":\"cp1\","
            "\"defaultKID\":[\"kid-a\",\"kid-b\"],"
            "\"scheme\":\"cbcs\","
            "\"drmSystem\":{"
            "\"systemID\":\"edef8ba9-79d6-4ace-a3c8-27dcd51d21ed\","
            "\"laURL\":{\"url\":\"https://la.example/x\",\"type\":\"EME-1.0\"},"
            "\"certURL\":{\"url\":\"https://cert.example/y\"},"
            "\"pssh\":\"AAAB\",\"robustness\":\"HW_SECURE_ALL\"}}],"
            "\"tracks\":[{\"name\":\"v\",\"packaging\":\"cmaf\",\"isLive\":true,"
            "\"contentProtectionRefIDs\":[\"cp1\"]}]}";
        moq_msf_catalog_t cat;
        CHECK(moq_msf_catalog_parse(alloc, lit(json), &cat) == MOQ_OK);
        CHECK_EQ_SIZE(cat.content_protection_count, 1);
        const moq_cmsf_content_protection_t *cp = &cat.content_protections[0];
        CHECK(bv_eq(cp->ref_id, "cp1"));
        CHECK(bv_eq(cp->scheme, "cbcs"));
        CHECK_EQ_SIZE(cp->default_kid_count, 2);
        CHECK(bv_eq(cp->default_kids[0], "kid-a"));
        CHECK(bv_eq(cp->default_kids[1], "kid-b"));
        CHECK(bv_eq(cp->drm_system.system_id,
                    "edef8ba9-79d6-4ace-a3c8-27dcd51d21ed"));
        CHECK(cp->drm_system.la_url.present &&
              bv_eq(cp->drm_system.la_url.url, "https://la.example/x"));
        CHECK(cp->drm_system.la_url.has_type &&
              bv_eq(cp->drm_system.la_url.type, "EME-1.0"));
        CHECK(cp->drm_system.cert_url.present &&
              !cp->drm_system.cert_url.has_type);
        CHECK(cp->drm_system.has_pssh && bv_eq(cp->drm_system.pssh, "AAAB"));
        CHECK(cp->drm_system.has_robustness &&
              bv_eq(cp->drm_system.robustness, "HW_SECURE_ALL"));

        CHECK_EQ_SIZE(cat.tracks[0].cp_ref_id_count, 1);
        CHECK(bv_eq(cat.tracks[0].cp_ref_ids[0], "cp1"));
        const moq_cmsf_content_protection_t *r =
            moq_msf_catalog_find_content_protection(&cat,
                cat.tracks[0].cp_ref_ids[0]);
        CHECK(r == cp);
        CHECK(moq_msf_catalog_find_content_protection(&cat, lit("nope")) == NULL);

        /* Parse is lenient -- it accepted the non-UUID defaultKIDs above -- but
         * the public encoder enforces CMSF §4 authoring syntax and REJECTS them.
         * (The receive path keeps such values via apply_delta's lenient
         * re-encode; authoring strictness is verified in the next block.) */
        moq_rcbuf_t *enc = NULL;
        CHECK(moq_msf_catalog_encode(alloc, &cat, &enc) == MOQ_ERR_INVAL);
        CHECK(enc == NULL);
        moq_msf_catalog_cleanup(alloc, &cat);
    }

    /* -- CMSF §4 contentProtection authoring syntax on encode ---------- *
     * Public encode enforces scheme enum (§4.1.1.3), UUID systemID/defaultKID
     * (§4.1.1.4.1/§4.1.1.2), and unique refIDs (§4.1.1.1). Parse stays lenient
     * (asserted in the block above). Struct-built catalogs. */
    {
        moq_bytes_t kid = lit("01234567-89ab-cdef-0123-456789abcdef");
        moq_cmsf_content_protection_t cp;
        moq_msf_track_t tr;
        moq_msf_catalog_t cat;
        moq_rcbuf_t *enc;
        #define CP_RESET()                                                  \
            do {                                                            \
                memset(&cp, 0, sizeof(cp));                                 \
                cp.ref_id = lit("cp1");                                     \
                cp.scheme = lit("cenc");                                    \
                cp.default_kids = &kid; cp.default_kid_count = 1;           \
                cp.drm_system.system_id =                                   \
                    lit("edef8ba9-79d6-4ace-a3c8-27dcd51d21ed");           \
                memset(&tr, 0, sizeof(tr)); tr.struct_size = sizeof(tr);    \
                tr.name = lit("v"); tr.packaging = lit("cmaf");             \
                tr.is_live = true;                                         \
                memset(&cat, 0, sizeof(cat)); cat.struct_size = sizeof(cat);\
                cat.version = MOQ_MSF_VERSION;                              \
                cat.tracks = &tr; cat.track_count = 1;                      \
                cat.content_protections = &cp;                             \
                cat.content_protection_count = 1;                          \
                enc = NULL;                                                 \
            } while (0)

        /* Baseline: a valid CP encodes and round-trips. */
        CP_RESET();
        CHECK(moq_msf_catalog_encode(alloc, &cat, &enc) == MOQ_OK);
        CHECK(enc != NULL);
        {
            moq_bytes_t out = { moq_rcbuf_data(enc), moq_rcbuf_len(enc) };
            moq_msf_catalog_t back;
            CHECK(moq_msf_catalog_parse(alloc, out, &back) == MOQ_OK);
            CHECK_EQ_SIZE(back.content_protection_count, 1);
            moq_msf_catalog_cleanup(alloc, &back);
        }
        moq_rcbuf_decref(enc);

        /* scheme not in the CENC enum -> INVAL (§4.1.1.3). */
        CP_RESET(); cp.scheme = lit("aes-128");
        CHECK(moq_msf_catalog_encode(alloc, &cat, &enc) == MOQ_ERR_INVAL);
        CHECK(enc == NULL);

        /* systemID not a UUID -> INVAL (§4.1.1.4.1). */
        CP_RESET(); cp.drm_system.system_id = lit("not-a-uuid");
        CHECK(moq_msf_catalog_encode(alloc, &cat, &enc) == MOQ_ERR_INVAL);
        CHECK(enc == NULL);

        /* a defaultKID that is not a UUID (16 hex, no hyphens) -> INVAL. */
        CP_RESET();
        moq_bytes_t bad_kid = lit("0123456789abcdef");
        cp.default_kids = &bad_kid;
        CHECK(moq_msf_catalog_encode(alloc, &cat, &enc) == MOQ_ERR_INVAL);
        CHECK(enc == NULL);

        /* UPPERCASE-hex UUIDs accepted (UUID syntax is case-insensitive). */
        CP_RESET();
        moq_bytes_t uc_kid = lit("01234567-89AB-CDEF-0123-456789ABCDEF");
        cp.default_kids = &uc_kid;
        cp.drm_system.system_id = lit("EDEF8BA9-79D6-4ACE-A3C8-27DCD51D21ED");
        CHECK(moq_msf_catalog_encode(alloc, &cat, &enc) == MOQ_OK);
        CHECK(enc != NULL);
        moq_rcbuf_decref(enc);

        /* duplicate refID across the array -> INVAL (§4.1.1.1). */
        CP_RESET();
        moq_cmsf_content_protection_t cps[2];
        cps[0] = cp; cps[1] = cp;        /* identical refID "cp1" */
        cat.content_protections = cps; cat.content_protection_count = 2;
        CHECK(moq_msf_catalog_encode(alloc, &cat, &enc) == MOQ_ERR_INVAL);
        CHECK(enc == NULL);

        /* CMSF §4.1.1.4.5: pssh is a Base64-encoded PSSH box. */
        /* valid Base64 pssh -> OK. */
        CP_RESET();
        cp.drm_system.has_pssh = true;
        cp.drm_system.pssh = lit("AAAAAQ==");   /* well-formed Base64 */
        CHECK(moq_msf_catalog_encode(alloc, &cat, &enc) == MOQ_OK);
        CHECK(enc != NULL);
        moq_rcbuf_decref(enc);

        /* pssh that is not valid Base64 -> INVAL. */
        CP_RESET();
        cp.drm_system.has_pssh = true;
        cp.drm_system.pssh = lit("not!base64");   /* bad alphabet + length */
        CHECK(moq_msf_catalog_encode(alloc, &cat, &enc) == MOQ_ERR_INVAL);
        CHECK(enc == NULL);

        /* present-but-empty pssh -> INVAL (a PSSH box is never empty). */
        CP_RESET();
        cp.drm_system.has_pssh = true;
        cp.drm_system.pssh = (moq_bytes_t){ NULL, 0 };
        CHECK(moq_msf_catalog_encode(alloc, &cat, &enc) == MOQ_ERR_INVAL);
        CHECK(enc == NULL);
        #undef CP_RESET
    }

    /* -- catalog uniqueness on strict authoring encode -------------------
     * MSF §5.2.3: a track name is unique per namespace; §5.1.7/CMSF §3.1: an
     * initDataList id is unique within the catalog. Struct-built catalogs;
     * parse stays lenient (not asserted strict here). */
    {
        moq_msf_track_t tr[2];
        moq_msf_catalog_t cat;
        moq_rcbuf_t *enc;
        #define TR(i, nm) do {                                              \
                memset(&tr[i], 0, sizeof(tr[i]));                           \
                tr[i].struct_size = sizeof(tr[i]);                          \
                tr[i].name = lit(nm); tr[i].packaging = lit("loc");         \
                tr[i].is_live = true;                                       \
            } while (0)
        #define ENC() (cat.tracks = tr, cat.track_count = 2, enc = NULL,    \
                       moq_msf_catalog_encode(alloc, &cat, &enc))

        memset(&cat, 0, sizeof(cat)); cat.struct_size = sizeof(cat);
        cat.version = MOQ_MSF_VERSION;

        /* duplicate (name, absent namespace) -> INVAL. */
        TR(0, "v"); TR(1, "v");
        CHECK(ENC() == MOQ_ERR_INVAL);
        CHECK(enc == NULL);

        /* same name in DIFFERENT namespaces -> OK (unique per namespace). */
        TR(0, "v"); TR(1, "v");
        tr[0].has_namespace = true; tr[0].namespace_ = lit("ns-a");
        tr[1].has_namespace = true; tr[1].namespace_ = lit("ns-b");
        CHECK(ENC() == MOQ_OK);
        CHECK(enc != NULL);
        moq_rcbuf_decref(enc);

        /* same name, SAME namespace -> INVAL. */
        TR(0, "v"); TR(1, "v");
        tr[0].has_namespace = true; tr[0].namespace_ = lit("ns-a");
        tr[1].has_namespace = true; tr[1].namespace_ = lit("ns-a");
        CHECK(ENC() == MOQ_ERR_INVAL);
        CHECK(enc == NULL);
        #undef TR
        #undef ENC

        /* duplicate initDataList id -> INVAL; making them unique -> OK. */
        moq_msf_track_t one;
        memset(&one, 0, sizeof(one)); one.struct_size = sizeof(one);
        one.name = lit("v"); one.packaging = lit("cmaf"); one.is_live = true;
        moq_msf_init_data_entry_t idl[2];
        memset(idl, 0, sizeof(idl));
        idl[0].id = lit("init0"); idl[0].type = lit("cmsf"); idl[0].data = lit("AAAB");
        idl[1].id = lit("init0"); idl[1].type = lit("cmsf"); idl[1].data = lit("BBBC");
        memset(&cat, 0, sizeof(cat)); cat.struct_size = sizeof(cat);
        cat.version = MOQ_MSF_VERSION; cat.tracks = &one; cat.track_count = 1;
        cat.init_data_list = idl; cat.init_data_count = 2;
        enc = NULL;
        CHECK(moq_msf_catalog_encode(alloc, &cat, &enc) == MOQ_ERR_INVAL);
        CHECK(enc == NULL);

        idl[1].id = lit("init1");        /* now unique */
        enc = NULL;
        CHECK(moq_msf_catalog_encode(alloc, &cat, &enc) == MOQ_OK);
        CHECK(enc != NULL);
        moq_rcbuf_decref(enc);
    }

    /* -- malformed contentProtections (drmSystem w/o systemID) -> PROTO */
    {
        moq_msf_catalog_t cat;
        CHECK(moq_msf_catalog_parse(alloc, lit(
            "{\"version\":\"1\",\"contentProtections\":[{"
            "\"refID\":\"x\",\"defaultKID\":[\"k\"],\"scheme\":\"cenc\","
            "\"drmSystem\":{}}],"
            "\"tracks\":[{\"name\":\"v\",\"packaging\":\"cmaf\",\"isLive\":true}]}"),
            &cat) == MOQ_ERR_PROTO);
        /* present but not an array */
        CHECK(moq_msf_catalog_parse(alloc, lit(
            "{\"version\":\"1\",\"contentProtections\":{},"
            "\"tracks\":[{\"name\":\"v\",\"packaging\":\"cmaf\",\"isLive\":true}]}"),
            &cat) == MOQ_ERR_PROTO);
    }

    /* -- maxGrp/ObjSapStartingType: parse + encode ------------------- */
    {
        moq_msf_catalog_t cat;
        CHECK(moq_msf_catalog_parse(alloc, lit(
            "{\"version\":\"1\",\"tracks\":[{\"name\":\"v\",\"packaging\":\"cmaf\","
            "\"isLive\":true,\"maxGrpSapStartingType\":2,"
            "\"maxObjSapStartingType\":3}]}"), &cat) == MOQ_OK);
        CHECK(cat.tracks[0].has_max_grp_sap && cat.tracks[0].max_grp_sap == 2);
        CHECK(cat.tracks[0].has_max_obj_sap && cat.tracks[0].max_obj_sap == 3);

        moq_rcbuf_t *enc = NULL;
        CHECK(moq_msf_catalog_encode(alloc, &cat, &enc) == MOQ_OK);
        moq_bytes_t out = { moq_rcbuf_data(enc), moq_rcbuf_len(enc) };
        CHECK(contains_bytes(out, "\"maxGrpSapStartingType\":2"));
        CHECK(contains_bytes(out, "\"maxObjSapStartingType\":3"));
        moq_msf_catalog_t back;
        CHECK(moq_msf_catalog_parse(alloc, out, &back) == MOQ_OK);
        CHECK(back.tracks[0].max_grp_sap == 2 && back.tracks[0].max_obj_sap == 3);
        moq_msf_catalog_cleanup(alloc, &back);
        moq_rcbuf_decref(enc);
        moq_msf_catalog_cleanup(alloc, &cat);
    }

    /* -- unknown fields (root / CP / drmSystem / track) ignored ------ */
    {
        moq_msf_catalog_t cat;
        CHECK(moq_msf_catalog_parse(alloc, lit(
            "{\"version\":\"1\",\"futureRoot\":42,"
            "\"contentProtections\":[{\"refID\":\"x\",\"defaultKID\":[\"k\"],"
            "\"scheme\":\"cenc\",\"drmSystem\":{\"systemID\":\"s\",\"futureDrm\":1},"
            "\"futureCp\":true}],"
            "\"tracks\":[{\"name\":\"v\",\"packaging\":\"cmaf\",\"isLive\":true,"
            "\"futureTrack\":\"ignored\",\"contentProtectionRefIDs\":[\"x\"]}]}"),
            &cat) == MOQ_OK);
        CHECK_EQ_SIZE(cat.content_protection_count, 1);
        CHECK(bv_eq(cat.content_protections[0].ref_id, "x"));
        CHECK_EQ_SIZE(cat.tracks[0].cp_ref_id_count, 1);
        moq_msf_catalog_cleanup(alloc, &cat);
    }

    /* -- leak balance: full CMSF catalog parse + cleanup ------------- */
    {
        oom_state_t os = { 0, 0, 0 };   /* fail_at = 0 -> never fail */
        moq_alloc_t oa = oom_allocator(&os);
        moq_msf_catalog_t cat;
        CHECK(moq_msf_catalog_parse(&oa, lit(
            "{\"version\":\"1\","
            "\"contentProtections\":[{\"refID\":\"1\",\"defaultKID\":[\"a\",\"b\"],"
            "\"scheme\":\"cbcs\",\"drmSystem\":{\"systemID\":\"s\","
            "\"laURL\":{\"url\":\"u\"}}}],"
            "\"tracks\":[{\"name\":\"v\",\"packaging\":\"cmaf\",\"isLive\":true,"
            "\"initRef\":\"i1\",\"contentProtectionRefIDs\":[\"1\"]}],"
            "\"initDataList\":[{\"id\":\"i1\",\"type\":\"inline\",\"data\":\"AAAB\"}]}"),
            &cat) == MOQ_OK);
        moq_msf_catalog_cleanup(&oa, &cat);
        CHECK(os.balance == 0);   /* every allocation freed */
    }

    /* -- leak balance: malformed CP mid-array frees partial allocs --- */
    {
        oom_state_t os = { 0, 0, 0 };
        moq_alloc_t oa = oom_allocator(&os);
        moq_msf_catalog_t cat;
        /* entry 1 valid (allocs default_kids); entry 2 has no systemID -> PROTO */
        CHECK(moq_msf_catalog_parse(&oa, lit(
            "{\"version\":\"1\",\"contentProtections\":["
            "{\"refID\":\"1\",\"defaultKID\":[\"a\"],\"scheme\":\"cenc\","
            "\"drmSystem\":{\"systemID\":\"s\"}},"
            "{\"refID\":\"2\",\"defaultKID\":[\"b\"],\"scheme\":\"cenc\","
            "\"drmSystem\":{}}],"
            "\"tracks\":[{\"name\":\"v\",\"packaging\":\"cmaf\",\"isLive\":true}]}"),
            &cat) == MOQ_ERR_PROTO);
        CHECK(os.balance == 0);   /* partial CP kid arrays freed on error */
    }

    /* -- leak balance: track parse error frees prior cp_ref_ids ------ */
    {
        oom_state_t os = { 0, 0, 0 };
        moq_alloc_t oa = oom_allocator(&os);
        moq_msf_catalog_t cat;
        /* track 0 valid w/ cp_ref_ids; track 1 missing name -> PROTO */
        CHECK(moq_msf_catalog_parse(&oa, lit(
            "{\"version\":\"1\",\"tracks\":["
            "{\"name\":\"a\",\"packaging\":\"cmaf\",\"isLive\":true,"
            "\"contentProtectionRefIDs\":[\"x\",\"y\"]},"
            "{\"packaging\":\"cmaf\",\"isLive\":true}]}"),
            &cat) == MOQ_ERR_PROTO);
        CHECK(os.balance == 0);   /* track 0's cp_ref_ids freed on error */
    }

    /* -- duplicate allocated-array keys rejected (no leak) ----------- */
    {
        oom_state_t os = { 0, 0, 0 };
        moq_alloc_t oa = oom_allocator(&os);
        moq_msf_catalog_t cat;
        /* duplicate defaultKID */
        CHECK(moq_msf_catalog_parse(&oa, lit(
            "{\"version\":\"1\",\"contentProtections\":[{\"refID\":\"1\","
            "\"defaultKID\":[\"a\"],\"defaultKID\":[\"b\"],\"scheme\":\"cenc\","
            "\"drmSystem\":{\"systemID\":\"s\"}}],"
            "\"tracks\":[{\"name\":\"v\",\"packaging\":\"cmaf\",\"isLive\":true}]}"),
            &cat) == MOQ_ERR_PROTO);
        CHECK(os.balance == 0);

        os.balance = 0; os.alloc_count = 0;
        /* duplicate contentProtectionRefIDs */
        CHECK(moq_msf_catalog_parse(&oa, lit(
            "{\"version\":\"1\",\"tracks\":[{\"name\":\"v\",\"packaging\":\"cmaf\","
            "\"isLive\":true,\"contentProtectionRefIDs\":[\"x\"],"
            "\"contentProtectionRefIDs\":[\"y\"]}]}"),
            &cat) == MOQ_ERR_PROTO);
        CHECK(os.balance == 0);

        os.balance = 0; os.alloc_count = 0;
        /* duplicate depends */
        CHECK(moq_msf_catalog_parse(&oa, lit(
            "{\"version\":1,\"tracks\":[{\"name\":\"sap\","
            "\"packaging\":\"eventtimeline\",\"isLive\":true,\"eventType\":\"x\","
            "\"depends\":[\"a\"],\"depends\":[\"b\"]}]}"),
            &cat) == MOQ_ERR_PROTO);
        CHECK(os.balance == 0);
    }

    /* -- malformed DRM URL shapes -> PROTO -------------------------- */
    {
        moq_msf_catalog_t cat;
        /* laURL is not an object */
        CHECK(moq_msf_catalog_parse(alloc, lit(
            "{\"version\":\"1\",\"contentProtections\":[{\"refID\":\"1\","
            "\"defaultKID\":[\"a\"],\"scheme\":\"cenc\","
            "\"drmSystem\":{\"systemID\":\"s\",\"laURL\":\"bad\"}}],"
            "\"tracks\":[{\"name\":\"v\",\"packaging\":\"cmaf\",\"isLive\":true}]}"),
            &cat) == MOQ_ERR_PROTO);
        /* laURL object without the required url */
        CHECK(moq_msf_catalog_parse(alloc, lit(
            "{\"version\":\"1\",\"contentProtections\":[{\"refID\":\"1\","
            "\"defaultKID\":[\"a\"],\"scheme\":\"cenc\","
            "\"drmSystem\":{\"systemID\":\"s\",\"laURL\":{\"type\":\"EME-1.0\"}}}],"
            "\"tracks\":[{\"name\":\"v\",\"packaging\":\"cmaf\",\"isLive\":true}]}"),
            &cat) == MOQ_ERR_PROTO);
    }

#ifdef MOQ_MSF_FIXTURE_DIR
    /* -- load the CMSF fixture (5.1 hd/md/sd+audio + SAP timeline) --- */
    {
        size_t flen = 0;
        char *fjson = read_fixture("cmsf_cmaf_simulcast.json", &flen);
        CHECK(fjson != NULL);
        if (fjson) {
            moq_msf_catalog_t cat;
            CHECK(moq_msf_catalog_parse(alloc,
                (moq_bytes_t){ (const uint8_t *)fjson, flen }, &cat) == MOQ_OK);
            CHECK_EQ_SIZE(cat.track_count, 5);
            CHECK_EQ_SIZE(cat.init_data_count, 4);

            const moq_msf_track_t *hd = NULL, *sap = NULL;
            for (size_t i = 0; i < cat.track_count; i++) {
                if (bv_eq(cat.tracks[i].name, "hd")) hd = &cat.tracks[i];
                if (bv_eq(cat.tracks[i].name, "sap-timeline")) sap = &cat.tracks[i];
            }
            CHECK(hd && bv_eq(hd->packaging, "cmaf") &&
                  hd->has_alt_group && hd->has_init_ref);
            CHECK(hd && moq_msf_catalog_find_init_data(&cat, hd->init_ref) != NULL);
            CHECK(sap && bv_eq(sap->packaging, "eventtimeline") &&
                  sap->has_event_type &&
                  bv_eq(sap->event_type, "org.ietf.moq.cmsf.sap"));

            /* round-trip the loaded fixture */
            moq_rcbuf_t *enc = NULL;
            CHECK(moq_msf_catalog_encode(alloc, &cat, &enc) == MOQ_OK);
            if (enc) {
                moq_msf_catalog_t back;
                CHECK(moq_msf_catalog_parse(alloc,
                    (moq_bytes_t){ moq_rcbuf_data(enc), moq_rcbuf_len(enc) },
                    &back) == MOQ_OK);
                CHECK_EQ_SIZE(back.track_count, 5);
                CHECK_EQ_SIZE(back.init_data_count, 4);
                moq_msf_catalog_cleanup(alloc, &back);
                moq_rcbuf_decref(enc);
            }
            moq_msf_catalog_cleanup(alloc, &cat);
            free(fjson);
        }
    }

    /* -- DRM/CBCS fixture (CMSF §5.2) -------------------------------- */
    {
        size_t flen = 0;
        char *fjson = read_fixture("cmsf_drm_cbcs.json", &flen);
        CHECK(fjson != NULL);
        if (fjson) {
            moq_msf_catalog_t cat;
            CHECK(moq_msf_catalog_parse(alloc,
                (moq_bytes_t){ (const uint8_t *)fjson, flen }, &cat) == MOQ_OK);
            CHECK_EQ_SIZE(cat.content_protection_count, 3);
            CHECK(bv_eq(cat.content_protections[0].scheme, "cbcs"));

            const moq_msf_track_t *vp = NULL;
            for (size_t i = 0; i < cat.track_count; i++)
                if (bv_eq(cat.tracks[i].name, "video_protected")) vp = &cat.tracks[i];
            CHECK(vp && vp->cp_ref_id_count == 3);
            CHECK(vp && moq_msf_catalog_find_content_protection(&cat,
                            vp->cp_ref_ids[2]) != NULL);
            const moq_cmsf_content_protection_t *fp =
                moq_msf_catalog_find_content_protection(&cat, lit("3"));
            CHECK(fp && fp->drm_system.cert_url.present);

            moq_rcbuf_t *enc = NULL;
            CHECK(moq_msf_catalog_encode(alloc, &cat, &enc) == MOQ_OK);
            if (enc) {
                moq_msf_catalog_t back;
                CHECK(moq_msf_catalog_parse(alloc,
                    (moq_bytes_t){ moq_rcbuf_data(enc), moq_rcbuf_len(enc) },
                    &back) == MOQ_OK);
                CHECK_EQ_SIZE(back.content_protection_count, 3);
                moq_msf_catalog_cleanup(alloc, &back);
                moq_rcbuf_decref(enc);
            }
            moq_msf_catalog_cleanup(alloc, &cat);
            free(fjson);
        }
    }

    /* -- ClearKey fixture (CMSF §5.3) -------------------------------- */
    {
        size_t flen = 0;
        char *fjson = read_fixture("cmsf_clearkey.json", &flen);
        CHECK(fjson != NULL);
        if (fjson) {
            moq_msf_catalog_t cat;
            CHECK(moq_msf_catalog_parse(alloc,
                (moq_bytes_t){ (const uint8_t *)fjson, flen }, &cat) == MOQ_OK);
            CHECK_EQ_SIZE(cat.content_protection_count, 1);
            CHECK(bv_eq(cat.content_protections[0].scheme, "cenc"));
            CHECK(bv_eq(cat.content_protections[0].drm_system.system_id,
                        "1077efec-c0b2-4d02-ace3-3c1e52e2fb4b"));
            CHECK(cat.content_protections[0].drm_system.la_url.has_type &&
                  bv_eq(cat.content_protections[0].drm_system.la_url.type,
                        "EME-1.0"));
            moq_msf_catalog_cleanup(alloc, &cat);
            free(fjson);
        }
    }

    /* -- deltaUpdate add+clone fixture: parse + round-trip (§5.6.4) --- */
    {
        size_t flen = 0;
        char *fjson = read_fixture("delta_add_clone.json", &flen);
        CHECK(fjson != NULL);
        if (fjson) {
            moq_msf_catalog_t cat;
            CHECK(moq_msf_catalog_parse(alloc,
                (moq_bytes_t){ (const uint8_t *)fjson, flen }, &cat) == MOQ_OK);
            CHECK(cat.delta_update != NULL);
            CHECK_EQ_SIZE(cat.delta_update_count, 2);
            CHECK_EQ_SIZE(cat.track_count, 0);
            CHECK(cat.has_generated_at);
            CHECK(cat.delta_update[0].op == MOQ_MSF_DELTA_OP_ADD);
            CHECK_EQ_SIZE(cat.delta_update[0].track_count, 1);
            CHECK(bv_eq(cat.delta_update[0].tracks[0].name, "slides"));
            /* the add example omits packaging/isLive -> isLive present, no pkg */
            CHECK(cat.delta_update[0].tracks[0].has_is_live);
            CHECK(cat.delta_update[0].tracks[0].has_packaging == false);
            CHECK(cat.delta_update[1].op == MOQ_MSF_DELTA_OP_CLONE);
            const moq_msf_track_t *cl = &cat.delta_update[1].tracks[0];
            CHECK(bv_eq(cl->name, "video-720"));
            CHECK(cl->has_parent_name && bv_eq(cl->parent_name, "video-1080"));
            CHECK(cl->has_parent_namespace &&
                  bv_eq(cl->parent_namespace, "example.com/custom"));
            CHECK(cl->has_width && cl->width == 1280);
            CHECK(cl->has_bitrate && cl->bitrate == 600000);
            CHECK(cl->has_packaging == false);   /* inherited, not redefined */

            moq_rcbuf_t *enc = NULL;
            CHECK(moq_msf_catalog_encode(alloc, &cat, &enc) == MOQ_OK);
            moq_bytes_t ej = { moq_rcbuf_data(enc), moq_rcbuf_len(enc) };
            CHECK(contains_bytes(ej, "\"deltaUpdate\""));
            CHECK(contains_bytes(ej, "\"parentName\":\"video-1080\""));
            moq_msf_catalog_t back;
            CHECK(moq_msf_catalog_parse(alloc, ej, &back) == MOQ_OK);
            CHECK_EQ_SIZE(back.delta_update_count, 2);
            CHECK(back.delta_update[0].op == MOQ_MSF_DELTA_OP_ADD);
            CHECK(back.delta_update[1].op == MOQ_MSF_DELTA_OP_CLONE);
            CHECK(bv_eq(back.delta_update[1].tracks[0].parent_name, "video-1080"));
            CHECK(back.delta_update[1].tracks[0].has_width &&
                  back.delta_update[1].tracks[0].width == 1280);
            moq_msf_catalog_cleanup(alloc, &back);
            moq_rcbuf_decref(enc);
            moq_msf_catalog_cleanup(alloc, &cat);
            free(fjson);
        }
    }

    /* -- deltaUpdate remove fixture: parse + round-trip (§5.6.5) ------ */
    {
        size_t flen = 0;
        char *fjson = read_fixture("delta_remove.json", &flen);
        CHECK(fjson != NULL);
        if (fjson) {
            moq_msf_catalog_t cat;
            CHECK(moq_msf_catalog_parse(alloc,
                (moq_bytes_t){ (const uint8_t *)fjson, flen }, &cat) == MOQ_OK);
            CHECK_EQ_SIZE(cat.delta_update_count, 1);
            CHECK(cat.delta_update[0].op == MOQ_MSF_DELTA_OP_REMOVE);
            CHECK_EQ_SIZE(cat.delta_update[0].track_count, 2);
            CHECK(bv_eq(cat.delta_update[0].tracks[0].name, "video"));
            CHECK(bv_eq(cat.delta_update[0].tracks[1].name, "slides"));

            moq_rcbuf_t *enc = NULL;
            CHECK(moq_msf_catalog_encode(alloc, &cat, &enc) == MOQ_OK);
            moq_bytes_t ej = { moq_rcbuf_data(enc), moq_rcbuf_len(enc) };
            CHECK(contains_bytes(ej, "\"op\":\"remove\""));
            moq_msf_catalog_t back;
            CHECK(moq_msf_catalog_parse(alloc, ej, &back) == MOQ_OK);
            CHECK_EQ_SIZE(back.delta_update[0].track_count, 2);
            moq_msf_catalog_cleanup(alloc, &back);
            moq_rcbuf_decref(enc);
            moq_msf_catalog_cleanup(alloc, &cat);
            free(fjson);
        }
    }

    /* -- media timeline template fixture: parse + round-trip (§5.6.10) - */
    {
        size_t flen = 0;
        char *fjson = read_fixture("template.json", &flen);
        CHECK(fjson != NULL);
        if (fjson) {
            moq_msf_catalog_t cat;
            CHECK(moq_msf_catalog_parse(alloc,
                (moq_bytes_t){ (const uint8_t *)fjson, flen }, &cat) == MOQ_OK);
            CHECK_EQ_SIZE(cat.track_count, 2);
            for (size_t i = 0; i < cat.track_count; i++) {
                const moq_msf_track_t *t = &cat.tracks[i];
                CHECK(t->has_template);
                /* §5.6.10: [0, 2002, [0,0], [1,0], 1759924158381, 2002] */
                CHECK_EQ_U64(t->template_.start_media_ms, 0);
                CHECK_EQ_U64(t->template_.delta_media_ms, 2002);
                CHECK_EQ_U64(t->template_.start_group, 0);
                CHECK_EQ_U64(t->template_.start_object, 0);
                CHECK_EQ_U64(t->template_.delta_group, 1);
                CHECK_EQ_U64(t->template_.delta_object, 0);
                CHECK_EQ_U64(t->template_.start_wallclock_ms, 1759924158381ULL);
                CHECK_EQ_U64(t->template_.delta_wallclock_ms, 2002);
            }

            /* round-trip: encode emits the template, re-parse preserves it */
            moq_rcbuf_t *enc = NULL;
            CHECK(moq_msf_catalog_encode(alloc, &cat, &enc) == MOQ_OK);
            if (enc) {
                moq_bytes_t ej = { moq_rcbuf_data(enc), moq_rcbuf_len(enc) };
                CHECK(contains_bytes(ej, "\"template\":[0,2002,[0,0],[1,0],"
                                         "1759924158381,2002]"));
                moq_msf_catalog_t back;
                CHECK(moq_msf_catalog_parse(alloc, ej, &back) == MOQ_OK);
                CHECK_EQ_SIZE(back.track_count, 2);
                CHECK(back.tracks[0].has_template &&
                      back.tracks[0].template_.delta_media_ms == 2002 &&
                      back.tracks[0].template_.delta_group == 1 &&
                      back.tracks[0].template_.start_wallclock_ms ==
                          1759924158381ULL);
                moq_msf_catalog_cleanup(alloc, &back);
                moq_rcbuf_decref(enc);
            }
            moq_msf_catalog_cleanup(alloc, &cat);
            free(fjson);
        }
    }

    /* -- mediatimeline track declaration fixture (§5.6.9): lowercase
     *    "mimetype" in, canonical "mimeType" out --------------------- */
    {
        size_t flen = 0;
        char *fjson = read_fixture("mediatimeline.json", &flen);
        CHECK(fjson != NULL);
        if (fjson) {
            moq_msf_catalog_t cat;
            CHECK(moq_msf_catalog_parse(alloc,
                (moq_bytes_t){ (const uint8_t *)fjson, flen }, &cat) == MOQ_OK);
            CHECK_EQ_SIZE(cat.track_count, 3);
            const moq_msf_track_t *hist = NULL;
            for (size_t i = 0; i < cat.track_count; i++)
                if (bv_eq(cat.tracks[i].name, "history")) hist = &cat.tracks[i];
            CHECK(hist && bv_eq(hist->packaging, "mediatimeline"));
            CHECK(hist && hist->has_mime_type &&
                  bv_eq(hist->mime_type, "application/json"));
            CHECK(hist && hist->depends_count == 2 &&
                  bv_eq(hist->depends[0], "1080p-video") &&
                  bv_eq(hist->depends[1], "audio"));
            CHECK(hist && !hist->has_event_type);

            moq_rcbuf_t *enc = NULL;
            CHECK(moq_msf_catalog_encode(alloc, &cat, &enc) == MOQ_OK);
            if (enc) {
                moq_bytes_t ej = { moq_rcbuf_data(enc), moq_rcbuf_len(enc) };
                /* canonical key on output regardless of lowercase input */
                CHECK(contains_bytes(ej, "\"mimeType\":\"application/json\""));
                CHECK(!contains_bytes(ej, "\"mimetype\""));
                CHECK(contains_bytes(ej, "\"packaging\":\"mediatimeline\""));
                moq_msf_catalog_t back;
                CHECK(moq_msf_catalog_parse(alloc, ej, &back) == MOQ_OK);
                CHECK_EQ_SIZE(back.track_count, 3);
                moq_msf_catalog_cleanup(alloc, &back);
                moq_rcbuf_decref(enc);
            }
            moq_msf_catalog_cleanup(alloc, &cat);
            free(fjson);
        }
    }

    /* -- VOD fixture (§5.6.7): isLive=false + trackDuration -------------- */
    {
        size_t flen = 0;
        char *fjson = read_fixture("vod.json", &flen);
        CHECK(fjson != NULL);
        if (fjson) {
            moq_msf_catalog_t cat;
            CHECK(moq_msf_catalog_parse(alloc,
                (moq_bytes_t){ (const uint8_t *)fjson, flen }, &cat) == MOQ_OK);
            CHECK_EQ_SIZE(cat.track_count, 2);
            for (size_t i = 0; i < cat.track_count; i++) {
                CHECK(cat.tracks[i].is_live == false);
                CHECK(cat.tracks[i].has_track_duration);
                CHECK_EQ_U64(cat.tracks[i].track_duration_ms, 8072340);
            }
            moq_rcbuf_t *enc = NULL;
            CHECK(moq_msf_catalog_encode(alloc, &cat, &enc) == MOQ_OK);
            if (enc) {
                moq_bytes_t ej = { moq_rcbuf_data(enc), moq_rcbuf_len(enc) };
                CHECK(contains_bytes(ej, "\"trackDuration\":8072340"));
                CHECK(contains_bytes(ej, "\"isLive\":false"));
                moq_msf_catalog_t back;
                CHECK(moq_msf_catalog_parse(alloc, ej, &back) == MOQ_OK);
                CHECK_EQ_SIZE(back.track_count, 2);
                CHECK(back.tracks[0].has_track_duration &&
                      back.tracks[0].track_duration_ms == 8072340 &&
                      back.tracks[0].is_live == false);
                moq_msf_catalog_cleanup(alloc, &back);
                moq_rcbuf_decref(enc);
            }
            moq_msf_catalog_cleanup(alloc, &cat);
            free(fjson);
        }
    }

    /* -- termination fixture (§5.6.13): isComplete=true + empty tracks --- */
    {
        size_t flen = 0;
        char *fjson = read_fixture("termination.json", &flen);
        CHECK(fjson != NULL);
        if (fjson) {
            moq_msf_catalog_t cat;
            CHECK(moq_msf_catalog_parse(alloc,
                (moq_bytes_t){ (const uint8_t *)fjson, flen }, &cat) == MOQ_OK);
            CHECK(cat.is_complete);
            CHECK_EQ_SIZE(cat.track_count, 0);
            CHECK(cat.has_generated_at);
            moq_rcbuf_t *enc = NULL;
            CHECK(moq_msf_catalog_encode(alloc, &cat, &enc) == MOQ_OK);
            if (enc) {
                moq_bytes_t ej = { moq_rcbuf_data(enc), moq_rcbuf_len(enc) };
                CHECK(contains_bytes(ej, "\"isComplete\":true"));
                CHECK(contains_bytes(ej, "\"tracks\":[]"));
                moq_msf_catalog_t back;
                CHECK(moq_msf_catalog_parse(alloc, ej, &back) == MOQ_OK);
                CHECK(back.is_complete);
                CHECK_EQ_SIZE(back.track_count, 0);
                moq_msf_catalog_cleanup(alloc, &back);
                moq_rcbuf_decref(enc);
            }
            moq_msf_catalog_cleanup(alloc, &cat);
            free(fjson);
        }
    }
#endif

    /* -- apply_delta: add + clone -> expected effective catalog ------- */
    {
        const char *base_json =
            "{\"version\":1,\"generatedAt\":1,\"tracks\":["
            "{\"name\":\"video-1080\",\"packaging\":\"loc\",\"isLive\":true,"
            "\"role\":\"video\",\"codec\":\"av01.0.08M.10.0.110.09\","
            "\"width\":1920,\"height\":1080,\"bitrate\":2000000}"
            "]}";
        const char *delta_json =
            "{\"generatedAt\":2,\"deltaUpdate\":["
            "{\"op\":\"add\",\"tracks\":[{\"name\":\"slides\","
            "\"packaging\":\"loc\",\"isLive\":true,\"role\":\"video\"}]},"
            "{\"op\":\"clone\",\"tracks\":[{\"parentName\":\"video-1080\","
            "\"name\":\"video-720\",\"width\":1280,\"height\":720,"
            "\"bitrate\":600000}]}"
            "]}";
        moq_msf_catalog_t base, delta, eff;
        CHECK(moq_msf_catalog_parse(alloc, lit(base_json), &base) == MOQ_OK);
        CHECK(moq_msf_catalog_parse(alloc, lit(delta_json), &delta) == MOQ_OK);
        CHECK(moq_msf_catalog_apply_delta(alloc, &base, &delta, &eff) == MOQ_OK);
        CHECK(eff.delta_update == NULL);
        CHECK_EQ_SIZE(eff.track_count, 3);
        CHECK(bv_eq(eff.tracks[0].name, "video-1080"));
        CHECK(bv_eq(eff.tracks[1].name, "slides"));
        const moq_msf_track_t *c = &eff.tracks[2];
        CHECK(bv_eq(c->name, "video-720"));
        CHECK(bv_eq(c->packaging, "loc"));                 /* inherited */
        CHECK(c->is_live == true);                          /* inherited */
        CHECK(c->has_codec &&
              bv_eq(c->codec, "av01.0.08M.10.0.110.09"));   /* inherited */
        CHECK(c->has_width && c->width == 1280);            /* overridden */
        CHECK(c->has_height && c->height == 720);           /* overridden */
        CHECK(c->has_bitrate && c->bitrate == 600000);      /* overridden */
        CHECK(c->has_parent_name == false);                 /* markers cleared */
        CHECK(eff.has_generated_at && eff.generated_at == 2);
        moq_msf_catalog_cleanup(alloc, &eff);
        moq_msf_catalog_cleanup(alloc, &delta);
        moq_msf_catalog_cleanup(alloc, &base);
    }

    /* -- apply_delta keeps receive leniency: a base with non-CMSF-strict
     * contentProtection (non-UUID systemID/KID) still applies its delta. The
     * internal re-encode must NOT enforce the authoring UUID/enum rules that
     * the public moq_msf_catalog_encode does (else inbound interop breaks). */
    {
        const char *base_json =
            "{\"version\":1,\"generatedAt\":1,"
            "\"contentProtections\":[{\"refID\":\"cp1\","
            "\"defaultKID\":[\"loose-kid\"],\"scheme\":\"cbcs\","
            "\"drmSystem\":{\"systemID\":\"not-a-uuid\"}}],"
            "\"tracks\":[{\"name\":\"v\",\"packaging\":\"loc\",\"isLive\":true,"
            "\"contentProtectionRefIDs\":[\"cp1\"]}]}";
        const char *delta_json =
            "{\"generatedAt\":2,\"deltaUpdate\":["
            "{\"op\":\"add\",\"tracks\":[{\"name\":\"a\","
            "\"packaging\":\"loc\",\"isLive\":true}]}]}";
        moq_msf_catalog_t base, delta, eff;
        CHECK(moq_msf_catalog_parse(alloc, lit(base_json), &base) == MOQ_OK);
        CHECK(moq_msf_catalog_parse(alloc, lit(delta_json), &delta) == MOQ_OK);
        /* Lenient: the loose CP does NOT block delta application. */
        CHECK(moq_msf_catalog_apply_delta(alloc, &base, &delta, &eff) == MOQ_OK);
        CHECK_EQ_SIZE(eff.content_protection_count, 1);
        CHECK(bv_eq(eff.content_protections[0].drm_system.system_id,
                    "not-a-uuid"));   /* carried through unchanged */
        CHECK_EQ_SIZE(eff.track_count, 2);
        moq_msf_catalog_cleanup(alloc, &eff);
        moq_msf_catalog_cleanup(alloc, &delta);
        moq_msf_catalog_cleanup(alloc, &base);
    }

    /* -- apply_delta: remove drops matched tracks -------------------- */
    {
        const char *base_json =
            "{\"version\":1,\"tracks\":["
            "{\"name\":\"video\",\"packaging\":\"loc\",\"isLive\":true},"
            "{\"name\":\"slides\",\"packaging\":\"loc\",\"isLive\":true},"
            "{\"name\":\"audio\",\"packaging\":\"loc\",\"isLive\":true}"
            "]}";
        const char *delta_json =
            "{\"deltaUpdate\":[{\"op\":\"remove\",\"tracks\":["
            "{\"name\":\"video\"},{\"name\":\"slides\"}]}]}";
        moq_msf_catalog_t base, delta, eff;
        CHECK(moq_msf_catalog_parse(alloc, lit(base_json), &base) == MOQ_OK);
        CHECK(moq_msf_catalog_parse(alloc, lit(delta_json), &delta) == MOQ_OK);
        CHECK(moq_msf_catalog_apply_delta(alloc, &base, &delta, &eff) == MOQ_OK);
        CHECK_EQ_SIZE(eff.track_count, 1);
        CHECK(bv_eq(eff.tracks[0].name, "audio"));
        moq_msf_catalog_cleanup(alloc, &eff);
        moq_msf_catalog_cleanup(alloc, &delta);
        moq_msf_catalog_cleanup(alloc, &base);
    }

    /* -- apply_delta: error cases ------------------------------------ */
    {
        moq_msf_catalog_t indep, dlt, out2;
        CHECK(moq_msf_catalog_parse(alloc, lit(
            "{\"version\":1,\"tracks\":[{\"name\":\"a\",\"packaging\":\"loc\","
            "\"isLive\":true}]}"), &indep) == MOQ_OK);
        CHECK(moq_msf_catalog_parse(alloc, lit(
            "{\"deltaUpdate\":[{\"op\":\"clone\",\"tracks\":[{\"parentName\":"
            "\"ghost\",\"name\":\"b\"}]}]}"), &dlt) == MOQ_OK);
        /* clone of an absent parent fails */
        CHECK(moq_msf_catalog_apply_delta(alloc, &indep, &dlt, &out2)
              == MOQ_ERR_PROTO);
        /* base must be independent; delta must be a delta */
        CHECK(moq_msf_catalog_apply_delta(alloc, &dlt, &dlt, &out2)
              == MOQ_ERR_INVAL);
        CHECK(moq_msf_catalog_apply_delta(alloc, &indep, &indep, &out2)
              == MOQ_ERR_INVAL);
        moq_msf_catalog_cleanup(alloc, &dlt);
        moq_msf_catalog_cleanup(alloc, &indep);
    }

    /* -- apply_delta: tuple semantics (5.3) -------------------------- */
    {
        const char *base2 =
            "{\"version\":1,\"tracks\":["
            "{\"name\":\"a\",\"packaging\":\"loc\",\"isLive\":true},"
            "{\"name\":\"b\",\"packaging\":\"loc\",\"isLive\":true}"
            "]}";
        struct { const char *delta; } cases[] = {
            /* duplicate add: "a" already exists */
            {"{\"deltaUpdate\":[{\"op\":\"add\",\"tracks\":[{\"name\":\"a\","
             "\"packaging\":\"loc\",\"isLive\":true}]}]}"},
            /* clone to an existing tuple: new name "b" already exists */
            {"{\"deltaUpdate\":[{\"op\":\"clone\",\"tracks\":[{\"parentName\":"
             "\"a\",\"name\":\"b\"}]}]}"},
            /* remove of an absent tuple */
            {"{\"deltaUpdate\":[{\"op\":\"remove\",\"tracks\":[{\"name\":"
             "\"ghost\"}]}]}"},
        };
        for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
            moq_msf_catalog_t base, delta, eff;
            CHECK(moq_msf_catalog_parse(alloc, lit(base2), &base) == MOQ_OK);
            CHECK(moq_msf_catalog_parse(alloc, lit(cases[i].delta), &delta) == MOQ_OK);
            CHECK(moq_msf_catalog_apply_delta(alloc, &base, &delta, &eff)
                  == MOQ_ERR_PROTO);
            moq_msf_catalog_cleanup(alloc, &delta);
            moq_msf_catalog_cleanup(alloc, &base);
        }
    }

    /* -- direct encode rejects an unknown delta op kind -------------- */
    {
        moq_msf_track_t tr;
        memset(&tr, 0, sizeof(tr));
        tr.struct_size = sizeof(tr);
        tr.name = lit("x");
        tr.packaging = lit("loc"); tr.has_packaging = true;
        tr.is_live = true; tr.has_is_live = true;
        moq_msf_delta_op_t op;
        op.op = (moq_msf_delta_op_kind_t)99;   /* not add/remove/clone */
        op.tracks = &tr;
        op.track_count = 1;
        moq_msf_catalog_t cat;
        memset(&cat, 0, sizeof(cat));
        cat.struct_size = sizeof(cat);
        cat.delta_update = &op;
        cat.delta_update_count = 1;
        moq_rcbuf_t *enc = NULL;
        CHECK(moq_msf_catalog_encode(alloc, &cat, &enc) == MOQ_ERR_INVAL);
        CHECK(enc == NULL);
    }

    /* -- deltaUpdate malformed shapes -> PROTO ----------------------- */
    {
        static const char *bad[] = {
            "{\"deltaUpdate\":{}}",                                  /* not array */
            "{\"version\":1,\"deltaUpdate\":[{\"op\":\"add\",\"tracks\":"
                "[{\"name\":\"x\",\"packaging\":\"loc\",\"isLive\":true}]}]}", /* +version */
            "{\"tracks\":[],\"deltaUpdate\":[{\"op\":\"add\",\"tracks\":"
                "[{\"name\":\"x\",\"packaging\":\"loc\",\"isLive\":true}]}]}", /* +tracks */
            "{\"deltaUpdate\":[]}",                                  /* empty ops */
            "{\"deltaUpdate\":[{\"tracks\":[{\"name\":\"x\"}]}]}",   /* no op */
            "{\"deltaUpdate\":[{\"op\":\"frob\",\"tracks\":[{\"name\":\"x\"}]}]}", /* unknown op */
            "{\"deltaUpdate\":[{\"op\":\"add\"}]}",                  /* no tracks */
            "{\"deltaUpdate\":[{\"op\":\"add\",\"tracks\":[]}]}",    /* empty op tracks */
            "{\"deltaUpdate\":[{\"op\":\"clone\",\"tracks\":[{\"name\":\"b\"}]}]}", /* no parentName */
            "{\"deltaUpdate\":[{\"op\":\"remove\",\"tracks\":[{\"namespace\":\"x\"}]}]}", /* no name */
            /* parentName/parentNamespace only allowed in clone (5.2.33/34) */
            "{\"version\":1,\"tracks\":[{\"name\":\"v\",\"packaging\":\"loc\","
                "\"isLive\":true,\"parentName\":\"p\"}]}",                /* independent +parentName */
            "{\"deltaUpdate\":[{\"op\":\"add\",\"tracks\":[{\"name\":\"x\","
                "\"packaging\":\"loc\",\"isLive\":true,\"parentName\":\"p\"}]}]}", /* add +parentName */
            "{\"deltaUpdate\":[{\"op\":\"add\",\"tracks\":[{\"name\":\"x\","
                "\"parentNamespace\":\"ns\"}]}]}",                        /* add +parentNamespace */
            /* remove must hold only name (+namespace) (5.1.6) */
            "{\"deltaUpdate\":[{\"op\":\"remove\",\"tracks\":[{\"name\":\"x\",\"role\":\"video\"}]}]}",
            "{\"deltaUpdate\":[{\"op\":\"remove\",\"tracks\":[{\"name\":\"x\",\"packaging\":\"loc\"}]}]}",
            "{\"deltaUpdate\":[{\"op\":\"remove\",\"tracks\":[{\"name\":\"x\",\"isLive\":true}]}]}",
            "{\"deltaUpdate\":[{\"op\":\"remove\",\"tracks\":[{\"name\":\"x\",\"parentName\":\"p\"}]}]}",
        };
        for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); i++) {
            moq_msf_catalog_t mc;
            CHECK(moq_msf_catalog_parse(alloc, lit(bad[i]), &mc) == MOQ_ERR_PROTO);
        }
    }

    /* -- malformed template -> PROTO at parse (a present structured field is
     *    never silently dropped; MSF 5.2.15 / §7.4) --------------------- */
    {
        static const char *bad[] = {
            /* wrong length: 5 elements */
            "{\"version\":1,\"tracks\":[{\"name\":\"v\",\"packaging\":\"loc\","
                "\"isLive\":true,\"template\":[0,2002,[0,0],[1,0],1759924158381]}]}",
            /* wrong length: 7 elements */
            "{\"version\":1,\"tracks\":[{\"name\":\"v\",\"packaging\":\"loc\","
                "\"isLive\":true,\"template\":[0,2002,[0,0],[1,0],1,2,3]}]}",
            /* startLocation is a number, not a [g,o] array */
            "{\"version\":1,\"tracks\":[{\"name\":\"v\",\"packaging\":\"loc\","
                "\"isLive\":true,\"template\":[0,2002,0,[1,0],1,2]}]}",
            /* startLocation array has 3 elements */
            "{\"version\":1,\"tracks\":[{\"name\":\"v\",\"packaging\":\"loc\","
                "\"isLive\":true,\"template\":[0,2002,[0,0,5],[1,0],1,2]}]}",
            /* deltaLocation contains non-numbers */
            "{\"version\":1,\"tracks\":[{\"name\":\"v\",\"packaging\":\"loc\","
                "\"isLive\":true,\"template\":[0,2002,[0,0],[\"a\",\"b\"],1,2]}]}",
            /* startMediaTime is a string */
            "{\"version\":1,\"tracks\":[{\"name\":\"v\",\"packaging\":\"loc\","
                "\"isLive\":true,\"template\":[\"x\",2002,[0,0],[1,0],1,2]}]}",
            /* fractional value (non-integral) */
            "{\"version\":1,\"tracks\":[{\"name\":\"v\",\"packaging\":\"loc\","
                "\"isLive\":true,\"template\":[0,2002.5,[0,0],[1,0],1,2]}]}",
            /* negative value */
            "{\"version\":1,\"tracks\":[{\"name\":\"v\",\"packaging\":\"loc\","
                "\"isLive\":true,\"template\":[0,2002,[0,0],[-1,0],1,2]}]}",
            /* template is not an array */
            "{\"version\":1,\"tracks\":[{\"name\":\"v\",\"packaging\":\"loc\","
                "\"isLive\":true,\"template\":123}]}",
            /* duplicate template key */
            "{\"version\":1,\"tracks\":[{\"name\":\"v\",\"packaging\":\"loc\","
                "\"isLive\":true,\"template\":[0,2002,[0,0],[1,0],1,2],"
                "\"template\":[0,2002,[0,0],[1,0],1,2]}]}",
        };
        for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); i++) {
            moq_msf_catalog_t mc;
            CHECK(moq_msf_catalog_parse(alloc, lit(bad[i]), &mc) == MOQ_ERR_PROTO);
        }
    }

    /* -- mediatimeline validation: parses leniently but the encoder rejects an
     *    incoherent declaration (MSF §7.2) ----------------------------- */
    {
        static const char *bad[] = {
            /* missing mimeType */
            "{\"version\":1,\"tracks\":[{\"name\":\"h\",\"packaging\":"
                "\"mediatimeline\",\"isLive\":true,\"depends\":[\"v\"]}]}",
            /* wrong mimeType */
            "{\"version\":1,\"tracks\":[{\"name\":\"h\",\"packaging\":"
                "\"mediatimeline\",\"isLive\":true,\"mimeType\":\"text/plain\","
                "\"depends\":[\"v\"]}]}",
            /* missing depends */
            "{\"version\":1,\"tracks\":[{\"name\":\"h\",\"packaging\":"
                "\"mediatimeline\",\"isLive\":true,\"mimeType\":"
                "\"application/json\"}]}",
            /* eventType present (forbidden outside eventtimeline) */
            "{\"version\":1,\"tracks\":[{\"name\":\"h\",\"packaging\":"
                "\"mediatimeline\",\"isLive\":true,\"mimeType\":"
                "\"application/json\",\"depends\":[\"v\"],\"eventType\":\"x\"}]}",
        };
        for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); i++) {
            moq_msf_catalog_t mc;
            CHECK(moq_msf_catalog_parse(alloc, lit(bad[i]), &mc) == MOQ_OK);
            moq_rcbuf_t *enc = NULL;
            CHECK(moq_msf_catalog_encode(alloc, &mc, &enc) == MOQ_ERR_INVAL);
            CHECK(enc == NULL);
            moq_msf_catalog_cleanup(alloc, &mc);
        }
    }

    /* -- template survives delta clone (inherit) + override ----------- */
    {
        const char *base_json =
            "{\"version\":1,\"tracks\":[{\"name\":\"v\",\"packaging\":\"loc\","
            "\"isLive\":true,\"template\":[0,2002,[0,0],[1,0],1000,2002]}]}";
        /* clone with no template -> inherits; clone with template -> override */
        const char *delta_json =
            "{\"deltaUpdate\":[{\"op\":\"clone\",\"tracks\":[{\"parentName\":"
            "\"v\",\"name\":\"v2\"}]},{\"op\":\"clone\",\"tracks\":[{"
            "\"parentName\":\"v\",\"name\":\"v3\",\"template\":[5,1000,[2,0],"
            "[1,0],9000,1000]}]}]}";
        moq_msf_catalog_t base, delta, eff;
        CHECK(moq_msf_catalog_parse(alloc, lit(base_json), &base) == MOQ_OK);
        CHECK(moq_msf_catalog_parse(alloc, lit(delta_json), &delta) == MOQ_OK);
        CHECK(moq_msf_catalog_apply_delta(alloc, &base, &delta, &eff) == MOQ_OK);
        CHECK_EQ_SIZE(eff.track_count, 3);
        /* v2 inherits the parent template verbatim */
        CHECK(bv_eq(eff.tracks[1].name, "v2") && eff.tracks[1].has_template);
        CHECK_EQ_U64(eff.tracks[1].template_.delta_media_ms, 2002);
        CHECK_EQ_U64(eff.tracks[1].template_.start_wallclock_ms, 1000);
        /* v3 overrides with its own template */
        CHECK(bv_eq(eff.tracks[2].name, "v3") && eff.tracks[2].has_template);
        CHECK_EQ_U64(eff.tracks[2].template_.start_media_ms, 5);
        CHECK_EQ_U64(eff.tracks[2].template_.delta_media_ms, 1000);
        CHECK_EQ_U64(eff.tracks[2].template_.start_group, 2);
        CHECK_EQ_U64(eff.tracks[2].template_.start_wallclock_ms, 9000);
        moq_msf_catalog_cleanup(alloc, &eff);
        moq_msf_catalog_cleanup(alloc, &delta);
        moq_msf_catalog_cleanup(alloc, &base);
    }

    /* -- VOD/completion negatives ------------------------------------ */
    {
        /* trackDuration on a live track -> parses leniently, encode rejects. */
        moq_msf_catalog_t mc;
        CHECK(moq_msf_catalog_parse(alloc, lit(
            "{\"version\":1,\"tracks\":[{\"name\":\"v\",\"packaging\":\"loc\","
            "\"isLive\":true,\"trackDuration\":1000}]}"), &mc) == MOQ_OK);
        moq_rcbuf_t *enc = NULL;
        CHECK(moq_msf_catalog_encode(alloc, &mc, &enc) == MOQ_ERR_INVAL);
        CHECK(enc == NULL);
        moq_msf_catalog_cleanup(alloc, &mc);

        /* a delta carrying isComplete is rejected at parse (5.1.3). */
        moq_msf_catalog_t dc;
        CHECK(moq_msf_catalog_parse(alloc, lit(
            "{\"isComplete\":true,\"deltaUpdate\":[{\"op\":\"add\",\"tracks\":["
            "{\"name\":\"x\",\"packaging\":\"loc\",\"isLive\":true}]}]}"), &dc)
            == MOQ_ERR_PROTO);

        /* a remove-op track carrying trackDuration is name/namespace-only viol. */
        moq_msf_catalog_t rc2;
        CHECK(moq_msf_catalog_parse(alloc, lit(
            "{\"deltaUpdate\":[{\"op\":\"remove\",\"tracks\":[{\"name\":\"x\","
            "\"trackDuration\":1000}]}]}"), &rc2) == MOQ_ERR_PROTO);

        /* a hand-built delta catalog with is_complete is rejected at encode. */
        moq_msf_track_t at;
        memset(&at, 0, sizeof(at));
        at.struct_size = sizeof(at);
        at.name = lit("x");
        at.packaging = lit("loc"); at.has_packaging = true;
        at.is_live = true; at.has_is_live = true;
        moq_msf_delta_op_t op = { MOQ_MSF_DELTA_OP_ADD, &at, 1 };
        moq_msf_catalog_t dcat;
        memset(&dcat, 0, sizeof(dcat));
        dcat.struct_size = sizeof(dcat);
        dcat.delta_update = &op;
        dcat.delta_update_count = 1;
        dcat.is_complete = true;
        moq_rcbuf_t *denc = NULL;
        CHECK(moq_msf_catalog_encode(alloc, &dcat, &denc) == MOQ_ERR_INVAL);
        CHECK(denc == NULL);
    }

    /* -- apply_delta: trackDuration clone inherit / override / live viol - */
    {
        const char *base_json =
            "{\"version\":1,\"tracks\":[{\"name\":\"v\",\"packaging\":\"loc\","
            "\"isLive\":false,\"trackDuration\":5000}]}";
        const char *delta_json =
            "{\"deltaUpdate\":[{\"op\":\"clone\",\"tracks\":[{\"parentName\":"
            "\"v\",\"name\":\"v2\"}]},{\"op\":\"clone\",\"tracks\":[{"
            "\"parentName\":\"v\",\"name\":\"v3\",\"trackDuration\":9000}]}]}";
        moq_msf_catalog_t base, delta, eff;
        CHECK(moq_msf_catalog_parse(alloc, lit(base_json), &base) == MOQ_OK);
        CHECK(moq_msf_catalog_parse(alloc, lit(delta_json), &delta) == MOQ_OK);
        CHECK(moq_msf_catalog_apply_delta(alloc, &base, &delta, &eff) == MOQ_OK);
        CHECK_EQ_SIZE(eff.track_count, 3);
        /* v2 inherits the parent's duration (and isLive=false). */
        CHECK(bv_eq(eff.tracks[1].name, "v2"));
        CHECK(eff.tracks[1].is_live == false);
        CHECK(eff.tracks[1].has_track_duration &&
              eff.tracks[1].track_duration_ms == 5000);
        /* v3 overrides it. */
        CHECK(bv_eq(eff.tracks[2].name, "v3"));
        CHECK(eff.tracks[2].has_track_duration &&
              eff.tracks[2].track_duration_ms == 9000);
        moq_msf_catalog_cleanup(alloc, &eff);
        moq_msf_catalog_cleanup(alloc, &delta);
        moq_msf_catalog_cleanup(alloc, &base);

        /* A clone that yields an effective LIVE track carrying trackDuration is
         * rejected when the effective catalog is materialized (lenient delta,
         * strict effective). */
        const char *live_base =
            "{\"version\":1,\"tracks\":[{\"name\":\"v\",\"packaging\":\"loc\","
            "\"isLive\":true}]}";
        const char *bad_delta =
            "{\"deltaUpdate\":[{\"op\":\"clone\",\"tracks\":[{\"parentName\":"
            "\"v\",\"name\":\"v2\",\"trackDuration\":1000}]}]}";
        moq_msf_catalog_t b2, d2, e2;
        CHECK(moq_msf_catalog_parse(alloc, lit(live_base), &b2) == MOQ_OK);
        CHECK(moq_msf_catalog_parse(alloc, lit(bad_delta), &d2) == MOQ_OK);
        CHECK(moq_msf_catalog_apply_delta(alloc, &b2, &d2, &e2) < 0);
        moq_msf_catalog_cleanup(alloc, &d2);
        moq_msf_catalog_cleanup(alloc, &b2);
    }

    /* -- apply_delta against a complete base is terminal (§5.1.3) ----- */
    {
        moq_msf_catalog_t base, delta, eff;
        CHECK(moq_msf_catalog_parse(alloc, lit(
            "{\"version\":\"1\",\"isComplete\":true,\"tracks\":[]}"), &base)
            == MOQ_OK);
        CHECK(base.is_complete);
        CHECK(moq_msf_catalog_parse(alloc, lit(
            "{\"deltaUpdate\":[{\"op\":\"add\",\"tracks\":[{\"name\":\"x\","
            "\"packaging\":\"loc\",\"isLive\":true}]}]}"), &delta) == MOQ_OK);
        /* a complete catalog admits no further changes -> rejected, no effective. */
        CHECK(moq_msf_catalog_apply_delta(alloc, &base, &delta, &eff)
              == MOQ_ERR_PROTO);
        moq_msf_catalog_cleanup(alloc, &delta);
        moq_msf_catalog_cleanup(alloc, &base);
    }

    /* -- leak balance: parse + cleanup of a delta is allocation-neutral */
    {
        sz_state_t szs = { 0, 0, 0 };
        moq_alloc_t sza = { &szs, sz_alloc_fn, NULL, sz_free_fn };
        moq_msf_catalog_t cat;
        CHECK(moq_msf_catalog_parse(&sza, lit(
            "{\"deltaUpdate\":[{\"op\":\"clone\",\"tracks\":[{\"parentName\":"
            "\"p\",\"name\":\"c\",\"width\":10,\"depends\":[\"a\"]}]},"
            "{\"op\":\"remove\",\"tracks\":[{\"name\":\"x\"}]}]}"), &cat) == MOQ_OK);
        moq_msf_catalog_cleanup(&sza, &cat);
        CHECK_EQ_U64((uint64_t)szs.balance, 0);
    }

    /* -- leak balance: apply_delta + cleanup is allocation-neutral --- */
    {
        sz_state_t szs = { 0, 0, 0 };
        moq_alloc_t sza = { &szs, sz_alloc_fn, NULL, sz_free_fn };
        moq_msf_catalog_t base, delta, eff;
        CHECK(moq_msf_catalog_parse(&sza, lit(
            "{\"version\":1,\"tracks\":[{\"name\":\"p\",\"packaging\":\"loc\","
            "\"isLive\":true,\"codec\":\"av01\"}]}"), &base) == MOQ_OK);
        CHECK(moq_msf_catalog_parse(&sza, lit(
            "{\"deltaUpdate\":[{\"op\":\"clone\",\"tracks\":[{\"parentName\":"
            "\"p\",\"name\":\"c\",\"bitrate\":1000}]},{\"op\":\"add\","
            "\"tracks\":[{\"name\":\"n\",\"packaging\":\"loc\",\"isLive\":true}]}]}"),
            &delta) == MOQ_OK);
        CHECK(moq_msf_catalog_apply_delta(&sza, &base, &delta, &eff) == MOQ_OK);
        CHECK_EQ_SIZE(eff.track_count, 3);
        moq_msf_catalog_cleanup(&sza, &eff);
        moq_msf_catalog_cleanup(&sza, &delta);
        moq_msf_catalog_cleanup(&sza, &base);
        CHECK_EQ_U64((uint64_t)szs.balance, 0);
    }

    /* -- OOM during delta parse leaves no leak (balance returns to 0) - */
    {
        const char *dj =
            "{\"deltaUpdate\":[{\"op\":\"clone\",\"tracks\":[{\"parentName\":"
            "\"p\",\"name\":\"c\",\"depends\":[\"a\",\"b\"]}]},{\"op\":\"add\","
            "\"tracks\":[{\"name\":\"n\",\"packaging\":\"loc\",\"isLive\":true}]}]}";
        for (uint64_t fail = 1; fail <= 8; fail++) {
            oom_state_t oom = { 0, 0, fail };
            moq_alloc_t fa = oom_allocator(&oom);
            moq_msf_catalog_t cat;
            moq_result_t rc = moq_msf_catalog_parse(&fa, lit(dj), &cat);
            if (rc == MOQ_OK) moq_msf_catalog_cleanup(&fa, &cat);
            CHECK_EQ_U64((uint64_t)oom.balance, 0);
        }
    }

    /* == media timeline (§7.1.1) explicit payload codec ================ */

    /* -- decode: spec example ----------------------------------------- */
    {
        moq_msf_media_timeline_record_t recs[8];
        size_t n = 999;
        moq_result_t rc = moq_msf_media_timeline_decode(lit(
            "[[0,[0,0],1759924158381],"
            "[2002,[1,0],1759924160383],"
            "[4004,[2,0],1759924162385],"
            "[6006,[3,0],1759924164387],"
            "[8008,[4,0],1759924166389]]"),
            recs, 8, &n);
        CHECK(rc == MOQ_OK);
        CHECK_EQ_SIZE(n, 5);
        CHECK_EQ_U64(recs[0].media_time_ms, 0);
        CHECK_EQ_U64(recs[0].group, 0);
        CHECK_EQ_U64(recs[0].object, 0);
        CHECK_EQ_U64(recs[0].wallclock_ms, 1759924158381ull);
        CHECK_EQ_U64(recs[4].media_time_ms, 8008);
        CHECK_EQ_U64(recs[4].group, 4);
        CHECK_EQ_U64(recs[4].object, 0);
        CHECK_EQ_U64(recs[4].wallclock_ms, 1759924166389ull);
    }

    /* -- decode: empty array is valid, count 0 ------------------------ */
    {
        moq_msf_media_timeline_record_t recs[4];
        size_t n = 999;
        moq_result_t rc = moq_msf_media_timeline_decode(lit("[]"), recs, 4, &n);
        CHECK(rc == MOQ_OK);
        CHECK_EQ_SIZE(n, 0);
        /* whitespace-padded empty array */
        rc = moq_msf_media_timeline_decode(lit("  [ ]  "), recs, 4, &n);
        CHECK(rc == MOQ_OK);
        CHECK_EQ_SIZE(n, 0);
    }

    /* -- decode: single record + wallclock 0 (VOD) -------------------- */
    {
        moq_msf_media_timeline_record_t recs[4];
        size_t n = 999;
        moq_result_t rc = moq_msf_media_timeline_decode(
            lit("[[4004,[2,7],0]]"), recs, 4, &n);
        CHECK(rc == MOQ_OK);
        CHECK_EQ_SIZE(n, 1);
        CHECK_EQ_U64(recs[0].media_time_ms, 4004);
        CHECK_EQ_U64(recs[0].group, 2);
        CHECK_EQ_U64(recs[0].object, 7);
        CHECK_EQ_U64(recs[0].wallclock_ms, 0);
    }

    /* -- decode: malformed -> MOQ_ERR_PROTO, out_count 0 -------------- */
    {
        static const char *bad[] = {
            "{}",                      /* non-array root (object) */
            "42",                      /* non-array root (number) */
            "\"x\"",                   /* non-array root (string) */
            "[[0,[0,0]]]",             /* record length 2 */
            "[[0,[0,0],1,2]]",         /* record length 4 */
            "[[0,5,1]]",               /* non-array location */
            "[[0,[0],1]]",             /* location too short */
            "[[0,[0,0,0],1]]",         /* location too long */
            "[[\"x\",[0,0],1]]",       /* string where number expected */
            "[[-1,[0,0],1]]",          /* negative number */
            "[[0.5,[0,0],1]]",         /* fractional number */
            "[[0,[0,0],1e3]]",         /* exponent form */
            "[[0,[0,0],01]]",          /* leading zero (wallclock) */
            "[[00,[0,0],0]]",          /* leading zero (mediaTime) */
            "[[0,[01,0],0]]",          /* leading zero (location group) */
            "[[0,[0,0],1]]x",          /* trailing garbage */
            "[[0,[0,0],1",             /* truncated */
            "[",                       /* truncated empty */
            "[[0,[0,0],1],]",          /* trailing comma */
        };
        for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); i++) {
            moq_msf_media_timeline_record_t recs[4];
            size_t n = 999;
            moq_result_t rc =
                moq_msf_media_timeline_decode(lit(bad[i]), recs, 4, &n);
            if (rc != MOQ_ERR_PROTO || n != 0)
                fprintf(stderr, "FAIL: bad[%zu]=\"%s\" rc=%d n=%zu\n",
                        i, bad[i], (int)rc, n);
            CHECK(rc == MOQ_ERR_PROTO);
            CHECK_EQ_SIZE(n, 0);
        }
    }

    /* -- decode: capacity (buffer too small + count probe + retry) ---- */
    {
        const char *doc =
            "[[0,[0,0],10],[1,[1,0],11],[2,[2,0],12]]";   /* 3 records */
        moq_msf_media_timeline_record_t recs[3];
        size_t n = 999;

        /* cap too small: BUFFER + needed count, caller ignores partial out. */
        moq_result_t rc = moq_msf_media_timeline_decode(lit(doc), recs, 2, &n);
        CHECK(rc == MOQ_ERR_BUFFER);
        CHECK_EQ_SIZE(n, 3);

        /* count probe: out=NULL, cap=0. */
        n = 999;
        rc = moq_msf_media_timeline_decode(lit(doc), NULL, 0, &n);
        CHECK(rc == MOQ_ERR_BUFFER);
        CHECK_EQ_SIZE(n, 3);

        /* count probe on the empty array is a plain success, count 0. */
        n = 999;
        rc = moq_msf_media_timeline_decode(lit("[]"), NULL, 0, &n);
        CHECK(rc == MOQ_OK);
        CHECK_EQ_SIZE(n, 0);

        /* retry with sufficient capacity succeeds. */
        n = 999;
        rc = moq_msf_media_timeline_decode(lit(doc), recs, 3, &n);
        CHECK(rc == MOQ_OK);
        CHECK_EQ_SIZE(n, 3);
        CHECK_EQ_U64(recs[2].media_time_ms, 2);
        CHECK_EQ_U64(recs[2].group, 2);
        CHECK_EQ_U64(recs[2].wallclock_ms, 12);
    }

    /* -- decode: invalid args ----------------------------------------- */
    {
        moq_msf_media_timeline_record_t recs[2];
        size_t n = 999;
        /* out_count NULL */
        CHECK(moq_msf_media_timeline_decode(lit("[]"), recs, 2, NULL)
              == MOQ_ERR_INVAL);
        /* out NULL with cap > 0 */
        CHECK(moq_msf_media_timeline_decode(lit("[]"), NULL, 2, &n)
              == MOQ_ERR_INVAL);
        /* NULL json.data is malformed, not a crash (no NULL pointer arith). */
        n = 999;
        CHECK(moq_msf_media_timeline_decode(
                  (moq_bytes_t){ NULL, 0 }, recs, 2, &n) == MOQ_ERR_PROTO);
        CHECK_EQ_SIZE(n, 0);
    }

    /* -- encode: exact compact output --------------------------------- */
    {
        moq_msf_media_timeline_record_t recs[2] = {
            { 0,    0, 0, 1759924158381ull },
            { 2002, 1, 0, 1759924160383ull },
        };
        const char *want =
            "[[0,[0,0],1759924158381],[2002,[1,0],1759924160383]]";
        uint8_t buf[128];
        size_t len = 999;
        moq_result_t rc =
            moq_msf_media_timeline_encode(recs, 2, buf, sizeof(buf), &len);
        CHECK(rc == MOQ_OK);
        CHECK_EQ_SIZE(len, strlen(want));
        CHECK(len <= sizeof(buf) && memcmp(buf, want, len) == 0);
    }

    /* -- encode: empty vector -> "[]" --------------------------------- */
    {
        uint8_t buf[8];
        size_t len = 999;
        moq_result_t rc =
            moq_msf_media_timeline_encode(NULL, 0, buf, sizeof(buf), &len);
        CHECK(rc == MOQ_OK);
        CHECK_EQ_SIZE(len, 2);
        CHECK(memcmp(buf, "[]", 2) == 0);
    }

    /* -- encode -> decode round trip ---------------------------------- */
    {
        moq_msf_media_timeline_record_t in[3] = {
            { 0,    0, 0, 0 },
            { 2002, 1, 0, 1759924160383ull },
            { 4004, 2, 5, 0 },
        };
        uint8_t buf[256];
        size_t len = 0;
        CHECK(moq_msf_media_timeline_encode(in, 3, buf, sizeof(buf), &len)
              == MOQ_OK);

        moq_msf_media_timeline_record_t out[3];
        size_t n = 0;
        CHECK(moq_msf_media_timeline_decode(
                  (moq_bytes_t){ buf, len }, out, 3, &n) == MOQ_OK);
        CHECK_EQ_SIZE(n, 3);
        for (size_t i = 0; i < 3; i++) {
            CHECK_EQ_U64(out[i].media_time_ms, in[i].media_time_ms);
            CHECK_EQ_U64(out[i].group, in[i].group);
            CHECK_EQ_U64(out[i].object, in[i].object);
            CHECK_EQ_U64(out[i].wallclock_ms, in[i].wallclock_ms);
        }
    }

    /* -- encode: small buffer -> BUFFER + needed length --------------- */
    {
        moq_msf_media_timeline_record_t recs[1] = { { 0, 0, 0, 12345 } };
        const char *want = "[[0,[0,0],12345]]";
        uint8_t small[4];
        size_t len = 999;
        moq_result_t rc =
            moq_msf_media_timeline_encode(recs, 1, small, sizeof(small), &len);
        CHECK(rc == MOQ_ERR_BUFFER);
        CHECK_EQ_SIZE(len, strlen(want));

        /* exact-1 is still too small; exact fits. */
        uint8_t exact[64];
        size_t need = strlen(want);
        len = 999;
        rc = moq_msf_media_timeline_encode(recs, 1, exact, need - 1, &len);
        CHECK(rc == MOQ_ERR_BUFFER);
        CHECK_EQ_SIZE(len, need);
        len = 999;
        rc = moq_msf_media_timeline_encode(recs, 1, exact, need, &len);
        CHECK(rc == MOQ_OK);
        CHECK_EQ_SIZE(len, need);
        CHECK(memcmp(exact, want, need) == 0);
    }

    /* -- encode: length probe (buf NULL / cap 0) + invalid args ------- */
    {
        moq_msf_media_timeline_record_t recs[1] = { { 0, 0, 0, 12345 } };
        size_t len = 999;
        moq_result_t rc =
            moq_msf_media_timeline_encode(recs, 1, NULL, 0, &len);
        CHECK(rc == MOQ_ERR_BUFFER);
        CHECK_EQ_SIZE(len, strlen("[[0,[0,0],12345]]"));

        uint8_t buf[8];
        len = 999;
        rc = moq_msf_media_timeline_encode(recs, 1, buf, 0, &len);
        CHECK(rc == MOQ_ERR_BUFFER);
        CHECK_EQ_SIZE(len, strlen("[[0,[0,0],12345]]"));

        /* out_len NULL */
        CHECK(moq_msf_media_timeline_encode(recs, 1, buf, sizeof(buf), NULL)
              == MOQ_ERR_INVAL);
        /* records NULL with count > 0 */
        CHECK(moq_msf_media_timeline_encode(NULL, 1, buf, sizeof(buf), &len)
              == MOQ_ERR_INVAL);
    }

    /* -- Array-size overflow guards (parser/apply_delta hardening) ----- */

    /* Direct boundary test of the internal checked sizer. A real JSON input
     * cannot reach a SIZE_MAX-overflowing array on a 64-bit host (the JSON DOM
     * itself would need that many elements), so exercise the helper directly. */
    {
        const size_t elem = sizeof(moq_msf_track_t);
        size_t bytes = 12345;

        /* Just over the representable limit -> overflow rejected. */
        CHECK(!moq_msf_checked_array_bytes(SIZE_MAX / elem + 1, elem, &bytes));

        /* Exactly the limit -> representable, computes the product. */
        size_t boundary = SIZE_MAX / elem;
        bytes = 0;
        CHECK(moq_msf_checked_array_bytes(boundary, elem, &bytes));
        CHECK(bytes == boundary * elem);

        /* Small, normal counts compute as expected. */
        bytes = 0;
        CHECK(moq_msf_checked_array_bytes(4, elem, &bytes));
        CHECK(bytes == 4 * elem);

        /* elem == 0 never overflows (and yields 0 bytes). */
        bytes = 1;
        CHECK(moq_msf_checked_array_bytes(SIZE_MAX, 0, &bytes));
        CHECK(bytes == 0);
    }

    /* apply_delta: the effective-track capacity multiplication must be guarded
     * BEFORE allocation. A base whose track_count is large enough that
     * cap * sizeof(track) wraps size_t must fail (NOMEM) without ever calling
     * the allocator. The recording allocator returns NULL on first use, so the
     * neutered code path fails cleanly too -- the discriminator is alloc_count.
     * (RED: with the guard removed, the wrapped size is allocated -> count 1.) */
    {
        moq_msf_track_t rt;
        memset(&rt, 0, sizeof(rt));

        moq_msf_catalog_t base;
        memset(&base, 0, sizeof(base));
        base.struct_size = sizeof(base);
        base.version = MOQ_MSF_VERSION;
        base.tracks = NULL;   /* never read: guard fails before the copy loop */
        base.track_count = SIZE_MAX / sizeof(moq_msf_track_t) + 1;

        moq_msf_delta_op_t op;
        memset(&op, 0, sizeof(op));
        op.op = MOQ_MSF_DELTA_OP_REMOVE;   /* does not grow cap */
        op.tracks = &rt;
        op.track_count = 1;

        moq_msf_catalog_t delta;
        memset(&delta, 0, sizeof(delta));
        delta.struct_size = sizeof(delta);
        delta.delta_update = &op;
        delta.delta_update_count = 1;

        oom_state_t os = { 0, 0, 1 };   /* fail_at=1: first alloc returns NULL */
        moq_alloc_t a = oom_allocator(&os);
        moq_msf_catalog_t out;
        CHECK(moq_msf_catalog_apply_delta(&a, &base, &delta, &out)
              == MOQ_ERR_NOMEM);
        CHECK(os.alloc_count == 0);   /* failed before any allocation */
    }

    /* apply_delta: the capacity ADDITION (base + add/clone track counts) must
     * also be guarded against size_t wrap before allocation. */
    {
        moq_msf_track_t rt;
        memset(&rt, 0, sizeof(rt));

        moq_msf_catalog_t base;
        memset(&base, 0, sizeof(base));
        base.struct_size = sizeof(base);
        base.version = MOQ_MSF_VERSION;
        base.tracks = NULL;
        base.track_count = SIZE_MAX - 2;

        moq_msf_delta_op_t op;
        memset(&op, 0, sizeof(op));
        op.op = MOQ_MSF_DELTA_OP_ADD;   /* cap += op.track_count -> wraps */
        op.tracks = &rt;
        op.track_count = 5;

        moq_msf_catalog_t delta;
        memset(&delta, 0, sizeof(delta));
        delta.struct_size = sizeof(delta);
        delta.delta_update = &op;
        delta.delta_update_count = 1;

        oom_state_t os = { 0, 0, 1 };
        moq_alloc_t a = oom_allocator(&os);
        moq_msf_catalog_t out;
        CHECK(moq_msf_catalog_apply_delta(&a, &base, &delta, &out)
              == MOQ_ERR_NOMEM);
        CHECK(os.alloc_count == 0);
    }

    printf("%s: %d failures\n", failures ? "FAIL" : "PASS", failures);
    return failures ? 1 : 0;
}
