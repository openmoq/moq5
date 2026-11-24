/*
 * The catalog's name hashing and matching (msf_names.c) executed on a
 * 32-bit target, where size_t is 32 bits wide. scripts/check_ilp32_headers.sh
 * builds this freestanding and static for i686, with undefined behaviour
 * trapping, and runs it where the host executes i686 code. It prints the
 * phase it is in, so a trap names the code that hit it, then PASS or FAIL.
 */
#include "../src/msf_names.c"

/* -- What msf_names.c needs from the rest of the library and from libc --- */

bool moq_msf_checked_array_bytes(size_t count, size_t elem, size_t *out_bytes)
{
    if (elem != 0 && count > SIZE_MAX / elem) return false;
    *out_bytes = count * elem;
    return true;
}

int memcmp(const void *a, const void *b, size_t n)
{
    const unsigned char *x = a, *y = b;
    for (size_t i = 0; i < n; i++)
        if (x[i] != y[i]) return x[i] < y[i] ? -1 : 1;
    return 0;
}

void *memcpy(void *dst, const void *src, size_t n)
{
    unsigned char *d = dst;
    const unsigned char *s = src;
    for (size_t i = 0; i < n; i++) d[i] = s[i];
    return dst;
}

void *memset(void *dst, int c, size_t n)
{
    unsigned char *d = dst;
    for (size_t i = 0; i < n; i++) d[i] = (unsigned char)c;
    return dst;
}

size_t strlen(const char *s)
{
    size_t n = 0;
    while (s[n]) n++;
    return n;
}

/* -- Process ------------------------------------------------------------- */

static long sys3(long nr, long a, long b, long c)
{
    long r;
    __asm__ volatile("int $0x80" : "=a"(r) : "a"(nr), "b"(a), "c"(b), "d"(c)
                     : "memory");
    return r;
}

static void say(const char *s)
{
    (void)sys3(4, 1, (long)s, (long)strlen(s));   /* write(1, ...) */
}

static _Noreturn void quit(int code)
{
    (void)sys3(1, code, 0, 0);                     /* exit */
    for (;;) {}
}

/* A bump allocator: the checks free nothing they need again. */
static unsigned char g_heap[1u << 16];
static size_t g_heap_used;

static void *bump_alloc(size_t n, void *ctx)
{
    (void)ctx;
    size_t at = (g_heap_used + 7u) & ~(size_t)7u;
    if (n > sizeof(g_heap) || at > sizeof(g_heap) - n) return NULL;
    g_heap_used = at + n;
    return g_heap + at;
}

static void *bump_realloc(void *p, size_t old_n, size_t new_n, void *ctx)
{
    (void)p; (void)old_n; (void)new_n; (void)ctx;
    return NULL;
}

static void bump_free(void *p, size_t n, void *ctx)
{
    (void)p; (void)n; (void)ctx;
}

static int g_failures;
#define CHECK(e) do { if (!(e)) g_failures++; } while (0)

static moq_bytes_t lit(const char *s)
{
    return (moq_bytes_t){ (const uint8_t *)s, strlen(s) };
}

/* -- Checks ---------------------------------------------------------------- */

#define NTRACKS 40
static char g_names[NTRACKS][8];
static char g_long_name[300];

/* Full Track Names hash alike however the namespace is written: a track
 * that inherits the catalog namespace x/y and a reference that writes "x/y"
 * find each other through the index, as do the reverse, for names short
 * and long. */
static void check_identity_hashing(void)
{
    moq_alloc_t a = { .ctx = NULL, .alloc = bump_alloc,
                      .realloc = bump_realloc, .free = bump_free };
    static const moq_bytes_t xy[2] = {
        { (const uint8_t *)"x", 1 }, { (const uint8_t *)"y", 1 },
    };
    msf_ident_mode_t fields = { .fields = true, .catalog_ns = { xy, 2 } };
    moq_msf_track_t t[NTRACKS];
    memset(t, 0, sizeof(t));
    for (size_t i = 0; i < sizeof(g_long_name) - 1; i++) g_long_name[i] = 'L';
    for (int i = 0; i < NTRACKS; i++) {
        g_names[i][0] = 'n';
        g_names[i][1] = (char)('0' + i / 10);
        g_names[i][2] = (char)('0' + i % 10);
        t[i].name = lit(g_names[i]);
        if (i % 2) { t[i].has_namespace = true; t[i].namespace_ = lit("x/y"); }
    }
    t[NTRACKS - 1].name = lit(g_long_name);
    msf_index_t ix;
    CHECK(moq_msf_index_init(&ix, NTRACKS, &fields, &a) == MOQ_OK);
    for (int i = 0; i < NTRACKS; i++) {
        CHECK(moq_msf_index_find(&ix, t, t[i].has_namespace, t[i].namespace_,
                                 t[i].name) < 0);
        moq_msf_index_insert(&ix, t, i);
    }
    moq_bytes_t none = { NULL, 0 };
    for (int i = 0; i < NTRACKS; i++) {
        CHECK(moq_msf_index_find(&ix, t, true, lit("x/y"), t[i].name) == i);
        CHECK(moq_msf_index_find(&ix, t, false, none, t[i].name) == i);
        CHECK(moq_msf_index_find(&ix, t, true, lit("x"), t[i].name) < 0);
        CHECK(moq_msf_index_find(&ix, t, true, lit("x/y/z"), t[i].name) < 0);
    }
    moq_msf_index_free(&ix, &a);

    /* MSF-01 compares the namespace as written. */
    msf_ident_mode_t written = { .fields = false, .catalog_ns = { NULL, 0 } };
    CHECK(moq_msf_index_init(&ix, NTRACKS, &written, &a) == MOQ_OK);
    for (int i = 0; i < NTRACKS; i++) moq_msf_index_insert(&ix, t, i);
    for (int i = 0; i < NTRACKS; i++) {
        CHECK(moq_msf_index_find(&ix, t, t[i].has_namespace, t[i].namespace_,
                                 t[i].name) == i);
        CHECK(moq_msf_index_find(&ix, t, !t[i].has_namespace, lit("x/y"),
                                 t[i].name) < 0);
    }
    moq_msf_index_free(&ix, &a);

    /* Keys counted by byte span. */
    msf_span_index_t sx;
    CHECK(moq_msf_span_index_init(&sx, 4, &a) == MOQ_OK);
    CHECK(moq_msf_span_index_add(&sx, lit(g_long_name)) == 1);
    CHECK(moq_msf_span_index_add(&sx, lit(g_long_name)) == 2);
    CHECK(moq_msf_span_index_count(&sx, lit("n00")) == 0);
    moq_msf_span_index_free(&sx, &a);
}

/* The public matcher on 32-bit, with malformed MOQT input among it. */
static void check_matcher(void)
{
    moq_msf_track_t in, ex;
    memset(&in, 0, sizeof(in));
    memset(&ex, 0, sizeof(ex));
    in.name = lit("in");
    ex.name = lit("ex");
    ex.has_namespace = true;
    ex.namespace_ = lit("a/b");
    moq_bytes_t ab[2] = { lit("a"), lit("b") };
    moq_bytes_t slash[1] = { lit("a/b") };
    moq_bytes_t null_field[1] = { { NULL, 1 } };
    moq_namespace_t ns_ab = { ab, 2 }, ns_slash = { slash, 1 };
    moq_namespace_t ns_null = { null_field, 1 }, none = { NULL, 0 };
    CHECK(moq_msf_track_matches(&ex, none, ns_ab, lit("ex")));
    CHECK(!moq_msf_track_matches(&ex, none, ns_slash, lit("ex")));
    CHECK(moq_msf_track_matches(&in, ns_ab, ns_ab, lit("in")));
    CHECK(moq_msf_track_matches(&in, ns_slash, ns_slash, lit("in")));
    CHECK(!moq_msf_track_matches(&in, ns_slash, ns_ab, lit("in")));
    CHECK(!moq_msf_track_matches(&ex, none, ns_null, lit("ex")));
    CHECK(!moq_msf_track_matches(&in, ns_null, ns_null, lit("in")));
}

__attribute__((force_align_arg_pointer)) void _start(void)
{
    say("sizes\n");
    CHECK(sizeof(size_t) == 4 && sizeof(void *) == 4);
    say("identity hashing\n");
    check_identity_hashing();
    say("matcher\n");
    check_matcher();
    say(g_failures ? "FAIL\n" : "PASS\n");
    quit(g_failures ? 1 : 0);
}
