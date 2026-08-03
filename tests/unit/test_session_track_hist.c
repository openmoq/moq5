/*
 * Per-track largest-location history registry (Design v8 §1).
 *
 * Covers the foundation: the public moq_session_note_object_published API
 * and the moq_session_cfg_t.max_track_history_records capacity field, plus the
 * internal reserve/release/merge lifecycle (empty-reservation reclamation,
 * observed-record persistence, rollback) via moq-core-test-internals.
 */
#include <moq/codec.h>
#include "test_session_support.h"
#include "../../core/src/session/session_internal.h"
#include <moq/wire.h>
#include <string.h>

/* Build a one-part namespace + name; caller supplies storage. */
static moq_namespace_t ns_of(moq_bytes_t *part, const char *s)
{
    part->data = (const uint8_t *)s;
    part->len = strlen(s);
    moq_namespace_t ns = { part, 1 };
    return ns;
}

/* -- Fail-injecting allocator (counts allocs, fails the Nth onward) --- */
typedef struct {
    int64_t balance;
    int     alloc_calls;
    int     fail_at;      /* 0 = never fail; else fail on call index >= fail_at */
} th_fail_state_t;

static void *fail_alloc(size_t size, void *ctx)
{
    th_fail_state_t *st = (th_fail_state_t *)ctx;
    st->alloc_calls++;
    if (st->fail_at && st->alloc_calls >= st->fail_at) return NULL;
    void *p = malloc(size);
    if (p) st->balance++;
    return p;
}
static void *fail_realloc(void *ptr, size_t o, size_t n, void *ctx)
{ (void)o; (void)ctx; return realloc(ptr, n); }
static void fail_free(void *ptr, size_t size, void *ctx)
{
    th_fail_state_t *st = (th_fail_state_t *)ctx;
    (void)size;
    if (ptr) st->balance--;
    free(ptr);
}
static moq_alloc_t th_fail_allocator(th_fail_state_t *st)
{
    moq_alloc_t a = { st, fail_alloc, fail_realloc, fail_free };
    return a;
}

/* Look up the record for (ns,name) using the shared canonical key. */
static moq_track_hist_t *find_rec(moq_session_t *s, const moq_namespace_t *ns,
                                  moq_bytes_t name)
{
    size_t klen = 0;
    uint8_t *key = moq_build_track_key(s, ns, name, &klen);
    if (!key && klen > 0) return NULL;
    moq_track_hist_t *r = track_hist_find(s, key, klen);
    if (key) s->alloc.free(key, klen, s->alloc.ctx);
    return r;
}

int main(void)
{
    int failures = 0;

    /* == cfg canary: old-size cfg gets the default cap ================ *
     * Discriminating under ASan: the cfg is stored in an allocation sized
     * EXACTLY to the pre-field prefix, so any out-of-prefix read of
     * max_track_history_records during create is a heap-buffer-overflow, not
     * a zero from a full-struct tail. */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        const size_t old_size =
            offsetof(moq_session_cfg_t, max_track_history_records);
        moq_session_cfg_t full;
        memset(&full, 0, sizeof(full));
        full.struct_size = (uint32_t)old_size;   /* claims the old, shorter ABI */
        full.alloc = &alloc;
        full.perspective = MOQ_PERSPECTIVE_CLIENT;

        void *raw = malloc(old_size);            /* exact old size, no tail */
        MOQ_TEST_CHECK(raw != NULL);
        memcpy(raw, &full, old_size);
        moq_session_t *s = NULL;
        MOQ_TEST_CHECK(
            moq_session_create((const moq_session_cfg_t *)raw, 0, &s) == MOQ_OK);
        MOQ_TEST_CHECK(s != NULL);
        if (s) {
            MOQ_TEST_CHECK_EQ_SIZE(s->th_cap, (size_t)(64 + 16 + 8));
            moq_session_destroy(s);
        }
        free(raw);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == full-size cfg honours an explicit cap ======================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t cfg = MOQ_SESSION_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
        cfg.max_track_history_records = 5;
        moq_session_t *s = NULL;
        MOQ_TEST_CHECK(moq_session_create(&cfg, 0, &s) == MOQ_OK);
        if (s) {
            MOQ_TEST_CHECK_EQ_SIZE(s->th_cap, (size_t)5);
            moq_session_destroy(s);
        }
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == public note: success, max-merge, idempotency ================= */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t p;
        moq_namespace_t ns = ns_of(&p, "ns");
        moq_bytes_t name = MOQ_BYTES_LITERAL("t");

        /* First observation records the location. */
        MOQ_TEST_CHECK(moq_session_note_object_published(sv, &ns, name, 3, 7)
            == MOQ_OK);
        moq_track_hist_t *r = find_rec(sv, &ns, name);
        MOQ_TEST_CHECK(r != NULL);
        MOQ_TEST_CHECK(r && r->has_largest);
        MOQ_TEST_CHECK(r && r->largest_group == 3 && r->largest_object == 7);

        /* A strictly greater location advances (object, then group). */
        MOQ_TEST_CHECK(moq_session_note_object_published(sv, &ns, name, 3, 9)
            == MOQ_OK);
        r = find_rec(sv, &ns, name);
        MOQ_TEST_CHECK(r && r->largest_group == 3 && r->largest_object == 9);
        MOQ_TEST_CHECK(moq_session_note_object_published(sv, &ns, name, 5, 0)
            == MOQ_OK);
        r = find_rec(sv, &ns, name);
        MOQ_TEST_CHECK(r && r->largest_group == 5 && r->largest_object == 0);

        /* A lesser-or-equal location is an idempotent no-op success. */
        MOQ_TEST_CHECK(moq_session_note_object_published(sv, &ns, name, 5, 0)
            == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_note_object_published(sv, &ns, name, 4, 99)
            == MOQ_OK);
        r = find_rec(sv, &ns, name);
        MOQ_TEST_CHECK(r && r->largest_group == 5 && r->largest_object == 0);

        /* A distinct track name gets its own record. */
        moq_bytes_t name2 = MOQ_BYTES_LITERAL("t2");
        MOQ_TEST_CHECK(moq_session_note_object_published(sv, &ns, name2, 1, 1)
            == MOQ_OK);
        moq_track_hist_t *r2 = find_rec(sv, &ns, name2);
        MOQ_TEST_CHECK(r2 != NULL && r2 != r);
        MOQ_TEST_CHECK(r2 && r2->largest_group == 1);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == canonical key is copied (not borrowed) ======================= */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        char namebuf[] = "vol";
        moq_bytes_t p;
        moq_namespace_t ns = ns_of(&p, "ns");
        moq_bytes_t name = { (const uint8_t *)namebuf, 3 };
        MOQ_TEST_CHECK(moq_session_note_object_published(sv, &ns, name, 2, 2)
            == MOQ_OK);
        /* Mutating the caller's buffer must not perturb the stored key. */
        namebuf[0] = 'X';
        moq_bytes_t orig = MOQ_BYTES_LITERAL("vol");
        moq_track_hist_t *r = find_rec(sv, &ns, orig);
        MOQ_TEST_CHECK(r != NULL);
        MOQ_TEST_CHECK(r && r->largest_group == 2 && r->largest_object == 2);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == invalid state + varint bounds ================================ */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        /* Fresh, not-yet-active session: note is WRONG_STATE. */
        moq_session_cfg_t cfg = MOQ_SESSION_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
        moq_session_t *s = NULL;
        MOQ_TEST_CHECK(moq_session_create(&cfg, 0, &s) == MOQ_OK);
        moq_bytes_t p;
        moq_namespace_t ns = ns_of(&p, "ns");
        moq_bytes_t name = MOQ_BYTES_LITERAL("t");
        MOQ_TEST_CHECK(moq_session_note_object_published(s, &ns, name, 0, 0)
            == MOQ_ERR_WRONG_STATE);
        MOQ_TEST_CHECK(find_rec(s, &ns, name) == NULL);
        moq_session_destroy(s);

        /* Active session: out-of-range locations are INVAL, no record. */
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);
        MOQ_TEST_CHECK(moq_session_note_object_published(
            sv, &ns, name, MOQ_QUIC_VARINT_MAX + 1, 0) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_session_note_object_published(
            sv, &ns, name, 0, MOQ_QUIC_VARINT_MAX + 1) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(find_rec(sv, &ns, name) == NULL);
        /* NULL args. */
        MOQ_TEST_CHECK(moq_session_note_object_published(NULL, &ns, name, 0, 0)
            == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_session_note_object_published(sv, NULL, name, 0, 0)
            == MOQ_ERR_INVAL);
        /* Exactly at the ceiling is valid. */
        MOQ_TEST_CHECK(moq_session_note_object_published(
            sv, &ns, name, MOQ_QUIC_VARINT_MAX, MOQ_QUIC_VARINT_MAX) == MOQ_OK);
        moq_track_hist_t *r = find_rec(sv, &ns, name);
        MOQ_TEST_CHECK(r && r->largest_group == MOQ_QUIC_VARINT_MAX);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == hard capacity bound: full registry fails, no record ========== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t extra = MOQ_SESSION_CFG_INIT;
        extra.max_track_history_records = 2;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &extra);
        MOQ_TEST_CHECK_EQ_SIZE(sv->th_cap, (size_t)2);

        moq_bytes_t p;
        moq_namespace_t ns = ns_of(&p, "ns");
        MOQ_TEST_CHECK(moq_session_note_object_published(
            sv, &ns, MOQ_BYTES_LITERAL("a"), 1, 0) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_note_object_published(
            sv, &ns, MOQ_BYTES_LITERAL("b"), 1, 0) == MOQ_OK);
        /* Third distinct track cannot get a record. */
        MOQ_TEST_CHECK(moq_session_note_object_published(
            sv, &ns, MOQ_BYTES_LITERAL("c"), 1, 0) == MOQ_ERR_NOMEM);
        MOQ_TEST_CHECK(find_rec(sv, &ns, MOQ_BYTES_LITERAL("c")) == NULL);
        /* An already-recorded track still merges (no new record needed). */
        MOQ_TEST_CHECK(moq_session_note_object_published(
            sv, &ns, MOQ_BYTES_LITERAL("a"), 9, 0) == MOQ_OK);
        moq_track_hist_t *ra = find_rec(sv, &ns, MOQ_BYTES_LITERAL("a"));
        MOQ_TEST_CHECK(ra && ra->largest_group == 9);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == internal lifecycle: reservation reclamation, rollback,
     *    observed-record persistence, and no permanent exhaustion ===== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t extra = MOQ_SESSION_CFG_INIT;
        extra.max_track_history_records = 2;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &extra);

        moq_bytes_t p;
        moq_namespace_t ns = ns_of(&p, "ns");
        size_t kl = 0;
        uint8_t *ka = moq_build_track_key(sv, &ns, MOQ_BYTES_LITERAL("a"), &kl);
        MOQ_TEST_CHECK(ka != NULL);

        /* Reserve then release an empty record: reclaimed (rollback). */
        moq_track_hist_t *r = track_hist_reserve(sv, ka, kl);
        MOQ_TEST_CHECK(r != NULL && r->refs == 1 && !r->has_largest);
        track_hist_release(sv, r);
        MOQ_TEST_CHECK(track_hist_find(sv, ka, kl) == NULL);   /* reclaimed */

        /* Double reserve shares one record; releasing once keeps it. */
        moq_track_hist_t *r1 = track_hist_reserve(sv, ka, kl);
        moq_track_hist_t *r2 = track_hist_reserve(sv, ka, kl);
        MOQ_TEST_CHECK(r1 == r2 && r1->refs == 2);
        track_hist_release(sv, r1);
        MOQ_TEST_CHECK(track_hist_find(sv, ka, kl) != NULL && r1->refs == 1);

        /* Observing a largest pins the record past its last release. */
        track_hist_merge(r1, 4, 4);
        track_hist_release(sv, r1);
        moq_track_hist_t *pinned = track_hist_find(sv, ka, kl);
        MOQ_TEST_CHECK(pinned != NULL);          /* pinned by has_largest */
        MOQ_TEST_CHECK(pinned && pinned->refs == 0 && pinned->has_largest);

        /* Cap is 2, one slot pinned. Empty churn on the other slot must not
         * permanently consume capacity: reserve/release many distinct names. */
        for (int i = 0; i < 5; i++) {
            char nm[8];
            nm[0] = 'x'; nm[1] = (char)('0' + i); nm[2] = 0;
            size_t kl2 = 0;
            moq_bytes_t nb = { (const uint8_t *)nm, 2 };
            uint8_t *k2 = moq_build_track_key(sv, &ns, nb, &kl2);
            MOQ_TEST_CHECK(k2 != NULL);
            moq_track_hist_t *rr = track_hist_reserve(sv, k2, kl2);
            MOQ_TEST_CHECK(rr != NULL);          /* free slot always available */
            track_hist_release(sv, rr);          /* reclaimed immediately */
            if (k2) sv->alloc.free(k2, kl2, sv->alloc.ctx);
        }

        if (ka) sv->alloc.free(ka, kl, sv->alloc.ctx);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == malformed track names rejected, no state change ============== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t name = MOQ_BYTES_LITERAL("t");

        /* NULL parts with count 1. */
        moq_namespace_t ns_nullparts = { NULL, 1 };
        MOQ_TEST_CHECK(moq_session_note_object_published(
            sv, &ns_nullparts, name, 1, 1) == MOQ_ERR_INVAL);

        /* Zero-count namespace. */
        moq_bytes_t p;
        moq_namespace_t ns_ok = ns_of(&p, "ns");
        moq_namespace_t ns_zero = { ns_ok.parts, 0 };
        MOQ_TEST_CHECK(moq_session_note_object_published(
            sv, &ns_zero, name, 1, 1) == MOQ_ERR_INVAL);

        /* Component with nonzero length but NULL data. */
        moq_bytes_t bad_part = { NULL, 3 };
        moq_namespace_t ns_badpart = { &bad_part, 1 };
        MOQ_TEST_CHECK(moq_session_note_object_published(
            sv, &ns_badpart, name, 1, 1) == MOQ_ERR_INVAL);

        /* Track name with nonzero length but NULL data. */
        moq_bytes_t bad_name = { NULL, 4 };
        MOQ_TEST_CHECK(moq_session_note_object_published(
            sv, &ns_ok, bad_name, 1, 1) == MOQ_ERR_INVAL);

        /* No record was created for any rejected call, and no valid one
         * leaked: the registry is still empty for the good namespace. */
        MOQ_TEST_CHECK(find_rec(sv, &ns_ok, name) == NULL);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == borrowed inputs copied: mutate a namespace component ========== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        char nsbuf[] = "room";
        moq_bytes_t part = { (const uint8_t *)nsbuf, 4 };
        moq_namespace_t ns = { &part, 1 };
        moq_bytes_t name = MOQ_BYTES_LITERAL("t");
        MOQ_TEST_CHECK(moq_session_note_object_published(sv, &ns, name, 6, 1)
            == MOQ_OK);
        /* Mutate the caller's namespace-component bytes after the call. */
        nsbuf[0] = 'Z';
        moq_bytes_t orig_part = MOQ_BYTES_LITERAL("room");
        moq_namespace_t ns_orig = { &orig_part, 1 };
        moq_track_hist_t *r = find_rec(sv, &ns_orig, name);
        MOQ_TEST_CHECK(r != NULL);
        MOQ_TEST_CHECK(r && r->largest_group == 6 && r->largest_object == 1);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == post-close note is WRONG_STATE ================================ */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_session_close(sv, 0, NULL, 0);
        moq_bytes_t p;
        moq_namespace_t ns = ns_of(&p, "ns");
        moq_bytes_t name = MOQ_BYTES_LITERAL("t");
        MOQ_TEST_CHECK(moq_session_note_object_published(sv, &ns, name, 1, 1)
            == MOQ_ERR_WRONG_STATE);
        MOQ_TEST_CHECK(find_rec(sv, &ns, name) == NULL);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == allocation-failure atomicity: note key build + record key copy = *
     * Inspection uses a canonical key built up-front with injection OFF and
     * queried via track_hist_find() (no allocation), so a failing find_rec
     * cannot mask a leaked/mutated record. */
    {
        th_fail_state_t fs = {0};
        moq_alloc_t alloc = th_fail_allocator(&fs);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t p;
        moq_namespace_t ns = ns_of(&p, "ns");
        moq_bytes_t name = MOQ_BYTES_LITERAL("t");

        /* Pre-build the inspection key while allocation still works. */
        size_t kl = 0;
        uint8_t *k = moq_build_track_key(sv, &ns, name, &kl);
        MOQ_TEST_CHECK(k != NULL);

        /* (a) key-build alloc fails: NOMEM, no record created. */
        fs.fail_at = fs.alloc_calls + 1;
        MOQ_TEST_CHECK(moq_session_note_object_published(sv, &ns, name, 2, 2)
            == MOQ_ERR_NOMEM);
        fs.fail_at = 0;
        MOQ_TEST_CHECK(track_hist_find(sv, k, kl) == NULL);

        /* (b) key-copy alloc fails (2nd alloc in the create path): NOMEM,
         * still no record. */
        fs.fail_at = fs.alloc_calls + 2;
        MOQ_TEST_CHECK(moq_session_note_object_published(sv, &ns, name, 2, 2)
            == MOQ_ERR_NOMEM);
        fs.fail_at = 0;
        MOQ_TEST_CHECK(track_hist_find(sv, k, kl) == NULL);

        /* Seed a record (injection off), then prove a failed advance leaves
         * the stored largest unchanged (atomic). */
        MOQ_TEST_CHECK(moq_session_note_object_published(sv, &ns, name, 2, 2)
            == MOQ_OK);
        moq_track_hist_t *r = track_hist_find(sv, k, kl);
        MOQ_TEST_CHECK(r && r->largest_group == 2 && r->largest_object == 2);

        /* (c) advance attempt whose key-build alloc fails: NOMEM, largest
         * unchanged. */
        fs.fail_at = fs.alloc_calls + 1;
        MOQ_TEST_CHECK(moq_session_note_object_published(sv, &ns, name, 9, 9)
            == MOQ_ERR_NOMEM);
        fs.fail_at = 0;
        r = track_hist_find(sv, k, kl);
        MOQ_TEST_CHECK(r && r->largest_group == 2 && r->largest_object == 2);

        /* Retry with injection off advances. */
        MOQ_TEST_CHECK(moq_session_note_object_published(sv, &ns, name, 9, 9)
            == MOQ_OK);
        r = track_hist_find(sv, k, kl);
        MOQ_TEST_CHECK(r && r->largest_group == 9 && r->largest_object == 9);

        if (k) sv->alloc.free(k, kl, sv->alloc.ctx);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(fs.balance == 0);
    }

    /* == allocation-failure at session create: registry pool =========== */
    {
        /* Count allocations of a successful create, then fail the last one
         * (the registry pool is allocated last), and confirm create fails
         * NOMEM with *out reset to NULL and the arena fully freed. */
        th_fail_state_t probe = {0};
        moq_alloc_t palloc = th_fail_allocator(&probe);
        moq_session_cfg_t cfg = MOQ_SESSION_CFG_INIT;
        cfg.alloc = &palloc;
        cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
        moq_session_t *s = NULL;
        MOQ_TEST_CHECK(moq_session_create(&cfg, 0, &s) == MOQ_OK);
        int total_allocs = probe.alloc_calls;
        moq_session_destroy(s);
        MOQ_TEST_CHECK(probe.balance == 0);

        th_fail_state_t fs = {0};
        moq_alloc_t alloc = th_fail_allocator(&fs);
        fs.fail_at = total_allocs;      /* fail the last create-time alloc */
        moq_session_cfg_t cfg2 = MOQ_SESSION_CFG_INIT;
        cfg2.alloc = &alloc;
        cfg2.perspective = MOQ_PERSPECTIVE_CLIENT;
        moq_session_t *s2 = (moq_session_t *)0x1;   /* poison: must be reset */
        moq_result_t rc = moq_session_create(&cfg2, 0, &s2);
        MOQ_TEST_CHECK(rc == MOQ_ERR_NOMEM);
        MOQ_TEST_CHECK(s2 == NULL);        /* create resets *out to NULL */
        MOQ_TEST_CHECK(fs.balance == 0);   /* arena fully unwound */

        /* Retry with injection off succeeds. */
        th_fail_state_t fs2 = {0};
        moq_alloc_t alloc2 = th_fail_allocator(&fs2);
        moq_session_cfg_t cfg3 = MOQ_SESSION_CFG_INIT;
        cfg3.alloc = &alloc2;
        cfg3.perspective = MOQ_PERSPECTIVE_CLIENT;
        moq_session_t *s3 = NULL;
        MOQ_TEST_CHECK(moq_session_create(&cfg3, 0, &s3) == MOQ_OK);
        moq_session_destroy(s3);
        MOQ_TEST_CHECK(fs2.balance == 0);
    }

    if (failures == 0) printf("PASS\n");
    else printf("FAIL: %d checks failed\n", failures);
    return failures ? 1 : 0;
}
