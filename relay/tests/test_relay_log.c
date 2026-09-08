/*
 * Track-log primitive tests: track log + cursors + eviction + capacity + trace.
 *
 * Every mutating step is followed by moqr_log_capacity_check (the "cannot
 * OOM" invariant is asserted continuously, not sampled), and every test
 * ends with allocator balance zero.
 */

#include <moq/relay/capacity.h>
#include <moq/relay/log.h>
#include <moq/relay/trace.h>

#include <moq/rcbuf.h>

#include "log_internal.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../../tests/unit/test_support.h"

/* -- counting allocator ------------------------------------------------- */

typedef struct count_alloc {
    moq_alloc_t vt;
    long        allocs;
    long        frees;
    long        live_bytes;
    long        peak_bytes;
} count_alloc_t;

/* An upstream terminal arrives in some connection's draft; these rigs speak
 * draft-16 unless a case says otherwise. */
static moqr_reset_desc_t
rd_wire(uint64_t code)
{
    moqr_reset_desc_t d;

    if (moqr_reset_desc_wire(MOQ_VERSION_DRAFT_16, code, &d) != MOQR_OK) {
        return moqr_reset_desc_none();
    }
    return d;
}

static void *
ca_alloc(size_t size, void *ctx)
{
    count_alloc_t *ca = ctx;
    void *p = malloc(size);
    if (p != NULL) {
        ca->allocs++;
        ca->live_bytes += (long)size;
        if (ca->live_bytes > ca->peak_bytes) {
            ca->peak_bytes = ca->live_bytes;
        }
    }
    return p;
}

static void *
ca_realloc(void *ptr, size_t old_size, size_t new_size, void *ctx)
{
    count_alloc_t *ca = ctx;
    void *p = realloc(ptr, new_size);
    if (p != NULL) {
        ca->live_bytes += (long)new_size - (long)old_size;
        if (ca->live_bytes > ca->peak_bytes) {
            ca->peak_bytes = ca->live_bytes;
        }
    }
    return p;
}

static void
ca_free(void *ptr, size_t size, void *ctx)
{
    count_alloc_t *ca = ctx;
    if (ptr != NULL) {
        ca->frees++;
        ca->live_bytes -= (long)size;
    }
    free(ptr);
}

static void
ca_init(count_alloc_t *ca)
{
    memset(ca, 0, sizeof(*ca));
    ca->vt.ctx = ca;
    ca->vt.alloc = ca_alloc;
    ca->vt.realloc = ca_realloc;
    ca->vt.free = ca_free;
}

/* -- helpers -------------------------------------------------------------- */

static int g_release_count;

static void
count_release(void *ctx, const uint8_t *data, size_t len)
{
    (void)ctx;
    (void)data;
    (void)len;
    g_release_count++;
}

static moq_rcbuf_t *
mkbuf(const moq_alloc_t *a, size_t len, uint8_t fill)
{
    uint8_t tmp[512];
    if (len > sizeof(tmp)) {
        return NULL;
    }
    memset(tmp, fill, len);
    moq_rcbuf_t *b = NULL;
    if (moq_rcbuf_create(a, tmp, len, &b) != 0) {
        return NULL;
    }
    return b;
}

static int g_failures_capacity;   /* folded into per-test failures */

#define STEP_CHECK(log)                                             \
    do {                                                            \
        const char *why = NULL;                                     \
        if (moqr_log_capacity_check((log), &why) != MOQR_OK) {      \
            printf("FAIL: %s:%d: capacity check: %s\n", __FILE__,   \
                   __LINE__, why);                                  \
            g_failures_capacity++;                                  \
        }                                                           \
    } while (0)

static moqr_result_t
append_obj(moqr_log_t *log, const moq_alloc_t *a, uint64_t group,
           uint64_t subgroup, uint64_t object, size_t len, bool eog,
           moqr_obj_status_t status, bool datagram)
{
    moqr_log_append_desc_t d;
    moqr_log_append_desc_init(&d);
    d.group_id = group;
    d.subgroup_id = subgroup;
    d.object_id = object;
    d.publisher_priority = 128;
    d.status = status;
    d.datagram_pref = datagram;
    d.end_of_group = eog;
    d.payload = status == MOQR_OBJ_NORMAL ? mkbuf(a, len, (uint8_t)object)
                                          : NULL;
    d.now_us = 1000 + object;
    moqr_result_t rc = moqr_log_append(log, &d);
    if (rc != MOQR_OK) {
        moq_rcbuf_decref(d.payload);   /* refused: caller keeps refs */
    }
    return rc;
}

static moqr_log_t *
mklog(count_alloc_t *ca, uint32_t max_groups, uint64_t max_bytes,
      moqr_trace_t *trace)
{
    moqr_log_cfg_t cfg;
    moqr_log_cfg_init_sized(&cfg, sizeof(cfg), &ca->vt);
    cfg.budget.max_groups = max_groups;
    cfg.budget.max_bytes = max_bytes;
    cfg.trace = trace;
    cfg.trace_id = 42;
    moqr_log_t *log = NULL;
    if (moqr_log_create(&cfg, &log) != MOQR_OK) {
        return NULL;
    }
    return log;
}

/* -- tests ----------------------------------------------------------------- */

static int
test_append_and_cursor_roundtrip(void)
{
    int failures = 0;
    count_alloc_t ca;
    ca_init(&ca);

    moqr_log_t *log = mklog(&ca, 4, 1 << 20, NULL);
    MOQ_TEST_CHECK(log != NULL);

    /* Two groups, two subgroups each, plus datagrams. */
    MOQ_TEST_CHECK(append_obj(log, &ca.vt, 10, 0, 0, 100, false,
                              MOQR_OBJ_NORMAL, false) == MOQR_OK);
    STEP_CHECK(log);
    MOQ_TEST_CHECK(append_obj(log, &ca.vt, 10, 1, 1, 50, false,
                              MOQR_OBJ_NORMAL, false) == MOQR_OK);
    MOQ_TEST_CHECK(append_obj(log, &ca.vt, 10, 0, 2, 100, false,
                              MOQR_OBJ_NORMAL, false) == MOQR_OK);
    MOQ_TEST_CHECK(append_obj(log, &ca.vt, 10, 0, 5, 10, false,
                              MOQR_OBJ_NORMAL, true) == MOQR_OK);
    MOQ_TEST_CHECK(append_obj(log, &ca.vt, 11, 0, 0, 100, false,
                              MOQR_OBJ_NORMAL, false) == MOQR_OK);
    STEP_CHECK(log);

    /* Arrival-order read. */
    moqr_cursor_t cur = MOQR_CURSOR_INVALID;
    MOQ_TEST_CHECK(moqr_log_cursor_open(log, MOQR_CURSOR_FROM_OLDEST,
                                        &cur) == MOQR_OK);
    const uint64_t want_obj[] = { 0, 1, 2, 5, 0 };
    const uint64_t want_grp[] = { 10, 10, 10, 10, 11 };
    for (size_t i = 0; i < 5; i++) {
        moqr_record_view_t v;
        bool skipped = true;
        MOQ_TEST_CHECK(moqr_log_cursor_next(log, cur, &v, &skipped) ==
                       MOQR_OK);
        MOQ_TEST_CHECK(!skipped);
        MOQ_TEST_CHECK_EQ_U64(v.group_id, want_grp[i]);
        MOQ_TEST_CHECK_EQ_U64(v.object_id, want_obj[i]);
        MOQ_TEST_CHECK_EQ_U64(v.arrival_seq, i);
        /* append_obj set now_us = 1000 + object; the ingest time carries
         * through the record to the view for downstream latency. */
        MOQ_TEST_CHECK_EQ_U64(v.arrival_us, 1000 + v.object_id);
        if (i == 3) {
            MOQ_TEST_CHECK(v.datagram_pref);
        }
        if (v.payload != NULL) {
            MOQ_TEST_CHECK(moq_rcbuf_data(v.payload)[0] ==
                           (uint8_t)v.object_id);
        }
    }
    moqr_record_view_t v;
    bool skipped = false;
    MOQ_TEST_CHECK(moqr_log_cursor_next(log, cur, &v, &skipped) ==
                   MOQR_DONE);
    MOQ_TEST_CHECK(!skipped);

    /* Live cursor only sees post-open records. */
    moqr_cursor_t live = MOQR_CURSOR_INVALID;
    MOQ_TEST_CHECK(moqr_log_cursor_open(log, MOQR_CURSOR_FROM_LIVE,
                                        &live) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_log_cursor_next(log, live, &v, &skipped) ==
                   MOQR_DONE);
    MOQ_TEST_CHECK(append_obj(log, &ca.vt, 11, 0, 1, 20, false,
                              MOQR_OBJ_NORMAL, false) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_log_cursor_next(log, live, &v, &skipped) ==
                   MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(v.object_id, 1);

    /* Stats sanity. */
    moqr_log_stats_t st;
    moqr_log_get_stats(log, &st);
    MOQ_TEST_CHECK_EQ_U64(st.group_count, 2);
    MOQ_TEST_CHECK_EQ_U64(st.record_count, 6);
    MOQ_TEST_CHECK_EQ_U64(st.cursor_count, 2);
    MOQ_TEST_CHECK_EQ_U64(st.oldest_group_id, 10);
    MOQ_TEST_CHECK_EQ_U64(st.newest_group_id, 11);

    MOQ_TEST_CHECK(moqr_log_cursor_close(log, cur) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_log_cursor_next(log, cur, &v, &skipped) ==
                   MOQR_ERR_STALE_HANDLE);
    moqr_log_destroy(log);
    MOQ_TEST_CHECK_EQ_INT((int)ca.live_bytes, 0);
    MOQ_TEST_CHECK_EQ_INT((int)(ca.allocs - ca.frees), 0);
    MOQ_TEST_PASS("append_and_cursor_roundtrip");
    return failures;
}

static int
test_ordering_rules(void)
{
    int failures = 0;
    count_alloc_t ca;
    ca_init(&ca);
    moqr_log_t *log = mklog(&ca, 4, 1 << 20, NULL);
    MOQ_TEST_CHECK(log != NULL);

    MOQ_TEST_CHECK(append_obj(log, &ca.vt, 1, 0, 5, 10, false,
                              MOQR_OBJ_NORMAL, false) == MOQR_OK);
    /* Non-ascending within subgroup: equal and lower both refused. */
    MOQ_TEST_CHECK(append_obj(log, &ca.vt, 1, 0, 5, 10, false,
                              MOQR_OBJ_NORMAL, false) == MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(append_obj(log, &ca.vt, 1, 0, 3, 10, false,
                              MOQR_OBJ_NORMAL, false) == MOQR_ERR_INVAL);
    /* Another subgroup starts fresh. */
    MOQ_TEST_CHECK(append_obj(log, &ca.vt, 1, 7, 3, 10, false,
                              MOQR_OBJ_NORMAL, false) == MOQR_OK);
    /* Datagrams: out-of-order allowed, duplicates refused. */
    MOQ_TEST_CHECK(append_obj(log, &ca.vt, 1, 0, 9, 10, false,
                              MOQR_OBJ_NORMAL, true) == MOQR_OK);
    MOQ_TEST_CHECK(append_obj(log, &ca.vt, 1, 0, 7, 10, false,
                              MOQR_OBJ_NORMAL, true) == MOQR_OK);
    MOQ_TEST_CHECK(append_obj(log, &ca.vt, 1, 0, 9, 10, false,
                              MOQR_OBJ_NORMAL, true) == MOQR_ERR_INVAL);
    STEP_CHECK(log);

    /* Status objects carry no payload. */
    moqr_log_append_desc_t d;
    moqr_log_append_desc_init(&d);
    d.group_id = 1;
    d.subgroup_id = 0;
    d.object_id = 100;
    d.status = MOQR_OBJ_END_OF_GROUP;
    d.payload = mkbuf(&ca.vt, 4, 0xEE);
    MOQ_TEST_CHECK(moqr_log_append(log, &d) == MOQR_ERR_INVAL);
    moq_rcbuf_decref(d.payload);

    /* The subgroup-header EOG bit is subgroup METADATA, not a per-record
     * terminal: objects keep flowing on the marked subgroup until its FIN,
     * and the SEAL then resolves the group's final object to the subgroup's
     * LAST retained object. */
    MOQ_TEST_CHECK(append_obj(log, &ca.vt, 1, 0, 20, 10, true,
                              MOQR_OBJ_NORMAL, false) == MOQR_OK);
    MOQ_TEST_CHECK(append_obj(log, &ca.vt, 1, 0, 21, 10, false,
                              MOQR_OBJ_NORMAL, false) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_log_seal_subgroup(log, 1, 0) == MOQR_OK);
    /* final resolved to 21: a higher object on the group is refused... */
    MOQ_TEST_CHECK(append_obj(log, &ca.vt, 1, 0, 22, 10, false,
                              MOQR_OBJ_NORMAL, false) == MOQR_ERR_INVAL);
    /* ...while at-or-below the final is still admissible (other subgroup). */
    MOQ_TEST_CHECK(append_obj(log, &ca.vt, 1, 8, 15, 10, false,
                              MOQR_OBJ_NORMAL, false) == MOQR_OK);
    STEP_CHECK(log);

    /* END_OF_TRACK closes the track. */
    MOQ_TEST_CHECK(append_obj(log, &ca.vt, 2, 0, 4, 0, false,
                              MOQR_OBJ_END_OF_TRACK, false) == MOQR_OK);
    MOQ_TEST_CHECK(append_obj(log, &ca.vt, 3, 0, 0, 10, false,
                              MOQR_OBJ_NORMAL, false) == MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(append_obj(log, &ca.vt, 2, 0, 9, 10, false,
                              MOQR_OBJ_NORMAL, false) == MOQR_ERR_INVAL);
    STEP_CHECK(log);

    moqr_log_stats_t st;
    moqr_log_get_stats(log, &st);
    MOQ_TEST_CHECK(st.end_of_track);

    moqr_log_destroy(log);
    MOQ_TEST_CHECK_EQ_INT((int)ca.live_bytes, 0);
    MOQ_TEST_PASS("ordering_rules");
    return failures;
}

/* Every final-evidence source preflights already-retained content before
 * committing, and a contradiction rejects ATOMICALLY (nothing admitted, no
 * seal, no final): the cache must never retain an object or group beyond a
 * declared final location. Sources: header-EOG seal, END_OF_GROUP status,
 * EOG datagram, END_OF_TRACK. Controls prove valid finals still commit and
 * enforce. */
static int
test_final_evidence_preflight(void)
{
    int failures = 0;
    count_alloc_t ca;
    ca_init(&ca);
    moqr_log_t *log = mklog(&ca, 8, 1 << 20, NULL);
    MOQ_TEST_CHECK(log != NULL);

    /* (a) header-EOG seal vs retained higher object. */
    MOQ_TEST_CHECK(append_obj(log, &ca.vt, 1, 0, 20, 10, true,
                              MOQR_OBJ_NORMAL, false) == MOQR_OK);
    MOQ_TEST_CHECK(append_obj(log, &ca.vt, 1, 8, 21, 10, false,
                              MOQR_OBJ_NORMAL, false) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_log_seal_subgroup(log, 1, 0) == MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(!moqr_log_subgroup_is_final(log, 1, 0, 20)); /* unsealed */
    MOQ_TEST_CHECK(append_obj(log, &ca.vt, 1, 8, 25, 10, false,
                              MOQR_OBJ_NORMAL, false) == MOQR_OK); /* no final */
    MOQ_TEST_CHECK(moqr_log_seal_subgroup(log, 1, 0) == MOQR_ERR_INVAL);

    /* (b) END_OF_GROUP status at 22 vs retained 25: the append itself is
     * refused atomically — the status record is NOT admitted. (Unique id and
     * fresh identity, so the duplicate/monotonicity guards stay quiet and
     * the FINAL preflight is what rejects.) */
    moqr_log_stats_t st0, st1;
    moqr_log_get_stats(log, &st0);
    MOQ_TEST_CHECK(append_obj(log, &ca.vt, 1, 0, 22, 0, false,
                              MOQR_OBJ_END_OF_GROUP, false) == MOQR_ERR_INVAL);
    moqr_log_get_stats(log, &st1);
    MOQ_TEST_CHECK_EQ_U64(st1.record_count, st0.record_count);

    /* (c) EOG datagram at unique id 19 vs retained 25: refused atomically. */
    MOQ_TEST_CHECK(append_obj(log, &ca.vt, 1, 0, 19, 10, true,
                              MOQR_OBJ_NORMAL, true) == MOQR_ERR_INVAL);
    moqr_log_get_stats(log, &st1);
    MOQ_TEST_CHECK_EQ_U64(st1.record_count, st0.record_count);

    /* (d) END_OF_TRACK below retained content: an EOT on a FRESH subgroup
     * (no monotonicity interference) whose group-final would sit under a
     * retained object, and an EOT in an earlier group than a retained one —
     * both refused atomically. */
    MOQ_TEST_CHECK(append_obj(log, &ca.vt, 2, 0, 7, 10, false,
                              MOQR_OBJ_NORMAL, false) == MOQR_OK);
    MOQ_TEST_CHECK(append_obj(log, &ca.vt, 2, 1, 3, 0, false,
                              MOQR_OBJ_END_OF_TRACK, false) == MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(append_obj(log, &ca.vt, 1, 9, 30, 0, false,
                              MOQR_OBJ_END_OF_TRACK, false) == MOQR_ERR_INVAL);
    moqr_log_get_stats(log, &st1);
    MOQ_TEST_CHECK(!st1.end_of_track);   /* zero mutation */

    /* (e) EOT must validate its group even when the group final is already
     * KNOWN: final 30 committed (retained 28, 30), then EOT at 20 — both the
     * retained content and the declared final sit beyond the proposed track
     * final, so the append refuses and end_of_track stays unset. */
    MOQ_TEST_CHECK(append_obj(log, &ca.vt, 6, 0, 28, 10, false,
                              MOQR_OBJ_NORMAL, false) == MOQR_OK);
    MOQ_TEST_CHECK(append_obj(log, &ca.vt, 6, 0, 30, 0, false,
                              MOQR_OBJ_END_OF_GROUP, false) == MOQR_OK);
    MOQ_TEST_CHECK(append_obj(log, &ca.vt, 6, 1, 20, 0, false,
                              MOQR_OBJ_END_OF_TRACK, false) == MOQR_ERR_INVAL);
    moqr_log_get_stats(log, &st1);
    MOQ_TEST_CHECK(!st1.end_of_track);

    /* Controls: valid finals commit and enforce their locations. */
    MOQ_TEST_CHECK(append_obj(log, &ca.vt, 3, 0, 20, 10, true,
                              MOQR_OBJ_NORMAL, false) == MOQR_OK);
    MOQ_TEST_CHECK(append_obj(log, &ca.vt, 3, 8, 15, 10, false,
                              MOQR_OBJ_NORMAL, false) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_log_seal_subgroup(log, 3, 0) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_log_subgroup_is_final(log, 3, 0, 20));
    MOQ_TEST_CHECK(append_obj(log, &ca.vt, 3, 8, 21, 10, false,
                              MOQR_OBJ_NORMAL, false) == MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(append_obj(log, &ca.vt, 4, 0, 5, 0, false,
                              MOQR_OBJ_END_OF_GROUP, false) == MOQR_OK);
    MOQ_TEST_CHECK(append_obj(log, &ca.vt, 4, 0, 6, 10, false,
                              MOQR_OBJ_NORMAL, false) == MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(append_obj(log, &ca.vt, 7, 0, 9, 0, false,
                              MOQR_OBJ_END_OF_TRACK, false) == MOQR_OK);
    moqr_log_get_stats(log, &st1);
    MOQ_TEST_CHECK(st1.end_of_track);
    MOQ_TEST_CHECK(append_obj(log, &ca.vt, 8, 0, 0, 10, false,
                              MOQR_OBJ_NORMAL, false) == MOQR_ERR_INVAL);
    STEP_CHECK(log);

    moqr_log_destroy(log);
    MOQ_TEST_CHECK_EQ_INT((int)ca.live_bytes, 0);
    MOQ_TEST_PASS("final_evidence_preflight");
    return failures;
}

/* Rejecting malformed final evidence must not COST anything: the preflight
 * runs before group activation and capacity eviction, so an invalid final
 * whose payload would have pushed the log over its byte budget leaves every
 * previously retained group untouched. */
static int
test_final_evidence_atomicity(void)
{
    int failures = 0;
    count_alloc_t ca;
    ca_init(&ca);
    moqr_log_t *log = mklog(&ca, 8, 1000, NULL);
    MOQ_TEST_CHECK(log != NULL);

    MOQ_TEST_CHECK(append_obj(log, &ca.vt, 10, 0, 0, 400, false,
                              MOQR_OBJ_NORMAL, false) == MOQR_OK);
    MOQ_TEST_CHECK(append_obj(log, &ca.vt, 11, 0, 5, 400, false,
                              MOQR_OBJ_NORMAL, false) == MOQR_OK);

    /* An EOG datagram at object 3 (below retained 5 in group 11) carrying a
     * payload that would force the byte budget to evict group 10: the final
     * preflight must reject BEFORE any eviction runs. */
    MOQ_TEST_CHECK(append_obj(log, &ca.vt, 11, 0, 3, 400, true,
                              MOQR_OBJ_NORMAL, true) == MOQR_ERR_INVAL);
    moqr_record_view_t v;
    MOQ_TEST_CHECK(moqr_log_read_rec(log, 10, 0, 0, &v) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(moqr_log_group_count(log), 2);
    moqr_log_stats_t st;
    moqr_log_get_stats(log, &st);
    MOQ_TEST_CHECK_EQ_U64(st.evicted_groups_total, 0);

    moqr_log_destroy(log);
    MOQ_TEST_CHECK_EQ_INT((int)ca.live_bytes, 0);
    MOQ_TEST_PASS("final_evidence_atomicity");
    return failures;
}

static int
test_eviction_and_refcounts(void)
{
    int failures = 0;
    count_alloc_t ca;
    ca_init(&ca);
    moqr_log_t *log = mklog(&ca, 2, 1 << 20, NULL);
    MOQ_TEST_CHECK(log != NULL);

    /* Wrapped payloads with a release counter prove exactly-once release. */
    g_release_count = 0;
    static uint8_t ext[64];
    for (uint64_t g = 1; g <= 2; g++) {
        for (uint64_t o = 0; o < 3; o++) {
            moq_rcbuf_t *b = NULL;
            MOQ_TEST_CHECK(moq_rcbuf_wrap(&ca.vt, ext, sizeof(ext),
                                          count_release, NULL, &b) == 0);
            moqr_log_append_desc_t d;
            moqr_log_append_desc_init(&d);
            d.group_id = g;
            d.subgroup_id = 0;
            d.object_id = o;
            d.payload = b;
            d.now_us = g * 100 + o;
            MOQ_TEST_CHECK(moqr_log_append(log, &d) == MOQR_OK);
        }
    }
    STEP_CHECK(log);

    /* Group cap is 2: appending group 3 evicts group 1 (3 releases). */
    MOQ_TEST_CHECK(append_obj(log, &ca.vt, 3, 0, 0, 10, false,
                              MOQR_OBJ_NORMAL, false) == MOQR_OK);
    STEP_CHECK(log);
    MOQ_TEST_CHECK_EQ_INT(g_release_count, 3);

    /* Late arrival for the evicted group: TOO_OLD, refs kept by caller. */
    MOQ_TEST_CHECK(append_obj(log, &ca.vt, 1, 0, 9, 10, false,
                              MOQR_OBJ_NORMAL, false) == MOQR_ERR_TOO_OLD);

    moqr_log_stats_t st;
    moqr_log_get_stats(log, &st);
    MOQ_TEST_CHECK_EQ_U64(st.group_count, 2);
    MOQ_TEST_CHECK_EQ_U64(st.evicted_groups_total, 1);
    MOQ_TEST_CHECK_EQ_U64(st.evicted_records_total, 3);
    MOQ_TEST_CHECK_EQ_U64(st.oldest_group_id, 2);

    /* Destroy releases the rest exactly once. */
    moqr_log_destroy(log);
    MOQ_TEST_CHECK_EQ_INT(g_release_count, 6);
    MOQ_TEST_CHECK_EQ_INT((int)ca.live_bytes, 0);
    MOQ_TEST_PASS("eviction_and_refcounts");
    return failures;
}

static int
test_eviction_under_cursor(void)
{
    int failures = 0;
    count_alloc_t ca;
    ca_init(&ca);
    moqr_trace_t *trace = NULL;
    MOQ_TEST_CHECK(moqr_trace_create(&ca.vt, 64, &trace) == MOQR_OK);
    moqr_log_t *log = mklog(&ca, 2, 1 << 20, trace);
    MOQ_TEST_CHECK(log != NULL);

    for (uint64_t o = 0; o < 4; o++) {
        MOQ_TEST_CHECK(append_obj(log, &ca.vt, 1, 0, o, 10, false,
                                  MOQR_OBJ_NORMAL, false) == MOQR_OK);
    }
    MOQ_TEST_CHECK(append_obj(log, &ca.vt, 2, 0, 0, 10, false,
                              MOQR_OBJ_NORMAL, false) == MOQR_OK);

    /* Cursor reads one record of group 1, then group 1 is evicted. */
    moqr_cursor_t cur = MOQR_CURSOR_INVALID;
    MOQ_TEST_CHECK(moqr_log_cursor_open(log, MOQR_CURSOR_FROM_OLDEST,
                                        &cur) == MOQR_OK);
    moqr_record_view_t v;
    bool skipped = false;
    MOQ_TEST_CHECK(moqr_log_cursor_next(log, cur, &v, &skipped) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(v.group_id, 1);
    MOQ_TEST_CHECK(!skipped);

    MOQ_TEST_CHECK(append_obj(log, &ca.vt, 3, 0, 0, 10, false,
                              MOQR_OBJ_NORMAL, false) == MOQR_OK);
    STEP_CHECK(log);   /* group 1 evicted (cap 2) */

    /* Next read skips to group 2's first record, flagged. */
    MOQ_TEST_CHECK(moqr_log_cursor_next(log, cur, &v, &skipped) == MOQR_OK);
    MOQ_TEST_CHECK(skipped);
    MOQ_TEST_CHECK_EQ_U64(v.group_id, 2);
    MOQ_TEST_CHECK_EQ_U64(v.object_id, 0);

    /* Subsequent reads are clean. */
    MOQ_TEST_CHECK(moqr_log_cursor_next(log, cur, &v, &skipped) == MOQR_OK);
    MOQ_TEST_CHECK(!skipped);
    MOQ_TEST_CHECK_EQ_U64(v.group_id, 3);

    /* The skip surfaced in the trace stream. */
    moqr_trace_rec_t recs[64];
    size_t n = moqr_trace_read(trace, recs, 64);
    bool saw_skip = false, saw_evict = false;
    for (size_t i = 0; i < n; i++) {
        if (recs[i].kind == MOQR_TRACE_CURSOR_SKIP) {
            saw_skip = true;
        }
        if (recs[i].kind == MOQR_TRACE_EVICT) {
            saw_evict = true;
        }
    }
    MOQ_TEST_CHECK(saw_skip);
    MOQ_TEST_CHECK(saw_evict);

    moqr_log_destroy(log);
    moqr_trace_destroy(trace);
    MOQ_TEST_CHECK_EQ_INT((int)ca.live_bytes, 0);
    MOQ_TEST_PASS("eviction_under_cursor");
    return failures;
}

static int
test_byte_budget_and_caps(void)
{
    int failures = 0;
    count_alloc_t ca;
    ca_init(&ca);

    /* Byte budget: 300 bytes; 100-byte objects; groups of 2. */
    moqr_log_t *log = mklog(&ca, 8, 300, NULL);
    MOQ_TEST_CHECK(log != NULL);
    MOQ_TEST_CHECK(append_obj(log, &ca.vt, 1, 0, 0, 100, false,
                              MOQR_OBJ_NORMAL, false) == MOQR_OK);
    MOQ_TEST_CHECK(append_obj(log, &ca.vt, 1, 0, 1, 100, false,
                              MOQR_OBJ_NORMAL, false) == MOQR_OK);
    MOQ_TEST_CHECK(append_obj(log, &ca.vt, 2, 0, 0, 100, false,
                              MOQR_OBJ_NORMAL, false) == MOQR_OK);
    STEP_CHECK(log);
    /* Next 100B forces eviction of group 1 (200B out, 100B in). */
    MOQ_TEST_CHECK(append_obj(log, &ca.vt, 2, 0, 1, 100, false,
                              MOQR_OBJ_NORMAL, false) == MOQR_OK);
    STEP_CHECK(log);
    moqr_log_stats_t st;
    moqr_log_get_stats(log, &st);
    MOQ_TEST_CHECK_EQ_U64(st.group_count, 1);
    MOQ_TEST_CHECK_EQ_U64(st.oldest_group_id, 2);
    MOQ_TEST_CHECK_EQ_U64(st.retained_bytes, 200);

    /* Appending to the sole (oldest == target) group past budget: refuse. */
    MOQ_TEST_CHECK(append_obj(log, &ca.vt, 2, 0, 2, 200, false,
                              MOQR_OBJ_NORMAL, false) == MOQR_ERR_CAPACITY);
    /* A single object larger than the whole budget: refuse outright. */
    MOQ_TEST_CHECK(append_obj(log, &ca.vt, 3, 0, 0, 301, false,
                              MOQR_OBJ_NORMAL, false) == MOQR_ERR_CAPACITY);
    STEP_CHECK(log);
    moqr_log_destroy(log);
    MOQ_TEST_CHECK_EQ_INT((int)ca.live_bytes, 0);

    /* Per-group object cap. */
    moqr_log_cfg_t cfg;
    moqr_log_cfg_init_sized(&cfg, sizeof(cfg), &ca.vt);
    cfg.max_objects_per_group = 2;
    moqr_log_t *small = NULL;
    MOQ_TEST_CHECK(moqr_log_create(&cfg, &small) == MOQR_OK);
    MOQ_TEST_CHECK(append_obj(small, &ca.vt, 1, 0, 0, 8, false,
                              MOQR_OBJ_NORMAL, false) == MOQR_OK);
    MOQ_TEST_CHECK(append_obj(small, &ca.vt, 1, 0, 1, 8, false,
                              MOQR_OBJ_NORMAL, false) == MOQR_OK);
    MOQ_TEST_CHECK(append_obj(small, &ca.vt, 1, 0, 2, 8, false,
                              MOQR_OBJ_NORMAL, false) == MOQR_ERR_CAPACITY);
    STEP_CHECK(small);
    moqr_log_destroy(small);
    MOQ_TEST_CHECK_EQ_INT((int)ca.live_bytes, 0);

    /* Age eviction through tick (time is an input). */
    moqr_log_cfg_t acfg;
    moqr_log_cfg_init_sized(&acfg, sizeof(acfg), &ca.vt);
    acfg.budget.max_age_us = 1000;
    moqr_log_t *aged = NULL;
    MOQ_TEST_CHECK(moqr_log_create(&acfg, &aged) == MOQR_OK);
    moqr_log_append_desc_t d;
    moqr_log_append_desc_init(&d);
    d.group_id = 1;
    d.object_id = 0;
    d.payload = mkbuf(&ca.vt, 8, 1);
    d.now_us = 5000;
    MOQ_TEST_CHECK(moqr_log_append(aged, &d) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_SIZE(moqr_log_tick(aged, 5500), (size_t)0);
    MOQ_TEST_CHECK_EQ_SIZE(moqr_log_tick(aged, 6001), (size_t)1);
    STEP_CHECK(aged);
    moqr_log_destroy(aged);
    MOQ_TEST_CHECK_EQ_INT((int)ca.live_bytes, 0);

    MOQ_TEST_PASS("byte_budget_and_caps");
    return failures;
}

static uint64_t
run_traced_sequence(count_alloc_t *ca, uint64_t salt)
{
    moqr_trace_t *trace = NULL;
    if (moqr_trace_create(&ca->vt, 128, &trace) != MOQR_OK) {
        return 0;
    }
    moqr_log_t *log = mklog(ca, 2, 4096, trace);
    if (log == NULL) {
        moqr_trace_destroy(trace);
        return 0;
    }
    moqr_cursor_t cur = MOQR_CURSOR_INVALID;
    (void)moqr_log_cursor_open(log, MOQR_CURSOR_FROM_OLDEST, &cur);
    for (uint64_t g = 1; g <= 4; g++) {
        for (uint64_t o = 0; o < 3; o++) {
            (void)append_obj(log, &ca->vt, g + salt, 0, o, 64,
                             o == 2 /* EOG on last */, MOQR_OBJ_NORMAL,
                             false);
            moqr_record_view_t v;
            bool skipped = false;
            while (moqr_log_cursor_next(log, cur, &v, &skipped) == MOQR_OK) {
            }
        }
    }
    uint64_t h = moqr_trace_hash(trace);
    moqr_log_destroy(log);
    moqr_trace_destroy(trace);
    return h;
}

static int
test_vector_growth(void)
{
    /* Regression: record vectors that grow past MOQR_REC_VEC_INITIAL must
     * preserve every earlier record (count, bytes, refs). Caught first by
     * bench_relay_core: the growth path dropped the count, truncating the
     * group and leaking every pre-growth payload ref on eviction. */
    int failures = 0;
    count_alloc_t ca;
    ca_init(&ca);
    moqr_log_t *log = mklog(&ca, 2, 1 << 20, NULL);
    MOQ_TEST_CHECK(log != NULL);

    const uint64_t N = 40;   /* several doublings past the initial 8 */
    for (uint64_t o = 0; o < N; o++) {
        MOQ_TEST_CHECK(append_obj(log, &ca.vt, 1, 0, o, 32, false,
                                  MOQR_OBJ_NORMAL, false) == MOQR_OK);
        STEP_CHECK(log);
    }
    moqr_log_stats_t st;
    moqr_log_get_stats(log, &st);
    MOQ_TEST_CHECK_EQ_U64(st.record_count, N);
    MOQ_TEST_CHECK_EQ_U64(st.retained_bytes, N * 32);

    /* Every record is still readable with the right identity+payload. */
    moqr_cursor_t cur = MOQR_CURSOR_INVALID;
    MOQ_TEST_CHECK(moqr_log_cursor_open(log, MOQR_CURSOR_FROM_OLDEST,
                                        &cur) == MOQR_OK);
    for (uint64_t o = 0; o < N; o++) {
        moqr_record_view_t v;
        bool skipped = false;
        MOQ_TEST_CHECK(moqr_log_cursor_next(log, cur, &v, &skipped) ==
                       MOQR_OK);
        MOQ_TEST_CHECK_EQ_U64(v.object_id, o);
        MOQ_TEST_CHECK(v.payload != NULL &&
                       moq_rcbuf_data(v.payload)[0] == (uint8_t)o);
    }

    /* Eviction releases ALL N refs (balance-zero proves it). */
    MOQ_TEST_CHECK_EQ_SIZE(moqr_log_evict_groups(log, 1), (size_t)1);
    STEP_CHECK(log);
    moqr_log_destroy(log);
    MOQ_TEST_CHECK_EQ_INT((int)ca.live_bytes, 0);
    MOQ_TEST_CHECK_EQ_INT((int)(ca.allocs - ca.frees), 0);
    MOQ_TEST_PASS("vector_growth");
    return failures;
}

static int
test_trace_hash_determinism(void)
{
    int failures = 0;
    count_alloc_t ca;
    ca_init(&ca);

    uint64_t h1 = run_traced_sequence(&ca, 0);
    uint64_t h2 = run_traced_sequence(&ca, 0);
    uint64_t h3 = run_traced_sequence(&ca, 100);
    MOQ_TEST_CHECK(h1 != 0);
    MOQ_TEST_CHECK_EQ_U64(h1, h2);      /* run-twice equality */
    MOQ_TEST_CHECK(h1 != h3);           /* different inputs diverge */
    MOQ_TEST_CHECK_EQ_INT((int)ca.live_bytes, 0);
    MOQ_TEST_PASS("trace_hash_determinism");
    return failures;
}

static int
test_group_wide_duplicate_identity(void)
{
    /* An object is uniquely identified by (track, group, object)
     * (-18 :795-798); the same identity re-appearing in another subgroup,
     * as a datagram, or with a different shape is malformed
     * (-18 :1069-1077). The log is the cache: duplicates are refused
     * group-wide, whatever list they target. */
    int failures = 0;
    count_alloc_t ca;
    ca_init(&ca);
    moqr_log_t *log = mklog(&ca, 4, 1 << 20, NULL);
    MOQ_TEST_CHECK(log != NULL);

    MOQ_TEST_CHECK(append_obj(log, &ca.vt, 1, 0, 5, 16, false,
                              MOQR_OBJ_NORMAL, false) == MOQR_OK);
    /* Same (group, object) in a different subgroup: refused. */
    MOQ_TEST_CHECK(append_obj(log, &ca.vt, 1, 1, 5, 16, false,
                              MOQR_OBJ_NORMAL, false) == MOQR_ERR_INVAL);
    /* Same identity as a datagram: refused. */
    MOQ_TEST_CHECK(append_obj(log, &ca.vt, 1, 0, 5, 16, false,
                              MOQR_OBJ_NORMAL, true) == MOQR_ERR_INVAL);
    /* Datagram first, then the same identity on a subgroup: refused. */
    MOQ_TEST_CHECK(append_obj(log, &ca.vt, 1, 0, 8, 16, false,
                              MOQR_OBJ_NORMAL, true) == MOQR_OK);
    MOQ_TEST_CHECK(append_obj(log, &ca.vt, 1, 2, 8, 16, false,
                              MOQR_OBJ_NORMAL, false) == MOQR_ERR_INVAL);
    /* Distinct ids across lists remain fine. */
    MOQ_TEST_CHECK(append_obj(log, &ca.vt, 1, 1, 6, 16, false,
                              MOQR_OBJ_NORMAL, false) == MOQR_OK);
    STEP_CHECK(log);

    /* EOG at id 20; the same id 20 arriving again anywhere is a dup. */
    MOQ_TEST_CHECK(append_obj(log, &ca.vt, 1, 0, 20, 16, true,
                              MOQR_OBJ_NORMAL, false) == MOQR_OK);
    MOQ_TEST_CHECK(append_obj(log, &ca.vt, 1, 3, 20, 16, false,
                              MOQR_OBJ_NORMAL, false) == MOQR_ERR_INVAL);
    STEP_CHECK(log);

    moqr_log_stats_t st;
    moqr_log_get_stats(log, &st);
    MOQ_TEST_CHECK_EQ_U64(st.record_count, 4);

    moqr_log_destroy(log);
    MOQ_TEST_CHECK_EQ_INT((int)ca.live_bytes, 0);
    MOQ_TEST_PASS("group_wide_duplicate_identity");
    return failures;
}

static int
test_cfg_prefix_consumers(void)
{
    /* Prefix safety, applied to consumers: create() and describe() must
     * not read fields the caller's struct_size does not contain. Fill the
     * full struct with garbage, initialize only a prefix, and the
     * defaults — not the garbage — must govern. */
    int failures = 0;
    count_alloc_t ca;
    ca_init(&ca);

    union {
        moqr_log_cfg_t c;
        unsigned char  raw[sizeof(moqr_log_cfg_t)];
    } u;

    /* Prefix ends right after alloc: budget/caps/trace are garbage bytes
     * beyond struct_size and must be ignored. */
    memset(u.raw, 0x5A, sizeof(u.raw));
    size_t prefix = offsetof(moqr_log_cfg_t, alloc) +
                    sizeof(((moqr_log_cfg_t *)0)->alloc);
    moqr_log_cfg_init_sized(&u.c, prefix, &ca.vt);
    MOQ_TEST_CHECK(u.c.alloc == &ca.vt);

    moqr_log_capacity_t cap;
    moqr_log_capacity_describe(&u.c, &cap);
    MOQ_TEST_CHECK_EQ_U64(cap.payload_bytes, 8u * 1024u * 1024u);

    moqr_log_t *log = NULL;
    MOQ_TEST_CHECK(moqr_log_create(&u.c, &log) == MOQR_OK);
    MOQ_TEST_CHECK(log != NULL);
    if (log != NULL) {
        /* Garbage budget would make this tiny append overflow tiny caps
         * or huge ones; with defaults it must simply work. */
        MOQ_TEST_CHECK(append_obj(log, &ca.vt, 1, 0, 0, 64, false,
                                  MOQR_OBJ_NORMAL, false) == MOQR_OK);
        STEP_CHECK(log);
        moqr_log_destroy(log);
    }
    MOQ_TEST_CHECK_EQ_INT((int)ca.live_bytes, 0);

    /* A cfg too small to contain alloc is rejected outright. */
    memset(u.raw, 0, sizeof(u.raw));
    u.c.struct_size = (uint32_t)offsetof(moqr_log_cfg_t, alloc);
    moqr_log_t *bad = NULL;
    MOQ_TEST_CHECK(moqr_log_create(&u.c, &bad) == MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(bad == NULL);

    MOQ_TEST_PASS("cfg_prefix_consumers");
    return failures;
}

static int
test_capacity_structure_bytes(void)
{
    /* The checker enforces allocator-visible structure bytes against the
     * ceiling, not just logical counts: an internally over-cap shape must
     * fail the check. White-box via log_internal.h. */
    int failures = 0;
    count_alloc_t ca;
    ca_init(&ca);
    moqr_log_t *log = mklog(&ca, 2, 4096, NULL);
    MOQ_TEST_CHECK(log != NULL);
    for (uint64_t o = 0; o < 12; o++) {
        MOQ_TEST_CHECK(append_obj(log, &ca.vt, 1, 0, o, 32, false,
                                  MOQR_OBJ_NORMAL, false) == MOQR_OK);
    }
    STEP_CHECK(log);

    /* Fabricate an absurd vector capacity: the structure-byte check must
     * trip even though logical counts stay coherent. */
    moqr_group_t *g = &log->groups[0];
    uint32_t saved_cap = g->sgs[0].v.cap;
    g->sgs[0].v.cap = 1u << 30;
    const char *why = NULL;
    MOQ_TEST_CHECK(moqr_log_capacity_check(log, &why) == MOQR_ERR_INTERNAL);
    g->sgs[0].v.cap = saved_cap;
    STEP_CHECK(log);

    moqr_log_destroy(log);
    MOQ_TEST_CHECK_EQ_INT((int)ca.live_bytes, 0);
    MOQ_TEST_PASS("capacity_structure_bytes");
    return failures;
}

static int
test_capacity_describe_bounds(void)
{
    int failures = 0;
    count_alloc_t ca;
    ca_init(&ca);

    moqr_log_cfg_t cfg;
    moqr_log_cfg_init_sized(&cfg, sizeof(cfg), &ca.vt);
    cfg.budget.max_groups = 2;
    cfg.budget.max_bytes = 4096;
    moqr_log_capacity_t cap;
    moqr_log_capacity_describe(&cfg, &cap);
    MOQ_TEST_CHECK(cap.structure_bytes > 0);
    MOQ_TEST_CHECK_EQ_U64(cap.payload_bytes, 4096);
    /* Headers are part of the ceiling: one payload + one properties buffer
     * per possible record, one slice per chunk node, H each. */
    {
        size_t h = 0;
        MOQ_TEST_CHECK(moq_rcbuf_allocation_size(0, &h) == MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(cap.header_bytes,
                              (uint64_t)h * (2u * cap.max_records +
                                             cap.max_chunk_nodes));
    }
    MOQ_TEST_CHECK_EQ_U64(cap.total_bytes,
                          cap.structure_bytes + cap.payload_bytes +
                              cap.header_bytes);

    /* The ceiling is a true ceiling: fill the log, compare live bytes. */
    moqr_log_t *log = NULL;
    MOQ_TEST_CHECK(moqr_log_create(&cfg, &log) == MOQR_OK);
    for (uint64_t g = 1; g <= 6; g++) {
        for (uint64_t o = 0; o < 8; o++) {
            (void)append_obj(log, &ca.vt, g, o % 3, o, 128, false,
                             MOQR_OBJ_NORMAL, false);
        }
    }
    STEP_CHECK(log);
    MOQ_TEST_CHECK((uint64_t)ca.live_bytes <= cap.total_bytes);
    moqr_log_destroy(log);
    MOQ_TEST_CHECK_EQ_INT((int)ca.live_bytes, 0);

    /* Prefix-safe cfg init: alloc region untouched below its offset. */
    union {
        moqr_log_cfg_t c;
        unsigned char  raw[sizeof(moqr_log_cfg_t)];
    } u;
    memset(u.raw, 0x5A, sizeof(u.raw));
    size_t prefix = offsetof(moqr_log_cfg_t, alloc);
    moqr_log_cfg_init_sized(&u.c, prefix, &ca.vt);
    MOQ_TEST_CHECK_EQ_U64(u.c.struct_size, prefix);
    int canary_ok = 1;
    for (size_t i = prefix; i < sizeof(u.raw); i++) {
        if (u.raw[i] != 0x5A) {
            canary_ok = 0;
            break;
        }
    }
    MOQ_TEST_CHECK(canary_ok);

    MOQ_TEST_PASS("capacity_describe_bounds");
    return failures;
}

/* -- OPEN-record (chunk-through) machinery --------------------------------- */

/* Open an incrementally-filled record at (group, subgroup, object). */
static moqr_result_t
open_rec(moqr_log_t *log, uint64_t group, uint64_t subgroup, uint64_t object,
         uint64_t declared_len)
{
    moqr_log_append_desc_t d;
    moqr_log_append_desc_init(&d);
    d.group_id = group;
    d.subgroup_id = subgroup;
    d.object_id = object;
    d.publisher_priority = 128;
    d.status = MOQR_OBJ_NORMAL;
    d.obj_state = MOQR_OBJ_OPEN;
    d.declared_len = declared_len;
    d.now_us = 1000 + object;
    return moqr_log_open_record(log, &d);
}

/* Append one `len`-byte chunk (filled with `fill`); the log increfs it, so the
 * creating ref is dropped after the call. */
static moqr_result_t
append_chunk_n(moqr_log_t *log, const moq_alloc_t *a, uint64_t group,
               uint64_t subgroup, uint64_t object, size_t len, uint8_t fill)
{
    moq_rcbuf_t *b = mkbuf(a, len, fill);
    if (b == NULL) {
        return MOQR_ERR_NOMEM;
    }
    moqr_result_t rc = moqr_log_append_chunk(log, group, subgroup, object, b);
    moq_rcbuf_decref(b);
    return rc;
}

/* Open -> append chunks -> complete -> read exact concatenated
 * payload; over-declared and complete-before-full are rejected. */
static int
test_chunk_open_append_complete(void)
{
    int failures = 0;
    count_alloc_t ca;
    ca_init(&ca);
    moqr_log_t *log = mklog(&ca, 4, 1u << 20, NULL);
    MOQ_TEST_CHECK(log != NULL);
    if (log == NULL) {
        return failures;
    }

    /* A 30-byte object as 3x10-byte chunks. */
    MOQ_TEST_CHECK(open_rec(log, 7, 0, 3, 30) == MOQR_OK);
    MOQ_TEST_CHECK(append_chunk_n(log, &ca.vt, 7, 0, 3, 10, 0xA0) == MOQR_OK);
    MOQ_TEST_CHECK(append_chunk_n(log, &ca.vt, 7, 0, 3, 10, 0xA1) == MOQR_OK);
    /* A chunk exceeding declared_len is rejected; the record stays OPEN. */
    MOQ_TEST_CHECK(append_chunk_n(log, &ca.vt, 7, 0, 3, 11, 0xFF) ==
                   MOQR_ERR_INVAL);
    /* Completing before all bytes arrive is rejected. */
    MOQ_TEST_CHECK(moqr_log_complete_record(log, 7, 0, 3) == MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(append_chunk_n(log, &ca.vt, 7, 0, 3, 10, 0xA2) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_log_complete_record(log, 7, 0, 3) == MOQR_OK);

    moqr_record_view_t v;
    MOQ_TEST_CHECK(moqr_log_read_rec(log, 7, 0, 0, &v) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(v.obj_state, MOQR_OBJ_COMPLETE);
    MOQ_TEST_CHECK_EQ_U64(v.object_id, 3);
    MOQ_TEST_CHECK(v.payload == NULL);          /* chunked: no single buffer */
    MOQ_TEST_CHECK_EQ_U64(v.chunk_count, 3);
    MOQ_TEST_CHECK_EQ_U64(v.declared_len, 30);
    const uint8_t want[3] = { 0xA0, 0xA1, 0xA2 };
    uint64_t total = 0;
    for (uint32_t i = 0; i < 3; i++) {
        const moq_rcbuf_t *cb = NULL;
        uint64_t cl = 0;
        MOQ_TEST_CHECK(moqr_log_view_chunk(log, &v, i, &cb, &cl) == MOQR_OK);
        MOQ_TEST_CHECK_EQ_U64(cl, 10);
        MOQ_TEST_CHECK(cb != NULL && moq_rcbuf_data(cb)[0] == want[i]);
        total += cl;
    }
    MOQ_TEST_CHECK_EQ_U64(total, 30);
    const moq_rcbuf_t *cb2 = NULL;
    uint64_t cl2 = 0;
    MOQ_TEST_CHECK(moqr_log_view_chunk(log, &v, 3, &cb2, &cl2) == MOQR_DONE);

    /* The cursor reader must populate the chunk fields identically (view_fill
     * is the single source of truth) and feed moqr_log_view_chunk. */
    moqr_cursor_t cur;
    MOQ_TEST_CHECK(moqr_log_cursor_open(log, MOQR_CURSOR_FROM_OLDEST, &cur) ==
                   MOQR_OK);
    moqr_record_view_t cv;
    bool cskip = false;
    MOQ_TEST_CHECK(moqr_log_cursor_next(log, cur, &cv, &cskip) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(cv.obj_state, MOQR_OBJ_COMPLETE);
    MOQ_TEST_CHECK_EQ_U64(cv.chunk_count, 3);
    MOQ_TEST_CHECK_EQ_U64(cv.declared_len, 30);
    MOQ_TEST_CHECK(cv.payload == NULL);
    const moq_rcbuf_t *ccb = NULL;
    uint64_t ccl = 0;
    MOQ_TEST_CHECK(moqr_log_view_chunk(log, &cv, 0, &ccb, &ccl) == MOQR_OK);
    MOQ_TEST_CHECK(ccb != NULL && ccl == 10 && moq_rcbuf_data(ccb)[0] == 0xA0);
    MOQ_TEST_CHECK(moqr_log_view_chunk(log, &cv, 2, &ccb, &ccl) == MOQR_OK);
    MOQ_TEST_CHECK(ccb != NULL && moq_rcbuf_data(ccb)[0] == 0xA2);
    MOQ_TEST_CHECK(moqr_log_cursor_close(log, cur) == MOQR_OK);

    moqr_log_stats_t st;
    moqr_log_get_stats(log, &st);
    MOQ_TEST_CHECK_EQ_U64(st.retained_bytes, 30);
    STEP_CHECK(log);
    moqr_log_destroy(log);
    MOQ_TEST_CHECK_EQ_INT((int)ca.live_bytes, 0);
    MOQ_TEST_CHECK_EQ_INT((int)(ca.allocs - ca.frees), 0);
    MOQ_TEST_PASS("chunk_open_append_complete");
    return failures;
}

/* Open -> append -> abandon: bytes drop, view reports ABANDONED, and
 * the subgroup unblocks for the next object. */
static int
test_chunk_abandon(void)
{
    int failures = 0;
    count_alloc_t ca;
    ca_init(&ca);
    moqr_log_t *log = mklog(&ca, 4, 1u << 20, NULL);
    MOQ_TEST_CHECK(log != NULL);
    if (log == NULL) {
        return failures;
    }

    MOQ_TEST_CHECK(open_rec(log, 7, 0, 3, 30) == MOQR_OK);
    MOQ_TEST_CHECK(append_chunk_n(log, &ca.vt, 7, 0, 3, 10, 0xB0) == MOQR_OK);
    MOQ_TEST_CHECK(append_chunk_n(log, &ca.vt, 7, 0, 3, 10, 0xB1) == MOQR_OK);
    moqr_log_stats_t st;
    moqr_log_get_stats(log, &st);
    MOQ_TEST_CHECK_EQ_U64(st.retained_bytes, 20);

    MOQ_TEST_CHECK(moqr_log_abandon_record(log, 7, 0, 3, rd_wire(0)) == MOQR_OK);
    moqr_log_get_stats(log, &st);
    MOQ_TEST_CHECK_EQ_U64(st.retained_bytes, 0);   /* chunks released */

    moqr_record_view_t v;
    MOQ_TEST_CHECK(moqr_log_read_rec(log, 7, 0, 0, &v) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(v.obj_state, MOQR_OBJ_ABANDONED);
    MOQ_TEST_CHECK_EQ_U64(v.chunk_count, 0);
    MOQ_TEST_CHECK(v.payload == NULL);

    /* Abandon clears the OPEN block: the next ascending object appends. */
    MOQ_TEST_CHECK(append_obj(log, &ca.vt, 7, 0, 4, 5, false,
                              MOQR_OBJ_NORMAL, false) == MOQR_OK);
    /* Chunk ops against the abandoned identity now fail (no OPEN record). */
    MOQ_TEST_CHECK(append_chunk_n(log, &ca.vt, 7, 0, 3, 1, 0xB2) ==
                   MOQR_ERR_WRONG_STATE);

    STEP_CHECK(log);
    moqr_log_destroy(log);
    MOQ_TEST_CHECK_EQ_INT((int)ca.live_bytes, 0);
    MOQ_TEST_CHECK_EQ_INT((int)(ca.allocs - ca.frees), 0);
    MOQ_TEST_PASS("chunk_abandon");
    return failures;
}

/* Identity + ordering — a duplicate object id is rejected, and no
 * later object can be appended to a subgroup while a record is OPEN. */
static int
test_chunk_ordering_and_dup(void)
{
    int failures = 0;
    count_alloc_t ca;
    ca_init(&ca);
    moqr_log_t *log = mklog(&ca, 4, 1u << 20, NULL);
    MOQ_TEST_CHECK(log != NULL);
    if (log == NULL) {
        return failures;
    }

    MOQ_TEST_CHECK(open_rec(log, 7, 0, 5, 100) == MOQR_OK);
    /* Duplicate identity (re-open object 5) -> INVAL (group-wide id set). */
    MOQ_TEST_CHECK(open_rec(log, 7, 0, 5, 100) == MOQR_ERR_INVAL);
    /* Non-ascending object while 5 is OPEN -> INVAL (ordering). */
    MOQ_TEST_CHECK(append_obj(log, &ca.vt, 7, 0, 4, 5, false,
                              MOQR_OBJ_NORMAL, false) == MOQR_ERR_INVAL);
    /* A LATER object cannot overtake the OPEN one -> WRONG_STATE. */
    MOQ_TEST_CHECK(append_obj(log, &ca.vt, 7, 0, 6, 5, false,
                              MOQR_OBJ_NORMAL, false) == MOQR_ERR_WRONG_STATE);
    MOQ_TEST_CHECK(open_rec(log, 7, 0, 6, 10) == MOQR_ERR_WRONG_STATE);
    /* A different subgroup is independent — its object streams in parallel.
     * (Object ids are unique GROUP-wide, so use a distinct id here.) */
    MOQ_TEST_CHECK(open_rec(log, 7, 1, 8, 10) == MOQR_OK);

    /* Finish object 5, then object 6 in subgroup 0 appends fine. */
    MOQ_TEST_CHECK(append_chunk_n(log, &ca.vt, 7, 0, 5, 100, 0xC0) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_log_complete_record(log, 7, 0, 5) == MOQR_OK);
    MOQ_TEST_CHECK(append_obj(log, &ca.vt, 7, 0, 6, 5, false,
                              MOQR_OBJ_NORMAL, false) == MOQR_OK);

    STEP_CHECK(log);
    moqr_log_destroy(log);
    MOQ_TEST_CHECK_EQ_INT((int)ca.live_bytes, 0);
    MOQ_TEST_CHECK_EQ_INT((int)(ca.allocs - ca.frees), 0);
    MOQ_TEST_PASS("chunk_ordering_and_dup");
    return failures;
}

/* Capacity — chunk-node pool exhaustion and declared_len > max_bytes
 * both fail closed with no leak / no half-open corruption; pool recycles. */
static int
test_chunk_capacity(void)
{
    int failures = 0;
    count_alloc_t ca;
    ca_init(&ca);
    /* Tiny chunk-node pool (2 nodes) and a small byte budget. */
    moqr_log_cfg_t cfg;
    moqr_log_cfg_init_sized(&cfg, sizeof(cfg), &ca.vt);
    cfg.budget.max_groups = 4;
    cfg.budget.max_bytes = 100;
    cfg.max_chunk_nodes = 2;
    moqr_log_t *log = NULL;
    MOQ_TEST_CHECK(moqr_log_create(&cfg, &log) == MOQR_OK);
    if (log == NULL) {
        return failures;
    }

    /* declared_len > max_bytes fails at open (nothing admitted). */
    MOQ_TEST_CHECK(open_rec(log, 1, 0, 0, 200) == MOQR_ERR_CAPACITY);
    { moqr_log_stats_t st; moqr_log_get_stats(log, &st);
      MOQ_TEST_CHECK_EQ_U64(st.record_count, 0); }

    /* Open, fill the 2-node pool, then the 3rd chunk exhausts it -> CAPACITY;
     * the record stays OPEN and holds its 2 chunks (no half-open loss). */
    MOQ_TEST_CHECK(open_rec(log, 1, 0, 0, 30) == MOQR_OK);
    MOQ_TEST_CHECK(append_chunk_n(log, &ca.vt, 1, 0, 0, 10, 0xD0) == MOQR_OK);
    MOQ_TEST_CHECK(append_chunk_n(log, &ca.vt, 1, 0, 0, 10, 0xD1) == MOQR_OK);
    MOQ_TEST_CHECK(append_chunk_n(log, &ca.vt, 1, 0, 0, 10, 0xD2) ==
                   MOQR_ERR_CAPACITY);
    { moqr_record_view_t v;
      MOQ_TEST_CHECK(moqr_log_read_rec(log, 1, 0, 0, &v) == MOQR_OK);
      MOQ_TEST_CHECK_EQ_U64(v.obj_state, MOQR_OBJ_OPEN);
      MOQ_TEST_CHECK_EQ_U64(v.chunk_count, 2); }

    /* Abandon returns both nodes to the pool; a fresh record can reuse them. */
    MOQ_TEST_CHECK(moqr_log_abandon_record(log, 1, 0, 0, rd_wire(0)) == MOQR_OK);
    MOQ_TEST_CHECK(open_rec(log, 1, 0, 1, 20) == MOQR_OK);
    MOQ_TEST_CHECK(append_chunk_n(log, &ca.vt, 1, 0, 1, 10, 0xE0) == MOQR_OK);
    MOQ_TEST_CHECK(append_chunk_n(log, &ca.vt, 1, 0, 1, 10, 0xE1) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_log_complete_record(log, 1, 0, 1) == MOQR_OK);

    STEP_CHECK(log);
    moqr_log_destroy(log);
    MOQ_TEST_CHECK_EQ_INT((int)ca.live_bytes, 0);   /* every slice decref'd */
    MOQ_TEST_CHECK_EQ_INT((int)(ca.allocs - ca.frees), 0);
    MOQ_TEST_PASS("chunk_capacity");
    return failures;
}

/* Retained-byte accounting rises per chunk and drops on eviction. */
static int
test_chunk_byte_accounting_eviction(void)
{
    int failures = 0;
    count_alloc_t ca;
    ca_init(&ca);
    moqr_log_t *log = mklog(&ca, 4, 1u << 20, NULL);
    MOQ_TEST_CHECK(log != NULL);
    if (log == NULL) {
        return failures;
    }

    moqr_log_stats_t st;
    MOQ_TEST_CHECK(open_rec(log, 2, 0, 0, 25) == MOQR_OK);
    MOQ_TEST_CHECK(append_chunk_n(log, &ca.vt, 2, 0, 0, 10, 0x10) == MOQR_OK);
    moqr_log_get_stats(log, &st);
    MOQ_TEST_CHECK_EQ_U64(st.retained_bytes, 10);
    MOQ_TEST_CHECK(append_chunk_n(log, &ca.vt, 2, 0, 0, 15, 0x11) == MOQR_OK);
    moqr_log_get_stats(log, &st);
    MOQ_TEST_CHECK_EQ_U64(st.retained_bytes, 25);   /* rises per chunk */
    MOQ_TEST_CHECK(moqr_log_complete_record(log, 2, 0, 0) == MOQR_OK);

    /* Evicting the group drops the chunked record's bytes and decrefs slices. */
    MOQ_TEST_CHECK(moqr_log_evict_groups(log, 1) == 1);
    moqr_log_get_stats(log, &st);
    MOQ_TEST_CHECK_EQ_U64(st.retained_bytes, 0);
    MOQ_TEST_CHECK_EQ_U64(st.record_count, 0);

    STEP_CHECK(log);
    moqr_log_destroy(log);
    MOQ_TEST_CHECK_EQ_INT((int)ca.live_bytes, 0);
    MOQ_TEST_CHECK_EQ_INT((int)(ca.allocs - ca.frees), 0);
    MOQ_TEST_PASS("chunk_byte_accounting_eviction");
    return failures;
}

int
main(void)
{
    int failures = 0;
    failures += test_append_and_cursor_roundtrip();
    failures += test_ordering_rules();
    failures += test_final_evidence_preflight();
    failures += test_final_evidence_atomicity();
    failures += test_eviction_and_refcounts();
    failures += test_eviction_under_cursor();
    failures += test_byte_budget_and_caps();
    failures += test_vector_growth();
    failures += test_group_wide_duplicate_identity();
    failures += test_cfg_prefix_consumers();
    failures += test_capacity_structure_bytes();
    failures += test_trace_hash_determinism();
    failures += test_capacity_describe_bounds();
    failures += test_chunk_open_append_complete();
    failures += test_chunk_abandon();
    failures += test_chunk_ordering_and_dup();
    failures += test_chunk_capacity();
    failures += test_chunk_byte_accounting_eviction();
    failures += g_failures_capacity;
    return failures == 0 ? 0 : 1; /* exit status truncates to 8 bits */
}
