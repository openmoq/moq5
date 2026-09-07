/* Config parser tests: strict schema, loud failures, capacity-only path. */

#include "../cli/admin_listen_layout.h"
#include "../cli/config.h"
#include "../cli/snapshot.h"
#include "../admin/moqr_admin.h"
#include "../cli/info_doc.h"
#include "../cli/shards_doc.h"

#include <pthread.h>
#include <sched.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>
#include <errno.h>

#include "../../../tests/unit/test_support.h"

static moqr_result_t
parse(const char *json, moqr_cli_config_t *out, char *err, size_t errlen)
{
    return moqr_cli_config_parse(json, strlen(json), out, err, errlen);
}

/* -- the metrics-snapshot publish/collect protocol --------------------------- */

/* A fixed "newest requested epoch" source. */
static uint64_t
fixed_epoch(void *ctx)
{
    return *(const uint64_t *)ctx;
}

/* A scripted epoch source: returns vals[] in call order (saturating at the
 * last value) — the deterministic injection seam that requests E+1 exactly
 * between the coordinator's copy and its re-read. */
typedef struct epoch_seq {
    uint64_t vals[8];
    int      idx;
    int      count;
} epoch_seq_t;

static uint64_t
seq_epoch(void *ctx)
{
    epoch_seq_t *e = ctx;
    uint64_t v = e->vals[e->idx];
    if (e->idx + 1 < e->count) {
        e->idx++;
    }
    return v;
}

/* A cycling epoch source: repeats vals[0..count-1] forever — models a
 * request storm where every produced document is obsoleted post-production
 * yet each fresh collect still finds a matching (stale) target. */
typedef struct epoch_cycle {
    uint64_t vals[4];
    int      count;
    int      idx;
} epoch_cycle_t;

static uint64_t
cycle_epoch(void *ctx)
{
    epoch_cycle_t *e = ctx;
    uint64_t v = e->vals[e->idx % e->count];
    e->idx++;
    return v;
}

/* Newest-epoch-only coalescing: a set renders only when every row carries
 * exactly the newest requested epoch; a lagging or mixed set is "epoch
 * incomplete"; and an all-E set copied just before E+1 was requested is
 * DISCARDED, never handed out. */
static int
test_snapshot_protocol(void)
{
    int failures = 0;
    const moq_alloc_t *alloc = moq_alloc_default();
    moqr_cli_snapshot_t snap;
    MOQ_TEST_CHECK(moqr_cli_snapshot_init(&snap, 2, alloc) == MOQR_OK);

    moqr_cli_snapshot_stats_t st;
    memset(&st, 0, sizeof(st));
    moqr_cli_snapshot_stats_t rows[2];
    uint64_t e = 1, got = 0;

    /* Nothing published yet: epoch 1 is incomplete. */
    MOQ_TEST_CHECK(moqr_cli_snapshot_collect(&snap, fixed_epoch, &e, rows,
                                             &got) == MOQR_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK_EQ_U64(got, 1);

    /* One lane published: still incomplete — never a partial render. */
    st.core.ingested_total = 7;
    st.lane_wakes = 3;
    moqr_cli_snapshot_publish(&snap, 0, &st, 1);
    MOQ_TEST_CHECK(moqr_cli_snapshot_collect(&snap, fixed_epoch, &e, rows,
                                             &got) == MOQR_ERR_WOULD_BLOCK);

    /* Both lanes at epoch 1: one coherent set. */
    st.core.ingested_total = 9;
    st.lane_wakes = 5;
    moqr_cli_snapshot_publish(&snap, 1, &st, 1);
    MOQ_TEST_CHECK(moqr_cli_snapshot_collect(&snap, fixed_epoch, &e, rows,
                                             &got) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(got, 1);
    MOQ_TEST_CHECK_EQ_U64(rows[0].core.ingested_total, 7);
    MOQ_TEST_CHECK_EQ_U64(rows[0].lane_wakes, 3);
    MOQ_TEST_CHECK_EQ_U64(rows[1].core.ingested_total, 9);

    /* Back-to-back epochs: 2 requested, one lane still at 1 — a MIXED set
     * never renders. */
    e = 2;
    st.core.ingested_total = 17;
    moqr_cli_snapshot_publish(&snap, 0, &st, 2);
    MOQ_TEST_CHECK(moqr_cli_snapshot_collect(&snap, fixed_epoch, &e, rows,
                                             &got) == MOQR_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK_EQ_U64(got, 2);
    st.core.ingested_total = 19;
    moqr_cli_snapshot_publish(&snap, 1, &st, 2);
    MOQ_TEST_CHECK(moqr_cli_snapshot_collect(&snap, fixed_epoch, &e, rows,
                                             &got) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(got, 2);
    MOQ_TEST_CHECK_EQ_U64(rows[0].core.ingested_total, 17);

    /* The copy-then-request-then-render race, pinned by injection: every
     * row carries 2, and the scripted source reports 2 for the target read
     * but 3 for the post-copy re-read — the all-2 set MUST be discarded
     * and the retry reported incomplete against 3, never rendered as 2. */
    epoch_seq_t sq = { { 2, 3, 3, 3 }, 0, 4 };
    MOQ_TEST_CHECK(moqr_cli_snapshot_collect(&snap, seq_epoch, &sq, rows,
                                             &got) == MOQR_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK_EQ_U64(got, 3);

    /* Once the lanes catch up to 3, the newest epoch renders. */
    st.core.ingested_total = 23;
    moqr_cli_snapshot_publish(&snap, 0, &st, 3);
    moqr_cli_snapshot_publish(&snap, 1, &st, 3);
    e = 3;
    MOQ_TEST_CHECK(moqr_cli_snapshot_collect(&snap, fixed_epoch, &e, rows,
                                             &got) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(got, 3);

    /* Argument hygiene. */
    MOQ_TEST_CHECK(moqr_cli_snapshot_collect(NULL, fixed_epoch, &e, rows,
                                             &got) == MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(moqr_cli_snapshot_collect(&snap, NULL, NULL, rows,
                                             &got) == MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(moqr_cli_snapshot_collect(&snap, fixed_epoch, &e, NULL,
                                             &got) == MOQR_ERR_INVAL);
    moqr_cli_snapshot_publish(&snap, 99, &st, 4);   /* bad lane: no-op */

    moqr_cli_snapshot_destroy(&snap);
    MOQ_TEST_PASS("snapshot_protocol");
    return failures;
}

/* A produce probe: counts serializations and echoes a scripted result —
 * proving what was produced, when, and that discarded output never leaks
 * to the caller as OK. */
typedef struct produce_probe {
    int           calls;
    uint64_t      last_epoch;
    moqr_result_t rc;
} produce_probe_t;

static moqr_result_t
probe_produce(void *ctx, const moqr_cli_snapshot_stats_t *rows,
              uint64_t epoch)
{
    (void)rows;
    produce_probe_t *p = ctx;
    p->calls++;
    p->last_epoch = epoch;
    return p->rc;
}

/* The emission gate: moqr_cli_snapshot_render licenses emission only when
 * NO newer epoch was requested up to the post-production re-read — a
 * request injected AFTER collection and serialization (the window collect()
 * alone cannot see) discards the produced document. */
static int
test_snapshot_render_gate(void)
{
    int failures = 0;
    const moq_alloc_t *alloc = moq_alloc_default();
    moqr_cli_snapshot_t snap;
    MOQ_TEST_CHECK(moqr_cli_snapshot_init(&snap, 2, alloc) == MOQR_OK);
    moqr_cli_snapshot_stats_t st;
    memset(&st, 0, sizeof(st));
    st.shard_cap = MOQR_CLI_CAP_VALID;
    moqr_cli_snapshot_stats_t rows[2];
    moqr_cli_snapshot_publish(&snap, 0, &st, 1);
    moqr_cli_snapshot_publish(&snap, 1, &st, 1);
    uint64_t e = 1, got = 0;

    /* Stable epoch: produced exactly once, emission licensed. */
    produce_probe_t pr = { 0, 0, MOQR_OK };
    MOQ_TEST_CHECK(moqr_cli_snapshot_render(&snap, fixed_epoch, &e, rows,
                                            probe_produce, &pr, &got) ==
                   MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(got, 1);
    MOQ_TEST_CHECK_EQ_INT(pr.calls, 1);
    MOQ_TEST_CHECK_EQ_U64(pr.last_epoch, 1);

    /* Injection AFTER collection + serialization: the scripted source
     * reports 1 for both of collect's reads (a coherent all-1 set is
     * copied AND produced), then 2 on the post-production read — the
     * produced epoch-1 document must be DISCARDED, the retry reported
     * incomplete against 2, and OK-at-1 never returned. */
    epoch_seq_t sq = { { 1, 1, 2, 2, 2 }, 0, 5 };
    pr = (produce_probe_t){ 0, 0, MOQR_OK };
    MOQ_TEST_CHECK(moqr_cli_snapshot_render(&snap, seq_epoch, &sq, rows,
                                            probe_produce, &pr, &got) ==
                   MOQR_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK_EQ_U64(got, 2);
    MOQ_TEST_CHECK_EQ_INT(pr.calls, 1);   /* produced once, then discarded */
    MOQ_TEST_CHECK_EQ_U64(pr.last_epoch, 1);

    /* A producer failure (the renderer's suppression) returns verbatim. */
    pr = (produce_probe_t){ 0, 0, MOQR_ERR_INVAL };
    e = 1;
    MOQ_TEST_CHECK(moqr_cli_snapshot_render(&snap, fixed_epoch, &e, rows,
                                            probe_produce, &pr, &got) ==
                   MOQR_ERR_INVAL);
    MOQ_TEST_CHECK_EQ_U64(got, 1);
    MOQ_TEST_CHECK_EQ_INT(pr.calls, 1);

    /* A POISONED lane snapshot (shard stats refused, published invalid)
     * suppresses the complete epoch: INVAL, and the producer is never
     * called — zeroed stand-ins never render. */
    st.shard_cap = MOQR_CLI_CAP_REFUSED;
    moqr_cli_snapshot_publish(&snap, 1, &st, 2);
    st.shard_cap = MOQR_CLI_CAP_VALID;
    moqr_cli_snapshot_publish(&snap, 0, &st, 2);
    pr = (produce_probe_t){ 0, 0, MOQR_OK };
    e = 2;
    MOQ_TEST_CHECK(moqr_cli_snapshot_render(&snap, fixed_epoch, &e, rows,
                                            probe_produce, &pr, &got) ==
                   MOQR_ERR_INVAL);
    MOQ_TEST_CHECK_EQ_U64(got, 2);
    MOQ_TEST_CHECK_EQ_INT(pr.calls, 0);
    /* The lane recovering (a valid republish) un-poisons the next epoch. */
    moqr_cli_snapshot_publish(&snap, 1, &st, 3);
    moqr_cli_snapshot_publish(&snap, 0, &st, 3);
    e = 3;
    MOQ_TEST_CHECK(moqr_cli_snapshot_render(&snap, fixed_epoch, &e, rows,
                                            probe_produce, &pr, &got) ==
                   MOQR_OK);
    MOQ_TEST_CHECK_EQ_INT(pr.calls, 1);

    /* Retry exhaustion under a request storm: every attempt collects a
     * coherent all-1 set (the cycling source's collect reads return 1),
     * produces it, and is then obsoleted by a post-production read of 7 —
     * when the retries run out, the reported epoch must be the NEWEST
     * OBSERVED request (7), never the last produced one (1). */
    moqr_cli_snapshot_publish(&snap, 0, &st, 1);
    moqr_cli_snapshot_publish(&snap, 1, &st, 1);
    epoch_cycle_t cy = { { 1, 1, 7, 0 }, 3, 0 };
    pr = (produce_probe_t){ 0, 0, MOQR_OK };
    MOQ_TEST_CHECK(moqr_cli_snapshot_render(&snap, cycle_epoch, &cy, rows,
                                            probe_produce, &pr, &got) ==
                   MOQR_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK_EQ_U64(got, 7);
    MOQ_TEST_CHECK(pr.calls >= 2);        /* it really cycled */
    MOQ_TEST_CHECK_EQ_U64(pr.last_epoch, 1);

    /* An incomplete epoch never produces anything. */
    pr = (produce_probe_t){ 0, 0, MOQR_OK };
    e = 5;
    MOQ_TEST_CHECK(moqr_cli_snapshot_render(&snap, fixed_epoch, &e, rows,
                                            probe_produce, &pr, &got) ==
                   MOQR_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK_EQ_INT(pr.calls, 0);
    MOQ_TEST_CHECK(moqr_cli_snapshot_render(&snap, fixed_epoch, &e, rows,
                                            NULL, NULL, &got) ==
                   MOQR_ERR_INVAL);
    moqr_cli_snapshot_destroy(&snap);
    MOQ_TEST_PASS("snapshot_render_gate");
    return failures;
}

/* -- the snapshot protocol under real threads (TSan-covered) ----------------- */

typedef struct snap_lane {
    moqr_cli_snapshot_t *snap;
    uint32_t             lane;
    _Atomic unsigned    *epoch;   /* the coordinator's requested counter */
    _Atomic int         *stop;
    int                  errs;
} snap_lane_t;

/* Encode (lane, epoch) into a stat so the coordinator can prove every
 * accepted set is epoch-coherent per row. */
static uint64_t
snap_encode(uint32_t lane, unsigned epoch)
{
    return (uint64_t)lane * 1000000u + epoch;
}

static void *
snap_lane_run(void *arg)
{
    snap_lane_t *t = arg;
    unsigned last = 0;
    while (!atomic_load(t->stop)) {
        unsigned e = atomic_load(t->epoch);
        if (e != last) {
            moqr_cli_snapshot_stats_t st;
            memset(&st, 0, sizeof(st));
            st.shard_cap = MOQR_CLI_CAP_VALID;
            st.core.ingested_total = snap_encode(t->lane, e);
            moqr_cli_snapshot_publish(t->snap, t->lane, &st, e);
            last = e;
        }
        sched_yield();
    }
    return NULL;
}

static uint64_t
atomic_epoch_read(void *ctx)
{
    return (uint64_t)atomic_load((_Atomic unsigned *)ctx);
}

/* Lanes publish rows concurrently under their dump-only mutexes while the
 * coordinator collects: every accepted set is coherent (each row's payload
 * encodes exactly the accepted epoch — a mixed or stale render would break
 * the encoding), and the coordinator touches ONLY the snapshot rows. After
 * join, one direct read needs no epoch. */
static int
test_snapshot_protocol_mt(void)
{
    int failures = 0;
    enum { SNAP_LANES = 3, SNAP_EPOCHS = 64 };
    const moq_alloc_t *alloc = moq_alloc_default();
    moqr_cli_snapshot_t snap;
    MOQ_TEST_CHECK(moqr_cli_snapshot_init(&snap, SNAP_LANES, alloc) ==
                   MOQR_OK);
    _Atomic unsigned epoch = 0;
    _Atomic int stop = 0;
    snap_lane_t lanes[SNAP_LANES];
    pthread_t tids[SNAP_LANES];
    for (uint32_t i = 0; i < SNAP_LANES; i++) {
        lanes[i] = (snap_lane_t){ &snap, i, &epoch, &stop, 0 };
        MOQ_TEST_CHECK(pthread_create(&tids[i], NULL, snap_lane_run,
                                      &lanes[i]) == 0);
    }
    moqr_cli_snapshot_stats_t rows[SNAP_LANES];
    int rendered = 0;
    for (unsigned e = 1; e <= SNAP_EPOCHS; e++) {
        atomic_store(&epoch, e);
        for (int spin = 0; spin < 2000000; spin++) {
            uint64_t got = 0;
            moqr_result_t rc = moqr_cli_snapshot_collect(
                &snap, atomic_epoch_read, &epoch, rows, &got);
            if (rc == MOQR_OK) {
                /* Coherent by construction: every row's payload encodes
                 * the exact accepted epoch. */
                MOQ_TEST_CHECK_EQ_U64(got, e);
                for (uint32_t i = 0; i < SNAP_LANES; i++) {
                    MOQ_TEST_CHECK_EQ_U64(rows[i].core.ingested_total,
                                          snap_encode(i, e));
                }
                rendered++;
                break;
            }
            MOQ_TEST_CHECK(rc == MOQR_ERR_WOULD_BLOCK);
            sched_yield();
        }
    }
    MOQ_TEST_CHECK_EQ_INT(rendered, SNAP_EPOCHS);
    atomic_store(&stop, 1);
    for (uint32_t i = 0; i < SNAP_LANES; i++) {
        MOQ_TEST_CHECK(pthread_join(tids[i], NULL) == 0);
        MOQ_TEST_CHECK_EQ_INT(lanes[i].errs, 0);
    }
    /* Post-join: the rows read directly, no further epoch requested. */
    uint64_t e_final = SNAP_EPOCHS, got = 0;
    MOQ_TEST_CHECK(moqr_cli_snapshot_collect(&snap, fixed_epoch, &e_final,
                                             rows, &got) == MOQR_OK);
    moqr_cli_snapshot_destroy(&snap);
    MOQ_TEST_PASS("snapshot_protocol_mt");
    return failures;
}

/* A failing allocator: succeeds until `fail_after` allocations have been
 * served, then refuses — the coordinator's allocation-failure pin. */
typedef struct failing_alloc {
    moq_alloc_t vt;
    int         served;
    int         fail_after;   /* allocations to allow before refusing */
} failing_alloc_t;

static void *
fa_alloc(size_t n, void *ctx)
{
    failing_alloc_t *a = ctx;
    if (a->served >= a->fail_after) {
        return NULL;
    }
    a->served++;
    return malloc(n);
}

static void *
fa_realloc(void *p, size_t o, size_t n, void *ctx)
{
    (void)o;
    (void)ctx;
    return realloc(p, n);
}

static void
fa_free(void *p, size_t n, void *ctx)
{
    (void)n;
    (void)ctx;
    free(p);
}

static void
fa_init(failing_alloc_t *a, int fail_after)
{
    memset(a, 0, sizeof(*a));
    a->vt.ctx = a;
    a->vt.alloc = fa_alloc;
    a->vt.realloc = fa_realloc;
    a->vt.free = fa_free;
    a->fail_after = fail_after;
}

/* A counting allocator that tracks live bytes: the independent oracle for the
 * admission delta — it measures the bytes a runtime actually requests, with no
 * knowledge of the capacity formula, so it catches a model term the formula
 * omits or mis-sizes. */
typedef struct count_alloc {
    moq_alloc_t vt;
    long        live;
} count_alloc_t;

static void *
ka_alloc(size_t n, void *ctx)
{
    count_alloc_t *a = ctx;
    void         *p = malloc(n);
    if (p != NULL) {
        a->live += (long)n;
    }
    return p;
}

static void *
ka_realloc(void *p, size_t o, size_t n, void *ctx)
{
    count_alloc_t *a = ctx;
    void          *q = realloc(p, n);
    if (q != NULL) {
        a->live += (long)n - (long)o;
    }
    return q;
}

static void
ka_free(void *p, size_t n, void *ctx)
{
    count_alloc_t *a = ctx;
    if (p != NULL) {
        a->live -= (long)n;
    }
    free(p);
}

static void
ka_init(count_alloc_t *a)
{
    memset(a, 0, sizeof(*a));
    a->vt.ctx = a;
    a->vt.alloc = ka_alloc;
    a->vt.realloc = ka_realloc;
    a->vt.free = ka_free;
}

static void
no_emit(void *ectx, const char *doc, size_t len, uint64_t epoch)
{
    (void)doc;
    (void)len;
    (void)epoch;
    (*(int *)ectx)++;
}

/* Coordinator allocation failures surface as NOMEM — a distinct, retryable
 * result the CLI diagnoses (never OK, never mistaken for an incomplete
 * epoch) and NOTHING is emitted. Both transient allocations are covered:
 * the row copy and the document buffer. Also the lane bound: the snapshot
 * refuses more lanes than the coordinator's fixed view assembly holds. */
static int
test_snapshot_coord_failures(void)
{
    int failures = 0;
    moqr_obs_labels_t labels[2] = {
        { 0, "sim", "draft-18" },
        { 1, "sim", "draft-18" },
    };
    for (int fail_at = 0; fail_at <= 2; fail_at++) {
        /* fail_at=0: the transient row copy fails; =1: the initial
         * document buffer fails; =2: the document REGROW fails (the
         * 2-shard exposition outgrows the first 32 KiB buffer). init's
         * own row-block allocation is budgeted separately (+1). */
        failing_alloc_t fa;
        fa_init(&fa, 1 + fail_at);
        moqr_cli_snapshot_t snap;
        MOQ_TEST_CHECK(moqr_cli_snapshot_init(&snap, 2, &fa.vt) == MOQR_OK);
        moqr_cli_snapshot_stats_t st;
        memset(&st, 0, sizeof(st));
        st.shard_cap = MOQR_CLI_CAP_VALID;
        moqr_cli_snapshot_publish(&snap, 0, &st, 1);
        moqr_cli_snapshot_publish(&snap, 1, &st, 1);
        uint64_t e = 1, got = 0;
        int emitted = 0;
        MOQ_TEST_CHECK(moqr_cli_coord_dump(&snap, labels, fixed_epoch, &e,
                                           no_emit, &emitted, &got) ==
                       MOQR_ERR_NOMEM);
        MOQ_TEST_CHECK_EQ_INT(emitted, 0);
        /* EVERY failure names the epoch it served — the caller's per-epoch
         * de-spam keys on it, so a zero here would mute the diagnostic. */
        MOQ_TEST_CHECK_EQ_U64(got, 1);
        MOQ_TEST_CHECK(moqr_cli_coord_dump(&snap, labels, NULL, NULL,
                                           no_emit, &emitted, &got) ==
                       MOQR_ERR_INVAL);
        /* Recovery: with allocations flowing again, the same epoch dumps. */
        fa.fail_after = 1000000;
        MOQ_TEST_CHECK(moqr_cli_coord_dump(&snap, labels, fixed_epoch, &e,
                                           no_emit, &emitted, &got) ==
                       MOQR_OK);
        MOQ_TEST_CHECK_EQ_INT(emitted, 1);
        MOQ_TEST_CHECK_EQ_U64(got, 1);
        moqr_cli_snapshot_destroy(&snap);
    }
    /* The lane bound: MOQR_SHARDS_MAX is accepted, one past it refused. */
    {
        const moq_alloc_t *alloc = moq_alloc_default();
        moqr_cli_snapshot_t snap;
        MOQ_TEST_CHECK(moqr_cli_snapshot_init(&snap, MOQR_SHARDS_MAX,
                                              alloc) == MOQR_OK);
        moqr_cli_snapshot_destroy(&snap);
        MOQ_TEST_CHECK(moqr_cli_snapshot_init(&snap, MOQR_SHARDS_MAX + 1u,
                                              alloc) == MOQR_ERR_INVAL);
    }
    MOQ_TEST_PASS("snapshot_coord_failures");
    return failures;
}

/* -- the PRODUCTION coordinator flow over a live runtime --------------------- */

/* Captured emissions from moqr_cli_coord_dump (the production emit seam). */
typedef struct emit_cap {
    int      calls;
    uint64_t last_epoch;
    int      bad_docs;
} emit_cap_t;

static void
capture_emit(void *ectx, const char *doc, size_t len, uint64_t epoch)
{
    emit_cap_t *c = ectx;
    c->calls++;
    c->last_epoch = epoch;
    if (len == 0 || strlen(doc) != len ||
        strstr(doc, "moqrelay_process_objects_ingested_total") == NULL ||
        strstr(doc, "moqrelay_journal_epoch{shard=\"0\"") == NULL ||
        strstr(doc, "moqrelay_channel_enqueues_total") == NULL) {
        c->bad_docs++;
    }
}

typedef struct coord_lane {
    moqr_shards_t       *s;
    moqr_cli_snapshot_t *snap;
    uint16_t             shard;
    _Atomic unsigned    *epoch;
    _Atomic int         *stop;
    int                  errs;
} coord_lane_t;

/* A lane: churns its shard's announce state (journals and mailboxes mutate
 * continuously on BOTH shards through the mirrors) while publishing its
 * metrics row for every newly requested epoch — the production lane shape. */
static void *
coord_lane_run(void *arg)
{
    coord_lane_t *t = arg;
    moqr_binding_t pub;
    if (moqr_core_binding_open(moqr_shards_core(t->s, t->shard), 1, &pub) !=
        MOQR_OK) {
        t->errs++;
        return NULL;
    }
    char nm[16];
    snprintf(nm, sizeof(nm), "cp%u", (unsigned)t->shard);
    moq_bytes_t part = { (const uint8_t *)nm, (uint32_t)strlen(nm) };
    moqr_ns_t ns = { &part, 1 };
    unsigned last = 0;
    bool announced = false;
    while (!atomic_load(t->stop)) {
        /* Journal churn: announce/withdraw flips candidates, mirrors, and
         * journal epochs on both shards every few steps. */
        moqr_result_t rc =
            announced
                ? moqr_core_unannounce(moqr_shards_core(t->s, t->shard), pub,
                                       ns)
                : moqr_core_announce(moqr_shards_core(t->s, t->shard), pub,
                                     ns);
        if (rc != MOQR_OK) {
            t->errs++;
        }
        announced = !announced;
        for (int r = 0; r < 2; r++) {
            uint64_t mask = 0;
            if (moqr_shards_step_shard(t->s, t->shard, 1000, &mask) !=
                MOQR_OK) {
                t->errs++;
            }
        }
        unsigned e = atomic_load(t->epoch);
        if (e != last) {
            moqr_cli_snapshot_stats_t st;
            memset(&st, 0, sizeof(st));
            moqr_core_get_stats(moqr_shards_core(t->s, t->shard), &st.core);
            moqr_bind_get_stats(moqr_shards_bind(t->s, t->shard), &st.bind);
            st.shard_cap =
                (moqr_shards_get_stats(t->s, t->shard, &st.shard) == MOQR_OK)
                    ? MOQR_CLI_CAP_VALID
                    : MOQR_CLI_CAP_REFUSED;
            moqr_cli_snapshot_publish(t->snap, t->shard, &st, e);
            last = e;
        }
        sched_yield();
    }
    return NULL;
}

/* The PRODUCTION coordinator (moqr_cli_coord_dump — the exact seam
 * cmd_serve_lanes drives) against a LIVE runtime: lanes churn journals and
 * mailboxes on their own threads while the coordinator renders complete
 * epochs from the published rows alone. The seam is never handed the shard
 * runtime, so route/journal traversal is impossible by construction; under
 * TSan this test additionally proves the whole flow touches nothing a live
 * lane mutates. */
static int
test_snapshot_coord_production(void)
{
    int failures = 0;
    enum { COORD_EPOCHS = 12 };
    const moq_alloc_t *alloc = moq_alloc_default();
    moqr_shards_cfg_t scfg;
    moqr_shards_cfg_init_sized(&scfg, sizeof(scfg), alloc);
    scfg.shards = 2;
    scfg.live_visibility = true;
    moqr_shards_t *s = NULL;
    MOQ_TEST_CHECK(moqr_shards_create(&scfg, &s) == MOQR_OK);
    moqr_cli_snapshot_t snap;
    MOQ_TEST_CHECK(moqr_cli_snapshot_init(&snap, 2, alloc) == MOQR_OK);
    moqr_obs_labels_t labels[2] = {
        { 0, "sim", "draft-18" },
        { 1, "sim", "draft-18" },
    };

    _Atomic unsigned epoch = 0;
    _Atomic int stop = 0;
    coord_lane_t lanes[2] = {
        { s, &snap, 0, &epoch, &stop, 0 },
        { s, &snap, 1, &epoch, &stop, 0 },
    };
    pthread_t tids[2];
    MOQ_TEST_CHECK(pthread_create(&tids[0], NULL, coord_lane_run,
                                  &lanes[0]) == 0);
    MOQ_TEST_CHECK(pthread_create(&tids[1], NULL, coord_lane_run,
                                  &lanes[1]) == 0);

    emit_cap_t cap = { 0, 0, 0 };
    int rendered = 0;
    for (unsigned e = 1; e <= COORD_EPOCHS; e++) {
        atomic_store(&epoch, e);
        for (int spin = 0; spin < 2000000; spin++) {
            uint64_t got = 0;
            moqr_result_t rc =
                moqr_cli_coord_dump(&snap, labels, atomic_epoch_read, &epoch,
                                    capture_emit, &cap, &got);
            if (rc == MOQR_OK) {
                MOQ_TEST_CHECK_EQ_U64(got, e);
                MOQ_TEST_CHECK_EQ_U64(cap.last_epoch, e);
                rendered++;
                break;
            }
            MOQ_TEST_CHECK(rc == MOQR_ERR_WOULD_BLOCK);
            sched_yield();
        }
    }
    MOQ_TEST_CHECK_EQ_INT(rendered, COORD_EPOCHS);
    MOQ_TEST_CHECK_EQ_INT(cap.calls, COORD_EPOCHS);
    MOQ_TEST_CHECK_EQ_INT(cap.bad_docs, 0);
    atomic_store(&stop, 1);
    MOQ_TEST_CHECK(pthread_join(tids[0], NULL) == 0);
    MOQ_TEST_CHECK(pthread_join(tids[1], NULL) == 0);
    MOQ_TEST_CHECK_EQ_INT(lanes[0].errs, 0);
    MOQ_TEST_CHECK_EQ_INT(lanes[1].errs, 0);
    moqr_cli_snapshot_destroy(&snap);
    moqr_shards_destroy(s);
    MOQ_TEST_PASS("snapshot_coord_production");
    return failures;
}


/*
 * F5: the single-lane composition now allocates one snapshot row for its
 * whole lifetime, so the ceiling must count it. It is a permanent request,
 * not a transient render buffer, and the model's rule is that permanent
 * requests are counted.
 */
static int
test_k1_counts_its_snapshot_row(void)
{
    int failures = 0;
    const moq_alloc_t *alloc = moq_alloc_default();
    moqr_cli_config_t cfg;
    char err[256];
    const char *json =
        "{\"listener\":{\"port\":4433,\"cert\":\"c\",\"key\":\"k\"}}";
    MOQ_TEST_CHECK_EQ_INT(parse(json, &cfg, err, sizeof(err)), MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(moqr_cli_total_lanes(&cfg), 1u);

    moqr_cli_capacity_t cap;
    MOQ_TEST_CHECK_EQ_INT(moqr_cli_describe_capacity(&cfg, alloc, 0, &cap),
                          MOQR_OK);

    /* Exactly one row, and exactly once. */
    MOQ_TEST_CHECK_EQ_U64(cap.cli_runtime_bytes, moqr_cli_snapshot_bytes(1u));
    MOQ_TEST_CHECK(cap.cli_runtime_bytes > 0u);

    /* And it is inside the reported total, not reported beside it. */
    uint64_t parts = cap.core_structure_bytes + cap.core_payload_bytes +
                     cap.bind_structure_bytes + cap.trace_bytes +
                     cap.cross_shard_bytes + cap.cli_runtime_bytes;
    MOQ_TEST_CHECK_EQ_U64(cap.total_bytes, parts);
    return failures;
}

/* -- the admin endpoint section --------------------------------------------
 *
 * Strict by construction: disabled by default, exactly one endpoint mode,
 * loopback-only TCP in v1, and a UDS path that startup will refuse to share
 * with any pre-existing entry. Every rejection below is a closed refusal, not
 * a permissive fallback. */
static int
test_admin_config(void)
{
    int failures = 0;
    moqr_cli_config_t cfg;
    char err[192];
    static const char *BASE =
        "{\"listener\":{\"port\":4433,\"versions\":[18]},";

    /* Absent: disabled, and nothing else asserted about it. */
    {
        const char *j = "{\"listener\":{\"port\":4433,\"versions\":[18]}}";
        if (parse(j, &cfg, err, sizeof(err)) != MOQR_OK) {
            printf("  a config without an admin section was rejected: %s\n", err);
            failures++;
        } else if (cfg.admin.enabled) {
            printf("  the admin endpoint defaulted to enabled\n");
            failures++;
        }
    }

    /* Accepted shapes. */
    static const struct { const char *name; const char *frag; } ok[] = {
        { "tcp default host", "\"admin\":{\"tcp\":{\"port\":9109}}" },
        { "tcp explicit v4",  "\"admin\":{\"tcp\":{\"host\":\"127.0.0.1\",\"port\":9109}}" },
        { "tcp explicit v6",  "\"admin\":{\"tcp\":{\"host\":\"::1\",\"port\":9109}}" },
        { "tcp loopback /8",  "\"admin\":{\"tcp\":{\"host\":\"127.9.9.9\",\"port\":1}}" },
        { "explicit disable",  "\"admin\":{\"enabled\":false}" },
    };
    for (size_t i = 0; i < sizeof(ok) / sizeof(ok[0]); i++) {
        char j[512];
        (void)snprintf(j, sizeof(j), "%s%s}", BASE, ok[i].frag);
        if (parse(j, &cfg, err, sizeof(err)) != MOQR_OK) {
            printf("  admin/%s was rejected: %s\n", ok[i].name, err);
            failures++;
        }
    }
    /* An accepted TCP section carries the documented default host. */
    {
        char j[512];
        (void)snprintf(j, sizeof(j), "%s%s}", BASE,
                       "\"admin\":{\"tcp\":{\"port\":9109}}");
        if (parse(j, &cfg, err, sizeof(err)) == MOQR_OK) {
            if (!cfg.admin.enabled ||
                cfg.admin.mode != MOQR_CLI_ADMIN_TCP ||
                strcmp(cfg.admin.host, "127.0.0.1") != 0 ||
                cfg.admin.port != 9109) {
                printf("  the accepted TCP admin section did not materialise\n");
                failures++;
            }
        }
    }
    /* v1 is TCP loopback only. UDS is DEFERRED, not hidden: pathname unlink
     * cannot be made to target the inode this process created, and a
     * protected-parent assumption dressed up as an inode-safe guarantee would
     * be a false promise. `admin.uds` therefore fails through the ordinary
     * unknown-key rule, like any other key the schema does not define. */
    {
        char j[512];
        err[0] = '\0';
        (void)snprintf(j, sizeof(j), "%s%s}", BASE,
                       "\"admin\":{\"uds\":{\"path\":\"/tmp/a\"}}");
        if (parse(j, &cfg, err, sizeof(err)) == MOQR_OK) {
            printf("  admin.uds was ACCEPTED\n");
            failures++;
        } else if (strstr(err, "unknown key") == NULL) {
            printf("  admin.uds did not fail as an unknown key: %s\n", err);
            failures++;
        }
    }

    /* Closed refusals. Each names the rule it breaks. */
    static const struct { const char *name; const char *frag; } bad[] = {
        { "unknown key",        "\"admin\":{\"tcp\":{\"port\":9109},\"nope\":1}" },
        { "unknown tcp key",    "\"admin\":{\"tcp\":{\"port\":9109,\"nope\":1}}" },
        { "neither endpoint",   "\"admin\":{\"enabled\":true}" },
        { "uds is unknown",     "\"admin\":{\"uds\":{\"path\":\"/tmp/a\"}}" },
        { "uds beside tcp",     "\"admin\":{\"tcp\":{\"port\":9109},\"uds\":{\"path\":\"/tmp/a\"}}" },
        { "port zero refused",  "\"admin\":{\"tcp\":{\"port\":0}}" },
        { "tcp without port",   "\"admin\":{\"tcp\":{\"host\":\"127.0.0.1\"}}" },
        { "port zero",          "\"admin\":{\"tcp\":{\"port\":0}}" },
        { "port too large",     "\"admin\":{\"tcp\":{\"port\":65536}}" },
        { "port not a number",  "\"admin\":{\"tcp\":{\"port\":\"9109\"}}" },
        { "wildcard v4",        "\"admin\":{\"tcp\":{\"host\":\"0.0.0.0\",\"port\":9109}}" },
        { "wildcard v6",        "\"admin\":{\"tcp\":{\"host\":\"::\",\"port\":9109}}" },
        { "empty host",         "\"admin\":{\"tcp\":{\"host\":\"\",\"port\":9109}}" },
        { "public v4",          "\"admin\":{\"tcp\":{\"host\":\"10.0.0.5\",\"port\":9109}}" },
        { "public v4 b",        "\"admin\":{\"tcp\":{\"host\":\"192.0.2.10\",\"port\":9109}}" },
        { "hostname",           "\"admin\":{\"tcp\":{\"host\":\"localhost\",\"port\":9109}}" },
        { "v4-mapped",          "\"admin\":{\"tcp\":{\"host\":\"::ffff:127.0.0.1\",\"port\":9109}}" },
        { "enabled false + tcp","\"admin\":{\"enabled\":false,\"tcp\":{\"port\":9109}}" },
        { "admin not object",   "\"admin\":[]" },
        /* Spellings sscanf accepts and inet_pton does not. Accepting them here
         * defers the refusal to bind time, after readiness decisions have
         * already been made on a config that was called valid. */
        { "leading space",      "\"admin\":{\"tcp\":{\"host\":\" 127.0.0.1\",\"port\":9109}}" },
        { "leading tab",        "\"admin\":{\"tcp\":{\"host\":\"\\t127.0.0.1\",\"port\":9109}}" },
        { "signed octet",       "\"admin\":{\"tcp\":{\"host\":\"+127.0.0.1\",\"port\":9109}}" },
        { "trailing space",     "\"admin\":{\"tcp\":{\"host\":\"127.0.0.1 \",\"port\":9109}}" },
        { "zero-padded octets", "\"admin\":{\"tcp\":{\"host\":\"127.000.000.001\",\"port\":9109}}" },
        { "three octets",       "\"admin\":{\"tcp\":{\"host\":\"127.0.1\",\"port\":9109}}" },
        /* Duplicate keys: JSON objects with repeated names are ambiguous, and
         * last-one-wins silently discards whichever the operator meant. */
        { "duplicate admin",    "\"admin\":{\"tcp\":{\"port\":9109}},\"admin\":{\"uds\":{\"path\":\"/tmp/a\"}}" },
        { "duplicate enabled",  "\"admin\":{\"enabled\":true,\"enabled\":false,\"tcp\":{\"port\":9109}}" },
        { "duplicate tcp",      "\"admin\":{\"tcp\":{\"port\":9109},\"tcp\":{\"port\":9110}}" },
        { "duplicate host",     "\"admin\":{\"tcp\":{\"host\":\"127.0.0.1\",\"host\":\"127.0.0.2\",\"port\":9109}}" },
        { "duplicate port",     "\"admin\":{\"tcp\":{\"port\":9109,\"port\":9110}}" },
        { "tcp not object",     "\"admin\":{\"tcp\":9109}" },
    };
    for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); i++) {
        char j[768];
        (void)snprintf(j, sizeof(j), "%s%s}", BASE, bad[i].frag);
        err[0] = '\0';
        if (parse(j, &cfg, err, sizeof(err)) == MOQR_OK) {
            printf("  admin/%s was ACCEPTED\n", bad[i].name);
            failures++;
        } else if (strstr(err, "admin") == NULL) {
            printf("  admin/%s reported an unrelated error: %s\n",
                   bad[i].name, err);
            failures++;
        }
    }
    return failures;
}

/* -- the logging section ------------------------------------------------------
 *
 * Strict: `format` is `"text"` (the default) or `"json"`, nothing else; every
 * other key, type, value and duplicate refuses with a message naming the
 * section. */
static int
test_logging_config(void)
{
    int failures = 0;
    moqr_cli_config_t cfg;
    char err[192];
    static const char *BASE =
        "{\"listener\":{\"port\":4433,\"versions\":[18]},";

    /* Absent: text. */
    if (parse("{\"listener\":{\"port\":4433,\"versions\":[18]}}", &cfg, err,
              sizeof(err)) != MOQR_OK) {
        printf("  a config without a logging section was rejected: %s\n", err);
        failures++;
    } else if (cfg.logging.format != MOQR_CLI_LOG_TEXT) {
        printf("  logging.format did not default to text\n");
        failures++;
    }
    static const struct { const char *frag; moqr_cli_log_format_t want; } ok[] = {
        { "\"logging\":{}",                    MOQR_CLI_LOG_TEXT },
        { "\"logging\":{\"format\":\"text\"}", MOQR_CLI_LOG_TEXT },
        { "\"logging\":{\"format\":\"json\"}", MOQR_CLI_LOG_JSON },
        /* escaped spellings of the SAME key bytes are the same key */
        { "\"logging\":{\"form\\u0061t\":\"json\"}", MOQR_CLI_LOG_JSON },
        { "\"logg\\u0069ng\":{\"format\":\"json\"}", MOQR_CLI_LOG_JSON },
    };
    for (size_t i = 0; i < sizeof(ok) / sizeof(ok[0]); i++) {
        char j[512];
        (void)snprintf(j, sizeof(j), "%s%s}", BASE, ok[i].frag);
        memset(&cfg, 0xff, sizeof(cfg));
        if (parse(j, &cfg, err, sizeof(err)) != MOQR_OK) {
            printf("  logging/%s was rejected: %s\n", ok[i].frag, err);
            failures++;
        } else if (cfg.logging.format != ok[i].want) {
            printf("  logging/%s materialised as %u\n", ok[i].frag,
                   (unsigned)cfg.logging.format);
            failures++;
        }
    }
    static const struct { const char *name; const char *frag; } bad[] = {
        { "unknown value",     "\"logging\":{\"format\":\"jsonl\"}" },
        { "case",              "\"logging\":{\"format\":\"JSON\"}" },
        { "empty value",       "\"logging\":{\"format\":\"\"}" },
        { "not a string",      "\"logging\":{\"format\":1}" },
        { "null",              "\"logging\":{\"format\":null}" },
        { "unknown key",       "\"logging\":{\"format\":\"json\",\"level\":\"info\"}" },
        { "unknown key alone", "\"logging\":{\"sink\":\"stdout\"}" },
        { "duplicate format",  "\"logging\":{\"format\":\"text\",\"format\":\"json\"}" },
        { "duplicate section", "\"logging\":{\"format\":\"json\"},\"logging\":{\"format\":\"text\"}" },
        { "not an object",     "\"logging\":\"json\"" },
        { "array",             "\"logging\":[\"json\"]" },
        /* a key is its exact bytes: an embedded NUL or any suffix is a
         * DIFFERENT key, not a spelling of format/logging */
        { "format NUL alone",  "\"logging\":{\"format\\u0000\":\"json\"}" },
        { "format NUL suffix", "\"logging\":{\"format\\u0000x\":\"json\"}" },
        { "format suffix",     "\"logging\":{\"formatx\":\"json\"}" },
        { "logging NUL alone", "\"logging\\u0000\":{\"format\":\"json\"}" },
        { "logging NUL suffix","\"logging\\u0000x\":{\"format\":\"json\"}" },
        { "logging suffix",    "\"loggingx\":{\"format\":\"json\"}" },
        { "logging prefix NUL","\"logging\":{\"\\u0000format\":\"json\"}" },
    };
    for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); i++) {
        char j[768];
        (void)snprintf(j, sizeof(j), "%s%s}", BASE, bad[i].frag);
        err[0] = '\0';
        if (parse(j, &cfg, err, sizeof(err)) == MOQR_OK) {
            printf("  logging/%s was ACCEPTED\n", bad[i].name);
            failures++;
        } else if (strstr(err, "logging") == NULL &&
                   strstr(err, "top-level") == NULL) {
            printf("  logging/%s reported an unrelated error: %s\n",
                   bad[i].name, err);
            failures++;
        }
    }
    /* an unknown logging key never sets the format */
    {
        char j[512];
        (void)snprintf(j, sizeof(j), "%s%s}", BASE,
                       "\"logging\":{\"format\\u0000x\":\"json\"}");
        memset(&cfg, 0, sizeof(cfg));
        cfg.logging.format = MOQR_CLI_LOG_JSON;
        if (parse(j, &cfg, err, sizeof(err)) == MOQR_OK ||
            cfg.logging.format == MOQR_CLI_LOG_JSON) {
            printf("  a NUL-suffixed format key selected json\n");
            failures++;
        }
    }
    return failures;
}

/* -- the /api/v1/info document ----------------------------------------------
 *
 * A FINITE projection of the configuration: exact bytes for a dual-listener
 * fixture, no secret under any key, exact-fit and one-byte-short capacity,
 * the renderer's bound honoured, and whole-document refusal of a configured
 * string that is not complete UTF-8. */
static int
info_inputs(const moqr_cli_config_t *cfg, moqr_cli_shard_plan_t *plan,
            moqr_shards_cfg_t *scfg, moqr_shards_limits_t *lim,
            moqr_cli_capacity_t *cap, moqr_cli_info_inputs_t *in)
{
    char perr[192];
    if (moqr_cli_shard_plan(cfg, plan, perr, sizeof(perr)) != MOQR_OK) {
        printf("  info: the shard plan refused: %s\n", perr);
        return 1;
    }
    /* The same pure builders serve and capacity use, with the allocator the
     * resolver requires to size a log record. */
    moqr_cli_build_shards_cfg(cfg, moq_alloc_default(), scfg);
    if (moqr_shards_cfg_resolve(scfg, lim) != MOQR_OK) {
        printf("  info: the resolver refused\n");
        return 1;
    }
    if (moqr_cli_describe_capacity(cfg, moq_alloc_default(), 0, cap) != MOQR_OK) {
        printf("  info: describe_capacity refused\n");
        return 1;
    }
    in->cfg = cfg;
    in->plan = plan;
    in->limits = lim;
    in->capacity = cap;
    in->dual_listener_build = true;
    in->verify_build = false;
    return 0;
}

static int
test_info_document(void)
{
    int failures = 0;
    char err[192];
    static moqr_cli_config_t cfg;
    moqr_cli_shard_plan_t plan;
    moqr_shards_cfg_t scfg;
    moqr_shards_limits_t lim;
    moqr_cli_capacity_t cap;
    moqr_cli_info_inputs_t in;
    static char doc[16384];
    static char want[16384];
    size_t len = 0;
    /* Distinctive values everywhere, canaries in every secret. The WT path
     * carries a quote so escaping is exercised on a real configured string. */
    static const char *J =
        "{\"listener\":{\"host\":\"10.0.0.7\",\"port\":4433,"
        "\"versions\":[16,18],\"lanes\":2,"
        "\"cert\":\"/tmp/CANARY-raw-cert.pem\",\"key\":\"/tmp/CANARY-raw-key.pem\"},"
        "\"webtransport\":{\"host\":\"10.0.0.8\",\"port\":4434,\"path\":\"/moq\\\"p\","
        "\"versions\":[18],\"lanes\":1,"
        "\"cert\":\"/tmp/CANARY-wt-cert.pem\",\"key\":\"/tmp/CANARY-wt-key.pem\"},"
        "\"admin\":{\"tcp\":{\"port\":9109}}}";

    if (parse(J, &cfg, err, sizeof(err)) != MOQR_OK) {
        printf("  info: the dual fixture was rejected: %s\n", err);
        return 1;
    }
    failures += info_inputs(&cfg, &plan, &scfg, &lim, &cap, &in);
    if (failures != 0) {
        return failures;
    }
    if (moqr_cli_info_render(&in, doc, sizeof(doc), &len) != MOQR_OK) {
        printf("  info: the render refused a valid configuration\n");
        return failures + 1;
    }
    /* The EXACT document. Numbers come from the same resolved structs the
     * renderer was given; the structure, keys, order, escaping and every
     * string value are stated here independently. */
    (void)snprintf(want, sizeof(want),
        "{\"api\":\"v1\",\"relay\":\"moq5-relay\","
        "\"build\":{\"dual_listener\":true,\"verify\":false},"
        "\"listeners\":["
        "{\"kind\":\"raw\",\"host\":\"10.0.0.7\",\"port\":4433,\"transport\":\"msquic\","
        "\"offered_versions\":[\"moqt-16\",\"moqt-18\"],\"alpn_set\":\"moqt-16+moqt-18\","
        "\"lanes\":2,\"shards\":{\"first\":0,\"count\":2}},"
        "{\"kind\":\"webtransport\",\"host\":\"10.0.0.8\",\"port\":4434,"
        "\"path\":\"/moq\\\"p\",\"profile\":\"current\",\"transport\":\"wtquic-msquic\","
        "\"offered_subprotocols\":[\"moqt-18\"],\"alpn_set\":\"moqt-18\","
        "\"lanes\":1,\"shards\":{\"first\":2,\"count\":1}}],"
        "\"admin\":{\"host\":\"127.0.0.1\",\"port\":9109,"
        "\"targets\":[\"/metrics\",\"/api/v1/info\",\"/api/v1/shards\"],\"clients\":%u,\"banks\":%u},"
        "\"insecure_skip_verify\":false,"
        "\"budgets\":{\"core\":{\"max_bindings\":%u,\"max_tracks\":%u,\"max_subs\":%u,"
        "\"max_ns_nodes\":%u,\"max_ns_subs\":%u,\"max_intents\":%u,"
        "\"name_intern_bytes\":%u,\"log_max_subgroups\":%u,"
        "\"log_max_objects_per_group\":%u,\"log_max_cursors\":%u,\"linger_us\":%llu},"
        "\"telemetry\":{\"trace_ring_records\":%u},"
        "\"cross_shard\":{\"pools_active\":true,"
        "\"configured_overrides\":{\"journal_entries\":%u,\"mailbox_entries\":%u,"
        "\"demand_channel_entries\":%u,\"pending_demands\":%u,\"subgroup_slots\":%u,"
        "\"demand_channel_bytes\":%llu},"
        "\"resolved\":{\"shards\":%u,\"admit\":%s,\"mailbox_cap\":%u,\"journal_cap\":%u,"
        "\"pending_cap\":%u,\"demand_channel_cap\":%u,\"demand_channel_byte_cap\":%llu,"
        "\"subgroup_slots\":%u,\"trace_ring\":%u,\"pump_turn_messages\":%u,"
        "\"pump_turn_bytes\":%llu,\"usable_bindings\":%u}}},"
        "\"capacity\":{\"model\":\"allocator-request-ceiling\",\"total_bytes\":%llu,"
        "\"per_shard\":{\"core_structure\":%llu,\"core_payload\":%llu,"
        "\"bind_structure\":%llu,\"trace\":%llu},"
        "\"cross_shard_bytes\":%llu,\"cli_runtime_bytes\":%llu,\"admin_bytes\":%llu,"
        "\"usable_bindings_per_shard\":%u,"
        "\"reservations\":{\"admin_thread_stack_bytes\":%llu}}}",
        (unsigned)MOQR_ADMIN_MAX_CLIENTS, (unsigned)MOQR_ADMIN_BANKS,
        cfg.core.max_bindings, cfg.core.max_tracks, cfg.core.max_subs,
        cfg.core.max_ns_nodes, cfg.core.max_ns_subs, cfg.core.max_intents,
        cfg.core.name_intern_bytes, cfg.core.log_max_subgroups,
        cfg.core.log_max_objects_per_group, cfg.core.log_max_cursors,
        (unsigned long long)cfg.core.linger_us,
        cfg.telemetry.trace_ring_records,
        cfg.cross_shard.journal_entries, cfg.cross_shard.mailbox_entries,
        cfg.cross_shard.demand_channel_entries, cfg.cross_shard.pending_demands,
        cfg.cross_shard.subgroup_slots,
        (unsigned long long)cfg.cross_shard.demand_channel_bytes,
        (unsigned)lim.shards, lim.admit ? "true" : "false", lim.mbox_cap,
        lim.jrn_cap, lim.pend_cap, lim.dch_cap,
        (unsigned long long)lim.dch_byte_cap, lim.sg_slots, lim.trace_ring,
        lim.pump_turn_msgs, (unsigned long long)lim.pump_turn_bytes,
        lim.usable_bindings,
        (unsigned long long)cap.total_bytes,
        (unsigned long long)cap.core_structure_bytes,
        (unsigned long long)cap.core_payload_bytes,
        (unsigned long long)cap.bind_structure_bytes,
        (unsigned long long)cap.trace_bytes,
        (unsigned long long)cap.cross_shard_bytes,
        (unsigned long long)cap.cli_runtime_bytes,
        (unsigned long long)cap.admin_bytes,
        cap.usable_bindings_per_shard,
        (unsigned long long)cap.admin_thread_stack_bytes);
    if (strcmp(doc, want) != 0 || len != strlen(want)) {
        size_t i = 0;
        while (doc[i] != '\0' && want[i] != '\0' && doc[i] == want[i]) {
            i++;
        }
        printf("  info: the document differs from the stated bytes at %zu:\n"
               "    got  [%.80s]\n    want [%.80s]\n", i, doc + i, want + i);
        failures++;
    }
    /* No secret, under any key. The canaries were configured in every
     * secret field; none may appear anywhere in the document. */
    if (strstr(doc, "CANARY") != NULL || strstr(doc, ".pem") != NULL) {
        printf("  info: a secret value reached the document\n");
        failures++;
    }
    if (strstr(doc, "\"cert\"") != NULL || strstr(doc, "\"key\"") != NULL ||
        strstr(doc, "\"auth\"") != NULL || strstr(doc, "alpns") != NULL) {
        printf("  info: a forbidden key reached the document\n");
        failures++;
    }
    /* Exact fit: a body of N bytes needs N + 1; N refuses as CAPACITY, and
     * more room never breaks a document that fitted. */
    {
        size_t n2 = 0;
        if (moqr_cli_info_render(&in, doc, len + 1u, &n2) != MOQR_OK || n2 != len) {
            printf("  info: cap = N + 1 did not fit the document\n");
            failures++;
        }
        if (moqr_cli_info_render(&in, doc, len, &n2) != MOQR_ERR_CAPACITY ||
            doc[0] != '\0') {
            printf("  info: cap = N was not refused as CAPACITY with nothing left\n");
            failures++;
        }
        if (moqr_cli_info_render(&in, doc, len + 100u, &n2) != MOQR_OK || n2 != len) {
            printf("  info: extra capacity broke the document\n");
            failures++;
        }
    }
    if ((uint64_t)len > moqr_cli_info_bound()) {
        printf("  info: the document (%zu) exceeds the renderer's bound (%llu)\n",
               len, (unsigned long long)moqr_cli_info_bound());
        failures++;
    }
    return failures;
}

/* The widest configuration the parser admits still fits the bound, and one
 * that is not complete UTF-8 is refused whole. */
static int
test_info_bound_and_refusal(void)
{
    int failures = 0;
    char err[192];
    static moqr_cli_config_t cfg;
    moqr_cli_shard_plan_t plan;
    moqr_shards_cfg_t scfg;
    moqr_shards_limits_t lim;
    moqr_cli_capacity_t cap;
    moqr_cli_info_inputs_t in;
    static char doc[65536];
    static char j[4096];
    size_t len = 0;
    /* 255-byte hosts and path made entirely of quotes: every byte escapes
     * to two; the parser admits them (they are strings of the right length). */
    static char wide[256];
    memset(wide, '"', 255);
    wide[255] = '\0';
    {
        /* In JSON source a quote is written \" so the parser's copy is 255
         * raw quotes. */
        static char esc[512];
        size_t k = 0;
        for (size_t i = 0; i < 254; i++) {
            esc[k++] = '\\';
            esc[k++] = '"';
        }
        esc[k] = '\0';
        /* 254 quotes: the hosts take one more byte than the path, whose
         * leading '/' fills its array to the same 255-byte maximum. */
        (void)snprintf(j, sizeof(j),
            "{\"listener\":{\"host\":\"\\\"%s\",\"port\":65535,\"versions\":[16,18],\"lanes\":4},"
            "\"webtransport\":{\"host\":\"\\\"%s\",\"port\":65535,\"path\":\"/%s\","
            "\"versions\":[16,18],\"lanes\":2,\"cert\":\"c\",\"key\":\"k\","
            "\"profile\":\"d02_rfc9297_compat\","
            "\"origin_policy\":\"allowlist\","
            "\"allowed_origins\":[\"https://zqx-info-5512.example\"]},"
            "\"admin\":{\"tcp\":{\"port\":65535}}}", esc, esc, esc);
    }
    if (parse(j, &cfg, err, sizeof(err)) != MOQR_OK) {
        printf("  info-bound: the wide fixture was rejected: %s\n", err);
        return 1;
    }
    failures += info_inputs(&cfg, &plan, &scfg, &lim, &cap, &in);
    if (failures != 0) {
        return failures;
    }
    if (moqr_cli_info_render(&in, doc, sizeof(doc), &len) != MOQR_OK) {
        printf("  info-bound: the wide configuration was refused\n");
        failures++;
    } else if ((uint64_t)len > moqr_cli_info_bound()) {
        printf("  info-bound: the wide document (%zu) exceeds the bound (%llu)\n",
               len, (unsigned long long)moqr_cli_info_bound());
        failures++;
    } else {
        size_t n2 = 0;
        /* the longest profile label reaches the document */
        if (strstr(doc, "\"profile\":\"d02_rfc9297_compat\"") == NULL) {
            printf("  info-bound: the document does not carry the profile\n");
            failures++;
        }
        /*
         * Authorization configuration is published nowhere.
         *
         * The fixture configures a policy and a distinctive Origin, so a
         * document that names either -- by key or by value -- is leaking
         * operator authorization state to every reader of /api/v1/info.
         */
        if (strstr(doc, "origin_policy") != NULL ||
            strstr(doc, "allowed_origins") != NULL ||
            strstr(doc, "allowlist") != NULL ||
            strstr(doc, "zqx-info-5512") != NULL) {
            printf("  info-bound: the document leaked Origin authorization "
                   "configuration\n");
            failures++;
        }
        /* exactly enough room, and one byte short */
        if (moqr_cli_info_render(&in, doc, len + 1u, &n2) != MOQR_OK ||
            n2 != len) {
            printf("  info-bound: an exactly-sized buffer was refused\n");
            failures++;
        }
        if (moqr_cli_info_render(&in, doc, len, &n2) != MOQR_ERR_CAPACITY) {
            printf("  info-bound: a one-byte-short buffer was not refused\n");
            failures++;
        }
    }
    /* A configured host holding a raw 0xff: the parser copies it; the
     * document boundary refuses the whole document as INVAL. */
    {
        static const char bad[] =
            "{\"listener\":{\"host\":\"h\xff\",\"port\":4433,\"versions\":[18]},"
            "\"admin\":{\"tcp\":{\"port\":9109}}}";
        if (parse(bad, &cfg, err, sizeof(err)) != MOQR_OK) {
            printf("  info-bound: the parser refused the 0xff fixture (%s); the "
                   "boundary refusal cannot be reached\n", err);
            failures++;
        } else {
            failures += info_inputs(&cfg, &plan, &scfg, &lim, &cap, &in);
            memset(doc, 'x', 16);
            if (moqr_cli_info_render(&in, doc, sizeof(doc), &len) != MOQR_ERR_INVAL ||
                doc[0] != '\0' || len != 0) {
                printf("  info-bound: a configured string that is not UTF-8 was "
                       "not refused whole\n");
                failures++;
            }
        }
    }
    return failures;
}

/* The bound is a maximum over SIX-byte expansion: a parser-admitted
 * configuration whose hosts and path are entirely control bytes (written as
 * JSON escapes in the source) renders, fits the bound, and fits the owner's
 * bound + 1 allocation. A bound measured over two-byte quotes fails this. */
static int
test_info_six_byte_expansion(void)
{
    int failures = 0;
    char err[192];
    static moqr_cli_config_t cfg;
    moqr_cli_shard_plan_t plan;
    moqr_shards_cfg_t scfg;
    moqr_shards_limits_t lim;
    moqr_cli_capacity_t cap;
    moqr_cli_info_inputs_t in;
    static char esc[255 * 6 + 1];
    static char j[6000];
    static char doc[65536];
    char *owned;
    size_t len = 0, owned_len = 0;
    uint64_t bound = moqr_cli_info_bound();

    for (size_t i = 0; i < 255; i++) {
        memcpy(esc + i * 6, "\\u001f", 6);
    }
    esc[sizeof(esc) - 1] = '\0';
    (void)snprintf(j, sizeof(j),
        "{\"listener\":{\"host\":\"%s\",\"port\":65535,\"versions\":[16,18],\"lanes\":4},"
        "\"webtransport\":{\"host\":\"%s\",\"port\":65535,\"path\":\"/%s\","
        "\"versions\":[16,18],\"lanes\":2,\"cert\":\"c\",\"key\":\"k\"},"
        "\"admin\":{\"tcp\":{\"port\":65535}}}", esc, esc, esc + 6);
    if (parse(j, &cfg, err, sizeof(err)) != MOQR_OK) {
        printf("  six-byte: the control-byte fixture was rejected: %s\n", err);
        return 1;
    }
    failures += info_inputs(&cfg, &plan, &scfg, &lim, &cap, &in);
    if (failures != 0) {
        return failures;
    }
    if (moqr_cli_info_render(&in, doc, sizeof(doc), &len) != MOQR_OK) {
        printf("  six-byte: the renderer refused a valid configuration\n");
        return failures + 1;
    }
    if (strstr(doc, "\\u001f\\u001f") == NULL) {
        printf("  six-byte: the control bytes were not escaped to six\n");
        failures++;
    }
    if (bound == UINT64_MAX || (uint64_t)len > bound) {
        printf("  six-byte: the document (%zu) exceeds the bound (%llu)\n", len,
               (unsigned long long)bound);
        failures++;
    }
    /* Exactly what the owner allocates. */
    owned = malloc((size_t)bound + 1u);
    if (owned == NULL) {
        printf("  six-byte: no memory for the owner-sized buffer\n");
        return failures + 1;
    }
    if (moqr_cli_info_render(&in, owned, (size_t)bound + 1u, &owned_len) != MOQR_OK ||
        owned_len != len) {
        printf("  six-byte: the owner-sized buffer could not hold the document\n");
        failures++;
    }
    free(owned);
    /* The bound fits the machine's finite document contract. */
    if (bound + 1u > (uint64_t)MOQR_ADMIN_MAX_STATIC_DOC) {
        printf("  six-byte: the bound (%llu) exceeds the finite document limit\n",
               (unsigned long long)bound);
        failures++;
    }
    return failures;
}

/* Every refusal leaves nothing usable in a supplied output, the early ones
 * included; a NULL or zero-capacity output is never written. */
static int
test_info_refusals_clear_output(void)
{
    int failures = 0;
    char err[192];
    static moqr_cli_config_t cfg;
    moqr_cli_shard_plan_t plan;
    moqr_shards_cfg_t scfg;
    moqr_shards_limits_t lim;
    moqr_cli_capacity_t cap;
    moqr_cli_info_inputs_t in, broken;
    char doc[64];
    size_t len = 7;
    static const char *J =
        "{\"listener\":{\"port\":4433,\"versions\":[18]},"
        "\"admin\":{\"tcp\":{\"port\":9109}}}";

    if (parse(J, &cfg, err, sizeof(err)) != MOQR_OK) {
        printf("  clear: fixture rejected: %s\n", err);
        return 1;
    }
    failures += info_inputs(&cfg, &plan, &scfg, &lim, &cap, &in);
    memcpy(doc, "previous", 9);
    if (moqr_cli_info_render(NULL, doc, sizeof(doc), &len) != MOQR_ERR_INVAL ||
        doc[0] != '\0' || len != 0) {
        printf("  clear: a NULL input left [%s] (len %zu)\n", doc, len);
        failures++;
    }
    broken = in;
    broken.limits = NULL;
    memcpy(doc, "previous", 9);
    len = 7;
    if (moqr_cli_info_render(&broken, doc, sizeof(doc), &len) != MOQR_ERR_INVAL ||
        doc[0] != '\0' || len != 0) {
        printf("  clear: a missing member left [%s] (len %zu)\n", doc, len);
        failures++;
    }
    broken = in;
    broken.capacity = NULL;
    memcpy(doc, "previous", 9);
    if (moqr_cli_info_render(&broken, doc, sizeof(doc), NULL) != MOQR_ERR_INVAL ||
        doc[0] != '\0') {
        printf("  clear: a missing capacity left [%s]\n", doc);
        failures++;
    }
    /* No output to clear: refused without touching anything. */
    if (moqr_cli_info_render(&in, NULL, 64, &len) != MOQR_ERR_INVAL || len != 0 ||
        moqr_cli_info_render(&in, doc, 0, &len) != MOQR_ERR_INVAL || len != 0) {
        printf("  clear: a NULL or zero-capacity output was not refused\n");
        failures++;
    }
    return failures;
}

/* The bound is read-only and reentrant: two threads measuring at once agree
 * with each other and with a serial measurement, every time. Under TSan this
 * is where shared scratch would show. */
static void *
bound_worker(void *arg)
{
    uint64_t *out = (uint64_t *)arg;
    uint64_t first = moqr_cli_info_bound();
    for (unsigned i = 1; i < 100u; i++) {
        if (moqr_cli_info_bound() != first) {
            first = UINT64_MAX;
            break;
        }
    }
    *out = first;
    return NULL;
}

/* The worker acquisition seam. Ordinarily the real pthread calls; a case can
 * refuse the Nth create, or refuse the join, to drive the partial-acquisition
 * paths deterministically. Every successful real join is counted, so a test
 * can prove a worker was settled rather than assume it. */
static unsigned g_bound_fail_create_at;   /* 0: never                     */
static int      g_bound_fail_join;        /* nonzero: refuse every join   */
static unsigned g_bound_creates;
static unsigned g_bound_joins;

static int
bound_thread_create(pthread_t *th, void *(*fn)(void *), void *arg)
{
    if (++g_bound_creates == g_bound_fail_create_at) {
        return EAGAIN;
    }
    return pthread_create(th, NULL, fn, arg);
}

static int
bound_thread_join(pthread_t th)
{
    int rc;
    if (g_bound_fail_join) {
        return EINVAL;   /* refused: the thread is NOT proved dead */
    }
    rc = pthread_join(th, NULL);
    if (rc == 0) {
        g_bound_joins++;
    }
    return rc;
}

/* Run the pair. Returns 0 when both workers measured, or 1 when a worker
 * could not be started -- and in that case every worker that WAS started has
 * been settled before this returns, so no worker can reach the caller's
 * storage afterwards. */
typedef struct owned_worker {
    pthread_t th;
    bool      owned;
} owned_worker_t;

/* Settle every owned worker. A join that succeeds proves the worker dead and
 * releases its claim on the caller's storage. A join that does not is not a
 * settled worker, and returning past storage that worker can still reach is
 * not an option: the process stops here, before any teardown. Every worker
 * still owned at that point is detached first -- the abandonment is
 * deliberate, not a forgotten handle. */
static void
bound_settle_all(owned_worker_t *w, size_t n)
{
    static const char msg[] =
        "  bound: a worker could not be joined and can still reach this "
        "frame; stopping before teardown\n";
    for (size_t i = 0; i < n; i++) {
        if (!w[i].owned) {
            continue;
        }
        if (bound_thread_join(w[i].th) != 0) {
            for (size_t k = 0; k < n; k++) {
                if (w[k].owned) {
                    (void)pthread_detach(w[k].th);
                    w[k].owned = false;
                }
            }
            (void)write(STDERR_FILENO, msg, sizeof(msg) - 1u);
            _exit(90);
        }
        w[i].owned = false;
    }
}

static int
bound_pair(uint64_t *ra, uint64_t *rb)
{
    owned_worker_t w[2] = { { 0 }, { 0 } };

    if (bound_thread_create(&w[0].th, bound_worker, ra) != 0) {
        printf("  bound: could not start the first worker\n");
        return 1;   /* nothing acquired */
    }
    w[0].owned = true;
    if (bound_thread_create(&w[1].th, bound_worker, rb) != 0) {
        printf("  bound: could not start the second worker\n");
        bound_settle_all(w, 1);   /* the first still reaches ra */
        return 1;
    }
    w[1].owned = true;
    bound_settle_all(w, 2);
    return 0;
}

static int
test_info_bound_is_pure(void)
{
    int failures = 0;
    uint64_t ra = 0, rb = 0;
    uint64_t serial = moqr_cli_info_bound();

    if (serial == UINT64_MAX) {
        printf("  bound: the measurement refused\n");
        return 1;
    }
    g_bound_fail_create_at = 0;
    g_bound_fail_join = 0;
    g_bound_creates = 0;
    g_bound_joins = 0;
    if (bound_pair(&ra, &rb) != 0) {
        printf("  bound: the pair refused with real threads available\n");
        return 1;
    }
    if (g_bound_joins != 2u) {
        printf("  bound: %u of 2 workers were settled\n", g_bound_joins);
        failures++;
    }
    if (ra != serial || rb != serial) {
        printf("  bound: concurrent measurements disagree (%llu, %llu, serial "
               "%llu)\n", (unsigned long long)ra, (unsigned long long)rb,
               (unsigned long long)serial);
        failures++;
    }
    return failures;
}

/* PARTIAL ACQUISITION SETTLES WHAT IT ACQUIRED.
 *
 * A refused first create owns nothing. A refused second create owns one live
 * worker holding a pointer into this frame; that worker must be joined before
 * the frame is left. A refused join proves nothing about the worker, so the
 * only correct outcome is a stop before teardown -- exercised in a child so
 * the suite goes on. */
static int
test_info_bound_workers_settle_on_partial_acquisition(void)
{
    int failures = 0;
    uint64_t serial = moqr_cli_info_bound();
    uint64_t ra, rb;

    /* First create refused: nothing was acquired, nothing to settle. */
    ra = rb = 0;
    g_bound_fail_create_at = 1;
    g_bound_fail_join = 0;
    g_bound_creates = 0;
    g_bound_joins = 0;
    if (bound_pair(&ra, &rb) != 1 || g_bound_creates != 1u ||
        g_bound_joins != 0u) {
        printf("  settle[first]: refused create 1: creates %u joins %u\n",
               g_bound_creates, g_bound_joins);
        failures++;
    }
    /* Second create refused: the first worker is live and must be settled
     * before the pair returns -- proved by its real join and by its result
     * having landed. */
    ra = rb = 0;
    g_bound_fail_create_at = 2;
    g_bound_creates = 0;
    g_bound_joins = 0;
    if (bound_pair(&ra, &rb) != 1 || g_bound_creates != 2u) {
        printf("  settle[second]: refused create 2: creates %u\n",
               g_bound_creates);
        failures++;
    }
    if (g_bound_joins != 1u || ra != serial) {
        printf("  settle[second]: the first worker was not settled before the "
               "pair returned (joins %u, result %llu)\n", g_bound_joins,
               (unsigned long long)ra);
        failures++;
    }
    /* A refused join: the worker is not proved dead, so the pair must stop
     * the process before teardown rather than return past its storage. */
    {
        pid_t pid;
        int status = 0;
        fflush(stdout);
        pid = fork();
        if (pid < 0) {
            printf("  settle[join]: no fork\n");
            return failures + 1;
        }
        if (pid == 0) {
            uint64_t ca = 0, cb = 0;
            g_bound_fail_create_at = 0;
            g_bound_fail_join = 1;
            g_bound_creates = 0;
            g_bound_joins = 0;
            (void)bound_pair(&ca, &cb);
            _exit(3);   /* returned past a worker it could not prove dead */
        }
        if (waitpid(pid, &status, 0) != pid) {
            printf("  settle[join]: could not reap the child\n");
            failures++;
        } else if (!WIFEXITED(status) || WEXITSTATUS(status) != 90) {
            printf("  settle[join]: a refused join did not stop the process "
                   "(status %d)\n", WIFEXITED(status) ? WEXITSTATUS(status) : -1);
            failures++;
        }
    }
    g_bound_fail_create_at = 0;
    g_bound_fail_join = 0;
    return failures;
}

/* -- the /api/v1/shards document, pure ----------------------------------------
 *
 * Max-width counters at lanes 1 and MOQR_SHARDS_MAX fit the bound; exact
 * fit and one-byte-short; an absent row omits shard_plane; a row whose shard
 * plane contradicts its core is refused whole; an unknown transport label is
 * refused; a label that is not UTF-8 is refused. */
static int
test_shards_document_pure(void)
{
    int failures = 0;
    static moqr_core_stats_t core;
    static moqr_bind_stats_t bind;
    static moqr_shards_stats_t shard;
    static moqr_snapshot_view_t views[MOQR_SHARDS_MAX];
    static char doc[1 << 20];
    size_t len = 0;

    memset(&core, 0xff, sizeof(core));
    memset(&bind, 0xff, sizeof(bind));
    memset(&shard, 0xff, sizeof(shard));
    for (uint32_t i = 0; i < MOQR_SHARDS_MAX; i++) {
        views[i].core = &core;
        views[i].bind = &bind;
        views[i].shard = &shard;
        views[i].labels.shard = UINT16_MAX;
        views[i].labels.transport = "wtquic-msquic";
        views[i].labels.version = "moqt-16+moqt-18+moqt-16+moqt-18";
        views[i].lane_wakes = UINT64_MAX;
    }
    for (uint32_t lanes = 1; lanes <= MOQR_SHARDS_MAX; lanes += MOQR_SHARDS_MAX - 1u) {
        uint64_t bound = moqr_cli_shards_bound(lanes);
        if (moqr_cli_shards_render(views, lanes, UINT64_MAX, doc, sizeof(doc), &len) != MOQR_OK) {
            printf("  shards-pure: lanes %u refused\n", lanes);
            failures++;
            continue;
        }
        if (bound == UINT64_MAX || (uint64_t)len > bound) {
            printf("  shards-pure: lanes %u: %zu bytes exceed the bound %llu\n",
                   lanes, len, (unsigned long long)bound);
            failures++;
        }
        if (moqr_cli_shards_render(views, lanes, UINT64_MAX, doc, len + 1u, NULL) != MOQR_OK ||
            moqr_cli_shards_render(views, lanes, UINT64_MAX, doc, len, NULL) != MOQR_ERR_CAPACITY ||
            doc[0] != '\0') {
            printf("  shards-pure: lanes %u: exact fit / one short misbehaved\n", lanes);
            failures++;
        }
        if (strstr(doc, "\"listener\":\"webtransport\"") == NULL) {
            /* doc was cleared by the refusal above; re-render to inspect */
        }
    }
    (void)moqr_cli_shards_render(views, 1u, 9u, doc, sizeof(doc), &len);
    if (strstr(doc, "\"epoch\":9,") == NULL ||
        strstr(doc, "\"capability\":\"valid\"") == NULL ||
        strstr(doc, "\"shard_plane\":{") == NULL ||
        strstr(doc, "\"lane_wakes\":18446744073709551615") == NULL ||
        strstr(doc, "\"listener\":\"webtransport\"") == NULL) {
        printf("  shards-pure: a valid row is wrong: [%.200s]\n", doc);
        failures++;
    }
    /* Absent: no shard plane, never fabricated. */
    views[0].shard = NULL;
    views[0].labels.transport = "msquic";
    if (moqr_cli_shards_render(views, 1u, 9u, doc, sizeof(doc), &len) != MOQR_OK ||
        strstr(doc, "\"capability\":\"absent\"") == NULL ||
        strstr(doc, "shard_plane") != NULL || strstr(doc, "lane_wakes") != NULL ||
        strstr(doc, "\"listener\":\"raw\"") == NULL) {
        printf("  shards-pure: an absent row is wrong: [%.200s]\n", doc);
        failures++;
    }
    /* Out-of-range lane counts refuse; so does an unknown transport. */
    if (moqr_cli_shards_render(views, 0u, 9u, doc, sizeof(doc), &len) != MOQR_ERR_INVAL ||
        moqr_cli_shards_render(views, MOQR_SHARDS_MAX + 1u, 9u, doc, sizeof(doc), &len) != MOQR_ERR_INVAL ||
        moqr_cli_shards_bound(0) != UINT64_MAX ||
        moqr_cli_shards_bound(MOQR_SHARDS_MAX + 1u) != UINT64_MAX) {
        printf("  shards-pure: lane bounds were not enforced\n");
        failures++;
    }
    views[0].labels.transport = "quiche";
    memcpy(doc, "stale", 6);
    if (moqr_cli_shards_render(views, 1u, 9u, doc, sizeof(doc), &len) != MOQR_ERR_INVAL ||
        doc[0] != '\0' || len != 0) {
        printf("  shards-pure: an unknown transport was not refused whole\n");
        failures++;
    }
    views[0].labels.transport = "msquic";
    views[0].labels.version = "moqt\xff";
    if (moqr_cli_shards_render(views, 1u, 9u, doc, sizeof(doc), &len) != MOQR_ERR_INVAL) {
        printf("  shards-pure: a label that is not UTF-8 was accepted\n");
        failures++;
    }
    views[0].labels.version = "moqt-18";
    /* A shard plane that contradicts its core row is refused, as the metrics
     * renderer refuses it: more internal bindings than bindings. */
    {
        static moqr_core_stats_t small;
        static moqr_shards_stats_t plane;
        memset(&small, 0, sizeof(small));
        memset(&plane, 0, sizeof(plane));
        small.bindings = 2;
        plane.internal_bindings = 3;
        views[0].core = &small;
        views[0].shard = &plane;
        memcpy(doc, "stale", 6);
        if (moqr_cli_shards_render(views, 1u, 9u, doc, sizeof(doc), &len) != MOQR_ERR_INVAL ||
            doc[0] != '\0') {
            printf("  shards-pure: a contradictory shard plane was rendered\n");
            failures++;
        }
        plane.internal_bindings = 2;
        if (moqr_cli_shards_render(views, 1u, 9u, doc, sizeof(doc), &len) != MOQR_OK) {
            printf("  shards-pure: an equal internal count was refused\n");
            failures++;
        }
    }
    return failures;
}

/* -- admin capacity accounting ---------------------------------------------
 *
 * Every configured dimension must move the model, in BOTH lane-count branches,
 * and the fixed thread stack is reported separately from allocator-owned
 * bytes because it is not an allocator request. */
static int
test_admin_capacity(void)
{
    int failures = 0;
    char err[192];
    static const char *L1 = "{\"listener\":{\"port\":4433,\"versions\":[18]}";
    static const char *L4 =
        "{\"listener\":{\"port\":4433,\"versions\":[18],\"lanes\":4}";

    for (int branch = 0; branch < 2; branch++) {
        const char *base = branch == 0 ? L1 : L4;
        const char *what = branch == 0 ? "lanes=1" : "lanes>1";
        moqr_cli_config_t off_cfg, on_cfg;
        moqr_cli_capacity_t off_cap, on_cap;
        char j[512];

        (void)snprintf(j, sizeof(j), "%s}", base);
        if (parse(j, &off_cfg, err, sizeof(err)) != MOQR_OK) {
            printf("  %s: disabled fixture rejected: %s\n", what, err);
            failures++;
            continue;
        }
        (void)snprintf(j, sizeof(j), "%s,\"admin\":{\"tcp\":{\"port\":9109}}}",
                       base);
        if (parse(j, &on_cfg, err, sizeof(err)) != MOQR_OK) {
            printf("  %s: enabled fixture rejected: %s\n", what, err);
            failures++;
            continue;
        }
        if (moqr_cli_describe_capacity(&off_cfg, NULL, 0, &off_cap) != MOQR_OK ||
            moqr_cli_describe_capacity(&on_cfg, NULL, 0, &on_cap) != MOQR_OK) {
            printf("  %s: describe_capacity refused a valid config\n", what);
            failures++;
            continue;
        }
        /* Enabling the endpoint must cost allocator-owned bytes. */
        if (on_cap.admin_bytes == 0u) {
            printf("  %s: an enabled admin endpoint accounted 0 bytes\n", what);
            failures++;
        }
        if (off_cap.admin_bytes != 0u) {
            printf("  %s: a disabled admin endpoint accounted %llu bytes\n",
                   what, (unsigned long long)off_cap.admin_bytes);
            failures++;
        }
        if (on_cap.cli_runtime_bytes <= off_cap.cli_runtime_bytes) {
            printf("  %s: enabling admin did not raise cli_runtime_bytes\n",
                   what);
            failures++;
        }
        if (on_cap.total_bytes <= off_cap.total_bytes) {
            printf("  %s: enabling admin did not raise the printed ceiling\n",
                   what);
            failures++;
        }
        /* The admin term is counted exactly once. */
        if (on_cap.cli_runtime_bytes - off_cap.cli_runtime_bytes !=
            on_cap.admin_bytes) {
            printf("  %s: the admin term is not counted exactly once "
                   "(delta=%llu, term=%llu)\n", what,
                   (unsigned long long)(on_cap.cli_runtime_bytes -
                                        off_cap.cli_runtime_bytes),
                   (unsigned long long)on_cap.admin_bytes);
            failures++;
        }
        /* The thread stack is reported SEPARATELY: it is a reservation, not an
         * allocator request, so it must not be folded into the ceiling. */
        if (on_cap.admin_thread_stack_bytes == 0u) {
            printf("  %s: no admin thread stack reservation was reported\n",
                   what);
            failures++;
        }
        if (off_cap.admin_thread_stack_bytes != 0u) {
            printf("  %s: a disabled endpoint reserved a thread stack\n", what);
            failures++;
        }
        /* Exact, not a magnitude comparison: the banks are legitimately
         * larger than the stack, so "the delta exceeds the stack" proves
         * nothing. The ceiling must move by the allocator term ALONE. */
        if (on_cap.total_bytes - off_cap.total_bytes != on_cap.admin_bytes) {
            printf("  %s: the ceiling moved by %llu, not by the allocator "
                   "term %llu -- the stack reservation was folded in\n", what,
                   (unsigned long long)(on_cap.total_bytes -
                                        off_cap.total_bytes),
                   (unsigned long long)on_cap.admin_bytes);
            failures++;
        }
        /* EXACT, not bracketing. The allocator term is the one listener
         * object plus both bodies in both banks, and nothing else. This is
         * recomputed here from the layout and the checked renderer bound, so a
         * dropped term, an extra term, a wrong bank count or a folded-in stack
         * each fail on their own. */
        {
            uint32_t lanes = moqr_cli_total_lanes(&on_cfg);
            uint64_t b1 = 0, b2 = 0;
            if (moqr_metrics_bound(lanes, MOQR_OBS_FMT_OPENMETRICS_100, true,
                                   true, &b1) != MOQR_OK ||
                moqr_metrics_bound(lanes, MOQR_OBS_FMT_PROMETHEUS_004, true,
                                   true, &b2) != MOQR_OK) {
                printf("  %s: the renderer bound could not be computed\n", what);
                failures++;
            } else {
                uint64_t b3 = moqr_cli_shards_bound(lanes);
                /* Three bodies per bank: both metrics formats and the shards
                 * JSON document, each with its terminator. */
                uint64_t banks = ((b1 + 1u) + (b2 + 1u) + (b3 + 1u)) * MOQR_ADMIN_BANKS;
                /* ...plus the coordinator's own frozen-projection body for
                 * the signal sink, one Prometheus document wide. */
                /* ...and the coordinator's immutable /api/v1/info document,
                 * one bound wide plus its terminator. */
                uint64_t want = banks + (b2 + 1u) + (moqr_cli_info_bound() + 1u) +
                                (uint64_t)sizeof(struct moqr_admin_listen) +
                                (uint64_t)lanes *
                                    ((uint64_t)sizeof(moqr_cli_snapshot_stats_t) +
                                     (uint64_t)sizeof(moqr_snapshot_view_t));
                if (on_cap.admin_bytes != want) {
                    printf("  %s: admin_bytes is %llu, not the exact term "
                           "%llu (object %llu + banks %llu)\n", what,
                           (unsigned long long)on_cap.admin_bytes,
                           (unsigned long long)want,
                           (unsigned long long)sizeof(struct moqr_admin_listen),
                           (unsigned long long)banks);
                    failures++;
                }
                if (on_cap.admin_thread_stack_bytes !=
                    (uint64_t)MOQR_CLI_ADMIN_STACK_BYTES) {
                    printf("  %s: the stack reservation is %llu, not %llu\n",
                           what,
                           (unsigned long long)on_cap.admin_thread_stack_bytes,
                           (unsigned long long)MOQR_CLI_ADMIN_STACK_BYTES);
                    failures++;
                }
            }
        }
    }
    return failures;
}

int
main(void)
{
    int failures = 0;

    failures += test_admin_config();
    failures += test_admin_capacity();
    failures += test_info_document();
    failures += test_info_bound_and_refusal();
    failures += test_info_six_byte_expansion();
    failures += test_info_refusals_clear_output();
    failures += test_info_bound_is_pure();
    failures += test_shards_document_pure();
    failures += test_info_bound_workers_settle_on_partial_acquisition();
    moqr_cli_config_t cfg;
    char err[128];

    /* Minimal valid config: port only; defaults fill the rest. The msquic
     * listener is exact-version, so the default is ONE offer (the newest). */
    MOQ_TEST_CHECK(parse("{\"listener\":{\"port\":4443}}", &cfg, err,
                         sizeof(err)) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_INT(cfg.port, 4443);
    MOQ_TEST_CHECK(strcmp(cfg.host, "0.0.0.0") == 0);
    MOQ_TEST_CHECK_EQ_SIZE(cfg.alpn_count, (size_t)1);
    MOQ_TEST_CHECK(strcmp(cfg.alpns[0], "moqt-18") == 0);
    MOQ_TEST_CHECK_EQ_U64(cfg.version, 18);
    MOQ_TEST_CHECK_EQ_U64(cfg.lanes, 1);   /* default single-lane */
    MOQ_TEST_CHECK(!cfg.insecure_skip_verify);

    /* listener.lanes: accepted at the boundaries; rejected outside 1..64 and
     * for a non-number, each with an error naming the key. */
    MOQ_TEST_CHECK(parse("{\"listener\":{\"port\":1,\"lanes\":2}}", &cfg, err,
                         sizeof(err)) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(cfg.lanes, 2);
    MOQ_TEST_CHECK(parse("{\"listener\":{\"port\":1,\"lanes\":64}}", &cfg, err,
                         sizeof(err)) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(cfg.lanes, 64);
    MOQ_TEST_CHECK(parse("{\"listener\":{\"port\":1,\"lanes\":0}}", &cfg, err,
                         sizeof(err)) == MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(strstr(err, "lanes") != NULL);
    MOQ_TEST_CHECK(parse("{\"listener\":{\"port\":1,\"lanes\":65}}", &cfg, err,
                         sizeof(err)) == MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(strstr(err, "lanes") != NULL);
    MOQ_TEST_CHECK(parse("{\"listener\":{\"port\":1,\"lanes\":\"two\"}}", &cfg,
                         err, sizeof(err)) == MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(strstr(err, "lanes") != NULL);

    /* Full config round-trips into core budgets. */
    const char *full =
        "{\"listener\":{\"transport\":\"msquic\",\"host\":\"::1\","
        "\"port\":9,\"cert\":\"c.pem\",\"key\":\"k.pem\","
        "\"versions\":[16],\"insecure_skip_verify\":true},"
        "\"budgets\":{\"max_tracks\":7,\"max_subs\":11,"
        "\"log\":{\"max_groups\":3,\"max_bytes\":4096,\"max_age_us\":50}},"
        "\"linger_us\":250}";
    MOQ_TEST_CHECK(parse(full, &cfg, err, sizeof(err)) == MOQR_OK);
    MOQ_TEST_CHECK(strcmp(cfg.host, "::1") == 0);
    MOQ_TEST_CHECK(strcmp(cfg.cert, "c.pem") == 0);
    MOQ_TEST_CHECK_EQ_SIZE(cfg.alpn_count, (size_t)1);
    MOQ_TEST_CHECK(strcmp(cfg.alpns[0], "moqt-16") == 0);
    MOQ_TEST_CHECK_EQ_U64(cfg.version, 16);
    MOQ_TEST_CHECK(cfg.insecure_skip_verify);
    MOQ_TEST_CHECK_EQ_U64(cfg.core.max_tracks, 7);
    MOQ_TEST_CHECK_EQ_U64(cfg.core.max_subs, 11);
    MOQ_TEST_CHECK_EQ_U64(cfg.core.log_budget.max_groups, 3);
    MOQ_TEST_CHECK_EQ_U64(cfg.core.log_budget.max_bytes, 4096);
    MOQ_TEST_CHECK_EQ_U64(cfg.core.linger_us, 250);

    /* Missing required listener fields. */
    MOQ_TEST_CHECK(parse("{}", &cfg, err, sizeof(err)) == MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(strstr(err, "listener") != NULL);
    MOQ_TEST_CHECK(parse("{\"listener\":{\"host\":\"x\"}}", &cfg, err,
                         sizeof(err)) == MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(strstr(err, "port") != NULL);

    /* Bad transport (picoquic is no longer a production transport). */
    MOQ_TEST_CHECK(parse("{\"listener\":{\"port\":1,\"transport\":\"tcp\"}}",
                         &cfg, err, sizeof(err)) == MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(strstr(err, "transport") != NULL);
    MOQ_TEST_CHECK(
        parse("{\"listener\":{\"port\":1,\"transport\":\"picoquic\"}}", &cfg,
              err, sizeof(err)) == MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(strstr(err, "transport") != NULL);

    /* listener.versions is an ORDERED, non-empty set. Everything malformed
     * fails closed; a multi-entry list is accepted and keeps its order. */
    MOQ_TEST_CHECK(parse("{\"listener\":{\"port\":1,\"versions\":[17]}}",
                         &cfg, err, sizeof(err)) == MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(parse("{\"listener\":{\"port\":1,\"versions\":[]}}",
                         &cfg, err, sizeof(err)) == MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(parse("{\"listener\":{\"port\":1,\"versions\":[18,18]}}",
                         &cfg, err, sizeof(err)) == MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(strstr(err, "duplicate") != NULL);
    MOQ_TEST_CHECK(parse("{\"listener\":{\"port\":1,\"versions\":[true]}}",
                         &cfg, err, sizeof(err)) == MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(parse("{\"listener\":{\"port\":1,\"versions\":[18.5]}}",
                         &cfg, err, sizeof(err)) == MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(parse("{\"listener\":{\"port\":1,\"versions\":[\"18\"]}}",
                         &cfg, err, sizeof(err)) == MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(parse("{\"listener\":{\"port\":1,\"versions\":18}}",
                         &cfg, err, sizeof(err)) == MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(
        parse("{\"listener\":{\"port\":1,\"versions\":[18,16,18,16,18]}}",
              &cfg, err, sizeof(err)) == MOQR_ERR_INVAL);

    /* Default is exactly [18], and one entry keeps exact-listener semantics:
     * the label is the bare ALPN and the plan carries no list. */
    MOQ_TEST_CHECK(parse("{\"listener\":{\"port\":1}}", &cfg, err,
                         sizeof(err)) == MOQR_OK);
    MOQ_TEST_CHECK(cfg.version_count == 1);
    MOQ_TEST_CHECK(cfg.versions[0] == MOQ_VERSION_DRAFT_18);
    MOQ_TEST_CHECK(strcmp(cfg.alpn_set, "moqt-18") == 0);
    {
        moqr_cli_version_plan_t pl;
        moqr_cli_version_plan(&cfg, &pl);
        MOQ_TEST_CHECK(pl.exact == MOQ_VERSION_DRAFT_18);
        MOQ_TEST_CHECK(pl.list == NULL);
        MOQ_TEST_CHECK(pl.count == 0);   /* count 0 IS the exact plan */
        /* An exact plan is never ambiguous: after a successful parse it cannot
         * have both a zero version and a zero count, which would read as "no
         * plan at all" to the caller. */
        MOQ_TEST_CHECK(!(pl.exact == 0 && pl.count == 0));
        MOQ_TEST_CHECK(pl.count != 1);   /* count 1 is never produced */
    }
    MOQ_TEST_CHECK(parse("{\"listener\":{\"port\":1,\"versions\":[16]}}",
                         &cfg, err, sizeof(err)) == MOQR_OK);
    MOQ_TEST_CHECK(cfg.version_count == 1);
    MOQ_TEST_CHECK(strcmp(cfg.alpn_set, "moqt-16") == 0);
    {
        moqr_cli_version_plan_t pl;
        moqr_cli_version_plan(&cfg, &pl);
        MOQ_TEST_CHECK(pl.exact == MOQ_VERSION_DRAFT_16);
        MOQ_TEST_CHECK(pl.count == 0);
        MOQ_TEST_CHECK(!(pl.exact == 0 && pl.count == 0));
        MOQ_TEST_CHECK(pl.count != 1);
    }

    /* A multi-entry list is accepted, ORDER PRESERVED, and its plan hands the
     * ordered list over instead of an exact version. */
    MOQ_TEST_CHECK(parse("{\"listener\":{\"port\":1,\"versions\":[18,16]}}",
                         &cfg, err, sizeof(err)) == MOQR_OK);
    MOQ_TEST_CHECK(cfg.version_count == 2);
    if (cfg.version_count == 2) {
        MOQ_TEST_CHECK(cfg.versions[0] == MOQ_VERSION_DRAFT_18);
        MOQ_TEST_CHECK(cfg.versions[1] == MOQ_VERSION_DRAFT_16);
    }
    MOQ_TEST_CHECK(strcmp(cfg.alpn_set, "moqt-18+moqt-16") == 0);
    {
        moqr_cli_version_plan_t pl;
        moqr_cli_version_plan(&cfg, &pl);
        MOQ_TEST_CHECK(pl.exact == 0);
        MOQ_TEST_CHECK(pl.count == 2);
        MOQ_TEST_CHECK(pl.list != NULL);
        if (pl.list != NULL && pl.count == 2) {
            MOQ_TEST_CHECK(pl.list[0] == MOQ_VERSION_DRAFT_18);
            MOQ_TEST_CHECK(pl.list[1] == MOQ_VERSION_DRAFT_16);
        }
    }
    /* The reverse order is a DIFFERENT preference, not a normalized set. */
    MOQ_TEST_CHECK(parse("{\"listener\":{\"port\":1,\"versions\":[16,18]}}",
                         &cfg, err, sizeof(err)) == MOQR_OK);
    if (cfg.version_count == 2) {
        MOQ_TEST_CHECK(cfg.versions[0] == MOQ_VERSION_DRAFT_16);
        MOQ_TEST_CHECK(cfg.versions[1] == MOQ_VERSION_DRAFT_18);
    }
    MOQ_TEST_CHECK(strcmp(cfg.alpn_set, "moqt-16+moqt-18") == 0);

    /* Bad port. */
    MOQ_TEST_CHECK(parse("{\"listener\":{\"port\":70000}}", &cfg, err,
                         sizeof(err)) == MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(parse("{\"listener\":{\"port\":0}}", &cfg, err,
                         sizeof(err)) == MOQR_ERR_INVAL);

    /* Oversized budgets are config errors, not requests. */
    MOQ_TEST_CHECK(
        parse("{\"listener\":{\"port\":1},"
              "\"budgets\":{\"max_tracks\":9999999}}",
              &cfg, err, sizeof(err)) == MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(
        parse("{\"listener\":{\"port\":1},\"budgets\":{\"log\":"
              "{\"max_bytes\":2199023255552}}}",
              &cfg, err, sizeof(err)) == MOQR_ERR_INVAL);

    /* Unknown keys fail loudly. */
    MOQ_TEST_CHECK(parse("{\"listener\":{\"port\":1},\"typo\":1}", &cfg,
                         err, sizeof(err)) == MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(parse("{\"listener\":{\"port\":1,\"prot\":1}}", &cfg,
                         err, sizeof(err)) == MOQR_ERR_INVAL);

    /* Not JSON at all. */
    MOQ_TEST_CHECK(parse("not json", &cfg, err, sizeof(err)) ==
                   MOQR_ERR_INVAL);

    /* Negative and overflowing numbers are rejected, not wrapped. A bare
     * "-1" must not become UINT64_MAX in an uncapped field (linger_us). */
    MOQ_TEST_CHECK(parse("{\"listener\":{\"port\":1},\"linger_us\":-1}",
                         &cfg, err, sizeof(err)) == MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(
        parse("{\"listener\":{\"port\":1},\"budgets\":{\"log\":"
              "{\"max_age_us\":-1}}}",
              &cfg, err, sizeof(err)) == MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(parse("{\"listener\":{\"port\":-1}}", &cfg, err,
                         sizeof(err)) == MOQR_ERR_INVAL);
    /* Overflow past 2^64 (ERANGE). */
    MOQ_TEST_CHECK(
        parse("{\"listener\":{\"port\":1},\"linger_us\":"
              "99999999999999999999999}}",
              &cfg, err, sizeof(err)) == MOQR_ERR_INVAL);

    /* Telemetry object: trace ring depth parses into the config. */
    MOQ_TEST_CHECK(
        parse("{\"listener\":{\"port\":1},"
              "\"telemetry\":{\"trace_ring_records\":4096}}",
              &cfg, err, sizeof(err)) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(cfg.telemetry.trace_ring_records, 4096);
    /* Absent telemetry leaves the default (0 = library default). */
    MOQ_TEST_CHECK(parse("{\"listener\":{\"port\":1}}", &cfg, err,
                         sizeof(err)) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(cfg.telemetry.trace_ring_records, 0);
    /* Unknown telemetry key fails loudly. */
    MOQ_TEST_CHECK(
        parse("{\"listener\":{\"port\":1},\"telemetry\":{\"depth\":8}}",
              &cfg, err, sizeof(err)) == MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(strstr(err, "telemetry") != NULL);
    /* Oversized and non-object telemetry are config errors. */
    MOQ_TEST_CHECK(
        parse("{\"listener\":{\"port\":1},\"telemetry\":"
              "{\"trace_ring_records\":9999999}}",
              &cfg, err, sizeof(err)) == MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(
        parse("{\"listener\":{\"port\":1},\"telemetry\":5}",
              &cfg, err, sizeof(err)) == MOQR_ERR_INVAL);
    /* Negative trace depth is rejected, not wrapped. */
    MOQ_TEST_CHECK(
        parse("{\"listener\":{\"port\":1},\"telemetry\":"
              "{\"trace_ring_records\":-1}}",
              &cfg, err, sizeof(err)) == MOQR_ERR_INVAL);

    /* Capacity-only path: a parsed config yields a nonzero ceiling. */
    MOQ_TEST_CHECK(parse("{\"listener\":{\"port\":4443}}", &cfg, err,
                         sizeof(err)) == MOQR_OK);
    cfg.core.alloc = moq_alloc_default();
    moqr_core_capacity_t cap;
    moqr_core_capacity_describe(&cfg.core, &cap);
    MOQ_TEST_CHECK(cap.total_bytes > 0);
    MOQ_TEST_CHECK_EQ_U64(cap.total_bytes,
                          cap.structure_bytes + cap.payload_bytes);

    /* log_max_chunk_nodes is a public capacity knob like the neighbouring
     * per-track log limits: it parses, reaches core capacity, and zero keeps
     * the default. */
    {
        moqr_cli_config_t dflt, tuned, zero;
        MOQ_TEST_CHECK(parse("{\"listener\":{\"port\":1},\"budgets\":{"
                             "\"max_tracks\":1,\"max_bindings\":2,"
                             "\"log_max_chunk_nodes\":0}}",
                             &zero, err, sizeof(err)) == MOQR_OK);
        MOQ_TEST_CHECK(parse("{\"listener\":{\"port\":1},\"budgets\":{"
                             "\"max_tracks\":1,\"max_bindings\":2}}",
                             &dflt, err, sizeof(err)) == MOQR_OK);
        MOQ_TEST_CHECK(parse("{\"listener\":{\"port\":1},\"budgets\":{"
                             "\"max_tracks\":1,\"max_bindings\":2,"
                             "\"log_max_chunk_nodes\":7}}",
                             &tuned, err, sizeof(err)) == MOQR_OK);
        MOQ_TEST_CHECK_EQ_U64(tuned.core.log_max_chunk_nodes, 7);
        moqr_core_capacity_t cd, ct, cz;
        MOQ_TEST_CHECK(moqr_core_capacity_describe(&dflt.core, &cd) ==
                       MOQR_OK);
        MOQ_TEST_CHECK(moqr_core_capacity_describe(&tuned.core, &ct) ==
                       MOQR_OK);
        MOQ_TEST_CHECK(moqr_core_capacity_describe(&zero.core, &cz) ==
                       MOQR_OK);
        MOQ_TEST_CHECK_EQ_U64(cd.total_bytes, cz.total_bytes);
        MOQ_TEST_CHECK(ct.total_bytes < cd.total_bytes);

        MOQ_TEST_CHECK(parse("{\"listener\":{\"port\":1},\"budgets\":{"
                             "\"log_max_chunk_nodes\":-1}}",
                             &cfg, err, sizeof(err)) == MOQR_ERR_INVAL);
        MOQ_TEST_CHECK(strstr(err, "log_max_chunk_nodes") != NULL);
        MOQ_TEST_CHECK(parse("{\"listener\":{\"port\":1},\"budgets\":{"
                             "\"log_max_chunk_nodes\":4294967296}}",
                             &cfg, err, sizeof(err)) == MOQR_ERR_INVAL);
        MOQ_TEST_CHECK(strstr(err, "log_max_chunk_nodes") != NULL);
    }

    /* The telemetry knob is real, not just parsed: build_core attaches a
     * trace ring that records actual core transitions, and the configured
     * depth takes effect (a 4-record ring caps the retained tail while the
     * total climbs). */
    {
        moqr_cli_config_t bc;
        MOQ_TEST_CHECK(
            parse("{\"listener\":{\"port\":1},"
                  "\"budgets\":{\"max_bindings\":32},"
                  "\"telemetry\":{\"trace_ring_records\":4}}",
                  &bc, err, sizeof(err)) == MOQR_OK);
        const moq_alloc_t *alloc = moq_alloc_default();
        moqr_trace_t *trace = NULL;
        moqr_core_t *core = NULL;
        MOQ_TEST_CHECK(moqr_cli_build_core(&bc, alloc, &trace, &core) ==
                       MOQR_OK);
        MOQ_TEST_CHECK(trace != NULL && core != NULL);
        MOQ_TEST_CHECK_EQ_U64(moqr_trace_count(trace), 0);   /* nothing yet */

        /* Configured budgets.max_bindings resolves into the core limit that
         * serve() feeds to the transport admission cap (tcfg.max_connections =
         * moqr_core_get_limits(core).max_bindings) — so a non-default budget
         * moves the transport cap, not just the core pool. */
        moqr_core_limits_t lim;
        moqr_core_get_limits(core, &lim);
        MOQ_TEST_CHECK_EQ_U64(lim.max_bindings, 32);

        /* A real transition (binding open → ROUTE_ADD) lands on the ring. */
        moqr_binding_t b0;
        MOQ_TEST_CHECK(moqr_core_binding_open(core, 1, &b0) == MOQR_OK);
        MOQ_TEST_CHECK(moqr_trace_count(trace) >= 1);

        /* Overflow the 4-record ring; retained caps at 4 though more emit —
         * proving the configured depth flowed into the ring, not a default. */
        for (uint64_t i = 0; i < 10; i++) {
            moqr_binding_t bx;
            MOQ_TEST_CHECK(moqr_core_binding_open(core, 100 + i, &bx) ==
                           MOQR_OK);
        }
        moqr_trace_rec_t recs[16];
        size_t retained = moqr_trace_read(trace, recs, 16);
        MOQ_TEST_CHECK_EQ_SIZE(retained, (size_t)4);
        MOQ_TEST_CHECK(moqr_trace_count(trace) > 4);

        moqr_core_destroy(core);
        moqr_trace_destroy(trace);
    }

    /* The trace ring is accounted in the process ceiling: a deeper ring
     * raises the reported total by exactly its record-array growth, while the
     * (identical) core budgets stay fixed. Proves capacity output tracks
     * what build_core actually reserves, not just the core pools. */
    {
        moqr_cli_config_t ca1, ca2, ca0;
        MOQ_TEST_CHECK(parse("{\"listener\":{\"port\":1},"
                             "\"telemetry\":{\"trace_ring_records\":1024}}",
                             &ca1, err, sizeof(err)) == MOQR_OK);
        MOQ_TEST_CHECK(parse("{\"listener\":{\"port\":1},"
                             "\"telemetry\":{\"trace_ring_records\":8192}}",
                             &ca2, err, sizeof(err)) == MOQR_OK);
        const moq_alloc_t *alloc = moq_alloc_default();
        moqr_cli_capacity_t p1, p2;
        MOQ_TEST_CHECK(moqr_cli_describe_capacity(&ca1, alloc, 0, &p1) ==
                       MOQR_OK);
        MOQ_TEST_CHECK(moqr_cli_describe_capacity(&ca2, alloc, 0, &p2) ==
                       MOQR_OK);
        /* Identical core budgets => identical core AND bind terms. */
        MOQ_TEST_CHECK_EQ_U64(p1.core_structure_bytes,
                              p2.core_structure_bytes);
        MOQ_TEST_CHECK_EQ_U64(p1.core_payload_bytes, p2.core_payload_bytes);
        MOQ_TEST_CHECK(p1.bind_structure_bytes > 0);
        MOQ_TEST_CHECK_EQ_U64(p1.bind_structure_bytes,
                              p2.bind_structure_bytes);
        /* lanes=1 is the direct composition: core + bind + trace, no shard
         * container, plus the one permanent snapshot row the single-lane
         * serve keeps so every lane count renders through one path. */
        MOQ_TEST_CHECK_EQ_U64(p1.cross_shard_bytes, 0);
        MOQ_TEST_CHECK_EQ_U64(p1.cli_runtime_bytes,
                              moqr_cli_snapshot_bytes(1u));
        MOQ_TEST_CHECK_EQ_U64(p1.total_bytes, p1.core_structure_bytes +
                                                  p1.core_payload_bytes +
                                                  p1.bind_structure_bytes +
                                                  p1.trace_bytes +
                                                  p1.cli_runtime_bytes);
        MOQ_TEST_CHECK(p2.trace_bytes > p1.trace_bytes);
        MOQ_TEST_CHECK_EQ_U64(p2.trace_bytes - p1.trace_bytes,
                              (uint64_t)(8192 - 1024) *
                                  sizeof(moqr_trace_rec_t));
        MOQ_TEST_CHECK_EQ_U64(p2.total_bytes - p1.total_bytes,
                              p2.trace_bytes - p1.trace_bytes);
        /* Default depth (0) still accounts a non-zero ring. */
        MOQ_TEST_CHECK(parse("{\"listener\":{\"port\":1}}", &ca0, err,
                             sizeof(err)) == MOQR_OK);
        moqr_cli_capacity_t p0;
        MOQ_TEST_CHECK(moqr_cli_describe_capacity(&ca0, alloc, 0, &p0) ==
                       MOQR_OK);
        MOQ_TEST_CHECK(p0.trace_bytes > 0);

        /* lanes>1 describes the WHOLE process off the same shared builder
         * serve consumes: cross-shard terms appear, the CLI runtime joins
         * the total, and the per-lane clamp shows in usable bindings. */
        moqr_cli_config_t cl1, cl4;
        MOQ_TEST_CHECK(parse("{\"listener\":{\"port\":1,\"lanes\":1}}", &cl1,
                             err, sizeof(err)) == MOQR_OK);
        MOQ_TEST_CHECK(parse("{\"listener\":{\"port\":1,\"lanes\":4}}", &cl4,
                             err, sizeof(err)) == MOQR_OK);
        moqr_cli_capacity_t q1, q4;
        MOQ_TEST_CHECK(moqr_cli_describe_capacity(&cl1, alloc, 0, &q1) ==
                       MOQR_OK);
        MOQ_TEST_CHECK(moqr_cli_describe_capacity(&cl4, alloc, 512, &q4) ==
                       MOQR_OK);
        MOQ_TEST_CHECK(q4.total_bytes > q1.total_bytes);
        MOQ_TEST_CHECK(q4.cross_shard_bytes > 0);
        MOQ_TEST_CHECK_EQ_U64(q4.cli_runtime_bytes,
                              512 + moqr_cli_snapshot_bytes(4));
        /* Default core max_bindings 64: 4 lanes leave 60 external each. */
        MOQ_TEST_CHECK_EQ_U64(q4.usable_bindings_per_shard, 60);
        MOQ_TEST_CHECK_EQ_U64(q1.usable_bindings_per_shard, 64);
        /* The same builder underpins both commands: its resolved config is
         * byte-stable across two invocations. */
        moqr_shards_cfg_t b1, b2;
        moqr_cli_build_shards_cfg(&cl4, alloc, &b1);
        moqr_cli_build_shards_cfg(&cl4, alloc, &b2);
        MOQ_TEST_CHECK(memcmp(&b1, &b2, sizeof(b1)) == 0);
        /* Production admission is ON whenever the CLI runs multiple lanes
         * (the one place the rule lives), OFF at a single lane. */
        MOQ_TEST_CHECK(b1.admit_remote_demand);   /* lanes=4 -> on */
        MOQ_TEST_CHECK_EQ_U64(b1.bind_cfg.max_conns, 60);
        {
            moqr_shards_cfg_t b1a;
            moqr_cli_build_shards_cfg(&cl1, alloc, &b1a);
            MOQ_TEST_CHECK(!b1a.admit_remote_demand);   /* lanes=1 -> off */
        }
        /* CLI-composition overflow refuses INVAL: a poisoned serve-context
         * size must never wrap into a small OK total. */
        moqr_cli_capacity_t qo;
        MOQ_TEST_CHECK(moqr_cli_describe_capacity(&cl4, alloc, SIZE_MAX,
                                                  &qo) == MOQR_ERR_INVAL);
        MOQ_TEST_CHECK_EQ_U64(qo.total_bytes, 0);
    }

    /* ---- production admission flip: the builder rule + exact capacity
     * delta, proven structurally (not through slack) ---- */
    {
        const moq_alloc_t *alloc = moq_alloc_default();
        /* lanes=1 off; lanes 2, 4, and the max lane count all on — the
         * builder is the sole authority, and the resolved shards limits +
         * capacity admission flag both track it. */
        struct {
            uint32_t lanes;
            bool     admit;
        } cases[] = { { 1, false }, { 2, true }, { 4, true },
                      { MOQR_CLI_MAX_LANES, true } };
        for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
            /* The maximum lane count needs a binding pool large enough to
             * leave one external slot per lane after each manager consumes
             * its K slots (the K>1 binding-budget guard); every case below
             * uses a headroom pool so all four RESOLVE. */
            char js[128];
            snprintf(js, sizeof(js),
                     "{\"listener\":{\"port\":1,\"lanes\":%u},"
                     "\"budgets\":{\"max_bindings\":256}}",
                     cases[i].lanes);
            moqr_cli_config_t c;
            MOQ_TEST_CHECK(parse(js, &c, err, sizeof(err)) == MOQR_OK);
            moqr_shards_cfg_t sc;
            moqr_cli_build_shards_cfg(&c, alloc, &sc);
            MOQ_TEST_CHECK(sc.admit_remote_demand == cases[i].admit);
            /* The resolved limits and the capacity model agree with the
             * builder — no third place decides admission. */
            moqr_shards_limits_t lim;
            MOQ_TEST_CHECK(moqr_shards_cfg_resolve(&sc, &lim) == MOQR_OK);
            MOQ_TEST_CHECK(lim.admit == cases[i].admit);
            moqr_shards_capacity_t cap;
            MOQ_TEST_CHECK(moqr_shards_capacity_describe(&sc, &cap) ==
                           MOQR_OK);
            MOQ_TEST_CHECK(cap.admission == cases[i].admit);
        }

        /* The EXACT admission delta, structurally: clone one K>1 builder
         * output and turn admission OFF only in the comparison copy. The
         * on/off shard structures must differ by ONLY the admission
         * progress-table term the capacity model adds; every other term is
         * byte-equal. Since the private progress-slot size is not a public
         * constant, the delta is taken from the model's OWN computed term
         * (shards_structure on minus off) and proven structural three ways:
         * it is confined to the shard structure, it scales EXACTLY linearly
         * with K (so it is the per-shard progress storage times K, not a
         * fixed overhead), and it scales linearly with the subgroup-slot
         * budget (so it is the pend x sg progress table, not some other
         * admission cost). */
        moqr_cli_config_t c4;
        MOQ_TEST_CHECK(parse("{\"listener\":{\"port\":1,\"lanes\":4}}", &c4,
                             err, sizeof(err)) == MOQR_OK);
        moqr_shards_cfg_t on, off;
        moqr_cli_build_shards_cfg(&c4, alloc, &on);
        off = on;
        off.admit_remote_demand = false;
        MOQ_TEST_CHECK(on.admit_remote_demand);
        moqr_shards_capacity_t cap_on, cap_off;
        MOQ_TEST_CHECK(moqr_shards_capacity_describe(&on, &cap_on) ==
                       MOQR_OK);
        MOQ_TEST_CHECK(moqr_shards_capacity_describe(&off, &cap_off) ==
                       MOQR_OK);
        uint64_t delta = cap_on.shards_structure_bytes -
                         cap_off.shards_structure_bytes;
        MOQ_TEST_CHECK(delta > 0);
        /* Only the shard structure moves: every other term is byte-equal. */
        MOQ_TEST_CHECK_EQ_U64(cap_on.channel_byte_ceiling,
                              cap_off.channel_byte_ceiling);
        MOQ_TEST_CHECK_EQ_U64(cap_on.canon_byte_ceiling,
                              cap_off.canon_byte_ceiling);
        MOQ_TEST_CHECK_EQ_U64(cap_on.staging_byte_ceiling,
                              cap_off.staging_byte_ceiling);
        MOQ_TEST_CHECK_EQ_U64(cap_on.core_structure_bytes,
                              cap_off.core_structure_bytes);
        MOQ_TEST_CHECK_EQ_U64(cap_on.core_payload_bytes,
                              cap_off.core_payload_bytes);
        MOQ_TEST_CHECK_EQ_U64(cap_on.bind_structure_bytes,
                              cap_off.bind_structure_bytes);
        MOQ_TEST_CHECK_EQ_U64(cap_on.trace_bytes, cap_off.trace_bytes);
        /* The ceiling moves by exactly the admission delta. */
        MOQ_TEST_CHECK_EQ_U64(cap_on.relay_alloc_ceiling -
                                  cap_off.relay_alloc_ceiling,
                              delta);
        /* The CLI total moves by exactly the same delta (cli_runtime is
         * admission-independent). */
        moqr_cli_capacity_t con;
        MOQ_TEST_CHECK(moqr_cli_describe_capacity(&c4, alloc,
                                                  sizeof(void *) * 8, &con) ==
                       MOQR_OK);
        MOQ_TEST_CHECK_EQ_U64(con.total_bytes,
                              cap_on.relay_alloc_ceiling +
                                  con.cli_runtime_bytes);
        MOQ_TEST_CHECK_EQ_U64(con.total_bytes -
                                  (cap_off.relay_alloc_ceiling +
                                   con.cli_runtime_bytes),
                              delta);
        /* K-linearity: the same delta at lanes=2 is EXACTLY half (the
         * per-shard progress storage times K). */
        {
            moqr_cli_config_t c2;
            MOQ_TEST_CHECK(parse("{\"listener\":{\"port\":1,\"lanes\":2}}",
                                 &c2, err, sizeof(err)) == MOQR_OK);
            moqr_shards_cfg_t on2 = on, off2;
            moqr_cli_build_shards_cfg(&c2, alloc, &on2);
            off2 = on2;
            off2.admit_remote_demand = false;
            moqr_shards_capacity_t con2, coff2;
            MOQ_TEST_CHECK(moqr_shards_capacity_describe(&on2, &con2) ==
                           MOQR_OK);
            MOQ_TEST_CHECK(moqr_shards_capacity_describe(&off2, &coff2) ==
                           MOQR_OK);
            uint64_t d2 = con2.shards_structure_bytes -
                          coff2.shards_structure_bytes;
            /* delta(K=4)/4 == delta(K=2)/2 (constant per shard). */
            MOQ_TEST_CHECK_EQ_U64(delta * 2u, d2 * 4u);
        }
        /* Subgroup-slot linearity, EXACT: the admission term is
         * K·(2·pend·sg·psg + pend). Doubling subgroup_slots doubles the
         * 2·pend·sg part while the flat +pend part is unchanged, so
         *   sdelta == 2·delta − K·pend
         * must hold to the byte. This pins the term SHAPE (an affine function
         * of sg with the right slope-doubling and intercept), independent of
         * the private progress-slot size. */
        {
            moqr_shards_limits_t blim;
            MOQ_TEST_CHECK(moqr_shards_cfg_resolve(&on, &blim) == MOQR_OK);
            moqr_cli_config_t cs;
            MOQ_TEST_CHECK(parse(
                "{\"listener\":{\"port\":1,\"lanes\":4},\"budgets\":{"
                "\"cross_shard\":{\"subgroup_slots\":128}}}",
                &cs, err, sizeof(err)) == MOQR_OK);
            moqr_shards_cfg_t son, soff;
            moqr_cli_build_shards_cfg(&cs, alloc, &son);
            soff = son;
            soff.admit_remote_demand = false;
            moqr_shards_limits_t slim;
            MOQ_TEST_CHECK(moqr_shards_cfg_resolve(&son, &slim) == MOQR_OK);
            /* Premises: subgroup slots doubled; pend and K held constant. */
            MOQ_TEST_CHECK_EQ_U64(slim.sg_slots, 2u * (uint64_t)blim.sg_slots);
            MOQ_TEST_CHECK_EQ_U64(slim.pend_cap, blim.pend_cap);
            MOQ_TEST_CHECK_EQ_U64(slim.shards, blim.shards);
            moqr_shards_capacity_t scon, scoff;
            MOQ_TEST_CHECK(moqr_shards_capacity_describe(&son, &scon) ==
                           MOQR_OK);
            MOQ_TEST_CHECK(moqr_shards_capacity_describe(&soff, &scoff) ==
                           MOQR_OK);
            uint64_t sdelta = scon.shards_structure_bytes -
                              scoff.shards_structure_bytes;
            MOQ_TEST_CHECK_EQ_U64(sdelta,
                                  2u * delta -
                                      (uint64_t)slim.shards * slim.pend_cap);
        }

        /* Independent, NON-CIRCULAR pin: a live K>1 runtime requests exactly
         * the model's admission term more when admission is on. The counting
         * allocator knows nothing of the capacity formula — it measures the
         * bytes create() actually asks for — so an admission component the
         * model omits or mis-sizes makes the measured and described deltas
         * disagree. (The K-linearity and slot-shape checks above prove the
         * term's structure; this proves its magnitude against reality.) */
        {
            count_alloc_t on_a, off_a;
            ka_init(&on_a);
            ka_init(&off_a);
            moqr_shards_cfg_t rt_on, rt_off;
            moqr_cli_build_shards_cfg(&c4, &on_a.vt, &rt_on);
            moqr_cli_build_shards_cfg(&c4, &off_a.vt, &rt_off);
            rt_off.admit_remote_demand = false;
            MOQ_TEST_CHECK(rt_on.admit_remote_demand);
            moqr_shards_capacity_t mon, moff;
            MOQ_TEST_CHECK(moqr_shards_capacity_describe(&rt_on, &mon) ==
                           MOQR_OK);
            MOQ_TEST_CHECK(moqr_shards_capacity_describe(&rt_off, &moff) ==
                           MOQR_OK);
            uint64_t model_delta =
                mon.shards_structure_bytes - moff.shards_structure_bytes;
            /* same K=4 config as `on`/`off` above -> same described term. */
            MOQ_TEST_CHECK_EQ_U64(model_delta, delta);
            moqr_shards_t *rt_on_s = NULL, *rt_off_s = NULL;
            MOQ_TEST_CHECK(moqr_shards_create(&rt_on, &rt_on_s) == MOQR_OK);
            MOQ_TEST_CHECK(moqr_shards_create(&rt_off, &rt_off_s) == MOQR_OK);
            MOQ_TEST_CHECK(on_a.live > off_a.live);
            MOQ_TEST_CHECK_EQ_U64((uint64_t)(on_a.live - off_a.live),
                                  model_delta);
            moqr_shards_destroy(rt_on_s);
            moqr_shards_destroy(rt_off_s);
            /* clean teardown: the difference was solely the admission tables. */
            MOQ_TEST_CHECK_EQ_INT((int)on_a.live, 0);
            MOQ_TEST_CHECK_EQ_INT((int)off_a.live, 0);
        }

        /* K=1 on/off capacity is byte-identical: admission adds zero at a
         * single lane (no manager, no progress tables). */
        moqr_cli_config_t c1;
        MOQ_TEST_CHECK(parse("{\"listener\":{\"port\":1,\"lanes\":1}}", &c1,
                             err, sizeof(err)) == MOQR_OK);
        moqr_shards_cfg_t k1;
        moqr_cli_build_shards_cfg(&c1, alloc, &k1);
        MOQ_TEST_CHECK(!k1.admit_remote_demand);
        moqr_shards_cfg_t k1on = k1;
        k1on.admit_remote_demand = true;   /* force on: inert at K=1 */
        moqr_shards_capacity_t k1c_off, k1c_on;
        MOQ_TEST_CHECK(moqr_shards_capacity_describe(&k1, &k1c_off) ==
                       MOQR_OK);
        MOQ_TEST_CHECK(moqr_shards_capacity_describe(&k1on, &k1c_on) ==
                       MOQR_OK);
        /* The resolver normalizes effective admission to (K>1 && configured),
         * so a forced admit is reported as OFF at a single lane: the two
         * descriptors — every field, including the admission flag — are
         * byte-identical (exact, no ceiling-only fallback). */
        MOQ_TEST_CHECK(!k1c_on.admission);
        MOQ_TEST_CHECK(memcmp(&k1c_off, &k1c_on, sizeof(k1c_off)) == 0);

        /* The serve composition seam (what cmd_serve_lanes consumes) is the
         * SAME builder output — admission preserved, never overwritten —
         * plus the facade cap = lanes * usable_bindings_per_shard (the
         * strict per-lane clamp, not lanes * max_bindings). */
        moqr_shards_cfg_t serve_scfg;
        uint32_t serve_max = 0;
        MOQ_TEST_CHECK(moqr_cli_serve_compose(&c4, alloc, &serve_scfg,
                                              &serve_max) == MOQR_OK);
        moqr_shards_cfg_t direct;
        moqr_cli_build_shards_cfg(&c4, alloc, &direct);
        MOQ_TEST_CHECK(memcmp(&serve_scfg, &direct, sizeof(direct)) == 0);
        MOQ_TEST_CHECK(serve_scfg.admit_remote_demand);   /* not overwritten */
        moqr_shards_limits_t sl;
        MOQ_TEST_CHECK(moqr_shards_cfg_resolve(&serve_scfg, &sl) == MOQR_OK);
        MOQ_TEST_CHECK_EQ_U64(serve_max, 4u * sl.usable_bindings);
        MOQ_TEST_CHECK(serve_max != 4u * 64u);   /* NOT the old coarse rule */
        /* lanes=1 serve composition: admission off, facade = usable (64). */
        moqr_shards_cfg_t s1cfg;
        uint32_t s1max = 0;
        MOQ_TEST_CHECK(moqr_cli_serve_compose(&c1, alloc, &s1cfg, &s1max) ==
                       MOQR_OK);
        MOQ_TEST_CHECK(!s1cfg.admit_remote_demand);
        MOQ_TEST_CHECK_EQ_U64(s1max, 64u);
    }

    /* ---- budgets.cross_shard: strict operator keys for the K>1 pools ---- */
    {
        const moq_alloc_t *alloc = moq_alloc_default();
        /* Absent vs {}: byte-identical builder resolution and capacity. */
        moqr_cli_config_t ab, emp;
        MOQ_TEST_CHECK(parse("{\"listener\":{\"port\":1,\"lanes\":4}}", &ab,
                             err, sizeof(err)) == MOQR_OK);
        MOQ_TEST_CHECK(parse("{\"listener\":{\"port\":1,\"lanes\":4},"
                             "\"budgets\":{\"cross_shard\":{}}}",
                             &emp, err, sizeof(err)) == MOQR_OK);
        moqr_shards_cfg_t ba, be;
        moqr_cli_build_shards_cfg(&ab, alloc, &ba);
        moqr_cli_build_shards_cfg(&emp, alloc, &be);
        MOQ_TEST_CHECK(memcmp(&ba, &be, sizeof(ba)) == 0);
        moqr_cli_capacity_t ca_, ce_;
        MOQ_TEST_CHECK(moqr_cli_describe_capacity(&ab, alloc, 0, &ca_) ==
                       MOQR_OK);
        MOQ_TEST_CHECK(moqr_cli_describe_capacity(&emp, alloc, 0, &ce_) ==
                       MOQR_OK);
        MOQ_TEST_CHECK_EQ_U64(ca_.total_bytes, ce_.total_bytes);

        /* Every key parses, maps through the ONE builder, and lands on the
         * shard config with distinct values. */
        moqr_cli_config_t full;
        MOQ_TEST_CHECK(parse(
            "{\"listener\":{\"port\":1,\"lanes\":4},\"budgets\":{"
            "\"cross_shard\":{\"journal_entries\":11,"
            "\"mailbox_entries\":12,\"demand_channel_entries\":13,"
            "\"demand_channel_bytes\":16777216,\"pending_demands\":14,"
            "\"subgroup_slots\":15}}}",
            &full, err, sizeof(err)) == MOQR_OK);
        moqr_shards_cfg_t bf;
        moqr_cli_build_shards_cfg(&full, alloc, &bf);
        MOQ_TEST_CHECK_EQ_U64(bf.journal_entries, 11);
        MOQ_TEST_CHECK_EQ_U64(bf.mailbox_entries, 12);
        MOQ_TEST_CHECK_EQ_U64(bf.demand_channel_entries, 13);
        MOQ_TEST_CHECK_EQ_U64(bf.demand_channel_bytes, 16777216);
        MOQ_TEST_CHECK_EQ_U64(bf.pending_demand_entries, 14);
        MOQ_TEST_CHECK_EQ_U64(bf.pump_subgroup_slots, 15);
        /* ...and each knob reaches the capacity model: bumping the mailbox
         * moves the canon ceiling; the byte cap moves the channel term. */
        moqr_shards_capacity_t sf, sb;
        MOQ_TEST_CHECK(moqr_shards_capacity_describe(&bf, &sf) == MOQR_OK);
        MOQ_TEST_CHECK(moqr_shards_capacity_describe(&ba, &sb) == MOQR_OK);
        MOQ_TEST_CHECK(sf.canon_byte_ceiling != sb.canon_byte_ceiling);
        MOQ_TEST_CHECK(sf.channel_byte_ceiling != sb.channel_byte_ceiling);
        /* Boundary acceptance: 0 (library default) and the entry maximum. */
        moqr_cli_config_t bd;
        MOQ_TEST_CHECK(parse(
            "{\"listener\":{\"port\":1,\"lanes\":2},\"budgets\":{"
            "\"cross_shard\":{\"journal_entries\":0,"
            "\"mailbox_entries\":1048576}}}",
            &bd, err, sizeof(err)) == MOQR_OK);
        moqr_shards_cfg_t bbd;
        moqr_cli_build_shards_cfg(&bd, alloc, &bbd);
        MOQ_TEST_CHECK_EQ_U64(bbd.journal_entries, 0);
        MOQ_TEST_CHECK_EQ_U64(bbd.mailbox_entries, 1048576);

        /* Strict rejection: oversize, negative, non-number, non-object,
         * unknown keys, and any admission/pump-tuning attempt. */
        static const char *bad[] = {
            "{\"listener\":{\"port\":1},\"budgets\":{\"cross_shard\":{"
            "\"mailbox_entries\":1048577}}}",
            "{\"listener\":{\"port\":1},\"budgets\":{\"cross_shard\":{"
            "\"demand_channel_bytes\":1099511627777}}}",
            "{\"listener\":{\"port\":1},\"budgets\":{\"cross_shard\":{"
            "\"journal_entries\":-1}}}",
            "{\"listener\":{\"port\":1},\"budgets\":{\"cross_shard\":{"
            "\"journal_entries\":\"many\"}}}",
            "{\"listener\":{\"port\":1},\"budgets\":{\"cross_shard\":7}}",
            "{\"listener\":{\"port\":1},\"budgets\":{\"cross_shard\":{"
            "\"bogus\":1}}}",
            "{\"listener\":{\"port\":1},\"budgets\":{\"cross_shard\":{"
            "\"admit_remote_demand\":true}}}",
            "{\"listener\":{\"port\":1},\"budgets\":{\"cross_shard\":{"
            "\"admit\":1}}}",
            "{\"listener\":{\"port\":1},\"budgets\":{\"cross_shard\":{"
            "\"pump_turn_messages\":8}}}",
            "{\"listener\":{\"port\":1},\"budgets\":{\"cross_shard\":{"
            "\"pump_turn_bytes\":8}}}",
        };
        for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); i++) {
            moqr_cli_config_t r;
            MOQ_TEST_CHECK(parse(bad[i], &r, err, sizeof(err)) ==
                           MOQR_ERR_INVAL);
            MOQ_TEST_CHECK(strstr(err, "cross_shard") != NULL);
            if (i >= 5) {   /* unknown/admission/pump keys: the whitelist
                             * speaks first — "unknown key", never a type
                             * complaint about a rejected key's value. */
                MOQ_TEST_CHECK(strstr(err, "unknown key") != NULL);
            }
        }

        /* The shared resolver stays authoritative: an explicit channel byte
         * cap below the resolved log payload budget refuses through the
         * CLI capacity path. */
        moqr_cli_config_t low;
        MOQ_TEST_CHECK(parse(
            "{\"listener\":{\"port\":1,\"lanes\":2},\"budgets\":{"
            "\"log\":{\"max_bytes\":1048576},"
            "\"cross_shard\":{\"demand_channel_bytes\":1024}}}",
            &low, err, sizeof(err)) == MOQR_OK);   /* schema-valid... */
        moqr_cli_capacity_t cl;
        MOQ_TEST_CHECK(moqr_cli_describe_capacity(&low, alloc, 0, &cl) ==
                       MOQR_ERR_INVAL);            /* ...resolver refuses */
        /* The same invariant holds at lanes=1 — accepting the object means
         * accepting VALID inert settings, never bypassing cross-field
         * validation. */
        moqr_cli_config_t low1;
        MOQ_TEST_CHECK(parse(
            "{\"listener\":{\"port\":1,\"lanes\":1},\"budgets\":{"
            "\"log\":{\"max_bytes\":1048576},"
            "\"cross_shard\":{\"demand_channel_bytes\":1024}}}",
            &low1, err, sizeof(err)) == MOQR_OK);
        moqr_cli_capacity_t cl1;
        MOQ_TEST_CHECK(moqr_cli_describe_capacity(&low1, alloc, 0, &cl1) ==
                       MOQR_ERR_INVAL);
        /* The SHARED pre-command validator — the gate main() runs before
         * capacity AND both serve paths — refuses the same configs at both
         * lane counts, and passes valid ones. */
        MOQ_TEST_CHECK(moqr_cli_config_validate(&low1, alloc) ==
                       MOQR_ERR_INVAL);
        MOQ_TEST_CHECK(moqr_cli_config_validate(&low, alloc) ==
                       MOQR_ERR_INVAL);
        MOQ_TEST_CHECK(moqr_cli_config_validate(&full, alloc) == MOQR_OK);
        MOQ_TEST_CHECK(moqr_cli_config_validate(&emp, alloc) == MOQR_OK);

        /* lanes=1 accepts the object; the direct runtime composition is
         * untouched by cross-shard knobs. */
        moqr_cli_config_t l1p, l1c;
        MOQ_TEST_CHECK(parse("{\"listener\":{\"port\":1,\"lanes\":1}}",
                             &l1p, err, sizeof(err)) == MOQR_OK);
        MOQ_TEST_CHECK(parse(
            "{\"listener\":{\"port\":1,\"lanes\":1},\"budgets\":{"
            "\"cross_shard\":{\"mailbox_entries\":12,"
            "\"journal_entries\":11}}}",
            &l1c, err, sizeof(err)) == MOQR_OK);
        moqr_cli_capacity_t k1p, k1c;
        MOQ_TEST_CHECK(moqr_cli_describe_capacity(&l1p, alloc, 0, &k1p) ==
                       MOQR_OK);
        MOQ_TEST_CHECK(moqr_cli_describe_capacity(&l1c, alloc, 0, &k1c) ==
                       MOQR_OK);
        MOQ_TEST_CHECK_EQ_U64(k1p.total_bytes, k1c.total_bytes);
        MOQ_TEST_CHECK_EQ_U64(k1c.cross_shard_bytes, 0);
    }

    /* ---- auth: allow-all default, toy policy, strict failures ---- */

    /* Absent "auth" -> allow-all (no hook). */
    MOQ_TEST_CHECK(parse("{\"listener\":{\"port\":1}}", &cfg, err,
                         sizeof(err)) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_INT((int)cfg.auth.mode, (int)MOQR_CLI_AUTH_ALLOW_ALL);
    MOQ_TEST_CHECK(
        parse("{\"listener\":{\"port\":1},\"auth\":{\"mode\":\"allow_all\"}}",
              &cfg, err, sizeof(err)) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_INT((int)cfg.auth.mode, (int)MOQR_CLI_AUTH_ALLOW_ALL);

    /* A toy policy parses: mode + default + one prefix rule. */
    MOQ_TEST_CHECK(
        parse("{\"listener\":{\"port\":1},\"auth\":{\"mode\":\"toy\","
              "\"default\":\"allow\",\"rules\":[{\"action\":\"subscribe\","
              "\"namespace_prefix\":[\"live\"],\"decision\":\"deny\","
              "\"reason\":\"unscoped\"}]}}",
              &cfg, err, sizeof(err)) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_INT((int)cfg.auth.mode, (int)MOQR_CLI_AUTH_TOY);
    MOQ_TEST_CHECK_EQ_SIZE(cfg.auth.toy.rule_count, (size_t)1);
    MOQ_TEST_CHECK(cfg.auth.toy.default_decision == MOQR_AUTH_ALLOW);
    MOQ_TEST_CHECK(cfg.auth.rules[0].action == MOQR_AUTH_SUBSCRIBE);
    MOQ_TEST_CHECK(cfg.auth.rules[0].decision == MOQR_AUTH_DENY);
    MOQ_TEST_CHECK_EQ_SIZE(cfg.auth.rules[0].ns_prefix.count, (size_t)1);

    /* Strict failures: every malformed auth doc is rejected loudly. */
    MOQ_TEST_CHECK(parse("{\"listener\":{\"port\":1},\"auth\":{}}", &cfg, err,
                         sizeof(err)) == MOQR_ERR_INVAL);   /* mode required  */
    MOQ_TEST_CHECK(   /* unknown auth key */
        parse("{\"listener\":{\"port\":1},\"auth\":{\"mode\":\"toy\","
              "\"bogus\":1}}", &cfg, err, sizeof(err)) == MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(strstr(err, "auth") != NULL);
    MOQ_TEST_CHECK(   /* toy needs an explicit default */
        parse("{\"listener\":{\"port\":1},\"auth\":{\"mode\":\"toy\","
              "\"rules\":[]}}", &cfg, err, sizeof(err)) == MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(   /* allow_all rejects default/rules */
        parse("{\"listener\":{\"port\":1},\"auth\":{\"mode\":\"allow_all\","
              "\"default\":\"deny\"}}", &cfg, err, sizeof(err)) ==
        MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(   /* unknown action */
        parse("{\"listener\":{\"port\":1},\"auth\":{\"mode\":\"toy\","
              "\"default\":\"deny\",\"rules\":[{\"action\":\"bogus\","
              "\"decision\":\"allow\"}]}}", &cfg, err, sizeof(err)) ==
        MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(   /* DEFER is not authorable in a rule */
        parse("{\"listener\":{\"port\":1},\"auth\":{\"mode\":\"toy\","
              "\"default\":\"deny\",\"rules\":[{\"action\":\"subscribe\","
              "\"decision\":\"defer\"}]}}", &cfg, err, sizeof(err)) ==
        MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(   /* nor as the default */
        parse("{\"listener\":{\"port\":1},\"auth\":{\"mode\":\"toy\","
              "\"default\":\"defer\"}}", &cfg, err, sizeof(err)) ==
        MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(   /* unknown reason */
        parse("{\"listener\":{\"port\":1},\"auth\":{\"mode\":\"toy\","
              "\"default\":\"deny\",\"rules\":[{\"action\":\"subscribe\","
              "\"decision\":\"deny\",\"reason\":\"bogus\"}]}}", &cfg, err,
              sizeof(err)) == MOQR_ERR_INVAL);

    /* build_core installs the toy hook and it ENFORCES — not just parsed. */
    {
        moqr_cli_config_t ac;
        MOQ_TEST_CHECK(
            parse("{\"listener\":{\"port\":1},\"auth\":{\"mode\":\"toy\","
                  "\"default\":\"allow\",\"rules\":[{\"action\":\"subscribe\","
                  "\"namespace_prefix\":[\"live\"],\"decision\":\"deny\","
                  "\"reason\":\"unscoped\"}]}}",
                  &ac, err, sizeof(err)) == MOQR_OK);
        const moq_alloc_t *alloc = moq_alloc_default();
        moqr_trace_t *trace = NULL;
        moqr_core_t *core = NULL;
        MOQ_TEST_CHECK(moqr_cli_build_core(&ac, alloc, &trace, &core) ==
                       MOQR_OK);

        moqr_auth_request_t req;
        moqr_auth_verdict_t v;
        moq_bytes_t live[1];
        moq_bytes_t vod[1];
        live[0].data = (const uint8_t *)"live";
        live[0].len = 4;
        vod[0].data = (const uint8_t *)"vod";
        vod[0].len = 3;

        /* subscribe under "live" -> rule -> DENY(unscoped) */
        memset(&req, 0, sizeof(req));
        req.struct_size = (uint32_t)sizeof(req);
        req.action = MOQR_AUTH_SUBSCRIBE;
        req.ns.parts = live;
        req.ns.count = 1;
        moqr_core_authorize(core, &req, &v);
        MOQ_TEST_CHECK(v.decision == MOQR_AUTH_DENY);
        MOQ_TEST_CHECK(v.reason == MOQR_AUTH_REASON_UNSCOPED);

        /* subscribe elsewhere -> default ALLOW (proves the hook, not a stub) */
        req.ns.parts = vod;
        req.ns.count = 1;
        moqr_core_authorize(core, &req, &v);
        MOQ_TEST_CHECK(v.decision == MOQR_AUTH_ALLOW);

        moqr_core_destroy(core);
        moqr_trace_destroy(trace);
    }

    failures += test_snapshot_protocol();
    failures += test_snapshot_render_gate();
    failures += test_snapshot_protocol_mt();
    failures += test_snapshot_coord_production();
    failures += test_snapshot_coord_failures();
    failures += test_k1_counts_its_snapshot_row();
    failures += test_logging_config();

    MOQ_TEST_PASS("relay_cli_config");
    return failures == 0 ? 0 : 1; /* exit status truncates to 8 bits */
}
