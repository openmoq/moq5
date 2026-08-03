#ifndef MOQ_TEST_FAILPOINT_H
#define MOQ_TEST_FAILPOINT_H

/*
 * Failpoint allocator for transactional allocation-failure sweeps.
 *
 * Wraps libc while keeping three kinds of evidence a plain counting or
 * fail-at-N allocator cannot provide:
 *
 *  - an ATTEMPT LOG of injectable allocation attempts only (alloc and both
 *    realloc forms; frees never enter it, so `fail_at`, declared attempt
 *    counts and log-prefix comparisons all index the same monotonic
 *    attempt-ordinal space);
 *  - a LIVE POINTER->SIZE TABLE that validates the moq_alloc_t size
 *    contract: realloc's old_size and free's size are checked against the
 *    recorded size, and a mismatch, an unknown pointer, or table overflow
 *    each latch a sticky failure naming the offending attempt ordinal;
 *  - EXACT-STATE ORACLES: an unordered-set snapshot of the live map for
 *    failure-boundary equality, and a signed size-multiset delta (added
 *    sizes, removed sizes, count delta, live-byte delta) for comparing a
 *    recovered run's allocation footprint against a baseline run whose
 *    pointer values necessarily differ.
 *
 * Ordinal semantics: `call_count` counts every alloc/realloc attempt over
 * the allocator's lifetime and is never reset; an operation window is a
 * `[pre = call_count]` watermark, and `fail_at` is armed as an absolute
 * ordinal (`pre + k`). Exactly that attempt fails, once.
 *
 * A declared SIGNATURE pins an operation's attempt sequence: the exact
 * count and, per attempt, the kind and a size matcher. Matchers are
 * relational where a size is not knowable to a test (private struct sizes):
 * exact, same-as-an-earlier-attempt, or any. The signature detects count,
 * kind and known-size drift; a swap of two same-kind same-size allocations
 * is deliberately outside its power and belongs to the semantic checks.
 *
 * Storage is fixed-capacity; overflow latches a sticky failure rather than
 * truncating silently. NOT installed. Test infrastructure only.
 */

#include <moq/moq.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#if defined(__GNUC__)
#define FP_UNUSED __attribute__((unused))
#else
#define FP_UNUSED
#endif

/* Negative self-tests exercise the comparison helpers on states that are
 * SUPPOSED to diverge; their diagnostics would read as failures in a
 * passing run's output. Set while asserting an expected divergence. */
FP_UNUSED static int fp_quiet;

#define FP_DIAG(...) do { if (!fp_quiet) fprintf(stderr, __VA_ARGS__); } while (0)

typedef enum fp_attempt_kind {
    FP_ALLOC = 1,        /* alloc(size) */
    FP_REALLOC_NEW,      /* realloc(NULL, 0, size) */
    FP_REALLOC_GROW,     /* realloc(ptr, old, new) with a live ptr */
} fp_attempt_kind_t;

typedef struct fp_attempt {
    uint64_t ordinal;    /* lifetime attempt ordinal, 1-indexed */
    int      kind;       /* fp_attempt_kind_t */
    size_t   size;       /* requested (new) size */
    size_t   old_size;   /* realloc-grow only; 0 otherwise */
} fp_attempt_t;

#define FP_LOG_CAP    64
#define FP_TABLE_CAP  512

typedef struct fp_live {
    void  *ptr;
    size_t size;
} fp_live_t;

typedef struct fp_alloc_state {
    int64_t  balance;        /* live block count */
    int64_t  live_bytes;     /* live byte count per the size contract */
    uint64_t call_count;     /* lifetime alloc/realloc attempts; never reset */
    uint64_t fail_at;        /* absolute attempt ordinal to fail once; 0=off */

    /* Attempt log, gated by the operation-window watermark. */
    uint64_t     log_from;   /* log only attempts with ordinal > log_from */
    fp_attempt_t log[FP_LOG_CAP];
    size_t       log_len;

    /* Live pointer->size table. */
    fp_live_t table[FP_TABLE_CAP];
    size_t    table_len;

    /* Sticky failures; each names the attempt ordinal that tripped it
     * (0 = clean). Frees trip against the most recent attempt ordinal. */
    uint64_t sticky_log_overflow;
    uint64_t sticky_table_overflow;
    uint64_t sticky_unknown_ptr;
    uint64_t sticky_size_mismatch;
} fp_alloc_state_t;

/* -- table ---------------------------------------------------------- */

FP_UNUSED static int fp_table_find(const fp_alloc_state_t *s, const void *ptr)
{
    for (size_t i = 0; i < s->table_len; i++)
        if (s->table[i].ptr == ptr) return (int)i;
    return -1;
}

/* Room for one more live block? Checked BEFORE the underlying malloc, so
 * an overflow refuses the allocation outright -- a block the table cannot
 * track could never be freed or reallocated through the wrapper again. */
FP_UNUSED static bool fp_table_room(fp_alloc_state_t *s)
{
    if (s->table_len < FP_TABLE_CAP) return true;
    if (!s->sticky_table_overflow)
        s->sticky_table_overflow = s->call_count ? s->call_count : 1;
    return false;
}

FP_UNUSED static void fp_table_add(fp_alloc_state_t *s, void *ptr, size_t size)
{
    s->table[s->table_len].ptr = ptr;
    s->table[s->table_len].size = size;
    s->table_len++;
}

FP_UNUSED static void fp_table_remove_at(fp_alloc_state_t *s, size_t i)
{
    s->table[i] = s->table[s->table_len - 1];
    s->table_len--;
}

/* -- allocator hooks ------------------------------------------------- */

FP_UNUSED static void fp_log_attempt(fp_alloc_state_t *s, int kind,
                                     size_t size, size_t old_size)
{
    if (s->call_count <= s->log_from) return;
    if (s->log_len == FP_LOG_CAP) {
        if (!s->sticky_log_overflow) s->sticky_log_overflow = s->call_count;
        return;
    }
    s->log[s->log_len].ordinal = s->call_count;
    s->log[s->log_len].kind = kind;
    s->log[s->log_len].size = size;
    s->log[s->log_len].old_size = old_size;
    s->log_len++;
}

FP_UNUSED static void *fp_alloc_fn(size_t size, void *ctx)
{
    fp_alloc_state_t *s = (fp_alloc_state_t *)ctx;
    s->call_count++;
    fp_log_attempt(s, FP_ALLOC, size, 0);
    if (s->fail_at > 0 && s->call_count == s->fail_at) return NULL;
    if (!fp_table_room(s)) return NULL;
    void *p = malloc(size);
    if (p) {
        s->balance++;
        s->live_bytes += (int64_t)size;
        fp_table_add(s, p, size);
    }
    return p;
}

FP_UNUSED static void *fp_realloc_fn(void *ptr, size_t old_sz, size_t new_sz,
                                     void *ctx)
{
    fp_alloc_state_t *s = (fp_alloc_state_t *)ctx;
    s->call_count++;
    fp_log_attempt(s, ptr ? FP_REALLOC_GROW : FP_REALLOC_NEW, new_sz,
                   ptr ? old_sz : 0);
    int idx = -1;
    size_t recorded = 0;
    if (ptr) {
        idx = fp_table_find(s, ptr);
        if (idx < 0) {
            /* An unknown pointer must NOT be delegated to libc realloc --
             * that is undefined behavior and would turn the promised sticky
             * diagnostic into a crash. Refuse the attempt instead: the
             * caller sees an allocation failure, and the sticky flag names
             * the ordinal. */
            if (!s->sticky_unknown_ptr) s->sticky_unknown_ptr = s->call_count;
            return NULL;
        }
        recorded = s->table[idx].size;
        if (recorded != old_sz) {
            /* Accounting continues from the RECORDED size, never from the
             * untrusted caller size, so the oracle stays coherent while the
             * sticky flag reports the contract violation. */
            if (!s->sticky_size_mismatch)
                s->sticky_size_mismatch = s->call_count;
        }
    }
    if (s->fail_at > 0 && s->call_count == s->fail_at) return NULL;
    if (!ptr) {
        if (!fp_table_room(s)) return NULL;
        void *p = malloc(new_sz);
        if (p) {
            s->balance++;
            s->live_bytes += (int64_t)new_sz;
            fp_table_add(s, p, new_sz);
        }
        return p;
    }
    void *p = realloc(ptr, new_sz);
    if (p) {
        /* The table entry was resolved BEFORE the realloc: the old pointer
         * value must not be inspected once the block may have moved. */
        fp_table_remove_at(s, (size_t)idx);
        s->live_bytes += (int64_t)new_sz - (int64_t)recorded;
        fp_table_add(s, p, new_sz);
    }
    return p;
}

FP_UNUSED static void fp_free_fn(void *ptr, size_t size, void *ctx)
{
    fp_alloc_state_t *s = (fp_alloc_state_t *)ctx;
    if (!ptr) return;
    int i = fp_table_find(s, ptr);
    if (i < 0) {
        /* An unknown pointer must NOT be delegated to libc free -- that is
         * undefined behavior, and it would also corrupt the accounting with
         * an untrusted size. Latch the sticky flag and leak the (unknown)
         * block; the diagnostic, not a crash, is the report. */
        if (!s->sticky_unknown_ptr)
            s->sticky_unknown_ptr = s->call_count ? s->call_count : 1;
        return;
    }
    /* Accounting uses the RECORDED size; a caller-size mismatch is reported
     * through the sticky flag rather than believed. */
    size_t recorded = s->table[i].size;
    if (recorded != size && !s->sticky_size_mismatch)
        s->sticky_size_mismatch = s->call_count ? s->call_count : 1;
    fp_table_remove_at(s, (size_t)i);
    s->balance--;
    s->live_bytes -= (int64_t)recorded;
    free(ptr);
}

FP_UNUSED static moq_alloc_t fp_allocator(fp_alloc_state_t *state)
{
    moq_alloc_t alloc = { state, fp_alloc_fn, fp_realloc_fn, fp_free_fn };
    return alloc;
}

/* All four sticky flags clean; returns the number of dirty flags after
 * printing each with its offending ordinal. */
FP_UNUSED static int fp_sticky_clean(const fp_alloc_state_t *s,
                                     const char *op)
{
    int dirty = 0;
    if (s->sticky_log_overflow) {
        fprintf(stderr, "FAILPOINT %s: attempt-log overflow at ordinal %llu\n",
                op, (unsigned long long)s->sticky_log_overflow);
        dirty++;
    }
    if (s->sticky_table_overflow) {
        fprintf(stderr, "FAILPOINT %s: table overflow at ordinal %llu\n",
                op, (unsigned long long)s->sticky_table_overflow);
        dirty++;
    }
    if (s->sticky_unknown_ptr) {
        fprintf(stderr, "FAILPOINT %s: unknown pointer at ordinal %llu\n",
                op, (unsigned long long)s->sticky_unknown_ptr);
        dirty++;
    }
    if (s->sticky_size_mismatch) {
        fprintf(stderr, "FAILPOINT %s: size mismatch at ordinal %llu\n",
                op, (unsigned long long)s->sticky_size_mismatch);
        dirty++;
    }
    return dirty;
}

/* -- live-map snapshot + oracles ------------------------------------- */

typedef struct fp_map_snap {
    fp_live_t entries[FP_TABLE_CAP];
    size_t    count;
    int64_t   live_bytes;
    int64_t   balance;
} fp_map_snap_t;

FP_UNUSED static void fp_map_capture(const fp_alloc_state_t *s,
                                     fp_map_snap_t *out)
{
    memcpy(out->entries, s->table, s->table_len * sizeof(fp_live_t));
    out->count = s->table_len;
    out->live_bytes = s->live_bytes;
    out->balance = s->balance;
}

/* Unordered-set equality of the CURRENT live map against a snapshot: same
 * pointers with the same sizes, nothing more, nothing less -- and the
 * aggregate counters (balance, live bytes) equal too, so a divergence
 * between the set and the counters is itself reported. Prints the first
 * divergence. Returns 0 when equal. */
FP_UNUSED static int fp_map_equals(const fp_alloc_state_t *s,
                                   const fp_map_snap_t *snap, const char *op)
{
    if (s->table_len != snap->count) {
        FP_DIAG( "FAILPOINT %s: live map count %zu, expected %zu\n",
                op, s->table_len, snap->count);
        return 1;
    }
    if (s->live_bytes != snap->live_bytes) {
        FP_DIAG( "FAILPOINT %s: live bytes %lld, expected %lld\n",
                op, (long long)s->live_bytes, (long long)snap->live_bytes);
        return 1;
    }
    if (s->balance != snap->balance) {
        FP_DIAG( "FAILPOINT %s: balance %lld, expected %lld\n",
                op, (long long)s->balance, (long long)snap->balance);
        return 1;
    }
    for (size_t i = 0; i < snap->count; i++) {
        int j = fp_table_find(s, snap->entries[i].ptr);
        if (j < 0 || s->table[j].size != snap->entries[i].size) {
            FP_DIAG( "FAILPOINT %s: live map lost or resized the "
                    "block of %zu bytes at entry %zu\n",
                    op, snap->entries[i].size, i);
            return 1;
        }
    }
    return 0;
}

/* Signed size-multiset delta between a snapshot and the CURRENT map:
 * added sizes (present now, not then), removed sizes (then, not now), the
 * count delta and the live-byte delta -- all pointer-free, so two runs of
 * the same operation can be compared even though their pointers differ. */
typedef struct fp_delta {
    size_t  added[FP_TABLE_CAP];
    size_t  added_n;
    size_t  removed[FP_TABLE_CAP];
    size_t  removed_n;
    int64_t count_delta;
    int64_t byte_delta;
} fp_delta_t;

FP_UNUSED static int fp_size_cmp(const void *a, const void *b)
{
    size_t x = *(const size_t *)a, y = *(const size_t *)b;
    return x < y ? -1 : x > y ? 1 : 0;
}

FP_UNUSED static void fp_delta_compute(const fp_alloc_state_t *s,
                                       const fp_map_snap_t *pre,
                                       fp_delta_t *out)
{
    size_t now[FP_TABLE_CAP], then[FP_TABLE_CAP];
    size_t nn = s->table_len, tn = pre->count;
    for (size_t i = 0; i < nn; i++) now[i] = s->table[i].size;
    for (size_t i = 0; i < tn; i++) then[i] = pre->entries[i].size;
    qsort(now, nn, sizeof(size_t), fp_size_cmp);
    qsort(then, tn, sizeof(size_t), fp_size_cmp);

    memset(out, 0, sizeof(*out));
    size_t i = 0, j = 0;
    while (i < nn || j < tn) {
        if (j == tn || (i < nn && now[i] < then[j]))
            out->added[out->added_n++] = now[i++];
        else if (i == nn || then[j] < now[i])
            out->removed[out->removed_n++] = then[j++];
        else { i++; j++; }
    }
    out->count_delta = (int64_t)nn - (int64_t)tn;
    out->byte_delta = s->live_bytes - pre->live_bytes;
}

/* Component-wise equality of two deltas. Returns 0 when equal. */
FP_UNUSED static int fp_delta_equals(const fp_delta_t *a, const fp_delta_t *b,
                                     const char *op)
{
    if (a->count_delta != b->count_delta || a->byte_delta != b->byte_delta ||
        a->added_n != b->added_n || a->removed_n != b->removed_n ||
        memcmp(a->added, b->added, a->added_n * sizeof(size_t)) != 0 ||
        memcmp(a->removed, b->removed, a->removed_n * sizeof(size_t)) != 0) {
        FP_DIAG( "FAILPOINT %s: allocation footprint differs from "
                "baseline (count %+lld vs %+lld, bytes %+lld vs %+lld, "
                "added %zu vs %zu, removed %zu vs %zu)\n", op,
                (long long)a->count_delta, (long long)b->count_delta,
                (long long)a->byte_delta, (long long)b->byte_delta,
                a->added_n, b->added_n, a->removed_n, b->removed_n);
        return 1;
    }
    return 0;
}

/* -- declared signatures ---------------------------------------------- */

typedef enum fp_size_matcher {
    FP_SIZE_ANY = 0,     /* size not knowable to the test (private type) */
    FP_SIZE_EXACT,       /* exactly `size` bytes */
    FP_SIZE_SAME_AS,     /* same size as the window attempt at `ref` (0-based) */
} fp_size_matcher_t;

typedef struct fp_expect {
    int    kind;         /* fp_attempt_kind_t */
    int    match;        /* fp_size_matcher_t */
    size_t size;         /* FP_SIZE_EXACT only */
    size_t ref;          /* FP_SIZE_SAME_AS only */
} fp_expect_t;

/* Check the operation window that began at log index `win` against a
 * declared signature: exact attempt count, per-attempt kind, and the size
 * matcher. Prints the first divergence. Returns the number of failures. */
FP_UNUSED static int fp_check_signature(const fp_alloc_state_t *s, size_t win,
                                        const fp_expect_t *exp, size_t n,
                                        const char *op)
{
    if (s->sticky_log_overflow) {
        FP_DIAG( "FAILPOINT %s: signature unverifiable, log overflow\n",
                op);
        return 1;
    }
    if (win > s->log_len) {
        FP_DIAG( "FAILPOINT %s: window start %zu past the log (%zu)\n",
                op, win, s->log_len);
        return 1;
    }
    if (s->log_len - win != n) {
        FP_DIAG( "FAILPOINT %s: %zu attempts in the window, "
                "declared %zu\n", op, s->log_len - win, n);
        return 1;
    }
    for (size_t i = 0; i < n; i++) {
        const fp_attempt_t *a = &s->log[win + i];
        if (a->kind != exp[i].kind) {
            FP_DIAG( "FAILPOINT %s: attempt %zu kind %d, declared %d\n",
                    op, i, a->kind, exp[i].kind);
            return 1;
        }
        if (exp[i].match == FP_SIZE_EXACT && a->size != exp[i].size) {
            FP_DIAG( "FAILPOINT %s: attempt %zu size %zu, declared "
                    "%zu\n", op, i, a->size, exp[i].size);
            return 1;
        }
        if (exp[i].match == FP_SIZE_SAME_AS && exp[i].ref >= i) {
            FP_DIAG( "FAILPOINT %s: declaration error -- attempt %zu "
                    "references attempt %zu, not an earlier one\n",
                    op, i, exp[i].ref);
            return 1;
        }
        if (exp[i].match == FP_SIZE_SAME_AS &&
            a->size != s->log[win + exp[i].ref].size) {
            FP_DIAG( "FAILPOINT %s: attempt %zu size %zu, declared "
                    "same as attempt %zu (%zu)\n", op, i, a->size,
                    exp[i].ref, s->log[win + exp[i].ref].size);
            return 1;
        }
    }
    return 0;
}

/* Log-prefix agreement between a failing run's window and the baseline
 * window: the first `k` attempts must match on kind and size, proving the
 * failing run took the same allocation path up to the injected failure.
 * Returns the number of failures. */
FP_UNUSED static int fp_check_prefix(const fp_alloc_state_t *s, size_t win,
                                     const fp_attempt_t *base, size_t k,
                                     const char *op)
{
    if (win > s->log_len) {
        FP_DIAG( "FAILPOINT %s: window start %zu past the log (%zu)\n",
                op, win, s->log_len);
        return 1;
    }
    if (s->log_len - win < k) {
        FP_DIAG( "FAILPOINT %s: only %zu attempts before the failure, "
                "expected %zu\n", op, s->log_len - win, k);
        return 1;
    }
    for (size_t i = 0; i < k; i++) {
        const fp_attempt_t *a = &s->log[win + i];
        if (a->kind != base[i].kind || a->size != base[i].size) {
            FP_DIAG( "FAILPOINT %s: prefix diverges at attempt %zu "
                    "(kind %d size %zu, baseline kind %d size %zu)\n",
                    op, i, a->kind, a->size, base[i].kind, base[i].size);
            return 1;
        }
    }
    return 0;
}

/* -- declared outcomes -------------------------------------------------
 * One row per attempt ordinal of an operation, machine-checked by the
 * sweep: the loop asserts the table length equals the declared attempt
 * count (an undeclared allocation origin is a test failure, never a
 * silently swept one), and each row selects the recovery the ordinal's
 * fixture must drive. Only the PRE_COMMIT phase is supported here; the
 * other phases arrive with their first consumer. */

typedef enum fp_phase {
    FP_PHASE_PRE_COMMIT = 1,
} fp_phase_t;

typedef enum fp_outcome {
    FP_NOMEM_RETAIN = 1,   /* owner + bytes retained; recovery = empty re-feed */
    FP_NOMEM_REDELIVER,    /* carrier retired; recovery = full re-delivery */
} fp_outcome_t;

typedef struct fp_outcome_row {
    int phase;             /* fp_phase_t */
    int outcome;           /* fp_outcome_t */
} fp_outcome_row_t;

/* Dump the current operation window -- for AUTHORING a declared signature
 * against the real allocation sequence, and for diagnosing a signature or
 * prefix divergence. */
FP_UNUSED static void fp_dump_window(const fp_alloc_state_t *s, const char *op)
{
    fprintf(stderr, "FAILPOINT %s window (%zu attempts):\n", op, s->log_len);
    for (size_t i = 0; i < s->log_len; i++)
        fprintf(stderr, "  [%zu] ordinal=%llu kind=%d size=%zu old=%zu\n",
                i, (unsigned long long)s->log[i].ordinal, s->log[i].kind,
                s->log[i].size, s->log[i].old_size);
}

/* One-line context, printed before a swept ordinal's assertions so a bare
 * FAIL: file:line is attributable. */
FP_UNUSED static void fp_context(const char *op, uint64_t k, uint64_t n,
                                 const fp_alloc_state_t *s)
{
    size_t size = 0;
    for (size_t i = 0; i < s->log_len; i++)
        if (s->log[i].ordinal == s->fail_at) size = s->log[i].size;
    fprintf(stderr, "FAILPOINT %s ordinal=%llu/%llu size=%zu\n", op,
            (unsigned long long)k, (unsigned long long)n, size);
}

#endif /* MOQ_TEST_FAILPOINT_H */
