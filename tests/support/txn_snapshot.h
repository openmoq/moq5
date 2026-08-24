#ifndef MOQ_TEST_TXN_SNAPSHOT_H
#define MOQ_TEST_TXN_SNAPSHOT_H

/*
 * Semantic transaction snapshots for allocation-failure sweeps.
 *
 * A snapshot is a value copy of the session facts a failed transaction must
 * leave untouched: session state, queue depths, the event-scratch cursor,
 * receive-budget and drain-ref accounting, and the ownership record of each
 * stream the fixture names (owner kind, slot, entry state, generation --
 * keyed through the request registry or the namespace-sub index). It holds
 * no pointers, no capacities, no borrow epoch, and is never a memcmp of
 * moq_session_t.
 *
 * The generic snapshot is necessary but NOT sufficient: every fixture
 * additionally declares a mutation inventory -- the durable fields its
 * operation can touch -- and registers operation-specific capture/check
 * hooks for the inventoried fields the generic struct does not cover
 * (request-ID counters, registry occupancy, buffered-input ownership,
 * send_len, auth state, ...). An empty hook must say WHY in the inventory.
 *
 * Normalized output records let a recovered run be compared against a
 * baseline run ORDERED and byte-for-byte: event/action spans are borrowed
 * only until the next advancing call, so each record deep-copies the
 * semantic fields (kind plus a caller-normalized byte image) into
 * libc-backed, caller-owned storage -- never the injected allocator, so
 * capture cost cannot perturb a sweep. Appends are fallible; a false
 * return is a test failure, never a silent drop.
 *
 * Comparison helpers return the number of failed checks after printing
 * each divergence, matching the file-local `failures +=` idiom.
 *
 * NOT installed. Requires the session internals; link
 * moq-core-test-internals.
 */

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../../core/src/session/session_internal.h"

#if defined(__GNUC__)
#define TXS_UNUSED __attribute__((unused))
#else
#define TXS_UNUSED
#endif

/* Set while a negative self-test asserts an EXPECTED divergence, so its
 * diagnostics do not read as failures in a passing run's output. */
TXS_UNUSED static int txs_quiet;

#define TXS_DIAG(...) do { if (!txs_quiet) fprintf(stderr, __VA_ARGS__); } while (0)

/* -- per-owner record ------------------------------------------------ */

/* Who owns one stream ref, and in what state. Namespace subscriptions key
 * their bidi in idx_ns_by_ref; every other family keys it in the request
 * registry. `kind` is negative for the index-keyed case so the two can
 * never compare equal by accident; TXS_OWNER_NONE records "no owner",
 * which is itself a comparable fact (a retired carrier). */
#define TXS_OWNER_NONE (-1)
#define TXS_OWNER_NS   (-2)

typedef struct txs_owner {
    int      kind;        /* moq_request_kind_t, TXS_OWNER_NS or _NONE */
    int      slot;
    int      state;
    uint32_t generation;
    int      invalid;     /* capture could not resolve this kind: the record
                           * is incomparable, and comparing FAILS -- two
                           * identical incomplete records must never pass */
} txs_owner_t;

TXS_UNUSED static txs_owner_t txs_owner_capture(const moq_session_t *s,
                                                moq_stream_ref_t ref)
{
    txs_owner_t o = { TXS_OWNER_NONE, -1, -1, 0, 0 };

    /* A slot arrives from an index, so it is only as trustworthy as that
     * index. Reading a stale or corrupt key out of bounds would be undefined
     * behaviour in the very harness meant to diagnose it, so an out-of-range
     * slot becomes a LOUD incomparable record instead. */
/* Parameters are deliberately not named `slot`/`kind`: the preprocessor
 * substitutes after `.` too, which would rewrite `o.slot` inside the body. */
#define TXS_SLOT_OK(slotval, capval) \
    ((slotval) >= 0 && (size_t)(slotval) < (capval))
#define TXS_BAD_SLOT(kindval, slotval, capval) do { \
        TXS_DIAG("TXN: owner capture slot %d out of range for kind %d" \
                " (capacity %zu)\n", (int)(slotval), (int)(kindval), \
                (size_t)(capval)); \
        o.kind = -2000 - (int)(kindval); \
        o.slot = (int)(slotval); \
        o.invalid = 1; \
        return o; \
    } while (0)

    int32_t ns = moq_index_find(s->idx_ns_by_ref, s->idx_ns_mask, ref._v);
    if (ns >= 0) {
        if (!TXS_SLOT_OK(ns, s->ns_sub_cap))
            TXS_BAD_SLOT(TXS_OWNER_NS, ns, s->ns_sub_cap);
        o.kind = TXS_OWNER_NS;
        o.slot = (int)ns;
        o.state = (int)s->ns_subs[ns].state;
        o.generation = s->ns_subs[ns].generation;
        return o;
    }

    moq_request_endpoint_t ep = request_registry_find_by_streamref(s, ref);
    if (ep.kind == MOQ_REQ_NONE) return o;
    o.kind = (int)ep.kind;
    o.slot = (int)ep.slot;
    {
        /* Supported-ness and capacity are tracked separately: a recognized
         * kind whose pool is EMPTY must still be bounds-checked, or a zero
         * capacity would wave every slot through to an indexed read. */
        size_t cap = 0;
        bool supported = true;
        switch (ep.kind) {
        case MOQ_REQ_SUBSCRIPTION:     cap = s->sub_cap; break;
        case MOQ_REQ_FETCH:            cap = s->fetch_cap; break;
        case MOQ_REQ_TRACK_STATUS:     cap = s->ts_cap; break;
        case MOQ_REQ_ANNOUNCEMENT:     cap = s->ann_cap; break;
        case MOQ_REQ_PUBLISH:          cap = s->pub_cap; break;
        case MOQ_REQ_SUBSCRIBE_TRACKS: cap = s->track_sub_cap; break;
        default: supported = false; break;
        }
        if (supported && !TXS_SLOT_OK(ep.slot, cap))
            TXS_BAD_SLOT(ep.kind, ep.slot, cap);
    }
    switch (ep.kind) {
    case MOQ_REQ_SUBSCRIPTION:
        o.state = (int)s->subs[ep.slot].state;
        o.generation = s->subs[ep.slot].generation;
        break;
    case MOQ_REQ_FETCH:
        o.state = (int)s->fetches[ep.slot].state;
        o.generation = s->fetches[ep.slot].generation;
        break;
    case MOQ_REQ_TRACK_STATUS:
        o.state = (int)s->track_statuses[ep.slot].state;
        o.generation = s->track_statuses[ep.slot].generation;
        break;
    case MOQ_REQ_ANNOUNCEMENT:
        o.state = (int)s->announcements[ep.slot].state;
        o.generation = s->announcements[ep.slot].generation;
        break;
    case MOQ_REQ_PUBLISH:
        o.state = (int)s->publishes[ep.slot].state;
        o.generation = s->publishes[ep.slot].generation;
        break;
    case MOQ_REQ_SUBSCRIBE_TRACKS:
        o.state = (int)s->track_subs[ep.slot].state;
        o.generation = s->track_subs[ep.slot].generation;
        break;
    default:
        /* A kind this helper does not resolve must be LOUD, and its record
         * distinct from every complete one -- two incomplete records of
         * the same kind would otherwise compare equal and mean nothing. */
        TXS_DIAG("TXN: owner capture cannot resolve request kind %d"
                " -- extend txs_owner_capture before relying on it\n",
                (int)ep.kind);
        o.kind = -1000 - (int)ep.kind;
        o.invalid = 1;
        break;
    }
#undef TXS_SLOT_OK
#undef TXS_BAD_SLOT
    return o;
}

TXS_UNUSED static int txs_owner_equals(const txs_owner_t *a,
                                       const txs_owner_t *b, const char *op)
{
    if (a->invalid || b->invalid) {
        TXS_DIAG("TXN %s: incomparable owner record (unresolved kind)\n",
                 op);
        return 1;
    }
    if (a->kind != b->kind || a->slot != b->slot || a->state != b->state ||
        a->generation != b->generation) {
        TXS_DIAG("TXN %s: owner (kind %d slot %d state %d gen %u) "
                "expected (kind %d slot %d state %d gen %u)\n", op,
                a->kind, a->slot, a->state, a->generation,
                b->kind, b->slot, b->state, b->generation);
        return 1;
    }
    return 0;
}

/* -- generic snapshot ------------------------------------------------- */

#define TXS_MAX_OWNERS 4

typedef struct txs_snapshot {
    int      session_state;
    size_t   event_depth;         /* event_tail - event_head */
    size_t   action_depth;        /* action_tail - action_head */
    size_t   event_scratch_len;
    size_t   recv_payload_bytes;
    size_t   drain_ref_count;
    size_t   owner_count;
    int      owner_overflow;      /* more refs requested than tracked */
    txs_owner_t owners[TXS_MAX_OWNERS];
} txs_snapshot_t;

TXS_UNUSED static void txs_capture(const moq_session_t *s,
                                   const moq_stream_ref_t *refs, size_t n,
                                   txs_snapshot_t *out)
{
    memset(out, 0, sizeof(*out));
    out->session_state = (int)s->state;
    out->event_depth = s->event_tail - s->event_head;
    out->action_depth = s->action_tail - s->action_head;
    out->event_scratch_len = s->event_scratch_len;
    out->recv_payload_bytes = s->recv_payload_bytes;
    out->drain_ref_count = s->drain_ref_count;
    /* Overflow is EXPLICIT, never a silent truncation: the check side fails
     * an overflowed snapshot. */
    out->owner_overflow = n > TXS_MAX_OWNERS;
    out->owner_count = out->owner_overflow ? TXS_MAX_OWNERS : n;
    for (size_t i = 0; i < out->owner_count; i++)
        out->owners[i] = txs_owner_capture(s, refs[i]);
}

/* Field-by-field equality of the CURRENT session facts against a snapshot,
 * naming each differing field. Returns the number of failed checks. */
TXS_UNUSED static int txs_check_eq(const moq_session_t *s,
                                   const moq_stream_ref_t *refs, size_t n,
                                   const txs_snapshot_t *want, const char *op)
{
    int bad = 0;
    txs_snapshot_t now;
    txs_capture(s, refs, n, &now);
    if (now.owner_overflow || want->owner_overflow) {
        TXS_DIAG( "TXN %s: owner snapshot overflowed (max %d)\n",
                op, TXS_MAX_OWNERS);
        bad++;
    }
    if (now.owner_count != want->owner_count) {
        TXS_DIAG( "TXN %s: %zu owner records, expected %zu\n",
                op, now.owner_count, want->owner_count);
        bad++;
    }

#define TXS_FIELD(f, fmt) do { \
        if (now.f != want->f) { \
            TXS_DIAG("TXN %s: " #f " " fmt ", expected " fmt "\n", \
                    op, now.f, want->f); \
            bad++; \
        } \
    } while (0)
    TXS_FIELD(session_state, "%d");
    TXS_FIELD(event_depth, "%zu");
    TXS_FIELD(action_depth, "%zu");
    TXS_FIELD(event_scratch_len, "%zu");
    TXS_FIELD(recv_payload_bytes, "%zu");
    TXS_FIELD(drain_ref_count, "%zu");
#undef TXS_FIELD
    for (size_t i = 0; i < now.owner_count && i < want->owner_count; i++)
        bad += txs_owner_equals(&now.owners[i], &want->owners[i], op);
    return bad;
}

/* -- normalized output records ---------------------------------------- */

typedef struct txs_norm_rec {
    uint64_t kind;       /* event or action kind */
    uint8_t *bytes;      /* libc-owned normalized image; may be NULL */
    size_t   len;
} txs_norm_rec_t;

#define TXS_NORM_CAP 32

typedef struct txs_norm_vec {
    txs_norm_rec_t recs[TXS_NORM_CAP];
    size_t         count;
    int            overflowed;
} txs_norm_vec_t;

TXS_UNUSED static void txs_norm_init(txs_norm_vec_t *v)
{
    memset(v, 0, sizeof(*v));
}

TXS_UNUSED static void txs_norm_free(txs_norm_vec_t *v)
{
    for (size_t i = 0; i < v->count; i++) free(v->recs[i].bytes);
    memset(v, 0, sizeof(*v));
}

/* Append one record, deep-copying `bytes` with libc. Returns false on
 * capacity or copy failure -- the caller must treat that as a test
 * failure, never a silent drop. */
TXS_UNUSED static bool txs_norm_append(txs_norm_vec_t *v, uint64_t kind,
                                       const uint8_t *bytes, size_t len)
{
    if (v->count == TXS_NORM_CAP) { v->overflowed = 1; return false; }
    txs_norm_rec_t *r = &v->recs[v->count];
    r->kind = kind;
    r->len = len;
    r->bytes = NULL;
    if (len > 0) {
        if (!bytes) {
            TXS_DIAG("TXN: record kind %llu declares %zu bytes with a NULL "
                     "pointer\n", (unsigned long long)kind, len);
            return false;
        }
        r->bytes = (uint8_t *)malloc(len);
        if (!r->bytes) return false;
        memcpy(r->bytes, bytes, len);
    }
    v->count++;
    return true;
}

/* Record-for-record equality, naming the first differing record and how it
 * differs. Returns the number of failed checks (0 or 1). */
TXS_UNUSED static int txs_norm_equals(const txs_norm_vec_t *a,
                                      const txs_norm_vec_t *b, const char *op)
{
    if (a->overflowed || b->overflowed) {
        TXS_DIAG( "TXN %s: normalized vector overflowed\n", op);
        return 1;
    }
    if (a->count != b->count) {
        TXS_DIAG( "TXN %s: %zu output records, expected %zu\n",
                op, a->count, b->count);
        return 1;
    }
    for (size_t i = 0; i < a->count; i++) {
        if (a->recs[i].kind != b->recs[i].kind) {
            TXS_DIAG( "TXN %s: record %zu kind %llu, expected %llu\n",
                    op, i, (unsigned long long)a->recs[i].kind,
                    (unsigned long long)b->recs[i].kind);
            return 1;
        }
        if (a->recs[i].len != b->recs[i].len ||
            (a->recs[i].len > 0 &&
             memcmp(a->recs[i].bytes, b->recs[i].bytes, a->recs[i].len) != 0)) {
            TXS_DIAG( "TXN %s: record %zu payload differs "
                    "(%zu vs %zu bytes)\n", op, i,
                    a->recs[i].len, b->recs[i].len);
            return 1;
        }
    }
    return 0;
}

/* A bounded byte image for building one normalized record from a record's
 * semantic fields: u64 scalars and length-prefixed byte ranges, appended
 * in declaration order. Overflow marks the image; appending an overflowed
 * image fails, never truncates silently. */
typedef struct txs_img {
    uint8_t buf[512];
    size_t  len;
    int     overflow;
    /* Sticky: a non-empty span was offered with a NULL pointer. The product
     * does not emit that for a valid field, so it means malformed harness or
     * decoder input -- which must produce a NAMED diagnostic, never an
     * out-of-bounds read. Kept separate from `overflow` so the two cannot be
     * confused in a failure report. */
    int     null_span;
    /* Sticky and DISTINCT from null_span: a container count was malformed or
     * beyond its declared bound. An over-bound count is not a NULL pointer, and
     * reporting it as one would misname the fault for any caller that ignored
     * the helper's return value. */
    int     bad_count;
} txs_img_t;

TXS_UNUSED static void txs_img_init(txs_img_t *im) { memset(im, 0, sizeof(*im)); }

TXS_UNUSED static void txs_img_u64(txs_img_t *im, uint64_t v)
{
    if (im->len + 8 > sizeof(im->buf)) { im->overflow = 1; return; }
    memcpy(im->buf + im->len, &v, 8);
    im->len += 8;
}

TXS_UNUSED static void txs_img_bytes(txs_img_t *im, const uint8_t *b,
                                     size_t n)
{
    txs_img_u64(im, (uint64_t)n);
    if (im->overflow) return;
    /* Length first, then the pointer, THEN the copy: a non-empty span with a
     * NULL pointer is latched and skipped rather than dereferenced. */
    if (n > sizeof(im->buf) - im->len) { im->overflow = 1; return; }
    if (n > 0 && !b) { im->null_span = 1; return; }
    if (n > 0) memcpy(im->buf + im->len, b, n);
    im->len += n;
}

/* Direct-assertion guard (#279). The count-assertion macros do NOT
 * short-circuit, so an index after a failed count check still runs. This
 * validates count, index, container pointer and leaf pointer IN THAT ORDER,
 * fails BY NAME, and only then compares -- and it never silently skips the
 * comparison when the pointer is NULL, which would let a missing span pass. */
TXS_UNUSED static int txs_check_part_bytes(const moq_namespace_t *ns,
                                           size_t idx, const void *want,
                                           size_t want_len, const char *what)
{
    if (!ns) { TXS_DIAG("TXN %s: NULL namespace\n", what); return 1; }
    if (idx >= ns->count) {
        TXS_DIAG("TXN %s: part %zu out of range (count %zu)\n", what, idx,
                 ns->count);
        return 1;
    }
    if (!ns->parts) {
        TXS_DIAG("TXN %s: count %zu with a NULL parts array\n", what,
                 ns->count);
        return 1;
    }
    if (ns->parts[idx].len != want_len) {
        TXS_DIAG("TXN %s: part %zu length %zu, expected %zu\n", what, idx,
                 ns->parts[idx].len, want_len);
        return 1;
    }
    if (want_len > 0 && !ns->parts[idx].data) {
        TXS_DIAG("TXN %s: part %zu declares %zu bytes with NULL data\n", what,
                 idx, want_len);
        return 1;
    }
    if (want_len > 0 && memcmp(ns->parts[idx].data, want, want_len) != 0) {
        TXS_DIAG("TXN %s: part %zu bytes differ\n", what, idx);
        return 1;
    }
    return 0;
}

/* Direct raw-span check (#279). The `if (len == want && ptr) memcmp(...)`
 * shape silently DROPS a declared comparison when the pointer is NULL, so a
 * missing span passes. This validates the exact length, then REQUIRES the
 * pointer, then compares -- failing by name at each step and never
 * dereferencing after a failed guard (`MOQ_TEST_CHECK` does not
 * short-circuit). */
TXS_UNUSED static int txs_check_span_bytes(const void *got, size_t got_len,
                                           const void *want, size_t want_len,
                                           const char *what)
{
    if (got_len != want_len) {
        TXS_DIAG("TXN %s: span length %zu, expected %zu\n", what, got_len,
                 want_len);
        return 1;
    }
    if (want_len > 0 && !got) {
        TXS_DIAG("TXN %s: declares %zu bytes with a NULL pointer\n", what,
                 want_len);
        return 1;
    }
    if (want_len > 0 && memcmp(got, want, want_len) != 0) {
        TXS_DIAG("TXN %s: span bytes differ\n", what);
        return 1;
    }
    return 0;
}

/* -- borrowed CONTAINER guards (#279) ---------------------------------- *
 * `txs_img_bytes()` can guard a leaf `moq_bytes_t.data`, but it is reached only
 * AFTER the array has been indexed -- so it cannot protect the array itself. A
 * nonzero count with a NULL container, or an implausible count over a small
 * backing array, walks out of bounds before any image overflow could latch.
 * These helpers enforce the order the sweep requires: bound the count, then
 * require the pointer, THEN index.
 *
 * On failure they latch the APPROPRIATE distinct sticky state as well as
 * returning false -- `bad_count` for a malformed or over-bound count,
 * `null_span` for a nonempty NULL container or leaf -- so a caller that ignores
 * the result still cannot build a silently short image, and cannot have the
 * fault misnamed either. */
TXS_UNUSED static bool txs_img_parts(txs_img_t *im, const char *what,
                                     const moq_bytes_t *arr, size_t count,
                                     size_t max_count)
{
    txs_img_u64(im, (uint64_t)count);
    if (count > max_count) {
        TXS_DIAG("TXN %s: namespace part count %zu exceeds the bound %zu\n",
                 what, count, max_count);
        im->bad_count = 1;
        return false;
    }
    if (count > 0 && !arr) {
        TXS_DIAG("TXN %s: namespace part count %zu with a NULL parts array\n",
                 what, count);
        im->null_span = 1;
        return false;
    }
    for (size_t i = 0; i < count; i++) {
        if (arr[i].len && !arr[i].data) {
            TXS_DIAG("TXN %s: namespace part %zu declares %zu bytes with NULL "
                     "data\n", what, i, arr[i].len);
            im->null_span = 1;
            return false;
        }
        txs_img_bytes(im, arr[i].data, arr[i].len);
    }
    return true;
}

TXS_UNUSED static bool txs_img_tokens(txs_img_t *im, const char *what,
                                      const moq_resolved_token_t *toks,
                                      size_t count, size_t max_count)
{
    txs_img_u64(im, (uint64_t)count);
    if (count > max_count) {
        TXS_DIAG("TXN %s: token count %zu exceeds the bound %zu\n", what,
                 count, max_count);
        im->bad_count = 1;
        return false;
    }
    if (count > 0 && !toks) {
        TXS_DIAG("TXN %s: token count %zu with a NULL tokens array\n", what,
                 count);
        im->null_span = 1;
        return false;
    }
    for (size_t i = 0; i < count; i++) {
        txs_img_u64(im, toks[i].token_type);
        if (toks[i].token_value.len && !toks[i].token_value.data) {
            TXS_DIAG("TXN %s: token %zu declares %zu value bytes with NULL "
                     "data\n", what, i, toks[i].token_value.len);
            im->null_span = 1;
            return false;
        }
        txs_img_bytes(im, toks[i].token_value.data, toks[i].token_value.len);
    }
    return true;
}

/* Permanent self-check: the two container failure classes must stay
 * distinguishable in BOTH helpers -- a regression confined to either token
 * branch would otherwise stay green. Returns the number of failures.
 *
 * Genuinely quiet: it deliberately provokes the helpers, so it saves the
 * caller's `txs_quiet`, silences the expected negative probes, and restores the
 * saved value on every exit. Expected probe output must never be mistaken for a
 * real diagnostic. */
TXS_UNUSED static int txs_selfcheck_img_states(void)
{
    const int saved_quiet = txs_quiet;
    txs_quiet = 1;
    int bad = 0;
    const moq_bytes_t one = { (const uint8_t *)"x", 1 };
    /* A VALID local token for the over-bound case: the count guard must fire
     * first, so this is never indexed. */
    const moq_resolved_token_t tok = { 0, { (const uint8_t *)"t", 1 } };

    txs_img_t a; txs_img_init(&a);
    if (txs_img_parts(&a, "selfcheck", &one, 99, 32)) bad++;   /* over bound */
    if (!a.bad_count || a.null_span) bad++;

    txs_img_t b; txs_img_init(&b);
    if (txs_img_parts(&b, "selfcheck", NULL, 2, 32)) bad++;    /* NULL array */
    if (!b.null_span || b.bad_count) bad++;

    txs_img_t c; txs_img_init(&c);
    if (txs_img_tokens(&c, "selfcheck", &tok, 99, 16)) bad++;  /* over bound */
    if (!c.bad_count || c.null_span) bad++;

    txs_img_t d; txs_img_init(&d);
    if (txs_img_tokens(&d, "selfcheck", NULL, 2, 16)) bad++;   /* NULL array */
    if (!d.null_span || d.bad_count) bad++;

    txs_quiet = saved_quiet;
    return bad;
}

TXS_UNUSED static bool txs_norm_append_img(txs_norm_vec_t *v, uint64_t kind,
                                           const txs_img_t *im)
{
    if (im->bad_count) {
        TXS_DIAG("TXN: record kind %llu carries a malformed or over-bound "
                 "container count; refusing to build the image\n",
                 (unsigned long long)kind);
        return false;
    }
    if (im->null_span) {
        TXS_DIAG("TXN: record kind %llu carries a NON-EMPTY span with a NULL "
                 "pointer; refusing to build the image\n",
                 (unsigned long long)kind);
        return false;
    }
    if (im->overflow) {
        TXS_DIAG("TXN: record kind %llu overflowed the image buffer\n",
                 (unsigned long long)kind);
        return false;
    }
    return txs_norm_append(v, kind, im->buf, im->len);
}

/* -- operation hooks --------------------------------------------------- */

/* Operation-specific snapshot/check callbacks covering the fixture's
 * mutation-inventory fields that the generic snapshot does not, plus the
 * output normalizers. Capture storage is caller-owned per call (a fixture
 * legitimately holds SEVERAL captures at once -- pre-op, baseline-final,
 * pre-head -- so the instance deliberately owns none); the harness never
 * allocates hook state and hook storage never touches the injected
 * allocator. `check` compares within one run (pointer identities
 * included); `check_values` compares across runs (values only). A fixture
 * registering an empty hook must justify it in its mutation inventory. */
typedef struct txs_op_hooks {
    void  *ctx;          /* fixture-owned: refs, slots, expected lengths */
    void (*capture)(const moq_session_t *s, void *ctx, void *state);
    /* each returns the number of failed checks */
    int  (*check)(const moq_session_t *s, void *ctx, const void *state);
    int  (*check_values)(const moq_session_t *s, void *ctx,
                         const void *state);
    /* deep-copy one polled event / drained action; false = test failure */
    bool (*normalize_event)(const moq_event_t *ev, void *ctx,
                            txs_norm_vec_t *out);
    bool (*normalize_action)(const moq_action_t *a, void *ctx,
                             txs_norm_vec_t *out);
} txs_op_hooks_t;

#endif /* MOQ_TEST_TXN_SNAPSHOT_H */
