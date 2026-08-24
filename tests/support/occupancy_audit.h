#ifndef MOQ_TEST_OCCUPANCY_AUDIT_H
#define MOQ_TEST_OCCUPANCY_AUDIT_H

/*
 * Bounded, test-only auditor for the pools' intrusive OCCUPANCY lists.
 *
 * The lists are what every advancing-preamble scan walks, so their topology is
 * a real invariant and not incidental bookkeeping. A membership bit alone
 * proves almost nothing: it cannot see an unreachable linked entry, a wrong
 * head, broken reciprocity, wrong ordering, a duplicate or a cycle, or an edge
 * pointing outside the pool. This walks the whole structure and names the first
 * fault it finds.
 *
 * FAIL CLOSED: every index is range-checked BEFORE it is used to subscript, and
 * the walk is bounded by the pool capacity, so a corrupt list is a reported
 * fault rather than an out-of-range read or a hang.
 *
 * The checker returns a code and touches no test macros, so a self-check can
 * probe it QUIETLY; the reporting wrapper is separate.
 *
 * NOT installed. Requires the session internals; link moq-core-test-internals.
 */

#include "../../core/src/session/session_internal.h"
#include <stddef.h>

typedef enum {
    OCC_OK = 0,
    OCC_NULL_SESSION,      /* nothing to audit: the session pointer is NULL */
    OCC_BAD_HEAD,          /* head neither -1 nor a valid slot */
    OCC_BAD_NEXT,          /* a next edge points outside the pool */
    OCC_BAD_PREV,          /* a prev edge points outside the pool */
    OCC_HEAD_PREV,         /* the head's prev is not -1 */
    OCC_NOT_ASCENDING,     /* order is not strictly ascending by slot */
    OCC_TOO_LONG,          /* more steps than the pool has slots: cycle/dup */
    OCC_NO_RECIPROCITY,    /* next(prev(x)) != x */
    OCC_REACHED_FREE,      /* a reached entry is not allocated */
    OCC_REACHED_UNFLAGGED, /* a reached entry is not marked linked */
    OCC_ALLOC_UNLINKED,    /* an allocated slot is not a member */
    OCC_FREE_LINKED,       /* a free slot is marked linked */
    OCC_FREE_RESIDUE,      /* a free slot's edges are not the -1 sentinel */
    OCC_COUNT_MISMATCH,    /* members reached != allocated slots */
} occ_fault_t;

static inline const char *occ_fault_name(occ_fault_t f)
{
    switch (f) {
    case OCC_OK:                return "ok";
    case OCC_NULL_SESSION:      return "session pointer is NULL";
    case OCC_BAD_HEAD:          return "head out of range";
    case OCC_BAD_NEXT:          return "next edge out of range";
    case OCC_BAD_PREV:          return "prev edge out of range";
    case OCC_HEAD_PREV:         return "head's prev is not -1";
    case OCC_NOT_ASCENDING:     return "not strictly ascending by slot";
    case OCC_TOO_LONG:          return "walk exceeded the pool: cycle or duplicate";
    case OCC_NO_RECIPROCITY:    return "prev/next are not reciprocal";
    case OCC_REACHED_FREE:      return "a reached entry is not allocated";
    case OCC_REACHED_UNFLAGGED: return "a reached entry is not flagged linked";
    case OCC_ALLOC_UNLINKED:    return "an allocated slot is not linked";
    case OCC_FREE_LINKED:       return "a free slot is flagged linked";
    case OCC_FREE_RESIDUE:      return "a free slot's edges are not -1/-1";
    case OCC_COUNT_MISMATCH:    return "members reached != allocated slots";
    }
    return "?";
}

/*
 * One checker per pool. `ALLOCATED` is the pool's own allocation predicate --
 * membership is defined as "allocated", so the two must agree everywhere.
 * `*out_slot` names the offending slot when a fault is returned.
 */
#define MOQ_DEFINE_OCC_CHECK(NAME, ARR, CAP, HEAD, ALLOC_EXPR)                 \
static inline occ_fault_t occ_check_##NAME(const moq_session_t *s,            \
                                           size_t *out_slot)                  \
{                                                                             \
    if (out_slot) *out_slot = (size_t)-1;                                     \
    /* Fail closed BEFORE the first dereference: an oracle advertised as       \
     * bounds-safe must not turn a failed session creation into a crash. */    \
    if (!s) return OCC_NULL_SESSION;                                          \
    size_t cap = s->CAP;                                                      \
    int32_t head = s->HEAD;                                                   \
    if (head < -1 || (head >= 0 && (size_t)head >= cap)) {                    \
        if (out_slot) *out_slot = (size_t)(head < 0 ? 0 : head);              \
        return OCC_BAD_HEAD;                                                  \
    }                                                                         \
    size_t reached = 0;                                                       \
    int32_t prev_seen = -1;                                                   \
    for (int32_t it = head; it >= 0; ) {                                      \
        if ((size_t)it >= cap) { if (out_slot) *out_slot = (size_t)it;        \
                                 return OCC_BAD_NEXT; }                       \
        if (out_slot) *out_slot = (size_t)it;                                 \
        if (++reached > cap) return OCC_TOO_LONG;                             \
        int32_t pv = s->ARR[it].occ_prev;                                     \
        if (pv < -1 || (pv >= 0 && (size_t)pv >= cap)) return OCC_BAD_PREV;   \
        if (pv != prev_seen) return (prev_seen < 0) ? OCC_HEAD_PREV           \
                                                    : OCC_NO_RECIPROCITY;     \
        if (prev_seen >= 0 && it <= prev_seen) return OCC_NOT_ASCENDING;      \
        if (!(s->ARR[it].ALLOC_EXPR)) return OCC_REACHED_FREE;                            \
        if (!s->ARR[it].occ_linked) return OCC_REACHED_UNFLAGGED;             \
        int32_t nx = s->ARR[it].occ_next;                                     \
        if (nx < -1 || (nx >= 0 && (size_t)nx >= cap)) return OCC_BAD_NEXT;   \
        prev_seen = it;                                                       \
        it = nx;                                                              \
    }                                                                         \
    /* Every allocated slot must be a member; every free slot must not be,    \
     * and must carry the declared unlinked sentinel image. */                \
    size_t allocated = 0;                                                     \
    for (size_t it = 0; it < cap; it++) {                                     \
        if (out_slot) *out_slot = it;                                         \
        if (s->ARR[it].ALLOC_EXPR) {                                                      \
            allocated++;                                                      \
            if (!s->ARR[it].occ_linked) return OCC_ALLOC_UNLINKED;            \
        } else {                                                              \
            if (s->ARR[it].occ_linked) return OCC_FREE_LINKED;                \
            if (s->ARR[it].occ_next != -1 || s->ARR[it].occ_prev != -1)       \
                return OCC_FREE_RESIDUE;                                      \
        }                                                                     \
    }                                                                         \
    if (out_slot) *out_slot = (size_t)-1;                                     \
    /* Strict ascent already forbids revisiting a slot, so equal counts close  \
     * the "reachable exactly once" obligation. */                            \
    if (reached != allocated) return OCC_COUNT_MISMATCH;                      \
    return OCC_OK;                                                            \
}

/* ALLOC_EXPR is a MEMBER expression appended to the entry: membership is
 * defined as "allocated", so the two must agree at every slot. */
MOQ_DEFINE_OCC_CHECK(pub,   publishes,  pub_cap,   pub_occ_head,
                     state != MOQ_PUB_FREE)
MOQ_DEFINE_OCC_CHECK(sub,   subs,       sub_cap,   sub_occ_head,
                     state != MOQ_SUB_FREE)
MOQ_DEFINE_OCC_CHECK(sg,    subgroups,  sg_cap,    sg_occ_head,
                     state != MOQ_SG_FREE)
MOQ_DEFINE_OCC_CHECK(fetch, fetches,    fetch_cap, fetch_occ_head,
                     state != MOQ_FETCH_FREE)
MOQ_DEFINE_OCC_CHECK(rx,    rx_streams, rx_cap,    rx_occ_head,
                     active)

#undef MOQ_DEFINE_OCC_CHECK

/* Quiet: the first faulting pool, or OCC_OK. */
static inline occ_fault_t occ_check_all(const moq_session_t *s,
                                        const char **out_pool, size_t *out_slot)
{
    occ_fault_t f;
    if (out_pool) *out_pool = "";
    if (out_slot) *out_slot = (size_t)-1;
    if (!s) { if (out_pool) *out_pool = "-"; return OCC_NULL_SESSION; }
    if ((f = occ_check_pub(s, out_slot)) != OCC_OK)
        { if (out_pool) *out_pool = "pub";   return f; }
    if ((f = occ_check_sub(s, out_slot)) != OCC_OK)
        { if (out_pool) *out_pool = "sub";   return f; }
    if ((f = occ_check_sg(s, out_slot)) != OCC_OK)
        { if (out_pool) *out_pool = "sg";    return f; }
    if ((f = occ_check_rx(s, out_slot)) != OCC_OK)
        { if (out_pool) *out_pool = "rx";    return f; }
    if ((f = occ_check_fetch(s, out_slot)) != OCC_OK)
        { if (out_pool) *out_pool = "fetch"; return f; }
    return OCC_OK;
}

#endif /* MOQ_TEST_OCCUPANCY_AUDIT_H */
