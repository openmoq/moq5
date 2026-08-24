#ifndef MOQ_TEST_OWNERSHIP_GRAPH_H
#define MOQ_TEST_OWNERSHIP_GRAPH_H

/*
 * Ownership-graph auditor: the raw index -> owner edges of a session, checked
 * for absolute referential integrity and compared against fixture-declared
 * topology.
 *
 * Three indexes bind a key to a pool slot: idx_req_by_rid (request id ->
 * packed request endpoint), idx_req_by_streamref (request bidi stream ref ->
 * packed request endpoint), and idx_ns_by_ref (namespace-sub bidi stream ref
 * -> ns_subs slot). Capture walks each TABLE, not the lookup helpers, so an
 * edge that a probe-chain lookup would never reach is still seen -- duplicate
 * keys, edges to freed slots, and edges left behind by a handoff all show up
 * as edges rather than as absences.
 *
 * Every edge is decoded and BOUNDS-CHECKED against its pool before anything is
 * dereferenced: a corrupt or out-of-range target becomes a loud invalid record,
 * never an out-of-bounds read inside the harness meant to diagnose it.
 *
 * Walking the table is necessary but not sufficient. Integrity therefore
 * RE-PROBES every captured edge with the production lookup and requires it to
 * land on that entry: an edge stranded behind a hole (a broken backshift) is
 * invisible to every real lookup while a raw scan still sees it, so the two
 * views must agree. A missing table is likewise not an empty one -- it makes
 * the graph incomparable, because every "no such edge" answer drawn from it
 * would be unfounded.
 *
 * Expectations are ABSOLUTE, declared by the fixture: required edges, forbidden
 * edges, and an owner's EXACT edge set. Capture-before / compare-after is not
 * enough on its own -- a wrong baseline would validate itself -- so a fixture
 * states the topology it believes correct and the auditor checks it.
 *
 * Normalized comparison renders one owner's closure with its ids, refs, slot
 * and handle replaced by symbols assigned in a fixed traversal order, so two
 * structurally equivalent owners compare equal regardless of which ordinals
 * they happened to receive. That is what makes ordinal equivariance testable:
 * an owner on request id 0 and an owner on request id 2 must normalize equal.
 *
 * SCOPE OF THE KEY <-> OWNER IDENTITY CHECK. A namespace subscription's
 * ownership policy is unambiguous in source: its commit installs a request-id
 * edge keyed on the entry's own request_id (session_namespace_sub.c:930) and
 * its bidi is keyed in idx_ns_by_ref on the entry's own stream_ref, and it
 * never registers in idx_req_by_streamref. So its keys are matched against the
 * owner's declared identity. For every OTHER family the auditor deliberately
 * checks structure only -- supported kind, in-range slot, live target, no
 * duplicate key or edge -- and does NOT invent a key-matching rule; those
 * families' policies (which of request_id / update_request_id owns a key, and
 * when an entry deliberately holds none) are not settled here. Extending a
 * family means recording its policy first, not widening this header quietly.
 *
 * All comparison helpers return the number of FAILED checks after printing each
 * divergence, matching the file-local `failures +=` idiom. Overflow and
 * unresolvable records are STICKY and make the graph INCOMPARABLE: every check
 * against it fails loudly rather than passing on a truncated picture.
 *
 * Fixed capacity, no allocation, no production allocator, nothing exported.
 * NOT installed. Requires the session internals; link moq-core-test-internals.
 */

#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include "../../core/src/session/session_internal.h"

#if defined(__GNUC__)
#define OG_UNUSED __attribute__((unused))
#else
#define OG_UNUSED
#endif

/* Set while a negative self-test asserts an EXPECTED divergence, so its
 * diagnostics do not read as failures in a passing run's output. */
OG_UNUSED static int og_quiet;

#define OG_DIAG(...) do { if (!og_quiet) fprintf(stderr, __VA_ARGS__); } while (0)

/* -- domains ---------------------------------------------------------- */

typedef enum og_domain {
    OG_DOM_REQ_RID       = 0,   /* idx_req_by_rid       : request id  -> packed */
    OG_DOM_REQ_STREAMREF = 1,   /* idx_req_by_streamref : stream ref  -> packed */
    OG_DOM_NS_REF        = 2,   /* idx_ns_by_ref        : stream ref  -> ns slot */
    OG_DOM_COUNT         = 3
} og_domain_t;

OG_UNUSED static const char *og_domain_name(og_domain_t d)
{
    switch (d) {
    case OG_DOM_REQ_RID:       return "req_by_rid";
    case OG_DOM_REQ_STREAMREF: return "req_by_streamref";
    case OG_DOM_NS_REF:        return "ns_by_ref";
    default:                   return "?";
    }
}

/* -- capacities ------------------------------------------------------- */

#define OG_MAX_EDGES  32
#define OG_MAX_OWNERS 16

/* -- one edge --------------------------------------------------------- */

typedef struct og_edge {
    og_domain_t domain;
    uint64_t    key;
    int         kind;        /* moq_request_kind_t of the target pool */
    int         slot;        /* pool slot */
    int32_t     raw;         /* the value the table holds for this key */
    size_t      table_index; /* raw probe position, for diagnostics */
    int         decodable;   /* 0: kind unsupported or slot out of range --
                              * the target was NEVER dereferenced */
} og_edge_t;

/* -- one owner -------------------------------------------------------- */

typedef struct og_owner {
    int      kind;             /* moq_request_kind_t */
    int      slot;
    int      live;             /* pool state != FREE */
    int      state;
    uint32_t generation;
    int      identity_known;   /* the family's key-ownership policy is modelled;
                                * see the scope note at the top of this file */
    int      has_request_id;
    uint64_t request_id;
    int      has_stream_ref;
    uint64_t stream_ref;
    int      has_handle;
    uint64_t handle;
    int      invalid;          /* unresolvable: incomparable, fails loudly */
} og_owner_t;

/* -- the graph -------------------------------------------------------- */

typedef struct og_graph {
    og_edge_t  edges[OG_MAX_EDGES];
    size_t     edge_count;
    og_owner_t owners[OG_MAX_OWNERS];
    size_t     owner_count;
    /* The tables themselves, so integrity can re-probe each captured edge the
     * way production looks it up (see the reachability check). */
    const moq_index_entry_t *tbl[OG_DOM_COUNT];
    size_t     mask[OG_DOM_COUNT];
    int        overflow;   /* sticky: the picture is truncated */
    int        invalid;    /* sticky: an edge or owner could not be resolved,
                            * or a capture source was missing entirely */
} og_graph_t;

/* -- pool resolution --------------------------------------------------- */

/* Capacity of the pool a request kind targets. `supported` is tracked
 * separately from capacity: a recognized kind whose pool is EMPTY must still be
 * bounds-checked, or a zero capacity would wave every slot through. */
OG_UNUSED static size_t og_pool_cap(const moq_session_t *s, int kind,
                                    int *supported)
{
    *supported = 1;
    switch (kind) {
    case MOQ_REQ_SUBSCRIPTION:        return s->sub_cap;
    case MOQ_REQ_SUBSCRIPTION_UPDATE: return s->sub_cap;
    case MOQ_REQ_ANNOUNCEMENT:        return s->ann_cap;
    case MOQ_REQ_NAMESPACE_SUB:       return s->ns_sub_cap;
    case MOQ_REQ_FETCH:               return s->fetch_cap;
    case MOQ_REQ_PUBLISH:             return s->pub_cap;
    case MOQ_REQ_PUBLICATION_UPDATE:  return s->pub_cap;
    case MOQ_REQ_TRACK_STATUS:        return s->ts_cap;
    case MOQ_REQ_SUBSCRIBE_TRACKS:    return s->track_sub_cap;
    default: break;
    }
    *supported = 0;
    return 0;
}

/* Capture one owner. An out-of-range or unsupported target NEVER reaches an
 * indexed read: it returns a loud incomparable record instead. */
OG_UNUSED static og_owner_t og_owner_capture(const moq_session_t *s,
                                             int kind, int slot)
{
    og_owner_t o;
    memset(&o, 0, sizeof(o));
    o.kind = kind;
    o.slot = slot;

    int supported = 0;
    size_t cap = og_pool_cap(s, kind, &supported);
    if (!supported) {
        OG_DIAG("OG: owner capture cannot resolve request kind %d"
                " -- record its ownership policy before relying on it\n", kind);
        o.invalid = 1;
        return o;
    }
    if (slot < 0 || (size_t)slot >= cap) {
        OG_DIAG("OG: owner capture slot %d out of range for kind %d"
                " (capacity %zu)\n", slot, kind, cap);
        o.invalid = 1;
        return o;
    }

    switch (kind) {
    case MOQ_REQ_SUBSCRIPTION:
    case MOQ_REQ_SUBSCRIPTION_UPDATE:
        o.state = (int)s->subs[slot].state;
        o.generation = s->subs[slot].generation;
        break;
    case MOQ_REQ_ANNOUNCEMENT:
        o.state = (int)s->announcements[slot].state;
        o.generation = s->announcements[slot].generation;
        break;
    case MOQ_REQ_NAMESPACE_SUB:
        o.state = (int)s->ns_subs[slot].state;
        o.generation = s->ns_subs[slot].generation;
        /* The one family whose key ownership is settled in source: the commit
         * installs the request-id edge from the entry's own request_id, the
         * bidi is keyed on the entry's own stream_ref, and it never registers
         * in idx_req_by_streamref. */
        o.identity_known = 1;
        o.has_request_id = 1;
        o.request_id = s->ns_subs[slot].request_id;
        o.has_stream_ref = 1;
        o.stream_ref = s->ns_subs[slot].stream_ref._v;
        o.has_handle = 1;
        o.handle = s->ns_subs[slot].handle._opaque;
        break;
    case MOQ_REQ_FETCH:
        o.state = (int)s->fetches[slot].state;
        o.generation = s->fetches[slot].generation;
        break;
    case MOQ_REQ_PUBLISH:
    case MOQ_REQ_PUBLICATION_UPDATE:
        o.state = (int)s->publishes[slot].state;
        o.generation = s->publishes[slot].generation;
        break;
    case MOQ_REQ_TRACK_STATUS:
        o.state = (int)s->track_statuses[slot].state;
        o.generation = s->track_statuses[slot].generation;
        break;
    case MOQ_REQ_SUBSCRIBE_TRACKS:
        o.state = (int)s->track_subs[slot].state;
        o.generation = s->track_subs[slot].generation;
        break;
    default:
        o.invalid = 1;
        return o;
    }
    /* Every pool spells its unused slot 0. */
    o.live = (o.state != 0);
    return o;
}

OG_UNUSED static int og_owner_equals(const og_owner_t *a, const og_owner_t *b,
                                     const char *what)
{
    if (a->invalid || b->invalid) {
        OG_DIAG("OG %s: incomparable owner record\n", what);
        return 1;
    }
    if (a->kind != b->kind || a->slot != b->slot || a->live != b->live ||
        a->state != b->state || a->generation != b->generation) {
        OG_DIAG("OG %s: owner (kind %d slot %d live %d state %d gen %u)"
                " expected (kind %d slot %d live %d state %d gen %u)\n", what,
                a->kind, a->slot, a->live, a->state, a->generation,
                b->kind, b->slot, b->live, b->state, b->generation);
        return 1;
    }
    /* Identity, not just liveness: a reused slot that keeps its generation but
     * takes a new request id or stream ref is still a different owner, and the
     * presence flags are compared too so an unmodelled family cannot silently
     * match a modelled one. */
    if (a->identity_known != b->identity_known ||
        a->has_request_id != b->has_request_id ||
        a->has_stream_ref != b->has_stream_ref ||
        a->has_handle != b->has_handle ||
        a->request_id != b->request_id || a->stream_ref != b->stream_ref ||
        a->handle != b->handle) {
        OG_DIAG("OG %s: owner identity (known %d rid %d:%llu ref %d:%llu"
                " handle %d:%llu) expected (known %d rid %d:%llu ref %d:%llu"
                " handle %d:%llu)\n", what,
                a->identity_known, a->has_request_id,
                (unsigned long long)a->request_id, a->has_stream_ref,
                (unsigned long long)a->stream_ref, a->has_handle,
                (unsigned long long)a->handle,
                b->identity_known, b->has_request_id,
                (unsigned long long)b->request_id, b->has_stream_ref,
                (unsigned long long)b->stream_ref, b->has_handle,
                (unsigned long long)b->handle);
        return 1;
    }
    return 0;
}

/* -- capture ----------------------------------------------------------- */

static og_owner_t *og_owner_slot(og_graph_t *g, const moq_session_t *s,
                                 int kind, int slot)
{
    for (size_t i = 0; i < g->owner_count; i++) {
        if (g->owners[i].kind == kind && g->owners[i].slot == slot)
            return &g->owners[i];
    }
    if (g->owner_count >= OG_MAX_OWNERS) {
        OG_DIAG("OG: owner capacity %d exceeded -- graph is incomparable\n",
                OG_MAX_OWNERS);
        g->overflow = 1;
        return NULL;
    }
    og_owner_t *o = &g->owners[g->owner_count++];
    *o = og_owner_capture(s, kind, slot);
    if (o->invalid) g->invalid = 1;
    return o;
}

static void og_capture_table(og_graph_t *g, const moq_session_t *s,
                             og_domain_t domain,
                             const moq_index_entry_t *tbl, size_t mask)
{
    g->tbl[domain] = tbl;
    g->mask[domain] = mask;
    if (!tbl) {
        /* A missing capture source is not an empty one: every "no such edge"
         * answer derived from it would be unfounded. */
        OG_DIAG("OG: index table %s is absent -- graph is incomparable\n",
                og_domain_name(domain));
        g->invalid = 1;
        return;
    }
    size_t cap = mask + 1;
    for (size_t i = 0; i < cap; i++) {
        if (tbl[i].slot < 0) continue;
        if (g->edge_count >= OG_MAX_EDGES) {
            OG_DIAG("OG: edge capacity %d exceeded -- graph is incomparable\n",
                    OG_MAX_EDGES);
            g->overflow = 1;
            return;
        }
        og_edge_t *e = &g->edges[g->edge_count++];
        memset(e, 0, sizeof(*e));
        e->domain = domain;
        e->key = tbl[i].key;
        e->raw = tbl[i].slot;
        e->table_index = i;

        if (domain == OG_DOM_NS_REF) {
            e->kind = MOQ_REQ_NAMESPACE_SUB;
            e->slot = (int)tbl[i].slot;
        } else {
            e->kind = (int)req_kind(tbl[i].slot);
            e->slot = req_slot(tbl[i].slot);
        }

        int supported = 0;
        size_t pool_cap = og_pool_cap(s, e->kind, &supported);
        if (!supported || e->slot < 0 || (size_t)e->slot >= pool_cap) {
            OG_DIAG("OG: %s edge key %llu targets kind %d slot %d, which is"
                    " %s (capacity %zu) -- target NOT dereferenced\n",
                    og_domain_name(domain), (unsigned long long)e->key,
                    e->kind, e->slot,
                    supported ? "out of range" : "an unsupported kind",
                    pool_cap);
            e->decodable = 0;
            g->invalid = 1;
            continue;
        }
        e->decodable = 1;
        (void)og_owner_slot(g, s, e->kind, e->slot);
    }
}

OG_UNUSED static void og_capture(const moq_session_t *s, og_graph_t *g)
{
    memset(g, 0, sizeof(*g));
    og_capture_table(g, s, OG_DOM_REQ_RID,
                     s->idx_req_by_rid, s->idx_req_mask);
    og_capture_table(g, s, OG_DOM_REQ_STREAMREF,
                     s->idx_req_by_streamref, s->idx_req_streamref_mask);
    og_capture_table(g, s, OG_DOM_NS_REF,
                     s->idx_ns_by_ref, s->idx_ns_mask);
}

/* A graph that overflowed or could not resolve a record proves nothing; every
 * check against it must fail rather than pass on a truncated picture. */
static int og_usable(const og_graph_t *g, const char *what)
{
    if (g->overflow) {
        OG_DIAG("OG %s: graph capture OVERFLOWED -- incomparable\n", what);
        return 0;
    }
    if (g->invalid) {
        OG_DIAG("OG %s: graph holds an unresolvable record -- incomparable\n",
                what);
        return 0;
    }
    return 1;
}

OG_UNUSED static const og_owner_t *og_owner_find(const og_graph_t *g,
                                                 int kind, int slot)
{
    for (size_t i = 0; i < g->owner_count; i++) {
        if (g->owners[i].kind == kind && g->owners[i].slot == slot)
            return &g->owners[i];
    }
    return NULL;
}

/* -- absolute referential integrity ------------------------------------ */

/* Structural truths that hold of ANY correct session, independent of what a
 * fixture expects: every edge decodes to a supported kind and an in-range slot,
 * its target is live, its key matches the target's declared identity where that
 * family's policy is modelled, and no logical key or edge is duplicated. */
OG_UNUSED static int og_check_integrity(const og_graph_t *g, const char *what)
{
    int failures = 0;
    if (g->overflow || g->invalid) {
        /* og_usable prints the reason; the incomparability IS the failure. */
        (void)og_usable(g, what);
        return 1;
    }

    for (size_t i = 0; i < g->edge_count; i++) {
        const og_edge_t *e = &g->edges[i];

        /* REACHABILITY. Capture walks the table so nothing can hide, but an
         * entry the production probe cannot reach is not a usable edge: a
         * broken backshift that leaves a hole in front of it makes every
         * lookup answer "absent" while a raw scan still sees it. Re-probe each
         * captured edge exactly the way production does and require the probe
         * to land on THIS entry's value. (A duplicate key legitimately fails
         * here for the losing copy; the duplicate itself is reported above.) */
        int probed = moq_index_find(g->tbl[e->domain], g->mask[e->domain],
                                    e->key);
        if (probed != (int)e->raw) {
            OG_DIAG("OG %s: %s edge key %llu at table position %zu is"
                    " UNREACHABLE by probing (lookup yields %d, entry holds"
                    " %d)\n", what, og_domain_name(e->domain),
                    (unsigned long long)e->key, e->table_index, probed,
                    (int)e->raw);
            failures++;
        }

        /* duplicate logical key within a domain, and duplicate identical edge */
        for (size_t j = i + 1; j < g->edge_count; j++) {
            const og_edge_t *f = &g->edges[j];
            if (f->domain != e->domain || f->key != e->key) continue;
            if (f->kind == e->kind && f->slot == e->slot) {
                OG_DIAG("OG %s: duplicate %s edge key %llu -> (kind %d slot %d)"
                        " at table positions %zu and %zu\n", what,
                        og_domain_name(e->domain), (unsigned long long)e->key,
                        e->kind, e->slot, e->table_index, f->table_index);
            } else {
                OG_DIAG("OG %s: duplicate %s key %llu -> (kind %d slot %d) and"
                        " (kind %d slot %d)\n", what,
                        og_domain_name(e->domain), (unsigned long long)e->key,
                        e->kind, e->slot, f->kind, f->slot);
            }
            failures++;
        }

        const og_owner_t *o = og_owner_find(g, e->kind, e->slot);
        if (!o) {
            OG_DIAG("OG %s: %s edge key %llu has no captured owner record\n",
                    what, og_domain_name(e->domain),
                    (unsigned long long)e->key);
            failures++;
            continue;
        }
        if (!o->live) {
            OG_DIAG("OG %s: %s edge key %llu targets a FREE slot"
                    " (kind %d slot %d)\n", what, og_domain_name(e->domain),
                    (unsigned long long)e->key, e->kind, e->slot);
            failures++;
            continue;
        }
        if (!o->identity_known) continue;

        if (e->domain == OG_DOM_REQ_RID && o->has_request_id &&
            e->key != o->request_id) {
            OG_DIAG("OG %s: req_by_rid key %llu targets (kind %d slot %d)"
                    " whose declared request id is %llu\n", what,
                    (unsigned long long)e->key, e->kind, e->slot,
                    (unsigned long long)o->request_id);
            failures++;
        }
        if (e->domain == OG_DOM_NS_REF && o->has_stream_ref &&
            e->key != o->stream_ref) {
            OG_DIAG("OG %s: ns_by_ref key %llu targets (kind %d slot %d)"
                    " whose declared stream ref is %llu\n", what,
                    (unsigned long long)e->key, e->kind, e->slot,
                    (unsigned long long)o->stream_ref);
            failures++;
        }
        if (e->domain == OG_DOM_REQ_STREAMREF &&
            e->kind == MOQ_REQ_NAMESPACE_SUB) {
            OG_DIAG("OG %s: req_by_streamref key %llu targets a namespace"
                    " subscription (slot %d), which never registers there\n",
                    what, (unsigned long long)e->key, e->slot);
            failures++;
        }
    }
    return failures;
}

/* -- declared expectations --------------------------------------------- */

typedef struct og_edge_spec {
    og_domain_t domain;
    uint64_t    key;
} og_edge_spec_t;

/* A required edge: this key, in this domain, pointing at exactly this owner. */
OG_UNUSED static int og_check_edge(const og_graph_t *g, og_domain_t domain,
                                   uint64_t key, int kind, int slot,
                                   const char *what)
{
    if (!og_usable(g, what)) return 1;
    for (size_t i = 0; i < g->edge_count; i++) {
        const og_edge_t *e = &g->edges[i];
        if (e->domain != domain || e->key != key) continue;
        if (e->kind == kind && e->slot == slot) return 0;
        OG_DIAG("OG %s: %s key %llu -> (kind %d slot %d), expected"
                " (kind %d slot %d)\n", what, og_domain_name(domain),
                (unsigned long long)key, e->kind, e->slot, kind, slot);
        return 1;
    }
    OG_DIAG("OG %s: required %s edge key %llu -> (kind %d slot %d) is ABSENT\n",
            what, og_domain_name(domain), (unsigned long long)key, kind, slot);
    return 1;
}

/* A forbidden key: nothing in this domain may hold it. */
OG_UNUSED static int og_check_no_edge(const og_graph_t *g, og_domain_t domain,
                                      uint64_t key, const char *what)
{
    if (!og_usable(g, what)) return 1;
    int failures = 0;
    for (size_t i = 0; i < g->edge_count; i++) {
        const og_edge_t *e = &g->edges[i];
        if (e->domain != domain || e->key != key) continue;
        OG_DIAG("OG %s: forbidden %s edge key %llu -> (kind %d slot %d)"
                " is PRESENT\n", what, og_domain_name(domain),
                (unsigned long long)key, e->kind, e->slot);
        failures++;
    }
    return failures;
}

/* An owner's EXACT edge set: every declared edge present and pointing here,
 * and no other edge anywhere pointing here. */
OG_UNUSED static int og_check_owner_edges(const og_graph_t *g, int kind,
                                          int slot,
                                          const og_edge_spec_t *want,
                                          size_t want_n, const char *what)
{
    if (!og_usable(g, what)) return 1;
    int failures = 0;

    for (size_t k = 0; k < want_n; k++) {
        size_t seen = 0;
        for (size_t i = 0; i < g->edge_count; i++) {
            const og_edge_t *e = &g->edges[i];
            if (e->domain == want[k].domain && e->key == want[k].key &&
                e->kind == kind && e->slot == slot)
                seen++;
        }
        if (seen != 1) {
            OG_DIAG("OG %s: owner (kind %d slot %d) has %zu %s edges with key"
                    " %llu, expected exactly 1\n", what, kind, slot, seen,
                    og_domain_name(want[k].domain),
                    (unsigned long long)want[k].key);
            failures++;
        }
    }

    for (size_t i = 0; i < g->edge_count; i++) {
        const og_edge_t *e = &g->edges[i];
        if (e->kind != kind || e->slot != slot) continue;
        int declared = 0;
        for (size_t k = 0; k < want_n; k++) {
            if (want[k].domain == e->domain && want[k].key == e->key) {
                declared = 1;
                break;
            }
        }
        if (!declared) {
            OG_DIAG("OG %s: owner (kind %d slot %d) carries an UNDECLARED %s"
                    " edge key %llu\n", what, kind, slot,
                    og_domain_name(e->domain), (unsigned long long)e->key);
            failures++;
        }
    }
    return failures;
}

/* No edge anywhere may point at this owner. */
OG_UNUSED static int og_check_owner_unreferenced(const og_graph_t *g, int kind,
                                                 int slot, const char *what)
{
    return og_check_owner_edges(g, kind, slot, NULL, 0, what);
}

/* Two graphs hold the SAME edges. Integrity proves each is internally valid;
 * only this proves that a valid edge was not removed, added or repointed
 * between two captures. */
OG_UNUSED static int og_check_same_topology(const og_graph_t *a,
                                            const og_graph_t *b,
                                            const char *what)
{
    if (!og_usable(a, what) || !og_usable(b, what)) return 1;
    int failures = 0;
    if (a->edge_count != b->edge_count) {
        OG_DIAG("OG %s: %zu edges, expected %zu\n", what, a->edge_count,
                b->edge_count);
        failures++;
    }
    for (size_t i = 0; i < a->edge_count; i++) {
        const og_edge_t *e = &a->edges[i];
        size_t seen = 0;
        for (size_t j = 0; j < b->edge_count; j++) {
            const og_edge_t *f = &b->edges[j];
            if (f->domain == e->domain && f->key == e->key &&
                f->kind == e->kind && f->slot == e->slot) seen++;
        }
        if (seen != 1) {
            OG_DIAG("OG %s: %s edge key %llu -> (kind %d slot %d) appears %zu"
                    " times in the expected topology, once required\n", what,
                    og_domain_name(e->domain), (unsigned long long)e->key,
                    e->kind, e->slot, seen);
            failures++;
        }
    }
    for (size_t j = 0; j < b->edge_count; j++) {
        const og_edge_t *f = &b->edges[j];
        int seen = 0;
        for (size_t i = 0; i < a->edge_count; i++) {
            const og_edge_t *e = &a->edges[i];
            if (f->domain == e->domain && f->key == e->key &&
                f->kind == e->kind && f->slot == e->slot) seen = 1;
        }
        if (!seen) {
            OG_DIAG("OG %s: expected %s edge key %llu -> (kind %d slot %d) is"
                    " MISSING\n", what, og_domain_name(f->domain),
                    (unsigned long long)f->key, f->kind, f->slot);
            failures++;
        }
    }
    return failures;
}

/* -- normalized owner closure ------------------------------------------ */

#define OG_NORM_CAP 512

typedef struct og_norm {
    char text[OG_NORM_CAP];
    int  valid;
} og_norm_t;

/* Render one owner's closure with every ordinal replaced by a symbol assigned
 * in a fixed traversal order (domains in enum order; keys ascending within a
 * domain), so two structurally equivalent owners compare equal whatever ids,
 * refs, slots or handles they received. Generation is deliberately excluded:
 * it is an identity fact, checked by og_owner_equals against a snapshot, not a
 * structural one. */
OG_UNUSED static int og_normalize_owner(const og_graph_t *g, int kind, int slot,
                                        og_norm_t *out, const char *what)
{
    memset(out, 0, sizeof(*out));
    if (!og_usable(g, what)) return 1;

    const og_owner_t *o = og_owner_find(g, kind, slot);
    og_owner_t local;
    if (!o) {
        /* An owner with no edges at all still has a closure worth comparing,
         * but the graph holds no record for it: the caller must declare it. */
        OG_DIAG("OG %s: no owner record for (kind %d slot %d) -- it carries no"
                " edges; capture it directly if that is the intended shape\n",
                what, kind, slot);
        return 1;
    }
    local = *o;

    size_t n = 0;
    /* Every append is bounds-checked and a truncation is INCOMPARABLE, never a
     * silently shortened closure that might still compare equal. */
#define OG_APPEND(...) do { \
        int rc_ = snprintf(out->text + n, OG_NORM_CAP - n, __VA_ARGS__); \
        if (rc_ < 0 || (size_t)rc_ >= OG_NORM_CAP - n) { \
            OG_DIAG("OG %s: normalized closure exceeded %d bytes" \
                    " -- incomparable\n", what, OG_NORM_CAP); \
            memset(out, 0, sizeof(*out)); \
            return 1; \
        } \
        n += (size_t)rc_; \
    } while (0)

    OG_APPEND("owner kind=%d live=%d state=%d slot=$SLOT",
              local.kind, local.live, local.state);
    if (local.has_handle)
        OG_APPEND(" handle=$HANDLE");

    int symbol = 0;
    for (int d = 0; d < OG_DOM_COUNT; d++) {
        /* Ascending-key order within a domain, chosen without sorting the
         * capture: repeatedly take the smallest key not yet emitted. */
        uint64_t prev = 0;
        int have_prev = 0;
        for (;;) {
            int found = 0;
            uint64_t best = 0;
            for (size_t i = 0; i < g->edge_count; i++) {
                const og_edge_t *e = &g->edges[i];
                if ((int)e->domain != d || e->kind != kind || e->slot != slot)
                    continue;
                if (have_prev && e->key <= prev) continue;
                if (!found || e->key < best) { best = e->key; found = 1; }
            }
            if (!found) break;
            OG_APPEND(" [%s key=$K%d]", og_domain_name((og_domain_t)d),
                      symbol++);
            prev = best;
            have_prev = 1;
        }
        OG_APPEND(" [%s end]", og_domain_name((og_domain_t)d));
    }
#undef OG_APPEND

    out->valid = 1;
    return 0;
}

OG_UNUSED static int og_check_norm_equal(const og_norm_t *a, const og_norm_t *b,
                                         const char *what)
{
    if (!a->valid || !b->valid) {
        OG_DIAG("OG %s: incomparable normalized closure\n", what);
        return 1;
    }
    if (strcmp(a->text, b->text) != 0) {
        OG_DIAG("OG %s: normalized closures differ\n  A: %s\n  B: %s\n",
                what, a->text, b->text);
        return 1;
    }
    return 0;
}

#endif /* MOQ_TEST_OWNERSHIP_GRAPH_H */
