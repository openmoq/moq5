/*
 * Self-checks for the ownership-graph auditor.
 *
 * These pin the AUDITOR, not the protocol: each case builds the index and pool
 * state directly, so a change in product behaviour cannot make a harness bug
 * pass. The auditor's behaviour against a real handoff is proven separately by
 * the live fixtures that consume it.
 *
 * Every negative case runs under og_quiet, so a passing run prints nothing:
 * the expected divergence diagnostics would otherwise read as failures.
 */
#include <moq/moq.h>
#include "test_support.h"
#include "../support/ownership_graph.h"
#include "../../core/src/session/session_internal.h"

static int failures = 0;

/* A bare draft-18 session: these cases never speak the protocol, they only
 * need the pools and the three index tables. */
static moq_session_t *bare_session(void)
{
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(),
                               MOQ_PERSPECTIVE_SERVER);
    cfg.version = MOQ_VERSION_DRAFT_18;
    moq_session_t *s = NULL;
    if (moq_session_create(&cfg, 0, &s) < 0) return NULL;
    return s;
}

/* Publish a namespace-subscription owner in `slot` with the identity a real
 * commit would give it, and the two edges that commit installs. */
static void place_ns_owner(moq_session_t *s, int slot, uint64_t request_id,
                           uint64_t stream_ref, uint64_t handle)
{
    moq_ns_sub_entry_t *e = &s->ns_subs[slot];
    memset(e, 0, sizeof(*e));
    e->state = MOQ_NS_SUB_ESTABLISHED;
    e->generation = 2;
    e->handle._opaque = handle;
    e->request_id = request_id;
    e->stream_ref = moq_stream_ref_from_u64(stream_ref);
    moq_index_insert(s->idx_ns_by_ref, s->idx_ns_mask, stream_ref, slot);
    moq_index_insert(s->idx_req_by_rid, s->idx_req_mask, request_id,
                     req_pack(MOQ_REQ_NAMESPACE_SUB, slot));
}

/* -- a valid graph passes, and its declared topology holds ------------- */

static int test_valid_graph(void)
{
    int f = 0;
    moq_session_t *s = bare_session();
    MOQ_TEST_CHECK(s != NULL);
    if (!s) return 1;
    MOQ_TEST_CHECK(s->ns_sub_cap >= 2);

    place_ns_owner(s, 0, 2, 4, 0x1001);

    og_graph_t g;
    og_capture(s, &g);
    MOQ_TEST_CHECK_EQ_INT(g.overflow, 0);
    MOQ_TEST_CHECK_EQ_INT(g.invalid, 0);
    MOQ_TEST_CHECK_EQ_SIZE(g.edge_count, 2);
    MOQ_TEST_CHECK_EQ_SIZE(g.owner_count, 1);
    f += og_check_integrity(&g, "valid");

    f += og_check_edge(&g, OG_DOM_REQ_RID, 2, MOQ_REQ_NAMESPACE_SUB, 0, "valid");
    f += og_check_edge(&g, OG_DOM_NS_REF, 4, MOQ_REQ_NAMESPACE_SUB, 0, "valid");
    f += og_check_no_edge(&g, OG_DOM_REQ_STREAMREF, 4, "valid");

    const og_edge_spec_t want[] = {
        { OG_DOM_REQ_RID, 2 },
        { OG_DOM_NS_REF,  4 },
    };
    f += og_check_owner_edges(&g, MOQ_REQ_NAMESPACE_SUB, 0, want, 2, "valid");

    moq_session_destroy(s);
    return f;
}

/* -- out-of-range target: loud, and never dereferenced ----------------- */

static int test_out_of_range_target(void)
{
    int f = 0;
    moq_session_t *s = bare_session();
    MOQ_TEST_CHECK(s != NULL);
    if (!s) return 1;

    /* A slot one past the pool. Capture must classify it without touching the
     * pool; under a sanitizer an indexed read here would abort the run. */
    int bad = (int)s->ns_sub_cap + 5;
    moq_index_insert(s->idx_ns_by_ref, s->idx_ns_mask, 4, bad);

    og_graph_t g;
    og_quiet = 1;
    og_capture(s, &g);
    MOQ_TEST_CHECK_EQ_INT(g.invalid, 1);
    MOQ_TEST_CHECK_EQ_SIZE(g.owner_count, 0);
    MOQ_TEST_CHECK_EQ_INT(g.edges[0].decodable, 0);
    MOQ_TEST_CHECK(og_check_integrity(&g, "oob") > 0);
    MOQ_TEST_CHECK(og_check_edge(&g, OG_DOM_NS_REF, 4,
                                 MOQ_REQ_NAMESPACE_SUB, 0, "oob") > 0);
    og_quiet = 0;

    moq_session_destroy(s);
    return f;
}

/* An unsupported packed kind is the same class of refusal. */
static int test_unsupported_kind(void)
{
    int f = 0;
    moq_session_t *s = bare_session();
    MOQ_TEST_CHECK(s != NULL);
    if (!s) return 1;

    moq_index_insert(s->idx_req_by_rid, s->idx_req_mask, 6,
                     req_pack((moq_request_kind_t)77, 0));

    og_graph_t g;
    og_quiet = 1;
    og_capture(s, &g);
    MOQ_TEST_CHECK_EQ_INT(g.invalid, 1);
    MOQ_TEST_CHECK_EQ_SIZE(g.owner_count, 0);
    MOQ_TEST_CHECK(og_check_integrity(&g, "kind") > 0);
    og_quiet = 0;

    moq_session_destroy(s);
    return f;
}

/* -- an edge to a free slot fails -------------------------------------- */

static int test_edge_to_free_slot(void)
{
    int f = 0;
    moq_session_t *s = bare_session();
    MOQ_TEST_CHECK(s != NULL);
    if (!s) return 1;

    /* In range, so the target IS read -- and it is FREE. */
    moq_index_insert(s->idx_ns_by_ref, s->idx_ns_mask, 4, 0);

    og_graph_t g;
    og_capture(s, &g);
    MOQ_TEST_CHECK_EQ_INT(g.invalid, 0);
    MOQ_TEST_CHECK_EQ_SIZE(g.owner_count, 1);
    MOQ_TEST_CHECK_EQ_INT(g.owners[0].live, 0);
    og_quiet = 1;
    MOQ_TEST_CHECK(og_check_integrity(&g, "free") > 0);
    og_quiet = 0;

    moq_session_destroy(s);
    return f;
}

/* -- a missing required edge fails ------------------------------------- */

static int test_missing_required_edge(void)
{
    int f = 0;
    moq_session_t *s = bare_session();
    MOQ_TEST_CHECK(s != NULL);
    if (!s) return 1;

    place_ns_owner(s, 0, 2, 4, 0x1001);
    /* Delete the request-id edge the commit installed -- the shape the
     * ordinal-zero cross-owner deletion produces. */
    moq_index_remove(s->idx_req_by_rid, s->idx_req_mask, 2);

    og_graph_t g;
    og_capture(s, &g);
    /* Structural integrity still holds: what is left is self-consistent. The
     * loss is only visible against the DECLARED topology. */
    f += og_check_integrity(&g, "missing");

    og_quiet = 1;
    MOQ_TEST_CHECK(og_check_edge(&g, OG_DOM_REQ_RID, 2,
                                 MOQ_REQ_NAMESPACE_SUB, 0, "missing") > 0);
    const og_edge_spec_t want[] = {
        { OG_DOM_REQ_RID, 2 },
        { OG_DOM_NS_REF,  4 },
    };
    MOQ_TEST_CHECK(og_check_owner_edges(&g, MOQ_REQ_NAMESPACE_SUB, 0,
                                        want, 2, "missing") > 0);
    og_quiet = 0;

    moq_session_destroy(s);
    return f;
}

/* -- an unexpected foreign edge fails ---------------------------------- */

static int test_foreign_edge(void)
{
    int f = 0;
    moq_session_t *s = bare_session();
    MOQ_TEST_CHECK(s != NULL);
    if (!s) return 1;

    place_ns_owner(s, 0, 2, 4, 0x1001);
    /* A namespace subscription never registers in the stream-ref request
     * registry; an edge that says otherwise is a foreign edge. */
    moq_index_insert(s->idx_req_by_streamref, s->idx_req_streamref_mask, 4,
                     req_pack(MOQ_REQ_NAMESPACE_SUB, 0));

    og_graph_t g;
    og_capture(s, &g);
    og_quiet = 1;
    MOQ_TEST_CHECK(og_check_integrity(&g, "foreign") > 0);
    MOQ_TEST_CHECK(og_check_no_edge(&g, OG_DOM_REQ_STREAMREF, 4,
                                    "foreign") > 0);
    const og_edge_spec_t want[] = {
        { OG_DOM_REQ_RID, 2 },
        { OG_DOM_NS_REF,  4 },
    };
    MOQ_TEST_CHECK(og_check_owner_edges(&g, MOQ_REQ_NAMESPACE_SUB, 0,
                                        want, 2, "foreign") > 0);
    og_quiet = 0;

    moq_session_destroy(s);
    return f;
}

/* -- duplicate key and duplicate edge both fail ------------------------ */

static int test_duplicates(void)
{
    int f = 0;

    /* Same key, two different targets. */
    {
        moq_session_t *s = bare_session();
        MOQ_TEST_CHECK(s != NULL);
        if (!s) return 1;
        MOQ_TEST_CHECK(s->ns_sub_cap >= 2);
        place_ns_owner(s, 0, 2, 4, 0x1001);
        place_ns_owner(s, 1, 6, 8, 0x1002);
        /* A second edge on key 2 pointing at the other owner. moq_index_insert
         * appends without checking, so a lookup would only ever see the first
         * -- which is exactly why capture walks the table. */
        moq_index_insert(s->idx_req_by_rid, s->idx_req_mask, 2,
                         req_pack(MOQ_REQ_NAMESPACE_SUB, 1));
        og_graph_t g;
        og_capture(s, &g);
        og_quiet = 1;
        MOQ_TEST_CHECK(og_check_integrity(&g, "dupkey") > 0);
        og_quiet = 0;
        moq_session_destroy(s);
    }

    /* Same key, same target, twice. */
    {
        moq_session_t *s = bare_session();
        MOQ_TEST_CHECK(s != NULL);
        if (!s) return 1;
        place_ns_owner(s, 0, 2, 4, 0x1001);
        moq_index_insert(s->idx_ns_by_ref, s->idx_ns_mask, 4, 0);
        og_graph_t g;
        og_capture(s, &g);
        og_quiet = 1;
        MOQ_TEST_CHECK(og_check_integrity(&g, "dupedge") > 0);
        /* The owner's exact edge set sees two ns_by_ref edges where one was
         * declared. */
        const og_edge_spec_t want[] = {
            { OG_DOM_REQ_RID, 2 },
            { OG_DOM_NS_REF,  4 },
        };
        MOQ_TEST_CHECK(og_check_owner_edges(&g, MOQ_REQ_NAMESPACE_SUB, 0,
                                            want, 2, "dupedge") > 0);
        og_quiet = 0;
        moq_session_destroy(s);
    }

    return f;
}

/* -- capture overflow is incomparable ---------------------------------- */

static int test_overflow_incomparable(void)
{
    int f = 0;
    moq_session_t *s = bare_session();
    MOQ_TEST_CHECK(s != NULL);
    if (!s) return 1;

    /* The table must have room for more live entries than the graph can hold,
     * or this case would prove nothing. Assert it rather than skip. */
    size_t tbl_cap = s->idx_req_mask + 1;
    MOQ_TEST_CHECK(tbl_cap > (size_t)OG_MAX_EDGES + 1);
    if (tbl_cap <= (size_t)OG_MAX_EDGES + 1) { moq_session_destroy(s); return 1; }

    place_ns_owner(s, 0, 2, 4, 0x1001);
    for (size_t i = 0; i < (size_t)OG_MAX_EDGES + 1; i++)
        moq_index_insert(s->idx_req_by_rid, s->idx_req_mask, 1000 + i,
                         req_pack(MOQ_REQ_NAMESPACE_SUB, 0));

    og_graph_t g;
    og_quiet = 1;
    og_capture(s, &g);
    MOQ_TEST_CHECK_EQ_INT(g.overflow, 1);
    /* Every check against a truncated picture must FAIL, including one whose
     * edge was captured before the cut. */
    MOQ_TEST_CHECK(og_check_integrity(&g, "overflow") > 0);
    MOQ_TEST_CHECK(og_check_edge(&g, OG_DOM_REQ_RID, 2,
                                 MOQ_REQ_NAMESPACE_SUB, 0, "overflow") > 0);
    MOQ_TEST_CHECK(og_check_no_edge(&g, OG_DOM_REQ_STREAMREF, 4,
                                    "overflow") > 0);
    og_norm_t n;
    MOQ_TEST_CHECK(og_normalize_owner(&g, MOQ_REQ_NAMESPACE_SUB, 0, &n,
                                      "overflow") > 0);
    MOQ_TEST_CHECK_EQ_INT(n.valid, 0);
    og_quiet = 0;

    moq_session_destroy(s);
    return f;
}

/* -- ordinal equivariance: ids 0 and 2 normalize equal ----------------- */

static int test_normalization(void)
{
    int f = 0;
    moq_session_t *s = bare_session();
    MOQ_TEST_CHECK(s != NULL);
    if (!s) return 1;
    MOQ_TEST_CHECK(s->ns_sub_cap >= 2);

    /* Two structurally identical owners that differ in every ordinal: request
     * id, stream ref, pool slot and handle. */
    place_ns_owner(s, 0, 0, 4, 0x1001);
    place_ns_owner(s, 1, 2, 8, 0x1002);

    og_graph_t g;
    og_capture(s, &g);
    f += og_check_integrity(&g, "norm");

    og_norm_t a, b;
    f += og_normalize_owner(&g, MOQ_REQ_NAMESPACE_SUB, 0, &a, "norm-a");
    f += og_normalize_owner(&g, MOQ_REQ_NAMESPACE_SUB, 1, &b, "norm-b");
    f += og_check_norm_equal(&a, &b, "norm");

    /* Delete one renamed edge and the closures must diverge. */
    moq_index_remove(s->idx_req_by_rid, s->idx_req_mask, 2);
    og_capture(s, &g);
    og_norm_t a2, b2;
    f += og_normalize_owner(&g, MOQ_REQ_NAMESPACE_SUB, 0, &a2, "norm-a2");
    f += og_normalize_owner(&g, MOQ_REQ_NAMESPACE_SUB, 1, &b2, "norm-b2");
    og_quiet = 1;
    MOQ_TEST_CHECK(og_check_norm_equal(&a2, &b2, "norm-diff") > 0);
    og_quiet = 0;

    moq_session_destroy(s);
    return f;
}

/* -- an edge stranded behind a hole is UNREACHABLE, not present -------- */

/* Find the raw table position holding `key`, without knowing the hash. */
static int tbl_pos_of(const moq_index_entry_t *tbl, size_t mask, uint64_t key)
{
    for (size_t i = 0; i <= mask; i++)
        if (tbl[i].slot >= 0 && tbl[i].key == key) return (int)i;
    return -1;
}

static int test_unreachable_edge(void)
{
    int f = 0;
    moq_session_t *s = bare_session();
    MOQ_TEST_CHECK(s != NULL);
    if (!s) return 1;

    place_ns_owner(s, 0, 2, 4, 0x1001);
    /* The only entry in an empty table sits at its home position. Shift it one
     * probe step forward and leave a hole where it was: the raw scan still
     * sees the edge, and every real lookup now answers "absent" -- the shape a
     * broken backshift leaves behind. */
    int p = tbl_pos_of(s->idx_ns_by_ref, s->idx_ns_mask, 4);
    MOQ_TEST_CHECK(p >= 0);
    if (p < 0) { moq_session_destroy(s); return 1; }
    size_t nxt = ((size_t)p + 1) & s->idx_ns_mask;
    MOQ_TEST_CHECK(s->idx_ns_by_ref[nxt].slot < 0);
    s->idx_ns_by_ref[nxt] = s->idx_ns_by_ref[p];
    s->idx_ns_by_ref[p].slot = -1;
    MOQ_TEST_CHECK(moq_index_find(s->idx_ns_by_ref, s->idx_ns_mask, 4) < 0);

    og_graph_t g;
    og_capture(s, &g);
    /* The raw scan still reports the edge, so the declared-topology checks
     * pass -- which is exactly why integrity must not. */
    MOQ_TEST_CHECK_EQ_SIZE(g.edge_count, 2);
    f += og_check_edge(&g, OG_DOM_NS_REF, 4, MOQ_REQ_NAMESPACE_SUB, 0,
                       "hole-raw");
    og_quiet = 1;
    MOQ_TEST_CHECK(og_check_integrity(&g, "hole") > 0);
    og_quiet = 0;

    moq_session_destroy(s);
    return f;
}

/* -- an absent table is INCOMPARABLE, not empty ------------------------ */

static int test_absent_table(void)
{
    int f = 0;
    moq_session_t *s = bare_session();
    MOQ_TEST_CHECK(s != NULL);
    if (!s) return 1;

    place_ns_owner(s, 0, 2, 4, 0x1001);
    moq_index_entry_t *saved = s->idx_ns_by_ref;
    s->idx_ns_by_ref = NULL;

    og_graph_t g;
    og_quiet = 1;
    og_capture(s, &g);
    MOQ_TEST_CHECK_EQ_INT(g.invalid, 1);
    /* A "no such edge" answer drawn from a table that was never read must
     * FAIL, not pass. */
    MOQ_TEST_CHECK(og_check_no_edge(&g, OG_DOM_NS_REF, 4, "absent") > 0);
    MOQ_TEST_CHECK(og_check_integrity(&g, "absent") > 0);
    og_quiet = 0;

    s->idx_ns_by_ref = saved;
    moq_session_destroy(s);
    return f;
}

/* -- topology comparison: same edges, however they are ordered --------- */

static int test_topology(void)
{
    int f = 0;
    moq_session_t *s = bare_session();
    MOQ_TEST_CHECK(s != NULL);
    if (!s) return 1;
    MOQ_TEST_CHECK(s->ns_sub_cap >= 2);
    place_ns_owner(s, 0, 2, 4, 0x1001);
    place_ns_owner(s, 1, 6, 8, 0x1002);

    og_graph_t a, b;
    og_capture(s, &a);
    og_capture(s, &b);
    f += og_check_integrity(&a, "topo");
    f += og_check_same_topology(&a, &b, "topo-identical");

    /* Order is not identity: the same multiset in a different order passes. */
    {
        og_graph_t r = a;
        for (size_t i = 0; i < r.edge_count / 2; i++) {
            og_edge_t t = r.edges[i];
            r.edges[i] = r.edges[r.edge_count - 1 - i];
            r.edges[r.edge_count - 1 - i] = t;
        }
        f += og_check_same_topology(&r, &a, "topo-reordered");
    }

    og_quiet = 1;
    /* Missing. */
    {
        og_graph_t m = a;
        m.edges[0] = m.edges[m.edge_count - 1];
        m.edge_count--;
        MOQ_TEST_CHECK(og_check_same_topology(&m, &a, "topo-missing") > 0);
        MOQ_TEST_CHECK(og_check_same_topology(&a, &m, "topo-extra") > 0);
    }
    /* Key repointed. */
    {
        og_graph_t k = a;
        k.edges[0].key += 100;
        MOQ_TEST_CHECK(og_check_same_topology(&k, &a, "topo-key") > 0);
    }
    /* Kind and slot repointed, each on its own. */
    {
        og_graph_t k = a;
        k.edges[0].kind = MOQ_REQ_SUBSCRIPTION;
        MOQ_TEST_CHECK(og_check_same_topology(&k, &a, "topo-kind") > 0);
        k = a;
        k.edges[0].slot = k.edges[0].slot == 0 ? 1 : 0;
        MOQ_TEST_CHECK(og_check_same_topology(&k, &a, "topo-slot") > 0);
    }
    /* Multiplicity: the same edge twice is not the same multiset. */
    {
        og_graph_t d = a;
        MOQ_TEST_CHECK(d.edge_count < OG_MAX_EDGES);
        d.edges[d.edge_count] = d.edges[0];
        d.edge_count++;
        MOQ_TEST_CHECK(og_check_same_topology(&d, &a, "topo-dup") > 0);
    }
    /* An incomparable graph fails on either side. */
    {
        og_graph_t bad = a;
        bad.invalid = 1;
        MOQ_TEST_CHECK(og_check_same_topology(&bad, &a, "topo-invalid") > 0);
        MOQ_TEST_CHECK(og_check_same_topology(&a, &bad, "topo-invalid") > 0);
        bad = a;
        bad.overflow = 1;
        MOQ_TEST_CHECK(og_check_same_topology(&bad, &a, "topo-overflow") > 0);
        MOQ_TEST_CHECK(og_check_same_topology(&a, &bad, "topo-overflow") > 0);
    }
    og_quiet = 0;

    moq_session_destroy(s);
    return f;
}

/* -- slot reuse: edges to the retired owner's identity fail ------------ */

static int test_slot_reuse(void)
{
    int f = 0;
    moq_session_t *s = bare_session();
    MOQ_TEST_CHECK(s != NULL);
    if (!s) return 1;

    place_ns_owner(s, 0, 2, 4, 0x1001);
    og_graph_t g;
    og_capture(s, &g);
    f += og_check_integrity(&g, "reuse");
    const og_owner_t *before = og_owner_find(&g, MOQ_REQ_NAMESPACE_SUB, 0);
    MOQ_TEST_CHECK(before != NULL);
    if (!before) { moq_session_destroy(s); return 1; }
    og_owner_t snap = *before;

    /* The slot is retired and handed to a NEW owner: new generation, new
     * request id, new stream ref, new handle -- but the retired owner's edges
     * are left behind, which is the defect. They still point at a LIVE target,
     * so only the key/identity agreement can catch them. */
    s->ns_subs[0].generation += 2;
    s->ns_subs[0].handle._opaque = 0x2002;
    s->ns_subs[0].request_id = 6;
    s->ns_subs[0].stream_ref = moq_stream_ref_from_u64(8);

    og_capture(s, &g);
    og_quiet = 1;
    MOQ_TEST_CHECK(og_check_integrity(&g, "reuse-stale") > 0);
    og_quiet = 0;
    const og_owner_t *after = og_owner_find(&g, MOQ_REQ_NAMESPACE_SUB, 0);
    MOQ_TEST_CHECK(after != NULL);
    if (!after) { moq_session_destroy(s); return 1; }
    og_quiet = 1;
    MOQ_TEST_CHECK(og_owner_equals(after, &snap, "reuse") > 0);
    og_quiet = 0;

    /* Identity alone discriminates: same generation, different request id. */
    {
        og_owner_t renamed = snap;
        renamed.request_id = snap.request_id + 4;
        og_quiet = 1;
        MOQ_TEST_CHECK(og_owner_equals(&renamed, &snap, "reuse-rid") > 0);
        og_quiet = 0;
    }

    /* Replacing the stale edges with the new owner's keys makes it green. */
    moq_index_remove(s->idx_ns_by_ref, s->idx_ns_mask, 4);
    moq_index_remove(s->idx_req_by_rid, s->idx_req_mask, 2);
    moq_index_insert(s->idx_ns_by_ref, s->idx_ns_mask, 8, 0);
    moq_index_insert(s->idx_req_by_rid, s->idx_req_mask, 6,
                     req_pack(MOQ_REQ_NAMESPACE_SUB, 0));
    og_capture(s, &g);
    f += og_check_integrity(&g, "reuse-replaced");
    {
        const og_edge_spec_t want[] = {
            { OG_DOM_REQ_RID, 6 },
            { OG_DOM_NS_REF,  8 },
        };
        f += og_check_owner_edges(&g, MOQ_REQ_NAMESPACE_SUB, 0, want, 2,
                                  "reuse-replaced");
    }

    /* An unresolvable record is incomparable even against itself. */
    og_owner_t bad = snap;
    bad.invalid = 1;
    og_quiet = 1;
    MOQ_TEST_CHECK(og_owner_equals(&bad, &bad, "reuse-invalid") > 0);
    og_quiet = 0;

    moq_session_destroy(s);
    return f;
}

int main(void)
{
    failures += test_valid_graph();
    failures += test_out_of_range_target();
    failures += test_unsupported_kind();
    failures += test_edge_to_free_slot();
    failures += test_missing_required_edge();
    failures += test_foreign_edge();
    failures += test_duplicates();
    failures += test_overflow_incomparable();
    failures += test_normalization();
    failures += test_topology();
    failures += test_unreachable_edge();
    failures += test_absent_table();
    failures += test_slot_reuse();

    if (failures) {
        fprintf(stderr, "FAILURES: %d\n", failures);
        return 1;
    }
    MOQ_TEST_PASS("ownership_graph");
    return 0;
}
