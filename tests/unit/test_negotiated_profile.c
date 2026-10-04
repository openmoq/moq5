/*
 * Negotiated-profile substrate (Step 5): the green declaration.
 *
 * This target owns the product-independent half: the pure oracle, the four
 * declared tables, the fail-closed corpus, the derived pair relation, and the
 * drift gate against core profile availability and the Step-3 ALPN table.
 * The endpoint-offer/AUTO half of the drift gate needs the service tier and
 * lives in service/tests/test_negotiated_profile_offer.c.
 *
 * There are NO service closures here: LOC-01 cells carry temporary closure
 * waivers, and Step 6 replaces those with the shared send/receive functions.
 * Nothing in this file calls a LOC, KVP or integer-codec product API.
 */
#include "np_oracle.h"
#include "np_tables.h"
#include "np_corpus.h"
#include "test_support.h"

/* the drift gate's two product authorities */
#include "profile.h"       /* moq_profile_lookup: core availability */
#include "moq_alpn.h"      /* the Step-3 canonical ALPN table */

#include <inttypes.h>
#include <stdio.h>
#include <string.h>

static int failures = 0;

#ifndef NP_CORPUS_PATH
#error "NP_CORPUS_PATH must be defined at configure time"
#endif

/* ===================================================================== *
 * 1. The oracle, before anything trusts it
 * ===================================================================== */
static void t_oracle(void)
{
    MOQ_TEST_CHECK_EQ_INT(np_oracle_self_check(), 0);
    MOQ_TEST_PASS("np_oracle");
}

/* ===================================================================== *
 * 2. The declared topology
 * ===================================================================== */
static void t_tables(void)
{
    MOQ_TEST_CHECK_EQ_INT(np_tables_validate(), 0);

    size_t nt = 0, nm = 0, nc = 0, np_ = 0;
    (void)np_transports(&nt); (void)np_medias(&nm);
    (void)np_cells(&nc);      (void)np_pairs(&np_);
    MOQ_TEST_CHECK_EQ_SIZE(nt, 3);
    MOQ_TEST_CHECK_EQ_SIZE(nm, 2);
    MOQ_TEST_CHECK_EQ_SIZE(nc, nt * nm);
    MOQ_TEST_CHECK_EQ_SIZE(np_, 1);

    /* the reviewed transport facts, asserted as literals */
    const np_transport_row_t *d16 = np_transport_by_draft(16);
    const np_transport_row_t *d18 = np_transport_by_draft(18);
    MOQ_TEST_CHECK(d16 && d18);
    if (d16 && d18) {
        MOQ_TEST_CHECK(strcmp(d16->alpn, "moqt-16") == 0);
        MOQ_TEST_CHECK(strcmp(d18->alpn, "moqt-18") == 0);
        MOQ_TEST_CHECK(d16->enc == NP_ENC_QUIC_VARINT);
        MOQ_TEST_CHECK(d18->enc == NP_ENC_VI64);
        MOQ_TEST_CHECK_EQ_U64(d16->int_max, NP_QUIC_VARINT_MAX);
        MOQ_TEST_CHECK_EQ_U64(d18->int_max, NP_VI64_MAX);
        /* AUTO offers the NEWEST draft first (21, 18, 16) */
        MOQ_TEST_CHECK_EQ_INT(d18->auto_rank, 2);
        MOQ_TEST_CHECK_EQ_INT(d16->auto_rank, 3);
    }

    /* Draft 21: wire ready, AVAILABLE, offered first. Its media cells stay
     * UNSUPPORTED until the LOC property ids are implemented for it, so the row
     * keeps a reason. */
    MOQ_TEST_CHECK(moq_profile_lookup(MOQ_VERSION_DRAFT_21) != NULL);
    MOQ_TEST_CHECK(moq_profile_wire_ready(MOQ_VERSION_DRAFT_21));
    MOQ_TEST_CHECK(moq_profile_wire_ready(MOQ_VERSION_DRAFT_16));
    MOQ_TEST_CHECK(moq_profile_wire_ready(MOQ_VERSION_DRAFT_18));
    const np_transport_row_t *d21 = np_transport_by_draft(21);
    MOQ_TEST_CHECK(d21 != NULL);
    if (d21) {
        MOQ_TEST_CHECK(strcmp(d21->alpn, "moqt-21") == 0);
        MOQ_TEST_CHECK(d21->enc == NP_ENC_VI64);
        MOQ_TEST_CHECK_EQ_U64(d21->int_max, NP_VI64_MAX);
        MOQ_TEST_CHECK(d21->state == NP_T_AVAILABLE);
        MOQ_TEST_CHECK(d21->endpoint_offered);
        MOQ_TEST_CHECK_EQ_INT(d21->auto_rank, 1);
        MOQ_TEST_CHECK(d21->unusable_reason && d21->unusable_reason[0]);
    }
    {
        const np_cell_row_t *c1 = np_cell(21, "loc01");
        const np_cell_row_t *c2 = np_cell(21, "loc02");
        MOQ_TEST_CHECK(c1 && c2);
        if (c1 && c2) {
            MOQ_TEST_CHECK(c1->state == NP_C_UNSUPPORTED);
            MOQ_TEST_CHECK(c2->state == NP_C_UNSUPPORTED);
            MOQ_TEST_CHECK(c1->reason && c1->reason[0]);
            MOQ_TEST_CHECK(c2->reason && c2->reason[0]);
        }
    }

    /* the reviewed media facts, including the id collision that motivates
     * the whole arc */
    const np_media_row_t *l1 = np_media_by_id("loc01");
    const np_media_row_t *l2 = np_media_by_id("loc02");
    MOQ_TEST_CHECK(l1 && l2);
    if (l1 && l2) {
        MOQ_TEST_CHECK(l1->state == NP_M_IMPLEMENTED);
        MOQ_TEST_CHECK(l2->state == NP_M_RESERVED);
        MOQ_TEST_CHECK_EQ_U64(l1->timestamp_property_id, 2);
        MOQ_TEST_CHECK_EQ_U64(l2->timestamp_property_id, 6);
        MOQ_TEST_CHECK(l2->reason && l2->reason[0]);
    }

    /* every LOC-01 cell is SUPPORTED-with-waiver and BOTH callbacks absent;
     * every LOC-02 cell is UNSUPPORTED with a reason and no participation */
    for (uint32_t d = 16; d <= 18; d += 2) {
        const np_cell_row_t *c1 = np_cell(d, "loc01");
        const np_cell_row_t *c2 = np_cell(d, "loc02");
        MOQ_TEST_CHECK(c1 && c2);
        if (c1) {
            MOQ_TEST_CHECK(c1->state == NP_C_SUPPORTED);
            MOQ_TEST_CHECK(c1->waiver && c1->waiver[0]);
            MOQ_TEST_CHECK(c1->send_closure == NULL);
            MOQ_TEST_CHECK(c1->recv_closure == NULL);
        }
        if (c2) {
            MOQ_TEST_CHECK(c2->state == NP_C_UNSUPPORTED);
            MOQ_TEST_CHECK(c2->reason && c2->reason[0]);
            MOQ_TEST_CHECK(c2->waiver == NULL);
            MOQ_TEST_CHECK(c2->send_closure == NULL);
            MOQ_TEST_CHECK(c2->recv_closure == NULL);
        }
    }

    const np_pair_row_t *pairs = np_pairs(&np_);
    MOQ_TEST_CHECK(np_ == 1 && strcmp(pairs[0].media, "loc01") == 0);
    MOQ_TEST_CHECK(pairs[0].draft_a == 16 && pairs[0].draft_b == 18);
    MOQ_TEST_CHECK(pairs[0].relation == NP_R_ENCODING_DIVERGES);

    MOQ_TEST_PASS("np_tables");
}

/* ===================================================================== *
 * 3. Broken topologies must be REFUSED (quiet expected-failures)
 * ===================================================================== */
#define NP_MAX_T 4
#define NP_MAX_M 4
#define NP_MAX_C 16
#define NP_MAX_P 8

typedef struct {
    np_transport_row_t t[NP_MAX_T];
    np_media_row_t     m[NP_MAX_M];
    np_cell_row_t      c[NP_MAX_C];
    np_pair_row_t      p[NP_MAX_P];
    np_topology_t      view;
} np_scratch_t;

static void scratch_from_declared(np_scratch_t *s)
{
    np_topology_t d = np_declared_topology();
    memcpy(s->t, d.transports, d.n_transports * sizeof(s->t[0]));
    memcpy(s->m, d.medias,     d.n_medias     * sizeof(s->m[0]));
    memcpy(s->c, d.cells,      d.n_cells      * sizeof(s->c[0]));
    memcpy(s->p, d.pairs,      d.n_pairs      * sizeof(s->p[0]));
    s->view.transports = s->t; s->view.n_transports = d.n_transports;
    s->view.medias     = s->m; s->view.n_medias     = d.n_medias;
    s->view.cells      = s->c; s->view.n_cells      = d.n_cells;
    s->view.pairs      = s->p; s->view.n_pairs      = d.n_pairs;
}

/* Real closure stand-ins of the DECLARED types, so the waiver self-checks
 * never cast a function pointer through a void * (which ISO C forbids). */
static void np_probe_send(void) { }
static void np_probe_recv(void) { }

static void expect_refused(const char *name, const np_scratch_t *s)
{
    int problems = np_topology_validate(&s->view, true);   /* quiet */
    if (problems == 0) {
        fprintf(stderr, "FAIL: topology[%s] was ACCEPTED\n", name);
        failures++;
    }
}

static void t_topology_negatives(void)
{
    np_scratch_t s;

    /* the declared topology itself is accepted (guards against a validator
     * that simply rejects everything) */
    scratch_from_declared(&s);
    MOQ_TEST_CHECK_EQ_INT(np_topology_validate(&s.view, true), 0);

    scratch_from_declared(&s); s.view.n_transports = 1;   /* cells now extra */
    expect_refused("missing_transport", &s);

    scratch_from_declared(&s); s.t[1].draft = 16;
    expect_refused("duplicate_transport", &s);

    scratch_from_declared(&s); s.t[1].alpn = "moqt-16"; s.t[1].alpn_len = 7;
    expect_refused("duplicate_alpn", &s);

    scratch_from_declared(&s); s.view.n_medias = 1;
    expect_refused("missing_media", &s);

    scratch_from_declared(&s); s.m[1].id = "loc01";
    expect_refused("duplicate_media", &s);

    scratch_from_declared(&s); s.m[1].reason = "";
    expect_refused("reserved_without_reason", &s);

    scratch_from_declared(&s); s.view.n_cells = 3;
    expect_refused("missing_cell", &s);

    scratch_from_declared(&s); s.c[3] = s.c[2];
    expect_refused("duplicate_cell", &s);

    scratch_from_declared(&s); s.c[2].reason = NULL;
    expect_refused("unsupported_without_reason", &s);

    /* the waiver rule: half states and empty waivers fail */
    scratch_from_declared(&s); s.c[0].waiver = NULL;
    expect_refused("supported_without_waiver_or_callbacks", &s);
    scratch_from_declared(&s); s.c[0].waiver = "";
    expect_refused("empty_waiver", &s);
    scratch_from_declared(&s); s.c[0].send_closure = np_probe_send;
    expect_refused("half_callbacks_send_only", &s);
    scratch_from_declared(&s); s.c[0].recv_closure = np_probe_recv;
    expect_refused("half_callbacks_recv_only", &s);
    scratch_from_declared(&s);
    s.c[0].send_closure = np_probe_send;
    s.c[0].recv_closure = np_probe_recv;
    expect_refused("callbacks_and_waiver_together", &s);

    /* AUTO ranks */
    scratch_from_declared(&s); s.t[0].auto_rank = 1;
    expect_refused("duplicate_auto_rank", &s);
    scratch_from_declared(&s); s.t[0].auto_rank = 4;     /* 1, 2 and 4, no 3 */
    expect_refused("non_contiguous_auto_rank", &s);
    /* an endpoint-offered AVAILABLE row that is missing from AUTO */
    scratch_from_declared(&s); s.t[0].auto_rank = NP_AUTO_RANK_NONE;
    expect_refused("offered_but_unranked", &s);
    /* ... and dropping BOTH leaves an empty AUTO list, also refused */
    scratch_from_declared(&s);
    s.t[0].auto_rank = NP_AUTO_RANK_NONE; s.t[0].endpoint_offered = false;
    s.t[1].auto_rank = NP_AUTO_RANK_NONE; s.t[1].endpoint_offered = false;
    s.t[2].auto_rank = NP_AUTO_RANK_NONE; s.t[2].endpoint_offered = false;
    expect_refused("auto_empty", &s);
    scratch_from_declared(&s); s.t[0].state = NP_T_ABSENT;
    expect_refused("absent_transport_ranked", &s);
    /*
     * THE ALPN-ONLY HOLE: a registered-but-unavailable row claiming to be
     * endpoint-offered while carrying NO rank. The case above leaves the rank
     * in place and so does not discriminate this shape.
     */
    scratch_from_declared(&s);
    s.t[0].state = NP_T_ABSENT;
    s.t[0].auto_rank = NP_AUTO_RANK_NONE;
    s.t[0].endpoint_offered = true;
    s.t[0].unusable_reason = "registered ALPN, no profile built here";
    expect_refused("absent_row_offered_unranked", &s);
    /* and the same row NOT offered and not ranked is valid (with its reason,
     * and with its cells still declared) */
    scratch_from_declared(&s);
    s.t[0].state = NP_T_ABSENT;
    s.t[0].auto_rank = NP_AUTO_RANK_NONE;
    s.t[0].endpoint_offered = false;
    s.t[0].unusable_reason = "registered ALPN, no profile built here";
    s.c[0].state = NP_C_UNSUPPORTED;
    s.c[0].reason = "no draft-16 profile in this hypothetical";
    s.c[0].waiver = NULL;
    s.view.n_pairs = 0;
    MOQ_TEST_CHECK_EQ_INT(np_topology_validate(&s.view, true), 0);
    /* a ranked row that is not offered disagrees too */
    scratch_from_declared(&s); s.t[0].endpoint_offered = false;
    expect_refused("ranked_but_not_offered", &s);
    scratch_from_declared(&s); s.t[0].endpoint_offered = false;
    expect_refused("offered_without_endpoint", &s);

    /* a RESERVED profile may not be SUPPORTED; an IMPLEMENTED one must be
     * supported somewhere */
    scratch_from_declared(&s); s.c[2].state = NP_C_SUPPORTED;
    s.c[2].reason = NULL; s.c[2].waiver = "x";
    expect_refused("reserved_media_supported", &s);
    scratch_from_declared(&s);
    s.c[0].state = NP_C_UNSUPPORTED; s.c[0].reason = "r"; s.c[0].waiver = NULL;
    s.c[1].state = NP_C_UNSUPPORTED; s.c[1].reason = "r"; s.c[1].waiver = NULL;
    expect_refused("implemented_media_unsupported_everywhere", &s);

    /*
     * VALID shape that must be ACCEPTED: an implemented media profile that is
     * incompatible with D16 but supported on D18 -- one supported cell, so
     * zero pairs. Under the supported-participation invariant D16 then carries
     * no implemented media, so its ROW must say why; that is the invariant
     * working, not an obstacle to the case.
     */
    scratch_from_declared(&s);
    s.c[0].state = NP_C_UNSUPPORTED;
    s.c[0].reason = "hypothetical profile needs vi64 reach beyond 2^62-1";
    s.c[0].waiver = NULL;
    s.t[0].unusable_reason =
        "D16 carries no implemented media profile in this hypothetical";
    s.view.n_pairs = 0;
    MOQ_TEST_CHECK_EQ_INT(np_topology_validate(&s.view, true), 0);
    /* ... the same shape WITHOUT the CELL's reason is refused */
    s.c[0].reason = NULL;
    expect_refused("d16_incompatible_without_cell_reason", &s);
    /* ... and WITHOUT the TRANSPORT's reason it is refused too: an available
     * transport that carries nothing must declare that, not imply it */
    scratch_from_declared(&s);
    s.c[0].state = NP_C_UNSUPPORTED;
    s.c[0].reason = "hypothetical incompatibility";
    s.c[0].waiver = NULL;
    s.view.n_pairs = 0;
    expect_refused("d16_carries_nothing_without_transport_reason", &s);

    /* a participating transport may NOT carry a stale unusable reason */
    scratch_from_declared(&s);
    s.t[0].unusable_reason = "left over from an earlier state";
    expect_refused("stale_unusable_reason", &s);

    /* a REGISTERED-BUT-UNAVAILABLE row is representable, and must say why:
     * an ALPN registered upstream with no profile built here. */
    scratch_from_declared(&s);
    s.t[3] = s.t[0];
    s.t[3].draft = 20;
    s.t[3].alpn = "moqt-20";
    s.t[3].alpn_len = 7;
    s.t[3].state = NP_T_ABSENT;
    s.t[3].endpoint_offered = false;
    s.t[3].auto_rank = NP_AUTO_RANK_NONE;
    s.t[3].unusable_reason = "registered ALPN; the draft-20 profile is not "
                            "built in this configuration";
    s.view.n_transports = 4;
    /* its cells must still exist -- one per transport x media */
    s.c[6] = s.c[2]; s.c[6].draft = 20; s.c[6].media = "loc01";
    s.c[6].state = NP_C_UNSUPPORTED;
    s.c[6].reason = "no draft-20 profile in this build";
    s.c[6].waiver = NULL;
    s.c[7] = s.c[2]; s.c[7].draft = 20;
    s.view.n_cells = 8;
    MOQ_TEST_CHECK_EQ_INT(np_topology_validate(&s.view, true), 0);
    /* ... and dropping its reason is refused */
    s.t[3].unusable_reason = NULL;
    expect_refused("absent_row_without_reason", &s);
    /* ... and with the pair row left behind it is refused */
    scratch_from_declared(&s);
    s.c[0].state = NP_C_UNSUPPORTED; s.c[0].reason = "r"; s.c[0].waiver = NULL;
    expect_refused("stale_pair_row", &s);

    /* pair table */
    scratch_from_declared(&s); s.p[0].draft_a = 18; s.p[0].draft_b = 16;
    expect_refused("pair_not_canonical", &s);
    scratch_from_declared(&s); s.view.n_pairs = 0;
    expect_refused("missing_pair", &s);
    scratch_from_declared(&s); s.p[1] = s.p[0]; s.view.n_pairs = 2;
    expect_refused("duplicate_pair", &s);
    scratch_from_declared(&s); s.p[0].relation = NP_R_UNKNOWN;
    expect_refused("pair_relation_unknown", &s);
    /* a pair naming a media profile THIS topology does not declare. The
     * earlier spelling of this check consulted the global declared table and
     * was short-circuited by a non-NULL test, so it could never fail. */
    scratch_from_declared(&s); s.p[0].media = "loc99";
    expect_refused("pair_media_undeclared", &s);

    MOQ_TEST_PASS("np_topology_negatives");
}

/* ===================================================================== *
 * 4. The corpus, and the relation DERIVED from it
 * ===================================================================== */
/*
 * THE RECORD ORACLE, factored so it can be driven directly.
 *
 * Each record kind names its own semantic facts -- in particular a complete
 * even property names its ABSOLUTE TYPE in its token (even_t2/4/6), so the
 * Type is never inferred from the bytes being checked. Returns the derived
 * byte count, or 0 for an unknown kind.
 */
static uint64_t even_token_type(const char *property)
{
    if (strcmp(property, "even_t2") == 0) return 2;
    if (strcmp(property, "even_t4") == 0) return 4;
    if (strcmp(property, "even_t6") == 0) return 6;
    return 0;
}

static size_t derive_record(const np_corpus_rec_t *r,
                            const np_transport_row_t *tr,
                            uint8_t *buf, size_t cap)
{
    if (!r || !tr || !buf) return 0;

    if (strcmp(r->property, "timestamp") == 0 ||
        strcmp(r->property, "type_delta") == 0)
        return np_encode(tr->enc, r->value, buf);

    uint64_t etype = even_token_type(r->property);
    if (etype != 0)
        return np_encode_prop_int(tr->enc, 0, etype, r->value, buf, cap);

    if (strcmp(r->property, "odd_hdr") == 0)
        return np_encode_prop_bytes_header(tr->enc, 0, 3, r->value, buf, cap);

    if (strcmp(r->property, "odd_prop") == 0) {
        uint8_t payload[8];
        if (r->value > sizeof(payload)) return 0;
        for (size_t k = 0; k < r->value; k++)
            payload[k] = (uint8_t)(0xa0 + k);
        return np_encode_prop_bytes(tr->enc, 0, 3, payload,
                                    (size_t)r->value, buf, cap);
    }

    /* previous Type 3 (ODD) -> Type 4 (EVEN): delta 1, integer form */
    if (strcmp(r->property, "after_odd") == 0)
        return np_encode_prop_int(tr->enc, 3, 4, r->value, buf, cap);

    /*
     * Two consecutive properties of the SAME Type 2 -- deliberately, so the
     * second carries delta 0. That shape is kept as-is rather than varied for
     * its own sake: delta 0 is legal and worth pinning, and the point of the
     * record is the one-byte desynchronization, not the Type sequence.
     */
    if (strcmp(r->property, "desync_2prop") == 0) {
        size_t n = np_encode_prop_int(tr->enc, 0, 2, r->value, buf, cap);
        if (n == 0) return 0;
        size_t n2 = np_encode_prop_int(tr->enc, 2, 2, 7, buf + n, cap - n);
        return n2 == 0 ? 0 : n + n2;
    }

    return 0;   /* an unknown record kind derives nothing */
}

static np_corpus_t g_corpus;

/*
 * The checked-in corpus's REVIEWED count and digest, spelled here as literals.
 * The C++ and Swift readers carry the same two numbers independently; a
 * C-only lane must be able to catch a semantically valid corpus edit on its
 * own, so these are not computed from the file at configure time.
 */
#define NP_EXPECT_COUNT  79u
#define NP_EXPECT_DIGEST UINT64_C(0xaf21b87308068ae6)

static void t_corpus(void)
{
    const char *why = NULL;
    MOQ_TEST_CHECK_EQ_INT(np_corpus_load(NP_CORPUS_PATH, &g_corpus, &why), 0);
    if (why) fprintf(stderr, "  corpus rejected: %s\n", why);
    MOQ_TEST_CHECK_EQ_SIZE(g_corpus.n, (size_t)NP_EXPECT_COUNT);
    MOQ_TEST_CHECK_EQ_U64(g_corpus.digest, NP_EXPECT_DIGEST);
    printf("  corpus: %zu records, %zu bytes, digest 0x%016" PRIx64 "\n",
           g_corpus.n, g_corpus.file_len, g_corpus.digest);

    /* EVERY row is re-derived by the oracle: the corpus is a transcript of
     * the drafts, not a second source of truth. */
    for (size_t i = 0; i < g_corpus.n; i++) {
        const np_corpus_rec_t *r = &g_corpus.recs[i];
        const np_transport_row_t *tr =
            np_transport_by_draft(strcmp(r->transport, "d16") == 0 ? 16 : 18);
        MOQ_TEST_CHECK(tr != NULL);
        if (!tr) continue;
        uint8_t buf[NP_CORPUS_MAX_BYTES];
        size_t n = derive_record(r, tr, buf, sizeof(buf));
        MOQ_TEST_CHECK(n > 0);
        MOQ_TEST_CHECK(n == r->n_bytes &&
                       memcmp(buf, r->bytes, n == r->n_bytes ? n : 0) == 0);
    }

    /*
     * RELATION DERIVATION, PER PAIR. Each declared pair is checked against
     * evidence filtered by its own (media, draft_a, draft_b), so one pair's
     * difference can never authorize another's declared relation.
     */
    static np_rel_rec_t rel[NP_CORPUS_MAX_RECORDS];
    for (size_t i = 0; i < g_corpus.n; i++) {
        rel[i].transport = g_corpus.recs[i].transport;
        rel[i].media     = g_corpus.recs[i].media;
        rel[i].property  = g_corpus.recs[i].property;
        rel[i].value     = g_corpus.recs[i].value;
        rel[i].bytes     = g_corpus.recs[i].bytes;
        rel[i].n_bytes   = g_corpus.recs[i].n_bytes;
    }
    np_topology_t declared = np_declared_topology();
    MOQ_TEST_CHECK_EQ_INT(
        np_pairs_check_relations(&declared, rel, g_corpus.n, false), 0);

    size_t npairs = 0;
    const np_pair_row_t *pairs = np_pairs(&npairs);
    for (size_t p = 0; p < npairs; p++) {
        np_pair_evidence_t e = np_pair_evidence(&pairs[p], rel, g_corpus.n);
        printf("  pair %s %u/%u: %zu shared, %zu differing\n",
               pairs[p].media, pairs[p].draft_a, pairs[p].draft_b,
               e.shared, e.differing);
        MOQ_TEST_CHECK(e.shared > 0);
        /* the agreement band is real: not everything differs */
        MOQ_TEST_CHECK(e.differing < e.shared);
    }

    MOQ_TEST_PASS("np_corpus");
}

/* ===================================================================== *
 * 5. Corpus grammar negatives (quiet) + FNV known answers
 * ===================================================================== */
static void expect_corpus_rejected(const char *name, const char *text)
{
    np_corpus_t c;
    const char *why = NULL;
    if (np_corpus_parse(text, strlen(text), &c, &why) == 0) {
        fprintf(stderr, "FAIL: corpus[%s] was ACCEPTED\n", name);
        failures++;
    }
}

/* Length-aware variant: the input CONTAINS a NUL, so its length cannot come
 * from strlen -- which is exactly the bug being tested for. */
static void expect_corpus_rejected_n(const char *name, const char *text,
                                     size_t len)
{
    np_corpus_t c;
    const char *why = NULL;
    if (np_corpus_parse(text, len, &c, &why) == 0) {
        fprintf(stderr, "FAIL: corpus[%s] was ACCEPTED\n", name);
        failures++;
    }
}

/* Build a record line with `nbytes` hex-encoded value bytes, to probe the
 * declared per-record byte cap at its exact boundary. */
static void expect_bytes_cap(size_t nbytes, bool want_accept)
{
    char buf[64 + 4 * NP_CORPUS_MAX_BYTES];
    size_t off = (size_t)snprintf(buf, sizeof(buf),
        "np-corpus 1\ncount 1\nd16 loc01 timestamp 1 ");
    for (size_t i = 0; i < nbytes && off + 3 < sizeof(buf); i++) {
        buf[off++] = 'a';
        buf[off++] = '0';
    }
    off += (size_t)snprintf(buf + off, sizeof(buf) - off, "\nend\n");
    np_corpus_t c;
    const char *why = NULL;
    bool accepted = np_corpus_parse(buf, off, &c, &why) == 0;
    if (accepted != want_accept) {
        fprintf(stderr, "FAIL: corpus byte cap at %zu bytes: %s\n", nbytes,
                accepted ? "ACCEPTED but should not be"
                         : "REJECTED but should be accepted");
        failures++;
    }
}

static void t_corpus_negatives(void)
{
    /* a minimal VALID corpus, so the negatives are single-fact deltas */
    static const char kOK[] =
        "np-corpus 1\ncount 2\n"
        "d16 loc01 timestamp 64 4040\n"
        "d18 loc01 timestamp 64 40\n"
        "end\n";
    np_corpus_t c;
    const char *why = NULL;
    MOQ_TEST_CHECK_EQ_INT(np_corpus_parse(kOK, strlen(kOK), &c, &why), 0);
    MOQ_TEST_CHECK_EQ_SIZE(c.n, 2);

    expect_corpus_rejected("count_short",
        "np-corpus 1\ncount 1\n"
        "d16 loc01 timestamp 64 4040\nd18 loc01 timestamp 64 40\nend\n");
    expect_corpus_rejected("count_long",
        "np-corpus 1\ncount 3\n"
        "d16 loc01 timestamp 64 4040\nd18 loc01 timestamp 64 40\nend\n");
    expect_corpus_rejected("duplicate_key",
        "np-corpus 1\ncount 2\n"
        "d16 loc01 timestamp 64 4040\nd16 loc01 timestamp 64 4040\nend\n");
    expect_corpus_rejected("uppercase_hex",
        "np-corpus 1\ncount 1\nd16 loc01 timestamp 64 4A40\nend\n");
    expect_corpus_rejected("odd_hex",
        "np-corpus 1\ncount 1\nd16 loc01 timestamp 64 404\nend\n");
    expect_corpus_rejected("non_hex",
        "np-corpus 1\ncount 1\nd16 loc01 timestamp 64 40zz\nend\n");
    expect_corpus_rejected("empty_hex",
        "np-corpus 1\ncount 1\nd16 loc01 timestamp 64 \nend\n");
    expect_corpus_rejected("unknown_transport",
        "np-corpus 1\ncount 1\nd17 loc01 timestamp 64 4040\nend\n");
    expect_corpus_rejected("unknown_media",
        "np-corpus 1\ncount 1\nd16 loc02 timestamp 64 4040\nend\n");
    expect_corpus_rejected("unknown_property",
        "np-corpus 1\ncount 1\nd16 loc01 timestampx 64 4040\nend\n");
    expect_corpus_rejected("leading_zero_value",
        "np-corpus 1\ncount 1\nd16 loc01 timestamp 064 4040\nend\n");
    expect_corpus_rejected("negative_value",
        "np-corpus 1\ncount 1\nd16 loc01 timestamp -1 4040\nend\n");
    expect_corpus_rejected("truncated_final_line",
        "np-corpus 1\ncount 1\nd16 loc01 timestamp 64 4040\nend");
    expect_corpus_rejected("trailing_record",
        "np-corpus 1\ncount 1\nd16 loc01 timestamp 64 4040\n"
        "d18 loc01 timestamp 64 40\nend\n");
    expect_corpus_rejected("trailing_garbage",
        "np-corpus 1\ncount 1\nd16 loc01 timestamp 64 4040\nend\nx\n");
    expect_corpus_rejected("missing_terminator",
        "np-corpus 1\ncount 1\nd16 loc01 timestamp 64 4040\n");
    expect_corpus_rejected("bad_magic",
        "np-corpus 2\ncount 1\nd16 loc01 timestamp 64 4040\nend\n");
    expect_corpus_rejected("double_space",
        "np-corpus 1\ncount 1\nd16  loc01 timestamp 64 4040\nend\n");
    expect_corpus_rejected("tab_separator",
        "np-corpus 1\ncount 1\nd16\tloc01 timestamp 64 4040\nend\n");
    expect_corpus_rejected("extra_field",
        "np-corpus 1\ncount 1\nd16 loc01 timestamp 64 4040 x\nend\n");
    expect_corpus_rejected("missing_field",
        "np-corpus 1\ncount 1\nd16 loc01 timestamp 4040\nend\n");
    expect_corpus_rejected("empty_input", "");
    expect_corpus_rejected("count_zero", "np-corpus 1\ncount 0\nend\n");

    /*
     * EMBEDDED NUL in each closed-token field. A reader that copies the span
     * into a C string and then uses strcmp accepts "d16\0x" as "d16"; the
     * independent Swift reader keeps the byte and rejects it, so the two would
     * disagree about the same file. Lengths are explicit here -- using strlen
     * would truncate the very input under test.
     */
    {
        static const char kNulTransport[] =
            "np-corpus 1\ncount 1\nd16\0x loc01 timestamp 1 40\nend\n";
        static const char kNulMedia[] =
            "np-corpus 1\ncount 1\nd16 loc01\0x timestamp 1 40\nend\n";
        static const char kNulProperty[] =
            "np-corpus 1\ncount 1\nd16 loc01 timestamp\0x 1 40\nend\n";
        expect_corpus_rejected_n("embedded_nul_transport", kNulTransport,
                                 sizeof(kNulTransport) - 1);
        expect_corpus_rejected_n("embedded_nul_media", kNulMedia,
                                 sizeof(kNulMedia) - 1);
        expect_corpus_rejected_n("embedded_nul_property", kNulProperty,
                                 sizeof(kNulProperty) - 1);
    }

    /* the DECLARED per-record byte cap, at its exact boundary */
    expect_bytes_cap(NP_CORPUS_MAX_BYTES, true);        /* 64 accepted */
    expect_bytes_cap(NP_CORPUS_MAX_BYTES + 1, false);   /* 65 refused  */

    /* FNV-1a 64 KNOWN ANSWERS -- the same two every reader must reproduce. */
    MOQ_TEST_CHECK_EQ_U64(np_fnv1a64("", 0), NP_FNV1A64_OFFSET);
    MOQ_TEST_CHECK_EQ_U64(np_fnv1a64("abc", 3), UINT64_C(0xe71fa2190541574b));
    /* a changed byte changes the digest */
    MOQ_TEST_CHECK(np_fnv1a64("abd", 3) != np_fnv1a64("abc", 3));
    /* and a truncation does too */
    MOQ_TEST_CHECK(np_fnv1a64("ab", 2) != np_fnv1a64("abc", 3));

    MOQ_TEST_PASS("np_corpus_negatives");
}

/* ===================================================================== *
 * 6. The future-draft DRIFT GATE (core half)
 *
 * Sweeps every moq_version_t value 0..255 and compares the declared rows
 * against core profile availability and the Step-3 canonical ALPN table in
 * BOTH directions. The ALPN literals are the authority: a round trip through
 * the product alone would pass with two consistently wrong strings.
 * ===================================================================== */
static void t_drift_gate(void)
{
    for (unsigned v = 0; v <= 255; v++) {
        moq_version_t ver = (moq_version_t)v;
        const np_transport_row_t *row = np_transport_by_draft(v);
        /* AVAILABLE means wire ready: a profile that exists only for the
         * simulator (draft 21 while transitional) is declared ABSENT. */
        bool product_has_profile = moq_profile_wire_ready(ver);

        /* declared AVAILABLE <=> the core has a wire-ready profile for it */
        if (row && row->state == NP_T_AVAILABLE) {
            if (!product_has_profile) {
                fprintf(stderr, "FAIL: drift: draft %u declared AVAILABLE but "
                                "core has no profile\n", v);
                failures++;
            }
        } else if (product_has_profile) {
            fprintf(stderr, "FAIL: drift: core has a profile for draft %u but "
                            "no row declares it AVAILABLE\n", v);
            failures++;
        }

        /* declared ALPN <=> the Step-3 table, in both directions */
        const char *product_alpn = moq_alpn_for_version(ver);
        if (row) {
            MOQ_TEST_CHECK(product_alpn != NULL);
            MOQ_TEST_CHECK(product_alpn && strcmp(product_alpn, row->alpn) == 0);
            moq_version_t back = (moq_version_t)0;
            MOQ_TEST_CHECK(moq_alpn_to_version(row->alpn, row->alpn_len, &back));
            MOQ_TEST_CHECK(back == ver);
        } else if (product_alpn != NULL) {
            fprintf(stderr, "FAIL: drift: the ALPN table maps draft %u to "
                            "\"%s\" but no transport row declares it\n",
                    v, product_alpn);
            failures++;
        }
    }

    /* the ALPN table declares nothing beyond the declared rows */
    size_t n_alpn = sizeof(moq_alpn_table) / sizeof(moq_alpn_table[0]);
    size_t nt = 0;
    (void)np_transports(&nt);
    MOQ_TEST_CHECK_EQ_SIZE(n_alpn, nt);
    for (size_t i = 0; i < n_alpn; i++) {
        const np_transport_row_t *row =
            np_transport_by_draft((uint32_t)moq_alpn_table[i].version);
        MOQ_TEST_CHECK(row != NULL);
        MOQ_TEST_CHECK(row && (size_t)moq_alpn_table[i].len ==
                              (size_t)row->alpn_len);
        MOQ_TEST_CHECK(row && strcmp(moq_alpn_table[i].alpn, row->alpn) == 0);
    }

    /* AVAILABLE, endpoint_offered and auto_rank are INDEPENDENT facts: the
     * declared rows must not encode one as a function of another. The
     * endpoint/AUTO comparison itself needs the service tier and lives in
     * service/tests/test_negotiated_profile_offer.c. */
    {
        np_scratch_t s;
        scratch_from_declared(&s);
        s.t[0].endpoint_offered = false;   /* still AVAILABLE, still ranked */
        expect_refused("offer_independent_of_available", &s);
    }

    MOQ_TEST_PASS("np_drift_gate");
}

/* ===================================================================== *
 * 4b. A SYNTHETIC two-pair topology, so per-pair isolation is proven
 *     rather than asserted. Pair A (16/18) diverges; pair B (18/20) reuses.
 *     The on-disk corpus grammar is untouched: these records are in-memory
 *     np_rel_rec_t values, not corpus text.
 * ===================================================================== */
static void t_pair_isolation(void)
{
    static const uint8_t b_1[1]    = { 0x40 };
    static const uint8_t b_2[2]    = { 0x40, 0x40 };
    static const uint8_t b_same[1] = { 0x07 };

    /*
     * ONE record set, TWO pairs. Under (16,18) the shared key
     * (loc01, timestamp, 64) has different literals -> DIVERGES. Under
     * (18,20) the shared key (loc01, timestamp, 7) has identical literals
     * -> REUSED. Both are checked in a single run, so a checker that pooled
     * the evidence would let pair A's difference satisfy pair B.
     */
    static const np_rel_rec_t recs[] = {
        { "d16", "loc01", "timestamp", 64, b_2,    2 },
        { "d18", "loc01", "timestamp", 64, b_1,    1 },
        { "d18", "loc01", "timestamp",  7, b_same, 1 },
        { "d20", "loc01", "timestamp",  7, b_same, 1 },
    };
    static const size_t n = sizeof(recs) / sizeof(recs[0]);

    np_pair_row_t pairs[2] = {
        { "loc01", 16, 18, NP_R_ENCODING_DIVERGES },
        { "loc01", 18, 20, NP_R_ENCODING_REUSED   },
    };
    np_topology_t t;
    memset(&t, 0, sizeof(t));
    t.pairs = pairs; t.n_pairs = 2;

    /* both declarations hold against their own evidence */
    MOQ_TEST_CHECK_EQ_INT(np_pairs_check_relations(&t, recs, n, true), 0);
    {
        np_pair_evidence_t a = np_pair_evidence(&pairs[0], recs, n);
        np_pair_evidence_t b = np_pair_evidence(&pairs[1], recs, n);
        MOQ_TEST_CHECK(a.shared == 1 && a.differing == 1);
        MOQ_TEST_CHECK(b.shared == 1 && b.differing == 0);
    }

    /* MUTATING EITHER RELATION fails, and only that one */
    pairs[0].relation = NP_R_ENCODING_REUSED;
    MOQ_TEST_CHECK_EQ_INT(np_pairs_check_relations(&t, recs, n, true), 1);
    pairs[0].relation = NP_R_ENCODING_DIVERGES;

    pairs[1].relation = NP_R_ENCODING_DIVERGES;
    MOQ_TEST_CHECK_EQ_INT(np_pairs_check_relations(&t, recs, n, true), 1);
    pairs[1].relation = NP_R_ENCODING_REUSED;

    /* THE AGGREGATION BUG, as a test: pair B declared DIVERGES must not be
     * satisfied by pair A's difference, and vice versa. Both mutations above
     * report exactly ONE problem, not zero and not two. */

    /* AN UNKNOWN RELATION is refused BY THIS HELPER, not by a validator the
     * other probes here deliberately bypass. */
    {
        np_pair_row_t unknown[1] = { { "loc01", 16, 18, NP_R_UNKNOWN } };
        np_topology_t tu;
        memset(&tu, 0, sizeof(tu));
        tu.pairs = unknown; tu.n_pairs = 1;
        MOQ_TEST_CHECK_EQ_INT(np_pairs_check_relations(&tu, recs, n, true), 1);
        /* including when there IS shared evidence, which is the case that
         * previously slipped through with neither branch matching */
        unknown[0].relation = (np_relation_t)77;
        MOQ_TEST_CHECK_EQ_INT(np_pairs_check_relations(&tu, recs, n, true), 1);
    }

    /* a pair with ZERO shared keys fails rather than passing vacuously */
    {
        np_pair_row_t lonely[1] = { { "loc01", 16, 20, NP_R_ENCODING_REUSED } };
        np_topology_t tl;
        memset(&tl, 0, sizeof(tl));
        tl.pairs = lonely; tl.n_pairs = 1;
        MOQ_TEST_CHECK_EQ_INT(np_pairs_check_relations(&tl, recs, n, true), 1);
        MOQ_TEST_CHECK_EQ_INT(np_pairs_check_relations(&t, NULL, 0, true), 2);
    }

    MOQ_TEST_PASS("np_pair_isolation");
}

/* ===================================================================== *
 * 4c. THE SEMANTIC TYPE IS DECLARED, NOT INFERRED.
 *
 * Driven through derive_record() directly, so no digest or grammar gate is
 * involved: a record whose token says even_t2 but whose bytes carry a valid,
 * SAME-LENGTH Type-4 prefix must be rejected by the re-derivation itself.
 * Under the old one-token grammar the checker tried Types 2, 4 and 6 and
 * accepted whichever matched, so those bytes authorized themselves.
 * ===================================================================== */
static void t_semantic_type_declared(void)
{
    const np_transport_row_t *d18 = np_transport_by_draft(18);
    MOQ_TEST_CHECK(d18 != NULL);
    if (!d18) return;

    uint8_t want2[8], want4[8], got[8];
    size_t n2 = np_encode_prop_int(d18->enc, 0, 2, 7, want2, sizeof(want2));
    size_t n4 = np_encode_prop_int(d18->enc, 0, 4, 7, want4, sizeof(want4));
    MOQ_TEST_CHECK(n2 == n4 && n2 == 2);          /* same length, different bytes */
    MOQ_TEST_CHECK(memcmp(want2, want4, n2) != 0);

    /* a well-formed even_t2 record derives its own bytes */
    np_corpus_rec_t rec;
    memset(&rec, 0, sizeof(rec));
    snprintf(rec.transport, sizeof(rec.transport), "d18");
    snprintf(rec.media, sizeof(rec.media), "loc01");
    snprintf(rec.property, sizeof(rec.property), "even_t2");
    rec.value = 7;
    memcpy(rec.bytes, want2, n2);
    rec.n_bytes = n2;
    size_t n = derive_record(&rec, d18, got, sizeof(got));
    MOQ_TEST_CHECK(n == rec.n_bytes && memcmp(got, rec.bytes, n) == 0);

    /* THE MUTANT: keep the token, swap in the valid Type-4 prefix. The
     * derivation still produces the Type-2 bytes, so it MUST disagree. */
    memcpy(rec.bytes, want4, n4);
    n = derive_record(&rec, d18, got, sizeof(got));
    MOQ_TEST_CHECK(n == n2);
    MOQ_TEST_CHECK(memcmp(got, rec.bytes, n) != 0);

    /* and the mirror: an even_t4 token with Type-2 bytes */
    snprintf(rec.property, sizeof(rec.property), "even_t4");
    memcpy(rec.bytes, want2, n2);
    n = derive_record(&rec, d18, got, sizeof(got));
    MOQ_TEST_CHECK(n == n4 && memcmp(got, rec.bytes, n) != 0);

    /* an unknown record kind derives NOTHING rather than guessing */
    snprintf(rec.property, sizeof(rec.property), "even_t9");
    MOQ_TEST_CHECK_EQ_SIZE(derive_record(&rec, d18, got, sizeof(got)), 0);

    MOQ_TEST_PASS("np_semantic_type_declared");
}

int main(void)
{
    t_oracle();
    t_tables();
    t_topology_negatives();
    t_corpus();
    t_pair_isolation();
    t_semantic_type_declared();
    t_corpus_negatives();
    t_drift_gate();

    printf("%s: %d failures\n", failures ? "FAIL" : "PASS", failures);
    return failures ? 1 : 0;
}
