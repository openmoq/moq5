#include "np_tables.h"

#include <stdio.h>
#include <string.h>

/* =====================================================================
 * The declared topology.
 *
 * Transport rows: both built drafts are AVAILABLE, with the reviewed ALPN
 * literals (Step 3's canonical table is the authority the drift gate compares
 * against), the integer codec each draft specifies, and the endpoint-offer
 * facts. AUTO ranks are 1-based and D18 is offered FIRST.
 *
 * Media rows: LOC-01 is implemented; LOC-02 is RESERVED with a reason. The
 * reviewed property IDs are recorded because they are the collision at the
 * heart of the arc -- LOC-01 Capture Timestamp is ID 2
 * (draft-ietf-moq-loc-01 §2.3.1.1) while LOC-02 Timestamp is ID 0x06
 * (draft-ietf-moq-loc-02 §2.3.1.1), which is LOC-01's AUDIO LEVEL id.
 * ===================================================================== */

static const np_transport_row_t kTransports[] = {
    /* draft, state, ALPN, len, codec, max, offered, rank, unusable reason */
    { 16, NP_T_AVAILABLE, "moqt-16", 7, NP_ENC_QUIC_VARINT,
      NP_QUIC_VARINT_MAX, true, 2, NULL },
    { 18, NP_T_AVAILABLE, "moqt-18", 7, NP_ENC_VI64,
      NP_VI64_MAX,        true, 1, NULL },
    /* Draft 21 has a core profile and a registered ALPN, but the profile is
     * still being converted from its draft-18 starting point (it emits draft-18
     * wire bytes), so it is not wire ready: ABSENT here means "no wire-ready
     * profile", the same notion the endpoint's supported set uses. LOC property
     * ids for draft 21 are also not implemented. */
    { 21, NP_T_ABSENT, "moqt-21", 7, NP_ENC_VI64,
      NP_VI64_MAX,        false, NP_AUTO_RANK_NONE,
      "draft-21 profile is transitional (it emits draft-18 wire bytes) and "
      "draft-21 LOC property ids are not implemented; not wire ready" },
};

static const np_media_row_t kMedias[] = {
    { "loc01", NP_M_IMPLEMENTED, NULL, 2 },
    { "loc02", NP_M_RESERVED,
      "draft-ietf-moq-loc-02 renumbers Timestamp to 0x06, which is LOC-01's "
      "Audio Level id; adopting it is a separate wire-visible decision and no "
      "product code implements it yet",
      6 },
};

/*
 * Closure cells. LOC-01 is SUPPORTED on both drafts but carries a temporary
 * closure waiver with both callbacks NULL: Step 5 declares the topology and
 * leaves the service closures to Step 6, which will replace these waivers
 * with the shared send/receive functions.
 */
static const np_cell_row_t kCells[] = {
    { 16, "loc01", NP_C_SUPPORTED, NULL,
      "step-5 substrate: service closure callbacks land in step 6",
      NULL, NULL },
    { 18, "loc01", NP_C_SUPPORTED, NULL,
      "step-5 substrate: service closure callbacks land in step 6",
      NULL, NULL },
    { 16, "loc02", NP_C_UNSUPPORTED,
      "LOC-02 is RESERVED: no product encoder or decoder exists for its "
      "renumbered property ids on any draft",
      NULL, NULL, NULL },
    { 18, "loc02", NP_C_UNSUPPORTED,
      "LOC-02 is RESERVED: no product encoder or decoder exists for its "
      "renumbered property ids on any draft",
      NULL, NULL, NULL },
    { 21, "loc01", NP_C_UNSUPPORTED,
      "draft-21 renumbers the provisional LOC property ids (Timestamp 0x10, "
      "Video Frame Marking 0x09) and no product code emits them yet",
      NULL, NULL, NULL },
    { 21, "loc02", NP_C_UNSUPPORTED,
      "LOC-02 is RESERVED: no product encoder or decoder exists for its "
      "renumbered property ids on any draft",
      NULL, NULL, NULL },
};

/*
 * The canonical unordered pair table: one row per unordered pair of drafts
 * that are SUPPORTED for a given media profile. LOC-01 has both drafts, so
 * exactly one pair; LOC-02 has none supported, so none.
 */
static const np_pair_row_t kPairs[] = {
    { "loc01", 16, 18, NP_R_ENCODING_DIVERGES },
};

#define NELEMS(a) (sizeof(a) / sizeof((a)[0]))

const np_transport_row_t *np_transports(size_t *n)
{ *n = NELEMS(kTransports); return kTransports; }
const np_media_row_t *np_medias(size_t *n)
{ *n = NELEMS(kMedias); return kMedias; }
const np_cell_row_t *np_cells(size_t *n)
{ *n = NELEMS(kCells); return kCells; }
const np_pair_row_t *np_pairs(size_t *n)
{ *n = NELEMS(kPairs); return kPairs; }

np_topology_t np_declared_topology(void)
{
    np_topology_t t;
    t.transports = kTransports; t.n_transports = NELEMS(kTransports);
    t.medias     = kMedias;     t.n_medias     = NELEMS(kMedias);
    t.cells      = kCells;      t.n_cells      = NELEMS(kCells);
    t.pairs      = kPairs;      t.n_pairs      = NELEMS(kPairs);
    return t;
}

const np_transport_row_t *np_transport_by_draft(uint32_t draft)
{
    for (size_t i = 0; i < NELEMS(kTransports); i++)
        if (kTransports[i].draft == draft) return &kTransports[i];
    return NULL;
}

const np_media_row_t *np_media_by_id(const char *id)
{
    if (!id) return NULL;
    for (size_t i = 0; i < NELEMS(kMedias); i++)
        if (strcmp(kMedias[i].id, id) == 0) return &kMedias[i];
    return NULL;
}

const np_cell_row_t *np_cell(uint32_t draft, const char *media)
{
    if (!media) return NULL;
    for (size_t i = 0; i < NELEMS(kCells); i++)
        if (kCells[i].draft == draft && strcmp(kCells[i].media, media) == 0)
            return &kCells[i];
    return NULL;
}

/* ---- validation -------------------------------------------------------- */

static bool nonempty(const char *s) { return s != NULL && s[0] != '\0'; }

static int vfail;
static bool vquiet;
#define VC(name, expr) do { if (!(expr)) { \
    if (!vquiet) fprintf(stderr, "FAIL: np_tables[%s]: %s:%d: %s\n", \
                         (name), __FILE__, __LINE__, #expr); \
    vfail++; } } while (0)

int np_topology_validate(const np_topology_t *t, bool quiet)
{
    vfail = 0;
    vquiet = quiet;
    if (!t) { VC("topology", false); return vfail; }

    /* --- transports: unique drafts, declared state, ALPN shape ---------- */
    VC("transport.count", t->n_transports > 0);
    for (size_t i = 0; i < t->n_transports; i++) {
        const np_transport_row_t *r = &t->transports[i];
        VC("transport.state", r->state == NP_T_AVAILABLE ||
                              r->state == NP_T_ABSENT);
        VC("transport.alpn", nonempty(r->alpn));
        VC("transport.alpn_len",
           r->alpn && strlen(r->alpn) == (size_t)r->alpn_len);
        VC("transport.enc", r->enc == NP_ENC_QUIC_VARINT ||
                            r->enc == NP_ENC_VI64);
        VC("transport.int_max",
           r->int_max == (r->enc == NP_ENC_QUIC_VARINT ? NP_QUIC_VARINT_MAX
                                                       : NP_VI64_MAX));
        for (size_t j = i + 1; j < t->n_transports; j++) {
            VC("transport.dup_draft", r->draft != t->transports[j].draft);
            VC("transport.dup_alpn",
               !(r->alpn && t->transports[j].alpn &&
                 strcmp(r->alpn, t->transports[j].alpn) == 0));
        }
    }

    /* --- AUTO: only AVAILABLE rows are offered; ranks are unique and form
     *     a contiguous 1..k with no gap; exact length matches the count. --- */
    {
        size_t offered = 0;
        for (size_t i = 0; i < t->n_transports; i++) {
            const np_transport_row_t *r = &t->transports[i];
            if (r->auto_rank != NP_AUTO_RANK_NONE) {
                offered++;
                VC("auto.only_available", r->state == NP_T_AVAILABLE);
                VC("auto.offered_implies_endpoint", r->endpoint_offered);
            }
            /*
             * THE AUTO MEMBERSHIP FACTS MUST AGREE FOR EVERY ROW, whatever its
             * state -- an earlier spelling applied this only to AVAILABLE rows,
             * so a registered-but-unavailable row could claim to be
             * endpoint-offered while carrying no rank and pass.
             *
             *   endpoint_offered == (auto_rank != NONE)
             *   ranked           => state == AVAILABLE
             *
             * This is a consistency CHECK between three independently declared
             * facts, not a derivation of one from another: each is still
             * written out per row, and a disagreement fails here rather than
             * being silently reconciled. An AVAILABLE row may still decline to
             * be offered -- both facts are then false, which agrees.
             */
            VC("auto.offered_iff_ranked",
               r->endpoint_offered == (r->auto_rank != NP_AUTO_RANK_NONE));
            VC("auto.ranked_implies_available",
               r->auto_rank == NP_AUTO_RANK_NONE ||
               r->state == NP_T_AVAILABLE);
            for (size_t j = i + 1; j < t->n_transports; j++)
                VC("auto.dup_rank",
                   r->auto_rank == NP_AUTO_RANK_NONE ||
                   r->auto_rank != t->transports[j].auto_rank);
        }
        for (uint8_t want = 1; want <= (uint8_t)offered; want++) {
            size_t hits = 0;
            for (size_t i = 0; i < t->n_transports; i++)
                if (t->transports[i].auto_rank == want) hits++;
            VC("auto.contiguous", hits == 1);
        }
        VC("auto.length", offered > 0);
    }

    /* --- the SUPPORTED-PARTICIPATION invariant --------------------------- *
     * Every AVAILABLE transport must carry at least one IMPLEMENTED media
     * profile, or declare in its own row why it cannot. An ABSENT row -- an
     * ALPN registered upstream with no profile built here -- must likewise say
     * so. A row that participates normally must NOT carry a reason, so a stale
     * reason cannot linger after a profile lands. (The cells are validated
     * further down; this loop needs only their state.)
     */
    for (size_t i = 0; i < t->n_transports; i++) {
        const np_transport_row_t *r = &t->transports[i];
        bool reason = nonempty(r->unusable_reason);
        if (r->state == NP_T_ABSENT) {
            VC("transport.absent_reason", reason);
            continue;
        }
        size_t carries = 0;
        for (size_t c = 0; c < t->n_cells; c++) {
            if (t->cells[c].draft != r->draft) continue;
            if (t->cells[c].state != NP_C_SUPPORTED) continue;
            for (size_t j = 0; j < t->n_medias; j++)
                if (t->cells[c].media && t->medias[j].id &&
                    strcmp(t->cells[c].media, t->medias[j].id) == 0 &&
                    t->medias[j].state == NP_M_IMPLEMENTED) carries++;
        }
        if (carries == 0) VC("transport.unusable_reason", reason);
        else              VC("transport.no_stale_reason", !reason);
    }

    /* --- media rows ------------------------------------------------------ */
    VC("media.count", t->n_medias > 0);
    for (size_t i = 0; i < t->n_medias; i++) {
        const np_media_row_t *m = &t->medias[i];
        VC("media.id", nonempty(m->id));
        VC("media.state", m->state == NP_M_IMPLEMENTED ||
                          m->state == NP_M_RESERVED);
        if (m->state == NP_M_RESERVED)
            VC("media.reserved_reason", nonempty(m->reason));
        for (size_t j = i + 1; j < t->n_medias; j++)
            VC("media.dup_id", !(m->id && t->medias[j].id &&
                                 strcmp(m->id, t->medias[j].id) == 0));
    }

    /* --- cells: EXACTLY one per transport x media, nothing extra -------- */
    VC("cell.count", t->n_cells == t->n_transports * t->n_medias);
    for (size_t i = 0; i < t->n_transports; i++)
        for (size_t j = 0; j < t->n_medias; j++) {
            size_t hits = 0;
            for (size_t c = 0; c < t->n_cells; c++)
                if (t->cells[c].draft == t->transports[i].draft &&
                    t->cells[c].media && t->medias[j].id &&
                    strcmp(t->cells[c].media, t->medias[j].id) == 0)
                    hits++;
            VC("cell.exactly_one", hits == 1);
        }
    for (size_t c = 0; c < t->n_cells; c++) {
        const np_cell_row_t *cell = &t->cells[c];
        bool declared_transport = false, declared_media = false;
        for (size_t i = 0; i < t->n_transports; i++)
            if (t->transports[i].draft == cell->draft) declared_transport = true;
        for (size_t j = 0; j < t->n_medias; j++)
            if (cell->media && t->medias[j].id &&
                strcmp(cell->media, t->medias[j].id) == 0) declared_media = true;
        VC("cell.declared_transport", declared_transport);
        VC("cell.declared_media", declared_media);
        VC("cell.state", cell->state == NP_C_SUPPORTED ||
                         cell->state == NP_C_UNSUPPORTED);
        if (cell->state == NP_C_UNSUPPORTED) {
            VC("cell.unsupported_reason", nonempty(cell->reason));
            /* an unsupported cell requires no product participation */
            VC("cell.unsupported_no_callbacks",
               cell->send_closure == NULL && cell->recv_closure == NULL);
            VC("cell.unsupported_no_waiver", !nonempty(cell->waiver));
        } else {
            /* THE WAIVER RULE, exact: both callbacks or a non-empty waiver,
             * never a half state. */
            bool both_cb = cell->send_closure && cell->recv_closure;
            bool no_cb   = !cell->send_closure && !cell->recv_closure;
            bool waived  = nonempty(cell->waiver);
            VC("cell.waiver_exclusive",
               (both_cb && !waived) || (no_cb && waived));
            VC("cell.no_half_callbacks", both_cb || no_cb);
        }
    }
    /* a RESERVED media profile may not be SUPPORTED anywhere */
    for (size_t j = 0; j < t->n_medias; j++)
        if (t->medias[j].state == NP_M_RESERVED)
            for (size_t c = 0; c < t->n_cells; c++)
                if (t->cells[c].media && t->medias[j].id &&
                    strcmp(t->cells[c].media, t->medias[j].id) == 0)
                    VC("cell.reserved_unsupported",
                       t->cells[c].state == NP_C_UNSUPPORTED);
    /* an IMPLEMENTED media profile must be SUPPORTED somewhere: a profile
     * incompatible with every transport is not implemented, it is reserved */
    for (size_t j = 0; j < t->n_medias; j++) {
        if (t->medias[j].state != NP_M_IMPLEMENTED) continue;
        size_t supported = 0;
        for (size_t c = 0; c < t->n_cells; c++)
            if (t->cells[c].media && t->medias[j].id &&
                strcmp(t->cells[c].media, t->medias[j].id) == 0 &&
                t->cells[c].state == NP_C_SUPPORTED) supported++;
        VC("cell.implemented_has_support", supported > 0);
    }

    /* --- pairs: exactly n(n-1)/2 over the SUPPORTED drafts, per media ---- */
    for (size_t j = 0; j < t->n_medias; j++) {
        const char *media = t->medias[j].id;
        size_t sup = 0;
        for (size_t c = 0; c < t->n_cells; c++)
            if (t->cells[c].media && media &&
                strcmp(t->cells[c].media, media) == 0 &&
                t->cells[c].state == NP_C_SUPPORTED) sup++;
        size_t want = sup * (sup - 1) / 2;
        size_t have = 0;
        for (size_t p = 0; p < t->n_pairs; p++)
            if (t->pairs[p].media && media &&
                strcmp(t->pairs[p].media, media) == 0) have++;
        VC("pair.exact_coverage", have == want);
    }
    for (size_t p = 0; p < t->n_pairs; p++) {
        const np_pair_row_t *pr = &t->pairs[p];
        /* the pair's media must be declared in THIS topology -- the earlier
         * spelling consulted the global declared table and was additionally
         * short-circuited by a non-NULL test, so it could never fail */
        bool media_declared = false;
        for (size_t j = 0; j < t->n_medias; j++)
            if (pr->media && t->medias[j].id &&
                strcmp(pr->media, t->medias[j].id) == 0) media_declared = true;
        VC("pair.media_declared", media_declared);
        VC("pair.canonical_order", pr->draft_a < pr->draft_b);
        VC("pair.relation", pr->relation == NP_R_ENCODING_REUSED ||
                            pr->relation == NP_R_ENCODING_DIVERGES);
        /* both endpoints must be SUPPORTED cells of that media profile */
        for (int e = 0; e < 2; e++) {
            uint32_t d = e ? pr->draft_b : pr->draft_a;
            size_t hits = 0;
            for (size_t c = 0; c < t->n_cells; c++)
                if (t->cells[c].draft == d && t->cells[c].media && pr->media &&
                    strcmp(t->cells[c].media, pr->media) == 0 &&
                    t->cells[c].state == NP_C_SUPPORTED) hits++;
            VC("pair.endpoint_supported", hits == 1);
        }
        for (size_t q = p + 1; q < t->n_pairs; q++)
            VC("pair.dup",
               !(t->pairs[q].media && pr->media &&
                 strcmp(t->pairs[q].media, pr->media) == 0 &&
                 t->pairs[q].draft_a == pr->draft_a &&
                 t->pairs[q].draft_b == pr->draft_b));
    }

    return vfail;
}

int np_tables_validate(void)
{
    np_topology_t t = np_declared_topology();
    return np_topology_validate(&t, false);
}

/* ---- per-pair relation evidence ---------------------------------------- */

/* The corpus spells a draft as "d<number>". Formatted rather than table-driven
 * so a synthetic topology can name a draft the current tables do not declare
 * (which is exactly what the per-pair isolation probe needs). */
static bool draft_token(uint32_t draft, char *out, size_t cap)
{
    int n = snprintf(out, cap, "d%u", (unsigned)draft);
    return n > 0 && (size_t)n < cap;
}

np_pair_evidence_t np_pair_evidence(const np_pair_row_t *pair,
                                    const np_rel_rec_t *recs, size_t n)
{
    np_pair_evidence_t e = { 0, 0 };
    char ta[16], tb[16];
    if (!pair || !recs) return e;
    if (!draft_token(pair->draft_a, ta, sizeof(ta))) return e;
    if (!draft_token(pair->draft_b, tb, sizeof(tb))) return e;

    for (size_t i = 0; i < n; i++) {
        const np_rel_rec_t *a = &recs[i];
        if (!a->transport || strcmp(a->transport, ta) != 0) continue;
        if (!a->media || !pair->media) continue;
        if (strcmp(a->media, pair->media) != 0) continue;
        for (size_t j = 0; j < n; j++) {
            const np_rel_rec_t *b = &recs[j];
            if (!b->transport || strcmp(b->transport, tb) != 0) continue;
            if (!b->media || strcmp(b->media, pair->media) != 0) continue;
            if (!a->property || !b->property) continue;
            if (strcmp(a->property, b->property) != 0) continue;
            if (a->value != b->value) continue;
            e.shared++;
            if (a->n_bytes != b->n_bytes ||
                memcmp(a->bytes, b->bytes, a->n_bytes) != 0) e.differing++;
        }
    }
    return e;
}

int np_pairs_check_relations(const np_topology_t *t,
                             const np_rel_rec_t *recs, size_t n, bool quiet)
{
    int problems = 0;
    if (!t) return 1;
    for (size_t p = 0; p < t->n_pairs; p++) {
        const np_pair_row_t *pr = &t->pairs[p];
        /*
         * FAIL CLOSED on an unrecognized relation. This helper is called
         * directly -- including by tests that deliberately bypass
         * np_topology_validate -- so it cannot lean on that validator having
         * rejected NP_R_UNKNOWN first: with neither branch below matching, an
         * unknown relation would otherwise be reported as satisfied.
         */
        if (pr->relation != NP_R_ENCODING_REUSED &&
            pr->relation != NP_R_ENCODING_DIVERGES) {
            if (!quiet)
                fprintf(stderr, "FAIL: pair[%s %u/%u]: relation %d is not a "
                                "declared relation\n",
                        pr->media ? pr->media : "(null)",
                        pr->draft_a, pr->draft_b, (int)pr->relation);
            problems++;
            continue;
        }
        np_pair_evidence_t e = np_pair_evidence(pr, recs, n);
        if (e.shared == 0) {
            if (!quiet)
                fprintf(stderr, "FAIL: pair[%s %u/%u]: no shared semantic key "
                                "-- nothing to have a relation about\n",
                        pr->media ? pr->media : "(null)",
                        pr->draft_a, pr->draft_b);
            problems++;
            continue;
        }
        if (pr->relation == NP_R_ENCODING_DIVERGES && e.differing == 0) {
            if (!quiet)
                fprintf(stderr, "FAIL: pair[%s %u/%u]: declared DIVERGES but "
                                "all %zu shared literals are equal\n",
                        pr->media, pr->draft_a, pr->draft_b, e.shared);
            problems++;
        }
        if (pr->relation == NP_R_ENCODING_REUSED && e.differing != 0) {
            if (!quiet)
                fprintf(stderr, "FAIL: pair[%s %u/%u]: declared REUSED but "
                                "%zu of %zu shared literals differ\n",
                        pr->media, pr->draft_a, pr->draft_b,
                        e.differing, e.shared);
            problems++;
        }
    }
    return problems;
}
