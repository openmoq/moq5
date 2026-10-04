/*
 * Negotiated-profile substrate: the DECLARED TOPOLOGY.
 *
 * Four separate tables, kept separate on purpose (0661 §5): a transport
 * profile is not a media profile, a compatibility cell is not either of them,
 * and the canonical unordered pair relation is a fact about two transports at
 * one media profile. Collapsing any of these would let a missing row hide
 * behind a present one.
 *
 * These rows are FIXTURE DECLARATIONS. They are compared against the product
 * (profile availability, endpoint offers, AUTO order, the ALPN table) by the
 * drift gate in the test; they are never generated from it.
 *
 * This header is product-free by construction: it names transport versions by
 * their draft number, not by a moq_* constant, so the oracle/table layer can
 * be compiled and symbol-scanned without the product.
 */
#ifndef NP_TABLES_H
#define NP_TABLES_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "np_oracle.h"

/* ---- transport rows ---------------------------------------------------- */

typedef enum {
    NP_T_UNKNOWN   = 0,
    NP_T_AVAILABLE = 1,   /* a wire-ready profile for this draft is linked in */
    NP_T_ABSENT    = 2,   /* registered, but no wire-ready profile here: not
                           * built, or built but still transitional */
} np_transport_state_t;

#define NP_AUTO_RANK_NONE 0   /* not in the AUTO offer at all */

typedef struct {
    uint32_t             draft;          /* 16, 18, ... */
    np_transport_state_t state;
    const char          *alpn;           /* reviewed literal, NUL-terminated */
    uint8_t              alpn_len;       /* wire length */
    np_enc_t             enc;            /* integer codec for this draft */
    uint64_t             int_max;        /* largest representable integer */
    bool                 endpoint_offered;
    uint8_t              auto_rank;      /* 1 = offered first; 0 = not offered */
    /*
     * Why this transport cannot carry media, when it cannot. REQUIRED
     * non-empty in exactly two situations, and forbidden otherwise:
     *
     *   - state == ABSENT: the ALPN is registered but no wire-ready profile
     *     is built here (none, or one still transitional), so the row exists
     *     to be compared against, not used;
     *   - state == AVAILABLE with no SUPPORTED cell against an IMPLEMENTED
     *     media profile: an available transport that carries nothing is a
     *     declaration, not an oversight, and must say why.
     *
     * A row that participates normally leaves this NULL.
     */
    const char          *unusable_reason;
} np_transport_row_t;

/* ---- media rows -------------------------------------------------------- */

typedef enum {
    NP_M_UNKNOWN     = 0,
    NP_M_IMPLEMENTED = 1,
    NP_M_RESERVED    = 2,   /* declared, deliberately not implemented yet */
} np_media_state_t;

typedef struct {
    const char      *id;        /* "loc01", "loc02" */
    np_media_state_t state;
    const char      *reason;    /* required non-empty when RESERVED */
    uint64_t         timestamp_property_id;   /* the reviewed property ID */
} np_media_row_t;

/* ---- compatibility cells: exactly one per transport x media ------------- */

typedef enum {
    NP_C_UNKNOWN     = 0,
    NP_C_SUPPORTED   = 1,
    NP_C_UNSUPPORTED = 2,
} np_cell_state_t;

/*
 * A cell's two closure callbacks are the future Step-6 service closures. In
 * Step 5 they are absent and a non-empty temporary WAIVER stands in their
 * place. The rule is exact and half-states fail:
 *
 *   callbacks BOTH present  <=> waiver absent
 *   callbacks BOTH absent   <=> waiver present and non-empty
 */
typedef void (*np_send_closure_fn)(void);
typedef void (*np_recv_closure_fn)(void);

typedef struct {
    uint32_t            draft;
    const char         *media;
    np_cell_state_t     state;
    const char         *reason;        /* required non-empty when UNSUPPORTED */
    const char         *waiver;       /* temporary Step-5 closure waiver */
    np_send_closure_fn  send_closure;
    np_recv_closure_fn  recv_closure;
} np_cell_row_t;

/* ---- canonical unordered pair relation, per media profile -------------- */

typedef enum {
    NP_R_UNKNOWN           = 0,
    NP_R_ENCODING_REUSED   = 1,   /* every shared literal is byte-equal */
    NP_R_ENCODING_DIVERGES = 2,   /* at least one shared literal differs */
} np_relation_t;

typedef struct {
    const char   *media;
    uint32_t      draft_a;   /* declared with draft_a < draft_b */
    uint32_t      draft_b;
    np_relation_t relation;
} np_pair_row_t;

/* ---- accessors --------------------------------------------------------- */

const np_transport_row_t *np_transports(size_t *n);
const np_media_row_t     *np_medias(size_t *n);
const np_cell_row_t      *np_cells(size_t *n);
const np_pair_row_t      *np_pairs(size_t *n);

const np_transport_row_t *np_transport_by_draft(uint32_t draft);
const np_media_row_t     *np_media_by_id(const char *id);
const np_cell_row_t      *np_cell(uint32_t draft, const char *media);

/*
 * Structural validation of the four tables against each other: exact
 * cross-product coverage, no duplicate or extra rows, required reasons, AUTO
 * rank contiguity/uniqueness, supported participation, exact n(n-1)/2 pair
 * coverage per media profile, and the waiver/callback rule. Returns the number
 * of problems; quiet on success.
 */
int np_tables_validate(void);

/* The same validator over CALLER-SUPPLIED tables, so the self-checks can feed
 * it deliberately broken topologies without touching the declared ones. */
typedef struct {
    const np_transport_row_t *transports; size_t n_transports;
    const np_media_row_t     *medias;     size_t n_medias;
    const np_cell_row_t      *cells;      size_t n_cells;
    const np_pair_row_t      *pairs;      size_t n_pairs;
} np_topology_t;

int np_topology_validate(const np_topology_t *t, bool quiet);

/* The declared topology as a np_topology_t, for self-checks that perturb a
 * copy of it. */
np_topology_t np_declared_topology(void);

/* ---- per-pair relation evidence ---------------------------------------- *
 * A pair's relation is DERIVED from the corpus, and it must be derived from
 * THAT PAIR's rows only: one global shared/differing tally would let a
 * difference under one pair authorize another pair's declared relation the
 * moment a third transport or a second implemented media profile appears.
 * These counts are therefore filtered by the pair's exact
 * (media, draft_a, draft_b).
 */
typedef struct {
    size_t shared;      /* semantic keys present under BOTH drafts */
    size_t differing;   /* of those, how many have differing literals */
} np_pair_evidence_t;

/*
 * Count the evidence for one pair over a caller-supplied record list. The
 * record shape is passed as parallel accessors so this stays free of the
 * corpus type (and so a synthetic in-memory corpus can be used).
 */
typedef struct {
    const char *transport;   /* "d16" / "d18" */
    const char *media;
    const char *property;
    uint64_t    value;
    const uint8_t *bytes;
    size_t      n_bytes;
} np_rel_rec_t;

np_pair_evidence_t np_pair_evidence(const np_pair_row_t *pair,
                                    const np_rel_rec_t *recs, size_t n);

/*
 * Check every declared pair against its OWN evidence. A pair with no shared
 * semantic key fails (there is nothing to have a relation about); DIVERGES
 * requires at least one differing literal; REUSED requires none. Returns the
 * number of problems and names each one unless quiet.
 */
int np_pairs_check_relations(const np_topology_t *t,
                             const np_rel_rec_t *recs, size_t n, bool quiet);

#endif /* NP_TABLES_H */
