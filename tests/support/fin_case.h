#ifndef MOQ_TEST_FIN_CASE_H
#define MOQ_TEST_FIN_CASE_H

/*
 * Shared FIN-lifecycle case descriptors.
 *
 * The session-side classes (owner / non-destination) live with the session
 * fixtures, but the BRIDGE class is used where the transport facilities are --
 * paired sessions, fake endpoints and endpoint-operation oracles -- so it lives
 * here rather than being duplicated or left dead in the session file.
 */

#include <stddef.h>

/* The session-side classes use this; the bridge class does not, so mark it
 * permissible-unused rather than duplicating it per consumer. */
#if defined(__GNUC__) || defined(__clang__)
#define MOQ_UNUSED_OK __attribute__((unused))
#else
#define MOQ_UNUSED_OK
#endif
#include "txn_snapshot.h"
#include "ownership_graph.h"

/* Every class drives the same output and mutation-inventory machinery, so
 * every descriptor must supply the full hook set: a missing normalizer would
 * silently skip the records it was meant to describe. */
MOQ_UNUSED_OK static int fin_hooks_problems(const txs_op_hooks_t *h)
{
    int bad = 0;
    if (!h->ctx) bad++;
    if (!h->capture) bad++;
    if (!h->check) bad++;
    if (!h->normalize_event) bad++;
    if (!h->normalize_action) bad++;
    return bad;
}

/*
 * Bridge-retirement case: the PHYSICAL FIN obligation, which is a different
 * fact from the session's semantic consumption and is asserted separately.
 * Its callbacks take the bridge, which is why it cannot share the session
 * descriptors.
 */
/* What the producer's ingress DOES with the FIN. A family whose handoff carries
 * the FIN consumes it in the same call; one whose destination cannot yet take
 * it retains the obligation on the bridge instead. The two need different
 * ingress results and different bridge flags, so the descriptor DECLARES which
 * it is rather than the runner inferring it from an incidental conditional. */
typedef enum fin_bridge_ingress {
    FIN_BR_CONSUMED = 1,   /* feed == MOQ_OK; FIN transferred to the owner */
    FIN_BR_RETAINED        /* feed == MOQ_ERR_WOULD_BLOCK; obligation retained */
} fin_bridge_ingress_t;

/*
 * The destination family: ONE object owning every family-varying part --
 * identity, snapshot size AND its writer/checker, output normalizers, edge and
 * wire oracles. Keeping them together is the point: a size paired with another
 * family's writer, or two disagreeing contexts, cannot be expressed.
 *
 * The family is a pure vtable. The per-run context lives on the CASE and is
 * handed to every callback, so there is exactly one context and nothing to
 * disagree with.
 */
#define FIN_BR_SNAP_MAX 1024

/* Alignment-safe opaque storage: a byte array would be under-aligned for the
 * family's real snapshot type. */
typedef union fin_bridge_snap {
    max_align_t _align;
    unsigned char bytes[FIN_BR_SNAP_MAX];
} fin_bridge_snap_t;

typedef struct fin_bridge_family {
    int       owner_kind;    /* MOQ_REQ_* */
    uint32_t  pool_tag;      /* MOQ_HANDLE_POOL_*, and must MATCH owner_kind */
    size_t    snap_size;     /* <= FIN_BR_SNAP_MAX */

    /* Snapshot. `cap` is the storage actually available, so a family whose
     * real state outgrew its declared size cannot overwrite the buffer. */
    void (*capture)(const moq_session_t *s, void *ctx, void *state, size_t cap);
    int  (*check)(const moq_session_t *s, void *ctx, const void *state,
                  size_t cap, const char *what);

    /* Output. */
    bool (*normalize_event)(const moq_event_t *ev, void *ctx,
                            txs_norm_vec_t *out);
    bool (*normalize_action)(const moq_action_t *a, void *ctx,
                             txs_norm_vec_t *out);
    int  (*want_request)(txs_norm_vec_t *v, uint64_t handle);

    /* Identity and lifecycle. */
    int  (*derive_slot)(const moq_session_t *s, uint32_t *out_gen);
    int  (*check_live)(const moq_session_t *s, moq_stream_ref_t ref,
                       int want_slot, uint32_t want_gen, uint64_t want_handle,
                       const char *what);
    int  (*check_retired)(const moq_session_t *s, moq_stream_ref_t ref,
                          int want_slot, const char *what);
    /* `live` selects the admitted edge set or the fully-retired (empty) one. */
    int  (*check_edges)(const og_graph_t *g, moq_stream_ref_t ref,
                        int want_slot, int live, const char *what);
    int  (*check_drain)(const moq_session_t *s, const char *what);
    int  (*check_terminal_wire)(void *ctx, const char *what);
} fin_bridge_family_t;

/* Only these (kind, pool) pairs are meaningful; an unknown kind, or a tag that
 * does not belong to it, is a descriptor error rather than a silent mismatch. */
static int fin_bridge_kind_pool_ok(int kind, uint32_t tag)
{
    switch (kind) {
    case MOQ_REQ_SUBSCRIPTION:     return tag == MOQ_HANDLE_POOL_SUBSCRIPTION;
    case MOQ_REQ_ANNOUNCEMENT:     return tag == MOQ_HANDLE_POOL_ANNOUNCEMENT;
    case MOQ_REQ_NAMESPACE_SUB:    return tag == MOQ_HANDLE_POOL_NAMESPACE_SUB;
    case MOQ_REQ_FETCH:            return tag == MOQ_HANDLE_POOL_FETCH;
    case MOQ_REQ_PUBLISH:          return tag == MOQ_HANDLE_POOL_PUBLICATION;
    case MOQ_REQ_TRACK_STATUS:     return tag == MOQ_HANDLE_POOL_TRACK_STATUS;
    case MOQ_REQ_SUBSCRIBE_TRACKS: return tag == MOQ_HANDLE_POOL_SUBSCRIBE_TRACKS;
    default:                       return 0;
    }
}


typedef struct fin_bridge_case {
    const char *name;
    void       *ctx;                        /* the ONE context */
    fin_bridge_ingress_t         ingress;
    const fin_bridge_family_t   *family;
    /* `transport_id` is the TRANSPORT stream identity the peer bytes arrive
     * on. It is deliberately a uint64_t and not a moq_stream_ref_t: the
     * session's internal ref is assigned by the bridge and is a DIFFERENT
     * value, so a descriptor taking a ref here would invite exactly the
     * conflation that makes owner lookups silently resolve to nothing. */
    moq_result_t (*feed)(moq_transport_bridge_t *b, void *ctx,
                         uint64_t transport_id, bool fin);
    moq_result_t (*terminal)(moq_transport_bridge_t *b, void *ctx,
                             uint64_t transport_id);
} fin_bridge_case_t;

static int fin_bridge_problems(const fin_bridge_case_t *f)
{
    int bad = 0;
    if (!f->name) bad++;
    if (!f->ctx) bad++;
    if (!f->feed) bad++;
    if (!f->terminal) bad++;
    if (f->ingress != FIN_BR_CONSUMED && f->ingress != FIN_BR_RETAINED) bad++;
    if (!f->family) bad++;
    else {
        const fin_bridge_family_t *o = f->family;
        if (!fin_bridge_kind_pool_ok(o->owner_kind, o->pool_tag)) bad++;
        if (o->snap_size == 0 || o->snap_size > FIN_BR_SNAP_MAX) bad++;
        if (!o->capture || !o->check || !o->normalize_event ||
            !o->normalize_action || !o->want_request || !o->derive_slot ||
            !o->check_live || !o->check_retired || !o->check_edges ||
            !o->check_drain || !o->check_terminal_wire) bad++;
    }
    return bad;
}

#endif /* MOQ_TEST_FIN_CASE_H */
