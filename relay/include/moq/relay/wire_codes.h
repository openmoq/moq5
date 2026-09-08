#ifndef MOQRELAY_WIRE_CODES_H
#define MOQRELAY_WIRE_CODES_H

/*
 * Terminal codes, carried by meaning rather than by number.
 *
 * One listener accepts draft-16 and draft-18 at once and routes between those
 * connections, and the two drafts disagree inside the same registry:
 * PUBLISH_DONE swaps EXPIRED and TOO_FAR_BEHIND, and the data-stream reset
 * registry moves UNKNOWN_OBJECT_STATUS while adding two codes draft-16 never
 * assigned. Forwarding a raw integer across that boundary therefore states
 * something the sender never said. Every cross-connection terminal is decoded
 * to a semantic in its own draft and re-encoded for the draft that will read
 * it, and a code with no reviewed meaning in the target draft fails closed to
 * that domain's INTERNAL_ERROR rather than acquiring a neighbour's meaning.
 *
 * Sources, verbatim: PUBLISH_DONE d16 Section 13.4.3 / d18 Section 15.10.3;
 * data-stream reset d16 Section 13.4.4 / d18 Section 15.10.4.
 */

#include <stdbool.h>
#include <stdint.h>

#include <moq/session.h>

#include <moq/relay/types.h>

#ifdef __cplusplus
extern "C" {
#endif

/* -- PUBLISH_DONE status ---------------------------------------------------- */

typedef enum moqr_pd_status {
    MOQR_PD_UNKNOWN = 0,        /* no reviewed meaning in the source draft */
    MOQR_PD_INTERNAL_ERROR,
    MOQR_PD_UNAUTHORIZED,
    MOQR_PD_TRACK_ENDED,
    MOQR_PD_SUBSCRIPTION_ENDED,
    MOQR_PD_GOING_AWAY,
    MOQR_PD_EXPIRED,
    MOQR_PD_TOO_FAR_BEHIND,
    MOQR_PD_UPDATE_FAILED,
    MOQR_PD_EXCESSIVE_LOAD,     /* draft-18 only */
    MOQR_PD_MALFORMED_TRACK,
} moqr_pd_status_t;

/* -- Data-stream reset ------------------------------------------------------ */

typedef enum moqr_reset_cause {
    MOQR_RESET_UNKNOWN = 0,
    MOQR_RESET_INTERNAL_ERROR,
    MOQR_RESET_CANCELLED,
    MOQR_RESET_DELIVERY_TIMEOUT,
    MOQR_RESET_SESSION_CLOSED,
    MOQR_RESET_UNKNOWN_OBJECT_STATUS,
    MOQR_RESET_GOING_AWAY,          /* draft-18 only */
    MOQR_RESET_TOO_FAR_BEHIND,      /* draft-18 only */
    MOQR_RESET_EXPIRED_AUTH_TOKEN,  /* draft-18 only */
    MOQR_RESET_EXCESSIVE_LOAD,      /* draft-18 only */
    MOQR_RESET_MALFORMED_TRACK,
} moqr_reset_cause_t;

/*
 * Decode a wire code in `version`'s registry. An unassigned code — and any
 * unsupported `version`, including the unattached sentinel 0 — decodes to
 * *_UNKNOWN. A code is never guessed into a neighbouring meaning, and an
 * unsupported draft is never read through draft-16's table.
 */
MOQR_CORE_API moqr_pd_status_t   moqr_pd_decode(moq_version_t version, uint64_t wire);
MOQR_CORE_API moqr_reset_cause_t moqr_reset_decode(moq_version_t version, uint64_t wire);

/*
 * Encode a semantic for `version`, writing the wire code to *out.
 *
 * These are result-bearing because `INTERNAL_ERROR` is 0x0 in both domains: a
 * plain return could not tell a caller whether it received a legitimate
 * INTERNAL_ERROR or a refusal. An unsupported `version` returns
 * MOQR_ERR_INVAL and leaves *out untouched.
 *
 * A meaning the target draft never assigned falls back to the nearest truthful
 * code its registry does define — never to a number that draft gives a
 * different meaning:
 *
 *   PD  EXCESSIVE_LOAD          -> d16 INTERNAL_ERROR
 *   RS  GOING_AWAY              -> d16 CANCELLED
 *   RS  TOO_FAR_BEHIND          -> d16 CANCELLED
 *   RS  EXPIRED_AUTH_TOKEN      -> d16 INTERNAL_ERROR
 *   RS  EXCESSIVE_LOAD          -> d16 INTERNAL_ERROR
 *
 * *_UNKNOWN encodes to the domain's INTERNAL_ERROR.
 */
MOQR_CORE_API moqr_result_t moqr_pd_encode(moq_version_t version, moqr_pd_status_t status,
                             uint64_t *out);
MOQR_CORE_API moqr_result_t moqr_reset_encode(moq_version_t version,
                                moqr_reset_cause_t cause, uint64_t *out);

/*
 * Forward a code between connections. Both drafts are validated BEFORE any
 * same-draft shortcut, so an unsupported pair can never hand a peer its own
 * bytes back: invalid returns MOQR_ERR_INVAL with *out untouched.
 *
 * A valid same-draft hop is byte-verbatim, including codes this relay does not
 * recognize, so an unknown extension survives unchanged. Across drafts an
 * unrecognized code cannot be carried verbatim — the number means something
 * else there — so it fails closed to the target domain's INTERNAL_ERROR.
 */
MOQR_CORE_API moqr_result_t moqr_pd_forward(moq_version_t from, moq_version_t to,
                              uint64_t wire, uint64_t *out);
MOQR_CORE_API moqr_result_t moqr_reset_forward(moq_version_t from, moq_version_t to,
                                 uint64_t wire, uint64_t *out);

/*
 * Relay-internal reset causes are not wire codes. They name why this relay
 * reset a stream and must be encoded per draft before they reach a peer.
 */
MOQR_CORE_API moqr_result_t moqr_reset_encode_internal(moq_version_t version,
                                         uint64_t internal, uint64_t *out);

/* -- Retained terminal codes ------------------------------------------------ *
 *
 * A terminal code can outlive the connection that produced it: a retained group
 * is replayable to a subscriber that arrives long after the source is gone, and
 * a replacement source may speak the other draft. A stored number is therefore
 * not self-describing — "0x4" alone cannot say whether it is draft-16 wire,
 * draft-18 wire, or one of this relay's own cause tags. Every retained code
 * carries a tag saying which of those it is, so it is decoded in the registry
 * it was written in and translated exactly once, at emission.
 */

typedef enum moqr_code_tag {
    MOQR_CODE_NONE = 0,   /* no terminal code exists here                     */
    MOQR_CODE_WIRE_D16,   /* foreign wire, read in draft-16's registry        */
    MOQR_CODE_WIRE_D18,   /* foreign wire, read in draft-18's registry        */
    MOQR_CODE_LOCAL,      /* this relay's own meaning, not any draft's number */
    /* A relay-authored PUBLISH_DONE extension number. Both drafts say an
     * application SHOULD — not MUST — use a registered status, and draft-18
     * requires an unknown status to be read as INTERNAL_ERROR without closing
     * the session, so a policy code is legitimate. It is emitted unchanged to
     * either draft, which is only safe while the number means nothing in
     * either: the constructor and emission both refuse anything registered,
     * draft-specific, GREASE-reserved, or beyond the common wire ceiling.
     * PUBLISH_DONE only — the reset domain has no such allowance. */
    MOQR_CODE_LOCAL_PD_EXTENSION,
} moqr_code_tag_t;

/* `value` is raw wire for the WIRE_* tags and a domain semantic
 * (moqr_pd_status_t / moqr_reset_cause_t) for LOCAL. It is meaningless for
 * NONE and must not be read. */
typedef struct moqr_pd_desc {
    moqr_code_tag_t tag;
    uint64_t        value;
} moqr_pd_desc_t;

typedef struct moqr_reset_desc {
    moqr_code_tag_t tag;
    uint64_t        value;
} moqr_reset_desc_t;

/* Constructors. The wire forms refuse an unsupported draft, so a descriptor can
 * never be built claiming an origin the translator would later reject. */
MOQR_CORE_API moqr_result_t moqr_pd_desc_wire(moq_version_t origin, uint64_t wire,
                                moqr_pd_desc_t *out);
MOQR_CORE_API moqr_result_t moqr_reset_desc_wire(moq_version_t origin, uint64_t wire,
                                   moqr_reset_desc_t *out);
/* The LOCAL forms accept only a defined, non-UNKNOWN meaning; the internal form
 * accepts only a supported relay cause tag. Anything else returns
 * MOQR_ERR_INVAL and leaves the destination descriptor untouched, so a corrupt
 * payload cannot be smuggled in under a valid tag. */
MOQR_CORE_API moqr_result_t moqr_pd_desc_local(moqr_pd_status_t status,
                                 moqr_pd_desc_t *out);
MOQR_CORE_API moqr_result_t moqr_reset_desc_local(moqr_reset_cause_t cause,
                                    moqr_reset_desc_t *out);
MOQR_CORE_API moqr_result_t moqr_reset_desc_internal(uint64_t internal,
                                       moqr_reset_desc_t *out);
/* A relay-authored PUBLISH_DONE extension code. Accepted only when the number
 * is unassigned in BOTH drafts' registries, outside draft-18's GREASE
 * reservation (0x7f * N + 0x9D), and within MOQ_QUIC_VARINT_MAX. A caller that
 * wants a registered meaning must use moqr_pd_desc_local instead. */
MOQR_CORE_API moqr_result_t moqr_pd_desc_extension(uint64_t code, moqr_pd_desc_t *out);
MOQR_CORE_API moqr_pd_desc_t    moqr_pd_desc_none(void);
MOQR_CORE_API moqr_reset_desc_t moqr_reset_desc_none(void);

/*
 * Emit a retained descriptor to `target`'s registry: foreign wire is forwarded
 * exactly once from its own origin, a local meaning is encoded directly, and
 * NONE or a corrupt tag returns MOQR_ERR_INVAL with *out untouched — a terminal
 * site with nothing truthful to say must fail its connection closed rather than
 * emit a plausible number.
 */
/* True when a descriptor is one this relay could have built. Public core
 * entries check it BEFORE touching durable state: the structs are copyable, so
 * a caller can hand in a corrupt tag or payload, and a terminal that cannot be
 * stated must not first retire a sub, abandon a record or queue an intent. */
MOQR_CORE_API bool moqr_pd_desc_valid(moqr_pd_desc_t desc);
MOQR_CORE_API bool moqr_reset_desc_valid(moqr_reset_desc_t desc);

MOQR_CORE_API moqr_result_t moqr_pd_desc_emit(moqr_pd_desc_t desc, moq_version_t target,
                                uint64_t *out);
MOQR_CORE_API moqr_result_t moqr_reset_desc_emit(moqr_reset_desc_t desc,
                                   moq_version_t target, uint64_t *out);

#ifdef __cplusplus
}
#endif

#endif /* MOQRELAY_WIRE_CODES_H */
