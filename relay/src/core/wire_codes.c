#include <moq/relay/wire_codes.h>
#include <moq/relay/relay.h>

/*
 * The two registries, transcribed per draft. Codes absent from a draft are
 * marked so encoding falls back deliberately instead of colliding with the
 * meaning that draft gives the same number.
 */

#define NA UINT64_MAX   /* this draft assigns no code for this meaning */

/* The QUIC varint ceiling, 2^62 - 1: the common wire limit for BOTH descriptor
 * domains, hence the domain-neutral name. Spelled out here rather than included, because relay code may not
 * reach into the wire layer. A value above it is not an extensible unknown —
 * no peer could have encoded it. */
#define WIRE_CODE_MAX ((uint64_t)0x3FFFFFFFFFFFFFFFULL)

typedef struct code_row {
    int      semantic;
    uint64_t d16;
    uint64_t d18;
} code_row_t;

static const code_row_t PD_ROWS[] = {
    { MOQR_PD_INTERNAL_ERROR,     0x0u,  0x0u  },
    { MOQR_PD_UNAUTHORIZED,       0x1u,  0x1u  },
    { MOQR_PD_TRACK_ENDED,        0x2u,  0x2u  },
    { MOQR_PD_SUBSCRIPTION_ENDED, 0x3u,  0x3u  },
    { MOQR_PD_GOING_AWAY,         0x4u,  0x4u  },
    /* the swap: the same two meanings trade numbers between drafts */
    { MOQR_PD_EXPIRED,            0x5u,  0x6u  },
    { MOQR_PD_TOO_FAR_BEHIND,     0x6u,  0x5u  },
    { MOQR_PD_UPDATE_FAILED,      0x8u,  0x8u  },
    { MOQR_PD_EXCESSIVE_LOAD,     NA,    0x9u  },
    { MOQR_PD_MALFORMED_TRACK,    0x12u, 0x12u },
};

static const code_row_t RS_ROWS[] = {
    { MOQR_RESET_INTERNAL_ERROR,        0x0u,  0x0u  },
    { MOQR_RESET_CANCELLED,             0x1u,  0x1u  },
    { MOQR_RESET_DELIVERY_TIMEOUT,      0x2u,  0x2u  },
    { MOQR_RESET_SESSION_CLOSED,        0x3u,  0x3u  },
    /* draft-16 numbers this 0x4, which draft-18 gives to GOING_AWAY */
    { MOQR_RESET_UNKNOWN_OBJECT_STATUS, 0x4u,  0x6u  },
    { MOQR_RESET_GOING_AWAY,            NA,    0x4u  },
    { MOQR_RESET_TOO_FAR_BEHIND,        NA,    0x5u  },
    { MOQR_RESET_EXPIRED_AUTH_TOKEN,    NA,    0x7u  },
    { MOQR_RESET_EXCESSIVE_LOAD,        NA,    0x9u  },
    { MOQR_RESET_MALFORMED_TRACK,       0x12u, 0x12u },
};

/* Exactly the two drafts this relay speaks. Anything else — including the
 * unattached sentinel 0 — selects no table at all. */
static bool
version_ok(moq_version_t version)
{
    return version == MOQ_VERSION_DRAFT_16 || version == MOQ_VERSION_DRAFT_18;
}

static bool
is_d18(moq_version_t version)
{
    return version == MOQ_VERSION_DRAFT_18;
}

static int
decode_row(const code_row_t *rows, size_t n, moq_version_t version,
           uint64_t wire)
{
    if (!version_ok(version)) {
        return 0;   /* *_UNKNOWN: an unsupported draft has no registry here */
    }
    for (size_t i = 0; i < n; i++) {
        uint64_t code = is_d18(version) ? rows[i].d18 : rows[i].d16;

        if (code != NA && code == wire) {
            return rows[i].semantic;
        }
    }
    return 0;   /* *_UNKNOWN */
}

static uint64_t
encode_row(const code_row_t *rows, size_t n, moq_version_t version,
           int semantic, uint64_t fallback)
{
    for (size_t i = 0; i < n; i++) {
        if (rows[i].semantic != semantic) {
            continue;
        }
        uint64_t code = is_d18(version) ? rows[i].d18 : rows[i].d16;
        return code != NA ? code : fallback;
    }
    return fallback;
}

moqr_pd_status_t
moqr_pd_decode(moq_version_t version, uint64_t wire)
{
    return (moqr_pd_status_t)decode_row(PD_ROWS,
                                        sizeof(PD_ROWS) / sizeof(PD_ROWS[0]),
                                        version, wire);
}

moqr_reset_cause_t
moqr_reset_decode(moq_version_t version, uint64_t wire)
{
    return (moqr_reset_cause_t)decode_row(RS_ROWS,
                                          sizeof(RS_ROWS) / sizeof(RS_ROWS[0]),
                                          version, wire);
}

moqr_result_t
moqr_pd_encode(moq_version_t version, moqr_pd_status_t status, uint64_t *out)
{
    if (out == NULL || !version_ok(version)) {
        return MOQR_ERR_INVAL;
    }
    /* A meaning this draft never assigned degrades to INTERNAL_ERROR: truthful
     * and inert, where a neighbouring number would assert something else. */
    *out = encode_row(PD_ROWS, sizeof(PD_ROWS) / sizeof(PD_ROWS[0]),
                      version, (int)status, 0x0u);
    return MOQR_OK;
}

moqr_result_t
moqr_reset_encode(moq_version_t version, moqr_reset_cause_t cause,
                  uint64_t *out)
{
    if (out == NULL || !version_ok(version)) {
        return MOQR_ERR_INVAL;
    }
    /* draft-16 has no GOING_AWAY or TOO_FAR_BEHIND reset: both are a publisher
     * ending this stream deliberately, so CANCELLED is the truthful generic it
     * does define. 0x4 there means UNKNOWN_OBJECT_STATUS and must not be
     * borrowed. The auth/load causes have no such generic and degrade to
     * INTERNAL_ERROR. */
    uint64_t fallback = 0x0u;

    if (cause == MOQR_RESET_GOING_AWAY || cause == MOQR_RESET_TOO_FAR_BEHIND) {
        fallback = 0x1u;   /* CANCELLED */
    }
    *out = encode_row(RS_ROWS, sizeof(RS_ROWS) / sizeof(RS_ROWS[0]),
                      version, (int)cause, fallback);
    return MOQR_OK;
}

moqr_result_t
moqr_pd_forward(moq_version_t from, moq_version_t to, uint64_t wire,
                uint64_t *out)
{
    /* Validation precedes the same-draft shortcut: an unsupported pair must
     * never be handed its own bytes back. */
    if (out == NULL || !version_ok(from) || !version_ok(to)) {
        return MOQR_ERR_INVAL;
    }
    if (from == to) {
        *out = wire;   /* same draft: byte-verbatim, unknown codes included */
        return MOQR_OK;
    }
    moqr_pd_status_t sem = moqr_pd_decode(from, wire);

    if (sem == MOQR_PD_UNKNOWN) {
        *out = 0x0u;   /* INTERNAL_ERROR: it would mean something else there */
        return MOQR_OK;
    }
    return moqr_pd_encode(to, sem, out);
}

moqr_result_t
moqr_reset_forward(moq_version_t from, moq_version_t to, uint64_t wire,
                   uint64_t *out)
{
    if (out == NULL || !version_ok(from) || !version_ok(to)) {
        return MOQR_ERR_INVAL;
    }
    if (from == to) {
        *out = wire;
        return MOQR_OK;
    }
    moqr_reset_cause_t sem = moqr_reset_decode(from, wire);

    if (sem == MOQR_RESET_UNKNOWN) {
        *out = 0x0u;
        return MOQR_OK;
    }
    return moqr_reset_encode(to, sem, out);
}

moqr_result_t
moqr_reset_encode_internal(moq_version_t version, uint64_t internal,
                           uint64_t *out)
{
    if (out == NULL || !version_ok(version)) {
        return MOQR_ERR_INVAL;
    }
    switch (internal) {
    case MOQR_RESET_CODE_STOPPED:
        /* the upstream stopped this stream: the publisher ended it */
        return moqr_reset_encode(version, MOQR_RESET_CANCELLED, out);
    case MOQR_RESET_CODE_EVICTED:
        /* the subscriber fell behind this relay's capacity: draft-18 names
         * that exactly; draft-16 has only the generic. */
        return moqr_reset_encode(version, MOQR_RESET_TOO_FAR_BEHIND, out);
    default:
        return moqr_reset_encode(version, MOQR_RESET_INTERNAL_ERROR, out);
    }
}

/* -- Retained terminal descriptors ------------------------------------------ */

static moqr_code_tag_t
tag_of(moq_version_t origin)
{
    return origin == MOQ_VERSION_DRAFT_18 ? MOQR_CODE_WIRE_D18
                                          : MOQR_CODE_WIRE_D16;
}

static moq_version_t
origin_of(moqr_code_tag_t tag)
{
    return tag == MOQR_CODE_WIRE_D18 ? MOQ_VERSION_DRAFT_18
                                     : MOQ_VERSION_DRAFT_16;
}

moqr_result_t
moqr_pd_desc_wire(moq_version_t origin, uint64_t wire, moqr_pd_desc_t *out)
{
    /* Extensible means an unknown but WELL-FORMED code. A value no peer could
     * have encoded is not a foreign code, it is corruption. */
    if (out == NULL || !version_ok(origin) || wire > WIRE_CODE_MAX) {
        return MOQR_ERR_INVAL;
    }
    out->tag = tag_of(origin);
    out->value = wire;
    return MOQR_OK;
}

moqr_result_t
moqr_reset_desc_wire(moq_version_t origin, uint64_t wire,
                     moqr_reset_desc_t *out)
{
    if (out == NULL || !version_ok(origin) || wire > WIRE_CODE_MAX) {
        return MOQR_ERR_INVAL;
    }
    out->tag = tag_of(origin);
    out->value = wire;
    return MOQR_OK;
}

/* A LOCAL payload names one of this relay's meanings. UNKNOWN is the absence of
 * a meaning, not a meaning, and an arbitrary cast is nothing at all — neither
 * may be emitted. Checked here AND at emission, because the descriptors are
 * copyable and publicly constructible: emission cannot assume a constructor
 * ever ran. */
static bool
pd_semantic_ok(uint64_t value)
{
    switch (value) {
    case MOQR_PD_INTERNAL_ERROR:
    case MOQR_PD_UNAUTHORIZED:
    case MOQR_PD_TRACK_ENDED:
    case MOQR_PD_SUBSCRIPTION_ENDED:
    case MOQR_PD_GOING_AWAY:
    case MOQR_PD_EXPIRED:
    case MOQR_PD_TOO_FAR_BEHIND:
    case MOQR_PD_UPDATE_FAILED:
    case MOQR_PD_EXCESSIVE_LOAD:
    case MOQR_PD_MALFORMED_TRACK:
        return true;
    default:
        return false;
    }
}

static bool
reset_semantic_ok(uint64_t value)
{
    switch (value) {
    case MOQR_RESET_INTERNAL_ERROR:
    case MOQR_RESET_CANCELLED:
    case MOQR_RESET_DELIVERY_TIMEOUT:
    case MOQR_RESET_SESSION_CLOSED:
    case MOQR_RESET_UNKNOWN_OBJECT_STATUS:
    case MOQR_RESET_GOING_AWAY:
    case MOQR_RESET_TOO_FAR_BEHIND:
    case MOQR_RESET_EXPIRED_AUTH_TOKEN:
    case MOQR_RESET_EXCESSIVE_LOAD:
    case MOQR_RESET_MALFORMED_TRACK:
        return true;
    default:
        return false;
    }
}

/* A relay-authored extension number is safe to emit unchanged only while it
 * means nothing in either draft. Anything registered in either registry, inside
 * draft-18's GREASE reservation, or beyond the common wire ceiling is refused:
 * such a value would either assert a meaning we did not choose or fail to
 * encode on one of the two profiles. */
static bool
pd_extension_ok(uint64_t code)
{
    if (code > WIRE_CODE_MAX) {
        return false;
    }
    if (moqr_pd_decode(MOQ_VERSION_DRAFT_16, code) != MOQR_PD_UNKNOWN ||
        moqr_pd_decode(MOQ_VERSION_DRAFT_18, code) != MOQR_PD_UNKNOWN) {
        return false;
    }
    /* draft-18 Section 14: 0x7f * N + 0x9D is reserved for greasing. */
    if (code >= 0x9Du && ((code - 0x9Du) % 0x7Fu) == 0) {
        return false;
    }
    return true;
}

moqr_result_t
moqr_pd_desc_extension(uint64_t code, moqr_pd_desc_t *out)
{
    if (out == NULL || !pd_extension_ok(code)) {
        return MOQR_ERR_INVAL;
    }
    out->tag = MOQR_CODE_LOCAL_PD_EXTENSION;
    out->value = code;
    return MOQR_OK;
}

moqr_result_t
moqr_pd_desc_local(moqr_pd_status_t status, moqr_pd_desc_t *out)
{
    if (out == NULL || !pd_semantic_ok((uint64_t)status)) {
        return MOQR_ERR_INVAL;
    }
    out->tag = MOQR_CODE_LOCAL;
    out->value = (uint64_t)status;
    return MOQR_OK;
}

moqr_result_t
moqr_reset_desc_local(moqr_reset_cause_t cause, moqr_reset_desc_t *out)
{
    if (out == NULL || !reset_semantic_ok((uint64_t)cause)) {
        return MOQR_ERR_INVAL;
    }
    out->tag = MOQR_CODE_LOCAL;
    out->value = (uint64_t)cause;
    return MOQR_OK;
}

moqr_result_t
moqr_reset_desc_internal(uint64_t internal, moqr_reset_desc_t *out)
{
    switch (internal) {
    case MOQR_RESET_CODE_STOPPED:
        /* the upstream stopped this stream: the publisher ended it */
        return moqr_reset_desc_local(MOQR_RESET_CANCELLED, out);
    case MOQR_RESET_CODE_EVICTED:
        /* the subscriber fell behind this relay's capacity */
        return moqr_reset_desc_local(MOQR_RESET_TOO_FAR_BEHIND, out);
    default:
        /* An unrecognized relay cause is a bug in the caller, not a terminal
         * to invent a number for. */
        return MOQR_ERR_INVAL;
    }
}

moqr_pd_desc_t
moqr_pd_desc_none(void)
{
    moqr_pd_desc_t d = { MOQR_CODE_NONE, 0 };

    return d;
}

moqr_reset_desc_t
moqr_reset_desc_none(void)
{
    moqr_reset_desc_t d = { MOQR_CODE_NONE, 0 };

    return d;
}

bool
moqr_pd_desc_valid(moqr_pd_desc_t desc)
{
    switch (desc.tag) {
    case MOQR_CODE_WIRE_D16:
    case MOQR_CODE_WIRE_D18:
        return desc.value <= WIRE_CODE_MAX;   /* extensible, but well-formed */
    case MOQR_CODE_LOCAL:
        return pd_semantic_ok(desc.value);
    case MOQR_CODE_LOCAL_PD_EXTENSION:
        return pd_extension_ok(desc.value);
    default:
        return false;
    }
}

bool
moqr_reset_desc_valid(moqr_reset_desc_t desc)
{
    switch (desc.tag) {
    case MOQR_CODE_WIRE_D16:
    case MOQR_CODE_WIRE_D18:
        return desc.value <= WIRE_CODE_MAX;
    case MOQR_CODE_LOCAL:
        return reset_semantic_ok(desc.value);
    default:
        return false;   /* NONE, and the PD-only extension, are not resets */
    }
}

moqr_result_t
moqr_pd_desc_emit(moqr_pd_desc_t desc, moq_version_t target, uint64_t *out)
{
    if (out == NULL || !version_ok(target)) {
        return MOQR_ERR_INVAL;
    }
    switch (desc.tag) {
    case MOQR_CODE_WIRE_D16:
    case MOQR_CODE_WIRE_D18:
        if (desc.value > WIRE_CODE_MAX) {
            return MOQR_ERR_INVAL;
        }
        return moqr_pd_forward(origin_of(desc.tag), target, desc.value, out);
    case MOQR_CODE_LOCAL:
        if (!pd_semantic_ok(desc.value)) {
            return MOQR_ERR_INVAL;
        }
        return moqr_pd_encode(target, (moqr_pd_status_t)desc.value, out);
    case MOQR_CODE_LOCAL_PD_EXTENSION:
        /* Re-checked here because the struct is copyable: a descriptor whose
         * number became registered (or was never validated) must not be
         * emitted as though it still meant nothing. */
        if (!pd_extension_ok(desc.value)) {
            return MOQR_ERR_INVAL;
        }
        *out = desc.value;   /* unchanged in either draft, by construction */
        return MOQR_OK;
    case MOQR_CODE_NONE:
    default:
        return MOQR_ERR_INVAL;
    }
}

moqr_result_t
moqr_reset_desc_emit(moqr_reset_desc_t desc, moq_version_t target,
                     uint64_t *out)
{
    if (out == NULL || !version_ok(target)) {
        return MOQR_ERR_INVAL;
    }
    switch (desc.tag) {
    case MOQR_CODE_WIRE_D16:
    case MOQR_CODE_WIRE_D18:
        if (desc.value > WIRE_CODE_MAX) {
            return MOQR_ERR_INVAL;
        }
        return moqr_reset_forward(origin_of(desc.tag), target, desc.value, out);
    case MOQR_CODE_LOCAL:
        if (!reset_semantic_ok(desc.value)) {
            return MOQR_ERR_INVAL;
        }
        return moqr_reset_encode(target, (moqr_reset_cause_t)desc.value, out);
    case MOQR_CODE_NONE:
    default:
        return MOQR_ERR_INVAL;
    }
}
