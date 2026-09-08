/*
 * Cross-draft terminal translation.
 *
 * The numbers here are transcribed from the pinned drafts, not from the
 * implementation: PUBLISH_DONE (d16 Section 13.4.3 / d18 Section 15.10.3) and
 * the data-stream reset registry (d16 Section 13.4.4 / d18 Section 15.10.4).
 * Two drafts on one listener disagree inside the same registry, so a forwarded
 * raw integer states something the sender never said.
 */

#include <moq/relay/wire_codes.h>
#include <moq/relay/relay.h>

#include "test_support.h"

#include <stdio.h>
#include <string.h>

#define D16 MOQ_VERSION_DRAFT_16
#define D18 MOQ_VERSION_DRAFT_18

/* The swap is the whole reason this module exists: the same two meanings trade
 * numbers, so verbatim forwarding turns one into the other. */
static int
test_publish_done_swap(void)
{
    int failures = 0;
    uint64_t o = 0;

    MOQ_TEST_CHECK(moqr_pd_encode(D16, MOQR_PD_EXPIRED, &o) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(o, 0x5u);
    MOQ_TEST_CHECK(moqr_pd_encode(D18, MOQR_PD_EXPIRED, &o) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(o, 0x6u);
    MOQ_TEST_CHECK(moqr_pd_encode(D16, MOQR_PD_TOO_FAR_BEHIND, &o) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(o, 0x6u);
    MOQ_TEST_CHECK(moqr_pd_encode(D18, MOQR_PD_TOO_FAR_BEHIND, &o) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(o, 0x5u);

    /* EXPIRED survives the hop as EXPIRED, in both directions. */
    MOQ_TEST_CHECK(moqr_pd_forward(D16, D18, 0x5u, &o) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(o, 0x6u);
    MOQ_TEST_CHECK(moqr_pd_forward(D18, D16, 0x6u, &o) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(o, 0x5u);
    /* TOO_FAR_BEHIND likewise, and never as EXPIRED. */
    MOQ_TEST_CHECK(moqr_pd_forward(D16, D18, 0x6u, &o) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(o, 0x5u);
    MOQ_TEST_CHECK(moqr_pd_forward(D18, D16, 0x5u, &o) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(o, 0x6u);

    /* The codes both drafts agree on are unchanged by a hop. */
    MOQ_TEST_CHECK(moqr_pd_forward(D16, D18, 0x2u, &o) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(o, 0x2u);
    MOQ_TEST_CHECK(moqr_pd_forward(D18, D16, 0x4u, &o) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(o, 0x4u);
    return failures;
}

/* draft-18 moved UNKNOWN_OBJECT_STATUS and put GOING_AWAY on the number
 * draft-16 still uses for it — the collision a verbatim hop would create. */
static int
test_reset_registry_divergence(void)
{
    int failures = 0;
    uint64_t o = 0;

    MOQ_TEST_CHECK(moqr_reset_encode(D16, MOQR_RESET_UNKNOWN_OBJECT_STATUS,
                                     &o) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(o, 0x4u);
    MOQ_TEST_CHECK(moqr_reset_encode(D18, MOQR_RESET_UNKNOWN_OBJECT_STATUS,
                                     &o) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(o, 0x6u);
    MOQ_TEST_CHECK(moqr_reset_forward(D16, D18, 0x4u, &o) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(o, 0x6u);
    MOQ_TEST_CHECK(moqr_reset_forward(D18, D16, 0x6u, &o) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(o, 0x4u);

    /* d18-only causes have no d16 number. They must degrade to a code d16
     * actually defines, never to 0x4 (which d16 reads as
     * UNKNOWN_OBJECT_STATUS). */
    MOQ_TEST_CHECK(moqr_reset_forward(D18, D16, 0x4u, &o) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(o, 0x1u);   /* GOING_AWAY -> CANCELLED */
    MOQ_TEST_CHECK(moqr_reset_forward(D18, D16, 0x5u, &o) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(o, 0x1u);   /* TOO_FAR_BEHIND -> CANCELLED */
    MOQ_TEST_CHECK(moqr_reset_forward(D18, D16, 0x7u, &o) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(o, 0x0u);   /* EXPIRED_AUTH_TOKEN -> INTERNAL_ERROR */
    return failures;
}

/* Same draft forwards byte-verbatim, including codes this relay has never
 * heard of; a cross-draft hop cannot, because the number is claimed there. */
static int
test_unknown_codes(void)
{
    int failures = 0;
    uint64_t o = 0;

    MOQ_TEST_CHECK(moqr_pd_forward(D18, D18, 0x4242u, &o) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(o, 0x4242u);
    MOQ_TEST_CHECK(moqr_reset_forward(D16, D16, 0x4242u, &o) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(o, 0x4242u);

    MOQ_TEST_CHECK(moqr_pd_forward(D18, D16, 0x4242u, &o) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(o, 0x0u);
    MOQ_TEST_CHECK(moqr_reset_forward(D16, D18, 0x4242u, &o) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(o, 0x0u);
    /* draft-18's EXCESSIVE_LOAD has no draft-16 counterpart. */
    MOQ_TEST_CHECK(moqr_pd_forward(D18, D16, 0x9u, &o) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(o, 0x0u);

    MOQ_TEST_CHECK(moqr_pd_decode(D16, 0x4242u) == MOQR_PD_UNKNOWN);
    MOQ_TEST_CHECK(moqr_reset_decode(D18, 0x4242u) == MOQR_RESET_UNKNOWN);
    return failures;
}

/* The relay's own cause tags are not wire codes and must never reach a peer
 * unchanged. */
static int
test_internal_causes(void)
{
    int failures = 0;
    uint64_t o = 0;

    MOQ_TEST_CHECK(moqr_reset_encode_internal(D16, MOQR_RESET_CODE_STOPPED,
                                              &o) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(o, 0x1u);
    MOQ_TEST_CHECK(moqr_reset_encode_internal(D18, MOQR_RESET_CODE_STOPPED,
                                              &o) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(o, 0x1u);
    /* draft-18 names eviction exactly; draft-16 has only the generic. */
    MOQ_TEST_CHECK(moqr_reset_encode_internal(D18, MOQR_RESET_CODE_EVICTED,
                                              &o) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(o, 0x5u);
    MOQ_TEST_CHECK(moqr_reset_encode_internal(D16, MOQR_RESET_CODE_EVICTED,
                                              &o) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(o, 0x1u);
    MOQ_TEST_CHECK(o != MOQR_RESET_CODE_EVICTED);
    return failures;
}

/* An unsupported draft has no table and no equality shortcut: the translator is
 * its own boundary, and a caller holding the unattached sentinel 0 must not be
 * silently served draft-16 numbering or handed its own bytes back. */
static int
test_invalid_versions(void)
{
    int failures = 0;
    const moq_version_t bad[3] = { (moq_version_t)0, (moq_version_t)17,
                                   (moq_version_t)0xFFFF };

    for (int i = 0; i < 3; i++) {
        moq_version_t v = bad[i];
        uint64_t o = 0xDEADu;

        /* Never decode through the draft-16 table. */
        MOQ_TEST_CHECK(moqr_pd_decode(v, 0x5u) == MOQR_PD_UNKNOWN);
        MOQ_TEST_CHECK(moqr_reset_decode(v, 0x4u) == MOQR_RESET_UNKNOWN);

        /* Refused in both domains, in source and target position alike. */
        MOQ_TEST_CHECK(moqr_pd_encode(v, MOQR_PD_TRACK_ENDED, &o) ==
                       MOQR_ERR_INVAL);
        MOQ_TEST_CHECK(moqr_reset_encode(v, MOQR_RESET_CANCELLED, &o) ==
                       MOQR_ERR_INVAL);
        MOQ_TEST_CHECK(moqr_reset_encode_internal(v, MOQR_RESET_CODE_STOPPED,
                                                  &o) == MOQR_ERR_INVAL);
        MOQ_TEST_CHECK(moqr_pd_forward(v, D18, 0x2u, &o) == MOQR_ERR_INVAL);
        MOQ_TEST_CHECK(moqr_pd_forward(D18, v, 0x2u, &o) == MOQR_ERR_INVAL);
        MOQ_TEST_CHECK(moqr_reset_forward(v, D16, 0x1u, &o) == MOQR_ERR_INVAL);
        MOQ_TEST_CHECK(moqr_reset_forward(D16, v, 0x1u, &o) == MOQR_ERR_INVAL);

        /* The equal-version pair proves validation runs before the verbatim
         * shortcut: (0,0) and (17,17) must not hand the bytes back. */
        MOQ_TEST_CHECK(moqr_pd_forward(v, v, 0x4242u, &o) == MOQR_ERR_INVAL);
        MOQ_TEST_CHECK(moqr_reset_forward(v, v, 0x4242u, &o) == MOQR_ERR_INVAL);

        /* Every refusal above left the caller's value untouched. */
        MOQ_TEST_CHECK_EQ_U64(o, 0xDEADu);
    }
    return failures;
}

/* A retained code outlives its connection, so it must say what it is. These
 * pin that the three kinds stay distinct and that emission translates a
 * foreign code exactly once, from the registry it was written in. */
static int
test_retained_descriptors(void)
{
    int failures = 0;
    uint64_t o = 0xDEADu;
    moqr_pd_desc_t pd;
    moqr_reset_desc_t rs;

    /* Foreign wire keeps its own origin, whatever draft reads it later. */
    MOQ_TEST_CHECK(moqr_pd_desc_wire(D16, 0x5u, &pd) == MOQR_OK);
    MOQ_TEST_CHECK(pd.tag == MOQR_CODE_WIRE_D16);
    MOQ_TEST_CHECK(moqr_pd_desc_emit(pd, D16, &o) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(o, 0x5u);            /* same draft: verbatim */
    MOQ_TEST_CHECK(moqr_pd_desc_emit(pd, D18, &o) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(o, 0x6u);            /* EXPIRED, renumbered */

    /* A d18-origin descriptor is not reinterpreted by a d16 reader. */
    MOQ_TEST_CHECK(moqr_pd_desc_wire(D18, 0x5u, &pd) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_pd_desc_emit(pd, D16, &o) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(o, 0x6u);            /* TOO_FAR_BEHIND, renumbered */

    /* A local meaning is encoded per target, never forwarded. */
    MOQ_TEST_CHECK(moqr_pd_desc_local(MOQR_PD_TRACK_ENDED, &pd) == MOQR_OK);
    MOQ_TEST_CHECK(pd.tag == MOQR_CODE_LOCAL);
    MOQ_TEST_CHECK(moqr_pd_desc_emit(pd, D16, &o) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(o, 0x2u);
    MOQ_TEST_CHECK(moqr_pd_desc_emit(pd, D18, &o) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(o, 0x2u);

    /* This relay's cause tags become local meanings, never wire. */
    MOQ_TEST_CHECK(moqr_reset_desc_internal(MOQR_RESET_CODE_EVICTED, &rs) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(rs.tag == MOQR_CODE_LOCAL);
    MOQ_TEST_CHECK(rs.value != MOQR_RESET_CODE_EVICTED);
    MOQ_TEST_CHECK(moqr_reset_desc_emit(rs, D18, &o) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(o, 0x5u);
    MOQ_TEST_CHECK(moqr_reset_desc_emit(rs, D16, &o) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(o, 0x1u);

    /* An upstream reset keeps its origin across the same hop. */
    MOQ_TEST_CHECK(moqr_reset_desc_wire(D16, 0x4u, &rs) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_reset_desc_emit(rs, D18, &o) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(o, 0x6u);   /* UNKNOWN_OBJECT_STATUS relocated */

    /* Nothing to say fails closed, and leaves the caller's value alone. */
    o = 0xBEEFu;
    MOQ_TEST_CHECK(moqr_pd_desc_emit(moqr_pd_desc_none(), D18, &o) ==
                   MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(moqr_reset_desc_emit(moqr_reset_desc_none(), D16, &o) ==
                   MOQR_ERR_INVAL);
    /* A corrupt tag is refused rather than guessed. */
    pd.tag = (moqr_code_tag_t)99;
    MOQ_TEST_CHECK(moqr_pd_desc_emit(pd, D18, &o) == MOQR_ERR_INVAL);
    rs.tag = (moqr_code_tag_t)99;
    MOQ_TEST_CHECK(moqr_reset_desc_emit(rs, D18, &o) == MOQR_ERR_INVAL);
    /* An unsupported target is refused at the descriptor boundary too. */
    {
        moqr_pd_desc_t local_pd;
        MOQ_TEST_CHECK(moqr_pd_desc_local(MOQR_PD_TRACK_ENDED, &local_pd) ==
                       MOQR_OK);
        MOQ_TEST_CHECK(moqr_pd_desc_emit(local_pd, (moq_version_t)0, &o) ==
                       MOQR_ERR_INVAL);
    }
    /* A descriptor can never be built claiming an origin translation rejects. */
    MOQ_TEST_CHECK(moqr_pd_desc_wire((moq_version_t)17, 0x2u, &pd) ==
                   MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(moqr_reset_desc_wire((moq_version_t)0, 0x1u, &rs) ==
                   MOQR_ERR_INVAL);
    MOQ_TEST_CHECK_EQ_U64(o, 0xBEEFu);
    return failures;
}

/* A corrupt tag is refused, so a corrupt payload under a VALID tag must be too:
 * the structs are copyable and publicly constructible, so emission cannot trust
 * that a constructor ever ran. Foreign raw wire stays extensible — an unknown
 * but well-formed wire code is not a corrupt payload. */
static int
test_local_payload_validation(void)
{
    int failures = 0;
    uint64_t o = 0xBEEFu;
    moqr_pd_desc_t pd;
    moqr_reset_desc_t rs;

    /* UNKNOWN is not a meaning anything may be emitted as. */
    pd.tag = MOQR_CODE_LOCAL;
    pd.value = (uint64_t)MOQR_PD_UNKNOWN;
    MOQ_TEST_CHECK(moqr_pd_desc_emit(pd, D18, &o) == MOQR_ERR_INVAL);
    rs.tag = MOQR_CODE_LOCAL;
    rs.value = (uint64_t)MOQR_RESET_UNKNOWN;
    MOQ_TEST_CHECK(moqr_reset_desc_emit(rs, D16, &o) == MOQR_ERR_INVAL);

    /* An arbitrary value under a valid tag is equally not a meaning. */
    pd.value = 4242u;
    MOQ_TEST_CHECK(moqr_pd_desc_emit(pd, D16, &o) == MOQR_ERR_INVAL);
    rs.value = 4242u;
    MOQ_TEST_CHECK(moqr_reset_desc_emit(rs, D18, &o) == MOQR_ERR_INVAL);

    /* Foreign wire is NOT validated against our table: unknown extension
     * bytes must still travel a same-draft hop. */
    MOQ_TEST_CHECK(moqr_pd_desc_wire(D18, 0x4242u, &pd) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_pd_desc_emit(pd, D18, &o) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(o, 0x4242u);

    /* The constructors refuse the same payloads and write nothing at all. The
     * destinations are deterministically initialized first so the comparison
     * covers the whole object representation, padding included — a refusal must
     * not stage a partially initialized descriptor for a later store. */
    moqr_pd_desc_t pd_before;
    moqr_reset_desc_t rs_before;

    memset(&pd, 0xA5, sizeof(pd));
    pd.tag = MOQR_CODE_NONE;
    pd.value = 7u;
    memcpy(&pd_before, &pd, sizeof(pd));
    MOQ_TEST_CHECK(moqr_pd_desc_local(MOQR_PD_UNKNOWN, &pd) == MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(moqr_pd_desc_local((moqr_pd_status_t)4242, &pd) ==
                   MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(moqr_pd_desc_wire((moq_version_t)17, 0x2u, &pd) ==
                   MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(memcmp(&pd, &pd_before, sizeof(pd)) == 0);

    memset(&rs, 0x5A, sizeof(rs));
    rs.tag = MOQR_CODE_NONE;
    rs.value = 9u;
    memcpy(&rs_before, &rs, sizeof(rs));
    MOQ_TEST_CHECK(moqr_reset_desc_local(MOQR_RESET_UNKNOWN, &rs) ==
                   MOQR_ERR_INVAL);
    /* An unrecognized relay cause is a caller bug, not a terminal to invent. */
    MOQ_TEST_CHECK(moqr_reset_desc_internal(0xDEADu, &rs) == MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(memcmp(&rs, &rs_before, sizeof(rs)) == 0);

    /* Every refusal above left the caller's wire output untouched. */
    o = 0xBEEFu;
    pd.tag = MOQR_CODE_LOCAL;
    pd.value = (uint64_t)MOQR_PD_UNKNOWN;
    MOQ_TEST_CHECK(moqr_pd_desc_emit(pd, D18, &o) == MOQR_ERR_INVAL);
    MOQ_TEST_CHECK_EQ_U64(o, 0xBEEFu);
    return failures;
}

/* A relay-authored PUBLISH_DONE extension is emitted unchanged to either draft,
 * which is only safe while the number means nothing in either. Both drafts say
 * an application SHOULD — not MUST — use a registered status, and draft-18
 * requires an unknown status to read as INTERNAL_ERROR without closing the
 * session, so the capability is legitimate; the constraints are what keep it
 * from asserting a meaning we did not choose. */
static int
test_pd_extension(void)
{
    int failures = 0;
    uint64_t o = 0;
    moqr_pd_desc_t pd;

    /* 0x7 is unassigned in both registries: byte-verbatim either way. */
    MOQ_TEST_CHECK(moqr_pd_desc_extension(0x7u, &pd) == MOQR_OK);
    MOQ_TEST_CHECK(pd.tag == MOQR_CODE_LOCAL_PD_EXTENSION);
    MOQ_TEST_CHECK(moqr_pd_desc_emit(pd, D16, &o) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(o, 0x7u);
    MOQ_TEST_CHECK(moqr_pd_desc_emit(pd, D18, &o) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(o, 0x7u);

    /* Registered in BOTH: refused. */
    MOQ_TEST_CHECK(moqr_pd_desc_extension(0x2u, &pd) == MOQR_ERR_INVAL);
    /* Registered in one draft only — the asymmetric cases that would otherwise
     * assert a meaning to exactly one peer. */
    MOQ_TEST_CHECK(moqr_pd_desc_extension(0x9u, &pd) == MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(moqr_pd_desc_extension(0x5u, &pd) == MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(moqr_pd_desc_extension(0x6u, &pd) == MOQR_ERR_INVAL);
    /* draft-18 GREASE: 0x7f * N + 0x9D. */
    MOQ_TEST_CHECK(moqr_pd_desc_extension(0x9Du, &pd) == MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(moqr_pd_desc_extension(0x9Du + 0x7Fu, &pd) ==
                   MOQR_ERR_INVAL);
    /* Beyond the common wire ceiling. */
    MOQ_TEST_CHECK(moqr_pd_desc_extension(0x4000000000000000ULL, &pd) ==
                   MOQR_ERR_INVAL);

    /* A refusal leaves the destination untouched, padding included. */
    moqr_pd_desc_t before;
    memset(&pd, 0x3C, sizeof(pd));
    pd.tag = MOQR_CODE_NONE;
    pd.value = 3u;
    memcpy(&before, &pd, sizeof(pd));
    MOQ_TEST_CHECK(moqr_pd_desc_extension(0x2u, &pd) == MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(memcmp(&pd, &before, sizeof(pd)) == 0);

    /* Emission re-checks: a copyable descriptor whose number is registered
     * cannot be emitted just because its tag says extension. */
    pd.tag = MOQR_CODE_LOCAL_PD_EXTENSION;
    pd.value = 0x2u;
    o = 0xBEEFu;
    MOQ_TEST_CHECK(moqr_pd_desc_emit(pd, D18, &o) == MOQR_ERR_INVAL);
    pd.value = 0x9Du;
    MOQ_TEST_CHECK(moqr_pd_desc_emit(pd, D16, &o) == MOQR_ERR_INVAL);
    MOQ_TEST_CHECK_EQ_U64(o, 0xBEEFu);

    /* The reset domain has no such allowance: the tag is refused there. */
    moqr_reset_desc_t rs;
    rs.tag = MOQR_CODE_LOCAL_PD_EXTENSION;
    rs.value = 0x7u;
    MOQ_TEST_CHECK(moqr_reset_desc_emit(rs, D18, &o) == MOQR_ERR_INVAL);
    MOQ_TEST_CHECK_EQ_U64(o, 0xBEEFu);

    /* Extensible means an unknown but WELL-FORMED code. A value no peer could
     * have encoded is corruption, refused at the constructor, by validation and
     * at emission alike — in both domains. */
    moqr_reset_desc_t rs2;
    const uint64_t over = UINT64_C(0x4000000000000000);   /* varint max + 1 */

    memset(&pd, 0xA5, sizeof(pd));
    moqr_pd_desc_t pd_snap;
    memcpy(&pd_snap, &pd, sizeof(pd));
    MOQ_TEST_CHECK(moqr_pd_desc_wire(D16, over, &pd) == MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(moqr_pd_desc_wire(D18, UINT64_MAX, &pd) == MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(memcmp(&pd, &pd_snap, sizeof(pd)) == 0);
    /* The reset constructor gets its own initialized snapshot: a refusal must
     * leave the whole object representation untouched, padding included. */
    moqr_reset_desc_t rs_snap;
    memset(&rs2, 0x5A, sizeof(rs2));
    memcpy(&rs_snap, &rs2, sizeof(rs2));
    MOQ_TEST_CHECK(moqr_reset_desc_wire(D16, over, &rs2) == MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(moqr_reset_desc_wire(D18, UINT64_MAX, &rs2) ==
                   MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(memcmp(&rs2, &rs_snap, sizeof(rs2)) == 0);

    /* The largest legal varint round-trips through reset construction and a
     * same-draft hop as well. */
    MOQ_TEST_CHECK(moqr_reset_desc_wire(D16, over - 1u, &rs2) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_reset_desc_emit(rs2, D16, &o) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(o, over - 1u);

    /* The largest legal varint is still accepted and travels verbatim. */
    MOQ_TEST_CHECK(moqr_pd_desc_wire(D18, over - 1u, &pd) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_pd_desc_emit(pd, D18, &o) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(o, over - 1u);

    /* A hand-built over-limit WIRE descriptor is refused by emission too. */
    pd.tag = MOQR_CODE_WIRE_D18;
    pd.value = over;
    o = 0xBEEFu;
    MOQ_TEST_CHECK(moqr_pd_desc_emit(pd, D18, &o) == MOQR_ERR_INVAL);
    rs2.tag = MOQR_CODE_WIRE_D16;
    rs2.value = over;
    MOQ_TEST_CHECK(moqr_reset_desc_emit(rs2, D16, &o) == MOQR_ERR_INVAL);
    MOQ_TEST_CHECK_EQ_U64(o, 0xBEEFu);

    /* Received unknown wire is NOT an extension: it stays a WIRE descriptor,
     * transparent within a draft and fail-closed across. */
    MOQ_TEST_CHECK(moqr_pd_desc_wire(D18, 0x7u, &pd) == MOQR_OK);
    MOQ_TEST_CHECK(pd.tag == MOQR_CODE_WIRE_D18);
    MOQ_TEST_CHECK(moqr_pd_desc_emit(pd, D16, &o) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(o, 0x0u);
    return failures;
}

int
main(void)
{
    int failures = 0;

    failures += test_publish_done_swap();
    failures += test_reset_registry_divergence();
    failures += test_unknown_codes();
    failures += test_internal_causes();
    failures += test_invalid_versions();
    failures += test_retained_descriptors();
    failures += test_local_payload_validation();
    failures += test_pd_extension();
    if (failures == 0) {
        printf("PASS: relay_wire_codes\n");
    }
    return failures != 0;
}
