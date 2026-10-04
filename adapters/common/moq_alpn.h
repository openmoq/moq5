#ifndef MOQ_ADAPTER_COMMON_ALPN_H
#define MOQ_ADAPTER_COMMON_ALPN_H

/*
 * Internal adapter helper: map between a negotiated MoQ-over-QUIC ALPN
 * string and a moq_version_t profile.
 *
 * NOT a public API: header-only, no MOQ_API, not installed. Intended for
 * native / WebTransport-over-QUIC managed adapters (e.g. picoquic-threaded,
 * mvfst managed) to fill moq_session_cfg_t.version from the ALPN the
 * transport negotiated. Attach-mode hosts already own the connection and
 * set cfg.version themselves; they do not need this helper.
 *
 * SCOPE — this maps the *MoQ* ALPN only (e.g. "moqt-16"). It must NOT be
 * called for the H3 WebTransport ALPN "h3": under H3-WT the MoQ version is
 * negotiated via WT-Available-Protocols, a separate surface not handled
 * here. Passing "h3" simply returns false (it is not a MoQ ALPN).
 *
 * UNKNOWN-ALPN SAFETY — moq_alpn_to_version() returns false and writes
 * nothing on an unknown/unsupported ALPN. It never yields a usable
 * version, so an unsupported ALPN can never be misrepresented as
 * cfg.version == 0 (which means "default to D16" in moq_session_create).
 *
 * The "moqt-16" literal here intentionally matches the public adapter
 * constant MOQ_PQ_ALPN_DRAFT16; the public macro stays the canonical
 * spelling and is not "backed by" this private helper.
 */

#include <moq/session.h>

#include <assert.h>      /* static_assert in C11 and C++ alike */
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

/*
 * THE ALPN TABLE -- the single source for both directions.
 *
 * Every ALPN literal and every version-specific decision lives in these two
 * rows: neither function below contains an ALPN string or a per-version
 * branch, so a new registration is one row, added once, and cannot be added
 * to one direction only.
 *
 * `len` is the WIRE length, DERIVED from each literal's own storage so the
 * two cannot disagree. The literals are source-owned and NUL-terminated
 * because moq_alpn_for_version() hands them back as C strings; the terminator
 * is not part of the ALPN, and the inverse direction matches on `len` alone.
 *
 * Private: no MOQ_API, not installed, no exported symbol. Row ORDER is not
 * part of the contract -- both lookups scan, and neither reports position.
 */
typedef struct moq_alpn_entry {
    moq_version_t version;
    const char   *alpn;   /* NUL-terminated internal literal */
    uint8_t       len;    /* wire length, excluding the terminator */
} moq_alpn_entry_t;

/*
 * C and C++17 both initialize BY FIELD NAME, and both instantiate the same two
 * semantic rows into the same `moq_alpn_table` object that both functions
 * scan. C uses designated initializers directly. C++17 has no standard
 * designated initialization, so it names the fields through a `static
 * constexpr` helper that zero-initializes an entry and then assigns each
 * field. A positional aggregate is deliberately NOT used, and no GNU/C++20
 * designated-initializer extension is relied on.
 *
 * `static` is about LINKAGE, not visibility: the helper's name is still
 * declared in every including translation unit, as any header entity is. What
 * `static` buys is internal linkage -- no global or exported symbol, and no
 * collision with an identically named entity in another TU.
 *
 * (A constexpr function is preferred over an immediately invoked lambda here
 * only because it keeps the array statically initialized and reads the same as
 * the C form; the intent -- named-field initialization -- is the same one the
 * ruling asked for.)
 */
#if defined(__cplusplus)
static constexpr moq_alpn_entry_t
moq_alpn_row_(moq_version_t version, const char *alpn, uint8_t len)
{
    moq_alpn_entry_t e{};
    e.version = version;
    e.alpn    = alpn;
    e.len     = len;
    return e;
}
#define MOQ_ALPN_ROW_(v, a) moq_alpn_row_((v), (a), MOQ_ALPN_LEN_(a))
#else
#define MOQ_ALPN_ROW_(v, a) \
    { .version = (v), .alpn = (a), .len = MOQ_ALPN_LEN_(a) }
#endif

/*
 * The wire length is DERIVED from the literal's own storage, so it cannot
 * drift from the bytes: a row is (version, literal) and nothing else. The
 * cast is guarded by the representability assertions below rather than
 * silently truncating.
 */
#define MOQ_ALPN_LEN_(a) ((uint8_t)(sizeof(a) - 1u))

static_assert(sizeof("moqt-16") - 1u <= 255u,
              "an ALPN literal must fit the uint8_t wire length");
static_assert(sizeof("moqt-18") - 1u <= 255u,
              "an ALPN literal must fit the uint8_t wire length");
static_assert(sizeof("moqt-21") - 1u <= 255u,
              "an ALPN literal must fit the uint8_t wire length");

static const moq_alpn_entry_t moq_alpn_table[] = {
    MOQ_ALPN_ROW_(MOQ_VERSION_DRAFT_16, "moqt-16"),
    MOQ_ALPN_ROW_(MOQ_VERSION_DRAFT_18, "moqt-18"),
    MOQ_ALPN_ROW_(MOQ_VERSION_DRAFT_21, "moqt-21"),
};

#undef MOQ_ALPN_ROW_
#undef MOQ_ALPN_LEN_

/*
 * Map a negotiated MoQ ALPN (alpn/len, not NUL-terminated) to a profile
 * version. Returns true and sets *out for a recognized ALPN; returns false
 * (leaving *out untouched) for unknown input, including NULL/empty, "h3",
 * legacy "moq-00", and the final "moqt".
 *
 * ALPN registration and profile availability are enforced INDEPENDENTLY: a
 * recognized ALPN says only that the version is registered here. Whether a
 * session can be created for it is moq_session_create()'s decision, made
 * against the profiles this build actually links.
 */
static inline bool moq_alpn_to_version(const char *alpn, size_t len,
                                       moq_version_t *out)
{
    size_t i;

    if (!alpn || len == 0 || !out)
        return false;

    for (i = 0; i < sizeof(moq_alpn_table) / sizeof(moq_alpn_table[0]); i++) {
        /* the explicit wire length decides the match; the terminator does not
         * participate, and a longer buffer with the right length still
         * matches exactly as the wire form does */
        if ((size_t)moq_alpn_table[i].len != len)
            continue;
        if (memcmp(alpn, moq_alpn_table[i].alpn, len) != 0)
            continue;
        *out = moq_alpn_table[i].version;
        return true;
    }

    return false;
}

/*
 * Map a registered profile version to its MoQ-over-QUIC ALPN string
 * (NUL-terminated). Returns NULL for unregistered versions. A non-NULL result
 * reflects ALPN registration, not profile availability.
 */
static inline const char *moq_alpn_for_version(moq_version_t version)
{
    size_t i;

    for (i = 0; i < sizeof(moq_alpn_table) / sizeof(moq_alpn_table[0]); i++)
        if (moq_alpn_table[i].version == version)
            return moq_alpn_table[i].alpn;

    return NULL;
}

#endif /* MOQ_ADAPTER_COMMON_ALPN_H */
