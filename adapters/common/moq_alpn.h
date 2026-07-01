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

#include <stdbool.h>
#include <stddef.h>
#include <string.h>

/*
 * Map a negotiated MoQ ALPN (alpn/len, not NUL-terminated) to a profile
 * version. Returns true and sets *out for a recognized ALPN; returns false
 * (leaving *out untouched) for unknown input, including NULL/empty, "h3",
 * legacy "moq-00", and the final "moqt".
 *
 * A recognized ALPN names a registered version; whether a session can be
 * created for it depends on profile availability, which moq_session_create()
 * enforces. ("moqt-18" maps to MOQ_VERSION_DRAFT_18, but session creation is
 * refused until the draft-18 profile is present.)
 */
static inline bool moq_alpn_to_version(const char *alpn, size_t len,
                                       moq_version_t *out)
{
    if (!alpn || len == 0 || !out)
        return false;

    if (len == 7 && memcmp(alpn, "moqt-16", 7) == 0) {
        *out = MOQ_VERSION_DRAFT_16;
        return true;
    }
    if (len == 7 && memcmp(alpn, "moqt-18", 7) == 0) {
        *out = MOQ_VERSION_DRAFT_18;
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
    switch (version) {
    case MOQ_VERSION_DRAFT_16:
        return "moqt-16";
    case MOQ_VERSION_DRAFT_18:
        return "moqt-18";
    default:
        return NULL;
    }
}

#endif /* MOQ_ADAPTER_COMMON_ALPN_H */
