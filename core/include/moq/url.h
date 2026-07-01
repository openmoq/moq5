#ifndef MOQ_URL_H
#define MOQ_URL_H

/*
 * MoQ application URL parser.
 *
 * Zero-allocation parser for MoQ relay URLs. All output slices
 * borrow from the input buffer and remain valid only while the
 * input is alive.
 *
 * Accepted schemes: "moqt", "moq", "https".
 * All other schemes are rejected with MOQ_ERR_INVAL.
 *
 * Default ports by scheme when no explicit port is given:
 *   moqt → 4433, moq → 4433, https → 443.
 * An explicit port always overrides the default.
 * An empty explicit port (e.g. "moqt://host:") is rejected.
 *
 * No URL decoding is performed; percent-encoded components are
 * returned as-is.
 *
 * Link against moq::core.
 */

#include "types.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct moq_url {
    uint32_t    struct_size;
    moq_bytes_t scheme;   /* borrowed slice, e.g. "moqt" */
    moq_bytes_t host;     /* borrowed slice, bracketless for IPv6 */
    uint16_t    port;
    moq_bytes_t path;     /* borrowed slice, leading "/" if present */
    moq_bytes_t query;    /* borrowed raw query without "?" */
} moq_url_t;

/*
 * Initialize a moq_url_t to safe defaults.
 *
 * Safe to call with NULL (no-op).
 */
MOQ_API void moq_url_init(moq_url_t *url);

/*
 * Parse a MoQ application URL.
 *
 * Input must be scheme://authority form. Accepted schemes are
 * "moqt", "moq", and "https".
 *
 * If no port is specified, a scheme-dependent default is used:
 * moqt and moq → 4433, https → 443. An explicit port overrides
 * the default.
 *
 * Bracketed IPv6 addresses ([::1]) are unwrapped: the host
 * output contains "::1" without brackets.
 *
 * Path and query components are split on the first "?".
 * No URL decoding is performed.
 *
 * Returns MOQ_OK on success.
 * Returns MOQ_ERR_INVAL on:
 *   - NULL url or input.data
 *   - missing, empty, or unsupported scheme
 *   - empty host
 *   - empty explicit port (e.g. "moqt://host:")
 *   - zero, non-numeric, or >65535 explicit port
 *   - malformed bracketed IPv6 (missing ']')
 */
MOQ_API moq_result_t moq_url_parse(moq_bytes_t input, moq_url_t *url);

#ifdef __cplusplus
}
#endif

#endif /* MOQ_URL_H */
