#ifndef MOQ_INTEROP_CLIENT_URL_POLICY_H
#define MOQ_INTEROP_CLIENT_URL_POLICY_H

/*
 * Interop-runner URL policy. Private to this tool: the library and the service
 * endpoint keep their general "/moq" default for an unspecified WebTransport
 * path, which is right for a MoQ-specific deployment and wrong for an interop
 * relay published at the origin root. A relay URL with NO path at all is a
 * request for "/", not for "/moq".
 *
 * Only a successfully parsed, pathless https:// URL qualifies. An explicit path
 * -- including an explicit "/moq" -- is the caller's choice and is left to the
 * endpoint's own resolution, as are raw moqt:// URLs and anything that does not
 * parse.
 *
 * The helper produces the override itself rather than a bare verdict, so the
 * bytes the tool installs are the bytes a test can compare.
 */

#include <moq/url.h>

#include <stdbool.h>
#include <string.h>

/* The root mount. Static storage: cfg.wt_path borrows the span it is given. */
static const uint8_t k_interop_root_wt_path[] = "/";

/*
 * Resolve the WebTransport CONNECT path for a relay URL.
 *
 * Returns true when this tool overrides the path, writing the override to
 * *out. Returns false and leaves *out COMPLETELY untouched when the endpoint's
 * own resolution should stand -- so a caller may pre-seed *out and rely on it
 * surviving.
 */
static inline bool interop_resolve_wt_path(const char *url, moq_bytes_t *out)
{
    if (!url || !out) return false;

    moq_url_t parsed;
    moq_bytes_t input = { (const uint8_t *)url, strlen(url) };
    if (moq_url_parse(input, &parsed) != MOQ_OK) return false;

    if (parsed.scheme.len != 5 || parsed.scheme.data == NULL ||
        memcmp(parsed.scheme.data, "https", 5) != 0)
        return false;
    if (parsed.path.len != 0) return false;

    out->data = k_interop_root_wt_path;
    out->len  = sizeof(k_interop_root_wt_path) - 1;
    return true;
}

#endif /* MOQ_INTEROP_CLIENT_URL_POLICY_H */
