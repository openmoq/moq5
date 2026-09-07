/* Fuzz the relay admin request parser and content negotiation.
 *
 * The parser is pure: bytes in, one of eight statuses out. The property under
 * test is that no input reaches an unreachable status, overruns a bound, or
 * crashes -- the sanitizers supply the last part. */
#include <moqr_admin.h>

#include <stddef.h>
#include <stdint.h>
#include <string.h>

int
LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    moqr_http_request_t req;
    moqr_http_status_t st;
    char head[1024];

    memset(&req, 0, sizeof(req));
    st = moqr_http_parse((const char *)data, size, &req);

    switch (st) {
    case MOQR_HTTP_200:
    case MOQR_HTTP_400:
    case MOQR_HTTP_404:
    case MOQR_HTTP_405:
    case MOQR_HTTP_406:
    case MOQR_HTTP_431:
    case MOQR_HTTP_500:
    case MOQR_HTTP_503:
        break;
    default:
        __builtin_trap();   /* a status outside the finite table */
    }
    /* A served request is one of the CLOSED (target, representation, format)
     * triples: metrics with a real format, or the info or shards document as
     * JSON with the canonical non-index. Anything else that parsed as 200 is
     * a defect. */
    if (st == MOQR_HTTP_200) {
        bool metrics = req.target == MOQR_HTTP_TARGET_METRICS &&
                       req.rep == MOQR_HTTP_REP_METRICS &&
                       req.fmt < MOQR_OBS_FMT__COUNT;
        bool info = (req.target == MOQR_HTTP_TARGET_INFO ||
                     req.target == MOQR_HTTP_TARGET_SHARDS) &&
                    req.rep == MOQR_HTTP_REP_JSON &&
                    req.fmt == MOQR_OBS_FMT__COUNT;
        if (!metrics && !info) {
            __builtin_trap();
        }
    }

    /* Negotiation on the same bytes, so the Accept grammar is reached even
     * when the request line is nonsense. */
    {
        moqr_obs_format_t fmt = 0;
        (void)moqr_http_negotiate((const char *)data, size, &fmt);
    }

    /* Every status in the table, with a valid format, MUST render a head into
     * a buffer this size. A silent zero here would mean a request that parsed
     * successfully could not be answered at all -- the caller would have
     * nothing to send and no error to report. */
    {
        size_t n = moqr_http_write_head(st, req.rep, req.fmt, 0, head,
                                        sizeof(head));
        if (n == 0) {
            __builtin_trap();
        }
        head[n] = '\0';
        if (st != MOQR_HTTP_200 &&
            (strstr(head, MOQR_ADMIN_CT_OPENMETRICS) != NULL ||
             strstr(head, MOQR_ADMIN_CT_PROMETHEUS) != NULL ||
             strstr(head, MOQR_ADMIN_CT_JSON) != NULL)) {
            __builtin_trap();
        }
        /* A 200 head names exactly its representation's content type. */
        if (st == MOQR_HTTP_200) {
            bool json_ct = strstr(head, MOQR_ADMIN_CT_JSON) != NULL;
            bool metrics_ct = strstr(head, MOQR_ADMIN_CT_OPENMETRICS) != NULL ||
                              strstr(head, MOQR_ADMIN_CT_PROMETHEUS) != NULL;
            if (req.rep == MOQR_HTTP_REP_JSON ? (!json_ct || metrics_ct)
                                              : (json_ct || !metrics_ct)) {
                __builtin_trap();
            }
        }
        if (st == MOQR_HTTP_405 && strstr(head, "Allow: GET") == NULL) {
            __builtin_trap();
        }
        if (st == MOQR_HTTP_503 && strstr(head, "Retry-After: ") == NULL) {
            __builtin_trap();
        }
        if (strstr(head, "Connection: close") == NULL) {
            __builtin_trap();
        }
    }
    /* An invalid (representation, format) pair must never render a 200 head:
     * metrics with the non-index, JSON with a real format, or an unknown
     * representation. */
    if (moqr_http_write_head(MOQR_HTTP_200, MOQR_HTTP_REP_METRICS,
                             MOQR_OBS_FMT__COUNT, 0, head,
                             sizeof(head)) != 0 ||
        moqr_http_write_head(MOQR_HTTP_200, MOQR_HTTP_REP_JSON,
                             MOQR_OBS_FMT_OPENMETRICS_100, 0, head,
                             sizeof(head)) != 0 ||
        moqr_http_write_head(MOQR_HTTP_200, 7u, MOQR_OBS_FMT__COUNT, 0, head,
                             sizeof(head)) != 0) {
        __builtin_trap();
    }

    /* Grammar outcomes, not merely "some finite status".
     *
     * A span that does not END in exactly one CRLFCRLF can never be a served
     * request: no terminator, an early one, or trailing bytes are all 400 or a
     * bound failure, never 200. This is the one-request-per-connection claim
     * expressed where arbitrary input can attack it. */
    if (st == MOQR_HTTP_200) {
        size_t k;
        bool found = false;
        for (k = 0; k + 3 < size; k++) {
            if (data[k] == '\r' && data[k + 1] == '\n' &&
                data[k + 2] == '\r' && data[k + 3] == '\n') {
                found = true;
                break;
            }
        }
        if (!found || k + 4u != size) {
            __builtin_trap();
        }
        /* A served request is a GET for one of the three targets we publish
         * -- /metrics, /api/v1/info, /api/v1/shards -- and the parsed target
         * names the one that was asked for. */
        const char *pfx = (req.target == MOQR_HTTP_TARGET_INFO)
                              ? "GET /api/v1/info "
                          : (req.target == MOQR_HTTP_TARGET_SHARDS)
                              ? "GET /api/v1/shards " : "GET /metrics ";
        size_t pl = strlen(pfx);
        if (size < pl || memcmp(data, pfx, pl) != 0) {
            __builtin_trap();
        }
        /* An HTTP/1.1 request that is served carries a Host field. */
        if (size > pl + 8u && memcmp(data + pl, "HTTP/1.1", 8u) == 0) {
            bool host = false;
            for (size_t j = 0; j + 5u < size; j++) {
                if ((data[j] == 'H' || data[j] == 'h') &&
                    (data[j + 1] == 'o' || data[j + 1] == 'O') &&
                    (data[j + 2] == 's' || data[j + 2] == 'S') &&
                    (data[j + 3] == 't' || data[j + 3] == 'T') &&
                    data[j + 4] == ':') {
                    host = true;
                    break;
                }
            }
            if (!host) {
                __builtin_trap();
            }
        }
        if (req.target != MOQR_HTTP_TARGET_METRICS &&
            req.target != MOQR_HTTP_TARGET_INFO &&
            req.target != MOQR_HTTP_TARGET_SHARDS) {
            __builtin_trap();
        }
    }
    /* Bounds outrank syntax: anything over the head bound is 431, whatever
     * else is wrong with it. */
    if (size > MOQR_ADMIN_MAX_REQUEST && st != MOQR_HTTP_431) {
        __builtin_trap();
    }
    return 0;
}
