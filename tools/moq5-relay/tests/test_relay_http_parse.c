/* The bounded request parser and the decided content-negotiation table.
 *
 * Every case here is a rule the plan fixed before any code existed, so the
 * test file reads as that table rather than as a description of the parser. */

#include <moqr_admin.h>

#include <stdio.h>
#include <string.h>

#include "../../../tests/unit/test_support.h"

#define OM  MOQR_OBS_FMT_OPENMETRICS_100
#define PR  MOQR_OBS_FMT_PROMETHEUS_004

static int
neg(const char *accept, moqr_http_status_t want, moqr_obs_format_t want_fmt)
{
    int failures = 0;
    moqr_obs_format_t got = 0xffffffffu;
    size_t len = (accept == NULL) ? 0u : strlen(accept);
    moqr_http_status_t st = moqr_http_negotiate(accept, len, &got);
    if (st != want) {
        printf("  accept=%s: status %u, want %u\n",
               accept ? accept : "(absent)", (unsigned)st, (unsigned)want);
        failures++;
    } else if (want == MOQR_HTTP_200 && got != want_fmt) {
        printf("  accept=%s: fmt %u, want %u\n",
               accept ? accept : "(absent)", (unsigned)got, (unsigned)want_fmt);
        failures++;
    }
    return failures;
}

/* One request head: status, and on 200 the whole parse result. */
static int
parse_req(const char *req, moqr_http_status_t want,
          moqr_http_target_t want_target, moqr_http_rep_t want_rep,
          moqr_obs_format_t want_fmt)
{
    int failures = 0;
    moqr_http_request_t r;
    moqr_http_status_t st;
    memset(&r, 0xee, sizeof(r));   /* nothing may be left for luck */
    st = moqr_http_parse(req, strlen(req), &r);
    if (st != want) {
        printf("  parse: status %u, want %u for [%.40s]\n", (unsigned)st,
               (unsigned)want, req);
        return 1;
    }
    if (want != MOQR_HTTP_200) {
        return 0;
    }
    if (r.target != want_target || r.rep != want_rep || r.fmt != want_fmt) {
        printf("  parse: target %u rep %u fmt %u, want %u/%u/%u for [%.40s]\n",
               (unsigned)r.target, (unsigned)r.rep, (unsigned)r.fmt,
               (unsigned)want_target, (unsigned)want_rep, (unsigned)want_fmt,
               req);
        failures++;
    }
    return failures;
}

/* The negotiation table, exactly as decided. */
static int
test_negotiation(void)
{
    int failures = 0;

    /* A missing Accept, and the full wildcard, both select OpenMetrics. */
    failures += neg(NULL, MOQR_HTTP_200, OM);
    failures += neg("", MOQR_HTTP_200, OM);
    failures += neg("*/*", MOQR_HTTP_200, OM);
    failures += neg("*/*;q=1.0", MOQR_HTTP_200, OM);

    /* Each exact type selects itself. */
    failures += neg("application/openmetrics-text", MOQR_HTTP_200, OM);
    failures += neg("text/plain", MOQR_HTTP_200, PR);
    failures += neg("application/openmetrics-text;version=1.0.0", MOQR_HTTP_200, OM);
    failures += neg("text/plain;version=0.0.4", MOQR_HTTP_200, PR);

    /* Highest q wins, in either written order. */
    failures += neg("text/plain;q=0.9, application/openmetrics-text;q=0.1",
                    MOQR_HTTP_200, PR);
    failures += neg("application/openmetrics-text;q=0.1, text/plain;q=0.9",
                    MOQR_HTTP_200, PR);
    failures += neg("text/plain;q=0.1, application/openmetrics-text;q=0.9",
                    MOQR_HTTP_200, OM);

    /* Ties go to OpenMetrics, again independent of written order. */
    failures += neg("text/plain;q=0.5, application/openmetrics-text;q=0.5",
                    MOQR_HTTP_200, OM);
    failures += neg("application/openmetrics-text;q=0.5, text/plain;q=0.5",
                    MOQR_HTTP_200, OM);
    failures += neg("text/plain, application/openmetrics-text", MOQR_HTTP_200, OM);

    /* q=0 means NOT acceptable -- the other range must win. */
    failures += neg("application/openmetrics-text;q=0, text/plain", MOQR_HTTP_200, PR);
    failures += neg("text/plain;q=0, application/openmetrics-text", MOQR_HTTP_200, OM);
    failures += neg("text/plain;q=0.000", MOQR_HTTP_406, OM);
    /* Everything excluded, including the wildcard, is 406. */
    failures += neg("*/*;q=0", MOQR_HTTP_406, OM);
    failures += neg("text/plain;q=0, application/openmetrics-text;q=0",
                    MOQR_HTTP_406, OM);

    /* A valid range naming something we do not serve is SKIPPED, not fatal. */
    failures += neg("application/json, application/openmetrics-text",
                    MOQR_HTTP_200, OM);
    failures += neg("image/png;q=0.9, text/plain;q=0.2", MOQR_HTTP_200, PR);
    failures += neg("application/json", MOQR_HTTP_406, OM);

    /* A more specific range governs its own format outright. Excluding
     * OpenMetrics exactly while admitting everything else must yield
     * Prometheus -- taking the maximum q across matching ranges instead would
     * resurrect the excluded format and tie back to OpenMetrics. */
    failures += neg("application/openmetrics-text;q=0, */*", MOQR_HTTP_200, PR);
    failures += neg("text/plain;q=0, */*", MOQR_HTTP_200, OM);

    /* A subtype wildcard still matches its type. */
    failures += neg("text/*", MOQR_HTTP_200, PR);

    /* Malformed syntax REFUSES rather than guessing intent. */
    failures += neg("text/", MOQR_HTTP_400, OM);
    failures += neg("/plain", MOQR_HTTP_400, OM);
    failures += neg("text/plain;q=abc", MOQR_HTTP_400, OM);
    failures += neg("text/plain;q=1.5", MOQR_HTTP_400, OM);
    failures += neg("text/plain;;q=1", MOQR_HTTP_400, OM);
    failures += neg(",", MOQR_HTTP_400, OM);
    failures += neg("text plain", MOQR_HTTP_400, OM);

    /* A wild type with a named subtype is not a legal media range. It is
     * malformed rather than merely unserved: a client that cannot express
     * itself must not have its intent guessed. */
    failures += neg("*/json", MOQR_HTTP_400, OM);
    failures += neg("*/*, */json", MOQR_HTTP_400, OM);

    /* Parameters are parsed, not ignored. A version we do not produce makes
     * that representation UNAVAILABLE -- it must never be answered with a
     * different version under the name it asked for. */
    failures += neg("application/openmetrics-text;version=1.0.0", MOQR_HTTP_200, OM);
    failures += neg("application/openmetrics-text;version=9.9", MOQR_HTTP_406, OM);
    failures += neg("text/plain;version=0.0.4", MOQR_HTTP_200, PR);
    failures += neg("text/plain;version=9.9", MOQR_HTTP_406, OM);
    /* ...but another range can still serve the request. */
    failures += neg("application/openmetrics-text;version=9.9, text/plain",
                    MOQR_HTTP_200, PR);
    failures += neg("text/plain;charset=utf-8", MOQR_HTTP_200, PR);
    failures += neg("text/plain;charset=iso-8859-1", MOQR_HTTP_406, OM);

    /* Duplicate recognised parameters are malformed: last-one-wins is not a
     * decision, it is an accident of iteration order. */
    failures += neg("text/plain;q=0.5;q=0.6", MOQR_HTTP_400, OM);
    failures += neg("text/plain;version=0.0.4;version=9.9", MOQR_HTTP_400, OM);
    failures += neg("text/plain;charset=utf-8;charset=ascii", MOQR_HTTP_400, OM);

    /* Delimiters inside a quoted-string are DATA, and a quoted-pair escapes the
     * byte after it (RFC 9110 5.6.4). Splitting on every comma or semicolon
     * before recognising quotes tears these valid values apart. */
    failures += neg("text/plain;version=\"a,b\", text/plain", MOQR_HTTP_200, PR);
    failures += neg("text/plain;version=\"a;b\", text/plain", MOQR_HTTP_200, PR);
    failures += neg("text/plain;version=\"a\\\"b\", text/plain", MOQR_HTTP_200, PR);
    /* A quoted-pair that DECODES to the served version must match. Comparing
     * the raw bytes instead leaves this range unavailable and the whole request
     * unacceptable, so the escape handling is observable here rather than only
     * in the splitting. */
    failures += neg("text/plain;version=\"0.0\\.4\"", MOQR_HTTP_200, PR);
    failures += neg("application/openmetrics-text;version=\"1\\.0.0\"",
                    MOQR_HTTP_200, OM);
    failures += neg("text/plain;version=\"a\\\\b\", text/plain", MOQR_HTTP_200, PR);
    /* A parameter value is EXACTLY ONE quoted-string; two stuck together are
     * not one value. A quoted-pair escapes a byte but does not make a control
     * byte legal. And a weight is unquoted: `q="0.5"` is not a qvalue. */
    failures += neg("text/plain;version=\"0.0\"\".4\"", MOQR_HTTP_400, OM);
    failures += neg("text/plain;version=\"a\\\x01b\"", MOQR_HTTP_400, OM);
    failures += neg("text/plain;q=\"0.5\"", MOQR_HTTP_400, OM);
    /* ...while an escaped visible character stays legal. */
    failures += neg("text/plain;version=\"0.0\\.4\"", MOQR_HTTP_200, PR);

    /* ...but an unterminated quote or escape, or a control byte inside one, is
     * still malformed. */
    failures += neg("text/plain;version=\"ab", MOQR_HTTP_400, OM);
    failures += neg("text/plain;version=\"ab\\", MOQR_HTTP_400, OM);
    failures += neg("text/plain;version=\"a\x01b\"", MOQR_HTTP_400, OM);
    /* A quoted value that matches is accepted. */
    failures += neg("text/plain;version=\"0.0.4\"", MOQR_HTTP_200, PR);

    /* Parameter names are tokens and values are token-or-quoted-string. */
    failures += neg("text/plain;a b=1", MOQR_HTTP_400, OM);
    failures += neg("text/plain;q\"=1", MOQR_HTTP_400, OM);
    failures += neg("text/plain;version=\"0.0.4\"", MOQR_HTTP_200, PR);
    failures += neg("text/plain;version=\"0.0.4", MOQR_HTTP_400, OM);
    failures += neg("text/plain;version=a\"b", MOQR_HTTP_400, OM);

    /* An unrecognised media parameter NARROWS the representation, so that range
     * becomes unavailable rather than silently matching a document that does
     * not satisfy it. Another range can still serve the request. */
    failures += neg("text/plain;profile=strict", MOQR_HTTP_406, OM);
    failures += neg("text/plain;profile=strict, application/openmetrics-text",
                    MOQR_HTTP_200, OM);
    /* RFC 9110 removed accept-ext, and asks recipients to recognise q wherever
     * it appears -- so position no longer decides whether a parameter is a
     * media parameter. An unknown one narrows even after q. */
    failures += neg("text/plain;q=1;ext=1", MOQR_HTTP_406, OM);
    /* ...and before q, identically: position does not exempt a parameter. */
    failures += neg("text/plain;ext=1;q=1", MOQR_HTTP_406, OM);
    failures += neg("text/plain;q=1;ext=1, application/openmetrics-text",
                    MOQR_HTTP_200, OM);

    /* A range whose media parameters MATCH the served representation is more
     * specific than the bare type (RFC 9110 12.5.1), so its q governs that
     * format even when a broader range carries a higher q -- in either written
     * order. Excluding the exact representation excludes it. */
    failures += neg("application/openmetrics-text;version=1.0.0;q=0, "
                    "application/openmetrics-text;q=1", MOQR_HTTP_406, OM);
    failures += neg("application/openmetrics-text;q=1, "
                    "application/openmetrics-text;version=1.0.0;q=0",
                    MOQR_HTTP_406, OM);
    failures += neg("application/openmetrics-text;version=1.0.0;q=0, "
                    "application/openmetrics-text;q=1, text/plain;q=0.5",
                    MOQR_HTTP_200, PR);
    failures += neg("application/openmetrics-text;version=1.0.0;q=0.1, "
                    "application/openmetrics-text;q=1, text/plain;q=0.5",
                    MOQR_HTTP_200, PR);
    failures += neg("text/plain;version=0.0.4;q=0, text/plain, "
                    "application/openmetrics-text;q=0.5", MOQR_HTTP_200, OM);
    /* charset is a media parameter too. */
    failures += neg("text/plain;charset=utf-8;q=0, text/plain;q=1",
                    MOQR_HTTP_406, OM);
    failures += neg("text/plain;charset=utf-8;q=0, text/plain;q=1, "
                    "application/openmetrics-text;q=0.2", MOQR_HTTP_200, OM);
    /* Two matching parameters are more specific than one. */
    failures += neg("text/plain;version=0.0.4;charset=utf-8;q=0.2, "
                    "text/plain;version=0.0.4;q=1, "
                    "application/openmetrics-text;q=0.5", MOQR_HTTP_200, OM);
    failures += neg("text/plain;version=0.0.4;q=1, "
                    "text/plain;version=0.0.4;charset=utf-8;q=0.2, "
                    "application/openmetrics-text;q=0.5", MOQR_HTTP_200, OM);
    /* ...and the wildcard neighbours still stand below all of them. */
    failures += neg("text/plain;version=0.0.4;q=0, */*", MOQR_HTTP_200, OM);
    failures += neg("text/plain;version=0.0.4;q=0, text/*;q=1, "
                    "application/openmetrics-text;q=0.3", MOQR_HTTP_200, OM);
    failures += neg("application/openmetrics-text;version=1.0.0;q=1, "
                    "text/plain;q=0.5", MOQR_HTTP_200, OM);
    failures += neg("application/openmetrics-text;q=0, application/*;q=1",
                    MOQR_HTTP_406, OM);
    /* Ranges of EQUAL specificity for one format: the highest q among them
     * governs, in either written order -- never the last one written. */
    failures += neg("text/plain;q=0.9, text/plain;q=0.1, "
                    "application/openmetrics-text;q=0.5", MOQR_HTTP_200, PR);
    failures += neg("text/plain;q=0.1, text/plain;q=0.9, "
                    "application/openmetrics-text;q=0.5", MOQR_HTTP_200, PR);
    failures += neg("text/plain;version=0.0.4;q=0.9, text/plain;version=0.0.4;q=0, "
                    "application/openmetrics-text;q=0.5", MOQR_HTTP_200, PR);
    /* Rank is lexicographic: type/subtype specificity first, matching
     * parameters second. So NO number of matching parameters lifts a wildcard
     * over an exact range -- the exact exclusion beside it still governs, and
     * an exact preference beats a parametrised wildcard for selection too. */
    failures += neg("*/*;version=0.0.4;charset=utf-8;q=1, text/plain;q=0",
                    MOQR_HTTP_406, OM);
    failures += neg("*/*;version=0.0.4;charset=utf-8;q=1, text/*;q=0",
                    MOQR_HTTP_406, OM);
    failures += neg("text/*;version=0.0.4;charset=utf-8;q=1, text/plain;q=0",
                    MOQR_HTTP_406, OM);
    failures += neg("*/*;charset=utf-8;q=1, text/*;q=0, "
                    "application/openmetrics-text;q=0.2", MOQR_HTTP_200, OM);
    /* ...but among ranges of ONE type/subtype form, matching parameters do
     * refine wildcards exactly as they refine exact ranges: a parametrised
     * wildcard outranks its bare wildcard, in either written order. */
    failures += neg("application/*;version=1.0.0;q=0, application/*;q=1",
                    MOQR_HTTP_406, OM);
    failures += neg("application/*;q=1, application/*;version=1.0.0;q=0",
                    MOQR_HTTP_406, OM);
    failures += neg("*/*;charset=utf-8;q=0, */*;q=1", MOQR_HTTP_406, OM);
    failures += neg("*/*;q=1, */*;charset=utf-8;q=0", MOQR_HTTP_406, OM);
    failures += neg("text/*;charset=utf-8;q=0.1, text/*;q=1, "
                    "application/openmetrics-text;q=0.5", MOQR_HTTP_200, OM);
    failures += neg("*/*;version=1.0.0;q=0.1, */*;q=1, text/plain;q=0.5",
                    MOQR_HTTP_200, PR);
    failures += neg("text/*;version=0.0.4;q=0, text/*;q=1, "
                    "application/openmetrics-text;q=0.3", MOQR_HTTP_200, OM);
    /* Two matching parameters outrank one on a wildcard as well. */
    failures += neg("text/*;version=0.0.4;charset=utf-8;q=0, "
                    "text/*;version=0.0.4;q=1, "
                    "application/openmetrics-text;q=0.3", MOQR_HTTP_200, OM);
    /* A version we do not produce never makes a range specific: that range is
     * unavailable, and the bare exclusion beside it stands. */
    failures += neg("application/openmetrics-text;version=9.9;q=1, "
                    "application/openmetrics-text;q=0", MOQR_HTTP_406, OM);

    /* qvalue = ( "0" [ "." 0*3DIGIT ] ) / ( "1" [ "." 0*3("0") ] ): a
     * trailing dot with no digits is a legal weight (RFC 9110 12.4.2). */
    failures += neg("text/plain;q=1.", MOQR_HTTP_200, PR);
    failures += neg("text/plain;q=0., application/openmetrics-text;q=1",
                    MOQR_HTTP_200, OM);
    failures += neg("text/plain;q=0.", MOQR_HTTP_406, OM);
    failures += neg("text/plain;q=1.000", MOQR_HTTP_200, PR);
    failures += neg("text/plain;q=1.0000", MOQR_HTTP_400, OM);
    failures += neg("text/plain;q=.5", MOQR_HTTP_400, OM);
    failures += neg("text/plain;q=1.1", MOQR_HTTP_400, OM);
    failures += neg("text/plain;q=1..", MOQR_HTTP_400, OM);

    /* More media ranges than the fixed bound is a BOUND failure, not syntax. */
    failures += neg("a/1,b/2,c/3,d/4,e/5,f/6,g/7,h/8,i/9", MOQR_HTTP_431, OM);

    return failures;
}

static int
parse_is(const char *req, moqr_http_status_t want)
{
    moqr_http_request_t r;
    memset(&r, 0, sizeof(r));
    moqr_http_status_t st = moqr_http_parse(req, strlen(req), &r);
    if (st != want) {
        printf("  parse: status %u, want %u for [%.40s]\n",
               (unsigned)st, (unsigned)want, req);
        return 1;
    }
    return 0;
}

static int
test_parse(void)
{
    int failures = 0;

    failures += parse_is("GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n", MOQR_HTTP_200);
    failures += parse_is("GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n", MOQR_HTTP_200);

    /* Unknown target. */
    failures += parse_is("GET /nope HTTP/1.1\r\nHost: x\r\n\r\n", MOQR_HTTP_404);
    failures += parse_is("GET / HTTP/1.1\r\nHost: x\r\n\r\n", MOQR_HTTP_404);

    /* Any non-GET method. */
    failures += parse_is("POST /metrics HTTP/1.1\r\nHost: x\r\n\r\n", MOQR_HTTP_405);
    failures += parse_is("DELETE /metrics HTTP/1.1\r\nHost: x\r\n\r\n", MOQR_HTTP_405);
    failures += parse_is("HEAD /metrics HTTP/1.1\r\nHost: x\r\n\r\n", MOQR_HTTP_405);

    /* Malformed request lines. */
    failures += parse_is("GET /metrics\r\n\r\n", MOQR_HTTP_400);
    failures += parse_is("GET\r\n\r\n", MOQR_HTTP_400);
    failures += parse_is("GET /metrics HTTP/9.9\r\n\r\n", MOQR_HTTP_400);
    failures += parse_is("GET /metrics HTTP/1.1\r\nHost: x\r\nBadHeader\r\n\r\n", MOQR_HTTP_400);

    /* v1 takes no body: an entity header is malformed, never ignored. */
    failures += parse_is("GET /metrics HTTP/1.1\r\nHost: x\r\nContent-Length: 3\r\n\r\n",
                         MOQR_HTTP_400);
    failures += parse_is("GET /metrics HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n",
                         MOQR_HTTP_400);

    /* No query parameters in v1. */
    failures += parse_is("GET /metrics?usedonly=1 HTTP/1.1\r\nHost: x\r\n\r\n", MOQR_HTTP_404);

    /* RFC 9112 3.2: an HTTP/1.1 request carries exactly one valid Host. */
    failures += parse_is("GET /metrics HTTP/1.1\r\n\r\n", MOQR_HTTP_400);
    failures += parse_is("GET /metrics HTTP/1.1\r\nHost: a\r\nHost: b\r\n\r\n",
                         MOQR_HTTP_400);
    failures += parse_is("GET /metrics HTTP/1.1\r\nHost:\r\n\r\n",
                         MOQR_HTTP_400);
    failures += parse_is("GET /metrics HTTP/1.1\r\nHost:    \r\n\r\n",
                         MOQR_HTTP_400);
    failures += parse_is("GET /metrics HTTP/1.1\r\nHost: a b\r\n\r\n",
                         MOQR_HTTP_400);
    failures += parse_is("GET /metrics HTTP/1.1\r\nHost: example.test:9\r\n\r\n",
                         MOQR_HTTP_200);
    /* Host is `uri-host [ ":" port ]`, parsed as a grammar. The narrowing to
     * unreserved reg-name (no sub-delims, no userinfo) is project policy for a
     * closed admin surface, not a quotation of the RFC. */
    {
        static const struct { const char *host; moqr_http_status_t want; }
        hosts[] = {
            { "a,b",                MOQR_HTTP_400 },
            { "example.test/path",  MOQR_HTTP_400 },
            { "user@example.test",  MOQR_HTTP_400 },
            { "%zz",                MOQR_HTTP_400 },
            { "[::1",               MOQR_HTTP_400 },
            { "example.test:nope",  MOQR_HTTP_400 },
            /* IP-literal grammar, not character filtering. */
            { "[]",                 MOQR_HTTP_400 },
            { "[::::]",             MOQR_HTTP_400 },
            { "[.]",                MOQR_HTTP_400 },
            { "[%]",                MOQR_HTTP_400 },
            { "[1.2.3]",            MOQR_HTTP_400 },
            { "[::%25]",            MOQR_HTTP_400 },
            { "[2001:db8:::1]",     MOQR_HTTP_400 },
            /* At most ONE elision. */
            { "[1::2::3]",          MOQR_HTTP_400 },
            { "[::1::]",            MOQR_HTTP_400 },
            { "[12345::1]",         MOQR_HTTP_400 },
            { "[1:2:3:4:5:6:7:8:9]", MOQR_HTTP_400 },
            { "[1:2:3:4:5:6:7]",    MOQR_HTTP_400 },
            { "example.test:99999999", MOQR_HTTP_400 },
            /* Positive controls. */
            { "example.test",       MOQR_HTTP_200 },
            { "example.test:9090",  MOQR_HTTP_200 },
            { "192.0.2.10",         MOQR_HTTP_200 },
            { "192.0.2.10:80",      MOQR_HTTP_200 },
            { "[::1]",              MOQR_HTTP_200 },
            { "[::1]:9090",         MOQR_HTTP_200 },
            { "[2001:db8::1]",      MOQR_HTTP_200 },
            { "[1:2:3:4:5:6:7:8]",  MOQR_HTTP_200 },
            { "[::ffff:192.0.2.1]", MOQR_HTTP_200 },
            { "[fe80::1%25eth0]",   MOQR_HTTP_200 },
            /* IPvFuture: the production permits sub-delims, so admitting only
             * unreserved and colon would be a narrowing the comment does not
             * claim. The invalid shapes stay refused. */
            { "[v7.abc]",           MOQR_HTTP_200 },
            { "[vF.!$&'()*+,;=:]",  MOQR_HTTP_200 },
            { "[v7.]",              MOQR_HTTP_400 },
            { "[v.abc]",            MOQR_HTTP_400 },
            { "[v7.a%zz]",          MOQR_HTTP_400 },
        };
        for (size_t hi = 0; hi < sizeof(hosts) / sizeof(hosts[0]); hi++) {
            char req[256];
            int rn = snprintf(req, sizeof(req),
                              "GET /metrics HTTP/1.1\r\nHost: %s\r\n\r\n",
                              hosts[hi].host);
            if (rn < 0 || (size_t)rn >= sizeof(req)) {
                printf("  host fixture too long: %s\n", hosts[hi].host);
                failures++;
                continue;
            }
            if (moqr_http_parse(req, (size_t)rn, NULL) != hosts[hi].want) {
                printf("  Host: %s -> %u, want %u\n", hosts[hi].host,
                       (unsigned)moqr_http_parse(req, (size_t)rn, NULL),
                       (unsigned)hosts[hi].want);
                failures++;
            }
        }
    }

    /* HTTP/1.0 predates the requirement and is deliberately exempt. */
    failures += parse_is("GET /metrics HTTP/1.0\r\n\r\n", MOQR_HTTP_200);

    /* The read-only configuration document is a target of its own, with its
     * own served representation: JSON, negotiated by the same grammar. */
    failures += parse_req("GET /api/v1/info HTTP/1.1\r\nHost: x\r\n\r\n",
                          MOQR_HTTP_200, MOQR_HTTP_TARGET_INFO,
                          MOQR_HTTP_REP_JSON, MOQR_OBS_FMT__COUNT);
    failures += parse_req("GET /api/v1/info HTTP/1.1\r\nHost: x\r\n"
                          "Accept: */*\r\n\r\n",
                          MOQR_HTTP_200, MOQR_HTTP_TARGET_INFO,
                          MOQR_HTTP_REP_JSON, MOQR_OBS_FMT__COUNT);
    failures += parse_req("GET /api/v1/info HTTP/1.1\r\nHost: x\r\n"
                          "Accept: application/json\r\n\r\n",
                          MOQR_HTTP_200, MOQR_HTTP_TARGET_INFO,
                          MOQR_HTTP_REP_JSON, MOQR_OBS_FMT__COUNT);
    failures += parse_req("GET /api/v1/info HTTP/1.1\r\nHost: x\r\n"
                          "Accept: application/*;q=0.5, text/plain\r\n\r\n",
                          MOQR_HTTP_200, MOQR_HTTP_TARGET_INFO,
                          MOQR_HTTP_REP_JSON, MOQR_OBS_FMT__COUNT);
    failures += parse_req("GET /api/v1/info HTTP/1.1\r\nHost: x\r\n"
                          "Accept: application/json;charset=utf-8\r\n\r\n",
                          MOQR_HTTP_200, MOQR_HTTP_TARGET_INFO,
                          MOQR_HTTP_REP_JSON, MOQR_OBS_FMT__COUNT);
    /* The metrics pair is validated the same way. */
    failures += parse_req("GET /metrics HTTP/1.1\r\nHost: x\r\n"
                          "Accept: text/plain\r\n\r\n",
                          MOQR_HTTP_200, MOQR_HTTP_TARGET_METRICS,
                          MOQR_HTTP_REP_METRICS, PR);
    failures += parse_req("GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n",
                          MOQR_HTTP_200, MOQR_HTTP_TARGET_METRICS,
                          MOQR_HTTP_REP_METRICS, OM);
    /* JSON is not served with a version parameter, the metrics types are not
     * JSON, exclusions and malformed syntax keep their meaning, and the
     * precedence order is unchanged (method before target before
     * negotiation). */
    failures += parse_is("GET /api/v1/info HTTP/1.1\r\nHost: x\r\n"
                         "Accept: text/plain\r\n\r\n", MOQR_HTTP_406);
    failures += parse_is("GET /api/v1/info HTTP/1.1\r\nHost: x\r\n"
                         "Accept: application/openmetrics-text\r\n\r\n",
                         MOQR_HTTP_406);
    failures += parse_is("GET /api/v1/info HTTP/1.1\r\nHost: x\r\n"
                         "Accept: application/json;version=1\r\n\r\n",
                         MOQR_HTTP_406);
    failures += parse_is("GET /api/v1/info HTTP/1.1\r\nHost: x\r\n"
                         "Accept: application/json;charset=latin1\r\n\r\n",
                         MOQR_HTTP_406);
    failures += parse_is("GET /api/v1/info HTTP/1.1\r\nHost: x\r\n"
                         "Accept: application/json;q=0, */*\r\n\r\n",
                         MOQR_HTTP_406);
    failures += parse_is("GET /api/v1/info HTTP/1.1\r\nHost: x\r\n"
                         "Accept: application/json;q=abc\r\n\r\n",
                         MOQR_HTTP_400);
    failures += parse_is("POST /api/v1/info HTTP/1.1\r\nHost: x\r\n\r\n",
                         MOQR_HTTP_405);
    failures += parse_is("POST /api/v1/nothing HTTP/1.1\r\nHost: x\r\n\r\n",
                         MOQR_HTTP_405);
    failures += parse_is("GET /api/v1/nothing HTTP/1.1\r\nHost: x\r\n"
                         "Accept: text/html\r\n\r\n", MOQR_HTTP_404);
    /* The read-only configuration document is a target of its own. */
    failures += parse_is("GET /api/v1/info HTTP/1.1\r\nHost: x\r\n\r\n",
                         MOQR_HTTP_200);
    /* ...and nothing near it is: no trailing slash, no suffix, no query, and
     * the shards document is not served. */
    failures += parse_is("GET /api/v1/info/ HTTP/1.1\r\nHost: x\r\n\r\n",
                         MOQR_HTTP_404);
    failures += parse_is("GET /api/v1/infoX HTTP/1.1\r\nHost: x\r\n\r\n",
                         MOQR_HTTP_404);
    failures += parse_is("GET /api/v1/info?x HTTP/1.1\r\nHost: x\r\n\r\n",
                         MOQR_HTTP_404);
    /* The shards document is a target of its own: JSON only, exact bytes. */
    failures += parse_req("GET /api/v1/shards HTTP/1.1\r\nHost: x\r\n\r\n",
                          MOQR_HTTP_200, MOQR_HTTP_TARGET_SHARDS,
                          MOQR_HTTP_REP_JSON, MOQR_OBS_FMT__COUNT);
    failures += parse_req("GET /api/v1/shards HTTP/1.1\r\nHost: x\r\n"
                          "Accept: application/json\r\n\r\n",
                          MOQR_HTTP_200, MOQR_HTTP_TARGET_SHARDS,
                          MOQR_HTTP_REP_JSON, MOQR_OBS_FMT__COUNT);
    failures += parse_is("GET /api/v1/shards HTTP/1.1\r\nHost: x\r\n"
                         "Accept: text/plain\r\n\r\n", MOQR_HTTP_406);
    failures += parse_is("GET /api/v1/shards HTTP/1.1\r\nHost: x\r\n"
                         "Accept: application/openmetrics-text\r\n\r\n",
                         MOQR_HTTP_406);
    failures += parse_is("GET /api/v1/shards/ HTTP/1.1\r\nHost: x\r\n\r\n",
                         MOQR_HTTP_404);
    failures += parse_is("GET /api/v1/shardsX HTTP/1.1\r\nHost: x\r\n\r\n",
                         MOQR_HTTP_404);
    failures += parse_is("GET /api/v1/shards?lane=0 HTTP/1.1\r\nHost: x\r\n\r\n",
                         MOQR_HTTP_404);
    failures += parse_is("POST /api/v1/shards HTTP/1.1\r\nHost: x\r\n\r\n",
                         MOQR_HTTP_405);
    failures += parse_is("GET /api/v1 HTTP/1.1\r\nHost: x\r\n\r\n",
                         MOQR_HTTP_404);

    /* Exactly one final CRLFCRLF: absent, partial, early or trailing is 400. */
    failures += parse_is("GET /metrics HTTP/1.1\r\nHost: x\r\n", MOQR_HTTP_400);
    failures += parse_is("GET /metrics HTTP/1.1\r\nHost: x\r\n\r", MOQR_HTTP_400);
    failures += parse_is("GET /metrics HTTP/1.1", MOQR_HTTP_400);
    failures += parse_is("", MOQR_HTTP_400);
    failures += parse_is("GET /metrics HTTP/1.1\r\nHost: x\r\n", MOQR_HTTP_400);

    /* A present Accept field with an empty or whitespace-only value is
     * malformed -- it is a different assertion from omitting the field. */
    failures += parse_is("GET /metrics HTTP/1.1\r\nHost: x\r\nAccept:\r\n\r\n",
                         MOQR_HTTP_400);
    failures += parse_is("GET /metrics HTTP/1.1\r\nHost: x\r\nAccept:   \r\n\r\n",
                         MOQR_HTTP_400);
    failures += parse_is(
        "GET /metrics HTTP/1.1\r\nHost: x\r\nAccept:\r\nAccept: text/plain\r\n\r\n",
        MOQR_HTTP_400);
    /* ...while omitting it entirely selects OpenMetrics. */
    {
        moqr_http_request_t r;
        const char *req = "GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n";
        memset(&r, 0, sizeof(r));
        MOQ_TEST_CHECK_EQ_INT((int)moqr_http_parse(req, strlen(req), &r),
                              (int)MOQR_HTTP_200);
        MOQ_TEST_CHECK_EQ_INT((int)r.fmt, (int)OM);
    }

    /* The accepted request grammar is closed. */
    /* A field name containing SP -- the classic smuggling shape. */
    failures += parse_is("GET /metrics HTTP/1.1\r\nHost: x\r\nBad Header: 1\r\n\r\n",
                         MOQR_HTTP_400);
    failures += parse_is("GET /metrics HTTP/1.1\r\nHost: x\r\nX : 1\r\n\r\n",
                         MOQR_HTTP_400);
    /* An obsolete line fold. */
    failures += parse_is("GET /metrics HTTP/1.1\r\nHost: x\r\nX: a\r\n b\r\n\r\n",
                         MOQR_HTTP_400);
    /* A control byte in a field value. */
    failures += parse_is("GET /metrics HTTP/1.1\r\nHost: x\r\nX: a\x01b\r\n\r\n",
                         MOQR_HTTP_400);
    /* Protocol changes and framing negotiation are refused, not ignored. */
    failures += parse_is("GET /metrics HTTP/1.1\r\nHost: x\r\nUpgrade: h2c\r\n\r\n",
                         MOQR_HTTP_400);
    failures += parse_is("GET /metrics HTTP/1.1\r\nHost: x\r\nExpect: 100-continue\r\n\r\n",
                         MOQR_HTTP_400);
    failures += parse_is("GET /metrics HTTP/1.1\r\nHost: x\r\nTE: trailers\r\n\r\n",
                         MOQR_HTTP_400);
    failures += parse_is("GET /metrics HTTP/1.1\r\nHost: x\r\nTrailer: X\r\n\r\n",
                         MOQR_HTTP_400);
    /* A method that is not a token is syntax, not an unsupported method. */
    failures += parse_is("GE T /metrics HTTP/1.1\r\nHost: x\r\n\r\n", MOQR_HTTP_400);
    failures += parse_is("G\x01T /metrics HTTP/1.1\r\nHost: x\r\n\r\n", MOQR_HTTP_400);
    /* A target that is not an absolute path is syntax. */
    failures += parse_is("GET metrics HTTP/1.1\r\nHost: x\r\n\r\n", MOQR_HTTP_400);
    /* Exactly one terminated head per span: a pipelined second request or an
     * unframed body is refused rather than answered from the prefix. */
    failures += parse_is(
        "GET /metrics HTTP/1.1\r\nHost: x\r\n\r\nGET /metrics HTTP/1.1\r\n\r\n",
        MOQR_HTTP_400);
    failures += parse_is("GET /metrics HTTP/1.1\r\nHost: x\r\n\r\nxx", MOQR_HTTP_400);
    /* An unknown but syntactically valid header stays skippable. */
    failures += parse_is("GET /metrics HTTP/1.1\r\nHost: x\r\nX-Whatever: 1\r\n\r\n",
                         MOQR_HTTP_200);

    /* Negotiation reached through the parser. */
    failures += parse_is("GET /metrics HTTP/1.1\r\nHost: x\r\nAccept: application/json\r\n\r\n",
                         MOQR_HTTP_406);
    failures += parse_is("GET /metrics HTTP/1.1\r\nHost: x\r\nAccept: text/\r\n\r\n",
                         MOQR_HTTP_400);

    /* Duplicate Accept headers combine in order, as one comma-separated field. */
    {
        moqr_http_request_t r;
        const char *req =
            "GET /metrics HTTP/1.1\r\nHost: x\r\n"
            "Accept: text/plain;q=0.9\r\n"
            "Accept: application/openmetrics-text;q=0.1\r\n\r\n";
        memset(&r, 0, sizeof(r));
        MOQ_TEST_CHECK_EQ_INT((int)moqr_http_parse(req, strlen(req), &r),
                              (int)MOQR_HTTP_200);
        MOQ_TEST_CHECK_EQ_INT((int)r.fmt, (int)PR);
    }

    return failures;
}

/* Precedence is part of the contract: a request can break several rules at
 * once, and which one is reported must not depend on code order. */
static int
test_precedence(void)
{
    int failures = 0;
    char big[MOQR_ADMIN_MAX_REQUEST * 2];

    /* bounds beat everything: an over-long head with a bad method and target. */
    memset(big, 'x', sizeof(big));
    memcpy(big, "POST /nope HTTP/1.1\r\nHost: x\r\nX: ", 24);
    memcpy(big + sizeof(big) - 4, "\r\n\r\n", 4);
    failures += (moqr_http_parse(big, sizeof(big), NULL) == MOQR_HTTP_431) ? 0 : 1;

    /* The TOTAL head bound is load-bearing on its own: this request keeps
     * every line and the header count well inside their limits and exceeds
     * only the whole-head size. */
    {
        char many[MOQR_ADMIN_MAX_REQUEST * 2u];
        size_t at = 0;
        const char *rl = "GET /metrics HTTP/1.1\r\nHost: x\r\n";
        memcpy(many, rl, strlen(rl));
        at = strlen(rl);
        for (int i = 0; i < 6; i++) {
            memcpy(many + at, "X: ", 3);
            at += 3;
            memset(many + at, 'a', 400);
            at += 400;
            memcpy(many + at, "\r\n", 2);
            at += 2;
        }
        memcpy(many + at, "\r\n", 2);
        at += 2;
        if (at <= MOQR_ADMIN_MAX_REQUEST || at > sizeof(many)) {
            printf("  the oversize fixture does not sit inside its buffer\n");
            failures++;
        }
        failures += (moqr_http_parse(many, at, NULL) == MOQR_HTTP_431) ? 0 : 1;
    }

    /* The PER-LINE bound is load-bearing on its own: one long but otherwise
     * well-formed header, with the whole head and the header count both well
     * inside their limits. */
    {
        char one[MOQR_ADMIN_MAX_REQUEST];
        size_t at = 0;
        const char *rl = "GET /metrics HTTP/1.1\r\nHost: x\r\n";
        memcpy(one, rl, strlen(rl));
        at = strlen(rl);
        memcpy(one + at, "X: ", 3);
        at += 3;
        memset(one + at, 'a', MOQR_ADMIN_MAX_LINE + 32u);
        at += MOQR_ADMIN_MAX_LINE + 32u;
        memcpy(one + at, "\r\n\r\n", 4);
        at += 4;
        if (at > MOQR_ADMIN_MAX_REQUEST) {
            printf("  the long-line fixture also breaks the head bound\n");
            failures++;
        }
        failures += (moqr_http_parse(one, at, NULL) == MOQR_HTTP_431) ? 0 : 1;
    }

    /* Bounds beat the terminator rule too: an over-bound span that also has an
     * early terminator is 431, not 400. */
    {
        char big2[MOQR_ADMIN_MAX_REQUEST * 2u];
        size_t at = 0;
        const char *rl = "GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n";
        memcpy(big2, rl, strlen(rl));
        at = strlen(rl);
        memset(big2 + at, 'x', MOQR_ADMIN_MAX_REQUEST + 8u);
        at += MOQR_ADMIN_MAX_REQUEST + 8u;
        failures += (moqr_http_parse(big2, at, NULL) == MOQR_HTTP_431) ? 0 : 1;
    }

    /* syntax beats method: a bad version with a bad method is 400. */
    failures += parse_is("POST /metrics HTTP/9.9\r\n\r\n", MOQR_HTTP_400);
    /* method beats target: POST to an unknown path is 405, not 404. */
    failures += parse_is("POST /nope HTTP/1.1\r\nHost: x\r\n\r\n", MOQR_HTTP_405);
    /* target beats negotiation: unknown path with unusable Accept is 404. */
    failures += parse_is("GET /nope HTTP/1.1\r\nHost: x\r\nAccept: application/json\r\n\r\n",
                         MOQR_HTTP_404);
    return failures;
}

/* No error response may carry a metrics body, and 405 must advertise Allow. */
static int
test_response_heads(void)
{
    int failures = 0;
    char buf[512];
    static const moqr_http_status_t errs[] = {
        MOQR_HTTP_400, MOQR_HTTP_404, MOQR_HTTP_405, MOQR_HTTP_406,
        MOQR_HTTP_431, MOQR_HTTP_500, MOQR_HTTP_503,
    };

    for (size_t i = 0; i < sizeof(errs) / sizeof(errs[0]); i++) {
        size_t n = moqr_http_write_head(errs[i], MOQR_HTTP_REP_METRICS, OM, 0, buf, sizeof(buf));
        if (n == 0 || n >= sizeof(buf)) {
            printf("  status %u produced no head\n", (unsigned)errs[i]);
            failures++;
            continue;
        }
        buf[n] = '\0';
        if (strstr(buf, "moqrelay_") != NULL) {
            printf("  status %u leaked a metrics body\n", (unsigned)errs[i]);
            failures++;
        }
        if (strstr(buf, MOQR_ADMIN_CT_OPENMETRICS) != NULL ||
            strstr(buf, MOQR_ADMIN_CT_PROMETHEUS) != NULL) {
            printf("  status %u claims a metrics content type\n",
                   (unsigned)errs[i]);
            failures++;
        }
        if (strstr(buf, "Connection: close") == NULL) {
            printf("  status %u does not close the connection\n",
                   (unsigned)errs[i]);
            failures++;
        }
    }

    /* 405 carries Allow: GET. */
    {
        size_t n = moqr_http_write_head(MOQR_HTTP_405, MOQR_HTTP_REP_METRICS, OM, 0, buf, sizeof(buf));
        buf[n] = '\0';
        if (strstr(buf, "Allow: GET") == NULL) {
            printf("  405 does not advertise Allow: GET\n");
            failures++;
        }
    }
    /* 503 carries a fixed Retry-After. */
    {
        size_t n = moqr_http_write_head(MOQR_HTTP_503, MOQR_HTTP_REP_METRICS, OM, 0, buf, sizeof(buf));
        buf[n] = '\0';
        if (strstr(buf, "Retry-After: 1") == NULL) {
            printf("  503 does not carry a fixed Retry-After\n");
            failures++;
        }
    }
    /* 200 carries exactly one precise content type, matching the format. */
    {
        size_t n = moqr_http_write_head(MOQR_HTTP_200, MOQR_HTTP_REP_METRICS, OM, 42, buf, sizeof(buf));
        buf[n] = '\0';
        if (strstr(buf, MOQR_ADMIN_CT_OPENMETRICS) == NULL ||
            strstr(buf, MOQR_ADMIN_CT_PROMETHEUS) != NULL) {
            printf("  200/OpenMetrics content type wrong\n");
            failures++;
        }
        if (strstr(buf, "Content-Length: 42") == NULL) {
            printf("  200 does not declare its length\n");
            failures++;
        }
        n = moqr_http_write_head(MOQR_HTTP_200, MOQR_HTTP_REP_METRICS, PR, 7, buf, sizeof(buf));
        buf[n] = '\0';
        if (strstr(buf, MOQR_ADMIN_CT_PROMETHEUS) == NULL ||
            strstr(buf, MOQR_ADMIN_CT_OPENMETRICS) != NULL) {
            printf("  200/Prometheus content type wrong\n");
            failures++;
        }
    }
    /* An unknown format enum must never render a 200 head. Defaulting it to
     * OpenMetrics would advertise a representation nobody selected, and the
     * caller would index a body array with the same bad value. */
    {
        /* The (representation, format) pair is validated as a pair: JSON
         * carries the canonical non-index and its own content type; JSON
         * with a real format, or metrics with the non-index, is refused. */
        size_t jn = moqr_http_write_head(MOQR_HTTP_200, MOQR_HTTP_REP_JSON,
                                         MOQR_OBS_FMT__COUNT, 9, buf,
                                         sizeof(buf));
        if (jn == 0 || strstr(buf, "Content-Type: " MOQR_ADMIN_CT_JSON "\r\n") == NULL ||
            strstr(buf, "Content-Length: 9\r\n") == NULL) {
            printf("  a JSON 200 head is wrong: [%.120s]\n", buf);
            failures++;
        }
        if (moqr_http_write_head(MOQR_HTTP_200, MOQR_HTTP_REP_JSON, OM, 1, buf,
                                 sizeof(buf)) != 0 ||
            moqr_http_write_head(MOQR_HTTP_200, MOQR_HTTP_REP_JSON, PR, 1, buf,
                                 sizeof(buf)) != 0) {
            printf("  a JSON representation with a metrics format was labelled\n");
            failures++;
        }
        if (moqr_http_write_head(MOQR_HTTP_200, 7u, OM, 1, buf, sizeof(buf)) != 0) {
            printf("  an unknown representation was labelled\n");
            failures++;
        }
        if (moqr_http_write_head(MOQR_HTTP_200, MOQR_HTTP_REP_METRICS, MOQR_OBS_FMT__COUNT, 1, buf,
                                 sizeof(buf)) != 0) {
            printf("  an unknown format enum rendered a 200 head\n");
            failures++;
        }
        if (moqr_http_write_head(MOQR_HTTP_200, MOQR_HTTP_REP_METRICS, 99u, 1, buf,
                                 sizeof(buf)) != 0) {
            printf("  format 99 rendered a 200 head\n");
            failures++;
        }
    }
    /* A capacity too small must refuse, never truncate a head onto the wire. */
    {
        char tiny[8];
        if (moqr_http_write_head(MOQR_HTTP_200, MOQR_HTTP_REP_METRICS, OM, 1, tiny, sizeof(tiny)) != 0) {
            printf("  a too-small buffer was not refused\n");
            failures++;
        }
    }
    return failures;
}

int
main(void)
{
    int failures = 0;
    failures += test_negotiation();
    failures += test_parse();
    failures += test_precedence();
    failures += test_response_heads();
    if (failures != 0) {
        printf("FAIL: %d http parser/negotiation violation(s)\n", failures);
        return 1;
    }
    printf("OK\n");
    return 0;
}
