/*
 * The bounded request parser and content negotiation.
 *
 * Pure text in, status out. No allocation, no clock, no I/O: every bound is a
 * compile-time constant and every refusal is one of the eight statuses in the
 * finite table.
 */
#include "moqr_admin.h"

#include <stdio.h>
#include <string.h>

/* Media ranges we serve, most specific spelling first. */
#define MT_OM_TYPE "application"
#define MT_OM_SUB  "openmetrics-text"
#define MT_PR_TYPE "text"
#define MT_PR_SUB  "plain"

/* The exact representations we serve. A request that names a version we do not
 * produce must be treated as unavailable, never quietly answered with a
 * different version under the name it asked for. */
#define OM_VERSION "1.0.0"
#define PR_VERSION "0.0.4"
#define SERVED_CHARSET "utf-8"

/* q is carried as milli-units so the comparison is exact integer arithmetic;
 * a float here would make ties depend on rounding. */
#define Q_MAX 1000

/* How specifically a range named a format. A more specific range wins outright,
 * which is what lets `text/plain;q=0` exclude one format while the full wildcard still
 * admits the other.
 *
 * The rank is LEXICOGRAPHIC (RFC 9110 12.5.1): the type/subtype form first,
 * then the number of media parameters that MATCH the served representation.
 * So `text/plain;version=0.0.4` outranks `text/plain`, a type wildcard with a
 * matching charset outranks the bare type wildcard, and no number of matching
 * parameters lifts a wildcard over an exact range. SPEC_PARAM_STEPS exceeds
 * the most parameters a range can match, so the two orders never mix. */
#define SPEC_NONE     (-1)
#define SPEC_WILDCARD  0   /* the full wildcard      */
#define SPEC_TYPE      1   /* type with a wild subtype */
#define SPEC_EXACT     2
#define SPEC_PARAM_STEPS 4 /* rank = form * STEPS + matching parameters */

/* A served representation: what one target can produce. The negotiation runs
 * over a target's table; the FIRST entry is the default for an absent Accept
 * and wins ties. `version` NULL means no version parameter is served, so a
 * range naming one is unavailable rather than matched. */
typedef struct rep_desc {
    const char *type;
    const char *sub;
    const char *version;
    const char *charset;
} rep_desc_t;

#define MOQR_HTTP_MAX_REPS 2u

/* /metrics: OpenMetrics first (the decided default and tie winner). */
static const rep_desc_t k_metrics_reps[MOQR_OBS_FMT__COUNT] = {
    { MT_OM_TYPE, MT_OM_SUB, OM_VERSION, SERVED_CHARSET },
    { MT_PR_TYPE, MT_PR_SUB, PR_VERSION, SERVED_CHARSET },
};
static const moqr_obs_format_t k_metrics_fmt[MOQR_OBS_FMT__COUNT] = {
    MOQR_OBS_FMT_OPENMETRICS_100, MOQR_OBS_FMT_PROMETHEUS_004,
};

/* /api/v1/info: one JSON representation, no version parameter. */
static const rep_desc_t k_json_reps[1] = {
    { "application", "json", NULL, SERVED_CHARSET },
};

static bool
is_tchar(char c)
{
    if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
        (c >= '0' && c <= '9')) {
        return true;
    }
    switch (c) {
    case '!': case '#': case '$': case '%': case '&': case '\'': case '*':
    case '+': case '-': case '.': case '^': case '_': case '`': case '|':
    case '~':
        return true;
    default:
        return false;
    }
}

/* Case-SENSITIVE. Methods, versions and paths are case-sensitive in HTTP; only
 * header names and media-type tokens are not. */
static bool
token_eq(const char *s, size_t n, const char *lit)
{
    size_t l = strlen(lit);
    return n == l && memcmp(s, lit, n) == 0;
}

/* Case-INSENSITIVE. `lit` must be lower-case. */
static bool
token_is(const char *s, size_t n, const char *lit)
{
    size_t l = strlen(lit);
    if (n != l) {
        return false;
    }
    for (size_t i = 0; i < n; i++) {
        char a = s[i];
        char b = lit[i];
        if (a >= 'A' && a <= 'Z') {
            a = (char)(a - 'A' + 'a');
        }
        if (a != b) {
            return false;
        }
    }
    return true;
}

/* Parse a q-value. Only 0[.ddd] and 1[.000] are legal; anything else -- a
 * letter, an out-of-range value, a missing digit -- is malformed syntax, not a
 * value to clamp. Clamping would silently accept a request we did not
 * understand. */
static bool
parse_q(const char *s, size_t n, int *out)
{
    size_t i = 0;
    int whole;
    int frac = 0;
    int scale = 100;

    if (n == 0) {
        return false;
    }
    if (s[0] == '0') {
        whole = 0;
    } else if (s[0] == '1') {
        whole = 1;
    } else {
        return false;
    }
    i = 1;
    if (i < n) {
        if (s[i] != '.') {
            return false;
        }
        i++;
        /* qvalue = ( "0" [ "." 0*3DIGIT ] ) / ( "1" [ "." 0*3("0") ] ): the
         * fraction may be empty, so `1.` and `0.` are weights. */
        size_t digits = 0;
        while (i < n) {
            if (s[i] < '0' || s[i] > '9') {
                return false;
            }
            if (digits >= 3) {
                return false;
            }
            frac += (s[i] - '0') * scale;
            scale /= 10;
            digits++;
            i++;
        }
    }
    if (whole == 1 && frac != 0) {
        return false;       /* 1.5 is out of range */
    }
    *out = (whole == 1) ? Q_MAX : frac;
    return true;
}

/* Find the next top-level occurrence of `delim`, honouring quoted-string.
 *
 * RFC 9110 5.6.4: inside a quoted-string a delimiter is DATA, and a quoted-pair
 * escapes the byte after the backslash. Splitting on every comma or semicolon
 * before recognising quotes therefore tears valid field values apart -- which
 * is exactly what rejected `text/plain;version="a,b"` as malformed.
 *
 * Returns the index of the delimiter, or `n` if there is none. Sets *bad when
 * the span contains an unterminated quoted-string, an unterminated escape, or
 * a control byte inside quotes. */
static size_t
scan_to(const char *s, size_t n, char delim, bool *bad)
{
    bool in_q = false;
    size_t i = 0;

    while (i < n) {
        char c = s[i];
        if (in_q) {
            if (c == '\\') {
                unsigned char e;
                if (i + 1u >= n) {
                    *bad = true;        /* an escape with nothing to escape */
                    return n;
                }
                /* quoted-pair = "\\" ( HTAB / SP / VCHAR / obs-text ). The
                 * escaped byte is VALIDATED, not skipped: a backslash does not
                 * make a control byte legal. */
                e = (unsigned char)s[i + 1u];
                if (e != '\t' && (e < 0x20u || e == 0x7fu)) {
                    *bad = true;
                    return n;
                }
                i += 2u;
                continue;
            }
            if (c == '"') {
                in_q = false;
                i++;
                continue;
            }
            {
                unsigned char u = (unsigned char)c;
                if (u != '\t' && (u < 0x20u || u == 0x7fu)) {
                    *bad = true;
                    return n;
                }
            }
            i++;
            continue;
        }
        if (c == '"') {
            in_q = true;
            i++;
            continue;
        }
        if (c == delim) {
            return i;
        }
        i++;
    }
    if (in_q) {
        *bad = true;                    /* an unterminated quoted-string */
    }
    return n;
}

/* Compare a possibly quoted parameter value against a literal, decoding
 * quoted-pairs. Case-insensitive, like the other media-type tokens. */
static bool
value_is(const char *v, size_t n, const char *lit)
{
    size_t li = 0;
    size_t i = 0;
    size_t ll = strlen(lit);
    bool quoted = (n >= 2u && v[0] == '"' && v[n - 1u] == '"');

    if (quoted) {
        v++;
        n -= 2u;
    }
    while (i < n) {
        char a = v[i];
        if (quoted && a == '\\') {
            i++;
            if (i >= n) {
                return false;
            }
            a = v[i];
        }
        if (a >= 'A' && a <= 'Z') {
            a = (char)(a - 'A' + 'a');
        }
        if (li >= ll || a != lit[li]) {
            return false;
        }
        li++;
        i++;
    }
    return li == ll;
}

static bool
is_hexdig(char c)
{
    return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') ||
           (c >= 'A' && c <= 'F');
}

/* -- IP-literal grammar (RFC 3986 3.2.2, RFC 6874) -----------------------
 *
 * Character filtering is not a grammar: `[::::]`, `[.]`, `[%]` and `[1.2.3]`
 * all contain only "hex, colon, dot" and none of them is an address. These
 * parse the actual productions, bounded and without allocation. */

static bool
parse_ipv4(const char *v, size_t n)
{
    size_t i = 0;
    uint32_t octets = 0;

    while (octets < 4u) {
        uint32_t val = 0;
        size_t digits = 0;
        while (i < n && v[i] >= '0' && v[i] <= '9' && digits < 3u) {
            val = (val * 10u) + (uint32_t)(v[i] - '0');
            digits++;
            i++;
        }
        if (digits == 0u || val > 255u) {
            return false;
        }
        if (digits > 1u && v[i - digits] == '0') {
            return false;           /* a leading zero is not this grammar */
        }
        octets++;
        if (octets == 4u) {
            break;
        }
        if (i >= n || v[i] != '.') {
            return false;
        }
        i++;
    }
    return i == n;
}

/* IPv6address: eight 16-bit groups, at most one "::" elision, optionally
 * ending in an embedded IPv4address. */
static bool
parse_ipv6(const char *v, size_t n)
{
    size_t i = 0;
    uint32_t groups = 0;
    bool elided = false;

    if (n == 0) {
        return false;
    }
    if (v[0] == ':') {
        if (n < 2u || v[1] != ':') {
            return false;           /* a single leading colon is not legal */
        }
        elided = true;
        i = 2u;
        if (i == n) {
            return true;            /* "::" is the unspecified address */
        }
    }
    for (;;) {
        size_t start = i;
        size_t digits = 0;

        while (i < n && is_hexdig(v[i]) && digits < 4u) {
            i++;
            digits++;
        }
        if (digits == 0u) {
            return false;           /* an empty group where one is required */
        }
        if (i < n && v[i] == '.') {
            /* An embedded IPv4address occupies the last two groups. */
            if (!parse_ipv4(v + start, n - start)) {
                return false;
            }
            groups += 2u;
            i = n;
            break;
        }
        groups++;
        if (i == n) {
            break;
        }
        if (v[i] != ':') {
            return false;
        }
        i++;
        if (i < n && v[i] == ':') {
            if (elided) {
                return false;       /* at most one elision */
            }
            elided = true;
            i++;
            if (i == n) {
                break;
            }
        } else if (i == n) {
            return false;           /* a trailing single colon */
        }
    }
    if (groups > 8u) {
        return false;
    }
    /* An elision must stand for at least one omitted group. */
    return elided ? (groups < 8u) : (groups == 8u);
}

/* IP-literal = "[" ( IPv6address [ "%25" ZoneID ] / IPvFuture ) "]" */
static bool
parse_ip_literal(const char *v, size_t n)
{
    size_t zone = n;
    size_t i;

    if (n == 0) {
        return false;
    }
    if (v[0] == 'v' || v[0] == 'V') {
        /* IPvFuture = "v" 1*HEXDIG "." 1*( unreserved / sub-delims / ":" ) */
        size_t d = 1;
        while (d < n && is_hexdig(v[d])) {
            d++;
        }
        if (d == 1u || d >= n || v[d] != '.') {
            return false;
        }
        d++;
        if (d >= n) {
            return false;
        }
        for (; d < n; d++) {
            char c = v[d];
            /* unreserved */
            if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                (c >= '0' && c <= '9') || c == '-' || c == '.' || c == '_' ||
                c == '~' || c == ':') {
                continue;
            }
            /* sub-delims, which this production explicitly permits. Admitting
             * only unreserved and colon would have been a narrowing, and the
             * comment above claims the real production. */
            switch (c) {
            case '!': case '$': case '&': case '\'': case '(': case ')':
            case '*': case '+': case ',': case ';': case '=':
                continue;
            default:
                break;
            }
            return false;
        }
        return true;
    }
    /* A zone identifier is introduced by the percent-encoded "%25". */
    for (i = 0; i + 2u < n; i++) {
        if (v[i] == '%' && v[i + 1] == '2' && v[i + 2] == '5') {
            zone = i;
            break;
        }
    }
    if (!parse_ipv6(v, zone)) {
        return false;
    }
    if (zone == n) {
        return true;
    }
    /* ZoneID = 1*( unreserved / pct-encoded ) -- and it may not be empty. */
    {
        size_t z = zone + 3u;
        if (z >= n) {
            return false;
        }
        while (z < n) {
            char c = v[z];
            if (c == '%') {
                if (z + 2u >= n || !is_hexdig(v[z + 1]) ||
                    !is_hexdig(v[z + 2])) {
                    return false;
                }
                z += 3u;
                continue;
            }
            if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                (c >= '0' && c <= '9') || c == '-' || c == '.' || c == '_' ||
                c == '~') {
                z++;
                continue;
            }
            return false;
        }
    }
    return true;
}

/* Validate a Host field value as `uri-host [ ":" port ]` (RFC 9112 3.2).
 *
 * Bounded and allocation-free, and deliberately STRICTER than RFC 3986's
 * reg-name: this admin surface is addressed by a name or a literal address, so
 * the sub-delims that reg-name also permits (`,`, `!`, `$`, ...) and userinfo
 * are refused. That narrowing is project policy for a closed surface, not a
 * quotation of the RFC. */
static bool
host_is_valid(const char *v, size_t n)
{
    size_t i = 0;
    size_t host_end;

    if (n == 0) {
        return false;
    }
    if (v[0] == '[') {
        size_t close = 0;
        bool found = false;
        for (i = 1; i < n; i++) {
            if (v[i] == ']') {
                close = i;
                found = true;
                break;
            }
        }
        if (!found || close == 1u) {
            return false;           /* unclosed, or an empty literal */
        }
        if (!parse_ip_literal(v + 1, close - 1u)) {
            return false;
        }
        host_end = close + 1u;
    } else {
        /* reg-name or IPv4address: unreserved plus percent-encoding. */
        for (i = 0; i < n; i++) {
            char c = v[i];
            if (c == ':') {
                break;
            }
            if (c == '%') {
                if (i + 2u >= n || !is_hexdig(v[i + 1]) ||
                    !is_hexdig(v[i + 2])) {
                    return false;   /* an invalid percent escape */
                }
                i += 2u;
                continue;
            }
            if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                (c >= '0' && c <= '9') || c == '-' || c == '.' || c == '_' ||
                c == '~') {
                continue;
            }
            return false;
        }
        host_end = i;
        if (host_end == 0u) {
            return false;
        }
    }
    if (host_end == n) {
        return true;
    }
    if (v[host_end] != ':') {
        return false;
    }
    if (n - host_end - 1u > 5u) {
        return false;
    }
    for (i = host_end + 1u; i < n; i++) {
        if (v[i] < '0' || v[i] > '9') {
            return false;
        }
    }
    return true;
}

static void
trim(const char **s, size_t *n)
{
    while (*n > 0 && (**s == ' ' || **s == '\t')) {
        (*s)++;
        (*n)--;
    }
    while (*n > 0 && ((*s)[*n - 1] == ' ' || (*s)[*n - 1] == '\t')) {
        (*n)--;
    }
}

/* One media range: `type/subtype` plus optional `;name=value` parameters. */
static moqr_http_status_t
parse_range(const char *s, size_t n, int *out_q, int spec[MOQR_HTTP_MAX_REPS],
            const rep_desc_t *reps, size_t nreps)
{
    const char *slash;
    const char *type;
    const char *sub;
    size_t type_n;
    size_t sub_n;
    size_t i;
    int q = Q_MAX;
    bool seen_q = false;
    bool seen_version = false;
    bool seen_charset = false;
    bool charset_matched = false;
    bool unavailable = false;
    const char *want_version = NULL;
    size_t want_version_n = 0;

    trim(&s, &n);
    if (n == 0) {
        return MOQR_HTTP_400;
    }

    /* Split off parameters at the first TOP-LEVEL ';'. */
    size_t head_n;
    {
        bool bad = false;
        head_n = scan_to(s, n, ';', &bad);
        if (bad) {
            return MOQR_HTTP_400;
        }
    }

    const char *head = s;
    size_t hn = head_n;
    trim(&head, &hn);
    slash = memchr(head, '/', hn);
    if (slash == NULL) {
        return MOQR_HTTP_400;
    }
    type = head;
    type_n = (size_t)(slash - head);
    sub = slash + 1;
    sub_n = hn - type_n - 1;
    if (type_n == 0 || sub_n == 0) {
        return MOQR_HTTP_400;
    }
    for (i = 0; i < type_n; i++) {
        if (!is_tchar(type[i]) && type[i] != '*') {
            return MOQR_HTTP_400;
        }
    }
    for (i = 0; i < sub_n; i++) {
        if (!is_tchar(sub[i]) && sub[i] != '*') {
            return MOQR_HTTP_400;
        }
    }

    /* Parameters. */
    i = head_n;
    while (i < n) {
        if (s[i] != ';') {
            return MOQR_HTTP_400;
        }
        i++;
        size_t start = i;
        {
            bool bad = false;
            size_t rel = scan_to(s + start, n - start, ';', &bad);
            if (bad) {
                return MOQR_HTTP_400;
            }
            i = start + rel;
        }
        const char *p = s + start;
        size_t pn = i - start;
        trim(&p, &pn);
        if (pn == 0) {
            return MOQR_HTTP_400;   /* an empty parameter is malformed */
        }
        const char *eq;
        {
            bool bad = false;
            size_t at = scan_to(p, pn, '=', &bad);
            if (bad || at == pn) {
                return MOQR_HTTP_400;
            }
            eq = p + at;
        }
        const char *pname = p;
        size_t pname_n = (size_t)(eq - p);
        const char *pval = eq + 1;
        size_t pval_n = pn - pname_n - 1;
        trim(&pname, &pname_n);
        trim(&pval, &pval_n);
        if (pname_n == 0 || pval_n == 0) {
            return MOQR_HTTP_400;
        }
        /* The parameter name is a token and the value is a token or a quoted
         * string. Neither was validated before, so `a b=1` and `q="` were
         * served as though well formed. */
        {
            size_t pi;
            for (pi = 0; pi < pname_n; pi++) {
                if (!is_tchar(pname[pi])) {
                    return MOQR_HTTP_400;
                }
            }
            if (pval[0] == '"') {
                bool bad = false;
                size_t qend;
                /* The scanner validates the quoting, including quoted-pairs. */
                if (scan_to(pval, pval_n, '\0', &bad) != pval_n || bad ||
                    pval_n < 2u || pval[pval_n - 1u] != '"') {
                    return MOQR_HTTP_400;
                }
                /* EXACTLY ONE quoted-string. `"0.0"".4"` is two of them stuck
                 * together, not one value, and concatenating them silently
                 * would invent a value the client never sent. */
                qend = 1u;
                while (qend < pval_n) {
                    if (pval[qend] == '\\') {
                        qend += 2u;
                        continue;
                    }
                    if (pval[qend] == '"') {
                        break;
                    }
                    qend++;
                }
                if (qend != pval_n - 1u) {
                    return MOQR_HTTP_400;
                }
            } else {
                for (pi = 0; pi < pval_n; pi++) {
                    if (!is_tchar(pval[pi])) {
                        return MOQR_HTTP_400;
                    }
                }
            }
        }
        if (token_is(pname, pname_n, "q")) {
            if (seen_q) {
                return MOQR_HTTP_400;   /* a duplicate q is malformed */
            }
            seen_q = true;
            /* weight = ";" ... "q=" qvalue (RFC 9110 12.4.2): the qvalue
             * grammar is unquoted, so `q="0.5"` is not a weight. */
            if (pval_n > 0u && pval[0] == '"') {
                return MOQR_HTTP_400;
            }
            if (!parse_q(pval, pval_n, &q)) {
                return MOQR_HTTP_400;
            }
        } else if (token_is(pname, pname_n, "version")) {
            if (seen_version) {
                return MOQR_HTTP_400;   /* last-one-wins is not a decision */
            }
            seen_version = true;
            want_version = pval;
            want_version_n = pval_n;
        } else if (token_is(pname, pname_n, "charset")) {
            if (seen_charset) {
                return MOQR_HTTP_400;
            }
            seen_charset = true;
            if (value_is(pval, pval_n, SERVED_CHARSET)) {
                charset_matched = true;
            } else {
                unavailable = true;
            }
        } else {
            /* A media parameter we do not recognise NARROWS the requested
             * representation: the client asked for something more specific than
             * what we produce. Ignoring it would answer a narrowed request with
             * an unnarrowed document -- a false match.
             *
             * RFC 9110 removed accept-ext, so position does not exempt a
             * parameter: an unrecognised one narrows wherever it appears,
             * whether it comes before or after q.
             *
             * This makes the range unavailable, not the request malformed, so
             * another range can still serve it. */
            unavailable = true;
        }
    }

    /* Which formats does this range name, and how specifically? */
    bool type_wild = (type_n == 1 && type[0] == '*');
    bool sub_wild = (sub_n == 1 && sub[0] == '*');

    if (type_wild && !sub_wild) {
        /* A wild type with a named subtype is not a legal media range. It is
         * malformed, not merely unserved: accepting it would mean guessing
         * what a client that cannot express itself actually wanted. */
        return MOQR_HTTP_400;
    }
    for (size_t f = 0; f < nreps; f++) {
        if (type_wild && sub_wild) {
            spec[f] = SPEC_WILDCARD;
        } else if (sub_wild) {
            spec[f] = token_is(type, type_n, reps[f].type) ? SPEC_TYPE
                                                            : SPEC_NONE;
        } else {
            spec[f] = (token_is(type, type_n, reps[f].type) &&
                       token_is(sub, sub_n, reps[f].sub)) ? SPEC_EXACT
                                                          : SPEC_NONE;
        }
    }
    /* A named version must be the one we produce for that representation. A
     * mismatch -- or a version where none is served -- makes that range
     * unavailable rather than a syntax error, so a client listing several
     * versions still gets served by another range. */
    if (want_version != NULL) {
        for (size_t f = 0; f < nreps; f++) {
            if (spec[f] != SPEC_NONE &&
                (reps[f].version == NULL ||
                 !value_is(want_version, want_version_n, reps[f].version))) {
                spec[f] = SPEC_NONE;
            }
        }
    }
    /* A media parameter that MATCHES makes a range more specific than the
     * same form without it, one step per parameter and below the next form:
     * `text/plain;version=0.0.4;q=0` excludes that representation beside
     * `text/plain;q=1`, and the full wildcard with a matching charset at q=0
     * excludes beside the bare full wildcard at q=1 -- while `text/plain;q=0`
     * still governs beside a full wildcard carrying two matching parameters.
     * A version that survived above matched; one that did not already made
     * the range unavailable for that representation. */
    {
        int matched = (want_version != NULL ? 1 : 0) + (charset_matched ? 1 : 0);
        for (size_t f = 0; f < nreps; f++) {
            if (spec[f] != SPEC_NONE) {
                spec[f] = spec[f] * SPEC_PARAM_STEPS + matched;
            }
        }
    }
    if (unavailable) {
        for (size_t f = 0; f < nreps; f++) {
            spec[f] = SPEC_NONE;
        }
    }
    *out_q = q;
    return MOQR_HTTP_200;
}

/* The one negotiation, over one target's served-representation table. On 200
 * `*out_idx` is the index into `reps` of the selected representation. */
static moqr_http_status_t
negotiate_over(const char *accept, size_t len, const rep_desc_t *reps,
               size_t nreps, size_t *out_idx)
{
    int best_q[MOQR_HTTP_MAX_REPS];
    int best_spec[MOQR_HTTP_MAX_REPS];
    size_t i;
    size_t start;
    unsigned ranges = 0;

    if (nreps == 0u || nreps > MOQR_HTTP_MAX_REPS) {
        return MOQR_HTTP_500;
    }
    for (i = 0; i < nreps; i++) {
        best_q[i] = 0;
        best_spec[i] = SPEC_NONE;
    }

    /* An absent or empty Accept is not an empty preference set: it means the
     * client stated none, and the decided default is the table's first
     * representation (OpenMetrics for /metrics). */
    if (accept == NULL || len == 0) {
        if (out_idx != NULL) {
            *out_idx = 0;
        }
        return MOQR_HTTP_200;
    }

    /* Count ranges before parsing any, so exceeding the bound is reported as a
     * bound and never masked by a syntax error further along the field. */
    {
        unsigned n = 1;
        size_t at = 0;
        while (at < len) {
            bool bad = false;
            size_t rel = scan_to(accept + at, len - at, ',', &bad);
            if (bad) {
                return MOQR_HTTP_400;
            }
            at += rel;
            if (at >= len) {
                break;
            }
            n++;
            at++;
        }
        if (n > MOQR_ADMIN_MAX_RANGES) {
            return MOQR_HTTP_431;
        }
    }

    start = 0;
    while (start <= len) {
        bool bad = false;
        size_t rel = scan_to(accept + start, len - start, ',', &bad);
        if (bad) {
            return MOQR_HTTP_400;
        }
        i = start + rel;
        int q = Q_MAX;
        int spec[MOQR_HTTP_MAX_REPS];
        moqr_http_status_t st;

        for (size_t f = 0; f < MOQR_HTTP_MAX_REPS; f++) {
            spec[f] = SPEC_NONE;
        }
        st = parse_range(accept + start, i - start, &q, spec, reps, nreps);
        if (st != MOQR_HTTP_200) {
            return st;
        }
        ranges++;
        for (size_t f = 0; f < nreps; f++) {
            if (spec[f] == SPEC_NONE) {
                continue;
            }
            /* A more specific range replaces a less specific one outright,
             * even to a lower q -- that is how a client excludes one format
             * while leaving a wildcard in place. */
            if (spec[f] > best_spec[f] ||
                (spec[f] == best_spec[f] && q > best_q[f])) {
                best_spec[f] = spec[f];
                best_q[f] = q;
            }
        }
        if (i >= len) {
            break;
        }
        start = i + 1;
    }
    (void)ranges;

    /* Highest q wins; the table's first representation takes ties (OpenMetrics
     * for /metrics), including the both-unspecified case. A q of zero is a
     * refusal, not a low preference. */
    {
        int best = 0;
        size_t pick = 0;
        for (size_t f = 0; f < nreps; f++) {
            int qf = (best_spec[f] == SPEC_NONE) ? 0 : best_q[f];
            if (qf > best) {
                best = qf;
                pick = f;
            }
        }
        if (best == 0) {
            return MOQR_HTTP_406;
        }
        if (out_idx != NULL) {
            *out_idx = pick;
        }
    }
    return MOQR_HTTP_200;
}

moqr_http_status_t
moqr_http_negotiate(const char *accept, size_t len, moqr_obs_format_t *out)
{
    size_t idx = 0;
    moqr_http_status_t st =
        negotiate_over(accept, len, k_metrics_reps, MOQR_OBS_FMT__COUNT, &idx);
    if (st == MOQR_HTTP_200 && out != NULL) {
        *out = k_metrics_fmt[idx];
    }
    return st;
}

/* -- the request head ----------------------------------------------------- */

static bool
header_name_is(const char *line, size_t n, const char *name)
{
    size_t l = strlen(name);
    if (n < l + 1 || line[l] != ':') {
        return false;
    }
    return token_is(line, l, name);
}

moqr_http_status_t
moqr_http_parse(const char *buf, size_t len, moqr_http_request_t *out)
{
    const char *p;
    const char *end;
    size_t line_n;
    unsigned headers = 0;
    char accept[MOQR_ADMIN_MAX_LINE * 2];
    size_t accept_n = 0;
    bool have_accept = false;
    bool is_11 = false;
    unsigned hosts = 0;
    bool bad_host = false;
    bool bad_syntax = false;
    bool bad_method = false;
    bool bad_target = false;
    moqr_http_target_t target = MOQR_HTTP_TARGET_UNKNOWN;

    if (buf == NULL) {
        return MOQR_HTTP_400;
    }


    /* Bounds first, and before anything is interpreted: an oversized head is
     * refused as a bound even when it is also malformed, so the reported
     * status does not depend on which check happened to run first. */
    if (len > MOQR_ADMIN_MAX_REQUEST) {
        return MOQR_HTTP_431;
    }
    {
        const char *q = buf;
        const char *stop = buf + len;
        while (q < stop) {
            const char *nl = memchr(q, '\n', (size_t)(stop - q));
            size_t seg = (nl == NULL) ? (size_t)(stop - q)
                                      : (size_t)(nl - q);
            if (seg > MOQR_ADMIN_MAX_LINE) {
                return MOQR_HTTP_431;
            }
            if (nl == NULL) {
                break;
            }
            q = nl + 1;
        }
    }
    {
        unsigned n = 0;
        const char *q = buf;
        const char *stop = buf + len;
        while ((q = memchr(q, '\n', (size_t)(stop - q))) != NULL) {
            n++;
            q++;
            if (q >= stop) {
                break;
            }
        }
        /* request line + headers + the blank terminator */
        if (n > (unsigned)(MOQR_ADMIN_MAX_HEADERS + 2u)) {
            return MOQR_HTTP_431;
        }
    }

    /* The span must be exactly ONE terminated head: the final CRLFCRLF at its
     * end, and nowhere earlier. Absent means an incomplete request we must not
     * answer; earlier means a pipelined second request or an unframed body.
     * Both are refused rather than answered from a prefix.
     *
     * Placed AFTER the bounds above so the documented precedence is true: an
     * over-bound span with an early terminator is 431, not 400. */
    {
        size_t k;
        bool found = false;
        for (k = 0; k + 3 < len; k++) {
            if (buf[k] == '\r' && buf[k + 1] == '\n' &&
                buf[k + 2] == '\r' && buf[k + 3] == '\n') {
                found = true;
                break;
            }
        }
        if (!found || k + 4u != len) {
            return MOQR_HTTP_400;
        }
    }

    end = buf + len;
    p = buf;

    /* -- request line -- */
    {
        const char *nl = memchr(p, '\n', (size_t)(end - p));
        const char *sp1;
        const char *sp2;
        const char *tgt;
        size_t tgt_n;

        if (nl == NULL || nl == p || nl[-1] != '\r') {
            return MOQR_HTTP_400;
        }
        line_n = (size_t)(nl - p) - 1u;

        sp1 = memchr(p, ' ', line_n);
        if (sp1 == NULL) {
            bad_syntax = true;
        } else {
            size_t rest = line_n - (size_t)(sp1 - p) - 1u;
            sp2 = memchr(sp1 + 1, ' ', rest);
            if (sp2 == NULL) {
                bad_syntax = true;
            } else {
                const char *ver = sp2 + 1;
                size_t ver_n = line_n - (size_t)(ver - p);
                is_11 = token_eq(ver, ver_n, "HTTP/1.1");
                if (!is_11 && !token_eq(ver, ver_n, "HTTP/1.0")) {
                    bad_syntax = true;
                }
                /* A third space means a target we cannot interpret. */
                if (memchr(ver, ' ', ver_n) != NULL) {
                    bad_syntax = true;
                }
                {
                    size_t mn = (size_t)(sp1 - p);
                    size_t mi;
                    if (mn == 0) {
                        bad_syntax = true;
                    }
                    for (mi = 0; mi < mn; mi++) {
                        if (!is_tchar(p[mi])) {
                            bad_syntax = true;
                            break;
                        }
                    }
                    if (!token_eq(p, mn, "GET")) {
                        bad_method = true;
                    }
                }
                tgt = sp1 + 1;
                tgt_n = (size_t)(sp2 - tgt);
                if (tgt_n == 0 || tgt[0] != '/') {
                    bad_syntax = true;
                } else {
                    size_t ti;
                    for (ti = 0; ti < tgt_n; ti++) {
                        unsigned char ch = (unsigned char)tgt[ti];
                        if (ch < 0x21u || ch == 0x7fu) {
                            bad_syntax = true;
                            break;
                        }
                    }
                }
                /* The closed target table. Exact bytes only: no trailing
                 * slash, no suffix, no query -- a request for something near
                 * a target is a request for something we do not serve. */
                if (!bad_syntax) {
                    if (token_eq(tgt, tgt_n, "/metrics")) {
                        target = MOQR_HTTP_TARGET_METRICS;
                    } else if (token_eq(tgt, tgt_n, "/api/v1/info")) {
                        target = MOQR_HTTP_TARGET_INFO;
                    } else if (token_eq(tgt, tgt_n, "/api/v1/shards")) {
                        target = MOQR_HTTP_TARGET_SHARDS;
                    } else {
                        bad_target = true;
                    }
                }
            }
        }
        p = nl + 1;
    }

    /* -- headers -- */
    while (p < end) {
        const char *nl = memchr(p, '\n', (size_t)(end - p));
        if (nl == NULL || nl == p || nl[-1] != '\r') {
            bad_syntax = true;
            break;
        }
        line_n = (size_t)(nl - p) - 1u;
        if (line_n == 0) {
            p = nl + 1;
            break;              /* the blank line ends the head */
        }
        headers++;
        if (headers > MOQR_ADMIN_MAX_HEADERS) {
            return MOQR_HTTP_431;
        }
        /* An obs-fold continuation line. RFC 7230 requires rejecting these
         * outside a message body rather than unfolding them. */
        if (p[0] == ' ' || p[0] == '\t') {
            bad_syntax = true;
            break;
        }
        {
            const char *colon = memchr(p, ':', line_n);
            size_t name_n;
            size_t vi;
            if (colon == NULL) {
                bad_syntax = true;
                break;
            }
            name_n = (size_t)(colon - p);
            if (name_n == 0) {
                bad_syntax = true;
                break;
            }
            /* The field name is a token: no space, no separators. Whitespace
             * before the colon is the classic request-smuggling shape. */
            for (vi = 0; vi < name_n; vi++) {
                if (!is_tchar(p[vi])) {
                    bad_syntax = true;
                    break;
                }
            }
            if (bad_syntax) {
                break;
            }
            /* Field values carry no control bytes other than HTAB. */
            for (vi = name_n + 1u; vi < line_n; vi++) {
                unsigned char ch = (unsigned char)p[vi];
                if (ch != '\t' && (ch < 0x20u || ch == 0x7fu)) {
                    bad_syntax = true;
                    break;
                }
            }
            if (bad_syntax) {
                break;
            }
        }
        /* Protocol changes and body framing are refused outright rather than
         * ignored: answering a request whose framing we declined to honour is
         * how a proxy and an origin end up disagreeing about message
         * boundaries. */
        if (header_name_is(p, line_n, "upgrade") ||
            header_name_is(p, line_n, "expect") ||
            header_name_is(p, line_n, "trailer") ||
            header_name_is(p, line_n, "te")) {
            bad_syntax = true;
            break;
        }
        /* v1 reads no body. An entity header is refused rather than ignored:
         * ignoring it would leave bytes on the connection that the next parse
         * would read as a fresh request. */
        if (header_name_is(p, line_n, "content-length") ||
            header_name_is(p, line_n, "transfer-encoding")) {
            bad_syntax = true;
            break;
        }
        if (header_name_is(p, line_n, "host")) {
            const char *v = p + strlen("host") + 1;
            size_t vn = line_n - strlen("host") - 1;
            size_t hi;
            trim(&v, &vn);
            (void)hi;
            hosts++;
            if (!host_is_valid(v, vn)) {
                bad_host = true;
            }
        }
        if (header_name_is(p, line_n, "accept")) {
            const char *v = p + strlen("accept") + 1;
            size_t vn = line_n - strlen("accept") - 1;
            trim(&v, &vn);
            /* Duplicate Accept headers combine in order as one field. */
            if (have_accept && accept_n + 1 < sizeof(accept)) {
                accept[accept_n++] = ',';
            }
            if (accept_n + vn >= sizeof(accept)) {
                return MOQR_HTTP_431;
            }
            memcpy(accept + accept_n, v, vn);
            accept_n += vn;
            have_accept = true;
        }
        p = nl + 1;
    }

    /* RFC 9112 3.2: an HTTP/1.1 request carries exactly one valid Host field.
     * Missing, repeated or empty is 400 -- a server that answers without one
     * cannot know which authority it was asked about. HTTP/1.0 predates the
     * requirement and is deliberately left alone. */
    if (is_11 && (hosts != 1u || bad_host)) {
        bad_syntax = true;
    }

    /* Fixed precedence: bounds, syntax, method, target, negotiation. */
    if (bad_syntax) {
        return MOQR_HTTP_400;
    }
    if (bad_method) {
        return MOQR_HTTP_405;
    }
    if (bad_target) {
        return MOQR_HTTP_404;
    }
    /* The decided rule, one closed statement of it: a MISSING Accept field
     * selects OpenMetrics; a PRESENT field whose combined value is empty or
     * whitespace-only is malformed. "Absent" and "present but says nothing"
     * are different assertions by the client, and treating them alike would
     * answer a request that expressed no acceptable representation at all.
     * Duplicate fields combine first, so an empty one alongside a real one
     * yields an empty range and is likewise malformed. */
    if (have_accept) {
        const char *av = accept;
        size_t an = accept_n;
        trim(&av, &an);
        if (an == 0) {
            return MOQR_HTTP_400;
        }
    }
    /* Negotiation runs over the TARGET's served representations, and every
     * field of the result is set from that outcome: the target, the
     * representation, and a format that is either a real metrics format or
     * the canonical non-index for JSON. Nothing is left for a reader to
     * assume. */
    {
        size_t idx = 0;
        moqr_http_status_t st;
        moqr_http_request_t r;

        r.target = target;
        if (target == MOQR_HTTP_TARGET_METRICS) {
            st = negotiate_over(have_accept ? accept : NULL, accept_n,
                                k_metrics_reps, MOQR_OBS_FMT__COUNT, &idx);
            r.rep = MOQR_HTTP_REP_METRICS;
            r.fmt = k_metrics_fmt[idx < MOQR_OBS_FMT__COUNT ? idx : 0];
        } else if (target == MOQR_HTTP_TARGET_INFO ||
                   target == MOQR_HTTP_TARGET_SHARDS) {
            st = negotiate_over(have_accept ? accept : NULL, accept_n,
                                k_json_reps, 1u, &idx);
            r.rep = MOQR_HTTP_REP_JSON;
            r.fmt = MOQR_OBS_FMT__COUNT;
        } else {
            return MOQR_HTTP_404;   /* unreachable: bad_target answered above */
        }
        if (st != MOQR_HTTP_200) {
            return st;
        }
        if (out != NULL) {
            *out = r;
        }
    }
    return MOQR_HTTP_200;
}

/* -- response heads ------------------------------------------------------- */

static const char *
reason(moqr_http_status_t s)
{
    switch (s) {
    case MOQR_HTTP_200: return "OK";
    case MOQR_HTTP_400: return "Bad Request";
    case MOQR_HTTP_404: return "Not Found";
    case MOQR_HTTP_405: return "Method Not Allowed";
    case MOQR_HTTP_406: return "Not Acceptable";
    case MOQR_HTTP_431: return "Request Header Fields Too Large";
    case MOQR_HTTP_500: return "Internal Server Error";
    case MOQR_HTTP_503: return "Service Unavailable";
    default:            return NULL;
    }
}

size_t
moqr_http_write_head(moqr_http_status_t status, moqr_http_rep_t rep,
                     moqr_obs_format_t fmt, size_t content_len, char *buf,
                     size_t cap)
{
    const char *why = reason(status);
    const char *body;
    char retry[40];
    int n;

    /* Derived from the public constant, not written twice. A header that
     * disagrees with the value the API advertises is a contract the caller
     * cannot rely on. */
    (void)snprintf(retry, sizeof(retry), "Retry-After: %u\r\n",
                   (unsigned)MOQR_ADMIN_RETRY_AFTER_S);

    if (buf == NULL || why == NULL) {
        return 0;
    }
    if (status == MOQR_HTTP_200) {
        const char *ct;
        /* The (representation, format) pair is validated BEFORE anything is
         * written. Defaulting an unknown enum to OpenMetrics would advertise
         * a representation nobody selected, and the caller would then index a
         * body array with the same bad value; a JSON representation with a
         * real format would label a document as a metrics body. */
        if (rep == MOQR_HTTP_REP_METRICS && fmt < MOQR_OBS_FMT__COUNT) {
            ct = (fmt == MOQR_OBS_FMT_PROMETHEUS_004)
                     ? MOQR_ADMIN_CT_PROMETHEUS
                     : MOQR_ADMIN_CT_OPENMETRICS;
        } else if (rep == MOQR_HTTP_REP_JSON && fmt == MOQR_OBS_FMT__COUNT) {
            ct = MOQR_ADMIN_CT_JSON;
        } else {
            return 0;
        }
        n = snprintf(buf, cap,
                     "HTTP/1.1 200 OK\r\n"
                     "Content-Type: %s\r\n"
                     "Content-Length: %zu\r\n"
                     "Connection: close\r\n"
                     "\r\n",
                     ct, content_len);
        if (n < 0 || (size_t)n >= cap) {
            return 0;   /* refuse rather than put a truncated head on the wire */
        }
        return (size_t)n;
    }

    /* Error bodies are generated from the status alone. No metrics bytes ever
     * reach an error response, whole or partial. */
    body = why;
    n = snprintf(buf, cap,
                 "HTTP/1.1 %u %s\r\n"
                 "Content-Type: text/plain; charset=utf-8\r\n"
                 "Content-Length: %zu\r\n"
                 "%s%s"
                 "Connection: close\r\n"
                 "\r\n"
                 "%s",
                 (unsigned)status, why, strlen(body),
                 (status == MOQR_HTTP_405) ? "Allow: GET\r\n" : "",
                 (status == MOQR_HTTP_503) ? retry : "",
                 body);
    if (n < 0 || (size_t)n >= cap) {
        return 0;
    }
    return (size_t)n;
}
