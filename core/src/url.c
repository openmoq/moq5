#include <moq/url.h>
#include <string.h>

void moq_url_init(moq_url_t *url)
{
    if (!url) return;
    memset(url, 0, sizeof(*url));
    url->struct_size = sizeof(*url);
}

static const uint8_t *find_byte(const uint8_t *p, size_t len, uint8_t ch)
{
    for (size_t i = 0; i < len; i++) {
        if (p[i] == ch) return &p[i];
    }
    return NULL;
}

static uint16_t default_port(moq_bytes_t scheme)
{
    if (scheme.len == 4 && memcmp(scheme.data, "moqt", 4) == 0)
        return 4433;
    if (scheme.len == 3 && memcmp(scheme.data, "moq", 3) == 0)
        return 4433;
    if (scheme.len == 5 && memcmp(scheme.data, "https", 5) == 0)
        return 443;
    return 0;
}

moq_result_t moq_url_parse(moq_bytes_t input, moq_url_t *url)
{
    if (!url || !input.data)
        return MOQ_ERR_INVAL;

    moq_url_init(url);

    const uint8_t *p = input.data;
    const uint8_t *end = input.data + input.len;

    /* Find "://" to extract scheme. */
    const uint8_t *sep = find_byte(p, (size_t)(end - p), ':');
    if (!sep || sep == p)
        return MOQ_ERR_INVAL;
    if ((size_t)(end - sep) < 3 || sep[1] != '/' || sep[2] != '/')
        return MOQ_ERR_INVAL;

    url->scheme.data = p;
    url->scheme.len  = (size_t)(sep - p);
    p = sep + 3; /* skip "://" */

    /* Validate scheme and get default port. */
    uint16_t def_port = default_port(url->scheme);
    if (def_port == 0)
        return MOQ_ERR_INVAL;

    /* Parse host. */
    if (p >= end)
        return MOQ_ERR_INVAL;

    if (*p == '[') {
        /* Bracketed IPv6. */
        p++;
        const uint8_t *close = find_byte(p, (size_t)(end - p), ']');
        if (!close)
            return MOQ_ERR_INVAL;
        if (close == p)
            return MOQ_ERR_INVAL; /* empty host */
        url->host.data = p;
        url->host.len  = (size_t)(close - p);
        p = close + 1;
        /* After ']': expect ':', '/', '?', or end. */
        if (p < end && *p == ':') {
            p++;
        } else if (p >= end || *p == '/' || *p == '?') {
            /* No port — use default. */
            url->port = def_port;
            goto parse_path;
        } else {
            return MOQ_ERR_INVAL;
        }
    } else {
        /* Find the host/port boundary.
         * Look for ':' to split host:port. If no ':', the host
         * extends to '/', '?', or end and we use the default port. */
        const uint8_t *colon = NULL;
        const uint8_t *host_end = NULL;
        for (const uint8_t *s = p; s < end; s++) {
            if (*s == ':') { colon = s; break; }
            if (*s == '/' || *s == '?') { host_end = s; break; }
        }
        if (!colon) {
            /* No port specified. */
            host_end = host_end ? host_end : end;
            if (host_end == p)
                return MOQ_ERR_INVAL; /* empty host */
            url->host.data = p;
            url->host.len  = (size_t)(host_end - p);
            url->port = def_port;
            p = host_end;
            goto parse_path;
        }
        if (colon == p)
            return MOQ_ERR_INVAL; /* empty host */
        url->host.data = p;
        url->host.len  = (size_t)(colon - p);
        p = colon + 1;
    }

    /* Parse explicit port: digits terminated by '/', '?', or end. */
    if (p >= end || *p < '0' || *p > '9')
        return MOQ_ERR_INVAL; /* empty or non-numeric port */

    uint32_t port = 0;
    while (p < end && *p >= '0' && *p <= '9') {
        port = port * 10 + (*p - '0');
        if (port > 65535)
            return MOQ_ERR_INVAL;
        p++;
    }
    if (port == 0)
        return MOQ_ERR_INVAL;
    if (p < end && *p != '/' && *p != '?')
        return MOQ_ERR_INVAL; /* trailing junk after port */
    url->port = (uint16_t)port;

parse_path:
    /* Parse path and query. */
    if (p < end && *p == '/') {
        const uint8_t *path_start = p;
        const uint8_t *q = find_byte(p, (size_t)(end - p), '?');
        if (q) {
            url->path.data = path_start;
            url->path.len  = (size_t)(q - path_start);
            url->query.data = q + 1;
            url->query.len  = (size_t)(end - (q + 1));
        } else {
            url->path.data = path_start;
            url->path.len  = (size_t)(end - path_start);
        }
    } else if (p < end && *p == '?') {
        url->query.data = p + 1;
        url->query.len  = (size_t)(end - (p + 1));
    }

    return MOQ_OK;
}
