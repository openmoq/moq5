#include <moq/url.h>
#include "test_support.h"
#include <string.h>

static moq_bytes_t lit(const char *s)
{
    return (moq_bytes_t){ (const uint8_t *)s, strlen(s) };
}

static bool bv_eq(moq_bytes_t a, const char *s)
{
    size_t len = strlen(s);
    return a.len == len && memcmp(a.data, s, len) == 0;
}

int main(void)
{
    int failures = 0;

    /* -- moq_url_init ------------------------------------------------ */
    {
        moq_url_t url;
        moq_url_init(&url);
        MOQ_TEST_CHECK(url.struct_size == sizeof(moq_url_t));
        MOQ_TEST_CHECK(url.scheme.data == NULL);
        MOQ_TEST_CHECK(url.scheme.len == 0);
        MOQ_TEST_CHECK(url.host.data == NULL);
        MOQ_TEST_CHECK(url.port == 0);
        MOQ_TEST_CHECK(url.path.data == NULL);
        MOQ_TEST_CHECK(url.query.data == NULL);
        moq_url_init(NULL); /* no crash */
    }

    /* -- moqt://host:port (explicit) --------------------------------- */
    {
        moq_url_t url;
        moq_result_t rc = moq_url_parse(lit("moqt://relay.example.com:4433"), &url);
        MOQ_TEST_CHECK(rc == MOQ_OK);
        MOQ_TEST_CHECK(bv_eq(url.scheme, "moqt"));
        MOQ_TEST_CHECK(bv_eq(url.host, "relay.example.com"));
        MOQ_TEST_CHECK(url.port == 4433);
        MOQ_TEST_CHECK(url.path.len == 0);
        MOQ_TEST_CHECK(url.query.len == 0);
    }

    /* -- moqt://host (default port 4433) ----------------------------- */
    {
        moq_url_t url;
        moq_result_t rc = moq_url_parse(lit("moqt://relay.example.com"), &url);
        MOQ_TEST_CHECK(rc == MOQ_OK);
        MOQ_TEST_CHECK(bv_eq(url.scheme, "moqt"));
        MOQ_TEST_CHECK(bv_eq(url.host, "relay.example.com"));
        MOQ_TEST_CHECK(url.port == 4433);
    }

    /* -- moq://host (default port 4433) ------------------------------ */
    {
        moq_url_t url;
        moq_result_t rc = moq_url_parse(lit("moq://relay.example.com"), &url);
        MOQ_TEST_CHECK(rc == MOQ_OK);
        MOQ_TEST_CHECK(bv_eq(url.scheme, "moq"));
        MOQ_TEST_CHECK(bv_eq(url.host, "relay.example.com"));
        MOQ_TEST_CHECK(url.port == 4433);
    }

    /* -- moq://host:port (explicit overrides default) ---------------- */
    {
        moq_url_t url;
        moq_result_t rc = moq_url_parse(lit("moq://relay.example.com:5555"), &url);
        MOQ_TEST_CHECK(rc == MOQ_OK);
        MOQ_TEST_CHECK(url.port == 5555);
    }

    /* -- https://host/path (default port 443) ------------------------ */
    {
        moq_url_t url;
        moq_result_t rc = moq_url_parse(lit("https://cdn.example.com/moq/v1"), &url);
        MOQ_TEST_CHECK(rc == MOQ_OK);
        MOQ_TEST_CHECK(bv_eq(url.scheme, "https"));
        MOQ_TEST_CHECK(bv_eq(url.host, "cdn.example.com"));
        MOQ_TEST_CHECK(url.port == 443);
        MOQ_TEST_CHECK(bv_eq(url.path, "/moq/v1"));
        MOQ_TEST_CHECK(url.query.len == 0);
    }

    /* -- https://host:port/path (explicit overrides default) --------- */
    {
        moq_url_t url;
        moq_result_t rc = moq_url_parse(lit("https://cdn.example.com:8443/moq/v1"), &url);
        MOQ_TEST_CHECK(rc == MOQ_OK);
        MOQ_TEST_CHECK(url.port == 8443);
        MOQ_TEST_CHECK(bv_eq(url.path, "/moq/v1"));
    }

    /* -- https://host (default 443, no path) ------------------------- */
    {
        moq_url_t url;
        moq_result_t rc = moq_url_parse(lit("https://cdn.example.com"), &url);
        MOQ_TEST_CHECK(rc == MOQ_OK);
        MOQ_TEST_CHECK(url.port == 443);
        MOQ_TEST_CHECK(url.path.len == 0);
    }

    /* -- moqt://host?query (default port with query) ----------------- */
    {
        moq_url_t url;
        moq_result_t rc = moq_url_parse(lit("moqt://host?ns=foo&track=bar"), &url);
        MOQ_TEST_CHECK(rc == MOQ_OK);
        MOQ_TEST_CHECK(bv_eq(url.host, "host"));
        MOQ_TEST_CHECK(url.port == 4433);
        MOQ_TEST_CHECK(url.path.len == 0);
        MOQ_TEST_CHECK(bv_eq(url.query, "ns=foo&track=bar"));
    }

    /* -- bracketed IPv6 ---------------------------------------------- */
    {
        moq_url_t url;
        moq_result_t rc = moq_url_parse(lit("moqt://[::1]:4433"), &url);
        MOQ_TEST_CHECK(rc == MOQ_OK);
        MOQ_TEST_CHECK(bv_eq(url.scheme, "moqt"));
        MOQ_TEST_CHECK(bv_eq(url.host, "::1"));
        MOQ_TEST_CHECK(url.port == 4433);
    }

    /* -- bracketed IPv6 with default port ---------------------------- */
    {
        moq_url_t url;
        moq_result_t rc = moq_url_parse(lit("moqt://[::1]"), &url);
        MOQ_TEST_CHECK(rc == MOQ_OK);
        MOQ_TEST_CHECK(bv_eq(url.host, "::1"));
        MOQ_TEST_CHECK(url.port == 4433);
    }

    /* -- bracketed IPv6 with default port and path ------------------- */
    {
        moq_url_t url;
        moq_result_t rc = moq_url_parse(lit("https://[::1]/path"), &url);
        MOQ_TEST_CHECK(rc == MOQ_OK);
        MOQ_TEST_CHECK(bv_eq(url.host, "::1"));
        MOQ_TEST_CHECK(url.port == 443);
        MOQ_TEST_CHECK(bv_eq(url.path, "/path"));
    }

    /* -- full IPv6 address ------------------------------------------- */
    {
        moq_url_t url;
        moq_result_t rc = moq_url_parse(
            lit("moqt://[2001:db8::1]:5004"), &url);
        MOQ_TEST_CHECK(rc == MOQ_OK);
        MOQ_TEST_CHECK(bv_eq(url.host, "2001:db8::1"));
        MOQ_TEST_CHECK(url.port == 5004);
    }

    /* -- query string with explicit port ----------------------------- */
    {
        moq_url_t url;
        moq_result_t rc = moq_url_parse(
            lit("moqt://host:4433?ns=foo&track=bar"), &url);
        MOQ_TEST_CHECK(rc == MOQ_OK);
        MOQ_TEST_CHECK(bv_eq(url.host, "host"));
        MOQ_TEST_CHECK(url.port == 4433);
        MOQ_TEST_CHECK(url.path.len == 0);
        MOQ_TEST_CHECK(bv_eq(url.query, "ns=foo&track=bar"));
    }

    /* -- path and query ---------------------------------------------- */
    {
        moq_url_t url;
        moq_result_t rc = moq_url_parse(
            lit("https://host:4433/path/to?x=1&y=2"), &url);
        MOQ_TEST_CHECK(rc == MOQ_OK);
        MOQ_TEST_CHECK(bv_eq(url.path, "/path/to"));
        MOQ_TEST_CHECK(bv_eq(url.query, "x=1&y=2"));
    }

    /* -- path only, no query ----------------------------------------- */
    {
        moq_url_t url;
        moq_result_t rc = moq_url_parse(
            lit("https://host:443/just-path"), &url);
        MOQ_TEST_CHECK(rc == MOQ_OK);
        MOQ_TEST_CHECK(bv_eq(url.path, "/just-path"));
        MOQ_TEST_CHECK(url.query.len == 0);
    }

    /* -- port 1 (minimum valid) -------------------------------------- */
    {
        moq_url_t url;
        MOQ_TEST_CHECK(moq_url_parse(lit("moqt://h:1"), &url) == MOQ_OK);
        MOQ_TEST_CHECK(url.port == 1);
    }

    /* -- port 65535 (maximum valid) ---------------------------------- */
    {
        moq_url_t url;
        MOQ_TEST_CHECK(moq_url_parse(lit("moqt://h:65535"), &url) == MOQ_OK);
        MOQ_TEST_CHECK(url.port == 65535);
    }

    /* -- borrowed slices point into input ----------------------------- */
    {
        const char *raw = "moqt://myhost:9999/p?q=1";
        moq_bytes_t input = lit(raw);
        moq_url_t url;
        MOQ_TEST_CHECK(moq_url_parse(input, &url) == MOQ_OK);
        MOQ_TEST_CHECK(url.scheme.data >= input.data);
        MOQ_TEST_CHECK(url.scheme.data < input.data + input.len);
        MOQ_TEST_CHECK(url.host.data >= input.data);
        MOQ_TEST_CHECK(url.host.data < input.data + input.len);
        MOQ_TEST_CHECK(url.path.data >= input.data);
        MOQ_TEST_CHECK(url.query.data >= input.data);
    }

    /* ================================================================ */
    /* Reject cases                                                     */
    /* ================================================================ */

    /* -- NULL url pointer --------------------------------------------- */
    {
        MOQ_TEST_CHECK(moq_url_parse(lit("moqt://h:1"), NULL) == MOQ_ERR_INVAL);
    }

    /* -- NULL input data --------------------------------------------- */
    {
        moq_url_t url;
        moq_bytes_t bad = { NULL, 0 };
        MOQ_TEST_CHECK(moq_url_parse(bad, &url) == MOQ_ERR_INVAL);
    }

    /* -- missing scheme ---------------------------------------------- */
    {
        moq_url_t url;
        MOQ_TEST_CHECK(moq_url_parse(lit("host:4433"), &url) == MOQ_ERR_INVAL);
    }

    /* -- unsupported scheme ------------------------------------------ */
    {
        moq_url_t url;
        MOQ_TEST_CHECK(moq_url_parse(lit("http://host:80"), &url) == MOQ_ERR_INVAL);
    }

    /* -- wss scheme -------------------------------------------------- */
    {
        moq_url_t url;
        MOQ_TEST_CHECK(moq_url_parse(lit("wss://host:443"), &url) == MOQ_ERR_INVAL);
    }

    /* -- empty host -------------------------------------------------- */
    {
        moq_url_t url;
        MOQ_TEST_CHECK(moq_url_parse(lit("moqt://:4433"), &url) == MOQ_ERR_INVAL);
    }

    /* -- empty host with default port -------------------------------- */
    {
        moq_url_t url;
        MOQ_TEST_CHECK(moq_url_parse(lit("moqt://"), &url) == MOQ_ERR_INVAL);
    }

    /* -- empty explicit port (moqt://host:) -------------------------- */
    {
        moq_url_t url;
        MOQ_TEST_CHECK(moq_url_parse(lit("moqt://host:"), &url) == MOQ_ERR_INVAL);
    }

    /* -- empty explicit port with path (moqt://host:/path) ----------- */
    {
        moq_url_t url;
        MOQ_TEST_CHECK(moq_url_parse(lit("moqt://host:/path"), &url) == MOQ_ERR_INVAL);
    }

    /* -- port 0 ------------------------------------------------------ */
    {
        moq_url_t url;
        MOQ_TEST_CHECK(moq_url_parse(lit("moqt://host:0"), &url) == MOQ_ERR_INVAL);
    }

    /* -- port > 65535 ------------------------------------------------ */
    {
        moq_url_t url;
        MOQ_TEST_CHECK(moq_url_parse(lit("moqt://host:65536"), &url) == MOQ_ERR_INVAL);
    }

    /* -- port > 65535 (large) ---------------------------------------- */
    {
        moq_url_t url;
        MOQ_TEST_CHECK(moq_url_parse(lit("moqt://host:99999"), &url) == MOQ_ERR_INVAL);
    }

    /* -- non-numeric port -------------------------------------------- */
    {
        moq_url_t url;
        MOQ_TEST_CHECK(moq_url_parse(lit("moqt://host:abc"), &url) == MOQ_ERR_INVAL);
    }

    /* -- malformed bracketed IPv6: missing ] -------------------------- */
    {
        moq_url_t url;
        MOQ_TEST_CHECK(moq_url_parse(lit("moqt://[::1:4433"), &url) == MOQ_ERR_INVAL);
    }

    /* -- malformed bracketed IPv6: empty brackets -------------------- */
    {
        moq_url_t url;
        MOQ_TEST_CHECK(moq_url_parse(lit("moqt://[]:4433"), &url) == MOQ_ERR_INVAL);
    }

    /* -- malformed bracketed IPv6: junk after ] ---------------------- */
    {
        moq_url_t url;
        MOQ_TEST_CHECK(moq_url_parse(lit("moqt://[::1]x"), &url) == MOQ_ERR_INVAL);
    }

    /* -- trailing junk after port ------------------------------------ */
    {
        moq_url_t url;
        MOQ_TEST_CHECK(moq_url_parse(lit("moqt://host:4433abc"), &url) == MOQ_ERR_INVAL);
    }

    /* -- empty input ------------------------------------------------- */
    {
        moq_url_t url;
        MOQ_TEST_CHECK(moq_url_parse(lit(""), &url) == MOQ_ERR_INVAL);
    }

    /* -- IPv6 with path ---------------------------------------------- */
    {
        moq_url_t url;
        moq_result_t rc = moq_url_parse(lit("moqt://[::1]:4433/path"), &url);
        MOQ_TEST_CHECK(rc == MOQ_OK);
        MOQ_TEST_CHECK(bv_eq(url.host, "::1"));
        MOQ_TEST_CHECK(url.port == 4433);
        MOQ_TEST_CHECK(bv_eq(url.path, "/path"));
    }

    /* -- default port with path/query -------------------------------- */
    {
        moq_url_t url;
        moq_result_t rc = moq_url_parse(lit("https://host/path?q=1"), &url);
        MOQ_TEST_CHECK(rc == MOQ_OK);
        MOQ_TEST_CHECK(bv_eq(url.host, "host"));
        MOQ_TEST_CHECK(url.port == 443);
        MOQ_TEST_CHECK(bv_eq(url.path, "/path"));
        MOQ_TEST_CHECK(bv_eq(url.query, "q=1"));
    }

    MOQ_TEST_PASS("test_url");
    return failures;
}
