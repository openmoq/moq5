/*
 * Interop-runner URL policy: which relay URLs override the WebTransport
 * CONNECT path, and with exactly which bytes.
 *
 * The policy exists because a pathless https:// URL cannot mean "/moq" -- the
 * user named a host and no path, and interop relays commonly publish
 * WebTransport at "/". Everything else keeps the endpoint's own resolution.
 *
 * Every row asserts the applied result, not a verdict: an overriding row must
 * produce exactly one byte '/', and a control must leave a pre-seeded span
 * byte-for-byte untouched.
 */
#include "url_policy.h"
#include "test_support.h"

#include <string.h>

/* A pre-seed no control may disturb, distinct from both "/" and "/moq". */
static const uint8_t k_seed[] = "SEED-PATH";

static int check_override(const char *url, const char *what)
{
    int failures = 0;
    moq_bytes_t path = { k_seed, sizeof(k_seed) - 1 };

    if (!interop_resolve_wt_path(url, &path)) {
        fprintf(stderr, "FAIL: %s: %s did not override the path\n", what, url);
        return failures + 1;
    }
    MOQ_TEST_CHECK_EQ_SIZE(path.len, 1u);
    MOQ_TEST_CHECK(path.data != NULL);
    if (path.data != NULL && path.len == 1 && path.data[0] != '/') {
        fprintf(stderr, "FAIL: %s: %s -> '%c', expected '/'\n", what, url,
                (char)path.data[0]);
        failures++;
    }
    return failures;
}

static int check_no_override(const char *url, const char *what)
{
    int failures = 0;
    moq_bytes_t path = { k_seed, sizeof(k_seed) - 1 };

    if (interop_resolve_wt_path(url, &path)) {
        fprintf(stderr, "FAIL: %s: %s overrode the path\n", what,
                url ? url : "(NULL)");
        failures++;
    }
    /* The pre-seeded span must survive untouched, span and bytes. */
    MOQ_TEST_CHECK(path.data == k_seed);
    MOQ_TEST_CHECK_EQ_SIZE(path.len, sizeof(k_seed) - 1);
    if (path.data != NULL && path.len == sizeof(k_seed) - 1)
        MOQ_TEST_CHECK(memcmp(path.data, k_seed, sizeof(k_seed) - 1) == 0);
    return failures;
}

int main(void)
{
    int failures = 0;

    /* Pathless HTTPS: the policy, with and without an explicit port, and with
     * a query that leaves the path empty. */
    failures += check_override("https://relay.example", "bare host");
    failures += check_override("https://relay.example:4443", "explicit port");
    failures += check_override("https://[::1]:4443", "IPv6 literal");
    failures += check_override("https://relay.example?x=1", "query only");
    failures += check_override("https://relay.example:4443?x=1",
                               "port and query");

    /* An explicit path is the caller's choice -- including "/" itself, which
     * already resolves to the root, and "/moq", which must not be rewritten. */
    failures += check_no_override("https://relay.example/", "explicit root");
    failures += check_no_override("https://relay.example/moq", "explicit moq");
    failures += check_no_override("https://relay.example:4443/moq",
                                  "port and explicit moq");
    failures += check_no_override("https://relay.example/moq?x=1",
                                  "explicit moq with query");

    /* Raw QUIC URLs have no WebTransport path at all. */
    failures += check_no_override("moqt://relay.example:4433", "moqt port");
    failures += check_no_override("moqt://relay.example", "moqt bare");
    failures += check_no_override("moq://relay.example:4433", "moq scheme");

    /* Input the parser rejects, and the NULL the option table can hand us. */
    failures += check_no_override(NULL, "null url");
    failures += check_no_override("", "empty");
    failures += check_no_override("relay.example", "no scheme");
    failures += check_no_override("https://", "no host");
    failures += check_no_override("https://relay.example:", "empty port");
    failures += check_no_override("ftp://relay.example", "unsupported scheme");
    failures += check_no_override("https://relay.example:0", "zero port");

    /* A NULL output is refused rather than dereferenced. */
    MOQ_TEST_CHECK(!interop_resolve_wt_path("https://relay.example", NULL));

    MOQ_TEST_PASS("interop_url_policy");
    return failures;
}
