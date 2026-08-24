/*
 * Handshake-bound propagation: moq_endpoint_cfg_t.handshake_timeout_us must
 * reach the picoquic QUIC context through the REAL service connect path.
 *
 * Resolution alone is not evidence: with only the resolve tests, deleting
 * either the assignment into moq_endpoint_t or the
 * picoquic_set_default_handshake_timeout() call leaves everything green. This
 * test observes the end of the chain instead.
 *
 * How: the endpoint object set this binary links is compiled with
 * picoquic_set_default_handshake_timeout renamed to the recorder below (a
 * private, build-gated seam -- nothing is exported from the shipping library,
 * and the production sources are unmodified). The recorder forwards to the
 * real picoquic setter and then reads the context back through
 * <picoquic_internal.h>, so what is asserted is the value the QUIC context
 * actually holds, not merely the argument that was passed.
 *
 * No peer is involved: both picoquic facades run their configure_quic hook
 * synchronously while building the QUIC context inside create, so the whole
 * chain is exercised before connect() returns. Nothing here waits on a
 * handshake, and there is no elapsed-time assertion.
 */
#include "endpoint_internal.h"
#include "test_support.h"

#include <picoquic.h>
#include <picoquic_internal.h>

#include <stdlib.h>
#include <string.h>

static int failures = 0;

/* Bound the peer's own handshake, not the test's: nothing listens on this
 * port, so the connection never completes and is torn down by destroy(). */
#define DEAD_PEER_URL_QUIC "moqt://127.0.0.1:14433"
#define DEAD_PEER_URL_WT   "https://127.0.0.1:14434/moq"
#define HS_TIMEOUT_US      3000000ull

static unsigned g_calls;        /* how often the hook reached the setter */
static uint64_t g_arg;          /* the value it passed */
static uint64_t g_in_context;   /* what the QUIC context held afterwards */

/* The symbol the endpoint object set calls in place of the picoquic setter. */
void moq_test_set_default_handshake_timeout(picoquic_quic_t *quic,
                                            uint64_t handshake_timeout_us);
void moq_test_set_default_handshake_timeout(picoquic_quic_t *quic,
                                            uint64_t handshake_timeout_us)
{
    g_calls++;
    g_arg = handshake_timeout_us;
    picoquic_set_default_handshake_timeout(quic, handshake_timeout_us);
    g_in_context = quic->default_handshake_timeout;
}

static void reset_probe(void)
{
    g_calls = 0;
    g_arg = 0;
    g_in_context = 0;
}

static moq_bytes_t B(const char *s)
{
    moq_bytes_t b = { (const uint8_t *)s, strlen(s) };
    return b;
}

/* One connect through the real public API, with the tail field set the only
 * supported way (the sized initializer). */
static moq_result_t connect_with(const char *url,
                                 moq_transport_protocol_t protocol,
                                 uint64_t timeout_us,
                                 moq_endpoint_t **out)
{
    moq_endpoint_cfg_t c;
    moq_endpoint_cfg_init_sized(&c, sizeof(c));
    c.url = B(url);
    c.protocol = protocol;
    c.insecure_skip_verify = true;   /* no peer, no cert: keep create on the
                                        configuration path under test */
    c.handshake_timeout_us = timeout_us;
    return moq_endpoint_connect(&c, out);
}

/* The propagation assertion, shared by both picoquic protocol facades: they
 * install the SAME configure_quic hook, so this is the whole contract. */
static void check_propagates(const char *what, const char *url,
                             moq_transport_protocol_t protocol)
{
    reset_probe();
    moq_endpoint_t *ep = NULL;
    moq_result_t rc = connect_with(url, protocol, HS_TIMEOUT_US, &ep);
    /* A create failure here is an environment/product failure, not a pass:
     * report it rather than skipping the assertions below. */
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
    MOQ_TEST_CHECK(ep != NULL);
    if (rc == MOQ_OK && ep != NULL) {
        /* Reached the setter exactly once ... */
        MOQ_TEST_CHECK_EQ_U64((uint64_t)g_calls, 1);
        /* ... carrying the caller's value ... */
        MOQ_TEST_CHECK_EQ_U64(g_arg, HS_TIMEOUT_US);
        /* ... and the QUIC context holds it. */
        MOQ_TEST_CHECK_EQ_U64(g_in_context, HS_TIMEOUT_US);
        moq_endpoint_stop(ep);
        moq_endpoint_destroy(ep);
    }
    (void)what;
}

/* 0 means "leave the backend's own value": the setter must not be called at
 * all, which is what distinguishes "default" from "explicitly set to 0". */
static void check_zero_leaves_backend_default(const char *url,
                                              moq_transport_protocol_t protocol)
{
    reset_probe();
    moq_endpoint_t *ep = NULL;
    moq_result_t rc = connect_with(url, protocol, 0, &ep);
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
    /* Assert the handle too: a broken MOQ_OK + NULL must fail here rather than
     * skip the setter assertion below. */
    MOQ_TEST_CHECK(ep != NULL);
    if (rc == MOQ_OK && ep != NULL) {
        MOQ_TEST_CHECK_EQ_U64((uint64_t)g_calls, 0);
        moq_endpoint_stop(ep);
        moq_endpoint_destroy(ep);
    }
}

/* A caller that predates the field (v0-sized cfg via the pointer-only init)
 * must behave exactly like an explicit 0, with nothing read past its buffer. */
static void check_old_caller_leaves_backend_default(const char *url,
                                                    moq_transport_protocol_t p)
{
    reset_probe();
    moq_endpoint_cfg_t c;
    moq_endpoint_cfg_init(&c);           /* stamps the v0 floor: field absent */
    c.url = B(url);
    c.protocol = p;
    c.insecure_skip_verify = true;
    moq_endpoint_t *ep = NULL;
    moq_result_t rc = moq_endpoint_connect(&c, &ep);
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
    MOQ_TEST_CHECK(ep != NULL);
    if (rc == MOQ_OK && ep != NULL) {
        MOQ_TEST_CHECK_EQ_U64((uint64_t)g_calls, 0);
        moq_endpoint_stop(ep);
        moq_endpoint_destroy(ep);
    }
}

int main(void)
{
#ifdef MOQ_SERVICE_HAVE_PQ_THREADED
    check_propagates("raw QUIC", DEAD_PEER_URL_QUIC,
                     MOQ_TRANSPORT_PROTOCOL_RAW_QUIC);
    check_zero_leaves_backend_default(DEAD_PEER_URL_QUIC,
                                      MOQ_TRANSPORT_PROTOCOL_RAW_QUIC);
    check_old_caller_leaves_backend_default(DEAD_PEER_URL_QUIC,
                                            MOQ_TRANSPORT_PROTOCOL_RAW_QUIC);
#endif
#ifdef MOQ_SERVICE_HAVE_PICO_WT_MANAGED
    /* The second picoquic protocol facade, through the same shared hook. */
    check_propagates("WebTransport", DEAD_PEER_URL_WT,
                     MOQ_TRANSPORT_PROTOCOL_WEBTRANSPORT);
    check_zero_leaves_backend_default(DEAD_PEER_URL_WT,
                                      MOQ_TRANSPORT_PROTOCOL_WEBTRANSPORT);
#endif
#if !defined(MOQ_SERVICE_HAVE_PQ_THREADED) && \
    !defined(MOQ_SERVICE_HAVE_PICO_WT_MANAGED)
#error "test_endpoint_handshake_timeout requires a picoquic service facade"
#endif
    MOQ_TEST_PASS("endpoint_handshake_timeout");
    return failures != 0;
}
