/*
 * RFC 9000 section 20.1: only transport codes 0x100..0x1ff are TLS alerts.
 * Exercise the public endpoint query and real threaded-adapter observers
 * without sockets: a fixture-owned facade carries the latched close state.
 */
#include <moq/endpoint.h>
#include "../../adapters/picoquic/pq_threaded_internal.h"
#include "test_support.h"

#include <stdio.h>

moq_endpoint_t *moq_endpoint_test_make_bare(void);
void moq_endpoint_test_free_bare(moq_endpoint_t *ep);
void moq_endpoint_test_attach_picoquic(moq_endpoint_t *ep, moq_pq_threaded_t *t);

static int failures;

int main(void)
{
    static const struct {
        uint64_t error;
        moq_endpoint_terminal_reason_t reason;
    } rows[] = {
        { 0, MOQ_ENDPOINT_TERMINAL_TRANSPORT },
        { 0x0c, MOQ_ENDPOINT_TERMINAL_TRANSPORT },
        { 0xff, MOQ_ENDPOINT_TERMINAL_TRANSPORT },
        { 0x100, MOQ_ENDPOINT_TERMINAL_TLS },
        { 0x12a, MOQ_ENDPOINT_TERMINAL_TLS_CERTIFICATE },
        { 0x12c, MOQ_ENDPOINT_TERMINAL_TLS_CERTIFICATE },
        { 0x12d, MOQ_ENDPOINT_TERMINAL_TLS_CERTIFICATE },
        { 0x12e, MOQ_ENDPOINT_TERMINAL_TLS_CERTIFICATE },
        { 0x130, MOQ_ENDPOINT_TERMINAL_TLS_CERTIFICATE },
        { 0x178, MOQ_ENDPOINT_TERMINAL_TLS },
        { 0x1ff, MOQ_ENDPOINT_TERMINAL_TLS },
        { 0x200, MOQ_ENDPOINT_TERMINAL_TRANSPORT },
        { 0x201, MOQ_ENDPOINT_TERMINAL_TRANSPORT },
        { 0x10130, MOQ_ENDPOINT_TERMINAL_TRANSPORT },
        { UINT64_C(0x3fffffffffff0130), MOQ_ENDPOINT_TERMINAL_TRANSPORT },
        { UINT64_C(0x3fffffffffffffff), MOQ_ENDPOINT_TERMINAL_TRANSPORT },
    };
    moq_pq_threaded_t t = {0};
    int rc = pthread_mutex_init(&t.mutex, NULL);
    MOQ_TEST_CHECK_EQ_INT(rc, 0);
    if (rc != 0) return 1;
    moq_endpoint_t *ep = moq_endpoint_test_make_bare();
    MOQ_TEST_CHECK(ep != NULL);
    if (!ep) {
        pthread_mutex_destroy(&t.mutex);
        return 1;
    }
    moq_endpoint_test_attach_picoquic(ep, &t);
    t.fatal = true;
    for (size_t i = 0; i < sizeof(rows) / sizeof(rows[0]); i++) {
        int before = failures;
        t.terminal_error = rows[i].error;
        moq_endpoint_terminal_t terminal;
        MOQ_TEST_CHECK_EQ_INT(moq_endpoint_get_terminal(ep, &terminal,
            sizeof(terminal)), MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(terminal.struct_size, sizeof(terminal));
        MOQ_TEST_CHECK_EQ_INT(terminal.reason, rows[i].reason);
        MOQ_TEST_CHECK_EQ_U64(terminal.detail_code, rows[i].error);
        printf("%s: transport code 0x%llx\n",
            failures == before ? "PASS" : "FAIL",
            (unsigned long long)rows[i].error);
    }

    moq_endpoint_terminal_t terminal;
    t.fatal_code = 17;
    t.terminal_error = 0x130;
    MOQ_TEST_CHECK_EQ_INT(moq_endpoint_get_terminal(ep, &terminal,
        sizeof(terminal)), MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(terminal.reason, MOQ_ENDPOINT_TERMINAL_PROTOCOL);
    MOQ_TEST_CHECK_EQ_U64(terminal.detail_code, 17);

    t.fatal = false;
    t.fatal_code = 0;
    MOQ_TEST_CHECK_EQ_INT(moq_endpoint_get_terminal(ep, &terminal,
        sizeof(terminal)), MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(terminal.reason, MOQ_ENDPOINT_TERMINAL_NONE);
    MOQ_TEST_CHECK_EQ_U64(terminal.detail_code, 0);

    moq_endpoint_test_free_bare(ep);
    MOQ_TEST_CHECK_EQ_INT(pthread_mutex_destroy(&t.mutex), 0);
    return failures ? 1 : 0;
}
