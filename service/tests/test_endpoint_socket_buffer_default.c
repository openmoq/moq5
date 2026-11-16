/*
 * Service picoquic socket buffer default: on Linux, the raw-threaded client
 * that the service endpoint builds must ask for 8 MB UDP socket buffers
 * (socket_buffer_bytes), so a burst does not overflow the kernel's default
 * receive buffer of about 208 KB. Elsewhere it must leave the system default
 * (0), because macOS refuses a size above kern.ipc.maxsockbuf and the socket
 * would not open.
 *
 * It uses the same seam as test_endpoint_keepalive_default.c: the endpoint
 * object set this binary links calls the recorder below in place of
 * moq_pq_threaded_create, which forwards to the real create. Nothing listens
 * on the port, and destroy() tears the connection down.
 */
#include "endpoint_internal.h"
#include "test_support.h"

#include <moq/picoquic_threaded.h>

#include <stdlib.h>
#include <string.h>

static int failures = 0;

#define DEAD_PEER_URL_QUIC "moqt://127.0.0.1:14454"
#ifdef __linux__
#define WANT_SOCKET_BUFFER_BYTES 8388608u
#else
#define WANT_SOCKET_BUFFER_BYTES 0u
#endif

static unsigned g_calls;       /* how often create was reached */
static uint32_t g_buf_bytes;   /* socket_buffer_bytes it carried */

moq_result_t moq_test_pq_threaded_create(const moq_pq_threaded_cfg_t *cfg,
                                         moq_pq_threaded_t **out);
moq_result_t moq_test_pq_threaded_create(const moq_pq_threaded_cfg_t *cfg,
                                         moq_pq_threaded_t **out)
{
    g_calls++;
    g_buf_bytes = cfg->socket_buffer_bytes;
    return moq_pq_threaded_create(cfg, out);
}

static moq_bytes_t B(const char *s)
{
    moq_bytes_t b = { (const uint8_t *)s, strlen(s) };
    return b;
}

int main(void)
{
    moq_endpoint_cfg_t c;
    moq_endpoint_cfg_init_sized(&c, sizeof(c));
    c.url = B(DEAD_PEER_URL_QUIC);
    c.protocol = MOQ_TRANSPORT_PROTOCOL_RAW_QUIC;
    c.insecure_skip_verify = true;

    moq_endpoint_t *ep = NULL;
    moq_result_t rc = moq_endpoint_connect(&c, &ep);
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
    MOQ_TEST_CHECK(ep != NULL);
    if (rc == MOQ_OK && ep != NULL) {
        MOQ_TEST_CHECK_EQ_U64((uint64_t)g_calls, 1);
        MOQ_TEST_CHECK_EQ_U64((uint64_t)g_buf_bytes, (uint64_t)WANT_SOCKET_BUFFER_BYTES);
        moq_endpoint_stop(ep);
        moq_endpoint_destroy(ep);
    }

    if (failures) return 1;
    printf("PASS: endpoint_socket_buffer_default\n");
    return 0;
}
