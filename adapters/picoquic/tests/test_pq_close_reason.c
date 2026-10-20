#include "picoquic_endpoint.h"
#include <picoquic_internal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static unsigned failures;
static unsigned close_callbacks, path_callbacks;
#define CHECK(x) do { if (!(x)) { fprintf(stderr, "FAIL:%d: %s\n", __LINE__, #x); failures++; } } while (0)

static int deletion_callback(picoquic_cnx_t *cnx, uint64_t sid, uint8_t *bytes,
    size_t len, picoquic_call_back_event_t event, void *ctx, void *stream_ctx)
{
    (void)sid; (void)bytes; (void)len; (void)ctx; (void)stream_ctx;
    if (event == picoquic_callback_close || event == picoquic_callback_path_deleted) {
        CHECK(cnx->local_error_reason != NULL);
        if (cnx->local_error_reason)
            CHECK(strcmp(cnx->local_error_reason, "deferred adapter reason") == 0);
        if (event == picoquic_callback_close) close_callbacks++;
        else path_callbacks++;
    }
    return 0;
}

static void check_frame(picoquic_cnx_t *cnx, const char *reason)
{
    uint8_t bytes[2048];
    int more = 0, pure = 1;
    size_t len = strlen(reason);
    uint8_t *end = picoquic_format_application_close_frame(cnx, bytes,
        bytes + sizeof(bytes), &more, &pure);
    size_t prefix = len < 64 ? 3 : 4;
    CHECK(end == bytes + prefix + len);
    CHECK(!more && !pure);
    CHECK(bytes[0] == 0x1d && bytes[1] == 42);
    if (len < 64) CHECK(bytes[2] == len);
    else CHECK(bytes[2] == (0x40 | (len >> 8)) && bytes[3] == (len & 255));
    CHECK(memcmp(bytes + prefix, reason, len) == 0);
}

/* Return before encoding: the endpoint's temporary C string has expired. */
static void close_endpoint(picoquic_cnx_t *cnx)
{
    pq_endpoint_ctx_t ep = {0};
    moq_transport_endpoint_ops_t ops = {0};
    CHECK(pq_endpoint_init(&ops, &ep, cnx, moq_alloc_default(), 0) == 0);
    if (!ep.queue) return;
    static const uint8_t reason[] = "deferred adapter reason";
    CHECK(ops.close_transport(&ep, 42, reason, sizeof(reason) - 1) == MOQ_TRANSPORT_OK);
    pq_endpoint_cleanup(&ep);
}

int main(void)
{
    picoquic_quic_t *quic = picoquic_create(4, NULL, NULL, NULL, "moq-00",
        NULL, NULL, NULL, NULL, NULL, 0, NULL, NULL, NULL, 0);
    if (!quic) return 2;
    struct sockaddr_in peer = { .sin_family = AF_INET, .sin_port = htons(4433) };
    peer.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    picoquic_cnx_t *cnx = picoquic_create_client_cnx(quic,
        (struct sockaddr *)&peer, 0, 0, "localhost", "moq-00", NULL, NULL);
    if (!cnx) { picoquic_free(quic); return 2; }
    cnx->cnx_state = picoquic_state_ready;
    close_endpoint(cnx);
    check_frame(cnx, "deferred adapter reason");
    CHECK(picoquic_close_ex(cnx, 7, "refused replacement") != 0);
    CHECK(cnx->application_error == 42);
    check_frame(cnx, "deferred adapter reason");
    cnx->are_path_callbacks_enabled = 1;
    picoquic_set_callback(cnx, deletion_callback, NULL);
    picoquic_delete_cnx(cnx);
    CHECK(close_callbacks == 1 && path_callbacks == 1);

    cnx = picoquic_create_client_cnx(quic, (struct sockaddr *)&peer,
        0, 0, "localhost", "moq-00", NULL, NULL);
    if (!cnx) { picoquic_free(quic); return 2; }
    char mutable_reason[] = "owned handshake reason";
    CHECK(picoquic_close_ex(cnx, 42, mutable_reason) == 0);
    memset(mutable_reason, 'x', sizeof(mutable_reason) - 1);
    CHECK(strcmp(cnx->local_error_reason, "owned handshake reason") == 0);
    /* Handshake failure still accepts replacement, including aliased input. */
    CHECK(picoquic_close_ex(cnx, 42, cnx->local_error_reason + 6) == 0);
    CHECK(strcmp(cnx->local_error_reason, "handshake reason") == 0);
    CHECK(picoquic_close(cnx, 42) == 0);
    CHECK(cnx->local_error_reason == NULL);
    CHECK(picoquic_close_ex(cnx, 42, "") == 0);
    CHECK(cnx->local_error_reason == NULL);
    picoquic_delete_cnx(cnx);

    /* Every rejected state must reject before dereferencing caller storage. */
    for (int state = picoquic_state_disconnecting;
         state <= picoquic_state_disconnected; state++) {
        cnx = picoquic_create_client_cnx(quic, (struct sockaddr *)&peer,
            0, 0, "localhost", "moq-00", NULL, NULL);
        if (!cnx) { picoquic_free(quic); return 2; }
        cnx->cnx_state = (picoquic_state_enum)state;
        cnx->application_error = 19;
        cnx->local_error_reason = "borrowed literal";
        uint64_t wake = cnx->next_wake_time;
        CHECK(picoquic_close_ex(cnx, 42, (const char *)(uintptr_t)1) != 0);
        CHECK(cnx->cnx_state == (picoquic_state_enum)state);
        CHECK(cnx->application_error == 19 && cnx->next_wake_time == wake);
        CHECK(strcmp(cnx->local_error_reason, "borrowed literal") == 0);
        picoquic_delete_cnx(cnx);
    }

    cnx = picoquic_create_client_cnx(quic, (struct sockaddr *)&peer,
        0, 0, "localhost", "moq-00", NULL, NULL);
    if (!cnx) { picoquic_free(quic); return 2; }
    CHECK(picoquic_connection_error_ex(cnx, 1, 0, "transport literal") != 0);
    CHECK(picoquic_close_ex(cnx, 42, "application replacement") == 0);
    CHECK(strcmp(cnx->local_error_reason, "application replacement") == 0);
    picoquic_delete_cnx(cnx);

    const size_t lengths[] = {0, 254, 255, 256, 1024};
    for (size_t i = 0; i < sizeof(lengths) / sizeof(lengths[0]); i++) {
        cnx = picoquic_create_client_cnx(quic, (struct sockaddr *)&peer,
            0, 0, "localhost", "moq-00", NULL, NULL);
        if (!cnx) { picoquic_free(quic); return 2; }
        cnx->cnx_state = picoquic_state_ready;
        char input[1025], expected[1025];
        memset(input, 'a', lengths[i]);
        input[lengths[i]] = 0;
        memcpy(expected, input, lengths[i] + 1);
        CHECK(picoquic_close_ex(cnx, 42, input) == 0);
        memset(input, 'z', lengths[i]);
        uint8_t small[2];
        int more = 0, pure = 1;
        CHECK(picoquic_format_application_close_frame(cnx, small,
            small + sizeof(small), &more, &pure) == small);
        CHECK(more == 1);
        check_frame(cnx, expected);
        check_frame(cnx, expected);
        picoquic_delete_cnx(cnx);

        cnx = picoquic_create_client_cnx(quic, (struct sockaddr *)&peer,
            0, 0, "localhost", "moq-00", NULL, NULL);
        if (!cnx) { picoquic_free(quic); return 2; }
        cnx->cnx_state = picoquic_state_ready;
        pq_endpoint_ctx_t ep = {0};
        moq_transport_endpoint_ops_t ops = {0};
        CHECK(pq_endpoint_init(&ops, &ep, cnx, moq_alloc_default(), 0) == 0);
        if (!ep.queue) { picoquic_free(quic); return 2; }
        CHECK(ops.close_transport(&ep, 42, (const uint8_t *)expected,
            lengths[i]) == MOQ_TRANSPORT_OK);
        pq_endpoint_cleanup(&ep);
        if (lengths[i] > 255) expected[255] = 0;
        check_frame(cnx, expected);
        picoquic_delete_cnx(cnx);
    }
    picoquic_free(quic);
    printf("close reason ownership: %u failures\n", failures);
    return failures ? 1 : 0;
}
