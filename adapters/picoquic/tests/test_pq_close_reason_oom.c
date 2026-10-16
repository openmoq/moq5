#include <picoquic_internal.h>
#include <picoquic_utils.h>
#include <stdio.h>
#include <string.h>

static int fail_copy;
static unsigned copies, failures;
#define CHECK(x) do { if (!(x)) { fprintf(stderr, "FAIL:%d: %s\n", __LINE__, #x); failures++; } } while (0)

/* Only the test's sender translation unit calls this replacement. */
char *moq_test_reason_duplicate(const char *input)
{
    copies++;
    return fail_copy ? NULL : picoquic_string_duplicate(input);
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
    picoquic_state_enum state = cnx->cnx_state;
    uint64_t wake = cnx->next_wake_time;
    cnx->application_error = 19;
    cnx->local_error = 21;
    cnx->offending_frame_type = 23;
    cnx->local_error_reason = "borrowed original";
    fail_copy = 1;
    CHECK(picoquic_close_ex(cnx, 42, "initial failure") == PICOQUIC_ERROR_MEMORY);
    CHECK(copies == 1 && cnx->cnx_state == state && cnx->next_wake_time == wake);
    CHECK(cnx->application_error == 19 && cnx->local_error == 21);
    CHECK(cnx->offending_frame_type == 23);
    CHECK(strcmp(cnx->local_error_reason, "borrowed original") == 0);
    CHECK(cnx->local_close_reason_owned == NULL);
    fail_copy = 0;
    CHECK(picoquic_close_ex(cnx, 42, "owned original") == 0);
    const char *original = cnx->local_error_reason;
    state = cnx->cnx_state;
    wake = cnx->next_wake_time;
    uint64_t local = cnx->local_error;
    cnx->offending_frame_type = 27;
    fail_copy = 1;
    CHECK(picoquic_close_ex(cnx, 43, original + 6) == PICOQUIC_ERROR_MEMORY);
    CHECK(copies == 3 && cnx->cnx_state == state && cnx->next_wake_time == wake);
    CHECK(cnx->local_error_reason == original && strcmp(original, "owned original") == 0);
    CHECK(cnx->local_close_reason_owned == original);
    CHECK(cnx->application_error == 0 && cnx->local_error == local);
    CHECK(cnx->offending_frame_type == 27);
    fail_copy = 0;
    CHECK(picoquic_close_ex(cnx, 43, original + 6) == 0);
    CHECK(strcmp(cnx->local_error_reason, "original") == 0 && copies == 4);
    fail_copy = 1;
    CHECK(picoquic_close_ex(cnx, 42, "") == 0);
    CHECK(copies == 4 && cnx->local_error_reason == NULL);
    CHECK(picoquic_close(cnx, 42) == 0);
    CHECK(copies == 4 && cnx->local_error_reason == NULL);
    picoquic_free(quic);
    printf("close reason allocation transaction: %u failures\n", failures);
    return failures ? 1 : 0;
}
