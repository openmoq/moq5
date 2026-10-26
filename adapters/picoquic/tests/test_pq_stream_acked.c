/*
 * The send-half acknowledgement predicate behind a graceful drain
 * (adapters/common/moq_pq_stream_backlog.h), over stream heads whose SACK
 * lists are built with picoquic's own range code, as picoquic records an
 * acknowledged STREAM frame: [offset, offset + length - 1], one position
 * more for its FIN.
 *
 * Owed: every sent byte from offset 0, the FIN position once a FIN was sent,
 * and for a reset its RESET and its reliable prefix only. Not owed: a peer
 * FIN on an open stream, anything on a receive-only stream, a reset's
 * discarded tail.
 */
#include "../../common/moq_pq_stream_backlog.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int failures = 0;
#define CHECK(cond) do { \
    if (!(cond)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #cond); \
        failures++; \
    } } while (0)

/* Client-mode stream ids: 0 local bidi, 1 peer bidi, 2 local uni, 3 peer uni. */
static picoquic_stream_head_t *stream(uint64_t id, uint64_t sent)
{
    picoquic_stream_head_t *s = calloc(1, sizeof(*s));
    if (!s) abort();
    s->stream_id = id;
    s->sent_offset = sent;
    picoquic_sack_list_init(&s->sack_list, picoquic_sack_list_stream_bytes);
    return s;
}

static void ack(picoquic_stream_head_t *s, uint64_t first, uint64_t last)
{
    CHECK(picoquic_update_sack_list(&s->sack_list, first, last, 0) == 0);
}

static void release(picoquic_stream_head_t *s)
{
    picoquic_sack_list_free(&s->sack_list);
    free(s);
}

int main(void)
{
    picoquic_cnx_t *cnx = calloc(1, sizeof(*cnx));
    if (!cnx) return 1;
    cnx->client_mode = 1;

    /* A peer-initiated unidirectional stream has no send half. */
    picoquic_stream_head_t *s = stream(3, 10);
    CHECK(moq_pq_stream_send_acked(cnx, s));
    release(s);

    /* An open stream that sent nothing owes nothing, whatever its peer does. */
    s = stream(0, 0);
    CHECK(moq_pq_stream_send_acked(cnx, s));
    release(s);

    /* An open bidi (control, CONNECT, request) owes its sent bytes, never a
     * FIN: acknowledged through byte 9 it is settled with both directions
     * still open. */
    s = stream(0, 10);
    CHECK(!moq_pq_stream_send_acked(cnx, s));
    ack(s, 0, 4);
    CHECK(!moq_pq_stream_send_acked(cnx, s));
    ack(s, 5, 9);
    CHECK(moq_pq_stream_send_acked(cnx, s));
    release(s);

    /* The same on a peer-initiated bidi this side answered on. */
    s = stream(1, 3);
    CHECK(!moq_pq_stream_send_acked(cnx, s));
    ack(s, 0, 2);
    CHECK(moq_pq_stream_send_acked(cnx, s));
    release(s);

    /* A FIN with no data owes the FIN position 0. */
    s = stream(2, 0);
    s->fin_sent = 1;
    CHECK(!moq_pq_stream_send_acked(cnx, s));
    ack(s, 0, 0);
    CHECK(moq_pq_stream_send_acked(cnx, s));
    release(s);

    /* Bytes and FIN: the bytes alone are not enough, the FIN position is. */
    s = stream(2, 10);
    s->fin_sent = 1;
    ack(s, 0, 9);
    CHECK(!moq_pq_stream_send_acked(cnx, s));
    ack(s, 10, 10);
    CHECK(moq_pq_stream_send_acked(cnx, s));
    release(s);

    /* The final range acknowledged with an earlier hole is not enough. */
    s = stream(2, 10);
    s->fin_sent = 1;
    ack(s, 5, 10);
    CHECK(!moq_pq_stream_send_acked(cnx, s));
    ack(s, 0, 3);
    CHECK(!moq_pq_stream_send_acked(cnx, s));
    ack(s, 4, 4);
    CHECK(moq_pq_stream_send_acked(cnx, s));
    release(s);

    /* An ordinary reset owes its RESET, not the discarded bytes, from the
     * moment it is requested: requested but unsent, its acknowledged bytes do
     * not settle it. */
    s = stream(2, 10);
    ack(s, 0, 9);
    s->reset_requested = 1;
    CHECK(!moq_pq_stream_send_acked(cnx, s));
    s->reset_sent = 1;
    CHECK(!moq_pq_stream_send_acked(cnx, s));
    s->reset_acked = 1;
    CHECK(moq_pq_stream_send_acked(cnx, s));
    release(s);
    s = stream(2, 10);
    s->reset_requested = 1;
    s->reset_sent = 1;
    s->reset_acked = 1;
    CHECK(moq_pq_stream_send_acked(cnx, s));
    release(s);

    /* RESET_STREAM_AT owes its RESET and its reliable prefix, not the tail. */
    s = stream(2, 10);
    s->reset_requested = 1;
    s->reset_sent = 1;
    s->reliable_size = 4;
    s->reset_acked = 1;
    CHECK(!moq_pq_stream_send_acked(cnx, s));
    ack(s, 0, 2);
    CHECK(!moq_pq_stream_send_acked(cnx, s));
    ack(s, 3, 3);
    CHECK(moq_pq_stream_send_acked(cnx, s));
    s->reset_acked = 0;
    CHECK(!moq_pq_stream_send_acked(cnx, s));
    release(s);

    /* Server mode flips which ids are local: 3 is then this side's uni. */
    cnx->client_mode = 0;
    s = stream(3, 5);
    CHECK(!moq_pq_stream_send_acked(cnx, s));
    ack(s, 0, 4);
    CHECK(moq_pq_stream_send_acked(cnx, s));
    release(s);
    s = stream(2, 5);
    CHECK(moq_pq_stream_send_acked(cnx, s));
    release(s);

    free(cnx);
    if (failures) {
        fprintf(stderr, "FAIL: %d failures\n", failures);
        return 1;
    }
    printf("PASS: pq stream acked\n");
    return 0;
}
