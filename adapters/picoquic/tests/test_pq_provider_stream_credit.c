/*
 * Provider-capacity policy regression: picoquic's peer uni stream credit
 * around a gapped FIN and around RESET_STREAM / RESET_STREAM_AT.
 *
 * LibMoQ's receive-capacity bound needs two things from the provider's
 * MAX_STREAMS uni replenishment:
 *
 *   1. No replenishment on account of a stream whose FIN arrived past a gap:
 *      bytes below the final size are still missing, so the stream is still
 *      live and must not be counted yet.
 *   2. A stream that retires through a completed plain reset, or through a
 *      reliable reset once its reliable prefix is delivered, is counted
 *      exactly once.
 *
 * Under the provider's existing replenishment with a local uni stream limit
 * I of 1 (a new grant of I once 2c + I > 2L), that means: a gapped FIN leaves
 * the count c at 0 and the grant L at 1 until the gap is filled; a retired
 * stream leaves c at 1 and L at 2.
 *
 * This is a policy for the provider LibMoQ relies on, not a conformance
 * requirement. MAX_STREAMS grants cumulative permission; RFC 9000 does not
 * fix when a receiver replenishes it, so neither early credit nor the moment
 * this provider replenishes is itself an RFC rule.
 *
 * The pinned provider meets this policy through the tracked picoquic patch
 * 0009-return-stream-credit-at-final-size.patch, which counts a stream once
 * every byte up to its FIN is consumed (evaluated after the FIN's final size
 * is stored) or once a reset's reliable prefix is consumed. A provider
 * without it fails the policy rows: it counts a gapped FIN early, comparing
 * the delivered length with itself, and deletes a stream reset past its
 * delivered bytes without counting it. The control rows hold either way.
 *
 * Method: decoder and producer testing after a simulated handshake. Each row
 * builds a fresh client/server pair over the tls_api simulated links in
 * virtual time. Most rows hand a fixed handful of frames for client uni
 * streams to the server connection's own frame decoders, as received packets
 * would be, and call the MAX_STREAMS producer directly without transmitting
 * what it generates; credit_reaches_peer instead runs real packets and pins
 * that the replacement grant reaches the client and admits its next stream.
 * The application callbacks count bytes, reset and FIN events; they do not
 * compare payload contents. Every row prints the values it observed, so the
 * counterexample stays visible whatever the result.
 *
 * Results: a setup or decoder-acceptance failure is reported as a
 * PREREQUISITE failure and stops that row (exit 2); a policy or control
 * assertion that does not hold is reported as a POLICY FAILURE (exit 1).
 */
#include <picoquictest_internal.h>
#include <picoquic_utils.h>
#include <inttypes.h>
#include <stdio.h>
#include <string.h>

static int policy_failures;
static int prerequisite_failures;
static const char *row;

/* A policy or control expectation. */
#define CHECK(x) do { if (!(x)) { \
    fprintf(stderr, "POLICY FAILURE[%s]:%d: %s\n", row, __LINE__, #x); \
    policy_failures++; \
} } while (0)

/* A precondition the row depends on: on failure the row stops. */
#define REQUIRE(x) do { if (!(x)) { \
    fprintf(stderr, "PREREQUISITE[%s]:%d: %s\n", row, __LINE__, #x); \
    prerequisite_failures++; goto done; \
} } while (0)

/* -- Frozen frames (client-initiated uni stream 2) ------------------------ *
 * STREAM type 0x08 | OFF 0x04 | LEN 0x02 | FIN 0x01; every varint fits in
 * one byte. */
static const uint8_t S2_BYTE0[]      = { 0x0a, 0x02, 0x01, 0xaf };        /* off 0, 1 byte */
static const uint8_t S2_BYTE1[]      = { 0x0e, 0x02, 0x01, 0x01, 0x40 };  /* off 1, 1 byte */
static const uint8_t S2_FIN_AT2[]    = { 0x0f, 0x02, 0x02, 0x00 };        /* off 2, len 0, FIN */
/* RESET_STREAM (0x04): stream, error, final size. */
static const uint8_t S2_RESET_F3[]   = { 0x04, 0x02, 0x07, 0x03 };
static const uint8_t S2_RESET_F1[]   = { 0x04, 0x02, 0x07, 0x01 };
/* RESET_STREAM_AT (0x24): stream, error, final size, reliable size. */
static const uint8_t S2_RESET_AT_F3_R2[] = { 0x24, 0x02, 0x07, 0x03, 0x02 };
static const uint8_t S2_RESET_AT_F2_R2[] = { 0x24, 0x02, 0x07, 0x02, 0x02 };

#define RESET_CODE 7u

typedef struct {
    picoquic_test_tls_api_ctx_t *tls;
    uint64_t now, loss;
    /* Server-side application events on client uni stream 2. */
    size_t data;
    unsigned fins, resets, wrong_reset_codes;
} pair_t;

static int callback(picoquic_cnx_t *cnx, uint64_t sid, uint8_t *bytes,
                    size_t length, picoquic_call_back_event_t event,
                    void *context, void *stream_context)
{
    pair_t *p = context;
    (void)bytes; (void)stream_context;
    if (cnx != p->tls->cnx_server || sid != 2)
        return 0;
    if (event == picoquic_callback_stream_data || event == picoquic_callback_stream_fin) {
        p->data += length;
        if (event == picoquic_callback_stream_fin) p->fins++;
    } else if (event == picoquic_callback_stream_reset) {
        p->resets++;
        if (picoquic_get_remote_stream_error(cnx, sid) != RESET_CODE)
            p->wrong_reset_codes++;
    }
    return 0;
}

static void teardown(pair_t *p)
{
    if (p->tls) tls_api_delete_ctx(p->tls);
    p->tls = NULL;
}

/* A connected pair whose server advertises a local uni stream limit of one
 * and reset_stream_at. Returns 0 only when the server is ready with I = 1,
 * L = 1, c = 0, reset_stream_at negotiated and no stream 2 yet; otherwise
 * reports which precondition failed, unwinds, and returns -1. */
static int setup(pair_t *p)
{
    memset(p, 0, sizeof(*p));
    picoquic_solution_dir = PICOQUIC_SOURCE_DIR;
    if (tls_api_init_ctx(&p->tls, PICOQUIC_INTERNAL_TEST_VERSION_1,
            PICOQUIC_TEST_SNI, PICOQUIC_TEST_ALPN, &p->now, NULL, NULL, 0, 1, 0) != 0) {
        p->tls = NULL;
        REQUIRE(!"tls_api_init_ctx");
    }
    picoquic_set_callback(p->tls->cnx_client, callback, p);
    picoquic_set_default_callback(p->tls->qserver, callback, p);
    picoquic_tp_t tp = *picoquic_get_transport_parameters(p->tls->cnx_client, 1);
    tp.is_reset_stream_at_enabled = 1;
    picoquic_set_transport_parameters(p->tls->cnx_client, &tp);
    picoquic_set_default_tp_value(p->tls->qserver, picoquic_tp_reset_stream_at, 1);
    picoquic_set_default_tp_value(p->tls->qserver, picoquic_tp_initial_max_streams_uni, 1);
    REQUIRE(picoquic_start_client_cnx(p->tls->cnx_client) == 0);
    REQUIRE(tls_api_connection_loop(p->tls, &p->loss, 0, &p->now) == 0);
    REQUIRE(p->tls->cnx_server != NULL);

    /* The loop may stop at server false start; run the simulation on until
     * the server is ready (bounded, virtual time). */
    picoquic_cnx_t *s = p->tls->cnx_server;
    uint64_t limit = p->now + 2000000;
    for (unsigned i = 0; i < 1000 && picoquic_get_cnx_state(s) != picoquic_state_ready &&
                         p->now < limit; i++) {
        int active = 0;
        REQUIRE(tls_api_one_sim_round(p->tls, &p->now, limit, &active) == 0);
    }
    REQUIRE(picoquic_get_cnx_state(s) == picoquic_state_ready);
    REQUIRE(s->local_parameters.initial_max_stream_id_unidir == 1);
    REQUIRE(s->max_streams_unidir_local == 1);
    REQUIRE(s->max_streams_unidir_local_computed == 0);
    REQUIRE(s->is_reset_stream_at_enabled);
    REQUIRE(picoquic_find_stream(s, 2) == NULL);
    return 0;
done:
    teardown(p);
    return -1;
}

/* Hand one frame to the server's decoder for its type; returns whether it
 * was accepted (the decoder consumed it whole, no connection error). */
static int inject(pair_t *p, const uint8_t *f, size_t len)
{
    picoquic_cnx_t *s = p->tls->cnx_server;
    const uint8_t *end;
    if (f[0] >= 0x08 && f[0] <= 0x0f)
        end = picoquic_decode_stream_frame(s, f, f + len, NULL, p->now);
    else if (f[0] == picoquic_frame_type_reset_stream)
        end = picoquic_decode_reset_stream_frame(s, f, f + len);
    else
        end = picoquic_decode_reset_stream_at_frame(s, f, f + len);
    return end == f + len && s->local_error == 0 &&
           picoquic_get_cnx_state(s) == picoquic_state_ready;
}
#define INJECT(p, f) REQUIRE(inject((p), (f), sizeof(f)))

static uint64_t credit(pair_t *p) { return p->tls->cnx_server->max_streams_unidir_local_computed; }

/* Call picoquic's MAX_STREAMS producer directly, as packet preparation
 * would, into a scratch buffer that is never sent; return the uni grant it
 * leaves. Returns UINT64_MAX if the producer could not complete. */
static uint64_t grant(pair_t *p)
{
    uint8_t buf[64];
    int more = 0, pure = 1;
    (void)picoquic_format_max_streams_frame_if_needed(p->tls->cnx_server,
        buf, buf + sizeof(buf), &more, &pure);
    return more ? UINT64_MAX : p->tls->cnx_server->max_streams_unidir_local;
}

static void observe(pair_t *p, const char *when)
{
    picoquic_stream_head_t *st = picoquic_find_stream(p->tls->cnx_server, 2);
    printf("  observed[%s] %s: c=%" PRIu64 " L=%" PRIu64 " stream2=%s",
           row, when, credit(p), p->tls->cnx_server->max_streams_unidir_local,
           st ? "live" : "gone");
    if (st)
        printf(" consumed=%" PRIu64 " fin_offset=%" PRIu64 " fin_received=%d"
               " fin_signalled=%d counted=%d",
               st->consumed_offset, st->fin_offset, (int)st->fin_received,
               (int)st->fin_signalled, (int)st->max_stream_updated);
    printf(" data=%zu fins=%u resets=%u\n", p->data, p->fins, p->resets);
}

/* Policy 1: one byte delivered, then a zero-length FIN at offset 2 with byte
 * 1 never sent. The stream is still missing data, so it is not counted and
 * the grant does not move. */
static void gapped_fin(void)
{
    pair_t p; row = "gapped_fin";
    if (setup(&p) != 0) return;
    INJECT(&p, S2_BYTE0);
    REQUIRE(p.data == 1 && credit(&p) == 0);
    INJECT(&p, S2_FIN_AT2);
    observe(&p, "after gapped FIN");
    {
        picoquic_stream_head_t *st = picoquic_find_stream(p.tls->cnx_server, 2);
        REQUIRE(st != NULL && st->fin_received && !st->fin_signalled);
        REQUIRE(st->consumed_offset == 1 && st->fin_offset == 2);
        REQUIRE(p.fins == 0);
        CHECK(!st->max_stream_updated);
    }
    CHECK(credit(&p) == 0);
    uint64_t l = grant(&p);
    REQUIRE(l != UINT64_MAX);
    observe(&p, "after producer");
    CHECK(l == 1);
done:
    teardown(&p);
}

/* Control: the gap is filled before the FIN. Not counted while incomplete;
 * counted once at completion. */
static void contiguous_fin(void)
{
    pair_t p; row = "contiguous_fin";
    if (setup(&p) != 0) return;
    INJECT(&p, S2_BYTE0);
    INJECT(&p, S2_BYTE1);
    CHECK(credit(&p) == 0);
    INJECT(&p, S2_FIN_AT2);
    observe(&p, "after FIN");
    CHECK(p.fins == 1 && p.data == 2);
    CHECK(picoquic_find_stream(p.tls->cnx_server, 2) == NULL);
    CHECK(credit(&p) == 1);
    uint64_t l = grant(&p);
    REQUIRE(l != UINT64_MAX);
    CHECK(l == 2);
done:
    teardown(&p);
}

/* Policy 1 with the gap filled afterwards: not counted while the gap is
 * open; once the missing byte completes the stream, the FIN is delivered,
 * the stream retires, and it is counted exactly once. */
static void gap_filled_late(void)
{
    pair_t p; row = "gap_filled_late";
    if (setup(&p) != 0) return;
    INJECT(&p, S2_BYTE0);
    INJECT(&p, S2_FIN_AT2);
    observe(&p, "before fill");
    CHECK(credit(&p) == 0);
    INJECT(&p, S2_BYTE1);
    observe(&p, "after fill");
    CHECK(p.fins == 1 && p.data == 2);
    CHECK(picoquic_find_stream(p.tls->cnx_server, 2) == NULL);
    CHECK(credit(&p) == 1);                  /* exactly once */
done:
    teardown(&p);
}

/* Policy 2: one byte delivered, then RESET_STREAM with final size 3. The
 * reset completes reception; the stream retires and is counted once. */
static void reset_past_delivered(void)
{
    pair_t p; row = "reset_past_delivered";
    if (setup(&p) != 0) return;
    INJECT(&p, S2_BYTE0);
    INJECT(&p, S2_RESET_F3);
    observe(&p, "after reset");
    REQUIRE(p.resets == 1 && p.wrong_reset_codes == 0);
    REQUIRE(picoquic_find_stream(p.tls->cnx_server, 2) == NULL);
    CHECK(credit(&p) == 1);
    uint64_t l = grant(&p);
    REQUIRE(l != UINT64_MAX);
    observe(&p, "after producer");
    CHECK(l == 2);
done:
    teardown(&p);
}

/* Control: RESET_STREAM whose final size is what was delivered. */
static void reset_fully_consumed(void)
{
    pair_t p; row = "reset_fully_consumed";
    if (setup(&p) != 0) return;
    INJECT(&p, S2_BYTE0);
    INJECT(&p, S2_RESET_F1);
    observe(&p, "after reset");
    REQUIRE(p.resets == 1 && p.wrong_reset_codes == 0);
    CHECK(picoquic_find_stream(p.tls->cnx_server, 2) == NULL);
    CHECK(credit(&p) == 1);
    uint64_t l = grant(&p);
    REQUIRE(l != UINT64_MAX);
    CHECK(l == 2);
done:
    teardown(&p);
}

/* Policy 2: RESET_STREAM_AT with reliable size 2 < final size 3. The reset
 * waits for its prefix; once byte 1 delivers it, the reset is signalled, the
 * stream retires, and it is counted once. */
static void reliable_reset_past_delivered(void)
{
    pair_t p; row = "reliable_reset_past_delivered";
    if (setup(&p) != 0) return;
    INJECT(&p, S2_BYTE0);
    INJECT(&p, S2_RESET_AT_F3_R2);
    REQUIRE(p.resets == 0);                  /* prefix still owed */
    REQUIRE(picoquic_find_stream(p.tls->cnx_server, 2) != NULL);
    CHECK(credit(&p) == 0);
    INJECT(&p, S2_BYTE1);
    observe(&p, "after prefix");
    REQUIRE(p.data == 2 && p.resets == 1 && p.fins == 0 && p.wrong_reset_codes == 0);
    REQUIRE(picoquic_find_stream(p.tls->cnx_server, 2) == NULL);
    CHECK(credit(&p) == 1);
    uint64_t l = grant(&p);
    REQUIRE(l != UINT64_MAX);
    observe(&p, "after producer");
    CHECK(l == 2);
done:
    teardown(&p);
}

/* Control: RESET_STREAM_AT whose final size equals its reliable size. */
static void reliable_reset_fully_consumed(void)
{
    pair_t p; row = "reliable_reset_fully_consumed";
    if (setup(&p) != 0) return;
    INJECT(&p, S2_BYTE0);
    INJECT(&p, S2_RESET_AT_F2_R2);
    CHECK(p.resets == 0 && credit(&p) == 0);
    INJECT(&p, S2_BYTE1);
    observe(&p, "after prefix");
    REQUIRE(p.wrong_reset_codes == 0);
    CHECK(p.data == 2 && p.resets == 1);
    CHECK(picoquic_find_stream(p.tls->cnx_server, 2) == NULL);
    CHECK(credit(&p) == 1);
    uint64_t l = grant(&p);
    REQUIRE(l != UINT64_MAX);
    CHECK(l == 2);
done:
    teardown(&p);
}

/* Control: a FIN past a gap that is never filled. However long the
 * connection runs, the stream stays live and uncounted and the grant stays
 * where it was. */
static void unfinished_gap(void)
{
    pair_t p; row = "unfinished_gap";
    if (setup(&p) != 0) return;
    INJECT(&p, S2_BYTE0);
    INJECT(&p, S2_FIN_AT2);
    uint64_t limit = p.now + 500000;
    for (unsigned i = 0; i < 200 && p.now < limit; i++) {
        int active = 0;
        REQUIRE(tls_api_one_sim_round(p.tls, &p.now, limit, &active) == 0);
    }
    observe(&p, "after the wait");
    CHECK(picoquic_find_stream(p.tls->cnx_server, 2) != NULL);
    CHECK(credit(&p) == 0);
    CHECK(p.tls->cnx_server->max_streams_unidir_local == 1);
    CHECK(p.tls->cnx_client->max_streams_unidir_remote == 1);
done:
    teardown(&p);
}

/* Control: a gapped FIN, then a RESET_STREAM with the same final size, then
 * the missing byte. The reset ends reception; the stream is counted once,
 * and the late byte for the retired stream counts nothing more. */
static void duplicate_terminal(void)
{
    pair_t p; row = "duplicate_terminal";
    if (setup(&p) != 0) return;
    INJECT(&p, S2_BYTE0);
    INJECT(&p, S2_FIN_AT2);
    static const uint8_t reset_f2[] = { 0x04, 0x02, 0x07, 0x02 };
    INJECT(&p, reset_f2);
    observe(&p, "after reset");
    REQUIRE(p.resets == 1 && p.wrong_reset_codes == 0);
    CHECK(credit(&p) == 1);
    INJECT(&p, S2_BYTE1);
    observe(&p, "after late byte");
    CHECK(credit(&p) == 1);                  /* exactly once */
    CHECK(p.fins == 0);
done:
    teardown(&p);
}

/* Control over real packets in virtual time: with a local uni limit of one,
 * the client opens stream 2, sends a byte and its FIN. The replacement
 * MAX_STREAMS the server owes must reach the client, which can then open
 * stream 6, whose byte the server receives. */
static size_t s6_data;
static int callback6(picoquic_cnx_t *cnx, uint64_t sid, uint8_t *bytes,
                     size_t length, picoquic_call_back_event_t event,
                     void *context, void *stream_context)
{
    pair_t *p = context;
    if (cnx == p->tls->cnx_server && sid == 6 &&
        (event == picoquic_callback_stream_data ||
         event == picoquic_callback_stream_fin))
        s6_data += length;
    return callback(cnx, sid, bytes, length, event, context, stream_context);
}

static void credit_reaches_peer(void)
{
    pair_t p; row = "credit_reaches_peer";
    s6_data = 0;
    if (setup(&p) != 0) return;
    picoquic_set_callback(p.tls->cnx_server, callback6, &p);
    picoquic_cnx_t *c = p.tls->cnx_client;
    REQUIRE(c->max_streams_unidir_remote == 1);
    static const uint8_t one = 0x5a;
    REQUIRE(picoquic_add_to_stream(c, 2, &one, 1, 1) == 0);
    uint64_t limit = p.now + 2000000;
    for (unsigned i = 0; i < 2000 && p.now < limit &&
                         c->max_streams_unidir_remote < 2; i++) {
        int active = 0;
        REQUIRE(tls_api_one_sim_round(p.tls, &p.now, limit, &active) == 0);
    }
    observe(&p, "after stream 2");
    CHECK(p.fins == 1 && p.data == 1);
    CHECK(credit(&p) == 1);
    CHECK(c->max_streams_unidir_remote == 2);
    REQUIRE(picoquic_add_to_stream(c, 6, &one, 1, 1) == 0);
    limit = p.now + 2000000;
    for (unsigned i = 0; i < 2000 && p.now < limit && s6_data == 0; i++) {
        int active = 0;
        REQUIRE(tls_api_one_sim_round(p.tls, &p.now, limit, &active) == 0);
    }
    CHECK(s6_data == 1);
done:
    teardown(&p);
}

int main(void)
{
    printf("provider source: %s\n", PICOQUIC_SOURCE_DIR);

    gapped_fin();
    contiguous_fin();
    gap_filled_late();
    reset_past_delivered();
    reset_fully_consumed();
    reliable_reset_past_delivered();
    reliable_reset_fully_consumed();
    unfinished_gap();
    duplicate_terminal();
    credit_reaches_peer();

    if (prerequisite_failures) {
        fprintf(stderr, "test_pq_provider_stream_credit: %d PREREQUISITE failure(s), "
                "%d policy failure(s); rows with a failed prerequisite did not "
                "reach their policy checks\n", prerequisite_failures, policy_failures);
        return 2;
    }
    if (policy_failures) {
        fprintf(stderr, "test_pq_provider_stream_credit: POLICY FAILURE, %d failure(s): "
                "the provider does not meet LibMoQ's stream-credit policy\n",
                policy_failures);
        return 1;
    }
    printf("test_pq_provider_stream_credit: policy holds (10 rows)\n");
    return 0;
}
