/*
 * Pico-WT inbound receive backpressure and capacity invariance.
 *
 * A valid workload must deliver exactly the same objects, bytes and ordering,
 * and must complete, whatever the session's event-queue capacity. Capacity may
 * change how many service passes it takes; it may not change the outcome and it
 * must never terminate the connection.
 */
#include "pico_wt_harness.h"
#include "../pico_wt_adapter.h"

#include "picoquic_internal.h"

#include <moq/pico_wt.h>
#include <moq/rcbuf.h>
#include <moq/session.h>
#include <moq/transport_bridge.h>

#include <stdio.h>
#include <string.h>

static int g_failures;
#define CHECK(cond)                                                          \
    do {                                                                     \
        if (!(cond)) {                                                       \
            fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);  \
            g_failures++;                                                    \
        }                                                                    \
    } while (0)

/*
 * Workload sizing is load-bearing. The subgroup rides ONE unidirectional WT
 * stream, whose advertised receive window here is initial_max_stream_data_uni
 * (65535 bytes by picoquic default). The workload must exceed that window, or
 * the peer never needs additional credit, never stalls, and the test would
 * prove only "no fatal" rather than real backpressure.
 *   192 * 1024 = 196608 bytes  ~=  3x the window.
 */
#define OBJ_COUNT 192
#define OBJ_BYTES 1024

/* The adapter's own teardown sentinel. Asserted by code so a failure is
 * attributed to this adapter rather than to an unrelated fatal. */
#define PW_INBOUND_FATAL_CODE 0x10

typedef struct {
    uint32_t published;          /* writes the session ACCEPTED */
    bool     subgroup_closed;
    uint32_t objects;            /* OBJECT_RECEIVED events delivered */
    uint64_t bytes;
    bool     seen[OBJ_COUNT];    /* duplicate / omission detection */
    bool     bad_id;             /* an object id outside 0..OBJ_COUNT-1 */
    bool     bad_payload;        /* a byte that does not match its id */
    bool     duplicate;
    bool     out_of_order;
    uint64_t next_expected;      /* objects arrive in publication order */
    bool     completed;
    bool     fatal;
    uint64_t fatal_code;         /* bridge fatal code, when fatal */

    /* Wire-level flow-control facts, read from picoquic (see wire_stalled). */
    bool     wire_stalled;       /* peer spent all credit and is send-blocked */
    bool     window_moved_while_pending;  /* CONTRACT VIOLATION if ever true */
    bool     granted_after_pending;       /* a grant followed pending clearing */
    uint32_t grants;             /* number of window advances observed */
    uint64_t budget;             /* the per-stream receive budget in force */
    bool     credit_bound_violated;   /* maxdata_local > consumed_offset+budget */
    bool     conn_arrest_violated;    /* sender advanced while fully frozen */
    bool     conn_arrest_observed;    /* the arrest window actually ran */
} run_result_t;

/*
 * Wire facts.
 *
 * picoquic's `consumed_offset` is incremented BEFORE the stream callback runs
 * (picoquic/picoquic/frames.c:1266), so it counts bytes delivered from the
 * transport into the callback -- NOT bytes the application has processed. That
 * makes it the wrong anchor for "application progress", but exactly the right
 * one for "how much of the credit we granted has the peer already spent":
 * against a frozen maxdata_local, consumed_offset >= maxdata_local means the
 * peer has consumed the entire window and can send nothing further.
 *
 * The pending gate is what gives a later grant its meaning: because credit is
 * withheld while the stream still has retained bridge work, a window advance
 * can only happen after the session drained that work -- i.e. it corresponds to
 * session catch-up, not to transport-to-callback delivery.
 */
static picoquic_stream_head_t *data_stream(picoquic_cnx_t *cnx, uint64_t sid)
{
    return cnx ? picoquic_find_stream(cnx, sid) : NULL;
}

/* The subgroup rides one unidirectional stream; pick the uni stream carrying
 * the most payload (the MoQ control streams carry far less). */
static uint64_t find_subgroup_stream(picoquic_cnx_t *cnx)
{
    picoquic_stream_head_t *best = NULL;
    for (picoquic_stream_head_t *s = picoquic_first_stream(cnx); s != NULL;
         s = picoquic_next_stream(s)) {
        if ((s->stream_id & 2) == 0) continue;         /* bidirectional */
        if (best == NULL || s->consumed_offset > best->consumed_offset)
            best = s;
    }
    return best ? best->stream_id : UINT64_MAX;
}

static uint64_t rx_window(picoquic_cnx_t *rx, uint64_t sid)
{
    picoquic_stream_head_t *s = data_stream(rx, sid);
    return s ? s->maxdata_local : 0;
}

/*
 * True only under genuine flow-control arrest: the receiver's frozen window is
 * fully spent, the sender has hit that window, AND the sender still holds
 * queued bytes for the stream.
 *
 * The queued-data term is what makes this a stall rather than a coincidence. A
 * stream that simply finished with its last byte landing exactly on the limit
 * also satisfies `sent_offset >= maxdata_remote`; only the adapter's own send
 * queue distinguishes "blocked with more to push" from "idle at the limit".
 */
static bool wire_stalled(pico_wt_harness_t *h, uint64_t sid)
{
    picoquic_stream_head_t *rx = data_stream(h->test_ctx->cnx_client, sid);
    picoquic_stream_head_t *tx = data_stream(h->test_ctx->cnx_server, sid);
    if (rx == NULL || tx == NULL) return false;
    return rx->consumed_offset >= rx->maxdata_local &&
           tx->sent_offset >= tx->maxdata_remote &&
           moq_pq_send_queue_has_data(h->server_conn->endpoint_ctx.queue, sid);
}

/*
 * Per-stream credit bound, phrased without overflow:
 *     maxdata_local <= consumed_offset + budget
 * Sampled on every observation point, so a single over-grant anywhere in the
 * run fails the test rather than only at the end.
 */
static bool credit_bound_holds(picoquic_cnx_t *rx, uint64_t sid, uint64_t budget)
{
    picoquic_stream_head_t *s = data_stream(rx, sid);
    if (s == NULL) return true;                 /* retired: nothing to bound */
    if (s->maxdata_local < s->consumed_offset) return true;
    return s->maxdata_local - s->consumed_offset <= budget;
}

/* Validate each object against the EXPECTED sequence: its id, every payload
 * byte, its arrival position, and that it arrives exactly once. */
static void drain_client(pico_wt_harness_t *h, run_result_t *r)
{
    moq_event_t ev;
    while (moq_session_poll_events(h->client_session, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED &&
            ev.u.object_received.payload) {
            const uint8_t *d = moq_rcbuf_data(ev.u.object_received.payload);
            size_t l = moq_rcbuf_len(ev.u.object_received.payload);
            uint64_t id = ev.u.object_received.object_id;

            if (id >= OBJ_COUNT) {
                r->bad_id = true;
            } else {
                if (r->seen[id]) r->duplicate = true;
                r->seen[id] = true;
                if (id != r->next_expected) r->out_of_order = true;
                r->next_expected = id + 1;
                if (l != OBJ_BYTES) {
                    r->bad_payload = true;
                } else {
                    for (size_t i = 0; i < l; i++)
                        if (d[i] != (uint8_t)(id & 0xff)) {
                            r->bad_payload = true;
                            break;
                        }
                }
            }
            r->objects++;
            r->bytes += l;
        }
        moq_event_cleanup(&ev);
    }
}

/* Record the first fatal seen, with the bridge's code. */
static bool note_fatal(pico_wt_harness_t *h, run_result_t *r)
{
    moq_pico_wt_conn_t *conns[2] = { h->client_conn, h->server_conn };
    for (int i = 0; i < 2; i++) {
        if (conns[i] == NULL) continue;
        if (moq_pico_wt_conn_is_fatal(conns[i])) {
            if (!r->fatal) {
                r->fatal = true;
                r->fatal_code =
                    moq_transport_bridge_fatal_code(conns[i]->bridge);
            }
            return true;
        }
    }
    return false;
}

/* One full publish/subscribe run at the given event-queue capacity. */
static void run_at_capacity(uint32_t max_events, run_result_t *out)
{
    memset(out, 0, sizeof(*out));

    pico_wt_harness_t h;
    pico_wt_harness_cfg_t cfg = { .cid_byte = 0x21, .request_capacity = 10,
                                  .max_events = max_events };
    if (pico_wt_harness_setup(&h, &cfg) != 0 ||
        pico_wt_harness_handshake(&h) != 0) {
        CHECK(0);
        pico_wt_harness_cleanup(&h);
        return;
    }

    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    moq_bytes_t ns[] = { {(const uint8_t *)"pico", 4},
                         {(const uint8_t *)"wt", 2} };
    sc.track_namespace.parts = ns;
    sc.track_namespace.count = 2;
    sc.track_name = (moq_bytes_t){(const uint8_t *)"video", 5};
    sc.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
    moq_subscription_t client_sub;
    if (moq_session_subscribe(h.client_session, &sc, 0, &client_sub) < 0)
        goto done;
    moq_pico_wt_service(h.client_conn, h.now);
    if (pico_wt_harness_pump(&h, 4000) != 0) goto done;

    moq_subscription_t server_sub = MOQ_SUBSCRIPTION_INVALID;
    {
        moq_event_t ev;
        while (moq_session_poll_events(h.server_session, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
                server_sub = ev.u.subscribe_request.sub;
                moq_accept_subscribe_cfg_t acfg;
                moq_accept_subscribe_cfg_init(&acfg);
                if (moq_session_accept_subscribe(h.server_session, server_sub,
                                                 &acfg, 0) < 0)
                    server_sub = MOQ_SUBSCRIPTION_INVALID;
            }
            moq_event_cleanup(&ev);
        }
    }
    if (!moq_subscription_is_valid(server_sub)) goto done;
    moq_pico_wt_service(h.server_conn, h.now);
    if (pico_wt_harness_pump(&h, 4000) != 0) goto done;
    drain_client(&h, out);

    /* Publish OBJ_COUNT objects across one subgroup. */
    moq_subgroup_cfg_t sgcfg;
    moq_subgroup_cfg_init(&sgcfg);
    sgcfg.group_id = 0;
    sgcfg.publisher_priority = 200;
    moq_subgroup_handle_t sg;
    if (moq_session_open_subgroup(h.server_session, server_sub, &sgcfg, 0,
                                  &sg) < 0)
        goto done;

    /*
     * PHASE 1 -- the receiving application does NOT poll while the peer keeps
     * sending. That is what makes the receiver's bridge input stay pending
     * across a service pass: the condition the adapter must survive by pausing
     * the stream instead of tearing the connection down. Draining here would
     * keep that condition from arising at all.
     *
     * A write is never skipped. Correct flow control is EXPECTED to stall the
     * publisher here once the receiver's frozen window fills, so this phase
     * ends either because the receiver went fatal or because the publisher
     * legitimately backpressured. Whatever was accepted is
     * counted in out->published; the rest is finished in phase 2.
     */
    uint8_t payload[OBJ_BYTES];
    uint32_t next = 0;
    uint64_t sid = UINT64_MAX;
    for (int round = 0; round < 4000 && next < OBJ_COUNT; round++) {
        memset(payload, (int)(next & 0xff), sizeof(payload));
        moq_rcbuf_t *buf = NULL;
        if (moq_rcbuf_create(moq_alloc_default(), payload, sizeof(payload),
                             &buf) < 0 || buf == NULL)
            goto done;
        moq_result_t wr = moq_session_write_object(h.server_session, sg,
                                                   next, buf, 0);
        moq_rcbuf_decref(buf);
        if (wr >= 0) {
            next++;                       /* ACCEPTED: only then advance */
            out->published = next;
        } else if (wr != MOQ_ERR_WOULD_BLOCK) {
            goto done;                    /* a real error, not backpressure */
        }
        moq_pico_wt_service(h.server_conn, h.now);
        if (pico_wt_harness_pump(&h, 200) != 0) goto done;
        if (note_fatal(&h, out))
            goto done;                    /* the receiver tore it down */

        /*
         * End the non-draining phase the moment the WIRE has frozen.
         *
         * moq_session_write_object() buffers below QUIC flow control, so the
         * publisher never surfaces WOULD_BLOCK and cannot mark this boundary.
         * Continuing to pump past this point would advance simulated time
         * against an already-frozen wire and manufacture an idle timeout that
         * says nothing about the adapter.
         */
        if (sid == UINT64_MAX)
            sid = find_subgroup_stream(h.test_ctx->cnx_client);
        if (sid != UINT64_MAX && wire_stalled(&h, sid)) {
            out->wire_stalled = true;
            break;
        }
    }
    if (sid == UINT64_MAX)
        sid = find_subgroup_stream(h.test_ctx->cnx_client);

    /* The per-stream budget is the advertised uni receive window -- the same
     * value the adapter derives for this stream. */
    {
        const picoquic_tp_t *tp =
            picoquic_get_transport_parameters(h.test_ctx->cnx_client, 1);
        if (tp) out->budget = tp->initial_max_stream_data_uni;
    }

    /*
     * Derivative connection-level arrest.
     *
     * picoquic's automatic connection MAX_DATA advance is not gated by the
     * per-stream app-flow-control bit, and the only direct control
     * (picoquic_set_max_data_control) is quic-wide. Connection arrest is
     * therefore DERIVATIVE: freezing every stream stops delivery, which stops
     * the delivery-anchored advance. Prove it as an observation, not a knob --
     * with the wire stalled and the application still not draining, neither the
     * sender's stream offset nor the connection-level credit it has been
     * granted may move.
     *
     * Bounded to ~1s of simulated time, far below the 30s idle timeout that
     * stays enabled, so this window cannot itself manufacture a timeout.
     */
    if (out->wire_stalled && sid != UINT64_MAX) {
        picoquic_stream_head_t *tx = data_stream(h.test_ctx->cnx_server, sid);
        uint64_t sent0 = tx ? tx->sent_offset : 0;
        uint64_t conn0 = h.test_ctx->cnx_server->maxdata_remote;
        out->conn_arrest_observed = true;
        for (int k = 0; k < 20; k++) {
            moq_pico_wt_service(h.server_conn, h.now);
            moq_pico_wt_service(h.client_conn, h.now);   /* still no drain */
            if (pico_wt_harness_pump(&h, 50) != 0) break;
            tx = data_stream(h.test_ctx->cnx_server, sid);
            if (tx && tx->sent_offset > sent0)
                out->conn_arrest_violated = true;
            if (h.test_ctx->cnx_server->maxdata_remote > conn0)
                out->conn_arrest_violated = true;
            if (!credit_bound_holds(h.test_ctx->cnx_client, sid, out->budget))
                out->credit_bound_violated = true;
        }
    }

    /*
     * PHASE 2 -- the application starts draining.
     *
     * Draining events and servicing the receiver is purely LOCAL work: it
     * involves no packet and must not advance simulated transport time, or the
     * clock outruns the drain and the idle timer fires against a wire that is
     * frozen precisely because the test asked it to be. So each round runs the
     * local loop to a fixed point at the current `now`, and only then pumps.
     *
     * The idle timeout stays enabled throughout, so a genuine liveness failure
     * -- one where the receiver stops making local progress and never grants --
     * still surfaces as a dead connection rather than being masked.
     */
    uint64_t last_win = rx_window(h.test_ctx->cnx_client, sid);
    for (int i = 0; i < 12000; i++) {
        if (next < OBJ_COUNT) {
            memset(payload, (int)(next & 0xff), sizeof(payload));
            moq_rcbuf_t *buf = NULL;
            if (moq_rcbuf_create(moq_alloc_default(), payload, sizeof(payload),
                                 &buf) < 0 || buf == NULL)
                goto done;
            moq_result_t wr = moq_session_write_object(h.server_session, sg,
                                                       next, buf, 0);
            moq_rcbuf_decref(buf);
            if (wr >= 0) { next++; out->published = next; }
            else if (wr != MOQ_ERR_WOULD_BLOCK) goto done;
        } else if (!out->subgroup_closed) {
            if (moq_session_close_subgroup(h.server_session, sg, 0) >= 0)
                out->subgroup_closed = true;
        }
        moq_pico_wt_service(h.server_conn, h.now);

        /* Local fixed point at a FIXED `now`: drain, service the receiver so
         * retained inbound work is re-driven, repeat while anything moves. */
        for (;;) {
            uint32_t before = out->objects;
            drain_client(&h, out);
            moq_pico_wt_service(h.client_conn, h.now);

            /* Sample the credit contract on every local step. A window advance
             * while the stream still has retained bridge work would mean credit
             * was replenished from transport-to-callback delivery instead of
             * session catch-up -- the exact failure the pending gate exists to
             * prevent. */
            bool pend = moq_transport_bridge_stream_has_pending(
                h.client_conn->bridge, sid);
            uint64_t win = rx_window(h.test_ctx->cnx_client, sid);
            if (win > last_win) {
                if (pend) out->window_moved_while_pending = true;
                else      out->granted_after_pending = true;
                out->grants++;
                last_win = win;
            }
            if (!credit_bound_holds(h.test_ctx->cnx_client, sid, out->budget))
                out->credit_bound_violated = true;
            if (out->objects == before) break;
        }

        if (pico_wt_harness_pump(&h, 200) != 0) break;
        if (note_fatal(&h, out)) break;
        if (out->published == OBJ_COUNT && out->subgroup_closed &&
            out->objects >= OBJ_COUNT)
            break;
    }
    drain_client(&h, out);
    out->completed = (out->published == OBJ_COUNT) && out->subgroup_closed &&
                     (out->objects == OBJ_COUNT);

done:
    (void)note_fatal(&h, out);
    pico_wt_harness_cleanup(&h);
}

/* Capacity invariance: 1, 2, 16 and 64 must agree exactly, and none may take
 * the inbound-fatal path. */
static void test_capacity_invariance(void)
{
    const uint32_t caps[] = { 1, 2, 16, 64 };
    run_result_t r[4];
    for (int i = 0; i < 4; i++) {
        run_at_capacity(caps[i], &r[i]);
        fprintf(stderr,
                "  max_events=%2u published=%u closed=%d received=%u bytes=%llu"
                " wire_stall=%d grants=%u moved_while_pending=%d"
                " completed=%d fatal=%d code=0x%llx\n",
                caps[i], r[i].published, (int)r[i].subgroup_closed,
                r[i].objects, (unsigned long long)r[i].bytes,
                (int)r[i].wire_stalled, r[i].grants,
                (int)r[i].window_moved_while_pending, (int)r[i].completed,
                (int)r[i].fatal, (unsigned long long)r[i].fatal_code);
    }
    for (int i = 0; i < 4; i++) {
        /* Never the inbound-fatal path, whatever the queue size. */
        CHECK(!r[i].fatal);
        if (r[i].fatal)   /* if it did fatal, prove WHICH fatal, for the record */
            CHECK(r[i].fatal_code == PW_INBOUND_FATAL_CODE);

        /* Completion means: everything published, the subgroup closed, and
         * everything received. */
        CHECK(r[i].completed);
        CHECK(r[i].published == OBJ_COUNT);
        CHECK(r[i].subgroup_closed);
        CHECK(r[i].objects == OBJ_COUNT);
        CHECK(r[i].bytes == (uint64_t)OBJ_COUNT * OBJ_BYTES);

        /* Validated against the EXPECTED sequence, not against another run. */
        CHECK(!r[i].bad_id);
        CHECK(!r[i].bad_payload);
        CHECK(!r[i].duplicate);
        CHECK(!r[i].out_of_order);
        for (uint32_t o = 0; o < OBJ_COUNT; o++)
            CHECK(r[i].seen[o]);

        /* The fatal path must have been replaced by REAL peer-visible
         * backpressure, not by deferred local buffering. The workload is ~3x
         * the advertised window, so at every capacity the peer must spend its
         * whole credit and block on the wire before the application drains. */
        CHECK(r[i].wire_stalled);

        /* Credit is withheld while the stream still carries retained bridge
         * work. A window advance under pending would replenish from
         * transport-to-callback delivery rather than session catch-up. */
        CHECK(!r[i].window_moved_while_pending);

        /* Clearing pending is what releases credit, and that grant is what
         * resumes wire delivery -- completion above proves the resume. */
        CHECK(r[i].granted_after_pending);

        /* Per-stream credit never exceeds the budget ahead of what the peer
         * has spent, sampled throughout the run. */
        CHECK(!r[i].credit_bound_violated);

        /* Connection-level arrest is derivative: with every stream frozen and
         * the application undrained, the sender makes no progress and gains no
         * connection credit. */
        CHECK(r[i].conn_arrest_observed);
        CHECK(!r[i].conn_arrest_violated);

        /* Grants are batched at budget/2, so the count is bounded by the
         * workload divided by the quantum -- not one grant per object. */
        CHECK(r[i].budget > 0);
        if (r[i].budget > 0) {
            uint64_t quantum = r[i].budget / 2;
            uint64_t bound = ((uint64_t)OBJ_COUNT * OBJ_BYTES) / quantum + 2;
            CHECK((uint64_t)r[i].grants <= bound);
        }
    }
}

int main(void)
{
    test_capacity_invariance();
    if (g_failures == 0)
        printf("PASS: test_pico_wt_backpressure\n");
    else
        fprintf(stderr, "FAILED: test_pico_wt_backpressure (%d)\n", g_failures);
    return g_failures ? 1 : 0;
}
