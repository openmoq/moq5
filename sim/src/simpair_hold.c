#include "simpair_internal.h"

/* -- Receive-side backpressure ----------------------------------------- *
 * A session refuses an input with MOQ_ERR_WOULD_BLOCK when it cannot surface
 * what the input completes (its event queue is full) or cannot admit a new
 * stream. The refusal says what the session retained (session.h:
 * moq_session_on_data_bytes, _on_data_reset, _on_control_bytes,
 * _on_bidi_stream_bytes, moq_session_has_transport_stream), and the transport
 * keeps the rest. SimPair does what the transport bridge does for each input
 * kind (core/src/bridge/transport_bridge.c):
 *
 *   control bytes   retained by the session; retried with process_pending,
 *                   later control bytes wait (on_peer_control_bytes,
 *                   bridge_feed_peer_control, bridge_retry_inbound_pending).
 *                   On a unidirectional control pair a blocked control
 *                   channel also pauses the session's other unidirectional
 *                   inputs (bridge_uni_pair_inbound).
 *   data bytes      retained by an owning session (empty re-feed); a stream
 *                   the session did not admit is delivered again as it was
 *                   (bridge_feed_peer_data, the HOLD_INPUT admission).
 *   request bytes   retained by the owning session (empty re-feed); a refusal
 *                   for a request stream the session does not own is fatal
 *                   (on_peer_bidi_bytes).
 *   RESET           interrupts held bytes and reaches the session at once; a
 *                   refused reset is delivered again with its code
 *                   (on_peer_stream_reset, pending_reset).
 *   STOP_SENDING    a refused stop is delivered again with its code, apart
 *                   from the stream's bytes (on_peer_stop_sending,
 *                   pending_stop).
 *   datagram        a refused datagram is dropped (on_peer_datagram).
 *
 * Held inputs resume at the start of every step, so the caller drains events
 * between steps and nothing written is lost; a refusal never touches any
 * other input or action. */

static bool inbound_holding(const sim_inbound_t *in)
{
    return in->hold != SIM_HOLD_NONE || in->head != NULL;
}

static void inbound_free(moq_simpair_t *sp, sim_inbound_t *in)
{
    sim_delay_entry_t *e = in->head;
    while (e) {
        sim_delay_entry_t *next = e->next;
        sim_delay_free_entry(sp, e);
        e = next;
    }
    memset(in, 0, sizeof(*in));
}

/* Copy a chunk into the held queue, at the tail or (a refused chunk the
 * session did not retain, which precedes everything held) at the head. The
 * copy is not a delay entry: it takes no delay sequence number. */
static moq_result_t inbound_push(moq_simpair_t *sp, sim_inbound_t *in,
                                 const uint8_t *data, size_t len, bool fin,
                                 bool at_head)
{
    size_t total = sizeof(sim_delay_entry_t) + len;
    sim_delay_entry_t *e = (sim_delay_entry_t *)sp->alloc.alloc(
        total, sp->alloc.ctx);
    if (!e) return MOQ_ERR_NOMEM;
    memset(e, 0, sizeof(*e));
    e->alloc_size = total;
    e->len = len;
    e->fin = fin;
    e->bidi_slot = -1;
    e->data_slot = -1;
    if (len > 0 && data) memcpy(e->bytes, data, len);
    if (at_head) {
        e->next = in->head;
        in->head = e;
        if (!in->tail) in->tail = e;
    } else {
        if (in->tail) in->tail->next = e;
        else in->head = e;
        in->tail = e;
    }
    return MOQ_OK;
}

static sim_delay_entry_t *inbound_pop(sim_inbound_t *in)
{
    sim_delay_entry_t *e = in->head;
    in->head = e->next;
    if (!in->head) in->tail = NULL;
    e->next = NULL;
    return e;
}

static moq_session_t *session_of(moq_simpair_t *sp, moq_perspective_t p)
{
    return p == MOQ_PERSPECTIVE_CLIENT ? sp->client : sp->server;
}

static moq_perspective_t other_side(moq_perspective_t p)
{
    return p == MOQ_PERSPECTIVE_CLIENT ? MOQ_PERSPECTIVE_SERVER
                                       : MOQ_PERSPECTIVE_CLIENT;
}

static void count_progress(moq_perspective_t to_persp,
                           size_t *to_server, size_t *to_client)
{
    if (to_persp == MOQ_PERSPECTIVE_SERVER) {
        if (to_server) (*to_server)++;
    } else if (to_client) {
        (*to_client)++;
    }
}

/* -- Slot lifetime ------------------------------------------------------ */

void sim_stream_slot_deactivate(moq_simpair_t *sp, int slot)
{
    if (slot < 0 || slot >= MOQ_SIM_MAX_DATA_STREAMS) return;
    moq_sim_stream_map_t *m = &sp->stream_map[slot];
    inbound_free(sp, &m->in);
    m->active = false;
}

void sim_stream_slot_retire(moq_simpair_t *sp, int slot)
{
    if (slot < 0 || slot >= MOQ_SIM_MAX_DATA_STREAMS) return;
    sim_stream_slot_deactivate(sp, slot);
    sp->stream_map[slot].generation++;
}

void sim_bidi_slot_retire(moq_simpair_t *sp, int bslot)
{
    if (bslot < 0 || bslot >= MOQ_SIM_MAX_BIDI_STREAMS) return;
    moq_sim_bidi_map_t *m = &sp->bidi_map[bslot];
    inbound_free(sp, &m->in[0]);
    inbound_free(sp, &m->in[1]);
    m->active = false;
    m->generation++;
}

/* A bidi has two independent send halves, so a single FIN (e.g. a
 * responder's REQUEST_ERROR closing the response half) must not retire the
 * slot while the opener's half is still open. */
void sim_bidi_mark_fin(moq_simpair_t *sp, int bslot, moq_perspective_t from)
{
    if (bslot < 0 || bslot >= MOQ_SIM_MAX_BIDI_STREAMS) return;
    moq_sim_bidi_map_t *m = &sp->bidi_map[bslot];
    if (from == m->opener) m->opener_fin = true;
    else                   m->responder_fin = true;
    if (m->opener_fin && m->responder_fin)
        sim_bidi_slot_retire(sp, bslot);
}

void sim_held_clear(moq_simpair_t *sp)
{
    for (int i = 0; i < MOQ_SIM_MAX_DATA_STREAMS; i++)
        inbound_free(sp, &sp->stream_map[i].in);
    for (int i = 0; i < MOQ_SIM_MAX_BIDI_STREAMS; i++) {
        inbound_free(sp, &sp->bidi_map[i].in[0]);
        inbound_free(sp, &sp->bidi_map[i].in[1]);
    }
    inbound_free(sp, &sp->ctrl_in[0]);
    inbound_free(sp, &sp->ctrl_in[1]);
}

/* The receiver accepted the stream's FIN, so in this lossless transport
 * every byte of the sender's stream is committed; the sender learns it the
 * way an adapter would report it. */
static void sim_report_committed(moq_simpair_t *sp, int dslot)
{
    moq_session_t *sender = session_of(sp, sp->stream_map[dslot].sender);
    moq_stream_ref_t ref = moq_stream_ref_from_u64(sp->stream_map[dslot].sender_ref);
    if (sp->borrowed_sender == sender) {
        for (size_t i = 0; i < sp->committed_count; ++i)
            if (sp->committed[i]._v == ref._v) return;
        /* Each accepted FIN originates in one of this batch's actions.
         * Refuse loudly if that bound is ever violated; never overrun. */
        if (sp->committed_count == MOQ_SIM_ACTION_BATCH) {
            sp->committed_overflow = true;
            return;
        }
        sp->committed[sp->committed_count++] = ref;
    } else {
        (void)moq_session_on_data_committed(sender, ref, sp->now_us);
    }
}

/* -- Control channel ------------------------------------------------------ */

static moq_result_t control_deliver(moq_simpair_t *sp, moq_session_t *to,
                                    const uint8_t *data, size_t len,
                                    moq_perspective_t from,
                                    moq_perspective_t to_persp)
{
    moq_bytes_t bytes = { data, len };
    moq_result_t rc = moq_session_on_control_bytes(to, data, len, sp->now_us);
    trace_input(sp, MOQ_SIM_INPUT_CONTROL_BYTES, from, to_persp, bytes, rc,
                SIM_INPUT_DETAIL_NONE);
    if (rc == MOQ_ERR_WOULD_BLOCK) {
        /* Control bytes are always retained by the session. */
        sp->ctrl_in[sim_side(to_persp)].hold = SIM_HOLD_RETAINED;
        return MOQ_OK;
    }
    return rc;
}

moq_result_t sim_control_input(
    moq_simpair_t *sp, moq_session_t *to, const uint8_t *data, size_t len,
    moq_perspective_t from, moq_perspective_t to_persp)
{
    sim_inbound_t *in = &sp->ctrl_in[sim_side(to_persp)];
    if (inbound_holding(in))
        return inbound_push(sp, in, data, len, false, false);
    return control_deliver(sp, to, data, len, from, to_persp);
}

/* On a unidirectional control pair the bridge defers every other
 * unidirectional input to a session whose control channel is blocked, so
 * nothing is consumed out of order (bridge_uni_pair_inbound). */
static bool control_pauses_uni(const moq_simpair_t *sp,
                               moq_perspective_t to_persp)
{
    int k = sim_side(to_persp);
    return sp->ctrl_uni_pair[k] && inbound_holding(&sp->ctrl_in[k]);
}

static moq_result_t resume_control(moq_simpair_t *sp, moq_perspective_t to_persp,
                                   size_t *to_server, size_t *to_client)
{
    sim_inbound_t *in = &sp->ctrl_in[sim_side(to_persp)];
    if (!inbound_holding(in)) return MOQ_OK;
    moq_perspective_t from = other_side(to_persp);
    moq_session_t *to = session_of(sp, to_persp);
    bool progressed = false;
    if (in->hold == SIM_HOLD_RETAINED) {
        moq_result_t rc = moq_session_process_pending(to, sp->now_us);
        trace_input(sp, MOQ_SIM_INPUT_CONTROL_BYTES, from, to_persp,
                    (moq_bytes_t){0}, rc, SIM_INPUT_DETAIL_NONE);
        if (rc == MOQ_ERR_WOULD_BLOCK) return MOQ_OK;
        if (rc < 0) return rc;
        in->hold = SIM_HOLD_NONE;
        progressed = true;
    }
    while (in->hold == SIM_HOLD_NONE && in->head) {
        sim_delay_entry_t *e = inbound_pop(in);
        moq_result_t rc = control_deliver(sp, to, e->bytes, e->len, from,
                                          to_persp);
        sim_delay_free_entry(sp, e);
        if (rc < 0) return rc;
        progressed = true;
    }
    if (progressed) count_progress(to_persp, to_server, to_client);
    return MOQ_OK;
}

/* -- Data streams --------------------------------------------------------- */

/* Feed one chunk now. *refused reports a WOULD_BLOCK, which is recorded on
 * the slot as the owed refusal and is not returned. */
static moq_result_t data_deliver(
    moq_simpair_t *sp, moq_session_t *to, moq_stream_ref_t ref,
    const uint8_t *data, size_t len, bool fin, int dslot,
    moq_perspective_t from, moq_perspective_t to_persp, bool *refused)
{
    *refused = false;
    moq_bytes_t bytes = { data, len };
    moq_result_t rc = moq_session_on_data_bytes(
        to, ref, data, len, fin, sp->now_us);
    trace_input(sp, MOQ_SIM_INPUT_DATA_BYTES, from, to_persp, bytes, rc,
                (sim_input_detail_t){ ref, 0, fin });
    if (rc == MOQ_ERR_WOULD_BLOCK && dslot >= 0) {
        sim_inbound_t *in = &sp->stream_map[dslot].in;
        *refused = true;
        if (moq_session_has_transport_stream(to, ref)) {
            in->hold = SIM_HOLD_RETAINED;
            in->hold_fin = in->hold_fin || fin;
            return MOQ_OK;
        }
        in->hold = SIM_HOLD_REDELIVER;
        return inbound_push(sp, in, data, len, fin, true);
    }
    if (rc >= 0 && fin && dslot >= 0) {
        sim_report_committed(sp, dslot);
        sim_stream_slot_retire(sp, dslot);
    }
    return rc;
}

static moq_result_t data_reset_deliver(
    moq_simpair_t *sp, moq_session_t *to, moq_stream_ref_t ref,
    uint64_t error_code, int dslot,
    moq_perspective_t from, moq_perspective_t to_persp, bool *refused)
{
    *refused = false;
    moq_result_t rc = moq_session_on_data_reset(
        to, ref, error_code, sp->now_us);
    trace_input(sp, MOQ_SIM_INPUT_DATA_RESET, from, to_persp,
                (moq_bytes_t){0}, rc,
                (sim_input_detail_t){ ref, error_code, false });
    if (rc == MOQ_ERR_WOULD_BLOCK && dslot >= 0) {
        /* The reset is recorded; its closure is owed and completes when the
         * same reset is delivered again. */
        *refused = true;
        sp->stream_map[dslot].in.hold = SIM_HOLD_RESET;
        sp->stream_map[dslot].in.hold_code = error_code;
        return MOQ_OK;
    }
    if (rc >= 0 && dslot >= 0)
        sim_stream_slot_retire(sp, dslot);
    return rc;
}

/* Retire only on failure: a successful STOP lets the sender queue a
 * follow-up RESET_DATA, which routes back to the receiver and retires the
 * slot through the reset path. */
static moq_result_t data_stop_deliver(
    moq_simpair_t *sp, moq_session_t *sender_session,
    moq_stream_ref_t sender_ref, uint64_t error_code, int dslot,
    moq_perspective_t from, moq_perspective_t sender_persp, bool *refused)
{
    *refused = false;
    moq_result_t rc = moq_session_on_data_stop(
        sender_session, sender_ref, error_code, sp->now_us);
    trace_input(sp, MOQ_SIM_INPUT_DATA_STOP, from, sender_persp,
                (moq_bytes_t){0}, rc,
                (sim_input_detail_t){ sender_ref, error_code, false });
    if (rc == MOQ_ERR_WOULD_BLOCK && dslot >= 0) {
        *refused = true;
        sp->stream_map[dslot].in.stop_owed = true;
        sp->stream_map[dslot].in.stop_code = error_code;
        return MOQ_OK;
    }
    if (rc < 0 && dslot >= 0)
        sim_stream_slot_retire(sp, dslot);
    return rc;
}

moq_result_t sim_data_input(
    moq_simpair_t *sp, moq_session_t *to, moq_stream_ref_t ref,
    const uint8_t *data, size_t len, bool fin,
    int dslot, moq_perspective_t from, moq_perspective_t to_persp)
{
    if (dslot >= 0 && (inbound_holding(&sp->stream_map[dslot].in) ||
                       control_pauses_uni(sp, to_persp)))
        return inbound_push(sp, &sp->stream_map[dslot].in, data, len, fin,
                            false);
    bool refused;
    return data_deliver(sp, to, ref, data, len, fin, dslot, from, to_persp,
                        &refused);
}

moq_result_t sim_data_reset_input(
    moq_simpair_t *sp, moq_session_t *to, moq_stream_ref_t ref,
    uint64_t error_code, int dslot,
    moq_perspective_t from, moq_perspective_t to_persp)
{
    if (dslot >= 0 && inbound_holding(&sp->stream_map[dslot].in)) {
        sim_inbound_t *in = &sp->stream_map[dslot].in;
        /* A RESET interrupts delivery (on_peer_stream_reset): a stream the
         * session owns receives the reset now, whatever it retained, and the
         * data still held for it is abandoned when the slot retires (RFC 9000
         * §3.2 permits discarding a reset stream's unread data). A stream the
         * session never admitted is retired without a session input. */
        if (in->hold == SIM_HOLD_RESET) return MOQ_OK;   /* already owed */
        if (in->hold == SIM_HOLD_REDELIVER) {
            sim_stream_slot_retire(sp, dslot);
            return MOQ_OK;
        }
    }
    bool refused;
    return data_reset_deliver(sp, to, ref, error_code, dslot, from, to_persp,
                              &refused);
}

moq_result_t sim_data_stop_input(
    moq_simpair_t *sp, moq_session_t *sender_session,
    moq_stream_ref_t sender_ref, uint64_t error_code,
    int dslot, moq_perspective_t from, moq_perspective_t sender_persp)
{
    if (dslot >= 0 && sp->stream_map[dslot].in.stop_owed)
        return MOQ_OK;   /* the first STOP's code is the one owed */
    bool refused;
    return data_stop_deliver(sp, sender_session, sender_ref, error_code, dslot,
                             from, sender_persp, &refused);
}

static moq_result_t resume_data(moq_simpair_t *sp, int i,
                                size_t *to_server, size_t *to_client)
{
    moq_sim_stream_map_t *m = &sp->stream_map[i];
    moq_perspective_t from = m->sender;
    moq_perspective_t to_persp = other_side(from);
    moq_session_t *to = session_of(sp, to_persp);
    moq_stream_ref_t ref = moq_stream_ref_from_u64(m->receiver_ref);
    uint64_t generation = m->generation;
    bool progressed = false;
    bool refused = false;
    moq_result_t rc;

    if (inbound_holding(&m->in) && !control_pauses_uni(sp, to_persp)) {
        /* Settle the owed refusal first: it precedes everything held. */
        bool settled = true;
        switch (m->in.hold) {
        case SIM_HOLD_RETAINED: {
            bool fin = m->in.hold_fin;
            rc = moq_session_on_data_bytes(to, ref, NULL, 0, false, sp->now_us);
            trace_input(sp, MOQ_SIM_INPUT_DATA_BYTES, from, to_persp,
                        (moq_bytes_t){0}, rc,
                        (sim_input_detail_t){ ref, 0, false });
            if (rc == MOQ_ERR_WOULD_BLOCK) { settled = false; break; }
            if (rc < 0) return rc;
            m->in.hold = SIM_HOLD_NONE;
            m->in.hold_fin = false;
            progressed = true;
            if (fin) {
                /* The retained FIN is consumed with this re-feed. */
                sim_report_committed(sp, i);
                sim_stream_slot_retire(sp, i);
            }
            break;
        }
        case SIM_HOLD_RESET:
            rc = data_reset_deliver(sp, to, ref, m->in.hold_code, i, from,
                                    to_persp, &refused);
            if (rc < 0) return rc;
            if (refused) { settled = false; break; }
            progressed = true;
            break;
        case SIM_HOLD_REDELIVER:
            /* The refused chunk heads the held queue. */
            m->in.hold = SIM_HOLD_NONE;
            break;
        case SIM_HOLD_NONE:
            break;
        }

        /* Then the held chunks, in order, until the session refuses again. */
        while (settled && m->active && m->generation == generation &&
               m->in.hold == SIM_HOLD_NONE && m->in.head) {
            sim_delay_entry_t *e = inbound_pop(&m->in);
            rc = data_deliver(sp, to, ref, e->bytes, e->len, e->fin, i, from,
                              to_persp, &refused);
            /* A refused redelivery re-queued its own copy at the head. */
            bool repeated = refused && m->in.hold == SIM_HOLD_REDELIVER;
            sim_delay_free_entry(sp, e);
            if (rc < 0) return rc;
            if (!repeated) progressed = true;
        }
    }

    /* An owed STOP goes to the sender, independent of the bytes. */
    if (m->active && m->generation == generation && m->in.stop_owed) {
        rc = data_stop_deliver(sp, session_of(sp, m->sender),
                               moq_stream_ref_from_u64(m->sender_ref),
                               m->in.stop_code, i, to_persp, m->sender,
                               &refused);
        if (rc < 0) return rc;
        if (!refused) {
            if (m->active && m->generation == generation)
                m->in.stop_owed = false;
            count_progress(m->sender, to_server, to_client);
        }
    }

    if (progressed) count_progress(to_persp, to_server, to_client);
    return MOQ_OK;
}

/* -- Request streams (bidi) ----------------------------------------------- */

static moq_stream_ref_t bidi_ref_at(const moq_sim_bidi_map_t *m,
                                    moq_perspective_t to_persp)
{
    return moq_stream_ref_from_u64(to_persp == m->opener ? m->opener_ref
                                                         : m->responder_ref);
}

static moq_result_t bidi_deliver(
    moq_simpair_t *sp, moq_session_t *to, moq_stream_ref_t ref,
    const uint8_t *data, size_t len, bool fin, int bslot,
    moq_perspective_t from, moq_perspective_t to_persp)
{
    moq_bytes_t bytes = { data, len };
    moq_result_t rc = moq_session_on_bidi_stream_bytes(
        to, ref, data, len, fin, sp->now_us);
    trace_input(sp, MOQ_SIM_INPUT_BIDI_BYTES, from, to_persp, bytes, rc,
                (sim_input_detail_t){ ref, 0, fin });
    if (rc == MOQ_ERR_WOULD_BLOCK && bslot >= 0) {
        /* A refusal on a request stream the session does not own is fatal
         * to the bridge (on_peer_bidi_bytes), so it is an error here too. */
        if (!moq_session_has_transport_stream(to, ref))
            return MOQ_ERR_INTERNAL;
        sim_inbound_t *in = &sp->bidi_map[bslot].in[sim_side(to_persp)];
        in->hold = SIM_HOLD_RETAINED;
        in->hold_fin = in->hold_fin || fin;
        return MOQ_OK;
    }
    /* A FIN closes the sender's half; the slot retires once both close. */
    if (rc >= 0 && fin && bslot >= 0)
        sim_bidi_mark_fin(sp, bslot, from);
    return rc;
}

static moq_result_t bidi_reset_deliver(
    moq_simpair_t *sp, moq_session_t *to, moq_stream_ref_t ref,
    uint64_t error_code, int bslot,
    moq_perspective_t from, moq_perspective_t to_persp, bool *refused)
{
    *refused = false;
    moq_result_t rc = moq_session_on_bidi_stream_reset(
        to, ref, error_code, sp->now_us);
    trace_input(sp, MOQ_SIM_INPUT_BIDI_RESET, from, to_persp,
                (moq_bytes_t){0}, rc,
                (sim_input_detail_t){ ref, error_code, false });
    if (rc == MOQ_ERR_WOULD_BLOCK && bslot >= 0) {
        sim_inbound_t *in = &sp->bidi_map[bslot].in[sim_side(to_persp)];
        *refused = true;
        in->hold = SIM_HOLD_RESET;
        in->hold_code = error_code;
        return MOQ_OK;
    }
    if (rc >= 0 && bslot >= 0)
        sim_bidi_slot_retire(sp, bslot);
    return rc;
}

/* STOP_SENDING does not retire the bidi: the peer may follow it with a
 * RESET_STREAM that still needs the mapping. Retire only on failure. */
static moq_result_t bidi_stop_deliver(
    moq_simpair_t *sp, moq_session_t *to, moq_stream_ref_t ref,
    uint64_t error_code, int bslot,
    moq_perspective_t from, moq_perspective_t to_persp, bool *refused)
{
    *refused = false;
    moq_result_t rc = moq_session_on_bidi_stream_stop(
        to, ref, error_code, sp->now_us);
    trace_input(sp, MOQ_SIM_INPUT_BIDI_STOP, from, to_persp,
                (moq_bytes_t){0}, rc,
                (sim_input_detail_t){ ref, error_code, false });
    if (rc == MOQ_ERR_WOULD_BLOCK && bslot >= 0) {
        sim_inbound_t *in = &sp->bidi_map[bslot].in[sim_side(to_persp)];
        *refused = true;
        in->stop_owed = true;
        in->stop_code = error_code;
        return MOQ_OK;
    }
    if (rc < 0 && bslot >= 0)
        sim_bidi_slot_retire(sp, bslot);
    return rc;
}

moq_result_t sim_bidi_input(
    moq_simpair_t *sp, moq_session_t *to, moq_stream_ref_t ref,
    const uint8_t *data, size_t len, bool fin,
    int bslot, moq_perspective_t from, moq_perspective_t to_persp)
{
    if (bslot >= 0) {
        sim_inbound_t *in = &sp->bidi_map[bslot].in[sim_side(to_persp)];
        if (inbound_holding(in))
            return inbound_push(sp, in, data, len, fin, false);
    }
    return bidi_deliver(sp, to, ref, data, len, fin, bslot, from, to_persp);
}

moq_result_t sim_bidi_reset_input(
    moq_simpair_t *sp, moq_session_t *to, moq_stream_ref_t ref,
    uint64_t error_code, int bslot,
    moq_perspective_t from, moq_perspective_t to_persp)
{
    /* A RESET interrupts held bytes, as on a data stream: the session gets it
     * now, and the held bytes go when the slot retires. */
    if (bslot >= 0 &&
        sp->bidi_map[bslot].in[sim_side(to_persp)].hold == SIM_HOLD_RESET)
        return MOQ_OK;   /* already owed */
    bool refused;
    return bidi_reset_deliver(sp, to, ref, error_code, bslot, from, to_persp,
                              &refused);
}

moq_result_t sim_bidi_stop_input(
    moq_simpair_t *sp, moq_session_t *to, moq_stream_ref_t ref,
    uint64_t error_code, int bslot,
    moq_perspective_t from, moq_perspective_t to_persp)
{
    if (bslot >= 0 && sp->bidi_map[bslot].in[sim_side(to_persp)].stop_owed)
        return MOQ_OK;   /* the first STOP's code is the one owed */
    bool refused;
    return bidi_stop_deliver(sp, to, ref, error_code, bslot, from, to_persp,
                             &refused);
}

static moq_result_t resume_bidi_half(moq_simpair_t *sp, int b,
                                     moq_perspective_t to_persp,
                                     size_t *to_server, size_t *to_client)
{
    moq_sim_bidi_map_t *m = &sp->bidi_map[b];
    sim_inbound_t *in = &m->in[sim_side(to_persp)];
    moq_perspective_t from = other_side(to_persp);
    moq_session_t *to = session_of(sp, to_persp);
    moq_stream_ref_t ref = bidi_ref_at(m, to_persp);
    uint64_t generation = m->generation;
    bool progressed = false;
    bool refused = false;
    bool settled = true;
    moq_result_t rc;

    switch (in->hold) {
    case SIM_HOLD_RETAINED: {
        bool fin = in->hold_fin;
        rc = moq_session_on_bidi_stream_bytes(to, ref, NULL, 0, false,
                                              sp->now_us);
        trace_input(sp, MOQ_SIM_INPUT_BIDI_BYTES, from, to_persp,
                    (moq_bytes_t){0}, rc,
                    (sim_input_detail_t){ ref, 0, false });
        if (rc == MOQ_ERR_WOULD_BLOCK) { settled = false; break; }
        if (rc < 0) return rc;
        in->hold = SIM_HOLD_NONE;
        in->hold_fin = false;
        progressed = true;
        if (fin) sim_bidi_mark_fin(sp, b, from);
        break;
    }
    case SIM_HOLD_RESET:
        rc = bidi_reset_deliver(sp, to, ref, in->hold_code, b, from, to_persp,
                                &refused);
        if (rc < 0) return rc;
        if (refused) { settled = false; break; }
        progressed = true;
        break;
    case SIM_HOLD_REDELIVER:   /* not used on request streams */
    case SIM_HOLD_NONE:
        break;
    }

    while (settled && m->active && m->generation == generation &&
           in->hold == SIM_HOLD_NONE && in->head) {
        sim_delay_entry_t *e = inbound_pop(in);
        rc = bidi_deliver(sp, to, ref, e->bytes, e->len, e->fin, b, from,
                          to_persp);
        sim_delay_free_entry(sp, e);
        if (rc < 0) return rc;
        progressed = true;
    }

    if (m->active && m->generation == generation && in->stop_owed) {
        rc = bidi_stop_deliver(sp, to, ref, in->stop_code, b, from, to_persp,
                               &refused);
        if (rc < 0) return rc;
        if (!refused) {
            if (m->active && m->generation == generation)
                in->stop_owed = false;
            progressed = true;
        }
    }

    if (progressed) count_progress(to_persp, to_server, to_client);
    return MOQ_OK;
}

/* -- Datagrams ------------------------------------------------------------- */

moq_result_t sim_datagram_input(
    moq_simpair_t *sp, moq_session_t *to, const uint8_t *data, size_t len,
    moq_perspective_t from, moq_perspective_t to_persp)
{
    moq_bytes_t bytes = { data, len };
    moq_result_t rc = moq_session_on_datagram(to, data, len, sp->now_us);
    trace_input(sp, MOQ_SIM_INPUT_DATAGRAM, from, to_persp, bytes, rc,
                SIM_INPUT_DETAIL_NONE);
    /* Datagrams are unreliable: a refused one is dropped, as the bridge
     * drops it (on_peer_datagram), and nothing queued behind it is touched. */
    if (rc == MOQ_ERR_WOULD_BLOCK) return MOQ_OK;
    return rc;
}

/* -- Resume ---------------------------------------------------------------- */

moq_result_t sim_resume_held(moq_simpair_t *sp,
                             size_t *to_server, size_t *to_client)
{
    moq_result_t rc;
    /* Control first: on a unidirectional pair it gates the other inputs. */
    rc = resume_control(sp, MOQ_PERSPECTIVE_CLIENT, to_server, to_client);
    if (rc < 0) return rc;
    rc = resume_control(sp, MOQ_PERSPECTIVE_SERVER, to_server, to_client);
    if (rc < 0) return rc;
    for (int i = 0; i < MOQ_SIM_MAX_DATA_STREAMS; i++) {
        moq_sim_stream_map_t *m = &sp->stream_map[i];
        if (!m->active || (!inbound_holding(&m->in) && !m->in.stop_owed))
            continue;
        rc = resume_data(sp, i, to_server, to_client);
        if (rc < 0) return rc;
    }
    for (int b = 0; b < MOQ_SIM_MAX_BIDI_STREAMS; b++) {
        for (int k = 0; k < 2; k++) {
            moq_sim_bidi_map_t *m = &sp->bidi_map[b];
            sim_inbound_t *in = &m->in[k];
            if (!m->active || (!inbound_holding(in) && !in->stop_owed))
                continue;
            rc = resume_bidi_half(sp, b,
                                  k == 0 ? MOQ_PERSPECTIVE_CLIENT
                                         : MOQ_PERSPECTIVE_SERVER,
                                  to_server, to_client);
            if (rc < 0) return rc;
        }
    }
    return MOQ_OK;
}
