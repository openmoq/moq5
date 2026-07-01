#include "simpair_internal.h"

sim_delay_entry_t *sim_delay_alloc(moq_simpair_t *sp,
                                   sim_delay_kind_t kind,
                                   const uint8_t *data, size_t len,
                                   uint64_t due_us)
{
    size_t total = sizeof(sim_delay_entry_t) + len;
    sim_delay_entry_t *e = (sim_delay_entry_t *)sp->alloc.alloc(
        total, sp->alloc.ctx);
    if (!e) return NULL;
    memset(e, 0, sizeof(*e));
    e->alloc_size = total;
    e->kind = kind;
    e->due_us = due_us;
    e->seq = sp->delay_seq++;
    e->len = len;
    e->bidi_slot = -1;
    e->data_slot = -1;
    if (len > 0 && data)
        memcpy(e->bytes, data, len);
    return e;
}

void sim_delay_enqueue(moq_simpair_t *sp, sim_delay_entry_t *e)
{
    sim_delay_entry_t **pp = &sp->delay_head;
    while (*pp) {
        if (e->due_us < (*pp)->due_us ||
            (e->due_us == (*pp)->due_us && e->seq < (*pp)->seq))
            break;
        pp = &(*pp)->next;
    }
    e->next = *pp;
    *pp = e;
}

void sim_delay_free_entry(moq_simpair_t *sp, sim_delay_entry_t *e)
{
    sp->alloc.free(e, e->alloc_size, sp->alloc.ctx);
}

void sim_delay_clear(moq_simpair_t *sp)
{
    sim_delay_entry_t *e = sp->delay_head;
    while (e) {
        sim_delay_entry_t *next = e->next;
        sim_delay_free_entry(sp, e);
        e = next;
    }
    sp->delay_head = NULL;
}

void trace_delay_enqueue(moq_simpair_t *sp,
                         moq_perspective_t from,
                         moq_perspective_t to,
                         sim_delay_kind_t kind,
                         size_t len, uint64_t code,
                         uint64_t due_us, uint64_t seq,
                         bool fifo_forced)
{
    moq_sim_trace_record_t r;
    memset(&r, 0, sizeof(r));
    r.struct_size = sizeof(r);
    r.kind = MOQ_SIM_TRACE_DELAY_ENQUEUE;
    r.seed = sp->seed;
    r.step = sp->step;
    r.now_us = sp->now_us;
    r.from = from;
    r.to = to;
    switch (kind) {
    case SIM_DELAY_CONTROL_BYTES: r.input_kind = MOQ_SIM_INPUT_CONTROL_BYTES; break;
    case SIM_DELAY_BIDI_BYTES:    r.input_kind = MOQ_SIM_INPUT_BIDI_BYTES;    break;
    case SIM_DELAY_BIDI_RESET:    r.input_kind = MOQ_SIM_INPUT_BIDI_RESET;    break;
    case SIM_DELAY_DATA_BYTES:    r.input_kind = MOQ_SIM_INPUT_DATA_BYTES;    break;
    case SIM_DELAY_DATA_RESET:    r.input_kind = MOQ_SIM_INPUT_DATA_RESET;    break;
    case SIM_DELAY_DATA_STOP:     r.input_kind = MOQ_SIM_INPUT_DATA_STOP;     break;
    case SIM_DELAY_BIDI_STOP:     r.input_kind = MOQ_SIM_INPUT_BIDI_STOP;     break;
    }
    r.count = len;
    r.code = due_us;
    r.result = fifo_forced ? 1 : 0;
    trace_record(sp, &r);
    (void)code; (void)seq;
}

void trace_delay_stale(moq_simpair_t *sp,
                       const sim_delay_entry_t *e)
{
    moq_sim_trace_record_t r;
    memset(&r, 0, sizeof(r));
    r.struct_size = sizeof(r);
    r.kind = MOQ_SIM_TRACE_DELAY_STALE;
    r.seed = sp->seed;
    r.step = sp->step;
    r.now_us = sp->now_us;
    r.from = e->from;
    r.to = e->to;
    switch (e->kind) {
    case SIM_DELAY_CONTROL_BYTES: r.input_kind = MOQ_SIM_INPUT_CONTROL_BYTES; break;
    case SIM_DELAY_BIDI_BYTES:    r.input_kind = MOQ_SIM_INPUT_BIDI_BYTES;    break;
    case SIM_DELAY_BIDI_RESET:    r.input_kind = MOQ_SIM_INPUT_BIDI_RESET;    break;
    case SIM_DELAY_DATA_BYTES:    r.input_kind = MOQ_SIM_INPUT_DATA_BYTES;    break;
    case SIM_DELAY_DATA_RESET:    r.input_kind = MOQ_SIM_INPUT_DATA_RESET;    break;
    case SIM_DELAY_DATA_STOP:     r.input_kind = MOQ_SIM_INPUT_DATA_STOP;     break;
    case SIM_DELAY_BIDI_STOP:     r.input_kind = MOQ_SIM_INPUT_BIDI_STOP;     break;
    }
    r.count = e->len;
    bool is_data_kind = e->kind == SIM_DELAY_DATA_BYTES ||
                        e->kind == SIM_DELAY_DATA_RESET ||
                        e->kind == SIM_DELAY_DATA_STOP;
    r.code = is_data_kind ? e->data_generation : e->bidi_generation;
    trace_record(sp, &r);
}

moq_result_t deliver_or_delay_control_chunk(
    moq_simpair_t *sp, moq_session_t *to,
    const uint8_t *data, size_t len,
    moq_perspective_t from, moq_perspective_t to_persp)
{
    size_t didx = (size_t)sp->delay_decision_seq++;
    int tidx = (to_persp == MOQ_PERSPECTIVE_CLIENT) ? 0 : 1;
    bool fifo_forced = sp->last_control_due_us[tidx] > sp->now_us;
    if ((fifo_forced || sim_delay_fires(sp, didx)) && len > 0) {
        uint64_t due = sim_delay_compute_due(sp, didx);
        int tidx = (to_persp == MOQ_PERSPECTIVE_CLIENT) ? 0 : 1;
        if (due < sp->last_control_due_us[tidx])
            due = sp->last_control_due_us[tidx];
        sp->last_control_due_us[tidx] = due;
        sim_delay_entry_t *de = sim_delay_alloc(
            sp, SIM_DELAY_CONTROL_BYTES, data, len, due);
        if (!de) return MOQ_ERR_NOMEM;
        de->from = from;
        de->to = to_persp;
        trace_delay_enqueue(sp, from, to_persp,
            SIM_DELAY_CONTROL_BYTES, len, 0, due, de->seq,
            fifo_forced);
        sim_delay_enqueue(sp, de);
        return MOQ_OK;
    }
    moq_bytes_t bytes = { data, len };
    moq_result_t rc = moq_session_on_control_bytes(to, data, len, sp->now_us);
    trace_input(sp, MOQ_SIM_INPUT_CONTROL_BYTES, from, to_persp, bytes, rc);
    return rc;
}

moq_result_t deliver_or_delay_control(
    moq_simpair_t *sp, moq_session_t *to,
    const uint8_t *data, size_t len, size_t action_index,
    moq_perspective_t from, moq_perspective_t to_persp)
{
    if (sim_split_control_fires(sp, action_index) && len > 1) {
        uint64_t chunk_rng = sim_mix64(
            sp->seed ^ (sp->step * 0x9E3779B97F4A7C15ULL) ^
            ((uint64_t)action_index * 0xD6E8FEB86659FD93ULL) ^
            0xA1B2C3D4E5F60718ULL);
        size_t off = 0;
        while (off < len) {
            chunk_rng = sim_mix64(chunk_rng);
            size_t chunk = (chunk_rng % 8) + 1;
            if (chunk > len - off) chunk = len - off;
            moq_result_t rc = deliver_or_delay_control_chunk(
                sp, to, data + off, chunk, from, to_persp);
            if (rc < 0) return rc;
            off += chunk;
        }
        return MOQ_OK;
    }
    return deliver_or_delay_control_chunk(sp, to, data, len, from, to_persp);
}

/* Record a FIN on the sender's half of a bidi and retire the slot once both
 * halves are closed. A bidi has two independent send halves, so a single FIN
 * (e.g. a responder's REQUEST_ERROR closing the response half) must not retire
 * the slot while the opener's half is still open. */
static void sim_bidi_mark_fin(moq_simpair_t *sp, int bslot,
                              moq_perspective_t from)
{
    if (bslot < 0 || bslot >= MOQ_SIM_MAX_BIDI_STREAMS) return;
    moq_sim_bidi_map_t *m = &sp->bidi_map[bslot];
    if (from == m->opener) m->opener_fin = true;
    else                   m->responder_fin = true;
    if (m->opener_fin && m->responder_fin) {
        m->active = false;
        m->generation++;
    }
}

moq_result_t deliver_or_delay_bidi_chunk(
    moq_simpair_t *sp, moq_session_t *to, moq_stream_ref_t ref,
    const uint8_t *data, size_t len, bool fin,
    int bslot, moq_perspective_t from, moq_perspective_t to_persp)
{
    size_t didx = (size_t)sp->delay_decision_seq++;
    bool fifo_forced = bslot >= 0 &&
        sp->bidi_map[bslot].last_due_us > sp->now_us;
    if ((fifo_forced || sim_delay_fires(sp, didx)) && (len > 0 || fin)) {
        uint64_t due = sim_delay_compute_due(sp, didx);
        if (bslot >= 0) {
            if (due < sp->bidi_map[bslot].last_due_us)
                due = sp->bidi_map[bslot].last_due_us;
            sp->bidi_map[bslot].last_due_us = due;
        }
        sim_delay_entry_t *de = sim_delay_alloc(
            sp, SIM_DELAY_BIDI_BYTES, data, len, due);
        if (!de) return MOQ_ERR_NOMEM;
        de->from = from;
        de->to = to_persp;
        de->ref = ref;
        de->fin = fin;
        if (bslot >= 0) {
            de->bidi_slot = bslot;
            de->bidi_generation = sp->bidi_map[bslot].generation;
        }
        trace_delay_enqueue(sp, from, to_persp,
            SIM_DELAY_BIDI_BYTES, len, 0, due, de->seq,
            fifo_forced);
        sim_delay_enqueue(sp, de);
        return MOQ_OK;
    }
    moq_bytes_t bytes = { data, len };
    moq_result_t rc = moq_session_on_bidi_stream_bytes(
        to, ref, data, len, fin, sp->now_us);
    trace_input(sp, MOQ_SIM_INPUT_BIDI_BYTES, from, to_persp, bytes, rc);
    /* A FIN closes the sender's half; the slot retires once both halves close. */
    if (rc >= 0 && fin && bslot >= 0)
        sim_bidi_mark_fin(sp, bslot, from);
    return rc;
}

moq_result_t deliver_or_delay_bidi(
    moq_simpair_t *sp, moq_session_t *to, moq_stream_ref_t ref,
    const uint8_t *data, size_t len, bool fin,
    size_t action_index, int bslot,
    moq_perspective_t from, moq_perspective_t to_persp)
{
    if (len == 0 && fin)
        return deliver_or_delay_bidi_chunk(
            sp, to, ref, NULL, 0, true, bslot, from, to_persp);

    if (sim_split_bidi_fires(sp, action_index) && len > 1) {
        uint64_t chunk_rng = sim_mix64(
            sp->seed ^ (sp->step * 0x9E3779B97F4A7C15ULL) ^
            ((uint64_t)action_index * 0xD6E8FEB86659FD93ULL) ^
            0xF0E1D2C3B4A59687ULL);
        size_t off = 0;
        while (off < len) {
            chunk_rng = sim_mix64(chunk_rng);
            size_t chunk = (chunk_rng % 8) + 1;
            if (chunk > len - off) chunk = len - off;
            bool chunk_fin = fin && (off + chunk >= len);
            moq_result_t rc = deliver_or_delay_bidi_chunk(
                sp, to, ref, data + off, chunk, chunk_fin,
                bslot, from, to_persp);
            if (rc < 0) return rc;
            off += chunk;
        }
        return MOQ_OK;
    }
    return deliver_or_delay_bidi_chunk(
        sp, to, ref, data, len, fin, bslot, from, to_persp);
}

moq_result_t deliver_or_delay_bidi_reset(
    moq_simpair_t *sp, moq_session_t *to, moq_stream_ref_t ref,
    uint64_t error_code, int bslot,
    moq_perspective_t from, moq_perspective_t to_persp)
{
    size_t didx = (size_t)sp->delay_decision_seq++;
    bool fifo_forced = bslot >= 0 &&
        sp->bidi_map[bslot].last_due_us > sp->now_us;
    if (fifo_forced || sim_delay_fires(sp, didx)) {
        uint64_t due = sim_delay_compute_due(sp, didx);
        if (bslot >= 0) {
            if (due < sp->bidi_map[bslot].last_due_us)
                due = sp->bidi_map[bslot].last_due_us;
            sp->bidi_map[bslot].last_due_us = due;
        }
        sim_delay_entry_t *de = sim_delay_alloc(
            sp, SIM_DELAY_BIDI_RESET, NULL, 0, due);
        if (!de) return MOQ_ERR_NOMEM;
        de->from = from;
        de->to = to_persp;
        de->ref = ref;
        de->error_code = error_code;
        if (bslot >= 0) {
            de->bidi_slot = bslot;
            de->bidi_generation = sp->bidi_map[bslot].generation;
        }
        trace_delay_enqueue(sp, from, to_persp,
            SIM_DELAY_BIDI_RESET, 0, error_code, due, de->seq,
            fifo_forced);
        sim_delay_enqueue(sp, de);
        return MOQ_OK;
    }
    moq_result_t rc = moq_session_on_bidi_stream_reset(
        to, ref, error_code, sp->now_us);
    trace_input(sp, MOQ_SIM_INPUT_BIDI_RESET, from, to_persp,
                (moq_bytes_t){0}, rc);
    if (rc >= 0 && bslot >= 0) {
        sp->bidi_map[bslot].active = false;
        sp->bidi_map[bslot].generation++;
    }
    return rc;
}

moq_result_t deliver_or_delay_bidi_stop(
    moq_simpair_t *sp, moq_session_t *to, moq_stream_ref_t ref,
    uint64_t error_code, int bslot,
    moq_perspective_t from, moq_perspective_t to_persp)
{
    size_t didx = (size_t)sp->delay_decision_seq++;
    bool fifo_forced = bslot >= 0 &&
        sp->bidi_map[bslot].last_due_us > sp->now_us;
    if (fifo_forced || sim_delay_fires(sp, didx)) {
        uint64_t due = sim_delay_compute_due(sp, didx);
        if (bslot >= 0) {
            if (due < sp->bidi_map[bslot].last_due_us)
                due = sp->bidi_map[bslot].last_due_us;
            sp->bidi_map[bslot].last_due_us = due;
        }
        sim_delay_entry_t *de = sim_delay_alloc(
            sp, SIM_DELAY_BIDI_STOP, NULL, 0, due);
        if (!de) return MOQ_ERR_NOMEM;
        de->from = from;
        de->to = to_persp;
        de->ref = ref;
        de->error_code = error_code;
        if (bslot >= 0) {
            de->bidi_slot = bslot;
            de->bidi_generation = sp->bidi_map[bslot].generation;
        }
        trace_delay_enqueue(sp, from, to_persp,
            SIM_DELAY_BIDI_STOP, 0, error_code, due, de->seq,
            fifo_forced);
        sim_delay_enqueue(sp, de);
        return MOQ_OK;
    }
    /* STOP_SENDING does not retire the bidi: the peer may follow it with a
     * RESET_STREAM that still needs the mapping (mirrors data-stop). Retire
     * only on failure. */
    moq_result_t rc = moq_session_on_bidi_stream_stop(
        to, ref, error_code, sp->now_us);
    trace_input(sp, MOQ_SIM_INPUT_BIDI_STOP, from, to_persp,
                (moq_bytes_t){0}, rc);
    if (rc < 0 && bslot >= 0) {
        sp->bidi_map[bslot].active = false;
        sp->bidi_map[bslot].generation++;
    }
    return rc;
}

/* -- Data stream delay helpers -------------------------------------- */

moq_result_t deliver_or_delay_data_chunk(
    moq_simpair_t *sp, moq_session_t *to, moq_stream_ref_t ref,
    const uint8_t *data, size_t len, bool fin,
    int dslot, moq_perspective_t from, moq_perspective_t to_persp)
{
    size_t didx = (size_t)sp->delay_decision_seq++;
    bool fifo_forced = dslot >= 0 &&
        sp->stream_map[dslot].last_due_us > sp->now_us;
    if ((fifo_forced || sim_delay_fires(sp, didx)) && (len > 0 || fin)) {
        uint64_t due = sim_delay_compute_due(sp, didx);
        if (dslot >= 0) {
            if (due < sp->stream_map[dslot].last_due_us)
                due = sp->stream_map[dslot].last_due_us;
            sp->stream_map[dslot].last_due_us = due;
        }
        sim_delay_entry_t *de = sim_delay_alloc(
            sp, SIM_DELAY_DATA_BYTES, data, len, due);
        if (!de) return MOQ_ERR_NOMEM;
        de->from = from;
        de->to = to_persp;
        de->ref = ref;
        de->fin = fin;
        if (dslot >= 0) {
            de->data_slot = dslot;
            de->data_generation = sp->stream_map[dslot].generation;
        }
        trace_delay_enqueue(sp, from, to_persp,
            SIM_DELAY_DATA_BYTES, len, 0, due, de->seq,
            fifo_forced);
        sim_delay_enqueue(sp, de);
        if (fin && dslot >= 0)
            sp->stream_map[dslot].terminal_pending = true;
        return MOQ_OK;
    }
    moq_bytes_t bytes = { data, len };
    moq_result_t rc = moq_session_on_data_bytes(
        to, ref, data, len, fin, sp->now_us);
    trace_input(sp, MOQ_SIM_INPUT_DATA_BYTES, from, to_persp, bytes, rc);
    if (rc >= 0 && fin && dslot >= 0) {
        sp->stream_map[dslot].active = false;
        sp->stream_map[dslot].generation++;
    }
    return rc;
}

moq_result_t deliver_or_delay_data(
    moq_simpair_t *sp, moq_session_t *to, moq_stream_ref_t ref,
    const uint8_t *hdr, size_t hlen,
    const uint8_t *payload, size_t plen,
    bool fin, size_t action_index, int dslot,
    moq_perspective_t from, moq_perspective_t to_persp)
{
    if (plen > SIZE_MAX - hlen) return MOQ_ERR_INTERNAL;
    size_t total = hlen + plen;
    if (total == 0 && fin)
        return deliver_or_delay_data_chunk(
            sp, to, ref, NULL, 0, true, dslot, from, to_persp);

    if (sim_split_fires(sp, action_index) && total > 1) {
        uint64_t chunk_rng = sim_mix64(
            sp->seed ^ (sp->step * 0x9E3779B97F4A7C15ULL) ^
            ((uint64_t)action_index * 0xD6E8FEB86659FD93ULL));
        size_t off = 0;
        while (off < total) {
            chunk_rng = sim_mix64(chunk_rng);
            size_t chunk = (chunk_rng % 8) + 1;
            if (chunk > total - off) chunk = total - off;
            bool chunk_fin = fin && (off + chunk >= total);

            size_t done = 0;
            while (done < chunk) {
                size_t pos = off + done;
                const uint8_t *src;
                size_t piece;
                if (pos < hlen) {
                    src = hdr + pos;
                    size_t avail = hlen - pos;
                    piece = chunk - done < avail ? chunk - done : avail;
                } else {
                    src = payload + (pos - hlen);
                    piece = chunk - done;
                }
                bool piece_fin = chunk_fin && (done + piece >= chunk);
                moq_result_t rc = deliver_or_delay_data_chunk(
                    sp, to, ref, src, piece, piece_fin,
                    dslot, from, to_persp);
                if (rc < 0) return rc;
                done += piece;
            }
            off += chunk;
        }
        return MOQ_OK;
    }

    /* No split: deliver header then payload separately. */
    moq_result_t rc = MOQ_OK;
    if (hlen > 0) {
        bool hdr_fin = fin && plen == 0;
        rc = deliver_or_delay_data_chunk(sp, to, ref, hdr, hlen,
            hdr_fin, dslot, from, to_persp);
        if (rc < 0) return rc;
    }
    if (plen > 0) {
        rc = deliver_or_delay_data_chunk(sp, to, ref, payload, plen,
            fin, dslot, from, to_persp);
        if (rc < 0) return rc;
    }
    if (hlen == 0 && plen == 0 && fin) {
        rc = deliver_or_delay_data_chunk(sp, to, ref, NULL, 0, true,
            dslot, from, to_persp);
    }
    return rc;
}

moq_result_t deliver_or_delay_data_reset(
    moq_simpair_t *sp, moq_session_t *to, moq_stream_ref_t ref,
    uint64_t error_code, int dslot,
    moq_perspective_t from, moq_perspective_t to_persp)
{
    size_t didx = (size_t)sp->delay_decision_seq++;
    bool fifo_forced = dslot >= 0 &&
        sp->stream_map[dslot].last_due_us > sp->now_us;
    if (fifo_forced || sim_delay_fires(sp, didx)) {
        uint64_t due = sim_delay_compute_due(sp, didx);
        if (dslot >= 0) {
            if (due < sp->stream_map[dslot].last_due_us)
                due = sp->stream_map[dslot].last_due_us;
            sp->stream_map[dslot].last_due_us = due;
        }
        sim_delay_entry_t *de = sim_delay_alloc(
            sp, SIM_DELAY_DATA_RESET, NULL, 0, due);
        if (!de) return MOQ_ERR_NOMEM;
        de->from = from;
        de->to = to_persp;
        de->ref = ref;
        de->error_code = error_code;
        if (dslot >= 0) {
            de->data_slot = dslot;
            de->data_generation = sp->stream_map[dslot].generation;
        }
        trace_delay_enqueue(sp, from, to_persp,
            SIM_DELAY_DATA_RESET, 0, error_code, due, de->seq,
            fifo_forced);
        sim_delay_enqueue(sp, de);
        if (dslot >= 0)
            sp->stream_map[dslot].terminal_pending = true;
        return MOQ_OK;
    }
    moq_result_t rc = moq_session_on_data_reset(
        to, ref, error_code, sp->now_us);
    trace_input(sp, MOQ_SIM_INPUT_DATA_RESET, from, to_persp,
                (moq_bytes_t){0}, rc);
    if (rc >= 0 && dslot >= 0) {
        sp->stream_map[dslot].active = false;
        sp->stream_map[dslot].generation++;
    }
    return rc;
}

moq_result_t deliver_or_delay_data_stop(
    moq_simpair_t *sp, moq_session_t *sender_session,
    moq_stream_ref_t sender_ref, uint64_t error_code,
    int dslot, moq_perspective_t from, moq_perspective_t sender_persp)
{
    size_t didx = (size_t)sp->delay_decision_seq++;
    bool fifo_forced = dslot >= 0 &&
        sp->stream_map[dslot].last_due_us > sp->now_us;
    if (fifo_forced || sim_delay_fires(sp, didx)) {
        uint64_t due = sim_delay_compute_due(sp, didx);
        if (dslot >= 0) {
            if (due < sp->stream_map[dslot].last_due_us)
                due = sp->stream_map[dslot].last_due_us;
            sp->stream_map[dslot].last_due_us = due;
        }
        sim_delay_entry_t *de = sim_delay_alloc(
            sp, SIM_DELAY_DATA_STOP, NULL, 0, due);
        if (!de) return MOQ_ERR_NOMEM;
        de->from = from;
        de->to = sender_persp;
        de->ref = sender_ref;
        de->error_code = error_code;
        if (dslot >= 0) {
            de->data_slot = dslot;
            de->data_generation = sp->stream_map[dslot].generation;
        }
        trace_delay_enqueue(sp, from, sender_persp,
            SIM_DELAY_DATA_STOP, 0, error_code, due, de->seq,
            fifo_forced);
        sim_delay_enqueue(sp, de);
        if (dslot >= 0)
            sp->stream_map[dslot].terminal_pending = true;
        return MOQ_OK;
    }
    /* Retire only on failure. Successful STOP lets the sender
     * queue a follow-up RESET_DATA action, which routes back to
     * the receiver and retires the map through the normal reset
     * path. Retiring here would remove the mapping before the
     * follow-up RESET can find it. */
    moq_result_t rc = moq_session_on_data_stop(
        sender_session, sender_ref, error_code, sp->now_us);
    trace_input(sp, MOQ_SIM_INPUT_DATA_STOP, from, sender_persp,
                (moq_bytes_t){0}, rc);
    if (rc < 0 && dslot >= 0) {
        sp->stream_map[dslot].active = false;
        sp->stream_map[dslot].generation++;
    }
    return rc;
}

moq_result_t sim_delay_deliver_matured(moq_simpair_t *sp,
                                       size_t *delivered)
{
    while (sp->delay_head && sp->delay_head->due_us <= sp->now_us) {
        sim_delay_entry_t *e = sp->delay_head;
        sp->delay_head = e->next;

        moq_session_t *target =
            (e->to == MOQ_PERSPECTIVE_CLIENT) ? sp->client : sp->server;

        if (e->kind == SIM_DELAY_BIDI_BYTES || e->kind == SIM_DELAY_BIDI_RESET ||
            e->kind == SIM_DELAY_BIDI_STOP) {
            if (e->bidi_slot >= 0 && e->bidi_slot < MOQ_SIM_MAX_BIDI_STREAMS &&
                sp->bidi_map[e->bidi_slot].generation != e->bidi_generation) {
                trace_delay_stale(sp, e);
                sim_delay_free_entry(sp, e);
                (*delivered)++;
                continue;
            }
        }

        if (e->kind == SIM_DELAY_DATA_BYTES || e->kind == SIM_DELAY_DATA_RESET ||
            e->kind == SIM_DELAY_DATA_STOP) {
            if (e->data_slot >= 0 && e->data_slot < MOQ_SIM_MAX_DATA_STREAMS &&
                sp->stream_map[e->data_slot].generation != e->data_generation) {
                trace_delay_stale(sp, e);
                sim_delay_free_entry(sp, e);
                (*delivered)++;
                continue;
            }
        }

        moq_result_t rc = MOQ_OK;
        switch (e->kind) {
        case SIM_DELAY_CONTROL_BYTES:
            rc = moq_session_on_control_bytes(target, e->bytes, e->len,
                                               sp->now_us);
            trace_input(sp, MOQ_SIM_INPUT_CONTROL_BYTES, e->from, e->to,
                        (moq_bytes_t){ e->bytes, e->len }, rc);
            break;
        case SIM_DELAY_BIDI_BYTES:
            rc = moq_session_on_bidi_stream_bytes(target, e->ref,
                e->bytes, e->len, e->fin, sp->now_us);
            trace_input(sp, MOQ_SIM_INPUT_BIDI_BYTES, e->from, e->to,
                        (moq_bytes_t){ e->bytes, e->len }, rc);
            if (rc >= 0 && e->fin && e->bidi_slot >= 0 &&
                e->bidi_slot < MOQ_SIM_MAX_BIDI_STREAMS &&
                sp->bidi_map[e->bidi_slot].generation == e->bidi_generation)
                sim_bidi_mark_fin(sp, e->bidi_slot, e->from);
            break;
        case SIM_DELAY_BIDI_RESET:
            rc = moq_session_on_bidi_stream_reset(target, e->ref,
                e->error_code, sp->now_us);
            trace_input(sp, MOQ_SIM_INPUT_BIDI_RESET, e->from, e->to,
                        (moq_bytes_t){0}, rc);
            if (rc >= 0 && e->bidi_slot >= 0 &&
                e->bidi_slot < MOQ_SIM_MAX_BIDI_STREAMS &&
                sp->bidi_map[e->bidi_slot].generation == e->bidi_generation) {
                sp->bidi_map[e->bidi_slot].active = false;
                sp->bidi_map[e->bidi_slot].generation++;
            }
            break;
        case SIM_DELAY_DATA_BYTES:
            rc = moq_session_on_data_bytes(target, e->ref,
                e->bytes, e->len, e->fin, sp->now_us);
            trace_input(sp, MOQ_SIM_INPUT_DATA_BYTES, e->from, e->to,
                        (moq_bytes_t){ e->bytes, e->len }, rc);
            if (rc >= 0 && e->fin && e->data_slot >= 0 &&
                e->data_slot < MOQ_SIM_MAX_DATA_STREAMS &&
                sp->stream_map[e->data_slot].generation == e->data_generation) {
                sp->stream_map[e->data_slot].active = false;
                sp->stream_map[e->data_slot].generation++;
            }
            break;
        case SIM_DELAY_DATA_RESET:
            rc = moq_session_on_data_reset(target, e->ref,
                e->error_code, sp->now_us);
            trace_input(sp, MOQ_SIM_INPUT_DATA_RESET, e->from, e->to,
                        (moq_bytes_t){0}, rc);
            if (rc >= 0 && e->data_slot >= 0 &&
                e->data_slot < MOQ_SIM_MAX_DATA_STREAMS &&
                sp->stream_map[e->data_slot].generation == e->data_generation) {
                sp->stream_map[e->data_slot].active = false;
                sp->stream_map[e->data_slot].generation++;
            }
            break;
        case SIM_DELAY_DATA_STOP:
            /* Retire only on failure — see comment in
             * deliver_or_delay_data_stop for rationale. */
            rc = moq_session_on_data_stop(target, e->ref,
                e->error_code, sp->now_us);
            trace_input(sp, MOQ_SIM_INPUT_DATA_STOP, e->from, e->to,
                        (moq_bytes_t){0}, rc);
            if (rc < 0 && e->data_slot >= 0 &&
                e->data_slot < MOQ_SIM_MAX_DATA_STREAMS &&
                sp->stream_map[e->data_slot].generation == e->data_generation) {
                sp->stream_map[e->data_slot].active = false;
                sp->stream_map[e->data_slot].generation++;
            }
            break;
        case SIM_DELAY_BIDI_STOP:
            /* Retire only on failure — a peer RESET may still follow. */
            rc = moq_session_on_bidi_stream_stop(target, e->ref,
                e->error_code, sp->now_us);
            trace_input(sp, MOQ_SIM_INPUT_BIDI_STOP, e->from, e->to,
                        (moq_bytes_t){0}, rc);
            if (rc < 0 && e->bidi_slot >= 0 &&
                e->bidi_slot < MOQ_SIM_MAX_BIDI_STREAMS &&
                sp->bidi_map[e->bidi_slot].generation == e->bidi_generation) {
                sp->bidi_map[e->bidi_slot].active = false;
                sp->bidi_map[e->bidi_slot].generation++;
            }
            break;
        }

        (*delivered)++;
        sim_delay_free_entry(sp, e);
        if (rc < 0) return rc;
    }
    return MOQ_OK;
}
