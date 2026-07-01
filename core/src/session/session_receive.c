#include "session_internal.h"
#include "../base/rcbuf_internal.h"

/* -- Receive data stream pool -------------------------------------- */

static int rx_find_by_ref(moq_session_t *s, moq_stream_ref_t ref)
{
    return moq_index_find(s->idx_rx_by_ref, s->idx_rx_mask, ref._v);
}

static int rx_find_free(moq_session_t *s)
{
    for (size_t i = 0; i < s->rx_cap; i++)
        if (!s->rx_streams[i].active) return (int)i;
    return -1;
}

static void rx_free_entry(moq_session_t *s, size_t slot)
{
    moq_rx_stream_t *rx = &s->rx_streams[slot];
    moq_index_remove(s->idx_rx_by_ref, s->idx_rx_mask, rx->stream_ref._v);
    if (rx->payload_rcbuf) {
        /* An object assembled but not yet emitted: the payload lives inside
         * this rcbuf (payload_buf points into it), so decref rather than free
         * the cursor. */
        moq_rcbuf_decref(rx->payload_rcbuf);
        if ((size_t)rx->payload_expected <= s->recv_payload_bytes)
            s->recv_payload_bytes -= (size_t)rx->payload_expected;
        else
            s->recv_payload_bytes = 0;
        rx->payload_rcbuf = NULL;
        rx->payload_buf = NULL;
    }
    if (rx->cur_extensions) {
        size_t ext_len = moq_rcbuf_len(rx->cur_extensions);
        if (ext_len <= s->recv_payload_bytes)
            s->recv_payload_bytes -= ext_len;
        else
            s->recv_payload_bytes = 0;
        moq_rcbuf_decref(rx->cur_extensions);
        rx->cur_extensions = NULL;
    }
    if (rx->input_buf) {
        if (rx->input_cap <= s->recv_input_bytes)
            s->recv_input_bytes -= rx->input_cap;
        else
            s->recv_input_bytes = 0;
        s->alloc.free(rx->input_buf, rx->input_cap, s->alloc.ctx);
        rx->input_buf = NULL;
    }
    if (rx->pending_chunk) {
        moq_rcbuf_decref(rx->pending_chunk);
        rx->pending_chunk = NULL;
    }
    memset(rx, 0, sizeof(*rx));
}

static void rx_record_finished(moq_session_t *s, uint64_t ref_v)
{
    /* A FIN'd data stream bound to a subscriber-role publication counts toward
     * that publication's PUBLISH_DONE Stream Count (rx_record_finished is the
     * single FIN signal -- never called on a STOP). The rx is still live here
     * (freed by the caller right after). Fetch / subscription streams carry an
     * invalid pub_handle, so this is a no-op for them. */
    int rxslot = rx_find_by_ref(s, moq_stream_ref_from_u64(ref_v));
    if (rxslot >= 0 &&
        moq_publication_is_valid(s->rx_streams[rxslot].pub_handle))
        pub_note_stream_processed(s, s->rx_streams[rxslot].pub_handle);

    size_t idx = (s->rx_fin_head + s->rx_fin_count) % s->rx_fin_cap;
    if (s->rx_fin_count >= s->rx_fin_cap)
        s->rx_fin_head = (s->rx_fin_head + 1) % s->rx_fin_cap;
    else
        s->rx_fin_count++;
    s->rx_finished[idx] = ref_v;
}

static bool rx_is_finished(moq_session_t *s, uint64_t ref_v)
{
    for (size_t i = 0; i < s->rx_fin_count; i++) {
        size_t idx = (s->rx_fin_head + i) % s->rx_fin_cap;
        if (s->rx_finished[idx] == ref_v) return true;
    }
    return false;
}

/* -- Receive helpers ----------------------------------------------- */

static moq_result_t rx_try_stop(moq_session_t *s, int slot)
{
    moq_rx_stream_t *rx = &s->rx_streams[slot];
    if (action_queue_full(s)) {
        rx->parse_state = MOQ_RX_NEED_STOP;
        return MOQ_ERR_WOULD_BLOCK;
    }
    moq_action_t a;
    memset(&a, 0, sizeof(a));
    a.kind = MOQ_ACTION_STOP_DATA;
    a.detail_size = (uint32_t)sizeof(moq_stop_data_action_t);
    a.borrow_epoch = s->borrow_epoch;
    a.u.stop_data.stream_ref = rx->stream_ref;
    a.u.stop_data.error_code = 0;
    moq_result_t rc = push_action(s, &a);
    if (rc < 0) {
        rx->parse_state = MOQ_RX_NEED_STOP;
        return rc;
    }
    /* The STOP is now queued: if this stream was the late response to a
     * locally-cancelled fetch, release its cancel tombstone. Doing it here (not
     * at FETCH_HEADER decode) means a deferred/retried stop still consumes it. */
    if (rx->stop_consumes_cancel_tomb)
        fetch_cancel_tomb_consume(s, rx->cancel_tomb_request_id);
    rx_free_entry(s, (size_t)slot);
    return MOQ_OK;
}

/* A data stream is bound (at its header) to a subscription or publication. That
 * binding can be freed mid-stream -- e.g. the app rejects a still-pending PUBLISH
 * after early objects (§9.4) already bound this stream. Re-resolve the bound handle
 * so we never emit an object/chunk against a stale handle; an unbound stream (no
 * handle yet) and a FETCH stream (resolved separately) are always "alive" here. */
static bool rx_binding_alive(moq_session_t *s, const moq_rx_stream_t *rx)
{
    if (moq_subscription_is_valid(rx->sub))
        return sub_resolve_handle(s, rx->sub) >= 0;
    if (moq_publication_is_valid(rx->pub_handle))
        return pub_resolve_handle(s, rx->pub_handle) >= 0;
    return true;
}

/* -- Streaming chunk emit ------------------------------------------
 *
 * Ownership during PENDING_CHUNK retry:
 *   - Chunk payload bytes are owned by rx->pending_chunk (rcbuf) while
 *     retrying. Freed by rx_free_entry if the stream is torn down.
 *   - Begin-object extensions remain in rx->cur_extensions until
 *     push_event succeeds. On successful begin-event push, ownership
 *     transfers to the queued event and cur_extensions is set NULL.
 */

static moq_result_t rx_push_pending_chunk(moq_session_t *s, int slot)
{
    moq_rx_stream_t *rx = &s->rx_streams[slot];

    /* Re-allocate chunk if a prior attempt got NOMEM. */
    if (!rx->pending_chunk && rx->pending_data_len > 0) {
        moq_result_t rc = moq_rcbuf_create(&s->alloc,
            rx->input_buf, rx->pending_data_len, &rx->pending_chunk);
        if (rc < 0) return rc;
    }

    if (event_queue_full(s))
        return MOQ_ERR_WOULD_BLOCK;

    moq_event_t e;
    memset(&e, 0, sizeof(e));
    e.kind = MOQ_EVENT_OBJECT_CHUNK;
    e.detail_size = (uint32_t)sizeof(moq_object_chunk_event_t);
    e.borrow_epoch = s->borrow_epoch;
    e.u.object_chunk.sub = rx->sub;
    e.u.object_chunk.pub = rx->pub_handle;
    e.u.object_chunk.chunk = rx->pending_chunk;
    e.u.object_chunk.begin = rx->pending_begin;
    e.u.object_chunk.end = rx->pending_end;
    e.u.object_chunk.terminal = rx->pending_terminal;

    if (rx->pending_begin) {
        e.u.object_chunk.group_id = rx->group_id;
        e.u.object_chunk.subgroup_id = rx->subgroup_id;
        e.u.object_chunk.object_id = rx->cur_object_id;
        e.u.object_chunk.publisher_priority = rx->publisher_priority;
        e.u.object_chunk.status = rx->cur_status;
        e.u.object_chunk.end_of_group = rx->end_of_group;
        e.u.object_chunk.payload_length = rx->payload_expected;
        e.u.object_chunk.properties = rx->cur_extensions;
    }

    moq_result_t rc = push_event(s, &e);
    if (rc < 0) return rc;

    /* Ownership transferred to event queue. */
    rx->pending_chunk = NULL;
    if (rx->pending_begin)
        rx->cur_extensions = NULL;

    return MOQ_OK;
}

static moq_result_t rx_emit_chunk(moq_session_t *s, int slot,
                                   const uint8_t *data, size_t len,
                                   bool begin, bool end,
                                   moq_rcbuf_t *input_rcbuf,
                                   bool data_in_input_buf)
{
    moq_rx_stream_t *rx = &s->rx_streams[slot];

    /* Allocate chunk rcbuf first, before mutating state. */
    moq_rcbuf_t *chunk = NULL;
    bool zero_copy = false;
    if (len > 0) {
        if (input_rcbuf) {
            const uint8_t *rcbuf_start = moq_rcbuf_data(input_rcbuf);
            size_t rcbuf_len = moq_rcbuf_len(input_rcbuf);
            size_t offset = SIZE_MAX;
            /* Continuation chunks: data points directly into rcbuf. */
            uintptr_t dp = (uintptr_t)data;
            uintptr_t rs = (uintptr_t)rcbuf_start;
            if (rcbuf_start && dp >= rs &&
                len <= rcbuf_len && dp - rs <= rcbuf_len - len)
                offset = (size_t)(dp - rs);
            /* Begin chunks via input_src_rcbuf: data is in input_buf,
             * which is a copy of the rcbuf. Cursor offset maps
             * directly because hdr_len was 0 at entry. */
            else if (data_in_input_buf && rx->input_buf) {
                uintptr_t ib = (uintptr_t)rx->input_buf;
                if (dp >= ib && dp - ib < rx->input_len) {
                    size_t ib_off = (size_t)(dp - ib);
                    if (len <= rcbuf_len && ib_off <= rcbuf_len - len)
                        offset = ib_off;
                }
            }
            if (offset != SIZE_MAX) {
                moq_result_t rc = moq_rcbuf_slice(&s->alloc, input_rcbuf,
                    offset, len, &chunk);
                if (rc < 0) {
                    if (data_in_input_buf) {
                        rx->pending_chunk = NULL;
                        rx->pending_begin = begin;
                        rx->pending_end = end;
                        rx->pending_terminal = MOQ_OBJECT_TERMINAL_NORMAL;
                        rx->pending_data_len = len;
                        rx->pending_from_input = true;
                        rx->parse_state = MOQ_RX_PENDING_CHUNK;
                    }
                    return rc;
                }
                zero_copy = true;
            }
        }
        if (!zero_copy) {
            moq_result_t rc = moq_rcbuf_create(&s->alloc, data, len, &chunk);
            if (rc < 0) {
                rx->pending_chunk = NULL;
                rx->pending_begin = begin;
                rx->pending_end = end;
                rx->pending_terminal = MOQ_OBJECT_TERMINAL_NORMAL;
                rx->pending_data_len = len;
                rx->pending_from_input = true;
                rx->parse_state = MOQ_RX_PENDING_CHUNK;
                return rc;
            }
        }
    }

    /* Store complete pending state before attempting push. */
    rx->pending_chunk = chunk;
    rx->pending_begin = begin;
    rx->pending_end = end;
    rx->pending_terminal = MOQ_OBJECT_TERMINAL_NORMAL;
    rx->pending_data_len = len;
    rx->pending_from_input = data_in_input_buf;
    rx->parse_state = MOQ_RX_PENDING_CHUNK;

    moq_result_t rc = rx_push_pending_chunk(s, slot);
    if (rc < 0) return rc;

    /* Push succeeded. Finalize state transition. */
    if (end) {
        if (rx->pending_fin && rx->input_len == 0) {
            rx_record_finished(s, rx->stream_ref._v);
            rx_free_entry(s, (size_t)slot);
        } else {
            rx->parse_state = MOQ_RX_AWAITING_OBJECT;
            rx->payload_written = 0;
            rx->payload_expected = 0;
        }
    } else {
        rx->parse_state = MOQ_RX_STREAMING_PAYLOAD;
    }

    return MOQ_OK;
}

static moq_result_t rx_emit_object(moq_session_t *s, int slot)
{
    moq_rx_stream_t *rx = &s->rx_streams[slot];

    if (event_queue_full(s)) {
        rx->parse_state = MOQ_RX_PENDING_EMIT;
        return MOQ_ERR_WOULD_BLOCK;
    }

    moq_rcbuf_t *payload = NULL;
    if (rx->payload_rcbuf) {
        /* Payload was assembled directly into this rcbuf. It stays owned by rx
         * until push_event succeeds, so a WOULD_BLOCK retry re-emits the same
         * buffer without rebuilding it. */
        payload = rx->payload_rcbuf;
    } else if (rx->cur_status == MOQ_OBJECT_NORMAL) {
        /* Zero-length normal object: a distinct empty rcbuf, created here and
         * owned locally (not by rx) until the push succeeds. */
        moq_result_t rc = moq_rcbuf_create(&s->alloc, NULL, 0, &payload);
        if (rc < 0) {
            rx->parse_state = MOQ_RX_PENDING_EMIT;
            return rc;
        }
    }

    moq_event_t e;
    memset(&e, 0, sizeof(e));
    e.kind = MOQ_EVENT_OBJECT_RECEIVED;
    e.detail_size = (uint32_t)sizeof(moq_object_received_event_t);
    e.borrow_epoch = s->borrow_epoch;
    e.u.object_received.sub = rx->sub;
    e.u.object_received.pub = rx->pub_handle;
    e.u.object_received.group_id = rx->group_id;
    e.u.object_received.subgroup_id = rx->subgroup_id;
    e.u.object_received.object_id = rx->cur_object_id;
    e.u.object_received.publisher_priority = rx->publisher_priority;
    e.u.object_received.status = rx->cur_status;
    e.u.object_received.end_of_group = rx->end_of_group;
    e.u.object_received.payload = payload;
    e.u.object_received.properties = rx->cur_extensions;

    moq_result_t rc = push_event(s, &e);
    if (rc < 0) {
        /* Keep a pre-assembled payload owned by rx for the PENDING_EMIT retry;
         * only free a payload created locally in this call (the empty-object
         * rcbuf). Never decref rx->payload_rcbuf here -- teardown owns it. */
        if (payload && payload != rx->payload_rcbuf)
            moq_rcbuf_decref(payload);
        rx->parse_state = MOQ_RX_PENDING_EMIT;
        return rc;
    }

    rx->cur_extensions = NULL;

    /* Payload ownership transferred to the queued event; payload_buf pointed
     * into that rcbuf, so drop the references without freeing. */
    rx->payload_rcbuf = NULL;
    rx->payload_buf = NULL;
    rx->payload_written = 0;

    /* Handle deferred FIN: if no more buffered data, finish now. */
    if (rx->pending_fin && rx->hdr_len == 0 && rx->input_len == 0) {
        rx_record_finished(s, rx->stream_ref._v);
        rx_free_entry(s, (size_t)slot);
        return MOQ_OK;
    }

    rx->parse_state = MOQ_RX_AWAITING_OBJECT;
    return MOQ_OK;
}

/* Append caller bytes to the rx-owned input_buf for `slot`, enforcing the shared
 * receive budget (max_recv_buf covers retained payload + buffered input). On
 * success sets *out_appended = true and returns MOQ_OK. If the budget is
 * exceeded it issues a STOP (rx_try_stop, which may free the entry) and returns
 * that result with *out_appended = false; on allocation failure returns NOMEM
 * with *out_appended = false. In either failure case the caller must return the
 * value immediately -- the entry may already be gone. */
static moq_result_t rx_append_input(moq_session_t *s, int slot,
                                    const uint8_t *data, size_t len,
                                    bool *out_appended)
{
    moq_rx_stream_t *rx = &s->rx_streams[slot];
    *out_appended = false;

    size_t budget = s->max_recv_buf;
    if (len > SIZE_MAX - rx->input_len)
        return rx_try_stop(s, slot);
    size_t needed = rx->input_len + len;
    /* Only capacity growth beyond what this stream already holds consumes new
     * receive budget. The current input_cap is already charged to
     * recv_input_bytes (and stays charged across drains now that input_buf is
     * retained for reuse), so bytes that fit within it add no memory and must
     * not be re-charged -- otherwise a retained buffer would shrink the
     * effective budget for the next object on the same stream. When input_cap
     * is 0 (no retained buffer) addl == len, matching the prior check. */
    size_t addl = needed > rx->input_cap ? needed - rx->input_cap : 0;
    if (s->recv_payload_bytes >= budget ||
        s->recv_input_bytes > budget - s->recv_payload_bytes ||
        addl > budget - s->recv_payload_bytes - s->recv_input_bytes) {
        return rx_try_stop(s, slot);
    }
    if (needed > rx->input_cap) {
        size_t old_cap = rx->input_cap;
        /* Grow geometrically (double) to amortize repeated small appends into
         * O(log n) reallocs instead of one exact-fit realloc per chunk. Bound
         * the new capacity by what the receive budget still allows for THIS
         * buffer: cap_ceiling excludes this stream's own old_cap (already part
         * of recv_input_bytes) so it measures headroom against other consumers.
         * The budget check above proved `len` fits, which makes cap_ceiling
         * >= needed, so the clamp never drops below what this append requires. */
        size_t cap_ceiling = budget - s->recv_payload_bytes
                             - (s->recv_input_bytes - old_cap);
        size_t new_cap = old_cap ? old_cap : needed;
        while (new_cap < needed) {
            if (new_cap > SIZE_MAX / 2) { new_cap = needed; break; }
            new_cap *= 2;
        }
        if (new_cap > cap_ceiling)
            new_cap = cap_ceiling;
        uint8_t *nb;
        if (rx->input_buf) {
            nb = (uint8_t *)s->alloc.realloc(
                rx->input_buf, old_cap, new_cap, s->alloc.ctx);
        } else {
            nb = (uint8_t *)s->alloc.alloc(new_cap, s->alloc.ctx);
        }
        if (!nb) return MOQ_ERR_NOMEM;
        rx->input_buf = nb;
        rx->input_cap = new_cap;
        s->recv_input_bytes += new_cap - old_cap;
    }
    memcpy(rx->input_buf + rx->input_len, data, len);
    rx->input_len += len;
    *out_appended = true;
    return MOQ_OK;
}

/* -- Data bytes handler -------------------------------------------- */

static moq_result_t handle_data_bytes_impl(moq_session_t *s,
                                            moq_stream_ref_t stream_ref,
                                            const uint8_t *data, size_t len,
                                            bool fin,
                                            moq_rcbuf_t *input_rcbuf)
{
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;

    int slot = rx_find_by_ref(s, stream_ref);
    if (slot < 0) {
        if (len == 0 && !fin) return MOQ_OK;
        if (rx_is_finished(s, stream_ref._v))
            return close_with_error(s, 0x3, "data after FIN");
        slot = rx_find_free(s);
        if (slot < 0) {
            if (action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
            moq_action_t a;
            memset(&a, 0, sizeof(a));
            a.kind = MOQ_ACTION_STOP_DATA;
            a.detail_size = (uint32_t)sizeof(moq_stop_data_action_t);
            a.borrow_epoch = s->borrow_epoch;
            a.u.stop_data.stream_ref = stream_ref;
            a.u.stop_data.error_code = 0;
            return push_action(s, &a);
        }
        moq_rx_stream_t *rx = &s->rx_streams[slot];
        memset(rx, 0, sizeof(*rx));
        rx->active = true;
        rx->stream_kind = MOQ_STREAM_KIND_UNKNOWN;
        rx->stream_ref = stream_ref;
        rx->parse_state = MOQ_RX_AWAITING_HEADER;
        moq_index_insert(s->idx_rx_by_ref, s->idx_rx_mask,
                          stream_ref._v, slot);
    }

    moq_rx_stream_t *rx = &s->rx_streams[slot];

    if (rx->parse_state == MOQ_RX_NEED_STOP)
        return rx_try_stop(s, slot);

    /* If the bound subscription/publication was freed since this stream's header
     * bound it (e.g. a rejected pending PUBLISH), drop the stream instead of
     * emitting against a stale handle. The binding cannot change mid-call (it is
     * only freed by separate app calls), so one check per delivery suffices. */
    if (!rx_binding_alive(s, rx))
        return rx_try_stop(s, slot);

    /* If the stream is already awaiting a pending emit/chunk retry, retain any
     * newly delivered caller bytes (and a FIN) BEFORE retrying. The pending
     * retry can itself WOULD_BLOCK (the event queue is still full), so appending
     * first keeps the new bytes in rx->input_buf under the receive budget -- to
     * be parsed once the pending object/chunk drains -- instead of being silently
     * dropped (the transport does not re-deliver, and the bridge treats
     * WOULD_BLOCK here as "session retained these bytes"). Ordering holds: the
     * pending emit/chunk completes before the parse loop below consumes these
     * appended bytes, and a retained FIN finishes only once input_buf drains
     * (rx_emit_object / the loop check input_len == 0). An rcbuf input is copied
     * -- a backpressure path where simple lifetime beats zero-copy. */
    if ((len > 0 || fin) &&
        (rx->parse_state == MOQ_RX_PENDING_EMIT ||
         rx->parse_state == MOQ_RX_PENDING_CHUNK)) {
        /* Retain the FIN BEFORE the pending retry too: a FIN-only delivery
         * (len == 0) must set pending_fin even when the retry below WOULD_BLOCKs,
         * otherwise the bridge -- which marks fin_retained on that WOULD_BLOCK --
         * believes the FIN was retained while the session leaves the rx entry
         * active awaiting more data. */
        if (fin) rx->pending_fin = true;
        if (len > 0) {
            bool appended;
            moq_result_t arc = rx_append_input(s, slot, data, len, &appended);
            if (!appended) return arc;
            /* Consumed into input_buf; append/zero-copy paths below must skip. */
            data = NULL;
            len = 0;
            input_rcbuf = NULL;
        }
    }

    if (rx->parse_state == MOQ_RX_PENDING_EMIT) {
        moq_result_t erc = rx_emit_object(s, slot);
        if (erc < 0) return erc;
        if (!s->rx_streams[slot].active) return MOQ_OK;
        rx = &s->rx_streams[slot];
    }

    if (rx->parse_state == MOQ_RX_PENDING_CHUNK) {
        bool was_end = rx->pending_end;
        size_t data_len = rx->pending_data_len;
        /* A non-normal terminal (e.g. a RESET deferred by handle_data_reset
         * when the event queue was full) ends the stream: once emitted, the
         * rx slot must be freed, not recycled to AWAITING_OBJECT as a normal
         * object completion would be. */
        bool was_terminal = rx->pending_terminal != MOQ_OBJECT_TERMINAL_NORMAL;

        moq_result_t erc = rx_push_pending_chunk(s, slot);
        if (erc < 0) return erc;
        if (!s->rx_streams[slot].active) return MOQ_OK;
        rx = &s->rx_streams[slot];

        /* Advance payload tracking for the bytes in this chunk. */
        rx->payload_written += data_len;
        rx->pending_begin = false;
        rx->pending_end = false;
        rx->pending_data_len = 0;

        /* Consume the chunk data bytes from input_buf (only for
         * input_buf-backed pending chunks, not slices). */
        bool from_input = rx->pending_from_input;
        rx->pending_from_input = false;
        if (from_input && data_len > 0 && rx->input_buf &&
            data_len <= rx->input_len) {
            size_t remaining = rx->input_len - data_len;
            if (remaining > 0)
                memmove(rx->input_buf, rx->input_buf + data_len, remaining);
            /* Retain drained capacity for reuse (see the compact: label).
             * input_cap stays charged to recv_input_bytes until the stream
             * is freed. */
            rx->input_len = remaining;
        }

        if (was_end) {
            if (was_terminal) {
                /* Terminal reset: the stream is gone (mirrors the direct
                 * handle_data_reset path, which frees without recording a
                 * FIN). */
                rx_free_entry(s, (size_t)slot);
                return MOQ_OK;
            }
            if (rx->pending_fin && rx->input_len == 0) {
                rx_record_finished(s, rx->stream_ref._v);
                rx_free_entry(s, (size_t)slot);
                return MOQ_OK;
            }
            rx->parse_state = MOQ_RX_AWAITING_OBJECT;
            rx->payload_written = 0;
            rx->payload_expected = 0;
        } else {
            rx->parse_state = MOQ_RX_STREAMING_PAYLOAD;
        }
    }

    if (fin) rx->pending_fin = true;

    /* Zero-copy fast path: emit a single continuation chunk directly
     * from the caller's rcbuf when the entire buffer fits in one chunk
     * and no tail needs preserving. */
    if (input_rcbuf && len > 0 && rx->input_len == 0 &&
        rx->parse_state == MOQ_RX_STREAMING_PAYLOAD) {
        size_t need = (size_t)rx->payload_expected - rx->payload_written;
        size_t chunk_len = len < need ? len : need;
        if (chunk_len > MOQ_STREAM_CHUNK_MAX)
            chunk_len = MOQ_STREAM_CHUNK_MAX;

        /* Only zero-copy if the entire rcbuf is consumed by this chunk
         * (no tail to preserve on WOULD_BLOCK). */
        if (chunk_len > 0 && chunk_len >= len) {
            bool final = (rx->payload_written + chunk_len >=
                (size_t)rx->payload_expected);

            moq_result_t erc = rx_emit_chunk(s, slot,
                data, chunk_len, false, final, input_rcbuf, false);
            if (erc < 0) return erc;
            if (!s->rx_streams[slot].active) return MOQ_OK;
            rx = &s->rx_streams[slot];
            rx->payload_written += chunk_len;

            /* A FIN that arrives before the declared payload is complete is a
             * truncated object. Mirror the slow path (the STREAMING_PAYLOAD
             * loop case): emit the partial chunk, then close. Without this the
             * rx slot stays active (STREAMING_PAYLOAD, pending_fin) and is
             * pinned -- MOQ_OK is returned but the stream never finishes. */
            if (rx->pending_fin && !final)
                return close_with_error(s, 0x3, "truncated payload at FIN");

            if (rx->pending_fin && rx->active &&
                rx->parse_state != MOQ_RX_STREAMING_PAYLOAD) {
                if (rx->input_len == 0) {
                    rx_record_finished(s, rx->stream_ref._v);
                    rx_free_entry(s, (size_t)slot);
                }
            }
            return MOQ_OK;
        }
    }

    /* Track whether the input_buf contents came entirely from this
     * rcbuf (enables zero-copy begin chunk emission via offset mapping).
     * Only valid if no partial subgroup header was buffered from a prior
     * call — hdr_len > 0 breaks the cursor-to-rcbuf-offset mapping. */
    moq_rcbuf_t *input_src_rcbuf = NULL;
    if (input_rcbuf && rx->input_len == 0 && rx->hdr_len == 0 && len > 0)
        input_src_rcbuf = input_rcbuf;
    input_rcbuf = NULL;

    /* Append caller bytes to rx-owned input_buf before parsing. */
    if (len > 0) {
        bool appended;
        moq_result_t arc = rx_append_input(s, slot, data, len, &appended);
        if (!appended) return arc;
    }

    size_t cursor = 0;
    moq_result_t loop_rc = MOQ_OK;

    while (cursor < rx->input_len || (rx->pending_fin && rx->active)) {
        if (!rx->active) break;

        switch (rx->parse_state) {
        case MOQ_RX_DEFERRED_ALIAS:
            /* Alias not yet established by a SUBSCRIBE_OK: hold all buffered
             * bytes unparsed (compact + return). When the OK binds the alias,
             * this entry is flipped to AWAITING_OBJECT and re-driven. */
            goto compact;
        case MOQ_RX_AWAITING_HEADER: {
            size_t avail = rx->input_len - cursor;
            size_t space = MOQ_RX_HDR_BUF - rx->hdr_len;
            size_t copy = avail < space ? avail : space;
            if (copy > 0) {
                memcpy(rx->hdr_buf + rx->hdr_len, rx->input_buf + cursor, copy);
                rx->hdr_len += (uint8_t)copy;
                cursor += copy;
            }

            /* Classify stream kind from first byte(s) if not yet known. */
            if (rx->stream_kind == MOQ_STREAM_KIND_UNKNOWN && rx->hdr_len > 0) {
                moq_stream_kind_t k = (moq_stream_kind_t)
                    s->profile->classify_data_stream(rx->hdr_buf, rx->hdr_len);
                if (k == MOQ_STREAM_KIND_NEED_MORE) {
                    /* Leading stream type incomplete: buffer more and retry. */
                    if (rx->hdr_len >= MOQ_RX_HDR_BUF)
                        return close_with_error(s, 0x3, "unclassifiable data stream");
                    if (rx->pending_fin)
                        return close_with_error(s, 0x3,
                            "truncated data stream type at FIN");
                    goto compact;
                }
                rx->stream_kind = k;
                if (rx->stream_kind == MOQ_STREAM_KIND_UNKNOWN)
                    return close_with_error(s, 0x3, "unknown data stream type");
            }

            if (rx->stream_kind == MOQ_STREAM_KIND_FETCH) {
                moq_buf_reader_t r;
                moq_buf_reader_init(&r, rx->hdr_buf, rx->hdr_len);
                moq_decoded_fetch_stream_header_t fhdr;
                moq_result_t rc = s->profile->decode_fetch_header(s, &r, &fhdr);
                if (rc == MOQ_ERR_BUFFER) {
                    if (rx->hdr_len >= MOQ_RX_HDR_BUF)
                        return close_with_error(s, 0x3, "fetch header too large");
                    if (rx->pending_fin)
                        return close_with_error(s, 0x3, "truncated fetch header at FIN");
                    goto compact;
                }
                if (rc < 0)
                    return close_with_error(s, 0x3, "malformed fetch header");

                int fslot = fetch_find_by_request_id(s, fhdr.request_id);
                if (fslot < 0) {
                    /* Late data uni for a locally-cancelled fetch (its slot was
                     * freed at cancel): stop the stream. Mark the rx so the
                     * cancel tombstone is consumed when the STOP is actually
                     * queued -- including a stop deferred via MOQ_RX_NEED_STOP
                     * and completed on a later retry -- never closing. */
                    if (fetch_cancel_tomb_contains(s, fhdr.request_id)) {
                        rx->stop_consumes_cancel_tomb = true;
                        rx->cancel_tomb_request_id = fhdr.request_id;
                        moq_result_t src = rx_try_stop(s, slot);
                        if (src < 0) { loop_rc = src; goto compact; }
                        goto compact;
                    }
                    return close_with_error(s, 0x3, "FETCH_HEADER unknown request ID");
                }

                moq_fetch_entry_t *fe = &s->fetches[fslot];
                if (fe->role != MOQ_FETCH_ROLE_FETCHER)
                    return close_with_error(s, 0x3, "FETCH_HEADER for non-fetcher");

                /* Late data after a request-stream GOAWAY migration (§10.4):
                 * absorb the response WITHOUT stopping (graceful migration leaves
                 * the data uni alone). Release the migration tombstone and bind the
                 * already-invalidated handle so the object loop drops everything via
                 * the stale-handle path; the FIN then frees the rx context. */
                if (fe->state == MOQ_FETCH_GOAWAY_LOCAL) {
                    size_t gconsumed = moq_buf_reader_offset(&r);
                    uint8_t gleftover = rx->hdr_len - (uint8_t)gconsumed;
                    rx->fetch = fe->handle;        /* stale generation */
                    fetch_free_entry(s, fslot);    /* tombstone served its purpose */
                    cursor -= gleftover;           /* re-read object bytes */
                    rx->hdr_len = 0;
                    rx->parse_state = MOQ_RX_AWAITING_OBJECT;
                    continue;
                }

                /* A fetch binds exactly one response data stream. A FETCH_HEADER
                 * that arrives once a stream is already bound, on a *different*
                 * stream, is a protocol violation -- not a second response.
                 * Accepting it would overwrite data_stream_ref and could emit
                 * FETCH_COMPLETE / free the fetch while the original stream is
                 * still open. A re-feed of the same stream (e.g. a WOULD_BLOCK
                 * re-drive) keeps the same stream_ref and is fine; such retries
                 * also occur before data_stream_started is set, so this guard
                 * never rejects legitimate re-drive. (GOAWAY migration, handled
                 * above, frees the entry first, so it never reaches here.) */
                if (fe->data_stream_started &&
                    fe->data_stream_ref._v != stream_ref._v)
                    return close_with_error(s, 0x3,
                        "duplicate FETCH_HEADER on a second data stream");

                size_t consumed = moq_buf_reader_offset(&r);
                uint8_t leftover = rx->hdr_len - (uint8_t)consumed;
                size_t post_cursor = cursor - leftover;

                bool is_fin_complete = rx->pending_fin &&
                                       post_cursor >= rx->input_len;

                bool want_complete = is_fin_complete && fe->control_ok;
                if (want_complete && event_queue_full(s)) {
                    loop_rc = MOQ_ERR_WOULD_BLOCK;
                    goto compact;
                }

                /* Commit: safe to mutate — event slot reserved if needed. */
                fe->data_stream_ref = stream_ref;
                fe->data_stream_started = true;
                rx->fetch = fe->handle;
                cursor = post_cursor;
                rx->hdr_len = 0;

                if (is_fin_complete) {
                    fe->data_stream_fin = true;
                    if (want_complete) {
                        moq_event_t ev;
                        memset(&ev, 0, sizeof(ev));
                        ev.kind = MOQ_EVENT_FETCH_COMPLETE;
                        ev.detail_size = (uint32_t)sizeof(moq_fetch_complete_event_t);
                        ev.borrow_epoch = s->borrow_epoch;
                        ev.u.fetch_complete.fetch = fe->handle;
                        moq_result_t erc2 = push_event(s, &ev);
                        if (erc2 < 0) { loop_rc = erc2; goto compact; }
                        fetch_free_entry(s, (int)(fslot));
                    }
                    rx_record_finished(s, stream_ref._v);
                    rx_free_entry(s, (size_t)slot);
                    return MOQ_OK;
                }

                rx->parse_state = MOQ_RX_AWAITING_OBJECT;
                continue;
            }

            /* Subgroup header parse. */
            moq_buf_reader_t r;
            moq_buf_reader_init(&r, rx->hdr_buf, rx->hdr_len);
            moq_decoded_subgroup_header_t hdr;
            moq_result_t rc = s->profile->decode_subgroup_header(s, &r, &hdr);
            if (rc == MOQ_ERR_BUFFER) {
                if (rx->hdr_len >= MOQ_RX_HDR_BUF)
                    return close_with_error(s, 0x3, "subgroup header too large");
                if (rx->pending_fin) {
                    if (rx->hdr_len > 0)
                        return close_with_error(s, 0x3, "truncated subgroup header at FIN");
                    rx_record_finished(s, stream_ref._v);
                    rx_free_entry(s, (size_t)slot);
                    return MOQ_OK;
                }
                goto compact;
            }
            if (rc < 0)
                return close_with_error(s, 0x3, "malformed subgroup header");

            int sub_slot = sub_find_by_alias_subscriber(s, hdr.track_alias);
            int pub_slot_rx = -1;
            if (sub_slot < 0)
                pub_slot_rx = pub_find_by_alias_subscriber(s, hdr.track_alias);
            if (sub_slot < 0 && pub_slot_rx < 0) {
                /* Alias not yet established. If a forwarding subscription is
                 * pending, a SUBSCRIBE_OK may establish this alias imminently:
                 * defer the stream (retain its header, hold object bytes for
                 * control/data reordering) rather than stopping it. */
                if (session_has_forwarding_pending_subscriber(s)) {
                    rx->sub = MOQ_SUBSCRIPTION_INVALID;
                    rx->pub_handle = MOQ_PUBLICATION_INVALID;
                    rx->track_alias = hdr.track_alias;
                    rx->group_id = hdr.group_id;
                    rx->publisher_priority = hdr.publisher_priority;
                    rx->has_extensions = hdr.has_extensions;
                    rx->end_of_group = hdr.end_of_group;
                    rx->subgroup_id_from_first_object =
                        hdr.subgroup_id_from_first_object;
                    rx->subgroup_id = hdr.subgroup_id;
                    rx->subgroup_id_resolved = hdr.subgroup_id_resolved;
                    rx->parse_state = MOQ_RX_DEFERRED_ALIAS;
                    size_t consumed = moq_buf_reader_offset(&r);
                    uint8_t leftover = rx->hdr_len - (uint8_t)consumed;
                    cursor -= leftover;
                    rx->hdr_len = 0;
                    continue;
                }
                moq_result_t src = rx_try_stop(s, slot);
                if (src < 0) { loop_rc = src; goto compact; }
                goto compact;
            }

            if (sub_slot >= 0) {
                rx->sub = s->subs[sub_slot].handle;
            } else {
                rx->sub = MOQ_SUBSCRIPTION_INVALID;
                rx->pub_handle = s->publishes[pub_slot_rx].handle;
            }
            rx->track_alias = hdr.track_alias;
            rx->group_id = hdr.group_id;
            rx->publisher_priority = hdr.publisher_priority;
            rx->has_extensions = hdr.has_extensions;
            rx->end_of_group = hdr.end_of_group;
            rx->subgroup_id_from_first_object = hdr.subgroup_id_from_first_object;
            rx->subgroup_id = hdr.subgroup_id;
            rx->subgroup_id_resolved = hdr.subgroup_id_resolved;

            rx->parse_state = MOQ_RX_AWAITING_OBJECT;

            size_t consumed = moq_buf_reader_offset(&r);
            uint8_t leftover = rx->hdr_len - (uint8_t)consumed;
            cursor -= leftover;
            rx->hdr_len = 0;
            continue;
        }

        case MOQ_RX_AWAITING_OBJECT: {
            if (rx->stream_kind == MOQ_STREAM_KIND_FETCH) {
                size_t avail = rx->input_len - cursor;

                if (avail == 0 && rx->pending_fin) {
                    int fslot2 = fetch_resolve_handle(s, rx->fetch);
                    bool emit = false;
                    if (fslot2 >= 0) {
                        moq_fetch_entry_t *fe2 = &s->fetches[fslot2];
                        fe2->data_stream_fin = true;
                        emit = fe2->control_ok;
                    }
                    if (emit) {
                        if (event_queue_full(s)) {
                            loop_rc = MOQ_ERR_WOULD_BLOCK;
                            goto compact;
                        }
                        moq_event_t fev;
                        memset(&fev, 0, sizeof(fev));
                        fev.kind = MOQ_EVENT_FETCH_COMPLETE;
                        fev.detail_size = (uint32_t)sizeof(moq_fetch_complete_event_t);
                        fev.borrow_epoch = s->borrow_epoch;
                        fev.u.fetch_complete.fetch = rx->fetch;
                        moq_result_t ferc = push_event(s, &fev);
                        if (ferc < 0) { loop_rc = ferc; goto compact; }
                        fetch_free_entry(s, fslot2);
                    }
                    rx_record_finished(s, stream_ref._v);
                    rx_free_entry(s, (size_t)slot);
                    return MOQ_OK;
                }

                if (avail == 0) goto compact;

                moq_buf_reader_t fr;
                moq_buf_reader_init(&fr, rx->input_buf + cursor, avail);

                moq_decoded_fetch_object_t fobj;
                moq_result_t forc = s->profile->decode_fetch_object(
                    s, &fr, &rx->fetch_prior, &fobj);
                if (forc == MOQ_ERR_BUFFER) {
                    if (rx->pending_fin)
                        return close_with_error(s, 0x3,
                            "truncated fetch object at FIN");
                    goto compact;
                }
                if (forc < 0)
                    return close_with_error(s, 0x3,
                        "malformed fetch object");

                /* Stale handle check first — absorb late data after
                 * REQUEST_ERROR without needing an event slot. */
                if (fetch_resolve_handle(s, rx->fetch) < 0) {
                    cursor += moq_buf_reader_offset(&fr);
                    continue;
                }

                if (fobj.is_range_marker) {
                    if (event_queue_full(s)) {
                        loop_rc = MOQ_ERR_WOULD_BLOCK;
                        goto compact;
                    }
                    moq_event_t gev;
                    memset(&gev, 0, sizeof(gev));
                    gev.kind = MOQ_EVENT_FETCH_GAP;
                    gev.detail_size = (uint32_t)sizeof(moq_fetch_gap_event_t);
                    gev.borrow_epoch = s->borrow_epoch;
                    gev.u.fetch_gap.fetch = rx->fetch;
                    gev.u.fetch_gap.range_kind =
                        (moq_fetch_range_kind_t)fobj.range_kind;
                    gev.u.fetch_gap.group_id = fobj.group_id;
                    gev.u.fetch_gap.object_id = fobj.object_id;
                    moq_result_t gerc = push_event(s, &gev);
                    if (gerc < 0) { loop_rc = gerc; goto compact; }

                    /* An End-of-Range marker sets the prior Location only; the
                     * prior Subgroup/Priority (has_actual) come from the last
                     * actual object and are left untouched. */
                    rx->fetch_prior.has_prev = true;
                    rx->fetch_prior.group_id = fobj.group_id;
                    rx->fetch_prior.object_id = fobj.object_id;
                    cursor += moq_buf_reader_offset(&fr);
                    continue;
                }

                /* Normal fetch object: payload was consumed by the decoder. */
                if (fobj.payload_len > s->max_obj_payload)
                    return close_with_error(s, 0x3,
                        "fetch object exceeds max payload");

                size_t obj_budget = (size_t)fobj.payload_len + fobj.properties_len;
                if (s->recv_payload_bytes + obj_budget > s->max_recv_buf)
                    return rx_try_stop(s, slot);

                if (event_queue_full(s)) {
                    loop_rc = MOQ_ERR_WOULD_BLOCK;
                    goto compact;
                }

                moq_rcbuf_t *fpayload = NULL;
                if (fobj.payload_len > 0) {
                    const uint8_t *pdata = fr.data + fr.pos -
                                            (size_t)fobj.payload_len;
                    moq_result_t crc2 = moq_rcbuf_create(&s->alloc,
                        pdata, (size_t)fobj.payload_len, &fpayload);
                    if (crc2 < 0) return MOQ_ERR_NOMEM;
                }

                moq_rcbuf_t *fprops = NULL;
                if (fobj.properties_len > 0) {
                    moq_result_t crc3 = moq_rcbuf_create(&s->alloc,
                        fobj.properties, fobj.properties_len, &fprops);
                    if (crc3 < 0) {
                        if (fpayload) moq_rcbuf_decref(fpayload);
                        return MOQ_ERR_NOMEM;
                    }
                }

                moq_event_t oev;
                memset(&oev, 0, sizeof(oev));
                oev.kind = MOQ_EVENT_FETCH_OBJECT;
                oev.detail_size = (uint32_t)sizeof(moq_fetch_object_event_t);
                oev.borrow_epoch = s->borrow_epoch;
                oev.u.fetch_object.fetch = rx->fetch;
                oev.u.fetch_object.group_id = fobj.group_id;
                oev.u.fetch_object.subgroup_id = fobj.subgroup_id;
                oev.u.fetch_object.object_id = fobj.object_id;
                oev.u.fetch_object.publisher_priority = fobj.publisher_priority;
                oev.u.fetch_object.datagram = fobj.datagram;
                oev.u.fetch_object.payload = fpayload;
                oev.u.fetch_object.properties = fprops;

                moq_result_t oerc = push_event(s, &oev);
                if (oerc < 0) {
                    if (fpayload) moq_rcbuf_decref(fpayload);
                    if (fprops) moq_rcbuf_decref(fprops);
                    loop_rc = oerc;
                    goto compact;
                }

                s->recv_payload_bytes += obj_budget;

                rx->fetch_prior.has_prev = true;
                rx->fetch_prior.has_actual = true;   /* actual object metadata */
                rx->fetch_prior.group_id = fobj.group_id;
                rx->fetch_prior.subgroup_id = fobj.subgroup_id;
                rx->fetch_prior.object_id = fobj.object_id;
                rx->fetch_prior.publisher_priority = fobj.publisher_priority;
                cursor += moq_buf_reader_offset(&fr);
                continue;
            }

            size_t avail = rx->input_len - cursor;

            moq_buf_reader_t r;
            moq_buf_reader_init(&r,
                avail > 0 ? rx->input_buf + cursor : NULL, avail);

            moq_decoded_object_header_t obj;
            moq_result_t orc = s->profile->decode_object_header(s, &r,
                rx->has_extensions, rx->prev_object_id, rx->has_prev_object,
                &obj);
            if (orc == MOQ_ERR_BUFFER) {
                if (rx->pending_fin && avail == 0) {
                    rx_record_finished(s, stream_ref._v);
                    rx_free_entry(s, (size_t)slot);
                    return MOQ_OK;
                }
                if (rx->pending_fin)
                    return close_with_error(s, 0x3, "truncated object header at FIN");
                goto compact;
            }
            if (orc == MOQ_ERR_PROTO)
                return close_with_error(s, 0x3, "malformed object header");
            if (orc < 0)
                return close_with_error(s, 0x3, "object header decode error");

            moq_rcbuf_t *ext_rcbuf = NULL;
            if (obj.has_properties && obj.properties_len > 0) {
                moq_result_t erc = moq_rcbuf_create(&s->alloc,
                    obj.properties, obj.properties_len, &ext_rcbuf);
                if (erc < 0) { loop_rc = erc; goto compact; }
                s->recv_payload_bytes += obj.properties_len;
            }

            uint64_t object_id = obj.object_id;
            moq_object_status_t semantic = obj.status;
            uint64_t payload_len = obj.payload_len;

            if (!rx->subgroup_id_resolved &&
                rx->subgroup_id_from_first_object) {
                rx->subgroup_id = object_id;
                rx->subgroup_id_resolved = true;
            }

            if (payload_len > s->max_obj_payload) {
                if (ext_rcbuf) {
                    s->recv_payload_bytes -= moq_rcbuf_len(ext_rcbuf);
                    moq_rcbuf_decref(ext_rcbuf);
                }
                cursor += r.pos;
                loop_rc = rx_try_stop(s, slot);
                goto compact;
            }

            if (payload_len > 0) {
                size_t budget = s->max_recv_buf;
                if (s->recv_payload_bytes > budget ||
                    s->recv_input_bytes > budget - s->recv_payload_bytes ||
                    payload_len > budget - s->recv_payload_bytes - s->recv_input_bytes) {
                    if (ext_rcbuf) {
                        s->recv_payload_bytes -= moq_rcbuf_len(ext_rcbuf);
                        moq_rcbuf_decref(ext_rcbuf);
                    }
                    cursor += r.pos;
                    loop_rc = rx_try_stop(s, slot);
                    goto compact;
                }
            }

            rx->cur_object_id = object_id;
            rx->cur_status = semantic;
            rx->prev_object_id = object_id;
            rx->has_prev_object = true;
            rx->payload_expected = payload_len;
            rx->payload_written = 0;
            rx->cur_extensions = ext_rcbuf;

            cursor += r.pos;

            if (s->streaming_objects) {
                /* Streaming mode: emit begin chunk, possibly with
                 * initial payload data from the same input call. */
                size_t avail = rx->input_len - cursor;
                size_t need = (size_t)payload_len - rx->payload_written;
                size_t chunk_len = avail < need ? avail : need;
                if (chunk_len > MOQ_STREAM_CHUNK_MAX)
                    chunk_len = MOQ_STREAM_CHUNK_MAX;
                bool final = (rx->payload_written + chunk_len >=
                    (size_t)payload_len);
                bool is_begin_end = (payload_len == 0) || final;

                /* Zero-copy begin chunk emission: if input_buf came
                 * from a single rcbuf and this chunk consumes all
                 * remaining data (cursor + chunk_len == original len,
                 * no tail to preserve). */
                moq_rcbuf_t *begin_rcbuf = NULL;
                if (input_src_rcbuf && chunk_len > 0 &&
                    cursor + chunk_len == rx->input_len)
                    begin_rcbuf = input_src_rcbuf;

                moq_result_t erc = rx_emit_chunk(s, slot,
                    chunk_len > 0 ? rx->input_buf + cursor : NULL,
                    chunk_len, true, is_begin_end, begin_rcbuf, true);
                if (erc < 0) { loop_rc = erc; goto compact; }

                rx->payload_written += chunk_len;
                cursor += chunk_len;

                if (!s->rx_streams[slot].active) { loop_rc = MOQ_OK; goto compact; }
                rx = &s->rx_streams[slot];
                continue;
            }

            if (payload_len == 0) {
                moq_result_t erc = rx_emit_object(s, slot);
                if (erc < 0) { loop_rc = erc; goto compact; }
                continue;
            }

            /* Allocate the delivered payload rcbuf up front and stream the
             * object's bytes directly into its inline storage, so rx_emit_object
             * can hand it to the event without a copy. payload_buf is the
             * writable cursor into that rcbuf. */
            moq_result_t arc = moq_rcbuf_alloc_uninit(
                &s->alloc, (size_t)payload_len,
                &rx->payload_rcbuf, &rx->payload_buf);
            if (arc < 0) { loop_rc = arc; goto compact; }
            s->recv_payload_bytes += (size_t)payload_len;

            if (rx->payload_written >= (size_t)payload_len) {
                moq_result_t erc = rx_emit_object(s, slot);
                if (erc < 0) { loop_rc = erc; goto compact; }
                continue;
            }

            rx->parse_state = MOQ_RX_READING_PAYLOAD;
            continue;
        }

        case MOQ_RX_READING_PAYLOAD: {
            size_t avail = rx->input_len - cursor;
            size_t need = (size_t)rx->payload_expected - rx->payload_written;
            size_t copy = avail < need ? avail : need;
            if (copy > 0) {
                memcpy(rx->payload_buf + rx->payload_written, rx->input_buf + cursor, copy);
                rx->payload_written += copy;
                cursor += copy;
            }

            if (rx->payload_written >= (size_t)rx->payload_expected) {
                moq_result_t erc = rx_emit_object(s, slot);
                if (erc < 0) { loop_rc = erc; goto compact; }
                continue;
            }

            if (rx->pending_fin)
                return close_with_error(s, 0x3, "truncated payload at FIN");
            goto compact;
        }

        case MOQ_RX_STREAMING_PAYLOAD: {
            size_t avail = rx->input_len - cursor;
            size_t need = (size_t)rx->payload_expected - rx->payload_written;
            size_t chunk_len = avail < need ? avail : need;
            if (chunk_len > MOQ_STREAM_CHUNK_MAX)
                chunk_len = MOQ_STREAM_CHUNK_MAX;
            if (chunk_len == 0 && !rx->pending_fin) goto compact;

            bool final = (rx->payload_written + chunk_len >=
                (size_t)rx->payload_expected);

            if (chunk_len == 0 && rx->pending_fin && !final)
                return close_with_error(s, 0x3, "truncated payload at FIN");

            moq_result_t erc = rx_emit_chunk(s, slot,
                chunk_len > 0 ? rx->input_buf + cursor : NULL,
                chunk_len, false, final, input_rcbuf, true);
            if (erc < 0) { loop_rc = erc; goto compact; }

            rx = &s->rx_streams[slot];
            rx->payload_written += chunk_len;
            cursor += chunk_len;

            if (!s->rx_streams[slot].active) { loop_rc = MOQ_OK; goto compact; }
            continue;
        }

        default:
            goto compact;
        } /* switch */
    } /* while */

    if (rx->pending_fin && rx->active && cursor >= rx->input_len) {
        rx_record_finished(s, stream_ref._v);
        rx_free_entry(s, (size_t)slot);
        return MOQ_OK;
    }

compact:
    if (rx->active && cursor > 0) {
        size_t remaining = rx->input_len - cursor;
        if (remaining > 0)
            memmove(rx->input_buf, rx->input_buf + cursor, remaining);
        /* When the buffer fully drains, retain its capacity for the next
         * object on this stream instead of freeing it. A transport that
         * delivers each object as its own read (or splits header from
         * payload, as SimPair does) would otherwise re-allocate input_buf
         * per delivery; reuse removes that per-object staging alloc in the
         * steady-state receive path. The retained input_cap stays charged to
         * recv_input_bytes until rx_free_entry / session destroy, so the
         * receive budget still accounts for it exactly once -- which means a
         * retained buffer may reduce tight-buffer headroom for other consumers
         * (honest accounting: it is real allocated memory until the stream is
         * freed). rx_append_input charges only growth beyond input_cap, so
         * reusing this capacity for the next object adds no new budget. */
        rx->input_len = remaining;
    }

    return loop_rc;
}

moq_result_t handle_data_bytes(moq_session_t *s,
                                moq_stream_ref_t stream_ref,
                                const uint8_t *data, size_t len,
                                bool fin)
{
    return handle_data_bytes_impl(s, stream_ref, data, len, fin, NULL);
}

/* Classify a deferred-replay result and decide whether to stop the replay loop.
 * WOULD_BLOCK is the only retryable outcome here (the entry keeps resumed_deferred
 * set and a later advance re-drives it). Because replay has no external caller
 * holding the bytes and no return channel, a hard failure must be latched rather
 * than silently retried forever: MOQ_ERR_CLOSED means handle_data_bytes_impl
 * already latched a protocol close, and any other negative result (e.g.
 * MOQ_ERR_NOMEM) is latched here as an internal error. Returns true to stop. */
static bool replay_result_should_stop(moq_session_t *s, moq_result_t rc)
{
    if (rc >= 0 || rc == MOQ_ERR_WOULD_BLOCK)
        return false;
    if (rc != MOQ_ERR_CLOSED && session_is_active(s))
        (void)close_with_error(s, 0x1, "internal error replaying deferred objects");
    return true;
}

void session_resume_deferred_for_alias(moq_session_t *s, uint64_t alias)
{
    int sub_slot = sub_find_by_alias_subscriber(s, alias);
    if (sub_slot < 0) return;
    for (size_t i = 0; i < s->rx_cap; i++) {
        moq_rx_stream_t *rx = &s->rx_streams[i];
        if (!rx->active || rx->parse_state != MOQ_RX_DEFERRED_ALIAS ||
            rx->track_alias != alias)
            continue;
        /* Bind to the now-established subscription and drive the buffered
         * objects. Backpressure leaves the entry partially drained with
         * resumed_deferred set; session_retry_resumed_deferred finishes it. */
        rx->sub = s->subs[sub_slot].handle;
        rx->parse_state = MOQ_RX_AWAITING_OBJECT;
        rx->resumed_deferred = true;
        moq_result_t rc =
            handle_data_bytes_impl(s, rx->stream_ref, NULL, 0, false, NULL);
        if (replay_result_should_stop(s, rc))
            return;
    }
}

void session_retry_resumed_deferred(moq_session_t *s)
{
    for (size_t i = 0; i < s->rx_cap; i++) {
        moq_rx_stream_t *rx = &s->rx_streams[i];
        if (!rx->active || !rx->resumed_deferred) continue;
        /* Fully drained and idle: clear the marker. */
        if (rx->input_len == 0 && !rx->pending_fin &&
            rx->parse_state != MOQ_RX_PENDING_EMIT &&
            rx->parse_state != MOQ_RX_PENDING_CHUNK) {
            rx->resumed_deferred = false;
            continue;
        }
        moq_result_t rc =
            handle_data_bytes_impl(s, rx->stream_ref, NULL, 0, false, NULL);
        if (replay_result_should_stop(s, rc))
            return;
    }
}

void session_discard_deferred_streams(moq_session_t *s)
{
    for (size_t i = 0; i < s->rx_cap; i++) {
        moq_rx_stream_t *rx = &s->rx_streams[i];
        if (!rx->active || rx->parse_state != MOQ_RX_DEFERRED_ALIAS)
            continue;
        /* Best-effort STOP_DATA. If the action queue is full, rx_try_stop
         * leaves the entry in NEED_STOP without freeing -- but no forwarding
         * subscription remains to drive a later retry, so free it directly to
         * avoid retaining a stale stream/buffer. A late peer byte just creates
         * a fresh entry that is STOP'd then. */
        if (rx_try_stop(s, (int)i) != MOQ_OK && s->rx_streams[i].active)
            rx_free_entry(s, i);
    }
}

moq_result_t handle_data_bytes_rcbuf(moq_session_t *s,
                                      moq_stream_ref_t stream_ref,
                                      moq_rcbuf_t *input_rcbuf,
                                      bool fin)
{
    const uint8_t *data = input_rcbuf ? moq_rcbuf_data(input_rcbuf) : NULL;
    size_t len = input_rcbuf ? moq_rcbuf_len(input_rcbuf) : 0;
    return handle_data_bytes_impl(s, stream_ref, data, len, fin, input_rcbuf);
}

moq_result_t handle_data_reset(moq_session_t *s,
                                moq_stream_ref_t stream_ref)
{
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;

    int slot = rx_find_by_ref(s, stream_ref);
    if (slot < 0) return MOQ_OK;

    moq_rx_stream_t *rx = &s->rx_streams[slot];

    if (!s->streaming_objects ||
        (rx->parse_state != MOQ_RX_STREAMING_PAYLOAD &&
         rx->parse_state != MOQ_RX_PENDING_CHUNK)) {
        rx_free_entry(s, (size_t)slot);
        return MOQ_OK;
    }

    /* Streaming mid-object: push any pending payload chunk first,
     * then emit terminal RESET — unless the pending chunk already
     * completes the object normally (end=true). */
    if (rx->parse_state == MOQ_RX_PENDING_CHUNK &&
        rx->pending_terminal == MOQ_OBJECT_TERMINAL_NORMAL) {
        bool was_final = rx->pending_end;
        moq_result_t rc = rx_push_pending_chunk(s, slot);
        if (rc < 0) return rc;
        rx = &s->rx_streams[slot];
        rx->pending_begin = false;
        rx->pending_end = false;
        rx->pending_data_len = 0;
        if (was_final) {
            rx_free_entry(s, (size_t)slot);
            return MOQ_OK;
        }
    }

    /* Emit terminal RESET event. */
    rx->pending_chunk = NULL;
    rx->pending_begin = false;
    rx->pending_end = true;
    rx->pending_terminal = MOQ_OBJECT_TERMINAL_RESET;
    rx->pending_data_len = 0;
    rx->parse_state = MOQ_RX_PENDING_CHUNK;

    moq_result_t rc = rx_push_pending_chunk(s, slot);
    if (rc < 0) return rc;

    rx_free_entry(s, (size_t)slot);
    return MOQ_OK;
}
