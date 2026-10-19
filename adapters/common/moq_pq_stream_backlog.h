/*
 * moq_pq_stream_backlog.h -- libmoq-local "stream backlog empty" predicate.
 *
 * picoquic exposes picoquic_is_cnx_backlog_empty() (the sent-but-unacknowledged
 * packet/retransmission backlog) but no public predicate for "does any stream
 * still have queued or ready-to-send reliable data or an unsent FIN" -- the
 * signal moq_endpoint_drain() needs to know a graceful stop will not truncate
 * locally queued bytes. Rather than patch picoquic to add one, libmoq computes
 * it here from picoquic's stream state. This intentionally reads picoquic
 * private state from <picoquic_internal.h>, which picoquic does not install --
 * so the adapters that include this header (the raw picoquic threaded adapter
 * and the pico-WT managed adapter) require source-tree picoquic
 * (-DMOQ_PICOQUIC_SOURCE_DIR=...); their CMake fails configuration otherwise.
 * The struct-layout dependency is bounded to the picoquic revision in use.
 * Internal to adapter implementation only -- never exposed in a public header.
 *
 * Side-effect free: it mirrors the data-availability test in
 * picoquic_find_ready_stream_has_data() WITHOUT that helper's
 * cnx->stream_blocked mutation, and ignores flow-control / output-list state
 * (a stream whose data is merely flow-control-blocked is still backlogged for
 * the purpose of a drain -- stopping now would truncate it). A reset stream has
 * no pending reliable data. Network-thread only, like
 * picoquic_is_cnx_backlog_empty().
 */
#ifndef MOQ_PQ_STREAM_BACKLOG_H
#define MOQ_PQ_STREAM_BACKLOG_H

#include <picoquic.h>
#include <picoquic_internal.h>

static inline int moq_pq_cnx_stream_backlog_empty(picoquic_cnx_t *cnx)
{
    for (picoquic_stream_head_t *s = picoquic_first_stream(cnx); s != NULL;
         s = picoquic_next_stream(s)) {
        if (s->reset_sent)
            continue;
        if (s->is_active ||
            (s->send_queue != NULL &&
             s->send_queue->length > s->send_queue->offset) ||
            (s->fin_requested && !s->fin_sent))
            return 0;
    }
    return 1;
}

/*
 * Whether the peer has acknowledged everything this side still owes on one
 * stream's send half, from picoquic's per-stream SACK list. picoquic records
 * an acknowledged STREAM frame as the inclusive range [offset,
 * offset + length - 1], extended by one position for its FIN, so:
 *   - a peer-initiated unidirectional stream has no send half: nothing owed;
 *   - a reset, from the moment it is requested, owes its RESET sent and
 *     acknowledged (reset_sent, reset_acked) and the reliable prefix it keeps
 *     (reliable_size, zero for an ordinary reset), never the discarded tail;
 *   - a sent FIN owes every byte AND the FIN position (a zero-length FIN is
 *     position 0);
 *   - an open send half owes the bytes sent so far (none when nothing was
 *     sent), never a FIN, so an open control/CONNECT/request stream does not
 *     wait for its peer.
 * The coverage must start at offset 0: an acknowledged final range with an
 * earlier hole is not enough. Network-thread only.
 */
static inline int moq_pq_stream_send_acked(picoquic_cnx_t *cnx, picoquic_stream_head_t *s)
{
    if (!IS_BIDIR_STREAM_ID(s->stream_id) &&
        !IS_LOCAL_STREAM_ID(s->stream_id, cnx->client_mode))
        return 1;
    if (s->reset_requested || s->reset_sent)
        return s->reset_sent && s->reset_acked && picoquic_reliable_prefix_is_acked(s);
    if (s->fin_sent)
        return picoquic_check_sack_list(&s->sack_list, 0, s->sent_offset) != 0;
    return s->sent_offset == 0 ||
           picoquic_check_sack_list(&s->sack_list, 0, s->sent_offset - 1) != 0;
}

/*
 * Every stream's send half is acknowledged (moq_pq_stream_send_acked). A
 * stream picoquic has already retired was fully acknowledged. Unlike
 * picoquic_is_cnx_backlog_empty() this ignores packets that carry no stream
 * data (ACK-only and other control frames), so an idle connection settles.
 * Network-thread only.
 */
static inline int moq_pq_cnx_streams_acked(picoquic_cnx_t *cnx)
{
    for (picoquic_stream_head_t *s = picoquic_first_stream(cnx); s != NULL;
         s = picoquic_next_stream(s)) {
        if (!moq_pq_stream_send_acked(cnx, s))
            return 0;
    }
    return 1;
}

#endif /* MOQ_PQ_STREAM_BACKLOG_H */
