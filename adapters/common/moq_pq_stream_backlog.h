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

#endif /* MOQ_PQ_STREAM_BACKLOG_H */
