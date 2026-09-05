/*
 * Inbound peer Request ID tracking shared by the draft-16 and draft-18 profiles.
 *
 * The peer allocates its Request IDs sequentially with its parity (draft-18
 * section 10.1, draft-16 section 9.5), but from draft-16 on a request can ride
 * its own bidirectional stream, and streams carry no ordering guarantee between
 * them: a transport that services stream 9 before stream 5 delivers request 5
 * before request 3 (mvfst does; picoquic happens to serve lower ids first). The
 * draft makes only a wrong parity or a DUPLICATE id a session error
 * (INVALID_REQUEST_ID), so the receiver must tolerate arrival out of order.
 *
 * The tracker keeps a high-water mark `next` (the lowest id above every id seen
 * so far, which is also what GOAWAY reports as the next unprocessed request)
 * plus the set of ids below it that have not arrived yet. An id at the mark
 * advances it; one above it records the skipped ids as gaps and jumps; one
 * below it must be an open gap, otherwise it is a duplicate. The gap set is
 * bounded: a peer that runs more than MOQ_REQUEST_ID_GAP_MAX requests ahead of
 * its lowest outstanding one is refused as a protocol error, which keeps the
 * per-session cost fixed and still far exceeds any real request window.
 */
#ifndef MOQ_REQUEST_ID_WINDOW_H
#define MOQ_REQUEST_ID_WINDOW_H

#include <stddef.h>
#include <stdint.h>

#ifndef MOQ_REQUEST_ID_GAP_MAX
#define MOQ_REQUEST_ID_GAP_MAX 64
#endif

typedef struct moq_request_id_window {
    uint64_t next;                            /* high-water mark (peer parity) */
    uint64_t gaps[MOQ_REQUEST_ID_GAP_MAX];    /* ids < next not yet received */
    size_t   gap_count;
} moq_request_id_window_t;

/* Classification of an inbound id whose parity already matched the peer's. */
typedef enum moq_request_id_verdict {
    MOQ_REQUEST_ID_NEW = 0,       /* not seen before: accept */
    MOQ_REQUEST_ID_DUPLICATE,     /* already received (or below the lowest gap) */
    MOQ_REQUEST_ID_TOO_FAR_AHEAD  /* would leave more than MOQ_REQUEST_ID_GAP_MAX
                                     ids outstanding below the mark */
} moq_request_id_verdict_t;

static inline void moq_request_id_window_init(moq_request_id_window_t *w,
                                              uint64_t first_peer_id)
{
    w->next = first_peer_id;
    w->gap_count = 0;
}

/* Pure: classify without recording. Validation and commit are separate steps
 * in the profiles (a request can still be refused after validation). */
static inline moq_request_id_verdict_t
moq_request_id_window_classify(const moq_request_id_window_t *w, uint64_t id)
{
    if (id == w->next) return MOQ_REQUEST_ID_NEW;
    if (id > w->next) {
        uint64_t skipped = (id - w->next) / 2;
        return (skipped <= MOQ_REQUEST_ID_GAP_MAX &&
                w->gap_count + (size_t)skipped <= MOQ_REQUEST_ID_GAP_MAX)
                   ? MOQ_REQUEST_ID_NEW : MOQ_REQUEST_ID_TOO_FAR_AHEAD;
    }
    for (size_t i = 0; i < w->gap_count; i++)
        if (w->gaps[i] == id) return MOQ_REQUEST_ID_NEW;
    return MOQ_REQUEST_ID_DUPLICATE;
}

/* Record an id that classified as NEW. */
static inline void moq_request_id_window_commit(moq_request_id_window_t *w,
                                                uint64_t id)
{
    if (id == w->next) { w->next += 2; return; }
    if (id > w->next) {
        for (uint64_t x = w->next; x < id; x += 2)
            w->gaps[w->gap_count++] = x;   /* bounded by classify() */
        w->next = id + 2;
        return;
    }
    for (size_t i = 0; i < w->gap_count; i++) {
        if (w->gaps[i] == id) {
            w->gaps[i] = w->gaps[--w->gap_count];   /* order is irrelevant */
            return;
        }
    }
}

#endif /* MOQ_REQUEST_ID_WINDOW_H */
