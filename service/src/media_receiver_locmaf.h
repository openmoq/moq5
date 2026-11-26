#ifndef MOQ_MEDIA_RECEIVER_LOCMAF_H
#define MOQ_MEDIA_RECEIVER_LOCMAF_H

/*
 * The receiver's LOCMAF stage (internal; never exported).
 *
 * A LOCMAF Object (draft-einarsson-moq-locmaf-01) is a delta against the
 * previous Object of its MOQT Group, so it is decoded by a decoder that saw
 * that Object, and the canonical CMAF chunk it encodes (section 15) is what
 * the application receives. This stage decodes the live subscription of one
 * LOCMAF track; each fill of the track has a decoder of its own
 * (receiver_lm_dec_t), driven by the caller.
 *
 * Groups (section 3): every Object of a Group is on one subgroup, so each
 * Group arrives on its own stream, and MoQT orders no two Groups. The stage
 * keeps RECEIVER_LM_LIVE_GROUPS open Groups, each in a slot with its own
 * decoder, preallocated at create. An Object is routed to its Group's slot;
 * a Group already closed (retired, reset and reclaimed, evicted or refused)
 * is late. A Group that finds no free slot reclaims a reset one, else evicts
 * the open Group with the lowest ID when it is higher than that, else is
 * refused. A ring of the last RECEIVER_LM_CLOSED_GROUPS closed Groups and a
 * floor below which every Group is closed keep a Group from reopening.
 *
 * Each slot is one of:
 *   BOOTSTRAP  the subscription's first Group, joined by a delta: its deltas
 *              are held (at most RECEIVER_LM_HOLD, each for at most
 *              RECEIVER_LM_HOLD_US) until a fill of the same subscription
 *              holds the chain they continue, which it then hands on;
 *   RUNNING    a full header anchors the slot's decoder, unbroken since;
 *   RAW        the last Object was a rawBoxes Object: delivered, but no
 *              reference, so a delta is refused (section 9.3);
 *   BARRIER    loss was detected (a gap, a reset stream, a datagram, a
 *              second subgroup, a refused Object): deltas are dropped until
 *              a full header (RUNNING) or a rawBoxes Object (RAW) resumes
 *              the Group (section 3).
 * A hand-off happens only in BOOTSTRAP, from a fill decoder that decoded a
 * full header in the Group and every Object after it, up to the first held
 * one; never after detected loss.
 *
 * The stage takes ownership of each Object (an item) and gives every item
 * back exactly once as an output, in order: REBUILT with its canonical
 * chunk, LOSS (dropped: a loss at its Location), SILENT (dropped without
 * loss: a duplicate, an Object of another subscription, or one a fill
 * already delivered) or STATUS (an END_OF_GROUP or END_OF_TRACK status
 * Object, released behind the Objects held before it). Counts move as the
 * outputs are produced. Not thread-safe; the network thread owns it.
 */

#include <moq/locmaf.h>
#include <moq/rcbuf.h>
#include <moq/subscriber.h>
#include <moq/types.h>

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#define RECEIVER_LM_LIVE_GROUPS   2u        /* open live Groups per track */
#define RECEIVER_LM_CLOSED_GROUPS 8u        /* recently closed Groups remembered */
#define RECEIVER_LM_HOLD          64u       /* Objects held for a hand-off */
#define RECEIVER_LM_HOLD_US       300000u   /* how long each is held */
#define RECEIVER_LM_MAX_FILLS     32u       /* fill decoders offered at once */
/* Every item the stage holds or has given back and not yet taken: the
 * held ones, the parked status Objects and the one being taken in. */
#define RECEIVER_LM_OUT (RECEIVER_LM_HOLD + RECEIVER_LM_LIVE_GROUPS + 2u)

/* One decoder over its own storage, the Location it decodes next, and the
 * buffer its canonical chunks are written in. */
typedef struct receiver_lm_dec {
    moq_locmaf_decoder_t dec;
    moq_locmaf_track_t   track;
    void                *storage;
    size_t               storage_size;
    bool                 has_next;    /* set after each decoded Object */
    uint64_t             next_group, next_object;
    uint8_t             *scratch;
    size_t               scratch_cap;
} receiver_lm_dec_t;

moq_result_t receiver_lm_dec_create(const moq_alloc_t *alloc,
                                    const moq_locmaf_track_t *track,
                                    receiver_lm_dec_t **out);
void receiver_lm_dec_destroy(const moq_alloc_t *alloc, receiver_lm_dec_t *d);
void receiver_lm_dec_reset(receiver_lm_dec_t *d);
/* moq_locmaf_decoder_push, keeping the next Location. */
moq_result_t receiver_lm_dec_push(receiver_lm_dec_t *d, uint64_t group,
                                  uint64_t object, moq_bytes_t payload,
                                  moq_locmaf_chunk_t *out,
                                  moq_locmaf_reason_t *reason);

/* The canonical CMAF chunk of the chunk the decoder just decoded, as a new
 * buffer: written in the decoder's scratch (grown on demand), then copied.
 * MOQ_ERR_NOMEM, MOQ_ERR_INVAL. */
moq_result_t receiver_lm_rebuild(const moq_alloc_t *alloc, receiver_lm_dec_t *d,
                                 const moq_locmaf_chunk_t *chunk,
                                 moq_rcbuf_t **out);

/* Whether a decoder's result is a refusal of the Object itself (malformed,
 * not validatable, or over the limits) rather than a missing reference. */
bool receiver_lm_refused(moq_result_t rc);

/* One Object the stage was given. */
typedef struct receiver_lm_item {
    moq_sub_object_t so;     /* owns its payload and properties refs */
    uint64_t         seq;    /* the caller's ordering key */
    uint64_t         deadline_us;
    bool             held;   /* it waited in the stage beyond its intake */
} receiver_lm_item_t;

enum {
    RECEIVER_LM_OUT_REBUILT = 1,
    RECEIVER_LM_OUT_LOSS,
    RECEIVER_LM_OUT_SILENT,
    RECEIVER_LM_OUT_STATUS,
};

typedef struct receiver_lm_out {
    uint32_t           kind;
    receiver_lm_item_t item;
    moq_rcbuf_t       *rebuilt;      /* REBUILT: the canonical chunk, owned */
    bool               parse_drop;   /* LOSS of a malformed Object */
    /* REBUILT or LOSS: Objects of its Group before it never arrived (an
     * Object ID gap, section 3); gap_object is the first of them. */
    bool               gap;
    uint64_t           gap_object;
} receiver_lm_out_t;

typedef struct receiver_lm_counts {
    uint64_t rebuilt, undecodable, expired, late, groups_evicted, resets;
} receiver_lm_counts_t;

/* The fills that may hand a chain on: decoders of this subscription's
 * fills, and whether one of them can still deliver. */
typedef struct receiver_lm_fills {
    const receiver_lm_dec_t *dec[RECEIVER_LM_MAX_FILLS];
    size_t                   count;
    bool                     possible;
} receiver_lm_fills_t;

typedef enum {
    LM_SLOT_FREE = 0,
    LM_SLOT_BOOTSTRAP,
    LM_SLOT_RUNNING,
    LM_SLOT_RAW,
    LM_SLOT_BARRIER,
} receiver_lm_slot_state_t;

typedef struct receiver_lm receiver_lm_t;

/* A stage with both slots' decoders. MOQ_ERR_NOMEM, MOQ_ERR_INVAL. */
moq_result_t receiver_lm_create(const moq_alloc_t *alloc,
                                const moq_locmaf_track_t *track,
                                receiver_lm_t **out);
/* Free it; each item still in it, outputs included, goes to release. */
void receiver_lm_destroy(receiver_lm_t *lm,
                         void (*release)(void *ctx, receiver_lm_item_t *item),
                         void *ctx);

/* A NORMAL live Object; *item passes to the stage. True when the stage
 * kept it beyond this call (held or parked, and so `held` in its output),
 * even if it gave it back before returning. */
bool receiver_lm_object(receiver_lm_t *lm, uint64_t now_us,
                        const receiver_lm_fills_t *fills,
                        receiver_lm_item_t *item);
/* An END_OF_GROUP or END_OF_TRACK status Object; *item passes. True as
 * for receiver_lm_object. */
bool receiver_lm_status(receiver_lm_t *lm, receiver_lm_item_t *item);
/* A terminal of a live subgroup stream (MOQ_SUB_STREAM_*). */
void receiver_lm_terminal(receiver_lm_t *lm, uint32_t kind, uint64_t group,
                          uint64_t subgroup);
/* A fill progressed, or time passed: hand-off and hold expiry. */
void receiver_lm_progress(receiver_lm_t *lm, uint64_t now_us,
                          const receiver_lm_fills_t *fills);
/* The application disabled the track: every slot's decoder resets and the
 * slot is freed, and what is held or parked goes out (a parked END_OF_TRACK
 * still ends the track). The Objects that arrive meanwhile are dropped
 * before the stage, so a Group resumes at its next full header. */
void receiver_lm_disable(receiver_lm_t *lm);
/* The subscription is over: held Objects no fill can continue are given
 * up, and every slot closes after its parked END_OF_GROUP. */
void receiver_lm_finish(receiver_lm_t *lm, const receiver_lm_fills_t *fills);

/* The earliest hold deadline, or UINT64_MAX. */
uint64_t receiver_lm_deadline(const receiver_lm_t *lm);
/* Whether it holds or has parked anything. */
bool receiver_lm_holds(const receiver_lm_t *lm);
/* Whether it holds, has parked or has given back untaken an item whose seq
 * is below `barrier`. */
bool receiver_lm_holds_before(const receiver_lm_t *lm, uint64_t barrier);

/* The next output, or NULL; receiver_lm_pop takes it (the caller then owns
 * its item and rebuilt buffer). */
receiver_lm_out_t *receiver_lm_peek(receiver_lm_t *lm);
void receiver_lm_pop(receiver_lm_t *lm);
/* The outputs not yet taken, oldest first. */
size_t receiver_lm_out_count(const receiver_lm_t *lm);
receiver_lm_out_t *receiver_lm_out_at(receiver_lm_t *lm, size_t i);

receiver_lm_counts_t receiver_lm_counts(const receiver_lm_t *lm);
/* The state of the slot holding `group`, or LM_SLOT_FREE. */
receiver_lm_slot_state_t receiver_lm_slot_state(const receiver_lm_t *lm,
                                                uint64_t group);

#endif /* MOQ_MEDIA_RECEIVER_LOCMAF_H */
