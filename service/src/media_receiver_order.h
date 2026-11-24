#ifndef MOQ_MEDIA_RECEIVER_ORDER_H
#define MOQ_MEDIA_RECEIVER_ORDER_H

/*
 * The receiver's in-group ordering stage (internal; never exported).
 *
 * An MSF-02 "loc" track whose streamMapping is "object", or a value this
 * receiver does not recognize, may carry each Object of a Group on its own
 * subgroup stream (MSF-02 4.1.1), so Objects of one Group can arrive in any
 * order, and Objects of later Groups can arrive before an earlier Group's
 * last ones. LOC playback treats Object order as decode order, so the
 * receiver restores it here, on the live subscription only: a fill or a
 * Joining FETCH is one ordered stream and bypasses the stage.
 *
 * The stage holds opaque items (the caller's held Objects) and releases
 * them in (Group, Object) order. It keeps, per open Group, the facts it
 * knows about that Group, in a bounded table of open-group records sorted
 * by Group ID; the lowest is the current Group, the one being released.
 * Facts are:
 *   - a held Object (Group g, Object k);
 *   - fin_last: a FIN on the subgroup whose header carried END_OF_GROUP,
 *     with its last Object: no Object of g above it exists (MoQT
 *     draft-22 section 10.4.2; drafts 16/18 alike);
 *   - eog_marker: an END_OF_GROUP status Object at e: no Object of g at or
 *     above e exists; the status Object itself is held at position e and
 *     released in order;
 *   - a loss: a RESET whose cut Object the facade names; that Object of g
 *     was not delivered.
 * A record opens on the first fact about its Group and retires only when
 * that Group retires: completed (contiguous release reached its final
 * position), given up by the window, or by the capacity policy. Retiring
 * Group G never clears or reuses the facts of a later Group.
 *
 * Group order of the live origin is ascending (MSF-02 6.1). Every Group up
 * to the retired-group floor is closed: Objects, terminals and losses for
 * it are refused or ignored, even once the table is empty. The floor rises
 * to each Group that retires and, once the current Group's frontier first
 * moves, to the Group just below it, since an earlier Group could no longer
 * be released in order; until then a Group below the current one may still
 * open a record and become current. A Group whose record is complete is
 * closed as well, retired yet or not. The floor has its own presence bit,
 * so Group 0 is representable, and is cleared only by receiver_order_reset
 * (a new subscription generation).
 *
 * Release (current record r): while r's frontier position is held, known
 * absent or lost, release it or skip it (absent: no gap; lost: the next
 * released item reports the gap). Release of ID UINT64_MAX sets exhausted
 * instead of advancing; nothing at or above an exhausted frontier is ever
 * released again. r is complete when its frontier is exhausted (no position
 * is left) or has passed its final position (eog_marker when known, else
 * fin_last). When r is complete it retires and the next record becomes
 * current, frontier 0, with its own retained facts, at once.
 *
 * Waiting (r incomplete, its frontier not releasable), two mutually
 * exclusive cases, each with one deadline armed once per stall
 * (stall start + window_us). A stall is the current Group waiting at one
 * frontier: it starts at the now_us of the call that first finds it
 * waiting, and moving the frontier or changing the current Group ends it:
 *   - gap wait: r knows a position after its frontier (a held, lost or
 *     absent ID, or a known final position, held payload or not). On
 *     expiry the run from the frontier to that next known position (or to
 *     the final position) is given up: the next released item reports it as
 *     a gap and gaps_total adds its length.
 *   - boundary wait: r knows nothing after its frontier and has no final
 *     position, and a later record exists. r may still have in-flight tail
 *     Objects on other streams, so it is never cut over at once. On expiry r
 *     retires without reporting loss; the first item released after it
 *     reports gap_tail_unknown at r's frontier.
 *   - otherwise idle: no deadline.
 *
 * Capacity: max_slots counts held items and recorded losses together;
 * max_groups bounds the table. An intake that needs a slot or a record
 * when none is free returns MOQ_ERR_BUFFER; the caller then calls
 * receiver_order_force, polls what it released, and retries. Each force
 * step either frees a slot (a loss skipped, or a held item made due for the
 * next poll) or retires a record (it applies the expiry step to the current
 * record now, repeating within that record until one of the two happens; an
 * idle current record retires as on a boundary-wait expiry), and counts in
 * forced_total; a later Group's facts are never discarded to make room:
 * only the head retires. Force returns MOQ_DONE when no step can make room
 * (no record is open, the stage is finishing, or the current record's
 * frontier item is already due, which a poll and not a force frees), and
 * the caller refuses the incoming fact. So every MOQ_OK is one counted step
 * and a caller that cannot poll still stops.
 *
 * Arithmetic is checked: the frontier never wraps (exhausted), absent
 * ranges are formed only when fin_last < eog_marker, runs never wrap, and
 * the totals saturate.
 *
 * Not thread-safe: the receiver drives it from its network thread only. The
 * stage never calls back into the receiver, allocates only in
 * receiver_order_create, and never touches an item's contents.
 */

#include <moq/types.h>

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/* Whatever the mapping ("object", or one not recognized), only the cut
 * Object a RESET names is known lost: with one Object per subgroup a RESET
 * without a cut Object loses nothing, and of an unknown layout nothing more
 * can be inferred. The caller reports only cut Objects. */
typedef struct receiver_order_cfg {
    uint32_t max_groups;     /* open-group records; >= 1 */
    uint32_t max_slots;      /* held items plus recorded losses; >= 1 */
    uint64_t window_us;      /* how long a stall may wait */
} receiver_order_cfg_t;

typedef struct receiver_order receiver_order_t;

/* One released item, or the report a skipped position carries forward. */
typedef struct receiver_order_delivery {
    void    *ctx;            /* the item as offered */
    uint64_t group;
    uint64_t object;
    bool     is_status;      /* offered with receiver_order_offer_status */
    /* Loss before this item, since the previous released item: gap_group /
     * gap_object is where the earliest unreported run starts (a run given
     * up by the window, the capacity policy or finish, or a lost Object),
     * and gap_tail_unknown says that run is only the unconfirmed tail of an
     * earlier Group that retired without a final position (by the window,
     * the capacity policy or finish), not a known loss. A known
     * loss outranks an unconfirmed tail: when both are unreported, the
     * earliest known loss is reported and gap_tail_unknown is false. With
     * gap false, gap_group, gap_object and gap_tail_unknown are zero. */
    bool     gap;
    uint64_t gap_group;
    uint64_t gap_object;
    bool     gap_tail_unknown;
} receiver_order_delivery_t;

/* Allocate a stage through `alloc` (copied; it must stay valid until
 * destroy) in one allocation. MOQ_OK; MOQ_ERR_INVAL for NULL arguments (an
 * allocator without alloc or free included), a zero max_groups/max_slots,
 * or tables whose size does not fit size_t; MOQ_ERR_NOMEM. *out is NULL
 * on failure. */
moq_result_t receiver_order_create(const moq_alloc_t *alloc,
                                   const receiver_order_cfg_t *cfg,
                                   receiver_order_t **out);

/* Free the stage. Every item still held is passed to `release` first (in no
 * particular order), when it is not NULL. NULL-safe. */
void receiver_order_destroy(receiver_order_t *o,
                            void (*release)(void *ctx, void *item),
                            void *release_ctx);

/* A new subscription generation: everything held is passed to `release`
 * (when it is not NULL), every record, the retired-group floor, the
 * unreported gap, the armed stall and finish are cleared, and the totals
 * are kept. */
void receiver_order_reset(receiver_order_t *o,
                          void (*release)(void *ctx, void *item),
                          void *release_ctx);

/* Hold a NORMAL Object (g, k) carrying `item`.
 *   MOQ_OK          held (the stage owns the item until it is polled);
 *   MOQ_ERR_PROTO   refused, not held: g is closed (up to the floor, or
 *                   complete), or g is current and k is below its frontier
 *                   or the frontier is exhausted, or k is already held, or
 *                   k is above g's known final position (above fin_last,
 *                   or at or above eog_marker: a publisher inconsistency),
 *                   or the stage is finishing;
 *   MOQ_ERR_BUFFER  no free slot, or no free record for a new Group:
 *                   force, poll, retry;
 *   MOQ_ERR_INVAL   NULL arguments (item included).
 * An Object at a position recorded lost is held in place of the loss, in
 * the loss's slot. */
moq_result_t receiver_order_offer(receiver_order_t *o, uint64_t now_us,
                                  uint64_t group, uint64_t object, void *item);

/* Hold an END_OF_GROUP status Object at (g, e) carrying `item`: the
 * eog_marker fact plus the status item at position e. Results as
 * receiver_order_offer; a second marker of another value, one at or
 * below fin_last, or one below an Object or loss g already holds is a
 * publisher inconsistency: MOQ_ERR_PROTO, the first fact stands. */
moq_result_t receiver_order_offer_status(receiver_order_t *o, uint64_t now_us,
                                         uint64_t group, uint64_t end_object,
                                         void *item);

/* The END_OF_GROUP subgroup of g ended with a FIN after Object `last`.
 *   MOQ_OK          recorded (also when the same fin_last is known);
 *   MOQ_DONE        ignored: g closed or its frontier exhausted, a
 *                   different fin_last already known, last >= a known
 *                   eog_marker, last below what g already released, g
 *                   holding an Object above last or a loss at or above it,
 *                   or the stage finishing;
 *   MOQ_ERR_BUFFER  no free record for a new Group: force, poll, retry;
 *   MOQ_ERR_INVAL   NULL o. */
moq_result_t receiver_order_fin(receiver_order_t *o, uint64_t now_us,
                                uint64_t group, uint64_t last);

/* A RESET of a subgroup of g that cut Object `cut` short: that Object was
 * not delivered. Results as receiver_order_fin (ignored also for a cut
 * below g's frontier, at or above its final position (fin_last, or
 * eog_marker), or already held or recorded); MOQ_ERR_BUFFER also when no
 * slot is free for the loss. */
moq_result_t receiver_order_loss(receiver_order_t *o, uint64_t now_us,
                                 uint64_t group, uint64_t cut);

/* One capacity step (see the top): MOQ_OK when it freed a slot or retired
 * a record, MOQ_DONE when nothing can be freed, MOQ_ERR_INVAL for NULL o. */
moq_result_t receiver_order_force(receiver_order_t *o, uint64_t now_us);

/* The next item due at now_us, in order: MOQ_OK with *out (the stage no
 * longer holds the item), MOQ_DONE when none is due, MOQ_ERR_INVAL for
 * NULL arguments. *out is written only on MOQ_OK. Applies every expiry
 * due at now_us first. */
moq_result_t receiver_order_poll(receiver_order_t *o, uint64_t now_us,
                                 receiver_order_delivery_t *out);

/* The subscription ended: every held item is due, Group by Group in order,
 * and positions still missing are reported as gaps (and counted in
 * gaps_total); a Group left with no final position retires with its tail
 * unconfirmed. Later intake is refused until receiver_order_reset. */
void receiver_order_finish(receiver_order_t *o);

/* When poll next makes progress without new input: the armed stall's
 * deadline (saturating), UINT64_MAX when no stall is armed: idle,
 * finishing, or the next step already due, which poll takes at any
 * now_us. */
uint64_t receiver_order_next_deadline_us(const receiver_order_t *o);

/* Whether any item is held (also those due but not yet polled). */
bool receiver_order_holds(const receiver_order_t *o);

/* Calls fn for every held item (in no particular order). */
void receiver_order_for_each(const receiver_order_t *o,
                             void (*fn)(void *ctx, void *item), void *ctx);

/* Saturating totals since create: positions given up (by the window, the
 * capacity policy or finish; a lost Object is reported, not counted, and
 * an unconfirmed tail has no length) and capacity steps taken. */
uint64_t receiver_order_gaps_total(const receiver_order_t *o);
uint64_t receiver_order_forced_total(const receiver_order_t *o);

#endif /* MOQ_MEDIA_RECEIVER_ORDER_H */
