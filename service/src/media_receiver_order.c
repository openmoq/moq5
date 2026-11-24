/*
 * The receiver's in-group ordering stage (contract: media_receiver_order.h).
 *
 * One allocation holds the stage, its open-group records (sorted by Group
 * ID, the current Group first) and its slots. A slot is a held item or a
 * recorded loss keyed by (Group, Object), not by record, so records shift
 * as Groups retire without touching slots. Used slots are packed at the
 * front, so a lookup scans only those and freeing one is O(1).
 *
 * Every loop is bounded by the table or slot count: scans by their sizes,
 * and the release loops of poll and force by loop_bound, which counts the
 * steps one call can take (each one retires a record, skips a loss or an
 * absent range, delivers an item, or gives up a run that is followed by
 * one of those).
 */
#include "media_receiver_order.h"

#include <string.h>

typedef enum order_slot_kind {
    ORDER_SLOT_HELD = 1,    /* a NORMAL Object */
    ORDER_SLOT_STATUS,      /* the END_OF_GROUP status Object at eog */
    ORDER_SLOT_LOST         /* a cut Object: no item */
} order_slot_kind_t;

typedef struct order_slot {
    uint64_t group;
    uint64_t object;
    void    *item;          /* NULL for a loss */
    uint8_t  kind;          /* order_slot_kind_t */
} order_slot_t;

typedef struct order_record {
    uint64_t group;
    uint64_t frontier;      /* next position to release; 0 until current */
    uint64_t fin_last;      /* valid when has_fin */
    uint64_t eog;           /* eog_marker; valid when has_eog */
    uint32_t slots;         /* this Group's held items and losses */
    bool     exhausted;     /* position UINT64_MAX was consumed */
    bool     has_fin;
    bool     has_eog;
} order_record_t;

typedef enum order_wait {
    ORDER_IDLE,
    ORDER_GAP_WAIT,
    ORDER_BOUNDARY_WAIT
} order_wait_t;

struct receiver_order {
    moq_alloc_t          alloc;
    size_t               alloc_size;
    receiver_order_cfg_t cfg;
    order_record_t      *rec;           /* cfg.max_groups; rec_count used */
    order_slot_t        *slot;          /* cfg.max_slots; slots_used used */
    uint32_t             rec_count;
    uint32_t             slots_used;
    uint32_t             items_held;    /* HELD and STATUS slots */
    uint64_t             loop_bound;

    /* Generation-scoped closed state, kept apart from the table so that it
     * outlives it; has_floor makes Group 0 representable. */
    bool                 has_floor;
    uint64_t             floor;         /* every Group <= floor is closed */

    /* What the next released item reports. Runs are noted in release
     * order, which is ascending, so the first one noted is the earliest. */
    bool                 loss_pending;
    uint64_t             loss_group;
    uint64_t             loss_object;
    bool                 tail_pending;
    uint64_t             tail_group;
    uint64_t             tail_object;

    /* The armed stall: the current Group waiting at one frontier. */
    bool                 stall_armed;
    uint64_t             stall_group;
    uint64_t             stall_frontier;
    uint64_t             stall_deadline;

    bool                 finishing;
    uint64_t             gaps_total;
    uint64_t             forced_total;
};

/* -- Arithmetic and layout --------------------------------------------- */

static uint64_t sat_add(uint64_t a, uint64_t b)
{
    return b > UINT64_MAX - a ? UINT64_MAX : a + b;
}

/* count * elem fits in size_t. Taking the count as uint64_t keeps the check
 * meaningful on a 32-bit size_t without a tautological comparison on LP64. */
static bool order_product_fits(uint64_t count, size_t elem)
{
    return count <= (uint64_t)(SIZE_MAX / elem);
}

/* Round *x up to a multiple of a (a power of two); false if it overflows. */
static bool order_align_up(size_t *x, size_t a)
{
    size_t r = *x & (a - 1);
    if (r == 0) return true;
    if (*x > SIZE_MAX - (a - r)) return false;
    *x += a - r;
    return true;
}

/* The stage, then its records, then its slots, each aligned: false when
 * the total does not fit size_t. */
static bool order_layout(const receiver_order_cfg_t *cfg, size_t *rec_off,
                         size_t *slot_off, size_t *total)
{
    size_t off = sizeof(receiver_order_t);
    if (!order_align_up(&off, _Alignof(order_record_t))) return false;
    *rec_off = off;
    if (!order_product_fits(cfg->max_groups, sizeof(order_record_t)))
        return false;
    size_t rec_bytes = (size_t)cfg->max_groups * sizeof(order_record_t);
    if (rec_bytes > SIZE_MAX - off) return false;
    off += rec_bytes;
    if (!order_align_up(&off, _Alignof(order_slot_t))) return false;
    *slot_off = off;
    if (!order_product_fits(cfg->max_slots, sizeof(order_slot_t)))
        return false;
    size_t slot_bytes = (size_t)cfg->max_slots * sizeof(order_slot_t);
    if (slot_bytes > SIZE_MAX - off) return false;
    *total = off + slot_bytes;
    return true;
}

/* -- Floor ------------------------------------------------------------- */

static bool order_closed(const receiver_order_t *o, uint64_t g)
{
    return o->has_floor && g <= o->floor;
}

static void order_close_through(receiver_order_t *o, uint64_t g)
{
    if (!o->has_floor || g > o->floor) {
        o->floor = g;
        o->has_floor = true;
    }
}

/* The current Group's frontier moved: an earlier Group could no longer be
 * released in order. */
static void order_close_below(receiver_order_t *o, uint64_t g)
{
    if (g > 0) order_close_through(o, g - 1);
}

/* -- Records ----------------------------------------------------------- */

static order_record_t *order_record(receiver_order_t *o, uint64_t g)
{
    for (uint32_t i = 0; i < o->rec_count; i++) {
        if (o->rec[i].group == g) return &o->rec[i];
        if (o->rec[i].group > g) break;
    }
    return NULL;
}

/* eog_marker when known, else fin_last; meaningful with either. */
static uint64_t order_final(const order_record_t *r)
{
    return r->has_eog ? r->eog : r->fin_last;
}

/* No position is left: exhausted, or past the final position. With a final
 * position of UINT64_MAX only exhausted can hold, so nothing overflows. */
static bool order_complete(const order_record_t *r)
{
    if (r->exhausted) return true;
    return (r->has_fin || r->has_eog) && r->frontier > order_final(r);
}

/* Known not to exist: fin_last < k < eog_marker. The range is formed only
 * with both facts, whose intake keeps fin_last < eog_marker. */
static bool order_absent(const order_record_t *r, uint64_t k)
{
    return r->has_fin && r->has_eog && k > r->fin_last && k < r->eog;
}

/* A Group that takes no new fact: up to the floor, or with a complete
 * record, retired yet or not. */
static bool order_group_closed(const receiver_order_t *o, uint64_t g,
                               const order_record_t *r)
{
    return order_closed(o, g) || (r && order_complete(r));
}

/* Precondition: a free record. The new record keeps the table sorted; it
 * may become current only while the current Group has not moved, since
 * any lower Group is closed once it has. */
static order_record_t *order_open(receiver_order_t *o, uint64_t g)
{
    uint32_t i = 0;
    while (i < o->rec_count && o->rec[i].group < g) i++;
    memmove(&o->rec[i + 1], &o->rec[i],
            (size_t)(o->rec_count - i) * sizeof(o->rec[0]));
    o->rec_count++;
    order_record_t *r = &o->rec[i];
    memset(r, 0, sizeof(*r));
    r->group = g;
    return r;
}

/* The current record retires: it holds no slot by then (complete, or a
 * boundary wait, which holds nothing), and the next becomes current with
 * its own facts. */
static void order_retire_head(receiver_order_t *o)
{
    order_close_through(o, o->rec[0].group);
    o->rec_count--;
    memmove(&o->rec[0], &o->rec[1], (size_t)o->rec_count * sizeof(o->rec[0]));
    o->stall_armed = false;
}

/* -- Slots ------------------------------------------------------------- */

/* The slot at (g, k), or slots_used when there is none. */
static uint32_t order_find(const receiver_order_t *o, uint64_t g, uint64_t k)
{
    uint32_t i = 0;
    while (i < o->slots_used &&
           (o->slot[i].group != g || o->slot[i].object != k))
        i++;
    return i;
}

/* Whether g holds an Object or a loss above k, or a loss at k when
 * loss_at_k: facts a terminal at k would contradict. */
static bool order_contradicts(const receiver_order_t *o, uint64_t g,
                              uint64_t k, bool loss_at_k)
{
    for (uint32_t i = 0; i < o->slots_used; i++) {
        const order_slot_t *s = &o->slot[i];
        if (s->group != g || s->kind == ORDER_SLOT_STATUS) continue;
        if (s->object > k) return true;
        if (loss_at_k && s->kind == ORDER_SLOT_LOST && s->object == k)
            return true;
    }
    return false;
}

/* Precondition: a free slot. */
static void order_slot_add(receiver_order_t *o, order_record_t *r,
                           uint64_t k, void *item, order_slot_kind_t kind)
{
    order_slot_t *s = &o->slot[o->slots_used++];
    s->group = r->group;
    s->object = k;
    s->item = item;
    s->kind = (uint8_t)kind;
    r->slots++;
    if (kind != ORDER_SLOT_LOST) o->items_held++;
}

static void order_slot_free(receiver_order_t *o, order_record_t *r,
                            uint32_t i)
{
    if (o->slot[i].kind != ORDER_SLOT_LOST) o->items_held--;
    r->slots--;
    o->slots_used--;
    o->slot[i] = o->slot[o->slots_used];
}

static void order_release_all(receiver_order_t *o,
                              void (*release)(void *ctx, void *item),
                              void *release_ctx)
{
    if (!release) return;
    for (uint32_t i = 0; i < o->slots_used; i++)
        if (o->slot[i].kind != ORDER_SLOT_LOST)
            release(release_ctx, o->slot[i].item);
}

/* -- Release ----------------------------------------------------------- */

/* Past position k, the current record's frontier: releasing or skipping
 * UINT64_MAX exhausts the frontier instead of wrapping it. */
static void order_advance_past(receiver_order_t *o, order_record_t *r,
                               uint64_t k)
{
    if (k == UINT64_MAX) {
        r->exhausted = true;
        r->frontier = UINT64_MAX;
    } else {
        r->frontier = k + 1;
    }
    order_close_below(o, r->group);
}

static void order_note_loss(receiver_order_t *o, uint64_t g, uint64_t k)
{
    if (o->loss_pending) return;
    o->loss_pending = true;
    o->loss_group = g;
    o->loss_object = k;
}

static void order_note_tail(receiver_order_t *o, uint64_t g, uint64_t k)
{
    if (o->tail_pending) return;
    o->tail_pending = true;
    o->tail_group = g;
    o->tail_object = k;
}

static void order_skip_lost(receiver_order_t *o, order_record_t *r,
                            uint32_t i)
{
    uint64_t k = o->slot[i].object;
    order_slot_free(o, r, i);
    order_note_loss(o, r->group, k);
    order_advance_past(o, r, k);
}

/* The frontier sits in fin_last < k < eog_marker: jump to the marker.
 * Absent positions are not loss. */
static void order_skip_absent(receiver_order_t *o, order_record_t *r)
{
    r->frontier = r->eog;
    order_close_below(o, r->group);
}

static void order_deliver(receiver_order_t *o, order_record_t *r,
                          uint32_t i, receiver_order_delivery_t *out)
{
    const order_slot_t *s = &o->slot[i];
    memset(out, 0, sizeof(*out));
    out->ctx = s->item;
    out->group = s->group;
    out->object = s->object;
    out->is_status = s->kind == ORDER_SLOT_STATUS;
    /* A known loss outranks an unconfirmed tail behind it. */
    if (o->loss_pending) {
        out->gap = true;
        out->gap_group = o->loss_group;
        out->gap_object = o->loss_object;
    } else if (o->tail_pending) {
        out->gap = true;
        out->gap_group = o->tail_group;
        out->gap_object = o->tail_object;
        out->gap_tail_unknown = true;
    }
    o->loss_pending = false;
    o->tail_pending = false;
    uint64_t k = s->object;
    order_slot_free(o, r, i);
    order_advance_past(o, r, k);
}

/* The current record can make progress without the window: complete, or
 * its frontier held, lost or absent. */
static bool order_head_ready(const receiver_order_t *o)
{
    const order_record_t *r = &o->rec[0];
    return order_complete(r) || order_absent(r, r->frontier) ||
           order_find(o, r->group, r->frontier) < o->slots_used;
}

/* For a current record that is not ready. A gap wait knows a position
 * after the frontier: a slot (every slot of r is above its frontier), or a
 * final position. A boundary wait knows nothing after it, and a later
 * record exists. The two never hold at once. */
static order_wait_t order_wait_kind(const receiver_order_t *o,
                                    const order_record_t *r)
{
    if (r->slots > 0 || r->has_fin || r->has_eog) return ORDER_GAP_WAIT;
    return o->rec_count > 1 ? ORDER_BOUNDARY_WAIT : ORDER_IDLE;
}

/* Arm the stall when the current record has just started waiting at its
 * frontier; disarm when nothing waits or the next step is already due. */
static void order_refresh(receiver_order_t *o, uint64_t now_us)
{
    if (o->finishing || o->rec_count == 0 || order_head_ready(o) ||
        order_wait_kind(o, &o->rec[0]) == ORDER_IDLE) {
        o->stall_armed = false;
        return;
    }
    const order_record_t *r = &o->rec[0];
    if (o->stall_armed && o->stall_group == r->group &&
        o->stall_frontier == r->frontier)
        return;
    o->stall_armed = true;
    o->stall_group = r->group;
    o->stall_frontier = r->frontier;
    o->stall_deadline = sat_add(now_us, o->cfg.window_us);
}

/* Gap-wait expiry: give up the run from the frontier to the next known
 * position, or through the final position when only that is known. False
 * when nothing after the frontier is known (not a gap wait). */
static bool order_give_up_run(receiver_order_t *o, order_record_t *r)
{
    uint64_t pos = r->frontier;
    bool have_next = false;
    uint64_t next = 0;
    for (uint32_t i = 0; i < o->slots_used; i++) {
        const order_slot_t *s = &o->slot[i];
        if (s->group == r->group && s->object > pos &&
            (!have_next || s->object < next)) {
            next = s->object;
            have_next = true;
        }
    }
    /* With both terminals, fin_last + 1 is known (the first absent
     * position, or the marker); fin_last < eog_marker, so it cannot wrap. */
    if (r->has_fin && r->has_eog && r->fin_last >= pos &&
        (!have_next || r->fin_last + 1 < next)) {
        next = r->fin_last + 1;
        have_next = true;
    }
    uint64_t len;
    if (have_next) {
        len = next - pos;                  /* next > pos */
        r->frontier = next;
    } else if (r->has_fin && r->fin_last >= pos) {
        uint64_t span = r->fin_last - pos;
        len = span == UINT64_MAX ? UINT64_MAX : span + 1;
        if (r->fin_last == UINT64_MAX) {
            r->exhausted = true;
            r->frontier = UINT64_MAX;
        } else {
            r->frontier = r->fin_last + 1;
        }
    } else {
        return false;
    }
    o->gaps_total = sat_add(o->gaps_total, len);
    order_note_loss(o, r->group, pos);
    order_close_below(o, r->group);
    return true;
}

/* The expiry step for the current record, which is waiting (or idle, when
 * forced or finishing). A gap wait gives up its run; otherwise the record
 * retires without reporting loss, and what may follow its frontier is
 * reported as an unconfirmed tail. */
static void order_expire(receiver_order_t *o)
{
    order_record_t *r = &o->rec[0];
    o->stall_armed = false;
    if (order_wait_kind(o, r) == ORDER_GAP_WAIT && order_give_up_run(o, r))
        return;
    order_note_tail(o, r->group, r->frontier);
    order_retire_head(o);
}

/* -- Public ------------------------------------------------------------ */

moq_result_t receiver_order_create(const moq_alloc_t *alloc,
                                   const receiver_order_cfg_t *cfg,
                                   receiver_order_t **out)
{
    if (out) *out = NULL;
    if (!alloc || !alloc->alloc || !alloc->free || !cfg || !out ||
        cfg->max_groups == 0 || cfg->max_slots == 0)
        return MOQ_ERR_INVAL;
    size_t rec_off, slot_off, total;
    if (!order_layout(cfg, &rec_off, &slot_off, &total)) return MOQ_ERR_INVAL;
    unsigned char *block = alloc->alloc(total, alloc->ctx);
    if (!block) return MOQ_ERR_NOMEM;
    memset(block, 0, total);
    receiver_order_t *o = (receiver_order_t *)(void *)block;
    o->alloc = *alloc;
    o->alloc_size = total;
    o->cfg = *cfg;
    o->rec = (order_record_t *)(void *)(block + rec_off);
    o->slot = (order_slot_t *)(void *)(block + slot_off);
    /* Retirements, loss skips and absent skips, each at most once per
     * record or slot, and a given-up run before each of them. */
    o->loop_bound = 2u * ((uint64_t)cfg->max_slots +
                          2u * (uint64_t)cfg->max_groups) + 4u;
    *out = o;
    return MOQ_OK;
}

void receiver_order_destroy(receiver_order_t *o,
                            void (*release)(void *ctx, void *item),
                            void *release_ctx)
{
    if (!o) return;
    order_release_all(o, release, release_ctx);
    moq_alloc_t alloc = o->alloc;
    alloc.free(o, o->alloc_size, alloc.ctx);
}

void receiver_order_reset(receiver_order_t *o,
                          void (*release)(void *ctx, void *item),
                          void *release_ctx)
{
    if (!o) return;
    order_release_all(o, release, release_ctx);
    o->rec_count = 0;
    o->slots_used = 0;
    o->items_held = 0;
    o->has_floor = false;
    o->floor = 0;
    o->loss_pending = false;
    o->tail_pending = false;
    o->stall_armed = false;
    o->finishing = false;
}

static moq_result_t order_hold(receiver_order_t *o, uint64_t now_us,
                               uint64_t g, uint64_t k, void *item,
                               bool status)
{
    if (!o || !item) return MOQ_ERR_INVAL;
    if (o->finishing) return MOQ_ERR_PROTO;
    order_record_t *r = order_record(o, g);
    if (order_group_closed(o, g, r)) return MOQ_ERR_PROTO;
    uint32_t i = o->slots_used;
    if (r) {
        if (k < r->frontier) return MOQ_ERR_PROTO;
        i = order_find(o, g, k);
        if (i < o->slots_used && o->slot[i].kind != ORDER_SLOT_LOST)
            return MOQ_ERR_PROTO;                         /* already held */
        if (status) {
            if (r->has_eog || (r->has_fin && k <= r->fin_last) ||
                order_contradicts(o, g, k, false))
                return MOQ_ERR_PROTO;
        } else if ((r->has_fin && k > r->fin_last) ||
                   (r->has_eog && k >= r->eog)) {
            return MOQ_ERR_PROTO;                         /* above final */
        }
    }
    order_slot_kind_t kind = status ? ORDER_SLOT_STATUS : ORDER_SLOT_HELD;
    if (i < o->slots_used) {
        /* The Object a RESET cut arrived after all: it takes the loss's
         * slot. */
        o->slot[i].item = item;
        o->slot[i].kind = (uint8_t)kind;
        o->items_held++;
    } else {
        if (o->slots_used == o->cfg.max_slots ||
            (!r && o->rec_count == o->cfg.max_groups))
            return MOQ_ERR_BUFFER;
        if (!r) r = order_open(o, g);
        order_slot_add(o, r, k, item, kind);
    }
    if (status) {
        r->has_eog = true;
        r->eog = k;
    }
    order_refresh(o, now_us);
    return MOQ_OK;
}

moq_result_t receiver_order_offer(receiver_order_t *o, uint64_t now_us,
                                  uint64_t group, uint64_t object, void *item)
{
    return order_hold(o, now_us, group, object, item, false);
}

moq_result_t receiver_order_offer_status(receiver_order_t *o, uint64_t now_us,
                                         uint64_t group, uint64_t end_object,
                                         void *item)
{
    return order_hold(o, now_us, group, end_object, item, true);
}

moq_result_t receiver_order_fin(receiver_order_t *o, uint64_t now_us,
                                uint64_t group, uint64_t last)
{
    if (!o) return MOQ_ERR_INVAL;
    if (o->finishing) return MOQ_DONE;
    order_record_t *r = order_record(o, group);
    if (order_group_closed(o, group, r)) return MOQ_DONE;
    if (r) {
        if (r->has_fin) return r->fin_last == last ? MOQ_OK : MOQ_DONE;
        if ((r->has_eog && last >= r->eog) ||
            (r->frontier > 0 && last < r->frontier - 1) ||
            order_contradicts(o, group, last, true))
            return MOQ_DONE;
    } else {
        if (o->rec_count == o->cfg.max_groups) return MOQ_ERR_BUFFER;
        r = order_open(o, group);
    }
    r->has_fin = true;
    r->fin_last = last;
    order_refresh(o, now_us);
    return MOQ_OK;
}

moq_result_t receiver_order_loss(receiver_order_t *o, uint64_t now_us,
                                 uint64_t group, uint64_t cut)
{
    if (!o) return MOQ_ERR_INVAL;
    if (o->finishing) return MOQ_DONE;
    order_record_t *r = order_record(o, group);
    if (order_group_closed(o, group, r)) return MOQ_DONE;
    if (r && (cut < r->frontier ||
              (r->has_fin && cut >= r->fin_last) ||
              (r->has_eog && cut >= r->eog) ||
              order_find(o, group, cut) < o->slots_used))
        return MOQ_DONE;
    if (o->slots_used == o->cfg.max_slots ||
        (!r && o->rec_count == o->cfg.max_groups))
        return MOQ_ERR_BUFFER;
    if (!r) r = order_open(o, group);
    order_slot_add(o, r, cut, NULL, ORDER_SLOT_LOST);
    order_refresh(o, now_us);
    return MOQ_OK;
}

moq_result_t receiver_order_force(receiver_order_t *o, uint64_t now_us)
{
    if (!o) return MOQ_ERR_INVAL;
    if (o->finishing) return MOQ_DONE;
    bool stepped = false;
    moq_result_t rc = MOQ_DONE;
    for (uint64_t n = 0; n < o->loop_bound && o->rec_count > 0; n++) {
        order_record_t *r = &o->rec[0];
        if (order_complete(r)) {
            order_retire_head(o);
            rc = MOQ_OK;
            break;
        }
        uint32_t i = order_find(o, r->group, r->frontier);
        if (i < o->slots_used) {
            if (o->slot[i].kind == ORDER_SLOT_LOST) {
                order_skip_lost(o, r, i);
                rc = MOQ_OK;
            } else if (stepped) {
                rc = MOQ_OK;     /* made due: the next poll frees its slot */
            }
            /* An item already due is freed by a poll, not by a force. */
            break;
        }
        if (order_absent(r, r->frontier)) {
            order_skip_absent(o, r);
            continue;
        }
        if (stepped) {
            rc = MOQ_OK;
            break;
        }
        bool retires = order_wait_kind(o, r) != ORDER_GAP_WAIT;
        order_expire(o);
        stepped = true;
        if (retires) {
            rc = MOQ_OK;
            break;
        }
    }
    if (rc == MOQ_OK) o->forced_total = sat_add(o->forced_total, 1);
    order_refresh(o, now_us);
    return rc;
}

moq_result_t receiver_order_poll(receiver_order_t *o, uint64_t now_us,
                                 receiver_order_delivery_t *out)
{
    if (!o || !out) return MOQ_ERR_INVAL;
    for (uint64_t n = 0; n < o->loop_bound && o->rec_count > 0; n++) {
        order_record_t *r = &o->rec[0];
        if (order_complete(r)) {
            order_retire_head(o);
            continue;
        }
        uint32_t i = order_find(o, r->group, r->frontier);
        if (i < o->slots_used) {
            if (o->slot[i].kind == ORDER_SLOT_LOST) {
                order_skip_lost(o, r, i);
                continue;
            }
            order_deliver(o, r, i, out);
            order_refresh(o, now_us);
            return MOQ_OK;
        }
        if (order_absent(r, r->frontier)) {
            order_skip_absent(o, r);
            continue;
        }
        if (!o->finishing) {
            if (order_wait_kind(o, r) == ORDER_IDLE) break;
            order_refresh(o, now_us);        /* arms a new stall */
            if (now_us < o->stall_deadline) break;
        }
        order_expire(o);
    }
    order_refresh(o, now_us);
    return MOQ_DONE;
}

void receiver_order_finish(receiver_order_t *o)
{
    if (!o) return;
    o->finishing = true;
    o->stall_armed = false;
}

uint64_t receiver_order_next_deadline_us(const receiver_order_t *o)
{
    return o && o->stall_armed ? o->stall_deadline : UINT64_MAX;
}

bool receiver_order_holds(const receiver_order_t *o)
{
    return o && o->items_held > 0;
}

void receiver_order_for_each(const receiver_order_t *o,
                             void (*fn)(void *ctx, void *item), void *ctx)
{
    if (!o || !fn) return;
    for (uint32_t i = 0; i < o->slots_used; i++)
        if (o->slot[i].kind != ORDER_SLOT_LOST) fn(ctx, o->slot[i].item);
}

uint64_t receiver_order_gaps_total(const receiver_order_t *o)
{
    return o ? o->gaps_total : 0;
}

uint64_t receiver_order_forced_total(const receiver_order_t *o)
{
    return o ? o->forced_total : 0;
}
