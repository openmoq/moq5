/*
 * The receiver's LOCMAF stage: see media_receiver_locmaf.h.
 */

#include "media_receiver_locmaf.h"

#include <string.h>

/* Why a Group closed (the closed-Group record). */
enum { LM_CLOSED_RETIRED = 1, LM_CLOSED_RESET, LM_CLOSED_EVICTED, LM_CLOSED_REFUSED };

/* Which count an output moves. */
enum { LM_COUNT_NONE = 0, LM_COUNT_REBUILT, LM_COUNT_UNDECODABLE,
       LM_COUNT_EXPIRED, LM_COUNT_LATE };

typedef struct lm_slot {
    receiver_lm_slot_state_t state;
    uint64_t                 group;
    uint64_t                 opened_seq;
    bool                     has_subgroup;
    uint64_t                 subgroup;
    bool                     terminal_seen;  /* a FIN of its subgroup */
    bool                     reclaimable;    /* its subgroup was reset */
    /* BOOTSTRAP: the next contiguous Object a held delta must be. */
    bool                     boot_has_next;
    uint64_t                 boot_next;
    /* The next Object ID the slot's decoder takes in, for gap reports. */
    bool                     has_pos;
    uint64_t                 pos_next;
    bool                     eog_parked;
    receiver_lm_item_t       eog;
    receiver_lm_dec_t        dec;            /* storage allocated at create */
} lm_slot_t;

struct receiver_lm {
    moq_alloc_t          alloc;
    moq_locmaf_track_t   track;
    bool                 join_seen;      /* the subscription's first live Group */
    uint64_t             join_group;
    uint64_t             next_open_seq;
    lm_slot_t            live[RECEIVER_LM_LIVE_GROUPS];
    uint64_t             closed[RECEIVER_LM_CLOSED_GROUPS];
    uint8_t              closed_why[RECEIVER_LM_CLOSED_GROUPS];
    size_t               closed_count, closed_next;
    bool                 has_floor;      /* every Group at or below is closed */
    uint64_t             floor;
    /* The held deltas, of the BOOTSTRAP slot only, in Object order. */
    receiver_lm_item_t   held[RECEIVER_LM_HOLD];
    size_t               held_head, held_count;
    int                  held_slot;      /* that slot, or -1 */
    bool                 eot_parked;
    receiver_lm_item_t   eot;
    receiver_lm_out_t    out[RECEIVER_LM_OUT];
    size_t               out_head, out_count;
    bool                 kept;           /* the item being taken in was kept */
    receiver_lm_counts_t counts;
};

/* -- Decoders ---------------------------------------------------------- */

static moq_result_t lm_dec_init(const moq_alloc_t *alloc,
                                const moq_locmaf_track_t *track,
                                receiver_lm_dec_t *d)
{
    moq_locmaf_decoder_cfg_t cfg;
    moq_locmaf_decoder_cfg_init(&cfg);
    cfg.track = track;
    size_t bytes = 0;
    moq_result_t rc = moq_locmaf_decoder_storage_size(&cfg, &bytes);
    if (rc != MOQ_OK) return rc;
    if (bytes == 0) bytes = 1;
    void *mem = alloc->alloc(bytes, alloc->ctx);
    if (!mem) return MOQ_ERR_NOMEM;
    cfg.storage = mem;
    cfg.storage_size = bytes;
    rc = moq_locmaf_decoder_init(&d->dec, sizeof(d->dec), &cfg);
    if (rc != MOQ_OK) {
        alloc->free(mem, bytes, alloc->ctx);
        return rc;
    }
    d->track = *track;
    d->storage = mem;
    d->storage_size = bytes;
    d->has_next = false;
    d->next_group = d->next_object = 0;
    d->scratch = NULL;
    d->scratch_cap = 0;
    return MOQ_OK;
}

static void lm_dec_fini(const moq_alloc_t *alloc, receiver_lm_dec_t *d)
{
    if (d->storage) alloc->free(d->storage, d->storage_size, alloc->ctx);
    if (d->scratch) alloc->free(d->scratch, d->scratch_cap, alloc->ctx);
    d->storage = NULL;
    d->storage_size = 0;
    d->scratch = NULL;
    d->scratch_cap = 0;
}

moq_result_t receiver_lm_dec_create(const moq_alloc_t *alloc,
                                    const moq_locmaf_track_t *track,
                                    receiver_lm_dec_t **out)
{
    if (!alloc || !track || !out) return MOQ_ERR_INVAL;
    *out = NULL;
    receiver_lm_dec_t *d = (receiver_lm_dec_t *)alloc->alloc(sizeof(*d), alloc->ctx);
    if (!d) return MOQ_ERR_NOMEM;
    memset(d, 0, sizeof(*d));
    moq_result_t rc = lm_dec_init(alloc, track, d);
    if (rc != MOQ_OK) {
        alloc->free(d, sizeof(*d), alloc->ctx);
        return rc;
    }
    *out = d;
    return MOQ_OK;
}

void receiver_lm_dec_destroy(const moq_alloc_t *alloc, receiver_lm_dec_t *d)
{
    if (!d) return;
    lm_dec_fini(alloc, d);
    alloc->free(d, sizeof(*d), alloc->ctx);
}

void receiver_lm_dec_reset(receiver_lm_dec_t *d)
{
    if (!d) return;
    moq_locmaf_decoder_reset(&d->dec);
    d->has_next = false;
}

moq_result_t receiver_lm_dec_push(receiver_lm_dec_t *d, uint64_t group,
                                  uint64_t object, moq_bytes_t payload,
                                  moq_locmaf_chunk_t *out,
                                  moq_locmaf_reason_t *reason)
{
    moq_result_t rc = moq_locmaf_decoder_push(&d->dec, group, object, payload,
                                              out, sizeof(*out), reason);
    if (rc == MOQ_OK) {
        /* Object ID UINT64_MAX ends the Group: nothing follows it. */
        d->has_next = object != UINT64_MAX;
        d->next_group = group;
        d->next_object = d->has_next ? object + 1 : 0;
    } else if (rc != MOQ_ERR_INVAL) {
        /* A refused Object advanced the decoder past a broken chain. */
        d->has_next = false;
    }
    return rc;
}

bool receiver_lm_refused(moq_result_t rc)
{
    return rc == MOQ_ERR_PROTO || rc == MOQ_ERR_UNSUPPORTED || rc == MOQ_ERR_LIMIT;
}

moq_result_t receiver_lm_rebuild(const moq_alloc_t *alloc, receiver_lm_dec_t *d,
                                 const moq_locmaf_chunk_t *chunk,
                                 moq_rcbuf_t **out)
{
    *out = NULL;
    size_t len = 0;
    moq_result_t rc = moq_locmaf_write_cmaf(&d->track, chunk, NULL, 0, &len);
    if (rc != MOQ_OK && rc != MOQ_ERR_BUFFER) return rc;
    if (len == 0) return MOQ_ERR_INVAL;
    if (len > d->scratch_cap) {
        uint8_t *grown = (uint8_t *)alloc->alloc(len, alloc->ctx);
        if (!grown) return MOQ_ERR_NOMEM;
        if (d->scratch) alloc->free(d->scratch, d->scratch_cap, alloc->ctx);
        d->scratch = grown;
        d->scratch_cap = len;
    }
    rc = moq_locmaf_write_cmaf(&d->track, chunk, d->scratch, d->scratch_cap, &len);
    if (rc != MOQ_OK) return rc;
    return moq_rcbuf_create(alloc, d->scratch, len, out);
}

/* -- Outputs ----------------------------------------------------------- */

static moq_bytes_t item_payload(const receiver_lm_item_t *item)
{
    if (!item->so.payload) return (moq_bytes_t){ NULL, 0 };
    return (moq_bytes_t){ moq_rcbuf_data(item->so.payload),
                          moq_rcbuf_len(item->so.payload) };
}

static void lm_count(receiver_lm_t *lm, int which)
{
    switch (which) {
    case LM_COUNT_REBUILT:     lm->counts.rebuilt++; break;
    case LM_COUNT_UNDECODABLE: lm->counts.undecodable++; break;
    case LM_COUNT_EXPIRED:     lm->counts.expired++; break;
    case LM_COUNT_LATE:        lm->counts.late++; break;
    default: break;
    }
}

/* Give an item back: ownership of *item (and of rebuilt) passes to the
 * output. The ring holds every item the stage can have at once (see
 * RECEIVER_LM_OUT); were it full, the item is dropped here instead. */
static void lm_emit_gap(receiver_lm_t *lm, uint32_t kind, receiver_lm_item_t *item,
                        moq_rcbuf_t *rebuilt, bool parse_drop, int which,
                        bool gap, uint64_t gap_object)
{
    lm_count(lm, which);
    if (lm->out_count == RECEIVER_LM_OUT) {
        if (rebuilt) moq_rcbuf_decref(rebuilt);
        moq_sub_object_cleanup(&item->so);
        memset(item, 0, sizeof(*item));
        return;
    }
    receiver_lm_out_t *o = &lm->out[(lm->out_head + lm->out_count) % RECEIVER_LM_OUT];
    o->kind = kind;
    o->item = *item;
    o->rebuilt = rebuilt;
    o->parse_drop = parse_drop;
    o->gap = gap;
    o->gap_object = gap_object;
    lm->out_count++;
    memset(item, 0, sizeof(*item));
}

static void lm_emit(receiver_lm_t *lm, uint32_t kind, receiver_lm_item_t *item,
                    moq_rcbuf_t *rebuilt, bool parse_drop, int which)
{
    lm_emit_gap(lm, kind, item, rebuilt, parse_drop, which, false, 0);
}

receiver_lm_out_t *receiver_lm_peek(receiver_lm_t *lm)
{
    if (!lm || lm->out_count == 0) return NULL;
    return &lm->out[lm->out_head];
}

void receiver_lm_pop(receiver_lm_t *lm)
{
    if (!lm || lm->out_count == 0) return;
    memset(&lm->out[lm->out_head], 0, sizeof(lm->out[lm->out_head]));
    lm->out_head = (lm->out_head + 1) % RECEIVER_LM_OUT;
    lm->out_count--;
}

size_t receiver_lm_out_count(const receiver_lm_t *lm)
{
    return lm ? lm->out_count : 0;
}

receiver_lm_out_t *receiver_lm_out_at(receiver_lm_t *lm, size_t i)
{
    if (!lm || i >= lm->out_count) return NULL;
    return &lm->out[(lm->out_head + i) % RECEIVER_LM_OUT];
}

/* -- Closed Groups ----------------------------------------------------- */

static bool lm_closed(const receiver_lm_t *lm, uint64_t group)
{
    if (lm->has_floor && group <= lm->floor) return true;
    for (size_t i = 0; i < lm->closed_count; i++)
        if (lm->closed[i] == group) return true;
    return false;
}

/* Remember a closed Group; one pushed out of the ring raises the floor. */
static void lm_record_closed(receiver_lm_t *lm, uint64_t group, uint8_t why)
{
    if (lm->closed_count == RECEIVER_LM_CLOSED_GROUPS) {
        uint64_t old = lm->closed[lm->closed_next];
        if (!lm->has_floor || old > lm->floor) {
            lm->floor = old;
            lm->has_floor = true;
        }
    } else {
        lm->closed_count++;
    }
    lm->closed[lm->closed_next] = group;
    lm->closed_why[lm->closed_next] = why;
    lm->closed_next = (lm->closed_next + 1) % RECEIVER_LM_CLOSED_GROUPS;
}

/* -- Slots --------------------------------------------------------------- */

static lm_slot_t *lm_slot_of(receiver_lm_t *lm, uint64_t group)
{
    for (size_t i = 0; i < RECEIVER_LM_LIVE_GROUPS; i++)
        if (lm->live[i].state != LM_SLOT_FREE && lm->live[i].group == group)
            return &lm->live[i];
    return NULL;
}

static int lm_slot_index(const receiver_lm_t *lm, const lm_slot_t *s)
{
    return (int)(s - lm->live);
}

static receiver_lm_item_t *lm_held_at(receiver_lm_t *lm, size_t i)
{
    return &lm->held[(lm->held_head + i) % RECEIVER_LM_HOLD];
}

/* Give every held Object up as a loss counted by `which`. */
static void lm_drop_held(receiver_lm_t *lm, int which)
{
    while (lm->held_count > 0) {
        receiver_lm_item_t *it = lm_held_at(lm, 0);
        lm->held_head = (lm->held_head + 1) % RECEIVER_LM_HOLD;
        lm->held_count--;
        lm_emit(lm, RECEIVER_LM_OUT_LOSS, it, NULL, false, which);
    }
    lm->held_slot = -1;
}

/* Free a slot, keeping its decoder storage. Nothing may be held for it. */
static void lm_slot_clear(lm_slot_t *s)
{
    receiver_lm_dec_reset(&s->dec);
    s->state = LM_SLOT_FREE;
    s->group = 0;
    s->opened_seq = 0;
    s->has_subgroup = false;
    s->subgroup = 0;
    s->terminal_seen = false;
    s->reclaimable = false;
    s->boot_has_next = false;
    s->boot_next = 0;
    s->has_pos = false;
    s->pos_next = 0;
}

/* Close a slot as `why`: its parked END_OF_GROUP goes out first. */
static void lm_slot_close(receiver_lm_t *lm, lm_slot_t *s, uint8_t why)
{
    if (lm->held_slot == lm_slot_index(lm, s)) lm_drop_held(lm, LM_COUNT_EXPIRED);
    if (s->eog_parked) {
        s->eog_parked = false;
        lm_emit(lm, RECEIVER_LM_OUT_STATUS, &s->eog, NULL, false, LM_COUNT_NONE);
    }
    lm_record_closed(lm, s->group, why);
    lm_slot_clear(s);
}

/* Loss detected in the slot's Group (a reset stream, a datagram, a second
 * subgroup): what it holds is given up, its decoder forgets the chain, and
 * deltas are dropped until a full header or rawBoxes Object. */
static void lm_slot_barrier(receiver_lm_t *lm, lm_slot_t *s)
{
    if (lm->held_slot == lm_slot_index(lm, s)) lm_drop_held(lm, LM_COUNT_EXPIRED);
    receiver_lm_dec_reset(&s->dec);
    s->state = LM_SLOT_BARRIER;
    s->boot_has_next = false;
    s->has_pos = false;
    lm->counts.resets++;
}

/* A slot whose subgroup finished, or whose END_OF_GROUP is parked, closes
 * once nothing is held for it, its parked status Object going out behind
 * what was held; a parked END_OF_TRACK goes out once nothing at all is
 * held, and every slot closes. */
static void lm_settle(receiver_lm_t *lm)
{
    for (size_t i = 0; i < RECEIVER_LM_LIVE_GROUPS; i++) {
        lm_slot_t *s = &lm->live[i];
        if (s->state == LM_SLOT_FREE) continue;
        if (lm->held_slot == (int)i && lm->held_count > 0) continue;
        if (s->eog_parked || s->terminal_seen)
            lm_slot_close(lm, s, LM_CLOSED_RETIRED);
    }
    if (lm->held_count > 0) return;
    if (lm->eot_parked) {
        lm->eot_parked = false;
        for (size_t i = 0; i < RECEIVER_LM_LIVE_GROUPS; i++)
            if (lm->live[i].state != LM_SLOT_FREE)
                lm_slot_close(lm, &lm->live[i], LM_CLOSED_RETIRED);
        lm_emit(lm, RECEIVER_LM_OUT_STATUS, &lm->eot, NULL, false, LM_COUNT_NONE);
    }
}

/* The slot an Object of `group` goes to, opening one as needed (returned
 * FREE, for the caller to classify), or NULL when the Group is late: closed
 * already, or refused for capacity (counted). */
static lm_slot_t *lm_route(receiver_lm_t *lm, uint64_t group)
{
    lm_slot_t *s = lm_slot_of(lm, group);
    if (s) return s;
    if (lm_closed(lm, group)) return NULL;
    lm_slot_t *free_slot = NULL, *reclaim = NULL, *lowest = NULL;
    for (size_t i = 0; i < RECEIVER_LM_LIVE_GROUPS; i++) {
        lm_slot_t *c = &lm->live[i];
        if (c->state == LM_SLOT_FREE) {
            if (!free_slot) free_slot = c;
            continue;
        }
        if (c->reclaimable && (!reclaim || c->group < reclaim->group)) reclaim = c;
        if (!lowest || c->group < lowest->group) lowest = c;
    }
    if (!free_slot) {
        if (reclaim) {
            lm_slot_close(lm, reclaim, LM_CLOSED_RESET);
            free_slot = reclaim;
        } else if (lowest && group > lowest->group) {
            lm->counts.groups_evicted++;
            if (lm->held_slot == lm_slot_index(lm, lowest)) lm_drop_held(lm, LM_COUNT_LATE);
            lm_slot_close(lm, lowest, LM_CLOSED_EVICTED);
            free_slot = lowest;
        } else {
            lm->counts.groups_evicted++;
            lm_record_closed(lm, group, LM_CLOSED_REFUSED);
            return NULL;
        }
    }
    free_slot->group = group;
    free_slot->opened_seq = lm->next_open_seq++;
    return free_slot;
}

/* Decode an Object in its slot and give it back. */
static void lm_decode(receiver_lm_t *lm, lm_slot_t *s, receiver_lm_item_t *item)
{
    uint64_t o = item->so.object_id;
    if (s->has_pos && o < s->pos_next) {
        /* A duplicate, or an Object below the next expected one: nothing is
         * missing, and the chain is untouched. */
        lm_emit(lm, RECEIVER_LM_OUT_SILENT, item, NULL, false, LM_COUNT_UNDECODABLE);
        return;
    }
    bool gap = s->has_pos && o > s->pos_next;
    uint64_t gap_object = s->pos_next;
    moq_locmaf_chunk_t chunk;
    moq_locmaf_reason_t why = 0;
    moq_result_t rc = receiver_lm_dec_push(&s->dec, item->so.group_id, o,
                                           item_payload(item), &chunk, &why);
    if (rc == MOQ_ERR_INVAL) {
        lm_emit(lm, RECEIVER_LM_OUT_SILENT, item, NULL, false, LM_COUNT_UNDECODABLE);
        return;
    }
    /* The decoder's position advances past every Object it takes in. */
    s->has_pos = o != UINT64_MAX;
    s->pos_next = s->has_pos ? o + 1 : 0;
    if (rc != MOQ_OK) {
        s->state = LM_SLOT_BARRIER;
        lm_emit_gap(lm, RECEIVER_LM_OUT_LOSS, item, NULL, receiver_lm_refused(rc),
                    LM_COUNT_UNDECODABLE, gap, gap_object);
        return;
    }
    s->state = chunk.kind == MOQ_LOCMAF_OBJECT_RAW_BOXES ? LM_SLOT_RAW : LM_SLOT_RUNNING;
    moq_rcbuf_t *rebuilt = NULL;
    if (receiver_lm_rebuild(&lm->alloc, &s->dec, &chunk, &rebuilt) != MOQ_OK) {
        /* The decoder has advanced, so the chain stays valid; this Object
         * alone is lost. */
        lm_emit_gap(lm, RECEIVER_LM_OUT_LOSS, item, NULL, false, LM_COUNT_UNDECODABLE,
                    gap, gap_object);
        return;
    }
    lm_emit_gap(lm, RECEIVER_LM_OUT_REBUILT, item, rebuilt, false, LM_COUNT_REBUILT,
                gap, gap_object);
}

/* The shape of an Object, read without decoding it: NONE when malformed. */
static moq_locmaf_object_kind_t lm_kind(const receiver_lm_item_t *item)
{
    moq_locmaf_object_view_t view;
    size_t count = 0;
    moq_result_t rc = moq_locmaf_parse_object(item_payload(item), &view,
                                              sizeof(view), NULL, 0, &count, NULL);
    return rc == MOQ_OK || rc == MOQ_ERR_BUFFER ? view.kind : MOQ_LOCMAF_OBJECT_NONE;
}

/* BOOTSTRAP: hand the held deltas to the slot from a fill decoder that holds
 * the chain they continue (see media_receiver_locmaf.h). Held Objects a
 * fill has passed are dropped without loss: that fill delivered them. */
static void lm_try_handoff(receiver_lm_t *lm, const receiver_lm_fills_t *fills)
{
    if (lm->held_slot < 0 || !fills) return;
    lm_slot_t *s = &lm->live[lm->held_slot];
    if (s->state != LM_SLOT_BOOTSTRAP || s->group != lm->join_group) return;
    for (size_t f = 0; f < fills->count && lm->held_count > 0; f++) {
        const receiver_lm_dec_t *fd = fills->dec[f];
        if (!fd || !fd->has_next || fd->next_group != s->group) continue;
        while (lm->held_count > 0 &&
               lm_held_at(lm, 0)->so.object_id < fd->next_object) {
            receiver_lm_item_t *it = lm_held_at(lm, 0);
            lm->held_head = (lm->held_head + 1) % RECEIVER_LM_HOLD;
            lm->held_count--;
            lm_emit(lm, RECEIVER_LM_OUT_SILENT, it, NULL, false, LM_COUNT_UNDECODABLE);
        }
        if (lm->held_count == 0) break;
        uint64_t first = lm_held_at(lm, 0)->so.object_id;
        if (first != fd->next_object ||
            !moq_locmaf_decoder_expects(&fd->dec, s->group, first))
            continue;
        if (moq_locmaf_decoder_copy(&s->dec.dec, &fd->dec) != MOQ_OK) continue;
        s->dec.has_next = fd->has_next;
        s->dec.next_group = fd->next_group;
        s->dec.next_object = fd->next_object;
        s->has_pos = true;
        s->pos_next = fd->next_object;
        s->state = LM_SLOT_RUNNING;
        lm->held_slot = -1;
        while (lm->held_count > 0) {
            receiver_lm_item_t it = *lm_held_at(lm, 0);
            memset(lm_held_at(lm, 0), 0, sizeof(it));
            lm->held_head = (lm->held_head + 1) % RECEIVER_LM_HOLD;
            lm->held_count--;
            lm_decode(lm, s, &it);
        }
        return;
    }
}

/* BOOTSTRAP: give the hold up once its oldest Object's deadline passed or
 * no fill of the subscription can deliver any more. */
static void lm_expire(receiver_lm_t *lm, uint64_t now_us,
                      const receiver_lm_fills_t *fills)
{
    if (lm->held_slot < 0) return;
    lm_slot_t *s = &lm->live[lm->held_slot];
    bool possible = fills && fills->possible;
    if (lm->held_count > 0 &&
        (now_us >= lm_held_at(lm, 0)->deadline_us || !possible)) {
        lm_drop_held(lm, LM_COUNT_EXPIRED);
        s->state = LM_SLOT_BARRIER;
        s->boot_has_next = false;
    } else if (lm->held_count == 0 && !possible) {
        lm->held_slot = -1;
        s->state = LM_SLOT_BARRIER;
        s->boot_has_next = false;
    }
}

/* A delta of the BOOTSTRAP slot's Group: held, contiguously, or the hold
 * fails. */
static void lm_bootstrap_delta(receiver_lm_t *lm, lm_slot_t *s, uint64_t now_us,
                               const receiver_lm_fills_t *fills,
                               receiver_lm_item_t *item)
{
    uint64_t o = item->so.object_id;
    if (!s->boot_has_next || o < s->boot_next) {
        lm_emit(lm, RECEIVER_LM_OUT_SILENT, item, NULL, false, LM_COUNT_UNDECODABLE);
        return;
    }
    if (o != s->boot_next) {
        /* A gap among the Objects to hold: nothing can continue them. */
        lm_drop_held(lm, LM_COUNT_EXPIRED);
        s->state = LM_SLOT_BARRIER;
        s->boot_has_next = false;
        lm_decode(lm, s, item);
        return;
    }
    if (lm->held_count == RECEIVER_LM_HOLD) {
        lm_drop_held(lm, LM_COUNT_EXPIRED);
        s->state = LM_SLOT_BARRIER;
        s->boot_has_next = false;
        lm_emit(lm, RECEIVER_LM_OUT_LOSS, item, NULL, false, LM_COUNT_EXPIRED);
        return;
    }
    item->held = true;
    lm->kept = true;
    item->deadline_us = now_us > UINT64_MAX - RECEIVER_LM_HOLD_US
                            ? UINT64_MAX : now_us + RECEIVER_LM_HOLD_US;
    lm->held[(lm->held_head + lm->held_count) % RECEIVER_LM_HOLD] = *item;
    lm->held_count++;
    lm->held_slot = lm_slot_index(lm, s);
    memset(item, 0, sizeof(*item));
    s->boot_has_next = o != UINT64_MAX;
    s->boot_next = s->boot_has_next ? o + 1 : 0;
    lm_try_handoff(lm, fills);
    lm_expire(lm, now_us, fills);
}

/* -- Entry points -------------------------------------------------------- */

moq_result_t receiver_lm_create(const moq_alloc_t *alloc,
                                const moq_locmaf_track_t *track,
                                receiver_lm_t **out)
{
    if (!alloc || !track || !out) return MOQ_ERR_INVAL;
    *out = NULL;
    receiver_lm_t *lm = (receiver_lm_t *)alloc->alloc(sizeof(*lm), alloc->ctx);
    if (!lm) return MOQ_ERR_NOMEM;
    memset(lm, 0, sizeof(*lm));
    lm->alloc = *alloc;
    lm->track = *track;
    lm->held_slot = -1;
    for (size_t i = 0; i < RECEIVER_LM_LIVE_GROUPS; i++) {
        moq_result_t rc = lm_dec_init(alloc, track, &lm->live[i].dec);
        if (rc != MOQ_OK) {
            for (size_t j = 0; j < i; j++) lm_dec_fini(alloc, &lm->live[j].dec);
            alloc->free(lm, sizeof(*lm), alloc->ctx);
            return rc;
        }
    }
    *out = lm;
    return MOQ_OK;
}

void receiver_lm_destroy(receiver_lm_t *lm,
                         void (*release)(void *ctx, receiver_lm_item_t *item),
                         void *ctx)
{
    if (!lm) return;
    while (lm->out_count > 0) {
        receiver_lm_out_t *o = &lm->out[lm->out_head];
        if (o->rebuilt) moq_rcbuf_decref(o->rebuilt);
        release(ctx, &o->item);
        receiver_lm_pop(lm);
    }
    while (lm->held_count > 0) {
        release(ctx, lm_held_at(lm, 0));
        lm->held_head = (lm->held_head + 1) % RECEIVER_LM_HOLD;
        lm->held_count--;
    }
    for (size_t i = 0; i < RECEIVER_LM_LIVE_GROUPS; i++) {
        if (lm->live[i].eog_parked) release(ctx, &lm->live[i].eog);
        lm_dec_fini(&lm->alloc, &lm->live[i].dec);
    }
    if (lm->eot_parked) release(ctx, &lm->eot);
    moq_alloc_t a = lm->alloc;
    a.free(lm, sizeof(*lm), a.ctx);
}

static void lm_object(receiver_lm_t *lm, uint64_t now_us,
                      const receiver_lm_fills_t *fills,
                      receiver_lm_item_t *item)
{
    uint64_t g = item->so.group_id;
    if (item->so.datagram) {
        /* Section 3 binds the publisher to one subgroup and no datagrams:
         * this Object breaks the Group's delivery order. */
        lm_slot_t *s = lm_slot_of(lm, g);
        if (s) lm_slot_barrier(lm, s);
        lm_emit(lm, RECEIVER_LM_OUT_LOSS, item, NULL, false, LM_COUNT_UNDECODABLE);
        lm_settle(lm);
        return;
    }
    lm_slot_t *s = lm_route(lm, g);
    if (!s) {
        lm_emit(lm, RECEIVER_LM_OUT_LOSS, item, NULL, false, LM_COUNT_LATE);
        lm_settle(lm);
        return;
    }
    if (s->state == LM_SLOT_FREE) {
        bool first = !lm->join_seen;
        if (first) {
            lm->join_seen = true;
            lm->join_group = g;
        }
        s->has_subgroup = true;
        s->subgroup = item->so.subgroup_id;
        /* Joined by a delta: held for a hand-off only while a fill of the
         * subscription can still deliver the chain (none on drafts 16
         * and 18). */
        if (first && item->so.object_id > 0 && fills && fills->possible &&
            lm_kind(item) == MOQ_LOCMAF_OBJECT_DELTA) {
            s->state = LM_SLOT_BOOTSTRAP;
            s->boot_has_next = true;
            s->boot_next = item->so.object_id;
            lm_bootstrap_delta(lm, s, now_us, fills, item);
            lm_settle(lm);
            return;
        }
        s->state = LM_SLOT_BARRIER;   /* until a decoded Object anchors it */
        lm_decode(lm, s, item);
        lm_settle(lm);
        return;
    }
    if (s->subgroup != item->so.subgroup_id) {
        lm_slot_barrier(lm, s);
        lm_emit(lm, RECEIVER_LM_OUT_LOSS, item, NULL, false, LM_COUNT_UNDECODABLE);
        lm_settle(lm);
        return;
    }
    if (s->state == LM_SLOT_BOOTSTRAP) {
        if (lm_kind(item) == MOQ_LOCMAF_OBJECT_DELTA) {
            lm_bootstrap_delta(lm, s, now_us, fills, item);
        } else {
            /* A full header, a rawBoxes Object or a malformed one ends the
             * bootstrap: the held deltas go first, handed off when a fill
             * holds their chain, else lost. */
            lm_try_handoff(lm, fills);
            if (lm->held_slot == lm_slot_index(lm, s)) {
                lm_drop_held(lm, LM_COUNT_EXPIRED);
                s->state = LM_SLOT_BARRIER;
            } else if (s->state == LM_SLOT_BOOTSTRAP) {
                s->state = LM_SLOT_BARRIER;
            }
            s->boot_has_next = false;
            lm_decode(lm, s, item);
        }
        lm_settle(lm);
        return;
    }
    lm_decode(lm, s, item);
    lm_settle(lm);
}

bool receiver_lm_object(receiver_lm_t *lm, uint64_t now_us,
                        const receiver_lm_fills_t *fills,
                        receiver_lm_item_t *item)
{
    lm->kept = false;
    lm_object(lm, now_us, fills, item);
    return lm->kept;
}

static void lm_status(receiver_lm_t *lm, receiver_lm_item_t *item)
{
    if (item->so.status == MOQ_OBJECT_END_OF_TRACK) {
        if (receiver_lm_holds(lm) && !lm->eot_parked) {
            item->held = true;
            lm->kept = true;
            lm->eot = *item;
            lm->eot_parked = true;
            memset(item, 0, sizeof(*item));
            return;
        }
        for (size_t i = 0; i < RECEIVER_LM_LIVE_GROUPS; i++)
            if (lm->live[i].state != LM_SLOT_FREE && lm->held_slot != (int)i)
                lm_slot_close(lm, &lm->live[i], LM_CLOSED_RETIRED);
        lm_emit(lm, RECEIVER_LM_OUT_STATUS, item, NULL, false, LM_COUNT_NONE);
        return;
    }
    lm_slot_t *s = lm_slot_of(lm, item->so.group_id);
    if (s && lm->held_slot == lm_slot_index(lm, s) && lm->held_count > 0 &&
        !s->eog_parked) {
        item->held = true;
        lm->kept = true;
        s->eog = *item;
        s->eog_parked = true;
        memset(item, 0, sizeof(*item));
        return;
    }
    lm_emit(lm, RECEIVER_LM_OUT_STATUS, item, NULL, false, LM_COUNT_NONE);
    if (s && lm->held_slot != lm_slot_index(lm, s))
        lm_slot_close(lm, s, LM_CLOSED_RETIRED);
}

bool receiver_lm_status(receiver_lm_t *lm, receiver_lm_item_t *item)
{
    lm->kept = false;
    lm_status(lm, item);
    return lm->kept;
}

void receiver_lm_terminal(receiver_lm_t *lm, uint32_t kind, uint64_t group,
                          uint64_t subgroup)
{
    lm_slot_t *s = lm_slot_of(lm, group);
    if (!s) return;
    if (s->has_subgroup && s->subgroup != subgroup) {
        lm_slot_barrier(lm, s);
        lm_settle(lm);
        return;
    }
    if (kind == MOQ_SUB_STREAM_RESET) {
        lm_slot_barrier(lm, s);
        s->reclaimable = true;
        lm_settle(lm);
        return;
    }
    s->terminal_seen = true;
    lm_settle(lm);
}

void receiver_lm_progress(receiver_lm_t *lm, uint64_t now_us,
                          const receiver_lm_fills_t *fills)
{
    lm_try_handoff(lm, fills);
    lm_expire(lm, now_us, fills);
    lm_settle(lm);
}

void receiver_lm_disable(receiver_lm_t *lm)
{
    lm_drop_held(lm, LM_COUNT_EXPIRED);
    for (size_t i = 0; i < RECEIVER_LM_LIVE_GROUPS; i++) {
        lm_slot_t *s = &lm->live[i];
        if (s->eog_parked) {
            s->eog_parked = false;
            lm_emit(lm, RECEIVER_LM_OUT_STATUS, &s->eog, NULL, false, LM_COUNT_NONE);
        }
        if (s->state == LM_SLOT_FREE) continue;
        lm->counts.resets++;
        lm_slot_clear(s);
    }
    if (lm->eot_parked) {
        lm->eot_parked = false;
        lm_emit(lm, RECEIVER_LM_OUT_STATUS, &lm->eot, NULL, false, LM_COUNT_NONE);
    }
}

void receiver_lm_finish(receiver_lm_t *lm, const receiver_lm_fills_t *fills)
{
    if (lm->held_count > 0 && !(fills && fills->possible)) {
        lm_slot_t *s = &lm->live[lm->held_slot];
        lm_drop_held(lm, LM_COUNT_EXPIRED);
        s->state = LM_SLOT_BARRIER;
    }
    if (lm->held_count > 0) return;
    for (size_t i = 0; i < RECEIVER_LM_LIVE_GROUPS; i++) {
        lm_slot_t *s = &lm->live[i];
        if (s->state != LM_SLOT_FREE)
            lm_slot_close(lm, s, s->reclaimable ? LM_CLOSED_RESET : LM_CLOSED_RETIRED);
    }
    lm_settle(lm);
}

uint64_t receiver_lm_deadline(const receiver_lm_t *lm)
{
    if (!lm || lm->held_count == 0) return UINT64_MAX;
    return lm->held[lm->held_head].deadline_us;
}

bool receiver_lm_holds(const receiver_lm_t *lm)
{
    if (!lm) return false;
    if (lm->held_count > 0 || lm->eot_parked) return true;
    for (size_t i = 0; i < RECEIVER_LM_LIVE_GROUPS; i++)
        if (lm->live[i].eog_parked) return true;
    return false;
}

bool receiver_lm_holds_before(const receiver_lm_t *lm, uint64_t barrier)
{
    if (!lm) return false;
    for (size_t i = 0; i < lm->held_count; i++)
        if (lm->held[(lm->held_head + i) % RECEIVER_LM_HOLD].seq < barrier) return true;
    for (size_t i = 0; i < lm->out_count; i++)
        if (lm->out[(lm->out_head + i) % RECEIVER_LM_OUT].item.seq < barrier) return true;
    for (size_t i = 0; i < RECEIVER_LM_LIVE_GROUPS; i++)
        if (lm->live[i].eog_parked && lm->live[i].eog.seq < barrier) return true;
    return lm->eot_parked && lm->eot.seq < barrier;
}

receiver_lm_counts_t receiver_lm_counts(const receiver_lm_t *lm)
{
    receiver_lm_counts_t none;
    memset(&none, 0, sizeof(none));
    return lm ? lm->counts : none;
}

receiver_lm_slot_state_t receiver_lm_slot_state(const receiver_lm_t *lm,
                                                uint64_t group)
{
    if (!lm) return LM_SLOT_FREE;
    for (size_t i = 0; i < RECEIVER_LM_LIVE_GROUPS; i++)
        if (lm->live[i].state != LM_SLOT_FREE && lm->live[i].group == group)
            return lm->live[i].state;
    return LM_SLOT_FREE;
}
