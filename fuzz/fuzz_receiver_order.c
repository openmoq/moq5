/* The receiver's in-group ordering stage under arbitrary sequences of
 * Objects, status Objects, terminals, losses, capacity steps, polls, ends
 * and new generations. Aborts when:
 *   - an item is released out of (Group, Object) order within a generation,
 *     or twice, or after the stage gave it back;
 *   - the stage holds more items than its slots;
 *   - an item is neither released nor given back by the end of the run;
 *   - a capacity step reports progress without freeing a slot or retiring
 *     a Group, more often than the slots and Groups allow;
 *   - a finished stage still holds something after it was polled empty.
 */
#include "media_receiver_order.h"

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#define SLOTS  8u
#define GROUPS 3u
#define ITEMS  256u

typedef struct {
    uint64_t group, object;
    int      state;          /* 0 unused, 1 held, 2 released, 3 given back */
} item_t;

typedef struct {
    item_t   items[ITEMS];
    unsigned used;
    unsigned held;
    bool     has_last;
    uint64_t last_group, last_object;
} run_t;

static void give_back(void *ctx, void *it)
{
    run_t *r = (run_t *)ctx;
    item_t *i = (item_t *)it;
    if (i->state != 1) abort();
    i->state = 3;
    r->held--;
}

static void released(run_t *r, const receiver_order_delivery_t *d)
{
    item_t *i = (item_t *)d->ctx;
    if (!i || i->state != 1 || i->group != d->group || i->object != d->object) abort();
    if (r->has_last && (d->group < r->last_group ||
                        (d->group == r->last_group && d->object <= r->last_object)))
        abort();
    r->has_last = true;
    r->last_group = d->group;
    r->last_object = d->object;
    i->state = 2;
    r->held--;
}

static void drain(run_t *r, receiver_order_t *o, uint64_t now)
{
    receiver_order_delivery_t d;
    for (unsigned k = 0; k <= ITEMS && receiver_order_poll(o, now, &d) == MOQ_OK; k++)
        released(r, &d);
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    receiver_order_cfg_t cfg = { GROUPS, SLOTS, 100 };
    receiver_order_t *o = NULL;
    if (receiver_order_create(moq_alloc_default(), &cfg, &o) != MOQ_OK) return 0;
    run_t *r = (run_t *)calloc(1, sizeof(*r));
    if (!r) { receiver_order_destroy(o, NULL, NULL); return 0; }
    uint64_t now = 0;
    for (size_t p = 0; p + 2 < size; p += 3) {
        uint8_t op = data[p] & 0x0F;
        /* Small Group and Object spaces collide often; the top values
         * exercise the end of the Object ID space. */
        uint64_t g = data[p + 1] >> 5;
        uint64_t k = data[p + 1] & 0x1F;
        if (k == 0x1F) k = UINT64_MAX - (data[p + 2] & 1u);
        now += data[p + 2] >> 1;
        switch (op) {
        case 0: case 1: case 2: {
            if (r->used == ITEMS) break;
            item_t *i = &r->items[r->used];
            i->group = g;
            i->object = k;
            moq_result_t rc = op == 2
                ? receiver_order_offer_status(o, now, g, k, i)
                : receiver_order_offer(o, now, g, k, i);
            if (rc == MOQ_OK) {
                i->state = 1;
                r->used++;
                r->held++;
            }
            break;
        }
        case 3: (void)receiver_order_fin(o, now, g, k); break;
        case 4: (void)receiver_order_loss(o, now, g, k); break;
        case 5: {
            for (unsigned steps = 0; receiver_order_force(o, now) == MOQ_OK; steps++) {
                if (steps > SLOTS + GROUPS + 1) abort();
                drain(r, o, now);
            }
            break;
        }
        case 6: case 7: case 8: drain(r, o, now); break;
        case 9:
            receiver_order_finish(o);
            drain(r, o, now);
            if (receiver_order_holds(o)) abort();
            break;
        case 10:
            receiver_order_reset(o, give_back, r);
            r->has_last = false;
            break;
        default: {
            uint64_t d = receiver_order_next_deadline_us(o);
            if (d != UINT64_MAX && d > now) now = d;
            drain(r, o, now);
            break;
        }
        }
        if (r->held > SLOTS) abort();
    }
    receiver_order_destroy(o, give_back, r);
    if (r->held != 0) abort();
    for (unsigned i = 0; i < r->used; i++)
        if (r->items[i].state != 2 && r->items[i].state != 3) abort();
    free(r);
    return 0;
}
