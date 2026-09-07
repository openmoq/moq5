/* The snapshot broker: generation identity and output demand, kept apart. */

#include "broker.h"

#include <string.h>

moqr_result_t
moqr_broker_init(moqr_broker_t *b, uint32_t banks)
{
    if (b == NULL || banks != MOQR_BROKER_BANKS) {
        return MOQR_ERR_INVAL;
    }
    memset(b, 0, sizeof(*b));
    if (pthread_mutex_init(&b->mu, NULL) != 0) {
        return MOQR_ERR_NOMEM;
    }
    b->banks_total = banks;
    /* Serials start at 1 so 0 can mean "no generation", and a zero-initialized
     * row can never look like it published for one. */
    b->next_serial = 1u;
    b->serials_exhausted = false;
    return MOQR_OK;
}

void
moqr_broker_destroy(moqr_broker_t *b)
{
    if (b == NULL) {
        return;
    }
    pthread_mutex_destroy(&b->mu);
    memset(b, 0, sizeof(*b));
}

/* Caller holds the lock for every helper below. */

static moqr_broker_slot_t *
slot_collecting(moqr_broker_t *b)
{
    for (uint32_t i = 0; i < b->banks_total; i++) {
        if (b->slot[i].state == MOQR_BROKER_SLOT_COLLECTING) {
            return &b->slot[i];
        }
    }
    return NULL;
}

static bool
open_generation(moqr_broker_t *b, uint32_t demand, uint64_t *out_serial)
{
    for (uint32_t i = 0; i < b->banks_total; i++) {
        if (b->slot[i].state != MOQR_BROKER_SLOT_FREE) {
            continue;
        }
        b->slot[i].serial = b->next_serial;
        if (b->next_serial == UINT64_MAX) {
            /* Spent. The NEXT request is refused outright rather than wrapping
             * to zero -- which both this module and the admin read as "no
             * generation" -- or reusing a live identity. */
            b->serials_exhausted = true;
        } else {
            b->next_serial++;
        }
        b->slot[i].demand = demand;
        b->slot[i].state = MOQR_BROKER_SLOT_COLLECTING;
        *out_serial = b->slot[i].serial;
        return true;
    }
    return false;
}

moqr_result_t
moqr_broker_request(moqr_broker_t *b, uint32_t demand, uint64_t *out_serial,
                    bool *out_wake)
{
    if (b == NULL || out_serial == NULL || out_wake == NULL) {
        return MOQR_ERR_INVAL;
    }
    if (demand == 0u || (demand & ~(uint32_t)MOQR_BROKER_DEMAND__ALL) != 0u) {
        return MOQR_ERR_INVAL;
    }
    *out_wake = false;
    *out_serial = 0u;

    pthread_mutex_lock(&b->mu);
    moqr_result_t rc = MOQR_OK;
    moqr_broker_slot_t *open = slot_collecting(b);
    if (open != NULL) {
        /* Join, do not advance. Advancing per arrival is exactly the
         * starvation failure: every request would re-target the lanes and no
         * generation would ever finish. */
        open->demand |= demand;
        *out_serial = open->serial;
    } else if (b->serials_exhausted) {
        /* Terminal, not transient: no serial, no wake, and nothing deferred.
         * Retaining demand here would queue work for a generation that can
         * never be opened. */
        rc = MOQR_ERR_CAPACITY;
    } else if (open_generation(b, demand, out_serial)) {
        *out_wake = true;
    } else {
        /* Every bank holds a generation that is collected but not yet
         * delivered. Refuse; a refusal issues no token, so no serial is
         * reported.
         *
         * Only signal demand is retained. A signal is lossy to drop — nothing
         * re-raises it — whereas a refused scrape has already been told 503,
         * so keeping its bit would open a generation for a client that is no
         * longer listening. */
        b->deferred |= (demand & (uint32_t)MOQR_BROKER_DEMAND_SIGNAL);
        rc = MOQR_ERR_WOULD_BLOCK;
    }
    pthread_mutex_unlock(&b->mu);
    return rc;
}

void
moqr_broker_on_complete(moqr_broker_t *b, uint64_t serial)
{
    if (b == NULL || serial == 0u) {
        return;
    }
    pthread_mutex_lock(&b->mu);
    for (uint32_t i = 0; i < b->banks_total; i++) {
        if (b->slot[i].state == MOQR_BROKER_SLOT_COLLECTING &&
            b->slot[i].serial == serial) {
            b->slot[i].state = MOQR_BROKER_SLOT_READY;
            break;
        }
    }
    pthread_mutex_unlock(&b->mu);
}

bool
moqr_broker_take_ready(moqr_broker_t *b, uint64_t *out_serial,
                       uint32_t *out_demand, uint32_t *out_bank)
{
    if (b == NULL || out_serial == NULL || out_demand == NULL ||
        out_bank == NULL) {
        return false;
    }
    bool taken = false;
    pthread_mutex_lock(&b->mu);
    /* Oldest READY serial first, never lowest bank index. Banks are reused, so
     * a fresh generation can land on a lower index than an older one still
     * awaiting delivery; picking by index would hand out the newer document
     * and leave the older slot pinned with nothing to retire it. */
    uint32_t pick = b->banks_total;
    for (uint32_t i = 0; i < b->banks_total; i++) {
        if (b->slot[i].state != MOQR_BROKER_SLOT_READY) {
            continue;
        }
        if (pick == b->banks_total || b->slot[i].serial < b->slot[pick].serial) {
            pick = i;
        }
    }
    {
        uint32_t i = pick;
        if (i < b->banks_total) {
        b->slot[i].state = MOQR_BROKER_SLOT_SENDING;
        /* The token is assembled under this one lock: serial, demand and bank
         * all describe the SAME generation, which is what makes dispatching
         * from it safe. */
        *out_serial = b->slot[i].serial;
        *out_demand = b->slot[i].demand;
        *out_bank = i;
        taken = true;
        }
    }
    pthread_mutex_unlock(&b->mu);
    return taken;
}

bool
moqr_broker_take_serial(moqr_broker_t *b, uint64_t serial, uint32_t *out_demand,
                        uint32_t *out_bank)
{
    if (b == NULL || serial == 0u || out_demand == NULL || out_bank == NULL) {
        return false;
    }
    bool taken = false;
    pthread_mutex_lock(&b->mu);
    for (uint32_t i = 0; i < b->banks_total; i++) {
        if (b->slot[i].state != MOQR_BROKER_SLOT_READY ||
            b->slot[i].serial != serial) {
            continue;
        }
        b->slot[i].state = MOQR_BROKER_SLOT_SENDING;
        *out_demand = b->slot[i].demand;
        *out_bank = i;
        taken = true;
        break;
    }
    pthread_mutex_unlock(&b->mu);
    return taken;
}

moqr_result_t
moqr_broker_release(moqr_broker_t *b, uint64_t serial, uint32_t bank,
                    bool *out_wake)
{
    /* Required: this call may open a deferred generation, and *out_wake is
     * the only notice its lanes will get. */
    if (out_wake == NULL) {
        return MOQR_ERR_INVAL;
    }
    *out_wake = false;
    if (b == NULL || serial == 0u) {
        return MOQR_ERR_INVAL;
    }
    pthread_mutex_lock(&b->mu);
    moqr_result_t rc = MOQR_ERR_INVAL;
    /* Both halves of the token must match a pinned generation. Decrementing a
     * bare count for any input would let a stale or duplicated release free
     * somebody else's bank and overwrite a generation still being written. */
    if (bank < b->banks_total &&
        b->slot[bank].state == MOQR_BROKER_SLOT_SENDING &&
        b->slot[bank].serial == serial) {
        b->slot[bank].state = MOQR_BROKER_SLOT_FREE;
        b->slot[bank].serial = 0u;
        b->slot[bank].demand = 0u;
        rc = MOQR_OK;

        if (b->deferred != 0u && slot_collecting(b) == NULL) {
            uint32_t d = b->deferred;
            uint64_t opened = 0;
            if (open_generation(b, d, &opened)) {
                b->deferred = 0u;
                *out_wake = true;
            }
        }
    }
    pthread_mutex_unlock(&b->mu);
    return rc;
}

bool
moqr_broker_current(const moqr_broker_t *b, uint64_t *out_serial,
                    uint32_t *out_demand)
{
    if (b == NULL || out_serial == NULL || out_demand == NULL) {
        return false;
    }
    moqr_broker_t *nb = (moqr_broker_t *)(uintptr_t)b;
    bool found = false;
    pthread_mutex_lock(&nb->mu);
    moqr_broker_slot_t *open = slot_collecting(nb);
    if (open != NULL) {
        *out_serial = open->serial;
        *out_demand = open->demand;
        found = true;
    }
    pthread_mutex_unlock(&nb->mu);
    return found;
}

uint64_t
moqr_broker_epoch_fn(void *ctx)
{
    uint64_t serial = 0;
    uint32_t demand = 0;
    if (!moqr_broker_current((const moqr_broker_t *)ctx, &serial, &demand)) {
        return 0u;
    }
    return serial;
}

bool
moqr_broker_busy(const moqr_broker_t *b)
{
    if (b == NULL) {
        return false;
    }
    moqr_broker_t *nb = (moqr_broker_t *)(uintptr_t)b;
    bool busy = false;
    pthread_mutex_lock(&nb->mu);
    for (uint32_t i = 0; i < nb->banks_total; i++) {
        if (nb->slot[i].state != MOQR_BROKER_SLOT_FREE) {
            busy = true;
            break;
        }
    }
    pthread_mutex_unlock(&nb->mu);
    return busy;
}

void
moqr_broker_test_seed_serial(moqr_broker_t *b, uint64_t next)
{
    if (b == NULL) {
        return;
    }
    pthread_mutex_lock(&b->mu);
    b->next_serial = next == 0u ? 1u : next;
    b->serials_exhausted = false;
    pthread_mutex_unlock(&b->mu);
}
