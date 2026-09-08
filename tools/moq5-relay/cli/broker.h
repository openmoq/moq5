/*
 * The snapshot broker: who asked for a generation, which generation is being
 * collected, and who is entitled to receive it.
 *
 * Two concerns that look alike and must not be merged. PUBLICATION IDENTITY
 * is the serial every lane publishes its row under. OUTPUT DEMAND is which
 * sinks asked for that generation. Conflating them is how a metrics scrape
 * ends up writing an unsolicited dump to stderr.
 *
 * The broker holds one slot PER BANK, not one generation. A generation that
 * has been collected but not yet delivered still occupies its slot, so a new
 * request can open a fresh serial on a different bank without destroying it.
 * A state machine able to describe only one generation cannot honour a
 * two-bank contract.
 *
 * DELIVERY IS TOKEN-DRIVEN. `moqr_broker_take_ready` hands back an
 * indivisible {serial, demand, bank} triple, and that token is the ONLY thing
 * a caller may dispatch from. Sampling the serial and the demand through
 * separate getters can observe two different generations, and it drops a sink
 * that legitimately joined after the sample — the token cannot.
 *
 * Serials are assigned by ordinary code under a mutex, never in a signal
 * handler. A handler only latches a lock-free flag; the coordinator turns
 * that flag into a request. Serials are 64-bit and are never narrowed.
 *
 * Sans-I/O: no clock, no sockets, no threads of its own. Every entry point is
 * safe to call from any thread.
 */
#ifndef MOQR_CLI_BROKER_H
#define MOQR_CLI_BROKER_H

#include <moq/relay/relay.h>

#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>

/* Which sink asked. A generation carries the union of the demands that joined
 * it while it was still collecting, and is delivered only to those. */
#define MOQR_BROKER_DEMAND_SIGNAL 0x1u   /* SIGUSR1 -> stderr dump */
#define MOQR_BROKER_DEMAND_HTTP   0x2u   /* an admitted scrape     */
#define MOQR_BROKER_DEMAND__ALL   0x3u

/* Banks in flight: exactly two — one being delivered while the next collects.
 * The figure is fixed because the body storage Step 3 preallocates is sized
 * from it, so a broker able to hand out more banks than there are bodies
 * would promise a generation nowhere to live. */
#define MOQR_BROKER_BANKS 2u

typedef enum moqr_broker_slot_state {
    MOQR_BROKER_SLOT_FREE = 0,
    MOQR_BROKER_SLOT_COLLECTING,
    MOQR_BROKER_SLOT_READY,
    MOQR_BROKER_SLOT_SENDING,
} moqr_broker_slot_state_t;

typedef struct moqr_broker_slot {
    uint64_t                 serial;   /* 0 when FREE                     */
    uint32_t                 demand;   /* frozen once the slot leaves     */
                                       /* COLLECTING                      */
    moqr_broker_slot_state_t state;
} moqr_broker_slot_t;

typedef struct moqr_broker {
    pthread_mutex_t    mu;
    moqr_broker_slot_t slot[MOQR_BROKER_BANKS];
    uint32_t           banks_total;
    uint32_t           deferred;    /* demand that arrived with no free bank */
    uint64_t           next_serial;
    /* The identity space is finite. When it is spent the broker refuses new
     * generations rather than wrapping: serial 0 means "no generation", and
     * reusing an identity would let a stale release authenticate against a
     * live one. */
    bool               serials_exhausted;
} moqr_broker_t;

/* `banks` must be exactly MOQR_BROKER_BANKS. INVAL otherwise: the count
 * is a contract with the body storage, not a tuning knob. */
moqr_result_t moqr_broker_init(moqr_broker_t *b, uint32_t banks);
void          moqr_broker_destroy(moqr_broker_t *b);

/*
 * Ask for a generation carrying `demand` (a non-empty subset of the DEMAND_*
 * bits; anything else is INVAL).
 *
 * A generation is COLLECTING  -> the demand JOINS it, its serial is returned,
 *                                *out_wake false, MOQR_OK. The serial does NOT
 *                                advance: advancing per arrival is what would
 *                                starve completion under scrape load.
 * Otherwise, a free bank      -> a new serial is assigned on that bank,
 *                                *out_wake true, MOQR_OK.
 * Otherwise                   -> MOQR_ERR_WOULD_BLOCK with *out_serial 0 and
 *                                *out_wake false. A refusal issues no token,
 *                                so it must not hand back some other
 *                                generation's serial.
 *
 * Once the serial space is spent, every further request is MOQR_ERR_CAPACITY
 * with *out_serial 0 and *out_wake false: a terminal refusal that creates no
 * work and defers nothing. Wrapping would issue serial 0 -- the value both
 * this module and the admin use to mean "no generation" -- or reuse a live
 * identity.
 *
 * Only SIGNAL demand is REMEMBERED across a refusal. A signal is lossy to
 * drop: nothing re-raises it, and the operator asked once. An HTTP request
 * that is refused has already been answered — Step 3 returns 503 immediately
 * — so retaining its bit would open a generation for a client that is gone,
 * doing work nobody is waiting for. Rejected HTTP demand creates no work.
 */
moqr_result_t moqr_broker_request(moqr_broker_t *b, uint32_t demand,
                                  uint64_t *out_serial, bool *out_wake);

/*
 * Freeze the generation `serial`: every lane has published for it. Its demand
 * is fixed from here, so a later joiner opens a new generation instead of
 * silently missing this one's body. A serial that is not currently collecting
 * is ignored — a late report from a superseded generation must never promote
 * anything.
 */
void moqr_broker_on_complete(moqr_broker_t *b, uint64_t serial);

/*
 * Take the OLDEST frozen generation for delivery, pinning its bank, and return
 * the indivisible token to dispatch from. False when nothing is ready. Use
 * this to drain work; a producer that rendered a specific generation must use
 * moqr_broker_take_serial instead, or it can deliver one generation's token
 * alongside another's bytes.
 */
bool moqr_broker_take_ready(moqr_broker_t *b, uint64_t *out_serial,
                            uint32_t *out_demand, uint32_t *out_bank);

/*
 * Take exactly `serial`, pinning its bank. False unless that generation is
 * frozen and waiting. This is what a coordinator that has just rendered a
 * particular generation must call: taking whatever happens to be oldest would
 * pair one generation's demand and bank with another's bytes, delivering the
 * wrong document to the wrong requester and stranding the rendered one.
 */
bool moqr_broker_take_serial(moqr_broker_t *b, uint64_t serial,
                             uint32_t *out_demand, uint32_t *out_bank);

/*
 * Release the bank a token pinned. `serial` and `bank` must BOTH match a
 * pinned generation, so a stale, unknown, duplicate or wrong-bank release
 * fails closed with MOQR_ERR_INVAL and changes nothing — freeing another
 * consumer's bank would let a generation be overwritten while it is still
 * being written out.
 *
 * `out_wake` is REQUIRED. On success this call may open a deferred generation,
 * and that transition is the only notice its lanes will ever get; accepting a
 * NULL here would let the sole wake be consumed and the generation stall
 * forever. A NULL is MOQR_ERR_INVAL and mutates nothing.
 */
moqr_result_t moqr_broker_release(moqr_broker_t *b, uint64_t serial,
                                  uint32_t bank, bool *out_wake);

/*
 * The COLLECTING generation's serial and demand, read together under one
 * lock. False when nothing is collecting. This is for diagnostics about a
 * generation still in flight. DELIVERY must use a token: a producer that
 * rendered a particular generation takes it with moqr_broker_take_serial,
 * while a drainer uses moqr_broker_take_ready. A demand read here can still be
 * joined afterwards, so it must never decide who receives a document.
 */
bool moqr_broker_current(const moqr_broker_t *b, uint64_t *out_serial,
                         uint32_t *out_demand);

/* Adapter for moqr_cli_epoch_fn: `ctx` is a moqr_broker_t *. Returns the
 * collecting serial, or 0 when none is open. */
uint64_t moqr_broker_epoch_fn(void *ctx);

/* True when any slot is not FREE — i.e. there is work outstanding. */
bool moqr_broker_busy(const moqr_broker_t *b);

/* Test-only: start the serial sequence near a chosen value so the 32-bit
 * boundary can be crossed deterministically. Not called by production. */
void moqr_broker_test_seed_serial(moqr_broker_t *b, uint64_t next);

#endif /* MOQR_CLI_BROKER_H */
