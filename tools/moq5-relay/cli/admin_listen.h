/*
 * The admin endpoint boundary: one listening socket, one dedicated thread, and
 * the bounded multi-client state machine from the sans-I/O admin tier.
 *
 * WHAT LIVES HERE AND WHAT DOES NOT. This file owns file descriptors, one
 * pthread, and a wake mechanism. It owns no protocol decisions: parsing,
 * timeouts, response construction, generation identity, bank pinning and the
 * release protocol all belong to `admin/moqr_admin.h`, which is frozen. This
 * wrapper must never re-implement any of them, because two state machines that
 * agree today are two state machines that disagree after the next change.
 *
 * THE ADMIN THREAD NEVER READS LIVE RELAY STATE. Not relay, bind, shard, route,
 * session or track. Every byte it serves comes from a bank a lane pump
 * published into, through the snapshot path, at every lane count including
 * K=1. That is the whole reason the endpoint is safe to run beside the lanes.
 *
 * ONE THREAD, NOT ONE PER CONNECTION. A thread per connection would make the
 * client bound a scheduling accident rather than a fixed capacity.
 *
 * LOOPBACK ONLY. v1 binds a loopback TCP address and nothing else; both the
 * configuration parser and this final socket boundary refuse remote addresses.
 */
#ifndef MOQR_CLI_ADMIN_LISTEN_H
#define MOQR_CLI_ADMIN_LISTEN_H

#include "broker.h"
#include "config.h"

#include "../admin/moqr_admin.h"

#include <moq/relay/types.h>

#include <stdbool.h>
#include <stdint.h>

/*
 * FALLIBLE TEST-CLOCK OPERATIONS MUST BE CHECKED.
 *
 * Arming refuses when no floor can be sampled; advancing refuses an unarmed
 * clock and a step that would wrap. A discarded result turns a refusal into a
 * step the caller believes happened, and the failure then surfaces much later
 * as a timeout somewhere unrelated -- if it surfaces at all. The compiler is
 * the gate: with warnings as errors an ignored result does not build, so a new
 * call site cannot be added unchecked and no source scanner has to be trusted
 * to notice.
 */
#if defined(__GNUC__) || defined(__clang__)
#define MOQR_ADMIN_MUST_CHECK __attribute__((warn_unused_result))
#else
#define MOQR_ADMIN_MUST_CHECK
#endif

/* How much work one turn of the thread loop may do. Bounds, not tuning: an
 * unbounded turn is how one busy client starves the rest and how shutdown
 * latency becomes unpredictable. */
#define MOQR_ADMIN_ACCEPTS_PER_TURN 4u
#define MOQR_ADMIN_READ_CHUNK       512u
#define MOQR_ADMIN_WRITE_CHUNK      4096u
#define MOQR_ADMIN_POLL_MS          50
/* The refusal slot's own bounded deadline: an overload client that will not
 * read its 503 must not hold the slot against the next one. */
#define MOQR_ADMIN_REFUSE_DEADLINE_US (2u * 1000u * 1000u)
/* How long a stop waits for the owner to signal its own exit before declaring
 * that its death cannot be proved. A bound, not a tuning knob: pthread_join
 * blocks forever on a thread that will not exit, and a join that never returns
 * tells the caller nothing it can act on. */
#define MOQR_ADMIN_JOIN_PROOF_TICKS 1000u

typedef struct moqr_admin_listen moqr_admin_listen_t;

/*
 * THE injected-failure vocabulary, in ONE place.
 *
 * Every site that can be made to fail is named here once, and the values come
 * from an enumeration rather than from hand-written numbers. Two files each
 * maintaining their own list is how two different sites came to share an id:
 * arming one then silently selected the other, and an oracle that compared
 * only the integer could not tell them apart. Uniqueness is now a property of
 * the construction, not of anyone's care.
 */
#define MOQR_ADMIN_TEST_SITES(X)                                              \
    X(NONE)              X(ACK_RELEASE)      X(RESERVE)                       \
    X(SETTLE)            X(BANK_RELEASE)     X(SETTLE_SILENT)                 \
    X(FAIL_SERIAL)       X(CLOSE)            X(ON_BYTES)                      \
    X(SETTLE_RELEASE)    X(NOCARRIER_RELEASE) X(TAKE_READY)                   \
    X(ACCEPT)            X(BIND_SERIAL)      X(REFUSE)                        \
    X(ON_WRITTEN)        X(TICK)             X(ABORT)                         \
    X(COMMIT)            X(RECORD_TAKE)      X(BROKER_REQUEST)

enum moqr_admin_test_site {
#define MOQR_ADMIN_TEST_SITE_ENUM(name) MOQR_ADMIN_TEST_FAIL_##name,
    MOQR_ADMIN_TEST_SITES(MOQR_ADMIN_TEST_SITE_ENUM)
#undef MOQR_ADMIN_TEST_SITE_ENUM
    MOQR_ADMIN_TEST_FAIL__COUNT
};

/* The three destructive broker-release sites, as ledger indices. */
#define MOQR_ADMIN_REL_BANK      0
#define MOQR_ADMIN_REL_SETTLE    1
#define MOQR_ADMIN_REL_NOCARRIER 2

/* Which cleanup site dropped a client slot. */
#define MOQR_ADMIN_DROP_NONE 0
#define MOQR_ADMIN_DROP_TURN 1   /* the DONE sweep in an ordinary turn */
#define MOQR_ADMIN_DROP_EXIT 2   /* the owner's exit sequence          */

/* The name of a site, for diagnostics that must say which one was reached. */
const char *moqr_admin_listen_test_site_name(int site);

/*
 * The interior view a boundary test observes, published by the listener that
 * owns it. Declared here so the test and the listener agree on one shape; the
 * accessors exist only in a build that defines MOQR_ADMIN_LISTEN_TESTING.
 */
/* The timeout the owner actually passed to poll() for the turn a view
 * describes -- the mode as executed, not as requested or acknowledged. */
typedef struct moqr_admin_listen_probe {
    unsigned hits;
    unsigned unlocked;
    unsigned foreign;
    unsigned errors;
    unsigned disagreements;
    int      poll_used_ms;
} moqr_admin_listen_probe_t;

typedef struct moqr_admin_listen_view {
    uint64_t           gen;            /* 0 from a wait means "never came"  */
    int                state[MOQR_ADMIN_MAX_CLIENTS];
    unsigned long long bytes[MOQR_ADMIN_MAX_CLIENTS];
    /* the exact generation each client joined, 0 when it has joined none */
    unsigned long long serial[MOQR_ADMIN_MAX_CLIENTS];
    int                fds[MOQR_ADMIN_MAX_CLIENTS];
    int                pins[MOQR_ADMIN_BANKS];
    /* the retirement carrier, while it exists */
    int                retiring;
    unsigned long long retire_serial;
    unsigned           retire_demand;
    unsigned           retire_bank;
    int                retire_taken;
    int                paused;
    /* which site dropped each slot, and how many times */
    unsigned char      drop_site[MOQR_ADMIN_MAX_CLIENTS];
    unsigned char      drop_count[MOQR_ADMIN_MAX_CLIENTS];
    unsigned char      drop_attempt[MOQR_ADMIN_MAX_CLIENTS];
    int                paused_at_cancel;
    /* per-site release evidence, indexed by MOQR_ADMIN_REL_* */
    unsigned           rel_attempt[3];
    unsigned           rel_ok[3];
    unsigned long long rel_serial[3];
    unsigned           rel_bank[3];
    /* what the clock probe had observed at the instant this view was
     * published: a snapshot, so a delta between two published views is exact
     * rather than racing the next sample */
    moqr_admin_listen_probe_t probe;
    /* per-resource settlement counts */
    unsigned           n_cancel;
    unsigned           n_drop;
    unsigned           n_settle;
    unsigned           n_bank_release;
    unsigned           n_ack_release;
} moqr_admin_listen_view_t;

/*
 * Test-only observation, present only where MOQR_ADMIN_LISTEN_TESTING is
 * defined. Each takes THE listener whose view it means, so a second endpoint
 * can neither satisfy nor overwrite a waiter watching the first.
 *
 * `moqr_admin_listen_test_wait_view` blocks until that listener publishes a
 * view newer than `after_gen`, and returns 0 when the publication never came.
 * `budget_ms` is a fail-closed bound, not the mechanism.
 */
uint64_t moqr_admin_listen_test_wait_view(moqr_admin_listen_t *l,
                                          uint64_t after_gen, int budget_ms,
                                          moqr_admin_listen_view_t *out);
uint64_t moqr_admin_listen_test_view(moqr_admin_listen_t *l,
                                     moqr_admin_listen_view_t *out);
void moqr_admin_listen_test_pause_at_retire(moqr_admin_listen_t *l, int on);
void moqr_admin_listen_test_pause_at_cancel(moqr_admin_listen_t *l, int on);
void moqr_admin_listen_test_freeze_client(int slot);
/*
 * THE ONLY WAY TO DRIVE THE TEST CLOCK IS TO SAY WHAT YOU EXPECT.
 *
 * Both operations refuse -- arming when no floor can be sampled, advancing
 * when the clock is unarmed or the step would wrap. Returning a result the
 * caller must act on is not enough: `if (op(...)) { }` consumes it and
 * handles nothing. So these return nothing and take the failure ledger the
 * case is judged by, and a mismatch increments it here, in the comparison.
 * There is no branch a caller can leave empty.
 *
 * The raw operations are private to the implementation: there is no symbol to
 * parenthesize, alias or take the address of.
 *
 * Arming: the requested origin is a floor, never an assignment -- it is raised
 * to real time, to the last real reading the owner used, and to whatever the
 * logical clock has already been advanced to.
 */
void moqr_admin_listen_test_clock_arm_expect(moqr_admin_listen_t *l,
                                             uint64_t now_us, bool expect_ok,
                                             const char *who, int *failures);

/* The owner's clock as it stands, for the monotonicity proof. */
void moqr_admin_listen_test_clock_state(moqr_admin_listen_t *l,
                                        int *armed,
                                        uint64_t *logical_now_us,
                                        uint64_t *last_real_us);

/* No poll mode has been requested on this listener yet. */
#define MOQR_ADMIN_POLL_NO_MODE (-2)

/*
 * Ask the owner to adopt a poll timeout, and read back what it acknowledged.
 * The acknowledgement is published only after the owner has adopted the value
 * for the poll it is about to enter, and it names the request it answers -- so
 * a caller can prove the mode changed instead of waiting to see whether
 * something stops happening.
 */
unsigned moqr_admin_listen_test_request_poll_ms(moqr_admin_listen_t *l,
                                                int ms);

/* Hold the owner at the poll capture without giving up the view lock, and wait
 * -- by event, under one deadline -- until it is standing there. */
void moqr_admin_listen_test_poll_seam(moqr_admin_listen_t *l, int on);
bool moqr_admin_listen_test_poll_seam_await(moqr_admin_listen_t *l,
                                            int budget_ms);

/*
 * Exercise the clock probe's release-refusal arm for real: the injected result
 * is returned once, after the genuine release, so the arm and its counter run
 * while the mutex is left free.
 */
void moqr_admin_listen_test_fail_probe_unlock(int on);
int  moqr_admin_listen_test_probe_unlock_consumed(void);

/* Accessor calls that could not acquire the view lock. A reader that cannot
 * take the lock releases nothing; a run in which this moves unexpectedly has
 * an accessor that was called from inside a critical section. */
int moqr_admin_listen_test_accessor_lock_failures(void);

/* Run the probe once from a caller that does not hold the view lock. */
void moqr_admin_listen_test_probe_once(moqr_admin_listen_t *l);

/* Ask the REAL view lock whether it is busy: 1 busy, 0 free, -1 unexpected. */
int moqr_admin_listen_test_view_mu_is_busy(moqr_admin_listen_t *l);

/* Publish, and await, the fact that the real lock was found busy. */
void moqr_admin_listen_test_poll_seam_contended(moqr_admin_listen_t *l);
bool moqr_admin_listen_test_poll_seam_await_contended(moqr_admin_listen_t *l,
                                                      int budget_ms);

void moqr_admin_listen_test_poll_ack(moqr_admin_listen_t *l, unsigned *gen,
                                     int *ms, int *adopted);

/* The answer to ONE request: 1 given, 0 outstanding, -1 skipped. */
int moqr_admin_listen_test_poll_ack_for(moqr_admin_listen_t *l, unsigned gen,
                                        int *ms, int *adopted,
                                        int *drain_held);

/* Wakes actually posted to the owner, counted where they are posted. */
unsigned moqr_admin_listen_test_wake_posts(moqr_admin_listen_t *l);

/*
 * What the clock probe asked the real mutex. `hits` is how many samples
 * reached it -- the positive half. `unlocked`, `foreign`, `errors` and
 * `disagreements` are the ways the step can be wrong, and all four stay zero.
 */

void moqr_admin_listen_test_clock_probe(moqr_admin_listen_t *l,
                                        moqr_admin_listen_probe_t *out);
/*
 * Advance the logical clock, and wake the owner so the step is taken now.
 *
 * Refuses two states, and changes nothing in either: a clock that is not armed
 * -- the owner is on real time, so there is no such step and no wake is sent --
 * and a step that would wrap, which is the same backwards jump the floor exists
 * to prevent arriving through the other door.
 */
void moqr_admin_listen_test_clock_advance_expect(moqr_admin_listen_t *l,
                                                 uint64_t delta_us,
                                                 bool expect_ok,
                                                 const char *who,
                                                 int *failures);

/*
 * What the comparison itself recorded, independent of whatever ledger a caller
 * supplied. A run in which either exceeds the mismatches a case deliberately
 * registered has a mismatch that reached no verdict.
 */
int moqr_admin_listen_test_clock_expect_misdirected(void);
int moqr_admin_listen_test_clock_expect_mismatches(void);

/* Refuse the sample ARMING takes, without touching the owner's clock. */
void moqr_admin_listen_test_fail_arm_clock(int on);

/*
 * The endpoint's allocator-owned footprint, derived from the private layout the
 * constructor allocates. The capacity model and the constructor share this one
 * checked descriptor; two formulas would drift.
 *
 * `object_bytes` is the ONE listener object. `body_bytes` is both banks, each
 * holding one Prometheus, one OpenMetrics and one shards JSON body sized from
 * the checked renderer bounds. `thread_stack_bytes` is a reservation, reported beside the
 * total and never inside it; kernel socket buffers are excluded likewise.
 *
 * MOQR_ERR_CAPACITY when any term overflows or the renderer refuses to bound
 * the document, so a wrapped ceiling can never be reported as smaller than the
 * truth.
 */
typedef struct moqr_admin_listen_footprint {
    /* The exact per-slot body capacity the constructor allocates (the two
     * metrics formats and the shards JSON body), so the reported ceiling and
     * the real request come from one computation. */
    uint64_t body_cap[MOQR_ADMIN_BODY__COUNT];
    uint64_t object_bytes;
    uint64_t body_bytes;
    uint64_t total_alloc_bytes;
    uint64_t thread_stack_bytes;
} moqr_admin_listen_footprint_t;

moqr_result_t moqr_admin_listen_footprint(uint32_t lanes,
                                          moqr_admin_listen_footprint_t *out);

/* -- the coordinator seams ------------------------------------------------
 *
 * The admin thread is the SOLE owner of every moqr_admin_t transition and every
 * broker delivery transaction. It reaches the rest of the process only through
 * these callbacks, and it never traverses a live core, bind, shard, route,
 * session, track or facade.
 */

/*
 * Wake every applicable raw and WT lane once, for the resolved lane map.
 * Invoked exactly once per transition that reported wake == true, and never
 * after facade terminality has been observed. Must be safe to call from the
 * admin thread.
 *
 * Returns MOQR_OK only when EVERY lane in the map was woken. A partial wake is
 * a failed wake: the generation's one notification did not reach every lane
 * that owes it a row, so the epoch can never complete and reporting success
 * would turn that into an indefinite silent stall.
 */
typedef moqr_result_t (*moqr_admin_wake_all_fn)(void *ctx);

/*
 * Establish whether the exact generation `serial` is COMPLETE, and if it is,
 * copy its rows so the render below has a coherent set that cannot change
 * underneath it.
 *
 * This is deliberately separate from rendering. A bank must not be frozen,
 * taken or reserved on the strength of a document that may turn out to be
 * incomplete: a take is destructive, and a reservation that has to be undone
 * owes a release for a token that was never issued. Completeness is decided
 * first, on copied rows read under their own row mutexes -- which is not live
 * lane traversal.
 *
 *   MOQR_OK               every row for this serial is present and valid; the
 *                         copied set is held for the matching render call.
 *   MOQR_ERR_WOULD_BLOCK  the epoch is incomplete. The generation stays
 *                         COLLECTING and nothing is frozen.
 *   anything else         the epoch is poisoned. No body may be exposed.
 */
typedef moqr_result_t (*moqr_admin_collect_fn)(void *ctx, uint64_t serial);

/*
 * Render every body slot for `serial` -- both metrics formats and the shards
 * JSON document -- from the rows the matching collect copied, into the
 * reserved bank's storage. Called only after the generation is frozen, taken
 * and its bank reserved, so it writes into storage nothing else can reach. A
 * non-OK result exposes no partial body in any slot.
 */
typedef moqr_result_t (*moqr_admin_render_fn)(void *ctx, uint64_t serial,
                                              char *const bodies[MOQR_ADMIN_BODY__COUNT],
                                              const size_t caps[MOQR_ADMIN_BODY__COUNT],
                                              size_t out_len[MOQR_ADMIN_BODY__COUNT]);

/*
 * Consume the latched signal-metrics demand, if any, returning whether it was
 * set. A signal handler can only latch a lock-free flag: assigning a serial
 * needs a mutex. Turning that flag into a broker request is a DELIVERY
 * transaction, so it belongs to the same owner as every other one -- otherwise
 * two threads open generations against one broker. May be NULL when the
 * process has no signal sink. */
typedef bool (*moqr_admin_signal_pending_fn)(void *ctx);

/*
 * A generation carrying SIGNAL demand was POISONED: it completed but must be
 * suppressed rather than rendered. The signal sink is told so explicitly --
 * silence would be indistinguishable from a generation that is merely late.
 * Called once per poisoned generation, from outside every non-yielding
 * transaction. May be NULL when no signal sink exists.
 */
typedef void (*moqr_admin_emit_suppressed_fn)(void *ctx, uint64_t serial);

/* Emit the SIGNAL projection -- the Prometheus 0.0.4 document, which is what
 * the stderr dump has always carried -- from RESERVED storage, exactly once,
 * before any commit. Its result is deliberately not branched on by the owner: a short or
 * failed sink write never withdraws an otherwise valid HTTP body, and a partial
 * write is never retried. May be NULL when no signal sink exists. */
typedef void (*moqr_admin_emit_signal_fn)(void *ctx, uint64_t serial,
                                          const char *body, size_t len);

typedef struct moqr_admin_listen_cfg {
    const moqr_cli_admin_t   *admin;   /* the resolved config section       */
    uint32_t                  lanes;   /* for the renderer bound            */
    moqr_broker_t            *broker;  /* BY REFERENCE: one identity space  */
    moqr_admin_wake_all_fn    wake_all;
    moqr_admin_collect_fn     collect;
    moqr_admin_render_fn      render;
    /* These three are one optional contract: all NULL, or all non-NULL. */
    moqr_admin_signal_pending_fn signal_pending;
    moqr_admin_emit_signal_fn emit_signal;
    moqr_admin_emit_suppressed_fn emit_suppressed;
    void                     *ctx;
    /* The immutable /api/v1/info document: complete bytes, frozen by the
     * caller BEFORE the endpoint is created and owned by the caller until
     * after the endpoint has been destroyed (every writing client and the
     * joined owner borrow it). Required: a NULL or empty document, or one
     * longer than MOQR_ADMIN_MAX_STATIC_DOC, refuses construction without
     * reading it, since the endpoint advertises the target. */
    const char               *info;
    size_t                    info_len;
    /* TEST-ONLY: permit port 0 so a boundary test can take an ephemeral port.
     * The production parser refuses port 0, so this cannot be reached from a
     * configuration file. */
    bool                      allow_ephemeral_port;
} moqr_admin_listen_cfg_t;

/*
 * PHASE ONE. Prepare the endpoint: bind the address, arm the wake, and spawn
 * the owner thread PARKED at an activation gate.
 *
 * The socket is bound but NOT listening, and the owner processes nothing, so
 * after this call no connection can be accepted, no generation opened and no
 * lane woken. That matters because the caller still has work to do before the
 * endpoint may run: a lane's publication notification reaches the owner through
 * a pointer the caller has not installed yet, and a generation opened before
 * that install would lose its only notification.
 *
 * `moqr_admin_listen_port` is valid after this returns, so the caller can name
 * the bound port while the endpoint is still inert.
 *
 * ALL-OR-NOTHING. `*out` is set to NULL before any acquisition, every acquired
 * resource is recorded in an initialization ledger, and a failure unwinds only
 * what this call acquired, in strict reverse order.
 */
moqr_result_t moqr_admin_listen_start(const moqr_admin_listen_cfg_t *cfg,
                                      moqr_admin_listen_t **out);

/*
 * PHASE TWO. Begin listening and release the owner, then WAIT for it to report
 * that it has entered its serving loop.
 *
 * Starting a thread is not a readiness acknowledgement: the owner can fail its
 * very first clock read or poll. This call returns only once the owner has
 * either begun serving or failed, and reports which -- so a readiness line is
 * never printed over an endpoint that is already dead. On failure the endpoint
 * is stopped and the caller must not serve.
 *
 * Call it only after every pointer the owner's callbacks reach has been
 * installed.
 *
 * MOQR_ERR_WOULD_BLOCK carries the SAME meaning it does on stop: the owner
 * this call started could not be proved dead, so nothing it can reach may be
 * touched or freed. A failed activation is not a licence to tear down.
 */
moqr_result_t moqr_admin_listen_activate(moqr_admin_listen_t *l);

/*
 * The endpoint's health, as one named state.
 *
 * The owner can reach a condition from which it cannot honestly serve: a clock
 * that stopped answering, a poll that failed, or an invariant contradiction
 * between the broker and the state machine. Continuing to advertise /metrics
 * after that is a lie, so the owner records exactly which one it was, stops,
 * and the serve loop fails the whole relay.
 */
typedef uint32_t moqr_admin_listen_health_t;

#define MOQR_ADMIN_HEALTH_OK          0u  /* serving                         */
#define MOQR_ADMIN_HEALTH_STOPPED     1u  /* asked to stop; not a failure    */
#define MOQR_ADMIN_HEALTH_NO_CLOCK    2u  /* the monotonic clock refused     */
#define MOQR_ADMIN_HEALTH_POLL_FAILED 3u  /* the readiness wait failed       */
#define MOQR_ADMIN_HEALTH_INVARIANT   4u  /* broker/admin disagreement       */
#define MOQR_ADMIN_HEALTH_WAKE_FAILED 5u  /* a lane wake could not be issued */
#define MOQR_ADMIN_HEALTH_SERIAL_EXHAUSTED 6u /* no generation id remains    */

/* The endpoint's current health. MOQR_ADMIN_HEALTH_OK for a NULL endpoint --
 * an endpoint that does not exist cannot be unhealthy, and the caller's own
 * "is it enabled" test is what decides whether to ask. */
moqr_admin_listen_health_t
moqr_admin_listen_health(const moqr_admin_listen_t *l);

/* True once the owner thread has exited for ANY reason. A caller polls this to
 * notice a dead endpoint without joining it. */
bool moqr_admin_listen_owner_exited(const moqr_admin_listen_t *l);

/*
 * Request cancellation, wake the owner, and JOIN it. The owner performs the
 * cancellation and settlement itself; nothing else ever mutates the state
 * machine.
 *
 * The endpoint OBJECT stays alive. Stopping and destroying are separate on
 * purpose: a lane can still be publishing while the facades are being joined,
 * and its notification must remain safe to issue until that is finished. After
 * this returns, `moqr_admin_listen_notify` is a defined no-op rather than a
 * use-after-free.
 *
 * THREE OUTCOMES, and the caller must distinguish all three:
 *
 *   MOQR_OK            the owner is dead and it stopped because it was asked
 *                      to. Ordinary ordered teardown is safe.
 *   MOQR_ERR_INTERNAL  the owner is DEAD, but it had already failed --
 *                      `moqr_admin_listen_health` says why. Ordinary ordered
 *                      teardown is still safe; the relay should not keep
 *                      serving.
 *   MOQR_ERR_WOULD_BLOCK
 *                      the owner's death CANNOT BE PROVED. It may still be
 *                      running, and it can reach the caller's snapshot, broker,
 *                      lane facades and callback context -- not just this
 *                      object. NOTHING that owner can reach may be touched or
 *                      freed. There is no recovery from here: the only correct
 *                      response is to stop the process without further
 *                      teardown. `moqr_admin_listen_owner_unjoinable` reports
 *                      the same fact for a caller that did not keep the result.
 *
 * The distinction exists because the dependency closure is much larger than the
 * listener object. Freeing a snapshot, destroying a broker or joining a facade
 * while a live owner still holds them is a use-after-free in code that has
 * nothing to do with this file.
 */
moqr_result_t moqr_admin_listen_stop(moqr_admin_listen_t *l);

/* True when a stop could not prove the owner dead. Everything that owner can
 * reach must be treated as still in use, for the life of the process. */
bool moqr_admin_listen_owner_unjoinable(const moqr_admin_listen_t *l);

/*
 * Release everything the endpoint owns. Must follow a successful
 * `moqr_admin_listen_stop`; calling it while the owner thread might still be
 * running is refused rather than performed, because freeing storage a live
 * thread can reach is not recoverable. Deliberately leaks the object in that
 * case: an unjoinable thread's memory is not ours to reuse.
 */
void moqr_admin_listen_destroy(moqr_admin_listen_t *l);

/* Facade terminality observed: no further lane wakes are issued and pending
 * generations are cancelled so waiters fail closed. */
void moqr_admin_listen_note_terminal(moqr_admin_listen_t *l);

/*
 * Notify the owner that lane publication advanced.
 *
 * THIS IS THE PRODUCTION PUBLICATION NOTIFICATION, called by a lane at the
 * point it actually publishes a row -- not by the wake callback, which only
 * asks lanes to publish later. Level-triggered: it posts to the owner's wake
 * pipe, so a publish landing between the owner's check and its wait is still
 * pending when the wait runs.
 *
 * Safe on a NULL endpoint, and safe after `moqr_admin_listen_stop` -- a lane
 * may publish while the facades are still being joined. The poll timeout is a
 * safety bound for a lost notification, not a substitute for this call.
 */
void moqr_admin_listen_notify(moqr_admin_listen_t *l);

/* Diagnostics. No live state is exposed. */
uint64_t moqr_admin_listen_accepts(const moqr_admin_listen_t *l);
uint64_t moqr_admin_listen_refusals(const moqr_admin_listen_t *l);
uint64_t moqr_admin_listen_wakes(const moqr_admin_listen_t *l);
int      moqr_admin_listen_port(const moqr_admin_listen_t *l);

#endif /* MOQR_CLI_ADMIN_LISTEN_H */
