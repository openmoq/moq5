/*
 * The listener's private layout.
 *
 * CLI-private: this header exists so the capacity descriptor and the
 * constructor derive `object_bytes` from ONE definition. Two formulas would
 * drift, and the reported ceiling would stop describing the real allocation.
 *
 * It is a layout header only. It declares no functions and pulls in no socket
 * or thread implementation, so the capacity model can learn a size without ten
 * config consumers linking a listener.
 */
#ifndef MOQR_CLI_ADMIN_LISTEN_LAYOUT_H
#define MOQR_CLI_ADMIN_LISTEN_LAYOUT_H

#include "admin_listen.h"
#include "broker.h"
#include "config.h"
#include "../admin/moqr_admin.h"

#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>

/* The fixed 503 the refusal slot writes. Sized for the head the renderer
 * produces for that status; the slot never parses and never renders. */
#define MOQR_ADMIN_REFUSE_HEAD_MAX 256u

/* Every resource the constructor may acquire, so unwind is exact and in strict
 * reverse order rather than "close whatever looks open". */
#define MOQR_ADMIN_LEDGER_BODIES  (1u << 0)
#define MOQR_ADMIN_LEDGER_ADMIN   (1u << 1)
#define MOQR_ADMIN_LEDGER_SOCKET  (1u << 2)
#define MOQR_ADMIN_LEDGER_WAKE    (1u << 3)
#define MOQR_ADMIN_LEDGER_THREAD  (1u << 4)
#define MOQR_ADMIN_LEDGER_GATE    (1u << 5)
#define MOQR_ADMIN_LEDGER_VIEW_MU (1u << 6)
#define MOQR_ADMIN_LEDGER_VIEW_CV (1u << 7)
#define MOQR_ADMIN_LEDGER_SEAM_MU (1u << 8)
#define MOQR_ADMIN_LEDGER_SEAM_CV (1u << 9)

struct moqr_admin_listen {
    /* resolved endpoint identity */
    int      port;

    /* descriptors */
    int      listen_fd;
    int      wake_r;
    int      wake_w;
    int      client_fd[MOQR_ADMIN_MAX_CLIENTS];

    /* the ONE bounded refusal slot, outside the normal client table. It never
     * parses a request, never takes a broker generation, never pins a body
     * bank, and never allocates. */
    int      refuse_fd;
    char     refuse_head[MOQR_ADMIN_REFUSE_HEAD_MAX];
    size_t   refuse_len;
    size_t   refuse_sent;
    uint64_t refuse_start_us;
    /* Once the 503 is fully written the slot DRAINS: a close with unread
     * inbound bytes sends RST on the BSD sockets this runs on, which discards
     * the response that was just written. This state is entered only after a
     * successful half-close; a failed shutdown drops the slot immediately. */
    bool     refuse_draining;

    /* the frozen tiers. The broker is held BY REFERENCE: there is exactly one
     * identity space in the process, shared with signal demand. */
    moqr_admin_t   admin;
    moqr_broker_t *broker;
    char          *bodies[MOQR_ADMIN_BANKS][MOQR_ADMIN_BODY__COUNT];
    size_t         caps[MOQR_ADMIN_BODY__COUNT];

    /* coordinator seams. Stored as their own function-pointer types: a round
     * trip through void * is not a conversion ISO C defines. */
    moqr_admin_wake_all_fn    wake_all;
    moqr_admin_collect_fn     collect;
    moqr_admin_render_fn      render;
    moqr_admin_signal_pending_fn signal_pending;
    moqr_admin_emit_signal_fn emit_signal;
    moqr_admin_emit_suppressed_fn emit_suppressed;
    void                     *ctx;

    /* thread and control */
    pthread_t     thread;
    atomic_int    stop;
    atomic_int    terminal;
    atomic_int    owner_done;
    /* Why the owner stopped. Written only by the owner, read by anyone. */
    atomic_uint   health;
    /* True only when pthread_join proved the owner dead. Storage is never
     * freed without it. */
    bool          owner_joined;
    /* Set when a stop could NOT prove the owner dead. Everything that owner
     * can reach -- not just this object -- must then be treated as still in
     * use for the life of the process. */
    atomic_int    owner_unjoinable;

    /* The activation gate. The owner is created parked and processes nothing
     * until the caller has installed every pointer its callbacks reach; it
     * then reports whether it entered its serving loop, so a readiness line is
     * never printed over an endpoint that already failed. */
    pthread_mutex_t gate_mu;
    pthread_cond_t  gate_cv;
    bool            gate_open;      /* the caller released the owner        */
    int             gate_report;    /* 0 pending, 1 serving, -1 failed      */
    /* Cancellation on terminality happens ONCE. Re-cancelling every turn would
     * also destroy connections that arrived afterwards before they could be
     * told 503. */
    bool          terminal_cancelled;
    uint32_t      ledger;

    /* Generations poisoned by a failed collect, awaiting retirement. A
     * poisoned SIGNAL generation owes its sink an explicit suppression
     * diagnostic, and that is only knowable from the authoritative token --
     * which is taken later, in the settle pass. Bounded by the generation
     * count; a serial of 0 is an empty entry. */
    uint64_t      poisoned[MOQR_ADMIN_MAX_CLIENTS];

#ifdef MOQR_ADMIN_LISTEN_TESTING
    /*
     * THIS listener's published interior view, and the settlement counters
     * that make "exactly once" arithmetic rather than a final empty state.
     *
     * Owned by the listener, not by the process: a second endpoint publishes
     * into its own object, so one cannot satisfy or overwrite a waiter for the
     * other. The generation counter is monotonic and the condition variable is
     * the wake, so a reader waits on PUBLICATION rather than on elapsed time.
     */
    unsigned long long view_serial[MOQR_ADMIN_MAX_CLIENTS];
    moqr_admin_listen_probe_t view_probe;
    pthread_mutex_t    view_mu;
    pthread_cond_t     view_cv;
    uint64_t           view_gen;
    int                view_state[MOQR_ADMIN_MAX_CLIENTS];
    unsigned long long view_bytes[MOQR_ADMIN_MAX_CLIENTS];
    int                view_fds[MOQR_ADMIN_MAX_CLIENTS];
    int                view_pins[MOQR_ADMIN_BANKS];
    /* The retirement carrier, while it exists. */
    int                view_retiring;
    unsigned long long view_retire_serial;
    unsigned           view_retire_demand;
    unsigned           view_retire_bank;
    int                view_retire_taken;
    /* Which site dropped each slot, and how many times. Aggregate counts
     * cannot tell two cleanup sites apart, and a final empty table cannot tell
     * one drop from three. */
    unsigned char      drop_site[MOQR_ADMIN_MAX_CLIENTS];
    unsigned char      drop_count[MOQR_ADMIN_MAX_CLIENTS];
    unsigned char      drop_attempt[MOQR_ADMIN_MAX_CLIENTS];
    /* A pause immediately after terminal cancellation, before any cleanup. */
    int                pause_at_cancel;
    int                paused_at_cancel;
    /* Per-site release evidence: attempts and successes, with the identity of
     * the last one, so a scenario can prove exactly one release through the
     * expected site and none through the others. */
    unsigned           rel_attempt[3];
    unsigned           rel_ok[3];
    unsigned long long rel_serial[3];
    unsigned           rel_bank[3];

    /* Per-resource settlement counts. */
    unsigned           n_cancel;
    unsigned           n_drop;
    unsigned           n_settle;
    unsigned           n_bank_release;
    unsigned           n_ack_release;
    /* A test-owned logical clock: the owner reads this instead of the real
     * one, so an interior deadline is a state the test steps rather than a
     * wall-clock interval it waits out. */
    int                logical_clock;
    uint64_t           logical_now_us;
    /* The last real time the owner actually observed. The logical clock is
     * floored against it, so arming can never move the owner backwards. */
    uint64_t           last_real_us;
    /*
     * The clock-choice probe.
     *
     * `hits` is positive evidence that the sample point was reached at all --
     * a counter that only ever stays at zero proves nothing, including when
     * the probe has been deleted. `unlocked` and `foreign` are the two
     * distinct ways the step can be wrong: nobody holds the view lock, or
     * somebody other than this thread does. Ownership is recorded by the same
     * checked boundary the clock code runs inside, so contention is never
     * mistaken for ownership.
     */
    /* Every wake actually posted to the owner, counted at the boundary that
     * posts it. A test asking "was the owner woken" is asking about this, not
     * about whether a view happened to arrive within some interval. */
    unsigned           wake_posts;
    unsigned           clock_probe_hits;
    unsigned           clock_unlocked_samples;
    unsigned           clock_foreign_holder_samples;
    unsigned           clock_probe_errors;
    unsigned           clock_marker_disagreements;
    /*
     * A poll-mode request the owner ACKNOWLEDGES. The test asks for a
     * timeout; the owner adopts it for the poll it is about to enter and only
     * then publishes the acknowledgement, carrying both the request it is
     * answering and the value it actually adopted. Nothing here is a timeout
     * standing in for a state.
     */
    unsigned           poll_req_gen;
    int                poll_req_ms;
    /* The last few acknowledgements, so an observer that looks after a later
     * one has landed can still find its own -- while a generation that was
     * genuinely never answered stays absent. */
#define MOQR_ADMIN_POLL_ACK_HIST 8u
    struct {
        unsigned gen;
        int      ms;
        int      adopted;
        int      drain_held;
    }                  poll_ack_hist[MOQR_ADMIN_POLL_ACK_HIST];
    unsigned           poll_ack_hist_n;
    /* Whether the owner still owned the view lock at the moment it drained. */
    int                poll_drain_held;
    unsigned           poll_ack_gen;
    int                poll_ack_ms;
    int                poll_adopted_ms;
    /* The timeout actually handed to poll() for the turn a view describes. */
    int                poll_used_ms;
    /*
     * A rendezvous at the poll capture, on its OWN lock: the owner keeps
     * view_mu the whole time it is held here, so a request that arrives
     * meanwhile is genuinely blocked rather than merely late.
     */
    int                poll_seam_armed;
    int                poll_seam_held;
    /* A second party has attempted the REAL view lock and found it busy. */
    int                poll_seam_contended;
    pthread_mutex_t    seam_mu;
    pthread_cond_t     seam_cv;
    int                view_held;
    pthread_t          view_holder;

    /* A deterministic pause at the cancellation/retirement boundary. */
    int                pause_at_retire;
    int                paused;
#endif

    /* counters */
    atomic_ullong accepts;
    atomic_ullong refusals;
    atomic_ullong wakes;
};

#endif /* MOQR_CLI_ADMIN_LISTEN_LAYOUT_H */
