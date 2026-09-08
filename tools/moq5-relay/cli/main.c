/*
 * MOQ5 Relay: a deterministic MoQ relay over the MsQuic managed server.
 * Sessions live on their lane's thread; the production binding
 * (moq/relay/moqr_bind.h) runs inside on_lane_pump and is the only code that
 * touches them. Default is one lane / one shard (the single-core production
 * path); listener.lanes > 1 partitions connections across N independent
 * lock-domain lanes, each driving one relay shard.
 *
 * Usage:
 *   moq5-relay serve    --config relay.json
 *   moq5-relay capacity --config relay.json     (print the ceiling, exit)
 */

#include "admin_listen.h"
#include "info_doc.h"
#include "shards_doc.h"
#include "cliargs.h"
#include "broker.h"
#include "config.h"
#ifdef MOQR_DUAL_LISTENER
#include <moq/wtquic_msquic_managed.h>
#endif

#include "conn_reap.h"
#ifdef MOQR_DUAL_LISTENER
#include "wtcfg.h"
#endif
#include "versions.h"
#include "lanestats.h"
#include "logevent.h"
#include "servelog.h"

/* The sink's I/O seam: production hands the real functions; the test build
 * may hand scripted ones (see the seams at the end of this file). */
#ifdef MOQR_PUMP_TESTING
static const moqr_cli_log_io_t *g_test_serve_log_io;
static uint8_t g_test_serve_log_last_state;
#define SERVE_LOG_IO g_test_serve_log_io
#else
#define SERVE_LOG_IO NULL
#endif
#include "snapshot.h"
#ifdef MOQR_BIND_TESTING
#include "blockedstats.h"   /* verify build only: RELAY_BLOCKED_V0 emission */
#endif
#ifdef MOQR_VERIFY_SEAM
/* The constrained-pool seam is compiled into two binaries (config.h): the
 * reason-proof verify build and the symmetric timing build. Their messages
 * self-identify so a harness log never leaves which binary ran ambiguous. */
#ifdef MOQR_BIND_TESTING
#define MOQR_SEAM_NAME "moq-relay-verify"
#else
#define MOQR_SEAM_NAME "moq-relay-measure"
#endif
#endif

#include <moq/relay/moqr_bind.h>
#ifdef MOQR_BIND_TESTING
#include <moq/relay/moqr_bind_test.h>
#endif
#include <moq/relay/moqr_shards.h>
#if defined(MOQR_RELAY_INSPECT) || defined(MOQR_BIND_TESTING)
#include <moq/relay/moqr_shards_test.h>
#endif

#include <moq/relay/moqr_obs.h>

#include <moq/msquic_managed.h>

#include <inttypes.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <poll.h>
#include <pthread.h>
#include <time.h>

/* The signal flags are written from a signal handler (delivered on an
 * arbitrary thread) and read from both the lane thread (relay_lane_pump)
 * and the main thread (the wait loop). volatile sig_atomic_t is only
 * async-signal-safe within one thread, not a cross-thread synchronization
 * primitive — so use lock-free atomics. int is lock-free on every supported
 * target, which a handler may safely touch (C11 7.14.1.1). */
_Static_assert(ATOMIC_INT_LOCK_FREE == 2,
               "signal flags require an always-lock-free atomic_int");
/* ATOMIC_INT_LOCK_FREE governs atomic_uint too (C11 groups signed/unsigned
 * int), so the multi-lane dump epochs below are equally handler-safe. */

/* The config lane cap is decoupled from the shard runtime; prove they agree
 * here, the one TU that sees both headers. */
_Static_assert(MOQR_CLI_MAX_LANES == MOQR_SHARDS_MAX,
               "listener.lanes cap must equal the shard runtime cap");

static atomic_int g_stop;

/*
 * Dump demand, kept separate from publication identity.
 *
 * A handler cannot walk a per-lane heap context, and assigning a generation
 * serial needs a mutex, so a handler only latches a lock-free flag. Ordinary
 * code turns that flag into a broker request, and the broker assigns the
 * serial the lanes publish under. Bumping one shared counter from the handler
 * would fuse "a generation exists" with "stderr asked for it", which is how a
 * metrics scrape ends up printing an unsolicited dump.
 *
 * `g_route_dump_epoch` stays a plain counter because it is signal-only: it
 * carries SIGUSR1's routes/journal half, which no other sink requests.
 */
static atomic_int  g_signal_metrics_pending;
static atomic_uint g_route_dump_epoch;
static atomic_uint g_dump_trace_epoch;

/* The broker owns the metrics generation serial for the multi-lane serve. */
static moqr_broker_t g_metrics_broker;

static void
on_signal(int sig)
{
    (void)sig;
    atomic_store(&g_stop, 1);
}

/*
 * Async-signal-safe: latch only. SIGUSR1 asks for metrics AND routes, so it
 * raises the metrics demand flag and bumps the routes epoch; the metrics
 * generation's serial is assigned later, off the handler, by the broker.
 *
 * Both serve compositions share this: the demand a signal expresses is the
 * same whether or not a shard runtime exists underneath.
 */
static void
on_dump_signal(int sig)
{
    if (sig == SIGUSR1) {
        atomic_store(&g_signal_metrics_pending, 1);
        atomic_fetch_add(&g_route_dump_epoch, 1u);
    } else if (sig == SIGUSR2) {
        atomic_fetch_add(&g_dump_trace_epoch, 1u);
    }
}

/* The multi-lane path installs its own named handler so the signal-order gate
 * can pin each composition independently; the latch itself is identical. */
static void
on_dump_signal_lanes(int sig)
{
    on_dump_signal(sig);
}

/* Readiness implies a graceful stop.
 *
 * The handlers go in BEFORE the listening record is published, because that
 * record is what an operator waits on: an immediate SIGTERM must take the
 * graceful path that drains, emits terminal lane statistics and exits 0, not
 * the default disposition. Publishing readiness first leaves a window in which
 * the advertised process dies abruptly instead.
 *
 * `g_stop` is cleared here as well: the flag is process-global, and a serve
 * invocation must never inherit a stop latched before it started.
 */
static void
serve_install_signals(void (*dump)(int))
{
    atomic_store(&g_stop, 0);
    signal(SIGINT, on_signal);
    signal(SIGTERM, on_signal);
    signal(SIGUSR1, dump);
    signal(SIGUSR2, dump);
}

/* The coordinator targets whichever generation the broker has open. */
static uint64_t
coord_epoch(void *unused)
{
    (void)unused;
    return moqr_broker_epoch_fn(&g_metrics_broker);
}

/*
 * The generation lifecycle both coordinators share.
 *
 * `coord_retire` runs while the rendered bytes are STILL VALID and takes the
 * exact serial it rendered. Two rules meet here and neither may be relaxed:
 *
 *  - The document is borrowed. `moqr_cli_coord_dump` frees its buffer as soon
 *    as it returns, so anything that reads those bytes must do so before then.
 *    Storing the pointer and using it afterwards is a use-after-free.
 *  - The token must name the generation that produced the bytes. Taking
 *    whatever is oldest would pair one generation's demand and bank with
 *    another's document — delivering the wrong body to the wrong requester and
 *    stranding the rendered generation READY forever.
 */
typedef struct coord_retire_ctx {
    uint64_t    target;      /* the generation being rendered      */
    const char *banner;      /* stderr banner for a signal sink    */
    bool        wake_lanes;  /* release opened deferred work       */
    bool        retired;     /* the exact token was taken and released */
} coord_retire_ctx_t;

#ifdef MOQR_PUMP_TESTING
static void (*g_coord_midflight)(void *);
static void  *g_coord_midflight_ctx;
static void (*g_coord_pinned)(void *);
static void  *g_coord_pinned_ctx;
static bool   g_corrupt_release_bank;

/* Fired between the early demand observation and rendering, so a test can land
 * a join in exactly the window where dispatching from the early sample would
 * lose a requester. Never compiled into a shipped binary. */
static void
coord_midflight(void)
{
    if (g_coord_midflight != NULL) {
        g_coord_midflight(g_coord_midflight_ctx);
    }
}

/* Fired while the generation's bank is pinned, which is the only window in
 * which a new demand finds no collecting generation and no free bank -- and so
 * the only way to reach the deferred-work path deterministically. */
static void
coord_pinned(void)
{
    if (g_coord_pinned != NULL) {
        g_coord_pinned(g_coord_pinned_ctx);
    }
}
#else
static void
coord_midflight(void)
{
}
static void
coord_pinned(void)
{
}
#endif

/*
 * Freeze the rendered generation, take ITS token, dispatch by the frozen
 * demand, and release. `doc` may be NULL when the generation was suppressed
 * rather than rendered; it is only read while this call is on the stack.
 */
static void
coord_retire(coord_retire_ctx_t *c, const char *doc, uint64_t epoch,
             bool suppressed)
{
    moqr_broker_on_complete(&g_metrics_broker, c->target);
    uint32_t demand = 0;
    uint32_t bank = 0;
    if (!moqr_broker_take_serial(&g_metrics_broker, c->target, &demand,
                                 &bank)) {
        /* Structurally unreachable while one owner completes and takes its own
         * generation: on_complete just froze this serial, and only this thread
         * takes it. Recorded rather than assumed, so a future second owner
         * cannot make the generation silently count as rendered. */
        return;
    }
    if ((demand & MOQR_BROKER_DEMAND_SIGNAL) != 0u) {
        if (suppressed) {
            /* A lane published a refused shard snapshot, or an
             * internal-entity exclusion went negative: diagnose and consume,
             * never floored, never a zeroed stand-in, never spun on. */
            fprintf(stderr,
                    "MOQ5 Relay: metrics epoch %llu suppressed — a lane's "
                    "shard snapshot was refused or an internal-entity "
                    "exclusion went negative (snapshot contradicts itself)\n",
                    (unsigned long long)epoch);
        } else if (doc != NULL) {
            fprintf(stderr, c->banner, (unsigned long long)epoch, doc);
        }
    }
    /* An HTTP requester's own status is delivered by the admin surface from
     * this same token; nothing about it belongs on stderr. */
    coord_pinned();
    bool wake = false;
    /* Release is authenticated too, so its result decides retirement. A
     * failed release leaves the slot SENDING; recording the epoch as rendered
     * anyway would be exactly the silent false completion the token model
     * exists to prevent, and the generation would never be retried. */
    uint32_t release_bank = bank;
#ifdef MOQR_PUMP_TESTING
    if (g_corrupt_release_bank) {
        release_bank = bank + 7u;   /* never a valid bank index */
    }
#endif
    if (moqr_broker_release(&g_metrics_broker, c->target, release_bank,
                            &wake) != MOQR_OK) {
        return;
    }
    c->wake_lanes = c->wake_lanes || wake;
    c->retired = true;
}

/* The licensed document arrives here with its bytes still owned by the dump,
 * so the whole retirement happens inside the callback. */
static void
coord_emit_retire(void *ectx, const char *doc, size_t len, uint64_t epoch)
{
    (void)len;
    coord_retire((coord_retire_ctx_t *)ectx, doc, epoch, false);
}

/*
 * One coordinator attempt at the outstanding metrics generation, entirely
 * through the CLI-private moqr_cli_coord_dump seam (which, by construction, is
 * never handed the shard runtime -- the rows are all it can read).
 *
 * Both serve compositions call this: the single-lane path passes its one label
 * set, the multi-lane path its per-shard array. `*out_wake_lanes` is set when
 * retiring the generation opened deferred work, which is the caller's only
 * notice that lanes must be woken.
 */
static uint64_t
coord_try_dump_labels(const moqr_obs_labels_t *labels,
                      moqr_cli_snapshot_t *snap, uint64_t rendered,
                      uint64_t *incomplete_told, uint64_t *fail_told,
                      bool *out_wake_lanes)
{
    if (out_wake_lanes != NULL) {
        *out_wake_lanes = false;
    }
    uint64_t target = 0;
    uint32_t target_demand = 0;
    if (!moqr_broker_current(&g_metrics_broker, &target, &target_demand)) {
        return rendered;   /* nothing collecting */
    }

    coord_midflight();

    coord_retire_ctx_t rc_ctx = {
        target, "\n===== MOQ5 Relay metrics (epoch %llu) =====\n%s\n", false,
        false
    };
    uint64_t      epoch = 0;
    moqr_result_t rc = moqr_cli_coord_dump(snap, labels, coord_epoch, NULL,
                                           coord_emit_retire, &rc_ctx, &epoch);

    if (rc == MOQR_ERR_INVAL) {
        /* Suppressed: no document exists, but the generation still finished
         * and must be retired or its slot would never free. */
        coord_retire(&rc_ctx, NULL, epoch, true);
    }
    if ((rc == MOQR_OK || rc == MOQR_ERR_INVAL) && rc_ctx.retired) {
        rendered = epoch;
        if (out_wake_lanes != NULL) {
            *out_wake_lanes = rc_ctx.wake_lanes;
        }
    } else if (rc == MOQR_ERR_WOULD_BLOCK) {
        /* Retryable, and addressed: only a generation stderr asked for
         * reports its incompleteness there. */
        if ((target_demand & MOQR_BROKER_DEMAND_SIGNAL) != 0u &&
            *incomplete_told != epoch) {
            fprintf(stderr,
                    "MOQ5 Relay: metrics epoch %llu incomplete — waiting "
                    "for every lane to publish\n",
                    (unsigned long long)epoch);
            *incomplete_told = epoch;
        }
    } else if ((target_demand & MOQR_BROKER_DEMAND_SIGNAL) != 0u &&
               *fail_told != epoch) {
        /* Unexpected but retryable (e.g. a transient allocation failure):
         * the generation stays outstanding and retries on the wait cadence —
         * diagnosed once per generation, never a silent spin. */
        fprintf(stderr,
                "MOQ5 Relay: metrics epoch %llu dump failed (%d) — "
                "retrying\n",
                (unsigned long long)epoch, (int)rc);
        *fail_told = epoch;
    }
    return rendered;
}

/* -- the admin endpoint's owner context ------------------------------------
 *
 * When the admin endpoint is enabled it becomes the SOLE owner of every broker
 * delivery transaction, including the one the latched signal demand opens. The
 * main loop stops running the coordinator entirely: two owners against one
 * broker is the defect this arrangement exists to prevent.
 *
 * Everything here runs on the admin thread and touches nothing but the copied
 * snapshot rows -- never relay, bind, shard, route, session or facade state. */
typedef struct admin_owner {
    moqr_cli_snapshot_t     *snap;
    const moqr_obs_labels_t *labels;   /* `lanes` entries                  */
    uint32_t                 lanes;
    moqr_cli_snapshot_stats_t *rows;   /* preallocated, `lanes` entries    */
    moqr_snapshot_view_t      *views;  /* preallocated, `lanes` entries    */
    uint64_t                  collected;  /* the serial `rows` describes   */
    /* The signal sink's document: the FROZEN Prometheus exposition, rendered
     * from the same copied rows as the admin banks but by the legacy writer,
     * so enabling the endpoint moves no byte an existing consumer parses.
     * Preallocated one Prometheus document wide; rendered per signal demand
     * from the frozen rows, never from live lane state. */
    char                     *signal_body;
    size_t                    signal_cap;
    /* The immutable /api/v1/info document: rendered ONCE here, before the
     * endpoint exists, from the configuration and the described capacity;
     * borrowed by the endpoint for its whole life and freed only after the
     * owner has been joined. Never rewritten. */
    char                     *info;
    size_t                    info_len;
    size_t                    info_cap;
    moqr_result_t           (*wake)(void *);
    void                     *wake_ctx;
} admin_owner_t;

/* Wake the single lane of the raw MsQuic facade. The endpoint's only reach
 * into the transport is this one call. */
static moqr_result_t
serve_wake_lane0(void *ctx)
{
    moq_msquic_managed_t *t = (moq_msquic_managed_t *)ctx;
    if (t == NULL) {
        return MOQR_ERR_INVAL;
    }
    return moq_msquic_lane_wake(moq_msquic_managed_lane(t, 0)) == MOQ_OK
               ? MOQR_OK
               : MOQR_ERR_INTERNAL;
}

/*
 * Whether this build may serve the admin endpoint at all.
 *
 * The verify composition emits RELAY_BLOCKED_V0 on SIGUSR1; the endpoint's
 * signal projection is the Prometheus exposition. Enabling one would silently
 * replace the other, so the two do not coexist. Expressed as a constant rather
 * than a preprocessor branch so both arms are compiled in every build and
 * neither can rot.
 */
#ifdef MOQR_BIND_TESTING
#define MOQR_ADMIN_ENDPOINT_AVAILABLE 0
#else
#define MOQR_ADMIN_ENDPOINT_AVAILABLE 1
#endif

/*
 * The production publication notification.
 *
 * A lane calls this at the point it has actually published its row -- which is
 * the only moment that can tell the owner an epoch may now be complete. The
 * wake callback is the opposite direction: it ASKS lanes to publish, and a
 * notification issued there would announce work that has not happened yet.
 *
 * The pointer is published once, before any generation can exist, and cleared
 * only after every facade lane has joined. It is read atomically because lanes
 * run on their own threads, and `moqr_admin_listen_notify` stays valid after
 * the owner has been joined -- the endpoint object outlives the facades on
 * purpose.
 */
static void
admin_notify_published(_Atomic(moqr_admin_listen_t *) *slot)
{
    moqr_admin_listen_t *l = atomic_load_explicit(slot, memory_order_acquire);
    if (l != NULL) {
        moqr_admin_listen_notify(l);
    }
}

static moqr_result_t
admin_wake_all(void *ctx)
{
    admin_owner_t *o = (admin_owner_t *)ctx;
    if (o->wake == NULL) {
        return MOQR_ERR_INVAL;
    }
    return o->wake(o->wake_ctx);
}

static bool
admin_signal_pending(void *ctx)
{
    (void)ctx;
    /* The handler latches; the owner converts. Exchanging here consumes the
     * latch exactly once. */
    return atomic_exchange(&g_signal_metrics_pending, 0) != 0;
}

static moqr_result_t
admin_collect(void *ctx, uint64_t serial)
{
    admin_owner_t *o = (admin_owner_t *)ctx;
    uint64_t epoch = 0;
    moqr_result_t rc;

    o->collected = 0;
    rc = moqr_cli_snapshot_collect(o->snap, coord_epoch, NULL, o->rows,
                                   &epoch);
    if (rc != MOQR_OK) {
        return rc;   /* WOULD_BLOCK = incomplete; anything else is poison */
    }
    if (epoch != serial) {
        /* A newer generation was requested during the copy. The set that came
         * back is not this generation's, so nothing here may be frozen. */
        return MOQR_ERR_WOULD_BLOCK;
    }
    /* Poison is decided on the copied rows, before a bank is taken: a REFUSED
     * or unknown capability is a broken epoch, not a late one. */
    for (uint32_t i = 0; i < o->lanes; i++) {
        if (o->rows[i].shard_cap != MOQR_CLI_CAP_ABSENT &&
            o->rows[i].shard_cap != MOQR_CLI_CAP_VALID) {
            return MOQR_ERR_INVAL;
        }
    }
    o->collected = serial;
    return MOQR_OK;
}

static moqr_result_t
admin_render(void *ctx, uint64_t serial, char *const bodies[MOQR_ADMIN_BODY__COUNT],
             const size_t caps[MOQR_ADMIN_BODY__COUNT],
             size_t out_len[MOQR_ADMIN_BODY__COUNT])
{
    admin_owner_t *o = (admin_owner_t *)ctx;

    if (o->collected != serial || serial == 0u) {
        return MOQR_ERR_WRONG_STATE;   /* render only what collect froze */
    }
    for (uint32_t i = 0; i < o->lanes; i++) {
        o->views[i].core = &o->rows[i].core;
        o->views[i].bind = &o->rows[i].bind;
        o->views[i].shard = o->rows[i].shard_cap == MOQR_CLI_CAP_VALID
                                ? &o->rows[i].shard
                                : NULL;
        o->views[i].labels = o->labels[i];
        o->views[i].lane_wakes = o->rows[i].lane_wakes;
    }
    for (uint32_t k = 0; k < MOQR_OBS_FMT__COUNT; k++) {
        size_t written = 0;
        moqr_result_t rc = moqr_metrics_write_multi_ex(
            o->views, o->lanes, (moqr_obs_format_t)k, bodies[k], caps[k],
            &written);
        if (rc != MOQR_OK) {
            return rc;   /* a suppressed exposition exposes no partial body */
        }
        out_len[k] = written;
    }
    /* The shards document, from the SAME frozen views: one generation, one
     * row set, every slot. */
    {
        size_t written = 0;
        moqr_result_t rc = moqr_cli_shards_render(
            o->views, o->lanes, serial, bodies[MOQR_ADMIN_BODY_SHARDS],
            caps[MOQR_ADMIN_BODY_SHARDS], &written);
        if (rc != MOQR_OK) {
            return rc;
        }
        out_len[MOQR_ADMIN_BODY_SHARDS] = written;
    }
    return MOQR_OK;
}

/*
 * The signal sink keeps its existing surface: the Prometheus 0.0.4 document,
 * under the same banner, on stderr.
 *
 * The bytes come from ONE formatter. A test that reproduced the format string
 * would move in lockstep with any change to it and could never notice one, so
 * the comparison against the old coordinator drives these callbacks themselves
 * -- there is no second copy of the text to agree with.
 */
static void
admin_emit_signal(void *ctx, uint64_t serial, const char *body, size_t len)
{
    admin_owner_t *o = (admin_owner_t *)ctx;
    size_t n = 0;

    /* The bank body is the ADMIN document, with its extended families. The
     * signal sink keeps the frozen projection, rendered here from the rows the
     * render for this same serial froze. */
    (void)body;
    (void)len;
    if (o->collected != serial || o->signal_body == NULL ||
        moqr_metrics_write_prometheus_multi(o->views, o->lanes, o->signal_body,
                                            o->signal_cap, &n) != MOQR_OK) {
        fprintf(stderr,
                "MOQ5 Relay: metrics epoch %llu suppressed on the signal sink "
                "— its frozen projection could not be rendered from the "
                "generation the endpoint served\n",
                (unsigned long long)serial);
        return;
    }
    fprintf(stderr, "\n===== MOQ5 Relay metrics (epoch %llu) =====\n%s\n",
            (unsigned long long)serial, o->signal_body);
}

/* And its existing suppression diagnostic, byte for byte. A poisoned epoch
 * that simply produced nothing would be indistinguishable from one that is
 * merely late. */
static void
admin_emit_suppressed(void *ctx, uint64_t serial)
{
    (void)ctx;
    fprintf(stderr,
            "MOQ5 Relay: metrics epoch %llu suppressed — a lane's "
            "shard snapshot was refused or an internal-entity "
            "exclusion went negative (snapshot contradicts itself)\n",
            (unsigned long long)serial);
}

/*
 * The owner context's own storage: one copied row and one render view per lane.
 * Held for the endpoint's lifetime rather than allocated per scrape, so a
 * scrape performs no allocation at all -- and counted in the capacity model
 * beside the listener's own footprint.
 */
/*
 * Built ONLY when the endpoint is enabled.
 *
 * With the section absent there is no socket, no thread, no body bank, no
 * owner rows or views, and no admin-only failure path -- which is what makes
 * the disabled configuration's reported ceiling exactly the one it allocates.
 * An unconditional allocation here would both change the default path and make
 * the printed ceiling undercount the process.
 */
/* The /api/v1/info document, frozen from the configuration, the resolved
 * shard plan and budgets (the SAME pure builders serve and capacity use) and
 * the described ceiling. A refusal here is a refusal to serve: an endpoint
 * that advertises the target must be able to serve it. */
static moqr_result_t
admin_owner_render_info(admin_owner_t *o, const moqr_cli_config_t *cfg,
                        size_t serve_ctx_bytes)
{
    moqr_cli_shard_plan_t plan;
    char perr[192] = { 0 };
    moqr_shards_cfg_t scfg;
    moqr_shards_limits_t lim;
    moqr_cli_capacity_t cap;
    moqr_cli_info_inputs_t in;
    uint64_t bound;
    moqr_result_t rc;

    if (moqr_cli_shard_plan(cfg, &plan, perr, sizeof(perr)) != MOQR_OK) {
        return MOQR_ERR_INVAL;
    }
    moqr_cli_build_shards_cfg(cfg, moq_alloc_default(), &scfg);
    if (moqr_shards_cfg_resolve(&scfg, &lim) != MOQR_OK) {
        return MOQR_ERR_INVAL;
    }
    if (moqr_cli_describe_capacity(cfg, moq_alloc_default(), serve_ctx_bytes,
                                   &cap) != MOQR_OK) {
        return MOQR_ERR_CAPACITY;
    }
    bound = moqr_cli_info_bound();
    /* The owner's buffer is governed by the same finite contract the machine
     * enforces: a bound that would not fit is a refusal to serve. */
    if (bound == UINT64_MAX || bound >= (uint64_t)SIZE_MAX ||
        bound + 1u > (uint64_t)MOQR_ADMIN_MAX_STATIC_DOC) {
        return MOQR_ERR_CAPACITY;
    }
    o->info_cap = (size_t)bound + 1u;
    o->info = malloc(o->info_cap);
    if (o->info == NULL) {
        o->info_cap = 0;
        return MOQR_ERR_NOMEM;
    }
    in.cfg = cfg;
    in.plan = &plan;
    in.limits = &lim;
    in.capacity = &cap;
#ifdef MOQR_DUAL_LISTENER
    in.dual_listener_build = true;
#else
    in.dual_listener_build = false;
#endif
#ifdef MOQR_BIND_TESTING
    in.verify_build = true;
#else
    in.verify_build = false;
#endif
    rc = moqr_cli_info_render(&in, o->info, o->info_cap, &o->info_len);
    if (rc != MOQR_OK) {
        free(o->info);
        o->info = NULL;
        o->info_len = 0;
        o->info_cap = 0;
        return rc;
    }
    return MOQR_OK;
}

static moqr_result_t
admin_owner_init(admin_owner_t *o, const moqr_cli_config_t *cfg,
                 moqr_cli_snapshot_t *snap,
                 const moqr_obs_labels_t *labels, uint32_t lanes,
                 size_t serve_ctx_bytes,
                 moqr_result_t (*wake)(void *), void *wake_ctx)
{
    memset(o, 0, sizeof(*o));
    if (cfg == NULL || !cfg->admin.enabled) {
        return MOQR_OK;   /* nothing owned, nothing to release */
    }
    if (lanes == 0u) {
        return MOQR_ERR_INVAL;
    }
    {
        moqr_admin_listen_footprint_t fp;
        if (moqr_admin_listen_footprint(lanes, &fp) != MOQR_OK) {
            return MOQR_ERR_CAPACITY;
        }
        o->signal_cap = (size_t)fp.body_cap[MOQR_OBS_FMT_PROMETHEUS_004];
    }
    o->rows = calloc(lanes, sizeof(*o->rows));
    o->views = calloc(lanes, sizeof(*o->views));
    o->signal_body = malloc(o->signal_cap);
    if (o->rows == NULL || o->views == NULL || o->signal_body == NULL) {
        free(o->rows);
        free(o->views);
        free(o->signal_body);
        o->rows = NULL;
        o->views = NULL;
        o->signal_body = NULL;
        o->signal_cap = 0;
        return MOQR_ERR_NOMEM;
    }
    {
        moqr_result_t rc = admin_owner_render_info(o, cfg, serve_ctx_bytes);
        if (rc != MOQR_OK) {
            fprintf(stderr,
                    "MOQ5 Relay: the /api/v1/info document could not be "
                    "rendered from this configuration (%d) — refusing to "
                    "serve\n", (int)rc);
            free(o->rows);
            free(o->views);
            free(o->signal_body);
            o->rows = NULL;
            o->views = NULL;
            o->signal_body = NULL;
            o->signal_cap = 0;
            return rc;
        }
    }
    o->snap = snap;
    o->labels = labels;
    o->lanes = lanes;
    o->wake = wake;
    o->wake_ctx = wake_ctx;
    return MOQR_OK;
}

static void
admin_owner_destroy(admin_owner_t *o)
{
    free(o->rows);
    free(o->views);
    free(o->signal_body);
    free(o->info);
    o->rows = NULL;
    o->views = NULL;
    o->signal_body = NULL;
    o->signal_cap = 0;
    o->info = NULL;
    o->info_len = 0;
    o->info_cap = 0;
}

/*
 * The endpoint's callback wiring, in ONE place.
 *
 * Production and the wiring check see the same assignments, so a deleted or
 * swapped seam is a real difference rather than something a second copy in a
 * test would agree with.
 */
static void
admin_fill_listen_cfg(moqr_admin_listen_cfg_t *lcfg,
                      const moqr_cli_config_t *cfg, admin_owner_t *o)
{
    memset(lcfg, 0, sizeof(*lcfg));
    lcfg->admin = &cfg->admin;
    lcfg->lanes = o->lanes;
    lcfg->broker = &g_metrics_broker;   /* BY REFERENCE: one identity space */
    lcfg->wake_all = admin_wake_all;
    lcfg->collect = admin_collect;
    lcfg->render = admin_render;
    lcfg->signal_pending = admin_signal_pending;
    lcfg->emit_signal = admin_emit_signal;
    lcfg->emit_suppressed = admin_emit_suppressed;
    lcfg->ctx = o;
    /* The frozen document, by exact pointer and length. */
    lcfg->info = o->info;
    lcfg->info_len = o->info_len;
}

/*
 * Start the endpoint, or refuse to serve.
 *
 * ALL-OR-NOTHING, and before the readiness line: a relay that announces itself
 * ready while its admin endpoint failed to bind is telling an operator
 * something untrue. `*out` stays NULL when no endpoint is configured, which is
 * not a failure.
 */
static moqr_result_t
admin_endpoint_start(const moqr_cli_config_t *cfg, admin_owner_t *o,
                     moqr_admin_listen_t **out)
{
    moqr_admin_listen_cfg_t lcfg;

    if (out == NULL) {
        return MOQR_ERR_INVAL;
    }
    *out = NULL;
    if (!cfg->admin.enabled) {
        return MOQR_OK;
    }
    if (MOQR_ADMIN_ENDPOINT_AVAILABLE == 0) {
        /*
         * The verify composition's SIGUSR1 contract is the RELAY_BLOCKED_V0
         * projection, not the Prometheus exposition. Enabling the endpoint
         * would silently replace one signal document with another, so that
         * build refuses the section outright rather than changing what SIGUSR1
         * means. Serving /metrics from the verify binary is a separate
         * decision, with its own contract.
         */
        fprintf(stderr,
                "MOQ5 Relay: the admin endpoint is not available in this "
                "build — its SIGUSR1 projection is RELAY_BLOCKED_V0, not "
                "Prometheus\n");
        return MOQR_ERR_UNSUPPORTED;
    }
    admin_fill_listen_cfg(&lcfg, cfg, o);
    return moqr_admin_listen_start(&lcfg, out);
}

/*
 * Shut the endpoint down before anything it reads goes away.
 *
 * Terminality first, so no further lane wake is issued and the in-flight
 * generation is cancelled; then the join, so the owner thread has finished
 * every transition and settled every bank before the facade is stopped, the
 * snapshot destroyed or the broker torn down.
 */
/*
 * Stop the endpoint, and refuse to continue if its owner cannot be proved dead.
 *
 * A live owner reaches far past the listener object: the snapshot it copies
 * rows from, the broker it takes tokens from, the lane facades its wake
 * callback drives, and the owner context holding both. Running the ordinary
 * teardown while that thread may still be executing would be a use-after-free
 * in code that has nothing to do with the admin endpoint, so this path does not
 * unwind at all -- it stops the process while everything is still valid.
 *
 * There is no recovery to attempt. Pretending otherwise would mean choosing
 * which of those objects to gamble on.
 */
/* -- the emergency halt -----------------------------------------------------
 *
 * Termination must not depend on reporting. The sink this report goes to can be
 * a pipe nobody is draining, and the thread this halt is ABOUT may be blocked
 * inside stdio holding the stream's lock -- a plain fprintf/fflush here would
 * then wait on the very thread that could not be joined, and the last resort
 * would never happen.
 *
 * So the report is best effort and bounded, and the halting thread shares NO
 * lock with the reporter. A detached reporter thread writes the message to the
 * descriptor itself (never through the FILE, whose lock may be held), attempts
 * the ordinary stdout flush, and then reports completion by writing one byte
 * to a pipe. The halting thread polls that pipe under ONE monotonic deadline
 * of ADMIN_HALT_REPORT_MS -- no mutex to reacquire, no join, no wall clock --
 * and stops the process whatever the poll's outcome. A reporter that stalls at
 * any point, including inside its completion notification, delays nothing
 * beyond the deadline. No flag on the caller-owned standard streams is
 * changed. If the pipe, the clock or the thread cannot be had, the halt
 * proceeds without a report.
 */
#define ADMIN_HALT_STATUS     70
#define ADMIN_HALT_REPORT_MS  250

typedef struct halt_report {
    const char *msg;
    size_t      len;
    int         done_fd;   /* the reporter's completion notification */
} halt_report_t;

/* The reporter's completion notification: one byte, never retried. */
static void
halt_report_done(int done_fd)
{
    (void)write(done_fd, "D", 1);
}

static void *
halt_report_main(void *arg)
{
    halt_report_t *r = (halt_report_t *)arg;
    const char *p = r->msg;
    size_t left = r->len;

    while (left > 0) {
        ssize_t n = write(STDERR_FILENO, p, left);
        if (n < 0 && errno == EINTR) {
            continue;
        }
        if (n <= 0) {
            break;   /* the sink refused; the report is best effort */
        }
        p += n;
        left -= (size_t)n;
    }
    (void)fflush(stdout);
    halt_report_done(r->done_fd);
    return NULL;
}

/* Milliseconds left until `deadline` on the monotonic clock; 0 once passed or
 * if the clock refuses, so the caller never waits on a clock it cannot read. */
static int
halt_ms_until(const struct timespec *deadline)
{
    struct timespec now;
    long long ms;

    if (clock_gettime(CLOCK_MONOTONIC, &now) != 0) {
        return 0;
    }
    ms = (long long)(deadline->tv_sec - now.tv_sec) * 1000LL +
         (deadline->tv_nsec - now.tv_nsec) / 1000000L;
    if (ms <= 0) {
        return 0;
    }
    return ms > ADMIN_HALT_REPORT_MS ? ADMIN_HALT_REPORT_MS : (int)ms;
}

_Noreturn static void
admin_halt(const char *msg)
{
    halt_report_t r;
    pthread_t th;
    int done[2];
    struct timespec deadline;

    r.msg = msg;
    r.len = strlen(msg);
    if (clock_gettime(CLOCK_MONOTONIC, &deadline) == 0 && pipe(done) == 0) {
        deadline.tv_sec += ADMIN_HALT_REPORT_MS / 1000;
        deadline.tv_nsec += (long)(ADMIN_HALT_REPORT_MS % 1000) * 1000000L;
        if (deadline.tv_nsec >= 1000000000L) {
            deadline.tv_sec++;
            deadline.tv_nsec -= 1000000000L;
        }
        r.done_fd = done[1];
        if (pthread_create(&th, NULL, halt_report_main, &r) == 0) {
            /* Never joined: the process ends whether or not it finished. */
            (void)pthread_detach(th);
            for (;;) {
                struct pollfd pf = { .fd = done[0], .events = POLLIN,
                                     .revents = 0 };
                int left = halt_ms_until(&deadline);
                if (left <= 0) {
                    break;
                }
                if (poll(&pf, 1, left) < 0 && errno == EINTR) {
                    continue;   /* the same deadline, re-measured */
                }
                break;   /* notified, refused or expired: all end the grace */
            }
        }
    }
    _exit(ADMIN_HALT_STATUS);
}

static moqr_result_t
admin_endpoint_stop(moqr_admin_listen_t *l)
{
    moqr_result_t rc;

    if (l == NULL) {
        return MOQR_OK;
    }
    /* Terminality first, so no further lane wake is issued and the in-flight
     * generation is cancelled; then the join, so the owner has finished every
     * transition and settled every bank. The OBJECT stays alive: lanes are
     * still being joined and may still notify. */
    moqr_admin_listen_note_terminal(l);
    rc = moqr_admin_listen_stop(l);
    if (rc == MOQR_ERR_WOULD_BLOCK) {
        admin_halt("MOQ5 Relay: the admin endpoint owner could not be proved "
                   "stopped — halting without teardown, because that thread "
                   "can still reach the snapshot, the broker and the lane "
                   "facades\n");
    }
    return rc;
}

/*
 * Activate the endpoint, or refuse to serve -- and never tear down over a live
 * owner while doing it.
 *
 * A failed activation still started a thread, and stopping that thread can fail
 * to prove it dead exactly like any other stop. Treating "activation failed"
 * as a single answer would send the caller into its ordinary cleanup, freeing
 * the owner context, the snapshot and the broker that a live owner still
 * holds. So the unprovable outcome takes the SAME halt the ordinary stop path
 * takes, before any dependency is released.
 */
static moqr_result_t
admin_endpoint_activate(moqr_admin_listen_t *l)
{
    moqr_result_t rc;

    if (l == NULL) {
        return MOQR_OK;   /* no endpoint configured */
    }
    rc = moqr_admin_listen_activate(l);
    if (rc == MOQR_ERR_WOULD_BLOCK || moqr_admin_listen_owner_unjoinable(l)) {
        admin_halt("MOQ5 Relay: the admin endpoint owner could not be proved "
                   "stopped after a failed activation — halting without "
                   "teardown, because that thread can still reach the "
                   "snapshot, the broker and the lane facades\n");
    }
    return rc;
}

/* After every facade lane has joined, nothing can notify any more. */
static void
admin_endpoint_destroy(moqr_admin_listen_t *l, admin_owner_t *o)
{
    moqr_admin_listen_destroy(l);
    admin_owner_destroy(o);
}

typedef struct serve_ctx {
    moqr_bind_t      *bind;
    moqr_core_t      *core;
    moqr_trace_t     *trace;
    moqr_obs_labels_t labels;
    /* The single-lane composition publishes into a one-row snapshot so that
     * every lane count shares one collect/render path. */
    moqr_cli_snapshot_t *snap;
    unsigned             seen_route_epoch;
    unsigned             seen_trace_epoch;
    uint64_t             seen_metrics_serial;
    /* The admin endpoint to notify when this lane publishes. NULL until the
     * endpoint is started, and only cleared after this lane has joined. */
    _Atomic(moqr_admin_listen_t *) admin_listen;
} serve_ctx_t;

/* Render `fn` into a heap buffer, growing once on truncation (the serializers
 * report the size they need), then write it to stderr under a banner. Network
 * thread only. The buffer is transient — allocated and freed per dump — so it
 * is not part of the process capacity ceiling. */
typedef moqr_result_t (*dump_fn)(void *ctx, char *buf, size_t cap,
                                 size_t *written);

static void
dump_emit(const char *label, dump_fn fn, void *ctx)
{
    size_t cap = 16u * 1024u;
    char  *buf = malloc(cap);
    if (buf == NULL) {
        return;
    }
    size_t w = 0;
    moqr_result_t rc = fn(ctx, buf, cap, &w);
    if (rc == MOQR_ERR_CAPACITY) {
        char *bigger = realloc(buf, w + 1);
        if (bigger != NULL) {
            buf = bigger;
            rc = fn(ctx, buf, w + 1, &w);
        }
    }
    if (rc == MOQR_OK) {
        fprintf(stderr, "\n===== MOQ5 Relay %s =====\n%s\n", label, buf);
    }
    free(buf);
}

static moqr_result_t
dump_routes_fn(void *ctx, char *b, size_t c, size_t *w)
{
    return moqr_core_route_dump_text((moqr_core_t *)ctx, b, c, w);
}

static moqr_result_t
dump_trace_fn(void *ctx, char *b, size_t c, size_t *w)
{
    return moqr_trace_write_jsonl((moqr_trace_t *)ctx, b, c, w);
}

/* The direct single-lane metrics render is retired: it was safe only because
 * it ran on the owning lane, and every composition now publishes a snapshot
 * row and renders through the one coordinator path instead. */

/* Owning-lane journal dump (multi-lane only; K=1 has no journal). */
typedef struct dump_journal_ctx {
    moqr_shards_t *shards;
    uint16_t       shard;
} dump_journal_ctx_t;

static moqr_result_t
dump_journal_fn(void *vctx, char *b, size_t c, size_t *w)
{
    dump_journal_ctx_t *j = vctx;
    return moqr_shards_journal_dump_text(j->shards, j->shard, b, c, w);
}

/* Per-connection relay state is a TAG in the adapter's conn_user slot (never a
 * map keyed by session/conn pointer — those values can be reused by a successor
 * connection). Nothing is allocated, so there is nothing to free at reap.
 *   NULL   -> never seen: attach to the binding on first sight.
 *   OPENED -> attached; the binding owns per-connection state from here.
 *   DEAD   -> the binding detached it (SESSION_CLOSED) or refused it: never
 *             re-attach, even while the terminal connection stays visible for
 *             its final pump batch. */
#define RELAY_CONN_OPENED MOQR_CONN_OPENED
#define RELAY_CONN_DEAD   MOQR_CONN_DEAD

/* Lane thread, per pump: attach new connections, run the binding, then
 * service any pending operator dump signals. Single lane (lane 0) drives the
 * one relay binding; every accepted session lives on this lane. */
static int
relay_lane_pump(moq_msquic_managed_t *m, moq_msquic_managed_lane_t *lane,
                uint64_t now_us, void *vctx)
{
    serve_ctx_t *ctx = vctx;
    (void)m;
    if (moq_msquic_lane_index(lane) != 0) {
        return 0;   /* single-lane relay: only lane 0 carries connections */
    }
    for (moq_msquic_managed_conn_t *conn = moq_msquic_lane_next_conn(lane,
                                                                     NULL);
         conn != NULL; conn = moq_msquic_lane_next_conn(lane, conn)) {
        void *tag = moq_msquic_managed_conn_user(conn);
        if (tag == RELAY_CONN_DEAD) {
            continue;
        }
        moq_session_t *s = moq_msquic_managed_conn_session(conn);
        if (s == NULL) {
            continue;
        }
        if (tag == NULL) {
            if (moqr_bind_conn_open(ctx->bind, s,
                                    moq_msquic_managed_conn_negotiated_version(conn)) == MOQR_OK) {
                moq_msquic_managed_conn_set_user(conn, RELAY_CONN_OPENED);
            } else {
                /* Bind table full: refuse the connection outright. */
                moq_msquic_managed_conn_set_user(conn, RELAY_CONN_DEAD);
                moq_msquic_managed_conn_close(conn, 0);
            }
        }
    }
    (void)moqr_bind_pump(ctx->bind, now_us);
    /* A binding this pump's own work detached must be retired now: nothing
     * else wakes this lane on its behalf. */
    if (!moqr_relay_reap_pass(ctx->bind, lane, NULL)) {
        fprintf(stderr, "MOQ5 Relay: connection retirement failed — stopping\n");
        return 1;
    }

    unsigned re = atomic_load(&g_route_dump_epoch);
    if (ctx->seen_route_epoch != re) {
        ctx->seen_route_epoch = re;
        dump_emit("routes", dump_routes_fn, ctx->core);
    }

    /* Publish this lane's metrics row for the open generation. There is no
     * shard runtime in this composition, so the capability is ABSENT: the
     * shard-plane families are omitted downstream rather than rendered from a
     * zeroed struct. Stats are gathered only when the serial advances. */
    uint64_t serial = moqr_broker_epoch_fn(&g_metrics_broker);
    if (ctx->snap != NULL && serial != 0u &&
        ctx->seen_metrics_serial != serial) {
        ctx->seen_metrics_serial = serial;
        moqr_cli_snapshot_stats_t st;
        memset(&st, 0, sizeof(st));
        moqr_core_get_stats(ctx->core, &st.core);
        moqr_bind_get_stats(ctx->bind, &st.bind);
        st.shard_cap = MOQR_CLI_CAP_ABSENT;
        st.lane_wakes = 0;
        moqr_cli_snapshot_publish(ctx->snap, 0, &st, serial);
        /* Published -- tell the owner, at the publish point. */
        admin_notify_published(&ctx->admin_listen);
    }

    unsigned te = atomic_load(&g_dump_trace_epoch);
    if (ctx->seen_trace_epoch != te) {
        ctx->seen_trace_epoch = te;
        dump_emit("trace", dump_trace_fn, ctx->trace);
    }
    return atomic_load(&g_stop) ? 1 : 0;
}

/* Prints the relay-state allocation-request ceiling for THIS config, in
 * the composition production actually allocates (direct core+bind+trace at
 * lanes=1; the full shard runtime + CLI context/rows at lanes>1), on the
 * stream the caller names: stdout for the capacity subcommand, the serve
 * log's prose stream for a serve (stdout in text mode, stderr in JSON mode,
 * where stdout carries only events). Returns nonzero when the config cannot
 * be described (create would refuse too). */
static int
print_capacity(const moqr_cli_config_t *cfg, size_t serve_ctx_bytes, FILE *out)
{
    moqr_cli_capacity_t cap;
    if (out == NULL) {
        return 1;
    }
    if (moqr_cli_describe_capacity(cfg, moq_alloc_default(), serve_ctx_bytes,
                                   &cap) != MOQR_OK) {
        fprintf(stderr,
                "capacity: refused — the ceiling overflows a 64-bit byte "
                "count (or the budgets cannot fit the lanes); refusing "
                "rather than printing an understated number\n");
        return 1;
    }
    fprintf(out, "relay-state allocation-request ceiling: %" PRIu64 " bytes\n",
           cap.total_bytes);
    fprintf(out, "  (allocator requests only — excludes malloc metadata, "
           "size-class rounding,\n   fragmentation, thread stacks, "
           "transient diagnostics, and MsQuic/session\n   memory that "
           "remains adapter-owned)\n");
    fprintf(out, "  per shard: structure %" PRIu64 " + payloads %" PRIu64
           " + binding %" PRIu64 " + trace %" PRIu64 " bytes\n",
           cap.core_structure_bytes, cap.core_payload_bytes,
           cap.bind_structure_bytes, cap.trace_bytes);
    if (cap.cross_shard_bytes != 0) {
        fprintf(out, "  cross-shard: %" PRIu64
               " bytes (runtime, channels, canon keys, staging)\n",
               cap.cross_shard_bytes);
    }
    if (cap.cli_runtime_bytes != 0) {
        fprintf(out, "  cli runtime: %" PRIu64 " bytes (snapshot rows, plus the "
               "serve context at lanes > 1; included above)\n",
               cap.cli_runtime_bytes);
    }
    if (cap.admin_bytes != 0) {
        fprintf(out, "  admin endpoint: %" PRIu64 " bytes (listener object, both "
               "body banks, and the coordinator's per-lane rows and views, "
               "its signal-sink body and its /api/v1/info document; included "
               "above)\n",
               cap.admin_bytes);
    }
    if (cap.admin_thread_stack_bytes != 0) {
        /* A reservation, not an allocator request: reported beside the ceiling
         * rather than inside it, on the same footing as the other thread
         * stacks the ceiling excludes. */
        fprintf(out, "  admin thread stack reservation: %" PRIu64 " bytes (not part "
               "of the ceiling above)\n",
               cap.admin_thread_stack_bytes);
    }
    fprintf(out, "  usable client bindings per shard: %u\n",
           cap.usable_bindings_per_shard);
    return 0;
}


/* The ordinary return of a serve function after its sink exists: the sink's
 * finalization (the JSON stream's checked final flush; text closes without
 * I/O) is the last act before the status goes back to main. Every return
 * after moqr_cli_log_init goes through here -- refusals before readiness as
 * much as the post-join return -- and nothing is emitted afterwards. The
 * nonreturning emergency halt never comes here. */
static int
serve_done(moqr_cli_log_t *log, int rc)
{
    moqr_cli_log_finish(log);
#ifdef MOQR_PUMP_TESTING
    g_test_serve_log_last_state = log->sink_state;
#endif
    return rc;
}

static int
cmd_serve(const moqr_cli_config_t *cfg)
{
    if (cfg->cert[0] == '\0' || cfg->key[0] == '\0') {
        fprintf(stderr, "serve requires listener.cert and listener.key\n");
        return 2;
    }
    /* The serve log, before any resource exists: a refused sink (the fixed
     * line cannot hold every event of the schema) serves nothing. */
    moqr_cli_log_t log;
    if (moqr_cli_log_init(&log, cfg->logging.format, stdout, stderr,
                          SERVE_LOG_IO) != MOQR_OK) {
        fprintf(stderr, "logging: the event sink could not be initialised\n");
        return 2;
    }

    /* Assemble the core with its flight-recorder trace attached (depth from
     * telemetry.trace_ring_records). Owns trace; destroy core THEN trace. */
    moqr_trace_t *trace = NULL;
    moqr_core_t *core = NULL;
    if (moqr_cli_build_core(cfg, moq_alloc_default(), &trace, &core) !=
        MOQR_OK) {
        fprintf(stderr, "relay core create failed\n");
        return serve_done(&log, 1);
    }
    moqr_bind_cfg_t bcfg;
    moqr_bind_cfg_init_sized(&bcfg, sizeof(bcfg), moq_alloc_default());
    bcfg.core = core;
    moqr_bind_t *bind = NULL;
    if (moqr_bind_create(&bcfg, &bind) != MOQR_OK) {
        fprintf(stderr, "relay binding create failed\n");
        moqr_core_destroy(core);
        moqr_trace_destroy(trace);
        return serve_done(&log, 1);
    }
    /* One generation in flight, plus one spare, matching the multi-lane
     * composition: the broker's bank accounting is the same either way. */
    if (moqr_broker_init(&g_metrics_broker, MOQR_BROKER_BANKS) != MOQR_OK) {
        fprintf(stderr, "MOQ5 Relay: metrics broker init failed\n");
        moqr_bind_destroy(bind);
        moqr_core_destroy(core);
        moqr_trace_destroy(trace);
        return serve_done(&log, 1);
    }
    moqr_cli_snapshot_t snap;
    memset(&snap, 0, sizeof(snap));
    if (moqr_cli_snapshot_init(&snap, 1u, moq_alloc_default()) != MOQR_OK) {
        fprintf(stderr, "MOQ5 Relay: metrics snapshot init failed\n");
        moqr_broker_destroy(&g_metrics_broker);
        moqr_bind_destroy(bind);
        moqr_core_destroy(core);
        moqr_trace_destroy(trace);
        return serve_done(&log, 1);
    }
    serve_ctx_t ctx = {
        .bind = bind,
        .core = core,
        .trace = trace,
        .labels = { .shard = 0, .transport = "msquic",
                    .version = cfg->alpn_set },
        .snap = &snap,
    };

    moq_msquic_managed_cfg_t tcfg;
    moq_msquic_managed_cfg_init_sized(&tcfg, sizeof(tcfg));
    tcfg.alloc = moq_alloc_default();
    tcfg.perspective = MOQ_PERSPECTIVE_SERVER;
    tcfg.host = cfg->host;
    tcfg.port = (uint16_t)cfg->port;
    tcfg.cert_path = cfg->cert;
    tcfg.key_path = cfg->key;
    tcfg.insecure_skip_verify = cfg->insecure_skip_verify;
    tcfg.send_request_capacity = true;
    tcfg.initial_request_capacity = 1024;
    tcfg.streaming_objects = true;   /* stream-through forwarding: chunk-by-chunk
                                      * with live-edge delivery */
    moqr_cli_apply_versions(cfg, &tcfg);
    /* One lane == one lock domain == the one relay binding. Size the transport
     * admission cap to the binding's own resolved connection table: a defaulted
     * bind resolves max_conns from the core's max_bindings, so read the same
     * limit here rather than hard-coding a default a configured budget can move. */
    moqr_core_limits_t lim;
    moqr_core_get_limits(core, &lim);
    tcfg.lane_count = 1;
    tcfg.max_connections = lim.max_bindings;
    tcfg.on_lane_pump = relay_lane_pump;
    tcfg.on_lane_pump_user = &ctx;

    moq_msquic_managed_t *t = NULL;
    if (moq_msquic_managed_create(&tcfg, &t) != MOQ_OK) {
        fprintf(stderr, "transport create failed (port %d)\n", cfg->port);
        moqr_cli_snapshot_destroy(&snap);
        moqr_broker_destroy(&g_metrics_broker);
        moqr_bind_destroy(bind);
        moqr_core_destroy(core);
        moqr_trace_destroy(trace);
        return serve_done(&log, 1);
    }

    if (print_capacity(cfg, 0, moqr_cli_log_prose_stream(&log)) != 0) {
        /* An undescribable ceiling never serves. The facade is destroyed
         * first, so its lane is joined before the broker it reads goes away. */
        moq_msquic_managed_destroy(t);
        moqr_cli_snapshot_destroy(&snap);
        moqr_broker_destroy(&g_metrics_broker);
        moqr_bind_destroy(bind);
        moqr_core_destroy(core);
        moqr_trace_destroy(trace);
        return serve_done(&log, 2);
    }
    serve_install_signals(on_dump_signal);
    /* The endpoint comes up BEFORE the readiness line, and its failure refuses
     * the whole serve. Its owner thread becomes the sole owner of the broker
     * from here on. */
    admin_owner_t admin_owner;
    moqr_admin_listen_t *admin_l = NULL;
    int admin_rc = 0;
    if (admin_owner_init(&admin_owner, cfg, &snap, &ctx.labels, 1u,
                         0u,   /* no multi-lane context at one lane */
                         serve_wake_lane0, t) != MOQR_OK) {
        fprintf(stderr, "MOQ5 Relay: admin owner context allocation failed\n");
        moq_msquic_managed_destroy(t);
        moqr_cli_snapshot_destroy(&snap);
        moqr_broker_destroy(&g_metrics_broker);
        moqr_bind_destroy(bind);
        moqr_core_destroy(core);
        moqr_trace_destroy(trace);
        return serve_done(&log, 1);
    }
    if (admin_endpoint_start(cfg, &admin_owner, &admin_l) != MOQR_OK) {
        fprintf(stderr,
                "MOQ5 Relay: admin endpoint could not bind %s:%d — refusing "
                "to serve\n", cfg->admin.host, cfg->admin.port);
        admin_owner_destroy(&admin_owner);
        moq_msquic_managed_destroy(t);
        moqr_cli_snapshot_destroy(&snap);
        moqr_broker_destroy(&g_metrics_broker);
        moqr_bind_destroy(bind);
        moqr_core_destroy(core);
        moqr_trace_destroy(trace);
        return serve_done(&log, 1);
    }
    /*
     * Install the lane-visible pointer while the endpoint is still INERT --
     * bound but not listening, its owner parked -- then activate it. Starting
     * the owner and installing the pointer afterwards would leave a window in
     * which a scraper opens a generation and the lane that publishes for it
     * has nowhere to send its notification.
     */
    atomic_store_explicit(&ctx.admin_listen, admin_l, memory_order_release);
    if (admin_endpoint_activate(admin_l) != MOQR_OK) {
        fprintf(stderr,
                "MOQ5 Relay: the admin endpoint did not start serving — "
                "refusing to serve\n");
        /* Keep the listener object alive until every lane that could already
         * have loaded this pointer has joined. Clearing an atomic pointer is
         * not a grace period for a reader holding the old value. */
        (void)moq_msquic_managed_stop(t);
        moq_msquic_managed_destroy(t);
        atomic_store_explicit(&ctx.admin_listen, NULL, memory_order_release);
        admin_endpoint_destroy(admin_l, &admin_owner);
        moqr_cli_snapshot_destroy(&snap);
        moqr_broker_destroy(&g_metrics_broker);
        moqr_bind_destroy(bind);
        moqr_core_destroy(core);
        moqr_trace_destroy(trace);
        return serve_done(&log, 1);
    }
    {
        /* lanes = 1 composes no shard runtime; report the same resolved
         * operating point the multi-lane path would have used */
        moqr_shards_cfg_t rc_cfg;
        moqr_cli_build_shards_cfg(cfg, moq_alloc_default(), &rc_cfg);
        moqr_cli_serve_log_readiness(&log, cfg, MOQR_CLI_LOG_COMP_K1,
                            admin_l != NULL ? moqr_admin_listen_port(admin_l)
                                            : -1,
                            0u, &rc_cfg);
    }
    /* A server facade outlives any single accepted connection: only our own
     * stop (SIGINT/SIGTERM -> g_stop -> the lane pump returns nonzero) is a
     * lifetime terminal. Do NOT gate on moq_msquic_managed_is_fatal(): on a
     * server that is a per-connection convenience latch (the first accepted
     * connection can set it), so gating here would let one bad client take the
     * whole relay down. wait() returns MOQ_ERR_CLOSED only on a true facade
     * terminal (stop requested or a lane-pump exit); a timeout or per-conn
     * terminal keeps the listener serving. */
    {
        uint64_t rendered_epoch = 0;
        uint64_t incomplete_told = 0;
        uint64_t fail_told = 0;
        while (!atomic_load(&g_stop)) {
            if (moq_msquic_managed_wait(t, 500 * 1000) == MOQ_ERR_CLOSED) {
                break;
            }
            if (admin_l != NULL) {
                /* An endpoint whose owner has died is a dead admin surface --
                 * /metrics, /api/v1/info and /api/v1/shards alike. Continuing
                 * to serve while advertising it is the falsehood this check
                 * exists to prevent. */
                if (moqr_admin_listen_owner_exited(admin_l)) {
                    fprintf(stderr,
                            "MOQ5 Relay: the admin endpoint owner exited "
                            "(health %u) — stopping\n",
                            (unsigned)moqr_admin_listen_health(admin_l));
                    admin_rc = 1;
                    break;
                }
                /* The admin thread owns every broker transaction, including
                 * the one the latched signal demand opens, and every wake
                 * those transitions imply. Doing any of it here as well would
                 * put two owners on one broker. */
                continue;
            }
            /* The serial is assigned here, never in the handler. */
            if (atomic_exchange(&g_signal_metrics_pending, 0)) {
                uint64_t issued = 0;
                bool     w = false;
                (void)moqr_broker_request(&g_metrics_broker,
                                          MOQR_BROKER_DEMAND_SIGNAL, &issued,
                                          &w);
                (void)w;
            }
            if (moqr_broker_busy(&g_metrics_broker)) {
                moq_msquic_lane_wake(moq_msquic_managed_lane(t, 0));
                bool wake_after_release = false;
                rendered_epoch =
                    coord_try_dump_labels(&ctx.labels, &snap, rendered_epoch,
                                          &incomplete_told, &fail_told,
                                          &wake_after_release);
                /* Retiring the generation may have opened deferred work; that
                 * flag is its only notice, so the wake happens here rather
                 * than waiting for the next poll. */
                if (wake_after_release) {
                    moq_msquic_lane_wake(moq_msquic_managed_lane(t, 0));
                }
            }
        }
    }
    /* The endpoint is cancelled and joined BEFORE the facade is stopped, the
     * snapshot destroyed or the broker torn down -- everything its owner
     * thread reads outlives it. The endpoint OBJECT is kept until the facade
     * lane has joined, because a lane can still be publishing and its
     * notification must stay safe to issue. */
    if (admin_endpoint_stop(admin_l) != MOQR_OK && admin_rc == 0) {
        fprintf(stderr, "MOQ5 Relay: the admin endpoint owner did not stop "
                        "cleanly\n");
        admin_rc = 1;
    }
    /* stop/wait/destroy run only here, outside every adapter callback. */
    (void)moq_msquic_managed_stop(t);

    /* The single-lane attribution row: adapter doorbell counters for lane 0;
     * the shard-plane fields are TRUE zeros — the lanes=1 composition has no
     * cross-shard machinery at all (structurally inert, not refused). */
    {
        moq_msquic_lane_stats_t ad;
        memset(&ad, 0, sizeof(ad));
        ad.struct_size = (uint32_t)sizeof(ad);
        bool ad_ok = moq_msquic_lane_get_stats(moq_msquic_managed_lane(t, 0),
                                               &ad, sizeof(ad)) == MOQ_OK;
        moqr_cli_serve_log_lane(&log, 0, ad_ok, &ad, true, NULL);
    }

    moqr_bind_stats_t bs;
    moqr_bind_get_stats(bind, &bs);
    moqr_core_stats_t cs;
    moqr_core_get_stats(core, &cs);
    moqr_cli_serve_log_stop_k1(&log, &bs, &cs);

    moq_msquic_managed_destroy(t);
    /* The facade is stopped and its lane joined, so nothing can publish into
     * the snapshot or notify the endpoint any more; only now is it safe to
     * tear either of them down. */
    atomic_store_explicit(&ctx.admin_listen, NULL, memory_order_release);
    admin_endpoint_destroy(admin_l, &admin_owner);
    moqr_cli_snapshot_destroy(&snap);
    moqr_broker_destroy(&g_metrics_broker);
    moqr_bind_destroy(bind);
    moqr_core_destroy(core);
    moqr_trace_destroy(trace);   /* after the core that borrowed it */
    return serve_done(&log, admin_rc);
}

/* -- multi-lane serve (listener.lanes > 1) --------------------------------- */

/* Lane-local state, indexed by shard == lane index. Written/read only on the
 * owning lane's pump, or by the main thread after every lane has joined. */
typedef struct serve_lanes_ctx {
    moqr_shards_t       *shards;
    moqr_cli_snapshot_t *snap;   /* metrics rows: lanes publish, main renders */
    uint32_t             lanes;  /* TOTAL shards: raw lanes + WebTransport   */
    /* Which global shards belong to which facade. Every lane<->shard crossing
     * goes through this, so a cross-shard wake reaches the lane that actually
     * owns the destination shard instead of the same-numbered lane on the
     * wrong facade. */
    moqr_cli_shard_plan_t plan;
    moq_msquic_managed_t *raw;
#ifdef MOQR_DUAL_LISTENER
    moq_wtquic_msquic_managed_t *wt;   /* NULL unless configured */
#endif
    moqr_obs_labels_t    labels[MOQR_CLI_MAX_LANES];
    /* Routes and metrics are driven by different requesters now, so a lane
     * tracks them separately: the routes dump answers SIGUSR1 alone, while a
     * metrics row is published for whichever generation the broker opened. */
    unsigned             seen_route_epoch[MOQR_CLI_MAX_LANES];
    uint64_t             seen_metrics_serial[MOQR_CLI_MAX_LANES];
    unsigned             seen_trace_epoch[MOQR_CLI_MAX_LANES];
    uint64_t             pump_turns[MOQR_CLI_MAX_LANES];
    uint64_t             wake_pushes[MOQR_CLI_MAX_LANES];
    /* The admin endpoint to notify when a lane publishes. NULL until the
     * endpoint is started, and only cleared after every lane has joined. */
    _Atomic(moqr_admin_listen_t *) admin_listen;
} serve_lanes_ctx_t;

/* Lane i drives shard i: attach/reap this lane's connections against shard i's
 * binding, step the shard, then wake exactly the destination lanes whose
 * control mailboxes accepted a cross-shard push. Every session touched here is
 * owned by this lane, under this lane's lock domain. */
/* Wake the lane that owns a GLOBAL shard, on whichever facade that is. The
 * cross-shard mask is expressed in global shards, so this is the only place
 * allowed to turn one into a lane handle.
 *
 * Returns whether the wake actually reached a lane. A shard the plan does not
 * place on any live facade has not been woken, and saying otherwise would let
 * an epoch wait forever on a row that will never be published. */
static bool
serve_wake_shard_checked(serve_lanes_ctx_t *ctx, moq_msquic_managed_t *raw,
                         uint32_t shard)
{
    if (raw != NULL && shard >= ctx->plan.raw_first &&
        shard < ctx->plan.raw_first + ctx->plan.raw_count) {
        return moq_msquic_lane_wake(
                   moq_msquic_managed_lane(raw, shard - ctx->plan.raw_first)) ==
               MOQ_OK;
    }
#ifdef MOQR_DUAL_LISTENER
    if (ctx->wt != NULL && shard >= ctx->plan.wt_first &&
        shard < ctx->plan.wt_first + ctx->plan.wt_count) {
        return moq_wtquic_msquic_lane_wake(
                   moq_wtquic_msquic_managed_lane(
                       ctx->wt, shard - ctx->plan.wt_first)) == MOQ_OK;
    }
#endif
    return false;
}

static void
serve_wake_shard(serve_lanes_ctx_t *ctx, moq_msquic_managed_t *raw,
                 uint32_t shard)
{
    (void)serve_wake_shard_checked(ctx, raw, shard);
}

/*
 * One label set per GLOBAL shard, naming the transport that owns it, so a row
 * is attributable to the listener that produced it.
 *
 * Factored out because the admin endpoint is handed exactly this array and
 * exactly this count: a test that rebuilt them could not notice production
 * passing a raw lane count instead of the total, or laying either transport's
 * range down wrongly.
 */
static void
serve_lanes_fill_labels(moqr_obs_labels_t *labels,
                        const moqr_cli_config_t *cfg,
                        const moqr_cli_shard_plan_t *plan)
{
    for (uint32_t i = 0; i < plan->raw_count; i++) {
        labels[plan->raw_first + i] =
            (moqr_obs_labels_t){ .shard = (uint16_t)(plan->raw_first + i),
                                 .transport = "msquic",
                                 .version = cfg->alpn_set };
    }
    for (uint32_t i = 0; i < plan->wt_count; i++) {
        labels[plan->wt_first + i] =
            (moqr_obs_labels_t){ .shard = (uint16_t)(plan->wt_first + i),
                                 .transport = "wtquic-msquic",
                                 .version = cfg->wt.alpn_set };
    }
}

/* Every lane, for the admin endpoint's wake seam. The endpoint knows nothing
 * about facades or shard plans; it asks once and this turns that into the
 * facade-correct wake for every global shard. */
typedef struct serve_wake_all_ctx {
    serve_lanes_ctx_t    *ctx;
    moq_msquic_managed_t *raw;
    uint32_t              shards;
} serve_wake_all_ctx_t;

static moqr_result_t
serve_wake_every_shard(void *p)
{
    serve_wake_all_ctx_t *w = (serve_wake_all_ctx_t *)p;
    bool all = true;
    for (uint32_t d = 0; d < w->shards; d++) {
        /* Every lane is attempted even after one fails: a partial wake is
         * still better than an aborted one, and the caller is told the truth
         * about it either way. */
        all = serve_wake_shard_checked(w->ctx, w->raw, d) && all;
    }
    return all ? MOQR_OK : MOQR_ERR_INTERNAL;
}

/*
 * Everything a shard step owes the runtime, independent of which transport
 * delivered the bytes: advance the shard, wake the lanes its cross-shard
 * pushes reached, retire detached connections, and publish this shard's
 * diagnostics.
 *
 * The retirement pass arrives as a callback because a lane handle belongs to
 * exactly one facade. Each caller supplies a thunk defined beside its own
 * facade, so the cast back to a concrete lane type happens where that type is
 * known to be right -- there is no generic lane, and no way for one facade's
 * pump to retire the other's connection.
 */
typedef bool (*relay_reap_fn)(moqr_bind_t *bind, void *lane);

/* Raw MsQuic: the lane came from moq_msquic_managed_lane(). */
static bool
reap_raw(moqr_bind_t *bind, void *lane)
{
    return moqr_relay_reap_pass(bind, (moq_msquic_managed_lane_t *)lane, NULL);
}

/* Read a WebTransport shard's doorbell counters into the V3 row's adapter
 * struct. Returns false when this build or this run has no WebTransport
 * facade, which the caller reports as a refusal rather than as zeros. */
static bool
moqr_wt_lane_stats(const serve_lanes_ctx_t *ctx,
                   const moqr_cli_shard_plan_t *plan, uint32_t shard,
                   moq_msquic_lane_stats_t *out)
{
#ifdef MOQR_DUAL_LISTENER
    if (ctx != NULL && ctx->wt != NULL && shard >= plan->wt_first &&
        shard < plan->wt_first + plan->wt_count) {
        moq_wtquic_msquic_lane_stats_t w;
        memset(&w, 0, sizeof(w));
        w.struct_size = (uint32_t)sizeof(w);
        if (moq_wtquic_msquic_lane_get_stats(
                moq_wtquic_msquic_managed_lane(ctx->wt,
                                               shard - plan->wt_first),
                &w, sizeof(w)) != MOQ_OK) {
            return false;
        }
        out->wakes_same_lane = w.wakes_same_lane;
        out->wakes_cross_lane = w.wakes_cross_lane;
        out->wakes_external = w.wakes_external;
        out->wakes_coalesced = w.wakes_coalesced;
        out->pump_sweeps = w.pump_sweeps;
        out->deadline_sweeps = w.deadline_sweeps;
        out->idle_cap_wakes = w.idle_cap_wakes;
        out->wake_to_pump_max_us = w.wake_to_pump_max_us;
        out->wake_to_pump_total_us = w.wake_to_pump_total_us;
        out->wake_to_pump_samples = w.wake_to_pump_samples;
        out->service_passes = w.service_passes;
        out->flush_sends = w.flush_sends;
        out->flush_bytes = w.flush_bytes;
        return true;
    }
#else
    (void)ctx;
    (void)plan;
    (void)shard;
    (void)out;
#endif
    return false;
}

#ifdef MOQR_DUAL_LISTENER
/* WebTransport: the lane came from moq_wtquic_msquic_managed_lane(). */
static bool
reap_wt(moqr_bind_t *bind, void *lane)
{
    return moqr_relay_reap_pass_wt(
        bind, (moq_wtquic_msquic_managed_lane_t *)lane, NULL);
}
#endif

static int
relay_pump_shard(serve_lanes_ctx_t *ctx, moq_msquic_managed_t *raw,
                 uint32_t shard, uint64_t now_us, moqr_bind_t *bind,
                 relay_reap_fn reap, void *lane)
{
    uint64_t mask = 0;
    moqr_result_t rc =
        moqr_shards_step_shard(ctx->shards, (uint16_t)shard, now_us, &mask);
    ctx->pump_turns[shard]++;
    for (uint32_t d = 0; mask != 0 && d < ctx->lanes; d++) {
        if (mask & (1ull << d)) {
            serve_wake_shard(ctx, raw, d);
            ctx->wake_pushes[shard]++;
        }
    }
    if (rc != MOQR_OK) {
        /* Bind pump error or this shard's manager fail-stop: terminate the
         * whole facade cleanly rather than serve over a lost observation. */
        fprintf(stderr, "MOQ5 Relay: shard %u step failed (%d) — stopping\n",
                shard, (int)rc);
        return 1;
    }
    /* A binding this step detached must be retired now: nothing else wakes
     * this lane on its behalf. */
    if (!reap(bind, lane)) {
        fprintf(stderr, "MOQ5 Relay: shard %u connection retirement failed "
                        "— stopping\n", shard);
        return 1;
    }

    /* Operator dumps: THIS lane renders its own route + journal dumps once
     * per new epoch (safe here — its core/mgr/trace are read only inside
     * its own pump), and PUBLISHES its metrics row for the coordinator —
     * per-lane Prometheus documents are gone; the main thread renders one
     * multi-shard exposition once every row carries the newest epoch. */
    char lbl[32];
    unsigned re = atomic_load(&g_route_dump_epoch);
    if (ctx->seen_route_epoch[shard] != re) {
        ctx->seen_route_epoch[shard] = re;
        snprintf(lbl, sizeof(lbl), "shard %u routes", shard);
        dump_emit(lbl, dump_routes_fn,
                  moqr_shards_core(ctx->shards, (uint16_t)shard));
        dump_journal_ctx_t jc = { ctx->shards, (uint16_t)shard };
        snprintf(lbl, sizeof(lbl), "shard %u journal", shard);
        dump_emit(lbl, dump_journal_fn, &jc);
    }

    /* Publish a metrics row for the open generation, at most once per
     * generation. Stats are gathered ONLY when the serial advances, so an
     * idle lane costs nothing. */
    uint64_t serial = moqr_broker_epoch_fn(&g_metrics_broker);
    if (serial != 0u && ctx->seen_metrics_serial[shard] != serial) {
        ctx->seen_metrics_serial[shard] = serial;
        moqr_cli_snapshot_stats_t st;
        memset(&st, 0, sizeof(st));
        moqr_core_get_stats(moqr_shards_core(ctx->shards, (uint16_t)shard),
                            &st.core);
        moqr_bind_get_stats(bind, &st.bind);
        /* This composition owns a shard runtime, so stats are expected: a
         * refusal poisons the epoch rather than publishing zeros. ABSENT is
         * for the single-lane composition, which has no shard plane at all. */
        st.shard_cap =
            (moqr_shards_get_stats(ctx->shards, (uint16_t)shard, &st.shard) ==
             MOQR_OK)
                ? MOQR_CLI_CAP_VALID
                : MOQR_CLI_CAP_REFUSED;
        st.lane_wakes = ctx->wake_pushes[shard];
#ifdef MOQR_BIND_TESTING
        /* Verify build: the LIVE blocked-reason aggregate for this lane,
         * read on the owning lane thread while connections are still up and
         * parked state is intact — a post-detach read would report zeros. */
        st.blocked_valid =
            moqr_bind_debug_blocked_aggregate(bind, &st.blocked) == MOQR_OK;
        /* Stranding diagnostics (lane-owned, NON-authoritative — the
         * coordinated RELAY_BLOCKED_V0 record is unchanged): per ACTIVE
         * conn slot over the RESOLVED capacity, the downstream pool
         * occupancy, the delivery-scheduler state, and the three historical
         * blockage counters — the blocked connection is IDENTIFIED by its
         * counters (the parser requires exactly one bind_sg_total > 0 row),
         * never inferred. Interpretation of the occ/ready split is
         * PRE-REGISTERED in moqr_cli_sg_diagnose (blockedstats.h): combined
         * with seal presence over an interval-complete SEALLOG window it
         * yields exactly one of NOT_APPLIED / NO_RELEASE / PASS_INCOMPLETE
         * / NOT_REARMED / DOORBELL_UNCONSUMED — never a post-hoc story. */
        for (uint32_t sl = 0; sl < moqr_bind_debug_max_conns(bind); sl++) {
            uint32_t occ = moqr_bind_debug_conn_open_sgs(bind, sl);
            bool rd = false, pk = false;
            uint8_t reason = 0;
            moqr_bind_debug_dl_state(bind, sl, &rd, &pk, &reason);
            uint64_t bc[3];
            moqr_bind_debug_conn_blocked_counts(bind, sl, bc);
            if (occ == 0 && !rd && !pk && bc[0] == 0 && bc[1] == 0 &&
                bc[2] == 0) {
                continue;   /* quiet slot: not worth a line */
            }
            /* Demand attribution joins the blocked conn to its SEALLOG
             * seals exactly: the bind captured the refused delivery's
             * TRACK (first refusal latches; a second track — a second
             * demand — flags ambiguity), and the shard's demand table
             * resolves track → demand id. 0 stays 0 (a local track has no
             * demand) and the acceptance fails closed on it. */
            uint64_t bsd = 0;
            bool bsa = false;
            {
                uint64_t btr = 0, btg = 0;
                if (moqr_bind_debug_conn_bind_sg_track(bind, sl, &btr, &btg,
                                                       &bsa)) {
                    bsd = moqr_shards_debug_track_demand(ctx->shards, shard,
                                                         btr, btg);
                }
            }
            moqr_cli_sgdiag_row_t dg = {
                .epoch = serial,
                .lane = shard,
                .slot = sl,
                .occ = occ,
                .ready = rd ? 1u : 0u,
                .parked = pk ? 1u : 0u,
                .reason = reason,
                .action_cap_total = bc[0],
                .session_sg_total = bc[1],
                .bind_sg_total = bc[2],
                .bind_sg_demand = bsd,
                .bind_sg_demand_ambiguous = bsa ? 1u : 0u,
            };
            char dline[MOQR_BLOCKED_ROW_MAX];
            if (moqr_cli_sgdiag_format(dline, sizeof(dline), &dg) > 0) {
                fprintf(stderr, "%s\n", dline);
            }
        }
        /* Destination SG_SEAL ingest evidence, oldest first (bounded ring;
         * seq is the lifetime ingest order on this lane). The ring keeps only
         * the newest window, so lifetime completeness is impossible after it
         * wraps: emit a RELAY_SEALMETA header FIRST (always, even at zero
         * events) pinning the window's lower edge and the lifetime total, so
         * a later dump can prove interval-completeness against a prior cursor
         * without every historical seal surviving. */
        {
            moqr_shards_seal_ev_t evs[32];
            uint64_t seal_total = 0;
            uint32_t n = moqr_shards_debug_seal_log(ctx->shards, shard, evs,
                                                    32, &seal_total);
            moqr_cli_seallog_meta_t sm = {
                .epoch = serial,
                .lane = shard,
                /* oldest retained seq; when nothing is retained the window is
                 * the empty interval [total, total). */
                .first_retained_seq = n > 0 ? evs[0].seq : seal_total,
                .lifetime_total = seal_total,
                .retained_count = n,
            };
            char mline[MOQR_BLOCKED_ROW_MAX];
            if (moqr_cli_seallog_meta_format(mline, sizeof(mline), &sm) > 0) {
                fprintf(stderr, "%s\n", mline);
            }
            for (uint32_t k = 0; k < n; k++) {
                moqr_cli_seallog_row_t sr = {
                    .epoch = serial,
                    .lane = shard,
                    .seq = evs[k].seq,
                    .total = seal_total,
                    .src = evs[k].src,
                    .demand = evs[k].demand_id,
                    .group_id = evs[k].group_id,
                    .subgroup_id = evs[k].subgroup_id,
                };
                char sline[MOQR_BLOCKED_ROW_MAX];
                if (moqr_cli_seallog_format(sline, sizeof(sline), &sr) > 0) {
                    fprintf(stderr, "%s\n", sline);
                }
            }
        }
#endif
        moqr_cli_snapshot_publish(ctx->snap, shard, &st, serial);
        /* Published -- tell the owner, at the publish point. */
        admin_notify_published(&ctx->admin_listen);
    }
    unsigned te = atomic_load(&g_dump_trace_epoch);
    if (ctx->seen_trace_epoch[shard] != te) {
        ctx->seen_trace_epoch[shard] = te;
        snprintf(lbl, sizeof(lbl), "shard %u trace", shard);
        dump_emit(lbl, dump_trace_fn,
                  moqr_shards_trace(ctx->shards, (uint16_t)shard));
    }
    return atomic_load(&g_stop) ? 1 : 0;
}

static int
relay_lanes_pump(moq_msquic_managed_t *m, moq_msquic_managed_lane_t *lane,
                 uint64_t now_us, void *vctx)
{
    serve_lanes_ctx_t *ctx = vctx;
    uint32_t shard =
        moqr_cli_shard_of_raw_lane(&ctx->plan, moq_msquic_lane_index(lane));
    if (shard == UINT32_MAX || shard >= moqr_shards_count(ctx->shards)) {
        return 0;   /* adapter clamped lanes below shards: nothing to drive */
    }
    moqr_bind_t *bind = moqr_shards_bind(ctx->shards, (uint16_t)shard);
    for (moq_msquic_managed_conn_t *conn = moq_msquic_lane_next_conn(lane,
                                                                     NULL);
         conn != NULL; conn = moq_msquic_lane_next_conn(lane, conn)) {
        void *tag = moq_msquic_managed_conn_user(conn);
        if (tag == RELAY_CONN_DEAD) {
            continue;
        }
        moq_session_t *s = moq_msquic_managed_conn_session(conn);
        if (s == NULL) {
            continue;
        }
        if (tag == NULL) {
            if (moqr_bind_conn_open(bind, s,
                                    moq_msquic_managed_conn_negotiated_version(conn)) == MOQR_OK) {
                moq_msquic_managed_conn_set_user(conn, RELAY_CONN_OPENED);
            } else {
                moq_msquic_managed_conn_set_user(conn, RELAY_CONN_DEAD);
                moq_msquic_managed_conn_close(conn, 0);
            }
        }
    }

    return relay_pump_shard(ctx, m, shard, now_us, bind, reap_raw, lane);
}

#ifdef MOQR_DUAL_LISTENER
/* The WebTransport lane pump. Same runtime, same shard step; only the facade
 * that owns the connections differs. Its lanes address the shard range the
 * plan gave WebTransport, never the raw range. */
static int
relay_wt_lanes_pump(moq_wtquic_msquic_managed_t *m,
                    moq_wtquic_msquic_managed_lane_t *lane, uint64_t now_us,
                    void *vctx)
{
    serve_lanes_ctx_t *ctx = vctx;
    (void)m;
    uint32_t shard = moqr_cli_shard_of_wt_lane(
        &ctx->plan, moq_wtquic_msquic_lane_index(lane));
    if (shard == UINT32_MAX || shard >= moqr_shards_count(ctx->shards)) {
        return 0;
    }
    moqr_bind_t *bind = moqr_shards_bind(ctx->shards, (uint16_t)shard);
    for (moq_wtquic_msquic_managed_conn_t *conn =
             moq_wtquic_msquic_lane_next_conn(lane, NULL);
         conn != NULL;
         conn = moq_wtquic_msquic_lane_next_conn(lane, conn)) {
        void *tag = moq_wtquic_msquic_managed_conn_user(conn);
        if (tag == RELAY_CONN_DEAD) {
            continue;
        }
        moq_session_t *sess = moq_wtquic_msquic_managed_conn_session(conn);
        if (sess == NULL) {
            continue;
        }
        if (tag == NULL) {
            if (moqr_bind_conn_open(
                    bind, sess,
                    moq_wtquic_msquic_managed_conn_negotiated_version(conn)) ==
                MOQR_OK) {
                moq_wtquic_msquic_managed_conn_set_user(conn,
                                                        RELAY_CONN_OPENED);
            } else {
                moq_wtquic_msquic_managed_conn_set_user(conn, RELAY_CONN_DEAD);
                moq_wtquic_msquic_managed_conn_close(conn, 0);
            }
        }
    }
    return relay_pump_shard(ctx, ctx->raw, shard, now_us, bind, reap_wt,
                            lane);
}
#endif /* MOQR_DUAL_LISTENER */

#ifndef MOQR_BIND_TESTING
static uint64_t
coord_try_dump(const serve_lanes_ctx_t *ctx, moqr_cli_snapshot_t *snap,
               uint64_t rendered, uint64_t *incomplete_told,
               uint64_t *fail_told, bool *out_wake_lanes)
{
    return coord_try_dump_labels(ctx->labels, snap, rendered, incomplete_told,
                                 fail_told, out_wake_lanes);
}
#else  /* MOQR_BIND_TESTING: moq-relay-verify emits RELAY_BLOCKED_V0 instead */

/* The blocked-document producer: serialize one coherent same-epoch row set
 * (already collected under the snapshot mutexes) into K RELAY_BLOCKED_V0
 * lines, lane 0..K-1 ascending. A lane whose LIVE aggregate failed
 * (blocked_valid == false) emits an explicit refusal row that invalidates the
 * whole epoch for the gauntlet parser — never valid-looking zeros. Produce
 * only; the render cycle's final epoch re-read licenses emission. */
typedef struct blocked_doc {
    uint32_t lanes;
    size_t   len;
    char    *buf;    /* lanes * ~256 B, caller-allocated */
    size_t   cap;
} blocked_doc_t;

static moqr_result_t
blocked_produce(void *pctx, const moqr_cli_snapshot_stats_t *rows,
                uint64_t epoch)
{
    blocked_doc_t *d = pctx;
    d->len = 0;
    for (uint32_t i = 0; i < d->lanes; i++) {
        char line[MOQR_BLOCKED_ROW_MAX];   /* fits any legal all-max row */
        int w;
        if (rows[i].blocked_valid) {
            const moqr_bind_blocked_agg_t *a = &rows[i].blocked;
            moqr_cli_blocked_row_t r = {
                .epoch = epoch,
                .lane = i,
                .live_conns = a->live_conns,
                .conns_action_cap = a->conns_action_cap,
                .conns_session_sg = a->conns_session_sg,
                .conns_bind_sg = a->conns_bind_sg,
                .action_cap_total = a->action_cap_total,
                .session_sg_total = a->session_sg_total,
                .bind_sg_total = a->bind_sg_total,
                .parked_action_cap = a->parked_action_cap,
                .parked_session_sg = a->parked_session_sg,
            };
            w = moqr_cli_blocked_format(line, sizeof(line), &r);
            if (w < 0) {
                /* A legal row cannot overflow MOQR_BLOCKED_ROW_MAX; if it
                 * somehow does, degrade this lane to an explicit refusal
                 * (invalidates the epoch) rather than stalling every epoch. */
                w = moqr_cli_blocked_format_refused(line, sizeof(line), epoch,
                                                    i);
            }
        } else {
            w = moqr_cli_blocked_format_refused(line, sizeof(line), epoch, i);
        }
        if (w < 0 || d->len + (size_t)w + 1 >= d->cap) {
            return MOQR_ERR_INTERNAL;   /* fail closed: no partial document */
        }
        memcpy(d->buf + d->len, line, (size_t)w);
        d->len += (size_t)w;
        d->buf[d->len++] = '\n';
    }
    d->buf[d->len] = '\0';
    return MOQR_OK;
}

/* One verify-build coordinator attempt: the same collect -> produce ->
 * final-epoch-reread cycle, but the producer is the blocked document. Emits
 * exactly once, only when the cycle licenses it. */
static uint64_t
coord_try_dump_blocked(const serve_lanes_ctx_t *ctx, moqr_cli_snapshot_t *snap,
                       uint64_t rendered, uint64_t *incomplete_told,
                       uint64_t *fail_told, bool *out_wake_lanes)
{
    if (out_wake_lanes != NULL) {
        *out_wake_lanes = false;
    }
    moqr_cli_snapshot_stats_t *rows = calloc(ctx->lanes, sizeof(*rows));
    blocked_doc_t doc;
    doc.lanes = ctx->lanes;
    /* Room for the newline after each row plus the document NUL — sized off
     * the shared per-row maximum so a full lane set of all-max rows fits. */
    doc.cap = (size_t)ctx->lanes * (MOQR_BLOCKED_ROW_MAX + 1u) + 1u;
    doc.buf = malloc(doc.cap);
    if (rows == NULL || doc.buf == NULL) {
        free(rows);
        free(doc.buf);
        return rendered;   /* transient; retried on the next wait tick */
    }

    /* The verify producer rides the SAME generation lifecycle as the normal
     * one. Rendering without retiring the generation would leave its slot
     * COLLECTING forever: the lanes have already recorded that serial so they
     * never republish, the coordinator stays busy reprinting a finished
     * document, and every later request joins a generation that can never
     * retire. */
    uint64_t target = 0;
    uint32_t target_demand = 0;
    if (!moqr_broker_current(&g_metrics_broker, &target, &target_demand)) {
        free(rows);
        free(doc.buf);
        return rendered;
    }

    coord_midflight();

    coord_retire_ctx_t rc_ctx = {
        target, "\n===== moq-relay-verify blocked (epoch %llu) =====\n%s\n",
        false, false
    };
    uint64_t      epoch = 0;
    moqr_result_t rc = moqr_cli_snapshot_render(
        snap, coord_epoch, NULL, rows, blocked_produce, &doc, &epoch);

    if (rc == MOQR_OK || rc == MOQR_ERR_INVAL) {
        /* doc.buf is owned here, so it is still valid across the retirement. */
        coord_retire(&rc_ctx, rc == MOQR_OK ? doc.buf : NULL, epoch,
                     rc == MOQR_ERR_INVAL);
        if (rc_ctx.retired) {
            rendered = epoch;
            if (out_wake_lanes != NULL) {
                *out_wake_lanes = rc_ctx.wake_lanes;
            }
        }
    } else if (rc == MOQR_ERR_WOULD_BLOCK) {
        if ((target_demand & MOQR_BROKER_DEMAND_SIGNAL) != 0u &&
            *incomplete_told != epoch) {
            fprintf(stderr,
                    "moq-relay-verify: blocked epoch %llu incomplete — "
                    "waiting for every lane to publish\n",
                    (unsigned long long)epoch);
            *incomplete_told = epoch;
        }
    } else if ((target_demand & MOQR_BROKER_DEMAND_SIGNAL) != 0u &&
               *fail_told != epoch) {
        fprintf(stderr,
                "moq-relay-verify: blocked epoch %llu dump failed (%d) — "
                "retrying\n",
                (unsigned long long)epoch, (int)rc);
        *fail_told = epoch;
    }
    free(rows);
    free(doc.buf);
    return rendered;
}
#endif  /* MOQR_BIND_TESTING */

static int
cmd_serve_lanes(const moqr_cli_config_t *cfg)
{
    if (cfg->cert[0] == '\0' || cfg->key[0] == '\0') {
        fprintf(stderr, "serve requires listener.cert and listener.key\n");
        return 2;
    }
    /* The serve log, before any resource exists: a refused sink (the fixed
     * line cannot hold every event of the schema) serves nothing. */
    moqr_cli_log_t log;
    if (moqr_cli_log_init(&log, cfg->logging.format, stdout, stderr,
                          SERVE_LOG_IO) != MOQR_OK) {
        fprintf(stderr, "logging: the event sink could not be initialised\n");
        return 2;
    }

    /* One shard per lane; each shard = {core, bind, trace}. The CLI-private
     * serve seam builds the shard config (production admission = lanes > 1,
     * the strict per-lane bind clamp) AND the facade admission cap from the
     * ONE builder `capacity` also consumes — so the ceiling described is
     * exactly the config running here. live_visibility = true: lanes step
     * concurrently with no shared round to advance. */
    /* Resolve the shard plan first: a combined lane count past the runtime
     * cap is a configuration error, and it is refused here -- before a shard,
     * a facade or a listener exists. */
    moqr_cli_shard_plan_t plan;
    {
        char perr[192] = { 0 };
        if (moqr_cli_shard_plan(cfg, &plan, perr, sizeof(perr)) != MOQR_OK) {
            fprintf(stderr, "serve: %s\n", perr);
            return serve_done(&log, 2);
        }
    }
#ifndef MOQR_DUAL_LISTENER
    /* The shared preflight above already refused this for every command; this
     * is the defensive backstop for internal entry points that reach the serve
     * composition directly. It asks the SAME question, through the same helper,
     * so the two can never answer differently. */
    if (moqr_cli_config_has_webtransport(cfg)) {
        fprintf(stderr,
                "serve: this build has no WebTransport listener support\n");
        return serve_done(&log, 2);
    }
#endif
    if (plan.wt_count > 0 &&
        (cfg->wt.cert[0] == '\0' || cfg->wt.key[0] == '\0')) {
        fprintf(stderr, "serve requires webtransport.cert and .key\n");
        return serve_done(&log, 2);
    }

    moqr_shards_cfg_t scfg;
    uint32_t serve_max_conns = 0;
    uint32_t raw_max_conns = 0, wt_max_conns = 0;
    if (moqr_cli_facade_caps(cfg, moq_alloc_default(), &raw_max_conns,
                             &wt_max_conns) != MOQR_OK) {
        fprintf(stderr, "serve: config refused (admission caps)\n");
        return serve_done(&log, 2);
    }
    if (moqr_cli_serve_compose(cfg, moq_alloc_default(), &scfg,
                               &serve_max_conns) != MOQR_OK) {
        fprintf(stderr, "serve: config refused (budgets vs lanes)\n");
        return serve_done(&log, 2);
    }

    moqr_shards_t *shards = NULL;
    if (moqr_shards_create(&scfg, &shards) != MOQR_OK) {
        fprintf(stderr, "relay shard runtime create failed\n");
        return serve_done(&log, 1);
    }

    serve_lanes_ctx_t *ctx = calloc(1, sizeof(*ctx));
    if (ctx == NULL) {
        moqr_shards_destroy(shards);
        return serve_done(&log, 1);
    }
    /* Snapshot-row storage (counted in cli_runtime_bytes). Publication and
     * rendering are wired by the observability slice; allocating the rows
     * here keeps the printed ceiling equal to what serve really requests. */
    moqr_cli_snapshot_t snap;
    /* Every lane pump reads the broker, and a managed facade starts its
     * doorbell threads before its create call returns — so the broker must be
     * fully constructed before either facade can exist, not merely before the
     * loop that drives them. */
    if (moqr_broker_init(&g_metrics_broker, MOQR_BROKER_BANKS) != MOQR_OK) {
        fprintf(stderr, "MOQ5 Relay: metrics broker init failed\n");
        moqr_shards_destroy(shards);
        free(ctx);
        return serve_done(&log, 1);
    }
    if (moqr_cli_snapshot_init(&snap, plan.total_shards,
                               moq_alloc_default()) !=
        MOQR_OK) {
        free(ctx);
        moqr_broker_destroy(&g_metrics_broker);
        moqr_shards_destroy(shards);
        return serve_done(&log, 1);
    }
    ctx->shards = shards;
    ctx->snap = &snap;
    ctx->lanes = plan.total_shards;
    ctx->plan = plan;
    serve_lanes_fill_labels(ctx->labels, cfg, &plan);


    moq_msquic_managed_cfg_t tcfg;
    moq_msquic_managed_cfg_init_sized(&tcfg, sizeof(tcfg));
    tcfg.alloc = moq_alloc_default();
    tcfg.perspective = MOQ_PERSPECTIVE_SERVER;
    tcfg.host = cfg->host;
    tcfg.port = (uint16_t)cfg->port;
    tcfg.cert_path = cfg->cert;
    tcfg.key_path = cfg->key;
    tcfg.insecure_skip_verify = cfg->insecure_skip_verify;
    tcfg.send_request_capacity = true;
    tcfg.initial_request_capacity = 1024;
    tcfg.streaming_objects = true;
    moqr_cli_apply_versions(cfg, &tcfg);
    tcfg.lane_count = cfg->lanes;
    /* Facade admission cap: the seam's usable-bindings-per-shard x lanes
     * rule (the strict per-lane clamp, not lanes x max_bindings). */
    tcfg.max_connections = raw_max_conns;
    tcfg.on_lane_pump = relay_lanes_pump;
    tcfg.on_lane_pump_user = ctx;
#ifdef MOQR_VERIFY_SEAM
    /* Blocked-scenario seam: constrain every relay-side session's outgoing
     * subgroup pool (SESSION_SG induction; the bind override rode the shard
     * builder above). Echoed so the gauntlet asserts the constraint applied
     * rather than trusting the environment reached the process. */
    tcfg.max_open_subgroups = moqr_cli_verify_session_max_sgs();
    if (moqr_cli_verify_bind_max_sgs() != 0 ||
        moqr_cli_verify_session_max_sgs() != 0) {
        printf(MOQR_SEAM_NAME ": seam bind_max_open_subgroups=%u "
               "session_max_open_subgroups=%u (0=default)\n",
               moqr_cli_verify_bind_max_sgs(),
               moqr_cli_verify_session_max_sgs());
    }
#endif

    moq_msquic_managed_t *t = NULL;
    if (moq_msquic_managed_create(&tcfg, &t) != MOQ_OK) {
        fprintf(stderr, "transport create failed (port %d)\n", cfg->port);
        moqr_cli_snapshot_destroy(&snap);
        moqr_broker_destroy(&g_metrics_broker);
        moqr_shards_destroy(shards);
        free(ctx);
        return serve_done(&log, 1);
    }

    ctx->raw = t;

#ifdef MOQR_DUAL_LISTENER
    /* The optional second listener. Startup is all-or-nothing: if this one
     * cannot come up, the raw listener that already did is torn down and no
     * readiness line is printed, so nothing downstream can mistake a
     * half-open relay for a serving one. */
    if (plan.wt_count > 0) {
        /* This listener's own share of the plan, never the process totals. */
        if (moqr_cli_wt_listener_create(cfg, plan.wt_count, wt_max_conns,
                                        relay_wt_lanes_pump, ctx,
                                        &ctx->wt) != MOQ_OK) {
            fprintf(stderr,
                    "webtransport transport create failed (port %d)\n",
                    cfg->wt.port);
            (void)moq_msquic_managed_stop(t);
            moq_msquic_managed_destroy(t);
            moqr_cli_snapshot_destroy(&snap);
            moqr_broker_destroy(&g_metrics_broker);
            moqr_shards_destroy(shards);
            free(ctx);
            return serve_done(&log, 1);
        }
    }
#endif

    /*
     * The ceiling gate runs BEFORE the endpoint is started and before any
     * readiness line is printed. Publishing readiness over a gate that can
     * still fail is a falsehood, and unwinding a live owner thread from that
     * failure branch would mean tearing down the snapshot and broker
     * underneath it.
     */
    if (print_capacity(cfg, sizeof(serve_lanes_ctx_t),
                       moqr_cli_log_prose_stream(&log)) != 0) {
        /* An undescribable ceiling never serves. No endpoint exists yet. */
#ifdef MOQR_DUAL_LISTENER
        if (ctx->wt != NULL) {
            (void)moq_wtquic_msquic_managed_stop(ctx->wt);
            moq_wtquic_msquic_managed_destroy(ctx->wt);
            ctx->wt = NULL;
        }
#endif
        (void)moq_msquic_managed_stop(t);
        moq_msquic_managed_destroy(t);
        /* Facades stopped and lanes joined: nothing can reach the broker or
         * the snapshot any more, so both are safe to tear down here. */
        moqr_cli_snapshot_destroy(&snap);
        moqr_broker_destroy(&g_metrics_broker);
        moqr_shards_destroy(shards);
        free(ctx);
        return serve_done(&log, 2);
    }
    serve_install_signals(on_dump_signal_lanes);
    /* The endpoint comes up BEFORE the readiness line, and its failure refuses
     * the whole serve. Its owner thread becomes the sole owner of the broker
     * from here on. */
    admin_owner_t admin_owner;
    moqr_admin_listen_t *admin_l = NULL;
    int admin_rc = 0;
    serve_wake_all_ctx_t admin_wake_ctx = { ctx, t, plan.total_shards };
    /* total_shards, not cfg->lanes: the snapshot has one row per GLOBAL shard,
     * and with the WebTransport listener present that is more than the raw
     * listener's lane count. */
    if (admin_owner_init(&admin_owner, cfg, &snap, ctx->labels,
                         plan.total_shards,
                         sizeof(serve_lanes_ctx_t),
                         serve_wake_every_shard, &admin_wake_ctx) != MOQR_OK ||
        admin_endpoint_start(cfg, &admin_owner, &admin_l) != MOQR_OK) {
        fprintf(stderr,
                "MOQ5 Relay: admin endpoint could not start on %s:%d — "
                "refusing to serve\n", cfg->admin.host, cfg->admin.port);
        admin_owner_destroy(&admin_owner);
#ifdef MOQR_DUAL_LISTENER
        if (ctx->wt != NULL) {
            (void)moq_wtquic_msquic_managed_stop(ctx->wt);
            moq_wtquic_msquic_managed_destroy(ctx->wt);
        }
#endif
        (void)moq_msquic_managed_stop(t);
        moq_msquic_managed_destroy(t);
        moqr_cli_snapshot_destroy(&snap);
        moqr_broker_destroy(&g_metrics_broker);
        moqr_shards_destroy(shards);
        free(ctx);
        return serve_done(&log, 1);
    }
    /* The same two-phase order: the pointer is installed while the endpoint is
     * inert, and only then is it allowed to serve. */
    atomic_store_explicit(&ctx->admin_listen, admin_l, memory_order_release);
    if (admin_endpoint_activate(admin_l) != MOQR_OK) {
        fprintf(stderr,
                "MOQ5 Relay: the admin endpoint did not start serving — "
                "refusing to serve\n");
        /* The facade joins are the reader grace period for admin_listen. Keep
         * the object published and alive until every possible notifier is
         * gone; an atomic clear alone cannot revoke a pointer already loaded. */
#ifdef MOQR_DUAL_LISTENER
        if (ctx->wt != NULL) {
            (void)moq_wtquic_msquic_managed_stop(ctx->wt);
            moq_wtquic_msquic_managed_destroy(ctx->wt);
        }
#endif
        (void)moq_msquic_managed_stop(t);
        moq_msquic_managed_destroy(t);
        atomic_store_explicit(&ctx->admin_listen, NULL, memory_order_release);
        admin_endpoint_destroy(admin_l, &admin_owner);
        moqr_cli_snapshot_destroy(&snap);
        moqr_broker_destroy(&g_metrics_broker);
        moqr_shards_destroy(shards);
        free(ctx);
        return serve_done(&log, 1);
    }
    moqr_cli_serve_log_readiness(&log, cfg, MOQR_CLI_LOG_COMP_LANES,
                        admin_l != NULL ? moqr_admin_listen_port(admin_l) : -1,
                        plan.wt_count, &scfg);
    /* The coordinator loop: between waits, service any outstanding metrics
     * epoch by copying the lanes' published rows (their dump-only mutexes
     * are the ONLY lane state touched — never a live lane's core, bind,
     * journal, or trace) and rendering one multi-shard exposition once the
     * newest requested epoch is complete on every row. An IDLE lane's pump
     * never runs on its own, so an outstanding dump epoch also wakes every
     * lane — each pump then renders its route/journal (or trace) dump and
     * publishes its row; redundant wakes are harmless by design. */
    uint64_t rendered_epoch = 0;
    uint64_t incomplete_told = 0;
    uint64_t fail_told = 0;
    unsigned woken_trace = 0;
    while (!atomic_load(&g_stop)) {
        if (moq_msquic_managed_wait(t, 500 * 1000) == MOQ_ERR_CLOSED) {
            break;
        }
        /* An endpoint whose owner has died is a dead admin surface (every
         * target it advertises), and a relay that keeps serving while
         * advertising one is lying about it. */
        if (admin_l != NULL && moqr_admin_listen_owner_exited(admin_l)) {
            fprintf(stderr,
                    "MOQ5 Relay: the admin endpoint owner exited (health %u) "
                    "— stopping\n",
                    (unsigned)moqr_admin_listen_health(admin_l));
            admin_rc = 1;
            break;
        }
#ifdef MOQR_DUAL_LISTENER
        /* A terminal WebTransport facade must end the whole relay: leaving the
         * raw listener serving would be a half-open relay that still answers on
         * one transport while browsers can no longer reach it. Only a true
         * terminal counts -- a zero-timeout wait that simply expires is not one,
         * and does not stall this loop. */
        if (ctx->wt != NULL &&
            moq_wtquic_msquic_managed_wait(ctx->wt, 0) == MOQ_ERR_CLOSED) {
            break;
        }
#endif
        /* Turn a latched signal into a broker request. Assigning the serial
         * here rather than in the handler is what keeps generation identity
         * and output demand separable. */
        bool wake_lanes = false;
        if (admin_l == NULL && atomic_exchange(&g_signal_metrics_pending, 0)) {
            uint64_t issued = 0;
            bool     w = false;
            if (moqr_broker_request(&g_metrics_broker,
                                    MOQR_BROKER_DEMAND_SIGNAL, &issued,
                                    &w) == MOQR_ERR_WOULD_BLOCK) {
                /* Every bank is busy; the broker remembered the demand and
                 * will open a generation when one frees. */
                (void)issued;
            }
            wake_lanes = wake_lanes || w;
        }

        /* When the endpoint exists, EVERY broker-driven wake belongs to its
         * owner: it already consumed the generation transition's one wake, and
         * repeating it here on the wait cadence would wake every lane again
         * for the whole life of a slow generation. Only trace demand remains
         * main-loop-owned. */
        bool state_owed =
            admin_l == NULL && moqr_broker_busy(&g_metrics_broker);
        unsigned te = atomic_load(&g_dump_trace_epoch);
        if (wake_lanes || state_owed || te != woken_trace) {
            woken_trace = te;
            for (uint32_t d = 0; d < plan.total_shards; d++) {
                /* a WebTransport shard has no raw lane: route by plan */
                serve_wake_shard(ctx, t, d);
            }
        }
        bool wake_after_release = false;
        /* `state_owed` is already false whenever the endpoint exists, so the
         * coordinator cannot run beside its owner. */
        if (state_owed) {
#ifdef MOQR_BIND_TESTING
            rendered_epoch = coord_try_dump_blocked(
                ctx, &snap, rendered_epoch, &incomplete_told, &fail_told,
                &wake_after_release);
#else
            rendered_epoch = coord_try_dump(ctx, &snap, rendered_epoch,
                                            &incomplete_told, &fail_told,
                                            &wake_after_release);
#endif
            /* Retiring the generation may have opened deferred work; that flag
             * is its only notice, so wake every lane now rather than waiting
             * for the next poll cadence. */
            if (wake_after_release) {
                for (uint32_t d = 0; d < plan.total_shards; d++) {
                    serve_wake_shard(ctx, t, d);
                }
            }
        }
    }
    /* The endpoint is cancelled and joined BEFORE either facade stops, so the
     * owner thread has settled every bank while everything it reads is still
     * alive. The endpoint OBJECT outlives this, until every lane has joined. */
    if (admin_endpoint_stop(admin_l) != MOQR_OK && admin_rc == 0) {
        fprintf(stderr,
                "MOQ5 Relay: the admin endpoint owner did not stop cleanly\n");
        admin_rc = 1;
    }
#ifdef MOQR_DUAL_LISTENER
    /* Both facades stop and join BEFORE any shard is destroyed: a lane still
     * running would be stepping a shard that was going away underneath it. */
    if (ctx->wt != NULL) {
        (void)moq_wtquic_msquic_managed_stop(ctx->wt);
    }
#endif
    (void)moq_msquic_managed_stop(t);   /* joins every lane; safe to read now */

    /* A global shard's doorbell counters live in the facade that owns it, so
     * the WebTransport range must be read from the WebTransport facade -- the
     * raw facade has no lane for those shards at all. */
    #define MOQR_ADAPTER_STATS_FOR(shard_ix, out_ad)                          \
        (((shard_ix) < plan.raw_first + plan.raw_count)                       \
             ? moq_msquic_lane_get_stats(                                     \
                   moq_msquic_managed_lane(t, (shard_ix) - plan.raw_first),   \
                   &(out_ad), sizeof(out_ad)) == MOQ_OK                       \
             : moqr_wt_lane_stats(ctx, &plan, (shard_ix), &(out_ad)))

    /* Per-lane attribution rows: the adapter's doorbell counters joined with
     * the same lane's shard counters, one stable machine-readable line per
     * lane (post-join, coordinator thread — both getters are legal here: an
     * external thread snapshots the adapter stats under the lane lock). A
     * refused getter emits an explicit REFUSAL record, never zeros. */
    for (uint32_t i = 0; i < plan.total_shards; i++) {
        moq_msquic_lane_stats_t ad;
        moqr_shards_stats_t ss;
        memset(&ad, 0, sizeof(ad));
        ad.struct_size = (uint32_t)sizeof(ad);
        bool ad_ok = MOQR_ADAPTER_STATS_FOR(i, ad);
        bool sh_ok =
            moqr_shards_get_stats(shards, (uint16_t)i, &ss) == MOQR_OK;
        moqr_cli_serve_log_lane(&log, i, ad_ok, &ad, sh_ok, &ss);
    }
    /* Per-directed-pair rows, in the deterministic record order the parser
     * enforces (src-major, ascending, self skipped): exactly K*(K-1) rows
     * or a refusal — a partial pair record must fail closed downstream. */
    for (uint32_t src = 0; src < plan.total_shards; src++) {
        for (uint32_t dst = 0; dst < plan.total_shards; dst++) {
            if (dst == src) {
                continue;
            }
            moqr_shards_pair_stats_t pst;
            bool ok = moqr_shards_get_pair_stats(shards, (uint16_t)src,
                                                 (uint16_t)dst, &pst,
                                                 sizeof(pst)) == MOQR_OK;
            moqr_cli_serve_log_pair(&log, src, dst, ok, &pst);
        }
    }

    uint64_t sum_ingested = 0, sum_delivered = 0, sum_sesserr = 0;
    uint32_t sum_conns = 0, sum_tracks = 0;
    for (uint32_t i = 0; i < plan.total_shards; i++) {
        moqr_bind_stats_t bs;
        moqr_bind_get_stats(moqr_shards_bind(shards, (uint16_t)i), &bs);
        moqr_core_stats_t cs;
        moqr_core_get_stats(moqr_shards_core(shards, (uint16_t)i), &cs);
        /* Post-join direct snapshot: every lane has joined, so the shard
         * stats are readable here without requesting another epoch. A
         * refused (poisoned) snapshot is diagnosed, never printed as
         * zeros-that-look-valid. */
        moqr_shards_stats_t ss;
        bool ss_ok = moqr_shards_get_stats(shards, (uint16_t)i, &ss) ==
                     MOQR_OK;
        moqr_cli_serve_log_stop_shard(&log, i, &bs, &cs, ctx->pump_turns[i],
                             ctx->wake_pushes[i], ss_ok, &ss);
        sum_conns += bs.conns;
        sum_tracks += cs.tracks;
        sum_ingested += cs.ingested_total;
        sum_delivered += cs.delivered_total;
        sum_sesserr += bs.session_errors;
    }
    moqr_cli_serve_log_stop_total(&log, plan.total_shards, sum_conns, sum_tracks,
                         sum_ingested, sum_delivered, sum_sesserr);

#ifdef MOQR_DUAL_LISTENER
    /* Both facades have stopped (and therefore joined) above; destroy the
     * WebTransport one here, before any shard is destroyed. */
    if (ctx->wt != NULL) {
        moq_wtquic_msquic_managed_destroy(ctx->wt);
        ctx->wt = NULL;
    }
#endif
    moq_msquic_managed_destroy(t);
    /* Every facade lane has joined, so nothing can publish or notify any more;
     * only now is the endpoint object safe to release. */
    atomic_store_explicit(&ctx->admin_listen, NULL, memory_order_release);
    admin_endpoint_destroy(admin_l, &admin_owner);
    moqr_cli_snapshot_destroy(&snap);
    moqr_broker_destroy(&g_metrics_broker);
    moqr_shards_destroy(shards);   /* owns per-shard bind/core/trace teardown */
    free(ctx);
    return serve_done(&log, admin_rc);
}

int
main(int argc, char **argv)
{
    moqr_cli_args_t args;

    if (!moqr_cli_args_parse(argc, argv, &args)) {
        fprintf(stderr, "MOQ5 Relay: %s\n", args.error);
        moqr_cli_print_try_help(stderr);
        return 2;
    }
    /* Informational commands are answered from argv alone — before the
     * verify-seam environment, the configuration file, the capacity preflight
     * and any allocation. `--help` has to work on a host where none of those
     * are in order. */
    if (moqr_cli_cmd_is_informational(args.cmd)) {
        if (args.cmd == MOQR_CLI_CMD_VERSION) {
            moqr_cli_print_version(stdout);
        } else {
            moqr_cli_print_help(stdout, args.cmd);
        }
        return 0;
    }

    const char *cmd = args.cmd == MOQR_CLI_CMD_SERVE ? "serve" : "capacity";
    const char *path = args.config;

#ifdef MOQR_VERIFY_SEAM
    /* Blocked-scenario constraint seam: read + validate BEFORE the config,
     * so a junk value refuses here rather than serving unconstrained (the
     * scenario's acceptance would then fail by timeout, not by diagnosis). */
    {
        char verr[192];
        if (moqr_cli_verify_env_load(verr, sizeof(verr)) != MOQR_OK) {
            fprintf(stderr, MOQR_SEAM_NAME ": %s\n", verr);
            return 2;
        }
    }
#endif

    moqr_cli_config_t cfg;
    char err[128];
    if (moqr_cli_config_load(path, &cfg, err, sizeof(err)) != MOQR_OK) {
        fprintf(stderr, "config error: %s\n", err);
        return 2;
    }

#ifndef MOQR_DUAL_LISTENER
    /*
     * What this build can serve, decided once for every command.
     *
     * A binary compiled without WebTransport support cannot run a configured
     * `webtransport` object, and that is a property of the build rather than of
     * one subcommand. Deciding it per command is how an operator who validates
     * with `capacity` gets a clean answer -- and a ceiling counting lanes this
     * binary will never open -- for a configuration `serve` refuses. Refused
     * here, after the configuration is understood and before any ceiling is
     * computed, any readiness record is written, or any runtime exists.
     */
    if (moqr_cli_config_has_webtransport(&cfg)) {
        fprintf(stderr,
                "config error: this build has no WebTransport listener "
                "support; a configured webtransport object cannot be served\n");
        return 2;
    }
#endif

#ifdef MOQR_VERIFY_SEAM
    /* The verify and measure builds print seam and RELAY_BLOCKED_V0 rows
     * straight to stdout; a JSON-only stdout cannot be promised there, so a
     * serve refuses it before the preflight and before any resource exists.
     * The capacity subcommand serves no events and is unaffected. */
    if (strcmp(cmd, "serve") == 0 &&
        moqr_cli_verify_refuse_json_logging(&cfg, err, sizeof(err)) !=
            MOQR_OK) {
        fprintf(stderr, MOQR_SEAM_NAME ": %s\n", err);
        return 2;
    }
    /* The seam lives in the multi-lane path (the shard-config builder and,
     * in the verify build, the lanes coordinator that emits
     * RELAY_BLOCKED_V0). A constrained single-lane serve would silently
     * ignore the bind override — refuse. */
    if (moqr_cli_total_lanes(&cfg) <= 1 &&
        (moqr_cli_verify_bind_max_sgs() != 0 ||
                           moqr_cli_verify_session_max_sgs() != 0)) {
        fprintf(stderr, MOQR_SEAM_NAME ": MOQR_VERIFY_*_MAX_OPEN_SUBGROUPS "
                        "requires a multi-shard relay: "
                        "listener.lanes + webtransport.lanes > 1\n");
        return 2;
    }
#endif

    /* Full capacity preflight ahead of EVERY command: semantic resolution
     * AND ceiling arithmetic both complete here, so an invalid or
     * undescribable config refuses identically for capacity and both serve
     * paths — before any runtime allocation, listener start, or "listening"
     * banner. The in-command print re-renders the same pure function. */
    {
        moqr_cli_capacity_t pre;
        size_t ctxb = moqr_cli_total_lanes(&cfg) > 1
                          ? sizeof(serve_lanes_ctx_t)
                          : 0;
        if (moqr_cli_describe_capacity(&cfg, moq_alloc_default(), ctxb,
                                       &pre) != MOQR_OK) {
            fprintf(stderr,
                    "config refused: cross-field validation or ceiling "
                    "arithmetic failed (e.g. demand_channel_bytes below one "
                    "resolved log record, or an overflowing budget)\n");
            return 2;
        }
    }

    if (strcmp(cmd, "capacity") == 0) {
        /* Valid at every lane count: lanes>1 uses the SAME shard-config
         * builder serve consumes, so the printed ceiling is the config the
         * process would actually run. */
        return print_capacity(&cfg,
                              moqr_cli_total_lanes(&cfg) > 1
                                  ? sizeof(serve_lanes_ctx_t)
                                  : 0,
                              stdout) != 0
                   ? 2
                   : 0;
    }
    /* The composition follows the shards the process will allocate, across
     * both listeners: the single-facade path owns exactly one. */
    return moqr_cli_total_lanes(&cfg) > 1 ? cmd_serve_lanes(&cfg)
                                          : cmd_serve(&cfg);
}

#ifdef MOQR_PUMP_TESTING
/* Test-only accessors: hand the REAL production lane pumps and their context
 * builders to a deterministic test, and drive the REAL signal handler. The
 * test links this translation unit with main renamed away; nothing here is
 * compiled into a shipped binary. */
moq_msquic_lane_pump_fn moqr_test_single_pump(void) { return relay_lane_pump; }
moq_msquic_lane_pump_fn moqr_test_lanes_pump(void) { return relay_lanes_pump; }
#ifdef MOQR_DUAL_LISTENER
/* The WebTransport lane pump, so a test can drive BOTH facades of the real
 * dual composition against one shard runtime. */
moq_wtquic_msquic_lane_pump_fn moqr_test_wt_lanes_pump(void)
{
    return relay_wt_lanes_pump;
}

/* Plan the context the way a dual config resolves: raw lanes first, then the
 * WebTransport range. The test drives the shipping pumps, so the mapping it
 * exercises is the production one. */
void moqr_test_lanes_ctx_plan_dual(void *vctx, uint32_t raw_lanes,
                                   uint32_t wt_lanes)
{
    serve_lanes_ctx_t *ctx = vctx;

    if (ctx == NULL) {
        return;
    }
    ctx->plan.raw_first = 0;
    ctx->plan.raw_count = raw_lanes;
    ctx->plan.wt_first = raw_lanes;
    ctx->plan.wt_count = wt_lanes;
    ctx->plan.total_shards = raw_lanes + wt_lanes;
    ctx->lanes = raw_lanes + wt_lanes;
}

/* The raw facade handle, so a WebTransport lane's cross-shard push can wake a
 * raw lane exactly as it does in production. */
void moqr_test_lanes_ctx_set_raw(void *vctx, moq_msquic_managed_t *raw)
{
    serve_lanes_ctx_t *ctx = vctx;

    if (ctx != NULL) {
        ctx->raw = raw;
    }
}
#endif

/* The serve functions themselves, and the sink's I/O seam they hand to
 * moqr_cli_log_init in this build, so a test can drive a REAL serve start
 * against the fake facade, inject a deterministic post-initialisation
 * refusal, and observe the sink's finalization. */
void moqr_test_serve_log_set_io(const moqr_cli_log_io_t *io)
{
    g_test_serve_log_io = io;
}

int moqr_test_cmd_serve(const moqr_cli_config_t *cfg) { return cmd_serve(cfg); }
uint8_t moqr_test_serve_log_last_state(void) { return g_test_serve_log_last_state; }
/* A value no sink state uses, so a previous serve's CLOSED can never satisfy
 * the next observation. */
void moqr_test_serve_log_reset_last_state(void) { g_test_serve_log_last_state = 0xffu; }
int moqr_test_cmd_serve_lanes(const moqr_cli_config_t *cfg)
{
    return cmd_serve_lanes(cfg);
}

int moqr_test_serve_print_capacity(const moqr_cli_config_t *cfg, FILE *out)
{
    return print_capacity(cfg,
                          moqr_cli_total_lanes(cfg) > 1
                              ? sizeof(serve_lanes_ctx_t)
                              : 0,
                          out);
}

void *moqr_test_mk_serve_ctx(moqr_bind_t *bind, moqr_core_t *core,
                             moqr_trace_t *trace)
{
    serve_ctx_t *ctx = calloc(1, sizeof(*ctx));

    if (ctx != NULL) {
        ctx->bind = bind;
        ctx->core = core;
        ctx->trace = trace;
    }
    return ctx;
}

void *moqr_test_mk_lanes_ctx(moqr_shards_t *shards, uint32_t lanes)
{
    serve_lanes_ctx_t *ctx = calloc(1, sizeof(*ctx));

    if (ctx != NULL) {
        ctx->shards = shards;
        ctx->lanes = lanes;
        /* the raw-only plan: every lane is its own shard, no WebTransport
         * range, exactly what a listener-only config resolves to */
        ctx->plan.raw_first = 0;
        ctx->plan.raw_count = lanes;
        ctx->plan.wt_first = lanes;
        ctx->plan.wt_count = 0;
        ctx->plan.total_shards = lanes;
    }
    return ctx;
}

uint64_t moqr_test_lanes_ctx_pump_turns(void *vctx, uint32_t lane)
{
    serve_lanes_ctx_t *ctx = vctx;

    return lane < MOQR_CLI_MAX_LANES ? ctx->pump_turns[lane] : 0;
}

uint64_t moqr_test_lanes_ctx_wake_pushes(void *vctx, uint32_t lane)
{
    serve_lanes_ctx_t *ctx = vctx;

    return lane < MOQR_CLI_MAX_LANES ? ctx->wake_pushes[lane] : 0;
}

void moqr_test_raise_stop(void) { on_signal(SIGTERM); }

/* The two production coordinators, reachable so their generation lifecycle
 * can be driven directly: produce, freeze, take the token, dispatch by frozen
 * demand, release. The verify coordinator is only compiled into the verify
 * build, so its seam follows the same guard. */
moqr_broker_t *moqr_test_metrics_broker(void) { return &g_metrics_broker; }

void
moqr_test_set_coord_midflight(void (*fn)(void *), void *ctx)
{
    g_coord_midflight = fn;
    g_coord_midflight_ctx = ctx;
}

void
moqr_test_set_coord_pinned(void (*fn)(void *), void *ctx)
{
    g_coord_pinned = fn;
    g_coord_pinned_ctx = ctx;
}

/* Make the coordinator's own authenticated release refuse, WITHOUT touching
 * the pinned slot: the bank index it passes is corrupted, so the broker
 * rejects the token and the generation stays SENDING. Releasing the slot from
 * the test instead would retire the generation and prove the wrong thing --
 * the branch under test is a refused release, not an already-finished one.
 * Under the single-owner contract this branch is otherwise unreachable, which
 * is exactly why it is checked. */
void
moqr_test_set_release_bank_corruption(bool on)
{
    g_corrupt_release_bank = on;
}

/*
 * The endpoint's actual callback wiring, compared IN PLACE.
 *
 * Each member is checked against its exact production callback here, where
 * both have their real function-pointer types. Handing the pointers out as
 * object pointers would erase those types -- a conversion C does not define
 * for function pointers -- and would let a wrong-but-non-NULL seam pass as
 * "installed". The result is a bitmask of what is correctly wired.
 */
/*
 * The owner's /api/v1/info lifetime, driven through the PRODUCTION
 * admin_owner_init: an enabled configuration renders the document before any
 * endpoint exists; a disabled one allocates none of it; a configuration whose
 * strings cannot be represented refuses -- which is what keeps the readiness
 * line from ever being printed over an endpoint that cannot serve its target.
 */
moqr_result_t
moqr_test_admin_owner_probe(const moqr_cli_config_t *cfg, char *out,
                            size_t cap, size_t *out_len, bool *out_has_info)
{
    admin_owner_t o;
    moqr_cli_snapshot_t snap;
    moqr_obs_labels_t labels = { .shard = 0, .transport = "msquic",
                                 .version = "moqt-18" };
    moqr_result_t rc;

    if (out_len != NULL) {
        *out_len = 0;
    }
    if (out_has_info != NULL) {
        *out_has_info = false;
    }
    memset(&snap, 0, sizeof(snap));
    if (moqr_cli_snapshot_init(&snap, 1u, moq_alloc_default()) != MOQR_OK) {
        return MOQR_ERR_NOMEM;
    }
    rc = admin_owner_init(&o, cfg, &snap, &labels, 1u, 0u, NULL, NULL);
    if (out_has_info != NULL) {
        *out_has_info = (o.info != NULL);
    }
    if (rc == MOQR_OK && o.info != NULL && out != NULL && cap > o.info_len) {
        memcpy(out, o.info, o.info_len);
        out[o.info_len] = '\0';
        if (out_len != NULL) {
            *out_len = o.info_len;
        }
    }
    admin_owner_destroy(&o);
    moqr_cli_snapshot_destroy(&snap);
    return rc;
}

uint32_t
moqr_test_admin_seam_wiring(int *available)
{
    moqr_admin_listen_cfg_t lcfg;
    moqr_cli_config_t cfg;
    admin_owner_t o;
    uint32_t ok = 0;

    static const char sentinel[] = "{\"seam\":true}";

    memset(&cfg, 0, sizeof(cfg));
    memset(&o, 0, sizeof(o));
    o.info = (char *)sentinel;
    o.info_len = sizeof(sentinel) - 1u;
    /* The production wiring function, not a copy of it. */
    admin_fill_listen_cfg(&lcfg, &cfg, &o);
    ok |= lcfg.wake_all == admin_wake_all ? (1u << 0) : 0u;
    ok |= lcfg.collect == admin_collect ? (1u << 1) : 0u;
    ok |= lcfg.render == admin_render ? (1u << 2) : 0u;
    ok |= lcfg.signal_pending == admin_signal_pending ? (1u << 3) : 0u;
    ok |= lcfg.emit_signal == admin_emit_signal ? (1u << 4) : 0u;
    ok |= lcfg.emit_suppressed == admin_emit_suppressed ? (1u << 5) : 0u;
    /* The document handoff: the exact owner bytes and length, not a copy. */
    ok |= lcfg.info == o.info ? (1u << 6) : 0u;
    ok |= lcfg.info_len == o.info_len ? (1u << 7) : 0u;
    if (available != NULL) {
        *available = MOQR_ADMIN_ENDPOINT_AVAILABLE;
    }
    return ok;
}

/*
 * The RESOLVED dual-facade plan, built by production code.
 *
 * The endpoint is handed a global shard count and one label set per shard. A
 * test that constructed those itself could not notice production passing the
 * raw lane count instead of the total, or building either transport's range
 * wrongly -- so this seam runs the same resolution the multi-lane serve path
 * runs and reports what the owner would actually receive.
 */
uint32_t
moqr_test_admin_resolved_labels(const moqr_cli_config_t *cfg,
                                moqr_obs_labels_t *out, uint32_t cap)
{
    moqr_cli_shard_plan_t plan;
    char perr[192] = { 0 };
    moqr_obs_labels_t labels[MOQR_CLI_MAX_LANES];
    uint32_t n;

    if (cfg == NULL || out == NULL) {
        return 0;
    }
    if (moqr_cli_shard_plan(cfg, &plan, perr, sizeof(perr)) != MOQR_OK) {
        return 0;
    }
    memset(labels, 0, sizeof(labels));
    /* The production resolution, and the production label construction. */
    serve_lanes_fill_labels(labels, cfg, &plan);
    /* And the production count the endpoint is handed. */
    n = plan.total_shards;
    if (n > cap) {
        return 0;   /* refuse rather than truncate a coordinate set */
    }
    for (uint32_t i = 0; i < n; i++) {
        out[i] = labels[i];
    }
    return n;
}

#ifndef MOQR_BIND_TESTING
/*
 * The signal-compatibility seam.
 *
 * The endpoint's owner renders the SIGUSR1 document through a different entry
 * point than the coordinator it replaces. "Different entry point, same bytes"
 * is a claim, and a claim about a shipped surface has to be checked -- so both
 * renderings of the SAME copied rows are exposed here and compared byte for
 * byte, banner included.
 */
moqr_result_t
moqr_test_admin_render_signal(moqr_cli_snapshot_t *snap,
                              const moqr_obs_labels_t *labels, uint32_t lanes,
                              moqr_cli_snapshot_stats_t *rows,
                              moqr_snapshot_view_t *views, uint64_t serial)
{
    admin_owner_t o;
    char *bodies[MOQR_ADMIN_BODY__COUNT];
    size_t caps[MOQR_ADMIN_BODY__COUNT];
    size_t len[MOQR_ADMIN_BODY__COUNT];
    char *scratch[MOQR_ADMIN_BODY__COUNT];
    moqr_result_t rc;
    uint64_t bound = 0;

    memset(&o, 0, sizeof(o));
    o.snap = snap;
    o.labels = labels;
    o.lanes = lanes;
    o.rows = rows;
    o.views = views;
    for (uint32_t k = 0; k < MOQR_ADMIN_BODY__COUNT; k++) {
        if (k == MOQR_ADMIN_BODY_SHARDS) {
            bound = moqr_cli_shards_bound(lanes);
            if (bound == UINT64_MAX) {
                return MOQR_ERR_CAPACITY;
            }
        } else if (moqr_metrics_bound(lanes, (moqr_obs_format_t)k, true, true,
                                      &bound) != MOQR_OK) {
            return MOQR_ERR_CAPACITY;
        }
        caps[k] = (size_t)bound + 1u;
        scratch[k] = malloc(caps[k]);
        bodies[k] = scratch[k];
        len[k] = 0;
        if (scratch[k] == NULL) {
            for (uint32_t j = 0; j < k; j++) {
                free(scratch[j]);
            }
            return MOQR_ERR_NOMEM;
        }
    }
    /* The owner's signal-sink body, exactly as admin_owner_init sizes it. */
    o.signal_cap = caps[MOQR_OBS_FMT_PROMETHEUS_004];
    o.signal_body = malloc(o.signal_cap);
    if (o.signal_body == NULL) {
        for (uint32_t k = 0; k < MOQR_ADMIN_BODY__COUNT; k++) {
            free(scratch[k]);
        }
        return MOQR_ERR_NOMEM;
    }
    rc = admin_collect(&o, serial);
    if (rc == MOQR_OK) {
        rc = admin_render(&o, serial, bodies, caps, len);
    }
    if (rc != MOQR_OK && rc != MOQR_ERR_WOULD_BLOCK) {
        /* Exactly what the owner does with a poisoned generation: it tells the
         * signal sink, through the real callback. The decision to call it comes
         * from the production collect, not from the test. */
        admin_emit_suppressed(&o, serial);
    }
    if (rc == MOQR_OK) {
        /*
         * Exactly what the owner does with a rendered generation: it hands the
         * Prometheus body to the signal callback. Both the SELECTION and the
         * FORMATTING are production code here, so changing either is visible
         * to the comparison.
         */
        admin_emit_signal(&o, serial, bodies[MOQR_OBS_FMT_PROMETHEUS_004],
                          len[MOQR_OBS_FMT_PROMETHEUS_004]);
    }
    for (uint32_t k = 0; k < MOQR_ADMIN_BODY__COUNT; k++) {
        free(scratch[k]);
    }
    free(o.signal_body);
    return rc;
}

/*
 * The shards document from REAL copied rows, through the production collect
 * and render: what the bank's JSON slot would hold for `serial`. A refused
 * generation (a REFUSED row) renders nothing, exactly as the owner serves
 * nothing for it.
 */
moqr_result_t
moqr_test_admin_render_shards(moqr_cli_snapshot_t *snap,
                              const moqr_obs_labels_t *labels, uint32_t lanes,
                              moqr_cli_snapshot_stats_t *rows,
                              moqr_snapshot_view_t *views, uint64_t serial,
                              char *out, size_t cap, size_t *out_len)
{
    admin_owner_t o;
    char *bodies[MOQR_ADMIN_BODY__COUNT];
    size_t caps[MOQR_ADMIN_BODY__COUNT];
    size_t len[MOQR_ADMIN_BODY__COUNT];
    moqr_result_t rc;
    uint64_t bound = 0;

    if (out_len != NULL) {
        *out_len = 0;
    }
    if (out != NULL && cap > 0u) {
        out[0] = '\0';
    }
    memset(&o, 0, sizeof(o));
    o.snap = snap;
    o.labels = labels;
    o.lanes = lanes;
    o.rows = rows;
    o.views = views;
    for (uint32_t k = 0; k < MOQR_ADMIN_BODY__COUNT; k++) {
        if (k == MOQR_ADMIN_BODY_SHARDS) {
            bound = moqr_cli_shards_bound(lanes);
            if (bound == UINT64_MAX) {
                return MOQR_ERR_CAPACITY;
            }
        } else if (moqr_metrics_bound(lanes, (moqr_obs_format_t)k, true, true,
                                      &bound) != MOQR_OK) {
            return MOQR_ERR_CAPACITY;
        }
        caps[k] = (size_t)bound + 1u;
        bodies[k] = malloc(caps[k]);
        len[k] = 0;
        if (bodies[k] == NULL) {
            for (uint32_t j = 0; j < k; j++) {
                free(bodies[j]);
            }
            return MOQR_ERR_NOMEM;
        }
    }
    rc = admin_collect(&o, serial);
    if (rc == MOQR_OK) {
        rc = admin_render(&o, serial, bodies, caps, len);
    }
    if (rc == MOQR_OK && out != NULL && cap > len[MOQR_ADMIN_BODY_SHARDS]) {
        memcpy(out, bodies[MOQR_ADMIN_BODY_SHARDS], len[MOQR_ADMIN_BODY_SHARDS]);
        out[len[MOQR_ADMIN_BODY_SHARDS]] = '\0';
        if (out_len != NULL) {
            *out_len = len[MOQR_ADMIN_BODY_SHARDS];
        }
    }
    for (uint32_t k = 0; k < MOQR_ADMIN_BODY__COUNT; k++) {
        free(bodies[k]);
    }
    return rc;
}

uint64_t
moqr_test_coord_try_dump(const moqr_obs_labels_t *labels,
                         moqr_cli_snapshot_t *snap, uint64_t rendered,
                         uint64_t *incomplete_told, uint64_t *fail_told)
{
    bool wake = false;
    return coord_try_dump_labels(labels, snap, rendered, incomplete_told,
                                 fail_told, &wake);
}

uint64_t
moqr_test_coord_try_dump_wake(const moqr_obs_labels_t *labels,
                              moqr_cli_snapshot_t *snap, uint64_t rendered,
                              uint64_t *inc, uint64_t *fail, bool *wake)
{
    return coord_try_dump_labels(labels, snap, rendered, inc, fail, wake);
}
#else
/* The blocked coordinator needs only a lane count and labels from its serve
 * context, so the seam builds a minimal one rather than exporting the type. */
uint64_t
moqr_test_coord_try_dump_blocked(const moqr_obs_labels_t *labels,
                                 uint32_t lanes, moqr_cli_snapshot_t *snap,
                                 uint64_t rendered, uint64_t *incomplete_told,
                                 uint64_t *fail_told)
{
    serve_lanes_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.lanes = lanes;
    for (uint32_t i = 0; i < lanes && i < MOQR_CLI_MAX_LANES; i++) {
        ctx.labels[i] = labels[i];
    }
    bool wake = false;
    return coord_try_dump_blocked(&ctx, snap, rendered, incomplete_told,
                                  fail_told, &wake);
}

uint64_t
moqr_test_coord_try_dump_blocked_wake(const moqr_obs_labels_t *labels,
                                      uint32_t lanes,
                                      moqr_cli_snapshot_t *snap,
                                      uint64_t rendered, uint64_t *inc,
                                      uint64_t *fail, bool *wake)
{
    serve_lanes_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.lanes = lanes;
    for (uint32_t i = 0; i < lanes && i < MOQR_CLI_MAX_LANES; i++) {
        ctx.labels[i] = labels[i];
    }
    return coord_try_dump_blocked(&ctx, snap, rendered, inc, fail, wake);
}
#endif

void moqr_test_clear_stop(void) { atomic_store(&g_stop, 0); }
#endif /* MOQR_PUMP_TESTING */
