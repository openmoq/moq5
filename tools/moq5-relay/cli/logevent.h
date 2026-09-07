/*
 * The serve's stdout sink and its structured events.
 *
 * TEXT is the accepted default: today's bytes, streams and flush points,
 * unchanged, formatted by the existing text formatters under their existing
 * per-family buffer limits. JSON makes the serve's stdout a stream of one
 * event per line -- rendered whole through the validating JSON writer, then
 * submitted with ONE fwrite and ONE checked fflush -- and moves the capacity
 * prose to stderr. Everything else on stderr is unchanged in both modes.
 *
 * THE JSON STREAM LATCHES ON ITS FIRST FAILURE. A short write, a write error
 * or a failed checked flush moves the sink to FAILED: nothing is submitted
 * to that stream again, no clearerr, no retry, at most one stderr
 * diagnostic. The bytes already handed to the stream before the failure --
 * possibly a partial line -- are a documented I/O-failure outcome, never a
 * JSON event. A submitted-and-flushed record is not a durable one: the
 * relay never claims what a reader received.
 *
 * ELAPSED TIME IS NULLABLE. The sink samples the monotonic clock once at
 * init as the serve's baseline and once per event; a failed read, or a
 * sample that regresses against the baseline or the last accepted sample,
 * renders `null` (never a fabricated number), keeps the baseline and the
 * last accepted sample, and diagnoses once. Text mode samples no clock.
 *
 * Every formatter here is pure: it takes the values it renders (elapsed
 * validity and value included) and reads no clock, no global and no I/O.
 */
#ifndef MOQR_CLI_LOGEVENT_H
#define MOQR_CLI_LOGEVENT_H

#include "config.h"
#include "lanestats.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>

/* The one line buffer: a C constant. Every event variant's maximum, measured
 * at init over an immutable synthetic maximum, must fit below CAP - 2 (LF
 * and NUL); init refuses otherwise. */
#define MOQR_CLI_LOG_LINE_CAP 6144u

/* sink states */
#define MOQR_CLI_LOG_SINK_OPEN    1u
#define MOQR_CLI_LOG_SINK_FAILED  2u
#define MOQR_CLI_LOG_SINK_CLOSED  3u
/* clock states */
#define MOQR_CLI_LOG_CLOCK_OK           1u
#define MOQR_CLI_LOG_CLOCK_UNAVAILABLE  2u
/* the event families that carry a once-only render diagnostic */
#define MOQR_CLI_LOG_FAMILY_READY       (1u << 0)
#define MOQR_CLI_LOG_FAMILY_RUN_CONFIG  (1u << 1)
#define MOQR_CLI_LOG_FAMILY_LANE        (1u << 2)
#define MOQR_CLI_LOG_FAMILY_PAIR        (1u << 3)
#define MOQR_CLI_LOG_FAMILY_STOP        (1u << 4)

/* TEST-ONLY I/O seam: the real sink uses clock_gettime, fwrite and fflush.
 * A test supplies scripted versions to inject failures deterministically and
 * to count the sink's own calls. NULL selects the real functions. */
typedef struct moqr_cli_log_io {
    bool   (*clock_us)(void *ctx, uint64_t *out_us);
    size_t (*write)(void *ctx, FILE *f, const char *p, size_t n);
    int    (*flush)(void *ctx, FILE *f);
    void    *ctx;
} moqr_cli_log_io_t;

typedef struct moqr_cli_log {
    moqr_cli_log_format_t     format;
    FILE                     *rows;     /* stdout                          */
    FILE                     *prose;    /* stdout (text) | stderr (json)   */
    const moqr_cli_log_io_t  *io;
    uint64_t                  base_us;
    uint64_t                  last_us;  /* last ACCEPTED sample            */
    uint8_t                   clock_state;
    bool                      clock_told;
    uint8_t                   sink_state;
    bool                      sink_told;
    uint8_t                   render_told;   /* family bits               */
    /* counters a test reads; production ignores them */
    uint32_t                  writes;   /* sink-level fwrite attempts      */
    uint32_t                  flushes;  /* sink-level fflush attempts      */
    uint32_t                  dropped;  /* JSON events not submitted       */
    char                      line[MOQR_CLI_LOG_LINE_CAP];
} moqr_cli_log_t;

/* Which serve composition is speaking; the text lines differ. */
#define MOQR_CLI_LOG_COMP_K1     1u
#define MOQR_CLI_LOG_COMP_LANES  2u

/* Readiness: one per listener kind. Strings are borrowed pointer+length. */
#define MOQR_CLI_LOG_READY_RAW          1u
#define MOQR_CLI_LOG_READY_WEBTRANSPORT 2u
#define MOQR_CLI_LOG_READY_ADMIN        3u
#define MOQR_CLI_LOG_READY_SIGNALS      4u

typedef struct moqr_cli_log_ready {
    uint32_t    kind;
    uint32_t    composition;
    const char *host;    size_t host_n;
    int         port;
    const char *alpn_set; size_t alpn_n;
    uint32_t    lanes;
    const char *path;    size_t path_n;      /* webtransport */
    const char *profile; size_t profile_n;   /* webtransport */
} moqr_cli_log_ready_t;

/* Stop: per-shard (K>1), the K=1 line, or the total. */
typedef struct moqr_cli_log_stop {
    bool     total;
    bool     per_shard;          /* K>1 per-shard row (carries ctx counters) */
    bool     poisoned;           /* per-shard: shard stats refused           */
    bool     have_wake_requests; /* per-shard: shard stats valid              */
    uint32_t shard;
    uint32_t shards;             /* total: how many                           */
    uint32_t conns, tracks;
    uint64_t ingested, delivered, session_errors;
    uint64_t pump_turns, wakes;  /* serve-context counters (per_shard only)   */
    uint64_t wr_push, wr_credit, wr_local;
} moqr_cli_log_stop_t;

/* -- pure formatters -------------------------------------------------------- */
int moqr_cli_log_ready_text(char *buf, size_t cap, const moqr_cli_log_ready_t *v);
int moqr_cli_log_stop_text(char *buf, size_t cap, const moqr_cli_log_stop_t *v);
moqr_result_t moqr_cli_log_ready_json(char *buf, size_t cap, bool elapsed_valid,
                                      uint64_t elapsed_us,
                                      const moqr_cli_log_ready_t *v,
                                      size_t *out_len);
moqr_result_t moqr_cli_log_stop_json(char *buf, size_t cap, bool elapsed_valid,
                                     uint64_t elapsed_us,
                                     const moqr_cli_log_stop_t *v,
                                     size_t *out_len);
/* The largest body any variant can produce (excluding LF/NUL), measured over
 * an immutable synthetic maximum. UINT64_MAX if the measurement refused. */
uint64_t moqr_cli_log_line_bound(void);

/* -- the sink ----------------------------------------------------------------- */
moqr_result_t moqr_cli_log_init(moqr_cli_log_t *log, moqr_cli_log_format_t fmt,
                                FILE *rows, FILE *prose,
                                const moqr_cli_log_io_t *io);
void moqr_cli_log_ready(moqr_cli_log_t *log, const moqr_cli_log_ready_t *v);
void moqr_cli_log_run_config(moqr_cli_log_t *log,
                             const moqr_cli_run_config_row_t *row);
void moqr_cli_log_run_config_refused(moqr_cli_log_t *log, const char *which);
void moqr_cli_log_lane(moqr_cli_log_t *log, const moqr_cli_lane_stats_row_t *row);
void moqr_cli_log_lane_refused(moqr_cli_log_t *log, uint32_t lane,
                               const char *which);
void moqr_cli_log_pair(moqr_cli_log_t *log, const moqr_cli_pair_stats_row_t *row);
void moqr_cli_log_pair_refused(moqr_cli_log_t *log, uint32_t src, uint32_t dst,
                               const char *which);
void moqr_cli_log_stop(moqr_cli_log_t *log, const moqr_cli_log_stop_t *v);
/* Text mode's readiness flush point (today's fflush(stdout)); JSON flushes
 * per event and does nothing here. */
void moqr_cli_log_readiness_flush(moqr_cli_log_t *log);
/* The stream a human-prose block (the capacity print) goes to. */
FILE *moqr_cli_log_prose_stream(const moqr_cli_log_t *log);
/* The ordinary-return finalization. JSON: the checked final flush, then
 * CLOSED (a failed flush latches FAILED with one diagnostic). Text: no I/O
 * at all -- the state closes, and that is the whole effect. After it every
 * entry point is inert: repeated calls, and emits after it, touch no buffer,
 * no stream and no clock. */
void moqr_cli_log_finish(moqr_cli_log_t *log);

#endif /* MOQR_CLI_LOGEVENT_H */
