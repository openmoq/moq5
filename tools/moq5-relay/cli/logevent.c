#include "logevent.h"

#include "admin_targets.h"

#include "../admin/json_out.h"

#include <errno.h>
#include <inttypes.h>
#include <string.h>
#include <time.h>

/* -- the closed vocabularies ---------------------------------------------------- */

static const char *
listener_word(uint32_t kind)
{
    switch (kind) {
    case MOQR_CLI_LOG_READY_RAW:          return "raw";
    case MOQR_CLI_LOG_READY_WEBTRANSPORT: return "webtransport";
    case MOQR_CLI_LOG_READY_ADMIN:        return "admin";
    case MOQR_CLI_LOG_READY_SIGNALS:      return "signals";
    default:                              return NULL;
    }
}

/* -- text: the accepted bytes, unchanged -------------------------------------- */

int
moqr_cli_log_ready_text(char *buf, size_t cap, const moqr_cli_log_ready_t *v)
{
    if (buf == NULL || v == NULL) {
        return -1;
    }
    switch (v->kind) {
    case MOQR_CLI_LOG_READY_RAW:
        if (v->composition == MOQR_CLI_LOG_COMP_K1) {
            return snprintf(buf, cap, "MOQ5 Relay: listening on %.*s:%d (%.*s)",
                            (int)v->host_n, v->host, v->port, (int)v->alpn_n,
                            v->alpn_set);
        }
        return snprintf(buf, cap,
                        "MOQ5 Relay: listening on %.*s:%d (%.*s), %u lanes",
                        (int)v->host_n, v->host, v->port, (int)v->alpn_n,
                        v->alpn_set, v->lanes);
    case MOQR_CLI_LOG_READY_ADMIN:
        return snprintf(buf, cap,
                        "MOQ5 Relay: admin endpoint on %.*s:%d (GET /metrics)",
                        (int)v->host_n, v->host, v->port);
    case MOQR_CLI_LOG_READY_WEBTRANSPORT:
        if (v->host == NULL || v->path == NULL || v->alpn_set == NULL ||
            v->profile == NULL) {
            return -1;
        }
        return snprintf(buf, cap,
                        "MOQ5 Relay: WebTransport on %.*s:%d%.*s (%.*s, profile "
                        "%.*s), %u lanes",
                        (int)v->host_n, v->host, v->port, (int)v->path_n,
                        v->path, (int)v->alpn_n, v->alpn_set,
                        (int)v->profile_n, v->profile, v->lanes);
    case MOQR_CLI_LOG_READY_SIGNALS:
        return snprintf(buf, cap,
                        v->composition == MOQR_CLI_LOG_COMP_K1
                            ? "signals: SIGUSR1 -> metrics + route dump, "
                              "SIGUSR2 -> trace JSONL (both to stderr)"
                            : "signals: SIGUSR1 -> metrics + route dump, "
                              "SIGUSR2 -> trace JSONL (per shard, to stderr)");
    default:
        return -1;
    }
}

int
moqr_cli_log_stop_text(char *buf, size_t cap, const moqr_cli_log_stop_t *v)
{
    if (buf == NULL || v == NULL) {
        return -1;
    }
    if (v->total) {
        return snprintf(buf, cap,
                        "MOQ5 Relay: stopping — total conns %u, tracks %u, "
                        "ingested %" PRIu64 ", delivered %" PRIu64
                        ", session errors %" PRIu64,
                        v->conns, v->tracks, v->ingested, v->delivered,
                        v->session_errors);
    }
    if (!v->per_shard) {
        return snprintf(buf, cap,
                        "MOQ5 Relay: stopping — conns %u, tracks %u, ingested %"
                        PRIu64 ", delivered %" PRIu64 ", session errors %" PRIu64,
                        v->conns, v->tracks, v->ingested, v->delivered,
                        v->session_errors);
    }
    if (v->have_wake_requests) {
        return snprintf(buf, cap,
                        "MOQ5 Relay: shard %u — conns %u, tracks %u, ingested %"
                        PRIu64 ", delivered %" PRIu64 ", session errors %" PRIu64
                        ", pump turns %" PRIu64 ", wakes %" PRIu64
                        " (requests: push %" PRIu64 ", credit %" PRIu64
                        ", local %" PRIu64 ")",
                        v->shard, v->conns, v->tracks, v->ingested,
                        v->delivered, v->session_errors, v->pump_turns,
                        v->wakes, v->wr_push, v->wr_credit, v->wr_local);
    }
    return snprintf(buf, cap,
                    "MOQ5 Relay: shard %u — conns %u, tracks %u, ingested %"
                    PRIu64 ", delivered %" PRIu64 ", session errors %" PRIu64
                    ", pump turns %" PRIu64 ", wakes %" PRIu64
                    " (shard stats refused: poisoned snapshot)",
                    v->shard, v->conns, v->tracks, v->ingested, v->delivered,
                    v->session_errors, v->pump_turns, v->wakes);
}

/* -- JSON ------------------------------------------------------------------------ */

static void
envelope(moqr_json_w_t *w, const char *schema, bool elapsed_valid,
         uint64_t elapsed_us)
{
    moqr_json_object_begin(w);
    moqr_json_key(w, "schema");
    moqr_json_str(w, schema, strlen(schema));
    moqr_json_key(w, "elapsed_us");
    if (elapsed_valid) {
        moqr_json_u64(w, elapsed_us);
    } else {
        moqr_json_null(w);
    }
}

static moqr_result_t
finish_doc(moqr_json_w_t *w, char *buf, size_t *out_len, bool strings)
{
    size_t len = 0;
    moqr_json_object_end(w);
    if (!moqr_json_end(w, &len)) {
        if (buf != NULL) {
            buf[0] = '\0';
        }
        if (out_len != NULL) {
            *out_len = 0;
        }
        /* With strings in play the writer may have refused a value; tell
         * the two apart by measuring the same document without storage. */
        if (strings) {
            return MOQR_ERR_CAPACITY;   /* refined by the caller */
        }
        return MOQR_ERR_CAPACITY;
    }
    if (out_len != NULL) {
        *out_len = len;
    }
    return MOQR_OK;
}

static void
write_ready(moqr_json_w_t *w, bool ev, uint64_t eus,
            const moqr_cli_log_ready_t *v, const char *word)
{
    envelope(w, "RELAY_READY_V1", ev, eus);
    moqr_json_key(w, "listener");
    moqr_json_str(w, word, strlen(word));
    switch (v->kind) {
    case MOQR_CLI_LOG_READY_RAW:
        moqr_json_key(w, "host");     moqr_json_str(w, v->host, v->host_n);
        moqr_json_key(w, "port");     moqr_json_i64(w, v->port);
        moqr_json_key(w, "alpn_set"); moqr_json_str(w, v->alpn_set, v->alpn_n);
        moqr_json_key(w, "lanes");
        moqr_json_u64(w, v->composition == MOQR_CLI_LOG_COMP_K1 ? 1u : v->lanes);
        break;
    case MOQR_CLI_LOG_READY_WEBTRANSPORT:
        moqr_json_key(w, "host");     moqr_json_str(w, v->host, v->host_n);
        moqr_json_key(w, "port");     moqr_json_i64(w, v->port);
        moqr_json_key(w, "path");     moqr_json_str(w, v->path, v->path_n);
        moqr_json_key(w, "alpn_set"); moqr_json_str(w, v->alpn_set, v->alpn_n);
        moqr_json_key(w, "profile");  moqr_json_str(w, v->profile, v->profile_n);
        moqr_json_key(w, "lanes");    moqr_json_u64(w, v->lanes);
        break;
    case MOQR_CLI_LOG_READY_ADMIN:
        moqr_json_key(w, "host");     moqr_json_str(w, v->host, v->host_n);
        moqr_json_key(w, "port");     moqr_json_i64(w, v->port);
        moqr_json_key(w, "targets");
        moqr_json_array_begin(w);
        for (size_t i = 0; i < MOQR_CLI_ADMIN_TARGET_COUNT; i++) {
            const char *t = moqr_cli_admin_target(i);
            moqr_json_str(w, t, strlen(t));
        }
        moqr_json_array_end(w);
        break;
    default: /* signals */
        moqr_json_key(w, "sigusr1"); moqr_json_str(w, "metrics+routes", 14);
        moqr_json_key(w, "sigusr2"); moqr_json_str(w, "trace", 5);
        moqr_json_key(w, "sink");    moqr_json_str(w, "stderr", 6);
        break;
    }
}

moqr_result_t
moqr_cli_log_ready_json(char *buf, size_t cap, bool elapsed_valid,
                        uint64_t elapsed_us, const moqr_cli_log_ready_t *v,
                        size_t *out_len)
{
    moqr_json_w_t w;
    const char *word;

    if (out_len != NULL) {
        *out_len = 0;
    }
    if (buf == NULL || cap == 0u || v == NULL) {
        return MOQR_ERR_INVAL;
    }
    buf[0] = '\0';
    word = listener_word(v->kind);
    if (word == NULL ||
        (v->kind != MOQR_CLI_LOG_READY_SIGNALS && v->host == NULL) ||
        ((v->kind == MOQR_CLI_LOG_READY_RAW ||
          v->kind == MOQR_CLI_LOG_READY_WEBTRANSPORT) && v->alpn_set == NULL) ||
        (v->kind == MOQR_CLI_LOG_READY_WEBTRANSPORT &&
         (v->path == NULL || v->profile == NULL))) {
        return MOQR_ERR_INVAL;
    }
    moqr_json_begin(&w, buf, cap);
    write_ready(&w, elapsed_valid, elapsed_us, v, word);
    if (finish_doc(&w, buf, out_len, true) != MOQR_OK) {
        /* A value that failed validation, or a body that did not fit. */
        moqr_json_w_t count;
        moqr_json_begin(&count, NULL, 0);
        write_ready(&count, elapsed_valid, elapsed_us, v, word);
        moqr_json_object_end(&count);
        return moqr_json_end(&count, NULL) ? MOQR_ERR_CAPACITY : MOQR_ERR_INVAL;
    }
    return MOQR_OK;
}

moqr_result_t
moqr_cli_log_stop_json(char *buf, size_t cap, bool elapsed_valid,
                       uint64_t elapsed_us, const moqr_cli_log_stop_t *v,
                       size_t *out_len)
{
    moqr_json_w_t w;

    if (out_len != NULL) {
        *out_len = 0;
    }
    if (buf == NULL || cap == 0u || v == NULL) {
        return MOQR_ERR_INVAL;
    }
    moqr_json_begin(&w, buf, cap);
    envelope(&w, "RELAY_STOP_V1", elapsed_valid, elapsed_us);
    if (v->total) {
        moqr_json_key(&w, "total");  moqr_json_bool(&w, true);
        moqr_json_key(&w, "shards"); moqr_json_u64(&w, v->shards);
    } else {
        moqr_json_key(&w, "shard");  moqr_json_u64(&w, v->shard);
        if (v->per_shard && v->poisoned) {
            moqr_json_key(&w, "refused"); moqr_json_str(&w, "poisoned", 8);
        }
    }
    moqr_json_key(&w, "conns");          moqr_json_u64(&w, v->conns);
    moqr_json_key(&w, "tracks");         moqr_json_u64(&w, v->tracks);
    moqr_json_key(&w, "ingested");       moqr_json_u64(&w, v->ingested);
    moqr_json_key(&w, "delivered");      moqr_json_u64(&w, v->delivered);
    moqr_json_key(&w, "session_errors"); moqr_json_u64(&w, v->session_errors);
    if (v->per_shard && !v->total) {
        /* The serve context's own counters: not the lane-stats row's
         * shard-plane pump_turns, a different object. */
        moqr_json_key(&w, "pump_turns"); moqr_json_u64(&w, v->pump_turns);
        moqr_json_key(&w, "wakes");      moqr_json_u64(&w, v->wakes);
        if (v->have_wake_requests) {
            moqr_json_key(&w, "wake_requests");
            moqr_json_object_begin(&w);
            moqr_json_key(&w, "push");   moqr_json_u64(&w, v->wr_push);
            moqr_json_key(&w, "credit"); moqr_json_u64(&w, v->wr_credit);
            moqr_json_key(&w, "local");  moqr_json_u64(&w, v->wr_local);
            moqr_json_object_end(&w);
        }
    }
    return finish_doc(&w, buf, out_len, false);
}

/* -- the bound: every variant over an immutable maximum ------------------------- */

#define C8 "\x1f\x1f\x1f\x1f\x1f\x1f\x1f\x1f"
#define C64 C8 C8 C8 C8 C8 C8 C8 C8
static const char k_ctl255[] = C64 C64 C64 C8 C8 C8 C8 C8 C8 C8 "\x1f\x1f\x1f\x1f\x1f\x1f\x1f";
static const char k_ctl67[] = C64 "\x1f\x1f\x1f";
static const char k_ctl63[] = C8 C8 C8 C8 C8 C8 C8 "\x1f\x1f\x1f\x1f\x1f\x1f\x1f";
static const char k_profile_max[] = MOQR_CLI_WT_PROFILE_NAME_MAX;
/* The width this record reserves for the profile is the width the
 * vocabulary can actually produce. Bound here rather than only at the
 * shared name, so a local literal that drifts shorter is a build error
 * instead of a bound that silently under-measures the widest record. */
_Static_assert(sizeof(k_profile_max) ==
                   sizeof(MOQR_CLI_WT_PROFILE_NAME_MAX),
               "the maximum-width record must measure itself against "
               "the longest profile name the vocabulary can produce");

uint64_t
moqr_cli_log_line_bound(void)
{
    moqr_json_w_t w;
    size_t len = 0, best = 0;
    moqr_cli_log_ready_t v;
    moqr_cli_log_stop_t s;
    moqr_cli_lane_stats_row_t lane;
    moqr_cli_pair_stats_row_t pair;
    moqr_cli_run_config_row_t rc;
    static const uint32_t kinds[4] = { MOQR_CLI_LOG_READY_RAW,
                                       MOQR_CLI_LOG_READY_WEBTRANSPORT,
                                       MOQR_CLI_LOG_READY_ADMIN,
                                       MOQR_CLI_LOG_READY_SIGNALS };

    memset(&v, 0, sizeof(v));
    v.composition = MOQR_CLI_LOG_COMP_LANES;
    v.host = k_ctl255; v.host_n = sizeof(k_ctl255) - 1u;
    v.port = INT32_MIN;
    v.alpn_set = k_ctl67; v.alpn_n = sizeof(k_ctl67) - 1u;
    v.lanes = UINT32_MAX;
    v.path = k_ctl255; v.path_n = sizeof(k_ctl255) - 1u;
    v.profile = k_profile_max; v.profile_n = sizeof(k_profile_max) - 1u;
    for (size_t k = 0; k < 4; k++) {
        v.kind = kinds[k];
        if (v.kind == MOQR_CLI_LOG_READY_ADMIN) {
            v.host = k_ctl63; v.host_n = sizeof(k_ctl63) - 1u;
        }
        moqr_json_begin(&w, NULL, 0);
        write_ready(&w, true, UINT64_MAX, &v, listener_word(v.kind));
        moqr_json_object_end(&w);
        if (!moqr_json_end(&w, &len)) {
            return UINT64_MAX;
        }
        best = len > best ? len : best;
        v.host = k_ctl255; v.host_n = sizeof(k_ctl255) - 1u;
    }
    /* the widest stop row: per-shard, healthy, every counter at its widest */
    memset(&s, 0xff, sizeof(s));
    s.total = false; s.per_shard = true; s.poisoned = false;
    s.have_wake_requests = true;
    {
        char tmp[1024];
        if (moqr_cli_log_stop_json(tmp, sizeof(tmp), true, UINT64_MAX, &s, &len) != MOQR_OK) {
            return UINT64_MAX;
        }
        best = len > best ? len : best;
    }
    memset(&lane, 0xff, sizeof(lane));
    memset(&pair, 0xff, sizeof(pair));
    memset(&rc, 0xff, sizeof(rc));
    {
        char tmp[4096];
        if (moqr_cli_lane_stats_json(tmp, sizeof(tmp), true, UINT64_MAX, &lane, &len) != MOQR_OK) {
            return UINT64_MAX;
        }
        best = len > best ? len : best;
        if (moqr_cli_pair_stats_json(tmp, sizeof(tmp), true, UINT64_MAX, &pair, &len) != MOQR_OK) {
            return UINT64_MAX;
        }
        best = len > best ? len : best;
        if (moqr_cli_run_config_json(tmp, sizeof(tmp), true, UINT64_MAX, &rc, &len) != MOQR_OK) {
            return UINT64_MAX;
        }
        best = len > best ? len : best;
    }
    return (uint64_t)best;
}

/* -- the sink --------------------------------------------------------------------- */

static bool
real_clock(void *ctx, uint64_t *out_us)
{
    struct timespec ts;
    (void)ctx;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0 || ts.tv_sec < 0) {
        return false;
    }
    *out_us = (uint64_t)ts.tv_sec * 1000000u + (uint64_t)ts.tv_nsec / 1000u;
    return true;
}

static bool
sink_clock(moqr_cli_log_t *log, uint64_t *out)
{
    if (log->io != NULL && log->io->clock_us != NULL) {
        return log->io->clock_us(log->io->ctx, out);
    }
    return real_clock(NULL, out);
}

static size_t
sink_write(moqr_cli_log_t *log, const char *p, size_t n)
{
    log->writes++;
    if (log->io != NULL && log->io->write != NULL) {
        return log->io->write(log->io->ctx, log->rows, p, n);
    }
    return fwrite(p, 1, n, log->rows);
}

static int
sink_flush(moqr_cli_log_t *log)
{
    log->flushes++;
    if (log->io != NULL && log->io->flush != NULL) {
        return log->io->flush(log->io->ctx, log->rows);
    }
    return fflush(log->rows);
}

static void
diag(moqr_cli_log_t *log, const char *text)
{
    /* best effort, on the prose stream (stderr in JSON mode); never checked,
     * never retried, never through the rows stream */
    (void)fputs(text, log->prose);
}

moqr_result_t
moqr_cli_log_init(moqr_cli_log_t *log, moqr_cli_log_format_t fmt, FILE *rows,
                  FILE *prose, const moqr_cli_log_io_t *io)
{
    uint64_t now = 0;

    if (log == NULL) {
        return MOQR_ERR_INVAL;
    }
    memset(log, 0, sizeof(*log));
    if ((fmt != MOQR_CLI_LOG_TEXT && fmt != MOQR_CLI_LOG_JSON) || rows == NULL ||
        prose == NULL) {
        return MOQR_ERR_INVAL;   /* the zeroed sink accepts nothing */
    }
    log->format = fmt;
    log->rows = rows;
    log->prose = prose;
    log->io = io;
    if (fmt == MOQR_CLI_LOG_JSON) {
        /* Every variant's maximum must fit the fixed line. */
        uint64_t bound = moqr_cli_log_line_bound();
        if (bound == UINT64_MAX || bound + 2u > (uint64_t)MOQR_CLI_LOG_LINE_CAP) {
            memset(log, 0, sizeof(*log));
            return MOQR_ERR_CAPACITY;
        }
        if (sink_clock(log, &now)) {
            log->base_us = now;
            log->last_us = now;
            log->clock_state = MOQR_CLI_LOG_CLOCK_OK;
        } else {
            log->clock_state = MOQR_CLI_LOG_CLOCK_UNAVAILABLE;
            log->clock_told = true;
            diag(log, "MOQ5 Relay: logging: the monotonic clock is "
                      "unavailable; elapsed_us will be null\n");
        }
    }
    log->sink_state = MOQR_CLI_LOG_SINK_OPEN;
    return MOQR_OK;
}

/* Sample for one event. Accepted samples never regress against the baseline
 * or the last accepted sample; anything else is `null` and diagnosed once. */
static bool
sink_elapsed(moqr_cli_log_t *log, uint64_t *out)
{
    uint64_t now = 0;
    if (log->clock_state != MOQR_CLI_LOG_CLOCK_OK) {
        return false;
    }
    if (sink_clock(log, &now) && now >= log->base_us && now >= log->last_us) {
        log->last_us = now;
        *out = now - log->base_us;
        return true;
    }
    if (!log->clock_told) {
        log->clock_told = true;
        diag(log, "MOQ5 Relay: logging: the monotonic clock refused or went "
                  "backwards; elapsed_us is null for that event\n");
    }
    return false;
}

static void
render_refused(moqr_cli_log_t *log, uint8_t family, const char *schema)
{
    log->dropped++;
    if ((log->render_told & family) == 0u) {
        char msg[128];
        log->render_told |= family;
        (void)snprintf(msg, sizeof(msg),
                       "MOQ5 Relay: logging: could not render %s; the event "
                       "was dropped\n", schema);
        diag(log, msg);
    }
}

/* Submit the rendered line (len body bytes in log->line). */
static void
sink_submit(moqr_cli_log_t *log, size_t len)
{
    size_t n;
    if (len + 2u > MOQR_CLI_LOG_LINE_CAP) {
        return;   /* impossible after init's bound check; refuse anyway */
    }
    log->line[len] = '\n';
    log->line[len + 1u] = '\0';
    n = sink_write(log, log->line, len + 1u);
    if (n != len + 1u || ferror(log->rows) ||
        sink_flush(log) != 0) {
        log->sink_state = MOQR_CLI_LOG_SINK_FAILED;
        if (!log->sink_told) {
            char msg[160];
            log->sink_told = true;
            (void)snprintf(msg, sizeof(msg),
                           "MOQ5 Relay: logging: stdout write failed (errno %d) "
                           "— JSON output has stopped; the last line may be "
                           "partial\n", errno);
            diag(log, msg);
        }
    }
}

/* The terminal check every entry point makes BEFORE it formats anything: an
 * emit on a sink that is not OPEN with a known format touches no buffer, no
 * stream, no clock. A FAILED JSON stream counts what it drops. */
static bool
sink_accepts(moqr_cli_log_t *log)
{
    if (log == NULL) {
        return false;
    }
    if (log->format != MOQR_CLI_LOG_TEXT && log->format != MOQR_CLI_LOG_JSON) {
        return false;
    }
    if (log->sink_state != MOQR_CLI_LOG_SINK_OPEN) {
        if (log->format == MOQR_CLI_LOG_JSON &&
            log->sink_state == MOQR_CLI_LOG_SINK_FAILED) {
            log->dropped++;
        }
        return false;
    }
    return true;
}

/* Text: today's printf("%s\n") on the rows stream as one unchecked write,
 * no flush, no latch. */
static void
text_put(moqr_cli_log_t *log, const char *s)
{
    size_t n = strlen(s);
    if (n + 2u > sizeof(log->line)) {
        return;
    }
    if (s != log->line) {
        memcpy(log->line, s, n);
    }
    log->line[n] = '\n';
    log->line[n + 1u] = '\0';
    (void)sink_write(log, log->line, n + 1u);
}

/* One text row under the old per-family buffer limit; the truncation
 * branches print the same literals as before. */
static void
text_line(moqr_cli_log_t *log, int len, size_t limit, const char *fallback)
{
    if (len > 0 && (size_t)len < limit) {
        text_put(log, log->line);
    } else {
        text_put(log, fallback);
    }
}

#define TEXT_LIMIT_LANE   2048u
#define TEXT_LIMIT_PAIR   512u
#define TEXT_LIMIT_CFG    512u

void
moqr_cli_log_ready(moqr_cli_log_t *log, const moqr_cli_log_ready_t *v)
{
    if (v == NULL || !sink_accepts(log)) {
        return;
    }
    if (log->format == MOQR_CLI_LOG_TEXT) {
        if (moqr_cli_log_ready_text(log->line, sizeof(log->line) - 1u, v) > 0) {
            text_put(log, log->line);
        } else {
            render_refused(log, MOQR_CLI_LOG_FAMILY_READY, "RELAY_READY_V1");
        }
        return;
    }
    {
        uint64_t eus = 0;
        bool ev = sink_elapsed(log, &eus);
        size_t len = 0;
        if (moqr_cli_log_ready_json(log->line, MOQR_CLI_LOG_LINE_CAP - 1u, ev, eus,
                                    v, &len) != MOQR_OK) {
            render_refused(log, MOQR_CLI_LOG_FAMILY_READY, "RELAY_READY_V1");
            return;
        }
        sink_submit(log, len);
    }
}

void
moqr_cli_log_run_config(moqr_cli_log_t *log, const moqr_cli_run_config_row_t *row)
{
    if (row == NULL || !sink_accepts(log)) {
        return;
    }
    if (log->format == MOQR_CLI_LOG_TEXT) {
        int len = moqr_cli_run_config_format(log->line, TEXT_LIMIT_CFG, row);
        text_line(log, len, TEXT_LIMIT_CFG, MOQR_RUN_CONFIG_PREFIX ",refused=format");
        return;
    }
    {
        uint64_t eus = 0;
        bool ev = sink_elapsed(log, &eus);
        size_t len = 0;
        if (moqr_cli_run_config_json(log->line, MOQR_CLI_LOG_LINE_CAP - 1u, ev, eus,
                                     row, &len) != MOQR_OK) {
            /* The fixed refusal record, once; if it refuses too the event is
             * dropped -- never a second fallback. */
            if (moqr_cli_run_config_refused_json(log->line, MOQR_CLI_LOG_LINE_CAP - 1u,
                                                 ev, eus, "format", &len) != MOQR_OK) {
                render_refused(log, MOQR_CLI_LOG_FAMILY_RUN_CONFIG, "RELAY_RUN_CONFIG_V1");
                return;
            }
        }
        sink_submit(log, len);
    }
}

void
moqr_cli_log_run_config_refused(moqr_cli_log_t *log, const char *which)
{
    if (which == NULL || !sink_accepts(log)) {
        return;
    }
    if (log->format == MOQR_CLI_LOG_TEXT) {
        char row[128];
        (void)snprintf(row, sizeof(row), MOQR_RUN_CONFIG_PREFIX ",refused=%s", which);
        text_put(log, row);
        return;
    }
    {
        uint64_t eus = 0;
        bool ev = sink_elapsed(log, &eus);
        size_t len = 0;
        if (moqr_cli_run_config_refused_json(log->line, MOQR_CLI_LOG_LINE_CAP - 1u, ev,
                                             eus, which, &len) != MOQR_OK) {
            render_refused(log, MOQR_CLI_LOG_FAMILY_RUN_CONFIG, "RELAY_RUN_CONFIG_V1");
            return;
        }
        sink_submit(log, len);
    }
}

void
moqr_cli_log_lane(moqr_cli_log_t *log, const moqr_cli_lane_stats_row_t *row)
{
    if (row == NULL || !sink_accepts(log)) {
        return;
    }
    if (log->format == MOQR_CLI_LOG_TEXT) {
        char fallback[64];
        int len = moqr_cli_lane_stats_format(log->line, TEXT_LIMIT_LANE, row);
        (void)snprintf(fallback, sizeof(fallback),
                       MOQR_LANE_STATS_PREFIX ",lane=%u,refused=adapter", row->lane);
        text_line(log, len, TEXT_LIMIT_LANE, fallback);
        return;
    }
    {
        uint64_t eus = 0;
        bool ev = sink_elapsed(log, &eus);
        size_t len = 0;
        if (moqr_cli_lane_stats_json(log->line, MOQR_CLI_LOG_LINE_CAP - 1u, ev, eus,
                                     row, &len) != MOQR_OK) {
            render_refused(log, MOQR_CLI_LOG_FAMILY_LANE, "RELAY_LANE_STATS_V3");
            return;
        }
        sink_submit(log, len);
    }
}

void
moqr_cli_log_lane_refused(moqr_cli_log_t *log, uint32_t lane, const char *which)
{
    if (which == NULL || !sink_accepts(log)) {
        return;
    }
    if (log->format == MOQR_CLI_LOG_TEXT) {
        char fallback[64];
        int len = moqr_cli_lane_stats_format_refused(log->line, TEXT_LIMIT_LANE, lane, which);
        (void)snprintf(fallback, sizeof(fallback),
                       MOQR_LANE_STATS_PREFIX ",lane=%u,refused=adapter", lane);
        text_line(log, len, TEXT_LIMIT_LANE, fallback);
        return;
    }
    {
        uint64_t eus = 0;
        bool ev = sink_elapsed(log, &eus);
        size_t len = 0;
        if (moqr_cli_lane_stats_refused_json(log->line, MOQR_CLI_LOG_LINE_CAP - 1u, ev,
                                             eus, lane, which, &len) != MOQR_OK) {
            render_refused(log, MOQR_CLI_LOG_FAMILY_LANE, "RELAY_LANE_STATS_V3");
            return;
        }
        sink_submit(log, len);
    }
}

void
moqr_cli_log_pair(moqr_cli_log_t *log, const moqr_cli_pair_stats_row_t *row)
{
    if (row == NULL || !sink_accepts(log)) {
        return;
    }
    if (log->format == MOQR_CLI_LOG_TEXT) {
        char fallback[80];
        int len = moqr_cli_pair_stats_format(log->line, TEXT_LIMIT_PAIR, row);
        (void)snprintf(fallback, sizeof(fallback),
                       MOQR_PAIR_STATS_PREFIX ",src=%u,dst=%u,refused=shard",
                       row->src, row->dst);
        text_line(log, len, TEXT_LIMIT_PAIR, fallback);
        return;
    }
    {
        uint64_t eus = 0;
        bool ev = sink_elapsed(log, &eus);
        size_t len = 0;
        if (moqr_cli_pair_stats_json(log->line, MOQR_CLI_LOG_LINE_CAP - 1u, ev, eus,
                                     row, &len) != MOQR_OK) {
            render_refused(log, MOQR_CLI_LOG_FAMILY_PAIR, "RELAY_PAIR_STATS_V1");
            return;
        }
        sink_submit(log, len);
    }
}

void
moqr_cli_log_pair_refused(moqr_cli_log_t *log, uint32_t src, uint32_t dst,
                          const char *which)
{
    if (which == NULL || !sink_accepts(log)) {
        return;
    }
    if (log->format == MOQR_CLI_LOG_TEXT) {
        char row[128];
        (void)snprintf(row, sizeof(row),
                       MOQR_PAIR_STATS_PREFIX ",src=%u,dst=%u,refused=%s", src, dst,
                       which);
        text_put(log, row);
        return;
    }
    {
        uint64_t eus = 0;
        bool ev = sink_elapsed(log, &eus);
        size_t len = 0;
        if (moqr_cli_pair_stats_refused_json(log->line, MOQR_CLI_LOG_LINE_CAP - 1u, ev,
                                             eus, src, dst, which, &len) != MOQR_OK) {
            render_refused(log, MOQR_CLI_LOG_FAMILY_PAIR, "RELAY_PAIR_STATS_V1");
            return;
        }
        sink_submit(log, len);
    }
}

void
moqr_cli_log_stop(moqr_cli_log_t *log, const moqr_cli_log_stop_t *v)
{
    if (v == NULL || !sink_accepts(log)) {
        return;
    }
    if (log->format == MOQR_CLI_LOG_TEXT) {
        if (moqr_cli_log_stop_text(log->line, sizeof(log->line) - 1u, v) > 0) {
            text_put(log, log->line);
        }
        return;
    }
    {
        uint64_t eus = 0;
        bool ev = sink_elapsed(log, &eus);
        size_t len = 0;
        if (moqr_cli_log_stop_json(log->line, MOQR_CLI_LOG_LINE_CAP - 1u, ev, eus, v,
                                   &len) != MOQR_OK) {
            render_refused(log, MOQR_CLI_LOG_FAMILY_STOP, "RELAY_STOP_V1");
            return;
        }
        sink_submit(log, len);
    }
}

void
moqr_cli_log_readiness_flush(moqr_cli_log_t *log)
{
    /* not an event: a closed or failed sink flushes nothing and drops nothing */
    if (log == NULL || log->sink_state != MOQR_CLI_LOG_SINK_OPEN) {
        return;
    }
    if (log->format == MOQR_CLI_LOG_TEXT) {
        (void)sink_flush(log);   /* today's fflush(stdout), unchecked */
    }
    /* JSON: every event was already flushed under the checked policy. */
}

FILE *
moqr_cli_log_prose_stream(const moqr_cli_log_t *log)
{
    if (log == NULL || log->sink_state == 0u) {
        return NULL;
    }
    return log->format == MOQR_CLI_LOG_JSON ? log->prose : log->rows;
}

void
moqr_cli_log_finish(moqr_cli_log_t *log)
{
    if (log == NULL || log->sink_state != MOQR_CLI_LOG_SINK_OPEN) {
        return;   /* FAILED stays FAILED; CLOSED stays CLOSED; zeroed does nothing */
    }
    if (log->format == MOQR_CLI_LOG_JSON) {
        if (sink_flush(log) != 0) {
            log->sink_state = MOQR_CLI_LOG_SINK_FAILED;
            if (!log->sink_told) {
                log->sink_told = true;
                diag(log, "MOQ5 Relay: logging: the final stdout flush failed; "
                          "the last JSON record may not have been delivered\n");
            }
            return;
        }
    }
    log->sink_state = MOQR_CLI_LOG_SINK_CLOSED;
}
