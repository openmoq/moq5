#include "lanestats.h"

#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* The row grammar: one shared key table drives BOTH the formatter and the
 * parser, so a field can never be emitted without being parsed (or appear
 * twice under one name). Order is the emission order and is enforced on
 * parse — rows are byte-deterministic for identical counters. */
static const char *const k_keys[] = {
    "wakes_same_lane", "wakes_cross_lane", "wakes_external",
    "wakes_coalesced", "pump_sweeps", "deadline_sweeps", "idle_cap_wakes",
    "wake_to_pump_max_us", "wake_to_pump_total_us", "wake_to_pump_samples",
    "service_passes", "flush_sends", "flush_bytes",
    "pump_turns", "pump_messages", "pump_bytes",
    "wake_requests_push", "wake_requests_credit",
    "enq_demand", "enq_undemand", "enq_done", "enq_ack",
    "enq_obj", "enq_obj_open", "enq_obj_chunk", "enq_obj_end",
    "enq_obj_reset", "enq_grp_reset", "enq_grp_evict", "enq_sg_seal",
    "channel_entries_hwm", "channel_bytes_hwm",
    "turns_msg_budget", "turns_byte_budget", "turns_blocked",
    "turns_drained", "turns_with_messages", "arb_class_refusals",
    "wake_requests_local",
};
#define K_NKEYS (sizeof(k_keys) / sizeof(k_keys[0]))

/* The row's u64 fields in k_keys order (lane is handled separately). */
static const uint64_t *field_ptr(const moqr_cli_lane_stats_row_t *r, size_t i)
{
    const uint64_t *fields[K_NKEYS] = {
        &r->wakes_same_lane, &r->wakes_cross_lane, &r->wakes_external,
        &r->wakes_coalesced, &r->pump_sweeps, &r->deadline_sweeps,
        &r->idle_cap_wakes, &r->wake_to_pump_max_us,
        &r->wake_to_pump_total_us, &r->wake_to_pump_samples,
        &r->service_passes, &r->flush_sends, &r->flush_bytes,
        &r->pump_turns, &r->pump_messages, &r->pump_bytes,
        &r->wake_requests_push, &r->wake_requests_credit,
        &r->enq_demand, &r->enq_undemand, &r->enq_done, &r->enq_ack,
        &r->enq_obj, &r->enq_obj_open, &r->enq_obj_chunk, &r->enq_obj_end,
        &r->enq_obj_reset, &r->enq_grp_reset, &r->enq_grp_evict,
        &r->enq_sg_seal, &r->channel_entries_hwm, &r->channel_bytes_hwm,
        &r->turns_msg_budget, &r->turns_byte_budget, &r->turns_blocked,
        &r->turns_drained, &r->turns_with_messages, &r->arb_class_refusals,
        &r->wake_requests_local,
    };
    return fields[i];
}

int moqr_cli_lane_stats_format(char *buf, size_t n,
                               const moqr_cli_lane_stats_row_t *row)
{
    size_t off = 0;
    int w = snprintf(buf, n, MOQR_LANE_STATS_PREFIX ",lane=%u", row->lane);
    if (w < 0)
        return w;
    off = (size_t)w;
    for (size_t i = 0; i < K_NKEYS; i++) {
        w = snprintf(off < n ? buf + off : NULL, off < n ? n - off : 0,
                     ",%s=%" PRIu64, k_keys[i], *field_ptr(row, i));
        if (w < 0)
            return w;
        off += (size_t)w;
    }
    /* end-of-row marker: a row truncated mid-write (even mid-NUMBER, which
     * would otherwise still parse as a shorter valid value) is malformed. */
    w = snprintf(off < n ? buf + off : NULL, off < n ? n - off : 0, ",eor=1");
    if (w < 0)
        return w;
    off += (size_t)w;
    return (int)off;
}

int moqr_cli_lane_stats_format_refused(char *buf, size_t n, uint32_t lane,
                                       const char *which)
{
    return snprintf(buf, n, MOQR_LANE_STATS_PREFIX ",lane=%u,refused=%s",
                    lane, which);
}

static bool parse_u64(const char **p, uint64_t *out)
{
    if (**p < '0' || **p > '9')
        return false;
    char *end = NULL;
    errno = 0;
    unsigned long long v = strtoull(*p, &end, 10);
    if (end == *p || errno == ERANGE)
        return false;   /* > UINT64_MAX is malformed, never saturated */
    *p = end;
    *out = (uint64_t)v;
    return true;
}

moqr_cli_lane_stats_kind_t moqr_cli_lane_stats_parse(
    const char *line, moqr_cli_lane_stats_row_t *row, uint32_t *refused_lane)
{
    static const char pfx[] = MOQR_LANE_STATS_PREFIX ",lane=";
    if (strncmp(line, pfx, sizeof(pfx) - 1) != 0)
        return MOQR_LANE_STATS_MALFORMED;
    const char *p = line + sizeof(pfx) - 1;
    uint64_t lane64 = 0;
    if (!parse_u64(&p, &lane64) || lane64 > 0xFFFFFFFFull)
        return MOQR_LANE_STATS_MALFORMED;

    if (strncmp(p, ",refused=", 9) == 0) {
        const char *which = p + 9;
        if (strcmp(which, "adapter") != 0 && strcmp(which, "shard") != 0)
            return MOQR_LANE_STATS_MALFORMED;
        if (refused_lane != NULL)
            *refused_lane = (uint32_t)lane64;
        return MOQR_LANE_STATS_REFUSED;
    }

    memset(row, 0, sizeof(*row));
    row->lane = (uint32_t)lane64;
    for (size_t i = 0; i < K_NKEYS; i++) {
        size_t klen = strlen(k_keys[i]);
        if (*p != ',' ||
            strncmp(p + 1, k_keys[i], klen) != 0 || p[1 + klen] != '=')
            return MOQR_LANE_STATS_MALFORMED;
        p += 2 + klen;
        uint64_t v = 0;
        if (!parse_u64(&p, &v))
            return MOQR_LANE_STATS_MALFORMED;
        *(uint64_t *)(uintptr_t)field_ptr(row, i) = v;
    }
    if (strcmp(p, ",eor=1") != 0)
        return MOQR_LANE_STATS_MALFORMED;   /* truncated / junk / extra */
    return MOQR_LANE_STATS_ROW;
}

bool moqr_cli_lane_stats_record(const char *const *lines, size_t nlines,
                                uint32_t expected_lanes,
                                moqr_cli_lane_stats_row_t *rows)
{
    if (nlines != expected_lanes)
        return false;   /* missing or extra lane rows */
    for (size_t i = 0; i < nlines; i++) {
        uint32_t rl = 0;
        moqr_cli_lane_stats_kind_t k =
            moqr_cli_lane_stats_parse(lines[i], &rows[i], &rl);
        if (k != MOQR_LANE_STATS_ROW)
            return false;   /* refusal or malformed line poisons the rep */
        if (rows[i].lane != (uint32_t)i)
            return false;   /* duplicate, missing, or out-of-order lane */
    }
    return true;
}

bool moqr_cli_lane_stats_avg_us(const moqr_cli_lane_stats_row_t *row,
                                double *out_avg_us)
{
    if (row->wake_to_pump_samples == 0)
        return false;   /* UNAVAILABLE — never zero */
    *out_avg_us = (double)row->wake_to_pump_total_us /
                  (double)row->wake_to_pump_samples;
    return true;
}

/* --- pair rows (RELAY_PAIR_STATS_V1) --------------------------------------
 * Same shared-key-table discipline as the lane rows: one table drives the
 * formatter AND the parser, exact order enforced, ,eor=1 terminator. */
static const char *const k_pair_keys[] = {
    "data_messages", "data_bytes", "control_messages",
    "refused_entries", "refused_bytes",
};
#define K_NPAIR (sizeof(k_pair_keys) / sizeof(k_pair_keys[0]))

static const uint64_t *pair_field_ptr(const moqr_cli_pair_stats_row_t *r,
                                      size_t i)
{
    const uint64_t *fields[K_NPAIR] = {
        &r->data_messages, &r->data_bytes, &r->control_messages,
        &r->refused_entries, &r->refused_bytes,
    };
    return fields[i];
}

int moqr_cli_pair_stats_format(char *buf, size_t n,
                               const moqr_cli_pair_stats_row_t *row)
{
    size_t off = 0;
    int w = snprintf(buf, n, MOQR_PAIR_STATS_PREFIX ",src=%u,dst=%u",
                     row->src, row->dst);
    if (w < 0)
        return w;
    off = (size_t)w;
    for (size_t i = 0; i < K_NPAIR; i++) {
        w = snprintf(off < n ? buf + off : NULL, off < n ? n - off : 0,
                     ",%s=%" PRIu64, k_pair_keys[i], *pair_field_ptr(row, i));
        if (w < 0)
            return w;
        off += (size_t)w;
    }
    w = snprintf(off < n ? buf + off : NULL, off < n ? n - off : 0, ",eor=1");
    if (w < 0)
        return w;
    off += (size_t)w;
    return (int)off;
}

moqr_cli_lane_stats_kind_t moqr_cli_pair_stats_parse(
    const char *line, moqr_cli_pair_stats_row_t *row)
{
    static const char pfx[] = MOQR_PAIR_STATS_PREFIX ",src=";
    if (strncmp(line, pfx, sizeof(pfx) - 1) != 0)
        return MOQR_LANE_STATS_MALFORMED;
    const char *p = line + sizeof(pfx) - 1;
    uint64_t v = 0;
    if (!parse_u64(&p, &v) || v > 0xFFFFFFFFull)
        return MOQR_LANE_STATS_MALFORMED;
    memset(row, 0, sizeof(*row));
    row->src = (uint32_t)v;
    if (strncmp(p, ",dst=", 5) != 0)
        return MOQR_LANE_STATS_MALFORMED;
    p += 5;
    if (!parse_u64(&p, &v) || v > 0xFFFFFFFFull)
        return MOQR_LANE_STATS_MALFORMED;
    row->dst = (uint32_t)v;
    for (size_t i = 0; i < K_NPAIR; i++) {
        size_t klen = strlen(k_pair_keys[i]);
        if (*p != ',' ||
            strncmp(p + 1, k_pair_keys[i], klen) != 0 || p[1 + klen] != '=')
            return MOQR_LANE_STATS_MALFORMED;
        p += 2 + klen;
        if (!parse_u64(&p, &v))
            return MOQR_LANE_STATS_MALFORMED;
        *(uint64_t *)(uintptr_t)pair_field_ptr(row, i) = v;
    }
    if (strcmp(p, ",eor=1") != 0)
        return MOQR_LANE_STATS_MALFORMED;
    return MOQR_LANE_STATS_ROW;
}

bool moqr_cli_pair_stats_record(const char *const *lines, size_t nlines,
                                uint32_t shards,
                                moqr_cli_pair_stats_row_t *rows)
{
    if (shards == 0 ||
        nlines != (size_t)shards * (shards - 1u))
        return false;   /* a record is EXACTLY the K*(K-1) non-self pairs —
                         * for K == 1 that is exactly ZERO rows (the lanes=1
                         * control is a complete, valid, empty record) */
    size_t i = 0;
    for (uint32_t src = 0; src < shards; src++) {
        for (uint32_t dst = 0; dst < shards; dst++) {
            if (dst == src)
                continue;
            if (moqr_cli_pair_stats_parse(lines[i], &rows[i]) !=
                MOQR_LANE_STATS_ROW)
                return false;
            if (rows[i].src != src || rows[i].dst != dst)
                return false;   /* missing/duplicate/self/out-of-order */
            i++;
        }
    }
    return true;
}

/* --- the run-config record (RELAY_RUN_CONFIG_V1) --------------------------- */

static const char *const k_cfg_keys[] = {
    "pump_turn_messages", "pump_turn_bytes",
    "demand_channel_entries", "demand_channel_bytes",
};
#define K_NCFG (sizeof(k_cfg_keys) / sizeof(k_cfg_keys[0]))

static const uint64_t *cfg_field_ptr(const moqr_cli_run_config_row_t *r,
                                     size_t i)
{
    const uint64_t *fields[K_NCFG] = {
        &r->pump_turn_messages, &r->pump_turn_bytes,
        &r->demand_channel_entries, &r->demand_channel_bytes,
    };
    return fields[i];
}

int moqr_cli_run_config_format(char *buf, size_t n,
                               const moqr_cli_run_config_row_t *row)
{
    size_t off = 0;
    int w = snprintf(buf, n, "%s", MOQR_RUN_CONFIG_PREFIX);
    if (w < 0)
        return w;
    off = (size_t)w;
    for (size_t i = 0; i < K_NCFG; i++) {
        w = snprintf(off < n ? buf + off : NULL, off < n ? n - off : 0,
                     ",%s=%" PRIu64, k_cfg_keys[i], *cfg_field_ptr(row, i));
        if (w < 0)
            return w;
        off += (size_t)w;
    }
    w = snprintf(off < n ? buf + off : NULL, off < n ? n - off : 0, ",eor=1");
    if (w < 0)
        return w;
    off += (size_t)w;
    return (int)off;
}

moqr_cli_lane_stats_kind_t moqr_cli_run_config_parse(
    const char *line, moqr_cli_run_config_row_t *row)
{
    static const char pfx[] = MOQR_RUN_CONFIG_PREFIX;
    if (strncmp(line, pfx, sizeof(pfx) - 1) != 0)
        return MOQR_LANE_STATS_MALFORMED;
    const char *p = line + sizeof(pfx) - 1;
    memset(row, 0, sizeof(*row));
    for (size_t i = 0; i < K_NCFG; i++) {
        size_t klen = strlen(k_cfg_keys[i]);
        if (*p != ',' ||
            strncmp(p + 1, k_cfg_keys[i], klen) != 0 || p[1 + klen] != '=')
            return MOQR_LANE_STATS_MALFORMED;
        p += 2 + klen;
        uint64_t v = 0;
        if (!parse_u64(&p, &v))
            return MOQR_LANE_STATS_MALFORMED;
        /* resolved-value bounds: the resolver guarantees every field
         * nonzero, and the message/entry fields are uint32_t in the
         * runtime — an impossible value here is a forged or corrupted
         * record, and this is the strict anti-pooling boundary */
        if (v == 0)
            return MOQR_LANE_STATS_MALFORMED;
        if ((i == 0 || i == 2) && v > 0xFFFFFFFFull)
            return MOQR_LANE_STATS_MALFORMED;
        *(uint64_t *)(uintptr_t)cfg_field_ptr(row, i) = v;
    }
    if (strcmp(p, ",eor=1") != 0)
        return MOQR_LANE_STATS_MALFORMED;
    return MOQR_LANE_STATS_ROW;
}

/* -- the same records as JSON events -----------------------------------------
 *
 * Driven by the SAME key tables and getters as the text formatters above, so a
 * numeric field appears in both outputs or in neither. The envelope is
 * `schema` (the versioned prefix) and `elapsed_us` (an unsigned integer, or
 * `null` when the caller says the elapsed time is not available); the
 * coordinates (`lane`, `src`/`dst`) precede the numeric fields. No clock, no
 * I/O, nothing read that the caller did not pass. */
#include "../admin/json_out.h"

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
finish_doc(moqr_json_w_t *w, char *buf, size_t *out_len)
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
        /* The inputs are integers and program literals: the only refusal a
         * numeric record can meet is the capacity of the caller's buffer. */
        return MOQR_ERR_CAPACITY;
    }
    if (out_len != NULL) {
        *out_len = len;
    }
    return MOQR_OK;
}

/* The closed refusal vocabulary of each record. */
static bool
refusal_word_ok(const char *which, const char *const *words, size_t n)
{
    if (which == NULL) {
        return false;
    }
    for (size_t i = 0; i < n; i++) {
        if (strcmp(which, words[i]) == 0) {
            return true;
        }
    }
    return false;
}

static const char *const k_lane_refusals[] = { "adapter", "shard" };
static const char *const k_pair_refusals[] = { "shard" };
static const char *const k_cfg_refusals[] = { "resolver", "format" };

static bool
args_ok(const char *buf, size_t cap, const void *row, size_t *out_len)
{
    if (out_len != NULL) {
        *out_len = 0;
    }
    return buf != NULL && cap != 0u && row != NULL;
}

moqr_result_t
moqr_cli_lane_stats_json(char *buf, size_t cap, bool elapsed_valid,
                         uint64_t elapsed_us,
                         const moqr_cli_lane_stats_row_t *row,
                         size_t *out_len)
{
    moqr_json_w_t w;
    if (!args_ok(buf, cap, row, out_len)) {
        return MOQR_ERR_INVAL;
    }
    moqr_json_begin(&w, buf, cap);
    envelope(&w, MOQR_LANE_STATS_PREFIX, elapsed_valid, elapsed_us);
    moqr_json_key(&w, "lane");
    moqr_json_u64(&w, row->lane);
    for (size_t i = 0; i < K_NKEYS; i++) {
        moqr_json_key(&w, k_keys[i]);
        moqr_json_u64(&w, *field_ptr(row, i));
    }
    return finish_doc(&w, buf, out_len);
}

moqr_result_t
moqr_cli_lane_stats_refused_json(char *buf, size_t cap, bool elapsed_valid,
                                 uint64_t elapsed_us, uint32_t lane,
                                 const char *which, size_t *out_len)
{
    moqr_json_w_t w;
    if (out_len != NULL) {
        *out_len = 0;
    }
    if (buf == NULL || cap == 0u ||
        !refusal_word_ok(which, k_lane_refusals, 2)) {
        return MOQR_ERR_INVAL;
    }
    moqr_json_begin(&w, buf, cap);
    envelope(&w, MOQR_LANE_STATS_PREFIX, elapsed_valid, elapsed_us);
    moqr_json_key(&w, "lane");
    moqr_json_u64(&w, lane);
    moqr_json_key(&w, "refused");
    moqr_json_str(&w, which, strlen(which));
    return finish_doc(&w, buf, out_len);
}

moqr_result_t
moqr_cli_pair_stats_json(char *buf, size_t cap, bool elapsed_valid,
                         uint64_t elapsed_us,
                         const moqr_cli_pair_stats_row_t *row,
                         size_t *out_len)
{
    moqr_json_w_t w;
    if (!args_ok(buf, cap, row, out_len)) {
        return MOQR_ERR_INVAL;
    }
    moqr_json_begin(&w, buf, cap);
    envelope(&w, MOQR_PAIR_STATS_PREFIX, elapsed_valid, elapsed_us);
    moqr_json_key(&w, "src");
    moqr_json_u64(&w, row->src);
    moqr_json_key(&w, "dst");
    moqr_json_u64(&w, row->dst);
    for (size_t i = 0; i < K_NPAIR; i++) {
        moqr_json_key(&w, k_pair_keys[i]);
        moqr_json_u64(&w, *pair_field_ptr(row, i));
    }
    return finish_doc(&w, buf, out_len);
}

moqr_result_t
moqr_cli_pair_stats_refused_json(char *buf, size_t cap, bool elapsed_valid,
                                 uint64_t elapsed_us, uint32_t src,
                                 uint32_t dst, const char *which,
                                 size_t *out_len)
{
    moqr_json_w_t w;
    if (out_len != NULL) {
        *out_len = 0;
    }
    if (buf == NULL || cap == 0u ||
        !refusal_word_ok(which, k_pair_refusals, 1)) {
        return MOQR_ERR_INVAL;
    }
    moqr_json_begin(&w, buf, cap);
    envelope(&w, MOQR_PAIR_STATS_PREFIX, elapsed_valid, elapsed_us);
    moqr_json_key(&w, "src");
    moqr_json_u64(&w, src);
    moqr_json_key(&w, "dst");
    moqr_json_u64(&w, dst);
    moqr_json_key(&w, "refused");
    moqr_json_str(&w, which, strlen(which));
    return finish_doc(&w, buf, out_len);
}

moqr_result_t
moqr_cli_run_config_json(char *buf, size_t cap, bool elapsed_valid,
                         uint64_t elapsed_us,
                         const moqr_cli_run_config_row_t *row,
                         size_t *out_len)
{
    moqr_json_w_t w;
    if (!args_ok(buf, cap, row, out_len)) {
        return MOQR_ERR_INVAL;
    }
    moqr_json_begin(&w, buf, cap);
    envelope(&w, MOQR_RUN_CONFIG_PREFIX, elapsed_valid, elapsed_us);
    for (size_t i = 0; i < K_NCFG; i++) {
        moqr_json_key(&w, k_cfg_keys[i]);
        moqr_json_u64(&w, *cfg_field_ptr(row, i));
    }
    return finish_doc(&w, buf, out_len);
}

moqr_result_t
moqr_cli_run_config_refused_json(char *buf, size_t cap, bool elapsed_valid,
                                 uint64_t elapsed_us, const char *which,
                                 size_t *out_len)
{
    moqr_json_w_t w;
    if (out_len != NULL) {
        *out_len = 0;
    }
    if (buf == NULL || cap == 0u ||
        !refusal_word_ok(which, k_cfg_refusals, 2)) {
        return MOQR_ERR_INVAL;
    }
    moqr_json_begin(&w, buf, cap);
    envelope(&w, MOQR_RUN_CONFIG_PREFIX, elapsed_valid, elapsed_us);
    moqr_json_key(&w, "refused");
    moqr_json_str(&w, which, strlen(which));
    return finish_doc(&w, buf, out_len);
}

size_t moqr_cli_lane_stats_field_count(void) { return K_NKEYS; }
size_t moqr_cli_pair_stats_field_count(void) { return K_NPAIR; }
size_t moqr_cli_run_config_field_count(void) { return K_NCFG; }
