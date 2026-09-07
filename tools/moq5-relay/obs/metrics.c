/* Prometheus text-format exporter over relay stat snapshots (pure). */

#include "moqr_obs.h"

#include <stdarg.h>
#include <stdio.h>
#include <string.h>

/* Bounded append writer: snprintf-style, counts the full required length in
 * `len` even past `cap`, so callers learn the needed size on truncation. */
typedef struct {
    char             *buf;
    size_t            cap;
    size_t            len;
    moqr_obs_format_t fmt;
    /* Extended families: series that did not exist in the frozen legacy
     * exposition. The legacy entry points render without them so their bytes
     * stay exactly as accepted; the format-aware entry points render with
     * them. See the note above the compatibility wrappers. */
    bool              ext;
    /* Sticky: a length accumulation that would wrap. Once set, the reported
     * length is meaningless as a size, so every caller must refuse rather
     * than allocate from it. */
    bool              overflow;
} mw_t;

static void
mw_addf(mw_t *w, const char *fmt, ...)
{
    char  *dst = (w->len < w->cap) ? w->buf + w->len : NULL;
    size_t avail = (w->len < w->cap) ? w->cap - w->len : 0;
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(dst, avail, fmt, ap);
    va_end(ap);
    if (n < 0) {
        /* An encoding error cannot be turned into a length; refuse. */
        w->overflow = true;
        return;
    }
    /* Checked accumulation. A wrapped length would be reported as a small
     * buffer requirement, which is the one arithmetic mistake in this file
     * that could hand a caller an under-sized allocation. */
    if ((size_t)n > SIZE_MAX - w->len) {
        w->overflow = true;
        w->len = SIZE_MAX;
        return;
    }
    w->len += (size_t)n;
}

/* Escape a label value per the Prometheus exposition format (backslash,
 * double-quote, newline). Values are short bounded tokens; a truncated
 * escape is impossible for the sets we emit but the guard keeps it safe. */
static void
esc_label(const char *s, char *out, size_t outcap)
{
    size_t o = 0;
    if (s == NULL) {
        s = "";
    }
    for (; *s != '\0' && o + 2 < outcap; s++) {
        if (*s == '"' || *s == '\\') {
            out[o++] = '\\';
            out[o++] = *s;
        } else if (*s == '\n') {
            out[o++] = '\\';
            out[o++] = 'n';
        } else {
            out[o++] = *s;
        }
    }
    out[o] = '\0';
}

/* Emit one counter/gauge line: the base labels plus an optional extra
 * label (reason/state). `extra_key == NULL` emits base labels only. */
static void
emit(mw_t *w, const char *name, const char *base, const char *extra_key,
     const char *extra_val, unsigned long long value)
{
    if (extra_key != NULL) {
        char ev[64];
        esc_label(extra_val, ev, sizeof(ev));
        mw_addf(w, "%s{%s,%s=\"%s\"} %llu\n", name, base, extra_key, ev,
                value);
    } else {
        mw_addf(w, "%s{%s} %llu\n", name, base, value);
    }
}

/* Emit one line with TWO extra labels (the auth vector's action × verdict). */
static void
emit2(mw_t *w, const char *name, const char *base, const char *k1,
      const char *v1, const char *k2, const char *v2,
      unsigned long long value)
{
    char e1[64];
    char e2[64];
    esc_label(v1, e1, sizeof(e1));
    esc_label(v2, e2, sizeof(e2));
    mw_addf(w, "%s{%s,%s=\"%s\",%s=\"%s\"} %llu\n", name, base, k1, e1, k2, e2,
            value);
}

/* Stable label token for an auth verdict (bounded: 3 values). */
static const char *
verdict_name(uint32_t decision)
{
    switch (decision) {
    case MOQR_AUTH_ALLOW:
        return "allow";
    case MOQR_AUTH_DENY:
        return "deny";
    case MOQR_AUTH_DEFER:
        return "defer";
    default:
        return "unknown";
    }
}

/* The base units that may appear as a metric-name suffix. OpenMetrics wants
 * the unit restated in a `# UNIT` line, and requires it to be the tail of the
 * MetricFamily name — so this table is also the authority for which names may
 * carry one. */
static const char *
unit_suffix(const char *family)
{
    static const char *const units[] = { "seconds", "bytes", "ratio" };
    size_t flen = strlen(family);
    for (size_t i = 0; i < sizeof(units) / sizeof(units[0]); i++) {
        size_t ulen = strlen(units[i]);
        /* Must be a whole trailing name segment: `..._bytes`, never
         * `..._bytes_highwater` and never a name that merely ends in the
         * letters. */
        if (flen > ulen + 1 && family[flen - ulen - 1] == '_' &&
            strcmp(family + flen - ulen, units[i]) == 0) {
            return units[i];
        }
    }
    return NULL;
}

/*
 * Emit a family's metadata block.
 *
 * The two formats disagree about the name: Prometheus 0.0.4 names the family
 * exactly as the sample is named, while OpenMetrics names a counter family
 * WITHOUT the `_total` the sample carries. Everything downstream keeps
 * emitting the full sample name, so the split lives here alone.
 */
static void
header(mw_t *w, const char *name, const char *help, const char *type)
{
    if (w->fmt != MOQR_OBS_FMT_OPENMETRICS_100) {
        mw_addf(w, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, type);
        return;
    }

    char        fam[128];
    const char *use = name;
    size_t      nlen = strlen(name);
    const char *suffix = "_total";
    size_t      slen = strlen(suffix);
    if (strcmp(type, "counter") == 0 && nlen > slen &&
        strcmp(name + nlen - slen, suffix) == 0 && nlen - slen < sizeof(fam)) {
        memcpy(fam, name, nlen - slen);
        fam[nlen - slen] = '\0';
        use = fam;
    }

    mw_addf(w, "# HELP %s %s\n# TYPE %s %s\n", use, help, use, type);
    const char *unit = unit_suffix(use);
    if (unit != NULL) {
        mw_addf(w, "# UNIT %s %s\n", use, unit);
    }
}

/*
 * Terminate the buffer and decide the result. Both entry points end here so
 * the truncation and overflow contracts cannot drift apart between them.
 *
 * An overflowed accumulation is fail-closed: its length is not a usable size,
 * so it is reported as capacity exhaustion with no requirement a caller could
 * act on. Handing back a wrapped length would be the one arithmetic mistake
 * here that yields an under-sized allocation.
 */
static moqr_result_t
mw_finish(mw_t *w, char *buf, size_t cap, size_t *written, bool *poisoned)
{
    if (poisoned != NULL) {
        *poisoned = w->overflow;
    }
    if (cap > 0) {
        buf[w->len < cap ? w->len : cap - 1] = '\0';
    }
    if (w->overflow) {
        if (written != NULL) {
            *written = 0;
        }
        return MOQR_ERR_CAPACITY;
    }
    if (written != NULL) {
        *written = w->len;
    }
    return w->len < cap ? MOQR_OK : MOQR_ERR_CAPACITY;
}

/* Close the document. OpenMetrics requires a terminal `# EOF`; 0.0.4 has no
 * end marker at all. */
static void
finish(mw_t *w)
{
    if (w->fmt == MOQR_OBS_FMT_OPENMETRICS_100) {
        mw_addf(w, "# EOF\n");
    }
}

/* A single-series counter (HELP + TYPE + one value line). */
static void
counter1(mw_t *w, const char *name, const char *help, const char *base,
         unsigned long long value)
{
    header(w, name, help, "counter");
    emit(w, name, base, NULL, NULL, value);
}

/* A single-series gauge. */
static void
gauge1(mw_t *w, const char *name, const char *help, const char *base,
       unsigned long long value)
{
    header(w, name, help, "gauge");
    emit(w, name, base, NULL, NULL, value);
}

static moqr_result_t
write_single(const moqr_core_stats_t *core, const moqr_bind_stats_t *bind,
             const moqr_obs_labels_t *labels, moqr_obs_format_t fmt, bool ext,
             char *buf, size_t cap, size_t *written)
{
    if (written != NULL) {
        *written = 0;
    }
    if (core == NULL || buf == NULL || fmt >= MOQR_OBS_FMT__COUNT) {
        return MOQR_ERR_INVAL;
    }

    /* Build the shared label block once. transport/version are escaped in
     * case a future caller passes a non-token value. */
    char tr[64];
    char ver[64];
    esc_label(labels != NULL ? labels->transport : NULL, tr, sizeof(tr));
    esc_label(labels != NULL ? labels->version : NULL, ver, sizeof(ver));
    char base[192];
    snprintf(base, sizeof(base), "shard=\"%u\",transport=\"%s\",version=\"%s\"",
             (unsigned)(labels != NULL ? labels->shard : 0), tr, ver);

    mw_t w = { buf, cap, 0, fmt, ext, false };

    /* -- object flow (counters) -- */
    counter1(&w, "moqrelay_objects_ingested_total",
             "Objects ingested from upstream publishers.", base,
             (unsigned long long)core->ingested_total);
    counter1(&w, "moqrelay_objects_delivered_total",
             "Objects delivered to downstream subscribers.", base,
             (unsigned long long)core->delivered_total);
    counter1(&w, "moqrelay_objects_evicted_total",
             "Object records evicted from track logs by retention.", base,
             (unsigned long long)core->evicted_total);

    /* -- admission refusals by resource (counter vector) -- */
    header(&w, "moqrelay_refusals_total",
           "Admission requests refused, by exhausted resource.", "counter");
    for (uint32_t i = 0; i < MOQR_REFUSE__COUNT; i++) {
        emit(&w, "moqrelay_refusals_total", base, "reason",
             moqr_refuse_reason_name(i), (unsigned long long)core->refusals[i]);
    }

    /* -- authorization decisions (counter vector: action × verdict) --
     * Bounded cardinality: action (9) × verdict (3). Labels are action/
     * verdict tokens only — never token bytes or any peer identity. */
    header(&w, "moqrelay_auth_decisions_total",
           "Authorization decisions, by action and verdict.", "counter");
    for (uint32_t act = 0; act < MOQR_AUTH_ACTION__COUNT; act++) {
        for (uint32_t d = 0; d < 3u; d++) {
            emit2(&w, "moqrelay_auth_decisions_total", base, "action",
                  moqr_auth_action_name(act), "verdict", verdict_name(d),
                  (unsigned long long)core->auth_decisions[act][d]);
        }
    }

    /* -- authorization denials by reason (counter vector) -- */
    header(&w, "moqrelay_auth_denials_total",
           "Authorization denials, by reason.", "counter");
    for (uint32_t r = 0; r < MOQR_AUTH_REASON__COUNT; r++) {
        emit(&w, "moqrelay_auth_denials_total", base, "reason",
             moqr_auth_reason_name(r),
             (unsigned long long)core->auth_denials[r]);
    }

    /* -- live entities (gauges) -- */
    gauge1(&w, "moqrelay_bindings", "Live session bindings.", base,
           (unsigned long long)core->bindings);
    gauge1(&w, "moqrelay_tracks", "Live tracks.", base,
           (unsigned long long)core->tracks);

    header(&w, "moqrelay_subscriptions",
           "Live downstream subscriptions, by state.", "gauge");
    emit(&w, "moqrelay_subscriptions", base, "state", "parked",
         (unsigned long long)core->subs_parked);
    emit(&w, "moqrelay_subscriptions", base, "state", "active",
         (unsigned long long)core->subs_active);

    gauge1(&w, "moqrelay_namespace_nodes", "Namespace trie nodes.", base,
           (unsigned long long)core->ns_nodes);
    gauge1(&w, "moqrelay_namespace_subscriptions",
           "Live namespace (prefix) subscriptions.", base,
           (unsigned long long)core->ns_subs);
    gauge1(&w, "moqrelay_retained_bytes",
           "Payload and property bytes retained across track logs.", base,
           (unsigned long long)core->retained_bytes);
    gauge1(&w, "moqrelay_intent_ring_highwater",
           "Peak intent-ring occupancy since start.", base,
           (unsigned long long)core->intent_highwater);
    gauge1(&w, "moqrelay_route_epoch",
           "Monotonic route-mutation epoch (this shard).", base,
           (unsigned long long)core->route_epoch);

    /* -- binding-tier stats (optional) -- */
    if (bind != NULL) {
        gauge1(&w, "moqrelay_connections", "Attached transport connections.",
               base, (unsigned long long)bind->conns);
        counter1(&w, "moqrelay_events_translated_total",
                 "Session events translated into core inputs.", base,
                 (unsigned long long)bind->events_translated);
        counter1(&w, "moqrelay_deliveries_written_total",
                 "Deliveries written to downstream sessions.", base,
                 (unsigned long long)bind->deliveries_written);
        counter1(&w, "moqrelay_ingest_refusals_total",
                 "Ingest records refused as too old or out of order.", base,
                 (unsigned long long)bind->ingest_refusals);
        counter1(&w, "moqrelay_session_errors_total",
                 "Unexpected session-call failures.", base,
                 (unsigned long long)bind->session_errors);

        /* Forward-latency histogram (Prometheus shape). Buckets are stored
         * non-cumulative; the exposition needs cumulative `le` counts. le and
         * _sum are seconds (base unit), formatted from microseconds without a
         * float so the output stays deterministic and locale-independent. */
        const moqr_latency_hist_t *h = &bind->forward_latency;
        header(&w, "moqrelay_forward_latency_seconds",
               "Object ingest-to-delivery latency (successful writes only).",
               "histogram");
        unsigned long long cum = 0;
        for (uint32_t i = 0; i < MOQR_LATENCY_BUCKETS; i++) {
            cum += (unsigned long long)h->bucket[i];
            uint64_t b_us = moqr_latency_bound_us(i);
            char le[32];
            snprintf(le, sizeof(le), "%llu.%06llu",
                     (unsigned long long)(b_us / 1000000u),
                     (unsigned long long)(b_us % 1000000u));
            emit(&w, "moqrelay_forward_latency_seconds_bucket", base, "le", le,
                 cum);
        }
        cum += (unsigned long long)h->bucket[MOQR_LATENCY_BUCKETS];
        emit(&w, "moqrelay_forward_latency_seconds_bucket", base, "le", "+Inf",
             cum);
        mw_addf(&w, "moqrelay_forward_latency_seconds_sum{%s} %llu.%06llu\n",
                base, (unsigned long long)(h->sum_us / 1000000u),
                (unsigned long long)(h->sum_us % 1000000u));
        mw_addf(&w, "moqrelay_forward_latency_seconds_count{%s} %llu\n", base,
                (unsigned long long)h->count);
    }

    finish(&w);

    return mw_finish(&w, buf, cap, written, NULL);
}

/* -- multi-snapshot exposition (one document, N shards) ---------------------- */

/* Bounded by the shard runtime's own cap; the per-view label blocks live on
 * the stack. */
#define MV_MAX MOQR_SHARDS_MAX
/* Rendered label-set prefix per view: shard, transport and version. */
#define MV_BASE_CAP 192

typedef unsigned long long (*mv_get_fn)(const moqr_snapshot_view_t *v);
typedef bool (*mv_has_fn)(const moqr_snapshot_view_t *v);

#define MV_AGG_NONE 0
#define MV_AGG_SUM  1
#define MV_AGG_MAX  2

/* moqrelay_<x> -> moqrelay_process_<x>: the separate process-aggregate
 * series name (per-shard names never carry aggregated values). */
static void
mv_proc_name(char *out, size_t cap, const char *name)
{
    snprintf(out, cap, "moqrelay_process_%s",
             name + strlen("moqrelay_"));
}

static bool
mv_has_bind(const moqr_snapshot_view_t *v)
{
    return v->bind != NULL;
}

static bool
mv_has_shard(const moqr_snapshot_view_t *v)
{
    return v->shard != NULL;
}

/* One single-series-per-shard family: HELP/TYPE once, one line per view
 * that carries the family, then (unless MV_AGG_NONE) the process aggregate
 * under its own moqrelay_process_* name. */
static void
mv_family(mw_t *w, const char *name, const char *help, const char *type,
          const moqr_snapshot_view_t *vs, uint32_t n,
          const char bases[][MV_BASE_CAP], mv_has_fn has, mv_get_fn get,
          int agg)
{
    bool present = false;
    for (uint32_t i = 0; i < n; i++) {
        if (has == NULL || has(&vs[i])) {
            present = true;
            break;
        }
    }
    if (!present) {
        return;
    }
    header(w, name, help, type);
    unsigned long long acc = 0;
    bool any = false;
    for (uint32_t i = 0; i < n; i++) {
        if (has != NULL && !has(&vs[i])) {
            continue;
        }
        unsigned long long v = get(&vs[i]);
        mw_addf(w, "%s{%s} %llu\n", name, bases[i], v);
        if (agg == MV_AGG_SUM) {
            acc += v;
        } else if (agg == MV_AGG_MAX && (!any || v > acc)) {
            acc = v;
        }
        any = true;
    }
    if (agg != MV_AGG_NONE) {
        char pn[96];
        mv_proc_name(pn, sizeof(pn), name);
        header(w, pn, help, type);
        mw_addf(w, "%s %llu\n", pn, acc);
    }
}

/* -- getters (core) -- */
static unsigned long long mvg_ingested(const moqr_snapshot_view_t *v)
{ return v->core->ingested_total; }
static unsigned long long mvg_delivered(const moqr_snapshot_view_t *v)
{ return v->core->delivered_total; }
static unsigned long long mvg_evicted(const moqr_snapshot_view_t *v)
{ return v->core->evicted_total; }
static unsigned long long mvg_tracks(const moqr_snapshot_view_t *v)
{ return v->core->tracks; }
static unsigned long long mvg_ns_nodes(const moqr_snapshot_view_t *v)
{ return v->core->ns_nodes; }
static unsigned long long mvg_retained(const moqr_snapshot_view_t *v)
{ return v->core->retained_bytes; }
static unsigned long long mvg_intent_hwm(const moqr_snapshot_view_t *v)
{ return v->core->intent_highwater; }
static unsigned long long mvg_route_epoch(const moqr_snapshot_view_t *v)
{ return v->core->route_epoch; }
/* User-facing gauges: the manager's own entities subtracted, like from
 * like (the non-negative invariant is checked before anything renders). */
static unsigned long long mvg_bindings(const moqr_snapshot_view_t *v)
{
    uint32_t internal = v->shard != NULL ? v->shard->internal_bindings : 0;
    return v->core->bindings - internal;
}
static unsigned long long mvg_ns_subs(const moqr_snapshot_view_t *v)
{
    uint32_t internal = v->shard != NULL ? v->shard->internal_ns_subs : 0;
    return v->core->ns_subs - internal;
}
/* -- getters (bind) -- */
static unsigned long long mvg_conns(const moqr_snapshot_view_t *v)
{ return v->bind->conns; }
static unsigned long long mvg_events(const moqr_snapshot_view_t *v)
{ return v->bind->events_translated; }
static unsigned long long mvg_writes(const moqr_snapshot_view_t *v)
{ return v->bind->deliveries_written; }
static unsigned long long mvg_ing_ref(const moqr_snapshot_view_t *v)
{ return v->bind->ingest_refusals; }
static unsigned long long mvg_sess_err(const moqr_snapshot_view_t *v)
{ return v->bind->session_errors; }
/* -- getters (shard) -- */
static unsigned long long mvg_pump_turns(const moqr_snapshot_view_t *v)
{ return v->shard->pump_turns; }
static unsigned long long mvg_pump_msgs(const moqr_snapshot_view_t *v)
{ return v->shard->pump_messages; }
static unsigned long long mvg_pump_bytes(const moqr_snapshot_view_t *v)
{ return v->shard->pump_bytes; }
static unsigned long long mvg_rd_refused(const moqr_snapshot_view_t *v)
{ return v->shard->remote_demand_refused; }
static unsigned long long mvg_rd_resolved(const moqr_snapshot_view_t *v)
{ return v->shard->remote_demand_resolved; }
static unsigned long long mvg_rd_rejected(const moqr_snapshot_view_t *v)
{ return v->shard->remote_data_rejected; }
static unsigned long long mvg_lane_wakes(const moqr_snapshot_view_t *v)
{ return v->lane_wakes; }
static unsigned long long mvg_pending(const moqr_snapshot_view_t *v)
{ return v->shard->pending_demands; }
static unsigned long long mvg_internal_b(const moqr_snapshot_view_t *v)
{ return v->shard->internal_bindings; }
static unsigned long long mvg_owner_slots(const moqr_snapshot_view_t *v)
{ return v->shard->owner_progress_slots; }
static unsigned long long mvg_req_open(const moqr_snapshot_view_t *v)
{ return v->shard->requester_open_objects; }
static unsigned long long mvg_mbox(const moqr_snapshot_view_t *v)
{ return v->shard->mailbox_pending; }
static unsigned long long mvg_ch_entries(const moqr_snapshot_view_t *v)
{ return v->shard->inbound_channel_entries; }
static unsigned long long mvg_ch_bytes(const moqr_snapshot_view_t *v)
{ return v->shard->inbound_channel_bytes; }
static unsigned long long mvg_jrn_epoch(const moqr_snapshot_view_t *v)
{ return v->shard->journal_epoch; }
static unsigned long long mvg_ch_e_hwm(const moqr_snapshot_view_t *v)
{ return v->shard->channel_entries_hwm; }
static unsigned long long mvg_ch_b_hwm(const moqr_snapshot_view_t *v)
{ return v->shard->channel_bytes_hwm; }

static moqr_result_t
write_multi(const moqr_snapshot_view_t *vs, uint32_t n, moqr_obs_format_t fmt,
            bool ext, char *buf, size_t cap, size_t *written, bool *poisoned)
{
    if (written != NULL) {
        *written = 0;
    }
    if (vs == NULL || n == 0 || n > MV_MAX || buf == NULL ||
        fmt >= MOQR_OBS_FMT__COUNT) {
        return MOQR_ERR_INVAL;
    }
    for (uint32_t i = 0; i < n; i++) {
        if (vs[i].core == NULL) {
            return MOQR_ERR_INVAL;
        }
    }
    /* The internal-entity exclusion invariant, checked BEFORE anything
     * renders: a subtraction that would go negative means the snapshot
     * contradicts itself (more internal entities than entities), so the
     * whole exposition is suppressed — never floored, never invented. */
    for (uint32_t i = 0; i < n; i++) {
        const moqr_shards_stats_t *sh = vs[i].shard;
        if (sh == NULL) {
            continue;
        }
        if (vs[i].core->subs_parked < sh->pump_subs_parked ||
            vs[i].core->subs_active < sh->pump_subs_active ||
            vs[i].core->bindings < sh->internal_bindings ||
            vs[i].core->ns_subs < sh->internal_ns_subs) {
            if (cap > 0) {
                buf[0] = '\0';
            }
            return MOQR_ERR_INVAL;
        }
    }

    char bases[MV_MAX][MV_BASE_CAP];
    for (uint32_t i = 0; i < n; i++) {
        char tr[64];
        char ver[64];
        esc_label(vs[i].labels.transport, tr, sizeof(tr));
        esc_label(vs[i].labels.version, ver, sizeof(ver));
        snprintf(bases[i], sizeof(bases[i]),
                 "shard=\"%u\",transport=\"%s\",version=\"%s\"",
                 (unsigned)vs[i].labels.shard, tr, ver);
    }

    /* Qualification through the array-pointer level is not an implicit
     * conversion in ISO C before C23: char (*)[N] does not convert to
     * const char (*)[N] the way char * converts to const char *. One explicit
     * add-const alias, taken after `bases` is fully populated, lets every
     * family call keep the callee's read-only contract without a cast at each
     * call site. */
    const char (*const cbases)[MV_BASE_CAP] =
        (const char (*)[MV_BASE_CAP])bases;

    mw_t w = { buf, cap, 0, fmt, ext, false };

    /* -- object flow (counters) -- */
    mv_family(&w, "moqrelay_objects_ingested_total",
              "Objects ingested from upstream publishers.", "counter", vs, n,
              cbases, NULL, mvg_ingested, MV_AGG_SUM);
    mv_family(&w, "moqrelay_objects_delivered_total",
              "Objects delivered to downstream subscribers.", "counter", vs,
              n, cbases, NULL, mvg_delivered, MV_AGG_SUM);
    mv_family(&w, "moqrelay_objects_evicted_total",
              "Object records evicted from track logs by retention.",
              "counter", vs, n, cbases, NULL, mvg_evicted, MV_AGG_SUM);

    /* -- admission refusals by resource -- */
    header(&w, "moqrelay_refusals_total",
           "Admission requests refused, by exhausted resource.", "counter");
    for (uint32_t i = 0; i < n; i++) {
        for (uint32_t r = 0; r < MOQR_REFUSE__COUNT; r++) {
            emit(&w, "moqrelay_refusals_total", bases[i], "reason",
                 moqr_refuse_reason_name(r),
                 (unsigned long long)vs[i].core->refusals[r]);
        }
    }
    header(&w, "moqrelay_process_refusals_total",
           "Admission requests refused, by exhausted resource.", "counter");
    for (uint32_t r = 0; r < MOQR_REFUSE__COUNT; r++) {
        unsigned long long acc = 0;
        for (uint32_t i = 0; i < n; i++) {
            acc += vs[i].core->refusals[r];
        }
        mw_addf(&w, "moqrelay_process_refusals_total{reason=\"%s\"} %llu\n",
                moqr_refuse_reason_name(r), acc);
    }

    /* -- authorization decisions / denials -- */
    header(&w, "moqrelay_auth_decisions_total",
           "Authorization decisions, by action and verdict.", "counter");
    for (uint32_t i = 0; i < n; i++) {
        for (uint32_t act = 0; act < MOQR_AUTH_ACTION__COUNT; act++) {
            for (uint32_t d = 0; d < 3u; d++) {
                emit2(&w, "moqrelay_auth_decisions_total", bases[i], "action",
                      moqr_auth_action_name(act), "verdict", verdict_name(d),
                      (unsigned long long)vs[i].core->auth_decisions[act][d]);
            }
        }
    }
    header(&w, "moqrelay_process_auth_decisions_total",
           "Authorization decisions, by action and verdict.", "counter");
    for (uint32_t act = 0; act < MOQR_AUTH_ACTION__COUNT; act++) {
        for (uint32_t d = 0; d < 3u; d++) {
            unsigned long long acc = 0;
            for (uint32_t i = 0; i < n; i++) {
                acc += vs[i].core->auth_decisions[act][d];
            }
            mw_addf(&w,
                    "moqrelay_process_auth_decisions_total{action=\"%s\","
                    "verdict=\"%s\"} %llu\n",
                    moqr_auth_action_name(act), verdict_name(d), acc);
        }
    }
    header(&w, "moqrelay_auth_denials_total",
           "Authorization denials, by reason.", "counter");
    for (uint32_t i = 0; i < n; i++) {
        for (uint32_t r = 0; r < MOQR_AUTH_REASON__COUNT; r++) {
            emit(&w, "moqrelay_auth_denials_total", bases[i], "reason",
                 moqr_auth_reason_name(r),
                 (unsigned long long)vs[i].core->auth_denials[r]);
        }
    }
    header(&w, "moqrelay_process_auth_denials_total",
           "Authorization denials, by reason.", "counter");
    for (uint32_t r = 0; r < MOQR_AUTH_REASON__COUNT; r++) {
        unsigned long long acc = 0;
        for (uint32_t i = 0; i < n; i++) {
            acc += vs[i].core->auth_denials[r];
        }
        mw_addf(&w,
                "moqrelay_process_auth_denials_total{reason=\"%s\"} %llu\n",
                moqr_auth_reason_name(r), acc);
    }

    /* -- live entities (gauges; user-facing after internal exclusion) -- */
    mv_family(&w, "moqrelay_bindings",
              "Live session bindings (manager-internal excluded).", "gauge",
              vs, n, cbases, NULL, mvg_bindings, MV_AGG_SUM);
    mv_family(&w, "moqrelay_tracks", "Live tracks.", "gauge", vs, n, cbases,
              NULL, mvg_tracks, MV_AGG_SUM);

    header(&w, "moqrelay_subscriptions",
           "Live downstream subscriptions, by state (pump subscriptions "
           "excluded).", "gauge");
    for (uint32_t i = 0; i < n; i++) {
        uint32_t pp = vs[i].shard != NULL ? vs[i].shard->pump_subs_parked : 0;
        uint32_t pa = vs[i].shard != NULL ? vs[i].shard->pump_subs_active : 0;
        emit(&w, "moqrelay_subscriptions", bases[i], "state", "parked",
             (unsigned long long)(vs[i].core->subs_parked - pp));
        emit(&w, "moqrelay_subscriptions", bases[i], "state", "active",
             (unsigned long long)(vs[i].core->subs_active - pa));
    }
    header(&w, "moqrelay_process_subscriptions",
           "Live downstream subscriptions, by state (pump subscriptions "
           "excluded).", "gauge");
    {
        unsigned long long parked = 0, active = 0;
        for (uint32_t i = 0; i < n; i++) {
            uint32_t pp =
                vs[i].shard != NULL ? vs[i].shard->pump_subs_parked : 0;
            uint32_t pa =
                vs[i].shard != NULL ? vs[i].shard->pump_subs_active : 0;
            parked += vs[i].core->subs_parked - pp;
            active += vs[i].core->subs_active - pa;
        }
        mw_addf(&w,
                "moqrelay_process_subscriptions{state=\"parked\"} %llu\n",
                parked);
        mw_addf(&w,
                "moqrelay_process_subscriptions{state=\"active\"} %llu\n",
                active);
    }

    mv_family(&w, "moqrelay_namespace_nodes", "Namespace trie nodes.",
              "gauge", vs, n, cbases, NULL, mvg_ns_nodes, MV_AGG_SUM);
    mv_family(&w, "moqrelay_namespace_subscriptions",
              "Live namespace (prefix) subscriptions (manager watcher "
              "excluded).", "gauge", vs, n, cbases, NULL, mvg_ns_subs,
              MV_AGG_SUM);
    mv_family(&w, "moqrelay_retained_bytes",
              "Payload and property bytes retained across track logs.",
              "gauge", vs, n, cbases, NULL, mvg_retained, MV_AGG_SUM);
    mv_family(&w, "moqrelay_intent_ring_highwater",
              "Peak intent-ring occupancy since start.", "gauge", vs, n,
              cbases, NULL, mvg_intent_hwm, MV_AGG_MAX);
    /* Per-shard identity: never aggregated. */
    mv_family(&w, "moqrelay_route_epoch",
              "Monotonic route-mutation epoch (this shard).", "gauge", vs, n,
              cbases, NULL, mvg_route_epoch, MV_AGG_NONE);

    /* -- binding-tier -- */
    mv_family(&w, "moqrelay_connections", "Attached transport connections.",
              "gauge", vs, n, cbases, mv_has_bind, mvg_conns, MV_AGG_SUM);
    mv_family(&w, "moqrelay_events_translated_total",
              "Session events translated into core inputs.", "counter", vs,
              n, cbases, mv_has_bind, mvg_events, MV_AGG_SUM);
    mv_family(&w, "moqrelay_deliveries_written_total",
              "Deliveries written to downstream sessions.", "counter", vs, n,
              cbases, mv_has_bind, mvg_writes, MV_AGG_SUM);
    mv_family(&w, "moqrelay_ingest_refusals_total",
              "Ingest records refused as too old or out of order.",
              "counter", vs, n, cbases, mv_has_bind, mvg_ing_ref, MV_AGG_SUM);
    mv_family(&w, "moqrelay_session_errors_total",
              "Unexpected session-call failures.", "counter", vs, n, cbases,
              mv_has_bind, mvg_sess_err, MV_AGG_SUM);

    /* Forward-latency histogram: per-shard series, then the process
     * aggregate with buckets/count/sum SUMMED (cumulated per the
     * exposition shape). */
    {
        bool any_bind = false;
        for (uint32_t i = 0; i < n; i++) {
            any_bind = any_bind || vs[i].bind != NULL;
        }
        if (any_bind) {
            header(&w, "moqrelay_forward_latency_seconds",
                   "Object ingest-to-delivery latency (successful writes "
                   "only).", "histogram");
            for (uint32_t i = 0; i < n; i++) {
                if (vs[i].bind == NULL) {
                    continue;
                }
                const moqr_latency_hist_t *h = &vs[i].bind->forward_latency;
                unsigned long long cum = 0;
                for (uint32_t b = 0; b < MOQR_LATENCY_BUCKETS; b++) {
                    cum += (unsigned long long)h->bucket[b];
                    uint64_t b_us = moqr_latency_bound_us(b);
                    char le[32];
                    snprintf(le, sizeof(le), "%llu.%06llu",
                             (unsigned long long)(b_us / 1000000u),
                             (unsigned long long)(b_us % 1000000u));
                    emit(&w, "moqrelay_forward_latency_seconds_bucket",
                         bases[i], "le", le, cum);
                }
                cum += (unsigned long long)h->bucket[MOQR_LATENCY_BUCKETS];
                emit(&w, "moqrelay_forward_latency_seconds_bucket", bases[i],
                     "le", "+Inf", cum);
                mw_addf(&w,
                        "moqrelay_forward_latency_seconds_sum{%s} "
                        "%llu.%06llu\n",
                        bases[i], (unsigned long long)(h->sum_us / 1000000u),
                        (unsigned long long)(h->sum_us % 1000000u));
                mw_addf(&w,
                        "moqrelay_forward_latency_seconds_count{%s} %llu\n",
                        bases[i], (unsigned long long)h->count);
            }
            header(&w, "moqrelay_process_forward_latency_seconds",
                   "Object ingest-to-delivery latency (successful writes "
                   "only).", "histogram");
            unsigned long long cum = 0;
            unsigned long long sum_us = 0, count = 0;
            for (uint32_t b = 0; b < MOQR_LATENCY_BUCKETS; b++) {
                for (uint32_t i = 0; i < n; i++) {
                    if (vs[i].bind != NULL) {
                        cum += (unsigned long long)
                                   vs[i].bind->forward_latency.bucket[b];
                    }
                }
                uint64_t b_us = moqr_latency_bound_us(b);
                mw_addf(&w,
                        "moqrelay_process_forward_latency_seconds_bucket"
                        "{le=\"%llu.%06llu\"} %llu\n",
                        (unsigned long long)(b_us / 1000000u),
                        (unsigned long long)(b_us % 1000000u), cum);
            }
            for (uint32_t i = 0; i < n; i++) {
                if (vs[i].bind == NULL) {
                    continue;
                }
                const moqr_latency_hist_t *h = &vs[i].bind->forward_latency;
                cum += (unsigned long long)h->bucket[MOQR_LATENCY_BUCKETS];
                sum_us += (unsigned long long)h->sum_us;
                count += (unsigned long long)h->count;
            }
            mw_addf(&w,
                    "moqrelay_process_forward_latency_seconds_bucket"
                    "{le=\"+Inf\"} %llu\n",
                    cum);
            mw_addf(&w,
                    "moqrelay_process_forward_latency_seconds_sum "
                    "%llu.%06llu\n",
                    sum_us / 1000000u, sum_us % 1000000u);
            mw_addf(&w,
                    "moqrelay_process_forward_latency_seconds_count %llu\n",
                    count);
        }
    }

    /* -- cross-shard plane -- */
    mv_family(&w, "moqrelay_pump_turns_total",
              "Data-pump turns (extraction attempts) as the owner.",
              "counter", vs, n, cbases, mv_has_shard, mvg_pump_turns,
              MV_AGG_SUM);
    mv_family(&w, "moqrelay_pump_messages_total",
              "Data messages pushed across shard boundaries.", "counter", vs,
              n, cbases, mv_has_shard, mvg_pump_msgs, MV_AGG_SUM);
    mv_family(&w, "moqrelay_pump_bytes_total",
              "Logical bytes pushed by the data pump.", "counter", vs, n,
              cbases, mv_has_shard, mvg_pump_bytes, MV_AGG_SUM);

    {
        bool any_shard = false;
        for (uint32_t i = 0; i < n; i++) {
            any_shard = any_shard || vs[i].shard != NULL;
        }
        if (any_shard) {
            header(&w, "moqrelay_channel_enqueues_total",
                   "Durable demand-channel enqueues, by message kind.",
                   "counter");
            for (uint32_t i = 0; i < n; i++) {
                if (vs[i].shard == NULL) {
                    continue;
                }
                for (uint32_t k = 0; k < MOQR_SHARDS_MSG__COUNT; k++) {
                    emit(&w, "moqrelay_channel_enqueues_total", bases[i],
                         "kind", moqr_shards_msg_kind_name(k),
                         (unsigned long long)vs[i].shard->enqueued[k]);
                }
            }
            header(&w, "moqrelay_process_channel_enqueues_total",
                   "Durable demand-channel enqueues, by message kind.",
                   "counter");
            for (uint32_t k = 0; k < MOQR_SHARDS_MSG__COUNT; k++) {
                unsigned long long acc = 0;
                for (uint32_t i = 0; i < n; i++) {
                    if (vs[i].shard != NULL) {
                        acc += vs[i].shard->enqueued[k];
                    }
                }
                mw_addf(&w,
                        "moqrelay_process_channel_enqueues_total"
                        "{kind=\"%s\"} %llu\n",
                        moqr_shards_msg_kind_name(k), acc);
            }
        }
    }

    mv_family(&w, "moqrelay_remote_demand_refused_total",
              "Remote demands resolved as refused (requester side).",
              "counter", vs, n, cbases, mv_has_shard, mvg_rd_refused,
              MV_AGG_SUM);
    mv_family(&w, "moqrelay_remote_demand_resolved_total",
              "Owner DONE answers resolved as the requester.", "counter", vs,
              n, cbases, mv_has_shard, mvg_rd_resolved, MV_AGG_SUM);
    mv_family(&w, "moqrelay_remote_data_rejected_total",
              "Cross-shard records refused at ingest (loss-visible).",
              "counter", vs, n, cbases, mv_has_shard, mvg_rd_rejected,
              MV_AGG_SUM);

    {
        bool any_shard = false;
        for (uint32_t i = 0; i < n; i++) {
            any_shard = any_shard || vs[i].shard != NULL;
        }
        if (any_shard) {
            header(&w, "moqrelay_remote_demand_terminals_total",
                   "Remote demands terminated locally, by cause.",
                   "counter");
            for (uint32_t i = 0; i < n; i++) {
                if (vs[i].shard == NULL) {
                    continue;
                }
                emit(&w, "moqrelay_remote_demand_terminals_total", bases[i],
                     "reason", "capacity",
                     (unsigned long long)vs[i].shard->term_capacity);
                emit(&w, "moqrelay_remote_demand_terminals_total", bases[i],
                     "reason", "overrun",
                     (unsigned long long)vs[i].shard->term_overrun);
            }
            header(&w, "moqrelay_process_remote_demand_terminals_total",
                   "Remote demands terminated locally, by cause.",
                   "counter");
            unsigned long long tc = 0, to = 0;
            for (uint32_t i = 0; i < n; i++) {
                if (vs[i].shard != NULL) {
                    tc += vs[i].shard->term_capacity;
                    to += vs[i].shard->term_overrun;
                }
            }
            mw_addf(&w,
                    "moqrelay_process_remote_demand_terminals_total"
                    "{reason=\"capacity\"} %llu\n",
                    tc);
            mw_addf(&w,
                    "moqrelay_process_remote_demand_terminals_total"
                    "{reason=\"overrun\"} %llu\n",
                    to);

            /* The legacy help text says "cross-shard", which stopped
             * being true once the local continuation cause joined the
             * family: a local self-wake crosses nothing. The frozen legacy
             * exposition keeps its accepted wording; the extended one states
             * what the family now actually counts. */
            header(&w, "moqrelay_wake_requests_total",
                   w.ext ? "Wake requests, by cause (mask-level: one per "
                           "destination per step; the local cause is a "
                           "self-wake and crosses no shard)."
                         : "Cross-shard wake requests, by cause (mask-level: "
                           "one per destination per step).",
                   "counter");
            for (uint32_t i = 0; i < n; i++) {
                if (vs[i].shard == NULL) {
                    continue;
                }
                emit(&w, "moqrelay_wake_requests_total", bases[i], "cause",
                     "push",
                     (unsigned long long)vs[i].shard->wake_requests_push);
                emit(&w, "moqrelay_wake_requests_total", bases[i], "cause",
                     "credit",
                     (unsigned long long)vs[i].shard->wake_requests_credit);
                if (w.ext) {
                    emit(&w, "moqrelay_wake_requests_total", bases[i],
                         "cause", "local",
                         (unsigned long long)vs[i].shard->wake_requests_local);
                }
            }
            header(&w, "moqrelay_process_wake_requests_total",
                   w.ext ? "Wake requests, by cause (mask-level: one per "
                           "destination per step; the local cause is a "
                           "self-wake and crosses no shard)."
                         : "Cross-shard wake requests, by cause (mask-level: "
                           "one per destination per step).",
                   "counter");
            unsigned long long wp = 0, wc = 0, wl = 0;
            for (uint32_t i = 0; i < n; i++) {
                if (vs[i].shard != NULL) {
                    wp += vs[i].shard->wake_requests_push;
                    wc += vs[i].shard->wake_requests_credit;
                    wl += vs[i].shard->wake_requests_local;
                }
            }
            mw_addf(&w,
                    "moqrelay_process_wake_requests_total{cause=\"push\"} "
                    "%llu\n",
                    wp);
            mw_addf(&w,
                    "moqrelay_process_wake_requests_total{cause=\"credit\"} "
                    "%llu\n",
                    wc);
            if (w.ext) {
                mw_addf(&w,
                        "moqrelay_process_wake_requests_total{cause=\"local\"} "
                        "%llu\n",
                        wl);
            }
        }
    }

    /* The CLI's merged-mask counter: ACTUAL lane_wake calls, deliberately
     * a separate series from the mask-level wake_requests above. */
    mv_family(&w, "moqrelay_lane_wakes_total",
              "Actual lane wake calls issued by the runtime (merged mask).",
              "counter", vs, n, cbases, mv_has_shard, mvg_lane_wakes,
              MV_AGG_SUM);

    mv_family(&w, "moqrelay_pending_demands",
              "Recorded remote-owner demands (requester side).", "gauge", vs,
              n, cbases, mv_has_shard, mvg_pending, MV_AGG_SUM);

    {
        bool any_shard = false;
        for (uint32_t i = 0; i < n; i++) {
            any_shard = any_shard || vs[i].shard != NULL;
        }
        if (any_shard) {
            header(&w, "moqrelay_pump_subscriptions",
                   "Manager-internal pump subscriptions, by state.",
                   "gauge");
            for (uint32_t i = 0; i < n; i++) {
                if (vs[i].shard == NULL) {
                    continue;
                }
                emit(&w, "moqrelay_pump_subscriptions", bases[i], "state",
                     "parked",
                     (unsigned long long)vs[i].shard->pump_subs_parked);
                emit(&w, "moqrelay_pump_subscriptions", bases[i], "state",
                     "active",
                     (unsigned long long)vs[i].shard->pump_subs_active);
            }
            header(&w, "moqrelay_process_pump_subscriptions",
                   "Manager-internal pump subscriptions, by state.",
                   "gauge");
            unsigned long long pp = 0, pa = 0;
            for (uint32_t i = 0; i < n; i++) {
                if (vs[i].shard != NULL) {
                    pp += vs[i].shard->pump_subs_parked;
                    pa += vs[i].shard->pump_subs_active;
                }
            }
            mw_addf(&w,
                    "moqrelay_process_pump_subscriptions{state=\"parked\"} "
                    "%llu\n",
                    pp);
            mw_addf(&w,
                    "moqrelay_process_pump_subscriptions{state=\"active\"} "
                    "%llu\n",
                    pa);
        }
    }

    mv_family(&w, "moqrelay_internal_bindings",
              "Manager-owned binding slots inside the shard core.", "gauge",
              vs, n, cbases, mv_has_shard, mvg_internal_b, MV_AGG_SUM);
    mv_family(&w, "moqrelay_owner_progress_slots",
              "Live owner-side chunk-resume cursors.", "gauge", vs, n, cbases,
              mv_has_shard, mvg_owner_slots, MV_AGG_SUM);
    mv_family(&w, "moqrelay_requester_open_objects",
              "Live requester-side open-object slots.", "gauge", vs, n,
              cbases, mv_has_shard, mvg_req_open, MV_AGG_SUM);
    mv_family(&w, "moqrelay_mailbox_pending",
              "Inbound control-mailbox slots occupied.", "gauge", vs, n,
              cbases, mv_has_shard, mvg_mbox, MV_AGG_SUM);
    mv_family(&w, "moqrelay_demand_channel_entries",
              "Inbound demand-channel entries queued.", "gauge", vs, n,
              cbases, mv_has_shard, mvg_ch_entries, MV_AGG_SUM);
    mv_family(&w, "moqrelay_demand_channel_bytes",
              "Inbound demand-channel logical bytes queued.", "gauge", vs, n,
              cbases, mv_has_shard, mvg_ch_bytes, MV_AGG_SUM);
    /* Per-shard identity: never aggregated. */
    mv_family(&w, "moqrelay_journal_epoch",
              "Journal projection generation (this shard).", "gauge", vs, n,
              cbases, mv_has_shard, mvg_jrn_epoch, MV_AGG_NONE);
    mv_family(&w, "moqrelay_demand_channel_entries_highwater",
              "Peak inbound demand-channel entries.", "gauge", vs, n, cbases,
              mv_has_shard, mvg_ch_e_hwm, MV_AGG_MAX);
    mv_family(&w, "moqrelay_demand_channel_bytes_highwater",
              "Peak inbound demand-channel logical bytes.", "gauge", vs, n,
              cbases, mv_has_shard, mvg_ch_b_hwm, MV_AGG_MAX);

    finish(&w);

    return mw_finish(&w, buf, cap, written, poisoned);
}

/* -- entry points --------------------------------------------------------- */

/*
 * Two families of entry point, and the difference between them is deliberate.
 *
 * The legacy pair renders the FROZEN exposition: exactly the bytes the
 * accepted build emitted, for consumers already parsing them. It is closed to
 * new series.
 *
 * The `_ex` pair renders the ADMIN-FACING exposition: the same document plus
 * extended families, in the requested format. `_ex` with
 * MOQR_OBS_FMT_PROMETHEUS_004 is therefore NOT byte-equal to the legacy
 * writer, and must not be — that is how a new series reaches a scrape without
 * moving ground under an existing consumer. Frozen goldens pin the legacy
 * bytes; the format tests pin the extended ones.
 */
moqr_result_t
moqr_metrics_write_prometheus(const moqr_core_stats_t *core,
                              const moqr_bind_stats_t *bind,
                              const moqr_obs_labels_t *labels, char *buf,
                              size_t cap, size_t *written)
{
    return write_single(core, bind, labels, MOQR_OBS_FMT_PROMETHEUS_004, false,
                        buf, cap, written);
}

moqr_result_t
moqr_metrics_write_prometheus_multi(const moqr_snapshot_view_t *vs, uint32_t n,
                                    char *buf, size_t cap, size_t *written)
{
    return write_multi(vs, n, MOQR_OBS_FMT_PROMETHEUS_004, false, buf, cap,
                       written, NULL);
}

moqr_result_t
moqr_metrics_write_ex(const moqr_core_stats_t *core,
                      const moqr_bind_stats_t *bind,
                      const moqr_obs_labels_t *labels, moqr_obs_format_t fmt,
                      char *buf, size_t cap, size_t *written)
{
    return write_single(core, bind, labels, fmt, true, buf, cap, written);
}

moqr_result_t
moqr_metrics_write_multi_ex(const moqr_snapshot_view_t *vs, uint32_t n,
                            moqr_obs_format_t fmt, char *buf, size_t cap,
                            size_t *written)
{
    return write_multi(vs, n, fmt, true, buf, cap, written, NULL);
}

/* -- worst-case exposition size ------------------------------------------- */

/*
 * The bound is computed BY THE RENDERER, against a synthetic snapshot chosen
 * to maximize every rendered field width. Deriving it any other way would let
 * a hand-maintained arithmetic model drift away from the document the code
 * actually emits; here the two cannot disagree, because they are the same
 * code path.
 *
 * Choosing the worst case needs one non-obvious step. Several gauges are
 * rendered as `core minus internal` while the internal is rendered on its own
 * line, so the pair is zero-sum: saturating both operands makes the
 * difference render as a single `0` digit, and saturating neither makes the
 * internal render short instead. Neither extreme is the widest. Setting each
 * internal to half its saturated value makes the difference AND the internal
 * both render at full width, which is the true maximum.
 */
static void
bound_worst_case(moqr_core_stats_t *core, moqr_bind_stats_t *bind,
                 moqr_shards_stats_t *shard)
{
    memset(core, 0xff, sizeof(*core));
    memset(bind, 0xff, sizeof(*bind));
    memset(shard, 0xff, sizeof(*shard));
    shard->pump_subs_parked = core->subs_parked / 2u;
    shard->pump_subs_active = core->subs_active / 2u;
    shard->internal_bindings = core->bindings / 2u;
    shard->internal_ns_subs = core->ns_subs / 2u;
}

/*
 * Decide the bound from one measuring render.
 *
 * Truncation against the one-byte sink is the ORDINARY outcome here and
 * carries the exact length. A poisoned accumulation reports the same
 * MOQR_ERR_CAPACITY but its length is meaningless, so the two must not be
 * conflated: collapsing them would hand a caller a zero-byte allocation for a
 * document that could not be measured. Poison saturates and refuses.
 */
static moqr_result_t
bound_finalize(moqr_result_t rc, bool poisoned, size_t need,
               uint64_t *out_bytes)
{
    if (poisoned) {
        *out_bytes = UINT64_MAX;
        return MOQR_ERR_CAPACITY;
    }
    if (rc != MOQR_OK && rc != MOQR_ERR_CAPACITY) {
        return rc;
    }
    *out_bytes = (uint64_t)need;
    return MOQR_OK;
}

moqr_result_t
moqr_metrics_bound(uint32_t lanes, moqr_obs_format_t fmt, bool with_bind,
                   bool with_shard, uint64_t *out_bytes)
{
    if (out_bytes == NULL || lanes == 0 || lanes > MV_MAX ||
        fmt >= MOQR_OBS_FMT__COUNT) {
        return MOQR_ERR_INVAL;
    }
    *out_bytes = 0;

    static const char worst_label[] =
        "\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\""
        "\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"";

    moqr_core_stats_t   core;
    moqr_bind_stats_t   bind;
    moqr_shards_stats_t shard;
    bound_worst_case(&core, &bind, &shard);

    /* Caller-local, never static: a shared workspace would both race
     * between concurrent callers and briefly retain pointers to another
     * call's stack snapshots. MV_MAX views is a few kilobytes. */
    moqr_snapshot_view_t vs[MV_MAX];
    memset(vs, 0, sizeof(vs));
    for (uint32_t i = 0; i < lanes; i++) {
        vs[i].core = &core;
        vs[i].bind = with_bind ? &bind : NULL;
        vs[i].shard = with_shard ? &shard : NULL;
        vs[i].lane_wakes = UINT64_MAX;
        /* Widest label block: the highest shard number, and tokens whose
         * every byte escapes to two (esc_label caps the expansion, so this
         * saturates the escaped form rather than merely being long). */
        vs[i].labels.shard = UINT16_MAX;
        vs[i].labels.transport = worst_label;
        vs[i].labels.version = worst_label;
    }

    /* A one-byte sink: the writer still accumulates the full required length
     * past its capacity, which is exactly the number we want. */
    char   sink = 0;
    size_t need = 0;
    bool   poisoned = false;
    moqr_result_t rc =
        write_multi(vs, lanes, fmt, true, &sink, 1u, &need, &poisoned);
    return bound_finalize(rc, poisoned, need, out_bytes);
}
