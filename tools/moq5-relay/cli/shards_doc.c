#include "shards_doc.h"

#include "../admin/json_out.h"
#include "../shard/moqr_shards.h"

#include <string.h>

/* -- the finite field mapping -------------------------------------------------
 *
 * Each table row is one JSON field name and the copied source field it is
 * read from, verbatim and unscaled. Nothing is enumerated from a struct. */
typedef struct core_field {
    const char *name;
    uint64_t  (*get)(const moqr_core_stats_t *c);
} core_field_t;
typedef struct bind_field {
    const char *name;
    uint64_t  (*get)(const moqr_bind_stats_t *b);
} bind_field_t;
typedef struct shard_field {
    const char *name;
    uint64_t  (*get)(const moqr_shards_stats_t *s);
} shard_field_t;

#define CF(field) static uint64_t cf_##field(const moqr_core_stats_t *c) { return c->field; }
#define BF(field) static uint64_t bf_##field(const moqr_bind_stats_t *b) { return b->field; }
#define SF(field) static uint64_t sf_##field(const moqr_shards_stats_t *s) { return s->field; }

CF(bindings) CF(tracks) CF(subs) CF(subs_parked) CF(subs_active) CF(ns_nodes)
CF(ns_subs) CF(retained_bytes) CF(ingested_total) CF(delivered_total)
CF(evict_sweeps) CF(evicted_total) CF(note_emitted_total) CF(intent_highwater)
CF(route_epoch)

BF(conns) BF(events_translated) BF(deliveries_written) BF(ingest_refusals)
BF(session_errors) BF(pending_high_water) BF(pending_nonscalar_blocked)
BF(nsu_high_water) BF(nsu_live) BF(nsu_retained) BF(nsu_failed_closed)
BF(ordered_failed_closed)

SF(pump_turns) SF(pump_messages) SF(pump_bytes) SF(remote_demand_refused)
SF(remote_demand_resolved) SF(remote_data_rejected) SF(term_capacity)
SF(term_overrun) SF(wake_requests_push) SF(wake_requests_credit)
SF(wake_requests_local) SF(turns_msg_budget) SF(turns_byte_budget)
SF(turns_blocked) SF(turns_drained) SF(turns_with_messages)
SF(arb_class_refusals) SF(pending_demands) SF(pump_subs_parked)
SF(pump_subs_active) SF(owner_progress_slots) SF(requester_open_objects)
SF(internal_bindings) SF(internal_ns_subs) SF(mailbox_pending)
SF(inbound_channel_entries) SF(inbound_channel_bytes) SF(journal_epoch)
SF(channel_entries_hwm) SF(channel_bytes_hwm)

static const core_field_t k_core[] = {
    { "bindings", cf_bindings }, { "tracks", cf_tracks }, { "subs", cf_subs },
    { "subs_parked", cf_subs_parked }, { "subs_active", cf_subs_active },
    { "ns_nodes", cf_ns_nodes }, { "ns_subs", cf_ns_subs },
    { "retained_bytes", cf_retained_bytes },
    { "ingested_total", cf_ingested_total },
    { "delivered_total", cf_delivered_total },
    { "evict_sweeps", cf_evict_sweeps }, { "evicted_total", cf_evicted_total },
    { "note_emitted_total", cf_note_emitted_total },
    { "intent_highwater", cf_intent_highwater },
    { "route_epoch", cf_route_epoch },
};
static const bind_field_t k_bind[] = {
    { "conns", bf_conns }, { "events_translated", bf_events_translated },
    { "deliveries_written", bf_deliveries_written },
    { "ingest_refusals", bf_ingest_refusals },
    { "session_errors", bf_session_errors },
    { "pending_high_water", bf_pending_high_water },
    { "pending_nonscalar_blocked", bf_pending_nonscalar_blocked },
    { "nsu_high_water", bf_nsu_high_water }, { "nsu_live", bf_nsu_live },
    { "nsu_retained", bf_nsu_retained },
    { "nsu_failed_closed", bf_nsu_failed_closed },
    { "ordered_failed_closed", bf_ordered_failed_closed },
};
static const shard_field_t k_shard[] = {
    { "pump_turns", sf_pump_turns }, { "pump_messages", sf_pump_messages },
    { "pump_bytes", sf_pump_bytes },
    { "remote_demand_refused", sf_remote_demand_refused },
    { "remote_demand_resolved", sf_remote_demand_resolved },
    { "remote_data_rejected", sf_remote_data_rejected },
    { "term_capacity", sf_term_capacity }, { "term_overrun", sf_term_overrun },
    { "wake_requests_push", sf_wake_requests_push },
    { "wake_requests_credit", sf_wake_requests_credit },
    { "wake_requests_local", sf_wake_requests_local },
    { "turns_msg_budget", sf_turns_msg_budget },
    { "turns_byte_budget", sf_turns_byte_budget },
    { "turns_blocked", sf_turns_blocked }, { "turns_drained", sf_turns_drained },
    { "turns_with_messages", sf_turns_with_messages },
    { "arb_class_refusals", sf_arb_class_refusals },
    { "pending_demands", sf_pending_demands },
    { "pump_subs_parked", sf_pump_subs_parked },
    { "pump_subs_active", sf_pump_subs_active },
    { "owner_progress_slots", sf_owner_progress_slots },
    { "requester_open_objects", sf_requester_open_objects },
    { "internal_bindings", sf_internal_bindings },
    { "internal_ns_subs", sf_internal_ns_subs },
    { "mailbox_pending", sf_mailbox_pending },
    { "inbound_channel_entries", sf_inbound_channel_entries },
    { "inbound_channel_bytes", sf_inbound_channel_bytes },
    { "journal_epoch", sf_journal_epoch },
    { "channel_entries_hwm", sf_channel_entries_hwm },
    { "channel_bytes_hwm", sf_channel_bytes_hwm },
};

#define N(a) (sizeof(a) / sizeof((a)[0]))

/* The listener kind is derived from the closed transport label set the
 * coordinator constructs; an unknown label is a refusal, never a guess. */
static const char *
listener_kind(const char *transport)
{
    if (strcmp(transport, "msquic") == 0) {
        return "raw";
    }
    if (strcmp(transport, "wtquic-msquic") == 0) {
        return "webtransport";
    }
    return NULL;
}

static bool
row_is_consistent(const moqr_snapshot_view_t *v)
{
    /* The same contradiction the metrics renderer refuses: more internal
     * entities than entities. Never floored, never invented. */
    const moqr_shards_stats_t *sh = v->shard;
    if (sh == NULL) {
        return true;
    }
    return v->core->subs_parked >= sh->pump_subs_parked &&
           v->core->subs_active >= sh->pump_subs_active &&
           v->core->bindings >= sh->internal_bindings &&
           v->core->ns_subs >= sh->internal_ns_subs;
}

static bool
write_doc(moqr_json_w_t *w, const moqr_snapshot_view_t *views, uint32_t n,
          uint64_t serial)
{
    moqr_json_object_begin(w);
    moqr_json_key(w, "api");   moqr_json_str(w, "v1", 2);
    moqr_json_key(w, "epoch"); moqr_json_u64(w, serial);
    moqr_json_key(w, "shards");
    moqr_json_array_begin(w);
    for (uint32_t i = 0; i < n; i++) {
        const moqr_snapshot_view_t *v = &views[i];
        const char *kind;
        if (v->core == NULL || v->bind == NULL ||
            v->labels.transport == NULL || v->labels.version == NULL) {
            return false;
        }
        kind = listener_kind(v->labels.transport);
        if (kind == NULL || !row_is_consistent(v)) {
            return false;
        }
        moqr_json_object_begin(w);
        moqr_json_key(w, "shard");     moqr_json_u64(w, v->labels.shard);
        moqr_json_key(w, "transport");
        moqr_json_str(w, v->labels.transport, strlen(v->labels.transport));
        /* The offered-set label the metrics carry, not a negotiated draft. */
        moqr_json_key(w, "version");
        moqr_json_str(w, v->labels.version, strlen(v->labels.version));
        moqr_json_key(w, "listener");  moqr_json_str(w, kind, strlen(kind));
        moqr_json_key(w, "capability");
        moqr_json_str(w, v->shard != NULL ? "valid" : "absent",
                      v->shard != NULL ? 5 : 6);
        moqr_json_key(w, "core");
        moqr_json_object_begin(w);
        for (size_t f = 0; f < N(k_core); f++) {
            moqr_json_key(w, k_core[f].name);
            moqr_json_u64(w, k_core[f].get(v->core));
        }
        moqr_json_object_end(w);
        moqr_json_key(w, "bind");
        moqr_json_object_begin(w);
        for (size_t f = 0; f < N(k_bind); f++) {
            moqr_json_key(w, k_bind[f].name);
            moqr_json_u64(w, k_bind[f].get(v->bind));
        }
        moqr_json_object_end(w);
        if (v->shard != NULL) {
            /* Present only with the shard plane: never fabricated. */
            moqr_json_key(w, "shard_plane");
            moqr_json_object_begin(w);
            for (size_t f = 0; f < N(k_shard); f++) {
                moqr_json_key(w, k_shard[f].name);
                moqr_json_u64(w, k_shard[f].get(v->shard));
            }
            moqr_json_key(w, "lane_wakes"); moqr_json_u64(w, v->lane_wakes);
            moqr_json_object_end(w);
        }
        moqr_json_object_end(w);
    }
    moqr_json_array_end(w);
    moqr_json_object_end(w);
    return true;
}

moqr_result_t
moqr_cli_shards_render(const moqr_snapshot_view_t *views, uint32_t n,
                       uint64_t serial, char *buf, size_t cap, size_t *out_len)
{
    moqr_json_w_t w;
    size_t len = 0;

    if (out_len != NULL) {
        *out_len = 0;
    }
    if (buf != NULL && cap > 0u) {
        buf[0] = '\0';
    }
    if (views == NULL || n == 0u || n > MOQR_SHARDS_MAX || buf == NULL ||
        cap == 0u) {
        return MOQR_ERR_INVAL;
    }
    moqr_json_begin(&w, buf, cap);
    if (!write_doc(&w, views, n, serial)) {
        buf[0] = '\0';
        return MOQR_ERR_INVAL;
    }
    if (!moqr_json_end(&w, &len)) {
        moqr_json_w_t count;
        moqr_json_begin(&count, NULL, 0);
        (void)write_doc(&count, views, n, serial);
        return moqr_json_end(&count, NULL) ? MOQR_ERR_CAPACITY
                                           : MOQR_ERR_INVAL;
    }
    if (out_len != NULL) {
        *out_len = len;
    }
    return MOQR_OK;
}

/* -- the bound ---------------------------------------------------------------
 *
 * Immutable synthetic maximum: every counter at UINT64_MAX / UINT32_MAX (the
 * widest each type prints), the shard number at UINT16_MAX, every row valid
 * (the widest row shape), and both labels as long as the coordinator can make
 * them, made of a control byte (six output bytes each): the transport label
 * is one of two constants (13 bytes at most) and the version label is the
 * offered-set label (MOQR_CLI_MAX_VERSIONS * 17 - 1 = 67 bytes at most).
 * Nothing here is written to. */
#define C8  "\x1f\x1f\x1f\x1f\x1f\x1f\x1f\x1f"
static const char k_transport_max[] = C8 "\x1f\x1f\x1f\x1f\x1f";          /* 13 */
static const char k_version_max[] =
    C8 C8 C8 C8 C8 C8 C8 C8 "\x1f\x1f\x1f";                              /* 67 */

static const moqr_core_stats_t k_core_max = {
    .bindings = UINT32_MAX, .tracks = UINT32_MAX, .subs = UINT32_MAX,
    .subs_parked = UINT32_MAX, .subs_active = UINT32_MAX,
    .ns_nodes = UINT32_MAX, .ns_subs = UINT32_MAX,
    .retained_bytes = UINT64_MAX, .ingested_total = UINT64_MAX,
    .delivered_total = UINT64_MAX, .evict_sweeps = UINT64_MAX,
    .evicted_total = UINT64_MAX, .note_emitted_total = UINT64_MAX,
    .intent_highwater = UINT32_MAX, .route_epoch = UINT64_MAX,
};
static const moqr_bind_stats_t k_bind_max = {
    .conns = UINT32_MAX, .events_translated = UINT64_MAX,
    .deliveries_written = UINT64_MAX, .ingest_refusals = UINT64_MAX,
    .session_errors = UINT64_MAX, .pending_high_water = UINT32_MAX,
    .pending_nonscalar_blocked = UINT32_MAX, .nsu_high_water = UINT32_MAX,
    .nsu_live = UINT32_MAX, .nsu_retained = UINT64_MAX,
    .nsu_failed_closed = UINT64_MAX, .ordered_failed_closed = UINT64_MAX,
};
static const moqr_shards_stats_t k_shard_max = {
    .pump_turns = UINT64_MAX, .pump_messages = UINT64_MAX,
    .pump_bytes = UINT64_MAX, .remote_demand_refused = UINT64_MAX,
    .remote_demand_resolved = UINT64_MAX, .remote_data_rejected = UINT64_MAX,
    .term_capacity = UINT64_MAX, .term_overrun = UINT64_MAX,
    .wake_requests_push = UINT64_MAX, .wake_requests_credit = UINT64_MAX,
    .wake_requests_local = UINT64_MAX, .turns_msg_budget = UINT64_MAX,
    .turns_byte_budget = UINT64_MAX, .turns_blocked = UINT64_MAX,
    .turns_drained = UINT64_MAX, .turns_with_messages = UINT64_MAX,
    .arb_class_refusals = UINT64_MAX, .pending_demands = UINT32_MAX,
    .pump_subs_parked = UINT32_MAX, .pump_subs_active = UINT32_MAX,
    .owner_progress_slots = UINT32_MAX, .requester_open_objects = UINT32_MAX,
    .internal_bindings = UINT32_MAX, .internal_ns_subs = UINT32_MAX,
    .mailbox_pending = UINT32_MAX, .inbound_channel_entries = UINT64_MAX,
    .inbound_channel_bytes = UINT64_MAX, .journal_epoch = UINT64_MAX,
    .channel_entries_hwm = UINT32_MAX, .channel_bytes_hwm = UINT64_MAX,
};

uint64_t
moqr_cli_shards_bound(uint32_t lanes)
{
    /* Caller-local views over the immutable maxima: a few kilobytes, never
     * shared. The consistency refusal is bypassed for the measurement, since
     * the maxima satisfy it (every field equal). */
    moqr_snapshot_view_t vs[MOQR_SHARDS_MAX];
    moqr_json_w_t w;
    size_t len = 0;

    if (lanes == 0u || lanes > MOQR_SHARDS_MAX) {
        return UINT64_MAX;
    }
    for (uint32_t i = 0; i < lanes; i++) {
        vs[i].core = &k_core_max;
        vs[i].bind = &k_bind_max;
        vs[i].shard = &k_shard_max;
        vs[i].labels.shard = UINT16_MAX;
        vs[i].labels.transport = k_transport_max;
        vs[i].labels.version = k_version_max;
        vs[i].lane_wakes = UINT64_MAX;
    }
    /* The maximum's labels are not a known transport, so the document is
     * measured through the writer directly rather than through the kind
     * table; the kind strings are bounded separately below. */
    moqr_json_begin(&w, NULL, 0);
    moqr_json_object_begin(&w);
    moqr_json_key(&w, "api");   moqr_json_str(&w, "v1", 2);
    moqr_json_key(&w, "epoch"); moqr_json_u64(&w, UINT64_MAX);
    moqr_json_key(&w, "shards");
    moqr_json_array_begin(&w);
    for (uint32_t i = 0; i < lanes; i++) {
        const moqr_snapshot_view_t *v = &vs[i];
        moqr_json_object_begin(&w);
        moqr_json_key(&w, "shard");     moqr_json_u64(&w, v->labels.shard);
        moqr_json_key(&w, "transport");
        moqr_json_str(&w, v->labels.transport, strlen(v->labels.transport));
        moqr_json_key(&w, "version");
        moqr_json_str(&w, v->labels.version, strlen(v->labels.version));
        moqr_json_key(&w, "listener");  moqr_json_str(&w, "webtransport", 12);
        moqr_json_key(&w, "capability"); moqr_json_str(&w, "absent", 6);
        moqr_json_key(&w, "core");
        moqr_json_object_begin(&w);
        for (size_t f = 0; f < N(k_core); f++) {
            moqr_json_key(&w, k_core[f].name);
            moqr_json_u64(&w, k_core[f].get(v->core));
        }
        moqr_json_object_end(&w);
        moqr_json_key(&w, "bind");
        moqr_json_object_begin(&w);
        for (size_t f = 0; f < N(k_bind); f++) {
            moqr_json_key(&w, k_bind[f].name);
            moqr_json_u64(&w, k_bind[f].get(v->bind));
        }
        moqr_json_object_end(&w);
        moqr_json_key(&w, "shard_plane");
        moqr_json_object_begin(&w);
        for (size_t f = 0; f < N(k_shard); f++) {
            moqr_json_key(&w, k_shard[f].name);
            moqr_json_u64(&w, k_shard[f].get(v->shard));
        }
        moqr_json_key(&w, "lane_wakes"); moqr_json_u64(&w, v->lane_wakes);
        moqr_json_object_end(&w);
        moqr_json_object_end(&w);
    }
    moqr_json_array_end(&w);
    moqr_json_object_end(&w);
    if (!moqr_json_end(&w, &len)) {
        return UINT64_MAX;
    }
    return (uint64_t)len;
}
