#include "info_doc.h"

#include "admin_targets.h"

#include "../admin/json_out.h"
#include "../admin/moqr_admin.h"

#include <limits.h>
#include <string.h>

/*
 * The renderer works from a VIEW: every string as a pointer and an explicit
 * length, every number widened to its widest type, every optional part as a
 * flag. The real document fills the view from the configuration with bounded
 * scans; the bound fills it from an immutable synthetic maximum. One writer
 * serves both, so the bound is measured by the code that renders.
 */
typedef struct sv {
    const char *p;
    size_t      n;
} sv_t;

typedef struct info_view {
    bool     dual_listener_build;
    bool     verify_build;
    /* raw listener */
    sv_t     host;
    int64_t  port;
    size_t   nver;
    sv_t     ver[MOQR_CLI_MAX_VERSIONS];
    sv_t     alpn_set;
    uint64_t raw_first, raw_count;
    /* webtransport listener */
    bool     wt;
    sv_t     wt_host;
    int64_t  wt_port;
    sv_t     wt_path;
    sv_t     profile;
    size_t   nsub;
    sv_t     sub[MOQR_CLI_MAX_VERSIONS];
    sv_t     wt_alpn_set;
    uint64_t wt_first, wt_count;
    /* admin */
    sv_t     admin_host;
    int64_t  admin_port;
    bool     insecure_skip_verify;
    /* budgets */
    uint64_t core[11];
    uint64_t trace_ring_records;
    bool     pools_active;
    uint64_t ovr[6];
    uint64_t res_shards;
    bool     admit;
    uint64_t res[10];
    /* capacity */
    uint64_t cap_total, cap_core_structure, cap_core_payload,
             cap_bind_structure, cap_trace, cap_cross, cap_cli, cap_admin,
             cap_usable, cap_stack;
} info_view_t;

static const char *const k_core_keys[11] = {
    "max_bindings", "max_tracks", "max_subs", "max_ns_nodes", "max_ns_subs",
    "max_intents", "name_intern_bytes", "log_max_subgroups",
    "log_max_objects_per_group", "log_max_cursors", "linger_us",
};
static const char *const k_ovr_keys[6] = {
    "journal_entries", "mailbox_entries", "demand_channel_entries",
    "pending_demands", "subgroup_slots", "demand_channel_bytes",
};
static const char *const k_res_keys[10] = {
    "mailbox_cap", "journal_cap", "pending_cap", "demand_channel_cap",
    "demand_channel_byte_cap", "subgroup_slots", "trace_ring",
    "pump_turn_messages", "pump_turn_bytes", "usable_bindings",
};

static void
put_sv(moqr_json_w_t *w, sv_t v)
{
    moqr_json_str(w, v.p, v.n);
}

static void
listener_shards(moqr_json_w_t *w, uint64_t first, uint64_t count)
{
    moqr_json_key(w, "shards");
    moqr_json_object_begin(w);
    moqr_json_key(w, "first"); moqr_json_u64(w, first);
    moqr_json_key(w, "count"); moqr_json_u64(w, count);
    moqr_json_object_end(w);
}

/* The document, written once through `w`, from a view. */
static void
write_doc(moqr_json_w_t *w, const info_view_t *v)
{
    moqr_json_object_begin(w);
    moqr_json_key(w, "api");   moqr_json_str(w, "v1", 2);
    moqr_json_key(w, "relay"); moqr_json_str(w, "moq5-relay", 10);

    moqr_json_key(w, "build");
    moqr_json_object_begin(w);
    moqr_json_key(w, "dual_listener"); moqr_json_bool(w, v->dual_listener_build);
    moqr_json_key(w, "verify");        moqr_json_bool(w, v->verify_build);
    moqr_json_object_end(w);

    moqr_json_key(w, "listeners");
    moqr_json_array_begin(w);
    /* The raw MoQ-over-QUIC listener. `offered_versions` are the configured
     * MoQ drafts in preference order -- what is OFFERED, not a negotiated
     * per-shard draft -- and `alpn_set` is the same set as one label. */
    moqr_json_object_begin(w);
    moqr_json_key(w, "kind"); moqr_json_str(w, "raw", 3);
    moqr_json_key(w, "host"); put_sv(w, v->host);
    moqr_json_key(w, "port"); moqr_json_i64(w, v->port);
    moqr_json_key(w, "transport"); moqr_json_str(w, "msquic", 6);
    moqr_json_key(w, "offered_versions");
    moqr_json_array_begin(w);
    for (size_t i = 0; i < v->nver; i++) {
        put_sv(w, v->ver[i]);
    }
    moqr_json_array_end(w);
    moqr_json_key(w, "alpn_set"); put_sv(w, v->alpn_set);
    moqr_json_key(w, "lanes"); moqr_json_u64(w, v->raw_count);
    listener_shards(w, v->raw_first, v->raw_count);
    moqr_json_object_end(w);
    /* The WebTransport listener, when configured. Its MoQ drafts are offered
     * as WebTransport SUBPROTOCOLS; nothing here is a TLS ALPN claim. */
    if (v->wt) {
        moqr_json_object_begin(w);
        moqr_json_key(w, "kind"); moqr_json_str(w, "webtransport", 12);
        moqr_json_key(w, "host"); put_sv(w, v->wt_host);
        moqr_json_key(w, "port"); moqr_json_i64(w, v->wt_port);
        moqr_json_key(w, "path"); put_sv(w, v->wt_path);
        moqr_json_key(w, "profile"); put_sv(w, v->profile);
        moqr_json_key(w, "transport"); moqr_json_str(w, "wtquic-msquic", 13);
        moqr_json_key(w, "offered_subprotocols");
        moqr_json_array_begin(w);
        for (size_t i = 0; i < v->nsub; i++) {
            put_sv(w, v->sub[i]);
        }
        moqr_json_array_end(w);
        moqr_json_key(w, "alpn_set"); put_sv(w, v->wt_alpn_set);
        moqr_json_key(w, "lanes"); moqr_json_u64(w, v->wt_count);
        listener_shards(w, v->wt_first, v->wt_count);
        moqr_json_object_end(w);
    }
    moqr_json_array_end(w);

    moqr_json_key(w, "admin");
    moqr_json_object_begin(w);
    moqr_json_key(w, "host"); put_sv(w, v->admin_host);
    moqr_json_key(w, "port"); moqr_json_i64(w, v->admin_port);
    moqr_json_key(w, "targets");
    moqr_json_array_begin(w);
    for (size_t i = 0; i < MOQR_CLI_ADMIN_TARGET_COUNT; i++) {
        const char *t = moqr_cli_admin_target(i);
        moqr_json_str(w, t, strlen(t));
    }
    moqr_json_array_end(w);
    moqr_json_key(w, "clients"); moqr_json_u64(w, MOQR_ADMIN_MAX_CLIENTS);
    moqr_json_key(w, "banks");   moqr_json_u64(w, MOQR_ADMIN_BANKS);
    moqr_json_object_end(w);

    moqr_json_key(w, "insecure_skip_verify");
    moqr_json_bool(w, v->insecure_skip_verify);

    moqr_json_key(w, "budgets");
    moqr_json_object_begin(w);
    moqr_json_key(w, "core");
    moqr_json_object_begin(w);
    for (size_t i = 0; i < 11; i++) {
        moqr_json_key(w, k_core_keys[i]); moqr_json_u64(w, v->core[i]);
    }
    moqr_json_object_end(w);
    moqr_json_key(w, "telemetry");
    moqr_json_object_begin(w);
    moqr_json_key(w, "trace_ring_records"); moqr_json_u64(w, v->trace_ring_records);
    moqr_json_object_end(w);
    /* Cross-shard: what was WRITTEN (zero means "the default was requested")
     * and what the pure resolver RESOLVED. With one shard the resolved values
     * are the template the resolver would apply; no cross-shard pool exists
     * in that composition, which `pools_active` states outright. */
    moqr_json_key(w, "cross_shard");
    moqr_json_object_begin(w);
    moqr_json_key(w, "pools_active"); moqr_json_bool(w, v->pools_active);
    moqr_json_key(w, "configured_overrides");
    moqr_json_object_begin(w);
    for (size_t i = 0; i < 6; i++) {
        moqr_json_key(w, k_ovr_keys[i]); moqr_json_u64(w, v->ovr[i]);
    }
    moqr_json_object_end(w);
    moqr_json_key(w, "resolved");
    moqr_json_object_begin(w);
    moqr_json_key(w, "shards"); moqr_json_u64(w, v->res_shards);
    moqr_json_key(w, "admit");  moqr_json_bool(w, v->admit);
    for (size_t i = 0; i < 10; i++) {
        moqr_json_key(w, k_res_keys[i]); moqr_json_u64(w, v->res[i]);
    }
    moqr_json_object_end(w);
    moqr_json_object_end(w);
    moqr_json_object_end(w);

    /* The capacity model, with its meaning: an ALLOCATOR-REQUEST ceiling, not
     * resident memory. admin_bytes is already inside cli_runtime_bytes and
     * total_bytes; the thread stack is a reservation reported beside the
     * ceiling; kernel buffers are not represented at all. */
    moqr_json_key(w, "capacity");
    moqr_json_object_begin(w);
    moqr_json_key(w, "model"); moqr_json_str(w, "allocator-request-ceiling", 25);
    moqr_json_key(w, "total_bytes"); moqr_json_u64(w, v->cap_total);
    moqr_json_key(w, "per_shard");
    moqr_json_object_begin(w);
    moqr_json_key(w, "core_structure"); moqr_json_u64(w, v->cap_core_structure);
    moqr_json_key(w, "core_payload");   moqr_json_u64(w, v->cap_core_payload);
    moqr_json_key(w, "bind_structure"); moqr_json_u64(w, v->cap_bind_structure);
    moqr_json_key(w, "trace");          moqr_json_u64(w, v->cap_trace);
    moqr_json_object_end(w);
    moqr_json_key(w, "cross_shard_bytes"); moqr_json_u64(w, v->cap_cross);
    moqr_json_key(w, "cli_runtime_bytes"); moqr_json_u64(w, v->cap_cli);
    moqr_json_key(w, "admin_bytes");       moqr_json_u64(w, v->cap_admin);
    moqr_json_key(w, "usable_bindings_per_shard"); moqr_json_u64(w, v->cap_usable);
    moqr_json_key(w, "reservations");
    moqr_json_object_begin(w);
    moqr_json_key(w, "admin_thread_stack_bytes"); moqr_json_u64(w, v->cap_stack);
    moqr_json_object_end(w);
    moqr_json_object_end(w);

    moqr_json_object_end(w);
}

/* -- the real view ----------------------------------------------------------- */

/* A fixed configuration array as a view; false when no terminator lies inside
 * the array, so nothing ever scans past it. */
static bool
sv_bounded(const char *arr, size_t cap, sv_t *out)
{
    const char *end = memchr(arr, '\0', cap);
    if (end == NULL) {
        return false;
    }
    out->p = arr;
    out->n = (size_t)(end - arr);
    return true;
}

#define SV(arr, out) sv_bounded((arr), sizeof(arr), (out))

static bool
view_from_inputs(const moqr_cli_info_inputs_t *in, info_view_t *v)
{
    const moqr_cli_config_t *cfg = in->cfg;
    const moqr_cli_shard_plan_t *plan = in->plan;
    const moqr_shards_limits_t *lim = in->limits;
    const moqr_cli_capacity_t *cap = in->capacity;
    const char *profile;

    memset(v, 0, sizeof(*v));
    if (cfg->version_count > MOQR_CLI_MAX_VERSIONS ||
        cfg->wt.version_count > MOQR_CLI_MAX_VERSIONS) {
        return false;
    }
    v->dual_listener_build = in->dual_listener_build;
    v->verify_build = in->verify_build;
    if (!SV(cfg->host, &v->host) || !SV(cfg->alpn_set, &v->alpn_set) ||
        !SV(cfg->admin.host, &v->admin_host)) {
        return false;
    }
    v->port = cfg->port;
    v->nver = cfg->version_count;
    for (size_t i = 0; i < v->nver; i++) {
        if (!SV(cfg->alpn_buf[i], &v->ver[i])) {
            return false;
        }
    }
    v->raw_first = plan->raw_first;
    v->raw_count = plan->raw_count;
    v->wt = plan->wt_count > 0u;
    if (v->wt) {
        profile = moqr_cli_wt_profile_name(cfg->wt.profile);
        if (profile == NULL || !SV(cfg->wt.host, &v->wt_host) ||
            !SV(cfg->wt.path, &v->wt_path) ||
            !SV(cfg->wt.alpn_set, &v->wt_alpn_set)) {
            return false;
        }
        v->profile.p = profile;
        v->profile.n = strlen(profile);
        v->wt_port = cfg->wt.port;
        v->nsub = cfg->wt.version_count;
        for (size_t i = 0; i < v->nsub; i++) {
            if (!SV(cfg->wt.subproto_buf[i], &v->sub[i])) {
                return false;
            }
        }
        v->wt_first = plan->wt_first;
        v->wt_count = plan->wt_count;
    }
    v->admin_port = cfg->admin.port;
    v->insecure_skip_verify = cfg->insecure_skip_verify;
    v->core[0] = cfg->core.max_bindings;
    v->core[1] = cfg->core.max_tracks;
    v->core[2] = cfg->core.max_subs;
    v->core[3] = cfg->core.max_ns_nodes;
    v->core[4] = cfg->core.max_ns_subs;
    v->core[5] = cfg->core.max_intents;
    v->core[6] = cfg->core.name_intern_bytes;
    v->core[7] = cfg->core.log_max_subgroups;
    v->core[8] = cfg->core.log_max_objects_per_group;
    v->core[9] = cfg->core.log_max_cursors;
    v->core[10] = cfg->core.linger_us;
    v->trace_ring_records = cfg->telemetry.trace_ring_records;
    v->pools_active = plan->total_shards > 1u;
    v->ovr[0] = cfg->cross_shard.journal_entries;
    v->ovr[1] = cfg->cross_shard.mailbox_entries;
    v->ovr[2] = cfg->cross_shard.demand_channel_entries;
    v->ovr[3] = cfg->cross_shard.pending_demands;
    v->ovr[4] = cfg->cross_shard.subgroup_slots;
    v->ovr[5] = cfg->cross_shard.demand_channel_bytes;
    v->res_shards = lim->shards;
    v->admit = lim->admit;
    v->res[0] = lim->mbox_cap;
    v->res[1] = lim->jrn_cap;
    v->res[2] = lim->pend_cap;
    v->res[3] = lim->dch_cap;
    v->res[4] = lim->dch_byte_cap;
    v->res[5] = lim->sg_slots;
    v->res[6] = lim->trace_ring;
    v->res[7] = lim->pump_turn_msgs;
    v->res[8] = lim->pump_turn_bytes;
    v->res[9] = lim->usable_bindings;
    v->cap_total = cap->total_bytes;
    v->cap_core_structure = cap->core_structure_bytes;
    v->cap_core_payload = cap->core_payload_bytes;
    v->cap_bind_structure = cap->bind_structure_bytes;
    v->cap_trace = cap->trace_bytes;
    v->cap_cross = cap->cross_shard_bytes;
    v->cap_cli = cap->cli_runtime_bytes;
    v->cap_admin = cap->admin_bytes;
    v->cap_usable = cap->usable_bindings_per_shard;
    v->cap_stack = cap->admin_thread_stack_bytes;
    return true;
}

moqr_result_t
moqr_cli_info_render(const moqr_cli_info_inputs_t *in, char *buf, size_t cap,
                     size_t *out_len)
{
    moqr_json_w_t w;
    info_view_t v;
    size_t len = 0;

    if (out_len != NULL) {
        *out_len = 0;
    }
    /* Nothing usable is ever left in a supplied output, on ANY refusal --
     * including these early ones. A NULL or zero-capacity output is simply
     * not written. */
    if (buf != NULL && cap > 0u) {
        buf[0] = '\0';
    }
    if (in == NULL || in->cfg == NULL || in->plan == NULL ||
        in->limits == NULL || in->capacity == NULL || buf == NULL ||
        cap == 0u) {
        return MOQR_ERR_INVAL;
    }
    if (!view_from_inputs(in, &v)) {
        return MOQR_ERR_INVAL;
    }
    moqr_json_begin(&w, buf, cap);
    write_doc(&w, &v);
    if (!moqr_json_end(&w, &len)) {
        /* Either a value failed validation or the body did not fit. The
         * counting pass tells the two apart without any storage. */
        moqr_json_w_t count;
        moqr_json_begin(&count, NULL, 0);
        write_doc(&count, &v);
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
 * An IMMUTABLE synthetic maximum: every string as long as its configuration
 * array allows and made entirely of a control byte, which is the widest
 * expansion the writer has (six output bytes per input byte); every array at
 * its bounded maximum length; every number at the widest width its type can
 * print (20 digits unsigned, 20 with the sign for the signed ports); every
 * bool false (five bytes, one more than true); every optional part present;
 * the longest profile name. Nothing here is written to, so measuring it from
 * any number of threads at once is a read of constant data.
 */
#define C8   "\x1f\x1f\x1f\x1f\x1f\x1f\x1f\x1f"
#define C64  C8 C8 C8 C8 C8 C8 C8 C8
#define C255 C64 C64 C64 C8 C8 C8 C8 C8 C8 C8 "\x1f\x1f\x1f\x1f\x1f\x1f\x1f"

static const char k_ctl255[] = C255;          /* host, path      (255) */
static const char k_ctl67[]  = C64 "\x1f\x1f\x1f"; /* alpn_set   (67)  */
static const char k_ctl63[]  = C8 C8 C8 C8 C8 C8 C8 "\x1f\x1f\x1f\x1f\x1f\x1f\x1f"; /* admin host (63) */
static const char k_ctl15[]  = C8 "\x1f\x1f\x1f\x1f\x1f\x1f\x1f";                /* version token (15) */
static const char k_profile_max[] = MOQR_CLI_WT_PROFILE_NAME_MAX;
/* The width this document reserves for the profile is the width the
 * vocabulary can actually produce. Bound here rather than only at the
 * shared name, so a local literal that drifts shorter is a build error
 * instead of a bound that silently under-measures the widest document. */
_Static_assert(sizeof(k_profile_max) ==
                   sizeof(MOQR_CLI_WT_PROFILE_NAME_MAX),
               "the maximum-width document must measure itself against "
               "the longest profile name the vocabulary can produce");

#define SVC(arr) { (arr), sizeof(arr) - 1u }
#define U64MAX UINT64_MAX

static const info_view_t k_max_view = {
    .dual_listener_build = false,
    .verify_build = false,
    .host = SVC(k_ctl255),
    .port = INT64_MIN,
    .nver = MOQR_CLI_MAX_VERSIONS,
    .ver = { SVC(k_ctl15), SVC(k_ctl15), SVC(k_ctl15), SVC(k_ctl15) },
    .alpn_set = SVC(k_ctl67),
    .raw_first = U64MAX, .raw_count = U64MAX,
    .wt = true,
    .wt_host = SVC(k_ctl255),
    .wt_port = INT64_MIN,
    .wt_path = SVC(k_ctl255),
    .profile = SVC(k_profile_max),
    .nsub = MOQR_CLI_MAX_VERSIONS,
    .sub = { SVC(k_ctl15), SVC(k_ctl15), SVC(k_ctl15), SVC(k_ctl15) },
    .wt_alpn_set = SVC(k_ctl67),
    .wt_first = U64MAX, .wt_count = U64MAX,
    .admin_host = SVC(k_ctl63),
    .admin_port = INT64_MIN,
    .insecure_skip_verify = false,
    .core = { U64MAX, U64MAX, U64MAX, U64MAX, U64MAX, U64MAX, U64MAX, U64MAX,
              U64MAX, U64MAX, U64MAX },
    .trace_ring_records = U64MAX,
    .pools_active = false,
    .ovr = { U64MAX, U64MAX, U64MAX, U64MAX, U64MAX, U64MAX },
    .res_shards = U64MAX,
    .admit = false,
    .res = { U64MAX, U64MAX, U64MAX, U64MAX, U64MAX, U64MAX, U64MAX, U64MAX,
             U64MAX, U64MAX },
    .cap_total = U64MAX, .cap_core_structure = U64MAX,
    .cap_core_payload = U64MAX, .cap_bind_structure = U64MAX,
    .cap_trace = U64MAX, .cap_cross = U64MAX, .cap_cli = U64MAX,
    .cap_admin = U64MAX, .cap_usable = U64MAX, .cap_stack = U64MAX,
};

uint64_t
moqr_cli_info_bound(void)
{
    moqr_json_w_t w;
    size_t len = 0;

    /* The array sizes above are the configuration's own array sizes, checked
     * here so a widened configuration field cannot leave the maximum behind. */
    if (sizeof(k_ctl255) != sizeof(((moqr_cli_config_t *)0)->host) ||
        sizeof(k_ctl255) != sizeof(((moqr_cli_config_t *)0)->wt.path) ||
        sizeof(k_ctl255) != sizeof(((moqr_cli_config_t *)0)->wt.host) ||
        sizeof(k_ctl67) < sizeof(((moqr_cli_config_t *)0)->alpn_set) ||
        sizeof(k_ctl63) != sizeof(((moqr_cli_config_t *)0)->admin.host) ||
        sizeof(k_ctl15) < sizeof(((moqr_cli_config_t *)0)->alpn_buf[0])) {
        return UINT64_MAX;
    }
    moqr_json_begin(&w, NULL, 0);
    write_doc(&w, &k_max_view);
    if (!moqr_json_end(&w, &len)) {
        return UINT64_MAX;
    }
    return (uint64_t)len;
}
