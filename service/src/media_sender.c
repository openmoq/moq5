#include "media_sender_internal.h"
#include "media_sender_catalog_internal.h"
#include "media_sender_mpeg2ts_internal.h"
#include "media_sender_pump_internal.h"
#include "media_sender_tracks_internal.h"
#include "media_sender_write_internal.h"

#define SENDER_DEFAULT_BLOCK_TIMEOUT_US  1000000ull


/* Frozen v0 base size for the caller-filled stats struct (see media_receiver.c):
 * the validation floor, computed from the last v0 field so it stays constant as
 * fields are appended. Must NOT change when fields are added. */
#define MEDIA_SENDER_STATS_V0_SIZE \
    (offsetof(moq_media_sender_stats_t, last_error) + sizeof(moq_result_t))



/* Minimum accepted moq_media_sender_cfg_t size: the prefix through the last
 * pre-callbacks field. A create/attach caller must supply at least this; the
 * appended `callbacks` field is read only when struct_size covers it, so an
 * older/smaller cfg keeps working (no callbacks) and a newer/larger cfg's
 * unknown trailing fields are ignored. Must NOT change when fields are added. */
#define MEDIA_SENDER_CFG_MIN_SIZE \
    offsetof(moq_media_sender_cfg_t, callbacks)


/* Automatic catalog refresh is OPT-IN. MSF-01 §5: a catalog object SHOULD be
 * published only when track availability changes, or after enough time that
 * the prior object might have left a delivery-network cache. A cfg field of 0
 * (or an old-size caller whose struct_size predates it) therefore resolves to
 * the disabled sentinel below; only an explicit finite interval republishes on
 * a timer. */
#define SENDER_CATALOG_REFRESH_DISABLED UINT64_MAX



/* -- small helpers ---------------------------------------------------- */

void sender_set_fatal_locked(moq_media_sender_t *s, uint64_t code)
{
    if (!s->fatal) {
        s->fatal = true;
        s->fatal_code = code;
    }
    s->refresh_wake_deadline_us = UINT64_MAX;   /* terminal: no managed wake */
    pthread_cond_broadcast(&s->space_cv);   /* unblock BLOCK_TIMEOUT waiters */
}


/* Demand callbacks fire only for app-visible media, not the internal catalog
 * or either generated timeline. */
bool track_is_app_media(const moq_media_track_t *t)
{
    return !t->is_catalog && !t->is_event_timeline && !t->is_media_timeline;
}


bool sender_terminal(const moq_media_sender_t *s)
{
    moq_media_sender_t *ms = (moq_media_sender_t *)(uintptr_t)s;
    pthread_mutex_lock(&ms->mu);
    bool fatal = ms->fatal;
    pthread_mutex_unlock(&ms->mu);
    return fatal || (s->ep && moq_endpoint_is_closed(s->ep));
}


/* "Endpoint closed" check that does NOT take ep->mu, for use while s->mu is held
 * (the BLOCK_TIMEOUT loop). The endpoint hook lock order is ep->mu -> s->mu, so
 * a holder of s->mu reachable from a hook MUST NOT take ep->mu -- the public
 * moq_endpoint_is_closed() does, so the BLOCK_TIMEOUT loop uses this internal
 * ep->mu-free variant instead (the lock-inversion fix). Shared with the test
 * seam so a regression back to an ep->mu-taking endpoint API here is caught. */
bool sender_ep_closed(const moq_media_sender_t *s)
{
    return s->ep && moq_endpoint_is_closed_internal(s->ep);
}


/* Whether the companion in effect can encode for the latched draft. An
 * unresolved AUTO companion is usable on none. CALLER MUST HOLD s->mu. */
bool sender_version_usable(const moq_media_sender_t *s)
{
    if (s->companion_profile == MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC)
        return s->transport_version == MOQ_VERSION_DRAFT_22;
    if (s->companion_profile == MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01)
        return s->transport_version == MOQ_VERSION_DRAFT_16 ||
               s->transport_version == MOQ_VERSION_DRAFT_18;
    return false;
}


/* Whether a media write is refused as a companion/negotiated-version
 * mismatch: the latched draft is one the companion in effect cannot use,
 * or an explicit D22 companion has not yet observed draft 22. An unresolved
 * AUTO companion before any draft is latched is not refused. CALLER MUST HOLD s->mu. */
bool sender_companion_refused_locked(const moq_media_sender_t *s)
{
    if (sender_version_usable(s)) return false;
    return s->transport_version != 0 ||
           s->companion_profile == MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC;
}


/* -- cfg -------------------------------------------------------------- */

/* Frozen v0 floors. The callbacks prefix runs through on_subscriber_left --
 * everything before the appended on_ready/on_closed/on_track_closed fields.
 * The cfg prefix runs through the v0 PORTION of the embedded callbacks (the
 * pre-publish layout ended with the nested callbacks as its last field), so
 * an old caller's sizeof is offsetof(callbacks) + the v0 callbacks size --
 * NOT offsetof(callbacks) + sizeof(current callbacks), which includes the
 * appended callback fields. Pointer-only initializers must never clear past
 * these floors. */
#define MEDIA_SENDER_CALLBACKS_V0_SIZE \
    offsetof(moq_media_sender_callbacks_t, on_ready)

#define MEDIA_SENDER_CFG_V0_SIZE \
    (offsetof(moq_media_sender_cfg_t, callbacks) + \
     MEDIA_SENDER_CALLBACKS_V0_SIZE)


void moq_media_sender_cfg_init(moq_media_sender_cfg_t *cfg)
{
    if (!cfg) return;
    /* Clear and stamp ONLY the frozen v0 prefix: writing sizeof(*cfg) would
     * overflow a caller compiled against the pre-publish (smaller) struct.
     * Appended fields (the on_ready/on_closed/on_track_closed callbacks,
     * publish_tracks, drop_without_demand, catalog_refresh_interval_us) stay
     * disabled/default; callers that set them use the _sized initializers. */
    memset(cfg, 0, MEDIA_SENDER_CFG_V0_SIZE);
    cfg->struct_size = (uint32_t)MEDIA_SENDER_CFG_V0_SIZE;
    /* Nested struct_size: the v0 portion this initializer cleared. */
    cfg->callbacks.struct_size = (uint32_t)MEDIA_SENDER_CALLBACKS_V0_SIZE;
    /* CMSF §3.3/§3.4: validate CMAF output by default (strict). Affects CMAF
     * tracks only; RAW/LOC is unaffected. Callers wanting passthrough set
     * validate_cmaf = false explicitly after init. */
    cfg->validate_cmaf = true;
    /* backpressure stays UNSET on purpose: forced choice (§7.5). */
}


void moq_media_sender_callbacks_init(moq_media_sender_callbacks_t *cb)
{
    if (!cb) return;
    /* Frozen v0 prefix only (see MEDIA_SENDER_CALLBACKS_V0_SIZE): the
     * appended lifecycle callbacks need moq_media_sender_callbacks_init_sized. */
    memset(cb, 0, MEDIA_SENDER_CALLBACKS_V0_SIZE);
    cb->struct_size = (uint32_t)MEDIA_SENDER_CALLBACKS_V0_SIZE;
}


void moq_media_sender_cfg_init_live(moq_media_sender_cfg_t *cfg)
{
    moq_media_sender_cfg_init(cfg);
    if (cfg) cfg->backpressure = MOQ_MEDIA_SEND_BP_DROP_TO_KEYFRAME;
}


void moq_media_sender_cfg_init_lossless(moq_media_sender_cfg_t *cfg)
{
    moq_media_sender_cfg_init(cfg);
    if (!cfg) return;
    cfg->backpressure = MOQ_MEDIA_SEND_BP_BLOCK_TIMEOUT;
    cfg->block_timeout_us = SENDER_DEFAULT_BLOCK_TIMEOUT_US;
}


void moq_media_sender_callbacks_init_sized(moq_media_sender_callbacks_t *cb,
                                           size_t cb_size)
{
    if (!cb) return;
    /* Clear exactly what the caller allocated, never more than this library's
     * struct knows about (same contract as moq_pub_cfg_init_sized). */
    size_t n = cb_size < sizeof(*cb) ? cb_size : sizeof(*cb);
    if (n < sizeof(cb->struct_size)) return;  /* too small to even stamp */
    memset(cb, 0, n);
    cb->struct_size = (uint32_t)n;
}


void moq_media_sender_cfg_init_sized(moq_media_sender_cfg_t *cfg,
                                     size_t cfg_size)
{
    if (!cfg) return;
    size_t n = cfg_size < sizeof(*cfg) ? cfg_size : sizeof(*cfg);
    if (n < sizeof(cfg->struct_size)) return;  /* too small to even stamp */
    memset(cfg, 0, n);
    cfg->struct_size = (uint32_t)n;
    /* Initialize as much of the nested callbacks struct as the caller's
     * storage covers (bounded by BOTH the outer bytes available and the
     * library's own callbacks size). */
    {
        size_t cb_off = offsetof(moq_media_sender_cfg_t, callbacks);
        if (n > cb_off)
            moq_media_sender_callbacks_init_sized(&cfg->callbacks, n - cb_off);
    }
    if (n >= offsetof(moq_media_sender_cfg_t, validate_cmaf) +
             sizeof(cfg->validate_cmaf))
        cfg->validate_cmaf = true;
    if (n >= offsetof(moq_media_sender_cfg_t, companion_profile) +
             sizeof(cfg->companion_profile))
        cfg->companion_profile = MOQ_MEDIA_COMPANION_AUTO;
    /* backpressure stays UNSET on purpose: forced choice (§7.5). */
}


/* The caller-storage prefix the preset wrappers may touch. Computed from
 * cfg_size, NEVER read back from cfg->struct_size: when cfg_size is smaller
 * than the struct_size field itself, the base initializer cannot stamp it,
 * so the field holds whatever (possibly poisoned) bytes the caller had --
 * gating on it could write a preset default out of bounds. */
static size_t sender_cfg_prefix(size_t cfg_size)
{
    return cfg_size < sizeof(moq_media_sender_cfg_t)
               ? cfg_size : sizeof(moq_media_sender_cfg_t);
}


void moq_media_sender_cfg_init_live_sized(moq_media_sender_cfg_t *cfg,
                                          size_t cfg_size)
{
    moq_media_sender_cfg_init_sized(cfg, cfg_size);
    if (!cfg) return;
    size_t n = sender_cfg_prefix(cfg_size);
    if (n >= offsetof(moq_media_sender_cfg_t, backpressure) +
                 sizeof(cfg->backpressure))
        cfg->backpressure = MOQ_MEDIA_SEND_BP_DROP_TO_KEYFRAME;
}


void moq_media_sender_cfg_init_lossless_sized(moq_media_sender_cfg_t *cfg,
                                              size_t cfg_size)
{
    moq_media_sender_cfg_init_sized(cfg, cfg_size);
    if (!cfg) return;
    size_t n = sender_cfg_prefix(cfg_size);
    if (n >= offsetof(moq_media_sender_cfg_t, backpressure) +
                 sizeof(cfg->backpressure))
        cfg->backpressure = MOQ_MEDIA_SEND_BP_BLOCK_TIMEOUT;
    if (n >= offsetof(moq_media_sender_cfg_t, block_timeout_us) +
                 sizeof(cfg->block_timeout_us))
        cfg->block_timeout_us = SENDER_DEFAULT_BLOCK_TIMEOUT_US;
}


void moq_media_track_cfg_init_sized(moq_media_track_cfg_t *cfg, size_t cfg_size)
{
    if (!cfg || cfg_size < sizeof(cfg->struct_size)) return;
    size_t n = cfg_size < sizeof(*cfg) ? cfg_size : sizeof(*cfg);
    memset(cfg, 0, n);
    cfg->struct_size = (uint32_t)n;
    if (n >= offsetof(moq_media_track_cfg_t, is_live) + sizeof(cfg->is_live))
        cfg->is_live = true;
}


void (moq_media_track_cfg_init)(moq_media_track_cfg_t *cfg)
{
    moq_media_track_cfg_init_sized(cfg, MEDIA_TRACK_CFG_SIZE_THROUGH_ALT_GROUP);
}

#ifdef MOQ_MEDIA_SENDER_TESTING


/* -- Construction ----------------------------------------------------- */

/* Test-only allocator injection for construction-arithmetic oracles: lets a
 * counting/refusing allocator observe every request sender_new makes, in
 * order, and refuse a nominated one. NULL restores the default. Never compiled
 * into the shipping library. */
static const moq_alloc_t *g_test_alloc;

void moq_media_sender_test_set_alloc(const moq_alloc_t *alloc);

void moq_media_sender_test_set_alloc(const moq_alloc_t *alloc)
{
    g_test_alloc = alloc;
}

void moq_media_sender_test_entry_sizes(size_t *ring_entry, size_t *track);

void moq_media_sender_test_entry_sizes(size_t *ring_entry, size_t *track)
{
    if (ring_entry) *ring_entry = sizeof(sender_preq_entry_t);
    if (track)      *track      = sizeof(moq_media_track_t);
}

#define SENDER_CTOR_ALLOC() (g_test_alloc ? g_test_alloc : moq_alloc_default())

/* Test-only pre-traversal observation for configuration validation: called
 * with the namespace part count immediately before the parts are walked.
 * Returning false stops validation with MOQ_ERR_INVAL, which keeps a fixture
 * with an intrinsically unrepresentable count bounded (no element is read)
 * while still recording that the walk was about to start. NULL disables. */
static bool (*g_test_parts_walk)(size_t count, void *ctx);

static void *g_test_parts_walk_ctx;

void moq_media_sender_test_set_parts_walk_hook(bool (*fn)(size_t, void *), void *ctx);

void moq_media_sender_test_set_parts_walk_hook(bool (*fn)(size_t, void *), void *ctx)
{
    g_test_parts_walk = fn;
    g_test_parts_walk_ctx = ctx;
}

#define SENDER_CFG_PARTS_WALK_HOOK(count) \
    do { if (g_test_parts_walk && !g_test_parts_walk((count), g_test_parts_walk_ctx)) \
             return MOQ_ERR_INVAL; } while (0)

/* Test-only indirection for the BLOCK_TIMEOUT wait's clock reads and timed
 * wait, so a scripted oracle can declare exact clock values and wait results
 * and observe the exact absolute time the product passes to the wait. With no
 * script installed both expand to the real calls, so behaviour is unchanged. */
int (*g_test_clock)(struct timespec *ts, void *ctx);

void *g_test_clock_ctx;

void moq_media_sender_test_set_clock_hook(int (*fn)(struct timespec *, void *), void *ctx);

void moq_media_sender_test_set_clock_hook(int (*fn)(struct timespec *, void *), void *ctx)
{
    g_test_clock = fn;
    g_test_clock_ctx = ctx;
}

#else
#define SENDER_CTOR_ALLOC() moq_alloc_default()

#define SENDER_CFG_PARTS_WALK_HOOK(count) ((void)0)


#endif

/* The configured companion. An absent or partially covered field is AUTO,
 * the same default the sized initializers stamp. */
static moq_media_companion_profile_t sender_cfg_companion(
    const moq_media_sender_cfg_t *cfg)
{
    if (cfg->struct_size >= offsetof(moq_media_sender_cfg_t, companion_profile) +
                            sizeof(cfg->companion_profile))
        return cfg->companion_profile;
    return MOQ_MEDIA_COMPANION_AUTO;
}


moq_result_t sender_validate_cfg(const moq_media_sender_cfg_t *cfg)
{
    if (!cfg) return MOQ_ERR_INVAL;
    if (cfg->struct_size < MEDIA_SENDER_CFG_MIN_SIZE)
        return MOQ_ERR_INVAL;
    moq_media_companion_profile_t companion = sender_cfg_companion(cfg);
    if (companion != MOQ_MEDIA_COMPANION_AUTO &&
        companion != MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01 &&
        companion != MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC)
        return MOQ_ERR_INVAL;
    if (cfg->namespace_.count == 0 || !cfg->namespace_.parts)
        return MOQ_ERR_INVAL;
    /* count * sizeof(moq_bytes_t) must be representable before a single part
     * is read: an intrinsically impossible count is refused, not traversed. */
    if (cfg->namespace_.count > SIZE_MAX / sizeof(moq_bytes_t))
        return MOQ_ERR_INVAL;
    SENDER_CFG_PARTS_WALK_HOOK(cfg->namespace_.count);
    for (size_t i = 0; i < cfg->namespace_.count; i++) {
        if (cfg->namespace_.parts[i].len == 0 ||
            !cfg->namespace_.parts[i].data)
            return MOQ_ERR_INVAL;
    }
    if (cfg->catalog_track.len > 0 && !cfg->catalog_track.data)
        return MOQ_ERR_INVAL;
    switch (cfg->backpressure) {
    case MOQ_MEDIA_SEND_BP_DROP_TO_KEYFRAME:
    case MOQ_MEDIA_SEND_BP_DROP_GROUP:
    case MOQ_MEDIA_SEND_BP_BLOCK_TIMEOUT:
    case MOQ_MEDIA_SEND_BP_RETURN_WOULD_BLOCK:
        break;
    default:
        return MOQ_ERR_INVAL;   /* UNSET: the choice is forced */
    }
    /* Root contentProtections shape (validated before connect so create()
     * rejects a bad cfg without standing up an endpoint). */
    if (cfg->content_protection_count > 0 && !cfg->content_protections)
        return MOQ_ERR_INVAL;
    for (size_t i = 0; i < cfg->content_protection_count; i++) {
        moq_result_t cr = cp_validate_one(&cfg->content_protections[i]);
        if (cr != MOQ_OK) return cr;
    }
    /* CMSF §4.1.1.1: each contentProtection refID is unique across the array
     * (information MUST NOT be duplicated). Reject a duplicate at create/attach
     * so track refID resolution is unambiguous. */
    for (size_t i = 0; i < cfg->content_protection_count; i++)
        for (size_t j = i + 1; j < cfg->content_protection_count; j++) {
            moq_bytes_t a = cfg->content_protections[i].ref_id;
            moq_bytes_t b = cfg->content_protections[j].ref_id;
            if (a.len == b.len && a.data && b.data &&
                memcmp(a.data, b.data, a.len) == 0)
                return MOQ_ERR_INVAL;
        }
    return MOQ_OK;
}


void sender_free(moq_media_sender_t *s)
{
    moq_media_auth_operation_clear(&s->namespace_auth, &s->alloc);
    moq_media_auth_source_clear(&s->resource_auth, &s->alloc);
    if (s->tracks) {
        for (size_t i = 0; i < s->track_count; i++)
            track_free(s, s->tracks[i]);
        s->alloc.free(s->tracks, s->track_cap * sizeof(moq_media_track_t *),
                      s->alloc.ctx);
    }
    if (s->preq) {
        while (s->preq_head != s->preq_tail) {
            preq_entry_release(s, &s->preq[s->preq_head % s->ring_cap]);
            s->preq_head++;
        }
        s->alloc.free(s->preq, s->ring_cap * sizeof(sender_preq_entry_t),
                      s->alloc.ctx);
    }
    if (s->ns_parts)
        s->alloc.free(s->ns_parts, s->namespace_.count * sizeof(moq_bytes_t),
                      s->alloc.ctx);
    if (s->ns_data)
        s->alloc.free(s->ns_data, s->ns_data_size, s->alloc.ctx);
    if (s->published_catalog) moq_rcbuf_decref(s->published_catalog);
    for (size_t pi = 0; pi < s->pending_obj_count; pi++)
        if (s->pending_objs[pi]) moq_rcbuf_decref(s->pending_objs[pi]);
    if (s->pending_current) moq_rcbuf_decref(s->pending_current);
    free_root_cps(s);
    pthread_cond_destroy(&s->space_cv);
    pthread_mutex_destroy(&s->mu);
    s->alloc.free(s, sizeof(*s), s->alloc.ctx);
}


/* Copy the cfg's appended tail (callbacks + publish_tracks +
 * drop_without_demand) into the sender, prefix-safely. The effective
 * callbacks prefix is bounded by BOTH the outer bytes the caller's
 * struct_size makes available past offsetof(callbacks) AND the nested
 * callbacks.struct_size -- so an old caller whose (complete, for its build)
 * callbacks block is smaller than this library's keeps every field it set,
 * and a mis-sized outer struct_size can never install bytes past the
 * caller's real storage. Field gates are whole-field: a partially-covered
 * pointer is never copied. s->callbacks must already be zeroed. */
void sender_copy_cfg_tail(moq_media_sender_t *s,
                          const moq_media_sender_cfg_t *cfg)
{
    s->companion_profile = sender_cfg_companion(cfg);
    s->companion_auto = s->companion_profile == MOQ_MEDIA_COMPANION_AUTO;
    size_t cb_off = offsetof(moq_media_sender_cfg_t, callbacks);
    if (cfg->struct_size > cb_off) {
        const moq_media_sender_callbacks_t *cb = &cfg->callbacks;
        size_t avail = cfg->struct_size - cb_off;
        if (avail > sizeof(*cb)) avail = sizeof(*cb);
        /* The nested struct_size is only readable when the outer prefix
         * covers it; then it further bounds the effective prefix. */
        size_t eff = 0;
        if (avail >= offsetof(moq_media_sender_callbacks_t, ctx))
            eff = cb->struct_size < avail ? cb->struct_size : avail;
#define SENDER_CB_FIELD_IN(name) \
        (eff >= offsetof(moq_media_sender_callbacks_t, name) + \
                sizeof(cb->name))
        if (SENDER_CB_FIELD_IN(ctx))
            s->callbacks.ctx = cb->ctx;
        if (SENDER_CB_FIELD_IN(on_subscriber_joined))
            s->callbacks.on_subscriber_joined = cb->on_subscriber_joined;
        if (SENDER_CB_FIELD_IN(on_subscriber_left))
            s->callbacks.on_subscriber_left = cb->on_subscriber_left;
        if (SENDER_CB_FIELD_IN(on_ready))
            s->callbacks.on_ready = cb->on_ready;
        if (SENDER_CB_FIELD_IN(on_closed))
            s->callbacks.on_closed = cb->on_closed;
        if (SENDER_CB_FIELD_IN(on_track_closed))
            s->callbacks.on_track_closed = cb->on_track_closed;
#undef SENDER_CB_FIELD_IN
        s->callbacks.struct_size = sizeof(s->callbacks);
    }
    if (cfg->struct_size >= offsetof(moq_media_sender_cfg_t, publish_tracks) +
                                sizeof(cfg->publish_tracks))
        s->publish_tracks = cfg->publish_tracks;
    if (cfg->struct_size >= offsetof(moq_media_sender_cfg_t,
                                     drop_without_demand) +
                                sizeof(cfg->drop_without_demand))
        s->drop_without_demand = cfg->drop_without_demand;
    /* Whole-field gate: an old caller whose struct_size predates this field
     * reads as 0. 0 -> disabled (the default: MSF-01 §5 says publish only on
     * availability change or cache staleness), UINT64_MAX -> disabled, else the
     * caller's opt-in interval. The deadline is armed later, when the initial
     * catalog installs. */
    {
        uint64_t raw = 0;
        if (cfg->struct_size >= offsetof(moq_media_sender_cfg_t,
                                         catalog_refresh_interval_us) +
                                    sizeof(cfg->catalog_refresh_interval_us))
            raw = cfg->catalog_refresh_interval_us;
        s->catalog_refresh_interval_us =
            (raw == 0) ? SENDER_CATALOG_REFRESH_DISABLED : raw;
    }
    s->catalog_refresh_deadline_us = UINT64_MAX;   /* not armed yet */
    s->refresh_wake_deadline_us = UINT64_MAX;      /* no managed wake yet */
}


/* count * elem fits in size_t. Taking the count as uint64_t keeps the check
 * meaningful on a 32-bit size_t without a tautological comparison on LP64. */
bool sender_product_fits(uint64_t count, size_t elem)
{
    return count <= (uint64_t)(SIZE_MAX / elem);
}


/* MSF-01 6.1: a publisher that restarts MUST start each track's Group IDs
 * above any it published before. The draft-22 companion takes the approach
 * 6.1 suggests, best-effort: the first Group ID of every track of the instance
 * is the wallclock in milliseconds since the Unix epoch, read once before the
 * sender exists, and later groups count up from it. This guarantees the 6.1
 * ordering only while the clock moves forward between instances and an
 * instance never runs ahead of it (more groups than elapsed milliseconds);
 * neither holds in general, so restart identity remains the application's
 * responsibility. The legacy companion starts at 0. An AUTO companion reads
 * the base the same way, failing creation the same way, because it may
 * resolve to draft 22 and objects written before then are numbered from it.
 *
 * The base must be a Group ID: a clock that cannot be read (or reads before
 * the epoch) is MOQ_ERR_INTERNAL, and a reading above MOQ_QUIC_VARINT_MAX is
 * MOQ_ERR_LIMIT, both before any allocation or endpoint effect. A base of
 * exactly MOQ_QUIC_VARINT_MAX is valid; the next group is then refused with
 * MOQ_ERR_LIMIT where it is requested. */
moq_result_t sender_resolve_group_base(bool msf01, uint64_t *out)
{
    *out = 0;
    if (!msf01) return MOQ_OK;
    struct timespec ts;
    if (SENDER_CLOCK_REALTIME(&ts) != 0 || ts.tv_sec < 0 || ts.tv_nsec < 0 ||
        ts.tv_nsec >= SENDER_NS_PER_S)
        return MOQ_ERR_INTERNAL;
    uint64_t sec = (uint64_t)ts.tv_sec;
    if (sec > MOQ_QUIC_VARINT_MAX / 1000u) return MOQ_ERR_LIMIT;
    uint64_t ms = sec * 1000u + (uint64_t)ts.tv_nsec / 1000000u;
    if (ms > MOQ_QUIC_VARINT_MAX) return MOQ_ERR_LIMIT;
    *out = ms;
    return MOQ_OK;
}


/* RFC 3629: reject overlongs, surrogates and values above U+10FFFF. */
bool sender_utf8_valid(moq_bytes_t b)
{
    size_t i = 0;
    while (i < b.len) {
        uint8_t c = b.data[i];
        size_t n;
        uint8_t lo = 0x80, hi = 0xBF;
        if (c < 0x80) { i++; continue; }
        if (c >= 0xC2 && c <= 0xDF) n = 1;
        else if (c >= 0xE0 && c <= 0xEF) {
            n = 2;
            if (c == 0xE0) lo = 0xA0;
            else if (c == 0xED) hi = 0x9F;
        } else if (c >= 0xF0 && c <= 0xF4) {
            n = 3;
            if (c == 0xF0) lo = 0x90;
            else if (c == 0xF4) hi = 0x8F;
        } else return false;
        if (n > b.len - i - 1) return false;
        if (b.data[i + 1] < lo || b.data[i + 1] > hi) return false;
        for (size_t k = 2; k <= n; k++)
            if ((b.data[i + k] & 0xC0) != 0x80) return false;
        i += n + 1;
    }
    return true;
}


moq_result_t sender_prepare(const moq_media_sender_cfg_t *cfg,
                            sender_sizes_t *sz)
{
    moq_result_t rc = sender_validate_cfg(cfg);
    if (rc < 0) return rc;
    memset(sz, 0, sizeof(*sz));
    sz->queue_cap = cfg->queue_max_objects
        ? cfg->queue_max_objects : SENDER_DEFAULT_QUEUE_OBJECTS;
    sz->preq_cap = cfg->pre_ready_max_objects
        ? cfg->pre_ready_max_objects : SENDER_DEFAULT_PRE_READY_OBJECTS;
    /* One ring serves both phases; size it for the larger bound (+1 so a
     * full ring is distinguishable from empty). The +1 is taken in 64 bits:
     * the largest representable capacity is UINT32_MAX-1 (ring_cap
     * UINT32_MAX); UINT32_MAX itself would wrap the ring to zero. */
    uint64_t ring = (uint64_t)(sz->queue_cap > sz->preq_cap ? sz->queue_cap
                                                             : sz->preq_cap) + 1u;
    if (ring > UINT32_MAX) return MOQ_ERR_INVAL;
    sz->ring_cap = (uint32_t)ring;
    if (!sender_product_fits(sz->ring_cap, sizeof(sender_preq_entry_t)))
        return MOQ_ERR_INVAL;
    sz->ring_bytes = (size_t)sz->ring_cap * sizeof(sender_preq_entry_t);
    for (size_t i = 0; i < cfg->namespace_.count; i++) {
        size_t len = cfg->namespace_.parts[i].len;
        if (len > SIZE_MAX - sz->ns_bytes) return MOQ_ERR_INVAL;
        sz->ns_bytes += len;
    }
    size_t catalog_len = cfg->catalog_track.len
        ? cfg->catalog_track.len : (size_t)MOQ_MSF_CATALOG_TRACK_NAME_LEN;
    if (catalog_len > SIZE_MAX - sz->ns_bytes) return MOQ_ERR_INVAL;
    sz->ns_data_size = sz->ns_bytes + catalog_len;
    /* AUTO reads the base too: objects written before the draft is known
     * are numbered from it, and a legacy resolution moves them down by it. */
    bool msf01 = sender_cfg_companion(cfg) !=
                 MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01;
    rc = sender_resolve_group_base(msf01, &sz->group_base);
    if (rc != MOQ_OK) return rc;
    sz->alloc = *SENDER_CTOR_ALLOC();
    const moq_media_auth_cfg_t *auth = NULL;
    if (cfg->struct_size >= offsetof(moq_media_sender_cfg_t, resource_auth) +
                            sizeof(cfg->resource_auth))
        auth = cfg->resource_auth;
    return moq_media_auth_source_copy(&sz->resource_auth, auth, &sz->alloc);
}


moq_result_t sender_new(moq_endpoint_t *ep, bool owns,
                        const moq_media_sender_cfg_t *cfg,
                        sender_sizes_t *sz,
                        moq_media_sender_t **out)
{
    const moq_alloc_t *alloc = &sz->alloc;
    moq_media_sender_t *s = (moq_media_sender_t *)alloc->alloc(
        sizeof(*s), alloc->ctx);
    if (!s) {
        moq_media_auth_source_clear(&sz->resource_auth, alloc);
        return MOQ_ERR_NOMEM;
    }
    memset(s, 0, sizeof(*s));
#ifdef MOQ_MEDIA_SENDER_TESTING
    s->test_block_republish_obj = -1;
#endif
    s->alloc = *alloc;
    s->resource_auth = sz->resource_auth;
    memset(&sz->resource_auth, 0, sizeof(sz->resource_auth));
    s->ep = ep;
    s->owns_endpoint = owns;
    s->backpressure = cfg->backpressure;
    s->validate_cmaf = cfg->validate_cmaf;
    sender_copy_cfg_tail(s, cfg);
    s->group_base = sz->group_base;
    s->block_timeout_us = cfg->block_timeout_us
        ? cfg->block_timeout_us : SENDER_DEFAULT_BLOCK_TIMEOUT_US;
    s->queue_cap = sz->queue_cap;
    s->queue_byte_cap = cfg->queue_max_bytes
        ? cfg->queue_max_bytes : SENDER_DEFAULT_QUEUE_BYTES;
    s->preq_cap = sz->preq_cap;
    s->preq_byte_cap = cfg->pre_ready_max_bytes
        ? cfg->pre_ready_max_bytes : SENDER_DEFAULT_PRE_READY_BYTES;
    s->ring_cap = sz->ring_cap;
    /* Cleanup metadata is recorded before the buffers it describes are
     * requested, so a partial construction frees each block with the exact
     * size it was allocated with (sender_free reads these fields). */
    s->namespace_ = (moq_namespace_t){ NULL, cfg->namespace_.count };
    s->ns_data_size = sz->ns_data_size;
    s->stats.struct_size = (uint32_t)sizeof(s->stats);
    pthread_mutex_init(&s->mu, NULL);
    pthread_cond_init(&s->space_cv, NULL);

    /* Deep-copy the root content protections (validated in sender_validate_cfg;
     * sender_copy_root_cps leaves no partial state on failure). */
    {
        moq_result_t cprc = sender_copy_root_cps(s, cfg->content_protections,
                                                 cfg->content_protection_count);
        if (cprc < 0) { sender_free(s); return cprc; }
    }

    moq_bytes_t catalog = cfg->catalog_track;
    if (catalog.len == 0)
        catalog = (moq_bytes_t){
            (const uint8_t *)MOQ_MSF_CATALOG_TRACK_NAME,
            MOQ_MSF_CATALOG_TRACK_NAME_LEN };
    s->ns_parts = (moq_bytes_t *)s->alloc.alloc(
        cfg->namespace_.count * sizeof(moq_bytes_t), s->alloc.ctx);
    if (!s->ns_parts) { sender_free(s); return MOQ_ERR_NOMEM; }
    s->ns_data = (uint8_t *)s->alloc.alloc(sz->ns_data_size, s->alloc.ctx);
    if (!s->ns_data) { sender_free(s); return MOQ_ERR_NOMEM; }
    size_t off = 0;
    for (size_t i = 0; i < cfg->namespace_.count; i++) {
        memcpy(s->ns_data + off, cfg->namespace_.parts[i].data,
               cfg->namespace_.parts[i].len);
        s->ns_parts[i] = (moq_bytes_t){ s->ns_data + off,
                                        cfg->namespace_.parts[i].len };
        off += cfg->namespace_.parts[i].len;
    }
    memcpy(s->ns_data + off, catalog.data, catalog.len);
    s->catalog_name = s->ns_data + off;
    s->catalog_name_len = catalog.len;
    s->namespace_ = (moq_namespace_t){ s->ns_parts, cfg->namespace_.count };

    s->preq = (sender_preq_entry_t *)s->alloc.alloc(sz->ring_bytes, s->alloc.ctx);
    if (!s->preq) { sender_free(s); return MOQ_ERR_NOMEM; }
    memset(s->preq, 0, sz->ring_bytes);

    /* A private catalog track handle (not in the app's list; carries the
     * namespace advertisement and the retained catalog). */
    s->catalog_track = (moq_media_track_t *)s->alloc.alloc(
        sizeof(moq_media_track_t), s->alloc.ctx);
    if (!s->catalog_track) { sender_free(s); return MOQ_ERR_NOMEM; }
    memset(s->catalog_track, 0, sizeof(*s->catalog_track));
    s->catalog_track->is_catalog = true;

    /* ep == NULL is the test seam (moq_media_sender_test_new_cfg): the test
     * drives sender_hook directly, so skip the endpoint attach. Production
     * create/attach always passes a live endpoint. */
    if (ep) {
        moq_endpoint_hook_ops_t ops = {
            .pump = sender_hook,
            .next_deadline_us = sender_next_deadline_us,
            .has_pending_output = sender_has_pending_output,
        };
        moq_result_t arc = moq_endpoint_attach_hook(
            ep, MOQ_ENDPOINT_HOOK_SENDER, &ops, s);
        if (arc < 0) {
            track_free(s, s->catalog_track);
            s->catalog_track = NULL;
            sender_free(s);
            return arc;
        }
    }
    *out = s;
    return MOQ_OK;
}


moq_result_t moq_media_sender_attach(moq_endpoint_t *ep,
                                     const moq_media_sender_cfg_t *cfg,
                                     moq_media_sender_t **out)
{
    if (!ep || !out) return MOQ_ERR_INVAL;
    *out = NULL;
    sender_sizes_t sz;
    moq_result_t rc = sender_prepare(cfg, &sz);
    if (rc < 0) return rc;
    if (cfg->endpoint != NULL) {
        moq_media_auth_source_clear(&sz.resource_auth, &sz.alloc);
        return MOQ_ERR_INVAL;   /* attach borrows */
    }
    return sender_new(ep, false, cfg, &sz, out);
}


moq_result_t moq_media_sender_create(const moq_media_sender_cfg_t *cfg,
                                     moq_media_sender_t **out)
{
    if (!out) return MOQ_ERR_INVAL;
    *out = NULL;
    sender_sizes_t sz;
    moq_result_t rc = sender_prepare(cfg, &sz);   /* before any endpoint effect */
    if (rc < 0) return rc;
    if (cfg->endpoint == NULL) {
        moq_media_auth_source_clear(&sz.resource_auth, &sz.alloc);
        return MOQ_ERR_INVAL;   /* create owns */
    }

    moq_endpoint_t *ep = NULL;
    rc = moq_endpoint_connect(cfg->endpoint, &ep);
    if (rc < 0) {
        moq_media_auth_source_clear(&sz.resource_auth, &sz.alloc);
        return rc;
    }
    rc = sender_new(ep, true, cfg, &sz, out);
    if (rc < 0) {
        (void)moq_endpoint_stop(ep);
        moq_endpoint_destroy(ep);
        return rc;
    }
    return MOQ_OK;
}


void moq_media_sender_destroy(moq_media_sender_t *s)
{
    if (!s) return;

    moq_endpoint_detach_hook(s->ep, MOQ_ENDPOINT_HOOK_SENDER, s);

    /* The publisher lives on the network thread: tear it down there if the
     * endpoint is still live, else inline (single-threaded once detached
     * from a terminal endpoint -- the hook can no longer run). */
    if (s->pub) {
        if (moq_endpoint_post(s->ep, sender_destroy_pub_task,
                              s->pub) != MOQ_OK)
            moq_pub_destroy(s->pub);
        s->pub = NULL;
    }

    if (s->owns_endpoint) {
        (void)moq_endpoint_stop(s->ep);
        moq_endpoint_destroy(s->ep);
    }
    if (s->catalog_track) track_free(s, s->catalog_track);
    sender_free(s);
}


moq_result_t moq_media_sender_get_stats(const moq_media_sender_t *s,
                                        moq_media_sender_stats_t *out,
                                        size_t out_size)
{
    if (!s || !out || out_size < MEDIA_SENDER_STATS_V0_SIZE)
        return MOQ_ERR_INVAL;
    moq_media_sender_t *ms = (moq_media_sender_t *)(uintptr_t)s;
    /* Snapshot into a full local, then copy the clamped prefix to the caller
     * (frozen v0 floor + min(caller, current) copy; same contract as the
     * receiver outputs). */
    moq_media_sender_stats_t snap;
    pthread_mutex_lock(&ms->mu);
    snap = ms->stats;
    /* MEDIA accepted by write() and still queued -- the same population as
     * objects_written / objects_sent / objects_dropped / bytes_queued. A
     * private zero-byte END_OF_TRACK work item occupies a physical ring slot
     * but is NOT part of that population, so the physical depth
     * (preq_tail - preq_head, which capacity checks keep using) is not the
     * public answer. O(1): maintained at the enqueue/release chokepoints. */
    snap.objects_queued = ms->preq_media;
    snap.bytes_queued = ms->preq_bytes;
    pthread_mutex_unlock(&ms->mu);
    size_t n = out_size < sizeof(*out) ? out_size : sizeof(*out);
    snap.struct_size = (uint32_t)n;
    memcpy(out, &snap, n);
    return MOQ_OK;
}


/* The first published size of moq_media_sender_track_stats_t: the whole
 * struct as first released, through mpeg2ts_continuation_objects_discarded. */
#define MEDIA_SENDER_TRACK_STATS_V0_SIZE \
    (offsetof(moq_media_sender_track_stats_t, \
              mpeg2ts_continuation_objects_discarded) + sizeof(uint64_t))


moq_result_t moq_media_sender_get_track_stats(
    const moq_media_sender_t *s, const moq_media_track_t *track,
    moq_media_sender_track_stats_t *out, size_t out_size)
{
    if (!s || !track || !out || out_size < MEDIA_SENDER_TRACK_STATS_V0_SIZE)
        return MOQ_ERR_INVAL;
    moq_media_sender_t *ms = (moq_media_sender_t *)(uintptr_t)s;
    moq_media_sender_track_stats_t snap;
    pthread_mutex_lock(&ms->mu);
    /* Only a handle of this sender is dereferenced. */
    moq_media_track_t *t = NULL;
    for (size_t i = 0; i < ms->track_count; i++)
        if (ms->tracks[i] == track) { t = ms->tracks[i]; break; }
    if (!t) {
        pthread_mutex_unlock(&ms->mu);
        return MOQ_ERR_INVAL;
    }
    snap = t->tstats;
#ifdef MOQ_SERVICE_HAVE_MPEG2TS
    if (t->ts) {
        const sender_ts_t *ts = (const sender_ts_t *)t->ts;
        moq_mpeg2ts_packetizer_stats_t ps;
        if (moq_mpeg2ts_packetizer_get_stats(&ts->pz, &ps, sizeof(ps))
                == MOQ_OK) {
            snap.mpeg2ts_groups = ps.groups;
            snap.mpeg2ts_objects = ps.objects;
            snap.mpeg2ts_packets_in = ps.packets_in;
            snap.mpeg2ts_packets_dropped_before_random_access =
                ps.packets_dropped_before_random_access;
            snap.mpeg2ts_max_group_27mhz = ps.max_group_27mhz;
            snap.mpeg2ts_groups_over_2s = ps.groups_over_2s;
            snap.mpeg2ts_peak_rate_bps = ps.peak_rate_bps;
            snap.mpeg2ts_source_pcr_discontinuities =
                ps.source_pcr_discontinuities;
        }
        const moq_msf_mpeg2ts_t *rec = sender_ts_record(t);
        snap.mpeg2ts_mux_rate_below_peak =
            rec->has_mux_rate && rec->mux_rate < snap.mpeg2ts_peak_rate_bps;
        snap.mpeg2ts_program_gone =
            ts->filter && moq_mpeg2ts_program_filter_program_gone(ts->filter);
        snap.mpeg2ts_objects_parked = ts->parked_n;
        snap.mpeg2ts_partial_bytes_discarded = ts->partial_bytes_discarded;
        snap.mpeg2ts_continuation_objects_discarded = ts->continuation_discarded;
    }
#endif
    pthread_mutex_unlock(&ms->mu);
    size_t n = out_size < sizeof(*out) ? out_size : sizeof(*out);
    snap.struct_size = (uint32_t)n;
    memcpy(out, &snap, n);
    return MOQ_OK;
}


/* -- State accessors -------------------------------------------------- */

bool moq_media_sender_is_ready(const moq_media_sender_t *s)
{
    if (!s) return false;
    moq_media_sender_t *ms = (moq_media_sender_t *)(uintptr_t)s;
    pthread_mutex_lock(&ms->mu);
    /* A fatal sender is terminal, never ready: a rejected/cancelled namespace
     * is no longer announced, so readiness cannot hold once the sender is
     * fatal (and a not-yet-ready sender can never flip ready afterwards). */
    bool r = ms->ready && !ms->fatal;
    pthread_mutex_unlock(&ms->mu);
    return r;
}


size_t moq_media_sender_track_subscriptions(const moq_media_sender_t *s,
                                            const moq_media_track_t *track)
{
    if (!s || !track) return 0;
    moq_media_sender_t *ms = (moq_media_sender_t *)(uintptr_t)s;
    size_t n = 0;
    pthread_mutex_lock(&ms->mu);
    /* Validate ownership (the catalog track is private, not in tracks[]). */
    bool owned = (ms->catalog_track == track);
    if (!owned)
        for (size_t i = 0; i < ms->track_count; i++)
            if (ms->tracks[i] == track) { owned = true; break; }
    if (owned) n = track->active_subs;
    pthread_mutex_unlock(&ms->mu);
    return n;
}


moq_result_t moq_media_sender_take_new_group_request(moq_media_sender_t *s,
                                                     moq_media_track_t *track,
                                                     uint64_t *requested_group)
{
    if (!s || !track) return MOQ_ERR_INVAL;
    moq_result_t rc = MOQ_ERR_INVAL;
    pthread_mutex_lock(&s->mu);
    for (size_t i = 0; i < s->track_count; i++) {
        if (s->tracks[i] != track) continue;
        rc = MOQ_DONE;
        if (track->new_group_pending) {
            track->new_group_pending = false;
            if (requested_group) *requested_group = track->new_group_value;
            rc = MOQ_OK;
        }
        break;
    }
    pthread_mutex_unlock(&s->mu);
    return rc;
}


bool moq_media_sender_track_has_subscriber(const moq_media_sender_t *s,
                                           const moq_media_track_t *track)
{
    return moq_media_sender_track_subscriptions(s, track) > 0;
}


bool moq_media_sender_has_media_subscriber(const moq_media_sender_t *s)
{
    if (!s) return false;
    moq_media_sender_t *ms = (moq_media_sender_t *)(uintptr_t)s;
    bool any = false;
    pthread_mutex_lock(&ms->mu);
    for (size_t i = 0; i < ms->track_count; i++) {
        moq_media_track_t *t = ms->tracks[i];
        if (t->removed || !track_is_app_media(t)) continue;
        if (t->active_subs > 0) { any = true; break; }
    }
    pthread_mutex_unlock(&ms->mu);
    return any;
}


bool moq_media_sender_is_closed(const moq_media_sender_t *s)
{
    if (!s) return true;
    return moq_endpoint_is_closed(s->ep);
}


bool moq_media_sender_is_fatal(const moq_media_sender_t *s)
{
    if (!s) return false;
    moq_media_sender_t *ms = (moq_media_sender_t *)(uintptr_t)s;
    pthread_mutex_lock(&ms->mu);
    bool f = ms->fatal;
    pthread_mutex_unlock(&ms->mu);
    return f || moq_endpoint_is_fatal(s->ep);
}


uint64_t moq_media_sender_fatal_code(const moq_media_sender_t *s)
{
    if (!s) return 0;
    moq_media_sender_t *ms = (moq_media_sender_t *)(uintptr_t)s;
    pthread_mutex_lock(&ms->mu);
    bool f = ms->fatal;
    uint64_t code = ms->fatal_code;
    pthread_mutex_unlock(&ms->mu);
    if (f) return code;
    return moq_endpoint_fatal_code(s->ep);
}
