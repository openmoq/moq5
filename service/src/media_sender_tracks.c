#include "media_sender_internal.h"
#include "media_sender_tracks_internal.h"
#include "media_sender_catalog_internal.h"
#include "media_sender_complete_internal.h"
#include "media_sender_mpeg2ts_internal.h"
#include "media_sender_write_internal.h"

moq_bytes_t default_role(moq_media_type_t t)
{
    if (t == MOQ_MEDIA_TYPE_AUDIO)
        return (moq_bytes_t){ (const uint8_t *)"audio", 5 };
    return (moq_bytes_t){ (const uint8_t *)"video", 5 };
}


/* -- Track lookup ---------------------------------------------------- */

bool removed_pubtrack_with_name(moq_media_sender_t *s,
                                moq_media_track_t *const *tracks,
                                size_t n, moq_bytes_t name);  /* below */

#ifdef MOQ_MEDIA_SENDER_TESTING
/* Test-only gap observation in add_track: called after argument validation
 * and the initial state checks, before any allocation or the registration
 * critical section, so a test can drive a terminal transition into the gap. */
static void (*g_test_add_gap)(void *ctx);

static void *g_test_add_gap_ctx;

void moq_media_sender_test_set_add_track_gap_hook(void (*fn)(void *), void *ctx);

void moq_media_sender_test_set_add_track_gap_hook(void (*fn)(void *), void *ctx)
{
    g_test_add_gap = fn;
    g_test_add_gap_ctx = ctx;
}

#define SENDER_ADD_TRACK_GAP_HOOK() \
    do { if (g_test_add_gap) g_test_add_gap(g_test_add_gap_ctx); } while (0)

/* Test-only pre-traversal observation for add_track's content-protection
 * ref-id walk: called with the ref count immediately before the ids are
 * read. Returning false stops add_track with MOQ_ERR_INVAL, which keeps a
 * fixture with an intrinsically unrepresentable count bounded (no element is
 * read) while still recording that the walk was about to start. */
static bool (*g_test_refs_walk)(size_t count, void *ctx);

static void *g_test_refs_walk_ctx;

void moq_media_sender_test_set_refs_walk_hook(bool (*fn)(size_t, void *), void *ctx);

void moq_media_sender_test_set_refs_walk_hook(bool (*fn)(size_t, void *), void *ctx)
{
    g_test_refs_walk = fn;
    g_test_refs_walk_ctx = ctx;
}

#define SENDER_REFS_WALK_HOOK(count) \
    do { if (g_test_refs_walk && !g_test_refs_walk((count), g_test_refs_walk_ctx)) \
             return MOQ_ERR_INVAL; } while (0)


#else
#define SENDER_ADD_TRACK_GAP_HOOK() ((void)0)

#define SENDER_REFS_WALK_HOOK(count) ((void)0)


#endif


void track_free(moq_media_sender_t *s, moq_media_track_t *t)
{
    if (!t) return;
    moq_media_auth_operation_clear(&t->publish_auth, &s->alloc);
    sender_ts_free(s, t);
    if (t->ext)
        s->alloc.free(t->ext, t->ext_block_size, s->alloc.ctx);
    if (t->cp_ref_ids)
        s->alloc.free(t->cp_ref_ids,
            t->cp_ref_id_count * sizeof(moq_bytes_t), s->alloc.ctx);
    /* Generated timeline allocations (the depends bytes themselves belong to
     * the media track's `strings`, so only the index array is freed here). */
    if (t->depends)
        s->alloc.free(t->depends,
            t->depends_count * sizeof(moq_bytes_t), s->alloc.ctx);
    if (t->sap_recs)
        s->alloc.free(t->sap_recs,
            t->sap_rec_cap * sizeof(sender_sap_rec_t), s->alloc.ctx);
    if (t->mt_recs)
        s->alloc.free(t->mt_recs,
            t->mt_rec_cap * sizeof(moq_msf_media_timeline_record_t),
            s->alloc.ctx);
    if (t->tl_pend) moq_rcbuf_decref(t->tl_pend);
    if (t->init_ref_alloc)
        s->alloc.free(t->init_ref_alloc, t->init_ref_alloc_size, s->alloc.ctx);
    if (t->strings) s->alloc.free(t->strings, t->strings_size, s->alloc.ctx);
    s->alloc.free(t, sizeof(*t), s->alloc.ctx);
}


/* Build (but do not register) a generated timeline track for `media`. With
 * media_timeline=false this is the SAP eventtimeline "<media>.sap"; with
 * media_timeline=true it is the MSF §7 mediatimeline "<media>.timeline". Both
 * 'depend' on the media track. Returns NULL on allocation failure. Borrows
 * media->name (stable for the sender's life). */
/* The generated sibling names are the media name plus one of these suffixes.
 * add_track derives and CHECKS each enabled sibling's name length before any
 * allocation and passes it in, so the addition is computed once. */
static const char SENDER_SAP_SUFFIX[4] = { '.', 's', 'a', 'p' };

static const char SENDER_MT_SUFFIX[9] =
    { '.', 't', 'i', 'm', 'e', 'l', 'i', 'n', 'e' };


static moq_media_track_t *sender_make_timeline(moq_media_sender_t *s,
                                               moq_media_track_t *media,
                                               uint32_t history_groups,
                                               bool media_timeline,
                                               size_t namelen)
{
    const char *suffix = media_timeline ? SENDER_MT_SUFFIX : SENDER_SAP_SUFFIX;
    size_t suffixlen = media_timeline ? sizeof(SENDER_MT_SUFFIX)
                                      : sizeof(SENDER_SAP_SUFFIX);
    moq_media_track_t *tl = (moq_media_track_t *)s->alloc.alloc(
        sizeof(*tl), s->alloc.ctx);
    uint8_t *nbuf = (uint8_t *)s->alloc.alloc(namelen, s->alloc.ctx);
    moq_bytes_t *deps = (moq_bytes_t *)s->alloc.alloc(
        sizeof(moq_bytes_t), s->alloc.ctx);
    if (!tl || !nbuf || !deps) {
        if (tl) s->alloc.free(tl, sizeof(*tl), s->alloc.ctx);
        if (nbuf) s->alloc.free(nbuf, namelen, s->alloc.ctx);
        if (deps) s->alloc.free(deps, sizeof(moq_bytes_t), s->alloc.ctx);
        return NULL;
    }
    memset(tl, 0, sizeof(*tl));
    memcpy(nbuf, media->name.data, media->name.len);
    memcpy(nbuf + media->name.len, suffix, suffixlen);
    tl->strings = nbuf;
    tl->strings_size = namelen;
    tl->name = (moq_bytes_t){ nbuf, namelen };
    tl->is_live = media->is_live;
    if (media_timeline) {
        tl->is_media_timeline = true;
        tl->mt_history_groups = history_groups ? history_groups
                            : SENDER_DEFAULT_MEDIA_TIMELINE_HISTORY_GROUPS;
    } else {
        tl->is_event_timeline = true;
        tl->sap_history_groups = history_groups ? history_groups
                                                : SENDER_DEFAULT_SAP_HISTORY_GROUPS;
    }
    deps[0] = media->name;   /* borrows the media track's stable name bytes */
    tl->depends = deps;
    tl->depends_count = 1;
    return tl;
}


/* True if a track named `name` is already registered (mu held). Catalog track
 * names must be unique, so this guards both app tracks and generated ones. */
static bool sender_init_ref_taken(const moq_media_sender_t *s, moq_bytes_t id)
{
    for (size_t i = 0; i < s->track_count; i++) {
        moq_bytes_t r = s->tracks[i]->init_ref;
        if (r.len == id.len && r.len > 0 && memcmp(r.data, id.data, r.len) == 0)
            return true;
    }
    return false;
}


/* Assign t's initDataList id (see moq_media_track.init_ref). Every track ever
 * registered on this sender -- removed ones included, since their objects and
 * ids outlive removal -- is considered, so an id is never rebound to
 * different bytes: identical init bytes share the existing id; otherwise the
 * id is the track's name, or "<name>~<n>" with the least n that is free when
 * the name already identifies other bytes (a removed name reused for new
 * init data). Returns false on allocation failure.
 * CALLER MUST HOLD s->mu; t is not yet registered. */
static bool sender_assign_init_ref(moq_media_sender_t *s, moq_media_track_t *t)
{
    if (!sender_init_by_reference(t))
        return true;
    for (size_t i = 0; i < s->track_count; i++) {
        const moq_media_track_t *u = s->tracks[i];
        if (u->init_ref.len > 0 && u->init_data.len == t->init_data.len &&
            memcmp(u->init_data.data, t->init_data.data,
                   t->init_data.len) == 0) {
            t->init_ref = u->init_ref;
            return true;
        }
    }
    if (!sender_init_ref_taken(s, t->name)) {
        t->init_ref = t->name;
        return true;
    }
    /* name + '~' + up to 20 decimal digits. A free suffix exists within
     * track_count + 1 tries, since at most track_count ids are taken. */
    size_t cap = t->name.len + 1 + 20;
    uint8_t *buf = (uint8_t *)s->alloc.alloc(cap, s->alloc.ctx);
    if (!buf) return false;
    memcpy(buf, t->name.data, t->name.len);
    for (uint64_t n = 1;; n++) {
        uint8_t digits[20];
        size_t nd = 0;
        for (uint64_t v = n; v > 0; v /= 10)
            digits[nd++] = (uint8_t)('0' + v % 10);
        size_t w = t->name.len;
        buf[w++] = '~';
        while (nd > 0) buf[w++] = digits[--nd];
        moq_bytes_t id = { buf, w };
        if (!sender_init_ref_taken(s, id)) {
            t->init_ref = id;
            t->init_ref_alloc = buf;
            t->init_ref_alloc_size = cap;
            return true;
        }
    }
}


static bool sender_name_taken(const moq_media_sender_t *s, moq_bytes_t name)
{
    for (size_t i = 0; i < s->track_count; i++) {
        if (s->tracks[i]->removed) continue;   /* a removed name may be reused */
        moq_bytes_t n = s->tracks[i]->name;
        if (n.len == name.len && memcmp(n.data, name.data, n.len) == 0)
            return true;
    }
    return false;
}


/* True if a REMOVED track named `name` still holds a publisher track (its
 * teardown -- end + remove -- has not finished). A replacement same-name track
 * must defer its own registration until the old publisher track is gone, or
 * moq_pub_add_track would reject the duplicate identity. Network thread: walks a
 * caller-held track snapshot (so a concurrent app-thread realloc cannot free the
 * vector mid-scan) and reads `removed` (app-written under s->mu) under the lock.
 * pub_track is network-confined and name bytes are immutable. The caller does not
 * hold s->mu, so taking it here is safe. */
bool removed_pubtrack_with_name(moq_media_sender_t *s,
                                moq_media_track_t *const *tracks,
                                size_t n, moq_bytes_t name)
{
    bool found = false;
    pthread_mutex_lock(&s->mu);
    for (size_t i = 0; i < n; i++) {
        const moq_media_track_t *t = tracks[i];
        if (!t->removed || !t->pub_track) continue;
        if (t->name.len == name.len &&
            memcmp(t->name.data, name.data, name.len) == 0) {
            found = true;
            break;
        }
    }
    pthread_mutex_unlock(&s->mu);
    return found;
}


/* True if a removed track named `name` has not finished tearing down (mu held).
 * A post-ready same-name re-add must wait (retryable) until the old track is
 * fully gone, so the removal generation is published before the replacement and
 * the receiver sees remove-then-add rather than a changed tuple. */
static bool removed_samename_pending(const moq_media_sender_t *s,
                                     moq_bytes_t name)
{
    for (size_t i = 0; i < s->track_count; i++) {
        const moq_media_track_t *t = s->tracks[i];
        if (!t->removed || t->teardown_done) continue;
        if (t->name.len == name.len &&
            memcmp(t->name.data, name.data, name.len) == 0)
            return true;
    }
    return false;
}


/* Append one moq_media_track_t* to s->tracks (mu held). Returns false on
 * realloc failure (the track is NOT freed -- the caller owns cleanup). */
static bool sender_tracks_push(moq_media_sender_t *s, moq_media_track_t *t)
{
    if (s->track_count == s->track_cap) {
        size_t nc = s->track_cap ? s->track_cap * 2 : 4;
        moq_media_track_t **nt = (moq_media_track_t **)s->alloc.realloc(
            s->tracks, s->track_cap * sizeof(*nt), nc * sizeof(*nt),
            s->alloc.ctx);
        if (!nt) return false;
        s->tracks = nt;
        s->track_cap = nc;
    }
    s->tracks[s->track_count++] = t;
    return true;
}


/* Every size add_track derives from a validated track cfg, each checked for
 * representability BEFORE the first allocation, copy or registration: the
 * media spans plus the resolved ref-id bytes (one strings buffer), the
 * ref-id index product, and each ENABLED generated sibling's name. An
 * unrepresentable configuration is a cfg error (MOQ_ERR_INVAL), never a
 * wrapped request. */
typedef struct track_sizes {
    size_t strings;       /* media spans + resolved ref-id bytes */
    size_t cpref_bytes;
    size_t cpref_index;   /* count * sizeof(moq_bytes_t) */
    size_t sap_name;      /* 0 when the sibling is disabled */
    size_t mt_name;
} track_sizes_t;


static moq_result_t track_derive_sizes(const moq_media_track_cfg_t *cfg,
                                       track_sizes_t *sz)
{
    memset(sz, 0, sizeof(*sz));
    size_t media = 0;
    if (!sender_sum_fits(&media, cfg->name.len) ||
        !sender_sum_fits(&media, cfg->codec.len) ||
        !sender_sum_fits(&media, cfg->init_data.len) ||
        !sender_sum_fits(&media, cfg->role.len) ||
        !sender_sum_fits(&media, cfg->lang.len) ||
        !sender_sum_fits(&media, cfg->label.len) ||
        !sender_sum_fits(&media, cfg->channel_config.len) ||
        !sender_sum_fits(&media, cfg->track_properties.len))
        return MOQ_ERR_INVAL;
    /* Each resolved ref id contributes its bytes to the SAME buffer. Every
     * subtotal addition is checked, so the final sum below can never receive
     * an already-wrapped subtotal. */
    for (size_t i = 0; i < cfg->content_protection_ref_id_count; i++)
        if (!sender_sum_fits(&sz->cpref_bytes,
                             cfg->content_protection_ref_ids[i].len))
            return MOQ_ERR_INVAL;
    sz->strings = media;
    if (!sender_sum_fits(&sz->strings, sz->cpref_bytes))
        return MOQ_ERR_INVAL;
    /* The count product was checked before the ids were traversed. */
    sz->cpref_index = cfg->content_protection_ref_id_count * sizeof(moq_bytes_t);
    if (cfg->emit_sap_timeline) {
        sz->sap_name = cfg->name.len;
        if (!sender_sum_fits(&sz->sap_name, sizeof(SENDER_SAP_SUFFIX)))
            return MOQ_ERR_INVAL;
    }
    if (cfg->emit_media_timeline) {
        sz->mt_name = cfg->name.len;
        if (!sender_sum_fits(&sz->mt_name, sizeof(SENDER_MT_SUFFIX)))
            return MOQ_ERR_INVAL;
    }
    return MOQ_OK;
}


/* Read a caller's track cfg into a full current struct: the v0 prefix, then
 * each appended field only when struct_size covers all of it, and a has_X /
 * X pair or an array / count pair only when it covers the whole pair. Fields
 * left out keep their zero default. The caller has checked struct_size >=
 * MEDIA_TRACK_CFG_MIN_SIZE. */
static void sender_track_cfg_read(const moq_media_track_cfg_t *in,
                                  moq_media_track_cfg_t *out)
{
    memset(out, 0, sizeof(*out));
    memcpy(out, in, MEDIA_TRACK_CFG_MIN_SIZE);
#define TCFG_HAS(f) \
    (in->struct_size >= offsetof(moq_media_track_cfg_t, f) + sizeof(in->f))
    if (TCFG_HAS(track_duration_ms)) {
        out->has_track_duration = in->has_track_duration;
        out->track_duration_ms = in->track_duration_ms;
    }
    if (TCFG_HAS(emit_media_timeline))
        out->emit_media_timeline = in->emit_media_timeline;
    if (TCFG_HAS(media_timeline_history_groups))
        out->media_timeline_history_groups = in->media_timeline_history_groups;
    if (TCFG_HAS(alt_group)) {
        out->has_alt_group = in->has_alt_group;
        out->alt_group = in->alt_group;
    }
    if (TCFG_HAS(publisher_priority)) {
        out->has_publisher_priority = in->has_publisher_priority;
        out->publisher_priority = in->publisher_priority;
    }
    if (TCFG_HAS(extension_count)) {
        out->extensions = in->extensions;
        out->extension_count = in->extension_count;
    }
    if (TCFG_HAS(mpeg2ts_app_objects))
        out->mpeg2ts_app_objects = in->mpeg2ts_app_objects;
    if (TCFG_HAS(mpeg2ts_object_packets))
        out->mpeg2ts_object_packets = in->mpeg2ts_object_packets;
    if (TCFG_HAS(mpeg2ts_min_group_ms))
        out->mpeg2ts_min_group_ms = in->mpeg2ts_min_group_ms;
    if (TCFG_HAS(mpeg2ts_forward_before_random_access))
        out->mpeg2ts_forward_before_random_access =
            in->mpeg2ts_forward_before_random_access;
    if (TCFG_HAS(track_properties))
        out->track_properties = in->track_properties;
    if (TCFG_HAS(dynamic_groups))
        out->dynamic_groups = in->dynamic_groups;
    if (TCFG_HAS(render_group) && in->has_render_group) {
        out->has_render_group = true;
        out->render_group = in->render_group;
    }
    if (TCFG_HAS(label) && in->has_label) {
        out->has_label = true;
        out->label = in->label;
    }
    if (TCFG_HAS(group_order))
        out->group_order = in->group_order;
#undef TCFG_HAS
    out->struct_size = (uint32_t)sizeof(*out);
}


moq_result_t moq_media_sender_add_track(moq_media_sender_t *s,
                                        const moq_media_track_cfg_t *cfg,
                                        moq_media_track_t **out)
{
    if (out) *out = NULL;
    if (!s || !cfg || !out) return MOQ_ERR_INVAL;
    if (cfg->struct_size < MEDIA_TRACK_CFG_MIN_SIZE)
        return MOQ_ERR_INVAL;
    /* Prefix-safe by struct_size: read the caller's (possibly older/smaller,
     * or newer/larger) cfg into a zero-filled local, so appended fields an
     * older caller lacks default to absent and a newer caller's unknown
     * trailing fields are ignored. All reads below use the local copy. */
    moq_media_track_cfg_t cfg_local;
    sender_track_cfg_read(cfg, &cfg_local);
    cfg = &cfg_local;
    if (cfg->name.len == 0 || !cfg->name.data) return MOQ_ERR_INVAL;
    if (cfg->media_type != MOQ_MEDIA_TYPE_VIDEO &&
        cfg->media_type != MOQ_MEDIA_TYPE_AUDIO)
        return MOQ_ERR_INVAL;
    bool is_ts = cfg->packaging == MOQ_MEDIA_PACKAGING_MPEG2TS;
    if (cfg->packaging != MOQ_MEDIA_PACKAGING_RAW &&
        cfg->packaging != MOQ_MEDIA_PACKAGING_CMAF && !is_ts)
        return MOQ_ERR_INVAL;
#ifndef MOQ_SERVICE_HAVE_MPEG2TS
    if (is_ts) return MOQ_ERR_UNSUPPORTED;   /* not in this build */
#endif
    const moq_msf_mpeg2ts_t *ts_rec = NULL;
    {
        moq_result_t erc = sender_check_extensions(cfg, &ts_rec);
        if (erc < 0) return erc;
    }
    /* Every optional span must be coherent: a non-zero length with a NULL
     * data pointer would crash the deep-copy below. */
    if ((cfg->codec.len && !cfg->codec.data) ||
        (cfg->init_data.len && !cfg->init_data.data) ||
        (cfg->role.len && !cfg->role.data) ||
        (cfg->lang.len && !cfg->lang.data) ||
        (cfg->label.len && !cfg->label.data) ||
        (cfg->channel_config.len && !cfg->channel_config.data) ||
        (cfg->track_properties.len && !cfg->track_properties.data))
        return MOQ_ERR_INVAL;
    /* MSF-01: an audio/video track has an inherent codec, so it MUST carry the
     * conditional catalog fields for that codec -- the sender never emits a
     * track that is invalid by construction. codec (5.2.18) and maximum bitrate
     * (5.2.22) are required for both media types; samplerate (5.2.28) and
     * channelConfig (5.2.29) are additionally required for audio. media_type is
     * always VIDEO or AUDIO here (checked above). Generated timeline tracks
     * (.sap/.timeline) are synthesized on a separate internal path and never
     * reach this public entry, so they remain exempt. */
    /* An MPEG2TS track is a multiplex: it has no inherent codec, and
     * samplerate / channelConfig describe one elementary stream, not it. */
    if (cfg->codec.len == 0 && !is_ts) return MOQ_ERR_INVAL;   /* 5.2.18 */
    if (cfg->bitrate == 0) return MOQ_ERR_INVAL;         /* 5.2.22 max bitrate */
    if (cfg->media_type == MOQ_MEDIA_TYPE_AUDIO && !is_ts) {
        if (cfg->samplerate == 0) return MOQ_ERR_INVAL;          /* 5.2.28 */
        if (cfg->channel_config.len == 0) return MOQ_ERR_INVAL;  /* 5.2.29 */
    }
    /* CMSF-01 3.1 requires the CMAF header in the catalog on every
     * transport. Refuse missing init before allocation or registration,
     * including unresolved AUTO. Object validation opt-out does not waive it. */
    if (cfg->packaging == MOQ_MEDIA_PACKAGING_CMAF && cfg->init_data.len == 0)
        return MOQ_ERR_INVAL;
    /* contentProtectionRefIDs: each id must be coherent AND resolve to a
     * configured root contentProtections entry (catalog coherence, not DRM
     * semantics). Root entries are immutable after construction, so this read
     * needs no lock. */
    if (cfg->content_protection_ref_id_count > 0 &&
        !cfg->content_protection_ref_ids)
        return MOQ_ERR_INVAL;
    /* The index product must be representable before a single id is read: an
     * intrinsically impossible count is refused, not traversed. */
    if (!sender_product_fits(cfg->content_protection_ref_id_count,
                             sizeof(moq_bytes_t)))
        return MOQ_ERR_INVAL;
    SENDER_REFS_WALK_HOOK(cfg->content_protection_ref_id_count);
    for (size_t i = 0; i < cfg->content_protection_ref_id_count; i++) {
        moq_bytes_t ref = cfg->content_protection_ref_ids[i];
        if (ref.len == 0 || !ref.data) return MOQ_ERR_INVAL;
        if (!sender_has_cp_ref(s, ref)) return MOQ_ERR_INVAL;
    }
    /* Derive every allocation size now, with the other cfg-shape checks and
     * before any state check, allocation, copy or registration. */
    track_sizes_t sz;
    {
        moq_result_t drc = track_derive_sizes(cfg, &sz);
        if (drc < 0) return drc;
    }
    if (!sender_utf8_valid(cfg->label)) return MOQ_ERR_INVAL;
    /* MSF §5.2.35: trackDuration MUST NOT appear on a live track. */
    if (cfg->has_track_duration && cfg->is_live) return MOQ_ERR_INVAL;
    /* Also when no version is known yet to check the properties against. */
    if (cfg->group_order > MOQ_GROUP_ORDER_DESCENDING) return MOQ_ERR_INVAL;

    /* A terminal sender (fatal, or its endpoint closed) registers nothing:
     * checked here outside s->mu (sender_terminal takes it), and again inside
     * the registration critical section below with the ep->mu-free probe, so
     * a transition in between cannot slip a track in. */
    if (sender_terminal(s)) return MOQ_ERR_CLOSED;

    /* add_track is legal both before and after READY : a post-ready add
     * registers the track and triggers an independent catalog republish. Once
     * completion has been requested (§11.3) the sender is terminal. */
    pthread_mutex_lock(&s->mu);
    if (s->fatal) { pthread_mutex_unlock(&s->mu); return MOQ_ERR_CLOSED; }
    if (s->completing) { pthread_mutex_unlock(&s->mu); return MOQ_ERR_WRONG_STATE; }
    moq_version_t negotiated = s->transport_version;
    pthread_mutex_unlock(&s->mu);
    SENDER_ADD_TRACK_GAP_HOOK();

    /* Track Properties are checked here, as the publisher facade will check
     * them, so an unpublishable set is refused at the call rather than
     * failing the sender when the track activates: against the negotiated
     * version once known, before that against every version the endpoint
     * offers (any may be negotiated). */
    if (cfg->track_properties.len > 0 || cfg->dynamic_groups ||
        cfg->group_order != MOQ_GROUP_ORDER_DEFAULT) {
        moq_version_t versions[MOQ_ENDPOINT_MAX_VERSIONS];
        size_t nv = 1;
        if (negotiated) versions[0] = negotiated;
        else nv = moq_endpoint_offered_versions_internal(s->ep, versions,
                                                         MOQ_ENDPOINT_MAX_VERSIONS);
        for (size_t i = 0; i < nv && i < MOQ_ENDPOINT_MAX_VERSIONS; i++) {
            moq_result_t vrc = moq_core_internal_track_properties_validate(
                versions[i], cfg->track_properties.data,
                cfg->track_properties.len, cfg->dynamic_groups,
                cfg->group_order);
            if (vrc == MOQ_ERR_INVAL || vrc == MOQ_ERR_NOMEM) return vrc;
        }
    }

    /* Deep-copy every span into one buffer so the cfg need not outlive the
     * call and the catalog encode borrows stable bytes. The contentProtection
     * ref-id bytes share that buffer; their moq_bytes_t[] index is a separate
     * (aligned) allocation. */
    size_t ncpref = cfg->content_protection_ref_id_count;
    size_t need = sz.strings;          /* derived and checked above */
    moq_media_track_t *t = (moq_media_track_t *)s->alloc.alloc(
        sizeof(*t), s->alloc.ctx);
    uint8_t *buf = need ? (uint8_t *)s->alloc.alloc(need, s->alloc.ctx) : NULL;
    moq_bytes_t *cprefs = ncpref
        ? (moq_bytes_t *)s->alloc.alloc(sz.cpref_index, s->alloc.ctx)
        : NULL;
    if (!t || (need && !buf) || (ncpref && !cprefs)) {
        if (t) s->alloc.free(t, sizeof(*t), s->alloc.ctx);
        if (buf) s->alloc.free(buf, need, s->alloc.ctx);
        if (cprefs) s->alloc.free(cprefs, sz.cpref_index, s->alloc.ctx);
        return MOQ_ERR_NOMEM;
    }
    memset(t, 0, sizeof(*t));
    t->strings = buf;
    t->strings_size = need;
    t->cp_ref_ids = cprefs;
    t->cp_ref_id_count = ncpref;
    size_t off = 0;
#define COPY_SPAN(dst, src) do { \
        if ((src).len) { memcpy(buf + off, (src).data, (src).len); \
            (dst) = (moq_bytes_t){ buf + off, (src).len }; off += (src).len; } \
    } while (0)
    COPY_SPAN(t->name, cfg->name);
    COPY_SPAN(t->codec, cfg->codec);
    COPY_SPAN(t->init_data, cfg->init_data);
    COPY_SPAN(t->role, cfg->role);
    COPY_SPAN(t->lang, cfg->lang);
    COPY_SPAN(t->label, cfg->label);
    COPY_SPAN(t->channel_config, cfg->channel_config);
    COPY_SPAN(t->track_properties, cfg->track_properties);
#undef COPY_SPAN
    /* Read from the owned copy. Only a RAW track of a sender that can
     * synthesize LOC-04 (AUTO) needs it; it gates typed capture times in
     * write(). */
    if (cfg->packaging == MOQ_MEDIA_PACKAGING_RAW && s->companion_auto)
        t->track_scale = sender_track_scale_of(t->track_properties);
    /* contentProtection ref ids: copy bytes into the shared buffer, index via
     * the dedicated array (validated above; each ref.len > 0). */
    for (size_t i = 0; i < ncpref; i++) {
        moq_bytes_t r = cfg->content_protection_ref_ids[i];
        memcpy(buf + off, r.data, r.len);
        cprefs[i] = (moq_bytes_t){ buf + off, r.len };
        off += r.len;
    }
    t->media_type = cfg->media_type;
    t->packaging = cfg->packaging;
    /* A TS track's timing travels in the packets: its catalog carries a
     * timescale only when the app gives one. */
    t->timescale = cfg->timescale ? cfg->timescale : is_ts ? 0u : 1000000u;
    t->is_live = cfg->is_live;
    t->has_track_duration = cfg->has_track_duration;
    t->track_duration_ms = cfg->track_duration_ms;
    t->has_alt_group = cfg->has_alt_group;
    t->alt_group = cfg->alt_group;
    t->has_publisher_priority = cfg->has_publisher_priority;
    t->publisher_priority = cfg->publisher_priority;
    t->has_render_group = cfg->has_render_group;
    t->render_group = cfg->render_group;
    t->has_label = cfg->has_label;
    t->group_order = cfg->group_order;
    t->has_max_grp_sap = cfg->has_max_grp_sap;
    t->max_grp_sap = cfg->max_grp_sap;
    t->has_max_obj_sap = cfg->has_max_obj_sap;
    t->max_obj_sap = cfg->max_obj_sap;
    t->dynamic_groups = cfg->dynamic_groups;

    /* Parse a CMAF init segment once to cache its track_ID, so CMAF
     * validation can check object track_IDs against the init (CMSF §3.3), and
     * to enforce protected-track coherence (below). A non-CMAF track or an
     * unparseable init simply leaves the track_ID absent. */
    bool cmaf_init_parsed = false;
    moq_cmaf_init_info_t ii;
    moq_cmaf_init_info_init(&ii);
    if (t->packaging == MOQ_MEDIA_PACKAGING_CMAF && t->init_data.len > 0) {
        cmaf_init_parsed = (moq_cmaf_parse_init_sized(t->init_data, &ii, sizeof(ii)) == MOQ_OK);
        if (cmaf_init_parsed && ii.track_id != 0) {
            t->has_cmaf_init_track_id = true;
            t->cmaf_init_track_id = ii.track_id;
        }
    }

    /* CMSF §4.2: a CMAF track that declares contentProtectionRefIDs is
     * CENC-encrypted (§4.1.2), so its init data MUST carry the CENC boxes
     * (sinf/schm/schi/tenc). Authoring an encrypted track without protected
     * init would emit an internally inconsistent catalog, so this is always-on
     * catalog coherence -- like the ref-id-resolution check above -- not gated
     * under validate_cmaf. (No defaultKID<->tenc comparison or DRM semantics.) */
    if (t->packaging == MOQ_MEDIA_PACKAGING_CMAF && ncpref > 0) {
        if (t->init_data.len == 0 || !cmaf_init_parsed || !ii.has_cenc) {
            track_free(s, t);
            return MOQ_ERR_INVAL;
        }
    }
    t->width = cfg->width;
    t->height = cfg->height;
    t->framerate_millis = cfg->framerate_millis;
    t->samplerate = cfg->samplerate;
    t->bitrate = cfg->bitrate;

    /* The packaging-extension records, and an MPEG2TS track's own rules. */
    {
        moq_result_t xrc = sender_copy_extensions(s, t, ts_rec);
        if (xrc == MOQ_OK && is_ts) xrc = sender_ts_prepare(s, t, cfg);
        if (xrc < 0) { track_free(s, t); return xrc; }
    }

    /* Build the generated timeline siblings before the lock (they borrow the
     * media track's stable name bytes). A media track may opt into both the SAP
     * eventtimeline and the MSF media timeline; each is an independent generated
     * track. Registered atomically with the media track below so a collision /
     * NOMEM leaves the sender unchanged. */
    moq_media_track_t *tl = NULL;    /* SAP eventtimeline "<name>.sap" */
    moq_media_track_t *mtl = NULL;   /* media timeline "<name>.timeline" */
    if (cfg->emit_sap_timeline) {
        tl = sender_make_timeline(s, t, cfg->sap_timeline_history_groups, false,
                                  sz.sap_name);
        if (!tl) { track_free(s, t); return MOQ_ERR_NOMEM; }
        t->sap_timeline = tl;
    }
    if (cfg->emit_media_timeline) {
        mtl = sender_make_timeline(s, t, cfg->media_timeline_history_groups,
                                   true, sz.mt_name);
        if (!mtl) {
            if (tl) track_free(s, tl);
            track_free(s, t);
            return MOQ_ERR_NOMEM;
        }
        t->media_timeline = mtl;
    }

    pthread_mutex_lock(&s->mu);
    /* Re-check terminal state under the lock (ep->mu-free probe: s->mu is
     * held and the hook order is ep->mu -> s->mu). */
    if (s->fatal || sender_ep_closed(s)) {
        pthread_mutex_unlock(&s->mu);
        if (mtl) track_free(s, mtl);
        if (tl) track_free(s, tl);
        track_free(s, t);
        return MOQ_ERR_CLOSED;
    }
    /* Post-ready same-name reuse: if a removed track with this name is still
     * tearing down, the replacement must wait so the removal generation is
     * published first (and its publisher track is freed before the new one
     * registers). Retryable -- the caller re-adds once teardown completes. */
    if (s->ready &&
        (removed_samename_pending(s, t->name) ||
         (tl && removed_samename_pending(s, tl->name)) ||
         (mtl && removed_samename_pending(s, mtl->name)))) {
        pthread_mutex_unlock(&s->mu);
        if (mtl) track_free(s, mtl);
        if (tl) track_free(s, tl);
        track_free(s, t);
        return MOQ_ERR_WOULD_BLOCK;
    }
    /* Catalog coherence: no duplicate track names. This rejects both an app
     * track whose name collides with an existing track (including a previously
     * generated "<x>.sap" / "<x>.timeline") and a generated timeline name that
     * collides with an existing app track. */
    if (sender_name_taken(s, t->name) ||
        (tl && sender_name_taken(s, tl->name)) ||
        (mtl && sender_name_taken(s, mtl->name))) {
        pthread_mutex_unlock(&s->mu);
        if (mtl) track_free(s, mtl);
        if (tl) track_free(s, tl);
        track_free(s, t);
        return MOQ_ERR_INVAL;
    }
    /* A post-ready add needs a catalog generation to advertise it. */
    if (!sender_catalog_group_available(s)) {
        pthread_mutex_unlock(&s->mu);
        if (mtl) track_free(s, mtl);
        if (tl) track_free(s, tl);
        track_free(s, t);
        return MOQ_ERR_LIMIT;
    }
    if (!sender_assign_init_ref(s, t) || !sender_tracks_push(s, t)) {
        pthread_mutex_unlock(&s->mu);
        if (mtl) track_free(s, mtl);
        if (tl) track_free(s, tl);
        track_free(s, t);
        return MOQ_ERR_NOMEM;
    }
    if (tl && !sender_tracks_push(s, tl)) {
        /* Roll the media track back out so nothing is half-registered. */
        s->track_count--;
        pthread_mutex_unlock(&s->mu);
        if (mtl) track_free(s, mtl);
        track_free(s, tl);
        track_free(s, t);
        return MOQ_ERR_NOMEM;
    }
    if (mtl && !sender_tracks_push(s, mtl)) {
        /* Roll back the media track (and the SAP sibling if pushed). */
        s->track_count -= tl ? 2 : 1;
        pthread_mutex_unlock(&s->mu);
        track_free(s, mtl);
        if (tl) track_free(s, tl);
        track_free(s, t);
        return MOQ_ERR_NOMEM;
    }
    /* Startup can retain the initial catalog before a blocked PUBLISH. Keep
     * mutations in that interval just like post-ready mutations. */
    bool wake = s->ready || s->tracks_frozen;
    if (s->ready || s->tracks_frozen) s->catalog_dirty = true;
    pthread_mutex_unlock(&s->mu);
    if (wake && s->ep) moq_endpoint_wake(s->ep);
    *out = t;
    return MOQ_OK;
}


moq_result_t moq_media_sender_end_track(moq_media_sender_t *s,
                                        moq_media_track_t *track)
{
    if (!s || !track) return MOQ_ERR_INVAL;
    if (moq_endpoint_interrupted_internal(s->ep)) return MOQ_ERR_INTERRUPTED;
    if (sender_terminal(s)) return MOQ_ERR_CLOSED;

    pthread_mutex_lock(&s->mu);
    if (s->fatal) { pthread_mutex_unlock(&s->mu); return MOQ_ERR_CLOSED; }

    bool owned = false;
    for (size_t i = 0; i < s->track_count; i++)
        if (s->tracks[i] == track) { owned = true; break; }
    if (!owned) { pthread_mutex_unlock(&s->mu); return MOQ_ERR_INVAL; }

    /* A removed track is terminal: it is being torn down (END_OF_TRACK + pub
     * removal) by the hook, so end_track is refused rather than queuing a
     * terminal marker the drain would drop. */
    if (track->removed || track->auth_failed || s->completing) {
        pthread_mutex_unlock(&s->mu);
        return MOQ_ERR_WRONG_STATE;
    }

    /* Idempotent: a second end_track is a no-op success (the terminal is
     * already queued/sent). */
    if (track->end_requested) { pthread_mutex_unlock(&s->mu); return MOQ_OK; }

#ifdef MOQ_SERVICE_HAVE_MPEG2TS
    /* A byte-stream TS track ends its source first: the Objects held back,
     * then the packetizer's last ones, each through the normal admission. A
     * WOULD_BLOCK on the way is retried by the next end_track. */
    if (track->ts) {
        sender_ts_t *ts = (sender_ts_t *)track->ts;
        if (ts->busy) { pthread_mutex_unlock(&s->mu); return MOQ_ERR_WRONG_STATE; }
        ts->busy = true;
        bool queued_any = false;
        moq_result_t rc = sender_ts_queue(s, track, ts->parked, &ts->parked_n,
                                          &queued_any);
        if (rc == MOQ_OK && !ts->ending) {
            ts->ending = true;
            if (moq_mpeg2ts_packetizer_flush(&ts->pz) != MOQ_OK)
                rc = MOQ_ERR_INTERNAL;
            else
                rc = sender_ts_queue_staged(s, track, &queued_any);
        }
        ts->busy = false;
        /* MOQ_ERR_NOMEM (a last Object lost) is reported like any other
         * failure: the track is not ended yet, and the next end_track
         * queues the END_OF_TRACK. */
        if (rc != MOQ_OK) {
            bool wake = queued_any && s->ready;
            pthread_mutex_unlock(&s->mu);
            if (wake) moq_endpoint_wake(s->ep);
            return rc;
        }
        /* A partial packet is not a packet: discarded and counted. */
        ts->partial_bytes_discarded += ts->carry_len;
        ts->carry_len = 0;
    }
#endif

    /* The terminal marker needs one slot against the ACTIVE queue OBJECT bound
     * -- the same bound write() is held to (preq_fits) -- not against the
     * larger physical allocation, which is an implementation detail. It is a
     * zero-byte entry, so the byte budget does not apply. Nothing is latched
     * on refusal: end_requested stays clear and the caller retries. */
    if ((s->preq_tail - s->preq_head) >= active_obj_cap(s)) {
        pthread_mutex_unlock(&s->mu);
        return MOQ_ERR_WOULD_BLOCK;   /* queue full; retry after it drains */
    }

    sender_preq_entry_t *e = &s->preq[s->preq_tail % s->ring_cap];
    memset(e, 0, sizeof(*e));
    e->track = track;
    e->is_end = true;
    e->group_seq = track->group_seq;
    s->preq_tail++;
    preq_note_enqueued(s, e);   /* a marker is not queued media */

    track->end_requested = true;
    track->group_open = false;   /* no further objects open a group */
    bool wake = s->ready;
    pthread_mutex_unlock(&s->mu);

    if (wake)
        moq_endpoint_wake(s->ep);
    return MOQ_OK;
}


moq_result_t moq_media_sender_remove_track(moq_media_sender_t *s,
                                           moq_media_track_t *track)
{
    if (!s || !track) return MOQ_ERR_INVAL;
    if (s->ep && moq_endpoint_interrupted_internal(s->ep)) return MOQ_ERR_INTERRUPTED;
    if (sender_terminal(s)) return MOQ_ERR_CLOSED;

    pthread_mutex_lock(&s->mu);
    if (s->fatal) { pthread_mutex_unlock(&s->mu); return MOQ_ERR_CLOSED; }

    bool owned = false;
    for (size_t i = 0; i < s->track_count; i++)
        if (s->tracks[i] == track) { owned = true; break; }
    if (!owned) { pthread_mutex_unlock(&s->mu); return MOQ_ERR_INVAL; }

    /* A generated/internal track (SAP or media timeline) is not user-removable:
     * it is coupled to its media track and removed with it. */
    if (track_is_generated_timeline(track)) {
        pthread_mutex_unlock(&s->mu);
        return MOQ_ERR_INVAL;
    }
    /* Removing an already-removed handle is a terminal-state error (the handle
     * stays valid but inert until sender destroy), and so is removing one
     * after complete(). */
    if (track->removed || s->completing) {
        pthread_mutex_unlock(&s->mu);
        return MOQ_ERR_WRONG_STATE;
    }
    if (!sender_catalog_group_available(s)) {
        pthread_mutex_unlock(&s->mu);
        return MOQ_ERR_LIMIT;   /* no Group ID left for the removal catalog */
    }

    track->removed = true;
    if (track->sap_timeline) track->sap_timeline->removed = true;  /* couple */
    if (track->media_timeline) track->media_timeline->removed = true;  /* couple */
    /* A startup PUBLISH can be admission-blocked after retaining the initial
     * catalog. Remember changes in that interval as well as after readiness. */
    bool wake = s->ready || s->tracks_frozen;
    if (s->ready || s->tracks_frozen) s->catalog_dirty = true;
    pthread_mutex_unlock(&s->mu);

    if (wake && s->ep)
        moq_endpoint_wake(s->ep);
    return MOQ_OK;
}
