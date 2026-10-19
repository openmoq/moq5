#include <moq/playback.h>
#include <moq/subscriber.h>
#include <moq/media_object.h>
#include <moq/cmaf.h>
#include <moq/loc.h>
#include <moq/rcbuf.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

/* -- Layout pins ----------------------------------------------------- */

_Static_assert(sizeof(moq_playback_media_type_t) == 4 &&
               sizeof(moq_playback_packaging_t) == 4 &&
               sizeof(moq_playback_cmd_kind_t) == 4 &&
               sizeof(moq_playback_reset_reason_t) == 4 &&
               sizeof(moq_playback_event_kind_t) == 4 &&
               sizeof(moq_playback_drop_reason_t) == 4 &&
               sizeof(moq_playback_feedback_kind_t) == 4,
               "playback kinds are fixed-width");

/* The v0 track-config layout, through is_live. The first appended field
 * must start at or past its complete sizeof, so an old caller's tail
 * padding is never read as a field. */
typedef struct {
    uint32_t    struct_size;
    uint32_t    media_type;
    uint32_t    packaging;
    moq_bytes_t codec;
    moq_bytes_t init_data;
    uint32_t    timescale, width, height, samplerate, channel_count;
    uint64_t    target_latency_us;
    bool        is_live;
} pb_track_cfg_v0_t;
_Static_assert(offsetof(moq_playback_track_cfg_t, transport_version) >=
               sizeof(pb_track_cfg_v0_t),
               "track cfg tail starts past the v0 sizeof");
_Static_assert(offsetof(moq_playback_track_cfg_t, is_live) ==
               offsetof(pb_track_cfg_v0_t, is_live),
               "track cfg v0 prefix is unchanged");
_Static_assert(MOQ_PLAYBACK_TRACK_CFG_V0_SIZE ==
               offsetof(pb_track_cfg_v0_t, is_live) + sizeof(bool),
               "track cfg v0 floor");

/* Records and inputs whose v0 size is their complete current layout: a
 * field appended to any of them must move these pins deliberately. */
_Static_assert(sizeof(moq_playback_cmd_t) == MOQ_PLAYBACK_CMD_V0_SIZE,
               "command record v0 size");
_Static_assert(sizeof(moq_playback_event_t) == MOQ_PLAYBACK_EVENT_V0_SIZE,
               "event record v0 size");
_Static_assert(sizeof(moq_playback_object_t) == MOQ_PLAYBACK_OBJECT_V0_SIZE,
               "object input v0 size");
_Static_assert(sizeof(moq_playback_feedback_t) == MOQ_PLAYBACK_FEEDBACK_V0_SIZE,
               "feedback input v0 size");
/* max_known_ranges starts at the complete sizeof of the layout through
 * infer_end_of_group_from_next_group, never in its tail padding. */
_Static_assert(offsetof(moq_playback_cfg_t, max_known_ranges) ==
               ((offsetof(moq_playback_cfg_t, infer_end_of_group_from_next_group) +
                 sizeof(bool) + _Alignof(moq_playback_cfg_t) - 1) &
                ~(size_t)(_Alignof(moq_playback_cfg_t) - 1)),
               "max_known_ranges at the old sizeof");
_Static_assert(offsetof(moq_playback_track_cfg_t, gap_policy) ==
               offsetof(moq_playback_track_cfg_t, track_properties) +
               sizeof(moq_bytes_t), "gap_policy follows track_properties");
_Static_assert(sizeof(moq_playback_stream_terminal_t) ==
               MOQ_PLAYBACK_STREAM_TERMINAL_V0_SIZE,
               "stream terminal v0 size");
_Static_assert(MOQ_PLAYBACK_PARSED_INPUT_V0_SIZE ==
               offsetof(moq_playback_parsed_input_t, timescale) +
               sizeof(uint32_t), "parsed input v0 floor");

/* -- Monotonic creation epoch ---------------------------------------- */

static _Atomic uint64_t pb_epoch_counter = 0;

static uint64_t pb_next_epoch(void)
{
    return atomic_fetch_add_explicit(&pb_epoch_counter, 1, memory_order_relaxed) + 1;
}

/* -- Internal types -------------------------------------------------- */

/* What the engine knows about a span of a track's (group, object) ids,
 * inclusive at both ends; an object id of UINT64_MAX means through the end
 * of its group, and (UINT64_MAX, UINT64_MAX) the end of the track. */
#define PB_KNOW_ABSENT     1u  /* authoritative: the ids do not exist */
#define PB_KNOW_UNRESOLVED 2u  /* delivery left them out; they may arrive */
typedef struct {
    uint32_t kind;
    moq_playback_gap_cause_t cause;  /* UNRESOLVED: why */
    uint64_t g0, o0, g1, o1;
    bool     mark;                   /* scratch of know_add, else false */
} pb_range_t;

/* A stream fact's reach and when it came: delivery got as far as (g, o)
 * by `us`. A track keeps the facts no other dominates (one reaching at
 * least as far, no later), so in order of reach their times rise too. */
typedef struct {
    uint64_t g, o, us;
} pb_evidence_t;

typedef struct {
    bool                       active;
    uint32_t                   generation;
    moq_playback_media_type_t  media_type;
    moq_playback_packaging_t   packaging;
    moq_rcbuf_t               *codec;
    moq_rcbuf_t               *init_data;
    moq_rcbuf_t               *codec_config;
    uint32_t                   timescale;
    uint32_t                   width;
    uint32_t                   height;
    uint32_t                   samplerate;
    uint32_t                   channel_count;
    uint64_t                   target_latency_us;
    bool                       is_live;
    bool                       anchored;
    bool                       configured;
    bool                       ended;
    bool                       waiting_for_keyframe;
    bool                       kf_wait_event_emitted;
    bool                       needs_reset;
    bool                       has_skip_pending;
    uint64_t                   abandoned_group;
    bool                       has_last_dts;
    uint64_t                   last_dts;
    bool                       gap_waiting;
    uint64_t                   gap_group;
    uint64_t                   gap_object;
    uint64_t                   gap_started_us;
    bool                       paused_by_pressure;
    uint64_t                   next_expected_group;
    uint64_t                   next_expected_object;

    /* moq_version_t value of the track's transport draft (16, 18 or 22). */
    uint32_t                   transport_version;
    /* Draft 22: the copied track properties and their parsed metadata,
     * whose config spans borrow track_properties. */
    moq_rcbuf_t               *track_properties;
    bool                       has_track_loc;
    moq_loc_metadata_t         track_loc;

    /* Two configurations, kept apart. cur_config is what the last
     * CONFIGURE carried, the decoder's configuration. rel_config is the one
     * released media established: the configuration of the last unit
     * whose decode was emitted, which an object without its own inherits.
     * They differ while a CONFIGURE waits for its decode. Each is a ref,
     * NULL with its _set flag for an empty configuration. */
    bool                       cur_config_set;
    moq_rcbuf_t               *cur_config;
    bool                       rel_config_set;
    moq_rcbuf_t               *rel_config;

    /* What happens on giving up a missing unit (moq_playback_gap_policy_t). */
    moq_playback_gap_policy_t  gap_policy;
    /* Known absence and unresolved delivery ahead of the cursor, sorted by
     * start: range_count of the max_known_ranges entries the engine's
     * range table holds for this track. */
    pb_range_t                *ranges;
    uint32_t                   range_count;
    /* Stream facts as evidence for the wait on a missing id, sorted by
     * reach: evidence_count of the max_known_ranges entries the engine's
     * evidence table holds for this track. */
    pb_evidence_t             *evidence;
    uint32_t                   evidence_count;
    /* GAP_DETECTED of the current wait went out; the step it announces is
     * still to commit (for want of an event slot). */
    bool                       gap_reported;
    /* The end of the last released unit: its decode time plus duration. */
    bool                       has_last_end;
    uint64_t                   last_end_us;

    /* The cursor has consumed something: from here on an object below it
     * can never be played. Until then an entry below the cursor moves it
     * back. */
    bool                       started;
    /* Objects below the cursor were buffered before release started; they
     * are dropped when it starts. */
    bool                       entry_cleanup_pending;
    /* Nothing is waited for (moq_playback_settle_track, and every
     * draining track). */
    bool                       settling;
    /* No more input will come (moq_playback_drain_track): the track ends
     * once nothing is buffered past the cursor. */
    bool                       draining;
} pb_track_t;

/* One configuration copy the engine made at admission, and who in the
 * engine holds it: buffered objects, track configuration slots, and queued
 * CONFIGURE commands (ring_holders of holders). Membership is what makes a
 * buffer a copy: a track's catalog configuration is never registered. */
typedef struct {
    moq_rcbuf_t *buf;
    uint32_t     holders;
    uint32_t     ring_holders;
    uint32_t     hold_holders;   /* command holds among the holders */
    /* Which registration this is, so a slot reused by a later copy is not
     * taken for it; 0 once the engine's serials are spent (never matched). */
    uint64_t     serial;
} pb_copy_t;

/* One command hold: what it keeps charged, the engine's own refs on it,
 * and the exact handle issued for it. */
typedef struct {
    uint64_t     handle;         /* while active */
    bool         active;
    uint64_t     bytes;          /* payload and sample-table charge */
    moq_rcbuf_t *config;         /* a registered copy, or NULL */
    moq_rcbuf_t *payload;        /* the unit's payload, or NULL */
} pb_hold_t;

/* Hold identities are issued, never reused, from one counter for every
 * engine of this library instance. Ticket t (1..PB_HOLD_TICKETS) names
 * the handle tag (q >> 27) + 1 and generation ((q & (2^27 - 1)) << 1) | 1,
 * q = t - 1: injective over the 16-bit nonzero tag and the 27 odd-
 * generation bits a handle carries, so no two holds of any engines share
 * an identity. Issuance stops at the last ticket; it never wraps. A test
 * engine may draw from a private issuer instead. */
#define PB_HOLD_GEN_BITS 27u
#define PB_HOLD_TICKETS  ((uint64_t)0xFFFFu << PB_HOLD_GEN_BITS)
typedef struct {
    _Atomic uint64_t issued;     /* tickets handed out */
    uint64_t         limit;      /* the last ticket */
} pb_issuer_t;

static pb_issuer_t pb_hold_issuer = { 0, PB_HOLD_TICKETS };

/* The next ticket, or 0 once the issuer is spent. No call or allocation in
 * the loop: it is safe across threads, not from a signal handler. */
static uint64_t pb_issue_ticket(pb_issuer_t *is)
{
    uint64_t cur = atomic_load_explicit(&is->issued, memory_order_relaxed);
    while (cur < is->limit) {
        if (atomic_compare_exchange_weak_explicit(&is->issued, &cur, cur + 1,
                                                  memory_order_relaxed,
                                                  memory_order_relaxed))
            return cur + 1;
    }
    return 0;
}

/* One released or dropped location, for absorbing late duplicates. */
typedef struct {
    uint64_t track;
    uint64_t group_id;
    uint64_t object_id;
} pb_released_t;

typedef struct {
    bool                    occupied;
    moq_playback_track_t    track;
    uint64_t                group_id;
    uint64_t                subgroup_id;
    uint64_t                object_id;
    uint8_t                 publisher_priority;
    moq_object_status_t     status;
    bool                    end_of_group;
    bool                    datagram;
    moq_rcbuf_t            *payload;
    size_t                  payload_len;
    bool                    has_capture_time;
    uint64_t                capture_time_us;
    uint64_t                decode_time_us;
    int64_t                 composition_offset_us;
    uint64_t                presentation_time_us;
    bool                    keyframe;
    uint32_t                sample_duration_us;
    size_t                  mdat_offset;
    size_t                  mdat_len;
    size_t                  sample_count;
    /* Multi-sample CMAF: samples already emitted. The object stays at the
     * cursor until every sample has left through the command ring. */
    size_t                  sample_cursor;
    /* Multi-sample CMAF: the owned sample table and the fragment's base
     * decode time in ticks, taken once at admission; release reads only
     * these. */
    moq_cmaf_sample_t      *samples;
    uint64_t                base_ticks;
    /* The object's effective in-band decoder configuration (draft 22): an
     * owned copy, or NULL with config_present for a present-but-empty one. */
    bool                    config_present;
    moq_rcbuf_t            *config;
    /* Bytes charged to max_buffered_bytes: payload, sample table, config. */
    uint64_t                charge;
    /* now_us of the push that admitted it: evidence for the wait on a
     * missing id before it. */
    uint64_t                arrived_us;
} pb_object_t;

struct moq_playback {
    moq_alloc_t              alloc;
    moq_playback_cfg_t       cfg;
    uint16_t                 creation_tag;
    uint32_t                 generation_seed;

    moq_playback_cmd_t      *cmd_ring;
    size_t                   cmd_head;
    size_t                   cmd_count;
    /* Per ring slot, the bytes its command keeps charged until it is
     * polled, and whether it is a sample of a multi-sample object
     * (allocated with the ring); the bytes' sum. */
    uint64_t                *cmd_charge;
    uint8_t                 *cmd_multi;
    uint64_t                 cmd_bytes;

    /* Command holds: max_held_commands slots, the free ones' indexes on a
     * stack, and the bytes active holds keep charged besides their copies;
     * the issuer of their identities (the library's, or a test's own). */
    pb_issuer_t             *issuer;
#ifdef MOQ_PLAYBACK_TESTING
    pb_issuer_t              local_issuer;
#endif
    pb_hold_t               *holds;
    uint32_t                *hold_free;
    uint32_t                 hold_free_count;
    uint32_t                 hold_active;
    uint64_t                 hold_bytes;

    moq_playback_event_t    *evt_ring;
    size_t                   evt_head;
    size_t                   evt_count;

    pb_track_t              *tracks;
    uint32_t                 track_count;

    pb_object_t             *objects;
    size_t                   obj_count;
    uint64_t                 retained_bytes;

    moq_cmaf_sample_t       *sample_scratch;

    /* Configuration copies alive in the engine. Each has at least one
     * holder, and the holders number at most max_buffered_objects +
     * 2 * max_tracks + max_commands + max_held_commands, which sizes the
     * table. */
    pb_copy_t               *copies;
    size_t                   copy_cap;
    uint64_t                 copy_serial;   /* the last one given */

    /* max_known_ranges ranges and as many evidence entries per track slot,
     * in one allocation of range_slots entries of each. */
    pb_range_t              *ranges;
    pb_evidence_t           *evidence;
    size_t                   range_slots;
#ifdef MOQ_PLAYBACK_TESTING
    uint64_t                 absence_forgotten;
#endif

    /* Ring of the last max_buffered_objects locations that left the buffer. */
    pb_released_t           *history;
    size_t                   history_next;
    size_t                   history_count;

    /* The most recent object a push refused for buffer capacity: its track
     * and location, and the bytes it needs. A refused push changes no
     * buffered object, ref, history entry or event; it only records this,
     * so the next tick can make room when room can be made. Relief, its
     * only reader, validates it first and retires it once its request is
     * settled or its track is gone. */
#ifdef MOQ_PLAYBACK_TESTING
    uint64_t                 fragment_reads;
#endif
    bool                     has_waiting_admission;
    uint64_t                 wait_track;
    uint64_t                 wait_group_id;
    uint64_t                 wait_object_id;
    uint64_t                 wait_bytes;
    /* The copy wait_bytes counted on reusing, if any: its slot and serial,
     * and the bytes the reuse saved, owed again once the copy is gone. */
    size_t                   wait_reuse_slot;
    uint64_t                 wait_reuse_serial;
    uint64_t                 wait_reuse_len;
};

/* -- Fragment index reads --------------------------------------------- */

/* Every read of a CMAF fragment's index goes through here, so the test
 * build can count them. */
static moq_result_t pb_read_fragment(moq_playback_t *pb, moq_bytes_t bytes,
                                     moq_cmaf_fragment_info_t *frag)
{
#ifdef MOQ_PLAYBACK_TESTING
    pb->fragment_reads++;
#else
    (void)pb;
#endif
    return moq_cmaf_parse_fragment(bytes, frag);
}

/* -- Handle helpers -------------------------------------------------- */

static moq_playback_track_t pb_encode_handle(uint16_t creation_tag,
                                              uint32_t generation,
                                              uint32_t slot)
{
    moq_playback_track_t h;
    h._opaque = moq_handle_pack(MOQ_HANDLE_POOL_PLAYBACK_TRACK,
                                creation_tag, generation, slot);
    return h;
}

static bool pb_resolve_handle(const moq_playback_t *pb,
                               moq_playback_track_t h,
                               uint32_t *slot_out)
{
    if (h._opaque == 0) return false;
    if (moq_handle_pool_tag(h._opaque) != MOQ_HANDLE_POOL_PLAYBACK_TRACK)
        return false;
    if (moq_handle_session_tag(h._opaque) != pb->creation_tag)
        return false;
    uint32_t slot = moq_handle_slot(h._opaque);
    if (slot >= pb->cfg.max_tracks) return false;
    if (!pb->tracks[slot].active) return false;
    uint32_t gen = moq_handle_generation(h._opaque);
    if (gen != pb->tracks[slot].generation) return false;
    *slot_out = slot;
    return true;
}

/* -- Init functions -------------------------------------------------- */

#define PB_COVERS(type, size, field) \
    ((size) >= offsetof(type, field) + sizeof(((type *)0)->field))

void moq_playback_cfg_init_sized(moq_playback_cfg_t *cfg, size_t cfg_size)
{
    if (!cfg || cfg_size < sizeof(uint32_t)) return;
    size_t n = cfg_size < sizeof(*cfg) ? cfg_size : sizeof(*cfg);
    memset(cfg, 0, n);
    cfg->struct_size = (uint32_t)n;
#define PB_CFG_DEFAULT(field, value) \
    if (PB_COVERS(moq_playback_cfg_t, n, field)) cfg->field = (value)
    PB_CFG_DEFAULT(max_tracks, 4);
    PB_CFG_DEFAULT(max_buffered_objects, 256);
    PB_CFG_DEFAULT(max_buffered_bytes, 16u * 1024 * 1024);
    PB_CFG_DEFAULT(max_backlog_groups, 3);
    PB_CFG_DEFAULT(max_commands, 64);
    PB_CFG_DEFAULT(max_events, 16);
    PB_CFG_DEFAULT(gap_timeout_us, 500000);
    PB_CFG_DEFAULT(max_release_per_tick, 8);
    PB_CFG_DEFAULT(max_track_config_bytes, 4096);
    PB_CFG_DEFAULT(max_samples_per_object, 16);
    PB_CFG_DEFAULT(max_known_ranges, 64);
#undef PB_CFG_DEFAULT
}

void moq_playback_track_cfg_init_sized(moq_playback_track_cfg_t *cfg,
                                       size_t cfg_size)
{
    if (!cfg || cfg_size < sizeof(uint32_t)) return;
    size_t n = cfg_size < sizeof(*cfg) ? cfg_size : sizeof(*cfg);
    memset(cfg, 0, n);
    cfg->struct_size = (uint32_t)n;
    if (PB_COVERS(moq_playback_track_cfg_t, n, transport_version))
        cfg->transport_version = (uint32_t)MOQ_VERSION_DRAFT_16;
    if (PB_COVERS(moq_playback_track_cfg_t, n, gap_policy))
        cfg->gap_policy = MOQ_PLAYBACK_GAP_DISCONTINUITY;
}

void moq_playback_object_init_sized(moq_playback_object_t *obj,
                                    size_t obj_size)
{
    if (!obj || obj_size < sizeof(uint32_t)) return;
    size_t n = obj_size < sizeof(*obj) ? obj_size : sizeof(*obj);
    memset(obj, 0, n);
    obj->struct_size = (uint32_t)n;
}

void moq_playback_feedback_init_sized(moq_playback_feedback_t *fb,
                                      size_t fb_size)
{
    if (!fb || fb_size < sizeof(uint32_t)) return;
    size_t n = fb_size < sizeof(*fb) ? fb_size : sizeof(*fb);
    memset(fb, 0, n);
    fb->struct_size = (uint32_t)n;
}

/* -- Command cleanup ------------------------------------------------- */

/* Releases the refs of a command record by kind. The engine's own ring
 * entries go through here directly; callers go through the public cleanup,
 * which first checks the record's stamp. */
static void cmd_release_refs(moq_playback_cmd_t *cmd)
{
    switch (cmd->kind) {
    case MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO:
        moq_rcbuf_decref(cmd->u.configure_video.codec);
        moq_rcbuf_decref(cmd->u.configure_video.codec_config);
        cmd->u.configure_video.codec = NULL;
        cmd->u.configure_video.codec_config = NULL;
        break;
    case MOQ_PLAYBACK_CMD_DECODE_VIDEO:
        moq_rcbuf_decref(cmd->u.decode_video.payload);
        cmd->u.decode_video.payload = NULL;
        break;
    case MOQ_PLAYBACK_CMD_DECODE_CMAF:
        moq_rcbuf_decref(cmd->u.decode_cmaf.fragment);
        cmd->u.decode_cmaf.fragment = NULL;
        break;
    case MOQ_PLAYBACK_CMD_CONFIGURE_AUDIO:
        moq_rcbuf_decref(cmd->u.configure_audio.codec);
        moq_rcbuf_decref(cmd->u.configure_audio.codec_config);
        cmd->u.configure_audio.codec = NULL;
        cmd->u.configure_audio.codec_config = NULL;
        break;
    case MOQ_PLAYBACK_CMD_DECODE_AUDIO:
        moq_rcbuf_decref(cmd->u.decode_audio.payload);
        cmd->u.decode_audio.payload = NULL;
        break;
    default:
        break;
    }
}

void moq_playback_cmd_cleanup(moq_playback_cmd_t *cmd)
{
    if (!cmd || cmd->struct_size < MOQ_PLAYBACK_CMD_V0_SIZE) return;
    cmd_release_refs(cmd);
}

/* -- Internal queue helpers ------------------------------------------ */

static moq_result_t push_cmd(moq_playback_t *pb,
                              const moq_playback_cmd_t *cmd)
{
    if (pb->cmd_count >= pb->cfg.max_commands)
        return MOQ_ERR_WOULD_BLOCK;
    size_t idx = (pb->cmd_head + pb->cmd_count) % pb->cfg.max_commands;
    pb->cmd_ring[idx] = *cmd;
    pb->cmd_charge[idx] = 0;
    pb->cmd_multi[idx] = 0;
    pb->cmd_count++;
    return MOQ_OK;
}

static moq_result_t push_evt(moq_playback_t *pb,
                              const moq_playback_event_t *evt)
{
    if (pb->evt_count >= pb->cfg.max_events)
        return MOQ_ERR_WOULD_BLOCK;
    size_t idx = (pb->evt_head + pb->evt_count) % pb->cfg.max_events;
    pb->evt_ring[idx] = *evt;
    pb->evt_count++;
    return MOQ_OK;
}

/* -- Defaults -------------------------------------------------------- */

static void apply_defaults(moq_playback_cfg_t *c)
{
    moq_playback_cfg_t def;
    moq_playback_cfg_init(&def);
    if (c->max_tracks == 0)            c->max_tracks = def.max_tracks;
    if (c->max_buffered_objects == 0)   c->max_buffered_objects = def.max_buffered_objects;
    if (c->max_buffered_bytes == 0)     c->max_buffered_bytes = def.max_buffered_bytes;
    if (c->max_backlog_groups == 0)     c->max_backlog_groups = def.max_backlog_groups;
    if (c->max_commands == 0)           c->max_commands = def.max_commands;
    if (c->max_events == 0)            c->max_events = def.max_events;
    if (c->gap_timeout_us == 0)        c->gap_timeout_us = def.gap_timeout_us;
    if (c->max_track_config_bytes == 0) c->max_track_config_bytes = def.max_track_config_bytes;
    if (c->max_samples_per_object == 0) c->max_samples_per_object = def.max_samples_per_object;
    if (c->max_known_ranges == 0)      c->max_known_ranges = def.max_known_ranges;
}

/* -- Track helpers --------------------------------------------------- */

/* -- Configuration copies ---------------------------------------------- */

/* A configuration copy is charged to max_buffered_bytes once, from its
 * registration until its last engine holder lets go; the charge moves
 * between holders. A ref handed to the app by a poll is no longer an
 * engine holder. */
static pb_copy_t *copy_find(const moq_playback_t *pb, const moq_rcbuf_t *b)
{
    if (!b) return NULL;
    for (size_t i = 0; i < pb->copy_cap; i++)
        if (pb->copies[i].buf == b) return &pb->copies[i];
    return NULL;
}

/* Register a fresh copy whose one ref the first holder owns. */
static void copy_register(moq_playback_t *pb, moq_rcbuf_t *b)
{
    for (size_t i = 0; i < pb->copy_cap; i++) {
        if (!pb->copies[i].buf) {
            pb->copies[i].buf = b;
            pb->copies[i].holders = 1;
            pb->copies[i].ring_holders = 0;
            pb->copies[i].serial = pb->copy_serial < UINT64_MAX ? ++pb->copy_serial : 0;
            pb->retained_bytes += moq_rcbuf_len(b);
            return;
        }
    }
}

/* A new holder takes a ref on `b`. */
static void cfg_hold(moq_playback_t *pb, moq_rcbuf_t *b, bool ring)
{
    if (!b) return;
    pb_copy_t *c = copy_find(pb, b);
    if (c) {
        c->holders++;
        if (ring) c->ring_holders++;
    }
    moq_rcbuf_incref(b);
}

/* A holder lets go of `b`; `drop_ref` releases its ref, otherwise the ref
 * leaves the engine with it (a poll). */
static void cfg_unhold(moq_playback_t *pb, moq_rcbuf_t *b, bool ring,
                       bool drop_ref)
{
    if (!b) return;
    pb_copy_t *c = copy_find(pb, b);
    if (c) {
        if (ring) c->ring_holders--;
        if (--c->holders == 0) {
            pb->retained_bytes -= moq_rcbuf_len(b);
            memset(c, 0, sizeof(*c));
        }
    }
    if (drop_ref) moq_rcbuf_decref(b);
}

/* Point one of a track's configuration slots at `b`. */
static void cfg_set_slot(moq_playback_t *pb, moq_rcbuf_t **slot, bool *set,
                         moq_rcbuf_t *b)
{
    cfg_hold(pb, b, false);
    moq_rcbuf_t *old = *slot;
    bool had = *set;
    *slot = b;
    *set = true;
    if (had) cfg_unhold(pb, old, false, true);
}

/* Bytes of copies held only by queued commands and command holds:
 * polling and releasing free them. */
static uint64_t copies_drainable(const moq_playback_t *pb)
{
    uint64_t n = 0;
    for (size_t i = 0; i < pb->copy_cap; i++) {
        const pb_copy_t *c = &pb->copies[i];
        if (c->buf && c->holders == c->ring_holders + c->hold_holders)
            n += moq_rcbuf_len(c->buf);
    }
    return n;
}

/* The configuration a CONFIGURE command owns, or NULL. */
static moq_rcbuf_t *cmd_config(const moq_playback_cmd_t *c)
{
    if (c->kind == MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO)
        return c->u.configure_video.codec_config;
    if (c->kind == MOQ_PLAYBACK_CMD_CONFIGURE_AUDIO)
        return c->u.configure_audio.codec_config;
    return NULL;
}

/* Free everything a buffered object owns and return its charge. */
static void free_object_storage(moq_playback_t *pb, pb_object_t *o)
{
    pb->retained_bytes -= o->charge;
    moq_rcbuf_decref(o->payload);
    moq_rcbuf_t *config = o->config;
    if (o->samples)
        pb->alloc.free(o->samples, o->sample_count * sizeof(moq_cmaf_sample_t),
                       pb->alloc.ctx);
    memset(o, 0, sizeof(*o));
    pb->obj_count--;
    cfg_unhold(pb, config, false, true);
}

static void release_track_objects(moq_playback_t *pb,
                                   moq_playback_track_t handle)
{
    for (size_t i = 0; i < pb->cfg.max_buffered_objects; i++) {
        pb_object_t *o = &pb->objects[i];
        if (o->occupied && o->track._opaque == handle._opaque)
            free_object_storage(pb, o);
    }
}

static void release_track_config(moq_playback_t *pb, pb_track_t *t)
{
    if (t->cur_config_set) cfg_unhold(pb, t->cur_config, false, true);
    if (t->rel_config_set) cfg_unhold(pb, t->rel_config, false, true);
    t->cur_config = NULL;
    t->rel_config = NULL;
    t->cur_config_set = false;
    t->rel_config_set = false;
    moq_rcbuf_decref(t->codec);
    moq_rcbuf_decref(t->init_data);
    moq_rcbuf_decref(t->codec_config);
    moq_rcbuf_decref(t->track_properties);
    t->codec = NULL;
    t->init_data = NULL;
    t->codec_config = NULL;
    t->track_properties = NULL;
    t->has_track_loc = false;
}

/* -- Lifecycle ------------------------------------------------------- */

moq_result_t moq_playback_create(const moq_alloc_t *alloc,
                                  const moq_playback_cfg_t *cfg,
                                  moq_playback_t **out)
{
    if (!alloc || !cfg || !out) return MOQ_ERR_INVAL;
    *out = NULL;

    if (!alloc->alloc || !alloc->free) return MOQ_ERR_INVAL;
    uint32_t cfg_size;
    memcpy(&cfg_size, cfg, sizeof(cfg_size));
    if (cfg_size < MOQ_PLAYBACK_CFG_V0_SIZE) return MOQ_ERR_INVAL;

    /* The caller's object may be an older, smaller struct: copy the v0
     * prefix, then each appended field only when wholly covered. */
    moq_playback_cfg_t c;
    memset(&c, 0, sizeof(c));
    memcpy(&c, cfg, MOQ_PLAYBACK_CFG_V0_SIZE);
#define PB_CFG_TAIL(field) \
    if (PB_COVERS(moq_playback_cfg_t, cfg_size, field)) \
        memcpy(&c.field, (const unsigned char *)cfg + \
               offsetof(moq_playback_cfg_t, field), sizeof(c.field))
    PB_CFG_TAIL(max_samples_per_object);
    PB_CFG_TAIL(ignore_eog_bit);
    PB_CFG_TAIL(infer_end_of_group_from_next_group);
    PB_CFG_TAIL(max_known_ranges);
    PB_CFG_TAIL(max_held_commands);
#undef PB_CFG_TAIL
    c.struct_size = sizeof(c);
    apply_defaults(&c);

    if (c.max_tracks > 0xFFFFu) return MOQ_ERR_INVAL;
    /* A hold's slot is a 16-bit handle field: slots 0..0xFFFF. */
    if (c.max_held_commands > 0x10000u) return MOQ_ERR_INVAL;

    /* count*sizeof overflow guards, written as `sizeof > SIZE_MAX/count` so the
     * divided right-hand side is a runtime size_t. The equivalent
     * `(size_t)count > SIZE_MAX/sizeof(T)` form has a compile-time RHS that is
     * always-false for a uint32 count on 64-bit and trips gcc
     * -Werror=type-limits (the check still matters on 32-bit). */
    if (c.max_commands != 0 &&
        sizeof(moq_playback_cmd_t) + sizeof(uint64_t) + 1 > SIZE_MAX / (size_t)c.max_commands)
        return MOQ_ERR_INVAL;
    if (c.max_held_commands != 0 &&
        sizeof(pb_hold_t) + sizeof(uint32_t) > SIZE_MAX / (size_t)c.max_held_commands)
        return MOQ_ERR_INVAL;
    if (c.max_events != 0 &&
        sizeof(moq_playback_event_t) > SIZE_MAX / (size_t)c.max_events)
        return MOQ_ERR_INVAL;
    if (c.max_tracks != 0 &&
        sizeof(pb_track_t) > SIZE_MAX / (size_t)c.max_tracks)
        return MOQ_ERR_INVAL;
    if (c.max_buffered_objects != 0 &&
        sizeof(pb_object_t) > SIZE_MAX / (size_t)c.max_buffered_objects)
        return MOQ_ERR_INVAL;
    if (c.max_samples_per_object != 0 &&
        sizeof(moq_cmaf_sample_t) > SIZE_MAX / (size_t)c.max_samples_per_object)
        return MOQ_ERR_INVAL;
    if (sizeof(pb_released_t) > SIZE_MAX / (size_t)c.max_buffered_objects)
        return MOQ_ERR_INVAL;
    /* The range and evidence tables: max_tracks * max_known_ranges entries
     * of each. The count is checked before it is formed, then its bytes. */
    if ((size_t)c.max_known_ranges > SIZE_MAX / (size_t)c.max_tracks)
        return MOQ_ERR_INVAL;
    size_t range_slots = (size_t)c.max_tracks * c.max_known_ranges;
    if (sizeof(pb_range_t) + sizeof(pb_evidence_t) > SIZE_MAX / range_slots)
        return MOQ_ERR_INVAL;
    /* The copy table: max_buffered_objects + 2 * max_tracks + max_commands
     * + max_held_commands entries. The count is summed with a check at each
     * step (size_t may be 32 bits), then its bytes. */
    size_t copy_cap = (size_t)c.max_buffered_objects;
    if (2 * (size_t)c.max_tracks > SIZE_MAX - copy_cap) return MOQ_ERR_INVAL;
    copy_cap += 2 * (size_t)c.max_tracks;
    if ((size_t)c.max_commands > SIZE_MAX - copy_cap) return MOQ_ERR_INVAL;
    copy_cap += (size_t)c.max_commands;
    if ((size_t)c.max_held_commands > SIZE_MAX - copy_cap) return MOQ_ERR_INVAL;
    copy_cap += (size_t)c.max_held_commands;
    if (sizeof(pb_copy_t) > SIZE_MAX / copy_cap) return MOQ_ERR_INVAL;

    moq_playback_t *pb = (moq_playback_t *)alloc->alloc(
        sizeof(moq_playback_t), alloc->ctx);
    if (!pb) return MOQ_ERR_NOMEM;
    memset(pb, 0, sizeof(*pb));
    pb->alloc = *alloc;
    pb->cfg = c;
    {
        uint64_t epoch = pb_next_epoch();
        uint16_t tag = (uint16_t)(epoch & 0xFFFFu);
        if (tag == 0) tag = 1;
        uint32_t seed = (uint32_t)((epoch * 2654435761u) & 0xFFFFFFFu) | 1u;
        pb->creation_tag = tag;
        pb->generation_seed = seed;
    }

    pb->issuer = &pb_hold_issuer;

    /* The ring, its per-slot charges and multi-sample marks, in one
     * allocation. */
    size_t cmd_bytes = c.max_commands * (sizeof(moq_playback_cmd_t) + sizeof(uint64_t) + 1);
    pb->cmd_ring = (moq_playback_cmd_t *)alloc->alloc(cmd_bytes, alloc->ctx);
    if (!pb->cmd_ring) goto fail_cmd;
    memset(pb->cmd_ring, 0, cmd_bytes);
    pb->cmd_charge = (uint64_t *)(void *)(pb->cmd_ring + c.max_commands);
    pb->cmd_multi = (uint8_t *)(void *)(pb->cmd_charge + c.max_commands);

    /* The hold slots and their free stack, in one allocation; none when
     * held polls are disabled. Slot i starts at generation 1. */
    size_t hold_bytes = c.max_held_commands * (sizeof(pb_hold_t) + sizeof(uint32_t));
    if (hold_bytes) {
        pb->holds = (pb_hold_t *)alloc->alloc(hold_bytes, alloc->ctx);
        if (!pb->holds) goto fail_holds;
        memset(pb->holds, 0, hold_bytes);
        pb->hold_free = (uint32_t *)(void *)(pb->holds + c.max_held_commands);
        for (uint32_t i = 0; i < c.max_held_commands; i++)
            pb->hold_free[i] = c.max_held_commands - 1 - i;
        pb->hold_free_count = c.max_held_commands;
    }

    size_t evt_bytes = c.max_events * sizeof(moq_playback_event_t);
    pb->evt_ring = (moq_playback_event_t *)alloc->alloc(evt_bytes, alloc->ctx);
    if (!pb->evt_ring) goto fail_evt;
    memset(pb->evt_ring, 0, evt_bytes);

    size_t trk_bytes = c.max_tracks * sizeof(pb_track_t);
    pb->tracks = (pb_track_t *)alloc->alloc(trk_bytes, alloc->ctx);
    if (!pb->tracks) goto fail_trk;
    memset(pb->tracks, 0, trk_bytes);

    size_t obj_bytes = c.max_buffered_objects * sizeof(pb_object_t);
    pb->objects = (pb_object_t *)alloc->alloc(obj_bytes, alloc->ctx);
    if (!pb->objects) goto fail_obj;
    memset(pb->objects, 0, obj_bytes);

    size_t samp_bytes = c.max_samples_per_object * sizeof(moq_cmaf_sample_t);
    pb->sample_scratch = (moq_cmaf_sample_t *)alloc->alloc(
        samp_bytes, alloc->ctx);
    if (!pb->sample_scratch) goto fail_samp;
    memset(pb->sample_scratch, 0, samp_bytes);

    size_t copy_bytes = copy_cap * sizeof(pb_copy_t);
    pb->copies = (pb_copy_t *)alloc->alloc(copy_bytes, alloc->ctx);
    if (!pb->copies) goto fail_copies;
    memset(pb->copies, 0, copy_bytes);
    pb->copy_cap = copy_cap;

    size_t hist_bytes = c.max_buffered_objects * sizeof(pb_released_t);
    pb->history = (pb_released_t *)alloc->alloc(hist_bytes, alloc->ctx);
    if (!pb->history) goto fail_hist;
    memset(pb->history, 0, hist_bytes);

    size_t range_bytes = range_slots * (sizeof(pb_range_t) + sizeof(pb_evidence_t));
    pb->ranges = (pb_range_t *)alloc->alloc(range_bytes, alloc->ctx);
    if (!pb->ranges) goto fail_ranges;
    memset(pb->ranges, 0, range_bytes);
    pb->evidence = (pb_evidence_t *)(void *)(pb->ranges + range_slots);
    pb->range_slots = range_slots;

    *out = pb;
    return MOQ_OK;

fail_ranges:
    alloc->free(pb->history, hist_bytes, alloc->ctx);
fail_hist:
    alloc->free(pb->copies, copy_bytes, alloc->ctx);
fail_copies:
    alloc->free(pb->sample_scratch, samp_bytes, alloc->ctx);
fail_samp:
    alloc->free(pb->objects, obj_bytes, alloc->ctx);
fail_obj:
    alloc->free(pb->tracks, trk_bytes, alloc->ctx);
fail_trk:
    alloc->free(pb->evt_ring, evt_bytes, alloc->ctx);
fail_evt:
    if (pb->holds) alloc->free(pb->holds, hold_bytes, alloc->ctx);
fail_holds:
    alloc->free(pb->cmd_ring, cmd_bytes, alloc->ctx);
fail_cmd:
    alloc->free(pb, sizeof(moq_playback_t), alloc->ctx);
    return MOQ_ERR_NOMEM;
}

void moq_playback_destroy(moq_playback_t *pb)
{
    if (!pb) return;

    for (size_t i = 0; i < pb->cfg.max_buffered_objects; i++) {
        pb_object_t *o = &pb->objects[i];
        if (o->occupied)
            free_object_storage(pb, o);
    }

    for (uint32_t i = 0; i < pb->cfg.max_tracks; i++) {
        if (pb->tracks[i].active)
            release_track_config(pb, &pb->tracks[i]);
    }

    while (pb->cmd_count > 0) {
        moq_playback_cmd_t *cmd =
            &pb->cmd_ring[pb->cmd_head % pb->cfg.max_commands];
        cmd_release_refs(cmd);
        pb->cmd_head++;
        pb->cmd_count--;
    }

    /* Active holds end with the engine: their own refs go. The refs of
     * the commands they were taken with are the caller's, and stay. */
    for (uint32_t i = 0; i < pb->cfg.max_held_commands; i++) {
        pb_hold_t *h = &pb->holds[i];
        if (!h->active) continue;
        if (h->config) moq_rcbuf_decref(h->config);
        if (h->payload) moq_rcbuf_decref(h->payload);
    }

    /* The copy table goes with the engine; the refs above were the copies'
     * last engine refs. */
    moq_alloc_t a = pb->alloc;
    if (pb->holds)
        a.free(pb->holds, pb->cfg.max_held_commands *
               (sizeof(pb_hold_t) + sizeof(uint32_t)), a.ctx);
    a.free(pb->copies, pb->copy_cap * sizeof(pb_copy_t), a.ctx);
    a.free(pb->ranges, pb->range_slots *
           (sizeof(pb_range_t) + sizeof(pb_evidence_t)), a.ctx);
    a.free(pb->history,
        pb->cfg.max_buffered_objects * sizeof(pb_released_t), a.ctx);
    a.free(pb->sample_scratch,
        pb->cfg.max_samples_per_object * sizeof(moq_cmaf_sample_t), a.ctx);
    a.free(pb->objects,
        pb->cfg.max_buffered_objects * sizeof(pb_object_t), a.ctx);
    a.free(pb->tracks,
        pb->cfg.max_tracks * sizeof(pb_track_t), a.ctx);
    a.free(pb->evt_ring,
        pb->cfg.max_events * sizeof(moq_playback_event_t), a.ctx);
    a.free(pb->cmd_ring,
        pb->cfg.max_commands * (sizeof(moq_playback_cmd_t) + sizeof(uint64_t) + 1), a.ctx);
    a.free(pb, sizeof(moq_playback_t), a.ctx);
}

/* -- Track management ------------------------------------------------ */

moq_result_t moq_playback_add_track(moq_playback_t *pb,
                                     const moq_playback_track_cfg_t *cfg,
                                     moq_playback_track_t *out)
{
    if (!pb || !cfg || !out) return MOQ_ERR_INVAL;
    *out = MOQ_PLAYBACK_TRACK_INVALID;

    uint32_t cfg_size;
    memcpy(&cfg_size, cfg, sizeof(cfg_size));
    if (cfg_size < MOQ_PLAYBACK_TRACK_CFG_V0_SIZE) return MOQ_ERR_INVAL;

    /* The caller's object may be an older, smaller struct. Every read below
     * comes from this current, default-initialized clone: the v0 prefix,
     * then each appended field by byte offset only when wholly covered. */
    moq_playback_track_cfg_t tc;
    moq_playback_track_cfg_init(&tc);
    memcpy(&tc, cfg, MOQ_PLAYBACK_TRACK_CFG_V0_SIZE);
#define PB_TRACK_TAIL(field) \
    if (PB_COVERS(moq_playback_track_cfg_t, cfg_size, field)) \
        memcpy(&tc.field, (const unsigned char *)cfg + \
               offsetof(moq_playback_track_cfg_t, field), sizeof(tc.field))
    PB_TRACK_TAIL(transport_version);
    PB_TRACK_TAIL(track_properties);
    PB_TRACK_TAIL(gap_policy);
#undef PB_TRACK_TAIL
    if (tc.gap_policy != MOQ_PLAYBACK_GAP_DISCONTINUITY &&
        tc.gap_policy != MOQ_PLAYBACK_GAP_CONTINUE)
        return MOQ_ERR_INVAL;
    uint32_t version = tc.transport_version;
    moq_bytes_t track_props = tc.track_properties;
    if (version != (uint32_t)MOQ_VERSION_DRAFT_16 &&
        version != (uint32_t)MOQ_VERSION_DRAFT_18 &&
        version != (uint32_t)MOQ_VERSION_DRAFT_22)
        return MOQ_ERR_INVAL;
    if (track_props.len > 0 && !track_props.data) return MOQ_ERR_INVAL;
    if (track_props.len > 0 && version != (uint32_t)MOQ_VERSION_DRAFT_22)
        return MOQ_ERR_INVAL;

    if (tc.media_type != MOQ_PLAYBACK_MEDIA_VIDEO &&
        tc.media_type != MOQ_PLAYBACK_MEDIA_AUDIO)
        return MOQ_ERR_INVAL;
    if (tc.packaging != MOQ_PLAYBACK_PACKAGING_RAW &&
        tc.packaging != MOQ_PLAYBACK_PACKAGING_CMAF)
        return MOQ_ERR_INVAL;

    if (tc.init_data.len > 0 && !tc.init_data.data)
        return MOQ_ERR_INVAL;
    if (tc.codec.len > 0 && !tc.codec.data)
        return MOQ_ERR_INVAL;
    if (tc.codec.len > pb->cfg.max_track_config_bytes)
        return MOQ_ERR_INVAL;

    /* Enforce the documented per-track codec+init cap on the RAW init_data
     * for ALL packaging, BEFORE any parsing. For CMAF the init_data is the
     * init segment, so an untrusted oversized segment must be rejected here
     * rather than handed to moq_cmaf_parse_init() — otherwise the cap is
     * bypassed and parse work scales with the attacker-controlled length.
     * (codec.len <= max is checked just above, so the subtraction is safe.) */
    if (tc.init_data.len >
        pb->cfg.max_track_config_bytes - tc.codec.len)
        return MOQ_ERR_INVAL;
    if (track_props.len > pb->cfg.max_track_config_bytes - tc.codec.len -
                          tc.init_data.len)
        return MOQ_ERR_INVAL;

    /* Draft-22 track properties are validated before anything is
     * allocated; their parsed form is rebuilt over the owned copy below. */
    moq_loc_metadata_t track_loc;
    moq_loc_metadata_init_sized(&track_loc, sizeof(track_loc));
    if (track_props.len > 0) {
        moq_result_t lr = moq_loc_parse_metadata(MOQ_VERSION_DRAFT_22,
            MOQ_LOC_PROFILE_04, track_props, &track_loc, sizeof(track_loc));
        if (lr == MOQ_ERR_NOMEM) return lr;
        if (lr != MOQ_OK) return MOQ_ERR_PROTO;
    }

    uint32_t slot_idx = 0;
    pb_track_t *slot = NULL;
    for (uint32_t i = 0; i < pb->cfg.max_tracks; i++) {
        if (!pb->tracks[i].active) {
            slot = &pb->tracks[i];
            slot_idx = i;
            break;
        }
    }
    if (!slot) return MOQ_ERR_WOULD_BLOCK;

    moq_rcbuf_t *config_buf = NULL;
    uint32_t eff_timescale = tc.timescale;
    uint32_t eff_width = tc.width;
    uint32_t eff_height = tc.height;
    uint32_t eff_samplerate = tc.samplerate;
    uint32_t eff_channel_count = tc.channel_count;

    if (tc.packaging == MOQ_PLAYBACK_PACKAGING_CMAF &&
        tc.init_data.len > 0) {
        moq_cmaf_init_info_t info;
        moq_cmaf_init_info_init(&info);
        moq_result_t ir = moq_cmaf_parse_init(tc.init_data, &info);
        if (ir != MOQ_OK) return MOQ_ERR_PROTO;
        if (info.timescale != 0) eff_timescale = info.timescale;
        if (info.width != 0) eff_width = info.width;
        if (info.height != 0) eff_height = info.height;
        if (info.samplerate != 0) eff_samplerate = info.samplerate;
        if (info.channel_count != 0) eff_channel_count = info.channel_count;

        if (info.codec_config.len > 0) {
            if (info.codec_config.len >
                pb->cfg.max_track_config_bytes - tc.codec.len)
                return MOQ_ERR_INVAL;
            moq_result_t rc = moq_rcbuf_create(&pb->alloc,
                info.codec_config.data, info.codec_config.len, &config_buf);
            if (rc != MOQ_OK) return rc;
        }
    }

    moq_rcbuf_t *codec_buf = NULL;
    if (tc.codec.len > 0) {
        moq_result_t rc = moq_rcbuf_create(&pb->alloc,
            tc.codec.data, tc.codec.len, &codec_buf);
        if (rc != MOQ_OK) {
            moq_rcbuf_decref(config_buf);
            return rc;
        }
    }

    moq_rcbuf_t *init_buf = NULL;
    if (tc.packaging != MOQ_PLAYBACK_PACKAGING_CMAF &&
        tc.init_data.len > 0) {
        moq_result_t rc = moq_rcbuf_create(&pb->alloc,
            tc.init_data.data, tc.init_data.len, &init_buf);
        if (rc != MOQ_OK) {
            moq_rcbuf_decref(codec_buf);
            moq_rcbuf_decref(config_buf);
            return rc;
        }
    }

    moq_rcbuf_t *props_buf = NULL;
    if (track_props.len > 0) {
        moq_result_t rc = moq_rcbuf_create(&pb->alloc,
            track_props.data, track_props.len, &props_buf);
        if (rc == MOQ_OK) {
            moq_bytes_t owned = { moq_rcbuf_data(props_buf),
                                  moq_rcbuf_len(props_buf) };
            rc = moq_loc_parse_metadata(MOQ_VERSION_DRAFT_22,
                MOQ_LOC_PROFILE_04, owned, &track_loc, sizeof(track_loc));
        }
        if (rc != MOQ_OK) {
            moq_rcbuf_decref(props_buf);
            moq_rcbuf_decref(init_buf);
            moq_rcbuf_decref(codec_buf);
            moq_rcbuf_decref(config_buf);
            return rc;
        }
    }

    uint32_t next_gen;
    if (slot->generation == 0)
        next_gen = pb->generation_seed;
    else {
        next_gen = slot->generation + 2;
        if (next_gen > 0xFFFFFFFu) next_gen = 1;
    }
    memset(slot, 0, sizeof(*slot));
    slot->active = true;
    slot->generation = next_gen;
    slot->media_type = tc.media_type;
    slot->packaging = tc.packaging;
    slot->codec = codec_buf;
    slot->init_data = init_buf;
    slot->codec_config = config_buf;
    slot->timescale = eff_timescale;
    slot->width = eff_width;
    slot->height = eff_height;
    slot->samplerate = eff_samplerate;
    slot->channel_count = eff_channel_count;
    slot->target_latency_us = tc.target_latency_us;
    slot->is_live = tc.is_live;
    slot->waiting_for_keyframe =
        (tc.media_type == MOQ_PLAYBACK_MEDIA_VIDEO);
    slot->transport_version = version;
    slot->track_properties = props_buf;
    slot->has_track_loc = props_buf != NULL;
    if (props_buf) slot->track_loc = track_loc;
    slot->gap_policy = tc.gap_policy;
    slot->ranges = pb->ranges + (size_t)slot_idx * pb->cfg.max_known_ranges;
    slot->evidence = pb->evidence + (size_t)slot_idx * pb->cfg.max_known_ranges;

    pb->track_count++;
    *out = pb_encode_handle(pb->creation_tag, next_gen, slot_idx);
    return MOQ_OK;
}

moq_result_t moq_playback_settle_track(moq_playback_t *pb,
                                        moq_playback_track_t track)
{
    if (!pb) return MOQ_ERR_INVAL;
    uint32_t slot;
    if (!pb_resolve_handle(pb, track, &slot))
        return MOQ_ERR_STALE_HANDLE;
    pb->tracks[slot].settling = true;
    return MOQ_OK;
}

moq_result_t moq_playback_drain_track(moq_playback_t *pb,
                                       moq_playback_track_t track)
{
    if (!pb) return MOQ_ERR_INVAL;
    uint32_t slot;
    if (!pb_resolve_handle(pb, track, &slot))
        return MOQ_ERR_STALE_HANDLE;
    pb->tracks[slot].settling = true;
    pb->tracks[slot].draining = true;
    return MOQ_OK;
}

moq_result_t moq_playback_remove_track(moq_playback_t *pb,
                                        moq_playback_track_t track)
{
    if (!pb) return MOQ_ERR_INVAL;
    uint32_t slot;
    if (!pb_resolve_handle(pb, track, &slot))
        return MOQ_ERR_STALE_HANDLE;

    release_track_objects(pb, track);
    release_track_config(pb, &pb->tracks[slot]);
    pb->tracks[slot].active = false;
    pb->track_count--;

    return MOQ_OK;
}

/* -- Object input ---------------------------------------------------- */

static moq_result_t push_drop_event(moq_playback_t *pb,
                                     moq_playback_track_t track,
                                     moq_playback_drop_reason_t reason,
                                     uint64_t group_id,
                                     uint64_t object_id)
{
    moq_playback_event_t evt;
    memset(&evt, 0, sizeof(evt));
    evt.struct_size = sizeof(evt);
    evt.kind = MOQ_PLAYBACK_EVENT_OBJECT_DROPPED;
    evt.track = track;
    evt.u.object_dropped.reason = reason;
    evt.u.object_dropped.group_id = group_id;
    evt.u.object_dropped.object_id = object_id;
    return push_evt(pb, &evt);
}

static void release_buffered_object(moq_playback_t *pb, pb_object_t *o);
static bool pb_scale_checked(uint64_t ticks, uint32_t ts, uint64_t *out);
static int pb_loc_cmp(uint64_t g, uint64_t o, uint64_t g2, uint64_t o2);
static pb_object_t *find_object(moq_playback_t *pb,
                                 moq_playback_track_t track,
                                 uint64_t group_id,
                                 uint64_t object_id);

/* Parser drop reasons map to playback reasons by name. A reason without a
 * playback counterpart is not a drop this engine reports. */
static bool pb_map_parser_drop(moq_media_drop_reason_t in,
                               moq_playback_drop_reason_t *out)
{
    switch (in) {
    case MOQ_MEDIA_DROP_MALFORMED_LOC:
        *out = MOQ_PLAYBACK_DROP_MALFORMED_LOC; return true;
    case MOQ_MEDIA_DROP_MALFORMED_CMAF:
        *out = MOQ_PLAYBACK_DROP_MALFORMED_CMAF; return true;
    case MOQ_MEDIA_DROP_MISSING_TIMESTAMP:
        *out = MOQ_PLAYBACK_DROP_MISSING_TIMESTAMP; return true;
    case MOQ_MEDIA_DROP_NON_MONOTONIC_DTS:
        *out = MOQ_PLAYBACK_DROP_NON_MONOTONIC_DTS; return true;
    case MOQ_MEDIA_DROP_UNSUPPORTED_MULTI_SAMPLE:
        *out = MOQ_PLAYBACK_DROP_UNSUPPORTED_MULTI_SAMPLE; return true;
    case MOQ_MEDIA_DROP_STALE:
        *out = MOQ_PLAYBACK_DROP_STALE; return true;
    case MOQ_MEDIA_DROP_KEYFRAME_WAIT:
        *out = MOQ_PLAYBACK_DROP_KEYFRAME_WAIT; return true;
    default:
        return false;
    }
}

static bool loc_before(uint64_t g, uint64_t o, uint64_t cg, uint64_t co)
{
    return g < cg || (g == cg && o < co);
}

static bool history_contains(const moq_playback_t *pb,
                             moq_playback_track_t track,
                             uint64_t group_id, uint64_t object_id)
{
    for (size_t i = 0; i < pb->history_count; i++) {
        const pb_released_t *r = &pb->history[i];
        if (r->track == track._opaque && r->group_id == group_id &&
            r->object_id == object_id)
            return true;
    }
    return false;
}

static void history_record(moq_playback_t *pb, const pb_object_t *o)
{
    pb_released_t *r = &pb->history[pb->history_next];
    r->track = o->track._opaque;
    r->group_id = o->group_id;
    r->object_id = o->object_id;
    pb->history_next = (pb->history_next + 1) % pb->cfg.max_buffered_objects;
    if (pb->history_count < pb->cfg.max_buffered_objects)
        pb->history_count++;
}

/* Parse one NORMAL object with the track's companion: LOC-01 through the
 * frozen parser on drafts 16/18, LOC-04 through the sized parser on 22. */
static moq_result_t parse_track_object(moq_playback_t *pb,
                                       const pb_track_t *t,
                                       const moq_playback_object_t *obj,
                                       moq_media_parsed_object_t *parsed,
                                       moq_media_drop_reason_t *drop_reason)
{
    moq_media_track_info_t mti;
    moq_media_track_info_init_sized(&mti, sizeof(mti));
    mti.media_type = (t->media_type == MOQ_PLAYBACK_MEDIA_AUDIO)
        ? MOQ_MEDIA_TYPE_AUDIO : MOQ_MEDIA_TYPE_VIDEO;
    mti.packaging = (t->packaging == MOQ_PLAYBACK_PACKAGING_CMAF)
        ? MOQ_MEDIA_PACKAGING_CMAF : MOQ_MEDIA_PACKAGING_RAW;
    mti.timescale = t->timescale;
    mti.transport_version = (moq_version_t)t->transport_version;

    moq_media_object_input_t moi;
    moq_media_object_input_init(&moi);
    moi.group_id = obj->group_id;
    moi.object_id = obj->object_id;
    moi.status = obj->status;
    moi.end_of_group = obj->end_of_group;
    moi.datagram = obj->datagram;
    moi.payload = obj->payload;
    moi.properties = obj->properties;

    if (t->transport_version != (uint32_t)MOQ_VERSION_DRAFT_22)
        return moq_media_object_parse(&mti, &moi, pb->sample_scratch,
            pb->cfg.max_samples_per_object, parsed, drop_reason);

    moq_media_object_parse_cfg_t pcfg = MOQ_MEDIA_OBJECT_PARSE_CFG_INIT;
    pcfg.companion_profile = MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC;
    if (t->has_track_loc) pcfg.track_loc_metadata = &t->track_loc;
    moq_result_t pr = moq_media_object_parse_sized(&mti, &moi, &pcfg,
        pb->sample_scratch, pb->cfg.max_samples_per_object,
        parsed, sizeof(*parsed), drop_reason);
    /* On the draft-22 companion a repeated known LOC property is refused
     * as UNSUPPORTED rather than reported as a drop: for playback it is a
     * malformed object from the peer, not a caller error. */
    if (pr == MOQ_ERR_UNSUPPORTED) {
        *drop_reason = MOQ_MEDIA_DROP_MALFORMED_LOC;
        return MOQ_ERR_PROTO;
    }
    return pr;
}

/* -- Admission, shared by push_object and push_parsed ------------------ */

/* Identity and framing of one object offered for admission. */
typedef struct {
    moq_playback_track_t track;
    uint64_t             group_id;
    uint64_t             subgroup_id;
    uint64_t             object_id;
    uint8_t              publisher_priority;
    moq_object_status_t  status;
    bool                 end_of_group;
    bool                 datagram;
    moq_rcbuf_t         *payload;
    uint64_t             arrived_us;   /* the push's now_us */
} pb_offer_t;

/* Checks that need no parsing. Sets *done when the offer is settled here:
 * absorbed as a duplicate, absorbed as a stale marker or duplicate of a
 * released object, or dropped STALE (whose event result is returned). */
/* What admission does with `in` before placement, decided without acting
 * on it: placement, absorption (already buffered, or below the cursor and
 * remembered or a status marker), or a STALE drop. Its checks, and their
 * order, are the push's. */
typedef enum { PB_ADMIT_PLACE, PB_ADMIT_ABSORB, PB_ADMIT_DROP_STALE } pb_admit_disp_t;

static moq_result_t admit_classify(moq_playback_t *pb, const pb_offer_t *in,
                                   pb_track_t **t_out, pb_admit_disp_t *disp)
{
    *disp = PB_ADMIT_PLACE;
    uint32_t track_slot;
    if (!pb_resolve_handle(pb, in->track, &track_slot))
        return MOQ_ERR_STALE_HANDLE;

    if (in->status != MOQ_OBJECT_NORMAL &&
        in->status != MOQ_OBJECT_END_OF_GROUP &&
        in->status != MOQ_OBJECT_END_OF_TRACK)
        return MOQ_ERR_INVAL;
    if (in->status == MOQ_OBJECT_NORMAL && !in->payload)
        return MOQ_ERR_INVAL;
    if (in->status != MOQ_OBJECT_NORMAL && in->payload)
        return MOQ_ERR_INVAL;

    pb_track_t *t = &pb->tracks[track_slot];
    *t_out = t;
    if (t->ended)
        return MOQ_ERR_WRONG_STATE;

    if (find_object(pb, in->track, in->group_id, in->object_id)) {
        *disp = PB_ADMIT_ABSORB;
        return MOQ_OK;
    }

    /* Below the cursor after release started: never playable. */
    bool below = t->anchored &&
        loc_before(in->group_id, in->object_id,
                   t->next_expected_group, t->next_expected_object);
    if (below && t->started) {
        if (history_contains(pb, in->track, in->group_id, in->object_id) ||
            in->status != MOQ_OBJECT_NORMAL)
            *disp = PB_ADMIT_ABSORB;
        else
            *disp = PB_ADMIT_DROP_STALE;
    }
    return MOQ_OK;
}

static moq_result_t admit_precheck(moq_playback_t *pb, const pb_offer_t *in,
                                   pb_track_t **t_out, bool *done)
{
    *done = false;
    pb_admit_disp_t disp;
    moq_result_t rc = admit_classify(pb, in, t_out, &disp);
    if (rc != MOQ_OK) return rc;
    if (disp == PB_ADMIT_PLACE) return MOQ_OK;
    *done = true;
    if (disp == PB_ADMIT_ABSORB) return MOQ_OK;
    return push_drop_event(pb, in->track, MOQ_PLAYBACK_DROP_STALE,
                           in->group_id, in->object_id);
}

/* Media rules that apply to every parsed NORMAL object, however it was
 * parsed. Sets *done when the object is dropped with its event. */
static moq_playback_drop_reason_t media_drop_reason(const pb_track_t *t,
                                                    const moq_media_parsed_object_t *parsed)
{
    moq_playback_drop_reason_t reason = 0;

    /* moq_cmaf_parse_fragment describes only the first chunk of an object
     * that holds several; playing that chunk would silently drop the
     * rest, so such an object is refused instead. */
    if (t->packaging == MOQ_PLAYBACK_PACKAGING_CMAF && parsed->chunk_count > 1)
        reason = MOQ_PLAYBACK_DROP_UNSUPPORTED_MULTI_CHUNK;

    /* Playback keeps the stricter timing policy the object parser relaxed:
     * a RAW/LOC object without usable media time cannot be scheduled, so
     * it is dropped here rather than buffered. Usable time is a capture
     * timestamp on drafts 16/18; on draft 22 it is the parsed LOC-04
     * time, UNIX or application media time, which the parser reports
     * only when its timescale makes it representable. (CMAF derives
     * timing from its samples, so this applies to RAW only.) */
    bool d22 = t->transport_version == (uint32_t)MOQ_VERSION_DRAFT_22;
    bool timed = parsed->has_capture_time ||
                 (d22 && parsed->metadata.has_time_us);
    if (t->packaging == MOQ_PLAYBACK_PACKAGING_RAW && !timed)
        reason = MOQ_PLAYBACK_DROP_MISSING_TIMESTAMP;
    return reason;
}

/* The media rules, acted on: a dropped object with its event sets *done. */
static moq_result_t admit_media_checks(moq_playback_t *pb, const pb_track_t *t,
                                       const pb_offer_t *in,
                                       const moq_media_parsed_object_t *parsed,
                                       bool *done)
{
    *done = false;
    moq_playback_drop_reason_t reason = media_drop_reason(t, parsed);
    if (reason == 0) return MOQ_OK;
    *done = true;
    return push_drop_event(pb, in->track, reason, in->group_id, in->object_id);
}

/* What admission keeps beyond the parsed scalars: a multi-sample CMAF
 * object's sample table and base tick (borrowed from the engine's scratch
 * until placement copies them), and the effective in-band decoder
 * configuration (borrowed from the caller until placement copies it). */
typedef struct {
    const moq_cmaf_sample_t *samples;
    size_t                   sample_count;
    uint64_t                 base_ticks;
    bool                     config_present;
    moq_bytes_t              config;
} pb_capture_t;

/* Collect what placement must own. A CMAF object's fragment index is read
 * once here when it has several samples, or when `verify` asks that the
 * caller's record be checked against it: the record must then come from
 * this payload with the track's timescale (same sample table, and its
 * decode time equal to the fragment's base tick at that scale). A track
 * without a timescale has no timing context: the scaling check refuses
 * such a record.
 * The draft-22 effective configuration for the track's media type is
 * taken from the record's LOC metadata; presence with no bytes is a
 * present-but-empty configuration. */
static moq_result_t admit_capture(moq_playback_t *pb, const pb_track_t *t,
                                  const pb_offer_t *in,
                                  const moq_media_parsed_object_t *parsed,
                                  uint32_t parsed_size, bool verify,
                                  pb_capture_t *cap)
{
    memset(cap, 0, sizeof(*cap));
    if (in->status != MOQ_OBJECT_NORMAL) return MOQ_OK;

    if (t->packaging == MOQ_PLAYBACK_PACKAGING_CMAF &&
        (verify || parsed->sample_count > 1)) {
        moq_bytes_t frag_bytes = {
            moq_rcbuf_data(in->payload), moq_rcbuf_len(in->payload) };
        moq_cmaf_fragment_info_t frag;
        moq_cmaf_fragment_info_init(&frag, pb->sample_scratch,
                                     pb->cfg.max_samples_per_object);
        if (pb_read_fragment(pb, frag_bytes, &frag) != MOQ_OK ||
            frag.sample_count != parsed->sample_count)
            return MOQ_ERR_INVAL;
        if (verify) {
            /* The record's times at the track's scale: its decode time is
             * the base tick, its sample duration the first sample's. */
            uint64_t base_us, dur_us;
            if (!pb_scale_checked(frag.base_decode_time, t->timescale,
                                  &base_us) ||
                base_us != parsed->decode_time_us ||
                !pb_scale_checked(frag.samples[0].duration, t->timescale,
                                  &dur_us) ||
                dur_us != parsed->sample_duration_us)
                return MOQ_ERR_INVAL;
            if (parsed->samples &&
                memcmp(parsed->samples, frag.samples,
                       frag.sample_count * sizeof(moq_cmaf_sample_t)) != 0)
                return MOQ_ERR_INVAL;
            /* Its media range is the fragment's mdat, and the samples fit
             * inside it. */
            const uint8_t *base = moq_rcbuf_data(in->payload);
            if (frag.mdat.data < base ||
                (size_t)(frag.mdat.data - base) != parsed->mdat_offset ||
                frag.mdat.len != parsed->mdat_len)
                return MOQ_ERR_INVAL;
            uint64_t total = 0;
            for (size_t i = 0; i < frag.sample_count; i++)
                total += frag.samples[i].size;
            if (total > parsed->mdat_len)
                return MOQ_ERR_INVAL;
        }
        if (frag.sample_count > 1) {
            cap->samples = frag.samples;
            cap->sample_count = frag.sample_count;
            cap->base_ticks = frag.base_decode_time;
        }
    }

    if (t->transport_version == (uint32_t)MOQ_VERSION_DRAFT_22 &&
        parsed_size >= MOQ_MEDIA_PARSED_OBJECT_METADATA_SIZE) {
        bool audio = t->media_type == MOQ_PLAYBACK_MEDIA_AUDIO;
        uint32_t bit = audio ? MOQ_LOC_METADATA_HAS_AUDIO_CONFIG
                             : MOQ_LOC_METADATA_HAS_VIDEO_CONFIG;
        if (parsed->metadata.loc.present & bit) {
            cap->config_present = true;
            cap->config = audio ? parsed->metadata.loc.audio_config
                                : parsed->metadata.loc.video_config;
            if (cap->config.len > 0 && !cap->config.data)
                return MOQ_ERR_INVAL;
        }
    }
    return MOQ_OK;
}

/* What placing `in` would charge, with the engine as it is: payload,
 * sample table and a new configuration copy unless one the track holds is
 * reused. MOQ_ERR_LIMIT when it can never fit (placement's LIMIT). */
typedef struct {
    size_t       plen, table_bytes, new_config;
    moq_rcbuf_t *reuse;
    uint64_t     charge;       /* payload + sample table */
    uint64_t     need;         /* charge + new configuration */
} pb_charge_t;

static moq_result_t admission_charge(moq_playback_t *pb, const pb_track_t *t,
                                     const pb_offer_t *in, const pb_capture_t *cap,
                                     pb_charge_t *out)
{
    memset(out, 0, sizeof(*out));

    /* The charge is everything the object keeps: payload, sample table and
     * configuration copy. A configuration over max_track_config_bytes, or
     * a charge over the whole byte budget, can never be admitted. */
    size_t plen = in->payload ? moq_rcbuf_len(in->payload) : 0;
    size_t table_bytes = cap->sample_count * sizeof(moq_cmaf_sample_t);
    if (cap->config.len > pb->cfg.max_track_config_bytes)
        return MOQ_ERR_LIMIT;

    /* A configuration the track already holds, as its decoder or release
     * configuration, on one of its buffered objects or in one of its
     * queued CONFIGURE commands, is reused: no new copy, no new charge. */
    moq_rcbuf_t *reuse = NULL;
    if (cap->config_present && cap->config.len > 0) {
        moq_rcbuf_t *cand[2] = { t->cur_config, t->rel_config };
        for (int k = 0; k < 2 && !reuse; k++)
            if (cand[k] && moq_rcbuf_len(cand[k]) == cap->config.len &&
                memcmp(moq_rcbuf_data(cand[k]), cap->config.data,
                       cap->config.len) == 0)
                reuse = cand[k];
        for (size_t i = 0; i < pb->cfg.max_buffered_objects && !reuse; i++) {
            pb_object_t *o = &pb->objects[i];
            if (o->occupied && o->track._opaque == in->track._opaque &&
                o->config && moq_rcbuf_len(o->config) == cap->config.len &&
                memcmp(moq_rcbuf_data(o->config), cap->config.data,
                       cap->config.len) == 0)
                reuse = o->config;
        }
        for (size_t i = 0; i < pb->cmd_count && !reuse; i++) {
            moq_playback_cmd_t *c =
                &pb->cmd_ring[(pb->cmd_head + i) % pb->cfg.max_commands];
            moq_rcbuf_t *cc = cmd_config(c);
            if (cc && c->track._opaque == in->track._opaque &&
                copy_find(pb, cc) && moq_rcbuf_len(cc) == cap->config.len &&
                memcmp(moq_rcbuf_data(cc), cap->config.data,
                       cap->config.len) == 0)
                reuse = cc;
        }
    }
    size_t new_config = (cap->config_present && !reuse) ? cap->config.len : 0;

    uint64_t charge = (uint64_t)plen;
    if (table_bytes > UINT64_MAX - charge) return MOQ_ERR_LIMIT;
    charge += table_bytes;
    uint64_t need = charge;
    if (new_config > UINT64_MAX - need) return MOQ_ERR_LIMIT;
    need += new_config;
    if (need > pb->cfg.max_buffered_bytes)
        return MOQ_ERR_LIMIT;
    /* With nothing buffered, what is retained is configuration the tracks
     * keep, which no release, relief or timer can free, and bytes polling
     * and hold release free: copies and units only queued commands and
     * command holds still own. An object that does not fit beside the
     * first kind never will. */
    if (pb->obj_count == 0 &&
        need > pb->cfg.max_buffered_bytes -
               (pb->retained_bytes - copies_drainable(pb) - pb->cmd_bytes -
                pb->hold_bytes))
        return MOQ_ERR_LIMIT;

    out->plen = plen;
    out->table_bytes = table_bytes;
    out->new_config = new_config;
    out->reuse = reuse;
    out->charge = charge;
    out->need = need;
    return MOQ_OK;
}

/* Place an admitted object. MOQ_ERR_LIMIT when it can never fit the byte
 * budget; MOQ_ERR_WOULD_BLOCK, noting the request for tick, when the
 * buffer is full now. Either refusal retains nothing and changes no
 * buffered object, ref, history entry or event. */
static moq_result_t admit_place(moq_playback_t *pb, pb_track_t *t,
                                const pb_offer_t *in,
                                const moq_media_parsed_object_t *parsed,
                                const pb_capture_t *cap)
{
    pb_charge_t ch;
    moq_result_t crc = admission_charge(pb, t, in, cap, &ch);
    if (crc != MOQ_OK) return crc;
    size_t plen = ch.plen, table_bytes = ch.table_bytes, new_config = ch.new_config;
    moq_rcbuf_t *reuse = ch.reuse;
    uint64_t charge = ch.charge, need = ch.need;

    pb_object_t *oslot = NULL;
    if (pb->obj_count < pb->cfg.max_buffered_objects &&
        need <= pb->cfg.max_buffered_bytes - pb->retained_bytes) {
        for (size_t i = 0; i < pb->cfg.max_buffered_objects; i++) {
            if (!pb->objects[i].occupied) {
                oslot = &pb->objects[i];
                break;
            }
        }
    }
    if (!oslot) {
        pb->has_waiting_admission = true;
        pb->wait_track = in->track._opaque;
        pb->wait_group_id = in->group_id;
        pb->wait_object_id = in->object_id;
        pb->wait_bytes = need;
        /* A reused catalog configuration is no copy and never freed. */
        const pb_copy_t *rc = copy_find(pb, reuse);
        pb->wait_reuse_slot = rc ? (size_t)(rc - pb->copies) : 0;
        pb->wait_reuse_serial = rc ? rc->serial : 0;
        pb->wait_reuse_len = rc ? cap->config.len : 0;
        return MOQ_ERR_WOULD_BLOCK;
    }

    /* Owned copies, before any state changes: an allocation failure
     * retains nothing. */
    moq_cmaf_sample_t *table = NULL;
    if (table_bytes) {
        table = (moq_cmaf_sample_t *)pb->alloc.alloc(table_bytes,
                                                     pb->alloc.ctx);
        if (!table) return MOQ_ERR_NOMEM;
        memcpy(table, cap->samples, table_bytes);
    }
    moq_rcbuf_t *config = NULL;
    if (new_config > 0) {
        moq_result_t rc = moq_rcbuf_create(&pb->alloc, cap->config.data,
                                           cap->config.len, &config);
        if (rc != MOQ_OK) {
            if (table) pb->alloc.free(table, table_bytes, pb->alloc.ctx);
            return rc;
        }
    }

    /* Entry placement before release starts. An entry is a video object
     * with independence evidence or any audio object. The first entry sets
     * the cursor, wherever it lies; a later entry below the cursor moves it
     * back. An END_OF_TRACK marker sets the cursor of a track that has no
     * entry, so a track that never produced one still ends. Anything else
     * waits in the buffer; whatever is below the cursor when release starts
     * is dropped then. */
    bool below = t->anchored &&
        loc_before(in->group_id, in->object_id,
                   t->next_expected_group, t->next_expected_object);
    bool entry = in->status == MOQ_OBJECT_NORMAL &&
        (t->media_type == MOQ_PLAYBACK_MEDIA_AUDIO || parsed->keyframe);
    if (!t->anchored) {
        if (entry || in->status == MOQ_OBJECT_END_OF_TRACK) {
            t->anchored = true;
            t->next_expected_group = in->group_id;
            t->next_expected_object = in->object_id;
        }
        t->entry_cleanup_pending = true;
    } else if (below) {
        if (entry) {
            t->next_expected_group = in->group_id;
            t->next_expected_object = in->object_id;
            t->gap_waiting = false;
        }
        t->entry_cleanup_pending = true;
    }

    if (in->payload)
        moq_rcbuf_incref(in->payload);

    memset(oslot, 0, sizeof(*oslot));
    oslot->occupied = true;
    oslot->track = in->track;
    oslot->group_id = in->group_id;
    oslot->subgroup_id = in->subgroup_id;
    oslot->object_id = in->object_id;
    oslot->publisher_priority = in->publisher_priority;
    oslot->status = in->status;
    oslot->end_of_group = in->end_of_group;
    oslot->datagram = in->datagram;
    oslot->payload = in->payload;
    oslot->payload_len = plen;
    oslot->has_capture_time = parsed->has_capture_time;
    oslot->capture_time_us = parsed->capture_time_us;
    oslot->decode_time_us = parsed->decode_time_us;
    oslot->composition_offset_us = parsed->composition_offset_us;
    oslot->presentation_time_us = parsed->presentation_time_us;
    oslot->keyframe = parsed->keyframe;
    oslot->sample_duration_us = parsed->sample_duration_us;
    oslot->mdat_offset = parsed->mdat_offset;
    oslot->mdat_len = parsed->mdat_len;
    oslot->sample_count = table ? cap->sample_count : parsed->sample_count;
    oslot->samples = table;
    oslot->base_ticks = cap->base_ticks;
    oslot->config_present = cap->config_present;
    oslot->charge = charge;
    oslot->arrived_us = in->arrived_us;
    if (config) {
        /* A fresh copy: this object is its first holder. */
        oslot->config = config;
        copy_register(pb, config);
    } else if (reuse) {
        cfg_hold(pb, reuse, false);
        oslot->config = reuse;
    }

    pb->obj_count++;
    pb->retained_bytes += charge;
    return MOQ_OK;
}

moq_result_t moq_playback_push_object(moq_playback_t *pb,
                                       const moq_playback_object_t *obj,
                                       uint64_t now_us)
{
    if (!pb || !obj) return MOQ_ERR_INVAL;
    if (obj->struct_size < MOQ_PLAYBACK_OBJECT_V0_SIZE)
        return MOQ_ERR_INVAL;
    pb_offer_t in = {
        .track = obj->track, .group_id = obj->group_id,
        .subgroup_id = obj->subgroup_id, .object_id = obj->object_id,
        .publisher_priority = obj->publisher_priority, .status = obj->status,
        .end_of_group = obj->end_of_group, .datagram = obj->datagram,
        .payload = obj->payload, .arrived_us = now_us,
    };
    pb_track_t *t = NULL;
    bool done = false;
    moq_result_t rc = admit_precheck(pb, &in, &t, &done);
    if (rc != MOQ_OK || done) return rc;

    moq_media_parsed_object_t parsed;
    memset(&parsed, 0, sizeof(parsed));

    if (obj->status == MOQ_OBJECT_NORMAL) {
        moq_media_drop_reason_t drop_reason = 0;
        moq_result_t pr = parse_track_object(pb, t, obj, &parsed,
                                             &drop_reason);

        if (pr == MOQ_ERR_BUFFER)
            return push_drop_event(pb, obj->track,
                MOQ_PLAYBACK_DROP_UNSUPPORTED_MULTI_SAMPLE,
                obj->group_id, obj->object_id);
        moq_playback_drop_reason_t pb_reason;
        if (pr == MOQ_ERR_PROTO && pb_map_parser_drop(drop_reason, &pb_reason))
            return push_drop_event(pb, obj->track, pb_reason,
                obj->group_id, obj->object_id);
        if (pr != MOQ_OK) return MOQ_ERR_INVAL;

        rc = admit_media_checks(pb, t, &in, &parsed, &done);
        if (rc != MOQ_OK || done) return rc;
    }
    pb_capture_t cap;
    rc = admit_capture(pb, t, &in, &parsed, (uint32_t)sizeof(parsed), false,
                       &cap);
    if (rc != MOQ_OK) return rc;
    return admit_place(pb, t, &in, &parsed, &cap);
}

void moq_playback_parsed_input_init_sized(moq_playback_parsed_input_t *in,
                                          size_t in_size)
{
    if (!in || in_size < sizeof(uint32_t)) return;
    size_t n = in_size < sizeof(*in) ? in_size : sizeof(*in);
    memset(in, 0, n);
    in->struct_size = (uint32_t)n;
}

/* push_parsed's checks on the record, in its order, and what admission
 * then works from. *over_samples: a CMAF object over the sample cap, which
 * admission drops (its capture is skipped). Shared by push_parsed and the
 * admission observer, so both refuse exactly the same records. */
static moq_result_t prepare_parsed(moq_playback_t *pb,
                                   const moq_playback_parsed_input_t *in,
                                   uint64_t now_us, pb_track_t **t_out,
                                   pb_offer_t *offer_out,
                                   moq_media_parsed_object_t *parsed_out,
                                   pb_capture_t *cap_out, bool *over_samples)
{
    if (!pb || !in) return MOQ_ERR_INVAL;
    if (in->struct_size < MOQ_PLAYBACK_PARSED_INPUT_V0_SIZE)
        return MOQ_ERR_INVAL;

    const moq_media_parsed_object_t *p = in->parsed;
    if (!p || p->struct_size < MOQ_MEDIA_PARSED_OBJECT_V0_SIZE)
        return MOQ_ERR_INVAL;
    uint32_t slot;
    if (!pb_resolve_handle(pb, in->track, &slot))
        return MOQ_ERR_STALE_HANDLE;
    pb_track_t *t = &pb->tracks[slot];
    *t_out = t;
    *over_samples = false;

    /* Every check on the record comes before any admission effect, so an
     * invalid record is refused even where a valid one would be absorbed
     * or dropped. A draft-22 record must cover the metadata, whatever its
     * status. */
    if (t->transport_version == (uint32_t)MOQ_VERSION_DRAFT_22 &&
        p->struct_size < MOQ_MEDIA_PARSED_OBJECT_METADATA_SIZE)
        return MOQ_ERR_INVAL;

    /* The record is trusted as the parser's output for this payload; what
     * the engine relies on is re-checked. Only a copy of the scalars is
     * kept: the record and its spans are read during this call only. */
    moq_media_parsed_object_t parsed;
    memset(&parsed, 0, sizeof(parsed));
    size_t take = p->struct_size < sizeof(parsed) ? p->struct_size
                                                 : sizeof(parsed);
    memcpy(&parsed, p, take);
    *parsed_out = parsed;

    pb_offer_t offer = {
        .track = in->track, .group_id = in->group_id,
        .subgroup_id = in->subgroup_id, .object_id = in->object_id,
        .publisher_priority = in->publisher_priority, .status = parsed.status,
        .end_of_group = parsed.end_of_group, .datagram = parsed.datagram,
        .payload = in->payload, .arrived_us = now_us,
    };
    if ((offer.status == MOQ_OBJECT_NORMAL) != (in->payload != NULL))
        return MOQ_ERR_INVAL;
    *offer_out = offer;

    pb_capture_t cap;
    memset(&cap, 0, sizeof(cap));
    if (parsed.status == MOQ_OBJECT_NORMAL) {
        bool cmaf = t->packaging == MOQ_PLAYBACK_PACKAGING_CMAF;
        size_t plen = moq_rcbuf_len(in->payload);
        if (parsed.packaging != (cmaf ? MOQ_MEDIA_PACKAGING_CMAF
                                      : MOQ_MEDIA_PACKAGING_RAW))
            return MOQ_ERR_INVAL;
        if (!cmaf && (parsed.payload.data != moq_rcbuf_data(in->payload) ||
                      parsed.payload.len != plen))
            return MOQ_ERR_INVAL;
        if (cmaf && (parsed.sample_count == 0 ||
                     parsed.mdat_offset > plen ||
                     parsed.mdat_len > plen - parsed.mdat_offset))
            return MOQ_ERR_INVAL;
        /* The timing context is explicit: the caller names the scale it
         * parsed with, and it must be the track's. This precedes every
         * disposition, the sample-cap drop included. */
        if (cmaf && (in->timescale == 0 || in->timescale != t->timescale))
            return MOQ_ERR_INVAL;
        if (!(cmaf && parsed.sample_count > pb->cfg.max_samples_per_object)) {
            moq_result_t vr = admit_capture(pb, t, &offer, &parsed,
                                            p->struct_size, true, &cap);
            if (vr != MOQ_OK) return vr;
        } else {
            *over_samples = true;
        }
    }
    *cap_out = cap;
    return MOQ_OK;
}


moq_result_t moq_playback_push_parsed(moq_playback_t *pb,
                                       const moq_playback_parsed_input_t *in,
                                       uint64_t now_us)
{
    pb_track_t *t = NULL;
    pb_offer_t offer;
    moq_media_parsed_object_t parsed;
    pb_capture_t cap;
    bool over_samples = false;
    moq_result_t rc = prepare_parsed(pb, in, now_us, &t, &offer, &parsed, &cap,
                                     &over_samples);
    if (rc != MOQ_OK) return rc;

    bool done = false;
    rc = admit_precheck(pb, &offer, &t, &done);
    if (rc != MOQ_OK || done) return rc;

    if (parsed.status == MOQ_OBJECT_NORMAL) {
        if (over_samples)
            return push_drop_event(pb, in->track,
                MOQ_PLAYBACK_DROP_UNSUPPORTED_MULTI_SAMPLE,
                in->group_id, in->object_id);
        rc = admit_media_checks(pb, t, &offer, &parsed, &done);
        if (rc != MOQ_OK || done) return rc;
    }
    return admit_place(pb, t, &offer, &parsed, &cap);
}

/* -- Admission observation ----------------------------------------- */

/* Release sets for the admission observer: what relief alone can free
 * (buffered objects of tracks without an entry, relief's only victims),
 * and what the application can free besides (buffered objects of tracks
 * paused by queue pressure, queued commands and command holds). */
typedef enum { PB_SET_RELIEF = 1, PB_SET_APPLICATION = 2 } pb_release_set_t;

static bool obj_in_set(const moq_playback_t *pb, const pb_object_t *o, unsigned sets)
{
    uint32_t slot;
    if (!o->occupied || !pb_resolve_handle(pb, o->track, &slot)) return false;
    const pb_track_t *t = &pb->tracks[slot];
    if ((sets & PB_SET_RELIEF) && !t->anchored) return true;
    if ((sets & PB_SET_APPLICATION) && t->anchored && t->paused_by_pressure) return true;
    return false;
}

/* A configuration copy is freed by a release only when every holder is in
 * it: a track's decoder or release slot keeps it in every set. */
static bool copy_freed_by(const moq_playback_t *pb, const pb_copy_t *c, unsigned sets)
{
    uint32_t objects = 0, in_set = 0;
    for (size_t i = 0; i < pb->cfg.max_buffered_objects; i++) {
        const pb_object_t *o = &pb->objects[i];
        if (!o->occupied || o->config != c->buf) continue;
        objects++;
        if (obj_in_set(pb, o, sets)) in_set++;
    }
    uint32_t queued = c->ring_holders + c->hold_holders;
    if (c->holders < queued || c->holders - queued < objects) return false;
    if (c->holders - queued - objects != 0) return false;   /* a track slot */
    if (in_set != objects) return false;
    return queued == 0 || (sets & PB_SET_APPLICATION);
}

/* Whether a configuration copy matching `cap` survives the release: one the
 * track's slots hold always does; one a buffered object or a queued command
 * of the track holds does unless the release frees it. */
static bool config_reuse_survives(const moq_playback_t *pb, const pb_track_t *t,
                                  moq_playback_track_t track, const pb_capture_t *cap,
                                  unsigned sets)
{
    const moq_rcbuf_t *cand[2] = { t->cur_config, t->rel_config };
    for (int k = 0; k < 2; k++)
        if (cand[k] && moq_rcbuf_len(cand[k]) == cap->config.len &&
            memcmp(moq_rcbuf_data(cand[k]), cap->config.data, cap->config.len) == 0)
            return true;
    for (size_t i = 0; i < pb->copy_cap; i++) {
        const pb_copy_t *c = &pb->copies[i];
        if (!c->buf || moq_rcbuf_len(c->buf) != cap->config.len ||
            memcmp(moq_rcbuf_data(c->buf), cap->config.data, cap->config.len) != 0)
            continue;
        /* A holder of this track's: a buffered object or a queued command. */
        bool ours = false;
        for (size_t j = 0; j < pb->cfg.max_buffered_objects && !ours; j++)
            ours = pb->objects[j].occupied && pb->objects[j].config == c->buf &&
                   pb->objects[j].track._opaque == track._opaque;
        for (size_t j = 0; j < pb->cmd_count && !ours; j++) {
            const moq_playback_cmd_t *cmd =
                &pb->cmd_ring[(pb->cmd_head + j) % pb->cfg.max_commands];
            ours = cmd_config(cmd) == c->buf && cmd->track._opaque == track._opaque;
        }
        if (ours && !copy_freed_by(pb, c, sets)) return true;
    }
    return false;
}

/* Whether `in` would fit once the release `sets` has freed what it frees. */
static bool fits_after(const moq_playback_t *pb, const pb_track_t *t,
                       moq_playback_track_t track, const pb_capture_t *cap,
                       const pb_charge_t *ch, unsigned sets)
{
    size_t slots = 0;
    uint64_t freed = 0;
    for (size_t i = 0; i < pb->cfg.max_buffered_objects; i++) {
        const pb_object_t *o = &pb->objects[i];
        if (!obj_in_set(pb, o, sets)) continue;
        slots++;
        freed += o->charge;
    }
    if (sets & PB_SET_APPLICATION) freed += pb->cmd_bytes + pb->hold_bytes;
    for (size_t i = 0; i < pb->copy_cap; i++) {
        const pb_copy_t *c = &pb->copies[i];
        if (c->buf && copy_freed_by(pb, c, sets)) freed += moq_rcbuf_len(c->buf);
    }
    uint64_t retained = freed < pb->retained_bytes ? pb->retained_bytes - freed : 0;
    uint64_t need = ch->charge;
    if (cap->config_present && cap->config.len > 0 &&
        !config_reuse_survives(pb, t, track, cap, sets)) {
        if (cap->config.len > UINT64_MAX - need) return false;
        need += cap->config.len;
    }
    size_t objects = pb->obj_count >= slots ? pb->obj_count - slots : 0;
    return objects < pb->cfg.max_buffered_objects &&
           need <= pb->cfg.max_buffered_bytes &&
           retained <= pb->cfg.max_buffered_bytes - need;
}

moq_result_t moq_playback_get_admission(const moq_playback_t *cpb,
                                        const moq_playback_parsed_input_t *in,
                                        moq_playback_admission_info_t *out,
                                        size_t out_size)
{
    if (!cpb || !in || !out || out_size < MOQ_PLAYBACK_ADMISSION_INFO_V0_SIZE)
        return MOQ_ERR_INVAL;
    /* Read-only: the validation reads a CMAF index into the engine's
     * scratch, which holds nothing between calls; nothing else is
     * written. */
    moq_playback_t *pb = (moq_playback_t *)cpb;
    pb_track_t *t = NULL;
    pb_offer_t offer;
    moq_media_parsed_object_t parsed;
    pb_capture_t cap;
    bool over_samples = false;
    moq_result_t rc = prepare_parsed(pb, in, 0, &t, &offer, &parsed, &cap, &over_samples);
    if (rc != MOQ_OK) return rc;
    pb_admit_disp_t disp;
    rc = admit_classify(pb, &offer, &t, &disp);
    if (rc != MOQ_OK) return rc;

    moq_playback_admission_info_t info;
    memset(&info, 0, sizeof(info));
    info.kind = MOQ_PLAYBACK_ADMISSION_FITS;
    bool placed = disp == PB_ADMIT_PLACE &&
        !(parsed.status == MOQ_OBJECT_NORMAL &&
          (over_samples || media_drop_reason(t, &parsed) != 0));
    if (placed) {
        pb_charge_t ch;
        rc = admission_charge(pb, t, &offer, &cap, &ch);
        if (rc == MOQ_ERR_LIMIT) {
            info.kind = MOQ_PLAYBACK_ADMISSION_NEVER;
        } else if (rc != MOQ_OK) {
            return rc;
        } else {
            info.charge_bytes = ch.need;
            if (fits_after(pb, t, offer.track, &cap, &ch, 0))
                info.kind = MOQ_PLAYBACK_ADMISSION_FITS;
            else if (fits_after(pb, t, offer.track, &cap, &ch, PB_SET_RELIEF))
                info.kind = MOQ_PLAYBACK_ADMISSION_RELIEF;
            else if (fits_after(pb, t, offer.track, &cap, &ch,
                                PB_SET_RELIEF | PB_SET_APPLICATION))
                info.kind = MOQ_PLAYBACK_ADMISSION_APPLICATION;
            else
                info.kind = MOQ_PLAYBACK_ADMISSION_ENGINE;
        }
    }
    size_t n = out_size < sizeof(info) ? out_size : sizeof(info);
    info.struct_size = (uint32_t)n;
    memcpy(out, &info, n);
    return MOQ_OK;
}

/* -- Tick ------------------------------------------------------------- */

static pb_object_t *find_object(moq_playback_t *pb,
                                 moq_playback_track_t track,
                                 uint64_t group_id,
                                 uint64_t object_id)
{
    for (size_t i = 0; i < pb->cfg.max_buffered_objects; i++) {
        pb_object_t *o = &pb->objects[i];
        if (o->occupied &&
            o->track._opaque == track._opaque &&
            o->group_id == group_id &&
            o->object_id == object_id)
            return o;
    }
    return NULL;
}

static bool obj_closes_group(const moq_playback_t *pb,
                              const pb_object_t *o)
{
    if (o->status == MOQ_OBJECT_END_OF_GROUP) return true;
    if (pb->cfg.ignore_eog_bit) return false;
    return o->end_of_group;
}

static bool has_later_object_in_group(moq_playback_t *pb,
                                       moq_playback_track_t track,
                                       uint64_t group_id,
                                       uint64_t after_object_id)
{
    for (size_t i = 0; i < pb->cfg.max_buffered_objects; i++) {
        pb_object_t *o = &pb->objects[i];
        if (o->occupied && o->track._opaque == track._opaque &&
            o->group_id == group_id && o->object_id > after_object_id)
            return true;
    }
    return false;
}

/* The track's buffered object at the lowest location after (g, o). */
static pb_object_t *next_present(moq_playback_t *pb, moq_playback_track_t track,
                                 uint64_t g, uint64_t o)
{
    pb_object_t *best = NULL;
    for (size_t i = 0; i < pb->cfg.max_buffered_objects; i++) {
        pb_object_t *x = &pb->objects[i];
        if (!x->occupied || x->track._opaque != track._opaque) continue;
        if (pb_loc_cmp(x->group_id, x->object_id, g, o) <= 0) continue;
        if (!best || pb_loc_cmp(x->group_id, x->object_id,
                                best->group_id, best->object_id) < 0)
            best = x;
    }
    return best;
}

static bool has_objects_in_group(moq_playback_t *pb,
                                  moq_playback_track_t track,
                                  uint64_t group_id)
{
    for (size_t i = 0; i < pb->cfg.max_buffered_objects; i++) {
        pb_object_t *o = &pb->objects[i];
        if (o->occupied && o->track._opaque == track._opaque &&
            o->group_id == group_id)
            return true;
    }
    return false;
}

static bool has_buffered_objects(const moq_playback_t *pb,
                                 moq_playback_track_t track)
{
    for (size_t i = 0; i < pb->cfg.max_buffered_objects; i++) {
        const pb_object_t *o = &pb->objects[i];
        if (o->occupied && o->track._opaque == track._opaque)
            return true;
    }
    return false;
}

static bool group_is_closed(moq_playback_t *pb,
                             moq_playback_track_t track,
                             uint64_t group_id)
{
    for (size_t i = 0; i < pb->cfg.max_buffered_objects; i++) {
        pb_object_t *o = &pb->objects[i];
        if (!o->occupied || o->track._opaque != track._opaque ||
            o->group_id != group_id)
            continue;
        if (obj_closes_group(pb, o))
            return true;
    }
    return false;
}

static void release_buffered_object(moq_playback_t *pb, pb_object_t *o)
{
    history_record(pb, o);
    free_object_storage(pb, o);
}

/* Release an object whose last unit just entered the command ring: its
 * charge moves to that command and stays until the command is polled. */
static void release_into_command(moq_playback_t *pb, pb_object_t *o)
{
    uint64_t charge = o->charge;
    release_buffered_object(pb, o);
    size_t last = (pb->cmd_head + pb->cmd_count - 1) % pb->cfg.max_commands;
    pb->retained_bytes += charge;
    pb->cmd_charge[last] += charge;
    pb->cmd_bytes += charge;
}

/* A queued command leaves the ring: its charge goes with it. */
static void cmd_uncharge(moq_playback_t *pb, size_t ri)
{
    pb->retained_bytes -= pb->cmd_charge[ri];
    pb->cmd_bytes -= pb->cmd_charge[ri];
    pb->cmd_charge[ri] = 0;
}

static void enter_recovery(pb_track_t *t, uint64_t abandoned)
{
    /* A CONTINUE track's units do not depend on earlier ones: it keeps its
     * decoder state and resumes at the next unit. */
    bool cont = t->gap_policy == MOQ_PLAYBACK_GAP_CONTINUE;
    t->waiting_for_keyframe =
        !cont && t->media_type == MOQ_PLAYBACK_MEDIA_VIDEO;
    t->kf_wait_event_emitted = false;
    t->needs_reset = !cont;
    t->has_skip_pending = true;
    t->abandoned_group = abandoned;
    t->gap_waiting = false;
}

static uint64_t highest_buffered_group(moq_playback_t *pb,
                                        moq_playback_track_t track)
{
    uint64_t hi = 0;
    bool found = false;
    for (size_t i = 0; i < pb->cfg.max_buffered_objects; i++) {
        pb_object_t *o = &pb->objects[i];
        if (o->occupied && o->track._opaque == track._opaque) {
            if (!found || o->group_id > hi) {
                hi = o->group_id;
                found = true;
            }
        }
    }
    return found ? hi : 0;
}

static void drop_groups_range(moq_playback_t *pb,
                               moq_playback_track_t track,
                               uint64_t from_group, uint64_t to_group)
{
    for (size_t i = 0; i < pb->cfg.max_buffered_objects; i++) {
        pb_object_t *o = &pb->objects[i];
        if (o->occupied && o->track._opaque == track._opaque &&
            o->group_id >= from_group && o->group_id <= to_group)
            release_buffered_object(pb, o);
    }
}

static void drop_group_objects(moq_playback_t *pb,
                                moq_playback_track_t track,
                                uint64_t group_id)
{
    for (size_t i = 0; i < pb->cfg.max_buffered_objects; i++) {
        pb_object_t *o = &pb->objects[i];
        if (o->occupied &&
            o->track._opaque == track._opaque &&
            o->group_id == group_id)
            release_buffered_object(pb, o);
    }
}

/* A consumed object/marker closed t->next_expected_group: drop any remaining
 * buffered objects from that same group (higher object IDs that arrived before
 * the closer) so they are not stranded once the anchor advances, then advance
 * to the next group. The consumed closer must already be released --
 * drop_group_objects() safely skips its now-empty slot. Call ONLY after all
 * command/event emission for the closer has succeeded, to preserve the
 * commit-last contract (no cleanup on a WOULD_BLOCK before advancement). */
static void advance_past_closed_group(moq_playback_t *pb,
                                       pb_track_t *t,
                                       moq_playback_track_t handle)
{
    drop_group_objects(pb, handle, t->next_expected_group);
    t->next_expected_group++;
    t->next_expected_object = 0;
}

/* -- Knowledge: absence and unresolved delivery ------------------------ */

static int pb_loc_cmp(uint64_t g, uint64_t o, uint64_t g2, uint64_t o2)
{
    if (g != g2) return g < g2 ? -1 : 1;
    if (o != o2) return o < o2 ? -1 : 1;
    return 0;
}

/* The location after (g, o); false past the end of the track. */
static bool pb_loc_next(uint64_t g, uint64_t o, uint64_t *ng, uint64_t *no)
{
    if (o != UINT64_MAX) { *ng = g; *no = o + 1; return true; }
    if (g == UINT64_MAX) return false;
    *ng = g + 1;
    *no = 0;
    return true;
}

/* Whether `b` starts at or before the location right after `a` ends. */
static bool pb_range_touches(const pb_range_t *a, const pb_range_t *b)
{
    uint64_t ng, no;
    if (!pb_loc_next(a->g1, a->o1, &ng, &no)) return true;
    return pb_loc_cmp(b->g0, b->o0, ng, no) <= 0 &&
           pb_loc_cmp(a->g0, a->o0, b->g1, b->o1) <= 0;
}

static void pb_range_union(pb_range_t *into, const pb_range_t *r)
{
    if (pb_loc_cmp(r->g0, r->o0, into->g0, into->o0) < 0) {
        into->g0 = r->g0;
        into->o0 = r->o0;
    }
    if (pb_loc_cmp(r->g1, r->o1, into->g1, into->o1) > 0) {
        into->g1 = r->g1;
        into->o1 = r->o1;
    }
}

/* Drop the ranges wholly behind the cursor: they can no longer matter. */
static void know_prune(pb_track_t *t)
{
    uint32_t w = 0;
    for (uint32_t i = 0; i < t->range_count; i++) {
        const pb_range_t *r = &t->ranges[i];
        if (pb_loc_cmp(r->g1, r->o1, t->next_expected_group,
                       t->next_expected_object) < 0)
            continue;
        t->ranges[w++] = *r;
    }
    t->range_count = w;
}

/* The range of `kind` holding (g, o), or NULL. */
static const pb_range_t *know_find(const pb_track_t *t, uint32_t kind,
                                   uint64_t g, uint64_t o)
{
    for (uint32_t i = 0; i < t->range_count; i++) {
        const pb_range_t *r = &t->ranges[i];
        if (r->kind == kind && pb_loc_cmp(r->g0, r->o0, g, o) <= 0 &&
            pb_loc_cmp(g, o, r->g1, r->o1) <= 0)
            return r;
    }
    return NULL;
}

static void know_insert_sorted(pb_track_t *t, const pb_range_t *r)
{
    uint32_t i = t->range_count;
    while (i > 0 && pb_loc_cmp(t->ranges[i - 1].g0, t->ranges[i - 1].o0,
                               r->g0, r->o0) > 0) {
        t->ranges[i] = t->ranges[i - 1];
        i--;
    }
    t->ranges[i] = *r;
    t->range_count++;
}

/* Distance from the end of `a` to the start of `b` (b starts after a's
 * start), as a group count then an object count; 0 when they overlap. */
static void pb_range_gap(const pb_range_t *a, const pb_range_t *b,
                         uint64_t *dg, uint64_t *dobj)
{
    if (pb_loc_cmp(b->g0, b->o0, a->g1, a->o1) <= 0) {
        *dg = 0;
        *dobj = 0;
        return;
    }
    *dg = b->g0 - a->g1;
    *dobj = *dg ? b->o0 : b->o0 - a->o1;
}

static moq_result_t push_forgotten(moq_playback_t *pb, moq_playback_track_t h,
                                   const pb_range_t *r)
{
    moq_playback_event_t evt;
    memset(&evt, 0, sizeof(evt));
    evt.struct_size = sizeof(evt);
    evt.kind = MOQ_PLAYBACK_EVENT_KNOWLEDGE_FORGOTTEN;
    evt.track = h;
    evt.u.knowledge_forgotten.first_group_id = r->g0;
    evt.u.knowledge_forgotten.last_group_id = r->g1;
    return push_evt(pb, &evt);
}

/* Record `nr` for track `t`. Touching ranges of the same kind (and, when
 * unresolved, the same cause) merge; absence never merges across a gap.
 * With the bound reached, the two closest ranges ahead of the cursor
 * coalesce into one unresolved range; each absence demoted that way is
 * evented. Everything is worked out first, so a refusal for event room
 * changes nothing. */
static moq_result_t know_add(moq_playback_t *pb, pb_track_t *t,
                             moq_playback_track_t h, pb_range_t nr)
{
    know_prune(t);
    if (pb_loc_cmp(nr.g1, nr.o1, t->next_expected_group,
                   t->next_expected_object) < 0)
        return MOQ_OK;
    if (pb_loc_cmp(nr.g0, nr.o0, t->next_expected_group,
                   t->next_expected_object) < 0) {
        nr.g0 = t->next_expected_group;
        nr.o0 = t->next_expected_object;
    }
    nr.mark = false;

    /* The ranges `nr` absorbs: marked, until nothing more touches it. */
    uint32_t absorbed = 0;
    for (bool grew = true; grew;) {
        grew = false;
        for (uint32_t i = 0; i < t->range_count; i++) {
            pb_range_t *r = &t->ranges[i];
            if (r->mark || r->kind != nr.kind ||
                (nr.kind == PB_KNOW_UNRESOLVED && r->cause != nr.cause))
                continue;
            bool touch = pb_loc_cmp(r->g0, r->o0, nr.g0, nr.o0) <= 0
                ? pb_range_touches(r, &nr) : pb_range_touches(&nr, r);
            if (!touch) continue;
            pb_range_union(&nr, r);
            r->mark = true;
            absorbed++;
            grew = true;
        }
    }

    /* With the bound reached, the closest pair in start order among the
     * unmarked ranges and `nr`. Indices name the unmarked ranges in order,
     * `nr` at position `at`. */
    uint32_t live = t->range_count - absorbed;
    bool full = live >= pb->cfg.max_known_ranges;
    pb_range_t x = nr, y = nr;
    if (full) {
        /* Walk the unmarked ranges and `nr` in start order. */
        const pb_range_t *prev = NULL;
        uint64_t best_g = UINT64_MAX, best_o = UINT64_MAX;
        bool placed = false, have = false;
        uint32_t i = 0;
        for (;;) {
            while (i < t->range_count && t->ranges[i].mark) i++;
            const pb_range_t *cur;
            if (!placed && (i == t->range_count ||
                            pb_loc_cmp(t->ranges[i].g0, t->ranges[i].o0,
                                       nr.g0, nr.o0) > 0)) {
                cur = &nr;
                placed = true;
            } else if (i < t->range_count) {
                cur = &t->ranges[i++];
            } else {
                break;
            }
            if (prev) {
                uint64_t dg, dobj;
                pb_range_gap(prev, cur, &dg, &dobj);
                if (!have || dg < best_g || (dg == best_g && dobj < best_o)) {
                    best_g = dg;
                    best_o = dobj;
                    x = *prev;
                    y = *cur;
                    have = true;
                }
            }
            prev = cur;
        }
        /* One event reports the whole coalesced range: every id in it is
         * now only unresolved, so whatever absence it held is forgotten. */
        uint32_t demoted = (x.kind == PB_KNOW_ABSENT) + (y.kind == PB_KNOW_ABSENT);
        if (demoted && pb->evt_count >= pb->cfg.max_events) {
            for (uint32_t i = 0; i < t->range_count; i++) t->ranges[i].mark = false;
            return MOQ_ERR_WOULD_BLOCK;
        }
        if (demoted) {
            pb_range_t span = x;
            pb_range_union(&span, &y);
            (void)push_forgotten(pb, h, &span);
        }
#ifdef MOQ_PLAYBACK_TESTING
        pb->absence_forgotten += demoted;
#endif
    }

    /* Commit: drop the absorbed ranges, then place `nr`, or with the bound
     * reached the coalesced pair. */
    uint32_t w = 0;
    for (uint32_t i = 0; i < t->range_count; i++)
        if (!t->ranges[i].mark) t->ranges[w++] = t->ranges[i];
    t->range_count = w;
    if (!full) {
        know_insert_sorted(t, &nr);
        return MOQ_OK;
    }
    pb_range_t merged = x;
    pb_range_union(&merged, &y);
    merged.kind = PB_KNOW_UNRESOLVED;
    merged.cause = x.kind == PB_KNOW_UNRESOLVED && y.kind == PB_KNOW_UNRESOLVED &&
                   x.cause == y.cause ? x.cause : MOQ_PLAYBACK_GAP_CAUSE_UNKNOWN;
    merged.mark = false;
    /* Remove the members of the pair that are stored (one or both; `nr`
     * never is), then place `nr` unless it was one of them, and the merge. */
    bool nr_in_pair = false;
    const pb_range_t *pair[2] = { &x, &y };
    for (int k = 0; k < 2; k++) {
        const pb_range_t *m = pair[k];
        if (m->kind == nr.kind && m->cause == nr.cause &&
            m->g0 == nr.g0 && m->o0 == nr.o0 && m->g1 == nr.g1 && m->o1 == nr.o1) {
            nr_in_pair = true;
            continue;
        }
        for (uint32_t i = 0; i < t->range_count; i++) {
            const pb_range_t *r = &t->ranges[i];
            if (r->kind == m->kind && r->g0 == m->g0 && r->o0 == m->o0 &&
                r->g1 == m->g1 && r->o1 == m->o1) {
                memmove(&t->ranges[i], &t->ranges[i + 1],
                        (t->range_count - i - 1) * sizeof(pb_range_t));
                t->range_count--;
                break;
            }
        }
    }
    if (!nr_in_pair) know_insert_sorted(t, &nr);
    know_insert_sorted(t, &merged);
    return MOQ_OK;
}

_Static_assert(sizeof(pb_range_t) % _Alignof(pb_evidence_t) == 0,
               "the evidence table follows the range table aligned");

/* A stream fact reaching (g, o) at `now_us` is evidence for the wait on a
 * missing id at or before it: delivery got that far by then. A subgroup's
 * stream speaks for the rest of its group, so its facts reach the group's
 * end. Facts behind the cursor are dropped. A fact another dominates
 * (reaching at least as far, no later) adds nothing; one that dominates
 * others replaces them. With the bound reached, the entry whose time is
 * closest to its successor's gives way: the ids it covered then count from
 * the successor's later time, so the wait can lengthen but never start
 * before its evidence came. */
static void note_evidence(moq_playback_t *pb, pb_track_t *t,
                          uint64_t g, uint64_t o, uint64_t now_us)
{
    uint32_t w = 0;
    for (uint32_t i = 0; i < t->evidence_count; i++) {
        const pb_evidence_t *e = &t->evidence[i];
        if (pb_loc_cmp(e->g, e->o, t->next_expected_group,
                       t->next_expected_object) < 0)
            continue;
        t->evidence[w++] = *e;
    }
    t->evidence_count = w;
    if (pb_loc_cmp(g, o, t->next_expected_group, t->next_expected_object) < 0)
        return;
    for (uint32_t i = 0; i < t->evidence_count; i++) {
        const pb_evidence_t *e = &t->evidence[i];
        if (pb_loc_cmp(e->g, e->o, g, o) >= 0 && e->us <= now_us) return;
    }
    w = 0;
    for (uint32_t i = 0; i < t->evidence_count; i++) {
        const pb_evidence_t *e = &t->evidence[i];
        if (pb_loc_cmp(e->g, e->o, g, o) <= 0 && e->us >= now_us) continue;
        t->evidence[w++] = *e;
    }
    t->evidence_count = w;
    if (t->evidence_count == pb->cfg.max_known_ranges) {
        uint32_t drop = 0;
        uint64_t best = UINT64_MAX;
        for (uint32_t i = 0; i + 1 < t->evidence_count; i++) {
            uint64_t d = t->evidence[i + 1].us - t->evidence[i].us;
            if (d < best) { best = d; drop = i; }
        }
        if (t->evidence_count == 1 ||
            pb_loc_cmp(g, o, t->evidence[t->evidence_count - 1].g,
                       t->evidence[t->evidence_count - 1].o) > 0) {
            /* The new fact reaches furthest and comes last: it is the
             * successor of the last entry. */
            uint64_t d = now_us - t->evidence[t->evidence_count - 1].us;
            if (t->evidence_count == 1 || d < best)
                drop = t->evidence_count - 1;
        }
        memmove(&t->evidence[drop], &t->evidence[drop + 1],
                (t->evidence_count - drop - 1) * sizeof(pb_evidence_t));
        t->evidence_count--;
    }
    uint32_t at = t->evidence_count;
    while (at > 0 && pb_loc_cmp(t->evidence[at - 1].g, t->evidence[at - 1].o,
                                g, o) > 0) {
        t->evidence[at] = t->evidence[at - 1];
        at--;
    }
    t->evidence[at] = (pb_evidence_t){ .g = g, .o = o, .us = now_us };
    t->evidence_count++;
}

/* When the earliest evidence of delivery past the cursor came: an object
 * of the track buffered past it, or a stream fact reaching it. False
 * when there is none, so there is nothing to time out on. */
static bool gap_evidence(const moq_playback_t *pb, const pb_track_t *t,
                         moq_playback_track_t h, uint64_t *out)
{
    bool found = false;
    uint64_t best = UINT64_MAX;
    for (size_t i = 0; i < pb->cfg.max_buffered_objects; i++) {
        const pb_object_t *o = &pb->objects[i];
        if (!o->occupied || o->track._opaque != h._opaque) continue;
        if (pb_loc_cmp(o->group_id, o->object_id, t->next_expected_group,
                       t->next_expected_object) <= 0)
            continue;
        if (!found || o->arrived_us < best) best = o->arrived_us;
        found = true;
    }
    /* Sorted by reach with rising times: the first entry reaching the
     * cursor came first. */
    for (uint32_t i = 0; i < t->evidence_count; i++) {
        const pb_evidence_t *e = &t->evidence[i];
        if (pb_loc_cmp(e->g, e->o, t->next_expected_group,
                       t->next_expected_object) < 0)
            continue;
        if (!found || e->us < best) best = e->us;
        found = true;
        break;
    }
    *out = best;
    return found;
}

moq_result_t moq_playback_on_stream_terminal(moq_playback_t *pb,
                                             moq_playback_track_t track,
                                             const moq_playback_stream_terminal_t *rec,
                                             uint64_t now_us)
{
    if (!pb || !rec) return MOQ_ERR_INVAL;
    uint32_t size;
    memcpy(&size, rec, sizeof(size));
    if (size < MOQ_PLAYBACK_STREAM_TERMINAL_V0_SIZE) return MOQ_ERR_INVAL;
    moq_playback_stream_terminal_t r;
    memcpy(&r, rec, sizeof(r));
    if (r.kind < MOQ_PLAYBACK_STREAM_FIN_END_OF_GROUP ||
        r.kind > MOQ_PLAYBACK_STREAM_OBJECT_REFUSED)
        return MOQ_ERR_INVAL;
    if (r.kind == MOQ_PLAYBACK_STREAM_OBJECT_REFUSED && r.object_id == UINT64_MAX)
        return MOQ_ERR_INVAL;
    if (r.kind == MOQ_PLAYBACK_STREAM_RECEIVER_DROP &&
        r.last_group_id < r.group_id)
        return MOQ_ERR_INVAL;
    uint32_t slot;
    if (!pb_resolve_handle(pb, track, &slot)) return MOQ_ERR_STALE_HANDLE;
    pb_track_t *t = &pb->tracks[slot];
    if (t->ended) return MOQ_OK;

    /* The knowledge the fact carries, and where it says delivery reached. */
    bool learn = false;
    pb_range_t nr;
    memset(&nr, 0, sizeof(nr));
    uint64_t eg = r.group_id, eo = UINT64_MAX;
    switch (r.kind) {
    case MOQ_PLAYBACK_STREAM_FIN_END_OF_GROUP:
        if (r.has_last_object) {
            if (r.last_object_id != UINT64_MAX) {
                learn = true;
                nr = (pb_range_t){ .kind = PB_KNOW_ABSENT, .g0 = r.group_id,
                    .o0 = r.last_object_id + 1, .g1 = r.group_id,
                    .o1 = UINT64_MAX };
            }
        }
        break;
    case MOQ_PLAYBACK_STREAM_FIN:
        break;
    case MOQ_PLAYBACK_STREAM_RESET: {
        uint64_t from = 0;
        bool any = true;
        if (r.has_cut_object) from = r.cut_object_id;
        else if (r.has_last_object) {
            if (r.last_object_id == UINT64_MAX) any = false;
            else from = r.last_object_id + 1;
        }
        if (any) {
            learn = true;
            nr = (pb_range_t){ .kind = PB_KNOW_UNRESOLVED,
                .cause = MOQ_PLAYBACK_GAP_CAUSE_STREAM_RESET, .g0 = r.group_id,
                .o0 = from, .g1 = r.group_id, .o1 = UINT64_MAX };
        }
        break;
    }
    case MOQ_PLAYBACK_STREAM_RECEIVER_DROP:
        eg = r.last_group_id;
        learn = true;
        nr = (pb_range_t){ .kind = PB_KNOW_UNRESOLVED,
            .cause = MOQ_PLAYBACK_GAP_CAUSE_RECEIVER_DROP, .g0 = r.group_id,
            .o0 = 0, .g1 = r.last_group_id, .o1 = UINT64_MAX };
        break;
    case MOQ_PLAYBACK_STREAM_OBJECT_REFUSED:
        eo = r.object_id;
        learn = true;
        nr = (pb_range_t){ .kind = PB_KNOW_UNRESOLVED,
            .cause = MOQ_PLAYBACK_GAP_CAUSE_LOCAL_REFUSAL, .g0 = r.group_id,
            .o0 = r.object_id, .g1 = r.group_id, .o1 = r.object_id };
        break;
    default: /* END_OF_TRACK */
        eg = UINT64_MAX;
        learn = true;
        nr = (pb_range_t){ .kind = PB_KNOW_ABSENT, .g0 = r.group_id,
            .o0 = r.object_id, .g1 = UINT64_MAX, .o1 = UINT64_MAX };
        break;
    }
    if (learn) {
        moq_result_t rc = know_add(pb, t, track, nr);
        if (rc != MOQ_OK) return rc;
    }
    note_evidence(pb, t, eg, eo, now_us);
    /* A committed refusal settles the request the same object's push left
     * waiting for room: nothing will be retried for it, so nothing may be
     * evicted for it. Any other request stays. */
    if (r.kind == MOQ_PLAYBACK_STREAM_OBJECT_REFUSED && pb->has_waiting_admission &&
        pb->wait_track == track._opaque && pb->wait_group_id == r.group_id &&
        pb->wait_object_id == r.object_id)
        pb->has_waiting_admission = false;
    return MOQ_OK;
}

/* The configuration `obj` decodes under, in release order: its own when it
 * has one (NULL when present but empty), else the one released media
 * established, else the track's catalog configuration. */
static moq_rcbuf_t *config_for(const pb_track_t *t, const pb_object_t *obj)
{
    if (obj && obj->config_present) return obj->config;
    if (t->rel_config_set) return t->rel_config;
    return t->codec_config ? t->codec_config : t->init_data;
}

static bool config_equal(const moq_rcbuf_t *a, const moq_rcbuf_t *b)
{
    size_t la = a ? moq_rcbuf_len(a) : 0, lb = b ? moq_rcbuf_len(b) : 0;
    if (la != lb) return false;
    return la == 0 || memcmp(moq_rcbuf_data(a), moq_rcbuf_data(b), la) == 0;
}

/* A CONFIGURE is due before `obj` when the decoder is unconfigured, or when
 * the configuration `obj` decodes under is not the decoder's. */
static bool configure_due(const pb_track_t *t, const pb_object_t *obj)
{
    if (!t->configured || !t->cur_config_set) return true;
    return !config_equal(config_for(t, obj), t->cur_config);
}

static moq_result_t emit_configure(moq_playback_t *pb,
                                    pb_track_t *t,
                                    moq_playback_track_t handle,
                                    const pb_object_t *obj)
{
    moq_playback_cmd_t cfg_cmd;
    memset(&cfg_cmd, 0, sizeof(cfg_cmd));
    cfg_cmd.struct_size = sizeof(cfg_cmd);
    cfg_cmd.track = handle;

    moq_rcbuf_t *codec_ref = NULL;
    moq_rcbuf_t *config_ref = NULL;

    if (t->codec) {
        moq_rcbuf_incref(t->codec);
        codec_ref = t->codec;
    }
    moq_rcbuf_t *cc = config_for(t, obj);
    if (cc) {
        cfg_hold(pb, cc, true);
        config_ref = cc;
    }

    if (t->media_type == MOQ_PLAYBACK_MEDIA_AUDIO) {
        cfg_cmd.kind = MOQ_PLAYBACK_CMD_CONFIGURE_AUDIO;
        cfg_cmd.u.configure_audio.codec = codec_ref;
        cfg_cmd.u.configure_audio.codec_config = config_ref;
        cfg_cmd.u.configure_audio.samplerate = t->samplerate;
        cfg_cmd.u.configure_audio.channel_count = t->channel_count;
    } else {
        cfg_cmd.kind = MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO;
        cfg_cmd.u.configure_video.codec = codec_ref;
        cfg_cmd.u.configure_video.codec_config = config_ref;
        cfg_cmd.u.configure_video.width = t->width;
        cfg_cmd.u.configure_video.height = t->height;
    }

    moq_result_t rc = push_cmd(pb, &cfg_cmd);
    if (rc != MOQ_OK) {
        moq_rcbuf_decref(codec_ref);
        cfg_unhold(pb, config_ref, true, true);
        return rc;
    }
    cfg_set_slot(pb, &t->cur_config, &t->cur_config_set, cc);
    t->configured = true;
    return MOQ_OK;
}

static bool pb_scale_checked(uint64_t ticks, uint32_t ts, uint64_t *out) {
    if (ts == 0) return false;
    uint64_t sec = ticks / ts;
    uint64_t rem = ticks % ts;
    if (sec > UINT64_MAX / 1000000u) return false;
    uint64_t us_sec = sec * 1000000u;
    uint64_t us_rem = rem * 1000000u / ts;
    if (us_sec > UINT64_MAX - us_rem) return false;
    *out = us_sec + us_rem;
    return true;
}

static bool cmaf_sample_is_kf(uint32_t flags) {
    if (flags & 0x00010000u) return false;
    uint32_t dep = (flags >> 24) & 0x3u;
    if (dep == 1) return false;
    if (dep == 2) return true;
    return (flags == 0);
}

/* Validate every sample's timing of a multi-sample CMAF object, without
 * emitting anything. */
static moq_result_t preflight_samples(const pb_track_t *t,
                                      const pb_object_t *obj)
{
    /* Sample spans were checked against the media range at admission;
     * this validates the timing arithmetic. */
    uint64_t check_dts = obj->base_ticks;
    for (size_t si = 0; si < obj->sample_count; si++) {
        const moq_cmaf_sample_t *s = &obj->samples[si];
        uint64_t dts_us;
        if (!pb_scale_checked(check_dts, t->timescale, &dts_us))
            return MOQ_ERR_INVAL;
        bool neg = s->composition_offset < 0;
        uint32_t abs_co = neg ? (uint32_t)(-(int64_t)s->composition_offset)
                              : (uint32_t)s->composition_offset;
        uint64_t abs_us;
        if (!pb_scale_checked(abs_co, t->timescale, &abs_us))
            return MOQ_ERR_INVAL;
        if (abs_us > (uint64_t)INT64_MAX) return MOQ_ERR_INVAL;
        int64_t comp = neg ? -(int64_t)abs_us : (int64_t)abs_us;
        if (comp >= 0) {
            if (dts_us > UINT64_MAX - (uint64_t)comp) return MOQ_ERR_INVAL;
        } else {
            if ((uint64_t)(-comp) > dts_us) return MOQ_ERR_INVAL;
        }
        uint64_t dur64;
        if (!pb_scale_checked(s->duration, t->timescale, &dur64))
            return MOQ_ERR_INVAL;
        if (dur64 > UINT32_MAX) return MOQ_ERR_INVAL;
        if (check_dts > UINT64_MAX - s->duration) return MOQ_ERR_INVAL;
        check_dts += s->duration;
    }
    return MOQ_OK;
}

/* Emit decode commands for `obj`, starting at its sample cursor and
 * stopping when the command ring is full: one command for a RAW or
 * single-sample object, one per sample otherwise. Each sample's decode
 * time is converted from its own tick position, never accumulated in
 * rounded microseconds. Advances obj->sample_cursor and sets *out_last_dts
 * to the last emitted decode time and *out_last_end to where that unit
 * ends (its decode time plus duration). The caller has checked that at least
 * one command slot is free and, for a multi-sample object starting at
 * sample 0, that preflight_samples accepted it. */
static moq_result_t emit_decode(moq_playback_t *pb,
                                 pb_track_t *t,
                                 moq_playback_track_t handle,
                                 pb_object_t *obj,
                                 uint64_t *out_last_dts,
                                 uint64_t *out_last_end)
{
    bool is_cmaf = (t->packaging == MOQ_PLAYBACK_PACKAGING_CMAF);
    bool is_audio = (t->media_type == MOQ_PLAYBACK_MEDIA_AUDIO);

    if (!(is_cmaf && obj->sample_count > 1)) {
        moq_playback_cmd_t dec_cmd;
        memset(&dec_cmd, 0, sizeof(dec_cmd));
        dec_cmd.struct_size = sizeof(dec_cmd);
        dec_cmd.track = handle;

        moq_rcbuf_incref(obj->payload);

        if (is_audio) {
            dec_cmd.kind = MOQ_PLAYBACK_CMD_DECODE_AUDIO;
            dec_cmd.u.decode_audio.group_id = obj->group_id;
            dec_cmd.u.decode_audio.object_id = obj->object_id;
            dec_cmd.u.decode_audio.decode_time_us = obj->decode_time_us;
            dec_cmd.u.decode_audio.composition_offset_us = obj->composition_offset_us;
            dec_cmd.u.decode_audio.presentation_time_us = obj->presentation_time_us;
            dec_cmd.u.decode_audio.has_capture_time = obj->has_capture_time;
            dec_cmd.u.decode_audio.capture_time_us = obj->capture_time_us;
            dec_cmd.u.decode_audio.sample_duration_us = obj->sample_duration_us;
            dec_cmd.u.decode_audio.payload = obj->payload;
            dec_cmd.u.decode_audio.mdat_offset = obj->mdat_offset;
            dec_cmd.u.decode_audio.mdat_len = obj->mdat_len;
        } else if (is_cmaf) {
            dec_cmd.kind = MOQ_PLAYBACK_CMD_DECODE_CMAF;
            dec_cmd.u.decode_cmaf.group_id = obj->group_id;
            dec_cmd.u.decode_cmaf.object_id = obj->object_id;
            dec_cmd.u.decode_cmaf.decode_time_us = obj->decode_time_us;
            dec_cmd.u.decode_cmaf.composition_offset_us = obj->composition_offset_us;
            dec_cmd.u.decode_cmaf.presentation_time_us = obj->presentation_time_us;
            dec_cmd.u.decode_cmaf.has_capture_time = obj->has_capture_time;
            dec_cmd.u.decode_cmaf.capture_time_us = obj->capture_time_us;
            dec_cmd.u.decode_cmaf.keyframe = obj->keyframe;
            dec_cmd.u.decode_cmaf.sample_duration_us = obj->sample_duration_us;
            dec_cmd.u.decode_cmaf.fragment = obj->payload;
            dec_cmd.u.decode_cmaf.mdat_offset = obj->mdat_offset;
            dec_cmd.u.decode_cmaf.mdat_len = obj->mdat_len;
        } else {
            dec_cmd.kind = MOQ_PLAYBACK_CMD_DECODE_VIDEO;
            dec_cmd.u.decode_video.group_id = obj->group_id;
            dec_cmd.u.decode_video.object_id = obj->object_id;
            dec_cmd.u.decode_video.decode_time_us = obj->decode_time_us;
            dec_cmd.u.decode_video.composition_offset_us = obj->composition_offset_us;
            dec_cmd.u.decode_video.presentation_time_us = obj->presentation_time_us;
            dec_cmd.u.decode_video.has_capture_time = obj->has_capture_time;
            dec_cmd.u.decode_video.capture_time_us = obj->capture_time_us;
            dec_cmd.u.decode_video.keyframe = obj->keyframe;
            dec_cmd.u.decode_video.payload = obj->payload;
        }
        push_cmd(pb, &dec_cmd);
        obj->sample_cursor = 1;
        *out_last_dts = obj->decode_time_us;
        *out_last_end = obj->decode_time_us > UINT64_MAX - obj->sample_duration_us
            ? UINT64_MAX : obj->decode_time_us + obj->sample_duration_us;
        return MOQ_OK;
    }

    /* Multi-sample CMAF: the owned table and base tick from admission
     * (preflight already validated every sample's math). */
    size_t mdat_cursor = obj->mdat_offset;
    uint64_t dts_ticks = obj->base_ticks;
    for (size_t si = 0; si < obj->sample_cursor; si++) {
        mdat_cursor += obj->samples[si].size;
        dts_ticks += obj->samples[si].duration;
    }

    uint64_t last_sample_dts = 0, last_sample_end = 0;
    size_t si = obj->sample_cursor;
    for (; si < obj->sample_count && pb->cmd_count < pb->cfg.max_commands;
         si++) {
        const moq_cmaf_sample_t *s = &obj->samples[si];
        uint64_t dts_us;
        if (!pb_scale_checked(dts_ticks, t->timescale, &dts_us))
            return MOQ_ERR_INVAL;

        bool neg = s->composition_offset < 0;
        uint32_t abs_co = neg ? (uint32_t)(-(int64_t)s->composition_offset)
                              : (uint32_t)s->composition_offset;
        uint64_t abs_us;
        if (!pb_scale_checked(abs_co, t->timescale, &abs_us))
            return MOQ_ERR_INVAL;
        int64_t comp_us = neg ? -(int64_t)abs_us : (int64_t)abs_us;

        uint64_t pts_us = (comp_us >= 0)
            ? dts_us + (uint64_t)comp_us
            : dts_us - (uint64_t)(-comp_us);

        uint64_t dur64;
        if (!pb_scale_checked(s->duration, t->timescale, &dur64))
            return MOQ_ERR_INVAL;
        uint32_t dur_us = (uint32_t)dur64;

        moq_playback_cmd_t dec_cmd;
        memset(&dec_cmd, 0, sizeof(dec_cmd));
        dec_cmd.struct_size = sizeof(dec_cmd);
        dec_cmd.track = handle;
        moq_rcbuf_incref(obj->payload);

        if (is_audio) {
            dec_cmd.kind = MOQ_PLAYBACK_CMD_DECODE_AUDIO;
            dec_cmd.u.decode_audio.group_id = obj->group_id;
            dec_cmd.u.decode_audio.object_id = obj->object_id;
            dec_cmd.u.decode_audio.decode_time_us = dts_us;
            dec_cmd.u.decode_audio.composition_offset_us = comp_us;
            dec_cmd.u.decode_audio.presentation_time_us = pts_us;
            dec_cmd.u.decode_audio.has_capture_time = obj->has_capture_time;
            dec_cmd.u.decode_audio.capture_time_us = obj->capture_time_us;
            dec_cmd.u.decode_audio.sample_duration_us = dur_us;
            dec_cmd.u.decode_audio.payload = obj->payload;
            dec_cmd.u.decode_audio.mdat_offset = mdat_cursor;
            dec_cmd.u.decode_audio.mdat_len = s->size;
        } else {
            dec_cmd.kind = MOQ_PLAYBACK_CMD_DECODE_CMAF;
            dec_cmd.u.decode_cmaf.group_id = obj->group_id;
            dec_cmd.u.decode_cmaf.object_id = obj->object_id;
            dec_cmd.u.decode_cmaf.decode_time_us = dts_us;
            dec_cmd.u.decode_cmaf.composition_offset_us = comp_us;
            dec_cmd.u.decode_cmaf.presentation_time_us = pts_us;
            dec_cmd.u.decode_cmaf.has_capture_time = obj->has_capture_time;
            dec_cmd.u.decode_cmaf.capture_time_us = obj->capture_time_us;
            dec_cmd.u.decode_cmaf.keyframe = cmaf_sample_is_kf(s->flags);
            dec_cmd.u.decode_cmaf.sample_duration_us = dur_us;
            dec_cmd.u.decode_cmaf.fragment = obj->payload;
            dec_cmd.u.decode_cmaf.mdat_offset = mdat_cursor;
            dec_cmd.u.decode_cmaf.mdat_len = s->size;
        }

        push_cmd(pb, &dec_cmd);
        last_sample_dts = dts_us;
        last_sample_end = dts_us > UINT64_MAX - dur_us ? UINT64_MAX : dts_us + dur_us;
        mdat_cursor += s->size;
        dts_ticks += s->duration;
    }

    obj->sample_cursor = si;
    *out_last_dts = last_sample_dts;
    *out_last_end = last_sample_end;
    return MOQ_OK;
}

/* Release starts with the first action that commits at the cursor; tick
 * sets `started` after that action's mutation. From then on, objects still
 * buffered below the cursor can never be played: each is dropped, a NORMAL
 * object with a STALE event. A full event queue stops the cleanup with
 * WOULD_BLOCK; each dropped object already has its event, so a later tick
 * resumes. */
static moq_result_t drop_below_cursor(moq_playback_t *pb, pb_track_t *t,
                                      moq_playback_track_t handle)
{
    for (size_t i = 0; i < pb->cfg.max_buffered_objects; i++) {
        pb_object_t *o = &pb->objects[i];
        if (!o->occupied || o->track._opaque != handle._opaque ||
            !loc_before(o->group_id, o->object_id,
                        t->next_expected_group, t->next_expected_object))
            continue;
        if (o->status == MOQ_OBJECT_NORMAL) {
            moq_result_t rc = push_drop_event(pb, handle,
                MOQ_PLAYBACK_DROP_STALE, o->group_id, o->object_id);
            if (rc != MOQ_OK) return rc;
        }
        release_buffered_object(pb, o);
    }
    t->entry_cleanup_pending = false;
    return MOQ_OK;
}

/* The copy the waiting note counted on reusing, while it is still that
 * registration; NULL once it is gone or cannot be told apart. */
static const pb_copy_t *wait_reuse_copy(const moq_playback_t *pb)
{
    if (pb->wait_reuse_serial == 0 || pb->wait_reuse_slot >= pb->copy_cap) return NULL;
    const pb_copy_t *c = &pb->copies[pb->wait_reuse_slot];
    return c->buf && c->serial == pb->wait_reuse_serial ? c : NULL;
}

/* Make room for the object a push last refused for capacity, from the
 * objects of tracks that have no entry yet. Those objects wait for an entry
 * that may never come; a track with an entry keeps its decode sequence and
 * is never dropped from. Nothing is dropped unless dropping every eligible
 * object would make room: an object that can never fit costs nothing.
 * Victims come from the track holding the most waiting objects, lowest
 * location first, each dropped KEYFRAME_WAIT with an event naming its own
 * track. A full event queue stops with WOULD_BLOCK; each drop already had
 * its event, so the next tick resumes. The drops are local buffer policy:
 * they record nothing about the objects' existence. */
static moq_result_t relieve_admission_pressure(moq_playback_t *pb)
{
    if (!pb->has_waiting_admission) return MOQ_OK;

    /* The note must still describe a pending request: its full handle
     * resolves to a live track that has not ended, and the location is
     * neither buffered already nor below a committed cursor (a retry there
     * would be absorbed or dropped STALE, needing no payload room). */
    moq_playback_track_t wt = { pb->wait_track };
    uint32_t wslot;
    if (!pb_resolve_handle(pb, wt, &wslot) || pb->tracks[wslot].ended ||
        find_object(pb, wt, pb->wait_group_id, pb->wait_object_id) ||
        (pb->tracks[wslot].started &&
         loc_before(pb->wait_group_id, pb->wait_object_id,
                    pb->tracks[wslot].next_expected_group,
                    pb->tracks[wslot].next_expected_object))) {
        pb->has_waiting_admission = false;
        return MOQ_OK;
    }
    uint64_t need = pb->wait_bytes;

    size_t elig_count = 0;
    uint64_t elig_bytes = 0;
    for (size_t i = 0; i < pb->cfg.max_buffered_objects; i++) {
        const pb_object_t *o = &pb->objects[i];
        uint32_t slot;
        if (!o->occupied || !pb_resolve_handle(pb, o->track, &slot) ||
            pb->tracks[slot].anchored)
            continue;
        elig_count++;
        elig_bytes += o->charge;
    }
    /* A configuration copy comes back too when every holder it has is an
     * eligible object: counted once, however many of them share it. */
    for (size_t i = 0; i < pb->copy_cap; i++) {
        const pb_copy_t *c = &pb->copies[i];
        if (!c->buf) continue;
        uint32_t victims = 0;
        for (size_t j = 0; j < pb->cfg.max_buffered_objects; j++) {
            const pb_object_t *o = &pb->objects[j];
            uint32_t slot;
            if (o->occupied && o->config == c->buf &&
                pb_resolve_handle(pb, o->track, &slot) &&
                !pb->tracks[slot].anchored)
                victims++;
        }
        if (victims == c->holders)
            elig_bytes += moq_rcbuf_len(c->buf);
    }
    /* The retry needs the reused copy's bytes again once it is gone, also
     * when dropping every eligible object would free it: its bytes then
     * count among what relief frees and as much among what the retry needs.
     * A copy whose identity cannot be told (no serial) counts as gone. */
    uint64_t need_all = need;
    if (pb->wait_reuse_len > 0) {
        const pb_copy_t *rc = wait_reuse_copy(pb);
        bool lost = !rc;
        if (rc) {
            uint32_t victims = 0;
            for (size_t j = 0; j < pb->cfg.max_buffered_objects; j++) {
                const pb_object_t *o = &pb->objects[j];
                uint32_t slot;
                if (o->occupied && o->config == rc->buf &&
                    pb_resolve_handle(pb, o->track, &slot) &&
                    !pb->tracks[slot].anchored)
                    victims++;
            }
            lost = victims == rc->holders;
        }
        if (lost) {
            if (pb->wait_reuse_len > UINT64_MAX - need_all) return MOQ_OK;
            need_all += pb->wait_reuse_len;
        }
    }
    size_t need_count = pb->obj_count >= pb->cfg.max_buffered_objects
        ? pb->obj_count + 1 - pb->cfg.max_buffered_objects : 0;
    /* Bytes to free. For an object larger than the whole budget this
     * exceeds everything buffered, so the check below refuses it. */
    uint64_t room = pb->cfg.max_buffered_bytes - pb->retained_bytes;
    uint64_t need_bytes = need_all > room ? need_all - room : 0;
    if (elig_count < need_count || elig_bytes < need_bytes)
        return MOQ_OK;                       /* cannot make room this way */

    /* Victims are dropped until the retry fits as priced now: with the
     * reused copy's bytes once a drop has freed it. Dropping every eligible
     * object makes room (checked above), so the loop ends there at the
     * latest; a smaller set that would have fit by sparing the copy is not
     * searched, so the relief made is enough, not necessarily the least. */
    for (;;) {
        uint64_t now_need = need;
        if (pb->wait_reuse_len > 0 && !wait_reuse_copy(pb)) {
            if (pb->wait_reuse_len > UINT64_MAX - need) return MOQ_OK;
            now_need = need + pb->wait_reuse_len;
        }
        if (pb->obj_count < pb->cfg.max_buffered_objects &&
            now_need <= pb->cfg.max_buffered_bytes - pb->retained_bytes)
            break;
        /* The eligible track with the most buffered objects. */
        uint32_t best = UINT32_MAX;
        size_t best_n = 0;
        for (uint32_t ti = 0; ti < pb->cfg.max_tracks; ti++) {
            const pb_track_t *t = &pb->tracks[ti];
            if (!t->active || t->anchored) continue;
            moq_playback_track_t h =
                pb_encode_handle(pb->creation_tag, t->generation, ti);
            size_t n = 0;
            for (size_t i = 0; i < pb->cfg.max_buffered_objects; i++)
                if (pb->objects[i].occupied &&
                    pb->objects[i].track._opaque == h._opaque)
                    n++;
            if (n > best_n) { best_n = n; best = ti; }
        }
        if (best == UINT32_MAX) return MOQ_OK;
        moq_playback_track_t h = pb_encode_handle(pb->creation_tag,
            pb->tracks[best].generation, best);
        pb_object_t *victim = NULL;
        for (size_t i = 0; i < pb->cfg.max_buffered_objects; i++) {
            pb_object_t *o = &pb->objects[i];
            if (!o->occupied || o->track._opaque != h._opaque) continue;
            if (!victim || loc_before(o->group_id, o->object_id,
                                      victim->group_id, victim->object_id))
                victim = o;
        }
        if (victim->status == MOQ_OBJECT_NORMAL) {
            moq_result_t rc = push_drop_event(pb, h,
                MOQ_PLAYBACK_DROP_KEYFRAME_WAIT,
                victim->group_id, victim->object_id);
            if (rc != MOQ_OK) return rc;
        }
        release_buffered_object(pb, victim);
    }
    return MOQ_OK;
}

/* Report TRACK_ENDED and settle the track: nothing it holds is released. */
static moq_result_t end_track(moq_playback_t *pb, pb_track_t *t,
                              moq_playback_track_t handle)
{
    moq_playback_event_t evt;
    memset(&evt, 0, sizeof(evt));
    evt.struct_size = sizeof(evt);
    evt.kind = MOQ_PLAYBACK_EVENT_TRACK_ENDED;
    evt.track = handle;
    moq_result_t rc = push_evt(pb, &evt);
    if (rc != MOQ_OK) return rc;
    release_track_objects(pb, handle);
    t->ended = true;
    t->started = true;
    t->gap_waiting = false;
    return MOQ_OK;
}

/* Drop every object a track without an entry holds, each media object with
 * its KEYFRAME_WAIT event; a full event queue stops with WOULD_BLOCK, each
 * drop already reported. */
static moq_result_t drop_unanchored(moq_playback_t *pb,
                                    moq_playback_track_t handle)
{
    for (size_t i = 0; i < pb->cfg.max_buffered_objects; i++) {
        pb_object_t *o = &pb->objects[i];
        if (!o->occupied || o->track._opaque != handle._opaque) continue;
        if (o->status == MOQ_OBJECT_NORMAL) {
            moq_result_t rc = push_drop_event(pb, handle,
                MOQ_PLAYBACK_DROP_KEYFRAME_WAIT, o->group_id, o->object_id);
            if (rc != MOQ_OK) return rc;
        }
        release_buffered_object(pb, o);
    }
    return MOQ_OK;
}

moq_result_t moq_playback_tick(moq_playback_t *pb, uint64_t now_us)
{
    if (!pb) return MOQ_ERR_INVAL;

    uint32_t released = 0;

    for (uint32_t ti = 0; ti < pb->cfg.max_tracks; ti++) {
        pb_track_t *t = &pb->tracks[ti];
        if (!t->active || t->ended || t->paused_by_pressure) continue;

        moq_playback_track_t handle =
            pb_encode_handle(pb->creation_tag, t->generation, ti);

        /* A drained track without an entry can never release what it
         * holds: each object is dropped with its event, then it ends. */
        if (!t->anchored && t->draining) {
            moq_result_t rc = drop_unanchored(pb, handle);
            if (rc != MOQ_OK) return rc;
            rc = end_track(pb, t, handle);
            if (rc != MOQ_OK) return rc;
            continue;
        }

        /* A video track holding objects but no entry yet reports that it
         * waits for a keyframe, once; nothing is released or dropped. */
        if (!t->anchored) {
            if (t->media_type == MOQ_PLAYBACK_MEDIA_VIDEO &&
                !t->kf_wait_event_emitted &&
                has_buffered_objects(pb, handle)) {
                moq_playback_event_t evt;
                memset(&evt, 0, sizeof(evt));
                evt.struct_size = sizeof(evt);
                evt.kind = MOQ_PLAYBACK_EVENT_KEYFRAME_WAITING;
                evt.track = handle;
                moq_result_t rc = push_evt(pb, &evt);
                if (rc != MOQ_OK) return rc;
                t->kf_wait_event_emitted = true;
            }
            continue;
        }

        while (pb->cfg.max_release_per_tick == 0 ||
               released < pb->cfg.max_release_per_tick) {

            if (t->started && t->entry_cleanup_pending) {
                moq_result_t cr = drop_below_cursor(pb, t, handle);
                if (cr != MOQ_OK) return cr;
            }

            pb_object_t *obj = find_object(pb, handle,
                t->next_expected_group, t->next_expected_object);

            /* -- Missing expected object -------------------------------- */
            if (!obj) {
                /* Known absent: nothing to wait for and nothing missed, so
                 * the cursor moves past without a decoder reset. Absence to
                 * the end of the track ends it. */
                const pb_range_t *absent = know_find(t, PB_KNOW_ABSENT,
                    t->next_expected_group, t->next_expected_object);
                uint64_t ng, no;
                if (absent && !pb_loc_next(absent->g1, absent->o1, &ng, &no)) {
                    moq_playback_event_t evt;
                    memset(&evt, 0, sizeof(evt));
                    evt.struct_size = sizeof(evt);
                    evt.kind = MOQ_PLAYBACK_EVENT_TRACK_ENDED;
                    evt.track = handle;
                    moq_result_t rc = push_evt(pb, &evt);
                    if (rc != MOQ_OK) return rc;
                    release_track_objects(pb, handle);
                    t->ended = true;
                    t->started = true;
                    break;
                }
                if (absent) {
                    t->next_expected_group = ng;
                    t->next_expected_object = no;
                    t->gap_waiting = false;
                    t->started = true;
                    /* Objects inside the absence contradict it; they now
                     * sit below the cursor and are dropped stale. */
                    t->entry_cleanup_pending = true;
                    continue;
                }

                /* A drained track ends once nothing is left past the
                 * cursor. */
                if (t->draining && !next_present(pb, handle,
                        t->next_expected_group, t->next_expected_object)) {
                    moq_result_t rc = end_track(pb, t, handle);
                    if (rc != MOQ_OK) return rc;
                    break;
                }

                /* Closed-group partial abandon (precedence over timer). */
                if (group_is_closed(pb, handle, t->next_expected_group)) {
                    moq_playback_event_t evt;
                    memset(&evt, 0, sizeof(evt));
                    evt.struct_size = sizeof(evt);
                    evt.kind = MOQ_PLAYBACK_EVENT_PARTIAL_GROUP_ABANDONED;
                    evt.track = handle;
                    evt.u.partial_group_abandoned.from_group_id =
                        t->next_expected_group;
                    evt.u.partial_group_abandoned.to_group_id =
                        t->next_expected_group;

                    moq_result_t rc = push_evt(pb, &evt);
                    if (rc != MOQ_OK) return rc;

                    drop_group_objects(pb, handle, t->next_expected_group);
                    uint64_t ag = t->next_expected_group;
                    t->next_expected_group++;
                    t->next_expected_object = 0;
                    enter_recovery(t, ag);
                    released++;
                    t->started = true;
                    continue;
                }

                /* Inferred group boundary: current group has no more
                 * buffered objects beyond the expected one. */
                if (pb->cfg.infer_end_of_group_from_next_group &&
                    !has_later_object_in_group(pb, handle,
                        t->next_expected_group, t->next_expected_object))
                {
                    /* If the next group is already buffered, advance. */
                    if (has_objects_in_group(pb, handle,
                            t->next_expected_group + 1)) {
                        t->next_expected_group++;
                        t->next_expected_object = 0;
                        t->gap_waiting = false;
                        released++;
                        t->started = true;
                        continue;
                    }
                    /* No later groups buffered at all — wait without
                     * starting the gap timer. If a sparse future group
                     * IS buffered, fall through to backlog/gap. */
                    uint64_t hi = highest_buffered_group(pb, handle);
                    if (hi <= t->next_expected_group) {
                        t->gap_waiting = false;
                        break;
                    }
                }

                /* Backlog shedding: only when blocked on missing object. */
                if (pb->cfg.max_backlog_groups > 0) {
                    uint64_t hi = highest_buffered_group(pb, handle);
                    if (hi > t->next_expected_group &&
                        hi - t->next_expected_group >
                            pb->cfg.max_backlog_groups) {
                        uint64_t keep_from =
                            hi - pb->cfg.max_backlog_groups;
                        uint64_t raw_dropped =
                            keep_from - t->next_expected_group;
                        uint32_t dropped = (raw_dropped > UINT32_MAX)
                            ? UINT32_MAX : (uint32_t)raw_dropped;
                        uint32_t remaining = pb->cfg.max_backlog_groups;


                        moq_playback_event_t shed;
                        memset(&shed, 0, sizeof(shed));
                        shed.struct_size = sizeof(shed);
                        shed.kind = MOQ_PLAYBACK_EVENT_BACKLOG_SHED;
                        shed.track = handle;
                        shed.u.backlog_shed.dropped_groups = dropped;
                        shed.u.backlog_shed.remaining_groups = remaining;

                        moq_result_t rc = push_evt(pb, &shed);
                        if (rc != MOQ_OK) return rc;

                        drop_groups_range(pb, handle,
                            t->next_expected_group, keep_from - 1);
                        enter_recovery(t, keep_from - 1);
                        t->next_expected_group = keep_from;
                        t->next_expected_object = 0;
                        released++;
                        t->started = true;
                        continue;
                    }
                }

                /* The wait: gap_timeout_us from the first evidence that
                 * delivery moved past the missing id (a later object, or a
                 * stream fact at or past it). Without such evidence there
                 * is nothing to time out on yet. */
                if (!t->gap_waiting ||
                    t->gap_group != t->next_expected_group ||
                    t->gap_object != t->next_expected_object) {
                    uint64_t since;
                    if (!gap_evidence(pb, t, handle, &since)) {
                        t->gap_waiting = false;
                        break;
                    }
                    t->gap_waiting = true;
                    t->gap_reported = false;
                    t->gap_group = t->next_expected_group;
                    t->gap_object = t->next_expected_object;
                    t->gap_started_us = since;
                }

                uint64_t elapsed = (now_us >= t->gap_started_us)
                    ? now_us - t->gap_started_us : 0;
                if (elapsed < pb->cfg.gap_timeout_us && !t->settling)
                    break;

                /* The wait is over. GAP_DETECTED commits first; the step it
                 * announces commits when its own event has room, on this
                 * tick or a later one, without a second report. */
                if (!t->gap_reported) {
                    if (pb->evt_count >= pb->cfg.max_events)
                        return MOQ_ERR_WOULD_BLOCK;
                    const pb_range_t *unres = know_find(t, PB_KNOW_UNRESOLVED,
                        t->next_expected_group, t->next_expected_object);
                    moq_playback_event_t gap_evt;
                    memset(&gap_evt, 0, sizeof(gap_evt));
                    gap_evt.struct_size = sizeof(gap_evt);
                    gap_evt.kind = MOQ_PLAYBACK_EVENT_GAP_DETECTED;
                    gap_evt.track = handle;
                    gap_evt.u.gap_detected.group_id = t->gap_group;
                    gap_evt.u.gap_detected.object_id = t->gap_object;
                    gap_evt.u.gap_detected.cause = unres ? unres->cause
                        : MOQ_PLAYBACK_GAP_CAUSE_UNKNOWN;
                    push_evt(pb, &gap_evt);
                    t->gap_reported = true;
                }

                pb_object_t *next = next_present(pb, handle,
                    t->next_expected_group, t->next_expected_object);

                /* CONTINUE: resume at the next unit with the decoder state
                 * kept, reporting the hole for concealment. */
                if (t->gap_policy == MOQ_PLAYBACK_GAP_CONTINUE && next) {
                    if (pb->evt_count >= pb->cfg.max_events)
                        return MOQ_ERR_WOULD_BLOCK;
                    moq_playback_event_t hole;
                    memset(&hole, 0, sizeof(hole));
                    hole.struct_size = sizeof(hole);
                    hole.kind = MOQ_PLAYBACK_EVENT_GAP_CONTINUED;
                    hole.track = handle;
                    uint64_t from = t->has_last_end ? t->last_end_us
                                                    : next->decode_time_us;
                    hole.u.gap_continued.hole_start_us = from;
                    hole.u.gap_continued.hole_duration_us =
                        next->decode_time_us > from ? next->decode_time_us - from : 0;
                    push_evt(pb, &hole);
                    t->next_expected_group = next->group_id;
                    t->next_expected_object = next->object_id;
                    t->gap_waiting = false;
                    t->gap_reported = false;
                    t->started = true;
                    continue;
                }

                /* DISCONTINUITY inside a group: the next present unit of the
                 * group follows the hole only when it is independent, behind
                 * a decoder reset. */
                if (t->gap_policy == MOQ_PLAYBACK_GAP_DISCONTINUITY && next &&
                    next->group_id == t->next_expected_group && next->keyframe) {
                    if (pb->evt_count >= pb->cfg.max_events)
                        return MOQ_ERR_WOULD_BLOCK;
                    moq_playback_event_t skip;
                    memset(&skip, 0, sizeof(skip));
                    skip.struct_size = sizeof(skip);
                    skip.kind = MOQ_PLAYBACK_EVENT_GAP_SKIPPED_IN_GROUP;
                    skip.track = handle;
                    skip.u.gap_skipped_in_group.group_id = next->group_id;
                    skip.u.gap_skipped_in_group.from_object_id = t->next_expected_object;
                    skip.u.gap_skipped_in_group.to_object_id = next->object_id;
                    push_evt(pb, &skip);
                    t->needs_reset = true;
                    t->next_expected_object = next->object_id;
                    t->gap_waiting = false;
                    t->gap_reported = false;
                    t->started = true;
                    continue;
                }

                if (pb->evt_count >= pb->cfg.max_events)
                    return MOQ_ERR_WOULD_BLOCK;
                t->gap_reported = false;

                moq_playback_event_t pa_evt;
                memset(&pa_evt, 0, sizeof(pa_evt));
                pa_evt.struct_size = sizeof(pa_evt);
                pa_evt.kind = MOQ_PLAYBACK_EVENT_PARTIAL_GROUP_ABANDONED;
                pa_evt.track = handle;
                pa_evt.u.partial_group_abandoned.from_group_id =
                    t->next_expected_group;
                pa_evt.u.partial_group_abandoned.to_group_id =
                    t->next_expected_group;
                push_evt(pb, &pa_evt);

                drop_group_objects(pb, handle, t->next_expected_group);
                uint64_t ag = t->next_expected_group;
                t->next_expected_group++;
                t->next_expected_object = 0;
                enter_recovery(t, ag);
                released++;
                t->started = true;
                continue;
            }

            /* Expected object found. */
            t->gap_waiting = false;
            t->gap_reported = false;

            bool is_cmaf_ms = (t->packaging == MOQ_PLAYBACK_PACKAGING_CMAF
                               && obj->sample_count > 1);

            /* A multi-sample object part-way out of the command ring resumes
             * where it stopped: its gating and checks ran at sample 0. */
            if (obj->sample_cursor > 0) {
                if (pb->cmd_count >= pb->cfg.max_commands)
                    return MOQ_ERR_WOULD_BLOCK;
                goto emit_samples;
            }

            /* -- END_OF_TRACK ------------------------------------------- */
            if (obj->status == MOQ_OBJECT_END_OF_TRACK) {
                moq_playback_event_t evt;
                memset(&evt, 0, sizeof(evt));
                evt.struct_size = sizeof(evt);
                evt.kind = MOQ_PLAYBACK_EVENT_TRACK_ENDED;
                evt.track = handle;

                moq_result_t rc = push_evt(pb, &evt);
                if (rc != MOQ_OK) return rc;

                release_track_objects(pb, handle);
                t->ended = true;
                released++;
                t->started = true;
                break;
            }

            /* -- END_OF_GROUP marker ------------------------------------ */
            if (obj->status == MOQ_OBJECT_END_OF_GROUP) {
                release_buffered_object(pb, obj);
                advance_past_closed_group(pb, t, handle);
                released++;
                t->started = true;
                continue;
            }

            /* -- Pending RESET after gap -------------------------------- */
            if (t->needs_reset) {
                moq_playback_cmd_t rst;
                memset(&rst, 0, sizeof(rst));
                rst.struct_size = sizeof(rst);
                rst.kind = MOQ_PLAYBACK_CMD_RESET;
                rst.track = handle;
                rst.u.reset.reason = MOQ_PLAYBACK_RESET_GAP;

                moq_result_t rc = push_cmd(pb, &rst);
                if (rc != MOQ_OK) return rc;

                t->needs_reset = false;
                t->configured = false;
                t->has_last_dts = false;
            }

            /* -- Keyframe gating ---------------------------------------- */
            if (t->waiting_for_keyframe) {
                if (!obj->keyframe) {
                    if (!t->kf_wait_event_emitted) {
                        moq_playback_event_t evt;
                        memset(&evt, 0, sizeof(evt));
                        evt.struct_size = sizeof(evt);
                        evt.kind = MOQ_PLAYBACK_EVENT_KEYFRAME_WAITING;
                        evt.track = handle;

                        moq_result_t rc = push_evt(pb, &evt);
                        if (rc != MOQ_OK) return rc;
                        t->kf_wait_event_emitted = true;
                    }

                    moq_result_t rc = push_drop_event(pb, handle,
                        MOQ_PLAYBACK_DROP_KEYFRAME_WAIT,
                        obj->group_id, obj->object_id);
                    if (rc != MOQ_OK) return rc;

                    bool advance_group = obj_closes_group(pb, obj);
                    release_buffered_object(pb, obj);
                    if (advance_group) {
                        advance_past_closed_group(pb, t, handle);
                    } else {
                        t->next_expected_object++;
                    }
                    released++;
                    t->started = true;
                    continue;
                }

                if (t->has_skip_pending) {
                    moq_playback_event_t evt;
                    memset(&evt, 0, sizeof(evt));
                    evt.struct_size = sizeof(evt);
                    evt.kind = MOQ_PLAYBACK_EVENT_SKIP_FORWARD;
                    evt.track = handle;
                    evt.u.skip_forward.from_group_id = t->abandoned_group;
                    evt.u.skip_forward.to_group_id = obj->group_id;

                    moq_result_t rc = push_evt(pb, &evt);
                    if (rc != MOQ_OK) return rc;
                    t->has_skip_pending = false;
                }

                t->waiting_for_keyframe = false;
                t->kf_wait_event_emitted = false;
            }

            /* -- Non-monotonic DTS check -------------------------------- */
            /* CMAF carries a decode clock (tfdt/trun), so one going
             * backwards within a timeline is malformed. A RAW (LOC) object
             * carries only its presentation timestamp, and its decode order
             * is object order: frame reordering lowers that timestamp
             * without anything being wrong. */
            if (t->packaging == MOQ_PLAYBACK_PACKAGING_CMAF &&
                t->has_last_dts && obj->decode_time_us < t->last_dts) {
                moq_result_t rc = push_drop_event(pb, handle,
                    MOQ_PLAYBACK_DROP_NON_MONOTONIC_DTS,
                    obj->group_id, obj->object_id);
                if (rc != MOQ_OK) return rc;

                bool advance_group = obj_closes_group(pb, obj);
                release_buffered_object(pb, obj);
                if (advance_group) {
                    advance_past_closed_group(pb, t, handle);
                } else {
                    t->next_expected_object++;
                }
                released++;
                t->started = true;
                continue;
            }

            /* -- Multi-sample preflight before any emission --------------- */
            if (is_cmaf_ms) {
                moq_result_t pf = preflight_samples(t, obj);
                if (pf != MOQ_OK) {
                    moq_result_t dr = push_drop_event(pb, handle,
                        MOQ_PLAYBACK_DROP_MALFORMED_CMAF,
                        obj->group_id, obj->object_id);
                    if (dr != MOQ_OK) return dr;
                    bool ag = obj_closes_group(pb, obj);
                    release_buffered_object(pb, obj);
                    if (ag) {
                        advance_past_closed_group(pb, t, handle);
                    } else {
                        t->next_expected_object++;
                    }
                    released++;
                    t->started = true;
                    continue;
                }
            }

            /* -- CONFIGURE, committed on its own when due ---------------- */
            /* It configures the track's decoder and consumes no object, so
             * it does not start release: an earlier entry may still move
             * the cursor back. */
            if (configure_due(t, obj)) {
                moq_result_t rc = emit_configure(pb, t, handle, obj);
                if (rc != MOQ_OK) return rc;
            }
            if (pb->cmd_count >= pb->cfg.max_commands)
                return MOQ_ERR_WOULD_BLOCK;

        emit_samples:;
            bool first_unit = obj->sample_cursor == 0;
            uint64_t emitted_last_dts = 0, emitted_last_end = 0;
            size_t cmds_before = pb->cmd_count;
            moq_result_t rc = emit_decode(pb, t, handle, obj,
                                           &emitted_last_dts, &emitted_last_end);
            /* Every sample command of a multi-sample object is marked, the
             * last included: a hold could not say which bytes it keeps. */
            if (is_cmaf_ms)
                for (size_t k = cmds_before; k < pb->cmd_count; k++)
                    pb->cmd_multi[(pb->cmd_head + k) % pb->cfg.max_commands] = 1;
            if (rc != MOQ_OK) return rc;
            /* Released media now runs under this object's configuration. */
            if (first_unit)
                cfg_set_slot(pb, &t->rel_config, &t->rel_config_set,
                             config_for(t, obj));

            t->has_last_dts = true;
            t->last_dts = emitted_last_dts;
            t->has_last_end = true;
            t->last_end_us = emitted_last_end;

            /* A multi-sample object stays at the cursor until its last
             * sample has left through the command ring. */
            if (is_cmaf_ms && obj->sample_cursor < obj->sample_count) {
                released++;
                t->started = true;
                return MOQ_ERR_WOULD_BLOCK;
            }

            bool advance_group = obj_closes_group(pb, obj);
            release_into_command(pb, obj);

            if (advance_group) {
                advance_past_closed_group(pb, t, handle);
            } else {
                t->next_expected_object++;
            }
            released++;
            t->started = true;
        }
    }

    /* Room for a refused push, once release had its chance to free it. */
    return relieve_admission_pressure(pb);
}

/* -- Polling --------------------------------------------------------- */

moq_result_t moq_playback_poll_command(moq_playback_t *pb,
                                        moq_playback_cmd_t *out,
                                        size_t out_size)
{
    if (!pb || !out) return MOQ_ERR_INVAL;
    if (out_size < MOQ_PLAYBACK_CMD_V0_SIZE) return MOQ_ERR_ABI_MISMATCH;
    if (pb->cmd_count == 0) return MOQ_DONE;

    size_t ri = pb->cmd_head % pb->cfg.max_commands;
    moq_playback_cmd_t *src = &pb->cmd_ring[ri];
    /* The command's configuration ref moves to the caller: the engine
     * stops holding it. Its bytes leave the budget with it. */
    cfg_unhold(pb, cmd_config(src), true, false);
    cmd_uncharge(pb, ri);
    pb->cmd_multi[ri] = 0;
    size_t n = out_size < sizeof(*src) ? out_size : sizeof(*src);
    memcpy(out, src, n);
    out->struct_size = (uint32_t)n;
    memset(src, 0, sizeof(*src));
    pb->cmd_head++;
    pb->cmd_count--;
    return MOQ_OK;
}

/* The payload a decode command carries, or NULL. */
static moq_rcbuf_t *cmd_payload(const moq_playback_cmd_t *c)
{
    switch (c->kind) {
    case MOQ_PLAYBACK_CMD_DECODE_VIDEO: return c->u.decode_video.payload;
    case MOQ_PLAYBACK_CMD_DECODE_AUDIO: return c->u.decode_audio.payload;
    case MOQ_PLAYBACK_CMD_DECODE_CMAF:  return c->u.decode_cmaf.fragment;
    default:                            return NULL;
    }
}

/* The handle ticket `t` names, for slot `slot`. */
static uint64_t hold_handle(uint64_t t, uint32_t slot)
{
    uint64_t q = t - 1;
    uint16_t tag = (uint16_t)((q >> PB_HOLD_GEN_BITS) + 1);
    uint32_t gen = (uint32_t)(((q & ((UINT64_C(1) << PB_HOLD_GEN_BITS) - 1)) << 1) | 1u);
    return moq_handle_pack(MOQ_HANDLE_POOL_PLAYBACK_COMMAND_HOLD, tag, gen, slot);
}

/* The active hold `h` names in `pb`, or NULL: the slot it locates must be
 * holding exactly that issued handle. */
static pb_hold_t *hold_resolve(const moq_playback_t *pb, moq_playback_command_hold_t h)
{
    if (h._opaque == 0) return NULL;
    if (moq_handle_pool_tag(h._opaque) != MOQ_HANDLE_POOL_PLAYBACK_COMMAND_HOLD) return NULL;
    uint32_t slot = moq_handle_slot(h._opaque);
    if (slot >= pb->cfg.max_held_commands) return NULL;
    pb_hold_t *e = &pb->holds[slot];
    if (!e->active || e->handle != h._opaque) return NULL;
    return e;
}

moq_result_t moq_playback_poll_command_held(moq_playback_t *pb,
                                             moq_playback_cmd_t *out,
                                             size_t out_size,
                                             moq_playback_command_hold_t *hold)
{
    if (!pb || !out || !hold) return MOQ_ERR_INVAL;
    if (out_size < MOQ_PLAYBACK_CMD_V0_SIZE) return MOQ_ERR_ABI_MISMATCH;
    if (pb->cfg.max_held_commands == 0) return MOQ_ERR_UNSUPPORTED;
    if (pb->cmd_count == 0) return MOQ_DONE;
    size_t ri = pb->cmd_head % pb->cfg.max_commands;
    if (pb->cmd_multi[ri]) return MOQ_ERR_UNSUPPORTED;
    if (pb->hold_active >= pb->cfg.max_held_commands) return MOQ_ERR_WOULD_BLOCK;
    uint64_t ticket = pb_issue_ticket(pb->issuer);
    if (ticket == 0) return MOQ_ERR_LIMIT;

    uint32_t slot = pb->hold_free[--pb->hold_free_count];
    pb_hold_t *e = &pb->holds[slot];
    const moq_playback_cmd_t *src = &pb->cmd_ring[ri];
    /* The hold becomes a holder of the command's copy before the command
     * stops being one, so the copy stays charged throughout. */
    moq_rcbuf_t *config = cmd_config(src);
    pb_copy_t *copy = copy_find(pb, config);
    if (copy) {
        cfg_hold(pb, config, false);
        copy->hold_holders++;
        e->config = config;
    }
    moq_rcbuf_t *payload = cmd_payload(src);
    if (payload) moq_rcbuf_incref(payload);
    e->payload = payload;
    e->bytes = pb->cmd_charge[ri];

    moq_result_t rc = moq_playback_poll_command(pb, out, out_size);
    (void)rc;   /* the checks above leave only success */
    /* What the poll uncharged stays charged, now for the hold. */
    pb->retained_bytes += e->bytes;
    pb->hold_bytes += e->bytes;
    e->handle = hold_handle(ticket, slot);
    e->active = true;
    pb->hold_active++;
    hold->_opaque = e->handle;
    return MOQ_OK;
}

void moq_playback_release_command_hold(moq_playback_t *pb,
                                        moq_playback_command_hold_t hold)
{
    if (!pb) return;
    pb_hold_t *e = hold_resolve(pb, hold);
    if (!e) return;
    pb->retained_bytes -= e->bytes;
    pb->hold_bytes -= e->bytes;
    if (e->config) {
        pb_copy_t *copy = copy_find(pb, e->config);
        if (copy) copy->hold_holders--;
        cfg_unhold(pb, e->config, false, true);
    }
    if (e->payload) moq_rcbuf_decref(e->payload);
    e->config = NULL;
    e->payload = NULL;
    e->bytes = 0;
    e->handle = 0;
    e->active = false;
    pb->hold_active--;
    pb->hold_free[pb->hold_free_count++] = (uint32_t)(e - pb->holds);
}

uint64_t moq_playback_command_hold_bytes(const moq_playback_t *pb)
{
    if (!pb) return 0;
    uint64_t n = pb->hold_bytes;
    for (size_t i = 0; i < pb->copy_cap; i++) {
        const pb_copy_t *c = &pb->copies[i];
        if (c->buf && c->hold_holders) n += moq_rcbuf_len(c->buf);
    }
    return n;
}

bool moq_playback_command_hold_is_valid(moq_playback_command_hold_t h)
{
    if (h._opaque == 0) return false;
    if (moq_handle_pool_tag(h._opaque) != MOQ_HANDLE_POOL_PLAYBACK_COMMAND_HOLD) return false;
    if (moq_handle_session_tag(h._opaque) == 0) return false;
    return (moq_handle_generation(h._opaque) & 1u) != 0;
}

bool moq_playback_command_hold_eq(moq_playback_command_hold_t a,
                                  moq_playback_command_hold_t b)
{
    return a._opaque == b._opaque;
}

uint64_t moq_playback_command_hold_hash(moq_playback_command_hold_t h)
{
    return h._opaque * 0x9E3779B97F4A7C15ULL;
}

uint64_t moq_playback_command_hold_id_for_trace(moq_playback_command_hold_t h)
{
    return h._opaque;
}

moq_result_t moq_playback_poll_event(moq_playback_t *pb,
                                      moq_playback_event_t *out,
                                      size_t out_size)
{
    if (!pb || !out) return MOQ_ERR_INVAL;
    if (out_size < MOQ_PLAYBACK_EVENT_V0_SIZE) return MOQ_ERR_ABI_MISMATCH;
    if (pb->evt_count == 0) return MOQ_DONE;

    const moq_playback_event_t *src =
        &pb->evt_ring[pb->evt_head % pb->cfg.max_events];
    size_t n = out_size < sizeof(*src) ? out_size : sizeof(*src);
    memcpy(out, src, n);
    out->struct_size = (uint32_t)n;
    pb->evt_head++;
    pb->evt_count--;
    return MOQ_OK;
}

uint64_t moq_playback_next_deadline_us(const moq_playback_t *pb)
{
    if (!pb) return UINT64_MAX;
    uint64_t best = UINT64_MAX;
    for (uint32_t i = 0; i < pb->cfg.max_tracks; i++) {
        const pb_track_t *t = &pb->tracks[i];
        if (!t->active || !t->anchored || t->ended ||
            t->paused_by_pressure || !t->gap_waiting)
            continue;
        if (t->gap_started_us > UINT64_MAX - pb->cfg.gap_timeout_us)
            continue;
        uint64_t due = t->gap_started_us + pb->cfg.gap_timeout_us;
        if (due < best) best = due;
    }
    return best;
}

/* -- Subscriber convenience ------------------------------------------ */

moq_result_t moq_playback_push_sub_object(
    moq_playback_t *pb,
    moq_playback_track_t track,
    const moq_sub_object_t *src,
    uint64_t now_us)
{
    if (!pb || !src) return MOQ_ERR_INVAL;

    moq_playback_object_t po;
    moq_playback_object_init(&po);
    po.track             = track;
    po.group_id          = src->group_id;
    po.subgroup_id       = src->subgroup_id;
    po.object_id         = src->object_id;
    po.publisher_priority = src->publisher_priority;
    po.status            = src->status;
    po.end_of_group      = src->end_of_group;
    po.datagram          = src->datagram;
    po.payload           = src->payload;
    po.properties        = src->properties;

    return moq_playback_push_object(pb, &po, now_us);
}

/* -- Feedback -------------------------------------------------------- */

moq_result_t moq_playback_handle_feedback(moq_playback_t *pb,
                                           const moq_playback_feedback_t *fb,
                                           uint64_t now_us)
{
    if (!pb || !fb) return MOQ_ERR_INVAL;
    if (fb->struct_size < MOQ_PLAYBACK_FEEDBACK_V0_SIZE) return MOQ_ERR_INVAL;
    (void)now_us;

    uint32_t slot;
    if (!pb_resolve_handle(pb, fb->track, &slot))
        return MOQ_ERR_STALE_HANDLE;
    pb_track_t *t = &pb->tracks[slot];

    switch (fb->kind) {
    case MOQ_PLAYBACK_FEEDBACK_QUEUE_PRESSURE:
        t->paused_by_pressure =
            (fb->u.queue_pressure.depth > fb->u.queue_pressure.max_recommended);
        return MOQ_OK;

    case MOQ_PLAYBACK_FEEDBACK_DECODE_ERROR: {
        bool is_video = (t->media_type == MOQ_PLAYBACK_MEDIA_VIDEO);

        if (is_video && pb->evt_count >= pb->cfg.max_events)
            return MOQ_ERR_WOULD_BLOCK;

        size_t purge_count = 0;
        for (size_t i = 0; i < pb->cmd_count; i++) {
            size_t ri = (pb->cmd_head + i) % pb->cfg.max_commands;
            if (pb->cmd_ring[ri].track._opaque == fb->track._opaque)
                purge_count++;
        }
        if (pb->cmd_count - purge_count >= pb->cfg.max_commands)
            return MOQ_ERR_WOULD_BLOCK;

        {
            size_t write = 0;
            for (size_t i = 0; i < pb->cmd_count; i++) {
                size_t ri = (pb->cmd_head + i) % pb->cfg.max_commands;
                moq_playback_cmd_t *c = &pb->cmd_ring[ri];
                if (c->track._opaque == fb->track._opaque) {
                    cfg_unhold(pb, cmd_config(c), true, false);
                    cmd_release_refs(c);
                    cmd_uncharge(pb, ri);
                    pb->cmd_multi[ri] = 0;
                    memset(c, 0, sizeof(*c));
                } else {
                    if (write != i) {
                        size_t wi = (pb->cmd_head + write) %
                                    pb->cfg.max_commands;
                        pb->cmd_ring[wi] = *c;
                        pb->cmd_charge[wi] = pb->cmd_charge[ri];
                        pb->cmd_charge[ri] = 0;
                        pb->cmd_multi[wi] = pb->cmd_multi[ri];
                        pb->cmd_multi[ri] = 0;
                        memset(c, 0, sizeof(*c));
                    }
                    write++;
                }
            }
            pb->cmd_count = write;
        }

        /* The decoder lost its state: an object part-way out of the
         * command ring starts again from its first sample, behind the
         * reset and keyframe gating. */
        for (size_t i = 0; i < pb->cfg.max_buffered_objects; i++)
            if (pb->objects[i].occupied &&
                pb->objects[i].track._opaque == fb->track._opaque)
                pb->objects[i].sample_cursor = 0;

        moq_playback_cmd_t rst;
        memset(&rst, 0, sizeof(rst));
        rst.struct_size = sizeof(rst);
        rst.kind = MOQ_PLAYBACK_CMD_RESET;
        rst.track = fb->track;
        rst.u.reset.reason = MOQ_PLAYBACK_RESET_DECODE_ERROR;
        push_cmd(pb, &rst);

        if (is_video) {
            moq_playback_event_t evt;
            memset(&evt, 0, sizeof(evt));
            evt.struct_size = sizeof(evt);
            evt.kind = MOQ_PLAYBACK_EVENT_KEYFRAME_WAITING;
            evt.track = fb->track;
            push_evt(pb, &evt);
        }

        t->waiting_for_keyframe = is_video;
        t->kf_wait_event_emitted = is_video;
        t->configured = false;
        t->has_last_dts = false;
        t->needs_reset = false;
        t->has_skip_pending = false;
        t->gap_waiting = false;
        return MOQ_OK;
    }

    default:
        return MOQ_ERR_INVAL;
    }
}

/* -- Test-only helpers ----------------------------------------------- */

#ifdef MOQ_PLAYBACK_TESTING

moq_result_t moq_playback_test_push_cmd(moq_playback_t *pb,
                                         const moq_playback_cmd_t *cmd)
{
    if (!pb || !cmd) return MOQ_ERR_INVAL;
    return push_cmd(pb, cmd);
}

moq_result_t moq_playback_test_push_event(moq_playback_t *pb,
                                           const moq_playback_event_t *evt)
{
    if (!pb || !evt) return MOQ_ERR_INVAL;
    return push_evt(pb, evt);
}

uint64_t moq_playback_test_fragment_reads(const moq_playback_t *pb)
{
    return pb ? pb->fragment_reads : 0;
}

uint64_t moq_playback_test_retained_bytes(const moq_playback_t *pb)
{
    return pb ? pb->retained_bytes : 0;
}

size_t moq_playback_test_buffered_count(const moq_playback_t *pb)
{
    return pb ? pb->obj_count : 0;
}

void moq_playback_test_set_copy_serial(moq_playback_t *pb, uint64_t serial)
{
    if (pb) pb->copy_serial = serial;
}

bool moq_playback_test_track_anchored(const moq_playback_t *pb,
                                       moq_playback_track_t h)
{
    uint32_t slot;
    if (!pb || !pb_resolve_handle(pb, h, &slot)) return false;
    return pb->tracks[slot].anchored;
}

uint64_t moq_playback_test_anchor_group(const moq_playback_t *pb,
                                         moq_playback_track_t h)
{
    uint32_t slot;
    if (!pb || !pb_resolve_handle(pb, h, &slot)) return 0;
    return pb->tracks[slot].next_expected_group;
}

uint64_t moq_playback_test_anchor_object(const moq_playback_t *pb,
                                          moq_playback_track_t h)
{
    uint32_t slot;
    if (!pb || !pb_resolve_handle(pb, h, &slot)) return 0;
    return pb->tracks[slot].next_expected_object;
}

moq_rcbuf_t *moq_playback_test_track_codec(const moq_playback_t *pb,
                                            moq_playback_track_t h)
{
    uint32_t slot;
    if (!pb || !pb_resolve_handle(pb, h, &slot)) return NULL;
    return pb->tracks[slot].codec;
}

moq_rcbuf_t *moq_playback_test_track_init_data(const moq_playback_t *pb,
                                                moq_playback_track_t h)
{
    uint32_t slot;
    if (!pb || !pb_resolve_handle(pb, h, &slot)) return NULL;
    return pb->tracks[slot].init_data;
}

uint16_t moq_playback_test_creation_tag(const moq_playback_t *pb)
{
    return pb ? pb->creation_tag : 0;
}

uint32_t moq_playback_test_generation_seed(const moq_playback_t *pb)
{
    return pb ? pb->generation_seed : 0;
}

bool moq_playback_test_gap_waiting(const moq_playback_t *pb,
                                    moq_playback_track_t h)
{
    uint32_t slot;
    if (!pb || !pb_resolve_handle(pb, h, &slot)) return false;
    return pb->tracks[slot].gap_waiting;
}

/* Skip `n` engine creation epochs, to reach the creation tag's cycle
 * without creating tens of thousands of engines. Returns the last epoch
 * used: the next engine's is one more. */
uint64_t moq_playback_test_skip_epochs(uint64_t n);
uint64_t moq_playback_test_skip_epochs(uint64_t n)
{
    return atomic_fetch_add_explicit(&pb_epoch_counter, n, memory_order_relaxed) + n;
}

/* Give `pb` a private issuer with `remaining` tickets left before its
 * last, numbered from the library issuer's final range, to reach
 * exhaustion without billions of holds and without spending the
 * library's issuer. */
void moq_playback_test_local_issuer(moq_playback_t *pb, uint64_t remaining);
void moq_playback_test_local_issuer(moq_playback_t *pb, uint64_t remaining)
{
    if (!pb) return;
    if (remaining > PB_HOLD_TICKETS) remaining = PB_HOLD_TICKETS;
    atomic_store_explicit(&pb->local_issuer.issued, PB_HOLD_TICKETS - remaining,
                          memory_order_relaxed);
    pb->local_issuer.limit = PB_HOLD_TICKETS;
    pb->issuer = &pb->local_issuer;
}

/* One more hold for `pb` before its issuer is spent: the first call gives
 * it a private issuer with nothing left; each call adds one. (The slot and
 * generation are not used: identities come from the issuer.) */
void moq_playback_test_set_hold_generation(moq_playback_t *pb, uint32_t slot,
                                           uint32_t gen);
void moq_playback_test_set_hold_generation(moq_playback_t *pb, uint32_t slot,
                                           uint32_t gen)
{
    (void)slot;
    (void)gen;
    if (!pb) return;
    if (pb->issuer != &pb->local_issuer) moq_playback_test_local_issuer(pb, 0);
    uint64_t issued = atomic_load_explicit(&pb->local_issuer.issued, memory_order_relaxed);
    if (issued > 0)
        atomic_store_explicit(&pb->local_issuer.issued, issued - 1, memory_order_relaxed);
}

uint32_t moq_playback_test_known_ranges(const moq_playback_t *pb,
                                        moq_playback_track_t h);
uint32_t moq_playback_test_known_ranges(const moq_playback_t *pb,
                                        moq_playback_track_t h)
{
    uint32_t slot;
    if (!pb || !pb_resolve_handle(pb, h, &slot)) return 0;
    return pb->tracks[slot].range_count;
}

uint64_t moq_playback_test_absence_forgotten(const moq_playback_t *pb);
uint64_t moq_playback_test_absence_forgotten(const moq_playback_t *pb)
{
    return pb ? pb->absence_forgotten : 0;
}

#endif
