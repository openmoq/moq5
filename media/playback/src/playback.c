#include <moq/playback.h>
#include <moq/subscriber.h>
#include <moq/media_object.h>
#include <moq/cmaf.h>
#include <moq/rcbuf.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

/* -- Monotonic creation epoch ---------------------------------------- */

static uint64_t pb_next_epoch(void)
{
    static _Atomic uint64_t counter = 0;
    return atomic_fetch_add_explicit(&counter, 1, memory_order_relaxed) + 1;
}

/* -- Internal types -------------------------------------------------- */

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
} pb_track_t;

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
} pb_object_t;

struct moq_playback {
    moq_alloc_t              alloc;
    moq_playback_cfg_t       cfg;
    uint16_t                 creation_tag;
    uint32_t                 generation_seed;

    moq_playback_cmd_t      *cmd_ring;
    size_t                   cmd_head;
    size_t                   cmd_count;

    moq_playback_event_t    *evt_ring;
    size_t                   evt_head;
    size_t                   evt_count;

    pb_track_t              *tracks;
    uint32_t                 track_count;

    pb_object_t             *objects;
    size_t                   obj_count;
    uint64_t                 retained_bytes;

    moq_cmaf_sample_t       *sample_scratch;
};

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

void moq_playback_cfg_init(moq_playback_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = sizeof(*cfg);
    cfg->max_tracks = 4;
    cfg->max_buffered_objects = 256;
    cfg->max_buffered_bytes = 16u * 1024 * 1024;
    cfg->max_backlog_groups = 3;
    cfg->max_commands = 64;
    cfg->max_events = 16;
    cfg->gap_timeout_us = 500000;
    cfg->max_release_per_tick = 8;
    cfg->max_track_config_bytes = 4096;
    cfg->max_samples_per_object = 16;
}

void moq_playback_track_cfg_init(moq_playback_track_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = sizeof(*cfg);
}

void moq_playback_object_init(moq_playback_object_t *obj)
{
    if (!obj) return;
    memset(obj, 0, sizeof(*obj));
    obj->struct_size = sizeof(*obj);
}

void moq_playback_feedback_init(moq_playback_feedback_t *fb)
{
    if (!fb) return;
    memset(fb, 0, sizeof(*fb));
    fb->struct_size = sizeof(*fb);
}

/* -- Command cleanup ------------------------------------------------- */

void moq_playback_cmd_cleanup(moq_playback_cmd_t *cmd)
{
    if (!cmd) return;
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

/* -- Internal queue helpers ------------------------------------------ */

static moq_result_t push_cmd(moq_playback_t *pb,
                              const moq_playback_cmd_t *cmd)
{
    if (pb->cmd_count >= pb->cfg.max_commands)
        return MOQ_ERR_WOULD_BLOCK;
    size_t idx = (pb->cmd_head + pb->cmd_count) % pb->cfg.max_commands;
    pb->cmd_ring[idx] = *cmd;
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
}

/* -- Track helpers --------------------------------------------------- */

static void release_track_objects(moq_playback_t *pb,
                                   moq_playback_track_t handle)
{
    for (size_t i = 0; i < pb->cfg.max_buffered_objects; i++) {
        pb_object_t *o = &pb->objects[i];
        if (o->occupied && o->track._opaque == handle._opaque) {
            pb->retained_bytes -= o->payload_len;
            moq_rcbuf_decref(o->payload);
            memset(o, 0, sizeof(*o));
            pb->obj_count--;
        }
    }
}

static void release_track_config(pb_track_t *t)
{
    moq_rcbuf_decref(t->codec);
    moq_rcbuf_decref(t->init_data);
    moq_rcbuf_decref(t->codec_config);
    t->codec = NULL;
    t->init_data = NULL;
    t->codec_config = NULL;
}

/* -- Lifecycle ------------------------------------------------------- */

#define PB_CFG_MIN_SIZE \
    (offsetof(moq_playback_cfg_t, max_track_config_bytes) + \
     sizeof(((moq_playback_cfg_t *)0)->max_track_config_bytes))

moq_result_t moq_playback_create(const moq_alloc_t *alloc,
                                  const moq_playback_cfg_t *cfg,
                                  moq_playback_t **out)
{
    if (!alloc || !cfg || !out) return MOQ_ERR_INVAL;
    *out = NULL;

    if (!alloc->alloc || !alloc->free) return MOQ_ERR_INVAL;
    if (cfg->struct_size < PB_CFG_MIN_SIZE) return MOQ_ERR_INVAL;

    moq_playback_cfg_t c;
    memset(&c, 0, sizeof(c));
    size_t copy_sz = cfg->struct_size < sizeof(c) ? cfg->struct_size : sizeof(c);
    memcpy(&c, cfg, copy_sz);
    c.struct_size = sizeof(c);
    apply_defaults(&c);

    if (c.max_tracks > 0xFFFFu) return MOQ_ERR_INVAL;

    /* count*sizeof overflow guards, written as `sizeof > SIZE_MAX/count` so the
     * divided right-hand side is a runtime size_t. The equivalent
     * `(size_t)count > SIZE_MAX/sizeof(T)` form has a compile-time RHS that is
     * always-false for a uint32 count on 64-bit and trips gcc
     * -Werror=type-limits (the check still matters on 32-bit). */
    if (c.max_commands != 0 &&
        sizeof(moq_playback_cmd_t) > SIZE_MAX / (size_t)c.max_commands)
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

    size_t cmd_bytes = c.max_commands * sizeof(moq_playback_cmd_t);
    pb->cmd_ring = (moq_playback_cmd_t *)alloc->alloc(cmd_bytes, alloc->ctx);
    if (!pb->cmd_ring) goto fail_cmd;
    memset(pb->cmd_ring, 0, cmd_bytes);

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

    *out = pb;
    return MOQ_OK;

fail_samp:
    alloc->free(pb->objects, obj_bytes, alloc->ctx);
fail_obj:
    alloc->free(pb->tracks, trk_bytes, alloc->ctx);
fail_trk:
    alloc->free(pb->evt_ring, evt_bytes, alloc->ctx);
fail_evt:
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
            moq_rcbuf_decref(o->payload);
    }

    for (uint32_t i = 0; i < pb->cfg.max_tracks; i++) {
        if (pb->tracks[i].active)
            release_track_config(&pb->tracks[i]);
    }

    while (pb->cmd_count > 0) {
        moq_playback_cmd_t *cmd =
            &pb->cmd_ring[pb->cmd_head % pb->cfg.max_commands];
        moq_playback_cmd_cleanup(cmd);
        pb->cmd_head++;
        pb->cmd_count--;
    }

    moq_alloc_t a = pb->alloc;
    a.free(pb->sample_scratch,
        pb->cfg.max_samples_per_object * sizeof(moq_cmaf_sample_t), a.ctx);
    a.free(pb->objects,
        pb->cfg.max_buffered_objects * sizeof(pb_object_t), a.ctx);
    a.free(pb->tracks,
        pb->cfg.max_tracks * sizeof(pb_track_t), a.ctx);
    a.free(pb->evt_ring,
        pb->cfg.max_events * sizeof(moq_playback_event_t), a.ctx);
    a.free(pb->cmd_ring,
        pb->cfg.max_commands * sizeof(moq_playback_cmd_t), a.ctx);
    a.free(pb, sizeof(moq_playback_t), a.ctx);
}

/* -- Track management ------------------------------------------------ */

#define PB_TRACK_CFG_MIN_SIZE \
    (offsetof(moq_playback_track_cfg_t, is_live) + \
     sizeof(((moq_playback_track_cfg_t *)0)->is_live))

moq_result_t moq_playback_add_track(moq_playback_t *pb,
                                     const moq_playback_track_cfg_t *cfg,
                                     moq_playback_track_t *out)
{
    if (!pb || !cfg || !out) return MOQ_ERR_INVAL;
    *out = MOQ_PLAYBACK_TRACK_INVALID;

    if (cfg->struct_size < PB_TRACK_CFG_MIN_SIZE) return MOQ_ERR_INVAL;
    if (cfg->media_type != MOQ_PLAYBACK_MEDIA_VIDEO &&
        cfg->media_type != MOQ_PLAYBACK_MEDIA_AUDIO)
        return MOQ_ERR_INVAL;
    if (cfg->packaging != MOQ_PLAYBACK_PACKAGING_RAW &&
        cfg->packaging != MOQ_PLAYBACK_PACKAGING_CMAF)
        return MOQ_ERR_INVAL;

    if (cfg->init_data.len > 0 && !cfg->init_data.data)
        return MOQ_ERR_INVAL;
    if (cfg->codec.len > 0 && !cfg->codec.data)
        return MOQ_ERR_INVAL;
    if (cfg->codec.len > pb->cfg.max_track_config_bytes)
        return MOQ_ERR_INVAL;

    /* Enforce the documented per-track codec+init cap on the RAW init_data
     * for ALL packaging, BEFORE any parsing. For CMAF the init_data is the
     * init segment, so an untrusted oversized segment must be rejected here
     * rather than handed to moq_cmaf_parse_init() — otherwise the cap is
     * bypassed and parse work scales with the attacker-controlled length.
     * (codec.len <= max is checked just above, so the subtraction is safe.) */
    if (cfg->init_data.len >
        pb->cfg.max_track_config_bytes - cfg->codec.len)
        return MOQ_ERR_INVAL;

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
    uint32_t eff_timescale = cfg->timescale;
    uint32_t eff_width = cfg->width;
    uint32_t eff_height = cfg->height;
    uint32_t eff_samplerate = cfg->samplerate;
    uint32_t eff_channel_count = cfg->channel_count;

    if (cfg->packaging == MOQ_PLAYBACK_PACKAGING_CMAF &&
        cfg->init_data.len > 0) {
        moq_cmaf_init_info_t info;
        moq_cmaf_init_info_init(&info);
        moq_result_t ir = moq_cmaf_parse_init(cfg->init_data, &info);
        if (ir != MOQ_OK) return MOQ_ERR_PROTO;
        if (info.timescale != 0) eff_timescale = info.timescale;
        if (info.width != 0) eff_width = info.width;
        if (info.height != 0) eff_height = info.height;
        if (info.samplerate != 0) eff_samplerate = info.samplerate;
        if (info.channel_count != 0) eff_channel_count = info.channel_count;

        if (info.codec_config.len > 0) {
            if (info.codec_config.len >
                pb->cfg.max_track_config_bytes - cfg->codec.len)
                return MOQ_ERR_INVAL;
            moq_result_t rc = moq_rcbuf_create(&pb->alloc,
                info.codec_config.data, info.codec_config.len, &config_buf);
            if (rc != MOQ_OK) return rc;
        }
    }

    moq_rcbuf_t *codec_buf = NULL;
    if (cfg->codec.len > 0) {
        moq_result_t rc = moq_rcbuf_create(&pb->alloc,
            cfg->codec.data, cfg->codec.len, &codec_buf);
        if (rc != MOQ_OK) {
            moq_rcbuf_decref(config_buf);
            return rc;
        }
    }

    moq_rcbuf_t *init_buf = NULL;
    if (cfg->packaging != MOQ_PLAYBACK_PACKAGING_CMAF &&
        cfg->init_data.len > 0) {
        moq_result_t rc = moq_rcbuf_create(&pb->alloc,
            cfg->init_data.data, cfg->init_data.len, &init_buf);
        if (rc != MOQ_OK) {
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
    slot->media_type = cfg->media_type;
    slot->packaging = cfg->packaging;
    slot->codec = codec_buf;
    slot->init_data = init_buf;
    slot->codec_config = config_buf;
    slot->timescale = eff_timescale;
    slot->width = eff_width;
    slot->height = eff_height;
    slot->samplerate = eff_samplerate;
    slot->channel_count = eff_channel_count;
    slot->target_latency_us = cfg->target_latency_us;
    slot->is_live = cfg->is_live;
    slot->waiting_for_keyframe =
        (cfg->media_type == MOQ_PLAYBACK_MEDIA_VIDEO);

    pb->track_count++;
    *out = pb_encode_handle(pb->creation_tag, next_gen, slot_idx);
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
    release_track_config(&pb->tracks[slot]);
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

moq_result_t moq_playback_push_object(moq_playback_t *pb,
                                       const moq_playback_object_t *obj,
                                       uint64_t now_us)
{
    if (!pb || !obj) return MOQ_ERR_INVAL;
    if (obj->struct_size < sizeof(moq_playback_object_t))
        return MOQ_ERR_INVAL;

    uint32_t track_slot;
    if (!pb_resolve_handle(pb, obj->track, &track_slot))
        return MOQ_ERR_STALE_HANDLE;
    (void)now_us;

    if (obj->status != MOQ_OBJECT_NORMAL &&
        obj->status != MOQ_OBJECT_END_OF_GROUP &&
        obj->status != MOQ_OBJECT_END_OF_TRACK)
        return MOQ_ERR_INVAL;

    if (obj->status == MOQ_OBJECT_NORMAL && !obj->payload)
        return MOQ_ERR_INVAL;

    if (obj->status != MOQ_OBJECT_NORMAL && obj->payload)
        return MOQ_ERR_INVAL;

    pb_track_t *t = &pb->tracks[track_slot];
    if (t->ended)
        return MOQ_ERR_WRONG_STATE;

    for (size_t i = 0; i < pb->cfg.max_buffered_objects; i++) {
        pb_object_t *e = &pb->objects[i];
        if (e->occupied &&
            e->track._opaque == obj->track._opaque &&
            e->group_id == obj->group_id &&
            e->object_id == obj->object_id)
            return MOQ_OK;
    }

    moq_media_parsed_object_t parsed;
    memset(&parsed, 0, sizeof(parsed));

    if (obj->status == MOQ_OBJECT_NORMAL) {
        moq_media_track_info_t mti;
        moq_media_track_info_init(&mti);
        mti.media_type = (t->media_type == MOQ_PLAYBACK_MEDIA_AUDIO)
            ? MOQ_MEDIA_TYPE_AUDIO : MOQ_MEDIA_TYPE_VIDEO;
        mti.packaging = (t->packaging == MOQ_PLAYBACK_PACKAGING_CMAF)
            ? MOQ_MEDIA_PACKAGING_CMAF : MOQ_MEDIA_PACKAGING_RAW;
        mti.timescale = t->timescale;

        moq_media_object_input_t moi;
        moq_media_object_input_init(&moi);
        moi.group_id = obj->group_id;
        moi.object_id = obj->object_id;
        moi.status = obj->status;
        moi.end_of_group = obj->end_of_group;
        moi.datagram = obj->datagram;
        moi.payload = obj->payload;
        moi.properties = obj->properties;

        moq_media_drop_reason_t drop_reason = 0;
        moq_result_t pr = moq_media_object_parse(
            &mti, &moi, pb->sample_scratch,
            pb->cfg.max_samples_per_object, &parsed, &drop_reason);

        if (pr == MOQ_ERR_BUFFER) {
            moq_result_t er = push_drop_event(pb, obj->track,
                MOQ_PLAYBACK_DROP_UNSUPPORTED_MULTI_SAMPLE,
                obj->group_id, obj->object_id);
            if (er != MOQ_OK) return er;
            return MOQ_OK;
        }
        if (pr == MOQ_ERR_PROTO && drop_reason != 0) {
            moq_playback_drop_reason_t pb_reason =
                (moq_playback_drop_reason_t)drop_reason;
            moq_result_t er = push_drop_event(pb, obj->track, pb_reason,
                obj->group_id, obj->object_id);
            if (er != MOQ_OK) return er;
            return MOQ_OK;
        }
        if (pr != MOQ_OK) return MOQ_ERR_INVAL;

        /* Playback keeps the stricter timing policy the object parser relaxed:
         * the object layer now surfaces a RAW/LOC object without a capture
         * timestamp (has_capture_time=false) instead of dropping it, but a
         * playback pipeline needs a presentation time to schedule decode -- a
         * timestamp-less RAW object cannot be played, so drop it here rather than
         * buffer it. (CMAF derives timing from its samples, so this applies to
         * RAW only.) */
        if (mti.packaging == MOQ_MEDIA_PACKAGING_RAW &&
            !parsed.has_capture_time) {
            moq_result_t er = push_drop_event(pb, obj->track,
                MOQ_PLAYBACK_DROP_MISSING_TIMESTAMP,
                obj->group_id, obj->object_id);
            if (er != MOQ_OK) return er;
            return MOQ_OK;
        }
    }

    size_t plen = obj->payload ? moq_rcbuf_len(obj->payload) : 0;

    if (pb->obj_count >= pb->cfg.max_buffered_objects)
        return MOQ_ERR_WOULD_BLOCK;
    if (plen > pb->cfg.max_buffered_bytes - pb->retained_bytes)
        return MOQ_ERR_WOULD_BLOCK;

    pb_object_t *oslot = NULL;
    for (size_t i = 0; i < pb->cfg.max_buffered_objects; i++) {
        if (!pb->objects[i].occupied) {
            oslot = &pb->objects[i];
            break;
        }
    }
    if (!oslot) return MOQ_ERR_WOULD_BLOCK;

    if (!t->anchored) {
        t->anchored = true;
        t->next_expected_group = obj->group_id;
        t->next_expected_object = obj->object_id;
    }

    if (obj->payload)
        moq_rcbuf_incref(obj->payload);

    oslot->occupied = true;
    oslot->track = obj->track;
    oslot->group_id = obj->group_id;
    oslot->subgroup_id = obj->subgroup_id;
    oslot->object_id = obj->object_id;
    oslot->publisher_priority = obj->publisher_priority;
    oslot->status = obj->status;
    oslot->end_of_group = obj->end_of_group;
    oslot->datagram = obj->datagram;
    oslot->payload = obj->payload;
    oslot->payload_len = plen;
    oslot->has_capture_time = parsed.has_capture_time;
    oslot->capture_time_us = parsed.capture_time_us;
    oslot->decode_time_us = parsed.decode_time_us;
    oslot->composition_offset_us = parsed.composition_offset_us;
    oslot->presentation_time_us = parsed.presentation_time_us;
    oslot->keyframe = parsed.keyframe;
    oslot->sample_duration_us = parsed.sample_duration_us;
    oslot->mdat_offset = parsed.mdat_offset;
    oslot->mdat_len = parsed.mdat_len;
    oslot->sample_count = parsed.sample_count;

    pb->obj_count++;
    pb->retained_bytes += plen;

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
    pb->retained_bytes -= o->payload_len;
    moq_rcbuf_decref(o->payload);
    pb->obj_count--;
    memset(o, 0, sizeof(*o));
}

static void enter_recovery(pb_track_t *t, uint64_t abandoned)
{
    t->waiting_for_keyframe =
        (t->media_type == MOQ_PLAYBACK_MEDIA_VIDEO);
    t->kf_wait_event_emitted = false;
    t->needs_reset = true;
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

static moq_result_t emit_configure(moq_playback_t *pb,
                                    pb_track_t *t,
                                    moq_playback_track_t handle)
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
    moq_rcbuf_t *cc = t->codec_config ? t->codec_config : t->init_data;
    if (cc) {
        moq_rcbuf_incref(cc);
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
        moq_rcbuf_decref(config_ref);
        return rc;
    }
    t->configured = true;
    return MOQ_OK;
}

static bool pb_scale_checked(uint64_t ticks, uint32_t ts, uint64_t *out) {
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

static moq_result_t emit_decode(moq_playback_t *pb,
                                 pb_track_t *t,
                                 moq_playback_track_t handle,
                                 pb_object_t *obj,
                                 uint64_t *out_last_dts)
{
    bool is_cmaf = (t->packaging == MOQ_PLAYBACK_PACKAGING_CMAF);
    bool is_audio = (t->media_type == MOQ_PLAYBACK_MEDIA_AUDIO);
    size_t n_cmds = (is_cmaf && obj->sample_count > 1)
                    ? obj->sample_count : 1;

    bool preflight_only = (out_last_dts == NULL);

    if (!preflight_only &&
        pb->cmd_count + n_cmds > pb->cfg.max_commands)
        return MOQ_ERR_WOULD_BLOCK;

    if (n_cmds == 1) {
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
        *out_last_dts = obj->decode_time_us;
        return MOQ_OK;
    }

    /* Multi-sample CMAF: re-parse, preflight all math, then emit. */
    moq_bytes_t frag_bytes = {
        moq_rcbuf_data(obj->payload), moq_rcbuf_len(obj->payload) };
    moq_cmaf_fragment_info_t frag;
    moq_cmaf_fragment_info_init(&frag, pb->sample_scratch,
                                 pb->cfg.max_samples_per_object);
    if (moq_cmaf_parse_fragment(frag_bytes, &frag) != MOQ_OK)
        return MOQ_ERR_INVAL;

    /* Pass 1: validate all per-sample math without mutation. */
    {
        uint64_t check_dts = frag.base_decode_time;
        for (size_t si = 0; si < frag.sample_count; si++) {
            moq_cmaf_sample_t *s = &frag.samples[si];
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
    }

    if (!out_last_dts) return MOQ_OK;

    /* Pass 2: emit commands (all math guaranteed safe). */
    size_t mdat_cursor = obj->mdat_offset;
    uint64_t dts_ticks = frag.base_decode_time;
    uint64_t last_sample_dts = 0;

    for (size_t si = 0; si < frag.sample_count; si++) {
        moq_cmaf_sample_t *s = &frag.samples[si];
        uint64_t dts_us;
        pb_scale_checked(dts_ticks, t->timescale, &dts_us);

        bool neg = s->composition_offset < 0;
        uint32_t abs_co = neg ? (uint32_t)(-(int64_t)s->composition_offset)
                              : (uint32_t)s->composition_offset;
        uint64_t abs_us;
        pb_scale_checked(abs_co, t->timescale, &abs_us);
        int64_t comp_us = neg ? -(int64_t)abs_us : (int64_t)abs_us;

        uint64_t pts_us = (comp_us >= 0)
            ? dts_us + (uint64_t)comp_us
            : dts_us - (uint64_t)(-comp_us);

        uint64_t dur64;
        pb_scale_checked(s->duration, t->timescale, &dur64);
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
        mdat_cursor += s->size;
        dts_ticks += s->duration;
    }

    *out_last_dts = last_sample_dts;
    return MOQ_OK;
}

moq_result_t moq_playback_tick(moq_playback_t *pb, uint64_t now_us)
{
    if (!pb) return MOQ_ERR_INVAL;

    uint32_t released = 0;

    for (uint32_t ti = 0; ti < pb->cfg.max_tracks; ti++) {
        pb_track_t *t = &pb->tracks[ti];
        if (!t->active || !t->anchored || t->ended ||
            t->paused_by_pressure) continue;

        moq_playback_track_t handle =
            pb_encode_handle(pb->creation_tag, t->generation, ti);

        while (pb->cfg.max_release_per_tick == 0 ||
               released < pb->cfg.max_release_per_tick) {

            pb_object_t *obj = find_object(pb, handle,
                t->next_expected_group, t->next_expected_object);

            /* -- Missing expected object -------------------------------- */
            if (!obj) {
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
                        continue;
                    }
                }

                /* Gap timer: start or check timeout. */
                if (!t->gap_waiting) {
                    t->gap_waiting = true;
                    t->gap_group = t->next_expected_group;
                    t->gap_object = t->next_expected_object;
                    t->gap_started_us = now_us;
                    break;
                }

                uint64_t elapsed = (now_us >= t->gap_started_us)
                    ? now_us - t->gap_started_us : 0;
                if (elapsed < pb->cfg.gap_timeout_us)
                    break;

                /* Gap timeout: need 2 event slots atomically. */
                if (pb->evt_count + 2 > pb->cfg.max_events)
                    return MOQ_ERR_WOULD_BLOCK;

                moq_playback_event_t gap_evt;
                memset(&gap_evt, 0, sizeof(gap_evt));
                gap_evt.struct_size = sizeof(gap_evt);
                gap_evt.kind = MOQ_PLAYBACK_EVENT_GAP_DETECTED;
                gap_evt.track = handle;
                gap_evt.u.gap_detected.group_id = t->gap_group;
                push_evt(pb, &gap_evt);

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
                continue;
            }

            /* Expected object found — clear gap timer. */
            t->gap_waiting = false;

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
                break;
            }

            /* -- END_OF_GROUP marker ------------------------------------ */
            if (obj->status == MOQ_OBJECT_END_OF_GROUP) {
                release_buffered_object(pb, obj);
                advance_past_closed_group(pb, t, handle);
                released++;
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
            if (t->has_last_dts && obj->decode_time_us < t->last_dts) {
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
                continue;
            }

            /* -- Multi-sample preflight before any emission --------------- */
            bool is_cmaf_ms = (t->packaging == MOQ_PLAYBACK_PACKAGING_CMAF
                               && obj->sample_count > 1);
            if (is_cmaf_ms) {
                moq_result_t pf = emit_decode(pb, t, handle, obj,
                                               NULL);
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
                    continue;
                }
            }

            /* -- Pre-check capacity for CONFIGURE + DECODE --------------- */
            {
                size_t need = is_cmaf_ms ? obj->sample_count : 1;
                if (!t->configured) need++;
                if (pb->cmd_count + need > pb->cfg.max_commands)
                    return MOQ_ERR_WOULD_BLOCK;
            }

            if (!t->configured) {
                moq_result_t rc = emit_configure(pb, t, handle);
                if (rc != MOQ_OK) return rc;
            }

            uint64_t emitted_last_dts = 0;
            moq_result_t rc = emit_decode(pb, t, handle, obj,
                                           &emitted_last_dts);
            if (rc != MOQ_OK) return rc;

            t->has_last_dts = true;
            t->last_dts = emitted_last_dts;

            bool advance_group = obj_closes_group(pb, obj);
            release_buffered_object(pb, obj);

            if (advance_group) {
                advance_past_closed_group(pb, t, handle);
            } else {
                t->next_expected_object++;
            }
            released++;
        }
    }

    return MOQ_OK;
}

/* -- Polling --------------------------------------------------------- */

moq_result_t moq_playback_poll_command(moq_playback_t *pb,
                                        moq_playback_cmd_t *out)
{
    if (!pb || !out) return MOQ_ERR_INVAL;
    if (pb->cmd_count == 0) return MOQ_DONE;

    *out = pb->cmd_ring[pb->cmd_head % pb->cfg.max_commands];
    memset(&pb->cmd_ring[pb->cmd_head % pb->cfg.max_commands],
           0, sizeof(moq_playback_cmd_t));
    pb->cmd_head++;
    pb->cmd_count--;
    return MOQ_OK;
}

moq_result_t moq_playback_poll_event(moq_playback_t *pb,
                                      moq_playback_event_t *out)
{
    if (!pb || !out) return MOQ_ERR_INVAL;
    if (pb->evt_count == 0) return MOQ_DONE;

    *out = pb->evt_ring[pb->evt_head % pb->cfg.max_events];
    pb->evt_head++;
    pb->evt_count--;
    return MOQ_OK;
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

#define PB_FEEDBACK_MIN_SIZE \
    (offsetof(moq_playback_feedback_t, u) + \
     sizeof(((moq_playback_feedback_t *)0)->u))

moq_result_t moq_playback_handle_feedback(moq_playback_t *pb,
                                           const moq_playback_feedback_t *fb,
                                           uint64_t now_us)
{
    if (!pb || !fb) return MOQ_ERR_INVAL;
    if (fb->struct_size < PB_FEEDBACK_MIN_SIZE) return MOQ_ERR_INVAL;
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
                    moq_playback_cmd_cleanup(c);
                    memset(c, 0, sizeof(*c));
                } else {
                    if (write != i) {
                        size_t wi = (pb->cmd_head + write) %
                                    pb->cfg.max_commands;
                        pb->cmd_ring[wi] = *c;
                        memset(c, 0, sizeof(*c));
                    }
                    write++;
                }
            }
            pb->cmd_count = write;
        }

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

uint64_t moq_playback_test_retained_bytes(const moq_playback_t *pb)
{
    return pb ? pb->retained_bytes : 0;
}

size_t moq_playback_test_buffered_count(const moq_playback_t *pb)
{
    return pb ? pb->obj_count : 0;
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

#endif
