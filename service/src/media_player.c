/*
 * moq_media_player: a receiver and a playback engine composed on the
 * application's thread (see <moq/media_player.h>).
 *
 * Input goes receiver -> engine in the receiver's order: track events
 * first, then objects one at a time, each followed by the stream terminals
 * it made pollable. An object the engine refuses for room is held, and
 * while it is held nothing more is taken from the receiver, so no later
 * object or stream fact overtakes it: one held object is all there ever
 * is.
 *
 * Track events are taken even while an object is held: they may start a
 * track's end, but its END needs the engine to end the track, which waits
 * for the held object to go in.
 *
 * Output goes engine -> player queue: every command becomes one output (a
 * timeline's first unit is preceded by its TIMELINE), polled with its bytes
 * still charged to the engine's budget until the application polls the
 * output. The engine's reports are settled as soon as they exist, and
 * before any trigger, so loss is placed where it happened; a track's END
 * waits until the engine holds no more commands, so it follows the
 * track's last unit.
 */
#include <moq/media_player.h>

#include <stdlib.h>
#include <string.h>

#include "endpoint_internal.h"
#include "media_receiver_internal.h"

#define PLAYER_DEFAULT_TAIL_US      2000000u
#define PLAYER_DEFAULT_CUSHION_US    100000u
#define PLAYER_DEFAULT_OUTPUTS          128u
#define PLAYER_DEFAULT_EVENTS            64u
#define PLAYER_DEFAULT_HELD_BYTES (8u * 1024u * 1024u)
#define PLAYER_TERMINAL_EVENT_RESERVE     2u

/* PUBLISH_DONE / SUBSCRIBE_DONE codes that end a track normally. */
#define PLAYER_DONE_TRACK_ENDED        0x2u
#define PLAYER_DONE_SUBSCRIPTION_ENDED 0x3u

_Static_assert(sizeof(((moq_media_player_output_t *)0)->u) ==
               MOQ_MEDIA_PLAYER_OUTPUT_DETAIL_MAX,
               "output details fit their reserved size");
_Static_assert(sizeof(((moq_media_player_event_t *)0)->u) ==
               MOQ_MEDIA_PLAYER_EVENT_DETAIL_MAX,
               "event details fit their reserved size");

/* One timeline: a render group's, or one track's. */
typedef struct {
    bool     used;
    uint32_t id;
    uint32_t domain;              /* moq_media_time_domain_t */
    bool     has_render_group;
    int      render_group;
    bool     independent;         /* a track's own; never joined */
    bool     shared_epoch;        /* every member asserted it */
    uint32_t members;
    bool     anchored;
    uint64_t anchor_media_us;
    uint64_t anchor_local_us;
} player_timeline_t;

/* One selected track. */
typedef struct {
    bool                  used;
    moq_media_track_t    *track;
    moq_playback_track_t  eng;
    bool                  eng_live;      /* eng is a track of the engine */
    uint32_t              timeline;      /* index into timelines */
    uint32_t              domain;
    uint32_t              media_type;    /* moq_media_type_t value */
    uint32_t              timescale;
    uint64_t              config_gen;
    bool                  requires_config;
    bool                  config_seen;
    bool                  paused;        /* DECODER_QUEUE above its mark */
    bool                  held;          /* the held object is its */
    bool                  settled;       /* the engine waits for nothing */
    /* Terminal processing. */
    uint32_t              cause;         /* moq_media_player_end_cause_t, 0 none */
    bool                  tail_armed;
    uint64_t              tail_left_us;  /* of the tail wait */
    uint64_t              tail_step_us;  /* when it was last marked */
    bool                  tail_frozen;   /* since then: held up locally */
    bool                  cut_off;       /* a bounded wait gave up */
    bool                  drain_requested;
    bool                  drained;       /* the engine was told: no more input */
    bool                  end_owed;      /* the engine ended it; END to emit */
    bool                  end_emitted;
    /* The receiver ended the track for good without a cause the player
     * acts on (a normal or statusless TRACK_ENDED): an END_OF_TRACK may
     * still arrive, so this is a bounded wait, not a cause. */
    bool                  done_hint;
    uint64_t              done_wait_left_us; /* of the tail wait */
    uint64_t              done_wait_step_us; /* when it was last marked */
    bool                  done_wait_frozen;  /* since then: held up locally */
    /* The receiver reported a normal end of the track (its END_OF_TRACK,
     * or a normal or statusless done). */
    bool                  peer_ended;
} player_track_t;

/* The object held for the engine. */
typedef struct {
    uint32_t           track;            /* index into tracks */
    uint64_t           charge;
    moq_media_object_t obj;
} player_held_t;

/* A refusal the engine had no room to record yet. */
typedef struct {
    uint32_t             track;          /* index into tracks */
    moq_playback_track_t eng;
    uint64_t             group_id, object_id;
} player_refusal_t;

struct moq_media_player {
    moq_alloc_t           alloc;
    moq_media_receiver_t *r;
    bool                  owns_receiver;
    moq_playback_t       *pb;

    uint32_t selection, timing_policy;
    bool     assume_shared_epoch;
    uint8_t *lang;
    size_t   lang_len;
    uint64_t tail_us, cushion_us;
    uint32_t transport_version;

    player_track_t    *tracks;
    player_timeline_t *timelines;
    uint32_t           max_tracks;
    uint32_t           next_timeline_id;

    /* Every track handle the receiver announced, in order. */
    moq_media_track_t **known;
    size_t              known_count, known_cap;
    bool                catalog_ready;

    bool              has_held;
    player_held_t     held;
    uint64_t          max_held_bytes;
    bool                        has_held_terminal;
    moq_media_stream_terminal_t held_terminal;
    bool                        has_held_refusal;
    player_refusal_t            held_refusal;
    /* No engine command can be held any more: every track ends. */
    bool                        holds_exhausted;

    moq_media_player_output_t *outputs;
    moq_playback_command_hold_t *out_hold;   /* per slot: what it keeps charged */
    uint32_t                   out_cap, out_head, out_count;
    /* The engine's next command, taken under its hold when the output
     * queue had no room for it: it goes out ahead of every later one. */
    bool                        has_staged;
    moq_playback_cmd_t          staged;
    moq_playback_command_hold_t staged_hold;
    /* The engine has a command the output queue has no room for: one is
     * staged. */
    bool                       engine_backlog;
    /* AUTO is owed: the catalog is ready but selection could not run. */
    bool                       auto_pending;
    moq_media_player_event_t  *events;
    uint32_t                   ev_cap, ev_limit, ev_head, ev_count;

    bool     receiver_terminal;   /* closed or fatal */
    bool     input_exhausted;     /* terminal and its object queue empty */
    bool     fatal;
    uint64_t fatal_code;
    bool     ended_reported, closed_reported;

    moq_media_player_stats_t stats;
};

static void *player_calloc(const moq_alloc_t *a, size_t count, size_t size);

/* -- Configuration ------------------------------------------------------ */

void moq_media_player_cfg_init_sized(moq_media_player_cfg_t *cfg, size_t cfg_size)
{
    if (!cfg || cfg_size < sizeof(uint32_t)) return;
    size_t n = cfg_size < sizeof(*cfg) ? cfg_size : sizeof(*cfg);
    memset(cfg, 0, n);
    cfg->struct_size = (uint32_t)n;
}

void moq_media_player_select_cfg_init_sized(moq_media_player_select_cfg_t *cfg,
                                            size_t cfg_size)
{
    if (!cfg || cfg_size < sizeof(uint32_t)) return;
    size_t n = cfg_size < sizeof(*cfg) ? cfg_size : sizeof(*cfg);
    memset(cfg, 0, n);
    cfg->struct_size = (uint32_t)n;
}

void moq_media_player_feedback_init_sized(moq_media_player_feedback_t *fb,
                                          size_t fb_size)
{
    if (!fb || fb_size < sizeof(uint32_t)) return;
    size_t n = fb_size < sizeof(*fb) ? fb_size : sizeof(*fb);
    memset(fb, 0, n);
    fb->struct_size = (uint32_t)n;
}

/* -- Queues ------------------------------------------------------------- */

static uint32_t out_free(const moq_media_player_t *p)
{
    return p->out_cap - p->out_count;
}

static moq_media_player_output_t *out_push(moq_media_player_t *p,
                                           moq_media_player_output_kind_t kind,
                                           const player_track_t *t,
                                           uint32_t detail_size)
{
    uint32_t slot = (p->out_head + p->out_count) % p->out_cap;
    moq_media_player_output_t *o = &p->outputs[slot];
    memset(o, 0, sizeof(*o));
    p->out_hold[slot] = MOQ_PLAYBACK_COMMAND_HOLD_INVALID;
    o->struct_size = (uint32_t)sizeof(*o);
    o->kind = kind;
    o->detail_size = detail_size;
    if (t) {
        o->track = t->track;
        o->timeline_id = p->timelines[t->timeline].id;
        o->configuration_generation = t->config_gen;
    }
    p->out_count++;
    return o;
}

/* Queue an event. A full queue drops its oldest diagnostic to make room;
 * ENDED and CLOSED use the slots kept for them and are never dropped. */
static void ev_push(moq_media_player_t *p, const moq_media_player_event_t *ev)
{
    bool terminal = ev->kind == MOQ_MEDIA_PLAYER_EVENT_ENDED ||
                    ev->kind == MOQ_MEDIA_PLAYER_EVENT_CLOSED;
    uint32_t limit = terminal ? p->ev_cap : p->ev_limit;
    while (p->ev_count >= limit) {
        moq_media_player_event_t *old = &p->events[p->ev_head % p->ev_cap];
        if (old->kind == MOQ_MEDIA_PLAYER_EVENT_ENDED ||
            old->kind == MOQ_MEDIA_PLAYER_EVENT_CLOSED) {
            /* Only terminal events are left ahead: the diagnostic goes. */
            p->stats.events_dropped++;
            return;
        }
        p->ev_head++;
        p->ev_count--;
        p->stats.events_dropped++;
    }
    moq_media_player_event_t *e = &p->events[(p->ev_head + p->ev_count) % p->ev_cap];
    *e = *ev;
    e->struct_size = (uint32_t)sizeof(*e);
    p->ev_count++;
}

static moq_media_player_event_t ev_make(moq_media_player_event_kind_t kind,
                                        moq_media_track_t *track,
                                        uint32_t detail_size)
{
    moq_media_player_event_t ev;
    memset(&ev, 0, sizeof(ev));
    ev.kind = kind;
    ev.track = track;
    ev.detail_size = detail_size;
    return ev;
}

/* The output just queued keeps its command's hold until it is polled. */
static void out_keep_hold(moq_media_player_t *p, const moq_media_player_output_t *o,
                          moq_playback_command_hold_t hold)
{
    p->out_hold[o - p->outputs] = hold;
}

/* An output leaves the queue: its bytes leave the engine's budget. */
static void out_release_hold(moq_media_player_t *p, uint32_t slot)
{
    moq_playback_release_command_hold(p->pb, p->out_hold[slot]);
    p->out_hold[slot] = MOQ_PLAYBACK_COMMAND_HOLD_INVALID;
}

#define DETAIL(member) ((uint32_t)sizeof(((moq_media_player_event_t *)0)->u.member))
#define OUT_DETAIL(member) ((uint32_t)sizeof(((moq_media_player_output_t *)0)->u.member))

static void ev_drop(moq_media_player_t *p, moq_media_track_t *track,
                    uint32_t reason, uint64_t g, uint64_t o)
{
    moq_media_player_event_t ev = ev_make(MOQ_MEDIA_PLAYER_EVENT_DROP, track,
                                          DETAIL(drop));
    ev.u.drop.reason = reason;
    ev.u.drop.group_id = g;
    ev.u.drop.object_id = o;
    ev_push(p, &ev);
}

/* A drop of the player's own, for track `t`: counted and evented. Once
 * the track's end has begun it is loss in that end, which can then no
 * longer be COMPLETE. */
static void player_drop(moq_media_player_t *p, player_track_t *t, uint32_t reason,
                        uint64_t g, uint64_t o)
{
    p->stats.objects_dropped++;
    ev_drop(p, t->track, reason, g, o);
    if (t->cause && !t->end_emitted) t->cut_off = true;
}

/* Record a refusal in the engine; false when it has no event room yet. A
 * stale or ended track has nothing to record. */
static bool refusal_feed(moq_media_player_t *p, const player_refusal_t *r,
                         uint64_t now_us)
{
    moq_playback_stream_terminal_t f;
    memset(&f, 0, sizeof(f));
    f.struct_size = sizeof(f);
    f.kind = MOQ_PLAYBACK_STREAM_OBJECT_REFUSED;
    f.group_id = r->group_id;
    f.last_group_id = r->group_id;
    f.object_id = r->object_id;
    return moq_playback_on_stream_terminal(p->pb, r->eng, &f, now_us) !=
           MOQ_ERR_WOULD_BLOCK;
}

/* An object the player refused before the engine took it: a drop, and its
 * id unresolved in the engine, so a wait on it reports why, and nothing
 * reads its absence. A refusal the engine cannot record yet is held, one
 * at a time; input stops behind it, as behind a held object. */
static void player_refuse(moq_media_player_t *p, player_track_t *t, uint32_t reason,
                          uint64_t g, uint64_t o, uint64_t now_us)
{
    player_drop(p, t, reason, g, o);
    /* Every group ID is valid; an object ID of UINT64_MAX is the engine's
     * end-of-group marker, never a real object, so it has no fact. */
    if (!t->eng_live || o == UINT64_MAX) return;
    player_refusal_t r = { .track = (uint32_t)(t - p->tracks), .eng = t->eng,
                           .group_id = g, .object_id = o };
    if (refusal_feed(p, &r, now_us)) return;
    p->held_refusal = r;
    p->has_held_refusal = true;
}

/* Retry the held refusal; false while it is still held. */
static bool refusal_retry(moq_media_player_t *p, uint64_t now_us)
{
    if (!p->has_held_refusal) return true;
    if (!refusal_feed(p, &p->held_refusal, now_us)) return false;
    p->has_held_refusal = false;
    return true;
}

#ifdef MOQ_MEDIA_PLAYER_TESTING
/* The player's engine, for engine seams. */
moq_playback_t *moq_media_player_test_engine(const moq_media_player_t *p);
moq_playback_t *moq_media_player_test_engine(const moq_media_player_t *p)
{
    return p ? p->pb : NULL;
}
#endif

void moq_media_player_output_cleanup(moq_media_player_output_t *out)
{
    if (!out || out->struct_size < sizeof(*out)) return;
    switch (out->kind) {
    case MOQ_MEDIA_PLAYER_OUTPUT_CONFIGURE:
        if (out->u.configure.codec) moq_rcbuf_decref(out->u.configure.codec);
        if (out->u.configure.codec_config)
            moq_rcbuf_decref(out->u.configure.codec_config);
        out->u.configure.codec = NULL;
        out->u.configure.codec_config = NULL;
        break;
    case MOQ_MEDIA_PLAYER_OUTPUT_UNIT:
        if (out->u.unit.payload) moq_rcbuf_decref(out->u.unit.payload);
        out->u.unit.payload = NULL;
        break;
    default:
        break;
    }
}

/* -- Tracks ------------------------------------------------------------- */

static player_track_t *track_of(moq_media_player_t *p, const moq_media_track_t *track,
                                uint32_t *index)
{
    for (uint32_t i = 0; i < p->max_tracks; i++) {
        if (p->tracks[i].used && p->tracks[i].track == track) {
            if (index) *index = i;
            return &p->tracks[i];
        }
    }
    return NULL;
}

static player_track_t *track_of_engine(moq_media_player_t *p, moq_playback_track_t eng)
{
    for (uint32_t i = 0; i < p->max_tracks; i++)
        if (p->tracks[i].used && p->tracks[i].eng_live &&
            p->tracks[i].eng._opaque == eng._opaque)
            return &p->tracks[i];
    return NULL;
}

static bool span_is(moq_bytes_t b, const char *lit)
{
    size_t n = strlen(lit);
    return b.len == n && (n == 0 || memcmp(b.data, lit, n) == 0);
}

/* What this release plays, judged from the catalog alone. */
static moq_result_t track_supported(const moq_media_player_t *p,
                                    const moq_media_track_desc_t *d,
                                    bool *requires_config)
{
    *requires_config = false;
    if (d->info.packaging != MOQ_MEDIA_PACKAGING_RAW ||
        !(span_is(d->packaging, "loc") || span_is(d->packaging, "")))
        return MOQ_ERR_UNSUPPORTED;
    if (d->depends_count > 0) return MOQ_ERR_UNSUPPORTED;
    if (d->info.media_type == MOQ_MEDIA_TYPE_VIDEO) return MOQ_OK;
    if (d->info.media_type != MOQ_MEDIA_TYPE_AUDIO) return MOQ_ERR_UNSUPPORTED;
    if (span_is(d->codec, "opus")) return MOQ_OK;
    if (span_is(d->codec, "mp4a.40.2")) {
        /* AAC-LC needs its AudioSpecificConfig: from the catalog, or in
         * band, which only draft 22's LOC-04 can carry. */
        *requires_config = true;
        bool catalog = d->init.codec_config.len > 0 || d->init_data.len > 0;
        if (!catalog && p->transport_version != MOQ_VERSION_DRAFT_22)
            return MOQ_ERR_UNSUPPORTED;
        return MOQ_OK;
    }
    return MOQ_ERR_UNSUPPORTED;
}

/* The domain a track's timestamps are in, from its catalog entry. */
static uint32_t track_domain(const moq_media_player_t *p,
                             const moq_media_track_desc_t *d)
{
    if (p->transport_version == MOQ_VERSION_DRAFT_22 && d->info.timescale != 0)
        return MOQ_MEDIA_TIME_DOMAIN_APPLICATION_MEDIA;
    return MOQ_MEDIA_TIME_DOMAIN_UNIX_US;
}

static uint32_t parse_channels(moq_bytes_t s)
{
    uint32_t v = 0;
    for (size_t i = 0; i < s.len; i++) {
        if (s.data[i] < '0' || s.data[i] > '9') break;
        if (v > 1000) return 0;
        v = v * 10u + (uint32_t)(s.data[i] - '0');
    }
    return v;
}

static moq_result_t player_refresh_version(moq_media_player_t *p)
{
    moq_media_receiver_info_t info;
    moq_result_t rc = moq_media_receiver_get_info(p->r, &info, sizeof(info));
    if (rc != MOQ_OK) return rc;
    p->transport_version = info.transport_version;
    return MOQ_OK;
}

/* Find or make the timeline a track joins. MOQ_ERR_UNSUPPORTED when its
 * render group's timeline cannot take it. */
static moq_result_t timeline_for(moq_media_player_t *p,
                                 const moq_media_track_desc_t *d,
                                 uint32_t domain,
                                 const moq_media_player_select_cfg_t *sc,
                                 uint32_t *out, bool *created)
{
    *created = false;
    if (!sc->independent_timing && d->has_render_group) {
        for (uint32_t i = 0; i < p->max_tracks; i++) {
            player_timeline_t *tl = &p->timelines[i];
            if (!tl->used || tl->independent || !tl->has_render_group ||
                tl->render_group != d->render_group)
                continue;
            if (tl->domain != domain) return MOQ_ERR_UNSUPPORTED;
            if (domain == MOQ_MEDIA_TIME_DOMAIN_APPLICATION_MEDIA &&
                !(tl->shared_epoch && sc->shared_epoch))
                return MOQ_ERR_UNSUPPORTED;
            *out = i;
            return MOQ_OK;
        }
    }
    for (uint32_t i = 0; i < p->max_tracks; i++) {
        player_timeline_t *tl = &p->timelines[i];
        if (tl->used) continue;
        memset(tl, 0, sizeof(*tl));
        tl->used = true;
        tl->id = ++p->next_timeline_id;
        if (tl->id == 0) tl->id = ++p->next_timeline_id;
        tl->domain = domain;
        tl->independent = sc->independent_timing || !d->has_render_group;
        tl->has_render_group = d->has_render_group;
        tl->render_group = d->render_group;
        tl->shared_epoch = sc->shared_epoch;
        *out = i;
        *created = true;
        return MOQ_OK;
    }
    return MOQ_ERR_LIMIT;
}

static void timeline_leave(moq_media_player_t *p, uint32_t index)
{
    player_timeline_t *tl = &p->timelines[index];
    if (tl->members > 0) tl->members--;
    if (tl->members == 0) memset(tl, 0, sizeof(*tl));
}

static moq_result_t player_select(moq_media_player_t *p, moq_media_track_t *track,
                                  const moq_media_player_select_cfg_t *sc)
{
    if (p->receiver_terminal) return MOQ_ERR_WRONG_STATE;
    if (track_of(p, track, NULL)) return MOQ_ERR_WRONG_STATE;
    moq_media_track_state_t state;
    moq_result_t rc = moq_media_receiver_track_state(p->r, track, &state);
    if (rc != MOQ_OK) return rc;
    if (state == MOQ_MEDIA_TRACK_STATE_ENDED || state == MOQ_MEDIA_TRACK_STATE_FAILED)
        return MOQ_ERR_WRONG_STATE;
    moq_media_track_desc_t d;
    rc = moq_media_receiver_track_desc_copy(p->r, track, &d, sizeof(d));
    if (rc != MOQ_OK) return rc;
    if (p->transport_version == 0 && (rc = player_refresh_version(p)) != MOQ_OK)
        return rc;
    if (p->transport_version == 0) return MOQ_ERR_WRONG_STATE;
    bool requires_config;
    rc = track_supported(p, &d, &requires_config);
    if (rc != MOQ_OK) return rc;

    uint32_t slot = UINT32_MAX;
    for (uint32_t i = 0; i < p->max_tracks && slot == UINT32_MAX; i++)
        if (!p->tracks[i].used) slot = i;
    if (slot == UINT32_MAX) return MOQ_ERR_LIMIT;

    uint32_t domain = track_domain(p, &d);
    uint32_t tl;
    bool created;
    rc = timeline_for(p, &d, domain, sc, &tl, &created);
    if (rc != MOQ_OK) return rc;

    bool video = d.info.media_type == MOQ_MEDIA_TYPE_VIDEO;
    moq_playback_track_cfg_t tc;
    moq_playback_track_cfg_init(&tc);
    tc.media_type = video ? MOQ_PLAYBACK_MEDIA_VIDEO : MOQ_PLAYBACK_MEDIA_AUDIO;
    tc.packaging = MOQ_PLAYBACK_PACKAGING_RAW;
    tc.codec = d.codec;
    tc.init_data = d.init.codec_config.len ? d.init.codec_config : d.init_data;
    tc.timescale = d.info.timescale;
    tc.width = d.has_width ? d.width : 0;
    tc.height = d.has_height ? d.height : 0;
    tc.samplerate = d.has_samplerate ? d.samplerate : 0;
    tc.channel_count = parse_channels(d.channel_config);
    tc.is_live = d.is_live;
    tc.transport_version = p->transport_version;
    tc.gap_policy = video ? MOQ_PLAYBACK_GAP_DISCONTINUITY : MOQ_PLAYBACK_GAP_CONTINUE;
    moq_playback_track_t eng;
    rc = moq_playback_add_track(p->pb, &tc, &eng);
    if (rc == MOQ_ERR_WOULD_BLOCK) rc = MOQ_ERR_LIMIT;
    if (rc != MOQ_OK) {
        if (created) memset(&p->timelines[tl], 0, sizeof(p->timelines[tl]));
        return rc;
    }

    moq_media_receiver_track_subscribe_cfg_t rs;
    moq_media_receiver_track_subscribe_cfg_init_sized(&rs, sizeof(rs));
    rs.start = (moq_media_start_mode_t)sc->start;
    rc = moq_media_receiver_subscribe_track(p->r, track, &rs);
    if (rc != MOQ_OK) {
        (void)moq_playback_remove_track(p->pb, eng);
        if (created) memset(&p->timelines[tl], 0, sizeof(p->timelines[tl]));
        return rc;
    }

    player_track_t *t = &p->tracks[slot];
    memset(t, 0, sizeof(*t));
    t->used = true;
    t->track = track;
    t->eng = eng;
    t->eng_live = true;
    t->timeline = tl;
    t->domain = domain;
    t->media_type = (uint32_t)d.info.media_type;
    t->timescale = d.info.timescale;
    t->requires_config = requires_config;
    t->config_seen = !requires_config;
    p->timelines[tl].members++;
    p->stats.tracks_selected++;
    return MOQ_OK;
}

moq_result_t moq_media_player_select_track(moq_media_player_t *p,
                                           moq_media_track_t *track,
                                           const moq_media_player_select_cfg_t *cfg)
{
    if (!p || !track) return MOQ_ERR_INVAL;
    moq_media_player_select_cfg_t sc;
    moq_media_player_select_cfg_init(&sc);
    if (cfg) {
        if (cfg->struct_size < MOQ_MEDIA_PLAYER_SELECT_CFG_V0_SIZE) return MOQ_ERR_INVAL;
        size_t n = cfg->struct_size < sizeof(sc) ? cfg->struct_size : sizeof(sc);
        memcpy(&sc, cfg, n);
        sc.struct_size = (uint32_t)sizeof(sc);
    }
    if (sc.start != MOQ_MEDIA_START_CURRENT && sc.start != MOQ_MEDIA_START_NEXT_GROUP)
        return MOQ_ERR_INVAL;
    return player_select(p, track, &sc);
}

static void staged_drop(moq_media_player_t *p, const player_track_t *t);

static void held_drop(moq_media_player_t *p)
{
    if (!p->has_held) return;
    moq_media_object_cleanup(&p->held.obj);
    p->tracks[p->held.track].held = false;
    memset(&p->held, 0, sizeof(p->held));
    p->has_held = false;
}

static void track_release(moq_media_player_t *p, uint32_t index, bool unsubscribe)
{
    player_track_t *t = &p->tracks[index];
    if (p->has_held && p->held.track == index) held_drop(p);
    if (p->has_held_refusal && p->held_refusal.track == index) p->has_held_refusal = false;
    staged_drop(p, t);
    if (t->eng_live) (void)moq_playback_remove_track(p->pb, t->eng);
    if (unsubscribe) (void)moq_media_receiver_unsubscribe_track(p->r, t->track);
    timeline_leave(p, t->timeline);
    memset(t, 0, sizeof(*t));
    if (p->stats.tracks_selected > 0) p->stats.tracks_selected--;
}

moq_result_t moq_media_player_deselect_track(moq_media_player_t *p,
                                             moq_media_track_t *track)
{
    if (!p || !track) return MOQ_ERR_INVAL;
    uint32_t index;
    if (!track_of(p, track, &index)) return MOQ_ERR_INVAL;
    track_release(p, index, true);
    return MOQ_OK;
}

/* -- AUTO selection ------------------------------------------------------- */

static bool desc_of(moq_media_player_t *p, moq_media_track_t *track,
                    moq_media_track_desc_t *d)
{
    return moq_media_receiver_track_desc_copy(p->r, track, d, sizeof(*d)) == MOQ_OK;
}

static bool auto_candidate(moq_media_player_t *p, moq_media_track_t *track,
                           moq_media_track_desc_t *d)
{
    bool requires_config;
    moq_media_track_state_t state;
    if (!desc_of(p, track, d)) return false;
    if (moq_media_receiver_track_state(p->r, track, &state) != MOQ_OK) return false;
    if (state == MOQ_MEDIA_TRACK_STATE_ENDED || state == MOQ_MEDIA_TRACK_STATE_FAILED)
        return false;
    return track_supported(p, d, &requires_config) == MOQ_OK;
}

/* Of an alternate group's members (indices into d), the one AUTO plays: a
 * preferred language match first, then the middle of the bitrate ladder.
 * `idx` has room for every member. */
static size_t alt_pick(const moq_media_player_t *p, const moq_media_track_desc_t *d,
                       const size_t *members, size_t m, size_t *idx)
{
    size_t k = 0;
    for (size_t i = 0; i < m; i++) {
        const moq_media_track_desc_t *x = &d[members[i]];
        if (p->lang_len && x->lang.len == p->lang_len &&
            memcmp(x->lang.data, p->lang, p->lang_len) == 0)
            idx[k++] = members[i];
    }
    if (k == 0)
        for (size_t i = 0; i < m; i++) idx[k++] = members[i];
    /* Order by bitrate (unknown last), then take the middle. */
    for (size_t i = 1; i < k; i++) {
        size_t v = idx[i], j = i;
        uint64_t bv = d[v].has_bitrate ? d[v].bitrate : UINT64_MAX;
        while (j > 0) {
            size_t u = idx[j - 1];
            uint64_t bu = d[u].has_bitrate ? d[u].bitrate : UINT64_MAX;
            if (bu <= bv) break;
            idx[j] = u;
            j--;
        }
        idx[j] = v;
    }
    return idx[(k - 1) / 2];
}

/* AUTO over every announced track. MOQ_ERR_NOMEM when its scratch cannot
 * be had: nothing is selected and AUTO stays owed. */
static moq_result_t auto_select(moq_media_player_t *p)
{
    size_t n = p->known_count;
    if (n == 0) return MOQ_OK;
    moq_media_track_desc_t *d = (moq_media_track_desc_t *)player_calloc(
        &p->alloc, n, sizeof(*d));
    bool *cand = (bool *)player_calloc(&p->alloc, n, sizeof(bool));
    size_t *scratch = (size_t *)player_calloc(&p->alloc, n, 2 * sizeof(size_t));
    if (!d || !cand || !scratch) {
        if (d) p->alloc.free(d, n * sizeof(*d), p->alloc.ctx);
        if (cand) p->alloc.free(cand, n * sizeof(bool), p->alloc.ctx);
        if (scratch) p->alloc.free(scratch, n * 2 * sizeof(size_t), p->alloc.ctx);
        return MOQ_ERR_NOMEM;
    }
    size_t *members = scratch, *idx = scratch + n;
    bool have_group = false;
    int group = 0;
    for (size_t i = 0; i < n; i++) {
        cand[i] = auto_candidate(p, p->known[i], &d[i]);
        if (cand[i] && d[i].has_render_group &&
            (!have_group || d[i].render_group < group)) {
            have_group = true;
            group = d[i].render_group;
        }
    }
    /* The tracks of the chosen group: the lowest render group, or without
     * any, one track by itself: the first video, else the first. */
    size_t single = SIZE_MAX;
    if (!have_group) {
        for (size_t i = 0; i < n && single == SIZE_MAX; i++)
            if (cand[i] && d[i].info.media_type == MOQ_MEDIA_TYPE_VIDEO) single = i;
        for (size_t i = 0; i < n && single == SIZE_MAX; i++)
            if (cand[i]) single = i;
    }
    for (size_t i = 0; i < n; i++) {
        bool in = have_group ? (cand[i] && d[i].has_render_group &&
                                d[i].render_group == group)
                             : i == single;
        cand[i] = in;
    }
    /* One per alternate group: each group decided once, over all of its
     * members. */
    for (size_t i = 0; i < n; i++) {
        if (!cand[i] || !d[i].has_alt_group) continue;
        size_t m = 0;
        for (size_t j = i; j < n; j++)
            if (cand[j] && d[j].has_alt_group && d[j].alt_group == d[i].alt_group)
                members[m++] = j;
        size_t keep = alt_pick(p, d, members, m, idx);
        for (size_t k = 0; k < m; k++) {
            cand[members[k]] = members[k] == keep;
            d[members[k]].has_alt_group = false;   /* decided */
        }
    }
    /* One timeline needs one domain, and application media time needs the
     * shared-epoch assertion. */
    size_t chosen = 0;
    bool unix_seen = false, app_seen = false;
    for (size_t i = 0; i < n; i++) {
        if (!cand[i]) continue;
        chosen++;
        if (track_domain(p, &d[i]) == MOQ_MEDIA_TIME_DOMAIN_UNIX_US) unix_seen = true;
        else app_seen = true;
    }
    uint32_t reason = 0;
    if (chosen > 1 && unix_seen && app_seen) reason = MOQ_MEDIA_PLAYER_SYNC_MIXED_DOMAINS;
    else if (chosen > 1 && app_seen && !p->assume_shared_epoch)
        reason = MOQ_MEDIA_PLAYER_SYNC_NO_SHARED_EPOCH;
    bool independent = false;
    if (reason) {
        moq_media_player_event_kind_t kind =
            p->timing_policy == MOQ_MEDIA_TIMING_ALLOW_INDEPENDENT
                ? MOQ_MEDIA_PLAYER_EVENT_TIMING_INDEPENDENT
                : MOQ_MEDIA_PLAYER_EVENT_SYNC_UNAVAILABLE;
        moq_media_player_event_t ev = ev_make(kind, NULL, DETAIL(sync));
        ev.u.sync.has_render_group = have_group;
        ev.u.sync.render_group = have_group ? group : 0;
        ev.u.sync.reason = reason;
        ev_push(p, &ev);
        if (p->timing_policy != MOQ_MEDIA_TIMING_ALLOW_INDEPENDENT) {
            for (size_t i = 0; i < n; i++) cand[i] = false;
        }
        independent = true;
    }
    /* A chosen track that cannot be selected is counted and stays
     * available. */
    for (size_t i = 0; i < n; i++) {
        if (!cand[i]) continue;
        moq_media_player_select_cfg_t sc;
        moq_media_player_select_cfg_init(&sc);
        sc.independent_timing = independent;
        sc.shared_epoch = p->assume_shared_epoch;
        if (player_select(p, p->known[i], &sc) != MOQ_OK) p->stats.selections_failed++;
    }
    p->alloc.free(d, n * sizeof(*d), p->alloc.ctx);
    p->alloc.free(cand, n * sizeof(bool), p->alloc.ctx);
    p->alloc.free(scratch, n * 2 * sizeof(size_t), p->alloc.ctx);
    return MOQ_OK;
}

/* -- Terminal processing -------------------------------------------------- */

static bool cause_immediate(uint32_t cause)
{
    return cause == MOQ_MEDIA_PLAYER_END_LOCAL_FAILURE ||
           cause == MOQ_MEDIA_PLAYER_END_CLOSED ||
           cause == MOQ_MEDIA_PLAYER_END_FATAL;
}

static void settle_reports(moq_media_player_t *p);
static bool held_by_application(moq_media_player_t *p);
static bool tail_frozen_now(const moq_media_player_t *p, const player_track_t *t,
                            bool application);

/* A trigger of terminal processing. The highest cause seen before END is
 * queued wins, also while the END waits for output room. A tail wait
 * starts at the first trigger that has one; a higher trigger arriving
 * during it ends the wait. Immediate causes end it at once. Every report
 * the engine already made is settled first: what happened before the
 * trigger is not loss in the end. */
static void track_trigger(moq_media_player_t *p, player_track_t *t, uint32_t cause,
                          uint64_t now_us)
{
    if (t->end_emitted) return;
    settle_reports(p);
    bool raised = cause > t->cause;
    if (raised) t->cause = cause;
    if (t->end_owed) return;           /* the engine ended it: only the cause rises */
    if (cause_immediate(cause)) {
        t->drain_requested = true;
        return;
    }
    if (!t->tail_armed) {
        t->tail_armed = true;
        t->tail_left_us = p->tail_us;
        t->tail_step_us = now_us;
        /* The engine is asked only when its answer can matter. */
        bool application = t->cause == MOQ_MEDIA_PLAYER_END_PEER_END_OF_TRACK &&
                           !t->paused && !p->engine_backlog && held_by_application(p);
        t->tail_frozen = tail_frozen_now(p, t, application);
    } else if (raised) {
        t->cut_off = true;
        t->drain_requested = true;
    }
}

static bool done_hint_open(const player_track_t *t)
{
    return t->done_hint && !t->cause && !t->end_owed && !t->drain_requested &&
           !t->end_emitted;
}

/* Input held up by the application, not the peer: the player holds input
 * (an object, a stream terminal or a refusal) while the engine has output
 * the queue has no room for (the application is not polling), or holds an
 * object the engine admits only once the application releases what it
 * holds up (`application`: paused decoders' buffered media, unpolled
 * commands, unreleased command holds). An END_OF_TRACK already received
 * may sit behind such input, or the track's own media may wait behind it.
 * Another track's pause alone is no reason; nor is input the engine waits
 * behind for its own progress, such as a missing unit the done says will
 * never come. */
static bool input_held_up(const moq_media_player_t *p, bool application)
{
    if (p->has_held && application) return true;
    return (p->has_held || p->has_held_terminal || p->has_held_refusal) && p->engine_backlog;
}

/* The done's wait is held up locally: as input_held_up, or the track's own
 * decoder is paused (the engine releases and settles nothing of a paused
 * track). */
static bool done_wait_frozen_now(const moq_media_player_t *p, const player_track_t *t,
                                 bool application)
{
    return t->paused || input_held_up(p, application);
}

/* An END_OF_TRACK's tail wait is held up locally as a done's, and also
 * while the engine has a command the output queue has no room for, held
 * input or not: the END_OF_TRACK bounds where the track ends, not what has
 * arrived before it, and the wait for that is the peer's to use, not time
 * the application keeps the player from taking or releasing it. A queue
 * that is merely full, with the engine holding nothing, is no reason.
 * Other causes' waits run regardless. */
static bool tail_frozen_now(const moq_media_player_t *p, const player_track_t *t,
                            bool application)
{
    return t->cause == MOQ_MEDIA_PLAYER_END_PEER_END_OF_TRACK &&
           (t->paused || p->engine_backlog || input_held_up(p, application));
}

/* The receiver ended `t` for good, normally or with no done status: what
 * the peer delivered is final, but its END_OF_TRACK can still be behind on
 * the stream-terminal lane. Waits the tail wait for it; a cause arriving
 * meanwhile (END_OF_TRACK included) takes over, and its own wait applies. */
static void done_hint(moq_media_player_t *p, player_track_t *t, uint64_t now_us)
{
    if (t->end_emitted || t->done_hint) return;
    t->done_hint = true;
    t->done_wait_left_us = p->tail_us;
    t->done_wait_step_us = now_us;
    t->done_wait_frozen = done_wait_frozen_now(p, t, held_by_application(p));
}

static bool tail_open(const player_track_t *t)
{
    return t->tail_armed && !t->drain_requested && !t->end_emitted;
}

/* Charge `left` for the time since `step`, unless the interval was frozen,
 * and move `step` to `now`. */
static void wait_charge(uint64_t *left, uint64_t *step, bool frozen, uint64_t now_us)
{
    if (now_us <= *step) return;
    uint64_t elapsed = now_us - *step;
    if (!frozen) *left = elapsed >= *left ? 0 : *left - elapsed;
    *step = now_us;
}

/* Charge each bounded wait (a done's, a tail's) for the time since its last
 * mark, by the state recorded at the start of that interval, then record
 * the state from `now` on. Every operation that carries the caller's clock
 * marks: tick, before and after its work, and feedback, before and after
 * the pause it changes. An output poll carries no clock: the release it
 * causes is recorded at the next mark, so the interval up to it stays
 * frozen. The engine is asked about the held object afresh at every mark. */
static void waits_mark(moq_media_player_t *p, uint64_t now_us)
{
    bool asked = false, application = false;
    for (uint32_t i = 0; i < p->max_tracks; i++) {
        player_track_t *t = &p->tracks[i];
        if (!t->used) continue;
        bool done = done_hint_open(t), tail = tail_open(t);
        if (!done && !tail) continue;
        if (!asked) {
            application = held_by_application(p);
            asked = true;
        }
        if (done) {
            wait_charge(&t->done_wait_left_us, &t->done_wait_step_us,
                        t->done_wait_frozen, now_us);
            t->done_wait_frozen = done_wait_frozen_now(p, t, application);
        }
        if (tail) {
            wait_charge(&t->tail_left_us, &t->tail_step_us, t->tail_frozen, now_us);
            t->tail_frozen = tail_frozen_now(p, t, application);
        }
    }
}

/* The done's wait has run out: end the track as the peer's done,
 * END{PEER_DONE, TAIL_CUTOFF}, never COMPLETE. */
static bool done_hint_expired(const player_track_t *t)
{
    return done_hint_open(t) && t->done_wait_left_us == 0;
}

static uint32_t track_disposition(const player_track_t *t)
{
    if (cause_immediate(t->cause)) return MOQ_MEDIA_PLAYER_END_IMMEDIATE;
    if (t->cause == MOQ_MEDIA_PLAYER_END_PEER_END_OF_TRACK && !t->cut_off)
        return MOQ_MEDIA_PLAYER_END_COMPLETE;
    return MOQ_MEDIA_PLAYER_END_TAIL_CUTOFF;
}

/* -- Input ---------------------------------------------------------------- */

typedef enum { PUSH_TAKEN, PUSH_HOLD } push_outcome_t;

/* The engine's input for one object: `in` borrows `po`. */
static void engine_input(const player_track_t *t, const moq_media_object_t *obj,
                         moq_media_parsed_object_t *po, moq_playback_parsed_input_t *in)
{
    moq_media_parsed_object_init_sized(po, sizeof(*po));
    po->packaging = obj->packaging;
    po->status = obj->status;
    po->end_of_group = obj->end_of_group;
    po->datagram = obj->datagram;
    po->has_capture_time = obj->has_capture_time;
    po->capture_time_us = obj->capture_time_us;
    po->decode_time_us = obj->decode_time_us;
    po->composition_offset_us = obj->composition_offset_us;
    po->presentation_time_us = obj->presentation_time_us;
    po->keyframe = obj->keyframe;
    po->payload = obj->payload;
    po->sap_type = MOQ_SAP_UNKNOWN;
    if (obj->struct_size >= offsetof(moq_media_object_t, metadata) +
                            sizeof(moq_media_metadata_t))
        po->metadata = obj->metadata;
    moq_playback_parsed_input_init(in);
    in->track = t->eng;
    in->group_id = obj->group_id;
    in->subgroup_id = obj->subgroup_id;
    in->object_id = obj->object_id;
    in->publisher_priority = obj->publisher_priority;
    in->parsed = po;
    in->payload = obj->status == MOQ_OBJECT_NORMAL ? obj->payload_ref : NULL;
    in->timescale = t->timescale;
}

/* Hand one object to the engine. On PUSH_HOLD the caller keeps it. */
static push_outcome_t push_object(moq_media_player_t *p, player_track_t *t,
                                  moq_media_object_t *obj, uint64_t now_us)
{
    moq_media_parsed_object_t po;
    moq_playback_parsed_input_t in;
    engine_input(t, obj, &po, &in);
    moq_result_t rc = moq_playback_push_parsed(p->pb, &in, now_us);
    switch (rc) {
    case MOQ_OK:
        p->stats.objects_pushed++;
        break;
    case MOQ_ERR_WOULD_BLOCK:
    case MOQ_ERR_NOMEM:
        return PUSH_HOLD;
    case MOQ_ERR_LIMIT:
        player_refuse(p, t, MOQ_MEDIA_PLAYER_DROP_OVERSIZE, obj->group_id,
                      obj->object_id, now_us);
        break;
    case MOQ_ERR_WRONG_STATE:
        player_drop(p, t, MOQ_MEDIA_PLAYER_DROP_LATE_AFTER_END, obj->group_id,
                    obj->object_id);
        break;
    default:
        player_refuse(p, t, MOQ_MEDIA_PLAYER_DROP_MALFORMED, obj->group_id,
                      obj->object_id, now_us);
        break;
    }
    moq_media_object_cleanup(obj);
    return PUSH_TAKEN;
}

/* Whether the held object waits for admission on what the application
 * holds up and nothing else. Any other classification, one this player
 * does not know included, and a refused question, are no such wait. */
static bool held_by_application(moq_media_player_t *p)
{
    if (!p->has_held) return false;
    const player_track_t *t = &p->tracks[p->held.track];
    if (!t->eng_live) return false;
    moq_media_parsed_object_t po;
    moq_playback_parsed_input_t in;
    engine_input(t, &p->held.obj, &po, &in);
    moq_playback_admission_info_t info;
    if (moq_playback_get_admission(p->pb, &in, &info, sizeof(info)) != MOQ_OK)
        return false;
    return info.kind == MOQ_PLAYBACK_ADMISSION_APPLICATION;
}

/* Retry the held object; false while it is still held. */
static bool held_retry(moq_media_player_t *p, uint64_t now_us)
{
    if (!p->has_held) return true;
    player_track_t *t = &p->tracks[p->held.track];
    if (push_object(p, t, &p->held.obj, now_us) == PUSH_HOLD) return false;
    t->held = false;
    memset(&p->held, 0, sizeof(p->held));
    p->has_held = false;
    return true;
}

static void held_add(moq_media_player_t *p, uint32_t index,
                     moq_media_object_t *obj, uint64_t charge)
{
    p->held.track = index;
    p->held.charge = charge;
    p->held.obj = *obj;
    p->has_held = true;
    p->tracks[index].held = true;
}

/* One object from the receiver. One the engine has no room for is held,
 * unless it may not be (larger than a held object may be): then it is
 * dropped OVERSIZE. */
static void take_object(moq_media_player_t *p, moq_media_object_t *obj,
                        uint64_t charge, bool may_hold, uint64_t now_us)
{
    p->stats.objects_taken++;
    uint32_t index;
    player_track_t *t = track_of(p, obj->track, &index);
    if (!t) {
        p->stats.objects_discarded++;
        moq_media_object_cleanup(obj);
        return;
    }
    if (t->drained || t->end_owed || t->end_emitted) {
        player_drop(p, t, MOQ_MEDIA_PLAYER_DROP_LATE_AFTER_END, obj->group_id,
                    obj->object_id);
        moq_media_object_cleanup(obj);
        return;
    }
    bool has_md = obj->struct_size >= offsetof(moq_media_object_t, metadata) +
                                      sizeof(moq_media_metadata_t);
    if (has_md && obj->status == MOQ_OBJECT_NORMAL &&
        obj->metadata.time_domain != MOQ_MEDIA_TIME_DOMAIN_NONE &&
        obj->metadata.time_domain != t->domain) {
        player_refuse(p, t, MOQ_MEDIA_PLAYER_DROP_TIME_DOMAIN, obj->group_id,
                      obj->object_id, now_us);
        moq_media_object_cleanup(obj);
        return;
    }
    if (push_object(p, t, obj, now_us) != PUSH_HOLD) return;
    if (may_hold) {
        held_add(p, index, obj, charge);
        return;
    }
    player_refuse(p, t, MOQ_MEDIA_PLAYER_DROP_OVERSIZE, obj->group_id, obj->object_id,
                  now_us);
    moq_media_object_cleanup(obj);
}

static moq_playback_stream_terminal_kind_t fact_kind(moq_media_stream_terminal_kind_t k)
{
    switch (k) {
    case MOQ_MEDIA_STREAM_FIN_END_OF_GROUP: return MOQ_PLAYBACK_STREAM_FIN_END_OF_GROUP;
    case MOQ_MEDIA_STREAM_FIN:              return MOQ_PLAYBACK_STREAM_FIN;
    case MOQ_MEDIA_STREAM_RESET:            return MOQ_PLAYBACK_STREAM_RESET;
    case MOQ_MEDIA_STREAM_RECEIVER_DROP:    return MOQ_PLAYBACK_STREAM_RECEIVER_DROP;
    case MOQ_MEDIA_STREAM_END_OF_TRACK:     return MOQ_PLAYBACK_STREAM_END_OF_TRACK;
    default:                                return 0;
    }
}

/* One stream terminal from the receiver; false when the engine has no room
 * for it yet (the caller holds it). */
static bool take_terminal(moq_media_player_t *p, const moq_media_stream_terminal_t *rec,
                          uint64_t now_us)
{
    player_track_t *t = track_of(p, rec->track, NULL);
    moq_playback_stream_terminal_kind_t kind = fact_kind(rec->kind);
    if (!t || !kind || t->drained || t->end_owed || t->end_emitted) return true;
    moq_playback_stream_terminal_t f;
    memset(&f, 0, sizeof(f));
    f.struct_size = sizeof(f);
    f.kind = kind;
    f.group_id = rec->group_id;
    f.subgroup_id = rec->subgroup_id;
    f.object_id = rec->object_id;
    f.last_group_id = rec->kind == MOQ_MEDIA_STREAM_RECEIVER_DROP
                          ? rec->last_group_id : rec->group_id;
    f.error_code = rec->error_code;
    f.has_last_object = rec->has_last_object;
    f.last_object_id = rec->last_object_id;
    f.has_cut_object = rec->has_cut_object;
    f.cut_object_id = rec->cut_object_id;
    moq_result_t rc = moq_playback_on_stream_terminal(p->pb, t->eng, &f, now_us);
    if (rc == MOQ_ERR_WOULD_BLOCK) return false;
    if (rec->kind == MOQ_MEDIA_STREAM_RESET || rec->kind == MOQ_MEDIA_STREAM_RECEIVER_DROP) {
        moq_media_player_event_t ev = ev_make(MOQ_MEDIA_PLAYER_EVENT_LOSS, t->track,
                                              DETAIL(loss));
        ev.u.loss.kind = rec->kind;
        ev.u.loss.group_id = rec->group_id;
        ev.u.loss.last_group_id = f.last_group_id;
        ev.u.loss.error_code = rec->error_code;
        ev_push(p, &ev);
    }
    if (rec->kind == MOQ_MEDIA_STREAM_END_OF_TRACK)
        track_trigger(p, t, MOQ_MEDIA_PLAYER_END_PEER_END_OF_TRACK, now_us);
    return true;
}

/* Take the receiver's pollable stream terminals; false when one is held. */
static bool take_terminals(moq_media_player_t *p, uint64_t now_us)
{
    if (p->has_held_terminal) {
        if (!take_terminal(p, &p->held_terminal, now_us)) return false;
        p->has_held_terminal = false;
    }
    moq_media_stream_terminal_t rec;
    while (moq_media_receiver_poll_stream_terminal(p->r, &rec, sizeof(rec)) == MOQ_OK) {
        p->stats.terminals_taken++;
        if (!take_terminal(p, &rec, now_us)) {
            p->held_terminal = rec;
            p->has_held_terminal = true;
            return false;
        }
    }
    return true;
}

static void take_input(moq_media_player_t *p, uint64_t now_us)
{
    if (!refusal_retry(p, now_us)) return;
    if (!held_retry(p, now_us)) return;
    if (p->has_held_refusal) return;
    if (!take_terminals(p, now_us)) return;
    for (;;) {
        uint64_t charge = 0;
        moq_media_object_t obj;
        moq_result_t rc = moq_media_receiver_poll_object_within(p->r, p->max_held_bytes,
                                                                &charge, &obj, sizeof(obj));
        if (rc == MOQ_ERR_BUFFER) {
            /* Larger than a held object may be: it is taken only when the
             * engine accepts it at once. */
            rc = moq_media_receiver_poll_object_within(p->r, UINT64_MAX, &charge,
                                                       &obj, sizeof(obj));
            if (rc != MOQ_OK) return;
            take_object(p, &obj, charge, false, now_us);
        } else if (rc == MOQ_OK) {
            take_object(p, &obj, charge, true, now_us);
        } else {
            if (rc == MOQ_ERR_CLOSED) p->input_exhausted = true;
            return;
        }
        if (p->has_held || p->has_held_refusal) return;
        if (!take_terminals(p, now_us)) return;
    }
}

/* -- Track events --------------------------------------------------------- */

/* Room for one more announced track, made before the next track event is
 * taken, so a failed allocation leaves that event in the receiver for a
 * later tick instead of losing a handle. */
static bool known_reserve(moq_media_player_t *p)
{
    if (p->known_count < p->known_cap) return true;
    size_t cap = p->known_cap ? p->known_cap * 2 : 8;
    if (cap > SIZE_MAX / sizeof(*p->known)) return false;
    moq_media_track_t **k = (moq_media_track_t **)p->alloc.alloc(
        cap * sizeof(*k), p->alloc.ctx);
    if (!k) return false;
    if (p->known_count)
        memcpy(k, p->known, p->known_count * sizeof(*k));
    if (p->known)
        p->alloc.free(p->known, p->known_cap * sizeof(*p->known), p->alloc.ctx);
    p->known = k;
    p->known_cap = cap;
    return true;
}

static void known_add(moq_media_player_t *p, moq_media_track_t *track)
{
    for (size_t i = 0; i < p->known_count; i++)
        if (p->known[i] == track) return;
    p->known[p->known_count++] = track;          /* room reserved */
}

static void announce_available(moq_media_player_t *p, moq_media_track_t *track)
{
    if (track_of(p, track, NULL)) return;
    moq_media_player_event_t ev = ev_make(MOQ_MEDIA_PLAYER_EVENT_TRACK_AVAILABLE,
                                          track, 0);
    ev_push(p, &ev);
}

/* The catalog is ready: run AUTO when it is owed, then announce what is
 * not selected and report READY. A failed AUTO stays owed, for the next
 * tick, and nothing is announced before it ran. */
static void catalog_settle(moq_media_player_t *p)
{
    if (p->auto_pending) {
        if (auto_select(p) != MOQ_OK) return;
        p->auto_pending = false;
    }
    for (size_t i = 0; i < p->known_count; i++)
        announce_available(p, p->known[i]);
    moq_media_player_event_t ready = ev_make(MOQ_MEDIA_PLAYER_EVENT_READY, NULL, 0);
    ev_push(p, &ready);
}

static void take_track_events(moq_media_player_t *p, uint64_t now_us)
{
    moq_media_track_event_t ev;
    while (known_reserve(p) &&
           moq_media_receiver_poll_track(p->r, &ev, sizeof(ev)) == MOQ_OK) {
        player_track_t *t = ev.track ? track_of(p, ev.track, NULL) : NULL;
        switch (ev.kind) {
        case MOQ_MEDIA_TRACK_ADDED:
            known_add(p, ev.track);
            if (p->catalog_ready && !p->auto_pending) announce_available(p, ev.track);
            break;
        case MOQ_MEDIA_CATALOG_READY:
            if (p->catalog_ready) break;
            p->catalog_ready = true;
            (void)player_refresh_version(p);
            p->auto_pending = p->selection == MOQ_MEDIA_SELECTION_AUTO;
            catalog_settle(p);
            break;
        case MOQ_MEDIA_TRACK_REMOVED: {
            moq_media_player_event_t rm =
                ev_make(MOQ_MEDIA_PLAYER_EVENT_TRACK_REMOVED, ev.track, 0);
            ev_push(p, &rm);
            /* A completed broadcast (MSF 11.3) removes every track; for one
             * the peer has already ended, that removal is not news. Its end
             * is its END_OF_TRACK, COMPLETE only if every location up to it
             * is accounted for, or the normal done's bounded wait. Why the
             * track was removed is what the receiver recorded at the
             * removal, not the current catalog: a withdrawal stays a cause
             * even when a terminal catalog follows before this is read. */
            if (t && !(t->peer_ended &&
                       moq_media_receiver_track_removed_by_completion_internal(
                           p->r, ev.track)))
                track_trigger(p, t, MOQ_MEDIA_PLAYER_END_REMOVED, now_us);
            break;
        }
        case MOQ_MEDIA_TRACK_ENDED:
            /* An END_OF_TRACK comes on the stream-terminal lane too, ordered
             * with the media. An error done is a cause; a normal done, or an
             * end with no done status (a refused re-subscription), is not:
             * it bounds the wait for an END_OF_TRACK that may never come. */
            if (!t) break;
            if (ev.has_done_status &&
                ev.done_status != PLAYER_DONE_TRACK_ENDED &&
                ev.done_status != PLAYER_DONE_SUBSCRIPTION_ENDED) {
                track_trigger(p, t, MOQ_MEDIA_PLAYER_END_PEER_DONE, now_us);
            } else {
                t->peer_ended = true;
                done_hint(p, t, now_us);
            }
            break;
        case MOQ_MEDIA_TRACK_DELIVERY_LOCAL_FAILURE:
            /* A fill child's failure ends the fill, not its track: what the
             * fill would have delivered is missing like any other unit, and
             * the engine's wait on it reports it. */
            if (ev.delivery_origin == MOQ_MEDIA_DELIVERY_FILL) break;
            if (t) track_trigger(p, t, MOQ_MEDIA_PLAYER_END_LOCAL_FAILURE, now_us);
            break;
        case MOQ_MEDIA_TRACK_LOCAL_FAILURE:
            if (t) track_trigger(p, t, MOQ_MEDIA_PLAYER_END_LOCAL_FAILURE, now_us);
            break;
        default:
            break;
        }
    }
}

static void discard_timeline_records(moq_media_player_t *p)
{
    moq_media_sap_record_t sap;
    while (moq_media_receiver_poll_sap(p->r, &sap, sizeof(sap)) == MOQ_OK)
        p->stats.timeline_records_discarded++;
    moq_media_timeline_record_t mt;
    while (moq_media_receiver_poll_media_timeline(p->r, &mt, sizeof(mt)) == MOQ_OK)
        p->stats.timeline_records_discarded++;
}

/* -- Engine output --------------------------------------------------------- */

static uint32_t discontinuity_reason(moq_playback_reset_reason_t r)
{
    return r == MOQ_PLAYBACK_RESET_DECODE_ERROR ? MOQ_MEDIA_PLAYER_DISCONTINUITY_DECODE_ERROR
                                                : MOQ_MEDIA_PLAYER_DISCONTINUITY_GAP;
}

static void emit_unit(moq_media_player_t *p, player_track_t *t,
                      moq_playback_cmd_t *c, moq_playback_command_hold_t hold,
                      uint64_t now_us)
{
    bool video = c->kind == MOQ_PLAYBACK_CMD_DECODE_VIDEO;
    uint64_t g = video ? c->u.decode_video.group_id : c->u.decode_audio.group_id;
    uint64_t o = video ? c->u.decode_video.object_id : c->u.decode_audio.object_id;
    if (!t->config_seen) {
        player_drop(p, t, MOQ_MEDIA_PLAYER_DROP_CONFIGURATION_MISSING, g, o);
        moq_playback_release_command_hold(p->pb, hold);
        moq_playback_cmd_cleanup(c);
        return;
    }
    uint64_t pts = video ? c->u.decode_video.presentation_time_us
                         : c->u.decode_audio.presentation_time_us;
    player_timeline_t *tl = &p->timelines[t->timeline];
    if (!tl->anchored) {
        tl->anchored = true;
        tl->anchor_media_us = pts;
        tl->anchor_local_us = now_us > UINT64_MAX - p->cushion_us ? UINT64_MAX
                                                                  : now_us + p->cushion_us;
        moq_media_player_output_t *to = out_push(p, MOQ_MEDIA_PLAYER_OUTPUT_TIMELINE, t,
                                                 OUT_DETAIL(timeline));
        to->u.timeline.domain = tl->domain;
        to->u.timeline.timeline_id = tl->id;
        to->u.timeline.timeline_epoch = 1;
        to->u.timeline.anchor_media_us = tl->anchor_media_us;
        to->u.timeline.anchor_local_us = tl->anchor_local_us;
        to->u.timeline.rate = 1.0;
        moq_media_player_event_t st = ev_make(MOQ_MEDIA_PLAYER_EVENT_STARTED, t->track,
                                              DETAIL(started));
        st.u.started.timeline_id = tl->id;
        ev_push(p, &st);
    }
    moq_media_player_output_t *u = out_push(p, MOQ_MEDIA_PLAYER_OUTPUT_UNIT, t,
                                            OUT_DETAIL(unit));
    u->u.unit.group_id = g;
    u->u.unit.object_id = o;
    u->u.unit.present = true;
    if (video) {
        u->u.unit.decode_time_us = c->u.decode_video.decode_time_us;
        u->u.unit.presentation_time_us = c->u.decode_video.presentation_time_us;
        u->u.unit.composition_offset_us = c->u.decode_video.composition_offset_us;
        u->u.unit.has_capture_time = c->u.decode_video.has_capture_time;
        u->u.unit.capture_time_us = c->u.decode_video.capture_time_us;
        u->u.unit.independence_known = c->u.decode_video.keyframe;
        u->u.unit.independent = c->u.decode_video.keyframe;
        u->u.unit.payload = c->u.decode_video.payload;
        c->u.decode_video.payload = NULL;
    } else {
        u->u.unit.decode_time_us = c->u.decode_audio.decode_time_us;
        u->u.unit.presentation_time_us = c->u.decode_audio.presentation_time_us;
        u->u.unit.composition_offset_us = c->u.decode_audio.composition_offset_us;
        u->u.unit.has_capture_time = c->u.decode_audio.has_capture_time;
        u->u.unit.capture_time_us = c->u.decode_audio.capture_time_us;
        u->u.unit.sample_duration_us = c->u.decode_audio.sample_duration_us;
        u->u.unit.payload = c->u.decode_audio.payload;
        c->u.decode_audio.payload = NULL;
    }
    u->u.unit.offset = 0;
    u->u.unit.len = u->u.unit.payload ? moq_rcbuf_len(u->u.unit.payload) : 0;
    out_keep_hold(p, u, hold);
    p->stats.units_output++;
    moq_playback_cmd_cleanup(c);
}

/* One engine command, polled under `hold`: the output that takes its refs
 * keeps the hold; a command that becomes no output, or one without refs,
 * releases it. */
static void emit_command(moq_media_player_t *p, moq_playback_cmd_t *c,
                         moq_playback_command_hold_t hold, uint64_t now_us)
{
    player_track_t *t = track_of_engine(p, c->track);
    if (!t) {
        moq_playback_release_command_hold(p->pb, hold);
        moq_playback_cmd_cleanup(c);
        return;
    }
    switch (c->kind) {
    case MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO:
    case MOQ_PLAYBACK_CMD_CONFIGURE_AUDIO: {
        bool video = c->kind == MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO;
        t->config_gen++;
        moq_media_player_output_t *o = out_push(p, MOQ_MEDIA_PLAYER_OUTPUT_CONFIGURE, t,
                                                OUT_DETAIL(configure));
        o->u.configure.media_type = t->media_type;
        o->u.configure.packaging = (uint32_t)MOQ_MEDIA_PACKAGING_RAW;
        o->u.configure.timescale = t->timescale;
        if (video) {
            o->u.configure.codec = c->u.configure_video.codec;
            o->u.configure.codec_config = c->u.configure_video.codec_config;
            o->u.configure.width = c->u.configure_video.width;
            o->u.configure.height = c->u.configure_video.height;
            c->u.configure_video.codec = NULL;
            c->u.configure_video.codec_config = NULL;
        } else {
            o->u.configure.codec = c->u.configure_audio.codec;
            o->u.configure.codec_config = c->u.configure_audio.codec_config;
            o->u.configure.samplerate = c->u.configure_audio.samplerate;
            o->u.configure.channel_count = c->u.configure_audio.channel_count;
            c->u.configure_audio.codec = NULL;
            c->u.configure_audio.codec_config = NULL;
        }
        if (o->u.configure.codec_config && moq_rcbuf_len(o->u.configure.codec_config) > 0)
            t->config_seen = true;
        out_keep_hold(p, o, hold);
        p->stats.configures_output++;
        moq_playback_cmd_cleanup(c);
        return;
    }
    case MOQ_PLAYBACK_CMD_DECODE_VIDEO:
    case MOQ_PLAYBACK_CMD_DECODE_AUDIO:
        emit_unit(p, t, c, hold, now_us);
        return;
    case MOQ_PLAYBACK_CMD_RESET: {
        t->config_gen++;
        moq_media_player_output_t *o = out_push(p, MOQ_MEDIA_PLAYER_OUTPUT_DISCONTINUITY,
                                                t, OUT_DETAIL(discontinuity));
        o->u.discontinuity.reason = discontinuity_reason(c->u.reset.reason);
        o->u.discontinuity.decoder_reset_required = true;
        p->stats.discontinuities_output++;
        moq_playback_release_command_hold(p->pb, hold);
        moq_playback_cmd_cleanup(c);
        return;
    }
    default:
        moq_playback_release_command_hold(p->pb, hold);
        moq_playback_cmd_cleanup(c);
        return;
    }
}

/* An engine report: the track's state changes since its first trigger
 * decide whether its end was complete. */
static void emit_report(moq_media_player_t *p, const moq_playback_event_t *e)
{
    player_track_t *t = track_of_engine(p, e->track);
    if (!t) return;
    bool gave_up = false;
    moq_media_player_event_t ev;
    switch (e->kind) {
    case MOQ_PLAYBACK_EVENT_GAP_DETECTED:
        /* Announces a step that may follow; the step's own report
         * (SKIP_FORWARD, PARTIAL_GROUP_ABANDONED, GAP_SKIPPED_IN_GROUP) is
         * the loss. */
        ev = ev_make(MOQ_MEDIA_PLAYER_EVENT_GAP, t->track, DETAIL(gap));
        ev.u.gap.group_id = e->u.gap_detected.group_id;
        ev.u.gap.object_id = e->u.gap_detected.object_id;
        ev.u.gap.cause = e->u.gap_detected.cause;
        ev_push(p, &ev);
        break;
    case MOQ_PLAYBACK_EVENT_GAP_CONTINUED:
        ev = ev_make(MOQ_MEDIA_PLAYER_EVENT_GAP, t->track, DETAIL(gap));
        ev.u.gap.crossed = true;
        ev.u.gap.hole_start_us = e->u.gap_continued.hole_start_us;
        ev.u.gap.hole_duration_us = e->u.gap_continued.hole_duration_us;
        ev_push(p, &ev);
        gave_up = true;
        break;
    case MOQ_PLAYBACK_EVENT_KEYFRAME_WAITING:
        ev = ev_make(MOQ_MEDIA_PLAYER_EVENT_STALL, t->track, 0);
        ev_push(p, &ev);
        break;
    case MOQ_PLAYBACK_EVENT_SKIP_FORWARD:
        ev = ev_make(MOQ_MEDIA_PLAYER_EVENT_SKIP, t->track, DETAIL(skip));
        ev.u.skip.from_group_id = e->u.skip_forward.from_group_id;
        ev.u.skip.to_group_id = e->u.skip_forward.to_group_id;
        ev_push(p, &ev);
        gave_up = true;
        break;
    case MOQ_PLAYBACK_EVENT_PARTIAL_GROUP_ABANDONED:
        ev = ev_make(MOQ_MEDIA_PLAYER_EVENT_SKIP, t->track, DETAIL(skip));
        ev.u.skip.from_group_id = e->u.partial_group_abandoned.from_group_id;
        ev.u.skip.to_group_id = e->u.partial_group_abandoned.to_group_id;
        ev_push(p, &ev);
        gave_up = true;
        break;
    case MOQ_PLAYBACK_EVENT_BACKLOG_SHED:
        ev = ev_make(MOQ_MEDIA_PLAYER_EVENT_SHED, t->track, DETAIL(shed));
        ev.u.shed.dropped_groups = e->u.backlog_shed.dropped_groups;
        ev.u.shed.remaining_groups = e->u.backlog_shed.remaining_groups;
        ev_push(p, &ev);
        gave_up = true;
        break;
    case MOQ_PLAYBACK_EVENT_OBJECT_DROPPED:
        ev_drop(p, t->track, e->u.object_dropped.reason, e->u.object_dropped.group_id,
                e->u.object_dropped.object_id);
        gave_up = e->u.object_dropped.reason != MOQ_PLAYBACK_DROP_STALE;
        break;
    case MOQ_PLAYBACK_EVENT_KNOWLEDGE_FORGOTTEN:
        ev = ev_make(MOQ_MEDIA_PLAYER_EVENT_KNOWLEDGE_FORGOTTEN, t->track,
                     DETAIL(knowledge_forgotten));
        ev.u.knowledge_forgotten.first_group_id = e->u.knowledge_forgotten.first_group_id;
        ev.u.knowledge_forgotten.last_group_id = e->u.knowledge_forgotten.last_group_id;
        ev_push(p, &ev);
        break;
    case MOQ_PLAYBACK_EVENT_GAP_SKIPPED_IN_GROUP:
        /* The settlement GAP_DETECTED announced, reported by its GAP event
         * and the DISCONTINUITY before the resumed unit. */
        gave_up = true;
        break;
    case MOQ_PLAYBACK_EVENT_TRACK_ENDED:
        t->end_owed = true;
        break;
    default:
        break;
    }
    if (gave_up && t->cause) t->cut_off = true;
}

/* Take every report the engine has made. */
static void settle_reports(moq_media_player_t *p)
{
    moq_playback_event_t e;
    while (moq_playback_poll_event(p->pb, &e, sizeof(e)) == MOQ_OK)
        emit_report(p, &e);
}

/* Emit owed ENDs; false when the output queue has no room. */
static bool emit_ends(moq_media_player_t *p)
{
    for (uint32_t i = 0; i < p->max_tracks; i++) {
        player_track_t *t = &p->tracks[i];
        if (!t->used || !t->end_owed || t->end_emitted) continue;
        if (out_free(p) == 0) return false;
        if (!t->cause) t->cause = MOQ_MEDIA_PLAYER_END_PEER_END_OF_TRACK;
        moq_media_player_output_t *o = out_push(p, MOQ_MEDIA_PLAYER_OUTPUT_END, t,
                                                (uint32_t)sizeof(((moq_media_player_output_t *)0)->u.end));
        o->u.end.cause = t->cause;
        o->u.end.disposition = track_disposition(t);
        t->end_owed = false;
        t->end_emitted = true;
        t->tail_armed = false;
        t->done_hint = false;
        p->stats.ends_output++;
        if (t->eng_live) {
            (void)moq_playback_remove_track(p->pb, t->eng);
            t->eng_live = false;
        }
    }
    return true;
}

/* The engine can hold no more commands: every track still running ends
 * now, LOCAL_FAILURE, and so does any track selected later. */
static void holds_exhausted(moq_media_player_t *p, uint64_t now_us)
{
    p->holds_exhausted = true;
    for (uint32_t i = 0; i < p->max_tracks; i++) {
        player_track_t *t = &p->tracks[i];
        if (t->used && !t->end_emitted)
            track_trigger(p, t, MOQ_MEDIA_PLAYER_END_LOCAL_FAILURE, now_us);
    }
}

/* Move the engine's commands to the output queue and settle its reports;
 * owed ENDs follow once the engine holds no more commands. A command the
 * queue has no room for is taken anyway, under its hold, and staged: it
 * proves the backlog and goes out first once there is room. False while one
 * is staged. */
static bool drain_engine(moq_media_player_t *p, uint64_t now_us)
{
    p->engine_backlog = false;
    for (;;) {
        if (p->has_staged) {
            if (out_free(p) < 2) {
                p->engine_backlog = true;
                break;
            }
            p->has_staged = false;
            emit_command(p, &p->staged, p->staged_hold, now_us);
            continue;
        }
        moq_playback_cmd_t c;
        if (p->holds_exhausted) {
            /* What can no longer be held is never queued: it is discarded,
             * and its track's end is a local failure. */
            if (moq_playback_poll_command(p->pb, &c, sizeof(c)) != MOQ_OK) break;
            moq_playback_cmd_cleanup(&c);
            continue;
        }
        moq_playback_command_hold_t hold = MOQ_PLAYBACK_COMMAND_HOLD_INVALID;
        moq_result_t rc = moq_playback_poll_command_held(p->pb, &c, sizeof(c), &hold);
        if (rc == MOQ_ERR_UNSUPPORTED || rc == MOQ_ERR_LIMIT) {
            /* Permanently: holds are spent (or the command is one no hold
             * can be taken for, which RAW selection excludes). An unheld
             * output would leave the budget, so none is made. */
            holds_exhausted(p, now_us);
            continue;
        }
        if (rc != MOQ_OK) break;
        if (out_free(p) < 2) {
            p->staged = c;
            p->staged_hold = hold;
            p->has_staged = true;
            p->engine_backlog = true;
            break;
        }
        emit_command(p, &c, hold, now_us);
    }
    settle_reports(p);
    if (p->engine_backlog) return false;
    return emit_ends(p);
}

/* Drop the staged command if `t` owns it (NULL: whoever does): its hold
 * ends and nothing of it goes out. The backlog it proved goes with it; the
 * next drain looks again. */
static void staged_drop(moq_media_player_t *p, const player_track_t *t)
{
    if (!p->has_staged) return;
    if (t && (!t->eng_live || p->staged.track._opaque != t->eng._opaque)) return;
    moq_playback_release_command_hold(p->pb, p->staged_hold);
    moq_playback_cmd_cleanup(&p->staged);
    memset(&p->staged, 0, sizeof(p->staged));
    p->staged_hold = MOQ_PLAYBACK_COMMAND_HOLD_INVALID;
    p->has_staged = false;
    p->engine_backlog = false;
}

/* -- Tick ------------------------------------------------------------------ */

static bool engine_holds(const moq_media_player_t *p)
{
    for (uint32_t i = 0; i < p->max_tracks; i++) {
        const player_track_t *t = &p->tracks[i];
        if (t->used && !t->end_emitted) return true;
    }
    return false;
}

/* The receiver closed or failed: what it still has is the track's input,
 * taken before its drain. */
static bool drain_needs_input_end(const player_track_t *t)
{
    return t->cause == MOQ_MEDIA_PLAYER_END_CLOSED || t->cause == MOQ_MEDIA_PLAYER_END_FATAL;
}

/* Whether a track whose drain was requested may be drained now: the player
 * holds nothing more for it, and the receiver no input it admitted for it.
 * Intake stops behind any held object or fact, so another track's hold can
 * leave this one's admitted objects queued; they are taken first. For
 * CLOSED and FATAL the receiver's whole input ends first. */
static bool drain_ready(const moq_media_player_t *p, const player_track_t *t)
{
    if (t->held) return false;
    if (p->has_held_refusal && &p->tracks[p->held_refusal.track] == t) return false;
    if (drain_needs_input_end(t)) return p->input_exhausted;
    return moq_media_receiver_track_input_internal(p->r, t->track) == 0;
}

/* A track whose end must drain stops waiting at once (settled), so the
 * engine makes room for what the player still holds and no gap blocks it;
 * it is drained, and so can end, once drain_ready. A receiver that closed
 * or failed raises every track's cause until its END is queued, drained or
 * not. */
static void track_drains(moq_media_player_t *p, uint64_t now_us)
{
    for (uint32_t i = 0; i < p->max_tracks; i++) {
        player_track_t *t = &p->tracks[i];
        if (!t->used || t->end_emitted) continue;
        if (p->holds_exhausted)
            track_trigger(p, t, MOQ_MEDIA_PLAYER_END_LOCAL_FAILURE, now_us);
        if (p->receiver_terminal)
            track_trigger(p, t, p->fatal ? MOQ_MEDIA_PLAYER_END_FATAL
                                         : MOQ_MEDIA_PLAYER_END_CLOSED, now_us);
        if (t->drained || t->end_owed) continue;
        if (done_hint_expired(t)) {
            settle_reports(p);
            t->cause = MOQ_MEDIA_PLAYER_END_PEER_DONE;
            t->cut_off = true;
            t->drain_requested = true;
        }
        if (t->tail_armed && !t->drain_requested && t->tail_left_us == 0) {
            t->cut_off = true;
            t->drain_requested = true;
        }
        if (!t->drain_requested) continue;
        if (!t->settled) {
            if (t->eng_live) (void)moq_playback_settle_track(p->pb, t->eng);
            t->settled = true;
        }
        if (!drain_ready(p, t)) continue;
        if (t->eng_live) (void)moq_playback_drain_track(p->pb, t->eng);
        t->drained = true;
    }
}

static void report_terminal(moq_media_player_t *p)
{
    bool any = false, all = true;
    for (uint32_t i = 0; i < p->max_tracks; i++) {
        if (!p->tracks[i].used) continue;
        any = true;
        if (!p->tracks[i].end_emitted) all = false;
    }
    if (any && all && !p->ended_reported) {
        p->ended_reported = true;
        moq_media_player_event_t ev = ev_make(MOQ_MEDIA_PLAYER_EVENT_ENDED, NULL, 0);
        ev_push(p, &ev);
    }
    if (p->receiver_terminal && p->input_exhausted && !engine_holds(p) &&
        !p->has_held && !p->closed_reported) {
        p->closed_reported = true;
        moq_media_player_event_t ev = ev_make(MOQ_MEDIA_PLAYER_EVENT_CLOSED, NULL, 0);
        ev_push(p, &ev);
    }
}

moq_result_t moq_media_player_tick(moq_media_player_t *p, uint64_t now_us)
{
    if (!p) return MOQ_ERR_INVAL;
    if (moq_media_player_is_closed(p)) return MOQ_ERR_CLOSED;
    waits_mark(p, now_us);
    if (p->auto_pending) catalog_settle(p);
    take_track_events(p, now_us);
    discard_timeline_records(p);
    if (!p->receiver_terminal &&
        (moq_media_receiver_is_closed(p->r) || moq_media_receiver_is_fatal(p->r))) {
        p->receiver_terminal = true;
        p->fatal = moq_media_receiver_is_fatal(p->r);
        p->fatal_code = moq_media_receiver_fatal_code(p->r);
    }
    take_input(p, now_us);
    track_drains(p, now_us);
    bool room = true;
    for (int round = 0; round < 8; round++) {
        moq_result_t rc = moq_playback_tick(p->pb, now_us);
        room = drain_engine(p, now_us);
        if (!room || rc != MOQ_ERR_WOULD_BLOCK) break;
    }
    /* Room made in the engine lets held input go in, which may release
     * more this tick. */
    if (room && (p->has_held || p->has_held_refusal)) {
        take_input(p, now_us);
        track_drains(p, now_us);
        (void)moq_playback_tick(p->pb, now_us);
        room = drain_engine(p, now_us);
    }
    report_terminal(p);
    waits_mark(p, now_us);
    return room ? MOQ_OK : MOQ_ERR_WOULD_BLOCK;
}

uint64_t moq_media_player_next_deadline_us(const moq_media_player_t *p)
{
    if (!p) return UINT64_MAX;
    uint64_t d = moq_playback_next_deadline_us(p->pb);
    for (uint32_t i = 0; i < p->max_tracks; i++) {
        const player_track_t *t = &p->tracks[i];
        if (t->used && tail_open(t) && !t->tail_frozen) {
            uint64_t due = t->tail_step_us > UINT64_MAX - t->tail_left_us
                               ? UINT64_MAX : t->tail_step_us + t->tail_left_us;
            if (due < d) d = due;
        }
        if (t->used && done_hint_open(t) && !t->done_wait_frozen) {
            uint64_t due = t->done_wait_step_us > UINT64_MAX - t->done_wait_left_us
                               ? UINT64_MAX : t->done_wait_step_us + t->done_wait_left_us;
            if (due < d) d = due;
        }
    }
    return d;
}

/* Terminal work tick can do now: an END to emit, a settle or drain to
 * start, a higher cause to apply. */
static bool drain_due(const moq_media_player_t *p)
{
    for (uint32_t i = 0; i < p->max_tracks; i++) {
        const player_track_t *t = &p->tracks[i];
        if (!t->used || t->end_emitted) continue;
        if (t->end_owed) return true;
        if (p->receiver_terminal &&
            t->cause < (p->fatal ? MOQ_MEDIA_PLAYER_END_FATAL : MOQ_MEDIA_PLAYER_END_CLOSED))
            return true;
        if (t->drained) continue;
        if (t->drain_requested && !t->settled) return true;
        if (t->drain_requested && drain_ready(p, t)) return true;
    }
    return false;
}

static bool paused_work(const moq_media_player_t *p)
{
    for (uint32_t i = 0; i < p->max_tracks; i++) {
        const player_track_t *t = &p->tracks[i];
        if (t->used && t->paused && !t->end_emitted) return true;
    }
    return false;
}

moq_result_t moq_media_player_wait(moq_media_player_t *p, uint64_t now_us,
                                   uint64_t timeout_us)
{
    if (!p) return MOQ_ERR_INVAL;
    moq_endpoint_t *ep = moq_media_receiver_endpoint_internal(p->r);
    if (ep && moq_endpoint_interrupted_internal(ep)) return MOQ_ERR_INTERRUPTED;
    if (p->out_count > 0 || p->ev_count > 0) return MOQ_OK;
    if (moq_media_player_is_closed(p)) return MOQ_ERR_CLOSED;
    if (p->engine_backlog) return MOQ_OK;
    uint64_t d = moq_media_player_next_deadline_us(p);
    if (d <= now_us) return MOQ_OK;
    if (drain_due(p)) return MOQ_OK;
    bool receiver_terminal = p->receiver_terminal ||
        moq_media_receiver_is_closed(p->r) || moq_media_receiver_is_fatal(p->r);
    if (receiver_terminal && !p->receiver_terminal) return MOQ_OK;
    /* Input tick can take is work, whatever else waits on feedback. */
    if (!p->has_held && !p->has_held_terminal && !p->has_held_refusal &&
        moq_media_receiver_has_input_internal(p->r))
        return MOQ_OK;
    /* Work only decoder feedback can advance waits on the application,
     * unless a deadline of other work comes first. */
    if (paused_work(p) && d == UINT64_MAX) return MOQ_DONE;
    if (receiver_terminal && p->input_exhausted) return MOQ_DONE;
    uint64_t sleep = timeout_us;
    bool deadline_first = false;
    if (d != UINT64_MAX && d - now_us < sleep) {
        sleep = d - now_us;
        deadline_first = true;
    }
    moq_result_t rc;
    if (p->has_held || p->has_held_terminal || p->has_held_refusal) {
        /* Taking more input waits for engine room, which only polling
         * outputs or a deadline makes; the receiver's queued input is not
         * work yet. */
        if (!ep) return deadline_first ? MOQ_OK : MOQ_DONE;
        rc = moq_endpoint_wait(ep, sleep);
        if (rc == MOQ_DONE) return deadline_first ? MOQ_OK : MOQ_DONE;
        if (rc == MOQ_OK || rc == MOQ_ERR_CLOSED) return MOQ_OK;
        return rc;
    }
    rc = moq_media_receiver_wait(p->r, sleep);
    if (rc == MOQ_DONE) return deadline_first ? MOQ_OK : MOQ_DONE;
    /* A receiver that became terminal is work for tick. */
    if (rc == MOQ_OK || rc == MOQ_ERR_CLOSED) return MOQ_OK;
    return rc;
}

/* -- Polls ----------------------------------------------------------------- */

moq_result_t moq_media_player_poll_output(moq_media_player_t *p,
                                          moq_media_player_output_t *out,
                                          size_t out_size)
{
    if (!p || !out) return MOQ_ERR_INVAL;
    if (out_size < sizeof(*out)) return MOQ_ERR_ABI_MISMATCH;
    if (p->out_count == 0)
        return moq_media_player_is_closed(p) ? MOQ_ERR_CLOSED : MOQ_DONE;
    moq_media_player_output_t *o = &p->outputs[p->out_head];
    *out = *o;
    memset(o, 0, sizeof(*o));
    out_release_hold(p, p->out_head);
    p->out_head = (p->out_head + 1) % p->out_cap;
    p->out_count--;
    return MOQ_OK;
}

moq_result_t moq_media_player_poll_event(moq_media_player_t *p,
                                         moq_media_player_event_t *ev,
                                         size_t ev_size)
{
    if (!p || !ev) return MOQ_ERR_INVAL;
    if (ev_size < sizeof(*ev)) return MOQ_ERR_ABI_MISMATCH;
    if (p->ev_count == 0)
        return moq_media_player_is_closed(p) ? MOQ_ERR_CLOSED : MOQ_DONE;
    *ev = p->events[p->ev_head % p->ev_cap];
    p->ev_head++;
    p->ev_count--;
    return MOQ_OK;
}

/* -- Feedback -------------------------------------------------------------- */

/* Unpolled outputs a decoder reset makes moot: the track's units and
 * configurations. Its timeline and END stay. */
static void outputs_drop_decode(moq_media_player_t *p, const player_track_t *t)
{
    uint32_t write = 0;
    for (uint32_t i = 0; i < p->out_count; i++) {
        uint32_t ri = (p->out_head + i) % p->out_cap;
        moq_media_player_output_t *o = &p->outputs[ri];
        bool moot = o->track == t->track &&
                    (o->kind == MOQ_MEDIA_PLAYER_OUTPUT_UNIT ||
                     o->kind == MOQ_MEDIA_PLAYER_OUTPUT_CONFIGURE ||
                     o->kind == MOQ_MEDIA_PLAYER_OUTPUT_DISCONTINUITY);
        if (moot) {
            if (o->kind == MOQ_MEDIA_PLAYER_OUTPUT_UNIT && p->stats.units_output > 0)
                p->stats.units_output--;
            moq_media_player_output_cleanup(o);
            memset(o, 0, sizeof(*o));
            out_release_hold(p, ri);
            continue;
        }
        uint32_t wi = (p->out_head + write) % p->out_cap;
        moq_media_player_output_t *w = &p->outputs[wi];
        if (w != o) {
            *w = *o;
            memset(o, 0, sizeof(*o));
            p->out_hold[wi] = p->out_hold[ri];
            p->out_hold[ri] = MOQ_PLAYBACK_COMMAND_HOLD_INVALID;
        }
        write++;
    }
    p->out_count = write;
}

moq_result_t moq_media_player_handle_feedback(moq_media_player_t *p,
                                              const moq_media_player_feedback_t *fb,
                                              uint64_t now_us)
{
    if (!p || !fb) return MOQ_ERR_INVAL;
    if (fb->struct_size < MOQ_MEDIA_PLAYER_FEEDBACK_V0_SIZE) return MOQ_ERR_INVAL;
    player_track_t *t = track_of(p, fb->track, NULL);
    if (!t) return MOQ_ERR_INVAL;
    switch (fb->kind) {
    case MOQ_MEDIA_PLAYER_FEEDBACK_DECODER_QUEUE:
    case MOQ_MEDIA_PLAYER_FEEDBACK_DECODE_ERROR:
        break;
    case MOQ_MEDIA_PLAYER_FEEDBACK_FLUSH_COMPLETE:
        return MOQ_OK;
    default:
        return MOQ_ERR_INVAL;
    }
    if (fb->configuration_generation != t->config_gen) {
        p->stats.feedback_stale++;
        return MOQ_OK;
    }
    if (!t->eng_live) return MOQ_OK;
    moq_playback_feedback_t ef;
    moq_playback_feedback_init(&ef);
    ef.track = t->eng;
    if (fb->kind == MOQ_MEDIA_PLAYER_FEEDBACK_DECODER_QUEUE) {
        ef.kind = MOQ_PLAYBACK_FEEDBACK_QUEUE_PRESSURE;
        ef.u.queue_pressure.depth = fb->u.decoder_queue.depth;
        ef.u.queue_pressure.max_recommended = fb->u.decoder_queue.max_recommended;
        moq_result_t rc = moq_playback_handle_feedback(p->pb, &ef, now_us);
        if (rc != MOQ_OK) return rc;
        /* The pause changes only from now: charge the interval before it. */
        waits_mark(p, now_us);
        t->paused = fb->u.decoder_queue.depth > fb->u.decoder_queue.max_recommended;
        waits_mark(p, now_us);
        return MOQ_OK;
    }
    ef.kind = MOQ_PLAYBACK_FEEDBACK_DECODE_ERROR;
    ef.u.decode_error.message = fb->u.decode_error.message;
    moq_result_t rc = moq_playback_handle_feedback(p->pb, &ef, now_us);
    if (rc != MOQ_OK) return rc;
    staged_drop(p, t);
    outputs_drop_decode(p, t);
    return MOQ_OK;
}

/* -- Observing ------------------------------------------------------------- */

bool moq_media_player_is_closed(const moq_media_player_t *p)
{
    if (!p) return false;
    return p->closed_reported && p->out_count == 0 && p->ev_count == 0;
}

bool moq_media_player_is_fatal(const moq_media_player_t *p)
{
    return p && p->fatal;
}

uint64_t moq_media_player_fatal_code(const moq_media_player_t *p)
{
    return p ? p->fatal_code : 0;
}

moq_media_receiver_t *moq_media_player_receiver(const moq_media_player_t *p)
{
    return p ? p->r : NULL;
}

moq_result_t moq_media_player_get_stats(const moq_media_player_t *p,
                                        moq_media_player_stats_t *out,
                                        size_t out_size)
{
    if (!p || !out) return MOQ_ERR_INVAL;
    if (out_size < MOQ_MEDIA_PLAYER_STATS_V0_SIZE) return MOQ_ERR_ABI_MISMATCH;
    moq_media_player_stats_t s = p->stats;
    s.outputs_queued = p->out_count;
    s.events_queued = p->ev_count;
    s.held_objects = p->has_held ? 1 : 0;
    s.held_bytes = p->has_held ? p->held.charge : 0;
    s.outputs_bytes = moq_playback_command_hold_bytes(p->pb);
    size_t n = out_size < sizeof(s) ? out_size : sizeof(s);
    s.struct_size = (uint32_t)n;
    memcpy(out, &s, n);
    return MOQ_OK;
}

/* -- Lifecycle ------------------------------------------------------------- */

static moq_result_t player_cfg_read(const moq_media_player_cfg_t *in,
                                    moq_media_player_cfg_t *c)
{
    if (!in || in->struct_size < MOQ_MEDIA_PLAYER_CFG_V0_SIZE) return MOQ_ERR_INVAL;
    moq_media_player_cfg_init(c);
    size_t n = in->struct_size < sizeof(*c) ? in->struct_size : sizeof(*c);
    memcpy(c, in, n);
    c->struct_size = (uint32_t)sizeof(*c);
    if (c->selection == 0) c->selection = MOQ_MEDIA_SELECTION_AUTO;
    if (c->timing_policy == 0) c->timing_policy = MOQ_MEDIA_TIMING_REQUIRE_SYNC;
    if (c->selection != MOQ_MEDIA_SELECTION_AUTO &&
        c->selection != MOQ_MEDIA_SELECTION_MANUAL)
        return MOQ_ERR_INVAL;
    if (c->timing_policy != MOQ_MEDIA_TIMING_REQUIRE_SYNC &&
        c->timing_policy != MOQ_MEDIA_TIMING_ALLOW_INDEPENDENT)
        return MOQ_ERR_INVAL;
    if (c->preferred_lang.len && !c->preferred_lang.data) return MOQ_ERR_INVAL;
    if (c->max_outputs > MOQ_MEDIA_PLAYER_MAX_QUEUE ||
        c->max_events > MOQ_MEDIA_PLAYER_MAX_QUEUE)
        return MOQ_ERR_INVAL;
    return MOQ_OK;
}

static void player_free(moq_media_player_t *p)
{
    if (!p) return;
    if (p->has_held) moq_media_object_cleanup(&p->held.obj);
    if (p->has_staged) moq_playback_cmd_cleanup(&p->staged);
    for (uint32_t i = 0; i < p->out_count; i++)
        moq_media_player_output_cleanup(&p->outputs[(p->out_head + i) % p->out_cap]);
    if (p->pb) moq_playback_destroy(p->pb);
    moq_alloc_t a = p->alloc;
    if (p->tracks) a.free(p->tracks, p->max_tracks * sizeof(*p->tracks), a.ctx);
    if (p->timelines) a.free(p->timelines, p->max_tracks * sizeof(*p->timelines), a.ctx);
    if (p->outputs) a.free(p->outputs, p->out_cap * sizeof(*p->outputs), a.ctx);
    if (p->out_hold) a.free(p->out_hold, p->out_cap * sizeof(*p->out_hold), a.ctx);
    if (p->events) a.free(p->events, p->ev_cap * sizeof(*p->events), a.ctx);
    if (p->known) a.free(p->known, p->known_cap * sizeof(*p->known), a.ctx);
    if (p->lang) a.free(p->lang, p->lang_len, a.ctx);
    a.free(p, sizeof(*p), a.ctx);
}

static void *player_calloc(const moq_alloc_t *a, size_t count, size_t size)
{
    if (count && size > SIZE_MAX / count) return NULL;
    void *v = a->alloc(count * size, a->ctx);
    if (v) memset(v, 0, count * size);
    return v;
}

static moq_result_t player_new(const moq_media_player_cfg_t *c, moq_media_player_t **out)
{
    const moq_alloc_t *a = moq_alloc_default();
    moq_media_player_t *p = (moq_media_player_t *)player_calloc(a, 1, sizeof(*p));
    if (!p) return MOQ_ERR_NOMEM;
    p->alloc = *a;
    p->selection = c->selection;
    p->timing_policy = c->timing_policy;
    p->assume_shared_epoch = c->assume_shared_epoch;
    p->tail_us = c->end_tail_wait_us ? c->end_tail_wait_us : PLAYER_DEFAULT_TAIL_US;
    p->cushion_us = c->cushion_us ? c->cushion_us : PLAYER_DEFAULT_CUSHION_US;
    p->out_cap = c->max_outputs ? c->max_outputs : PLAYER_DEFAULT_OUTPUTS;
    if (p->out_cap < 2) p->out_cap = 2;
    p->ev_limit = c->max_events ? c->max_events : PLAYER_DEFAULT_EVENTS;
    p->ev_cap = p->ev_limit + PLAYER_TERMINAL_EVENT_RESERVE;
    p->max_held_bytes = c->max_held_bytes ? c->max_held_bytes : PLAYER_DEFAULT_HELD_BYTES;
    moq_playback_cfg_t pc;
    moq_playback_cfg_init(&pc);
    if (c->playback) {
        if (c->playback->struct_size < MOQ_PLAYBACK_CFG_V0_SIZE) {
            player_free(p);
            return MOQ_ERR_INVAL;
        }
        size_t n = c->playback->struct_size < sizeof(pc) ? c->playback->struct_size
                                                         : sizeof(pc);
        memcpy(&pc, c->playback, n);
        pc.struct_size = (uint32_t)n;
    }
    /* The player ends a track by FIN facts and END_OF_TRACK, never by the
     * subgroup header's END_OF_GROUP bit or the next group's arrival. */
    pc.ignore_eog_bit = true;
    pc.infer_end_of_group_from_next_group = false;
    /* Each queued output holds its command until the application polls it,
     * and so does a staged command: one hold more than output slots, up to
     * the engine's 65536, which is MOQ_MEDIA_PLAYER_MAX_QUEUE too. At that
     * bound none is short: a command is staged only when fewer than two
     * slots are free, and a command that fills two leaves one of them
     * without a hold, so a hold is always free for it.
     * The copy's uncovered fields are cfg_init defaults, so it is complete
     * at its full size. */
    pc.struct_size = (uint32_t)sizeof(pc);
    pc.max_held_commands = p->out_cap < MOQ_MEDIA_PLAYER_MAX_QUEUE ? p->out_cap + 1
                                                                   : p->out_cap;
    moq_result_t rc = moq_playback_create(a, &pc, &p->pb);
    if (rc != MOQ_OK) {
        player_free(p);
        return rc;
    }
    p->max_tracks = pc.max_tracks ? pc.max_tracks : 4;
    p->tracks = (player_track_t *)player_calloc(a, p->max_tracks, sizeof(*p->tracks));
    p->timelines = (player_timeline_t *)player_calloc(a, p->max_tracks,
                                                      sizeof(*p->timelines));
    p->outputs = (moq_media_player_output_t *)player_calloc(a, p->out_cap,
                                                            sizeof(*p->outputs));
    p->out_hold = (moq_playback_command_hold_t *)player_calloc(a, p->out_cap,
                                                              sizeof(*p->out_hold));
    p->events = (moq_media_player_event_t *)player_calloc(a, p->ev_cap,
                                                          sizeof(*p->events));
    if (c->preferred_lang.len) {
        p->lang = (uint8_t *)a->alloc(c->preferred_lang.len, a->ctx);
        if (p->lang) {
            memcpy(p->lang, c->preferred_lang.data, c->preferred_lang.len);
            p->lang_len = c->preferred_lang.len;
        }
    }
    if (!p->tracks || !p->timelines || !p->outputs || !p->out_hold || !p->events ||
        (c->preferred_lang.len && !p->lang)) {
        player_free(p);
        return MOQ_ERR_NOMEM;
    }
    *out = p;
    return MOQ_OK;
}

moq_result_t moq_media_player_create(const moq_media_player_cfg_t *cfg,
                                     moq_media_player_t **out)
{
    if (!cfg || !out) return MOQ_ERR_INVAL;
    *out = NULL;
    moq_media_player_cfg_t c;
    moq_result_t rc = player_cfg_read(cfg, &c);
    if (rc != MOQ_OK) return rc;
    if (!c.receiver) return MOQ_ERR_INVAL;
    if (c.receiver->struct_size < MOQ_MEDIA_RECEIVER_CFG_V0_SIZE) return MOQ_ERR_INVAL;
    moq_media_receiver_cfg_t rc_cfg;
    moq_media_receiver_cfg_init_sized(&rc_cfg, sizeof(rc_cfg));
    size_t n = c.receiver->struct_size < sizeof(rc_cfg) ? c.receiver->struct_size
                                                        : sizeof(rc_cfg);
    uint32_t keep = rc_cfg.struct_size;
    moq_media_receiver_cfg_t defaults = rc_cfg;
    memcpy(&rc_cfg, c.receiver, n);
    /* Fields the caller's struct does not cover keep their defaults. */
    if (n < sizeof(rc_cfg))
        memcpy((uint8_t *)&rc_cfg + n, (const uint8_t *)&defaults + n, sizeof(rc_cfg) - n);
    rc_cfg.struct_size = keep;
    if (rc_cfg.time_mode == MOQ_MEDIA_TIME_SHARED_EPOCH) return MOQ_ERR_UNSUPPORTED;
    rc_cfg.auto_subscribe = false;
    rc_cfg.surface_stream_terminals = true;
    moq_media_player_t *p;
    rc = player_new(&c, &p);
    if (rc != MOQ_OK) return rc;
    rc = moq_media_receiver_create(&rc_cfg, &p->r);
    if (rc != MOQ_OK) {
        player_free(p);
        return rc;
    }
    p->owns_receiver = true;
    *out = p;
    return MOQ_OK;
}

moq_result_t moq_media_player_attach(moq_media_receiver_t *r,
                                     const moq_media_player_cfg_t *cfg,
                                     moq_media_player_t **out)
{
    if (!r || !cfg || !out) return MOQ_ERR_INVAL;
    *out = NULL;
    moq_media_player_cfg_t c;
    moq_result_t rc = player_cfg_read(cfg, &c);
    if (rc != MOQ_OK) return rc;
    if (c.receiver) return MOQ_ERR_INVAL;
    moq_media_receiver_info_t info;
    rc = moq_media_receiver_get_info(r, &info, sizeof(info));
    if (rc != MOQ_OK) return rc;
    if (info.time_mode == (uint32_t)MOQ_MEDIA_TIME_SHARED_EPOCH) return MOQ_ERR_UNSUPPORTED;
    if (info.auto_subscribe || !info.surface_stream_terminals ||
        info.track_events_polled > 0)
        return MOQ_ERR_WRONG_STATE;
    moq_media_player_t *p;
    rc = player_new(&c, &p);
    if (rc != MOQ_OK) return rc;
    p->r = r;
    p->transport_version = info.transport_version;
    *out = p;
    return MOQ_OK;
}

void moq_media_player_destroy(moq_media_player_t *p)
{
    if (!p) return;
    for (uint32_t i = 0; i < p->max_tracks; i++)
        if (p->tracks[i].used) track_release(p, i, !p->owns_receiver);
    moq_media_receiver_t *r = p->owns_receiver ? p->r : NULL;
    player_free(p);
    if (r) moq_media_receiver_destroy(r);
}
