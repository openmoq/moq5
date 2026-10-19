#ifndef MOQ_MEDIA_PLAYER_H
#define MOQ_MEDIA_PLAYER_H

/*
 * moq_media_player: decode-order media for an application's decoders.
 *
 * The player composes a moq_media_receiver (catalog, subscriptions, the
 * object queue and its stream-terminal lane) with a moq_playback engine
 * (per-track decode order, knowledge of what exists, gap policy). The
 * application gets, per selected track, ordered OUTPUT records:
 *
 *   CONFIGURE      a self-contained decoder configuration snapshot;
 *   UNIT           one access unit in decode order, with its decode and
 *                  presentation times (on CMAF the decode time is the
 *                  decode clock; on RAW/LOC it is a copy of the LOC media
 *                  time, equal to the presentation time, and not evidence
 *                  of a known decode clock: decode order is output order,
 *                  and with frame reordering it can decrease from one unit
 *                  to the next);
 *   DISCONTINUITY  the decoder must be reset before the next unit;
 *   TIMELINE       the presentation mapping of the track's timeline,
 *                  before its first unit;
 *   END            after the track's last unit, with why and how it ended.
 *
 * Decoders, renderers, audio output and the presentation clock stay with
 * the application. Diagnostics (gaps, skips, drops, loss, selection) are
 * EVENT records on a separate queue and never interleave with OUTPUT.
 *
 * This release supports RAW/LOC video and the RAW/LOC audio codecs `opus`
 * and `mp4a.40.2`, on drafts 16, 18 and 22. CMAF, MPEG-2 TS, dependent
 * spatial layers and timeline tracks are refused at selection with
 * MOQ_ERR_UNSUPPORTED; the receiver serves them directly.
 *
 * Threading: the player owns no thread. Every call on a player, wait and
 * the observers included, is made by the application on one thread at a
 * time: none may run concurrently with another, tick included. wait and
 * the observers do not advance it. The receiver keeps its own network
 * thread.
 *
 * Clock: now_us is the application's monotonic clock in microseconds, one
 * clock for tick, wait, handle_feedback and every deadline the player
 * reports; the player never reads a clock itself.
 *
 * Records: OUTPUT and EVENT records are frozen polled-union layouts. Their
 * sizes and detail reserves are pinned; a poll needs a record of at least
 * the library's size and never truncates one. The configuration, feedback
 * and stats records follow the sized-init contract: a caller's struct_size
 * covers at least the frozen v0 floor, and fields past it read as zero.
 */

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include <moq/types.h>
#include <moq/rcbuf.h>
#include <moq/media_object.h>
#include <moq/media_receiver.h>
#include <moq/playback.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct moq_media_player moq_media_player_t;

/* -- Kinds and policies ----------------------------------------------- *
 * Each set is fixed-width uint32_t. Values are never renumbered; a
 * consumer ignores a kind or reason it does not know. */

/* How tracks are chosen. AUTO picks a render group's tracks itself once
 * the catalog is ready (see select_track); MANUAL selects nothing until
 * the application does. */
typedef uint32_t moq_media_selection_mode_t;
#define MOQ_MEDIA_SELECTION_AUTO   1u
#define MOQ_MEDIA_SELECTION_MANUAL 2u

/* Whether AUTO may select a render group whose tracks cannot share one
 * timeline. REQUIRE_SYNC (the default) does not select it and reports
 * SYNC_UNAVAILABLE; ALLOW_INDEPENDENT selects it with a timeline per track
 * and reports TIMING_INDEPENDENT. */
typedef uint32_t moq_media_timing_policy_t;
#define MOQ_MEDIA_TIMING_REQUIRE_SYNC      1u
#define MOQ_MEDIA_TIMING_ALLOW_INDEPENDENT 2u

/* -- Configuration ------------------------------------------------------ */

typedef struct moq_media_player_cfg {
    uint32_t struct_size;
    uint32_t selection;                 /* moq_media_selection_mode_t; 0 = AUTO */
    /* create only: the receiver the player creates and owns. BORROWED for
     * the call. The player forces RAW time mode (a SHARED_EPOCH receiver
     * is refused with MOQ_ERR_UNSUPPORTED), auto_subscribe = false and the
     * stream-terminal lane on (max_stream_terminals as given, 0 = 64).
     * attach: must be NULL. */
    const moq_media_receiver_cfg_t *receiver;
    /* The engine's configuration, BORROWED for the call and copied; NULL
     * selects its defaults. Its max_tracks bounds the selected tracks. */
    const moq_playback_cfg_t *playback;
    /* AUTO: within an alternate group, prefer this catalog lang; empty =
     * any. BORROWED for the call and copied. */
    moq_bytes_t preferred_lang;
    /* How long a track whose input ended without proof of completion, or
     * was removed, keeps accepting late input before it is cut off. Local
     * policy, not a transport timer. 0 selects 2 s. */
    uint64_t end_tail_wait_us;
    /* Timeline lead: a timeline is anchored with its first unit presented
     * this long after the tick that released it. 0 selects 100 ms. */
    uint64_t cushion_us;
    /* Queue depths, each at most MOQ_MEDIA_PLAYER_MAX_QUEUE; a larger value
     * is refused. The EVENT queue keeps room for ENDED and CLOSED beyond
     * max_events. */
    uint32_t max_outputs;               /* OUTPUT queue depth; 0 = 128 */
    uint32_t max_events;                /* EVENT queue depth; 0 = 64 */
    uint32_t timing_policy;             /* moq_media_timing_policy_t; 0 = REQUIRE_SYNC */
    uint32_t _reserved_cfg;             /* zero; never read */
    /* An object the engine has no room for yet is held by the player, and
     * no later object or stream terminal is taken from the receiver until
     * it goes in. Track events still are: a removal, a done or a local
     * failure may start the track's end, but its END follows the held
     * object, which is released first or, for a cause that allows no
     * wait, settled into the engine before the drain. The bytes it may
     * take, by moq_media_object_charge; a larger object is taken only when
     * the engine accepts it at once (else dropped OVERSIZE). 0 selects
     * 8 MiB. */
    uint64_t max_held_bytes;
    /* AUTO: treat a render group whose tracks are all in application media
     * time (LOC with a Timescale) as one timeline: the application asserts
     * they share an epoch. false: such a group of more than one track is
     * not selected. */
    bool     assume_shared_epoch;
} moq_media_player_cfg_t;

/* The largest max_outputs and max_events. */
#define MOQ_MEDIA_PLAYER_MAX_QUEUE 65536u

/* Frozen v0 floor: the complete layout through assume_shared_epoch. */
#define MOQ_MEDIA_PLAYER_CFG_V0_SIZE \
    (offsetof(moq_media_player_cfg_t, assume_shared_epoch) + sizeof(bool))

/* Clears min(cfg_size, sizeof) bytes and stamps struct_size with that
 * count; every field's zero is its default. A cfg_size below
 * sizeof(uint32_t), or a NULL cfg, writes nothing. */
MOQ_API void moq_media_player_cfg_init_sized(moq_media_player_cfg_t *cfg,
                                             size_t cfg_size);
#define moq_media_player_cfg_init(cfg) \
    moq_media_player_cfg_init_sized((cfg), sizeof(*(cfg)))

/* -- Lifecycle ----------------------------------------------------------- */

/*
 * Create a player that creates and owns its receiver (cfg->receiver
 * REQUIRED). Results: MOQ_OK; MOQ_ERR_INVAL for NULL arguments, a
 * struct_size below the v0 floor, an unknown selection mode or timing
 * policy, or a receiver or playback configuration their own create refuses;
 * MOQ_ERR_UNSUPPORTED for a SHARED_EPOCH receiver; MOQ_ERR_NOMEM; and the
 * receiver's create results.
 */
MOQ_API moq_result_t moq_media_player_create(const moq_media_player_cfg_t *cfg,
                                             moq_media_player_t **out);

/*
 * Attach a player to an existing receiver (cfg->receiver NULL). The player
 * becomes the sole consumer of every receive queue of `r` (track events,
 * objects, stream terminals, SAP and media-timeline records); the
 * application keeps `r` for observing calls only, and `r` must outlive the
 * player. Every check runs before the player takes anything; a refused
 * attach leaves `r` untouched. Results: MOQ_OK; MOQ_ERR_INVAL for NULL
 * arguments, a non-NULL cfg->receiver or a bad cfg; MOQ_ERR_UNSUPPORTED for
 * a SHARED_EPOCH receiver; MOQ_ERR_WRONG_STATE for a receiver that
 * auto-subscribes, has the stream-terminal lane off, or has already had a
 * track event polled (the player must see discovery from the start);
 * MOQ_ERR_NOMEM.
 */
MOQ_API moq_result_t moq_media_player_attach(moq_media_receiver_t *r,
                                             const moq_media_player_cfg_t *cfg,
                                             moq_media_player_t **out);

/* Destroy the player: unsubscribe every track it selected and release all
 * it holds. A created player destroys its receiver too; an attached one
 * returns the receiver's queues to the application. Polled outputs stay
 * valid (they own their refs). NULL-safe. */
MOQ_API void moq_media_player_destroy(moq_media_player_t *p);

/* The receiver, for observing calls (track_desc_get, get_stats, ...). Never
 * poll it while the player exists. */
MOQ_API moq_media_receiver_t *moq_media_player_receiver(const moq_media_player_t *p);

/* -- Selection ------------------------------------------------------------ */

typedef struct moq_media_player_select_cfg {
    uint32_t struct_size;
    uint32_t start;              /* moq_media_start_mode_t: CURRENT (default)
                                    or NEXT_GROUP */
    /* The track gets a timeline of its own instead of its render group's:
     * an explicit choice to play it unsynchronized. */
    bool     independent_timing;
    /* The application asserts that this track and the other application
     * media time tracks of its render group share an epoch. */
    bool     shared_epoch;
} moq_media_player_select_cfg_t;

#define MOQ_MEDIA_PLAYER_SELECT_CFG_V0_SIZE \
    (offsetof(moq_media_player_select_cfg_t, shared_epoch) + sizeof(bool))

MOQ_API void moq_media_player_select_cfg_init_sized(
    moq_media_player_select_cfg_t *cfg, size_t cfg_size);
#define moq_media_player_select_cfg_init(cfg) \
    moq_media_player_select_cfg_init_sized((cfg), sizeof(*(cfg)))

/*
 * Select a track the receiver announced (a handle from a TRACK_AVAILABLE
 * event or the receiver's descriptor). The track is subscribed and its
 * media goes to the engine.
 *
 * Timelines: the tracks of one render group (one catalog renderGroup; a
 * track without one is a group by itself) share one timeline when their
 * timestamps share a domain: all wall-clock (LOC without a Timescale, and
 * every draft-16/18 track), or all application media time (LOC with a
 * Timescale) with shared_epoch asserted by each. A track that would join a
 * group's timeline otherwise is refused with MOQ_ERR_UNSUPPORTED unless it
 * asks for independent_timing. On draft 22 a track's domain is known from
 * its catalog Timescale; an object in another domain is dropped
 * (DROP{TIME_DOMAIN}).
 *
 * Results: MOQ_OK; MOQ_ERR_INVAL for NULL, a bad struct_size, an unknown
 * start mode or a handle the receiver does not own; MOQ_ERR_UNSUPPORTED for
 * a packaging, media type or codec this release does not play (above), and
 * for an mp4a.40.2 track that can carry no decoder configuration (no
 * catalog configuration on drafts 16/18); MOQ_ERR_WRONG_STATE for a track
 * already selected, ended, removed, or once the receiver is terminal;
 * MOQ_ERR_LIMIT when the engine's max_tracks are in use; MOQ_ERR_NOMEM; and
 * the receiver's subscribe results. A refusal selects nothing.
 */
MOQ_API moq_result_t moq_media_player_select_track(
    moq_media_player_t *p, moq_media_track_t *track,
    const moq_media_player_select_cfg_t *cfg);

/* Deselect: unsubscribe and drop what the player and engine hold for the
 * track. Outputs already queued stay; no END follows. MOQ_ERR_INVAL for a
 * track that is not selected. */
MOQ_API moq_result_t moq_media_player_deselect_track(moq_media_player_t *p,
                                                     moq_media_track_t *track);

/* -- Advancing ------------------------------------------------------------ */

/*
 * Do everything that can be done at now_us: take track events, objects and
 * stream terminals from the receiver in order, run the engine, and turn its
 * commands into outputs and its reports into events.
 *
 * Results: MOQ_OK; MOQ_ERR_WOULD_BLOCK when the output queue is full and
 * the engine holds more (poll outputs and tick again; work done before
 * stands); MOQ_ERR_CLOSED once the player is closed; MOQ_ERR_INVAL for NULL.
 */
MOQ_API moq_result_t moq_media_player_tick(moq_media_player_t *p, uint64_t now_us);

/*
 * Wait for something to do, without advancing the player or consuming
 * anything. In order:
 *   MOQ_ERR_INTERRUPTED while the endpoint's interrupt latch is set;
 *   MOQ_OK when an output or event is pollable, when the receiver has
 *     input for tick, or when a deadline (next_deadline_us) is at or
 *     before now_us: the caller ticks;
 *   MOQ_DONE at once when the only work left is paused by DECODER_QUEUE
 *     feedback: that pause lifts only through handle_feedback on this
 *     thread, so wait on the decoder, not here;
 *   MOQ_ERR_CLOSED once the player is closed (receiver terminal and
 *     nothing held anywhere);
 *   otherwise it sleeps on the receiver for at most min(timeout_us, the
 *     time to the next deadline). MOQ_OK when the receiver woke it or the
 *     deadline came; MOQ_DONE when timeout_us elapsed first.
 * A held input waiting for engine room is not a deadline: polling outputs
 * makes that room. MOQ_ERR_INVAL for NULL.
 */
MOQ_API moq_result_t moq_media_player_wait(moq_media_player_t *p,
                                           uint64_t now_us,
                                           uint64_t timeout_us);

/* The earliest now_us at which tick has time-driven work: an engine gap
 * wait, or a track's end tail wait. UINT64_MAX when there is none and for
 * NULL. Observes only. */
MOQ_API uint64_t moq_media_player_next_deadline_us(const moq_media_player_t *p);

/* -- Outputs --------------------------------------------------------------- */

typedef uint32_t moq_media_player_output_kind_t;
#define MOQ_MEDIA_PLAYER_OUTPUT_CONFIGURE     1u
#define MOQ_MEDIA_PLAYER_OUTPUT_UNIT          2u
#define MOQ_MEDIA_PLAYER_OUTPUT_DISCONTINUITY 3u
#define MOQ_MEDIA_PLAYER_OUTPUT_TIMELINE      4u
#define MOQ_MEDIA_PLAYER_OUTPUT_END           6u

/* Why a track's DISCONTINUITY. */
typedef uint32_t moq_media_player_discontinuity_t;
#define MOQ_MEDIA_PLAYER_DISCONTINUITY_GAP          1u /* units were skipped */
#define MOQ_MEDIA_PLAYER_DISCONTINUITY_DECODE_ERROR 2u /* DECODE_ERROR feedback */

/* What ended a track (END.cause), in precedence order: when several apply
 * before END, the highest wins. PEER_DONE: the peer ended the track's
 * delivery without an END_OF_TRACK the player could use -- an error done,
 * or a track its receiver ended for good (a normal done that ends the
 * subscription, or a refused VOD re-subscription) whose END_OF_TRACK did
 * not arrive within the tail wait. A normal done is no cause of its own: an
 * END_OF_TRACK arriving within that wait still ends the track COMPLETE. */
typedef uint32_t moq_media_player_end_cause_t;
#define MOQ_MEDIA_PLAYER_END_PEER_END_OF_TRACK 1u /* its END_OF_TRACK */
#define MOQ_MEDIA_PLAYER_END_PEER_DONE         2u /* delivery ended, no END_OF_TRACK */
#define MOQ_MEDIA_PLAYER_END_REMOVED           3u /* left the catalog */
#define MOQ_MEDIA_PLAYER_END_LOCAL_FAILURE     4u /* receiver-local failure */
#define MOQ_MEDIA_PLAYER_END_CLOSED            5u /* the receiver closed */
#define MOQ_MEDIA_PLAYER_END_FATAL             6u /* the receiver failed */

/* How its input was settled (END.disposition). COMPLETE: every location up
 * to its END_OF_TRACK was played or known not to exist (only with cause
 * PEER_END_OF_TRACK). TAIL_CUTOFF: a bounded wait gave up on missing input
 * (the tail wait, or a gap the engine stopped waiting for). IMMEDIATE: no
 * wait was possible (LOCAL_FAILURE, CLOSED, FATAL). */
typedef uint32_t moq_media_player_end_disposition_t;
#define MOQ_MEDIA_PLAYER_END_COMPLETE    1u
#define MOQ_MEDIA_PLAYER_END_TAIL_CUTOFF 2u
#define MOQ_MEDIA_PLAYER_END_IMMEDIATE   3u

/* A presentation mapping. A unit of the timeline presents at
 *   anchor_local_us + (presentation_time_us - anchor_media_us) / rate
 * on the caller's clock. A frozen leaf. */
typedef struct moq_media_timeline {
    moq_media_time_domain_t domain;
    uint32_t timeline_id;        /* nonzero, player-assigned */
    uint64_t timeline_epoch;     /* changes when the mapping is re-anchored */
    uint64_t anchor_media_us;
    uint64_t anchor_local_us;
    double   rate;               /* 1.0 */
} moq_media_timeline_t;

#define MOQ_MEDIA_PLAYER_OUTPUT_DETAIL_MAX 112u

typedef struct moq_media_player_output {
    uint32_t                       struct_size;   /* stamped */
    moq_media_player_output_kind_t kind;
    uint32_t                       detail_size;   /* meaningful bytes of u */
    uint32_t                       timeline_id;   /* the track's timeline */
    /* The track's decoder epoch: incremented by every CONFIGURE and every
     * DISCONTINUITY. DECODER_QUEUE, DECODE_ERROR and FLUSH_COMPLETE
     * feedback name the epoch they observed. */
    uint64_t                       configuration_generation;
    moq_media_track_t             *track;         /* receiver handle */
    union {
        struct {
            uint32_t              media_type;     /* moq_media_type_t value */
            uint32_t              packaging;      /* moq_media_packaging_t value */
            moq_rcbuf_t          *codec;          /* OWNED: codec string */
            moq_rcbuf_t          *codec_config;   /* OWNED or NULL: extradata */
            uint32_t              timescale;
            uint32_t              width, height;
            uint32_t              samplerate, channel_count;
        } configure;
        struct {
            uint64_t     group_id, object_id;
            uint64_t     decode_time_us;
            uint64_t     presentation_time_us;
            int64_t      composition_offset_us;
            bool         has_capture_time;
            bool         independence_known; /* video: marked or not */
            bool         independent;
            bool         present;             /* true in this release */
            uint32_t     sample_duration_us;  /* 0 when unknown */
            uint64_t     capture_time_us;
            moq_rcbuf_t *payload;             /* OWNED */
            size_t       offset, len;         /* the unit's bytes in payload */
        } unit;
        struct {
            moq_media_player_discontinuity_t reason;
            bool decoder_reset_required;      /* true in this release */
        } discontinuity;
        moq_media_timeline_t timeline;
        struct {
            moq_media_player_end_cause_t       cause;
            moq_media_player_end_disposition_t disposition;
        } end;
        uint8_t _reserved[MOQ_MEDIA_PLAYER_OUTPUT_DETAIL_MAX];
    } u;
} moq_media_player_output_t;

/*
 * Dequeue the next output into `out`, whose size must be at least
 * sizeof(moq_media_player_output_t): a record owns refs, so it is never
 * truncated. Its refs pass to the caller, who gives every polled record,
 * whatever its kind, to moq_media_player_output_cleanup.
 *
 * Order: per track, CONFIGURE precedes the units it applies to, a
 * DISCONTINUITY the unit it precedes, TIMELINE the timeline's first unit,
 * and END follows the track's last unit.
 *
 * Results: MOQ_OK; MOQ_DONE when none is queued; MOQ_ERR_CLOSED when none
 * is queued and the player is closed; MOQ_ERR_ABI_MISMATCH when out_size is
 * too small (nothing dequeued); MOQ_ERR_INVAL for NULL.
 */
MOQ_API moq_result_t moq_media_player_poll_output(moq_media_player_t *p,
                                                  moq_media_player_output_t *out,
                                                  size_t out_size);

/* Release the refs a polled output owns and clear those pointers. Knows
 * every kind this library emits, so a caller that ignores an unknown kind
 * still returns its refs. Idempotent, NULL-safe, and legal after the player
 * is destroyed: an output holds rcbuf refs only. */
MOQ_API void moq_media_player_output_cleanup(moq_media_player_output_t *out);

/* -- Events ---------------------------------------------------------------- */

typedef uint32_t moq_media_player_event_kind_t;
#define MOQ_MEDIA_PLAYER_EVENT_TRACK_AVAILABLE      1u /* announced, not selected */
#define MOQ_MEDIA_PLAYER_EVENT_TRACK_REMOVED        2u /* left the catalog */
#define MOQ_MEDIA_PLAYER_EVENT_READY                3u /* catalog ready; AUTO done */
#define MOQ_MEDIA_PLAYER_EVENT_STARTED              4u /* a timeline's first unit */
#define MOQ_MEDIA_PLAYER_EVENT_GAP                  5u /* gap: waited, or crossed */
#define MOQ_MEDIA_PLAYER_EVENT_SKIP                 6u /* groups abandoned */
#define MOQ_MEDIA_PLAYER_EVENT_SHED                 7u /* backlog shed */
#define MOQ_MEDIA_PLAYER_EVENT_DROP                 8u /* one object dropped */
#define MOQ_MEDIA_PLAYER_EVENT_LOSS                 9u /* a reset or receiver drop */
#define MOQ_MEDIA_PLAYER_EVENT_STALL               11u /* video waits for an entry */
#define MOQ_MEDIA_PLAYER_EVENT_ENDED               14u /* every selected track ended */
#define MOQ_MEDIA_PLAYER_EVENT_CLOSED              15u /* the player closed */
#define MOQ_MEDIA_PLAYER_EVENT_TIMING_INDEPENDENT  16u /* AUTO, ALLOW_INDEPENDENT */
#define MOQ_MEDIA_PLAYER_EVENT_SYNC_UNAVAILABLE    17u /* AUTO left a group out */
#define MOQ_MEDIA_PLAYER_EVENT_KNOWLEDGE_FORGOTTEN 18u /* absence demoted */

/* Player drop reasons (DROP.reason) beyond the engine's
 * moq_playback_drop_reason_t, which DROP also carries as is. */
#define MOQ_MEDIA_PLAYER_DROP_OVERSIZE        64u /* larger than the engine */
#define MOQ_MEDIA_PLAYER_DROP_LATE_AFTER_END  65u /* its track had ended */
#define MOQ_MEDIA_PLAYER_DROP_TIME_DOMAIN     66u /* not its timeline's domain */
#define MOQ_MEDIA_PLAYER_DROP_CONFIGURATION_MISSING 67u /* no decoder config yet */
#define MOQ_MEDIA_PLAYER_DROP_MALFORMED       68u /* the engine refused it */

/* Why AUTO left a render group out, or timed it independently. */
#define MOQ_MEDIA_PLAYER_SYNC_MIXED_DOMAINS     1u
#define MOQ_MEDIA_PLAYER_SYNC_NO_SHARED_EPOCH   2u

#define MOQ_MEDIA_PLAYER_EVENT_DETAIL_MAX 48u

typedef struct moq_media_player_event {
    uint32_t                      struct_size;
    moq_media_player_event_kind_t kind;
    uint32_t                      detail_size;
    uint32_t                      _reserved_event;  /* zero; never read */
    moq_media_track_t            *track;            /* NULL: player-wide */
    union {
        struct {
            uint64_t group_id, object_id;   /* the missing unit */
            uint32_t cause;                 /* moq_playback_gap_cause_t; 0 when crossed */
            bool     crossed;               /* continued past without a reset */
            uint64_t hole_start_us, hole_duration_us; /* when crossed */
        } gap;
        struct { uint64_t from_group_id, to_group_id; } skip;
        struct { uint32_t dropped_groups, remaining_groups; } shed;
        struct { uint32_t reason; uint64_t group_id, object_id; } drop;
        struct {
            moq_media_stream_terminal_kind_t kind; /* RESET or RECEIVER_DROP */
            uint64_t group_id, last_group_id;
            uint64_t error_code;                   /* RESET */
        } loss;
        struct { uint64_t first_group_id, last_group_id; } knowledge_forgotten;
        struct {
            bool     has_render_group;
            int32_t  render_group;
            uint32_t reason;                       /* MOQ_MEDIA_PLAYER_SYNC_* */
        } sync;
        struct { uint32_t timeline_id; } started;
        uint8_t _reserved[MOQ_MEDIA_PLAYER_EVENT_DETAIL_MAX];
    } u;
} moq_media_player_event_t;

/*
 * Dequeue the next event, at least sizeof(moq_media_player_event_t) bytes.
 * Events own nothing. When the queue is full the oldest diagnostic is
 * dropped (counted in events_dropped); ENDED and CLOSED have room kept for
 * them and are never dropped, and a full queue never stops playback.
 * Results as poll_output's.
 */
MOQ_API moq_result_t moq_media_player_poll_event(moq_media_player_t *p,
                                                 moq_media_player_event_t *ev,
                                                 size_t ev_size);

/* -- Feedback -------------------------------------------------------------- */

typedef uint32_t moq_media_player_feedback_kind_t;
/* The track's decoder queue: release pauses while depth exceeds
 * max_recommended, and resumes below it. */
#define MOQ_MEDIA_PLAYER_FEEDBACK_DECODER_QUEUE  1u
/* The decoder failed: the track resumes at its next independent unit
 * behind a DISCONTINUITY{DECODE_ERROR}. */
#define MOQ_MEDIA_PLAYER_FEEDBACK_DECODE_ERROR   2u
/* The decoder flushed after END (counted only). */
#define MOQ_MEDIA_PLAYER_FEEDBACK_FLUSH_COMPLETE 5u

typedef struct moq_media_player_feedback {
    uint32_t                         struct_size;
    moq_media_player_feedback_kind_t kind;
    moq_media_track_t               *track;
    /* The configuration_generation of the outputs it observed: feedback
     * for an older one is ignored and counted (feedback_stale). */
    uint64_t                         configuration_generation;
    union {
        struct { uint32_t depth, max_recommended; } decoder_queue;
        struct { moq_bytes_t message; } decode_error;   /* BORROWED for the call */
        uint8_t _reserved[32];
    } u;
} moq_media_player_feedback_t;

/* Frozen v0 floor: the complete layout through u. */
#define MOQ_MEDIA_PLAYER_FEEDBACK_V0_SIZE \
    (offsetof(moq_media_player_feedback_t, u) + \
     sizeof(((moq_media_player_feedback_t *)0)->u))

MOQ_API void moq_media_player_feedback_init_sized(moq_media_player_feedback_t *fb,
                                                  size_t fb_size);
#define moq_media_player_feedback_init(fb) \
    moq_media_player_feedback_init_sized((fb), sizeof(*(fb)))

/* Results: MOQ_OK (applied, or stale and ignored); MOQ_ERR_INVAL for NULL,
 * a struct_size below the v0 floor, an unknown kind or a track that is not
 * selected. */
MOQ_API moq_result_t moq_media_player_handle_feedback(
    moq_media_player_t *p, const moq_media_player_feedback_t *fb, uint64_t now_us);

/* -- Observing ------------------------------------------------------------- */

/* Closed: the receiver is terminal and every output, event, held input and
 * engine buffer is empty. */
MOQ_API bool     moq_media_player_is_closed(const moq_media_player_t *p);
MOQ_API bool     moq_media_player_is_fatal(const moq_media_player_t *p);
MOQ_API uint64_t moq_media_player_fatal_code(const moq_media_player_t *p);

typedef struct moq_media_player_stats {
    uint32_t struct_size;
    uint32_t tracks_selected;
    uint64_t outputs_queued;
    uint64_t events_queued;
    uint64_t events_dropped;
    uint64_t held_objects;             /* 0 or 1 */
    uint64_t held_bytes;
    uint64_t objects_taken;            /* polled from the receiver */
    uint64_t objects_pushed;           /* accepted by the engine */
    uint64_t objects_dropped;          /* DROP events of the player's own */
    uint64_t objects_discarded;        /* for no selected track */
    uint64_t terminals_taken;          /* stream terminals polled */
    uint64_t units_output;
    uint64_t configures_output;
    uint64_t discontinuities_output;
    uint64_t ends_output;
    uint64_t timeline_records_discarded; /* SAP and media-timeline records */
    uint64_t feedback_stale;
    /* Bytes queued outputs keep charged to the engine's max_buffered_bytes
     * until they are polled. */
    uint64_t outputs_bytes;
    /* Tracks AUTO chose that could not be selected (left TRACK_AVAILABLE). */
    uint64_t selections_failed;
} moq_media_player_stats_t;

#define MOQ_MEDIA_PLAYER_STATS_V0_SIZE \
    (offsetof(moq_media_player_stats_t, selections_failed) + sizeof(uint64_t))

/* Fill min(out_size, sizeof) bytes and stamp struct_size. MOQ_ERR_INVAL for
 * NULL; MOQ_ERR_ABI_MISMATCH below the v0 floor. */
MOQ_API moq_result_t moq_media_player_get_stats(const moq_media_player_t *p,
                                                moq_media_player_stats_t *out,
                                                size_t out_size);

#ifdef __cplusplus
}
#endif

#endif /* MOQ_MEDIA_PLAYER_H */
