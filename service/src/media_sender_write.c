#include "media_sender_internal.h"
#include "media_sender_write_internal.h"
#include "media_sender_catalog_internal.h"
#include "media_sender_complete_internal.h"
#include "media_sender_mpeg2ts_internal.h"
#include "media_sender_timeline_internal.h"

/* The largest LOC timestamp write() admits for the latched draft `v`. It is
 * read from the LOC version table moq_loc_encode itself checks against, so a
 * value admitted here is one the drain path can encode: the whole uint64
 * range on legacy draft18's vi64 codec,
 * MOQ_QUIC_VARINT_MAX where it is the QUIC variable-length integer (draft
 * 16). Before the session's draft has been observed (or for a draft the
 * table does not register, which the drain refuses anyway) the bound is the
 * narrowest of the drafts the sender can encode: a write that has not yet
 * met its session must not be admitted on a promise only some drafts could
 * keep. */
static uint64_t sender_loc_ts_max(moq_version_t v)
{
    uint64_t max = moq_loc_value_max_for_version(v);
    if (max != 0) return max;
    static const moq_version_t k_drafts[] = {
        MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18,
    };
    max = UINT64_MAX;
    for (size_t i = 0; i < sizeof(k_drafts) / sizeof(k_drafts[0]); i++) {
        uint64_t m = moq_loc_value_max_for_version(k_drafts[i]);
        if (m < max) max = m;
    }
    return max;
}


/* -- Send queue (mu held by caller) ----------------------------------- *
 * One ring serves both phases: the pre-ready bound applies until ready,
 * the (typically larger) send-queue bound after. The ring is sized to the
 * max of both so the bound is purely a policy gate, never a realloc. */

/* The single release chokepoint: every path that removes an entry from the ring
 * comes through here, so the media count is maintained in exactly one place.
 * A non-marker entry was necessarily counted at enqueue, so the decrement is
 * EXACT -- deliberately not clamped at zero, because saturating there would
 * hide a double release or a missed enqueue instead of surfacing it. */
void preq_entry_release(moq_media_sender_t *s, sender_preq_entry_t *e)
{
    if (!e->is_end) s->preq_media--;
    if (e->payload)    moq_rcbuf_decref(e->payload);
    if (e->properties) moq_rcbuf_decref(e->properties);
    memset(e, 0, sizeof(*e));
}


/* THE drop-accounting chokepoint: one queued MEDIA entry is being discarded.
 * EVERY discard path goes through here -- the no-demand policy, lifecycle
 * removal/completion, and capacity-policy eviction -- so none of them can drift
 * from the others on bytes, objects, keyframes, or groups.
 *
 * `groups_dropped` counts CUMULATIVE DISTINCT (track, group_seq) values from
 * which at least one queued media object was discarded. group_seq is monotonic
 * per track, so remembering the last dropped group for the track's LIFETIME is
 * sufficient and makes the count independent of how the discards are split
 * across drain passes or across discard paths.
 *
 * This is purely the cumulative fact. The per-pass "first discard for this
 * track" fact that the no-demand path needs to arm its wire RESET once is
 * `drain_dropped`, handled at that call site. Markers are NOT media and must
 * never reach here. */
void preq_note_dropped(moq_media_sender_t *s, moq_media_track_t *t,
                       const sender_preq_entry_t *e)
{
    s->preq_bytes -= e->bytes;
    s->stats.objects_dropped++;
    t->tstats.objects_dropped++;
    t->tstats.objects_queued--;
    t->tstats.bytes_queued -= e->bytes;
    if (e->keyframe) {
        s->stats.keyframes_dropped++;
        t->tstats.keyframes_dropped++;
    }
    if (!t->drop_group_valid || t->drop_group_last != e->group_seq) {
        s->stats.groups_dropped++;
        t->tstats.groups_dropped++;
    }
    t->drop_group_valid = true;
    t->drop_group_last = e->group_seq;
}


/* The single enqueue chokepoint for the media count. It reads the COMMITTED
 * entry rather than taking a caller-supplied flag, so the two chokepoints
 * cannot disagree about what an entry is. */
void preq_note_enqueued(moq_media_sender_t *s,
                        const sender_preq_entry_t *e)
{
    if (!e->is_end) s->preq_media++;
}


uint32_t active_obj_cap(const moq_media_sender_t *s)
{
    return s->ready ? s->queue_cap : s->preq_cap;
}

static uint64_t active_byte_cap(const moq_media_sender_t *s)
{
    return s->ready ? s->queue_byte_cap : s->preq_byte_cap;
}


static bool preq_fits(const moq_media_sender_t *s, size_t need)
{
    uint32_t depth = s->preq_tail - s->preq_head;
    return depth < active_obj_cap(s) &&
           s->preq_bytes + need <= active_byte_cap(s);
}


/* The facade holds Group g of this track: its subgroup is open on the wire, or
 * a write of it was refused with WOULD_BLOCK and waits for a retry. A drop of
 * g must reset it, or the next write of another Group is refused. */
bool sender_holds_group(const moq_media_track_t *t, uint64_t g)
{
    return (t->emit_open && t->emit_group == g) ||
           (t->write_blocked && t->blocked_group == g);
}


/* Evict one specific group (every queued entry of track t + group g,
 * wherever it sits). With multiple tracks the streams interleave, so a
 * group's entries are not necessarily contiguous -- dropping only the
 * contiguous head run would strand same-group entries behind a later sync
 * point (the corruption class the receiver slice fixed). Compact the ring,
 * keeping order. Counts the drop and, if the group is currently open on
 * the wire, arms the track's pending_reset so the drain abandons the
 * subgroup wire-correctly rather than truncating it. mu held. */
void preq_evict_group(moq_media_sender_t *s,
                      moq_media_track_t *t, uint64_t g)
{
    uint32_t rd = s->preq_head, wr = s->preq_head;
    bool dropped_any = false;
    while (rd != s->preq_tail) {
        sender_preq_entry_t *e = &s->preq[rd % s->ring_cap];
        /* Drop this group's media entries, but PRESERVE any terminal
         * END_OF_TRACK marker (is_end) even when it shares the dropped group --
         * it is the only reliable EOS and must survive the eviction. */
        if (e->track == t && e->group_seq == g && !e->is_end) {
            preq_note_dropped(s, t, e);   /* THE shared accounting chokepoint */
            preq_entry_release(s, e);     /* clears the slot */
            dropped_any = true;
        } else {
            if (wr != rd) s->preq[wr % s->ring_cap] = *e;
            wr++;
        }
        rd++;
    }
    s->preq_tail = wr;
    /* A Group that lost queued media to eviction cannot be continued: a later
     * Object would follow a hole in it, and once its wire subgroup is reset
     * the drain would reopen the Group at Object 0 and repeat a Location. It
     * is closed for writes, so the next write must be a sync point, which
     * opens a new Group. (A no-demand drop instead leaves the Group open and
     * discards the rest of it in the drain; see skip_open.) */
    if (dropped_any && t->group_open && g == t->group_seq) {
        t->group_open = false;
        t->cur_group_anchored = false;
    }
    if (dropped_any) {
        /* Counting is done by preq_note_dropped above. What remains here is
         * this path's own wire semantics: if the subgroup for this group is
         * open, its tail just went away -- the drain must RESET it, not leave
         * it truncated. */
        if (sender_holds_group(t, g))
            t->pending_reset = true;
    }
}


/* The most recent sync-starting group seq for `track`, or false when that
 * track has none queued. The sync anchor is PER TRACK: each track's
 * retained suffix must independently begin at a sync point. mu held. */
static bool preq_track_anchor(const moq_media_sender_t *s,
                              const moq_media_track_t *track,
                              uint64_t *out_seq)
{
    bool found = false;
    for (uint32_t i = s->preq_head; i != s->preq_tail; i++) {
        const sender_preq_entry_t *e = &s->preq[i % s->ring_cap];
        if (e->track == track && e->starts_group && e->is_sync) {
            *out_seq = e->group_seq;
            found = true;
        }
    }
    return found;
}


/* Whether an entry's group is evictable for an incoming object of
 * `in_track` (in_anchors = the incoming object re-anchors its own track,
 * making all of that track's queued groups droppable). The one eviction
 * rule: preq_evict_one and preq_make_room's feasibility count both use it.
 * mu held. */
static bool entry_evictable(moq_media_sender_t *s,
                            const sender_preq_entry_t *e,
                            const moq_media_track_t *in_track, bool in_anchors)
{
    /* A queued END_OF_TRACK marker is the ONLY chance to emit EOS reliably
     * (end_track is idempotent and later writes are refused), so it is never
     * evictable -- a drop policy must surface WOULD_BLOCK rather than discard
     * it. Excluding it here keeps preq_make_room's feasibility count honest. */
    if (e->is_end) return false;
    if (e->track == in_track) {
        if (in_anchors) return true;
        /* The incoming object continues its track's open Group, so that
         * Group is kept: evicting it would close the Group the object is
         * about to join. */
        if (in_track->group_open && e->group_seq == in_track->group_seq)
            return false;
    }
    uint64_t aseq = 0;
    bool found = preq_track_anchor(s, e->track, &aseq);
    return !found || e->group_seq < aseq;
}


/* Evict the oldest group that can go without stranding any track's
 * retained GOP: a group (track T, seq g) is evictable iff g precedes T's
 * retained anchor. The incoming object's own track, when the incoming
 * object is itself a fresh anchor, has ALL its queued groups evictable
 * (the new keyframe re-anchors it). Returns false when nothing is
 * evictable (caller surfaces WOULD_BLOCK). mu held. */
static bool preq_evict_one(moq_media_sender_t *s,
                           const moq_media_track_t *in_track, bool in_anchors)
{
    for (uint32_t i = s->preq_head; i != s->preq_tail; i++) {
        sender_preq_entry_t *e = &s->preq[i % s->ring_cap];
        if (entry_evictable(s, e, in_track, in_anchors)) {
            preq_evict_group(s, e->track, e->group_seq);
            return true;
        }
    }
    return false;
}


/* Make room for one entry of `need` bytes per the backpressure policy,
 * preserving every track's sync anchor (§7.2). Drop policies first prove
 * the object CAN fit after evicting everything evictable -- only then do
 * they mutate the queue. If it can never fit (a retained GOP plus the
 * object exceed the bound, or the object alone exceeds the byte budget),
 * return false with the queue untouched, honoring the no-mutation
 * contract for the WOULD_BLOCK the caller will surface. mu held. */
static bool preq_make_room(moq_media_sender_t *s,
                           const moq_media_track_t *track,
                           bool incoming_anchors, size_t need)
{
    if (preq_fits(s, need)) return true;

    bool drop = (s->backpressure == MOQ_MEDIA_SEND_BP_DROP_TO_KEYFRAME ||
                 s->backpressure == MOQ_MEDIA_SEND_BP_DROP_GROUP);
    if (!drop)
        return false;   /* lossless/would-block: never evict */

    /* Feasibility (no mutation): sum what eviction could free, then check
     * the retained remainder + the incoming object fits both bounds. */
    uint64_t free_bytes = 0;
    uint32_t free_cnt = 0;
    for (uint32_t i = s->preq_head; i != s->preq_tail; i++) {
        const sender_preq_entry_t *e = &s->preq[i % s->ring_cap];
        if (entry_evictable(s, e, track, incoming_anchors)) {
            free_bytes += e->bytes;
            free_cnt++;
        }
    }
    uint32_t used_cnt = s->preq_tail - s->preq_head;
    if (used_cnt - free_cnt + 1 > active_obj_cap(s)) return false;
    if (s->preq_bytes - free_bytes + need > active_byte_cap(s)) return false;

    while (!preq_fits(s, need)) {
        if (!preq_evict_one(s, track, incoming_anchors))
            return false;   /* unreachable: feasibility proved room exists */
    }
    return true;
}


/* -- Drain: emit queued objects (network thread, s->mu held) ---------- */

/* The LOC-04 block an AUTO sender resolved to draft 22 generates for a RAW
 * object written without a metadata snapshot, from the typed fields:
 *   - a capture time is wall-clock time: an unscaled Timestamp (0x10), which
 *     LOC-04 2.3.1.1 defines as microseconds since the Unix epoch;
 *   - otherwise the presentation time is media time: the Timestamp with a
 *     Timescale (0x08) of 1000000, which LOC-04 2.3.1.2 makes media time with
 *     an application-defined anchor -- never a presentation time passed off
 *     as a Unix wall clock;
 *   - on video, a one-byte frame marking (0x09) whose I bit is the object's
 *     sync flag. */
static bool sender_build_typed_loc04(moq_media_sender_t *s,
                                     const sender_preq_entry_t *e,
                                     moq_rcbuf_t **out)
{
    moq_loc_metadata_t m;
    moq_loc_metadata_init_sized(&m, sizeof(m));
    m.present = MOQ_LOC_METADATA_HAS_TIMESTAMP;
    if (e->has_capture_time) {
        m.timestamp = e->capture_time_us;
    } else {
        m.timestamp = e->pts_us;
        m.present |= MOQ_LOC_METADATA_HAS_TIMESCALE;
        m.timescale = 1000000u;
    }
    if (e->track->media_type == MOQ_MEDIA_TYPE_VIDEO) {
        m.present |= MOQ_LOC_METADATA_HAS_MARKING;
        m.marking_len = 1;
        m.marking[0] = e->keyframe ? 0x20u : 0x00u;   /* RFC 9626 I bit */
    }
    return moq_loc_encode_metadata(&s->alloc, MOQ_VERSION_DRAFT_22,
                                   MOQ_LOC_PROFILE_04, &m, out) == MOQ_OK &&
           *out;
}


/* Legacy RAW generates LOC-01 capture/PTS fallback plus video sync marking.
 * D22 RAW reuses the already-owned metadata snapshot, or, for an AUTO sender's
 * object written without one, generates LOC-04 from the typed fields; CMAF
 * passes its complete app block unchanged. RAW extras remain refused. *out is
 * a new ref (or NULL) the caller decrefs. Invalid legacy eligibility or
 * allocation fails closed. */
static bool sender_build_props(moq_media_sender_t *s,
                               const sender_preq_entry_t *e,
                               moq_rcbuf_t **out)
{
    *out = NULL;
    moq_media_track_t *t = e->track;

    if (t->packaging == MOQ_MEDIA_PACKAGING_RAW &&
        s->companion_profile == MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC &&
        !e->properties && s->companion_auto) {
        if (!sender_version_usable(s)) return false;
        return sender_build_typed_loc04(s, e, out);
    }
    if (t->packaging != MOQ_MEDIA_PACKAGING_RAW ||
        s->companion_profile == MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC) {
        if (e->properties) *out = moq_rcbuf_incref(e->properties);
        return true;
    }

    moq_loc_headers_t h;
    moq_loc_headers_init(&h);
    h.has_timestamp = true;
    /* Legacy LOC-01 Capture Timestamp (0x02) on drafts16/18. Emit the
     * app-supplied capture_time_us verbatim when present (no clamp/scale --
     * large epoch values are valid); otherwise fall back to
     * presentation_time_us so callers without a capture clock keep a
     * timestamped LOC header. */
    h.timestamp = e->has_capture_time ? e->capture_time_us : e->pts_us;
    if (t->media_type == MOQ_MEDIA_TYPE_VIDEO) {
        h.has_video_frame_marking = true;
        h.video_frame_marking.independent = e->keyframe;
    }
    /* Fail closed on an unobserved or unsupported draft: an object may not
     * be encoded before the session's version has been latched, and there
     * is deliberately no draft-16 fallback here. */
    if (!sender_version_usable(s))
        return false;
    moq_rcbuf_t *loc = NULL;
    if (moq_loc_encode(&s->alloc, s->transport_version,
                       MOQ_LOC_PROFILE_01, &h, &loc) != MOQ_OK)
        return false;
    *out = loc;   /* has_timestamp is always set, so loc is non-NULL */
    return true;
}


/* Only representability is needed here, not a converted clock or anchor.
 * Exact fractional scaling uses bounded quotient/remainder arithmetic. */
static bool sender_loc_time_usable(uint64_t ticks, uint64_t scale)
{
    if (!scale) return false;
    uint64_t whole = ticks / scale, rem = ticks % scale;
    if (whole > UINT64_MAX / 1000000u) return false;
    uint64_t frac = 0, r = 0;
    for (uint32_t bit = UINT32_C(1) << 19; bit; bit >>= 1) {
        frac *= 2;
        if (r >= scale - r) { r -= scale - r; ++frac; }
        else r += r;
        if (UINT32_C(1000000) & bit) {
            if (r >= scale - rem) { r -= scale - rem; ++frac; }
            else r += rem;
        }
    }
    return frac <= UINT64_MAX - whole * 1000000u;
}


static moq_result_t sender_encode_metadata(moq_media_sender_t *s,
    const moq_media_send_object_t *obj, moq_rcbuf_t **out)
{
    if (obj->struct_size < offsetof(moq_media_send_object_t, metadata) +
                           sizeof(obj->metadata) || !obj->metadata)
        return MOQ_ERR_INVAL;
    const moq_media_metadata_t *m = obj->metadata;
    if (m->struct_size < MOQ_MEDIA_METADATA_V0_SIZE ||
        m->loc.struct_size < MOQ_LOC_METADATA_V0_SIZE ||
        !(m->loc.present & MOQ_LOC_METADATA_HAS_TIMESTAMP))
        return MOQ_ERR_INVAL;
    if ((m->loc.present & MOQ_LOC_METADATA_HAS_TIMESCALE) &&
        !sender_loc_time_usable(m->loc.timestamp, m->loc.timescale))
        return MOQ_ERR_INVAL;
    /* LOC-04 pairs only with draft 22, so its codec is known even before an
     * AUTO sender has latched the session's draft. */
    return moq_loc_encode_metadata(&s->alloc, MOQ_VERSION_DRAFT_22,
                                   MOQ_LOC_PROFILE_04, &m->loc, out);
}


/* THE pending-reset state transition. An armed `pending_reset` is lifecycle
 * work OWED by the track: any abandoned open subgroup must be RESET on the wire
 * before anything can turn it into a clean FIN. An unopened pending write needs
 * only local release; the facade reports the actual reset effects.
 *
 *   MOQ_OK        -- and only then -- clears the obligation, clears emit_open,
 *                    and counts it once if any retry actually reset a slot.
 *   WOULD_BLOCK   -- shared capacity: the obligation and any actual reset
 *                    effects are preserved for retry; nothing is counted yet.
 *   any other <0  -- fatal; the obligation is NOT cleared and no successful
 *                    abandonment is counted.
 *
 * Returns MOQ_OK when there was nothing to do, so callers can treat "not owed"
 * and "discharged" alike. s->mu held. */
moq_result_t sender_flush_reset(moq_media_sender_t *s,
                                moq_media_track_t *t, uint64_t now_us)
{
    if (!t->pending_reset) return MOQ_OK;
    if (!t->pub_track) return MOQ_OK;   /* nothing on the wire to abandon */
    t->drain_reset_tried = true;
#ifdef MOQ_MEDIA_SENDER_TESTING
    s->test_reset_attempts++;
    if (s->test_refuse_reset) {
        s->test_refuse_reset--;
        return MOQ_ERR_WOULD_BLOCK;                  /* retryable, untouched */
    }
#endif
    bool did_reset = false;
    moq_result_t rrc = moq_core_internal_pub_reset_group(
        s->pub, t->pub_track, 0x10, now_us, &did_reset);
    t->reset_had_effect |= did_reset;
    if (rrc == MOQ_ERR_WOULD_BLOCK) return rrc;      /* obligation retained */
    if (rrc < 0) {
        sender_set_fatal_locked(s, MOQ_MEDIA_SENDER_FATAL_SETUP_FAILED);
        return rrc;                                   /* obligation retained */
    }
    t->pending_reset = false;
    t->emit_open = false;
    t->write_blocked = false;
    if (t->reset_had_effect) {
        s->stats.groups_abandoned++;
        t->tstats.groups_abandoned++;
    }
    t->reset_had_effect = false;
    return MOQ_OK;
}


void sender_drain(moq_media_sender_t *s, uint64_t now_us)
{
    /* ONE forward compaction pass over [preq_head, preq_tail), using the same
     * read/write-cursor idiom as preq_evict_group:
     * an entry that can be emitted now is emitted and released, an entry that
     * cannot is RETAINED IN PLACE by copying it to the write cursor. preq_head
     * never moves; preq_tail becomes the write cursor at the end. Work per
     * invocation is therefore one pass over the current depth -- no rotation,
     * no restart, no nested full-ring helper, no allocation.
     *
     * Lack of demand is TRACK-LOCAL and may be skipped: an undemanded track
     * must not stop demanded media behind it. A real MOQ_ERR_WOULD_BLOCK from
     * a facade/session operation is SHARED output-capacity backpressure: the
     * refused entry and the whole unprocessed suffix are retained verbatim and
     * this invocation stops. */
    uint32_t rd = s->preq_head, wr = s->preq_head;
    const uint32_t depth0 = s->preq_tail - s->preq_head;
    bool stop = false;          /* shared capacity refused, or fatal */

    /* Per-pass scratch, cleared before the pass. */
    for (size_t i = 0; i < s->track_count; i++) {
        s->tracks[i]->drain_hold = false;
        s->tracks[i]->drain_dropped = false;
        s->tracks[i]->drain_reset_tried = false;
        s->tracks[i]->drain_demand = -1;
    }

    while (rd != s->preq_tail) {
        sender_preq_entry_t *e = &s->preq[rd % s->ring_cap];
        moq_media_track_t *t = e->track;

        /* Shared capacity already refused: retain this and every later entry
         * exactly as they are. */
        if (stop) {
            if (wr != rd) s->preq[wr % s->ring_cap] = *e;
            wr++; rd++;
            continue;
        }

        /* A removed track's queued work is discarded: the catalog no longer
         * declares it and its pub track is ended by the hook. This reaches
         * both moq_media_sender_remove_track() and moq_media_sender_complete()
         * (whose finalize marks every track removed once nothing of it is left
         * queued), and the discard is COUNTED like
         * any other -- the public conservation identity is unconditional.
         * The private terminal marker is not media and is not counted. No wire
         * RESET is armed here: removed-track teardown runs before the drain
         * and owns its terminal semantics. */
        if (t->removed) {
            if (!e->is_end) preq_note_dropped(s, t, e);
            preq_entry_release(s, e);
            rd++;
            continue;
        }

        /* This track already retained an entry in this pass: everything later
         * on the same track stays behind it, END_OF_TRACK included. That is
         * what keeps per-track FIFO exact and stops a terminal overtaking its
         * own held media. */
        if (t->drain_hold) {
            if (wr != rd) s->preq[wr % s->ring_cap] = *e;
            wr++; rd++;
            continue;
        }

        if (t->drain_demand < 0)
            t->drain_demand = sender_track_demand(s, t) ? 1 : 0;
        bool has_sub = t->drain_demand == 1;

        /* Terminal marker: emit a reliable END_OF_TRACK after this track's
         * queued objects (per-track order is preserved by drain_hold above).
         * The facade closes any open subgroup and FINs a fresh one; the session
         * stays alive.
         *
         * Once admitted, handled BEFORE the no-demand branch below:
         * moq_pub_end_track() treats "no subscriber" as local terminal success.
         * A registered pull track, or a resolved push publication whose peer
         * left, can drain its terminal here without overtaking held media. */
        if (e->is_end) {
            if (!sender_terminal_admitted(s, t)) {
                t->drain_hold = true;  /* track-local wait, not shared capacity */
                if (wr != rd) s->preq[wr % s->ring_cap] = *e;
                wr++; rd++;
                continue;
            }
            /* A drop abandoned an open group: the obligation is discharged
             * through THE one transition before the terminal can turn the same
             * subgroup into a clean FIN -- independent of the cached demand
             * answer, which is not what decides whether the abandon is owed. */
            {
                moq_result_t rrc = sender_flush_reset(s, t, now_us);
                if (rrc != MOQ_OK) {   /* shared capacity, or fatal */
                    t->drain_hold = true;
                    stop = true;
                    if (wr != rd) s->preq[wr % s->ring_cap] = *e;
                    wr++; rd++;
                    continue;
                }
            }
            moq_result_t erc = moq_pub_end_track(s->pub, t->pub_track, now_us);
            if (erc != MOQ_OK) {
                /* WOULD_BLOCK is shared capacity, NOT a fatal: retry later. */
                if (erc != MOQ_ERR_WOULD_BLOCK)
                    sender_set_fatal_locked(s, MOQ_MEDIA_SENDER_FATAL_SETUP_FAILED);
                t->drain_hold = true;
                stop = true;
                if (wr != rd) s->preq[wr % s->ring_cap] = *e;
                wr++; rd++;
                continue;
            }
            t->emit_open = false;
            t->end_sent = true;
            /* pending_reset is NOT cleared here: the only transition that may
             * clear it is sender_flush_reset(), which ran above. */
            preq_entry_release(s, e);
            rd++;
            continue;
        }

        /* No demand: drop_without_demand releases this track's media as it is
         * encountered (staying at the live edge) and the pass CONTINUES with
         * other tracks; the default retains it for its own track only. */
        if (!has_sub) {
            if (s->drop_without_demand) {
                preq_note_dropped(s, t, e);
                t->skip_group = e->group_seq;
                t->skip_open = true;
                /* Arm the wire RESET once per affected track in this pass: an
                 * open wire subgroup just lost its tail and must not be
                 * truncated. */
                if (!t->drain_dropped) {
                    t->drain_dropped = true;
                    if (t->emit_open || sender_holds_group(t, e->group_seq))
                        t->pending_reset = true;
                }
                preq_entry_release(s, e);
                rd++;
                continue;
            }
            t->drain_hold = true;
            if (wr != rd) s->preq[wr % s->ring_cap] = *e;
            wr++; rd++;
            continue;
        }

        /* Demand arrived inside a group whose head was dropped. Sending the
         * rest would start the subscriber on a mid-group object numbered 0,
         * so it waits for the next group start instead. */
        if (t->skip_open && e->group_seq == t->skip_group) {
            preq_note_dropped(s, t, e);
            preq_entry_release(s, e);
            rd++;
            continue;
        }
        t->skip_open = false;

        /* A drop abandoned this track's open group: reset it on the wire
         * before emitting the next (newer) group. */
        if (t->pending_reset) {
            moq_result_t rrc = sender_flush_reset(s, t, now_us);
            if (rrc != MOQ_OK) {   /* shared capacity, or fatal */
                t->drain_hold = true;
                stop = true;
                if (wr != rd) s->preq[wr % s->ring_cap] = *e;
                wr++; rd++;
                continue;
            }
        }

        moq_rcbuf_t *props = NULL;
        if (!sender_build_props(s, e, &props)) {
            sender_set_fatal_locked(s, MOQ_MEDIA_SENDER_FATAL_SETUP_FAILED);
            t->drain_hold = true;
            stop = true;
            if (wr != rd) s->preq[wr % s->ring_cap] = *e;
            wr++; rd++;
            continue;
        }

        bool new_group = !t->emit_open || t->emit_group != e->group_seq;
        uint64_t oid = new_group ? 0 : t->emit_obj;

        moq_pub_object_cfg_t ocfg;
        moq_pub_object_cfg_init_sized(&ocfg, sizeof(ocfg));
        ocfg.group_id = e->group_seq;
        ocfg.object_id = oid;
        ocfg.payload = e->payload;
        ocfg.properties = props;
        /* v0 emits one subgroup per group, so each subgroup IS its group's
         * only (hence last) subgroup: END_OF_GROUP is a subgroup-header bit
         * set at open and required uniform across the subgroup, so it is
         * true for every object. The group is delimited on the wire by the
         * group_id change to the next group. */
        ocfg.end_of_group = true;
        moq_result_t wrc = moq_pub_write_object_ex(s->pub, t->pub_track,
                                                   &ocfg, now_us);
        if (props) moq_rcbuf_decref(props);   /* facade took its own ref */

        if (wrc != MOQ_OK) {
            /* WOULD_BLOCK is shared capacity, NOT a fatal: retry later. A
             * drop of this Group before the retry must abandon the write. */
            if (wrc == MOQ_ERR_WOULD_BLOCK) {
                t->write_blocked = true;
                t->blocked_group = e->group_seq;
            } else {
                sender_set_fatal_locked(s, MOQ_MEDIA_SENDER_FATAL_SETUP_FAILED);
            }
            t->drain_hold = true;
            stop = true;
            if (wr != rd) s->preq[wr % s->ring_cap] = *e;
            wr++; rd++;
            continue;
        }

        t->write_blocked = false;
        t->emit_group = e->group_seq;
        t->emit_open = true;          /* facade subgroup stays open until the
                                         next group; complete groups have no
                                         queued tail, so no spurious reset */
        t->emit_obj = oid + 1;
        s->stats.objects_sent++;
        t->tstats.objects_sent++;
        t->tstats.objects_queued--;
        t->tstats.bytes_queued -= e->bytes;

        /* Generated SAP timeline: record this Object's declared SAP at its real
         * wire Location (group, oid). Group starts always record (write()
         * already required a declared TYPE_1/2); a mid-group Object records only
         * an actual SAP (declared type != NONE). A dropped Object never reaches
         * here, so it correctly yields no record. */
        if (t->sap_timeline && !t->sap_timeline->auth_failed && e->has_sap_type &&
            (oid == 0 || e->sap_type != (uint8_t)MOQ_SAP_NONE))
            sap_history_append(s, t->sap_timeline, e->group_seq, oid,
                               e->sap_type, sap_ept_ms_from_us(e->pts_us));

        /* Generated MSF §7 media timeline: record one explicit entry per media
         * group, at the group-start Object (oid == 0), at its real wire Location.
         * mediaTime is the object's presentation time (ms); wallclock is the emit
         * time (ms) for a live track, 0 once the track is non-live/VOD. */
        if (t->media_timeline && !t->media_timeline->auth_failed && oid == 0)
            mt_history_append(s, t->media_timeline, e->group_seq, oid,
                              e->pts_us / 1000u,
                              t->is_live ? now_us / 1000u : 0u);

        s->preq_bytes -= e->bytes;
        preq_entry_release(s, e);     /* decrefs the entry's refs */
        rd++;
    }

    s->preq_tail = wr;
    /* Any slot freed before a refusal must still wake blocked writers. */
    if ((s->preq_tail - s->preq_head) < depth0)
        pthread_cond_broadcast(&s->space_cv);

    /* An armed pending_reset is owed EVEN IF the track has no queued entry for
     * the pass above to hang it off -- an abandoned tail with nothing behind it
     * must still be RESET, not left open. Sweep every track once (bounded
     * O(track_count)), including any obligation the no-demand discards just
     * armed in this same pass, but SKIP a track the queue path already
     * attempted: nothing frees capacity in between, so a second attempt in the
     * same pass is futile. Either way the obligation stays retryable for the
     * next production hook; a fatal is already latched. */
    if (!s->fatal) {
        for (size_t i = 0; i < s->track_count; i++) {
            moq_media_track_t *t = s->tracks[i];
            if (!t->pending_reset || t->removed || t->drain_reset_tried)
                continue;
            if (sender_flush_reset(s, t, now_us) != MOQ_OK) break;
        }
    }

    /* Emit generated timelines from their side history. They never enter the
     * media FIFO above, so an unsubscribed timeline track cannot stall media;
     * each is gated on its own subscriber (SAP MSF §8.3 / media §7.3). */
    for (size_t i = 0; i < s->track_count; i++) {
        if (s->tracks[i]->auth_failed) continue;
        if (s->tracks[i]->is_event_timeline)
            sender_emit_sap_for(s, s->tracks[i], now_us);
        else if (s->tracks[i]->is_media_timeline)
            sender_emit_mt_for(s, s->tracks[i], now_us);
    }

    if (!s->fatal) sender_drain_completion(s, now_us);
}

#ifdef MOQ_MEDIA_SENDER_TESTING
/* Test-only observation that a BLOCK_TIMEOUT write has entered its blocking
 * path (called with s->mu held, after the stall is counted and before the
 * deadline is constructed; the hook must only signal, never call back into
 * the sender). NULL disables. */
static void (*g_test_wait_entry)(void *ctx);

static void *g_test_wait_entry_ctx;

void moq_media_sender_test_set_wait_entry_hook(void (*fn)(void *), void *ctx);

void moq_media_sender_test_set_wait_entry_hook(void (*fn)(void *), void *ctx)
{
    g_test_wait_entry = fn;
    g_test_wait_entry_ctx = ctx;
}

#define SENDER_WAIT_ENTRY_HOOK() \
    do { if (g_test_wait_entry) g_test_wait_entry(g_test_wait_entry_ctx); } while (0)


static int (*g_test_cond_wait)(const struct timespec *abs, void *ctx);

static void *g_test_cond_wait_ctx;

void moq_media_sender_test_set_cond_wait_hook(int (*fn)(const struct timespec *, void *), void *ctx);

void moq_media_sender_test_set_cond_wait_hook(int (*fn)(const struct timespec *, void *), void *ctx)
{
    g_test_cond_wait = fn;
    g_test_cond_wait_ctx = ctx;
}
#define SENDER_COND_TIMEDWAIT(cv, mu, abs) \
    (g_test_cond_wait ? g_test_cond_wait((abs), g_test_cond_wait_ctx) \
                      : pthread_cond_timedwait((cv), (mu), (abs)))


#else
#define SENDER_WAIT_ENTRY_HOOK() ((void)0)

#define SENDER_COND_TIMEDWAIT(cv, mu, abs) pthread_cond_timedwait((cv), (mu), (abs))


#endif


/* -- BLOCK_TIMEOUT budget arithmetic ---------------------------------------
 * The wait keeps the REALTIME origin of the first clock read and the caller's
 * full uint64 microsecond duration as its reference; the absolute deadline is
 * never materialised, so no duration is narrowed and no budget is clamped.
 * Each iteration classifies what is left and forms only the NEXT bounded
 * absolute wait, which is the one value that must fit time_t.
 *
 * Representation, assumed and checked below: time_t is a signed two's
 * complement integer with no padding bits and no wider than uint64_t. Under
 * exactly those assumptions an ordered seconds difference can be taken in
 * unsigned arithmetic (converting a negative value to uint64_t is well
 * defined, and the difference of two converted values is the true difference
 * for any in-range ordered pair), and the maximum below is the value with
 * every non-sign bit set. The assertions check signedness, width and two's
 * complement; absence of padding bits is an ASSUMPTION of the supported
 * platforms, not something they verify. Narrower supported widths are covered
 * by the fixture's separate arithmetic model, not by this host's execution. */
#define SENDER_SLICE_NS 50000000L


static const time_t SENDER_TIME_T_MAX =
    (time_t)(((uintmax_t)1 << (sizeof(time_t) * CHAR_BIT - 1)) - 1);


typedef enum {
    SENDER_BUDGET_EXPIRED,
    SENDER_BUDGET_UNDER_SLICE,     /* less than one slice remains */
    SENDER_BUDGET_AT_LEAST_SLICE
} sender_budget_t;


static bool ts_ge(struct timespec a, struct timespec b)
{
    return a.tv_sec > b.tv_sec ||
           (a.tv_sec == b.tv_sec && a.tv_nsec >= b.tv_nsec);
}


/* a - b for a >= b, as (seconds, nanoseconds). The seconds subtraction is
 * unsigned, so it cannot overflow for any pair of representable instants. */
static void ts_diff(struct timespec a, struct timespec b,
                    uint64_t *out_sec, long *out_nsec)
{
    uint64_t sec = (uint64_t)a.tv_sec - (uint64_t)b.tv_sec;
    long nsec = a.tv_nsec - b.tv_nsec;
    if (nsec < 0) { nsec += SENDER_NS_PER_S; sec -= 1u; }
    *out_sec = sec;
    *out_nsec = nsec;
}


/* How much of the requested budget is left at `now`. On UNDER_SLICE the exact
 * remainder (in nanoseconds, sub-microsecond precision preserved) is written
 * to *rem_ns; the other classes need no value. */
static sender_budget_t sender_budget(struct timespec origin, uint64_t dur_sec,
                                     long dur_nsec, struct timespec now,
                                     long *rem_ns)
{
    uint64_t rs;
    long rn;
    if (ts_ge(now, origin)) {
        uint64_t el_sec; long el_nsec;
        ts_diff(now, origin, &el_sec, &el_nsec);
        if (el_sec > dur_sec ||
            (el_sec == dur_sec && el_nsec >= dur_nsec))
            return SENDER_BUDGET_EXPIRED;
        rs = dur_sec - el_sec;
        rn = dur_nsec - el_nsec;
        if (rn < 0) { rn += SENDER_NS_PER_S; rs--; }
    } else {
        /* The clock moved BEFORE the origin. That distance is still to cover,
         * so the remaining budget is the requested duration plus it. A whole
         * second or more of backward distance already exceeds a slice, so the
         * class is known without forming a sum that could overflow. */
        uint64_t be_sec; long be_nsec;
        ts_diff(origin, now, &be_sec, &be_nsec);
        if (be_sec > 0) return SENDER_BUDGET_AT_LEAST_SLICE;
        rs = dur_sec;
        rn = dur_nsec + be_nsec;                 /* both < 1e9 */
        if (rn >= SENDER_NS_PER_S) { rn -= SENDER_NS_PER_S; rs++; }
    }
    if (rs > 0 || rn >= SENDER_SLICE_NS) return SENDER_BUDGET_AT_LEAST_SLICE;
    *rem_ns = rn;
    return SENDER_BUDGET_UNDER_SLICE;
}


/* acc + add fits in size_t; on success acc carries the sum. */
bool sender_sum_fits(size_t *acc, size_t add)
{
    if (add > SIZE_MAX - *acc) return false;
    *acc += add;
    return true;
}


/* Admit one media object of `track` into the send queue: the sync-anchor
 * rule, the backpressure policy, the Group id, and the enqueue. Called and
 * returns with s->mu held; a BLOCK_TIMEOUT wait releases it while waiting.
 * On MOQ_OK the queue owns the object's refs; on any other result nothing
 * was taken. The caller wakes the endpoint after unlocking when ready. */
moq_result_t sender_admit_locked(moq_media_sender_t *s,
                                 moq_media_track_t *track,
                                 const sender_admit_t *a)
{
    /* The queue takes ownership of both refs, so both count toward the
     * byte budget. */
    size_t need = moq_rcbuf_len(a->payload);
    size_t property_bytes = moq_rcbuf_len(a->properties);
    if (property_bytes > SIZE_MAX - need) {
        s->stats.last_error = MOQ_ERR_INVAL;
        return MOQ_ERR_INVAL;
    }
    need += property_bytes;

    /* Would this object open a new group? (Pure -- no mutation until the
     * enqueue is committed.) A sync point that opens a group is a stream
     * anchor: every group must begin with one. */
    bool opened = a->starts_group || !track->group_open;
    bool anchor = a->is_sync || (a->anchors_group && a->starts_group);
    bool incoming_anchors = anchor && opened;

    /* Sync-anchor invariant (§7.2), PER TRACK, against EMITTED-or-queued
     * anchor state (not just the queue): a group opened by a sync point
     * stays anchored even after that keyframe has drained off the ring, so
     * later deltas of a still-open group are accepted. A new group must be
     * opened by a sync point; a delta may only continue an already-anchored
     * group. Otherwise the object would be a non-decodable lead -- refuse,
     * taking no ownership. */
    if (opened) {
        if (!anchor) {
            s->stats.last_error = MOQ_ERR_WOULD_BLOCK;
            return MOQ_ERR_WOULD_BLOCK;   /* a group cannot lead with a delta */
        }
        /* The new group's ID must exist: after MOQ_QUIC_VARINT_MAX the track
         * has no further group (never a wrap or a reused ID). Refused here,
         * before anything is evicted or owned. */
        if (track->group_started && track->group_seq >= MOQ_QUIC_VARINT_MAX) {
            s->stats.last_error = MOQ_ERR_LIMIT;
            return MOQ_ERR_LIMIT;
        }
    } else if (!track->cur_group_anchored) {
        s->stats.last_error = MOQ_ERR_WOULD_BLOCK;
        return MOQ_ERR_WOULD_BLOCK;
    }

    moq_rcbuf_t *generated = NULL;
    if (a->metadata_src) {
        moq_result_t rc = sender_encode_metadata(s, a->metadata_src, &generated);
        if (rc != MOQ_OK) {
            s->stats.last_error = rc;
            return rc;
        }
        size_t n = moq_rcbuf_len(generated);
        if (n > SIZE_MAX - need) {
            moq_rcbuf_decref(generated);
            s->stats.last_error = MOQ_ERR_INVAL;
            return MOQ_ERR_INVAL;
        }
        need += n;
    }

    /* Make room per the backpressure policy. Drop policies evict (and may
     * arm a wire reset); RETURN_WOULD_BLOCK refuses immediately;
     * BLOCK_TIMEOUT waits for the drain to free space, breaking promptly on
     * terminal/interrupt and surfacing WOULD_BLOCK on timeout -- never a
     * silent drop. */
    if (a->completing ? !preq_fits(s, need)
                      : !preq_make_room(s, track, incoming_anchors, need)) {
        if (a->completing || s->backpressure != MOQ_MEDIA_SEND_BP_BLOCK_TIMEOUT) {
            moq_rcbuf_decref(generated);
            s->stats.last_error = MOQ_ERR_WOULD_BLOCK;
            return MOQ_ERR_WOULD_BLOCK;
        }
        s->stats.backpressure_stalls++;
        SENDER_WAIT_ENTRY_HOOK();
        moq_result_t wres = MOQ_OK;
        struct timespec origin;
        if (SENDER_CLOCK_REALTIME(&origin) != 0) {
            /* The budget cannot be anchored: report the system failure rather
             * than waiting against an unknown origin. */
            wres = MOQ_ERR_INTERNAL;
        } else {
            const uint64_t dur_sec = s->block_timeout_us / 1000000ull;
            const long dur_nsec =
                (long)((s->block_timeout_us % 1000000ull) * 1000ull);
            for (;;) {
                if (track->auth_failed) { wres = MOQ_ERR_WRONG_STATE; break; }
                if (preq_fits(s, need)) { wres = MOQ_OK; break; }
                if (s->fatal) { wres = MOQ_ERR_CLOSED; break; }
                /* ep->mu-free closed check (sender_ep_closed): this loop holds s->mu,
                 * and the endpoint hook lock order is ep->mu -> s->mu, so calling the
                 * public moq_endpoint_is_closed() (which takes ep->mu) here would
                 * invert it and can deadlock the network thread. */
                if (sender_ep_closed(s)) { wres = MOQ_ERR_CLOSED; break; }
                if (moq_endpoint_interrupted_internal(s->ep)) {
                    wres = MOQ_ERR_INTERRUPTED; break;
                }
                struct timespec now;
                if (SENDER_CLOCK_REALTIME(&now) != 0) {
                    wres = MOQ_ERR_INTERNAL; break;
                }
                long rem_ns = 0;
                sender_budget_t cls = sender_budget(origin, dur_sec, dur_nsec,
                                                    now, &rem_ns);
                if (cls == SENDER_BUDGET_EXPIRED) {
                    /* The capacity check at the top of this iteration already
                     * ran under this same lock hold, so it cannot have changed
                     * since: the budget is spent with no room. */
                    wres = MOQ_ERR_WOULD_BLOCK;
                    break;
                }
                /* Bounded slice so external terminal/interrupt (which do not
                 * signal space_cv) are observed promptly, not at full timeout;
                 * a shorter remainder is waited exactly, to the nanosecond. */
                struct timespec until = now;
                until.tv_nsec += (cls == SENDER_BUDGET_AT_LEAST_SLICE)
                                 ? SENDER_SLICE_NS : rem_ns;
                if (until.tv_nsec >= SENDER_NS_PER_S) {
                    if (until.tv_sec >= SENDER_TIME_T_MAX) {
                        /* The next required instant is not representable. A
                         * maximum second is legal; going past it is not, and
                         * clamping would either expire the wait early or make
                         * no progress. */
                        wres = MOQ_ERR_INTERNAL; break;
                    }
                    until.tv_sec++;
                    until.tv_nsec -= SENDER_NS_PER_S;
                }
                int wrc = SENDER_COND_TIMEDWAIT(&s->space_cv, &s->mu, &until);
                if (wrc != 0 && wrc != ETIMEDOUT) {
                    wres = MOQ_ERR_INTERNAL; break;
                }
                /* A wake or a slice timeout re-enters the loop, where capacity,
                 * terminal state, the interrupt latch and then expiry are all
                 * re-evaluated in that order. */
            }
        }
        if (wres != MOQ_OK) {
            moq_rcbuf_decref(generated);
            s->stats.last_error = wres;
            return wres;   /* no ownership taken */
        }
        /* The wait released s->mu. The track may have been ended or removed
         * meanwhile, or its open Group closed by a drop; the object is then
         * refused as it would be now, and the caller retries. */
        if (track->end_requested || track->removed || track->auth_failed || track->vod_converted) {
            moq_rcbuf_decref(generated);
            s->stats.last_error = MOQ_ERR_WRONG_STATE;
            return MOQ_ERR_WRONG_STATE;
        }
        if ((a->starts_group || !track->group_open) != opened) {
            moq_rcbuf_decref(generated);
            s->stats.last_error = MOQ_ERR_WOULD_BLOCK;
            return MOQ_ERR_WOULD_BLOCK;
        }
    }

    /* Commit the service group id: a new group bumps the seq (the first
     * group ever is the instance's group base, sender_group_base). group_open
     * tracks completion separately so an ends_group does not make the next
     * group reuse the same id.
     * cur_group_anchored records that this group was opened by a sync
     * point, so a later delta is accepted even after the keyframe drains. */
    if (opened) {
        track->group_seq = track->group_started ? track->group_seq + 1
                                                : sender_group_base(s);
        track->group_started = true;
        track->group_open = true;
        track->cur_group_anchored = anchor;
    }

    sender_preq_entry_t *e = &s->preq[s->preq_tail % s->ring_cap];
    memset(e, 0, sizeof(*e));
    e->track = track;
    e->payload = a->payload;            /* ownership transfers */
    e->properties = generated ? generated : a->properties;
    e->is_sync = anchor;
    e->keyframe = a->is_sync;
    e->starts_group = opened;
    e->ends_group = a->ends_group;
    e->group_seq = track->group_seq;
    e->pts_us = a->pts_us;
    e->has_capture_time = a->has_capture_time;
    e->capture_time_us = a->capture_time_us;
    e->bytes = need;
    /* Carry the SAP declaration so the timeline record is generated at emit
     * time with the real wire Location (a dropped Object then yields no record). */
    if (track->sap_timeline && a->has_sap_type) {
        e->has_sap_type = true;
        e->sap_type = a->sap_type;
    }
    s->preq_bytes += need;
    s->preq_tail++;
    preq_note_enqueued(s, e);
    s->stats.objects_written++;
    track->tstats.objects_written++;
    track->tstats.objects_queued++;
    track->tstats.bytes_queued += need;
    if (a->ends_group) track->group_open = false;
    return MOQ_OK;
}



/* -- Write (app thread) ----------------------------------------------- */

moq_result_t moq_media_sender_write(moq_media_sender_t *s,
                                    moq_media_track_t *track,
                                    const moq_media_send_object_t *obj)
{
    /* Only obj fields are touched here; the opaque track (shared with the
     * receiver API) is NOT dereferenced until proven sender-owned below. */
    if (!s || !track || !obj) return MOQ_ERR_INVAL;
    if (obj->struct_size < MOQ_MEDIA_SEND_OBJECT_V0_SIZE)
        return MOQ_ERR_INVAL;
    if (!obj->payload) return MOQ_ERR_INVAL;
    /* A declared SAP type must be concrete (CMSF §3.6.1: 0/1/2/3); the internal
     * UNKNOWN sentinel is not a serializable/declarable value. */
    if (obj->has_sap_type &&
        obj->sap_type != MOQ_SAP_NONE && obj->sap_type != MOQ_SAP_TYPE_1 &&
        obj->sap_type != MOQ_SAP_TYPE_2 && obj->sap_type != MOQ_SAP_TYPE_3)
        return MOQ_ERR_INVAL;

    if (s->ep && moq_endpoint_interrupted_internal(s->ep))
        return MOQ_ERR_INTERRUPTED;
    if (sender_terminal(s)) return MOQ_ERR_CLOSED;

    pthread_mutex_lock(&s->mu);
    if (s->fatal) { pthread_mutex_unlock(&s->mu); return MOQ_ERR_CLOSED; }

    /* Verify the track belongs to this sender BEFORE touching any of its
     * fields -- a foreign / receiver / stale handle must not be
     * dereferenced. (The private catalog track is not in s->tracks, so it
     * is rejected here too.) */
    bool owned = false;
    for (size_t i = 0; i < s->track_count; i++)
        if (s->tracks[i] == track) { owned = true; break; }
    if (!owned) { pthread_mutex_unlock(&s->mu); return MOQ_ERR_INVAL; }

    /* A generated timeline track (SAP or media) is service-owned: the app never
     * writes to it (its objects are synthesized from the media track). */
    if (track_is_generated_timeline(track)) {
        pthread_mutex_unlock(&s->mu);
        return MOQ_ERR_INVAL;
    }

    /* No writes after the track has been ended, removed from the catalog, or
     * converted to VOD (V4: isLive=false means no new content). */
    if (track->end_requested || track->removed || track->auth_failed || track->vod_converted) {
        pthread_mutex_unlock(&s->mu);
        return MOQ_ERR_WRONG_STATE;
    }

    if (sender_companion_refused_locked(s)) {
        s->stats.last_error = MOQ_ERR_UNSUPPORTED;
        pthread_mutex_unlock(&s->mu);
        return MOQ_ERR_UNSUPPORTED;
    }

    /* The legacy LOC-01 Capture Timestamp on drafts16/18 is emitted by
     * moq_loc_encode, but ONLY
     * for RAW packaging (CMAF passes the app's property block through
     * untouched, so its timestamp fields are never encoded here). A value the
     * negotiated draft's codec cannot represent cannot be encoded; left to the
     * drain path it would fatal the sender AFTER the object was queued and its
     * payload ownership taken. Reject it synchronously -- before any enqueue /
     * ownership transfer. The encoded value is capture_time_us when
     * has_capture_time, else the presentation_time_us fallback
     * (sender_build_props), so validate whichever will be encoded, against
     * sender_loc_ts_max (the encoder's own bound for the latched draft). */
    /* An unresolved AUTO companion may still become legacy, so its RAW
     * timestamps are held to the same bound. */
    if (track->packaging == MOQ_MEDIA_PACKAGING_RAW &&
        s->companion_profile != MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC) {
        uint64_t loc_ts = obj->has_capture_time ? obj->capture_time_us
                                                : obj->presentation_time_us;
        if (loc_ts > sender_loc_ts_max(s->transport_version)) {
            s->stats.last_error = MOQ_ERR_INVAL;
            pthread_mutex_unlock(&s->mu);
            return MOQ_ERR_INVAL;
        }
    }

    /* CMAF validation (CMSF §3.3/§3.4), STRICT BY DEFAULT (cfg_init sets
     * validate_cmaf = true; set it false for explicit passthrough). When
     * the track has a parsed CMAF init, the object track_ID is matched against
     * it. Without an app-declared SAP type, a group-start object MUST start with
     * a SYNC sample (§3.4): a non-sync first sample is rejected even when its
     * sap_type is UNKNOWN (an open-GOP independent sample could only be SAP
     * type 3, which is not allowed at a group start). An explicit declaration of
     * TYPE_1/2 overrides this. No ownership is taken on rejection. */
    if (s->validate_cmaf && track->packaging == MOQ_MEDIA_PACKAGING_CMAF) {
        moq_bytes_t obj_bytes = {
            moq_rcbuf_data(obj->payload), moq_rcbuf_len(obj->payload) };
        moq_cmaf_init_info_t vinit;
        moq_cmaf_init_info_init(&vinit);
        const moq_cmaf_init_info_t *initp = NULL;
        if (track->has_cmaf_init_track_id) {
            vinit.track_id = track->cmaf_init_track_id;
            initp = &vinit;
        }
        moq_cmaf_object_report_t rep;
        bool ok = (moq_cmaf_validate_object(initp, obj_bytes, &rep) == MOQ_OK);
        if (ok && obj->starts_group) {
            if (obj->has_sap_type) {
                /* §3.4: with a declared SAP type, a group MUST start with type
                 * 1 or 2 (the app declares the exact type; we never infer it). */
                if (obj->sap_type != MOQ_SAP_TYPE_1 &&
                    obj->sap_type != MOQ_SAP_TYPE_2)
                    ok = false;
            } else if (!rep.starts_with_sync) {
                /* No declaration: §3.4 requires a group to start with a SAP of
                 * type 1 or 2. Only a SYNC sample qualifies without an explicit
                 * declaration (its exact type is UNKNOWN but necessarily 1/2).
                 * A known non-SAP (P/B frame, sap_type NONE) AND an open-GOP
                 * non-sync independent sample (sap_type UNKNOWN, which could
                 * only be SAP type 3) both have starts_with_sync=false and are
                 * rejected -- the app must declare TYPE_1/2 to start a group
                 * with either. */
                ok = false;
            }
        }
        if (!ok) {
            s->stats.last_error = MOQ_ERR_PROTO;
            pthread_mutex_unlock(&s->mu);
            return MOQ_ERR_PROTO;
        }
    }

    /* MPEG2TS: write() is the app-Object mode; a byte-stream track takes
     * write_mpeg2ts() only. Each Object is whole source packets with their
     * sync bytes (Object Payload Format), and with mpeg2tsRandomAccess
     * declared, a Group's first Object holds a random access point (Random
     * Access). No ownership is taken on rejection. */
    if (track->packaging == MOQ_MEDIA_PACKAGING_MPEG2TS) {
        if (!track->ts_app_objects) {
            pthread_mutex_unlock(&s->mu);
            return MOQ_ERR_WRONG_STATE;
        }
        bool ok = true;
#ifdef MOQ_SERVICE_HAVE_MPEG2TS
        const moq_msf_mpeg2ts_t *rec = sender_ts_record(track);
        moq_bytes_t pl = { moq_rcbuf_data(obj->payload),
                           moq_rcbuf_len(obj->payload) };
        moq_mpeg2ts_object_rules_t rules;
        moq_mpeg2ts_object_rules_init(&rules);
        rules.packet_size = rec->packet_size;
        moq_mpeg2ts_object_report_t rep;
        moq_mpeg2ts_object_report_init(&rep);
        ok = moq_mpeg2ts_validate_object(&rules, pl, &rep, sizeof(rep)) == MOQ_OK;
        if (ok && track->ts_random_access &&
            (obj->starts_group || !track->group_open)) {
            moq_mpeg2ts_inspect_cfg_t ip;
            moq_mpeg2ts_inspect_cfg_init(&ip);
            ip.packet_size = rec->packet_size;
            ip.random_access_pid = MOQ_MPEG2TS_PID_AUTO;
            if (rec->has_program_number)
                ip.program_number = (uint16_t)rec->program_number;
            moq_mpeg2ts_object_info_t info;
            moq_mpeg2ts_object_info_init(&info);
            ok = moq_mpeg2ts_inspect_object(&ip, pl, &info, sizeof(info)) == MOQ_OK &&
                 info.has_random_access;
        }
#endif
        if (!ok) {
            s->stats.last_error = MOQ_ERR_PROTO;
            pthread_mutex_unlock(&s->mu);
            return MOQ_ERR_PROTO;
        }
    }

    /* LOC ownership (§7.4): for RAW/LOC media the service GENERATES the
     * whole property block from the typed fields. App-supplied `properties`
     * cannot be carried on a RAW object in v0: the property block is a
     * delta-coded KVP sequence, so the generated LOC block and an
     * independently-encoded app block cannot simply be concatenated (the
     * app keys' deltas would decode relative to the LOC block's last key).
     * A correct ordered KVP merge is a follow-up; until then RAW extras are
     * rejected (the service owns RAW/LOC properties anyway). CMAF carries
     * the app's complete block through unchanged -- no generation, no
     * merge. */
    if (track->packaging == MOQ_MEDIA_PACKAGING_RAW && obj->properties) {
        s->stats.last_error = MOQ_ERR_INVAL;
        pthread_mutex_unlock(&s->mu);
        return MOQ_ERR_INVAL;   /* no ownership taken */
    }

    /* Would this object open a new group? (Pure.) */
    bool opened = obj->starts_group || !track->group_open;

    /* CMSF §3.6.1 / §3.4: when this track generates a SAP timeline, every
     * group-start Object MUST declare a concrete SAP type of 1 or 2 (the value
     * recorded at [group,0]). A missing declaration or NONE/UNKNOWN/TYPE_3 at a
     * group start is rejected with no ownership taken. (UNKNOWN was already
     * rejected at the top of write().) */
    if (track->sap_timeline && opened &&
        (!obj->has_sap_type ||
         (obj->sap_type != MOQ_SAP_TYPE_1 && obj->sap_type != MOQ_SAP_TYPE_2))) {
        s->stats.last_error = MOQ_ERR_INVAL;
        pthread_mutex_unlock(&s->mu);
        return MOQ_ERR_INVAL;
    }

    sender_admit_t a;
    memset(&a, 0, sizeof(a));
    a.payload = obj->payload;
    a.properties = obj->properties;
    a.is_sync = obj->is_sync;
    a.starts_group = obj->starts_group;
    a.ends_group = obj->ends_group;
    a.pts_us = obj->presentation_time_us;
    a.has_capture_time = obj->has_capture_time;
    a.capture_time_us = obj->capture_time_us;
    /* Explicit D22 RAW requires the metadata snapshot. An AUTO sender that
     * is (or may still become) draft-22 encodes one when the object carries
     * it, and otherwise generates LOC-04 from the typed fields at drain; a
     * sender resolved to legacy ignores it, as explicit legacy does. */
    if (track->packaging == MOQ_MEDIA_PACKAGING_RAW) {
        bool has_metadata =
            obj->struct_size >= offsetof(moq_media_send_object_t, metadata) +
                                sizeof(obj->metadata) && obj->metadata;
        /* A typed capture time is sent as an unscaled LOC-04 Timestamp, Unix
         * microseconds (LOC-04 2.3.1.1). A Timescale among the Track
         * Properties applies to every object without its own (2.3.1.2), so
         * the receiver would read that capture time as media time at the
         * track's scale. Refused, before anything is evicted or owned,
         * whenever the object may go out as draft 22; a track whose
         * properties do not parse as LOC-04 metadata may carry a Timescale
         * and is refused the same way. Explicit metadata and the
         * presentation-time form are unaffected. */
        if (!has_metadata && obj->has_capture_time &&
            track->track_scale != SENDER_TRACK_SCALE_NONE &&
            sender_auto_d22_possible_locked(s)) {
            s->stats.last_error = MOQ_ERR_INVAL;
            pthread_mutex_unlock(&s->mu);
            return MOQ_ERR_INVAL;
        }
        if (s->companion_auto
                ? s->companion_profile !=
                      MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01 && has_metadata
                : s->companion_profile ==
                      MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC)
            a.metadata_src = obj;
    }
    if (track->sap_timeline && obj->has_sap_type) {
        a.has_sap_type = true;
        a.sap_type = (uint8_t)obj->sap_type;
    }
    moq_result_t rc = sender_admit_locked(s, track, &a);
    bool wake = rc == MOQ_OK && s->ready;  /* post-ready: kick the drain */
    pthread_mutex_unlock(&s->mu);

    /* Wake the network thread so the drain runs without waiting for unrelated
     * traffic. Non-allocating moq_endpoint_wake() (queues no task, cannot fail);
     * called OUTSIDE s->mu. Pre-ready writes need no wake -- the readiness
     * transition itself drives the first drain. */
    if (wake)
        moq_endpoint_wake(s->ep);
    return rc;
}


/* The wait level (s->mu held): ready AND the active queue has headroom for
 * at least one more object under both bounds. Advisory -- see the header. */
static bool sender_wait_level(const moq_media_sender_t *s)
{
    uint32_t depth = s->preq_tail - s->preq_head;
    return s->ready && depth < active_obj_cap(s) &&
           s->preq_bytes < active_byte_cap(s);
}


/* Level-triggered wait, the sender twin of moq_media_receiver_wait: check
 * the level, block in the ENDPOINT wait (queue drain and the readiness flip
 * both happen on pump cycles, which mark the endpoint's coalesced activity),
 * re-check. PRIORITY before every MOQ_OK, pre- and post-wait: interrupt
 * latch, then terminal (sender fatal / endpoint closed), then the level --
 * write() refuses MOQ_ERR_INTERRUPTED under the latch and MOQ_ERR_CLOSED
 * once terminal, so reporting "write now" in either state would be a lie
 * (unlike the receiver, whose polls stay legal while latched and keep
 * draining queued items after terminal). Lock order: s->mu is never held
 * across the endpoint calls (hook order is ep->mu -> s->mu; see the
 * BLOCK_TIMEOUT comment above); the latch reads use the ep->mu-free
 * internal probe. */
moq_result_t moq_media_sender_wait(moq_media_sender_t *s, uint64_t timeout_us)
{
    if (!s) return MOQ_ERR_INVAL;

    if (moq_endpoint_interrupted_internal(s->ep)) return MOQ_ERR_INTERRUPTED;

    pthread_mutex_lock(&s->mu);
    bool level = sender_wait_level(s);
    bool fatal = s->fatal;
    bool mismatch = s->transport_version && !sender_version_usable(s);
    pthread_mutex_unlock(&s->mu);
    if (fatal || moq_endpoint_is_closed(s->ep)) return MOQ_ERR_CLOSED;
    if (mismatch) return MOQ_ERR_UNSUPPORTED;
    if (level) return MOQ_OK;

    moq_result_t rc = moq_endpoint_wait(s->ep, timeout_us);
    if (rc == MOQ_ERR_INTERRUPTED || rc == MOQ_DONE) return rc;
    if (moq_endpoint_interrupted_internal(s->ep)) return MOQ_ERR_INTERRUPTED;

    pthread_mutex_lock(&s->mu);
    level = sender_wait_level(s);
    fatal = s->fatal;
    mismatch = s->transport_version && !sender_version_usable(s);
    pthread_mutex_unlock(&s->mu);
    if (fatal || moq_endpoint_is_closed(s->ep)) return MOQ_ERR_CLOSED;
    if (mismatch) return MOQ_ERR_UNSUPPORTED;
    if (level) return MOQ_OK;
    return rc < 0 ? rc : MOQ_OK;
}
