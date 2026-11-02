#include "media_sender_internal.h"
#include "media_sender_complete_internal.h"
#include "media_sender_catalog_internal.h"
#include "media_sender_mpeg2ts_internal.h"
#include "media_sender_pump_internal.h"

/* An irrecoverable failure while a completion is unfinished -- complete()
 * called, its terminal catalog not yet committed -- stops that completion:
 * the first one is kept, exactly, in stats.completion_error. A refusal for
 * room is not a failure; after the terminal commit nothing is the
 * completion's. s->mu held. */
void sender_note_completion_failure(moq_media_sender_t *s, moq_result_t rc)
{
    if (!s->completing || s->complete_terminal_committed) return;
    if (rc >= 0 || rc == MOQ_ERR_WOULD_BLOCK) return;
    s->stats.last_error = rc;
    if (s->stats.completion_error == MOQ_OK) s->stats.completion_error = rc;
}


/* True for a service-owned generated timeline track (SAP eventtimeline or MSF
 * media timeline): the app never writes to or removes it directly. */
bool track_is_generated_timeline(const moq_media_track_t *t)
{
    return t->is_event_timeline || t->is_media_timeline;
}


/* Registration and the push handshake must precede terminal admission. The
 * callback latch survives local unpublish: an end may still owe pull slots
 * after the facade's current published state has cleared. s->mu held. */
bool sender_terminal_admitted(const moq_media_sender_t *s,
                              const moq_media_track_t *t)
{
    return t->pub_track &&
           (!s->publish_tracks || (t->publish_submitted && t->publish_resolved));
}


/* Emit queued objects in decode order until the queue drains or the
 * transport backpressures. s->mu held; the session is network-thread
 * confined, so facade calls are safe under this lock (nothing the facade
 * touches re-enters s->mu). */
/* Whether every record a generated timeline holds has gone out, a write
 * refused for room included. */
bool sender_timeline_caught_up(const moq_media_track_t *tl)
{
    if (tl->tl_pend) return false;
    return tl->is_event_timeline
               ? tl->sap_lead == tl->sap_base_seq + tl->sap_rec_count
               : tl->mt_lead == tl->mt_base_seq + tl->mt_rec_count;
}


/* Whether the media track that feeds generated timeline `tl` has had the
 * end a completion waits on sent, so it can produce no further record. */
bool sender_timeline_media_ended(const moq_media_sender_t *s,
                                 const moq_media_track_t *tl)
{
    for (size_t i = 0; i < s->track_count; i++) {
        const moq_media_track_t *t = s->tracks[i];
        if (t->sap_timeline != tl && t->media_timeline != tl) continue;
        if (t->removed || t->auth_failed) return true;
        return !t->complete_end_owed &&
               !(t->end_requested && !t->end_sent);
    }
    return true;
}


/* Graceful completion (moq_media_sender_complete, MSF §11.3), after the
 * drain pass: each track complete() owes an END_OF_TRACK gets it once
 * nothing of the track is left queued -- a byte-stream TS track's source
 * finished first (sender_ts_complete_step), its accepted media out, or,
 * without demand, retained with the end owed behind it for a joiner --
 * through the same facade end as an end_track marker. Each generated
 * timeline ends once its media track's end is sent and every record that
 * media produced has gone out (sender_emit_*_for, gated on the timeline's own
 * demand), independently of the other tracks; one without demand
 * keeps its records for a joiner and holds the completion with them, as
 * retained media does. A refusal for room keeps the obligation for a later
 * pump. Once every end is sent, the completion is finalized: the tracks are
 * marked removed (the hook then tears each down, its publisher track already
 * ended) and the terminal catalog is made owed. A lost Object complete() owed
 * (complete_failed) holds it unfinalized. Under s->mu. */
void sender_drain_completion(moq_media_sender_t *s, uint64_t now_us)
{
    if (!s->completing || s->complete_finalized || s->complete_failed) return;
    bool all_sent = true;
    for (size_t i = 0; i < s->track_count; i++) {
        moq_media_track_t *t = s->tracks[i];
        if (t->is_catalog || t->removed || t->auth_failed ||
            track_is_generated_timeline(t)) continue;
        if (!t->complete_end_owed) {
            if (t->end_requested && !t->end_sent) all_sent = false;
            continue;
        }
#ifdef MOQ_SERVICE_HAVE_MPEG2TS
        if (t->ts && !sender_ts_complete_step(s, t)) {
            all_sent = false;
            if (s->complete_failed) return;
            continue;
        }
#endif
        if (t->tstats.objects_queued != 0 || t->pending_reset) {
            all_sent = false;
            continue;
        }
        if (!sender_terminal_admitted(s, t)) {
            all_sent = false;
            continue;
        }
        {
            moq_result_t erc = moq_pub_end_track(s->pub, t->pub_track, now_us);
            if (erc == MOQ_ERR_WOULD_BLOCK || erc == MOQ_ERR_WRONG_STATE) {
                all_sent = false;
                continue;   /* shared capacity / mid-object: a later pump */
            }
            if (erc != MOQ_OK) {
                sender_note_completion_failure(s, erc);
                sender_set_fatal_locked(s, MOQ_MEDIA_SENDER_FATAL_SETUP_FAILED);
                return;
            }
            t->emit_open = false;
        }
        t->complete_end_owed = false;
        t->end_sent = true;
    }
    for (size_t i = 0; i < s->track_count; i++) {
        moq_media_track_t *tl = s->tracks[i];
        if (!track_is_generated_timeline(tl) || tl->removed || tl->auth_failed ||
            tl->end_sent) continue;
        /* Its media first: that media's records feed the timeline. */
        if (!sender_timeline_media_ended(s, tl) || !sender_timeline_caught_up(tl)) {
            all_sent = false;   /* emitted with demand, else retained */
            continue;
        }
        if (!sender_terminal_admitted(s, tl)) {
            all_sent = false;
            continue;
        }
        {
            moq_result_t erc = moq_pub_end_track(s->pub, tl->pub_track, now_us);
            if (erc == MOQ_ERR_WOULD_BLOCK || erc == MOQ_ERR_WRONG_STATE) {
                all_sent = false;
                continue;
            }
            if (erc != MOQ_OK) {
                sender_note_completion_failure(s, erc);
                sender_set_fatal_locked(s, MOQ_MEDIA_SENDER_FATAL_SETUP_FAILED);
                return;
            }
            tl->emit_open = false;
        }
        tl->end_sent = true;
    }
    if (!all_sent) return;
    s->complete_finalized = true;
    for (size_t i = 0; i < s->track_count; i++)
        if (!s->tracks[i]->is_catalog)
            s->tracks[i]->removed = true;   /* media + generated timeline tracks */
    s->catalog_dirty = true;
}


/* MSF §11.3 step 1: send Track Ended (status 0x2) to the active subscribers of
 * every converting track BEFORE its now-VOD catalog may publish. The publisher
 * track is left registered and retained -- NOT ended -- so the converted content
 * stays subscribable/joinable for VOD (the retained-replay guarantee is the
 * publisher's, from moq_pub_finish_subscribers). Returns true when no conversion
 * finish remains pending (the catalog republish may proceed); false if a finish
 * still needs a retry (the conversion catalog stays gated and catalog_dirty is
 * preserved for the next pump). A converting track that never registered a
 * publisher track had no live subscribers, so its finish is trivially complete.
 * Runs on the network thread, after the drain and before the republish. */
bool sender_finish_conversions(moq_media_sender_t *s, uint64_t now_us)
{
    size_t n = 0;
    moq_media_track_t **snap = NULL;
    /* NOMEM: preserve the conversion gate (NOT all_done) so the conversion
     * catalog cannot publish before Track Ended; retry next pump. */
    if (!sender_tracks_snapshot(s, &snap, &n)) return false;
    bool all_done = true;
    for (size_t i = 0; i < n; i++) {
        moq_media_track_t *t = snap[i];
        pthread_mutex_lock(&s->mu);
        bool pending = t->vod_finish_pending;
        pthread_mutex_unlock(&s->mu);
        if (!pending) continue;
        if (!t->pub_track) {
            /* Never advertised -> no live subscribers to finish. */
            pthread_mutex_lock(&s->mu);
            t->vod_finish_pending = false;
            pthread_mutex_unlock(&s->mu);
            continue;
        }
        moq_result_t fr = moq_pub_finish_subscribers(
            s->pub, t->pub_track, MOQ_PUB_DONE_TRACK_ENDED, now_us);
        if (fr == MOQ_OK) {
            pthread_mutex_lock(&s->mu);
            t->vod_finish_pending = false;
            pthread_mutex_unlock(&s->mu);
        } else if (fr == MOQ_ERR_WOULD_BLOCK || fr == MOQ_ERR_WRONG_STATE) {
            all_done = false;   /* retry next pump; keep the conversion gated */
        } else {
            sender_set_fatal(s, MOQ_MEDIA_SENDER_FATAL_SETUP_FAILED);
            all_done = false;
            break;
        }
    }
    sender_tracks_release(s, snap, n);
    return all_done;
}


moq_result_t moq_media_sender_complete(moq_media_sender_t *s)
{
    if (!s) return MOQ_ERR_INVAL;
    if (s->ep && moq_endpoint_interrupted_internal(s->ep))
        return MOQ_ERR_INTERRUPTED;
    if (sender_terminal(s)) return MOQ_ERR_CLOSED;

    pthread_mutex_lock(&s->mu);
    if (s->fatal) { pthread_mutex_unlock(&s->mu); return MOQ_ERR_CLOSED; }
    /* Idempotent: a second complete() is a no-op success (§11.3 is terminal). */
    if (s->completing) { pthread_mutex_unlock(&s->mu); return MOQ_OK; }
    /* Completion is meaningful only once the catalog is established. */
    if (!s->ready) { pthread_mutex_unlock(&s->mu); return MOQ_ERR_WRONG_STATE; }
    if (!sender_catalog_group_available(s)) {
        pthread_mutex_unlock(&s->mu);
        return MOQ_ERR_LIMIT;   /* no Group ID left for the terminal catalog */
    }

    /* MSF §11.3 permanent termination, in its order: no further writes; every
     * track's accepted media goes out and the track then ends with
     * END_OF_TRACK (complete_end_owed, sent by the drain behind its queued
     * media; a track already ended by end_track keeps its own marker); once
     * every end is sent the tracks are removed and the terminal catalog --
     * isComplete:true with an empty tracks array -- is published
     * (sender_drain_completion). Once completing, add/write/remove/end are
     * refused. The endpoint/namespace stay up so the retained terminal
     * catalog remains retrievable until destroy. */
    s->completing = true;
    for (size_t i = 0; i < s->track_count; i++) {
        moq_media_track_t *t = s->tracks[i];
        if (t->is_catalog || t->removed || t->auth_failed ||
            track_is_generated_timeline(t)) continue;
        if (t->end_requested) continue;   /* its end_track marker is queued */
        t->end_requested = true;          /* refuses further writes */
        /* A byte-stream TS track's open Group continues with the Objects
         * its source still holds; the completion step closes it. */
        if (!t->ts) t->group_open = false;
        t->complete_end_owed = true;
    }
    pthread_mutex_unlock(&s->mu);

    moq_endpoint_wake(s->ep);
    return MOQ_OK;
}


moq_result_t moq_media_sender_convert_to_vod(moq_media_sender_t *s,
                                             const moq_media_vod_track_t *items,
                                             size_t count)
{
    if (!s || !items || count == 0) return MOQ_ERR_INVAL;
    if (s->ep && moq_endpoint_interrupted_internal(s->ep))
        return MOQ_ERR_INTERRUPTED;
    if (sender_terminal(s)) return MOQ_ERR_CLOSED;

    pthread_mutex_lock(&s->mu);
    if (s->fatal) { pthread_mutex_unlock(&s->mu); return MOQ_ERR_CLOSED; }
    if (s->completing || !s->ready) {
        pthread_mutex_unlock(&s->mu);
        return MOQ_ERR_WRONG_STATE;     /* post-ready, pre-completion only */
    }
    /* Validate the WHOLE batch first so the conversion is atomic -- a single bad
     * item converts nothing. Reject duplicates within the batch too. */
    for (size_t i = 0; i < count; i++) {
        moq_media_track_t *t = items[i].track;
        if (!t) { pthread_mutex_unlock(&s->mu); return MOQ_ERR_INVAL; }
        bool owned = false;
        for (size_t j = 0; j < s->track_count; j++)
            if (s->tracks[j] == t) { owned = true; break; }
        if (!owned || t->is_catalog || track_is_generated_timeline(t) ||
            t->removed || t->auth_failed || t->end_requested ||
            !t->is_live || t->has_track_duration) {
            pthread_mutex_unlock(&s->mu);
            return MOQ_ERR_INVAL;       /* foreign/internal/removed/ended/not-
                                         * live/already-converted */
        }
        for (size_t j = 0; j < i; j++)
            if (items[j].track == t) {
                pthread_mutex_unlock(&s->mu);
                return MOQ_ERR_INVAL;   /* duplicate in the batch */
            }
    }
    if (!sender_catalog_group_available(s)) {
        pthread_mutex_unlock(&s->mu);
        return MOQ_ERR_LIMIT;   /* no Group ID left for the conversion catalog */
    }
    /* Apply: flip each to VOD (isLive false + trackDuration), arm the §11.3
     * step-1 subscriber finish, and dirty the catalog. The metadata change is an
     * in-place mutation of a surviving tuple, which a deltaUpdate's add/remove
     * ops cannot express; catalog_meta_dirty forces the next generation carrying
     * it to a full INDEPENDENT catalog even when coalesced with an add/remove
     * (which the receiver accepts as a live->VOD conversion). The network pump
     * sends Track Ended to each track's active subscribers before that catalog
     * publishes (vod_finish_pending gates it). */
    for (size_t i = 0; i < count; i++) {
        moq_media_track_t *t = items[i].track;
        t->is_live = false;
        t->has_track_duration = true;
        t->track_duration_ms = items[i].duration_ms;
        t->vod_converted = true;
        t->vod_finish_pending = true;
        t->catalog_meta_dirty = true;
    }
    s->catalog_dirty = true;
    pthread_mutex_unlock(&s->mu);

    moq_endpoint_wake(s->ep);
    return MOQ_OK;
}
