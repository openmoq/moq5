#include "media_sender_internal.h"
#include "media_sender_catalog_internal.h"
#include "media_sender_complete_internal.h"
#include "media_sender_mpeg2ts_internal.h"
#include "media_sender_pump_internal.h"
#include "media_sender_tracks_internal.h"
#include "media_sender_write_internal.h"

#ifdef MOQ_MEDIA_SENDER_TESTING


/* -- White-box test seam (not built into the shipping library) ---------- *
 * Tests the catalog-content model deterministically with no endpoint/network:
 * add/remove tracks, then build the catalog the sender would publish. The
 * pump-driven wire republish (group numbering, retained group, staged
 * backpressure) is covered by the loopback tests. */

bool moq_media_sender_test_auth_unlocked(moq_media_sender_t *s);

bool moq_media_sender_test_auth_unlocked(moq_media_sender_t *s)
{
    if (pthread_mutex_trylock(&s->mu) != 0) return false;
    pthread_mutex_unlock(&s->mu);
    return true;
}


/* Passive native-call observation. NULL identifies the private catalog. */
bool moq_media_sender_test_publish_admission(moq_media_sender_t *s,
    moq_media_track_t *track, uint64_t *attempts, moq_result_t *result);

bool moq_media_sender_test_publish_admission(moq_media_sender_t *s,
    moq_media_track_t *track, uint64_t *attempts, moq_result_t *result)
{
    if (!s) return false;
    pthread_mutex_lock(&s->mu);
    moq_media_track_t *t = track ? NULL : s->catalog_track;
    for (size_t i = 0; track && i < s->track_count; ++i)
        if (s->tracks[i] == track) { t = track; break; }
    if (t) {
        if (attempts) *attempts = t->test_publish_attempts;
        if (result) *result = t->test_publish_result;
    }
    pthread_mutex_unlock(&s->mu);
    return t != NULL;
}


bool moq_media_sender_test_auth_failed(moq_media_sender_t *s, moq_media_track_t *t);

bool moq_media_sender_test_auth_failed(moq_media_sender_t *s, moq_media_track_t *t)
{
    pthread_mutex_lock(&s->mu);
    bool failed = t ? t->auth_failed : s->fatal;
    pthread_mutex_unlock(&s->mu);
    return failed;
}


bool moq_media_sender_test_auth_named_failed(moq_media_sender_t *s, moq_bytes_t name);

bool moq_media_sender_test_auth_named_failed(moq_media_sender_t *s, moq_bytes_t name)
{
    bool failed = false;
    pthread_mutex_lock(&s->mu);
    for (size_t i = 0; i < s->track_count; ++i) {
        moq_media_track_t *t = s->tracks[i];
        if (t->name.len == name.len && memcmp(t->name.data, name.data, name.len) == 0)
            failed = t->auth_failed;
    }
    pthread_mutex_unlock(&s->mu);
    return failed;
}


/* Called between deterministic hook steps, never concurrently with prepare. */
size_t moq_media_sender_test_auth_pending(moq_media_sender_t *s);

size_t moq_media_sender_test_auth_pending(moq_media_sender_t *s)
{
    size_t n = s->namespace_auth.prepared ? 1u : 0u;
    if (s->catalog_track && s->catalog_track->publish_auth.prepared) ++n;
    pthread_mutex_lock(&s->mu);
    for (size_t i = 0; i < s->track_count; ++i)
        if (s->tracks[i]->publish_auth.prepared) ++n;
    pthread_mutex_unlock(&s->mu);
    return n;
}


/* The actual full catalog baseline committed by the production hook, not a
 * fresh hypothetical encoding of the current track list. */
moq_rcbuf_t *moq_media_sender_test_auth_catalog(moq_media_sender_t *s);

moq_rcbuf_t *moq_media_sender_test_auth_catalog(moq_media_sender_t *s)
{
    return s->published_catalog ? moq_rcbuf_incref(s->published_catalog) : NULL;
}


/* The exact config gate moq_media_sender_create()/attach() run before any
 * networking, exposed so authoring validation (contentProtections syntax) is
 * testable without standing up an endpoint. */
moq_result_t moq_media_sender_test_validate_cfg(const moq_media_sender_cfg_t *cfg)
{
    return sender_validate_cfg(cfg);
}


/* Build a sender from a real cfg with NO endpoint (mirrors the receiver's
 * scripted seam): moq_media_sender_add_track and the real sender_hook can
 * then be driven against a caller-supplied SimPair session, so rejection
 * paths are exercised through the PRODUCTION hook -- not a handler stub. */
moq_media_sender_t *moq_media_sender_test_new_cfg(
    const moq_media_sender_cfg_t *cfg)
{
    sender_sizes_t sz;
    if (sender_prepare(cfg, &sz) < 0) return NULL;
    moq_media_sender_t *s = NULL;
    if (sender_new(NULL, false, cfg, &sz, &s) != MOQ_OK) return NULL;
    return s;
}


/* The same construction as moq_media_sender_test_new_cfg, surfacing the REAL
 * result (INVAL from validation vs NOMEM from the allocator) so a construction
 * oracle can distinguish a refusal from an allocation failure. No endpoint. */
moq_result_t moq_media_sender_test_construct(const moq_media_sender_cfg_t *cfg,
                                             moq_media_sender_t **out);

moq_result_t moq_media_sender_test_construct(const moq_media_sender_cfg_t *cfg,
                                             moq_media_sender_t **out)
{
    if (!out) return MOQ_ERR_INVAL;
    *out = NULL;
    sender_sizes_t sz;
    moq_result_t rc = sender_prepare(cfg, &sz);
    if (rc < 0) return rc;
    return sender_new(NULL, false, cfg, &sz, out);
}


/* Test-only: how many times the pending-reset transition has been ATTEMPTED.
 * Behind MOQ_MEDIA_SENDER_TESTING, so it is absent from the shipping library. */
uint32_t moq_media_sender_test_reset_attempts(const moq_media_sender_t *s)
{
    return s ? s->test_reset_attempts : 0u;
}


/* Drive the production sender_hook with a caller-controlled session (the hook
 * ignores its endpoint argument -- (void)ep). */
/* Refuse the next `n` removed-track teardown END attempts (retryable). */
void moq_media_sender_test_refuse_teardown_end(moq_media_sender_t *s, uint32_t n);

void moq_media_sender_test_refuse_teardown_end(moq_media_sender_t *s, uint32_t n)
{
    pthread_mutex_lock(&s->mu);
    s->test_refuse_teardown_end = n;
    pthread_mutex_unlock(&s->mu);
}


/* Refuse the next `n` pending-reset attempts (retryable). */
void moq_media_sender_test_refuse_reset(moq_media_sender_t *s, uint32_t n);

void moq_media_sender_test_refuse_reset(moq_media_sender_t *s, uint32_t n)
{
    pthread_mutex_lock(&s->mu);
    s->test_refuse_reset = n;
    pthread_mutex_unlock(&s->mu);
}


/* The owed-output answer the endpoint's drain reads after each pump. */
bool moq_media_sender_test_has_pending_output(moq_media_sender_t *s);

bool moq_media_sender_test_has_pending_output(moq_media_sender_t *s)
{
    return sender_has_pending_output(s);
}


void moq_media_sender_test_pump(moq_media_sender_t *s,
                                moq_session_t *session, uint64_t now_us)
{
    sender_hook(NULL, session, now_us, s);
}


moq_media_sender_t *moq_media_sender_test_new(void)
{
    const moq_alloc_t *alloc = moq_alloc_default();
    moq_media_sender_t *s = (moq_media_sender_t *)alloc->alloc(
        sizeof(*s), alloc->ctx);
    if (!s) return NULL;
    memset(s, 0, sizeof(*s));
    s->test_block_republish_obj = -1;
    s->alloc = *alloc;
    s->ep = NULL;
    s->backpressure = MOQ_MEDIA_SEND_BP_DROP_TO_KEYFRAME;
    /* Production-default queue bounds: a zeroed cap is not a reachable
     * production state (sender_new always applies the defaults), and the
     * wait-level predicate reads them. */
    s->queue_cap      = SENDER_DEFAULT_QUEUE_OBJECTS;
    s->queue_byte_cap = SENDER_DEFAULT_QUEUE_BYTES;
    s->preq_cap       = SENDER_DEFAULT_PRE_READY_OBJECTS;
    s->preq_byte_cap  = SENDER_DEFAULT_PRE_READY_BYTES;
    pthread_mutex_init(&s->mu, NULL);
    pthread_cond_init(&s->space_cv, NULL);
    return s;
}


void moq_media_sender_test_free(moq_media_sender_t *s)
{
    if (!s) return;
    /* The pumped hook creates the publisher facade (moq_pub_create), and
     * sender_free does not own it -- moq_media_sender_destroy does. Tear it
     * down here for the same reason the catalog track is released below, or a
     * seam test that pumped even once retains the whole publisher graph.
     *
     * Inline destruction is correct HERE: this endpoint-free seam is driven
     * and destroyed on its owning test thread, so the hook cannot be running
     * concurrently. Production posts sender_destroy_pub_task instead because
     * its facade lives on the endpoint's network thread. */
    if (s->pub) { moq_pub_destroy(s->pub); s->pub = NULL; }
    /* A test may have lazily created the private catalog track (via
     * moq_media_sender_test_catalog_track); sender_free does not own it (destroy
     * does), so release it here to keep the test leak-free. */
    if (s->catalog_track) { track_free(s, s->catalog_track); s->catalog_track = NULL; }
    sender_free(s);
}


/* Force or clear the sender-fatal flag, so a wait-contract test can prove
 * terminal state beats the ready+space level. Clearing is test-only (fatal is
 * one-way in production; sender_set_fatal_locked never clears). */
void moq_media_sender_test_set_fatal(moq_media_sender_t *s, bool fatal)
{
    pthread_mutex_lock(&s->mu);
    if (fatal) {
        sender_set_fatal_locked(s, 0x1);
    } else {
        s->fatal = false;
        s->fatal_code = 0;
    }
    pthread_mutex_unlock(&s->mu);
}


/* Point a test sender at a (bare) endpoint so the BLOCK_TIMEOUT closed-check can
 * be exercised; pair with set_ep(s, NULL) before free so sender_free does not
 * touch a test-owned endpoint. */
void moq_media_sender_test_set_ep(moq_media_sender_t *s, moq_endpoint_t *ep)
{
    s->ep = ep;
}


/* Run the exact closed-check the BLOCK_TIMEOUT loop uses. The lock-order
 * regression calls this from a worker while the test holds ep->mu: it must
 * return WITHOUT blocking on ep->mu (taking ep->mu here would deadlock against
 * a caller that holds it). */
bool moq_media_sender_test_endpoint_closed(moq_media_sender_t *s)
{
    return sender_ep_closed(s);
}


/* Release the oldest queued entry. PRECONDITION: s->mu is already held by the
 * caller (the scripted wait hook runs inside the write's critical section), so
 * this must not lock. Mirrors the drain's release + byte accounting. */
void moq_media_sender_test_release_one_locked(moq_media_sender_t *s);

void moq_media_sender_test_release_one_locked(moq_media_sender_t *s)
{
    if (!s || s->preq_head == s->preq_tail) return;
    sender_preq_entry_t *e = &s->preq[s->preq_head % s->ring_cap];
    s->preq_bytes -= e->bytes;
    preq_entry_release(s, e);
    s->preq_head++;
}


/* Registered track handles (app media + generated), read under s->mu. */
size_t moq_media_sender_test_track_count(moq_media_sender_t *s);

size_t moq_media_sender_test_track_count(moq_media_sender_t *s)
{
    pthread_mutex_lock(&s->mu);
    size_t n = s->track_count;
    pthread_mutex_unlock(&s->mu);
    return n;
}


/* Simulate the post-ready transition (so add/remove dirty the catalog). */
/* The i-th queued entry, oldest first: its track, payload (borrowed), Group
 * id, and whether it opens its Group. False past the end. */
bool moq_media_sender_test_queue_entry(moq_media_sender_t *s, size_t i,
                                       const moq_media_track_t **track,
                                       const moq_rcbuf_t **payload,
                                       uint64_t *group, bool *starts_group);

bool moq_media_sender_test_queue_entry(moq_media_sender_t *s, size_t i,
                                       const moq_media_track_t **track,
                                       const moq_rcbuf_t **payload,
                                       uint64_t *group, bool *starts_group)
{
    pthread_mutex_lock(&s->mu);
    bool ok = i < (size_t)(s->preq_tail - s->preq_head);
    if (ok) {
        const sender_preq_entry_t *e =
            &s->preq[(s->preq_head + (uint32_t)i) % s->ring_cap];
        if (track) *track = e->track;
        if (payload) *payload = e->payload;
        if (group) *group = e->group_seq;
        if (starts_group) *starts_group = e->starts_group;
    }
    pthread_mutex_unlock(&s->mu);
    return ok;
}


/* Take the queue head as the drain would after a successful write to the
 * session: the wire state (open Group, next Object id) and the sent counters
 * advance, and the payload reference passes to the caller. False when the
 * queue is empty or the head is the END_OF_TRACK marker. */
bool moq_media_sender_test_emit_head(moq_media_sender_t *s,
                                     moq_rcbuf_t **payload);

bool moq_media_sender_test_emit_head(moq_media_sender_t *s,
                                     moq_rcbuf_t **payload)
{
    pthread_mutex_lock(&s->mu);
    if (s->preq_head == s->preq_tail ||
        s->preq[s->preq_head % s->ring_cap].is_end) {
        pthread_mutex_unlock(&s->mu);
        return false;
    }
    sender_preq_entry_t *e = &s->preq[s->preq_head % s->ring_cap];
    moq_media_track_t *t = e->track;
    bool new_group = !t->emit_open || t->emit_group != e->group_seq;
    t->emit_obj = (new_group ? 0 : t->emit_obj) + 1;
    t->write_blocked = false;
    t->emit_group = e->group_seq;
    t->emit_open = true;
    s->stats.objects_sent++;
    t->tstats.objects_sent++;
    t->tstats.objects_queued--;
    t->tstats.bytes_queued -= e->bytes;
    s->preq_bytes -= e->bytes;
    *payload = e->payload;
    e->payload = NULL;
    preq_entry_release(s, e);
    s->preq_head++;
    pthread_cond_broadcast(&s->space_cv);
    pthread_mutex_unlock(&s->mu);
    return true;
}


/* Discard every queued object of `t` through the drop path, as a policy
 * would. Caller holds s->mu (e.g. from the cond-wait hook). */
void moq_media_sender_test_drop_track_locked(moq_media_sender_t *s,
                                             moq_media_track_t *t);

void moq_media_sender_test_drop_track_locked(moq_media_sender_t *s,
                                             moq_media_track_t *t)
{
    for (;;) {
        bool any = false;
        uint64_t g = 0;
        for (uint32_t i = s->preq_head; i != s->preq_tail; i++) {
            const sender_preq_entry_t *e = &s->preq[i % s->ring_cap];
            if (e->track == t && !e->is_end) { any = true; g = e->group_seq; break; }
        }
        if (!any) return;
        preq_evict_group(s, t, g);
    }
}


void moq_media_sender_test_set_ready(moq_media_sender_t *s)
{
    pthread_mutex_lock(&s->mu);
    s->ready = true;
    pthread_mutex_unlock(&s->mu);
}


/* Supply only the authoritative count read by the production resync. The
 * mirror update, callback filtering and terminal guard remain production code.
 * Like the other fire seams, called on the test's single executor. */
void moq_media_sender_test_resync_track_demand(moq_media_sender_t *s,
                                              moq_media_track_t *t, size_t count);

void moq_media_sender_test_resync_track_demand(moq_media_sender_t *s,
                                              moq_media_track_t *t, size_t count)
{
    t->test_demand_set = true;
    t->test_demand = count;
    sender_resync_track_demand(s, t);
    t->test_demand_set = false;
}


/* Read the real generated sibling; creation still uses public add_track. */
moq_media_track_t *moq_media_sender_test_timeline_track(
    const moq_media_track_t *track, bool media_timeline);

moq_media_track_t *moq_media_sender_test_timeline_track(
    const moq_media_track_t *track, bool media_timeline)
{
    return media_timeline ? track->media_timeline : track->sap_timeline;
}


/* Install lifecycle callbacks on a test sender (no endpoint), so the fire
 * seams below can be observed. Mirrors the negotiated copy sender_new does. */
void moq_media_sender_test_set_callbacks(moq_media_sender_t *s,
                                         const moq_media_sender_callbacks_t *cb)
{
    pthread_mutex_lock(&s->mu);
    s->callbacks.ctx = cb->ctx;
    s->callbacks.on_ready = cb->on_ready;
    s->callbacks.on_closed = cb->on_closed;
    s->callbacks.on_track_closed = cb->on_track_closed;
    s->callbacks.struct_size = sizeof(s->callbacks);
    pthread_mutex_unlock(&s->mu);
}


/* Drive the core on_publish_finished handler for a track, mapping by pub_track
 * exactly as the network thread does. The test assigns the track a sentinel
 * pub_track (its own address) so the production mapping loop resolves it; then
 * the real handler runs (app-media filter + per-track latch + catalog->on_closed
 * routing). Pass the catalog track to exercise the catalog-termination path. */
void moq_media_sender_test_fire_publish_finished(moq_media_sender_t *s,
                                                 moq_media_track_t *track)
{
    track->pub_track = (moq_pub_track_t *)track;   /* sentinel for the map */
    sender_pub_on_publish_finished(s, track->pub_track);
}


/* Expose the catalog track handle so the test can drive its termination. */
moq_media_track_t *moq_media_sender_test_catalog_track(moq_media_sender_t *s)
{
    if (!s->catalog_track) {
        s->catalog_track = (moq_media_track_t *)s->alloc.alloc(
            sizeof(moq_media_track_t), s->alloc.ctx);
        if (s->catalog_track) {
            memset(s->catalog_track, 0, sizeof(*s->catalog_track));
            s->catalog_track->is_catalog = true;
        }
    }
    return s->catalog_track;
}


/* Drive the PRODUCTION fire helpers (same code the network thread runs), so the
 * test exercises the real single-fire latch + callback dispatch, not a copy. */
void moq_media_sender_test_fire_ready(moq_media_sender_t *s)
{
    sender_fire_ready(s);
}


void moq_media_sender_test_fire_closed(moq_media_sender_t *s, bool is_fatal,
                                       uint64_t fatal_code)
{
    sender_fire_closed(s, is_fatal, fatal_code);
}


/* Drive the fatal path end-to-end: sender_set_fatal marks fatal AND fires
 * on_closed(is_fatal=true) inline, exactly as a network-thread fatal does. */
void moq_media_sender_test_fire_fatal(moq_media_sender_t *s, uint64_t code)
{
    sender_set_fatal(s, code);
}


/* Simulate a finalized complete() without an endpoint: completing, every
 * track's end sent (no publisher track exists to end) and the tracks marked
 * removed, so the next build emits isComplete:true + empty tracks. */
/* The part of the completion drain that finishes byte-stream TS sources,
 * without a publisher: true once every TS track complete() owes an end has
 * its source finished. */
bool moq_media_sender_test_complete_ts_step(moq_media_sender_t *s)
{
    bool done = true;
    pthread_mutex_lock(&s->mu);
#ifdef MOQ_SERVICE_HAVE_MPEG2TS
    for (size_t i = 0; i < s->track_count; i++) {
        moq_media_track_t *t = s->tracks[i];
        if (t->complete_end_owed && t->ts && !sender_ts_complete_step(s, t)) done = false;
    }
#endif
    pthread_mutex_unlock(&s->mu);
    return done;
}


bool moq_media_sender_test_complete_failed(moq_media_sender_t *s)
{
    pthread_mutex_lock(&s->mu);
    bool f = s->complete_failed;
    pthread_mutex_unlock(&s->mu);
    return f;
}


/* Refuse the next catalog build's first allocation (one-shot). */
void moq_media_sender_test_refuse_catalog_build(moq_media_sender_t *s);

void moq_media_sender_test_refuse_catalog_build(moq_media_sender_t *s)
{
    pthread_mutex_lock(&s->mu);
    s->test_refuse_catalog_build = true;
    pthread_mutex_unlock(&s->mu);
}


/* Whether a generated timeline holds a write the facade refused for room. */
bool moq_media_sender_test_timeline_held(moq_media_sender_t *s);

bool moq_media_sender_test_timeline_held(moq_media_sender_t *s)
{
    pthread_mutex_lock(&s->mu);
    bool held = false;
    for (size_t i = 0; i < s->track_count && !held; i++)
        held = s->tracks[i]->tl_pend != NULL;
    pthread_mutex_unlock(&s->mu);
    return held;
}


void moq_media_sender_test_complete(moq_media_sender_t *s)
{
    pthread_mutex_lock(&s->mu);
    s->completing = true;
    s->complete_finalized = true;
    for (size_t i = 0; i < s->track_count; i++)
        s->tracks[i]->removed = true;
    s->catalog_dirty = true;
    pthread_mutex_unlock(&s->mu);
}


/* Build the catalog the sender would currently publish (skips removed tracks). */
moq_result_t moq_media_sender_test_build_catalog(moq_media_sender_t *s,
                                                 moq_rcbuf_t **out)
{
    pthread_mutex_lock(&s->mu);
    moq_result_t rc = sender_build_catalog_fresh(s, out, false, NULL);
    pthread_mutex_unlock(&s->mu);
    return rc;
}


bool moq_media_sender_test_catalog_dirty(const moq_media_sender_t *s)
{
    moq_media_sender_t *ms = (moq_media_sender_t *)(uintptr_t)s;
    pthread_mutex_lock(&ms->mu);
    bool d = ms->catalog_dirty;
    pthread_mutex_unlock(&ms->mu);
    return d;
}


/* The last committed catalog generation number (0 = the initial catalog). Each
 * add/remove/conversion or automatic refresh that commits advances it by one. */
uint64_t moq_media_sender_test_catalog_group(const moq_media_sender_t *s)
{
    moq_media_sender_t *ms = (moq_media_sender_t *)(uintptr_t)s;
    pthread_mutex_lock(&ms->mu);
    uint64_t g = ms->catalog_group;
    pthread_mutex_unlock(&ms->mu);
    return g;
}


/* Force the last-committed catalog generation number, so a test can drive the
 * group ceiling (UINT64_MAX) without publishing 2^64 generations. */
void moq_media_sender_test_set_catalog_group(moq_media_sender_t *s, uint64_t g)
{
    pthread_mutex_lock(&s->mu);
    s->catalog_group = g;
    pthread_mutex_unlock(&s->mu);
}


/* The cached managed-adapter wake deadline (what sender_next_deadline_us
 * returns): a pure scalar, UINT64_MAX when refresh is not timer-waiting. */
uint64_t moq_media_sender_test_next_deadline_us(moq_media_sender_t *s)
{
    return sender_next_deadline_us(s);
}


/* The RESOLVED automatic-refresh interval (UINT64_MAX when disabled, which
 * is the default for 0/absent as well as explicit UINT64_MAX). Lets a config
 * test prove the default / custom / disable resolution without standing up an
 * endpoint. */
uint64_t moq_media_sender_test_refresh_interval(const moq_media_sender_t *s)
{
    moq_media_sender_t *ms = (moq_media_sender_t *)(uintptr_t)s;
    pthread_mutex_lock(&ms->mu);
    uint64_t iv = ms->catalog_refresh_interval_us;
    pthread_mutex_unlock(&ms->mu);
    return iv;
}


/* Count of successful catalog retained-group installs (initial + every
 * committed generation). A refresh that advances the group installs exactly
 * one, so this distinguishes a real group advance from a no-op. */
unsigned moq_media_sender_test_retained_installs(const moq_media_sender_t *s)
{
    moq_media_sender_t *ms = (moq_media_sender_t *)(uintptr_t)s;
    pthread_mutex_lock(&ms->mu);
    unsigned n = ms->test_retained_installs;
    pthread_mutex_unlock(&ms->mu);
    return n;
}


/* Snapshot the current catalog as the "published" baseline (simulates a prior
 * committed generation). */
void moq_media_sender_test_mark_published(moq_media_sender_t *s)
{
    pthread_mutex_lock(&s->mu);
    moq_rcbuf_t *j = NULL;
    uint64_t stamp = 0;
    if (sender_build_catalog_fresh(s, &j, false, &stamp) == MOQ_OK && j) {
        if (s->published_catalog) moq_rcbuf_decref(s->published_catalog);
        s->published_catalog = j;
        s->published_generated_at = stamp;    /* the stamp THESE bytes carry */
        /* A committed generation also adopts the current tuple set as the
         * deltaUpdate baseline (in_published) -- mirror that so the next stage
         * diffs against it instead of treating every track as a fresh add. */
        sender_commit_baseline(s);
    }
    pthread_mutex_unlock(&s->mu);
}


/* Select the media companion of an endpoint-free test sender (one built by
 * moq_media_sender_test_new), as moq_media_sender_cfg_t.companion_profile
 * does at creation. Call before adding tracks: Group-ID state derived from
 * the profile is fixed at creation. Test-only; declared
 * by the tests that use it. */
void moq_media_sender_test_set_companion(moq_media_sender_t *s,
                                         moq_media_companion_profile_t profile);

void moq_media_sender_test_set_companion(moq_media_sender_t *s,
                                         moq_media_companion_profile_t profile)
{
    pthread_mutex_lock(&s->mu);
    s->companion_profile = profile;
    s->companion_auto = profile == MOQ_MEDIA_COMPANION_AUTO;
    (void)sender_resolve_group_base(
        profile != MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01, &s->group_base);
    pthread_mutex_unlock(&s->mu);
}


/* The companion currently in effect: MOQ_MEDIA_COMPANION_AUTO until an AUTO
 * sender's first draft-16/18/22 latch, then the resolved companion. */
moq_media_companion_profile_t moq_media_sender_test_companion(
    const moq_media_sender_t *s);

moq_media_companion_profile_t moq_media_sender_test_companion(
    const moq_media_sender_t *s)
{
    moq_media_sender_t *ms = (moq_media_sender_t *)(uintptr_t)s;
    pthread_mutex_lock(&ms->mu);
    moq_media_companion_profile_t c = ms->companion_profile;
    pthread_mutex_unlock(&ms->mu);
    return c;
}


/* Pin (or release) this sender's catalog clock. `set` false restores the real
 * wallclock. Instance-owned: another sender in the same process is unaffected.
 * Declared by the tests that use it -- there is no header for it, so it adds no
 * public API or ABI surface, and it exists only under MOQ_MEDIA_SENDER_TESTING. */
void moq_media_sender_test_set_clock_ms(moq_media_sender_t *s,
                                        bool set, uint64_t ms)
{
    pthread_mutex_lock(&s->mu);
    s->test_clock_set = set;
    s->test_clock_ms = ms;
    pthread_mutex_unlock(&s->mu);
}


/* Whether a republish now would actually cut a new generation: true iff the
 * resolved catalog differs from the published baseline. Mirrors the no-op
 * dedup decision in sender_republish_catalog. */
bool moq_media_sender_test_build_changed(moq_media_sender_t *s)
{
    pthread_mutex_lock(&s->mu);
    bool unchanged = false;
    /* The SAME helper production staging uses, with the only_registered choice
     * this query owns -- so the two cannot drift apart. This seam has no error
     * channel, so a comparison failure conservatively reports "changed"; the
     * production path treats the same failure as fatal instead. */
    moq_result_t rc = sender_catalog_semantically_unchanged(s, false,
                                                            &unchanged);
    bool changed = (rc != MOQ_OK) || !unchanged;
    pthread_mutex_unlock(&s->mu);
    return changed;
}


/* True iff `track` still has its §11.3 step-1 subscriber finish armed (the
 * conversion catalog is gated on this track). */
bool moq_media_sender_test_track_finish_pending(const moq_media_sender_t *s,
                                                const moq_media_track_t *track)
{
    moq_media_sender_t *ms = (moq_media_sender_t *)(uintptr_t)s;
    pthread_mutex_lock(&ms->mu);
    bool p = track && track->vod_finish_pending;
    pthread_mutex_unlock(&ms->mu);
    return p;
}


/* True iff ANY track still has a pending conversion finish -- i.e. the pump
 * would gate the conversion catalog republish right now. */
bool moq_media_sender_test_conversion_finish_pending(const moq_media_sender_t *s)
{
    moq_media_sender_t *ms = (moq_media_sender_t *)(uintptr_t)s;
    pthread_mutex_lock(&ms->mu);
    bool any = false;
    for (size_t i = 0; i < ms->track_count; i++)
        if (ms->tracks[i]->vod_finish_pending) { any = true; break; }
    pthread_mutex_unlock(&ms->mu);
    return any;
}


/* Run the network-pump's conversion-finish step once (no endpoint). Without
 * registered publisher tracks there are no live subscribers to finish, so each
 * pending flag clears and this returns true -- exercising the gate/flag
 * lifecycle and the catalog_dirty-preservation invariant deterministically. */
bool moq_media_sender_test_run_finish_conversions(moq_media_sender_t *s)
{
    return sender_finish_conversions(s, 0);
}


/* Stamp a sentinel publisher-track handle onto every advertised track so the
 * post-ready publish path (track_in_catalog with only_registered) treats them as
 * registered. The handle is never dereferenced on the white-box build/stage path
 * -- it only gates catalog membership -- so a non-NULL sentinel suffices. */
void moq_media_sender_test_mark_registered(moq_media_sender_t *s)
{
    pthread_mutex_lock(&s->mu);
    for (size_t i = 0; i < s->track_count; i++) {
        moq_media_track_t *t = s->tracks[i];
        if (!t->is_catalog && !t->removed && !t->pub_track)
            t->pub_track = (moq_pub_track_t *)(uintptr_t)0x1;
    }
    pthread_mutex_unlock(&s->mu);
}


/* Stage one catalog generation and hand the dense wire object list (object 0 =
 * independent base, objects 1..N = deltaUpdate objects) to the caller, which
 * reconstructs the receiver-effective catalog exactly as a peer would. Transfers
 * a ref on each object into objs[0..return) (caller decrefs) and leaves the
 * sender's pending generation cleared, so this inspects staging without
 * committing a wire send. Returns 0 if staging produced no generation (e.g. the
 * resolved catalog matched the published baseline). */
size_t moq_media_sender_test_stage(moq_media_sender_t *s,
                                   moq_rcbuf_t **objs, size_t cap)
{
    pthread_mutex_lock(&s->mu);
    if (!sender_stage_generation(s)) { pthread_mutex_unlock(&s->mu); return 0; }
    size_t n = s->pending_obj_count, out = 0;
    for (size_t i = 0; i < n; i++) {
        if (out < cap) objs[out++] = s->pending_objs[i];   /* move ref */
        else if (s->pending_objs[i]) moq_rcbuf_decref(s->pending_objs[i]);
        s->pending_objs[i] = NULL;
    }
    if (s->pending_current) { moq_rcbuf_decref(s->pending_current); s->pending_current = NULL; }
    s->pending_generated_at = 0;
    s->pending_obj_count = 0;
    s->pending_obj_cursor = 0;
    s->pending_retained_set = false;
    pthread_mutex_unlock(&s->mu);
    return out;
}

/* Run the PRODUCTION cfg-tail copier (callbacks + publish_tracks +
 * drop_without_demand) against a test sender, so the ABI prefix rules are
 * tested on the exact code create/attach runs -- not a test-only mirror. */
void moq_media_sender_test_apply_cfg_tail(moq_media_sender_t *s,
                                          const moq_media_sender_cfg_t *cfg)
{
    pthread_mutex_lock(&s->mu);
    memset(&s->callbacks, 0, sizeof(s->callbacks));
    s->publish_tracks = false;
    s->drop_without_demand = false;
    sender_copy_cfg_tail(s, cfg);
    pthread_mutex_unlock(&s->mu);
}


/* Which callbacks the copier installed (1 joined, 2 left, 4 ready, 8 closed,
 * 16 track_closed, 32 ctx) plus the copied flags (256 publish_tracks,
 * 512 drop_without_demand). */
unsigned moq_media_sender_test_cfg_tail_mask(const moq_media_sender_t *s)
{
    unsigned m = 0;
    if (s->callbacks.on_subscriber_joined) m |= 1u;
    if (s->callbacks.on_subscriber_left)   m |= 2u;
    if (s->callbacks.on_ready)             m |= 4u;
    if (s->callbacks.on_closed)            m |= 8u;
    if (s->callbacks.on_track_closed)      m |= 16u;
    if (s->callbacks.ctx)                  m |= 32u;
    if (s->publish_tracks)                 m |= 256u;
    if (s->drop_without_demand)            m |= 512u;
    return m;
}


/* Arm the one-shot republish fault: the live-write loop returns (as on
 * WOULD_BLOCK) immediately BEFORE writing `object_id` of the next pending
 * generation, snapshotting the loop state. */
void moq_media_sender_test_block_republish_before(moq_media_sender_t *s,
                                                  uint64_t object_id)
{
    pthread_mutex_lock(&s->mu);
    s->test_block_republish_obj = (int64_t)object_id;
    s->test_block_fired = false;
    pthread_mutex_unlock(&s->mu);
}


/* Arm a one-shot WOULD_BLOCK at the retained-install step (set_retained_group),
 * AFTER the live objects are written -- exercises retained-install backpressure
 * distinct from an object-write block. */
void moq_media_sender_test_block_retained_install_once(moq_media_sender_t *s)
{
    pthread_mutex_lock(&s->mu);
    s->test_block_retained_once = true;
    pthread_mutex_unlock(&s->mu);
}


/* Whether the armed fault fired; on true, returns the snapshot: the cursor
 * (== the blocked object id: everything before it was written exactly once),
 * the pending count (nonzero == the generation is NOT committed), and
 * whether the retained group had been installed at that instant. */
bool moq_media_sender_test_block_republish_state(moq_media_sender_t *s,
                                                 size_t *cursor,
                                                 size_t *count,
                                                 bool *retained_set)
{
    pthread_mutex_lock(&s->mu);
    bool fired = s->test_block_fired;
    if (fired) {
        if (cursor) *cursor = s->test_block_cursor;
        if (count) *count = s->test_block_count;
        if (retained_set) *retained_set = s->test_block_retained_set;
    }
    pthread_mutex_unlock(&s->mu);
    return fired;
}


/* Live republish state under the sender lock: the CURRENT pending cursor and
 * count (count == 0 means the last generation committed) and how many
 * retained-group installs have succeeded so far. */
void moq_media_sender_test_republish_live(moq_media_sender_t *s,
                                          size_t *cursor, size_t *count,
                                          unsigned *retained_installs)
{
    pthread_mutex_lock(&s->mu);
    if (cursor) *cursor = s->pending_obj_cursor;
    if (count) *count = s->pending_obj_count;
    if (retained_installs) *retained_installs = s->test_retained_installs;
    pthread_mutex_unlock(&s->mu);
}

#endif /* MOQ_MEDIA_SENDER_TESTING */
