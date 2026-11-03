#include "media_sender_internal.h"
#include "media_sender_pump_internal.h"
#include "media_sender_catalog_internal.h"
#include "media_sender_complete_internal.h"
#include "media_sender_timeline_internal.h"
#include "media_sender_tracks_internal.h"
#include "media_sender_write_internal.h"

#ifdef MOQ_MEDIA_SENDER_TESTING


static void (*sender_test_after_select)(moq_media_sender_t *, moq_media_track_t *, void *);

static void *sender_test_after_select_ctx;

#endif


/* Fire on_closed at most once. Clear every demand mirror with the close latch,
 * then deliver left callbacks and on_closed OUTSIDE the lock. Network thread. */
void sender_fire_closed(moq_media_sender_t *s, bool is_fatal,
                        uint64_t fatal_code)
{
    pthread_mutex_lock(&s->mu);
    if (s->closed_fired) { pthread_mutex_unlock(&s->mu); return; }
    s->closed_fired = true;
    moq_media_track_t *left = NULL;
    /* All request intents end with the service. Network-thread confined:
     * neither selectors nor native admission can be active concurrently. */
    moq_media_auth_operation_clear(&s->namespace_auth, &s->alloc);
    if (s->catalog_track) {
        moq_media_auth_operation_clear(&s->catalog_track->publish_auth, &s->alloc);
        s->catalog_track->active_subs = 0;
    }
    for (size_t i = 0; i < s->track_count; ++i) {
        moq_media_track_t *t = s->tracks[i];
        moq_media_auth_operation_clear(&t->publish_auth, &s->alloc);
        if (t->active_subs && track_is_app_media(t)) {
            t->closed_next = left;
            left = t;
        }
        t->active_subs = 0;
    }
    /* Terminal (incl. a CLEAN close): disarm the managed wake atomically with
     * the close latch, so a cached finite deadline cannot survive to a cycle
     * that returns early (e.g. session == NULL) before the next recompute. */
    s->refresh_wake_deadline_us = UINT64_MAX;
    void *cbctx = s->callbacks.ctx;
    void (*left_cb)(void *, moq_media_sender_t *, moq_media_track_t *, size_t)
        = s->callbacks.on_subscriber_left;
    void (*cb)(void *, moq_media_sender_t *, bool, uint64_t)
        = s->callbacks.on_closed;
    pthread_mutex_unlock(&s->mu);

    /* Handles, including removed tracks, live until sender destruction; only
     * the vector can move on an app-thread add. Link handles under mu instead
     * of allocating a snapshot, so even NOMEM cannot strand terminal demand.
     * All counts are already zero when the first callback queries them. */
    while (left) {
        moq_media_track_t *t = left;
        left = t->closed_next;
        t->closed_next = NULL;
        if (left_cb) left_cb(cbctx, s, t, 0);
    }
    if (cb) cb(cbctx, s, is_fatal, fatal_code);
}


/* Fire on_ready at most once, on the network thread, OUTSIDE s->mu. Claims the
 * ready_fired latch under the lock, then invokes the callback after release. */
void sender_fire_ready(moq_media_sender_t *s)
{
    pthread_mutex_lock(&s->mu);
    if (s->ready_fired) { pthread_mutex_unlock(&s->mu); return; }
    s->ready_fired = true;
    void *cbctx = s->callbacks.ctx;
    void (*cb)(void *, moq_media_sender_t *) = s->callbacks.on_ready;
    pthread_mutex_unlock(&s->mu);

    if (cb) cb(cbctx, s);
}


void sender_set_fatal(moq_media_sender_t *s, uint64_t code)
{
    pthread_mutex_lock(&s->mu);
    sender_set_fatal_locked(s, code);
    pthread_mutex_unlock(&s->mu);
    /* A fatal is a terminal close: surface it as on_closed(is_fatal=true). */
    sender_fire_closed(s, true, code);
}

/* Resource names and namespace storage survive until sender destruction. */
static moq_media_auth_request_t sender_auth_request(moq_media_sender_t *s,
    moq_media_track_t *t, moq_media_auth_action_t action)
{
    moq_media_auth_request_t request = {0};
    request.struct_size = sizeof(request);
    request.action = action;
    request.track_namespace = s->namespace_;
    if (action != MOQ_MEDIA_AUTH_ACTION_PUBLISH_NAMESPACE)
        request.track_name = t->is_catalog
            ? (moq_bytes_t){s->catalog_name, s->catalog_name_len} : t->name;
    return request;
}


/* The failure latch and queue purge precede the allocation-free notification.
 * Only this resource's service-owned objects are discarded. */
static void sender_auth_fail(moq_media_sender_t *s, moq_media_track_t *t,
    moq_media_auth_operation_t *op, const moq_media_auth_request_t *request)
{
    pthread_mutex_lock(&s->mu);
    t->auth_failed = true;
    if (t->sap_timeline) t->sap_timeline->removed = true;
    if (t->media_timeline) t->media_timeline->removed = true;
    if (t->is_catalog) sender_set_fatal_locked(s, MOQ_MEDIA_SENDER_FATAL_SETUP_FAILED);
    uint32_t wr = s->preq_head;
    for (uint32_t rd = s->preq_head; rd != s->preq_tail; ++rd) {
        sender_preq_entry_t *e = &s->preq[rd % s->ring_cap];
        if (t->is_catalog || e->track == t) {
            if (!e->is_end) preq_note_dropped(s, e->track, e);
            preq_entry_release(s, e);
        } else {
            if (wr != rd) s->preq[wr % s->ring_cap] = *e;
            ++wr;
        }
    }
    s->preq_tail = wr;
    if (s->ready || s->tracks_frozen) s->catalog_dirty = true;
    pthread_cond_broadcast(&s->space_cv);
    pthread_mutex_unlock(&s->mu);
    moq_media_auth_operation_notify(op, &s->resource_auth, request,
        t->is_catalog ? MOQ_MEDIA_AUTH_SCOPE_SERVICE : MOQ_MEDIA_AUTH_SCOPE_TRACK,
        t->is_catalog || track_is_generated_timeline(t) ? NULL : t);
    moq_media_auth_operation_clear(op, &s->alloc);
    if (t->is_catalog) {
        moq_media_auth_operation_clear(&s->namespace_auth, &s->alloc);
        moq_media_auth_operation_clear(&t->publish_auth, &s->alloc);
        pthread_mutex_lock(&s->mu);
        for (size_t i = 0; i < s->track_count; ++i)
            moq_media_auth_operation_clear(&s->tracks[i]->publish_auth, &s->alloc);
        pthread_mutex_unlock(&s->mu);
    }
    if (s->ep) moq_endpoint_wake(s->ep);
}


static bool sender_admission_retry(moq_result_t rc)
{
    return rc == MOQ_ERR_WOULD_BLOCK || rc == MOQ_ERR_REQUEST_BLOCKED;
}


/* The helper already validated the prepared list. Native INVAL can reject
 * request identity, so it retains the ordinary service failure path. */
static bool sender_auth_native_error(moq_media_sender_t *s,
    moq_media_auth_operation_t *op, moq_result_t rc)
{
    if (!s->resource_auth.configured) return false;
    if (rc == MOQ_ERR_BUFFER) op->outcome = MOQ_MEDIA_AUTH_OUTCOME_TOKEN_LIMIT;
    else if (rc == MOQ_ERR_UNSUPPORTED) op->outcome = MOQ_MEDIA_AUTH_OUTCOME_UNSUPPORTED_PROFILE;
    else return false;
    op->status = op->result = rc;
    return true;
}


/* Network executor, no private lock on entry. Track identity is immutable and
 * its allocation survives removal. Recheck the intent after every selector. */
static moq_result_t sender_select(moq_media_sender_t *s, moq_media_track_t *t,
    moq_media_auth_operation_t *op, moq_media_auth_action_t action)
{
    pthread_mutex_lock(&s->mu);
    bool cancelled = s->fatal || t->removed || t->auth_failed;
    pthread_mutex_unlock(&s->mu);
    if (cancelled) {
        moq_media_auth_operation_clear(op, &s->alloc);
        return MOQ_DONE;
    }
    moq_media_auth_request_t request = sender_auth_request(s, t, action);
    moq_result_t rc = moq_media_auth_operation_prepare(op, &s->resource_auth,
        &request, s->transport_version, &s->alloc);
#ifdef MOQ_MEDIA_SENDER_TESTING
    /* Model an app-thread intent change AFTER the selector returns, not
     * prohibited callback reentry. No shipping hook or export. */
    if (sender_test_after_select)
        sender_test_after_select(s, t, sender_test_after_select_ctx);
#endif
    pthread_mutex_lock(&s->mu);
    cancelled = s->fatal || t->removed || t->auth_failed;
    pthread_mutex_unlock(&s->mu);
    if (cancelled) {
        moq_media_auth_operation_clear(op, &s->alloc);
        return MOQ_DONE;
    }
    if (rc != MOQ_OK) sender_auth_fail(s, t, op, &request);
    return rc;
}


static moq_result_t sender_publish_track(moq_media_sender_t *s,
    moq_media_track_t *t, uint64_t now_us)
{
    if (!s->publish_tracks || t->publish_submitted) return MOQ_OK;
    moq_result_t rc = sender_select(s, t, &t->publish_auth, MOQ_MEDIA_AUTH_ACTION_PUBLISH);
    if (rc != MOQ_OK) return rc;
    moq_pub_publish_cfg_t cfg;
    moq_pub_publish_cfg_init(&cfg);
    cfg.auth_tokens = t->publish_auth.list.tokens;
    cfg.auth_token_count = t->publish_auth.list.count;
    pthread_mutex_lock(&s->mu);
    if (t->removed || t->auth_failed || s->fatal) rc = MOQ_DONE;
    else {
        rc = moq_pub_publish_track(s->pub, t->pub_track, &cfg, now_us);
#ifdef MOQ_MEDIA_SENDER_TESTING
        t->test_publish_attempts++;
        t->test_publish_result = rc;
#endif
        if (rc == MOQ_OK) t->publish_submitted = true;
    }
    pthread_mutex_unlock(&s->mu);
    if (sender_admission_retry(rc)) return rc;
    if (rc < 0) {
        if (sender_auth_native_error(s, &t->publish_auth, rc)) {
            moq_media_auth_request_t req = sender_auth_request(s, t, MOQ_MEDIA_AUTH_ACTION_PUBLISH);
            sender_auth_fail(s, t, &t->publish_auth, &req);
        } else sender_set_fatal(s, MOQ_MEDIA_SENDER_FATAL_SETUP_FAILED);
    }
    moq_media_auth_operation_clear(&t->publish_auth, &s->alloc);
    return rc;
}


static moq_result_t sender_add_pub_track(moq_media_sender_t *s, moq_media_track_t *t,
                                        bool advertise, uint64_t now_us)
{
    moq_result_t rc;
    if (advertise) {
        rc = sender_select(s, t, &s->namespace_auth, MOQ_MEDIA_AUTH_ACTION_PUBLISH_NAMESPACE);
        if (rc != MOQ_OK) return rc;
    }
    /* Select even before local registration/catalog inclusion. Catalog's
     * PUBLISH itself waits until its retained initial group is installed. */
    if (s->publish_tracks) {
        rc = sender_select(s, t, &t->publish_auth, MOQ_MEDIA_AUTH_ACTION_PUBLISH);
        if (rc != MOQ_OK) return rc;
    }
    moq_pub_track_cfg_t tcfg;
    /* Sized init: the catalog branch below sets the APPENDED
     * has_publisher_priority tail field, which moq_pub_add_track reads only
     * when struct_size covers it -- the pointer-only init's frozen v0
     * struct_size would silently drop it (default priority 128, not 0). */
    moq_pub_track_cfg_init_sized(&tcfg, sizeof(tcfg));
    tcfg.track_namespace = s->namespace_;
    tcfg.track_name = t->is_catalog
        ? (moq_bytes_t){ s->catalog_name, s->catalog_name_len }
        : t->name;
    tcfg.advertise_namespace = advertise;
    if (advertise) {
        tcfg.namespace_auth_tokens = s->namespace_auth.list.tokens;
        tcfg.namespace_auth_token_count = s->namespace_auth.list.count;
    }

    if (t->is_catalog) {
        tcfg.has_publisher_priority = true;
        tcfg.publisher_priority = 0;
    } else if (t->has_publisher_priority) {
        tcfg.has_publisher_priority = true;
        tcfg.publisher_priority = t->publisher_priority;
    }
    tcfg.group_order = t->group_order;
    /* The sender's production is strictly monotonic on every track: media
     * emission drains group_seq in order (eviction only drops OLDER groups
     * before emission; an abandoned group never rewinds), and catalog
     * generations only advance, with delta objects appended to the OPEN
     * generation group. Declaring it lets finite subscription filters
     * auto-complete once production passes their end group. */
    tcfg.monotonic_groups = true;
    tcfg.track_properties = t->track_properties;
    tcfg.dynamic_groups = t->dynamic_groups;

    pthread_mutex_lock(&s->mu);
    if (t->removed || t->auth_failed || s->fatal) rc = MOQ_DONE;
    else rc = moq_pub_add_track(s->pub, &tcfg, now_us, &t->pub_track);
    pthread_mutex_unlock(&s->mu);
    if (sender_admission_retry(rc)) return rc;
    if (rc != MOQ_OK) {
        if (advertise && sender_auth_native_error(s, &s->namespace_auth, rc)) {
            moq_media_auth_request_t req = sender_auth_request(s, t, MOQ_MEDIA_AUTH_ACTION_PUBLISH_NAMESPACE);
            sender_auth_fail(s, t, &s->namespace_auth, &req);
        } else if (rc < 0) sender_set_fatal(s, MOQ_MEDIA_SENDER_FATAL_SETUP_FAILED);
        moq_media_auth_operation_clear(&t->publish_auth, &s->alloc);
        if (advertise) moq_media_auth_operation_clear(&s->namespace_auth, &s->alloc);
        return rc;
    }
    if (advertise) moq_media_auth_operation_clear(&s->namespace_auth, &s->alloc);
    return t->is_catalog ? MOQ_OK : sender_publish_track(s, t, now_us);
}


/* Retire a cancelled registration in startup as well as after readiness.
 * Auth-refused publications were never sent: remove their local registration
 * without manufacturing a successful END_OF_TRACK. Cleanup retries do not
 * reselect credentials or reissue a refused publication. */
static bool sender_retire_track(moq_media_sender_t *s, moq_media_track_t *t,
                                 uint64_t now_us)
{
    moq_media_auth_operation_clear(&t->publish_auth, &s->alloc);
    pthread_mutex_lock(&s->mu);
    bool failed = t->auth_failed;
    /* Direct removal can precede eviction: the facade still owns a blocked
     * WRITE_OBJECT even when no subgroup opened. Abandon it retryably before
     * END_OF_TRACK, which cannot overtake that retained operation. */
    if (t->pub_track && t->write_blocked) t->pending_reset = true;
    moq_result_t rc = t->pending_reset ? sender_flush_reset(s, t, now_us) : MOQ_OK;
    pthread_mutex_unlock(&s->mu);
    if (rc != MOQ_OK) return false;
    if (t->pub_track) {
        if (!failed && !t->pub_ended) {
#ifdef MOQ_MEDIA_SENDER_TESTING
            pthread_mutex_lock(&s->mu);
            bool refuse = s->test_refuse_teardown_end > 0;
            if (refuse) s->test_refuse_teardown_end--;
            pthread_mutex_unlock(&s->mu);
            if (refuse) return false;
#endif
            rc = moq_pub_end_track(s->pub, t->pub_track, now_us);
            if (rc == MOQ_ERR_WOULD_BLOCK || rc == MOQ_ERR_WRONG_STATE) return false;
            if (rc != MOQ_OK) {
                sender_set_fatal(s, MOQ_MEDIA_SENDER_FATAL_SETUP_FAILED);
                return false;
            }
            t->pub_ended = true;
        }
        rc = moq_pub_remove_track(s->pub, t->pub_track, now_us);
        if (rc == MOQ_ERR_WOULD_BLOCK) return false;
        if (rc != MOQ_OK) {
            sender_set_fatal(s, MOQ_MEDIA_SENDER_FATAL_SETUP_FAILED);
            return false;
        }
        t->pub_track = NULL;
    }
    pthread_mutex_lock(&s->mu);
    t->teardown_done = true;
    pthread_mutex_unlock(&s->mu);
    return true;
}


/* -- Demand visibility (§7.2): subscriber join/left mirroring -------- */

/* Reconcile one track's mirrored subscriber count with the publisher's
 * AUTHORITATIVE count and fire the service callback on a change. Polling rather
 * than relying on the core on_subscriber_left is required because the publisher
 * clears subscriber slots WITHOUT a left callback on session close
 * (MOQ_EVENT_SESSION_CLOSED) and on local finish paths (end_track / VOD finish);
 * an increment/decrement mirror would go stale after those. Network thread: the
 * authoritative count is read outside s->mu (a const query, never re-entering
 * the lock), the mirror is updated under s->mu, and the app callback fires
 * OUTSIDE s->mu for app-visible media tracks (the non-reentrant contract). */
void sender_resync_track_demand(moq_media_sender_t *s,
                                moq_media_track_t *t)
{
    size_t real = (s->pub && t->pub_track)
        ? moq_pub_active_subscriptions(s->pub, t->pub_track) : 0;
#ifdef MOQ_MEDIA_SENDER_TESTING
    if (t->test_demand_set) real = t->test_demand;
#endif

    pthread_mutex_lock(&s->mu);
    /* A close may precede publisher event consumption. Its zero demand is
     * final even if a later resync still sees live facade subscriptions. */
    if (s->closed_fired) { pthread_mutex_unlock(&s->mu); return; }
    size_t old = t->active_subs;
    if (real == old) { pthread_mutex_unlock(&s->mu); return; }
    t->active_subs = real;
    bool fire = track_is_app_media(t);
    void *cbctx = s->callbacks.ctx;
    void (*joined_cb)(void *, moq_media_sender_t *, moq_media_track_t *, size_t)
        = s->callbacks.on_subscriber_joined;
    void (*left_cb)(void *, moq_media_sender_t *, moq_media_track_t *, size_t)
        = s->callbacks.on_subscriber_left;
    pthread_mutex_unlock(&s->mu);

    if (!fire) return;
    if (real > old) { if (joined_cb) joined_cb(cbctx, s, t, real); }
    else            { if (left_cb)   left_cb(cbctx, s, t, real); }
}


/* Resync demand across all tracks. Called after moq_pub_tick() and after the
 * service teardown paths (end/remove/VOD finish) that can clear subscriptions.
 * The catalog track is private (not in s->tracks[]); resync it too so its
 * mirror is accurate, though it never fires an app callback. */
static void sender_resync_demand(moq_media_sender_t *s)
{
    if (!s->pub) return;
    if (s->catalog_track) sender_resync_track_demand(s, s->catalog_track);
    size_t n = 0;
    moq_media_track_t **snap = NULL;
    /* NOMEM: skip this resync pass -- the mirror reconciles next pump. */
    if (!sender_tracks_snapshot(s, &snap, &n)) return;
    for (size_t i = 0; i < n; i++)
        sender_resync_track_demand(s, snap[i]);
    sender_tracks_release(s, snap, n);
}


/* Peer rejected a catalog or media-track PUBLISH: the sender can never
 * deliver on it, so become fatal (PUBLISH_REJECTED) -- surfaced once via
 * on_closed(is_fatal=true) and waking any blocked writer. No auto-retry:
 * the service has no auth/policy context to redecide; the app may recreate
 * the sender. */
static void sender_pub_on_publish_error(void *ctx, moq_pub_track_t *track,
                                        moq_request_error_t error_code)
{
    (void)track; (void)error_code;
    moq_media_sender_t *s = (moq_media_sender_t *)ctx;
    sender_set_fatal(s, MOQ_MEDIA_SENDER_FATAL_PUBLISH_REJECTED);
}


/* Loss of the advertised namespace is sender-local. Keep operation closure
 * distinct from a peer REQUEST_ERROR and use the existing once-only fatal path. */
static void sender_pub_on_namespace_terminal(void *ctx,
    const moq_pub_namespace_terminal_info_t *info)
{
    moq_media_sender_t *s = (moq_media_sender_t *)ctx;
    uint64_t code = info->kind == MOQ_PUB_NAMESPACE_TERMINATED
        ? MOQ_MEDIA_SENDER_FATAL_NAMESPACE_TERMINATED
        : info->kind == MOQ_PUB_NAMESPACE_CANCELLED
            ? MOQ_MEDIA_SENDER_FATAL_NAMESPACE_CANCELLED
            : MOQ_MEDIA_SENDER_FATAL_NAMESPACE_REJECTED;
    sender_set_fatal(s, code);
}


/* A subscriber asked for a new group on a dynamic-groups track: latch it for
 * moq_media_sender_take_new_group_request. Fires on the network thread from
 * moq_pub_tick, which runs without s->mu. */
/* Publisher callback on the network thread inside moq_pub_tick (sender_hook,
 * ep->mu held): the peer's session-wide GOAWAY. The endpoint keeps its New
 * Session URI for moq_endpoint_get_goaway_uri(). */
static void sender_pub_on_goaway(void *ctx, moq_bytes_t new_session_uri)
{
    moq_media_sender_t *s = (moq_media_sender_t *)ctx;
    moq_endpoint_note_goaway_locked(s->ep, new_session_uri);
}


static void sender_pub_on_new_group_request(
    void *ctx, moq_pub_track_t *track, const moq_pub_new_group_request_info_t *info)
{
    moq_media_sender_t *s = (moq_media_sender_t *)ctx;
    pthread_mutex_lock(&s->mu);
    for (size_t i = 0; i < s->track_count; i++) {
        moq_media_track_t *t = s->tracks[i];
        if (t->pub_track != track) continue;
        t->new_group_pending = true;
        t->new_group_value = info->requested_group;
        break;
    }
    pthread_mutex_unlock(&s->mu);
}


static void sender_pub_on_closed(void *ctx, uint64_t error_code)
{
    (void)error_code;   /* the peer's session error is not the sender fatal code */
    moq_media_sender_t *s = (moq_media_sender_t *)ctx;
    sender_fire_closed(s, false, 0);
}


/* A subscription was accepted (inside moq_pub_tick, which the hook runs
 * without s->mu). Only a catalog join matters here, while a terminal
 * generation's live write has begun -- written in part, or held by the facade
 * with its destinations already fixed -- and before its subscriptions are
 * finished: that subscription misses the generation
 * (sender_republish_catalog). Repeated joins coalesce into one rewrite. */
static void sender_pub_on_subscriber_joined(void *ctx, moq_pub_track_t *pt)
{
    moq_media_sender_t *s = (moq_media_sender_t *)ctx;
    pthread_mutex_lock(&s->mu);
    if (s->catalog_track && s->catalog_track->pub_track == pt &&
        s->pending_terminal && s->pending_obj_count > 0 &&
        (s->pending_obj_cursor > 0 || s->pending_write_open) &&
        !s->pending_subs_finished)
        s->pending_rejoined = true;
    pthread_mutex_unlock(&s->mu);
}


static void sender_pub_on_publish_ok(void *ctx, moq_pub_track_t *pt, bool forward)
{
    (void)forward;   /* Forward=0 resolves the handshake without demand. */
    moq_media_sender_t *s = (moq_media_sender_t *)ctx;
    pthread_mutex_lock(&s->mu);
    if (s->catalog_track && s->catalog_track->pub_track == pt)
        s->catalog_track->publish_resolved = true;
    for (size_t i = 0; i < s->track_count; i++) {
        if (s->tracks[i]->pub_track != pt) continue;
        s->tracks[i]->publish_resolved = true;
        break;
    }
    pthread_mutex_unlock(&s->mu);
}


void sender_pub_on_publish_finished(void *ctx, moq_pub_track_t *pt)
{
    moq_media_sender_t *s = (moq_media_sender_t *)ctx;

    pthread_mutex_lock(&s->mu);

    /* The catalog track is private (not in s->tracks[]); check it first. Only
     * the peer ends a publication here (a local unpublish reports nothing), so
     * this is never the completion's own finish. Resolved, a completion
     * waiting on the PUBLISH answer settles without a publication to end. */
    if (s->catalog_track && s->catalog_track->pub_track == pt) {
        s->catalog_track->publish_resolved = true;
        pthread_mutex_unlock(&s->mu);
        sender_fire_closed(s, false, 0);   /* no catalog -> session is done */
        return;
    }

    moq_media_track_t *t = NULL;
    for (size_t i = 0; i < s->track_count; i++) {
        if (s->tracks[i]->pub_track == pt) { t = s->tracks[i]; break; }
    }
    if (t) t->publish_resolved = true;  /* includes private generated timelines */
    /* Fire once per track, for app-visible media only. */
    bool fire = t && track_is_app_media(t) && !t->pub_closed_fired;
    if (fire) t->pub_closed_fired = true;
    void *cbctx = s->callbacks.ctx;
    void (*cb)(void *, moq_media_sender_t *, moq_media_track_t *)
        = s->callbacks.on_track_closed;
    pthread_mutex_unlock(&s->mu);

    if (fire && cb) cb(cbctx, s, t);
}


/* Resolve an AUTO companion against the draft just latched: draft 22 selects
 * D22_MSF01_LOC04_PUBLIC, drafts 16/18 LEGACY_MSF01_LOC01, and any other draft
 * leaves it unresolved, which no draft can use. The latch runs before this
 * session's publisher, catalog or first object exists, so nothing reaches the
 * wire under an unresolved companion. Resolution happens once:
 *   draft-22: Group IDs already assigned keep the wallclock base read at
 *     creation. InitDataList IDs were assigned at registration on every profile.
 *   legacy: Group IDs assigned meanwhile were counted from that base; they
 *     are moved down by it, so the legacy companion still starts at 0.
 * CALLER MUST HOLD s->mu. */
static void sender_resolve_companion_locked(moq_media_sender_t *s)
{
    if (s->companion_profile == MOQ_MEDIA_COMPANION_AUTO) {
        moq_version_t v = s->transport_version;
        if (v == MOQ_VERSION_DRAFT_22) {
            s->companion_profile = MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC;
        } else if (v == MOQ_VERSION_DRAFT_16 || v == MOQ_VERSION_DRAFT_18) {
            s->companion_profile = MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01;
            /* A configured initial Group ID is the application's, for either
             * companion: nothing moves down by it. */
            uint64_t base = s->group_base_explicit ? 0 : s->group_base;
            for (size_t i = 0; i < s->track_count; i++) {
                moq_media_track_t *t = s->tracks[i];
                if (t->group_started) t->group_seq -= base;
                if (t->drop_group_valid) t->drop_group_last -= base;
            }
            for (uint32_t i = s->preq_head; i != s->preq_tail; i++) {
                sender_preq_entry_t *e = &s->preq[i % s->ring_cap];
                if (e->track && e->track->group_started) e->group_seq -= base;
            }
            if (!s->group_base_explicit) s->group_base = 0;
        }
    }
}

#ifdef MOQ_MEDIA_SENDER_TESTING


static atomic_size_t startup_hook_calls, startup_first_track_count;

void moq_media_sender_test_startup_hook_reset(void);

void moq_media_sender_test_startup_hook_reset(void)
{
    atomic_store(&startup_hook_calls, 0);
    atomic_store(&startup_first_track_count, 0);
}

size_t moq_media_sender_test_startup_hook_calls(void);

size_t moq_media_sender_test_startup_hook_calls(void) { return atomic_load(&startup_hook_calls); }

size_t moq_media_sender_test_startup_first_tracks(void);

size_t moq_media_sender_test_startup_first_tracks(void) { return atomic_load(&startup_first_track_count); }

#endif


void sender_hook(moq_endpoint_t *ep, moq_session_t *session,
                 uint64_t now_us, void *ctx)
{
    (void)ep;
    moq_media_sender_t *s = (moq_media_sender_t *)ctx;

    pthread_mutex_lock(&s->mu);
#ifdef MOQ_MEDIA_SENDER_TESTING
    if (atomic_fetch_add(&startup_hook_calls, 1) == 0)
        atomic_store(&startup_first_track_count, s->track_count);
#endif
    bool fatal = s->fatal;
    uint64_t fatal_code = s->fatal_code;
    pthread_mutex_unlock(&s->mu);
    if (fatal) {
        sender_fire_closed(s, true, fatal_code);
        return;
    }

    if (sender_ep_closed(s)) {
        sender_fire_closed(s, moq_endpoint_is_fatal_internal(s->ep),
                           moq_endpoint_fatal_code_internal(s->ep));
        return;
    }
    if (!session) return;

    /* Read the negotiated draft from the live session, under our own mutex,
     * before any work in this pass can encode an object. The session's
     * version is fixed at creation and immutable, so the latch cannot go
     * stale. Endpoint v0 owns one session per endpoint and never reconnects
     * (MOQ_ENDPOINT_RECONNECTING is reserved but unentered), so re-reading
     * each pass is redundant today -- it is the ordering that stays correct
     * when reconnect lands. */
    {
        moq_version_t v = moq_session_version(session);
        pthread_mutex_lock(&s->mu);
        s->transport_version = v;
        sender_resolve_companion_locked(s);
        bool usable = sender_version_usable(s);
        if (!usable) s->stats.last_error = MOQ_ERR_UNSUPPORTED;
        pthread_mutex_unlock(&s->mu);
        if (!usable) return;
    }

    if (!s->pub) {
        moq_session_state_t st = moq_session_state(session);
        /* SETUP and a session-wide GOAWAY can both precede this sender's
         * first pass (or the sender is attached after them): the facade is
         * still created, as the session's one event owner, so the GOAWAY and
         * its New Session URI are observed; nothing is published on a
         * session that is going away. */
        bool observe = st == MOQ_SESS_DRAINING;
        if (st != MOQ_SESS_ESTABLISHED && !observe)
            return;

        moq_pub_cfg_t pcfg;
        moq_pub_cfg_init_sized(&pcfg, sizeof(pcfg));
        pcfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
        pcfg.callbacks.ctx = s;
        pcfg.callbacks.on_closed = sender_pub_on_closed;
        pcfg.callbacks.on_subscriber_joined = sender_pub_on_subscriber_joined;
        pcfg.callbacks.on_publish_ok = sender_pub_on_publish_ok;
        pcfg.callbacks.on_publish_finished = sender_pub_on_publish_finished;
        pcfg.callbacks.on_publish_error = sender_pub_on_publish_error;
        pcfg.callbacks.on_namespace_terminal =
            sender_pub_on_namespace_terminal;
        pcfg.on_new_group_request = sender_pub_on_new_group_request;
        pcfg.on_new_group_request_ctx = s;
        pcfg.on_goaway = sender_pub_on_goaway;
        if (moq_pub_create(session, &s->alloc, &pcfg, &s->pub) != MOQ_OK) {
            sender_set_fatal(s, MOQ_MEDIA_SENDER_FATAL_SETUP_FAILED);
            return;
        }
        s->observe_only = observe;
    }
    if (s->observe_only) {
        (void)moq_pub_tick(s->pub, now_us);
        return;
    }
    if (!s->tracks_frozen) {

        /* Freeze the app's track list and build pub tracks from it. The
         * catalog track carries the single namespace advertisement; media
         * tracks share the namespace (already announced) so the publisher
         * answers their subscribes without re-advertising. */
        pthread_mutex_lock(&s->mu);
        s->tracks_frozen = true;
        pthread_mutex_unlock(&s->mu);
    }

    if (!s->startup_complete) {
        /* Consume namespace/publication replies even while another startup
         * request is waiting for native admission. Selection stays retained. */
        (void)moq_pub_tick(s->pub, now_us);
        pthread_mutex_lock(&s->mu);
        fatal = s->fatal;
        pthread_mutex_unlock(&s->mu);
        if (fatal) return;
        if (!s->catalog_track->pub_track &&
            sender_add_pub_track(s, s->catalog_track, true, now_us) != MOQ_OK)
            return;

        /* Walk a snapshot so a concurrent app-thread realloc of s->tracks cannot
         * free the vector mid-walk; the per-track registration below takes s->mu
         * to decide and create the publisher track atomically. NOMEM here is
         * fatal -- proceeding
         * as "no tracks" would publish an empty initial catalog. */
        size_t n = 0;
        moq_media_track_t **snap = NULL;
        if (!sender_tracks_snapshot(s, &snap, &n)) {
            sender_set_fatal(s, MOQ_MEDIA_SENDER_FATAL_SETUP_FAILED);
            return;
        }
        for (size_t i = 0; i < n; i++) {
            moq_media_track_t *t = snap[i];
            if (t->is_catalog) continue;   /* creation-time immutable */
            /* A track removed before ready must not get a publisher track (it is
             * absent from the initial catalog -- otherwise it would be an
             * undeclared live track). removed is app-written and monotonic, so
             * the decision and the registration must be atomic under s->mu, or a
             * concurrent remove_track in the gap could leave a removed track with
             * a publisher track. */
            pthread_mutex_lock(&s->mu);
            bool skip = t->removed || t->auth_failed;
            pthread_mutex_unlock(&s->mu);
            if (skip) {
                if (!sender_retire_track(s, t, now_us)) {
                    sender_tracks_release(s, snap, n);
                    return;
                }
                continue;
            }
            moq_result_t rc = t->pub_track ? sender_publish_track(s, t, now_us)
                : sender_add_pub_track(s, t, false, now_us);
            if (sender_admission_retry(rc)) {
                sender_tracks_release(s, snap, n);
                return;
            }
        }
        sender_tracks_release(s, snap, n);
        pthread_mutex_lock(&s->mu);
        fatal = s->fatal;
        pthread_mutex_unlock(&s->mu);
        if (fatal) return;

        /* Derive the initial catalog and install it as the retained group
         * (generation 0, object 0) so an explicit Joining FETCH(offset 0) can
         * pull it. A plain SUBSCRIBE delivers no retained objects; the receiver
         * obtains the catalog via SUBSCRIBE + Joining FETCH (MSF-01 §5).
         * sender_build_catalog_fresh requires s->mu (consistent track view). */
        if (!s->catalog_published) {
            moq_rcbuf_t *json = NULL;
            uint64_t json_stamp = 0;
            pthread_mutex_lock(&s->mu);
            s->catalog_dirty = false;
            moq_result_t bcrc = sender_build_catalog_fresh(s, &json, true,
                                                          &json_stamp);
            uint64_t first_group = sender_group_base(s);
            /* Commit the exact encoded track set before a blocked PUBLISH.
             * Later app mutations must remain dirty until the next generation. */
            if (bcrc == MOQ_OK && json)
                for (size_t i = 0; i < s->track_count; ++i)
                    s->tracks[i]->in_published = track_in_catalog(s->tracks[i], true);
            pthread_mutex_unlock(&s->mu);
            if (bcrc != MOQ_OK || !json) {
                sender_set_fatal(s, MOQ_MEDIA_SENDER_FATAL_CATALOG_ENCODE);
                return;
            }
            moq_pub_retained_object_t robj = { .object_id = 0, .payload = json };
            moq_pub_retained_group_cfg_t rg;
            moq_pub_retained_group_cfg_init(&rg);
            rg.group_id = first_group;
            rg.objects = &robj;
            rg.object_count = 1;
            moq_result_t src = moq_pub_set_retained_group(
                s->pub, s->catalog_track->pub_track, &rg);
            if (src != MOQ_OK) {
                moq_rcbuf_decref(json);
                sender_set_fatal(s, MOQ_MEDIA_SENDER_FATAL_SETUP_FAILED);
                return;
            }
#ifdef MOQ_MEDIA_SENDER_TESTING
            pthread_mutex_lock(&s->mu);
            s->test_retained_installs++;
            pthread_mutex_unlock(&s->mu);
#endif
            s->published_catalog = json;
            s->published_generated_at = json_stamp;
            s->catalog_group = first_group;
            s->catalog_published = true;
        }
        if (sender_publish_track(s, s->catalog_track, now_us) != MOQ_OK) return;
        s->startup_complete = true;
        sender_arm_refresh(s, now_us);
    }

    (void)moq_pub_tick(s->pub, now_us);

    /* A dispatched event (namespace rejection/cancellation, or a rejected
     * PUBLISH) can have turned the sender fatal inside the tick above. That
     * is terminal: stop this cycle before any further work -- especially the
     * readiness block below, which would otherwise fire on_ready after the
     * fatal on_closed. The fatal path already fired on_closed and woke
     * waiters. */
    {
        pthread_mutex_lock(&s->mu);
        bool now_fatal = s->fatal;
        pthread_mutex_unlock(&s->mu);
        if (now_fatal) return;
    }

    /* Reconcile demand right after the tick so a fresh subscribe fires the join
     * callback in the same cycle it is accepted (before the drain runs). */
    sender_resync_demand(s);

    /* Publish-initiated: once the peer accepts the catalog PUBLISH, push the
     * initial catalog object live so a relay caches it without a FETCH. */
    if (s->publish_tracks && !s->live_catalog_sent && s->catalog_published &&
        sender_track_demand(s, s->catalog_track) &&
        s->published_catalog) {
        moq_pub_object_cfg_t ocfg;
        moq_pub_object_cfg_init_sized(&ocfg, sizeof(ocfg));

        /* Same (first group, object 0) as the retained group: it is the same
         * catalog object, so a live subscriber and a FETCH joiner see one
         * identity. */
        pthread_mutex_lock(&s->mu);
        ocfg.group_id = sender_group_base(s);
        pthread_mutex_unlock(&s->mu);
        ocfg.object_id = 0;
        ocfg.payload = s->published_catalog;
        ocfg.end_of_group = true;
        moq_result_t wrc = moq_pub_write_object_ex(
            s->pub, s->catalog_track->pub_track, &ocfg, now_us);
        /* The entry gate above already required demand, and nothing between it
         * and here can withdraw it: moq_pub_write_object_ex() never touches
         * publish_ok / publish_forward and drains no events, so a second
         * sender_track_demand() read could not differ. */
        if (wrc == MOQ_OK)
            s->live_catalog_sent = true;
    }

    /* Ready: catalog published AND accepted by the peer - via the namespace
     * advertisement (pull) or the catalog's PUBLISH (publish). */
    if (!s->ready && s->catalog_published &&
        (moq_pub_namespace_accepted(s->pub, s->catalog_track->pub_track) ||
         (s->publish_tracks &&
          moq_pub_track_is_published(s->pub, s->catalog_track->pub_track)))) {
        pthread_mutex_lock(&s->mu);
        s->ready = true;
        pthread_cond_broadcast(&s->space_cv);  /* bound widened to queue_cap */
        pthread_mutex_unlock(&s->mu);
        sender_fire_ready(s);
    }

    /* Post-ready track maintenance: tear down removed tracks (end-of-track, then
     * remove the publisher track) and register a pub track for any post-ready
     * add. The walk is over a snapshot; publisher calls run without s->mu except
     * the registration decision+create, which is taken under s->mu so it is
     * atomic against a concurrent remove_track. Both honor MOQ_ERR_WOULD_BLOCK
     * (retry next
     * pump). Runs before the drain so a track written immediately after a
     * post-ready add_track has its wire track ready when its queue drains. */
    if (s->ready && !s->fatal) {
        size_t n = 0;
        moq_media_track_t **snap = NULL;
        /* On NOMEM the snapshot yields snap=NULL/n=0, so this idempotent pass is
         * simply skipped and retried next pump -- it advances no state, which is
         * the correct "preserve" behavior here (distinct from a real empty list,
         * which also legitimately does nothing). */
        (void)sender_tracks_snapshot(s, &snap, &n);
        for (size_t i = 0; i < n; i++) {
            moq_media_track_t *t = snap[i];
            if (t->is_catalog) continue;   /* creation-time immutable */
            /* Intent latches are private-lock protected. The retirement
             * helper checks any pending reset before ending the track. */
            pthread_mutex_lock(&s->mu);
            bool removed = t->removed;
            bool auth_failed = t->auth_failed;
            bool stopped = s->fatal;
            pthread_mutex_unlock(&s->mu);
            if (stopped) break;
            if (removed || auth_failed) {
                (void)sender_retire_track(s, t, now_us);
                continue;
            }
            if (!t->pub_track || (s->publish_tracks && !t->publish_submitted)) {
                /* Defer registering a replacement until the old same-name
                 * publisher track (if any) is actually gone. */
                if (removed_pubtrack_with_name(s, snap, n, t->name)) continue;
                /* Decide and register atomically under s->mu: re-check removed
                 * (app-written, monotonic) in the same lock that creates the
                 * publisher track so a concurrent remove_track cannot leave a
                 * removed track advertised. A track only becomes catalog-eligible
                 * once registered (only_registered build); registration does not
                 * otherwise dirty the catalog, so a track excluded from the prior
                 * publish while unregistered must trigger a republish now or it
                 * could never be advertised -- set catalog_dirty under the same
                 * lock. */
                moq_result_t rc = t->pub_track ? sender_publish_track(s, t, now_us)
                    : sender_add_pub_track(s, t, false, now_us);
                pthread_mutex_lock(&s->mu);
                if (rc == MOQ_OK && !t->removed && !t->auth_failed)
                    s->catalog_dirty = true;
                pthread_mutex_unlock(&s->mu);
            }
        }
        sender_tracks_release(s, snap, n);
    }

    /* Drain queued media once ready. Holds s->mu across emission: the
     * session is network-thread confined and nothing the facade touches
     * re-enters s->mu, so this cannot deadlock with the app thread. */
    pthread_mutex_lock(&s->mu);
    if (s->ready && !s->fatal)
        sender_drain(s, now_us);
    pthread_mutex_unlock(&s->mu);

    /* MSF §11.3 step 1: finish converting tracks' live subscribers (Track Ended)
     * BEFORE the conversion catalog publishes. Strict ordering -- the done is
     * sent first, then the independent conversion catalog. A still-pending finish
     * gates the republish and preserves catalog_dirty for the next pump, so the
     * conversion catalog is never emitted early. */
    bool conv_done = true;
    if (s->ready && !s->fatal)
        conv_done = sender_finish_conversions(s, now_us);

    /* Coalesced independent catalog republish for post-ready add/remove/convert. */
    if (s->ready && !s->fatal && conv_done)
        sender_republish_catalog(s, now_us);

    (void)moq_pub_tick(s->pub, now_us);

    /* Reconcile demand AFTER the tick + the teardown/finish work above, so the
     * mirror and app callbacks reflect subscriptions cleared without a left
     * event (session close / end / VOD finish). */
    sender_resync_demand(s);

    /* Cache the next managed-adapter wake deadline from the now-settled state
     * (facade access is legal on this path); sender_next_deadline_us returns it
     * as a pure scalar so an idle managed loop wakes to fire the refresh. */
    pthread_mutex_lock(&s->mu);
    sender_recompute_wake_deadline(s);
    pthread_mutex_unlock(&s->mu);
}


/* Publisher teardown posted to the network thread (ctx = publisher alone;
 * the sender may be freed before the task runs). */
moq_result_t sender_destroy_pub_task(moq_endpoint_t *ep,
                                     moq_session_t *session,
                                     uint64_t now_us, void *ctx)
{
    (void)ep; (void)session; (void)now_us;
    moq_pub_destroy((moq_publisher_t *)ctx);
    return MOQ_OK;
}

#ifdef MOQ_MEDIA_SENDER_TESTING


void moq_media_sender_test_auth_after_select(
    void (*fn)(moq_media_sender_t *, moq_media_track_t *, void *), void *ctx);

void moq_media_sender_test_auth_after_select(
    void (*fn)(moq_media_sender_t *, moq_media_track_t *, void *), void *ctx)
{
    sender_test_after_select = fn;
    sender_test_after_select_ctx = ctx;
}

#endif
