/*
 * ASPIRATIONAL — does not compile. This is the API we wish existed.
 *
 * Goal: a minimal relay that forwards objects between an upstream
 * publisher and a downstream subscriber. Two sessions, handle mapping,
 * namespace routing, object forwarding, unknown extension preservation.
 *
 * If this reads poorly, the core abstraction is probably wrong.
 *
 * Lifetime note: advancing on session A does not invalidate borrows
 * from session B. Within a single poll batch, all pointers are stable.
 */

#include <moq/moq.h>
#include <stdio.h>
#include <string.h>

typedef struct relay_track {
    moq_subscription_t down_sub;    /* downstream (relay is publisher) */
    moq_subscription_t up_sub;      /* upstream (relay is subscriber) */
    bool               up_established;
} relay_track_t;

#define MAX_TRACKS 64
static relay_track_t g_tracks[MAX_TRACKS];
static size_t        g_track_count = 0;

static relay_track_t *find_by_down(moq_subscription_t h)
{
    for (size_t i = 0; i < g_track_count; i++)
        if (moq_subscription_eq(g_tracks[i].down_sub, h))
            return &g_tracks[i];
    return NULL;
}

static relay_track_t *find_by_up(moq_subscription_t h)
{
    for (size_t i = 0; i < g_track_count; i++)
        if (moq_subscription_eq(g_tracks[i].up_sub, h))
            return &g_tracks[i];
    return NULL;
}

/* ── Process downstream events (subscriber → relay) ───────────────── */

static void process_downstream(moq_session_t *down,
                               moq_session_t *up,
                               uint64_t now)
{
    moq_event_t evts[32];
    size_t n;
    while ((n = moq_session_poll_events(down, evts, 32)) > 0) {
        for (size_t i = 0; i < n; i++) {
            switch (evts[i].kind) {

            case MOQ_EVENT_INCOMING_SUBSCRIBE: {
                const moq_incoming_subscribe_event_t *isub =
                    &evts[i].u.incoming_subscribe;

                relay_track_t *t = &g_tracks[g_track_count++];
                t->down_sub = isub->subscription;
                t->up_established = false;

                /*
                 * Subscribe upstream for the same track.
                 * Namespace and track_name are borrowed from the
                 * downstream session. Passing them to subscribe on the
                 * upstream session is safe: advancing on 'up' does not
                 * invalidate borrows from 'down'.
                 */
                moq_subscribe_params_t params = MOQ_SUBSCRIBE_PARAMS_INIT;
                params.ns         = isub->ns;
                params.track_name = isub->track_name;
                params.filter_type = MOQ_FILTER_LATEST_GROUP;

                moq_session_subscribe(up, &params, now, &t->up_sub);
                break;
            }

            default:
                break;
            }
        }
    }
}

/* ── Process upstream events (origin → relay) ─────────────────────── */

static void process_upstream(moq_session_t *up,
                             moq_session_t *down,
                             uint64_t now)
{
    moq_event_t evts[32];
    size_t n;
    while ((n = moq_session_poll_events(up, evts, 32)) > 0) {
        for (size_t i = 0; i < n; i++) {
            switch (evts[i].kind) {

            case MOQ_EVENT_SUBSCRIPTION_ESTABLISHED: {
                const moq_subscription_established_event_t *est =
                    &evts[i].u.established;

                relay_track_t *t = find_by_up(est->subscription);
                if (!t) break;
                t->up_established = true;

                moq_subscribe_ok_params_t ok = MOQ_SUBSCRIBE_OK_PARAMS_INIT;
                /* Track alias auto-assigned on downstream session. */
                /* Forward track extensions verbatim. */
                ok.track_extensions     = est->track_extensions;
                ok.track_extensions_len = est->track_extensions_len;

                moq_session_subscribe_ok(down, t->down_sub, &ok, now);
                break;
            }

            case MOQ_EVENT_OBJECT: {
                const moq_object_event_t *obj = &evts[i].u.object;

                relay_track_t *t = find_by_up(obj->subscription);
                if (!t) break;

                /*
                 * Forward object with extensions preserved.
                 * Payload and extensions are borrowed from upstream.
                 * publish_object copies immediately. Safe because
                 * publish_object advances 'down', not 'up'.
                 */
                moq_object_params_t fwd = MOQ_OBJECT_PARAMS_INIT;
                fwd.group_id       = obj->group_id;
                fwd.subgroup_id    = obj->subgroup_id;
                fwd.object_id      = obj->object_id;
                fwd.priority       = obj->publisher_priority;
                fwd.object_status  = obj->object_status;
                fwd.payload        = obj->payload;
                fwd.payload_len    = obj->payload_len;
                fwd.extensions     = obj->extensions;
                fwd.extensions_len = obj->extensions_len;

                moq_session_publish_object(down, t->down_sub, &fwd, now);
                break;
            }

            case MOQ_EVENT_SUBSCRIPTION_DONE: {
                const moq_subscription_done_event_t *done_ev =
                    &evts[i].u.done;

                relay_track_t *t = find_by_up(done_ev->subscription);
                if (!t) break;

                moq_session_publish_done(down, t->down_sub,
                                         done_ev->status_code,
                                         done_ev->reason,
                                         now);
                break;
            }

            case MOQ_EVENT_SUBSCRIPTION_ERROR: {
                const moq_subscription_error_event_t *err =
                    &evts[i].u.error;

                relay_track_t *t = find_by_up(err->subscription);
                if (!t) break;

                moq_session_subscribe_error(down, t->down_sub,
                                            err->error_code,
                                            err->reason,
                                            now);
                break;
            }

            default:
                break;
            }
        }
    }
}

/* ── Main relay loop ──────────────────────────────────────────────── */

int main(void)
{
    moq_config_t *up_cfg;
    moq_config_create(NULL, &up_cfg);
    moq_config_set_role(up_cfg, MOQ_ROLE_SUBSCRIBER);

    moq_config_t *down_cfg;
    moq_config_create(NULL, &down_cfg);
    moq_config_set_role(down_cfg, MOQ_ROLE_PUBLISHER);

    moq_session_t *up, *down;
    moq_session_create(up_cfg, 0, &up);
    moq_session_create(down_cfg, 0, &down);
    moq_config_destroy(up_cfg);
    moq_config_destroy(down_cfg);

    uint64_t now = 0;
    for (int tick = 0; tick < 10000; tick++) {
        now += 1000;

        process_downstream(down, up, now);
        process_upstream(up, down, now);

        moq_action_t acts[64];
        while (moq_session_poll_actions(up, acts, 64) > 0) { /* send */ }
        while (moq_session_poll_actions(down, acts, 64) > 0) { /* send */ }
    }

    /* Three-way lifecycle: goaway for graceful drain, close for
     * immediate protocol close, destroy for memory cleanup. */
    moq_session_destroy(up);
    moq_session_destroy(down);
    return 0;
}
