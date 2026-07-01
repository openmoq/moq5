/*
 * ASPIRATIONAL — does not compile. This is the API we wish existed.
 *
 * Goal: publish a track, wait for a subscriber, send objects in a loop,
 * then shut down cleanly.
 */

#include <moq/moq.h>
#include <stdio.h>
#include <string.h>

int main(void)
{
    /* ── 1. Configure ──────────────────────────────────────────── */

    moq_config_t *cfg;
    moq_config_create(NULL, &cfg);
    moq_config_set_role(cfg, MOQ_ROLE_PUBLISHER);

    /* ── 2. Create session ─────────────────────────────────────── */

    moq_session_t *s;
    moq_session_create(cfg, 0, &s);
    moq_config_destroy(cfg);

    /* ── 3. Announce namespace ─────────────────────────────────── */

    moq_bytes_t ns_parts[] = {
        moq_bytes_from_str("chat.example.com"),
        moq_bytes_from_str("room42"),
    };
    moq_namespace_t ns = { .parts = ns_parts, .count = 2 };

    moq_announcement_t ann;
    moq_result_t rc = moq_session_announce_namespace(s, &ns, 0 /* now_us */, &ann);
    if (rc != MOQ_OK) {
        fprintf(stderr, "announce failed: %s\n", moq_strerror(rc));
        moq_session_destroy(s);
        return 1;
    }

    /* ── 4. Event loop ─────────────────────────────────────────── */

    uint64_t now = 0;
    uint64_t group_id = 0;
    uint64_t object_id = 0;
    moq_subscription_t active_sub = MOQ_SUBSCRIPTION_INVALID;
    bool publishing = false;
    bool done = false;

    while (!done) {
        now += 33000; /* ~30fps in microseconds */

        moq_event_t evts[16];
        size_t n;
        while ((n = moq_session_poll_events(s, evts, 16)) > 0) {
            for (size_t i = 0; i < n; i++) {
                switch (evts[i].kind) {
                case MOQ_EVENT_SETUP_COMPLETE:
                    printf("session ready\n");
                    break;

                case MOQ_EVENT_INCOMING_SUBSCRIBE: {
                    /*
                     * A subscriber wants our track. Accept with subscribe_ok.
                     *
                     * The typed handle is in the per-event-kind struct.
                     * Track alias is auto-assigned by the library.
                     */
                    const moq_incoming_subscribe_event_t *isub =
                        &evts[i].u.incoming_subscribe;

                    moq_subscribe_ok_params_t ok = MOQ_SUBSCRIBE_OK_PARAMS_INIT;
                    /* track_alias defaults to MOQ_TRACK_ALIAS_AUTO */

                    rc = moq_session_subscribe_ok(s, isub->subscription, &ok, now);
                    if (rc == MOQ_OK) {
                        printf("accepted subscriber\n");
                        active_sub = isub->subscription;
                        publishing = true;
                    }
                    break;
                }

                case MOQ_EVENT_SUBSCRIPTION_DONE:
                    printf("subscriber left\n");
                    publishing = false;
                    done = true;
                    break;

                case MOQ_EVENT_SESSION_CLOSED:
                    done = true;
                    break;

                default:
                    break;
                }
            }
        }

        /* ── 5. Publish objects ─────────────────────────────────── */

        if (publishing && moq_subscription_is_valid(active_sub)) {
            char payload[64];
            int len = snprintf(payload, sizeof(payload),
                               "msg #%llu", (unsigned long long)object_id);

            moq_object_params_t obj = MOQ_OBJECT_PARAMS_INIT;
            obj.group_id    = group_id;
            obj.object_id   = object_id;
            obj.priority    = 128;
            obj.payload     = (const uint8_t *)payload;
            obj.payload_len = (size_t)len;

            /*
             * publish_object copies the payload immediately.
             * After this call, 'payload' can be reused/freed.
             *
             * Future convenience: moq_session_group_begin/push/end
             * will auto-manage group_id/object_id. Raw IDs stay
             * available for relays, fuzzers, and protocol tools.
             */
            rc = moq_session_publish_object(s, active_sub, &obj, now);
            if (rc != MOQ_OK) {
                fprintf(stderr, "publish failed: %s\n", moq_strerror(rc));
            }

            object_id++;
            if (object_id % 30 == 0) {
                group_id++;
                object_id = 0;
            }
        }

        /* Drain actions (adapter would send these to QUIC) */
        moq_action_t acts[64];
        while (moq_session_poll_actions(s, acts, 64) > 0) {
            /* adapter dispatches each action to the QUIC stack */
        }
    }

    /* ── 6. Shutdown ───────────────────────────────────────────── */

    /* Three distinct operations:
     *   goaway   — protocol-level graceful drain (GOAWAY message)
     *   close    — protocol-level immediate close (CONNECTION_CLOSE)
     *   destroy  — local memory cleanup
     * In this simple example, destroy handles everything. */
    moq_session_destroy(s);
    return 0;
}
