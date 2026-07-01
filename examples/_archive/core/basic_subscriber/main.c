/*
 * ASPIRATIONAL — does not compile. This is the API we wish existed.
 *
 * Goal: subscribe to a single track on a known server, receive and
 * print objects until the track ends, then shut down cleanly.
 *
 * This is the "hello world" of libmoq. If this feels bad, everything
 * built on top will too.
 */

#include <moq/moq.h>
#include <stdio.h>
#include <string.h>

/*
 * Lifetime categories:
 *
 *   "Advancing" calls may invalidate borrowed pointers from prior
 *   events/actions: on_stream_bytes, on_timer, subscribe, publish_object,
 *   subscribe_ok, subscribe_error, unsubscribe, announce_namespace,
 *   bind_stream, close, goaway, destroy, etc.
 *
 *   "Observing" calls never invalidate borrows: poll_events, poll_actions,
 *   next_deadline_us, session_state.
 *
 *   Within a single poll_events batch, all returned pointers are stable
 *   until the next advancing call.
 */

static void on_object(const moq_object_event_t *obj)
{
    printf("object group=%llu obj=%llu len=%zu\n",
           (unsigned long long)obj->group_id,
           (unsigned long long)obj->object_id,
           obj->payload_len);

    if (obj->object_status == MOQ_OBJECT_STATUS_END_OF_TRACK) {
        printf("track ended\n");
    }
}

int main(void)
{
    /* ── 1. Configure ──────────────────────────────────────────── */

    moq_config_t *cfg;
    moq_result_t rc = moq_config_create(NULL /* default libc allocator */, &cfg);
    if (rc != MOQ_OK) return 1;

    moq_config_set_role(cfg, MOQ_ROLE_SUBSCRIBER);

    /* ── 2. Create session ─────────────────────────────────────── */

    moq_session_t *s;
    rc = moq_session_create(cfg, 0 /* now_us */, &s);
    moq_config_destroy(cfg); /* config is copied into session; safe to destroy */
    if (rc != MOQ_OK) {
        fprintf(stderr, "session create failed: %s\n", moq_strerror(rc));
        return 1;
    }

    /* ── 3. Subscribe ──────────────────────────────────────────── */

    /*
     * Namespace is a tuple of byte spans. Parts are {data, len} pairs.
     * Byte-safe: parts are not required to be UTF-8 or NUL-terminated.
     */
    moq_bytes_t ns_parts[] = {
        moq_bytes_from_str("chat.example.com"),
        moq_bytes_from_str("room42"),
    };
    moq_namespace_t ns = { .parts = ns_parts, .count = 2 };

    moq_subscribe_params_t sub_params = MOQ_SUBSCRIBE_PARAMS_INIT;
    sub_params.ns          = ns;
    sub_params.track_name  = moq_bytes_from_str("messages");
    sub_params.filter_type = MOQ_FILTER_LATEST_GROUP;

    moq_subscription_t sub;
    rc = moq_session_subscribe(s, &sub_params, 0 /* now_us */, &sub);
    if (rc != MOQ_OK) {
        fprintf(stderr, "subscribe failed: %s\n", moq_strerror(rc));
        moq_session_destroy(s);
        return 1;
    }

    /* ── 4. Event loop (adapter drives this — simplified here) ── */

    bool done = false;
    while (!done) {
        /*
         * In real usage, a QUIC adapter feeds stream bytes in and
         * drains actions out. Here we just show the event side.
         * See examples/core/quic_adapter_minimal for the full loop.
         */

        moq_event_t evts[16];
        size_t n;
        while ((n = moq_session_poll_events(s, evts, 16)) > 0) {
            for (size_t i = 0; i < n; i++) {
                switch (evts[i].kind) {
                case MOQ_EVENT_SETUP_COMPLETE:
                    printf("connected\n");
                    break;

                case MOQ_EVENT_SUBSCRIPTION_ESTABLISHED:
                    printf("subscribed (alias assigned by library)\n");
                    break;

                case MOQ_EVENT_OBJECT:
                    on_object(&evts[i].u.object);
                    break;

                case MOQ_EVENT_SUBSCRIPTION_DONE:
                    printf("subscription done: code=%d reason=%s\n",
                           evts[i].u.done.status_code,
                           evts[i].u.done.reason ? evts[i].u.done.reason : "(none)");
                    done = true;
                    break;

                case MOQ_EVENT_SESSION_CLOSED:
                    done = true;
                    break;

                default:
                    break; /* future event kinds */
                }
            }
        }
    }

    /* ── 5. Cleanup ────────────────────────────────────────────── */

    /* close sends protocol-level CONNECTION_CLOSE; destroy frees memory.
     * In this simple example we just destroy (implicit close). */
    moq_session_destroy(s);
    return 0;
}
