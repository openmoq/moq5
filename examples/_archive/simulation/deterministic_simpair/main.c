/*
 * ASPIRATIONAL — does not compile. This is the API we wish existed.
 *
 * Goal: two sessions connected back-to-back in a deterministic
 * simulation. Publisher sends objects, subscriber receives them.
 * Zero sockets, zero threads, runs in microseconds.
 */

#include <moq/moq.h>
#include <moq/sim.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>

int main(void)
{
    /* ── 1. Create a SimPair ───────────────────────────────────── */

    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.seed      = 42;
    cfg.role_a    = MOQ_ROLE_SUBSCRIBER;
    cfg.role_b    = MOQ_ROLE_PUBLISHER;

    moq_simpair_t *sp = moq_simpair_new(&cfg);
    assert(sp);

    moq_session_t *sub = moq_simpair_session_a(sp);
    moq_session_t *pub = moq_simpair_session_b(sp);

    /* ── 2. Drive handshake ────────────────────────────────────── */

    moq_simpair_run(sp, 100);

    assert(moq_session_state(sub) == MOQ_SESS_ESTABLISHED);
    assert(moq_session_state(pub) == MOQ_SESS_ESTABLISHED);

    /* ── 3. Subscribe ──────────────────────────────────────────── */

    moq_bytes_t ns_parts[] = { moq_bytes_from_str("test") };
    moq_namespace_t ns = { .parts = ns_parts, .count = 1 };

    moq_subscribe_params_t sub_params = MOQ_SUBSCRIBE_PARAMS_INIT;
    sub_params.ns         = ns;
    sub_params.track_name = moq_bytes_from_str("data");

    moq_subscription_t sub_handle;
    moq_session_subscribe(sub, &sub_params, moq_simpair_now_us(sp), &sub_handle);

    moq_simpair_run(sp, 100);

    /* ── 4. Publisher accepts ──────────────────────────────────── */

    moq_subscription_t pub_sub = MOQ_SUBSCRIPTION_INVALID;
    {
        moq_event_t evts[8];
        size_t n;
        while ((n = moq_session_poll_events(pub, evts, 8)) > 0) {
            for (size_t i = 0; i < n; i++) {
                if (evts[i].kind == MOQ_EVENT_INCOMING_SUBSCRIBE) {
                    const moq_incoming_subscribe_event_t *isub =
                        &evts[i].u.incoming_subscribe;
                    pub_sub = isub->subscription;

                    moq_subscribe_ok_params_t ok = MOQ_SUBSCRIBE_OK_PARAMS_INIT;
                    moq_session_subscribe_ok(pub, pub_sub, &ok,
                                            moq_simpair_now_us(sp));
                }
            }
        }
    }
    assert(moq_subscription_is_valid(pub_sub));

    moq_simpair_run(sp, 100);

    /* ── 5. Publish an object ──────────────────────────────────── */

    moq_object_params_t obj = MOQ_OBJECT_PARAMS_INIT;
    obj.group_id    = 0;
    obj.object_id   = 0;
    obj.priority    = 128;
    obj.payload     = (const uint8_t *)"hello-sim";
    obj.payload_len = 9;

    moq_session_publish_object(pub, pub_sub, &obj, moq_simpair_now_us(sp));

    /* ── 6. Drive until subscriber receives ────────────────────── */

    moq_simpair_run(sp, 100);

    moq_event_t evts[16];
    size_t n;
    bool got_object = false;
    while ((n = moq_session_poll_events(sub, evts, 16)) > 0) {
        for (size_t i = 0; i < n; i++) {
            if (evts[i].kind == MOQ_EVENT_OBJECT) {
                const moq_object_event_t *o = &evts[i].u.object;
                assert(o->group_id == 0);
                assert(o->object_id == 0);
                assert(o->payload_len == 9);
                assert(memcmp(o->payload, "hello-sim", 9) == 0);
                got_object = true;
            }
        }
    }
    assert(got_object);

    /* ── 7. Cleanup ────────────────────────────────────────────── */

    moq_simpair_destroy(sp);

    printf("PASS: deterministic SimPair test (seed=42)\n");
    return 0;
}
