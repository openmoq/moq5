#include <moq/session.h>
#include <moq/control.h>
#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>

#define FUZZ_MAX_INPUT 4096

typedef struct {
    int64_t balance;
} fuzz_alloc_state_t;

static void *fuzz_alloc(size_t size, void *ctx)
{
    fuzz_alloc_state_t *s = (fuzz_alloc_state_t *)ctx;
    void *p = malloc(size);
    if (p) s->balance++;
    return p;
}

static void *fuzz_realloc(void *ptr, size_t old_size, size_t new_size, void *ctx)
{
    fuzz_alloc_state_t *s = (fuzz_alloc_state_t *)ctx;
    (void)old_size;
    if (new_size == 0) {
        if (ptr) { free(ptr); s->balance--; }
        return NULL;
    }
    if (!ptr) {
        void *p = malloc(new_size);
        if (p) s->balance++;
        return p;
    }
    return realloc(ptr, new_size);
}

static void fuzz_free(void *ptr, size_t size, void *ctx)
{
    fuzz_alloc_state_t *s = (fuzz_alloc_state_t *)ctx;
    (void)size;
    if (ptr) s->balance--;
    free(ptr);
}

static void drain_actions(moq_session_t *s)
{
    moq_action_t acts[16];
    size_t n;
    while ((n = moq_session_poll_actions(s, acts, 16)) > 0)
        for (size_t i = 0; i < n; i++)
            moq_action_cleanup(&acts[i]);
}

static void drain_events(moq_session_t *s)
{
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0)
        moq_event_cleanup(&ev);
}

static void pump_control(moq_session_t *from, moq_session_t *to)
{
    moq_action_t acts[16];
    size_t n;
    while ((n = moq_session_poll_actions(from, acts, 16)) > 0) {
        for (size_t i = 0; i < n; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                moq_session_on_control_bytes(to,
                    acts[i].u.send_control.data,
                    acts[i].u.send_control.len, 0);
            moq_action_cleanup(&acts[i]);
        }
    }
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    if (size < 1 || size > FUZZ_MAX_INPUT)
        return 0;

    fuzz_alloc_state_t as = { 0 };
    moq_alloc_t alloc = { &as, fuzz_alloc, fuzz_realloc, fuzz_free };

    moq_session_cfg_t ccfg = MOQ_SESSION_CFG_INIT;
    ccfg.alloc = &alloc;
    ccfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    ccfg.send_request_capacity = true;
    ccfg.initial_request_capacity = 8;
    ccfg.max_actions = 16;
    ccfg.max_events = 8;
    ccfg.max_subscriptions = 8;
    ccfg.max_data_streams = 8;
    ccfg.max_object_payload_size = 4096;
    ccfg.max_receive_buffer_bytes = 8192;
    ccfg.send_buffer_size = 2048;
    ccfg.recv_buffer_size = 2048;
    ccfg.output_scratch_size = 4096;

    moq_session_cfg_t scfg = ccfg;
    scfg.perspective = MOQ_PERSPECTIVE_SERVER;

    moq_session_t *client = NULL;
    moq_session_t *server = NULL;

    if (moq_session_create(&ccfg, 0, &client) < 0) goto done;
    if (moq_session_create(&scfg, 0, &server) < 0) goto done;

    moq_session_start(client, 0);
    pump_control(client, server);
    pump_control(server, client);
    drain_events(client);
    drain_events(server);

    {
        moq_bytes_t ns_parts[] = { { (const uint8_t *)"fuzz", 4 } };
        moq_namespace_t ns = { ns_parts, 1 };

        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = (moq_bytes_t){ (const uint8_t *)"track", 5 };
        sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;

        moq_subscription_t sub;
        if (moq_session_subscribe(client, &sub_cfg, 0, &sub) < 0) goto done;
        pump_control(client, server);

        moq_event_t ev;
        if (moq_session_poll_events(server, &ev, 1) != 1) goto done;
        if (ev.kind != MOQ_EVENT_SUBSCRIBE_REQUEST) {
            moq_event_cleanup(&ev);
            goto done;
        }
        moq_subscription_t server_sub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);

        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        if (moq_session_accept_subscribe(server, server_sub, &acc, 0) < 0)
            goto done;
        pump_control(server, client);

        if (moq_session_poll_events(client, &ev, 1) != 1) goto done;
        if (ev.kind != MOQ_EVENT_SUBSCRIBE_OK) {
            moq_event_cleanup(&ev);
            goto done;
        }
        moq_event_cleanup(&ev);
    }

    moq_session_destroy(server);
    server = NULL;

    {
        uint8_t ctrl = data[0];
        const uint8_t *payload = data + 1;
        size_t payload_len = size - 1;
        bool fin = (ctrl & 1);
        uint8_t split_mode = (ctrl >> 1) & 3;
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);

        switch (split_mode) {
        case 0:
            moq_session_on_data_bytes(client, ref,
                payload, payload_len, fin, 0);
            break;
        case 1: {
            size_t mid = payload_len / 2;
            if (mid > 0)
                moq_session_on_data_bytes(client, ref,
                    payload, mid, false, 0);
            moq_session_on_data_bytes(client, ref,
                payload + mid, payload_len - mid, fin, 0);
            break;
        }
        case 2:
            for (size_t i = 0; i < payload_len; i++) {
                bool last = (i == payload_len - 1);
                moq_session_on_data_bytes(client, ref,
                    payload + i, 1, last ? fin : false, 0);
            }
            if (payload_len == 0)
                moq_session_on_data_bytes(client, ref, NULL, 0, fin, 0);
            break;
        case 3:
            if (payload_len > 0)
                moq_session_on_data_bytes(client, ref,
                    payload, payload_len - 1, false, 0);
            moq_session_on_data_bytes(client, ref,
                payload_len > 0 ? payload + payload_len - 1 : NULL,
                payload_len > 0 ? 1 : 0, true, 0);
            break;
        }

        drain_actions(client);
        drain_events(client);
    }

done:
    moq_session_destroy(server);
    moq_session_destroy(client);

    if (as.balance != 0) abort();
    return 0;
}
