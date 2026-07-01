/*
 * Goal: demonstrate a complete client/server setup handshake using
 * only the public session API and byte buffers. No QUIC stack, no
 * sockets, no threads. This is exactly how SimPair will work.
 */

#include <moq/moq.h>
#include <stdio.h>
#include <stdlib.h>

/* -- Minimal allocator wrapping libc ------------------------------- */

static void *ex_alloc(size_t size, void *ctx) { (void)ctx; return malloc(size); }
static void *ex_realloc(void *p, size_t old, size_t new_sz, void *ctx)
    { (void)old; (void)ctx; return realloc(p, new_sz); }
static void ex_free(void *p, size_t size, void *ctx)
    { (void)size; (void)ctx; free(p); }

static const moq_alloc_t g_alloc = { NULL, ex_alloc, ex_realloc, ex_free };

/* -- Pump actions from one session to the other -------------------- */

static void pump(moq_session_t *from, moq_session_t *to, uint64_t now)
{
    moq_action_t acts[16];
    size_t n;
    while ((n = moq_session_poll_actions(from, acts, 16)) > 0) {
        for (size_t i = 0; i < n; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL) {
                moq_session_on_control_bytes(to,
                    acts[i].u.send_control.data,
                    acts[i].u.send_control.len,
                    now);
            }
            moq_action_cleanup(&acts[i]);
        }
    }
}

static void print_events(const char *name, moq_session_t *s)
{
    moq_event_t evts[4];
    size_t ne = moq_session_poll_events(s, evts, 4);
    for (size_t i = 0; i < ne; i++) {
        if (evts[i].kind == MOQ_EVENT_SETUP_COMPLETE)
            printf("%s: setup complete\n", name);
        moq_event_cleanup(&evts[i]);
    }
}

int main(void)
{
    int rc = 1;

    moq_session_cfg_t client_cfg = MOQ_SESSION_CFG_INIT;
    client_cfg.alloc       = &g_alloc;
    client_cfg.perspective = MOQ_PERSPECTIVE_CLIENT;

    moq_session_cfg_t server_cfg = MOQ_SESSION_CFG_INIT;
    server_cfg.alloc       = &g_alloc;
    server_cfg.perspective = MOQ_PERSPECTIVE_SERVER;

    moq_session_t *client = NULL;
    moq_session_t *server = NULL;

    if (moq_session_create(&client_cfg, 0, &client) < 0) goto done;
    if (moq_session_create(&server_cfg, 0, &server) < 0) goto done;

    /* Client starts: initiates setup handshake.
     * Server is ready immediately after create — no start needed. */
    if (moq_session_start(client, 1000) < 0) goto done;
    printf("client: SETUP_SENT\n");

    pump(client, server, 1000);
    printf("server: %s\n",
           moq_session_state(server) == MOQ_SESS_ESTABLISHED
               ? "ESTABLISHED" : "NOT ESTABLISHED");
    print_events("server", server);

    pump(server, client, 1000);
    printf("client: %s\n",
           moq_session_state(client) == MOQ_SESS_ESTABLISHED
               ? "ESTABLISHED" : "NOT ESTABLISHED");
    print_events("client", client);

    rc = 0;

done:
    moq_session_destroy(client);
    moq_session_destroy(server);
    printf("%s\n", rc == 0 ? "done" : "failed");
    return rc;
}
