/*
 * The managed picoquic facade presents the configured authorization tokens in
 * each session's SETUP (moq_pq_threaded_cfg_t.setup_auth_tokens, copied at
 * create): over a real loopback connection the server's SETUP_COMPLETE
 * carries them, by value; a client configured without tokens sends none.
 * A token set whose SETUP cannot fit the session send arena is refused at
 * create; one just inside it arrives whole, on the legacy single ALPN and on
 * negotiated draft-18 and draft-16. The facade's copy is wiped when it is
 * released (the core and service tests pin that); token values here hold no
 * zero byte, which a receiving session on this profile set refuses.
 */
#include <moq/picoquic_threaded.h>
#include <moq/types.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int failures = 0;
#define CHECK(e) do { if (!(e)) { fprintf(stderr, "FAIL: %s:%d: %s\n", \
    __FILE__, __LINE__, #e); failures++; } } while (0)

typedef struct {
    int      setups;          /* atomic */
    size_t   token_count;
    uint64_t type0;
    uint8_t  value0[8];
    size_t   len0;
    bool     pattern0;        /* the whole first value matches big_byte() */
} srv_ctx_t;

static uint8_t big_byte(size_t i) { return (uint8_t)((i * 37u + 5u) % 255u + 1u); }

static int server_pump(moq_pq_threaded_t *t, moq_pq_threaded_lane_t *lane,
                       uint64_t now, void *u)
{
    (void)t; (void)now;
    srv_ctx_t *c = (srv_ctx_t *)u;
    for (moq_pq_threaded_conn_t *cn = moq_pq_threaded_lane_next_conn(lane, NULL); cn;
         cn = moq_pq_threaded_lane_next_conn(lane, cn)) {
        moq_session_t *s = moq_pq_threaded_conn_session(cn);
        if (!s) continue;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SETUP_COMPLETE) {
                c->token_count = ev.u.setup_complete.token_count;
                if (c->token_count > 0) {
                    const moq_resolved_token_t *k = ev.u.setup_complete.tokens;
                    c->type0 = k[0].token_type;
                    c->len0 = k[0].token_value.len < sizeof(c->value0)
                        ? k[0].token_value.len : sizeof(c->value0);
                    if (c->len0) memcpy(c->value0, k[0].token_value.data, c->len0);
                    c->pattern0 = true;
                    for (size_t i = 0; i < k[0].token_value.len; i++)
                        if (k[0].token_value.data[i] != big_byte(i)) {
                            c->pattern0 = false;
                            break;
                        }
                }
                __atomic_add_fetch(&c->setups, 1, __ATOMIC_ACQ_REL);
            }
            moq_event_cleanup(&ev);
        }
    }
    return 0;
}

static int client_pump(moq_pq_threaded_t *t, moq_pq_threaded_lane_t *lane,
                       uint64_t now, void *u)
{
    (void)t; (void)lane; (void)now; (void)u;
    return 0;
}

static void run_client(int port, srv_ctx_t *sc, const moq_auth_token_t *tokens,
                       size_t count, const char *const *alpn)
{
    int before = __atomic_load_n(&sc->setups, __ATOMIC_ACQUIRE);
    moq_pq_threaded_cfg_t cfg;
    moq_pq_threaded_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.host = "127.0.0.1";
    cfg.port = port;
    cfg.insecure_skip_verify = true;
    cfg.on_lane_pump = client_pump;
    cfg.setup_auth_tokens = tokens;
    cfg.setup_auth_token_count = count;
    if (alpn) { cfg.alpn_list = alpn; cfg.alpn_count = 1; }
    moq_pq_threaded_t *cli = NULL;
    CHECK(moq_pq_threaded_create(&cfg, &cli) == MOQ_OK);
    if (!cli) return;
    for (int i = 0; i < 400; i++) {
        if (__atomic_load_n(&sc->setups, __ATOMIC_ACQUIRE) > before) break;
        if (moq_pq_threaded_is_fatal(cli)) break;
        moq_pq_threaded_wait(cli, 25000);
    }
    if (__atomic_load_n(&sc->setups, __ATOMIC_ACQUIRE) != before + 1)
        fprintf(stderr, "client: fatal=%d code=0x%llx terminal=0x%llx version=0x%x\n",
                (int)moq_pq_threaded_is_fatal(cli),
                (unsigned long long)moq_pq_threaded_fatal_code(cli),
                (unsigned long long)moq_pq_threaded_terminal_error(cli),
                (unsigned)moq_pq_threaded_negotiated_version(cli));
    CHECK(__atomic_load_n(&sc->setups, __ATOMIC_ACQUIRE) == before + 1);
    moq_pq_threaded_stop(cli);
    moq_pq_threaded_destroy(cli);
}

int main(void)
{
    srand((unsigned)getpid());
    int port = 16300 + (rand() % 300);
    srv_ctx_t sc;
    memset(&sc, 0, sizeof(sc));
    moq_pq_threaded_cfg_t srv;
    moq_pq_threaded_cfg_init_sized(&srv, sizeof(srv));
    srv.alloc = moq_alloc_default();
    srv.perspective = MOQ_PERSPECTIVE_SERVER;
    srv.cert_path = MOQ_TEST_CERT_PATH;
    srv.key_path = MOQ_TEST_KEY_PATH;
    srv.port = port;
    srv.on_lane_pump = server_pump;
    srv.on_lane_pump_ctx = &sc;
    static const char *const offer[2] = { "moqt-18", "moqt-16" };
    srv.alpn_list = offer;
    srv.alpn_count = 2;
    moq_pq_threaded_t *server = NULL;
    CHECK(moq_pq_threaded_create(&srv, &server) == MOQ_OK);

    /* Refused at create: a value the default 4096-byte arena cannot hold,
     * and two that fit alone but not together. */
    static uint8_t big[4096];
    for (size_t i = 0; i < sizeof(big); i++) big[i] = big_byte(i);
    {
        moq_pq_threaded_cfg_t c;
        moq_pq_threaded_cfg_init_sized(&c, sizeof(c));
        c.alloc = moq_alloc_default();
        c.perspective = MOQ_PERSPECTIVE_CLIENT;
        c.host = "127.0.0.1";
        c.port = port;
        c.insecure_skip_verify = true;
        c.on_lane_pump = client_pump;
        moq_auth_token_t two[2] = { { 1, { big, sizeof(big) } }, { 2, { big, 2000 } } };
        c.setup_auth_tokens = two;
        c.setup_auth_token_count = 1;
        moq_pq_threaded_t *cli = NULL;
        CHECK(moq_pq_threaded_create(&c, &cli) == MOQ_ERR_INVAL && !cli);
        two[0].token_value.len = 2000;
        c.setup_auth_token_count = 2;
        CHECK(moq_pq_threaded_create(&c, &cli) == MOQ_ERR_INVAL && !cli);
        /* A larger configured arena holds what the default does not. */
        two[0].token_value.len = sizeof(big);
        c.setup_auth_token_count = 1;
        c.send_buffer_size = 8192;
        CHECK(moq_pq_threaded_create(&c, &cli) == MOQ_OK && cli);
        if (cli) { moq_pq_threaded_stop(cli); moq_pq_threaded_destroy(cli); }
    }

    if (server) {
        uint8_t value[] = { 0xde, 0xad, 0xbe };
        moq_auth_token_t tok = { 11, { value, sizeof(value) } };
        run_client(port, &sc, &tok, 1, NULL);
        CHECK(sc.token_count == 1);
        CHECK(sc.type0 == 11 && sc.len0 == 3 && memcmp(sc.value0, value, 3) == 0);
        run_client(port, &sc, NULL, 0, NULL);
        CHECK(sc.token_count == 0);
        /* Just inside the default arena: whole, on the legacy ALPN and on
         * negotiated draft-18 and draft-16. */
        moq_auth_token_t near = { 12, { big, 3800 } };
        const char *const *alpns[3] = { NULL, offer, offer + 1 };
        moq_auth_token_t small = { 12, { big, 16 } };
        for (int i = 0; i < 6; i++) {
            memset(&sc.token_count, 0, sizeof(sc) - offsetof(srv_ctx_t, token_count));
            run_client(port, &sc, i < 3 ? &small : &near, 1, alpns[i % 3]);
            if (sc.token_count != 1 || sc.type0 != 12 || !sc.pattern0)
                fprintf(stderr, "%s token, offer %s\n", i < 3 ? "small" : "near-limit",
                        alpns[i % 3] ? alpns[i % 3][0] : "legacy");
            CHECK(sc.token_count == 1);
            CHECK(sc.type0 == 12 && sc.pattern0);
        }
        moq_pq_threaded_stop(server);
        moq_pq_threaded_destroy(server);
    }
    if (failures) {
        fprintf(stderr, "threaded_setup_tokens: %d failure(s)\n", failures);
        return 1;
    }
    printf("threaded_setup_tokens: SETUP tokens reach the server\n");
    return 0;
}
