/* A program that needs the simulator and, through it, the core. Built as C
 * and as C++ from `pkg-config --cflags --libs libmoq-sim`. Silent on
 * success. */
#include <stdio.h>

#include <moq/sim.h>

#define CHECK(expr) do { if (!(expr)) { \
    fprintf(stderr, "tier consumer sim:%d: %s\n", __LINE__, #expr); \
    return 1; \
} } while (0)

int main(void)
{
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    moq_simpair_t *pair = NULL;

    cfg.alloc = moq_alloc_default();
    cfg.seed = 1;
    cfg.initial_now_us = 1000;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 10;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = 10;

    /* A setup handshake drives the pair and its pump, and both sessions:
     * the simulator's objects and the core closure behind them must link. */
    CHECK(moq_simpair_create(&cfg, &pair) == MOQ_OK);
    CHECK(pair != NULL);
    CHECK(moq_simpair_start(pair) == MOQ_OK);
    CHECK(moq_simpair_run_until_quiescent(pair, 8, NULL) == MOQ_OK);
    CHECK(moq_session_state(moq_simpair_client(pair)) == MOQ_SESS_ESTABLISHED);
    CHECK(moq_session_state(moq_simpair_server(pair)) == MOQ_SESS_ESTABLISHED);
    moq_simpair_destroy(pair);
    return 0;
}
