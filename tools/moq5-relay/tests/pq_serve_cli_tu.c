/* The production CLI translation unit, compiled into the PicoQUIC serve test
 * with its entry point renamed away -- the test provides main. The serve
 * composition the test runs is the exact production one, PicoQUIC facade
 * creation and its configuration included. */
#define main moqr_cli_disabled_main
#include "../cli/main.c"
#undef main

/* The serve context the shard runtime's capacity print accounts for. */
size_t moqr_test_serve_lanes_ctx_bytes(void)
{
    return sizeof(serve_lanes_ctx_t);
}
