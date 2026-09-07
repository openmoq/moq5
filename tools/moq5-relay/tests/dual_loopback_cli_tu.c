/* The production CLI translation unit, compiled into the dual-loopback test
 * with its entry point renamed away — the test provides main. Including the .c
 * keeps this target-scoped: the shipping moq5-relay binaries are unchanged, and
 * the lane pumps the test drives are the exact production objects. */
#define main moqr_cli_disabled_main
#include "../cli/main.c"
#undef main
