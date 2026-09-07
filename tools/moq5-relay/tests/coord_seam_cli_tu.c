/* The production CLI translation unit, compiled into the coordinator test with
 * its entry point renamed away. The coordinators under test are the exact
 * production objects, not a re-implementation. */
#define main moqr_cli_disabled_main
#include "../cli/main.c"
#undef main
