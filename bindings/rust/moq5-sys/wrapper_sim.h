/* The simulator tier (`sim` feature): the deterministic two-peer harness,
 * generated apart from the core. Everything it references from the core is
 * in the core bindings. As in wrapper_core.h, the values bindgen cannot
 * evaluate are restated as MOQ5_SYSVAL_ statics whose prefix the build
 * script strips. */

#include <moq/sim.h>

#define MOQ5_SYSVAL(type, name) static const type MOQ5_SYSVAL_##name = name

/* sim.h */
MOQ5_SYSVAL(size_t, MOQ_SIM_TRACE_RECORD_V0_SIZE);

#undef MOQ5_SYSVAL
