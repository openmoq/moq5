# Resolve one existing path to its fully canonical form, INCLUDING a symlink in
# the final component, and print it.
#
# `pwd -P` on the parent directory plus `basename` leaves the final component's
# symlink unresolved, so two aliases of the same archive would compare unequal.
# `readlink -f` is not portable (BSD readlink has no -f), and a Python
# dependency is not assumed. The drivers already receive the exact CMake
# executable, so this is the portable resolver.
#
# Driven with -DMOQ_RP_IN=<path>.
if(NOT DEFINED MOQ_RP_IN OR MOQ_RP_IN STREQUAL "")
    message(FATAL_ERROR "moq_real_path: MOQ_RP_IN is required")
endif()
file(REAL_PATH "${MOQ_RP_IN}" _moq_rp_out)
message("${_moq_rp_out}")
