# Dependency-local policy for the picotls LibMoQ links, applied inside
# picotls's project() by scripts/setup_picoquic_deps.sh (prebuilt picotls) and
# by MoqPicotlsProjectPolicy.cmake (picotls that picoquic builds itself).
# Directory compile options follow CMAKE_C_FLAGS and configuration flags,
# preserving caller flags while keeping picotls invariants active even when
# Release defines NDEBUG.
if(MSVC)
    add_compile_options(/WX /UNDEBUG)
else()
    add_compile_options(-Werror -UNDEBUG)
endif()
