# Dependency-local policy for the picotls LibMoQ links, applied inside
# picotls's project() by scripts/setup_picoquic_deps.sh (prebuilt picotls) and
# by MoqPicotlsProjectPolicy.cmake (picotls that picoquic builds itself).
# picotls's targets get the options after CMAKE_C_FLAGS, the configuration
# flags and any options a caller's hook gave the directory, preserving caller
# flags while keeping picotls invariants active even when Release defines
# NDEBUG.
include("${CMAKE_CURRENT_LIST_DIR}/MoqDirectoryTargets.cmake")
# Brotli certificate compression stays off. picotls enables it when pkg-config
# finds libbrotli; picoquic does not use it, refuses a picotls it builds
# itself with it, and a found library would add a dependency nobody asked for.
set(WITH_BROTLI OFF CACHE BOOL "Build picotls with Brotli certificate compression" FORCE)
if(MSVC)
    # picotls uses GCC-style warning flags that MSVC cannot build warning-free.
    set(_moq_ptls_options /UNDEBUG)
else()
    set(_moq_ptls_options -Werror -UNDEBUG)
endif()

if(NOT WIN32)
    moq_directory_targets(COMPILE_OPTIONS ${_moq_ptls_options})
else()
    # picotls' CMake build has no Windows configuration; mirror what its Visual
    # Studio project (picotlsvs/) does: _WINDOWS selects wincompat.h, which lives
    # in picotlsvs/picotls together with the gettimeofday replacement that
    # picotls-core calls on Windows.
    moq_directory_targets(COMPILE_OPTIONS ${_moq_ptls_options}
        COMPILE_DEFINITIONS _WINDOWS
        INCLUDE_DIRECTORIES "${PROJECT_SOURCE_DIR}/picotlsvs/picotls")
    cmake_language(DEFER CALL target_sources picotls-core PRIVATE
        "${PROJECT_SOURCE_DIR}/picotlsvs/picotls/wintimeofday.c")

    # Multi-config generators (the Visual Studio default) would put archives in
    # build/<Config>/; keep them in the build root where FindPicoquic and
    # MOQ_PICOTLS_PREFIX expect them.
    foreach(_moq_ptls_cfg Debug Release RelWithDebInfo MinSizeRel)
        string(TOUPPER "${_moq_ptls_cfg}" _moq_ptls_cfg)
        set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY_${_moq_ptls_cfg} "${PROJECT_BINARY_DIR}")
    endforeach()
    unset(_moq_ptls_cfg)
endif()
unset(_moq_ptls_options)
