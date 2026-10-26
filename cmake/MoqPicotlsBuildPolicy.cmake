# Dependency-local policy for the picotls LibMoQ links, applied inside
# picotls's project() by scripts/setup_picoquic_deps.sh (prebuilt picotls) and
# by MoqPicotlsProjectPolicy.cmake (picotls that picoquic builds itself).
# Directory compile options follow CMAKE_C_FLAGS and configuration flags,
# preserving caller flags while keeping picotls invariants active even when
# Release defines NDEBUG.
if(MSVC)
    # picotls uses GCC-style warning flags that MSVC cannot build warning-free.
    add_compile_options(/UNDEBUG)
else()
    add_compile_options(-Werror -UNDEBUG)
endif()

if(WIN32)
    # picotls' CMake build has no Windows configuration; mirror what its Visual
    # Studio project (picotlsvs/) does: _WINDOWS selects wincompat.h, which lives
    # in picotlsvs/picotls together with the gettimeofday replacement that
    # picotls-core calls on Windows.
    add_compile_definitions(_WINDOWS)
    include_directories("${PROJECT_SOURCE_DIR}/picotlsvs/picotls")
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
