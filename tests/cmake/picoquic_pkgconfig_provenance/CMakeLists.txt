# Driver for PicoquicPkgConfigProvenanceTest.cmake: the pkg-config include
# and library flags cmake/MoqPicoquicPkgConfig.cmake derives for each
# provider topology, on synthetic imported targets (no compiler). ROOT is a
# fresh directory the test owns; it creates the provider trees first.
cmake_minimum_required(VERSION 3.20)
project(picoquic_pkgconfig_provenance NONE)
include("${MOQ_SOURCE_DIR}/cmake/MoqPicoquicPkgConfig.cmake")

function(expect what got want)
    if(NOT "${got}" STREQUAL "${want}")
        message(FATAL_ERROR "${what}: got [${got}], want [${want}]")
    endif()
    message(STATUS "ok ${what}: [${got}]")
endfunction()

# The picoquic library flags alone: the link closure minus what follows the
# picoquic archives (picotls, TLS provider, pthread).
function(pq_libs out_var)
    moq_picoquic_pkgconfig_libs(_all)
    string(REGEX REPLACE "(^| )-lpicoquic-core.*$" "\\1-lpicoquic-core" _pq "${_all}")
    set(${out_var} "${_pq}" PARENT_SCOPE)
endfunction()

function(imported_provider inc lib)
    if(NOT TARGET picoquic::picoquic-core)
        add_library(picoquic::picoquic-core STATIC IMPORTED)
    endif()
    set_target_properties(picoquic::picoquic-core PROPERTIES
        INTERFACE_INCLUDE_DIRECTORIES "${inc}"
        IMPORTED_LOCATION "${lib}/libpicoquic-core.a")
endfunction()

set(CMAKE_INSTALL_LIBDIR lib)

# 1. Installed inside the prefix, in its own subtree: both roots named
#    under ${prefix}.
set(CMAKE_INSTALL_PREFIX "${ROOT}/prefix")
set(CMAKE_INSTALL_INCLUDEDIR sdk/include)
imported_provider("${ROOT}/prefix/deps/include" "${ROOT}/prefix/deps/lib")
moq_picoquic_pkgconfig_cflags(c)
pq_libs(l)
expect("nested cflags" "${c}" "-I\${prefix}/deps/include")
expect("nested libs" "${l}" "-L\${prefix}/deps/lib -lpicoquic-core")

# 2. Installed into LibMoQ's own include and lib directories: nothing more.
set(CMAKE_INSTALL_INCLUDEDIR include)
imported_provider("${ROOT}/prefix/include" "${ROOT}/prefix/lib")
moq_picoquic_pkgconfig_cflags(c)
pq_libs(l)
expect("shared dirs cflags" "${c}" "")
expect("shared dirs libs" "${l}" "-lpicoquic-core")

# 3. Same prefix, LibMoQ's headers moved: picoquic's include/ still named.
set(CMAKE_INSTALL_INCLUDEDIR sdk/include)
moq_picoquic_pkgconfig_cflags(c)
expect("own includedir moved" "${c}" "-I\${prefix}/include")

# 4. An external install (the prefix's parent): fixed absolute paths.
file(REAL_PATH "${ROOT}" real_root)
imported_provider("${ROOT}/ext/include" "${ROOT}/ext/lib")
moq_picoquic_pkgconfig_cflags(c)
pq_libs(l)
expect("external cflags" "${c}" "-I${real_root}/ext/include")
expect("external libs" "${l}" "-L${real_root}/ext/lib -lpicoquic-core")

# 5. A future prefix that does not exist yet: the same answers, and no
#    diagnostics (the test runs this configure with developer warnings as
#    errors).
set(CMAKE_INSTALL_PREFIX "${ROOT}/not-yet/prefix")
moq_picoquic_pkgconfig_cflags(c)
pq_libs(l)
expect("future prefix cflags" "${c}" "-I${real_root}/ext/include")
expect("future prefix libs" "${l}" "-L${real_root}/ext/lib -lpicoquic-core")
set_target_properties(picoquic::picoquic-core PROPERTIES IMPORTED_LOCATION "")
