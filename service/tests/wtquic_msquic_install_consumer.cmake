# Installed-service packaging regression for a wtquic-MsQuic-enabled build,
# RELOCATABLE and SELF-CONTAINED: build+install a BUILD_HTTP picoquic into a
# dependency prefix, install libmoq into the same prefix, then consume the
# service tier from that INSTALLED prefix ONLY --
#   1. find_package(libmoq REQUIRED COMPONENTS service): the installed config
#      pulls the wtquic MsQuic managed component (and its attach base) and
#      resolves its picoquic/picohttp dependency from the installed prefix;
#   2. (when the .pc was generated) pkg-config --cflags --libs libmoq-service.
# The consumer discovers every dependency through CMAKE_PREFIX_PATH /
# PKG_CONFIG_PATH -- it is NEVER handed the producer's source checkout. A
# negative configure against a libmoq-only prefix (no installed picoquic) proves
# the dependency is genuinely required and discovered from the prefix, not
# silently found elsewhere or via a source fallback.
#
# Args (-D): BUILD (libmoq build dir), SRC (consumer dir with main.c), WORK
# (scratch), WTQUIC_PREFIX (wtquic install prefix), PICOQUIC_SOURCE_DIR + PICOTLS
# _PREFIX (used ONLY to materialize the installed picoquic, never handed to the
# consumer); optional MSQUIC_DIR_HINT, C_COMPILER, BUILD_TYPE, OPENSSL_ROOT,
# DO_PKGCONFIG (ON/OFF), LIBDIR (defaults to lib).

foreach(_v BUILD SRC WORK WTQUIC_PREFIX PICOQUIC_SOURCE_DIR PICOTLS_PREFIX)
    if(NOT DEFINED ${_v} OR "${${_v}}" STREQUAL "")
        message(FATAL_ERROR "pass -D${_v}=<path>")
    endif()
endforeach()
if(NOT DEFINED LIBDIR)
    set(LIBDIR lib)
endif()

set(_prefix "${WORK}/prefix")       # installed picoquic + libmoq (positive)
set(_bare "${WORK}/bare-prefix")    # installed libmoq only (negative)
set(_pqbuild "${WORK}/pq-build")
set(_cbuild "${WORK}/consumer-build")
set(_cbuild_neg "${WORK}/consumer-build-neg")
set(_csrc "${WORK}/consumer-src")
file(REMOVE_RECURSE "${_prefix}" "${_bare}" "${_pqbuild}"
                    "${_cbuild}" "${_cbuild_neg}" "${_csrc}")

# --- materialize an INSTALLED picoquic (BUILD_HTTP=ON) into the dep prefix -----
# picoquic installs neither picotls nor a picotls .pc, so bake the picotls search
# path in at picoquic-build time (CMAKE_LIBRARY_PATH); its exported targets then
# carry absolute picotls paths and the relocated consumer needs no picotls hint.
get_filename_component(_ptls_src "${PICOTLS_PREFIX}" DIRECTORY)
set(_pq_cfg
    -S "${PICOQUIC_SOURCE_DIR}" -B "${_pqbuild}"
    -DBUILD_HTTP=ON -DBUILD_DEMO=OFF -DBUILD_PQBENCH=OFF
    -DBUILD_PICO_SIM=OFF -Dpicoquic_BUILD_TESTS=OFF -DBUILD_LOGLIB=ON
    "-DCMAKE_INSTALL_PREFIX=${_prefix}"
    "-DPTLS_PREFIX=${_ptls_src}"
    "-DCMAKE_LIBRARY_PATH=${PICOTLS_PREFIX}")
if(DEFINED C_COMPILER AND NOT C_COMPILER STREQUAL "")
    list(APPEND _pq_cfg "-DCMAKE_C_COMPILER=${C_COMPILER}")
endif()
if(DEFINED BUILD_TYPE AND NOT BUILD_TYPE STREQUAL "")
    list(APPEND _pq_cfg "-DCMAKE_BUILD_TYPE=${BUILD_TYPE}")
endif()
if(DEFINED OPENSSL_ROOT AND NOT OPENSSL_ROOT STREQUAL "")
    list(APPEND _pq_cfg "-DOPENSSL_ROOT_DIR=${OPENSSL_ROOT}")
endif()
execute_process(COMMAND ${CMAKE_COMMAND} ${_pq_cfg}
    RESULT_VARIABLE _rc OUTPUT_VARIABLE _out ERROR_VARIABLE _out)
if(NOT _rc EQUAL 0)
    message(FATAL_ERROR "picoquic configure failed:\n${_out}")
endif()
execute_process(COMMAND ${CMAKE_COMMAND} --build "${_pqbuild}"
    RESULT_VARIABLE _rc OUTPUT_VARIABLE _out ERROR_VARIABLE _out)
if(NOT _rc EQUAL 0)
    message(FATAL_ERROR "picoquic build failed:\n${_out}")
endif()
execute_process(COMMAND ${CMAKE_COMMAND} --install "${_pqbuild}"
    RESULT_VARIABLE _rc OUTPUT_VARIABLE _out ERROR_VARIABLE _out)
if(NOT _rc EQUAL 0)
    message(FATAL_ERROR "picoquic install failed:\n${_out}")
endif()

# --- install libmoq into the dep prefix (positive) and a bare prefix (negative)
execute_process(
    COMMAND ${CMAKE_COMMAND} --install "${BUILD}" --prefix "${_prefix}"
    RESULT_VARIABLE _rc OUTPUT_VARIABLE _out ERROR_VARIABLE _out)
if(NOT _rc EQUAL 0)
    message(FATAL_ERROR "libmoq install (dep prefix) failed:\n${_out}")
endif()
execute_process(
    COMMAND ${CMAKE_COMMAND} --install "${BUILD}" --prefix "${_bare}"
    RESULT_VARIABLE _rc OUTPUT_VARIABLE _out ERROR_VARIABLE _out)
if(NOT _rc EQUAL 0)
    message(FATAL_ERROR "libmoq install (bare prefix) failed:\n${_out}")
endif()

# --- consumer project (requests ONLY COMPONENTS service, links only moq::service)
file(MAKE_DIRECTORY "${_csrc}")
configure_file("${SRC}/main.c" "${_csrc}/main.c" COPYONLY)
file(WRITE "${_csrc}/CMakeLists.txt" "
cmake_minimum_required(VERSION 3.16)
project(moq_service_wtquic_msquic_consumer LANGUAGES C)
find_package(libmoq REQUIRED COMPONENTS service)
add_executable(consumer main.c)
target_link_libraries(consumer PRIVATE moq::service)
")

# Only installed-prefix hints -- NO MOQ_PICOQUIC_SOURCE_DIR, so the consumer must
# discover the installed picoquic, not fall back to a source tree.
set(_fwd "")
if(DEFINED MSQUIC_DIR_HINT AND NOT MSQUIC_DIR_HINT STREQUAL "" AND
   NOT MSQUIC_DIR_HINT MATCHES "NOTFOUND")
    list(APPEND _fwd "-Dmsquic_DIR=${MSQUIC_DIR_HINT}")
endif()
if(DEFINED C_COMPILER AND NOT C_COMPILER STREQUAL "")
    list(APPEND _fwd "-DCMAKE_C_COMPILER=${C_COMPILER}")
endif()
if(DEFINED OPENSSL_ROOT AND NOT OPENSSL_ROOT STREQUAL "")
    list(APPEND _fwd "-DOPENSSL_ROOT_DIR=${OPENSSL_ROOT}")
endif()

# --- 1: POSITIVE -- installed find_package(COMPONENTS service), prefixes only ---
execute_process(
    COMMAND ${CMAKE_COMMAND} -S "${_csrc}" -B "${_cbuild}"
        "-DCMAKE_PREFIX_PATH=${_prefix};${WTQUIC_PREFIX}" ${_fwd}
    RESULT_VARIABLE _rc OUTPUT_VARIABLE _out ERROR_VARIABLE _out)
if(NOT _rc EQUAL 0)
    message(FATAL_ERROR "relocated consumer configure failed:\n${_out}")
endif()
execute_process(COMMAND ${CMAKE_COMMAND} --build "${_cbuild}"
    RESULT_VARIABLE _rc OUTPUT_VARIABLE _out ERROR_VARIABLE _out)
if(NOT _rc EQUAL 0)
    message(FATAL_ERROR "relocated consumer build failed:\n${_out}")
endif()
execute_process(COMMAND "${_cbuild}/consumer" RESULT_VARIABLE _rc)
if(NOT _rc EQUAL 0)
    message(FATAL_ERROR "relocated consumer run failed: ${_rc}")
endif()

# --- 2: NEGATIVE -- libmoq present, installed picoquic ABSENT -> must FAIL ------
# Same consumer, same forwarded hints, but a prefix that carries libmoq without
# an installed picoquic. If this configures cleanly the dependency was found
# somewhere it should not be (a leaked source tree / system copy), so the
# positive result would not actually prove installed-prefix discovery.
execute_process(
    COMMAND ${CMAKE_COMMAND} -S "${_csrc}" -B "${_cbuild_neg}"
        "-DCMAKE_PREFIX_PATH=${_bare};${WTQUIC_PREFIX}" ${_fwd}
    RESULT_VARIABLE _rc OUTPUT_VARIABLE _out ERROR_VARIABLE _out)
if(_rc EQUAL 0)
    message(FATAL_ERROR
        "negative test: the consumer configured WITHOUT an installed picoquic -- "
        "the dependency is not genuinely required/discovered from the prefix")
endif()

# --- 3: pkg-config consumer (when the .pc is generated) ------------------------
if(DO_PKGCONFIG)
    find_program(_pkgcfg pkg-config)
    if(NOT _pkgcfg)
        message(FATAL_ERROR "pkg-config not found but DO_PKGCONFIG=ON")
    endif()
    set(ENV{PKG_CONFIG_PATH}
        "${_prefix}/${LIBDIR}/pkgconfig:${WTQUIC_PREFIX}/${LIBDIR}/pkgconfig")
    execute_process(
        COMMAND ${_pkgcfg} --static --cflags --libs libmoq-service
        RESULT_VARIABLE _rc OUTPUT_VARIABLE _flags ERROR_VARIABLE _err)
    if(NOT _rc EQUAL 0)
        message(FATAL_ERROR "pkg-config resolve failed:\n${_err}")
    endif()
    string(STRIP "${_flags}" _flags)
    separate_arguments(_flag_list UNIX_COMMAND "${_flags}")
    if(NOT DEFINED C_COMPILER OR C_COMPILER STREQUAL "")
        set(C_COMPILER cc)
    endif()
    execute_process(
        COMMAND ${C_COMPILER} "${SRC}/main.c" -o "${WORK}/pc-consumer"
            ${_flag_list}
        RESULT_VARIABLE _rc OUTPUT_VARIABLE _out ERROR_VARIABLE _out)
    if(NOT _rc EQUAL 0)
        message(FATAL_ERROR
            "pkg-config consumer compile/link failed (${_flags}):\n${_out}")
    endif()
    execute_process(COMMAND "${WORK}/pc-consumer" RESULT_VARIABLE _rc)
    if(NOT _rc EQUAL 0)
        message(FATAL_ERROR "pkg-config consumer run failed: ${_rc}")
    endif()
endif()

message(STATUS "service_wtquic_msquic_install_consumer: OK (relocatable)")
