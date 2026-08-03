# Install the configured libmoq build to a scratch prefix, then build
# and run a standalone consumer that does
#     find_package(libmoq REQUIRED COMPONENTS adapter-wtquic)
# against it. Pins that the installed package re-resolves the wtquic
# dependency through normal package search.
#
# Args (all -D): BUILD (libmoq build dir), SRC (consumer source dir),
# WORK (scratch dir), WTQUIC_PREFIX (wtquic install prefix), and
# optional C_COMPILER/C_FLAGS/LINK_FLAGS forwarded from the parent.

foreach(_v BUILD SRC WORK WTQUIC_PREFIX)
    if(NOT DEFINED ${_v})
        message(FATAL_ERROR "pass -D${_v}=<path>")
    endif()
endforeach()

set(_prefix "${WORK}/prefix")
set(_cbuild "${WORK}/consumer-build")
file(REMOVE_RECURSE "${_prefix}" "${_cbuild}")

execute_process(
    COMMAND ${CMAKE_COMMAND} --install "${BUILD}" --prefix "${_prefix}"
    RESULT_VARIABLE _rc OUTPUT_VARIABLE _out ERROR_VARIABLE _out)
if(NOT _rc EQUAL 0)
    message(FATAL_ERROR "libmoq install failed:\n${_out}")
endif()

set(_fwd "")
if(DEFINED C_COMPILER AND NOT C_COMPILER STREQUAL "")
    list(APPEND _fwd "-DCMAKE_C_COMPILER=${C_COMPILER}")
endif()
if(DEFINED C_FLAGS AND NOT C_FLAGS STREQUAL "")
    list(APPEND _fwd "-DCMAKE_C_FLAGS=${C_FLAGS}")
endif()
if(DEFINED LINK_FLAGS AND NOT LINK_FLAGS STREQUAL "")
    list(APPEND _fwd "-DCMAKE_EXE_LINKER_FLAGS=${LINK_FLAGS}")
endif()

if(DEFINED NETWORK_MANAGED AND NETWORK_MANAGED)
    list(APPEND _fwd "-DWITH_NETWORK_MANAGED=ON")
endif()

execute_process(
    COMMAND ${CMAKE_COMMAND} -S "${SRC}" -B "${_cbuild}"
        "-DCMAKE_PREFIX_PATH=${_prefix};${WTQUIC_PREFIX}" ${_fwd}
    RESULT_VARIABLE _rc OUTPUT_VARIABLE _out ERROR_VARIABLE _out)
if(NOT _rc EQUAL 0)
    message(FATAL_ERROR "consumer configure failed:\n${_out}")
endif()

execute_process(
    COMMAND ${CMAKE_COMMAND} --build "${_cbuild}"
    RESULT_VARIABLE _rc OUTPUT_VARIABLE _out ERROR_VARIABLE _out)
if(NOT _rc EQUAL 0)
    message(FATAL_ERROR "consumer build failed:\n${_out}")
endif()

execute_process(
    COMMAND "${_cbuild}/moq_wtquic_consumer_test"
    RESULT_VARIABLE _rc)
if(NOT _rc EQUAL 0)
    message(FATAL_ERROR "consumer run failed: ${_rc}")
endif()

if(DEFINED NETWORK_MANAGED AND NETWORK_MANAGED)
    foreach(_exe moq_wtquic_network_consumer_test moq_wtquic_network_consumer_test_cxx)
        execute_process(
            COMMAND "${_cbuild}/${_exe}"
            RESULT_VARIABLE _rc)
        if(NOT _rc EQUAL 0)
            message(FATAL_ERROR "${_exe} run failed: ${_rc}")
        endif()
    endforeach()

    # --- relocated prefix: the SAME install keeps working after a move ------
    set(_moved "${WORK}/prefix-moved")
    set(_cbuild_moved "${WORK}/consumer-build-moved")
    file(REMOVE_RECURSE "${_moved}" "${_cbuild_moved}")
    file(RENAME "${_prefix}" "${_moved}")
    execute_process(
        COMMAND ${CMAKE_COMMAND} -S "${SRC}" -B "${_cbuild_moved}"
            "-DCMAKE_PREFIX_PATH=${_moved};${WTQUIC_PREFIX}" ${_fwd}
        RESULT_VARIABLE _rc OUTPUT_VARIABLE _out ERROR_VARIABLE _out)
    if(NOT _rc EQUAL 0)
        message(FATAL_ERROR "relocated consumer configure failed:\n${_out}")
    endif()
    execute_process(
        COMMAND ${CMAKE_COMMAND} --build "${_cbuild_moved}"
        RESULT_VARIABLE _rc OUTPUT_VARIABLE _out ERROR_VARIABLE _out)
    if(NOT _rc EQUAL 0)
        message(FATAL_ERROR "relocated consumer build failed:\n${_out}")
    endif()
    execute_process(
        COMMAND "${_cbuild_moved}/moq_wtquic_network_consumer_test"
        RESULT_VARIABLE _rc)
    if(NOT _rc EQUAL 0)
        message(FATAL_ERROR "relocated network consumer run failed: ${_rc}")
    endif()

    # --- static pkg-config against the RELOCATED prefix ----------------------
    find_program(_pkgconf NAMES pkg-config)
    find_program(_cc NAMES cc clang)
    if(_pkgconf AND _cc)
        execute_process(
            COMMAND ${CMAKE_COMMAND} -E env
                "PKG_CONFIG_PATH=${_moved}/lib/pkgconfig:${WTQUIC_PREFIX}/lib/pkgconfig"
                ${_pkgconf} --static --cflags --libs
                libmoq-wtquic-network-managed
            RESULT_VARIABLE _rc OUTPUT_VARIABLE _flags ERROR_VARIABLE _err)
        if(NOT _rc EQUAL 0)
            message(FATAL_ERROR
                "pkg-config libmoq-wtquic-network-managed failed:\n${_err}")
        endif()
        string(STRIP "${_flags}" _flags)
        separate_arguments(_flags_list UNIX_COMMAND "${_flags}")
        separate_arguments(_cflags_extra UNIX_COMMAND "${C_FLAGS}")
        separate_arguments(_lflags_extra UNIX_COMMAND "${LINK_FLAGS}")
        execute_process(
            COMMAND ${_cc} -std=c11 "${SRC}/main_network.c"
                -o "${WORK}/network_pc_consumer"
                ${_cflags_extra} ${_flags_list} ${_lflags_extra}
            RESULT_VARIABLE _rc OUTPUT_VARIABLE _out ERROR_VARIABLE _out)
        if(NOT _rc EQUAL 0)
            message(FATAL_ERROR "network pkg-config consumer link failed:\n${_out}")
        endif()
        execute_process(COMMAND "${WORK}/network_pc_consumer"
            RESULT_VARIABLE _rc)
        if(NOT _rc EQUAL 0)
            message(FATAL_ERROR "network pkg-config consumer run failed: ${_rc}")
        endif()
    else()
        message(STATUS "pkg-config or cc unavailable: static pc lane skipped")
    endif()

    # --- missing component fails loudly ---------------------------------------
    set(_cbuild_missing "${WORK}/consumer-build-missing")
    file(REMOVE_RECURSE "${_cbuild_missing}")
    file(REMOVE "${_moved}/lib/cmake/libmoq/libmoqWtquicNetworkManagedTargets.cmake")
    execute_process(
        COMMAND ${CMAKE_COMMAND} -S "${SRC}" -B "${_cbuild_missing}"
            -DWITH_NETWORK_MANAGED=ON
            "-DCMAKE_PREFIX_PATH=${_moved};${WTQUIC_PREFIX}" ${_fwd}
        RESULT_VARIABLE _rc OUTPUT_VARIABLE _out ERROR_VARIABLE _out)
    if(_rc EQUAL 0)
        message(FATAL_ERROR
            "REQUIRED COMPONENTS adapter-wtquic-network-managed succeeded "
            "without the component's targets file")
    endif()
    if(NOT _out MATCHES "adapter-wtquic-network-managed")
        message(FATAL_ERROR
            "missing-component failure lacks a component message:\n${_out}")
    endif()
endif()

# --- adapter-wtquic-msquic-managed: self-contained (own fresh install so it is
#     independent of the network block's destructive relocation above) --------
if(DEFINED MSQUIC_MANAGED AND MSQUIC_MANAGED)
    set(_mpfx "${WORK}/prefix-msquic")
    set(_mbuild "${WORK}/consumer-build-msquic")
    file(REMOVE_RECURSE "${_mpfx}" "${_mbuild}")
    execute_process(
        COMMAND ${CMAKE_COMMAND} --install "${BUILD}" --prefix "${_mpfx}"
        RESULT_VARIABLE _rc OUTPUT_VARIABLE _out ERROR_VARIABLE _out)
    if(NOT _rc EQUAL 0)
        message(FATAL_ERROR "msquic-managed install failed:\n${_out}")
    endif()
    execute_process(
        COMMAND ${CMAKE_COMMAND} -S "${SRC}" -B "${_mbuild}"
            -DWITH_MSQUIC_MANAGED=ON
            "-DCMAKE_PREFIX_PATH=${_mpfx};${WTQUIC_PREFIX}" ${_fwd}
        RESULT_VARIABLE _rc OUTPUT_VARIABLE _out ERROR_VARIABLE _out)
    if(NOT _rc EQUAL 0)
        message(FATAL_ERROR "msquic-managed consumer configure failed:\n${_out}")
    endif()
    execute_process(
        COMMAND ${CMAKE_COMMAND} --build "${_mbuild}"
        RESULT_VARIABLE _rc OUTPUT_VARIABLE _out ERROR_VARIABLE _out)
    if(NOT _rc EQUAL 0)
        message(FATAL_ERROR "msquic-managed consumer build failed:\n${_out}")
    endif()
    foreach(_exe moq_wtquic_msquic_consumer_test moq_wtquic_msquic_consumer_test_cxx)
        execute_process(COMMAND "${_mbuild}/${_exe}" RESULT_VARIABLE _rc)
        if(NOT _rc EQUAL 0)
            message(FATAL_ERROR "${_exe} run failed: ${_rc}")
        endif()
    endforeach()

    # relocated prefix: the same install keeps working after a move
    set(_mmoved "${WORK}/prefix-msquic-moved")
    set(_mbuild_moved "${WORK}/consumer-build-msquic-moved")
    file(REMOVE_RECURSE "${_mmoved}" "${_mbuild_moved}")
    file(RENAME "${_mpfx}" "${_mmoved}")
    execute_process(
        COMMAND ${CMAKE_COMMAND} -S "${SRC}" -B "${_mbuild_moved}"
            -DWITH_MSQUIC_MANAGED=ON
            "-DCMAKE_PREFIX_PATH=${_mmoved};${WTQUIC_PREFIX}" ${_fwd}
        RESULT_VARIABLE _rc OUTPUT_VARIABLE _out ERROR_VARIABLE _out)
    if(NOT _rc EQUAL 0)
        message(FATAL_ERROR "relocated msquic consumer configure failed:\n${_out}")
    endif()
    execute_process(
        COMMAND ${CMAKE_COMMAND} --build "${_mbuild_moved}"
        RESULT_VARIABLE _rc OUTPUT_VARIABLE _out ERROR_VARIABLE _out)
    if(NOT _rc EQUAL 0)
        message(FATAL_ERROR "relocated msquic consumer build failed:\n${_out}")
    endif()
    execute_process(COMMAND "${_mbuild_moved}/moq_wtquic_msquic_consumer_test"
        RESULT_VARIABLE _rc)
    if(NOT _rc EQUAL 0)
        message(FATAL_ERROR "relocated msquic consumer run failed: ${_rc}")
    endif()

    # static pkg-config against the RELOCATED prefix
    find_program(_pkgconf NAMES pkg-config)
    find_program(_cc NAMES cc clang)
    if(_pkgconf AND _cc)
        execute_process(
            COMMAND ${CMAKE_COMMAND} -E env
                "PKG_CONFIG_PATH=${_mmoved}/lib/pkgconfig:${WTQUIC_PREFIX}/lib/pkgconfig"
                ${_pkgconf} --static --cflags --libs
                libmoq-wtquic-msquic-managed
            RESULT_VARIABLE _rc OUTPUT_VARIABLE _flags ERROR_VARIABLE _err)
        if(NOT _rc EQUAL 0)
            message(FATAL_ERROR
                "pkg-config libmoq-wtquic-msquic-managed failed:\n${_err}")
        endif()
        string(STRIP "${_flags}" _flags)
        separate_arguments(_flags_list UNIX_COMMAND "${_flags}")
        separate_arguments(_cflags_extra UNIX_COMMAND "${C_FLAGS}")
        separate_arguments(_lflags_extra UNIX_COMMAND "${LINK_FLAGS}")
        # NB: link with pkg-config output ALONE (no injected -L). The MsQuic
        # directory is carried transitively by wtquic-msquic.pc's Libs.private
        # (pulled in via this package's Requires:), so a real consumer needs
        # nothing out-of-band.
        execute_process(
            COMMAND ${_cc} -std=c11 "${SRC}/main_msquic.c"
                -o "${WORK}/msquic_pc_consumer"
                ${_cflags_extra} ${_flags_list} ${_lflags_extra}
            RESULT_VARIABLE _rc OUTPUT_VARIABLE _out ERROR_VARIABLE _out)
        if(NOT _rc EQUAL 0)
            message(FATAL_ERROR "msquic pkg-config consumer link failed:\n${_out}")
        endif()
        execute_process(COMMAND "${WORK}/msquic_pc_consumer" RESULT_VARIABLE _rc)
        if(NOT _rc EQUAL 0)
            message(FATAL_ERROR "msquic pkg-config consumer run failed: ${_rc}")
        endif()
    else()
        message(STATUS "pkg-config or cc unavailable: msquic static pc lane skipped")
    endif()

    # missing component fails loudly
    set(_mbuild_missing "${WORK}/consumer-build-msquic-missing")
    file(REMOVE_RECURSE "${_mbuild_missing}")
    file(REMOVE "${_mmoved}/lib/cmake/libmoq/libmoqWtquicMsquicManagedTargets.cmake")
    execute_process(
        COMMAND ${CMAKE_COMMAND} -S "${SRC}" -B "${_mbuild_missing}"
            -DWITH_MSQUIC_MANAGED=ON
            "-DCMAKE_PREFIX_PATH=${_mmoved};${WTQUIC_PREFIX}" ${_fwd}
        RESULT_VARIABLE _rc OUTPUT_VARIABLE _out ERROR_VARIABLE _out)
    if(_rc EQUAL 0)
        message(FATAL_ERROR
            "REQUIRED COMPONENTS adapter-wtquic-msquic-managed succeeded "
            "without the component's targets file")
    endif()
    if(NOT _out MATCHES "adapter-wtquic-msquic-managed")
        message(FATAL_ERROR
            "missing-component failure lacks a component message:\n${_out}")
    endif()
endif()

message(STATUS "wtquic_install_consumer: OK")
