include(${CMAKE_CURRENT_LIST_DIR}/loader_images.cmake)
# Install the configured libmoq build to a scratch prefix, then build
# and run a standalone consumer that does
#     find_package(libmoq REQUIRED COMPONENTS adapter-wtquic)
# against it. Pins that the installed package re-resolves the wtquic
# dependency through normal package search.
#
# Args (all -D): BUILD (libmoq build dir), SRC (consumer source dir),
# WORK (scratch dir), WTQUIC_PREFIX (wtquic install prefix), and
# optional C_COMPILER/C_FLAGS/LINK_FLAGS/OPENSSL_ROOT forwarded from the parent.

foreach(_v BUILD SRC WORK WTQUIC_PREFIX)
    if(NOT DEFINED ${_v})
        message(FATAL_ERROR "pass -D${_v}=<path>")
    endif()
endforeach()

set(_prefix "${WORK}/prefix")
set(_cbuild "${WORK}/consumer-build")
file(REMOVE_RECURSE "${_prefix}" "${_cbuild}")

function(assert_child_openssl_root dir label)
    if(DEFINED OPENSSL_ROOT AND NOT OPENSSL_ROOT STREQUAL "")
        unset(_child_OPENSSL_ROOT_DIR)
        load_cache("${dir}" READ_WITH_PREFIX _child_ OPENSSL_ROOT_DIR)
        if(NOT DEFINED _child_OPENSSL_ROOT_DIR OR
           NOT _child_OPENSSL_ROOT_DIR STREQUAL OPENSSL_ROOT)
            message(FATAL_ERROR
                "${label} did not retain the selected OpenSSL root: got "
                "'${_child_OPENSSL_ROOT_DIR}', expected '${OPENSSL_ROOT}'")
        endif()
    endif()
endfunction()

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
if(DEFINED OPENSSL_ROOT AND NOT OPENSSL_ROOT STREQUAL "")
    list(APPEND _fwd "-DOPENSSL_ROOT_DIR=${OPENSSL_ROOT}")
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
assert_child_openssl_root("${_cbuild}" "installed consumer")

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
    assert_child_openssl_root("${_cbuild_moved}" "relocated consumer")
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
    assert_child_openssl_root("${_mbuild}" "msquic-managed consumer")
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
    assert_child_openssl_root("${_mbuild_moved}"
        "relocated msquic-managed consumer")
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

    # -- pkg-config consumers against the RELOCATED prefix -------------------
    #
    # Two independent questions, kept apart on purpose:
    #
    #   1. LINK CLOSURE comes from the installed pkg-config metadata alone.
    #      No -l/-L is injected, so a gap in Requires:/Libs.private surfaces
    #      as a link failure rather than being papered over.
    #   2. RUNTIME DISCOVERY of a nonstandard relocated private prefix is the
    #      consumer's own responsibility. This lane records consumer-owned
    #      RPATHs naming exactly the relocated LibMoQ prefix, the accepted
    #      WTQuic prefix and the selected MsQuic directory -- no ambient
    #      fallback, no system loader change, and nothing added to any
    #      installed .pc file.
    #
    # `--static` asks pkg-config for the PRIVATE dependency closure. It is not
    # a linker mode and does not by itself select archives; which of a DSO or
    # an archive the linker chose is proved separately below.
    find_program(_pkgconf NAMES pkg-config)
    # the configured compiler when the harness forwarded one, else the usual
    # names; this lane REQUIRES both tools rather than skipping quietly
    unset(_cc)
    unset(_cc CACHE)
    if(DEFINED C_COMPILER AND NOT "${C_COMPILER}" STREQUAL "")
        set(_cc "${C_COMPILER}")
    else()
        find_program(_cc NAMES cc clang)
    endif()
    if(NOT _pkgconf OR NOT _cc)
        message(FATAL_ERROR
            "pkg-config and a C compiler are required for this lane "
            "(pkg-config='${_pkgconf}' cc='${_cc}')")
    endif()
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
        if(NOT "${_out}" STREQUAL "")
            message(FATAL_ERROR
                "unexpected output from a successful link:\n${_out}")
        endif()
        # Build the expected runtime-image set from the artifacts the linker
        # can actually select. BUILD_SHARED_LIBS describes this LibMoQ build;
        # it says nothing about an independently supplied WTQuic or MsQuic
        # prefix. In particular, a static LibMoQ can and routinely does link a
        # shared WTQuic backend.
        set(_expect "")
        set(_private_prefix_shared 0)
        if(SHARED)
            set(_moq_names libmoq-adapter-wtquic-msquic-managed
                           libmoq-adapter-wtquic libmoq-core)
            foreach(_n IN LISTS _moq_names)
                file(GLOB _f "${_mmoved}/lib/${_n}.dylib"
                             "${_mmoved}/lib/${_n}.so")
                if(_f STREQUAL "")
                    message(FATAL_ERROR
                        "the relocated prefix has no ${_n} shared object")
                endif()
                list(GET _f 0 _f0)
                get_filename_component(_r "${_f0}" REALPATH)
                list(APPEND _expect "${_r}")
            endforeach()
            set(_private_prefix_shared 1)
        endif()
        set(_wtq_names libwtquic-msquic libwtquic)
        foreach(_n IN LISTS _wtq_names)
            file(GLOB _f "${WTQUIC_PREFIX}/lib/${_n}.dylib"
                         "${WTQUIC_PREFIX}/lib/${_n}.so")
            if(NOT _f STREQUAL "")
                list(GET _f 0 _f0)
                get_filename_component(_r "${_f0}" REALPATH)
                list(APPEND _expect "${_r}")
                set(_private_prefix_shared 1)
            endif()
        endforeach()
        if(NOT MSQUIC_LIB OR NOT EXISTS "${MSQUIC_LIB}")
            message(FATAL_ERROR
                "the accepted MsQuic library was not forwarded (MSQUIC_LIB="
                "'${MSQUIC_LIB}')")
        endif()
        get_filename_component(_msq_dir "${MSQUIC_LIB}" DIRECTORY)
        loader_artifact_is_shared("${MSQUIC_LIB}" _msq_shared)
        if(_msq_shared)
            get_filename_component(_msq_real "${MSQUIC_LIB}" REALPATH)
            list(APPEND _expect "${_msq_real}")
        endif()
        list(REMOVE_DUPLICATES _expect)

        # (a) THE DISCRIMINATOR, and it only means something when a LibMoQ or
        #     WTQuic layer comes from a private-prefix shared object: with no
        #     runtime path such a consumer must fail BEFORE main. If it ever
        #     succeeds, something ambient is satisfying that private prefix
        #     and everything below would prove nothing. A closure whose
        #     LibMoQ and WTQuic layers are archives has no private-prefix image
        #     to find, so it must run even when its external MsQuic dependency
        #     is shared but directly loadable (for example, by absolute Darwin
        #     install name or the system loader cache).
        execute_process(COMMAND "${WORK}/msquic_pc_consumer"
            RESULT_VARIABLE _rc OUTPUT_VARIABLE _o ERROR_VARIABLE _o)
        if(_private_prefix_shared)
            if(_rc EQUAL 0)
                message(FATAL_ERROR
                    "the no-runtime-path consumer RAN; an ambient loader path "
                    "is satisfying the relocated prefix, so this lane proves "
                    "nothing")
            endif()
            # ANY nonzero result is not the control: this consumer does real
            # create/dial work, so an ordinary failure, a signal death or a
            # missing executable must not be mistaken for the loader refusing
            # a missing library before main.
            loader_is_missing_library("${_rc}" "${_o}"
                "${CMAKE_HOST_SYSTEM_NAME}" _is_missing)
            if(NOT _is_missing)
                message(FATAL_ERROR
                    "the no-runtime-path consumer failed for a reason that is "
                    "NOT the pre-main missing-library refusal (rc=${_rc}):\n${_o}")
            endif()
        else()
            if(NOT _rc EQUAL 0)
                message(FATAL_ERROR
                    "an archive-linked consumer needs no runtime path but "
                    "failed: ${_rc}\n${_o}")
            endif()
        endif()
        # (b) the SAME link closure, plus consumer-owned RPATHs
        execute_process(
            COMMAND ${_cc} -std=c11 "${SRC}/main_msquic.c"
                -o "${WORK}/msquic_pc_consumer_rpath"
                ${_cflags_extra} ${_flags_list} ${_lflags_extra}
                "-Wl,-rpath,${_mmoved}/lib"
                "-Wl,-rpath,${WTQUIC_PREFIX}/lib"
                "-Wl,-rpath,${_msq_dir}"
            RESULT_VARIABLE _rc OUTPUT_VARIABLE _out ERROR_VARIABLE _out)
        if(NOT _rc EQUAL 0)
            message(FATAL_ERROR "rpath consumer link failed:\n${_out}")
        endif()
        if(NOT "${_out}" STREQUAL "")
            message(FATAL_ERROR "unexpected compiler/linker output:\n${_out}")
        endif()
        execute_process(COMMAND "${WORK}/msquic_pc_consumer_rpath"
            RESULT_VARIABLE _rc OUTPUT_VARIABLE _o2 ERROR_VARIABLE _e2)
        if(NOT _rc EQUAL 0)
            message(FATAL_ERROR
                "consumer run failed with consumer RPATHs: ${_rc}\n${_e2}")
        endif()
        if(NOT "${_o2}${_e2}" STREQUAL "")
            message(FATAL_ERROR
                "unexpected output from a successful consumer run:"
                "\n${_o2}${_e2}")
        endif()
        # (c) the ORIGINAL prefix is gone, and every MoQ/WT image actually
        #     loaded came from the relocated or accepted prefix
        if(EXISTS "${_mpfx}")
            message(FATAL_ERROR
                "the original install prefix still exists; the relocation "
                "control is not in force")
        endif()
        # (e) the ORDINARY --libs closure must also link and run when either
        #     public facade layer is a shared object
        execute_process(
            COMMAND ${CMAKE_COMMAND} -E env
                "PKG_CONFIG_PATH=${_mmoved}/lib/pkgconfig:${WTQUIC_PREFIX}/lib/pkgconfig"
                ${_pkgconf} --cflags --libs libmoq-wtquic-msquic-managed
            RESULT_VARIABLE _rc OUTPUT_VARIABLE _pubflags ERROR_VARIABLE _err)
        if(NOT _rc EQUAL 0)
            message(FATAL_ERROR "pkg-config --libs failed:\n${_err}")
        endif()
        string(STRIP "${_pubflags}" _pubflags)
        separate_arguments(_pubflags_list UNIX_COMMAND "${_pubflags}")
        if(_private_prefix_shared)
            execute_process(
                COMMAND ${_cc} -std=c11 "${SRC}/main_msquic.c"
                    -o "${WORK}/msquic_pc_consumer_pub"
                    ${_cflags_extra} ${_pubflags_list} ${_lflags_extra}
                    "-Wl,-rpath,${_mmoved}/lib"
                    "-Wl,-rpath,${WTQUIC_PREFIX}/lib"
                    "-Wl,-rpath,${_msq_dir}"
                RESULT_VARIABLE _rc OUTPUT_VARIABLE _out ERROR_VARIABLE _out)
            if(NOT _rc EQUAL 0)
                message(FATAL_ERROR
                    "ordinary --libs consumer link failed:\n${_out}")
            endif()
            if(NOT "${_out}" STREQUAL "")
                message(FATAL_ERROR
                    "unexpected output from the --libs link:\n${_out}")
            endif()
            execute_process(COMMAND "${WORK}/msquic_pc_consumer_pub"
                RESULT_VARIABLE _rc OUTPUT_VARIABLE _o1 ERROR_VARIABLE _e1)
            if(NOT _rc EQUAL 0)
                message(FATAL_ERROR
                    "ordinary --libs consumer run failed: ${_rc}\n${_e1}")
            endif()
            if(NOT "${_o1}${_e1}" STREQUAL "")
                message(FATAL_ERROR
                    "unexpected output from a successful --libs run:"
                    "\n${_o1}${_e1}")
            endif()
        endif()

        loader_trace_env(_trace_env)
        if(_trace_env STREQUAL "")
            message(FATAL_ERROR
                "no loaded-image inspection is implemented for host "
                "'${CMAKE_HOST_SYSTEM_NAME}'; this gate does not skip")
        endif()
        # record what those files ARE, so a later claim names bytes
        set(_expect_hashes "")
        foreach(_f IN LISTS _expect)
            file(SHA256 "${_f}" _h)
            list(APPEND _expect_hashes "${_h}")
            message(STATUS "expected image ${_h}  ${_f}")
        endforeach()

        # The REQUIRED set, chosen by the configuration rather than by what
        # happens to be on disk: a stale shared executable must not sneak into
        # a static run, and a missing required one must refuse.
        set(_required msquic_pc_consumer_rpath)
        if(_private_prefix_shared)
            list(APPEND _required msquic_pc_consumer_pub)
        else()
            file(REMOVE "${WORK}/msquic_pc_consumer_pub")
        endif()
        set(_validated "")
        foreach(_exe IN LISTS _required)
            if(NOT EXISTS "${WORK}/${_exe}")
                message(FATAL_ERROR
                    "a required consumer executable is missing: ${_exe}")
            endif()
            # inherited loader overrides are scrubbed: this run must stand on
            # the consumer's own recorded paths
            execute_process(
                COMMAND ${CMAKE_COMMAND} -E env
                    --unset=DYLD_LIBRARY_PATH
                    --unset=DYLD_FALLBACK_LIBRARY_PATH
                    --unset=DYLD_INSERT_LIBRARIES
                    --unset=LD_LIBRARY_PATH --unset=LD_PRELOAD
                    "${_trace_env}" "${WORK}/${_exe}"
                RESULT_VARIABLE _rc OUTPUT_VARIABLE _out
                ERROR_VARIABLE _loaded)
            if(NOT _rc EQUAL 0)
                message(FATAL_ERROR
                    "loader inspection run failed for ${_exe}: ${_rc}")
            endif()
            # the loader trace is stderr; the application says nothing
            if(NOT "${_out}" STREQUAL "")
                message(FATAL_ERROR
                    "${_exe}: unexpected application output during loader "
                    "inspection:\n${_out}")
            endif()
            loader_only_image_records("${_loaded}" "${CMAKE_HOST_SYSTEM_NAME}"
                _why)
            if(NOT _why STREQUAL "")
                message(FATAL_ERROR "${_exe}: ${_why}")
            endif()
            loader_parse_images("${_loaded}" "${CMAKE_HOST_SYSTEM_NAME}" _imgs)
            loader_require_images("${_imgs}" "${_expect}"
                "${_mmoved}/lib;${WTQUIC_PREFIX}/lib;${_msq_dir}"
                "${_exe}" _why)
            if(NOT _why STREQUAL "")
                message(FATAL_ERROR "${_why}")
            endif()
            # the recorded identities must still be the bytes that loaded
            foreach(_f IN LISTS _expect)
                file(SHA256 "${_f}" _h_now)
                list(FIND _expect "${_f}" _fi)
                list(GET _expect_hashes ${_fi} _h_then)
                if(NOT _h_now STREQUAL "${_h_then}")
                    message(FATAL_ERROR
                        "${_exe}: an expected image changed between recording "
                        "and use: ${_f}")
                endif()
            endforeach()
            list(APPEND _validated "${_exe}")
            message(STATUS "${_exe}: required images loaded from the "
                           "relocated/accepted prefixes")
        endforeach()
        # the completion check: every required executable really was validated
        list(LENGTH _required _nreq)
        list(LENGTH _validated _nval)
        if(NOT _nreq EQUAL _nval)
            message(FATAL_ERROR
                "loader validation did not run for every required consumer "
                "(${_nval} of ${_nreq}); the gate must not report success")
        endif()
        set(_loader_validation_done "${_nval}")
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

# The lane may only report success once the loader validation actually ran.
# A structural edit that leaves the validation in a branch nobody takes is
# invisible to every assertion inside it -- this is the check that notices,
# and it lives on the driver path rather than in the helper.
if(DEFINED MSQUIC_MANAGED AND MSQUIC_MANAGED)
    if(NOT DEFINED _loader_validation_done)
        message(FATAL_ERROR
            "the loader validation never ran; the installed-consumer gate "
            "cannot report success without it")
    endif()
    list(LENGTH _required _want_validated)
    if(NOT _loader_validation_done EQUAL _want_validated)
        message(FATAL_ERROR
            "the loader validation covered ${_loader_validation_done} "
            "consumer(s), not the required ${_want_validated}")
    endif()
endif()

message(STATUS "wtquic_install_consumer: OK")
