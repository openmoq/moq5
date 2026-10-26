# FindPicoquic.cmake
#
# Provides picoquic::picoquic-core for moq-adapter-picoquic, and -- when the
# WebTransport adapter is built (MOQ_BUILD_ADAPTER_PICO_WT; the managed helper
# MOQ_BUILD_PICO_WT_MANAGED requires it) -- picoquic::picohttp-core for HTTP/3.
#
# HTTP/WebTransport is an EXPLICIT requirement of this module: when WT is built,
# it guarantees picoquic::picohttp-core or fails here with a precise message. It
# must never report success with only the core target and let the pico_wt
# adapter discover the missing HTTP support later (the WT dependency contract
# lives here, not as a late "target not found" in adapters/pico_wt).
#
# Resolution order:
# 1. Already-defined target (parent add_subdirectory or prior find)
# 2. Installed config package (picoquic-config.cmake in prefix)
# 3. Source-tree mode (local dev):
#      -DMOQ_PICOQUIC_SOURCE_DIR=/path/to/picoquic
#      -DMOQ_PICOTLS_PREFIX=/path/to/picotls/build

# WebTransport needs picoquic's HTTP/3 library (picohttp-core). Managed WT
# requires the base WT adapter (enforced in the top-level CMakeLists), so this
# one flag covers both.
set(_MOQ_PQ_NEED_HTTP ${MOQ_BUILD_ADAPTER_PICO_WT})

# A drain-capable adapter (raw threaded helper or managed WebTransport) compiles
# adapters/common/moq_pq_stream_backlog.h against picoquic's PRIVATE
# picoquic_internal.h from MOQ_PICOQUIC_SOURCE_DIR. The picoquic LIBRARY it links
# MUST be that same source revision: linking an installed picoquic of a different
# revision is a silent ABI mismatch (it reads picoquic_stream_head_t's layout).
# So in that mode the source tree is authoritative -- the installed CONFIG
# package is bypassed entirely (see "Installed config package" below), which
# prevents the "headers from source, library from install" split build.
set(_MOQ_PQ_REQUIRE_SOURCE OFF)
if((MOQ_BUILD_PQ_THREADED OR MOQ_BUILD_PICO_WT_MANAGED) AND MOQ_PICOQUIC_SOURCE_DIR)
    set(_MOQ_PQ_REQUIRE_SOURCE ON)
endif()

# Normalize a non-namespaced picohttp-core to picoquic::picohttp-core. ALIAS
# targets are global, so either spelling resolves to the namespaced name the
# pico_wt adapter and the install export reference.
function(_moq_pq_alias_http)
    if(TARGET picohttp-core AND NOT TARGET picoquic::picohttp-core)
        add_library(picoquic::picohttp-core ALIAS picohttp-core)
    endif()
endfunction()

# Require picoquic::picohttp-core, or fail with an actionable message. We do not
# synthesize an imported picohttp-core: its transitive link interface (h3zero +
# webtransport over picoquic-core/log, picotls, OpenSSL) is build-config
# dependent and cannot be reconstructed reliably from an arbitrary prefix, so a
# silently-wrong target would only move the failure to link time.
function(_moq_pq_require_http where)
    _moq_pq_alias_http()
    if(TARGET picoquic::picohttp-core)
        return()
    endif()
    message(FATAL_ERROR
        "libmoq WebTransport support is enabled (MOQ_BUILD_ADAPTER_PICO_WT=ON), "
        "which requires picoquic's HTTP/3 library, but ${where} provides "
        "picoquic without an exported picohttp-core / picoquic::picohttp-core "
        "target.\n"
        "Fix one of:\n"
        "  - reinstall picoquic built with -DBUILD_HTTP=ON and a complete CMake "
        "export (it must install AND export picohttp-core, i.e. h3zero + "
        "webtransport), then point CMAKE_PREFIX_PATH at that prefix; or\n"
        "  - build libmoq against the picoquic source tree with "
        "-DMOQ_PICOQUIC_SOURCE_DIR=/path/to/picoquic (libmoq forces "
        "BUILD_HTTP=ON there).")
endfunction()

# Normalize the picotls headers (PTLS_INCLUDE_DIR) and the picotls library
# directory (MOQ_PTLS_LIB_DIR) once, reliably, in every mode -- picoquic links
# picotls but installs neither its headers, its libraries, nor a .pc, so libmoq
# must locate both itself.
#
#   PTLS_INCLUDE_DIR  -- dir with picotls.h / picotls/openssl.h. Needed by the
#       cert-verifier helper (moq_picoquic_verify.c) and the managed edge tests.
#       In source-tree mode picoquic's FindPTLS sets it; in CONFIG / already-
#       available mode nothing does.
#   MOQ_PTLS_LIB_DIR  -- dir with libpicotls-*.a. Needed so the generated .pc
#       files can bake an absolute -L for the picotls libraries. This must NOT
#       be derived from PTLS_OPENSSL_LIBRARY: picoquic's FindPTLS sets that to a
#       bare target name ("picotls-openssl") when picotls builds in-tree -- a
#       non-cache var in picoquic's subdirectory scope, invisible at top-level
#       .pc time -- and to an absolute path only when found prebuilt. Resolve
#       the directory directly from the MOQ_PICOTLS_PREFIX hint instead.
#
# Both are cache vars (globally visible after find_package(Picoquic)). Absent a
# usable MOQ_PICOTLS_PREFIX hint they stay unset and consumers fall back to
# their own resolution / an actionable error.
function(_moq_pq_resolve_ptls)
    if(NOT PTLS_INCLUDE_DIR OR NOT EXISTS "${PTLS_INCLUDE_DIR}/picotls.h")
        find_path(PTLS_INCLUDE_DIR
            NAMES picotls.h
            HINTS
                ${MOQ_PICOTLS_PREFIX}/include
                ${MOQ_PICOTLS_PREFIX}
                ${MOQ_PICOTLS_PREFIX}/../include
            DOC "picotls include directory (picotls.h)")
    endif()
    if(NOT MOQ_PTLS_LIB_DIR OR NOT EXISTS "${MOQ_PTLS_LIB_DIR}")
        if(PTLS_CORE_LIBRARY AND IS_ABSOLUTE "${PTLS_CORE_LIBRARY}"
           AND EXISTS "${PTLS_CORE_LIBRARY}")
            # picotls-core exists for EVERY provider, so it is the reliable
            # source. Deriving from the OpenSSL library instead left this unset
            # on an mbedTLS build, and the generated .pc then emitted
            # -lpicotls-* with no -L for it to resolve against.
            get_filename_component(_moq_ptls_dir "${PTLS_CORE_LIBRARY}" DIRECTORY)
            set(MOQ_PTLS_LIB_DIR "${_moq_ptls_dir}"
                CACHE PATH "picotls library directory (libpicotls-*.a)" FORCE)
        elseif(PTLS_OPENSSL_LIBRARY AND EXISTS "${PTLS_OPENSSL_LIBRARY}")
            # Source layouts where PTLS_CORE_LIBRARY is a target name rather
            # than an absolute file still resolve through the OpenSSL library's
            # absolute path.
            get_filename_component(_moq_ptls_dir "${PTLS_OPENSSL_LIBRARY}" DIRECTORY)
            set(MOQ_PTLS_LIB_DIR "${_moq_ptls_dir}"
                CACHE PATH "picotls library directory (libpicotls-*.a)" FORCE)
        else()
            find_library(MOQ_PTLS_CORE_LIBRARY
                NAMES picotls-core
                HINTS
                    ${MOQ_PICOTLS_PREFIX}
                    ${MOQ_PICOTLS_PREFIX}/lib
                    ${MOQ_PICOTLS_PREFIX}/../build
                    ${PTLS_PREFIX}/lib
                DOC "picotls core library (libpicotls-core.a)")
            if(MOQ_PTLS_CORE_LIBRARY)
                get_filename_component(_moq_ptls_dir "${MOQ_PTLS_CORE_LIBRARY}" DIRECTORY)
                set(MOQ_PTLS_LIB_DIR "${_moq_ptls_dir}"
                    CACHE PATH "picotls library directory (libpicotls-*.a)" FORCE)
            endif()
        endif()
    endif()
endfunction()

# Source-tree mode: select picotls from MOQ_PICOTLS_PREFIX, which is the
# authority for BOTH its headers and its archives. Upstream's FindPTLS would
# otherwise search sibling paths relative to the top-level project and the build
# tree first, and cache whatever picotls it happens to find beside them -- a
# tree other than the one declared, or headers from one tree beside archives
# from another. The selection runs BEFORE picoquic is added, by existence checks
# only (no find_* search), and seeds the PTLS_* cache entries upstream reads, so
# upstream's own searches are satisfied and never run.
#
# Which archives are required follows upstream's provider branches: mbedTLS
# needs core + minicrypto; OpenSSL also needs picotls-openssl; fusion is used
# when present. Every PTLS_* entry follows one rule:
#   * unset, or equal to what LibMoQ recorded when it last selected it  ->
#     the prefix's artifact (so a changed prefix is followed);
#   * the prefix's artifact by real path                                ->
#     accepted as it is;
#   * anything else -- a caller's value, or an entry some earlier search
#     cached -- cannot be told apart, so it FAILS by name rather than being
#     silently replaced or silently mixed with the prefix's tree.
# PICOQUIC_FETCH_PTLS=ON makes upstream build picotls from source with
# FetchContent and ignore these entries, so that mode has its own authority;
# see _moq_pq_check_fetched_ptls_source().
function(_moq_pq_select_ptls_from_prefix)
    file(REAL_PATH "${MOQ_PICOTLS_PREFIX}" _p)
    set(_inc "")
    foreach(_c "${_p}/include" "${_p}" "${_p}/../include")
        if(EXISTS "${_c}/picotls.h" AND EXISTS "${_c}/picotls/minicrypto.h")
            file(REAL_PATH "${_c}" _inc)
            break()
        endif()
    endforeach()
    if(NOT _inc)
        message(FATAL_ERROR
            "MOQ_PICOTLS_PREFIX=${MOQ_PICOTLS_PREFIX} has no picotls headers "
            "(picotls.h and picotls/minicrypto.h) in ${_p}/include, ${_p} or "
            "${_p}/../include.")
    endif()
    set(_libdir "")
    foreach(_c "${_p}" "${_p}/lib")
        if(EXISTS "${_c}/${CMAKE_STATIC_LIBRARY_PREFIX}picotls-core${CMAKE_STATIC_LIBRARY_SUFFIX}")
            file(REAL_PATH "${_c}" _libdir)
            break()
        endif()
    endforeach()
    if(NOT _libdir)
        message(FATAL_ERROR
            "MOQ_PICOTLS_PREFIX=${MOQ_PICOTLS_PREFIX} has no picotls-core "
            "archive in ${_p} or ${_p}/lib.")
    endif()
    set(_want PTLS_INCLUDE_DIR "${_inc}")
    set(_names core minicrypto)
    if(NOT WITH_MBEDTLS AND (NOT DEFINED WITH_OPENSSL OR WITH_OPENSSL))
        list(APPEND _names openssl)
    endif()
    foreach(_n IN LISTS _names)
        set(_a "${_libdir}/${CMAKE_STATIC_LIBRARY_PREFIX}picotls-${_n}${CMAKE_STATIC_LIBRARY_SUFFIX}")
        if(NOT EXISTS "${_a}")
            message(FATAL_ERROR
                "MOQ_PICOTLS_PREFIX=${MOQ_PICOTLS_PREFIX} has no picotls-${_n} "
                "archive (${_a}), which the selected picoquic provider needs.")
        endif()
        string(TOUPPER "${_n}" _N)
        list(APPEND _want "PTLS_${_N}_LIBRARY" "${_a}")
    endforeach()
    if(NOT WITH_MBEDTLS)
        set(_a "${_libdir}/${CMAKE_STATIC_LIBRARY_PREFIX}picotls-fusion${CMAKE_STATIC_LIBRARY_SUFFIX}")
        if(EXISTS "${_a}")
            list(APPEND _want PTLS_FUSION_LIBRARY "${_a}")
        endif()
    endif()

    set(_selected "")
    while(_want)
        list(POP_FRONT _want _var _path)
        set(_cur "${${_var}}")
        set(_rec "${_MOQ_PTLS_SELECTED_${_var}}")
        set(_ok FALSE)
        if(NOT _cur OR _cur MATCHES "-NOTFOUND$" OR (_rec AND _cur STREQUAL _rec))
            set(_ok TRUE)
        elseif(EXISTS "${_cur}")
            file(REAL_PATH "${_cur}" _cur_real)
            if(_cur_real STREQUAL _path)
                set(_ok TRUE)
            endif()
        endif()
        if(NOT _ok)
            message(FATAL_ERROR
                "${_var}=${_cur} is not the picotls of "
                "MOQ_PICOTLS_PREFIX=${MOQ_PICOTLS_PREFIX} (which selects "
                "${_path}). Remove the stale PTLS_* cache entries (for example "
                "cmake -U 'PTLS_*' <build-dir>) or use a fresh build directory.")
        endif()
        set(_type FILEPATH)
        if(_var STREQUAL "PTLS_INCLUDE_DIR")
            set(_type PATH)
        endif()
        set(${_var} "${_path}" CACHE ${_type}
            "picotls artifact selected from MOQ_PICOTLS_PREFIX" FORCE)
        set(_MOQ_PTLS_SELECTED_${_var} "${_path}" CACHE INTERNAL
            "the value LibMoQ last selected for ${_var}")
        list(APPEND _selected "${_var}")
    endwhile()
    set(MOQ_PTLS_LIB_DIR "${_libdir}"
        CACHE PATH "picotls library directory (libpicotls-*.a)" FORCE)
    set(_MOQ_PTLS_SELECTED_VARS "${_selected}" CACHE INTERNAL
        "the PTLS_* entries LibMoQ selected from MOQ_PICOTLS_PREFIX")
endfunction()

# PICOQUIC_FETCH_PTLS=ON builds picotls from a SOURCE checkout, and the checkout
# is the authority. The supported shape names one local checkout twice:
# MOQ_PICOTLS_PREFIX and FETCHCONTENT_SOURCE_DIR_PICOTLS (FetchContent's local
# source override, so nothing is downloaded). Both must be the same directory,
# and it must be a picotls checkout; this runs before picoquic is added, so a
# mismatch stops the configure before FetchContent does anything. Any other
# fetch configuration is not claimed here: a MOQ_PICOTLS_PREFIX that
# FindPicoquic derived from adjacency, or one given without the local source
# override, is not verified against what upstream fetches.
function(_moq_pq_check_fetched_ptls_source out_checked)
    set(${out_checked} FALSE PARENT_SCOPE)
    if(NOT FETCHCONTENT_SOURCE_DIR_PICOTLS OR _moq_ptls_prefix_derived)
        return()
    endif()
    file(REAL_PATH "${MOQ_PICOTLS_PREFIX}" _prefix)
    file(REAL_PATH "${FETCHCONTENT_SOURCE_DIR_PICOTLS}" _source)
    if(NOT _prefix STREQUAL _source)
        message(FATAL_ERROR
            "MOQ_PICOTLS_PREFIX=${MOQ_PICOTLS_PREFIX} and "
            "FETCHCONTENT_SOURCE_DIR_PICOTLS=${FETCHCONTENT_SOURCE_DIR_PICOTLS} "
            "name different picotls checkouts; with PICOQUIC_FETCH_PTLS=ON picoquic "
            "builds picotls from FETCHCONTENT_SOURCE_DIR_PICOTLS.")
    endif()
    if(NOT EXISTS "${_source}/CMakeLists.txt" OR NOT EXISTS "${_source}/include/picotls.h")
        message(FATAL_ERROR
            "FETCHCONTENT_SOURCE_DIR_PICOTLS=${FETCHCONTENT_SOURCE_DIR_PICOTLS} is "
            "not a picotls source checkout (no CMakeLists.txt and include/picotls.h).")
    endif()
    set(${out_checked} TRUE PARENT_SCOPE)
endfunction()

# After picoquic is added in that shape: the picotls it built, and the include
# directory picoquic-core compiles with, must be the checkout.
function(_moq_pq_check_fetched_ptls_result)
    include(FetchContent)
    FetchContent_GetProperties(picotls)
    file(REAL_PATH "${FETCHCONTENT_SOURCE_DIR_PICOTLS}" _source)
    set(_built "")
    if(picotls_SOURCE_DIR)
        file(REAL_PATH "${picotls_SOURCE_DIR}" _built)
    endif()
    if(NOT _built STREQUAL _source)
        message(FATAL_ERROR
            "picoquic built picotls from '${picotls_SOURCE_DIR}', not the checkout "
            "FETCHCONTENT_SOURCE_DIR_PICOTLS=${FETCHCONTENT_SOURCE_DIR_PICOTLS}.")
    endif()
    get_target_property(_incs picoquic-core INCLUDE_DIRECTORIES)
    set(_found FALSE)
    foreach(_d IN LISTS _incs)
        if(NOT _d MATCHES "^\\$<" AND EXISTS "${_d}/picotls.h")
            file(REAL_PATH "${_d}" _real)
            if(_real STREQUAL "${_source}/include")
                set(_found TRUE)
            else()
                message(FATAL_ERROR
                    "picoquic-core compiles with picotls headers from ${_d}, not "
                    "the checkout ${_source}/include.")
            endif()
        endif()
    endforeach()
    if(NOT _found)
        message(FATAL_ERROR
            "picoquic-core does not compile with the checkout's picotls headers "
            "(${_source}/include).")
    endif()
endfunction()

# After picoquic is added: every PTLS_* artifact upstream ended up using must be
# the one selected from the prefix. An archive upstream found on its own (fusion
# beside some other picotls) would mix trees, so it fails by name.
function(_moq_pq_check_ptls_selection)
    foreach(_var PTLS_INCLUDE_DIR PTLS_CORE_LIBRARY PTLS_MINICRYPTO_LIBRARY
                 PTLS_OPENSSL_LIBRARY PTLS_FUSION_LIBRARY)
        set(_cur "${${_var}}")
        if(NOT _cur OR _cur MATCHES "-NOTFOUND$" OR NOT IS_ABSOLUTE "${_cur}")
            continue()
        endif()
        if(NOT _var IN_LIST _MOQ_PTLS_SELECTED_VARS
           OR NOT _cur STREQUAL "${_MOQ_PTLS_SELECTED_${_var}}")
            message(FATAL_ERROR
                "${_var}=${_cur} was not selected from "
                "MOQ_PICOTLS_PREFIX=${MOQ_PICOTLS_PREFIX}; picoquic would mix it "
                "with that picotls. Remove it (cmake -U ${_var} <build-dir>).")
        endif()
    endforeach()
endfunction()

# -- Already available --------------------------------------------------
if(TARGET picoquic::picoquic-core OR TARGET picoquic-core)
    if(TARGET picoquic-core AND NOT TARGET picoquic::picoquic-core)
        add_library(picoquic::picoquic-core ALIAS picoquic-core)
    endif()
    if(TARGET picoquic-log AND NOT TARGET picoquic::picoquic-log)
        add_library(picoquic::picoquic-log ALIAS picoquic-log)
    endif()
    if(_MOQ_PQ_NEED_HTTP)
        # Core may be present while HTTP is not yet loaded (e.g. a prior find of
        # only the core export); give the installed config a chance to populate
        # the full export set before deciding.
        _moq_pq_alias_http()
        if(NOT TARGET picoquic::picohttp-core)
            find_package(picoquic CONFIG QUIET)
        endif()
        _moq_pq_require_http("the already-available picoquic")
    endif()
    _moq_pq_resolve_ptls()
    set(Picoquic_FOUND TRUE)
    return()
endif()

# -- Installed config package -------------------------------------------
# Bypassed when a drain-capable adapter requires the source tree (above): the
# linked library must match the private headers, so an installed picoquic is not
# eligible -- fall through to source-tree mode regardless of picoquic_DIR.
if(_MOQ_PQ_REQUIRE_SOURCE)
    if(picoquic_DIR)
        message(STATUS
            "libmoq: ignoring installed picoquic (picoquic_DIR=${picoquic_DIR}); "
            "a drain-capable picoquic adapter is enabled, so picoquic is built "
            "from MOQ_PICOQUIC_SOURCE_DIR=${MOQ_PICOQUIC_SOURCE_DIR} to keep "
            "picoquic_internal.h and the linked library at the same revision.")
    endif()
else()
    find_package(picoquic CONFIG QUIET)
    if(TARGET picoquic::picoquic-core)
        if(_MOQ_PQ_NEED_HTTP)
            _moq_pq_require_http("the installed picoquic CONFIG package")
        endif()
        _moq_pq_resolve_ptls()
        set(Picoquic_FOUND TRUE)
        return()
    endif()
endif()

# -- Source-tree mode ---------------------------------------------------
if(MOQ_PICOQUIC_SOURCE_DIR)
    if(NOT EXISTS "${MOQ_PICOQUIC_SOURCE_DIR}/CMakeLists.txt")
        message(FATAL_ERROR
            "MOQ_PICOQUIC_SOURCE_DIR does not contain CMakeLists.txt:\n"
            "  ${MOQ_PICOQUIC_SOURCE_DIR}")
    endif()

    # Resolve picotls build prefix for picoquic's FindPTLS.
    if(NOT MOQ_PICOTLS_PREFIX)
        set(_ptls_build "${MOQ_PICOQUIC_SOURCE_DIR}/../picotls/build")
        if(EXISTS "${_ptls_build}")
            get_filename_component(MOQ_PICOTLS_PREFIX "${_ptls_build}" ABSOLUTE)
            set(_moq_ptls_prefix_derived TRUE)
        else()
            message(FATAL_ERROR
                "picotls build not found adjacent to picoquic.\n"
                "Pass -DMOQ_PICOTLS_PREFIX=/path/to/picotls/build\n"
                "or build picotls: cd /path/to/picotls && cmake -B build && cmake --build build")
        endif()
    endif()

    # picoquic's FindPTLS searches PTLS_PREFIX/include for headers
    # and PTLS_PREFIX/lib for libraries. picotls source builds put
    # headers in source/include and libraries in build root.
    get_filename_component(_ptls_src "${MOQ_PICOTLS_PREFIX}/.." ABSOLUTE)
    if(EXISTS "${_ptls_src}/include/picotls/minicrypto.h")
        set(PTLS_PREFIX "${_ptls_src}" CACHE PATH "" FORCE)
        list(APPEND CMAKE_LIBRARY_PATH "${MOQ_PICOTLS_PREFIX}")
    else()
        set(PTLS_PREFIX "${MOQ_PICOTLS_PREFIX}" CACHE PATH "" FORCE)
    endif()

    set(_moq_ptls_fetch_checked FALSE)
    if(PICOQUIC_FETCH_PTLS)
        _moq_pq_check_fetched_ptls_source(_moq_ptls_fetch_checked)
    else()
        _moq_pq_select_ptls_from_prefix()
    endif()

    # Disable picoquic extras.
    set(BUILD_DEMO OFF CACHE BOOL "" FORCE)
    set(BUILD_PQBENCH OFF CACHE BOOL "" FORCE)
    set(BUILD_LOGLIB ON CACHE BOOL "" FORCE)
    set(BUILD_PICO_SIM OFF CACHE BOOL "" FORCE)

    # picohttp/h3zero needed for the picoquic WebTransport adapter.
    # picoquic-test needed for loopback sim harness. Upstream test
    # executables are built but disabled from our CTest suite.
    if(MOQ_PICO_WT_BUILD_LOOPBACK)
        set(picoquic_BUILD_TESTS ON CACHE BOOL "" FORCE)
    else()
        set(picoquic_BUILD_TESTS OFF CACHE BOOL "" FORCE)
    endif()
    if(_MOQ_PQ_NEED_HTTP)
        set(BUILD_HTTP ON CACHE BOOL "" FORCE)
    else()
        set(BUILD_HTTP OFF CACHE BOOL "" FORCE)
    endif()

    add_subdirectory("${MOQ_PICOQUIC_SOURCE_DIR}"
        "${CMAKE_BINARY_DIR}/_deps/picoquic" EXCLUDE_FROM_ALL)
    if(NOT PICOQUIC_FETCH_PTLS)
        _moq_pq_check_ptls_selection()
    elseif(_moq_ptls_fetch_checked)
        _moq_pq_check_fetched_ptls_result()
    endif()

    # picoquic_BUILD_TESTS=ON (forced above for the picoquic-test simulation
    # library our loopback/sim harness links) also makes picoquic register its
    # own conformance tests (picoquic_ct, picohttp_ct) in CTest. picoquic was
    # added EXCLUDE_FROM_ALL, so the normal libmoq build does not build their
    # executables -- which would leave the registered tests pointing at missing
    # binaries ("Not Run"). Since they ARE registered in our suite and run
    # quickly (~27s total), build their executables as part of the normal build
    # so they actually run. They are not hidden, ignored, or disabled.
    if(TARGET picoquic_ct AND TARGET picohttp_ct)
        add_custom_target(moq-picoquic-upstream-tests ALL
            DEPENDS picoquic_ct picohttp_ct)
    endif()

    if(TARGET picoquic-core)
        # The source-tree link graph is upstream's, unchanged: picoquic-log
        # links picoquic-core, and picoquic-core declares no dependency on
        # picoquic-log. Yet core's configuration and socket-loop objects call
        # picoquic_set_qlog, which picoquic-log defines. So a DIRECT consumer
        # of that core functionality links picoquic-log (which brings core),
        # or lists log before core, as upstream's own executables do; LibMoQ's
        # adapters carry the pair's closure (cmake/MoqPicoquicLink.cmake). A
        # core -> log edge here would close a
        # static-archive cycle that CMake repeats on the link line for every
        # consumer that leaves CMP0156 unset (any project below CMake 3.29),
        # and Apple's linker reports the repeated archives as duplicates.
        if(NOT TARGET picoquic::picoquic-core)
            add_library(picoquic::picoquic-core ALIAS picoquic-core)
        endif()
        if(TARGET picoquic-log AND NOT TARGET picoquic::picoquic-log)
            add_library(picoquic::picoquic-log ALIAS picoquic-log)
        endif()
        set(Picoquic_FOUND TRUE)
    elseif(TARGET picoquic::picoquic-core)
        set(Picoquic_FOUND TRUE)
    else()
        message(FATAL_ERROR "picoquic add_subdirectory did not produce picoquic-core target")
    endif()

    # Sanitize include dirs: some picoquic source trees add
    # $<BUILD_INTERFACE:path> for directories that may not exist.
    if(TARGET picoquic-core)
        get_target_property(_pq_inc picoquic-core INTERFACE_INCLUDE_DIRECTORIES)
        if(_pq_inc)
            set(_pq_clean "")
            foreach(_pq_d IN LISTS _pq_inc)
                set(_pq_keep TRUE)
                if(_pq_d MATCHES "^\\$<BUILD_INTERFACE:(.+)>$")
                    if(NOT EXISTS "${CMAKE_MATCH_1}")
                        set(_pq_keep FALSE)
                    endif()
                elseif(NOT _pq_d MATCHES "^\\$<" AND NOT EXISTS "${_pq_d}")
                    set(_pq_keep FALSE)
                endif()
                if(_pq_keep)
                    list(APPEND _pq_clean "${_pq_d}")
                endif()
            endforeach()
            set_target_properties(picoquic-core PROPERTIES
                INTERFACE_INCLUDE_DIRECTORIES "${_pq_clean}")
        endif()
    endif()

    # Source-tree mode forces BUILD_HTTP=ON when WT is needed, so picohttp-core
    # is built here; normalize it and assert the contract (the FATAL message
    # would only fire if upstream picoquic stopped producing the target).
    if(_MOQ_PQ_NEED_HTTP)
        _moq_pq_require_http("the picoquic source tree")
    endif()

    # picoquic add_subdirectory'd above runs picoquic's FindPTLS in a child
    # scope, so PTLS_INCLUDE_DIR / picotls lib paths set there are not reliably
    # visible here (and PTLS_OPENSSL_LIBRARY may be a bare target name). Resolve
    # both directly so .pc generation and the verifier helper see them.
    _moq_pq_resolve_ptls()

    return()
endif()

# -- Not found ----------------------------------------------------------
message(FATAL_ERROR
    "picoquic not found.\n"
    "Either install picoquic to a prefix in CMAKE_PREFIX_PATH,\n"
    "or pass -DMOQ_PICOQUIC_SOURCE_DIR=/path/to/picoquic\n"
    "(picotls must be adjacent or set -DMOQ_PICOTLS_PREFIX).")
