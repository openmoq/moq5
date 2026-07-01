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
        if(PTLS_OPENSSL_LIBRARY AND EXISTS "${PTLS_OPENSSL_LIBRARY}")
            # picoquic's FindPTLS found a prebuilt picotls: trust its absolute
            # path's directory.
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
        # picoquic-core references picoquic-log symbols (picoquic_set_qlog)
        # but does not link it. Patch the dependency so downstream gets
        # the correct link order on all platforms.
        if(TARGET picoquic-log)
            get_target_property(_pq_core_libs picoquic-core INTERFACE_LINK_LIBRARIES)
            if(NOT _pq_core_libs OR NOT "picoquic-log" IN_LIST _pq_core_libs)
                target_link_libraries(picoquic-core PUBLIC picoquic-log)
            endif()
        endif()
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
