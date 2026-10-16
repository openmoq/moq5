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
# webtransport over picoquic-log and picoquic-core, picotls, OpenSSL) is build-config
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

# One OpenSSL link identity for source-tree picoquic and the adapter.
#
# picoquic-core links the OpenSSL it finds as the raw artifact paths in
# ${OPENSSL_LIBRARIES}; moq-adapter-picoquic links the OpenSSL::SSL /
# OpenSSL::Crypto imported targets. The two spellings of one artifact are two
# link items, so CMake's link de-duplication (CMP0156/CMP0179, applied per
# linker capability) cannot merge them: a static OpenSSL then reaches a
# consumer's link line twice, which capability-aware linkers report as
# duplicate libraries. The adapter keeps its target dependency (its verifier
# uses OpenSSL directly); picoquic-core is given the same targets instead.
#
# Applies only when the adapter will link OpenSSL: the adapter is built and
# its verifier backend resolves to openssl (the rule in
# adapters/picoquic/CMakeLists.txt: "openssl", or "auto" unless picoquic is
# configured WITH_OPENSSL=OFF). OpenSSL-off, mbedTLS, installed or
# pre-existing picoquic targets are left as they are.
function(_moq_pq_openssl_identity_wanted out_var)
    set(${out_var} FALSE PARENT_SCOPE)
    if(NOT MOQ_BUILD_ADAPTER_PICOQUIC)
        return()
    endif()
    if(DEFINED WITH_OPENSSL AND NOT WITH_OPENSSL)
        return()
    endif()
    string(TOLOWER "${MOQ_PICOQUIC_VERIFY_BACKEND}" _backend)
    if(_backend STREQUAL "openssl" OR _backend STREQUAL "auto")
        set(${out_var} TRUE PARENT_SCOPE)
    endif()
endfunction()

# Find OpenSSL in this (top-level) directory before picoquic is added, so the
# imported targets are visible to the picoquic subtree and to the adapter.
# FindOpenSSL creates OpenSSL::SSL / OpenSSL::Crypto only when they do not
# exist yet, so the later find_package(OpenSSL) calls in both reuse them. The
# selection inputs are the ones picoquic's own call uses (the same cache
# entries and OPENSSL_ROOT_DIR). Called as a function, so the module's
# OPENSSL_* result variables stay local; imported targets and cache entries
# are directory- and project-wide by nature.
function(_moq_pq_openssl_targets)
    find_package(OpenSSL REQUIRED)
endfunction()

# In <target>'s LINK_LIBRARIES and INTERFACE_LINK_LIBRARIES, respell each item
# that is exactly the artifact an OpenSSL imported target resolves to (also
# inside $<LINK_ONLY:...>) as that target. Only imported targets with a
# single, configuration-independent IMPORTED_LOCATION are used; every other
# item, its position and its wrapper stay as they are.
function(_moq_pq_openssl_respell target)
    set(_from "")
    set(_to "")
    foreach(_imp IN ITEMS OpenSSL::SSL OpenSSL::Crypto)
        if(NOT TARGET ${_imp})
            continue()
        endif()
        get_target_property(_cfgs ${_imp} IMPORTED_CONFIGURATIONS)
        get_target_property(_loc ${_imp} IMPORTED_LOCATION)
        if(_cfgs OR NOT _loc)
            continue()
        endif()
        list(APPEND _from "${_loc}")
        list(APPEND _to "${_imp}")
    endforeach()
    if(NOT _from)
        return()
    endif()
    foreach(_prop IN ITEMS LINK_LIBRARIES INTERFACE_LINK_LIBRARIES)
        get_target_property(_items ${target} ${_prop})
        if(NOT _items)
            continue()
        endif()
        set(_out "")
        set(_changed FALSE)
        foreach(_item IN LISTS _items)
            set(_new "${_item}")
            list(FIND _from "${_item}" _i)
            if(_i GREATER -1)
                list(GET _to ${_i} _new)
            elseif(_item MATCHES "^\\$<LINK_ONLY:([^<>]+)>$")
                list(FIND _from "${CMAKE_MATCH_1}" _i)
                if(_i GREATER -1)
                    list(GET _to ${_i} _imp)
                    set(_new "$<LINK_ONLY:${_imp}>")
                endif()
            endif()
            if(NOT _new STREQUAL _item)
                set(_changed TRUE)
            endif()
            list(APPEND _out "${_new}")
        endforeach()
        if(_changed)
            set_property(TARGET ${target} PROPERTY ${_prop} "${_out}")
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

    # Run MoqPicoquicProjectPolicy.cmake inside picoquic's project() scope
    # (link-line de-duplication policies; see that file). A caller-provided
    # CMAKE_PROJECT_picoquic_INCLUDE -- a file, a module name or a list of
    # them -- is composed, not replaced: the policy file includes every entry
    # afterwards. Only the normal variable is touched, never a cache entry,
    # and its exact prior state (unset, empty or set) comes back once picoquic
    # has been added, so later project() calls elsewhere see the original.
    # The hand-over variable is likewise restored to whatever the caller had.
    if(DEFINED CMAKE_PROJECT_picoquic_INCLUDE)
        set(_moq_pq_include_was_defined TRUE)
        set(_moq_pq_saved_project_include "${CMAKE_PROJECT_picoquic_INCLUDE}")
    else()
        set(_moq_pq_include_was_defined FALSE)
    endif()
    if(DEFINED MOQ_PICOQUIC_CALLER_PROJECT_INCLUDE)
        set(_moq_pq_handover_was_defined TRUE)
        set(_moq_pq_saved_handover "${MOQ_PICOQUIC_CALLER_PROJECT_INCLUDE}")
    else()
        set(_moq_pq_handover_was_defined FALSE)
    endif()
    set(MOQ_PICOQUIC_CALLER_PROJECT_INCLUDE "${CMAKE_PROJECT_picoquic_INCLUDE}")
    set(CMAKE_PROJECT_picoquic_INCLUDE
        "${CMAKE_CURRENT_LIST_DIR}/MoqPicoquicProjectPolicy.cmake")

    # Resolved before picoquic exists as a target, so that pre-existing
    # picoquic targets (the branches above) are never touched.
    _moq_pq_openssl_identity_wanted(_moq_pq_openssl_identity)
    if(_moq_pq_openssl_identity)
        _moq_pq_openssl_targets()
    endif()

    add_subdirectory("${MOQ_PICOQUIC_SOURCE_DIR}"
        "${CMAKE_BINARY_DIR}/_deps/picoquic" EXCLUDE_FROM_ALL)

    if(_moq_pq_include_was_defined)
        set(CMAKE_PROJECT_picoquic_INCLUDE "${_moq_pq_saved_project_include}")
    else()
        unset(CMAKE_PROJECT_picoquic_INCLUDE)
    endif()
    if(_moq_pq_handover_was_defined)
        set(MOQ_PICOQUIC_CALLER_PROJECT_INCLUDE "${_moq_pq_saved_handover}")
    else()
        unset(MOQ_PICOQUIC_CALLER_PROJECT_INCLUDE)
    endif()
    unset(_moq_pq_include_was_defined)
    unset(_moq_pq_saved_project_include)
    unset(_moq_pq_handover_was_defined)
    unset(_moq_pq_saved_handover)

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
        # picoquic-core was created by the add_subdirectory above. Only it is
        # respelled: it is the provider edge the adapter's consumers link.
        # picohttp-core and an in-tree picotls-openssl carry raw OpenSSL
        # edges of their own and are left as they are.
        if(_moq_pq_openssl_identity)
            _moq_pq_openssl_respell(picoquic-core)
        endif()
        # The provider's own edges are used as declared: picoquic-log links
        # picoquic-core, picoquic-test links picoquic-log, and picoquic-core
        # needs nothing from picoquic-log. That holds for the qualified source
        # baseline (picoquic a602e60b), whose core no longer calls the log
        # library. Automatic accommodation of the older core/log cycle (e.g.
        # 55b473e2, whose core calls picoquic_set_qlog) is retired, and there
        # is no configure-time capability check. Consumers of such a source
        # tree MAY fail to link: its static archives still build with the
        # symbol unresolved, and another dependency may or may not supply
        # picoquic-log.
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
