# FindMsQuic
# ----------
# Locates MsQuic and defines the imported target msquic::msquic.
#
# Search order:
#   0. An msquic::msquic target the caller already defined. Target injection
#      wins over everything, including a pin: the caller has not asked us to
#      find MsQuic, they have handed us one. This precedence predates the pin
#      and is preserved deliberately.
#   1. An explicit artifact pin: MOQ_MSQUIC_PIN_INCLUDE_DIR and
#      MOQ_MSQUIC_PIN_LIBRARY supplied together. This is how an MsQuic built
#      OUTSIDE its checkout is consumed -- the root branch below can only find
#      a library under build/bin/, so an out-of-tree build has no root to point
#      at. The pin is honoured BEFORE the package search: an explicit
#      instruction that an ambient package could shadow is not a pin.
#   2. An installed msquic CMake config package (e.g. Homebrew
#      libmsquic).
#   3. MOQ_MSQUIC_ROOT (cache var), defaulting to ../msquic next to
#      this checkout: header at src/inc/msquic.h, library under
#      build/bin/.
#
# INPUTS vs RESULTS. The pin variables are inputs and nothing else writes them,
# so adding a pin to an already-configured tree switches it on the very next
# configure. MOQ_MSQUIC_INCLUDE_DIR and MOQ_MSQUIC_LIBRARY are results.
#
# The pin publishes its results as NORMAL variables, never cache entries. A
# cached result would outlive the pin that produced it: clear the pin and
# find_path/find_library find the leftovers already populated, skip searching
# the root they were asked about, and report a root-mode success that resolved
# nothing under that root. A configure-scope variable cannot go stale that way,
# because it is recomputed from the inputs every configure.
#
# A pin is checked, because every near-miss spelling of one otherwise resolves
# some OTHER MsQuic silently. Half a pin is refused before anything else runs,
# so no other input can complete it; so is a library that is
# missing or is a directory, an include root that is not a directory, and an
# include root whose msquic.h is missing or is itself a directory. Paths are
# canonicalized before checking, so a symlink to a real file or directory is
# valid, while what gets published is still the spelling the caller asked for.
# Either variable may be given untyped (plain -DVAR=value); both are normalized
# to PATH / FILEPATH cache entries here.
#
# LEGACY INPUTS. Before the pin existed, the only way to name an out-of-tree
# build was to preseed the result variables and rely on find_path/find_library
# skipping a populated cache entry. Such a caller is migrated with a warning
# rather than quietly resolved to some other MsQuic -- see the legacy block
# below for why that is only possible on a tree's first configure.
#
# Result variables: MsQuic_FOUND, and MOQ_MSQUIC_DISCOVERY_MODE recording
# WHICH branch actually resolved MsQuic ("preexisting", "pinned", "package", or
# "root").
# The mode is a fact about this configure, not a hint: a nonempty msquic_DIR
# cache entry can be stale while package discovery is disabled and the checkout
# fallback is what really ran, so nothing may infer the mode from that hint.
# It is a NORMAL result variable, like MsQuic_FOUND: a fact about the current
# configure, scoped to the caller and inherited by subdirectories added after
# this call. It is deliberately not cached -- a consuming project's cache should
# not carry it, and a normal variable also shadows any stale user-supplied cache
# entry of the same name.

if(TARGET msquic::msquic)
    set(MOQ_MSQUIC_DISCOVERY_MODE "preexisting")
    set(MsQuic_FOUND TRUE)
    return()
endif()

set(MOQ_MSQUIC_PIN_INCLUDE_DIR "${MOQ_MSQUIC_PIN_INCLUDE_DIR}" CACHE PATH
    "Directory holding msquic.h, pinning one MsQuic build directly")
set(MOQ_MSQUIC_PIN_LIBRARY "${MOQ_MSQUIC_PIN_LIBRARY}" CACHE FILEPATH
    "The MsQuic library file, pinning one MsQuic build directly")

# -- legacy result-variable inputs -------------------------------------------
# A populated result entry means "the caller supplied this" only until the
# first configure completes; after that the root branch's own find_path /
# find_library results live in those same entries and mean nothing of the kind.
# So this runs once per tree, gated on a marker written at the end.
# The explicit pin is decided FIRST, in full, before any legacy variable is
# looked at. That ordering is the contract: a caller who typed a pin has
# answered the question, and a leftover entry from the old spelling may neither
# override a complete pin nor quietly finish an incomplete one. Getting this
# backwards means half a pin plus a legacy pair configures happily against the
# MsQuic the operator did NOT name, with their explicit path discarded in
# silence.
set(_moq_pin_any FALSE)
set(_moq_pin_complete FALSE)
if(NOT MOQ_MSQUIC_PIN_INCLUDE_DIR STREQUAL "" OR
   NOT MOQ_MSQUIC_PIN_LIBRARY STREQUAL "")
    set(_moq_pin_any TRUE)
endif()
if(NOT MOQ_MSQUIC_PIN_INCLUDE_DIR STREQUAL "" AND
   NOT MOQ_MSQUIC_PIN_LIBRARY STREQUAL "")
    set(_moq_pin_complete TRUE)
endif()

# Half a pin would leave the other half to a search that can reach a different
# MsQuic, producing a header and a library from two builds. It is rejected here,
# ahead of everything, so nothing downstream can complete it.
if(_moq_pin_any AND NOT _moq_pin_complete)
    message(FATAL_ERROR
        "FindMsQuic: MOQ_MSQUIC_PIN_INCLUDE_DIR and MOQ_MSQUIC_PIN_LIBRARY "
        "pin one MsQuic build and must be supplied together.\n"
        "  MOQ_MSQUIC_PIN_INCLUDE_DIR: '${MOQ_MSQUIC_PIN_INCLUDE_DIR}'\n"
        "  MOQ_MSQUIC_PIN_LIBRARY:     '${MOQ_MSQUIC_PIN_LIBRARY}'")
endif()

# Legacy migration runs only when the caller supplied no explicit pin input at
# all. A complete pin above is already authoritative, and an incomplete one has
# already failed.
if(NOT MOQ_MSQUIC_DISCOVERY_INITIALIZED AND NOT _moq_pin_any)
    set(_moq_legacy_inc "$CACHE{MOQ_MSQUIC_INCLUDE_DIR}")
    set(_moq_legacy_lib "$CACHE{MOQ_MSQUIC_LIBRARY}")
    if(NOT _moq_legacy_inc STREQUAL "" OR NOT _moq_legacy_lib STREQUAL "")
        if(_moq_legacy_inc STREQUAL "" OR _moq_legacy_lib STREQUAL "")
            message(FATAL_ERROR
                "FindMsQuic: MOQ_MSQUIC_INCLUDE_DIR and MOQ_MSQUIC_LIBRARY are "
                "results, and preseeding one of them no longer names an MsQuic "
                "build. Use the pin inputs instead, and supply both:\n"
                "  -DMOQ_MSQUIC_PIN_INCLUDE_DIR=<dir with msquic.h>\n"
                "  -DMOQ_MSQUIC_PIN_LIBRARY=<the library file>\n"
                "  MOQ_MSQUIC_INCLUDE_DIR: '${_moq_legacy_inc}'\n"
                "  MOQ_MSQUIC_LIBRARY:     '${_moq_legacy_lib}'")
        endif()
        # A complete legacy pair is unambiguous about which MsQuic was meant,
        # so honour it rather than resolving something else behind the
        # caller's back.
        message(DEPRECATION
            "FindMsQuic: MOQ_MSQUIC_INCLUDE_DIR / MOQ_MSQUIC_LIBRARY are "
            "results now, not inputs. This build's pair is being used as a "
            "pin; switch to MOQ_MSQUIC_PIN_INCLUDE_DIR and "
            "MOQ_MSQUIC_PIN_LIBRARY.")
        set(MOQ_MSQUIC_PIN_INCLUDE_DIR "${_moq_legacy_inc}" CACHE PATH
            "Directory holding msquic.h, pinning one MsQuic build directly"
            FORCE)
        set(MOQ_MSQUIC_PIN_LIBRARY "${_moq_legacy_lib}" CACHE FILEPATH
            "The MsQuic library file, pinning one MsQuic build directly"
            FORCE)
        set(_moq_pin_complete TRUE)
        # The caller's spelling has been captured as pin inputs; clear the
        # results so the pin branch is what publishes them.
        unset(MOQ_MSQUIC_INCLUDE_DIR CACHE)
        unset(MOQ_MSQUIC_LIBRARY CACHE)
    endif()
endif()

if(_moq_pin_complete)
    # Checks run against the resolved paths, so a symlink is judged by what it
    # finally reaches. What gets PUBLISHED is still what the caller asked for:
    # canonicalizing a versioned dylib symlink would silently hand consumers a
    # different filename than the one on their command line.
    get_filename_component(_moq_pin_inc "${MOQ_MSQUIC_PIN_INCLUDE_DIR}" REALPATH)
    get_filename_component(_moq_pin_lib "${MOQ_MSQUIC_PIN_LIBRARY}" REALPATH)

    if(NOT EXISTS "${_moq_pin_lib}")
        message(FATAL_ERROR
            "FindMsQuic: pinned MOQ_MSQUIC_PIN_LIBRARY does not exist: "
            "${MOQ_MSQUIC_PIN_LIBRARY}")
    endif()
    if(IS_DIRECTORY "${_moq_pin_lib}")
        message(FATAL_ERROR
            "FindMsQuic: pinned MOQ_MSQUIC_PIN_LIBRARY is a directory, not a "
            "library file: ${MOQ_MSQUIC_PIN_LIBRARY}")
    endif()
    if(NOT IS_DIRECTORY "${_moq_pin_inc}")
        message(FATAL_ERROR
            "FindMsQuic: pinned MOQ_MSQUIC_PIN_INCLUDE_DIR is not a directory: "
            "${MOQ_MSQUIC_PIN_INCLUDE_DIR}")
    endif()
    if(NOT EXISTS "${_moq_pin_inc}/msquic.h")
        message(FATAL_ERROR
            "FindMsQuic: pinned MOQ_MSQUIC_PIN_INCLUDE_DIR has no msquic.h: "
            "${MOQ_MSQUIC_PIN_INCLUDE_DIR}")
    endif()
    if(IS_DIRECTORY "${_moq_pin_inc}/msquic.h")
        message(FATAL_ERROR
            "FindMsQuic: pinned MOQ_MSQUIC_PIN_INCLUDE_DIR has a msquic.h that "
            "is a directory: ${MOQ_MSQUIC_PIN_INCLUDE_DIR}/msquic.h")
    endif()

    # Normalize the inputs so a plain -DVAR=value becomes a real typed cache
    # entry rather than staying UNINITIALIZED. The results are normal variables
    # on purpose (see INPUTS vs RESULTS above).
    set(MOQ_MSQUIC_PIN_INCLUDE_DIR "${MOQ_MSQUIC_PIN_INCLUDE_DIR}" CACHE PATH
        "Directory holding msquic.h, pinning one MsQuic build directly" FORCE)
    set(MOQ_MSQUIC_PIN_LIBRARY "${MOQ_MSQUIC_PIN_LIBRARY}" CACHE FILEPATH
        "The MsQuic library file, pinning one MsQuic build directly" FORCE)
    # A pinned tree holds NO cached results, whoever wrote them. Entries left
    # by an earlier search branch would survive a later pin removal exactly as
    # the pin's own would, and send the next configure down the same
    # never-searched-the-root path.
    unset(MOQ_MSQUIC_INCLUDE_DIR CACHE)
    unset(MOQ_MSQUIC_LIBRARY CACHE)
    set(MOQ_MSQUIC_INCLUDE_DIR "${MOQ_MSQUIC_PIN_INCLUDE_DIR}")
    set(MOQ_MSQUIC_LIBRARY "${MOQ_MSQUIC_PIN_LIBRARY}")

    set(MOQ_MSQUIC_DISCOVERY_MODE "pinned")
    set(MsQuic_FOUND TRUE)
endif()

# Neither a caller-supplied target nor a pin: fall through to the two search
# branches.
if(NOT MsQuic_FOUND)
    find_package(msquic CONFIG QUIET)
    if(msquic_FOUND)
        if(TARGET msquic AND NOT TARGET msquic::msquic)
            add_library(msquic::msquic ALIAS msquic)
        endif()
        set(MOQ_MSQUIC_DISCOVERY_MODE "package")
        set(MsQuic_FOUND TRUE)
        set(MOQ_MSQUIC_DISCOVERY_INITIALIZED TRUE CACHE INTERNAL
            "This tree has resolved MsQuic at least once")
        return()
    endif()

    set(MOQ_MSQUIC_ROOT "${CMAKE_CURRENT_LIST_DIR}/../../msquic" CACHE PATH
        "Path to an MsQuic checkout (header at src/inc/msquic.h)")

    find_path(MOQ_MSQUIC_INCLUDE_DIR msquic.h
        PATHS "${MOQ_MSQUIC_ROOT}/src/inc"
        NO_DEFAULT_PATH
    )
    find_library(MOQ_MSQUIC_LIBRARY
        NAMES msquic
        PATHS
            "${MOQ_MSQUIC_ROOT}/build/bin/Release"
            "${MOQ_MSQUIC_ROOT}/build/bin/Debug"
            "${MOQ_MSQUIC_ROOT}/build/bin"
        NO_DEFAULT_PATH
    )

    include(FindPackageHandleStandardArgs)
    find_package_handle_standard_args(MsQuic
        REQUIRED_VARS MOQ_MSQUIC_INCLUDE_DIR MOQ_MSQUIC_LIBRARY
    )

    # Recorded only once the fallback has actually RESOLVED MsQuic. The contract
    # is "which branch resolved it", so a failed root lookup must not claim
    # `root`.
    if(MsQuic_FOUND)
        set(MOQ_MSQUIC_DISCOVERY_MODE "root")
    endif()
endif()

# Recorded only on success. A failed configure leaves the result variables
# holding -NOTFOUND, which is not the caller input the legacy block looks for;
# marking such a tree initialized would retire the migration before it ever
# ran, and the caller's next attempt -- with a complete legacy pair -- would be
# shadowed by whatever package is installed.
if(MsQuic_FOUND)
    set(MOQ_MSQUIC_DISCOVERY_INITIALIZED TRUE CACHE INTERNAL
        "This tree has resolved MsQuic at least once")
endif()

if(MsQuic_FOUND AND NOT TARGET msquic::msquic)
    add_library(msquic::msquic UNKNOWN IMPORTED)
    set_target_properties(msquic::msquic PROPERTIES
        IMPORTED_LOCATION "${MOQ_MSQUIC_LIBRARY}"
        INTERFACE_INCLUDE_DIRECTORIES "${MOQ_MSQUIC_INCLUDE_DIR}"
    )
endif()
