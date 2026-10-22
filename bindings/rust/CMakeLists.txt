# The Rust binding's qualification rows (MOQ_BUILD_RUST_BINDING): cargo over
# an SDK staged from this tree. ON means every row is required -- a missing
# tool or input is a configure error, never a skipped row; the relay rows
# (MOQ_RUST_RELAY_ROWS=OFF), the cross-build row (MOQ_RUST_CROSS_BUILD=OFF)
# and the player's rows (MOQ_RUST_PLAYER=OFF) alone can be left out
# explicitly.
if(NOT MOQ_BUILD_TESTS)
    message(FATAL_ERROR
        "MOQ_BUILD_RUST_BINDING=ON requires MOQ_BUILD_TESTS=ON: the binding is qualified through ctest rows")
endif()
if(NOT TARGET moq::service)
    message(FATAL_ERROR
        "MOQ_BUILD_RUST_BINDING=ON requires MOQ_BUILD_SERVICE=ON: moq5-sys links libmoq-service")
endif()
find_program(MOQ_CARGO cargo)
if(NOT MOQ_CARGO)
    message(FATAL_ERROR "MOQ_BUILD_RUST_BINDING=ON requires cargo on PATH")
endif()
find_program(MOQ_RUSTUP rustup)
set(MOQ_RUST_MSRV "1.85" CACHE STRING
    "The toolchain the rust_msrv row checks with, through `rustup run`")
set(MOQ_RUST_TOOLCHAIN "stable" CACHE STRING
    "The rustup toolchain whose cargo the rust_doc and cross rows run (it owns the foreign targets they add)")
if(NOT MOQ_RUSTUP)
    message(FATAL_ERROR
        "MOQ_BUILD_RUST_BINDING=ON requires rustup: the rust_msrv, rust_doc and cross rows add toolchains and targets through it")
endif()
foreach(_toolchain IN ITEMS "${MOQ_RUST_MSRV}" "${MOQ_RUST_TOOLCHAIN}")
    execute_process(COMMAND "${MOQ_RUSTUP}" run "${_toolchain}" cargo --version
        RESULT_VARIABLE _rc OUTPUT_QUIET ERROR_QUIET)
    if(NOT _rc STREQUAL "0")
        message(FATAL_ERROR
            "MOQ_BUILD_RUST_BINDING=ON needs the rustup toolchain ${_toolchain} (`rustup toolchain install ${_toolchain}`)")
    endif()
endforeach()
find_program(MOQ_PKG_CONFIG pkg-config)
if(NOT MOQ_PKG_CONFIG)
    message(FATAL_ERROR "MOQ_BUILD_RUST_BINDING=ON requires pkg-config on PATH")
endif()
find_program(MOQ_PYTHON3 python3)
if(NOT MOQ_PYTHON3)
    message(FATAL_ERROR
        "MOQ_BUILD_RUST_BINDING=ON requires python3 on PATH: the consumer and package rows classify cargo's structured diagnostics with it")
endif()
set(MOQ_RUST_LIBCLANG_PATH "$ENV{LIBCLANG_PATH}" CACHE PATH
    "The libclang directory bindgen loads (LIBCLANG_PATH); empty lets clang-sys search")
# The rows link an installed SDK. By default this tree is installed into a
# test-owned prefix; a tree whose service has no pkg-config representation
# (the relay's direct MsQuic managed backend, mvfst, proxygen) names an SDK
# built without one instead.
set(MOQ_RUST_SDK_PREFIX "" CACHE PATH
    "An installed SDK with libmoq-service.pc for the rows; empty stages this tree")
# An SDK whose pkg-config closure spans its dependency prefixes (the WTQuic
# managed SDK Requires the wtquic install's own .pc files) names their
# pkg-config directories here; the rows search them beside the SDK's own.
set(MOQ_RUST_SDK_PKG_CONFIG_PATH "" CACHE STRING
    "pkg-config directories (a : list) of the SDK's dependency prefixes, when its closure spans them")
option(MOQ_RUST_RELAY_ROWS
    "Run the real relay rows (needs the moq5-relay target and an SDK with the picoquic backend)" ON)
if(MOQ_RUST_RELAY_ROWS)
    if(NOT TARGET moq5-relay)
        message(FATAL_ERROR
            "MOQ_RUST_RELAY_ROWS=ON needs the moq5-relay target (MOQ_BUILD_RELAY=ON with its inputs); set MOQ_RUST_RELAY_ROWS=OFF to leave the relay rows out explicitly")
    endif()
    if(NOT MOQ_RUST_SDK_PREFIX AND NOT MOQ_BUILD_PQ_THREADED)
        message(FATAL_ERROR
            "MOQ_RUST_RELAY_ROWS=ON needs MOQ_BUILD_PQ_THREADED=ON (the rows connect through the picoquic backend of the staged SDK) or MOQ_RUST_SDK_PREFIX naming an SDK with it")
    endif()
endif()

# The cross rows. The refusal row needs only a foreign target's Rust
# standard library (the row adds it through rustup): a cross build that
# names no target SDK or sysroot is refused before anything is probed. The
# build row configures a target SDK from this source tree with
# MOQ_RUST_CROSS_BUILD_CMAKE_ARGS and checks the crates for that target
# against it; its defaults cover the other macOS architecture.
if(CMAKE_HOST_SYSTEM_NAME STREQUAL "Darwin")
    if(CMAKE_HOST_SYSTEM_PROCESSOR STREQUAL "arm64")
        set(_other_arch x86_64)
        set(_other_triple x86_64-apple-darwin)
    else()
        set(_other_arch arm64)
        set(_other_triple aarch64-apple-darwin)
    endif()
    set(_cross_build_default ON)
    set(_cross_args_default "-DCMAKE_OSX_ARCHITECTURES=${_other_arch}")
    if(CMAKE_OSX_SYSROOT)
        set(_cross_sysroot_default "${CMAKE_OSX_SYSROOT}")
    else()
        execute_process(COMMAND xcrun --show-sdk-path
            OUTPUT_VARIABLE _cross_sysroot_default OUTPUT_STRIP_TRAILING_WHITESPACE)
    endif()
elseif(CMAKE_HOST_SYSTEM_NAME STREQUAL "Linux")
    if(CMAKE_HOST_SYSTEM_PROCESSOR STREQUAL "x86_64")
        set(_other_triple aarch64-unknown-linux-gnu)
    else()
        set(_other_triple x86_64-unknown-linux-gnu)
    endif()
    set(_cross_build_default OFF)
    set(_cross_args_default "")
    set(_cross_sysroot_default "")
else()
    set(_other_triple "")
    set(_cross_build_default OFF)
    set(_cross_args_default "")
    set(_cross_sysroot_default "")
endif()
set(MOQ_RUST_CROSS_REFUSAL_TARGET "${_other_triple}" CACHE STRING
    "A target triple other than the host's for the rust_cross_refusal row")
if(NOT MOQ_RUST_CROSS_REFUSAL_TARGET)
    message(FATAL_ERROR
        "MOQ_BUILD_RUST_BINDING=ON needs MOQ_RUST_CROSS_REFUSAL_TARGET (a foreign target triple) for the rust_cross_refusal row on this host")
endif()
option(MOQ_RUST_CROSS_BUILD
    "Run the rust_cross_build row: a target SDK from this tree, the crates checked against it" ${_cross_build_default})
set(MOQ_RUST_CROSS_BUILD_TARGET "${_other_triple}" CACHE STRING
    "The target triple the rust_cross_build row checks the crates for")
set(MOQ_RUST_CROSS_BUILD_SYSROOT "${_cross_sysroot_default}" CACHE PATH
    "The target sysroot the rust_cross_build row hands to libclang")
set(MOQ_RUST_CROSS_BUILD_CMAKE_ARGS "${_cross_args_default}" CACHE STRING
    "Configure arguments (a ; list) that make this tree build for MOQ_RUST_CROSS_BUILD_TARGET")
if(MOQ_RUST_CROSS_BUILD)
    if(NOT MOQ_RUST_CROSS_BUILD_TARGET OR NOT MOQ_RUST_CROSS_BUILD_SYSROOT)
        message(FATAL_ERROR
            "MOQ_RUST_CROSS_BUILD=ON needs MOQ_RUST_CROSS_BUILD_TARGET and MOQ_RUST_CROSS_BUILD_SYSROOT; set MOQ_RUST_CROSS_BUILD=OFF to leave the cross-build row out explicitly")
    endif()
endif()

# What the SDK carries beyond the service, stated so that a feature it lacks
# is never a skipped row: the media player (moq5's `player` feature, over
# <moq/media_player.h>; MOQ_BUILD_PLAYBACK) and MPEG-TS (MOQ_BUILD_MPEG2TS).
# Each defaults to what this tree builds, or for a named SDK to the headers
# its own pkg-config metadata places (scripts/sdk_features.cmake), and every
# row checks the statement against the SDK it links. MOQ_RUST_PLAYER=OFF
# leaves the player's rows out explicitly; MOQ_RUST_MPEG2TS is the outcome
# the MPEG-TS relay rows require: the stream, or the SDK's refusal.
include("${CMAKE_CURRENT_SOURCE_DIR}/scripts/sdk_features.cmake")
set(_rust_player_default OFF)
set(_rust_mpeg2ts_default OFF)
if(MOQ_RUST_SDK_PREFIX)
    moq_rust_sdk_carries("${MOQ_RUST_SDK_PREFIX}" "${MOQ_PKG_CONFIG}" "${MOQ_RUST_SDK_PKG_CONFIG_PATH}"
        _rust_player_default _rust_mpeg2ts_default _rust_sdk_includedir)
    message(STATUS "Rust rows: ${MOQ_RUST_SDK_PREFIX} keeps its headers in "
        "'${_rust_sdk_includedir}' (player ${_rust_player_default}, MPEG-TS ${_rust_mpeg2ts_default})")
else()
    if(TARGET moq::playback)
        set(_rust_player_default ON)
    endif()
    if(TARGET moq::mpeg2ts)
        set(_rust_mpeg2ts_default ON)
    endif()
endif()
option(MOQ_RUST_PLAYER
    "Qualify moq5's player feature (needs an SDK with the player: MOQ_BUILD_PLAYBACK=ON)" ${_rust_player_default})
option(MOQ_RUST_MPEG2TS
    "The SDK carries MPEG-TS (MOQ_BUILD_MPEG2TS=ON): the MPEG-TS relay rows require the stream, not the refusal"
    ${_rust_mpeg2ts_default})
if(NOT MOQ_RUST_SDK_PREFIX)
    if(MOQ_RUST_PLAYER AND NOT TARGET moq::playback)
        message(FATAL_ERROR
            "MOQ_RUST_PLAYER=ON needs the player in the staged SDK (MOQ_BUILD_PLAYBACK=ON); set MOQ_RUST_PLAYER=OFF to leave the player's rows out explicitly")
    endif()
    if(MOQ_RUST_MPEG2TS AND NOT TARGET moq::mpeg2ts)
        message(FATAL_ERROR
            "MOQ_RUST_MPEG2TS=ON states that the staged SDK carries MPEG-TS, which needs MOQ_BUILD_MPEG2TS=ON")
    endif()
    if(NOT MOQ_RUST_MPEG2TS AND TARGET moq::mpeg2ts)
        message(FATAL_ERROR
            "MOQ_RUST_MPEG2TS=OFF states that the staged SDK refuses MPEG-TS, but this tree builds it (MOQ_BUILD_MPEG2TS=ON)")
    endif()
endif()
if(MOQ_RUST_PLAYER)
    set(_rust_player 1)
else()
    set(_rust_player 0)
endif()
if(MOQ_RUST_MPEG2TS)
    set(_rust_mpeg2ts 1)
else()
    set(_rust_mpeg2ts 0)
endif()

set(_rust_src "${CMAKE_CURRENT_SOURCE_DIR}")
set(_rust_work "${CMAKE_CURRENT_BINARY_DIR}/work")
if(MOQ_RUST_SDK_PREFIX)
    set(_rust_sdk "${MOQ_RUST_SDK_PREFIX}")
    set(_rust_stage "-DEXISTING=ON")
else()
    set(_rust_sdk "${_rust_work}/sdk")
    set(_rust_stage "-DEXISTING=OFF")
endif()
set(_rust_env
    "CARGO=${MOQ_CARGO}"
    "RUSTUP=${MOQ_RUSTUP}"
    "PKG_CONFIG=${MOQ_PKG_CONFIG}"
    "PYTHON=${MOQ_PYTHON3}"
    "CC=${CMAKE_C_COMPILER}"
    "MOQ5_MSRV_TOOLCHAIN=${MOQ_RUST_MSRV}"
    "MOQ5_RUST_TOOLCHAIN=${MOQ_RUST_TOOLCHAIN}"
    "MOQ5_RUST_WORK=${_rust_work}"
    "MOQ5_RUST_SDK=${_rust_sdk}"
    "MOQ5_RUST_SOURCE=${PROJECT_SOURCE_DIR}"
    "MOQ5_RUST_PLAYER=${_rust_player}"
    "MOQ5_RUST_MPEG2TS=${_rust_mpeg2ts}")
if(MOQ_RUST_LIBCLANG_PATH)
    list(APPEND _rust_env "LIBCLANG_PATH=${MOQ_RUST_LIBCLANG_PATH}")
endif()
if(MOQ_RUST_SDK_PKG_CONFIG_PATH)
    list(APPEND _rust_env "MOQ5_SDK_PKG_CONFIG_PATH=${MOQ_RUST_SDK_PKG_CONFIG_PATH}")
endif()
# The relay rows' native peer includes the installed picoquic_threaded.h,
# which includes picoquic's own headers; a picoquic SDK built from a source
# tree does not install those, so the peer compiles with that tree's include
# directory beside the SDK closure's.
if(MOQ_PICOQUIC_SOURCE_DIR)
    list(APPEND _rust_env "MOQ5_RELAY_PEER_CFLAGS=-I${MOQ_PICOQUIC_SOURCE_DIR}/picoquic")
endif()

# The SDK the rows consume, staged from this tree or named, and required to
# carry libmoq-service.pc (the configurations the binding links).
add_test(NAME rust_sdk_install
    COMMAND ${CMAKE_COMMAND}
        "-DBUILD=${PROJECT_BINARY_DIR}" "-DPREFIX=${_rust_sdk}" "-DCONFIG=$<CONFIG>"
        ${_rust_stage} -P "${_rust_src}/scripts/stage_sdk.cmake")
set_tests_properties(rust_sdk_install PROPERTIES
    FIXTURES_SETUP rust_sdk TIMEOUT 600 LABELS "rust")

function(moq_rust_row name timeout)
    add_test(NAME rust_${name}
        COMMAND bash "${_rust_src}/scripts/ctest_row.sh" ${name} ${ARGN})
    set_tests_properties(rust_${name} PROPERTIES
        ENVIRONMENT "${_rust_env}" FIXTURES_REQUIRED rust_sdk
        TIMEOUT ${timeout} RUN_SERIAL TRUE LABELS "rust")
endfunction()

moq_rust_row(fmt 120)
moq_rust_row(clippy 900)
moq_rust_row(doc 900)
moq_rust_row(tests 900)
moq_rust_row(fake_tests 900)
moq_rust_row(fake_asan 900)
moq_rust_row(layout 900)
if(BUILD_SHARED_LIBS)
    moq_rust_row(installed_consumer_shared 900)
else()
    moq_rust_row(installed_consumer_static 900)
endif()
moq_rust_row(package 900)
moq_rust_row(msrv 900)
moq_rust_row(gate_controls 300)
moq_rust_row(cross_refusal 600 "${MOQ_RUST_CROSS_REFUSAL_TARGET}")
if(MOQ_RUST_CROSS_BUILD)
    moq_rust_row(cross_build 1800
        "${MOQ_RUST_CROSS_BUILD_TARGET}" "${MOQ_RUST_CROSS_BUILD_SYSROOT}" ${MOQ_RUST_CROSS_BUILD_CMAKE_ARGS})
endif()
if(MOQ_RUST_RELAY_ROWS)
    moq_rust_row(relay_rows 1200 "$<TARGET_FILE:moq5-relay>")
    set_tests_properties(rust_relay_rows PROPERTIES LABELS "rust;relay")
endif()
