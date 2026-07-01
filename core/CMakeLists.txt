add_library(moq-core
    src/version.c
    src/types.c
    src/wire/quic_varint.c
    src/wire/vi64.c
    src/wire/kvp.c
    src/wire/buf.c
    src/wire/control_d16.c
    src/wire/control_d18.c
    src/base/rcbuf.c
    src/session/session.c
    src/session/session_setup.c
    src/session/session_subscribe.c
    src/session/session_fetch.c
    src/session/session_publish.c
    src/session/session_track_status.c
    src/session/session_namespace.c
    src/session/session_namespace_sub.c
    src/session/session_subscribe_tracks.c
    src/session/session_subgroup.c
    src/session/session_receive.c
    src/session/session_auth.c
    src/session/profile_d16.c
    src/session/profile_d18.c
    src/facade/publisher.c
    src/facade/subscriber.c
    src/bridge/transport_bridge.c
    src/url.c
)

add_library(moq::core ALIAS moq-core)

target_include_directories(moq-core
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

target_compile_features(moq-core PUBLIC c_std_11)

set_target_properties(moq-core PROPERTIES
    C_VISIBILITY_PRESET hidden
    OUTPUT_NAME moq-core
    EXPORT_NAME core
)

install(TARGETS moq-core
    EXPORT libmoqTargets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})

install(DIRECTORY include/moq
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    FILES_MATCHING PATTERN "*.h"
    PATTERN "transport_bridge.h" EXCLUDE)

target_compile_definitions(moq-core PRIVATE MOQ_BUILDING)

if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
    target_compile_options(moq-core PRIVATE -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
endif()

# Static library for white-box tests that access internal symbols.
# Identical sources but without hidden visibility. Not installed.
if(MOQ_BUILD_TESTS)
    add_library(moq-core-test-internals STATIC
        src/version.c src/types.c
        src/wire/quic_varint.c src/wire/vi64.c src/wire/kvp.c src/wire/buf.c
        src/wire/control_d16.c src/wire/control_d18.c
        src/base/rcbuf.c
        src/session/session.c src/session/session_setup.c
        src/session/session_subscribe.c src/session/session_fetch.c
        src/session/session_publish.c src/session/session_track_status.c
        src/session/session_namespace.c src/session/session_namespace_sub.c
        src/session/session_subscribe_tracks.c
        src/session/session_subgroup.c src/session/session_receive.c
        src/session/session_auth.c src/session/profile_d16.c
        src/session/profile_d18.c
        src/facade/publisher.c src/facade/subscriber.c
        src/bridge/transport_bridge.c
        src/url.c
    )
    target_include_directories(moq-core-test-internals
        PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    )
    target_compile_features(moq-core-test-internals PUBLIC c_std_11)
endif()
