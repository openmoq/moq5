add_library(moq-playback
    src/playback.c
)

add_library(moq::playback ALIAS moq-playback)

target_include_directories(moq-playback
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

target_link_libraries(moq-playback PUBLIC moq::core moq::media-object)
target_compile_features(moq-playback PUBLIC c_std_11)

set_target_properties(moq-playback PROPERTIES
    C_VISIBILITY_PRESET hidden
    OUTPUT_NAME moq-playback
    EXPORT_NAME playback
)

install(TARGETS moq-playback
    EXPORT libmoqTargets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})

install(DIRECTORY include/moq
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    FILES_MATCHING PATTERN "*.h")

target_compile_definitions(moq-playback PRIVATE MOQ_BUILDING)

if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
    target_compile_options(moq-playback PRIVATE -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
endif()

if(MOQ_BUILD_TESTS)
    # Static library for white-box tests that call the moq_playback_test_*
    # helpers. Same source as moq-playback, built with MOQ_PLAYBACK_TESTING and
    # without hidden visibility, so the helpers link in static and shared
    # builds alike while the shipped library never contains them. Not
    # installed, not exported.
    add_library(moq-playback-test-internals STATIC src/playback.c)
    target_include_directories(moq-playback-test-internals
        PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>)
    target_link_libraries(moq-playback-test-internals
        PUBLIC moq::core moq::media-object)
    target_compile_features(moq-playback-test-internals PUBLIC c_std_11)
    target_compile_definitions(moq-playback-test-internals
        PRIVATE MOQ_PLAYBACK_TESTING)
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(moq-playback-test-internals PRIVATE
            -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()

    add_executable(test_playback_api tests/test_playback_api.c)
    target_link_libraries(test_playback_api PRIVATE moq-playback-test-internals)
    target_compile_definitions(test_playback_api PRIVATE MOQ_PLAYBACK_TESTING)
    add_test(NAME playback_api COMMAND test_playback_api)

    add_executable(test_playback_oom tests/test_playback_oom.c)
    target_link_libraries(test_playback_oom PRIVATE moq::playback)
    add_test(NAME playback_oom COMMAND test_playback_oom)

    add_executable(test_playback_scenario tests/test_playback_scenario.c)
    target_link_libraries(test_playback_scenario PRIVATE moq-playback-test-internals)
    target_compile_definitions(test_playback_scenario PRIVATE MOQ_PLAYBACK_TESTING)
    add_test(NAME playback_scenario COMMAND test_playback_scenario)
endif()
