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
    target_compile_definitions(moq-playback PRIVATE MOQ_PLAYBACK_TESTING)

    add_executable(test_playback_api tests/test_playback_api.c)
    target_link_libraries(test_playback_api PRIVATE moq::playback)
    target_compile_definitions(test_playback_api PRIVATE MOQ_PLAYBACK_TESTING)
    add_test(NAME playback_api COMMAND test_playback_api)

    add_executable(test_playback_oom tests/test_playback_oom.c)
    target_link_libraries(test_playback_oom PRIVATE moq::playback)
    add_test(NAME playback_oom COMMAND test_playback_oom)

    add_executable(test_playback_scenario tests/test_playback_scenario.c)
    target_link_libraries(test_playback_scenario PRIVATE moq::playback)
    target_compile_definitions(test_playback_scenario PRIVATE MOQ_PLAYBACK_TESTING)
    add_test(NAME playback_scenario COMMAND test_playback_scenario)
endif()
