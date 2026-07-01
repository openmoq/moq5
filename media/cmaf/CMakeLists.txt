add_library(moq-cmaf
    src/cmaf.c
)

add_library(moq::cmaf ALIAS moq-cmaf)

target_include_directories(moq-cmaf
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

target_link_libraries(moq-cmaf PUBLIC moq::core)
target_compile_features(moq-cmaf PUBLIC c_std_11)

set_target_properties(moq-cmaf PROPERTIES
    C_VISIBILITY_PRESET hidden
    OUTPUT_NAME moq-cmaf
    EXPORT_NAME cmaf
)

install(TARGETS moq-cmaf
    EXPORT libmoqTargets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})

install(DIRECTORY include/moq
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    FILES_MATCHING PATTERN "*.h")

target_compile_definitions(moq-cmaf PRIVATE MOQ_BUILDING)

if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
    target_compile_options(moq-cmaf PRIVATE -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
endif()

if(MOQ_BUILD_TESTS)
    add_executable(test_cmaf
        tests/test_cmaf.c
        ${CMAKE_SOURCE_DIR}/tests/support/media_builders.c)
    target_include_directories(test_cmaf PRIVATE
        ${CMAKE_SOURCE_DIR}/tests/support)
    target_link_libraries(test_cmaf PRIVATE moq::cmaf)
    add_test(NAME cmaf COMMAND test_cmaf)
endif()
