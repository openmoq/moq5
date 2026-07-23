add_library(moq-codec-string
    src/codec_string.c
)

add_library(moq::codec-string ALIAS moq-codec-string)

target_include_directories(moq-codec-string
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

target_link_libraries(moq-codec-string PUBLIC moq::core)
target_compile_features(moq-codec-string PUBLIC c_std_11)

set_target_properties(moq-codec-string PROPERTIES
    C_VISIBILITY_PRESET hidden
    OUTPUT_NAME moq-codec-string
    EXPORT_NAME codec-string
)

install(TARGETS moq-codec-string
    EXPORT libmoqTargets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})

install(DIRECTORY include/moq
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    FILES_MATCHING PATTERN "*.h")

target_compile_definitions(moq-codec-string PRIVATE MOQ_BUILDING)

if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
    target_compile_options(moq-codec-string PRIVATE -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
endif()

if(MOQ_BUILD_TESTS)
    add_executable(test_codec_string
        tests/test_codec_string.c)
    target_link_libraries(test_codec_string PRIVATE moq::codec-string)
    add_test(NAME codec_string COMMAND test_codec_string)
endif()
