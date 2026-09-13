add_library(moq-cmaf-packager
    src/cmaf_packager.c
)

add_library(moq::cmaf-packager ALIAS moq-cmaf-packager)

target_include_directories(moq-cmaf-packager
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

# moq::cmaf for moq_cmaf_codec_kind_t (public header), and moq::core through it.
target_link_libraries(moq-cmaf-packager PUBLIC moq::cmaf)
target_compile_features(moq-cmaf-packager PUBLIC c_std_11)

set_target_properties(moq-cmaf-packager PROPERTIES
    C_VISIBILITY_PRESET hidden
    OUTPUT_NAME moq-cmaf-packager
    EXPORT_NAME cmaf-packager
)

install(TARGETS moq-cmaf-packager
    EXPORT libmoqTargets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})

install(DIRECTORY include/moq
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    FILES_MATCHING PATTERN "*.h")

target_compile_definitions(moq-cmaf-packager PRIVATE MOQ_BUILDING)

if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
    target_compile_options(moq-cmaf-packager PRIVATE -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
endif()

if(MOQ_BUILD_TESTS)
    add_executable(test_cmaf_packager
        tests/test_cmaf_packager.c)
    # The round-trip tests parse what the packager writes.
    target_link_libraries(test_cmaf_packager PRIVATE moq::cmaf-packager moq::cmaf)
    add_test(NAME cmaf_packager COMMAND test_cmaf_packager)
endif()
