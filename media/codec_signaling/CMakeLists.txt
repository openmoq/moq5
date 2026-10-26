add_library(moq-codec-signaling
    src/codec_signaling.c
)

add_library(moq::codec-signaling ALIAS moq-codec-signaling)

target_include_directories(moq-codec-signaling
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

target_link_libraries(moq-codec-signaling PUBLIC moq::core)
target_compile_features(moq-codec-signaling PUBLIC c_std_11)

set_target_properties(moq-codec-signaling PROPERTIES
    C_VISIBILITY_PRESET hidden
    OUTPUT_NAME moq-codec-signaling
    EXPORT_NAME codec-signaling
)

install(TARGETS moq-codec-signaling
    EXPORT libmoqTargets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})

install(DIRECTORY include/moq
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    FILES_MATCHING PATTERN "*.h")

target_compile_definitions(moq-codec-signaling PRIVATE MOQ_BUILDING)

if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
    target_compile_options(moq-codec-signaling PRIVATE -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
elseif(MSVC)
    target_compile_options(moq-codec-signaling PRIVATE $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:/WX>)
endif()

if(MOQ_BUILD_TESTS)
    add_executable(test_codec_signaling
        tests/test_codec_signaling.c)
    target_link_libraries(test_codec_signaling PRIVATE moq::codec-signaling)
    add_test(NAME codec_signaling COMMAND test_codec_signaling)

    # Conformance vectors constructed from the primary specifications rather
    # than from producer samples, so a parser can be wrong in a band the
    # samples never visit and still be caught. Kept as its own executable so
    # the donor suite stays recognizable.
    add_executable(test_codec_signaling_spec
        tests/test_codec_signaling_spec.c)
    target_link_libraries(test_codec_signaling_spec PRIVATE moq::codec-signaling)
    add_test(NAME codec_signaling_spec COMMAND test_codec_signaling_spec)

    # The tests are owned code and carry the same warning policy as the
    # library they exercise.
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        foreach(t test_codec_signaling test_codec_signaling_spec)
            target_compile_options(${t} PRIVATE
                -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endforeach()
    endif()
endif()
