add_library(moq-locmaf
    src/locmaf_wire.c
    src/locmaf_decode.c
    src/locmaf_canon.c
    src/locmaf_write.c
    src/locmaf_chunk.c
    src/locmaf_encode.c
)

add_library(moq::locmaf ALIAS moq-locmaf)

target_include_directories(moq-locmaf
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

# <moq/locmaf.h> includes <moq/cmaf.h> for moq_locmaf_track_from_cmaf().
target_link_libraries(moq-locmaf PUBLIC moq::core moq::cmaf)
target_compile_features(moq-locmaf PUBLIC c_std_11)

set_target_properties(moq-locmaf PROPERTIES
    C_VISIBILITY_PRESET hidden
    OUTPUT_NAME moq-locmaf
    EXPORT_NAME locmaf
)

install(TARGETS moq-locmaf
    EXPORT libmoqTargets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})

install(DIRECTORY include/moq
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    FILES_MATCHING PATTERN "*.h")

target_compile_definitions(moq-locmaf PRIVATE MOQ_BUILDING)

if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
    target_compile_options(moq-locmaf PRIVATE -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
elseif(MSVC)
    target_compile_options(moq-locmaf PRIVATE $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:/WX>)
endif()

if(MOQ_BUILD_TESTS)
    set(_locmaf_test_support
        tests/locmaf_fixtures.c
        ${PROJECT_SOURCE_DIR}/tests/support/media_builders.c)

    function(moq_locmaf_test name)
        add_executable(test_${name} tests/test_${name}.c ${_locmaf_test_support})
        target_include_directories(test_${name} PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/tests
            ${PROJECT_SOURCE_DIR}/tests/support)
        target_link_libraries(test_${name} PRIVATE moq::locmaf ${ARGN})
        if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
            target_compile_options(test_${name} PRIVATE -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()
        add_test(NAME ${name} COMMAND test_${name})
        set_tests_properties(${name} PROPERTIES LABELS "media;locmaf")
    endfunction()

    moq_locmaf_test(locmaf_wire)
    moq_locmaf_test(locmaf_abi)
    moq_locmaf_test(locmaf_decode)
    moq_locmaf_test(locmaf_write)
    moq_locmaf_test(locmaf_cenc)
    moq_locmaf_test(locmaf_encode)
    moq_locmaf_test(locmaf_roundtrip)
    if(TARGET moq-cmaf-packager)
        moq_locmaf_test(locmaf_packager_roundtrip moq::cmaf-packager)
    endif()

    # The functions kept out of the public API, built from the library's
    # sources because a shared build does not export them.
    add_executable(test_locmaf_internal tests/test_locmaf_internal.c
        src/locmaf_wire.c src/locmaf_decode.c src/locmaf_canon.c
        src/locmaf_write.c src/locmaf_chunk.c src/locmaf_encode.c)
    target_compile_definitions(test_locmaf_internal PRIVATE MOQ_BUILDING)
    target_include_directories(test_locmaf_internal PRIVATE include src)
    target_link_libraries(test_locmaf_internal PRIVATE moq::core moq::cmaf)
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_locmaf_internal PRIVATE -Wall -Wextra
            -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    add_test(NAME locmaf_internal COMMAND test_locmaf_internal)
    set_tests_properties(locmaf_internal PROPERTIES LABELS "media;locmaf")

    add_test(NAME locmaf_symbols COMMAND ${CMAKE_COMMAND}
        -DNM=${CMAKE_NM} -DREADELF=${CMAKE_READELF}
        -DELF=$<STREQUAL:${CMAKE_EXECUTABLE_FORMAT},ELF>
        -DLIBRARY=$<TARGET_FILE:moq-locmaf>
        -DSHARED=$<BOOL:${BUILD_SHARED_LIBS}>
        -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_symbols.cmake)
    set_tests_properties(locmaf_symbols PROPERTIES TIMEOUT 90 LABELS "media;locmaf")
    if(CMAKE_EXECUTABLE_FORMAT STREQUAL "ELF")
        add_test(NAME locmaf_export_oracle COMMAND ${CMAKE_COMMAND}
            -DCC=${CMAKE_C_COMPILER} -DNM=${CMAKE_NM}
            -DREADELF=${CMAKE_READELF} -DAR=${CMAKE_AR}
            -DOBJCOPY=${CMAKE_OBJCOPY}
            -DCHECK_DIR=${CMAKE_CURRENT_BINARY_DIR}/symbol-fixtures
            -DSOURCE_DIR=${CMAKE_CURRENT_SOURCE_DIR}/tests
            -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_symbols_oracle.cmake)
        set_tests_properties(locmaf_export_oracle PROPERTIES TIMEOUT 90 LABELS "media;locmaf")
    endif()
    # The installed package, found and linked by a cold CMake consumer.
    if(MOQ_BUILD_PACKAGE_EXPORTS)
        include("${PROJECT_SOURCE_DIR}/tests/cmake/ConsumerBuildContext.cmake")
        set(locmaf_context "${CMAKE_CURRENT_BINARY_DIR}/consumer-context.cmake")
        moq_write_consumer_context("${locmaf_context}")
        add_test(NAME locmaf_consumer COMMAND ${CMAKE_COMMAND}
            -DBUILD_DIR=${PROJECT_BINARY_DIR} -DCONTEXT=${locmaf_context}
            -DCHECK_DIR=${CMAKE_CURRENT_BINARY_DIR}/cold-consumer
            -DSOURCE_DIR=${CMAKE_CURRENT_SOURCE_DIR}/tests
            -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_consumer.cmake)
        set_tests_properties(locmaf_consumer PROPERTIES TIMEOUT 90 LABELS "media;locmaf")
    endif()
endif()
