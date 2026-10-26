add_library(moq-cmaf-packager
    src/cmaf_packager.c
)

add_library(moq::cmaf-packager ALIAS moq-cmaf-packager)

target_include_directories(moq-cmaf-packager
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

target_link_libraries(moq-cmaf-packager PUBLIC moq::core PRIVATE moq::codec-signaling)
target_include_directories(moq-cmaf-packager PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}/../codec_signaling/src")
target_compile_features(moq-cmaf-packager PUBLIC c_std_11)

set_target_properties(moq-cmaf-packager PROPERTIES
    C_VISIBILITY_PRESET hidden
    POSITION_INDEPENDENT_CODE ON
    OUTPUT_NAME moq-cmaf-packager
    EXPORT_NAME cmaf-packager
)
if(APPLE)
    set_target_properties(moq-cmaf-packager PROPERTIES INSTALL_RPATH "@loader_path")
elseif(UNIX)
    set_target_properties(moq-cmaf-packager PROPERTIES INSTALL_RPATH "$ORIGIN")
endif()

install(TARGETS moq-cmaf-packager
    EXPORT libmoqTargets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})

install(DIRECTORY include/moq
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    FILES_MATCHING PATTERN "*.h")

target_compile_definitions(moq-cmaf-packager PRIVATE MOQ_BUILDING)

if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
    target_compile_options(moq-cmaf-packager PRIVATE -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
endif()

if(MOQ_BUILD_TESTS)
    add_executable(test_cmaf_packager tests/test_cmaf_packager.c)
    target_link_libraries(test_cmaf_packager PRIVATE moq::cmaf-packager moq::cmaf)
    add_executable(test_cmaf_packager_regression tests/test_cmaf_packager_regression.c)
    target_link_libraries(test_cmaf_packager_regression PRIVATE moq::cmaf-packager)
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        foreach(t test_cmaf_packager test_cmaf_packager_regression)
            target_compile_options(${t} PRIVATE -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endforeach()
    endif()
    add_test(NAME cmaf_packager_regression COMMAND test_cmaf_packager_regression)
    add_test(NAME cmaf_packager COMMAND test_cmaf_packager)
    add_test(NAME cmaf_packager_symbols COMMAND ${CMAKE_COMMAND}
        -DNM=${CMAKE_NM} -DREADELF=${CMAKE_READELF}
        -DELF=$<STREQUAL:${CMAKE_EXECUTABLE_FORMAT},ELF>
        -DLIBRARY=$<TARGET_FILE:moq-cmaf-packager>
        -DSHARED=$<BOOL:${BUILD_SHARED_LIBS}>
        -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_symbols.cmake)
    add_executable(test_cmaf_packager_audio tests/test_cmaf_packager_audio.c)
    target_link_libraries(test_cmaf_packager_audio PRIVATE moq::cmaf-packager
        moq::cmaf moq::codec-signaling)
    target_include_directories(test_cmaf_packager_audio PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}/../codec_signaling/src")
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_cmaf_packager_audio PRIVATE -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    add_test(NAME cmaf_packager_aac_metadata COMMAND test_cmaf_packager_audio aac)
    add_test(NAME cmaf_packager_opus_metadata COMMAND test_cmaf_packager_audio opus)
    if(CMAKE_EXECUTABLE_FORMAT STREQUAL "ELF")
        add_test(NAME cmaf_packager_export_oracle COMMAND ${CMAKE_COMMAND}
            -DCC=${CMAKE_C_COMPILER} -DNM=${CMAKE_NM}
            -DREADELF=${CMAKE_READELF} -DAR=${CMAKE_AR}
            -DCHECK_DIR=${CMAKE_CURRENT_BINARY_DIR}/symbol-fixtures
            -DSOURCE_DIR=${CMAKE_CURRENT_SOURCE_DIR}/tests
            -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_symbols_oracle.cmake)
    endif()
    include("${PROJECT_SOURCE_DIR}/tests/cmake/ConsumerBuildContext.cmake")
    set(packager_context "${CMAKE_CURRENT_BINARY_DIR}/consumer-context.cmake")
    moq_write_consumer_context("${packager_context}")
    if(MOQ_BUILD_PACKAGE_EXPORTS)
    add_test(NAME cmaf_packager_consumer COMMAND ${CMAKE_COMMAND}
        -DBUILD_DIR=${PROJECT_BINARY_DIR} -DCONTEXT=${packager_context}
        -DCHECK_DIR=${CMAKE_CURRENT_BINARY_DIR}/cold-consumer
        -DSOURCE_DIR=${CMAKE_CURRENT_SOURCE_DIR}/tests
        -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_consumer.cmake)
    set_tests_properties(cmaf_packager_consumer PROPERTIES TIMEOUT 90 LABELS "media;packager")
    endif()
    set_tests_properties(cmaf_packager cmaf_packager_regression cmaf_packager_symbols
        PROPERTIES TIMEOUT 90 LABELS "media;packager")
endif()
