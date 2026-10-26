add_library(moq-media-object
    src/media_object.c
)

add_library(moq::media-object ALIAS moq-media-object)

target_include_directories(moq-media-object
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

target_link_libraries(moq-media-object PUBLIC moq::core moq::loc moq::cmaf)
# The mpeg2ts packagings are parsed only when moq::mpeg2ts is in the build.
# Linked privately: no public header of this target includes <moq/mpeg2ts.h>.
if(TARGET moq::mpeg2ts)
    target_link_libraries(moq-media-object PRIVATE moq::mpeg2ts)
    target_compile_definitions(moq-media-object PRIVATE MOQ_MEDIA_OBJECT_HAVE_MPEG2TS)
endif()
target_compile_features(moq-media-object PUBLIC c_std_11)

set_target_properties(moq-media-object PROPERTIES
    C_VISIBILITY_PRESET hidden
    OUTPUT_NAME moq-media-object
    EXPORT_NAME media-object
)

install(TARGETS moq-media-object
    EXPORT libmoqTargets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})

install(DIRECTORY include/moq
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    FILES_MATCHING PATTERN "*.h")

target_compile_definitions(moq-media-object PRIVATE MOQ_BUILDING)

if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
    target_compile_options(moq-media-object PRIVATE -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
elseif(MSVC)
    target_compile_options(moq-media-object PRIVATE $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:/WX>)
endif()

if(MOQ_BUILD_TESTS)
    add_executable(test_media_object tests/test_media_object.c)
    target_link_libraries(test_media_object PRIVATE moq::media-object)
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_media_object PRIVATE -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    add_test(NAME media_object COMMAND test_media_object)

    add_executable(test_media_object_mpeg2ts tests/test_media_object_mpeg2ts.c
        tests/legacy_media_object_client.c)
    target_link_libraries(test_media_object_mpeg2ts PRIVATE moq::media-object)
    target_include_directories(test_media_object_mpeg2ts PRIVATE
        ${PROJECT_SOURCE_DIR}/media/mpeg2ts/tests)
    if(TARGET moq::mpeg2ts)
        target_compile_definitions(test_media_object_mpeg2ts PRIVATE
            MOQ_TEST_HAVE_MPEG2TS)
    endif()
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(test_media_object_mpeg2ts PRIVATE -Wall -Wextra
            -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    add_test(NAME media_object_mpeg2ts COMMAND test_media_object_mpeg2ts)

    foreach(test IN ITEMS media_metadata media_old_client media_metadata_layout
                          media_parsed_init)
        add_executable(test_${test} tests/test_${test}.c)
        target_link_libraries(test_${test} PRIVATE moq::media-object)
        if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
            target_compile_options(test_${test} PRIVATE -Wall -Wextra -Wpedantic
                $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
        endif()
        add_test(NAME ${test} COMMAND test_${test})
    endforeach()
    target_compile_definitions(test_media_metadata_layout PRIVATE
        MOQ_MEDIA_LAYOUT_EXECUTE=1)
endif()
