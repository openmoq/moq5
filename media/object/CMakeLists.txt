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
endif()

if(MOQ_BUILD_TESTS)
    add_executable(test_media_object tests/test_media_object.c)
    target_link_libraries(test_media_object PRIVATE moq::media-object)
    add_test(NAME media_object COMMAND test_media_object)
endif()
