cmake_minimum_required(VERSION 3.26)
project(moq5_python VERSION 0.1.0 LANGUAGES C)

find_package(Python3 3.12 REQUIRED COMPONENTS Interpreter Development.SABIModule)
find_package(libmoq 0.1 CONFIG REQUIRED COMPONENTS service)

option(MOQ_PYTHON_BUILD_TESTS "Build the isolated, non-installed bridge test provider" OFF)
option(MOQ_PYTHON_WARNINGS_AS_ERRORS "Treat binding warnings as errors" OFF)

function(moq5_python_module target)
    Python3_add_library(${target} MODULE USE_SABI 3.12 WITH_SOABI ${ARGN})
    target_compile_features(${target} PRIVATE c_std_11)
    set_target_properties(${target} PROPERTIES OUTPUT_NAME _native C_VISIBILITY_PRESET hidden)
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        target_compile_options(${target} PRIVATE -Wall -Wextra -Wpedantic
            $<$<BOOL:${MOQ_PYTHON_WARNINGS_AS_ERRORS}>:-Werror>)
    endif()
    # Static SDK objects retain their public C annotations. Only the Python
    # initializer belongs in this module's export surface.
    if(APPLE)
        target_link_options(${target} PRIVATE "LINKER:-exported_symbol,_PyInit__native")
        if(CMAKE_C_COMPILER_ID STREQUAL "GNU")
            # Our explicit module export list must not gain GCC runtime exports.
            target_link_options(${target} PRIVATE -nodefaultexport)
        endif()
        target_link_options(${target} PRIVATE
            $<$<BOOL:${MOQ_PYTHON_WARNINGS_AS_ERRORS}>:LINKER:-fatal_warnings>)
    elseif(CMAKE_SYSTEM_NAME STREQUAL "Linux")
        target_link_options(${target} PRIVATE
            "LINKER:--version-script,${CMAKE_CURRENT_SOURCE_DIR}/native/exports.map"
            $<$<BOOL:${MOQ_PYTHON_WARNINGS_AS_ERRORS}>:LINKER:--fatal-warnings>)
        set_property(TARGET ${target} APPEND PROPERTY LINK_DEPENDS
            "${CMAKE_CURRENT_SOURCE_DIR}/native/exports.map")
    endif()
endfunction()

moq5_python_module(_native native/module.c)
target_link_libraries(_native PRIVATE moq::service)
install(TARGETS _native LIBRARY DESTINATION moq5 RUNTIME DESTINATION moq5)
install(DIRECTORY src/moq5/ DESTINATION moq5 FILES_MATCHING PATTERN "*.py" PATTERN "py.typed")

if(MOQ_PYTHON_BUILD_TESTS)
    enable_testing()
    find_package(Threads REQUIRED)
    moq5_python_module(_native_fixture native/module.c tests/fake_service.c)
    target_compile_definitions(_native_fixture PRIVATE MOQ_PYTHON_TESTING=1)
    target_include_directories(_native_fixture PRIVATE tests
        $<TARGET_PROPERTY:moq::service,INTERFACE_INCLUDE_DIRECTORIES>)
    target_link_libraries(_native_fixture PRIVATE moq::core Threads::Threads)
    set_target_properties(_native_fixture PROPERTIES
        LIBRARY_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/test-package/$<CONFIG>/moq5")
    file(GLOB python_sources CONFIGURE_DEPENDS "src/moq5/*.py")
    foreach(source IN LISTS python_sources)
        get_filename_component(name "${source}" NAME)
        file(GENERATE OUTPUT "test-package/$<CONFIG>/moq5/${name}" INPUT "${source}")
    endforeach()
    add_test(NAME python_foundation COMMAND Python3::Interpreter -I -W error
        "${CMAKE_CURRENT_SOURCE_DIR}/tests/run_tests.py"
        "${CMAKE_CURRENT_BINARY_DIR}/test-package/$<CONFIG>")
    set_tests_properties(python_foundation PROPERTIES TIMEOUT 30)
endif()
