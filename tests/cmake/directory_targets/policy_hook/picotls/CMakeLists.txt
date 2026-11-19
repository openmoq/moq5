# Stand-in for a picotls that LibMoQ's policy is injected into after a caller's
# hook. Each target's final lists, and the platform the policy saw, are written
# for DirectoryTargetsTest.cmake.
cmake_minimum_required(VERSION 3.20)
project(picotls C)
file(WRITE "${CMAKE_BINARY_DIR}/probe.c" "int moq_picotls_model(void) { return 0; }\n")
add_library(picotls-core STATIC "${CMAKE_BINARY_DIR}/probe.c")
file(WRITE "${CMAKE_BINARY_DIR}/platform.txt" "msvc=${MSVC}\nwin32=${WIN32}\n")
foreach(target IN ITEMS caller_helper picotls-core)
    file(GENERATE OUTPUT "${CMAKE_BINARY_DIR}/${target}.txt" CONTENT
        "options=$<JOIN:$<TARGET_PROPERTY:${target},COMPILE_OPTIONS>,|>\nincludes=$<JOIN:$<TARGET_PROPERTY:${target},INCLUDE_DIRECTORIES>,|>\ndefinitions=$<JOIN:$<TARGET_PROPERTY:${target},COMPILE_DEFINITIONS>,|>\n")
endforeach()
