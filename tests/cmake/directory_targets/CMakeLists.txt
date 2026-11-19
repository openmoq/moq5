# Configure-only model of moq_directory_targets() (cmake/MoqDirectoryTargets.cmake):
# which targets get the declared values, and where in their lists. Nothing is
# compiled. Each target's final lists are written at generate time for
# DirectoryTargetsTest.cmake.
#   caller/                a caller (as a superproject would) gives its
#                          directory an option and an include directory
#   caller/declaring/      declares, inheriting them
#   MOQ_DEPENDENCY         a dependency's directory that declares first and
#                          then calls include_directories() itself, as picotls
#                          does under LibMoQ's policy (written by the test)
#   MOQ_INCLUDES_BEFORE    CMAKE_INCLUDE_DIRECTORIES_BEFORE for the whole model
#   MOQ_MODEL              twice: a second declaration, to be refused
cmake_minimum_required(VERSION 3.20)
project(moq_directory_targets_model LANGUAGES C)
include("${MOQ_SOURCE}/cmake/MoqDirectoryTargets.cmake")
file(WRITE "${CMAKE_BINARY_DIR}/probe.c" "int moq_directory_targets_probe(void) { return 0; }\n")
set(probe "${CMAKE_BINARY_DIR}/probe.c")
set(CMAKE_INCLUDE_DIRECTORIES_BEFORE "${MOQ_INCLUDES_BEFORE}")

if(MOQ_MODEL STREQUAL "twice")
    moq_directory_targets(COMPILE_OPTIONS -DFIRST)
    moq_directory_targets(COMPILE_OPTIONS -DSECOND)
    return()
endif()

add_subdirectory(caller)
add_subdirectory("${MOQ_DEPENDENCY}" dependency)

foreach(target IN ITEMS earlier earlier_sub bare own later_sub last dependency_lib)
    file(GENERATE OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/${target}.txt" CONTENT
        "options=$<JOIN:$<TARGET_PROPERTY:${target},COMPILE_OPTIONS>,|>\nincludes=$<JOIN:$<TARGET_PROPERTY:${target},INCLUDE_DIRECTORIES>,|>\ndefinitions=$<JOIN:$<TARGET_PROPERTY:${target},COMPILE_DEFINITIONS>,|>\n")
endforeach()
file(GENERATE OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/interface_only.txt" CONTENT
    "options=$<JOIN:$<TARGET_PROPERTY:interface_only,INTERFACE_COMPILE_OPTIONS>,|>\nincludes=$<JOIN:$<TARGET_PROPERTY:interface_only,INTERFACE_INCLUDE_DIRECTORIES>,|>\ndefinitions=$<JOIN:$<TARGET_PROPERTY:interface_only,INTERFACE_COMPILE_DEFINITIONS>,|>\n")
