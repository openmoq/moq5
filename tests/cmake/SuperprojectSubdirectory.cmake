# Configures a throwaway superproject that pulls libmoq in through
# add_subdirectory() with the parent build's options, then checks that
# nothing libmoq generated points at the superproject's own source or build
# root. Inside a superproject CMAKE_SOURCE_DIR / CMAKE_BINARY_DIR name the
# superproject, so a libmoq path built from them lands outside libmoq.
#
# Checked outputs: every target's sources and include directories (CMake
# File API codemodel), libmoq's CTestTestfile.cmake and cmake_install.cmake
# files, and the files present directly under the superproject build root.
# Paths under the libmoq binary subdirectory are libmoq's own; _deps/ is
# where a find module adds provider source trees, which belongs to the
# top-level build by design.
#
# Inputs: MOQ_SOURCE (libmoq checkout), MOQ_WORK (recreated),
# MOQ_CONTEXT (cache preload: toolchain plus the parent's libmoq options and
# provider hints), optional MOQ_GENERATOR.
cmake_minimum_required(VERSION 3.20)

foreach(var MOQ_SOURCE MOQ_WORK MOQ_CONTEXT)
    if(NOT DEFINED ${var} OR "${${var}}" STREQUAL "")
        message(FATAL_ERROR "${var} is required")
    endif()
endforeach()

set(super_src "${MOQ_WORK}/super-src")
set(super_bin "${MOQ_WORK}/super-build")
set(sub_bin "${super_bin}/libmoq-sub")
file(REMOVE_RECURSE "${MOQ_WORK}")
file(MAKE_DIRECTORY "${super_src}" "${super_bin}/.cmake/api/v1/query")
file(WRITE "${super_bin}/.cmake/api/v1/query/codemodel-v2" "")
file(WRITE "${super_src}/CMakeLists.txt"
"cmake_minimum_required(VERSION 3.20)
project(moq_superproject LANGUAGES C)
add_subdirectory([==[${MOQ_SOURCE}]==] libmoq-sub)
")

set(generator_args "")
if(DEFINED MOQ_GENERATOR AND NOT MOQ_GENERATOR STREQUAL "")
    set(generator_args -G "${MOQ_GENERATOR}")
endif()
execute_process(
    COMMAND "${CMAKE_COMMAND}" ${generator_args} -C "${MOQ_CONTEXT}"
        -S "${super_src}" -B "${super_bin}"
    RESULT_VARIABLE rc
    OUTPUT_VARIABLE out
    ERROR_VARIABLE out)
# The nested configure's status and whole output are kept and shown even on
# success, so its diagnostics can be judged after the work tree is gone.
file(WRITE "${MOQ_WORK}/logs/superproject.configure.log" "${out}")
file(WRITE "${MOQ_WORK}/logs/superproject.configure.status" "${rc}\n")
message(STATUS "superproject configure exit ${rc}:\n${out}")
if(NOT rc EQUAL 0)
    message(FATAL_ERROR "superproject configure failed (${rc}):\n${out}")
endif()

set(failures "")

# Rewrites the legitimate locations away; whatever still names the
# superproject roots is a libmoq path resolved against the wrong project.
function(_moq_foreign text where out_var)
    string(REPLACE "${sub_bin}" "<libmoq-build>" text "${text}")
    string(REPLACE "${super_bin}/_deps/" "<deps>/" text "${text}")
    set(found "")
    foreach(root "${super_src}" "${super_bin}")
        string(FIND "${text}" "${root}" at)
        if(NOT at EQUAL -1)
            string(SUBSTRING "${text}" ${at} 160 excerpt)
            string(REGEX REPLACE "[\"\n].*$" "" excerpt "${excerpt}")
            list(APPEND found "${where}: ${excerpt}")
        endif()
    endforeach()
    set(${out_var} "${found}" PARENT_SCOPE)
endfunction()

# 1. Codemodel: sources and include directories of every target.
file(GLOB index_files "${super_bin}/.cmake/api/v1/reply/index-*.json")
list(LENGTH index_files index_count)
if(NOT index_count EQUAL 1)
    message(FATAL_ERROR "expected one File API index, found ${index_count}")
endif()
set(reply_dir "${super_bin}/.cmake/api/v1/reply")
file(READ "${index_files}" index)
string(JSON codemodel_file GET "${index}" reply codemodel-v2 jsonFile)
file(READ "${reply_dir}/${codemodel_file}" codemodel)
string(JSON target_count LENGTH "${codemodel}" configurations 0 targets)
set(path_count 0)
if(target_count GREATER 0)
    math(EXPR last_target "${target_count} - 1")
    foreach(t RANGE ${last_target})
        string(JSON target_file GET "${codemodel}" configurations 0 targets ${t} jsonFile)
        file(READ "${reply_dir}/${target_file}" target)
        string(JSON target_name GET "${target}" name)
        set(paths "")
        string(JSON n ERROR_VARIABLE err LENGTH "${target}" sources)
        if(NOT err AND n GREATER 0)
            math(EXPR last "${n} - 1")
            foreach(i RANGE ${last})
                string(JSON p GET "${target}" sources ${i} path)
                list(APPEND paths "${p}")
            endforeach()
        endif()
        string(JSON groups ERROR_VARIABLE err LENGTH "${target}" compileGroups)
        if(NOT err AND groups GREATER 0)
            math(EXPR last_group "${groups} - 1")
            foreach(g RANGE ${last_group})
                string(JSON n ERROR_VARIABLE err LENGTH "${target}" compileGroups ${g} includes)
                if(NOT err AND n GREATER 0)
                    math(EXPR last "${n} - 1")
                    foreach(i RANGE ${last})
                        string(JSON p GET "${target}" compileGroups ${g} includes ${i} path)
                        list(APPEND paths "${p}")
                    endforeach()
                endif()
            endforeach()
        endif()
        foreach(p IN LISTS paths)
            math(EXPR path_count "${path_count} + 1")
            # Relative codemodel paths are inside the top-level source dir,
            # which is the superproject, never libmoq.
            if(NOT IS_ABSOLUTE "${p}")
                set(p "${super_src}/${p}")
            endif()
            _moq_foreign("${p}\n" "target ${target_name}" hits)
            list(APPEND failures ${hits})
        endforeach()
    endforeach()
endif()

# 2. Test and install scripts libmoq generated.
file(GLOB_RECURSE scripts "${sub_bin}/CTestTestfile.cmake" "${sub_bin}/cmake_install.cmake")
foreach(script IN LISTS scripts)
    file(READ "${script}" text)
    file(RELATIVE_PATH rel "${super_bin}" "${script}")
    _moq_foreign("${text}" "${rel}" hits)
    list(APPEND failures ${hits})
endforeach()

# 3. Files libmoq dropped at the superproject build root.
file(GLOB root_entries RELATIVE "${super_bin}" "${super_bin}/*" "${super_bin}/.*")
set(expected_root .cmake CMakeCache.txt CMakeFiles Makefile build.ninja
    .ninja_deps .ninja_log cmake_install.cmake CTestTestfile.cmake
    DartConfiguration.tcl compile_commands.json libmoq-sub _deps)
foreach(entry IN LISTS root_entries)
    if(NOT entry IN_LIST expected_root)
        list(APPEND failures "superproject build root: unexpected ${entry}")
    endif()
endforeach()

if(path_count EQUAL 0 OR NOT scripts)
    message(FATAL_ERROR "superproject scan saw no targets or scripts; nothing was checked")
endif()
if(failures)
    list(REMOVE_DUPLICATES failures)
    list(JOIN failures "\n  " failure_text)
    message(FATAL_ERROR "libmoq resolved paths against the superproject:\n  ${failure_text}")
endif()
list(LENGTH scripts script_count)
message(STATUS "superproject add_subdirectory: ${target_count} targets, ${path_count} paths, ${script_count} scripts stay inside libmoq")
