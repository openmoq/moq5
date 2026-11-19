# moq_directory_targets() (cmake/MoqDirectoryTargets.cmake), configure-only:
# it reaches targets and places values as add_compile_options(),
# add_compile_definitions() and include_directories() do.
# - A target the declaring directory defined before the call gets the include
#   directories (after what it has then, or ahead of it under
#   CMAKE_INCLUDE_DIRECTORIES_BEFORE) and the definitions, not the options.
# - Targets defined after the call, there or in a directory added beneath it
#   after the call, get all three: options after what the directory had and
#   before what came later; include directories likewise, or ahead of what the
#   directory had under BEFORE, including in a dependency's directory that
#   calls include_directories() after the declaration.
# - A directory added before the call keeps what it had; an interface library is
#   left alone.
# The declared values are readable as a directory property; a second
# declaration in a directory is refused. And LibMoQ's picotls policy, injected
# behind a caller's project hook that already defined a target that compiles,
# configures, reaching the hook's target only as those commands would.
cmake_minimum_required(VERSION 3.20)
foreach(var MOQ_SOURCE MOQ_WORK)
    if(NOT DEFINED ${var} OR "${${var}}" STREQUAL "")
        message(FATAL_ERROR "${var} is required")
    endif()
endforeach()
include("${MOQ_SOURCE}/tests/cmake/RejectConsumerWarnings.cmake")
file(REMOVE_RECURSE "${MOQ_WORK}")
set(generator_args)
if(MOQ_GENERATOR)
    list(APPEND generator_args -G "${MOQ_GENERATOR}")
endif()
set(fixture "${MOQ_SOURCE}/tests/cmake/directory_targets")

# The dependency's own directory: the declaration first (LibMoQ's policy runs
# inside its project()), then its own include_directories(), as picotls has.
set(dependency "${MOQ_WORK}/dependency-src")
string(CONCAT dependency_lists
    "moq_directory_targets(INCLUDE_DIRECTORIES /declared/include)\n"
    "include_directories(/dependency/include)\n"
    "add_library(dependency_lib STATIC \"\${probe}\")\n")
file(WRITE "${dependency}/CMakeLists.txt" "${dependency_lists}")

set(failures "")
# configure(<name> <expect: OK or text of the refusal> <-D arguments>...): sets
# <name>_configured when an OK configure succeeded, so a failed one costs its
# own rows only.
function(configure name expect)
    execute_process(COMMAND "${CMAKE_COMMAND}" ${generator_args} -S "${fixture}" -B "${MOQ_WORK}/${name}"
            "-DMOQ_SOURCE=${MOQ_SOURCE}" "-DMOQ_DEPENDENCY=${dependency}" ${ARGN}
        RESULT_VARIABLE rc OUTPUT_VARIABLE out ERROR_VARIABLE err)
    file(WRITE "${MOQ_WORK}/${name}.log" "${out}${err}")
    set(${name}_configured FALSE PARENT_SCOPE)
    if(expect STREQUAL "OK")
        if(NOT rc EQUAL 0)
            string(REGEX MATCH "CMake Error[^\n]*\n[^\n]*" first "${out}${err}")
            list(APPEND failures "${name}: configure failed (rc ${rc}): ${first}")
        else()
            moq_reject_consumer_output("${name}" "${out}${err}")
            set(${name}_configured TRUE PARENT_SCOPE)
        endif()
    else()
        # CMake wraps a message at its own width; compare the words.
        string(REGEX REPLACE "[ \t\r\n]+" " " flat "${out}${err}")
        if(rc EQUAL 0 OR NOT flat MATCHES "${expect}")
            list(APPEND failures "${name}: expected a refusal matching '${expect}' (rc ${rc})")
        endif()
    endif()
    set(failures "${failures}" PARENT_SCOPE)
endfunction()

# expect(<model> <target> <options> <includes> <definitions>), each list joined by "|".
function(expect model target options includes definitions)
    file(STRINGS "${MOQ_WORK}/${model}/${target}.txt" lines)
    foreach(key IN ITEMS options includes definitions)
        set(actual "")
        foreach(line IN LISTS lines)
            if(line MATCHES "^${key}=(.*)$")
                set(actual "${CMAKE_MATCH_1}")
            endif()
        endforeach()
        if(NOT actual STREQUAL "${${key}}")
            list(APPEND failures "${model} ${target} ${key}: '${actual}', expected '${${key}}'")
        endif()
    endforeach()
    set(failures "${failures}" PARENT_SCOPE)
endfunction()

set(declared_options "-DCALLER|-DDECLARED_A|-DDECLARED_B")
foreach(before IN ITEMS OFF ON)
    set(model "includes-before-${before}")
    configure(${model} OK "-DMOQ_INCLUDES_BEFORE=${before}")
    if(NOT ${model}_configured)
        continue()
    endif()
    if(before)
        set(declared_includes "/declared/include|/caller/include")
        set(dependency_includes "/dependency/include|/declared/include")
        set(earlier_includes "/declared/include|/caller/include|/earlier/include|/earlier/after")
    else()
        set(declared_includes "/caller/include|/declared/include")
        set(dependency_includes "/declared/include|/dependency/include")
        set(earlier_includes "/caller/include|/earlier/include|/declared/include|/earlier/after")
    endif()
    # The declaring directory's own target from before the call: includes and
    # definitions, at the call; not the options.
    expect(${model} earlier "-DCALLER|-DEARLIER_OWN" "${earlier_includes}" "DECLARED_DEFINITION")
    # A directory added before the declaration.
    expect(${model} earlier_sub "-DCALLER" "/caller/include" "")
    # The declaring directory's targets, first to last, and one with its own values.
    expect(${model} bare "${declared_options}" "${declared_includes}" "DECLARED_DEFINITION")
    expect(${model} own "${declared_options}|-DOWN" "${declared_includes}|/own/include" "DECLARED_DEFINITION")
    expect(${model} last "${declared_options}" "${declared_includes}" "DECLARED_DEFINITION")
    # A directory added after it: inherited, declared, its own directory's, its own.
    expect(${model} later_sub "${declared_options}|-DLATER_DIR|-DLATER_OWN" "${declared_includes}"
        "DECLARED_DEFINITION")
    # A dependency's directory whose own include_directories() follows the declaration.
    expect(${model} dependency_lib "" "${dependency_includes}" "")
    # An interface library compiles nothing and passes nothing on.
    expect(${model} interface_only "" "" "")
    file(READ "${MOQ_WORK}/${model}/declared.txt" declared)
    if(NOT declared STREQUAL "-DDECLARED_A;-DDECLARED_B")
        list(APPEND failures "${model} MOQ_DIRECTORY_COMPILE_OPTIONS: '${declared}', expected '-DDECLARED_A;-DDECLARED_B'")
    endif()
endforeach()

configure(twice "already declared its targets' requirements" -DMOQ_MODEL=twice)

# LibMoQ's picotls policy behind a caller's project hook that defined a target
# of its own: it configures; the options reach only picotls's own target; on
# Windows the policy's include directory and definition reach the hook's target
# too, as include_directories() and add_compile_definitions() reach a target
# that already exists.
set(hook "${fixture}/policy_hook")
execute_process(COMMAND "${CMAKE_COMMAND}" ${generator_args} -S "${hook}/picotls" -B "${MOQ_WORK}/policy-hook"
        "-DMOQ_SOURCE=${MOQ_SOURCE}" "-DCMAKE_PROJECT_picotls_INCLUDE=${hook}/inject.cmake"
        "-DMOQ_PICOTLS_CALLER_PROJECT_INCLUDE=${hook}/caller_hook.cmake"
    RESULT_VARIABLE rc OUTPUT_VARIABLE out ERROR_VARIABLE err)
file(WRITE "${MOQ_WORK}/policy-hook.log" "${out}${err}")
if(NOT rc EQUAL 0)
    string(REGEX MATCH "CMake Error[^\n]*\n[^\n]*" first "${out}${err}")
    list(APPEND failures "policy-hook: configure failed (rc ${rc}): ${first}")
else()
    moq_reject_consumer_output("policy-hook" "${out}${err}")
    file(STRINGS "${MOQ_WORK}/policy-hook/platform.txt" platform)
    set(policy_options "-Werror|-UNDEBUG")
    if("msvc=1" IN_LIST platform)
        set(policy_options "/UNDEBUG")
    endif()
    set(windows_includes "")
    set(windows_definitions "")
    if("win32=1" IN_LIST platform)
        set(windows_includes "${hook}/picotls/picotlsvs/picotls")
        set(windows_definitions "_WINDOWS")
    endif()
    expect(policy-hook caller_helper "" "${windows_includes}" "${windows_definitions}")
    expect(policy-hook picotls-core "${policy_options}" "${windows_includes}" "${windows_definitions}")
endif()

if(failures)
    string(REPLACE ";" "\n  " failures "${failures}")
    message(FATAL_ERROR "moq_directory_targets:\n  ${failures}")
endif()
message(STATUS "moq_directory_targets: reach, order, include placement OFF and ON, earlier target, dependency, interface, property, refusal, policy behind a caller hook")
