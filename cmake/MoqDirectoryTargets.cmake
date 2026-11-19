# Usage requirements for every target a directory builds, stated on the
# targets rather than on the directory.
#
#   moq_directory_targets([COMPILE_OPTIONS <option>...]
#       [COMPILE_DEFINITIONS <definition>...]
#       [INCLUDE_DIRECTORIES <dir>...])
#
# The targets that compile get the options, definitions and include
# directories PRIVATE, reaching the targets and placing the values as the
# directory-scoped add_compile_options(), add_compile_definitions() and
# include_directories() would have:
#
#                 targets the directory   targets it defines   directories added
#                 defined before the      after the call, and  beneath it before
#                 call                    those of directories the call
#                                         added beneath it
#                                         after the call
#   options       no                      after the            no
#                                         directory's earlier
#                                         options, before the
#                                         target's own
#   includes      yes, at the call:       after the            no
#                 after what the target   directory's earlier
#                 has then, or ahead of   includes, or ahead
#                 it under BEFORE         of them under BEFORE
#   definitions   yes                     yes                  no
#
# BEFORE is CMAKE_INCLUDE_DIRECTORIES_BEFORE in force at the call, as
# include_directories() reads it; the generators order definitions themselves.
# CMake 4.5 diagnoses those directory-scoped commands (non-target-directive, an
# author diagnostic), so a build that makes author diagnostics errors refuses
# them. A caller's project hook (CMAKE_PROJECT_<name>_INCLUDE) may have defined
# targets before a dependency's policy declares; they are reached as above.
#
# Targets defined after the call get the values at the end of the calling
# directory, so one defined further down gets them too. The declared values are readable as the
# directory properties MOQ_DIRECTORY_COMPILE_OPTIONS,
# MOQ_DIRECTORY_COMPILE_DEFINITIONS and MOQ_DIRECTORY_INCLUDE_DIRECTORIES.
# One call per directory.
include_guard(GLOBAL)

function(moq_directory_targets)
    cmake_parse_arguments(PARSE_ARGV 0 arg "" ""
        "COMPILE_OPTIONS;COMPILE_DEFINITIONS;INCLUDE_DIRECTORIES")
    if(arg_UNPARSED_ARGUMENTS)
        message(FATAL_ERROR "moq_directory_targets: unexpected arguments: ${arg_UNPARSED_ARGUMENTS}")
    endif()
    get_property(declared DIRECTORY PROPERTY _MOQ_DIRECTORY_TARGETS SET)
    if(declared)
        message(FATAL_ERROR "moq_directory_targets: ${CMAKE_CURRENT_SOURCE_DIR} already declared its targets' requirements")
    endif()
    get_property(earlier_targets DIRECTORY PROPERTY BUILDSYSTEM_TARGETS)
    get_property(earlier_dirs DIRECTORY PROPERTY SUBDIRECTORIES)
    get_property(options_before DIRECTORY PROPERTY COMPILE_OPTIONS)
    get_property(includes_before DIRECTORY PROPERTY INCLUDE_DIRECTORIES)
    set(includes_place AFTER)
    if(CMAKE_INCLUDE_DIRECTORIES_BEFORE)
        set(includes_place BEFORE)
    endif()
    # The directory's targets that already exist take the includes and the
    # definitions now, as the directory commands give them to existing
    # targets; the options do not reach them.
    set(before_keyword "")
    if(includes_place STREQUAL "BEFORE")
        set(before_keyword BEFORE)
    endif()
    foreach(target IN LISTS earlier_targets)
        _moq_directory_targets_compiles(${target} compiles)
        if(NOT compiles)
            continue()
        endif()
        if(arg_INCLUDE_DIRECTORIES)
            target_include_directories(${target} ${before_keyword} PRIVATE ${arg_INCLUDE_DIRECTORIES})
        endif()
        if(arg_COMPILE_DEFINITIONS)
            target_compile_definitions(${target} PRIVATE ${arg_COMPILE_DEFINITIONS})
        endif()
    endforeach()
    set_property(DIRECTORY PROPERTY _MOQ_DIRECTORY_TARGETS TRUE)
    set_property(DIRECTORY PROPERTY _MOQ_DIRECTORY_TARGETS_EARLIER_TARGETS ${earlier_targets})
    set_property(DIRECTORY PROPERTY _MOQ_DIRECTORY_TARGETS_EARLIER_DIRS ${earlier_dirs})
    set_property(DIRECTORY PROPERTY _MOQ_DIRECTORY_TARGETS_OPTIONS_BEFORE ${options_before})
    set_property(DIRECTORY PROPERTY _MOQ_DIRECTORY_TARGETS_INCLUDES_BEFORE ${includes_before})
    set_property(DIRECTORY PROPERTY _MOQ_DIRECTORY_TARGETS_INCLUDES_PLACE ${includes_place})
    set_property(DIRECTORY PROPERTY MOQ_DIRECTORY_COMPILE_OPTIONS ${arg_COMPILE_OPTIONS})
    set_property(DIRECTORY PROPERTY MOQ_DIRECTORY_COMPILE_DEFINITIONS ${arg_COMPILE_DEFINITIONS})
    set_property(DIRECTORY PROPERTY MOQ_DIRECTORY_INCLUDE_DIRECTORIES ${arg_INCLUDE_DIRECTORIES})
    cmake_language(DEFER CALL _moq_directory_targets_apply)
endfunction()

function(_moq_directory_targets_compiles target out)
    get_target_property(type ${target} TYPE)
    if(type MATCHES "^(EXECUTABLE|STATIC_LIBRARY|SHARED_LIBRARY|MODULE_LIBRARY|OBJECT_LIBRARY)$")
        set(${out} TRUE PARENT_SCOPE)
    else()
        set(${out} FALSE PARENT_SCOPE)
    endif()
endfunction()

# Runs at the end of the declaring directory, in its scope.
function(_moq_directory_targets_apply)
    get_property(earlier_targets DIRECTORY PROPERTY _MOQ_DIRECTORY_TARGETS_EARLIER_TARGETS)
    get_property(earlier_dirs DIRECTORY PROPERTY _MOQ_DIRECTORY_TARGETS_EARLIER_DIRS)
    get_property(options_before DIRECTORY PROPERTY _MOQ_DIRECTORY_TARGETS_OPTIONS_BEFORE)
    get_property(includes_before DIRECTORY PROPERTY _MOQ_DIRECTORY_TARGETS_INCLUDES_BEFORE)
    get_property(includes_place DIRECTORY PROPERTY _MOQ_DIRECTORY_TARGETS_INCLUDES_PLACE)
    get_property(options DIRECTORY PROPERTY MOQ_DIRECTORY_COMPILE_OPTIONS)
    get_property(definitions DIRECTORY PROPERTY MOQ_DIRECTORY_COMPILE_DEFINITIONS)
    get_property(includes DIRECTORY PROPERTY MOQ_DIRECTORY_INCLUDE_DIRECTORIES)

    set(pending "${CMAKE_CURRENT_SOURCE_DIR}")
    set(targets "")
    while(pending)
        list(POP_FRONT pending dir)
        get_property(dir_targets DIRECTORY "${dir}" PROPERTY BUILDSYSTEM_TARGETS)
        if(dir STREQUAL CMAKE_CURRENT_SOURCE_DIR AND earlier_targets)
            # Given their share at the declaration.
            list(REMOVE_ITEM dir_targets ${earlier_targets})
        endif()
        list(APPEND targets ${dir_targets})
        get_property(subdirs DIRECTORY "${dir}" PROPERTY SUBDIRECTORIES)
        if(dir STREQUAL CMAKE_CURRENT_SOURCE_DIR AND earlier_dirs)
            list(REMOVE_ITEM subdirs ${earlier_dirs})
        endif()
        list(APPEND pending ${subdirs})
    endwhile()

    foreach(target IN LISTS targets)
        _moq_directory_targets_compiles(${target} compiles)
        if(NOT compiles)
            continue()
        endif()
        if(options)
            # add_compile_options() appends, so a target's list starts with
            # what the directory had at the declaration.
            _moq_directory_targets_place(${target} COMPILE_OPTIONS "${options_before}" AFTER "${options}")
        endif()
        if(includes)
            _moq_directory_targets_place(${target} INCLUDE_DIRECTORIES "${includes_before}" ${includes_place}
                "${includes}")
        endif()
        if(definitions)
            target_compile_definitions(${target} PRIVATE ${definitions})
        endif()
    endforeach()
endfunction()

# Puts <values> into the target's <property> where the directory-scoped
# command put them. <before> is what the directory had at the declaration, a
# contiguous run in the target's list whatever the directory added later on
# either side: AFTER places the values right after it, BEFORE right ahead of
# it. With nothing before the declaration, AFTER places them first and BEFORE
# right after the entries the target took from its directory (which, with
# include_directories() prepending, all came after the declaration).
function(_moq_directory_targets_place target property before place values)
    get_target_property(current ${target} ${property})
    if(current STREQUAL "current-NOTFOUND")
        set(current "")
    endif()
    list(LENGTH current have)
    list(LENGTH before count)
    set(at -1)
    if(count GREATER 0)
        set(i 0)
        math(EXPR last "${have} - ${count}")
        while(NOT i GREATER last AND at LESS 0)
            list(SUBLIST current ${i} ${count} window)
            if("${window}" STREQUAL "${before}")
                set(at ${i})
                if(place STREQUAL "AFTER")
                    math(EXPR at "${i} + ${count}")
                endif()
            endif()
            math(EXPR i "${i} + 1")
        endwhile()
    elseif(place STREQUAL "AFTER")
        set(at 0)
    else()
        # The longest start of the target's list that ends its directory's
        # list: the entries it took from the directory.
        get_target_property(source_dir ${target} SOURCE_DIR)
        get_property(dir_list DIRECTORY "${source_dir}" PROPERTY ${property})
        list(LENGTH dir_list dir_count)
        set(at 0)
        set(k ${dir_count})
        if(k GREATER have)
            set(k ${have})
        endif()
        while(k GREATER 0 AND at EQUAL 0)
            list(SUBLIST current 0 ${k} head)
            math(EXPR from "${dir_count} - ${k}")
            list(SUBLIST dir_list ${from} ${k} tail)
            if("${head}" STREQUAL "${tail}")
                set(at ${k})
            endif()
            math(EXPR k "${k} - 1")
        endwhile()
    endif()
    if(at LESS 0)
        # The directory's earlier values are no longer in the target's list
        # (something replaced it): keep the declared ones ahead of the rest.
        set(at 0)
    endif()
    set(head "")
    set(rest "")
    if(at GREATER 0)
        list(SUBLIST current 0 ${at} head)
    endif()
    if(have GREATER at)
        list(SUBLIST current ${at} -1 rest)
    endif()
    set_property(TARGET ${target} PROPERTY ${property} ${head} ${values} ${rest})
endfunction()
