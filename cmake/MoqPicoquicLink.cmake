# The link closure every LibMoQ target carries when it puts picoquic-core on a
# link line.
#
# picoquic-core and picoquic-log are mutually dependent static archives: core's
# configuration and socket-loop objects call picoquic_set_qlog in picoquic-log,
# and picoquic-log calls back into core. Upstream declares only log -> core, and
# FindPicoquic keeps that graph as it is (a core -> log edge would close a cycle
# that CMake repeats for every consumer leaving CMP0156 unset, which Apple's
# linker reports as duplicate libraries). So the closure is spelled out here:
#
#   * the pair itself, core then log. A linker that resolves across archives
#     (Apple's) needs nothing more, and CMake orders the pair once;
#   * on Linux, whose linker is one-pass, the same two archives again as a
#     group of plain file paths, so a core object pulled late still finds log
#     after it. A group of TARGETS does not survive: CMake reorders targets by
#     their dependencies and leaves the group flags with nothing between them.
#     File paths are link items it keeps where they are written.
#
# moq_picoquic_link_closure(<out_var> <core_target> [<log_target>])
function(moq_picoquic_link_closure out_var core_target)
    set(_closure "${core_target}")
    if(ARGC GREATER 2 AND ARGV2)
        list(APPEND _closure "${ARGV2}")
        # Only real archives have a file to name; an INTERFACE stand-in (a
        # package that carries no library of its own) gets the pair alone.
        get_target_property(_core_type ${core_target} TYPE)
        get_target_property(_log_type ${ARGV2} TYPE)
        if(CMAKE_SYSTEM_NAME STREQUAL "Linux"
           AND _core_type STREQUAL "STATIC_LIBRARY"
           AND _log_type STREQUAL "STATIC_LIBRARY")
            list(APPEND _closure
                "-Wl,--start-group"
                "$<TARGET_FILE:${core_target}>"
                "$<TARGET_FILE:${ARGV2}>"
                "-Wl,--end-group")
        endif()
    endif()
    set(${out_var} "${_closure}" PARENT_SCOPE)
endfunction()

# The same closure wrapped item by item in $<BUILD_INTERFACE:...>, for an
# in-tree target whose install-tree interface is written separately.
function(moq_picoquic_build_link_closure out_var)
    moq_picoquic_link_closure(_closure ${ARGN})
    list(TRANSFORM _closure PREPEND "$<BUILD_INTERFACE:")
    list(TRANSFORM _closure APPEND ">")
    set(${out_var} "${_closure}" PARENT_SCOPE)
endfunction()
