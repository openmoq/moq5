# Repair static Boost::regex ICU edges for mvfst consumers.
#
# Homebrew's static Boost::regex imported target currently publishes ICU as
# bare library names (icudata;icui18n;icuuc). ICU is keg-only there, so those
# names are not linkable unless the ICU lib directory is added explicitly. Linux
# distro packages usually resolve this through the normal linker search path and
# this helper becomes a no-op.

function(moq_mvfst_resolve_boost_icu_link_directories out_var)
    set(_moq_mvfst_icu_dirs)

    if(TARGET Boost::regex)
        get_target_property(_moq_mvfst_boost_regex_libs
            Boost::regex INTERFACE_LINK_LIBRARIES)
    else()
        set(${out_var} "" PARENT_SCOPE)
        return()
    endif()

    if(NOT _moq_mvfst_boost_regex_libs)
        set(${out_var} "" PARENT_SCOPE)
        return()
    endif()

    set(_moq_mvfst_needs_icu_dirs OFF)
    foreach(_moq_mvfst_icu_name icudata icui18n icuuc)
        if(_moq_mvfst_icu_name IN_LIST _moq_mvfst_boost_regex_libs)
            set(_moq_mvfst_needs_icu_dirs ON)
        endif()
    endforeach()

    if(NOT _moq_mvfst_needs_icu_dirs)
        set(${out_var} "" PARENT_SCOPE)
        return()
    endif()

    foreach(_moq_mvfst_icu_name icudata icui18n icuuc)
        string(TOUPPER "${_moq_mvfst_icu_name}" _moq_mvfst_icu_upper)
        find_library(MOQ_MVFST_${_moq_mvfst_icu_upper}_LIBRARY
            NAMES ${_moq_mvfst_icu_name}
            PATHS
                "${ICU_ROOT}"
                "$ENV{ICU_ROOT}"
                /opt/homebrew/opt/icu4c
                /usr/local/opt/icu4c
            PATH_SUFFIXES lib)
        if(NOT MOQ_MVFST_${_moq_mvfst_icu_upper}_LIBRARY)
            message(FATAL_ERROR
                "Boost::regex links ${_moq_mvfst_icu_name} by bare name, "
                "but ${_moq_mvfst_icu_name} was not found. Add ICU's prefix "
                "to CMAKE_PREFIX_PATH or set ICU_ROOT.")
        endif()

        get_filename_component(_moq_mvfst_icu_dir
            "${MOQ_MVFST_${_moq_mvfst_icu_upper}_LIBRARY}" DIRECTORY)
        list(APPEND _moq_mvfst_icu_dirs "${_moq_mvfst_icu_dir}")
    endforeach()

    list(REMOVE_DUPLICATES _moq_mvfst_icu_dirs)
    set(${out_var} "${_moq_mvfst_icu_dirs}" PARENT_SCOPE)
endfunction()

function(moq_mvfst_apply_boost_icu_link_directories target)
    moq_mvfst_resolve_boost_icu_link_directories(_moq_mvfst_icu_dirs)
    if(_moq_mvfst_icu_dirs)
        target_link_directories(${target} INTERFACE ${_moq_mvfst_icu_dirs})
    endif()
endfunction()

function(moq_mvfst_patch_boost_icu_link_directories)
    moq_mvfst_resolve_boost_icu_link_directories(_moq_mvfst_icu_dirs)
    if(_moq_mvfst_icu_dirs AND TARGET Boost::regex)
        target_link_directories(Boost::regex INTERFACE ${_moq_mvfst_icu_dirs})
    endif()
endfunction()
