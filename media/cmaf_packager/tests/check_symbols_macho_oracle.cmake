file(MAKE_DIRECTORY "${CHECK_DIR}")
foreach(case good hidden extra static_good static_hidden static_extra
        static_hidden_support static_public_support alt_entry static_alt_entry
        alt_entry_extra static_alt_entry_extra)
    set(flags)
    if(case MATCHES "^(static_)?hidden$")
        list(APPEND flags -DHIDE_WRITE)
    elseif(case MATCHES "extra$")
        list(APPEND flags -DEXTRA_EXPORT)
    elseif(case STREQUAL static_hidden_support)
        list(APPEND flags -DHIDDEN_SUPPORT)
    elseif(case STREQUAL static_public_support)
        list(APPEND flags -DPUBLIC_SUPPORT)
    endif()
    if(case MATCHES "alt_entry")
        list(APPEND flags -DALT_ENTRY)
    endif()
    set(shared ON)
    set(link_flags -dynamiclib)
    set(library "${CHECK_DIR}/${case}.dylib")
    if(case MATCHES "^static_")
        set(shared OFF)
        set(link_flags -c)
        set(library "${CHECK_DIR}/${case}.o")
    endif()
    execute_process(COMMAND "${CC}" ${link_flags} -fPIC -fvisibility=hidden
        -Wall -Wextra -Wpedantic -Werror ${flags}
        "${SOURCE_DIR}/symbol_fixture.c" -o "${library}"
        RESULT_VARIABLE rc OUTPUT_VARIABLE out ERROR_VARIABLE err)
    if(NOT rc EQUAL 0 OR NOT "${out}${err}" STREQUAL "")
        message(FATAL_ERROR "fixture compile failed: ${out}${err}")
    endif()
    if(NOT shared)
        execute_process(COMMAND "${AR}" rcs "${CHECK_DIR}/${case}.a" "${library}"
            RESULT_VARIABLE rc OUTPUT_VARIABLE out ERROR_VARIABLE err)
        if(NOT rc EQUAL 0 OR NOT "${out}${err}" STREQUAL "")
            message(FATAL_ERROR "fixture archive failed: ${out}${err}")
        endif()
        set(library "${CHECK_DIR}/${case}.a")
    endif()
    if(case MATCHES "^static_alt_entry")
        execute_process(COMMAND "${NM}" -m -gU "${library}"
            RESULT_VARIABLE rc OUTPUT_VARIABLE symbols ERROR_VARIABLE err)
        if(NOT rc EQUAL 0 OR NOT "${err}" STREQUAL ""
                OR NOT symbols MATCHES "\\[alt entry\\]")
            message(FATAL_ERROR "alternate-entry fixture lacks its annotation: ${symbols}${err}")
        endif()
    endif()
    execute_process(COMMAND "${CMAKE_COMMAND}" "-DNM=${NM}"
        "-DLIBRARY=${library}" "-DSHARED=${shared}" -DELF=OFF
        -P "${SOURCE_DIR}/check_symbols.cmake"
        RESULT_VARIABLE rc OUTPUT_VARIABLE out ERROR_VARIABLE err)
    if(case STREQUAL good OR case STREQUAL static_good
            OR case STREQUAL static_hidden_support OR case STREQUAL alt_entry
            OR case STREQUAL static_alt_entry)
        if(NOT rc EQUAL 0 OR NOT "${out}${err}" STREQUAL "")
            message(FATAL_ERROR "valid export set refused (${case}): ${out}${err}")
        endif()
    elseif(rc EQUAL 0)
        message(FATAL_ERROR "invalid export set accepted (${case})")
    elseif(NOT "${out}${err}" MATCHES "packager export set mismatch")
        message(FATAL_ERROR "invalid export set failed for another reason (${case}): ${out}${err}")
    endif()
    message(STATUS "export oracle ${case}: expected result")
endforeach()
