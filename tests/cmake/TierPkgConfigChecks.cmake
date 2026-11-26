# Decisions of the tier pkg-config consumer test.
#
# They are kept in functions so TierPkgConfigControls.cmake can drive each one
# with a fixture instead of an installed SDK. Every caller sets WORK first:
# the shared consumer runner keeps each child's transcript there.
include("${CMAKE_CURRENT_LIST_DIR}/RejectConsumerWarnings.cmake")

# The marker a run prints, last, when it could not be carried out on this
# host or layout. The test is registered to report that as skipped.
set(TIER_UNSUPPORTED_MARKER "tier_pkgconfig_consumers: UNSUPPORTED:")
# The marker of a run that stops before installing (MOQ_PREFLIGHT_ONLY).
set(TIER_PREFLIGHT_MARKER "tier_pkgconfig_consumers: PREFLIGHT:")

# The ordered library names one package's link line must give, dependents
# before dependencies, and the consumer source that needs that tier.
function(tier_expected package out_libs out_source)
    if(package STREQUAL "libmoq-core")
        set(libs -lmoq-core)
        set(source core.c)
    elseif(package STREQUAL "libmoq-sim")
        set(libs -lmoq-sim -lmoq-core)
        set(source sim.c)
    elseif(package STREQUAL "libmoq-media")
        set(libs -lmoq-msf -lmoq-media-object -lmoq-cmaf -lmoq-mpeg2ts
            -lmoq-loc -lmoq-core)
        set(source media.c)
    elseif(package STREQUAL "libmoq-locmaf")
        set(libs -lmoq-locmaf -lmoq-cmaf -lmoq-core)
        set(source locmaf.c)
    else()
        message(FATAL_ERROR "no expectation for package ${package}")
    endif()
    set(${out_libs} "${libs}" PARENT_SCOPE)
    set(${out_source} "${source}" PARENT_SCOPE)
endfunction()

# Every destination the install will write to, read from the install scripts
# the build generated: the top-level cmake_install.cmake and each script it
# includes. Only directories something is installed into appear there, so an
# install directory the project never uses is not considered. A destination
# relative to the install prefix is spelled ${CMAKE_INSTALL_PREFIX}/<path>.
#   out_reason: "" when the scripts were read, otherwise why they could not be.
function(tier_install_destinations build out_dests out_reason)
    set(pending "${build}/cmake_install.cmake")
    set(seen "")
    set(dests "")
    set(reason "")
    while(pending)
        list(POP_FRONT pending script)
        list(FIND seen "${script}" at)
        if(NOT at EQUAL -1)
            continue()
        endif()
        list(APPEND seen "${script}")
        if(NOT EXISTS "${script}")
            set(reason "the build has no install script: ${script}")
            break()
        endif()
        file(READ "${script}" text)
        string(REGEX MATCHALL "file\\(INSTALL DESTINATION \"[^\"]*\"" found
            "${text}")
        foreach(entry IN LISTS found)
            string(REGEX REPLACE "^file\\(INSTALL DESTINATION \"([^\"]*)\"$"
                "\\1" dest "${entry}")
            list(APPEND dests "${dest}")
        endforeach()
        string(REGEX MATCHALL "include\\(\"[^\"]*cmake_install\\.cmake\"\\)"
            nested "${text}")
        foreach(entry IN LISTS nested)
            string(REGEX REPLACE "^include\\(\"([^\"]*)\"\\)$" "\\1" child
                "${entry}")
            list(APPEND pending "${child}")
        endforeach()
    endwhile()
    if(reason STREQUAL "" AND dests STREQUAL "")
        set(reason "the build's install scripts name no destination")
    endif()
    list(REMOVE_DUPLICATES dests)
    set(${out_dests} "${dests}" PARENT_SCOPE)
    set(${out_reason} "${reason}" PARENT_SCOPE)
endfunction()

# Whether every destination stays inside the install prefix. An absolute
# destination is a valid SDK layout, and so is a relative one that climbs out
# of the prefix, but neither can be installed into, consumed from and moved
# within a scratch tree. DESTDIR only prefixes paths and contains nothing, so
# this is decided from the destinations themselves, before installing.
# Paths are compared lexically after normalization.
#   out: "" when every destination stays inside, otherwise the reason.
function(tier_destination_verdict dests out)
    set(prefix_word "\${CMAKE_INSTALL_PREFIX}")
    set(placeholder "/tier-install-prefix")
    set(outside "")
    set(unresolved "")
    foreach(dest IN LISTS dests)
        string(FIND "${dest}" "${prefix_word}" at)
        if(at EQUAL 0)
            string(REPLACE "${prefix_word}" "${placeholder}" path "${dest}")
            if(path MATCHES "\\$")
                list(APPEND unresolved "${dest}")
            else()
                cmake_path(IS_PREFIX placeholder "${path}" NORMALIZE inside)
                if(NOT inside)
                    list(APPEND outside "${dest}")
                endif()
            endif()
        elseif(IS_ABSOLUTE "${dest}" AND NOT dest MATCHES "\\$")
            list(APPEND outside "${dest}")
        else()
            list(APPEND unresolved "${dest}")
        endif()
    endforeach()
    if(NOT outside STREQUAL "")
        string(REPLACE ";" " " outside "${outside}")
        set(${out}
            "install destinations outside the prefix, so nothing was installed: ${outside}"
            PARENT_SCOPE)
    elseif(NOT unresolved STREQUAL "")
        string(REPLACE ";" " " unresolved "${unresolved}")
        set(${out}
            "install destinations this test cannot resolve, so nothing was installed: ${unresolved}"
            PARENT_SCOPE)
    else()
        set(${out} "" PARENT_SCOPE)
    endif()
endfunction()

# `cmake -E env` arguments that remove, from a child's environment, every
# inherited setting that could redirect an install, package discovery, a
# compile, a link or a program's library lookup. Callers add the settings
# they own after these.
function(tier_scrub_environment out)
    set(args "")
    foreach(name IN ITEMS DESTDIR CPATH C_INCLUDE_PATH CPLUS_INCLUDE_PATH
            OBJC_INCLUDE_PATH LIBRARY_PATH)
        list(APPEND args "--unset=${name}")
    endforeach()
    execute_process(COMMAND "${CMAKE_COMMAND}" -E environment
        RESULT_VARIABLE rc OUTPUT_VARIABLE text ERROR_VARIABLE errors)
    if(NOT rc EQUAL 0)
        message(FATAL_ERROR "cannot read the environment (${rc}):\n${errors}")
    endif()
    # Only the names matter; keep values from splitting or merging records.
    string(REGEX REPLACE "[][;\\\\]" "_" text "${text}")
    string(REPLACE "\n" ";" lines "${text}")
    foreach(line IN LISTS lines)
        if(line MATCHES "^((DYLD_|LD_|PKG_CONFIG_|CMAKE_)[A-Za-z0-9_]*)=")
            list(APPEND args "--unset=${CMAKE_MATCH_1}")
        endif()
    endforeach()
    list(REMOVE_DUPLICATES args)
    set(${out} "${args}" PARENT_SCOPE)
endfunction()

# Run one child. Its stdout and stderr are both kept in ${WORK} and both
# inspected for diagnostics, also when it succeeds; stdout alone is returned
# for parsing. A nonzero status ends the test.
function(tier_run label)
    moq_consumer_execute(COMMAND ${ARGN}
        RESULT_VARIABLE rc OUTPUT_VARIABLE output ERROR_VARIABLE errors)
    if(NOT rc EQUAL 0)
        message(FATAL_ERROR "${label} failed (${rc}):\n${output}\n${errors}")
    endif()
    string(STRIP "${output}" output)
    set(tier_stdout "${output}" PARENT_SCOPE)
    set(tier_stderr "${errors}" PARENT_SCOPE)
endfunction()

# Run a command whose stdout is structured metadata, such as CTest's
# --show-only=json-v1 listing. That data legitimately quotes a registered
# test's FAIL_REGULAR_EXPRESSION, so the consumer diagnostic patterns apply
# to stderr alone, and the caller validates stdout as the data it must be.
# The status must be 0. The command takes the next consumer-command number:
# its combined transcript is kept as <n>.log, as for every other command,
# and its stdout, stderr and status are kept separately as <n>.stdout,
# <n>.stderr and <n>.status.
function(tier_run_metadata label)
    execute_process(COMMAND ${ARGN}
        RESULT_VARIABLE rc OUTPUT_VARIABLE output ERROR_VARIABLE errors)
    get_property(index GLOBAL PROPERTY MOQ_CONSUMER_COMMAND_INDEX)
    if(NOT index)
        set(index 0)
    endif()
    math(EXPR index "${index} + 1")
    set_property(GLOBAL PROPERTY MOQ_CONSUMER_COMMAND_INDEX "${index}")
    file(MAKE_DIRECTORY "${WORK}")
    set(base "${WORK}/consumer-command-${index}")
    file(WRITE "${base}.log" "${output}${errors}")
    file(WRITE "${base}.stdout" "${output}")
    file(WRITE "${base}.stderr" "${errors}")
    file(WRITE "${base}.status" "${rc}\n")
    if(NOT rc EQUAL 0)
        message(FATAL_ERROR "${label} failed (${rc}):\n${output}\n${errors}")
    endif()
    moq_reject_consumer_output("consumer command ${index} stderr" "${errors}")
    set(tier_stdout "${output}" PARENT_SCOPE)
    set(tier_stderr "${errors}" PARENT_SCOPE)
endfunction()

# Validate `listing`, the whole stdout of `ctest --show-only=json-v1` for one
# registered test: it must be one JSON document and nothing else (CTest
# prints it with `{` and `}` alone on its first and last lines, and nested
# values indented), of kind ctestInfo, version 1, listing exactly the test
# `name`, whose command runs `script` with -P and names its work directory
# with -DMOQ_WORK=, and whose FAIL_REGULAR_EXPRESSION is exactly the consumer
# diagnostic patterns. Sets `out_command` and `out_work`.
function(tier_registration_listing listing name script out_command out_work)
    string(STRIP "${listing}" doc)
    # The root's closing line is the only line that is `}` alone, and the
    # last: its first and last occurrences are the same, at the very end.
    set(lined "${doc}\n")
    string(LENGTH "${lined}" size)
    math(EXPR end "${size} - 3")
    string(FIND "${lined}" "\n}\n" first_close)
    string(FIND "${lined}" "\n}\n" last_close REVERSE)
    if(NOT doc MATCHES "^{\n" OR NOT first_close EQUAL end OR
       NOT last_close EQUAL end)
        message(FATAL_ERROR
            "the listing is not exactly one JSON document:\n${listing}")
    endif()
    string(JSON kind ERROR_VARIABLE error GET "${doc}" kind)
    if(NOT error STREQUAL "NOTFOUND" OR NOT kind STREQUAL "ctestInfo")
        message(FATAL_ERROR "the listing is not CTest test metadata: ${error}")
    endif()
    string(JSON major ERROR_VARIABLE error GET "${doc}" version major)
    if(NOT error STREQUAL "NOTFOUND" OR NOT major EQUAL 1)
        message(FATAL_ERROR "the listing is not json-v1 metadata: ${error}")
    endif()
    string(JSON count ERROR_VARIABLE error LENGTH "${doc}" tests)
    if(NOT error STREQUAL "NOTFOUND")
        message(FATAL_ERROR "the listing names no tests: ${error}")
    endif()
    if(NOT count EQUAL 1)
        message(FATAL_ERROR "the listing names ${count} tests, not exactly ${name}")
    endif()
    string(JSON listed ERROR_VARIABLE error GET "${doc}" tests 0 name)
    if(NOT error STREQUAL "NOTFOUND" OR NOT listed STREQUAL name)
        message(FATAL_ERROR "the listing names [${listed}], not ${name}")
    endif()

    string(JSON length ERROR_VARIABLE error LENGTH "${doc}" tests 0 command)
    if(NOT error STREQUAL "NOTFOUND" OR length LESS 3)
        message(FATAL_ERROR "${name} has no registered command: ${error}")
    endif()
    math(EXPR last "${length} - 1")
    set(command "")
    set(work "")
    foreach(i RANGE ${last})
        string(JSON arg GET "${doc}" tests 0 command ${i})
        list(APPEND command "${arg}")
        if(arg MATCHES "^-DMOQ_WORK=(.+)$")
            set(work "${CMAKE_MATCH_1}")
        endif()
    endforeach()
    math(EXPR flag "${length} - 2")
    string(JSON p GET "${doc}" tests 0 command ${flag})
    string(JSON registered GET "${doc}" tests 0 command ${last})
    get_filename_component(registered_real "${registered}" REALPATH)
    get_filename_component(script_real "${script}" REALPATH)
    if(NOT p STREQUAL "-P" OR NOT registered_real STREQUAL script_real)
        message(FATAL_ERROR
            "${name} does not run ${script}: ${command}")
    endif()
    if(work STREQUAL "")
        message(FATAL_ERROR "the registered command names no work directory: ${command}")
    endif()

    moq_consumer_warning_patterns(expected)
    set(found "")
    set(seen 0)
    string(JSON nprops ERROR_VARIABLE error LENGTH "${doc}" tests 0 properties)
    if(NOT error STREQUAL "NOTFOUND")
        set(nprops 0)
    endif()
    if(nprops GREATER 0)
        math(EXPR lastprop "${nprops} - 1")
        foreach(i RANGE ${lastprop})
            string(JSON pname GET "${doc}" tests 0 properties ${i} name)
            if(pname STREQUAL "FAIL_REGULAR_EXPRESSION")
                math(EXPR seen "${seen} + 1")
                set(values "")
                string(JSON nvalues LENGTH "${doc}" tests 0 properties ${i} value)
                if(nvalues GREATER 0)
                    math(EXPR lastvalue "${nvalues} - 1")
                    foreach(j RANGE ${lastvalue})
                        string(JSON v GET "${doc}" tests 0 properties ${i} value ${j})
                        list(APPEND values "${v}")
                    endforeach()
                endif()
                set(found "${values}")
            endif()
        endforeach()
    endif()
    if(NOT seen EQUAL 1 OR NOT found STREQUAL expected)
        message(FATAL_ERROR
            "${name} lists FAIL_REGULAR_EXPRESSION ${seen} time(s), as "
            "[${found}], not once as the consumer diagnostic patterns "
            "[${expected}]")
    endif()
    set(${out_command} "${command}" PARENT_SCOPE)
    set(${out_work} "${work}" PARENT_SCOPE)
endfunction()

# Does `path` lie at or inside `dir`, on path-component boundaries?
function(tier_path_within path dir out)
    get_filename_component(p "${path}" REALPATH)
    get_filename_component(d "${dir}" REALPATH)
    string(REGEX REPLACE "/+$" "" d "${d}")
    set(inside 0)
    if(p STREQUAL d)
        set(inside 1)
    else()
        string(LENGTH "${d}/" n)
        string(LENGTH "${p}" pn)
        if(pn GREATER n)
            string(SUBSTRING "${p}" 0 ${n} head)
            if(head STREQUAL "${d}/")
                set(inside 1)
            endif()
        endif()
    endif()
    set(${out} "${inside}" PARENT_SCOPE)
endfunction()

# The library names of a link line, in the order given and with nothing
# removed, against the expected sequence. `mode` is "--static" or "".
#   out: "" when they are the same sequence, otherwise the reason.
function(tier_check_link_line package mode names expected out)
    set(kind "link line")
    if(mode STREQUAL "--static")
        set(kind "static link line")
    endif()
    if("${names}" STREQUAL "${expected}")
        set(${out} "" PARENT_SCOPE)
    else()
        set(${out}
            "${package}: ${kind} names [${names}], expected [${expected}]"
            PARENT_SCOPE)
    endif()
endfunction()

# Every flag pkg-config returned: a header or library directory inside the
# installed SDK, or one of the expected library names. Nothing else.
#   out: "" when that holds, otherwise the reason.
function(tier_check_flags package kind flags sdk expected_libs out)
    set(why "")
    foreach(flag IN LISTS flags)
        if(flag MATCHES "^-[IL](.+)$")
            tier_path_within("${CMAKE_MATCH_1}" "${sdk}" inside)
            if(NOT inside)
                set(why "${package}: ${kind} names a path outside the installed SDK: ${flag}")
            endif()
        elseif(flag MATCHES "^-l")
            list(FIND expected_libs "${flag}" at)
            if(at EQUAL -1)
                set(why "${package}: ${kind} names an unexpected library: ${flag}")
            endif()
        else()
            set(why "${package}: ${kind} names an unexpected flag: ${flag}")
        endif()
        if(NOT why STREQUAL "")
            break()
        endif()
    endforeach()
    set(${out} "${why}" PARENT_SCOPE)
endfunction()

# The verdict on a link attempted with one SDK library out of reach. It must
# fail as a compiler driver fails when its linker cannot find a library:
#   - status 1, the driver's normal failure status, and no other;
#   - every line of the transcript a whole record of one of two kinds, from
#     its own tool, with nothing else on the line:
#       the linker's report that this library is missing:
#         ld: library '<lib>' not found
#         ld: library not found for -l<lib>
#         [<dir>/][<triple>-]ld: cannot find -l<lib>[: No such file or directory]
#         [<dir>/]ld.lld: error: unable to find library -l<lib>
#       the driver's summary of the linker's failure:
#         [<dir>/][<triple>-]clang[++][-<version>]: error: linker command
#           failed with exit code <n> (use -v to see invocation)
#         collect2: error: ld returned <n> exit status
#   - at least one missing-library record, and exactly one summary, whose
#     linker exit code is 1, as a missing library makes it.
# Any other line, a warning included, is refused. A line is kept whole: a
# semicolon or bracket in it is shown as "_" rather than splitting it.
#   library: the library name as the link line gives it, without -l.
#   out: "" when it failed that way, otherwise the reason.
function(tier_hidden_link_verdict package library rc transcript out)
    if("${rc}" STREQUAL "0")
        set(${out}
            "${package}: the consumer still links with the SDK's ${library} hidden, so ${library} came from outside the installed SDK"
            PARENT_SCOPE)
        return()
    endif()
    if(NOT "${rc}" STREQUAL "1")
        set(${out}
            "${package}: the link with ${library} hidden ended with status ${rc}, not the driver's normal failure status 1:\n${transcript}"
            PARENT_SCOPE)
        return()
    endif()
    string(REPLACE "\r" "" text "${transcript}")
    string(REGEX REPLACE "[][;\\\\]" "_" text "${text}")
    string(REPLACE "\n" ";" lines "${text}")
    set(tool_dir "([^ :]*/)?")
    set(triple "([A-Za-z0-9_.+-]+-)?")
    set(missing_forms
        "^ld: library '${library}' not found$"
        "^ld: library not found for -l${library}$"
        "^${tool_dir}${triple}ld(\\.bfd)?: cannot find -l${library}(: No such file or directory)?$"
        "^${tool_dir}ld\\.lld: error: unable to find library -l${library}$")
    set(clang_summary
        "^${tool_dir}${triple}clang(\\+\\+)?(-[0-9][0-9.]*)?: error: linker command failed with exit code [0-9]+ \\(use -v to see invocation\\)$")
    set(gcc_summary "^collect2: error: ld returned [0-9]+ exit status$")
    set(reported FALSE)
    set(summaries "")
    set(why "")
    foreach(line IN LISTS lines)
        string(STRIP "${line}" line)
        if(line STREQUAL "")
            continue()
        endif()
        set(known FALSE)
        foreach(form IN LISTS missing_forms)
            if(line MATCHES "${form}")
                set(known TRUE)
                set(reported TRUE)
            endif()
        endforeach()
        if(line MATCHES "${clang_summary}")
            string(REGEX REPLACE "^.* with exit code ([0-9]+) .*$" "\\1" code
                "${line}")
            list(APPEND summaries "${code}")
            set(known TRUE)
        elseif(line MATCHES "${gcc_summary}")
            string(REGEX REPLACE "^.* returned ([0-9]+) exit status$" "\\1"
                code "${line}")
            list(APPEND summaries "${code}")
            set(known TRUE)
        endif()
        if(NOT known)
            set(why "${package}: unexpected output from the link with ${library} hidden: ${line}")
            break()
        endif()
    endforeach()
    list(LENGTH summaries summary_count)
    if(why STREQUAL "" AND NOT reported)
        set(why "${package}: the link with ${library} hidden failed without reporting ${library} missing (${rc}):\n${transcript}")
    elseif(why STREQUAL "" AND NOT summary_count EQUAL 1)
        set(why "${package}: the link with ${library} hidden gave ${summary_count} driver summaries, not one:\n${transcript}")
    elseif(why STREQUAL "" AND NOT summaries STREQUAL "1")
        set(why "${package}: the driver reports linker exit code ${summaries} for the link with ${library} hidden, not the 1 a missing library gives")
    endif()
    set(${out} "${why}" PARENT_SCOPE)
endfunction()

# Link once with one SDK library file renamed out of reach, through the
# consumer runner, so the transcript is kept and inspected like any other
# child's; then put the file back and judge the failure.
#   out: "" when the link failed only because that library was missing.
function(tier_hidden_link package library file command out)
    if(NOT EXISTS "${file}")
        message(FATAL_ERROR "${package}: no SDK file to hide for ${library}: ${file}")
    endif()
    file(RENAME "${file}" "${file}.hidden")
    moq_consumer_execute(COMMAND ${command}
        RESULT_VARIABLE rc OUTPUT_VARIABLE output ERROR_VARIABLE errors)
    file(RENAME "${file}.hidden" "${file}")
    tier_hidden_link_verdict("${package}" "${library}" "${rc}"
        "${output}${errors}" why)
    set(${out} "${why}" PARENT_SCOPE)
endfunction()

# The consumer's link needs each SDK library it names: hidden alone, with
# every other library still in reach, each one makes the link fail. A
# library that some other directory on the link line can supply in its place
# is refused, even when the rest of the closure comes from the SDK.
#   names, files: parallel lists, one library name (without -l) and the SDK
#   file the linker resolves for it.
#   out: "" when every library was proven, otherwise the first reason.
function(tier_prove_each_library package names files command out)
    list(LENGTH names count)
    list(LENGTH files count_files)
    if(NOT count EQUAL count_files OR count EQUAL 0)
        message(FATAL_ERROR "${package}: ${count} names for ${count_files} files")
    endif()
    math(EXPR last "${count} - 1")
    set(why "")
    foreach(i RANGE ${last})
        list(GET names ${i} name)
        list(GET files ${i} file)
        tier_hidden_link("${package}" "${name}" "${file}" "${command}" why)
        if(NOT why STREQUAL "")
            break()
        endif()
    endforeach()
    set(${out} "${why}" PARENT_SCOPE)
endfunction()

# The proof above for one language's consumer: its own compiler, its own
# producer flags and its own source, so a library search path that only that
# language's flags add is challenged too.
#   out: "" when every library was proven for this language's link.
function(tier_prove_consumer lang compiler flags pc_cflags source exe pc_libs
         link_flags libdir package names files env out)
    tier_consumer_command(${lang} "${compiler}" "${flags}" "${pc_cflags}"
        "${source}" "${exe}-hidden" "${pc_libs}" "${link_flags}" "${libdir}"
        command)
    tier_prove_each_library("${package}" "${names}" "${files}"
        "${env};${command}" why)
    set(${out} "${why}" PARENT_SCOPE)
endfunction()

# Files a staged install wrote outside its logical prefix. Every destination
# was checked before installing; this checks what the install really did.
function(tier_stage_escapes stage staged_prefix out)
    file(GLOB_RECURSE files LIST_DIRECTORIES false "${stage}/*")
    set(escaped "")
    foreach(file IN LISTS files)
        tier_path_within("${file}" "${staged_prefix}" inside)
        if(NOT inside)
            list(APPEND escaped "${file}")
        endif()
    endforeach()
    set(${out} "${escaped}" PARENT_SCOPE)
endfunction()

# The producer's own compile and link flags, one list per language, from the
# record moq_write_consumer_context() left beside the context file. C, C++ and
# link flags are separate lists and stay separate. A multi-configuration
# producer records no build type there, so the selected configuration's flags
# are added here from the context's cache entries.
function(tier_context_flags json_file config out_c out_cxx out_link)
    file(READ "${json_file}" json)
    foreach(lang C CXX LINK)
        set(flags "")
        string(JSON count LENGTH "${json}" "${lang}")
        if(count GREATER 0)
            math(EXPR last "${count} - 1")
            foreach(i RANGE ${last})
                string(JSON flag GET "${json}" "${lang}" ${i})
                list(APPEND flags "${flag}")
            endforeach()
        endif()
        if("${CMAKE_BUILD_TYPE}" STREQUAL "" AND NOT "${config}" STREQUAL "")
            string(TOUPPER "${config}" upper)
            if(lang STREQUAL "LINK")
                set(raw "${CMAKE_EXE_LINKER_FLAGS_${upper}}")
            else()
                set(raw "${CMAKE_${lang}_FLAGS_${upper}}")
            endif()
            separate_arguments(more NATIVE_COMMAND "${raw}")
            list(APPEND flags ${more})
        endif()
        set(flags_${lang} "${flags}")
    endforeach()
    set(${out_c} "${flags_C}" PARENT_SCOPE)
    set(${out_cxx} "${flags_CXX}" PARENT_SCOPE)
    set(${out_link} "${flags_LINK}" PARENT_SCOPE)
endfunction()

# Whether the producer's sysroots can be given to the consumers, which are
# compiled and linked by one direct compiler call. The flag record carries
# only the Apple SDK or CMAKE_SYSROOT, so a producer that compiles and links
# against different sysroots, or that names one only through
# CMAKE_SYSROOT_COMPILE or CMAKE_SYSROOT_LINK, cannot be represented.
#   out: "" when it can be represented, otherwise the reason.
function(tier_sysroot_verdict osx sysroot compile link out)
    set(eff_compile "${sysroot}")
    if(NOT "${compile}" STREQUAL "")
        set(eff_compile "${compile}")
    endif()
    set(eff_link "${sysroot}")
    if(NOT "${link}" STREQUAL "")
        set(eff_link "${link}")
    endif()
    set(shown_compile "${eff_compile}")
    set(shown_link "${eff_link}")
    if(shown_compile STREQUAL "")
        set(shown_compile "none")
    endif()
    if(shown_link STREQUAL "")
        set(shown_link "none")
    endif()
    set(why "")
    if(NOT "${osx}" STREQUAL "")
        if(NOT eff_compile STREQUAL "" OR NOT eff_link STREQUAL "")
            set(why "the producer names an Apple SDK (${osx}) and a sysroot (compile: ${shown_compile}, link: ${shown_link}), which a direct compiler call cannot both be given, so nothing was installed")
        endif()
    elseif(NOT eff_compile STREQUAL "${sysroot}" OR NOT eff_link STREQUAL "${sysroot}")
        set(why "the producer compiles against sysroot ${shown_compile} and links against sysroot ${shown_link}, which a direct compiler call cannot represent, so nothing was installed")
    endif()
    set(${out} "${why}" PARENT_SCOPE)
endfunction()

# One consumer's compile-and-link command for `lang` (C or CXX). `flags` are
# that language's producer flags and no other language's.
function(tier_consumer_command lang compiler flags pc_cflags source exe
         pc_libs link_flags libdir out)
    if(lang STREQUAL "C")
        set(dialect -std=c11 -Wall -Wextra -Wpedantic -Werror)
    elseif(lang STREQUAL "CXX")
        set(dialect -std=c++17 -Wall -Wextra -Werror)
    else()
        message(FATAL_ERROR "no consumer dialect for language ${lang}")
    endif()
    set(${out}
        "${compiler}" ${dialect} ${flags} ${pc_cflags} "${source}"
        -o "${exe}" ${pc_libs} ${link_flags} "-Wl,-rpath,${libdir}"
        PARENT_SCOPE)
endfunction()

# How a built consumer may be run.
#   out_prefix:      the command that runs it (empty for a native run).
#   out_unsupported: "" when it can run; otherwise why it must not.
# A cross-compiled SDK's consumer is foreign code: it runs under the
# configured emulator or not at all, never natively.
function(tier_run_plan crosscompiling emulator out_prefix out_unsupported)
    set(prefix "")
    set(unsupported "")
    if(crosscompiling)
        if("${emulator}" STREQUAL "")
            set(unsupported
                "the SDK is cross-compiled and no emulator is configured: the consumers were built but not run")
        else()
            set(prefix ${emulator})
        endif()
    endif()
    set(${out_prefix} "${prefix}" PARENT_SCOPE)
    set(${out_unsupported} "${unsupported}" PARENT_SCOPE)
endfunction()

# Whether the libraries a running consumer loaded can be inspected. That
# inspection is a required check: whenever it cannot be made, the reason is
# returned and the run reports itself unsupported, never complete.
#   run_unsupported: tier_run_plan's verdict; the consumers did not run.
#   run_prefix:      tier_run_plan's command; an emulator runs them.
#   trace_env:       loader_trace_env's setting for this host, or "".
#   out_inspect:     TRUE when the inspection is made.
#   out_note:        "" or why it is not.
function(tier_inspection_plan run_unsupported run_prefix trace_env host
         out_inspect out_note)
    set(inspect FALSE)
    set(note "")
    if(NOT "${run_unsupported}" STREQUAL "")
        # Already reported: nothing ran, so nothing could be inspected.
    elseif(NOT "${run_prefix}" STREQUAL "")
        set(note "the consumers ran under the configured emulator, but which libraries they loaded was not inspected")
    elseif("${trace_env}" STREQUAL "")
        set(note "loaded-image inspection is not implemented for host ${host}: where the consumers' libraries were loaded from was not inspected")
    else()
        set(inspect TRUE)
    endif()
    set(${out_inspect} "${inspect}" PARENT_SCOPE)
    set(${out_note} "${note}" PARENT_SCOPE)
endfunction()
