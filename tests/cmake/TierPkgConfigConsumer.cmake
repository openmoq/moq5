# Proves that each tier pkg-config file is enough for an external program.
#
# Before anything is installed, every install destination is checked to stay
# inside the prefix, and the producer's compile and link context is checked
# to be representable by a direct compiler call. Then the build is installed
# into a scratch stage, the installed SDK is moved, and everything after that
# sees only the moved SDK:
#   - each package's link line names that tier and the tiers below it, in
#     dependency order and with nothing else, with and without --static;
#   - every flag pkg-config returns points inside the SDK;
#   - a consumer that calls into every library of the tier is compiled and
#     linked, as C and as C++, with the producer's own compile and link
#     context, and runs;
#   - each library the link names is needed from the SDK: hidden alone, it
#     makes the C link and the C++ link fail, and for that reason only;
#   - the running consumer loaded its shared libraries from the SDK, or none
#     at all from a static producer.
# Every child runs in an environment stripped of inherited install, discovery,
# compiler and loader settings, and every child's output is kept under
# ${MOQ_WORK}/logs and inspected for diagnostics even when it succeeds. A
# check that cannot be made on this host or layout makes the run report
# itself unsupported; it never passes in that case.
#
# Inputs: MOQ_BUILD (this build tree), MOQ_SOURCE, MOQ_WORK (recreated),
# MOQ_LIBDIR (the library install directory), MOQ_PKG_CONFIG, MOQ_CONTEXT
# (from moq_write_consumer_context), MOQ_PACKAGES (comma-separated),
# MOQ_SHARED (whether the producer builds shared libraries), MOQ_LIB_PREFIX,
# MOQ_SHARED_SUFFIX, MOQ_STATIC_SUFFIX. Optional: MOQ_CONFIG,
# MOQ_CROSSCOMPILING, MOQ_EMULATOR (the emulator command, its words joined
# with "|"), MOQ_PREFLIGHT_ONLY (stop after the checks made before
# installing).
cmake_minimum_required(VERSION 3.20)

foreach(var MOQ_BUILD MOQ_SOURCE MOQ_WORK MOQ_LIBDIR MOQ_PKG_CONFIG
            MOQ_CONTEXT MOQ_PACKAGES MOQ_SHARED MOQ_LIB_PREFIX
            MOQ_SHARED_SUFFIX MOQ_STATIC_SUFFIX)
    if(NOT DEFINED ${var} OR "${${var}}" STREQUAL "")
        message(FATAL_ERROR "${var} is required")
    endif()
endforeach()

include("${CMAKE_CURRENT_LIST_DIR}/TierPkgConfigChecks.cmake")
include("${MOQ_SOURCE}/adapters/wtquic/tests/loader_images.cmake")

file(REMOVE_RECURSE "${MOQ_WORK}")
file(MAKE_DIRECTORY "${MOQ_WORK}")
set(WORK "${MOQ_WORK}/logs")
string(REPLACE "|" ";" emulator "${MOQ_EMULATOR}")

# -- Destinations: decided before anything is installed ----------------------
tier_install_destinations("${MOQ_BUILD}" destinations why)
if(NOT why STREQUAL "")
    message(FATAL_ERROR "${why}")
endif()
tier_destination_verdict("${destinations}" layout_unsupported)
if(NOT layout_unsupported STREQUAL "")
    message("${TIER_UNSUPPORTED_MARKER} ${layout_unsupported}")
    return()
endif()
list(LENGTH destinations destination_count)
if(MOQ_PREFLIGHT_ONLY)
    message("${TIER_PREFLIGHT_MARKER} install destinations inside the prefix: ${destination_count}")
else()
    message(STATUS "install destinations inside the prefix: ${destination_count}")
endif()

# -- The producer's compile and link context: decided before installing -----
include("${MOQ_CONTEXT}")
tier_sysroot_verdict("${CMAKE_OSX_SYSROOT}" "${CMAKE_SYSROOT}"
    "${CMAKE_SYSROOT_COMPILE}" "${CMAKE_SYSROOT_LINK}" sysroot_unsupported)
if(NOT sysroot_unsupported STREQUAL "")
    message("${TIER_UNSUPPORTED_MARKER} ${sysroot_unsupported}")
    return()
endif()
tier_context_flags("${MOQ_CONTEXT}.json" "${MOQ_CONFIG}"
    c_flags cxx_flags link_flags)
if("${CMAKE_C_COMPILER}" STREQUAL "")
    message(FATAL_ERROR "the producer context names no C compiler")
endif()
set(languages C)
if("${CMAKE_CXX_COMPILER}" STREQUAL "")
    message(STATUS
        "the producer context names no C++ compiler: C++ consumers not built")
else()
    list(APPEND languages CXX)
endif()

if(MOQ_PREFLIGHT_ONLY)
    message("${TIER_PREFLIGHT_MARKER} install would proceed")
    return()
endif()

set(asan_lane FALSE)
foreach(flag IN LISTS c_flags cxx_flags link_flags)
    if(flag MATCHES "^-fsanitize=(.*)$")
        if(",${CMAKE_MATCH_1}," MATCHES ",address,")
            set(asan_lane TRUE)
        endif()
    elseif(flag MATCHES "^-fno-sanitize=(.*)$")
        if(",${CMAKE_MATCH_1}," MATCHES ",(address|all),")
            set(asan_lane FALSE)
        endif()
    endif()
endforeach()

# -- One environment for every child ----------------------------------------
tier_scrub_environment(scrub)
set(clean_env "${CMAKE_COMMAND}" -E env ${scrub})

# -- Staged install ---------------------------------------------------------
# Every destination was found inside the logical prefix above. DESTDIR then
# places the whole install under the stage; it only prefixes paths and
# contains nothing by itself. The logical prefix is inside the work directory
# too, so a rule that ignored DESTDIR would write there and be noticed below.
set(stage "${MOQ_WORK}/stage")
set(logical_prefix "${MOQ_WORK}/sdk")
set(staged_prefix "${stage}${logical_prefix}")
set(install_args --install "${MOQ_BUILD}" --prefix "${logical_prefix}")
if(DEFINED MOQ_CONFIG AND NOT MOQ_CONFIG STREQUAL "")
    list(APPEND install_args --config "${MOQ_CONFIG}")
endif()
tier_run("install" ${clean_env} "DESTDIR=${stage}"
    "${CMAKE_COMMAND}" ${install_args})
if(EXISTS "${logical_prefix}")
    message(FATAL_ERROR
        "an install rule ignored the staging directory and wrote to "
        "${logical_prefix}")
endif()
tier_stage_escapes("${stage}" "${staged_prefix}" escaped)
if(NOT escaped STREQUAL "")
    message(FATAL_ERROR
        "the install wrote outside its prefix, although every destination it "
        "declared was inside: ${escaped}")
endif()
if(NOT IS_DIRECTORY "${staged_prefix}")
    message(FATAL_ERROR "nothing was installed under ${staged_prefix}")
endif()

# -- Relocation: the installed SDK is moved, and only the moved one is used --
set(sdk "${MOQ_WORK}/relocated sdk")
file(RENAME "${staged_prefix}" "${sdk}")
set(libdir "${sdk}/${MOQ_LIBDIR}")
set(pcdir "${libdir}/pkgconfig")
# Only the moved SDK answers package queries.
set(pc ${clean_env} "PKG_CONFIG_LIBDIR=${pcdir}" "PKG_CONFIG_PATH="
    "${MOQ_PKG_CONFIG}")

tier_run_plan("${MOQ_CROSSCOMPILING}" "${emulator}"
    run_prefix run_unsupported)
set(run_env ${clean_env})
if(MOQ_SHARED AND CMAKE_HOST_SYSTEM_NAME STREQUAL "Linux")
    # A recorded run path serves only the program's direct dependencies there.
    list(APPEND run_env "LD_LIBRARY_PATH=${libdir}")
endif()
loader_trace_env(trace_env)
tier_inspection_plan("${run_unsupported}" "${run_prefix}" "${trace_env}"
    "${CMAKE_HOST_SYSTEM_NAME}" inspect_loader inspection_note)
set(unsupported_notes "")
foreach(note IN ITEMS "${run_unsupported}" "${inspection_note}")
    if(NOT note STREQUAL "")
        list(APPEND unsupported_notes "${note}")
    endif()
endforeach()

string(REPLACE "," ";" packages "${MOQ_PACKAGES}")
foreach(pkg IN LISTS packages)
    tier_expected("${pkg}" expected source_name)
    if(NOT EXISTS "${pcdir}/${pkg}.pc")
        message(FATAL_ERROR "${pkg}.pc was not installed under ${pcdir}")
    endif()

    # The file must not remember where it was built or first installed.
    file(READ "${pcdir}/${pkg}.pc" pc_text)
    foreach(place IN ITEMS "${MOQ_WORK}" "${MOQ_BUILD}" "${MOQ_SOURCE}")
        string(FIND "${pc_text}" "${place}" at)
        if(NOT at EQUAL -1)
            message(FATAL_ERROR
                "${pkg}.pc names the place it was built or first installed "
                "and cannot move: ${place}")
        endif()
    endforeach()

    # Order and membership of the link line, as pkg-config gives it: the
    # whole private closure first, then the plain one.
    foreach(closure IN ITEMS static plain)
        set(mode "")
        if(closure STREQUAL "static")
            set(mode --static)
        endif()
        tier_run("${pkg} ${closure} --libs-only-l"
            ${pc} ${mode} --libs-only-l "${pkg}")
        separate_arguments(names UNIX_COMMAND "${tier_stdout}")
        tier_check_link_line("${pkg}" "${mode}" "${names}" "${expected}" why)
        if(NOT why STREQUAL "")
            message(FATAL_ERROR "${why}")
        endif()
    endforeach()

    # Every returned flag points inside the moved SDK.
    tier_run("${pkg} --cflags" ${pc} --static --cflags "${pkg}")
    separate_arguments(pc_cflags UNIX_COMMAND "${tier_stdout}")
    tier_check_flags("${pkg}" "--cflags" "${pc_cflags}" "${sdk}" "" why)
    if(NOT why STREQUAL "")
        message(FATAL_ERROR "${why}")
    endif()
    tier_run("${pkg} --libs" ${pc} --static --libs "${pkg}")
    separate_arguments(pc_libs UNIX_COMMAND "${tier_stdout}")
    tier_check_flags("${pkg}" "--libs" "${pc_libs}" "${sdk}" "${expected}" why)
    if(NOT why STREQUAL "")
        message(FATAL_ERROR "${why}")
    endif()

    # The file the linker resolves for each library name, in the moved SDK.
    set(library_names "")
    set(library_files "")
    set(expected_images "")
    foreach(lib IN LISTS expected)
        string(REGEX REPLACE "^-l" "" name "${lib}")
        set(shared_file
            "${libdir}/${MOQ_LIB_PREFIX}${name}${MOQ_SHARED_SUFFIX}")
        set(static_file
            "${libdir}/${MOQ_LIB_PREFIX}${name}${MOQ_STATIC_SUFFIX}")
        if(MOQ_SHARED)
            if(NOT EXISTS "${shared_file}")
                message(FATAL_ERROR
                    "${pkg}: the shared library is not installed: ${shared_file}")
            endif()
            list(APPEND expected_images "${shared_file}")
            list(APPEND library_files "${shared_file}")
        else()
            if(NOT EXISTS "${static_file}")
                message(FATAL_ERROR
                    "${pkg}: the archive is not installed: ${static_file}")
            endif()
            if(EXISTS "${shared_file}")
                message(FATAL_ERROR
                    "${pkg}: a shared library is installed beside a static "
                    "producer's archive: ${shared_file}")
            endif()
            list(APPEND library_files "${static_file}")
        endif()
        list(APPEND library_names "${name}")
    endforeach()

    foreach(lang IN LISTS languages)
        if(lang STREQUAL "C")
            set(compiler "${CMAKE_C_COMPILER}")
            set(flags "${c_flags}")
            set(source "${MOQ_SOURCE}/tests/tier_consumers/${source_name}")
        else()
            set(compiler "${CMAKE_CXX_COMPILER}")
            set(flags "${cxx_flags}")
            set(source "${MOQ_WORK}/${pkg}.cpp")
            configure_file(
                "${MOQ_SOURCE}/tests/tier_consumers/${source_name}"
                "${source}" COPYONLY)
        endif()
        # Named apart from the libraries: the loader check below treats every
        # loaded image called libmoq-* as one of them.
        string(REGEX REPLACE "^libmoq-" "" tier "${pkg}")
        set(exe "${MOQ_WORK}/consumer-${tier}-${lang}")
        tier_consumer_command(${lang} "${compiler}" "${flags}" "${pc_cflags}"
            "${source}" "${exe}" "${pc_libs}" "${link_flags}" "${libdir}"
            command)
        tier_run("${pkg} ${lang} consumer build" ${clean_env} ${command})

        # Each library, hidden alone with the others in reach, must make this
        # language's own link fail: nothing in its producer flags, the build
        # tree or the host may stand in for any one of them. C and C++ each
        # carry their own flags, so each is challenged.
        tier_prove_consumer(${lang} "${compiler}" "${flags}" "${pc_cflags}"
            "${source}" "${exe}" "${pc_libs}" "${link_flags}" "${libdir}"
            "${pkg}" "${library_names}" "${library_files}" "${clean_env}" why)
        if(NOT why STREQUAL "")
            message(FATAL_ERROR "${pkg} ${lang} consumer: ${why}")
        endif()

        if(NOT run_unsupported STREQUAL "")
            continue()
        endif()
        tier_run("${pkg} ${lang} consumer run"
            ${run_env} ${run_prefix} "${exe}")
        if(NOT "${tier_stdout}${tier_stderr}" STREQUAL "")
            message(FATAL_ERROR
                "${pkg} ${lang} consumer printed on success:\n"
                "${tier_stdout}${tier_stderr}")
        endif()

        if(inspect_loader)
            # Which libraries the running consumer really loaded. The trace is
            # the loader's, on stderr; the consumer itself says nothing.
            moq_consumer_execute(
                COMMAND ${run_env} "${trace_env}" "${exe}"
                RESULT_VARIABLE rc OUTPUT_VARIABLE output
                ERROR_VARIABLE loaded)
            if(NOT rc EQUAL 0)
                message(FATAL_ERROR
                    "${pkg} ${lang} consumer failed under loader "
                    "inspection: ${rc}")
            endif()
            if(NOT "${output}" STREQUAL "")
                message(FATAL_ERROR
                    "${pkg} ${lang} consumer printed during loader "
                    "inspection:\n${output}")
            endif()
            loader_only_image_records("${loaded}" "${CMAKE_HOST_SYSTEM_NAME}"
                why "${asan_lane}")
            if(NOT why STREQUAL "")
                message(FATAL_ERROR "${pkg} ${lang} consumer: ${why}")
            endif()
            loader_parse_images("${loaded}" "${CMAKE_HOST_SYSTEM_NAME}" images)
            # A shared producer: exactly the tier's libraries, each once, from
            # the moved SDK. A static producer: none of them at all. A
            # libmoq-* image from anywhere else, the build tree included, is
            # refused.
            loader_require_images("${images}" "${expected_images}"
                "${libdir}" "${pkg} ${lang} consumer" why)
            if(NOT why STREQUAL "")
                message(FATAL_ERROR "${why}")
            endif()
        endif()
    endforeach()
    message(STATUS "${pkg}: link line [${expected}]; consumers checked for "
                   "${languages}")
endforeach()

# A tier that was not built must not have left a package behind.
foreach(pkg IN ITEMS libmoq-core libmoq-sim libmoq-media)
    list(FIND packages "${pkg}" at)
    if(at EQUAL -1 AND EXISTS "${pcdir}/${pkg}.pc")
        message(FATAL_ERROR "${pkg}.pc is installed but its tier is not built")
    endif()
endforeach()

# Last, and only after every check above held: what could not be done here.
foreach(note IN LISTS unsupported_notes)
    message("${TIER_UNSUPPORTED_MARKER} ${note}")
endforeach()
