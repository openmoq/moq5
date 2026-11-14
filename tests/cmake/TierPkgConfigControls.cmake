# Controls for the tier pkg-config files and their consumer test.
#
# Each control drives one decision with a fixture and requires the exact
# verdict: the paths a tier file must name for each install layout, and each
# way the consumer test must refuse a wrong SDK, layout, context, environment
# or child. The producer-context controls configure a small project with the
# real context writer. Nothing is installed, and pkg-config is not needed.
#
# Inputs: MOQ_SOURCE (this checkout), MOQ_WORK (recreated), CTEST_COMMAND,
# MOQ_GENERATOR. Optional: MOQ_MAKE_PROGRAM.
cmake_minimum_required(VERSION 3.20)

# -- Fixture children ---------------------------------------------------------
if(DEFINED TIER_FIXTURE)
    if(TIER_FIXTURE STREQUAL "linker-diagnostic")
        # Succeeds, and says what a linker says when it only warns.
        message("ld: warning: deliberate tier control diagnostic")
    elseif(TIER_FIXTURE STREQUAL "clean")
        execute_process(COMMAND "${CMAKE_COMMAND}" -E echo "tier control clean")
    elseif(TIER_FIXTURE STREQUAL "failing")
        message(FATAL_ERROR "deliberate tier control failure")
    elseif(TIER_FIXTURE STREQUAL "listing-with-diagnostic")
        # A well-formed listing on stdout, and what a tool prints on stderr
        # when it only warns.
        execute_process(COMMAND "${CMAKE_COMMAND}" -E cat "${TIER_LISTING}")
        message("warning: deliberate tier control diagnostic")
    else()
        message(FATAL_ERROR "unknown fixture ${TIER_FIXTURE}")
    endif()
    return()
endif()

foreach(var MOQ_SOURCE MOQ_WORK)
    if(NOT DEFINED ${var} OR "${${var}}" STREQUAL "")
        message(FATAL_ERROR "${var} is required")
    endif()
endforeach()
include("${CMAKE_CURRENT_LIST_DIR}/TierPkgConfigChecks.cmake")

# A stand-in linker for the provenance controls. It resolves each -l name
# against the -L directories in order, as a linker does, and reports a
# missing one the way a linker and its driver do. With --warn first it also
# prints an unrelated warning.
set(fake_linker "${MOQ_WORK}/fake-linker.sh")

# -- Controls that must end the run, or need a fresh scope: each is a child ---
if(DEFINED TIER_CONTROL)
    set(WORK "${MOQ_WORK}/${TIER_CONTROL}")
    if(TIER_CONTROL STREQUAL "diagnostic-on-success")
        tier_run("fixture link" "${CMAKE_COMMAND}"
            -DTIER_FIXTURE=linker-diagnostic -P "${CMAKE_CURRENT_LIST_FILE}")
    elseif(TIER_CONTROL STREQUAL "clean-success")
        tier_run("fixture link" "${CMAKE_COMMAND}"
            -DTIER_FIXTURE=clean -P "${CMAKE_CURRENT_LIST_FILE}")
        message(STATUS "returned [${tier_stdout}]")
    elseif(TIER_CONTROL STREQUAL "nonzero-status")
        tier_run("fixture link" "${CMAKE_COMMAND}"
            -DTIER_FIXTURE=failing -P "${CMAKE_CURRENT_LIST_FILE}")
    elseif(TIER_CONTROL STREQUAL "sysroot-verdict")
        # The context the real writer produced, read the way the consumer
        # test reads it.
        include("${TIER_CONTEXT}")
        tier_sysroot_verdict("${CMAKE_OSX_SYSROOT}" "${CMAKE_SYSROOT}"
            "${CMAKE_SYSROOT_COMPILE}" "${CMAKE_SYSROOT_LINK}" why)
        tier_context_flags("${TIER_CONTEXT}.json" "" c_flags cxx_flags
            link_flags)
        message(STATUS "verdict=[${why}]")
        message(STATUS "c-flags=[${c_flags}]")
    elseif(TIER_CONTROL STREQUAL "listing-clean")
        tier_run_metadata("fixture listing" "${CMAKE_COMMAND}" -E cat "${TIER_LISTING}")
        tier_registration_listing("${tier_stdout}" tier_pkgconfig_consumers
            "${TIER_SCRIPT}" command work)
        message(STATUS "listing work=[${work}]")
    elseif(TIER_CONTROL STREQUAL "listing-stderr-diagnostic")
        tier_run_metadata("fixture listing" "${CMAKE_COMMAND}"
            -DTIER_FIXTURE=listing-with-diagnostic "-DTIER_LISTING=${TIER_LISTING}"
            -P "${CMAKE_CURRENT_LIST_FILE}")
        message(STATUS "listing accepted")
    elseif(TIER_CONTROL STREQUAL "listing-check")
        file(READ "${TIER_LISTING}" listing)
        tier_registration_listing("${listing}" tier_pkgconfig_consumers
            "${TIER_SCRIPT}" command work)
        message(STATUS "listing work=[${work}]")
    elseif(TIER_CONTROL STREQUAL "hidden-link-warning")
        set(sdk "${MOQ_WORK}/hidden-link-warning-sdk")
        file(MAKE_DIRECTORY "${sdk}")
        file(WRITE "${sdk}/libmoq-core.a" "")
        tier_hidden_link(libmoq-core moq-core "${sdk}/libmoq-core.a"
            "/bin/sh;${fake_linker};--warn;-L${sdk};-lmoq-core" why)
        message(STATUS "hidden link verdict=[${why}]")
    else()
        message(FATAL_ERROR "unknown control ${TIER_CONTROL}")
    endif()
    return()
endif()

file(REMOVE_RECURSE "${MOQ_WORK}")
file(MAKE_DIRECTORY "${MOQ_WORK}")
set(WORK "${MOQ_WORK}/logs")
file(WRITE "${fake_linker}" [=[
warn=
if [ "$1" = "--warn" ]; then warn=1; shift; fi
if [ -n "$warn" ]; then echo "ld: warning: deliberate tier control warning" >&2; fi
dirs=
for a; do case $a in -L*) dirs="$dirs ${a#-L}";; esac; done
for a; do
  case $a in
  -l*)
    name=${a#-l}; found=
    for d in $dirs; do
      if [ -f "$d/lib$name.a" ]; then found=1; break; fi
    done
    if [ -z "$found" ]; then
      echo "ld: library '$name' not found" >&2
      echo "clang: error: linker command failed with exit code 1 (use -v to see invocation)" >&2
      exit 1
    fi;;
  esac
done
exit 0
]=])

function(control_equal name actual expected)
    if(NOT "${actual}" STREQUAL "${expected}")
        message(FATAL_ERROR
            "control ${name}: got [${actual}], expected [${expected}]")
    endif()
    message(STATUS "control ${name}: expected verdict")
endfunction()

# A control child runs in the same scrubbed environment as the consumer
# test's children.
function(control_child name out_rc out_text)
    execute_process(
        COMMAND ${clean_env} "${CMAKE_COMMAND}" "-DTIER_CONTROL=${name}"
            "-DMOQ_SOURCE=${MOQ_SOURCE}" "-DMOQ_WORK=${MOQ_WORK}" ${ARGN}
            -P "${CMAKE_CURRENT_LIST_FILE}"
        RESULT_VARIABLE rc OUTPUT_VARIABLE output ERROR_VARIABLE errors)
    set(${out_rc} "${rc}" PARENT_SCOPE)
    set(${out_text} "${output}${errors}" PARENT_SCOPE)
endfunction()

tier_scrub_environment(scrub)
set(clean_env "${CMAKE_COMMAND}" -E env ${scrub})

# A CTest --show-only=json-v1 listing in the layout CTest prints: one test
# per entry of `names`, each registering `script` with -P and the work
# directory `work`, and each with the FAIL_REGULAR_EXPRESSION values given
# as the JSON text `fail_values`.
function(control_listing out script work fail_values names)
    set(tests "")
    set(sep "")
    foreach(n IN LISTS names)
        string(APPEND tests "${sep}
    {
      \"backtrace\" : 1,
      \"command\" :
      [
        \"${CMAKE_COMMAND}\",
        \"-DMOQ_WORK=${work}\",
        \"-P\",
        \"${script}\"
      ],
      \"config\" : \"Debug\",
      \"name\" : \"${n}\",
      \"properties\" :
      [
        {
          \"name\" : \"FAIL_REGULAR_EXPRESSION\",
          \"value\" :
          [
${fail_values}
          ]
        },
        {
          \"name\" : \"TIMEOUT\",
          \"value\" : 600.0
        }
      ]
    }")
        set(sep ",")
    endforeach()
    set(${out} "{
  \"kind\" : \"ctestInfo\",
  \"tests\" :
  [${tests}
  ],
  \"version\" :
  {
    \"major\" : 1,
    \"minor\" : 0
  }
}" PARENT_SCOPE)
endfunction()

# == 1. The paths a tier file names, for each install layout ==================
include("${MOQ_SOURCE}/cmake/MoqTierPkgConfig.cmake")

moq_tier_pc_paths("lib" "include" prefix libdir includedir needs)
control_equal("paths relative: prefix" "${prefix}" "\${pcfiledir}/../..")
control_equal("paths relative: libdir" "${libdir}" "\${prefix}/lib")
control_equal("paths relative: includedir" "${includedir}" "\${prefix}/include")
control_equal("paths relative: install-time prefix" "${needs}" "FALSE")

moq_tier_pc_paths("lib/x86_64-linux-gnu" "include" prefix libdir includedir
    needs)
control_equal("paths multiarch: prefix" "${prefix}" "\${pcfiledir}/../../..")
control_equal("paths multiarch: libdir" "${libdir}"
    "\${prefix}/lib/x86_64-linux-gnu")

# An absolute include directory is named as it is. Joined to the prefix it
# would read ${prefix}//opt/moq/include, where no header is installed.
moq_tier_pc_paths("lib" "/opt/moq/include" prefix libdir includedir needs)
control_equal("paths absolute include: prefix" "${prefix}"
    "\${pcfiledir}/../..")
control_equal("paths absolute include: includedir" "${includedir}"
    "/opt/moq/include")
control_equal("paths absolute include: install-time prefix" "${needs}" "FALSE")

# An absolute library directory: the prefix is not an ascent from the file
# (that would reach the filesystem root); it is the install prefix.
moq_tier_pc_paths("/opt/moq/lib" "include" prefix libdir includedir needs)
control_equal("paths absolute lib: prefix" "${prefix}"
    "${MOQ_TIER_PC_PREFIX_TOKEN}")
control_equal("paths absolute lib: libdir" "${libdir}" "/opt/moq/lib")
control_equal("paths absolute lib: includedir" "${includedir}"
    "\${prefix}/include")
control_equal("paths absolute lib: install-time prefix" "${needs}" "TRUE")

moq_tier_pc_paths("/opt/moq/lib" "/opt/moq/include" prefix libdir includedir
    needs)
control_equal("paths both absolute: libdir" "${libdir}" "/opt/moq/lib")
control_equal("paths both absolute: includedir" "${includedir}"
    "/opt/moq/include")

# The whole file for the mixed layout, before and after it is installed.
set(CMAKE_INSTALL_LIBDIR "/opt/moq/lib")
set(CMAKE_INSTALL_INCLUDEDIR "include")
set(PROJECT_VERSION "0.0.0")
set(rendered "${MOQ_WORK}/libmoq-sim.pc")
moq_configure_tier_pc(libmoq-sim "control" "libmoq-core" "-lmoq-sim"
    "${rendered}" needs)
control_equal("rendered: install-time prefix" "${needs}" "TRUE")
file(STRINGS "${rendered}" lines REGEX "^(prefix|libdir|includedir|Requires|Libs|Cflags)[=:]")
control_equal("rendered: before install" "${lines}"
    "prefix=${MOQ_TIER_PC_PREFIX_TOKEN};libdir=/opt/moq/lib;includedir=\${prefix}/include;Requires: libmoq-core;Libs: -L\${libdir} -lmoq-sim;Cflags: -I\${includedir}")
moq_tier_pc_resolve_prefix("${rendered}" "/usr/local")
file(STRINGS "${rendered}" lines REGEX "^(prefix|libdir|includedir)=")
control_equal("rendered: installed for /usr/local" "${lines}"
    "prefix=/usr/local;libdir=/opt/moq/lib;includedir=\${prefix}/include")

# == 2. The destinations an install uses, decided before installing ===========
# The generated install scripts name each destination once per rule, and the
# top-level script includes each subdirectory's by absolute path.
set(scripts "${MOQ_WORK}/install-scripts")
file(WRITE "${scripts}/cmake_install.cmake"
    "if(NOT CMAKE_INSTALL_LOCAL_ONLY)\n"
    "  include(\"${scripts}/core/cmake_install.cmake\")\n"
    "endif()\n"
    "file(INSTALL DESTINATION \"\${CMAKE_INSTALL_PREFIX}/share/doc/libmoq\" TYPE FILE FILES \"/src/LICENSE.txt\")\n")
file(WRITE "${scripts}/core/cmake_install.cmake"
    "file(INSTALL DESTINATION \"\${CMAKE_INSTALL_PREFIX}/lib\" TYPE STATIC_LIBRARY FILES \"/build/libmoq-core.a\")\n"
    "file(INSTALL DESTINATION \"\${CMAKE_INSTALL_PREFIX}/include\" TYPE DIRECTORY FILES \"/src/core/include/moq\")\n"
    "include(\"${scripts}/cmake_install.cmake\")\n")
tier_install_destinations("${scripts}" dests why)
control_equal("destinations: read" "${why}" "")
control_equal("destinations: every script, each once" "${dests}"
    "\${CMAKE_INSTALL_PREFIX}/share/doc/libmoq;\${CMAKE_INSTALL_PREFIX}/lib;\${CMAKE_INSTALL_PREFIX}/include")
tier_install_destinations("${MOQ_WORK}/no-build" dests why)
control_equal("destinations: no install script" "${why}"
    "the build has no install script: ${MOQ_WORK}/no-build/cmake_install.cmake")

tier_destination_verdict(
    "\${CMAKE_INSTALL_PREFIX}/lib;\${CMAKE_INSTALL_PREFIX}/include/moq;\${CMAKE_INSTALL_PREFIX};\${CMAKE_INSTALL_PREFIX}/lib/../include"
    why)
control_equal("destinations inside the prefix" "${why}" "")
# A relative include directory that climbs out of the prefix. An absolute-only
# check passes it.
tier_destination_verdict(
    "\${CMAKE_INSTALL_PREFIX}/lib;\${CMAKE_INSTALL_PREFIX}/../headers" why)
control_equal("destinations: relative include escape" "${why}"
    "install destinations outside the prefix, so nothing was installed: \${CMAKE_INSTALL_PREFIX}/../headers")
tier_destination_verdict(
    "\${CMAKE_INSTALL_PREFIX}/share/../../outside-doc" why)
control_equal("destinations: documentation escape" "${why}"
    "install destinations outside the prefix, so nothing was installed: \${CMAKE_INSTALL_PREFIX}/share/../../outside-doc")
tier_destination_verdict("\${CMAKE_INSTALL_PREFIX}/lib;/opt/moq/include" why)
control_equal("destinations: absolute" "${why}"
    "install destinations outside the prefix, so nothing was installed: /opt/moq/include")
tier_destination_verdict("\${CMAKE_INSTALL_PREFIX}/lib;\${OTHER}/lib" why)
control_equal("destinations: unresolved" "${why}"
    "install destinations this test cannot resolve, so nothing was installed: \${OTHER}/lib")

# == 3. An install that wrote outside its prefix anyway ========================
set(stage "${MOQ_WORK}/stage")
set(staged_prefix "${stage}/work/sdk")
file(WRITE "${staged_prefix}/lib/pkgconfig/libmoq-core.pc" "")
tier_stage_escapes("${stage}" "${staged_prefix}" escaped)
control_equal("stage confined" "${escaped}" "")
file(WRITE "${stage}/opt/moq/include/moq/session.h" "")
tier_stage_escapes("${stage}" "${staged_prefix}" escaped)
control_equal("stage escape" "${escaped}"
    "${stage}/opt/moq/include/moq/session.h")
# A sibling whose name only begins like the prefix is outside it.
file(REMOVE_RECURSE "${stage}/opt")
file(WRITE "${stage}/work/sdk-other/file" "")
tier_stage_escapes("${stage}" "${staged_prefix}" escaped)
control_equal("stage sibling" "${escaped}" "${stage}/work/sdk-other/file")

# == 4. The producer's context, per language ==================================
set(context "${MOQ_WORK}/context.cmake.json")
file(WRITE "${context}"
    "{\"C\":[\"-DTIER_C_ONLY\",\"-arch\",\"x86_64\"],\"CXX\":[\"-DTIER_CXX_ONLY\",\"-arch\",\"x86_64\"],\"LINK\":[\"-Wl,-dead_strip\"]}\n")
set(CMAKE_BUILD_TYPE "Release")
tier_context_flags("${context}" "Release" c_flags cxx_flags link_flags)
control_equal("context C" "${c_flags}" "-DTIER_C_ONLY;-arch;x86_64")
control_equal("context C++" "${cxx_flags}" "-DTIER_CXX_ONLY;-arch;x86_64")
control_equal("context link" "${link_flags}" "-Wl,-dead_strip")
# A multi-configuration producer records no build type: the selected
# configuration's own flags are added, per language.
set(CMAKE_BUILD_TYPE "")
set(CMAKE_C_FLAGS_DEBUG "-g -DTIER_C_DEBUG")
set(CMAKE_CXX_FLAGS_DEBUG "-g -DTIER_CXX_DEBUG")
set(CMAKE_EXE_LINKER_FLAGS_DEBUG "-Wl,-tier_debug")
tier_context_flags("${context}" "Debug" c_flags cxx_flags link_flags)
control_equal("context C, selected configuration" "${c_flags}"
    "-DTIER_C_ONLY;-arch;x86_64;-g;-DTIER_C_DEBUG")
control_equal("context C++, selected configuration" "${cxx_flags}"
    "-DTIER_CXX_ONLY;-arch;x86_64;-g;-DTIER_CXX_DEBUG")
control_equal("context link, selected configuration" "${link_flags}"
    "-Wl,-dead_strip;-Wl,-tier_debug")
# The C++ consumer is given the C++ flags and no C flag.
tier_consumer_command(CXX "c++" "${cxx_flags}" "-I/sdk/include" "main.cpp"
    "main" "-L/sdk/lib;-lmoq-core" "${link_flags}" "/sdk/lib" command)
control_equal("C++ command" "${command}"
    "c++;-std=c++17;-Wall;-Wextra;-Werror;-DTIER_CXX_ONLY;-arch;x86_64;-g;-DTIER_CXX_DEBUG;-I/sdk/include;main.cpp;-o;main;-L/sdk/lib;-lmoq-core;-Wl,-dead_strip;-Wl,-tier_debug;-Wl,-rpath,/sdk/lib")
tier_consumer_command(C "cc" "${c_flags}" "-I/sdk/include" "main.c" "main"
    "-L/sdk/lib;-lmoq-core" "${link_flags}" "/sdk/lib" command)
control_equal("C command" "${command}"
    "cc;-std=c11;-Wall;-Wextra;-Wpedantic;-Werror;-DTIER_C_ONLY;-arch;x86_64;-g;-DTIER_C_DEBUG;-I/sdk/include;main.c;-o;main;-L/sdk/lib;-lmoq-core;-Wl,-dead_strip;-Wl,-tier_debug;-Wl,-rpath,/sdk/lib")
set(CMAKE_C_FLAGS_DEBUG "")
set(CMAKE_CXX_FLAGS_DEBUG "")
set(CMAKE_EXE_LINKER_FLAGS_DEBUG "")

# == 5. Sysroots, through the real context writer ==============================
# A project with no languages, so no compiler is checked against a sysroot
# that does not exist. The writer records what the producer configured; the
# verdict is reached from that record exactly as the consumer test reaches it.
foreach(var MOQ_GENERATOR CTEST_COMMAND)
    if(NOT DEFINED ${var} OR "${${var}}" STREQUAL "")
        message(FATAL_ERROR "${var} is required")
    endif()
endforeach()
set(project "${MOQ_WORK}/context-project")
file(WRITE "${project}/CMakeLists.txt"
    "cmake_minimum_required(VERSION 3.20)\n"
    "project(tier_context NONE)\n"
    "include(\"${MOQ_SOURCE}/tests/cmake/ConsumerBuildContext.cmake\")\n"
    "moq_write_consumer_context(\"\${CMAKE_BINARY_DIR}/context.cmake\")\n")
set(generator_args -G "${MOQ_GENERATOR}")
if(DEFINED MOQ_MAKE_PROGRAM AND NOT MOQ_MAKE_PROGRAM STREQUAL "")
    list(APPEND generator_args "-DCMAKE_MAKE_PROGRAM=${MOQ_MAKE_PROGRAM}")
endif()

function(control_sysroot name expected_verdict expected_c_flag)
    set(build "${MOQ_WORK}/context-${name}")
    tier_run("context project ${name}" ${clean_env} "${CMAKE_COMMAND}"
        ${generator_args} --no-warn-unused-cli -S "${project}" -B "${build}"
        "-DCMAKE_OSX_SYSROOT=" ${ARGN})
    control_child(sysroot-verdict rc text "-DTIER_CONTEXT=${build}/context.cmake")
    if(NOT rc EQUAL 0)
        message(FATAL_ERROR "control sysroot ${name}: the verdict child failed (${rc}):\n${text}")
    endif()
    string(FIND "${text}" "verdict=[${expected_verdict}]" at)
    if(at EQUAL -1)
        message(FATAL_ERROR
            "control sysroot ${name}: expected verdict [${expected_verdict}]:\n${text}")
    endif()
    if(NOT expected_c_flag STREQUAL "")
        string(FIND "${text}" "${expected_c_flag}" at)
        if(at EQUAL -1)
            message(FATAL_ERROR
                "control sysroot ${name}: the C flags lack ${expected_c_flag}:\n${text}")
        endif()
    endif()
    message(STATUS "control sysroot ${name}: expected verdict")
endfunction()

# Separate compile and link sysroots cannot be given to one direct call.
control_sysroot(split
    "the producer compiles against sysroot /tier/compile and links against sysroot /tier/link, which a direct compiler call cannot represent, so nothing was installed"
    "" "-DCMAKE_SYSROOT_COMPILE=/tier/compile" "-DCMAKE_SYSROOT_LINK=/tier/link")
# Only one of them, over a different CMAKE_SYSROOT, cannot either.
control_sysroot(compile-only
    "the producer compiles against sysroot /tier/compile and links against sysroot /tier/root, which a direct compiler call cannot represent, so nothing was installed"
    "" "-DCMAKE_SYSROOT=/tier/root" "-DCMAKE_SYSROOT_COMPILE=/tier/compile")
# One sysroot for both is carried in the compiler flags the consumer is given.
control_sysroot(single "" "--sysroot=/tier/root" "-DCMAKE_SYSROOT=/tier/root")
control_sysroot(none "" "")

# An Apple SDK beside a sysroot, decided directly: a project naming an SDK
# that does not exist may not configure on every host.
tier_sysroot_verdict("/tier/sdk" "/tier/root" "" "" why)
control_equal("sysroot Apple SDK and sysroot" "${why}"
    "the producer names an Apple SDK (/tier/sdk) and a sysroot (compile: /tier/root, link: /tier/root), which a direct compiler call cannot both be given, so nothing was installed")
tier_sysroot_verdict("/tier/sdk" "" "" "" why)
control_equal("sysroot Apple SDK alone" "${why}" "")

# == 6. Running and inspecting a consumer built for another machine ===========
tier_run_plan(0 "" run_prefix why)
control_equal("run native: prefix" "${run_prefix}" "")
control_equal("run native: verdict" "${why}" "")
tier_run_plan(1 "" run_prefix why)
control_equal("run foreign, no emulator" "${why}"
    "the SDK is cross-compiled and no emulator is configured: the consumers were built but not run")
tier_run_plan(1 "qemu-arm;-L;/sysroot" run_prefix run_why)
control_equal("run foreign, emulator: prefix" "${run_prefix}"
    "qemu-arm;-L;/sysroot")
control_equal("run foreign, emulator: verdict" "${run_why}" "")
# Under the emulator the consumers run, but the loaded-image check is not
# made, so the run must report itself unsupported rather than complete.
tier_inspection_plan("${run_why}" "${run_prefix}" "DYLD_PRINT_LIBRARIES=1"
    Darwin inspect note)
control_equal("inspection under an emulator: made" "${inspect}" "FALSE")
control_equal("inspection under an emulator: note" "${note}"
    "the consumers ran under the configured emulator, but which libraries they loaded was not inspected")
tier_inspection_plan("" "" "" FreeBSD inspect note)
control_equal("inspection, no tracer: made" "${inspect}" "FALSE")
control_equal("inspection, no tracer: note" "${note}"
    "loaded-image inspection is not implemented for host FreeBSD: where the consumers' libraries were loaded from was not inspected")
tier_inspection_plan("not run" "" "LD_DEBUG=libs" Linux inspect note)
control_equal("inspection, not run: made" "${inspect}" "FALSE")
control_equal("inspection, not run: note" "${note}" "")
tier_inspection_plan("" "" "LD_DEBUG=libs" Linux inspect note)
control_equal("inspection native: made" "${inspect}" "TRUE")
control_equal("inspection native: note" "${note}" "")

# == 7. The link line, in order ================================================
tier_expected(libmoq-media expected source)
control_equal("media sequence" "${expected}"
    "-lmoq-msf;-lmoq-media-object;-lmoq-cmaf;-lmoq-mpeg2ts;-lmoq-loc;-lmoq-core")
tier_check_link_line(libmoq-media "--static" "${expected}" "${expected}" why)
control_equal("link line as expected" "${why}" "")
# The same names with a dependency ahead of its dependent.
tier_check_link_line(libmoq-media "--static"
    "-lmoq-core;-lmoq-msf;-lmoq-media-object;-lmoq-cmaf;-lmoq-mpeg2ts;-lmoq-loc"
    "${expected}" why)
control_equal("link line out of order" "${why}"
    "libmoq-media: static link line names [-lmoq-core;-lmoq-msf;-lmoq-media-object;-lmoq-cmaf;-lmoq-mpeg2ts;-lmoq-loc], expected [${expected}]")
tier_expected(libmoq-sim expected source)
tier_check_link_line(libmoq-sim "" "-lmoq-sim;-lmoq-core;-lmoq-core"
    "${expected}" why)
control_equal("link line repeats a name" "${why}"
    "libmoq-sim: link line names [-lmoq-sim;-lmoq-core;-lmoq-core], expected [-lmoq-sim;-lmoq-core]")
tier_check_link_line(libmoq-sim "--static" "-lmoq-sim;-lmoq-extra;-lmoq-core"
    "${expected}" why)
control_equal("link line names another library" "${why}"
    "libmoq-sim: static link line names [-lmoq-sim;-lmoq-extra;-lmoq-core], expected [-lmoq-sim;-lmoq-core]")

# == 8. Where the returned flags point ========================================
set(sdk "${MOQ_WORK}/relocated sdk")
file(MAKE_DIRECTORY "${sdk}/include" "${sdk}/lib" "${MOQ_WORK}/elsewhere/lib"
    "${MOQ_WORK}/relocated sdk-other/lib")
tier_check_flags(libmoq-sim "--libs" "-L${sdk}/lib;-lmoq-sim;-lmoq-core"
    "${sdk}" "${expected}" why)
control_equal("flags inside the SDK" "${why}" "")
tier_check_flags(libmoq-sim "--libs"
    "-L${MOQ_WORK}/elsewhere/lib;-lmoq-sim;-lmoq-core" "${sdk}" "${expected}"
    why)
control_equal("flags outside the SDK" "${why}"
    "libmoq-sim: --libs names a path outside the installed SDK: -L${MOQ_WORK}/elsewhere/lib")
tier_check_flags(libmoq-sim "--libs"
    "-L${MOQ_WORK}/relocated sdk-other/lib;-lmoq-sim;-lmoq-core" "${sdk}"
    "${expected}" why)
control_equal("flags in a sibling of the SDK" "${why}"
    "libmoq-sim: --libs names a path outside the installed SDK: -L${MOQ_WORK}/relocated sdk-other/lib")
tier_check_flags(libmoq-sim "--libs"
    "-L${sdk}/lib;${sdk}/lib/libmoq-core.a" "${sdk}" "${expected}" why)
control_equal("flags name an archive by path" "${why}"
    "libmoq-sim: --libs names an unexpected flag: ${sdk}/lib/libmoq-core.a")
tier_check_flags(libmoq-sim "--cflags" "-I${sdk}/include;-DMOQ_OTHER"
    "${sdk}" "" why)
control_equal("flags carry a definition" "${why}"
    "libmoq-sim: --cflags names an unexpected flag: -DMOQ_OTHER")

# == 9. The link with one library hidden: its transcript ======================
set(clang_summary
    "clang: error: linker command failed with exit code 1 (use -v to see invocation)")
tier_hidden_link_verdict(libmoq-sim moq-sim 1
    "ld: library 'moq-sim' not found\n${clang_summary}\n" why)
control_equal("hidden link: missing library, ld64" "${why}" "")
tier_hidden_link_verdict(libmoq-sim moq-sim 1
    "/usr/bin/ld: cannot find -lmoq-sim: No such file or directory\ncollect2: error: ld returned 1 exit status\n"
    why)
control_equal("hidden link: missing library, GNU ld" "${why}" "")
tier_hidden_link_verdict(libmoq-sim moq-sim 1
    "ld.lld: error: unable to find library -lmoq-sim\n${clang_summary}\n" why)
control_equal("hidden link: missing library, lld" "${why}" "")
# The expected failure with an unrelated warning beside it.
tier_hidden_link_verdict(libmoq-sim moq-sim 1
    "ld: warning: deliberate tier control warning\nld: library 'moq-sim' not found\n${clang_summary}\n"
    why)
control_equal("hidden link: unrelated warning beside the failure" "${why}"
    "libmoq-sim: unexpected output from the link with moq-sim hidden: ld: warning: deliberate tier control warning")
# Another library reported missing than the one hidden.
tier_hidden_link_verdict(libmoq-sim moq-sim 1
    "ld: library 'moq-core' not found\n${clang_summary}\n" why)
control_equal("hidden link: another library missing" "${why}"
    "libmoq-sim: unexpected output from the link with moq-sim hidden: ld: library 'moq-core' not found")
tier_hidden_link_verdict(libmoq-sim moq-sim 1 "${clang_summary}\n" why)
control_equal("hidden link: no missing library reported" "${why}"
    "libmoq-sim: the link with moq-sim hidden failed without reporting moq-sim missing (1):\n${clang_summary}\n")
tier_hidden_link_verdict(libmoq-sim moq-sim 0 "" why)
control_equal("hidden link: link still succeeds" "${why}"
    "libmoq-sim: the consumer still links with the SDK's moq-sim hidden, so moq-sim came from outside the installed SDK")
# The expected records, but the link did not end as a driver does when its
# linker fails: a crash, by name or by number.
set(ld64_pair "ld: library 'moq-sim' not found\n${clang_summary}\n")
tier_hidden_link_verdict(libmoq-sim moq-sim "Segmentation fault" "${ld64_pair}"
    why)
control_equal("hidden link: abnormal status" "${why}"
    "libmoq-sim: the link with moq-sim hidden ended with status Segmentation fault, not the driver's normal failure status 1:\n${ld64_pair}")
tier_hidden_link_verdict(libmoq-sim moq-sim 139 "${ld64_pair}" why)
control_equal("hidden link: abnormal numeric status" "${why}"
    "libmoq-sim: the link with moq-sim hidden ended with status 139, not the driver's normal failure status 1:\n${ld64_pair}")
# An unrelated error whose line only ends like a missing-library record.
tier_hidden_link_verdict(libmoq-sim moq-sim 1
    "ld: error: unrelated input failure; cannot find -lmoq-sim\ncollect2: error: ld returned 1 exit status\n"
    why)
control_equal("hidden link: polluted prefix" "${why}"
    "libmoq-sim: unexpected output from the link with moq-sim hidden: ld: error: unrelated input failure_ cannot find -lmoq-sim")
tier_hidden_link_verdict(libmoq-sim moq-sim 1
    "/usr/bin/ld: cannot open output file main: cannot find -lmoq-sim\ncollect2: error: ld returned 1 exit status\n"
    why)
control_equal("hidden link: polluted record" "${why}"
    "libmoq-sim: unexpected output from the link with moq-sim hidden: /usr/bin/ld: cannot open output file main: cannot find -lmoq-sim")
# A summary whose linker exit code does not match a missing library.
tier_hidden_link_verdict(libmoq-sim moq-sim 1
    "ld: library 'moq-sim' not found\nclang: error: linker command failed with exit code 2 (use -v to see invocation)\n"
    why)
control_equal("hidden link: inconsistent summary status" "${why}"
    "libmoq-sim: the driver reports linker exit code 2 for the link with moq-sim hidden, not the 1 a missing library gives")
tier_hidden_link_verdict(libmoq-sim moq-sim 1
    "/usr/bin/ld: cannot find -lmoq-sim\ncollect2: error: ld returned 2 exit status\n"
    why)
control_equal("hidden link: inconsistent summary status, GNU" "${why}"
    "libmoq-sim: the driver reports linker exit code 2 for the link with moq-sim hidden, not the 1 a missing library gives")
tier_hidden_link_verdict(libmoq-sim moq-sim 1
    "ld: library 'moq-sim' not found\n${clang_summary}\n${clang_summary}\n" why)
control_equal("hidden link: two summaries" "${why}"
    "libmoq-sim: the link with moq-sim hidden gave 2 driver summaries, not one:\nld: library 'moq-sim' not found\n${clang_summary}\n${clang_summary}\n")
tier_hidden_link_verdict(libmoq-sim moq-sim 1 "ld: library 'moq-sim' not found\n"
    why)
control_equal("hidden link: no summary" "${why}"
    "libmoq-sim: the link with moq-sim hidden gave 0 driver summaries, not one:\nld: library 'moq-sim' not found\n")

# == 10. Each library proven, against a partial fallback ======================
# The SDK supplies the simulator and the core; another directory, ahead of it
# on the link line as a producer flag would be, supplies a simulator only.
set(proof_sdk "${MOQ_WORK}/proof-sdk")
set(proof_other "${MOQ_WORK}/proof-other")
file(WRITE "${proof_sdk}/libmoq-sim.a" "")
file(WRITE "${proof_sdk}/libmoq-core.a" "")
set(names "moq-sim;moq-core")
set(files "${proof_sdk}/libmoq-sim.a;${proof_sdk}/libmoq-core.a")
tier_prove_each_library(libmoq-sim "${names}" "${files}"
    "${clean_env};/bin/sh;${fake_linker};-L${proof_other};-L${proof_sdk};-lmoq-sim;-lmoq-core"
    why)
control_equal("each library from the SDK" "${why}" "")
file(WRITE "${proof_other}/libmoq-sim.a" "")
tier_prove_each_library(libmoq-sim "${names}" "${files}"
    "${clean_env};/bin/sh;${fake_linker};-L${proof_other};-L${proof_sdk};-lmoq-sim;-lmoq-core"
    why)
control_equal("partial fallback" "${why}"
    "libmoq-sim: the consumer still links with the SDK's moq-sim hidden, so moq-sim came from outside the installed SDK")
foreach(file IN LISTS files)
    if(NOT EXISTS "${file}")
        message(FATAL_ERROR "control partial fallback: a hidden file was not put back: ${file}")
    endif()
endforeach()
message(STATUS "control partial fallback restores the hidden files: expected verdict")

# The same fallback reachable only through the C++ producer flags. Each
# language's consumer command is built as the consumer test builds it; the C
# command, whose flags name no other directory, is proven, and the C++ one is
# refused.
set(fake_driver "/bin/sh;${fake_linker}")
tier_prove_consumer(C "${fake_driver}" "" "-I${proof_sdk}" "main.c"
    "${MOQ_WORK}/proof-main-c" "-L${proof_sdk};-lmoq-sim;-lmoq-core" ""
    "${proof_sdk}" libmoq-sim "${names}" "${files}" "${clean_env}" why)
control_equal("C context, C++-only fallback" "${why}" "")
tier_prove_consumer(CXX "${fake_driver}" "-L${proof_other}" "-I${proof_sdk}"
    "main.cpp" "${MOQ_WORK}/proof-main-cxx"
    "-L${proof_sdk};-lmoq-sim;-lmoq-core" "" "${proof_sdk}" libmoq-sim
    "${names}" "${files}" "${clean_env}" why)
control_equal("C++ context partial fallback" "${why}"
    "libmoq-sim: the consumer still links with the SDK's moq-sim hidden, so moq-sim came from outside the installed SDK")
foreach(file IN LISTS files)
    if(NOT EXISTS "${file}")
        message(FATAL_ERROR "control C++ context partial fallback: a hidden file was not put back: ${file}")
    endif()
endforeach()

# Through the consumer runner, an unrelated warning beside the expected
# failure ends the run, and the whole transcript is kept.
control_child(hidden-link-warning rc text)
if(rc EQUAL 0)
    message(FATAL_ERROR
        "control hidden link with extra output: the warning beside the failure was accepted:\n${text}")
endif()
if(NOT text MATCHES "consumer command 1: unexpected diagnostic:" OR
   NOT text MATCHES "deliberate tier control warning")
    message(FATAL_ERROR
        "control hidden link with extra output: refused for another reason:\n${text}")
endif()
set(log "${MOQ_WORK}/hidden-link-warning/consumer-command-1.log")
if(NOT EXISTS "${log}")
    message(FATAL_ERROR "control hidden link with extra output: the transcript was not kept")
endif()
file(READ "${log}" kept)
if(NOT kept MATCHES "deliberate tier control warning" OR
   NOT kept MATCHES "ld: library 'moq-core' not found")
    message(FATAL_ERROR
        "control hidden link with extra output: the kept transcript is incomplete:\n${kept}")
endif()
message(STATUS "control hidden link with extra output: expected verdict")

# == 11. The environment a child inherits =====================================
set(poison "tier-control-poison")
set(poisoned DESTDIR PKG_CONFIG_PATH PKG_CONFIG_LIBDIR PKG_CONFIG_SYSROOT_DIR
    PKG_CONFIG_ALLOW_SYSTEM_LIBS CPATH C_INCLUDE_PATH CPLUS_INCLUDE_PATH
    OBJC_INCLUDE_PATH LIBRARY_PATH LD_LIBRARY_PATH LD_RUN_PATH
    DYLD_LIBRARY_PATH DYLD_FALLBACK_LIBRARY_PATH CMAKE_PREFIX_PATH)
foreach(name IN LISTS poisoned)
    set(ENV{${name}} "${poison}")
endforeach()
# Without the scrub a child sees them. The loader variables are left out of
# this half: some hosts drop them on the way to a child by themselves.
execute_process(COMMAND "${CMAKE_COMMAND}" -E environment
    RESULT_VARIABLE rc OUTPUT_VARIABLE inherited)
foreach(name IN ITEMS DESTDIR PKG_CONFIG_SYSROOT_DIR CPATH LIBRARY_PATH)
    if(NOT "${inherited}" MATCHES "(^|\n)${name}=${poison}(\n|$)")
        message(FATAL_ERROR
            "control environment: ${name} did not reach an unscrubbed child")
    endif()
endforeach()
tier_scrub_environment(poison_scrub)
execute_process(
    COMMAND "${CMAKE_COMMAND}" -E env ${poison_scrub}
        "DESTDIR=tier-control-owned" "${CMAKE_COMMAND}" -E environment
    RESULT_VARIABLE rc OUTPUT_VARIABLE scrubbed)
foreach(name IN LISTS poisoned)
    unset(ENV{${name}})
endforeach()
if(NOT rc EQUAL 0)
    message(FATAL_ERROR "control environment: the scrubbed child failed: ${rc}")
endif()
if("${scrubbed}" MATCHES "${poison}")
    message(FATAL_ERROR
        "control environment: an inherited setting reached the child:\n${scrubbed}")
endif()
if(NOT "${scrubbed}" MATCHES "(^|\n)DESTDIR=tier-control-owned(\n|$)")
    message(FATAL_ERROR
        "control environment: the test's own setting did not reach the child")
endif()
if(NOT "${scrubbed}" MATCHES "(^|\n)PATH=")
    message(FATAL_ERROR
        "control environment: the scrub removed an unrelated variable")
endif()
message(STATUS "control environment: expected verdict")

# == 12. A child that warns and succeeds; one that fails ======================
control_child(clean-success rc text)
if(NOT rc EQUAL 0 OR NOT text MATCHES "returned \\[tier control clean\\]")
    message(FATAL_ERROR "control clean child: ${rc}\n${text}")
endif()
message(STATUS "control clean child: expected verdict")

control_child(diagnostic-on-success rc text)
if(rc EQUAL 0)
    message(FATAL_ERROR
        "control diagnostic on success: a child that only warned was accepted")
endif()
if(NOT text MATCHES "consumer command 1: unexpected diagnostic:" OR
   NOT text MATCHES "deliberate tier control diagnostic")
    message(FATAL_ERROR
        "control diagnostic on success: refused for another reason:\n${text}")
endif()
set(log "${MOQ_WORK}/diagnostic-on-success/consumer-command-1.log")
if(NOT EXISTS "${log}")
    message(FATAL_ERROR
        "control diagnostic on success: the child's output was not kept")
endif()
file(READ "${log}" kept)
if(NOT kept MATCHES "deliberate tier control diagnostic")
    message(FATAL_ERROR
        "control diagnostic on success: the kept output lacks the diagnostic")
endif()
message(STATUS "control diagnostic on success: expected verdict")

control_child(nonzero-status rc text)
if(rc EQUAL 0 OR NOT text MATCHES "fixture link failed \\(1\\):")
    message(FATAL_ERROR "control nonzero status: ${rc}\n${text}")
endif()
message(STATUS "control nonzero status: expected verdict")

# == 13. The unsupported verdict is reported as skipped ========================
# A run that ends with the marker is neither a pass nor, with the consumer
# diagnostic gate also set on the test, a failure.
moq_consumer_warning_patterns(patterns)
set(dir "${MOQ_WORK}/unsupported-verdict")
file(MAKE_DIRECTORY "${dir}")
file(WRITE "${dir}/CTestTestfile.cmake"
    "add_test(unsupported \"${CMAKE_COMMAND}\" -E echo \"${TIER_UNSUPPORTED_MARKER} control\")\n"
    "set_tests_properties(unsupported PROPERTIES SKIP_REGULAR_EXPRESSION [==[${TIER_UNSUPPORTED_MARKER}]==] FAIL_REGULAR_EXPRESSION [==[${patterns}]==])\n")
execute_process(COMMAND "${CTEST_COMMAND}" --test-dir "${dir}"
    RESULT_VARIABLE rc OUTPUT_VARIABLE output ERROR_VARIABLE errors)
file(WRITE "${dir}/receipt.log" "${output}${errors}")
if(NOT rc EQUAL 0 OR NOT output MATCHES "Skipped" OR
   NOT output MATCHES "did not run")
    message(FATAL_ERROR
        "control unsupported verdict: not reported as skipped (${rc}):\n${output}${errors}")
endif()
message(STATUS "control unsupported verdict: expected verdict")

# == 14. The registration listing is metadata, not consumer output ===========
# CTest's listing quotes the consumer test's FAIL_REGULAR_EXPRESSION, the
# diagnostic patterns themselves, so the generic gate refuses it. The
# metadata path applies the patterns to stderr alone, keeps each stream and
# the status separately, and accepts exactly one listing of the intended
# test, command and patterns.
set(dir "${MOQ_WORK}/listing")
file(MAKE_DIRECTORY "${dir}")
set(script "${MOQ_SOURCE}/tests/cmake/TierPkgConfigConsumer.cmake")
set(work "${dir}/work")
# The FAIL_REGULAR_EXPRESSION values exactly as CTest printed them.
set(v1 [==["[Ww][Aa][Rr][Nn][Ii][Nn][Gg]:"]==])
set(v2 [==["CMake (Deprecation )?Warning"]==])
set(v3 [==["\\[-Werror"]==])
set(real "            ${v1},\n            ${v2},\n            ${v3}")
control_listing(listing "${script}" "${work}" "${real}" tier_pkgconfig_consumers)
file(WRITE "${dir}/good.json" "${listing}")

set(self_match 0)
foreach(pattern IN LISTS patterns)
    if("${listing}" MATCHES "${pattern}")
        set(self_match 1)
    endif()
endforeach()
control_equal("registration listing quotes the patterns" "${self_match}" "1")

control_child(listing-clean rc text "-DTIER_LISTING=${dir}/good.json"
    "-DTIER_SCRIPT=${script}")
string(FIND "${text}" "listing work=[${work}]" at)
if(NOT rc EQUAL 0 OR at EQUAL -1)
    message(FATAL_ERROR "control registration listing: ${rc}\n${text}")
endif()
set(kept "${MOQ_WORK}/listing-clean/consumer-command-1")
foreach(suffix log stdout stderr status)
    if(NOT EXISTS "${kept}.${suffix}")
        message(FATAL_ERROR
            "control registration listing: ${kept}.${suffix} was not kept")
    endif()
endforeach()
file(READ "${kept}.stdout" kept_stdout)
file(READ "${kept}.stderr" kept_stderr)
file(READ "${kept}.status" kept_status)
control_equal("registration listing: stdout kept" "${kept_stdout}" "${listing}")
control_equal("registration listing: stderr kept" "${kept_stderr}" "")
control_equal("registration listing: status kept" "${kept_status}" "0\n")
message(STATUS "control registration listing accepted: expected verdict")

control_child(listing-stderr-diagnostic rc text "-DTIER_LISTING=${dir}/good.json")
if(rc EQUAL 0 OR
   NOT text MATCHES "consumer command 1 stderr: unexpected diagnostic:" OR
   NOT text MATCHES "deliberate tier control diagnostic")
    message(FATAL_ERROR
        "control registration listing with a diagnostic: not refused for it (${rc}):\n${text}")
endif()
set(kept "${MOQ_WORK}/listing-stderr-diagnostic/consumer-command-1")
file(READ "${kept}.stdout" kept_stdout)
file(READ "${kept}.stderr" kept_stderr)
control_equal("registration listing with a diagnostic: stdout kept" "${kept_stdout}" "${listing}")
if(NOT kept_stderr MATCHES "deliberate tier control diagnostic")
    message(FATAL_ERROR
        "control registration listing with a diagnostic: stderr not kept: [${kept_stderr}]")
endif()
message(STATUS "control registration listing with a diagnostic: expected verdict")

# Each wrong listing is refused, for its own reason.
function(control_listing_refused name file_text expect)
    file(WRITE "${dir}/${name}.json" "${file_text}")
    control_child(listing-check rc text "-DTIER_LISTING=${dir}/${name}.json"
        "-DTIER_SCRIPT=${script}")
    string(FIND "${text}" "${expect}" at)
    if(rc EQUAL 0 OR at EQUAL -1)
        message(FATAL_ERROR
            "control registration listing ${name}: not refused for [${expect}] (${rc}):\n${text}")
    endif()
    message(STATUS "control registration listing ${name}: expected verdict")
endfunction()
set(doc "is not exactly one JSON document")
control_listing_refused("text after it" "${listing}\nunrelated tool output" "${doc}")
control_listing_refused("second closing line" "${listing}\n}" "${doc}")
control_listing_refused("text before it" "CMake Deprecation Notice\n${listing}" "${doc}")
control_listing(missing "${script}" "${work}" "            ${v1},\n            ${v2}"
    tier_pkgconfig_consumers)
control_listing_refused("pattern missing" "${missing}"
    "lists FAIL_REGULAR_EXPRESSION 1 time(s)")
control_listing(none "${script}" "${work}" "${real}" "")
control_listing_refused("no test" "${none}" "the listing names 0 tests")
control_listing(two "${script}" "${work}" "${real}"
    "tier_pkgconfig_consumers;tier_pkgconfig_consumers")
control_listing_refused("two tests" "${two}" "the listing names 2 tests")
control_listing(other "${script}" "${work}" "${real}" tier_pkgconfig_other)
control_listing_refused("another test" "${other}"
    "the listing names [tier_pkgconfig_other]")
control_listing(elsewhere "${dir}/Other.cmake" "${work}" "${real}"
    tier_pkgconfig_consumers)
control_listing_refused("another script" "${elsewhere}" "does not run")

message(STATUS "tier pkg-config controls: every verdict as expected")
