# The loader-image helper's decisions, driven by captured fixture text.
#
# The helper decides what a real consumer run proved, so its own mistakes are
# invisible to that run: a parser that matches nothing turns a quiet success
# into a false pass. Every case here is fixture text with a known verdict, and
# both supported platforms are exercised on whichever host runs this.
#
# No process is started and no library is loaded.
include(${CMAKE_CURRENT_LIST_DIR}/loader_images.cmake)

set(_fail 0)
set(_cases 0)
function(expect label got want)
    math(EXPR _cases "${_cases} + 1")
    set(_cases "${_cases}" PARENT_SCOPE)
    if(NOT "${got}" STREQUAL "${want}")
        message("FAIL: ${label}: got [${got}], want [${want}]")
        set(_fail 1 PARENT_SCOPE)
    endif()
endfunction()
function(expect_nonempty label got)
    math(EXPR _cases "${_cases} + 1")
    set(_cases "${_cases}" PARENT_SCOPE)
    if("${got}" STREQUAL "")
        message("FAIL: ${label}: expected a refusal, got none")
        set(_fail 1 PARENT_SCOPE)
    endif()
endfunction()
function(expect_empty label got)
    math(EXPR _cases "${_cases} + 1")
    set(_cases "${_cases}" PARENT_SCOPE)
    if(NOT "${got}" STREQUAL "")
        message("FAIL: ${label}: expected acceptance, got [${got}]")
        set(_fail 1 PARENT_SCOPE)
    endif()
endfunction()

# -- selected artifact kind --------------------------------------------------
loader_artifact_is_shared("/p/lib/libx.a" _v)
expect("archive is not a runtime image" "${_v}" "0")
loader_artifact_is_shared("/p/lib/libx.dylib" _v)
expect("Darwin shared library is a runtime image" "${_v}" "1")
loader_artifact_is_shared("/p/lib/libx.1.dylib" _v)
expect("versioned Darwin shared library is a runtime image" "${_v}" "1")
loader_artifact_is_shared("/p/lib/libx.so" _v)
expect("Linux shared library is a runtime image" "${_v}" "1")

# A consumer can report the libraries actually mapped in its own process,
# without enabling loader debug output during sanitizer initialization.
set(_image_list "MOQ_LOADED_IMAGE\t/p/lib/libmoq-core.so\nMOQ_LOADED_IMAGE\t/q/error directory/libmsquic.so")
loader_only_image_records("${_image_list}" ImageList _why)
expect_empty("direct image report accepted" "${_why}")
loader_parse_images("${_image_list}" ImageList _reported)
expect("direct image report paths preserved" "${_reported}"
       "/p/lib/libmoq-core.so;/q/error directory/libmsquic.so")
loader_parse_image_report(0 "${_image_list}" "" _accepted _why)
expect_empty("successful report with empty stderr accepted" "${_why}")
expect("successful report returns its images" "${_accepted}" "${_reported}")
foreach(_status 1 139 "Process terminated")
    loader_parse_image_report("${_status}" "${_image_list}" "" _accepted _why)
    expect_nonempty("failed process cannot supply trusted images: ${_status}" "${_why}")
    expect("failed process returns no trusted images" "${_accepted}" "")
endforeach()
foreach(_stderr "warning: unexpected output" "AddressSanitizer: failure"
                 "MOQ_LOADED_IMAGE\t/foreign/libmsquic.so" " ")
    loader_parse_image_report(0 "${_image_list}" "${_stderr}" _accepted _why)
    expect_nonempty("stderr is not an image channel: ${_stderr}" "${_why}")
    expect("stderr failure returns no trusted images" "${_accepted}" "")
endforeach()
loader_parse_image_report(0 "" "" _accepted _why)
expect_nonempty("empty direct report refused" "${_why}")
expect("empty report returns no trusted images" "${_accepted}" "")
foreach(_bad_report
    "MOQ_LOADED_IMAGE\trelative.so"
    "MOQ_LOADED_IMAGE\t"
    " MOQ_LOADED_IMAGE\t/foreign/libmsquic.so"
    "prefix MOQ_LOADED_IMAGE\t/p/lib/libmsquic.so"
    "MOQ_LOADED_IMAGE_OTHER\t/p/lib/libmsquic.so"
    "MOQ_LOADED_IMAGE\t/p/lib/libmsquic.so\nwarning: unexpected output"
    "MOQ_LOADED_IMAGE\t/p/lib/libmsquic.so\nERROR: AddressSanitizer: injected"
    "MOQ_LOADED_IMAGE\t/p/lib/libmsquic.so\n123: program: error: symbol lookup error: undefined symbol: needed (fatal)")
    loader_only_image_records("${_bad_report}" ImageList _why)
    expect_nonempty("malformed image report or adjacent diagnostic refused: ${_bad_report}"
                    "${_why}")
endforeach()
foreach(_padded "/foreign/libmsquic.so" "/p/lib/libmoq-core.so")
    loader_only_image_records("${_image_list}\n MOQ_LOADED_IMAGE\t${_padded}"
                              ImageList _why)
    expect_nonempty("padded foreign or duplicate image cannot disappear: ${_padded}" "${_why}")
endforeach()
loader_parse_images("MOQ_LOADED_IMAGE\t/path;with__SEMI__/libmsquic.so " ImageList _opaque)
expect("direct image path preserves separators and trailing space" "${_opaque}"
       "/path\\;with__SEMI__/libmsquic.so ")
loader_require_images("${_reported};/p/lib/libmoq-core.so"
    "/p/lib/libmoq-core.so;/q/error directory/libmsquic.so" "/p/lib" duplicate_report _why)
expect_nonempty("duplicate direct image remains a failure" "${_why}")
loader_require_images("${_reported};/foreign/libmsquic.so"
    "/p/lib/libmoq-core.so;/q/error directory/libmsquic.so" "/p/lib" foreign_report _why)
expect_nonempty("foreign direct image remains a failure" "${_why}")

# Keep these records in scalars: putting the counterexamples in a CMake list
# would corrupt them before they reached the parser under test.
set(_boundary_open "${_image_list}\nMOQ_LOADED_IMAGE\t/foreign/[scratch/libmsquic.so\nMOQ_LOADED_IMAGE\t/usr/lib/libc.so.6")
set(_boundary_close "${_image_list}\nMOQ_LOADED_IMAGE\t/foreign/]scratch/libmsquic.so\nMOQ_LOADED_IMAGE\t/usr/lib/libc.so.6")
set(_boundary_backslash "${_image_list}\nMOQ_LOADED_IMAGE\t/foreign/libmsquic.so\\\nMOQ_LOADED_IMAGE\t/usr/lib/libc.so.6")
set(_boundary_balanced "${_image_list}\nMOQ_LOADED_IMAGE\t/foreign/[scratch]/libmsquic.so")
set(_boundary_tab "${_image_list}\nMOQ_LOADED_IMAGE\t/foreign/libmsquic.so\textra")
string(REPLACE "libmoq-core" "libmoq-\rcore" _boundary_cr "${_image_list}")
set(_boundary_blank "${_image_list}\n\nMOQ_LOADED_IMAGE\t/usr/lib/libc.so.6")
set(_boundary_whitespace "${_image_list}\n \nMOQ_LOADED_IMAGE\t/usr/lib/libc.so.6")
set(_boundary_root "${_image_list}\nMOQ_LOADED_IMAGE\t/")
foreach(_case IN ITEMS open close backslash balanced tab cr blank whitespace root)
    set(_raw "${_boundary_${_case}}")
    loader_parse_image_report(0 "${_raw}" "" _accepted _why)
    set(_membership_why "${_why}")
    if(_membership_why STREQUAL "")
        loader_require_images("${_accepted}" "${_reported}" "/p/lib"
                              "boundary ${_case}" _membership_why)
    endif()
    expect_nonempty("boundary ${_case}: parse-to-membership refuses report" "${_membership_why}")
    expect_nonempty("boundary ${_case}: malformed raw report refused by parser" "${_why}")
    expect("boundary ${_case}: no trusted images" "${_accepted}" "")
    loader_parse_images("${_raw}" ImageList _direct)
    expect("boundary ${_case}: direct extraction also fails closed" "${_direct}" "")
endforeach()
loader_parse_image_report(0
    "MOQ_LOADED_IMAGE\t/path;with__SEMI__/space directory/libmsquic.so \n"
    "" _accepted _why)
expect_empty("opaque path full report accepted" "${_why}")
expect("opaque path full report preserves bytes" "${_accepted}"
       "/path\\;with__SEMI__/space directory/libmsquic.so ")
loader_require_images("${_accepted}"
    "/path\\;with__SEMI__/space directory/libmsquic.so " "" opaque_report _why)
expect_empty("opaque path full membership accepted" "${_why}")
loader_artifact_is_shared("/p/lib/libx.so.2.5.9" _v)
expect("versioned Linux shared library is a runtime image" "${_v}" "1")

# -- parsing, both platforms ------------------------------------------------
set(_darwin "dyld[123]: <AAAA-BBBB> /p/lib/libmoq-core.dylib
dyld[123]: <CCCC-DDDD> /q/lib/libwtquic.dylib
dyld[123]: <EEEE-FFFF> /usr/lib/libSystem.B.dylib")
loader_parse_images("${_darwin}" Darwin _d)
list(LENGTH _d _dn)
expect("darwin parse count" "${_dn}" "3")
list(GET _d 0 _d0)
expect("darwin first image" "${_d0}" "/p/lib/libmoq-core.dylib")

set(_linux "      1234:     find library=libmoq-core.so [0]; searching
      1234:     calling init: /p/lib/libmoq-core.so
      1234:     calling init: /q/lib/libwtquic.so")
loader_parse_images("${_linux}" Linux _l)
list(LENGTH _l _ln)
expect("linux parse count" "${_ln}" "2")
list(GET _l 1 _l1)
expect("linux second image" "${_l1}" "/q/lib/libwtquic.so")

# a Darwin parser on Linux text must see nothing -- the very confusion the
# platform split exists to prevent
loader_parse_images("${_linux}" Darwin _x)
list(LENGTH _x _xn)
expect("darwin parser on linux text sees nothing" "${_xn}" "0")

# -- the required-image decision -------------------------------------------
set(_imgs "/p/lib/a.dylib;/p/lib/b.dylib;/usr/lib/libSystem.B.dylib")
loader_require_images("${_imgs}" "/p/lib/a.dylib;/p/lib/b.dylib" "/p/lib" ok _why)
expect_empty("exact expected set accepted" "${_why}")

loader_require_images("${_imgs}" "/p/lib/a.dylib;/p/lib/b.dylib;/p/lib/c.dylib"
                      "/p/lib" missing _why)
expect_nonempty("a missing required image is refused" "${_why}")

# an extra image from a directory we own is refused: this is the shape the
# review's one-DSO fixture had
loader_require_images("/p/lib/a.dylib;/p/lib/rogue.dylib" "/p/lib/a.dylib"
                      "/p/lib" rogue _why)
expect_nonempty("an unexpected owned image is refused" "${_why}")

loader_require_images("/p/lib/a.dylib;/p/lib/a.dylib" "/p/lib/a.dylib"
                      "/p/lib" dup _why)
expect_nonempty("a duplicated image is refused" "${_why}")

# a path is a path: a regex-special character must not match loosely
loader_require_images("/p+x/lib/a.dylib" "/pXx/lib/a.dylib" "" regex _why)
expect_nonempty("paths are compared as paths, not regexes" "${_why}")

# -- an ADDITIONAL image of a dependency family, wherever it came from -------
# The families this gate owns are LibMoQ, WTQuic and MsQuic. A second copy from
# an unrelated directory is a mixed dependency, not a bystander.
set(_ok3 "/p/lib/libmoq-core.dylib;/q/lib/libwtquic.dylib;/m/lib/libmsquic.dylib")
set(_exp3 "/p/lib/libmoq-core.dylib;/q/lib/libwtquic.dylib;/m/lib/libmsquic.dylib")
loader_require_images("${_ok3}" "${_exp3}" "/p/lib;/q/lib" base _why)
expect_empty("the exact three-family set is accepted" "${_why}")

foreach(_extra "/elsewhere/libmsquic.dylib" "/elsewhere/libwtquic-x.dylib"
               "/elsewhere/libmoq-extra.dylib")
    loader_require_images("${_ok3};${_extra}" "${_exp3}" "/p/lib;/q/lib"
                          extra _why)
    expect_nonempty("an extra ${_extra} outside owned dirs is refused" "${_why}")
endforeach()

# unrelated system objects remain none of its business
loader_require_images("${_ok3};/usr/lib/libSystem.B.dylib;/usr/lib/libc++.1.dylib"
                      "${_exp3}" "/p/lib;/q/lib" sys _why)
expect_empty("unrelated system images are ignored" "${_why}")

# directory ownership respects component boundaries
loader_path_within("/p/lib-other/x.dylib" "/p/lib" _w)
expect("a sibling directory is not inside" "${_w}" "0")
loader_path_within("/p/lib/x.dylib" "/p/lib" _w)
expect("a real child is inside" "${_w}" "1")
loader_require_images("${_ok3};/p/lib-other/unrelated.dylib" "${_exp3}"
                      "/p/lib;/q/lib" nb _why)
expect_empty("a neighbour directory is not treated as owned" "${_why}")
loader_require_images("${_ok3};/p/lib/unrelated.dylib" "${_exp3}"
                      "/p/lib;/q/lib" own _why)
expect_nonempty("an unexpected image inside an owned dir is refused" "${_why}")

# -- only image records ------------------------------------------------------
loader_only_image_records("${_darwin}" Darwin _why)
expect_empty("a clean darwin trace is accepted" "${_why}")
loader_only_image_records("${_darwin}
ld: warning: something unexpected" Darwin _why)
expect_nonempty("an injected warning in the trace is refused" "${_why}")
loader_only_image_records("${_linux}" Linux _why)
expect_empty("a clean linux trace is accepted" "${_why}")

# glibc runs an executable's preinit array before its library initializers.
# This is loader metadata, not an additional shared-library image.
set(_linux_preinit
"      54152:\tcalling preinit: /work/tree-relay-asan-lsan/adapters/msquic/install-consumer-work/consumer-build/moq_msquic_consumer_test")
loader_only_image_records("${_linux_preinit}\n${_linux}" Linux _why)
expect_empty("glibc executable preinit is accepted" "${_why}")
loader_parse_images("${_linux_preinit}\n${_linux}" Linux _preinit_imgs)
expect("preinit is not a shared-library image" "${_preinit_imgs}" "${_l}")
set(_linux_preinit_nested
    "      54152: calling preinit: /work/calling init: /p/lib/libmsquic.so")
loader_only_image_records("${_linux_preinit_nested}" Linux _why)
expect_empty("preinit path with nested record words is accepted" "${_why}")
loader_parse_images("${_linux_preinit_nested}\n${_linux}" Linux _nested_imgs)
expect("preinit path cannot fabricate a library image" "${_nested_imgs}" "${_l}")
foreach(_bad_preinit
    "      54152: calling preinit: relative-program"
    "      54152: calling preinit:"
    "calling preinit: /work/consumer"
    "      54152: calling preinit failed: /work/consumer"
    "      54152: calling preinit: /work/consumer\nAddressSanitizer: injected failure"
    "      54152: calling preinit: /work/consumer\n      54152: warning: injected failure")
    loader_only_image_records("${_bad_preinit}" Linux _why)
    expect_nonempty("invalid preinit or adjacent diagnostic is refused: ${_bad_preinit}"
                    "${_why}")
endforeach()
# other loader record kinds are fine; a loader WARNING is not
loader_only_image_records("${_darwin}
dyld[123]: move loaded to delayed: XPCSupport" Darwin _why)
expect_empty("another dyld record kind is accepted" "${_why}")
loader_only_image_records("${_darwin}
dyld[123]: warning: something odd" Darwin _why)
expect_nonempty("a dyld warning is refused" "${_why}")
# the record KIND decides, not a word anywhere in the line: a valid image
# whose path contains "error" is data, not a diagnostic
loader_only_image_records(
"dyld[123]: <AAAA> /work/error-handling/lib/libmoq-core.dylib" Darwin _why)
expect_empty("a valid image path containing 'error' is accepted" "${_why}")
loader_only_image_records(
"dyld[123]: WARNING: injected diagnostic" Darwin _why)
expect_nonempty("an upper-case injected warning is refused" "${_why}")
loader_only_image_records(
"dyld[123]: some unknown record kind" Darwin _why)
expect_nonempty("an unknown dyld record kind is refused" "${_why}")
loader_only_image_records(
"      99:     calling init: /work/error-handling/lib/libmoq-core.so" Linux _why)
expect_empty("a linux image path containing 'error' is accepted" "${_why}")
loader_only_image_records("      99:     something unknown here" Linux _why)
expect_nonempty("an unknown linux record is refused" "${_why}")

# The sanitizer lane's captured dyld info is not an image or a warning. It
# requires both the exact record kind and a matching PID's actual ASan image.
set(_asan_image "dyld[123]: <8B994207-FB33-3A22-8385-7FB6F5343956> /toolchain/lib/libclang_rt.asan_osx_dynamic.dylib")
set(_asan_info "dyld[123]: has interposing tuples so cannot be delayed: libclang_rt.asan_osx_dynamic.dylib")
set(_asan_trace "${_darwin}\n${_asan_image}\n${_asan_info}")
loader_only_image_records("${_asan_trace}" Darwin _why TRUE)
expect_empty("exact ASan loader info on sanitizer lane accepted" "${_why}")
loader_only_image_records("${_asan_trace}" Darwin _why FALSE)
expect_nonempty("ASan info on ordinary lane refused" "${_why}")
loader_only_image_records("${_darwin}\n${_asan_info}" Darwin _why TRUE)
expect_nonempty("ASan info without runtime image refused" "${_why}")
string(REPLACE "dyld[123]" "dyld[456]" _other_pid "${_asan_info}")
loader_only_image_records("${_asan_image}\n${_other_pid}" Darwin _why TRUE)
expect_nonempty("ASan info from unmatched PID refused" "${_why}")
foreach(_injected
    "arbitrary injected output"
    "dyld[123]: arbitrary injected output"
    "has interposing tuples so cannot be delayed: libclang_rt.asan_osx_dynamic.dylib"
    "dyld[123]: has interposing tuples so cannot be delayed: libunknown.dylib"
    "${_asan_info} trailing injected text"
    "dyld[123]: warning: injected diagnostic")
    loader_only_image_records("${_asan_trace}\n${_injected}" Darwin _why TRUE)
    expect_nonempty("sanitizer lane rejects injected record: ${_injected}" "${_why}")
endforeach()
loader_parse_images("${_asan_trace}" Darwin _asan_imgs)
list(LENGTH _asan_imgs _asan_count)
expect("info is not parsed as an actual image" "${_asan_count}" "4")
loader_require_images("${_asan_imgs}" "/missing/libmsquic.dylib"
    "/p/lib;/q/lib" missing _why)
expect_nonempty("ASan info cannot supply a missing provider image" "${_why}")
loader_require_images("${_asan_imgs};/wrong/libwtquic.dylib"
    "/p/lib/libmoq-core.dylib;/q/lib/libwtquic.dylib"
    "/p/lib;/q/lib" wrong _why)
expect_nonempty("ASan info cannot excuse a wrong provider image" "${_why}")

# -- the named pre-main discriminator ---------------------------------------
loader_is_missing_library(133
"dyld[1]: Library not loaded: @rpath/libmoq-core.dylib
  Reason: no LC_RPATH's found" Darwin _v)
expect("darwin missing library recognised" "${_v}" "1")

# an ordinary application failure is NOT the loader failure
loader_is_missing_library(1 "consumer: create failed" Darwin _v)
expect("an application failure is not accepted" "${_v}" "0")
# nor a signal death with no loader message
loader_is_missing_library(139 "" Darwin _v)
expect("a signal death is not accepted" "${_v}" "0")
# nor a missing executable
loader_is_missing_library(127
"sh: No such file or directory: /work/consumer" Darwin _v)
expect("a missing executable is not accepted" "${_v}" "0")
# nor success
loader_is_missing_library(0
"dyld[1]: Library not loaded: x
  Reason: tried:" Darwin _v)
expect("a successful run is never the failure" "${_v}" "0")
loader_is_missing_library(127
"consumer: error while loading shared libraries: libmoq-core.so: cannot open shared object file: No such file"
Linux _v)
expect("linux missing library recognised" "${_v}" "1")
loader_is_missing_library(1 "consumer: create failed" Linux _v)
expect("a linux application failure is not accepted" "${_v}" "0")

if(_fail)
    message(FATAL_ERROR "loader-image helper selftest FAILED")
endif()
message(STATUS "PASS: ${_cases} loader-image helper cases")
