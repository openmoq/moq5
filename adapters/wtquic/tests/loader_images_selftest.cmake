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
