# One warning classifier for the CMake-script subprocesses these tests drive.
#
# A configure that succeeds, or that fails with exactly the intentional
# diagnostic under test, can still emit CMake/compiler/linker warnings. This
# workspace's warning policy applies to those paths too, so they are rejected
# rather than discarded -- while the intentional error diagnostics survive.
#
# SCOPE, stated honestly: this gate drives CMake, GCC/Clang and POSIX linker
# subprocesses, and covers those diagnostic forms plus their promoted variants.
# The MSVC `warning C4996:`-style form is matched as well so a future MSVC lane
# is not silently uncovered, but it is not exercised here and this is NOT
# claimed to be a complete compiler-warning classifier for every toolchain.
#
# PROMOTED warnings are the dangerous class: under -Werror a warning appears
# only as `error: ... [-Werror=...]` (GCC) or `error: ... [-W...]` (Clang) and
# would be invisible to a classifier that looks for the word "warning". That
# false-pass pattern has already been seen in this workspace's wtquic packaging
# gate; it is matched explicitly here. The word "error" is NOT broadly
# suppressed or broadly matched: only the promoted-warning TAG counts.
#
# Include this file and call _moq_reject_warnings(<tag> <combined output>).

function(_moq_find_warnings text out_lines)
    string(REPLACE "\r" "" _t "${text}")
    string(REPLACE "\n" ";" _lines "${_t}")
    set(_hits "")
    foreach(_line IN LISTS _lines)
        set(_hit FALSE)
        # CMake's own banners. `CMake Deprecation Warning` is a SEPARATE
        # banner -- the words are not contiguous -- so matching only
        # "CMake Warning" would miss it entirely.
        if(_line MATCHES "CMake Warning" OR _line MATCHES "CMake Deprecation Warning")
            set(_hit TRUE)
        # Compiler/linker diagnostics: file:line:col, file:line, or bare, and
        # the `ld:` / `/usr/bin/ld:` linker forms.
        elseif(_line MATCHES "(^|[ \t])warning:")
            set(_hit TRUE)
        # A warning promoted to an error by -Werror still carries its tag.
        elseif(_line MATCHES "error:.*\\[-W")
            set(_hit TRUE)
        # MSVC's form, for a future lane; not exercised by this POSIX gate.
        elseif(_line MATCHES "warning C[0-9]+:")
            set(_hit TRUE)
        endif()
        if(_hit)
            list(APPEND _hits "${_line}")
        endif()
    endforeach()
    set(${out_lines} "${_hits}" PARENT_SCOPE)
endfunction()

function(_moq_reject_warnings tag text)
    _moq_find_warnings("${text}" _w)
    if(_w)
        string(REPLACE ";" "\n  " _wtext "${_w}")
        message(SEND_ERROR
            "FAIL[${tag}.warning]: the subprocess emitted warnings, which are "
            "failures on both the successful and the expected-failure paths:\n"
            "  ${_wtext}")
    endif()
endfunction()

# --- permanent controls, run at include time -------------------------------
# Independent of any subprocess result, so a broken classifier cannot pass
# silently. Each case names what it covers.
function(_moq_wc_expect name text should_hit)
    _moq_find_warnings("${text}" _h)
    if(should_hit AND NOT _h)
        message(SEND_ERROR
            "FAIL[warning_classifier.${name}]: this warning form was NOT "
            "detected, so it would pass invisibly on a subprocess path.")
    elseif(NOT should_hit AND _h)
        message(SEND_ERROR
            "FAIL[warning_classifier.${name}]: this was classified as a "
            "warning when it must not be.")
    endif()
endfunction()

_moq_wc_expect(cmake_warning "CMake Warning at x.cmake:1 (message):\n  synthetic\n" TRUE)
_moq_wc_expect(cmake_deprecation "CMake Deprecation Warning at CMakeLists.txt:1 (cmake_minimum_required):\n  Compatibility with CMake < 3.10 will be removed\n" TRUE)
_moq_wc_expect(cmake_dev_warning "CMake Warning (dev) at x.cmake:1:\n  synthetic\n" TRUE)
_moq_wc_expect(gcc_warning "/tmp/x.c:3:5: warning: unused variable 'v' [-Wunused-variable]\n" TRUE)
_moq_wc_expect(clang_warning "/tmp/x.c:3:5: warning: unused variable 'v' [-Wunused-variable]\n" TRUE)
_moq_wc_expect(warning_line_only "/tmp/x.c:3: warning: implicit declaration\n" TRUE)
_moq_wc_expect(bare_warning "warning: something happened\n" TRUE)
_moq_wc_expect(linker_warning "ld: warning: directory not found for option '-L/nope'\n" TRUE)
_moq_wc_expect(gnu_linker_warning "/usr/bin/ld: warning: cannot find entry symbol\n" TRUE)
_moq_wc_expect(gcc_promoted "/tmp/x.c:3:5: error: unused variable 'v' [-Werror=unused-variable]\n" TRUE)
_moq_wc_expect(clang_promoted "/tmp/x.c:3:5: error: unused variable 'v' [-Wunused-variable]\n" TRUE)
_moq_wc_expect(msvc_warning "x.c(3): warning C4996: 'strcpy': This function may be unsafe\n" TRUE)
_moq_wc_expect(clean_record "-- Configuring done\n-- Generating done\n" FALSE)
_moq_wc_expect(intentional_error "CMake Error at x.cmake:1 (message):\n  LIBMOQ_PICOQUIC_PROVIDER_FACTS_UNKNOWN: missing=WITH_MBEDTLS\n" FALSE)
