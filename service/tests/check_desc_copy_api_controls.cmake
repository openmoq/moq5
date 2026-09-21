# Controls for check_desc_copy_api.cmake: every way the gate could be greened
# falsely -- comment-only declaration, wrong signature, wrong floor, a suffixed
# or prefixed symbol, tool noise on either stream (with and without the
# expected missing-API error), a nonzero nm, a broken compiler, a missing
# library -- is exercised against scratch headers, scratch archives and
# wrapped tools, alongside the clean positive and the real product row. The
# gate must report each as the named fact and never as a different one.
#
#   cmake -DSCRIPT=<check_desc_copy_api.cmake> -DHEADER=<real header>
#         -DLIB=<real service archive> -DCC=<cc> -DAR=<ar> -DNM=<nm>
#         -DINCLUDES=<;-list> -DWORK=<scratch dir> -P check_desc_copy_api_controls.cmake
foreach(v SCRIPT HEADER LIB CC AR NM INCLUDES WORK)
    if(NOT DEFINED ${v})
        message(FATAL_ERROR "controls: ${v} not given")
    endif()
endforeach()
file(REMOVE_RECURSE "${WORK}")
file(MAKE_DIRECTORY "${WORK}")
set(_incs)
foreach(d ${INCLUDES})
    list(APPEND _incs "-I${d}")
endforeach()

# Scratch headers are derived from the REAL header (which declares the
# operation) by exact one-match textual edits, so each control removes or
# changes precisely the fact it tests and nothing else. A drift in the real
# declaration or floor macro fails the control loudly rather than silently
# testing a redeclaration.
file(READ "${HEADER}" _real)
set(DECL "MOQ_API moq_result_t moq_media_receiver_track_desc_copy(
    const moq_media_receiver_t *r, const moq_media_track_t *track,
    moq_media_track_desc_t *out, size_t out_size);")
set(FLOOR "#define MOQ_MEDIA_TRACK_DESC_V0_SIZE \\
    (offsetof(moq_media_track_desc_t, is_live) + sizeof(bool))")
# edit_header(<file> <old> <new>): exact one-match replacement in a scratch
# header; a missing or duplicated token is a loud control failure.
function(edit_header path old new)
    file(READ "${path}" _text)
    string(FIND "${_text}" "${old}" _at)
    if(_at EQUAL -1)
        message(FATAL_ERROR "controls: ${path} does not contain:\n${old}")
    endif()
    string(LENGTH "${old}" _len)
    math(EXPR _after "${_at} + ${_len}")
    string(SUBSTRING "${_text}" ${_after} -1 _rest)
    string(FIND "${_rest}" "${old}" _again)
    if(NOT _again EQUAL -1)
        message(FATAL_ERROR "controls: edit token occurs more than once in ${path}:\n${old}")
    endif()
    string(REPLACE "${old}" "${new}" _edited "${_text}")
    file(WRITE "${path}" "${_edited}")
endfunction()
foreach(h H_ok H_comment H_wrongsig H_wrongfloor)
    file(WRITE "${WORK}/${h}.h" "${_real}")
endforeach()
edit_header("${WORK}/H_comment.h"    "${DECL}"  "/* ${DECL} */")
edit_header("${WORK}/H_comment.h"    "${FLOOR}" "/* floor removed for control */")
edit_header("${WORK}/H_wrongsig.h"   "${DECL}"
    "MOQ_API moq_result_t moq_media_receiver_track_desc_copy(const moq_media_receiver_t *r, moq_media_track_desc_t *out);")
edit_header("${WORK}/H_wrongfloor.h" "${FLOOR}"
    "#define MOQ_MEDIA_TRACK_DESC_V0_SIZE (sizeof(moq_media_track_desc_t))")

# -- scratch archives ---------------------------------------------------------
file(WRITE "${WORK}/stub_ok.c"
"#include \"${WORK}/H_ok.h\"
moq_result_t moq_media_receiver_track_desc_copy(const moq_media_receiver_t *r, const moq_media_track_t *track, moq_media_track_desc_t *out, size_t out_size)
{ (void)r; (void)track; (void)out; (void)out_size; return MOQ_ERR_UNSUPPORTED; }
")
file(WRITE "${WORK}/stub_suffix.c"
"#include \"${WORK}/H_ok.h\"
moq_result_t moq_media_receiver_track_desc_copy2(const moq_media_receiver_t *r, const moq_media_track_t *track, moq_media_track_desc_t *out, size_t out_size);
moq_result_t moq_media_receiver_track_desc_copy2(const moq_media_receiver_t *r, const moq_media_track_t *track, moq_media_track_desc_t *out, size_t out_size)
{ (void)r; (void)track; (void)out; (void)out_size; return MOQ_ERR_UNSUPPORTED; }
moq_result_t xmoq_media_receiver_track_desc_copy(void);
moq_result_t xmoq_media_receiver_track_desc_copy(void) { return MOQ_OK; }
")
foreach(stub ok suffix)
    execute_process(COMMAND "${CC}" -std=c11 -c -Wall -Wextra -Werror ${_incs}
                            -o "${WORK}/stub_${stub}.o" "${WORK}/stub_${stub}.c"
                    RESULT_VARIABLE rc OUTPUT_VARIABLE out ERROR_VARIABLE err)
    if(NOT rc EQUAL 0)
        message(FATAL_ERROR "controls: cannot build stub_${stub}: ${out}${err}")
    endif()
    execute_process(COMMAND "${AR}" rcs "${WORK}/lib_${stub}.a" "${WORK}/stub_${stub}.o"
                    RESULT_VARIABLE rc OUTPUT_VARIABLE out ERROR_VARIABLE err)
    if(NOT rc EQUAL 0)
        message(FATAL_ERROR "controls: cannot archive stub_${stub}: ${out}${err}")
    endif()
endforeach()

# -- wrapped tools (real tool, controlled noise, real status) ------------------
file(WRITE "${WORK}/cc_warn.sh" "#!/bin/sh\n\"${CC}\" \"$@\"\nrc=$?\necho 'clang: warning: controlled compiler diagnostic' >&2\nexit $rc\n")
file(WRITE "${WORK}/nm_stderr.sh" "#!/bin/sh\n\"${NM}\" \"$@\"\necho 'nm: warning: controlled stderr diagnostic' >&2\nexit 0\n")
file(WRITE "${WORK}/nm_stdout.sh" "#!/bin/sh\necho 'nm: warning: controlled stdout diagnostic'\n\"${NM}\" \"$@\"\nexit 0\n")
file(WRITE "${WORK}/nm_nonzero.sh" "#!/bin/sh\necho '0000000000000000 T _moq_media_receiver_track_desc_copy'\nexit 1\n")
file(WRITE "${WORK}/cc_false.sh" "#!/bin/sh\nexit 1\n")
# Silent on the baseline TU, noisy only on the declaration probe: reaches the
# probe-stage classification with a warning beside the expected error.
file(WRITE "${WORK}/cc_warn_probe.sh" "#!/bin/sh\n\"${CC}\" \"$@\"\nrc=$?\ncase \"$*\" in *probe_fn*) echo 'clang: warning: controlled compiler diagnostic' >&2;; esac\nexit $rc\n")
# REAL promoted warning from the configured compiler: -Wundef plus a forced
# include that tests an undefined macro, applied only to the declaration
# probe (the baseline stays silent). Under -Werror the compiler reports it as
# an error tagged [-Werror,-Wundef] (clang) / [-Werror=undef] (gcc) beside
# the expected diagnostic.
file(WRITE "${WORK}/promoted.h" "#if MOQ5_CONTROL_UNDEFINED_MACRO\n#endif\n")
file(WRITE "${WORK}/cc_promoted_probe.sh" "#!/bin/sh\ncase \"$*\" in *probe_fn*) exec \"${CC}\" -Wundef -include \"${WORK}/promoted.h\" \"$@\";; esac\nexec \"${CC}\" \"$@\"\n")
# Injected promoted forms beside the real expected failure: gcc spelling and
# the bare tag.
file(WRITE "${WORK}/cc_gcc_promoted_probe.sh" "#!/bin/sh\n\"${CC}\" \"$@\"\nrc=$?\ncase \"$*\" in *probe_fn*) echo \"probe_fn.c:1:1: error: unused variable 'v' [-Werror=unused-variable]\" >&2;; esac\nexit $rc\n")
file(WRITE "${WORK}/cc_bare_promoted_probe.sh" "#!/bin/sh\n\"${CC}\" \"$@\"\nrc=$?\ncase \"$*\" in *probe_fn*) echo 'probe_fn.c:1:1: error: controlled diagnostic [-Werror]' >&2;; esac\nexit $rc\n")
foreach(s cc_warn cc_warn_probe cc_promoted_probe cc_gcc_promoted_probe cc_bare_promoted_probe nm_stderr nm_stdout nm_nonzero cc_false)
    file(CHMOD "${WORK}/${s}.sh" PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE)
endforeach()

# -- run the gate under each control ------------------------------------------
set(_failed 0)
# control(name header lib nm cc want_rc [pattern...]) -- a pattern starting
# with "!" must be ABSENT. want_rc 0 = must pass; 1 = must fail.
function(control name hdr lib nm cc want_rc)
    execute_process(COMMAND "${CMAKE_COMMAND}" "-DHEADER=${hdr}" "-DLIB=${lib}" "-DNM=${nm}" "-DCC=${cc}"
                            "-DINCLUDES=${INCLUDES}" "-DWORK=${WORK}/run_${name}" -P "${SCRIPT}"
                    RESULT_VARIABLE rc OUTPUT_VARIABLE out ERROR_VARIABLE err)
    # CMake wraps FATAL_ERROR text; match on a whitespace-normalized copy.
    string(REGEX REPLACE "[ \t\r\n]+" " " flat "${out}${err}")
    set(ok 1)
    if(want_rc EQUAL 0 AND NOT rc EQUAL 0)
        set(ok 0)
    elseif(want_rc EQUAL 1 AND rc EQUAL 0)
        set(ok 0)
    endif()
    foreach(pat IN LISTS ARGN)
        if(pat MATCHES "^!(.*)$")
            if(flat MATCHES "${CMAKE_MATCH_1}")
                set(ok 0)
                message(STATUS "  forbidden present: ${CMAKE_MATCH_1}")
            endif()
        elseif(NOT flat MATCHES "${pat}")
            set(ok 0)
            message(STATUS "  missing: ${pat}")
        endif()
    endforeach()
    string(REGEX MATCHALL "(PASS|FAIL)\\[[a-z.]+\\]" facts "${flat}")
    string(REPLACE ";" " " facts "${facts}")
    if(ok)
        message(STATUS "CONTROL PASS [${name}] rc=${rc}: ${facts}")
    else()
        message(STATUS "CONTROL FAIL [${name}] rc=${rc}: ${facts}\n${out}${err}")
        set(_failed 1 PARENT_SCOPE)
    endif()
endfunction()

set(W "${WORK}")
control(green         "${W}/H_ok.h"         "${W}/lib_ok.a"     "${NM}"            "${CC}"           0 "PASS\\[api.declared\\]" "PASS\\[api.signature\\]" "PASS\\[api.floor\\]" "PASS\\[api.exported\\]")
control(comment_only  "${W}/H_comment.h"    "${W}/lib_ok.a"     "${NM}"            "${CC}"           1 "FAIL\\[api.declared\\]" "FAIL\\[api.floor\\]: header does not define" "PASS\\[api.exported\\]" "contract RED")
control(wrong_sig     "${W}/H_wrongsig.h"   "${W}/lib_ok.a"     "${NM}"            "${CC}"           1 "PASS\\[api.declared\\]" "FAIL\\[api.signature\\]" "contract RED")
control(wrong_floor   "${W}/H_wrongfloor.h" "${W}/lib_ok.a"     "${NM}"            "${CC}"           1 "FAIL\\[api.floor\\]: MOQ_MEDIA_TRACK_DESC_V0_SIZE is not the end of is_live" "contract RED")
control(suffixed_sym  "${W}/H_ok.h"         "${W}/lib_suffix.a" "${NM}"            "${CC}"           1 "PASS\\[api.declared\\]" "FAIL\\[api.exported\\]" "contract RED")
control(nm_stderr     "${W}/H_ok.h"         "${W}/lib_ok.a"     "${W}/nm_stderr.sh" "${CC}"          1 "FAIL\\[tool.nm\\]: .* wrote to stderr" "!contract RED" "!PASS\\[api.exported\\]")
control(nm_stdout     "${W}/H_ok.h"         "${W}/lib_ok.a"     "${W}/nm_stdout.sh" "${CC}"          1 "FAIL\\[tool.nm\\]: .* non-symbol line to stdout" "!contract RED" "!PASS\\[api.exported\\]")
control(nm_nonzero    "${W}/H_ok.h"         "${W}/lib_ok.a"     "${W}/nm_nonzero.sh" "${CC}"         1 "FAIL\\[tool.nm\\]: .* exited 1" "!contract RED" "!PASS\\[api.exported\\]")
control(cc_warn_ok    "${W}/H_ok.h"         "${W}/lib_ok.a"     "${NM}"            "${W}/cc_warn.sh" 1 "FAIL\\[tool.compiler\\]: .* diagnostic on a successful compile" "!\\[api\\." "!contract RED")
control(cc_warn_absent "${W}/H_comment.h"   "${W}/lib_ok.a"     "${NM}"            "${W}/cc_warn.sh" 1 "FAIL\\[tool.compiler\\]" "!FAIL\\[api.declared\\]" "!contract RED")
control(cc_warn_probe_absent "${W}/H_comment.h" "${W}/lib_ok.a" "${NM}"          "${W}/cc_warn_probe.sh" 1 "FAIL\\[tool.probe\\]: .* warning diagnostic alongside the expected failure" "!FAIL\\[api.declared\\]" "!contract RED")
control(cc_warn_probe_ok "${W}/H_ok.h"       "${W}/lib_ok.a"     "${NM}"            "${W}/cc_warn_probe.sh" 1 "FAIL\\[tool.probe\\]: .* diagnostic on a successful compile" "!PASS\\[api.declared\\]" "!contract RED")
control(cc_promoted_absent "${W}/H_comment.h" "${W}/lib_ok.a" "${NM}"          "${W}/cc_promoted_probe.sh" 1 "FAIL\\[tool.probe\\]: .* promoted warning diagnostic alongside the expected failure" "-Werror" "!FAIL\\[api.declared\\]" "!contract RED")
control(cc_promoted_ok   "${W}/H_ok.h"      "${W}/lib_ok.a"     "${NM}"            "${W}/cc_promoted_probe.sh" 1 "FAIL\\[tool.probe\\]: .* promoted warning diagnostic alongside the expected failure" "!PASS\\[api.declared\\]" "!contract RED")
control(cc_gcc_promoted_absent "${W}/H_comment.h" "${W}/lib_ok.a" "${NM}"      "${W}/cc_gcc_promoted_probe.sh" 1 "FAIL\\[tool.probe\\]: .* promoted warning diagnostic" "!FAIL\\[api.declared\\]" "!contract RED")
control(cc_bare_promoted_absent "${W}/H_comment.h" "${W}/lib_ok.a" "${NM}"     "${W}/cc_bare_promoted_probe.sh" 1 "FAIL\\[tool.probe\\]: .* promoted warning diagnostic" "!FAIL\\[api.declared\\]" "!contract RED")
control(cc_broken     "${W}/H_ok.h"         "${W}/lib_ok.a"     "${NM}"            "${W}/cc_false.sh" 1 "FAIL\\[tool.compiler\\]" "!\\[api\\." "!contract RED")
control(lib_missing   "${W}/H_ok.h"         "${W}/nope.a"       "${NM}"            "${CC}"           1 "FAIL\\[tool.lib\\]" "!\\[api\\.")
control(real_green    "${HEADER}"           "${LIB}"            "${NM}"            "${CC}"           0 "PASS\\[api.declared\\]" "PASS\\[api.signature\\]" "PASS\\[api.floor\\]" "PASS\\[api.exported\\]" "!contract RED" "!FAIL\\[")

if(_failed)
    message(FATAL_ERROR "media_receiver_desc_copy_api_controls: a control did not hold")
endif()
