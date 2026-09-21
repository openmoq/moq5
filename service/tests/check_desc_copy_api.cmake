# API/export contract for the media-receiver current-description copy
# (prerequisite A), separate from the writer-lock finding. Three named facts,
# each established by a tool rather than by header text:
#
#   api.declared / api.signature  the public header declares
#       moq_result_t moq_media_receiver_track_desc_copy(
#           const moq_media_receiver_t *, const moq_media_track_t *,
#           moq_media_track_desc_t *, size_t)
#       -- proven by the CONFIGURED C compiler through a _Generic type match on
#       the declared function's address, so a comment cannot satisfy it and a
#       different signature is a distinct failure.
#   api.floor  MOQ_MEDIA_TRACK_DESC_V0_SIZE is defined and equals the end of
#       is_live -- proven by a _Static_assert in the same compiler.
#   api.exported  the built service library DEFINES the exact symbol (nm "T",
#       exact name, optional leading underscore) -- proven by the configured nm.
#
# Tool output is never discarded. A successful syntax-only compile must be
# SILENT; an expected compile failure must exit exactly 1 with no warning or
# promoted-warning diagnostic beside its error, or it is a tool outcome
# (tool.probe), not a classified API result. nm must exit 0, write nothing to
# stderr, and write
# only symbol lines / member headers to stdout. A compiler that cannot compile
# the header alone, or a missing library, is a tool failure (tool.*). Only
# api.* failures are the expected named RED until prerequisite A lands.
#
#   cmake -DHEADER=<media_receiver.h> -DLIB=<libmoq-service.a> -DNM=<nm>
#         -DCC=<c compiler> -DINCLUDES=<;-list> -DWORK=<scratch dir>
#         -P check_desc_copy_api.cmake
foreach(v HEADER LIB NM CC INCLUDES WORK)
    if(NOT DEFINED ${v})
        message(FATAL_ERROR "FAIL[tool.args]: ${v} not given")
    endif()
endforeach()
file(MAKE_DIRECTORY "${WORK}")
set(_cflags -std=c11 -fsyntax-only -Wall -Wextra -Werror)
set(_incs)
foreach(d ${INCLUDES})
    list(APPEND _incs "-I${d}")
endforeach()

# Compile one probe TU. Outcomes:
#   ok      rc 0 and no output at all
#   error   rc 1, no "warning:" and no "[-Werror" diagnostic, output in <out_text>
#   tool    anything else (silent-success violated, warning or promoted
#           warning present, other rc)
function(compile_probe name source out_kind out_text)
    file(WRITE "${WORK}/${name}.c" "${source}")
    execute_process(COMMAND "${CC}" ${_cflags} ${_incs} "${WORK}/${name}.c"
                    RESULT_VARIABLE rc OUTPUT_VARIABLE out ERROR_VARIABLE err)
    set(text "${out}${err}")
    set(${out_text} "${text}" PARENT_SCOPE)
    if(rc EQUAL 0)
        if("${text}" STREQUAL "")
            set(${out_kind} ok PARENT_SCOPE)
        else()
            set(${out_kind} "tool: diagnostic on a successful compile (rc 0)" PARENT_SCOPE)
        endif()
    elseif(rc EQUAL 1)
        # A promoted warning is an error line tagged with the promoting flag:
        # clang "[-Werror,-W<name>]", gcc "[-Werror=<name>]", bare "[-Werror]".
        if(text MATCHES "warning:")
            set(${out_kind} "tool: warning diagnostic alongside the expected failure (rc 1)" PARENT_SCOPE)
        elseif(text MATCHES "\\[-Werror")
            set(${out_kind} "tool: promoted warning diagnostic alongside the expected failure (rc 1)" PARENT_SCOPE)
        else()
            set(${out_kind} error PARENT_SCOPE)
        endif()
    else()
        set(${out_kind} "tool: unexpected compiler exit (rc ${rc})" PARENT_SCOPE)
    endif()
endfunction()

# -- tool validation ----------------------------------------------------------
if(NOT EXISTS "${LIB}")
    message(FATAL_ERROR "FAIL[tool.lib]: ${LIB} does not exist")
endif()
compile_probe(baseline "#include \"${HEADER}\"\nint moq5_desc_copy_api_baseline;\n" _kind _text)
if(NOT _kind STREQUAL "ok")
    message(FATAL_ERROR "FAIL[tool.compiler]: ${CC} cannot compile ${HEADER} alone silently (${_kind}):\n${_text}")
endif()

set(_fail 0)

# -- api.declared / api.signature (typed, compiler-checked) -------------------
compile_probe(probe_fn
"#include \"${HEADER}\"
#include <stddef.h>
typedef moq_result_t (*moq5_expected_fn)(const moq_media_receiver_t *,
                                         const moq_media_track_t *,
                                         moq_media_track_desc_t *, size_t);
_Static_assert(_Generic(&moq_media_receiver_track_desc_copy,
                        moq5_expected_fn: 1, default: 0),
               \"MOQ5_WRONG_SIGNATURE\");
int moq5_desc_copy_api_probe_fn;
" _kind _text)
# An undeclared identifier is reported first (clang: "use of undeclared
# identifier '<name>'"; gcc: "'<name>' undeclared"); only a DECLARED function
# of a different type reaches the static assertion alone.
if(_kind STREQUAL "ok")
    message(STATUS "PASS[api.declared]")
    message(STATUS "PASS[api.signature]")
elseif(NOT _kind STREQUAL "error")
    message(FATAL_ERROR "FAIL[tool.probe]: ${_kind}:\n${_text}")
elseif(_text MATCHES "undeclared identifier 'moq_media_receiver_track_desc_copy'" OR
       _text MATCHES "'moq_media_receiver_track_desc_copy' undeclared")
    message(STATUS "FAIL[api.declared]: header does not declare moq_media_receiver_track_desc_copy")
    set(_fail 1)
elseif(_text MATCHES "MOQ5_WRONG_SIGNATURE")
    message(STATUS "PASS[api.declared]")
    message(STATUS "FAIL[api.signature]: moq_media_receiver_track_desc_copy is declared with a different type")
    set(_fail 1)
else()
    message(FATAL_ERROR "FAIL[tool.probe]: unattributable compiler error:\n${_text}")
endif()

# -- api.floor (defined and equal to the end of is_live) ----------------------
compile_probe(probe_floor
"#include \"${HEADER}\"
#include <stddef.h>
#include <stdbool.h>
_Static_assert(MOQ_MEDIA_TRACK_DESC_V0_SIZE ==
               offsetof(moq_media_track_desc_t, is_live) + sizeof(bool),
               \"MOQ5_WRONG_FLOOR\");
int moq5_desc_copy_api_probe_floor;
" _kind _text)
if(_kind STREQUAL "ok")
    message(STATUS "PASS[api.floor]")
elseif(NOT _kind STREQUAL "error")
    message(FATAL_ERROR "FAIL[tool.probe]: ${_kind}:\n${_text}")
elseif(_text MATCHES "undeclared identifier 'MOQ_MEDIA_TRACK_DESC_V0_SIZE'" OR
       _text MATCHES "'MOQ_MEDIA_TRACK_DESC_V0_SIZE' undeclared")
    message(STATUS "FAIL[api.floor]: header does not define MOQ_MEDIA_TRACK_DESC_V0_SIZE")
    set(_fail 1)
elseif(_text MATCHES "MOQ5_WRONG_FLOOR")
    message(STATUS "FAIL[api.floor]: MOQ_MEDIA_TRACK_DESC_V0_SIZE is not the end of is_live")
    set(_fail 1)
else()
    message(FATAL_ERROR "FAIL[tool.probe]: unattributable compiler error:\n${_text}")
endif()

# -- api.exported (exact defined symbol, validated tool) ----------------------
execute_process(COMMAND "${NM}" -g "${LIB}"
                RESULT_VARIABLE _rc OUTPUT_VARIABLE _syms ERROR_VARIABLE _err)
if(NOT _rc EQUAL 0)
    message(FATAL_ERROR "FAIL[tool.nm]: ${NM} exited ${_rc}:\n${_err}")
endif()
if(NOT "${_err}" STREQUAL "")
    message(FATAL_ERROR "FAIL[tool.nm]: ${NM} wrote to stderr:\n${_err}")
endif()
if("${_syms}" STREQUAL "")
    message(FATAL_ERROR "FAIL[tool.nm]: ${NM} produced no symbols for ${LIB}")
endif()
# Every stdout line must be a symbol line ("<addr> <type> <name>", the address
# blank for undefined symbols), an archive member header ("<name>:"), or
# empty. Anything else is a diagnostic on stdout.
string(REPLACE ";" "\\;" _lines "${_syms}")
string(REPLACE "\n" ";" _lines "${_lines}")
foreach(_line IN LISTS _lines)
    string(REGEX REPLACE "\r$" "" _line "${_line}")
    if("${_line}" STREQUAL "" OR _line MATCHES "^[^ \t]+:$" OR
       _line MATCHES "^[0-9a-fA-F]* *[A-Za-z?-] [^ \t]+$")
        continue()
    endif()
    message(FATAL_ERROR "FAIL[tool.nm]: ${NM} wrote a non-symbol line to stdout:\n${_line}")
endforeach()
# One whole line: "<addr> T _?moq_media_receiver_track_desc_copy" then end of line.
if("${_syms}\n" MATCHES "[ \t]T _?moq_media_receiver_track_desc_copy\r?\n")
    message(STATUS "PASS[api.exported]")
else()
    message(STATUS "FAIL[api.exported]: ${LIB} does not define moq_media_receiver_track_desc_copy")
    set(_fail 1)
endif()

if(_fail)
    message(FATAL_ERROR "media_receiver_desc_copy_api: contract RED (prerequisite A not present)")
endif()
