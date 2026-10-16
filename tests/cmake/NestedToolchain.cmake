# C++ toolchain forwarding for nested consumer builds that configure picoquic.
#
# picoquic chooses its compile options with generator expressions on BOTH the
# C and the C++ compiler IDs (for example -frename-registers under
# $<CXX_COMPILER_ID:GNU>), and those options are applied to its C targets as
# well. A nested tree that is handed only this tree's C compiler detects its
# own default C++ compiler, so a Clang C build next to a default g++ receives
# GCC-only options and every C compile warns. Forwarding the C++ compiler keeps
# the nested toolchain identical to this one.
#
# moq_nested_cxx_args(<out> [FLAGS])
#   Sets <out> to " -DCMAKE_CXX_COMPILER='<cxx>'" (plus
#   " -DCMAKE_CXX_FLAGS='<flags>'" with FLAGS) for a `bash -c` command line,
#   or to "" when this tree has no C++ compiler.
#
# moq_nested_cxx_env(<out>)
#   Sets <out> to "CXX=<cxx>" for `cmake -E env`, which a nested script's
#   first configure honours, or to "" when this tree has no C++ compiler.

function(moq_nested_cxx_args out)
    cmake_parse_arguments(PARSE_ARGV 1 _moq_nested "FLAGS" "" "")
    set(_args "")
    if(CMAKE_CXX_COMPILER)
        string(APPEND _args " -DCMAKE_CXX_COMPILER='${CMAKE_CXX_COMPILER}'")
        if(_moq_nested_FLAGS)
            string(APPEND _args " -DCMAKE_CXX_FLAGS='${CMAKE_CXX_FLAGS}'")
        endif()
    endif()
    set(${out} "${_args}" PARENT_SCOPE)
endfunction()

function(moq_nested_cxx_env out)
    set(_env "")
    if(CMAKE_CXX_COMPILER)
        set(_env "CXX=${CMAKE_CXX_COMPILER}")
    endif()
    set(${out} "${_env}" PARENT_SCOPE)
endfunction()
