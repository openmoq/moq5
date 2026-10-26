# Split one line of pkg-config output into its words, and print each on its own
# line as "MOQ-ARG <word>".
#
# pkg-config escapes a space inside a path with a backslash ("-I/a\ b/include"),
# so its output is shell-quoted text rather than a plain whitespace-separated
# list. separate_arguments(UNIX_COMMAND) parses exactly that quoting without
# evaluating anything, so no shell `eval` is needed and no Python dependency is
# assumed. The drivers already receive the exact CMake executable.
#
# Driven with -DMOQ_SPLIT_IN=<text>. The caller rejects input this parser would
# have to guess about (quote characters, a newline, a trailing backslash) before
# calling it.
if(NOT DEFINED MOQ_SPLIT_IN)
    message(FATAL_ERROR "moq_split_command: MOQ_SPLIT_IN is required")
endif()
separate_arguments(_moq_split_words UNIX_COMMAND "${MOQ_SPLIT_IN}")
foreach(_moq_split_word IN LISTS _moq_split_words)
    message("MOQ-ARG ${_moq_split_word}")
endforeach()
