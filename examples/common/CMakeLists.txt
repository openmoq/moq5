# Shared, header-only support for LibMoQ example programs (terminal-safe output,
# example arg parsing). Not installed and not part of any public API.
#
# The headers themselves need no build target; this file only registers their
# unit tests. They are pure C with no libmoq dependency, so they build and run
# even in configurations without a transport/service tier.

if(MOQ_BUILD_TESTS)
    add_executable(test_moq_example_term tests/test_moq_example_term.c)
    add_test(NAME example_term_safe COMMAND test_moq_example_term)

    add_executable(test_moq_example_args tests/test_moq_example_args.c)
    add_test(NAME example_args_parse COMMAND test_moq_example_args)
endif()
