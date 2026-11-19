# A caller's picotls project hook (MOQ_PICOTLS_CALLER_PROJECT_INCLUDE) that
# defines a target of its own; it runs before LibMoQ's policy.
file(WRITE "${CMAKE_BINARY_DIR}/caller_helper.c" "int moq_caller_helper(void) { return 0; }\n")
add_library(caller_helper STATIC "${CMAKE_BINARY_DIR}/caller_helper.c")
