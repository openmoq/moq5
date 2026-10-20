# Caller project hook (file): records its turn and leaves a variable in the
# scope it runs in.
get_property(_moq_options DIRECTORY PROPERTY COMPILE_OPTIONS)
set_property(GLOBAL APPEND PROPERTY MOQ_TEST_HOOK_ORDER "A[${_moq_options}]")
set(MOQ_TEST_CALLER_SCOPE "set-by-hook-a")
