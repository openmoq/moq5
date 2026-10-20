# Caller project hook (module name, found through CMAKE_MODULE_PATH).
get_property(_moq_options DIRECTORY PROPERTY COMPILE_OPTIONS)
set_property(GLOBAL APPEND PROPERTY MOQ_TEST_HOOK_ORDER "B[${_moq_options}]")
