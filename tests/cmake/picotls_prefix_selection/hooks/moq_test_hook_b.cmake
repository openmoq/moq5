# Caller project hook (module name, found through CMAKE_MODULE_PATH): records
# its turn, with the picotls options LibMoQ's policy has declared by then.
get_property(_moq_options DIRECTORY PROPERTY MOQ_DIRECTORY_COMPILE_OPTIONS)
set_property(GLOBAL APPEND PROPERTY MOQ_TEST_HOOK_ORDER "B[${_moq_options}]")
