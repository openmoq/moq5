# Browser example (Emscripten only): the service-tier media receiver in a page.
# Built when MOQ_BUILD_WASM_WT_MANAGED=ON and MOQ_BUILD_EXAMPLES=ON.

add_executable(moq_example_browser_receive browser_receive.c)
target_link_libraries(moq_example_browser_receive PRIVATE moq::service)
set_target_properties(moq_example_browser_receive PROPERTIES SUFFIX ".js")
# The single-file module is created synchronously and exports a factory,
# MoqReceive({log}); the page drives it from timers (index.html).
target_link_options(moq_example_browser_receive PRIVATE
    -sMODULARIZE=1 -sEXPORT_NAME=MoqReceive -sSINGLE_FILE=1 -sWASM_ASYNC_COMPILATION=0
    -sALLOW_MEMORY_GROWTH=1 -sENVIRONMENT=web
    "-sEXPORTED_RUNTIME_METHODS=ccall,cwrap,UTF8ToString")
add_custom_command(TARGET moq_example_browser_receive POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/index.html
            $<TARGET_FILE_DIR:moq_example_browser_receive>/index.html)
