# moq-adapter-wasm-wt: the browser WebTransport facade for libmoq.
#
# Optional and experimental. Requires an Emscripten toolchain
# (MOQ_BUILD_WASM_WT_MANAGED=ON). The facade (wasm_wt_managed.c) drives the
# shared transport bridge; the browser side (wasm_wt_browser.js) is an
# Emscripten JS library that every executable linking this target pulls in
# through the INTERFACE link option below.

add_library(moq-adapter-wasm-wt STATIC wasm_wt_managed.c)
add_library(moq::adapter-wasm-wt-managed ALIAS moq-adapter-wasm-wt)

target_include_directories(moq-adapter-wasm-wt
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
)
target_link_libraries(moq-adapter-wasm-wt PUBLIC moq::core)
target_compile_definitions(moq-adapter-wasm-wt PRIVATE MOQ_BUILDING)
target_compile_features(moq-adapter-wasm-wt PUBLIC c_std_11)

# The JS library supplies the moq_wasm_wt_js_* functions the facade calls. The
# exported entry points the library calls back (moq_wasm_wt_on_*) are marked
# EMSCRIPTEN_KEEPALIVE in the facade, so they survive dead-code elimination.
target_link_options(moq-adapter-wasm-wt INTERFACE
    "SHELL:--js-library ${CMAKE_CURRENT_SOURCE_DIR}/wasm_wt_browser.js")
set_property(TARGET moq-adapter-wasm-wt APPEND PROPERTY INTERFACE_LINK_DEPENDS
    ${CMAKE_CURRENT_SOURCE_DIR}/wasm_wt_browser.js)

set_target_properties(moq-adapter-wasm-wt PROPERTIES
    C_VISIBILITY_PRESET hidden
    OUTPUT_NAME moq-adapter-wasm-wt
)

if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
    target_compile_options(moq-adapter-wasm-wt PRIVATE
        -Wall -Wextra -Wpedantic $<$<BOOL:${MOQ_WARNINGS_AS_ERRORS}>:-Werror>)
endif()
