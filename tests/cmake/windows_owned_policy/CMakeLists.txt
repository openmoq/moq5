cmake_minimum_required(VERSION 3.20)
project(moq_owned_warning_policy LANGUAGES C)
option(MOQ_WARNINGS_AS_ERRORS "Treat owned library warnings as errors" OFF)

# Host configure model: execute the actual target-local production branches,
# without compiling MSVC flags with a host compiler or copying the policy.
set(CMAKE_C_COMPILER_ID "${MODEL_COMPILER}")
set(MSVC FALSE)
if(MODEL_COMPILER STREQUAL "MSVC")
    set(MSVC TRUE)
endif()
file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/probe.c" "int moq_policy_probe(void) { return 0; }\n")
set(targets moq-core moq-service moq-adapter-picoquic moq-adapter-picoquic-threaded
    moq-adapter-pico-wt moq-adapter-pico-wt-managed moq-loc moq-msf moq-cmaf
    moq-codec-signaling moq-media-object)
set(paths core service adapters/picoquic adapters/picoquic adapters/pico_wt
    adapters/pico_wt media/loc media/msf media/cmaf media/codec_signaling media/object)
foreach(i RANGE 0 10)
    list(GET targets ${i} target)
    list(GET paths ${i} path)
    add_library(${target} STATIC "${CMAKE_CURRENT_BINARY_DIR}/probe.c")
    file(STRINGS "${MOQ_SOURCE}/${path}/CMakeLists.txt" lines)
    # Match a single warning branch, not a surrounding target or graph walk.
    set(found 0)
    set(in_branch FALSE)
    foreach(line IN LISTS lines)
        if(line MATCHES "^[ ]*if\\(CMAKE_C_COMPILER_ID MATCHES \"GNU\\|Clang\\|AppleClang\"\\)")
            set(in_branch TRUE)
            set(block "")
        endif()
        if(in_branch)
            string(APPEND block "${line}\n")
            if(line MATCHES "^[ ]*endif\\(\\)")
                if(block MATCHES "target_compile_options\\(${target} PRIVATE")
                    math(EXPR found "${found} + 1")
                    cmake_language(EVAL CODE "${block}")
                endif()
                set(in_branch FALSE)
            endif()
        endif()
    endforeach()
    if(NOT found EQUAL 1)
        message(FATAL_ERROR "Expected one production warning branch for ${target}, got ${found}")
    endif()
    get_target_property(interface ${target} INTERFACE_COMPILE_OPTIONS)
    if(interface)
        message(FATAL_ERROR "${target} exported private options: ${interface}")
    endif()
    file(GENERATE OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/${target}.options"
        CONTENT "$<TARGET_GENEX_EVAL:${target},$<JOIN:$<TARGET_PROPERTY:${target},COMPILE_OPTIONS>,;>>")
endforeach()
string(REPLACE ";" "\n" target_lines "${targets}")
file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/targets.txt" "${target_lines}\n")

# Read the real dependency-local warning policy in a child directory so a
# directory-wide /WX regression there is observable without affecting owners.
# The platform-specific picotls source injection is outside this policy model.
set(WIN32 FALSE)
add_subdirectory(dependency)
add_library(consumer STATIC "${CMAKE_CURRENT_BINARY_DIR}/probe.c")
target_link_libraries(consumer PRIVATE ${targets} dependency_probe)
file(GENERATE OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/consumer.options"
    CONTENT "$<TARGET_GENEX_EVAL:consumer,$<JOIN:$<TARGET_PROPERTY:consumer,COMPILE_OPTIONS>,;>>")
