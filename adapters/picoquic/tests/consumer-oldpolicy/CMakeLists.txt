cmake_minimum_required(VERSION 3.20)
project(moq_adapter_oldpolicy_consumer_test LANGUAGES C)

# CMP0156 is deliberately left UNSET. A 3.20-floor project that does not opt in
# -- and every project on CMake below 3.29, which does not know the policy --
# links with the OLD behaviour, which repeats static libraries caught in a
# dependency cycle. LibMoQ's adapter surfaces must still link without a
# warning. A future edit that sets the policy here would hide exactly that case.
if(POLICY CMP0156)
    cmake_policy(GET CMP0156 _moq_cmp0156)
    if(_moq_cmp0156)
        message(FATAL_ERROR "this consumer must leave CMP0156 unset (it is ${_moq_cmp0156})")
    endif()
endif()

# The components the parent built are REQUIRED here, so a surface that went
# missing fails the configure instead of silently dropping out of the gate.
set(_components adapter-picoquic)
if(OLDPOLICY_EXPECT_THREADED)
    list(APPEND _components adapter-picoquic-threaded)
endif()
if(OLDPOLICY_EXPECT_PICO_WT)
    list(APPEND _components adapter-pico-wt)
endif()
if(OLDPOLICY_EXPECT_PICO_WT_MANAGED)
    list(APPEND _components adapter-pico-wt-managed)
endif()
find_package(libmoq REQUIRED COMPONENTS ${_components})

set(_programs oldpolicy_adapter oldpolicy_log)
add_executable(oldpolicy_adapter adapter.c)
target_link_libraries(oldpolicy_adapter PRIVATE moq::adapter-picoquic)

# A direct picoquic-log consumer: log declares its dependency on core, so this
# alone links the pair once. (Direct upstream-target use that lists core first,
# or core alone, is upstream's target contract, not a LibMoQ surface.)
add_executable(oldpolicy_log log.c)
target_link_libraries(oldpolicy_log PRIVATE picoquic::picoquic-log)

if(OLDPOLICY_EXPECT_THREADED)
    add_executable(oldpolicy_threaded threaded.c)
    target_link_libraries(oldpolicy_threaded PRIVATE moq::adapter-picoquic-threaded)
    list(APPEND _programs oldpolicy_threaded)
endif()
if(OLDPOLICY_EXPECT_PICO_WT)
    add_executable(oldpolicy_pico_wt pico_wt.c)
    target_link_libraries(oldpolicy_pico_wt PRIVATE moq::adapter-pico-wt)
    list(APPEND _programs oldpolicy_pico_wt)
endif()
if(OLDPOLICY_EXPECT_PICO_WT_MANAGED)
    add_executable(oldpolicy_pico_wt_managed pico_wt_managed.c)
    target_link_libraries(oldpolicy_pico_wt_managed PRIVATE moq::adapter-pico-wt-managed)
    list(APPEND _programs oldpolicy_pico_wt_managed)
endif()

# The probes are owned code: strict C11, and every compiler warning an error,
# so a probe that is not valid ISO C cannot pass on a lenient default.
foreach(_program IN LISTS _programs)
    set_target_properties(${_program} PROPERTIES
        C_STANDARD 11 C_STANDARD_REQUIRED ON C_EXTENSIONS OFF)
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang")
        target_compile_options(${_program} PRIVATE -Wall -Wextra -Wpedantic -Werror)
    endif()
endforeach()

# The driver runs exactly the programs this configure built.
string(REPLACE ";" "\n" _programs_lines "${_programs}")
file(WRITE "${CMAKE_BINARY_DIR}/oldpolicy-programs.txt" "${_programs_lines}\n")
