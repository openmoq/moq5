# Preserve the parent's selected toolchain and dependency graph in nested builds.
# Bracket arguments retain spaces, semicolon lists, backslashes and dollar signs.
function(moq_consumer_cache_entry output name value)
    set(eq "")
    string(FIND "${value}]${eq}" "]${eq}]" end)
    while(NOT end EQUAL -1)
        string(APPEND eq "=")
        string(FIND "${value}]${eq}" "]${eq}]" end)
    endwhile()
    file(APPEND "${output}"
        "set(${name} [${eq}[${value}]${eq}] CACHE STRING \"Parent consumer context\" FORCE)\n")
endfunction()

function(moq_write_consumer_context output)
    file(WRITE "${output}" "# Generated from the parent build; do not edit.\n")
    set(vars CMAKE_C_COMPILER CMAKE_CXX_COMPILER CMAKE_TOOLCHAIN_FILE
        CMAKE_GENERATOR_PLATFORM CMAKE_GENERATOR_TOOLSET CMAKE_GENERATOR_INSTANCE
        CMAKE_SYSROOT CMAKE_SYSROOT_COMPILE CMAKE_SYSROOT_LINK
        CMAKE_OSX_SYSROOT CMAKE_OSX_ARCHITECTURES CMAKE_OSX_DEPLOYMENT_TARGET
        CMAKE_BUILD_TYPE CMAKE_CONFIGURATION_TYPES CMAKE_PREFIX_PATH
        CMAKE_FIND_ROOT_PATH CMAKE_FIND_ROOT_PATH_MODE_PACKAGE
        CMAKE_FIND_ROOT_PATH_MODE_LIBRARY CMAKE_FIND_ROOT_PATH_MODE_INCLUDE
        CMAKE_FIND_ROOT_PATH_MODE_PROGRAM CMAKE_CROSSCOMPILING_EMULATOR
        CMAKE_C_COMPILER_TARGET CMAKE_CXX_COMPILER_TARGET
        CMAKE_C_COMPILER_EXTERNAL_TOOLCHAIN CMAKE_CXX_COMPILER_EXTERNAL_TOOLCHAIN
        OPENSSL_INCLUDE_DIR OPENSSL_SSL_LIBRARY OPENSSL_CRYPTO_LIBRARY
        ZLIB_INCLUDE_DIR ZLIB_LIBRARY_RELEASE ZLIB_LIBRARY_DEBUG)
    get_cmake_property(cache_vars CACHE_VARIABLES)
    foreach(var IN LISTS cache_vars)
        if(var MATCHES "^CMAKE_(C|CXX|EXE_LINKER|SHARED_LINKER|MODULE_LINKER|STATIC_LINKER)_FLAGS($|_)"
           OR var MATCHES "^CMAKE_POLICY_DEFAULT_")
            list(APPEND vars "${var}")
        endif()
    endforeach()
    foreach(pkg proxygen wangle mvfst folly Fizz fmt liboqs Sodium Boost Glog glog gflags c-ares)
        list(APPEND vars "${pkg}_DIR")
    endforeach()
    foreach(var IN LISTS vars)
        if(DEFINED ${var})
            moq_consumer_cache_entry("${output}" "${var}" "${${var}}")
        endif()
    endforeach()
    if(MOQ_WARNINGS_AS_ERRORS)
        moq_consumer_cache_entry("${output}" CMAKE_COMPILE_WARNING_AS_ERROR ON)
    endif()
    # Match the parent project's use of Boost's package config, not FindBoost.
    if(POLICY CMP0167)
        cmake_policy(GET CMP0167 boost_policy)
        if(boost_policy)
            moq_consumer_cache_entry("${output}" CMAKE_POLICY_DEFAULT_CMP0167 "${boost_policy}")
        endif()
    endif()
endfunction()
