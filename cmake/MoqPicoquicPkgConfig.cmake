# Build the pkg-config link closure for the configured PicoQUIC provider.
# PicoQUIC does not ship pkg-config metadata, so consumers need its static
# archive cycle, picotls, and the selected TLS provider spelled out here.
function(moq_picoquic_pkgconfig_libs out_var)
    set(_libs "")

    if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
        string(APPEND _libs
            " -Wl,--start-group -lpicoquic-core -lpicoquic-log -Wl,--end-group")
    else()
        string(APPEND _libs " -lpicoquic-core -lpicoquic-log")
    endif()

    if(MOQ_PTLS_LIB_DIR)
        string(APPEND _libs " -L${MOQ_PTLS_LIB_DIR}")
    endif()

    set(_has_openssl OFF)
    if((DEFINED WITH_OPENSSL AND WITH_OPENSSL) OR
       MOQ_ADAPTER_PICOQUIC_NEEDS_OPENSSL)
        set(_has_openssl ON)
    endif()
    if(_has_openssl)
        string(APPEND _libs " -lpicotls-openssl")
    endif()
    string(APPEND _libs " -lpicotls-core -lpicotls-minicrypto")

    if(DEFINED WITH_MBEDTLS AND WITH_MBEDTLS)
        set(_mbed_dirs "")
        foreach(_mbed_var MBEDTLS_LIBRARY MBEDTLS_X509 MBEDTLS_CRYPTO)
            if(DEFINED ${_mbed_var} AND NOT "${${_mbed_var}}" STREQUAL "")
                get_filename_component(_mbed_dir "${${_mbed_var}}" DIRECTORY)
                list(APPEND _mbed_dirs "${_mbed_dir}")
            endif()
        endforeach()
        list(REMOVE_DUPLICATES _mbed_dirs)
        foreach(_mbed_dir IN LISTS _mbed_dirs)
            string(APPEND _libs " -L${_mbed_dir}")
        endforeach()
        string(APPEND _libs " -lmbedtls -lmbedx509 -lmbedcrypto")
    endif()

    if(_has_openssl)
        find_package(OpenSSL QUIET)
        if(OPENSSL_FOUND AND OPENSSL_CRYPTO_LIBRARY)
            get_filename_component(_openssl_dir "${OPENSSL_CRYPTO_LIBRARY}" DIRECTORY)
            string(APPEND _libs " -L${_openssl_dir}")
        endif()
        string(APPEND _libs " -lssl -lcrypto")
    endif()

    string(APPEND _libs " -lpthread")
    string(STRIP "${_libs}" _libs)
    set(${out_var} "${_libs}" PARENT_SCOPE)
endfunction()
