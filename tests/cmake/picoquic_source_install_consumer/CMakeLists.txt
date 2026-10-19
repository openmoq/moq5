# Downstream project resolving libmoq from an installed prefix while building
# picoquic from source (MOQ_PICOQUIC_SOURCE_DIR). Configure-only: success
# means the installed FindPicoquic added picoquic and its project() hook ran.
cmake_minimum_required(VERSION 3.20)
project(moq_picoquic_source_install_consumer LANGUAGES C)

if(NOT DEFINED MOQ_CONSUMER_COMPONENT)
    message(FATAL_ERROR "MOQ_CONSUMER_COMPONENT is required")
endif()

# A caller hook already in CMAKE_PROJECT_picoquic_INCLUDE is handed over to
# the installed policy module, which runs it inside picoquic's project()
# after setting the link de-duplication policies. The probe records what it
# sees there, so the checks below observe the installed module itself.
set(CMAKE_PROJECT_picoquic_INCLUDE "${CMAKE_CURRENT_SOURCE_DIR}/project_probe.cmake")

find_package(libmoq REQUIRED COMPONENTS ${MOQ_CONSUMER_COMPONENT})

string(FIND "${libmoq_DIR}" "${MOQ_CONSUMER_PREFIX}/" prefix_at)
if(NOT prefix_at EQUAL 0)
    message(FATAL_ERROR "libmoq resolved from ${libmoq_DIR}, not the installed prefix ${MOQ_CONSUMER_PREFIX}")
endif()
if(NOT TARGET picoquic-core)
    message(FATAL_ERROR "source-mode FindPicoquic did not add picoquic-core")
endif()
get_property(probe_ran GLOBAL PROPERTY MOQ_PICOQUIC_PROJECT_PROBE SET)
if(NOT probe_ran)
    message(FATAL_ERROR "picoquic's project() did not run the caller hook through the installed policy module")
endif()
foreach(policy CMP0156 CMP0179)
    if(POLICY ${policy})
        get_property(setting GLOBAL PROPERTY MOQ_PICOQUIC_PROJECT_${policy})
        if(NOT setting STREQUAL "NEW")
            message(FATAL_ERROR "${policy} inside picoquic's project() is '${setting}', expected NEW")
        endif()
    endif()
endforeach()
if(NOT CMAKE_PROJECT_picoquic_INCLUDE STREQUAL "${CMAKE_CURRENT_SOURCE_DIR}/project_probe.cmake")
    message(FATAL_ERROR "CMAKE_PROJECT_picoquic_INCLUDE was not restored after find_package")
endif()

# picotls comes from MOQ_PICOTLS_PREFIX only: a picotls build directory
# (headers in ../include) or an installed prefix (include/, lib/).
get_filename_component(ptls_tree "${MOQ_PICOTLS_PREFIX}/.." ABSOLUTE)
get_filename_component(ptls_include "${PTLS_INCLUDE_DIR}" ABSOLUTE)
if(NOT ptls_include STREQUAL "${ptls_tree}/include" AND
   NOT ptls_include STREQUAL "${MOQ_PICOTLS_PREFIX}/include")
    message(FATAL_ERROR "PTLS_INCLUDE_DIR is ${PTLS_INCLUDE_DIR}, not the headers beside MOQ_PICOTLS_PREFIX=${MOQ_PICOTLS_PREFIX}")
endif()
foreach(lib PTLS_CORE_LIBRARY PTLS_MINICRYPTO_LIBRARY)
    string(FIND "${${lib}}" "${MOQ_PICOTLS_PREFIX}/" at)
    if(NOT at EQUAL 0)
        message(FATAL_ERROR "${lib} is ${${lib}}, not under MOQ_PICOTLS_PREFIX=${MOQ_PICOTLS_PREFIX}")
    endif()
endforeach()

# One OpenSSL link identity. When the installed adapter recorded that it
# links OpenSSL, the source provider's picoquic-core and picohttp-core name
# OpenSSL only through the OpenSSL::SSL / OpenSSL::Crypto imported targets
# the adapter uses, never as their raw archive paths: two spellings of one
# archive cannot be de-duplicated on a consumer's link line.
if(_LIBMOQ_ADAPTER_PICOQUIC_NEEDS_OPENSSL AND NOT (DEFINED WITH_OPENSSL AND NOT WITH_OPENSSL))
    set(raw "")
    foreach(imp IN ITEMS OpenSSL::SSL OpenSSL::Crypto)
        if(NOT TARGET ${imp})
            message(FATAL_ERROR "${imp} is not a target after find_package(libmoq)")
        endif()
        get_target_property(loc ${imp} IMPORTED_LOCATION)
        if(loc)
            list(APPEND raw "${loc}")
        endif()
    endforeach()
    set(edges picoquic-core)
    if(TARGET picohttp-core)
        list(APPEND edges picohttp-core)
    endif()
    foreach(tgt IN LISTS edges)
        set(named "")
        foreach(prop IN ITEMS LINK_LIBRARIES INTERFACE_LINK_LIBRARIES)
            get_target_property(items ${tgt} ${prop})
            if(NOT items)
                continue()
            endif()
            foreach(item IN LISTS items)
                if(item MATCHES "^\\$<LINK_ONLY:([^<>]+)>$")
                    set(item "${CMAKE_MATCH_1}")
                endif()
                if(item IN_LIST raw)
                    message(FATAL_ERROR "${tgt} ${prop} links OpenSSL by its raw path ${item}")
                endif()
                if(item STREQUAL "OpenSSL::SSL" OR item STREQUAL "OpenSSL::Crypto")
                    list(APPEND named "${item}")
                endif()
            endforeach()
        endforeach()
        foreach(imp IN ITEMS OpenSSL::SSL OpenSSL::Crypto)
            if(NOT imp IN_LIST named)
                message(FATAL_ERROR "${tgt} does not link ${imp}")
            endif()
        endforeach()
    endforeach()
    message(STATUS "OpenSSL link identity: ${edges} name OpenSSL by its imported targets")
endif()
