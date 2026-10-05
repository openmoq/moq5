cmake_minimum_required(VERSION 3.20)
project(wtquic_api_probe LANGUAGES ${PROBE_LANGUAGE})
include("${MOQ_SOURCE}/cmake/MoqWtquicApi.cmake")
if(FIXTURE_LIBRARY)
    add_library(wtq::wtquic UNKNOWN IMPORTED)
    set_target_properties(wtq::wtquic PROPERTIES
        IMPORTED_LOCATION "${FIXTURE_LIBRARY}"
        INTERFACE_INCLUDE_DIRECTORIES "${FIXTURE_INCLUDE}")
else()
    find_package(wtquic CONFIG REQUIRED)
endif()
# A caller's static-only check preference must not turn this into header-only
# discovery, and its check variables must not be overwritten by the helper.
set(CMAKE_TRY_COMPILE_TARGET_TYPE STATIC_LIBRARY)
set(CMAKE_REQUIRED_LIBRARIES sentinel)
if(DEFINED NESTED_DIAGNOSTIC_MARKER)
    # Arm only after compiler detection, so the API check is the producer.
    file(WRITE "${NESTED_DIAGNOSTIC_MARKER}" "${NESTED_DIAGNOSTIC}")
endif()
moq_check_wtquic_api(ok why)
if(DEFINED NESTED_DIAGNOSTIC_MARKER)
    file(REMOVE "${NESTED_DIAGNOSTIC_MARKER}")
endif()
if(NOT CMAKE_TRY_COMPILE_TARGET_TYPE STREQUAL "STATIC_LIBRARY" OR
   NOT CMAKE_REQUIRED_LIBRARIES STREQUAL "sentinel")
    message(FATAL_ERROR "probe modified caller check state")
endif()
if(NOT ok)
    message(FATAL_ERROR "${why}")
endif()
