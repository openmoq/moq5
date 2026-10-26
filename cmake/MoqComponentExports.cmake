# Private registration of one existing public component. CMake owns artifact,
# configuration, language, SONAME/install-name and DLL/import-library metadata.
# Foreign targets stay in their own packages and export sets.
include_guard(GLOBAL)

function(moq_export_component target export_name module)
    if(NOT ARGC EQUAL 3 OR NOT TARGET "${target}" OR
       "${export_name}" STREQUAL "" OR "${module}" STREQUAL "")
        message(FATAL_ERROR "Invalid private component export registration")
    endif()
    set_property(TARGET "${target}" PROPERTY EXPORT_NAME "${export_name}")
    install(TARGETS "${target}" EXPORT "${module}"
        ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
        LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
        RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}")
    if(MOQ_BUILD_PACKAGE_EXPORTS)
        export(EXPORT "${module}" NAMESPACE moq::
            FILE "${PROJECT_BINARY_DIR}/${module}.cmake")
        install(EXPORT "${module}" NAMESPACE moq:: FILE "${module}.cmake"
            DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/libmoq")
    endif()
endfunction()
