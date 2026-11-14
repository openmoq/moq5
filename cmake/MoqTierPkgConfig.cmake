# One pkg-config file per dependency-free tier.
#
# libmoq.pc names every enabled component, adapters included. A tier file
# names one tier and requires only the tiers below it, so a consumer of the
# core, the simulator or the media parsers links nothing else. Each owning
# component calls moq_install_tier_pc() beside its own install rule; a tier
# that is not built installs no file.
#
# This file is also included by the install script and by tests in script
# mode, so everything outside moq_install_tier_pc() must work without a
# project.
set(MOQ_TIER_PC_TEMPLATE "${CMAKE_CURRENT_LIST_DIR}/libmoq-tier.pc.in")

# Stands for the install prefix in a generated file until it is installed.
# A plain word: this file is also read by the install script, whose policy
# settings must not matter to it.
set(MOQ_TIER_PC_PREFIX_TOKEN "__MOQ_TIER_PC_INSTALL_PREFIX__")

# The prefix, libdir and includedir values of a tier file for one install
# layout. CMAKE_INSTALL_LIBDIR and CMAKE_INSTALL_INCLUDEDIR may each be
# relative to the install prefix or absolute.
#
#   - Relative library directory: the file lives in <prefix>/<libdir>/pkgconfig,
#     so the prefix is found from the file's own location and the package
#     moves with its directory.
#   - Absolute library directory: the libraries are at that fixed place, and
#     the prefix cannot be found from the file. It is the install prefix,
#     which is known only when the file is installed: out_prefix is then
#     MOQ_TIER_PC_PREFIX_TOKEN and out_needs_prefix is TRUE.
#   - Relative include directory: under the prefix, whichever way it is found.
#   - Absolute include directory: named as it is, never joined to the prefix.
function(moq_tier_pc_paths libdir includedir
         out_prefix out_libdir out_includedir out_needs_prefix)
    if(IS_ABSOLUTE "${libdir}")
        set(prefix "${MOQ_TIER_PC_PREFIX_TOKEN}")
        set(lib "${libdir}")
        set(needs_prefix TRUE)
    else()
        # From <prefix>/<libdir>/pkgconfig back to <prefix>: one level per
        # component of libdir (lib, lib64, lib/x86_64-linux-gnu) and one more.
        file(RELATIVE_PATH up "/${libdir}/pkgconfig" "/")
        string(REGEX REPLACE "/+$" "" up "${up}")
        set(prefix "\${pcfiledir}/${up}")
        set(lib "\${prefix}/${libdir}")
        set(needs_prefix FALSE)
    endif()
    if(IS_ABSOLUTE "${includedir}")
        set(inc "${includedir}")
    else()
        set(inc "\${prefix}/${includedir}")
    endif()
    set(${out_prefix} "${prefix}" PARENT_SCOPE)
    set(${out_libdir} "${lib}" PARENT_SCOPE)
    set(${out_includedir} "${inc}" PARENT_SCOPE)
    set(${out_needs_prefix} "${needs_prefix}" PARENT_SCOPE)
endfunction()

# Write one tier file to `output` for the layout in CMAKE_INSTALL_LIBDIR and
# CMAKE_INSTALL_INCLUDEDIR. out_needs_prefix says whether the file still
# holds MOQ_TIER_PC_PREFIX_TOKEN for moq_tier_pc_resolve_prefix().
function(moq_configure_tier_pc name description requires libs output
         out_needs_prefix)
    moq_tier_pc_paths("${CMAKE_INSTALL_LIBDIR}" "${CMAKE_INSTALL_INCLUDEDIR}"
        MOQ_TIER_PC_PREFIX MOQ_TIER_PC_LIBDIR MOQ_TIER_PC_INCLUDEDIR
        needs_prefix)
    set(MOQ_TIER_PC_NAME "${name}")
    set(MOQ_TIER_PC_DESCRIPTION "${description}")
    set(MOQ_TIER_PC_REQUIRES "${requires}")
    set(MOQ_TIER_PC_LIBS "${libs}")
    set(MOQ_TIER_PC_DEFINES "")
    if(NOT BUILD_SHARED_LIBS AND WIN32)
        set(MOQ_TIER_PC_DEFINES " -DMOQ_STATIC")
    endif()
    configure_file("${MOQ_TIER_PC_TEMPLATE}" "${output}" @ONLY)
    set(${out_needs_prefix} "${needs_prefix}" PARENT_SCOPE)
endfunction()

# Replace MOQ_TIER_PC_PREFIX_TOKEN in an installed tier file with the prefix
# it was installed for. Runs from the install script, where the prefix is the
# one in effect for that install (`cmake --install --prefix` included).
function(moq_tier_pc_resolve_prefix file prefix)
    file(READ "${file}" text)
    string(REPLACE "${MOQ_TIER_PC_PREFIX_TOKEN}" "${prefix}" text "${text}")
    file(WRITE "${file}" "${text}")
endfunction()

function(moq_install_tier_pc name description requires libs)
    set(pc "${CMAKE_CURRENT_BINARY_DIR}/${name}.pc")
    moq_configure_tier_pc("${name}" "${description}" "${requires}" "${libs}"
        "${pc}" needs_prefix)
    install(FILES "${pc}"
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/pkgconfig)
    if(needs_prefix)
        # An absolute library directory: the installed file is at that fixed
        # place, under DESTDIR when the install is staged.
        install(CODE "
            include(\"${CMAKE_CURRENT_FUNCTION_LIST_FILE}\")
            moq_tier_pc_resolve_prefix(
                \"\$ENV{DESTDIR}${CMAKE_INSTALL_LIBDIR}/pkgconfig/${name}.pc\"
                \"\${CMAKE_INSTALL_PREFIX}\")
        ")
    endif()
endfunction()
