# Consumer runs must not inherit a loader workaround from the invoking shell.
function(msquic_consumer_environment out)
    set(${out} ${CMAKE_COMMAND} -E env
        --unset=LD_LIBRARY_PATH --unset=LD_PRELOAD --unset=LD_AUDIT
        --unset=LD_DEBUG --unset=LD_DEBUG_OUTPUT
        --unset=DYLD_LIBRARY_PATH --unset=DYLD_INSERT_LIBRARIES
        --unset=DYLD_FALLBACK_LIBRARY_PATH --unset=DYLD_PRINT_LIBRARIES
        PARENT_SCOPE)
endfunction()
