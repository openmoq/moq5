# Dependency-local policy for scripts/setup_picoquic_deps.sh. Directory compile
# options follow CMAKE_C_FLAGS and configuration flags, preserving caller flags
# while keeping picotls invariants active even when Release defines NDEBUG.
add_compile_options(-Werror -UNDEBUG)
