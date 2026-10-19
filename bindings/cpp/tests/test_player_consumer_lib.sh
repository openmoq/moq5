#!/usr/bin/env bash
#
# Controls for the installed C++ player consumer's path and configuration
# rules (player_consumer_lib.sh). Each case fails on the construction the
# runner used before:
#   - an installed picotls prefix /x/picotls-sdk/{include,lib}: the tree is
#     /x/picotls-sdk, not dirname of the prefix (/x);
#   - a checkout build directory /x/picotls/build with headers in
#     ../include: the tree is /x/picotls;
#   - a multi-config generator: no CMAKE_BUILD_TYPE, every step --config,
#     the executable under <config>/;
#   - a single-config generator: CMAKE_BUILD_TYPE set;
#   - prefix verification: a path in another SDK is refused, a path in the
#     prefix (also through a symlinked prefix) is accepted.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=player_consumer_lib.sh
. "$here/player_consumer_lib.sh"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
fails=0
expect() {   # name got want
    if [ "$2" = "$3" ]; then echo "PASS: $1"
    else echo "FAIL: $1: got '$2', want '$3'"; fails=$((fails + 1)); fi
}
real() { (cd "$1" && pwd -P); }

# picotls layouts
mkdir -p "$work/picotls-sdk/include/picotls" "$work/picotls-sdk/lib" \
         "$work/picotls/include/picotls" "$work/picotls/build"
expect installed_prefix_tree "$(pc_ptls_tree "$work/picotls-sdk/include")" \
       "$(real "$work/picotls-sdk")"
expect build_dir_tree "$(pc_ptls_tree "$work/picotls/include")" "$(real "$work/picotls")"
# the old construction, dirname(MOQ_PICOTLS_PREFIX), names the wrong tree
# for an installed prefix:
expect old_rule_differs "$([ "$(dirname "$work/picotls-sdk")" != "$(real "$work/picotls-sdk")" ] && echo yes)" yes

# configuration selection
expect multi_config_no_build_type "$(pc_configure_config_args Release 1)" ""
expect single_config_build_type "$(pc_configure_config_args Release 0)" "-DCMAKE_BUILD_TYPE=Release"
expect step_config "$(pc_step_config_args Debug | tr '\n' ' ')" "--config Debug "
mkdir -p "$work/bld/Debug" "$work/one"
printf '#!/bin/sh\n' > "$work/bld/Debug/app"; chmod +x "$work/bld/Debug/app"
printf '#!/bin/sh\n' > "$work/one/app"; chmod +x "$work/one/app"
expect multi_config_exe "$(pc_find_exe "$work/bld" app Debug)" "$work/bld/Debug/app"
expect single_config_exe "$(pc_find_exe "$work/one" app Release)" "$work/one/app"
expect absent_exe "$(pc_find_exe "$work/bld" app Release)" ""

# prefix verification
mkdir -p "$work/prefix/lib" "$work/foreign/lib"
touch "$work/prefix/lib/libmoq-core.a" "$work/foreign/lib/libmoq-core.a"
ln -s "$work/prefix" "$work/linked-prefix"
expect inside "$(pc_under_prefix "$work/prefix/lib/libmoq-core.a" "$work/prefix" && echo yes || echo no)" yes
expect foreign "$(pc_under_prefix "$work/foreign/lib/libmoq-core.a" "$work/prefix" && echo yes || echo no)" no
expect via_symlink "$(pc_under_prefix "$work/linked-prefix/lib/libmoq-core.a" "$work/prefix" && echo yes || echo no)" yes
expect missing "$(pc_under_prefix "$work/prefix/lib/nope.a" "$work/prefix" && echo yes || echo no)" no
# library-FILE symlinks: the final component and any chain are resolved
ln -s "$work/foreign/lib/libmoq-core.a" "$work/prefix/lib/escaping.a"
expect escaping_file_symlink \
    "$(pc_under_prefix "$work/prefix/lib/escaping.a" "$work/prefix" && echo yes || echo no)" no
ln -s libmoq-core.a "$work/prefix/lib/relative.a"
expect relative_in_prefix_symlink \
    "$(pc_under_prefix "$work/prefix/lib/relative.a" "$work/prefix" && echo yes || echo no)" yes
ln -s escaping.a "$work/prefix/lib/chain.a"
expect escaping_chain \
    "$(pc_under_prefix "$work/prefix/lib/chain.a" "$work/prefix" && echo yes || echo no)" no
ln -s "$work/prefix/lib/gone.a" "$work/prefix/lib/dangling.a"
expect dangling_link \
    "$(pc_under_prefix "$work/prefix/lib/dangling.a" "$work/prefix" && echo yes || echo no)" no

# the child provider's picotls selection, checked against the parent's
printf '%s\n' \
    'set(PTLS_INCLUDE_DIR [==[/sdk/include]==] CACHE PATH "Provider")' \
    'set(PTLS_CORE_LIBRARY [==[/sdk/lib/libpicotls-core.a]==] CACHE FILEPATH "Provider")' \
    > "$work/picotls-context.cmake"
expect preload_value "$(pc_setting "$work/picotls-context.cmake" PTLS_INCLUDE_DIR)" /sdk/include
expect preload_absent "$(pc_setting "$work/picotls-context.cmake" PTLS_FUSION_LIBRARY)" ""
# The runner's form: an assignment under set -e -o pipefail, for an entry
# neither the preload nor a cache has. Exiting here fails the script.
absent=$(pc_setting "$work/picotls-context.cmake" PTLS_FUSION_LIBRARY)
expect absent_assignment_survives "$absent" ""
printf '%s\n' 'PTLS_INCLUDE_DIR:PATH=/sdk/include' \
    'PTLS_CORE_LIBRARY:FILEPATH=/sdk/lib/libpicotls-core.a' \
    'PTLS_FUSION_LIBRARY:FILEPATH=PTLS_FUSION_LIBRARY-NOTFOUND' > "$work/child-ok.txt"
# a child that fell back to the sibling checkout's headers (the selection
# was not passed to it):
printf '%s\n' 'PTLS_INCLUDE_DIR:PATH=/checkout/picotls/include' \
    'PTLS_CORE_LIBRARY:FILEPATH=/sdk/lib/libpicotls-core.a' > "$work/child-fallback.txt"
expect child_selected "$(pc_selected "$work/child-ok.txt" PTLS_INCLUDE_DIR /sdk/include && echo yes || echo no)" yes
expect child_fell_back "$(pc_selected "$work/child-fallback.txt" PTLS_INCLUDE_DIR /sdk/include && echo yes || echo no)" no
expect child_no_fusion "$(pc_selected "$work/child-ok.txt" PTLS_FUSION_LIBRARY "" && echo yes || echo no)" yes
printf '%s\n' 'PTLS_FUSION_LIBRARY:FILEPATH=/checkout/picotls/build/libpicotls-fusion.a' > "$work/child-fusion.txt"
expect child_found_foreign_fusion "$(pc_selected "$work/child-fusion.txt" PTLS_FUSION_LIBRARY "" && echo yes || echo no)" no

if [ "$fails" -eq 0 ]; then echo "PASS: player consumer lib"; else echo "FAIL: $fails"; exit 1; fi
