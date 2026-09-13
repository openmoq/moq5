#!/usr/bin/env bash
# Offline setup-contract test using a real local Git repository and CMake install.
# It does not claim to test WTQuic or MsQuic runtime behavior.
set -euo pipefail
case "${1:-}" in
    --self-test) ;;
    *) echo 'Usage: bash scripts/selftest_dependency_setup.sh --self-test' >&2; exit 2 ;;
esac
script_dir=$(cd "$(dirname "$0")" && pwd)
work=$(mktemp -d "${TMPDIR:-/tmp}/moq-setup-test.XXXXXXXX")
trap 'rm -rf "$work"' EXIT
repo="$work/local source"
deps="$work/private deps"
prior="$work/prior one;$work/prior two"
mkdir -p "$repo"
cat > "$repo/CMakeLists.txt" <<'CMAKE'
cmake_minimum_required(VERSION 3.20)
project(setup_fixture NONE)
foreach(key WTQ_REQUIRE_MSQUIC WTQ_WARNINGS_AS_ERRORS WTQ_BUILD_NETWORK
            WTQ_BUILD_TESTS WTQ_BUILD_SIM WTQ_BUILD_BENCHMARKS WTQ_BUILD_EXAMPLES)
    message(STATUS "${key}=${${key}}")
endforeach()
if(NOT "${PASSTHROUGH}" STREQUAL "two words")
    message(FATAL_ERROR "extra argument was not preserved")
endif()
if(FAIL_CONFIGURE)
    message(FATAL_ERROR "deliberate configure refusal")
endif()
file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/wtquicConfig.cmake" "# fixture package\n")
install(FILES "${CMAKE_CURRENT_BINARY_DIR}/wtquicConfig.cmake"
        DESTINATION lib64/cmake/wtquic)
CMAKE
git init -q "$repo"
git -C "$repo" add CMakeLists.txt
git -C "$repo" -c user.name=Fixture -c user.email=fixture@example.invalid \
    commit -qm fixture
ref=$(git -C "$repo" rev-parse HEAD)
invoke() {
    WTQUIC_REPO="$repo" WTQUIC_REF="$ref" WTQUIC_DEPS_DIR="$deps" \
    WTQ_BUILD_NETWORK=OFF WTQ_FETCH_ONLY=0 CMAKE_PREFIX_PATH="$prior" \
    GITHUB_ENV="$work/github-env" \
        "$BASH" "$script_dir/setup_wtquic_deps.sh" "$@"
}
invoke '-DPASSTHROUGH=two words' > "$work/env" 2> "$work/log" || {
    cat "$work/log" >&2; exit 1
}
# A source-able result is part of the public setup contract, not just prose.
. "$work/env"
[ "$wtquic_DIR" = "$deps/prefix/lib64/cmake/wtquic" ]
[ "$CMAKE_PREFIX_PATH" = "$deps/prefix;$prior" ]
grep -qxF "CMAKE_PREFIX_PATH=$deps/prefix:${prior//;/:}" "$work/github-env"
[ -f "$wtquic_DIR/wtquicConfig.cmake" ]
[ -z "$(git -C "$deps/wtquic" status --porcelain)" ]
echo 'PASS extra argument, spaced paths, lib64, clean cached source'
if invoke '-DPASSTHROUGH=two words' -DFAIL_CONFIGURE=ON > "$work/refuse.out" 2> "$work/refuse.err"; then
    echo 'FAIL configure refusal returned success' >&2; exit 1
fi
[ ! -s "$work/refuse.out" ]
grep -q 'deliberate configure refusal' "$work/refuse.err"
echo 'PASS configure failure does not emit success assignments'
printf '\n# caller edit\n' >> "$deps/wtquic/CMakeLists.txt"
before=$(git -C "$deps/wtquic" diff)
if invoke '-DPASSTHROUGH=two words' > "$work/dirty.out" 2> "$work/dirty.err"; then
    echo 'FAIL dirty source accepted' >&2; exit 1
fi
grep -q 'checkout is dirty' "$work/dirty.err"
[ "$before" = "$(git -C "$deps/wtquic" diff)" ]
[ "$(git -C "$deps/wtquic" rev-parse HEAD)" = "$ref" ]
echo 'PASS dirty checkout refused and preserved'
"$BASH" "$script_dir/setup_msquic_deps.sh" --help > "$work/msquic-help"
grep -q setup_msquic_deps "$work/msquic-help"
echo 'PASS MsQuic recipe help'
cmake -S "$script_dir/.." -B "$work/relay-disabled" \
    -DMOQ_BUILD_RELAY=ON -DMOQ_BUILD_TESTS=OFF -DMOQ_BUILD_EXAMPLES=OFF \
    -DMOQ_BUILD_ADAPTER_MSQUIC=OFF -DMOQ_BUILD_MSQUIC_MANAGED=OFF \
    > "$work/relay-disabled.log" 2>&1
grep -qF 'moq5-relay executable disabled: enable MOQ_BUILD_ADAPTER_MSQUIC=ON' \
    "$work/relay-disabled.log"
echo 'PASS transport-free configure explains the absent command'
