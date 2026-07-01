#!/usr/bin/env bash
#
# FindPicoquic.cmake WebTransport dependency contract (hermetic, no picoquic
# install needed). With MOQ_BUILD_ADAPTER_PICO_WT=ON:
#
#   negative: a picoquic core target present but NO picohttp target must fail
#             *in FindPicoquic* with a precise, actionable message -- never fall
#             through so the pico_wt adapter reports a generic "target not found".
#   positive: a non-namespaced picohttp-core must be normalized to
#             picoquic::picohttp-core and configuration must succeed.
#
# find_package(picoquic) is disabled in the fixtures so the host environment
# (any real installed picoquic) cannot influence the result.
#
# Args: $1 = absolute path to cmake/FindPicoquic.cmake
#       $2 = cmake executable (optional; defaults to "cmake")

set -euo pipefail

FINDMODULE="${1:?path to FindPicoquic.cmake required}"
CMAKE="${2:-cmake}"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

fail() { echo "FAIL: $1" >&2; exit 1; }

# -- negative: core present, no picohttp, WT on -> precise failure here -------
mkdir -p "$WORK/neg"
cat > "$WORK/neg/CMakeLists.txt" <<EOF
cmake_minimum_required(VERSION 3.20)
project(neg C)
set(CMAKE_DISABLE_FIND_PACKAGE_picoquic ON)
add_library(_fake_core INTERFACE)
add_library(picoquic::picoquic-core ALIAS _fake_core)
set(MOQ_BUILD_ADAPTER_PICO_WT ON)
include("${FINDMODULE}")
message(STATUS "REACHED_PAST_INCLUDE")
EOF
if "$CMAKE" -S "$WORK/neg" -B "$WORK/neg/b" > "$WORK/neg.log" 2>&1; then
    cat "$WORK/neg.log" >&2
    fail "negative case configured successfully (expected a FATAL from FindPicoquic)"
fi
grep -q "without an exported picohttp-core" "$WORK/neg.log" \
    || { cat "$WORK/neg.log" >&2; fail "negative case lacks the precise FindPicoquic message"; }
if grep -q "REACHED_PAST_INCLUDE" "$WORK/neg.log"; then
    fail "negative case fell through past FindPicoquic instead of failing there"
fi

# -- positive: core + non-namespaced picohttp-core -> normalized + success ----
mkdir -p "$WORK/pos"
cat > "$WORK/pos/CMakeLists.txt" <<EOF
cmake_minimum_required(VERSION 3.20)
project(pos C)
set(CMAKE_DISABLE_FIND_PACKAGE_picoquic ON)
add_library(_fake_core INTERFACE)
add_library(picoquic::picoquic-core ALIAS _fake_core)
add_library(picohttp-core INTERFACE)
set(MOQ_BUILD_ADAPTER_PICO_WT ON)
include("${FINDMODULE}")
if(NOT TARGET picoquic::picohttp-core)
    message(FATAL_ERROR "picohttp-core was not normalized to picoquic::picohttp-core")
endif()
EOF
"$CMAKE" -S "$WORK/pos" -B "$WORK/pos/b" > "$WORK/pos.log" 2>&1 \
    || { cat "$WORK/pos.log" >&2; fail "positive case failed to configure"; }

echo "PASS: findpicoquic_http_contract"
