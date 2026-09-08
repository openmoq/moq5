#!/usr/bin/env bash
# Keeps the scenario-ownership map and the CTest registrations honest:
#
#   1. a scenario listed as retired must not be registered or built again;
#   2. every successor named for a retired scenario must still be a registered
#      CTest, so the map cannot quietly point at something that has since gone.
#
# Arguments 1 and 2 are the tool and library test CMakeLists files, argument 3
# is the ownership map, and argument 4 is the project source root.
set -u

tool_cmake="${1:-}"
relay_cmake="${2:-}"
ownermap="${3:-}"
project_root="${4:-}"
if [ ! -f "$tool_cmake" ] || [ ! -f "$relay_cmake" ] ||
   [ ! -f "$ownermap" ] || [ ! -d "$project_root" ]; then
    echo "FAIL: usage: $0 <tool-CMakeLists.txt> <relay-test-CMakeLists.txt>" \
         "<scenario_owners.md> <project-root>" >&2
    exit 2
fi

failures=0
fail() { echo "FAIL: $1" >&2; failures=$((failures + 1)); }

# The CMake source with comments stripped, so prose about the retired fixture
# cannot satisfy — or trip — the checks below.
cml_src=$(
    sed 's/#.*//' "$tool_cmake"
    sed 's/#.*//' "$relay_cmake"
)

# Registered CTest names, and every executable the two files build.
registered=$(
    grep -h -o 'add_test(NAME [A-Za-z_0-9]*' \
        "$tool_cmake" "$relay_cmake" |
             sed 's/add_test(NAME //')
built=$(
    grep -h -o 'add_executable([A-Za-z_0-9]*' \
        "$tool_cmake" "$relay_cmake" |
        sed 's/add_executable(//')

is_registered() {
    printf '%s\n' "$registered" | grep -qx "$1"
}

# The retired table: rows whose first cell is a `backticked` scenario name and
# whose last cell lists backticked successors.
retired_rows=$(sed -n '/^## Retired scenarios/,/^## /p' "$ownermap" |
               grep '^| *`')

if [ -z "$retired_rows" ]; then
    echo "FAIL: $ownermap lists no retired scenarios; the table is the gate" >&2
    exit 1
fi

while IFS= read -r row; do
    scenario=$(printf '%s\n' "$row" | sed 's/^| *`\([^`]*\)`.*/\1/')
    successors=$(printf '%s\n' "$row" | awk -F'|' '{print $(NF-1)}' |
                 grep -o '`[A-Za-z_0-9]*`' | tr -d '`')

    if is_registered "$scenario"; then
        echo "FAIL: retired scenario '$scenario' is registered again in" \
             "$tool_cmake or $relay_cmake" >&2
        failures=$((failures + 1))
    fi
    target=$(printf '%s\n' "$row" | awk -F'|' '{print $3}' |
             sed 's/^ *//;s/ *$//')
    case "$target" in
    removed)
        if printf '%s\n' "$built" | grep -qx "test_$scenario"; then
            echo "FAIL: retired scenario '$scenario' still has a build" \
                 "target 'test_$scenario'" >&2
            failures=$((failures + 1))
        fi
        ;;
    retained)
        # Only legitimate while another registered test runs that executable.
        if ! printf '%s\n' "$built" | grep -qx "test_$scenario"; then
            echo "FAIL: '$scenario' claims a retained build target" \
                 "'test_$scenario', which does not exist" >&2
            failures=$((failures + 1))
        fi
        ;;
    *)
        echo "FAIL: retired scenario '$scenario' has no build-target" \
             "disposition (expected 'removed' or 'retained', got" \
             "'$target')" >&2
        failures=$((failures + 1))
        ;;
    esac
    if [ -z "$successors" ]; then
        echo "FAIL: retired scenario '$scenario' names no successor owner" >&2
        failures=$((failures + 1))
        continue
    fi
    for s in $successors; do
        if ! is_registered "$s"; then
            echo "FAIL: '$scenario' names successor '$s', which is not a" \
                 "registered test" >&2
            failures=$((failures + 1))
        fi
    done
done <<EOF
$retired_rows
EOF

# The REGISTERED OWNERS table: a claim must not lose its last owner silently.
owner_rows=$(sed -n '/^## Registered owners/,/^## Lane labels/p' "$ownermap" |
             grep '^| *[^|-]' | grep '`')

if [ -z "$owner_rows" ]; then
    echo "FAIL: $ownermap lists no registered owners; the table is the gate" >&2
    exit 1
fi

while IFS= read -r row; do
    claim=$(printf '%s\n' "$row" | awk -F'|' '{print $2}' | sed 's/^ *//;s/ *$//')
    owners=$(printf '%s\n' "$row" | awk -F'|' '{print $(NF-1)}' |
             grep -o '`[A-Za-z_0-9]*`' | tr -d '`')

    if [ -z "$owners" ]; then
        continue
    fi
    for o in $owners; do
        if ! is_registered "$o"; then
            echo "FAIL: claim '$claim' names owner '$o', which is not a" \
                 "registered test" >&2
            failures=$((failures + 1))
        fi
    done
done <<EOF
$owner_rows
EOF

# The boundary fixture must stay deterministic: the committed test-only
# certificate and key, never a certificate minted at run time. A generated
# credential makes the physical cells depend on the host's tooling and on the
# clock, for bytes no test ever inspects.
cert="$project_root/adapters/msquic/tests/test_only_loopback_cert.pem"
key="$project_root/adapters/msquic/tests/test_only_loopback_key.pem"

if [ ! -f "$cert" ]; then
    fail "the committed test-only certificate is missing: $cert"
fi
if [ ! -f "$key" ]; then
    fail "the committed test-only key is missing: $key"
fi
# A dependency root forwarded to a configure-only child is not certificate
# generation. Remove that one exact, inert CMake hint before retaining the
# closed ban on any OpenSSL tool/reference in the relay build description.
cml_without_crypto_hint=$(printf '%s\n' "$cml_src" |
    sed 's/OPENSSL_ROOT_DIR//g')
if printf '%s\n' "$cml_without_crypto_hint" | grep -qi 'openssl'; then
    fail "runtime certificate generation is back in the relay CMake"
fi
if printf '%s\n' "$cml_src" | grep -q 'relay_gen_certs'; then
    fail "the relay_gen_certs generation fixture is back"
fi
if ! printf '%s\n' "$cml_src" | grep -q 'test_only_loopback_cert.pem'; then
    fail "the relay CMake no longer names the committed certificate"
fi
if ! printf '%s\n' "$cml_src" | grep -q 'test_only_loopback_key.pem'; then
    fail "the relay CMake no longer names the committed key"
fi

if [ "$failures" -ne 0 ]; then
    echo "FAIL: $failures test-inventory violation(s)" >&2
    exit 1
fi
echo "PASS: test inventory agrees with the scenario-ownership map"
exit 0
