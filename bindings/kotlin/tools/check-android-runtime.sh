#!/bin/sh
# The Android RUNTIME acceptance gate.
#
# Every other gate in this tree judges the library on the host: source text,
# Gradle declarations, a JVM unit test, an ELF inspection, an archive listing.
# None of them can answer the only question a consumer actually asks -- does
# this thing run on Android? -- because none of them ever put the artifact on a
# runtime.
#
# This gate does. It judges five facts, each on a real device or emulator:
#
#   TARGET   exactly one runtime is selected, by identity, and its ABI is one
#            the AAR actually ships; discovery never guesses;
#   LOAD     libmoq5_jni.so is resolved by NAME out of the application's own
#            library directory -- the Android packaging route -- and the file
#            is present there, so a host classpath or a file-path load cannot
#            be mistaken for packaging;
#   BRIDGE   the private ABI/feature handshake in jni/moq5_abi.h succeeds
#            against the Kotlin side's compiled-in authority, on the device;
#   ENDPOINT PUBLIC construction plus a close request complete on device
#            WITHOUT the main thread: the oracle holds the main looper
#            unavailable, proves the operation completes during that hold,
#            and only then releases it and proves service resumes; a control
#            that genuinely needs the looper must be classified dependent;
#   SAMPLES  both first-party sample applications install as ordinary APKs and
#            reach a foreground activity without crashing;
#   FRESH    the PUBLIC open works in a process nothing else has touched: the
#            fresh-process test runs in its own instrumentation invocation,
#            BEFORE any test that performs an explicit handshake, so its
#            verdict cannot borrow another test's initialization.
#
# The instrumentation verdict is read from AGP's own JUnit XML, never scraped
# out of console text: a run that never executed a test writes zero tests, and
# a gate that grepped stdout for "BUILD SUCCESSFUL" would call that green.
#
# Target discovery is EXPLICIT in all three directions, because the failure
# mode this gate exists to prevent is a silent skip:
#
#   * no adb            -> named failure, exit 2;
#   * no target and no
#     startable AVD     -> environment BLOCKER, exit 3, never a pass;
#   * several targets
#     with ANDROID_SERIAL
#     unset             -> named refusal; this gate does not choose for you.
#
# The selected target may be SHARED, so what the gate may do to it is bounded:
#
#   * every package ID it installs (its own, the samples', and the test APK
#     AGP's connected task installs and removes on its behalf) must be absent
#     before anything is built or installed; a present one stops the run by
#     name with nothing installed or uninstalled, because it belongs to
#     someone else;
#   * this script's own `adb install` calls never pass `-r`, so an ID that
#     appears from elsewhere after that check is refused by THOSE installs
#     rather than overwritten. AGP's connected task installs the test APK
#     outside them and may replace (below);
#   * cleanup uninstalls only IDs proved absent AND then installed by this run;
#   * the device-global crash buffer is read, never cleared, and a read the
#     gate cannot fully parse fails the crash row rather than passing it.
#
# These are safeguards, NOT an exclusive lease on the device. The absence check
# and the installs are separate moments, and AGP's connected task installs the
# test APK itself, possibly with replace semantics, outside this script's adb
# calls: an owner who installs that same ID in between may be replaced, and its
# package then removed with ours. The gate therefore REQUIRES a target this run
# owns exclusively (for example an AVD created for it); on a shared target the
# checks narrow the damage but cannot rule it out.
#
# Warnings are errors here as everywhere else. Gradle, Kotlin, lint, R8, D8,
# adb and the device-side runner all write into one captured log, and a scan
# that finds any warning line the gate did not deliberately induce fails the
# run.
set -u
LC_ALL=C; export LC_ALL

_self=${0##*/}
case $0 in
    */*) _selfdir=${0%/*} ;;
    *)   _selfdir=. ;;
esac
here=$(cd "$_selfdir" && pwd) || { echo "$_self: cannot resolve the script directory" >&2; exit 2; }
root=$(cd "$here/.." && pwd)  || { echo "$_self: cannot resolve bindings/kotlin" >&2; exit 2; }

for t in find grep printf sed awk sort tr mktemp rm sleep; do
    command -v "$t" >/dev/null 2>&1 \
        || { echo "$_self: required tool not found: $t" >&2; exit 2; }
done

scratch=$(mktemp -d) || { echo "$_self: cannot create a scratch directory" >&2; exit 2; }
# The IDs the ownership preflight proved absent, and the IDs this run then
# installed successfully. Cleanup needs BOTH: an ID in only one of them is not
# this run's to remove.
absent_before=''
installed=''
cleanup() {
    if [ -n "$installed" ] && [ -n "${ADB:-}" ] && [ -n "${SERIAL:-}" ]; then
        for _p in $installed; do
            case " $absent_before " in
                *" $_p "*) "$ADB" -s "$SERIAL" uninstall "$_p" >/dev/null 2>&1 ;;
            esac
        done
    fi
    rm -rf -- "$scratch"
}
trap 'cleanup; exit 130' HUP INT TERM
trap cleanup EXIT

fails=0
passes=0
pass()    { passes=$((passes + 1)); printf 'PASS[%s]: %s\n' "$1" "$2"; }
fail()    { fails=$((fails + 1)); printf 'FAIL[%s]: %s\n' "$1" "$2"; }
blocker() { printf 'BLOCKER[%s]: %s\n' "$1" "$2"; }

# One log for everything any tool writes, so the warning scan has a single
# subject rather than a per-command opinion about what was worth keeping.
log="$scratch/runtime.log"
: > "$log"
run() { "$@" >>"$log" 2>&1; }

# --- the SDK, discovered exactly the way the rest of the project does -------
# ANDROID_HOME and ANDROID_SDK_ROOT first, then an untracked local.properties.
# A pin is never an installation path, so nothing here is read from
# gradle/toolchain.properties.
sdk=''
for candidate in "${ANDROID_HOME:-}" "${ANDROID_SDK_ROOT:-}"; do
    [ -n "$candidate" ] && [ -d "$candidate" ] && { sdk=$candidate; break; }
done
if [ -z "$sdk" ] && [ -f "$root/local.properties" ]; then
    sdk=$(sed -n 's/^[[:space:]]*sdk\.dir[[:space:]]*=[[:space:]]*//p' "$root/local.properties" \
          | sed 's/\\:/:/g' | head -1)
fi
if [ -z "$sdk" ] || [ ! -d "$sdk" ]; then
    fail runtime.sdk-resolved \
        "no Android SDK: ANDROID_HOME, ANDROID_SDK_ROOT and local.properties sdk.dir all failed to name an existing directory"
    printf '\n%s: %d PASS / %d FAIL\n' "$_self" "$passes" "$fails"
    exit 1
fi
pass runtime.sdk-resolved "SDK at $sdk"

ADB="$sdk/platform-tools/adb"
if [ ! -x "$ADB" ]; then
    fail runtime.adb-present "no executable adb at $ADB"
    printf '\n%s: %d PASS / %d FAIL\n' "$_self" "$passes" "$fails"
    exit 2
fi
adb_version=$("$ADB" version 2>/dev/null | head -1)
pass runtime.adb-present "$ADB ($adb_version)"

# --- target discovery ------------------------------------------------------
# `adb devices` prints a header line and then one "<serial>\t<state>" row per
# attached target. Only the `device` state is usable: `offline` and
# `unauthorized` are targets the gate must NOT silently wait out or select.
list_ready() {
    "$ADB" devices 2>/dev/null \
        | awk 'NR > 1 && $2 == "device" { print $1 }' \
        | sort
}
list_all() {
    "$ADB" devices 2>/dev/null | awk 'NR > 1 && NF >= 2 { print $1 " " $2 }' | sort
}

ready=$(list_ready)
ready_count=$(printf '%s' "$ready" | grep -c . || true)

if [ "$ready_count" -eq 0 ]; then
    # Nothing attached. An emulator may still be startable from THIS SDK; that
    # is the only automatic escalation, and it is still explicit about which
    # AVD it chose.
    emu="$sdk/emulator/emulator"
    if [ ! -x "$emu" ]; then
        blocker runtime.target-discovered \
            "no target in the 'device' state and no emulator binary at $emu"
        exit 3
    fi
    avds=$("$emu" -list-avds 2>/dev/null | grep -c . || true)
    if [ "$avds" -eq 0 ]; then
        blocker runtime.target-discovered \
            "no target in the 'device' state and the local SDK declares no AVD ($emu -list-avds is empty)"
        exit 3
    fi
    chosen=${MOQ5_RUNTIME_AVD:-}
    if [ -z "$chosen" ]; then
        if [ "$avds" -gt 1 ]; then
            fail runtime.target-discovered \
                "$avds AVDs are declared and MOQ5_RUNTIME_AVD is unset; this gate does not choose an AVD for you"
            printf '\n%s: %d PASS / %d FAIL\n' "$_self" "$passes" "$fails"
            exit 1
        fi
        chosen=$("$emu" -list-avds 2>/dev/null | head -1)
    fi
    printf 'note: starting AVD %s from %s\n' "$chosen" "$emu"
    ( cd "$sdk/emulator" && ./emulator -avd "$chosen" -no-window -no-audio \
        -no-boot-anim -no-snapshot -gpu swiftshader_indirect >>"$log" 2>&1 & )
    waited=0
    while [ "$waited" -lt 300 ]; do
        ready=$(list_ready)
        ready_count=$(printf '%s' "$ready" | grep -c . || true)
        if [ "$ready_count" -ge 1 ]; then
            booted=$("$ADB" -s "$(printf '%s' "$ready" | head -1)" shell getprop sys.boot_completed 2>/dev/null | tr -d '\r')
            [ "$booted" = "1" ] && break
        fi
        sleep 5
        waited=$((waited + 5))
    done
    ready=$(list_ready)
    ready_count=$(printf '%s' "$ready" | grep -c . || true)
    if [ "$ready_count" -eq 0 ]; then
        blocker runtime.target-discovered \
            "AVD $chosen did not reach the 'device' state within ${waited}s"
        exit 3
    fi
fi

if [ -n "${ANDROID_SERIAL:-}" ]; then
    if ! printf '%s\n' "$ready" | grep -qx -- "$ANDROID_SERIAL"; then
        fail runtime.target-selected \
            "ANDROID_SERIAL names '$ANDROID_SERIAL', which is not in the 'device' state; attached: $(list_all | tr '\n' ' ')"
        printf '\n%s: %d PASS / %d FAIL\n' "$_self" "$passes" "$fails"
        exit 1
    fi
    SERIAL=$ANDROID_SERIAL
elif [ "$ready_count" -gt 1 ]; then
    fail runtime.target-selected \
        "$ready_count targets are in the 'device' state and ANDROID_SERIAL is unset; this gate refuses to choose one ($(printf '%s' "$ready" | tr '\n' ' '))"
    printf '\n%s: %d PASS / %d FAIL\n' "$_self" "$passes" "$fails"
    exit 1
else
    SERIAL=$(printf '%s' "$ready" | head -1)
fi
pass runtime.target-selected "$SERIAL"

prop() { "$ADB" -s "$SERIAL" shell getprop "$1" 2>/dev/null | tr -d '\r'; }
api=$(prop ro.build.version.sdk)
rel=$(prop ro.build.version.release)
abi=$(prop ro.product.cpu.abi)
abilist=$(prop ro.product.cpu.abilist)
model=$(prop ro.product.model)

if [ -z "$api" ] || [ -z "$abi" ]; then
    fail runtime.target-identity "the target answered no ro.build.version.sdk or ro.product.cpu.abi"
else
    pass runtime.target-identity \
        "model=$model api=$api release=$rel abi=$abi abilist=$abilist"
fi

# The AAR ships exactly the ABIs the audited topology packages. A target whose
# ABI is outside that set could only ever prove the library does not run there,
# so the mismatch is named rather than discovered as a load failure.
shipped=$(find "$root/.native/_topology" -maxdepth 1 -mindepth 1 -type d 2>/dev/null \
          | sed 's#.*/##' | grep -v '^_' | sort | tr '\n' ' ')

# True when any ABI the AAR ships appears in the target's own abilist. Written
# as a function so the loop's early answer is a RETURN, not an exit that would
# end the gate mid-run.
abi_intersects() {
    for _a in $shipped; do
        printf '%s\n' "$abilist" | tr ',' '\n' | grep -qxF "$_a" && return 0
    done
    return 1
}

if [ -z "$shipped" ]; then
    fail runtime.target-abi-shipped \
        "the audited topology under $root/.native/_topology declares no ABI; run tools/build-jni-topology.sh"
elif abi_intersects; then
    pass runtime.target-abi-shipped "target abilist [$abilist] intersects the shipped ABIs [$shipped]"
else
    fail runtime.target-abi-shipped \
        "target abilist [$abilist] contains none of the shipped ABIs [$shipped]"
fi

# --- device ownership ------------------------------------------------------
# The test application's id is AGP's for a library's androidTest:
# `<namespace>.test`. The samples are identified by their AGP-default
# applicationId (no applicationId is declared, so it is the namespace) and
# their manifest launcher activity.
TEST_PKG='org.openmoq.moq5.test'
SAMPLES='player:org.openmoq.moq5.samples.player:.PlayerActivity
publisher:org.openmoq.moq5.samples.publisher:.PublisherActivity'
OWNED_IDS=$TEST_PKG
for row in $SAMPLES; do
    rest=${row#*:}
    OWNED_IDS="$OWNED_IDS ${rest%%:*}"
done

# `-u` also lists packages uninstalled with their data kept, which are not this
# run's either. A device always lists some package, so a listing that fails or
# comes back empty proves nothing about absence and stops the run.
pkglist="$scratch/packages.txt"
if "$ADB" -s "$SERIAL" shell pm list packages -u >"$pkglist" 2>>"$log"    && grep -q '^package:' "$pkglist"; then
    present=''
    for _p in $OWNED_IDS; do
        tr -d '\r' <"$pkglist" | grep -qxF "package:$_p" && present="$present $_p"
    done
    if [ -n "$present" ]; then
        fail runtime.ownership.absent \
            "already installed on $SERIAL before this run:$present; this gate neither replaces nor removes a package it did not install, so nothing was built, installed or uninstalled"
        printf '\n%s: %d PASS / %d FAIL\n' "$_self" "$passes" "$fails"
        exit 1
    fi
    absent_before=$OWNED_IDS
    pass runtime.ownership.absent "none of [$OWNED_IDS] is installed on $SERIAL"
else
    fail runtime.ownership.absent \
        "the package listing on $SERIAL could not be read, so the absence of [$OWNED_IDS] is unproved; nothing was built, installed or uninstalled"
    printf '\n%s: %d PASS / %d FAIL\n' "$_self" "$passes" "$fails"
    exit 1
fi

# --- instrumentation -------------------------------------------------------
results="$root/moq5-android/build/outputs/androidTest-results/connected"
rm -rf -- "$results"

ANDROID_SERIAL=$SERIAL; export ANDROID_SERIAL

# The instrumentation APK, built by AGP and installed by adb here so the
# fresh-process row can be launched with `am instrument` in an invocation of
# its own. The runner is the declared one. Installs never pass `-r`: adb then
# refuses an ID that is already present instead of replacing it.
RUNNER='androidx.test.runner.AndroidJUnitRunner'
FRESH_CLASS='org.openmoq.moq5.runtime.FreshProcessPublicOpenTest'
FRESH_METHOD='publicOpenInAFreshProcessNeedsNoExplicitInitialization'

printf 'note: building and installing the instrumentation APK on %s\n' "$SERIAL"
run "$here/gradlew-verified" :moq5-android:assembleDebugAndroidTest --console=plain
test_apk="$root/moq5-android/build/outputs/apk/androidTest/debug/moq5-android-debug-androidTest.apk"
if [ ! -f "$test_apk" ]; then
    fail runtime.fresh-process.installed "no instrumentation APK at ${test_apk#$root/}"
elif run "$ADB" -s "$SERIAL" install -t "$test_apk"; then
    installed="$installed $TEST_PKG"
    pass runtime.fresh-process.installed "$TEST_PKG installed from ${test_apk#$root/}"
else
    fail runtime.fresh-process.installed "adb install refused the instrumentation APK"
fi

# ONE method, in ONE process that has run nothing else. `-r` gives the raw
# per-test status protocol: code 1 is a start, 0 a pass, -1 an error and -2 a
# failure; the run's own summary is INSTRUMENTATION_CODE (-1 is the runner's
# normal completion). The row requires exactly one start and one pass for the
# named method and nothing else, so a run that executed zero tests, a
# different test, or the method plus a failure cannot read as green.
fresh_out="$scratch/fresh.txt"
"$ADB" -s "$SERIAL" shell am instrument -w -r \
    -e class "$FRESH_CLASS#$FRESH_METHOD" \
    "$TEST_PKG/$RUNNER" > "$fresh_out" 2>&1
cat "$fresh_out" >> "$log"
starts=$(grep -c 'INSTRUMENTATION_STATUS_CODE: 1' "$fresh_out" || true)
passes_=$(grep -c 'INSTRUMENTATION_STATUS_CODE: 0' "$fresh_out" || true)
bad=$(grep -cE 'INSTRUMENTATION_STATUS_CODE: -[12]' "$fresh_out" || true)
named=$(grep -c "INSTRUMENTATION_STATUS: test=$FRESH_METHOD" "$fresh_out" || true)
if [ "$starts" -eq 1 ] && [ "$passes_" -eq 1 ] && [ "$bad" -eq 0 ] && [ "$named" -ge 1 ] \
   && grep -q 'INSTRUMENTATION_CODE: -1' "$fresh_out"; then
    pass runtime.fresh-process.public-open \
        "$FRESH_METHOD passed in its own instrumentation process on $SERIAL"
else
    fail runtime.fresh-process.public-open \
        "$FRESH_METHOD did not pass alone in a fresh process (starts=$starts passes=$passes_ failures=$bad named=$named): $(grep -E 'INSTRUMENTATION_STATUS: stack=|INSTRUMENTATION_RESULT|Error' "$fresh_out" | head -3 | cut -c1-200 | tr '\n' ' ')"
fi

# The rest of the suite, in a SEPARATE process, with the fresh-process class
# excluded by name: its precondition is an undecided bridge, which no shared
# process can offer twice.
printf 'note: running :moq5-android:connectedDebugAndroidTest on %s\n' "$SERIAL"
run "$here/gradlew-verified" :moq5-android:connectedDebugAndroidTest \
    "-Pandroid.testInstrumentationRunnerArguments.notClass=$FRESH_CLASS" \
    --rerun-tasks --console=plain
gradle_status=$?

# AGP names the report after the device -- "TEST-moq5_api36_arm64(AVD) - 16-...
# .xml" -- so the path carries spaces and parentheses. Every report is
# concatenated into ONE scratch file and parsed from there: passing the found
# paths to awk unquoted split them on whitespace, and awk then reported no
# counts at all, which the rows below read as zero failures. An oracle whose
# quoting bug looks like success is worse than no oracle.
report="$scratch/androidtest.xml"
: > "$report"
found_any=no
find "$results" -name '*.xml' -type f -exec cat {} + >> "$report" 2>/dev/null
[ -s "$report" ] && found_any=yes

# The named observations this gate exists to make. Each must be PRESENT in the
# report and must not have failed; naming them here is what stops the runtime
# proof from being deleted while the count stays plausible.
OBSERVATIONS='packagedImageResolvesFromTheApplicationLibraryDirectory
bridgeHandshakeAgreesWithTheCompiledAuthority
endpointConstructAndCloseCompletesOffTheMainThread
workThatNeedsTheMainLooperIsClassifiedAsDependent
aWorkerThatSurvivesCleanupIsRefusedAsAHarnessFailure
snapshotOnAnIdleEndpointReturnsPromptly
closeAndJoinOnAConnectingEndpointReturnsPromptly'

if [ "$found_any" = no ]; then
    fail runtime.instrumentation-ran \
        "connectedDebugAndroidTest wrote no JUnit XML under $results (gradle exit $gradle_status)"
    # A report that does not exist has not observed anything. Reporting the
    # named rows here too keeps the ledger the same width however the run
    # failed, so a missing report cannot quietly shrink the evidence.
    for named in $OBSERVATIONS; do
        fail "runtime.observed.$named" "no instrumentation report to observe it in"
    done
else
    counts=$(awk '
        /<testsuite / {
            t = f = e = s = 0
            if (match($0, /tests="[0-9]+"/))    t = substr($0, RSTART + 7, RLENGTH - 8)
            if (match($0, /failures="[0-9]+"/)) f = substr($0, RSTART + 10, RLENGTH - 11)
            if (match($0, /errors="[0-9]+"/))   e = substr($0, RSTART + 8, RLENGTH - 9)
            if (match($0, /skipped="[0-9]+"/))  s = substr($0, RSTART + 9, RLENGTH - 10)
            T += t; F += f; E += e; S += s
            suites++
        }
        END { if (suites) printf "%d %d %d %d", T, F, E, S }' "$report")
    if [ -z "$counts" ]; then
        # The file exists but carries no <testsuite>. Fail closed rather than
        # letting empty values flow into the integer comparisons below.
        fail runtime.instrumentation-ran "the instrumentation report declares no test suite"
        for named in $OBSERVATIONS; do
            fail "runtime.observed.$named" "the instrumentation report declares no test suite"
        done
    else
        tests=$(printf '%s' "$counts" | awk '{print $1}')
        tfail=$(printf '%s' "$counts" | awk '{print $2}')
        terr=$(printf '%s' "$counts" | awk '{print $3}')
        tskip=$(printf '%s' "$counts" | awk '{print $4}')
        if [ "$tests" -eq 0 ]; then
            fail runtime.instrumentation-ran "the instrumentation report records zero tests"
        else
            pass runtime.instrumentation-ran "$tests instrumentation tests executed on $SERIAL"
        fi
        if [ "$tfail" -ne 0 ] || [ "$terr" -ne 0 ]; then
            fail runtime.instrumentation-green "$tfail failures and $terr errors in the instrumentation report"
        else
            pass runtime.instrumentation-green "0 failures, 0 errors"
        fi
        if [ "$tskip" -ne 0 ]; then
            fail runtime.instrumentation-unskipped \
                "$tskip instrumentation tests were skipped; a skipped runtime test is not a runtime proof"
        else
            pass runtime.instrumentation-unskipped "0 skipped"
        fi

        # Presence is not enough: a <testcase> that FAILED still appears in the
        # report, so each named row is judged on whether its element carries a
        # <failure> or <error> child.
        for named in $OBSERVATIONS; do
            verdict=$(awk -v want="$named" '
                /<testcase / {
                    if (match($0, /name="[^"]*"/)) {
                        n = substr($0, RSTART + 6, RLENGTH - 7)
                    } else {
                        n = ""
                    }
                    here = (n == want)
                    if (here) { seen = 1; if ($0 ~ /\/>[[:space:]]*$/) { ok = 1; here = 0 } }
                    next
                }
                here && /<(failure|error)[ >]/ { bad = 1 }
                here && /<\/testcase>/ { if (!bad) ok = 1; here = 0 }
                END { if (!seen) print "absent"; else if (bad || !ok) print "failed"; else print "passed" }
            ' "$report")
            case $verdict in
                passed) pass "runtime.observed.$named" "executed and passed on $SERIAL" ;;
                failed) fail "runtime.observed.$named" "executed on $SERIAL and FAILED" ;;
                *)      fail "runtime.observed.$named" "absent from the instrumentation report" ;;
            esac
        done
    fi
fi

if [ "$found_any" = yes ] && grep -q "name=\"$FRESH_METHOD\"" "$report"; then
    fail runtime.fresh-process.isolated \
        "$FRESH_METHOD also ran inside the shared suite process; its verdict there is not about a fresh process"
elif [ "$found_any" = yes ]; then
    pass runtime.fresh-process.isolated "the fresh-process test ran only in its own process"
else
    fail runtime.fresh-process.isolated "no suite report to prove the isolation against"
fi

if [ "$gradle_status" -ne 0 ]; then
    fail runtime.instrumentation-exit "connectedDebugAndroidTest exited $gradle_status"
else
    pass runtime.instrumentation-exit "connectedDebugAndroidTest exited 0"
fi

# --- sample applications ---------------------------------------------------
# player and publisher, as SAMPLES declares them above.
printf 'note: assembling the sample debug APKs\n'
run "$here/gradlew-verified" :samples:player:assembleDebug :samples:publisher:assembleDebug \
    --console=plain
assemble_status=$?
if [ "$assemble_status" -ne 0 ]; then
    fail runtime.samples-assembled "sample assembleDebug exited $assemble_status"
else
    pass runtime.samples-assembled "player and publisher debug APKs built"
fi

for row in $SAMPLES; do
    name=${row%%:*}
    rest=${row#*:}
    pkg=${rest%%:*}
    activity=${rest#*:}

    apk=$(find "$root/samples/$name/build/outputs/apk/debug" -name '*.apk' 2>/dev/null | sort | head -1)
    if [ -z "$apk" ]; then
        fail "runtime.sample-apk.$name" "no debug APK under samples/$name/build/outputs/apk/debug"
        continue
    fi
    pass "runtime.sample-apk.$name" "${apk#$root/}"

    if run "$ADB" -s "$SERIAL" install -t "$apk"; then
        installed="$installed $pkg"
        pass "runtime.sample-install.$name" "$pkg installed on $SERIAL"
    else
        fail "runtime.sample-install.$name" "adb install refused $pkg"
        continue
    fi

    # The crash row must be a statement about THIS launch rather than about
    # the device's history, and the buffer is device-global, so it is bounded
    # in time instead of cleared: the device clock is read immediately before
    # the launch, and only `-v epoch` entries stamped at or after that second
    # are judged. An entry from earlier in the same second is judged too, which
    # can only fail the row, never pass it. Without a readable mark nothing can
    # be attributed to the launch, and the row fails.
    mark=$("$ADB" -s "$SERIAL" shell date +%s 2>>"$log" | tr -d '\r')
    case $mark in
        ''|*[!0-9]*) mark='' ;;
    esac
    start_out="$scratch/start-$name.txt"
    "$ADB" -s "$SERIAL" shell am start -W -n "$pkg/$activity" >"$start_out" 2>&1
    cat "$start_out" >>"$log"
    if grep -q 'Status: ok' "$start_out" && ! grep -qi '^Error' "$start_out"; then
        pass "runtime.sample-launch.$name" "$(grep -E '^(Status|TotalTime):' "$start_out" | tr '\n' ' ')"
    else
        fail "runtime.sample-launch.$name" "am start did not report Status: ok: $(tr '\n' ' ' <"$start_out")"
        continue
    fi

    # Give the activity a moment to reach its first frame and to run whatever
    # it starts there, then require the process to still exist. A crash-on-start
    # is exactly the failure a launch check is for, and a dead process is the
    # observable form of it.
    sleep 5
    pid=$("$ADB" -s "$SERIAL" shell pidof "$pkg" 2>/dev/null | tr -d '\r')
    if [ -n "$pid" ]; then
        pass "runtime.sample-alive.$name" "$pkg still running as pid $pid after launch"
    else
        fail "runtime.sample-alive.$name" "$pkg is not running 5s after a successful launch"
    fi

    # The read's diagnostics are evidence too: they are kept in the gate's log
    # (where the warning scan also sees them), and a read that succeeded while
    # reporting ANY diagnostic is not a clean read of the buffer.
    crash="$scratch/crash-$name.txt"
    crash_err="$scratch/crash-$name.err"
    since="$scratch/crash-$name-since.txt"
    if [ -z "$mark" ]; then
        fail "runtime.sample-nocrash.$name" \
            "the device clock could not be read before the launch, so no crash entry can be attributed to it"
    elif ! "$ADB" -s "$SERIAL" logcat -b crash -d -v epoch >"$crash" 2>"$crash_err"; then
        cat "$crash_err" >>"$log"
        fail "runtime.sample-nocrash.$name" \
            "the crash buffer could not be read: $(head -1 "$crash_err" | cut -c1-160)"
    elif cat "$crash_err" >>"$log" && [ -s "$crash_err" ]; then
        fail "runtime.sample-nocrash.$name" \
            "the crash read succeeded but wrote diagnostics: $(head -1 "$crash_err" | cut -c1-160)"
    elif ! awk -v m="$mark" -v since="$since" '
            # `-v epoch` output is buffer dividers and records only:
            #   --------- beginning of crash
            #     1758600000.123  1234  1234 E AndroidRuntime: ...
            # Every other non-blank line is output this gate does not
            # understand, and an unread entry cannot be declared crash-free.
            /^[[:space:]]*$/ { next }
            /^--------- (beginning of|switch to) [a-z]+$/ { next }
            /^[[:space:]]*[0-9]+[.][0-9]+[[:space:]]+[0-9]+[[:space:]]+[0-9]+[[:space:]]+[VDIWEFA][[:space:]]/ {
                t = $1; sub(/[.].*/, "", t)
                if (t + 0 >= m + 0) print > since
                next
            }
            { if (!bad++) first = $0 }
            END { printf "" > since; if (bad) { print first; exit 1 } }
        ' "$crash" >"$scratch/crash-$name-unparsed.txt"; then
        fail "runtime.sample-nocrash.$name" \
            "the crash buffer returned a line that is neither a divider nor an epoch record: $(head -1 "$scratch/crash-$name-unparsed.txt" | cut -c1-160)"
    else
        if grep -qF "$pkg" "$since"; then
            fail "runtime.sample-nocrash.$name" \
                "the crash buffer names $pkg since its launch: $(grep -m2 -F "$pkg" "$since" | tr '\n' ' ')"
        else
            pass "runtime.sample-nocrash.$name" \
                "no crash entry since the launch (device second $mark) names $pkg"
        fi
    fi
done

# --- warnings are errors ---------------------------------------------------
# Everything every tool wrote is in one file. The gate's own rows are excluded
# by their exact prefixes, and nothing else is: a warning from Gradle, Kotlin,
# lint, R8, D8, adb or the device runner is a blocker here.
warn="$scratch/warnings.txt"
grep -in 'warning' "$log" 2>/dev/null \
    | grep -v ':PASS\[' | grep -v ':FAIL\[' | grep -v ':BLOCKER\[' \
    > "$warn" || true
warn_count=$(grep -c . "$warn" || true)
if [ "$warn_count" -eq 0 ]; then
    pass runtime.no-warnings "no tool wrote a warning line during the runtime run"
else
    fail runtime.no-warnings "$warn_count warning lines in the captured runtime log:"
    sed -n '1,20p' "$warn" | sed 's/^/    /'
    printf '    (full log kept at %s)\n' "$log"
    cp "$log" "$root/build/android-runtime.log" 2>/dev/null || true
fi

printf '\n%s: %d PASS / %d FAIL\n' "$_self" "$passes" "$fails"
[ "$fails" -eq 0 ] || exit 1
exit 0
