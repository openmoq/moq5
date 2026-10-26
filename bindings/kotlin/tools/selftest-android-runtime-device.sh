#!/bin/sh
# The runtime DEVICE gate's host-side ledger for what it does to a device.
#
# tools/check-android-runtime.sh installs APKs, uninstalls them again, and
# reads the device's crash buffer. A device it runs on may be shared: a package
# with one of its IDs may already be there, owned by someone else, and the
# crash buffer holds other launches' history. What the gate may do to that
# device is therefore a contract of its own:
#
#   OWNERSHIP  every package ID the gate would install is proved ABSENT before
#              anything is built or installed; a present one stops the run by
#              name, with nothing installed and nothing uninstalled;
#   CLEANUP    the gate uninstalls only IDs it proved absent AND then installed
#              successfully itself -- never one whose install was refused, and
#              never one that appeared from elsewhere after the preflight and
#              that the gate's OWN adb install therefore refused;
#   CRASH      the no-crash row judges only crash entries logged after its own
#              launch began, WITHOUT clearing the device-global buffer: a new
#              crash of the launched package fails by name, an older crash in
#              the buffer neither fails the row nor is erased.
#
# This is not an exclusive-device lease, and nothing here claims one: AGP's
# connected task installs the test APK with replace semantics outside the
# gate's own adb calls, so an owner who installs that ID after the preflight is
# still replaced. The foreign-arrival case below therefore uses a SAMPLE ID,
# which only the gate itself installs, and proves nothing about AGP's install.
#
# None of that can be judged on a real device without risking the device, so
# the REAL gate is run here against a fake SDK whose adb keeps package and
# crash-buffer state in files, and a fake Gradle that writes the artifacts and
# reports AGP would. Each case sets one device fact and requires the matching
# outcome by name.
set -u
LC_ALL=C; export LC_ALL

_self=${0##*/}
case $0 in
    */*) _selfdir=${0%/*} ;;
    *)   _selfdir=. ;;
esac
here=$(cd "$_selfdir" && pwd) || { echo "$_self: cannot resolve the script directory" >&2; exit 2; }

for t in mktemp rm cp mkdir sed grep printf cat awk chmod; do
    command -v "$t" >/dev/null 2>&1 \
        || { echo "$_self: required tool not found: $t" >&2; exit 2; }
done

W=$(mktemp -d) || { echo "$_self: cannot create a scratch directory" >&2; exit 2; }
cleanup() { rm -rf -- "$W"; }
trap 'cleanup; exit 130' HUP INT TERM
trap cleanup EXIT

fails=0
passes=0
pass() { passes=$((passes + 1)); printf 'PASS[%s]: %s\n' "$1" "$2"; }
fail() { fails=$((fails + 1)); printf 'FAIL[%s]: %s\n' "$1" "$2"; }

TEST_PKG='org.openmoq.moq5.test'
PLAYER='org.openmoq.moq5.samples.player'
PUBLISHER='org.openmoq.moq5.samples.publisher'

R="$W/root"          # the bindings/kotlin mirror the gate runs in
SDK="$W/sdk"         # ANDROID_HOME, holding the fake adb
ST="$W/state"        # the fake device

# The fake device. State is plain files under $ST:
#   packages       installed package IDs, one per line
#   crash          the crash buffer, `-v epoch` lines ("<sec>.<ms> ...")
#   now            the device clock, epoch seconds
#   calls          every adb invocation, one per line
#   dead           packages whose process has died
#   refuse         package IDs whose install adb refuses
#   appear         package IDs installed by someone else between the
#                  preflight and this gate's own install
#   crash_launch   package IDs that crash when launched
#   no_pm / no_date   the listing / the clock query fails
#   garbled        the crash read exits 0 with text that is not logcat's
#                  `-v epoch` output
#   noisy          the crash read exits 0 with valid output AND a diagnostic
#                  on stderr
write_adb() {
    mkdir -p "$SDK/platform-tools" || return 1
    cat > "$SDK/platform-tools/adb" <<'ADB'
#!/bin/sh
st=$FAKE_ADB_STATE
printf '%s\n' "$*" >> "$st/calls"
has() { grep -qxF -- "$2" "$st/$1" 2>/dev/null; }
pkg_of() {
    case $1 in
        *androidTest*)          echo org.openmoq.moq5.test ;;
        */samples/player/*)     echo org.openmoq.moq5.samples.player ;;
        */samples/publisher/*)  echo org.openmoq.moq5.samples.publisher ;;
        *)                      echo unknown ;;
    esac
}
case $1 in
    version) echo 'Android Debug Bridge version 1.0.41'; exit 0 ;;
    devices) printf 'List of devices attached\nemulator-5554\tdevice\n\n'; exit 0 ;;
    -s) shift 2 ;;
    *) exit 1 ;;
esac
case "$1 $2" in
    'shell getprop')
        case $3 in
            sys.boot_completed) echo 1 ;;
            ro.build.version.sdk) echo 36 ;;
            ro.build.version.release) echo 16 ;;
            ro.product.cpu.abi|ro.product.cpu.abilist) echo arm64-v8a ;;
            ro.product.model) echo fake ;;
        esac
        exit 0 ;;
    'shell pm')
        [ -f "$st/no_pm" ] && { echo 'Error: could not access the Package Manager' >&2; exit 1; }
        printf 'package:android\npackage:com.android.shell\n'
        sed 's/^/package:/' "$st/packages"
        exit 0 ;;
    'shell date')
        [ -f "$st/no_date" ] && { echo 'date: unavailable' >&2; exit 1; }
        cat "$st/now"; exit 0 ;;
    'shell pidof')
        has packages "$3" && ! has dead "$3" && echo 4242
        exit 0 ;;
    'shell am')
        if [ "$3" = instrument ]; then
            has packages org.openmoq.moq5.test \
                || { echo 'INSTRUMENTATION_STATUS: Error=Unable to find instrumentation info'; exit 0; }
            m=publicOpenInAFreshProcessNeedsNoExplicitInitialization
            printf 'INSTRUMENTATION_STATUS: test=%s\nINSTRUMENTATION_STATUS_CODE: 1\n' "$m"
            printf 'INSTRUMENTATION_STATUS: test=%s\nINSTRUMENTATION_STATUS_CODE: 0\n' "$m"
            printf 'INSTRUMENTATION_CODE: -1\n'
            exit 0
        fi
        comp=; for a in "$@"; do comp=$a; done
        p=${comp%%/*}
        has packages "$p" || { echo "Error: Activity class {$comp} does not exist."; exit 0; }
        printf 'Starting: Intent { cmp=%s }\nStatus: ok\nTotalTime: 10\n' "$comp"
        if has crash_launch "$p"; then
            n=$(cat "$st/now")
            printf '%s.500  4242  4242 E AndroidRuntime: FATAL EXCEPTION: main\n' "$n" >> "$st/crash"
            printf '%s.500  4242  4242 E AndroidRuntime: Process: %s, PID: 4242\n' "$n" "$p" >> "$st/crash"
            echo "$p" >> "$st/dead"
        fi
        exit 0 ;;
esac
case $1 in
    install)
        shift; replace=no
        for a in "$@"; do [ "$a" = -r ] && replace=yes; apk=$a; done
        p=$(pkg_of "$apk")
        if has appear "$p"; then
            grep -vxF -- "$p" "$st/appear" > "$st/appear.t"; mv "$st/appear.t" "$st/appear"
            echo "$p" >> "$st/packages"
        fi
        has refuse "$p" && { echo 'Failure [INSTALL_FAILED_INTERNAL_ERROR]'; exit 1; }
        if has packages "$p"; then
            [ "$replace" = yes ] || { echo "Failure [INSTALL_FAILED_ALREADY_EXISTS: Attempt to re-install $p without first uninstalling.]"; exit 1; }
        else
            echo "$p" >> "$st/packages"
        fi
        echo Success; exit 0 ;;
    uninstall)
        has packages "$2" || { echo 'Failure [DELETE_FAILED_INTERNAL_ERROR]'; exit 1; }
        grep -vxF -- "$2" "$st/packages" > "$st/packages.t"; mv "$st/packages.t" "$st/packages"
        echo Success; exit 0 ;;
    logcat)
        for a in "$@"; do
            [ "$a" = -c ] && { : > "$st/crash"; exit 0; }
        done
        [ -f "$st/garbled" ] && { printf 'logd: read failure, buffer truncated\n'; exit 0; }
        [ -f "$st/noisy" ] && echo 'logcat: buffer read was truncated' >&2
        [ -s "$st/crash" ] && echo '--------- beginning of crash'
        cat "$st/crash"; exit 0 ;;
esac
exit 1
ADB
    chmod +x "$SDK/platform-tools/adb"
}

# The fake Gradle: the three task shapes the gate invokes. The connected task
# behaves as AGP's does -- it installs the test APK itself (replacing) and
# uninstalls it when done -- which is why the preflight must also cover the IDs
# AGP installs on the gate's behalf.
write_gradle() {
    cat > "$R/tools/gradlew-verified" <<'GRADLE'
#!/bin/sh
r=$(cd "$(dirname "$0")/.." && pwd)
printf 'gradle %s\n' "$*" >> "$FAKE_ADB_STATE/calls"
adb="$FAKE_SDK/platform-tools/adb"
case $1 in
    :moq5-android:assembleDebugAndroidTest)
        d="$r/moq5-android/build/outputs/apk/androidTest/debug"
        mkdir -p "$d" && : > "$d/moq5-android-debug-androidTest.apk" ;;
    :moq5-android:connectedDebugAndroidTest)
        apk="$r/moq5-android/build/outputs/apk/androidTest/debug/moq5-android-debug-androidTest.apk"
        "$adb" -s "$ANDROID_SERIAL" install -r -t "$apk" >/dev/null || exit 1
        d="$r/moq5-android/build/outputs/androidTest-results/connected"
        mkdir -p "$d"
        {
            echo '<testsuite name="runtime" tests="7" failures="0" errors="0" skipped="0">'
            for n in packagedImageResolvesFromTheApplicationLibraryDirectory \
                     bridgeHandshakeAgreesWithTheCompiledAuthority \
                     endpointConstructAndCloseCompletesOffTheMainThread \
                     workThatNeedsTheMainLooperIsClassifiedAsDependent \
                     aWorkerThatSurvivesCleanupIsRefusedAsAHarnessFailure \
                     snapshotOnAnIdleEndpointReturnsPromptly \
                     closeAndJoinOnAConnectingEndpointReturnsPromptly; do
                printf '  <testcase name="%s" classname="t"/>\n' "$n"
            done
            echo '</testsuite>'
        } > "$d/TEST-fake.xml"
        "$adb" -s "$ANDROID_SERIAL" uninstall org.openmoq.moq5.test >/dev/null ;;
    :samples:player:assembleDebug)
        for s in player publisher; do
            d="$r/samples/$s/build/outputs/apk/debug"
            mkdir -p "$d" && : > "$d/$s-debug.apk"
        done ;;
    *) exit 1 ;;
esac
exit 0
GRADLE
    chmod +x "$R/tools/gradlew-verified"
}

# A fresh device and tree per case. $1.. are "file:line" device facts.
fresh() {
    rm -rf -- "$R" "$SDK" "$ST"
    mkdir -p "$R/tools" "$R/.native/_topology/arm64-v8a" "$ST" || return 1
    cp "$here/check-android-runtime.sh" "$R/tools/" || return 1
    write_adb || return 1
    write_gradle || return 1
    : > "$ST/packages"; : > "$ST/calls"; : > "$ST/dead"; : > "$ST/refuse"
    : > "$ST/appear"; : > "$ST/crash_launch"
    echo 1758600000 > "$ST/now"
    : > "$ST/crash"
    for fact in "$@"; do
        printf '%s\n' "${fact#*:}" >> "$ST/${fact%%:*}"
    done
}

run_gate() {
    env -u ANDROID_SERIAL -u ANDROID_SDK_ROOT ANDROID_HOME="$SDK" \
        FAKE_ADB_STATE="$ST" FAKE_SDK="$SDK" \
        sh "$R/tools/check-android-runtime.sh" > "$W/out" 2>&1
    printf '%s' "$?"
}

installed_now() { grep -qxF -- "$1" "$ST/packages"; }
calls_match() { grep -qE -- "$1" "$ST/calls"; }
# The uninstall calls THIS gate made, excluding the fake AGP task's own.
gate_uninstalled() {
    awk '/^gradle :moq5-android:connectedDebugAndroidTest/ { agp = 1; next }
         agp && /uninstall org\.openmoq\.moq5\.test$/ { agp = 0; next }
         / uninstall / { print $NF }' "$ST/calls" | sort | tr '\n' ' '
}
row() {  # name, condition-already-evaluated (0 = holds), detail
    if [ "$2" -eq 0 ]; then pass "$1" "$3"; else fail "$1" "$3: $(grep -E '^(FAIL|BLOCKER)\[' "$W/out" | head -3 | tr '\n' ' ')"; fi
}
holds() { if "$@"; then echo 0; else echo 1; fi; }

# --- clean device: the whole gate passes and leaves the device as it found it
fresh
rc=$(run_gate)
row device.clean.passes "$(holds [ "$rc" = 0 ])" "exit $rc on a device holding none of the gate's IDs"
row device.clean.left-as-found "$(holds [ ! -s "$ST/packages" ])" \
    "packages left behind: $(tr '\n' ' ' < "$ST/packages")"
u=$(gate_uninstalled)
row device.clean.uninstalls-own "$(holds [ "$u" = "$PLAYER $PUBLISHER $TEST_PKG " ])" \
    "the gate uninstalled exactly its own installs: [$u]"
row device.clean.buffer-not-cleared "$(holds sh -c "! grep -q 'logcat.* -c' '$ST/calls'")" \
    "the device-global crash buffer was never cleared"
row device.clean.empty-buffer-passes \
    "$(holds grep -q "^PASS\[runtime.sample-nocrash.player\]" "$W/out")" \
    "a readable, empty crash buffer is a crash-free launch"

# --- a pre-existing package with the test ID stops the run before any change
fresh "packages:$TEST_PKG"
rc=$(run_gate)
row device.owned-test-pkg.refused "$(holds grep -q "^FAIL\[runtime.ownership.absent\]: .*$TEST_PKG" "$W/out")" \
    "the preflight names $TEST_PKG (exit $rc)"
row device.owned-test-pkg.nonzero "$(holds [ "$rc" != 0 ])" "exit $rc"
row device.owned-test-pkg.untouched "$(holds installed_now "$TEST_PKG")" \
    "the pre-existing $TEST_PKG is still installed"
row device.owned-test-pkg.nothing-done \
    "$(holds sh -c "! grep -qE '(^| )(install|uninstall) |^gradle ' '$ST/calls'")" \
    "no build, install or uninstall ran: $(grep -E '(^| )(install|uninstall) |^gradle ' "$ST/calls" | head -3 | tr '\n' ' ')"

# --- the same for a sample ID, which only the sample phase installs
fresh "packages:$PUBLISHER"
rc=$(run_gate)
row device.owned-sample.refused "$(holds grep -q "^FAIL\[runtime.ownership.absent\]: .*$PUBLISHER" "$W/out")" \
    "the preflight names $PUBLISHER (exit $rc)"
row device.owned-sample.untouched "$(holds installed_now "$PUBLISHER")" \
    "the pre-existing $PUBLISHER is still installed"
row device.owned-sample.nothing-done \
    "$(holds sh -c "! grep -qE '(^| )(install|uninstall) |^gradle ' '$ST/calls'")" \
    "no build, install or uninstall ran"

# --- a listing that cannot be read is not proof of absence
fresh "no_pm:1"
rc=$(run_gate)
row device.unlisted.refused "$(holds grep -q '^FAIL\[runtime.ownership.absent\]' "$W/out")" \
    "an unreadable package listing fails the preflight (exit $rc)"
row device.unlisted.nothing-done \
    "$(holds sh -c "! grep -qE '(^| )(install|uninstall) |^gradle ' '$ST/calls'")" \
    "no build, install or uninstall ran"

# --- cleanup never removes a package whose install was refused
fresh "refuse:$PLAYER"
rc=$(run_gate)
u=$(gate_uninstalled)
row device.refused-install.not-uninstalled "$(holds [ "$u" = "$PUBLISHER $TEST_PKG " ])" \
    "the gate uninstalled only what it installed: [$u]"

# --- nor one that appeared from elsewhere after the preflight: the gate's own
#     install must not replace it, and cleanup must not remove it. (A sample ID:
#     the test ID is also installed by AGP, whose replace install the gate does
#     not control.)
fresh "appear:$PLAYER"
rc=$(run_gate)
row device.foreign-arrival.not-replaced \
    "$(holds grep -q "^FAIL\[runtime.sample-install.player\]" "$W/out")" \
    "installing over a package that is now present is refused (exit $rc)"
row device.foreign-arrival.kept "$(holds installed_now "$PLAYER")" \
    "the package that arrived from elsewhere is still installed"
u=$(gate_uninstalled)
row device.foreign-arrival.not-uninstalled "$(holds [ "$u" = "$PUBLISHER $TEST_PKG " ])" \
    "the gate uninstalled only what it installed: [$u]"

# --- an OLDER crash in the buffer neither fails the row nor is erased
OLD="1758500000.100  1111  1111 E AndroidRuntime: Process: $PUBLISHER, PID: 1111"
fresh "crash:$OLD"
rc=$(run_gate)
row device.old-crash.passes "$(holds grep -q "^PASS\[runtime.sample-nocrash.publisher\]" "$W/out")" \
    "a crash logged before this launch does not fail the row (exit $rc)"
row device.old-crash.kept "$(holds grep -qF -- "$OLD" "$ST/crash")" \
    "the older entry is still in the device crash buffer"

# --- a NEW crash of the launched package fails the row by name, with an older
#     crash of a different package still present and still not blamed
fresh "crash_launch:$PLAYER" "crash:$OLD"
rc=$(run_gate)
row device.new-crash.fails "$(holds grep -q "^FAIL\[runtime.sample-nocrash.player\]" "$W/out")" \
    "a crash of $PLAYER after its launch fails the row (exit $rc)"
row device.new-crash.other-clean "$(holds grep -q "^PASS\[runtime.sample-nocrash.publisher\]" "$W/out")" \
    "the publisher's row is judged on its own launch"
row device.new-crash.nonzero "$(holds [ "$rc" != 0 ])" "exit $rc"

# --- a crash read that exits 0 with text the gate cannot parse is not a clean
#     buffer: fail closed, by name
fresh "garbled:1"
rc=$(run_gate)
row device.garbled-buffer.fails \
    "$(holds grep -q "^FAIL\[runtime.sample-nocrash.player\]: the crash buffer returned a line" "$W/out")" \
    "unparseable crash output fails the row (exit $rc)"
row device.garbled-buffer.nonzero "$(holds [ "$rc" != 0 ])" "exit $rc"

# --- a crash read that exits 0 but reports a diagnostic on stderr is not a
#     clean read either: fail by name, and keep the diagnostic as evidence
fresh "noisy:1"
rc=$(run_gate)
row device.noisy-read.fails \
    "$(holds grep -q "^FAIL\[runtime.sample-nocrash.player\]: the crash read succeeded but wrote diagnostics: logcat: buffer read was truncated" "$W/out")" \
    "a successful crash read with stderr output fails the row (exit $rc)"
row device.noisy-read.nonzero "$(holds [ "$rc" != 0 ])" "exit $rc"

# --- without a launch mark there is no causal observation: fail closed
fresh "no_date:1"
rc=$(run_gate)
row device.no-mark.fails "$(holds grep -q "^FAIL\[runtime.sample-nocrash.player\]" "$W/out")" \
    "no device clock, no crash verdict (exit $rc)"

printf '\n%s: %d PASS / %d FAIL\n' "$_self" "$passes" "$fails"
[ "$fails" -eq 0 ] || exit 1
exit 0
