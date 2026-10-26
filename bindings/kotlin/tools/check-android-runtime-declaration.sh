#!/bin/sh
# The DEVICE-FREE half of the Android runtime acceptance contract.
#
# tools/check-android-runtime.sh needs a device. A CI host, a review checkout
# and every other gate in this tree do not have one, so the substrate that gate
# depends on -- the instrumentation source set, the runner declaration, the
# pinned AndroidX Test artifacts, and the named observations -- would be
# deletable without anything noticing. This gate is what notices. It reads the
# tree only, so it belongs in the canonical `check`, and it is deliberately NOT
# a second copy of the device gate: it asserts that the runtime proof CAN be
# made, never that it was.
#
# Two rules govern how it reads:
#
#   * Kotlin and Gradle sources are judged as CODE. Comments and string
#     literals are removed first, because an oracle a file can satisfy by
#     describing itself is not an oracle. The one deliberate exception is the
#     runner row, whose subject IS a string literal; it is matched on the
#     assignment, not on a loose occurrence of the class name.
#
#   * The instrumentation must reach native code the way the PRODUCT does.
#     A test that loaded the object from a file path, or called
#     System.load/loadLibrary itself, would prove packaging works for tests and
#     nothing else, so both are refused by name.
set -u
LC_ALL=C; export LC_ALL

_self=${0##*/}
case $0 in
    */*) _selfdir=${0%/*} ;;
    *)   _selfdir=. ;;
esac
here=$(cd "$_selfdir" && pwd) || { echo "$_self: cannot resolve the script directory" >&2; exit 2; }
root=$(cd "$here/.." && pwd)  || { echo "$_self: cannot resolve bindings/kotlin" >&2; exit 2; }

for t in find grep printf sed awk sort mktemp rm; do
    command -v "$t" >/dev/null 2>&1 \
        || { echo "$_self: required tool not found: $t" >&2; exit 2; }
done

scratch=$(mktemp -d) || { echo "$_self: cannot create a scratch directory" >&2; exit 2; }
cleanup() { rm -rf -- "$scratch"; }
trap 'cleanup; exit 130' HUP INT TERM
trap cleanup EXIT

fails=0
passes=0
pass() { passes=$((passes + 1)); printf 'PASS[%s]: %s\n' "$1" "$2"; }
fail() { fails=$((fails + 1)); printf 'FAIL[%s]: %s\n' "$1" "$2"; }

module="$root/moq5-android"
androidtest="$module/src/androidTest"
sources="$androidtest/kotlin"
buildscript="$module/build.gradle.kts"
props="$root/gradle/toolchain.properties"
rootscript="$root/build.gradle.kts"
devicegate="$here/check-android-runtime.sh"

# --- code, not prose -------------------------------------------------------
# Every row below that judges Kotlin or Kotlin-DSL reads it through the shared
# stripper in tools/lib-kotlin-code.sh, which removes comments and string
# literals with a real state machine. A quote-toggling stripper is not merely
# imprecise here: `pin("...")` inside a template inverts its idea of "inside a
# string" for the rest of the file, so every later row silently reads the
# INVERSE of the code. It emits one output line per input line, which is what
# lets a row pair a raw line with its code form.
. "$here/lib-kotlin-code.sh"

# The whole instrumentation source set as one code stream.
testcode="$scratch/androidtest.code"
: > "$testcode"
if [ -d "$sources" ]; then
    find "$sources" -name '*.kt' -type f | sort | while IFS= read -r f; do
        kotlin_code "$f" >> "$testcode"
    done
fi

buildcode="$scratch/build.code"
[ -f "$buildscript" ] && kotlin_code "$buildscript" > "$buildcode" || : > "$buildcode"

rootcode="$scratch/root.code"
[ -f "$rootscript" ] && kotlin_code "$rootscript" > "$rootcode" || : > "$rootcode"

# --- TOPOLOGY --------------------------------------------------------------
if [ -d "$sources" ]; then
    pass runtime.decl.sourceset "moq5-android/src/androidTest/kotlin exists"
else
    fail runtime.decl.sourceset \
        "no instrumentation source set at moq5-android/src/androidTest/kotlin; without one AGP builds no androidTest APK and connectedDebugAndroidTest is a no-op"
fi

kt_count=$(find "$sources" -name '*.kt' -type f 2>/dev/null | grep -c . || true)
if [ "$kt_count" -gt 0 ]; then
    pass runtime.decl.tests-present "$kt_count Kotlin instrumentation source file(s)"
else
    fail runtime.decl.tests-present "the instrumentation source set declares no Kotlin source"
fi

if grep -q '@Test' "$testcode"; then
    pass runtime.decl.tests-annotated "the instrumentation source declares at least one @Test"
else
    fail runtime.decl.tests-annotated \
        "no @Test in the instrumentation source; a source set with no test executes nothing"
fi

if [ -f "$androidtest/AndroidManifest.xml" ] \
   && grep -q 'android.permission.INTERNET' "$androidtest/AndroidManifest.xml"; then
    pass runtime.decl.test-manifest "the instrumentation manifest declares INTERNET"
else
    fail runtime.decl.test-manifest \
        "moq5-android/src/androidTest/AndroidManifest.xml does not declare android.permission.INTERNET; an endpoint is a network client and the library's own manifest must not force the permission on consumers"
fi

# --- RUNNER ----------------------------------------------------------------
# Two halves, because neither alone is the fact. The ASSIGNMENT must exist in
# code -- a commented-out runner configures nothing -- and its VALUE is a string
# literal, which by construction the stripper removes, so the exact class name
# is matched against the raw script.
if grep -Eq '^[[:space:]]*testInstrumentationRunner[[:space:]]*=' "$buildcode" \
   && grep -Eq '^[[:space:]]*testInstrumentationRunner[[:space:]]*=[[:space:]]*"androidx\.test\.runner\.AndroidJUnitRunner"[[:space:]]*$' "$buildscript"; then
    pass runtime.decl.runner "defaultConfig assigns the AndroidJUnitRunner instrumentation"
else
    fail runtime.decl.runner \
        "moq5-android/build.gradle.kts does not assign testInstrumentationRunner = \"androidx.test.runner.AndroidJUnitRunner\" as code; without it AGP builds no androidTest APK"
fi

# --- DEPENDENCIES ----------------------------------------------------------
# Two facts, separately: the artifacts are declared on the INSTRUMENTATION
# configuration (so they can never reach the published AAR), and their versions
# come from the pin file rather than a literal that could drift from it.
for spec in 'androidx.test:runner:androidx.test.runner.version' \
            'androidx.test.ext:junit:androidx.test.junit.version'
do
    group=${spec%%:*}
    rest=${spec#*:}
    artifact=${rest%%:*}
    pinname=${rest#*:}
    coord="$group:$artifact"

    if grep -Fq "androidTestImplementation(\"$coord:\${pin(" "$buildscript"; then
        pass "runtime.decl.dependency.$artifact" \
            "$coord is declared on androidTestImplementation with a pinned version"
    elif grep -Fq "$coord" "$buildcode"; then
        fail "runtime.decl.dependency.$artifact" \
            "$coord is named in the build script but not as androidTestImplementation(\"$coord:\${pin(...)}\"); an instrumentation artifact must not reach another configuration and its version must not be a literal"
    else
        fail "runtime.decl.dependency.$artifact" "$coord is not declared"
    fi

    n=$(grep -c "^${pinname}=" "$props" || true)
    v=$(sed -n "s/^${pinname}=//p" "$props" | head -1)
    if [ "$n" -eq 1 ] && printf '%s' "$v" | grep -Eq '^[0-9]+(\.[0-9]+)*$'; then
        pass "runtime.decl.pin.$pinname" "$pinname=$v"
    elif [ "$n" -eq 0 ]; then
        fail "runtime.decl.pin.$pinname" "gradle/toolchain.properties declares no '$pinname'"
    elif [ "$n" -gt 1 ]; then
        fail "runtime.decl.pin.$pinname" \
            "'$pinname' is declared $n times; property loading is last-wins, so a duplicate silently changes the effective version"
    else
        fail "runtime.decl.pin.$pinname" "'$pinname' is not a version: '$v'"
    fi
done

# The instrumentation artifacts must never appear on a configuration a consumer
# resolves. The coordinates live inside string literals, so the row pairs each
# RAW line naming one with its CODE form at the same line number: a line whose
# code form is empty is a comment and declares nothing, and any other line must
# be an androidTestImplementation declaration.
leaked=$(awk 'NR == FNR { code[FNR] = $0; next }
    /androidx\.test/ {
        c = code[FNR]
        sub(/^[[:space:]]+/, "", c)
        if (c == "") next
        if (c ~ /^androidTestImplementation\(/) next
        # The runner assignment names the AndroidJUnitRunner class, not a
        # dependency coordinate. It is the instrumentation declaration this
        # gate REQUIRES above, so it is not a leak.
        if (c ~ /^testInstrumentationRunner[[:space:]]*=/) next
        printf "%d: %s\n", FNR, c
    }' "$buildcode" "$buildscript")
if [ -z "$leaked" ]; then
    pass runtime.decl.instrumentation-only \
        "every androidx.test coordinate in the build script is an androidTestImplementation declaration"
else
    fail runtime.decl.instrumentation-only \
        "an androidx.test coordinate is declared on a configuration a consumer resolves: $(printf '%s' "$leaked" | tr '\n' ' ')"
fi

# --- THE PACKAGED LOAD ROUTE ----------------------------------------------
if grep -q 'NativeBridgeOwner' "$testcode"; then
    pass runtime.decl.packaged-owner \
        "the instrumentation reaches the bridge through NativeBridgeOwner, the product's only load entry point"
else
    fail runtime.decl.packaged-owner \
        "the instrumentation never names NativeBridgeOwner; a runtime proof that does not use the product's load entry point proves nothing about the product"
fi

banned=$(grep -nE 'System\.load\(|System\.loadLibrary\(|NativeImage\(' "$testcode" || true)
if [ -z "$banned" ]; then
    pass runtime.decl.no-adhoc-load \
        "the instrumentation performs no System.load, System.loadLibrary or NativeImage construction of its own"
else
    fail runtime.decl.no-adhoc-load \
        "the instrumentation loads an object itself, which proves nothing about Android packaging: $(printf '%s' "$banned" | tr '\n' ' ')"
fi

if grep -q 'nativeLibraryDir' "$testcode"; then
    pass runtime.decl.packaging-asserted \
        "the instrumentation locates the object in the application's own nativeLibraryDir"
else
    fail runtime.decl.packaging-asserted \
        "no instrumentation test inspects applicationInfo.nativeLibraryDir; without it a successful load says something loaded but not that it came from Android packaging"
fi

for authority in ABI_EPOCH FEATURES_REQUIRED; do
    if grep -q "NativeBridge\.$authority" "$testcode"; then
        pass "runtime.decl.handshake.$authority" \
            "the handshake is compared against NativeBridge.$authority"
    else
        fail "runtime.decl.handshake.$authority" \
            "the instrumentation never compares the observed handshake with NativeBridge.$authority, so it compares the device with itself"
    fi
done

if grep -q 'getMainLooper' "$testcode"; then
    pass runtime.decl.mainthread-asserted \
        "an instrumentation test judges the endpoint path against the main looper"
else
    fail runtime.decl.mainthread-asserted \
        "no instrumentation test names getMainLooper, so nothing judges the main-thread claim"
fi

if grep -q 'MoqEndpoint' "$testcode"; then
    pass runtime.decl.public-endpoint \
        "the instrumentation exercises the PUBLIC MoqEndpoint surface"
else
    fail runtime.decl.public-endpoint \
        "the instrumentation never names MoqEndpoint; the endpoint proof must run the public surface, not an internal backend"
fi

# --- THE NAMED OBSERVATIONS ------------------------------------------------
# The device gate judges a named set of observations. If a name drifts on one
# side only, the device gate reports an absent observation, which reads as a
# device problem. Comparing the two sets here makes it a declaration failure
# instead. The list below IS the pinned set; no count is published anywhere.
for named in packagedImageResolvesFromTheApplicationLibraryDirectory \
             bridgeHandshakeAgreesWithTheCompiledAuthority \
             endpointConstructAndCloseCompletesOffTheMainThread \
             workThatNeedsTheMainLooperIsClassifiedAsDependent \
             aWorkerThatSurvivesCleanupIsRefusedAsAHarnessFailure \
             snapshotOnAnIdleEndpointReturnsPromptly \
             closeAndJoinOnAConnectingEndpointReturnsPromptly \
             publicOpenInAFreshProcessNeedsNoExplicitInitialization
do
    in_tests=no; in_gate=no
    grep -q "fun $named(" "$testcode" && in_tests=yes
    grep -Fq "$named" "$devicegate" && in_gate=yes
    if [ "$in_tests" = yes ] && [ "$in_gate" = yes ]; then
        pass "runtime.decl.observation.$named" "declared as a test and named by the device gate"
    elif [ "$in_tests" = no ]; then
        fail "runtime.decl.observation.$named" \
            "no instrumentation function of that name exists, but tools/check-android-runtime.sh requires the observation"
    else
        fail "runtime.decl.observation.$named" \
            "the test exists but tools/check-android-runtime.sh does not name it, so its deletion would go unreported"
    fi
done

# --- THE FRESH PROCESS -----------------------------------------------------
# The fresh-process test is only evidence if it never initializes the bridge
# itself. Judged on that file's CODE alone: an explicit initialize call in a
# comment is prose, in code it is the test doing the product's job.
fresh="$sources/org/openmoq/moq5/runtime/FreshProcessPublicOpenTest.kt"
if [ -f "$fresh" ]; then
    freshcode="$scratch/fresh.code"
    kotlin_code "$fresh" > "$freshcode"
    if grep -q 'MoqEndpoint' "$freshcode" && ! grep -q 'NativeBridgeOwner\.initialize\|BridgeInitializer\|System\.load' "$freshcode"; then
        pass runtime.decl.fresh-no-explicit-init \
            "FreshProcessPublicOpenTest opens the public endpoint and performs no initialization of its own"
    else
        fail runtime.decl.fresh-no-explicit-init \
            "FreshProcessPublicOpenTest must open MoqEndpoint and must not call NativeBridgeOwner.initialize, a BridgeInitializer or System.load; a test that initializes for the product proves nothing about a fresh process"
    fi
else
    fail runtime.decl.fresh-no-explicit-init "no FreshProcessPublicOpenTest.kt in the instrumentation source set"
fi

# --- THE MAIN-LOOPER ORACLE ------------------------------------------------
# The oracle is a shared helper with a closed verdict, and the endpoint row
# must accept ONLY the independent verdict while the control row must expect
# the dependent one. Rows judged over code, by the verdict names.
if grep -q 'object MainLooperHold' "$testcode" \
   && grep -q 'Independent' "$testcode" && grep -q 'MainThreadDependent' "$testcode"; then
    pass runtime.decl.oracle-helper "MainLooperHold declares a closed verdict with independent and dependent cases"
else
    fail runtime.decl.oracle-helper \
        "no MainLooperHold helper with Independent and MainThreadDependent verdicts in the instrumentation source"
fi

if grep -q 'MainLooperHold\.run' "$testcode"; then
    pass runtime.decl.oracle-used "the acceptance tests judge main-thread independence through MainLooperHold.run"
else
    fail runtime.decl.oracle-used "no test calls MainLooperHold.run; the main-thread claim has no oracle"
fi

# --- THE DEVICE GATE, AND THIS ONE'S PLACE IN THE BOUNDARY ------------------
if [ -s "$devicegate" ]; then
    pass runtime.decl.device-gate "tools/check-android-runtime.sh exists"
else
    fail runtime.decl.device-gate "tools/check-android-runtime.sh is missing or empty"
fi

# The device gate must NOT be wired into the canonical check: `check` runs on
# hosts with no device, and a gate that needs one would either fail there or be
# made skippable, which is the silent skip this slice exists to prevent.
#
# The path is a string literal, so it is matched in the RAW script and the same
# line's code form must be non-empty for it to count as wiring. The trailing
# quote is part of the match: `check-android-runtime-declaration.sh` shares a
# prefix with the device gate, and it is exactly what SHOULD be wired.
wired=$(awk 'NR == FNR { code[FNR] = $0; next }
    {
        c = code[FNR]
        sub(/^[[:space:]]+/, "", c)
        if (c != "" && index($0, "check-android-runtime.sh\"") > 0) printf "%d: %s\n", FNR, c
    }' "$rootcode" "$rootscript")
if [ -z "$wired" ]; then
    pass runtime.decl.device-gate-unwired \
        "the device gate is not reachable from the canonical check"
else
    fail runtime.decl.device-gate-unwired \
        "the root build script wires check-android-runtime.sh; the DEVICE gate must stay out of the canonical check, which runs where no device exists: $(printf '%s' "$wired" | tr '\n' ' ')"
fi

# Membership is code STRUCTURE, but a task name is a string LITERAL, so the row
# pairs the two streams by line number: the name is matched in the raw script,
# and the same line's code form must be non-empty -- a commented-out entry
# configures nothing and must not satisfy the boundary.
in_list() {
    awk -v want="$1" -v task="$2" '
        NR == FNR { code[FNR] = $0; next }
        {
            c = code[FNR]
            sub(/^[[:space:]]+/, "", c)
            if ($0 ~ ("val " want "[[:space:]]*=")) { inlist = 1; next }
            if (inlist && c ~ /^\)/) { inlist = 0; next }
            if (inlist && c != "" && index($0, "\"" task "\"") > 0) found = 1
        }
        END { exit(found ? 0 : 1) }' "$rootcode" "$rootscript"
}

for listname in expectedVerificationTasks requiredVerificationTasks; do
    for task in verifyAndroidRuntimeDeclaration verifyAndroidRuntimeDeclarationSelfTest; do
        if in_list "$listname" "$task"; then
            pass "runtime.decl.boundary.$listname.$task" "$task is a member of $listname"
        else
            fail "runtime.decl.boundary.$listname.$task" \
                "$task is not a member of $listname, so the canonical check does not run this contract"
        fi
    done
done

printf '\n%s: %d PASS / %d FAIL\n' "$_self" "$passes" "$fails"
[ "$fails" -eq 0 ] || exit 1
exit 0
