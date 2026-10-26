#!/bin/sh
# The runtime declaration gate's own ledger.
#
# The gate reads only the tree, so on a correct tree every one of its rows
# passes -- which is exactly the state in which a row can be passing
# VACUOUSLY. Three of its rows were vacuous when first written: the runner row
# matched a comment, the dependency rows matched text the stripper had already
# removed, and the device-gate-unwired row searched the code stream for a path
# that only ever exists as a string literal, so it would have passed while the
# device gate WAS wired.
#
# So this builds a disposable fixture the gate accepts completely, then breaks
# one fact at a time and requires the matching row to fail BY NAME. Several
# cases break a fact only inside a COMMENT or a string literal and require the
# gate to keep passing: a stripper that regressed to a quote toggle would go
# green on the kills and red on those, and both directions are checked here.
set -u
LC_ALL=C; export LC_ALL

_self=${0##*/}
case $0 in
    */*) _selfdir=${0%/*} ;;
    *)   _selfdir=. ;;
esac
here=$(cd "$_selfdir" && pwd) || { echo "$_self: cannot resolve the script directory" >&2; exit 2; }
root=$(cd "$here/.." && pwd)  || { echo "$_self: cannot resolve bindings/kotlin" >&2; exit 2; }

for t in mktemp rm cp mkdir sed grep printf cat awk; do
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

R="$W/root"
TESTS="$R/moq5-android/src/androidTest/kotlin/org/openmoq/moq5/runtime"
BUILD="$R/moq5-android/build.gradle.kts"
PROPS="$R/gradle/toolchain.properties"
ROOTBUILD="$R/build.gradle.kts"
DEVICEGATE="$R/tools/check-android-runtime.sh"
TESTFILE="$TESTS/AndroidRuntimeAcceptanceTest.kt"

# The fixture is a MINIMAL mirror, written from scratch rather than copied from
# the product. Copying would make every case a statement about today's sources:
# a mutator that failed to apply would still be reported as a kill because the
# real file already satisfied the row. Written here, each fact is present for
# exactly one reason and its removal is unambiguous.
#
# The two scripts under test ARE copied, because they are the subject.
build_fixture() {
    rm -rf -- "$R"
    mkdir -p "$TESTS" "$R/tools" "$R/gradle" "$R/moq5-android/src/androidTest" || return 1

    cp "$here/check-android-runtime-declaration.sh" "$R/tools/" || return 1
    cp "$here/lib-kotlin-code.sh" "$R/tools/" || return 1
    printf '#!/bin/sh\n# a device gate naming its observations\n' > "$DEVICEGATE" || return 1
    printf 'packagedImageResolvesFromTheApplicationLibraryDirectory\n' >> "$DEVICEGATE"
    printf 'bridgeHandshakeAgreesWithTheCompiledAuthority\n' >> "$DEVICEGATE"
    printf 'endpointConstructAndCloseCompletesOffTheMainThread\n' >> "$DEVICEGATE"
    printf 'workThatNeedsTheMainLooperIsClassifiedAsDependent\n' >> "$DEVICEGATE"
    printf 'aWorkerThatSurvivesCleanupIsRefusedAsAHarnessFailure\n' >> "$DEVICEGATE"
    printf 'snapshotOnAnIdleEndpointReturnsPromptly\n' >> "$DEVICEGATE"
    printf 'closeAndJoinOnAConnectingEndpointReturnsPromptly\n' >> "$DEVICEGATE"
    printf 'publicOpenInAFreshProcessNeedsNoExplicitInitialization\n' >> "$DEVICEGATE"

    cat > "$R/moq5-android/src/androidTest/AndroidManifest.xml" <<'XML'
<?xml version="1.0" encoding="utf-8"?>
<manifest xmlns:android="http://schemas.android.com/apk/res/android">
    <uses-permission android:name="android.permission.INTERNET" />
</manifest>
XML

    cat > "$BUILD" <<'KTS'
plugins {
    id("com.android.library")
}

fun pin(name: String): String = name

android {
    defaultConfig {
        minSdk = 21
        testInstrumentationRunner = "androidx.test.runner.AndroidJUnitRunner"
    }
}

dependencies {
    androidTestImplementation("androidx.test:runner:${pin("androidx.test.runner.version")}")
    androidTestImplementation("androidx.test.ext:junit:${pin("androidx.test.junit.version")}")
}
KTS

    cat > "$PROPS" <<'PROPS'
androidx.test.runner.version=1.7.0
androidx.test.junit.version=1.3.0
PROPS

    cat > "$ROOTBUILD" <<'KTS'
val expectedVerificationTasks =
    setOf(
        "verifyAndroidRuntimeDeclaration",
        "verifyAndroidRuntimeDeclarationSelfTest",
    )

val requiredVerificationTasks =
    listOf(
        "verifyAndroidRuntimeDeclaration",
        "verifyAndroidRuntimeDeclarationSelfTest",
    )

tasks.register<Exec>("verifyAndroidRuntimeDeclaration") {
    commandLine("/bin/sh", file("tools/check-android-runtime-declaration.sh").absolutePath)
}
KTS

    cat > "$TESTFILE" <<'KT'
package org.openmoq.moq5.runtime

class AndroidRuntimeAcceptanceTest {

    @Test
    fun packagedImageResolvesFromTheApplicationLibraryDirectory() {
        val dir = context.applicationInfo.nativeLibraryDir
        val outcome = NativeBridgeOwner.initialize()
    }

    @Test
    fun bridgeHandshakeAgreesWithTheCompiledAuthority() {
        val outcome = NativeBridgeOwner.initialize()
        assertEquals(NativeBridge.ABI_EPOCH, ready.abiEpoch)
        assertEquals(NativeBridge.FEATURES_REQUIRED, ready.features)
    }

    @Test
    fun endpointConstructAndCloseCompletesOffTheMainThread() {
        val mainLooper = Looper.getMainLooper()
        val verdict = MainLooperHold.run(1, 2, 3) {
            val endpoint = MoqEndpoint.open(EndpointConfig(url = loopbackUrl))
            endpoint.closeAndJoin()
        }
        assertTrue(verdict is MainLooperHold.Verdict.Independent)
    }

    @Test
    fun workThatNeedsTheMainLooperIsClassifiedAsDependent() {
        val verdict = MainLooperHold.run(1, 2, 3) { }
        assertTrue(verdict is MainLooperHold.Verdict.MainThreadDependent)
    }

    @Test
    fun aWorkerThatSurvivesCleanupIsRefusedAsAHarnessFailure() {
        val verdict = MainLooperHold.run(1, 2, 3) { }
        assertTrue(verdict is MainLooperHold.Verdict.MainThreadDependent)
    }

    @Test
    fun snapshotOnAnIdleEndpointReturnsPromptly() {
        val endpoint = MoqEndpoint.open(EndpointConfig(url = loopbackUrl))
        endpoint.closeAndJoin()
    }

    @Test
    fun closeAndJoinOnAConnectingEndpointReturnsPromptly() {
        val endpoint = MoqEndpoint.open(EndpointConfig(url = loopbackUrl))
        endpoint.closeAndJoin()
    }
}
KT

    cat > "$TESTS/MainLooperHold.kt" <<'KT'
package org.openmoq.moq5.runtime

internal object MainLooperHold {
    sealed interface Verdict<out T> {
        data class Independent<T>(val value: T) : Verdict<T>
        data class MainThreadDependent(val workDeadlineMillis: Long) : Verdict<Nothing>
    }
    fun <T> run(a: Long, b: Long, c: Long, work: () -> T): Verdict<T> = Verdict.Independent(work())
}
KT

    cat > "$TESTS/FreshProcessPublicOpenTest.kt" <<'KT'
package org.openmoq.moq5.runtime

class FreshProcessPublicOpenTest {
    @Test
    fun publicOpenInAFreshProcessNeedsNoExplicitInitialization() {
        // the product must load the bridge; NativeBridgeOwner.initialize() is not called here
        val endpoint = MoqEndpoint.open(EndpointConfig(url = loopbackUrl))
        endpoint.closeAndJoin()
    }
}
KT
    return 0
}

run_gate() { sh "$R/tools/check-android-runtime-declaration.sh" > "$W/out" 2>&1; printf '%s' "$?"; }

case_is() {
    _label=$1; _want=$2; _row=$3
    shift 3
    build_fixture || { fail "$_label" "the fixture could not be built"; return; }
    if [ "$#" -gt 0 ]; then "$@" || { fail "$_label" "the mutator failed"; return; }; fi
    _rc=$(run_gate)
    if [ "$_rc" != "$_want" ]; then
        fail "$_label" "expected exit $_want, got $_rc; output:
$(cat "$W/out")"
        return
    fi
    if [ -n "$_row" ]; then
        if grep -q "^FAIL\[$_row\]" "$W/out"; then
            pass "$_label" "the gate fails by name: $_row"
        else
            fail "$_label" "exit $_rc but no FAIL[$_row] row; output:
$(cat "$W/out")"
        fi
    else
        pass "$_label" "the gate accepts the fixture"
    fi
}

# The positive control FIRST. Without it every kill below could be an artefact
# of a fixture the gate never accepts in the first place.
case_is selftest.accepts 0 ''

# --- topology --------------------------------------------------------------
no_sourceset()  { rm -rf -- "$R/moq5-android/src/androidTest/kotlin"; }
no_tests()      { rm -f "$TESTS"/*.kt; }
no_annotation() { for f in "$TESTS"/*.kt; do sed -i.bak 's/@Test//' "$f" && rm -f "$f.bak"; done; }
no_manifest()   { rm -f "$R/moq5-android/src/androidTest/AndroidManifest.xml"; }
no_internet()   { sed -i.bak '/INTERNET/d' "$R/moq5-android/src/androidTest/AndroidManifest.xml" \
                  && rm -f "$R/moq5-android/src/androidTest/AndroidManifest.xml.bak"; }

case_is selftest.sourceset    1 runtime.decl.sourceset       no_sourceset
case_is selftest.tests        1 runtime.decl.tests-present   no_tests
case_is selftest.annotation   1 runtime.decl.tests-annotated no_annotation
case_is selftest.manifest     1 runtime.decl.test-manifest   no_manifest
case_is selftest.internet     1 runtime.decl.test-manifest   no_internet

# --- runner ----------------------------------------------------------------
no_runner()      { sed -i.bak '/testInstrumentationRunner/d' "$BUILD" && rm -f "$BUILD.bak"; }
runner_wrong()   { sed -i.bak 's/AndroidJUnitRunner/SomeOtherRunner/' "$BUILD" && rm -f "$BUILD.bak"; }
# The assignment survives only as PROSE. A gate that read raw text would still
# see the exact line and pass while AGP built no androidTest APK at all.
runner_comment() { sed -i.bak 's|^\( *\)testInstrumentationRunner|\1// testInstrumentationRunner|' "$BUILD" \
                   && rm -f "$BUILD.bak"; }

case_is selftest.runner-absent  1 runtime.decl.runner no_runner
case_is selftest.runner-wrong   1 runtime.decl.runner runner_wrong
case_is selftest.runner-comment 1 runtime.decl.runner runner_comment

# --- dependencies ----------------------------------------------------------
dep_wrong_config() { sed -i.bak 's/androidTestImplementation("androidx.test:runner/implementation("androidx.test:runner/' "$BUILD" \
                     && rm -f "$BUILD.bak"; }
dep_absent()       { sed -i.bak '/androidx.test.ext:junit/d' "$BUILD" && rm -f "$BUILD.bak"; }
dep_literal()      { sed -i.bak 's|androidx.test.ext:junit:${pin("androidx.test.junit.version")}|androidx.test.ext:junit:1.3.0|' "$BUILD" \
                     && rm -f "$BUILD.bak"; }
pin_absent()       { sed -i.bak '/^androidx.test.runner.version=/d' "$PROPS" && rm -f "$PROPS.bak"; }
pin_duplicate()    { printf 'androidx.test.junit.version=1.2.0\n' >> "$PROPS"; }
pin_not_version()  { sed -i.bak 's/^androidx.test.runner.version=.*/androidx.test.runner.version=latest/' "$PROPS" \
                     && rm -f "$PROPS.bak"; }
dep_leaks()        { printf 'dependencies { testImplementation("androidx.test:core:1.7.0") }\n' >> "$BUILD"; }

case_is selftest.dep-config   1 runtime.decl.dependency.runner   dep_wrong_config
case_is selftest.dep-absent   1 runtime.decl.dependency.junit    dep_absent
case_is selftest.dep-literal  1 runtime.decl.dependency.junit    dep_literal
case_is selftest.pin-absent   1 runtime.decl.pin.androidx.test.runner.version pin_absent
case_is selftest.pin-dup      1 runtime.decl.pin.androidx.test.junit.version  pin_duplicate
case_is selftest.pin-grammar  1 runtime.decl.pin.androidx.test.runner.version pin_not_version
case_is selftest.dep-leak     1 runtime.decl.instrumentation-only dep_leaks

# A coordinate named only in PROSE leaks nothing. This is the direction a
# stricter-looking raw-text gate gets wrong, and it must stay green.
dep_leak_comment() { printf '// testImplementation("androidx.test:core:1.7.0") would leak\n' >> "$BUILD"; }
case_is selftest.dep-leak-comment 0 '' dep_leak_comment

# --- the packaged load route ----------------------------------------------
no_owner()      { sed -i.bak 's/NativeBridgeOwner/SomethingElse/g' "$TESTFILE" && rm -f "$TESTFILE.bak"; }
adhoc_load()    { printf 'val x = System.loadLibrary("moq5_jni")\n' >> "$TESTFILE"; }
adhoc_image()   { printf 'val y = NativeImage("/tmp/libmoq5_jni.so")\n' >> "$TESTFILE"; }
no_libdir()     { sed -i.bak '/nativeLibraryDir/d' "$TESTFILE" && rm -f "$TESTFILE.bak"; }
no_epoch()      { sed -i.bak '/NativeBridge.ABI_EPOCH/d' "$TESTFILE" && rm -f "$TESTFILE.bak"; }
no_features()   { sed -i.bak '/NativeBridge.FEATURES_REQUIRED/d' "$TESTFILE" && rm -f "$TESTFILE.bak"; }
no_looper()     { sed -i.bak '/getMainLooper/d' "$TESTFILE" && rm -f "$TESTFILE.bak"; }
no_endpoint()   { for f in "$TESTS"/*.kt; do sed -i.bak 's/MoqEndpoint/PrivateBackend/g' "$f" && rm -f "$f.bak"; done; }

case_is selftest.owner       1 runtime.decl.packaged-owner      no_owner
case_is selftest.adhoc-load  1 runtime.decl.no-adhoc-load       adhoc_load
case_is selftest.adhoc-image 1 runtime.decl.no-adhoc-load       adhoc_image
case_is selftest.libdir      1 runtime.decl.packaging-asserted  no_libdir
case_is selftest.epoch       1 runtime.decl.handshake.ABI_EPOCH        no_epoch
case_is selftest.features    1 runtime.decl.handshake.FEATURES_REQUIRED no_features
case_is selftest.looper      1 runtime.decl.mainthread-asserted no_looper
case_is selftest.endpoint    1 runtime.decl.public-endpoint     no_endpoint

# A load that exists only in a comment or inside a string is not a load. Both
# must stay green, and both are shapes a raw-text gate reports as a violation.
load_in_comment() { printf '// System.loadLibrary("moq5_jni") is what the product does\n' >> "$TESTFILE"; }
load_in_string()  { printf 'val msg = "call System.loadLibrary( yourself and this proves nothing"\n' >> "$TESTFILE"; }
case_is selftest.load-comment 0 '' load_in_comment
case_is selftest.load-string  0 '' load_in_string

# --- the named observations ------------------------------------------------
rename_test()  { sed -i.bak 's/fun bridgeHandshakeAgreesWithTheCompiledAuthority(/fun handshake(/' "$TESTFILE" \
                 && rm -f "$TESTFILE.bak"; }
degate_test()  { sed -i.bak '/endpointConstructAndCloseCompletesOffTheMainThread/d' "$DEVICEGATE" \
                 && rm -f "$DEVICEGATE.bak"; }
empty_gate()   { : > "$DEVICEGATE"; }

case_is selftest.observation-renamed 1 \
    runtime.decl.observation.bridgeHandshakeAgreesWithTheCompiledAuthority rename_test
case_is selftest.observation-degated 1 \
    runtime.decl.observation.endpointConstructAndCloseCompletesOffTheMainThread degate_test
case_is selftest.device-gate-empty   1 runtime.decl.device-gate empty_gate

# --- the fresh process and the oracle --------------------------------------
FRESH="$TESTS/FreshProcessPublicOpenTest.kt"
HOLD="$TESTS/MainLooperHold.kt"
fresh_inits()      { printf 'val x = NativeBridgeOwner.initialize()\n' >> "$FRESH"; }
fresh_loads()      { printf 'val y = System.loadLibrary("moq5_jni")\n' >> "$FRESH"; }
fresh_absent()     { rm -f "$FRESH"; }
fresh_renamed()    { sed -i.bak 's/fun publicOpenInAFreshProcessNeedsNoExplicitInitialization(/fun freshOpen(/' "$FRESH" \
                     && rm -f "$FRESH.bak"; }
no_helper()        { rm -f "$HOLD"; sed -i.bak '/MainLooperHold/d' "$TESTFILE" && rm -f "$TESTFILE.bak"; }
helper_unused()    { sed -i.bak 's/MainLooperHold\.run/notTheOracle/g' "$TESTFILE" && rm -f "$TESTFILE.bak"; }

case_is selftest.fresh-inits    1 runtime.decl.fresh-no-explicit-init fresh_inits
case_is selftest.fresh-loads    1 runtime.decl.fresh-no-explicit-init fresh_loads
case_is selftest.fresh-absent   1 runtime.decl.fresh-no-explicit-init fresh_absent
case_is selftest.fresh-renamed  1 \
    runtime.decl.observation.publicOpenInAFreshProcessNeedsNoExplicitInitialization fresh_renamed
case_is selftest.oracle-absent  1 runtime.decl.oracle-helper no_helper
case_is selftest.oracle-unused  1 runtime.decl.oracle-used   helper_unused

# The initialize call already appears in the fresh test as a COMMENT; the
# positive control passes with it there. A load named only in prose must
# likewise stay green.
fresh_prose()      { printf '// System.loadLibrary("moq5_jni") is the product owner\x27s job\n' >> "$FRESH"; }
case_is selftest.fresh-prose    0 '' fresh_prose

# --- the verification boundary ---------------------------------------------
wire_device_gate() { sed -i.bak 's|check-android-runtime-declaration.sh|check-android-runtime.sh|' "$ROOTBUILD" \
                     && rm -f "$ROOTBUILD.bak"; }
drop_expected()    { sed -i.bak '/setOf(/,/^    )/ s/"verifyAndroidRuntimeDeclaration",//' "$ROOTBUILD" \
                     && rm -f "$ROOTBUILD.bak"; }
drop_required()    { sed -i.bak '/listOf(/,/^    )/ s/"verifyAndroidRuntimeDeclarationSelfTest",//' "$ROOTBUILD" \
                     && rm -f "$ROOTBUILD.bak"; }
comment_expected() { sed -i.bak '/setOf(/,/^    )/ s|^\( *\)"verifyAndroidRuntimeDeclarationSelfTest",|\1// "verifyAndroidRuntimeDeclarationSelfTest",|' "$ROOTBUILD" \
                     && rm -f "$ROOTBUILD.bak"; }

case_is selftest.device-gate-wired 1 runtime.decl.device-gate-unwired wire_device_gate
case_is selftest.boundary-expected 1 \
    runtime.decl.boundary.expectedVerificationTasks.verifyAndroidRuntimeDeclaration drop_expected
case_is selftest.boundary-required 1 \
    runtime.decl.boundary.requiredVerificationTasks.verifyAndroidRuntimeDeclarationSelfTest drop_required
case_is selftest.boundary-comment  1 \
    runtime.decl.boundary.expectedVerificationTasks.verifyAndroidRuntimeDeclarationSelfTest comment_expected

printf '\n%s: %d PASS / %d FAIL\n' "$_self" "$passes" "$fails"
[ "$fails" -eq 0 ] || exit 1
exit 0
