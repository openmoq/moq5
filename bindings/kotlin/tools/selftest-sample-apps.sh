#!/bin/sh
# The sample-app gate's own ledger.
#
# A gate is only worth its rows if each one can fail. Before the samples
# existed this gate reported eight failures -- module, settings, workflow and
# root-check, for each of the two samples -- and every one of them followed
# from a single fact: the modules were absent. That left the rest of the gate
# (the consumer shape, the warning policy, the private-package audit, the
# workflow census, the root list membership) entirely unexercised, and an
# unexercised row is one that could be passing vacuously.
#
# So this self-test builds a disposable fixture root that satisfies the gate
# COMPLETELY, proves the gate accepts it, and then breaks one fact at a time
# and requires the matching row to fail BY NAME. The fixture is a stand-in for
# a sample, not a sample: it exists only to give each failure path something to
# fire on.
set -u
LC_ALL=C; export LC_ALL

_self=${0##*/}
case $0 in
    */*) _selfdir=${0%/*} ;;
    *)   _selfdir=. ;;
esac
here=$(cd "$_selfdir" && pwd) || { echo "$_self: cannot resolve the script directory" >&2; exit 2; }

for t in mktemp rm cp sed grep printf mkdir; do
    command -v "$t" >/dev/null 2>&1 \
        || { echo "$_self: required tool not found: $t" >&2; exit 2; }
done

W=$(mktemp -d) || { echo "$_self: cannot create a scratch directory" >&2; exit 2; }
cleanup() { rm -rf -- "$W"; }
trap 'cleanup; exit 130' HUP INT TERM
trap cleanup EXIT

fails=0
pass() { printf 'PASS[%s]: %s\n' "$1" "$2"; }
fail() { fails=$((fails + 1)); printf 'FAIL[%s]: %s\n' "$1" "$2"; }

# A fixture root: bindings/kotlin's shape, with the gate copied in beside it so
# it resolves this root rather than the real one.
build_fixture() {
    rm -rf -- "$W/root"
    mkdir -p "$W/root/tools" || return 1
    cp "$here/check-sample-apps.sh" "$W/root/tools/" || return 1
    cat > "$W/root/settings.gradle.kts" <<'EOF'
rootProject.name = "moq5-bindings"
include(":moq5-android")
include(":samples:player")
include(":samples:publisher")
EOF
    # The root's module verification boundary, named twice as the real root
    # names it: once in the independent expected set, once in the configured
    # list. The gate requires both, so the fixture must carry both.
    cat > "$W/root/build.gradle.kts" <<'EOF'
// The module verification boundary: an independent expected set and the
// configured list, each naming ":samples:player:check" exactly once.
val expectedModuleVerificationTasks =
    setOf(":moq5-android:check", ":bridge-r8-fixture:check", ":samples:player:check", ":samples:publisher:check")
val requiredModuleVerificationTasks =
    listOf(":moq5-android:check", ":bridge-r8-fixture:check", ":samples:player:check", ":samples:publisher:check")
EOF
    for s in player publisher; do
        mkdir -p "$W/root/samples/$s/src/main/kotlin" || return 1
        cat > "$W/root/samples/$s/build.gradle.kts" <<'EOF'
plugins { id("com.android.application") }
android { lint { warningsAsErrors = true; abortOnError = true } }
kotlin { compilerOptions { allWarningsAsErrors.set(true) } }
tasks.withType<JavaCompile>().configureEach { options.compilerArgs.addAll(listOf("-Xlint:all", "-Werror")) }
dependencies { implementation(project(":moq5-android")) }
EOF
    done
    # Each fixture source exercises its whole workflow in EXECUTABLE-LOOKING
    # Kotlin. It is a stand-in, not a sample -- it never runs -- but it must be
    # code, because the census now reads code and would not see a comment.
    cat > "$W/root/samples/player/src/main/kotlin/Fixture.kt" <<'EOF'
package fixture

/* A stand-in for the player sample: the workflow, in source the census reads. */
suspend fun watch(url: String, ns: Namespace) {
    try {
        session(url, ns)
    } catch (cancelled: CancellationException) {
        throw cancelled
    } catch (failure: Exception) {
        report(failure)
    }
}

private suspend fun session(url: String, ns: Namespace) {
    val endpoint = MoqEndpoint.connect(EndpointConfig(url), 10.seconds)
    val receiver = endpoint.attachReceiver(ReceiverConfig(ns, OverflowPolicy.DropToKeyframe))
    val track = receiver.inventory.first { it.catalogReady }.tracks.first()
    receiver.subscribe(track)
    while (true) {
        val obj = receiver.objects.next() ?: break
        report(obj.mediaBytes().size)
    }
    report(receiver.statsSnapshot().objectsReceived)
    receiver.close()
    endpoint.closeAndJoin()
}
EOF
    # The session-wiring census reads ONE file per sample, at its real path, so
    # the fixture carries stand-ins there too. They are code for the same reason
    # the workflow fixtures are: the census reads executable text.
    mkdir -p "$W/root/samples/player/src/main/kotlin/org/openmoq/moq5/samples/player"
    cat > "$W/root/samples/player/src/main/kotlin/org/openmoq/moq5/samples/player/PlaybackSession.kt" <<'EOF'
package fixture

/* A stand-in for the player session: the seam calls the census reads. */
suspend fun play(track: Track, objects: Objects) {
    val format = DecoderFormats.of(MediaFacts.ofTrack(track.description))
    val decode = PlaybackDecode(format)
    decode.accept(MediaFacts.ofObject(objects.next()))
}
EOF
    mkdir -p "$W/root/samples/publisher/src/main/kotlin/org/openmoq/moq5/samples/publisher"
    cat > "$W/root/samples/publisher/src/main/kotlin/org/openmoq/moq5/samples/publisher/BroadcastSession.kt" <<'EOF'
package fixture

/* A stand-in for the publisher session: the seam calls the census reads, and
   the camera2 imports the bounded allowance permits -- fully qualified, so the
   class-aware allowance is actually exercised. */
import android.hardware.camera2.CameraAccessException
import android.hardware.camera2.CameraCharacteristics
import android.hardware.camera2.CameraManager
import android.hardware.camera2.CameraMetadata

fun cameraClock(manager: CameraManager, id: String): Int? =
    manager.getCameraCharacteristics(id).get(CameraCharacteristics.SENSOR_INFO_TIMESTAMP_SOURCE)

val unknownSource = CameraMetadata.SENSOR_INFO_TIMESTAMP_SOURCE_UNKNOWN

suspend fun broadcastFrames(encoder: Encoder, sender: Sender) {
    val clock = PlatformCaptureClock(Calibrator.measure(elapsed, wall))
    val pipeline = PublishPipeline(kind, timeline, clock)
    val captured = encoder.next()
    val step = pipeline.step(captured.output, captured.captureElapsedNanos)
    writeOne(pipeline, sink, step.obj, timeout)
    endpointCompletion(sender, endpoint).finishPublication(timeout, report)
}
EOF
    cat > "$W/root/samples/publisher/src/main/kotlin/Fixture.kt" <<'EOF'
package fixture

/* A stand-in for the publisher sample: the workflow, in source the census reads. */
suspend fun broadcast(url: String, ns: Namespace) {
    val endpoint = MoqEndpoint.connect(EndpointConfig(url), 10.seconds)
    val sender = endpoint.attachSender(SenderConfig(ns, SendBackpressure.ReturnWouldBlock))
    sender.awaitReady()
    val track = sender.addTrack(SendTrackConfig(name, MediaType.Video, PackagingKind.Raw, codec, 2_000_000uL))
    for (frame in frames()) {
        sender.write(track, SendObject(frame, isSync = true, startsGroup = true, presentationTime = now()), 1.seconds)
    }
    sender.endTrack(track, 1.seconds)
    sender.complete()
    endpoint.drain(5.seconds)
    sender.close()
    endpoint.closeAndJoin()
}
EOF
    return 0
}

run_gate() { sh "$W/root/tools/check-sample-apps.sh" > "$W/out" 2>&1; printf '%s' "$?"; }

# case <label> <expected-exit 0|1> <expected-row-or-empty> <mutator...>
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
        pass "$_label" "the gate accepts a complete pair of samples"
    fi
}

# The positive control FIRST: without it every kill below could be an artefact
# of a fixture the gate never accepts.
case_is selftest.accepts 0 ''

drop_module()      { rm -rf -- "$W/root/samples/player"; }
drop_include()     { sed -i.bak '/samples:player/d' "$W/root/settings.gradle.kts" && rm -f "$W/root/settings.gradle.kts.bak"; }
drop_dependency()  { sed -i.bak 's/implementation(project(":moq5-android"))/implementation("org.openmoq:moq5-android:1.0")/' "$W/root/samples/player/build.gradle.kts" && rm -f "$W/root/samples/player/build.gradle.kts.bak"; }
add_sourceset()    { printf 'android { sourceSets { getByName("main") { java.srcDir("../../moq5-android/src/main/kotlin") } } }\n' >> "$W/root/samples/player/build.gradle.kts"; }
drop_kotlin_werr() { sed -i.bak '/allWarningsAsErrors/d' "$W/root/samples/player/build.gradle.kts" && rm -f "$W/root/samples/player/build.gradle.kts.bak"; }
drop_lint_werr()   { sed -i.bak '/warningsAsErrors/d' "$W/root/samples/player/build.gradle.kts" && rm -f "$W/root/samples/player/build.gradle.kts.bak"; }
drop_javac_werr()  { sed -i.bak '/Werror/d' "$W/root/samples/player/build.gradle.kts" && rm -f "$W/root/samples/player/build.gradle.kts.bak"; }
add_native()       { printf 'android { externalNativeBuild { cmake { path = file("CMakeLists.txt") } } }\n' >> "$W/root/samples/player/build.gradle.kts"; }
add_publishing()   { printf 'plugins { id("maven-publish") }\n' >> "$W/root/samples/player/build.gradle.kts"; }
# The module carries the workflow fixture AND the session stand-in, so "no
# sources" means both are gone.
drop_sources()     { find "$W/root/samples/player/src" -name '*.kt' -exec rm -f {} +; }
use_engine()       { printf 'import org.openmoq.moq5.engine.EndpointEngine\n' >> "$W/root/samples/player/src/main/kotlin/Fixture.kt"; }
use_jni()          { printf 'import org.openmoq.moq5.jni.NativeBridge\n' >> "$W/root/samples/player/src/main/kotlin/Fixture.kt"; }
use_c_header()     { printf '// moq/media_receiver.h\n' >> "$W/root/samples/player/src/main/kotlin/Fixture.kt"; }
use_fixture_dso()  { printf '// System.loadLibrary("moq5_jni")\n' >> "$W/root/samples/player/src/main/kotlin/Fixture.kt"; }
drop_subscribe()   { sed -i.bak 's/receiver\.subscribe(track)//' "$W/root/samples/player/src/main/kotlin/Fixture.kt" && rm -f "$W/root/samples/player/src/main/kotlin/Fixture.kt.bak"; }
drop_complete()    { sed -i.bak 's/sender\.complete()//' "$W/root/samples/publisher/src/main/kotlin/Fixture.kt" && rm -f "$W/root/samples/publisher/src/main/kotlin/Fixture.kt.bak"; }
drop_application() { sed -i.bak 's/id("com.android.application")/id("com.android.library")/' "$W/root/samples/player/build.gradle.kts" && rm -f "$W/root/samples/player/build.gradle.kts.bak"; }

# The library exports coroutines; a sample that declares it teaches the wrong
# consumer contract and would keep passing if that export regressed.
declare_coroutines()         { printf 'dependencies { implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.2") }\n' >> "$W/root/samples/player/build.gradle.kts"; }
declare_coroutines_android() { printf 'dependencies { implementation("org.jetbrains.kotlinx:kotlinx-coroutines-android:1.10.2") }\n' >> "$W/root/samples/player/build.gradle.kts"; }
# Green on every host gate, broken on a device: exactly what this row exists for.
use_main_dispatcher()        { printf 'val scope = CoroutineScope(Dispatchers.Main)\n' >> "$W/root/samples/publisher/src/main/kotlin/Fixture.kt"; }
import_android_dispatcher()  { printf 'import kotlinx.coroutines.android.asCoroutineDispatcher\n' >> "$W/root/samples/player/src/main/kotlin/Fixture.kt"; }
# ... but EXPLAINING why it is avoided is not using it: the row reads code.
mention_main_in_comment()    { printf '// deliberately not Dispatchers.Main: see kotlinx.coroutines.android\n' >> "$W/root/samples/player/src/main/kotlin/Fixture.kt"; }

# Cancellation IS an Exception: a generic catch with no cancellation catch
# reports Stop as a failure, and one that catches but does not rethrow breaks
# structured concurrency just as thoroughly.
swallow_cancellation() {
    sed -i.bak '/catch (cancelled: CancellationException) {/,+1d' "$W/root/samples/player/src/main/kotlin/Fixture.kt" &&
        rm -f "$W/root/samples/player/src/main/kotlin/Fixture.kt.bak"
}
report_cancellation() {
    sed -i.bak 's/        throw cancelled/        report(cancelled)/' "$W/root/samples/player/src/main/kotlin/Fixture.kt" &&
        rm -f "$W/root/samples/player/src/main/kotlin/Fixture.kt.bak"
}

# The ONE widening ruled in 1748 F2: correct cleanup has to attach its own
# failures to the ACTUAL cancellation, so the rethrowing arm may first record
# the caught instance in a simple local slot. Everything else about the arm
# stays refused, and the three rows below are what hold that line.
record_then_rethrow() {
    sed -i.bak 's/^        throw cancelled$/        primary = cancelled\n        throw cancelled/' \
        "$W/root/samples/player/src/main/kotlin/Fixture.kt" &&
        rm -f "$W/root/samples/player/src/main/kotlin/Fixture.kt.bak"
}
# ... but only the instance that was caught.
record_then_rethrow_other() {
    sed -i.bak 's/^        throw cancelled$/        primary = cancelled\n        throw other/' \
        "$W/root/samples/player/src/main/kotlin/Fixture.kt" &&
        rm -f "$W/root/samples/player/src/main/kotlin/Fixture.kt.bak"
}
# ... and only one recording statement, not an arbitrary body.
record_twice_then_rethrow() {
    sed -i.bak 's/^        throw cancelled$/        primary = cancelled\n        report(cancelled)\n        throw cancelled/' \
        "$W/root/samples/player/src/main/kotlin/Fixture.kt" &&
        rm -f "$W/root/samples/player/src/main/kotlin/Fixture.kt.bak"
}
# ... and recording without rethrowing is still a swallow.
record_then_return() {
    sed -i.bak 's/^        throw cancelled$/        primary = cancelled\n        return/' \
        "$W/root/samples/player/src/main/kotlin/Fixture.kt" &&
        rm -f "$W/root/samples/player/src/main/kotlin/Fixture.kt.bak"
}

# The SECOND widening, ruled in 1750 F1 and SCOPED in 1752 F1: inside an
# explicit `withContext(NonCancellable)` cleanup block a close may raise its own
# cancellation, and that must be collected like any other close failure rather
# than abandoning the resources after it. Two things scope it: the collecting
# arm must sit inside such a block, and the generic arm after it must collect
# into the SAME list. The same pair in an ordinary body, after the block has
# closed, or under an unrelated context is still a swallow.
#
# The fixture below is CLEANUP-SHAPED, because a positive control written over
# an ordinary session body would enshrine exactly the false pass this scoping
# exists to refuse.
cleanup_fixture() {
    cat > "$W/root/samples/player/src/main/kotlin/Cleanup.kt" <<'EOF'
package fixture

internal suspend fun closeAll(closeChild: suspend () -> Unit, closeEndpoint: suspend () -> Unit) {
    val failures = mutableListOf<Exception>()
    withContext(NonCancellable) {
        try {
            closeChild()
        } catch (cancelled: CancellationException) {
            failures.add(cancelled)
        } catch (closeFailure: Exception) {
            failures.add(closeFailure)
        }
        try {
            closeEndpoint()
        } catch (cancelled: CancellationException) {
            failures.add(cancelled)
        } catch (closeFailure: Exception) {
            failures.add(closeFailure)
        }
    }
    if (failures.isNotEmpty()) throw failures.first()
}
EOF
}
# ... the same pair in an ORDINARY body, with no cleanup block at all.
collector_outside_cleanup() {
    cat > "$W/root/samples/player/src/main/kotlin/Cleanup.kt" <<'EOF'
package fixture

internal suspend fun drain(work: suspend () -> Unit) {
    val failures = mutableListOf<Exception>()
    try {
        work()
    } catch (cancelled: CancellationException) {
        failures.add(cancelled)
    } catch (failure: Exception) {
        failures.add(failure)
    }
}
EOF
}
# ... and after the cleanup block has CLOSED, the region no longer covers it.
collector_after_cleanup_block() {
    cat > "$W/root/samples/player/src/main/kotlin/Cleanup.kt" <<'EOF'
package fixture

internal suspend fun closeThenWork(closeChild: suspend () -> Unit, work: suspend () -> Unit) {
    val failures = mutableListOf<Exception>()
    withContext(NonCancellable) {
        closeChild()
    }
    try {
        work()
    } catch (cancelled: CancellationException) {
        failures.add(cancelled)
    } catch (failure: Exception) {
        failures.add(failure)
    }
}
EOF
}
# ... and an UNRELATED context is not a cleanup block either.
collector_unrelated_context() {
    cat > "$W/root/samples/player/src/main/kotlin/Cleanup.kt" <<'EOF'
package fixture

internal suspend fun onDefault(work: suspend () -> Unit) {
    val failures = mutableListOf<Exception>()
    withContext(Dispatchers.Default) {
        try {
            work()
        } catch (cancelled: CancellationException) {
            failures.add(cancelled)
        } catch (failure: Exception) {
            failures.add(failure)
        }
    }
}
EOF
}
# ... and inside the block, a DIFFERENT list is still not the same collector.
collector_other_list_in_cleanup() {
    cat > "$W/root/samples/player/src/main/kotlin/Cleanup.kt" <<'EOF'
package fixture

internal suspend fun closeAll(closeChild: suspend () -> Unit) {
    val failures = mutableListOf<Exception>()
    val others = mutableListOf<Exception>()
    withContext(NonCancellable) {
        try {
            closeChild()
        } catch (cancelled: CancellationException) {
            others.add(cancelled)
        } catch (closeFailure: Exception) {
            failures.add(closeFailure)
        }
    }
    if (failures.isNotEmpty()) throw failures.first()
}
EOF
}

# ORDER, not membership. Kotlin matches catch clauses in source order, so a
# cancellation catch written AFTER the generic one is unreachable: cancellation
# is an Exception and the first clause takes it. A count-based oracle accepted
# this (chats/libmoq/1505).
wrong_catch_order() {
    cat > "$W/root/samples/player/src/main/kotlin/Fixture.kt" <<'EOF'
package fixture

suspend fun watch(url: String, ns: Namespace) {
    try {
        session(url, ns)
    } catch (failure: Exception) {
        report(failure)
    } catch (cancelled: CancellationException) {
        throw cancelled
    }
}

private suspend fun session(url: String, ns: Namespace) {
    val endpoint = MoqEndpoint.connect(EndpointConfig(url), 10.seconds)
    val receiver = endpoint.attachReceiver(ReceiverConfig(ns, OverflowPolicy.DropToKeyframe))
    val track = receiver.inventory.first { it.catalogReady }.tracks.first()
    receiver.subscribe(track)
    while (true) {
        val obj = receiver.objects.next() ?: break
        report(obj.mediaBytes().size)
    }
    report(receiver.statsSnapshot().objectsReceived)
    receiver.close()
    endpoint.closeAndJoin()
}
EOF
}

# A rethrow somewhere ELSE in the file, while the session body's own generic
# catch is unguarded: the other shape a count-based oracle accepted.
unrelated_rethrow() {
    cat > "$W/root/samples/player/src/main/kotlin/Fixture.kt" <<'EOF'
package fixture

private fun unrelated() {
    try {
        somethingElse()
    } catch (cancelled: CancellationException) {
        throw cancelled
    }
}

suspend fun watch(url: String, ns: Namespace) {
    try {
        session(url, ns)
    } catch (failure: Exception) {
        report(failure)
    }
}

private suspend fun session(url: String, ns: Namespace) {
    val endpoint = MoqEndpoint.connect(EndpointConfig(url), 10.seconds)
    val receiver = endpoint.attachReceiver(ReceiverConfig(ns, OverflowPolicy.DropToKeyframe))
    val track = receiver.inventory.first { it.catalogReady }.tracks.first()
    receiver.subscribe(track)
    while (true) {
        val obj = receiver.objects.next() ?: break
        report(obj.mediaBytes().size)
    }
    report(receiver.statsSnapshot().objectsReceived)
    receiver.close()
    endpoint.closeAndJoin()
}
EOF
}
# `Throwable` is the broader swallow: it takes cancellation too, and it takes
# the errors application code should never handle. It also EMPTIES the
# cancellation rule -- with no `catch (...: Exception)` left, that row would
# report "nothing to guard" -- which is why it is refused by its own row.
catch_throwable() {
    sed -i.bak 's/catch (failure: Exception)/catch (failure: Throwable)/' "$W/root/samples/player/src/main/kotlin/Fixture.kt" &&
        rm -f "$W/root/samples/player/src/main/kotlin/Fixture.kt.bak"
}

# A QUALIFIED spelling is the same fact. Classifying by the bare type text let
# `kotlin.Throwable` and `java.lang.Exception` walk past both rows
# (chats/libmoq/1509), so each is now a named case -- and so is qualified
# CORRECT code, because a normalization that made the right thing unwritable
# would be its own defect.
catch_qualified_throwable() {
    sed -i.bak 's/catch (failure: Exception)/catch (failure: kotlin.Throwable)/' "$W/root/samples/player/src/main/kotlin/Fixture.kt" &&
        rm -f "$W/root/samples/player/src/main/kotlin/Fixture.kt.bak"
}
catch_qualified_exception_unguarded() {
    cat > "$W/root/samples/player/src/main/kotlin/Fixture.kt" <<EOF
package fixture

suspend fun watch(url: String, ns: Namespace) {
    try {
        session(url, ns)
    } catch (failure: java.lang.Exception) {
        report(failure)
    }
}

private suspend fun session(url: String, ns: Namespace) {
    val endpoint = MoqEndpoint.connect(EndpointConfig(url), 10.seconds)
    val receiver = endpoint.attachReceiver(ReceiverConfig(ns, OverflowPolicy.DropToKeyframe))
    val track = receiver.inventory.first { it.catalogReady }.tracks.first()
    receiver.subscribe(track)
    while (true) {
        val obj = receiver.objects.next() ?: break
        report(obj.mediaBytes().size)
    }
    report(receiver.statsSnapshot().objectsReceived)
    receiver.close()
    endpoint.closeAndJoin()
}
EOF
}
catch_qualified_but_correct() {
    cat > "$W/root/samples/player/src/main/kotlin/Fixture.kt" <<EOF
package fixture

suspend fun watch(url: String, ns: Namespace) {
    try {
        session(url, ns)
    } catch (cancelled: kotlinx.coroutines.CancellationException) {
        throw cancelled
    } catch (failure: java.lang.Exception) {
        report(failure)
    }
}

private suspend fun session(url: String, ns: Namespace) {
    val endpoint = MoqEndpoint.connect(EndpointConfig(url), 10.seconds)
    val receiver = endpoint.attachReceiver(ReceiverConfig(ns, OverflowPolicy.DropToKeyframe))
    val track = receiver.inventory.first { it.catalogReady }.tracks.first()
    receiver.subscribe(track)
    while (true) {
        val obj = receiver.objects.next() ?: break
        report(obj.mediaBytes().size)
    }
    report(receiver.statsSnapshot().objectsReceived)
    receiver.close()
    endpoint.closeAndJoin()
}
EOF
}

# A file with NO generic catch has nothing to guard, and the row must not fire
# on it -- otherwise it would push every sample toward catching Exception.
no_generic_catch() {
    sed -i.bak '/} catch (failure: Exception) {/,+1d' "$W/root/samples/player/src/main/kotlin/Fixture.kt" &&
        sed -i.bak2 '/catch (cancelled: CancellationException) {/,+1d' "$W/root/samples/player/src/main/kotlin/Fixture.kt" &&
        rm -f "$W/root/samples/player/src/main/kotlin/Fixture.kt.bak" "$W/root/samples/player/src/main/kotlin/Fixture.kt.bak2"
}
add_dep_compose()  { printf 'dependencies { implementation("androidx.compose.ui:ui:1.0.0") }\n' >> "$W/root/samples/player/build.gradle.kts"; }
add_import_codec() { printf 'import android.media.MediaCodec\n' >> "$W/root/samples/publisher/src/main/kotlin/Fixture.kt"; }
drop_root_check()  { sed -i.bak 's/, ":samples:player:check"//g' "$W/root/build.gradle.kts" && rm -f "$W/root/build.gradle.kts.bak"; }
# Named in the CONFIGURED list but not in the independent expected set: the
# half-wired case the root's own two-list discipline exists to catch. The
# address is the `setOf(` line, so this is deterministic on any POSIX sed.
half_root_check()  { sed -i.bak '/setOf(/s/, ":samples:player:check"//' "$W/root/build.gradle.kts" && rm -f "$W/root/build.gradle.kts.bak"; }

# The root boundary must be read from the LISTS, never counted as text. Each of
# these removes the real membership and leaves the literal somewhere inert, or
# piles it into one list while the other has none -- both shapes a raw-text
# count would have accepted (1497).
root_check_comments() {
    drop_root_check || return 1
    {
        printf '// the boundary once named ":samples:player:check" here\n'
        printf '/* and ":samples:player:check" again, in prose */\n'
    } >> "$W/root/build.gradle.kts"
}
root_check_strings() {
    drop_root_check || return 1
    {
        printf 'val note = ":samples:player:check"\n'
        printf 'val alsoNote = listOf(":samples:player:check")\n'
    } >> "$W/root/build.gradle.kts"
}
root_check_expected_twice() {
    sed -i.bak '/listOf(/s/, ":samples:player:check"//' "$W/root/build.gradle.kts" &&
        sed -i.bak2 '/setOf(/s/":samples:player:check"/":samples:player:check", ":samples:player:check"/' "$W/root/build.gradle.kts" &&
        rm -f "$W/root/build.gradle.kts.bak" "$W/root/build.gradle.kts.bak2"
}
root_check_required_twice() {
    sed -i.bak '/setOf(/s/, ":samples:player:check"//' "$W/root/build.gradle.kts" &&
        sed -i.bak2 '/listOf(/s/":samples:player:check"/":samples:player:check", ":samples:player:check"/' "$W/root/build.gradle.kts" &&
        rm -f "$W/root/build.gradle.kts.bak" "$W/root/build.gradle.kts.bak2"
}

# The workflow census must read CODE. A sample that deletes a call and leaves a
# comment -- or a string -- naming it is exactly the self-referential oracle
# 1495 F1 rejected, so both forms are killed by name.
comment_only()     { drop_subscribe && printf '// receiver.subscribe(track)\n' >> "$W/root/samples/player/src/main/kotlin/Fixture.kt"; }
block_comment_only() { drop_subscribe && printf '/* receiver.subscribe(track) /* nested */ */\n' >> "$W/root/samples/player/src/main/kotlin/Fixture.kt"; }
string_only()      { drop_subscribe && printf 'val doc = "receiver.subscribe(track)"\n' >> "$W/root/samples/player/src/main/kotlin/Fixture.kt"; }
raw_string_only()  { drop_subscribe && printf 'val doc = """receiver.subscribe(track)"""\n' >> "$W/root/samples/player/src/main/kotlin/Fixture.kt"; }

# The session-wiring census reads ONE file each, in code. A session that stops
# calling a seam -- or that leaves the call in a comment or a string -- is the
# regression this row exists for, and a session that re-derives what a seam owns
# is the other half of it.
_player_session="$W/root/samples/player/src/main/kotlin/org/openmoq/moq5/samples/player/PlaybackSession.kt"
_publisher_session="$W/root/samples/publisher/src/main/kotlin/org/openmoq/moq5/samples/publisher/BroadcastSession.kt"
drop_session_call()      { sed -i.bak '/decode.accept(/d' "$_player_session" && rm -f "$_player_session.bak"; }
session_call_comment()   { drop_session_call && printf '// decode.accept(MediaFacts.ofObject(o))\n' >> "$_player_session"; }
session_call_string()    { drop_session_call && printf 'val doc = "decode.accept(MediaFacts.ofObject(o))"\n' >> "$_player_session"; }
drop_session_timestamp() { sed -i.bak 's/captured.captureElapsedNanos/0L/' "$_publisher_session" && rm -f "$_publisher_session.bak"; }
derive_session_cadence() { printf 'val at = base + index * FRAME_INTERVAL_NANOS\n' >> "$_publisher_session"; }
cadence_in_comment()     { printf '// never derive a capture instant from FRAME_INTERVAL_NANOS\n' >> "$_publisher_session"; }
drop_session_finish()    { sed -i.bak '/finishPublication(/d' "$_publisher_session" && rm -f "$_publisher_session.bak"; }
complete_directly()      { printf 'suspend fun done(s: Sender) { s.complete() }\n' >> "$_publisher_session"; }
drain_directly()         { printf 'suspend fun done(e: Endpoint) { e.drain(timeout) }\n' >> "$_publisher_session"; }
finish_in_comment()      { printf '// never complete() before the media drain(); finishPublication orders them\n' >> "$_publisher_session"; }

# camera2 is bounded, not open: the publisher reads the metadata that names the
# capture clock, and nothing that opens a camera or runs a session. The player
# has no camera role at all.
_player_fixture="$W/root/samples/player/src/main/kotlin/Fixture.kt"
_publisher_fixture="$W/root/samples/publisher/src/main/kotlin/org/openmoq/moq5/samples/publisher/BroadcastSession.kt"
use_camera_device()    { printf 'val device: CameraDevice? = null\n' >> "$_publisher_fixture"; }
use_capture_session()  { printf 'val session: CameraCaptureSession? = null\n' >> "$_publisher_fixture"; }
use_open_camera()      { printf 'fun start(m: CameraManager) { m.openCamera(id, cb, handler) }\n' >> "$_publisher_fixture"; }
use_create_session()   { printf 'fun start(d: Any) { d.createCaptureSession(config) }\n' >> "$_publisher_fixture"; }
use_androidx_camera()  { printf 'import androidx.camera.core.Preview\n' >> "$_publisher_fixture"; }
use_unknown_camera2()  { printf 'import android.hardware.camera2.CameraExtensionSession\n' >> "$_publisher_fixture"; }
# A wildcard plus an UNLISTED class: no explicit deny name matches, so this is
# caught only if the wildcard itself is refused.
use_camera2_wildcard() { printf 'import android.hardware.camera2.*\nval smuggled: CameraExtensionSession? = null\n' >> "$_publisher_fixture"; }
player_camera2_wildcard() { printf 'import android.hardware.camera2.*\n' >> "$_player_fixture"; }
# The same directive with the whitespace the grammar permits, and the same token
# where it imports nothing.
spaced_camera2_wildcard() { printf 'import  android . hardware . camera2 . * ;\n' >> "$_publisher_fixture"; }
camera2_wildcard_comment() { printf '// import android.hardware.camera2.* is refused\n' >> "$_publisher_fixture"; }
camera2_wildcard_string()  { printf 'val doc = "import android.hardware.camera2.*"\n' >> "$_publisher_fixture"; }
use_mediacodec()       { printf 'import android.media.MediaCodec\n' >> "$_publisher_fixture"; }
# The exact classes C' will name, derived from the Phase A design and no wider:
# the capture request/result pair and the three state/capture callbacks; the
# encoder's format, capability and buffer-info types; the microphone, its
# format, its timestamp and the source constant that names the microphone; and
# the worker the platform delivers callbacks on.
use_capture_request()  { printf 'import android.hardware.camera2.CaptureRequest\nimport android.hardware.camera2.CaptureResult\nimport android.hardware.camera2.TotalCaptureResult\n' >> "$_publisher_fixture"; }
use_capture_callbacks() { printf 'val a: CameraDevice.StateCallback? = null\nval b: CameraCaptureSession.StateCallback? = null\nval c: CameraCaptureSession.CaptureCallback? = null\n' >> "$_publisher_fixture"; }
use_codec_format()     { printf 'import android.media.MediaFormat\nimport android.media.MediaCodecInfo\nval i: MediaCodec.BufferInfo? = null\nval k: MediaCodec.Callback? = null\nval e: MediaCodec.CodecException? = null\n' >> "$_publisher_fixture"; }
# MediaCodecInfo alone, outside the encoder driver: no dedicated row names it,
# and the boundary holds only because the fixed-string MediaCodec scan in the
# unjustified-imports row also matches the MediaCodecInfo prefix.
use_codec_info_only()  { printf 'import android.media.MediaCodecInfo\n' >> "$_publisher_fixture"; }
_audio_fixture="$W/root/samples/publisher/src/main/kotlin/org/openmoq/moq5/samples/publisher/AudioRecordCapture.kt"
use_audio_capture()    { printf 'import android.media.AudioRecord\nimport android.media.AudioFormat\nimport android.media.AudioTimestamp\nimport android.media.MediaRecorder\nval src = MediaRecorder.AudioSource.MIC\n' > "$_audio_fixture"; }
audio_record_outside() { printf 'import android.media.AudioRecord\n' >> "$_publisher_fixture"; }
audio_record_spaced()  { printf 'val rec: android . media . AudioRecord? = null\n' >> "$_publisher_fixture"; }
audio_record_comment() { printf '// import android.media.AudioRecord\nval note = "android.media.AudioRecord"\n' >> "$_publisher_fixture"; }
use_media_muxer()      { printf 'import android.media.MediaMuxer\n' >> "$_publisher_fixture"; }
# A name in a comment or a string is not a use, for the unjustified-imports row
# as for every other source row; the real import above stays refused.
muxer_comment()        { printf '// import android.media.MediaMuxer would be refused\n' >> "$_publisher_fixture"; }
muxer_string()         { printf 'val doc = "import android.media.MediaMuxer"\n' >> "$_publisher_fixture"; }
codec_comment_outside() { printf '/* android.media.MediaCodec belongs to the encoder driver */\n' >> "$_publisher_fixture"; }
androidx_comment()     { printf '// androidx.compose is deliberately absent\n' >> "$_publisher_fixture"; }
# Java sources are sample sources too: the role and file allowances hold for
# them exactly as for Kotlin.
_player_java="$W/root/samples/player/src/main/java/org/openmoq/moq5/samples/player/TopologyJavaProbe.java"
_publisher_java="$W/root/samples/publisher/src/main/java/org/openmoq/moq5/samples/publisher/TopologyJavaProbe.java"
java_file() { mkdir -p "$(dirname "$1")" && printf 'package org.openmoq.moq5.samples;\n%s\nfinal class TopologyJavaProbe {}\n' "$2" > "$1"; }
java_player_audio_record()    { java_file "$_player_java" 'import android.media.AudioRecord;'; }
java_player_audio_track()     { java_file "$_player_java" 'import android.media.AudioTrack;'; }
java_publisher_session()      { java_file "$_publisher_java" 'import android.hardware.camera2.CameraDevice;'; }
java_publisher_metadata()     { java_file "$_publisher_java" 'import android.hardware.camera2.CameraCharacteristics;'; }
java_publisher_audio_record() { java_file "$_publisher_java" 'import android.media.AudioRecord;'; }
# Java block comments do not nest: the first */ closes it, so what follows is code.
java_comment_not_nested()     { java_file "$_publisher_java" '/* a /* b */ import android.hardware.camera2.CameraDevice;'; }
java_comment_then_import()    { java_file "$_player_java" '/* explanatory /* text */
import android.media.AudioRecord;'; }
# The spacing the grammar permits around a dot is the same qualified name.
codec_spaced_outside() { printf 'import android . media . MediaCodec\n' >> "$_publisher_fixture"; }
androidx_spaced()      { printf 'import androidx . camera . core . Preview\n' >> "$_publisher_fixture"; }
use_unknown_audio()    { printf 'import android.media.AudioTrack\n' >> "$_publisher_fixture"; }
media_wildcard()       { printf 'import android.media.*\nval t: AudioTrack? = null\n' >> "$_publisher_fixture"; }
player_media_wildcard() { printf 'import android.media.*\n' >> "$_player_fixture"; }
media_wildcard_spaced() { printf 'import  android . media . * ;\n' >> "$_publisher_fixture"; }
media_wildcard_comment() { printf '// import android.media.* is refused\n' >> "$_publisher_fixture"; }
media_wildcard_string()  { printf 'val doc = "import android.media.*"\n' >> "$_publisher_fixture"; }
media_qualified_unknown() { printf 'val t: android . media . AudioTrack? = null\n' >> "$_publisher_fixture"; }
player_camera_device() { printf 'import android.hardware.camera2.CameraDevice\n' >> "$_player_fixture"; }
player_mediacodec()    { printf 'import android.media.MediaCodec\n' >> "$_player_fixture"; }
player_audio_record()  { printf 'import android.media.AudioRecord\n' >> "$_player_fixture"; }
player_uses_camera2()  { printf 'import android.hardware.camera2.CameraCharacteristics\n' >> "$_player_fixture"; }

# -- C' Phase A: the capture-session allowance, declared before it exists ------
#
# Opening the camera and running its session belong to the Camera2 driver
# file alone. The same shapes in the broadcast session are refused; the scoped
# positives are the camera-driver controls below. Every one of them has a
# player-side negative too: the publisher's allowance never reaches the player.
case_is selftest.camera2-device        1 sample.publisher.camera2           use_camera_device
case_is selftest.camera2-session       1 sample.publisher.camera2           use_capture_session
case_is selftest.camera2-open          1 sample.publisher.camera2           use_open_camera
case_is selftest.camera2-create        1 sample.publisher.camera2           use_create_session
case_is selftest.camera2-androidx      1 sample.publisher.unjustified-imports use_androidx_camera
case_is selftest.capture-request       1 sample.publisher.camera2           use_capture_request
case_is selftest.capture-callbacks     1 sample.publisher.camera2           use_capture_callbacks
case_is selftest.capture-codec-format  1 sample.publisher.unjustified-imports use_codec_format
case_is selftest.capture-codec-info-file 1 sample.publisher.unjustified-imports use_codec_info_only
case_is selftest.capture-audio         0 ''                                  use_audio_capture
case_is selftest.audio-record-outside  1 sample.publisher.audio-record-file  audio_record_outside
case_is selftest.audio-record-spaced   1 sample.publisher.audio-record-file  audio_record_spaced
case_is selftest.audio-record-comment  0 ''                                  audio_record_comment
# The role's class list is closed: AudioTrack is playback, not capture, and
# AudioRecord is capture, not playback. A wildcard cannot be classified.
case_is selftest.capture-audio-unknown 1 sample.publisher.media             use_unknown_audio
case_is selftest.media-qualified-unknown 1 sample.publisher.media           media_qualified_unknown
case_is selftest.media-wildcard        1 sample.publisher.media             media_wildcard
case_is selftest.media-wildcard-player 1 sample.player.media                player_media_wildcard
case_is selftest.media-wildcard-spaced 1 sample.publisher.media             media_wildcard_spaced
case_is selftest.media-wildcard-comment 0 ''                                  media_wildcard_comment
case_is selftest.media-wildcard-string  0 ''                                  media_wildcard_string
# What the widening must NOT reach, on either side.
case_is selftest.capture-muxer         1 sample.publisher.unjustified-imports use_media_muxer
case_is selftest.muxer-comment         0 ''                                  muxer_comment
case_is selftest.muxer-string          0 ''                                  muxer_string
case_is selftest.codec-comment-outside 0 ''                                  codec_comment_outside
case_is selftest.androidx-comment      0 ''                                  androidx_comment
case_is selftest.codec-spaced-outside  1 sample.publisher.unjustified-imports codec_spaced_outside
case_is selftest.java-player-audio-record 1 sample.player.media              java_player_audio_record
case_is selftest.java-player-audio-track 0 ''                                java_player_audio_track
case_is selftest.java-publisher-session 1 sample.publisher.camera2          java_publisher_session
case_is selftest.java-publisher-metadata 0 ''                               java_publisher_metadata
case_is selftest.java-publisher-audio-record 1 sample.publisher.audio-record-file java_publisher_audio_record
case_is selftest.java-comment-not-nested 1 sample.publisher.camera2         java_comment_not_nested
case_is selftest.java-comment-then-import 1 sample.player.media             java_comment_then_import
case_is selftest.androidx-spaced       1 sample.publisher.unjustified-imports androidx_spaced
case_is selftest.capture-player-camera 1 sample.player.camera2               player_camera_device
case_is selftest.capture-player-codec  1 sample.player.unjustified-imports   player_mediacodec
case_is selftest.capture-player-audio  1 sample.player.media                 player_audio_record
case_is selftest.camera2-unknown-class 1 sample.publisher.camera2           use_unknown_camera2
case_is selftest.camera2-wildcard      1 sample.publisher.camera2           use_camera2_wildcard
case_is selftest.camera2-wildcard-player 1 sample.player.camera2            player_camera2_wildcard
case_is selftest.camera2-wildcard-spaced 1 sample.publisher.camera2         spaced_camera2_wildcard
case_is selftest.camera2-wildcard-comment 0 ''                              camera2_wildcard_comment
case_is selftest.camera2-wildcard-string  0 ''                              camera2_wildcard_string
case_is selftest.camera2-player        1 sample.player.camera2              player_uses_camera2

# The encoder driver's allowance is one file and two exact classes. Inside it:
# the allowed imports pass; an unlisted class of the family, in any spacing, a
# wildcard, or the muxer is refused; the same text in a comment imports nothing.
# The same file name in the player is still the player's refusal, and every
# other publisher file keeps the refusal `selftest.import-codec` pins.
_encoder_fixture="$W/root/samples/publisher/src/main/kotlin/org/openmoq/moq5/samples/publisher/MediaCodecEncoder.kt"
_player_encoder="$W/root/samples/player/src/main/kotlin/org/openmoq/moq5/samples/publisher/MediaCodecEncoder.kt"
encoder_file()      { mkdir -p "$(dirname "$_encoder_fixture")" && printf 'package org.openmoq.moq5.samples.publisher\n\nimport android.media.MediaCodec\nimport android.media.MediaCodecInfo\nimport android.media.MediaFormat\n' > "$_encoder_fixture"; }
encoder_allowed()   { encoder_file; }
encoder_unlisted()  { encoder_file && printf 'import android.media.MediaCodecList\n' >> "$_encoder_fixture"; }
encoder_spaced()    { encoder_file && printf 'import  android . media . MediaCodecList\n' >> "$_encoder_fixture"; }
encoder_wildcard()  { encoder_file && printf 'import android.media.*\n' >> "$_encoder_fixture"; }
encoder_muxer()     { encoder_file && printf 'import android.media.MediaMuxer\n' >> "$_encoder_fixture"; }
encoder_comment()   { encoder_file && printf '// android.media.MediaCodecList is not used here\nval doc = "import android.media.*"\n' >> "$_encoder_fixture"; }
encoder_in_player() { mkdir -p "$(dirname "$_player_encoder")" && printf 'import android.media.MediaCodec\n' > "$_player_encoder"; }
case_is selftest.encoder-allowed       0 ''                                   encoder_allowed
# The platform encoder does not belong in the broadcast session: the scoped
# positive is the encoder driver above, and the same import in the session
# file is refused.
case_is selftest.camera2-mediacodec    1 sample.publisher.unjustified-imports use_mediacodec
case_is selftest.encoder-unlisted      1 sample.publisher.encoder-imports     encoder_unlisted
case_is selftest.encoder-spaced        1 sample.publisher.encoder-imports     encoder_spaced
case_is selftest.encoder-wildcard      1 sample.publisher.encoder-imports     encoder_wildcard
case_is selftest.encoder-muxer         1 sample.publisher.unjustified-imports encoder_muxer
case_is selftest.encoder-comment       0 ''                                   encoder_comment
case_is selftest.encoder-player        1 sample.player.unjustified-imports    encoder_in_player

# The player's real decoder and renderer may each import MediaCodec, but that
# exception is neither a new class family nor a third-file allowance.
_player_codec="$W/root/samples/player/src/main/kotlin/org/openmoq/moq5/samples/player/AndroidPlaybackCodec.kt"
_player_render="$W/root/samples/player/src/main/kotlin/org/openmoq/moq5/samples/player/AndroidPlaybackRender.kt"
player_codec_files()   { printf 'import android.media.MediaCodec\n' > "$_player_codec" && printf 'import android.media.MediaCodec\n' > "$_player_render"; }
player_codec_unlisted() { player_codec_files && printf 'import android.media.MediaCodecList\n' >> "$_player_codec"; }
player_codec_wildcard() { player_codec_files && printf 'import android.media.*\n' >> "$_player_render"; }
player_codec_comment()  { player_codec_files && printf '// android.media.MediaCodecList\nval note = "android.media.MediaCodecList"\n' >> "$_player_codec"; }
case_is selftest.player-codec-allowed   0 ''                                   player_codec_files
case_is selftest.player-codec-unlisted  1 sample.player.codec-imports          player_codec_unlisted
case_is selftest.player-codec-wildcard  1 sample.player.codec-imports          player_codec_wildcard
case_is selftest.player-codec-comment   0 ''                                   player_codec_comment

# The Camera2 driver's allowance is one file and exact classes: the metadata
# classes, the device, the session and the request. Inside it the open and
# session calls are allowed; an unlisted class, in any spacing, or a wildcard
# is refused, and the same file name in the player is the player's refusal.
_camera_fixture="$W/root/samples/publisher/src/main/kotlin/org/openmoq/moq5/samples/publisher/Camera2Capture.kt"
_player_camera="$W/root/samples/player/src/main/kotlin/org/openmoq/moq5/samples/publisher/Camera2Capture.kt"
camera_file()          { mkdir -p "$(dirname "$_camera_fixture")" && printf 'package org.openmoq.moq5.samples.publisher\n\nimport android.hardware.camera2.CameraCaptureSession\nimport android.hardware.camera2.CameraCharacteristics\nimport android.hardware.camera2.CameraDevice\nimport android.hardware.camera2.CameraManager\nimport android.hardware.camera2.CaptureRequest\nfun open(m: CameraManager, d: CameraDevice) { m.openCamera(id, cb, handler); d.createCaptureSession(outputs, cb, handler); d.createCaptureRequest(CameraDevice.TEMPLATE_RECORD) }\nval a: CameraDevice.StateCallback? = null\nval b: CameraCaptureSession.StateCallback? = null\n' > "$_camera_fixture"; }
camera_allowed()       { camera_file; }
camera_unlisted()      { camera_file && printf 'import android.hardware.camera2.CaptureResult\n' >> "$_camera_fixture"; }
camera_spaced()        { camera_file && printf 'import  android . hardware . camera2 . TotalCaptureResult\n' >> "$_camera_fixture"; }
camera_wildcard()      { camera_file && printf 'import android.hardware.camera2.*\n' >> "$_camera_fixture"; }
camera_in_player()     { mkdir -p "$(dirname "$_player_camera")" && printf 'import android.hardware.camera2.CameraDevice\n' > "$_player_camera"; }
case_is selftest.camera-driver-allowed  0 ''                                  camera_allowed
case_is selftest.camera-driver-unlisted 1 sample.publisher.camera2           camera_unlisted
case_is selftest.camera-driver-spaced   1 sample.publisher.camera2           camera_spaced
case_is selftest.camera-driver-wildcard 1 sample.publisher.camera2           camera_wildcard
case_is selftest.camera-driver-player   1 sample.player.camera2              camera_in_player

case_is selftest.session-call-dropped  1 sample.player.session-wiring     drop_session_call
case_is selftest.session-call-comment  1 sample.player.session-wiring     session_call_comment
case_is selftest.session-call-string   1 sample.player.session-wiring     session_call_string
case_is selftest.session-timestamp     1 sample.publisher.session-wiring  drop_session_timestamp
case_is selftest.session-cadence       1 sample.publisher.session-wiring  derive_session_cadence
case_is selftest.session-cadence-comment 0 ''                             cadence_in_comment
case_is selftest.session-finish-dropped  1 sample.publisher.session-wiring  drop_session_finish
case_is selftest.session-direct-complete 1 sample.publisher.session-wiring  complete_directly
case_is selftest.session-direct-drain    1 sample.publisher.session-wiring  drain_directly
case_is selftest.session-finish-comment  0 ''                             finish_in_comment

case_is selftest.module        1 sample.player.module            drop_module
case_is selftest.settings      1 sample.player.settings          drop_include
case_is selftest.consumer      1 sample.player.consumer          drop_dependency
case_is selftest.sourcesets    1 sample.player.sourcesets        add_sourceset
case_is selftest.kotlin-werror 1 sample.player.kotlin-warnings   drop_kotlin_werr
case_is selftest.lint-werror   1 sample.player.lint-warnings     drop_lint_werr
case_is selftest.javac-werror  1 sample.player.javac-warnings    drop_javac_werr
case_is selftest.native        1 sample.player.no-native         add_native
case_is selftest.published     1 sample.player.unpublished       add_publishing
case_is selftest.sources       1 sample.player.sources           drop_sources
case_is selftest.engine        1 sample.player.private-packages  use_engine
case_is selftest.jni           1 sample.player.private-packages  use_jni
case_is selftest.c-header      1 sample.player.c-headers         use_c_header
case_is selftest.private-dso   1 sample.player.private-artefacts use_fixture_dso
case_is selftest.application   1 sample.player.application       drop_application
case_is selftest.coroutines-dep 1 sample.player.no-coroutines-dep  declare_coroutines
case_is selftest.coroutines-android 1 sample.player.no-coroutines-dep declare_coroutines_android
case_is selftest.main-dispatcher 1 sample.publisher.no-main-dispatcher use_main_dispatcher
case_is selftest.android-dispatcher 1 sample.player.no-main-dispatcher import_android_dispatcher
case_is selftest.main-in-comment 0 ''                               mention_main_in_comment
case_is selftest.cancellation-swallowed 1 sample.player.cancellation   swallow_cancellation
case_is selftest.cancellation-not-rethrown 1 sample.player.cancellation report_cancellation
case_is selftest.cancellation-recorded-then-rethrown 0 ''                record_then_rethrow
case_is selftest.cancellation-rethrows-another 1 sample.player.cancellation record_then_rethrow_other
case_is selftest.cancellation-extra-statement 1 sample.player.cancellation record_twice_then_rethrow
case_is selftest.cancellation-recorded-then-returned 1 sample.player.cancellation record_then_return
# These two exact player files have nonliteral but causal cancellation exits:
# one carries identity through a nested coroutine scope, and one disposes a
# partially acquired native resource before rethrow. Neither exception leaks
# to a same-shaped catch in another player file. The Sync's old report-and-
# rethrow behavior remains refused.
_reader_file="$W/root/samples/player/src/main/kotlin/org/openmoq/moq5/samples/player/PlaybackReceiveLoop.kt"
_owner_file="$W/root/samples/player/src/main/kotlin/org/openmoq/moq5/samples/player/PlaybackResourceOwner.kt"
reader_carrier() { printf 'fun read() { try { work() } catch (cancelled: CancellationException) { throw PlaybackWorkFailure(cancelled) } catch (raised: Exception) { throw raised } }\n' > "$_reader_file"; }
reader_elsewhere() { printf 'fun read() { try { work() } catch (cancelled: CancellationException) { throw PlaybackWorkFailure(cancelled) } catch (raised: Exception) { throw raised } }\n' > "$W/root/samples/player/src/main/kotlin/OtherRead.kt"; }
reader_wrong_cause() { reader_carrier && sed -i.bak 's/PlaybackWorkFailure(cancelled)/PlaybackWorkFailure(other)/' "$_reader_file" && rm -f "$_reader_file.bak"; }
owner_cleanup() { printf 'fun setup() { try { work() } catch (cancelled: CancellationException) { failed(cancelled) } catch (raised: Exception) { failed(raised) } }\n' > "$_owner_file"; }
owner_elsewhere() { printf 'fun setup() { try { work() } catch (cancelled: CancellationException) { failed(cancelled) } catch (raised: Exception) { failed(raised) } }\n' > "$W/root/samples/player/src/main/kotlin/OtherOwner.kt"; }
owner_wrong_cause() { owner_cleanup && sed -i.bak 's/failed(cancelled)/failed(other)/' "$_owner_file" && rm -f "$_owner_file.bak"; }
sync_reports_cancel() { printf 'fun queue() { try { work() } catch (cancelled: CancellationException) { onFailure(cancelled); throw cancelled } catch (raised: Exception) { onFailure(raised); throw raised } }\n' > "$W/root/samples/player/src/main/kotlin/PlaybackSync.kt"; }
case_is selftest.player-reader-carrier      0 ''                         reader_carrier
case_is selftest.player-reader-elsewhere    1 sample.player.cancellation reader_elsewhere
case_is selftest.player-reader-wrong-cause  1 sample.player.cancellation reader_wrong_cause
case_is selftest.player-owner-cleanup       0 ''                         owner_cleanup
case_is selftest.player-owner-elsewhere     1 sample.player.cancellation owner_elsewhere
case_is selftest.player-owner-wrong-cause   1 sample.player.cancellation owner_wrong_cause
case_is selftest.player-sync-report-cancel  1 sample.player.cancellation sync_reports_cancel
case_is selftest.cleanup-collector-accepted 0 ''                          cleanup_fixture
case_is selftest.collector-outside-cleanup 1 sample.player.cancellation   collector_outside_cleanup
case_is selftest.collector-after-cleanup-block 1 sample.player.cancellation collector_after_cleanup_block
case_is selftest.collector-unrelated-context 1 sample.player.cancellation collector_unrelated_context
case_is selftest.collector-other-list-in-cleanup 1 sample.player.cancellation collector_other_list_in_cleanup
case_is selftest.cancellation-none 0 ''                                no_generic_catch
case_is selftest.cancellation-order 1 sample.player.cancellation       wrong_catch_order
case_is selftest.cancellation-unrelated 1 sample.player.cancellation   unrelated_rethrow
case_is selftest.throwable-catch 1 sample.player.no-throwable-catch     catch_throwable
case_is selftest.throwable-qualified 1 sample.player.no-throwable-catch catch_qualified_throwable
case_is selftest.exception-qualified 1 sample.player.cancellation       catch_qualified_exception_unguarded
case_is selftest.qualified-correct 0 ''                                 catch_qualified_but_correct
case_is selftest.dep-compose   1 sample.player.unjustified-deps  add_dep_compose
case_is selftest.import-codec  1 sample.publisher.unjustified-imports add_import_codec
case_is selftest.root-check              1 sample.player.root-check  drop_root_check
case_is selftest.root-check-half         1 sample.player.root-check  half_root_check
case_is selftest.root-check-comments     1 sample.player.root-check  root_check_comments
case_is selftest.root-check-strings      1 sample.player.root-check  root_check_strings
case_is selftest.root-check-expected-2x  1 sample.player.root-check  root_check_expected_twice
case_is selftest.root-check-required-2x  1 sample.player.root-check  root_check_required_twice
case_is selftest.workflow-rx   1 sample.player.workflow          drop_subscribe
case_is selftest.workflow-tx   1 sample.publisher.workflow       drop_complete
case_is selftest.workflow-comment      1 sample.player.workflow  comment_only
case_is selftest.workflow-block        1 sample.player.workflow  block_comment_only
case_is selftest.workflow-string       1 sample.player.workflow  string_only
case_is selftest.workflow-rawstring    1 sample.player.workflow  raw_string_only

printf '\n'
if [ "$fails" -eq 0 ]; then
    printf 'sample-app gate self-test: 0 failure(s)\n'
    exit 0
fi
printf 'sample-app gate self-test: %s failure(s)\n' "$fails"
exit 1
