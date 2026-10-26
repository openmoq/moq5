#!/bin/sh
# The first-party Android sample applications, as a contract.
#
# The assessment calls these apps "an API acceptance test, not a marketing
# shell": they exist to prove the published surface is sufficient and pleasant
# from outside the library. That claim is only worth something if the samples
# are REAL CONSUMERS -- separate Gradle modules that see exactly what an
# application developer sees, with no private source set, no internal engine or
# JNI package, no C service header and no test fixture to lean on.
#
# So this gate judges four independent facts per sample:
#
#   TOPOLOGY   the module is registered in settings and has its own build
#              script under bindings/kotlin/samples/<name>;
#   CONSUMER   it depends on :moq5-android through an ordinary project
#              dependency, and on nothing private;
#   SURFACE    its sources name no internal package, no C service header and no
#              native fixture; it declares no dependency this slice has not
#              justified, no coroutines artifact of its own (the library exports
#              that one), it builds no scope on the Android main dispatcher, and
#              it does not report cancellation as a failure, and it never
#              catches Throwable -- each judged by the type's basename, so a
#              qualified spelling is the same fact;
#   WORKFLOW   its sources exercise the workflow the sample is FOR, named
#              symbol by symbol, so a sample that compiles but demonstrates
#              nothing cannot pass.
#
# The workflow rows are a static census of the public API each sample calls,
# taken over Kotlin source with COMMENTS AND STRING LITERALS REMOVED. Searching
# raw text would let a sample delete a call, leave a comment naming it, and
# still pass -- an oracle satisfied by its own documentation. The stripper
# below is what makes the census evidence.
#
# The census is still deliberately a weaker claim than "the workflow runs": the
# library's scripted backend is `internal` to :moq5-android, so a consumer
# module cannot drive a fake endpoint, and only a device or a relay can run the
# real one. What it does prove is that the sample cannot silently stop
# demonstrating the surface it was written to demonstrate.
set -u
LC_ALL=C; export LC_ALL

_self=${0##*/}
case $0 in
    */*) _selfdir=${0%/*} ;;
    *)   _selfdir=. ;;
esac
here=$(cd "$_selfdir" && pwd) || { echo "$_self: cannot resolve the script directory" >&2; exit 2; }
root=$(cd "$here/.." && pwd)  || { echo "$_self: cannot resolve bindings/kotlin" >&2; exit 2; }

for t in find grep printf sed sort awk mktemp rm; do
    command -v "$t" >/dev/null 2>&1 \
        || { echo "$_self: required tool not found: $t" >&2; exit 2; }
done

scratch=$(mktemp -d) || { echo "$_self: cannot create a scratch directory" >&2; exit 2; }
cleanup() { rm -rf -- "$scratch"; }
trap 'cleanup; exit 130' HUP INT TERM
trap cleanup EXIT

fails=0
pass() { printf 'PASS[%s]: %s\n' "$1" "$2"; }
fail() { fails=$((fails + 1)); printf 'FAIL[%s]: %s\n' "$1" "$2"; }

settings="$root/settings.gradle.kts"
[ -f "$settings" ] || { echo "$_self: no settings.gradle.kts under $root" >&2; exit 2; }

# The two samples the assessment names, and the Gradle path each must occupy.
SAMPLES='player publisher'

# Packages a consumer must never reach into. `internal` is a Kotlin module
# boundary, so a separate Gradle module already cannot compile against these --
# this row is what stops someone dissolving that boundary (a copied source set,
# a `sourceSets` addition, a same-package trick) without saying so.
PRIVATE_PACKAGES='org.openmoq.moq5.engine org.openmoq.moq5.jni'

# Dependencies and platform APIs this slice has not justified. 1493 requires
# each to be PROPOSED before it appears, so that a first sample stays buildable
# and dependency-light on the plain Android SDK. Judged in the build script AND
# in the sources, because an import is how one of these actually arrives.
# The coroutines artifacts a sample must NOT declare. The library exports
# kotlinx-coroutines-core as an `api` dependency, because its public surface
# returns StateFlow and suspends; a sample that declared it too would teach the
# wrong consumer contract and would keep passing if that export regressed to
# `implementation`. kotlinx-coroutines-android is barred outright: no slice has
# authorized the Android dispatcher dependency.
COROUTINE_ARTIFACTS='kotlinx-coroutines-core kotlinx-coroutines-android'

# `Dispatchers.Main` is DECLARED by coroutines-core and IMPLEMENTED by
# coroutines-android. A sample that builds a scope on it compiles and lints
# cleanly and then fails on a device, which is the worst shape a sample can
# have: green here, broken in the hand.
MAIN_DISPATCHER='Dispatchers.Main kotlinx.coroutines.android'

UNJUSTIFIED='androidx.compose
androidx.lifecycle
androidx.camera
androidx.media3
android.media.MediaCodec
android.media.MediaMuxer
okhttp
okhttp3
retrofit'

# camera2 is BOUNDED, not open. The publisher may read the metadata that names
# the capture clock; it may not open a camera or run a capture session, and the
# player -- which has no camera role -- may not touch camera2 at all.
CAMERA2_PACKAGE='android.hardware.camera2'
# CameraAccessException is LOOKUP PLUMBING: getCameraCharacteristics declares it,
# so naming a failure requires naming the exception. It opens nothing.
PUBLISHER_CAMERA2_ALLOWED='CameraCharacteristics CameraMetadata CameraManager CameraAccessException'
PUBLISHER_CAMERA2_REFUSED='CameraDevice CameraCaptureSession CameraCaptureSession.StateCallback openCamera createCaptureSession createCaptureRequest'
# ONE publisher file may open the camera and run its capture session: the
# Camera2 driver. It may name the metadata classes above plus exactly the device,
# the session and the request it builds; the refused calls are its to make.
# Every other publisher file stays under the metadata-only rule.
PUBLISHER_CAMERA_FILE='samples/publisher/src/main/kotlin/org/openmoq/moq5/samples/publisher/Camera2Capture.kt'
PUBLISHER_CAMERA_FILE_ALLOWED="$PUBLISHER_CAMERA2_ALLOWED CameraDevice CameraCaptureSession CaptureRequest"

# The platform encoder is allowed in ONE publisher file, by exact class: the
# encoder driver, which names the codec and its capability constants and
# nothing else of that family. Every other publisher file stays under the
# android.media.MediaCodec refusal; the player has its own exact files below.
PUBLISHER_ENCODER_FILE='samples/publisher/src/main/kotlin/org/openmoq/moq5/samples/publisher/MediaCodecEncoder.kt'
PUBLISHER_ENCODER_ALLOWED='android.media.MediaCodec android.media.MediaCodecInfo'
PUBLISHER_AUDIO_FILE='samples/publisher/src/main/kotlin/org/openmoq/moq5/samples/publisher/AudioRecordCapture.kt'
PLAYER_CODEC_FILES='samples/player/src/main/kotlin/org/openmoq/moq5/samples/player/AndroidPlaybackCodec.kt
samples/player/src/main/kotlin/org/openmoq/moq5/samples/player/AndroidPlaybackRender.kt'

# Class membership is role-specific. The separate codec census below also
# restricts WHERE MediaCodec may be named; this list alone grants no new file
# allowance. AudioRecord is reserved for the publisher's capture role.
PUBLISHER_MEDIA_CLASSES='AudioFormat AudioRecord AudioTimestamp MediaCodec MediaCodecInfo MediaFormat MediaRecorder'
PLAYER_MEDIA_CLASSES='AudioAttributes AudioFormat AudioTrack MediaCodec MediaFormat MediaSync PlaybackParams'


# The seam calls each SESSION file must make, in its own executable text.
#
# The module-wide workflow census above cannot prove this: the same names are
# DEFINED in the contract files, so a module-wide text search still passes when
# a session body stops calling them. These lists are path-aware for exactly
# that reason -- they are checked against one file each.
PLAYER_SESSION_FILE='samples/player/src/main/kotlin/org/openmoq/moq5/samples/player/PlaybackSession.kt'
PLAYER_SESSION_CALLS='MediaFacts DecoderFormats PlaybackDecode accept'

PUBLISHER_SESSION_FILE='samples/publisher/src/main/kotlin/org/openmoq/moq5/samples/publisher/BroadcastSession.kt'
# The session's own loop settles each write through writeOne, the live drain's
# exact-settlement helper (a PendingWrite, then accepted/abandoned on every
# outcome), so that is the settlement call the session file must make. It ends
# the broadcast through finishPublication over endpointCompletion -- the same
# media drain, completion and catalog drain the live coordinator uses.
PUBLISHER_SESSION_CALLS='Calibrator PlatformCaptureClock PublishPipeline step writeOne captureElapsedNanos finishPublication endpointCompletion'

# A session must not re-derive a capture instant from its own loop cadence: the
# encoder seam owns each output's capture timestamp, so a frame-interval
# constant has no business in this file's executable text at all. Nor may it
# complete or drain on its own: completing before the accepted media has
# drained discards it, so the order belongs to finishPublication alone.
PUBLISHER_SESSION_FORBIDDEN='FRAME_INTERVAL_US FRAME_INTERVAL_NANOS complete drain'

# Things a sample must not reach past the AAR to touch.
FORBIDDEN_INCLUDES='moq/endpoint.h moq/media_receiver.h moq/media_sender.h moq5_abi.h moq5_registry.h'
FORBIDDEN_ARTEFACTS='moq5_jni bridge-r8-fixture testfixtures .native/_bridge'

# The public API each sample must be seen to exercise, one symbol per line.
# These are the workflow, not a style guide: every one is a step the sample
# exists to demonstrate.
PLAYER_WORKFLOW='EndpointConfig
MoqEndpoint
attachReceiver
ReceiverConfig
OverflowPolicy
inventory
subscribe
objects
statsSnapshot
close'

PUBLISHER_WORKFLOW='EndpointConfig
MoqEndpoint
attachSender
SenderConfig
SendBackpressure
addTrack
SendTrackConfig
awaitReady
SendObject
endTrack
complete
drain
close'

# -- 1. topology --------------------------------------------------------------
for s in $SAMPLES; do
    dir="$root/samples/$s"
    if [ ! -d "$dir" ]; then
        fail "sample.$s.module" "no module directory at bindings/kotlin/samples/$s"
        continue
    fi
    if [ ! -f "$dir/build.gradle.kts" ]; then
        fail "sample.$s.module" "bindings/kotlin/samples/$s has no build.gradle.kts, so Gradle sees no module there"
    else
        pass "sample.$s.module" "bindings/kotlin/samples/$s is a Gradle module"
    fi
done

for s in $SAMPLES; do
    if grep -qE "^[[:space:]]*include\(\":samples:$s\"\)" "$settings"; then
        pass "sample.$s.settings" "settings.gradle.kts includes \":samples:$s\""
    else
        fail "sample.$s.settings" "settings.gradle.kts does not include \":samples:$s\"; an unregistered directory is not a module and is never built or gated"
    fi
done

# -- 2. consumer shape --------------------------------------------------------
for s in $SAMPLES; do
    b="$root/samples/$s/build.gradle.kts"
    [ -f "$b" ] || continue
    if grep -qE 'id\("com\.android\.application"\)' "$b"; then
        pass "sample.$s.application" "is an application module"
    else
        fail "sample.$s.application" "samples/$s/build.gradle.kts does not apply com.android.application; a library module cannot be installed or run, so it could not be an acceptance app"
    fi
    if grep -qE 'implementation\(project\("[:]moq5-android"\)\)' "$b"; then
        pass "sample.$s.consumer" "depends on :moq5-android through an ordinary project dependency"
    else
        fail "sample.$s.consumer" "samples/$s/build.gradle.kts does not declare implementation(project(\":moq5-android\")); a sample must consume the published surface, not be wired into the library"
    fi
    if grep -qE 'sourceSets|srcDir' "$b"; then
        fail "sample.$s.sourcesets" "samples/$s/build.gradle.kts manipulates source sets; a sample must not reach into the library's sources"
    else
        pass "sample.$s.sourcesets" "declares no extra source set"
    fi
    # Warnings are failures here for the same reason they are in the library:
    # org.gradle.warning.mode=fail governs Gradle's own warnings only.
    if grep -q 'allWarningsAsErrors.set(true)' "$b"; then
        pass "sample.$s.kotlin-warnings" "Kotlin warnings are errors"
    else
        fail "sample.$s.kotlin-warnings" "samples/$s/build.gradle.kts does not set allWarningsAsErrors"
    fi
    if grep -q 'warningsAsErrors = true' "$b" && grep -q 'abortOnError = true' "$b"; then
        pass "sample.$s.lint-warnings" "lint warnings are errors and lint aborts"
    else
        fail "sample.$s.lint-warnings" "samples/$s/build.gradle.kts does not make lint warnings errors with abortOnError"
    fi
    if grep -q '"-Werror"' "$b"; then
        pass "sample.$s.javac-warnings" "javac warnings are errors"
    else
        fail "sample.$s.javac-warnings" "samples/$s/build.gradle.kts does not pass -Werror to JavaCompile"
    fi
    _dep=""
    for u in $UNJUSTIFIED; do
        grep -qF -- "$u" "$b" && _dep="$_dep $u"
    done
    if [ -n "$_dep" ]; then
        fail "sample.$s.unjustified-deps" "samples/$s/build.gradle.kts declares unproposed dependency(ies):$_dep -- each must be proposed with a reason before it appears"
    else
        pass "sample.$s.unjustified-deps" "declares no unproposed dependency"
    fi
    if grep -qE 'externalNativeBuild|ndkVersion|CMakeLists' "$b"; then
        fail "sample.$s.no-native" "samples/$s/build.gradle.kts configures a native build; a sample consumes the packaged JNI image, it does not build one"
    else
        pass "sample.$s.no-native" "builds no native code of its own"
    fi
done

# strip_kotlin <file...>  -- emit the sources with every line comment, block
# comment (Kotlin block comments NEST; a .java file's do not, so there the first
# */ closes one), raw string or Java text block ("""..."""), string and
# character literal removed. What survives is the executable text, which is the
# only place a workflow symbol may count.
strip_kotlin() {
    awk '
    BEGIN { depth = 0; raw = 0 }
    {
        line = $0; out = ""; i = 1; n = length(line)
        while (i <= n) {
            c  = substr(line, i, 1)
            c2 = substr(line, i, 2)
            c3 = substr(line, i, 3)
            if (raw) {                       # inside """ ... """
                if (c3 == "\"\"\"") { raw = 0; i += 3 } else { i++ }
                continue
            }
            if (depth > 0) {                 # inside a block comment: Kotlin nests, Java does not
                if (c2 == "/*") { if (FILENAME !~ /\.java$/) depth++; i += 2; continue }
                if (c2 == "*/") { depth--; i += 2; continue }
                i++
                continue
            }
            if (c3 == "\"\"\"") { raw = 1; i += 3; continue }
            if (c2 == "/*")   { depth++; i += 2; continue }
            if (c2 == "//")   { break }      # the rest of the line is a comment
            if (c == "\"") {                 # a string literal
                i++
                while (i <= n) {
                    d = substr(line, i, 1)
                    if (d == "\\") { i += 2; continue }
                    if (d == "\"")  { i++; break }
                    i++
                }
                continue
            }
            if (c == "\047") {               # a character literal
                i++
                while (i <= n) {
                    d = substr(line, i, 1)
                    if (d == "\\") { i += 2; continue }
                    if (d == "\047") { i++; break }
                    i++
                }
                continue
            }
            out = out c
            i++
        }
        print out
    }' "$@"
}

# -- 3. surface ---------------------------------------------------------------
for s in $SAMPLES; do
    dir="$root/samples/$s"
    [ -d "$dir" ] || continue
    srcs=$(find "$dir/src" -name '*.kt' -o -name '*.java' 2>/dev/null | sort)
    if [ -z "$srcs" ]; then
        fail "sample.$s.sources" "samples/$s has no Kotlin or Java source"
        continue
    fi
    pass "sample.$s.sources" "samples/$s carries $(printf '%s\n' "$srcs" | grep -c .) source file(s)"

    _hit=""
    for p in $PRIVATE_PACKAGES; do
        # shellcheck disable=SC2086
        grep -l -- "$p" $srcs >/dev/null 2>&1 && _hit="$_hit $p"
    done
    if [ -n "$_hit" ]; then
        fail "sample.$s.private-packages" "samples/$s names the library's internal package(s):$_hit"
    else
        pass "sample.$s.private-packages" "names no internal engine or JNI package"
    fi

    # The source row judges executable text: a name in a comment or a string is
    # not a use, and the spacing the grammar permits around a dot is the same
    # qualified name -- the projection the media and camera2 censuses read. Each
    # file is projected to its own copy, so the per-file scopes below still name
    # the file they exempt.
    _code="$scratch/$s.unjustified"
    rm -rf -- "$_code"
    _stripped=yes
    for _src in $srcs; do
        _rel=${_src#"$root"/}
        mkdir -p "$_code/$(dirname "$_rel")" \
            && strip_kotlin "$_src" > "$_code/$_rel.stripped" 2>/dev/null \
            && sed -E 's/[[:space:]]*\.[[:space:]]*/./g' "$_code/$_rel.stripped" > "$_code/$_rel" \
            || _stripped=no
    done
    _imp=""
    [ "$_stripped" = yes ] && for u in $UNJUSTIFIED; do
        _scope=$srcs
        # The encoder driver's own allowance is audited by class below.
        if [ "$s" = publisher ] && [ "$u" = android.media.MediaCodec ]; then
            _scope=$(printf '%s\n' "$srcs" | grep -vxF -- "$root/$PUBLISHER_ENCODER_FILE")
        fi
        if [ "$s" = player ] && [ "$u" = android.media.MediaCodec ]; then
            for _allowed in $PLAYER_CODEC_FILES; do
                _scope=$(printf '%s\n' "$_scope" | grep -vxF -- "$root/$_allowed")
            done
        fi
        for _src in $_scope; do
            if grep -q -F -- "$u" "$_code/${_src#"$root"/}"; then
                _imp="$_imp $u"
                break
            fi
        done
    done
    if [ "$_stripped" = no ]; then
        fail "sample.$s.unjustified-imports" "samples/$s sources could not be stripped of comments and literals"
    elif [ -n "$_imp" ]; then
        fail "sample.$s.unjustified-imports" "samples/$s imports unproposed platform or library API:$_imp"
    else
        pass "sample.$s.unjustified-imports" "imports no unproposed platform or library API"
    fi

    _inc=""
    for h in $FORBIDDEN_INCLUDES; do
        # shellcheck disable=SC2086
        grep -l -- "$h" $srcs >/dev/null 2>&1 && _inc="$_inc $h"
    done
    if [ -n "$_inc" ]; then
        fail "sample.$s.c-headers" "samples/$s names C service header(s):$_inc"
    else
        pass "sample.$s.c-headers" "names no C service header"
    fi

    _art=""
    for a in $FORBIDDEN_ARTEFACTS; do
        # shellcheck disable=SC2086
        grep -l -- "$a" $srcs >/dev/null 2>&1 && _art="$_art $a"
    done
    if [ -n "$_art" ]; then
        fail "sample.$s.private-artefacts" "samples/$s names a private native or fixture artefact:$_art"
    else
        pass "sample.$s.private-artefacts" "loads no private native image and names no test fixture"
    fi
done

# -- 4. workflow census, over comment-free and string-free source -------------
#

# A top-level class census complements the file-scoped codec rules. A wildcard
# has no class to classify, so it is refused before the per-class comparison.
media_census() {
    _s=$1
    if [ "$_s" = publisher ]; then _allowed=$PUBLISHER_MEDIA_CLASSES; else _allowed=$PLAYER_MEDIA_CLASSES; fi
    dir="$root/samples/$_s"
    srcs=$(find "$dir/src" \( -name '*.kt' -o -name '*.java' \) 2>/dev/null | sort)
    [ -n "$srcs" ] || { fail "sample.$_s.media" "no Kotlin source to audit"; return; }
    # shellcheck disable=SC2086
    strip_kotlin $srcs 2>/dev/null | sed -E 's/[[:space:]]*\.[[:space:]]*/./g' > "$scratch/$_s.media" || {
        fail "sample.$_s.media" "samples/$_s sources could not be stripped of comments and literals"
        return
    }
    if grep -qE '^[[:space:]]*import[[:space:]]+android\.media\.\*[[:space:]]*;?[[:space:]]*$' "$scratch/$_s.media"; then
        fail "sample.$_s.media" "samples/$_s imports android.media.* ; the class allowance is exact"
        return
    fi
    _unknown=""
    for _token in $(grep -oE 'android\.media\.[A-Za-z_][A-Za-z0-9_]*' "$scratch/$_s.media" | sort -u); do
        _class=${_token##*.}
        _ok=no
        for _member in $_allowed; do
            [ "$_class" = "$_member" ] && _ok=yes
        done
        [ "$_ok" = no ] && _unknown="$_unknown $_class"
    done
    if [ -n "$_unknown" ]; then
        fail "sample.$_s.media" "samples/$_s names android.media class(es) outside its role:$_unknown"
    else
        pass "sample.$_s.media" "names only declared android.media classes, with no wildcard import"
    fi
}
media_census player
media_census publisher

# Capturing PCM is a platform-adapter responsibility, not a permission for a
# session or codec-independent contract file to own AudioRecord directly.
publisher_audio_record_census() {
    _bad=""
    _srcs=$(find "$root/samples/publisher/src" \( -name '*.kt' -o -name '*.java' \) 2>/dev/null | sort)
    [ -n "$_srcs" ] || { fail "sample.publisher.audio-record-file" "no publisher Kotlin source to audit"; return; }
    for _file in $_srcs; do
        [ "$_file" = "$root/$PUBLISHER_AUDIO_FILE" ] && continue
        strip_kotlin "$_file" 2>/dev/null | sed -E 's/[[:space:]]*\.[[:space:]]*/./g' > "$scratch/audio-record" || {
            fail "sample.publisher.audio-record-file" "$_file could not be stripped of comments and literals"
            return
        }
        if grep -qE 'android\.media\.AudioRecord([^[:alnum:]_]|$)' "$scratch/audio-record"; then
            _bad="$_bad ${_file#"$root/"}"
        fi
    done
    if [ -n "$_bad" ]; then
        fail "sample.publisher.audio-record-file" "AudioRecord is named outside $PUBLISHER_AUDIO_FILE:$_bad"
    else
        pass "sample.publisher.audio-record-file" "only the AudioRecord platform adapter may name AudioRecord"
    fi
}
publisher_audio_record_census

census() {
    _s=$1
    _want=$2
    dir="$root/samples/$_s"
    [ -d "$dir" ] || { fail "sample.$_s.workflow" "no module to audit"; return; }
    srcs=$(find "$dir/src" -name '*.kt' 2>/dev/null | sort)
    if [ -z "$srcs" ]; then
        fail "sample.$_s.workflow" "no Kotlin source to audit"
        return
    fi
    # shellcheck disable=SC2086
    strip_kotlin $srcs > "$scratch/$_s.code" 2>/dev/null || {
        fail "sample.$_s.workflow" "samples/$_s sources could not be stripped of comments and literals"
        return
    }
    _missing=""
    for sym in $_want; do
        grep -qE "\\b$sym\\b" "$scratch/$_s.code" || _missing="$_missing $sym"
    done
    if [ -n "$_missing" ]; then
        fail "sample.$_s.workflow" "samples/$_s does not exercise (in executable source, not comments or strings):$_missing"
    else
        pass "sample.$_s.workflow" "exercises every step of the $_s workflow in executable source"
    fi
}
census player "$PLAYER_WORKFLOW"
census publisher "$PUBLISHER_WORKFLOW"

# -- 4b. the session-wiring census (one FILE each) ----------------------------
#
# The sessions are the code path the apps run. The publisher's finite publish
# loop (publishFinite, in the session file) is unit tested directly; what a
# sample module cannot test is the native-connected orchestration around it --
# MediaReceiver and MediaSender have internal constructors. This census keeps
# "the session file still names the accepted seams" true. It is a source-token
# check: a name present in the file is not proof the session invokes it.
session_census() {
    _s=$1
    _file=$2
    _want=$3
    _forbid=$4
    path="$root/$_file"
    [ -f "$path" ] || { fail "sample.$_s.session-wiring" "$_file is missing"; return; }
    strip_kotlin "$path" > "$scratch/$_s.session" 2>/dev/null || {
        fail "sample.$_s.session-wiring" "$_file could not be stripped of comments and literals"
        return
    }
    _missing=""
    for sym in $_want; do
        grep -qE "\\b$sym\\b" "$scratch/$_s.session" || _missing="$_missing $sym"
    done
    _present=""
    for sym in $_forbid; do
        grep -qE "\\b$sym\\b" "$scratch/$_s.session" && _present="$_present $sym"
    done
    if [ -n "$_missing" ]; then
        fail "sample.$_s.session-wiring" "$_file no longer calls (in executable source):$_missing"
    elif [ -n "$_present" ]; then
        fail "sample.$_s.session-wiring" "$_file derives what a seam owns:$_present"
    else
        pass "sample.$_s.session-wiring" "$_file routes through the accepted contracts"
    fi
}
# -- 4c. the bounded camera2 rule --------------------------------------------
camera2_census() {
    _s=$1
    dir="$root/samples/$_s"
    srcs=$(find "$dir/src" \( -name '*.kt' -o -name '*.java' \) 2>/dev/null | sort)
    if [ -z "$srcs" ]; then
        fail "sample.$_s.camera2" "no Kotlin source to audit"
        return
    fi
    # shellcheck disable=SC2086
    strip_kotlin $srcs > "$scratch/$_s.camera2" 2>/dev/null || {
        fail "sample.$_s.camera2" "samples/$_s sources could not be stripped of comments and literals"
        return
    }
    # A wildcard import names no class: a per-class scan sees nothing, and the
    # unqualified uses it enables cannot be attributed to the package. An exact
    # allow-list cannot be enforced through one, so the DIRECTIVE is refused --
    # matched as an import at the start of a line, in any grammar-permitted
    # spacing, so the same token elsewhere is not mistaken for one.
    if grep -qE "^[[:space:]]*import[[:space:]]+android[[:space:]]*\\.[[:space:]]*hardware[[:space:]]*\\.[[:space:]]*camera2[[:space:]]*\\.[[:space:]]*\\*[[:space:]]*;?[[:space:]]*$" "$scratch/$_s.camera2"; then
        fail "sample.$_s.camera2" "samples/$_s imports $CAMERA2_PACKAGE.* ; the allowance is exact, so a wildcard import is refused"
        return
    fi
    if [ "$_s" = player ]; then
        if grep -qF "$CAMERA2_PACKAGE" "$scratch/$_s.camera2"; then
            fail "sample.$_s.camera2" "samples/$_s names $CAMERA2_PACKAGE; the player has no camera role"
        else
            pass "sample.$_s.camera2" "names no camera2 API"
        fi
        return
    fi
    # The publisher: every file but the Camera2 driver reads metadata only, with
    # no session or open call; the driver names exactly its own allowance.
    _driver="$root/$PUBLISHER_CAMERA_FILE"
    _rest=$(printf '%s\n' "$srcs" | grep -vxF -- "$_driver")
    : > "$scratch/$_s.camera2.rest"
    if [ -n "$_rest" ]; then
        # shellcheck disable=SC2086
        strip_kotlin $_rest > "$scratch/$_s.camera2.rest" 2>/dev/null || {
            fail "sample.$_s.camera2" "samples/$_s sources could not be stripped of comments and literals"
            return
        }
    fi
    : > "$scratch/$_s.camera2.driver"
    if [ -f "$_driver" ]; then
        strip_kotlin "$_driver" 2>/dev/null | sed -E 's/[[:space:]]*\.[[:space:]]*/./g' > "$scratch/$_s.camera2.driver" || {
            fail "sample.$_s.camera2" "$PUBLISHER_CAMERA_FILE could not be stripped of comments and literals"
            return
        }
    fi
    _bad=""
    for sym in $PUBLISHER_CAMERA2_REFUSED; do
        grep -qE "\\b$sym\\b" "$scratch/$_s.camera2.rest" && _bad="$_bad $sym"
    done
    _unknown=""
    for line in $(grep -oE "$CAMERA2_PACKAGE\.[A-Za-z_][A-Za-z0-9_]*" "$scratch/$_s.camera2.rest" | sort -u); do
        _cls=${line##*.}
        _ok=no
        for allowed in $PUBLISHER_CAMERA2_ALLOWED; do
            [ "$_cls" = "$allowed" ] && _ok=yes
        done
        [ "$_ok" = no ] && _unknown="$_unknown $_cls"
    done
    _driver_unknown=""
    for line in $(grep -oE "$CAMERA2_PACKAGE\.[A-Za-z_][A-Za-z0-9_]*" "$scratch/$_s.camera2.driver" | sort -u); do
        _cls=${line##*.}
        _ok=no
        for allowed in $PUBLISHER_CAMERA_FILE_ALLOWED; do
            [ "$_cls" = "$allowed" ] && _ok=yes
        done
        [ "$_ok" = no ] && _driver_unknown="$_driver_unknown $_cls"
    done
    if [ -n "$_bad" ]; then
        fail "sample.$_s.camera2" "samples/$_s uses a camera session or open call outside $PUBLISHER_CAMERA_FILE:$_bad"
    elif [ -n "$_unknown" ]; then
        fail "sample.$_s.camera2" "samples/$_s imports camera2 class(es) outside the metadata allowance:$_unknown"
    elif [ -n "$_driver_unknown" ]; then
        fail "sample.$_s.camera2" "$PUBLISHER_CAMERA_FILE imports camera2 class(es) outside its allowance:$_driver_unknown"
    else
        pass "sample.$_s.camera2" "reads camera2 metadata only, except the Camera2 driver's exact allowance"
    fi
}
# The publisher's encoder driver: the exact platform-encoder classes it may
# name, read from its code only, with the spacing the grammar permits removed
# around each dot. A wildcard names no class, so it is refused outright.
encoder_census() {
    _f="$root/$PUBLISHER_ENCODER_FILE"
    if [ ! -f "$_f" ]; then
        pass "sample.publisher.encoder-imports" "no encoder driver names the platform encoder"
        return
    fi
    strip_kotlin "$_f" 2>/dev/null | sed -E 's/[[:space:]]*\.[[:space:]]*/./g' > "$scratch/encoder" || {
        fail "sample.publisher.encoder-imports" "$PUBLISHER_ENCODER_FILE could not be stripped of comments and literals"
        return
    }
    if grep -qE '^[[:space:]]*import[[:space:]]+android\.media\.\*' "$scratch/encoder"; then
        fail "sample.publisher.encoder-imports" "$PUBLISHER_ENCODER_FILE imports android.media.* ; the allowance is exact, so a wildcard import is refused"
        return
    fi
    _unknown=""
    for _tok in $(grep -oE 'android\.media\.MediaCodec[A-Za-z0-9_.]*' "$scratch/encoder" | sort -u); do
        _ok=no
        for allowed in $PUBLISHER_ENCODER_ALLOWED; do
            [ "$_tok" = "$allowed" ] && _ok=yes
        done
        [ "$_ok" = no ] && _unknown="$_unknown $_tok"
    done
    if [ -n "$_unknown" ]; then
        fail "sample.publisher.encoder-imports" "$PUBLISHER_ENCODER_FILE names platform-encoder API outside its allowance:$_unknown"
    else
        pass "sample.publisher.encoder-imports" "the encoder driver names only $PUBLISHER_ENCODER_ALLOWED"
    fi
}
encoder_census

# The decoder may use MediaCodec only in the two platform adapter files. A
# third player file, a wildcard, or another MediaCodec-family class is not an
# implicit consequence of allowing those exact imports.
player_codec_census() {
    _unknown=""
    for _rel in $PLAYER_CODEC_FILES; do
        _f="$root/$_rel"
        [ -f "$_f" ] || continue
        strip_kotlin "$_f" 2>/dev/null | sed -E 's/[[:space:]]*\.[[:space:]]*/./g' > "$scratch/player-codec" || {
            fail "sample.player.codec-imports" "$_rel could not be stripped of comments and literals"
            return
        }
        if grep -qE '^[[:space:]]*import[[:space:]]+android\.media\.\*' "$scratch/player-codec"; then
            fail "sample.player.codec-imports" "$_rel imports android.media.* ; the player codec allowance is exact"
            return
        fi
        for _tok in $(grep -oE 'android\.media\.MediaCodec[A-Za-z0-9_.]*' "$scratch/player-codec" | sort -u); do
            [ "$_tok" = android.media.MediaCodec ] || _unknown="$_unknown $_rel:$_tok"
        done
    done
    if [ -n "$_unknown" ]; then
        fail "sample.player.codec-imports" "player platform files name MediaCodec API outside their exact allowance:$_unknown"
    else
        pass "sample.player.codec-imports" "the two player platform files name only android.media.MediaCodec"
    fi
}
player_codec_census

camera2_census player
camera2_census publisher

session_census player "$PLAYER_SESSION_FILE" "$PLAYER_SESSION_CALLS" ""
session_census publisher "$PUBLISHER_SESSION_FILE" "$PUBLISHER_SESSION_CALLS" "$PUBLISHER_SESSION_FORBIDDEN"

# -- 5. the coroutines contract -----------------------------------------------
for s in $SAMPLES; do
    b="$root/samples/$s/build.gradle.kts"
    [ -f "$b" ] || continue
    _c=""
    for a in $COROUTINE_ARTIFACTS; do
        grep -qF -- "$a" "$b" && _c="$_c $a"
    done
    if [ -n "$_c" ]; then
        fail "sample.$s.no-coroutines-dep" "samples/$s/build.gradle.kts declares:$_c -- the library exports kotlinx-coroutines-core as an \`api\` dependency, so a sample USES coroutine APIs without declaring the artifact; declaring it hides a regression of that export"
    else
        pass "sample.$s.no-coroutines-dep" "declares no coroutines artifact of its own"
    fi

    dir="$root/samples/$s"
    srcs=$(find "$dir/src" -name '*.kt' 2>/dev/null | sort)
    [ -n "$srcs" ] || continue
    # Read CODE, so a comment explaining why the Main dispatcher is avoided
    # does not trip the row that forbids using it.
    # shellcheck disable=SC2086
    strip_kotlin $srcs > "$scratch/$s.nomain" 2>/dev/null || {
        fail "sample.$s.no-main-dispatcher" "samples/$s sources could not be stripped of comments and literals"
        continue
    }
    _m=""
    for d in $MAIN_DISPATCHER; do
        grep -qF -- "$d" "$scratch/$s.nomain" && _m="$_m $d"
    done
    if [ -n "$_m" ]; then
        fail "sample.$s.no-main-dispatcher" "samples/$s uses:$_m -- Dispatchers.Main is declared by coroutines-core but IMPLEMENTED by coroutines-android, which no slice has authorized, so this would pass every host gate and fail on a device"
    else
        pass "sample.$s.no-main-dispatcher" "builds no scope on the Android main dispatcher"
    fi
done

# -- 6. cancellation is not a failure, and Throwable is never caught ----------
#
# `CancellationException` IS an `Exception`, so a bare `catch (e: Exception)`
# around a session body reports an ordinary Stop -- or Activity teardown -- as
# a failure, AND swallows the signal structured concurrency runs on. `Throwable`
# is the broader swallow: it takes cancellation too, plus the errors
# application code has no business handling. A sample that taught either shape
# would teach it to every reader.
#
# ONE parser answers both rows, so they cannot drift apart, and it classifies
# every catch by the type's BASENAME: `kotlin.Throwable`, `java.lang.Exception`
# and `kotlinx.coroutines.CancellationException` are the same facts as their
# bare spellings, and a row that knew only the bare one would be bypassed by
# writing the qualified name (chats/libmoq/1509).
#
# It reads CODE in ORDER (comments and literals stripped first, so prose about
# cancellation can neither satisfy nor trip it), and requires every generic
# `catch (x: Exception)` to be IMMEDIATELY PRECEDED by a sibling
# `catch (y: CancellationException) { throw y }`. Counting occurrences is not
# enough: it would accept a cancellation catch written after the generic one --
# unreachable, since Kotlin matches clauses in source order -- or an unrelated
# rethrow elsewhere in the file (chats/libmoq/1505).
#
# catch_audit <stripped-file> <sample> <relative-path> -- emits zero or more `THROWABLE <name>` lines,
# then exactly one `CANCEL none|ok|<reason>` line.
catch_audit() {
    awk -v sample="$2" -v file="$3" '
    { src = src $0 "\n" }
    END {
        gsub(/[ \t\n]+/, " ", src)
        # THE CLEANUP REGIONS. A collecting cancellation arm is accepted only
        # INSIDE `withContext(NonCancellable) { ... }`, so each such span is
        # measured first by counting braces from that opening brace to its
        # match. Everything outside every span is an ordinary body, where the
        # collecting shape stays refused (ruling 1752 F1). Bounded brace
        # tracking over already-stripped source, not a Kotlin parser.
        regions = 0
        rpos = 1
        while (match(substr(src, rpos), /withContext *\( *NonCancellable *\) *\{/)) {
            rstart = rpos + RSTART - 1
            ropen = rstart + RLENGTH - 1        # the "{" itself
            depth = 0
            for (i = ropen; i <= length(src); i++) {
                c = substr(src, i, 1)
                if (c == "{") depth++
                else if (c == "}") {
                    depth--
                    if (depth == 0) break
                }
            }
            regions++
            rbeg[regions] = ropen
            rend[regions] = i
            rpos = ropen + 1
        }
        # A catch clause, with an optionally QUALIFIED type.
        clause = "catch *\\( *[A-Za-z_][A-Za-z0-9_]* *: *[A-Za-z_][A-Za-z0-9_.]* *\\)"
        pos = 1; prev_base = ""; prev_name = ""; prev_end = 0
        generic = 0; verdict = ""
        while (match(substr(src, pos), clause)) {
            start = pos + RSTART - 1
            end   = start + RLENGTH - 1
            text  = substr(src, start, RLENGTH)

            name = text; sub(/^catch *\( */, "", name); sub(/ *:.*$/, "", name)
            type = text; sub(/^.*: */, "", type);       sub(/ *\).*$/, "", type)
            base = type; sub(/^.*\./, "", base)          # kotlin.Throwable -> Throwable

            if (base == "Throwable") print "THROWABLE " name

            if (base == "Exception") {
                generic++
                if (verdict == "") {
                    if (prev_base != "CancellationException") {
                        verdict = "a generic catch (" name ": " type ") is not immediately preceded by a CancellationException catch"
                    } else {
                        between = substr(src, prev_end + 1, start - prev_end - 1)
                        gsub(/^ +| +$/, "", between)
                        # The arm may RECORD the caught instance in one
                        # simple local slot before rethrowing THAT SAME
                        # instance, because correct cleanup has to attach its
                        # own failures to the actual cancellation rather than
                        # infer one from Job state (ruling 1748 F2). Everything
                        # else stays refused: a swallowed cancellation, a
                        # rethrow of some other value, a `return`, any second
                        # statement, and a generic catch with no cancellation
                        # arm before it.
                        bare = "{ throw " prev_name " }"
                        recorded = "^\\{ [A-Za-z_][A-Za-z0-9_]* = " prev_name ";? throw " prev_name " \\}$"
                        # THE CLEANUP COLLECTOR (ruling 1750 F1). A close may
                        # raise its own cancellation, and abandoning the
                        # remaining resources or replacing an established
                        # primary would be wrong. So a cancellation arm may
                        # COLLECT into a list -- but ONLY when the generic arm
                        # immediately after it collects the same way into the
                        # SAME list. That pairing is what distinguishes an
                        # explicit cleanup collector from swallowing
                        # cancellation in an arbitrary body: anywhere else, a
                        # collecting arm still fails this row.
                        collector = "^\\{ ([A-Za-z_][A-Za-z0-9_]*)\\.add\\(" prev_name "\\) \\}$"
                        incleanup = 0
                        for (r = 1; r <= regions; r++) {
                            if (start > rbeg[r] && end < rend[r]) incleanup = 1
                        }
                        paired = 0
                        if (incleanup == 1 && between ~ collector) {
                            slot = between
                            sub(/^\{ */, "", slot); sub(/\.add\(.*$/, "", slot)
                            rest = substr(src, end + 1)
                            want = "^ *\\{ " slot "\\.add\\(" name "\\) \\}"
                            if (rest ~ want) paired = 1
                        }
                        # The player reader carries the original cancellation
                        # through a nested scope as data; its outer boundary
                        # unwraps and rethrows that same object. Resource
                        # setup instead disposes its partially acquired native
                        # handle before its Nothing-returning helper rethrows
                        # the original. These are exact file/body exceptions,
                        # not permissions to report cancellation as failure.
                        reader_carrier = sample == "player" && file == "samples/player/src/main/kotlin/org/openmoq/moq5/samples/player/PlaybackReceiveLoop.kt" && generic == 1 && between == "{ throw PlaybackWorkFailure(" prev_name ") }"
                        resource_cleanup = sample == "player" && file == "samples/player/src/main/kotlin/org/openmoq/moq5/samples/player/PlaybackResourceOwner.kt" && generic == 1 && between == "{ failed(" prev_name ") }"
                        if (paired == 0 && between != bare && between !~ recorded && !reader_carrier && !resource_cleanup) {
                            verdict = "the CancellationException catch before catch (" name ": " type ") has body \"" between "\", not \"{ throw " prev_name " }\" or \"{ <slot> = " prev_name " throw " prev_name " }\", and is not a paired collector inside a withContext(NonCancellable) cleanup block"
                        }
                    }
                }
            }
            prev_base = base; prev_name = name; prev_end = end
            pos = end + 1
        }
        if (verdict != "") print "CANCEL " verdict
        else if (generic == 0) print "CANCEL none"
        else print "CANCEL ok"
    }' "$1"
}

for s in $SAMPLES; do
    dir="$root/samples/$s"
    [ -d "$dir" ] || continue
    srcs=$(find "$dir/src" -name '*.kt' 2>/dev/null | sort)
    [ -n "$srcs" ] || continue
    _thr=""
    _bad=""
    _guarded=0
    for f in $srcs; do
        strip_kotlin "$f" > "$scratch/one.code" 2>/dev/null || {
            _thr="$_thr ${f##*/}(unreadable)"
            _bad="$_bad ${f##*/}(unreadable)"
            continue
        }
        catch_audit "$scratch/one.code" "$s" "${f#"$root"/}" > "$scratch/one.audit"
        grep -q '^THROWABLE ' "$scratch/one.audit" && _thr="$_thr ${f##*/}"
        _v=$(sed -n 's/^CANCEL //p' "$scratch/one.audit")
        case $_v in
            ok)   _guarded=$((_guarded + 1)) ;;
            none) : ;;
            *)    _bad="$_bad ${f##*/}: $_v;" ;;
        esac
    done

    # Its OWN row: a Throwable catch would satisfy the cancellation rule
    # vacuously -- with no `catch (...: Exception)` left, that row answers
    # "nothing to guard" -- while swallowing cancellation just the same
    # (chats/libmoq/1507).
    if [ -n "$_thr" ]; then
        fail "sample.$s.no-throwable-catch" "samples/$s catches Throwable in:$_thr -- a sample must not catch Throwable (however it is spelled: Throwable, kotlin.Throwable, java.lang.Throwable): it swallows coroutine cancellation and the errors application code should never handle. Show ordinary failures through the guarded Exception catch instead"
    else
        pass "sample.$s.no-throwable-catch" "catches no Throwable, under any spelling"
    fi

    if [ -n "$_bad" ]; then
        fail "sample.$s.cancellation" "samples/$s reports cancellation as a failure --$_bad Stop and Activity teardown arrive as CancellationException, which must be caught immediately before the generic catch and rethrown, not reported"
    elif [ "$_guarded" -eq 0 ]; then
        pass "sample.$s.cancellation" "no generic Exception catch to guard"
    else
        pass "sample.$s.cancellation" "every generic Exception catch has a proved cancellation path: direct rethrow, the player's exact identity-preserving reader/cleanup path, or a paired NonCancellable cleanup collector ($_guarded file(s))"
    fi
done

# -- 7. the samples are not published -----------------------------------------
# A sample is an acceptance test, never an artifact. The library's own
# inventory gate judges the AAR; this row is the declaration-level twin.
for s in $SAMPLES; do
    b="$root/samples/$s/build.gradle.kts"
    [ -f "$b" ] || continue
    if grep -qE 'maven-publish|publishing \{' "$b"; then
        fail "sample.$s.unpublished" "samples/$s configures publishing; a sample is an acceptance app, not an artifact"
    else
        pass "sample.$s.unpublished" "publishes nothing"
    fi
done

# -- 8. the root check's module boundary --------------------------------------
#
# The root deliberately names every module `check` it requires rather than
# leaning on Gradle's unqualified task-name expansion, and asserts that list
# against an INDEPENDENT literal. A sample module missing from either place is
# built but never verified by the canonical gate.
#
# Counting occurrences of the literal in the file would be the same inert-text
# oracle the workflow census had: two comments, or two unrelated strings, would
# satisfy it while both real lists had lost the value. So this reads the two
# lists themselves.
#
# list_members <file> <val-name> <constructor>
#   Emits one string member per line, from THAT declaration's balanced argument
#   list only. Comments are removed first (strings are kept, since the members
#   ARE strings), so nothing outside the selected block can contribute.
list_members() {
    awk -v valname="$2" -v ctor="$3" '
    { src = src $0 "\n" }
    END {
        # 1. strip comments, preserving string literals.
        n = length(src); i = 1; out = ""; depth = 0
        while (i <= n) {
            c  = substr(src, i, 1); c2 = substr(src, i, 2); c3 = substr(src, i, 3)
            if (depth > 0) {
                if (c2 == "/*") { depth++; i += 2; continue }
                if (c2 == "*/") { depth--; i += 2; continue }
                i++; continue
            }
            if (c3 == "\"\"\"") {
                out = out c3; i += 3
                while (i <= n) {
                    if (substr(src, i, 3) == "\"\"\"") { out = out "\"\"\""; i += 3; break }
                    out = out substr(src, i, 1); i++
                }
                continue
            }
            if (c2 == "/*") { depth++; i += 2; continue }
            if (c2 == "//") { while (i <= n && substr(src, i, 1) != "\n") i++; continue }
            if (c == "\"") {
                out = out c; i++
                while (i <= n) {
                    d = substr(src, i, 1)
                    if (d == "\\") { out = out substr(src, i, 2); i += 2; continue }
                    out = out d; i++
                    if (d == "\"") break
                }
                continue
            }
            out = out c; i++
        }

        # 2. locate `val <valname>` and, after it, the constructor call.
        k = index(out, "val " valname)
        if (k == 0) exit 0
        rest = substr(out, k)
        j = index(rest, ctor "(")
        if (j == 0) exit 0
        rest = substr(rest, j + length(ctor))       # now starts at "("

        # 3. take the BALANCED argument list, ignoring parens inside strings.
        m = length(rest); i = 1; d = 0; body = ""
        while (i <= m) {
            c = substr(rest, i, 1)
            if (c == "\"") {                        # copy a whole string literal
                body = body c; i++
                while (i <= m) {
                    e = substr(rest, i, 1)
                    if (e == "\\") { body = body substr(rest, i, 2); i += 2; continue }
                    body = body e; i++
                    if (e == "\"") break
                }
                continue
            }
            if (c == "(") { d++; i++; if (d == 1) continue; body = body c; continue }
            if (c == ")") { d--; i++; if (d == 0) break; body = body c; continue }
            body = body c; i++
        }
        if (d != 0) exit 0                          # unbalanced: no members

        # 4. emit every string literal of that block, one per line.
        m = length(body); i = 1
        while (i <= m) {
            c = substr(body, i, 1)
            if (c != "\"") { i++; continue }
            i++; lit = ""
            while (i <= m) {
                e = substr(body, i, 1)
                if (e == "\\") { lit = lit substr(body, i + 1, 1); i += 2; continue }
                if (e == "\"") { i++; break }
                lit = lit e; i++
            }
            print lit
        }
    }' "$1"
}

rootbuild="$root/build.gradle.kts"
for s in $SAMPLES; do
    _task=":samples:$s:check"
    _exp=$(list_members "$rootbuild" expectedModuleVerificationTasks setOf | grep -c -x -- "$_task")
    _req=$(list_members "$rootbuild" requiredModuleVerificationTasks listOf | grep -c -x -- "$_task")
    _miss=""
    [ "${_exp:-0}" -ge 1 ] || _miss="$_miss expectedModuleVerificationTasks"
    [ "${_req:-0}" -ge 1 ] || _miss="$_miss requiredModuleVerificationTasks"
    if [ -z "$_miss" ]; then
        pass "sample.$s.root-check" "\"$_task\" is a member of both expectedModuleVerificationTasks and requiredModuleVerificationTasks"
    else
        fail "sample.$s.root-check" "\"$_task\" is not a member of:$_miss (found $_exp in the expected set, $_req in the configured list); the root's module verification boundary must carry it in BOTH, or the sample is built and never verified"
    fi
done

printf '\n'
if [ "$fails" -eq 0 ]; then
    printf 'android sample apps: 0 failure(s)\n'
    exit 0
fi
printf 'android sample apps: %s failure(s)\n' "$fails"
exit 1
