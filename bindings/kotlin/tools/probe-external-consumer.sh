#!/bin/sh
# The external-consumer probe: can a stranger use this artifact?
#
# Every other publication row reads something we wrote. This one builds
# something we did not: a SEPARATE Gradle root, outside this build's settings,
# that resolves `org.openmoq:moq5-android` from a repository containing
# nothing but what we just published, and compiles Kotlin that names the public
# types our API returns.
#
# Three preconditions make the answer mean something, and each is enforced
# here rather than assumed:
#
#   EPHEMERAL   the repository is created under this worktree's build output
#               and deleted afterwards. Never ~/.m2, never any shared cache:
#               a resolve that succeeds because a previous run left an
#               artifact behind proves nothing about what we publish today.
#   SEPARATE    the consumer is a generated root with its own settings file.
#               A module added to our own settings would resolve the project,
#               not the artifact, and would pass while the POM was wrong.
#   PUBLIC-ONLY the consumer names no internal package. If it could reach
#               `org.openmoq.moq5.engine` or `.jni`, a green here would say
#               nothing about the surface we actually publish.
#
# It exits non-zero with a named reason, having first printed a bounded tail of
# whichever log failed, and it LEAVES NOTHING BEHIND: the probe tree is removed
# on every exit path, so a later run cannot resolve against an artifact this
# one left lying around. `MOQ5_PROBE_KEEP=1` retains it for debugging, and the
# script says so when it does.
#
# Both Gradle invocations go through `tools/gradlew-verified`, so the wrapper
# jar and distribution are checked for the consumer build too; the consumer is
# reached with `-p`, which the launcher passes through to the checked wrapper.
set -u
LC_ALL=C; export LC_ALL

_self=${0##*/}
case $0 in
    */*) _selfdir=${0%/*} ;;
    *)   _selfdir=. ;;
esac
here=$(cd "$_selfdir" && pwd) || { echo "$_self: cannot resolve the script directory" >&2; exit 2; }
root=$(cd "$here/.." && pwd)  || { echo "$_self: cannot resolve bindings/kotlin" >&2; exit 2; }

for t in printf rm mkdir find grep tail sed sort tr awk cat head; do
    command -v "$t" >/dev/null 2>&1 \
        || { echo "$_self: required tool not found: $t" >&2; exit 2; }
done

keep=${MOQ5_PROBE_KEEP:-0}

cleanup() {
    if [ "$keep" = "1" ]; then
        printf '%s: MOQ5_PROBE_KEEP=1, retaining %s\n' "$_self" "$probe" >&2
        return
    fi
    rm -rf -- "$probe"
}

# `die <message> [logfile]` -- names the reason, shows a bounded tail of the log
# that produced it, and still leaves the tree clean.
# code_of <file> -- the file with line and block comments removed, string
# literals preserved. Every guard below reads THIS, never raw text: the first
# run of the completed probe was refused by its own project-dependency check
# because the generated file's COMMENT says "No project(...)". A guard that
# reads its own documentation is the defect this whole gate keeps finding.
code_of() {
    awk '
    BEGIN { depth = 0 }
    {
        line = $0; n = length(line); i = 1; out = ""
        while (i <= n) {
            c = substr(line, i, 1); c2 = substr(line, i, 2)
            if (depth > 0) {
                if (c2 == "/*") { depth++; i += 2; continue }
                if (c2 == "*/") { depth--; i += 2; continue }
                i++; continue
            }
            if (c2 == "/*") { depth++; i += 2; continue }
            if (c2 == "//") { break }
            if (c == "\"") {
                out = out c; i++
                while (i <= n) {
                    d = substr(line, i, 1)
                    if (d == "\\") { out = out substr(line, i, 2); i += 2; continue }
                    out = out d; i++
                    if (d == "\"") break
                }
                continue
            }
            out = out c; i++
        }
        print out
    }' "$1"
}

# check_settings <settings-file>
#
# The generated consumer must resolve OUR group causally from THIS run: the
# ephemeral repository includes `org.openmoq` and the public repositories
# exclude it. Without that, the probe would start passing the day
# `org.openmoq:moq5-android` exists on Maven Central -- resolving the artifact
# from the internet while reporting that it resolved what we just published
# (chats/libmoq/1515 F1). Ordinary dependencies such as coroutines still come
# from Google and Central, which is why this is a content filter and not a
# repository removal.
#
# It reads CODE and checks ATTACHMENT. Counting `excludeGroup` occurrences
# anywhere in the file accepted them sitting in a comment beside an
# unrestricted `google()` -- a template edit could then drop the real filters
# and keep the prose (chats/libmoq/1517 F1). So comments are stripped first,
# and each filter must live INSIDE the block of the repository it governs.
#
# Exposed as `--check-settings <file>` so the guard itself can be exercised.
check_settings() {
    _f=$1
    [ -f "$_f" ] || { printf 'the settings file %s does not exist\n' "$_f"; return 1; }
    _v=$(awk '
    { src = src $0 "\n" }
    END {
        # 1. strip line and block comments, preserving string literals: the
        #    group names we match on ARE strings.
        n = length(src); i = 1; out = ""; depth = 0
        while (i <= n) {
            c = substr(src, i, 1); c2 = substr(src, i, 2)
            if (depth > 0) {
                if (c2 == "/*") { depth++; i += 2; continue }
                if (c2 == "*/") { depth--; i += 2; continue }
                i++; continue
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
        gsub(/[ \t\n]+/, " ", out)

        # 2. Judge ONLY the dependency repositories. `pluginManagement` names
        #    google() and mavenCentral() too, but those resolve PLUGINS; the
        #    filters that decide where org.openmoq comes from live in
        #    dependencyResolutionManagement, and scoping there is what stops a
        #    correct-looking plugin block from answering for the wrong one.
        whole = out                      # the project-dependency ban is file-wide
        deps = block_after(out, "dependencyResolutionManagement")
        if (deps == "") {
            print "the generated settings declare no dependencyResolutionManagement block"
            exit
        }
        out = deps

        problems = ""
        split("google mavenCentral", public_repos, " ")
        for (p = 1; p <= 2; p++) {
            name = public_repos[p]
            body = block_after(out, name)
            if (body == "") {
                problems = problems sprintf("%s is declared with no content filter, so it could supply org.openmoq; ", name)
            } else if (index(body, "excludeGroup(\"org.openmoq\")") == 0) {
                problems = problems sprintf("%s has a content block that does not excludeGroup(\"org.openmoq\"); ", name)
            }
        }
        local = block_after(out, "maven")
        if (local == "") problems = problems "the ephemeral maven repository has no content block; "
        else if (index(local, "includeGroup(\"org.openmoq\")") == 0)
            problems = problems "the ephemeral maven repository does not includeGroup(\"org.openmoq\"); "
        else if (index(local, "uri(") == 0)
            problems = problems "the ephemeral maven repository declares no url; "
        # Anywhere in the file, not only in the repository block: a project
        # dependency would resolve the library instead of the artifact.
        if (index(whole, "project(") > 0) problems = problems "a project dependency is named; the consumer must resolve by coordinates alone; "

        print (problems == "" ? "ok" : problems)
    }

    function block_after(s, name,   at, i, d, started, body, c, rest) {
        at = index(s, name)
        while (at > 0) {
            rest = substr(s, at + length(name))
            # Only a block attached to THIS name counts: skip whitespace, then
            # require "{". `google()` yields "" and is reported as unfiltered.
            i = 1
            while (i <= length(rest) && substr(rest, i, 1) == " ") i++
            if (substr(rest, i, 1) == "{") {
                d = 0; body = ""; started = 0
                while (i <= length(rest)) {
                    c = substr(rest, i, 1)
                    if (c == "{") { d++; started = 1; if (d == 1) { i++; continue } }
                    if (c == "}") { d--; if (d == 0) return body }
                    if (started) body = body c
                    i++
                }
                return body
            }
            # ADVANCE PROPERLY: index() into a substring returns a position
            # relative to THAT substring, so the next absolute position is
            # at + length(name) + nxt - 1. Adding only length(name) left `at`
            # standing still and spun forever on a name with no attached block.
            nxt = index(substr(s, at + length(name)), name)
            if (nxt == 0) return ""
            at = at + length(name) + nxt - 1
        }
        return ""
    }' "$_f")
    if [ "$_v" = "ok" ]; then
        printf 'ok\n'
        return 0
    fi
    printf '%s\n' "$_v"
    return 1
}

# The one settings template. Both the real run and `--emit-settings` write it,
# so the guard and the self-test judge exactly what a probe run produces.
emit_settings() {
    cat > "$1" <<EOF
// A SEPARATE Gradle root. It is not a module of the library build, so
// ":moq5-android" is not resolvable here: the only way to satisfy the
// dependency below is the published artifact in the ephemeral repository.
pluginManagement {
    repositories { google(); mavenCentral(); gradlePluginPortal() }
    plugins { id("com.android.application") version "$(sed -n 's/^agp\.version=//p' "$root/gradle/toolchain.properties")" }
}
dependencyResolutionManagement {
    repositoriesMode.set(RepositoriesMode.FAIL_ON_PROJECT_REPOS)
    repositories {
        // OUR group comes from THIS run's ephemeral repository and nowhere
        // else. Without the filters below, the probe would begin passing the
        // day org.openmoq:moq5-android exists publicly -- resolving from the
        // internet while reporting that it resolved what we just published.
        maven {
            url = uri("$repo")
            content { includeGroup("org.openmoq") }
        }
        // Ordinary dependencies (coroutines, the Android platform artifacts)
        // still resolve normally; they simply cannot supply org.openmoq.
        google { content { excludeGroup("org.openmoq") } }
        mavenCentral { content { excludeGroup("org.openmoq") } }
    }
}
rootProject.name = "moq5-external-consumer"
include(":app")
EOF
}

if [ "${1:-}" = "--check-settings" ]; then
    check_settings "${2:-}" || exit 1
    exit 0
fi

# `--emit-settings <file>` writes the settings this probe would generate, with
# a stand-in repository path, and nothing else. It exists so the guard can be
# run against the REAL template rather than a copy of it that could drift.
if [ "${1:-}" = "--emit-settings" ]; then
    _out=${2:?--emit-settings needs a destination}
    repo="/tmp/moq5-probe-repo"
    emit_settings "$_out"
    exit 0
fi

die() {
    printf '%s: %s\n' "$_self" "$1" >&2
    if [ -n "${2:-}" ] && [ -f "$2" ]; then
        printf '%s: last 15 lines of %s:\n' "$_self" "${2##*/}" >&2
        tail -15 "$2" >&2
    fi
    exit 1
}

# The ephemeral world, under this worktree's ignored build output.
probe="$root/build/publication-probe"
repo="$probe/repo"
consumer="$probe/consumer"

trap 'cleanup; exit 130' HUP INT TERM
trap cleanup EXIT

case $repo in
    "$HOME"/.m2/*|"$HOME"/.gradle/*)
        die "the probe repository resolves under a shared cache ($repo); evidence must be ephemeral and worktree-local" ;;
esac

rm -rf -- "$probe" || die "cannot clear $probe"
mkdir -p "$repo" "$consumer" || die "cannot create the probe tree"

# -- publish into the ephemeral repository ------------------------------------
# `moqPublicationRepo` is the property the product build must honour. Until the
# publication GREEN exists this fails, which is the point of the RED.
"$root/tools/gradlew-verified" -p "$root" \
    :moq5-android:publishAllPublicationsToProbeRepository \
    -PmoqPublicationRepo="$repo" --console=plain > "$probe/publish.log" 2>&1 \
    || die "publishing into the ephemeral repository failed" "$probe/publish.log"

# The COORDINATE path, not "the first AAR anywhere under the repo": the
# contract is about org.openmoq:moq5-android, so an artifact published at some
# other coordinate must not satisfy it (chats/libmoq/1515 F4).
coord="$repo/org/openmoq/moq5-android"
[ -d "$coord" ] || die "the ephemeral repository holds nothing at org/openmoq/moq5-android after publishing"

_vers=$(find "$coord" -mindepth 1 -maxdepth 1 -type d | sed 's#.*/##' | sort)
_n=$(printf '%s\n' "$_vers" | grep -c .)
[ "${_n:-0}" -eq 1 ] || die "expected exactly one version directory under $coord, found ${_n:-0}: $(printf '%s ' $_vers)"
version=$_vers
case $version in
    *[!0-9A-Za-z.\-+_]*|"") die "the published version '$version' is not a plausible Maven version" ;;
esac

published="$coord/$version/moq5-android-$version.aar"
[ -f "$published" ] \
    || die "no artifact at the exact coordinate path ${published#"$repo"/}; the repository holds $(find "$coord" -type f | sed "s#.*/##" | tr '\n' ' ')"

# -- the generated consumer root ----------------------------------------------
mkdir -p "$consumer/app/src/main/kotlin/probe" "$consumer/app/src/main" || die "cannot lay out the consumer"

emit_settings "$consumer/settings.gradle.kts"

cat > "$consumer/app/build.gradle.kts" <<EOF
plugins { id("com.android.application") }
android {
    namespace = "probe"
    compileSdk = $(sed -n 's/^sdk\.platform=android-//p' "$root/gradle/toolchain.properties" | sed 's/\..*//')
    defaultConfig { minSdk = 21 }
}
dependencies {
    // Coordinates only. No project(...), no source set, no local path. The
    // version is the one just published, read back from the repository.
    implementation("org.openmoq:moq5-android:$version")
}
EOF

# The SDK location, taken from THIS build's own local.properties rather than
# from the environment: the consumer is a separate root and Gradle will not
# inherit it. Machine-local, ephemeral, and removed with the probe tree.
_sdk=$(sed -n 's/^sdk\.dir=//p' "$root/local.properties" 2>/dev/null | head -1)
if [ -n "$_sdk" ]; then
    printf 'sdk.dir=%s\n' "$_sdk" > "$consumer/local.properties"
elif [ -n "${ANDROID_HOME:-}" ]; then
    printf 'sdk.dir=%s\n' "$ANDROID_HOME" > "$consumer/local.properties"
else
    die "no Android SDK location: $root/local.properties has no sdk.dir and ANDROID_HOME is unset"
fi

cat > "$consumer/app/src/main/AndroidManifest.xml" <<'EOF'
<?xml version="1.0" encoding="utf-8"?>
<manifest xmlns:android="http://schemas.android.com/apk/res/android" />
EOF

# Code that names the types whose compilation REQUIRES the POM to expose
# coroutines: a StateFlow-carrying surface on each of endpoint, receiver and
# sender. If coroutines were published as an implementation dependency this
# file would not compile, which is exactly the regression the row exists for.
cat > "$consumer/app/src/main/kotlin/probe/Consumer.kt" <<'EOF'
package probe

import kotlinx.coroutines.flow.StateFlow
import org.openmoq.moq5.EndpointSnapshot
import org.openmoq.moq5.MediaReceiver
import org.openmoq.moq5.MediaSender
import org.openmoq.moq5.MoqEndpoint
import org.openmoq.moq5.ReceiverInventory
import org.openmoq.moq5.SenderStatus

/**
 * A stranger's code. It reaches nothing internal, imports no C header and
 * loads no private artefact: it names the published surface only, including
 * the three StateFlow-typed members that require coroutines on a consumer's
 * COMPILE classpath.
 */
object Consumer {
    fun snapshots(endpoint: MoqEndpoint): StateFlow<EndpointSnapshot> = endpoint.snapshots

    fun inventory(receiver: MediaReceiver): StateFlow<ReceiverInventory> = receiver.inventory

    fun status(sender: MediaSender): StateFlow<SenderStatus> = sender.status
}
EOF

# The repository-shape precondition, checked on the file that was just
# generated: an edit to the template above that dropped a filter would
# otherwise weaken the probe silently.
_shape=$(check_settings "$consumer/settings.gradle.kts") \
    || die "the generated consumer could resolve our group from a public repository: $_shape"

# The PUBLIC-ONLY precondition, enforced on the generated source itself so a
# future edit to this probe cannot quietly weaken what it proves.
if code_of "$consumer/app/src/main/kotlin/probe/Consumer.kt" | grep -qE 'org\.openmoq\.moq5\.(engine|jni)'; then
    die "the generated consumer names an internal package; a green probe would then say nothing about the published surface"
fi
if code_of "$consumer/app/build.gradle.kts" | grep -q 'project('; then
    die "the generated consumer declares a project dependency; it must resolve the artifact by coordinates alone"
fi

# -- compile it ---------------------------------------------------------------
# The SAME checked launcher as the publish step: a consumer compile that
# reached a host Gradle would prove less than the publish that preceded it.
"$root/tools/gradlew-verified" -p "$consumer" :app:compileDebugKotlin --console=plain \
    > "$probe/consumer.log" 2>&1 \
    || die "the external consumer failed to compile" "$probe/consumer.log"

printf 'external consumer: compiled against org.openmoq:%s from %s\n' 'moq5-android' "$repo"
exit 0
