#!/bin/sh
# The Maven publication contract for the one product AAR.
#
# Everything before this slice proved the library builds and that a module in
# THIS build can consume it. Publication is a different claim: that a stranger
# with only our coordinates can add one line to their build file, compile
# against the types we return, and get a working native payload. Every step of
# that is a separate fact, and none of them is implied by a green in-tree
# build:
#
#   IDENTITY   exactly one publishable module, with explicit coordinates and a
#              version from a named source;
#   FLOOR      a product minSdk, declared before anything is published --
#              publishing without one ships a support claim nobody made;
#   PAYLOAD    the AAR actually carries the JNI shared object for every
#              supported ABI, plus the consumer rules that keep the bridge
#              bound, and no test-only class;
#   METADATA   the POM/module metadata carries the coroutines API dependency
#              in a consumer-COMPILE scope (our public API returns StateFlow),
#              and leaks no test-only dependency;
#   CONSUMER   a SEPARATE Gradle root, resolving from an ephemeral repository
#              under this worktree, compiles code naming those public types --
#              without `project(":moq5-android")` and without reaching any
#              internal package.
#
# The consumer row is the only one that can prove the others were not merely
# written down, which is why it is a real build rather than a metadata read.
set -u
LC_ALL=C; export LC_ALL

_self=${0##*/}
case $0 in
    */*) _selfdir=${0%/*} ;;
    *)   _selfdir=. ;;
esac
here=$(cd "$_selfdir" && pwd) || { echo "$_self: cannot resolve the script directory" >&2; exit 2; }
root=$(cd "$here/.." && pwd)  || { echo "$_self: cannot resolve bindings/kotlin" >&2; exit 2; }

# Every external command this gate runs, and ONLY those: a preflight naming a
# tool the script never invokes would refuse to run on a host that lacks it for
# no reason. `sort`, `tail` and `cut` were named here and never called; `tr` is
# called in the exact-AAR failure branch and was not named (chats/libmoq/1523).
#
# `python3` is deliberately absent: only the module-metadata row needs it, and
# that row fails CLOSED by name when it is missing rather than taking the whole
# gate down or degrading to a text scan.
for t in find grep printf sed awk tr unzip mktemp rm mkdir head; do
    command -v "$t" >/dev/null 2>&1 \
        || { echo "$_self: required tool not found: $t" >&2; exit 2; }
done

# The one hasher the other gates use, so "identical" means the same thing here.
# shellcheck source=lib-hash.sh
. "$here/lib-hash.sh"
moq_hash_available >/dev/null 2>&1 \
    || { echo "$_self: no SHA-256 tool; byte identity is unprovable" >&2; exit 2; }

scratch=$(mktemp -d) || { echo "$_self: cannot create a scratch directory" >&2; exit 2; }
cleanup() { rm -rf -- "$scratch"; }
trap 'cleanup; exit 130' HUP INT TERM
trap cleanup EXIT

fails=0
pass() { printf 'PASS[%s]: %s\n' "$1" "$2"; }
fail() { fails=$((fails + 1)); printf 'FAIL[%s]: %s\n' "$1" "$2"; }

settings="$root/settings.gradle.kts"
product="$root/moq5-android/build.gradle.kts"
[ -f "$settings" ] || { echo "$_self: no settings.gradle.kts under $root" >&2; exit 2; }
[ -f "$product" ]  || { echo "$_self: no moq5-android/build.gradle.kts under $root" >&2; exit 2; }

# The coordinates the assessment names. Independent literals: a build script
# that changed them would fail here rather than redefine the contract.
GROUP='org.openmoq'
ARTIFACT='moq5-android'

# The ABIs a published payload must carry, from the same assessment.
ABIS='arm64-v8a x86_64'

# The ONE version authority: the C library's own header, which the native build
# compiles against and which the product build script reads. The gate parses it
# independently and requires the PUBLISHED artifacts to carry that value, so
# "the version is read from a source" becomes "the version a consumer resolves
# IS the source's" (chats/libmoq/1521 F4).
VERSION_HEADER="$root/../../core/include/moq/version.h"

# Modules that must NEVER publish. The samples are acceptance apps and the
# fixture is verification infrastructure; publishing either would ship a
# consumer of the library as though it were the library.
UNPUBLISHABLE='samples/player samples/publisher bridge-r8-fixture'

# Comments are stripped before any build script is classified, so prose about
# publishing can neither satisfy nor trip a row (the lesson of 1495/1509).
#
# `assignments <file>` then emits one `<block path>|<name>|<value>` line per
# assignment, tracking brace nesting so each is reported WHERE IT LIVES:
#
#   |group|"org.openmoq"                         a top-level project property
#   tasks.register/|group|"verification"          a task's group -- not a coordinate
#   android/defaultConfig/|minSdk|21              the product's API floor
#
# Scope, not formatting. The column-zero anchors this replaces rejected a
# task's `group` correctly but only by accident of indentation, and they let
# ANY indented `minSdk =` satisfy the floor -- including one in the wrong block
# (chats/libmoq/1513 F3).
assignments() {
    awk '
    BEGIN { depth = 0; word = ""; last = "" }
    {
        line = $0; n = length(line); i = 1
        while (i <= n) {
            c = substr(line, i, 1)
            # An identifier run accumulates; anything else ENDS it, and the
            # completed run becomes the last identifier seen. Clearing on the
            # space before "=" is what made the first version of this parser
            # report no assignments at all.
            if (c ~ /[A-Za-z0-9_.]/) { word = word c; i++; continue }
            if (word != "") { last = word; word = "" }

            if (c == "{") {
                depth++
                stack[depth] = (last == "" ? "?" : last)
                last = ""
                i++
                continue
            }
            if (c == "}") {
                if (depth > 0) { delete stack[depth]; depth-- }
                last = ""
                i++
                continue
            }
            if (c == "=") {
                # A plain assignment only: not ==, !=, <=, >=.
                prev = (i > 1 ? substr(line, i - 1, 1) : " ")
                nxt  = substr(line, i + 1, 1)
                if (nxt != "=" && prev != "=" && prev != "!" && prev != "<" && prev != ">" && last != "") {
                    path = ""
                    for (k = 1; k <= depth; k++) path = path stack[k] "/"
                    value = substr(line, i + 1)
                    gsub(/^[ \t]+|[ \t]+$/, "", value)
                    print path "|" last "|" value
                }
                last = ""
                i++
                continue
            }
            i++
        }
        if (word != "") { last = word; word = "" }
    }' "$1"
}

strip_comments() {
    awk '
    BEGIN { depth = 0; raw = 0 }
    {
        line = $0; out = ""; i = 1; n = length(line)
        while (i <= n) {
            c = substr(line, i, 1); c2 = substr(line, i, 2); c3 = substr(line, i, 3)
            if (raw) { if (c3 == "\"\"\"") { raw = 0; i += 3 } else { i++ } ; continue }
            if (depth > 0) {
                if (c2 == "/*") { depth++; i += 2; continue }
                if (c2 == "*/") { depth--; i += 2; continue }
                i++; continue
            }
            if (c3 == "\"\"\"") { out = out c3; raw = 1; i += 3; continue }
            if (c2 == "/*") { depth++; i += 2; continue }
            if (c2 == "//") { break }
            out = out c; i++
        }
        print out
    }' "$@"
}

# -- 1. exactly one publishable module ----------------------------------------
strip_comments "$product" > "$scratch/product.kts"
if grep -qE 'id\("maven-publish"\)|`maven-publish`' "$scratch/product.kts"; then
    pass publication.publishable "moq5-android applies maven-publish"
    _publishable=1
else
    fail publication.publishable "moq5-android/build.gradle.kts does not apply maven-publish; the one product AAR is not publishable, so no coordinate, POM or consumer claim below can be true"
    _publishable=0
fi

_leak=""
for m in $UNPUBLISHABLE; do
    b="$root/$m/build.gradle.kts"
    [ -f "$b" ] || continue
    strip_comments "$b" > "$scratch/other.kts"
    grep -qE 'id\("maven-publish"\)|`maven-publish`|publishing[[:space:]]*\{' "$scratch/other.kts" && _leak="$_leak $m"
done
if [ -n "$_leak" ]; then
    fail publication.unpublishable "these modules configure publishing but must not:$_leak -- a sample is an acceptance app and the R8 fixture is verification infrastructure"
else
    pass publication.unpublishable "samples and the R8 fixture publish nothing"
fi

# The rejected two-artifact split must not return: a Kotlin `internal` backend
# SPI is module-scoped, so a second published artifact could not implement it.
if grep -qE '^[[:space:]]*include\("[:]moq5-api"\)' "$settings"; then
    fail publication.no-moq5-api "settings.gradle.kts includes \":moq5-api\"; that separately published artifact was rejected and must not be revived"
else
    pass publication.no-moq5-api "the rejected :moq5-api artifact is absent"
fi

# -- 2. explicit, stable coordinates ------------------------------------------
assignments "$scratch/product.kts" > "$scratch/assign"

# A PROJECT group is a TOP-LEVEL assignment (empty block path). The same name
# inside `tasks.register {}` is a task's group and says nothing about
# coordinates.
_g=$(sed -n 's/^|group|"\([^"]*\)".*/\1/p' "$scratch/assign" | head -1)
if [ "${_g:-}" = "$GROUP" ]; then
    pass publication.group "the product declares group \"$GROUP\""
else
    fail publication.group "the product declares group \"${_g:-<none>}\", the contract is \"$GROUP\""
fi

# The artifactId defaults to the Gradle project name, so the row asserts the
# name the settings file actually establishes rather than a string in a script.
_incl=$(grep -cE "^[[:space:]]*include\\(\":$ARTIFACT\"\\)" "$settings")
if [ "${_incl:-0}" -ge 1 ]; then
    pass publication.artifact "the module is \":$ARTIFACT\", so the artifactId is \"$ARTIFACT\""
else
    fail publication.artifact "settings.gradle.kts does not include \":$ARTIFACT\"; the artifactId would not be \"$ARTIFACT\""
fi

# -- 3. the version comes from a named source ---------------------------------
if [ ! -f "$VERSION_HEADER" ]; then
    fail publication.version-authority "no version authority at core/include/moq/version.h; the published version has nothing to be checked against"
    header_version=""
else
    header_version=$(sed -nE 's/^#define[[:space:]]+MOQ_VERSION_STRING[[:space:]]+"([^"]+)".*/\1/p' "$VERSION_HEADER" | head -1)
    if [ -z "$header_version" ]; then
        fail publication.version-authority "core/include/moq/version.h declares no MOQ_VERSION_STRING"
    else
        pass publication.version-authority "the version authority declares MOQ_VERSION_STRING \"$header_version\""
    fi
fi

# A literal in the build script is not a source: it is a second place for the
# number to live and drift from. The row requires the assignment to READ
# something, and requires the value to be non-empty.
_ver_line=$(sed -n 's/^|version|//p' "$scratch/assign" | head -1)
if [ -z "$_ver_line" ]; then
    fail publication.version "the product declares no version; a published artifact with no version is unresolvable"
elif printf '%s' "$_ver_line" | grep -qE '^"'; then
    # ANY quoted string, not merely one starting with a digit: `version = "dev"`
    # and `version = "v0.1.0"` are second homes for the number just as surely
    # as `"0.1.0"` is (chats/libmoq/1515 F2).
    fail publication.version "the product's version is a string literal ($_ver_line); it must be READ from a named source so the number lives in exactly one place"
else
    pass publication.version "the product's version is read from a source rather than written as a literal"
fi

# -- 4. the product declares its own minSdk -----------------------------------
# Publishing without one ships a support claim nobody made: consumers would
# infer the floor from whatever their own manifest merge happened to accept.
# The floor is the one AGP reads: `android { defaultConfig { minSdk = ... } }`.
# A `minSdk` anywhere else is a local variable or the wrong block, and would
# publish an artifact whose declared floor came from nothing.
_min=$(sed -n 's#^android/defaultConfig/|minSdk|##p' "$scratch/assign" | head -1)
if [ -n "$_min" ]; then
    pass publication.minsdk "the product declares android.defaultConfig.minSdk = $_min"
else
    _elsewhere=$(grep -c '|minSdk|' "$scratch/assign")
    if [ "${_elsewhere:-0}" -gt 0 ]; then
        fail publication.minsdk "moq5-android/build.gradle.kts assigns minSdk ${_elsewhere} time(s), but NONE inside android { defaultConfig { } }; only that assignment is the floor AGP publishes"
    else
        fail publication.minsdk "moq5-android/build.gradle.kts declares no minSdk; the module's own comment says the ruling is owed before any native payload is published, and publishing without it ships an API floor nobody decided"
    fi
fi

# -- 5. the archive carries what a consumer needs -----------------------------
# The EXACT archive AGP writes for this module. "The first *-release.aar
# anywhere under outputs" would let a stale or wrong-named neighbour answer for
# the artifact we publish (chats/libmoq/1521 F3).
aar="$root/moq5-android/build/outputs/aar/moq5-android-release.aar"
if [ ! -f "$aar" ]; then
    fail publication.aar-present "no release AAR at the exact AGP output path (moq5-android/build/outputs/aar/moq5-android-release.aar); run :moq5-android:assembleRelease first$(
        _others=$(find "$root/moq5-android/build/outputs/aar" -name '*-release.aar' 2>/dev/null | sed 's#.*/##' | tr '\n' ' ')
        [ -n "$_others" ] && printf '. The directory holds: %s' "$_others"
    )"
else
    pass publication.aar-present "release AAR at the exact AGP output path"
    unzip -l "$aar" > "$scratch/aar.list" 2>/dev/null || : > "$scratch/aar.list"

    # Presence is not the contract. A dummy, stale or wrong-ABI object would
    # satisfy a filename check while the audited topology said otherwise, so
    # each member must be BYTE-IDENTICAL to the package the topology gate
    # already vouches for (chats/libmoq/1513 F1).
    _bad=""
    for abi in $ABIS; do
        _pkg="$root/.native/_topology/$abi/package/libmoq5_jni.so"
        if ! grep -qE "jni/$abi/libmoq5_jni\\.so" "$scratch/aar.list"; then
            _bad="$_bad $abi(absent from the AAR)"
            continue
        fi
        if [ ! -f "$_pkg" ]; then
            _bad="$_bad $abi(no audited topology package at .native/_topology/$abi/package/libmoq5_jni.so)"
            continue
        fi
        rm -rf -- "$scratch/x"; mkdir -p "$scratch/x"
        if ! unzip -q -o -j "$aar" "jni/$abi/libmoq5_jni.so" -d "$scratch/x" 2>/dev/null; then
            _bad="$_bad $abi(the AAR member could not be extracted)"
            continue
        fi
        _h_aar=$(moq_hash_file "$scratch/x/libmoq5_jni.so" "$scratch") || { _bad="$_bad $abi(unhashable AAR member)"; continue; }
        _h_pkg=$(moq_hash_file "$_pkg" "$scratch") || { _bad="$_bad $abi(unhashable topology package)"; continue; }
        [ "$_h_aar" = "$_h_pkg" ] || _bad="$_bad $abi(AAR $_h_aar != topology $_h_pkg)"
    done
    if [ -n "$_bad" ]; then
        fail publication.aar-native "the release AAR's JNI payload does not match the audited topology package:$_bad -- the bridge calls System.loadLibrary(\"moq5_jni\"), so a consumer resolving this artifact would load whatever the archive happens to carry. The cross-built objects under .native/_topology are what the topology gate vouches for"
    else
        pass publication.aar-native "the release AAR carries libmoq5_jni.so byte-identical to the audited topology package, for every supported ABI"
    fi

    if grep -q 'proguard.txt' "$scratch/aar.list"; then
        pass publication.aar-rules "the release AAR carries the packaged consumer rules"
    else
        fail publication.aar-rules "the release AAR carries no proguard.txt; the bridge's RegisterNatives identity would not survive a consumer's R8"
    fi
fi

# -- 6. published metadata ----------------------------------------------------
# The POM is the only thing a stranger reads. Our public API returns StateFlow,
# so coroutines must be visible to their COMPILER, not merely present at
# runtime -- Maven's `compile` scope, which Gradle writes for an `api`
# dependency and NOT for an `implementation` one.
# The EXACT paths Gradle writes for the `release` publication. `find ... | head`
# would accept a stale or unrelated POM from anywhere under build/, which is
# how a wrong artifact passes a metadata row (chats/libmoq/1513 F2).
pom="$root/moq5-android/build/publications/release/pom-default.xml"
module="$root/moq5-android/build/publications/release/module.json"

if [ ! -f "$pom" ]; then
    fail publication.pom "no POM at the release publication's exact path (moq5-android/build/publications/release/pom-default.xml); with no publication configured there is no metadata for a consumer to read"
else
    pass publication.pom "POM at the release publication's exact path"

    _pg=$(sed -n 's#.*<groupId>\([^<]*\)</groupId>.*#\1#p' "$pom" | head -1)
    _pa=$(sed -n 's#.*<artifactId>\([^<]*\)</artifactId>.*#\1#p' "$pom" | head -1)
    _pv=$(sed -n 's#.*<version>\([^<]*\)</version>.*#\1#p' "$pom" | head -1)
    if [ "$_pg" = "$GROUP" ] && [ "$_pa" = "$ARTIFACT" ] && [ -n "$_pv" ]; then
        pass publication.pom-coordinates "the POM declares $_pg:$_pa:$_pv"
    else
        fail publication.pom-coordinates "the POM declares \"${_pg:-<none>}:${_pa:-<none>}:${_pv:-<none>}\", expected $GROUP:$ARTIFACT with a non-empty version"
    fi

    # ... and that version must BE the authority's, not merely non-empty.
    if [ -n "$header_version" ]; then
        if [ "$_pv" = "$header_version" ]; then
            pass publication.pom-version "the POM's version is the authority's ($header_version)"
        else
            fail publication.pom-version "the POM publishes version \"${_pv:-<none>}\" but core/include/moq/version.h declares \"$header_version\"; a consumer would resolve a number that describes no source"
        fi
    fi

    # Our public API returns StateFlow, so coroutines must reach a consumer's
    # COMPILER: Maven `compile` scope, which Gradle writes for an `api`
    # dependency and NOT for an `implementation` one.
    if awk '/<artifactId>kotlinx-coroutines-core<\/artifactId>/,/<\/dependency>/' "$pom" | grep -q '<scope>compile</scope>'; then
        pass publication.pom-coroutines "the POM carries kotlinx-coroutines-core in compile scope"
    else
        fail publication.pom-coroutines "the POM does not carry kotlinx-coroutines-core in compile scope; a consumer could not name the StateFlow types this API returns"
    fi
    if grep -q 'kotlinx-coroutines-test' "$pom"; then
        fail publication.pom-test-leak "the POM carries kotlinx-coroutines-test; a test-only dependency must not reach consumers"
    else
        pass publication.pom-test-leak "no test-only dependency in the POM"
    fi
fi

# Gradle metadata is a SEPARATE oracle, not a formality: a Gradle consumer
# prefers `.module` over the POM, so a correct POM beside wrong module metadata
# would still hand a stranger the wrong classpath -- and the external-consumer
# row alone would not say which of the two files was at fault.
#
# It is parsed STRUCTURALLY. Real module metadata nests attributes, dependency
# lists and file entries, so selecting a variant by "the text between its name
# and the next closing brace" can both reject a correct document and accept a
# string that appears somewhere else entirely (chats/libmoq/1515 F3). No text
# fallback: with no parser the row fails closed.
module_facts() {
    python3 - "$1" "$GROUP" "$ARTIFACT" <<'PY'
import json, sys

path, group, artifact = sys.argv[1], sys.argv[2], sys.argv[3]
# The api variant is selected by Gradle's OWN attribute, not by a name we
# guessed. AGP publishes it as `releaseVariantReleaseApiPublication`, not
# `releaseApiElements`, and a name prefix would have to be re-guessed at every
# AGP release; `org.gradle.usage == "java-api"` is the contract Gradle
# actually resolves on.
API_USAGE = ("org.gradle.usage", "java-api")
COROUTINES = ("org.jetbrains.kotlinx", "kotlinx-coroutines-core")
TEST_ARTIFACT = "kotlinx-coroutines-test"

def emit(key, problem):
    print("%s %s" % (key, problem if problem else "ok"))

try:
    with open(path) as f:
        doc = json.load(f)
except Exception as exc:                       # unparseable IS a failure
    for key in ("COORDS", "API", "TEST"):
        emit(key, "the module metadata could not be parsed: %s" % exc)
    raise SystemExit(0)

component = doc.get("component") or {}
problems = []
if component.get("group") != group:
    problems.append("component.group is %r, expected %r" % (component.get("group"), group))
if component.get("module") != artifact:
    problems.append("component.module is %r, expected %r" % (component.get("module"), artifact))
if not (component.get("version") or "").strip():
    problems.append("component.version is empty")
emit("COORDS", "; ".join(problems))
print("VERSION %s" % (component.get("version") or ""))

variants = doc.get("variants") or []
api = [v for v in variants
       if ((v.get("attributes") or {}).get(API_USAGE[0]) == API_USAGE[1])]
if not api:
    emit("API", "no variant carries %s=%s; saw %r" % (
        API_USAGE[0], API_USAGE[1],
        [(v.get("name"), (v.get("attributes") or {}).get(API_USAGE[0])) for v in variants]))
else:
    def has_coroutines(variant):
        for dep in variant.get("dependencies") or []:
            if (dep.get("group"), dep.get("module")) == COROUTINES:
                return True
        return False
    missing = [v.get("name") for v in api if not has_coroutines(v)]
    emit("API", "" if not missing else
         "the api variant(s) %r (usage=%s) do not depend on %s:%s" % (
             missing, API_USAGE[1], COROUTINES[0], COROUTINES[1]))

leaks = []
for variant in variants:
    for dep in variant.get("dependencies") or []:
        if dep.get("module") == TEST_ARTIFACT:
            leaks.append(variant.get("name"))
emit("TEST", "" if not leaks else "%s appears in variant(s) %r" % (TEST_ARTIFACT, sorted(set(leaks))))
PY
}

if [ ! -f "$module" ]; then
    fail publication.module "no Gradle module metadata at the release publication's exact path (module.json); a Gradle consumer prefers it over the POM, so it cannot be left unproven"
elif ! command -v python3 >/dev/null 2>&1; then
    fail publication.module "python3 is required to read Gradle module metadata structurally, and no text fallback is accepted: a nested document can defeat one. Install python3 or run this gate where it exists"
else
    pass publication.module "Gradle module metadata at the release publication's exact path"
    module_facts "$module" > "$scratch/module.facts" 2>&1 || :

    _row() {
        _v=$(sed -n "s/^$1 //p" "$scratch/module.facts")
        if [ "$_v" = "ok" ]; then
            pass "$2" "$3"
        else
            fail "$2" "${_v:-the module parser produced no $1 verdict}"
        fi
    }
    _row COORDS publication.module-coordinates "the module metadata declares $GROUP:$ARTIFACT with a non-empty version"
    if [ -n "$header_version" ]; then
        _mv=$(sed -n 's/^VERSION //p' "$scratch/module.facts")
        if [ "$_mv" = "$header_version" ]; then
            pass publication.module-version "the module metadata's version is the authority's ($header_version)"
        else
            fail publication.module-version "the module metadata publishes version \"${_mv:-<none>}\" but core/include/moq/version.h declares \"$header_version\"; a Gradle consumer prefers this file, so it would resolve that number"
        fi
    fi
    _row API    publication.module-coroutines  "the api variant of the module metadata depends on kotlinx-coroutines-core"
    _row TEST   publication.module-test-leak   "no test-only dependency in any variant of the module metadata"
fi

# -- 7. a separate external consumer compiles ---------------------------------
# The only row that proves the rest were not merely written down. It is
# delegated so its own preconditions -- an ephemeral repository under this
# worktree, never ~/.m2; a generated root that is NOT a module of this build --
# are stated in one place and can be self-tested on their own.
if [ "$_publishable" -eq 0 ]; then
    fail publication.external-consumer "cannot run: no publication is configured, so there is nothing for an external consumer to resolve. This row is RED for the same missing contract as publication.publishable, not for an unrelated build failure"
else
    if sh "$here/probe-external-consumer.sh" > "$scratch/probe.out" 2>&1; then
        pass publication.external-consumer "a separate Gradle root compiled against the published artifact from an ephemeral repository"
    else
        fail publication.external-consumer "the external consumer probe failed:
$(sed -n '1,20p' "$scratch/probe.out")"
    fi
fi

printf '\n'
if [ "$fails" -eq 0 ]; then
    printf 'android maven publication: 0 failure(s)\n'
    exit 0
fi
printf 'android maven publication: %s failure(s)\n' "$fails"
exit 1
