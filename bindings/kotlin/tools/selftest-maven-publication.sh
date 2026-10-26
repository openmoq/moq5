#!/bin/sh
# The publication gate's own ledger.
#
# On today's tree the gate reports seven failures, and six of them follow from
# one fact: no publication is configured. That leaves the rows which currently
# PASS -- samples publish nothing, the rejected `moq5-api` is absent, the
# artifactId, the packaged consumer rules -- entirely unexercised, and it
# leaves the metadata rows unexercised in BOTH directions. An unexercised row
# is one that could be passing vacuously the day publication lands.
#
# So this builds a disposable fixture root the gate ACCEPTS completely, then
# breaks one fact at a time and requires the matching row to fail BY NAME.
#
# The external-consumer row is exercised through a STUB probe in the fixture's
# own tools directory: what is under test here is the gate's handling of the
# probe's verdict. The probe's own preconditions -- an ephemeral repository, a
# separate root, a public-only consumer -- are guards inside that script and
# are not simulated by a fake publish.
set -u
LC_ALL=C; export LC_ALL

_self=${0##*/}
case $0 in
    */*) _selfdir=${0%/*} ;;
    *)   _selfdir=. ;;
esac
here=$(cd "$_selfdir" && pwd) || { echo "$_self: cannot resolve the script directory" >&2; exit 2; }

# Every external command this ledger runs, including the ones its failure-path
# mutators use: `mv` renames the archive for the exact-path case.
for t in mktemp rm cp mv sed grep printf mkdir zip cat; do
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

# The fixture mirrors the REAL layout, `<repo>/bindings/kotlin`, because the
# gate resolves its version authority at `$root/../../core/include/moq/version.h`.
# A flat fixture would put that path outside the scratch tree.
R="$W/repo/bindings/kotlin"

build_fixture() {
    rm -rf -- "$W/repo"
    mkdir -p "$R/tools" "$R/moq5-android/build/outputs/aar" "$R/samples/player" \
             "$R/samples/publisher" "$R/bridge-r8-fixture" || return 1
    cp "$here/check-maven-publication.sh" "$here/lib-hash.sh" "$R/tools/" || return 1

    # The version authority the gate parses independently of the build script.
    mkdir -p "$W/repo/core/include/moq" || return 1
    cat > "$W/repo/core/include/moq/version.h" <<'EOF'
#define MOQ_VERSION_MAJOR 0
#define MOQ_VERSION_MINOR 1
#define MOQ_VERSION_PATCH 0
#define MOQ_VERSION_STRING "0.1.0"
EOF

    # A stub probe: what the gate does with its verdict is the thing under test.
    cat > "$R/tools/probe-external-consumer.sh" <<'EOF'
#!/bin/sh
echo "stub probe: external consumer compiled"
exit 0
EOF

    cat > "$R/settings.gradle.kts" <<'EOF'
rootProject.name = "moq5-bindings"
include(":moq5-android")
include(":samples:player")
include(":samples:publisher")
include(":bridge-r8-fixture")
EOF

    cat > "$R/moq5-android/build.gradle.kts" <<'EOF'
plugins {
    id("com.android.library")
    id("maven-publish")
}

group = "org.openmoq"
version = libmoqVersion()

android {
    defaultConfig {
        minSdk = 21
    }
}

tasks.register("someCheck") {
    group = "verification"
    description = "a task group is not a coordinate"
}
EOF

    for m in samples/player samples/publisher bridge-r8-fixture; do
        printf 'plugins { id("com.android.application") }\ndependencies { implementation(project(":moq5-android")) }\n' \
            > "$R/$m/build.gradle.kts"
    done

    # The audited topology packages, and an archive whose members are those
    # exact bytes. Stand-in content is fine; the CONTRACT under test is
    # equality between the two, not a filename.
    for abi in arm64-v8a x86_64; do
        mkdir -p "$R/.native/_topology/$abi/package" || return 1
        printf 'the %s payload\n' "$abi" > "$R/.native/_topology/$abi/package/libmoq5_jni.so"
    done
    stage_aar || return 1

    mkdir -p "$R/moq5-android/build/publications/release" || return 1
    cat > "$R/moq5-android/build/publications/release/pom-default.xml" <<'EOF'
<project>
  <groupId>org.openmoq</groupId>
  <artifactId>moq5-android</artifactId>
  <version>0.1.0</version>
  <dependencies>
    <dependency>
      <groupId>org.jetbrains.kotlinx</groupId>
      <artifactId>kotlinx-coroutines-core</artifactId>
      <version>1.10.2</version>
      <scope>compile</scope>
    </dependency>
  </dependencies>
</project>
EOF
    cat > "$R/moq5-android/build/publications/release/module.json" <<'EOF'
{
  "formatVersion": "1.1",
  "component": {
    "group": "org.openmoq",
    "module": "moq5-android",
    "version": "0.1.0",
    "attributes": { "org.gradle.status": "release" }
  },
  "createdBy": { "gradle": { "version": "9.5.1" } },
  "variants": [
    {
      "name": "releaseVariantReleaseApiPublication",
      "attributes": {
        "org.gradle.category": "library",
        "org.gradle.usage": "java-api",
        "com.android.build.api.attributes.BuildTypeAttr": "release"
      },
      "dependencies": [
        {
          "group": "org.jetbrains.kotlinx",
          "module": "kotlinx-coroutines-core",
          "version": { "requires": "1.10.2" }
        }
      ],
      "files": [
        {
          "name": "moq5-android-0.1.0.aar",
          "url": "moq5-android-0.1.0.aar",
          "size": 547966,
          "sha512": "00",
          "sha256": "00",
          "sha1": "00",
          "md5": "00"
        }
      ]
    },
    {
      "name": "releaseVariantReleaseRuntimePublication",
      "attributes": {
        "org.gradle.category": "library",
        "org.gradle.usage": "java-runtime"
      },
      "dependencies": [
        {
          "group": "org.jetbrains.kotlinx",
          "module": "kotlinx-coroutines-core",
          "version": { "requires": "1.10.2" }
        }
      ],
      "files": [
        { "name": "moq5-android-0.1.0.aar", "url": "moq5-android-0.1.0.aar" }
      ]
    }
  ]
}
EOF
    return 0
}

# Repackage the fixture AAR from whatever the topology packages currently hold,
# so "matching" is produced the same way the GREEN would produce it.
stage_aar() {
    _a="$W/aarstage"
    rm -rf -- "$_a"
    mkdir -p "$_a/jni/arm64-v8a" "$_a/jni/x86_64" || return 1
    cp "$R/.native/_topology/arm64-v8a/package/libmoq5_jni.so" "$_a/jni/arm64-v8a/" || return 1
    cp "$R/.native/_topology/x86_64/package/libmoq5_jni.so" "$_a/jni/x86_64/" || return 1
    printf 'rules\n' > "$_a/proguard.txt"
    printf 'classes\n' > "$_a/classes.jar"
    rm -f "$R/moq5-android/build/outputs/aar/moq5-android-release.aar"
    ( cd "$_a" && zip -q -r "$R/moq5-android/build/outputs/aar/moq5-android-release.aar" . )
}

run_gate() { sh "$R/tools/check-maven-publication.sh" > "$W/out" 2>&1; printf '%s' "$?"; }

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
        pass "$_label" "the gate accepts a fully configured publication"
    fi
}

# The positive control FIRST: without it every kill below could be an artefact
# of a fixture the gate never accepts.
case_is selftest.accepts 0 ''

drop_publish()      { sed -i.bak '/maven-publish/d' "$R/moq5-android/build.gradle.kts" && rm -f "$R/moq5-android/build.gradle.kts.bak"; }
sample_publishes()  { printf 'plugins { id("maven-publish") }\n' >> "$R/samples/player/build.gradle.kts"; }
revive_moq5_api()   { printf 'include(":moq5-api")\n' >> "$R/settings.gradle.kts"; }
wrong_group()       { sed -i.bak 's/^group = "org.openmoq"/group = "com.example"/' "$R/moq5-android/build.gradle.kts" && rm -f "$R/moq5-android/build.gradle.kts.bak"; }
# The false diagnosis this gate had on its first run: a task's `group` is not a
# coordinate, and matching an indented assignment reported "verification" as
# the artifact's group.
group_only_in_task() { sed -i.bak 's/^group = "org.openmoq"//' "$R/moq5-android/build.gradle.kts" && rm -f "$R/moq5-android/build.gradle.kts.bak"; }
version_literal()   { sed -i.bak 's/^version = libmoqVersion()/version = "0.1.0"/' "$R/moq5-android/build.gradle.kts" && rm -f "$R/moq5-android/build.gradle.kts.bak"; }
no_version()        { sed -i.bak '/^version = /d' "$R/moq5-android/build.gradle.kts" && rm -f "$R/moq5-android/build.gradle.kts.bak"; }
no_minsdk()         { sed -i.bak '/minSdk = /d' "$R/moq5-android/build.gradle.kts" && rm -f "$R/moq5-android/build.gradle.kts.bak"; }
no_aar()            { rm -f "$R/moq5-android/build/outputs/aar/moq5-android-release.aar"; }
# The exact archive gone, a plausible neighbour left in its place: "the first
# *-release.aar anywhere" would have taken it (chats/libmoq/1521 F3).
aar_wrong_name()    {
    mv "$R/moq5-android/build/outputs/aar/moq5-android-release.aar" \
       "$R/moq5-android/build/outputs/aar/moq5-android-stale-release.aar"
}
# The published number must BE the authority's, not merely non-empty.
pom_version_drift()    { sed -i.bak 's#<version>0.1.0</version>#<version>9.9.9</version>#' "$R/moq5-android/build/publications/release/pom-default.xml" && rm -f "$R/moq5-android/build/publications/release/pom-default.xml.bak"; }
module_version_drift() { sed -i.bak 's#"version": "0.1.0",#"version": "9.9.9",#' "$R/moq5-android/build/publications/release/module.json" && rm -f "$R/moq5-android/build/publications/release/module.json.bak"; }
no_version_header()    { rm -f "$W/repo/core/include/moq/version.h"; }
aar_without_jni()   {
    _a="$W/aarstage2"; rm -rf -- "$_a"; mkdir -p "$_a" || return 1
    printf 'rules\n' > "$_a/proguard.txt"; printf 'classes\n' > "$_a/classes.jar"
    rm -f "$R/moq5-android/build/outputs/aar/moq5-android-release.aar"
    ( cd "$_a" && zip -q -r "$R/moq5-android/build/outputs/aar/moq5-android-release.aar" . )
}
aar_without_rules() {
    _a="$W/aarstage3"; rm -rf -- "$_a"; mkdir -p "$_a/jni/arm64-v8a" "$_a/jni/x86_64" || return 1
    cp "$R/.native/_topology/arm64-v8a/package/libmoq5_jni.so" "$_a/jni/arm64-v8a/"
    cp "$R/.native/_topology/x86_64/package/libmoq5_jni.so" "$_a/jni/x86_64/"
    rm -f "$R/moq5-android/build/outputs/aar/moq5-android-release.aar"
    ( cd "$_a" && zip -q -r "$R/moq5-android/build/outputs/aar/moq5-android-release.aar" . )
}
# BOTH member names present, one ABI's bytes stale: the dummy/stale payload a
# filename check would have accepted (chats/libmoq/1513 F1).
aar_stale_one_abi() {
    _a="$W/aarstage4"; rm -rf -- "$_a"; mkdir -p "$_a/jni/arm64-v8a" "$_a/jni/x86_64" || return 1
    cp "$R/.native/_topology/arm64-v8a/package/libmoq5_jni.so" "$_a/jni/arm64-v8a/"
    printf 'a stale x86_64 payload\n' > "$_a/jni/x86_64/libmoq5_jni.so"
    printf 'rules\n' > "$_a/proguard.txt"
    rm -f "$R/moq5-android/build/outputs/aar/moq5-android-release.aar"
    ( cd "$_a" && zip -q -r "$R/moq5-android/build/outputs/aar/moq5-android-release.aar" . )
}
no_topology_package() { rm -f "$R/.native/_topology/x86_64/package/libmoq5_jni.so"; }
# A stale POM elsewhere under build/ must not satisfy the exact-path row.
stale_pom_elsewhere() {
    rm -rf -- "$R/moq5-android/build/publications/release"
    mkdir -p "$R/moq5-android/build/intermediates/stale" || return 1
    printf '<project><groupId>org.openmoq</groupId><artifactId>moq5-android</artifactId><version>9.9.9</version><dependencies><dependency><artifactId>kotlinx-coroutines-core</artifactId><scope>compile</scope></dependency></dependencies></project>\n' \
        > "$R/moq5-android/build/intermediates/stale/pom-default.xml"
}
wrong_pom_coordinates() { sed -i.bak 's#<groupId>org.openmoq</groupId>#<groupId>com.example</groupId>#' "$R/moq5-android/build/publications/release/pom-default.xml" && rm -f "$R/moq5-android/build/publications/release/pom-default.xml.bak"; }
no_module()             { rm -f "$R/moq5-android/build/publications/release/module.json"; }
module_without_api()    { sed -i.bak 's#kotlinx-coroutines-core#kotlinx-serialization-json#g' "$R/moq5-android/build/publications/release/module.json" && rm -f "$R/moq5-android/build/publications/release/module.json.bak"; }
# No java-api variant at all: the row must say so rather than silently find
# nothing to check.
module_no_api_usage()   { sed -i.bak 's#"org.gradle.usage": "java-api"#"org.gradle.usage": "java-runtime"#' "$R/moq5-android/build/publications/release/module.json" && rm -f "$R/moq5-android/build/publications/release/module.json.bak"; }
module_test_leak()      { sed -i.bak 's#"kotlinx-coroutines-core"#"kotlinx-coroutines-test"#' "$R/moq5-android/build/publications/release/module.json" && rm -f "$R/moq5-android/build/publications/release/module.json.bak"; }
# A Gradle consumer prefers this file, so wrong coordinates here must fail even
# when the POM beside it is right (chats/libmoq/1515 F3).
module_wrong_coords()   { sed -i.bak 's#"module": "moq5-android"#"module": "moq5-android-oops"#' "$R/moq5-android/build/publications/release/module.json" && rm -f "$R/moq5-android/build/publications/release/module.json.bak"; }
module_empty_version()  { sed -i.bak 's#"version": "0.1.0",#"version": "",#' "$R/moq5-android/build/publications/release/module.json" && rm -f "$R/moq5-android/build/publications/release/module.json.bak"; }
module_unparseable()    { printf 'not json at all\n' > "$R/moq5-android/build/publications/release/module.json"; }
# Any quoted string is a second home for the number, not only a numeric one.
version_word_literal()  { sed -i.bak 's/^version = libmoqVersion()/version = "dev"/' "$R/moq5-android/build.gradle.kts" && rm -f "$R/moq5-android/build.gradle.kts.bak"; }
# A minSdk in the WRONG block is not the floor AGP publishes.
minsdk_wrong_block()    { sed -i.bak 's/^        minSdk = 21$//' "$R/moq5-android/build.gradle.kts" && printf 'val minSdk = 21\n' >> "$R/moq5-android/build.gradle.kts" && rm -f "$R/moq5-android/build.gradle.kts.bak"; }
no_pom()            { rm -f "$R/moq5-android/build/publications/release/pom-default.xml"; }
# `implementation` publishes as RUNTIME scope: present for a consumer's app,
# invisible to their compiler -- the exact defect 1501 F1 found by hand.
pom_runtime_scope() { sed -i.bak 's#<scope>compile</scope>#<scope>runtime</scope>#' "$R/moq5-android/build/publications/release/pom-default.xml" && rm -f "$R/moq5-android/build/publications/moq5-android.pom.bak"; }
pom_test_leak()     { sed -i.bak 's#</dependencies>#  <dependency><groupId>org.jetbrains.kotlinx</groupId><artifactId>kotlinx-coroutines-test</artifactId><scope>runtime</scope></dependency>\n</dependencies>#' "$R/moq5-android/build/publications/release/pom-default.xml" && rm -f "$R/moq5-android/build/publications/moq5-android.pom.bak"; }
probe_fails()       { printf '#!/bin/sh\necho "stub probe: consumer could not resolve the artifact"\nexit 1\n' > "$R/tools/probe-external-consumer.sh"; }

case_is selftest.publishable      1 publication.publishable       drop_publish
case_is selftest.sample-publishes 1 publication.unpublishable     sample_publishes
case_is selftest.moq5-api         1 publication.no-moq5-api       revive_moq5_api
case_is selftest.group            1 publication.group             wrong_group
case_is selftest.group-task-only  1 publication.group             group_only_in_task
case_is selftest.version-literal  1 publication.version           version_literal
case_is selftest.version-word     1 publication.version           version_word_literal
case_is selftest.version-absent   1 publication.version           no_version
case_is selftest.minsdk           1 publication.minsdk            no_minsdk
case_is selftest.aar-absent       1 publication.aar-present       no_aar
case_is selftest.aar-wrong-name   1 publication.aar-present       aar_wrong_name
case_is selftest.pom-version      1 publication.pom-version       pom_version_drift
case_is selftest.module-version-drift 1 publication.module-version module_version_drift
case_is selftest.version-authority 1 publication.version-authority no_version_header
case_is selftest.aar-native       1 publication.aar-native        aar_without_jni
case_is selftest.aar-rules        1 publication.aar-rules         aar_without_rules
case_is selftest.minsdk-wrong-block 1 publication.minsdk          minsdk_wrong_block
case_is selftest.aar-stale-abi    1 publication.aar-native        aar_stale_one_abi
case_is selftest.aar-no-topology  1 publication.aar-native        no_topology_package
case_is selftest.pom-absent       1 publication.pom               no_pom
case_is selftest.pom-stale-path   1 publication.pom               stale_pom_elsewhere
case_is selftest.pom-coordinates  1 publication.pom-coordinates   wrong_pom_coordinates
case_is selftest.pom-scope        1 publication.pom-coroutines    pom_runtime_scope
case_is selftest.pom-test-leak    1 publication.pom-test-leak     pom_test_leak
case_is selftest.module-absent    1 publication.module            no_module
case_is selftest.module-api       1 publication.module-coroutines module_without_api
case_is selftest.module-test-leak 1 publication.module-test-leak  module_test_leak
case_is selftest.module-coords    1 publication.module-coordinates module_wrong_coords
case_is selftest.module-version   1 publication.module-coordinates module_empty_version
case_is selftest.module-unparseable 1 publication.module-coordinates module_unparseable
case_is selftest.module-no-api-usage 1 publication.module-coroutines module_no_api_usage
case_is selftest.probe            1 publication.external-consumer probe_fails

# -- the probe's own repository-shape guard -----------------------------------
# The gate's external-consumer row is exercised through a stub, so the guard
# that keeps the probe causally local to its own run needs its own coverage:
# without it the probe would start passing the day org.openmoq exists publicly
# (chats/libmoq/1515 F1).
probe_case() {
    _label=$1; _want=$2; _body=$3
    printf '%s\n' "$_body" > "$W/settings.kts"
    if sh "$here/probe-external-consumer.sh" --check-settings "$W/settings.kts" > "$W/pout" 2>&1; then
        _got=0
    else
        _got=1
    fi
    if [ "$_got" = "$_want" ]; then
        pass "$_label" "$(sed -n 1p "$W/pout")"
    else
        fail "$_label" "expected exit $_want, got $_got: $(sed -n 1p "$W/pout")"
    fi
}

_good='dependencyResolutionManagement { repositories { maven { url = uri("r") content { includeGroup("org.openmoq") } }
google { content { excludeGroup("org.openmoq") } }
mavenCentral { content { excludeGroup("org.openmoq") } } } }'

probe_case selftest.probe-shape-ok        0 "$_good"
probe_case selftest.probe-public-fallback 1 'dependencyResolutionManagement { repositories { maven { url = uri("r") content { includeGroup("org.openmoq") } }
google()
mavenCentral() } }'
probe_case selftest.probe-one-exclusion   1 'dependencyResolutionManagement { repositories { maven { url = uri("r") content { includeGroup("org.openmoq") } }
google { content { excludeGroup("org.openmoq") } }
mavenCentral() } }'
probe_case selftest.probe-no-include      1 'dependencyResolutionManagement { repositories { maven { url = uri("r") }
google { content { excludeGroup("org.openmoq") } }
mavenCentral { content { excludeGroup("org.openmoq") } } } }'
probe_case selftest.probe-project-dep     1 "$_good
include(project(\":moq5-android\"))"
# The live false pass 1517 F1 found: the filters removed, their text left in a
# comment beside an unrestricted google()/mavenCentral().
probe_case selftest.probe-commented-out   1 'dependencyResolutionManagement { repositories { maven { url = uri("r") content { includeGroup("org.openmoq") } }
google() // content { excludeGroup("org.openmoq") }
mavenCentral() // content { excludeGroup("org.openmoq") } } }'
probe_case selftest.probe-block-comment   1 'dependencyResolutionManagement { repositories { maven { url = uri("r") content { includeGroup("org.openmoq") } }
google() /* content { excludeGroup("org.openmoq") } */
mavenCentral() /* content { excludeGroup("org.openmoq") } */ } }'
# Both exclusions present, but attached to the LOCAL repository instead of the
# public ones: a count would have accepted it.
probe_case selftest.probe-misattached     1 'dependencyResolutionManagement { repositories { maven { url = uri("r") content { includeGroup("org.openmoq") excludeGroup("org.openmoq") excludeGroup("org.openmoq") } }
google()
mavenCentral() } }'
probe_case selftest.probe-no-url          1 'dependencyResolutionManagement { repositories { maven { content { includeGroup("org.openmoq") } }
google { content { excludeGroup("org.openmoq") } }
mavenCentral { content { excludeGroup("org.openmoq") } } } }'
# The filters correct in pluginManagement (which resolves PLUGINS) and absent
# from dependencyResolutionManagement (which resolves org.openmoq).
probe_case selftest.probe-wrong-block      1 'pluginManagement { repositories { google { content { excludeGroup("org.openmoq") } }
mavenCentral { content { excludeGroup("org.openmoq") } } } }
dependencyResolutionManagement { repositories { maven { url = uri("r") content { includeGroup("org.openmoq") } }
google()
mavenCentral() } }'
# The probe's REAL template, emitted by the probe itself, must satisfy the
# probe's own guard: a copy in this file could drift from what it writes.
if sh "$here/probe-external-consumer.sh" --emit-settings "$W/real.kts" >/dev/null 2>&1 &&
    sh "$here/probe-external-consumer.sh" --check-settings "$W/real.kts" >/dev/null 2>&1; then
    pass selftest.probe-real-template "the settings the probe generates satisfy the probe's own guard"
else
    fail selftest.probe-real-template "the probe generates settings its own guard rejects: $(sh "$here/probe-external-consumer.sh" --check-settings "$W/real.kts" 2>&1 | sed -n 1p)"
fi

printf '\n'
if [ "$fails" -eq 0 ]; then
    printf 'publication gate self-test: 0 failure(s)\n'
    exit 0
fi
printf 'publication gate self-test: %s failure(s)\n' "$fails"
exit 1
