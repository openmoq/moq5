#!/bin/sh
# MoQ5 Android/Kotlin bindings -- durable toolchain contract.
#
# Every pin comes from gradle/toolchain.properties; this script hard-codes no
# version and no installation path. Each check FAILS BY NAME
# (FAIL[<check>]: <reason>) so a log says exactly which component is wrong.
#
# Three rules that make the result trustworthy:
#   * a missing command or file is a FAILURE, never a skip. There is no
#     "not installed, so not checked" outcome anywhere below.
#   * WHERE a component lives is discovered from standard sources, never
#     checked in. A missing source, and a conflict between two sources, each
#     fail by name rather than falling back silently.
#   * nothing here consults a Gradle on PATH. The Gradle version is read from
#     the checked-in wrapper, and running the contract through a non-wrapper
#     Gradle is itself a failure (check gradle.wrapper.invocation).
#
# Exit status: 0 when every check passes, 1 otherwise.

set -u

here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/.." && pwd)
props="$root/gradle/toolchain.properties"

fails=0
pass() { printf 'PASS[%s]: %s\n' "$1" "$2"; }
fail() { printf 'FAIL[%s]: %s\n' "$1" "$2"; fails=$((fails + 1)); }
finish() { printf '\ntoolchain contract: %d failure(s)\n' "$fails"; [ "$fails" -eq 0 ] || exit 1; exit 0; }

# --- the ONE declaration authority ----------------------------------------
# Parsing, cardinality, grammar and hashing all live in tools/lib-declaration.sh,
# which tools/gradlew-verified runs BEFORE Gradle launches. Sourcing the same
# file here is deliberate: a second parser in this script could accept a
# declaration the pre-wrapper layer rejects, or the reverse.
. "$here/lib-declaration.sh"

prop() { props_value "$props" "$1"; }

# Numeric dotted-version comparison: 0 = equal, 1 = a>b, 2 = a<b.
vercmp() {
    a=$1; b=$2
    [ "$a" = "$b" ] && return 0
    lo=$(printf '%s\n%s\n' "$a" "$b" | sort -t. -k1,1n -k2,2n -k3,3n | head -1)
    [ "$lo" = "$a" ] && return 2
    return 1
}
vge() { vercmp "$1" "$2"; [ $? -eq 2 ] && return 1; return 0; }

if [ ! -f "$props" ]; then
    fail toolchain.declaration "missing $props"
    finish
fi
pass toolchain.declaration "$props"

# The complete declaration contract -- required pins, cardinality, grammar, the
# project-owned property files, the wrapper property keys and the wrapper
# script. Identical to what the canonical launcher already enforced.
pf=$(decl_preflight "$root") || true
fails=$((fails + pf))
[ "$fails" -eq 0 ] || finish

GRADLE_VERSION=$(prop gradle.version)
GRADLE_SHA=$(prop gradle.distribution.sha256)
GRADLE_URL=$(prop gradle.distribution.url)
WRAPPER_JAR_SHA=$(prop gradle.wrapper.jar.sha256)
JDK_MAJOR=$(prop jdk.major)
AGP_VERSION=$(prop agp.version)
AGP_MIN_GRADLE=$(prop agp.minGradle)
AGP_MIN_JDK=$(prop agp.minJdk)
SDK_PLATFORM=$(prop sdk.platform)
SDK_BUILD_TOOLS=$(prop sdk.buildTools)
AGP_MIN_BUILD_TOOLS=$(prop agp.minBuildTools)
SDK_CMDLINE_TOOLS=$(prop sdk.cmdlineTools)
SDK_NDK=$(prop sdk.ndk)

pass toolchain.grammar "every declared version and digest is well formed"

# --- 1. internal consistency of the pins themselves ------------------------
if vge "$GRADLE_VERSION" "$AGP_MIN_GRADLE"; then
    pass pins.gradleVsAgp "gradle $GRADLE_VERSION >= AGP $AGP_VERSION minimum $AGP_MIN_GRADLE"
else
    fail pins.gradleVsAgp "gradle $GRADLE_VERSION is below AGP $AGP_VERSION minimum $AGP_MIN_GRADLE"
fi
if vge "$JDK_MAJOR" "$AGP_MIN_JDK"; then
    pass pins.jdkVsAgp "jdk $JDK_MAJOR >= AGP $AGP_VERSION minimum $AGP_MIN_JDK"
else
    fail pins.jdkVsAgp "jdk $JDK_MAJOR is below AGP $AGP_VERSION minimum $AGP_MIN_JDK"
fi
if vge "$SDK_BUILD_TOOLS" "$AGP_MIN_BUILD_TOOLS"; then
    pass pins.buildToolsVsAgp "build-tools $SDK_BUILD_TOOLS >= AGP minimum $AGP_MIN_BUILD_TOOLS"
else
    fail pins.buildToolsVsAgp "build-tools $SDK_BUILD_TOOLS is below AGP minimum $AGP_MIN_BUILD_TOOLS"
fi

# --- 2. the daemon JVM criterion (validated by the shared preflight) -------
pass jdk.criteria "daemon JVM criterion toolchainVersion=$(props_value "$root/gradle/gradle-daemon-jvm.properties" toolchainVersion)"

# --- 3. JDK: DISCOVERED, never checked in ----------------------------------
# JAVA_HOME is the explicit input; `java` on PATH is the ordinary fallback.
# Neither is a pin, and having neither fails by name.
jdk_src=""
JDK_BIN=""
if [ -n "${JAVA_HOME:-}" ]; then
    if [ -x "$JAVA_HOME/bin/java" ]; then
        JDK_BIN="$JAVA_HOME/bin/java"; jdk_src="JAVA_HOME"
    else
        fail jdk.discovery "JAVA_HOME is set to '$JAVA_HOME' but has no executable bin/java"
    fi
elif command -v java >/dev/null 2>&1; then
    JDK_BIN=$(command -v java); jdk_src="PATH"
else
    fail jdk.discovery "no JDK: JAVA_HOME is unset and no java is on PATH"
fi
if [ -n "$JDK_BIN" ]; then
    pass jdk.discovery "java from $jdk_src"
    jver=$("$JDK_BIN" -version 2>&1 | head -1 | sed 's/.*version "\([0-9][0-9]*\).*/\1/')
    if [ "$jver" = "$JDK_MAJOR" ]; then
        pass jdk.version "java $jver ($jdk_src)"
    else
        fail jdk.version "declared jdk.major=$JDK_MAJOR but the discovered JDK reports $jver"
    fi
fi

# --- 3b. gradle.properties must declare no host path and fail on warnings ---
# gradle.properties is validated by the shared preflight (no host JDK path,
# exactly one org.gradle.warning.mode, and its value 'fail').
pass gradle.properties.javaHome "no checked-in JDK path"
pass gradle.properties.warningMode "$(props_value "$root/gradle.properties" org.gradle.warning.mode)"

# --- 4. Gradle wrapper, including the JAR's own provenance -----------------
wrapper_props="$root/gradle/wrapper/gradle-wrapper.properties"
wrapper_jar="$root/gradle/wrapper/gradle-wrapper.jar"
gradlew="$root/gradlew"
if [ ! -f "$wrapper_props" ]; then
    fail gradle.wrapper "missing $wrapper_props (no pinned wrapper checked in)"
elif [ ! -f "$wrapper_jar" ]; then
    fail gradle.wrapper "missing $wrapper_jar"
elif [ ! -x "$gradlew" ]; then
    fail gradle.wrapper "missing or non-executable $gradlew"
else
    pass gradle.wrapper "$gradlew"
    jsha=$(decl_sha256 "$wrapper_jar") || jrc=$?
    if [ -z "$jsha" ]; then
        if [ "${jrc:-1}" -eq 2 ]; then
            fail gradle.wrapper.jarSha256 \
                "no sha256sum or shasum available to verify $wrapper_jar"
        else
            fail gradle.wrapper.jarSha256 "could not hash $wrapper_jar"
        fi
    elif [ "$jsha" = "$WRAPPER_JAR_SHA" ]; then
        pass gradle.wrapper.jarSha256 "$jsha"
    else
        fail gradle.wrapper.jarSha256 "jar hashes $jsha, declared $WRAPPER_JAR_SHA"
    fi
    wurl=$(props_value "$wrapper_props" distributionUrl)
    if [ "$wurl" = "$GRADLE_URL" ]; then
        pass gradle.distributionUrl "$wurl"
    else
        fail gradle.distributionUrl "wrapper points at '$wurl', declared '$GRADLE_URL'"
    fi
    wsha=$(props_value "$wrapper_props" distributionSha256Sum)
    if [ -z "$wsha" ]; then
        fail gradle.distributionSha256 "wrapper declares no distributionSha256Sum"
    elif [ "$wsha" = "$GRADLE_SHA" ]; then
        pass gradle.distributionSha256 "$wsha"
    else
        fail gradle.distributionSha256 "wrapper declares '$wsha', declared '$GRADLE_SHA'"
    fi
    # The version the wrapper actually RUNS, not the one it claims in a path.
    gv=$("$gradlew" --version -p "$root" 2>/dev/null | sed -n 's/^Gradle *//p' | head -1)
    if [ -z "$gv" ]; then
        fail gradle.version "the wrapper did not report a Gradle version"
    elif [ "$gv" = "$GRADLE_VERSION" ]; then
        pass gradle.version "$gv (via the checked-in wrapper)"
    else
        fail gradle.version "wrapper runs Gradle $gv, declared $GRADLE_VERSION"
    fi
fi

# --- 5. this contract must not be satisfied by a host Gradle ---------------
if [ -n "${MOQ5_TOOLCHAIN_VIA_WRAPPER:-}" ]; then
    if [ "${MOQ5_TOOLCHAIN_VIA_WRAPPER}" = "$GRADLE_VERSION" ]; then
        pass gradle.wrapper.invocation "invoked by the pinned wrapper ($GRADLE_VERSION)"
    else
        fail gradle.wrapper.invocation \
            "invoked by Gradle ${MOQ5_TOOLCHAIN_VIA_WRAPPER}, declared $GRADLE_VERSION"
    fi
else
    pass gradle.wrapper.invocation "standalone run (no Gradle involved)"
fi

# --- 6. Android SDK: DISCOVERED from the standard sources ------------------
# ANDROID_HOME and ANDROID_SDK_ROOT are the environment inputs; an untracked
# local.properties (sdk.dir=) is the per-checkout fallback. Two sources that
# disagree is a named conflict, not a silent precedence win.
sdk_src=""
SDK_ROOT=""
lp="$root/local.properties"
lp_sdk=""
[ -f "$lp" ] && lp_sdk=$(props_value "$lp" sdk.dir)
if [ -n "${ANDROID_HOME:-}" ] && [ -n "${ANDROID_SDK_ROOT:-}" ] \
   && [ "${ANDROID_HOME}" != "${ANDROID_SDK_ROOT}" ]; then
    fail sdk.discovery \
        "ANDROID_HOME ('${ANDROID_HOME}') and ANDROID_SDK_ROOT ('${ANDROID_SDK_ROOT}') disagree"
elif [ -n "${ANDROID_HOME:-}" ]; then
    SDK_ROOT="${ANDROID_HOME}"; sdk_src="ANDROID_HOME"
elif [ -n "${ANDROID_SDK_ROOT:-}" ]; then
    SDK_ROOT="${ANDROID_SDK_ROOT}"; sdk_src="ANDROID_SDK_ROOT"
elif [ -n "$lp_sdk" ]; then
    SDK_ROOT="$lp_sdk"; sdk_src="local.properties"
else
    fail sdk.discovery \
        "no Android SDK: set ANDROID_HOME or ANDROID_SDK_ROOT, or put sdk.dir= in an untracked local.properties"
fi
if [ -n "$SDK_ROOT" ] && [ -n "$lp_sdk" ] && [ "$lp_sdk" != "$SDK_ROOT" ] \
   && [ "$sdk_src" != "local.properties" ]; then
    fail sdk.discovery \
        "local.properties sdk.dir ('$lp_sdk') disagrees with $sdk_src ('$SDK_ROOT')"
    SDK_ROOT=""
fi

if [ -n "$SDK_ROOT" ]; then
    if [ ! -d "$SDK_ROOT" ]; then
        fail sdk.root "$sdk_src names '$SDK_ROOT', which is not a directory"
    else
        pass sdk.discovery "SDK from $sdk_src"
        pass sdk.root "$SDK_ROOT"
        if [ -d "$SDK_ROOT/platforms/$SDK_PLATFORM" ]; then
            pass sdk.platform "$SDK_PLATFORM"
        else
            fail sdk.platform "missing $SDK_ROOT/platforms/$SDK_PLATFORM"
        fi
        if [ -d "$SDK_ROOT/build-tools/$SDK_BUILD_TOOLS" ]; then
            pass sdk.buildTools "$SDK_BUILD_TOOLS"
        else
            fail sdk.buildTools "missing $SDK_ROOT/build-tools/$SDK_BUILD_TOOLS"
        fi

        sdkman="$SDK_ROOT/cmdline-tools/$SDK_CMDLINE_TOOLS/bin/sdkmanager"
        if [ ! -x "$sdkman" ]; then
            fail sdk.cmdlineTools "missing $sdkman"
        else
            ctrev=$(sed -n 's/^Pkg.Revision=//p' \
                    "$SDK_ROOT/cmdline-tools/$SDK_CMDLINE_TOOLS/source.properties" 2>/dev/null)
            if [ "$ctrev" = "$SDK_CMDLINE_TOOLS" ]; then
                pass sdk.cmdlineTools "$SDK_CMDLINE_TOOLS"
            else
                fail sdk.cmdlineTools \
                    "cmdline-tools/$SDK_CMDLINE_TOOLS reports revision '$ctrev', declared $SDK_CMDLINE_TOOLS"
            fi
        fi

        ndkdir="$SDK_ROOT/ndk/$SDK_NDK"
        if [ ! -d "$ndkdir" ]; then
            fail sdk.ndk "missing $ndkdir"
        else
            nrev=$(sed -n 's/^Pkg.Revision *= *//p' "$ndkdir/source.properties" 2>/dev/null)
            if [ "$nrev" = "$SDK_NDK" ]; then
                pass sdk.ndk "$SDK_NDK"
            else
                fail sdk.ndk "$ndkdir reports revision '$nrev', declared $SDK_NDK"
            fi
            # The NDK's prebuilt host tag is DERIVED, not checked in: it is the
            # NDK's own naming for the host it runs on, and hard-coding one made
            # the contract macOS-only. The tag is the lower-cased OS name plus
            # the NDK's single prebuilt architecture suffix -- the NDK ships one
            # x86_64 host toolchain per OS, including on Apple silicon, so the
            # suffix is a property of the NDK layout rather than of this host.
            host_os=""
            case "$(uname -s)" in
                Darwin|Linux) host_os=$(uname -s | tr '[:upper:]' '[:lower:]') ;;
                MINGW*|MSYS*|CYGWIN*) host_os=windows ;;
            esac
            host_tag=""
            [ -n "$host_os" ] && host_tag="${host_os}-x86_64"
            if [ -z "$host_tag" ]; then
                fail sdk.ndk.hostTag "no NDK prebuilt host tag is known for $(uname -s)"
            else
                pre="$ndkdir/toolchains/llvm/prebuilt/$host_tag"
                if [ ! -d "$pre" ]; then
                    fail sdk.ndk.hostTag "derived host tag '$host_tag' has no prebuilt at $pre"
                else
                    pass sdk.ndk.hostTag "$host_tag (derived from $(uname -s))"
                    # Content, not just presence of the directory. An sdkmanager
                    # run interrupted mid-unpack leaves source.properties in
                    # place while the toolchain is still incomplete, so the
                    # pieces the Phase 0.2 cross-build will actually invoke are
                    # named individually -- for BOTH required ABIs.
                    for t in "$pre/bin/clang" \
                             "$pre/bin/llvm-ar" \
                             "$pre/bin/aarch64-linux-android21-clang" \
                             "$pre/bin/x86_64-linux-android21-clang" \
                             "$pre/sysroot/usr/lib/aarch64-linux-android" \
                             "$pre/sysroot/usr/lib/x86_64-linux-android" \
                             "$ndkdir/build/cmake/android.toolchain.cmake" \
                             "$ndkdir/package.xml"; do
                        if [ -e "$t" ]; then
                            pass sdk.ndk.content "${t#"$ndkdir"/}"
                        else
                            fail sdk.ndk.content "missing $t"
                        fi
                    done
                    # The toolchain must actually RUN, not merely exist on disk.
                    if [ -x "$pre/bin/clang" ] && "$pre/bin/clang" --version >/dev/null 2>&1; then
                        pass sdk.ndk.clangRuns "$pre/bin/clang --version"
                    else
                        fail sdk.ndk.clangRuns "$pre/bin/clang did not execute"
                    fi
                fi
            fi
        fi
    fi
fi

finish
