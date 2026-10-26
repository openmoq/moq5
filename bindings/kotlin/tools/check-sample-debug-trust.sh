#!/bin/sh
# The samples' debug-only trust overlay, judged in source, in the merged
# manifests and in the built APKs.
#
# Each sample's DEBUG build carries exactly one Network Security Config whose
# only content is `debug-overrides > trust-anchors > certificates src="user"`,
# so a locally created CA installed as a user CA on a dedicated emulator is an
# anchor for the debuggable app. Its RELEASE build carries neither the manifest
# attribute nor the resource. Verification itself is never relaxed: the config
# has no base-config, no domain-config, no cleartext permission, no pin set and
# no bundled anchor, and the binding's default trust policy is untouched.
#
# Every manifest is read as XML: the attribute is the one `application`
# element's namespaced android:networkSecurityConfig, never matching text, so a
# comment or a string cannot stand in for it. Every APK is read by aapt2, and a
# reading counts only when aapt2 succeeds, says nothing on stderr, and returns
# a recognisable inventory: absence is never inferred from a failed or silent
# reader.
#
# This script reads artifacts ALREADY BUILT; it builds nothing, so on its own it
# cannot say they came from the current sources. The registered Gradle task
# `verifySampleDebugTrust` depends on all four sample assemble tasks and is the
# only form that proves the artifacts are the current sources' own. To run it
# directly, build first:
#   tools/gradlew-verified :samples:player:assembleDebug :samples:player:assembleRelease \
#       :samples:publisher:assembleDebug :samples:publisher:assembleRelease
#
# usage: check-sample-debug-trust.sh [bindings/kotlin root]
set -eu

_self=$(basename "$0")
root=${1:-$(cd "$(dirname "$0")/.." && pwd)}
[ -f "$root/settings.gradle.kts" ] || { echo "$_self: no settings.gradle.kts under $root" >&2; exit 2; }
command -v python3 >/dev/null 2>&1 || { echo "$_self: python3 is required to read XML" >&2; exit 2; }

fails=0
pass() { printf 'PASS[%s]: %s\n' "$1" "$2"; }
fail() { fails=$((fails + 1)); printf 'FAIL[%s]: %s\n' "$1" "$2"; }

# aapt2 from the pinned build-tools, in the SDK local.properties names.
_pin=$(sed -n 's/^sdk\.buildTools=//p' "$root/gradle/toolchain.properties")
_sdk=${ANDROID_SDK_ROOT:-${ANDROID_HOME:-}}
if [ -z "$_sdk" ] && [ -f "$root/local.properties" ]; then
    _sdk=$(sed -n 's/^sdk\.dir=//p' "$root/local.properties")
fi
# MOQ5_AAPT2 substitutes the reader; the selftest uses it to prove a reader
# that answers nothing, or answers with a diagnostic, proves nothing.
aapt2=${MOQ5_AAPT2:-"$_sdk/build-tools/$_pin/aapt2"}
[ -x "$aapt2" ] || { echo "$_self: no aapt2 at $aapt2 (sdk.buildTools=$_pin)" >&2; exit 2; }

scratch=$(mktemp -d "${TMPDIR:-/tmp}/sample-debug-trust-read.XXXXXX")
cleanup() { rm -rf -- "$scratch"; }
trap cleanup EXIT INT TERM

SAMPLES='player publisher'
CONFIG_REF='@xml/network_security_config'

# One XML reader for every judgement, so the rules cannot drift apart.
#   manifest FILE       -> "value <v>" | "absent" | "error <reason>"   (source or merged XML)
#   config FILE         -> "ok" | "<reason>"                            (source XML)
#   tree-manifest FILE  -> "value <v>" | "absent" | "error <reason>"   (aapt2 xmltree text)
#   tree-config FILE    -> "ok" | "<reason>"                            (aapt2 xmltree text)
xmlq() {
    python3 - "$@" <<'PY'
import re, sys
mode, path = sys.argv[1], sys.argv[2]
ANDROID = 'http://schemas.android.com/apk/res/android'
NSC = 'networkSecurityConfig'

def shape(root):
    """root is (tag, attrs, children): the accepted config, exactly."""
    tag, a, ch = root
    if tag != 'network-security-config':
        return 'the root is <%s>, not <network-security-config>' % tag
    node = root
    for want in ('debug-overrides', 'trust-anchors', 'certificates'):
        tag, a, ch = node
        if a:
            return '<%s> carries attribute(s) %s' % (tag, sorted(a))
        if [c[0] for c in ch] != [want]:
            return 'expected exactly <%s> under <%s>, found %s' % (want, tag, [c[0] for c in ch] or 'nothing')
        node = ch[0]
    tag, a, ch = node
    if a != {'src': 'user'}:
        return '<certificates> must be exactly src="user", found %s' % a
    if ch:
        return '<certificates> has children'
    return 'ok'

def from_etree(el):
    # Attribute identity is kept whole: a namespaced key stays "{uri}name", so
    # x:src is never mistaken for the unqualified src the config requires.
    return (el.tag, dict(el.attrib), [from_etree(c) for c in el])

def application_value(apps, get):
    if len(apps) != 1:
        return 'error the manifest has %d <application> elements, expected 1' % len(apps)
    v = get(apps[0])
    return 'absent' if v is None else 'value ' + v

def parse_tree(text):
    """aapt2's xmltree dump: E: elements and A: attributes, nested by indent."""
    top, stack = [], []
    for line in text.splitlines():
        m = re.match(r'^( *)(E|A): (.*)$', line)
        if not m:
            continue
        depth, kind, rest = len(m.group(1)), m.group(2), m.group(3)
        while stack and stack[-1][0] >= depth:
            stack.pop()
        if kind == 'E':
            node = (rest.split(' ', 1)[0], {}, [])
            (stack[-1][1][2] if stack else top).append(node)
            stack.append((depth, node))
        else:
            if not stack:
                raise ValueError('an attribute outside any element')
            am = re.match(r'^(?:(\S+):)?([A-Za-z_][\w.]*)(?:\(0x[0-9a-f]+\))?=(.*)$', rest)
            if not am:
                raise ValueError('an unreadable attribute line: ' + rest)
            ns, name, value = am.group(1) or '', am.group(2), am.group(3)
            raw = re.match(r'^"(.*)" \(Raw: ".*"\)$', value)
            value = raw.group(1) if raw else value.strip('"')
            stack[-1][1][1][(ns + '|' + name) if ns else name] = value
    return top

try:
    if mode in ('manifest', 'config'):
        import xml.etree.ElementTree as E
        root = E.parse(path).getroot()
        if mode == 'config':
            print(shape(from_etree(root)))
        elif root.tag != 'manifest':
            print('error the root is <%s>, not <manifest>' % root.tag)
        else:
            print(application_value(root.findall('application'), lambda a: a.get('{%s}%s' % (ANDROID, NSC))))
    else:
        top = parse_tree(open(path).read())
        if mode == 'tree-config':
            if len(top) != 1:
                print('the compiled config has %d root elements' % len(top))
            else:
                t, a, ch = top[0]
                print(shape((t, {k.split('|')[-1]: v for k, v in a.items()}, ch)))
        elif len(top) != 1 or top[0][0] != 'manifest':
            print('error the compiled manifest has no single <manifest> root')
        else:
            apps = [c for c in top[0][2] if c[0] == 'application']
            print(application_value(apps, lambda a: a[1].get(ANDROID + '|' + NSC)))
except Exception as e:
    print(('error %s' if mode in ('manifest', 'tree-manifest') else 'does not parse: %s') % e)
PY
}

# read_apk <label> <out> <aapt2 args...>: a reading that counts, or why not.
# It counts only on exit 0 with an empty stderr; the caller then requires a
# recognisable inventory.
read_apk() {
    _label=$1; _out=$2; shift 2
    if ! "$aapt2" "$@" > "$_out" 2> "$_out.err"; then
        printf 'aapt2 %s failed: %s' "$_label" "$(tr '\n' ' ' < "$_out.err")"
        return 1
    fi
    if [ -s "$_out.err" ]; then
        printf 'aapt2 %s reported: %s' "$_label" "$(tr '\n' ' ' < "$_out.err")"
        return 1
    fi
    return 0
}

for s in $SAMPLES; do
    dir="$root/samples/$s"
    [ -d "$dir" ] || { fail "trust.$s.module" "no module at samples/$s"; continue; }

    # -- source ---------------------------------------------------------------
    dm="$dir/src/debug/AndroidManifest.xml"
    dc="$dir/src/debug/res/xml/network_security_config.xml"
    if [ ! -f "$dm" ]; then
        fail "trust.$s.source-debug" "samples/$s has no src/debug/AndroidManifest.xml applying the debug config"
    else
        _v=$(xmlq manifest "$dm")
        if [ "$_v" = "value $CONFIG_REF" ]; then
            pass "trust.$s.source-debug" "src/debug's one <application> sets android:networkSecurityConfig to $CONFIG_REF"
        else
            fail "trust.$s.source-debug" "src/debug/AndroidManifest.xml <application> android:networkSecurityConfig is '$_v'; expected $CONFIG_REF"
        fi
    fi
    if [ ! -f "$dc" ]; then
        fail "trust.$s.source-config" "samples/$s has no src/debug/res/xml/network_security_config.xml"
    else
        _shape=$(xmlq config "$dc")
        if [ "$_shape" = ok ]; then
            pass "trust.$s.source-config" "the debug config is exactly debug-overrides > trust-anchors > certificates src=\"user\""
        else
            fail "trust.$s.source-config" "src/debug/res/xml/network_security_config.xml: $_shape"
        fi
    fi
    # Every other source set: no attribute, no config resource.
    _leak=""
    for f in $(find "$dir/src" -name AndroidManifest.xml ! -path "$dir/src/debug/*" 2>/dev/null | sort); do
        _v=$(xmlq manifest "$f")
        [ "$_v" = absent ] || _leak="$_leak ${f#"$dir"/}($_v)"
    done
    for f in $(find "$dir/src" -path '*/res/xml/*' -name 'network_security_config*' ! -path "$dir/src/debug/*" 2>/dev/null | sort); do
        _leak="$_leak ${f#"$dir"/}"
    done
    if [ -n "$_leak" ]; then
        fail "trust.$s.source-release" "a non-debug source set carries the trust config, or cannot be read:$_leak"
    else
        pass "trust.$s.source-release" "no non-debug source set sets the attribute or carries the config"
    fi

    # -- merged manifests ---------------------------------------------------------
    for v in debug release; do
        _V=$(printf '%s' "$v" | awk '{ print toupper(substr($0,1,1)) substr($0,2) }')
        mm="$dir/build/intermediates/merged_manifest/$v/process${_V}MainManifest/AndroidManifest.xml"
        if [ ! -f "$mm" ]; then
            fail "trust.$s.merged-$v" "no merged $v manifest at ${mm#"$root"/}; build :samples:$s:assemble$_V first"
            continue
        fi
        _v=$(xmlq manifest "$mm")
        if [ "$v" = debug ]; then _want="value $CONFIG_REF"; else _want=absent; fi
        if [ "$_v" = "$_want" ]; then
            pass "trust.$s.merged-$v" "the merged $v manifest's <application> attribute is '$_v'"
        else
            fail "trust.$s.merged-$v" "the merged $v manifest's <application> attribute is '$_v', expected '$_want'"
        fi
    done

    # -- APKs -------------------------------------------------------------------
    for v in debug release; do
        if [ "$v" = debug ]; then apk="$dir/build/outputs/apk/debug/$s-debug.apk"; else apk="$dir/build/outputs/apk/release/$s-release-unsigned.apk"; fi
        if [ ! -f "$apk" ]; then
            fail "trust.$s.apk-$v" "no $v APK at ${apk#"$root"/}"
            continue
        fi
        _m="$scratch/$s-$v.manifest"; _r="$scratch/$s-$v.resources"
        if ! _why=$(read_apk manifest "$_m" dump xmltree --file AndroidManifest.xml "$apk"); then
            fail "trust.$s.apk-$v" "$_why"
            continue
        fi
        if ! _why=$(read_apk resources "$_r" dump resources "$apk"); then
            fail "trust.$s.apk-$v" "$_why"
            continue
        fi
        # A recognisable inventory: aapt2's resource dump opens with its
        # "Binary APK" header even for an APK with no resources, and a manifest
        # dump has one <manifest> root.
        if [ "$(head -n 1 "$_r")" != "Binary APK" ]; then
            fail "trust.$s.apk-$v" "aapt2 returned no recognisable resource inventory"
            continue
        fi
        _attr=$(xmlq tree-manifest "$_m")
        case $_attr in
            error*)
                fail "trust.$s.apk-$v" "the compiled manifest: $_attr"
                continue
                ;;
        esac
        _res=$(grep -cE '^[[:space:]]+resource 0x[0-9a-f]+ xml/network_security_config$' "$_r" || true)
        if [ "$v" = debug ]; then
            _c="$scratch/$s-$v.config"
            if [ "$_res" != 1 ]; then
                fail "trust.$s.apk-debug" "the debug APK carries the config resource $_res time(s), expected 1"
                continue
            fi
            if ! _why=$(read_apk config "$_c" dump xmltree --file res/xml/network_security_config.xml "$apk"); then
                fail "trust.$s.apk-debug" "$_why"
                continue
            fi
            _shape=$(xmlq tree-config "$_c")
            case $_attr in
                "value @0x"*) _bound=yes ;;
                *) _bound=no ;;
            esac
            if [ "$_bound" = yes ] && [ "$_shape" = ok ]; then
                pass "trust.$s.apk-debug" "the debug APK's <application> sets networkSecurityConfig, and its one config is exactly the user-anchor overlay"
            else
                fail "trust.$s.apk-debug" "the debug APK: <application> attribute '$_attr', compiled config '$_shape'"
            fi
        else
            if [ "$_attr" = absent ] && [ "$_res" = 0 ]; then
                pass "trust.$s.apk-release" "the release APK's <application> has no networkSecurityConfig and it carries no config resource"
            else
                fail "trust.$s.apk-release" "the release APK: <application> attribute '$_attr', config resource $_res time(s); expected absent and 0"
            fi
        fi
    done
done

if [ "$fails" -ne 0 ]; then
    echo "$_self: $fails check(s) failed" >&2
    exit 1
fi
if [ "${MOQ5_TRUST_FRESH_BUILD:-}" = 1 ]; then
    echo "$_self: every sample's debug trust overlay is debug-only and exact, in the artifacts this build produced"
else
    echo "$_self: every sample's debug trust overlay is debug-only and exact, in PREVIOUSLY BUILT artifacts; verifySampleDebugTrust rebuilds them first"
fi
