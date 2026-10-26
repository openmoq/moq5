#!/bin/sh
# Do the declared third-party notices actually reach what we ship?
#
# The topology producer collects one notice per declared component per ABI and
# records component, digest, archive-relative path and pinned source in
# `<abi>/licenses.manifest` (tools/build-jni-topology.sh). Collecting them
# proves nothing on its own: what matters is whether those exact bytes are
# inside the artifact a consumer resolves, and inside the APK that consumer
# builds.
#
# THE EXPECTED CLOSURE IS THIS SCRIPT'S OWN AUTHORITY, not the manifest's.
# A manifest is an input to be validated, never the definition of success: a
# missing row, a renamed component, a duplicate, a malformed row or a forged
# digest must all fail, and none of them may shrink the required set. So the
# order is: validate the declarations against the required components and
# against the pinned sources' ACTUAL bytes, require the ABIs to agree, and only
# then look inside the archive.
#
# SHIPPED METADATA is checked as content, not as a filename. If the artifact
# carries a notice manifest of its own, it must follow the shipped schema --
# component, digest, archive-relative path -- every path must resolve to a
# member whose bytes hash to the declared digest, and no field may carry an
# absolute path. That is a different property from an absolute archive entry
# name, and both are checked separately.
#
# SCOPE. This is delivery of the DECLARED closure. It is not a legal-compliance
# judgement and it does not discover obligations that closure omits.
#
# AUTHORITY. In its production mode this script does not re-derive the accepted
# declaration contract: it RUNS `check-jni-topology.sh` over the same tree first
# and refuses everything if that audit does not pass. That audit already pins
# the exact ABI inventory, the canonical component-to-source mapping and the
# exact collected destinations, so two mutually agreeing forged manifests cannot
# satisfy this one either. The canonical mapping restated below is a redundant
# copy whose divergence the accepted audit would catch.
#
# The self-test drives disposable fixtures, which the accepted audit cannot be
# pointed at. That is `--fixture-mode`, which REQUIRES an explicit `--canonical`
# mapping and REFUSES to run against the real topology directory, so a test mode
# can never become the production invocation's source authority.
#
# Usage:
#   check-artifact-notices.sh --topology <dir> [--aar <file>] [--apk <file>]
#   check-artifact-notices.sh --fixture-mode --topology <dir> --canonical <file> ...
#
# With no archive it validates the declarations alone. Exit 0 when every check
# passes, non-zero with named failures otherwise.
set -u
LC_ALL=C; export LC_ALL

_self=${0##*/}
case $0 in
    */*) _selfdir=${0%/*} ;;
    *)   _selfdir=. ;;
esac
here=$(cd "$_selfdir" && pwd) || { echo "$_self: cannot resolve the script directory" >&2; exit 2; }

topology=""
aar=""
apk=""
canonical=""
fixture=0
while [ $# -gt 0 ]; do
    case $1 in
        --topology) topology=${2:?--topology needs a directory}; shift 2 ;;
        --aar) aar=${2:?--aar needs a path}; shift 2 ;;
        --apk) apk=${2:?--apk needs a path}; shift 2 ;;
        --canonical) canonical=${2:?--canonical needs a file}; shift 2 ;;
        --fixture-mode) fixture=1; shift ;;
        *) echo "$_self: unknown argument $1" >&2; exit 2 ;;
    esac
done
[ -n "$topology" ] || { echo "$_self: --topology is required" >&2; exit 2; }
[ -d "$topology" ] || { echo "$_self: no topology directory at $topology" >&2; exit 2; }
command -v python3 >/dev/null 2>&1 || { echo "$_self: no python3 for archive inspection" >&2; exit 2; }

root=$(cd "$here/.." && pwd) || { echo "$_self: cannot resolve bindings/kotlin" >&2; exit 2; }
repo=$(cd "$root/../.." && pwd) || { echo "$_self: cannot resolve the worktree root" >&2; exit 2; }
real_topology=$(cd "$root/.native/_topology" 2>/dev/null && pwd -P || printf '%s' "$root/.native/_topology")
given=$(cd "$topology" && pwd -P) || { echo "$_self: cannot resolve $topology" >&2; exit 2; }

if [ "$fixture" -eq 1 ]; then
    [ -n "$canonical" ] || { echo "$_self: --fixture-mode requires --canonical" >&2; exit 2; }
    [ -f "$canonical" ] || { echo "$_self: no canonical mapping at $canonical" >&2; exit 2; }
    if [ "$given" = "$real_topology" ]; then
        echo "$_self: --fixture-mode must not be pointed at the real topology directory" >&2
        exit 2
    fi
else
    [ -z "$canonical" ] || { echo "$_self: --canonical is only accepted with --fixture-mode" >&2; exit 2; }
    if [ "$given" != "$real_topology" ]; then
        echo "$_self: the production mode audits $real_topology; use --fixture-mode for a fixture" >&2
        exit 2
    fi
    # THE ACCEPTED AUTHORITY RUNS FIRST. Its contract -- exact ABIs, canonical
    # sources, exact destinations -- is the one this script relies on.
    if ! /bin/sh "$here/check-jni-topology.sh" > "${TMPDIR:-/tmp}/moq5-notices-audit.$$" 2>&1; then
        echo "FAIL[notices.authority]: the topology audit does not pass over $real_topology, so its"
        echo "  declarations are not authority for anything: $(grep -m1 '^FAIL' "${TMPDIR:-/tmp}/moq5-notices-audit.$$")"
        rm -f "${TMPDIR:-/tmp}/moq5-notices-audit.$$"
        echo ""
        echo "artifact notices: 1 failure(s)"
        exit 1
    fi
    rm -f "${TMPDIR:-/tmp}/moq5-notices-audit.$$"
    canonical="${TMPDIR:-/tmp}/moq5-notices-canonical.$$"
    dep="$root/.native"
    {
        printf 'libmoq\t%s\n'       "$repo/LICENSE.txt"
        printf 'picoquic\t%s\n'     "$dep/_src/picoquic/LICENSE"
        printf 'picotls\t%s\n'      "$dep/_src/picotls/README.md"
        printf 'openssl\t%s\n'      "$dep/_src/openssl/LICENSE.txt"
        printf 'cifra\t%s\n'        "$dep/_src/picotls/deps/cifra/COPYING"
        printf 'micro-ecc\t%s\n'    "$dep/_src/picotls/deps/micro-ecc/LICENSE.txt"
        printf 'moq-msf-json\t%s\n' "$repo/media/msf/THIRD_PARTY.md"
    } > "$canonical" || { echo "$_self: cannot write the canonical mapping" >&2; exit 2; }
    trap 'rm -f "$canonical"' EXIT
fi

python3 - "$topology" "$aar" "$apk" "$canonical" <<'PY'
import hashlib, io, os, re, sys, zipfile

topology, aar, apk, canonical_path = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]

# THE REQUIRED CLOSURE, and the exact ABIs it must be declared for. Stated here
# so no input can redefine either by omission.
REQUIRED = ("libmoq", "picoquic", "picotls", "openssl", "cifra", "micro-ecc", "moq-msf-json")
REQUIRED_ABIS = ("arm64-v8a", "x86_64")

# component -> the ONE canonical source. Supplied by the caller, which in
# production mode is this script itself after the accepted audit passed.
CANONICAL = {}
with open(canonical_path) as _f:
    for _line in _f:
        _line = _line.rstrip("\n")
        if not _line:
            continue
        _parts = _line.split("\t")
        if len(_parts) == 2:
            CANONICAL[_parts[0]] = _parts[1]

# The shipped metadata's own resource path, and its schema: three tab fields,
# component / digest / archive-relative path.
NOTICE_DIR = "META-INF/org.openmoq/moq5/notices"
SHIPPED = NOTICE_DIR + "/licenses.manifest"

failures = []
def fail(name, why):
    failures.append(name)
    print("FAIL[%s]: %s" % (name, why))

def sha(b):
    return hashlib.sha256(b).hexdigest()

HEX = re.compile(r"\A[0-9a-f]{64}\Z")
COMPONENT = re.compile(r"\A[A-Za-z0-9][A-Za-z0-9._-]*\Z")

def read_manifest(abi):
    """The declarations of one ABI, validated by GRAMMAR before meaning."""
    path = os.path.join(topology, abi, "licenses.manifest")
    if not os.path.isfile(path):
        fail("notices.%s.manifest" % abi, "no licence manifest at %s" % path)
        return None
    rows = {}
    with open(path, "rb") as f:
        raw = f.read()
    if not raw.endswith(b"\n"):
        fail("notices.%s.grammar" % abi, "the manifest does not end with a newline")
    for n, line in enumerate(raw.decode("utf-8", "replace").split("\n"), 1):
        if line == "":
            continue
        parts = line.split("\t")
        # A malformed row FAILS BY NAME. It is never skipped: a row that
        # vanishes is exactly how a component would leave the closure.
        if len(parts) != 4 or any(p == "" for p in parts):
            fail("notices.%s.grammar" % abi,
                 "row %d is not four non-empty tab-separated fields: %r" % (n, line))
            continue
        component, digest, rel, source = parts
        if not COMPONENT.match(component):
            fail("notices.%s.grammar" % abi, "row %d names an implausible component %r" % (n, component))
            continue
        if not HEX.match(digest):
            fail("notices.%s.grammar" % abi, "row %d has no sha256 digest: %r" % (n, digest))
            continue
        if component in rows:
            fail("notices.%s.duplicate" % abi, "%s is declared more than once" % component)
            continue
        rows[component] = {"digest": digest, "rel": rel, "source": source}
    declared = set(rows)
    required = set(REQUIRED)
    for missing in sorted(required - declared):
        fail("notices.%s.missing" % abi, "%s is not declared" % missing)
    for extra in sorted(declared - required):
        fail("notices.%s.extra" % abi, "%s is declared but is not part of the required closure" % extra)
    return rows

def verify_sources(abi, rows):
    """Each declaration against the PINNED SOURCE's actual bytes and the collected copy."""
    licenses_dir = os.path.realpath(os.path.join(topology, abi, "licenses"))
    for component in REQUIRED:
        r = rows.get(component)
        if r is None:
            continue
        # IDENTITY, not merely bytes: the source must be THE canonical one.
        canon = CANONICAL.get(component)
        if canon is None:
            continue
        if os.path.realpath(r["source"]) != os.path.realpath(canon):
            fail("notices.%s.source" % abi,
                 "%s declares source %s; the pinned canonical source is %s"
                 % (component, r["source"], canon))
            continue
        if not os.path.isfile(canon):
            fail("notices.%s.source" % abi, "%s's canonical source is absent: %s" % (component, canon))
            continue
        with open(canon, "rb") as f:
            actual = sha(f.read())
        if actual != r["digest"]:
            fail("notices.%s.source" % abi,
                 "%s declares %s but its canonical source hashes to %s"
                 % (component, r["digest"][:16], actual[:16]))
        # The destination is exact, relative, inside the licence directory and
        # not a symlink. An absolute path here would otherwise silently escape.
        want_rel = "licenses/%s.LICENSE" % component
        if r["rel"] != want_rel:
            fail("notices.%s.collected" % abi,
                 "%s declares destination %s; the exact inventory requires %s" % (component, r["rel"], want_rel))
            continue
        collected = os.path.join(topology, abi, r["rel"])
        if os.path.islink(collected):
            fail("notices.%s.collected" % abi, "%s's destination is a symlink" % component)
            continue
        if not os.path.isfile(collected):
            fail("notices.%s.collected" % abi, "%s has no collected copy at %s" % (component, r["rel"]))
            continue
        if os.path.dirname(os.path.realpath(collected)) != licenses_dir:
            fail("notices.%s.collected" % abi,
                 "%s's destination resolves outside the package licence directory" % component)
            continue
        with open(collected, "rb") as f:
            got = sha(f.read())
        if got != r["digest"]:
            fail("notices.%s.collected" % abi,
                 "%s's collected copy hashes to %s, not the declared %s"
                 % (component, got[:16], r["digest"][:16]))

# THE ABI SET IS REQUIRED, NOT DISCOVERED. An ABI whose directory or manifest is
# absent must fail; discovering "the ABIs that happen to declare something" is
# how a missing ABI would pass.
missing_component_map = [c for c in REQUIRED if c not in CANONICAL]
if missing_component_map:
    fail("notices.canonical", "no canonical source is defined for %s" % ", ".join(missing_component_map))

manifests = {}
for abi in REQUIRED_ABIS:
    if not os.path.isdir(os.path.join(topology, abi)):
        fail("notices.%s.abi" % abi, "the required ABI directory is absent from %s" % topology)
        continue
    rows = read_manifest(abi)
    if rows is not None:
        manifests[abi] = rows
        verify_sources(abi, rows)

# EVERY ABI MUST AGREE. One ABI shipping a different OpenSSL notice than the
# other is a real divergence, and a single-ABI check would never see it.
if len(manifests) > 1:
    base_abi = sorted(manifests)[0]
    base = manifests[base_abi]
    for abi in sorted(manifests)[1:]:
        for component in REQUIRED:
            a, b = base.get(component), manifests[abi].get(component)
            if a and b and a["digest"] != b["digest"]:
                fail("notices.abi-disagreement",
                     "%s differs between %s (%s) and %s (%s)"
                     % (component, base_abi, a["digest"][:16], abi, b["digest"][:16]))

# The expectation the archives are judged against: one digest per required
# component, taken from the validated declarations.
expected = {}
if manifests:
    first = manifests[sorted(manifests)[0]]
    for component in REQUIRED:
        if component in first:
            expected[component] = first[component]

def containers(path):
    """
    Every ZIP container in the artifact, keyed by its container prefix.

    A shipped index's references are resolved WITHIN THE CONTAINER THAT HOLDS
    IT and nowhere else: for an AAR's classes.jar that is classes.jar, for an
    APK it is the APK. There is no basename repair and no global nested
    fallback -- either would let an index name one thing and be satisfied by
    another.
    """
    out = {}
    def walk(data, prefix):
        try:
            z = zipfile.ZipFile(io.BytesIO(data))
        except zipfile.BadZipFile:
            return
        entries = {}
        for info in z.infolist():
            if info.is_dir():
                continue
            blob = z.read(info)
            if info.filename in entries:
                fail("notices.duplicate-entry",
                     "%s%s appears more than once; identity would be ambiguous" % (prefix, info.filename))
            entries[info.filename] = blob
            if info.filename.lower().endswith((".jar", ".aar", ".zip", ".apk")):
                walk(blob, prefix + info.filename + "!/")
        out[prefix] = entries
    with open(path, "rb") as f:
        walk(f.read(), "")
    return out

def check_archive(kind, path):
    conts = containers(path)
    everywhere = {}
    for prefix, entries in conts.items():
        for name, blob in entries.items():
            everywhere.setdefault(sha(blob), []).append(prefix + name)

    for component in REQUIRED:
        e = expected.get(component)
        if e is None:
            continue          # already failed at the declaration boundary
        where = everywhere.get(e["digest"])
        if where:
            print("PASS[notices.%s.%s]: the declared bytes are at %s" % (kind, component, where[0]))
        else:
            fail("notices.%s.%s" % (kind, component),
                 "the declared notice is absent from the %s; expected the exact bytes of %s (sha256 %s)"
                 % (kind, e["source"], e["digest"][:16]))

    # An absolute ARCHIVE ENTRY NAME would put this machine's layout into the
    # artifact. A different property from the metadata content check below.
    for prefix, entries in conts.items():
        for name in entries:
            if name.startswith("/") or re.match(r"\A[A-Za-z]:[\\/]", name):
                fail("notices.%s.entry-path" % kind, "%s%s is packaged at an absolute path" % (prefix, name))

    # SHIPPED METADATA, checked as CONTENT, resolved in ITS OWN container.
    holders = [p for p, entries in conts.items() if SHIPPED in entries]
    if not holders:
        fail("notices.%s.shipped-manifest" % kind,
             "the artifact carries no %s, so a consumer has no index of what it received" % SHIPPED)
        return
    if len(holders) > 1:
        fail("notices.%s.shipped-manifest" % kind,
             "the artifact carries %d copies of %s: %s" % (len(holders), SHIPPED, ", ".join(sorted(holders))))
    for prefix in holders:
        entries = conts[prefix]
        text = entries[SHIPPED].decode("utf-8", "replace")
        where = prefix + SHIPPED
        seen = set()
        targets = set()
        for n, line in enumerate(text.split("\n"), 1):
            if line == "":
                continue
            parts = line.split("\t")
            if len(parts) != 3 or any(p == "" for p in parts):
                fail("notices.%s.shipped-schema" % kind,
                     "%s row %d is not three non-empty fields (component, digest, container-relative path): %r"
                     % (where, n, line))
                continue
            component, digest, rel = parts
            if os.path.isabs(rel) or re.match(r"\A[A-Za-z]:[\\/]", rel) or ".." in rel.split("/"):
                fail("notices.%s.shipped-path" % kind,
                     "%s declares a non-relative resource path for %s: %s" % (where, component, rel))
                continue
            if component not in REQUIRED:
                fail("notices.%s.shipped-schema" % kind, "%s declares unknown component %s" % (where, component))
                continue
            if component in seen:
                fail("notices.%s.shipped-schema" % kind, "%s declares %s twice" % (where, component))
                continue
            seen.add(component)
            want = "%s/%s.LICENSE" % (NOTICE_DIR, component)
            if rel != want:
                fail("notices.%s.shipped-path" % kind,
                     "%s declares %s for %s; the exact layout is %s" % (where, rel, component, want))
                continue
            if rel in targets:
                fail("notices.%s.shipped-path" % kind, "%s points two components at %s" % (where, rel))
                continue
            targets.add(rel)
            e = expected.get(component)
            if e and digest != e["digest"]:
                fail("notices.%s.shipped-digest" % kind,
                     "%s declares %s for %s; the validated closure says %s"
                     % (where, digest[:16], component, e["digest"][:16]))
            # RESOLVED IN THIS CONTAINER ONLY.
            if rel not in entries:
                fail("notices.%s.shipped-path" % kind,
                     "%s points at %s, which its own container (%s) does not contain"
                     % (where, rel, prefix or "the archive root"))
                continue
            got = sha(entries[rel])
            if got != digest:
                fail("notices.%s.shipped-digest" % kind,
                     "%s says %s hashes to %s; the packaged bytes hash to %s"
                     % (where, rel, digest[:16], got[:16]))
        for component in REQUIRED:
            if component not in seen:
                fail("notices.%s.shipped-schema" % kind, "%s does not index %s" % (where, component))

if aar:
    if not os.path.isfile(aar):
        fail("notices.aar", "no archive at %s" % aar)
    else:
        check_archive("aar", aar)
if apk:
    if not os.path.isfile(apk):
        fail("notices.apk", "no archive at %s" % apk)
    else:
        check_archive("apk", apk)

print("\nartifact notices: %d failure(s)" % len(failures))
sys.exit(1 if failures else 0)
PY
