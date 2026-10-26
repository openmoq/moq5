#!/bin/sh
# The notice oracle's own controls.
#
# Every case below is a DISPOSABLE fixture built under an owned scratch tree:
# a topology declaration set and an artifact, shaped exactly as the producer
# and the packaging would shape them. Nothing here reads or writes the real
# .native tree, and nothing publishes.
#
# The cases are the ones that must not pass, including the five that an earlier
# revision of the oracle accepted (chats/libmoq/1599 F1) and the shipped-metadata
# leak it never looked at (F2). Each is asserted by its OWN named failure, so a
# fixture that fails for an unrelated reason does not count as a kill.
set -u
LC_ALL=C; export LC_ALL

_self=${0##*/}
case $0 in
    */*) _selfdir=${0%/*} ;;
    *)   _selfdir=. ;;
esac
here=$(cd "$_selfdir" && pwd) || { echo "$_self: cannot resolve the script directory" >&2; exit 2; }
checker="$here/check-artifact-notices.sh"
[ -x "$checker" ] || { echo "$_self: no checker at $checker" >&2; exit 2; }
command -v python3 >/dev/null 2>&1 || { echo "$_self: no python3" >&2; exit 2; }

scratch=$(mktemp -d "${TMPDIR:-/tmp}/moq5-notices-selftest-XXXXXXXX") \
    || { echo "$_self: cannot create a scratch" >&2; exit 2; }
# shellcheck disable=SC2329  # invoked from the traps below
cleanup() { rm -rf -- "$scratch"; }
trap 'cleanup; exit 130' HUP INT TERM
trap cleanup EXIT

python3 - "$scratch" "$checker" <<'PY'
import hashlib, io, os, subprocess, sys, zipfile

scratch, checker = sys.argv[1], sys.argv[2]
REQUIRED = ("libmoq", "picoquic", "picotls", "openssl", "cifra", "micro-ecc", "moq-msf-json")
SHIPPED = "META-INF/org.openmoq/moq5/notices/licenses.manifest"
NOTICE_DIR = "META-INF/org.openmoq/moq5/notices"

failures = 0
def report(ok, name, detail):
    global failures
    if ok:
        print("PASS[%s]: %s" % (name, detail))
    else:
        failures += 1
        print("FAIL[%s]: %s" % (name, detail))

def sha(b):
    return hashlib.sha256(b).hexdigest()

# -- the pinned sources every fixture is built from ---------------------------
src_root = os.path.join(scratch, "src")
os.makedirs(src_root)
sources = {}
for c in REQUIRED:
    p = os.path.join(src_root, c + ".txt")
    with open(p, "wb") as f:
        f.write(("the %s notice, and a URL https://example.invalid/%s that is legitimate prose\n" % (c, c)).encode())
    sources[c] = p

def build_topology(name, rows, abis=("arm64-v8a", "x86_64"), bodies=None):
    """A topology tree: `rows` maps component -> (digest, rel, source) per ABI."""
    root = os.path.join(scratch, "topology-" + name)
    for abi in abis:
        d = os.path.join(root, abi, "licenses")
        os.makedirs(d, exist_ok=True)
        lines = []
        for c, (digest, rel, source) in rows(abi).items():
            body = (bodies or {}).get((abi, c))
            if body is None:
                key = c if c in sources else source
                path = sources.get(c, source)
                with open(path, "rb") as f:
                    body = f.read()
            with open(os.path.join(root, abi, rel), "wb") as f:
                f.write(body)
            lines.append("\t".join([c, digest, rel, source]))
        with open(os.path.join(root, abi, "licenses.manifest"), "w") as f:
            f.write("\n".join(lines) + "\n")
    return root

def canonical(abi):
    out = {}
    for c in REQUIRED:
        with open(sources[c], "rb") as f:
            out[c] = (sha(f.read()), "licenses/%s.LICENSE" % c, sources[c])
    return out

def build_aar(name, notices, shipped_rows=None, extra=None, stray=None):
    """
    An AAR-shaped archive whose notices live where the proposal puts them:
    inside classes.jar, which is the container the index is resolved in.
    """
    p = os.path.join(scratch, name + ".aar")
    inner = io.BytesIO()
    with zipfile.ZipFile(inner, "w") as j:
        for rel, body in notices.items():
            j.writestr(NOTICE_DIR + "/" + rel, body)
        if shipped_rows is not None:
            j.writestr(SHIPPED, "\n".join("\t".join(r) for r in shipped_rows) + "\n")
        for k, v in (extra or {}).items():
            j.writestr(k, v)
    other = io.BytesIO()
    with zipfile.ZipFile(other, "w") as o:
        for k, v in (stray or {}).items():
            o.writestr(k, v)
    with zipfile.ZipFile(p, "w") as z:
        z.writestr("AndroidManifest.xml", "<manifest/>")
        z.writestr("classes.jar", inner.getvalue())
        if stray:
            z.writestr("libs/other.jar", other.getvalue())
    return p

def valid_notices():
    out = {}
    for c in REQUIRED:
        with open(sources[c], "rb") as f:
            out["%s.LICENSE" % c] = f.read()
    return out

def valid_shipped():
    rows = []
    for c in REQUIRED:
        with open(sources[c], "rb") as f:
            rows.append([c, sha(f.read()), "%s/%s.LICENSE" % (NOTICE_DIR, c)])
    return rows

def canonical_file(name, mapping=None):
    """The component -> canonical source mapping this fixture declares."""
    p = os.path.join(scratch, "canonical-" + name + ".tsv")
    with open(p, "w") as f:
        for c in REQUIRED:
            f.write("%s\t%s\n" % (c, (mapping or sources)[c]))
    return p

def run(topology, aar=None, canonical=None):
    cmd = ["/bin/sh", checker, "--fixture-mode", "--topology", topology,
           "--canonical", canonical or canonical_file("default")]
    if aar:
        cmd += ["--aar", aar]
    r = subprocess.run(cmd, capture_output=True, text=True)
    return r.returncode, r.stdout

def expect_pass(case, topology, aar=None, canonical=None):
    rc, out = run(topology, aar, canonical)
    report(rc == 0, "selftest.%s" % case, "the valid fixture passes" if rc == 0
           else "a valid fixture was refused:\n" + out)

def expect_named(case, needle, topology, aar=None, canonical=None):
    rc, out = run(topology, aar, canonical)
    named = [l for l in out.split("\n") if l.startswith("FAIL[") and needle in l]
    report(rc != 0 and named, "selftest.%s" % case,
           ("refused by %s" % named[0].split("]")[0][5:]) if named
           else "expected a failure naming %r; got:\n%s" % (needle, out))

# -- 1. the positive control --------------------------------------------------
good = build_topology("good", canonical)
expect_pass("valid", good, build_aar("valid", valid_notices(), valid_shipped()))

# -- 2. the five declarations that must not redefine the closure --------------
def drop_cifra(abi):
    rows = canonical(abi); rows.pop("cifra"); return rows
expect_named("declaration-missing-component", "notices.arm64-v8a.missing",
             build_topology("dropped", drop_cifra),
             build_aar("dropped", {k: v for k, v in valid_notices().items() if not k.startswith("cifra")},
                       [r for r in valid_shipped() if r[0] != "cifra"]))

def malformed(abi):
    return canonical(abi)
mal = build_topology("malformed", malformed)
for abi in ("arm64-v8a", "x86_64"):
    p = os.path.join(mal, abi, "licenses.manifest")
    text = [l for l in open(p).read().split("\n") if l]
    text = ["\t".join(l.split("\t")[:2]) if l.startswith("cifra\t") else l for l in text]
    open(p, "w").write("\n".join(text) + "\n")
expect_named("declaration-malformed-row", "notices.arm64-v8a.grammar", mal,
             build_aar("malformed", valid_notices(), valid_shipped()))

def renamed(abi):
    rows = canonical(abi)
    d, _, s = rows.pop("cifra")
    rows["not-cifra"] = (d, "licenses/not-cifra.LICENSE", s)
    return rows
expect_named("declaration-wrong-member", "notices.arm64-v8a.extra",
             build_topology("renamed", renamed),
             build_aar("renamed", valid_notices(), valid_shipped()))

def forged(abi):
    rows = canonical(abi)
    body = b"not a license"
    rows["cifra"] = (sha(body), "licenses/cifra.LICENSE", os.path.join(scratch, "absent-source.txt"))
    return rows
forged_bodies = {(abi, "cifra"): b"not a license" for abi in ("arm64-v8a", "x86_64")}
expect_named("declaration-forged-source", "notices.arm64-v8a.source",
             build_topology("forged", forged, bodies=forged_bodies),
             build_aar("forged", valid_notices(), valid_shipped()))

def duplicated(abi):
    return canonical(abi)
dup = build_topology("dup", duplicated)
for abi in ("arm64-v8a", "x86_64"):
    p = os.path.join(dup, abi, "licenses.manifest")
    text = open(p).read()
    open(p, "w").write(text + [l for l in text.split("\n") if l.startswith("cifra\t")][0] + "\n")
expect_named("declaration-duplicate", "notices.arm64-v8a.duplicate", dup,
             build_aar("dup", valid_notices(), valid_shipped()))

# -- 3. the ABIs must agree ---------------------------------------------------
# Each manifest is INTERNALLY CONSISTENT -- x86_64 declares a different pinned
# source and the digest of that source's real bytes -- so only the cross-ABI
# comparison can see the divergence.
other_openssl = os.path.join(src_root, "openssl-other.txt")
with open(other_openssl, "wb") as f:
    f.write(b"a different openssl notice\n")
with open(other_openssl, "rb") as f:
    other_digest = sha(f.read())
skew_bodies = {("x86_64", "openssl"): b"a different openssl notice\n"}
def skewed(abi):
    rows = canonical(abi)
    if abi == "x86_64":
        rows["openssl"] = (other_digest, "licenses/openssl.LICENSE", other_openssl)
    return rows
expect_named("declaration-abi-disagreement", "notices.abi-disagreement",
             build_topology("skew", skewed, bodies=skew_bodies),
             build_aar("skew", valid_notices(), valid_shipped()))

# -- 4. the artifact ----------------------------------------------------------
expect_named("artifact-missing-notice", "notices.aar.cifra", good,
             build_aar("aar-missing", {k: v for k, v in valid_notices().items() if not k.startswith("cifra")},
                       valid_shipped()))

wrong = valid_notices()
with open(sources["libmoq"], "rb") as f:
    wrong["cifra.LICENSE"] = f.read()          # same name, same count, wrong bytes
expect_named("artifact-same-count-wrong-member", "notices.aar.cifra", good,
             build_aar("aar-wrong", wrong, valid_shipped()))

changed = valid_notices()
changed["openssl.LICENSE"] = changed["openssl.LICENSE"] + b"\n"
expect_named("artifact-changed-bytes", "notices.aar.openssl", good,
             build_aar("aar-changed", changed, valid_shipped()))

# -- 5. shipped metadata ------------------------------------------------------
expect_named("shipped-manifest-absent", "notices.aar.shipped-manifest", good,
             build_aar("aar-noindex", valid_notices()))

leaked = [[c, d, sources[c]] for c, d, _ in valid_shipped()]
expect_named("shipped-host-path", "notices.aar.shipped-path", good,
             build_aar("aar-leaked", valid_notices(), leaked))

four_column = [[c, d, rel, sources[c]] for c, d, rel in valid_shipped()]
expect_named("shipped-producer-manifest-verbatim", "notices.aar.shipped-schema", good,
             build_aar("aar-fourcol", valid_notices(), four_column))

wrong_digest = [[c, (sha(b"other") if c == "openssl" else d), rel] for c, d, rel in valid_shipped()]
expect_named("shipped-wrong-digest", "notices.aar.shipped-digest", good,
             build_aar("aar-wrongdigest", valid_notices(), wrong_digest))

wrong_path = [[c, d, ("%s/absent.LICENSE" % NOTICE_DIR if c == "cifra" else rel)] for c, d, rel in valid_shipped()]
expect_named("shipped-wrong-path", "notices.aar.shipped-path", good,
             build_aar("aar-wrongpath", valid_notices(), wrong_path))

# The index must not be satisfied by a path outside the declared layout, even
# when the notice really is somewhere in the artifact.
elsewhere = [[c, d, ("absent-directory/cifra.LICENSE" if c == "cifra" else rel)] for c, d, rel in valid_shipped()]
expect_named("shipped-path-outside-layout", "notices.aar.shipped-path", good,
             build_aar("aar-elsewhere", valid_notices(), elsewhere))

# ... nor by a member of a DIFFERENT container.
split_notices = {k: v for k, v in valid_notices().items() if not k.startswith("cifra")}
expect_named("shipped-other-container", "notices.aar.shipped-path", good,
             build_aar("aar-split", split_notices, valid_shipped(),
                       stray={NOTICE_DIR + "/cifra.LICENSE": valid_notices()["cifra.LICENSE"]}))

# -- 6. authority: canonical sources and the required ABI set -----------------
# A readable but NONCANONICAL source, declared consistently across both ABIs
# with matching collected bytes and matching artifact bytes.
noncanon = dict(sources)
noncanon_path = os.path.join(src_root, "cifra-elsewhere.txt")
with open(sources["cifra"], "rb") as f:
    with open(noncanon_path, "wb") as g:
        g.write(f.read())
noncanon["cifra"] = noncanon_path
def with_noncanonical(abi):
    rows = canonical(abi)
    d, rel, _ = rows["cifra"]
    rows["cifra"] = (d, rel, noncanon_path)
    return rows
expect_named("authority-noncanonical-source", "notices.arm64-v8a.source",
             build_topology("noncanon", with_noncanonical),
             build_aar("aar-noncanon", valid_notices(), valid_shipped()))

# Arbitrary bytes at a readable path, consistent everywhere.
arb = os.path.join(src_root, "cifra-arbitrary.txt")
with open(arb, "wb") as f:
    f.write(b"arbitrary bytes that are not a licence\n")
with open(arb, "rb") as f:
    arb_digest = sha(f.read())
def with_arbitrary(abi):
    rows = canonical(abi)
    rows["cifra"] = (arb_digest, "licenses/cifra.LICENSE", arb)
    return rows
arb_bodies = {(abi, "cifra"): open(arb, "rb").read() for abi in ("arm64-v8a", "x86_64")}
arb_notices = valid_notices()
arb_notices["cifra.LICENSE"] = open(arb, "rb").read()
arb_shipped = [[c, (arb_digest if c == "cifra" else d), rel] for c, d, rel in valid_shipped()]
expect_named("authority-arbitrary-source", "notices.arm64-v8a.source",
             build_topology("arbitrary", with_arbitrary, bodies=arb_bodies),
             build_aar("aar-arbitrary", arb_notices, arb_shipped))

# A required ABI removed entirely.
one_abi = build_topology("oneabi", canonical, abis=("arm64-v8a",))
expect_named("authority-missing-abi", "notices.x86_64.abi", one_abi,
             build_aar("aar-oneabi", valid_notices(), valid_shipped()))

# The ABI directory present, its manifest removed.
no_manifest = build_topology("nomanifest", canonical)
os.remove(os.path.join(no_manifest, "x86_64", "licenses.manifest"))
expect_named("authority-missing-manifest", "notices.x86_64.manifest", no_manifest,
             build_aar("aar-nomanifest", valid_notices(), valid_shipped()))

# An ABSOLUTE collected path, which an unguarded join would silently accept.
def absolute_dest(abi):
    rows = canonical(abi)
    d, _, s = rows["cifra"]
    rows["cifra"] = (d, sources["cifra"], s)
    return rows
absdest = os.path.join(scratch, "topology-absdest")
for abi in ("arm64-v8a", "x86_64"):
    os.makedirs(os.path.join(absdest, abi, "licenses"), exist_ok=True)
    lines = []
    for c in REQUIRED:
        with open(sources[c], "rb") as f:
            body = f.read()
        rel = sources[c] if c == "cifra" else "licenses/%s.LICENSE" % c
        if c != "cifra":
            with open(os.path.join(absdest, abi, rel), "wb") as f:
                f.write(body)
        lines.append("\t".join([c, sha(body), rel, sources[c]]))
    with open(os.path.join(absdest, abi, "licenses.manifest"), "w") as f:
        f.write("\n".join(lines) + "\n")
expect_named("authority-absolute-destination", "notices.arm64-v8a.collected", absdest,
             build_aar("aar-absdest", valid_notices(), valid_shipped()))

# Legitimate prose containing a URL must NOT be mistaken for a host-path leak:
# the valid control above already carries one, and it passes.
print("\nartifact-notice self-test: %d failure(s)" % failures)
sys.exit(1 if failures else 0)
PY
