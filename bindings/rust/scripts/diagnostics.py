"""Classify every diagnostic of one cargo invocation and fail on any this
build owns or the allowlist does not name exactly.

    python3 -I diagnostics.py --json <stdout of cargo --message-format=json>
                              --stderr <its stderr> --allow <allowlist>
                              --inventory <out> [--label <name>]

The stdout carries cargo's structured records: every `compiler-message` is a
rustc diagnostic with its package, level, lint code, message and spans. The
stderr carries cargo's own diagnostics as text: manifest warnings,
build-script `cargo:warning` lines (`warning: <crate>@<version>: ...`), its
roll-up summaries, and anything else a tool printed. Under -vv cargo also
echoes every line a build script prints, its stdout to cargo's stdout and
its stderr to cargo's stderr, as `[<package> <version>] <line>`: there a
diagnostic is clang's own (`warning: ...`, or `<file>:<line>:<col>: warning:
...`, which libclang prints because bindgen opens its index with diagnostics
shown), bindgen's restatement of it (`clang diag: ...`), or any tool's
`warning:`/`error:` line. Both streams are read for them.

A diagnostic's identity is its package (crate and version), where it is, and
what it is. Where: the primary span's file, relative to the package's own
directory, and line (`src/link.rs:128`); a span inside a macro defined
outside the package is followed to its call site in the package; a located
build-output diagnostic's file and line; `-` when the diagnostic has no
location. What: its lint code, `message:<text>` when it has none,
`build-script:<text>` for a build script's warning, or
`build-output:<level>: <text>` for a diagnostic a build script printed.

Rules:
- an error anywhere fails, whatever the allowlist says;
- a rustc diagnostic of a path package (this workspace, the consumer, an
  extracted package) is this build's and fails;
- a diagnostic of a registry package fails unless an allowlist line names
  exactly its crate, version, location and lint (`crate version location
  lint`); there is no crate-wide, lint-wide or location-free entry, and a
  line with fewer fields is refused;
- a cargo-level line that is not one of cargo's known roll-up forms is a
  diagnostic too: a manifest warning or anything unlocated is this build's
  and fails; a registry crate's build-script warning must be allowed the same
  way, with location `-`;
- a diagnostic a build script printed belongs to the package that printed
  it: a path package's fails, a registry package's must be allowed the same
  way; the echoed `cargo:` directives are not diagnostics (cargo renders
  `cargo:warning` itself, above);
- the inventory lists every diagnostic found, allowed or not, with its
  location and count, and the allowlist lines this build did not see; the
  summary counts the accepted upstream warnings, so a passing build that has
  them is never reported as free of warnings.

Exit 0 only when every diagnostic is an allowed registry warning.
"""

import argparse
import json
import re
import sys
from collections import Counter

# Cargo's own roll-up and status lines, which are not diagnostics.
ROLLUP = re.compile(
    r"^warning: `[^`]+` \((?:lib|bin|test|example|bench|build script|doc)(?: \"[^\"]+\")?(?: test| doctest)?\) generated \d+ warnings?"
    r"|^warning: build failed, waiting for other jobs to finish\.\.\.$"
    r"|^error: could not compile `"
    r"|^error: (build|test) failed"
)
# A registry crate's build-script warning rendered by cargo under -vv.
BUILD_SCRIPT = re.compile(r"^warning: ([A-Za-z0-9_-]+)@([0-9][^:]*): (.*)$")
DIAGNOSTIC = re.compile(r"^(warning|error)(\[[A-Z0-9]+\])?: ")
REGISTRY = re.compile(r"^registry\+[^#]+#([A-Za-z0-9_-]+)@([0-9][^@]*)$")
PATH_PACKAGE = re.compile(r"^path\+file://")
# A line a build script printed, as cargo -vv echoes it.
ECHO = re.compile(r"^\[([A-Za-z0-9_-]+) ([0-9][^\]]*)\] (.*)$")
# A diagnostic in that output: clang's own, bindgen's `clang diag:`
# restatement of it, or any tool's; notes belong to a diagnostic.
ECHO_DIAGNOSTIC = re.compile(
    r"^(?:clang diag: )?(?:(?P<file>[^\s:][^:]*):(?P<line>\d+):(?P<col>\d+): )?"
    r"(?P<level>warning|error|fatal error)(?:\[[^\]]+\])?: (?P<text>.*)$"
)


def package_identity(package_id):
    """(`registry`, crate, version) or (`local`, name, version)."""
    registry = REGISTRY.match(package_id)
    if registry:
        return "registry", registry.group(1), registry.group(2)
    if PATH_PACKAGE.match(package_id):
        location, _, fragment = package_id.rpartition("#")
        if "@" in fragment:
            name, version = fragment.split("@", 1)
        else:
            # `path+file:///dir/name#version`: the directory is the name.
            name, version = location.rstrip("/").rsplit("/", 1)[-1], fragment
        return "local", name, version
    return "unknown", package_id, "?"


def location(message, origin, crate, version):
    """The primary span as `<file relative to the package>:<line>`, or `-`."""
    primary = [s for s in message.get("spans", []) if s.get("is_primary")]
    if not primary:
        return "-"
    if origin != "registry":
        span = primary[0]
        return f"{span.get('file_name')}:{span.get('line_start')}"
    marker = f"{crate}-{version}/"
    span = primary[0]
    while span is not None:
        file_name = span.get("file_name", "")
        at = file_name.find(marker)
        if at >= 0:
            return f"{file_name[at + len(marker):]}:{span.get('line_start')}"
        expansion = span.get("expansion")
        span = expansion.get("span") if expansion else None
    # Outside the package with no call site in it: kept as rustc names it,
    # which no allowlist line written for another machine matches.
    first = primary[0]
    return f"{first.get('file_name')}:{first.get('line_start')}"


def echoed_diagnostic(line, registry_packages):
    """The record of a diagnostic a build script printed, or None."""
    echo = ECHO.match(line)
    if not echo:
        return None
    package, version, payload = echo.groups()
    if payload.startswith("cargo:"):
        return None   # a directive; cargo renders `cargo:warning` itself
    found = ECHO_DIAGNOSTIC.match(payload)
    if not found:
        return None
    origin = "registry" if (package, version) in registry_packages else "local"
    where = "-"
    if found.group("file"):
        file_name = found.group("file")
        marker = f"{package}-{version}/"
        at = file_name.find(marker)
        relative = file_name[at + len(marker):] if at >= 0 else file_name
        where = f"{relative}:{found.group('line')}"
    level = "warning" if found.group("level") == "warning" else "error"
    identity = f"build-output:{found.group('level')}: {found.group('text')}"
    return (origin, package, version, where, identity, level)


def read_allowlist(path):
    allowed = set()
    with open(path, encoding="utf-8") as handle:
        for number, line in enumerate(handle, 1):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split(None, 3)
            if len(parts) != 4:
                raise ValueError(
                    f"{path}:{number}: an entry is `crate version location lint`; "
                    f"there is no crate-wide or location-free entry: {line!r}"
                )
            allowed.add(tuple(parts))
    return allowed


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", required=True)
    parser.add_argument("--stderr", required=True)
    parser.add_argument("--allow", required=True)
    parser.add_argument("--inventory", required=True)
    parser.add_argument("--label", default="")
    args = parser.parse_args()

    try:
        allowed = read_allowlist(args.allow)
    except ValueError as refused:
        print(f"diagnostics.py: {refused}", file=sys.stderr)
        return 2

    # One record per diagnostic: (origin, crate, version, location, identity, level).
    records = []
    echoed = []          # build-script output lines, both streams
    registry_packages = set()
    with open(args.json, encoding="utf-8", errors="replace") as handle:
        for line in handle:
            line = line.rstrip("\n")
            if not line.startswith("{"):
                echoed.append(line)   # a -vv echo of a build script's stdout
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                records.append(("local", "cargo", "?", "-", f"message:unparseable record {line[:80]!r}", "error"))
                continue
            origin, crate, version = package_identity(record.get("package_id", ""))
            if origin == "registry":
                registry_packages.add((crate, version))
            if record.get("reason") != "compiler-message":
                continue
            message = record.get("message", {})
            level = message.get("level", "")
            if level not in ("warning", "error"):
                continue   # notes and helps belong to a diagnostic already counted
            origin, crate, version = package_identity(record.get("package_id", ""))
            code = (message.get("code") or {}).get("code")
            identity = code if code else "message:" + message.get("message", "")
            where = location(message, origin, crate, version)
            records.append((origin, crate, version, where, identity, level))

    with open(args.stderr, encoding="utf-8", errors="replace") as handle:
        for line in handle:
            line = line.rstrip("\n")
            if ECHO.match(line):
                echoed.append(line)   # a -vv echo of a build script's stderr
                continue
            if not DIAGNOSTIC.match(line) or ROLLUP.match(line):
                continue
            level = "error" if line.startswith("error") else "warning"
            build_script = BUILD_SCRIPT.match(line)
            if build_script:
                crate, version, text = build_script.groups()
                # A path package's build script is this build's own.
                origin = "registry" if (crate, version) in registry_packages else "local"
                records.append((origin, crate, version, "-", "build-script:" + text, level))
            else:
                records.append(("local", "cargo", "?", "-", "message:" + line, level))

    for line in echoed:
        record = echoed_diagnostic(line, registry_packages)
        if record:
            records.append(record)

    counts = Counter(records)
    failures = []
    accepted = 0
    seen_entries = set()
    with open(args.inventory, "w", encoding="utf-8") as out:
        out.write(
            f"# diagnostics of {args.label or args.json} (cargo -vv: dependency lints capped at warn)\n"
            "# count  verdict  origin  crate  version  location  lint\n"
        )
        if not counts:
            out.write("none\n")
        for (origin, crate, version, where, identity, level), count in sorted(counts.items()):
            entry = (crate, version, where, identity)
            if level == "error":
                verdict = "error"
            elif origin == "registry" and entry in allowed:
                verdict = "allowed"
                accepted += count
                seen_entries.add(entry)
            elif origin == "registry":
                verdict = "not-allowed"
            else:
                verdict = "own"
            out.write(f"{count}\t{verdict}\t{origin}\t{crate}\t{version}\t{where}\t{identity}\n")
            if verdict != "allowed":
                failures.append(f"{verdict}: {crate} {version} {where} {identity} (x{count})")
        for entry in sorted(allowed - seen_entries):
            out.write("# allowlisted, not seen in this build: " + " ".join(entry) + "\n")

    crates = sorted({r[1] for r in counts if r[0] == "registry"})
    summary = (
        f"{accepted} accepted upstream warning(s) from {' '.join(crates)}"
        if accepted else "no accepted upstream warning"
    )
    print(f"diagnostics.py: {len(failures)} failing, {summary} -> {args.inventory}")
    if failures:
        for failure in failures:
            print("  " + failure, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
