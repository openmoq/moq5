#!/usr/bin/env bash
#
# validate_git_ref.sh - validate an UNTRUSTED git ref (a workflow_dispatch
# input) before it is used as a `git clone --branch` argument.
#
# Security report finding #11 (MOQ-WORKFLOW-DISPATCH-INJECTION): a manual ref
# input is interpolated by GitHub expression substitution BEFORE Bash parses the
# script, so a ref containing quotes, newlines, command substitutions, or shell
# metacharacters can become shell source in a package-writing job. The fix keeps
# the ref as DATA (a step-level `env:` value expanded only as "$REF") and rejects
# anything that is not a plain branch/tag/SHA-shaped token here, before GHCR
# login and before any package write.
#
# Usage: validate_git_ref.sh <ref> [name]
#   exits 0 if the ref is acceptable; non-zero with a message on stderr otherwise.
#
# Validation is TWO gates, in order:
#
#   1. Shell-safe whitelist (stricter than a blacklist by design): letters,
#      digits, '.', '_', '/', '-', with a first character other than '-'. This
#      REJECTS empty refs, refs starting with '-', quotes, whitespace/newlines,
#      '@', backslashes, and shell metacharacters ; & | $ ` ( ) < > by
#      construction. The Bash `[[ =~ ]]` test matches the WHOLE string (a
#      mid-string newline fails the class), so a multi-line ref cannot smuggle a
#      valid-looking first line past the check. This runs FIRST so the git check
#      below never sees a leading option or shell-shaped junk.
#
#   2. Git's own branch/tag shorthand validity: `git check-ref-format --branch`.
#      The whitelist alone still accepts tokens Git rejects as refnames
#      (`/bad`, `bad/`, `bad..ref`, `.bad`, `bad.lock`, `foo//bar`), so a
#      "validated ref" the publish job then clones must also be a real refname.
#
# Practical refs like `master`, `v1.2.3`, `feature/foo`, and full commit SHAs
# pass both gates.
set -euo pipefail

ref="${1-}"
name="${2-ref}"

if [ -z "$ref" ]; then
    printf 'invalid %s: empty\n' "$name" >&2
    exit 1
fi

# Gate 1: shell-safe whitelist (must pass before git ever sees the value).
if [[ ! "$ref" =~ ^[A-Za-z0-9._/][A-Za-z0-9._/-]*$ ]]; then
    printf 'invalid %s: only [A-Za-z0-9._/-] allowed and must not start with "-": %q\n' \
        "$name" "$ref" >&2
    exit 1
fi

# Gate 2: valid git branch/tag shorthand. Quoted as data; the whitelist above
# guarantees it is not a leading option or shell metacharacter.
if ! git check-ref-format --branch "$ref" >/dev/null 2>&1; then
    printf 'invalid %s: not a valid git ref name: %q\n' "$name" "$ref" >&2
    exit 1
fi

exit 0
