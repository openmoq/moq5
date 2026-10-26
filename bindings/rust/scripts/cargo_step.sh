# Sourced by package_check.sh and gate_controls.sh: one cargo step whose
# exit status and diagnostics both gate.
#
#   moq5_cargo_step <log-dir> <name> <stdout-file> <command...>
#
# The command's stdout goes to <stdout-file> and its stderr to
# <log-dir>/<name>.log. A nonzero exit fails the step with the log's tail.
# Then the stderr is classified by scripts/diagnostics.py (the step has no
# structured records: no --message-format) into
# <log-dir>/<name>.diagnostics.txt, and any diagnostic fails the step: cargo's
# own warnings (a package without metadata, a manifest key it ignores)
# exit 0 and are visible only there.

MOQ5_CARGO_STEP_SCRIPTS=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

moq5_cargo_step() {
    local dir=$1 name=$2 out=$3
    shift 3
    if ! "$@" > "$out" 2> "$dir/$name.log"; then
        echo "cargo step $name failed (see $dir/$name.log):" >&2
        tail -30 "$dir/$name.log" >&2
        return 1
    fi
    : > "$dir/$name.records.json"
    bash "$MOQ5_CARGO_STEP_SCRIPTS/diagnostics.sh" "$dir/$name.records.json" "$dir/$name.log" \
        "$dir/$name.diagnostics.txt" "$MOQ5_CARGO_STEP_SCRIPTS/dependency_diagnostics.allow" "$name"
}
