#!/usr/bin/env bash
# Readiness must imply graceful signal handling.
#
# Both serve paths publish a "listening on" record that an operator waits on.
# If the SIGINT/SIGTERM handlers are installed after that record, a signal
# delivered in the gap kills the process by default disposition instead of
# taking the graceful stop that drains and emits terminal lane statistics.
#
# The behavioural proof is a race, and a race is a poor merge gate: the window
# is small, so a passing run proves little. The ORDERING is what makes the
# window impossible, so the ordering is what is pinned here — per serve
# function, on executable source only. Checking file-global order instead would
# accept a correctly ordered call in an unrelated function while a serve path
# went unguarded, and checking unstripped text would accept a commented-out
# call as if it ran.
#
# Argument 1 is cli/main.c.
set -u

src="${1:-}"
if [ ! -f "$src" ]; then
    echo "FAIL: usage: $0 <cli/main.c>" >&2
    exit 2
fi

failures=0
fail() { echo "FAIL: $1" >&2; failures=$((failures + 1)); }

# -- executable source only -------------------------------------------------
# Comments and string/char literals are blanked in place, so line numbers and
# structure survive but nothing inside them can satisfy a check.
strip_c() {
    awk '
    {
        line = $0; out = ""; i = 1; n = length(line)
        while (i <= n) {
            c = substr(line, i, 1); d = substr(line, i, 2)
            if (in_block) {
                if (d == "*/") { in_block = 0; out = out "  "; i += 2 }
                else { out = out " "; i++ }
                continue
            }
            if (d == "/*") { in_block = 1; out = out "  "; i += 2; continue }
            if (d == "//") { while (i <= n) { out = out " "; i++ } break }
            if (c == "\"" || c == "'"'"'") {
                q = c; out = out " "; i++
                while (i <= n) {
                    e = substr(line, i, 1)
                    if (e == "\\") { out = out "  "; i += 2; continue }
                    out = out " "; i++
                    if (e == q) break
                }
                continue
            }
            out = out c; i++
        }
        print out
    }' "$1"
}

code=$(strip_c "$src")

# The readiness record is published through the serve log (the "listening on"
# text lives in the sink's formatter). Its LINE is located in the raw file,
# then required to be executable (i.e. the stripped line still carries the
# call it belongs to).
ready_raw=$(grep -n 'moqr_cli_serve_log_readiness(&log' "$src" | cut -d: -f1)

# -- function extraction ----------------------------------------------------
# Body of a top-level function: from its definition line to the closing brace
# in column 0. Emits "lineno:text" so ordering can be compared exactly.
body_of() {
    printf '%s\n' "$code" | awk -v fn="$1" '
        $0 ~ "^" fn "\\(" { inb = 1 }
        inb { print NR ":" $0 }
        inb && /^}/ { exit }'
}

check_path() {
    local fn="$1" dump="$2"
    local body first_line last_line
    body=$(body_of "$fn")
    if [ -z "$body" ]; then
        fail "$fn: function body not found"
        return
    fi
    first_line=$(printf '%s\n' "$body" | head -1 | cut -d: -f1)
    last_line=$(printf '%s\n' "$body" | tail -1 | cut -d: -f1)

    # exactly one executable installer call, with this path's dump handler
    local installs install_line
    installs=$(printf '%s\n' "$body" |
               grep -cE "serve_install_signals\\([[:space:]]*$dump[[:space:]]*\\)")
    if [ "$installs" -ne 1 ]; then
        fail "$fn: expected exactly one executable serve_install_signals($dump) call, found $installs"
        return
    fi
    install_line=$(printf '%s\n' "$body" |
                   grep -E "serve_install_signals\\([[:space:]]*$dump[[:space:]]*\\)" |
                   head -1 | cut -d: -f1)

    # exactly one readiness record, inside this function
    local ready_here count
    ready_here=""
    count=0
    for l in $ready_raw; do
        if [ "$l" -ge "$first_line" ] && [ "$l" -le "$last_line" ]; then
            ready_here="$l"; count=$((count + 1))
        fi
    done
    if [ "$count" -ne 1 ]; then
        fail "$fn: expected exactly one readiness record, found $count"
        return
    fi
    # the readiness line must still be executable after stripping (the call
    # remains); a bare comment mentioning it would strip to blanks
    if ! printf '%s\n' "$code" | sed -n "${ready_here}p" | grep -q 'moqr_cli_serve_log_readiness('; then
        fail "$fn: the readiness record on line $ready_here is not executable"
        return
    fi
    if [ "$install_line" -ge "$ready_here" ]; then
        fail "$fn: publishes readiness (line $ready_here) before installing handlers (line $install_line)"
    fi
}

check_path cmd_serve       on_dump_signal
check_path cmd_serve_lanes on_dump_signal_lanes

# -- the shared installer ---------------------------------------------------
inst=$(printf '%s\n' "$code" | awk '
    /^serve_install_signals\(void/ { inb = 1 }
    inb { print }
    inb && /^}/ { exit }')
if [ -z "$inst" ]; then
    fail "serve_install_signals body not found"
else
    printf '%s\n' "$inst" | grep -q 'atomic_store(&g_stop, 0)' ||
        fail "serve_install_signals does not clear g_stop"
    for s in SIGINT SIGTERM SIGUSR1 SIGUSR2; do
        printf '%s\n' "$inst" | grep -qE "signal\\([[:space:]]*$s[[:space:]]*," ||
            fail "serve_install_signals does not install $s"
    done
fi

# -- nothing installs the stop signals anywhere else ------------------------
total_stop=$(printf '%s\n' "$code" |
             grep -cE 'signal\([[:space:]]*(SIGINT|SIGTERM)[[:space:]]*,')
inst_stop=$(printf '%s\n' "$inst" |
            grep -cE 'signal\([[:space:]]*(SIGINT|SIGTERM)[[:space:]]*,')
if [ "$total_stop" -ne "$inst_stop" ]; then
    fail "SIGINT/SIGTERM installed outside the shared installer ($total_stop total, $inst_stop in the installer)"
fi

if [ "$failures" -ne 0 ]; then
    echo "FAIL: $failures signal-order violation(s)" >&2
    exit 1
fi
echo "PASS: each serve path installs handlers before publishing readiness"
exit 0
