#!/usr/bin/env bash
# The serve log's wiring contracts, read from cli/main.c:
#
#   1. every readiness, operating-point, attribution and stop record of BOTH
#      serve paths goes through the serve log helpers -- no printf of a
#      RELAY_ row or an operator banner survives in main.c;
#   2. each serve path initialises its sink once, before its ceiling gate,
#      and its ceiling prose goes to the sink's prose stream; only the
#      capacity subcommand prints its ceiling to stdout;
#   3. every ordinary return after the sink exists -- a refusal before
#      readiness as much as the post-join return -- goes through the one
#      finalizing helper, which does nothing else; returns before the sink
#      exists never finalize; the nonreturning halt never finalizes;
#   4. the sink is a serve-function local: it lives in no context the lanes
#      or the emergency halt can reach;
#   5. the verify and measure builds refuse a JSON serve after the config is
#      loaded and before the capacity preflight -- before any resource;
#   6. the coordinator seam test that proves the lifecycle (argument 2) stays
#      sans-I/O: its failure injection is the fake facade API, never a
#      socket, a listener or a sleep.
#
# Argument 1 is cli/main.c; argument 2 is tests/test_relay_coord_seam.c.
set -u
src="$1"
seam="${2:-}"
[ -r "$src" ] || { echo "FAIL: cannot read $src" >&2; exit 1; }
[ -n "$seam" ] && [ -r "$seam" ] || { echo "FAIL: cannot read the coordinator seam test ($seam)" >&2; exit 1; }
fail=0
fail() { echo "FAIL: $1"; fail=1; }

# Strip comments and string literals so anchors are executable code only.
strip_c() {
    awk '
    {
        line = $0; out = ""; i = 1; n = length(line)
        while (i <= n) {
            c = substr(line, i, 1); d = substr(line, i, 2)
            if (inc) { if (d == "*/") { inc = 0; i += 2 } else { i++ }; continue }
            if (d == "/*") { inc = 1; i += 2; continue }
            if (d == "//") { break }
            if (c == "\"") {
                out = out "\"\""; i++
                while (i <= n) {
                    c = substr(line, i, 1)
                    if (c == "\\") { i += 2; continue }
                    if (c == "\"") { i++; break }
                    i++
                }
                continue
            }
            out = out c; i++
        }
        print out
    }' "$1"
}
code=$(strip_c "$src")

body_of() {
    printf '%s\n' "$code" | awk -v fn="$1" '
        $0 ~ "^" fn "\\(" { inb = 1 }
        inb { print NR ":" $0 }
        inb && /^}/ { exit }'
}
count_in() { printf '%s\n' "$1" | grep -c -- "$2"; }
first_at() { printf '%s\n' "$1" | grep -- "$2" | head -1 | cut -d: -f1; }
last_at()  { printf '%s\n' "$1" | grep -- "$2" | tail -1 | cut -d: -f1; }

# -- 1. no record is printed around the sink --------------------------------
if grep -qE 'printf\((MOQR_(LANE_STATS|PAIR_STATS|RUN_CONFIG)_PREFIX|"MOQ5 Relay: (listening|admin endpoint|WebTransport|stopping|shard)|"signals: SIGUSR1)' "$src"; then
    fail "a readiness, attribution or stop record is still printed directly in main.c"
fi
if grep -q 'fflush(stdout)' <(printf '%s\n' "$code" | awk '/^cmd_serve(_lanes)?\(/{p=1} p{print} p&&/^}/{p=0}'); then
    fail "a serve path flushes stdout around the sink"
fi

for fn in cmd_serve cmd_serve_lanes; do
    body=$(body_of "$fn")
    [ -n "$body" ] || { fail "$fn not found"; continue; }
    # 2. one sink, initialised before the ceiling gate, prose on its stream
    [ "$(count_in "$body" 'moqr_cli_log_init(&log')" = "1" ] ||
        fail "$fn: expected exactly one sink initialisation"
    init=$(first_at "$body" 'moqr_cli_log_init(&log')
    capl=$(first_at "$body" 'print_capacity(cfg')
    [ -n "$capl" ] || fail "$fn: no ceiling gate"
    if [ -n "$init" ] && [ -n "$capl" ] && [ "$init" -ge "$capl" ]; then
        fail "$fn: the sink is initialised after the ceiling gate"
    fi
    ncap=$(count_in "$body" 'print_capacity(cfg')
    nprose=$(printf '%s\n' "$body" | grep -A1 -- 'print_capacity(cfg' | grep -c 'moqr_cli_log_prose_stream(&log)')
    [ "$ncap" = "$nprose" ] || fail "$fn: a ceiling print does not use the sink's prose stream"
    printf '%s\n' "$body" | grep -A1 -- 'print_capacity(cfg' | grep -q 'stdout' &&
        fail "$fn: a ceiling print names stdout"
    # readiness through the helper, after the sink exists
    [ "$(count_in "$body" 'moqr_cli_serve_log_readiness(&log')" = "1" ] ||
        fail "$fn: expected exactly one readiness sequence"
    # 3. every ordinary return after the sink exists finalizes through the one
    #    helper -- refusals before readiness included -- and no finish call
    #    sits anywhere else in the body
    # the sink exists from the line after its init refusal's own return
    init_ret=$(printf '%s\n' "$body" | awk -F: -v a="$init" '$1 > a' | grep -m1 'return 2;' | cut -d: -f1)
    [ -n "$init_ret" ] || fail "$fn: the sink init refusal cannot be located"
    nret=$(printf '%s\n' "$body" | awk -F: -v a="$init_ret" '$1 > a' | grep -cE '(^|[^a-z_])return ')
    ndone=$(printf '%s\n' "$body" | awk -F: -v a="$init_ret" '$1 > a' | grep -c 'return serve_done(&log,')
    [ "$nret" -ge 3 ] || fail "$fn: expected several ordinary returns after the sink init, found $nret"
    [ "$nret" = "$ndone" ] || fail "$fn: $((nret - ndone)) ordinary return(s) after the sink init bypass serve_done"
    [ "$(count_in "$body" 'moqr_cli_log_finish(')" = "0" ] || fail "$fn: finalizes outside serve_done"
    # returns BEFORE the sink exists never finalize
    [ "$(printf '%s\n' "$body" | awk -F: -v a="$init_ret" '$1 <= a' | grep -c 'serve_done(')" = "0" ] ||
        fail "$fn: a return before the sink init goes through serve_done"
    ret=$(last_at "$body" 'return serve_done(&log, admin_rc);')
    [ -n "$ret" ] || fail "$fn: the post-join return does not finalize"
    # the stop and attribution records of this composition
    [ "$(count_in "$body" 'moqr_cli_serve_log_lane(&log')" = "1" ] || fail "$fn: expected one lane-row emission site"
done
body=$(body_of cmd_serve)
[ "$(count_in "$body" 'moqr_cli_serve_log_stop_k1(&log')" = "1" ] || fail "cmd_serve: expected the single-facade stop record"
body=$(body_of cmd_serve_lanes)
[ "$(count_in "$body" 'moqr_cli_serve_log_pair(&log')" = "1" ] || fail "cmd_serve_lanes: expected one pair-row emission site"
[ "$(count_in "$body" 'moqr_cli_serve_log_stop_shard(&log')" = "1" ] || fail "cmd_serve_lanes: expected the per-shard stop record"
[ "$(count_in "$body" 'moqr_cli_serve_log_stop_total(&log')" = "1" ] || fail "cmd_serve_lanes: expected the total stop record"

# -- 4. the sink is a local ---------------------------------------------------
for st in serve_ctx_t serve_lanes_ctx_t; do
    if printf '%s\n' "$code" | awk -v st="$st" '
            $0 ~ "^typedef struct" { buf = "" }
            { buf = buf "\n" $0 }
            $0 ~ "} " st ";" { print buf; exit }' | grep -q 'moqr_cli_log'; then
        fail "$st carries the serve log"
    fi
done
if body_of admin_halt | grep -q 'moqr_cli_log\|moqr_cli_serve_log_\|serve_done'; then
    fail "the emergency halt reaches the serve log"
fi
[ "$(body_of serve_done | grep -c 'moqr_cli_log_finish(log)')" = "1" ] ||
    fail "serve_done does not finalize exactly once"
[ "$(body_of serve_done | grep -c 'moqr_cli_log_[a-z_]*(\|moqr_cli_serve_log_')" = "1" ] ||
    fail "serve_done does more than finalize"

# -- 5. verify/measure refuse JSON before the preflight ----------------------
body=$(body_of main)
load=$(first_at "$body" 'moqr_cli_config_load(')
refuse=$(first_at "$body" 'moqr_cli_verify_refuse_json_logging(&cfg')
pre=$(first_at "$body" 'moqr_cli_describe_capacity(&cfg')
[ -n "$load" ] && [ -n "$refuse" ] && [ -n "$pre" ] ||
    fail "main: the load, the JSON refusal or the preflight cannot be located"
if [ -n "$load" ] && [ -n "$refuse" ] && [ -n "$pre" ]; then
    [ "$load" -lt "$refuse" ] && [ "$refuse" -lt "$pre" ] ||
        fail "main: the JSON refusal is not between the config load and the preflight"
fi
printf '%s\n' "$body" | grep -B1 -- 'moqr_cli_verify_refuse_json_logging(&cfg' | grep -q 'strcmp(cmd, "")' ||
    fail "main: the JSON refusal is not scoped to the serve command"
printf '%s\n' "$body" | grep -A4 -- 'return print_capacity(&cfg' | grep -q 'stdout' ||
    fail "main: the capacity subcommand does not print to stdout"

# -- 6. the lifecycle proof is sans-I/O --------------------------------------
seam_code=$(strip_c "$seam")
if printf '%s\n' "$seam_code" | grep -qE '(^|[^a-zA-Z_])(socket|bind|listen|accept|connect|sleep|usleep|nanosleep)[[:space:]]*\('; then
    fail "the coordinator seam test opens a socket, listens or sleeps"
fi
printf '%s\n' "$seam_code" | grep -q 'api\.RegistrationOpen[[:space:]]*=' ||
    fail "the coordinator seam test does not inject its serve refusal through the fake facade API"

if [ "$fail" -ne 0 ]; then
    echo "FAIL: serve-log wiring violation(s)"
    exit 1
fi
echo "PASS: both serve paths emit through the serve log, finalize before returning, and keep the sink local"
