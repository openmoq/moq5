#!/usr/bin/env bash
# The upstream-termination causes must stay split, and neither may claim a
# timeout that did not elapse.
#
#   draft-18 Section 10.4 GOAWAY  -> the upstream is departing
#   draft-18 Section 10.6 REDIRECT-> nobody is departing; served elsewhere
#
# These map to different downstream terminals, and the owner-migration path in
# the shard layer must not report TIMEOUT either (Section 10.6 defines TIMEOUT
# as an implementation timeout). One of the three sites is not reachable from a
# test binding — the shard's own bind pump consumes the reject intent inside
# the same step — so the mapping is pinned here at the source.
#
# Argument 1 is relay/src/bind/moqr_bind.c, argument 2 is
# relay/src/shard/moqr_shards.c.
set -u

bind_c="${1:-}"
shard_c="${2:-}"
if [ ! -f "$bind_c" ] || [ ! -f "$shard_c" ]; then
    echo "FAIL: usage: $0 <moqr_bind.c> <moqr_shards.c>" >&2
    exit 2
fi

failures=0
fail() { echo "FAIL: $1" >&2; failures=$((failures + 1)); }

# Strip comments and string literals so prose cannot satisfy a check.
strip() {
    sed -e 's|/\*.*\*/||g' "$1" |
    awk '/\/\*/{inc=1} !inc{print} /\*\//{inc=0}' |
    sed -e 's|//.*||' -e 's|"[^"]*"||g'
}

bind_src=$(strip "$bind_c")
shard_src=$(strip "$shard_c")

# 1. The two causes exist as distinct tags.
for tag in BIND_UP_END_GOAWAY BIND_UP_END_REDIRECT; do
    if ! printf '%s\n' "$bind_src" | grep -q "$tag"; then
        fail "bind: cause tag '$tag' is gone; the two signals have collapsed"
    fi
done

# 2. The PUBLISH_DONE mapping: GOAWAY -> GOING_AWAY, REDIRECT -> TRACK_ENDED,
#    and the two must not return the same status.
pd_body=$(printf '%s\n' "$bind_src" |
          awk '/bind_upstream_end_pd\(bind_upstream_end_t/{f=1} f{print} f&&/^}/{exit}')
goaway_pd=$(printf '%s\n' "$pd_body" |
            awk '/BIND_UP_END_GOAWAY/{f=1} f&&/return/{print; exit}')
redirect_pd=$(printf '%s\n' "$pd_body" |
              awk '/BIND_UP_END_REDIRECT/{f=1} f&&/return/{print; exit}')

case "$goaway_pd" in
*MOQR_PD_GOING_AWAY*) ;;
*) fail "bind: GOAWAY no longer maps to PUBLISH_DONE GOING_AWAY (got:${goaway_pd})" ;;
esac
case "$redirect_pd" in
*MOQR_PD_TRACK_ENDED*) ;;
*) fail "bind: REDIRECT no longer maps to PUBLISH_DONE TRACK_ENDED (got:${redirect_pd})" ;;
esac
if [ "$goaway_pd" = "$redirect_pd" ]; then
    fail "bind: GOAWAY and REDIRECT return the same PUBLISH_DONE status"
fi

# 3. The REQUEST_ERROR fallback is the generic code, never TIMEOUT or REDIRECT.
if ! printf '%s\n' "$bind_src" | grep -q '#define BIND_REQERR_INTERNAL  *0x0u'; then
    fail "bind: BIND_REQERR_INTERNAL is not defined as 0x0"
fi
if printf '%s\n' "$bind_src" | grep -q 'BIND_REQERR_TIMEOUT'; then
    fail "bind: a REQUEST_ERROR TIMEOUT constant is back"
fi
reqerr_body=$(printf '%s\n' "$bind_src" |
              awk '/bind_upstream_end_reqerr\(bind_upstream_end_t/{f=1} f{print} f&&/^}/{exit}')
if printf '%s\n' "$reqerr_body" | grep -q 'return' &&
   printf '%s\n' "$reqerr_body" | grep 'return' |
       grep -qv 'BIND_REQERR_INTERNAL'; then
    fail "bind: the pending-upstream REQUEST_ERROR is no longer the generic code"
fi

# 4. The shard owner-migration site: generic, and no TIMEOUT constant left.
if printf '%s\n' "$shard_src" | grep -q 'R_ERR_TIMEOUT'; then
    fail "shard: R_ERR_TIMEOUT is back on the owner-migration path"
fi
if ! printf '%s\n' "$shard_src" | grep -q '#define R_ERR_INTERNAL  *0x0u'; then
    fail "shard: R_ERR_INTERNAL is not defined as 0x0"
fi
mig=$(printf '%s\n' "$shard_src" |
      grep -A 2 'moqr_core_upstream_error(' | grep 'track_gen, R_ERR')
if [ -z "$mig" ]; then
    fail "shard: the owner-migration upstream_error call site was not found"
elif printf '%s\n' "$mig" | grep -qv 'R_ERR_INTERNAL'; then
    fail "shard: an owner-migration upstream_error uses a non-generic code"
fi

if [ "$failures" -ne 0 ]; then
    echo "FAIL: $failures migration-code violation(s)" >&2
    exit 1
fi
echo "PASS: upstream-termination causes stay split and truthful"
exit 0
