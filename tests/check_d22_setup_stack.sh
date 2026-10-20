#!/bin/bash
set -euo pipefail
source "$(dirname "$0")/stack_gate_support.sh"
stack_init "$@"
stack_positive
stack_compile profile "${MEASURE_TU:-$root/core/src/session/profile_d22.c}"

# Require the control owner even when the handler has its own emitted frame.
stack_owner profile d22_process_control_data
stack_limit
stack_owner profile d22_handle_setup d22_process_control_data
setup_owner=$measured_owner
stack_limit
stack_owner profile d22_setup_tokens d22_handle_setup "$setup_owner"
stack_limit
if [[ $compiler_kind == gcc && $setup_owner != d22_handle_setup ]]; then
    stack_owner_controls profile d22_setup_tokens d22_handle_setup "$setup_owner"
fi

# The shared completion is a real out-of-line SETUP owner, not part of an
# inlined handler measurement. Keep its own measurement and limit explicit.
stack_compile completion "$root/core/src/session/session_setup.c"
stack_owner completion session_setup_complete_peer
stack_limit

# These inherited frames are not SETUP owners and are not qualified by this
# gate. An undeclared new oversized owner is a failure, not a warning exclusion.
awk -F '\t' '
    BEGIN {
        inherited["d22_encode_subscribe"]=1
        inherited["d22_encode_subscribe_ok"]=1
        inherited["d22_encode_request_update_ok"]=1
        inherited["d22_encode_request_error"]=1
        inherited["d22_encode_request_update"]=1
        inherited["d22_encode_publish_namespace"]=1
        inherited["d22_encode_publish"]=1
        inherited["d22_encode_publish_state_notify"]=1
        inherited["d22_encode_fetch"]=1
        inherited["d22_encode_subscribe_namespace"]=1
        inherited["d22_encode_namespace_msg"]=1
        inherited["d22_encode_subscribe_tracks"]=1
        inherited["d22_encode_publish_blocked"]=1
        inherited["d22_encode_namespace_update"]=1
        inherited["d22_decode_ns_sub_request"]=1
        inherited["d22_encode_track_status"]=1
        inherited["d22_encode_track_status_ok"]=1
        inherited["d22_pb_fill"]=1
    }
    $2>=1024 {
        name=$1; sub(/^.*:/,"",name)
        if (name=="d22_handle_setup" || name=="d22_process_control_data" || name=="d22_setup_tokens") next
        if (!inherited[name]) {
            print "UNEXPECTED[inherited.frame]: " $0 > "/dev/stderr"; bad=1; next
        }
        printf "OPEN[inherited.frame.%s]: %s bytes; unrelated unresolved frame\n",name,$2
    }
    END {exit bad}
' "$out/profile.su" > "$out/inherited.log" || stack_fail inventory.unknown 'undeclared oversized profile owner'
LC_ALL=C sort "$out/inherited.log"
test "$gate_failed" -eq 0 || stack_fail setup.frame 'one or more measured SETUP owners exceeds1023 bytes'
printf 'PASS[setup.frame.%s]: named owners below1024; not a whole-call-stack bound\n' "$compiler_kind"
