# API Friction Notes — Basic Subscriber (Round 3)

## Resolved

- Namespace: explicit `moq_bytes_t parts[]` + `count`. Byte-safe, no variadic macro.
- Constructor: uniform `moq_config_create` with out-param + `moq_result_t`.
- Lifetime: advancing vs observing call categories documented in header.
- Event structs: per-kind named types (`moq_object_event_t`, etc.).

## Remaining friction

1. **`moq_bytes_from_str` for track_name:** wraps a string literal into
   `{(const uint8_t*)str, strlen(str)}`. Fine for ASCII. For binary track
   names, caller constructs `moq_bytes_t` directly with `{ptr, len}`.

2. **Filter type default:** `MOQ_SUBSCRIBE_PARAMS_INIT` zeros filter_type.
   Is 0 a valid filter? If not, library should detect and return `MOQ_ERR_INVAL`.

3. **`poll_events` returns `size_t`:** 0 = drained, >0 = count. No error
   path since polling is observing-only. Consider adding
   `moq_session_has_events(s)` predicate.

4. **`MOQ_DONE` as sentinel:** convention is `< 0 && != MOQ_DONE` means
   hard error. `MOQ_DONE` itself is just "nothing available right now."

## Future convenience (not in sketch)

- `moq_session_open(role, profile, alloc, now, &s)` one-shot constructor
- `MOQ_TRY(expr)` error-propagation macro
- Borrow epoch for debug-build lifetime validation
