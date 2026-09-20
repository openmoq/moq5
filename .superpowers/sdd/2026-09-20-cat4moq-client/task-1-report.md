# Task 1 report: binary-safe inbound token payloads

## Changes

- Changed the shared resolved-token check to validate only the pointer/length
  representation. Token payload bytes, embedded zero bytes, and empty payloads
  are now left to the token-type verifier.
- Kept wire framing, alias operation, duplicate resolved-token, cache capacity,
  transaction commit/abort, and event scratch-copy behavior unchanged.
- Updated stale comments in the shared declaration and draft-16 SETUP handlers
  so they no longer claim that empty or NUL-containing payloads are malformed.
- Added exact-byte coverage using
  `d2 84 40 00 80 ff` for direct validation, draft-16 and draft-18 USE_VALUE,
  REGISTER/USE_ALIAS, alias replacement, retry rollback, and incoming SETUP.
- Changed scenario-generated REGISTER and USE_VALUE payloads to deterministic
  four-byte binary values containing an embedded zero byte.
- Updated empty-payload tests to assert successful transport where the wire
  envelope permits an empty value.

The binary fixture is intentionally only an opaque transport fixture. It is not
presented as a valid signed CWT; signed CBOR interoperability vectors remain
separate verifier coverage.

## TDD evidence

The initial focused run failed as expected:

- `auth_cache`: direct helper rejected the six-byte fixture.
- `session_auth`: draft-16 did not surface the binary token.
- `d18_auth`: draft-18 did not surface the binary token.
- `scenario_auth`: passed.

After the production change and test expectation updates, all four focused
tests passed.

## Validation

```sh
cmake --build build/dev --target test_auth_cache test_session_auth test_d18_auth test_scenario_auth -j4
ctest --test-dir build/dev -R '^(auth_cache|session_auth|d18_auth|scenario_auth)$' --output-on-failure
```

Result: 4/4 tests passed.

The same four targets were rebuilt and run under the prepared ASan+UBSan Debug
tree at `/tmp/moq5-cat4moq-sanitize`. Result: 4/4 tests passed with no
sanitizer findings.

`git diff --check` passed.

## Risks and boundaries

- Transport now accepts any non-NULL byte sequence and an empty sequence. This
  deliberately moves all payload-format and cryptographic checks to the
  token-type verifier.
- A positive length with a NULL pointer remains invalid.
- Structural token envelopes, invalid alias operations, truncated encodings,
  oversized parameter counts, duplicate aliases, cache overflow, transaction
  rollback, and event borrow epochs continue through their existing paths.
- This task does not add a generic CBOR parser or signed CAT/CWT verification.
