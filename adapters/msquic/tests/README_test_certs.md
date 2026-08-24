# `test_only_loopback_cert.pem` / `test_only_loopback_key.pem`

**A fixed, self-signed loopback identity that exists only so the MsQuic
tests can open a TLS connection to 127.0.0.1. It must never be used for
anything else.** The private key is committed in the clear, so treat it as
public: it secures nothing, and any service presenting it is trivially
impersonated.

The pair is checked in rather than generated per run. Generating a fresh
identity through an ambient `openssl` added no security coverage, made every
loopback test depend on that tool being on `PATH` at configure time, and —
worse — made the *registration* of those tests conditional on the discovery
succeeding, so a fresh configure could silently drop them.

## Consumers

Both paths are passed directly as argv by
`adapters/msquic/CMakeLists.txt` (`MOQ_MSQUIC_TEST_CERT` /
`MOQ_MSQUIC_TEST_KEY`); there is no CTest fixture to depend on.

| test | lane |
|---|---|
| `msquic_recv_loopback` | correctness |
| `msquic_over_window_credit` | qualification |
| `msquic_loopback` | correctness |
| `msquic_reap` | correctness |
| `msquic_conformance` | correctness |
| `msquic_stress` | soak |

## Provenance, and where it is recorded

The files are kept as **pure PEM**, with nothing prepended to either
payload. That is a deliberate choice rather than a claim about every
parser: PEM readers vary in how much leading text they tolerate, and a
fixture that depends on that tolerance is a fixture that breaks on the next
consumer. "Test only" is carried outside the payload instead, by:

- these filenames;
- this file;
- the CMake comment at the definition of `MOQ_MSQUIC_TEST_CERT`;
- the certificate's own subject:
  `CN=localhost, O=libmoq test fixture, OU=test-only-do-not-use`.

## Regenerating

Valid until **2046-08-12**. If it ever expires, or a stronger key is wanted,
regenerate by hand from `adapters/msquic/tests/` and commit the result:

```sh
openssl req -x509 -newkey rsa:2048 -sha256 -days 7300 -nodes \
    -keyout test_only_loopback_key.pem \
    -out    test_only_loopback_cert.pem \
    -subj "/CN=localhost/O=libmoq test fixture/OU=test-only-do-not-use"
```

`CN=localhost` is what the loopback tests connect to. They also set
`insecure_skip_verify` on the client side, so the identity is never actually
validated — it exists because the MsQuic *server* configuration requires a
credential to load.
