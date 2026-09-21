# Finite receiver runtime fixture

A real, finite RAW/LOC run: an independent C publisher (public service API
only) -> our `moq5-relay` on raw loopback -> the production `moq5` package
driving the shipped `examples/receive_loop.py`. Synthetic carrier bytes, not
playable media; no decoder. It opens sockets, so it is NOT part of the
deterministic suite and is run only by explicit invocation.

Declared data lives in `fixture.py` (count, payloads with embedded zero bytes,
sync/group starts, presentation times, track configuration). The publisher
sends exactly those bytes; its receipts are extra evidence, never the oracle.

## Build the publisher (consumer of an installed SDK; every input explicit)

    cmake -S bindings/python/tests/runtime -B build/py-runtime-fixture/publisher \
      -DCMAKE_BUILD_TYPE=Release \
      "-DCMAKE_PREFIX_PATH=<sdk install>;<picoquic install>" \
      -DMOQ_MSQUIC_PIN_INCLUDE_DIR=<msquic>/src/inc \
      -DMOQ_MSQUIC_PIN_LIBRARY=<msquic>/build-.../libmsquic.dylib \
      -DMOQ_PICOTLS_PREFIX=<picotls>/build -DPTLS_INCLUDE_DIR=<picotls>/include \
      -DOPENSSL_ROOT_DIR=/opt/homebrew/opt/openssl@3
    cmake --build build/py-runtime-fixture/publisher

## Run

    # offline first: comparator RED, ownership controls with stub children, the C
    # control reader's classification, and (after a run) verdict RED mutations
    python -I bindings/python/tests/runtime/run_finite_receiver.py --oracle-red
    python -I bindings/python/tests/runtime/run_finite_receiver.py --child-controls
    python -I bindings/python/tests/runtime/run_finite_receiver.py --control-selftests \
        --publisher build/py-runtime-fixture/publisher/moq5_runtime_publisher
    # one positive run (and --negative for the zero-object control); pull mode by default
    env -i PATH=/usr/bin:/bin DYLD_FALLBACK_LIBRARY_PATH=<msquic dylib dir> \
      python -I bindings/python/tests/runtime/run_finite_receiver.py \
        --package <installed moq5 dir> --relay <moq5-relay> \
        --publisher build/py-runtime-fixture/publisher/moq5_runtime_publisher [--negative] [--push]
    python -I bindings/python/tests/runtime/run_finite_receiver.py --verdict-red <run dir>/results.json

The supervisor is the direct owner of the relay, the publisher and the
receiver client and settles every handle itself; the receipt keeps the
declared and received inventories (payload bytes as hex), the description
images before and after close, every process status and the loaded native
images of the client and publisher.

Runs, credentials, logs and receipts land under `build/py-runtime-fixture/`
(git-ignored). The loader override names the external MsQuic directory the
installed relay and module were linked against; it is recorded in the
receipt.

## The finite PUBLISHING run (run_finite_publisher.py)

The mirror of the receiver fixture: the production `moq5` package driving the
shipped `examples/publish_loop.py` -> our own `moq5-relay` on raw loopback
QUIC -> a receiver child that drives the PUBLIC Receiver with THIS fixture's
stop policy (the declared inventory plus that track's ENDED event). It does
not run `receive_loop.py`, whose completion policy needs a VOD conversion or
an isComplete catalog that a publisher which never requests completion does
not produce, and it never requests completion to satisfy that policy.

    # offline first: the verdict comparator RED and the ownership controls
    python -I bindings/python/tests/runtime/run_finite_publisher.py --oracle-red
    python -I bindings/python/tests/runtime/run_finite_publisher.py --child-controls
    # one positive campaign, and the deliberate zero-object negative
    env -i PATH=/usr/bin:/bin DYLD_FALLBACK_LIBRARY_PATH=<msquic dylib dir> \
      python -I bindings/python/tests/runtime/run_finite_publisher.py \
        --package <installed moq5 dir> --relay <moq5-relay> [--negative]
    python -I bindings/python/tests/runtime/run_finite_publisher.py --verdict-red <run dir>

The publisher keeps its sender and endpoint alive until the receiver reports
its observations: `publish()` returning "submitted" does not release them.
The supervisor owns the relay, the publisher and the receiver and settles
every one of them; runs land under `build/py-publisher-fixture/`.

A positive run proves that those exact payload bytes, identities and mapped
object metadata reached a real receiver through our own relay, in order, and
that the track's ENDED event was observed. It does NOT prove the CAUSE of
that ENDED (the facade includes rejection in it), a wire END_OF_TRACK
discriminant, a flush, a remote acknowledgment, or any completion API.
