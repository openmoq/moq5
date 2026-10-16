# Dockerfile for moq-interop-client
#
# Build context should be the parent directory containing libmoq/,
# picoquic/, and picotls/ as siblings.
# Both providers must be independent Git checkouts, not copied worktree gitfiles.
#
# Example:
#   cd /path/to/parent   # contains libmoq/ picoquic/ picotls/
#   docker build -f libmoq/tools/moq-interop-client/Dockerfile -t moq-interop-client .
#
# Run:
#   docker run --rm moq-interop-client --relay moqt://relay:4443 --test setup-only --tls-disable-verify
#   docker run --rm -e RELAY_URL=moqt://relay:4443 -e TESTCASE=setup-only -e TLS_DISABLE_VERIFY=1 moq-interop-client

FROM ubuntu:24.04 AS build

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential cmake pkg-config libssl-dev ca-certificates git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src

COPY picotls/ picotls/
COPY picoquic/ picoquic/
COPY libmoq/ libmoq/

# Apply the ordered lists from scripts/setup_picoquic_deps.sh. Accept patched
# contexts, but refuse missing patches or conflicting sources before building.
RUN cd /src/picoquic \
    && { test -d .git && test "$(git rev-parse --show-toplevel 2>/dev/null)" = /src/picoquic \
         || { echo 'picoquic patch input must be its own git root' >&2; exit 1; }; } \
    && for name in 0002-reset-stream-after-fin 0006-own-application-close-reason; do \
         patch=/src/libmoq/patches/picoquic/$name.patch; \
         test -f "$patch" || { echo "picoquic patch $name missing" >&2; exit 1; }; \
         if git apply --check "$patch" >/dev/null 2>&1; then \
           git apply "$patch" || exit $?; \
         elif git apply --reverse --check "$patch" >/dev/null 2>&1; then \
           echo "picoquic patch $name already applied"; \
         else \
           echo "picoquic patch $name conflicts" >&2; exit 1; \
         fi; \
       done

# Initialize compiler-visible scratch and keep minicrypto archive members distinct.
RUN cd /src/picotls \
    && { test -d .git && test "$(git rev-parse --show-toplevel 2>/dev/null)" = /src/picotls \
         || { echo 'picotls patch input must be its own git root' >&2; exit 1; }; } \
    && for name in 0001-initialize-compiler-visible-scratch 0002-minicrypto-distinct-chacha20-object; do \
         patch=/src/libmoq/patches/picotls/$name.patch; \
         test -f "$patch" || { echo "picotls patch $name missing" >&2; exit 1; }; \
         if git apply --check "$patch" >/dev/null 2>&1; then \
           git apply "$patch" || exit $?; \
         elif git apply --reverse --check "$patch" >/dev/null 2>&1; then \
           echo "picotls patch $name already applied"; \
         else \
           echo "picotls patch $name conflicts" >&2; exit 1; \
         fi; \
       done

# Build picotls (fresh build dir to avoid host CMakeCache)
RUN cmake -B /build/picotls -S picotls \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_PROJECT_picotls_INCLUDE=/src/libmoq/cmake/MoqPicotlsBuildPolicy.cmake \
    -DCMAKE_INSTALL_PREFIX=/usr/local \
    && cmake --build /build/picotls -j$(nproc) \
    && cmake --install /build/picotls

# Build libmoq with the interop client on the service tier.
# The endpoint owns transport selection + version negotiation, so the client
# needs BOTH transports compiled in:
#   raw QUIC      -> MOQ_BUILD_PQ_THREADED (+ ADAPTER_PICOQUIC)
#   WebTransport  -> MOQ_BUILD_PICO_WT_MANAGED (+ ADAPTER_PICO_WT)
# and the service tier itself (+ its public moq::msf dependency).
RUN cmake -B /build/libmoq -S libmoq \
    -DCMAKE_BUILD_TYPE=Release \
    -DMOQ_BUILD_ADAPTER_PICOQUIC=ON \
    -DMOQ_BUILD_PQ_THREADED=ON \
    -DMOQ_BUILD_ADAPTER_PICO_WT=ON \
    -DMOQ_BUILD_PICO_WT_MANAGED=ON \
    -DMOQ_BUILD_MSF=ON \
    -DMOQ_BUILD_SERVICE=ON \
    -DMOQ_BUILD_INTEROP_CLIENT=ON \
    -DMOQ_BUILD_TESTS=OFF \
    -DMOQ_PICOQUIC_SOURCE_DIR=/src/picoquic \
    -DMOQ_PICOTLS_PREFIX=/build/picotls \
    -DPTLS_INCLUDE_DIR=/src/picotls/include \
    && cmake --build /build/libmoq -j$(nproc) --target moq-interop-client

# Runtime image
FROM ubuntu:24.04

RUN apt-get update && apt-get install -y --no-install-recommends \
    libssl3t64 ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /build/libmoq/tools/moq-interop-client/moq-interop-client /usr/local/bin/

ENTRYPOINT ["moq-interop-client"]
