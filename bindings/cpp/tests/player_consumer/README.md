# C++ player consumer

A cold consumer of an installed libmoq SDK that uses `<moq/media_player.hpp>`
through public targets only (`moq::cpp` and `moq::service`). Like every
service-tier consumer it needs an installed picoquic package built with
`-DBUILD_HTTP=ON` (it exports `picoquic::picohttp-core`), of the same source
revision the SDK was built against:

    cmake -S bindings/cpp/tests/player_consumer -B build/player-consumer \
      "-DCMAKE_PREFIX_PATH=<sdk install>;<picoquic install>" \
      -DMOQ_PICOTLS_PREFIX=<picotls>/build -DPTLS_INCLUDE_DIR=<picotls>/include \
      -DOPENSSL_ROOT_DIR=/opt/homebrew/opt/openssl@3
    cmake --build build/player-consumer
    build/player-consumer/moq_player_consumer_test

CTest runs it as `cpp_player_install_consumer`
(`run_player_install_consumer.sh`): it installs a BUILD_HTTP picoquic and
libmoq into a test-owned prefix, in the parent tree's library type, and
builds this project against that prefix only. A build-tree consumer in
source-mode picoquic has no picohttp-core package to find, so the test
always goes through an install.
