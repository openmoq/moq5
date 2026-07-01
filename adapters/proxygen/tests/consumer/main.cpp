/*
 * Cold-consumer smoke for moq::wt::Adapter (proxygen WebTransport adapter).
 *
 * Consumes the adapter exactly as a downstream app would: includes ONLY
 * the public header <moq/proxygen_wt.hpp> and links ONLY the public
 * imported target moq::adapter-proxygen-wt (resolved via
 * find_package(libmoq COMPONENTS adapter-proxygen-wt)). No private header,
 * no network, no fake transport.
 *
 * Proves the public surface compiles + links + runs: Config initializes,
 * and create() with an invalid config returns nullptr without touching a
 * transport.
 */
#include <moq/proxygen_wt.hpp>

#include <cstdio>

int main()
{
    moq::wt::Adapter::Config cfg{};   // all-null/zero: invalid config

    // Invalid config (no session/alloc/executor + null WebTransport) must
    // yield nullptr — exercises the public create() + Config layout with
    // no transport and no session.
    std::unique_ptr<moq::wt::Adapter> a =
        moq::wt::Adapter::create(cfg, nullptr);
    if (a != nullptr)
        return 1;

    std::printf("moq_proxygen_wt_consumer: create(invalid) -> nullptr OK\n");
    return 0;
}
