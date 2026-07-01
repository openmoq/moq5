//
// local-publish-subscribe: two MoQ sessions exchange one object in-process.
//
// No sockets, no threads, no wall clock. The caller explicitly pumps
// actions from one session into the other, demonstrating the sans-I/O
// architecture through the C++ binding.
//

#include <moq/moq.hpp>

#include <cstdio>
#include <cstring>
#include <vector>

// Sans-I/O pump: drain actions from one session and feed them into
// another, translating SEND_DATA/SEND_CONTROL/SEND_DATAGRAM into
// the corresponding input calls. A real adapter would hand these
// to a QUIC stack instead.
static void pump(moq::session &from, moq::session &to, uint64_t now)
{
    while (auto action = from.poll_action()) {
        action->visit(
            [&](const moq::action::send_control &a) {
                to.on_control_bytes(a.data, now);
            },
            [&](const moq::action::send_data &a) {
                std::vector<uint8_t> buf;
                buf.insert(buf.end(), a.header.begin(), a.header.end());
                buf.insert(buf.end(), a.payload.begin(), a.payload.end());
                to.on_data_bytes(a.ref, buf, a.fin, now);
            },
            [&](const moq::action::send_datagram &a) {
                to.on_datagram(a.data, now);
            },
            [](const auto &) {});
    }
}

int main()
{
    // ---- Create two sessions: client (subscriber) and server (publisher)

    auto client = moq::session::create({
        .perspective              = moq::perspective::client,
        .send_request_capacity    = true,
        .initial_request_capacity = 4,
    }).value();

    auto server = moq::session::create({
        .perspective              = moq::perspective::server,
        .send_request_capacity    = true,
        .initial_request_capacity = 4,
    }).value();

    // ---- Handshake

    client.start(0);
    pump(client, server, 0);
    pump(server, client, 0);

    // Drain setup events
    server.poll_event();
    client.poll_event();

    // ---- Client subscribes to demo/video

    auto sub = client.subscribe({
        .ns     = {"demo"},
        .track  = "video",
        .filter = moq::subscribe_filter::largest_object,
    }, 0).value();

    pump(client, server, 0);

    // ---- Server accepts the subscription

    moq::subscription srv_sub;
    server.poll_event()->visit(
        [&](const moq::event::subscribe_request &req) {
            std::printf("server: subscribe request for %.*s/%.*s\n",
                        (int)req.track_namespace()[0].size(),
                        req.track_namespace()[0].data(),
                        (int)req.track_name.size(),
                        (const char *)req.track_name.data());
            srv_sub = req.sub;
        },
        [](const auto &) {});

    server.accept_subscribe(srv_sub, {}, 0);
    pump(server, client, 0);
    client.poll_event(); // drain subscribe_ok

    // ---- Server publishes one object

    auto payload = moq::buffer::create(
        reinterpret_cast<const uint8_t *>("hello moq"), 9).value();

    auto sg = server.open_subgroup(srv_sub, {}, 0).value();
    server.write_object(sg, 0, payload, 0);
    server.close_subgroup(sg, 0);

    pump(server, client, 0);

    // ---- Client receives the object

    bool received = false;
    client.poll_event()->visit(
        [&](const moq::event::object_received &obj) {
            std::printf("received demo/video object %llu: %.*s\n",
                        (unsigned long long)obj.object_id,
                        (int)obj.payload_data.size(),
                        (const char *)obj.payload_data.data());
            received = (obj.payload_data.size() == 9 &&
                        std::memcmp(obj.payload_data.data(),
                                    "hello moq", 9) == 0);
        },
        [](const auto &) {});

    return received ? 0 : 1;
}
