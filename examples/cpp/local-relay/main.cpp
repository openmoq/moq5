//
// local-relay: a tiny MoQ relay that uses libmoq exactly the way a real
// relay host (e.g. an openmoq/moxygen-style relay) should, but with no
// third-party transport stack, so the OWNERSHIP pattern stays in focus.
//
// The boundary this example demonstrates:
//
//   HOST (this file) owns:           libmoq owns:
//     - the set of sessions            - moq::session protocol state
//     - the track -> subscribers map   - actions/events
//     - the fan-out loop               - rcbuf (moq::buffer) lifetime
//     - per-recipient backpressure     - the transport adapter (n/a here)
//
// libmoq provides NO relay framework: no fan-out API, no recipient group,
// no cache / namespace tree / subscription registry / scheduler. The relay
// below is plain host code over the low-level moq::session event/action
// API, the same layer a moxygen-derived relay drives.
//
// The only thing standing in for a transport is pump(): it drains a
// session's actions and feeds the bytes to its peer. It is intentionally
// tiny for this one-object demo. A deep in-process harness must also model
// receive-side backpressure: if on_data_bytes() returns would_block, stop
// feeding that stream/link, drain the peer, then retry with an empty data
// call before delivering more bytes. A REAL relay replaces pump() with its
// proxygen::WebTransport accept loop and attaches each connection's
// moq::session to it; the same ownership, routing, fan-out, and
// backpressure pattern carries over (a production relay also generalizes the
// single-track/single-group shortcuts this example takes).
//
// Scope: single shard (this thread), one namespace/track, one group. The
// routing is a track-keyed map (not a single hardcoded track) so growing to
// many tracks is additive; per-group subgroup lifecycle is the one thing
// deliberately left for later and is localized to the fan-out step. Because
// everything is same-shard, the upstream object's payload buffer is reused
// across subscribers as-is (a cheap shared ref, never copied); crossing a
// shard would instead use moq::buffer::clone_for_shard().
//

#include <moq/moq.hpp>

#include <cstdint>
#include <cstdio>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace {

// ---- Transport stand-in -------------------------------------------------
// Drain every queued action from `from` and feed the bytes into `to`.
// Returns how many actions were moved (0 == nothing left this pass). A real
// relay hands these bytes to a QUIC/WebTransport stack instead.
//
// This shim is deliberately not a complete transport model: it is enough for
// the single-object example below, but it ignores receive-side would_block.
// A stress/depth harness must retain blocked inbound bytes per stream, drain
// the peer, and retry on_data_bytes(ref, {}, false, now) before delivering
// more bytes for that stream.
size_t pump_once(moq::session &from, moq::session &to, uint64_t now)
{
    size_t moved = 0;
    while (auto action = from.poll_action()) {
        ++moved;
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
    return moved;
}

// Pump both directions until the link goes quiet.
void pump_link(moq::session &a, moq::session &b, uint64_t now)
{
    for (int i = 0; i < 64; ++i)
        if (pump_once(a, b, now) + pump_once(b, a, now) == 0)
            return;
}

// ---- Host-owned routing -------------------------------------------------
// A subscriber connection from the relay's point of view. `relay` is the
// libmoq session the relay drives; `client` is the in-process peer (the
// transport stand-in, absent in a real relay). `sub`/`blocked` are pure
// host bookkeeping; libmoq owns none of this.
// An object the relay accepted upstream but could not yet hand to a backed-up
// subscriber. The relay OWNS the payload (a retained ref to the
// object_received buffer) so it can retry without the publisher's original
// buffer, which a real relay never has.
struct Pending {
    uint64_t    group_id;
    uint64_t    object_id;
    moq::buffer payload;
};

struct Downstream {
    moq::session                        relay;   // relay-side session (server)
    moq::session                        client;  // subscriber peer (stand-in)
    std::optional<moq::subscription>    sub;     // accepted downstream sub
    std::optional<moq::subgroup_handle> sg;      // this subscriber's open subgroup
    std::optional<Pending>              pending; // object awaiting retry (relay-owned)
    const char                         *label = "";
};

// The relay holds only host-owned state: the upstream session, the
// downstream subscribers, and the routing maps. No libmoq object owns
// membership.
struct LocalRelay {
    moq::session upstream;   // relay-side session toward the publisher (client)
    moq::session origin;     // publisher peer (transport stand-in)

    std::vector<std::unique_ptr<Downstream>> subs;            // owners
    // track key -> subscribers interested in it (exact-match routing)
    std::unordered_map<std::string, std::vector<Downstream *>> route;
    // tracks we've already subscribed upstream for, and the reverse map
    // from the upstream subscription handle back to the track key.
    std::unordered_map<std::string, moq::subscription>        upstream_subs;
    std::unordered_map<moq::subscription, std::string>        track_of_upstream;

    LocalRelay(moq::session up, moq::session orig)
        : upstream(std::move(up)), origin(std::move(orig)) {}
};

// Build a stable owned key from a (namespace, track). The event's
// namespace/track are BORROWED and expire on the next advancing session
// call, so the relay copies them immediately, the single sharpest edge a
// real relay author must respect.
std::string track_key(const moq::namespace_name &ns, moq::bytes_view track)
{
    std::string key;
    for (size_t i = 0; i < ns.count(); ++i) {
        std::string_view part = ns[i];   // namespace_name::operator[] -> string_view
        key.append(part.data(), part.size());
        key.push_back('\x1f');
    }
    key.push_back('\x1e');
    key.append(reinterpret_cast<const char *>(track.data()), track.size());
    return key;
}

// A subscriber blocked (at open or write): the relay stashes the object it
// owns so it can retry later. Slice 1 is single-object, so a second pending
// object before the first drains is a bug: fail loudly rather than queue.
void stash_pending(Downstream *d, uint64_t group_id, uint64_t object_id,
                   moq::buffer payload)
{
    if (d->pending) {
        std::fprintf(stderr, "FAIL: %s already has a pending object "
                     "(Slice 1 retries one object at a time)\n", d->label);
        std::exit(1);
    }
    d->pending = Pending{ group_id, object_id, std::move(payload) };
}

} // namespace

int main()
{
    const uint64_t now = 0;
    std::string_view NS  = "demo";
    std::string_view TRK = "video";
    // The real object's id is higher than any backpressure-filler id below,
    // so a backed-up subscriber's fan-out write fails on the FULL QUEUE
    // (would_block), never on object-id ordering. write_object enforces
    // increasing object ids within a subgroup.
    const uint64_t OBJ = 100;

    // ---- Build the relay + its upstream link -----------------------------
    // The relay is a SUBSCRIBER toward the publisher: its upstream session is
    // the CLIENT; the publisher origin is the SERVER (accepts + publishes).
    auto relay = std::make_unique<LocalRelay>(
        moq::session::create({ .perspective = moq::perspective::client,
                               .send_request_capacity = true,
                               .initial_request_capacity = 8 }).value(),
        moq::session::create({ .perspective = moq::perspective::server,
                               .send_request_capacity = true,
                               .initial_request_capacity = 8 }).value());

    relay->upstream.start(now);
    pump_link(relay->upstream, relay->origin, now);
    relay->upstream.poll_event();  // drain setup
    relay->origin.poll_event();

    // ---- Build three downstream subscriber links -------------------------
    // The relay is the SERVER for each subscriber. "B" gets a shallow action
    // queue so we can model it as a backed-up/slow consumer below.
    const char *labels[3] = { "A", "B", "C" };
    for (int i = 0; i < 3; ++i) {
        uint32_t max_actions = (i == 1) ? 8 : 0;  // 0 = default; B is shallow
        auto d = std::make_unique<Downstream>(Downstream{
            moq::session::create({ .perspective = moq::perspective::server,
                                   .send_request_capacity = true,
                                   .initial_request_capacity = 8,
                                   .max_actions = max_actions }).value(),
            moq::session::create({ .perspective = moq::perspective::client,
                                   .send_request_capacity = true,
                                   .initial_request_capacity = 8 }).value(),
            std::nullopt, std::nullopt, std::nullopt, labels[i] });
        d->client.start(now);
        pump_link(d->relay, d->client, now);
        d->relay.poll_event();
        d->client.poll_event();
        relay->subs.push_back(std::move(d));
    }

    // ---- Each subscriber subscribes; the relay accepts + routes ----------
    // On the first demand for a track, the relay subscribes UPSTREAM toward
    // the publisher (real relays subscribe on demand).
    for (auto &d : relay->subs) {
        d->client.subscribe({ .ns = {NS}, .track = TRK,
                              .filter = moq::subscribe_filter::largest_object },
                            now).value();
        pump_link(d->relay, d->client, now);

        while (auto ev = d->relay.poll_event()) {
            ev->visit(
                [&](const moq::event::subscribe_request &req) {
                    // Copy the borrowed namespace/track NOW, before the
                    // accept advances the session and invalidates them.
                    std::string key = track_key(req.track_namespace(),
                                                req.track_name);
                    d->relay.accept_subscribe(req.sub, {}, now).value();
                    d->sub = req.sub;

                    // Host-owned routing: record this subscriber for the track.
                    relay->route[key].push_back(d.get());

                    // First demand for the track -> subscribe upstream.
                    if (relay->upstream_subs.find(key) ==
                        relay->upstream_subs.end()) {
                        auto h = relay->upstream.subscribe(
                            { .ns = {NS}, .track = TRK,
                              .filter = moq::subscribe_filter::largest_object },
                            now).value();
                        relay->upstream_subs.emplace(key, h);
                        relay->track_of_upstream.emplace(h, key);
                    }
                },
                [](const auto &) {});
        }
        pump_link(d->relay, d->client, now);
        d->client.poll_event();  // drain subscribe_ok
    }

    // ---- Publisher accepts the relay's upstream subscribe ----------------
    pump_link(relay->upstream, relay->origin, now);
    std::optional<moq::subscription> origin_sub;
    while (auto ev = relay->origin.poll_event()) {
        ev->visit(
            [&](const moq::event::subscribe_request &req) {
                relay->origin.accept_subscribe(req.sub, {}, now).value();
                origin_sub = req.sub;
            },
            [](const auto &) {});
    }
    pump_link(relay->upstream, relay->origin, now);
    relay->upstream.poll_event();  // drain subscribe_ok

    // ---- Model subscriber B as backed-up: fill its relay action queue ----
    // Its downstream consumer is slow, so the relay's send queue for B is
    // full when the real object arrives. A separate filler stream is used so
    // the demonstration is purely about the queue state.
    {
        Downstream *B = relay->subs[1].get();
        auto filler = moq::buffer::create(
            reinterpret_cast<const uint8_t *>("x"), 1).value();
        B->sg = B->relay.open_subgroup(*B->sub, {}, now).value();
        bool blocked = false;
        for (uint64_t oid = 0; oid < 1000 && !blocked; ++oid) {
            auto r = B->relay.write_object(*B->sg, oid, filler, now);
            if (!r.ok()) {
                if (r.error().code() == moq::errc::would_block)
                    blocked = true;
                else {
                    std::fprintf(stderr, "FAIL: filler write error %d\n",
                                 (int)r.error().code());
                    return 1;
                }
            }
        }
        if (!blocked) {
            std::fprintf(stderr, "FAIL: expected B's queue to fill (block)\n");
            return 1;
        }
    }

    // ---- Publisher writes ONE object; relay forwards it ------------------
    auto pub_payload = moq::buffer::create(
        reinterpret_cast<const uint8_t *>("hello relay"), 11).value();
    {
        auto sg = relay->origin.open_subgroup(*origin_sub, {}, now).value();
        relay->origin.write_object(sg, OBJ, pub_payload, now).value();
        relay->origin.close_subgroup(sg, now).value();
    }
    pump_link(relay->upstream, relay->origin, now);

    // The relay's upstream session surfaces the object. Look up the track it
    // belongs to and run the HOST FAN-OUT LOOP, the part a real relay copies.
    int accepted = 0, blocked = 0;
    while (auto ev = relay->upstream.poll_event()) {
        ev->visit(
            [&](const moq::event::object_received &obj) {
                auto it = relay->track_of_upstream.find(obj.sub);
                if (it == relay->track_of_upstream.end())
                    return;
                // One owned ref to the payload, reused for every same-shard
                // subscriber (a cheap shared ref, not a copy; see header).
                moq::buffer payload = obj.payload_owned();

                for (Downstream *d : relay->route[it->second]) {
                    // One subgroup per subscriber for this single-group MVP
                    // (per-group lifecycle is the localized bit deferred to
                    // later). Open lazily; a backed-up subscriber already has
                    // its subgroup open from earlier writes.
                    if (!d->sg) {
                        auto sg = d->relay.open_subgroup(
                            *d->sub, { .group_id = obj.group_id }, now);
                        if (!sg.ok()) {
                            if (sg.error().code() == moq::errc::would_block) {
                                // Pending-open: stash the relay-owned object
                                // and retry the open later. Keep going.
                                stash_pending(d, obj.group_id, obj.object_id,
                                              payload);
                                ++blocked; continue;
                            }
                            std::fprintf(stderr, "FAIL: open_subgroup %d\n",
                                         (int)sg.error().code());
                            std::exit(1);
                        }
                        d->sg = *sg;
                    }
                    // Per-recipient write. would_block stashes the object for
                    // retry and is SKIPPED; it must not abort the loop or
                    // drop the other subscribers.
                    auto w = d->relay.write_object(*d->sg, obj.object_id,
                                                   payload, now);
                    if (w.ok()) {
                        ++accepted;
                    } else if (w.error().code() == moq::errc::would_block) {
                        stash_pending(d, obj.group_id, obj.object_id, payload);
                        ++blocked;
                    } else {
                        std::fprintf(stderr, "FAIL: write_object sub=%s "
                                     "code=%d payload=%zu g=%llu o=%llu\n",
                                     d->label, (int)w.error().code(),
                                     payload.size(),
                                     (unsigned long long)obj.group_id,
                                     (unsigned long long)obj.object_id);
                        std::exit(1);
                    }
                }
            },
            [](const auto &) {});
    }

    // One blocked recipient (B) did not stop A and C.
    if (accepted != 2 || blocked != 1) {
        std::fprintf(stderr, "FAIL: fan-out accepted=%d blocked=%d (want 2/1)\n",
                     accepted, blocked);
        return 1;
    }

    // ---- A and C deliver, independent of B's block -----------------------
    int delivered = 0;
    for (auto &d : relay->subs) {
        if (d->pending) continue;   // blocked subscriber handled in retry
        pump_link(d->relay, d->client, now);
        while (auto ev = d->client.poll_event())
            ev->visit(
                [&](const moq::event::object_received &obj) {
                    if (obj.object_id == OBJ && obj.payload_data.size() == 11)
                        ++delivered;
                },
                [](const auto &) {});
    }
    if (delivered != 2) {
        std::fprintf(stderr, "FAIL: expected 2 deliveries, got %d\n", delivered);
        return 1;
    }

    // ---- Retry the blocked subscriber after its queue drains -------------
    // Its data is NOT dropped; once the downstream catches up, the relay
    // retries. (B's filler backlog drains as we pump.)
    for (auto &d : relay->subs) {
        if (!d->pending) continue;
        Pending &p = *d->pending;            // RELAY-OWNED object + payload
        pump_link(d->relay, d->client, now); // downstream catches up
        while (d->client.poll_event()) {}    // drain its backlog
        // Retry from the relay's own pending state, NOT the publisher's
        // buffer (a real relay never has that). Open the subgroup for the
        // pending group if the recipient blocked at open; then write the
        // pending object. Clear pending only after the write succeeds.
        if (!d->sg)
            d->sg = d->relay.open_subgroup(
                *d->sub, { .group_id = p.group_id }, now).value();
        d->relay.write_object(*d->sg, p.object_id, p.payload, now).value();
        uint64_t want = p.object_id;
        d->pending.reset();                  // cleared only on success
        pump_link(d->relay, d->client, now);
        bool got = false;
        while (auto ev = d->client.poll_event())
            ev->visit(
                [&](const moq::event::object_received &obj) {
                    if (obj.object_id == want && obj.payload_data.size() == 11)
                        got = true;
                },
                [](const auto &) {});
        if (!got) {
            std::fprintf(stderr, "FAIL: blocked subscriber %s never got "
                         "retried object\n", d->label);
            return 1;
        }
    }

    std::printf("local-relay: fanned one upstream object to 3 subscribers "
                "(A/C immediate, B blocked-then-retried). PASS\n");
    return 0;
}
