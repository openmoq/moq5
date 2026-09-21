"""Subscriber-demand queries on the sender.

    Sender.subscriptions(track) -> int
    Sender.has_subscriber(track) -> bool
    Sender.has_media_subscriber() -> bool

These rows were written RED, against methods that did not exist, and each
failed by a NAMED missing behaviour rather than skipping. They exist now and
the rows pass. The gate is kept: a missing or inert name is still a named
failure, the substrate rows still drive the ACTUAL native mirrors on their
own, and the stand-in controls still prove these bodies are satisfiable and
that they REJECT each named defect.

Scope: three INDEPENDENT point-in-time queries. Not events, not callbacks, not
a cached counter, not an aggregate snapshot, not a readiness poll, not an
atomic multi-query transaction, and never a promise that a later write will
have demand.

The contract, re-derived from THIS worktree:

  service/include/moq/media_sender.h:685-707
      App-thread-safe snapshots that read mirrored counts under the sender's
      lock; they never call into the core publisher. All return 0/false for a
      NULL sender or a NULL/foreign track.
      * track_subscriptions: active subscriptions for the EXACT handle,
        including a generated track if a caller somehow holds one. Zero for an
        unknown or removed-AND-DRAINED track.
      * track_has_subscriber: exactly `track_subscriptions(...) > 0`.
      * has_media_subscriber: true if ANY app-visible media track has a
        subscriber. The internal catalog track and generated SAP/media-timeline
        tracks are excluded, as are removed tracks -- so a catalog-only
        subscription does NOT make it true.

  service/src/media_sender.c:4869-4905, what the code actually does
      track_subscriptions validates OWNERSHIP (including the private catalog
      track) and then returns `track->active_subs`. It does NOT test
      `track->removed`. So a removed-but-not-yet-drained track still reports
      its mirrored count; the header's "zero" applies once teardown has
      cleared it. This file pins that timing as the SOURCE has it and does not
      add a Python-side removed latch to force an immediate zero.
      has_media_subscriber skips removed and non-app-media tracks explicitly.

Proposed surface: `subscriptions` returns an ordinary nonnegative Python int
covering the whole native size_t domain -- no int32/uint32 narrowing and no
artificial 62-bit ceiling. The boolean methods return real bools. The
binding's existing owner-thread, PID and live-owner rules still apply even
though the C getters are thread-safe: the native fallback of 0/false for a
NULL or foreign handle is NOT permission for this API to accept a foreign,
mistyped or destroyed one, and the bridge keeps those guards itself.
"""

import gc
import os
import threading
import unittest
from unittest import mock

import moq5
from moq5 import _native

import test_foundation as foundation  # noqa: F401
from test_sender import reap_child

NS = (b"svc", b"demo")

SURFACE = ("subscriptions", "has_subscriber", "has_media_subscriber")
BRIDGE = ("sender_subscriptions", "sender_has_subscriber",
          "sender_has_media_subscriber")

LIMITS = None          # filled at first use from the compiled toolchain


def limits():
    global LIMITS
    if LIMITS is None:
        LIMITS = _native._test_size_limits()
    return LIMITS


def width_cases():
    """Zero, one, above 2^32 when the host can hold it, and the native max.

    Derived from the compiled size_t, never assumed: a 32-bit host simply has
    no above-2^32 case, and that is reported rather than skipped silently.
    """
    facts = limits()
    cases = [("zero", 0), ("one", 1), ("native maximum", facts["size_t_max"])]
    if facts["size_t_max"] > (1 << 32):
        cases.insert(2, ("above 2^32", (1 << 32) + 7))
    return cases


def missing_surface() -> list[str]:
    return [name for name in SURFACE
            if not callable(getattr(moq5.Sender, name, None))]


class DemandFixtureBase(unittest.TestCase):
    def setUp(self):
        gc.collect()
        _native._test_reset()
        _native._test_sender_reset()
        _native._test_send_track_reset()
        _native._test_demand_reset()

    def tearDown(self):
        _native._test_demand_reset()

    def connect(self):
        return moq5.Endpoint.connect(moq5.EndpointConfig(url="moqt://fixture.invalid"))

    def attach(self, endpoint):
        return moq5.Sender.attach(
            endpoint,
            moq5.SenderConfig(namespace=NS,
                              backpressure=moq5.Backpressure.DROP_GROUP))

    def track(self, sender, **fields):
        fields.setdefault("name", b"v")
        fields.setdefault("media_type", moq5.MediaType.VIDEO)
        fields.setdefault("packaging", moq5.Packaging.RAW)
        fields.setdefault("codec", b"av01")
        fields.setdefault("bitrate", 1_500_000)
        return sender.add_track(moq5.SendTrackConfig(**fields))


# ---------------------------------------------------------------- substrate --

class DemandSubstrateTests(DemandFixtureBase):
    """The native mirrors, driven directly, without the bridge."""

    def test_a_an_owned_track_reports_its_mirrored_count(self):
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                self.track(sender, name=b"one")
                for name, value in width_cases():
                    with self.subTest(case=name):
                        _native._test_demand_set(0, value)
                        self.assertEqual(
                            _native._test_demand_simulate("subscriptions", 0),
                            value, "the mirrored count was not preserved")
                        self.assertIs(
                            _native._test_demand_simulate("has_subscriber", 0),
                            value > 0,
                            "has_subscriber is not exactly count > 0")
            finally:
                sender.close()

    def test_b_a_foreign_or_null_handle_reports_zero(self):
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                self.track(sender)
                _native._test_demand_set(0, 5)
                self.assertEqual(
                    _native._test_demand_simulate("subscriptions", 0, 1), 0,
                    "a foreign owner was served a count")
                self.assertEqual(
                    _native._test_demand_simulate("subscriptions", -1), 0,
                    "a NULL track was served a count")
                self.assertEqual(
                    _native._test_demand_simulate("subscriptions", 0, 2), 0,
                    "a NULL sender was served a count")
                self.assertIs(
                    _native._test_demand_simulate("has_media_subscriber", -1, 2),
                    False, "a NULL sender reported media demand")
            finally:
                sender.close()

    def test_c_a_removed_track_still_reports_until_it_drains(self):
        # SOURCE behaviour, not a wish: track_subscriptions validates
        # ownership and returns active_subs WITHOUT testing `removed`. Zero
        # arrives when teardown has cleared the mirror, which this fixture
        # models by the count itself going to zero.
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                track = self.track(sender)
                _native._test_demand_set(0, 3)
                sender.remove_track(track)
                self.assertEqual(
                    _native._test_demand_simulate("subscriptions", 0), 3,
                    "a removed-but-not-drained track was forced to zero")
                # the aggregate SKIPS removed tracks immediately, which is a
                # different rule in the same source
                self.assertIs(
                    _native._test_demand_simulate("has_media_subscriber"),
                    False, "the aggregate counted a removed track")
                _native._test_demand_set(0, 0)          # drained
                self.assertEqual(
                    _native._test_demand_simulate("subscriptions", 0), 0)
            finally:
                sender.close()

    def test_d_the_aggregate_ignores_generated_and_catalog_tracks(self):
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                self.track(sender, name=b"media")
                self.track(sender, name=b"gen")
                _native._test_demand_set(1, 4, 1)       # generated, subscribed
                self.assertIs(
                    _native._test_demand_simulate("has_media_subscriber"),
                    False,
                    "a catalog-or-generated-only subscription counted as "
                    "media demand")
                # the exact handle still reports its own count
                self.assertEqual(
                    _native._test_demand_simulate("subscriptions", 1), 4,
                    "the exact generated handle lost its mirrored count")
                _native._test_demand_set(0, 1)          # real media demand
                self.assertIs(
                    _native._test_demand_simulate("has_media_subscriber"),
                    True, "real media demand was not reported")
            finally:
                sender.close()

    def test_e_each_query_is_recorded_with_its_target(self):
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                self.track(sender, name=b"one")
                self.track(sender, name=b"two")
                _native._test_demand_set(1, 2)
                _native._test_demand_simulate("subscriptions", 1)
                counts = _native._test_demand_counts()
                self.assertEqual(counts["subscriptions"], 1)
                self.assertEqual(counts["last_track"], 1,
                                 "the query was routed to the wrong track")
                self.assertEqual(counts["last_sender"], 0)
                _native._test_demand_simulate("has_subscriber", 0)
                counts = _native._test_demand_counts()
                self.assertEqual(counts["has_subscriber"], 1)
                self.assertEqual(counts["last_track"], 0)
                _native._test_demand_simulate("has_media_subscriber")
                self.assertEqual(
                    _native._test_demand_counts()["has_media_subscriber"], 1)
            finally:
                sender.close()

    def test_f_reset_clears_the_script_and_the_inventory(self):
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                self.track(sender)
                _native._test_demand_set(0, 9)
                _native._test_demand_simulate("subscriptions", 0)
                self.assertGreater(
                    _native._test_demand_counts()["subscriptions"], 0)
                _native._test_demand_reset()
                self.assertEqual(_native._test_demand_counts(),
                                 {"subscriptions": 0, "has_subscriber": 0,
                                  "has_media_subscriber": 0,
                                  "last_track": -1, "last_sender": -1})
                self.assertEqual(
                    _native._test_demand_simulate("subscriptions", 0), 0,
                    "the scripted count survived reset")
            finally:
                sender.close()


# ------------------------------------------------------------- behavioural --

class DemandGatedBase(DemandFixtureBase):
    """Never skips. A missing or inert surface is a NAMED failure, asserted
    before the row acquires an endpoint."""

    def require_surface(self):
        # Kept after GREEN: an inert or removed name must still fail by name.
        missing = missing_surface()
        if missing:
            self.fail("the sender demand surface is not implemented: "
                      f"moq5.Sender is missing {', '.join(missing)}")

    def operations(self):
        return tuple(op for op in _native._test_sender_log()
                     if op in ("write", "end_track", "remove_track",
                               "sender_destroy", "add_track"))


class DemandValueTests(DemandGatedBase):
    def test_a_the_whole_native_width_survives(self):
        self.require_surface()
        facts = limits()
        self.assertGreaterEqual(facts["size_t_width"], 4,
                                "an implausible size_t width was reported")
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                track = self.track(sender)
                for name, value in width_cases():
                    with self.subTest(case=name, value=value):
                        _native._test_demand_set(0, value)
                        observed = sender.subscriptions(track)
                        self.assertEqual(observed, value,
                                         "the count was narrowed or clamped")
                        self.assertIs(type(observed), int,
                                      "subscriptions is not an ordinary int")
                        self.assertGreaterEqual(observed, 0,
                                                "a size_t was read as signed")
                        self.assertIs(sender.has_subscriber(track), value > 0,
                                      "has_subscriber is not exactly count > 0")
                        self.assertIs(type(sender.has_subscriber(track)), bool,
                                      "has_subscriber is not a real bool")
            finally:
                sender.close()

    def test_b_the_declared_track_is_the_one_queried(self):
        self.require_surface()
        # Two valid tracks of the SAME sender with DIFFERENT observations, and
        # both are queried: no first-target cache can satisfy this row.
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                first = self.track(sender, name=b"one")
                second = self.track(sender, name=b"two")
                _native._test_demand_set(0, 1)
                _native._test_demand_set(1, 7)
                self.assertEqual(sender.subscriptions(second), 7)
                self.assertEqual(_native._test_demand_counts()["last_track"], 1,
                                 "the query was routed to the wrong track")
                self.assertEqual(sender.subscriptions(first), 1)
                self.assertEqual(_native._test_demand_counts()["last_track"], 0,
                                 "a first-target cache answered")
                # the convenience query on BOTH tracks, with DISTINCT
                # observations, each recorded against its own target
                _native._test_demand_set(0, 0)
                _native._test_demand_set(1, 7)
                self.assertIs(sender.has_subscriber(first), False,
                              "an unsubscribed track reported demand")
                self.assertEqual(_native._test_demand_counts()["last_track"], 0,
                                 "has_subscriber was routed to the wrong track")
                self.assertIs(sender.has_subscriber(second), True)
                self.assertEqual(_native._test_demand_counts()["last_track"], 1,
                                 "has_subscriber was routed to the wrong track")
            finally:
                sender.close()

    def test_c_successive_calls_observe_the_current_value(self):
        self.require_surface()
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                track = self.track(sender)
                _native._test_demand_set(0, 2)
                self.assertEqual(sender.subscriptions(track), 2)
                _native._test_demand_set(0, 5)
                self.assertEqual(sender.subscriptions(track), 5,
                                 "a cached Python value was returned")
                _native._test_demand_set(0, 0)
                self.assertEqual(sender.subscriptions(track), 0)
                self.assertIs(sender.has_subscriber(track), False)
                counts = _native._test_demand_counts()
                self.assertEqual(counts["subscriptions"], 4,
                                 "one native query per call, and no more")
            finally:
                sender.close()

    def test_d_ready_and_demand_are_separate_facts(self):
        self.require_surface()
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                track = self.track(sender)
                _native._test_sender_state(True, False, 0)      # READY
                self.assertIs(sender.ready, True)
                _native._test_demand_set(0, 0)
                self.assertIs(sender.has_media_subscriber(), False,
                              "readiness was substituted for demand")
                self.assertEqual(sender.subscriptions(track), 0)
                # and demand without readiness
                _native._test_sender_state(False, False, 0)
                _native._test_demand_set(0, 3)
                self.assertIs(sender.ready, False)
                self.assertIs(sender.has_media_subscriber(), True,
                              "demand was gated on readiness")
            finally:
                sender.close()

    def test_e_catalog_or_generated_demand_is_not_media_demand(self):
        self.require_surface()
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                media = self.track(sender, name=b"media")
                generated = self.track(sender, name=b"gen")
                _native._test_demand_set(1, 4, 1)      # generated, subscribed
                self.assertIs(sender.has_media_subscriber(), False,
                              "a generated-only subscription counted as media")
                self.assertEqual(sender.subscriptions(generated), 4,
                                 "the exact generated handle was refused")
                _native._test_demand_set(0, 1)
                self.assertIs(sender.has_media_subscriber(), True)
                self.assertEqual(sender.subscriptions(media), 1)
            finally:
                sender.close()

    def test_f_a_removed_track_reports_what_the_service_reports(self):
        self.require_surface()
        # No Python removed latch: the binding reports the service's answer,
        # which for a removed-but-not-drained track is still its mirrored
        # count, and becomes zero once teardown clears it.
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                track = self.track(sender)
                _native._test_demand_set(0, 3)
                sender.remove_track(track)
                self.assertEqual(sender.subscriptions(track), 3,
                                 "the binding forced an immediate zero")
                self.assertIs(sender.has_subscriber(track), True)
                self.assertIs(sender.has_media_subscriber(), False,
                              "the aggregate counted a removed track")
                _native._test_demand_set(0, 0)          # drained
                self.assertEqual(sender.subscriptions(track), 0)
                self.assertIs(sender.has_subscriber(track), False)
            finally:
                sender.close()

    def test_g_the_aggregate_is_its_own_native_observation(self):
        self.require_surface()
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                self.track(sender, name=b"one")
                self.track(sender, name=b"two")
                _native._test_demand_set(1, 2)
                before = _native._test_demand_counts()
                self.assertIs(sender.has_media_subscriber(), True)
                counts = _native._test_demand_counts()
                self.assertEqual(counts["has_media_subscriber"],
                                 before["has_media_subscriber"] + 1,
                                 "the aggregate did not enter native")
                self.assertEqual(counts["subscriptions"],
                                 before["subscriptions"],
                                 "the aggregate was summed over wrappers in "
                                 "Python instead of being asked for")
            finally:
                sender.close()


class DemandGuardTests(DemandGatedBase):
    def test_a_wrong_types_and_a_receiver_track_are_refused(self):
        self.require_surface()
        endpoint = self.connect()
        sender = self.attach(endpoint)
        try:
            receiver = moq5.Receiver.attach(
                endpoint, moq5.ReceiverConfig.live(NS))
            try:
                receiver_track = receiver._track_from_capsule(
                    _native._test_new_track(
                        getattr(receiver, "_Receiver__handle")))
                before = _native._test_demand_counts()
                for value in (object(), None, receiver_track):
                    with self.subTest(value=type(value).__name__):
                        with self.assertRaises(TypeError):
                            sender.subscriptions(value)
                        with self.assertRaises(TypeError):
                            sender.has_subscriber(value)
                self.assertEqual(_native._test_demand_counts(), before,
                                 "a wrong-typed argument entered native")
            finally:
                receiver.close()
        finally:
            try:
                sender.close()
            finally:
                endpoint.close()

    def test_b_a_foreign_owners_track_is_refused_not_answered_zero(self):
        self.require_surface()
        # The native fallback of zero for a foreign handle is NOT permission
        # for this API to accept one.
        #
        # LIMITATION: the fixture serves one endpoint and one sender at a
        # time, so the two owners are SEQUENTIAL and the first is necessarily
        # closed by the time the second exists. This row pins refusal and zero
        # native entry, not which of the two reasons fired.
        endpoint = self.connect()
        first = self.attach(endpoint)
        foreign = self.track(first)
        first.close()
        endpoint.close()
        _native._test_reset()
        _native._test_sender_reset()
        _native._test_send_track_reset()
        _native._test_demand_reset()
        second_endpoint = self.connect()
        second = self.attach(second_endpoint)
        try:
            before = _native._test_demand_counts()
            with self.assertRaises(RuntimeError):
                second.subscriptions(foreign)
            with self.assertRaises(RuntimeError):
                second.has_subscriber(foreign)
            self.assertEqual(_native._test_demand_counts(), before,
                             "a foreign track entered native")
        finally:
            try:
                second.close()
            finally:
                second_endpoint.close()

    def test_c_a_closed_owner_is_refused_and_a_terminal_one_answers(self):
        self.require_surface()
        endpoint = self.connect()
        sender = self.attach(endpoint)
        try:
            track = self.track(sender)
            _native._test_demand_set(0, 2)
            # live but TERMINAL still answers: terminal is not closed
            _native._test_sender_state(True, True, 42)
            self.assertIs(sender.terminal.fatal, True)
            self.assertEqual(sender.subscriptions(track), 2,
                             "a terminal sender refused a demand query")
            self.assertIs(sender.has_media_subscriber(), True)
            sender.close()
            before = _native._test_demand_counts()
            for call in (lambda: sender.subscriptions(track),
                         lambda: sender.has_subscriber(track),
                         sender.has_media_subscriber):
                with self.assertRaises(RuntimeError):
                    call()
            self.assertEqual(_native._test_demand_counts(), before,
                             "a closed owner entered native")
        finally:
            try:
                sender.close()
            finally:
                endpoint.close()

    def test_d_the_owner_thread_and_pid_guards_precede_native_entry(self):
        self.require_surface()
        # The C getters are thread-safe, but this binding's contract is not
        # broadened silently: the owner thread rule still applies.
        endpoint = self.connect()
        sender = self.attach(endpoint)
        try:
            track = self.track(sender)
            _native._test_demand_set(0, 1)
            before = _native._test_demand_counts()
            refused = []

            def other():
                for call in (lambda: sender.subscriptions(track),
                             lambda: sender.has_subscriber(track),
                             sender.has_media_subscriber):
                    try:
                        call()
                    except RuntimeError as error:
                        refused.append(error)

            worker = threading.Thread(target=other, daemon=True)
            worker.start()
            worker.join(5.0)
            self.assertFalse(worker.is_alive(), "the guard worker never returned")
            self.assertEqual(len(refused), 3, "a foreign thread was admitted")
            self.assertEqual(_native._test_demand_counts(), before,
                             "a foreign thread entered native")

            if hasattr(os, "fork"):
                child = os.fork()
                if child == 0:
                    status = 0
                    for index, call in enumerate(
                            (lambda: sender.subscriptions(track),
                             lambda: sender.has_subscriber(track),
                             sender.has_media_subscriber)):
                        try:
                            call()
                            status = 3 + index   # the inherited handle worked
                            break
                        except RuntimeError:
                            continue
                        except BaseException:
                            status = 9
                            break
                    if status == 0 and _native._test_demand_counts() != before:
                        status = 8
                    os._exit(status)
                self.assertEqual(reap_child(child), 0,
                                 "the fork guard child did not exit cleanly")
        finally:
            try:
                sender.close()
            finally:
                endpoint.close()

    def test_e_the_private_bridge_refuses_wrong_capsules(self):
        self.require_surface()
        missing = tuple(n for n in BRIDGE if not hasattr(_native, n))
        self.assertEqual(missing, (),
                         "the demand bridge is not implemented: _native is "
                         f"missing {', '.join(missing)}")
        endpoint = self.connect()
        sender = self.attach(endpoint)
        self.addCleanup(endpoint.close)
        self.addCleanup(sender.close)
        track = self.track(sender)
        sender_handle = getattr(sender, "_Sender__handle")
        track_handle = getattr(track, "_SendTrack__handle")
        endpoint_handle = getattr(endpoint, "_Endpoint__handle")
        before = _native._test_demand_counts()
        with self.assertRaises((ValueError, TypeError)):
            _native.sender_subscriptions(sender_handle, endpoint_handle)
        with self.assertRaises((ValueError, TypeError)):
            _native.sender_subscriptions(endpoint_handle, track_handle)
        with self.assertRaises((ValueError, TypeError)):
            _native.sender_has_subscriber(sender_handle, endpoint_handle)
        with self.assertRaises((ValueError, TypeError)):
            _native.sender_has_subscriber(endpoint_handle, track_handle)
        with self.assertRaises((ValueError, TypeError)):
            _native.sender_has_media_subscriber(track_handle)
        with self.assertRaises(TypeError):
            _native.sender_subscriptions(sender_handle)
        with self.assertRaises(TypeError):
            _native.sender_has_subscriber(sender_handle)
        self.assertEqual(_native._test_demand_counts(), before,
                         "a wrong capsule reached the service")


class DemandEffectTests(DemandGatedBase):
    def test_a_a_query_has_no_lifecycle_effect(self):
        self.require_surface()
        endpoint = self.connect()
        sender = self.attach(endpoint)
        try:
            track = self.track(sender)
            _native._test_demand_set(0, 1)
            before = _native._test_sender_counts()
            baseline = self.operations()
            sender.subscriptions(track)
            sender.has_subscriber(track)
            sender.has_media_subscriber()
            self.assertEqual(self.operations(), baseline,
                             "a demand query performed a lifecycle operation")
            self.assertEqual(_native._test_sender_counts()["destroy_entries"],
                             before["destroy_entries"])
            self.assertIs(sender.closed, False)
            self.assertIs(endpoint.closed, False)
            self.assertIs(track.removed, False)
            counts = _native._test_demand_counts()
            self.assertEqual(counts["subscriptions"], 2,
                             "the count query did not run exactly once per call")
            self.assertEqual(counts["has_subscriber"], 1)
            self.assertEqual(counts["has_media_subscriber"], 1)
        finally:
            try:
                sender.close()
            finally:
                endpoint.close()

    def test_b_the_queries_do_not_promise_a_later_write(self):
        self.require_surface()
        # Demand now is not demand later, and it is certainly not an
        # acceptance guarantee: the write result is the service's own answer.
        endpoint = self.connect()
        sender = self.attach(endpoint)
        try:
            track = self.track(sender)
            _native._test_demand_set(0, 4)
            self.assertIs(sender.has_subscriber(track), True)
            _native._test_write_result(-8)                 # WOULD_BLOCK
            try:
                self.assertIs(sender.write(track, moq5.SendObject(payload=b"f")),
                              moq5.WriteOutcome.WOULD_BLOCK,
                              "demand was allowed to override the write result")
            finally:
                _native._test_write_result(0)
        finally:
            try:
                sender.close()
            finally:
                endpoint.close()


# ------------------------------------------------------------------ control --

PUBLIC_GATED_CLASSES = ("DemandValueTests", "DemandGuardTests",
                        "DemandEffectTests", "DemandConstructionTests")


class DemandConstructionTests(DemandGatedBase):
    """The one result-construction boundary that actually exists."""

    def test_a_a_count_construction_failure_leaves_the_owner_usable(self):
        self.require_surface()
        endpoint = self.connect()
        sender = self.attach(endpoint)
        try:
            track = self.track(sender)
            _native._test_demand_set(0, 4)
            before = _native._test_demand_counts()
            try:
                _native._test_fail_allocation_at("sender_subscriptions_result")
                with self.assertRaises(MemoryError):
                    sender.subscriptions(track)
            finally:
                _native._test_fail_allocation_at(None)
            self.assertTrue(
                _native._test_allocation_site_fired(
                    "sender_subscriptions_result"),
                "the named count-result fault never fired")
            self.assertEqual(_native._test_demand_counts()["subscriptions"],
                             before["subscriptions"] + 1,
                             "the query did not run before the failure")
            # no lifecycle effect, and the same live owner still answers
            self.assertEqual(self.operations(), ("add_track",),
                             "a failed construction had a lifecycle effect")
            self.assertIs(sender.closed, False)
            self.assertEqual(sender.subscriptions(track), 4,
                             "the owner was left unusable")
            self.assertIs(sender.has_subscriber(track), True)
            self.assertIs(sender.has_media_subscriber(), True)
        finally:
            try:
                sender.close()
            finally:
                endpoint.close()

    def test_b_the_boolean_queries_need_no_allocation_boundary(self):
        self.require_surface()
        # True and False are singletons: there is no construction to fail, and
        # no fabricated allocation was invented to pretend otherwise.
        endpoint = self.connect()
        sender = self.attach(endpoint)
        try:
            track = self.track(sender)
            _native._test_demand_set(0, 1)
            self.assertIs(sender.has_subscriber(track), True)
            self.assertIs(sender.has_media_subscriber(), True)
            _native._test_demand_set(0, 0)
            self.assertIs(sender.has_subscriber(track), False)
            self.assertIs(sender.has_media_subscriber(), False)
        finally:
            try:
                sender.close()
            finally:
                endpoint.close()


class DemandControlTests(unittest.TestCase):
    """This file's own machinery. Never evidence about the product.

    The inert-name control establishes the GATE: every gated row is invoked
    and refuses by a named assertion. It does NOT execute the bodies -- each
    stops inside require_surface. The stand-in control runs those bodies with
    ordinary Python types, and the discriminator control proves the
    behavioural rows reject a wrong target, a narrowed count, a stale result
    and readiness substituted for demand.
    """

    def test_a_inert_names_are_refused_by_every_gated_row(self):
        import test_send_demand as module

        saved = {name: getattr(moq5.Sender, name, None) for name in SURFACE}
        try:
            for name in SURFACE:
                setattr(moq5.Sender, name, object())
            loader = unittest.defaultTestLoader
            suite = unittest.TestSuite(
                loader.loadTestsFromTestCase(getattr(module, c))
                for c in PUBLIC_GATED_CLASSES)
            result = unittest.TestResult()
            suite.run(result)
        finally:
            for name, value in saved.items():
                if value is None:
                    if hasattr(moq5.Sender, name):
                        delattr(moq5.Sender, name)
                else:
                    setattr(moq5.Sender, name, value)
        self.assertGreater(result.testsRun, 0)
        self.assertEqual(result.skipped, [], "a gated row skipped")
        self.assertEqual(result.errors, [],
                         "a gated row raised an arbitrary error instead of "
                         "refusing by a named assertion")
        failed = {getattr(case, "test_case", case).id()
                  for case, _ in result.failures}
        self.assertEqual(len(failed), result.testsRun,
                         "an inert surface satisfied a behavioural row")

    @staticmethod
    def narrowing_mask():
        """A mask strictly NARROWER than the compiled size_t domain.

        `& 0xFFFFFFFF` is the identity where size_t is 32 bits, so the generic
        discriminator uses half the compiled width instead: it is a real
        narrowing on every host. A uint32-specific PRODUCT mutant is only
        meaningful where the real size_t is wider, and is reported as such.
        """
        bits = limits()["size_t_width"] * 8
        return (1 << (bits // 2)) - 1

    @staticmethod
    def stand_in(*, narrow=False, cache=False, first_target=False,
                 ready_for_demand=False):
        """This file's adapter over the real native mirrors.

        Perturbations are OPT-IN, so the same adapter serves the positive
        control and each discriminator.
        """
        remembered = {}
        first = {}

        def index_of(sender, track):
            if not isinstance(track, moq5.SendTrack):
                raise TypeError("track must be a SendTrack")
            if track.sender is not sender:
                raise RuntimeError("this track belongs to another sender")
            if sender.closed:
                raise RuntimeError("sender is closed")
            if first_target:
                return first.setdefault(id(sender),
                                        _native._test_send_track_index_for(
                                            track.name, 1))
            return _native._test_send_track_index_for(track.name, 1)

        def subscriptions(sender, track):
            index = index_of(sender, track)
            if cache and index in remembered:
                return remembered[index]
            value = _native._test_demand_simulate("subscriptions", index)
            if narrow:
                value &= DemandControlTests.narrowing_mask()
            remembered[index] = value
            return value

        def has_subscriber(sender, track):
            # routed to the NATIVE convenience query, as the bridge must be:
            # computing it in Python would change which native call runs
            index = index_of(sender, track)
            if cache and index in remembered:
                return remembered[index] > 0
            value = _native._test_demand_simulate("has_subscriber", index)
            if narrow and not value:
                return False
            return value

        def has_media_subscriber(sender):
            if sender.closed:
                raise RuntimeError("sender is closed")
            if ready_for_demand:
                return bool(sender.ready)
            return _native._test_demand_simulate("has_media_subscriber")

        return {"subscriptions": subscriptions,
                "has_subscriber": has_subscriber,
                "has_media_subscriber": has_media_subscriber}

    def run_bodies(self, adapter, excluded):
        import test_send_demand as module

        saved = {name: getattr(moq5.Sender, name, None) for name in SURFACE}
        try:
            for name, function in adapter.items():
                setattr(moq5.Sender, name, function)
            loader = unittest.defaultTestLoader
            names = []
            for class_name in PUBLIC_GATED_CLASSES:
                case_class = getattr(module, class_name)
                for test in loader.getTestCaseNames(case_class):
                    if f"{class_name}.{test}" in excluded:
                        continue
                    names.append(f"{class_name}.{test}")
            suite = loader.loadTestsFromNames(names, module)
            result = unittest.TestResult()
            suite.run(result)
        finally:
            for name, value in saved.items():
                if value is None:
                    if hasattr(moq5.Sender, name):
                        delattr(moq5.Sender, name)
                else:
                    setattr(moq5.Sender, name, value)
        return result

    # Rows about the NATIVE entry point, which a Python stand-in does not
    # have. They execute against the real bridge once it exists.
    EXCLUDED = {
        "DemandGuardTests.test_e_the_private_bridge_refuses_wrong_capsules",
        "DemandGuardTests.test_d_the_owner_thread_and_pid_guards_precede_native_entry",
        "DemandGuardTests.test_b_a_foreign_owners_track_is_refused_not_answered_zero",
        "DemandConstructionTests.test_a_a_count_construction_failure_leaves_the_owner_usable",
    }

    def expected_body_count(self):
        import test_send_demand as module

        loader = unittest.defaultTestLoader
        total = 0
        for class_name in PUBLIC_GATED_CLASSES:
            for test in loader.getTestCaseNames(getattr(module, class_name)):
                if f"{class_name}.{test}" not in self.EXCLUDED:
                    total += 1
        return total

    def assert_semantic_rejection(self, result, defect, expected_rows):
        """A defect must be caught by a NAMED assertion in the rows that own
        it -- not by an arbitrary exception, a skip, or a body that never ran.

        This is the validator the error-only control below is aimed at, and it
        is the same one every perturbation goes through.
        """
        self.assertEqual(result.testsRun, self.expected_body_count(),
                         f"the bodies did not all run for {defect}")
        self.assertEqual(
            [f"{case.id()}: {text}" for case, text in result.errors], [],
            f"{defect} raised an arbitrary error instead of failing an "
            "assertion")
        self.assertEqual(result.skipped, [], f"a body skipped for {defect}")
        # a subtest's id carries its parameters; the ROW is the part before
        failed = {case.id().rsplit(".", 1)[-1].split(" (", 1)[0]
                  for case, _ in result.failures}
        self.assertNotEqual(failed, set(), f"the rows accepted {defect}")
        self.assertTrue(
            failed & expected_rows,
            f"{defect} was caught, but not by {sorted(expected_rows)}; "
            f"the failing rows were {sorted(failed)}")

    def test_b_the_gated_bodies_pass_against_a_scoped_stand_in(self):
        result = self.run_bodies(self.stand_in(), self.EXCLUDED)
        self.assertEqual(
            [f"{case.id()}: {error}" for case, error in
             result.failures + result.errors], [],
            "a gated body failed against the declared contract")
        self.assertEqual(result.skipped, [], "a gated body skipped")
        self.assertEqual(result.testsRun, self.expected_body_count(),
                         "not every intended body ran")

    def test_c_the_bodies_reject_each_named_defect(self):
        for name, adapter, rows in (
                ("a narrowed count", self.stand_in(narrow=True),
                 {"test_a_the_whole_native_width_survives"}),
                ("a stale cached value", self.stand_in(cache=True),
                 {"test_c_successive_calls_observe_the_current_value"}),
                ("a first-target cache", self.stand_in(first_target=True),
                 {"test_b_the_declared_track_is_the_one_queried"}),
                ("readiness substituted for demand",
                 self.stand_in(ready_for_demand=True),
                 {"test_d_ready_and_demand_are_separate_facts",
                  "test_e_catalog_or_generated_demand_is_not_media_demand",
                  "test_g_the_aggregate_is_its_own_native_observation"})):
            with self.subTest(defect=name):
                result = self.run_bodies(adapter, self.EXCLUDED)
                self.assert_semantic_rejection(result, name, rows)

    def test_c2_an_arbitrary_exception_is_not_a_semantic_kill(self):
        """The validator itself, aimed at the case that fooled the old one.

        A stand-in whose methods only raise an unrelated exception makes every
        body ERROR. That is not a discriminator, and the SAME validator the
        perturbations use must refuse it.
        """
        def boom(*args, **kwargs):
            raise LookupError("unrelated")

        result = self.run_bodies(
            {"subscriptions": boom, "has_subscriber": boom,
             "has_media_subscriber": boom}, self.EXCLUDED)
        self.assertGreater(len(result.errors), 0,
                           "the errors-only stand-in did not error")
        with self.assertRaises(AssertionError):
            self.assert_semantic_rejection(
                result, "an unrelated exception",
                {"test_a_the_whole_native_width_survives"})

    def test_c3_the_narrowing_mask_is_a_real_narrowing_on_either_width(self):
        """Qualified under BOTH compiled-width facts.

        These are test-logic simulations of the mask's arithmetic, not a
        cross-build: the 32-bit row substitutes the declared limits and checks
        that the mask still narrows the declared maximum.
        """
        global LIMITS
        saved = LIMITS
        try:
            for width, maximum in ((8, (1 << 64) - 1), (4, (1 << 32) - 1)):
                with self.subTest(size_t_width=width):
                    LIMITS = {"size_t_width": width, "size_t_max": maximum}
                    mask = self.narrowing_mask()
                    self.assertLess(mask, maximum,
                                    "the mask is not narrower than the domain")
                    self.assertNotEqual(maximum & mask, maximum,
                                        "the mask is the identity on the "
                                        "declared maximum")
        finally:
            LIMITS = saved
        # and on THIS host the mask really is derived from the compiled width
        self.assertEqual(self.narrowing_mask(),
                         (1 << (limits()["size_t_width"] * 8 // 2)) - 1)

    def test_d_the_bridge_is_present_and_refuses_wrong_arguments(self):
        # This row was the RED gate's "the bridge is absent" assertion. The
        # bridge exists now, so it pins presence, callability and that a
        # wrong-shaped call is refused rather than crashing.
        missing = tuple(n for n in BRIDGE if not hasattr(_native, n))
        self.assertEqual(missing, (), f"the demand bridge lost {missing}")
        for name in BRIDGE:
            self.assertTrue(callable(getattr(_native, name)),
                            f"_native.{name} is not callable")
        before = _native._test_demand_counts()
        for name, args in (("sender_subscriptions", ()),
                           ("sender_subscriptions", (object(),)),
                           ("sender_subscriptions", (object(), object())),
                           ("sender_has_subscriber", ()),
                           ("sender_has_subscriber", (object(), object())),
                           ("sender_has_media_subscriber", ()),
                           ("sender_has_media_subscriber", (object(),))):
            with self.subTest(entry=name, arity=len(args)):
                with self.assertRaises((TypeError, ValueError)):
                    getattr(_native, name)(*args)
        self.assertEqual(_native._test_demand_counts(), before,
                         "a wrong-shaped call reached the service")
