"""Per-track ending: Sender.end_track(track) -> WriteOutcome.

These rows were written RED, against a method that did not exist, and each
failed by a NAMED missing behaviour rather than skipping. The method exists
now and they pass. The gate that made that meaningful is kept: a missing or
inert name is still a named failure, the substrate rows still execute the
recorder's oracles on their own, and the scoped-stand-in control still proves
this file's bodies are satisfiable independently of the product.

Scope: ONE asynchronous per-track termination request. Not a delivered
receipt, not broadcast completion, not sender destruction and not endpoint
teardown. No stats, demand, wait, request_complete, drain or graceful finish.

The contract, re-derived from THIS worktree rather than from a sketch:

  service/include/moq/media_sender.h:499-510
      After the track's queued objects drain the service emits a reliable
      END_OF_TRACK status object, which receivers see as MEDIA_TRACK_ENDED.
      Ending one track does NOT end other tracks or close the session or the
      endpoint. After end_track, write() on that track returns WRONG_STATE.
      end_track is IDEMPOTENT: a second call returns OK and does nothing.
      Documented results: OK, INVAL (NULL or foreign track), CLOSED
      (terminal), INTERRUPTED (latch set), WOULD_BLOCK (queue momentarily
      full; retry).

  service/src/media_sender.c:4594-4646, the ORDER the checks actually run
      1. NULL sender or track            -> INVAL
      2. endpoint interrupt latch        -> INTERRUPTED
      3. sender terminal, or fatal       -> CLOSED
      4. not one of this sender's tracks -> INVAL
      5. a REMOVED track                 -> WRONG_STATE
      6. already requested               -> OK, and nothing else happens
      7. no slot against active_obj_cap  -> WOULD_BLOCK, nothing latched
      otherwise the marker is queued, end_requested is set, the group is
      closed, and a ready sender is woken.

Two things that follow from the ORDER, and are pinned as such:

  * Terminal beats idempotence. Steps 2 and 3 run BEFORE step 6, so a repeat
    call after the sender terminalizes is CLOSED, not a courtesy OK. This file
    does not promise unconditional success for repeat calls.
  * WRONG_STATE for a removed track is in the implementation but NOT in the
    header's documented result list. It is reported as a divergence rather
    than silently canonised: the binding passes it through as a MoqError with
    its signed code, exactly as it does for any other unlisted result.

WriteOutcome is REUSED, not re-declared: ACCEPTED, WOULD_BLOCK, INTERRUPTED,
CLOSED. Anything else is a MoqError with its signed code and the operation
"end_track".

What the binding must NOT do: retry, loop, queue, latch an "ended" flag in
Python, remove the track, destroy anything, add a registry or a per-track
destructor. A caller's retry after WOULD_BLOCK is the caller's own second
call. Dropping a wrapper still ends nothing.

A scripted fake proves the BINDING's mapping, identity and guards. It does not
prove the service emits END_OF_TRACK on the wire: that is the native suite's,
where service/tests/test_media_sender.c drives the public end_track over a
real loopback peer.
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

WOULD_BLOCK = -8
INTERRUPTED = -13
CLOSED = -4
INVAL = -2
WRONG_STATE = -5
PROTO = -3
# A representable negative the binding has never heard of: "unknown code is
# preserved" means nothing if every code tested is one the enum already knows.
UNKNOWN_CODE = -12345

SURFACE = ("end_track",)
BRIDGE = ("sender_end_track",)


def missing_surface() -> list[str]:
    return [name for name in SURFACE if not callable(getattr(moq5.Sender, name, None))]


class EndFixtureBase(unittest.TestCase):
    def setUp(self):
        gc.collect()
        _native._test_reset()
        _native._test_sender_reset()
        _native._test_send_track_reset()
        _native._test_write_reset()
        _native._test_end_reset()

    def tearDown(self):
        _native._test_end_result(0)
        _native._test_write_result(0)
        _native._test_fail_allocation_at(None)
        _native._test_end_reset()
        _native._test_write_reset()

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

class EndSubstrateTests(EndFixtureBase):
    """The end recorder and its seams, exercised without the bridge."""

    def test_a_reset_reports_an_empty_inventory(self):
        self.assertEqual(_native._test_end_counts(),
                         {"entries": 0, "ended": 0, "seen": False})
        self.assertIsNone(_native._test_end_target())

    def test_b_a_driven_end_is_captured_with_its_exact_target(self):
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                self.track(sender, name=b"one")
                self.track(sender, name=b"two")
                self.assertEqual(_native._test_end_simulate(1), 0)
                self.assertEqual(_native._test_end_counts(),
                                 {"entries": 1, "ended": 1, "seen": True})
                self.assertEqual(_native._test_end_target(),
                                 {"track_index": 1, "sender_index": 0,
                                  "track_null": False, "sender_null": False})
                self.assertIs(_native._test_end_requested(1), True)
                self.assertIs(_native._test_end_requested(0), False,
                              "ending one track ended another")
            finally:
                sender.close()

    def test_c_a_repeat_is_idempotent_and_still_one_call_each(self):
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                self.track(sender)
                self.assertEqual(_native._test_end_simulate(0), 0)
                self.assertEqual(_native._test_end_simulate(0), 0)
                counts = _native._test_end_counts()
                self.assertEqual(counts["entries"], 2,
                                 "a repeat did not reach the service")
                self.assertEqual(counts["ended"], 1,
                                 "the repeat queued a second terminal marker")
            finally:
                sender.close()

    def test_d_a_scripted_refusal_latches_nothing(self):
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                self.track(sender)
                for code in (WOULD_BLOCK, INTERRUPTED, CLOSED, PROTO):
                    with self.subTest(code=code):
                        _native._test_end_result(code)
                        try:
                            self.assertEqual(_native._test_end_simulate(0), code)
                        finally:
                            _native._test_end_result(0)
                        self.assertIs(
                            _native._test_end_requested(0), False,
                            "a refusal latched an end request anyway")
                # after the refusals, an ordinary end still succeeds
                self.assertEqual(_native._test_end_simulate(0), 0)
                self.assertIs(_native._test_end_requested(0), True)
            finally:
                sender.close()

    def test_e_a_foreign_owner_and_a_removed_track_are_distinguished(self):
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                track = self.track(sender)
                self.assertEqual(_native._test_end_simulate(0, 1), INVAL,
                                 "a foreign owner was accepted")
                sender.remove_track(track)
                self.assertEqual(_native._test_end_simulate(0), WRONG_STATE,
                                 "a removed track was ended")
                self.assertIs(_native._test_end_requested(0), False)
            finally:
                sender.close()

    def test_e2_the_in_call_barrier_is_a_real_handshake(self):
        # The end recorder parks on the SAME bounded gate the write rows use,
        # so the signal row's arrangement is proved before it depends on it.
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                self.track(sender)
                reached, released, result = [], [], []

                def driver():
                    result.append(_native._test_end_simulate(0))

                _native._test_end_block(True)
                worker = threading.Thread(target=driver, daemon=True)
                worker.start()
                try:
                    reached.append(_native._test_write_wait_entered(5.0))
                    released.append(_native._test_write_release())
                finally:
                    _native._test_end_block(False)
                    _native._test_write_release()
                    worker.join(10.0)
                self.assertFalse(worker.is_alive(), "the driver never returned")
                self.assertEqual(reached, [True],
                                 "the end call never reached the barrier")
                self.assertEqual(released, [True],
                                 "the fixture's own bound expired: harness failure")
                self.assertEqual(result, [0])
                self.assertIs(_native._test_end_requested(0), True)
            finally:
                sender.close()

    def test_f_a_null_track_is_recorded_as_such(self):
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                self.assertEqual(_native._test_end_simulate(-1), INVAL)
                target = _native._test_end_target()
                self.assertIs(target["track_null"], True)
                self.assertEqual(target["track_index"], -1)
            finally:
                sender.close()


# ------------------------------------------------------------- behavioural --

class EndGatedBase(EndFixtureBase):
    """Never skips. A missing or inert surface is a NAMED failure, asserted
    before the row acquires an endpoint."""

    def require_surface(self):
        # Kept after GREEN: an inert or removed name must still fail by name,
        # which is what the inert-surface control exercises.
        missing = missing_surface()
        if missing:
            self.fail("the per-track end surface is not implemented: "
                      f"moq5.Sender is missing {', '.join(missing)}")
        self.assertTrue(hasattr(moq5, "WriteOutcome"),
                        "WriteOutcome is missing")

    def operations(self):
        return tuple(op for op in _native._test_sender_log()
                     if op in ("add_track", "remove_track", "end_track",
                               "sender_destroy", "write"))


class EndSurfaceTests(EndGatedBase):
    def test_a_the_declared_track_is_the_one_the_service_receives(self):
        self.require_surface()
        # Two valid tracks of the SAME sender: only the recorded identity
        # distinguishes them, so a bridge that ends the wrong valid track
        # cannot satisfy this row.
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                first = self.track(sender, name=b"one")
                second = self.track(sender, name=b"two")
                self.assertIs(sender.end_track(second),
                              moq5.WriteOutcome.ACCEPTED)
                self.assertEqual(_native._test_end_target(),
                                 {"track_index": 1, "sender_index": 0,
                                  "track_null": False, "sender_null": False},
                                 "the end reached the wrong native target")
                self.assertIs(_native._test_end_requested(0), False,
                              "an unrelated track was ended")
                self.assertEqual(_native._test_end_counts()["entries"], 1,
                                 "one call per invocation")
                # and the OTHER track, so a bridge that remembers its first
                # target cannot satisfy this row by ending the same one twice
                self.assertIs(sender.end_track(first),
                              moq5.WriteOutcome.ACCEPTED)
                self.assertEqual(_native._test_end_target(),
                                 {"track_index": 0, "sender_index": 0,
                                  "track_null": False, "sender_null": False},
                                 "the second end reached the wrong native target")
                counts = _native._test_end_counts()
                self.assertEqual(counts["entries"], 2, "one call per invocation")
                self.assertEqual(counts["ended"], 2,
                                 "the second end did not queue its own marker")
                self.assertIs(_native._test_end_requested(0), True)
                self.assertIs(_native._test_end_requested(1), True)
            finally:
                sender.close()

    def test_b_ending_removes_nothing_and_closes_nothing(self):
        self.require_surface()
        endpoint = self.connect()
        sender = self.attach(endpoint)
        try:
            track = self.track(sender)
            before = _native._test_sender_counts()
            self.assertIs(sender.end_track(track), moq5.WriteOutcome.ACCEPTED)
            self.assertEqual(self.operations()[-1:], ("end_track",),
                             "the bridge performed another native operation")
            self.assertEqual(self.operations().count("remove_track"), 0,
                             "ending removed the track")
            self.assertEqual(self.operations().count("sender_destroy"), 0,
                             "ending destroyed the sender")
            self.assertEqual(_native._test_sender_counts()["destroy_entries"],
                             before["destroy_entries"])
            self.assertIs(sender.closed, False, "ending closed the sender")
            self.assertIs(endpoint.closed, False, "ending closed the endpoint")
            self.assertIs(track.removed, False,
                          "ending marked the track removed in Python")
            self.assertIs(track.sender, sender, "the track lost its owner")
            self.assertEqual(track.name, b"v", "the track lost its name")
        finally:
            try:
                sender.close()
            finally:
                endpoint.close()

    def test_c_a_later_write_is_refused_by_the_service_not_by_python(self):
        self.require_surface()
        # The binding keeps NO ended latch. The refusal after an end is the
        # service's WRONG_STATE, translated, and it is proved by scripting the
        # service to answer OK: a Python latch would refuse anyway.
        endpoint = self.connect()
        sender = self.attach(endpoint)
        try:
            track = self.track(sender)
            self.assertIs(sender.end_track(track), moq5.WriteOutcome.ACCEPTED)
            _native._test_write_result(WRONG_STATE)
            with self.assertRaises(moq5.MoqError) as raised:
                sender.write(track, moq5.SendObject(payload=b"frame"))
            code, operation = raised.exception.code, raised.exception.operation
            del raised
            self.assertEqual(code, WRONG_STATE)
            self.assertEqual(operation, "write")
            self.assertEqual(_native._test_write_counts()["entries"], 1,
                             "a Python latch refused before the service did")
            _native._test_write_result(0)
            self.assertIs(sender.write(track, moq5.SendObject(payload=b"f")),
                          moq5.WriteOutcome.ACCEPTED,
                          "the binding kept its own ended state")
        finally:
            _native._test_write_result(0)
            try:
                sender.close()
            finally:
                endpoint.close()


class EndOutcomeTests(EndGatedBase):
    def test_a_the_agreed_results_are_outcomes_not_exceptions(self):
        self.require_surface()
        for code, expected in ((0, moq5.WriteOutcome.ACCEPTED),
                               (WOULD_BLOCK, moq5.WriteOutcome.WOULD_BLOCK),
                               (INTERRUPTED, moq5.WriteOutcome.INTERRUPTED),
                               (CLOSED, moq5.WriteOutcome.CLOSED)):
            with self.subTest(code=code):
                # per-ITERATION settlement: this fake holds one owner, and the
                # recorder's counts are per test, so each row starts clean
                _native._test_end_reset()
                endpoint = self.connect()
                sender = self.attach(endpoint)
                try:
                    track = self.track(sender)
                    _native._test_end_result(code)
                    outcome = sender.end_track(track)
                    self.assertIs(outcome, expected,
                                  "the exact outcome was not returned")
                    self.assertEqual(_native._test_end_counts()["entries"], 1,
                                     "the result was retried")
                finally:
                    _native._test_end_result(0)
                    try:
                        sender.close()
                    finally:
                        endpoint.close()

    def test_b_other_signed_errors_are_preserved_as_end_track(self):
        self.require_surface()
        for code in (INVAL, WRONG_STATE, PROTO, UNKNOWN_CODE):
            with self.subTest(code=code):
                _native._test_end_reset()
                endpoint = self.connect()
                sender = self.attach(endpoint)
                try:
                    track = self.track(sender)
                    _native._test_end_result(code)
                    with self.assertRaises(moq5.MoqError) as raised:
                        sender.end_track(track)
                    signed = raised.exception.code
                    operation = raised.exception.operation
                    del raised
                    self.assertEqual(signed, code,
                                     "the signed code was normalised")
                    self.assertEqual(operation, "end_track",
                                     "the operation was mislabelled")
                    self.assertEqual(_native._test_end_counts()["entries"], 1,
                                     "an unknown error was retried")
                finally:
                    _native._test_end_result(0)
                    try:
                        sender.close()
                    finally:
                        endpoint.close()

    def test_c_would_block_is_not_retried_and_the_caller_retries_explicitly(self):
        self.require_surface()
        endpoint = self.connect()
        sender = self.attach(endpoint)
        try:
            track = self.track(sender)
            _native._test_end_result(WOULD_BLOCK)
            self.assertIs(sender.end_track(track),
                          moq5.WriteOutcome.WOULD_BLOCK)
            self.assertEqual(_native._test_end_counts()["entries"], 1,
                             "the bridge retried a WOULD_BLOCK itself")
            self.assertIs(_native._test_end_requested(0), False,
                          "a WOULD_BLOCK latched an end anyway")
            # the CALLER retries; that is a separate operation
            _native._test_end_result(0)
            self.assertIs(sender.end_track(track), moq5.WriteOutcome.ACCEPTED)
            self.assertEqual(_native._test_end_counts()["entries"], 2,
                             "the explicit retry was not a second call")
            self.assertEqual(_native._test_end_counts()["ended"], 1)
        finally:
            _native._test_end_result(0)
            try:
                sender.close()
            finally:
                endpoint.close()

    def test_d_a_successful_repeat_is_its_own_operation(self):
        self.require_surface()
        endpoint = self.connect()
        sender = self.attach(endpoint)
        try:
            track = self.track(sender)
            self.assertIs(sender.end_track(track), moq5.WriteOutcome.ACCEPTED)
            self.assertIs(sender.end_track(track), moq5.WriteOutcome.ACCEPTED)
            counts = _native._test_end_counts()
            self.assertEqual(counts["entries"], 2,
                             "the repeat never reached the service")
            self.assertEqual(counts["ended"], 1,
                             "the repeat queued a second terminal marker")
        finally:
            try:
                sender.close()
            finally:
                endpoint.close()

    def test_e_terminal_beats_idempotence(self):
        self.require_surface()
        # The service checks interrupt and terminal BEFORE its idempotent
        # no-op, so a repeat after terminalization is CLOSED, not a courtesy
        # OK. The binding reports what the service returns.
        endpoint = self.connect()
        sender = self.attach(endpoint)
        try:
            track = self.track(sender)
            self.assertIs(sender.end_track(track), moq5.WriteOutcome.ACCEPTED)
            _native._test_end_result(CLOSED)
            self.assertIs(sender.end_track(track), moq5.WriteOutcome.CLOSED,
                          "a terminal repeat was reported as success")
            _native._test_end_result(INTERRUPTED)
            self.assertIs(sender.end_track(track),
                          moq5.WriteOutcome.INTERRUPTED)
        finally:
            _native._test_end_result(0)
            try:
                sender.close()
            finally:
                endpoint.close()


    def test_f_a_removed_track_is_refused_by_the_service(self):
        self.require_surface()
        # Removal is not ending. The handle stays valid but inert, the service
        # answers WRONG_STATE, and the binding translates that as end_track
        # without inventing a Python-side policy for it.
        endpoint = self.connect()
        sender = self.attach(endpoint)
        try:
            track = self.track(sender)
            sender.remove_track(track)
            before = _native._test_end_counts()
            with self.assertRaises(moq5.MoqError) as raised:
                sender.end_track(track)
            code, operation = raised.exception.code, raised.exception.operation
            del raised
            self.assertEqual(code, WRONG_STATE)
            self.assertEqual(operation, "end_track")
            counts = _native._test_end_counts()
            self.assertEqual(counts["entries"], before["entries"] + 1,
                             "the removed track was not delivered exactly once")
            self.assertEqual(counts["ended"], before["ended"],
                             "removal was interpreted as a successful ending")
            self.assertIs(
                _native._test_end_requested(
                    _native._test_send_track_index_for(b"v", 1)), False,
                "an end request was latched for a removed track")
            self.assertIs(track.removed, True, "the track lost its removal")
        finally:
            try:
                sender.close()
            finally:
                endpoint.close()


class EndGuardTests(EndGatedBase):
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
                before = _native._test_end_counts()
                for value in (object(), None, receiver_track):
                    with self.subTest(value=type(value).__name__):
                        with self.assertRaises(TypeError):
                            sender.end_track(value)
                self.assertEqual(_native._test_end_counts(), before,
                                 "a wrong-typed argument entered native")
            finally:
                receiver.close()
        finally:
            try:
                sender.close()
            finally:
                endpoint.close()

    def test_b_a_foreign_owners_track_is_refused(self):
        self.require_surface()
        # The fake serves one endpoint and one sender at a time, so the two
        # owners are SEQUENTIAL. LIMITATION, stated rather than implied: the
        # first owner is necessarily closed by the time the second exists, so
        # this row pins refusal and zero native entry but does NOT establish
        # whether "not my track" or "its owner is closed" fired.
        endpoint = self.connect()
        first = self.attach(endpoint)
        foreign = self.track(first)
        first.close()
        endpoint.close()
        _native._test_reset()
        _native._test_sender_reset()
        _native._test_send_track_reset()
        _native._test_end_reset()
        second_endpoint = self.connect()
        second = self.attach(second_endpoint)
        try:
            before = _native._test_end_counts()
            with self.assertRaises(RuntimeError):
                second.end_track(foreign)
            self.assertEqual(_native._test_end_counts(), before,
                             "a foreign track entered native")
        finally:
            try:
                second.close()
            finally:
                second_endpoint.close()

    def test_c_a_closed_sender_is_refused_before_native(self):
        self.require_surface()
        endpoint = self.connect()
        sender = self.attach(endpoint)
        try:
            track = self.track(sender)
            sender.close()
            before = _native._test_end_counts()
            with self.assertRaises(RuntimeError):
                sender.end_track(track)
            self.assertEqual(_native._test_end_counts(), before,
                             "a closed owner entered native")
        finally:
            try:
                sender.close()
            finally:
                endpoint.close()

    def test_d_the_private_bridge_keeps_its_own_guards(self):
        self.require_surface()
        missing = tuple(n for n in BRIDGE if not hasattr(_native, n))
        self.assertEqual(missing, (),
                         "the end bridge is not implemented: _native is "
                         f"missing {', '.join(missing)}")
        endpoint = self.connect()
        sender = self.attach(endpoint)
        self.addCleanup(endpoint.close)
        self.addCleanup(sender.close)
        track = self.track(sender)
        sender_handle = getattr(sender, "_Sender__handle")
        track_handle = getattr(track, "_SendTrack__handle")
        endpoint_handle = getattr(endpoint, "_Endpoint__handle")
        before = _native._test_end_counts()

        # a Python isinstance check is not a memory-safe capsule check
        for label, wrong in (("endpoint", endpoint_handle),
                             ("sender", sender_handle)):
            with self.subTest(capsule=label):
                with self.assertRaises((ValueError, TypeError)):
                    _native.sender_end_track(sender_handle, wrong)
        self.assertEqual(_native._test_end_counts(), before,
                         "a wrong capsule reached the service")

        refused = []

        def other():
            try:
                _native.sender_end_track(sender_handle, track_handle)
            except RuntimeError as error:
                refused.append(("thread", error))
            except BaseException as error:              # noqa: BLE001
                refused.append(("thread-wrong", error))

        worker = threading.Thread(target=other, daemon=True)
        worker.start()
        worker.join(5.0)
        self.assertFalse(worker.is_alive(), "the direct guard worker hung")
        self.assertEqual([kind for kind, _ in refused], ["thread"],
                         "the bridge admitted a foreign thread")

        if hasattr(os, "fork"):
            child = os.fork()
            if child == 0:
                status = 0
                try:
                    _native.sender_end_track(sender_handle, track_handle)
                    status = 3                  # the inherited handle worked
                except RuntimeError:
                    pass
                except BaseException:
                    status = 9
                if _native._test_end_counts() != before:
                    status = 4
                os._exit(status)
            self.assertEqual(reap_child(child), 0,
                             "the bridge admitted an inherited handle")

        sender.close()
        with self.assertRaises(RuntimeError):
            _native.sender_end_track(sender_handle, track_handle)
        self.assertEqual(_native._test_end_counts(), before,
                         "a guarded direct call still entered native")


class EndBoundaryTests(EndGatedBase):
    """Allocation and signal boundaries, as for write."""

    def test_a_the_success_carrier_precedes_the_native_mutation(self):
        self.require_surface()
        # A failure to build the successful result must not leave an end
        # queued that the caller never learns about, so the carrier is built
        # BEFORE the native call and the fault cannot fire after it.
        endpoint = self.connect()
        sender = self.attach(endpoint)
        try:
            track = self.track(sender)
            _native._test_fail_allocation_at("end_track_result")
            with self.assertRaises(MemoryError):
                sender.end_track(track)
            self.assertTrue(
                _native._test_allocation_site_fired("end_track_result"),
                "the named end_track_result fault never fired")
            counts = _native._test_end_counts()
            self.assertEqual(counts["entries"], 0,
                             "the native mutation ran before its carrier "
                             "could be built")
            self.assertIs(_native._test_end_requested(0), False,
                          "an end was queued that the caller never saw")
        finally:
            _native._test_fail_allocation_at(None)
            try:
                sender.close()
            finally:
                endpoint.close()

    @unittest.skipUnless(hasattr(os, "fork"), "fork is a POSIX-only contract")
    def test_b_a_signal_is_processed_after_the_result_is_settled(self):
        self.require_surface()
        # An arranged Python exception does NOT certify that the end was not
        # requested: the request is the service's, and the interrupt only
        # reaches the caller after the native result has been settled.
        child = os.fork()
        if child == 0:
            status = 0
            try:
                import signal as signal_module

                received = []

                def handler(*args):
                    received.append(True)
                    raise KeyboardInterrupt

                signal_module.signal(signal_module.SIGUSR1, handler)
                endpoint = self.connect()
                sender = self.attach(endpoint)
                helper = None
                try:
                    track = self.track(sender)
                    reached, released = [], []

                    def arm():
                        # the signal is raised while the call is PARKED inside
                        # the fixture's native end, on the same bounded gate
                        # the write rows use: no sleeps, no scheduling guess
                        entered = _native._test_write_wait_entered(5.0)
                        reached.append(entered)
                        if entered:
                            os.kill(os.getpid(), signal_module.SIGUSR1)
                        released.append(_native._test_write_release())

                    helper = threading.Thread(target=arm, daemon=True)
                    _native._test_end_block(True)
                    helper.start()
                    interrupted = False
                    try:
                        sender.end_track(track)
                    except KeyboardInterrupt:
                        interrupted = True
                    finally:
                        _native._test_end_block(False)
                        _native._test_write_release()
                        helper.join(10.0)
                    counts = _native._test_end_counts()
                    if helper.is_alive():
                        status = 10         # harness: the helper never settled
                    elif reached != [True]:
                        status = 11         # the call was never entered
                    elif released[:1] != [True]:
                        status = 12         # harness: the bound expired
                    elif not received:
                        status = 13         # the signal never arrived
                    elif not interrupted:
                        status = 14         # the interrupt was swallowed
                    elif counts["entries"] != 1:
                        status = 15         # more than one call was made
                    elif counts["ended"] != 1:
                        status = 16         # the end was not requested
                    elif not _native._test_end_requested(0):
                        # an arranged Python exception does NOT certify that
                        # the end was not requested: it WAS
                        status = 17
                finally:
                    _native._test_end_block(False)
                    _native._test_write_release()
                    if helper is not None:
                        helper.join(10.0)
                    sender.close()
                    endpoint.close()
            except BaseException:
                status = 29
            os._exit(status)
        self.assertEqual(reap_child(child), 0,
                         "the end-track signal child did not report a clean "
                         "one-call inventory")


# ------------------------------------------------------------------ control --

PUBLIC_GATED_CLASSES = ("EndSurfaceTests", "EndOutcomeTests", "EndGuardTests",
                        "EndBoundaryTests")


class EndControlTests(unittest.TestCase):
    """This file's own machinery. Never evidence about the product.

    The inert-name control establishes the GATE: every gated row is invoked
    and refuses by a named assertion. It does NOT execute the bodies -- each
    stops inside require_surface. The scoped stand-in control below is what
    runs those bodies, so this RED cannot hide a broken test.
    """

    def test_a_inert_names_are_refused_by_every_gated_row(self):
        import test_send_end as module

        saved = getattr(moq5.Sender, "end_track", None)
        try:
            moq5.Sender.end_track = object()
            loader = unittest.defaultTestLoader
            suite = unittest.TestSuite(
                loader.loadTestsFromTestCase(getattr(module, c))
                for c in PUBLIC_GATED_CLASSES)
            result = unittest.TestResult()
            suite.run(result)
        finally:
            if saved is None:
                del moq5.Sender.end_track
            else:
                moq5.Sender.end_track = saved
        self.assertGreater(result.testsRun, 0)
        self.assertEqual(result.skipped, [], "a gated row skipped")
        self.assertEqual(result.errors, [],
                         "a gated row raised an arbitrary error instead of "
                         "refusing by a named assertion")
        failed = {getattr(case, "test_case", case).id()
                  for case, _ in result.failures}
        self.assertEqual(len(failed), result.testsRun,
                         "an inert surface satisfied a behavioural row")

    def test_b_the_gated_bodies_pass_against_a_scoped_stand_in(self):
        """Every behavioural body, run against a stand-in end_track.

        The stand-in is this file's own adapter over the recorder, installed
        and removed here. It proves the rows are satisfiable and that their
        assertions are about the declared contract -- not that any product
        exists, which is what the RED attribution reports.
        """
        import test_send_end as module

        def end_track(sender, track):
            # the declared guard order, modelled: type, ownership, then a live
            # owner, all before any native effect
            if not isinstance(track, moq5.SendTrack):
                raise TypeError("track must be a SendTrack")
            if track.sender is not sender:
                raise RuntimeError("this track belongs to another sender")
            if sender.closed:
                raise RuntimeError("sender is closed")
            # a removed handle is still nameable in the fixture, so the
            # stand-in can represent the row the product must satisfy
            index = _native._test_send_track_index_for(track.name, 1)
            code = _native._test_end_simulate(index)
            if code in (0, WOULD_BLOCK, INTERRUPTED, CLOSED):
                return moq5.WriteOutcome(code)
            raise moq5.MoqError(code, "end_track", "scripted")

        saved = getattr(moq5.Sender, "end_track", None)
        self.addCleanup(_native._test_end_reset)
        try:
            moq5.Sender.end_track = end_track
            loader = unittest.defaultTestLoader
            names = []
            for name in PUBLIC_GATED_CLASSES:
                case_class = getattr(module, name)
                for test in loader.getTestCaseNames(case_class):
                    # the private-bridge and carrier rows are about a native
                    # entry point the stand-in does not have; they are named
                    # here rather than silently skipped
                    if test in ("test_d_the_private_bridge_keeps_its_own_guards",
                                "test_a_the_success_carrier_precedes_the_native_mutation"):
                        # Both are about the NATIVE entry point, which a Python
                        # stand-in does not have. They are named here rather
                        # than skipped silently, and they execute against the
                        # real bridge as ordinary gated rows.
                        continue
                    names.append(f"{name}.{test}")
            suite = loader.loadTestsFromNames(names, module)
            result = unittest.TestResult()
            suite.run(result)
        finally:
            if saved is None:
                del moq5.Sender.end_track
            else:
                moq5.Sender.end_track = saved
        self.assertEqual(
            [f"{case.id()}: {error}" for case, error in
             result.failures + result.errors], [],
            "a gated body failed against the declared contract")
        self.assertGreater(result.testsRun, 0)

    def test_c_the_bridge_is_present_and_refuses_wrong_arguments(self):
        # This row was the RED gate's "the bridge is absent" assertion. The
        # bridge exists now, so it pins presence, callability and that a
        # wrong-shaped call is refused rather than crashing.
        missing = tuple(n for n in BRIDGE if not hasattr(_native, n))
        self.assertEqual(missing, (), f"the end bridge lost {missing}")
        for name in BRIDGE:
            self.assertTrue(callable(getattr(_native, name)),
                            f"_native.{name} is not callable")
        before = _native._test_end_counts()
        for args in ((), (object(),), (object(), object()),
                     (object(), object(), object())):
            with self.subTest(arity=len(args)):
                with self.assertRaises((TypeError, ValueError)):
                    _native.sender_end_track(*args)
        self.assertEqual(_native._test_end_counts(), before,
                         "a wrong-shaped call reached the service")
