"""Broadcast completion as a REQUEST: Sender.request_complete() -> None.

These rows were written RED, against a method that did not exist, and each
failed by a NAMED missing behaviour rather than skipping. The method exists
now and they pass. The gate that made that meaningful is kept: a missing or
inert name is still a named failure, the substrate rows still execute the
recorder's oracles on their own, and the scoped stand-in still proves this
file's bodies are satisfiable independently of the product.

Scope: ONE asynchronous request that the broadcast be permanently terminated.
Not a graceful finish, not a flush, not a drain, not a delivery receipt, not
sender destruction and not endpoint teardown. No finish(), no drain(), no
completed predicate, no automatic close, no queue-empty inference.

The contract, re-derived from THIS worktree rather than from a sketch:

  service/include/moq/media_sender.h:509-527
      The service ends every active publisher track reliably (END_OF_TRACK,
      seen by receivers as MEDIA_TRACK_ENDED) together with their generated
      SAP-timeline siblings, then publishes a terminal INDEPENDENT catalog
      generation carrying isComplete:true and an empty tracks array, live to
      active subscribers and retained for a later joiner. The namespace and
      endpoint STAY UP: this is not destroy(). Legal only after readiness;
      pre-ready is WRONG_STATE. IDEMPOTENT: a second call returns OK and does
      nothing. After completion add_track, write, remove_track and end_track
      all return WRONG_STATE, and track handles stay valid but inert until
      destroy. Documented results: OK, INVAL (NULL sender), WRONG_STATE
      (pre-ready), CLOSED (terminal), INTERRUPTED (latch set).

  service/src/media_sender.c:4688-4716, the ORDER the checks actually run
      1. NULL sender                     -> INVAL
      2. endpoint interrupt latch        -> INTERRUPTED
      3. sender terminal                 -> CLOSED
      4. fatal, under the mutex          -> CLOSED
      5. already completing              -> OK, and nothing else happens
      6. not ready                       -> WRONG_STATE
      otherwise, under the mutex: completing is set and EVERY track (media
      and generated timeline) is marked removed; the catalog is dirtied; then
      the mutex is released and the endpoint is woken.

  draft-ietf-moq-msf-01 section 11.3, and 5.1.3 for the field
      Permanent termination without VOD conversion publishes an independent
      catalog update signalling isComplete TRUE with an empty Tracks field,
      after SUBSCRIBE_DONE (Track Ended) for the active tracks. The native
      implementation performs that work ASYNCHRONOUSLY, on its network pump.

What follows from the ORDER, and is pinned as such:

  * Interrupt and terminal beat idempotence. Steps 2-4 run BEFORE step 5, so a
    repeat call after the latch is set or the sender terminalizes is a
    refusal, not a courtesy success. Idempotence never erases a later refusal.
  * Readiness is checked AFTER the idempotent short-circuit, so a second
    request on an already-completing sender does not re-derive readiness.
  * The SYNCHRONOUS effects are the completing latch, every track's removed
    flag, the dirtied catalog and the endpoint wake. The END_OF_TRACKs, the
    terminal catalog generation and the DISCARD ACCOUNTING for the removed
    tracks' queued media all happen later, on the network pump: the discards
    run through the drain's removed-track branch (media_sender.c:1522-1534)
    and the single preq_note_dropped chokepoint. The request does not perform
    that accounting itself and does not manufacture a pump cycle.

    The accounting rows here are therefore about the BINDING, not about
    timing: this fake has no pump, so its counters cannot move by themselves,
    and an unchanged snapshot proves only that the binding adds no accounting
    of its own. Against the real service a concurrent pump may well move
    those counters between the request and the caller's next snapshot; no row
    here claims otherwise, and none tries to measure it.

The Python mapping this file declares:

    native OK          -> None
    every other code   -> MoqError(code, "request_complete")

There is no retryable value and no outcome enum: unlike write and end_track,
completion has no documented WOULD_BLOCK, so the binding invents none. INVAL,
WRONG_STATE, CLOSED and INTERRUPTED all raise with their signed codes intact.

Track state after an accepted request. Completion is a removal initiated
through this binding, so `SendTrack.removed` must answer True for the
sender's existing tracks once a request has been accepted -- the binding must
not report False for a track the service has just removed. The shape this
file pins is an acknowledgment recorded on the SENDER handle: one private bit
set after native MOQ_OK, with no allocation, no iteration and no per-track
registry, which the existing track accessor combines with its own
explicit-remove bit. That bit is binding-owned state: it is readable after
the sender is closed without touching the destroyed native sender, it never
skips or short-circuits a later native request, it never pre-empts add,
write, end or remove, it is not a public completed property, and it says
nothing about catalog emission. A refusal does not set it, and a refusal
after an accepted request does not erase it.

What the binding must NOT do beyond that: retry, loop, short-circuit a repeat
request, close or destroy anything, wait, drain, flush, or infer completion
from statistics.

A scripted fake proves the BINDING's mapping, identity, guards and lack of
side effects. It does not prove the service emits a terminal catalog or ends
publisher tracks: that belongs to the native suite, which drives the public
complete over a real peer.
"""

import gc
import os
import threading
import unittest
import weakref

import moq5
from moq5 import _native

import test_foundation as foundation  # noqa: F401
from test_sender import reap_child
# The accepted owned-child envelope, reused rather than rebuilt: every
# concurrent scenario here creates its own owners and workers inside it.
from test_send_wait import CHILD_MEANING, assert_child_ok, in_owned_child
from test_send_wait import child_fail_now as _envelope_fail_now

NS = (b"svc", b"demo")

OK = 0
INVAL = -2
PROTO = -3
CLOSED = -4
WRONG_STATE = -5
WOULD_BLOCK = -8
INTERRUPTED = -13
# A representable negative the binding has never heard of: "an unknown code is
# preserved" means nothing if every code tested is one it already knows.
UNKNOWN_CODE = -12345

SURFACE = ("request_complete",)
BRIDGE = ("sender_complete",)

PUBLIC_GATED_CLASSES = (
    "CompleteSurfaceTests",
    "CompleteOutcomeTests",
    "CompleteStateTests",
    "CompleteGuardTests",
    "CompleteBoundaryTests",
)

# Names this slice must NOT introduce while claiming a request-only contract.
FORBIDDEN = ("finish", "drain", "complete", "completed", "flush",
             "wait_for_completion")


# The completion-fixture reset entries when this child started. The envelope's
# receipt reports WAIT-fixture resets, which are a different family: nothing
# in it observes THIS recorder's reset, so the observation below is local.
_COMPLETE_RESET_BASELINE = None


def arm_completion_quarantine():
    """Record the completion-reset baseline for the child that is starting."""
    global _COMPLETE_RESET_BASELINE
    _COMPLETE_RESET_BASELINE = _native._test_complete_reset_entries()


def child_fail_now(status):
    """Report an unsettled verdict and leave IMMEDIATELY, observing first
    whether anything has reset THIS module's completion fixture since the
    child started.

    The observation is taken here, at the decision-to-exit boundary and as
    late as anything in this module runs, so a reset introduced at that
    boundary moves the verdict to the ownership status rather than passing
    unnoticed. Nothing unwinds afterwards: the envelope's helper writes the
    receipt and exits.
    """
    if _COMPLETE_RESET_BASELINE is not None:
        if _native._test_complete_reset_entries() != _COMPLETE_RESET_BASELINE:
            status = 17          # the fixture was reset under a live worker
    _envelope_fail_now(status)


def missing_surface() -> list[str]:
    return [name for name in SURFACE
            if not callable(getattr(moq5.Sender, name, None))]


def service_entries():
    """Every observable service entry, as ONE image.

    The sender log names the sender operations; the endpoint counters are
    ENTRY witnesses for the two endpoint operations that never write to that
    log. A row that only filtered the log could not say that an endpoint wait
    did not happen, because a wait leaves no line in it.
    """
    endpoint = _native._test_endpoint_entries()
    return {"log": _native._test_sender_log(),
            "endpoint_wait": endpoint["wait"],
            "endpoint_wake": endpoint["wake"]}


class CompleteFixtureBase(unittest.TestCase):
    def setUp(self):
        gc.collect()
        _native._test_reset()
        _native._test_sender_reset()
        _native._test_send_track_reset()
        _native._test_write_reset()
        _native._test_end_reset()
        _native._test_complete_reset()

    def tearDown(self):
        _native._test_complete_block(False)
        _native._test_complete_result(0)
        _native._test_write_result(0)
        _native._test_end_result(0)
        _native._test_complete_reset()

    def connect(self):
        return moq5.Endpoint.connect(
            moq5.EndpointConfig(url="moqt://fixture.invalid"))

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

    def live(self):
        """An attached sender, closed by the row's cleanup."""
        endpoint = self.connect()
        sender = self.attach(endpoint)
        self.addCleanup(endpoint.close)
        self.addCleanup(sender.close)
        return endpoint, sender


# ---------------------------------------------------------------- substrate --

class CompleteSubstrateTests(CompleteFixtureBase):
    """The completion recorder and its seams, exercised without the bridge."""

    def test_a_reset_reports_an_empty_inventory(self):
        self.assertEqual(_native._test_complete_counts(),
                         {"entries": 0, "accepted": 0, "tracks_removed": 0,
                          "completing": False, "seen": False})
        self.assertIsNone(_native._test_complete_target())

    def test_b_a_driven_request_is_captured_with_its_exact_target(self):
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                self.assertEqual(_native._test_complete_simulate(), OK)
                self.assertEqual(_native._test_complete_counts(),
                                 {"entries": 1, "accepted": 1,
                                  "tracks_removed": 0, "completing": True,
                                  "seen": True})
                self.assertEqual(_native._test_complete_target(),
                                 {"sender_index": 0, "sender_null": False})
            finally:
                sender.close()

    def test_c_acceptance_marks_this_senders_live_tracks_removed(self):
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                self.track(sender, name=b"one")
                self.track(sender, name=b"two")
                self.assertIs(_native._test_send_track_state(0)["removed"],
                              False)
                self.assertEqual(_native._test_complete_simulate(), OK)
                counts = _native._test_complete_counts()
                self.assertEqual(counts["tracks_removed"], 2,
                                 "the request did not mark the owned tracks")
                for index in (0, 1):
                    self.assertIs(
                        _native._test_send_track_state(index)["removed"], True,
                        "an owned track was left live by the request")
            finally:
                sender.close()

    def test_d_a_foreign_owners_tracks_are_not_marked(self):
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                self.track(sender, name=b"one")
                # the stub owner is a DIFFERENT sender: its request must not
                # reach this sender's tracks
                self.assertEqual(_native._test_complete_simulate(1), OK)
                self.assertEqual(
                    _native._test_complete_counts()["tracks_removed"], 0)
                self.assertIs(_native._test_send_track_state(0)["removed"],
                              False)
            finally:
                sender.close()

    def test_e_a_repeat_is_idempotent_and_still_one_call_each(self):
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                self.track(sender, name=b"one")
                self.assertEqual(_native._test_complete_simulate(), OK)
                self.assertEqual(_native._test_complete_simulate(), OK)
                counts = _native._test_complete_counts()
                self.assertEqual(counts["entries"], 2,
                                 "a repeat did not reach the recorder")
                self.assertEqual(counts["accepted"], 1,
                                 "a repeat was accepted a second time")
                self.assertEqual(counts["tracks_removed"], 1,
                                 "a repeat marked the tracks again")
            finally:
                sender.close()

    def test_f_a_scripted_refusal_latches_nothing(self):
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                self.track(sender, name=b"one")
                _native._test_complete_result(WRONG_STATE)
                self.assertEqual(_native._test_complete_simulate(),
                                 WRONG_STATE)
                counts = _native._test_complete_counts()
                self.assertEqual(counts["entries"], 1)
                self.assertEqual(counts["accepted"], 0)
                self.assertIs(counts["completing"], False,
                              "a refusal latched completion")
                self.assertIs(_native._test_send_track_state(0)["removed"],
                              False, "a refusal removed a track")
            finally:
                _native._test_complete_result(OK)
                sender.close()

    def test_g_a_refusal_after_acceptance_is_still_a_refusal(self):
        """The recorder orders its checks the way the service does: the
        scripted interrupt/terminal answer precedes the idempotent no-op, so
        an accepted request does not convert a later refusal into success."""
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                self.assertEqual(_native._test_complete_simulate(), OK)
                _native._test_complete_result(INTERRUPTED)
                self.assertEqual(_native._test_complete_simulate(),
                                 INTERRUPTED)
                self.assertIs(_native._test_complete_counts()["completing"],
                              True, "the refusal cleared the latch")
            finally:
                _native._test_complete_result(OK)
                sender.close()

    def test_h_a_null_sender_is_recorded_as_such(self):
        self.assertEqual(_native._test_complete_simulate(0, 1), OK)
        self.assertEqual(_native._test_complete_target(),
                         {"sender_index": -1, "sender_null": True})

    def test_i_the_in_call_barrier_is_a_real_handshake(self):
        """Proves the fixture's own barrier before any product row depends on
        it: a caller parks inside the native call and another thread reaches
        it and releases it. Nothing sleeps.

        It runs in an OWNED child because it starts a worker: an unsettled
        worker must never survive into this runner, and a failed join is not
        settlement. The child reports its own verdict and, when its worker has
        not settled, leaves immediately without unwinding anything.
        """
        def body():
            arm_completion_quarantine()
            reached = released = False
            result = []

            def driver():
                result.append(_native._test_complete_simulate())

            _native._test_complete_block(True)
            worker = threading.Thread(target=driver, daemon=True)
            worker.start()
            try:
                reached = _native._test_write_wait_entered(5.0)
                released = _native._test_write_release()
            finally:
                _native._test_complete_block(False)
                _native._test_write_release()
                worker.join(10.0)
                if worker.is_alive():
                    child_fail_now(10)       # never touch the fixture now
            if not reached:
                return 13
            if not released:
                return 12
            return 0 if result == [OK] else 11

        assert_child_ok(self, in_owned_child(self, body),
                        "the fixture barrier handshake")

    def test_j_an_unsettled_worker_terminates_its_child_and_nothing_here(self):
        """The unsettled path, qualified rather than asserted.

        A worker that cannot be settled is exactly the case the owned child
        exists for. The child reports the unsettled verdict and leaves at
        once, and its own COMPLETION-fixture reset entries are observed at
        that decision: a reset introduced there is reported as the ownership
        status instead of passing. The envelope's receipt reports sender
        destroys, sender attaches and WAIT-fixture resets -- a different
        family, named here so it is not mistaken for this observation.
        """
        never = threading.Event()

        def body():
            arm_completion_quarantine()
            worker = threading.Thread(target=never.wait, daemon=True)
            worker.start()
            worker.join(0.2)
            if worker.is_alive():
                child_fail_now(10)          # leaves immediately
            return 0                        # unreachable: the event is never set

        outcome = in_owned_child(self, body)
        self.assertEqual(
            outcome, 10,
            "the unsettled child did not report its verdict: "
            f"{CHILD_MEANING.get(outcome, 'unknown child status')}")
        destroys, attaches, _wait_resets = self.child_observations
        self.assertEqual((destroys, attaches), (0, 0),
                         "the child destroyed or attached an owner after "
                         "deciding it was unsettled")


# -------------------------------------------------------------------- gate --

class CompleteGatedBase(CompleteFixtureBase):
    """Never skips. A missing or inert surface is a NAMED failure, asserted
    before the row acquires an endpoint."""

    def require_surface(self):
        missing = missing_surface()
        if missing:
            self.fail("the completion-request surface is not implemented: "
                      f"moq5.Sender is missing {', '.join(missing)}")

    def request(self, sender):
        return getattr(sender, "request_complete")()


class CompleteSurfaceTests(CompleteGatedBase):
    def test_a_one_request_reaches_the_exact_sender(self):
        self.require_surface()
        endpoint, sender = self.live()
        self.track(sender, name=b"one")
        self.assertIsNone(self.request(sender),
                          "the request returned a value; it is None")
        counts = _native._test_complete_counts()
        self.assertEqual(counts["entries"], 1,
                         "the request did not make exactly one native call")
        self.assertEqual(_native._test_complete_target(),
                         {"sender_index": 0, "sender_null": False},
                         "another sender was completed")

    def test_b_the_request_performs_no_other_service_entry(self):
        """The ALLOWED delta, not a filter.

        Every sender operation the fixture records is compared, so an
        unrecognised one cannot be hidden, and the endpoint's own wait AND
        wake entries are each compared -- neither reaches the sender log, so
        a log-only oracle could not see them. The claim is scoped to entries
        this fixture actually observes: it does not stand in for a transport
        drain, which has no entry here at all.
        """
        self.require_surface()
        endpoint, sender = self.live()
        self.track(sender, name=b"one")
        before = service_entries()
        destroys_before = _native._test_sender_counts()["destroy_entries"]
        self.request(sender)
        after = service_entries()
        self.assertEqual(after["log"][len(before["log"]):], ("complete",),
                         "the request performed extra service operations")
        self.assertEqual(after["endpoint_wait"], before["endpoint_wait"],
                         "the request entered an endpoint wait of its own")
        # The permitted wake delta here is ZERO, and that is a statement about
        # THIS fixture: its recorder does not perform the wake the real
        # service's completion performs, so every wake seen here would be one
        # the binding added. It neither forbids nor measures the native
        # implementation's own required wake.
        self.assertEqual(after["endpoint_wake"], before["endpoint_wake"],
                         "the request woke the endpoint itself")
        self.assertEqual(_native._test_sender_counts()["destroy_entries"],
                         destroys_before,
                         "the request destroyed the sender")

    def test_c_the_owner_and_its_endpoint_are_retained(self):
        self.require_surface()
        endpoint, sender = self.live()
        track = self.track(sender, name=b"one")
        self.request(sender)
        self.assertIs(sender.closed, False, "the request closed the sender")
        self.assertIs(sender.endpoint, endpoint,
                      "the request dropped the endpoint")
        self.assertIs(endpoint.closed, False,
                      "the request closed the endpoint")
        self.assertIs(track.sender, sender)
        self.assertEqual(track.name, b"one",
                         "the track lost its declared name")

    def test_d_no_finish_drain_or_completed_predicate_appears(self):
        self.require_surface()
        for name in FORBIDDEN:
            self.assertFalse(
                hasattr(moq5.Sender, name),
                f"this slice introduced Sender.{name}: the request is not a "
                "graceful finish and has no completion predicate")

    def test_e_success_adds_no_accounting_of_its_own(self):
        """This fake has no pump, so nothing can move its counters except the
        binding. An unchanged snapshot therefore says the binding performs no
        accounting itself; it is NOT a claim that the real service's counters
        stay still across a request."""
        self.require_surface()
        endpoint, sender = self.live()
        self.track(sender, name=b"one")
        _native._test_sender_stats(7, 3, 4, 900, 0, 0, 0, 0, 0, 0, 0)
        before = sender.stats()
        self.request(sender)
        after = sender.stats()
        self.assertEqual(after, before,
                         "the binding changed the statistics snapshot")
        self.assertEqual(after.objects_queued, 4,
                         "the binding pretended the queue had drained")
        self.assertEqual(after.objects_dropped, 0,
                         "the binding accounted a discard the service makes "
                         "later, on its pump")


class CompleteOutcomeTests(CompleteGatedBase):
    def test_a_the_documented_refusals_raise_with_their_codes(self):
        self.require_surface()
        endpoint, sender = self.live()
        for name, code in (("pre-ready", WRONG_STATE),
                           ("interrupted", INTERRUPTED),
                           ("terminal", CLOSED),
                           ("null sender", INVAL)):
            with self.subTest(case=name, code=code):
                _native._test_complete_reset()
                _native._test_complete_result(code)
                with self.assertRaises(moq5.MoqError) as caught:
                    self.request(sender)
                self.assertEqual(caught.exception.code, code,
                                 "the signed native code was not preserved")
                self.assertEqual(caught.exception.operation,
                                 "request_complete",
                                 "the refusal was attributed to another "
                                 "operation")
                self.assertEqual(_native._test_complete_counts()["entries"], 1,
                                 "the refusal was retried")

    def test_b_an_unknown_signed_code_is_preserved(self):
        self.require_surface()
        endpoint, sender = self.live()
        _native._test_complete_result(UNKNOWN_CODE)
        with self.assertRaises(moq5.MoqError) as caught:
            self.request(sender)
        self.assertEqual(caught.exception.code, UNKNOWN_CODE)
        self.assertEqual(caught.exception.operation, "request_complete")

    def test_c_there_is_no_retryable_outcome(self):
        """Completion has no documented WOULD_BLOCK, so the binding invents
        no retry value for it: an unexpected one is an error like any other."""
        self.require_surface()
        endpoint, sender = self.live()
        _native._test_complete_result(WOULD_BLOCK)
        with self.assertRaises(moq5.MoqError) as caught:
            self.request(sender)
        self.assertEqual(caught.exception.code, WOULD_BLOCK)
        self.assertEqual(_native._test_complete_counts()["entries"], 1,
                         "a would-block was retried inside the binding")

    def test_d_a_repeat_is_its_own_native_call(self):
        self.require_surface()
        endpoint, sender = self.live()
        self.track(sender, name=b"one")
        self.assertIsNone(self.request(sender))
        self.assertIsNone(self.request(sender),
                          "the second request did not return None")
        counts = _native._test_complete_counts()
        self.assertEqual(counts["entries"], 2,
                         "the binding short-circuited the repeat instead of "
                         "asking the service")
        self.assertEqual(counts["accepted"], 1)

    def test_e_idempotence_does_not_erase_a_later_refusal(self):
        """Both refusals the ORDER puts before the idempotent no-op: the
        interrupt latch and terminal state."""
        self.require_surface()
        for name, code in (("a later interrupt", INTERRUPTED),
                           ("a later terminal", CLOSED)):
            with self.subTest(case=name, code=code):
                endpoint = self.connect()
                sender = self.attach(endpoint)
                try:
                    _native._test_complete_reset()
                    self.assertIsNone(self.request(sender))
                    _native._test_complete_result(code)
                    with self.assertRaises(moq5.MoqError) as caught:
                        self.request(sender)
                    self.assertEqual(
                        caught.exception.code, code,
                        "an earlier acceptance turned a refusal into success")
                    self.assertIs(
                        _native._test_complete_counts()["completing"], True,
                        "the refusal cleared the service's latch")
                finally:
                    _native._test_complete_result(OK)
                    sender.close()
                    endpoint.close()


class CompleteStateTests(CompleteGatedBase):
    def test_a_an_accepted_request_removes_the_services_tracks(self):
        self.require_surface()
        endpoint, sender = self.live()
        self.track(sender, name=b"one")
        self.track(sender, name=b"two")
        self.request(sender)
        for index in (0, 1):
            self.assertIs(_native._test_send_track_state(index)["removed"],
                          True, "the service kept a track after completion")

    def test_b_a_refusal_leaves_the_track_state_unchanged(self):
        self.require_surface()
        endpoint, sender = self.live()
        track = self.track(sender, name=b"one")
        _native._test_complete_result(WRONG_STATE)
        with self.assertRaises(moq5.MoqError):
            self.request(sender)
        self.assertIs(_native._test_send_track_state(0)["removed"], False,
                      "a refused request still removed a track")
        self.assertIs(_native._test_complete_counts()["completing"], False)
        self.assertIs(track.removed, False,
                      "a refused request acknowledged a removal")

    def test_c_later_operations_are_refused_by_the_service(self):
        """After completion the service refuses add_track, write, end_track
        and remove_track with WRONG_STATE. The binding must pass each through
        under ITS OWN operation name, with exactly one native call each, and
        must not pre-empt any of them with a latch of its own."""
        self.require_surface()
        endpoint, sender = self.live()
        track = self.track(sender, name=b"one")
        self.request(sender)

        _native._test_write_result(WRONG_STATE)
        _native._test_end_result(WRONG_STATE)
        _native._test_send_track_results(WRONG_STATE, 0)
        obj = moq5.SendObject(payload=b"frame")

        adds_before = _native._test_send_track_counts()["add_entries"]
        with self.assertRaises(moq5.MoqError) as add_error:
            self.track(sender, name=b"later")
        self.assertEqual(add_error.exception.code, WRONG_STATE)
        self.assertEqual(add_error.exception.operation, "add_track")
        self.assertEqual(_native._test_send_track_counts()["add_entries"],
                         adds_before + 1,
                         "the add did not reach the service exactly once")

        with self.assertRaises(moq5.MoqError) as write_error:
            sender.write(track, obj)
        self.assertEqual((write_error.exception.code,
                          write_error.exception.operation),
                         (WRONG_STATE, "write"))
        self.assertEqual(_native._test_write_counts()["entries"], 1,
                         "the write did not reach the service exactly once")

        with self.assertRaises(moq5.MoqError) as end_error:
            sender.end_track(track)
        self.assertEqual((end_error.exception.code,
                          end_error.exception.operation),
                         (WRONG_STATE, "end_track"))
        self.assertEqual(_native._test_end_counts()["entries"], 1,
                         "the end did not reach the service exactly once")

        # remove_track needs no script: the recorder already marked this
        # track removed when it accepted the request, exactly as the service
        # does, so a second removal is the service's own WRONG_STATE.
        removes_before = _native._test_send_track_counts()["remove_entries"]
        with self.assertRaises(moq5.MoqError) as remove_error:
            sender.remove_track(track)
        self.assertEqual((remove_error.exception.code,
                          remove_error.exception.operation),
                         (WRONG_STATE, "remove_track"))
        self.assertEqual(_native._test_send_track_counts()["remove_entries"],
                         removes_before + 1,
                         "the remove did not reach the service exactly once")

    def test_d_an_accepted_request_is_acknowledged_on_every_track(self):
        """Completion is a removal made through this binding, so the tracks
        report it. The acknowledgment is the sender's; it must not short-
        circuit a later native request."""
        self.require_surface()
        endpoint, sender = self.live()
        one = self.track(sender, name=b"one")
        two = self.track(sender, name=b"two")
        self.assertIs(one.removed, False)
        self.assertIs(two.removed, False)

        self.request(sender)
        self.assertIs(one.removed, True,
                      "a track the service removed still reports live")
        self.assertIs(two.removed, True,
                      "only some of the sender's tracks were acknowledged")

        # the bit is not a Python completion latch: a second request is still
        # the service's to answer
        entries = _native._test_complete_counts()["entries"]
        self.assertIsNone(self.request(sender))
        self.assertEqual(_native._test_complete_counts()["entries"],
                         entries + 1,
                         "the acknowledgment skipped a native request")

        # nor does a later refusal erase it
        _native._test_complete_result(INTERRUPTED)
        with self.assertRaises(moq5.MoqError):
            self.request(sender)
        self.assertIs(one.removed, True,
                      "a later refusal erased an accepted removal")

    def test_e_the_acknowledgment_survives_the_owners_close(self):
        """It is binding-owned state, so it is still readable once the native
        sender is gone -- without reaching for the destroyed sender. The
        track's retained metadata behaves as it already does."""
        self.require_surface()
        endpoint = self.connect()
        self.addCleanup(endpoint.close)
        sender = self.attach(endpoint)
        track = self.track(sender, name=b"one")
        self.request(sender)
        entries = _native._test_complete_counts()["entries"]
        sender.close()
        self.assertIs(track.removed, True,
                      "the acknowledgment was lost with the owner")
        self.assertEqual(track.name, b"one")
        self.assertIs(track.sender, sender)
        self.assertEqual(_native._test_complete_counts()["entries"], entries,
                         "reading the acknowledgment re-entered the service")


class CompleteGuardTests(CompleteGatedBase):
    def test_a_the_request_takes_no_arguments(self):
        self.require_surface()
        endpoint, sender = self.live()
        for args in ((None,), (1,), (sender,)):
            with self.subTest(arity=len(args)):
                with self.assertRaises(TypeError):
                    getattr(sender, "request_complete")(*args)
        self.assertEqual(_native._test_complete_counts()["entries"], 0,
                         "a wrong-shaped call reached the service")

    def test_b_a_closed_sender_is_refused_before_native_entry(self):
        self.require_surface()
        endpoint = self.connect()
        self.addCleanup(endpoint.close)
        sender = self.attach(endpoint)
        sender.close()
        with self.assertRaises(RuntimeError):
            self.request(sender)
        self.assertEqual(_native._test_complete_counts()["entries"], 0,
                         "a closed owner reached the service")

    def test_c_a_foreign_thread_is_refused_before_native_entry(self):
        """In an owned child, because it starts a worker: the child settles
        that worker itself, and leaves immediately without unwinding if it
        cannot."""
        self.require_surface()

        def body():
            arm_completion_quarantine()
            endpoint = self.connect()
            sender = self.attach(endpoint)
            refused = []

            def other():
                try:
                    self.request(sender)
                except RuntimeError:
                    refused.append(True)
                except BaseException:                 # noqa: BLE001
                    refused.append(False)
                else:
                    refused.append(None)

            worker = threading.Thread(target=other, daemon=True)
            worker.start()
            worker.join(10.0)
            if worker.is_alive():
                child_fail_now(10)
            status = 0
            if refused != [True]:
                status = 16            # a foreign thread was not refused
            elif _native._test_complete_counts()["entries"] != 0:
                status = 16            # ... or it reached the service
            sender.close()
            endpoint.close()
            return status

        assert_child_ok(self, in_owned_child(self, body),
                        "the foreign-thread guard")

    def test_d_an_inherited_handle_is_refused_before_native_entry(self):
        """A forked child inherits the capsule but not its creator process.
        The child touches nothing else and exits at once, so nothing of this
        runner's state can be disturbed."""
        self.require_surface()
        if not hasattr(os, "fork"):                  # pragma: no cover
            self.skipTest("fork is unavailable on this platform")
        endpoint, sender = self.live()
        before = _native._test_complete_counts()["entries"]
        child = os.fork()
        if child == 0:
            status = 0
            try:
                self.request(sender)
                status = 3                   # the inherited handle worked
            except RuntimeError:
                pass
            except BaseException:            # noqa: BLE001
                status = 9
            if _native._test_complete_counts()["entries"] != before:
                status = 4                   # ... or it reached the service
            os._exit(status)
        self.assertEqual(reap_child(child), 0,
                         "the inherited-handle child did not exit cleanly")
        self.assertEqual(_native._test_complete_counts()["entries"], before,
                         "an inherited handle reached the service")

    def test_e_the_private_bridge_keeps_its_own_guards(self):
        self.require_surface()
        missing = tuple(n for n in BRIDGE if not hasattr(_native, n))
        self.assertEqual(missing, (),
                         "the completion bridge is not implemented: _native "
                         f"is missing {', '.join(missing)}")
        endpoint, sender = self.live()
        bridge = getattr(_native, "sender_complete")
        for args in ((), (object(),), (None,),
                     (getattr(sender, "_Sender__handle"), object())):
            with self.subTest(arity=len(args)):
                with self.assertRaises((TypeError, ValueError)):
                    bridge(*args)
        self.assertEqual(_native._test_complete_counts()["entries"], 0,
                         "a wrong-shaped bridge call reached the service")


class CompleteBoundaryTests(CompleteGatedBase):
    def test_a_the_gil_is_released_around_the_native_call(self):
        """The OWNER parks inside the native request while another thread
        keeps running: if the bridge held the GIL, no observation could be
        made from there. The fixture's bound is what keeps a non-releasing
        implementation a finite failure instead of a hang.

        The sender is created AND used on the same thread -- the worker owns
        it, so this is a GIL oracle and not an owner-guard refusal in
        disguise -- and that thread does its own cleanup. The child's main
        thread is only the observer. Everything lives in an owned child, so
        an unsettled worker can never reach this runner; when the worker
        cannot be settled the child leaves at once, closing nothing.
        """
        self.require_surface()

        def body():
            arm_completion_quarantine()
            outcome, reached, released = [], [], []
            ready = threading.Event()

            def owner():
                # created here: this thread is the capsule's owner, and the
                # same thread makes the call and closes what it made
                try:
                    endpoint = self.connect()
                except BaseException as error:        # noqa: BLE001
                    outcome.append(error)
                    ready.set()
                    return
                try:
                    sender = self.attach(endpoint)
                    try:
                        ready.set()
                        outcome.append(self.request(sender))
                    finally:
                        sender.close()
                except BaseException as error:        # noqa: BLE001
                    outcome.append(error)
                    ready.set()
                finally:
                    endpoint.close()

            _native._test_complete_block(True)
            worker = threading.Thread(target=owner, daemon=True)
            worker.start()
            try:
                ready.wait(10.0)
                reached.append(_native._test_write_wait_entered(5.0))
                released.append(_native._test_write_release())
            finally:
                _native._test_complete_block(False)
                _native._test_write_release()
                worker.join(10.0)
                if worker.is_alive():
                    child_fail_now(10)
            if reached != [True]:
                return 13          # the request never reached the service
            if released != [True]:
                return 12          # the fixture's own bound expired
            if outcome != [None]:
                return 11          # the parked request did not return None
            return 0

        assert_child_ok(self, in_owned_child(self, body),
                        "the GIL boundary of the request")

    def signal_scenario(self, fault=None):
        """The raising-signal scenario, as an owned-child body.

        `fault="join"` induces a settlement FAILURE with a worker that is
        safely parked -- it has already released the native call and only
        waits on an Event this child never sets -- so the disposition of the
        worker is genuinely unknown at cleanup time. That is the path the
        failure-path discriminator drives.
        """
        def body():
            arm_completion_quarantine()
            import signal as signal_module

            received = []
            parked = threading.Event()

            def handler(*args):
                received.append(True)
                raise KeyboardInterrupt

            signal_module.signal(signal_module.SIGUSR1, handler)
            endpoint = self.connect()
            sender = self.attach(endpoint)
            track = self.track(sender, name=b"one")

            def deliver():
                if not _native._test_write_wait_entered(5.0):
                    return                    # the bound expired; reported below
                os.kill(os.getpid(), signal_module.SIGUSR1)
                _native._test_write_release()
                if fault == "join":
                    # nothing native is held here: the call is already
                    # released, and this thread only waits
                    parked.wait()

            interrupted = False
            failed = False
            settled = False
            status = 0
            _native._test_complete_block(True)
            worker = threading.Thread(target=deliver, daemon=True)
            worker.start()
            # Nothing may escape this child unattributed. A signal that lands
            # LATE -- outside the call, which is what a bridge holding the GIL
            # produces -- is caught and reported as a named status instead of
            # unwinding past the owners.
            try:
                try:
                    self.request(sender)
                except KeyboardInterrupt:
                    interrupted = True
                except BaseException:                 # noqa: BLE001
                    failed = True
                finally:
                    # from here a delivery must not raise anywhere
                    signal_module.signal(signal_module.SIGUSR1,
                                         lambda *args: received.append("late"))
                _native._test_complete_block(False)
                _native._test_write_release()
                if fault == "join":
                    raise RuntimeError("induced settlement failure")
                worker.join(10.0)
                if worker.is_alive():
                    child_fail_now(10)
                settled = True
                counts = _native._test_complete_counts()
                removed = track.removed
                if failed:
                    status = 11     # the request failed for another reason
                elif received[:1] != [True]:
                    status = 14     # the declared signal never arrived
                elif counts["entries"] != 1:
                    status = 15     # the request did not run exactly once
                elif not interrupted:
                    status = 16     # the signal did not reach the caller
                elif counts["accepted"] != 1 or not removed:
                    status = 11     # acceptance was lost with the exception
            except BaseException:                     # noqa: BLE001
                status = 11
            finally:
                # Closing requires settlement to have been ESTABLISHED, not
                # merely attempted: an exception while joining or inspecting
                # the worker proves nothing about it. On any unknown or
                # unsettled disposition this child leaves HERE -- no close, no
                # reset, nothing unwound -- and says so by name.
                if not settled or worker.is_alive():
                    child_fail_now(10)
                for owner in (sender, endpoint):
                    try:
                        owner.close()
                    except BaseException:             # noqa: BLE001
                        status = status or 11
            return status

        return body

    def test_c_an_accepted_request_survives_a_raising_signal(self):
        """Acceptance is recorded BEFORE the signal is processed.

        A signal raised while the call is parked inside the service reaches
        the caller on the way out: the call raises instead of returning None.
        What the service already did is not undone by that, so the tracks
        still report the removal. The signal is delivered by a worker while
        the MAIN thread -- which owns both the sender and Python's signal
        handling -- is the one parked in the call, inside an owned child so
        the handler and the owners belong to nothing else.
        """
        self.require_surface()
        assert_child_ok(self, in_owned_child(self, self.signal_scenario()),
                        "an accepted request interrupted on its way out")

    def test_e_a_failed_settlement_destroys_and_resets_nothing(self):
        """The failure path of the row above, driven through the same body.

        When settlement cannot be ESTABLISHED -- here because joining itself
        fails while the worker is still parked -- the child must not close an
        owner, must not reset the completion fixture, must say so by name,
        and must be reaped by this parent. An exception while joining is not
        proof of settlement.
        """
        self.require_surface()
        outcome = in_owned_child(self, self.signal_scenario(fault="join"))
        self.assertEqual(
            outcome, 10,
            "an unknown worker disposition was not reported as unsettled: "
            f"{CHILD_MEANING.get(outcome, 'unknown child status')}")
        destroys, attaches, _wait_resets = self.child_observations
        self.assertEqual(destroys, 0,
                         "an owner was destroyed while the worker's "
                         "disposition was unknown")
        self.assertEqual(attaches, 1,
                         "the scenario did not attach exactly one sender")

    def test_d_a_refused_request_is_not_acknowledged(self):
        """The other half of the ordering: a refusal records nothing, even
        though the same code path runs."""
        self.require_surface()
        endpoint, sender = self.live()
        track = self.track(sender, name=b"one")
        _native._test_complete_result(CLOSED)
        with self.assertRaises(moq5.MoqError):
            self.request(sender)
        self.assertIs(track.removed, False,
                      "a refused request acknowledged a removal")

    def test_b_a_refusal_is_not_retried(self):
        self.require_surface()
        endpoint, sender = self.live()
        _native._test_complete_result(INTERRUPTED)
        with self.assertRaises(moq5.MoqError):
            self.request(sender)
        self.assertEqual(_native._test_complete_counts()["entries"], 1,
                         "the binding retried a refused request")


# ----------------------------------------------------------------- controls --

class CompleteControlTests(unittest.TestCase):
    """This file's own machinery. Never evidence about the product.

    The inert-name control establishes the GATE: every gated row is invoked
    and refuses by a named assertion. It does NOT execute the bodies -- each
    stops inside require_surface. The scoped stand-in control below runs those
    bodies, so this RED cannot hide a broken test.
    """

    # Rows about the NATIVE entry point, which a Python stand-in cannot model:
    # the bridge itself, the owner-thread and creator-process confinement it
    # enforces, and the ordering inside its own call. They are named here
    # rather than skipped silently, and each runs against the real bridge as
    # an ordinary gated row.
    EXCLUDED = (
        "CompleteGuardTests.test_e_the_private_bridge_keeps_its_own_guards",
        "CompleteGuardTests.test_c_a_foreign_thread_is_refused_before_native_entry",
        "CompleteGuardTests.test_d_an_inherited_handle_is_refused_before_native_entry",
        # The ordering INSIDE the native call: acceptance is recorded before
        # the signal is processed. A Python stand-in has no such interior --
        # its acknowledgment is ordinary bytecode, which a pending signal can
        # pre-empt -- so this row is named here and runs against the real
        # bridge as an ordinary gated row.
        "CompleteBoundaryTests.test_c_an_accepted_request_survives_a_raising_signal",
    )

    def test_a_the_surface_and_its_bridge_are_present_and_refuse_misuse(self):
        # This row was the RED gate's "the surface and the bridge are absent"
        # assertion. Both exist now, so it pins presence, callability and that
        # a wrong-shaped call is refused rather than crashing.
        self.assertEqual(missing_surface(), [],
                         "the completion surface was lost")
        missing = tuple(n for n in BRIDGE if not hasattr(_native, n))
        self.assertEqual(missing, (), f"the completion bridge lost {missing}")
        for name in BRIDGE:
            self.assertTrue(callable(getattr(_native, name)),
                            f"_native.{name} is not callable")
        before = _native._test_complete_counts()
        for args in ((), (object(),), (None,), (object(), object())):
            with self.subTest(arity=len(args)):
                with self.assertRaises((TypeError, ValueError)):
                    _native.sender_complete(*args)
        self.assertEqual(_native._test_complete_counts(), before,
                         "a wrong-shaped call reached the service")

    def test_b_inert_names_are_refused_by_every_gated_row(self):
        import test_send_complete as module

        saved = getattr(moq5.Sender, "request_complete", None)
        try:
            moq5.Sender.request_complete = object()
            loader = unittest.defaultTestLoader
            suite = unittest.TestSuite(
                loader.loadTestsFromTestCase(getattr(module, c))
                for c in PUBLIC_GATED_CLASSES)
            result = unittest.TestResult()
            suite.run(result)
        finally:
            if saved is None:
                del moq5.Sender.request_complete
            else:
                moq5.Sender.request_complete = saved
        self.assertGreater(result.testsRun, 0)
        self.assertEqual(result.skipped, [], "a gated row skipped")
        self.assertEqual(result.errors, [],
                         "a gated row raised an arbitrary error instead of "
                         "refusing by a named assertion")
        failed = {getattr(case, "test_case", case).id()
                  for case, _ in result.failures}
        self.assertEqual(len(failed), result.testsRun,
                         "an inert surface satisfied a behavioural row")

    def test_c_the_gated_bodies_pass_against_a_scoped_stand_in(self):
        """Every behavioural body, run against a stand-in request_complete.

        The stand-in is this file's own adapter over the recorder, installed
        and removed here. It models the DECLARED contract, including the
        sender-level acknowledgment the track accessor combines with its own
        removal bit -- which is why it patches both names. It proves the rows
        are satisfiable and that their assertions are about that contract, not
        that any product exists, which is what the RED attribution reports.
        """
        import test_send_complete as module

        # identity, not id(): an id is reused once a sender is collected, and
        # a stale acknowledgment would then follow an unrelated sender
        acknowledged = weakref.WeakSet()

        def request_complete(sender):
            # the declared guard order, modelled: a live owner before any
            # native effect. The owner-thread and creator-process guards are
            # not modelled here -- they are taken from the REAL bridge, by
            # making an existing guarded call first, so a row that calls from
            # the wrong thread or an inherited handle is refused under the
            # stand-in exactly as the product will refuse it.
            if sender.closed:
                raise RuntimeError("sender is closed")
            _native.sender_ready(getattr(sender, "_Sender__handle"))
            code = _native._test_complete_simulate()
            if code != OK:
                raise moq5.MoqError(code, "request_complete", "scripted")
            # set only AFTER acceptance, and never unset
            acknowledged.add(sender)
            return None

        def removed(track):
            handle = getattr(track, "_SendTrack__handle")
            return (_native.send_track_removed(handle)
                    or track.sender in acknowledged)

        saved = getattr(moq5.Sender, "request_complete", None)
        saved_removed = moq5.SendTrack.removed
        self.addCleanup(_native._test_complete_reset)
        try:
            moq5.Sender.request_complete = request_complete
            moq5.SendTrack.removed = property(removed)
            loader = unittest.defaultTestLoader
            names = []
            for name in PUBLIC_GATED_CLASSES:
                case_class = getattr(module, name)
                for test in loader.getTestCaseNames(case_class):
                    if f"{name}.{test}" in self.EXCLUDED:
                        continue
                    names.append(f"{name}.{test}")
            suite = loader.loadTestsFromNames(names, module)
            result = unittest.TestResult()
            suite.run(result)
        finally:
            moq5.SendTrack.removed = saved_removed
            if saved is None:
                del moq5.Sender.request_complete
            else:
                moq5.Sender.request_complete = saved
        self.assertEqual(
            [f"{case.id()}: {error}" for case, error in
             result.failures + result.errors], [],
            "a gated body failed against the declared contract")
        self.assertGreater(result.testsRun, 0)


if __name__ == "__main__":                            # pragma: no cover
    unittest.main()
