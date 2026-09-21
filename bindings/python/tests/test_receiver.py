"""Receiver shell contract, against the same bridge with the test-only C provider."""

import gc
import os
import threading
import unittest
import warnings
from unittest import mock

import moq5
from moq5 import _endpoint, _native

# Imported as a module so the loader does not collect the foundation suite twice.
import test_foundation as foundation

NS = (b"live", b"cam\x001")


class ReceiverShellTests(unittest.TestCase):
    def setUp(self):
        gc.collect()
        _native._test_reset()

    # ---- fixtures -------------------------------------------------------------

    def connect(self):
        return moq5.Endpoint.connect(moq5.EndpointConfig(url="moqt://fixture.invalid"))

    def attach(self, endpoint, **fields):
        return moq5.Receiver.attach(endpoint, moq5.ReceiverConfig.live(NS, **fields))

    def track(self, receiver):
        handle = receiver._Receiver__handle
        return receiver._track_from_capsule(_native._test_new_track(handle))

    # ---- values -----------------------------------------------------------------

    def test_receiver_config_is_checked_before_any_native_effect(self):
        good = moq5.ReceiverConfig(namespace=[b"a"], overflow=moq5.OverflowPolicy.DROP_GROUP)
        self.assertEqual(good.namespace, (b"a",))
        self.assertIs(good.overflow, moq5.OverflowPolicy.DROP_GROUP)
        self.assertIs(moq5.ReceiverConfig.live(NS).overflow, moq5.OverflowPolicy.DROP_TO_KEYFRAME)
        self.assertIs(moq5.ReceiverConfig.flow_control(NS).overflow, moq5.OverflowPolicy.FLOW_CONTROL)
        # boundary values accepted, boundary+1 refused
        moq5.ReceiverConfig.live(NS, max_objects=2_863_311_529, max_bytes=12_297_829_382_473_034_410,
                                 max_track_events=2**32 - 1)
        moq5.ReceiverConfig.live(tuple(bytes([i + 1]) for i in range(32)))
        for bad in (
            dict(namespace=NS),                                   # overflow required
            dict(namespace=NS, overflow=0),                       # UNSET is not a member
            dict(namespace=NS, overflow=True),
            dict(namespace=NS, overflow=1.0),
            dict(namespace=(), overflow=1),
            dict(namespace=(b"",), overflow=1),
            dict(namespace=("text",), overflow=1),
            dict(namespace=tuple(bytes([i + 1]) for i in range(33)), overflow=1),
            dict(namespace=NS, overflow=1, max_objects=2_863_311_530),
            dict(namespace=NS, overflow=1, max_bytes=12_297_829_382_473_034_411),
            dict(namespace=NS, overflow=1, max_track_events=2**32),
            dict(namespace=NS, overflow=1, max_objects=-1),
            dict(namespace=NS, overflow=1, max_objects=True),
            dict(namespace=NS, overflow=1, catalog_track="catalog"),
            dict(namespace=NS, overflow=1, auto_subscribe=1),
            dict(namespace=NS, overflow=1, time_mode=7),
        ):
            with self.subTest(bad=bad):
                with self.assertRaises((TypeError, ValueError)):
                    moq5.ReceiverConfig(**bad)
        self.assertEqual(_native._test_receiver_counts(), (0, 0))

    def test_enums_are_the_compiled_constants(self):
        c = _native.constants
        self.assertEqual(moq5.PollOutcome.EMPTY, c["DONE"])
        self.assertEqual(moq5.PollOutcome.INTERRUPTED, c["ERR_INTERRUPTED"])
        self.assertEqual(moq5.PollOutcome.CLOSED, c["ERR_CLOSED"])
        self.assertEqual([k.name for k in moq5.TrackEventKind],
                         ["ADDED", "UPDATED", "REMOVED", "ENDED", "CATALOG_READY", "UPDATE_OK", "PARSE_DROP"])
        self.assertEqual([k.value for k in moq5.TrackEventKind],
                         [c[n] for n in ("MEDIA_TRACK_ADDED", "MEDIA_TRACK_UPDATED", "MEDIA_TRACK_REMOVED",
                                         "MEDIA_TRACK_ENDED", "MEDIA_CATALOG_READY", "MEDIA_TRACK_UPDATE_OK",
                                         "MEDIA_TRACK_PARSE_DROP")])
        self.assertEqual(list(moq5.TrackState), [0, 1, 2, 3, 4, 5])
        self.assertEqual(list(moq5.ReceiverFatal), [1, 2, 3, 4])
        self.assertEqual(list(moq5.StartMode), [0, 1])
        self.assertEqual(list(moq5.TimeMode), [0, 1])
        self.assertEqual(list(moq5.OverflowPolicy), [1, 2, 3])

    # ---- attach ----------------------------------------------------------------

    def test_attach_forwards_the_exact_configuration(self):
        with self.connect() as endpoint:
            with moq5.Receiver.attach(endpoint, moq5.ReceiverConfig(
                    namespace=NS, overflow=moq5.OverflowPolicy.FLOW_CONTROL, catalog_track=b"cat",
                    auto_subscribe=True, time_mode=moq5.TimeMode.SHARED_EPOCH,
                    max_objects=7, max_bytes=2**40, max_track_events=9)) as receiver:
                self.assertIs(receiver.endpoint, endpoint)
                self.assertFalse(receiver.closed)
                observed = _native._test_receiver_config()
                self.assertEqual(observed["namespace"], NS, "binary parts copied byte-exact, NUL included")
                self.assertEqual(observed["catalog_track"], b"cat")
                self.assertTrue(observed["auto_subscribe"])
                self.assertEqual(observed["time_mode"], 1)
                self.assertEqual(observed["overflow"], 3)
                self.assertEqual(observed["max_objects"], 7)
                self.assertEqual(observed["max_bytes"], 2**40)
                self.assertEqual(observed["max_track_events"], 9)
                self.assertTrue(observed["full_size"])
                self.assertTrue(observed["endpoint_null"], "attach borrows: cfg.endpoint is NULL")
            self.assertTrue(receiver.closed)
        self.assertEqual(_native._test_receiver_counts(), (1, 1))
        self.assertEqual(_native._test_counts(), (1, 1, 1))

    def test_attach_forwards_a_binary_catalog_track_byte_exact(self):
        # The catalog track name is a length-bearing span in C, like the
        # namespace parts: NUL bytes are legal and copied exact.
        for name in (b"cat\x00alog", b"catalog", b""):
            with self.subTest(name=name):
                with self.connect() as endpoint:
                    with moq5.Receiver.attach(endpoint, moq5.ReceiverConfig.live(NS, catalog_track=name)):
                        self.assertEqual(_native._test_receiver_config()["catalog_track"], name)
        # the endpoint's own text fields still refuse NUL (foundation contract)
        with self.assertRaises(ValueError):
            moq5.EndpointConfig(url="moqt://a\x00b")

    def test_attach_defaults_are_the_native_defaults(self):
        with self.connect() as endpoint, self.attach(endpoint):
            observed = _native._test_receiver_config()
            self.assertEqual(observed["catalog_track"], b"")
            self.assertFalse(observed["auto_subscribe"])
            self.assertEqual((observed["time_mode"], observed["overflow"]), (0, 1))
            self.assertEqual((observed["max_objects"], observed["max_bytes"], observed["max_track_events"]), (0, 0, 0))

    def test_attach_refusals_keep_the_signed_code_and_retain_nothing(self):
        for code in (-2, -5, -4, -1):
            with self.subTest(code=code):
                _native._test_reset()
                endpoint = self.connect()
                try:
                    _native._test_attach_result(code)
                    with self.assertRaises(moq5.MoqError) as caught:
                        self.attach(endpoint)
                    self.assertEqual((caught.exception.code, caught.exception.operation), (code, "attach"))
                    self.assertEqual(_native._test_receiver_counts(), (0, 0))
                    _native._test_attach_result(0)
                    endpoint.close()   # nothing retained the endpoint
                    self.assertTrue(endpoint.closed)
                finally:
                    _native._test_attach_result(0)
                    endpoint.close()
                self.assertEqual(_native._test_counts(), (1, 1, 1))

    def test_attach_rejects_wrong_types_before_native(self):
        with self.connect() as endpoint:
            with self.assertRaises(TypeError):
                moq5.Receiver.attach(endpoint, "config")
            with self.assertRaises(TypeError):
                moq5.Receiver.attach("endpoint", moq5.ReceiverConfig.live(NS))
            with self.assertRaises(TypeError):
                moq5.Receiver()
        self.assertEqual(_native._test_receiver_counts(), (0, 0))

    def test_attach_to_a_closed_endpoint_is_refused_by_the_owner_check(self):
        endpoint = self.connect()
        endpoint.close()
        with self.assertRaises(RuntimeError):
            self.attach(endpoint)
        self.assertEqual(_native._test_receiver_counts(), (0, 0))

    def test_second_attach_is_the_native_slot_refusal(self):
        with self.connect() as endpoint, self.attach(endpoint):
            with self.assertRaises(moq5.MoqError) as caught:
                self.attach(endpoint)
            self.assertEqual((caught.exception.code, caught.exception.operation), (-5, "attach"))

    # ---- retention and close ----------------------------------------------------

    def test_endpoint_close_is_refused_while_attached_and_nothing_is_destroyed(self):
        endpoint = self.connect()
        receiver = self.attach(endpoint)
        with self.assertRaises(moq5.MoqError) as caught:
            endpoint.close()
        self.assertEqual((caught.exception.code, caught.exception.operation), (-5, "close"))
        self.assertFalse(endpoint.closed)
        self.assertEqual(_native._test_counts(), (1, 0, 0), "no stop was counted, nothing destroyed")
        receiver.close()
        endpoint.close()
        self.assertEqual(_native._test_counts(), (1, 1, 1))
        self.assertEqual(_native._test_receiver_counts(), (1, 1))

    def test_a_live_receiver_retains_the_endpoint_wrapper_and_native_endpoint(self):
        with warnings.catch_warnings(record=True) as seen:
            warnings.simplefilter("always", ResourceWarning)
            endpoint = self.connect()
            receiver = self.attach(endpoint)
            del endpoint
            gc.collect()
            self.assertEqual(_native._test_counts(), (1, 0, 0), "the endpoint outlives its dropped wrapper")
            self.assertIsInstance(receiver.endpoint, moq5.Endpoint)
            self.assertFalse(receiver.endpoint.closed)
            receiver.close()
            self.assertEqual(_native._test_receiver_counts(), (1, 1))
            self.assertEqual(_native._test_counts(), (1, 0, 0), "close releases retention; the endpoint stays open")
            endpoint = receiver.endpoint
            endpoint.close()
        self.assertEqual(seen, [])
        self.assertEqual(_native._test_counts(), (1, 1, 1))

    def test_dropping_both_wrappers_destroys_receiver_before_endpoint(self):
        with warnings.catch_warnings(record=True) as seen:
            warnings.simplefilter("always", ResourceWarning)
            endpoint = self.connect()
            receiver = self.attach(endpoint)
            del receiver
            del endpoint
            gc.collect()
        self.assertEqual([str(w.message) for w in seen],
                         ["unclosed moq5.Receiver; use close() or a context manager",
                          "unclosed moq5.Endpoint; use close() or a context manager"])
        self.assertEqual(_native._test_receiver_counts(), (1, 1))
        self.assertEqual(_native._test_counts(), (1, 1, 1))

    def test_receiver_close_is_idempotent_and_refuses_reuse(self):
        with self.connect() as endpoint:
            receiver = self.attach(endpoint)
            track = self.track(receiver)
            receiver.close()
            receiver.close()
            self.assertTrue(receiver.closed)
            self.assertIs(receiver.endpoint, endpoint)
            for call in (lambda: receiver.terminal, receiver.stats, lambda: receiver.wait(0),
                         lambda: receiver.subscribe(track), lambda: receiver.unsubscribe(track),
                         lambda: receiver.track_state(track)):
                with self.subTest(call=call):
                    with self.assertRaises(RuntimeError) as refused:
                        call()
                    self.assertEqual(str(refused.exception), "receiver is closed")
            self.assertEqual(_native._test_receiver_counts(), (1, 1))

    def test_forgotten_receiver_warns_once_and_releases_exactly_once(self):
        with self.connect() as endpoint:
            with warnings.catch_warnings(record=True) as seen:
                warnings.simplefilter("always", ResourceWarning)
                receiver = self.attach(endpoint)
                del receiver
                gc.collect()
            self.assertEqual([w.category for w in seen], [ResourceWarning])
            self.assertEqual(str(seen[0].message), "unclosed moq5.Receiver; use close() or a context manager")
            self.assertEqual(_native._test_receiver_counts(), (1, 1))
        self.assertEqual(_native._test_counts(), (1, 1, 1))

    def test_context_entry_refuses_a_closed_receiver_before_the_body(self):
        with self.connect() as endpoint:
            receiver = self.attach(endpoint)
            receiver.close()
            entered = []
            with self.assertRaises(RuntimeError) as refused:
                with receiver:
                    entered.append(True)
            self.assertEqual(str(refused.exception), "receiver is closed")
            self.assertEqual(entered, [], "the body never ran")
            self.assertEqual(_native._test_receiver_counts(), (1, 1))

    def test_context_entry_refuses_a_foreign_thread_before_the_body(self):
        with self.connect() as endpoint, self.attach(endpoint) as receiver:
            outcome = []

            def other():
                try:
                    with receiver:
                        outcome.append("entered")
                except RuntimeError as refused:
                    outcome.append(str(refused))

            worker = threading.Thread(target=other)
            worker.start()
            worker.join(5)
            self.assertEqual(outcome, ["receiver operation requires its creating thread"])
            self.assertFalse(receiver.closed)
            self.assertEqual(_native._test_receiver_counts(), (1, 0))
            self.assertEqual(_native._test_counts(), (1, 0, 0))

    def test_with_body_failure_is_preserved(self):
        error = LookupError("application failure")
        with self.connect() as endpoint:
            with self.assertRaises(LookupError) as caught:
                with self.attach(endpoint):
                    raise error
            self.assertIs(caught.exception, error)
            self.assertEqual(_native._test_receiver_counts(), (1, 1))

    def test_with_body_and_cleanup_failure_preserves_both(self):
        # The native receiver destroy cannot refuse, and misuse is now refused
        # at context ENTRY, so a real double failure needs a scripted close.
        endpoint = self.connect()
        receiver = self.attach(endpoint)
        original = KeyboardInterrupt("body interrupted")
        cleanup_failure = RuntimeError("scripted close failure")
        with mock.patch.object(moq5.Receiver, "close", side_effect=cleanup_failure):
            with self.assertRaises(BaseExceptionGroup) as caught:
                with receiver:
                    raise original
        self.assertEqual(len(caught.exception.exceptions), 2)
        self.assertIs(caught.exception.exceptions[0], original)
        self.assertIs(caught.exception.exceptions[1], cleanup_failure)
        self.assertFalse(receiver.closed)
        self.assertEqual(_native._test_receiver_counts(), (1, 0))
        receiver.close()
        endpoint.close()
        self.assertEqual(_native._test_receiver_counts(), (1, 1))

    # ---- owner and fork guards --------------------------------------------------

    def test_ordinary_calls_reject_a_foreign_thread_before_native_effect(self):
        with self.connect() as endpoint, self.attach(endpoint) as receiver:
            track = self.track(receiver)
            refusals = []

            def other():
                for call in (lambda: receiver.terminal, receiver.stats, lambda: receiver.wait(0),
                             lambda: receiver.subscribe(track), lambda: receiver.unsubscribe(track),
                             lambda: receiver.track_state(track), receiver.close):
                    try:
                        call()
                    except RuntimeError as refused:
                        refusals.append(str(refused))
                    else:
                        refusals.append("accepted")
                refusals.append(receiver.closed)

            worker = threading.Thread(target=other)
            worker.start()
            worker.join(5)
            self.assertEqual(refusals, ["receiver operation requires its creating thread"] * 7 + [False])
            self.assertEqual(_native._test_subscriptions(), [])
            self.assertFalse(receiver.closed)

    @unittest.skipUnless(hasattr(os, "fork"), "fork is a POSIX-only negative contract")
    def test_forked_receiver_is_refused_without_native_cleanup(self):
        endpoint = self.connect()
        receiver = self.attach(endpoint)
        try:
            child = os.fork()
            if child == 0:
                try:
                    receiver.close()
                except RuntimeError:
                    pass
                else:
                    os._exit(3)
                with warnings.catch_warnings(record=True) as seen:
                    warnings.simplefilter("always", ResourceWarning)
                    del receiver
                    del endpoint
                    gc.collect()
                if seen or _native._test_receiver_counts() != (1, 0) or _native._test_counts() != (1, 0, 0):
                    os._exit(4)
                os._exit(0)
            _, status = os.waitpid(child, 0)
            self.assertEqual(os.waitstatus_to_exitcode(status), 0)
            self.assertEqual(_native._test_receiver_counts(), (1, 0))
        finally:
            receiver.close()
            endpoint.close()

    # ---- wait ---------------------------------------------------------------------

    def test_wait_priority_table(self):
        # latch, queued events, queued objects, terminal -> expected outcome
        rows = [
            (False, 0, 0, False, moq5.WaitResult.TIMED_OUT),   # nothing arrives: DONE as is
            (True, 1, 0, False, moq5.WaitResult.WOKEN),        # queued work precedes the latch
            (True, 0, 1, False, moq5.WaitResult.WOKEN),
            (True, 0, 0, False, moq5.WaitResult.INTERRUPTED),  # from the endpoint wait, as is
            (True, 0, 0, True, moq5.WaitResult.CLOSED),        # terminal precedes delegation
            (False, 0, 0, True, moq5.WaitResult.CLOSED),
            (False, 1, 0, True, moq5.WaitResult.WOKEN),        # queued work precedes terminal
        ]
        for latch, events, objects, terminal, expected in rows:
            with self.subTest(latch=latch, events=events, objects=objects, terminal=terminal):
                _native._test_reset()
                with self.connect() as endpoint, self.attach(endpoint) as receiver:
                    endpoint.set_interrupted(latch)
                    _native._test_receiver_queue(events, objects)
                    _native._test_receiver_state(terminal, False, 0)
                    self.assertIs(receiver.wait(1_000), expected)

    def test_arrival_during_the_native_wait_pins_both_post_delegation_branches(self):
        with self.connect() as endpoint, self.attach(endpoint) as receiver:
            handle = receiver._Receiver__handle
            # An item enqueued while the endpoint wait is blocked, then a native
            # OK wake: the post-delegation recheck finds it -> WOKEN.
            _native._test_block_wait()

            def arrive_and_wake():
                _native._test_wait_entered()
                _native._test_receiver_queue(0, 1)
                endpoint.wake()

            worker = threading.Thread(target=arrive_and_wake)
            worker.start()
            try:
                self.assertEqual(_native.receiver_wait(handle, 5_000_000), moq5.WaitResult.WOKEN)
            finally:
                worker.join(5)
            self.assertFalse(_native._test_watchdog_fired())
            # The same arrival with the native wait ending in DONE: that slice
            # returns TIMED_OUT as is (no recheck); the next observation is WOKEN.
            _native._test_receiver_queue(0, 0)
            _native._test_wait_result(1)
            _native._test_block_wait()

            def arrive_and_release():
                _native._test_wait_entered()
                _native._test_receiver_queue(0, 1)
                _native._test_unblock()

            worker = threading.Thread(target=arrive_and_release)
            worker.start()
            try:
                self.assertEqual(_native.receiver_wait(handle, 5_000_000), moq5.WaitResult.TIMED_OUT)
            finally:
                worker.join(5)
            self.assertEqual(_native.receiver_wait(handle, 0), moq5.WaitResult.WOKEN)
            self.assertIs(receiver.wait(0), moq5.WaitResult.WOKEN)

    def test_wait_is_sliced_like_the_endpoint_wait(self):
        with self.connect() as endpoint, self.attach(endpoint) as receiver:
            recorded = foundation.ScriptedWait([moq5.WaitResult.TIMED_OUT] * 5, advance_ns_per_call=250_000_000)
            with mock.patch.object(_endpoint, "_monotonic_ns", recorded.clock), \
                    mock.patch.object(_native, "receiver_wait", recorded.native_wait):
                self.assertIs(receiver.wait(1_000_000), moq5.WaitResult.TIMED_OUT)
            self.assertEqual(recorded.slices, [250_000, 250_000, 250_000, 250_000])
            # an already-expired budget still makes exactly one zero-length observation
            recorded = foundation.ScriptedWait([moq5.WaitResult.CLOSED], advance_ns_per_read=10_000)
            with mock.patch.object(_endpoint, "_monotonic_ns", recorded.clock), \
                    mock.patch.object(_native, "receiver_wait", recorded.native_wait):
                self.assertIs(receiver.wait(1), moq5.WaitResult.CLOSED)
            self.assertEqual(recorded.slices, [0])

    def test_wait_native_failure_is_an_error_and_bad_values_have_no_effect(self):
        with self.connect() as endpoint, self.attach(endpoint) as receiver:
            for bad in (-1, 2**63, True, 1.5):
                with self.subTest(bad=bad):
                    with self.assertRaises((TypeError, ValueError)):
                        receiver.wait(bad)
            _native._test_wait_result(-99)
            with self.assertRaises(moq5.MoqError) as caught:
                receiver.wait(1)
            self.assertEqual((caught.exception.code, caught.exception.operation), (-99, "wait"))

    # ---- terminal and stats -------------------------------------------------------

    def test_terminal_snapshot_maps_every_field_and_marks_origin_unknown(self):
        with self.connect() as endpoint, self.attach(endpoint) as receiver:
            terminal = receiver.terminal
            self.assertEqual((terminal.closed, terminal.fatal, terminal.fatal_code), (False, False, 0))
            self.assertIs(terminal.origin, moq5.FatalOrigin.NONE)
            self.assertEqual(terminal.endpoint, moq5.Terminal(0, 0))
            _native._test_receiver_state(True, True, 2**64 - 3)
            _native._test_terminal(2, 2**64 - 1)
            terminal = receiver.terminal
            self.assertEqual((terminal.closed, terminal.fatal, terminal.fatal_code), (True, True, 2**64 - 3))
            self.assertIs(terminal.origin, moq5.FatalOrigin.UNKNOWN)
            self.assertEqual(terminal.endpoint, moq5.Terminal(moq5.TerminalReason.PROTOCOL, 2**64 - 1))
            # PROTOCOL 2 from the endpoint and EVENT_OVERFLOW 2 from the receiver are
            # the SAME observation: the surface cannot tell them apart, so neither
            # value claims an origin.
            _native._test_receiver_state(True, True, 2)
            _native._test_terminal(2, 2)
            first = receiver.terminal
            self.assertEqual(first.fatal_code, moq5.ReceiverFatal.EVENT_OVERFLOW)
            self.assertIs(first.origin, moq5.FatalOrigin.UNKNOWN)
            self.assertEqual(first, receiver.terminal)
            with self.assertRaises(ValueError):
                moq5.ReceiverTerminal(True, True, 2, moq5.Terminal(0, 0), moq5.FatalOrigin.NONE)

    def test_stats_copies_every_field_and_gates_appended_fields_by_stamp(self):
        with self.connect() as endpoint, self.attach(endpoint) as receiver:
            _native._test_stats(1, 2, 3, 4, 5, 6, 7, 8, 9, True, 10, True)
            stats = receiver.stats()
            self.assertEqual(stats, moq5.ReceiverStats(1, 2, 3, 4, 5, 6, 7, 8, 9, True, 10, True))
            _native._test_stats(2**64 - 1, 0, 0, 0, 0, 0, 0, 0, 0, False, 2**64 - 1, False)
            stats = receiver.stats()
            self.assertEqual((stats.objects_received, stats.catalog_drops), (2**64 - 1, 2**64 - 1))
            self.assertFalse(stats.paused)
            # a v0 stamp: appended fields are None, not zero
            _native._test_stats_size(81)
            stats = receiver.stats()
            self.assertEqual((stats.objects_received, stats.paused), (2**64 - 1, False))
            self.assertIsNone(stats.catalog_drops)
            self.assertIsNone(stats.catalog_complete)
            # a stamp covering catalog_drops but not catalog_complete
            _native._test_stats_size(96)
            stats = receiver.stats()
            self.assertEqual(stats.catalog_drops, 2**64 - 1)
            self.assertIsNone(stats.catalog_complete)
            # OK with a malformed stamp is a BINDING fault, never a native code
            for size, detail in ((80, "below"), (200, "exceeds")):
                _native._test_stats_size(size)
                with self.assertRaises(moq5.BindingError) as caught:
                    receiver.stats()
                self.assertNotIsInstance(caught.exception, moq5.MoqError)
                self.assertIn("stats", str(caught.exception))
                self.assertIn(f"{size}", str(caught.exception))
                self.assertIn(detail, str(caught.exception))
            # an ACTUAL native ABI_MISMATCH is a MoqError with that exact code
            _native._test_stats_size(104)
            _native._test_stats_result(-11)
            with self.assertRaises(moq5.MoqError) as caught:
                receiver.stats()
            self.assertEqual((caught.exception.code, caught.exception.operation), (-11, "stats"))
            _native._test_stats_result(0)
            self.assertEqual(receiver.stats().catalog_drops, 2**64 - 1)

    def test_terminal_stamp_faults_are_binding_errors_and_native_codes_are_moq_errors(self):
        with self.connect() as endpoint, self.attach(endpoint) as receiver:
            self.assertEqual(receiver.terminal.endpoint, moq5.Terminal(0, 0))
            _native._test_terminal_size(4)
            with self.assertRaises(moq5.BindingError) as caught:
                receiver.terminal
            self.assertNotIsInstance(caught.exception, moq5.MoqError)
            self.assertIn("terminal", str(caught.exception))
            self.assertIn("4", str(caught.exception))
            _native._test_terminal_size(24)
            _native._test_terminal_result(-11)
            with self.assertRaises(moq5.MoqError) as caught:
                receiver.terminal
            self.assertEqual((caught.exception.code, caught.exception.operation), (-11, "terminal"))
            _native._test_terminal_result(0)
            self.assertEqual(receiver.terminal.endpoint, moq5.Terminal(0, 0))
            self.assertTrue(issubclass(moq5.BindingError, RuntimeError))

    # ---- track commands ---------------------------------------------------------

    def test_subscribe_forwards_start_and_priority_exactly(self):
        with self.connect() as endpoint, self.attach(endpoint) as receiver:
            track = self.track(receiver)
            other = self.track(receiver)
            receiver.subscribe(track)
            receiver.subscribe(other, start=moq5.StartMode.NEXT_GROUP, priority=0)
            receiver.subscribe(track, start=1, priority=255)
            receiver.unsubscribe(other)
            self.assertEqual(_native._test_subscriptions(), [
                ("subscribe", 0, 0, False, 0), ("subscribe", 1, 1, True, 0),
                ("subscribe", 0, 1, True, 255), ("unsubscribe", 1, -1, False, -1)])
            for bad in (dict(start=2), dict(start=True), dict(priority=256), dict(priority=-1), dict(priority=1.0)):
                with self.subTest(bad=bad):
                    with self.assertRaises((TypeError, ValueError)):
                        receiver.subscribe(track, **bad)
            self.assertEqual(len(_native._test_subscriptions()), 4, "bad values have no native effect")

    def test_track_state_maps_known_and_unknown_values(self):
        with self.connect() as endpoint, self.attach(endpoint) as receiver:
            track = self.track(receiver)
            self.assertIs(receiver.track_state(track), moq5.TrackState.DISCOVERED)
            _native._test_track_state(track._capsule, 2, False, False)
            self.assertIs(receiver.track_state(track), moq5.TrackState.ACTIVE)
            _native._test_track_state(track._capsule, 41, False, False)
            self.assertEqual(receiver.track_state(track), 41)
            _native._test_track_state(track._capsule, 2, True, False)
            self.assertIs(receiver.track_state(track), moq5.TrackState.ENDED)

    def test_ended_and_removed_tracks_have_distinct_command_tuples(self):
        # C: ended -> subscribe WRONG_STATE, unsubscribe OK, track_state ENDED;
        #    removed -> all three WRONG_STATE.
        with self.connect() as endpoint, self.attach(endpoint) as receiver:
            ended = self.track(receiver)
            removed = self.track(receiver)
            _native._test_track_state(ended._capsule, 2, True, False)
            _native._test_track_state(removed._capsule, 2, False, True)
            with self.assertRaises(moq5.MoqError) as caught:
                receiver.subscribe(ended)
            self.assertEqual((caught.exception.code, caught.exception.operation), (-5, "subscribe"))
            receiver.unsubscribe(ended)
            self.assertIs(receiver.track_state(ended), moq5.TrackState.ENDED)
            self.assertEqual(_native._test_subscriptions(), [("unsubscribe", 0, -1, False, -1)])
            for call, operation in ((lambda: receiver.subscribe(removed), "subscribe"),
                                    (lambda: receiver.unsubscribe(removed), "unsubscribe"),
                                    (lambda: receiver.track_state(removed), "track_state")):
                with self.subTest(operation=operation):
                    with self.assertRaises(moq5.MoqError) as caught:
                        call()
                    self.assertEqual((caught.exception.code, caught.exception.operation), (-5, operation))
            self.assertEqual(len(_native._test_subscriptions()), 1, "removed-track commands recorded nothing")

    def test_native_command_refusals_keep_their_signed_codes(self):
        with self.connect() as endpoint, self.attach(endpoint) as receiver:
            track = self.track(receiver)
            _native._test_track_state(track._capsule, 0, True, False)
            with self.assertRaises(moq5.MoqError) as caught:
                receiver.subscribe(track)
            self.assertEqual((caught.exception.code, caught.exception.operation), (-5, "subscribe"))
            _native._test_track_state(track._capsule, 0, False, False)
            _native._test_receiver_state(False, True, 1)
            for call, operation in ((lambda: receiver.subscribe(track), "subscribe"),
                                    (lambda: receiver.unsubscribe(track), "unsubscribe")):
                with self.assertRaises(moq5.MoqError) as caught:
                    call()
                self.assertEqual((caught.exception.code, caught.exception.operation), (-4, operation))
            self.assertIs(receiver.track_state(track), moq5.TrackState.ENDED)
            _native._test_receiver_state(False, False, 0)
            _native._test_subscribe_result(-14)
            with self.assertRaises(moq5.MoqError) as caught:
                receiver.subscribe(track)
            self.assertEqual(caught.exception.code, -14)
            self.assertEqual(_native._test_subscriptions(), [])

    def test_cross_receiver_and_closed_receiver_tracks_are_refused_before_native(self):
        with self.connect() as endpoint, self.attach(endpoint) as receiver:
            track = self.track(receiver)
            receiver.close()
        with self.connect() as endpoint, self.attach(endpoint) as other:
            mine = self.track(other)
            for foreign in (track, "track", None):
                for call in (lambda t: other.subscribe(t), lambda t: other.unsubscribe(t), lambda t: other.track_state(t)):
                    with self.subTest(foreign=foreign, call=call):
                        with self.assertRaises((RuntimeError, TypeError)):
                            call(foreign)
            self.assertEqual(_native._test_subscriptions(), [])
            self.assertIs(other.track_state(mine), moq5.TrackState.DISCOVERED)
            # the bridge refuses a foreign capsule too, even when the wrapper is bypassed
            with self.assertRaises(RuntimeError) as refused:
                _native.receiver_track_state(other._Receiver__handle, track._capsule)
            self.assertEqual(str(refused.exception), "track belongs to another receiver")
            self.assertEqual(track.receiver.closed, True)

    def test_track_identity_is_cached_weakly_and_equality_is_stable(self):
        with self.connect() as endpoint, self.attach(endpoint) as receiver:
            handle = receiver._Receiver__handle
            capsule = _native._test_new_track(handle)
            first = receiver._track_from_capsule(capsule)
            self.assertIs(receiver._track_from_capsule(capsule), first)
            self.assertIs(first.receiver, receiver)
            self.assertEqual(repr(first), "<moq5.Track>")
            key = _native.track_key(capsule)
            del first
            gc.collect()
            again = receiver._track_from_capsule(capsule)
            self.assertEqual(_native.track_key(again._capsule), key)
            self.assertEqual(again, moq5.Track(receiver, capsule, key))
            self.assertNotEqual(again, moq5.Track(receiver, capsule, key + 1))
            self.assertEqual(hash(again), hash(moq5.Track(receiver, capsule, key)))

    def test_track_keeps_its_receiver_alive_until_dropped(self):
        with warnings.catch_warnings(record=True) as seen:
            warnings.simplefilter("always", ResourceWarning)
            endpoint = self.connect()
            receiver = self.attach(endpoint)
            track = self.track(receiver)
            del receiver
            gc.collect()
            self.assertEqual(_native._test_receiver_counts(), (1, 0), "a live Track retains its receiver")
            track.receiver.close()
            del track
            gc.collect()
            endpoint.close()
        self.assertEqual(seen, [])
        self.assertEqual(_native._test_receiver_counts(), (1, 1))
        self.assertEqual(_native._test_counts(), (1, 1, 1))


class ReceiverChildTests(unittest.TestCase):
    """Isolated-child rows reusing the foundation's explicit-policy harness:
    a silent child, one JSON report, scalar-only unraisable inventory."""

    CHILD_PRELUDE = foundation.FoundationTests.CHILD_PRELUDE
    run_child = foundation.FoundationTests.run_child
    assert_child = foundation.FoundationTests.assert_child
    parse_report = foundation.FoundationTests.parse_report

    def test_forgotten_receiver_under_warnings_as_error_counts_once(self):
        report = self.assert_child(self.run_child("""
            endpoint = connect()
            receiver = moq5.Receiver.attach(endpoint, moq5.ReceiverConfig.live((b"live",)))
            del receiver
            gc.collect()
            endpoint.close()
            print(json.dumps({"seen": seen, "receivers": _native._test_receiver_counts(),
                              "counts": _native._test_counts()}))
        """))
        self.assertEqual(report["receivers"], [1, 1])
        self.assertEqual(report["counts"], [1, 1, 1])
        self.assertEqual(report["seen"], [{"type": "ResourceWarning",
                                           "message": "unclosed moq5.Receiver; use close() or a context manager",
                                           "object_is_none": True}])

    def test_forgotten_receiver_and_endpoint_finalize_in_native_order(self):
        report = self.assert_child(self.run_child("""
            endpoint = connect()
            receiver = moq5.Receiver.attach(endpoint, moq5.ReceiverConfig.live((b"live",)))
            del endpoint
            del receiver
            gc.collect()
            print(json.dumps({"seen": seen, "receivers": _native._test_receiver_counts(),
                              "counts": _native._test_counts()}))
        """))
        self.assertEqual(report["receivers"], [1, 1])
        self.assertEqual(report["counts"], [1, 1, 1], "the endpoint stop was accepted: the receiver went first")
        self.assertEqual([e["message"] for e in report["seen"]],
                         ["unclosed moq5.Receiver; use close() or a context manager",
                          "unclosed moq5.Endpoint; use close() or a context manager"])


if __name__ == "__main__":
    unittest.main()
