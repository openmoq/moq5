"""Sender shell contract: configuration, attach, and owned lifetime.

Three kinds of row live here.

SUBSTRATE rows exercise the test-only C provider directly through its
observation seams. They run today and prove the oracle is live before any
Python sender exists: the fixture records the exact configuration it was
handed, separates what was DECLARED from what it could safely CAPTURE,
refuses the shapes the SDK refuses, and witnesses every API entry.

BEHAVIOURAL rows are complete tests of the Python surface this slice
specifies -- real calls, expected values and exception identities. They are
gated at run time on that surface existing, so a genuinely missing binding
leaves them skipped behind ONE bootstrap failure. The gate is not a substitute
for the assertions: the sentinel control below supplies three bogus names and
requires every gated row to fail.

CONTROL rows check this file's own machinery and are never evidence about the
product or the native service.

The real C tests own backpressure, queue, catalog and terminal policy; this
file owns only the shell boundary.
"""

import contextlib
import gc
import io
import os
import signal
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import unittest
import warnings
from pathlib import Path
from unittest import mock

import moq5
from moq5 import _native
from moq5._endpoint import _endpoint_handle

# Imported as a module so the loader does not collect the foundation suite twice.
import run_tests
import test_foundation as foundation  # noqa: F401

NS = (b"live", b"cam\x001")
SENDER_SURFACE = ("Sender", "SenderConfig", "Backpressure")

# Native result codes, as the foundation tests use them.
INVAL, NOMEM, WRONG_STATE, CLOSED = -2, -1, -5, -4

# The four policies the SDK's closed set accepts; 0 is UNSET and is refused.
POLICIES = {"DROP_TO_KEYFRAME": 1, "DROP_GROUP": 2,
            "BLOCK_TIMEOUT": 3, "RETURN_WOULD_BLOCK": 4}


def missing_surface():
    return tuple(name for name in SENDER_SURFACE if not hasattr(moq5, name))


def simulate(namespace=(b"live",), catalog=b"cat", backpressure=2,
             block_timeout_us=0, queue_max_objects=0, queue_max_bytes=0,
             pre_ready_max_objects=0, pre_ready_max_bytes=0,
             validate_cmaf=False, publish_tracks=False,
             drop_without_demand=False, catalog_refresh_interval_us=0,
             null_part=-1, struct_size=0):
    """Drive the fixture's own attach with fully declared values.

    `null_part` clears one entry's POINTER in a fully backed array, and
    `struct_size` advertises a short prefix; both exist so the recorder's
    entry safety can be exercised without inaccessible storage.
    """
    return _native._test_sender_simulate_attach(
        tuple(namespace), catalog, backpressure, block_timeout_us,
        queue_max_objects, queue_max_bytes, pre_ready_max_objects,
        pre_ready_max_bytes, validate_cmaf, publish_tracks,
        drop_without_demand, catalog_refresh_interval_us, null_part, struct_size)


class _FailingConstruction:
    """Stands in for `object` inside the sender module so the WRAPPER stage
    itself fails. The C sites use the named allocation seam instead."""

    @staticmethod
    def __new__(cls, *args, **kwargs):
        raise MemoryError("sender wrapper construction")


@contextlib.contextmanager
def failing_site(site):
    """Arm exactly one named pre-attach stage to fail, then disarm it."""
    if site == "sender_object":
        import moq5._sender as module
        module.__dict__["object"] = _FailingConstruction
        try:
            yield
        finally:
            module.__dict__.pop("object", None)
    else:
        _native._test_fail_allocation_at(site)
        try:
            yield
        finally:
            _native._test_fail_allocation_at(None)


def authority_program():
    """The compiled SDK authority beside the lane's build tree, if it is there."""
    candidates = [c for parent in list(Path(moq5.__file__).resolve().parents)[:4]
                  for c in sorted(parent.glob("sender_cfg_authority"))]
    return [str(candidates[0])] if candidates else None


def sdk_authority(command=None):
    """Run the program built against the REAL SDK and return its report.

    It is the independent authority for the sender configuration initializer:
    the fixture mirrors that initializer, and this is what the mirror is
    checked against. Every value it prints is derived from C.

    Its contract is the declared stdout record, a zero status AND silence on
    stderr. A C program's or a sanitizer's diagnostic is not a Python warning,
    so `-W error` would never see it; any stderr output fails by name here.
    """
    if command is None:
        command = authority_program()
        if command is None:
            return None
    env = os.environ.copy()
    insert = env.pop("MOQ5_TEST_CHILD_DYLD_INSERT_LIBRARIES", None)
    if insert:
        env["DYLD_INSERT_LIBRARIES"] = insert
    report = subprocess.run(command, capture_output=True, text=True,
                            timeout=30, env=env)
    if report.returncode != 0:
        raise AssertionError(
            f"the SDK authority failed with status {report.returncode}: "
            f"{report.stderr.strip()}")
    if report.stderr:
        raise AssertionError(
            "the SDK authority wrote a diagnostic to stderr: "
            f"{report.stderr.strip()}")
    values = {}
    for line in report.stdout.split("\n"):
        if line:
            name, _, value = line.partition(" ")
            values[name] = int(value)
    return values


def reap_child(pid, timeout=10.0):
    """Wait for one owned child within a finite bound.

    Returns its exit code, or None when the bound elapsed -- in which case the
    child is killed and reaped here, so a stuck worker never outlives the
    fixture. This is a two-call envelope, not a process framework.
    """
    deadline = time.monotonic() + timeout
    while True:
        done, status = os.waitpid(pid, os.WNOHANG)
        if done == pid:
            return os.waitstatus_to_exitcode(status)
        if time.monotonic() >= deadline:
            os.kill(pid, signal.SIGKILL)
            os.waitpid(pid, 0)
            return None
        time.sleep(0.01)


class SenderFixtureBase(unittest.TestCase):
    def setUp(self):
        gc.collect()
        _native._test_reset()
        _native._test_sender_reset()

    def connect(self):
        return moq5.Endpoint.connect(moq5.EndpointConfig(url="moqt://fixture.invalid"))


# ---------------------------------------------------------------- substrate --

class SenderSubstrateTests(SenderFixtureBase):
    """The fixture provider and its seams, exercised without the binding."""

    def test_a_reset_reports_an_empty_sender_inventory(self):
        self.assertEqual(_native._test_sender_counts(), {
            "attach_entries": 0, "attached": 0,
            "destroy_entries": 0, "destroyed": 0, "live": False})
        self.assertIsNone(_native._test_sender_config())
        self.assertEqual(_native._test_sender_log(), ())

    def test_b_attach_records_every_field_with_non_default_values(self):
        with self.connect():
            self.assertEqual(simulate(
                namespace=(b"live", b"cam\x001"), catalog=b"c\x00t",
                backpressure=3, block_timeout_us=7_000_001,
                queue_max_objects=11, queue_max_bytes=12,
                pre_ready_max_objects=13, pre_ready_max_bytes=14,
                validate_cmaf=True, publish_tracks=True,
                drop_without_demand=True,
                catalog_refresh_interval_us=2**64 - 1), 0)
            cfg = _native._test_sender_config()
            _native._test_sender_simulate_destroy()
        self.assertEqual(cfg["namespace"], (b"live", b"cam\x001"))
        self.assertEqual(cfg["namespace_declared_count"], 2)
        self.assertEqual(cfg["namespace_declared_lengths"], (4, 5))
        self.assertEqual(cfg["catalog_track"], b"c\x00t")
        self.assertEqual(cfg["catalog_declared_length"], 3)
        self.assertEqual(cfg["backpressure"], 3)
        self.assertEqual(cfg["block_timeout_us"], 7_000_001)
        self.assertEqual(cfg["queue_max_objects"], 11)
        self.assertEqual(cfg["queue_max_bytes"], 12)
        self.assertEqual(cfg["pre_ready_max_objects"], 13)
        self.assertEqual(cfg["pre_ready_max_bytes"], 14)
        self.assertIs(cfg["validate_cmaf"], True)
        self.assertIs(cfg["publish_tracks"], True)
        self.assertIs(cfg["drop_without_demand"], True)
        self.assertEqual(cfg["catalog_refresh_interval_us"], 2**64 - 1)
        # attach borrows an endpoint and installs nothing of its own
        self.assertIs(cfg["endpoint_null"], True)
        self.assertIs(cfg["full_size"], True)
        self.assertIs(cfg["callbacks_absent"], True)
        self.assertIs(cfg["content_protections_absent"], True)
        # This helper declares an explicitly zero-initialized configuration,
        # NOT one built through the sized initializer, so the nested stamp is
        # zero here. The bridge's own path carries the initializer's stamp and
        # is asserted separately; absence never depends on either value.
        self.assertEqual(cfg["callbacks_struct_size"], 0)
        # a completely retained input never reports a fixture limit
        self.assertIs(cfg["oracle_limit"], False)
        self.assertIsNone(cfg["oracle_limit_reason"])
        self.assertIs(cfg["shape_refused"], False)

    def test_c_backpressure_is_a_closed_set_of_four(self):
        with self.connect():
            for value in (0, 5, 99, -1):
                with self.subTest(backpressure=value):
                    self.assertEqual(simulate(backpressure=value), INVAL)
            for value in POLICIES.values():
                with self.subTest(backpressure=value):
                    self.assertEqual(simulate(backpressure=value), 0)
                    _native._test_sender_simulate_destroy()

    def test_d_stopped_and_occupied_are_distinct_states(self):
        endpoint = self.connect()
        self.assertEqual(simulate(), 0)
        # the endpoint refuses to stop while the attachment is held
        with self.assertRaises(moq5.MoqError) as held:
            endpoint.close()
        self.assertEqual(held.exception.code, WRONG_STATE)
        # a second attachment is refused while the slot is occupied
        self.assertEqual(simulate(), WRONG_STATE)
        _native._test_sender_simulate_destroy()
        # the slot is free again once the attachment is released
        self.assertEqual(simulate(), 0)
        _native._test_sender_simulate_destroy()
        # a TERMINAL endpoint is a different refusal from an occupied slot
        _native._test_snapshot(int(moq5.EndpointState.CLOSED), 0)
        self.assertEqual(simulate(), CLOSED)
        _native._test_snapshot(int(moq5.EndpointState.ESTABLISHED), 0)
        endpoint.close()

    def test_e_a_scripted_failure_still_witnesses_the_call_and_the_config(self):
        with self.connect():
            _native._test_sender_attach_result(NOMEM)
            self.assertEqual(simulate(namespace=(b"only",)), NOMEM)
            _native._test_sender_attach_result(0)
            counts = _native._test_sender_counts()
            cfg = _native._test_sender_config()
        self.assertEqual(counts["attach_entries"], 1)   # the call happened
        self.assertEqual(counts["attached"], 0)         # nothing was attached
        self.assertIs(counts["live"], False)
        self.assertEqual(cfg["namespace"], (b"only",))  # and was recorded

    def test_f_argument_refusal_and_fixture_limit_are_distinct(self):
        with self.connect():
            # fits: captured completely, no limit
            self.assertEqual(simulate(namespace=(b"x" * 256,), catalog=b"y" * 256), 0)
            fits = _native._test_sender_config()
            _native._test_sender_simulate_destroy()
            # does not fit the fixture: accepted by the shell contract, but the
            # oracle says so by name instead of comparing a truncated prefix
            _native._test_sender_reset()
            self.assertEqual(simulate(namespace=(b"x" * 300,), catalog=b"c"), 0)
            over = _native._test_sender_config()
            _native._test_sender_simulate_destroy()
        self.assertIs(fits["oracle_limit"], False)
        self.assertEqual(fits["namespace"], (b"x" * 256,))
        self.assertEqual(fits["catalog_track"], b"y" * 256)
        self.assertIs(over["oracle_limit"], True)
        self.assertIn("namespace part", over["oracle_limit_reason"])
        self.assertEqual(over["namespace_declared_lengths"], (300,))
        self.assertEqual(over["namespace"], ())        # nothing unsafe is read back

    def test_g_an_empty_namespace_part_is_an_argument_refusal(self):
        with self.connect():
            self.assertEqual(simulate(namespace=(b"",)), INVAL)
            self.assertEqual(simulate(namespace=()), INVAL)
            cfg = _native._test_sender_config()
            counts = _native._test_sender_counts()
        self.assertIs(cfg["shape_refused"], False)      # coherent, just invalid
        self.assertEqual(counts["attach_entries"], 2)
        self.assertEqual(counts["attached"], 0)

    def test_h_the_lifecycle_log_orders_entry_attachment_and_destroy(self):
        with self.connect():
            self.assertEqual(simulate(), 0)
            _native._test_sender_simulate_destroy()
            log = _native._test_sender_log()
        self.assertEqual(log, ("sender_attach", "sender_attached", "sender_destroy"))

    def test_i_terminal_snapshots_fall_back_to_the_endpoint(self):
        """Read the fixture's OWN is_ready/is_fatal/fatal_code, not a proxy."""
        with self.connect():
            self.assertEqual(simulate(), 0)
            try:
                # neither fatal: ready follows the knob, code is the endpoint's
                _native._test_sender_state(True, False, 0)
                _native._test_terminal(int(moq5.TerminalReason.NONE), 0)
                snap = _native._test_sender_snapshot()
                self.assertIs(snap["ready"], True)
                self.assertIs(snap["fatal"], False)
                self.assertEqual(snap["fatal_code"], 0)
                # endpoint fatal, sender not: BOTH answers fall back to it
                _native._test_terminal(int(moq5.TerminalReason.PROTOCOL), 77)
                snap = _native._test_sender_snapshot()
                self.assertIs(snap["fatal"], True)
                self.assertEqual(snap["fatal_code"], 77)
                # sender fatal takes precedence, with its own distinct code
                _native._test_sender_state(True, True, 42)
                snap = _native._test_sender_snapshot()
                self.assertIs(snap["fatal"], True)
                self.assertEqual(snap["fatal_code"], 42)
                self.assertIs(snap["ready"], False)   # fatal is never ready
                # terminal is not the owner releasing the handle
                self.assertIs(snap["closed"], False)
            finally:
                _native._test_sender_simulate_destroy()

    def test_j_a_short_advertised_prefix_is_refused_before_any_member_is_read(self):
        """A fixture restriction, reported by name -- not a public C ABI rule."""
        with self.connect():
            self.assertEqual(simulate(struct_size=8), INVAL)
            cfg = _native._test_sender_config()
            counts = _native._test_sender_counts()
        self.assertIs(cfg["full_size"], False)
        self.assertIs(cfg["unsupported_prefix"], True)
        self.assertIn("full current", cfg["unsupported_prefix_reason"])
        # nothing beyond the size stamp was recorded
        self.assertEqual(cfg["namespace"], ())
        self.assertEqual(cfg["namespace_declared_count"], 0)
        self.assertEqual(cfg["backpressure"], 0)
        self.assertEqual(counts["attach_entries"], 1)
        self.assertEqual(counts["attached"], 0)

    def test_k_an_incoherent_span_is_refused_without_being_read(self):
        with self.connect():
            # within capture capacity
            self.assertEqual(simulate(namespace=(b"a", b"b"), null_part=1), INVAL)
            near = _native._test_sender_config()
            # BEYOND capture capacity, in a fully backed 40-part array
            _native._test_sender_reset()
            parts = tuple(bytes([65 + (i % 26)]) * 3 for i in range(40))
            self.assertEqual(simulate(namespace=parts, null_part=35), INVAL)
            far = _native._test_sender_config()
            counts = _native._test_sender_counts()
        for cfg in (near, far):
            self.assertIs(cfg["shape_refused"], True)
            self.assertIn("NULL pointer", cfg["shape_refused_reason"])
        self.assertIs(far["oracle_limit"], True)              # 40 > capacity
        self.assertEqual(far["namespace_declared_count"], 40)
        self.assertEqual(counts["attached"], 0)

    def test_l0_capture_is_the_current_call_and_never_adopts_old_bytes(self):
        """Consecutive calls: no slot may survive from an earlier call."""
        with self.connect():
            self.assertEqual(simulate(namespace=(b"old0", b"old1")), 0)
            _native._test_sender_simulate_destroy()
            # a refused call must not show any part of the previous image
            self.assertEqual(
                simulate(namespace=(b"new0", b"new1", b"new2"), null_part=1), INVAL)
            refused = _native._test_sender_config()
            # a short prefix records nothing beyond the size stamp
            self.assertEqual(simulate(struct_size=8), INVAL)
            short = _native._test_sender_config()
        self.assertNotIn(b"old1", refused["namespace"])
        self.assertEqual(refused["namespace"], (b"new0",))   # the captured prefix only
        self.assertEqual(refused["namespace_declared_count"], 3)
        self.assertIs(refused["shape_refused"], True)
        self.assertEqual(short["namespace"], ())
        self.assertEqual(short["namespace_declared_count"], 0)
        self.assertEqual(short["catalog_track"], b"")
        self.assertEqual(short["backpressure"], 0)
        self.assertIs(short["unsupported_prefix"], True)

    def test_l_a_sticky_witness_never_decides_a_later_call(self):
        with self.connect():
            self.assertEqual(simulate(namespace=(b"a",), null_part=0), INVAL)
            poisoned = _native._test_sender_config()
            # the SAME oracle, not reset: a valid call must still succeed
            self.assertEqual(simulate(namespace=(b"a",)), 0)
            after = _native._test_sender_config()
            _native._test_sender_simulate_destroy()
        self.assertIs(poisoned["shape_refused"], True)
        self.assertIs(after["shape_refused"], True)   # the witness is sticky
        self.assertEqual(after["namespace"], (b"a",))  # and the call still passed


# ------------------------------------------------------------- behavioural --

class SenderGatedBase(SenderFixtureBase):
    """Skipped only while the surface is absent; never a substitute for the body."""

    def setUp(self):
        missing = missing_surface()
        if missing:
            self.skipTest(f"moq5 sender surface absent: {', '.join(missing)}")
        # A name that is present but inert is not the surface. Refuse here, by
        # a named assertion and before the row acquires an endpoint, so the
        # inert-name control sees an intended failure rather than an arbitrary
        # AttributeError and a leaked endpoint.
        self.assertIsInstance(moq5.Sender, type, "moq5.Sender is not a type")
        self.assertIsInstance(moq5.SenderConfig, type,
                              "moq5.SenderConfig is not a type")
        self.assertTrue(callable(getattr(moq5.Sender, "attach", None)),
                        "moq5.Sender.attach is not callable")
        self.assertTrue(hasattr(moq5.Backpressure, "DROP_GROUP"),
                        "moq5.Backpressure has no DROP_GROUP")
        super().setUp()

    def config(self, **fields):
        fields.setdefault("namespace", NS)
        fields.setdefault("backpressure", moq5.Backpressure.DROP_GROUP)
        return moq5.SenderConfig(**fields)

    def attach(self, endpoint, **fields):
        return moq5.Sender.attach(endpoint, self.config(**fields))


class SenderConfigTests(SenderGatedBase):
    def test_backpressure_has_the_four_native_policies_and_refuses_unset(self):
        self.assertEqual({m.name: m.value for m in moq5.Backpressure}, POLICIES)
        for bad in (0, 5, -1):
            with self.subTest(value=bad), self.assertRaises(ValueError):
                moq5.Backpressure(bad)

    def test_config_copies_namespace_and_catalog_bytes(self):
        parts = [b"live", b"cam"]
        cfg = self.config(namespace=parts, catalog_track=b"cat")
        parts.append(b"extra")
        self.assertEqual(cfg.namespace, (b"live", b"cam"))
        self.assertIsInstance(cfg.namespace, tuple)
        self.assertEqual(cfg.catalog_track, b"cat")

    def test_config_rejects_out_of_range_and_wrongly_typed_fields(self):
        cases = (
            ({"namespace": ()}, ValueError),
            ({"namespace": (b"",)}, ValueError),
            ({"namespace": ("text",)}, TypeError),
            ({"catalog_track": "cat"}, TypeError),
            ({"block_timeout_us": 2**64}, ValueError),
            ({"block_timeout_us": -1}, ValueError),
            ({"queue_max_objects": 2**32}, ValueError),
            ({"queue_max_bytes": 2**32}, ValueError),
            ({"pre_ready_max_objects": 2**32}, ValueError),
            ({"pre_ready_max_bytes": 2**32}, ValueError),
            ({"catalog_refresh_interval_us": 2**64}, ValueError),
            ({"validate_cmaf": 1}, TypeError),
            ({"publish_tracks": 1}, TypeError),
            ({"drop_without_demand": 1}, TypeError),
            ({"backpressure": 99}, ValueError),
        )
        for fields, expected in cases:
            with self.subTest(**fields), self.assertRaises(expected):
                self.config(**fields)

    def test_config_defaults_mean_native_default(self):
        cfg = self.config()
        self.assertEqual(cfg.catalog_track, b"")
        for field in ("block_timeout_us", "queue_max_objects", "queue_max_bytes",
                      "pre_ready_max_objects", "pre_ready_max_bytes",
                      "catalog_refresh_interval_us"):
            self.assertEqual(getattr(cfg, field), 0, field)
        self.assertIs(cfg.validate_cmaf, True)      # the C preset is strict
        self.assertIs(cfg.publish_tracks, False)
        self.assertIs(cfg.drop_without_demand, False)
        self.assertEqual(moq5.CATALOG_REFRESH_DISABLED, 2**64 - 1)
        self.assertIs(moq5.SenderConfig.live(NS).backpressure,
                      moq5.Backpressure.DROP_TO_KEYFRAME)
        self.assertIs(moq5.SenderConfig.lossless(NS).backpressure,
                      moq5.Backpressure.BLOCK_TIMEOUT)

    def test_config_is_frozen(self):
        cfg = self.config()
        with self.assertRaises((AttributeError, TypeError)):
            cfg.queue_max_objects = 1


class SenderAttachTests(SenderGatedBase):
    def test_validation_precedes_every_native_effect(self):
        with self.connect() as endpoint:
            with self.assertRaises(ValueError):
                self.attach(endpoint, namespace=())
            counts = _native._test_sender_counts()
        self.assertEqual(counts["attach_entries"], 0)   # native was never entered
        self.assertEqual(counts["attached"], 0)

    def test_attach_passes_the_declared_configuration(self):
        with self.connect() as endpoint:
            sender = self.attach(
                endpoint, namespace=(b"live", b"cam\x001"), catalog_track=b"c\x00t",
                backpressure=moq5.Backpressure.BLOCK_TIMEOUT,
                block_timeout_us=7_000_001, queue_max_objects=11,
                queue_max_bytes=12, pre_ready_max_objects=13,
                pre_ready_max_bytes=14, validate_cmaf=False,
                publish_tracks=True, drop_without_demand=True,
                catalog_refresh_interval_us=moq5.CATALOG_REFRESH_DISABLED)
            cfg = _native._test_sender_config()
            sender.close()
        self.assertEqual(cfg["namespace"], (b"live", b"cam\x001"))
        self.assertEqual(cfg["catalog_track"], b"c\x00t")
        self.assertEqual(cfg["backpressure"], POLICIES["BLOCK_TIMEOUT"])
        self.assertEqual(cfg["block_timeout_us"], 7_000_001)
        self.assertEqual(cfg["queue_max_objects"], 11)
        self.assertEqual(cfg["queue_max_bytes"], 12)
        self.assertEqual(cfg["pre_ready_max_objects"], 13)
        self.assertEqual(cfg["pre_ready_max_bytes"], 14)
        self.assertIs(cfg["validate_cmaf"], False)
        self.assertIs(cfg["publish_tracks"], True)
        self.assertIs(cfg["drop_without_demand"], True)
        self.assertEqual(cfg["catalog_refresh_interval_us"], 2**64 - 1)
        self.assertIs(cfg["endpoint_null"], True)
        self.assertIs(cfg["callbacks_absent"], True)
        self.assertIs(cfg["content_protections_absent"], True)
        # Absence is about the pointers. The bridge builds its configuration
        # through the SDK's own sized initializer, so the nested callbacks
        # struct carries that initializer's real stamp, not a zero.
        authority = sdk_authority()
        self.assertIsNotNone(authority, "the SDK authority program was not built")
        self.assertEqual(cfg["callbacks_struct_size"], authority["callbacks_size"],
                         "the bridge did not initialize the nested callbacks")
        self.assertIs(cfg["oracle_limit"], False)
        # the binding promises the full current configuration size
        self.assertIs(cfg["full_size"], True)
        self.assertIs(cfg["unsupported_prefix"], False)

    def test_a_failing_wrapper_slot_store_never_strands_an_attachment(self):
        """Slot commitment is part of the transaction, not a step after it.

        `Sender` is subclassable and a subclass's `__setattr__` may raise, so
        an ordinary slot store is not universally infallible. Both slots must
        be committed while nothing is attached.
        """
        for slot in ("_Sender__handle", "_Sender__endpoint"):
            with self.subTest(slot=slot):
                _native._test_sender_reset()
                _native._test_reset()
                failing = slot

                class Throwing(moq5.Sender):
                    def __setattr__(self, name, value):
                        if name == failing:
                            raise MemoryError("wrapper slot store")
                        object.__setattr__(self, name, value)

                endpoint = self.connect()
                try:
                    with self.assertRaises(MemoryError):
                        Throwing.attach(endpoint, self.config())
                    counts = _native._test_sender_counts()
                    self.assertEqual(counts["attach_entries"], 0,
                                     "the native attach ran before the slot store")
                    self.assertEqual(counts["attached"], 0)
                    self.assertEqual(counts["destroy_entries"], 0,
                                     "an attachment was made and then undone")
                    self.assertIs(counts["live"], False)
                    self.assertIs(endpoint.closed, False)
                finally:
                    with warnings.catch_warnings(record=True) as seen:
                        warnings.simplefilter("always", ResourceWarning)
                        del endpoint
                        gc.collect()
                self.assertEqual([type(w.message) for w in seen], [ResourceWarning])
                self.assertEqual(_native._test_counts()[2], 1)

    def test_attach_refusals_keep_the_native_code_and_retain_nothing(self):
        for code in (INVAL, NOMEM, WRONG_STATE, CLOSED):
            with self.subTest(code=code):
                _native._test_reset()
                _native._test_sender_reset()
                endpoint = self.connect()
                sender = None
                try:
                    _native._test_sender_attach_result(code)
                    with self.assertRaises(moq5.MoqError) as raised:
                        sender = self.attach(endpoint)
                    self.assertEqual(raised.exception.code, code)
                    self.assertEqual(raised.exception.operation, "attach")
                    del raised                       # no traceback reference
                    counts = _native._test_sender_counts()
                    self.assertEqual(counts["attached"], 0)
                    self.assertIs(counts["live"], False)
                    self.assertIsNotNone(endpoint.state)  # still usable
                finally:
                    _native._test_sender_attach_result(0)
                    if sender is not None:
                        sender.close()
                    # Released, never closed: only an endpoint nothing refers to
                    # is finalized, so a reference the refusal path forgot to
                    # drop shows up as a missing destroy instead of being masked
                    # by an explicit close.
                    with warnings.catch_warnings(record=True) as seen:
                        warnings.simplefilter("always", ResourceWarning)
                        del endpoint, sender
                        gc.collect()
                self.assertEqual([type(w.message) for w in seen], [ResourceWarning])
                self.assertEqual(_native._test_counts()[2], 1)

    def test_a_refused_attach_releases_its_endpoint_even_if_the_error_is_kept(self):
        """A refusal must drop the retention itself, not wait to be finalized.

        The raised MoqError's traceback keeps the refused wrapper, its capsule
        and the attach frame reachable, so finalization proves nothing here.
        The endpoint capsule's own reference count does.
        """
        _native._test_sender_reset()
        _native._test_reset()
        kept = None
        with self.connect() as endpoint:
            capsule = _endpoint_handle(endpoint)
            before = sys.getrefcount(capsule)
            try:
                _native._test_sender_attach_result(WRONG_STATE)
                try:
                    self.attach(endpoint)
                except moq5.MoqError as error:
                    kept = error
            finally:
                _native._test_sender_attach_result(0)
            self.assertIsNotNone(kept, "the scripted refusal did not raise")
            self.assertIsNotNone(kept.__traceback__, "the wrapper is unreachable")
            self.assertEqual(
                sys.getrefcount(capsule), before,
                "the refused attachment still retains the endpoint capsule")
        self.assertEqual(_native._test_counts()[2], 1)

    def test_python_allocation_failure_before_attach_leaves_no_attachment(self):
        # The genuinely fallible pre-attach steps of the chosen transaction.
        # Py_NewRef is an ownership change and a slot store does not allocate,
        # so neither appears here.
        for site in ("sender_object", "sender_handle", "sender_capsule"):
            with self.subTest(site=site):
                _native._test_sender_reset()
                _native._test_reset()
                endpoint = self.connect()
                sender = None
                try:
                    with failing_site(site):
                        with self.assertRaises(MemoryError):
                            sender = self.attach(endpoint)
                    counts = _native._test_sender_counts()
                    self.assertEqual(counts["attach_entries"], 0, "native was entered")
                    self.assertEqual(counts["attached"], 0)
                    self.assertIs(counts["live"], False)
                    self.assertIs(endpoint.closed, False)   # still usable
                finally:
                    if sender is not None:
                        sender.close()
                    # Released, never closed: only an endpoint nothing refers to
                    # is finalized, so a reference the refusal path forgot to
                    # drop shows up as a missing destroy instead of being masked
                    # by an explicit close.
                    with warnings.catch_warnings(record=True) as seen:
                        warnings.simplefilter("always", ResourceWarning)
                        del endpoint, sender
                        gc.collect()
                self.assertEqual([type(w.message) for w in seen], [ResourceWarning])
                self.assertEqual(_native._test_counts()[2], 1)

    def test_sender_constructor_is_refused(self):
        # `Sender` must be the class itself, not merely a name that happens to
        # raise when called: an inert object would satisfy the TypeError alone.
        self.assertIsInstance(moq5.Sender, type)
        self.assertTrue(hasattr(moq5.Sender, "attach"))
        with self.assertRaises(TypeError):
            moq5.Sender()


class SenderLifetimeTests(SenderGatedBase):
    def test_sender_retains_its_endpoint_and_identity_holds(self):
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            self.assertIs(sender.endpoint, endpoint)
            sender.close()

    def test_endpoint_outlives_the_callers_reference_and_dies_after_the_sender(self):
        # This row deliberately never closes the endpoint: the release order is
        # what it measures. The finalizer therefore warns, and that warning is
        # DECLARED here rather than escaping as an unraisable diagnostic.
        with warnings.catch_warnings(record=True) as seen:
            warnings.simplefilter("always")
            endpoint = self.connect()
            sender = self.attach(endpoint)
            del endpoint                   # the caller drops its own reference
            gc.collect()
            # the native endpoint is still alive because the sender retains it
            self.assertEqual(_native._test_counts()[2], 0, "endpoint destroyed early")
            self.assertIs(sender.endpoint.closed, False)
            sender.close()
            del sender
            gc.collect()
        self.assertEqual([w.category for w in seen], [ResourceWarning],
                         "unexpected diagnostics while releasing the pair")
        self.assertIn("unclosed moq5.Endpoint", str(seen[0].message))
        log = _native._test_sender_log()
        self.assertIn("sender_destroy", log)
        self.assertIn("endpoint_destroy", log)
        self.assertLess(log.index("sender_destroy"), log.index("endpoint_destroy"),
                        "the endpoint was released before the sender")

    def test_endpoint_close_while_attached_refuses_and_stays_usable(self):
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            with self.assertRaises(moq5.MoqError) as raised:
                endpoint.close()
            self.assertEqual(raised.exception.code, WRONG_STATE)
            self.assertIs(endpoint.closed, False)
            sender.close()
            endpoint.close()

    def test_close_is_destructive_and_idempotent(self):
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            self.assertIs(sender.closed, False)
            sender.close()
            sender.close()
            self.assertIs(sender.closed, True)
            counts = _native._test_sender_counts()
            self.assertEqual(counts["destroyed"], 1)
            # a second destroy(NULL) would be hidden by the success count alone
            self.assertEqual(counts["destroy_entries"], 1)
            endpoint.close()

    def test_forgotten_sender_releases_in_order_with_a_resource_warning(self):
        endpoint = self.connect()
        with self.assertWarns(ResourceWarning):
            self.attach(endpoint)
            gc.collect()
        log = _native._test_sender_log()
        self.assertEqual(log[-1], "sender_destroy")
        self.assertEqual(_native._test_sender_counts()["destroyed"], 1)
        endpoint.close()

    # Exit codes of the owned thread-guard child. A potentially stuck worker
    # lives in a process the parent can end, never beside the parent's fixture.
    GUARD_OK = 0
    GUARD_WORKER_STUCK = 6        # the worker never returned; nothing torn down
    GUARD_NOT_REFUSED = 7         # the guard let a foreign thread through
    GUARD_NATIVE_ENTERED = 8      # a native destroy ran before the guard

    def run_thread_guard_child(self, block):
        """Run the owner-thread guard in an owned child and reap it.

        Returns the child's exit status, or None if the envelope had to end it.
        A worker that never returns ends WITH its process: its fixture is never
        destroyed underneath it.
        """
        child = os.fork()
        if child == 0:
            status = self.GUARD_OK
            try:
                endpoint = self.connect()
                sender = self.attach(endpoint)
                before = _native._test_sender_counts()["destroy_entries"]
                failures = []
                running = threading.Event()

                def other():
                    running.set()
                    if block:
                        while True:            # a guard worker that never returns
                            time.sleep(3600)
                    try:
                        sender.close()
                    except RuntimeError as error:
                        failures.append(error)

                thread = threading.Thread(target=other, daemon=True)
                thread.start()
                running.wait(5.0)
                thread.join(5.0)
                if thread.is_alive():
                    os._exit(self.GUARD_WORKER_STUCK)   # nothing is torn down
                if len(failures) != 1:
                    status = self.GUARD_NOT_REFUSED
                elif _native._test_sender_counts()["destroy_entries"] != before:
                    status = self.GUARD_NATIVE_ENTERED
                else:
                    sender.close()             # the owner itself still may
                    endpoint.close()
            except BaseException:
                traceback.print_exc()
                os._exit(9)
            os._exit(status)
        return reap_child(child)

    @unittest.skipUnless(hasattr(os, "fork"), "fork is a POSIX-only negative contract")
    def test_owner_thread_guard_refuses_without_entering_native(self):
        outcome = self.run_thread_guard_child(block=False)
        self.assertEqual(outcome, self.GUARD_OK,
                         "the owner-thread guard child did not exit cleanly")

    @unittest.skipUnless(hasattr(os, "fork"), "fork is a POSIX-only negative contract")
    def test_a_blocked_guard_worker_ends_with_its_own_process(self):
        """The envelope's named failure, not a fixture destroyed under a
        running worker and not an unbounded join in the parent."""
        outcome = self.run_thread_guard_child(block=True)
        self.assertEqual(outcome, self.GUARD_WORKER_STUCK,
                         "a blocked guard worker was not reported as stuck")
        # the parent's own fixture is untouched by the child's failure
        self.assertEqual(_native._test_sender_counts()["destroy_entries"], 0)

    @unittest.skipUnless(hasattr(os, "fork"), "fork is a POSIX-only negative contract")
    def test_forked_sender_is_refused_without_native_cleanup(self):
        endpoint = self.connect()
        sender = self.attach(endpoint)
        try:
            child = os.fork()
            if child == 0:
                try:
                    sender.close()
                except RuntimeError:
                    pass
                else:
                    os._exit(3)               # the inherited handle was used
                before = _native._test_counts()
                with warnings.catch_warnings(record=True) as seen:
                    warnings.simplefilter("always", ResourceWarning)
                    del sender
                    del endpoint
                    gc.collect()
                counts = _native._test_sender_counts()
                after = _native._test_counts()
                if seen or counts["destroy_entries"] != 0 or counts["destroyed"] != 0:
                    os._exit(4)               # a parent-owned sender was released
                if after != before:
                    os._exit(5)               # a parent-owned endpoint was released
                os._exit(0)
            outcome = reap_child(child)
            self.assertEqual(outcome, 0, "the fork guard child did not exit cleanly")
            # the parent's sender is untouched and still usable
            counts = _native._test_sender_counts()
            self.assertEqual(counts["destroy_entries"], 0)
            self.assertIs(sender.closed, False)
        finally:
            sender.close()
            endpoint.close()

    def test_context_manager_preserves_the_original_exception(self):
        original = ValueError("body")
        with self.connect() as endpoint:
            with self.assertRaises(ValueError) as raised:
                with self.attach(endpoint):
                    raise original
            self.assertIs(raised.exception, original)

    def test_terminal_snapshot_distinguishes_native_terminal_from_closure(self):
        with self.connect() as endpoint, self.attach(endpoint) as sender:
            self.assertIs(sender.ready, False)
            _native._test_sender_state(True, False, 0)
            self.assertIs(sender.ready, True)
            # endpoint fatal while the sender is not: both answers fall back
            _native._test_terminal(int(moq5.TerminalReason.PROTOCOL), 77)
            self.assertIs(sender.terminal.fatal, True)
            self.assertEqual(sender.terminal.fatal_code, 77)
            # the sender's own fatal takes precedence, with its own code
            _native._test_sender_state(False, True, 42)
            self.assertIs(sender.ready, False)
            self.assertIs(sender.terminal.fatal, True)
            self.assertEqual(sender.terminal.fatal_code, 42)
            self.assertIs(sender.closed, False)      # terminal is not closure
            sender.close()
            self.assertIs(sender.closed, True)


# ------------------------------------------------------------------ control --

GATED_CLASSES = ("SenderConfigTests", "SenderAttachTests", "SenderLifetimeTests")


class SenderSurfaceTests(unittest.TestCase):
    def test_sender_surface_is_present(self):
        missing = missing_surface()
        self.assertEqual(
            missing, (),
            "the Python sender shell is not implemented yet; "
            f"moq5 is missing {', '.join(missing)}. Every behavioural row in "
            "this file is skipped until it lands.")


class SenderOracleControlTests(unittest.TestCase):
    """This file's own machinery. Never evidence about the product."""

    def test_e_the_authority_contract_is_status_stdout_and_silence(self):
        """A C program's diagnostic is invisible to `-W error`, so the helper
        itself must refuse one. Positive first, then both negatives, each
        replaying the REAL program's valid record."""
        command = authority_program()
        self.assertIsNotNone(command, "the SDK authority program was not built")
        clean = sdk_authority()                       # positive: real program
        self.assertIn("callbacks_size", clean)

        record = subprocess.run(command, capture_output=True, text=True,
                                timeout=30).stdout
        self.assertTrue(record, "the authority printed no record")

        def stub(exit_code, diagnostic):
            body = (f"import sys\n"
                    f"sys.stdout.write({record!r})\n"
                    f"sys.stderr.write({diagnostic!r})\n"
                    f"raise SystemExit({exit_code})\n")
            return [sys.executable, "-I", "-c", body]

        with self.subTest(case="a diagnostic at exit zero"):
            with self.assertRaises(AssertionError) as raised:
                sdk_authority(stub(0, "warning: injected SDK authority diagnostic\n"))
            self.assertIn("wrote a diagnostic to stderr", str(raised.exception))
            self.assertIn("injected SDK authority diagnostic", str(raised.exception))

        with self.subTest(case="a nonzero status"):
            with self.assertRaises(AssertionError) as raised:
                sdk_authority(stub(3, ""))
            self.assertIn("failed with status 3", str(raised.exception))

        with self.subTest(case="the same record with no diagnostic still parses"):
            self.assertEqual(sdk_authority(stub(0, "")), clean)

    STALE_LEAF = (
        "import unittest\n"
        "import stale_probe\n"
        "\n"
        "\n"
        "class StaleLeaf(unittest.TestCase):\n"
        "    def test_the_selected_package_is_current(self):\n"
        "        self.assertEqual(stale_probe.VALUE, 'NEW')\n")

    def plant_stale_package(self, room):
        """A package whose bytecode is valid by size and timestamp and wrong."""
        stamp = 1758258000              # any fixed time; both writes share it
        package = room / "stale"
        package.mkdir(parents=True)
        probe = package / "stale_probe.py"
        probe.write_text("VALUE = 'OLD'\n")
        os.utime(probe, (stamp, stamp))
        compiled = subprocess.run(
            [sys.executable, "-I", "-c",
             f"import py_compile; py_compile.compile({str(probe)!r}, doraise=True)"],
            capture_output=True, text=True, timeout=60)
        self.assertEqual(compiled.returncode, 0, compiled.stderr)
        probe.write_text("VALUE = 'NEW'\n")      # same size, same timestamp
        os.utime(probe, (stamp, stamp))
        self.assertTrue(list(package.rglob("__pycache__")),
                        "no bytecode was cached beside the source")
        return package

    def run_driver(self, driver, package):
        """Invoke a driver exactly as the registered test command does."""
        return subprocess.run(
            [sys.executable, "-I", "-B", "-W", "error", str(driver), str(package)],
            capture_output=True, text=True, timeout=180)

    def test_f_the_registered_driver_program_never_credits_stale_bytecode(self):
        """The guard is exercised through the driver's own program entry.

        A byte-identical disposable copy of the COMPLETE driver runs beside one
        leaf test, so this stays a two-file run and never re-enters the suite.
        Removing only main's invocation of the guard must fail that leaf on the
        stale value.
        """
        driver = Path(__file__).resolve().parent / "run_tests.py"
        original = driver.read_bytes()
        self.assertTrue(original, "the registered driver is missing")

        with tempfile.TemporaryDirectory() as room:
            room = Path(room)
            package = self.plant_stale_package(room)

            # the incident itself: a plain interpreter still executes the stale
            # bytecode, so the package really is poisoned
            unguarded = subprocess.run(
                [sys.executable, "-I", "-B", "-W", "error", "-c",
                 f"import sys; sys.path.insert(0, {str(package)!r});"
                 f" import stale_probe; print(stale_probe.VALUE)"],
                capture_output=True, text=True, timeout=60)
            self.assertEqual(unguarded.returncode, 0, unguarded.stderr)
            self.assertEqual(unguarded.stderr, "")
            self.assertEqual(unguarded.stdout.strip(), "OLD",
                             "the stale-bytecode incident no longer reproduces")

            lane = room / "lane"
            lane.mkdir()
            copy = lane / "run_tests.py"
            copy.write_bytes(original)
            self.assertEqual(copy.read_bytes(), original,
                             "the disposable driver copy is not byte-identical")
            (lane / "test_stale_leaf.py").write_text(self.STALE_LEAF)

            with self.subTest(case="the real driver, as a program"):
                done = self.run_driver(copy, package)
                self.assertEqual(done.returncode, 0, done.stderr)
                self.assertIn("OK", done.stderr)
                self.assertNotIn("FAILED", done.stderr)
                self.assertEqual(list(package.rglob("__pycache__")), [],
                                 "the driver left a cache beside the package")

            with self.subTest(case="a clean package still passes"):
                clean = room / "clean"
                clean.mkdir()
                (clean / "stale_probe.py").write_text("VALUE = 'NEW'\n")
                done = self.run_driver(copy, clean)
                self.assertEqual(done.returncode, 0, done.stderr)
                self.assertIn("OK", done.stderr)

            with self.subTest(case="main no longer invokes the guard"):
                package = self.plant_stale_package(room / "again")
                invocation = ("    with isolate_bytecode_cache(sys.argv.pop()) "
                              "as selected:\n")
                text = original.decode()
                self.assertEqual(text.count(invocation), 1,
                                 "the driver's guard invocation moved")
                copy.write_text(text.replace(
                    invocation,
                    "    if True:\n"
                    "        selected = Path(sys.argv.pop()).resolve()\n"))
                try:
                    done = self.run_driver(copy, package)
                    self.assertNotEqual(done.returncode, 0,
                                        "bypassing the guard still passed")
                    self.assertIn("FAILED", done.stderr)
                    self.assertIn("'OLD' != 'NEW'", done.stderr)
                finally:
                    copy.write_bytes(original)
                self.assertEqual(copy.read_bytes(), original,
                                 "the mutant was not restored")

    def test_g_the_cache_guard_owns_its_prefix_for_the_whole_run(self):
        """The prefix must outlive the body and not outlive the guard."""
        before = sys.pycache_prefix
        with tempfile.TemporaryDirectory() as room:
            package = Path(room) / "package"
            package.mkdir()
            for case, raised in (("success", None),
                                 ("an exception", ValueError),
                                 ("SystemExit", SystemExit)):
                with self.subTest(case=case):
                    owned = None
                    try:
                        with run_tests.isolate_bytecode_cache(package) as selected:
                            owned = sys.pycache_prefix
                            self.assertEqual(selected, package.resolve())
                            self.assertNotEqual(owned, before)
                            self.assertTrue(Path(owned).is_dir(),
                                            "the prefix does not exist inside the body")
                            if raised is not None:
                                raise raised("the body failed")
                    except (ValueError, SystemExit):
                        pass
                    self.assertFalse(Path(owned).exists(),
                                     "the guard leaked its cache directory")
                    self.assertEqual(sys.pycache_prefix, before,
                                     "the previous cache prefix was not restored")
        self.assertEqual(sys.pycache_prefix, before)

    def test_d_the_fixture_initializer_matches_the_real_sdk(self):
        """The fixture mirrors moq_media_sender_cfg_init_sized. The SDK itself
        is the authority for what that mirror must produce, so the two are
        compared field by field and neither side hardcodes a layout."""
        authority = sdk_authority()
        self.assertIsNotNone(authority, "the SDK authority program was not built")
        # The same program also reports the TRACK configuration for the
        # send-track slice; this control owns the sender-config fields only.
        authority = {k: v for k, v in authority.items() if not k.startswith("track_")}
        mirror = _native._test_sender_cfg_shape()
        self.assertEqual(set(mirror), set(authority), "the two reports disagree on fields")
        for name, expected in sorted(authority.items()):
            with self.subTest(field=name):
                observed = mirror[name]
                self.assertEqual(
                    int(observed), expected,
                    f"the fixture initializer drifted from the SDK at {name}")
        # the nested stamp is a real value, not zero
        self.assertEqual(authority["callbacks_struct_size"], authority["callbacks_size"])
        self.assertGreater(authority["callbacks_size"], 0)

    def test_a_bogus_surface_does_not_satisfy_the_gated_rows(self):
        """Three inert names must make every gated row fail, not pass."""
        sentinels = {name: object() for name in SENDER_SURFACE}
        loader = unittest.TestLoader()
        suite = loader.loadTestsFromNames(
            [f"test_sender.{name}" for name in GATED_CLASSES])
        with warnings.catch_warnings(record=True) as raised:
            warnings.simplefilter("always")
            with mock.patch.multiple(moq5, create=True, **sentinels):
                result = unittest.TextTestRunner(
                    stream=io.StringIO(), verbosity=0).run(suite)
        self.assertGreater(result.testsRun, 0)
        self.assertEqual(result.skipped, [])
        self.assertEqual(result.errors, [],
                         "a gated row raised an arbitrary error instead of "
                         "refusing by a named assertion")
        # subTest failures report several entries for one row, so compare the
        # set of failing rows against the rows that ran.
        failing = {getattr(case, "test_case", case).id()
                   for case, _ in result.failures}
        self.assertEqual(len(failing), result.testsRun,
                         "a gated row passed against inert sentinel objects")
        self.assertEqual([w.category for w in raised], [],
                         "the inert-name control acquired resources")

    @unittest.skipUnless(hasattr(os, "fork"), "fork is a POSIX-only control")
    def test_c_the_child_envelope_bounds_a_non_returning_child(self):
        """A deliberately stuck child is detected and reaped within the bound."""
        child = os.fork()
        if child == 0:
            while True:
                time.sleep(1)
        started = time.monotonic()
        outcome = reap_child(child, timeout=0.5)
        elapsed = time.monotonic() - started
        self.assertIsNone(outcome, "a non-returning child was not detected")
        self.assertLess(elapsed, 10.0)
        # the child is gone: a second wait raises rather than hanging
        with self.assertRaises(ChildProcessError):
            os.waitpid(child, os.WNOHANG)

    def test_b_the_gate_reports_the_missing_names(self):
        self.assertEqual(missing_surface(), tuple(
            n for n in SENDER_SURFACE if not hasattr(moq5, n)))
