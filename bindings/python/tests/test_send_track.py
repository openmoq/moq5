"""RED for the Python send-track slice: SendTrackConfig, SendTrack, add, remove.

None of that surface exists yet. Every behavioural row runs a real body and
fails by a NAMED missing behaviour; none skips and none is a docstring. Inert
names do not turn them green, which the controls at the end pin.

What this file does NOT claim. A missing-surface refusal at the top of a row
proves the gate, not the deeper oracle below it; those oracles are written so
they discriminate once the surface lands, and the report says so. Forcing a
scripted native result proves the binding's translation of that code, never
that the real C operation emits it in ordinary execution.

Scope, with its deferrals named: no write, SendObject, end_track, stats, wait,
completion or demand query; no callbacks into Python, no sender registry, no
asyncio, no new endpoint API, no traffic, no examples. The native SAP hints,
content-protection reference ids, generated SAP and media timelines and the
CMSF alternate group stay unexposed and are asserted ABSENT.

Native contracts pinned here, from the installed headers and source:

  moq_media_sender_add_track    service/include/moq/media_sender.h:380-408
      OK; CLOSED terminal; INVAL malformed cfg, duplicate name, SAP-timeline
      name collision, unresolved content-protection ref, protected CMAF init;
      NOMEM; WOULD_BLOCK retryable while a just-removed name or a generated
      "<name>.sap"/"<name>.timeline" sibling is still held.
  moq_media_sender_remove_track service/include/moq/media_sender.h:410-423
      INVAL NULL/foreign/generated handle; WRONG_STATE already removed;
      CLOSED terminal; INTERRUPTED latch. The handle stays VALID BUT INERT
      until the sender is destroyed.
  moq_media_track_cfg_init      service/src/media_sender.c:2976-2982
      zeroes the whole struct, stamps struct_size, sets is_live true.

MSF-01 rules these mirror, from /Users/jekyll/Projects/MoQ/Spec:
  5.2.18 codec required for audio and video (line 1110)
  5.2.22 maximum bitrate required for audio and video (1149)
  5.2.28 audio sample rate required for audio (1205)
  5.2.29 channel configuration required for audio (1212)
  5.2.35 track duration MUST NOT be present when isLive is true (1270)

Python argument rules are this binding's own and are kept separate from both:
an omitted required argument is TypeError, a present but empty or zero one is
ValueError, and a wrong type is TypeError before any native entry.
"""

import gc
import os
import subprocess
import sys
import threading
import time
import unittest
import warnings
from unittest import mock
from pathlib import Path

import moq5
from moq5 import _native

import test_foundation as foundation  # noqa: F401
from test_sender import reap_child

INVAL = -2
NOMEM = -1
CLOSED = -4
WRONG_STATE = -5
WOULD_BLOCK = -8
INTERRUPTED = -13

NS = (b"svc", b"demo")

UINT32_MAX = 2 ** 32 - 1
UINT64_MAX = 2 ** 64 - 1

SURFACE = ("SendTrackConfig", "SendTrack", "TrackNameBusy")

# Bridge entry points this slice will need. They do not exist yet; the rows
# that name them are REDs against the bridge boundary, not a request to
# implement them in this round.
BRIDGE = ("send_track_prepare", "send_track_activate", "send_track_name",
          "send_track_removed")


def missing_surface(namespace=moq5):
    return tuple(n for n in SURFACE if not hasattr(namespace, n))


def authority_program():
    candidates = [c for parent in list(Path(moq5.__file__).resolve().parents)[:4]
                  for c in sorted(parent.glob("sender_cfg_authority"))]
    return [str(candidates[0])] if candidates else None


def sdk_authority():
    """The REAL SDK's own report: zero status, the declared record, no stderr."""
    command = authority_program()
    if command is None:
        return None
    env = os.environ.copy()
    insert = env.pop("MOQ5_TEST_CHILD_DYLD_INSERT_LIBRARIES", None)
    if insert:
        env["DYLD_INSERT_LIBRARIES"] = insert
    report = subprocess.run(command, capture_output=True, text=True, timeout=30,
                            env=env)
    if report.returncode != 0:
        raise AssertionError(f"the SDK authority failed with status "
                             f"{report.returncode}: {report.stderr.strip()}")
    if report.stderr:
        raise AssertionError("the SDK authority wrote a diagnostic to stderr: "
                             + report.stderr.strip())
    values = {}
    for line in report.stdout.split("\n"):
        if line:
            name, _, value = line.partition(" ")
            values[name] = int(value)
    return values


class SendTrackFixtureBase(unittest.TestCase):
    def setUp(self):
        gc.collect()
        _native._test_reset()
        _native._test_sender_reset()
        _native._test_send_track_reset()

    def connect(self):
        return moq5.Endpoint.connect(moq5.EndpointConfig(url="moqt://fixture.invalid"))

    def attach(self, endpoint):
        return moq5.Sender.attach(
            endpoint,
            moq5.SenderConfig(namespace=NS,
                              backpressure=moq5.Backpressure.DROP_GROUP))


# ---------------------------------------------------------------- substrate --

class SendTrackSubstrateTests(SendTrackFixtureBase):
    """The recorder and its seams, exercised without the Python track layer."""

    def test_a_reset_reports_an_empty_track_inventory(self):
        self.assertEqual(_native._test_send_track_counts(), {
            "add_entries": 0, "added": 0, "remove_entries": 0, "removed": 0,
            "allocated": 0, "live_native": 0, "pool_exhausted": False})
        self.assertIsNone(_native._test_send_track_config())

    def test_b_the_recorder_mirrors_the_sdk_track_initializer(self):
        authority = sdk_authority()
        self.assertIsNotNone(authority, "the SDK authority program was not built")
        mirror = _native._test_send_track_cfg_shape()
        pairs = {
            "cfg_size": "track_cfg_size", "struct_size": "track_struct_size",
            "is_live": "track_is_live", "media_type": "track_media_type",
            "packaging": "track_packaging", "timescale": "track_timescale",
            "bitrate": "track_bitrate",
            "has_track_duration": "track_has_track_duration",
            "has_max_grp_sap": "track_has_max_grp_sap",
            "emit_sap_timeline": "track_emit_sap_timeline",
            "emit_media_timeline": "track_emit_media_timeline",
            "has_alt_group": "track_has_alt_group",
            "content_protection_ref_id_count":
                "track_content_protection_ref_id_count",
        }
        for ours, theirs in sorted(pairs.items()):
            with self.subTest(field=ours):
                self.assertEqual(int(mirror[ours]), authority[theirs],
                                 f"the fixture track initializer drifted at {ours}")
        # These 13 are the fields this slice declares against. They are NOT
        # every field of the native struct, and this control does not claim to
        # compare the rest.
        self.assertEqual(authority["track_is_live"], 1,
                         "the one non-zero default")

    def test_c_a_short_prefix_is_refused_before_any_member_is_read(self):
        # The recorder is a full-current-config fixture; a short advertised
        # prefix is a named oracle limit, never a silent partial image.
        with self.connect():
            self.assertEqual(
                _native._test_send_track_simulate_add(b"v", 8), INVAL)
            cfg = _native._test_send_track_config()
        self.assertIs(cfg["full_size"], False)
        self.assertIs(cfg["oracle_limit"], True)
        self.assertIn("full current track config", cfg["oracle_limit_reason"])
        self.assertEqual(cfg["name"], b"")
        self.assertEqual(cfg["bitrate"], 0)

    def test_d_an_unrepresentable_span_is_a_named_limit_not_a_truncation(self):
        with self.connect():
            self.assertEqual(
                _native._test_send_track_simulate_add(b"n" * 300, 0), 0)
            cfg = _native._test_send_track_config()
            _native._test_send_track_reset()
        self.assertIs(cfg["oracle_limit"], True)
        self.assertIn("name exceeds fixture capacity", cfg["oracle_limit_reason"])
        self.assertEqual(cfg["name"], b"", "a truncated image was exposed")

    def test_e_reset_clears_the_image_and_the_states(self):
        with self.connect():
            self.assertEqual(_native._test_send_track_simulate_add(b"v", 0), 0)
            self.assertEqual(_native._test_send_track_counts()["allocated"], 1)
            _native._test_send_track_reset()
        self.assertIsNone(_native._test_send_track_config())
        self.assertEqual(_native._test_send_track_counts(), {
            "add_entries": 0, "added": 0, "remove_entries": 0, "removed": 0,
            "allocated": 0, "live_native": 0, "pool_exhausted": False})

    def test_f_destroying_the_sender_ends_its_tracks_natively(self):
        # Liveness is OBSERVED through destruction, never certified by a flag
        # that can never change. The accepted sender shell provides the owner.
        with self.connect() as endpoint:
            sender = moq5.Sender.attach(
                endpoint, moq5.SenderConfig(
                    namespace=NS, backpressure=moq5.Backpressure.DROP_GROUP))
            self.assertEqual(_native._test_send_track_simulate_add(b"v", 0), 0)
            self.assertEqual(_native._test_send_track_state(0), {
                "allocated": True, "removed": False, "owner_destroyed": False})
            self.assertEqual(_native._test_send_track_counts()["live_native"], 1)
            sender.close()                       # destroys the native sender
            self.assertEqual(_native._test_send_track_state(0), {
                "allocated": True, "removed": False, "owner_destroyed": True})
            self.assertEqual(_native._test_send_track_counts()["live_native"], 0)


# ------------------------------------------------------------- behavioural --

class SendTrackGatedBase(SendTrackFixtureBase):
    """Never skips. A missing or inert surface is a NAMED failure of the row,
    asserted BEFORE the row acquires an endpoint or a sender."""

    def require_surface(self):
        missing = missing_surface()
        if missing:
            self.fail("the Python send-track surface is not implemented: moq5 "
                      f"is missing {', '.join(missing)}")
        self.assertIsInstance(moq5.SendTrackConfig, type,
                              "SendTrackConfig is not a type")
        self.assertIsInstance(moq5.SendTrack, type, "SendTrack is not a type")
        self.assertTrue(isinstance(moq5.TrackNameBusy, type)
                        and issubclass(moq5.TrackNameBusy, moq5.MoqError),
                        "TrackNameBusy is not a MoqError subclass")
        for method in ("add_track", "remove_track"):
            self.assertTrue(callable(getattr(moq5.Sender, method, None)),
                            f"Sender.{method} is not callable")

    def video(self, **fields):
        fields.setdefault("name", b"v")
        fields.setdefault("media_type", moq5.MediaType.VIDEO)
        fields.setdefault("packaging", moq5.Packaging.RAW)
        fields.setdefault("codec", b"av01")
        fields.setdefault("bitrate", 1_500_000)
        return moq5.SendTrackConfig(**fields)

    def audio(self, **fields):
        fields.setdefault("name", b"a")
        fields.setdefault("media_type", moq5.MediaType.AUDIO)
        fields.setdefault("packaging", moq5.Packaging.RAW)
        fields.setdefault("codec", b"opus")
        fields.setdefault("bitrate", 96_000)
        fields.setdefault("samplerate", 48_000)
        fields.setdefault("channel_config", b"2")
        return moq5.SendTrackConfig(**fields)

    def activate(self, prepared, config):
        """The private bridge's own argument shape, used only by the direct
        boundary rows. The public path is Sender.add_track(config)."""
        return _native.send_track_activate(
            prepared, int(config.media_type), int(config.packaging),
            config.codec, config.bitrate, config.timescale, config.init_data,
            config.role, config.lang, config.is_live, config.width,
            config.height, config.framerate_millis, config.samplerate,
            config.channel_config, config.track_duration_ms)

    def sender_handle(self, sender):
        """The prepared retention layer: the sender's own capsule. Ownership
        held by a prepared track lives HERE, not at the endpoint, so this is
        what a retention oracle must count."""
        return getattr(sender, "_Sender__handle")


class SendTrackConfigTests(SendTrackGatedBase):
    def test_a_video_config_maps_every_declared_field(self):
        self.require_surface()
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                sender.add_track(self.video(
                    name=b"cam\x000", codec=b"av01.0.04M.08", bitrate=2_500_000,
                    timescale=90_000, init_data=b"\x00\x01sps", role=b"main",
                    lang=b"en", is_live=True, width=1920, height=1080,
                    framerate_millis=59_940))
                cfg = _native._test_send_track_config()
            finally:
                sender.close()
        self.assertEqual(cfg["name"], b"cam\x000")
        self.assertEqual(cfg["codec"], b"av01.0.04M.08")
        self.assertEqual(cfg["bitrate"], 2_500_000)
        self.assertEqual(cfg["timescale"], 90_000)
        self.assertEqual(cfg["init_data"], b"\x00\x01sps")
        self.assertEqual(cfg["role"], b"main")
        self.assertEqual(cfg["lang"], b"en")
        self.assertEqual(cfg["media_type"], int(moq5.MediaType.VIDEO))
        self.assertEqual(cfg["packaging"], int(moq5.Packaging.RAW))
        self.assertEqual(cfg["width"], 1920)
        self.assertEqual(cfg["height"], 1080)
        self.assertEqual(cfg["framerate_millis"], 59_940)
        self.assertIs(cfg["is_live"], True)
        self.assertIs(cfg["full_size"], True)
        self.assertIs(cfg["oracle_limit"], False)

    def test_b_cmaf_and_audio_packaging_both_map(self):
        self.require_surface()
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                sender.add_track(self.audio(packaging=moq5.Packaging.CMAF,
                                            samplerate=44_100,
                                            channel_config=b"5.1",
                                            init_data=b"\x00moov"))
                audio = _native._test_send_track_config()
                sender.add_track(self.video(name=b"v2",
                                            packaging=moq5.Packaging.CMAF))
                video = _native._test_send_track_config()
            finally:
                sender.close()
        self.assertEqual(audio["media_type"], int(moq5.MediaType.AUDIO))
        self.assertEqual(audio["packaging"], int(moq5.Packaging.CMAF))
        self.assertEqual(audio["samplerate"], 44_100)
        self.assertEqual(audio["channel_config"], b"5.1")
        self.assertEqual(audio["init_data"], b"\x00moov")
        self.assertEqual(video["packaging"], int(moq5.Packaging.CMAF))
        self.assertEqual(video["media_type"], int(moq5.MediaType.VIDEO))

    def test_c_values_above_thirty_two_bits_survive_intact(self):
        self.require_surface()
        # independently declared, not derived from any observation
        big_bitrate = 4_294_967_296        # UINT32_MAX + 1
        big_rate = 10_000_000_000
        big_duration = UINT64_MAX
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                sender.add_track(self.video(bitrate=big_bitrate,
                                            framerate_millis=big_rate))
                wide = _native._test_send_track_config()
                sender.add_track(self.video(name=b"vod", is_live=False,
                                            track_duration_ms=big_duration))
                vod = _native._test_send_track_config()
                sender.add_track(self.video(name=b"edge", bitrate=UINT64_MAX,
                                            timescale=UINT32_MAX,
                                            width=UINT32_MAX,
                                            samplerate=UINT32_MAX))
                edge = _native._test_send_track_config()
            finally:
                sender.close()
        self.assertEqual(wide["bitrate"], big_bitrate)
        self.assertEqual(wide["framerate_millis"], big_rate)
        self.assertEqual(vod["track_duration_ms"], big_duration)
        self.assertEqual(edge["bitrate"], UINT64_MAX)
        self.assertEqual(edge["timescale"], UINT32_MAX)
        self.assertEqual(edge["width"], UINT32_MAX)
        self.assertEqual(edge["samplerate"], UINT32_MAX)

    def test_d_an_absent_duration_differs_from_a_declared_zero(self):
        self.require_surface()
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                sender.add_track(self.video())                 # absent
                absent = _native._test_send_track_config()
                sender.add_track(self.video(name=b"z", is_live=False,
                                            track_duration_ms=0))
                zero = _native._test_send_track_config()
            finally:
                sender.close()
        self.assertIs(absent["has_track_duration"], False)
        self.assertEqual(absent["track_duration_ms"], 0)
        self.assertIs(zero["has_track_duration"], True,
                      "a declared zero duration was sent as absent")
        self.assertEqual(zero["track_duration_ms"], 0)

    def test_e_unset_optional_fields_reach_the_native_defaults(self):
        self.require_surface()
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                sender.add_track(self.video())
                cfg = _native._test_send_track_config()
            finally:
                sender.close()
        self.assertEqual(cfg["timescale"], 0)   # 0 means the native 1000000
        self.assertEqual(cfg["init_data"], b"")
        self.assertEqual(cfg["role"], b"")
        self.assertEqual(cfg["lang"], b"")
        self.assertEqual(cfg["width"], 0)
        self.assertEqual(cfg["height"], 0)
        self.assertEqual(cfg["framerate_millis"], 0)
        self.assertEqual(cfg["samplerate"], 0)
        self.assertEqual(cfg["channel_config"], b"")
        self.assertIs(cfg["is_live"], True)
        self.assertIs(cfg["has_track_duration"], False)

    def test_f_every_deferred_native_field_is_sent_absent(self):
        self.require_surface()
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                sender.add_track(self.video())
                cfg = _native._test_send_track_config()
            finally:
                sender.close()
        # The named deferrals. The content-protection array is checked as BOTH
        # a NULL pointer and a zero count, so the count alone is never treated
        # as proof about the pointer. These are the deferred fields this
        # recorder observes; it does not claim to observe every native field.
        self.assertIs(cfg["has_max_grp_sap"], False)
        self.assertIs(cfg["has_max_obj_sap"], False)
        self.assertIs(cfg["emit_sap_timeline"], False)
        self.assertIs(cfg["emit_media_timeline"], False)
        self.assertIs(cfg["has_alt_group"], False)
        self.assertEqual(cfg["content_protection_ref_id_count"], 0)
        self.assertIs(cfg["content_protection_ref_ids_null"], True)

    def test_g_bytes_are_the_only_accepted_span_type(self):
        self.require_surface()
        # bytes are immutable, so retention IS identity: there is nothing for
        # the binding to snapshot. The contract is that mutable buffers and
        # text are refused outright, and the retained value compares equal
        # byte for byte, embedded NULs included.
        for value in (bytearray(b"av01"), memoryview(b"av01"), "av01", 1, None):
            with self.subTest(codec=type(value).__name__):
                with self.assertRaises(TypeError):
                    self.video(codec=value)
        payload = b"\x00\xffinit\x00"
        config = self.video(init_data=payload, name=b"n\x00ul")
        self.assertEqual(config.init_data, payload)
        self.assertEqual(len(config.init_data), 7)
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                sender.add_track(config)
                cfg = _native._test_send_track_config()
            finally:
                sender.close()
        self.assertEqual(cfg["init_data"], b"\x00\xffinit\x00")
        self.assertEqual(cfg["name"], b"n\x00ul")
        self.assertEqual(config.init_data, payload, "the config lost its value")

    def test_h_the_config_is_frozen(self):
        self.require_surface()
        config = self.video()
        for field, value in (("bitrate", 1), ("name", b"other"),
                             ("init_data", b"x")):
            with self.subTest(field=field):
                with self.assertRaises(AttributeError):
                    setattr(config, field, value)
        self.assertEqual(config.bitrate, 1_500_000)
        self.assertEqual(config.name, b"v")

    def test_i_an_omitted_required_argument_is_a_type_error(self):
        self.require_surface()
        # Python argument rules, distinct from the MSF value rules below.
        for omit in ("name", "media_type", "packaging", "codec", "bitrate"):
            with self.subTest(omitted=omit):
                fields = dict(name=b"v", media_type=moq5.MediaType.VIDEO,
                              packaging=moq5.Packaging.RAW, codec=b"av01",
                              bitrate=1_500_000)
                del fields[omit]
                with self.assertRaises(TypeError):
                    moq5.SendTrackConfig(**fields)

    def test_j_a_present_but_empty_or_zero_required_value_is_a_value_error(self):
        self.require_surface()
        # MSF-01 5.2.18 and 5.2.22 as VALUE rules, mirrored from native
        with self.subTest(rule="5.2.18 empty codec"):
            with self.assertRaises(ValueError):
                self.video(codec=b"")
        with self.subTest(rule="5.2.22 zero bitrate"):
            with self.assertRaises(ValueError):
                self.video(bitrate=0)
        with self.subTest(rule="empty name"):
            with self.assertRaises(ValueError):
                self.video(name=b"")
        # MSF-01 5.2.28 and 5.2.29, conditional on the audio media type
        with self.subTest(rule="5.2.28 audio without samplerate"):
            with self.assertRaises(ValueError):
                moq5.SendTrackConfig(name=b"a", media_type=moq5.MediaType.AUDIO,
                                     packaging=moq5.Packaging.RAW, codec=b"opus",
                                     bitrate=96_000, channel_config=b"2")
        with self.subTest(rule="5.2.29 audio without channel_config"):
            with self.assertRaises(ValueError):
                moq5.SendTrackConfig(name=b"a", media_type=moq5.MediaType.AUDIO,
                                     packaging=moq5.Packaging.RAW, codec=b"opus",
                                     bitrate=96_000, samplerate=48_000)

    def test_k_each_bad_type_or_width_is_refused_before_any_native_call(self):
        self.require_surface()
        cases = (
            ("name", "str-not-bytes", TypeError),
            ("bitrate", -1, ValueError),
            ("bitrate", UINT64_MAX + 1, ValueError),
            ("bitrate", 1.5, TypeError),
            ("bitrate", True, TypeError),
            ("timescale", UINT32_MAX + 1, ValueError),
            ("timescale", -1, ValueError),
            ("width", -1, ValueError),
            ("height", UINT32_MAX + 1, ValueError),
            ("framerate_millis", UINT64_MAX + 1, ValueError),
            ("samplerate", UINT32_MAX + 1, ValueError),
            ("is_live", 1, TypeError),
            ("media_type", 99, ValueError),
            ("packaging", 0, ValueError),
            ("track_duration_ms", UINT64_MAX + 1, ValueError),
            ("lang", 7, TypeError),
        )
        for field, value, expected in cases:
            with self.subTest(field=field, value=repr(value)):
                _native._test_send_track_reset()
                with self.assertRaises(expected):
                    self.video(**{field: value})
                self.assertEqual(_native._test_send_track_counts()["add_entries"], 0,
                                 "native was entered for a rejected configuration")

    def test_l_a_live_track_may_not_declare_a_duration(self):
        self.require_surface()
        # MSF-01 5.2.35
        with self.assertRaises(ValueError):
            self.video(is_live=True, track_duration_ms=5_000)
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                sender.add_track(self.video(is_live=False,
                                            track_duration_ms=5_000))
                cfg = _native._test_send_track_config()
            finally:
                sender.close()
        self.assertIs(cfg["is_live"], False)
        self.assertIs(cfg["has_track_duration"], True)
        self.assertEqual(cfg["track_duration_ms"], 5_000)


class SendTrackAddTests(SendTrackGatedBase):
    def test_a_a_successful_add_enters_native_exactly_once(self):
        self.require_surface()
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                track = sender.add_track(self.video())
                counts = _native._test_send_track_counts()
                self.assertIsInstance(track, moq5.SendTrack)
                self.assertIs(track.sender, sender)
                self.assertEqual(track.name, b"v")
                self.assertIs(track.removed, False)
            finally:
                sender.close()
        self.assertEqual(counts["add_entries"], 1)
        self.assertEqual(counts["added"], 1)
        self.assertEqual(counts["allocated"], 1)

    def test_b_each_scripted_refusal_keeps_its_code_and_retains_nothing(self):
        self.require_surface()
        # This forces the binding's translation of each code. It does NOT show
        # the real C add emits them in ordinary execution.
        for code in (INVAL, NOMEM, CLOSED, INTERRUPTED):
            with self.subTest(code=code):
                _native._test_send_track_reset()
                _native._test_reset()
                endpoint = self.connect()
                sender = None
                try:
                    sender = self.attach(endpoint)
                    config = self.video()          # built BEFORE the script
                    _native._test_send_track_results(code, 0)
                    with self.assertRaises(moq5.MoqError) as raised:
                        sender.add_track(config)
                    self.assertEqual(raised.exception.code, code)
                    self.assertEqual(raised.exception.operation, "add_track")
                    self.assertNotIsInstance(raised.exception, moq5.TrackNameBusy)
                    del raised
                    counts = _native._test_send_track_counts()
                    self.assertEqual(counts["add_entries"], 1, "the call is witnessed")
                    self.assertEqual(counts["added"], 0)
                    self.assertEqual(counts["allocated"], 0)
                finally:
                    _native._test_send_track_results(0, 0)
                    if sender is not None:
                        sender.close()
                    endpoint.close()

    def test_c_a_busy_name_is_its_own_retryable_error(self):
        self.require_surface()
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                config = self.video()
                _native._test_send_track_results(WOULD_BLOCK, 0)
                with self.assertRaises(moq5.TrackNameBusy) as raised:
                    sender.add_track(config)
                self.assertEqual(raised.exception.code, WOULD_BLOCK)
                self.assertEqual(raised.exception.operation, "add_track")
                self.assertIsInstance(raised.exception, moq5.MoqError)
                del raised
            finally:
                _native._test_send_track_results(0, 0)
                sender.close()

    def test_d_a_refused_add_releases_its_prepared_retention(self):
        self.require_surface()
        # A prepared track retains its SENDER, so that is the layer a retention
        # oracle must count. The endpoint's count is a transitive layer and
        # would not move for an extra sender reference.
        kept = None
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            # One outer finally covers the configuration, the activation AND
            # the assertions, so no path can leave the script armed or the
            # sender open.
            try:
                handle = self.sender_handle(sender)
                before = sys.getrefcount(handle)
                config = self.video()
                _native._test_send_track_results(INVAL, 0)
                try:
                    sender.add_track(config)
                except moq5.MoqError as error:
                    kept = error
                self.assertIsNotNone(kept, "the scripted refusal did not raise")
                self.assertIsNotNone(kept.__traceback__, "the frames are gone")
                # the traceback still holds ordinary frame references; what must
                # be gone is ownership held by an inactive prepared handle
                self.assertEqual(sys.getrefcount(handle), before,
                                 "the refused add still retains its sender")
                self.assertEqual(_native._test_send_track_counts()["added"], 0)
            finally:
                _native._test_send_track_results(0, 0)
                sender.close()

    def test_e_every_allocation_site_before_add_leaves_no_registration(self):
        self.require_surface()
        # Native add is the LAST fallible step, so a failure at any of these
        # named sites must show zero native entry AND no retained ownership.
        # These are the two GENUINELY fallible acquisitions in the bridge. The
        # declared name is NOT among them: bytes are immutable, so the bridge
        # retains the caller's object by reference and there is no copy that
        # can fail. The wrapper construction boundary is exercised separately
        # below, at the internal construction symbol.
        for site in ("send_track_handle", "send_track_capsule"):
            with self.subTest(site=site):
                _native._test_send_track_reset()
                _native._test_reset()
                endpoint = self.connect()
                sender = None
                try:
                    sender = self.attach(endpoint)
                    handle = self.sender_handle(sender)
                    before = sys.getrefcount(handle)
                    config = self.video()      # built BEFORE the fault is armed
                    try:
                        _native._test_fail_allocation_at(site)
                        with self.assertRaises(MemoryError):
                            sender.add_track(config)
                    finally:
                        _native._test_fail_allocation_at(None)
                    self.assertTrue(_native._test_allocation_site_fired(site),
                                    f"the {site} site never fired")
                    counts = _native._test_send_track_counts()
                    self.assertEqual(counts["add_entries"], 0,
                                     "native add ran before the transaction completed")
                    self.assertEqual(counts["added"], 0)
                    self.assertEqual(sys.getrefcount(handle), before,
                                     "a prepared reference was retained")
                finally:
                    if sender is not None:
                        sender.close()
                    endpoint.close()

    def test_e2_a_wrapper_construction_failure_leaves_no_registration(self):
        self.require_surface()
        # The wrapper is built in Python, so its acquisition boundary is the
        # internal construction helper, not a C allocation site.
        import moq5._send_track as module

        original = module.SendTrack

        class Failing(original):
            @classmethod
            def _prepare(cls, sender, name, handle):
                raise MemoryError("wrapper construction")

        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                handle = self.sender_handle(sender)
                before = sys.getrefcount(handle)
                config = self.video()
                with mock.patch.object(module, "SendTrack", Failing):
                    with self.assertRaises(MemoryError):
                        sender.add_track(config)
                gc.collect()
                counts = _native._test_send_track_counts()
                self.assertEqual(counts["add_entries"], 0,
                                 "native add ran before the wrapper existed")
                self.assertEqual(counts["added"], 0)
                self.assertEqual(sys.getrefcount(handle), before,
                                 "a prepared reference was retained")
            finally:
                try:
                    self.assertIs(module.SendTrack, original,
                                  "the construction symbol was not restored")
                finally:
                    sender.close()

    def test_f_a_wrapper_store_failure_cannot_strand_a_registration(self):
        self.require_surface()
        # The public signature stays add_track(config). The throwing class is
        # injected at the PACKAGE-INTERNAL construction symbol under a scoped
        # patch, which is restored object-identically, so no user-facing
        # wrapper factory or fault-injection parameter exists.
        import moq5._send_track as module

        original = module.SendTrack

        class Throwing(original):
            def __setattr__(self, name, value):
                if name == "_SendTrack__handle":
                    raise MemoryError("wrapper slot store")
                object.__setattr__(self, name, value)

        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                config = self.video()
                with mock.patch.object(module, "SendTrack", Throwing):
                    with self.assertRaises(MemoryError):
                        sender.add_track(config)
                counts = _native._test_send_track_counts()
                self.assertEqual(counts["add_entries"], 0,
                                 "native add ran before the wrapper store")
                self.assertEqual(counts["added"], 0)
            finally:
                try:
                    self.assertIs(module.SendTrack, original,
                                  "the construction symbol was not restored")
                finally:
                    sender.close()

    def test_g_a_retry_after_a_refusal_registers_once(self):
        self.require_surface()
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                config = self.video()
                _native._test_send_track_results(WOULD_BLOCK, 0)
                with self.assertRaises(moq5.TrackNameBusy):
                    sender.add_track(config)
                _native._test_send_track_results(0, 0)
                track = sender.add_track(config)
                self.assertIsInstance(track, moq5.SendTrack)
                counts = _native._test_send_track_counts()
                self.assertEqual(counts["add_entries"], 2)
                self.assertEqual(counts["added"], 1, "the retry registered twice")
                self.assertEqual(counts["allocated"], 1)
            finally:
                _native._test_send_track_results(0, 0)
                sender.close()

    def test_h_direct_construction_is_refused(self):
        self.require_surface()
        with self.assertRaises(TypeError):
            moq5.SendTrack()


class SendTrackBridgeTests(SendTrackGatedBase):
    """The native boundary itself: a Python isinstance check is not enough."""

    def missing_bridge(self):
        return tuple(n for n in BRIDGE if not hasattr(_native, n))

    def test_a_the_bridge_exposes_a_prepare_and_activate_pair(self):
        missing = self.missing_bridge()
        self.assertEqual(missing, (),
                         "the send-track bridge is not implemented: _native is "
                         f"missing {', '.join(missing)}")

    def test_b_a_foreign_capsule_is_rejected_by_the_capsule_type(self):
        self.require_surface()
        self.assertEqual(self.missing_bridge(), (),
                         "the send-track bridge is not implemented")
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                track = sender.add_track(self.video())
                before = _native._test_send_track_counts()
                endpoint_capsule = getattr(endpoint, "_Endpoint__handle")
                sender_capsule = self.sender_handle(sender)
                track_capsule = getattr(track, "_SendTrack__handle")
                receiver = moq5.Receiver.attach(
                    endpoint, moq5.ReceiverConfig.live(NS))
                receiver_capsule = _native._test_new_track(
                    getattr(receiver, "_Receiver__handle"))
                try:
                    for name, wrong in (("endpoint", endpoint_capsule),
                                        ("sender", sender_capsule),
                                        ("receiver track", receiver_capsule)):
                        with self.subTest(capsule=name):
                            with self.assertRaises(ValueError):
                                _native.send_track_name(wrong)
                            self.assertEqual(
                                _native._test_send_track_counts(), before,
                                "a foreign capsule entered the track boundary")
                finally:
                    receiver.close()
                # the right capsule type is accepted
                self.assertEqual(_native.send_track_name(track_capsule), b"v")
                self.assertEqual(_native._test_send_track_counts(), before)
            finally:
                sender.close()

    def test_c_an_inert_prepared_capsule_cannot_be_used_or_activated_twice(self):
        self.require_surface()
        self.assertEqual(self.missing_bridge(), (),
                         "the send-track bridge is not implemented")
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                prepared = _native.send_track_prepare(self.sender_handle(sender),
                                                      b"v")
                before = _native._test_send_track_counts()
                # inert until activated: no native registration exists yet
                with self.assertRaises(RuntimeError):
                    _native.send_track_removed(prepared)
                self.assertEqual(_native._test_send_track_counts(), before)
                self.activate(prepared, self.video())
                with self.assertRaises(RuntimeError):
                    self.activate(prepared, self.video())
                self.assertEqual(
                    _native._test_send_track_counts()["add_entries"], 1,
                    "the repeated activation entered native again")
            finally:
                sender.close()

    def test_e_closing_the_owner_between_prepare_and_activate_is_refused(self):
        self.require_surface()
        self.assertEqual(self.missing_bridge(), (),
                         "the send-track bridge is not implemented")
        endpoint = self.connect()
        sender = self.attach(endpoint)
        try:
            prepared = _native.send_track_prepare(self.sender_handle(sender),
                                                  b"v")
            before = _native._test_send_track_counts()
            sender.close()                      # the owner goes away in between
            with self.assertRaises(RuntimeError):
                self.activate(prepared, self.video())
            self.assertEqual(_native._test_send_track_counts(), before,
                             "activation entered native after the owner closed")
            del prepared
            gc.collect()
        finally:
            endpoint.close()

    def test_d_a_receiver_track_is_rejected_by_the_public_surface(self):
        self.require_surface()
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            receiver = moq5.Receiver.attach(
                endpoint, moq5.ReceiverConfig.live(NS))
            try:
                handle = getattr(receiver, "_Receiver__handle")
                receiver_track = receiver._track_from_capsule(
                    _native._test_new_track(handle))
                with self.assertRaises(TypeError):
                    sender.remove_track(receiver_track)
                self.assertEqual(
                    _native._test_send_track_counts()["remove_entries"], 0,
                    "a receiver track entered the send-track bridge")
            finally:
                receiver.close()
                sender.close()


class SendTrackFailedActivationTests(SendTrackGatedBase):
    """A refused activation is an ATTEMPT, never a registration.

    The capsule is deliberately kept afterwards and every exposed operation is
    driven against it. Each must refuse by the declared RuntimeError, add no
    native entry, and leave no prepared ownership behind. The formerly crashing
    getters run in a bounded child that must exit NORMALLY.
    """

    def prepared_after_refusal(self, sender, code):
        """Prepare, force `code` from the native add, and keep the capsule."""
        handle = _native.send_track_prepare(self.sender_handle(sender), b"v")
        config = self.video()
        raised = None
        try:
            _native._test_send_track_results(code, 0)
            try:
                self.activate(handle, config)
            except _native.Error as error:     # the direct bridge, not _call
                raised = error
        finally:
            _native._test_send_track_results(0, 0)
        self.assertIsNotNone(raised, "the scripted refusal did not raise")
        native_code, operation, _ = raised.args
        self.assertEqual(native_code, code, "the original code was not preserved")
        self.assertEqual(operation, "add_track")
        return handle, raised

    def test_a_every_operation_on_a_refused_track_is_refused_by_name(self):
        self.require_surface()
        for code in (INVAL, WOULD_BLOCK):
            with self.subTest(code=code):
                _native._test_send_track_reset()
                with self.connect() as endpoint:
                    sender = self.attach(endpoint)
                    try:
                        handle, raised = self.prepared_after_refusal(sender, code)
                        before = _native._test_send_track_counts()
                        for label, call in (
                            ("name", lambda: _native.send_track_name(handle)),
                            ("removed", lambda: _native.send_track_removed(handle)),
                            ("activate again",
                             lambda: self.activate(handle, self.video())),
                            ("remove", lambda: _native.sender_remove_track(
                                self.sender_handle(sender), handle)),
                        ):
                            with self.subTest(operation=label):
                                with self.assertRaises(RuntimeError) as refusal:
                                    call()
                                self.assertIn("never registered",
                                              str(refusal.exception))
                        self.assertEqual(_native._test_send_track_counts(), before,
                                         "a refused track entered native again")
                        # the original refusal is still the one that was raised
                        self.assertEqual(raised.args[0], code)
                    finally:
                        sender.close()

    def test_b_a_refused_activation_retains_no_prepared_ownership(self):
        self.require_surface()
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                owner = self.sender_handle(sender)
                before = sys.getrefcount(owner)
                handle, raised = self.prepared_after_refusal(sender, INVAL)
                self.assertIsNotNone(raised.__traceback__, "the frames are gone")
                self.assertEqual(sys.getrefcount(owner), before,
                                 "the refused activation still retains its sender")
                del handle
                gc.collect()
                self.assertEqual(sys.getrefcount(owner), before)
            finally:
                sender.close()

    @unittest.skipUnless(hasattr(os, "fork"), "fork is a POSIX-only envelope")
    def test_c_the_formerly_crashing_getters_exit_normally(self):
        self.require_surface()
        # This exact sequence used to end the process with SIGSEGV. The child
        # must exit NORMALLY with the declared status, not merely non-zero.
        child = os.fork()
        if child == 0:
            status = 0
            try:
                endpoint = self.connect()
                sender = self.attach(endpoint)
                handle, _ = self.prepared_after_refusal(sender, INVAL)
                for call in (lambda: _native.send_track_name(handle),
                             lambda: _native.send_track_removed(handle)):
                    try:
                        call()
                        status = 21          # it returned instead of refusing
                    except RuntimeError:
                        pass
                sender.close()
                endpoint.close()
            except BaseException:
                os._exit(22)
            os._exit(status)
        self.assertEqual(reap_child(child), 0,
                         "the refused-track getters did not exit normally")


class SendTrackCancellationTests(SendTrackGatedBase):
    """Every pre-registration failure cancels the prepared transaction, with
    the exception and its traceback deliberately kept alive."""

    def sender_delta(self, sender, body):
        """Run `body`, keeping whatever it raises, and report the change in the
        SENDER capsule's reference count plus the native inventory."""
        owner = self.sender_handle(sender)
        before = sys.getrefcount(owner)
        counts = _native._test_send_track_counts()
        kept = None
        try:
            body()
        except BaseException as error:          # kept ON PURPOSE
            kept = error
        gc.collect()
        return kept, sys.getrefcount(owner) - before, counts

    def test_a_a_wrapper_construction_failure_cancels_the_preparation(self):
        self.require_surface()
        import moq5._send_track as module
        original = module.SendTrack

        class Failing(original):
            @classmethod
            def _prepare(cls, sender, name, handle):
                raise MemoryError("wrapper construction")

        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                config = self.video()
                with mock.patch.object(module, "SendTrack", Failing):
                    kept, delta, before = self.sender_delta(
                        sender, lambda: sender.add_track(config))
                self.assertIsInstance(kept, MemoryError)
                self.assertIsNotNone(kept.__traceback__, "the traceback was cleared")
                self.assertEqual(delta, 0,
                                 "the cancelled preparation still retains its sender")
                self.assertEqual(_native._test_send_track_counts(), before)
            finally:
                self.assertIs(module.SendTrack, original)
                sender.close()

    def test_b_a_wrapper_store_failure_cancels_the_preparation(self):
        self.require_surface()
        import moq5._send_track as module
        original = module.SendTrack

        class Throwing(original):
            def __setattr__(self, name, value):
                if name == "_SendTrack__handle":
                    raise MemoryError("wrapper slot store")
                object.__setattr__(self, name, value)

        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                config = self.video()
                with mock.patch.object(module, "SendTrack", Throwing):
                    kept, delta, before = self.sender_delta(
                        sender, lambda: sender.add_track(config))
                self.assertIsInstance(kept, MemoryError)
                self.assertIsNotNone(kept.__traceback__)
                self.assertEqual(delta, 0,
                                 "the cancelled preparation still retains its sender")
                self.assertEqual(_native._test_send_track_counts(), before)
            finally:
                self.assertIs(module.SendTrack, original)
                sender.close()

    def test_c_a_conversion_failure_cancels_the_preparation(self):
        self.require_surface()
        # The bridge is driven directly with a wrong-typed argument, which is
        # the conversion/validation boundary inside activation.
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                owner = self.sender_handle(sender)
                before = sys.getrefcount(owner)
                counts = _native._test_send_track_counts()
                handle = _native.send_track_prepare(owner, b"v")
                kept = None
                try:
                    _native.send_track_activate(
                        handle, int(moq5.MediaType.VIDEO),
                        int(moq5.Packaging.RAW), b"av01", "not-an-int",
                        0, b"", b"", b"", True, 0, 0, 0, 0, b"", None)
                except TypeError as error:
                    kept = error
                gc.collect()
                self.assertIsNotNone(kept, "a wrong-typed argument was accepted")
                self.assertIsNotNone(kept.__traceback__)
                self.assertEqual(sys.getrefcount(owner), before,
                                 "the failed conversion still retains its sender")
                self.assertEqual(_native._test_send_track_counts(), counts)
                with self.assertRaises(RuntimeError):
                    _native.send_track_name(handle)
            finally:
                sender.close()

    def test_d_an_owner_closed_between_prepare_and_activate_cancels_it(self):
        self.require_surface()
        endpoint = self.connect()
        sender = self.attach(endpoint)
        try:
            owner = self.sender_handle(sender)
            before = sys.getrefcount(owner)
            handle = _native.send_track_prepare(owner, b"v")
            counts = _native._test_send_track_counts()
            sender.close()
            kept = None
            try:
                self.activate(handle, self.video())
            except RuntimeError as error:
                kept = error
            gc.collect()
            self.assertIsNotNone(kept, "activation after close was accepted")
            self.assertIsNotNone(kept.__traceback__)
            self.assertEqual(_native._test_send_track_counts(), counts)
            self.assertEqual(sys.getrefcount(owner), before,
                             "the cancelled activation still retains its sender")
        finally:
            endpoint.close()

    def test_e_cancellation_is_idempotent_and_never_discards_a_registration(self):
        self.require_surface()
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                handle = _native.send_track_prepare(self.sender_handle(sender),
                                                    b"v")
                _native.send_track_abort(handle)
                _native.send_track_abort(handle)        # harmless to repeat
                track = sender.add_track(self.video())
                registered = getattr(track, "_SendTrack__handle")
                before = _native._test_send_track_counts()
                with self.assertRaises(RuntimeError) as refused:
                    _native.send_track_abort(registered)
                self.assertIn("registered track", str(refused.exception))
                self.assertEqual(_native._test_send_track_counts(), before)
                self.assertIs(track.removed, False, "the registration was discarded")
            finally:
                sender.close()


class SendTrackErrorBoundaryTests(SendTrackGatedBase):
    """Only an actual NATIVE add result may be translated.

    A configuration subclass can raise anything while its properties are read,
    including its own MoqError with any code or operation string. Origin, not
    the code and not the operation text, is what separates the caller's failure
    from the service's.
    """

    def hostile(self, error):
        """A valid config whose `codec` read raises `error` once armed."""
        class Hostile(moq5.SendTrackConfig):
            armed = False

            def __getattribute__(self, name):
                if name == "codec" and object.__getattribute__(self, "armed"):
                    raise error
                return object.__getattribute__(self, name)

        config = Hostile(name=b"v", media_type=moq5.MediaType.VIDEO,
                         packaging=moq5.Packaging.RAW, codec=b"av01",
                         bitrate=1_500_000)
        object.__setattr__(config, "armed", True)
        return config

    def run_hostile(self, sender, error):
        owner = self.sender_handle(sender)
        before = sys.getrefcount(owner)
        counts = _native._test_send_track_counts()
        config = self.hostile(error)
        kept = None
        try:
            sender.add_track(config)
        except BaseException as raised:          # kept ON PURPOSE
            kept = raised
        gc.collect()
        return kept, sys.getrefcount(owner) - before, counts

    def test_a_a_callers_would_block_error_is_not_reclassified(self):
        self.require_surface()
        source = moq5.MoqError(WOULD_BLOCK, "caller_property",
                               "intentional source error")
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                kept, delta, counts = self.run_hostile(sender, source)
                self.assertIs(kept, source,
                              "the caller's own error was replaced")
                self.assertNotIsInstance(kept, moq5.TrackNameBusy,
                                         "a caller's error became TrackNameBusy")
                self.assertEqual(kept.operation, "caller_property")
                self.assertEqual(kept.code, WOULD_BLOCK)
                self.assertIsNotNone(kept.__traceback__)
                self.assertEqual(_native._test_send_track_counts(), counts,
                                 "native was entered for a caller's failure")
                self.assertEqual(delta, 0, "the preparation was not cancelled")
            finally:
                sender.close()

    def test_b_the_operation_string_alone_does_not_make_it_native(self):
        self.require_surface()
        # A caller may legitimately use the same operation text. Origin is the
        # distinction, so this must NOT be translated either.
        source = moq5.MoqError(WOULD_BLOCK, "add_track", "caller's own add")
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                kept, delta, counts = self.run_hostile(sender, source)
                self.assertIs(kept, source, "the caller's error was replaced")
                self.assertNotIsInstance(kept, moq5.TrackNameBusy)
                self.assertIsNotNone(kept.__traceback__)
                self.assertEqual(_native._test_send_track_counts(), counts)
                self.assertEqual(delta, 0, "the preparation was not cancelled")
            finally:
                sender.close()

    def test_c_a_non_moq_property_failure_still_cancels(self):
        self.require_surface()
        source = ValueError("the caller's property refused")
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                kept, delta, counts = self.run_hostile(sender, source)
                self.assertIs(kept, source)
                self.assertIsNotNone(kept.__traceback__)
                self.assertEqual(_native._test_send_track_counts(), counts)
                self.assertEqual(delta, 0, "the preparation was not cancelled")
            finally:
                sender.close()

    def test_d_a_real_native_would_block_is_still_track_name_busy(self):
        self.require_surface()
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                config = self.video()
                before = _native._test_send_track_counts()["add_entries"]
                _native._test_send_track_results(WOULD_BLOCK, 0)
                with self.assertRaises(moq5.TrackNameBusy) as raised:
                    sender.add_track(config)
                self.assertEqual(raised.exception.code, WOULD_BLOCK)
                self.assertEqual(raised.exception.operation, "add_track")
                self.assertEqual(
                    _native._test_send_track_counts()["add_entries"], before + 1,
                    "exactly one native call is witnessed")
            finally:
                _native._test_send_track_results(0, 0)
                sender.close()


class SendTrackCleanupDisclosureTests(SendTrackGatedBase):
    """A cancellation that did not complete is disclosed, never hidden."""

    def failing_wrapper(self):
        import moq5._send_track as module

        class Failing(module.SendTrack):
            @classmethod
            def _prepare(cls, sender, name, handle):
                raise primary

        primary = MemoryError("wrapper construction")
        return module, Failing, primary

    def run_with_broken_abort(self, sender, cleanup_error):
        module, failing, primary = self.failing_wrapper()
        real_abort = _native.send_track_abort
        seen = []

        def broken(handle):
            seen.append(handle)
            raise cleanup_error

        owner = self.sender_handle(sender)
        before = sys.getrefcount(owner)
        kept = None
        try:
            with mock.patch.object(module, "SendTrack", failing):
                _native.send_track_abort = broken
                try:
                    sender.add_track(self.video())
                except BaseException as raised:
                    kept = raised
        finally:
            _native.send_track_abort = real_abort      # always restored
        self.assertIs(_native.send_track_abort, real_abort)
        self.assertEqual(len(seen), 1, "cancellation was not attempted once")
        leaked = sys.getrefcount(owner) - before
        # Release the prepared fixture the broken cancellation left behind, so
        # this row does not leak merely to show the diagnostic. The release is
        # measured HERE, where the claim is made: on the way out, the returned
        # exception's traceback legitimately keeps these frames alive.
        for handle in seen:
            _native.send_track_abort(handle)
        released = sys.getrefcount(owner) - before
        del seen, handle
        return kept, primary, leaked, released

    def test_a_a_failed_cancellation_is_reported_with_the_primary(self):
        self.require_surface()
        cleanup_error = RuntimeError("cancellation itself failed")
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                kept, primary, leaked, released = self.run_with_broken_abort(
                    sender, cleanup_error)
                self.assertIsInstance(kept, BaseExceptionGroup)
                self.assertEqual(len(kept.exceptions), 2)
                self.assertIs(kept.exceptions[0], primary, "the primary moved")
                self.assertIs(kept.exceptions[1], cleanup_error,
                              "the cleanup failure is missing")
                self.assertIsNotNone(kept.exceptions[0].__traceback__)
                self.assertIsNotNone(kept.exceptions[1].__traceback__)
                # a cancellation that did not complete really does leave the
                # retention behind, which is exactly what the group discloses
                self.assertEqual(leaked, 1, "the failed cancellation left nothing")
                self.assertEqual(released, 0,
                                 "the fixture was not released afterwards")
            finally:
                sender.close()

    def test_b_a_base_exception_cleanup_failure_is_reported_too(self):
        self.require_surface()
        cleanup_error = KeyboardInterrupt()
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                kept, primary, leaked, released = self.run_with_broken_abort(
                    sender, cleanup_error)
                self.assertIsInstance(kept, BaseExceptionGroup)
                self.assertIs(kept.exceptions[0], primary)
                self.assertIs(kept.exceptions[1], cleanup_error,
                              "a BaseException cleanup failure was swallowed")
                self.assertEqual(leaked, 1)
                self.assertEqual(released, 0,
                                 "the fixture was not released afterwards")
            finally:
                sender.close()

    def test_c_a_successful_cancellation_re_raises_the_original(self):
        self.require_surface()
        import moq5._send_track as module
        original = module.SendTrack
        primary = MemoryError("wrapper construction")

        class Failing(original):
            @classmethod
            def _prepare(cls, sender, name, handle):
                raise primary

        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                owner = self.sender_handle(sender)
                before = sys.getrefcount(owner)
                kept = None
                with mock.patch.object(module, "SendTrack", Failing):
                    try:
                        sender.add_track(self.video())
                    except BaseException as raised:
                        kept = raised
                gc.collect()
                self.assertIs(kept, primary,
                              "a successful cancellation changed the exception")
                self.assertNotIsInstance(kept, BaseExceptionGroup)
                self.assertIsNotNone(kept.__traceback__)
                self.assertEqual(sys.getrefcount(owner), before)
            finally:
                self.assertIs(module.SendTrack, original)
                sender.close()


class SendTrackRemoveTests(SendTrackGatedBase):
    def test_a_remove_succeeds_once_and_leaves_an_inert_handle(self):
        self.require_surface()
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                track = sender.add_track(self.video())
                sender.remove_track(track)
                counts = _native._test_send_track_counts()
                self.assertEqual(counts["remove_entries"], 1)
                self.assertEqual(counts["removed"], 1)
                self.assertEqual(counts["live_native"], 0)
                # the handle stays valid but inert until the sender is
                # destroyed (media_sender.h:410-423), so these stay readable
                self.assertIs(track.removed, True)
                self.assertIs(track.sender, sender)
                self.assertEqual(track.name, b"v")
            finally:
                sender.close()

    def test_b_a_second_removal_is_wrong_state(self):
        self.require_surface()
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                track = sender.add_track(self.video())
                sender.remove_track(track)
                with self.assertRaises(moq5.MoqError) as raised:
                    sender.remove_track(track)
                self.assertEqual(raised.exception.code, WRONG_STATE)
                self.assertEqual(raised.exception.operation, "remove_track")
                del raised
                counts = _native._test_send_track_counts()
                self.assertEqual(counts["remove_entries"], 2, "both are witnessed")
                self.assertEqual(counts["removed"], 1, "it was removed twice")
                self.assertIs(track.removed, True)
                self.assertEqual(track.name, b"v")
            finally:
                sender.close()

    def test_c_each_scripted_removal_failure_leaves_the_wrapper_unchanged(self):
        self.require_surface()
        for code in (INVAL, WRONG_STATE, CLOSED, INTERRUPTED):
            with self.subTest(code=code):
                _native._test_send_track_reset()
                endpoint = self.connect()
                sender = None
                try:
                    sender = self.attach(endpoint)
                    track = sender.add_track(self.video())
                    before = _native._test_send_track_counts()["remove_entries"]
                    _native._test_send_track_results(0, code)
                    with self.assertRaises(moq5.MoqError) as raised:
                        sender.remove_track(track)
                    self.assertEqual(raised.exception.code, code)
                    self.assertEqual(raised.exception.operation, "remove_track")
                    del raised
                    counts = _native._test_send_track_counts()
                    self.assertEqual(counts["remove_entries"], before + 1,
                                     "exactly one call is witnessed")
                    self.assertEqual(counts["removed"], 0)
                    self.assertIs(track.removed, False,
                                  "a refused removal marked the handle removed")
                    self.assertEqual(track.name, b"v")
                finally:
                    _native._test_send_track_results(0, 0)
                    if sender is not None:
                        sender.close()
                    endpoint.close()

    def test_d_a_track_belonging_to_another_sender_is_refused(self):
        self.require_surface()
        # The fake holds ONE endpoint and ONE sender (fake_service.c:346, :2428)
        # and refuses to reset live state (:2848), so the two owners are
        # SEQUENTIAL. Declared precedence: the receiving sender refuses because
        # the track is not ITS track, which is a Python owner check before any
        # native entry, regardless of the first sender's state.
        endpoint = self.connect()
        first = self.attach(endpoint)
        track = first.add_track(self.video())
        self.assertIs(track.sender, first)
        first.close()
        endpoint.close()
        _native._test_reset()
        _native._test_sender_reset()
        second_endpoint = self.connect()
        second = self.attach(second_endpoint)
        try:
            self.assertIsNot(second, first, "the two owners are the same object")
            before = _native._test_send_track_counts()
            with self.assertRaises(RuntimeError):
                second.remove_track(track)
            self.assertEqual(_native._test_send_track_counts(), before,
                             "a foreign track entered native")
            with self.assertRaises(TypeError):
                second.remove_track(object())
            self.assertEqual(_native._test_send_track_counts(), before)
        finally:
            second.close()
            second_endpoint.close()

    def test_e_a_closed_owner_refuses_further_track_operations(self):
        self.require_surface()
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            track = sender.add_track(self.video())
            sender.close()
            before = _native._test_send_track_counts()
            with self.assertRaises(RuntimeError):
                sender.add_track(self.audio())
            with self.assertRaises(RuntimeError):
                sender.remove_track(track)
            self.assertEqual(_native._test_send_track_counts(), before,
                             "a closed owner entered native")


class SendTrackOwnershipTests(SendTrackGatedBase):
    # exit codes of the owned guard child
    GUARD_OK = 0
    GUARD_NOT_REFUSED = 7
    GUARD_NATIVE_ENTERED = 8
    GUARD_SETUP = 9

    def test_a_dropping_a_track_wrapper_removes_nothing_natively(self):
        self.require_surface()
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                track = sender.add_track(self.video())
                del track
                gc.collect()
                counts = _native._test_send_track_counts()
                self.assertEqual(counts["remove_entries"], 0,
                                 "collecting the wrapper removed the native track")
                self.assertEqual(counts["removed"], 0)
                self.assertEqual(counts["live_native"], 1)
                self.assertEqual(_native._test_send_track_state(0), {
                    "allocated": True, "removed": False, "owner_destroyed": False})
            finally:
                sender.close()

    def test_b_a_track_keeps_its_sender_and_endpoint_alive(self):
        self.require_surface()
        endpoint = self.connect()
        sender = self.attach(endpoint)
        track = sender.add_track(self.video())
        del sender, endpoint
        gc.collect()
        try:
            self.assertIsInstance(track.sender, moq5.Sender)
            self.assertIsInstance(track.sender.endpoint, moq5.Endpoint)
            self.assertEqual(_native._test_send_track_counts()["removed"], 0)
            self.assertEqual(_native._test_counts()[2], 0,
                             "the endpoint was destroyed under a live track")
        finally:
            owner = track.sender
            owner_endpoint = owner.endpoint
            del track
            gc.collect()
            owner.close()
            owner_endpoint.close()

    def test_c_identity_is_stable_and_usable_as_a_key(self):
        self.require_surface()
        with self.connect() as endpoint:
            sender = self.attach(endpoint)
            try:
                first = sender.add_track(self.video())
                second = sender.add_track(self.audio())
                # distinct KEYS, which is what identity means here. Two unequal
                # objects are not required to have unequal hashes.
                self.assertIsNot(first, second)
                self.assertNotEqual(first, second)
                self.assertEqual(len({first, second}), 2)
                self.assertEqual({first: 1, second: 2}[first], 1)
                before = hash(first)
                sender.remove_track(first)
                self.assertEqual(hash(first), before, "hash changed on removal")
                self.assertEqual(first, first, "equality changed on removal")
                self.assertEqual(len({first, second}), 2)
            finally:
                sender.close()
            self.assertEqual(hash(first), before, "hash changed on close")
            self.assertEqual(first, first)

    def thread_guard_child(self):
        """The thread arm inside ONE owned child: a worker that outlives its
        join dies with the process rather than keeping the suite alive."""
        child = os.fork()
        if child == 0:
            status = self.GUARD_OK
            try:
                endpoint = self.connect()
                sender = self.attach(endpoint)
                track = sender.add_track(self.video())
                before = _native._test_send_track_counts()
                refused = []

                def other():
                    # The first two enter through the SENDER, whose own gate
                    # also refuses a foreign thread. Reading track.removed
                    # touches only the TRACK capsule, so it is the operation
                    # that isolates the track-level gate.
                    for call in (lambda: sender.add_track(self.audio()),
                                 lambda: sender.remove_track(track),
                                 lambda: track.removed):
                        try:
                            call()
                        except RuntimeError as error:
                            refused.append(error)

                thread = threading.Thread(target=other, daemon=True)
                thread.start()
                thread.join(5.0)
                if thread.is_alive():
                    os._exit(self.GUARD_NOT_REFUSED)
                if len(refused) != 3:
                    status = self.GUARD_NOT_REFUSED
                elif _native._test_send_track_counts() != before:
                    status = self.GUARD_NATIVE_ENTERED
                else:
                    sender.close()
                    endpoint.close()
            except BaseException:
                os._exit(self.GUARD_SETUP)
            os._exit(status)
        return reap_child(child)

    @unittest.skipUnless(hasattr(os, "fork"), "fork is a POSIX-only contract")
    def test_d_the_owner_thread_guard_precedes_native_entry(self):
        self.require_surface()
        self.assertEqual(self.thread_guard_child(), self.GUARD_OK,
                         "the owner-thread guard child did not exit cleanly")

    @unittest.skipUnless(hasattr(os, "fork"), "fork is a POSIX-only contract")
    def test_e_the_pid_guard_precedes_native_entry_for_add_and_remove(self):
        self.require_surface()
        # The fixture is built in the PARENT and exactly ONE child is forked,
        # so there is a single deadline and a single owner. A nested pair would
        # share one budget and could orphan the inner descendant.
        endpoint = self.connect()
        sender = self.attach(endpoint)
        track = sender.add_track(self.video())
        before = _native._test_send_track_counts()
        try:
            child = os.fork()
            if child == 0:
                refused = 0
                # the third touches only the TRACK capsule, isolating its gate
                for call in (lambda: sender.add_track(self.audio()),
                             lambda: sender.remove_track(track),
                             lambda: track.removed):
                    try:
                        call()
                    except RuntimeError:
                        refused += 1
                    except BaseException:
                        os._exit(self.GUARD_SETUP)
                counts = _native._test_send_track_counts()
                if refused != 3:
                    os._exit(self.GUARD_NOT_REFUSED)
                os._exit(self.GUARD_OK if counts == before
                         else self.GUARD_NATIVE_ENTERED)
            self.assertEqual(reap_child(child), self.GUARD_OK,
                             "the fork guard child did not exit cleanly")
            # the parent's own fixture is untouched by the child
            self.assertEqual(_native._test_send_track_counts(), before)
            self.assertIs(track.removed, False)
        finally:
            sender.close()
            endpoint.close()

    def test_f_close_invalidates_tracks_without_a_per_track_destructor(self):
        self.require_surface()
        endpoint = self.connect()
        sender = self.attach(endpoint)
        track = sender.add_track(self.video())
        sender.close()
        try:
            # the native sender owns its tracks until destruction: closing it
            # removes nothing and there is no per-track native destructor
            self.assertEqual(
                _native._test_send_track_counts()["remove_entries"], 0)
            self.assertIs(_native._test_send_track_state(0)["owner_destroyed"],
                          True)
            with self.assertRaises(RuntimeError):
                sender.remove_track(track)
            with warnings.catch_warnings(record=True) as seen:
                warnings.simplefilter("always")
                del track
                gc.collect()
            self.assertEqual([w.category for w in seen], [],
                             "dropping a track after close emitted a diagnostic")
        finally:
            endpoint.close()


# ------------------------------------------------------------------ control --

# Rows gated on the PUBLIC surface: every one must fail while the public names
# are missing or inert.
PUBLIC_GATED_CLASSES = ("SendTrackConfigTests", "SendTrackAddTests",
                        "SendTrackFailedActivationTests",
                        "SendTrackCancellationTests",
                        "SendTrackErrorBoundaryTests",
                        "SendTrackCleanupDisclosureTests",
                        "SendTrackRemoveTests", "SendTrackOwnershipTests")
# Rows gated on the NATIVE bridge. One of them asks only whether the bridge
# names exist, so it legitimately passes once the bridge lands even while the
# public surface is still inert. It is inventoried separately for that reason.
BRIDGE_GATED_CLASSES = ("SendTrackBridgeTests",)
BRIDGE_EXISTENCE_ROW = (
    "test_send_track.SendTrackBridgeTests."
    "test_a_the_bridge_exposes_a_prepare_and_activate_pair")


class SendTrackControlTests(unittest.TestCase):
    """This file's own machinery. Never evidence about the product.

    These prove the missing-name and inert-name gate only. They say nothing
    about the ownership, retention or bridge oracles further down each row.
    """

    def suite_for(self, classes):
        import test_send_track as module
        loader = unittest.defaultTestLoader
        return unittest.TestSuite(
            loader.loadTestsFromTestCase(getattr(module, c)) for c in classes)

    def run_suite(self, classes):
        result = unittest.TestResult()
        with warnings.catch_warnings(record=True) as raised:
            warnings.simplefilter("always")
            self.suite_for(classes).run(result)
        return result, raised

    def test_a_inert_public_names_do_not_satisfy_any_public_row(self):
        saved = {n: getattr(moq5, n, None) for n in SURFACE}
        try:
            for name in SURFACE:
                setattr(moq5, name, object())
            result, raised = self.run_suite(PUBLIC_GATED_CLASSES)
        finally:
            for name, value in saved.items():
                if value is None:
                    if hasattr(moq5, name):
                        delattr(moq5, name)
                else:
                    setattr(moq5, name, value)
        self.assertGreater(result.testsRun, 0)
        self.assertEqual(result.skipped, [], "a gated row skipped")
        self.assertEqual(result.errors, [],
                         "a gated row raised an arbitrary error instead of "
                         "refusing by a named assertion")
        failed = {getattr(case, "test_case", case).id()
                  for case, _ in result.failures}
        self.assertEqual(len(failed), result.testsRun,
                         "an inert public surface satisfied a behavioural row")
        self.assertEqual([w.category for w in raised], [],
                         "the gate acquired resources before refusing")

    def test_c_present_native_names_are_not_evidence_of_public_behaviour(self):
        """Bridge names on _native say nothing about the public surface. With
        the bridge present and the public names inert, every public row and
        every bridge row EXCEPT the existence check must still fail."""
        saved_public = {n: getattr(moq5, n, None) for n in SURFACE}
        saved_bridge = {n: getattr(_native, n, None) for n in BRIDGE}
        try:
            for name in SURFACE:
                setattr(moq5, name, object())
            for name in BRIDGE:
                setattr(_native, name, lambda *a, **k: None)
            result, raised = self.run_suite(
                PUBLIC_GATED_CLASSES + BRIDGE_GATED_CLASSES)
        finally:
            for name, value in saved_public.items():
                if value is None:
                    if hasattr(moq5, name):
                        delattr(moq5, name)
                else:
                    setattr(moq5, name, value)
            for name, value in saved_bridge.items():
                if value is None:
                    if hasattr(_native, name):
                        delattr(_native, name)
                else:
                    setattr(_native, name, value)
        self.assertGreater(result.testsRun, 0)
        self.assertEqual(result.skipped, [])
        self.assertEqual(result.errors, [],
                         "a row raised an arbitrary error instead of refusing")
        failed = {getattr(case, "test_case", case).id()
                  for case, _ in result.failures}
        # the existence row is the one that legitimately passes here
        self.assertNotIn(BRIDGE_EXISTENCE_ROW, failed,
                         "the bridge-existence row did not see the names")
        self.assertEqual(len(failed), result.testsRun - 1,
                         "a row other than the existence check passed on "
                         "names alone")
        self.assertEqual([w.category for w in raised], [])

    @unittest.skipUnless(hasattr(os, "fork"), "fork is a POSIX-only control")
    def test_d_the_envelope_leaves_no_owned_child_behind(self):
        """The guard rows use exactly one child each, bounded by this envelope.
        A child that never returns must be ended and reaped inside the bound,
        with nothing owned left over."""
        child = os.fork()
        if child == 0:
            try:
                while True:
                    time.sleep(3600)
            finally:
                os._exit(0)
        started = time.monotonic()
        outcome = reap_child(child, timeout=2.0)
        elapsed = time.monotonic() - started
        self.assertIsNone(outcome, "a non-returning child reported an exit status")
        self.assertLess(elapsed, 30.0, "the envelope did not bound the wait")
        with self.assertRaises(ChildProcessError):
            os.waitpid(child, os.WNOHANG)      # already reaped, nothing owned

    def test_b_the_missing_name_diagnosis_is_exercised_without_requiring_absence(self):
        """The product may land at any time, so this exercises the diagnosis
        against a TEMPORARY namespace rather than asserting the real one stays
        empty."""
        class Empty:
            pass

        controlled = Empty()
        self.assertEqual(missing_surface(controlled), SURFACE)
        for name in SURFACE:
            setattr(controlled, name, object())
        self.assertEqual(missing_surface(controlled), ())
        delattr(controlled, SURFACE[1])
        self.assertEqual(missing_surface(controlled), (SURFACE[1],))
        # and the real namespace is untouched by this control
        self.assertEqual(missing_surface(), missing_surface(moq5))
