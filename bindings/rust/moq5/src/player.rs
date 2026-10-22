//! The media player (`media_player.h`, the `player` feature): decode-order
//! media for an application's decoders, over a [`Receiver`].
//!
//! A [`Player`] composes a receiver (catalog, subscriptions, the object
//! queue and its stream-terminal lane) with libmoq's playback engine
//! (per-track decode order, knowledge of what exists, gap policy). For each
//! selected track it queues ordered outputs ([`PlayerOutput`]): a decoder
//! configuration, access units in decode order, discontinuities, the
//! presentation mapping of the track's timeline, and the track's end.
//! Diagnostics (gaps, skips, drops, loss, selection) are events
//! ([`PlayerEvent`]) on a separate queue and never interleave with outputs.
//! Decoders, renderers, audio output and the presentation clock stay with
//! the application, which reports back through
//! [`handle_feedback`](Player::handle_feedback).
//!
//! This release plays RAW/LOC video and the RAW/LOC audio codecs `opus` and
//! `mp4a.40.2`, on drafts 16, 18 and 22. CMAF, MPEG-TS, dependent spatial
//! layers and timeline tracks are refused at selection with
//! [`Error::UNSUPPORTED`]: the receiver serves those directly.
//!
//! **Threading and the clock.** The player owns no thread. Every call on it,
//! [`wait`](Player::wait) and the observers included, is the application's,
//! one at a time: the type is `Send` but not `Sync`. `now_us` is the
//! application's monotonic clock in microseconds, one clock for
//! [`tick`](Player::tick), [`wait`](Player::wait),
//! [`handle_feedback`](Player::handle_feedback) and every deadline and
//! timeline anchor the player reports; the player never reads a clock.
//!
//! **Outputs borrow the player.** An output holds references to buffers the
//! engine shares (the codec configuration, an object's payload), and
//! libmoq's refcounts are not atomic. So a [`PlayerOutput`] borrows its
//! player: it is released on the player's thread, before the player is
//! dropped or moves to another thread. Events own nothing and are plain
//! values.
//!
//! **The receiver belongs to the player.** While the player exists it is
//! the only consumer of every receive queue of its receiver (track events,
//! objects, stream terminals, timeline records), and it subscribes the
//! tracks it selects. [`Player::receiver`] offers the receiver's observing
//! calls only; [`Player::detach`] destroys the player and returns the
//! receiver, whose queues are then the application's again.
//!
//! ```no_run
//! # fn main() -> Result<(), moq5::Error> {
//! use std::time::{Duration, Instant};
//!
//! use moq5::player::{OutputDetail, Player, PlayerConfig, PlayerEventKind, Selection};
//! use moq5::{Endpoint, EndpointConfig, Namespace, ReceiverConfig};
//!
//! let endpoint = Endpoint::connect(&EndpointConfig::new("https://relay.example:4443/moq"))?;
//! let player = Player::create(
//!     &endpoint,
//!     &ReceiverConfig::live(Namespace::parse("live/demo")?),
//!     &PlayerConfig::new().selection(Selection::Manual),
//! )?;
//! let start = Instant::now();
//! let now_us = || start.elapsed().as_micros() as u64;
//! loop {
//!     player.tick(now_us())?;
//!     while let Some(event) = player.poll_event()? {
//!         if let (PlayerEventKind::TrackAvailable, Some(track)) = (event.kind, event.track) {
//!             player.select_track(track, None)?;
//!         }
//!     }
//!     while let Some(output) = player.poll_output()? {
//!         if let OutputDetail::Unit(unit) = output.detail() {
//!             let _bytes = unit.data(); // decode, at unit.presentation_time_us()
//!         }
//!     }
//!     player.wait(now_us(), Duration::from_millis(50))?;
//! }
//! # }
//! ```

use std::cell::Cell;
use std::fmt;
use std::marker::PhantomData;
use std::mem::{self, ManuallyDrop};
use std::ptr::{self, NonNull};
use std::time::Duration;

use crate::endpoint::Endpoint;
use crate::error::{Error, Result};
use crate::receiver::{
    ContentProtectionRef, Receiver, ReceiverConfig, ReceiverInfo, ReceiverStats, StartMode,
    StreamTerminalKind, TimeMode, Track, TrackDescription, TrackState,
};
use crate::types::{Location, MediaType, Packaging, TimeDomain};
use crate::{ffi, sys};

// -- configuration --------------------------------------------------------------

/// How tracks are chosen (`moq_media_selection_mode_t`).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Selection {
    /// Once the catalog is ready, the player selects a render group's
    /// tracks itself: the lowest render group, one track per alternate
    /// group, on one timeline (without render groups, one video track).
    /// The others are reported as available.
    #[default]
    Auto,
    /// Nothing is selected until the application does.
    Manual,
}

impl Selection {
    fn raw(self) -> u32 {
        match self {
            Selection::Auto => sys::MOQ_MEDIA_SELECTION_AUTO,
            Selection::Manual => sys::MOQ_MEDIA_SELECTION_MANUAL,
        }
    }
}

/// Whether [`Selection::Auto`] may select a render group whose tracks
/// cannot share one timeline (`moq_media_timing_policy_t`).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum TimingPolicy {
    /// Leave such a group out, reported as
    /// [`PlayerEventKind::SyncUnavailable`].
    #[default]
    RequireSync,
    /// Select it with a timeline per track, reported as
    /// [`PlayerEventKind::TimingIndependent`].
    AllowIndependent,
}

impl TimingPolicy {
    fn raw(self) -> u32 {
        match self {
            TimingPolicy::RequireSync => sys::MOQ_MEDIA_TIMING_REQUIRE_SYNC,
            TimingPolicy::AllowIndependent => sys::MOQ_MEDIA_TIMING_ALLOW_INDEPENDENT,
        }
    }
}

/// The playback engine's limits (`moq_playback_cfg_t`). Only the fields set
/// here override the engine's defaults. The player itself fixes how a
/// track's groups end (by its stream facts and END_OF_TRACK, never by the
/// header's end-of-group bit or the next group's arrival) and how many
/// commands it holds, so those are not offered.
#[derive(Clone, Debug, Default)]
pub struct PlaybackConfig {
    max_tracks: Option<u32>,
    max_buffered_objects: Option<u32>,
    max_buffered_bytes: Option<u64>,
    max_backlog_groups: Option<u32>,
    max_commands: Option<u32>,
    max_events: Option<u32>,
    gap_timeout: Option<Duration>,
    max_release_per_tick: Option<u32>,
    max_track_config_bytes: Option<u32>,
    max_samples_per_object: Option<u32>,
    max_known_ranges: Option<u32>,
}

impl PlaybackConfig {
    /// The engine's defaults.
    #[must_use]
    pub fn new() -> PlaybackConfig {
        PlaybackConfig::default()
    }
    /// Tracks the engine plays at once, which bounds the selected tracks
    /// (default 4).
    #[must_use]
    pub fn max_tracks(mut self, max: u32) -> Self {
        self.max_tracks = Some(max);
        self
    }
    /// Jitter-buffer entries (default 256).
    #[must_use]
    pub fn max_buffered_objects(mut self, max: u32) -> Self {
        self.max_buffered_objects = Some(max);
        self
    }
    /// Bytes kept for buffered media: payloads, sample tables and
    /// configuration copies, including those of queued outputs until they
    /// are polled (default 16 MiB).
    #[must_use]
    pub fn max_buffered_bytes(mut self, max: u64) -> Self {
        self.max_buffered_bytes = Some(max);
        self
    }
    /// The live backlog, in groups, past which the engine sheds (default 3).
    #[must_use]
    pub fn max_backlog_groups(mut self, max: u32) -> Self {
        self.max_backlog_groups = Some(max);
        self
    }
    /// The engine's command queue (default 64).
    #[must_use]
    pub fn max_commands(mut self, max: u32) -> Self {
        self.max_commands = Some(max);
        self
    }
    /// The engine's event queue (default 16).
    #[must_use]
    pub fn max_events(mut self, max: u32) -> Self {
        self.max_events = Some(max);
        self
    }
    /// How long a gap is waited on before it is skipped, exact microseconds
    /// (default 500 ms).
    #[must_use]
    pub fn gap_timeout(mut self, timeout: Duration) -> Self {
        self.gap_timeout = Some(timeout);
        self
    }
    /// Units released per tick; 0 releases without limit (default 8).
    #[must_use]
    pub fn max_release_per_tick(mut self, max: u32) -> Self {
        self.max_release_per_tick = Some(max);
        self
    }
    /// Per-track codec and init bytes (default 4096).
    #[must_use]
    pub fn max_track_config_bytes(mut self, max: u32) -> Self {
        self.max_track_config_bytes = Some(max);
        self
    }
    /// CMAF samples per object (default 16).
    #[must_use]
    pub fn max_samples_per_object(mut self, max: u32) -> Self {
        self.max_samples_per_object = Some(max);
        self
    }
    /// Ranges of known absence and of unresolved delivery kept per track
    /// (0 selects 64).
    #[must_use]
    pub fn max_known_ranges(mut self, max: u32) -> Self {
        self.max_known_ranges = Some(max);
        self
    }

    /// The C configuration, built with the engine's sized initializer.
    fn raw(&self) -> Result<sys::moq_playback_cfg_t> {
        let mut raw = sys::moq_playback_cfg_t::default();
        // SAFETY: a writable struct of exactly the size passed.
        unsafe { sys::moq_playback_cfg_init_sized(&mut raw, mem::size_of_val(&raw)) };
        if let Some(max) = self.max_tracks {
            raw.max_tracks = max;
        }
        if let Some(max) = self.max_buffered_objects {
            raw.max_buffered_objects = max;
        }
        if let Some(max) = self.max_buffered_bytes {
            raw.max_buffered_bytes = max;
        }
        if let Some(max) = self.max_backlog_groups {
            raw.max_backlog_groups = max;
        }
        if let Some(max) = self.max_commands {
            raw.max_commands = max;
        }
        if let Some(max) = self.max_events {
            raw.max_events = max;
        }
        if let Some(timeout) = self.gap_timeout {
            raw.gap_timeout_us = ffi::micros(timeout)?;
        }
        if let Some(max) = self.max_release_per_tick {
            raw.max_release_per_tick = max;
        }
        if let Some(max) = self.max_track_config_bytes {
            raw.max_track_config_bytes = max;
        }
        if let Some(max) = self.max_samples_per_object {
            raw.max_samples_per_object = max;
        }
        if let Some(max) = self.max_known_ranges {
            raw.max_known_ranges = max;
        }
        Ok(raw)
    }
}

/// How a [`Player`] plays (`moq_media_player_cfg_t`). Every field's zero is
/// its default; only the fields set here are given.
#[derive(Clone, Debug, Default)]
pub struct PlayerConfig {
    selection: Option<Selection>,
    playback: Option<PlaybackConfig>,
    preferred_lang: Option<String>,
    end_tail_wait: Option<Duration>,
    cushion: Option<Duration>,
    max_outputs: Option<u32>,
    max_events: Option<u32>,
    timing_policy: Option<TimingPolicy>,
    max_held_bytes: Option<u64>,
    assume_shared_epoch: Option<bool>,
}

impl PlayerConfig {
    /// [`Selection::Auto`] and every default.
    #[must_use]
    pub fn new() -> PlayerConfig {
        PlayerConfig::default()
    }
    /// How tracks are chosen.
    #[must_use]
    pub fn selection(mut self, selection: Selection) -> Self {
        self.selection = Some(selection);
        self
    }
    /// The engine's limits; its `max_tracks` bounds the selected tracks.
    #[must_use]
    pub fn playback(mut self, playback: PlaybackConfig) -> Self {
        self.playback = Some(playback);
        self
    }
    /// [`Selection::Auto`]: within an alternate group, prefer this catalog
    /// lang (unset: any).
    #[must_use]
    pub fn preferred_lang(mut self, lang: impl Into<String>) -> Self {
        self.preferred_lang = Some(lang.into());
        self
    }
    /// How long a track whose input ended without proof of completion, or
    /// that was removed, keeps accepting late input before it is cut off: a
    /// local policy, not a transport timer. Exact microseconds; zero selects
    /// 2 s.
    #[must_use]
    pub fn end_tail_wait(mut self, wait: Duration) -> Self {
        self.end_tail_wait = Some(wait);
        self
    }
    /// The timeline lead: a timeline is anchored with its first unit
    /// presented this long after the tick that released it. Exact
    /// microseconds; zero selects 100 ms.
    #[must_use]
    pub fn cushion(mut self, cushion: Duration) -> Self {
        self.cushion = Some(cushion);
        self
    }
    /// Output and event queue depths (0 = 128 and 64), each at most 65536;
    /// a larger value is refused. The event queue keeps room for
    /// [`PlayerEventKind::Ended`] and [`PlayerEventKind::Closed`] beyond
    /// its depth.
    #[must_use]
    pub fn queue_limits(mut self, max_outputs: u32, max_events: u32) -> Self {
        self.max_outputs = Some(max_outputs);
        self.max_events = Some(max_events);
        self
    }
    /// Whether [`Selection::Auto`] may play a render group unsynchronized.
    #[must_use]
    pub fn timing_policy(mut self, policy: TimingPolicy) -> Self {
        self.timing_policy = Some(policy);
        self
    }
    /// The bytes of the one object the player may hold while the engine has
    /// no room for it (0 = 8 MiB). No later object or stream terminal is
    /// taken from the receiver until it goes in; a larger object is taken
    /// only when the engine accepts it at once, and is otherwise dropped
    /// ([`DropReason::Oversize`]).
    #[must_use]
    pub fn max_held_bytes(mut self, max: u64) -> Self {
        self.max_held_bytes = Some(max);
        self
    }
    /// [`Selection::Auto`]: treat a render group whose tracks are all in
    /// application media time as one timeline, the application asserting
    /// that they share an epoch. Without it such a group of more than one
    /// track is not selected.
    #[must_use]
    pub fn assume_shared_epoch(mut self, assume: bool) -> Self {
        self.assume_shared_epoch = Some(assume);
        self
    }
}

/// How a selected track plays (`moq_media_player_select_cfg_t`). It is the
/// application's record (start from `Default`); it is not
/// `#[non_exhaustive]`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SelectOptions {
    /// Where its subscription starts.
    pub start: StartMode,
    /// Give the track a timeline of its own instead of its render group's:
    /// an explicit choice to play it unsynchronized.
    pub independent_timing: bool,
    /// The application asserts that this track and the other application
    /// media time tracks of its render group share an epoch.
    pub shared_epoch: bool,
}

// -- the player -------------------------------------------------------------------

/// Plays the tracks of one [`Receiver`] for the application's decoders. See
/// the [module documentation](self) for threading, the clock and output
/// ownership.
///
/// Dropping it destroys the player (unsubscribing every track it selected
/// and releasing what it holds), then its receiver.
pub struct Player {
    raw: NonNull<sys::moq_media_player_t>,
    receiver: ManuallyDrop<Receiver>,
    _not_sync: PhantomData<Cell<()>>,
}

// SAFETY: libmoq accepts player calls from one application thread at a
// time, which `!Sync` enforces, and the receiver it holds is `Send`. The
// buffer references the player's outputs share with its engine move with
// it: an output borrows the player, so none is alive when it moves.
unsafe impl Send for Player {}

/// A failed [`Player::attach`]: the error and the receiver, which the
/// caller still owns, untouched (every check runs before the player takes
/// anything). Converting it into an [`Error`] drops the receiver. The pair
/// is the whole record; it is not `#[non_exhaustive]`.
#[derive(Debug)]
#[must_use]
pub struct AttachError {
    /// What libmoq reported.
    pub error: Error,
    /// The receiver.
    pub receiver: Receiver,
}

impl fmt::Display for AttachError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "player attach failed: {}", self.error)
    }
}

impl std::error::Error for AttachError {}

impl From<AttachError> for Error {
    fn from(failure: AttachError) -> Error {
        failure.error
    }
}

/// What a [`Player::tick`] got done.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Tick {
    /// Everything that could be done at `now_us` was done.
    Done,
    /// The output queue filled while the engine held more: poll outputs and
    /// tick again. The work done before stands.
    OutputsFull,
}

/// What a [`Player::wait`] found.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PlayerWait {
    /// Something to do: an output or event is pollable, the receiver has
    /// input, or a deadline is at or before `now_us`. Tick.
    Ready,
    /// Nothing for tick: the timeout elapsed first, or the only work left
    /// is paused by decoder-queue feedback, which only
    /// [`handle_feedback`](Player::handle_feedback) lifts (wait on the
    /// decoder then, not here).
    Idle,
}

impl Player {
    /// Attach a player to `receiver`, which it takes: while the player
    /// exists it is the only consumer of every receive queue of the
    /// receiver (see [`detach`](Player::detach)). The receiver must not
    /// auto-subscribe, must have the stream-terminal lane on and must not
    /// have had a track event polled (the player sees discovery from the
    /// start); otherwise [`Error::WRONG_STATE`]. A shared-epoch receiver is
    /// [`Error::UNSUPPORTED`], a bad configuration [`Error::INVAL`]. A
    /// refused attach returns the receiver untouched.
    pub fn attach(
        receiver: Receiver,
        config: &PlayerConfig,
    ) -> std::result::Result<Player, AttachError> {
        match Player::attach_raw(&receiver, config) {
            Ok(raw) => Ok(Player {
                raw,
                receiver: ManuallyDrop::new(receiver),
                _not_sync: PhantomData,
            }),
            Err(error) => Err(AttachError { error, receiver }),
        }
    }

    fn attach_raw(
        receiver: &Receiver,
        config: &PlayerConfig,
    ) -> Result<NonNull<sys::moq_media_player_t>> {
        let playback = match &config.playback {
            Some(playback) => Some(playback.raw()?),
            None => None,
        };
        let mut raw_config = sys::moq_media_player_cfg_t::default();
        // SAFETY: a writable struct of exactly the size passed.
        unsafe {
            sys::moq_media_player_cfg_init_sized(&mut raw_config, mem::size_of_val(&raw_config))
        };
        raw_config.receiver = ptr::null(); // attach: the receiver is given
        if let Some(selection) = config.selection {
            raw_config.selection = selection.raw();
        }
        raw_config.playback = playback
            .as_ref()
            .map_or(ptr::null(), |playback| playback as *const _);
        raw_config.preferred_lang = ffi::optional_span(config.preferred_lang.as_deref());
        if let Some(wait) = config.end_tail_wait {
            raw_config.end_tail_wait_us = ffi::micros(wait)?;
        }
        if let Some(cushion) = config.cushion {
            raw_config.cushion_us = ffi::micros(cushion)?;
        }
        if let Some(max) = config.max_outputs {
            raw_config.max_outputs = max;
        }
        if let Some(max) = config.max_events {
            raw_config.max_events = max;
        }
        if let Some(policy) = config.timing_policy {
            raw_config.timing_policy = policy.raw();
        }
        if let Some(max) = config.max_held_bytes {
            raw_config.max_held_bytes = max;
        }
        if let Some(assume) = config.assume_shared_epoch {
            raw_config.assume_shared_epoch = assume;
        }
        let mut raw = ptr::null_mut();
        // SAFETY: a live receiver; every pointer in raw_config points into
        // config or playback, alive for the call, and the player copies
        // what it keeps.
        ffi::check(unsafe {
            sys::moq_media_player_attach(receiver.as_ptr(), &raw_config, &mut raw)
        })?;
        NonNull::new(raw).ok_or(Error::INTERNAL)
    }

    /// A player with a receiver of its own, attached to `endpoint` as the
    /// player needs it: the receiver configuration as given, except that it
    /// does not auto-subscribe and keeps the stream-terminal lane (as deep
    /// as given, else the service's default). A shared-epoch configuration
    /// is [`Error::UNSUPPORTED`], refused before anything is attached;
    /// otherwise the results are [`Receiver::attach`]'s and
    /// [`attach`](Player::attach)'s.
    pub fn create(
        endpoint: &Endpoint,
        receiver: &ReceiverConfig,
        config: &PlayerConfig,
    ) -> Result<Player> {
        if receiver.time_mode_requested() == TimeMode::SharedEpoch {
            return Err(Error::UNSUPPORTED);
        }
        let receiver = Receiver::attach(endpoint, &receiver.for_player())?;
        Player::attach(receiver, config).map_err(Error::from)
    }

    /// The receiver's observing calls. Polling, waiting on and subscribing
    /// the receiver are the player's while it exists.
    #[must_use]
    pub fn receiver(&self) -> ReceiverView<'_> {
        ReceiverView {
            receiver: &self.receiver,
        }
    }

    /// Destroy the player and return its receiver, whose queues are the
    /// application's again. The tracks the player selected are
    /// unsubscribed.
    pub fn detach(self) -> Receiver {
        let mut this = ManuallyDrop::new(self);
        // SAFETY: the sole owner; no output borrows the player (this call
        // takes it by value), and the receiver it was attached to is still
        // alive.
        unsafe { sys::moq_media_player_destroy(this.raw.as_ptr()) };
        // SAFETY: taken once; `this` is never used or dropped again.
        unsafe { ManuallyDrop::take(&mut this.receiver) }
    }

    /// The underlying `moq_media_player_t`.
    #[must_use]
    pub fn as_ptr(&self) -> *mut sys::moq_media_player_t {
        self.raw.as_ptr()
    }

    /// Select a track the receiver announced (from a
    /// [`PlayerEventKind::TrackAvailable`] event): it is subscribed and its
    /// media goes to the engine. The tracks of one render group share a
    /// timeline when their timestamps share a domain: all wall-clock (LOC
    /// without a timescale, and every draft-16/18 track), or all application
    /// media time with [`SelectOptions::shared_epoch`] asserted by each; a
    /// track that would join a timeline otherwise is
    /// [`Error::UNSUPPORTED`] unless it asks for independent timing. A
    /// refusal selects nothing.
    ///
    /// [`Error::UNSUPPORTED`] also for a packaging, media type or codec this
    /// release does not play, and for an `mp4a.40.2` track that can carry no
    /// decoder configuration; [`Error::WRONG_STATE`] for a track already
    /// selected, ended or removed, or a terminal receiver; [`Error::LIMIT`]
    /// when the engine's tracks are all in use; [`Error::INVAL`] for a
    /// foreign track.
    pub fn select_track(&self, track: Track, options: Option<SelectOptions>) -> Result<()> {
        let track = self.receiver.own(track)?;
        let raw_options = options.map(|options| {
            let mut raw = sys::moq_media_player_select_cfg_t::default();
            // SAFETY: a writable struct of exactly the size passed.
            unsafe {
                sys::moq_media_player_select_cfg_init_sized(&mut raw, mem::size_of_val(&raw))
            };
            raw.start = match options.start {
                StartMode::Current => sys::MOQ_MEDIA_START_CURRENT,
                StartMode::NextGroup => sys::MOQ_MEDIA_START_NEXT_GROUP,
            };
            raw.independent_timing = options.independent_timing;
            raw.shared_epoch = options.shared_epoch;
            raw
        });
        let options_ptr = raw_options
            .as_ref()
            .map_or(ptr::null(), |raw| raw as *const _);
        // SAFETY: a live player and one of its receiver's handles; the
        // options are borrowed for the call.
        ffi::check(unsafe { sys::moq_media_player_select_track(self.as_ptr(), track, options_ptr) })
    }

    /// Deselect a track: it is unsubscribed and what the player and engine
    /// hold for it is dropped. Outputs already queued stay, and no end
    /// follows. [`Error::INVAL`] for a track that is not selected.
    pub fn deselect_track(&self, track: Track) -> Result<()> {
        let track = self.receiver.own(track)?;
        // SAFETY: a live player and one of its receiver's handles.
        ffi::check(unsafe { sys::moq_media_player_deselect_track(self.as_ptr(), track) })
    }

    /// Do everything that can be done at `now_us`: take track events,
    /// objects and stream terminals from the receiver in order, run the
    /// engine, and turn its work into outputs and events.
    /// [`Error::CLOSED`] once the player is closed.
    pub fn tick(&self, now_us: u64) -> Result<Tick> {
        // SAFETY: a live player, called from its application thread.
        match unsafe { sys::moq_media_player_tick(self.as_ptr(), now_us) } {
            sys::MOQ_ERR_WOULD_BLOCK => Ok(Tick::OutputsFull),
            code => ffi::check(code).map(|()| Tick::Done),
        }
    }

    /// Wait for something to do, without advancing the player or consuming
    /// anything: [`PlayerWait::Ready`] at once for a pollable output or
    /// event, input for tick or a due deadline, [`PlayerWait::Idle`] at once
    /// when the only work left waits for decoder-queue feedback; otherwise
    /// it sleeps on the receiver for at most `timeout` (exact microseconds)
    /// or until the next deadline. [`Error::INTERRUPTED`] while the
    /// endpoint's latch is set (checked first), [`Error::CLOSED`] once the
    /// player is closed. An object held for engine room is not a deadline:
    /// polling outputs makes that room.
    pub fn wait(&self, now_us: u64, timeout: Duration) -> Result<PlayerWait> {
        let timeout_us = ffi::micros(timeout)?;
        // SAFETY: a live player, called from its application thread.
        match unsafe { sys::moq_media_player_wait(self.as_ptr(), now_us, timeout_us) } {
            sys::MOQ_OK => Ok(PlayerWait::Ready),
            sys::MOQ_DONE => Ok(PlayerWait::Idle),
            code => Err(Error::from_code(code)),
        }
    }

    /// The earliest `now_us` at which [`tick`](Player::tick) has time-driven
    /// work (a gap wait, a track's end tail wait); `None` when there is
    /// none. Observes only.
    #[must_use]
    pub fn next_deadline_us(&self) -> Option<u64> {
        // SAFETY: a live player.
        let deadline = unsafe { sys::moq_media_player_next_deadline_us(self.as_ptr()) };
        (deadline != u64::MAX).then_some(deadline)
    }

    /// The next output, or `None` when none is queued. Per track, a
    /// configuration precedes the units it applies to, a discontinuity the
    /// unit it precedes, a timeline its first unit, and the end follows the
    /// track's last unit. [`Error::CLOSED`] when none is queued and the
    /// player is closed; [`Error::ABI_MISMATCH`] when this crate's record is
    /// smaller than the library's (nothing is dequeued) or the library
    /// stamped one outside the frozen layout (its references are released).
    pub fn poll_output(&self) -> Result<Option<PlayerOutput<'_>>> {
        let mut raw = sys::moq_media_player_output_t::default();
        let size = mem::size_of_val(&raw);
        // SAFETY: a live player and a writable record of the size passed.
        let code = unsafe { sys::moq_media_player_poll_output(self.as_ptr(), &mut raw, size) };
        if code == sys::MOQ_DONE {
            return Ok(None);
        }
        ffi::check(code)?;
        // Owned from here: a refused record is released by its drop.
        let output = PlayerOutput {
            raw,
            owner: self.receiver.owner(),
            _player: PhantomData,
        };
        ffi::check_stamp(output.raw.struct_size, size, size)?;
        if let Some(floor) = output_detail_floor(&output.raw) {
            if (output.raw.detail_size as usize) < floor {
                return Err(Error::ABI_MISMATCH);
            }
        }
        Ok(Some(output))
    }

    /// The next event, or `None` when none is queued. A full queue drops its
    /// oldest diagnostic (counted in [`PlayerStats::events_dropped`]);
    /// [`PlayerEventKind::Ended`] and [`PlayerEventKind::Closed`] are never
    /// dropped, and a full queue never stops playback.
    pub fn poll_event(&self) -> Result<Option<PlayerEvent>> {
        let mut raw = sys::moq_media_player_event_t::default();
        let size = mem::size_of_val(&raw);
        // SAFETY: a live player and a writable record of the size passed.
        let code = unsafe { sys::moq_media_player_poll_event(self.as_ptr(), &mut raw, size) };
        if code == sys::MOQ_DONE {
            return Ok(None);
        }
        ffi::check(code)?;
        ffi::check_stamp(raw.struct_size, size, size)?;
        PlayerEvent::from_raw(&raw, self.receiver.owner()).map(Some)
    }

    /// Report what a decoder observed, at `now_us`. Feedback naming an
    /// older configuration generation than the track's is ignored and
    /// counted ([`PlayerStats::feedback_stale`]); `Ok` covers both.
    /// [`Error::INVAL`] for a track that is not selected.
    pub fn handle_feedback(&self, feedback: &Feedback<'_>, now_us: u64) -> Result<()> {
        let track = self.receiver.own(feedback.track)?;
        let mut raw = sys::moq_media_player_feedback_t::default();
        // SAFETY: a writable struct of exactly the size passed.
        unsafe { sys::moq_media_player_feedback_init_sized(&mut raw, mem::size_of_val(&raw)) };
        raw.track = track;
        raw.configuration_generation = feedback.configuration_generation;
        match feedback.kind {
            FeedbackKind::DecoderQueue {
                depth,
                max_recommended,
            } => {
                raw.kind = sys::MOQ_MEDIA_PLAYER_FEEDBACK_DECODER_QUEUE;
                raw.u.decoder_queue.depth = depth;
                raw.u.decoder_queue.max_recommended = max_recommended;
            }
            FeedbackKind::DecodeError { message } => {
                raw.kind = sys::MOQ_MEDIA_PLAYER_FEEDBACK_DECODE_ERROR;
                raw.u.decode_error.message = ffi::span(message);
            }
            FeedbackKind::FlushComplete => {
                raw.kind = sys::MOQ_MEDIA_PLAYER_FEEDBACK_FLUSH_COMPLETE;
            }
        }
        // SAFETY: a live player; the record and the message it borrows are
        // alive for the call.
        ffi::check(unsafe { sys::moq_media_player_handle_feedback(self.as_ptr(), &raw, now_us) })
    }

    /// A statistics snapshot.
    pub fn stats(&self) -> Result<PlayerStats> {
        let mut stats = sys::moq_media_player_stats_t::default();
        let size = mem::size_of_val(&stats);
        // SAFETY: a live player and a writable struct of the size passed.
        ffi::check(unsafe { sys::moq_media_player_get_stats(self.as_ptr(), &mut stats, size) })?;
        ffi::check_stamp(stats.struct_size, sys::MOQ_MEDIA_PLAYER_STATS_V0_SIZE, size)?;
        Ok(PlayerStats {
            tracks_selected: stats.tracks_selected,
            outputs_queued: stats.outputs_queued,
            events_queued: stats.events_queued,
            events_dropped: stats.events_dropped,
            held_objects: stats.held_objects,
            held_bytes: stats.held_bytes,
            objects_taken: stats.objects_taken,
            objects_pushed: stats.objects_pushed,
            objects_dropped: stats.objects_dropped,
            objects_discarded: stats.objects_discarded,
            terminals_taken: stats.terminals_taken,
            units_output: stats.units_output,
            configures_output: stats.configures_output,
            discontinuities_output: stats.discontinuities_output,
            ends_output: stats.ends_output,
            timeline_records_discarded: stats.timeline_records_discarded,
            feedback_stale: stats.feedback_stale,
            outputs_bytes: stats.outputs_bytes,
            selections_failed: stats.selections_failed,
        })
    }

    /// Closed: the receiver is terminal and every output, event, held input
    /// and engine buffer is empty.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        // SAFETY: a live player.
        unsafe { sys::moq_media_player_is_closed(self.as_ptr()) }
    }

    /// The receiver failed.
    #[must_use]
    pub fn is_fatal(&self) -> bool {
        // SAFETY: a live player.
        unsafe { sys::moq_media_player_is_fatal(self.as_ptr()) }
    }

    /// The fatal code, when [`is_fatal`](Player::is_fatal).
    #[must_use]
    pub fn fatal_code(&self) -> u64 {
        // SAFETY: a live player.
        unsafe { sys::moq_media_player_fatal_code(self.as_ptr()) }
    }
}

impl Drop for Player {
    fn drop(&mut self) {
        // SAFETY: the sole owner; no output borrows the player any more.
        // The player goes first, then the receiver it was attached to.
        unsafe {
            sys::moq_media_player_destroy(self.raw.as_ptr());
            ManuallyDrop::drop(&mut self.receiver);
        }
    }
}

// The receiver, not the raw pointer a derive would print.
impl fmt::Debug for Player {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Player")
            .field("receiver", &*self.receiver)
            .finish_non_exhaustive()
    }
}

/// The observing calls of a player's receiver, borrowed from the player.
/// Polling, waiting on and subscribing the receiver belong to the player
/// while it exists, so they are not offered here.
#[derive(Clone, Copy, Debug)]
pub struct ReceiverView<'player> {
    receiver: &'player Receiver,
}

impl<'player> ReceiverView<'player> {
    /// The endpoint the receiver is attached to (to wake or interrupt a
    /// [`Player::wait`] from another thread).
    #[must_use]
    pub fn endpoint(&self) -> &'player Endpoint {
        self.receiver.endpoint()
    }
    /// A track's current description; see [`Receiver::track_description`].
    pub fn track_description(&self, track: Track) -> Result<TrackDescription<'player>> {
        self.receiver.track_description(track)
    }
    /// A track's delivery state; see [`Receiver::track_state`].
    pub fn track_state(&self, track: Track) -> Result<TrackState> {
        self.receiver.track_state(track)
    }
    /// See [`Receiver::find_content_protection`].
    #[must_use]
    pub fn find_content_protection(&self, ref_id: &[u8]) -> Option<ContentProtectionRef<'player>> {
        self.receiver.find_content_protection(ref_id)
    }
    /// See [`Receiver::stats`].
    pub fn stats(&self) -> Result<ReceiverStats> {
        self.receiver.stats()
    }
    /// See [`Receiver::info`].
    pub fn info(&self) -> Result<ReceiverInfo> {
        self.receiver.info()
    }
    /// See [`Receiver::is_closed`].
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.receiver.is_closed()
    }
    /// See [`Receiver::is_fatal`].
    #[must_use]
    pub fn is_fatal(&self) -> bool {
        self.receiver.is_fatal()
    }
    /// See [`Receiver::fatal_code`].
    #[must_use]
    pub fn fatal_code(&self) -> u64 {
        self.receiver.fatal_code()
    }
    /// The underlying `moq_media_receiver_t`, for observing calls only:
    /// never poll it while the player exists.
    #[must_use]
    pub fn as_ptr(&self) -> *mut sys::moq_media_receiver_t {
        self.receiver.as_ptr()
    }
}

// -- outputs ----------------------------------------------------------------------

/// The size of the detail a known output kind's view reads.
fn output_detail_floor(raw: &sys::moq_media_player_output_t) -> Option<usize> {
    // SAFETY: only the sizes of the union's members are taken.
    unsafe {
        match raw.kind {
            sys::MOQ_MEDIA_PLAYER_OUTPUT_CONFIGURE => Some(mem::size_of_val(&raw.u.configure)),
            sys::MOQ_MEDIA_PLAYER_OUTPUT_UNIT => Some(mem::size_of_val(&raw.u.unit)),
            sys::MOQ_MEDIA_PLAYER_OUTPUT_DISCONTINUITY => {
                Some(mem::size_of_val(&raw.u.discontinuity))
            }
            sys::MOQ_MEDIA_PLAYER_OUTPUT_TIMELINE => Some(mem::size_of_val(&raw.u.timeline)),
            sys::MOQ_MEDIA_PLAYER_OUTPUT_END => Some(mem::size_of_val(&raw.u.end)),
            _ => None,
        }
    }
}

/// One output of a selected track, borrowed from its player
/// (`moq_media_player_output_t`). It holds references to buffers the engine
/// shares and releases them when dropped, whatever its kind; it cannot
/// leave the player's thread or outlive the player.
pub struct PlayerOutput<'player> {
    raw: sys::moq_media_player_output_t,
    owner: u64,
    _player: PhantomData<&'player Player>,
}

impl PlayerOutput<'_> {
    /// The output's track.
    #[must_use]
    pub fn track(&self) -> Option<Track> {
        NonNull::new(self.raw.track).map(|raw| Track::new(raw, self.owner))
    }
    /// The track's timeline (nonzero, player-assigned).
    #[must_use]
    pub fn timeline_id(&self) -> u32 {
        self.raw.timeline_id
    }
    /// The track's decoder epoch, incremented by every configuration and
    /// every discontinuity; feedback names the generation it observed.
    #[must_use]
    pub fn configuration_generation(&self) -> u64 {
        self.raw.configuration_generation
    }
    /// What the output says.
    #[must_use]
    pub fn detail(&self) -> OutputDetail<'_> {
        // SAFETY: the kind selects the union member, and poll_output checked
        // that the library's detail covers it.
        unsafe {
            match self.raw.kind {
                sys::MOQ_MEDIA_PLAYER_OUTPUT_CONFIGURE => {
                    OutputDetail::Configure(DecoderConfig(&self.raw.u.configure))
                }
                sys::MOQ_MEDIA_PLAYER_OUTPUT_UNIT => {
                    OutputDetail::Unit(AccessUnit(&self.raw.u.unit))
                }
                sys::MOQ_MEDIA_PLAYER_OUTPUT_DISCONTINUITY => {
                    let detail = &self.raw.u.discontinuity;
                    OutputDetail::Discontinuity(Discontinuity {
                        reason: DiscontinuityReason::from_raw(detail.reason),
                        decoder_reset_required: detail.decoder_reset_required,
                    })
                }
                sys::MOQ_MEDIA_PLAYER_OUTPUT_TIMELINE => {
                    OutputDetail::Timeline(Timeline::from_raw(&self.raw.u.timeline))
                }
                sys::MOQ_MEDIA_PLAYER_OUTPUT_END => {
                    let detail = &self.raw.u.end;
                    OutputDetail::End(TrackEnd {
                        cause: EndCause::from_raw(detail.cause),
                        disposition: EndDisposition::from_raw(detail.disposition),
                    })
                }
                kind => OutputDetail::Other(kind),
            }
        }
    }
    /// The raw record; its references stay owned by `self`.
    #[must_use]
    pub fn raw(&self) -> &sys::moq_media_player_output_t {
        &self.raw
    }
}

impl Drop for PlayerOutput<'_> {
    fn drop(&mut self) {
        // SAFETY: the references a successful poll transferred, released
        // once, on the player's thread; cleanup knows every kind the
        // library emits.
        unsafe { sys::moq_media_player_output_cleanup(&mut self.raw) }
    }
}

// The kind and the track, not the raw union a derive would print.
impl fmt::Debug for PlayerOutput<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PlayerOutput")
            .field("track", &self.track())
            .field("detail", &self.detail())
            .finish_non_exhaustive()
    }
}

/// What an output says, borrowed from it.
#[non_exhaustive]
#[derive(Debug)]
pub enum OutputDetail<'output> {
    /// A self-contained decoder configuration, for the units after it.
    Configure(DecoderConfig<'output>),
    /// One access unit, in decode order.
    Unit(AccessUnit<'output>),
    /// The decoder must be reset before the next unit.
    Discontinuity(Discontinuity),
    /// The presentation mapping of the track's timeline, before its first
    /// unit.
    Timeline(Timeline),
    /// After the track's last unit: why and how it ended.
    End(TrackEnd),
    /// A kind this crate does not read; the output still releases what it
    /// holds when dropped.
    Other(u32),
}

/// A decoder configuration snapshot, borrowed from its output.
#[derive(Clone, Copy)]
pub struct DecoderConfig<'output>(
    &'output sys::moq_media_player_output__bindgen_ty_1__bindgen_ty_1,
);

impl<'output> DecoderConfig<'output> {
    /// The media type.
    #[must_use]
    pub fn media_type(&self) -> Option<MediaType> {
        MediaType::from_raw(self.0.media_type)
    }
    /// The packaging.
    #[must_use]
    pub fn packaging(&self) -> Option<Packaging> {
        Packaging::from_raw(self.0.packaging)
    }
    /// The codec string, e.g. `avc1.42e01e`.
    #[must_use]
    pub fn codec(&self) -> &'output [u8] {
        // SAFETY: a reference the output owns until it is dropped.
        unsafe { buffer_bytes(self.0.codec) }
    }
    /// The decoder extradata (avcC, AudioSpecificConfig, ...); `None` when
    /// the track has none.
    #[must_use]
    pub fn codec_config(&self) -> Option<&'output [u8]> {
        // SAFETY: as for codec.
        (!self.0.codec_config.is_null()).then(|| unsafe { buffer_bytes(self.0.codec_config) })
    }
    /// The media timescale.
    #[must_use]
    pub fn timescale(&self) -> u32 {
        self.0.timescale
    }
    /// Video width, 0 when unknown.
    #[must_use]
    pub fn width(&self) -> u32 {
        self.0.width
    }
    /// Video height, 0 when unknown.
    #[must_use]
    pub fn height(&self) -> u32 {
        self.0.height
    }
    /// Audio sample rate, 0 when unknown.
    #[must_use]
    pub fn samplerate(&self) -> u32 {
        self.0.samplerate
    }
    /// Audio channels, 0 when unknown.
    #[must_use]
    pub fn channel_count(&self) -> u32 {
        self.0.channel_count
    }
}

// The values, not the raw references a derive would print.
impl fmt::Debug for DecoderConfig<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DecoderConfig")
            .field("media_type", &self.media_type())
            .field("codec", &String::from_utf8_lossy(self.codec()))
            .field("codec_config", &self.codec_config().map(<[u8]>::len))
            .finish_non_exhaustive()
    }
}

/// One access unit, borrowed from its output. On RAW/LOC the decode time
/// is a copy of the LOC media time, equal to the presentation time, and
/// not evidence of a decode clock: decode order is output order, and with
/// frame reordering it can decrease from one unit to the next.
#[derive(Clone, Copy)]
pub struct AccessUnit<'output>(&'output sys::moq_media_player_output__bindgen_ty_1__bindgen_ty_2);

impl<'output> AccessUnit<'output> {
    /// The unit's Location.
    #[must_use]
    pub fn location(&self) -> Location {
        Location {
            group_id: self.0.group_id,
            object_id: self.0.object_id,
        }
    }
    /// Decode time, microseconds of the timeline's domain.
    #[must_use]
    pub fn decode_time_us(&self) -> u64 {
        self.0.decode_time_us
    }
    /// Presentation time, microseconds of the timeline's domain.
    #[must_use]
    pub fn presentation_time_us(&self) -> u64 {
        self.0.presentation_time_us
    }
    /// Composition offset.
    #[must_use]
    pub fn composition_offset_us(&self) -> i64 {
        self.0.composition_offset_us
    }
    /// Capture time, Unix-epoch microseconds, when the unit carried one.
    #[must_use]
    pub fn capture_time_us(&self) -> Option<u64> {
        self.0.has_capture_time.then_some(self.0.capture_time_us)
    }
    /// Video: whether the unit is marked independent; `None` when unmarked.
    #[must_use]
    pub fn independent(&self) -> Option<bool> {
        self.0.independence_known.then_some(self.0.independent)
    }
    /// The unit is present (true in this release).
    #[must_use]
    pub fn present(&self) -> bool {
        self.0.present
    }
    /// The unit's duration, when known.
    #[must_use]
    pub fn sample_duration_us(&self) -> Option<u32> {
        (self.0.sample_duration_us != 0).then_some(self.0.sample_duration_us)
    }
    /// The unit's bytes.
    #[must_use]
    pub fn data(&self) -> &'output [u8] {
        // SAFETY: a reference the output owns until it is dropped.
        let payload = unsafe { buffer_bytes(self.0.payload) };
        let start = self.0.offset.min(payload.len());
        let end = start.saturating_add(self.0.len).min(payload.len());
        &payload[start..end]
    }
}

// The identity and timing, not the raw references a derive would print.
impl fmt::Debug for AccessUnit<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AccessUnit")
            .field("location", &self.location())
            .field("presentation_time_us", &self.presentation_time_us())
            .field("len", &self.data().len())
            .finish_non_exhaustive()
    }
}

/// The bytes of an rcbuf an output owns (empty for NULL).
///
/// # Safety
/// `buffer` must be NULL or a live rcbuf that outlives `'a`.
unsafe fn buffer_bytes<'a>(buffer: *mut sys::moq_rcbuf_t) -> &'a [u8] {
    if buffer.is_null() {
        return &[];
    }
    // SAFETY: the caller's contract.
    unsafe {
        ffi::bytes(sys::moq_bytes_t {
            data: sys::moq_rcbuf_data(buffer),
            len: sys::moq_rcbuf_len(buffer),
        })
    }
}

/// Why a track's decoder must be reset (`moq_media_player_discontinuity_t`).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DiscontinuityReason {
    /// Units were skipped.
    Gap,
    /// After decode-error feedback.
    DecodeError,
    /// A reason this crate does not know.
    Other(u32),
}

impl DiscontinuityReason {
    fn from_raw(code: u32) -> DiscontinuityReason {
        match code {
            sys::MOQ_MEDIA_PLAYER_DISCONTINUITY_GAP => DiscontinuityReason::Gap,
            sys::MOQ_MEDIA_PLAYER_DISCONTINUITY_DECODE_ERROR => DiscontinuityReason::DecodeError,
            code => DiscontinuityReason::Other(code),
        }
    }
}

/// A discontinuity output.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Discontinuity {
    /// Why.
    pub reason: DiscontinuityReason,
    /// The decoder must be reset (true in this release).
    pub decoder_reset_required: bool,
}

/// A presentation mapping (`moq_media_timeline_t`): a unit of the timeline
/// presents at `anchor_local_us + (presentation_time_us - anchor_media_us) /
/// rate` on the application's clock.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Timeline {
    /// The clock the units' times are in.
    pub domain: TimeDomain,
    /// Nonzero, player-assigned.
    pub timeline_id: u32,
    /// Changes when the mapping is re-anchored.
    pub timeline_epoch: u64,
    /// The media time anchored.
    pub anchor_media_us: u64,
    /// Where on the application's clock it presents.
    pub anchor_local_us: u64,
    /// The rate (1.0 in this release).
    pub rate: f64,
}

impl Timeline {
    fn from_raw(raw: &sys::moq_media_timeline_t) -> Timeline {
        Timeline {
            domain: TimeDomain::from_raw(raw.domain),
            timeline_id: raw.timeline_id,
            timeline_epoch: raw.timeline_epoch,
            anchor_media_us: raw.anchor_media_us,
            anchor_local_us: raw.anchor_local_us,
            rate: raw.rate,
        }
    }
}

/// What ended a track (`moq_media_player_end_cause_t`), in precedence
/// order: when several apply before the end, the highest (latest listed)
/// wins.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EndCause {
    /// Its END_OF_TRACK.
    PeerEndOfTrack,
    /// The peer ended the track's delivery without an END_OF_TRACK the
    /// player could use: an error done, or an end for good whose
    /// END_OF_TRACK did not arrive within the tail wait.
    PeerDone,
    /// Withdrawn from the catalog (not a completed broadcast's removal of a
    /// track the peer had already ended).
    Removed,
    /// A receiver-local failure.
    LocalFailure,
    /// The receiver closed.
    Closed,
    /// The receiver failed.
    Fatal,
    /// A cause this crate does not know.
    Other(u32),
}

impl EndCause {
    fn from_raw(code: u32) -> EndCause {
        match code {
            sys::MOQ_MEDIA_PLAYER_END_PEER_END_OF_TRACK => EndCause::PeerEndOfTrack,
            sys::MOQ_MEDIA_PLAYER_END_PEER_DONE => EndCause::PeerDone,
            sys::MOQ_MEDIA_PLAYER_END_REMOVED => EndCause::Removed,
            sys::MOQ_MEDIA_PLAYER_END_LOCAL_FAILURE => EndCause::LocalFailure,
            sys::MOQ_MEDIA_PLAYER_END_CLOSED => EndCause::Closed,
            sys::MOQ_MEDIA_PLAYER_END_FATAL => EndCause::Fatal,
            code => EndCause::Other(code),
        }
    }
}

/// How a track's input was settled (`moq_media_player_end_disposition_t`).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EndDisposition {
    /// Every location up to its END_OF_TRACK was played or known not to
    /// exist (only with [`EndCause::PeerEndOfTrack`]).
    Complete,
    /// A bounded wait gave up on missing input (the tail wait, or a gap the
    /// engine stopped waiting for).
    TailCutoff,
    /// No wait was possible (local failure, closed, fatal).
    Immediate,
    /// A disposition this crate does not know.
    Other(u32),
}

impl EndDisposition {
    fn from_raw(code: u32) -> EndDisposition {
        match code {
            sys::MOQ_MEDIA_PLAYER_END_COMPLETE => EndDisposition::Complete,
            sys::MOQ_MEDIA_PLAYER_END_TAIL_CUTOFF => EndDisposition::TailCutoff,
            sys::MOQ_MEDIA_PLAYER_END_IMMEDIATE => EndDisposition::Immediate,
            code => EndDisposition::Other(code),
        }
    }
}

/// A track's end.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TrackEnd {
    /// What ended it.
    pub cause: EndCause,
    /// How its input was settled.
    pub disposition: EndDisposition,
}

// -- events -----------------------------------------------------------------------

/// Why the engine waited on a missing unit (`moq_playback_gap_cause_t`).
/// The unit may still arrive another way; only the report differs.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GapCause {
    /// Nothing said why it is missing.
    Unknown,
    /// A stream reset left it undelivered.
    StreamReset,
    /// The receiver's overflow policy dropped its group.
    ReceiverDrop,
    /// The player refused that one object.
    LocalRefusal,
    /// A cause this crate does not know.
    Other(u32),
}

impl GapCause {
    fn from_raw(code: u32) -> GapCause {
        match code {
            sys::MOQ_PLAYBACK_GAP_CAUSE_UNKNOWN => GapCause::Unknown,
            sys::MOQ_PLAYBACK_GAP_CAUSE_STREAM_RESET => GapCause::StreamReset,
            sys::MOQ_PLAYBACK_GAP_CAUSE_RECEIVER_DROP => GapCause::ReceiverDrop,
            sys::MOQ_PLAYBACK_GAP_CAUSE_LOCAL_REFUSAL => GapCause::LocalRefusal,
            code => GapCause::Other(code),
        }
    }
}

/// A gap, waited on or crossed.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Gap {
    /// The engine waits on a missing unit; how the wait settles is reported
    /// by a later skip or discontinuity, or the unit simply plays.
    Waiting {
        /// The missing unit.
        missing: Location,
        /// Why it is missing.
        cause: GapCause,
    },
    /// The track continued past a hole without a decoder reset (a codec
    /// whose units stand alone).
    Crossed {
        /// Where the hole starts, microseconds of the timeline's domain.
        hole_start_us: u64,
        /// How long it is.
        hole_duration_us: u64,
    },
}

/// Why an object was dropped: the engine's reasons
/// (`moq_playback_drop_reason_t`), as is, and the player's own.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DropReason {
    /// Malformed LOC properties.
    MalformedLoc,
    /// A malformed CMAF fragment.
    MalformedCmaf,
    /// No timestamp.
    MissingTimestamp,
    /// CMAF decode times going backwards.
    NonMonotonicDts,
    /// More samples than the engine takes per object.
    UnsupportedMultiSample,
    /// Below the release cursor, never held by the engine.
    Stale,
    /// Waiting for a keyframe.
    KeyframeWait,
    /// A CMAF object of more than one chunk.
    UnsupportedMultiChunk,
    /// Larger than the engine.
    Oversize,
    /// Its track had ended.
    LateAfterEnd,
    /// Not its timeline's time domain.
    TimeDomain,
    /// No decoder configuration yet.
    ConfigurationMissing,
    /// The engine refused it.
    Malformed,
    /// A reason this crate does not know.
    Other(u32),
}

impl DropReason {
    fn from_raw(code: u32) -> DropReason {
        match code {
            sys::MOQ_PLAYBACK_DROP_MALFORMED_LOC => DropReason::MalformedLoc,
            sys::MOQ_PLAYBACK_DROP_MALFORMED_CMAF => DropReason::MalformedCmaf,
            sys::MOQ_PLAYBACK_DROP_MISSING_TIMESTAMP => DropReason::MissingTimestamp,
            sys::MOQ_PLAYBACK_DROP_NON_MONOTONIC_DTS => DropReason::NonMonotonicDts,
            sys::MOQ_PLAYBACK_DROP_UNSUPPORTED_MULTI_SAMPLE => DropReason::UnsupportedMultiSample,
            sys::MOQ_PLAYBACK_DROP_STALE => DropReason::Stale,
            sys::MOQ_PLAYBACK_DROP_KEYFRAME_WAIT => DropReason::KeyframeWait,
            sys::MOQ_PLAYBACK_DROP_UNSUPPORTED_MULTI_CHUNK => DropReason::UnsupportedMultiChunk,
            sys::MOQ_MEDIA_PLAYER_DROP_OVERSIZE => DropReason::Oversize,
            sys::MOQ_MEDIA_PLAYER_DROP_LATE_AFTER_END => DropReason::LateAfterEnd,
            sys::MOQ_MEDIA_PLAYER_DROP_TIME_DOMAIN => DropReason::TimeDomain,
            sys::MOQ_MEDIA_PLAYER_DROP_CONFIGURATION_MISSING => DropReason::ConfigurationMissing,
            sys::MOQ_MEDIA_PLAYER_DROP_MALFORMED => DropReason::Malformed,
            code => DropReason::Other(code),
        }
    }
}

/// Why [`Selection::Auto`] left a render group out or timed it
/// independently (`MOQ_MEDIA_PLAYER_SYNC_*`).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SyncReason {
    /// Its tracks' timestamps are in different domains.
    MixedDomains,
    /// Its tracks are in application media time with no shared epoch.
    NoSharedEpoch,
    /// A reason this crate does not know.
    Other(u32),
}

impl SyncReason {
    fn from_raw(code: u32) -> SyncReason {
        match code {
            sys::MOQ_MEDIA_PLAYER_SYNC_MIXED_DOMAINS => SyncReason::MixedDomains,
            sys::MOQ_MEDIA_PLAYER_SYNC_NO_SHARED_EPOCH => SyncReason::NoSharedEpoch,
            code => SyncReason::Other(code),
        }
    }
}

/// What an event reports (`moq_media_player_event_kind_t`), with its detail.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PlayerEventKind {
    /// A track was announced and is not selected.
    TrackAvailable,
    /// A track left the catalog.
    TrackRemoved,
    /// The catalog is ready, and [`Selection::Auto`] has chosen.
    Ready,
    /// A timeline's first unit was released.
    Started {
        /// The timeline.
        timeline_id: u32,
    },
    /// A gap was waited on or crossed.
    Gap(Gap),
    /// Groups were abandoned.
    Skip {
        /// The first group abandoned.
        from_group_id: u64,
        /// The group play resumed at.
        to_group_id: u64,
    },
    /// The live backlog was shed.
    Shed {
        /// Groups dropped.
        dropped_groups: u32,
        /// Groups left.
        remaining_groups: u32,
    },
    /// One object was dropped.
    Drop {
        /// Why.
        reason: DropReason,
        /// Its Location.
        location: Location,
    },
    /// A stream reset or a receiver drop lost objects.
    Loss {
        /// [`StreamTerminalKind::Reset`] or
        /// [`StreamTerminalKind::ReceiverDrop`].
        kind: StreamTerminalKind,
        /// The first group touched.
        group_id: u64,
        /// The last group touched.
        last_group_id: u64,
        /// The peer's RESET_STREAM code, for a reset.
        error_code: u64,
    },
    /// Video waits for an entry point.
    Stall,
    /// Every selected track ended.
    Ended,
    /// The player closed.
    Closed,
    /// [`Selection::Auto`] played a render group with a timeline per track
    /// ([`TimingPolicy::AllowIndependent`]).
    TimingIndependent {
        /// The group, when the tracks have one.
        render_group: Option<i32>,
        /// Why they could not share a timeline.
        reason: SyncReason,
    },
    /// [`Selection::Auto`] left a render group out.
    SyncUnavailable {
        /// The group, when the tracks have one.
        render_group: Option<i32>,
        /// Why its tracks could not share a timeline.
        reason: SyncReason,
    },
    /// Known absence was demoted: the engine may wait again for these
    /// groups' ids.
    KnowledgeForgotten {
        /// The first group of the coalesced range.
        first_group_id: u64,
        /// Its last group.
        last_group_id: u64,
    },
    /// A kind this crate does not know.
    Other(u32),
}

/// One player diagnostic (`moq_media_player_event_t`): a plain value that
/// owns nothing.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PlayerEvent {
    /// What happened.
    pub kind: PlayerEventKind,
    /// The track; `None` for a player-wide event.
    pub track: Option<Track>,
}

impl PlayerEvent {
    /// The event, its detail read under the library's detail size: a known
    /// kind whose detail it does not cover is [`Error::ABI_MISMATCH`].
    fn from_raw(raw: &sys::moq_media_player_event_t, owner: u64) -> Result<PlayerEvent> {
        let covered = |size: usize| {
            if (raw.detail_size as usize) < size {
                Err(Error::ABI_MISMATCH)
            } else {
                Ok(())
            }
        };
        // SAFETY: the kind selects the union member, read only once the
        // library's detail covers it.
        let kind = unsafe {
            match raw.kind {
                sys::MOQ_MEDIA_PLAYER_EVENT_TRACK_AVAILABLE => PlayerEventKind::TrackAvailable,
                sys::MOQ_MEDIA_PLAYER_EVENT_TRACK_REMOVED => PlayerEventKind::TrackRemoved,
                sys::MOQ_MEDIA_PLAYER_EVENT_READY => PlayerEventKind::Ready,
                sys::MOQ_MEDIA_PLAYER_EVENT_STARTED => {
                    let detail = &raw.u.started;
                    covered(mem::size_of_val(detail))?;
                    PlayerEventKind::Started {
                        timeline_id: detail.timeline_id,
                    }
                }
                sys::MOQ_MEDIA_PLAYER_EVENT_GAP => {
                    let detail = &raw.u.gap;
                    covered(mem::size_of_val(detail))?;
                    PlayerEventKind::Gap(if detail.crossed {
                        Gap::Crossed {
                            hole_start_us: detail.hole_start_us,
                            hole_duration_us: detail.hole_duration_us,
                        }
                    } else {
                        Gap::Waiting {
                            missing: Location {
                                group_id: detail.group_id,
                                object_id: detail.object_id,
                            },
                            cause: GapCause::from_raw(detail.cause),
                        }
                    })
                }
                sys::MOQ_MEDIA_PLAYER_EVENT_SKIP => {
                    let detail = &raw.u.skip;
                    covered(mem::size_of_val(detail))?;
                    PlayerEventKind::Skip {
                        from_group_id: detail.from_group_id,
                        to_group_id: detail.to_group_id,
                    }
                }
                sys::MOQ_MEDIA_PLAYER_EVENT_SHED => {
                    let detail = &raw.u.shed;
                    covered(mem::size_of_val(detail))?;
                    PlayerEventKind::Shed {
                        dropped_groups: detail.dropped_groups,
                        remaining_groups: detail.remaining_groups,
                    }
                }
                sys::MOQ_MEDIA_PLAYER_EVENT_DROP => {
                    let detail = &raw.u.drop;
                    covered(mem::size_of_val(detail))?;
                    PlayerEventKind::Drop {
                        reason: DropReason::from_raw(detail.reason),
                        location: Location {
                            group_id: detail.group_id,
                            object_id: detail.object_id,
                        },
                    }
                }
                sys::MOQ_MEDIA_PLAYER_EVENT_LOSS => {
                    let detail = &raw.u.loss;
                    covered(mem::size_of_val(detail))?;
                    PlayerEventKind::Loss {
                        kind: StreamTerminalKind::from_raw(detail.kind),
                        group_id: detail.group_id,
                        last_group_id: detail.last_group_id,
                        error_code: detail.error_code,
                    }
                }
                sys::MOQ_MEDIA_PLAYER_EVENT_STALL => PlayerEventKind::Stall,
                sys::MOQ_MEDIA_PLAYER_EVENT_ENDED => PlayerEventKind::Ended,
                sys::MOQ_MEDIA_PLAYER_EVENT_CLOSED => PlayerEventKind::Closed,
                kind @ (sys::MOQ_MEDIA_PLAYER_EVENT_TIMING_INDEPENDENT
                | sys::MOQ_MEDIA_PLAYER_EVENT_SYNC_UNAVAILABLE) => {
                    let detail = &raw.u.sync;
                    covered(mem::size_of_val(detail))?;
                    let render_group = detail.has_render_group.then_some(detail.render_group);
                    let reason = SyncReason::from_raw(detail.reason);
                    if kind == sys::MOQ_MEDIA_PLAYER_EVENT_TIMING_INDEPENDENT {
                        PlayerEventKind::TimingIndependent {
                            render_group,
                            reason,
                        }
                    } else {
                        PlayerEventKind::SyncUnavailable {
                            render_group,
                            reason,
                        }
                    }
                }
                sys::MOQ_MEDIA_PLAYER_EVENT_KNOWLEDGE_FORGOTTEN => {
                    let detail = &raw.u.knowledge_forgotten;
                    covered(mem::size_of_val(detail))?;
                    PlayerEventKind::KnowledgeForgotten {
                        first_group_id: detail.first_group_id,
                        last_group_id: detail.last_group_id,
                    }
                }
                kind => PlayerEventKind::Other(kind),
            }
        };
        Ok(PlayerEvent {
            kind,
            track: NonNull::new(raw.track).map(|track| Track::new(track, owner)),
        })
    }
}

// -- feedback ---------------------------------------------------------------------

/// What a decoder observed, for [`Player::handle_feedback`]
/// (`moq_media_player_feedback_t`). It is the application's record; it is
/// not `#[non_exhaustive]`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Feedback<'a> {
    /// The selected track.
    pub track: Track,
    /// The [`PlayerOutput::configuration_generation`] of the outputs it
    /// observed.
    pub configuration_generation: u64,
    /// What it observed.
    pub kind: FeedbackKind<'a>,
}

/// One kind of decoder feedback.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FeedbackKind<'a> {
    /// The track's decoder queue: release pauses while `depth` exceeds
    /// `max_recommended`, and resumes below it.
    DecoderQueue {
        /// Units queued at the decoder.
        depth: u32,
        /// The depth above which release pauses.
        max_recommended: u32,
    },
    /// The decoder failed: the track resumes at its next independent unit,
    /// behind a [`DiscontinuityReason::DecodeError`] discontinuity.
    DecodeError {
        /// The decoder's message, borrowed for the call.
        message: &'a [u8],
    },
    /// The decoder flushed after the track's end (counted only).
    FlushComplete,
}

/// A [`Player`] statistics snapshot (`moq_media_player_stats_t`).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PlayerStats {
    /// Tracks selected now.
    pub tracks_selected: u32,
    /// Outputs queued.
    pub outputs_queued: u64,
    /// Events queued.
    pub events_queued: u64,
    /// Diagnostics a full event queue dropped.
    pub events_dropped: u64,
    /// The object held for engine room: 0 or 1.
    pub held_objects: u64,
    /// Its bytes.
    pub held_bytes: u64,
    /// Objects polled from the receiver.
    pub objects_taken: u64,
    /// Objects the engine accepted.
    pub objects_pushed: u64,
    /// The player's own drops ([`PlayerEventKind::Drop`]).
    pub objects_dropped: u64,
    /// Objects for no selected track.
    pub objects_discarded: u64,
    /// Stream terminals polled from the receiver.
    pub terminals_taken: u64,
    /// Unit outputs.
    pub units_output: u64,
    /// Configuration outputs.
    pub configures_output: u64,
    /// Discontinuity outputs.
    pub discontinuities_output: u64,
    /// End outputs.
    pub ends_output: u64,
    /// SAP and media-timeline records discarded.
    pub timeline_records_discarded: u64,
    /// Feedback ignored as stale.
    pub feedback_stale: u64,
    /// Bytes queued outputs keep charged to the engine's buffer budget
    /// until they are polled.
    pub outputs_bytes: u64,
    /// Tracks [`Selection::Auto`] chose that could not be selected (they
    /// stay available).
    pub selections_failed: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_sets_round_trip() {
        assert_eq!(Selection::Auto.raw(), sys::MOQ_MEDIA_SELECTION_AUTO);
        assert_eq!(Selection::Manual.raw(), sys::MOQ_MEDIA_SELECTION_MANUAL);
        assert_eq!(
            TimingPolicy::AllowIndependent.raw(),
            sys::MOQ_MEDIA_TIMING_ALLOW_INDEPENDENT
        );
        assert_eq!(EndCause::from_raw(99), EndCause::Other(99));
        assert_eq!(
            EndDisposition::from_raw(sys::MOQ_MEDIA_PLAYER_END_TAIL_CUTOFF),
            EndDisposition::TailCutoff
        );
        assert_eq!(
            DropReason::from_raw(sys::MOQ_MEDIA_PLAYER_DROP_TIME_DOMAIN),
            DropReason::TimeDomain
        );
        assert_eq!(
            DropReason::from_raw(sys::MOQ_PLAYBACK_DROP_UNSUPPORTED_MULTI_CHUNK),
            DropReason::UnsupportedMultiChunk
        );
        assert_eq!(DropReason::from_raw(63), DropReason::Other(63));
        assert_eq!(GapCause::from_raw(0), GapCause::Other(0));
        assert_eq!(SyncReason::from_raw(3), SyncReason::Other(3));
        assert_eq!(
            DiscontinuityReason::from_raw(sys::MOQ_MEDIA_PLAYER_DISCONTINUITY_DECODE_ERROR),
            DiscontinuityReason::DecodeError
        );
    }

    #[test]
    fn the_engine_configuration_keeps_its_defaults() {
        let raw = PlaybackConfig::new().raw().expect("no duration to convert");
        let mut defaults = sys::moq_playback_cfg_t::default();
        // SAFETY: a writable struct of exactly the size passed.
        unsafe { sys::moq_playback_cfg_init_sized(&mut defaults, mem::size_of_val(&defaults)) };
        assert_eq!(raw.max_tracks, defaults.max_tracks);
        assert_eq!(raw.gap_timeout_us, defaults.gap_timeout_us);
        let raw = PlaybackConfig::new()
            .max_tracks(2)
            .gap_timeout(Duration::from_millis(250))
            .max_release_per_tick(0)
            .raw()
            .expect("exact microseconds");
        assert_eq!(
            (raw.max_tracks, raw.gap_timeout_us, raw.max_release_per_tick),
            (2, 250_000, 0)
        );
        assert_eq!(
            PlaybackConfig::new()
                .gap_timeout(Duration::from_nanos(1))
                .raw()
                .unwrap_err(),
            Error::INVAL
        );
    }
}
