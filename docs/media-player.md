# Media player

`moq_media_player` (`<moq/media_player.h>`, in `libmoq-service` when the
playback engine is built) turns a namespace into decode-order media for an
application's own decoders. It composes the media receiver (catalog,
subscriptions, the object queue and its stream-terminal lane) with the
`moq_playback` engine (per-track decode order, knowledge of what exists,
gap policy) on the application's thread. Decoders, renderers, audio output
and the presentation clock stay with the application.

This release plays RAW/LOC video and the RAW/LOC audio codecs `opus` and
`mp4a.40.2`, on drafts 16, 18 and 22. CMAF, MPEG-2 TS, dependent spatial
layers and timeline tracks are refused at selection; the receiver still
serves them directly.

## Shape of a player loop

```c
moq_media_player_cfg_t cfg;
moq_media_player_cfg_init(&cfg);
cfg.receiver = &receiver_cfg;          /* or attach to a receiver you made */
moq_media_player_t *p;
moq_media_player_create(&cfg, &p);

for (;;) {
    uint64_t now = monotonic_us();
    moq_result_t rc = moq_media_player_wait(p, now, 20000);
    if (rc == MOQ_ERR_CLOSED) break;
    moq_media_player_tick(p, monotonic_us());

    moq_media_player_output_t out;
    while (moq_media_player_poll_output(p, &out, sizeof(out)) == MOQ_OK) {
        switch (out.kind) {
        case MOQ_MEDIA_PLAYER_OUTPUT_CONFIGURE:     /* (re)configure the decoder */
        case MOQ_MEDIA_PLAYER_OUTPUT_DISCONTINUITY: /* reset it */
        case MOQ_MEDIA_PLAYER_OUTPUT_TIMELINE:      /* where units present */
        case MOQ_MEDIA_PLAYER_OUTPUT_UNIT:          /* decode one access unit */
        case MOQ_MEDIA_PLAYER_OUTPUT_END:           /* the track is over */
        default: break;
        }
        moq_media_player_output_cleanup(&out);      /* every record, any kind */
    }
    moq_media_player_event_t ev;
    while (moq_media_player_poll_event(p, &ev, sizeof(ev)) == MOQ_OK) { /* log */ }
}
moq_media_player_destroy(p);
```

`now_us` is one monotonic clock in microseconds: the same clock for
`tick`, `wait`, `handle_feedback` and every deadline the player reports.
Every call on a player, `wait` and the observers included, is made on one
thread at a time.

`wait` never advances the player. It returns `MOQ_OK` when there is
something for `tick`: an output or event to poll, receiver input,
commands the engine still holds after a full output queue was drained, a
deadline that is due, or end-of-track work. When nothing is left but work
paused by your own decoder-queue feedback and no other track has a timer
running, it returns `MOQ_DONE` at once (lift that with `handle_feedback`,
not by waiting here); another track's timer still runs, so it sleeps
toward that deadline. Otherwise it sleeps until the receiver wakes it, the
next deadline, or the timeout (`MOQ_DONE`).

## Outputs

Per track, in order: a `CONFIGURE` before the units it applies to, a
`TIMELINE` before the timeline's first unit, a `DISCONTINUITY` before the
unit that needs a reset decoder, and `END` after the last unit. Units are in
decode order with their own decode and presentation times. On CMAF the
decode time is the decode clock. On RAW/LOC it is a copy of the LOC media
time, equal to the presentation time: LOC has no decode timestamp, decode
order is object order, and with B-frames the copied time can decrease from
one unit to the next, so it is not evidence of a decode clock. Every output
carries the track's `configuration_generation` (it changes on `CONFIGURE`
and `DISCONTINUITY`); decoder feedback names the generation it observed,
and feedback for an older one is ignored and counted.

A unit presents at
`anchor_local_us + (presentation_time_us - anchor_media_us) / rate` on
your clock. This release anchors each timeline once, at its first unit,
plus a fixed cushion (`cfg.cushion_us`), at rate 1.0; it does not
re-anchor from a playhead.

## Selection and timelines

`MANUAL` selects nothing until you call `select_track`. `AUTO` selects,
once the catalog is ready, the lowest-numbered render group's tracks (one
per alternate group, over all of its members, preferring `preferred_lang`,
then the middle bitrate). A catalog with no render group gets one track,
its first video. Every other track is announced with `TRACK_AVAILABLE`,
and so is a chosen track whose selection fails (counted in
`stats.selections_failed`). If AUTO cannot run for lack of memory it runs
on a later tick, and `READY` waits for it.

The tracks of one render group share one timeline only when their
timestamps share a domain: all wall-clock (LOC without a Timescale, every
draft-16/18 track) or all application media time with the shared epoch
asserted (`select_cfg.shared_epoch`, or `cfg.assume_shared_epoch` under
AUTO). Otherwise a manual selection is refused unless it asks for
`independent_timing`. AUTO reports `SYNC_UNAVAILABLE` and selects none of
that group, or, with `MOQ_MEDIA_TIMING_ALLOW_INDEPENDENT`, selects it on
separate timelines and reports `TIMING_INDEPENDENT`.

## How a track ends

`END.cause` names what ended it. When several apply before `END`, the
highest wins: `FATAL > CLOSED > LOCAL_FAILURE > REMOVED > PEER_DONE >
PEER_END_OF_TRACK`. `END.disposition` names how its input was settled:

- `COMPLETE`: the track's END_OF_TRACK came on the stream-terminal lane,
  ordered after the media, and every location up to it was played or known
  not to exist. Only with cause `PEER_END_OF_TRACK`.
- `TAIL_CUTOFF`: a bounded wait gave up on missing input. That is either
  the tail wait (`cfg.end_tail_wait_us`, default 2 s, after an error done,
  a removal, an END_OF_TRACK with input still missing, or a track its
  receiver ended for good with no END_OF_TRACK yet) or a gap the
  engine gave up on after the end began: groups skipped, a partial group
  abandoned, or units skipped inside a group to its next independent unit.
  A `GAP` announcing a wait is not loss: the missing unit may still arrive
  and play.
- `IMMEDIATE`: no wait was possible (`LOCAL_FAILURE`, `CLOSED`, `FATAL`).
  The track stops waiting for missing units at once, what the player still
  holds goes into the engine, and what can play is released in decode order
  before END. Your own decoder-queue pause still holds release.

A normal done is not a cause. When the receiver ends a track for good
without its END_OF_TRACK -- a done that ends the subscription, or a VOD
re-subscription the peer refused -- the END_OF_TRACK may still be behind on
the stream-terminal lane, so the player waits the tail wait for it: if it
arrives the end is `COMPLETE` as usual; if not, the track ends
`PEER_DONE`, `TAIL_CUTOFF`. A done is never proof of completion. That wait
does not count while your application holds that track's input up: while
its own decoder queue is paused, while the player holds input behind
output you have not polled, or while it holds an object the engine admits
only once you release what you hold up (another track's paused decoder
filling the engine's shared buffer, commands you have not polled or holds
you have not released). Polling carries no clock, so time after a poll
counts from your next tick or feedback. Another track's pause alone does
not stop the wait: only when the held object waits on it.

The tail wait after an END_OF_TRACK with input still missing does not
count while the track's own decoder queue is paused, while the engine has
output ready that the output queue you have not polled leaves no room for
(held input or not; a queue that is merely full, with nothing behind it,
does not stop the wait), or while the player holds a late object below the
END_OF_TRACK that waits for room you hold. The waits after an error done
or a removal always count.

The cause can still rise until END is queued: also once the track is
draining, and while an END waits for output room. A track that drains
first takes the input the receiver already admitted for it, even when
another track's held object stopped intake. A live delivery failure is
reported only once the input the subscriber facade admitted for the track
has reached the receiver (or been disposed of), so none of it arrives after
the track's END. A delivery failure of a fill
(`delivery_origin` FILL) ends only that fill, not its track: what it would
have delivered is missing like any other unit.

A unit the player itself drops after the end began (no decoder
configuration, the wrong time domain, ...) is loss in that end: it is not
`COMPLETE`. Loss settled before the end began does not count against it.
An object the player refuses before the engine takes it (`OVERSIZE`,
`TIME_DOMAIN`, `MALFORMED`) is recorded at its exact location as unresolved
(`GAP` cause `LOCAL_REFUSAL`), so a wait on it says why; nothing reads it as
absent.

A normal done (TRACK_ENDED, or SUBSCRIPTION_ENDED on drafts 16/18) is not
a cause: with its END_OF_TRACK the end is still `COMPLETE`, in either
order. On its own it ends nothing; the track ends with the receiver.

## Bounds

The engine's budgets (`cfg.playback`) bound everything the player holds
for you. A unit's or a configuration's bytes stay charged to
`max_buffered_bytes` while they wait in the engine's command queue and in
the player's output queue, until you poll the output: each queued output
holds its engine command (`moq_playback_poll_command_held`), so a
configuration a queued CONFIGURE carries stays charged after the track has
moved on. `stats.outputs_bytes` reports what queued outputs keep charged,
each configuration copy once. Hold identities are never reused; if a
library instance ever issues the last one, the player queues nothing
unheld: every track ends `LOCAL_FAILURE`, `IMMEDIATE`, and what it could
not hold is discarded. Outputs you have not polled therefore make
the engine refuse more input, which backs up into the receiver.

An object the engine has no room for is held by the player, up to
`cfg.max_held_bytes` (by `moq_media_object_charge`). While it is held no
later object or stream terminal is taken from the receiver, so they back
up in order and none overtakes it. Track events still are: a removal, a
done or a local failure may start the track's end, but its END follows the
held object. A larger object is taken only when the engine accepts it at
once, and is otherwise dropped `OVERSIZE`, as is one larger than the
engine itself.

`max_outputs` and `max_events` are at most `MOQ_MEDIA_PLAYER_MAX_QUEUE`
(65536); a larger value is refused. A full output queue makes `tick`
return `MOQ_ERR_WOULD_BLOCK`; poll and tick again. A full event queue drops
its oldest diagnostics, never `ENDED` or `CLOSED`, which have room kept.

## Attach

`moq_media_player_attach` takes a receiver you created: RAW time mode,
`auto_subscribe` off, the stream-terminal lane on, and no track event
polled yet. The player then consumes all of its queues, and you keep it for
observing calls only.

## Records

Output and event records are frozen layouts with fixed detail reserves: a
poll needs a record of at least the library's size and never truncates
one. Every polled output goes to `moq_media_player_output_cleanup`, whatever
its kind, also after the player is destroyed. The configuration, feedback
and stats records take a `struct_size` of at least their frozen v0 floor.

## Examples and live checks

`examples/service/media_player.c` (`moq_example_media_player`) is a complete
player loop over a relay: AUTO selection, or MANUAL with `--track NAME`; one
owner thread; every output kind handled and released; decoder-queue feedback
(`--decoder-stall`). It has no decoder. With `--finite` it succeeds only when
every track ends `PEER_END_OF_TRACK`/`COMPLETE` before its deadline, and
`--dump DIR` writes each track's units, their boundaries and timestamps, and
its decoder configurations. `scripts/check_media_player_dump.py` decodes a
dump and compares it with the published file (frames, AAC packets, timestamp
spacing, A/V offset), and `scripts/live_media_player_cases.sh` runs a table of
live cases against a relay; neither runs under CTest. The receiver example,
`media_receive.c`, remains the one to start from for objects rather than
access units, and for packagings the player refuses.

## C++

`<moq/media_player.hpp>` (header-only, C++20) is a thin owner over the C
player: no thread, no scheduling, no state of its own, and every C contract
above still holds -- one thread at a time, `now_us` on your monotonic clock.
It is opt-in and not part of `<moq/moq.hpp>`: link `moq::service` next to
`moq::cpp` (`find_package(libmoq REQUIRED COMPONENTS service)`), so a
core-only C++ consumer never takes on the service tier.

- `moq::media_player::create(receiver_cfg, cfg)` owns its receiver;
  `attach(receiver, cfg)` makes the player the receiver's sole consumer
  (the receiver must outlive it). Both return `result<media_player>`; the
  player is move-only and destroyed exactly once.
- `poll_output()` and `poll_event()` return
  `result<std::optional<...>>`: an empty optional when nothing is queued,
  an error for anything else (`errc::closed` once the player is closed).
- `polled_output` is move-only and gives its refs back exactly once, for
  every kind including ones newer than the header. `variant()` yields views
  valid while that record lives; `payload()`, `codec()` and `codec_config()`
  return retained `moq::buffer`s that outlive the record and the player.
  Unknown kinds and unnamed numeric causes and dispositions pass through.
- A unit's `decode_time_us` is a decode clock only on CMAF
  (`configure::decode_time_is_decode_clock()`); on RAW/LOC decode order is
  output order.

```cpp
auto made = moq::media_player::create(receiver_cfg);   // AUTO selection
if (!made) return made.error().code();
moq::media_player player = std::move(*made);
for (;;) {
    uint64_t now = clock_now_us();
    if (auto t = player.tick(now); !t && t.error().code() != moq::errc::would_block)
        break;                                          // closed, or a real error
    while (auto ev = player.poll_event()) {
        if (!ev->has_value()) break;
        // diagnostics: (**ev).variant() -- ready, gap, loss, ended, ...
    }
    while (auto out = player.poll_output()) {
        if (!out->has_value()) break;
        moq::polled_output rec = std::move(**out);
        std::visit([&](const auto &o) {
            using T = std::decay_t<decltype(o)>;
            if constexpr (std::is_same_v<T, moq::output::configure>) {
                decoder.configure(o.codec_string(), o.codec_config);
            } else if constexpr (std::is_same_v<T, moq::output::unit>) {
                decoder.decode(o.data, o.presentation_time_us);
                (void)player.handle_feedback(moq::player_feedback::decoder_queue(
                    rec.track(), rec.configuration_generation(),
                    decoder.depth(), decoder.max_depth()), now);
            } else if constexpr (std::is_same_v<T, moq::output::discontinuity>) {
                decoder.reset();
            } else if constexpr (std::is_same_v<T, moq::output::timeline>) {
                renderer.anchor(o);              // your clock, your renderer
            } else if constexpr (std::is_same_v<T, moq::output::end>) {
                decoder.drain();
            }
        }, rec.variant());
    }
    if (auto w = player.wait(clock_now_us(), 10000); !w) break;   // interrupted / closed
}
```

The decoder, renderer and clock are yours; the player only orders and times
what reaches them.

## Swift

`MediaPlayer` (in `MoQService`) wraps the player for Swift. It attaches to a
`MoQEndpoint`, owns its receiver (taking the endpoint's receiver slot), and
runs every C call on the endpoint's service thread, which ticks the player on
each pass and wakes for the player's next deadline. `outputs` and `events`
are async sequences of copied records (`PlayerOutput`, `PlayerEvent`) staged
in bounded queues; the C player is polled only while they have room, so a
slow consumer backs up the player and the receiver behind it. A cancelled
consumer takes nothing: what is staged stays for the next. A deadline the
player could not act on (a pass made no progress on it: its outputs are
full, or the work waits on decoder feedback) does not keep waking the
service thread; the consumer's next poll does. One a pass did progress on
is acted on at once. Decoder
feedback is `reportDecoderQueue`, `reportDecodeError` and
`reportFlushComplete`. `PlayerDecoderConfiguration.makeFormatDescription()`
and `PlayerUnit.makeSampleBuffer(formatDescription:framing:)` build the
CoreMedia records (`.annexB` framing writes NAL lengths of the width the
avcC/hvcC description declares, and refuses a unit too long for it); the player's clock is the host clock, so a
`PlayerTimeline` starts an `AVSampleBufferRenderSynchronizer` directly. The
`SimplePlayer` example plays through it, and ends a finite stream only once
what it enqueued has presented; a `FATAL` or `LOCAL_FAILURE` END is a
failed watch, not a finished one.
