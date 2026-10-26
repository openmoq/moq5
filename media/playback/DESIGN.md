# moq-playback design

`moq_playback_t` is a sans-I/O engine that turns MoQ objects into an ordered
stream of abstract decoder commands. It parses LOC or CMAF metadata, buffers
objects per track, releases them in `(group_id, object_id)` order, gates video
on independent frames, handles end-of-group and end-of-track markers, times
out missing objects, sheds live backlog, and reacts to decoder feedback.

It has no sockets, threads, clock, decoder or renderer. Time comes in as
`now_us` on the calls that advance it. The public contract is
`include/moq/playback.h`; this document explains how the pieces fit.

## 1. Scope

In scope: RAW/LOC and CMAF packaging, one video or audio stream per track,
draft-16, draft-18 and draft-22 LOC companions, decode-order release, keyframe
gating, gap and backlog policy, decoder feedback.

Not in scope: decoding, presentation scheduling, A/V synchronization, startup
buffering, rate control, rendition switching, MPEG-2 TS, encrypted CMAF. A
service-tier player composes this engine with the media receiver for those.

## 2. Interface rules

- Every caller-filled struct (`cfg`, `track_cfg`, `object`, `feedback`) starts
  with `struct_size`. Each has a frozen v0 floor (`MOQ_PLAYBACK_*_V0_SIZE`).
  The engine refuses a struct below its floor and reads an appended field only
  when `struct_size` covers the whole field, copying by byte offset so an
  older, smaller caller struct is never read past its end.
- Initializers are `_init_sized(p, size)` exports. The plain `_init(p)` names
  are macros that pass the caller's `sizeof`. Bindings that cannot expand C
  macros (Swift) call `_init_sized` with their own layout size.
- Kinds and reasons are fixed-width `uint32_t` sets whose values are never
  renumbered; a consumer ignores values it does not know. The layout of every
  public struct is therefore independent of the compiler's enum width
  (`test_playback_abi_short_enums` builds the ABI rows with `-fshort-enums`).
- Commands and events are polled with `(pb, out, out_size)`. A poll needs
  room for the record's v0 size; a smaller record is refused with
  `MOQ_ERR_ABI_MISMATCH` without dequeuing. The library writes
  `min(out_size, sizeof)` bytes and stamps `struct_size` with that count.
- Every polled command goes through `moq_playback_cmd_cleanup`, which knows
  which kinds own rcbuf refs, reads only what the stamp covers, is idempotent,
  and is legal after the engine was destroyed.

Track config appends two fields after the v0 floor: `transport_version`
(16, 18 or 22; draft 16 when uncovered; zero or any other value refused) and,
on draft 22 only, `track_properties` (the track's LOC-04 properties, copied
and validated at `add_track`).

## 3. Objects in, commands out

| Call | Buffers | Parses | Commands | Events | Timers |
|---|:-:|:-:|:-:|:-:|:-:|
| `push_object` | yes | yes | no | OBJECT_DROPPED | no |
| `tick` | releases | no | yes | yes | starts and fires gap timers |
| `poll_command` / `poll_event` | no | no | dequeue | dequeue | no |
| `handle_feedback` | no | no | RESET on decode error | KEYFRAME_WAITING | no |
| `next_deadline_us` | observes only | | | | earliest gap-timer expiry |

Ownership: `push_object` borrows payload and properties for the call and, on
success, takes one ref on the payload. Properties are parsed, not retained.
`push_parsed` takes an object the caller already parsed: it borrows the
parsed record for the call, copies its scalars, and takes one ref on the
payload. It re-checks what it relies on (packaging, payload identity,
status against payload, CMAF media range, draft-22 metadata coverage, all
before any admission effect) and otherwise shares every admission rule with
`push_object`. For CMAF the timing context is explicit: the input names the
timescale the caller parsed with, and it must equal the track's. The record's
decode time is in microseconds while exact per-sample times need the
fragment's base tick, so the engine reads the fragment index once at
admission and admits the record only if it matches the payload at that
scale: the same sample table, decode time and first-sample duration, and the
same media range, with the samples fitting inside it.

Either path keeps owned copies of what release needs: a multi-sample CMAF
object's sample table and base tick, and a draft-22 object's effective
decoder configuration (LOC-04 Video or Audio Config). The byte charge is
payload plus those copies; a configuration over `max_track_config_bytes` or a
charge over the whole budget is `MOQ_ERR_LIMIT`, a failed copy
`MOQ_ERR_NOMEM`. A refusal on either path retains nothing. Release reads only
the owned copies; it never parses.

**Configuration.** Each track keeps two configurations apart: the decoder's
(what the last CONFIGURE carried) and the one released media established
(the configuration of the last unit whose decode was emitted). An object
decodes under its own configuration, else the released one, else the
catalog one; present-but-empty is a configuration (empty), absent inherits.
A CONFIGURE carrying that configuration precedes the decode whenever it is
not the decoder's. CONFIGURE commits on its own, so a one-slot command queue
progresses, and it does not start release (it consumes no object): an
earlier entry found afterwards gets its own configuration restored first.

**Configuration charge.** The engine registers each configuration copy it
makes and counts its holders: buffered objects, a track's two slots and
queued CONFIGURE commands. The copy is charged once while any holder
remains; the charge moves between holders and ends with the last. A poll
hands the command's ref to the app, so the engine stops holding it there.
Registration is what makes a buffer a copy, so a catalog configuration is
never charged, also after its track is removed. A configuration identical
to one the track holds (either slot, a buffered object, a queued command)
is reused at no charge.

Relief counts what dropping its eligible victims returns: their payloads
and tables, plus each copy whose holders are all victims, once. With
nothing buffered, the retained bytes are configuration; copies held only by
queued commands are freed by polling, the rest by nothing. An object that
cannot fit beside the latter is `MOQ_ERR_LIMIT`; pressure from the former
is `WOULD_BLOCK`.
Commands own their refs until cleanup. Events are scalar.

## 4. Parsing per draft

Drafts 16 and 18 parse LOC-01 through the frozen media-object parser with the
track's `transport_version`, which selects the property integer codec. Draft
22 parses LOC-04 through the sized parser on the draft-22 companion, passing
the track's parsed `track_properties` so an object without its own timescale
or codec configuration inherits them.

A RAW object must carry usable media time or it is dropped
`MISSING_TIMESTAMP`. On drafts 16/18 that is the LOC capture timestamp (UNIX
microseconds). On draft 22 it is the parsed LOC-04 time: UNIX microseconds
without a timescale, application media time with one; a zero timescale or an
overflowing conversion leaves it unusable. Application media time reaches the
commands as `decode_time_us` / `presentation_time_us` with
`has_capture_time == false`. CMAF timing comes from `tfdt`/`trun`.

Parser drop reasons map to playback drop reasons by name through an explicit
table; values 1-7 coincide. A draft-22 object that repeats a known LOC
property is dropped `MALFORMED_LOC`.

## 5. Admission: the release cursor

Each track has a release cursor `(next_expected_group, next_expected_object)`:
the next location the engine will release.

**Before release starts** (no action has committed at the cursor yet):
- The cursor is set only by an *entry*: a video object with independence
  evidence (LOC frame marking or a CMAF sync sample, as the parser reports
  it), or any audio object. An object 0 marked dependent is not an entry;
  object position alone proves nothing. The first entry sets the cursor
  wherever it lies; a later entry below the cursor moves it back.
- An END_OF_TRACK marker sets the cursor of a track that has no entry, so a
  track that never produced one still ends.
- Everything else is buffered meanwhile. A video track that holds objects but
  no entry reports KEYFRAME_WAITING once from `tick`; nothing is released or
  dropped.

**Buffer pressure.** A push refused for a full buffer changes no buffered
object, ref, history entry or event; it only notes the refused object. After
releasing, the next `tick` checks that the request is still pending: the
noted handle still resolves (generation included) to a live track that has
not ended, and the object is neither buffered already nor below a committed
cursor. A settled or orphaned request is retired at no cost. A pending one
gets room from objects of tracks that have no entry yet:
the track holding the most loses its lowest object first, each dropped
KEYFRAME_WAIT with an event naming its own track, one per free event slot.
Tracks with an entry are never dropped from, and nothing is dropped unless
dropping every eligible object would make room, so an object that can never
fit costs nothing. A track waiting for a keyframe therefore cannot block
another track's admission, and pressure relief progresses even with a
one-slot event queue. The drops are local policy; they say nothing about
whether the objects exist.

This lets a dependent first arrival wait for its group's entry, and a joining
fill's early objects arrive after the live edge without stranding the group's
prefix.

**When release starts** (the first action at the cursor commits; a `tick`
refused for queue capacity before any commit starts nothing), objects still
buffered below the cursor are dropped, one STALE event each for NORMAL
objects. With the event queue full the cleanup stops with `WOULD_BLOCK`; the
committed action stands, each dropped object already had its event, and the
next tick resumes.

**After release started**, an object below the cursor can never be played:
- a duplicate of a location that left the buffer is absorbed silently; the
  engine remembers the last `max_buffered_objects` such locations;
- a status marker is absorbed silently;
- anything else is dropped with reason `STALE` and is not retained. With the
  event queue full the push returns `WOULD_BLOCK` and nothing changes.

A duplicate of a buffered location is absorbed at any time.

## 6. Release

`tick` walks each active, unpaused track from its cursor, releasing up to
`max_release_per_tick` items per call:

- **Found object.** An END_OF_TRACK marker emits TRACK_ENDED and ends the
  track. An END_OF_GROUP marker advances to the next group. A pending reset
  emits RESET(GAP). Video waiting for a keyframe drops non-keyframes
  (KEYFRAME_WAITING once, then OBJECT_DROPPED(KEYFRAME_WAIT) each) and emits
  SKIP_FORWARD when the keyframe arrives after an abandonment. On a CMAF
  track, whose `tfdt`/`trun` give a decode clock, a DTS below the last
  emitted DTS is dropped `NON_MONOTONIC_DTS`. A RAW (LOC) object carries only
  its LOC media time (the capture timestamp on LOC-01), no decode timestamp,
  and its decode order is object order (LOC-01/LOC-04 §4.2: GroupID then
  ObjectID; the timestamp order of §4.5 excludes frame reordering), so a
  decreasing time is never a drop: its DECODE copies that time into both
  decode and presentation time, and the copy is not a decode clock.
  Otherwise the engine emits
  CONFIGURE (once per configuration) and DECODE. An object that closes its
  group (status marker, or the header end-of-group bit unless
  `ignore_eog_bit`) advances the cursor to the next group and drops any later
  objects of the closed group.
- **Missing object.** If the id is known absent, the cursor moves past the
  absence at once, with no event and no reset: nothing was missed. Absence
  to the end of the track ends it (TRACK_ENDED). If the group is known
  closed by a buffered marker, the engine emits PARTIAL_GROUP_ABANDONED and
  moves to the next group in recovery. With
  `infer_end_of_group_from_next_group` and no later object of the current
  group buffered, it moves on when the next group is buffered. If more than
  `max_backlog_groups` groups are buffered ahead, it sheds the oldest
  (BACKLOG_SHED). Otherwise it waits, `gap_timeout_us` from the first
  evidence that delivery moved past the missing id: the arrival of a later
  object of the track, or a stream fact reaching the id. Without evidence
  there is nothing to time out. When the wait ends it emits
  GAP_DETECTED with the id and its cause (unknown, or the reset or receiver
  drop that left it unresolved), then follows the track's gap policy:
  - DISCONTINUITY (default): if the next buffered unit of the same group is
    independent, it follows behind a RESET; otherwise the group is abandoned
    (PARTIAL_GROUP_ABANDONED) and the next group follows in recovery. Units
    after a hole may depend on what is missing, so only an independent unit
    follows one.
  - CONTINUE: the next buffered unit follows with the decoder state kept;
    GAP_CONTINUED reports the hole, from the end of the last released unit
    (its decode time plus its duration, when the packaging carries one) to
    the unit resumed at.

Recovery means: a RESET before the next decode, video waits for a keyframe,
and SKIP_FORWARD reports the jump. A CONTINUE track recovers without the
reset or the keyframe wait.

**Knowledge.** `moq_playback_on_stream_terminal` feeds the receiver's
stream-terminal lane. Per track the engine keeps absence (above an
END_OF_GROUP stream's last object, and from END_OF_TRACK on), unresolved
delivery (after a reset's last complete object or its cut object, and the
groups of a receiver drop) and its cursor apart, in at most
`max_known_ranges` sorted ranges. Touching ranges of one kind merge; ranges
behind the cursor are dropped. With the bound reached, the two closest
ranges ahead of the cursor coalesce into one unresolved range: a local
resource decision, so when that demotes absence one KNOWLEDGE_FORGOTTEN
reports the coalesced range, and a fact that needs that event and finds no
room is refused WOULD_BLOCK without effect. A plain FIN says nothing about
existence; it is evidence for the wait only.

**Evidence.** Each fact keeps its arrival time and its reach (the end of
its group; a receiver drop's last group), in a list bounded by
`max_known_ranges` that keeps only facts no other dominates. A wait counts
from the earliest fact reaching the missing id, so a later fact that
reaches further never lends its reach to an earlier time. A fact that must
give way passes its ids to its successor's later time: a wait may
lengthen, never start early.

**One event slot is enough.** A wait that ends commits GAP_DETECTED first,
then its abandon or GAP_CONTINUED when that event has room, without a
second report; coalescing emits at most one event.

CMAF objects with several samples emit one DECODE per sample from the owned
sample table taken at admission, after a preflight
at sample 0 that validates every sample's timing and media range. Samples
leave as command slots allow: the object stays at the cursor with a sample
cursor until its last sample is out, so an object larger than the command
ring drains across ticks. Each sample's decode time is converted from its own
tick position. A decode error while an object is part-way out restarts it
from its first sample behind the reset.

## 7. Backpressure

`push_object` and `push_parsed` return `WOULD_BLOCK` with nothing committed
when the buffer is full (`max_buffered_objects` or `max_buffered_bytes`) or a
drop event has no room, and `MOQ_ERR_LIMIT` for a payload larger than the
whole byte budget: it can never be admitted, so it is neither retried nor
made room for, and the cursor does not move (the location is simply missing,
like any other). `tick` returns `WOULD_BLOCK` when a command or event it must emit has no
room; each action is committed whole, and actions committed earlier in the
same call stand. Events are never dropped silently. The caller drains the
queues, ticks and retries.

`QUEUE_PRESSURE` feedback pauses release for its track while the reported
depth exceeds `max_recommended`; objects are still admitted. A paused track
has no deadline. `DECODE_ERROR` feedback purges the track's queued commands,
emits RESET(DECODE_ERROR) and, for video, KEYFRAME_WAITING.

## 8. Current behaviour worth knowing

These are properties of the engine as it stands, not of the protocol:

- Absence is known only from the stream facts fed to it and from in-band
  END_OF_GROUP and END_OF_TRACK markers. The Prior Object ID Gap property
  is not surfaced by the media-object parser, so it establishes nothing yet.
- Independence comes from per-unit evidence only (LOC marking, the CMAF
  sync flag). No layout is declared, so object 0 of a group proves nothing,
  and the temporal-layer rule (an unreceived id's layer from a declared
  object-id-to-layer rule) is not available.
- A late unit is still released: lateness needs the presentation clock,
  which the engine does not have yet, so DROP(LATE_DISCARDABLE) is not
  implemented.
- An audio track resets on a gap unless its gap policy is CONTINUE, which
  the caller supplies.
- A multi-chunk CMAF object is refused (`UNSUPPORTED_MULTI_CHUNK`):
  `moq_cmaf_parse_fragment` describes only the first chunk, and playing it
  alone would drop the rest.
- `target_latency_us` is stored and not used.
- Buffer lookups scan the object pool linearly.
- `push_sub_object` lives in this library. The subscriber facade it reads is
  part of `moq-core`, which the library links anyway, so it adds no
  dependency.

## 9. Tests

| Suite | Library | Covers |
|---|---|---|
| `playback_api` | test internals | the full behaviour matrix, white-box state |
| `playback_input` | test internals | admission, dedupe, entry anchoring, buffer pressure, draft 16/18/22 parsing, `next_deadline_us` |
| `playback_parsed` | test internals | `push_parsed` ownership and validation, `MOQ_ERR_LIMIT`, multi-sample release through a small ring, multi-chunk refusal |
| `playback_facts` | test internals | stream facts, absence versus unresolved, the wait from evidence, the dependency rule, gap policies, bounded knowledge and its exhaustion |
| `playback_abi` / `playback_abi_short_enums` | production | layout pins, sized polls and initializers, cleanup contract, prefix-safe readers |
| `playback_oom` | production | allocation failure at every point |
| `playback_scenario` | test internals | seeded randomized delivery against invariants |
