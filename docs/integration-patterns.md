# LibMoQ Integration Patterns

Recommended patterns for media applications using `moq_pq_threaded_t`
with the publisher/subscriber facades, media-object normalizer, and
playback pipeline.

## Threading Model

```
App / player thread              Network thread (picoquic)
───────────────────              ─────────────────────────
                                 on_lane_pump():
enqueue publish request            dequeue + write objects
moq_pq_threaded_wake() ────────→   moq_pub_write_object_ex
                                   moq_pub_tick
                                   moq_sub_tick
                                   moq_sub_poll_object
moq_pq_threaded_wait() ◄────────  push to app queue
dequeue + decode / render          mark_activity
```

**Rule: the network thread is the only place to call session,
adapter, or facade APIs.** The app thread communicates via
application-level queues.

### Callback rules

| Callback | Thread | May call libmoq APIs? |
|----------|--------|-----------------------|
| `on_lane_pump` | Network | Yes — the intended call site |
| `on_activity` | Network | No — signal only |

`on_lane_pump` runs between `moq_pq_service` calls. Create facades,
tick them, publish/poll objects here. Return 0 to continue,
nonzero to request clean shutdown.

Neither callback may call `moq_pq_threaded_stop`.

### Lifecycle

```c
moq_pq_threaded_create(&cfg, &t);   // start network thread

while (!done) {
    moq_pq_threaded_wake(t);        // ask on_lane_pump to run
    moq_pq_threaded_wait(t, timeout_us);
    // process results from app queue
}

moq_pq_threaded_stop(t);            // join thread
// destroy facade (pub/sub)
moq_pq_threaded_destroy(t);         // free resources
```

## Receiver (Subscriber) Pattern

The receiver subscribes to tracks, receives MoQ objects, parses
them into media samples, and pushes samples to the app thread.

### on_lane_pump implementation

```c
int receiver_pump(moq_pq_threaded_t *t, moq_pq_threaded_lane_t *lane,
              uint64_t now, void *ctx) {
    receiver_t *rx = ctx;
    moq_session_t *sess = moq_pq_threaded_session(t);
    if (!sess) return 0;

    /* Create subscriber after session is established. */
    if (!rx->sub &&
        moq_session_state(sess) == MOQ_SESS_ESTABLISHED) {
        moq_sub_cfg_t sc; moq_sub_cfg_init(&sc);
        sc.callbacks.ctx = rx;
        sc.callbacks.on_subscribed = on_sub;
        sc.callbacks.on_subscribe_error = on_err;
        if (moq_sub_create(sess, rx->alloc, &sc, &rx->sub) != MOQ_OK)
            return 1;

        /* Subscribe to catalog track. */
        moq_sub_track_cfg_t tc; moq_sub_track_cfg_init(&tc);
        tc.track_namespace = rx->ns;
        tc.track_name = MOQ_BYTES_LITERAL(MOQ_MSF_CATALOG_TRACK_NAME);
        if (moq_sub_subscribe(rx->sub, &tc, now,
                &rx->catalog_track) != MOQ_OK)
            return 1;
    }

    if (!rx->sub) return 0;
    moq_sub_tick(rx->sub, now);

    /* Poll received objects. */
    moq_sub_object_t obj;
    while (moq_sub_poll_object(rx->sub, &obj) == MOQ_OK) {
        if (obj.track == rx->catalog_track) {
            handle_catalog(rx, &obj, now);
        } else {
            /* Convert to normalizer input. */
            moq_media_object_input_t in;
            moq_media_object_input_from_sub_object(&obj, &in);

            /* Parse into media samples. */
            moq_cmaf_sample_t samples[30];
            moq_media_parsed_object_t parsed;
            moq_media_drop_reason_t reason = 0;
            moq_result_t prc = moq_media_object_parse(
                &rx->track_info, &in,
                samples, 30, &parsed, &reason);
            if (prc == MOQ_OK &&
                parsed.status == MOQ_OBJECT_NORMAL &&
                obj.payload != NULL) {
                /* Build an app-owned packet for the queue.
                 * parsed byte spans borrow from obj.payload —
                 * retain the rcbuf so it outlives cleanup. */
                app_packet_t pkt;
                pkt.decode_time_us = parsed.decode_time_us;
                pkt.keyframe       = parsed.keyframe;
                /* CMAF: use mdat_offset/mdat_len into the fragment.
                 * RAW:  offset=0, len=moq_rcbuf_len(obj.payload). */
                pkt.mdat_offset    = parsed.mdat_offset;
                pkt.mdat_len       = parsed.mdat_len;
                pkt.payload_ref    = obj.payload;
                moq_rcbuf_incref(obj.payload);
                app_queue_push(&rx->queue, &pkt);
                /* App thread owns this ref and must decref
                 * after decode/render. */
            }
        }
        moq_sub_object_cleanup(&obj);
    }
    return 0;
}
```

### App thread

```c
while (!done) {
    moq_result_t r = moq_pq_threaded_wait(t, 100000);
    if (r == MOQ_ERR_CLOSED) break;

    app_packet_t pkt;
    while (app_queue_pop(&rx.queue, &pkt)) {
        /* Decode / render on app thread using
         * moq_rcbuf_data(pkt.payload_ref) + pkt.mdat_offset. */
        process_packet(&pkt);
        moq_rcbuf_decref(pkt.payload_ref);
    }
}
```

### Media-object normalizer

`moq_media_object_parse` is a stateless, sans-I/O function that
extracts timestamps, keyframe flags, and sample data from raw MoQ
objects based on LOC properties and CMAF packaging. It runs on
the network thread inside `on_lane_pump`.

The track info must carry the transport draft the SESSION negotiated:
LOC object properties are Key-Value-Pairs, and draft-16 and draft-18
encode a KVP's integers differently (draft-16 §1.4 `(i)`, draft-18
§1.4.1 `(vi64)`). The two agree only below 64, so a wrong draft is
invisible on small values and wrong on everything else. The draft also
selects the LOC property registry: drafts 16 and 18 carry
draft-ietf-moq-loc-01 ids, draft-22 carries draft-ietf-moq-loc-04 ids
(`<moq/loc.h>`). A catalog
describes the track, never the session carrying it, so the version
comes from `moq_session_version(session)` — never from endpoint
configuration or an offered preference.

```c
/* Set up track info from an MSF catalog entry (recommended). */
moq_media_track_info_t track;
moq_cmaf_init_info_t   cmaf_init;
moq_rcbuf_t           *init_buf = NULL;
moq_result_t rc = moq_msf_track_to_media_info_sized(
    alloc, msf_track, &track, sizeof(track),
    moq_session_version(session),        /* the live session is the authority */
    &cmaf_init, sizeof(cmaf_init), &init_buf);
/* track.media_type, .packaging, .timescale, .transport_version are filled.
 * cmaf_init is the parsed CMAF header (sample entry, codec kind, CENC, ...);
 * its spans borrow from init_buf (decref when done). For a LOC track with
 * initData, cmaf_init.codec_config is that extradata and every other field
 * stays zero; without initData init_buf is NULL and cmaf_init is zeroed.
 *
 * An unobserved (0) or unsupported version is MOQ_ERR_INVAL here rather
 * than a silent draft-16 result -- fail closed, do not guess a codec. */

/* Or set up manually from app config: */
moq_media_track_info_init_sized(&track, sizeof(track));
track.media_type        = MOQ_MEDIA_TYPE_VIDEO;
track.packaging         = MOQ_MEDIA_PACKAGING_CMAF;
track.timescale         = 90000;
track.transport_version = moq_session_version(session);

/* Per-object parse. */
moq_media_object_input_t in;
moq_media_object_input_from_sub_object(&sub_object, &in);

moq_cmaf_sample_t samples[30];
moq_media_parsed_object_t out;
moq_media_drop_reason_t reason = 0;
moq_result_t rc = moq_media_object_parse(
    &track, &in, samples, 30, &out, &reason);
```

The output (`moq_media_parsed_object_t`) contains:
- Timestamps: decode_time_us, presentation_time_us, capture_time_us
- Keyframe flag (from CMAF sample flags or LOC video frame marking)
- Payload / fragment bytes (zero-copy, borrowed from input)
- CMAF sample table (if multi-sample)

Codec configuration comes from MSF catalog `initData` or the
playback pipeline's CONFIGURE commands, not from the normalizer.

The sized mapping above, the sized CMAF parser and the receiver's
`desc.cmaf` pointer replace the unsized exports and the embedded init record
of earlier builds. That change breaks both source and ABI: rebuild the
library, its bindings and the application together (see "Track metadata: a
deliberate source and ABI break" in the top-level README).

### MPEG-2 TS packaging (`mpeg2ts`)

`moq::mpeg2ts` (`-DMOQ_BUILD_MPEG2TS=ON`, default OFF; depends only on
`moq-core`) carries MPEG-2 Transport Stream source packets as the MSF
`"mpeg2ts"` packaging of draft-gregoire-moq-msfts (MSFTS format version 3,
editor's copy of 2026-10-09). It is a demultiplexer-lite, not a decoder:
stateless parsers plus caller-owned state structs, no heap, every field
bounds-checked, only constant shared data (independent states may run on
different threads). No state struct points into itself, so a state can be
moved by copying its bytes; state that refers to caller storage (reorder
slots, Object and lead-in buffers, section storage) still shares it, so only
one copy may go on being used.

Catalog side (`<moq/msf.h>`, always built with `moq-msf`): the `mpeg2ts*`
track keys form one record, `moq_msf_mpeg2ts_t` (sized, one `has_` flag per
key), carried as an entry of kind `MOQ_MSF_EXT_MPEG2TS` in the track's list
of packaging records (`moq_msf_track_ext_t`, see `msf.h`). Read it with
`moq_msf_track_mpeg2ts(track)` (NULL when absent); to author, point an entry
at a record set up with `moq_msf_mpeg2ts_init`. Parsing is lenient -- a bad mpeg2ts track never
fails the catalog, and any JSON spelling of an integral value is accepted --
and the subscriber's verdict comes from `moq_msf_track_mpeg2ts_check()`.
Authoring is strict, for independent catalogs, delta `add`/`clone` tracks and
the referenced initialization data alike.
`moq_msf_track_mpeg2ts_init_data()` resolves `initRef` to the `initDataList`
entry and checks it is `"inline"` whole source packets.

```c
moq_msf_mpeg2ts_error_t why;
if (moq_msf_track_mpeg2ts_check(track, &why) != MOQ_OK)
    return reject_track(why);          /* e.g. an unrecognized mpeg2tsMode */

moq_mpeg2ts_reorder_slot_t slots[16];
moq_mpeg2ts_receiver_cfg_t rc;
moq_mpeg2ts_receiver_cfg_init(&rc);   /* random access on the video PID */
const moq_msf_mpeg2ts_t *m = moq_msf_track_mpeg2ts(track);  /* checked */
rc.packet_size = m->packet_size;         /* 188 or 192 */
rc.es_packets  = m->mode == MOQ_MSF_MPEG2TS_MODE_ES_PACKETS;
rc.es_pid      = (uint16_t)m->es_pid;
rc.reorder_slots = slots;
rc.reorder_slot_count = 16;
rc.reorder_window_us = 300000;
moq_mpeg2ts_receiver_t rx;
moq_mpeg2ts_receiver_init(&rx, sizeof(rx), &rc);

/* Per received Object, as it arrives (the payload stays alive until it is
 * handed back below). obj.end_of_group is the subgroup header bit. */
moq_mpeg2ts_receiver_offer(&rx, now_us, obj.group_id, obj.object_id,
    obj.end_of_group ? MOQ_MPEG2TS_OBJ_SUBGROUP_ENDS_GROUP : 0, payload, obj_ref);
/* After each moq_sub_poll_object(), with moq_sub_cfg_t.surface_group_ends
 * set (or on MOQ_EVENT_SUBGROUP_FINISHED with end_of_group, when the session
 * events are visible): */
moq_sub_group_end_t ge;
while (moq_sub_poll_group_end(sub, &ge) == MOQ_OK)
    moq_mpeg2ts_receiver_end_group(&rx, ge.group_id);
/* An END_OF_GROUP status Object at (g, n): Objects n and above do not exist. */
moq_mpeg2ts_receiver_end_group_at(&rx, obj.group_id, obj.object_id);

moq_mpeg2ts_delivery_t d;
while (moq_mpeg2ts_receiver_poll(&rx, now_us, &d, sizeof(d)) == MOQ_OK) {
    if (d.result.gap && !d.result.gap_tail_unknown)
        report_gap(d.result.gap_group, d.result.gap_object,
                   d.result.gap_duration_27mhz);
    if (d.result.accepted) forward_unchanged(d.payload);  /* never rewrite CCs */
    release(d.ctx);
}
/* Wake again by moq_mpeg2ts_receiver_next_deadline_us(&rx). */
```

Group ends and ordering. MoQT drafts 16 and 18 put the END_OF_GROUP bit in
the subgroup header, so it rides on every Object of the subgroup, and the
Group's final Object is only known from the stream's FIN (or an END_OF_GROUP
status Object). Objects of adjacent Groups travel on different streams and
can arrive out of order. The receiver therefore takes per-Object flags rather
than a "last Object" boolean, keeps a bounded reorder window (caller slots,
a time window, release when full, `finish()` at the end of the track), and
moves to the next Group without a gap only when the previous one's end is
confirmed (`end_group()` on the subgroup's FIN, `end_group_at()` on an
END_OF_GROUP status Object, or `MOQ_MPEG2TS_OBJ_LAST_IN_GROUP`). A status
Object's ID is the first ID that does not exist, so its final Object is the
one before it, and a missing one is a known gap, not a confirmed end; a
status at Object 0 marks the Group empty. The END_OF_GROUP bit alone does not confirm
it -- the bit names the subgroup holding the largest Object, and only its FIN
says which Object that is (a reset stream says nothing) -- so otherwise the
move is reported as `gap_tail_unknown`, distinct from a known loss. The core
subscriber facade surfaces those FINs when `moq_sub_cfg_t.surface_group_ends`
is set: `moq_sub_poll_group_end()` returns each one only after every object
queued before it, so polling it after each object keeps the session's order.
A subscriber that cannot see FINs pays up to the window at each Group
boundary; `moq_mpeg2ts_receiver_push()` is the immediate, in-order form.

Subscriber helpers: `moq_mpeg2ts_strip_prefix` (192 -> 188 for a TS
receiver), `moq_mpeg2ts_align_join_psi` (catalog PAT/PMT copies with
join-safe continuity counters, duplicates kept duplicates),
`moq_mpeg2ts_switcher_*` (discontinuity_indicator on the first PCR and, where
an adaptation field can carry it, on each other PID's first packet; PAT/PMT
re-versioning across alternate-track switches, whole tables included; PMT PIDs
can be named at the switch), `moq_mpeg2ts_pmt_subset` (ES-level output
carrying a subset of streams), and `moq_mpeg2ts_pcr_interpolate` /
`moq_mpeg2ts_stuffing_packets` for an egress scheduler. Publisher helpers:
`moq_mpeg2ts_packetizer_*` (whole packets only, Groups opened at random access
points of the video PID with the PAT/PMT run that precedes them in the same
first Object, PCR-measured Group duration and peak rate for `mpeg2tsMuxRate`;
`moq_mpeg2ts_packetizer_end_object` makes a write boundary an Object boundary
and leaves a held PAT/PMT run for the next Group)
and `moq_mpeg2ts_program_filter_*` (per-program mode: one program's PIDs, PAT
and CAT tracked as whole tables, PAT rewritten with its own version_number and
CRC_32, ECM and EMM PIDs kept).

What stays with the application: subscribing to the PAT/PMT and PCR tracks of
an ES-level program and emitting the tables before the elementary streams at
the required interval; scheduling egress on the PCR; publishing a new
catalog or track when the PSI changes; aligning alternate tracks' Group
boundaries; keeping an unmodified-mode pipeline byte-exact (configure the
packetizer with `wait_for_random_access = false`); and CAT or SI table
rewriting, which these helpers do not do.

The normalizer accepts `MOQ_MEDIA_PACKAGING_MPEG2TS`, with the packet size
(188 or 192) in `moq_media_object_parse_cfg_t.mpeg2ts_packet_size`, passed to
`moq_media_object_parse_sized`, when the build
has `moq::mpeg2ts` (otherwise `MOQ_ERR_UNSUPPORTED`): it rejects
partial packets and missing sync bytes (`MOQ_MEDIA_DROP_MALFORMED_MPEG2TS`),
and for a video track takes the keyframe flag and timing from the program's
video stream. `moq_msf_track_to_media_info_sized` maps an `"mpeg2ts"` track
that passes `moq_msf_track_mpeg2ts_check()` (packaging, packet size, the
catalog PSI as the init buffer, video unless the role says audio; PSI/SI
table roles are not media).

Service tier or helpers? `moq_media_sender` / `moq_media_receiver` carry the
packaging like LOC and CMAF: a TS track is added with packaging `MPEG2TS` and
its record as a `moq_msf_track_ext_t` entry of the track cfg, fed with
`moq_media_sender_write_mpeg2ts()` (TS bytes in any chunking; the service
runs the program filter a per-program record implies and the packetizer) or,
with `mpeg2ts_app_objects`, with Objects the app forms through
`moq_media_sender_write()`. The receiver auto-subscribes TS tracks in a build
with `moq::mpeg2ts`, runs the reorder window per track, and delivers Objects
in Location order through `poll_object` with the loss report
(`has_loss_report`, `gap*`, `discontinuous`); `mpeg2ts_strip_prefix` and
`mpeg2ts_join_psi` apply the 192-to-188 and join-PSI steps. Choose the service
for "TS bytes in, TS bytes out"; choose the helpers for what it leaves out:
es-packets authoring, CAT/SI rewriting, egress pacing on the PCR, a custom
Group policy, rendition switching, and relays.

The `es-units` / `media-frames` modes of draft -01, which the editor's copy
removed, are rejected as unrecognized modes.

### Playback pipeline

For applications that need keyframe gating, gap timeout, backlog
shedding, and DTS monotonicity enforcement, the `moq_playback_t`
pipeline wraps the normalizer with stateful track management:

```c
/* Create pipeline in on_lane_pump after subscribing. */
moq_playback_cfg_t pcfg; moq_playback_cfg_init(&pcfg);
pcfg.max_samples_per_object = 30;
if (moq_playback_create(rx->alloc, &pcfg, &rx->playback) != MOQ_OK)
    return 1;

moq_playback_track_cfg_t ptc; moq_playback_track_cfg_init(&ptc);
ptc.media_type = MOQ_PLAYBACK_MEDIA_VIDEO;
ptc.packaging  = MOQ_PLAYBACK_PACKAGING_CMAF;
ptc.timescale  = 90000;
ptc.transport_version = (uint32_t)moq_session_version(session);
if (moq_playback_add_track(rx->playback, &ptc, &rx->pb_track) != MOQ_OK)
    return 1;

/* One pump step. rx->video_track is the media track's subscription, made
 * once the catalog names the track; rx->pb_track is its playback track.
 * The queues are bounded and every refusal is
 * MOQ_ERR_WOULD_BLOCK with nothing taken, so the step drains first and
 * retries; a held subscriber object stays owned by the caller until the
 * push accepts it. */
static int pump_playback(rx_t *rx, uint64_t now)
{
    /* Drain both queues. Commands own rcbuf refs (e.g. a DECODE payload);
     * the queue consumer calls moq_playback_cmd_cleanup(&cmd) after use. */
    moq_playback_cmd_t cmd;
    while (moq_playback_poll_command(rx->playback, &cmd, sizeof(cmd)) == MOQ_OK)
        app_queue_push(&rx->cmd_queue, &cmd);
    moq_playback_event_t ev;
    while (moq_playback_poll_event(rx->playback, &ev, sizeof(ev)) == MOQ_OK)
        app_log_playback_event(&ev);   /* drops, gaps, track end */

    /* Push the held object (if any), then new ones. Only objects of the
     * media track reach playback: the catalog and any other track are
     * routed first, and only a media object is ever held for retry. */
    while (rx->have_held || moq_sub_poll_object(rx->sub, &rx->held) == MOQ_OK) {
        if (!rx->have_held) {
            if (rx->held.track == rx->catalog_track)
                handle_catalog(rx, &rx->held, now);
            if (rx->held.track != rx->video_track) {
                moq_sub_object_cleanup(&rx->held);
                continue;
            }
            rx->have_held = true;
        }
        moq_result_t rc = moq_playback_push_sub_object(rx->playback,
                                                       rx->pb_track,
                                                       &rx->held, now);
        if (rc == MOQ_ERR_WOULD_BLOCK)
            break;                      /* keep it; retry next step */
        moq_sub_object_cleanup(&rx->held);
        rx->have_held = false;
        if (rc != MOQ_OK)
            return -1;                  /* caller error or ended track */
    }

    /* tick may have released some objects before it returns WOULD_BLOCK;
     * that progress stands. It also makes room for a refused push when
     * tracks still waiting for a keyframe fill the buffer. Drain on the
     * next step and tick again. */
    moq_result_t trc = moq_playback_tick(rx->playback, now);
    if (trc != MOQ_OK && trc != MOQ_ERR_WOULD_BLOCK)
        return -1;
    return 0;
}
```

## Publisher Pattern

The publisher receives media from the app thread via a queue,
encodes it as MoQ objects, and writes them on the network thread.
This pattern makes `moq_pub_tick` the session's only event consumer. It works
on every draft: service transport actions, call tick once, then return to the
driver after a bounded amount of application work. A tick that returns
`MOQ_ERR_WOULD_BLOCK` still permits event progress on the next round; do not
spin until it returns OK. For a shared event dispatcher, use the
[manual mode](#manual-publisher-pump) below instead of tick.

The application-defined `publisher_t` includes a `write_request_t pending_write`
and a `bool has_pending_write`, initially false. These fields belong to the
network thread. A blocked write stays there, ahead of later queued objects;
it does not rely on finding room to push a popped request back into a
concurrently filled queue.

### on_lane_pump implementation

```c
int publisher_pump(moq_pq_threaded_t *t, moq_pq_threaded_lane_t *lane,
              uint64_t now, void *ctx) {
    publisher_t *px = ctx;
    moq_session_t *sess = moq_pq_threaded_session(t);
    if (!sess) return 0;

    if (!px->pub) {
        moq_pub_cfg_t pc; moq_pub_cfg_init(&pc);
        pc.accept_mode = MOQ_PUB_ACCEPT_ALL;
        if (moq_pub_create(sess, px->alloc, &pc, &px->pub) != MOQ_OK)
            return 1;

        /* Add catalog track + retain the current catalog group for Joining FETCH. */
        if (moq_pub_add_track(px->pub, &catalog_cfg, now,
                &px->cat_track) != MOQ_OK)
            return 1;
        moq_rcbuf_t *catalog_json = build_msf_catalog(px);
        if (!catalog_json) return 1;
        moq_pub_retained_object_t ro = {
            .object_id = 0,
            .payload = catalog_json,
            .properties = NULL,
            .end_of_group = true,
        };
        moq_pub_retained_group_cfg_t rg;
        moq_pub_retained_group_cfg_init(&rg);
        rg.group_id = 0;
        rg.objects = &ro;
        rg.object_count = 1;
        if (moq_pub_set_retained_group(px->pub, px->cat_track,
                &rg) != MOQ_OK) {
            moq_rcbuf_decref(catalog_json);
            return 1;
        }
        moq_rcbuf_decref(catalog_json);
        /* set_retained_group increfs; app releases its ref above.
         * Current subscribers need a normal write_object_ex; retained groups
         * answer explicit Joining FETCH requests. */

        /* Add media track. */
        if (moq_pub_add_track(px->pub, &media_cfg, now,
                &px->media_track) != MOQ_OK)
            return 1;
    }

    { moq_result_t trc = moq_pub_tick(px->pub, now);
      if (trc < 0 && trc != MOQ_ERR_WOULD_BLOCK) return 1; }

    /* Application-chosen budget, not a library limit. Keep one owned
     * request until its write completes; a partial fan-out owns additional
     * facade refs and must be retried with identical identity and bytes. */
    for (size_t writes = 0; writes < 32; ++writes) {
        if (!px->has_pending_write) {
            if (!app_queue_pop(&px->write_queue, &px->pending_write)) break;
            px->has_pending_write = true;
        }
        const write_request_t *req = &px->pending_write;
        moq_pub_object_cfg_t obj;
        moq_pub_object_cfg_init_sized(&obj, sizeof(obj));
        obj.group_id   = req->group_id;
        obj.object_id  = req->object_id;
        obj.payload    = req->payload;
        obj.properties = req->properties;
        obj.end_of_group = req->end_of_group;
        moq_result_t wrc = moq_pub_write_object_ex(
            px->pub, px->media_track, &obj, now);
        if (wrc == MOQ_ERR_WOULD_BLOCK) {
            /* Retain this request for the next driver round. A terminal
             * reset may need event credit, not just action capacity. */
            break;
        }
        moq_rcbuf_decref(req->payload);
        moq_rcbuf_decref(req->properties);
        px->has_pending_write = false;
        if (wrc != MOQ_OK) return 1; /* stop; teardown releases facade refs */
    }
    return 0;
}
```

### App thread

```c
/* Encode a frame and queue it for the network thread. */
moq_rcbuf_t *payload = encode_frame(frame_data, frame_len);
moq_rcbuf_t *props = encode_loc_properties(timestamp, is_key);
write_request_t req = {
    .group_id = group++, .object_id = 0,
    .payload = payload, .properties = props,
    .end_of_group = true,
};
app_queue_push(&px.write_queue, &req);
moq_pq_threaded_wake(t);
```

The app thread transfers its owned rcbuf refs to the queue.
Once popped, the network thread owns them through `pending_write` and
decrefs after `write_object_ex` succeeds. On `WOULD_BLOCK`, keep the request's
identity, bytes and references unchanged. Flush/tick resumes staged request
work, but the application must retry the write itself. Other errors stop
this sample's pump; lifecycle cleanup must destroy the publisher and release
any still-pending or queued application refs. Arrange another driver round
while work remains after the 32-write budget, giving transport service a turn
first; wait for input, credit or an application decision when no progress is
possible.

An application that chooses to drop a partial write must first drive
`moq_pub_reset_group` to completion, retrying that same reset after
`MOQ_ERR_WOULD_BLOCK` while continuing normal event/action progress. Keep the
pending request until that abandonment succeeds; then release its application
refs and clear it. A clean group close or merely dropping local refs does not
abandon the facade's operation. The reset affects every active destination of
that track, and later Objects must obey the track's existing group-history
rules. Destroying the publisher is the alternative when ending its entire
lifetime.

### Manual publisher pump

When several owners share the session's events, use one application
dispatcher and `moq_pub_handle_event`; do not also call `moq_pub_tick`.
On every draft, one bounded progress round follows this sequence:

1. Service available transport actions. If action draining blocks, continue
   to the event steps below; a queued reset may need event credit.
2. Call `moq_pub_flush` once to retry work the publisher already owns.
   `MOQ_ERR_WOULD_BLOCK` means work remains, not that event polling must wait.
   Handle other errors according to the public contract.
3. Poll and dispatch one event at a time, up to an application-chosen event
   budget. Keep normal FIFO order. On `WOULD_BLOCK + CONSUMED`, the publisher
   has taken ownership of the necessary state: clean up the event exactly
   once and continue. Never save or replay that event after flush.
4. Offer `IGNORED` events to other owners while their borrowed spans are
   valid, before another advancing call on the session, then clean up once.
   Route fill request/terminal and outgoing-subgroup terminal events too;
   these can release credit or reconcile subgroup state needed by other work.
5. After dispatch returns credit, retry staged work in a bounded pass and
   service available actions. If no event, action or application progress
   occurred, yield to the driver rather than spinning for flush-to-OK.

Clean up every polled event exactly once, including on an error exit.
`CONSUMED` does not promise that an ACK or response reached the peer. An
initial decision may remain owned after destination allocation fails with
`NOMEM + CONSUMED`; recover resources and use flush to retry it, rather
than replaying the event. An intentionally deferred authorizer or fill can
keep returning WOULD_BLOCK until the application supplies its decision.
For deferred subscription resolution, the decision's ID becomes stale as
soon as it is recorded, even if resolution returns WOULD_BLOCK or NOMEM; see
[deferred authorization](architecture.md#deferred-subscribe-authorization).

## Session Tuning for Media

The default session capacities are conservative. Media workloads
should increase them:

```c
moq_pq_threaded_cfg_t cfg;
moq_pq_threaded_cfg_init(&cfg);

/* Larger action queue for burst writes. */
cfg.max_actions = 256;

/* More events for high-frequency object delivery. */
cfg.max_events = 64;

/* More data streams for concurrent subgroups. */
cfg.max_data_streams = 128;

/* Request capacity for subscription management. */
cfg.send_request_capacity = true;
cfg.initial_request_capacity = 64;
```

Exact values depend on the media profile:
- **Live video (30 fps, one object per group)**: ~30 objects/sec.
  Default capacities are usually sufficient.
- **Live video (60 fps, multiple objects per group)**: increase
  `max_actions` and `max_data_streams`.
- **Multi-track (video + audio + catalog)**: increase
  `max_subscriptions` and `max_events`.
- **High bitrate (4K, HDR)**: increase `send_buffer_size` and
  `recv_buffer_size` if backpressure occurs.

## What This Stack Does Not Do

- **Demuxing/muxing container formats.** The app provides raw
  CMAF fragments or opaque payloads. LibMoQ parses CMAF structure
  for timestamps and keyframes but does not decode media codecs.
- **Codec decode/encode.** Use your platform's codec APIs.
- **Rendering/display.** The app owns the render loop.
- **Relay forwarding.** Use the session API directly for relay
  implementations; the threaded helper is for endpoint apps.
- **Multi-connection server.** v0 supports one active server
  connection per `moq_pq_threaded_t` instance.
