# LibMoQ Integration Patterns

Recommended patterns for media applications using `moq_pq_threaded_t`
with the publisher/subscriber facades, media-object normalizer, and
playback pipeline.

## Threading Model

```
App / player thread              Network thread (picoquic)
───────────────────              ─────────────────────────
                                 on_pump():
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
| `on_pump` | Network | Yes — the intended call site |
| `on_activity` | Network | No — signal only |

`on_pump` runs between `moq_pq_service` calls. Create facades,
tick them, publish/poll objects here. Return 0 to continue,
nonzero to request clean shutdown.

Neither callback may call `moq_pq_threaded_stop`.

### Lifecycle

```c
moq_pq_threaded_create(&cfg, &t);   // start network thread

while (!done) {
    moq_pq_threaded_wake(t);        // ask on_pump to run
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

### on_pump implementation

```c
int receiver_pump(moq_pq_threaded_t *t, uint64_t now, void *ctx) {
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
the network thread inside `on_pump`.

```c
/* Set up track info from an MSF catalog entry (recommended). */
moq_media_track_info_t track;
moq_cmaf_init_info_t   cmaf_init;
moq_rcbuf_t           *init_buf = NULL;
moq_result_t rc = moq_msf_track_to_media_info(
    alloc, msf_track, &track, &cmaf_init, &init_buf);
/* track.media_type, .packaging, .timescale are filled.
 * cmaf_init.codec_config borrows from init_buf (decref when done).
 * For LOC tracks, init_buf is NULL and cmaf_init is zeroed. */

/* Or set up manually from app config: */
moq_media_track_info_init(&track);
track.media_type = MOQ_MEDIA_TYPE_VIDEO;
track.packaging  = MOQ_MEDIA_PACKAGING_CMAF;
track.timescale  = 90000;

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

### Playback pipeline

For applications that need keyframe gating, gap timeout, backlog
shedding, and DTS monotonicity enforcement, the `moq_playback_t`
pipeline wraps the normalizer with stateful track management:

```c
/* Create pipeline in on_pump after subscribing. */
moq_playback_cfg_t pcfg; moq_playback_cfg_init(&pcfg);
pcfg.max_samples_per_object = 30;
if (moq_playback_create(rx->alloc, &pcfg, &rx->playback) != MOQ_OK)
    return 1;

moq_playback_track_cfg_t ptc; moq_playback_track_cfg_init(&ptc);
ptc.media_type = MOQ_PLAYBACK_MEDIA_VIDEO;
ptc.packaging  = MOQ_PLAYBACK_PACKAGING_CMAF;
ptc.timescale  = 90000;
if (moq_playback_add_track(rx->playback, &ptc, &rx->pb_track) != MOQ_OK)
    return 1;

/* Feed objects in on_pump. */
moq_playback_push_sub_object(rx->playback, rx->pb_track,
    &sub_object, now);  /* returns MOQ_OK or error */

/* Tick and drain commands. */
moq_playback_tick(rx->playback, now);
moq_playback_cmd_t cmd;
while (moq_playback_poll_command(rx->playback, &cmd) == MOQ_OK) {
    app_queue_push(&rx->cmd_queue, &cmd);
    /* Commands own rcbuf refs (e.g. DECODE payload).
     * The queue consumer must call moq_playback_cmd_cleanup(&cmd)
     * after processing each command. */
}
```

## Publisher Pattern

The publisher receives media from the app thread via a queue,
encodes it as MoQ objects, and writes them on the network thread.

### on_pump implementation

```c
int publisher_pump(moq_pq_threaded_t *t, uint64_t now, void *ctx) {
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

    /* Drain write requests from app queue.
     * Each req owns one ref on payload and properties, transferred
     * from the app thread via the queue. write_object_ex borrows
     * them for the call and increfs internally for the action
     * queue. The pump decrefs the queue's ref after write. */
    write_request_t req;
    while (app_queue_pop(&px->write_queue, &req)) {
        moq_pub_object_cfg_t obj;
        moq_pub_object_cfg_init(&obj);
        obj.group_id   = req.group_id;
        obj.object_id  = req.object_id;
        obj.payload    = req.payload;
        obj.properties = req.properties;
        obj.end_of_group = req.end_of_group;
        moq_result_t wrc = moq_pub_write_object_ex(
            px->pub, px->media_track, &obj, now);
        if (wrc == MOQ_OK) {
            /* Written. Release queue's refs. */
            moq_rcbuf_decref(req.payload);
            if (req.properties) moq_rcbuf_decref(req.properties);
        } else if (wrc == MOQ_ERR_WOULD_BLOCK) {
            /* Action queue full. Requeue for retry on next pump
             * cycle — do not decref, queue still owns refs. */
            app_queue_push_front(&px->write_queue, &req);
            break;
            /* Alternative (low-latency drop policy): decref and
             * discard instead of requeuing. */
        } else {
            /* Permanent error. Release refs and drop. */
            moq_rcbuf_decref(req.payload);
            if (req.properties) moq_rcbuf_decref(req.properties);
        }
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
The network thread's `on_pump` owns them from that point and
decrefs after `write_object_ex` succeeds, or on permanent error.
On `WOULD_BLOCK`, refs stay with the requeued request.
Zero-copy through the entire path.

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
