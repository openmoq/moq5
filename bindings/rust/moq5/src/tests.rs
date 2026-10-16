//! Ownership and ordering rules, checked against fake libmoq service calls.

use std::ffi::c_void;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

use crate::sys::*;
use crate::testing::{install, Api, ApiGuard, REAL};
use crate::*;

static SERIAL: Mutex<()> = Mutex::new(());
static LOG: Mutex<Vec<String>> = Mutex::new(Vec::new());
static EP: AtomicU8 = AtomicU8::new(0);
static SENDER: AtomicU8 = AtomicU8::new(0);
static RECEIVER: AtomicU8 = AtomicU8::new(0);
static TRACKS: [AtomicU8; 4] = [const { AtomicU8::new(0) }; 4];

type TaskFn =
    unsafe extern "C" fn(*mut moq_endpoint_t, *mut moq_session_t, u64, *mut c_void) -> moq_result_t;

#[derive(Default)]
struct Fake {
    posted: Vec<(TaskFn, usize)>,
    post_result: moq_result_t,
    attach_result: moq_result_t,
    write_result: moq_result_t,
    tracks_added: usize,
    ready: Option<(
        unsafe extern "C" fn(*mut c_void, *mut moq_media_sender_t),
        usize,
    )>,
    ready_on_destroy: bool,
    cleaned: u32,
    objects: Vec<moq_media_object_t>,
}

// SAFETY: test fixtures only.
unsafe impl Send for Fake {}

static FAKE: Mutex<Option<Fake>> = Mutex::new(None);

fn fake<R>(f: impl FnOnce(&mut Fake) -> R) -> R {
    f(FAKE.lock().unwrap().get_or_insert_with(Fake::default))
}

fn log(s: impl Into<String>) {
    LOG.lock().unwrap().push(s.into());
}

fn take_log() -> Vec<String> {
    std::mem::take(&mut *LOG.lock().unwrap())
}

fn ptr<T>(a: &'static AtomicU8) -> *mut T {
    a as *const AtomicU8 as *mut T
}

unsafe extern "C" fn cfg_init(c: *mut moq_endpoint_cfg_t) {
    *c = moq_endpoint_cfg_t::default();
    (*c).struct_size = 1;
    log("cfg_init");
}
unsafe extern "C" fn connect(
    c: *const moq_endpoint_cfg_t,
    out: *mut *mut moq_endpoint_t,
) -> moq_result_t {
    let c = &*c;
    let url = std::slice::from_raw_parts(c.url.data, c.url.len);
    let versions = if c.versions.version_count > 0 {
        format!(" policy={} v={}", c.versions.policy, *c.versions.versions)
    } else {
        String::new()
    };
    log(format!(
        "connect {} proto={} insecure={}{}",
        String::from_utf8_lossy(url),
        c.protocol,
        c.insecure_skip_verify,
        versions
    ));
    *out = ptr(&EP);
    MOQ_OK
}
unsafe extern "C" fn stop(ep: *mut moq_endpoint_t) -> moq_result_t {
    assert_eq!(ep, ptr(&EP));
    log("stop");
    // A real stop runs every queued task closed.
    let posted = fake(|f| std::mem::take(&mut f.posted));
    for (task, ctx) in posted {
        assert_eq!(
            task(ep, std::ptr::null_mut(), 0, ctx as *mut c_void),
            MOQ_ERR_INTERRUPTED
        );
    }
    MOQ_OK
}
unsafe extern "C" fn destroy(_ep: *mut moq_endpoint_t) {
    log("destroy");
}
unsafe extern "C" fn post(
    _ep: *mut moq_endpoint_t,
    task: moq_endpoint_task_fn,
    ctx: *mut c_void,
) -> moq_result_t {
    fake(|f| {
        if f.post_result == MOQ_OK {
            f.posted.push((task.unwrap(), ctx as usize));
        }
        f.post_result
    })
}
unsafe extern "C" fn sender_cfg_init(c: *mut moq_media_sender_cfg_t, n: usize) {
    assert_eq!(n, std::mem::size_of::<moq_media_sender_cfg_t>());
    *c = moq_media_sender_cfg_t::default();
}
unsafe extern "C" fn callbacks_init(c: *mut moq_media_sender_callbacks_t, n: usize) {
    assert_eq!(n, std::mem::size_of::<moq_media_sender_callbacks_t>());
    *c = moq_media_sender_callbacks_t::default();
}
unsafe extern "C" fn attach(
    ep: *mut moq_endpoint_t,
    c: *const moq_media_sender_cfg_t,
    out: *mut *mut moq_media_sender_t,
) -> moq_result_t {
    assert_eq!(ep, ptr(&EP));
    let c = &*c;
    assert_eq!(c.namespace_.count, 2);
    log(format!("attach cps={}", c.content_protection_count));
    let rc = fake(|f| {
        f.ready = c.callbacks.on_ready.map(|r| (r, c.callbacks.ctx as usize));
        f.attach_result
    });
    if rc == MOQ_OK {
        *out = ptr(&SENDER);
    }
    rc
}
unsafe extern "C" fn sender_destroy(s: *mut moq_media_sender_t) {
    assert_eq!(s, ptr(&SENDER));
    log("sender_destroy");
    let (ready, on_destroy) = fake(|f| (f.ready, f.ready_on_destroy));
    if let (Some((f, ctx)), true) = (ready, on_destroy) {
        // The callback context must still be alive here.
        f(ctx as *mut c_void, s);
    }
}
unsafe extern "C" fn track_cfg_init(c: *mut moq_media_track_cfg_t, n: usize) {
    assert_eq!(n, std::mem::size_of::<moq_media_track_cfg_t>());
    *c = moq_media_track_cfg_t::default();
}
unsafe extern "C" fn add_track(
    _s: *mut moq_media_sender_t,
    c: *const moq_media_track_cfg_t,
    out: *mut *mut moq_media_track_t,
) -> moq_result_t {
    let c = &*c;
    let name =
        String::from_utf8_lossy(std::slice::from_raw_parts(c.name.data, c.name.len)).into_owned();
    log(format!(
        "add_track {name} refs={}",
        c.content_protection_ref_id_count
    ));
    let i = fake(|f| {
        f.tracks_added += 1;
        f.tracks_added - 1
    });
    *out = ptr(&TRACKS[i]);
    MOQ_OK
}
unsafe extern "C" fn write(
    _s: *mut moq_media_sender_t,
    _t: *mut moq_media_track_t,
    o: *const moq_media_send_object_t,
) -> moq_result_t {
    let rc = fake(|f| f.write_result);
    if rc == MOQ_OK {
        moq_rcbuf_decref((*o).payload); // ownership transferred
    }
    log(format!("write rc={rc}"));
    rc
}
unsafe extern "C" fn receiver_cfg_init(c: *mut moq_media_receiver_cfg_t) {
    *c = moq_media_receiver_cfg_t::default();
    (*c).auto_subscribe = true;
}
unsafe extern "C" fn receiver_attach(
    _ep: *mut moq_endpoint_t,
    c: *const moq_media_receiver_cfg_t,
    out: *mut *mut moq_media_receiver_t,
) -> moq_result_t {
    log(format!("receiver_attach auto={}", (*c).auto_subscribe));
    *out = ptr(&RECEIVER);
    MOQ_OK
}
unsafe extern "C" fn receiver_destroy(_r: *mut moq_media_receiver_t) {
    log("receiver_destroy");
}
unsafe extern "C" fn poll_object(
    _r: *mut moq_media_receiver_t,
    o: *mut moq_media_object_t,
    n: usize,
) -> moq_result_t {
    assert_eq!(n, std::mem::size_of::<moq_media_object_t>());
    match fake(|f| f.objects.pop()) {
        Some(obj) => {
            *o = obj;
            MOQ_OK
        }
        None => MOQ_DONE,
    }
}
unsafe extern "C" fn cleanup(_o: *mut moq_media_object_t) {
    fake(|f| f.cleaned += 1);
}

const API: Api = Api {
    moq_endpoint_cfg_init: cfg_init,
    moq_endpoint_connect: connect,
    moq_endpoint_stop: stop,
    moq_endpoint_destroy: destroy,
    moq_endpoint_post: post,
    moq_media_sender_cfg_init_live_sized: sender_cfg_init,
    moq_media_sender_callbacks_init_sized: callbacks_init,
    moq_media_sender_attach: attach,
    moq_media_sender_destroy: sender_destroy,
    moq_media_track_cfg_init_sized: track_cfg_init,
    moq_media_sender_add_track: add_track,
    moq_media_sender_write: write,
    moq_media_receiver_cfg_init_live: receiver_cfg_init,
    moq_media_receiver_attach: receiver_attach,
    moq_media_receiver_destroy: receiver_destroy,
    moq_media_receiver_poll_object: poll_object,
    moq_media_object_cleanup: cleanup,
    ..REAL
};

struct Suite {
    _api: ApiGuard,
    _serial: MutexGuard<'static, ()>,
}

fn suite() -> Suite {
    let serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    *FAKE.lock().unwrap() = Some(Fake::default());
    take_log();
    Suite {
        _api: install(API),
        _serial: serial,
    }
}

fn endpoint() -> Endpoint {
    let ep = Endpoint::connect(
        &EndpointConfig::new("https://relay:4443/moq")
            .protocol(Protocol::WebTransport)
            .versions(VersionOffer::Exact(Version::DRAFT_16)),
    )
    .unwrap();
    take_log();
    ep
}

fn ns() -> Namespace {
    Namespace::parse("live/demo").unwrap()
}

fn video() -> TrackConfig {
    TrackConfig::new("video", MediaType::Video, Packaging::Loc, "avc1.42e01e").bitrate(1)
}

/// Run the one posted task as the network thread would, live.
fn run_posted() -> moq_result_t {
    let (task, ctx) = fake(|f| f.posted.remove(0));
    unsafe { task(ptr(&EP), ptr(&EP), 7, ctx as *mut c_void) }
}

#[test]
fn connect_maps_the_config() {
    let _s = suite();
    let ep = Endpoint::connect(
        &EndpointConfig::new("https://relay:4443/moq")
            .protocol(Protocol::WebTransport)
            .insecure_skip_verify(true)
            .versions(VersionOffer::Exact(Version::DRAFT_16)),
    )
    .unwrap();
    assert_eq!(
        take_log(),
        [
            "cfg_init",
            &format!(
                "connect https://relay:4443/moq proto={} insecure=true policy={} v=16",
                MOQ_TRANSPORT_PROTOCOL_WEBTRANSPORT, MOQ_VERSION_POLICY_EXACT
            )
        ]
    );
    drop(ep);
    assert_eq!(take_log(), ["stop", "destroy"]);
}

#[test]
fn endpoint_stops_after_its_last_handle() {
    let _s = suite();
    let ep = endpoint();
    let receiver =
        MediaReceiver::attach(&ep, ReceiverConfig::live(ns()).auto_subscribe(false)).unwrap();
    let clone = ep.clone();
    drop(ep);
    drop(clone);
    assert_eq!(take_log(), ["receiver_attach auto=false"]);
    // The receiver holds the endpoint: it is destroyed first.
    drop(receiver);
    assert_eq!(take_log(), ["receiver_destroy", "stop", "destroy"]);
}

#[test]
fn preset_values_survive_unset_fields() {
    let _s = suite();
    let ep = endpoint();
    let receiver = MediaReceiver::attach(&ep, ReceiverConfig::live(ns())).unwrap();
    assert_eq!(take_log(), ["receiver_attach auto=true"]);
    drop(receiver);
}

#[test]
fn startup_attaches_and_adds_in_one_task() {
    let _s = suite();
    let ep = endpoint();
    let cps = vec![ContentProtection {
        ref_id: "cp0".into(),
        default_kids: vec!["00000000-0000-0000-0000-000000000000".into()],
        scheme: "cenc".into(),
        ..Default::default()
    }];
    let audio = TrackConfig::new("audio", MediaType::Audio, Packaging::Cmaf, "opus")
        .content_protection_ref_ids(vec!["cp0".into()]);
    let startup = MediaSender::start(
        &ep,
        SenderConfig::live(ns()).content_protections(cps),
        vec![video(), audio],
    )
    .unwrap();
    let startup = match startup.wait(Duration::ZERO) {
        StartupStatus::Pending(s) => s,
        StartupStatus::Done(_) => panic!("ran before the network thread did"),
    };
    assert!(take_log().is_empty());
    assert_eq!(run_posted(), MOQ_OK);
    assert_eq!(
        take_log(),
        [
            "attach cps=1",
            "add_track video refs=0",
            "add_track audio refs=1"
        ]
    );
    let StartupStatus::Done(Ok((sender, tracks))) = startup.wait(Duration::ZERO) else {
        panic!("startup failed");
    };
    assert_eq!(tracks.len(), 2);
    assert_eq!(tracks[1].as_ptr(), ptr(&TRACKS[1]));
    drop(ep);
    assert!(take_log().is_empty());
    drop(sender);
    assert_eq!(take_log(), ["sender_destroy", "stop", "destroy"]);
}

#[test]
fn cancelled_startup_runs_closed() {
    let _s = suite();
    let ep = endpoint();
    let startup = MediaSender::start(&ep, SenderConfig::live(ns()), vec![video()]).unwrap();
    startup.cancel();
    // The task has not run; stopping the endpoint runs it closed.
    drop(ep);
    assert_eq!(take_log(), ["stop", "destroy"]);
}

#[test]
fn startup_cancelled_after_attach_destroys_the_sender() {
    let _s = suite();
    let ep = endpoint();
    let startup = MediaSender::start(&ep, SenderConfig::live(ns()), vec![video()]).unwrap();
    assert_eq!(run_posted(), MOQ_OK);
    take_log();
    drop(startup);
    assert_eq!(take_log(), ["sender_destroy"]);
}

#[test]
fn failed_attach_reports_and_leaks_nothing() {
    let _s = suite();
    let ep = endpoint();
    fake(|f| f.attach_result = MOQ_ERR_INVAL);
    let startup = MediaSender::start(&ep, SenderConfig::live(ns()), vec![video()]).unwrap();
    assert_eq!(run_posted(), MOQ_ERR_INVAL);
    let StartupStatus::Done(Err(e)) = startup.wait(Duration::ZERO) else {
        panic!("expected a failure");
    };
    assert_eq!(e, Error::INVAL);
    assert_eq!(take_log(), ["attach cps=0"]);
}

#[test]
fn refused_post_drops_the_task_unrun() {
    let _s = suite();
    let ep = endpoint();
    fake(|f| f.post_result = MOQ_ERR_CLOSED);
    let ran = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let r = ran.clone();
    assert_eq!(
        ep.post(move |_| {
            r.store(true, Ordering::SeqCst);
            Ok(())
        }),
        Err(Error::CLOSED)
    );
    assert!(!ran.load(Ordering::SeqCst));
    assert_eq!(std::sync::Arc::strong_count(&ran), 1);
}

#[test]
fn posted_task_sees_the_session() {
    let _s = suite();
    let ep = endpoint();
    let seen = std::sync::Arc::new(Mutex::new(None));
    let s = seen.clone();
    ep.post(move |ctx| {
        *s.lock().unwrap() = ctx.map(|c| c.now_us());
        Ok(())
    })
    .unwrap();
    assert_eq!(run_posted(), MOQ_OK);
    assert_eq!(*seen.lock().unwrap(), Some(7));
}

#[test]
fn last_endpoint_dropped_inside_a_task_is_leaked_not_stopped() {
    let _s = suite();
    let ep = endpoint();
    let inner = ep.clone();
    ep.post(move |_| {
        drop(inner);
        Ok(())
    })
    .unwrap();
    drop(ep);
    assert!(take_log().is_empty(), "the task still holds a handle");
    // The task drops the last handle on the network thread, which cannot
    // join itself: the endpoint is leaked instead of stopped.
    assert_eq!(run_posted(), MOQ_OK);
    assert!(take_log().is_empty());
}

#[test]
fn write_transfers_on_success_and_returns_on_failure() {
    let _s = suite();
    let ep = endpoint();
    let sender = MediaSender::attach(&ep, SenderConfig::live(ns())).unwrap();
    let track = sender.add_track(video()).unwrap();
    take_log();

    let drops = std::sync::Arc::new(());
    struct Owned(#[allow(dead_code)] std::sync::Arc<()>);
    impl AsRef<[u8]> for Owned {
        fn as_ref(&self) -> &[u8] {
            b"frame"
        }
    }
    let obj = SendObject::new(RcBuf::from_owner(Owned(drops.clone())).unwrap());
    sender.write(track, obj).unwrap();
    assert_eq!(
        std::sync::Arc::strong_count(&drops),
        1,
        "released by the service"
    );

    fake(|f| f.write_result = MOQ_ERR_WOULD_BLOCK);
    let obj = SendObject::new(RcBuf::from_owner(Owned(drops.clone())).unwrap());
    let err = sender.write(track, obj).unwrap_err();
    assert_eq!(err.error, Error::WOULD_BLOCK);
    assert_eq!(err.object.payload.as_slice(), b"frame");
    assert_eq!(std::sync::Arc::strong_count(&drops), 2, "still ours");
    drop(err);
    assert_eq!(std::sync::Arc::strong_count(&drops), 1);
    assert_eq!(
        take_log(),
        ["write rc=0", &format!("write rc={MOQ_ERR_WOULD_BLOCK}")]
    );
}

#[test]
fn foreign_track_never_reaches_libmoq() {
    let _s = suite();
    let ep = endpoint();
    let a = MediaSender::attach(&ep, SenderConfig::live(ns())).unwrap();
    let track = a.add_track(video()).unwrap();
    drop(a);
    let b = MediaSender::attach(&ep, SenderConfig::live(ns())).unwrap();
    take_log();
    let err = b
        .write(
            track,
            SendObject::new(RcBuf::copy_from_slice(b"x").unwrap()),
        )
        .unwrap_err();
    assert_eq!(err.error, Error::INVAL);
    assert_eq!(b.end_track(track), Err(Error::INVAL));
    assert!(take_log().is_empty());
}

#[test]
fn callback_context_outlives_destroy() {
    let _s = suite();
    let ep = endpoint();
    let calls = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
    let c = calls.clone();
    let sender = MediaSender::attach(
        &ep,
        SenderConfig::live(ns()).on_ready(move || {
            c.fetch_add(1, Ordering::SeqCst);
        }),
    )
    .unwrap();
    let (f, ctx) = fake(|f| f.ready).expect("on_ready registered");
    unsafe { f(ctx as *mut c_void, sender.as_ptr()) };
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    fake(|f| f.ready_on_destroy = true);
    drop(sender);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[test]
fn objects_clean_up_once() {
    let _s = suite();
    let ep = endpoint();
    let receiver = MediaReceiver::attach(&ep, ReceiverConfig::live(ns())).unwrap();
    static PAYLOAD: [u8; 3] = [1, 2, 3];
    fake(|f| {
        f.objects.push(moq_media_object_t {
            track: ptr(&TRACKS[0]),
            packaging: MOQ_MEDIA_PACKAGING_RAW,
            payload: moq_bytes_t {
                data: PAYLOAD.as_ptr(),
                len: 3,
            },
            keyframe: true,
            ..Default::default()
        })
    });
    let obj = receiver.poll_object().unwrap().unwrap();
    assert_eq!(obj.payload(), [1, 2, 3]);
    assert_eq!(obj.packaging(), Some(Packaging::Loc));
    assert!(obj.is_keyframe());
    assert!(receiver.poll_object().unwrap().is_none());
    assert_eq!(fake(|f| f.cleaned), 0);
    drop(obj);
    assert_eq!(fake(|f| f.cleaned), 1);
}
