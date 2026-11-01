//! Initial sender registration as one endpoint task.

use std::cell::Cell;
use std::ffi::c_void;
use std::fmt;
use std::marker::PhantomData;
use std::mem;
use std::sync::{Arc, Mutex, TryLockError};

use crate::endpoint::{Endpoint, WeakEndpoint};
use crate::error::{Error, Result};
use crate::sender::{SendTrack, SendTrackConfig, Sender, SenderConfig};
use crate::{ffi, sys};

type Attached = (Sender, Vec<SendTrack>);

enum State {
    Pending(SenderConfig, Vec<SendTrackConfig>),
    Ready(Result<Attached>),
    Cancelled,
    Taken,
}

struct Shared {
    endpoint: WeakEndpoint,
    state: Mutex<State>,
}

/// A cancellable initial sender registration from [`Sender::attach_with_tracks`].
///
/// The service attaches the sender and adds every supplied track in one
/// finite managed turn, before its first pump. The task calls no application
/// code and accepts no media payloads. A failed add destroys the partial
/// sender before any sender hook can publish its catalog.
///
/// [`poll`](Self::poll) is nonblocking. While pending, wait on the endpoint
/// for activity and poll again; cancellation and deadlines belong to the
/// application. Success returns track handles in the input order. It means
/// local registration completed, not peer acceptance: observe
/// [`Sender::is_ready`] separately.
///
/// Dropping the operation cancels a queued task or settles a running finite
/// registration and discards its result. An attachment completed but not yet
/// polled is destroyed on the dropping application thread. The operation
/// retains the endpoint until cancellation is settled; the task retains only
/// a weak endpoint, so a queued task cannot keep a stopped connection alive.
/// Calls, including drop, belong to application threads; `Send`, not `Sync`.
#[must_use = "dropping the operation cancels sender registration"]
pub struct SenderStartup {
    shared: Arc<Shared>,
    // Kept through Drop's state-lock handshake: the managed task must never
    // drop the final endpoint handle and join its own execution context.
    _endpoint: Endpoint,
    _not_sync: PhantomData<Cell<()>>,
}

// Keep formatting outside the execution mutex: a caller-provided formatter
// can unwind, and must never poison the state used by the C task callback.
impl fmt::Debug for SenderStartup {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SenderStartup")
            .finish_non_exhaustive()
    }
}

impl SenderStartup {
    pub(crate) fn queue(
        endpoint: &Endpoint,
        config: SenderConfig,
        tracks: Vec<SendTrackConfig>,
    ) -> Result<SenderStartup> {
        if tracks.is_empty() {
            return Err(Error::INVAL);
        }
        let shared = Arc::new(Shared {
            endpoint: endpoint.downgrade(),
            state: Mutex::new(State::Pending(config, tracks)),
        });
        let startup = SenderStartup {
            shared: shared.clone(),
            _endpoint: endpoint.clone(),
            _not_sync: PhantomData,
        };
        let context = Box::into_raw(Box::new(shared));
        // SAFETY: the endpoint is retained by startup. An accepted post calls
        // run exactly once; a refused post never calls it and leaves context
        // ours. Only concrete binding configs cross the thread boundary.
        let result = ffi::check(unsafe {
            sys::moq_endpoint_post(endpoint.as_ptr(), Some(run), context.cast())
        });
        if let Err(error) = result {
            // SAFETY: post refused this exact allocation without taking it.
            drop(unsafe { Box::from_raw(context) });
            return Err(error);
        }
        Ok(startup)
    }

    /// Take completed registration once, or `Ok(None)` while pending.
    ///
    /// Reports the native attach/add error without changing the endpoint's
    /// terminal state. A terminal task reports [`Error::CLOSED`]. A second
    /// poll after taking either a success or an error is
    /// [`Error::WRONG_STATE`]. Dropping the operation is cancellation.
    pub fn poll(&mut self) -> Result<Option<(Sender, Vec<SendTrack>)>> {
        let mut state = match self.shared.state.try_lock() {
            Ok(state) => state,
            Err(TryLockError::WouldBlock) => return Ok(None),
            Err(TryLockError::Poisoned(_)) => panic!("startup task did not panic"),
        };
        if matches!(*state, State::Pending(..)) {
            return Ok(None);
        }
        match mem::replace(&mut *state, State::Taken) {
            State::Ready(result) => result.map(Some),
            _ => Err(Error::WRONG_STATE),
        }
    }
}

impl Drop for SenderStartup {
    fn drop(&mut self) {
        // A running task holds this lock until it has released its temporary
        // endpoint handle. Taking its result here keeps Sender destruction,
        // and the last Endpoint drop, on the application thread.
        let old = {
            let mut state = self
                .shared
                .state
                .lock()
                .expect("startup task did not panic");
            mem::replace(&mut *state, State::Cancelled)
        };
        drop(old);
    }
}

unsafe extern "C" fn run(
    _endpoint: *mut sys::moq_endpoint_t,
    session: *mut sys::moq_session_t,
    _now_us: u64,
    context: *mut c_void,
) -> sys::moq_result_t {
    // SAFETY: the exactly-once executor owns the Box queue installed.
    let shared = unsafe { Box::from_raw(context.cast::<Arc<Shared>>()) };
    let mut state = shared.state.lock().expect("startup task did not panic");
    let State::Pending(config, tracks) = mem::replace(&mut *state, State::Cancelled) else {
        return sys::MOQ_OK;
    };
    let result = if session.is_null() {
        Err(Error::CLOSED)
    } else if let Some(endpoint) = shared.endpoint.upgrade() {
        // SenderStartup cannot drop its strong endpoint until this state
        // lock is released. No application-owned payloads or callbacks are
        // installed, so dropping a partial sender here is a finite detach.
        let result = register(&endpoint, &config, tracks);
        drop(endpoint);
        result
    } else {
        Err(Error::CLOSED)
    };
    *state = State::Ready(result);
    sys::MOQ_OK
}

fn register(
    endpoint: &Endpoint,
    config: &SenderConfig,
    tracks: Vec<SendTrackConfig>,
) -> Result<Attached> {
    let sender = Sender::attach(endpoint, config)?;
    let mut handles = Vec::with_capacity(tracks.len());
    for track in tracks {
        handles.push(sender.add_track(&track)?);
    }
    Ok((sender, handles))
}
