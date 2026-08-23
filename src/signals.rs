use std::process::{ExitCode, Termination};
use std::sync::{Arc, Mutex};

use signal_hook::consts::{SIGINT, SIGTERM};
use signal_hook::iterator::Handle as SigHookHandle;
use tracing::{debug, debug_span, info_span, warn};

use registry::*;

use crate::actor::SecretAddress;
use crate::kill_switch::Switch;
mod registry {
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex, MutexGuard},
    };

    type Id = u64;

    pub(crate) struct Registry<T> {
        // TODO: this could be some concurrent hashmap to avoid the mutex, but it probably doesn't
        // make a noticeable difference
        regs: Arc<Mutex<HashMap<Id, T>>>,
        next: Id,
    }

    impl<T> Registry<T> {
        pub fn new() -> Self {
            Self {
                next: 0,
                regs: Arc::new(Mutex::new(HashMap::new())),
            }
        }

        pub fn register(&mut self, val: T) -> Guard<T> {
            let id = {
                let mut regs = self.regs.lock().unwrap();
                // NOTE: self.next is not guarded by the lock, but self is mut here, so we have
                // exclisive access
                let id = self.next;
                self.next += 1;
                let prev = regs.insert(id, val);
                debug_assert!(prev.is_none());
                id
            };
            Guard {
                regs: Arc::clone(&self.regs),
                id,
            }
        }

        pub fn values(&self) -> Iter<'_, T> {
            // RANT: I wished mapped lock guards were stable... This could have returned the
            // iterator directly. https://github.com/rust-lang/rust/issues/117108
            let guard = self.regs.lock().unwrap();
            Iter { guard }
        }
    }

    pub(crate) struct Iter<'a, T> {
        guard: MutexGuard<'a, HashMap<Id, T>>,
    }

    impl<T> Iter<'_, T> {
        pub fn iter(&self) -> impl Iterator<Item = &T> {
            self.guard.values()
        }
    }

    // TODO: this shouldn't actually need to know T, since it doesn't do anything with it
    pub(crate) struct Guard<T> {
        regs: Arc<Mutex<HashMap<Id, T>>>,
        id: Id,
    }

    impl<T> Drop for Guard<T> {
        fn drop(&mut self) {
            let mut regs = self.regs.lock().unwrap();
            let prev = regs.remove(&self.id);
            debug_assert!(prev.is_some());
        }
    }
}

#[derive(PartialOrd, Ord, PartialEq, Eq, Debug, Clone, Copy)]
enum Signal {
    Int,
    Term,
}

impl TryFrom<i32> for Signal {
    type Error = ();

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            SIGINT => Ok(Signal::Int),
            SIGTERM => Ok(Signal::Term),
            _ => Err(()),
        }
    }
}

impl From<Signal> for i32 {
    fn from(value: Signal) -> Self {
        match value {
            Signal::Int => SIGINT,
            Signal::Term => SIGTERM,
        }
    }
}

impl Signal {
    // TODO: this should return the never type
    fn execute_default_action(self) {
        debug!("Executing default signal handler for {self:?}");
        signal_hook::low_level::emulate_default_handler(self.into()).expect("The signal exists");
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Interrupt;

#[derive(Ord, PartialOrd, Eq, PartialEq, Debug)]
enum State {
    Running,
    Interrupted,
    InterruptedTwice,
    InterruptedMore,
}

impl State {
    fn next(&self) -> Self {
        match self {
            State::Running => State::Interrupted,
            State::Interrupted => State::InterruptedTwice,
            State::InterruptedTwice => State::InterruptedMore,
            State::InterruptedMore => State::InterruptedMore,
        }
    }
}

pub struct Signals {
    signals_handle: SigHookHandle,
    thread_handle: Option<std::thread::JoinHandle<Option<Signal>>>,
    sig_reg: SigRegistry,
}

impl Signals {
    pub fn new() -> std::io::Result<Self> {
        let mut signals = signal_hook::iterator::Signals::new([SIGINT, SIGTERM])?;
        let signals_handle = signals.handle();
        let sig_reg = SigRegistry::new(SigInner::new());

        let thread_handle = crate::utils::spawn("signals", {
            let sig_reg = sig_reg.clone();
            move || {
                debug!("Started");

                let mut largest: Option<Signal> = None;

                for raw_signal in signals.forever() {
                    let signal = Signal::try_from(raw_signal)
                        .expect("can't receive signals i'm not listening for");

                    let _inner_span = info_span!("handling", ?signal).entered();

                    largest = std::cmp::max(largest, Some(signal));

                    warn!("Received {signal:?}"); // TODO: impl display for the signal type

                    let mut lock = sig_reg.lock();
                    lock.state = lock.state.next();
                    debug!("Entering state {:?}", lock.state);

                    match lock.state {
                        State::Running => {
                            unreachable!("this is the first state, next() can't return this")
                        }
                        State::Interrupted => send_interrupts(lock.actor_registry.values()),
                        State::InterruptedTwice => {
                            for swt in lock.stage_registry.values().iter() {
                                swt.detonate();
                            }
                        }
                        State::InterruptedMore => {
                            let signal = largest.expect("this will be set at this point");
                            signal.execute_default_action();
                        }
                    }
                }

                debug!(?largest, "Exited");
                largest
            }
        });

        Ok(Self {
            thread_handle: Some(thread_handle),
            signals_handle,
            sig_reg,
        })
    }

    pub fn registry(&self) -> SigRegistry {
        self.sig_reg.clone()
    }

    fn kill_thread(&mut self) -> Option<Signal> {
        self.signals_handle.close();
        if let Some(thread_handle) = self.thread_handle.take() {
            debug!("Waiting for the signals thread to die");
            match thread_handle.join() {
                Ok(val) => val,
                Err(panic) => std::panic::resume_unwind(panic),
            }
        } else {
            None
        }
    }

    pub fn terminate<T>(self, wrap: T) -> SigTerminate<T> {
        SigTerminate {
            signals: self,
            wrapped: wrap,
        }
    }
}

fn send_interrupts(values: Iter<'_, SecretAddress<Interrupt>>) {
    let mut fulls = Vec::new();
    for adr in values.iter() {
        if let Err(err) = adr.try_send(Interrupt) {
            match err {
                crate::actor::TrySendError::Full => fulls.push(adr.clone()),
                crate::actor::TrySendError::Closed => (),
            }
        }
    }

    if !fulls.is_empty() {
        tracing::trace!("Some actors had full mailboxes, spawning a thread and sending to them");
        crate::utils::spawn("interrupter", move || {
            for adr in fulls {
                tracing::trace!("Trying to send an interrupt");
                let _: Result<(), _> = adr.send_blocking(Interrupt);
            }
            tracing::trace!("Done");
        });
    }
}

impl AsRef<SigRegistry> for Signals {
    fn as_ref(&self) -> &SigRegistry {
        &self.sig_reg
    }
}

impl Drop for Signals {
    fn drop(&mut self) {
        self.kill_thread();
    }
}

#[derive(Clone)]
pub struct SigRegistry {
    inner: Arc<Mutex<SigInner>>,
}

impl SigRegistry {
    fn new(inner: SigInner) -> Self {
        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, SigInner> {
        self.inner.lock().unwrap()
    }

    pub(crate) fn register_stage(&self, switch: Switch) -> StageGuard {
        self.lock().stage_registry.register(switch)
    }

    pub(crate) fn register_actor(&self, adr: SecretAddress<Interrupt>) -> ActorGuard {
        let mut lock = self.lock();
        let guard = lock.actor_registry.register(adr);
        ActorGuard {
            _guard: guard,
            is_interrupted: lock.state >= State::Interrupted,
        }
    }
}

impl AsRef<SigRegistry> for SigRegistry {
    fn as_ref(&self) -> &SigRegistry {
        self
    }
}

pub(crate) type StageGuard = Guard<Switch>;

pub(crate) struct ActorGuard {
    _guard: Guard<SecretAddress<Interrupt>>,
    // NOTE: is true if the actor gets registered after the first interrupt signal has been sent. An
    // alternate solution could have been to send the message to the address directly, but that is
    // not guaranteed to arrive if it had been closed, and the actor could dead lock itself if the
    // mailbox was full.
    pub is_interrupted: bool,
}

struct SigInner {
    stage_registry: Registry<Switch>,
    actor_registry: Registry<SecretAddress<Interrupt>>,
    state: State,
}

impl SigInner {
    fn new() -> Self {
        Self {
            stage_registry: Registry::new(),
            actor_registry: Registry::new(),
            state: State::Running,
        }
    }
}

pub struct SigTerminate<T> {
    signals: Signals,
    wrapped: T,
}

impl<T: Termination> Termination for SigTerminate<T> {
    fn report(mut self) -> ExitCode {
        let _span = debug_span!("sig_post_main").entered();
        let inner_code = self.wrapped.report();
        debug!(?inner_code);

        if let Some(sig) = self.signals.kill_thread() {
            sig.execute_default_action();
        }

        inner_code
    }
}
