use std::sync::{Arc, Mutex};

use signal_hook::consts::{SIGINT, SIGTERM};
use signal_hook::iterator::Handle as SigHookHandle;
use tracing::{debug, info_span, warn};

use registry::*;

use crate::actor::SecretAddress;
use crate::kill_switch::Switch;
mod registry {
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex, MutexGuard},
    };

    type Id = u64;

    pub struct Registry<T> {
        // TODO: this should/could be some concurrent hashmap to avoid the mutex
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

    pub struct Iter<'a, T> {
        guard: MutexGuard<'a, HashMap<Id, T>>,
    }

    impl<T> Iter<'_, T> {
        pub fn iter(&self) -> impl Iterator<Item = &T> {
            self.guard.values()
        }
    }

    // TODO: this shouldn't actually need to know T, since it doesn't do anything with it
    pub struct Guard<T> {
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

#[derive(Debug, Clone, Copy)]
pub struct Interrupted;

#[derive(Ord, PartialOrd, Eq, PartialEq)]
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
    thread_handle: Option<std::thread::JoinHandle<()>>,
    sig_reg: SigRegistry,
}

impl Signals {
    pub fn new() -> std::io::Result<Self> {
        let mut signals = signal_hook::iterator::Signals::new([SIGINT, SIGTERM])?;
        let signals_handle = signals.handle();
        let sig_reg = SigRegistry::new(SigInner::new());

        let thread_handle = std::thread::spawn({
            let sig_reg = sig_reg.clone();
            move || {
                let _span = info_span!("signals").entered();
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

                    match lock.state {
                        State::Running => (),
                        State::Interrupted => send_interrupts(lock.actor_registry.values()),
                        State::InterruptedTwice => {
                            for swt in lock.stage_registry.values().iter() {
                                swt.detonate();
                            }
                        }
                        State::InterruptedMore => break,
                    }
                }

                if let Some(signal) = largest {
                    debug!(?signal, "Executing default signal handler");
                    signal_hook::low_level::emulate_default_handler(signal.into())
                        .expect("The signal exists");
                }

                debug!("Exited");
            }
        });

        Ok(Self {
            thread_handle: Some(thread_handle),
            signals_handle,
            sig_reg,
        })
    }

    pub fn registry(&self) -> &SigRegistry {
        &self.sig_reg
    }
}

fn send_interrupts(values: Iter<'_, SecretAddress<Interrupted>>) {
    let mut fulls = Vec::new();
    for adr in values.iter() {
        if let Err(err) = adr.try_send(Interrupted) {
            match err {
                crate::actor::TrySendError::Full => fulls.push(adr.clone()),
                crate::actor::TrySendError::Closed => (),
            }
        }
    }

    if !fulls.is_empty() {
        tracing::trace!("Some actors had full mailboxes, spawning thread and sending to them");
        std::thread::spawn(move || {
            let _span = tracing::debug_span!("interrupter").entered();
            for adr in fulls {
                let _: Result<(), _> = adr.send_blocking(Interrupted);
            }
            tracing::debug!("Interrupter thread done");
        });
    }
}

impl Drop for Signals {
    fn drop(&mut self) {
        let _span = info_span!("signals_drop").entered();
        self.signals_handle.close();
        if let Some(thread_handle) = self.thread_handle.take() {
            debug!("Waiting for thread to die");
            let _: Result<_, _> = thread_handle.join();
        }
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

    pub fn register_stage(&self, switch: Switch) -> StageGuard {
        self.lock().stage_registry.register(switch)
    }

    pub fn register_actor(&self, adr: SecretAddress<Interrupted>) -> ActorGuard {
        let mut lock = self.lock();
        let guard = lock.actor_registry.register(adr);
        ActorGuard {
            guard,
            is_interrupted: lock.state >= State::Interrupted,
        }
    }
}

pub type StageGuard = Guard<Switch>;

pub struct ActorGuard {
    guard: Guard<SecretAddress<Interrupted>>,
    // NOTE: is true if the actor gets registerd after the first interrupts signal has been sent. An
    // alternate solution could have been to send the message to the address directly, but that is
    // not guaranteed to arrive if it had been closed, and the actor could dead lock itself if the
    // mailbox was full.
    pub is_interrupted: bool,
}

struct SigInner {
    stage_registry: Registry<Switch>,
    actor_registry: Registry<SecretAddress<Interrupted>>,
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
