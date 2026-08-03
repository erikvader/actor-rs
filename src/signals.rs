use async_broadcast::{InactiveReceiver, Receiver};
use signal_hook::{
    consts::{SIGINT, SIGTERM},
    iterator::Handle,
};
use tracing::{debug, error, info_span, warn};

#[derive(PartialOrd, Ord, PartialEq, Eq, Debug, Clone, Copy)]
pub enum Signal {
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

pub struct Signals {
    signals_handle: Handle,
    thread_handle: Option<std::thread::JoinHandle<()>>,
    recv: InactiveSignalStream,
}

pub type SignalStream = Receiver<Signal>;
pub type InactiveSignalStream = InactiveReceiver<Signal>;

impl Signals {
    pub fn new() -> std::io::Result<Self> {
        let mut signals = signal_hook::iterator::Signals::new([SIGINT, SIGTERM])?;
        let signals_handle = signals.handle();
        let (snd, rcv) = async_broadcast::broadcast(16);
        const MAXIMUM: i32 = 3;

        let thread_handle = std::thread::spawn(move || {
            let _span = info_span!("signals").entered();
            debug!("Started");

            let mut sigint_count = 0;
            let mut sigterm_count = 0;
            let mut largest: Option<Signal> = None;

            for raw_signal in signals.forever() {
                let signal = Signal::try_from(raw_signal)
                    .expect("can't receive signals i'm not listening for");

                largest = std::cmp::max(largest, Some(signal));

                match signal {
                    Signal::Int => {
                        sigint_count += 1;
                        warn!(
                            count = sigint_count,
                            force_quit = MAXIMUM,
                            "Received SIGINT"
                        );
                        if sigint_count >= MAXIMUM {
                            break;
                        }
                    }
                    Signal::Term => {
                        sigterm_count += 1;
                        warn!(
                            count = sigterm_count,
                            force_quit = MAXIMUM,
                            "Received SIGTERM"
                        );
                        if sigterm_count >= MAXIMUM {
                            break;
                        }
                    }
                };

                if let Err(e) = snd.try_broadcast(signal) {
                    error!(
                        error = &e as &dyn std::error::Error,
                        "Failed to broadcast signal"
                    );
                }
            }

            if let Some(signal) = largest {
                debug!(?signal, "Executing default signal handler");
                signal_hook::low_level::emulate_default_handler(signal.into())
                    .expect("The signal exists");
            }

            debug!("Exited");
        });

        Ok(Self {
            thread_handle: Some(thread_handle),
            signals_handle,
            recv: rcv.deactivate(),
        })
    }

    pub fn signal_stream(&self) -> SignalStream {
        self.recv.activate_cloned()
    }

    pub fn inactive_signal_stream(&self) -> InactiveSignalStream {
        self.recv.clone()
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

pub fn dummy_signal_stream() -> InactiveSignalStream {
    let (_, rcv) = async_broadcast::broadcast(1);
    rcv.deactivate()
}
