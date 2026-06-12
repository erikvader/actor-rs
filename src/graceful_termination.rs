use async_broadcast::{InactiveReceiver, Receiver};
use signal_hook::{
    consts::{SIGINT, SIGTERM},
    iterator::Handle,
};
use tracing::{debug, error, info_span};

#[derive(PartialOrd, Ord, PartialEq, Eq, Debug, Clone, Copy)]
pub enum Signal {
    Int,
    Term,
}

pub struct GracefulTermination {
    signals_handle: Handle,
    thread_handle: Option<std::thread::JoinHandle<()>>,
    recv: InactiveSignalStream,
}

pub type SignalStream = Receiver<Signal>;
pub type InactiveSignalStream = InactiveReceiver<Signal>;

impl GracefulTermination {
    pub fn new() -> std::io::Result<Self> {
        let mut signals = signal_hook::iterator::Signals::new([SIGINT, SIGTERM])?;
        let signals_handle = signals.handle();
        let (snd, rcv) = async_broadcast::broadcast(16);
        const MAXIMUM: i32 = 2;

        let thread_handle = std::thread::spawn(move || {
            let _span = info_span!("Signals thread").entered();
            debug!("Started");

            let mut sigint_count = 0;
            let mut sigterm_count = 0;
            let mut latest: Option<i32> = None;

            for raw_signal in signals.forever() {
                debug!(raw_signal, sigint_count, sigterm_count, "Received signal");

                latest = Some(raw_signal);
                let signal = match raw_signal {
                    SIGINT => {
                        sigint_count += 1;
                        if sigint_count >= MAXIMUM {
                            break;
                        }
                        Signal::Int
                    }
                    SIGTERM => {
                        sigterm_count += 1;
                        if sigterm_count >= MAXIMUM {
                            break;
                        }
                        Signal::Term
                    }
                    _ => unreachable!(),
                };

                if let Err(e) = snd.try_broadcast(signal) {
                    error!(error = %e, "Failed to broadcast signal");
                }
            }

            if let Some(signal) = latest {
                debug!(signal, "Executing default signal handler");
                signal_hook::low_level::emulate_default_handler(signal).expect("The signal exists");
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

impl Drop for GracefulTermination {
    fn drop(&mut self) {
        self.signals_handle.close();
        if let Some(thread_handle) = self.thread_handle.take() {
            let _: Result<_, _> = thread_handle.join();
        }
    }
}
