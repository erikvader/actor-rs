use std::pin::pin;

use blocking::Unblock;
use futures_util::{AsyncBufReadExt, StreamExt, io::BufReader};
use tracing::Instrument;

use crate::{
    actor::{Actor, Control, SecretAddress},
    heart::{Heart, Watch},
};

pub struct Line(pub String);

// TODO: make this take any readable thing
pub struct Stdin {
    send_to: Option<SecretAddress<Line>>,
}

impl Actor for Stdin {
    async fn enter(&mut self, ctl: &mut Control<Self>) {
        let send_to = self.send_to.take().expect("will exist here");
        let myself = ctl.address();
        let fut = move |mut heart: Heart| {
            async move {
                let _myself = myself; // NOTE: keep the actor alive

                let stdin = {
                    let stdin = std::io::stdin();
                    // NOTE: this can't be sent to another thread, it's unblock that will send it to
                    // a worker thread.
                    // let locked = stdin.lock();
                    BufReader::new(Unblock::new(stdin))
                };

                let mut lines = pin!(heart.watch_stream(stdin.lines()));
                while let Some(watch) = lines.next().await {
                    match watch {
                        Watch::Ready(Ok(line)) => {
                            if send_to.send(Line(line)).await.is_err() {
                                tracing::warn!("Receiver closed");
                            }
                        }
                        Watch::Ready(Err(error)) => {
                            // TODO: possible to send lossy non-utf8 lines?
                            tracing::error!(
                                error = &error as &dyn std::error::Error,
                                "Line read errored"
                            );
                        }
                        Watch::DeadPanic(error) => {
                            tracing::error!(
                                error = &error as &dyn std::error::Error,
                                "Actor panicked"
                            );
                            break;
                        }
                        Watch::Dead => break,
                    }
                }

                tracing::debug!("Exited");
            }
            .instrument(tracing::info_span!("bg_job"))
        };
        ctl.start_job(fut);
    }

    async fn interrupted(&mut self, ctl: &mut Control<Self>) {
        tracing::debug!("Interrupt received");
        ctl.escalating_exit();
    }
}
