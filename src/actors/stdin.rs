use std::{pin::pin, sync::Arc};

use blocking::Unblock;
use futures_util::{AsyncBufRead, AsyncBufReadExt, StreamExt, io::BufReader};
use snafu::{ResultExt, Snafu};

use crate::{
    actor::{Actor, Control, Hatchable, Receive, SecretAddress, bg_job::Job},
    deferred_info_span,
    kill_switch::{Bomb, Tick},
    signals::Interrupt,
};

#[derive(Debug, Snafu, Clone)]
pub enum Error {
    #[snafu(display("Failed while reading the next line"))]
    Reader {
        #[snafu(source(from(std::io::Error, Arc::new)))]
        source: Arc<std::io::Error>,
    },
    #[snafu(display("The receiver closed"))]
    Closed,
}

#[derive(Debug)]
pub struct Line(String);

impl From<Line> for String {
    fn from(value: Line) -> Self {
        value.into_string()
    }
}

impl Line {
    pub fn into_string(self) -> String {
        self.0
    }
}

pub struct Egg {
    send_to: SecretAddress<Line>,
}

impl Egg {
    pub fn new(send_to: SecretAddress<Line>) -> Self {
        Self { send_to }
    }
}

impl Hatchable for Egg {
    type Actor = Stdin;

    crate::default_span!("stdin");

    async fn hatch(
        self,
        ctl: &mut Control<Self::Actor>,
    ) -> Result<Self::Actor, <Self::Actor as Actor>::Error> {
        // NOTE: this can't be sent to another thread, it's unblock that will send it to
        // a worker thread. It would be really nice if this could lock stdin for performance
        // reasons.
        // TODO: The way to read from a locked stdin is to create a custom Unblock that creates a
        // thread and locks it there.
        // NOTE: Sending stdin to another thread to block read it is the easiest solution, it is
        // also possible to make it nonblocking and wrap it in some kind of AsyncFd, but care should
        // be taken to make sure the settings on the fd are restored since the terminal session
        // itself gets affected. I guess it could also be possible to use the fd directly and adding
        // support to Heart to interrupt a blocking fd using eventfd and select, but that is a lot
        // more work to implement.
        // let locked = stdin.lock();
        // NOTE: Unblock is moving the stdin to another thread and reads up to and buffers 8 MB of
        // data at a time, so this actor should not be killed and respawned without expecting data
        // loss.
        let stdin = BufReader::new(Unblock::new(std::io::stdin()));
        Ok(Stdin::new(ctl, self.send_to, stdin))
    }
}

pub struct Stdin {
    bomb: Bomb,
}

impl Stdin {
    fn new(
        ctl: &mut Control<Stdin>,
        send_to: SecretAddress<Line>,
        source: impl AsyncBufRead + 'static,
    ) -> Self {
        let bomb = Bomb::new();

        Job::new({
            let bomb = bomb.clone();
            async move {
                let mut res = Ok(());
                let mut lines = pin!(bomb.attach_stream(source.lines()));
                while let Some(watch) = lines.next().await {
                    match watch {
                        Tick::Tock(Ok(line)) => {
                            if send_to.send(Line(line)).await.is_err() {
                                tracing::debug!("Receiver closed");
                                res = ClosedSnafu.fail();
                                break;
                            }
                        }
                        Tick::Tock(Err(error)) => {
                            // NOTE: there are a lot of different recovery strategies, like continuing
                            // with the next line if an utf-8 error occurred, sending a lossy converted
                            // string or the raw bytes, etc. But the simplest and safest option that
                            // never causes silent data loss is to return early on any kind of error.
                            // What to do on certain errors depends on the context and what the Reader
                            // actually is, so i leave that for the future.
                            tracing::debug!(?error, "Reader errored");
                            res = Err(error).context(ReaderSnafu);
                            break;
                        }
                        Tick::Boom => {
                            tracing::debug!("Interrupted early");
                            break;
                        }
                    }
                }

                tracing::debug!("Exited");
                res
            }
        })
        .then(async |_, ctl, res| {
            ctl.close_mailbox();
            let prev = ctl.set_result(res);
            assert!(prev.is_none());
        })
        .instrument(deferred_info_span!("bg_job"))
        .start(ctl);

        Self { bomb }
    }
}

impl Actor for Stdin {
    type Error = Error;
}

impl Receive<Interrupt> for Stdin {
    type Retval = ();

    async fn receive(&mut self, _msg: Interrupt, ctl: &mut Control<Self>) -> Self::Retval {
        tracing::debug!("Interrupt message received");
        ctl.close_and_clear_mailbox();
        self.bomb.detonate();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor::Stage;
    use futures_util::FutureExt;

    struct TestEgg {
        send_to: SecretAddress<Line>,
        data: &'static [u8],
    }

    impl Hatchable for TestEgg {
        type Actor = Stdin;

        crate::default_span!("test_stdin");

        async fn hatch(
            self,
            ctl: &mut Control<Self::Actor>,
        ) -> Result<Self::Actor, <Self::Actor as Actor>::Error> {
            let input = BufReader::new(self.data);
            Ok(Stdin::new(ctl, self.send_to, input))
        }
    }

    fn test_template(input: &'static str) -> Vec<String> {
        let input = input.as_bytes();
        let (adr, output) = SecretAddress::new_channel();
        let stage = Stage::new();
        stage.summon(TestEgg {
            data: input,
            send_to: adr,
        });
        stage.assert_plays_within(10);
        let lines: Vec<String> = output
            .map(Into::into)
            .collect::<Vec<_>>()
            .now_or_never()
            .unwrap();
        lines
    }

    #[test]
    fn no_lines() {
        assert_eq!(test_template(""), Vec::<String>::new());
    }

    #[test]
    fn one_empty_line() {
        assert_eq!(test_template("\n"), vec![""]);
    }

    #[test]
    fn missing_last_terminator() {
        assert_eq!(test_template("line 1\nline 2"), vec!["line 1", "line 2"]);
    }

    #[test]
    fn one_line() {
        assert_eq!(test_template("line 1\n"), vec!["line 1"]);
    }
}
