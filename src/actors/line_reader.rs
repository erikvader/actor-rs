use std::{pin::pin, sync::Arc};

use blocking::Unblock;
use futures_util::{AsyncBufRead, AsyncBufReadExt, AsyncRead, StreamExt, io::BufReader};
use snafu::{ResultExt, Snafu};

use crate::{
    actor::{Actor, Control, Receive, SecretAddress, bg_job::Job},
    deferred_info_span,
    deferred_span::DeferredSpan,
    kill_switch::{Bomb, Tick},
    signals::Interrupt,
};

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

struct ThreadData<R, C> {
    send_to: SecretAddress<Line>,
    read_from: R,
    cleaner: C,
}

// NOTE: This was originally meant as something that would read lines from stdin, but then I thought
// "lets make it more general", and now I have something that only works for stdin, and maybe named
// pipes. It's primarily how to handle different errors that are difficult to handle generally,
// because some uses are okay with logging them, and some would want to crash as soon as possible. I
// don't plan on using this in a real application, so i will leave it as is, i.e. something general
// that only makes sense for a couple of use cases.
pub struct LineReader<R, C = ()> {
    thread_data: Option<ThreadData<R, C>>,
    bomb: Bomb,
    error: Result<(), Error>,
}

impl<R> LineReader<R>
where
    R: AsyncBufRead + 'static,
{
    pub fn new(reader: R, to: SecretAddress<Line>) -> Self {
        Self::with_cleanup(reader, to, ())
    }
}

impl<R, C> LineReader<R, C>
where
    R: AsyncBufRead + 'static,
    C: AsyncCleanup + 'static,
{
    pub fn with_cleanup(read_from: R, send_to: SecretAddress<Line>, cleaner: C) -> Self {
        Self {
            thread_data: Some(ThreadData {
                send_to,
                read_from,
                cleaner,
            }),
            bomb: Bomb::new(),
            error: Ok(()),
        }
    }
}

impl<R> LineReader<BufReader<R>>
where
    R: AsyncRead + 'static,
{
    pub fn from_readable(reader: R, to: SecretAddress<Line>) -> Self {
        Self::new(BufReader::new(reader), to)
    }
}

impl<R> LineReader<BufReader<Unblock<R>>>
where
    R: std::io::Read + Send + 'static,
{
    pub fn from_blocking_readable(reader: R, to: SecretAddress<Line>) -> Self {
        Self::from_readable(Unblock::new(reader), to)
    }
}

/// Cleanup when a normal Drop is not enough.
// TODO: this should ideally take the reader, or at least a pinned one, for maximum flexibility, but
// it is wrapped in a bunch of streams and stuff at the moment, so it is not easy to extract the
// reader again. I'm not super convinced this is even needed either?
pub trait AsyncCleanup {
    #[expect(async_fn_in_trait, reason = "I don't really understand this warning")]
    async fn cleanup(self);
}

impl AsyncCleanup for () {
    async fn cleanup(self) {}
}

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

impl<R, C> Actor for LineReader<R, C>
where
    R: AsyncBufRead + 'static,
    C: AsyncCleanup + 'static,
{
    type Error = Error;

    crate::default_span!("line_reader");

    async fn enter(&mut self, ctl: &mut Control<Self>) {
        let ThreadData {
            send_to,
            read_from,
            cleaner,
        } = self.thread_data.take().expect("will exist on start");

        Job::new({
            let bomb = self.bomb.clone();
            async move {
                let mut res = Ok(());
                let mut lines = pin!(bomb.attach_stream(read_from.lines()));
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

                // TODO: collect an error from this as well? I added the cleanup as a "I think this
                // could be useful", but I haven't actually used it yet, so fix this when i actually
                // need it.
                tracing::trace!("Cleaning up");
                cleaner.cleanup().await;

                tracing::debug!("Exited");
                res
            }
        })
        .then(async |actor: &mut Self, ctl, res| {
            ctl.close_mailbox();
            actor.error = res;
        })
        .instrument(deferred_info_span!("bg_job"))
        .start(ctl);
    }

    async fn leave(self, _ctl: &mut Control<Self>) -> Result<(), Self::Error> {
        self.error
    }
}

impl<R> Receive<Interrupt> for LineReader<R>
where
    R: AsyncBufRead + 'static,
{
    type Retval = ();

    async fn receive(&mut self, _msg: Interrupt, ctl: &mut Control<Self>) -> Self::Retval {
        tracing::debug!("Interrupt message received");
        ctl.close_mailbox();
        self.bomb.detonate();
    }
}

pub type Stdin = LineReader<BufReader<Unblock<std::io::Stdin>>>;

pub fn stdin(to: SecretAddress<Line>) -> Stdin {
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
    LineReader::from_blocking_readable(std::io::stdin(), to)
}

#[cfg(test)]
mod tests {
    use crate::actor::Stage;
    use futures_util::FutureExt;

    use super::*;

    fn test_template(input: &'static str) -> Vec<String> {
        let input = input.as_bytes();
        let (adr, output) = SecretAddress::new_channel();
        let stage = Stage::new();
        stage.summon(LineReader::new(input, adr));
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
