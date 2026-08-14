use std::{convert::Infallible, pin::pin};

use blocking::Unblock;
use futures_util::{AsyncBufRead, AsyncBufReadExt, AsyncRead, StreamExt, io::BufReader};
use tracing::Instrument;

use crate::{
    actor::{Actor, Address, Control, Receive, SecretAddress, bg_job::Job},
    deferred_info_span,
    kill_switch::{self, Bomb, Tick},
    utils::DeferredSpan,
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

pub struct LineReader<R, C = ()> {
    send_to: Option<SecretAddress<Line>>,
    read_from: Option<R>,
    cleaner: Option<C>,
}

impl<R> LineReader<R>
where
    R: AsyncBufRead + 'static,
{
    pub fn new(reader: R, to: SecretAddress<Line>) -> Self {
        Self {
            read_from: Some(reader),
            send_to: Some(to),
            cleaner: Some(()),
        }
    }
}

impl<R, C> LineReader<R, C>
where
    R: AsyncBufRead + 'static,
    C: AsyncCleanup + 'static,
{
    pub fn with_cleanup(reader: R, to: SecretAddress<Line>, cleanup: C) -> Self {
        Self {
            read_from: Some(reader),
            send_to: Some(to),
            cleaner: Some(cleanup),
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
// TODO: this should ideally take the reader, or at least a pinned one, but it is wrapped in a bunch
// of streams and stuff at the moment, so it is not easy to extract the reader again. I'm not super
// convinced this is even needed either?
pub trait AsyncCleanup {
    #[expect(async_fn_in_trait, reason = "I don't really understand this warning")]
    async fn cleanup(self);
}

impl AsyncCleanup for () {
    async fn cleanup(self) {}
}

impl<R, C> Actor for LineReader<R, C>
where
    R: AsyncBufRead + 'static,
    C: AsyncCleanup + 'static,
{
    type Error = Infallible;

    async fn enter(&mut self, ctl: &mut Control<Self>) {
        let send_to = self.send_to.take().expect("will exist here");
        let read_from = self.read_from.take().expect("will exist here");
        let cleanup = self.cleaner.take().expect("will exist here");

        // TODO: activate the switch on shutdown
        let (bomb, switch) = kill_switch::create();
        Job::new(async move {
            let mut lines = pin!(bomb.attach_stream(read_from.lines()));
            while let Some(watch) = lines.next().await {
                match watch {
                    Tick::Tock(Ok(line)) => {
                        if send_to.send(Line(line)).await.is_err() {
                            tracing::warn!("Receiver closed");
                        }
                    }
                    Tick::Tock(Err(error)) => {
                        // TODO: the lines stream will return this error if a line cannot be
                        // converted into an UTF-8 string, it will continue as normal with the
                        // next one though. Should I care about invalid lines? How to handle
                        // them?
                        tracing::error!(
                            error = &error as &dyn std::error::Error,
                            "Line read errored"
                        );
                    }
                    Tick::Boom => {
                        tracing::debug!("Interrupted early");
                        break;
                    }
                }
            }

            tracing::trace!("Cleaning up");
            cleanup.cleanup().await;

            tracing::debug!("Exited");
        });
        // TODO:
        // .start(ctl);

        // TODO: re-add the span to the job
        // ctl.start_job(fut, |parent| tracing::info_span!(parent: parent, "bg_job"));
    }

    async fn interrupted(&mut self, ctl: &mut Control<Self>) {
        tracing::debug!("Interrupt received");
        ctl.escalating_exit();
    }

    fn span(&self) -> DeferredSpan<'_> {
        // TODO: add what is being read from somehow?
        deferred_info_span!("line_reader")
    }
}

pub struct Exit;
impl<R> Receive<Exit> for LineReader<R>
where
    R: AsyncBufRead + 'static,
{
    type Retval = ();

    async fn receive(&mut self, _msg: Exit, ctl: &mut Control<Self>) -> Self::Retval {
        tracing::debug!("Exit message received");
        ctl.escalating_exit();
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
        let stage = Stage::without_signals();
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
