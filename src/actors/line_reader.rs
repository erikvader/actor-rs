use std::pin::pin;

use blocking::Unblock;
use futures_util::{AsyncBufRead, AsyncBufReadExt, AsyncRead, StreamExt, io::BufReader};
use tracing::Instrument;

use crate::{
    actor::{Actor, Address, Control, Receive, SecretAddress},
    kill_switch::{Bomb, Tick},
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

pub struct LineReader<R> {
    send_to: Option<SecretAddress<Line>>,
    read_from: Option<R>,
}

impl<R> LineReader<R>
where
    R: AsyncBufRead + 'static,
{
    pub fn new(reader: R, to: SecretAddress<Line>) -> Self {
        Self {
            read_from: Some(reader),
            send_to: Some(to),
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

impl<R> Actor for LineReader<R>
where
    R: AsyncBufRead + 'static,
{
    async fn enter(&mut self, ctl: &mut Control<Self>) {
        let send_to = self.send_to.take().expect("will exist here");
        let read_from = self.read_from.take().expect("will exist here");
        let myself: Address<Self> = ctl.address().expect("Is guaranteed to be alive in enter");

        let fut = move |bomb: Bomb| {
            async move {
                let _myself = myself; // NOTE: keep the actor alive

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

impl Stdin {
    pub fn stdin(to: SecretAddress<Line>) -> Self {
        // NOTE: this can't be sent to another thread, it's unblock that will send it to
        // a worker thread. It would be really nice if this could lock stdin for performance
        // reasons.
        // let locked = stdin.lock();
        // NOTE: Unblock is moving the stdin to another thread and reads up to and buffers 8 MB of
        // data at a time, so this actor should not be killed and respawned without expecting data
        // loss.
        LineReader::from_blocking_readable(std::io::stdin(), to)
    }
}

#[cfg(test)]
mod tests {
    use crate::actor::Stage;
    use futures_util::FutureExt;

    use super::*;

    fn test_template(input: &'static str) -> Vec<String> {
        let input = input.as_bytes();
        let (adr, output) = SecretAddress::new_channel();
        let stage = Stage::new_no_signals();
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
