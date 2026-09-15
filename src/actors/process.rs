use std::{io, marker::PhantomData, pin::pin, process::Stdio, sync::Arc};

use async_process::{ChildStderr, ChildStdin, ChildStdout, Command};
use futures_util::{
    AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncWriteExt, StreamExt, io::BufReader,
};
use snafu::{ResultExt, Snafu};

use crate::{
    actor::{Actor, Control, Receive, SecretAddress, bg_job::Job},
    deferred_info_span, deferred_span,
    deferred_span::DeferredSpan,
    kill_switch::Bomb,
};

#[derive(Debug, Clone, Snafu)]
pub enum Error {
    #[snafu(display("Process failed to start"))]
    Start {
        // TODO: is it better to wrap the whole error in an arc instead of only the source?
        #[snafu(source(from(std::io::Error, Arc::new)))]
        source: Arc<std::io::Error>,
    },
}

// TODO: add ways to modify the environment?
// TODO: when and how to kill the process? Start with sigterm and escalate?
pub struct Process<I, O, E> {
    exe: String,
    args: Vec<String>,
    input: I,
    output: Option<O>,
    errput: Option<E>,
    result: Result<(), Error>,
    bomb: Bomb,
}

impl Process<DevNull, LogLineUtf8, LogLineUtf8> {
    pub fn new(exe: impl Into<String>) -> Self {
        Self {
            exe: exe.into(),
            args: vec![],
            input: DevNull,
            output: Some(LogLineUtf8),
            errput: Some(LogLineUtf8),
            result: Ok(()),
            bomb: Bomb::new(),
        }
    }
}

trait Input {
    fn configure(&self) -> Stdio;
    fn register(&mut self, stdin: &mut Option<ChildStdin>);
    async fn write(&mut self, data: &[u8]) -> io::Result<()>;
}

pub struct Piped {
    stdin: Option<ChildStdin>,
}
impl Input for Piped {
    fn configure(&self) -> Stdio {
        Stdio::piped()
    }

    fn register(&mut self, stdin: &mut Option<ChildStdin>) {
        self.stdin = stdin.take();
        assert!(self.stdin.is_some());
    }

    async fn write(&mut self, data: &[u8]) -> io::Result<()> {
        let stdin = self.stdin.as_mut().unwrap();
        stdin.write_all(data).await?;
        // NOTE: I don't think this does anything for a stdin handle, at least not in the stdlib,
        // but it doesn't hurt to have it here.
        stdin.flush().await
    }
}

pub struct DevNull;
impl Input for DevNull {
    fn configure(&self) -> Stdio {
        Stdio::null()
    }

    fn register(&mut self, stdin: &mut Option<ChildStdin>) {
        assert!(stdin.is_none());
    }

    async fn write(&mut self, _data: &[u8]) -> io::Result<()> {
        panic!("I should not be able to be called");
    }
}
impl<S> Output<S> for DevNull {
    fn configure(&self) -> Stdio {
        Stdio::null()
    }

    async fn process(&self, sink: Option<S>) {
        assert!(sink.is_none());
    }
}

trait Output<S> {
    fn configure(&self) -> Stdio;
    async fn process(&self, sink: Option<S>);
}

pub struct LineUtf8 {
    adr: SecretAddress<OutputLine>,
}
impl<S> Output<S> for LineUtf8 {
    fn configure(&self) -> Stdio {
        Stdio::piped()
    }

    async fn process(&self, sink: Option<S>) {
        todo!()
    }
}

pub struct AllBytes {
    adr: SecretAddress<OutputBytes>,
}
impl<S> Output<S> for AllBytes {
    fn configure(&self) -> Stdio {
        Stdio::piped()
    }

    async fn process(&self, sink: Option<S>) {
        todo!()
    }
}

pub struct LogLineUtf8;
impl LogLineUtf8 {
    async fn print<R: AsyncRead>(&self, name: &'static str, read: R) {
        // NOTE: I know it's an anti-pattern to wrap a generic AsyncRead in a buffer, but in this
        // case, the readable will only be either child stdout or stderr, and this function takes
        // ownership of them, so it's okay in this case.
        let read = BufReader::new(read).lines();
        read.for_each(async |line| match line {
            Ok(line) => tracing::info!("{name}: {line}"),
            Err(err) => tracing::error!(error = &err as &dyn std::error::Error, "{name}"),
        })
        .await;
    }
}

impl<S: AsyncRead> Output<S> for LogLineUtf8 {
    fn configure(&self) -> Stdio {
        Stdio::piped()
    }

    async fn process(&self, sink: Option<S>) {
        // TODO: what should the first argument be set to?
        self.print("stderr", sink.expect("should be set")).await
    }
}

pub enum Std {
    Out,
    Err,
}

pub struct OutputLine {
    source: Std,
    line: String,
}

pub struct OutputBytes {
    source: Std,
    data: Vec<u8>,
}

pub struct InputLine {
    line: String,
}

pub struct InputBytes {
    data: Vec<u8>,
}

// TODO: interrupt should send sigterm
// TODO: kill on drop?
impl<I, O, E> Actor for Process<I, O, E>
where
    I: Input + 'static,
    O: Output<ChildStdout> + 'static,
    E: Output<ChildStderr> + 'static,
{
    type Error = Error;

    // TODO: move to the correct place
    // fn span(&self) -> DeferredSpan<'_> {
    //     crate::deferred_span_or!(
    //         crate::deferred_direct_span!(tracing::Level::DEBUG, "process", exe = self.exe, args = ?self.args);
    //         crate::deferred_direct_span!(tracing::Level::INFO, "process", exe = self.exe);
    //     )
    // }

    // TODO: should this actor run the async_process::driver? The stage?
    async fn enter(&mut self, ctl: &mut Control<Self>) -> Result<(), Self::Error> {
        let output = self.output.take().expect("will be here");
        let errput = self.errput.take().expect("will be here");

        tracing::debug!("Spawning process");
        let mut cmd = Command::new(&self.exe);
        cmd.args(&self.args);
        cmd.stdin(self.input.configure());
        cmd.stdout(output.configure());
        cmd.stderr(errput.configure());

        let mut child = match cmd.spawn() {
            Ok(child) => {
                tracing::debug!(pid = child.id(), "Spawned process");
                child
            }
            Err(err) => {
                tracing::error!(
                    error = &err as &dyn std::error::Error,
                    "Could not spawn process"
                );
                self.result = Err(err).context(StartSnafu);
                ctl.close_and_clear_mailbox();
                return Ok(());
            }
        };

        self.input.register(&mut child.stdin);

        Job::new({
            let stdout = child.stdout.take();
            let stderr = child.stderr.take();
            let bomb = self.bomb.clone();
            async move {
                let out = output.process(stdout);
                let err = errput.process(stderr);
                let res = bomb
                    .attach_future(futures_util::future::join(out, err))
                    .await;
                tracing::debug!(?res);
            }
        })
        .instrument(deferred_info_span!("outputs"))
        .start(ctl);

        // ctl.start_job(
        //     async move |bomb| {
        //         let mut status = pin!(child.status());
        //         // TODO: kill if heart is activated?
        //         let res = bomb.attach_future(&mut status).await;
        //         status.await;
        //         tracing::debug!(?res, "Process died"); // TODO: log with error on error?
        //     },
        //     |parent| tracing::info_span!(parent: parent, "wait_job"),
        // );

        Ok(())
    }

    async fn leave(self, _ctl: &mut Control<Self>) -> Result<(), Self::Error> {
        self.result
    }
}

impl<O, E> Receive<InputLine> for Process<Piped, O, E>
where
    O: Output<ChildStdout> + 'static,
    E: Output<ChildStderr> + 'static,
{
    type Retval = ();

    async fn receive(&mut self, msg: InputLine, _ctl: &mut Control<Self>) -> Self::Retval {
        // TODO: if this hangs it won't be possible to terminate the process? It will most likely be
        // fine since since a C-c from a terminal will send sigint to the process as well, but I
        // don't like that an actor can deadlock itself.
        let msg = msg.line + "\n";
        if let Err(err) = self.input.write(msg.as_bytes()).await {
            // TODO: i think this pretty much only can error if the process has died, so close the
            // actor early.
            tracing::error!(
                error = &err as &dyn std::error::Error,
                "Failed to write to process"
            );
        }
    }
}
