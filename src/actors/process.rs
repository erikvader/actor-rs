use std::{io, marker::PhantomData, process::Stdio, sync::Arc};

use async_process::{ChildStderr, ChildStdin, ChildStdout, Command};
use futures_util::{AsyncBufReadExt, AsyncRead, AsyncWriteExt, StreamExt, io::BufReader};
use snafu::{ResultExt, Snafu};

use crate::{
    actor::{Actor, Control, Hatchable, OneError, PreControl, Receive, SecretAddress, bg_job::Job},
    actor_error, deferred_info_span,
    deferred_span::DeferredSpan,
    kill_switch::{Bomb, Tick},
};

#[derive(Debug, Clone, Snafu)]
pub enum Error {
    #[snafu(display("Process failed to start"))]
    Start {
        #[snafu(source(from(std::io::Error, Arc::new)))]
        source: Arc<std::io::Error>,
    },
    #[snafu(display("The process died with a non-zero exit status"))]
    ExitError,
}

trait ChildStream: AsyncRead {
    fn kind() -> &'static str;
}

impl ChildStream for ChildStdout {
    fn kind() -> &'static str {
        "stdout"
    }
}

impl ChildStream for ChildStderr {
    fn kind() -> &'static str {
        "stderr"
    }
}

pub enum OutputConfig {
    DevNull,
    Inherit,
    LogLineUtf8,
    // TODO: SendLineUtf8(Address)
}

impl OutputConfig {
    fn start_job<A: Actor, S: ChildStream + 'static>(
        self,
        ctl: &mut PreControl<'_, A>,
        stream: Option<S>,
        bomb: Bomb,
    ) {
        let kind = S::kind();
        match self {
            OutputConfig::DevNull | OutputConfig::Inherit => assert!(stream.is_none()),
            OutputConfig::LogLineUtf8 => {
                Job::new({
                    let stream = stream.expect("should be present");
                    async move {
                        // TODO: i need to handle these read lines better somehow
                        let read = BufReader::new(stream).lines();
                        let read = bomb.attach_stream(read);
                        read.for_each(async |line| match line {
                            Tick::Tock(Ok(line)) => tracing::info!("{line}"),
                            // TODO: do i really want to print every error? Only the utf-8 decode one is relevant
                            // here?
                            Tick::Tock(Err(err)) => {
                                tracing::error!(error = &err as &dyn std::error::Error)
                            }
                            Tick::Boom => (),
                        })
                        .await;
                    }
                })
                .instrument(deferred_info_span!("stream", kind))
                .pre_start(ctl);
            }
        }
    }

    fn to_stdio(&self) -> Stdio {
        match self {
            OutputConfig::DevNull => Stdio::null(),
            OutputConfig::LogLineUtf8 => Stdio::piped(),
            OutputConfig::Inherit => Stdio::inherit(),
        }
    }
}

impl From<OutputConfig> for Stdio {
    fn from(value: OutputConfig) -> Self {
        value.to_stdio()
    }
}

enum InputConfig {
    DevNull,
    Piped,
    Inherit,
}

impl From<InputConfig> for Stdio {
    fn from(value: InputConfig) -> Self {
        match value {
            InputConfig::DevNull => Stdio::null(),
            InputConfig::Piped => Stdio::piped(),
            InputConfig::Inherit => Stdio::inherit(),
        }
    }
}

pub trait InputType: crate::private::Sealed + 'static {
    fn create(handle: &mut Option<ChildStdin>) -> Self;
}
pub struct Piped {
    handle: ChildStdin,
}
pub struct Unpiped;
impl crate::private::Sealed for Piped {}
impl crate::private::Sealed for Unpiped {}
impl InputType for Piped {
    fn create(handle: &mut Option<ChildStdin>) -> Self {
        Self {
            handle: handle.take().expect("should be here"),
        }
    }
}
impl InputType for Unpiped {
    fn create(handle: &mut Option<ChildStdin>) -> Self {
        assert!(handle.is_none());
        Self
    }
}

// TODO: add ways to modify the environment?
pub struct ProcessEgg<I: InputType> {
    exe: String,
    args: Vec<String>,
    // NOTE: this function pointer is used to make sure I isn't affecting any auto traits of this
    // struct. The return value variant is used to have the same variance as simply I, but it
    // doesn't matter in this case, i'm pretty sure, since no lifetimes are used.
    _phantom: PhantomData<fn() -> I>,
    input_config: InputConfig,
    output_config: OutputConfig,
    errput_config: OutputConfig,
}

impl ProcessEgg<Unpiped> {
    pub fn new(exe: impl Into<String>) -> Self {
        Self {
            exe: exe.into(),
            args: vec![],
            input_config: InputConfig::DevNull,
            output_config: OutputConfig::DevNull,
            errput_config: OutputConfig::DevNull,
            _phantom: PhantomData,
        }
    }

    pub fn stdin_piped(self) -> ProcessEgg<Piped> {
        let Self {
            exe,
            args,
            input_config,
            output_config,
            errput_config,
            _phantom: _,
        } = self;
        debug_assert!(matches!(input_config, InputConfig::DevNull));

        ProcessEgg {
            exe,
            args,
            input_config: InputConfig::Piped,
            output_config,
            errput_config,
            _phantom: PhantomData,
        }
    }

    pub fn stdin_inherit(self) -> Self {
        Self {
            input_config: InputConfig::Inherit,
            ..self
        }
    }
}

impl<I: InputType> ProcessEgg<I> {
    pub fn args(self, args: impl Into<Vec<String>>) -> Self {
        Self {
            args: args.into(),
            ..self
        }
    }

    pub fn stdout(self, output: OutputConfig) -> Self {
        Self {
            output_config: output,
            ..self
        }
    }

    pub fn stderr(self, errput: OutputConfig) -> Self {
        Self {
            errput_config: errput,
            ..self
        }
    }
}

impl<I> Hatchable for ProcessEgg<I>
where
    I: InputType,
{
    type Actor = Process<I>;

    fn span(&self) -> DeferredSpan<'_> {
        // TODO: make args into an option instead? How are those even printed? Do like utils::type_name?
        // TODO: even have all this info here? Is it too long? Just log once when starting? The exe
        // can get long if an absolute path is provided.
        crate::deferred_span_or!(
            crate::deferred_direct_span!(tracing::Level::DEBUG, "process", exe = self.exe, args = ?self.args);
            crate::deferred_direct_span!(tracing::Level::INFO, "process", exe = self.exe);
        )
    }

    async fn hatch(
        self,
        ctl: &mut PreControl<'_, Self::Actor>,
    ) -> Result<Self::Actor, actor_error!(Self::Actor)> {
        tracing::debug!(exe = self.exe, args = ?self.args, "Spawning process");
        let mut cmd = Command::new(&self.exe);
        cmd.args(&self.args);
        cmd.stdin(self.input_config);
        cmd.stdout(self.output_config.to_stdio());
        cmd.stderr(self.errput_config.to_stdio());

        let mut child = match cmd.spawn() {
            Ok(child) => child,
            Err(err) => {
                tracing::debug!(
                    error = &err as &dyn std::error::Error,
                    "Could not spawn process"
                );
                return Err(err).context(StartSnafu);
            }
        };

        let pid = child.id();
        tracing::debug!(pid, "Spawn success");
        let input = I::create(&mut child.stdin);
        let bomb = Bomb::new();

        self.output_config
            .start_job(ctl, child.stdout.take(), bomb.clone());
        self.errput_config
            .start_job(ctl, child.stderr.take(), bomb.clone());

        //         // ctl.start_job(
        //         //     async move |bomb| {
        //         //         let mut status = pin!(child.status());
        //         //         // TODO: kill if heart is activated?
        //         //         let res = bomb.attach_future(&mut status).await;
        //         //         status.await;
        //         //         tracing::debug!(?res, "Process died"); // TODO: log with error on error?
        //         //     },
        //         //     |parent| tracing::info_span!(parent: parent, "wait_job"),
        //         // );

        Ok(Process { input, bomb, pid })
    }
}

// TODO: when and how to kill the process? Start with sigterm and escalate?
pub struct Process<I: InputType> {
    input: I,
    bomb: Bomb,
    pid: u32,
}

impl<I> Actor for Process<I>
where
    I: InputType,
{
    type Corpse = OneError<Error>;
}

impl Receive<InputLine> for Process<Piped> {
    type Retval = ();

    async fn receive(&mut self, msg: InputLine, _ctl: &mut Control<Self>) -> Self::Retval {
        // // TODO: if this hangs it won't be possible to terminate the process? It will most likely be
        // // fine since since a C-c from a terminal will send sigint to the process as well, but I
        // // don't like that an actor can deadlock itself.
        // let msg = msg.line + "\n";
        // if let Err(err) = self.input.write(msg.as_bytes()).await {
        //     // TODO: i think this pretty much only can error if the process has died, so close the
        //     // actor early.
        //     tracing::error!(
        //         error = &err as &dyn std::error::Error,
        //         "Failed to write to process"
        //     );
        // }
    }
}

// trait Input {
//     fn configure(&self) -> Stdio;
//     fn register(&mut self, stdin: &mut Option<ChildStdin>);
//     async fn write(&mut self, data: &[u8]) -> io::Result<()>;
// }

// pub struct Piped {
//     stdin: Option<ChildStdin>,
// }
// impl Input for Piped {
//     fn configure(&self) -> Stdio {
//         Stdio::piped()
//     }

//     fn register(&mut self, stdin: &mut Option<ChildStdin>) {
//         self.stdin = stdin.take();
//         assert!(self.stdin.is_some());
//     }

//     async fn write(&mut self, data: &[u8]) -> io::Result<()> {
//         let stdin = self.stdin.as_mut().unwrap();
//         stdin.write_all(data).await?;
//         // NOTE: I don't think this does anything for a stdin handle, at least not in the stdlib,
//         // but it doesn't hurt to have it here.
//         stdin.flush().await
//     }
// }

// pub struct DevNull;
// impl Input for DevNull {
//     fn configure(&self) -> Stdio {
//         Stdio::null()
//     }

//     fn register(&mut self, stdin: &mut Option<ChildStdin>) {
//         assert!(stdin.is_none());
//     }

//     async fn write(&mut self, _data: &[u8]) -> io::Result<()> {
//         panic!("I should not be able to be called");
//     }
// }
// impl<S> Output<S> for DevNull {
//     fn configure(&self) -> Stdio {
//         Stdio::null()
//     }

//     async fn process(&self, sink: Option<S>) {
//         assert!(sink.is_none());
//     }
// }

// trait Output<S> {
//     fn configure(&self) -> Stdio;
//     async fn process(&self, sink: Option<S>);
// }

// pub struct LineUtf8 {
//     adr: SecretAddress<OutputLine>,
// }
// impl<S> Output<S> for LineUtf8 {
//     fn configure(&self) -> Stdio {
//         Stdio::piped()
//     }

//     async fn process(&self, sink: Option<S>) {
//         todo!()
//     }
// }

// pub struct AllBytes {
//     adr: SecretAddress<OutputBytes>,
// }
// impl<S> Output<S> for AllBytes {
//     fn configure(&self) -> Stdio {
//         Stdio::piped()
//     }

//     async fn process(&self, sink: Option<S>) {
//         todo!()
//     }
// }

// pub struct LogLineUtf8;
// impl LogLineUtf8 {
//     async fn print<R: AsyncRead>(&self, name: &'static str, read: R) {
//         // NOTE: I know it's an anti-pattern to wrap a generic AsyncRead in a buffer, but in this
//         // case, the readable will only be either child stdout or stderr, and this function takes
//         // ownership of them, so it's okay in this case.
//         // TODO: use a custom private trait that is only implemented on stdout and stderr instead of
//         // asyncread.
//         let read = BufReader::new(read).lines();
//         read.for_each(async |line| match line {
//             Ok(line) => tracing::info!("{name}: {line}"),
//             // TODO: do i really want to print every error? Only the utf-8 decode one is relevant
//             // here?
//             Err(err) => tracing::error!(error = &err as &dyn std::error::Error, "{name}"),
//         })
//         .await;
//     }
// }

// impl<S: AsyncRead> Output<S> for LogLineUtf8 {
//     fn configure(&self) -> Stdio {
//         Stdio::piped()
//     }

//     async fn process(&self, sink: Option<S>) {
//         // TODO: what should the first argument be set to?
//         self.print("stderr", sink.expect("should be set")).await
//     }
// }

// pub enum Std {
//     Out,
//     Err,
// }

// pub struct OutputLine {
//     source: Std,
//     line: String,
// }

// pub struct OutputBytes {
//     source: Std,
//     data: Vec<u8>,
// }

pub struct InputLine {
    line: String,
}

// pub struct InputBytes {
//     data: Vec<u8>,
// }

// // TODO: interrupt should send sigterm
// // TODO: kill on drop?
// impl<I> Actor for Process<I>
// where
//     I: Input + 'static,
//     O: Output<ChildStdout> + 'static,
//     E: Output<ChildStderr> + 'static,
// {
//     type Corpse = OneError<Error>;

//     // TODO: use Option instead of deferred_span_or for optional fields? Create a nice helper?
//     // TODO: move to the correct place
//     // fn span(&self) -> DeferredSpan<'_> {
//     //     crate::deferred_span_or!(
//     //         crate::deferred_direct_span!(tracing::Level::DEBUG, "process", exe = self.exe, args = ?self.args);
//     //         crate::deferred_direct_span!(tracing::Level::INFO, "process", exe = self.exe);
//     //     )
//     // }

//     // TODO: should this actor run the async_process::driver? The stage?
//     async fn enter(&mut self, ctl: &mut Control<Self>) -> Result<(), actor_error!(Self)> {
//         let output = self.output.take().expect("will be here");
//         let errput = self.errput.take().expect("will be here");

//         tracing::debug!("Spawning process");
//         let mut cmd = Command::new(&self.exe);
//         cmd.args(&self.args);
//         cmd.stdin(self.input.configure());
//         cmd.stdout(output.configure());
//         cmd.stderr(errput.configure());

//         let mut child = match cmd.spawn() {
//             Ok(child) => {
//                 tracing::debug!(pid = child.id(), "Spawned process");
//                 child
//             }
//             Err(err) => {
//                 tracing::error!(
//                     error = &err as &dyn std::error::Error,
//                     "Could not spawn process"
//                 );
//                 self.result = Err(err).context(StartSnafu);
//                 ctl.close_and_clear_mailbox();
//                 return Ok(());
//             }
//         };

//         self.input.register(&mut child.stdin);

//         Job::new({
//             let stdout = child.stdout.take();
//             let stderr = child.stderr.take();
//             let bomb = self.bomb.clone();
//             async move {
//                 let out = output.process(stdout);
//                 let err = errput.process(stderr);
//                 let res = bomb
//                     .attach_future(futures_util::future::join(out, err))
//                     .await;
//                 tracing::debug!(?res);
//             }
//         })
//         .instrument(deferred_info_span!("outputs"))
//         .start(ctl);

//         // ctl.start_job(
//         //     async move |bomb| {
//         //         let mut status = pin!(child.status());
//         //         // TODO: kill if heart is activated?
//         //         let res = bomb.attach_future(&mut status).await;
//         //         status.await;
//         //         tracing::debug!(?res, "Process died"); // TODO: log with error on error?
//         //     },
//         //     |parent| tracing::info_span!(parent: parent, "wait_job"),
//         // );

//         Ok(())
//     }

//     // TODO:
//     // async fn leave(self, _ctl: &mut Control<Self>) -> Result<(), actor_error!(Self)> {
//     //     self.result
//     // }
// }
