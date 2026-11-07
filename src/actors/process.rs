use std::{
    ffi::OsStr,
    io,
    marker::PhantomData,
    process::{ExitStatus, Stdio},
    sync::{Arc, OnceLock},
};

use async_process::{ChildStderr, ChildStdin, ChildStdout, Command};
use futures_util::{AsyncBufReadExt, AsyncRead, AsyncWriteExt, StreamExt, io::BufReader};
use snafu::{ResultExt, Snafu};

use crate::{
    actor::{Actor, Control, Hatchable, OneError, PreControl, Receive, SecretAddress, bg_job::Job},
    actor_error, deferred_info_span,
};

type Pid = u32;

#[derive(Debug, Clone, Snafu)]
pub enum ProcessError {
    #[snafu(display("Process failed to spawn"))]
    Spawn {
        #[snafu(source(from(std::io::Error, Arc::new)))]
        source: Arc<std::io::Error>,
    },
    #[snafu(display("The process died with a non-zero exit status: {status}"))]
    NonZero { status: ExitStatus },
    #[snafu(display("Failed to wait for process termination"))]
    Exit {
        #[snafu(source(from(std::io::Error, Arc::new)))]
        source: Arc<std::io::Error>,
    },
}

#[derive(Clone, Copy, Debug)]
pub enum Std {
    Out,
    Err,
}

trait ChildStream: AsyncRead + 'static {
    fn kind() -> Std;
}

impl ChildStream for ChildStdout {
    fn kind() -> Std {
        Std::Out
    }
}

impl ChildStream for ChildStderr {
    fn kind() -> Std {
        Std::Err
    }
}

// TODO: impl debug but skip the argument in SendLineUtf8? Or make SecretAddress also Debug?
#[derive(Default)]
pub enum OutputConfig {
    DevNull,
    Inherit,
    #[default]
    // TODO: handle non-utf8 strings as well?
    LogLineUtf8,
    SendLineUtf8(SecretAddress<OutputLine>),
}

impl OutputConfig {
    fn start_job<A: Actor, S: ChildStream>(self, ctl: &mut PreControl<'_, A>, stream: Option<S>) {
        async fn log_the_lines(stream: impl ChildStream) {
            let read = BufReader::new(stream).lines();
            read.for_each(async |line| match line {
                Ok(line) => tracing::info!("{line}"),
                Err(err) => {
                    tracing::debug!(error = &err as &dyn std::error::Error);
                    if err.kind() == io::ErrorKind::InvalidData {
                        tracing::error!("Read a line that didn't contain valid UTF-8");
                    }
                }
            })
            .await;
        }

        async fn send_the_lines(
            stream: impl ChildStream,
            adr: SecretAddress<OutputLine>,
            kind: Std,
        ) {
            let read = BufReader::new(stream).lines();
            read.for_each(async |line| match line {
                Ok(line) => {
                    tracing::trace!("Read: '{line}'");
                    if let Err(err) = adr.send(OutputLine { source: kind, line }).await {
                        // NOTE: should not exit early here, since the process could be doing
                        // something, and it should not be killed from a broken pipe.
                        tracing::debug!(error = &err as &dyn std::error::Error, "Receiver dead?");
                    }
                }
                Err(err) => {
                    tracing::debug!(error = &err as &dyn std::error::Error);
                    if err.kind() == io::ErrorKind::InvalidData {
                        tracing::error!("Read a line that didn't contain valid UTF-8");
                    }
                }
            })
            .await;
        }

        let kind = S::kind();
        match self {
            OutputConfig::DevNull | OutputConfig::Inherit => assert!(stream.is_none()),
            OutputConfig::LogLineUtf8 => {
                Job::new(log_the_lines(stream.expect("should be present")))
                    .instrument(deferred_info_span!("log", std = ?kind))
                    .pre_start(ctl);
            }
            OutputConfig::SendLineUtf8(adr) => {
                Job::new(send_the_lines(
                    stream.expect("should be present"),
                    adr,
                    kind,
                ))
                .instrument(deferred_info_span!("send", std = ?kind))
                .pre_start(ctl);
            }
        }
    }

    fn to_stdio(&self) -> Stdio {
        match self {
            OutputConfig::DevNull => Stdio::null(),
            OutputConfig::LogLineUtf8 => Stdio::piped(),
            OutputConfig::Inherit => Stdio::inherit(),
            OutputConfig::SendLineUtf8(_) => Stdio::piped(),
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
// TODO: add ways to change the current dir of the spawned process
// TODO: add a config to spawn the process in its own group? The kill could be sent to the entire
// group in that case. Just a thought, not even sure it's a good idea. The stdlib has nightly API
// for this
// TODO: close the stdin handle if EOF is reached. Simply drop the ChildStdin.
pub struct ProcessEgg<I: InputType> {
    cmd: Command,
    // NOTE: this function pointer is used to make sure it isn't affecting any auto traits of this
    // struct. The return value variant is used to have the same variance as simply I, but it
    // doesn't matter in this case, i'm pretty sure, since no lifetimes are used.
    _phantom: PhantomData<fn() -> I>,
    kill_on_drop: bool,
    stdout_config: OutputConfig,
    stderr_config: OutputConfig,
}

impl ProcessEgg<Unpiped> {
    pub fn new(exe: impl AsRef<OsStr>) -> Self {
        let mut cmd = Command::new(exe);
        cmd.stdin(InputConfig::DevNull);

        let stdout_config = OutputConfig::default();
        let stderr_config = OutputConfig::default();
        cmd.stdout(stdout_config.to_stdio());
        cmd.stderr(stderr_config.to_stdio());

        Self {
            _phantom: PhantomData,
            kill_on_drop: true,
            cmd,
            stdout_config,
            stderr_config,
        }
    }

    pub fn stdin_piped(mut self) -> ProcessEgg<Piped> {
        self.cmd.stdin(InputConfig::Piped);

        ProcessEgg {
            cmd: self.cmd,
            kill_on_drop: self.kill_on_drop,
            stdout_config: self.stdout_config,
            stderr_config: self.stderr_config,
            _phantom: PhantomData,
        }
    }

    pub fn stdin_inherit(mut self) -> Self {
        self.cmd.stdin(InputConfig::Inherit);
        self
    }
}

impl<I: InputType> ProcessEgg<I> {
    pub fn arg(mut self, arg: impl AsRef<OsStr>) -> Self {
        self.cmd.arg(arg);
        self
    }

    pub fn args(mut self, args: impl IntoIterator<Item = impl AsRef<OsStr>>) -> Self {
        self.cmd.args(args);
        self
    }

    pub fn stdout(mut self, output: OutputConfig) -> Self {
        self.cmd.stdout(output.to_stdio());
        self.stdout_config = output;
        self
    }

    pub fn stderr(mut self, errput: OutputConfig) -> Self {
        self.cmd.stderr(errput.to_stdio());
        self.stderr_config = errput;
        self
    }

    pub fn kill_on_drop(mut self, doit: bool) -> Self {
        self.kill_on_drop = doit;
        self
    }
}

impl<I> Hatchable for ProcessEgg<I>
where
    I: InputType,
{
    type Actor = Process<I>;

    crate::default_span!("process");

    async fn hatch(
        self,
        ctl: &mut PreControl<'_, Self::Actor>,
    ) -> Result<Self::Actor, actor_error!(Self::Actor)> {
        let mut child = {
            let mut cmd = self.cmd;
            tracing::debug!(
                exe = ?cmd.get_program(),
                args = ?cmd.get_args().collect::<Vec<_>>(),
                "Spawning process"
            );
            match cmd.spawn() {
                Ok(child) => child,
                Err(err) => {
                    tracing::debug!(
                        error = &err as &dyn std::error::Error,
                        "Could not spawn process"
                    );
                    // TODO: the exe (and args?) should be added here probably. And in all other
                    // variants as well?
                    return Err(err).context(SpawnSnafu);
                }
            }
        };

        let kill_guard = {
            // TODO: record this on the actor span?
            let pid: Pid = child.id();
            assert!(pid > 0, "a valid pid is 1 or greater, at least on linux");
            tracing::debug!(pid, "Spawn success");

            let res = ctl.aux_ref().set(pid);
            assert!(res.is_ok(), "this is only initialized here");

            let mut g = SoftKillGuard::new(pid);
            if !self.kill_on_drop {
                g.defuse();
            }
            g
        };

        let input = I::create(&mut child.stdin);
        self.stdout_config.start_job(ctl, child.stdout.take());
        self.stderr_config.start_job(ctl, child.stderr.take());

        Job::new(async move {
            debug_assert!(
                child.stdin.is_none(),
                "child::status drops stdin, but it should already have been taken at this point"
            );
            let status = child.status().await;
            tracing::trace!(?status, "Process died");
            status
        })
        .then(
            async |me, ctl: &mut Control<Process<_>>, status: Result<ExitStatus, _>| {
                me.kill_guard.defuse();
                match status {
                    Ok(status) if status.success() => tracing::debug!("{status}"),
                    Ok(status) => {
                        tracing::debug!("{status}");
                        ctl.corpse_mut()
                            .replace(NonZeroSnafu { status }.build())
                            .expect("the error is only set here");
                    }
                    // RANT: i don't understand why this error case is here... It seems like, on linux
                    // at least, that rust already handles sporadic wakeups of the syscall, and the
                    // other error cases are like if the pid is invalid, which i trust the standard
                    // library to not mess up. So it feels like this is just extra complexity, but im
                    // handling it just in case.
                    Err(err) => {
                        ctl.corpse_mut()
                            .replace_result(Err(err).context(ExitSnafu))
                            .expect("the error is only set here");
                    }
                }
            },
        )
        .instrument(deferred_info_span!("waiter"))
        .pre_start(ctl);

        Ok(Process { input, kill_guard })
    }
}

// TODO: should this actor run the async_process::driver? The stage? Let it handle itself? It will
// spawn its own thread, im pretty sure, unless something is polling the driver. But i feel it's
// more complicated to handle it myself, and i don't know how well it handles having multiple such
// futures and them getting dropped etc. It also doesn't feel like it is worth the effort to worry
// about this.
pub struct Process<I: InputType> {
    input: I,
    kill_guard: SoftKillGuard,
}

impl<I> Actor for Process<I>
where
    I: InputType,
{
    type Corpse = OneError<ProcessError>;
    type Aux = Arc<OnceLock<Pid>>;
}

#[derive(Debug)]
pub struct InputLine(pub String);
#[derive(Debug)]
pub struct OutputLine {
    pub source: Std,
    pub line: String,
}

impl Receive<InputLine> for Process<Piped> {
    type Retval = ();

    async fn receive(&mut self, msg: InputLine, _ctl: &mut Control<Self>) -> Self::Retval {
        // NOTE: this write can hang, which isn't good, but the termination message will send a
        // sigterm to the process, which will kill it and unblock the actor loop.
        let InputLine(msg) = msg;
        let msg = msg + "\n";
        if let Err(err) = self.input.handle.write(msg.as_bytes()).await {
            // NOTE: i think this pretty much only can error if the process has died
            tracing::debug!(
                error = &err as &dyn std::error::Error,
                "Failed to write to process"
            );
        }
    }
}

impl<I: InputType> Receive<crate::signals::Interrupt> for Process<I> {
    type Retval = ();

    #[tracing::instrument(skip_all)]
    async fn receive(
        &mut self,
        _msg: crate::signals::Interrupt,
        _ctl: &mut Control<Self>,
    ) -> Self::Retval {
        // TODO: don't escale kill signals automatically, instead, make different messages and let
        // the user decide. Maybe a combined messages can escalates after some timeout. Or maybe
        // configure this in the egg and only have this interrupt message. This is mostly do deal
        // with non-cooperative processes.
        self.kill_guard.detonate();
    }

    #[tracing::instrument(skip_all)]
    fn pre_send(aux: &Self::Aux) {
        // NOTE: this will make sure a blocked process actor still can be killed, even if the actor
        // loop is blocked.
        if let Some(pid) = aux.get() {
            SoftKillGuard::new(*pid).detonate();
        }
    }
}

#[cfg(unix)]
fn soft_kill(pid: Pid) -> io::Result<()> {
    let pid: libc::pid_t = pid
        .try_into()
        .expect("this will never fail on unix, the pid can't be too large");
    debug_assert!(
        pid >= 0,
        "the pid can't be negative cuz it came from an unsigned type"
    );
    if pid == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "The pid={pid} must be 1 or greater, \
                 else it will target process groups instead"
            ),
        ));
    }

    tracing::debug!("Sending SIGTERM to {pid}");

    // SAFETY: this isn't really unsafe, it's only marked cuz it's an external C function
    if unsafe { libc::kill(pid, libc::SIGTERM) } != 0 {
        return Err(io::Error::last_os_error());
    }

    Ok(())
}

// NOTE: i'm not using the kill_on_drop on command directly, since it sends sigkill by default
#[derive(Debug)]
struct SoftKillGuard {
    pid: Pid,
    defused: bool,
}

impl SoftKillGuard {
    fn new(pid: Pid) -> Self {
        Self {
            pid,
            defused: false,
        }
    }

    fn defuse(&mut self) {
        self.defused = true;
    }

    #[tracing::instrument(skip_all)]
    fn detonate(&mut self) {
        if self.defused {
            return;
        }

        self.defuse();
        if let Err(e) = soft_kill(self.pid) {
            tracing::debug!(
                error = &e as &dyn std::error::Error,
                "Failed to kill process, probably already dead"
            )
        }
    }
}

impl Drop for SoftKillGuard {
    #[tracing::instrument(skip_all)]
    fn drop(&mut self) {
        self.detonate();
    }
}
