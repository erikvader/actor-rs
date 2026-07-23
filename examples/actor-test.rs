use actor_rs::{
    actor::{Actor, Control, Receive, Stage},
    graceful_termination::GracefulTermination,
    whatever::*,
};
use tracing::info;

fn setup_tracing() {
    use anstream::ColorChoice;
    use tracing::level_filters::LevelFilter;
    use tracing_error::ErrorLayer;
    use tracing_subscriber::filter::Targets;
    use tracing_subscriber::prelude::*;

    let use_ansi = matches!(
        anstream::AutoStream::choice(&std::io::stdout()),
        ColorChoice::AlwaysAnsi | ColorChoice::Always
    );
    let fmt = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stdout)
        .with_ansi(use_ansi)
        .pretty();

    let filter = Targets::new().with_default(LevelFilter::TRACE);

    tracing_subscriber::registry()
        .with(fmt)
        .with(ErrorLayer::default())
        .with(filter)
        .init();

    let prev_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |arg| {
        let payload = arg.payload_as_str().unwrap_or("<some non-string payload>");
        let location = arg
            .location()
            .map(|l| l.to_string())
            .unwrap_or_else(|| "no location".to_string());
        tracing::error!(target: "panic", location, "{payload}");
        prev_hook(arg);
    }));
}

struct Alice;
impl Actor for Alice {
    async fn enter(&mut self, ctl: &mut Control<Self>) {
        let bob = ctl.summon(Bob);
        let reply = bob.send_receive(Hej(5)).await.unwrap();
        let reply = reply.await.unwrap();
        info!("I got {reply}");
    }
}

struct Bob;
impl Actor for Bob {}

struct Hej(i32);

impl Receive<Hej> for Bob {
    type Retval = i32;

    async fn receive(&mut self, msg: Hej, _ctl: &mut Control<Self>) -> Self::Retval {
        msg.0 * msg.0
    }
}

#[snafu::report]
fn main() -> Result<(), Whatever> {
    setup_tracing();
    let grace = GracefulTermination::new().expect("this should just work");
    let stage = Stage::new(grace.inactive_signal_stream());
    stage.summon(Alice);
    stage.play();
    Ok(())
}
