use actor_rs::{
    actor::{Actor, Control, Receive, Stage},
    graceful_termination::GracefulTermination,
    whatever::*,
};
use tracing::info;

fn setup_tracing() {
    use tracing::level_filters::LevelFilter;
    use tracing_error::ErrorLayer;
    use tracing_subscriber::filter::Targets;
    use tracing_subscriber::prelude::*;

    // TODO: detect ansi color support like anstream::auto() and set with with_ansi
    // https://docs.rs/anstream/latest/anstream/struct.AutoStream.html#method.choice
    // I guess this should look at the global value and let clap or something set that global when
    // appropriate flags or config are given
    let fmt = tracing_subscriber::fmt::layer().pretty();

    let filter = Targets::new().with_default(LevelFilter::TRACE);

    tracing_subscriber::registry()
        .with(fmt)
        .with(ErrorLayer::default())
        .with(filter)
        .init();
    // TODO: tracing-panic?
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
