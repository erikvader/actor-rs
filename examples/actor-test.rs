use actor_rs::{
    actor::{Actor, Control, Receive, stage_play},
    whatever::*,
};
use tracing::info;

fn setup_tracing() {
    use tracing::level_filters::LevelFilter;
    use tracing_error::ErrorLayer;
    use tracing_subscriber::filter::Targets;
    use tracing_subscriber::prelude::*;

    // TODO: detect ansi color support like anstream::auto() and set with with_ansi
    let fmt = tracing_subscriber::fmt::layer().pretty();

    let filter = Targets::new().with_default(LevelFilter::TRACE);

    tracing_subscriber::registry()
        .with(fmt)
        .with(ErrorLayer::default())
        .with(filter)
        .init();
}

struct Alice;
impl Actor for Alice {
    const MAIL_BOX_SIZE: usize = 0;
    async fn enter(&mut self, ctl: &mut Control) {
        let bob = ctl.summon(Bob);
        let reply = bob.send(Hej(5)).await.unwrap();
        let reply = reply.await.unwrap();
        println!("I got {reply}");
    }
}

struct Bob;
impl Actor for Bob {
    const MAIL_BOX_SIZE: usize = 0;
}

struct Hej(i32);

impl Receive<Hej> for Bob {
    type Retval = i32;

    async fn receive(&mut self, msg: Hej, _ctl: &mut Control) -> Self::Retval {
        msg.0 * msg.0
    }
}

#[snafu::report]
fn main() -> Result<(), Whatever> {
    setup_tracing();
    stage_play(Alice);
    Ok(())
}
