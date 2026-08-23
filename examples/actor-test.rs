use actor_rs::{
    actor::{Actor, Control, IntoActorExt, Receive, Stage},
    actors,
    signals::Signals,
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
    // TODO: setup with systemd if started from a service
    let fmt = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stdout)
        .with_ansi(use_ansi);

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

        let current = std::thread::current();
        let rust_id = current.id();
        let name = current
            .name()
            .map_or_else(|| "unnamed".to_string(), |s| s.to_string());

        tracing::error!(target: "panic", location, name, ?rust_id, "{payload}");

        // NOTE: the panic will be printed twice like this, but it's not really possible to ignore
        // the whole chain of hooks, since there can be important stuff in there that needs to be
        // executed.
        prev_hook(arg);
    }));
}

fn main() -> actor_rs::signals::SigTerminate<actor_rs::whatever::Report> {
    setup_tracing();
    let _span = actor_rs::utils::thread_info_span().entered();
    let signals = Signals::new().expect("Couldn't create signal handler");
    let res = inner_main(&signals);
    signals.terminate(res.into())
}

fn inner_main(signals: &Signals) -> Result<(), Whatever> {
    let mut stage = Stage::new();
    stage.register_signals(signals);

    {
        let printer_adr = stage.summon(actors::logger::Logger);
        stage.summon(actors::line_reader::stdin(printer_adr.secret()).interruptable(signals));
    }

    stage.play().whatever_context("Stage failed")
}
