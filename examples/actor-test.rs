use actor_rs::{
    actor::{IntoActorExt, Stage},
    actors,
    signals::Signals,
    whatever::*,
};

fn setup_tracing() {
    // NOTE: To my future self so i don't have research this again. Whether panics or the error from
    // main should be logged in tracing totally depends on the context this bin is supposed to
    // execute in. Different behaviours are desireable whether it's an CLI app or cloud thingy that
    // sends every log to a central server. Logging panics should probably not be done at all, since
    // there's a lot of edge cases that can occur, like what if the panic hook itself panics while
    // it tries to log and stuff. Whether the top-level error should be printed, logged or both
    // depends on if stderr is captured and easily accessible in the environment/infrastructure the
    // process is run in. There is no universal answer and it all depends. Also, CLI apps should
    // probably not log anything but errors by default. Also, SpanTraces get sanitized by default if
    // they are logged and not just simply printed, so the output contain escaped ansi codes, which
    // is not pretty. It's probably possible to get around this, but it's an argument to not log the
    // top-level error with pretty formatting. https://github.com/tokio-rs/tracing/issues/3476

    use anstream::ColorChoice;
    use tracing::level_filters::LevelFilter;
    use tracing_error::ErrorLayer;
    use tracing_subscriber::filter::Targets;
    use tracing_subscriber::prelude::*;

    let global_filter = Targets::new().with_default(LevelFilter::TRACE);
    let fmt_filter = LevelFilter::TRACE;

    let use_ansi = matches!(
        anstream::AutoStream::choice(&std::io::stdout()),
        ColorChoice::AlwaysAnsi | ColorChoice::Always
    );
    // TODO: setup with systemd if started from a service
    let fmt = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stdout)
        .with_ansi(use_ansi)
        .with_filter(fmt_filter);

    tracing_subscriber::registry()
        .with(fmt)
        .with(ErrorLayer::default())
        .with(global_filter)
        .init();
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

    let stdin_adr = {
        let printer_adr = stage.summon(actors::logger::Logger);
        stage
            .summon(actors::line_reader::stdin(printer_adr.secret()).interruptable(signals))
            .downgrade()
    };

    stage.play().whatever_context("Stage failed")?;
    stdin_adr
        .get_error()
        .map_or(Ok(()), Err)
        .whatever_context("The stdin reader did not exit cleanly")
}
