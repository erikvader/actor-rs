use actor_rs::{
    actor::{SpawnableExt, Stage},
    actors,
    rootcause::Report,
    signals::Signals,
};
use rootcause::compat::MainReport;

fn supports_colors() -> bool {
    use anstream::ColorChoice;
    matches!(
        anstream::AutoStream::choice(&std::io::stderr()),
        ColorChoice::AlwaysAnsi | ColorChoice::Always
    )
}

fn setup_tracing(use_ansi: bool) {
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

    use tracing::level_filters::LevelFilter;
    use tracing_subscriber::filter::Targets;
    use tracing_subscriber::prelude::*;

    let global_filter = Targets::new().with_default(LevelFilter::TRACE);
    let fmt_filter = LevelFilter::TRACE;

    // TODO: setup with systemd if started from a service
    let fmt = tracing_subscriber::fmt::layer()
        .without_time()
        .with_writer(std::io::stderr)
        .with_ansi(use_ansi)
        .with_filter(fmt_filter);

    tracing_subscriber::registry()
        .with(fmt)
        .with(rootcause_tracing::RootcauseLayer)
        // NOTE: this is last to filter out spans as early as possible. There is first a filtering
        // step that goes outside in (each .with wraps what they're are called on), as soon as a
        // layer says interest::never it short circuits and the span is disabled. Then, the second
        // step is when events are processed, and that is inside out, because the events needs to be
        // registered in the registry first. So that needs to be taken into account if layers depend
        // on each other.
        .with(global_filter)
        .init();
}

#[derive(Clone, Copy, PartialOrd, Ord, PartialEq, Eq)]
#[expect(dead_code)]
enum DebugLevel {
    Nothing,
    ALittle,
    ALot,
}

fn setup_rootcause(fancy: bool, debug_level: DebugLevel) {
    use actor_rs::rootcause::hooks::Hooks;
    use actor_rs::rootcause::hooks::builtin_hooks::report_formatter::DefaultReportFormatter;
    use actor_rs::rootcause::hooks::report_creation::ReportCreationHook;
    use rootcause::handlers::AttachmentHandler;
    use rootcause::hooks::context_formatter::ContextFormatterHook;

    // NOTE: inspired by how rootcause_tracing handles its spans
    #[derive(Debug)]
    struct TypeNameAttachment(&'static str);
    struct TypeNameCollector;
    impl ReportCreationHook for TypeNameCollector {
        fn on_local_creation(
            &self,
            report: rootcause::ReportMut<
                '_,
                rootcause::prelude::markers::Dynamic,
                rootcause::prelude::markers::Local,
            >,
        ) {
            let type_name = report.current_context_type_name();
            let _ = report.attach_custom::<TypeNameHandler, _>(TypeNameAttachment(type_name));
        }

        fn on_sendsync_creation(
            &self,
            report: rootcause::ReportMut<
                '_,
                rootcause::prelude::markers::Dynamic,
                rootcause::prelude::markers::SendSync,
            >,
        ) {
            let type_name = report.current_context_type_name();
            let _ = report.attach_custom::<TypeNameHandler, _>(TypeNameAttachment(type_name));
        }
    }
    struct TypeNameHandler;
    impl AttachmentHandler<TypeNameAttachment> for TypeNameHandler {
        fn display(
            value: &TypeNameAttachment,
            formatter: &mut core::fmt::Formatter<'_>,
        ) -> core::fmt::Result {
            write!(formatter, "{}", value.0)
        }

        fn debug(
            value: &TypeNameAttachment,
            formatter: &mut core::fmt::Formatter<'_>,
        ) -> core::fmt::Result {
            write!(formatter, "{:?}", value)
        }

        fn preferred_formatting_style(
            _value: &TypeNameAttachment,
            report_formatting_function: rootcause::prelude::handlers::FormattingFunction,
        ) -> rootcause::prelude::handlers::AttachmentFormattingStyle {
            rootcause::prelude::handlers::AttachmentFormattingStyle {
                function: report_formatting_function,
                priority: 19, // NOTE: below the Location (20)
                placement: rootcause::handlers::AttachmentFormattingPlacement::Inline,
            }
        }
    }

    struct DebugFmtCollector;
    impl ReportCreationHook for DebugFmtCollector {
        fn on_local_creation(
            &self,
            mut report: rootcause::ReportMut<
                '_,
                rootcause::prelude::markers::Dynamic,
                rootcause::prelude::markers::Local,
            >,
        ) {
            let att = rootcause::report_attachment!("Debug: {:?}", report.format_current_context())
                .into_local();
            report.attachments_mut().push(att);
        }

        fn on_sendsync_creation(
            &self,
            mut report: rootcause::ReportMut<
                '_,
                rootcause::prelude::markers::Dynamic,
                rootcause::prelude::markers::SendSync,
            >,
        ) {
            let att = rootcause::report_attachment!("Debug: {:?}", report.format_current_context());
            report.attachments_mut().push(att);
        }
    }

    struct IoErrorFormatter;
    impl ContextFormatterHook<std::io::Error> for IoErrorFormatter {
        fn display(
            &self,
            report: rootcause::ReportRef<
                '_,
                std::io::Error,
                rootcause::prelude::markers::Uncloneable,
                rootcause::prelude::markers::Local,
            >,
            formatter: &mut std::fmt::Formatter<'_>,
        ) -> std::fmt::Result {
            let err = report.current_context();
            if let Some(inner) = err.get_ref() {
                write!(formatter, "{} ({})", inner, err.kind())
            } else {
                write!(formatter, "{}", err)
            }
        }
    }

    let mut hooks = Hooks::new()
        // NOTE: I suspect there will be a lot of redundant info if every report in the chain
        // collected the current span, so it's probably best to let each report themselves decide
        // whether the current span trace should be attached or not.
        // .report_creation_hook(rootcause_tracing::SpanCollector::new())
        .context_formatter(IoErrorFormatter)
        .report_formatter(if fancy {
            DefaultReportFormatter::UNICODE_COLORS
        } else {
            DefaultReportFormatter::ASCII
        });

    if debug_level >= DebugLevel::ALittle {
        hooks = hooks.report_creation_hook(TypeNameCollector);
    }
    if debug_level >= DebugLevel::ALot {
        hooks = hooks.report_creation_hook(DebugFmtCollector)
    }

    hooks.install().expect("failed to install rootcause hooks");
}

fn main() -> actor_rs::signals::SigTerminate<Result<(), MainReport>> {
    {
        let colors = supports_colors();
        setup_rootcause(colors, DebugLevel::ALittle);
        setup_tracing(colors);
    }

    let _span = actor_rs::utils::thread_info_span();
    let signals = Signals::new().expect("Couldn't create signal handler");
    let res = inner_main(&signals);
    signals.terminate(res.map_err(Into::into))
}

fn inner_main(signals: &Signals) -> Result<(), Report> {
    let mut stage = Stage::new();
    stage.register_signals(signals);
    stage.play_adhoc({
        let signals = signals.registry();
        async |ctl| {
            let printer_adr = ctl
                .pre_summon(actors::logger::Logger.span(actor_rs::deferred_info_span!("logger2")))
                .aggregate();

            ctl.pre_summon(
                actors::stdin::StdinEgg::new(printer_adr.secret()).interruptable(signals),
            )
            .aggregate();

            let pegg = actors::process::ProcessEgg::new("sh")
                .arg("-c")
                .arg("echo hej");
            ctl.pre_summon(pegg).aggregate();

            Ok(())
        }
    })
}
