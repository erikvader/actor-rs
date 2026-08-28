use std::{
    error::Error,
    fmt,
    process::{ExitCode, Termination},
};

pub use snafu::prelude::*;

/// The same as the standard Whatever type from SNAFU, except that other location info other than
/// the backtrace is captured.
#[derive(Debug, Snafu)]
#[snafu(whatever)]
#[snafu(display("{message}"))]
pub struct Whatever {
    #[snafu(source(from(Box<dyn Error + Send + Sync>, Some)))]
    source: Option<Box<dyn Error + Send + Sync>>,
    message: String,
    // RANT: This whole type, and snafu in general, whould be so much more usable if the error
    // provide API was stable... https://github.com/rust-lang/rust/issues/99301. Then the printing
    // function could query each error in the chain if it has a certain kind of information, like
    // the location, and add that if available. The default snafu Report does support this already,
    // but on nightly only. The span trace could also be extracted this way, which would allow other
    // errors types to also capture span traces. Now it's only the innermost Whatever's span trace
    // that is shown, but it could've been the inner most error that provides a span trace, which is
    // much better.
    // #[snafu(implicit)]
    // location: snafu::Location,
    #[snafu(implicit)]
    span_trace: SpanTrace,
    // TODO: add normal backtrace as well? I rarely inspect it anyways.
}

#[derive(Debug)]
struct SpanTrace(tracing_error::SpanTrace);

impl std::fmt::Display for SpanTrace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl snafu::GenerateImplicitData for SpanTrace {
    fn generate() -> Self {
        Self(tracing_error::SpanTrace::capture())
    }
}

impl Whatever {
    fn most_specific_span_trace(&self) -> &tracing_error::SpanTrace {
        use tracing_error::ExtractSpanTrace;

        snafu::ChainCompat::new(self)
            .filter_map(|e| {
                e.downcast_ref::<Whatever>()
                    .map(|what| &what.span_trace.0)
                    .or_else(|| e.span_trace())
            })
            .last()
            .expect("there will always be at least one valid one, i.e., self")
    }
}

struct Formatter<'a> {
    error: &'a Whatever,
}

impl<'a> fmt::Display for Formatter<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let snafu_report = snafu::Report::from_error(self.error);
        write!(f, "{snafu_report}")?;

        let span_trace = self.error.most_specific_span_trace();
        writeln!(f, "\nSpan trace:")?;
        // NOTE: it seems like it's the subscriber that formats the span trace, so if it is
        // configured to use colors, then the span traces also use colors. This also makes it weird
        // to log a span trace, since the subscriber sanitizes all fields by default, so the ansi
        // codes are escaped and printed verbatim, which is not good. It's probably possible to
        // solve somehow. https://github.com/tokio-rs/tracing/issues/3476
        writeln!(f, "{span_trace}")?;

        Ok(())
    }
}

pub struct Report {
    result: Result<(), Whatever>,
}

impl<E> From<Result<(), E>> for Report
where
    E: Into<Whatever>,
{
    fn from(value: Result<(), E>) -> Self {
        Self {
            result: value.map_err(|e| e.into()),
        }
    }
}

impl Termination for Report {
    fn report(self) -> ExitCode {
        match self.result {
            Ok(()) => ExitCode::SUCCESS,
            Err(whatever) => {
                eprintln!("Error: {}", Formatter { error: &whatever });
                ExitCode::FAILURE
            }
        }
    }
}
