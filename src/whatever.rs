use std::{
    error::Error,
    process::{ExitCode, Termination},
};

pub use snafu::prelude::*;

/// The same as the standard Whetever type from SNAFU, except that other location info other than
/// the backtrace is captured.
#[derive(Debug, Snafu)]
#[snafu(whatever)]
// NOTE: this is not at all the prettiest formatting, but it works
// TODO: an alternative could be to create a custom snafu::Report that maybe only prints the
// innermost span trace at the bottom, like how the backtrace works in the default snafu::Whatever
// type.
#[snafu(display("{message}\n-> {location}\n{span_trace}"))]
pub struct Whatever {
    #[snafu(source(from(Box<dyn Error + Send + Sync>, Some)))]
    source: Option<Box<dyn Error + Send + Sync>>,
    message: String,
    #[snafu(implicit)]
    location: snafu::Location,
    #[snafu(implicit)]
    span_trace: SpanTrace,
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
                let snafu_report = snafu::Report::from_error(whatever);
                if tracing::event_enabled!(tracing::Level::ERROR) {
                    tracing::error!("Error: {}", snafu_report);
                } else {
                    eprintln!("Error: {}", snafu_report);
                }
                ExitCode::FAILURE
            }
        }
    }
}
