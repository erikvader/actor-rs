use std::convert::Infallible;

use crate::actor::{Actor, Receive};

pub struct Logger;

impl Actor for Logger {
    type Job = Infallible;
    type Error = Infallible;

    fn span(&self, parent: &tracing::Span) -> tracing::Span {
        tracing::info_span!(parent: parent, "logger")
    }
}

impl<T: std::fmt::Debug> Receive<T> for Logger {
    type Retval = ();

    async fn receive(&mut self, msg: T, _ctl: &mut crate::actor::Control<Self>) -> Self::Retval {
        tracing::info!("{msg:?}");
    }
}

#[repr(transparent)]
pub struct Display<T: std::fmt::Display>(T);

impl<T> std::fmt::Debug for Display<T>
where
    T: std::fmt::Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
