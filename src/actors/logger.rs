use std::convert::Infallible;

use crate::{
    actor::{Actor, Receive},
    deferred_info_span,
    utils::DeferredSpan,
};

pub struct Logger;

impl Actor for Logger {
    type Error = Infallible;

    fn span(&self) -> DeferredSpan<'_> {
        deferred_info_span!("logger")
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
