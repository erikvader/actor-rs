use crate::{
    actor::{Actor, NoError, Receive},
    self_hatched,
};

pub struct Logger;

impl Actor for Logger {
    type Corpse = NoError;
}
self_hatched!(Logger, "logger");

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
