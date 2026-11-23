use crate::{
    actor::{Actor, NoError, Receive},
    self_hatched,
};

pub struct Logger;

impl Actor for Logger {
    type Corpse = NoError;
    type Aux = ();
}
self_hatched!(Logger, "logger");

impl<T: std::fmt::Debug> Receive<T> for Logger {
    type Retval = ();

    async fn receive(&mut self, msg: T, _ctl: &mut crate::actor::Control<Self>) -> Self::Retval {
        tracing::info!("{msg:?}");
    }
}
