use crate::{
    actor::{Actor, Control, Hatchable, MultiError},
    whatever::Whatever,
};

pub struct Egg<F> {
    func: F,
}

impl<F> Hatchable for Egg<F>
where
    F: AsyncFnOnce(&mut Control<AdHoc>) -> Result<(), Whatever> + 'static,
{
    type Actor = AdHoc;

    fn span(&self) -> crate::deferred_span::DeferredSpan<'_> {
        crate::deferred_info_span!("adhoc", func = crate::utils::type_name::<F>())
    }

    async fn hatch(
        self,
        ctl: &mut Control<Self::Actor>,
    ) -> Result<Self::Actor, crate::actor_error!(Self::Actor)> {
        (self.func)(ctl).await?;
        Ok(AdHoc)
    }
}

pub struct AdHoc;

impl AdHoc {
    pub fn egg<F>(func: F) -> Egg<F>
    where
        // NOTE: this is not strictly needed here, but it greatly helps in giving good compiler
        // messages
        F: AsyncFnOnce(&mut Control<AdHoc>) -> Result<(), Whatever> + 'static,
    {
        Egg { func }
    }
}

impl Actor for AdHoc {
    type Corpse = MultiError<Whatever>;
}
