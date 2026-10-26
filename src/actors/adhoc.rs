use crate::{
    actor::{Actor, Hatchable, MultiError, PreControl},
    whatever::{Group, Whatever},
};

pub struct AdHoc;

impl Actor for AdHoc {
    type Corpse = MultiError<Whatever>;
}

pub struct AdHocEgg<F> {
    func: F,
}

impl<F> AdHocEgg<F>
where
    // NOTE: this is not strictly necessary, but it helps the compiler give better error messages
    F: AsyncFnOnce(&mut PreControl<AdHoc>) -> Result<(), crate::actor_error!(AdHoc)> + 'static,
{
    pub fn new(func: F) -> Self {
        Self { func }
    }
}

impl<F> Hatchable for AdHocEgg<F>
where
    F: AsyncFnOnce(&mut PreControl<AdHoc>) -> Result<(), crate::actor_error!(AdHoc)> + 'static,
{
    type Actor = AdHoc;

    fn span(&self) -> crate::deferred_span::DeferredSpan<'_> {
        crate::deferred_info_span!("adhoc", func = crate::utils::type_name::<F>())
    }

    async fn hatch(
        self,
        ctl: &mut crate::actor::PreControl<'_, Self::Actor>,
    ) -> Result<Self::Actor, crate::actor_error!(Self::Actor)> {
        (self.func)(ctl).await?;
        Ok(AdHoc)
    }
}
