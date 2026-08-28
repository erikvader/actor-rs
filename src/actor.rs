use crate::{
    actor::{
        id_generator::{Id, IdGenerator},
        unsafe_wrapper::UnsafeSendWrapper,
    },
    deferred_span::DeferredSpan,
    heart::{self, Heart, PanicError, Rune},
    kill_switch::{self, Bomb},
    signals::{ActorGuard, SigRegistry, StageGuard},
    stream_utils::{YieldPolicy, yield_guard},
};
use async_channel as channel;
use async_executor::LocalExecutor;
use async_io::block_on;
use futures_core::future::LocalBoxFuture;
use futures_util::{
    FutureExt as _, StreamExt as _,
    stream::{self, FuturesUnordered},
};
use snafu::prelude::*;
use static_assertions::{assert_impl_all, assert_not_impl_any};
use std::{
    any::type_name,
    convert::Infallible,
    marker::PhantomData,
    num::NonZeroUsize,
    pin::pin,
    rc::{Rc, Weak},
};
use tracing::{Instrument, Level, Span, debug, debug_span, span, trace, trace_span};

// TODO: this module probably needs to be split up into several submodules, but it's super tedious
// and rust-analyzer isn't that big of a help.

// NOTE: Since the output of `type_name` usually is long, i only use it on spans of level trace and
// events of levels debug or higher (verbosity).
// TODO: it would be cool if there was some nice way to shorten those type names

#[derive(Debug, Clone, Copy)]
pub enum MailboxSize {
    Unbounded,
    Bounded(NonZeroUsize),
}

impl MailboxSize {
    pub const fn default() -> Self {
        if cfg!(test) {
            // NOTE: makes testing easier by removing a source of Poll::pending
            Self::Unbounded
        } else {
            Self::Bounded(const { NonZeroUsize::new(16).unwrap() })
        }
    }
}

pub type NoError = Infallible;

#[macro_export]
macro_rules! default_span {
    ($name:expr) => {
        fn span(&self) -> DeferredSpan<'_> {
            $crate::deferred_info_span!($name)
        }
    };
}

// NOTE: this is Sized because that is required when using Self in function arguments
// NOTE: this is 'static because basically every usage of an Actor requires 'static
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not an actor",
    label = "this guy right here",
    note = "implement `Actor`"
)]
pub trait Actor: Sized + 'static {
    // RANT: these, annoyingly, can't have default values
    type Error: std::error::Error + Clone; // = NoError

    fn span(&self) -> DeferredSpan<'_>;

    #[expect(
        async_fn_in_trait,
        reason = "i don't really understand what this is complaining about"
    )]
    #[expect(unused_variables, reason = "the default is a noop")]
    async fn enter(&mut self, ctl: &mut Control<Self>) {}

    #[expect(
        async_fn_in_trait,
        reason = "i don't really understand what this is complaining about"
    )]
    #[expect(unused_variables, reason = "the default is a noop")]
    async fn leave(self, ctl: &mut Control<Self>) -> Result<(), Self::Error> {
        Ok(())
    }

    #[expect(
        async_fn_in_trait,
        reason = "i don't really understand what this is complaining about"
    )]
    #[expect(unused_variables, reason = "the default is a noop")]
    async fn mailbox_eof(&mut self, ctl: &mut Control<Self>) {}
}

// NOTE: for static assertions
struct DummyActor;
impl Actor for DummyActor {
    type Error = NoError;
    default_span!("dummy");
}

pub struct Control<A: Actor> {
    // NOTE: this could probably be a 'a, but I don't think i want that anyways. I think it would
    // pretty much mean all actors can borrow stack data from the main function.
    // NOTE: weak so the executor can drop itself even if there are actors still alive
    ex: Weak<LocalExecutor<'static>>,
    // NOTE: weak so the actor doesn't keep itself alive
    home: WeakAddress<A>,
    drain_mailbox: bool,
    // NOTE: this is unfortunately returng three boxed dyn traits in a row, which is not ideal.
    // It helps though, a little, that Box<()> doesn't actually allocate anything. It's possible
    // to reduce the number of boxes by implementing the async fn in the dyn trait as a poll
    // function instead of returning a boxed future, but that is a lot more complicated since
    // the state machine has to be written from scratch. It also doesn't help in this case
    // unfortunately, because async fns it awaits has to be boxed anyways...
    bg_jobs: FuturesUnordered<LocalBoxFuture<'static, ErasedLocalDeliverable<A>>>,
    idgen: IdGenerator,
    thread_root_span: Span,
    actor_span: Span,
    _signal_guard: Option<ActorGuard>,
    // HACK: these are last so they are dropped last, right before the task terminates. The actor is
    // as dead as possible as this point. The absolute best thing would be to track the task handle
    // itself, but that wasn't as easy. I tried to share the task handle using shared from
    // futures_util, but that required the error to be send, or sync or whatever it was, because it
    // used Arc internally, this would have been the optimal solution otherwise. So right now it's a
    // little weird, the return value from exiting is available before aliveness checks (whether the
    // sender is dropped or not), and all of that happens before the task has exited. I don't think
    // this will matter at all in the end, but it's annoying to know that this discrepancy exists
    // and could cause problems, potentially. The real solution is maybe to create a shared future
    // that doesn't require send/sync, or to take the Watch type from tokio or something.
    actor_rune: Rune,
    exit_send: Option<ExitSend<A::Error>>,
}

mod id_generator {
    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;

    pub type Id = u64;

    #[derive(Clone)]
    pub(super) struct IdGenerator {
        counter: Arc<AtomicU64>,
    }

    impl IdGenerator {
        pub fn new() -> Self {
            Self {
                counter: Arc::new(AtomicU64::new(0)),
            }
        }

        pub fn generate(&self) -> Id {
            self.counter
                // NOTE: this is a standalone counter that doesn't synchronize other data, so relaxed is
                // fine
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        }
    }
}

use builder::ActorBuilder;
pub use builder::{IntoActor, IntoActorExt, WithMailboxSize};
mod builder {
    use crate::signals::Interrupt;

    use super::*;

    pub(super) struct ActorBuilder<A: Actor> {
        actor: PhantomData<A>,
        ex: Rc<LocalExecutor<'static>>,
        rune: Rune,
        idgen: IdGenerator,
        thread_root_span: Span,
        mailbox_size: MailboxSize,
        yield_policy: YieldPolicy,
    }

    impl<A: Actor> ActorBuilder<A> {
        pub fn new(
            ex: Rc<LocalExecutor<'static>>,
            rune: Rune,
            idgen: IdGenerator,
            thread_root_span: Span,
        ) -> Self {
            Self {
                actor: PhantomData,
                ex,
                rune,
                idgen,
                thread_root_span,
                mailbox_size: MailboxSize::default(),
                yield_policy: YieldPolicy::default(),
            }
        }

        fn create_channel(&self) -> (AdrCore<A>, AdrRcv<A>) {
            match self.mailbox_size {
                MailboxSize::Unbounded => AdrCore::unbounded(),
                MailboxSize::Bounded(size) => AdrCore::bounded(size.get()),
            }
        }

        fn create_exit_channel() -> (ExitSend<A::Error>, ExitRecv<A::Error>) {
            oneshot_broadcast::create()
        }

        fn create_root_span(id: Id, parent: &Span) -> Span {
            span!(
                parent: parent,
                Level::DEBUG,
                "actor",
                id,
            )
        }

        fn raw(
            mut self,
            mut into_actor: impl IntoActor<A>,
        ) -> (impl Future<Output = ()> + 'static, Address<A>) {
            into_actor.adjust_builder(&mut self);

            let (snd, rcv) = self.create_channel();
            let (exit_snd, exit_rcv) = Self::create_exit_channel();
            let id = self.idgen.generate();
            debug!(id, "type" = type_name::<A>(), "Spawn new actor");

            // SAFETY: it's always safe to create from a brand new sender, the problem is if it is a
            // sender from another address.
            let home = unsafe { Address::new(snd, exit_rcv) };

            let sig_guard = into_actor.install_signals(&home);

            let actor = into_actor.into_actor();
            let span = {
                let span = Self::create_root_span(id, &self.thread_root_span);
                let span = if span.is_disabled() {
                    self.thread_root_span.clone()
                } else {
                    span
                };
                actor.span().create_or_parent(span)
            };

            let ctl = Control::new(
                Rc::downgrade(&self.ex),
                self.rune,
                home.downgrade(),
                self.idgen,
                self.thread_root_span,
                span.clone(),
                exit_snd,
                sig_guard,
            );

            // NOTE: I am pretty certain that if span is thread_root_span here it's actually redundant
            // cuz it gets entered twice, but i don't feel like that special case is worth worrying
            // about. This is of course only the case if the future is spawned as a task on an executor
            // that is blocked on its event loop where thread_root_span is active.
            let fut = actor_main(ctl, actor, rcv, home.clone(), self.yield_policy).instrument(span);
            (fut, home)
        }

        #[cfg(test)]
        pub fn no_spawn(self, actor: impl IntoActor<A>) -> (impl Future<Output = ()>, Address<A>) {
            self.raw(actor)
        }

        pub fn spawn(self, actor: impl IntoActor<A>) -> Address<A> {
            let ex = Rc::clone(&self.ex);
            let (fut, adr) = self.raw(actor);
            ex.spawn(fut).detach();
            adr
        }
    }

    #[expect(private_bounds, reason = "The builder should be private")]
    pub trait IntoActor<A: Actor>: IntoActorPriv<A> {}
    impl<A: Actor, I: IntoActorPriv<A>> IntoActor<A> for I {}

    trait IntoActorPriv<A: Actor> {
        fn adjust_builder(&mut self, _builder: &mut ActorBuilder<A>) {}
        fn install_signals(&mut self, _adr: &Address<A>) -> Option<ActorGuard> {
            None
        }
        fn into_actor(self) -> A;
    }

    impl<A: Actor> IntoActorPriv<A> for A {
        fn into_actor(self) -> A {
            self
        }
    }

    pub trait IntoActorExt<A: Actor>: IntoActor<A>
    where
        Self: Sized,
    {
        fn mailbox_size(self, mailbox_size: MailboxSize) -> WithMailboxSize<Self> {
            WithMailboxSize {
                wrapped: self,
                mailbox_size,
            }
        }

        fn yield_policy(self, policy: YieldPolicy) -> WithYieldPolicy<Self> {
            WithYieldPolicy {
                wrapped: self,
                policy,
            }
        }

        fn interruptable<R>(self, registry: R) -> WithInterruptable<Self, R> {
            WithInterruptable {
                wrapped: self,
                registry,
            }
        }
    }
    impl<A: Actor, I: IntoActorPriv<A>> IntoActorExt<A> for I {}

    pub struct WithMailboxSize<W> {
        wrapped: W,
        mailbox_size: MailboxSize,
    }

    impl<A: Actor, W: IntoActorPriv<A>> IntoActorPriv<A> for WithMailboxSize<W> {
        fn into_actor(self) -> A {
            self.wrapped.into_actor()
        }

        fn adjust_builder(&mut self, builder: &mut ActorBuilder<A>) {
            builder.mailbox_size = self.mailbox_size;
        }
    }

    pub struct WithYieldPolicy<W> {
        wrapped: W,
        policy: YieldPolicy,
    }

    impl<A: Actor, W: IntoActorPriv<A>> IntoActorPriv<A> for WithYieldPolicy<W> {
        fn into_actor(self) -> A {
            self.wrapped.into_actor()
        }

        fn adjust_builder(&mut self, builder: &mut ActorBuilder<A>) {
            builder.yield_policy = self.policy;
        }
    }

    pub struct WithInterruptable<W, R> {
        wrapped: W,
        registry: R,
    }

    impl<A, W, R> IntoActorPriv<A> for WithInterruptable<W, R>
    where
        A: Receive<Interrupt>,
        W: IntoActorPriv<A>,
        R: AsRef<SigRegistry>,
    {
        fn into_actor(self) -> A {
            self.wrapped.into_actor()
        }

        fn install_signals(&mut self, adr: &Address<A>) -> Option<ActorGuard> {
            let secret = adr.secret::<Interrupt>();
            let guard = self.registry.as_ref().register_actor(secret);
            if guard.is_interrupted {
                adr.try_send(Interrupt).expect(
                    "This address is freshly created, \
                     it has a size of at least one, and there is no \
                     other place that \"preloads\" it with messages, \
                     this will succeed",
                );
            }
            Some(guard)
        }
    }
}

impl<A: Actor> Control<A> {
    pub fn summon<A2>(&self, actor: impl IntoActor<A2>) -> Address<A2>
    where
        A2: Actor,
    {
        let ex = self
            .ex
            .upgrade()
            .expect("the executor is always alive here, it's what is running this function");

        ActorBuilder::new(
            ex,
            self.actor_rune.clone(),
            self.idgen.clone(),
            self.thread_root_span.clone(),
        )
        .spawn(actor)
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "it's private, it's fine, I guess"
    )]
    fn new(
        ex: Weak<LocalExecutor<'static>>,
        rune: Rune,
        home: WeakAddress<A>,
        idgen: IdGenerator,
        thread_root_span: Span,
        actor_span: Span,
        exit_send: ExitSend<A::Error>,
        signal_guard: Option<ActorGuard>,
    ) -> Self {
        Self {
            ex,
            actor_rune: rune,
            home,
            bg_jobs: FuturesUnordered::new(),
            drain_mailbox: false,
            idgen,
            thread_root_span,
            actor_span,
            exit_send: Some(exit_send),
            _signal_guard: signal_guard,
        }
    }

    pub fn close_mailbox(&mut self) {
        if self.home.close() {
            trace!("Closed the mailbox");
        }
    }

    pub fn close_and_clear_mailbox(&mut self) {
        self.close_mailbox();
        trace!("Draining the mailbox");
        self.drain_mailbox = true;
    }

    pub fn weak_address(&self) -> WeakAddress<A> {
        self.home.clone()
    }

    pub fn address(&self) -> Result<Address<A>, AddressClosedError> {
        // NOTE: This can fail if all addresses got dropped and/or a soft exit has been issued. It's
        // guaranteed to not fail in enter and message handlers, unless soft exiting.
        self.home.upgrade().context(AddressClosedSnafu)
    }

    fn add_bg_job(&mut self, job: LocalBoxFuture<'static, ErasedLocalDeliverable<A>>) {
        self.bg_jobs.push(job);
    }
}

#[derive(Debug, Snafu)]
#[snafu(display("The address is closed"))]
pub struct AddressClosedError;

#[derive(Debug, Snafu)]
#[snafu(module, context(suffix(false)))]
pub enum WaitError<T: std::error::Error + 'static> {
    #[snafu(display("No error available, the actor must have panicked"))]
    Panicked,
    #[snafu(display("Actor exited with an error"))]
    Exited { source: T },
}

#[derive(Debug, Snafu)]
#[snafu(module, context(suffix(false)))]
pub enum TryWaitError<T: std::error::Error + 'static> {
    #[snafu(display("No error available, the actor must have panicked"))]
    Panicked,
    #[snafu(display("The actor is still alive"))]
    StillAlive,
    #[snafu(display("Actor exited with an error"))]
    Exited { source: T },
}

type ExitRecv<E> = ob::MultiReceiver<Result<(), E>>;
type ExitSend<E> = ob::UniqueSender<Result<(), E>>;

use oneshot_broadcast as ob;
mod oneshot_broadcast {
    use async_broadcast as bhannel;
    use static_assertions::{assert_impl_all, assert_not_impl_any};

    pub(super) struct UniqueSender<T> {
        inner: bhannel::Sender<T>,
    }
    assert_not_impl_any!(UniqueSender<()>: Clone);

    impl<T: Clone> UniqueSender<T> {
        fn new(sender: bhannel::Sender<T>) -> Option<Self> {
            if sender.sender_count() != 1 {
                return None;
            }
            Some(Self { inner: sender })
        }

        pub fn broadcast(self, msg: T) {
            use bhannel::TrySendError;
            match self.inner.try_broadcast(msg) {
                Ok(None) | Err(TrySendError::Closed(_)) => (),
                Ok(Some(_)) | Err(TrySendError::Full(_)) | Err(TrySendError::Inactive(_)) => {
                    panic!("should not happen")
                }
            }
        }
    }

    #[derive(Clone)]
    pub(super) struct MultiReceiver<T> {
        inner: bhannel::Receiver<T>,
    }
    assert_impl_all!(MultiReceiver<()>: Clone);

    impl<T: Clone> MultiReceiver<T> {
        fn new(receiver: bhannel::Receiver<T>) -> Self {
            Self { inner: receiver }
        }

        pub async fn recv(&mut self) -> Option<T> {
            debug_assert!(self.inner.sender_count() <= 1);
            match self.inner.recv_direct().await {
                Ok(x) => Some(x),
                Err(async_broadcast::RecvError::Closed) => None,
                Err(async_broadcast::RecvError::Overflowed(_)) => panic!("this can't overflow"),
            }
        }

        pub fn try_recv(&mut self) -> Result<T, TryError> {
            debug_assert!(self.inner.sender_count() <= 1);
            match self.inner.try_recv() {
                Ok(x) => Ok(x),
                Err(async_broadcast::TryRecvError::Overflowed(_)) => panic!("this can't overflow"),
                Err(async_broadcast::TryRecvError::Empty) => Err(TryError::Empty),
                Err(async_broadcast::TryRecvError::Closed) => Err(TryError::Closed),
            }
        }

        pub fn is_sender_dropped(&self) -> bool {
            debug_assert!(self.inner.sender_count() <= 1);
            self.inner.is_closed()
        }
    }

    pub(super) enum TryError {
        Closed,
        Empty,
    }

    pub(super) fn create<T: Clone>() -> (UniqueSender<T>, MultiReceiver<T>) {
        // NOTE: up to one message will be sent on this, so the overflow flag doesn't matter.
        // NOTE: the receiver will never be inactive, so the await_active flag doesn't matter
        let (snd, rcv) = bhannel::broadcast(1);
        (
            UniqueSender::new(snd).expect("there is only one of them"),
            MultiReceiver::new(rcv),
        )
    }
}

// NOTE: both T and Retval are 'static everywhere, but it didn't help to add those here
#[diagnostic::on_unimplemented(
    message = "can't send messages of type `{T}` to actor `{Self}`",
    label = "this one",
    note = "implement `Receive<{T}>` on `{Self}` to make it be able to receive them"
)]
pub trait Receive<T>: Actor {
    // NOTE: I'm pretty sure the 'static bound on the Actor trait is implying that this also must be
    // 'static
    // RANT: these, annoyingly, can't have default values
    type Retval; // = ()

    #[expect(
        async_fn_in_trait,
        reason = "i don't really understand what this is complaining about"
    )]
    async fn receive(&mut self, msg: T, ctl: &mut Control<Self>) -> Self::Retval;
}

type ErasedDeliverable<A> = Box<dyn Deliverable<A> + Send>;
type ErasedLocalDeliverable<A> = Box<dyn Deliverable<A>>;

use scope::{Local, Remote, Scope};
mod scope {
    use std::{marker::PhantomData, rc::Rc};

    use static_assertions::{assert_impl_all, assert_not_impl_any, const_assert_eq};

    mod private {
        pub trait Sealed {}
    }

    pub trait Scope: private::Sealed + 'static {}

    pub struct Local {
        _priv: PhantomData<Rc<()>>,
    }
    impl private::Sealed for Local {}
    impl Scope for Local {}
    assert_not_impl_any!(Local: Send, Sync);
    const_assert_eq!(std::mem::size_of::<Local>(), 0);

    pub struct Remote {
        _priv: (),
    }
    impl private::Sealed for Remote {}
    impl Scope for Remote {}
    assert_impl_all!(Remote: Send, Sync);
    const_assert_eq!(std::mem::size_of::<Remote>(), 0);
}

use adr_core::{AdrCore, AdrRcv, ErasedAdr, WeakAdrCore};
pub use adr_core::{Reply, ReplyError, SendError, TrySendError};
mod adr_core {
    use super::{
        Actor, CanSendPackage, CanSendTicket, DummyActor, ErasedDeliverable, OneWayTicket, Package,
        Receive,
    };
    use async_channel as channel;
    use pin_project::pin_project;
    use snafu::{OptionExt, Snafu};
    use static_assertions::assert_impl_all;
    use std::any::{Any, type_name};

    pub(super) type AdrRcv<A> = channel::Receiver<ErasedDeliverable<A>>;

    pub(super) struct AdrCore<A: Actor> {
        sender: channel::Sender<ErasedDeliverable<A>>,
    }
    assert_impl_all!(AdrCore<DummyActor>: Send);

    impl<A: Actor> Clone for AdrCore<A> {
        fn clone(&self) -> Self {
            Self {
                sender: self.sender.clone(),
            }
        }
    }

    impl<A: Actor> AdrCore<A> {
        pub fn unbounded() -> (Self, AdrRcv<A>) {
            let (snd, rcv) = channel::unbounded();
            (Self { sender: snd }, rcv)
        }

        pub fn bounded(capacity: usize) -> (Self, AdrRcv<A>) {
            let (snd, rcv) = channel::bounded(capacity);
            (Self { sender: snd }, rcv)
        }

        pub fn downgrade(&self) -> WeakAdrCore<A> {
            let weak = self.sender.downgrade();
            WeakAdrCore { sender: weak }
        }

        pub fn is_closed(&self) -> bool {
            self.sender.is_closed()
        }

        #[expect(dead_code, reason = "unused for now")]
        pub fn close(&self) {
            self.sender.close();
        }

        pub fn erased(&self) -> ErasedAdr
        where
            A: 'static,
        {
            ErasedAdr {
                _inner: Box::new(self.sender.clone()),
            }
        }

        pub async fn send_ticket<S, T>(&self, msg: T) -> Result<(), SendError>
        where
            A: Receive<T>,
            S: CanSendTicket<A, T>,
        {
            tracing::trace!(
                "to" = type_name::<A>(),
                "msg" = type_name::<T>(),
                "Send message"
            );
            let ticket = OneWayTicket {
                msg,
                erased: self.erased(),
            };
            let erased = S::erase_ticket(ticket);
            self.sender.send(erased).await?;
            Ok(())
        }

        pub async fn send_package<S, T>(&self, msg: T) -> Result<Reply<A::Retval>, SendError>
        where
            A: Receive<T>,
            S: CanSendPackage<A, T>,
        {
            tracing::trace!(
                "to" = type_name::<A>(),
                "msg" = type_name::<T>(),
                "return" = type_name::<A::Retval>(),
                "Send and receive"
            );
            let (returner, ret_rcv) = oneshot::async_channel::<A::Retval>();
            let package = Package {
                msg,
                returner,
                erased: self.erased(),
            };
            let erased = S::erase_package(package);
            self.sender.send(erased).await?;
            Ok(Reply { recv: ret_rcv })
        }

        pub fn try_send_ticket<S, T>(&self, msg: T) -> Result<(), TrySendError>
        where
            A: Receive<T>,
            S: CanSendTicket<A, T>,
        {
            tracing::trace!(
                "to" = type_name::<A>(),
                "msg" = type_name::<T>(),
                "Try send message"
            );
            let ticket = OneWayTicket {
                msg,
                erased: self.erased(),
            };
            let erased = S::erase_ticket(ticket);
            self.sender.try_send(erased)?;
            Ok(())
        }

        pub fn send_blocking_ticket<S, T>(&self, msg: T) -> Result<(), SendError>
        where
            A: Receive<T>,
            S: CanSendTicket<A, T>,
        {
            tracing::trace!(
                "to" = type_name::<A>(),
                "msg" = type_name::<T>(),
                "Send blocking message"
            );
            let ticket = OneWayTicket {
                msg,
                erased: self.erased(),
            };
            let erased = S::erase_ticket(ticket);
            self.sender.send_blocking(erased)?;
            Ok(())
        }
    }

    pub(super) struct WeakAdrCore<A: Actor> {
        sender: channel::WeakSender<ErasedDeliverable<A>>,
    }
    assert_impl_all!(WeakAdrCore<DummyActor>: Send);

    impl<A: Actor> Clone for WeakAdrCore<A> {
        fn clone(&self) -> Self {
            Self {
                sender: self.sender.clone(),
            }
        }
    }

    impl<A: Actor> WeakAdrCore<A> {
        pub fn upgrade(&self) -> Option<AdrCore<A>> {
            self.sender.upgrade().map(|snd| AdrCore { sender: snd })
        }

        pub fn close(&self) -> bool {
            // NOTE: this will only fail if already closed
            if let Some(upgraded) = self.sender.upgrade() {
                return upgraded.close();
            }
            false
        }
    }

    pub(super) struct ErasedAdr {
        _inner: Box<dyn Any + Send>,
    }

    #[derive(Debug, Snafu)]
    #[snafu(display("Could not send message, actor dead :("))]
    // TODO: make these send errors return the value that was attempted to be sent? It's difficult to
    // get them back since they are erased in a Box, but it should be possible to downcast them back i
    // think.
    pub struct SendError;

    impl<T> From<channel::SendError<T>> for SendError {
        fn from(_value: channel::SendError<T>) -> Self {
            SendSnafu.build()
        }
    }

    #[derive(Debug, Snafu)]
    #[snafu(module, context(suffix(false)))]
    pub enum TrySendError {
        #[snafu(display("Could not send message, mailbox full"))]
        Full,
        #[snafu(display("Could not send message, actor dead :("))]
        Closed,
    }

    impl<T> From<channel::TrySendError<T>> for TrySendError {
        fn from(value: channel::TrySendError<T>) -> Self {
            match value {
                channel::TrySendError::Full(_) => try_send_error::Full.build(),
                channel::TrySendError::Closed(_) => try_send_error::Closed.build(),
            }
        }
    }

    #[derive(Debug, Snafu)]
    #[snafu(display("Could not receive reply, actor dead :("))]
    pub struct ReplyError;

    #[must_use = "this does nothing unless polled"]
    #[pin_project]
    pub struct Reply<R> {
        #[pin]
        recv: oneshot::AsyncReceiver<R>,
    }

    impl<R> Future for Reply<R> {
        type Output = Result<R, ReplyError>;

        fn poll(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Self::Output> {
            let this = self.project();
            let reply = std::task::ready!(this.recv.poll(cx));
            std::task::Poll::Ready(reply.ok().context(ReplySnafu))
        }
    }
}

pub struct GenericAddress<A: Actor, S: Scope> {
    core: AdrCore<A>,
    status: ExitRecv<A::Error>,
    _scope: PhantomData<S>,
}

pub type Address<A> = GenericAddress<A, Local>;
assert_not_impl_any!(Address<DummyActor>: Send, Sync);

pub type RemoteAddress<A> = GenericAddress<A, Remote>;
assert_impl_all!(RemoteAddress<DummyActor>: Send, Sync);

pub struct GenericWeakAddress<A: Actor, S: Scope> {
    core: WeakAdrCore<A>,
    status: ExitRecv<A::Error>,
    _scope: PhantomData<S>,
}

pub type WeakAddress<A> = GenericWeakAddress<A, Local>;
assert_not_impl_any!(WeakAddress<DummyActor>: Send, Sync);

pub type RemoteWeakAddress<A> = GenericWeakAddress<A, Remote>;
assert_impl_all!(RemoteWeakAddress<DummyActor>: Send, Sync);

impl<A: Actor, S: Scope> Clone for GenericAddress<A, S> {
    fn clone(&self) -> Self {
        let c = self.core.clone();
        let b = self.status.clone();
        // SAFETY: this gets the same scope as the original
        unsafe { Self::new(c, b) }
    }
}

impl<A: Actor, S: Scope> Clone for GenericWeakAddress<A, S> {
    fn clone(&self) -> Self {
        let c = self.core.clone();
        let b = self.status.clone();
        // SAFETY: this gets the same scope as the original
        unsafe { Self::new(c, b) }
    }
}

impl<A: Actor, S: Scope> GenericAddress<A, S> {
    // SAFETY: this is unsafe because it's not safe to create a local address from a remote one,
    // which would enable !Send data change threads.
    unsafe fn new(core: AdrCore<A>, status: ExitRecv<A::Error>) -> Self {
        Self {
            core,
            status,
            _scope: PhantomData,
        }
    }

    // TODO: getters for retrieving the channel capacity and size, useful for a multiplexing actor

    pub fn downgrade(&self) -> GenericWeakAddress<A, S> {
        let c = self.core.downgrade();
        let b = self.status.clone();
        // SAFETY: this gets the same scope as the original
        unsafe { GenericWeakAddress::new(c, b) }
    }

    /// Can't send any more messages, but the actor might still be alive
    pub fn is_closed(&self) -> bool {
        self.core.is_closed()
    }

    /// The actor is dead
    pub fn is_dead(&self) -> bool {
        self.status.is_sender_dropped()
    }

    /// Wait for the actor to die.
    pub async fn wait(self) -> Result<(), WaitError<A::Error>> {
        let weak = self.downgrade();
        drop(self);
        weak.wait().await
    }

    pub fn try_wait(self) -> Result<(), TryWaitError<A::Error>> {
        let weak = self.downgrade();
        drop(self);
        weak.try_wait()
    }
}

impl<A: Actor, S: Scope> GenericWeakAddress<A, S> {
    // SAFETY: this is unsafe because it's not safe to create a local address from a remote one,
    // which would enable !Send data change threads.
    unsafe fn new(core: WeakAdrCore<A>, status: ExitRecv<A::Error>) -> Self {
        Self {
            core,
            status,
            _scope: PhantomData,
        }
    }

    pub fn upgrade(&self) -> Option<GenericAddress<A, S>> {
        self.core
            .upgrade()
            // SAFETY: this gets the same scope as the original
            .map(|sender| {
                let b = self.status.clone();
                unsafe { GenericAddress::new(sender, b) }
            })
    }

    pub fn close(&self) -> bool {
        self.core.close()
    }

    /// The actor is dead
    pub fn is_dead(&self) -> bool {
        self.status.is_sender_dropped()
    }

    /// Wait for the actor to die.
    pub async fn wait(mut self) -> Result<(), WaitError<A::Error>> {
        self.status
            .recv()
            .await
            .map(|ok| ok.context(wait_error::Exited))
            .unwrap_or_else(|| wait_error::Panicked.fail())
    }

    pub fn try_wait(mut self) -> Result<(), TryWaitError<A::Error>> {
        self.status
            .try_recv()
            .map(|ok| ok.context(try_wait_error::Exited))
            .unwrap_or_else(|err| match err {
                ob::TryError::Empty => try_wait_error::StillAlive.fail(),
                ob::TryError::Closed => try_wait_error::Panicked.fail(),
            })
    }
}

impl<A: Actor> Address<A> {
    pub fn remote(&self) -> RemoteAddress<A> {
        // SAFETY: It's safe to go from a local address to a remote one, but not the other way
        // around, since a remote address can only send data that is Send.
        let c = self.core.clone();
        let b = self.status.clone();
        unsafe { RemoteAddress::new(c, b) }
    }
}

impl<A: Actor> WeakAddress<A> {
    pub fn remote(&self) -> RemoteWeakAddress<A> {
        // SAFETY: It's safe to go from a local address to a remote one, but not the other way
        // around, since a remote address can only send data that is Send.
        let c = self.core.clone();
        let b = self.status.clone();
        unsafe { RemoteWeakAddress::new(c, b) }
    }
}

#[expect(
    private_bounds,
    reason = "the private trait and all types it is using should be private"
)]
// NOTE: using do_not_recommend and on_unimplemented would hide the actual reason a value couldn't
// be sent, so those are not used anymore, even though the private part is exposed
pub trait CanSendPackage<A, T>: CanSendPackagePriv<A, T> {}
impl<A, T, X> CanSendPackage<A, T> for X where X: CanSendPackagePriv<A, T> {}

trait CanSendPackagePriv<A, T>: Scope {
    fn erase_package(p: Package<T, A::Retval>) -> ErasedDeliverable<A>
    where
        A: Receive<T>;
}

impl<A, T> CanSendPackagePriv<A, T> for Remote
where
    T: Send + 'static,
    A: Receive<T>,
    A::Retval: Send,
{
    fn erase_package(p: Package<T, A::Retval>) -> ErasedDeliverable<A> {
        Box::new(p)
    }
}

impl<A, T> CanSendPackagePriv<A, T> for Local
where
    A: Receive<T>,
    T: 'static,
{
    fn erase_package(p: Package<T, A::Retval>) -> ErasedDeliverable<A> {
        // SAFETY: one of these can only be sent by a local address, which means the sender has
        // never left the current thread, which means it's safe to send non-send data on it.
        Box::new(unsafe { UnsafeSendWrapper::new(p) })
    }
}

#[expect(
    private_bounds,
    reason = "the private trait and all types it is using should be private"
)]
// NOTE: using do_not_recommend and on_unimplemented would hide the actual reason a value couldn't
// be sent, so those are not used anymore, even though the private part is exposed
pub trait CanSendTicket<A, T>: CanSendTicketPriv<A, T> {}
impl<A, T, X> CanSendTicket<A, T> for X where X: CanSendTicketPriv<A, T> {}

trait CanSendTicketPriv<A, T>: Scope {
    fn erase_ticket(t: OneWayTicket<T>) -> ErasedDeliverable<A>
    where
        A: Receive<T>;
}

impl<A, T> CanSendTicketPriv<A, T> for Remote
where
    T: Send + 'static,
    A: Receive<T>,
{
    fn erase_ticket(t: OneWayTicket<T>) -> ErasedDeliverable<A> {
        Box::new(t)
    }
}

impl<A, T> CanSendTicketPriv<A, T> for Local
where
    A: Receive<T>,
    T: 'static,
{
    fn erase_ticket(t: OneWayTicket<T>) -> ErasedDeliverable<A> {
        // SAFETY: one of these can only be sent by a local address, which means the sender has
        // never left the current thread, which means it's safe to send non-send data on it.
        Box::new(unsafe { UnsafeSendWrapper::new(t) })
    }
}

trait Deliverable<A: Actor> {
    fn deliver<'a>(
        self: Box<Self>,
        actor: &'a mut A,
        ctl: &'a mut Control<A>,
    ) -> LocalBoxFuture<'a, ()>;
}

mod unsafe_wrapper {
    #[cfg(debug_assertions)]
    use std::thread;

    #[cfg_attr(not(debug_assertions), repr(transparent))]
    pub struct UnsafeSendWrapper<D> {
        inner: D,
        #[cfg(debug_assertions)]
        origin_thread: thread::ThreadId,
    }

    // SAFETY: the new function is unsafe, so the caller is responsible for safety
    unsafe impl<D> Send for UnsafeSendWrapper<D> {}

    impl<D> UnsafeSendWrapper<D> {
        // SAFETY: This makes !Send data appear as Send, so care should be taken to make sure this
        // value doesn't change threads.
        pub unsafe fn new(data: D) -> Self {
            Self {
                inner: data,
                #[cfg(debug_assertions)]
                origin_thread: thread::current().id(),
            }
        }

        #[cfg_attr(
            debug_assertions,
            expect(
                clippy::boxed_local,
                reason = "complains on the debug variant, but is needed on the release variant"
            )
        )]
        pub fn into_boxed_inner(self: Box<Self>) -> Box<D> {
            cfg_select! {
                debug_assertions => {
                    Box::new(self.inner)
                }
                _ => {
                    let raw = Box::into_raw(self);
                    let inner_raw = raw as *mut D;
                    // SAFETY: the wrapper is repr(transparent), so its guaranteed to have the same size and
                    // alignment, making this cast safe.
                    unsafe { Box::from_raw(inner_raw) }
                }
            }
        }

        #[expect(dead_code, reason = "not used yet")]
        pub fn inner(&self) -> &D {
            #[cfg(debug_assertions)]
            assert_eq!(
                thread::current().id(),
                self.origin_thread,
                "Value was sent to another thread"
            );
            &self.inner
        }
    }
}

impl<A, D> Deliverable<A> for UnsafeSendWrapper<D>
where
    A: Actor,
    D: Deliverable<A>,
{
    fn deliver<'a>(
        self: Box<Self>,
        actor: &'a mut A,
        ctl: &'a mut Control<A>,
    ) -> LocalBoxFuture<'a, ()> {
        let inner_box = self.into_boxed_inner();
        inner_box.deliver(actor, ctl)
    }
}

struct Package<T, R> {
    msg: T,
    returner: oneshot::Sender<R>,
    erased: ErasedAdr,
}

impl<A, T> Deliverable<A> for Package<T, A::Retval>
where
    A: Receive<T>,
    T: 'static,
{
    fn deliver<'a>(
        self: Box<Self>,
        actor: &'a mut A,
        ctl: &'a mut Control<A>,
    ) -> LocalBoxFuture<'a, ()> {
        async move {
            // NOTE: To prevent the problam where the channel would get closed before all messages
            // in it had been processed, which is bad since a message handler should be able to get
            // an address unless the actor is exiting. I could only think of two different ways to
            // keep the channel open if it had messages in its queue when all addresses were
            // dropped: wrapping the channel's control block in a mutex or by adding the number of
            // messages in the queue to the strong sender count. I guess i also could rewrite/mod
            // the concurrent queue that async-channels relies on, but that felt too far. The mutex
            // solution is not good since it ruins the lock-free nature of the channel, so the
            // modifying the sender count was the way to go. I could think of more solutions to this
            // problem, but they all had some race condition where a weak sender could get upgraded
            // even though it shouldn't have, although rare it wasn't good enough for me. I didn't
            // feel like forking the async-channel crate, but i realized that i could achieve the
            // same effect by cloning the address and sending it alongside the message. It's not as
            // efficient, but it achieves the same thing, which is good enough for me.
            let _keep_alive = self.erased;
            trace!("Received message");
            let ret = actor.receive(self.msg, ctl).await;
            let _: Result<_, _> = self.returner.send(ret);
        }
        .instrument(trace_span!(
            "snd_rcv",
            msg = type_name::<T>(),
            return = type_name::<A::Retval>(),
        ))
        .boxed_local()
    }
}

struct OneWayTicket<T> {
    msg: T,
    erased: ErasedAdr,
}

impl<T, A> Deliverable<A> for OneWayTicket<T>
where
    A: Receive<T>,
    T: 'static,
{
    fn deliver<'a>(
        self: Box<Self>,
        actor: &'a mut A,
        ctl: &'a mut Control<A>,
    ) -> LocalBoxFuture<'a, ()> {
        async move {
            let _keep_alive = self.erased;
            trace!("Received message");
            let _ret = actor.receive(self.msg, ctl).await;
        }
        .instrument(trace_span!("oneway", msg = type_name::<T>(),))
        .boxed_local()
    }
}

impl<A: Actor> Deliverable<A> for () {
    fn deliver<'a>(
        self: Box<Self>,
        _actor: &'a mut A,
        _ctl: &'a mut Control<A>,
    ) -> LocalBoxFuture<'a, ()> {
        // NOTE: I don't like that this is allocating something that doesn't do anything
        async {}.boxed_local()
    }
}

pub mod bg_job {
    use blocking::Unblock;

    use super::*;

    pub struct Skip {
        _priv: (),
    }

    pub struct NonSkip<S> {
        inner: S,
    }

    impl Skip {
        fn new() -> Self {
            Self { _priv: () }
        }
    }

    #[must_use = "the job must be started to do anything"]
    pub struct Job<'a, F, S = Skip> {
        future: F,
        sync: S,
        span: DeferredSpan<'a>,
    }

    impl<'a, F> Job<'a, F, Skip> {
        pub fn new(future: F) -> Self {
            Self {
                future,
                sync: Skip::new(),
                span: DeferredSpan::none(),
            }
        }

        pub fn then<S, A, O>(self, sync: S) -> Job<'a, F, NonSkip<S>>
        where
            // HACK: this is needed to workaround passing async |...| {} to the then function. I
            // honestly don't really understand why this is needed, something about closure being
            // early-binding and functions being late-binding.
            // https://github.com/rust-lang/rust/issues/70263 .This shouldn't actually be necessary,
            // but adding this here solves an annoying HRTB early-bind vs late-bind of lifetimes
            // when actually calling start on this job. I don't understand how this actually helps
            // the compiler, but it removes the error message about the closure not being general
            // enough.
            S: AsyncFnOnce(&mut A, &mut Control<A>, O),
        {
            Job {
                future: self.future,
                sync: NonSkip { inner: sync },
                span: self.span,
            }
        }
    }

    impl<'a, F> Job<'a, Unblock<F>, Skip> {
        pub fn new_blocking(func: F) -> Self {
            Self::new(Unblock::new(func))
        }
    }

    impl<'a, F, S> Job<'a, F, S> {
        pub fn instrument(mut self, span: DeferredSpan<'a>) -> Self {
            self.span = span;
            self
        }
    }

    impl<'a, F, S> Job<'a, F, NonSkip<S>> {
        pub fn start<A, O>(self, ctl: &mut Control<A>)
        where
            F: Future<Output = O> + 'static,
            S: AsyncFnOnce(&mut A, &mut Control<A>, O) + 'static,
            A: Actor,
            O: 'static,
        {
            let span = self.span.create_or_parent(ctl.actor_span.clone());
            let fut = {
                let span = span.clone();
                async move {
                    let output = self.future.await;
                    let deliv = SyncPointDeliv {
                        span,
                        output,
                        sync: self.sync.inner,
                    };
                    let erased: ErasedLocalDeliverable<A> = Box::new(deliv);
                    erased
                }
            }
            // HACK: if the deferred span is disabled, then this future will be instrumentet with
            // the actor_span, which is technically redundant since this future will be polled in a
            // context where the actor_span is the current span, or at least it should be. I will
            // leave this as is, fewer special cases to think about.
            .instrument(span)
            .boxed_local();

            ctl.add_bg_job(fut);
        }
    }

    impl<'a, F> Job<'a, F, Skip> {
        pub fn start<A>(self, ctl: &mut Control<A>)
        where
            F: Future<Output = ()> + 'static,
            A: Actor,
        {
            let span = self.span.create_or_parent(ctl.actor_span.clone());
            let fut = async {
                let output: () = self.future.await;
                // NOTE: () is a zero-sized type, so the box is not actually allocating anything
                let erased: ErasedLocalDeliverable<A> = Box::new(output);
                erased
            }
            // HACK: if the deferred span is disabled, then this future will be instrumentet with
            // the actor_span, which is technically redundant since this future will be polled in a
            // context where the actor_span is the current span, or at least it should be. I will
            // leave this as is, fewer special cases to think about.
            .instrument(span)
            .boxed_local();

            ctl.add_bg_job(fut);
        }
    }

    struct SyncPointDeliv<O, F> {
        span: Span,
        output: O,
        sync: F,
    }

    impl<O, F, A> Deliverable<A> for SyncPointDeliv<O, F>
    where
        A: Actor,
        F: AsyncFnOnce(&mut A, &mut Control<A>, O) + 'static,
        O: 'static,
    {
        fn deliver<'a>(
            self: Box<Self>,
            actor: &'a mut A,
            ctl: &'a mut Control<A>,
        ) -> LocalBoxFuture<'a, ()> {
            let Self { span, output, sync } = *self;
            async {
                sync(actor, ctl, output).await;
            }
            .instrument(span)
            .boxed_local()
        }
    }

    #[cfg(test)]
    mod tests {
        use crate::{
            deferred_span,
            test_utils::{assert_parent_span, assert_span},
        };

        use super::*;

        #[test]
        fn the_jobs_actually_run() {
            struct Alice {
                snd: SecretAddress<i32>,
            }
            impl Actor for Alice {
                type Error = Infallible;

                fn span(&self) -> DeferredSpan<'_> {
                    deferred_span!(Level::INFO, "alice")
                }

                async fn enter(&mut self, ctl: &mut Control<Self>) {
                    Job::new(async {
                        assert_span("double");
                        assert_parent_span("alice");
                        tracing::info!("Producing the value");
                        5
                    })
                    .then(async |act: &mut Self, _ctl, out| {
                        assert_span("double");
                        assert_parent_span("alice");
                        tracing::info!("Sending from the actor");
                        act.snd.send(out).await.unwrap()
                    })
                    .instrument(crate::deferred_info_span!("double"))
                    .start(ctl);

                    Job::new({
                        let snd = self.snd.clone();
                        async move {
                            assert_span("single");
                            assert_parent_span("alice");
                            tracing::info!("Sending from the job");
                            snd.send(5).await.unwrap()
                        }
                    })
                    .instrument(crate::deferred_info_span!("single"))
                    .start(ctl);

                    Job::new({
                        let snd = self.snd.clone();
                        async move {
                            assert_span("alice");
                            tracing::info!("Sending from the non-instrumented job");
                            snd.send(5).await.unwrap()
                        }
                    })
                    .start(ctl);
                }
            }

            let main_stage = Stage::new();
            let (snd, rcv) = SecretAddress::new_channel();
            main_stage.summon(Alice { snd });

            main_stage.assert_plays_within(100);

            let messages = rcv
                .collect::<Vec<_>>()
                .now_or_never()
                .expect("this should be ready");

            assert_eq!(messages, vec![5, 5, 5]);
        }
    }
}

impl<A: Actor, S: Scope> GenericAddress<A, S> {
    pub async fn send_receive<T>(&self, msg: T) -> Result<Reply<A::Retval>, SendError>
    where
        A: Receive<T>,
        S: CanSendPackage<A, T>,
    {
        self.core.send_package::<S, T>(msg).await
    }

    pub async fn send<T>(&self, msg: T) -> Result<(), SendError>
    where
        A: Receive<T>,
        S: CanSendTicket<A, T>,
    {
        self.core.send_ticket::<S, T>(msg).await
    }

    pub fn try_send<T>(&self, msg: T) -> Result<(), TrySendError>
    where
        A: Receive<T>,
        S: CanSendTicket<A, T>,
    {
        self.core.try_send_ticket::<S, T>(msg)
    }

    pub fn send_blocking<T>(&self, msg: T) -> Result<(), SendError>
    where
        A: Receive<T>,
        S: CanSendTicket<A, T>,
    {
        self.core.send_blocking_ticket::<S, T>(msg)
    }
}

pub use secret_adr::SecretAddress;
mod secret_adr {
    use super::{
        Actor, AdrCore, CanSendTicket, GenericAddress, Receive, Scope, SendError, TrySendError,
        UnsafeSendWrapper,
    };
    #[cfg(test)]
    use async_channel as channel;
    use futures_core::future::LocalBoxFuture;
    use futures_util::FutureExt;
    use static_assertions::{assert_impl_all, assert_not_impl_any};
    use std::{marker::PhantomData, rc::Rc};

    // TODO: create a weak secret address?
    pub struct SecretAddress<T> {
        secret: Box<dyn SecretSend<T> + Send>,
        _scope: PhantomData<T>,
    }
    assert_not_impl_any!(SecretAddress<Rc<()>>: Send);
    assert_impl_all!(SecretAddress<()>: Send);

    impl<T> Clone for SecretAddress<T> {
        fn clone(&self) -> Self {
            Self {
                secret: self.secret.clone(),
                _scope: self._scope,
            }
        }
    }

    impl<T> SecretAddress<T> {
        pub async fn send(&self, msg: T) -> Result<(), SendError> {
            self.secret.send(msg).await
        }

        pub fn try_send(&self, msg: T) -> Result<(), TrySendError> {
            self.secret.try_send(msg)
        }

        pub fn send_blocking(&self, msg: T) -> Result<(), SendError> {
            self.secret.send_blocking(msg)
        }

        #[cfg(test)]
        pub(crate) fn new_channel() -> (Self, channel::Receiver<T>)
        where
            T: Send + 'static,
        {
            let (snd, rcv) = channel::unbounded();
            let adr = Self {
                secret: Box::new((PhantomData::<super::Remote>, snd)),
                _scope: PhantomData,
            };
            (adr, rcv)
        }
    }

    trait SecretSend<T> {
        // RANT: this can't be a simple async fn, cuz that is not object safe...
        fn send<'a>(&'a self, msg: T) -> LocalBoxFuture<'a, Result<(), SendError>>
        where
            T: 'a;
        fn try_send(&self, msg: T) -> Result<(), TrySendError>;
        fn send_blocking(&self, msg: T) -> Result<(), SendError>;
        fn clone_secret_send(&self) -> Box<dyn SecretSend<T> + Send>;
    }

    impl<A, S, T> SecretSend<T> for (UnsafeSendWrapper<PhantomData<S>>, AdrCore<A>)
    where
        A: Receive<T>,
        S: CanSendTicket<A, T>,
    {
        fn send<'a>(&'a self, msg: T) -> LocalBoxFuture<'a, Result<(), SendError>>
        where
            T: 'a,
        {
            self.1.send_ticket::<S, T>(msg).boxed_local()
        }

        fn clone_secret_send(&self) -> Box<dyn SecretSend<T> + Send> {
            let core = self.1.clone();
            // SAFETY: the scope already was in a wrapper, this is just a copy
            let scope = unsafe { UnsafeSendWrapper::new(PhantomData::<S>) };
            let copy: Self = (scope, core);
            Box::new(copy)
        }

        fn try_send(&self, msg: T) -> Result<(), TrySendError> {
            self.1.try_send_ticket::<S, T>(msg)
        }

        fn send_blocking(&self, msg: T) -> Result<(), SendError> {
            self.1.send_blocking_ticket::<S, T>(msg)
        }
    }

    #[cfg(test)]
    impl<T> SecretSend<T> for (PhantomData<super::Remote>, channel::Sender<T>)
    where
        T: Send + 'static,
    {
        fn send<'a>(&'a self, msg: T) -> LocalBoxFuture<'a, Result<(), SendError>>
        where
            T: 'a,
        {
            use futures_util::TryFutureExt;

            self.1.send(msg).map_err(Into::into).boxed_local()
        }

        fn clone_secret_send(&self) -> Box<dyn SecretSend<T> + Send> {
            let clone: Self = (PhantomData, self.1.clone());
            Box::new(clone)
        }

        fn try_send(&self, msg: T) -> Result<(), TrySendError> {
            self.1.try_send(msg).map_err(Into::into)
        }

        fn send_blocking(&self, msg: T) -> Result<(), SendError> {
            self.1.send_blocking(msg).map_err(Into::into)
        }
    }

    impl<T> Clone for Box<dyn SecretSend<T> + Send> {
        fn clone(&self) -> Self {
            self.clone_secret_send()
        }
    }

    // TODO: figure out how to generalize this to more than two types.
    pub enum Multi<T1, T2> {
        Left(T1),
        Right(T2),
    }

    // HACK: the extra unit here is just to distinguish it from the other impl
    impl<A, S, T1, T2> SecretSend<Multi<T1, T2>> for (UnsafeSendWrapper<PhantomData<S>>, AdrCore<A>, ())
    where
        A: Receive<T1> + Receive<T2>,
        S: CanSendTicket<A, T1> + CanSendTicket<A, T2>,
    {
        fn send<'a>(&'a self, msg: Multi<T1, T2>) -> LocalBoxFuture<'a, Result<(), SendError>>
        where
            Multi<T1, T2>: 'a,
        {
            match msg {
                Multi::Left(m) => self.1.send_ticket::<S, T1>(m).boxed_local(),
                Multi::Right(m) => self.1.send_ticket::<S, T2>(m).boxed_local(),
            }
        }

        fn try_send(&self, msg: Multi<T1, T2>) -> Result<(), TrySendError> {
            match msg {
                Multi::Left(m) => self.1.try_send_ticket::<S, T1>(m),
                Multi::Right(m) => self.1.try_send_ticket::<S, T2>(m),
            }
        }

        fn send_blocking(&self, msg: Multi<T1, T2>) -> Result<(), SendError> {
            match msg {
                Multi::Left(m) => self.1.send_blocking_ticket::<S, T1>(m),
                Multi::Right(m) => self.1.send_blocking_ticket::<S, T2>(m),
            }
        }

        fn clone_secret_send(&self) -> Box<dyn SecretSend<Multi<T1, T2>> + Send> {
            let core = self.1.clone();
            // SAFETY: the scope already was in a wrapper, this is just a copy
            let scope = unsafe { UnsafeSendWrapper::new(PhantomData::<S>) };
            let copy: Self = (scope, core, ());
            Box::new(copy)
        }
    }

    pub type SecretMultiAddress<T1, T2> = SecretAddress<Multi<T1, T2>>;

    impl<T1, T2> SecretMultiAddress<T1, T2> {
        pub async fn send_left(&self, msg: T1) -> Result<(), SendError> {
            self.secret.send(Multi::Left(msg)).await
        }

        pub async fn send_right(&self, msg: T2) -> Result<(), SendError> {
            self.secret.send(Multi::Right(msg)).await
        }

        pub async fn try_send_left(&self, msg: T1) -> Result<(), TrySendError> {
            self.secret.try_send(Multi::Left(msg))
        }

        pub async fn try_send_right(&self, msg: T2) -> Result<(), TrySendError> {
            self.secret.try_send(Multi::Right(msg))
        }

        pub async fn send_blocking_left(&self, msg: T1) -> Result<(), SendError> {
            self.secret.send_blocking(Multi::Left(msg))
        }

        pub async fn send_blocking_right(&self, msg: T2) -> Result<(), SendError> {
            self.secret.send_blocking(Multi::Right(msg))
        }
    }

    impl<A: Actor, S: Scope> GenericAddress<A, S> {
        pub fn secret<T>(&self) -> SecretAddress<T>
        where
            A: Receive<T>,
            S: CanSendTicket<A, T>,
        {
            let core = self.core.clone();
            // SAFETY: It's the phantomdata, i.e. T, that determines if this secret address can be
            // sent between threads or not. For a remote address, only T: Send is safe since the
            // address could, or is going to, change thread, and sending !Send data would not be
            // good in that case. A local address can send T: !Send, since it has never and will
            // never leave the current thread, the secret address will thus never be able to leave
            // the current thread either. It is safe though for a local address to send T: Send data
            // too, but that will make the secret address Send, so the wrapped local address will
            // thus be abe to move to another thread, but that is fine, since a local address can
            // trivially be converted to a remote address, but not the other way around. It's safe
            // to just take the local address and send it to another thread, because that is
            // basically what conversion to remote is.
            let scope = unsafe { UnsafeSendWrapper::new(PhantomData::<S>) };
            SecretAddress {
                secret: Box::new((scope, core)),
                _scope: PhantomData,
            }
        }

        pub fn secret_multi<T1, T2>(&self) -> SecretMultiAddress<T1, T2>
        where
            A: Receive<T1> + Receive<T2>,
            S: CanSendTicket<A, T1> + CanSendTicket<A, T2>,
        {
            let core = self.core.clone();
            // SAFETY: the same reasoning as Self::secret()
            let scope = unsafe { UnsafeSendWrapper::new(PhantomData::<S>) };
            SecretAddress {
                secret: Box::new((scope, core, ())),
                _scope: PhantomData,
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::super::*;
        use crate::deferred_span;

        #[test]
        fn multi_send_can_send_both_halves() {
            #[derive(Debug, Snafu, Clone)]
            #[snafu(display("counted this much {count}"))]
            struct Counter {
                count: i32,
            }

            struct Bob {
                counter: i32,
            }
            impl Actor for Bob {
                type Error = Counter;

                fn span(&self) -> DeferredSpan<'_> {
                    deferred_span!(Level::INFO, "bob")
                }

                async fn leave(self, _ctl: &mut Control<Self>) -> Result<(), Self::Error> {
                    Err(Counter {
                        count: self.counter,
                    })
                }
            }

            impl Receive<bool> for Bob {
                type Retval = ();

                async fn receive(&mut self, _msg: bool, _ctl: &mut Control<Self>) -> Self::Retval {
                    self.counter += 5;
                }
            }

            impl Receive<i32> for Bob {
                type Retval = ();

                async fn receive(&mut self, _msg: i32, _ctl: &mut Control<Self>) -> Self::Retval {
                    self.counter += 7;
                }
            }

            let main_stage = Stage::new();
            let weak_adr = {
                let bob_adr = main_stage.summon(Bob { counter: 0 });
                let multi_adr = bob_adr.secret_multi::<bool, i32>();

                multi_adr
                    .send_left(true)
                    .now_or_never()
                    .expect("the future is ready")
                    .expect("the send succeeded");

                multi_adr
                    .send_right(8)
                    .now_or_never()
                    .expect("the future is ready")
                    .expect("the send succeeded");

                bob_adr.downgrade()
            };

            main_stage.assert_plays_within(10);
            let res = weak_adr
                .wait()
                .now_or_never()
                .expect("this should resolve immediately");

            let err = res.expect_err("should have returned an err");
            match err {
                WaitError::Exited {
                    source: Counter { count },
                } => assert_eq!(count, 12),
                e => panic!("returned the wrong thing: {e:?}"),
            }
        }
    }
}

// TODO: use tracing and/or metrics crate to collect how full all mailboxes are and present in some
// way. Could be nice to see where the slow path is, or which actors are slow. Could also be used to
// more easily tweak mail box sizes.
async fn actor_main<A: Actor>(
    // NOTE: control is the first argument so it is dropped last
    mut ctl: Control<A>,
    mut actor: A,
    rcv: channel::Receiver<ErasedDeliverable<A>>,
    home_adr: Address<A>,
    yield_policy: YieldPolicy,
) {
    debug!("Enter");
    actor.enter(&mut ctl).instrument(debug_span!("enter")).await;
    drop(home_adr); // NOTE: to make sure the address is alive during enter

    enum Event<A: Actor> {
        Delivery(ErasedDeliverable<A>),
        LocalDelivery(ErasedLocalDeliverable<A>),
        Eof,
    }
    let mut mailbox = pin!(
        rcv.map(Event::Delivery)
            .chain(stream::once(async { Event::Eof }))
    );
    let mut poll_next = stream::PollNext::default();

    yield_guard(yield_policy, async |guard| {
        loop {
            let mut events = stream::select_with_strategy(
                mailbox.as_mut(),
                (&mut ctl.bg_jobs).map(Event::LocalDelivery),
                |()| poll_next.toggle(),
            );
            match events.next().await {
                None => {
                    trace!("No more events");
                    break;
                }
                Some(Event::Eof) => {
                    actor
                        .mailbox_eof(&mut ctl)
                        .instrument(debug_span!("eof"))
                        .await;
                }
                Some(Event::Delivery(_) | Event::LocalDelivery(_)) if ctl.drain_mailbox => {
                    trace!("Dropping a delivery");
                }
                Some(Event::Delivery(delivery)) => {
                    // NOTE: the deliver method is responsible for logging and adding spans, since
                    // it has the concrete types.
                    delivery.deliver(&mut actor, &mut ctl).await
                }
                Some(Event::LocalDelivery(delivery)) => {
                    // NOTE: the deliver method is responsible for logging and adding spans, since
                    // it has the concrete types.
                    delivery.deliver(&mut actor, &mut ctl).await
                }
            }

            guard.yield_point().await;
        }
    })
    .await;

    trace!("Before leave");
    let exit_reason = actor.leave(&mut ctl).instrument(debug_span!("leave")).await;
    // NOTE: this is logged before all runes and stuff have dropped, but whatever
    debug!(error = exit_reason.is_err(), "Died");

    ctl.exit_send
        .take()
        .expect("is only sent here")
        .broadcast(exit_reason);
}

#[derive(Debug, Snafu)]
pub enum StageError {
    #[snafu(display("One or more actors panicked"))]
    ActorPanic { source: PanicError },
    #[snafu(display("Abruptly interrupted"))]
    Interrupted,
}

#[derive(Clone)]
pub struct StageCore {
    idgen: IdGenerator,
}
assert_impl_all!(StageCore: Send, Sync);

pub struct Stage {
    ex: Rc<LocalExecutor<'static>>,
    actor_heart: Heart,
    actor_rune: Rune,
    idgen: IdGenerator,
    thread_root_span: Span,
    sig_bomb: Bomb,
    sig_guard: Option<StageGuard>,
}
assert_not_impl_any!(Stage: Send, Sync);

impl Stage {
    pub fn from_core(core: StageCore) -> Self {
        let (actor_heart, actor_rune) = heart::create();
        Self {
            ex: Rc::new(LocalExecutor::new()),
            actor_heart,
            actor_rune,
            sig_bomb: Bomb::new(),
            sig_guard: None,
            idgen: core.idgen,
            thread_root_span: Span::current(),
        }
    }

    pub fn new() -> Self {
        Self::from_core(StageCore {
            idgen: IdGenerator::new(),
        })
    }

    pub fn register_signals(&mut self, signals: impl AsRef<SigRegistry>) {
        assert!(self.sig_guard.is_none()); // TODO: solve this with type state or smth instead?
        let guard = signals.as_ref().register_stage(self.sig_bomb.get_switch());
        self.sig_guard = Some(guard);
    }

    pub fn core(&self) -> StageCore {
        StageCore {
            idgen: self.idgen.clone(),
        }
    }

    fn actor_builder<A: Actor>(&self) -> ActorBuilder<A> {
        ActorBuilder::new(
            Rc::clone(&self.ex),
            self.actor_rune.clone(),
            self.idgen.clone(),
            self.thread_root_span.clone(),
        )
    }

    pub fn summon<A: Actor>(&self, actor: impl IntoActor<A>) -> Address<A> {
        self.actor_builder().spawn(actor)
    }

    pub fn play(self) -> Result<(), StageError> {
        self.play_internal(|heart, ex, bomb| {
            block_on(ex.run(async {
                match bomb.attach_future(heart.wait()).await {
                    kill_switch::Tick::Tock(res) => res.context(ActorPanicSnafu),
                    kill_switch::Tick::Boom => InterruptedSnafu.fail(),
                }
            }))
        })
    }

    #[cfg(test)]
    pub(crate) fn assert_plays_within(self, ticks: usize) {
        self.play_internal(|heart, ex, _| {
            for _ in 0..ticks {
                if ex.is_empty() {
                    break;
                }
                if !ex.try_tick() {
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }
            }
            assert!(ex.is_empty(), "Did not finish within {ticks} ticks");

            heart
                .wait()
                .now_or_never()
                .expect("all actors should be dead")
                .context(ActorPanicSnafu)
        })
        .expect("Actor(s) panicked");
    }

    fn play_internal<F>(self, f: F) -> Result<(), StageError>
    where
        F: FnOnce(Heart, &Rc<LocalExecutor<'static>>, Bomb) -> Result<(), StageError>,
    {
        fn take_essentials_drop_the_rest(
            this: Stage,
        ) -> (Heart, Rc<LocalExecutor<'static>>, Bomb, Option<StageGuard>) {
            (this.actor_heart, this.ex, this.sig_bomb, this.sig_guard)
        }
        let (heart, ex, sig_bomb, sig_guard) = take_essentials_drop_the_rest(self);
        let _span = debug_span!("stage").entered();

        debug!(num_actors = heart.rune_count(), "Action!");
        let res = f(heart, &ex, sig_bomb);
        debug!(?res, "Exited");

        let executor = Rc::into_inner(ex).expect("There should only be one strong reference");
        drop(sig_guard);
        drop(executor);
        res
    }
}

impl Default for Stage {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // TODO: is this actually stage tests?
    mod simple {
        use super::*;

        #[test]
        fn no_actors_added() {
            let stage = Stage::new();
            assert!(stage.play().is_ok());
        }

        #[test]
        fn terminate_when_all_addresses_gone() {
            let stage = Stage::new();
            let (fut, _) = stage.actor_builder().no_spawn(DummyActor);
            let mut fut = pin!(fut);
            assert_future_ready!(fut);
        }

        #[test]
        fn terminate_when_all_addresses_gone_after_one_poll_ready() {
            let stage = Stage::new();
            let (fut, adr) = stage.actor_builder().no_spawn(DummyActor);
            let mut fut = pin!(fut);
            assert_future_pending!(fut);

            drop(adr);
            assert_future_ready!(fut);
        }

        #[test]
        fn the_exit_value_is_available_when_cloning_a_dead_address() {
            let stage = Stage::new();
            let (fut, adr) = stage.actor_builder().no_spawn(DummyActor);
            let mut fut = pin!(fut);

            let weak = adr.downgrade();
            drop(adr);

            assert_future_ready!(fut);
            assert!(weak.is_dead());

            let weak2 = weak.clone();
            assert_future_ready!(pin weak.wait(), x => matches!(x, Ok(())));
            assert_future_ready!(pin weak2.wait(), x => matches!(x, Ok(())));
        }
    }

    mod multi_thread {
        use super::*;
        use crate::utils::thread_info_span;
        use tracing::info;

        #[derive(Debug, Snafu, Clone, Default)]
        #[snafu(display("I am not sendable between threads"))]
        struct NotSend {
            inner: Rc<()>,
        }
        assert_not_impl_any!(NotSend: Send);

        #[test]
        fn basic_send() {
            struct Alice;
            impl Actor for Alice {
                type Error = Infallible;

                default_span!("alice");
            }
            impl Receive<i32> for Alice {
                type Retval = i32;

                async fn receive(&mut self, msg: i32, _ctl: &mut Control<Self>) -> Self::Retval {
                    msg * msg
                }
            }

            struct Bob {
                alice: RemoteAddress<Alice>,
            }
            impl Actor for Bob {
                type Error = Infallible;

                async fn enter(&mut self, _ctl: &mut Control<Self>) {
                    info!("sending to alice");
                    let reply = self.alice.send_receive(5).await.unwrap();
                    let reply = reply.await.unwrap();
                    assert_eq!(reply, 25);
                }

                default_span!("bob");
            }

            let _span = thread_info_span().entered();
            let main_stage = Stage::new();
            let alice_adr = main_stage.summon(Alice);

            let t1 = std::thread::spawn({
                let alice_adr = alice_adr.remote();
                let core = main_stage.core();
                || {
                    let _span = thread_info_span().entered();
                    let stage = Stage::from_core(core);
                    stage.summon(Bob { alice: alice_adr });
                    stage.assert_plays_within(100);
                }
            });

            drop(alice_adr);
            main_stage.assert_plays_within(100);
            t1.join().unwrap();
        }

        #[test]
        fn sendness_should_not_matter_when_not_used() {
            struct LocalAlice;
            impl Actor for LocalAlice {
                type Error = NotSend;

                default_span!("alice");
            }
            impl Receive<i32> for LocalAlice {
                type Retval = NotSend;

                async fn receive(&mut self, _msg: i32, _ctl: &mut Control<Self>) -> Self::Retval {
                    NotSend::default()
                }
            }

            let main_stage = Stage::new();
            let local_adr = main_stage.summon(LocalAlice);
            let remote_adr = local_adr.remote();

            // NOTE: the whole test is to see if this compiles even though the retval is non-send.
            let _fut = remote_adr.send(5);

            // NOTE: the other whole test is to see if this also compiles, even though error is
            // non-send.
            let _secret_adr = remote_adr.secret();
            // TODO: create ui_test, not trybuild, tests or smth to verify that intended usages work and unintended usages
            // don't work. There's a lot to keep track of now
            // TODO: more tests to make sure send is not required in cases where it isn't. There are many
            // types that needs to be conditional now, so it's hard to make sure everything is correct. It's
            // difficult though to make negative tests, i.e. cases that should fail to compile.
            // TODO: check if wait can be called on remote/local depending on if Actor::Error is send or not
            // TODO: check that secretsend can't be sent to another thread if it's local
            // TODO: check that a remote address can't send local messages
            // TODO: check whether retval sendness affects a oneway send
        }

        #[test]
        fn sendness_of_the_actor_itself_shouldnt_matter() {
            struct LocalAlice {
                _inner: NotSend,
            }
            impl Actor for LocalAlice {
                type Error = NoError;

                default_span!("alice");
            }
            assert_not_impl_any!(LocalAlice: Send);
            assert_impl_all!(RemoteAddress<LocalAlice>: Send);
            assert_not_impl_any!(Address<LocalAlice>: Send);
        }

        #[test]
        fn sendness_of_the_error_should_matter() {
            struct LocalAlice;
            impl Actor for LocalAlice {
                type Error = NotSend;

                default_span!("alice");
            }
            assert_impl_all!(LocalAlice: Send);
            assert_not_impl_any!(RemoteAddress<LocalAlice>: Send);
            assert_not_impl_any!(Address<LocalAlice>: Send);
        }
    }
}
