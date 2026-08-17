use crate::{
    actor::{
        id_generator::{Id, IdGenerator},
        unsafe_wrapper::UnsafeSendWrapper,
    },
    heart::{self, Heart, PanicError, Rune},
    kill_switch::{self, Bomb, Switch},
    signals::{SigRegistry, StageGuard},
    stream_utils::{YieldPolicy, yield_guard},
    utils::DeferredSpan,
};
use async_broadcast as bhannel;
use async_channel as channel;
use async_executor::LocalExecutor;
use async_io::block_on;
use futures_core::future::LocalBoxFuture;
use futures_util::{
    FutureExt as _, StreamExt as _, TryFutureExt,
    stream::{self, FuturesUnordered},
};
use pin_project::pin_project;
use snafu::prelude::*;
use static_assertions::{assert_impl_all, assert_not_impl_any, const_assert_eq};
use std::{
    any::{Any, type_name},
    convert::Infallible,
    marker::PhantomData,
    num::NonZeroUsize,
    pin::{Pin, pin},
    rc::{Rc, Weak},
    task::ready,
};
use tracing::{Instrument, Level, Span, debug, debug_span, field, span, trace, trace_span};

// TODO: this module probably needs to be split up into several submodules, but it's super tedious
// and rust-analyzer isn't that big of a help.

// NOTE: Since the output of `type_name` usually is long, i only use it on spans of level trace and
// events of levels debug or higher (verbosity).

#[derive(Debug, Clone, Copy)]
pub enum MailBoxSize {
    Unbounded,
    Bounded(NonZeroUsize),
}

impl MailBoxSize {
    pub const fn default() -> Self {
        Self::Bounded(const { NonZeroUsize::new(16).unwrap() })
    }
}

// NOTE: this is Sized because that is required when using Self in function arguments
// NOTE: this is 'static because basically every usage of an Actor requires 'static
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not an actor",
    label = "this guy right here",
    note = "implement `Actor`"
)]
pub trait Actor: Sized + 'static {
    const MAIL_BOX_SIZE: MailBoxSize = MailBoxSize::default();
    const YIELD_POLICY: YieldPolicy = YieldPolicy::default();

    // RANT: these, annoyingly, can't have default values
    type Error: std::error::Error + Clone; // = Infallible; will never error

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
    async fn leave(&mut self, ctl: &mut Control<Self>) -> Result<(), Self::Error> {
        Ok(())
    }

    #[expect(
        async_fn_in_trait,
        reason = "i don't really understand what this is complaining about"
    )]
    #[expect(unused_variables, reason = "the default is a noop")]
    async fn mailbox_eof(&mut self, ctl: &mut Control<Self>) {
        debug!("The mailbox is closed and empty");
    }
}

// NOTE: for static assertions
struct DummyActor;
impl Actor for DummyActor {
    type Error = Infallible;

    fn span(&self) -> DeferredSpan<'_> {
        DeferredSpan::none()
    }
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
    // HACK: these are last so they are dropped last, right before the task terminates. The actor is
    // as dead as possible as this point. The absolute best thing would be to track the task handle
    // itself, but that wasn't as easy.
    actor_rune: Rune,
    exit_send: ExitSend<A::Error>,
}

mod id_generator {
    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;

    pub type Id = u64;

    #[derive(Clone)]
    pub struct IdGenerator {
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

type AdrSnd<A> = channel::Sender<ErasedDeliverable<A>>;
type AdrRcv<A> = channel::Receiver<ErasedDeliverable<A>>;

#[derive(Clone)]
struct ActorBuilder<A: Actor> {
    actor: A,
    ex: Rc<LocalExecutor<'static>>,
    rune: Rune,
    idgen: IdGenerator,
    thread_root_span: Span,
}

const GROUP_KEY: &str = "group";

impl<A: Actor> ActorBuilder<A> {
    fn new(
        actor: A,
        ex: Rc<LocalExecutor<'static>>,
        rune: Rune,
        idgen: IdGenerator,
        thread_root_span: Span,
    ) -> Self {
        Self {
            actor,
            ex,
            rune,
            idgen,
            thread_root_span,
        }
    }

    fn create_channel() -> (AdrSnd<A>, AdrRcv<A>) {
        match A::MAIL_BOX_SIZE {
            MailBoxSize::Unbounded => channel::unbounded(),
            MailBoxSize::Bounded(size) => channel::bounded(size.get()),
        }
    }

    fn create_exit_channel() -> (ExitSend<A::Error>, ExitRecv<A::Error>) {
        // NOTE: up to one message will be sent on this, so the overflow flag doesn't matter.
        // NOTE: the receiver will never be inactive, so the await_active flag doesn't matter
        bhannel::broadcast(1)
    }

    fn create_root_span(id: Id, parent: &Span) -> Span {
        span!(
            parent: parent,
            Level::DEBUG,
            "actor",
            id,
            {GROUP_KEY} = field::Empty,
        )
    }

    fn raw(
        self,
        (snd, rcv): (AdrSnd<A>, AdrRcv<A>),
        (exit_snd, exit_rcv): (ExitSend<A::Error>, ExitRecv<A::Error>),
        id: Id,
        group_id: Option<Id>,
    ) -> (impl Future<Output = ()>, Address<A>) {
        // SAFETY: it's always safe to create from a brand new sender, the problem is if it is a
        // sender from another address.
        let home = unsafe { Address::new(snd, exit_rcv) };

        let span = {
            let span = Self::create_root_span(id, &self.thread_root_span);
            if let Some(group_id) = group_id {
                span.record(GROUP_KEY, group_id);
            }

            let span = if span.is_disabled() {
                self.thread_root_span.clone()
            } else {
                span
            };
            self.actor.span().create_or_parent(span)
        };

        let ctl = Control::new(
            Rc::downgrade(&self.ex),
            self.rune,
            home.downgrade(),
            self.idgen,
            self.thread_root_span,
            span.clone(),
            exit_snd,
        );

        // NOTE: I am pretty certain that if span is thread_root_span here it's actually redundant
        // cuz it gets entered twice, but i don't feel like that special case is worth worrying
        // about. This is of course only the case if the future is spawned as a task on an executor
        // that is blocked on its event loop where thread_root_span is active.
        let fut = actor_main(ctl, self.actor, rcv, home.clone()).instrument(span);
        (fut, home)
    }

    #[cfg(test)]
    pub(crate) fn no_spawn(self) -> (impl Future<Output = ()>, Address<A>) {
        let id = self.idgen.generate();
        debug!(id, "type" = type_name::<A>(), "Non-spawn new actor");
        self.raw(
            Self::create_channel(),
            Self::create_exit_channel(),
            id,
            None,
        )
    }

    fn spawn(self) -> Address<A> {
        let ex = Rc::clone(&self.ex);
        let id = self.idgen.generate();
        let (fut, adr) = self.raw(
            Self::create_channel(),
            Self::create_exit_channel(),
            id,
            None,
        );
        debug!(id, "type" = type_name::<A>(), "Spawn new actor");
        ex.spawn(fut).detach();
        adr
    }

    fn spawn_multiplex(self, additional: usize) -> Address<A>
    where
        A: Clone,
    {
        let ex = Rc::clone(&self.ex);
        let channel = Self::create_channel();
        let exit_channel = Self::create_exit_channel();
        let group_id = self.idgen.generate();
        debug!(
            group_id,
            additional,
            "type" = type_name::<A>(),
            "Spawn new multiplex actor"
        );

        for _ in 0..additional {
            let id = self.idgen.generate();
            let (fut, _) =
                self.clone()
                    .raw(channel.clone(), exit_channel.clone(), id, Some(group_id));
            ex.spawn(fut).detach();
        }

        let (fut, adr) = self.raw(channel, exit_channel, group_id, Some(group_id));
        ex.spawn(fut).detach();
        adr
    }
}

impl<A: Actor> Control<A> {
    pub fn summon<A2>(&self, actor: A2) -> Address<A2>
    where
        A2: Actor,
    {
        let ex = self
            .ex
            .upgrade()
            .expect("the executor is always alive here, it's what is running this function");

        ActorBuilder::new(
            actor,
            ex,
            self.actor_rune.clone(),
            self.idgen.clone(),
            self.thread_root_span.clone(),
        )
        .spawn()
    }

    fn new(
        ex: Weak<LocalExecutor<'static>>,
        rune: Rune,
        home: WeakAddress<A>,
        idgen: IdGenerator,
        thread_root_span: Span,
        actor_span: Span,
        exit_send: ExitSend<A::Error>,
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
            exit_send,
        }
    }

    // TODO: and create a close_and_clear_mail_box
    pub fn close_mailbox(&mut self) {
        trace!("Closing the mailbox");
        // NOTE: this will only fail if already closed
        if let Some(adr) = self.home.upgrade() {
            adr.sender.close();
        }
    }

    pub fn close_and_clear_mail_box(&mut self) {
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
pub enum WaitError<T: std::error::Error + 'static> {
    #[snafu(display("No error available, the actor must have panicked"))]
    Panicked,
    #[snafu(display("Actor exited with an error: {source}"))]
    Exited { source: T },
}

type ExitRecv<E> = bhannel::Receiver<Result<(), E>>;
type ExitSend<E> = bhannel::Sender<Result<(), E>>;

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

pub struct GenericAddress<A: Actor, S: Scope> {
    sender: channel::Sender<ErasedDeliverable<A>>,
    status: ExitRecv<A::Error>,
    _scope: PhantomData<S>,
}

pub type Address<A> = GenericAddress<A, Local>;
assert_not_impl_any!(Address<DummyActor>: Send, Sync);

pub type RemoteAddress<A> = GenericAddress<A, Remote>;
assert_impl_all!(RemoteAddress<DummyActor>: Send, Sync);

pub struct GenericWeakAddress<A: Actor, S: Scope> {
    sender: channel::WeakSender<ErasedDeliverable<A>>,
    status: ExitRecv<A::Error>,
    _scope: PhantomData<S>,
}

pub type WeakAddress<A> = GenericWeakAddress<A, Local>;
assert_not_impl_any!(WeakAddress<DummyActor>: Send, Sync);

pub type RemoteWeakAddress<A> = GenericWeakAddress<A, Remote>;
assert_impl_all!(RemoteWeakAddress<DummyActor>: Send, Sync);

impl<A: Actor, S: Scope> Clone for GenericAddress<A, S> {
    fn clone(&self) -> Self {
        let c = self.sender.clone();
        let b = self.status.clone();
        // SAFETY: this gets the same scope as the original
        unsafe { Self::new(c, b) }
    }
}

impl<A: Actor, S: Scope> Clone for GenericWeakAddress<A, S> {
    fn clone(&self) -> Self {
        let c = self.sender.clone();
        let b = self.status.clone();
        // SAFETY: this gets the same scope as the original
        unsafe { Self::new(c, b) }
    }
}

async fn wait_on_exit_recv<A: Actor>(
    exit_recv: &mut ExitRecv<A::Error>,
) -> Result<(), WaitError<A::Error>> {
    let actor_res = match exit_recv.recv_direct().await {
        Ok(x) => x,
        Err(async_broadcast::RecvError::Closed) => return PanickedSnafu.fail(),
        Err(async_broadcast::RecvError::Overflowed(_)) => panic!("this can't overflow"),
    };
    actor_res.context(ExitedSnafu)
}

fn actor_is_dead<A: Actor>(exit_recv: &ExitRecv<A::Error>) -> bool {
    // NOTE: there is only supposed to be one sender but many receivers, there is one receiver
    // here, so if the channel is closed it must mean that the actor dropped its handle, hence
    // it has died.
    debug_assert!(exit_recv.sender_count() <= 1);
    debug_assert!(exit_recv.receiver_count() >= 1);
    exit_recv.is_closed()
}

impl<A: Actor, S: Scope> GenericAddress<A, S> {
    // SAFETY: this is unsafe because it's not safe to create a local address from a remote one,
    // which would enable !Send data change threads.
    unsafe fn new(
        sender: channel::Sender<ErasedDeliverable<A>>,
        status: ExitRecv<A::Error>,
    ) -> Self {
        Self {
            sender,
            status,
            _scope: PhantomData,
        }
    }

    pub fn downgrade(&self) -> GenericWeakAddress<A, S> {
        let c = self.sender.downgrade();
        let b = self.status.clone();
        // SAFETY: this gets the same scope as the original
        unsafe { GenericWeakAddress::new(c, b) }
    }

    /// Can't send any more messages, but the actor might still be alive
    pub fn is_closed(&self) -> bool {
        self.sender.is_closed()
    }

    /// The actor is dead
    pub fn is_dead(&self) -> bool {
        actor_is_dead::<A>(&self.status)
    }

    /// Wait for the actor to die. Remember that this non-weak address is keeping the actor alive.
    /// This will return some arbitrary error if used after the exit reason has been awaited once
    /// already.
    pub async fn wait(&mut self) -> Result<(), WaitError<A::Error>> {
        // TODO: shouldn't this require A::Error to be Send if the scope is remote? Or is this
        // required to always be send or something? A local address shouldn't care.
        wait_on_exit_recv::<A>(&mut self.status).await
    }
}

impl<A: Actor, S: Scope> GenericWeakAddress<A, S> {
    // SAFETY: this is unsafe because it's not safe to create a local address from a remote one,
    // which would enable !Send data change threads.
    unsafe fn new(
        sender: channel::WeakSender<ErasedDeliverable<A>>,
        status: ExitRecv<A::Error>,
    ) -> Self {
        Self {
            sender,
            status,
            _scope: PhantomData,
        }
    }

    pub fn upgrade(&self) -> Option<GenericAddress<A, S>> {
        self.sender
            .upgrade()
            // SAFETY: this gets the same scope as the original
            .map(|sender| {
                let b = self.status.clone();
                unsafe { GenericAddress::new(sender, b) }
            })
    }

    /// The actor is dead
    pub fn is_dead(&self) -> bool {
        actor_is_dead::<A>(&self.status)
    }

    /// Wait for the actor to die.
    pub async fn wait(&mut self) -> Result<(), WaitError<A::Error>> {
        wait_on_exit_recv::<A>(&mut self.status).await
    }
}

impl<A: Actor> Address<A> {
    pub fn remote(&self) -> RemoteAddress<A> {
        // SAFETY: It's safe to go from a local address to a remote one, but not the other way
        // around, since a remote address can only send data that is Send.
        let c = self.sender.clone();
        let b = self.status.clone();
        unsafe { RemoteAddress::new(c, b) }
    }
}

impl<A: Actor> WeakAddress<A> {
    pub fn remote(&self) -> RemoteWeakAddress<A> {
        // SAFETY: It's safe to go from a local address to a remote one, but not the other way
        // around, since a remote address can only send data that is Send.
        let c = self.sender.clone();
        let b = self.status.clone();
        unsafe { RemoteWeakAddress::new(c, b) }
    }
}

#[expect(
    private_bounds,
    reason = "CanSendPriv and all types it is using should be private"
)]
#[diagnostic::on_unimplemented(
    message = "can't send `{T}` to `{A}`",
    label = "here",
    note = "address scope is `{Self}`",
    note = "`{T}` must implement `Send` if it is sent across threads",
    note = "All (most) associated types of Actor must implement `Send`",
    note = "`{T}` must be `'static`, it can't borrow anything",
    note = "`{A}` must be able to receive `{T}`"
)]
pub trait CanSend<A, T>: CanSendPriv<A, T> {}
#[diagnostic::do_not_recommend]
impl<A, T, X> CanSend<A, T> for X where X: CanSendPriv<A, T> {}

trait CanSendPriv<A, T>: Scope {
    fn erase_package(p: Package<T, A::Retval>) -> ErasedDeliverable<A>
    where
        A: Receive<T>;
    fn erase_ticket(t: OneWayTicket<T>) -> ErasedDeliverable<A>
    where
        A: Receive<T>;
    fn erase_address(a: GenericAddress<A, Self>) -> Box<dyn SecretSend<T> + Send>
    where
        Self: Sized,
        A: Receive<T>;
}

impl<A, T> CanSendPriv<A, T> for Remote
where
    T: Send + 'static,
    A: Receive<T>,
    A::Retval: Send,
    A::Error: Send, // TODO: this shouldn't need to be here, this has nothing to do with the message
                    // being sent. This maybe fixes itself together with the TODO in the
                    // erase_address function, which is the one requiring this.
{
    fn erase_package(p: Package<T, A::Retval>) -> ErasedDeliverable<A> {
        Box::new(p)
    }

    fn erase_ticket(t: OneWayTicket<T>) -> ErasedDeliverable<A> {
        Box::new(t)
    }

    fn erase_address(a: GenericAddress<A, Self>) -> Box<dyn SecretSend<T> + Send> {
        Box::new(a)
    }
}

impl<A, T> CanSendPriv<A, T> for Local
where
    A: Receive<T>,
    T: 'static,
{
    fn erase_package(p: Package<T, A::Retval>) -> ErasedDeliverable<A> {
        // SAFETY: one of these can only be sent by a local address, which means the sender has
        // never left the current thread, which means it's safe to send non-send data on it.
        Box::new(unsafe { UnsafeSendWrapper::new(p) })
    }

    fn erase_ticket(t: OneWayTicket<T>) -> ErasedDeliverable<A> {
        // SAFETY: one of these can only be sent by a local address, which means the sender has
        // never left the current thread, which means it's safe to send non-send data on it.
        Box::new(unsafe { UnsafeSendWrapper::new(t) })
    }

    fn erase_address(a: GenericAddress<A, Self>) -> Box<dyn SecretSend<T> + Send> {
        // SAFETY: This is only used to copy something that already was in a box<dyn ...>, so the
        // thing has already been verified to be Send elsewhere. This is mainly here to only wrap in
        // the unsafe send wrapper when necessary.
        // TODO: maybe the argument to this function can be something else to make sure this
        // function is only used in this safe scenario?
        Box::new(unsafe { UnsafeSendWrapper::new(a) })
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
    erased: ErasedAddress,
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
            msg = type_name::<Self>(),
            return = type_name::<A::Retval>(),
            actor = type_name::<A>()
        ))
        .boxed_local()
    }
}

struct OneWayTicket<T> {
    msg: T,
    erased: ErasedAddress,
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
        .instrument(trace_span!(
            "oneway",
            msg = type_name::<Self>(),
            actor = type_name::<A>()
        ))
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
        fn start<A, O>(self, ctl: &mut Control<A>)
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
        fn start<A>(self, ctl: &mut Control<A>)
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
                const YIELD_POLICY: YieldPolicy = YieldPolicy::Never;

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

#[derive(Debug, Snafu)]
#[snafu(display("Could not send message, actor dead :("))]
// TODO: make these send errors return the value that was attempted to be sent? It's difficult to
// get them back since they are erased in a Box, but it should be possible to downcast them back i
// think.
pub struct SendError;

#[derive(Debug, Snafu)]
#[snafu(module, context(suffix(false)))]
pub enum TrySendError {
    #[snafu(display("Could not send message, mailbox full"))]
    Full,
    #[snafu(display("Could not send message, actor dead :("))]
    Closed,
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
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.project();
        let reply = ready!(this.recv.poll(cx));
        std::task::Poll::Ready(reply.ok().context(ReplySnafu))
    }
}

impl<A: Actor, S: Scope> GenericAddress<A, S> {
    pub async fn send_receive<T>(&self, msg: T) -> Result<Reply<A::Retval>, SendError>
    where
        A: Receive<T>,
        S: CanSend<A, T>,
    {
        trace!(
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
        self.sender.send(erased).await.ok().context(SendSnafu)?;
        Ok(Reply { recv: ret_rcv })
    }

    // TODO: the return value is constrained too much here. No value is ever sent, but it's still
    // constrained to be Send. This probably requires another trait to solve. Not sure how big of a
    // problem this is though, I imagine that most messages sent with this will have retval = (), so
    // it doesn't matter. This should maybe even require the retval on receive to be ()? Isn't it
    // weird to define a return value and then ignore it?
    pub async fn send<T>(&self, msg: T) -> Result<(), SendError>
    where
        A: Receive<T>,
        S: CanSend<A, T>,
    {
        trace!(
            "to" = type_name::<A>(),
            "msg" = type_name::<T>(),
            "Send message"
        );
        let ticket = OneWayTicket {
            msg,
            erased: self.erased(),
        };
        let erased = S::erase_ticket(ticket);
        self.sender.send(erased).await.ok().context(SendSnafu)?;
        Ok(())
    }

    pub fn try_send<T>(&self, msg: T) -> Result<(), TrySendError>
    where
        A: Receive<T>,
        S: CanSend<A, T>,
    {
        trace!(
            "to" = type_name::<A>(),
            "msg" = type_name::<T>(),
            "Try send message"
        );
        let ticket = OneWayTicket {
            msg,
            erased: self.erased(),
        };
        let erased = S::erase_ticket(ticket);
        self.sender.try_send(erased).map_err(|err| match err {
            async_channel::TrySendError::Full(_) => try_send_error::Full.build(),
            async_channel::TrySendError::Closed(_) => try_send_error::Closed.build(),
        })
    }

    pub fn send_blocking<T>(&self, msg: T) -> Result<(), SendError>
    where
        A: Receive<T>,
        S: CanSend<A, T>,
    {
        trace!(
            "to" = type_name::<A>(),
            "msg" = type_name::<T>(),
            "Send blocking message"
        );
        let ticket = OneWayTicket {
            msg,
            erased: self.erased(),
        };
        let erased = S::erase_ticket(ticket);
        self.sender.send_blocking(erased).ok().context(SendSnafu)?;
        Ok(())
    }

    fn erased(&self) -> ErasedAddress {
        let clone = self.clone();
        ErasedAddress {
            // SAFETY: this is never accessed, it's only here to keep the channel alive, so nothing
            // is ever sent on this channel
            _inner: Box::new(unsafe { UnsafeSendWrapper::new(clone) }),
        }
    }
}

// TODO: merge these with the other impls for specific addresses?
impl<A: Actor> GenericAddress<A, Remote> {
    pub fn secret<T>(&self) -> SecretAddress<T>
    where
        T: Send + 'static,
        A: Receive<T, Retval: Send, Error: Send>,
    {
        SecretAddress {
            secret: Box::new(self.clone()),
            _scope: PhantomData,
        }
    }
}

impl<A: Actor> GenericAddress<A, Local> {
    pub fn secret<T>(&self) -> SecretAddress<T>
    where
        T: 'static,
        A: Receive<T>,
    {
        SecretAddress {
            // SAFETY: It's the phantomdata, i.e. T, that determines if this secret address can be
            // sent between threads or not. For a remote address, only T: Send is safe since the
            // address could, or is going to, change thread, and sending !Send data would not be
            // good in that case. A local address can send T: !Send, since it has never and will
            // never leave the current thread, the secret address will this never be able to leave
            // the current thread either. It is safe though for a local address to send T: Send data
            // too, but that will make the secret address Send, so the wrapped local address will
            // thus be abe to move to another thread, but that is fine, since a local address can
            // trivially be converted to a remote address, but not the other way around. It's safe
            // to just take the local address and send it to another thread, because that is
            // basically what conversion to remote is.
            secret: Box::new(unsafe { UnsafeSendWrapper::new(self.clone()) }),
            _scope: PhantomData,
        }
    }
}

struct ErasedAddress {
    _inner: Box<dyn Any + Send>,
}

impl<T> Clone for Box<dyn SecretSend<T> + Send> {
    fn clone(&self) -> Self {
        self.clone_secret_send()
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

impl<A, S, T> SecretSend<T> for GenericAddress<A, S>
where
    A: Receive<T>,
    S: CanSend<A, T>,
{
    fn send<'a>(&'a self, msg: T) -> LocalBoxFuture<'a, Result<(), SendError>>
    where
        T: 'a,
    {
        self.send(msg).boxed_local()
    }

    fn clone_secret_send(&self) -> Box<dyn SecretSend<T> + Send> {
        let clone: Self = self.clone();
        S::erase_address(clone)
    }

    fn try_send(&self, msg: T) -> Result<(), TrySendError> {
        self.try_send(msg)
    }

    fn send_blocking(&self, msg: T) -> Result<(), SendError> {
        self.send_blocking(msg)
    }
}

impl<T, SS> SecretSend<T> for UnsafeSendWrapper<SS>
where
    SS: SecretSend<T>,
{
    fn send<'a>(&'a self, msg: T) -> LocalBoxFuture<'a, Result<(), SendError>>
    where
        T: 'a,
    {
        self.inner().send(msg)
    }

    fn clone_secret_send(&self) -> Box<dyn SecretSend<T> + Send> {
        self.inner().clone_secret_send()
    }

    fn try_send(&self, msg: T) -> Result<(), TrySendError> {
        self.inner().try_send(msg)
    }

    fn send_blocking(&self, msg: T) -> Result<(), SendError> {
        self.inner().send_blocking(msg)
    }
}

impl<T> SecretSend<T> for channel::Sender<T>
where
    T: Send + 'static,
{
    fn send<'a>(&'a self, msg: T) -> LocalBoxFuture<'a, Result<(), SendError>>
    where
        T: 'a,
    {
        self.send(msg).map_err(|_| SendError).boxed_local()
    }

    fn clone_secret_send(&self) -> Box<dyn SecretSend<T> + Send> {
        Box::new(self.clone())
    }

    fn try_send(&self, msg: T) -> Result<(), TrySendError> {
        self.try_send(msg).map_err(|err| match err {
            async_channel::TrySendError::Full(_) => try_send_error::Full.build(),
            async_channel::TrySendError::Closed(_) => try_send_error::Closed.build(),
        })
    }

    fn send_blocking(&self, msg: T) -> Result<(), SendError> {
        self.send_blocking(msg).map_err(|_| SendError)
    }
}

// TODO: create a weak secret address?
pub struct SecretAddress<T> {
    secret: Box<dyn SecretSend<T> + Send>,
    _scope: PhantomData<T>,
}
assert_not_impl_any!(SecretAddress<Rc<()>>: Send);
assert_impl_all!(SecretAddress<()>: Send);

impl<T> SecretAddress<T> {
    #[cfg(test)]
    pub(crate) fn new_channel() -> (Self, channel::Receiver<T>)
    where
        T: Send + 'static,
    {
        let (snd, rcv) = channel::unbounded();
        let adr = Self {
            secret: Box::new(snd),
            _scope: PhantomData,
        };
        (adr, rcv)
    }
}

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

    yield_guard(A::YIELD_POLICY, async |guard| {
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
    debug!(?exit_reason, "Died");

    use async_broadcast::TrySendError;
    match ctl.exit_send.try_broadcast(exit_reason) {
        Ok(None) | Err(TrySendError::Closed(_)) => (),
        Ok(Some(_)) | Err(TrySendError::Full(_)) | Err(TrySendError::Inactive(_)) => {
            panic!("should not happen")
        }
    }
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

    pub fn register_signals(&mut self, sig_reg: &SigRegistry) {
        assert!(self.sig_guard.is_none()); // TODO: solve this with type state or smth instead?
        let guard = sig_reg.register_stage(self.sig_bomb.get_switch());
        self.sig_guard = Some(guard);
    }

    pub fn core(&self) -> StageCore {
        StageCore {
            idgen: self.idgen.clone(),
        }
    }

    fn actor_builder<A: Actor>(&self, actor: A) -> ActorBuilder<A> {
        ActorBuilder::new(
            actor,
            Rc::clone(&self.ex),
            self.actor_rune.clone(),
            self.idgen.clone(),
            self.thread_root_span.clone(),
        )
    }

    pub fn summon<A: Actor>(&self, actor: A) -> Address<A> {
        self.actor_builder(actor).spawn()
    }

    pub fn summon_multiplex<A: Actor + Clone>(&self, actor: A, additional: usize) -> Address<A> {
        self.actor_builder(actor).spawn_multiplex(additional)
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

#[cfg(test)]
mod tests {
    use super::*;

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
            let (fut, _) = stage.actor_builder(DummyActor).no_spawn();
            let mut fut = pin!(fut);
            assert_future_ready!(fut);
        }

        #[test]
        fn terminate_when_all_addresses_gone_after_one_poll_ready() {
            let stage = Stage::new();
            let (fut, adr) = stage.actor_builder(DummyActor).no_spawn();
            let mut fut = pin!(fut);
            assert_future_pending!(fut);

            drop(adr);
            assert_future_ready!(fut);
        }
    }

    // TODO: more tests to make sure send is not required in cases where it isn't. There are many
    // types that needs to be conditional now, so it's hard to make sure everything is correct. It's
    // difficult though to make negative tests, i.e. cases that should fail to compile.
    mod multi_thread {
        use super::*;
        use crate::{deferred_span, utils::thread_info_span};
        use tracing::info;

        #[test]
        fn remote() {
            struct Alice;
            impl Actor for Alice {
                type Error = Infallible;
                const YIELD_POLICY: YieldPolicy = YieldPolicy::Never;

                fn span(&self) -> DeferredSpan<'_> {
                    deferred_span!(Level::INFO, "alice")
                }
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
                const YIELD_POLICY: YieldPolicy = YieldPolicy::Never;

                async fn enter(&mut self, _ctl: &mut Control<Self>) {
                    info!("sending to alice");
                    let reply = self.alice.send_receive(5).await.unwrap();
                    let reply = reply.await.unwrap();
                    assert_eq!(reply, 25);
                }

                fn span(&self) -> DeferredSpan<'_> {
                    deferred_span!(Level::INFO, "bob")
                }
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
    }
}
