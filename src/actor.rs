use crate::{
    actor::{
        id_generator::{Id, IdGenerator},
        unsafe_wrapper::UnsafeSendWrapper,
    },
    heart::{self, Heart, PanicError, Rune},
    kill_switch::{self, Bomb, Switch},
    signals::InactiveSignalStream,
    stream_utils::{StreamExt as _, YieldPolicy, yield_guard},
};
use async_channel as channel;
use async_executor::{FallibleTask, LocalExecutor};
use async_io::block_on;
use futures_core::future::LocalBoxFuture;
use futures_util::{FutureExt as _, StreamExt as _, TryFutureExt};
use pin_project::pin_project;
use snafu::prelude::*;
use static_assertions::{assert_impl_all, assert_not_impl_any, const_assert_eq};
use std::{
    any::{Any, type_name},
    marker::PhantomData,
    pin::{Pin, pin},
    rc::{Rc, Weak},
    task::ready,
};
use tracing::{
    Instrument, Level, Span, debug, debug_span, field, instrument, span, trace, trace_span, warn,
};

// TODO: this module probably needs to be split up into several submodules, but it's super tedious
// and rust-analyzer isn't that big of a help.

// NOTE: Since the output of `type_name` usually is long, i only use it on spans of level trace and
// events of levels debug or higher (verbosity).

// NOTE: this is Sized because that is required when using Self in function arguments
// NOTE: this is 'static because basically every usage of an Actor requires 'static
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not an actor",
    label = "this guy right here",
    note = "implement `Actor`"
)]
pub trait Actor: Sized + 'static {
    const MAIL_BOX_SIZE: usize = 0;
    const YIELD_POLICY: YieldPolicy = YieldPolicy::default();

    fn span(&self, parent: &Span) -> Span;

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
    async fn leave(&mut self, ctl: &mut Control<Self>) {}

    #[expect(
        async_fn_in_trait,
        reason = "i don't really understand what this is complaining about"
    )]
    #[expect(unused_variables, reason = "the default is a noop")]
    async fn interrupted(&mut self, ctl: &mut Control<Self>) {
        warn!("Ignoring an interrupt");
    }

    #[expect(
        async_fn_in_trait,
        reason = "i don't really understand what this is complaining about"
    )]
    #[expect(unused_variables, reason = "the default is a noop")]
    // TODO: associate each task with a key or something? Theres no way to know which task was done
    async fn task_done(&mut self, ctl: &mut Control<Self>, panicked: bool) {
        if panicked {
            panic!("A background task panicked");
        }
    }
}

// NOTE: for static assertions
struct DummyActor;
impl Actor for DummyActor {
    fn span(&self, parent: &Span) -> Span {
        tracing::info_span!(parent: parent, "dummy")
    }
}

pub struct Control<A: Actor> {
    // NOTE: this could probably be a 'a, but I don't think i want that anyways. I think it would
    // pretty much mean all actors can borrow stack data from the main function.
    // NOTE: weak so the executor can drop itself even if there are actors still alive
    ex: Weak<LocalExecutor<'static>>,
    state: State,
    // NOTE: weak so the actor doesn't keep itself alive
    home: WeakAddress<A>,
    // NOTE: being inactive doesn't count towards this channel getting closed, so it won't get
    // closed on accident by only having inactive receivers
    signals: InactiveSignalStream,
    new_tasks: Vec<FallibleTask<()>>,
    task_bomb: Bomb,
    task_switch: Switch,
    idgen: IdGenerator,
    thread_root_span: Span,
    actor_span: Span,
    // NOTE: these are last so they are dropped last, right before the task terminates
    actor_rune: Rune,
    // NOTE: I can't use a copy of the receiver here for the purpose of tracking when the actor has
    // actually died, since a soft exit will close the channel, so it will appear dead long before
    // the task is actually dropped.
    #[expect(dead_code, reason = "this is only here as a guard")]
    address_switch: Switch,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Ord, PartialOrd)]
enum State {
    Running,
    SoftExiting,
    HardExiting,
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
    signals: InactiveSignalStream,
    idgen: IdGenerator,
    thread_root_span: Span,
}

const GROUP_KEY: &str = "group";

impl<A: Actor> ActorBuilder<A> {
    fn new(
        actor: A,
        ex: Rc<LocalExecutor<'static>>,
        rune: Rune,
        signals: InactiveSignalStream,
        idgen: IdGenerator,
        thread_root_span: Span,
    ) -> Self {
        Self {
            actor,
            ex,
            rune,
            signals,
            idgen,
            thread_root_span,
        }
    }

    fn create_channel() -> (AdrSnd<A>, AdrRcv<A>) {
        if A::MAIL_BOX_SIZE == 0 {
            channel::unbounded()
        } else {
            channel::bounded(A::MAIL_BOX_SIZE)
        }
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
        (bomb, switch): (Bomb, Switch),
        id: Id,
        group_id: Option<Id>,
    ) -> (impl Future<Output = ()>, Address<A>) {
        // SAFETY: it's always safe to create from a brand new sender, the problem is if it is a
        // sender from another address.
        let home = unsafe { Address::new(snd, bomb) };

        let span = {
            let span = Self::create_root_span(id, &self.thread_root_span);
            if let Some(group_id) = group_id {
                span.record(GROUP_KEY, group_id);
            }
            self.actor.span(&span)
        };

        let ctl = Control::new(
            Rc::downgrade(&self.ex),
            self.rune,
            home.downgrade(),
            self.signals,
            self.idgen,
            self.thread_root_span,
            span.clone(),
            switch,
        );

        let fut = actor_main(ctl, self.actor, rcv, home.clone()).instrument(span);
        (fut, home)
    }

    #[cfg(test)]
    pub(crate) fn no_spawn(self) -> (impl Future<Output = ()>, Address<A>) {
        let id = self.idgen.generate();
        debug!(id, "type" = type_name::<A>(), "Non-spawn new actor");
        self.raw(Self::create_channel(), kill_switch::create(), id, None)
    }

    fn spawn(self) -> Address<A> {
        let ex = Rc::clone(&self.ex);
        let id = self.idgen.generate();
        let (fut, adr) = self.raw(Self::create_channel(), kill_switch::create(), id, None);
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
        let bomb = kill_switch::create();
        let group_id = self.idgen.generate();
        debug!(
            group_id,
            additional,
            "type" = type_name::<A>(),
            "Spawn new multiplex actor"
        );

        for _ in 0..additional {
            let id = self.idgen.generate();
            let (fut, _) = self
                .clone()
                .raw(channel.clone(), bomb.clone(), id, Some(group_id));
            ex.spawn(fut).detach();
        }

        let (fut, adr) = self.raw(channel, bomb, group_id, Some(group_id));
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
            self.signals.clone(),
            self.idgen.clone(),
            self.thread_root_span.clone(),
        )
        .spawn()
    }

    fn new(
        ex: Weak<LocalExecutor<'static>>,
        rune: Rune,
        home: WeakAddress<A>,
        signals: InactiveSignalStream,
        idgen: IdGenerator,
        thread_root_span: Span,
        actor_span: Span,
        address_switch: Switch,
    ) -> Self {
        let (task_bomb, task_switch) = kill_switch::create();
        Self {
            ex,
            actor_rune: rune,
            state: State::Running,
            home,
            signals,
            new_tasks: Vec::new(),
            task_bomb,
            task_switch,
            idgen,
            thread_root_span,
            actor_span,
            address_switch,
        }
    }

    #[instrument(skip_all)]
    pub fn soft_exit(&mut self) {
        if self.state >= State::SoftExiting {
            return;
        }
        debug!("Soft exit commanded");
        self.state = State::SoftExiting;
        // NOTE: this will only fail if already closed
        if let Some(adr) = self.home.upgrade() {
            adr.sender.close();
        }
        self.task_switch.detonate();
    }

    #[instrument(skip_all)]
    pub fn hard_exit(&mut self) {
        if self.state >= State::HardExiting {
            return;
        }
        debug!("Hard exit commanded");
        self.soft_exit();
        self.state = State::HardExiting;
    }

    #[instrument(skip_all)]
    pub fn escalating_exit(&mut self) {
        match self.state {
            State::Running => self.soft_exit(),
            State::SoftExiting => self.hard_exit(),
            State::HardExiting => debug!("Already hard exiting"),
        }
        debug_assert!(self.is_exiting());
    }

    pub fn weak_address(&self) -> WeakAddress<A> {
        self.home.clone()
    }

    pub fn address(&self) -> Result<Address<A>, AddressClosedError> {
        // NOTE: This can fail if all addresses got dropped and/or a soft exit has been issued. It's
        // guaranteed to not fail in enter and message handlers, unless soft exiting.
        self.home.upgrade().context(AddressClosedSnafu {
            exiting: self.is_exiting(),
        })
    }

    pub fn is_exiting(&self) -> bool {
        self.state >= State::SoftExiting
    }

    // TODO: somehow get some kind of identifier for this job
    pub fn start_job<F, S>(&mut self, future: F, spanner: S)
    where
        F: AsyncFnOnce(Bomb) + 'static,
        S: FnOnce(&Span) -> Span,
    {
        debug!("type" = type_name::<F>(), "Starting a job");
        let span = spanner(&self.actor_span);

        let task = self
            .ex
            .upgrade()
            .expect("the executor is always alive here")
            .spawn(future(self.task_bomb.clone()).instrument(span))
            .fallible();

        self.new_tasks.push(task);
    }

    pub fn start_blocking_job<F, S>(&mut self, thunk: F, spanner: S)
    where
        F: FnOnce(Bomb) + Send + 'static,
        S: FnOnce(&Span) -> Span,
    {
        debug!("type" = type_name::<F>(), "Starting a blocking job");
        let task = blocking::unblock({
            let heart = self.task_bomb.clone();
            let span = spanner(&self.actor_span);
            move || span.in_scope(|| thunk(heart))
        })
        .fallible();
        self.new_tasks.push(task);
    }
}

#[derive(Debug, Snafu)]
#[snafu(display("The address is closed, exiting={exiting}"))]
pub struct AddressClosedError {
    exiting: bool,
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
    type Retval;

    #[expect(
        async_fn_in_trait,
        reason = "i don't really understand what this is complaining about"
    )]
    async fn receive(&mut self, msg: T, ctl: &mut Control<Self>) -> Self::Retval;
}

type ErasedDeliverable<A> = Box<dyn Deliverable<A> + Send>;

mod private {
    pub trait Sealed {}
}

pub trait Scope: private::Sealed + 'static {}

pub struct Local(PhantomData<Rc<()>>);
impl private::Sealed for Local {}
impl Scope for Local {}
assert_not_impl_any!(Local: Send, Sync);
const_assert_eq!(std::mem::size_of::<Local>(), 0);

pub struct Remote;
impl private::Sealed for Remote {}
impl Scope for Remote {}
assert_impl_all!(Remote: Send, Sync);
const_assert_eq!(std::mem::size_of::<Remote>(), 0);

pub struct GenericAddress<A: Actor, S: Scope> {
    sender: channel::Sender<ErasedDeliverable<A>>,
    bomb: Bomb,
    _scope: PhantomData<S>,
}

pub type Address<A> = GenericAddress<A, Local>;
assert_not_impl_any!(Address<DummyActor>: Send, Sync);

pub type RemoteAddress<A> = GenericAddress<A, Remote>;
assert_impl_all!(RemoteAddress<DummyActor>: Send, Sync);

pub struct GenericWeakAddress<A: Actor, S: Scope> {
    sender: channel::WeakSender<ErasedDeliverable<A>>,
    bomb: Bomb,
    _scope: PhantomData<S>,
}

pub type WeakAddress<A> = GenericWeakAddress<A, Local>;
assert_not_impl_any!(WeakAddress<DummyActor>: Send, Sync);

pub type RemoteWeakAddress<A> = GenericWeakAddress<A, Remote>;
assert_impl_all!(RemoteWeakAddress<DummyActor>: Send, Sync);

impl<A: Actor, S: Scope> Clone for GenericAddress<A, S> {
    fn clone(&self) -> Self {
        let c = self.sender.clone();
        let b = self.bomb.clone();
        // SAFETY: this gets the same scope as the original
        unsafe { Self::new(c, b) }
    }
}

impl<A: Actor, S: Scope> Clone for GenericWeakAddress<A, S> {
    fn clone(&self) -> Self {
        let c = self.sender.clone();
        let b = self.bomb.clone();
        // SAFETY: this gets the same scope as the original
        unsafe { Self::new(c, b) }
    }
}

impl<A: Actor, S: Scope> GenericAddress<A, S> {
    // SAFETY: this is unsafe because it's not safe to create a local address from a remote one,
    // which would enable !Send data change threads.
    unsafe fn new(sender: channel::Sender<ErasedDeliverable<A>>, bomb: Bomb) -> Self {
        Self {
            sender,
            bomb,
            _scope: PhantomData,
        }
    }

    pub fn downgrade(&self) -> GenericWeakAddress<A, S> {
        let c = self.sender.downgrade();
        let b = self.bomb.clone();
        // SAFETY: this gets the same scope as the original
        unsafe { GenericWeakAddress::new(c, b) }
    }

    /// Can't send any more messages, but the actor might still be alive
    pub fn is_closed(&self) -> bool {
        self.sender.is_closed()
    }

    /// The actor is dead
    pub fn is_dead(&self) -> bool {
        self.bomb.has_exploded()
    }

    /// Wait for the actor to die. Remember that this non-weak address is keeping the actor alive
    pub async fn wait(&self) {
        self.bomb.wait().await
    }
}

impl<A: Actor, S: Scope> GenericWeakAddress<A, S> {
    // SAFETY: this is unsafe because it's not safe to create a local address from a remote one,
    // which would enable !Send data change threads.
    unsafe fn new(sender: channel::WeakSender<ErasedDeliverable<A>>, bomb: Bomb) -> Self {
        Self {
            sender,
            bomb,
            _scope: PhantomData,
        }
    }

    pub fn upgrade(&self) -> Option<GenericAddress<A, S>> {
        self.sender
            .upgrade()
            // SAFETY: this gets the same scope as the original
            .map(|sender| {
                let b = self.bomb.clone();
                unsafe { GenericAddress::new(sender, b) }
            })
    }

    /// The actor is dead
    pub fn is_dead(&self) -> bool {
        self.bomb.has_exploded()
    }

    /// Wait for the actor to die.
    pub async fn wait(&self) {
        self.bomb.wait().await
    }
}

impl<A: Actor> Address<A> {
    pub fn remote(&self) -> RemoteAddress<A> {
        // SAFETY: It's safe to go from a local address to a remote one, but not the other way
        // around, since a remote address can only send data that is Send.
        let c = self.sender.clone();
        let b = self.bomb.clone();
        unsafe { RemoteAddress::new(c, b) }
    }
}

impl<A: Actor> WeakAddress<A> {
    pub fn remote(&self) -> RemoteWeakAddress<A> {
        // SAFETY: It's safe to go from a local address to a remote one, but not the other way
        // around, since a remote address can only send data that is Send.
        let c = self.sender.clone();
        let b = self.bomb.clone();
        unsafe { RemoteWeakAddress::new(c, b) }
    }
}

#[allow(
    private_bounds,
    reason = "CanSendPriv and all types it is using should be private"
)]
#[diagnostic::on_unimplemented(
    message = "can't send `{T}` to `{A}`",
    label = "here",
    note = "address scope is `{Self}`",
    note = "`{T}` must implement `Send` if it is sent across threads",
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
{
    fn erase_package(p: Package<T, A::Retval>) -> ErasedDeliverable<A> {
        Box::new(p)
    }

    fn erase_ticket(t: OneWayTicket<T>) -> ErasedDeliverable<A>
    where
        A: Receive<T>,
    {
        Box::new(t)
    }

    fn erase_address(a: GenericAddress<A, Self>) -> Box<dyn SecretSend<T> + Send>
    where
        Self: Sized,
        A: Receive<T>,
    {
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

    fn erase_ticket(t: OneWayTicket<T>) -> ErasedDeliverable<A>
    where
        A: Receive<T>,
    {
        // SAFETY: one of these can only be sent by a local address, which means the sender has
        // never left the current thread, which means it's safe to send non-send data on it.
        Box::new(unsafe { UnsafeSendWrapper::new(t) })
    }

    fn erase_address(a: GenericAddress<A, Self>) -> Box<dyn SecretSend<T> + Send>
    where
        Self: Sized,
        A: Receive<T>,
    {
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
            // NOTE: There was a problem where the channel would get closed before all messages in
            // it had been processed, which is bad since a message handler should be able to get an
            // address unless the actor is exiting. I could only think of two different ways to keep
            // the channel open if it had messages in its queue when all addresses were dropped:
            // wrapping the channel's control block in a mutex or by adding the number of messages
            // in the queue to the strong sender count. I guess i also could rewrite/mod the
            // concurrent queue that async-channels relies on, but that felt too far. The mutex
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

#[derive(Debug, Snafu)]
#[snafu(display("Could not send message, actor dead :("))]
pub struct SendError;

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
        T: 'static,
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
    // constrained to be Send. This probably requires another trait to solve.
    pub async fn send<T>(&self, msg: T) -> Result<(), SendError>
    where
        T: 'static,
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
        A: Receive<T, Retval: Send>,
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
    fn send(&self, msg: T) -> LocalBoxFuture<'_, Result<(), SendError>>;
    fn clone_secret_send(&self) -> Box<dyn SecretSend<T> + Send>;
}

impl<A, S, T> SecretSend<T> for GenericAddress<A, S>
where
    A: Receive<T>,
    T: 'static,
    S: CanSend<A, T>,
{
    fn send(&self, msg: T) -> LocalBoxFuture<'_, Result<(), SendError>> {
        self.send(msg).boxed_local()
    }

    fn clone_secret_send(&self) -> Box<dyn SecretSend<T> + Send> {
        let clone: Self = self.clone();
        S::erase_address(clone)
    }
}

impl<T, SS> SecretSend<T> for UnsafeSendWrapper<SS>
where
    SS: SecretSend<T>,
{
    fn send(&self, msg: T) -> LocalBoxFuture<'_, Result<(), SendError>> {
        self.inner().send(msg)
    }

    fn clone_secret_send(&self) -> Box<dyn SecretSend<T> + Send> {
        self.inner().clone_secret_send()
    }
}

impl<T> SecretSend<T> for channel::Sender<T>
where
    T: Send + 'static,
{
    fn send(&self, msg: T) -> LocalBoxFuture<'_, Result<(), SendError>> {
        self.send(msg).map_err(|_| SendError).boxed_local()
    }

    fn clone_secret_send(&self) -> Box<dyn SecretSend<T> + Send> {
        Box::new(self.clone())
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
}

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

    enum Event<A> {
        Delivery(ErasedDeliverable<A>),
        TaskDone(bool),
        Signal,
    }
    let mut events = {
        let signal_stream = ctl.signals.activate_cloned().map(|_| Event::<A>::Signal);
        let delivery_stream = rcv.map(Event::Delivery);
        let events = delivery_stream.with_future_group().addon(signal_stream);
        pin!(events)
    };

    yield_guard(A::YIELD_POLICY, async |guard| {
        loop {
            {
                let signal_stream = events.addon_ref().get_ref();
                let task_group = events.main_ref().group_ref();
                let delivery_stream = events.main_ref().stream_ref().get_ref();
                let delivery_len = delivery_stream.len();
                trace!(
                    tasks.len = task_group.len(),
                    events.len = delivery_len,
                    signals.len = signal_stream.len(),
                    addresses.count.estimation = delivery_stream.sender_count() - delivery_len,
                    mailbox.size = A::MAIL_BOX_SIZE,
                    "Awaiting next event"
                );
            }

            match events.as_mut().next().await {
                Some(Event::Signal) => {
                    trace!("Signal event");
                    actor
                        .interrupted(&mut ctl)
                        .instrument(debug_span!("interrupt"))
                        .await
                }
                Some(Event::Delivery(delivery)) => {
                    // NOTE: the deliver method is responsible for logging and adding spans, since
                    // it has the concrete types.
                    delivery.deliver(&mut actor, &mut ctl).await
                }
                Some(Event::TaskDone(panicked)) => {
                    trace!("Task done event");
                    actor
                        .task_done(&mut ctl, panicked)
                        .instrument(debug_span!("task_done", panicked))
                        .await
                }
                None => {
                    trace!("No more events");
                    break;
                }
            }

            for task in ctl.new_tasks.drain(..) {
                events.as_mut().mut_pin_main().mut_pin_group().push(
                    crate::stream_utils::FutureExt::map(task, |res| {
                        // NOTE: I never cancel tasks in this loop, so it being none must mean it
                        // panicked
                        Event::TaskDone(res.is_none())
                    }),
                );
            }

            if ctl.state == State::HardExiting {
                debug!("Hard exit, breaking loop");
                break;
            }

            guard.yield_point().await;
        }
    })
    .await;

    trace!("Cancelling all bg tasks");
    {
        let pinned_group_ref = events.mut_pin_main().mut_pin_group();
        let group_ref = Pin::into_inner(pinned_group_ref);
        let group = std::mem::take(group_ref);
        futures_util::stream::iter(group.into_iter().map(|m| m.into_future().cancel()))
            .for_each_concurrent(None, async |fut| {
                let _: Option<()> = fut.await;
            })
            .await;
    }

    trace!("Before leave");
    actor.leave(&mut ctl).instrument(debug_span!("leave")).await;
    debug!("Died"); // NOTE: this is logged before all runes and stuff have dropped, but whatever
}

#[derive(Clone)]
pub struct StageCore {
    signal_stream: InactiveSignalStream,
    idgen: IdGenerator,
}
assert_impl_all!(StageCore: Send, Sync);

pub struct Stage {
    ex: Rc<LocalExecutor<'static>>,
    heart: Heart,
    rune: Rune,
    signal_stream: InactiveSignalStream,
    idgen: IdGenerator,
    thread_root_span: Span,
}
assert_not_impl_any!(Stage: Send, Sync);

impl Stage {
    pub fn from_core(core: StageCore) -> Self {
        let (heart, rune) = heart::create();
        Self {
            ex: Rc::new(LocalExecutor::new()),
            heart,
            rune,
            signal_stream: core.signal_stream,
            idgen: core.idgen,
            thread_root_span: Span::current(),
        }
    }

    fn new(signal_stream: InactiveSignalStream) -> Self {
        Self::from_core(StageCore {
            signal_stream,
            idgen: IdGenerator::new(),
        })
    }

    pub fn without_signals() -> Self {
        Self::new(crate::signals::dummy_signal_stream())
    }

    pub fn with_signals(signals: &crate::signals::Signals) -> Self {
        Self::new(signals.inactive_signal_stream())
    }

    pub fn core(&self) -> StageCore {
        StageCore {
            signal_stream: self.signal_stream.clone(),
            idgen: self.idgen.clone(),
        }
    }

    fn actor_builder<A: Actor>(&self, actor: A) -> ActorBuilder<A> {
        ActorBuilder::new(
            actor,
            Rc::clone(&self.ex),
            self.rune.clone(),
            self.signal_stream.clone(),
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

    pub fn play(self) -> Result<(), PanicError> {
        self.play_internal(|heart, ex| block_on(ex.run(heart.wait())))
    }

    #[cfg(test)]
    pub(crate) fn assert_plays_within(self, ticks: usize) {
        self.play_internal(|heart, ex| {
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
        })
        .expect("Actor(s) panicked");
    }

    fn play_internal<F>(self, f: F) -> Result<(), PanicError>
    where
        F: FnOnce(Heart, &Rc<LocalExecutor<'static>>) -> Result<(), PanicError>,
    {
        fn take_essentials_drop_the_rest(this: Stage) -> (Heart, Rc<LocalExecutor<'static>>) {
            (this.heart, this.ex)
        }
        let (heart, ex) = take_essentials_drop_the_rest(self);
        let _span = debug_span!("stage").entered();

        debug!(num_actors = heart.rune_count(), "Action!");
        let res = f(heart, &ex);
        debug!(?res, "Exited");

        assert!(ex.is_empty());
        assert_eq!(Rc::strong_count(&ex), 1);
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
            let stage = Stage::without_signals();
            assert!(stage.play().is_ok());
        }

        #[test]
        fn terminate_when_all_addresses_gone() {
            let stage = Stage::without_signals();
            let (fut, _) = stage.actor_builder(DummyActor).no_spawn();
            let mut fut = pin!(fut);
            assert_future_ready!(fut);
        }

        #[test]
        fn terminate_when_all_addresses_gone_after_one_poll_ready() {
            let stage = Stage::without_signals();
            let (fut, adr) = stage.actor_builder(DummyActor).no_spawn();
            let mut fut = pin!(fut);
            assert_future_pending!(fut);

            drop(adr);
            assert_future_ready!(fut);
        }
    }

    mod multi_thread {
        use tracing::info;

        use super::*;
        use crate::utils::thread_info_span;

        struct Alice;
        impl Actor for Alice {
            fn span(&self, parent: &Span) -> Span {
                tracing::info_span!(parent: parent, "alice")
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
            async fn enter(&mut self, _ctl: &mut Control<Self>) {
                info!("sending to alice");
                let reply = self.alice.send_receive(5).await.unwrap();
                let reply = reply.await.unwrap();
                assert_eq!(reply, 25);
            }

            fn span(&self, parent: &Span) -> Span {
                tracing::info_span!(parent: parent, "bob")
            }
        }

        #[test]
        fn remote() {
            let _span = thread_info_span().entered();
            let main_stage = Stage::without_signals();
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
