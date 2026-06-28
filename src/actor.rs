use crate::{
    graceful_termination::InactiveSignalStream,
    heart::{self, Heart, Rune},
    stream_utils::{StreamExt as _, YieldPolicy, yield_guard},
};
use async_channel as channel;
use async_executor::{LocalExecutor, Task};
use async_io::block_on;
use futures_core::future::LocalBoxFuture;
use futures_util::{FutureExt as _, StreamExt as _};
use pin_project::pin_project;
use snafu::prelude::*;
use static_assertions::{assert_impl_all, assert_not_impl_any};
use std::{
    marker::PhantomData,
    pin::{Pin, pin},
    rc::{Rc, Weak},
    task::ready,
};

// NOTE: this is Sized because that is required when using Self in function arguments
// NOTE: this is 'static because basically every usage of an Actor requires 'static
pub trait Actor: Sized + 'static {
    const MAIL_BOX_SIZE: usize = 0;
    const YIELD_POLICY: YieldPolicy = YieldPolicy::default();

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
    async fn interrupt(&mut self, ctl: &mut Control<Self>) {}
}

// NOTE: for static assertions
struct DummyActor;
impl Actor for DummyActor {}

// TODO: create a function to spawn tasks and save them in a futuregroup
pub struct Control<A: Actor> {
    // NOTE: this could probably be a 'a, but I don't think i want that anyways. I think it would
    // pretty much mean all actors can borrow stack data from the main function.
    // NOTE: weak so the executor can drop itself even if there are actors still alive
    ex: Weak<LocalExecutor<'static>>,
    state: State,
    actor_rune: Rune,
    // NOTE: weak so the actor doesn't keep itself alive
    home: WeakAddress<A>,
    // NOTE: being inactive doesn't count towards this channel getting closed, so it won't get
    // closed on accident by only having inactive receivers
    signals: InactiveSignalStream,
    new_tasks: Vec<Task<()>>,
    task_heart: Heart,
    task_rune: Rune,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum State {
    Running,
    SoftExiting,
    HardExiting,
}

fn summon_raw<A>(
    actor: A,
    ex: Weak<LocalExecutor<'static>>,
    rune: Rune,
    signals: InactiveSignalStream,
) -> (impl Future, Address<A>)
where
    A: Actor,
{
    let (local_snd, local_rcv) = if A::MAIL_BOX_SIZE == 0 {
        channel::unbounded()
    } else {
        channel::bounded(A::MAIL_BOX_SIZE)
    };

    // SAFETY: it's always safe to create a brand new one
    let home = unsafe { Address::new(local_snd) };
    let ctl2 = Control::new(ex, rune, home.downgrade(), signals);

    let fut = actor_runner(actor, local_rcv, ctl2);
    (fut, home)
}

fn summon_task<A>(
    actor: A,
    ex: Rc<LocalExecutor<'static>>,
    rune: Rune,
    signal: InactiveSignalStream,
) -> Address<A>
where
    A: Actor,
{
    let (fut, adr) = summon_raw(actor, Rc::downgrade(&ex), rune, signal);
    // TODO: this doesn't propagate panics, do i want it to? Should the rune get poisoned? Should
    // something await all Tasks? Send them to the heart and await all of them there?
    ex.spawn(fut).detach();
    adr
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
        summon_task(actor, ex, self.actor_rune.clone(), self.signals.clone())
    }

    fn new(
        ex: Weak<LocalExecutor<'static>>,
        rune: Rune,
        home: WeakAddress<A>,
        signals: InactiveSignalStream,
    ) -> Self {
        let (task_heart, task_rune) = heart::create();
        Self {
            ex,
            actor_rune: rune,
            state: State::Running,
            home,
            signals,
            new_tasks: Vec::new(),
            task_heart,
            task_rune,
        }
    }

    pub fn soft_exit(&mut self) {
        self.state = State::SoftExiting;
        // NOTE: this will only fail if already closed
        if let Some(adr) = self.home.upgrade() {
            adr.sender.close();
        }
        self.task_rune.kill_heart();
    }

    pub fn hard_exit(&mut self) {
        self.soft_exit();
        self.state = State::HardExiting;
    }

    // NOTE: this is returning the weak one since this actor could be in soft exit state, in which
    // case it's not possible to get a non-weak address.
    pub fn address(&self) -> WeakAddress<A> {
        self.home.clone()
    }

    pub fn is_exiting(&self) -> bool {
        match self.state {
            State::Running => false,
            State::SoftExiting | State::HardExiting => true,
        }
    }

    pub fn start_bg_job<F>(&mut self, future: F)
    where
        F: AsyncFnOnce(Heart) + 'static,
    {
        let task = self
            .ex
            .upgrade()
            .expect("the executor is always alive here")
            .spawn(future(self.task_heart.clone()));

        self.new_tasks.push(task);
    }

    pub fn start_bg_job_blocking<F>(&mut self, thunk: F)
    where
        F: FnOnce(Heart) + Send + 'static,
    {
        let task = blocking::unblock({
            let heart = self.task_heart.clone();
            move || thunk(heart)
        });
        self.new_tasks.push(task);
    }
}

// NOTE: both T and Retval are 'static everywhere, but it didn't help to add those here
pub trait Receive<T>: Actor {
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

pub struct Remote;
impl private::Sealed for Remote {}
impl Scope for Remote {}
assert_impl_all!(Remote: Send, Sync);

pub struct GenericAddress<A: Actor, S: Scope> {
    sender: channel::Sender<ErasedDeliverable<A>>,
    _scope: PhantomData<S>,
}

pub type Address<A> = GenericAddress<A, Local>;
assert_not_impl_any!(Address<DummyActor>: Send, Sync);

pub type RemoteAddress<A> = GenericAddress<A, Remote>;
assert_impl_all!(RemoteAddress<DummyActor>: Send, Sync);

pub struct GenericWeakAddress<A: Actor, S: Scope> {
    sender: channel::WeakSender<ErasedDeliverable<A>>,
    _scope: PhantomData<S>,
}

pub type WeakAddress<A> = GenericWeakAddress<A, Local>;
assert_not_impl_any!(WeakAddress<DummyActor>: Send, Sync);

pub type RemoteWeakAddress<A> = GenericWeakAddress<A, Remote>;
assert_impl_all!(RemoteWeakAddress<DummyActor>: Send, Sync);

impl<A: Actor, S: Scope> Clone for GenericAddress<A, S> {
    fn clone(&self) -> Self {
        let c = self.sender.clone();
        // SAFETY: this gets the same scope as the original
        unsafe { Self::new(c) }
    }
}

impl<A: Actor, S: Scope> Clone for GenericWeakAddress<A, S> {
    fn clone(&self) -> Self {
        let c = self.sender.clone();
        // SAFETY: this gets the same scope as the original
        unsafe { Self::new(c) }
    }
}

impl<A: Actor, S: Scope> GenericAddress<A, S> {
    unsafe fn new(sender: channel::Sender<ErasedDeliverable<A>>) -> Self {
        Self {
            sender,
            _scope: PhantomData,
        }
    }

    pub fn downgrade(&self) -> GenericWeakAddress<A, S> {
        let c = self.sender.downgrade();
        // SAFETY: this gets the same scope as the original
        unsafe { GenericWeakAddress::new(c) }
    }
}

impl<A: Actor, S: Scope> GenericWeakAddress<A, S> {
    unsafe fn new(sender: channel::WeakSender<ErasedDeliverable<A>>) -> Self {
        Self {
            sender,
            _scope: PhantomData,
        }
    }

    pub fn upgrade(&self) -> Option<GenericAddress<A, S>> {
        self.sender
            .upgrade()
            // SAFETY: this gets the same scope as the original
            .map(|sender| unsafe { GenericAddress::new(sender) })
    }
}

impl<A: Actor> Address<A> {
    pub fn remote(&self) -> RemoteAddress<A> {
        // SAFETY: It's safe to go from a local address to a remote one, but not the other way around
        let c = self.sender.clone();
        unsafe { RemoteAddress::new(c) }
    }
}

impl<A: Actor> WeakAddress<A> {
    pub fn remote(&self) -> RemoteWeakAddress<A> {
        // SAFETY: It's safe to go from a local address to a remote one, but not the other way around
        let c = self.sender.clone();
        unsafe { RemoteWeakAddress::new(c) }
    }
}

#[allow(
    private_bounds,
    reason = "CanSendImpl and all types it is using should be private"
)]
pub trait CanSend<A, T>: CanSendPriv<A, T> {}
impl<A, T, X> CanSend<A, T> for X where X: CanSendPriv<A, T> {}

trait CanSendPriv<A, T>: Scope {
    fn erase_package(p: Package<T, A::Retval>) -> ErasedDeliverable<A>
    where
        A: Receive<T>;
    fn erase_ticket(t: OneWayTicket<T>) -> ErasedDeliverable<A>
    where
        A: Receive<T, Retval = ()>;
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
        A: Receive<T, Retval = ()>,
    {
        Box::new(t)
    }
}

impl<A, T> CanSendPriv<A, T> for Local
where
    A: Receive<T>,
    T: 'static,
{
    fn erase_package(p: Package<T, A::Retval>) -> ErasedDeliverable<A> {
        Box::new(UnsafeSendWrapper(p))
    }

    fn erase_ticket(t: OneWayTicket<T>) -> ErasedDeliverable<A>
    where
        A: Receive<T, Retval = ()>,
    {
        Box::new(UnsafeSendWrapper(t))
    }
}

trait Deliverable<A: Actor> {
    fn deliver<'a>(
        self: Box<Self>,
        actor: &'a mut A,
        ctl: &'a mut Control<A>,
    ) -> LocalBoxFuture<'a, ()>;
}

#[repr(transparent)]
struct UnsafeSendWrapper<D>(D);
// SAFETY: one of these can only be sent by a local address, which means the sender has never left
// the current thread, which means it's safe to send non-send data on it.
unsafe impl<D> Send for UnsafeSendWrapper<D> {}

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
        let raw = Box::into_raw(self);
        let inner_raw = raw as *mut D;
        // SAFETY: the wrapper is repr(transparent), so its guaranteed to have the same size and
        // alignment, making this cast safe.
        let inner_box = unsafe { Box::from_raw(inner_raw) };
        inner_box.deliver(actor, ctl)
    }
}

struct Package<T, R> {
    msg: T,
    returner: oneshot::Sender<R>,
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
            let ret = actor.receive(self.msg, ctl).await;
            let _: Result<_, _> = self.returner.send(ret);
        }
        .boxed_local()
    }
}

struct OneWayTicket<T> {
    msg: T,
}

impl<T, A> Deliverable<A> for OneWayTicket<T>
where
    A: Receive<T, Retval = ()>,
    T: 'static,
{
    fn deliver<'a>(
        self: Box<Self>,
        actor: &'a mut A,
        ctl: &'a mut Control<A>,
    ) -> LocalBoxFuture<'a, ()> {
        actor.receive(self.msg, ctl).boxed_local()
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
        std::task::Poll::Ready(reply.map_err(|_| ReplyError))
    }
}

impl<A: Actor, S: Scope> GenericAddress<A, S> {
    pub async fn send_receive<T>(&self, msg: T) -> Result<Reply<A::Retval>, SendError>
    where
        T: 'static,
        A: Receive<T>,
        S: CanSend<A, T>,
    {
        let (returner, ret_rcv) = oneshot::async_channel::<A::Retval>();
        let package = Package { msg, returner };
        let erased = S::erase_package(package);
        self.sender.send(erased).await.map_err(|_| SendError)?;
        Ok(Reply { recv: ret_rcv })
    }

    pub async fn send<T>(&self, msg: T) -> Result<(), SendError>
    where
        T: 'static,
        A: Receive<T, Retval = ()>,
        S: CanSend<A, T>,
    {
        let ticket = OneWayTicket { msg };
        let erased = S::erase_ticket(ticket);
        self.sender.send(erased).await.map_err(|_| SendError)?;
        Ok(())
    }
}

// TODO:
// pub struct Accepts<T, S: Scope> {
//     address: Box<dyn CanReceive<T>>,
// }

// TODO: trace this up
async fn actor_runner<A: Actor>(
    mut actor: A,
    rcv: channel::Receiver<ErasedDeliverable<A>>,
    mut ctl: Control<A>,
) {
    actor.enter(&mut ctl).await;

    enum Event<A> {
        Delivery(ErasedDeliverable<A>),
        TaskDone,
        Signal,
    }
    let mut events = {
        let signal_stream = ctl.signals.activate_cloned().map(|_| Event::<A>::Signal);
        let delivery_stream = rcv.map(Event::Delivery);

        let events = delivery_stream.with_future_group().addon(signal_stream);
        pin!(events)
    };

    yield_guard(A::YIELD_POLICY, async |yielder| {
        loop {
            match events.as_mut().next().await {
                Some(Event::Signal) => actor.interrupt(&mut ctl).await,
                Some(Event::Delivery(delivery)) => delivery.deliver(&mut actor, &mut ctl).await,
                Some(Event::TaskDone) => (),
                None => break,
            }

            for task in ctl.new_tasks.drain(..) {
                events
                    .as_mut()
                    .mut_pin_me()
                    .mut_pin_group()
                    .insert(task.map(|_| Event::TaskDone));
            }

            if ctl.state == State::HardExiting {
                break;
            }

            // TODO: test that this even works
            yielder.point().await;
        }
    })
    .await;

    actor.leave(&mut ctl).await;
}

pub struct Stage {
    ex: Rc<LocalExecutor<'static>>,
    heart: Heart,
    rune: Rune,
    signal_stream: InactiveSignalStream,
}

impl Stage {
    pub fn new(signal_stream: InactiveSignalStream) -> Self {
        let (heart, rune) = heart::create();
        Self {
            ex: Rc::new(LocalExecutor::new()),
            heart,
            rune,
            signal_stream,
        }
    }

    pub fn new_no_signals() -> Self {
        Self::new(crate::graceful_termination::dummy())
    }

    // TODO: take a span as argument and make sure it is always entered?
    pub fn cast<A: Actor>(&self, actor: A) -> Address<A> {
        summon_task(
            actor,
            Rc::clone(&self.ex),
            self.rune.clone(),
            self.signal_stream.clone(),
        )
    }

    pub fn play(self) {
        fn take_essentials_drop_the_rest(this: Stage) -> (Heart, Rc<LocalExecutor<'static>>) {
            (this.heart, this.ex)
        }
        let (heart, ex) = take_essentials_drop_the_rest(self);
        block_on(ex.run(heart));
        assert_eq!(Rc::strong_count(&ex), 1);
    }
}

#[cfg(test)]
mod test {
    use super::*;

    struct Alice;
    impl Actor for Alice {}
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
            let reply = self.alice.send_receive(5).await.unwrap();
            let reply = reply.await.unwrap();
            // BUG: this panic is not propagated to the main thread, so the test is marked as passed
            // even if it fails
            assert_eq!(reply, 25);
        }
    }

    #[test]
    fn test_remote() {
        let main_stage = Stage::new_no_signals();
        let alice_adr = main_stage.cast(Alice);

        let t1 = std::thread::spawn({
            let alice_adr = alice_adr.remote();
            || {
                let stage = Stage::new_no_signals();
                stage.cast(Bob { alice: alice_adr });
                stage.play();
            }
        });

        main_stage.play();
        t1.join().unwrap();
    }
}
