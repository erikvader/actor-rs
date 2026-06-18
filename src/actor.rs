use crate::{
    graceful_termination::InactiveSignalStream,
    heart::{self, Heart, Rune},
};
use async_channel as channel;
use async_executor::LocalExecutor;
use futures_concurrency::future::FutureGroup;
use futures_core::future::LocalBoxFuture;
use futures_lite::{future::block_on, prelude::*};
use pin_project::pin_project;
use snafu::prelude::*;
use std::{
    pin::{Pin, pin},
    rc::{Rc, Weak},
    task::ready,
};

pub trait Actor {
    const MAIL_BOX_SIZE: usize;

    #[allow(async_fn_in_trait, unused_variables)]
    async fn enter(&mut self, ctl: &mut Control<Self>) {}
    #[allow(async_fn_in_trait, unused_variables)]
    async fn leave(&mut self, ctl: &mut Control<Self>) {}
    #[allow(async_fn_in_trait, unused_variables)]
    async fn interrupt(&mut self, ctl: &mut Control<Self>) {}
}

pub struct Control<A: Actor + ?Sized> {
    // NOTE: this could probably be a 'a, but I don't think i want that anyways. I think it would
    // pretty much mean all actors can borrow stack data from the main function.
    ex: Weak<LocalExecutor<'static>>,
    state: State,
    rune: Rune,
    home: WeakAddress<A>,
    signals: InactiveSignalStream,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum State {
    Running,
    Closing,
    Exiting,
}

fn summon<A>(
    actor: A,
    ex: Rc<LocalExecutor<'static>>,
    rune: Rune,
    signals: InactiveSignalStream,
) -> (Address<A>, RemoteAddress<A>)
where
    A: Actor + 'static,
{
    let ((local_snd, local_rcv), (remote_snd, remote_rcv)) = if A::MAIL_BOX_SIZE == 0 {
        (channel::unbounded(), channel::unbounded())
    } else {
        (
            channel::bounded(A::MAIL_BOX_SIZE),
            channel::bounded(A::MAIL_BOX_SIZE),
        )
    };

    let home = Address::new(local_snd);
    let remote_home = RemoteAddress::new(remote_snd); // TODO: save this in control as well?
    let ctl2 = Control::new(Rc::downgrade(&ex), rune, home.downgrade(), signals);

    // TODO: this doesn't propagate panics, do i want it to? Should the rune get poisoned? Should
    // something await all Tasks? Send them to the heart and await all of them there?
    ex.spawn(actor_runner(actor, local_rcv, remote_rcv, ctl2))
        .detach();

    (home, remote_home)
}

impl<A: Actor> Control<A> {
    pub fn summon<A2>(&self, actor: A2) -> Address<A2>
    where
        A2: Actor + 'static,
    {
        let ex = self
            .ex
            .upgrade()
            .expect("the executor is always alive here, it's what is running this function");
        summon(actor, ex, self.rune.clone(), self.signals.clone()).0 // TODO: create a summon_remote
    }

    fn new(
        ex: Weak<LocalExecutor<'static>>,
        rune: Rune,
        home: WeakAddress<A>,
        signals: InactiveSignalStream,
    ) -> Self {
        Self {
            ex,
            rune,
            state: State::Running,
            home,
            signals,
        }
    }

    pub fn ask_to_leave(&mut self) {
        self.state = State::Closing;
        self.home
            .upgrade()
            .expect("this will never fail on its own actor")
            .sender
            .close();
    }

    pub fn drag_out(&mut self) {
        self.ask_to_leave();
        self.state = State::Exiting;
    }

    pub fn address(&self) -> WeakAddress<A> {
        self.home.clone()
    }
}

pub trait Receive<T>: Actor {
    type Retval;

    #[allow(async_fn_in_trait)]
    async fn receive(&mut self, msg: T, ctl: &mut Control<Self>) -> Self::Retval;
}

trait Deliverable<A: Actor + ?Sized> {
    fn deliver<'a>(
        self: Box<Self>,
        actor: &'a mut A,
        ctl: &'a mut Control<A>,
    ) -> LocalBoxFuture<'a, ()>;
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
        Box::pin(async move {
            let ret = actor.receive(self.msg, ctl).await;
            let _: Result<_, _> = self.returner.send(ret);
        })
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
        Box::pin(async move {
            actor.receive(self.msg, ctl).await;
        })
    }
}

type ErasedDeliverable<A> = Box<dyn Deliverable<A>>;
type ErasedRemoteDeliverable<A> = Box<dyn Deliverable<A> + Send>;

pub struct Address<A: Actor + ?Sized> {
    sender: channel::Sender<ErasedDeliverable<A>>,
}

// TODO: create a weak variant?
pub struct RemoteAddress<A: Actor + ?Sized> {
    sender: channel::Sender<ErasedRemoteDeliverable<A>>,
}

pub struct WeakAddress<A: Actor + ?Sized> {
    sender: channel::WeakSender<ErasedDeliverable<A>>,
}

impl<A: Actor> Clone for Address<A> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
        }
    }
}

impl<A: Actor> Clone for RemoteAddress<A> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
        }
    }
}

impl<A: Actor> Clone for WeakAddress<A> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
        }
    }
}

#[derive(Debug, Snafu)]
#[snafu(display("Could not send message, actor dead :("))]
pub struct SendError;

#[derive(Debug, Snafu)]
#[snafu(display("Could not receive reply, actor dead :("))]
pub struct ReplyError;

#[pin_project(!Unpin)]
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

impl<A: Actor> RemoteAddress<A> {
    fn new(sender: channel::Sender<ErasedRemoteDeliverable<A>>) -> Self {
        Self { sender }
    }

    pub async fn send<T>(&self, msg: T) -> Result<Reply<A::Retval>, SendError>
    where
        T: Send + 'static,
        A: Receive<T>,
        A::Retval: Send + 'static,
    {
        let (returner, ret_rcv) = oneshot::async_channel::<A::Retval>();
        let erased = Box::new(Package { msg, returner });
        self.sender.send(erased).await.map_err(|_| SendError)?;
        Ok(Reply { recv: ret_rcv })
    }
}

impl<A: Actor> Address<A> {
    fn new(sender: channel::Sender<ErasedDeliverable<A>>) -> Self {
        Self { sender }
    }

    pub async fn send<T>(&self, msg: T) -> Result<Reply<A::Retval>, SendError>
    where
        T: 'static,
        A: Receive<T>,
        A::Retval: 'static,
    {
        let (returner, ret_rcv) = oneshot::async_channel::<A::Retval>();
        let erased = Box::new(Package { msg, returner });
        self.sender.send(erased).await.map_err(|_| SendError)?;
        Ok(Reply { recv: ret_rcv })
    }

    // TODO: create for RemoteAddress as well
    pub async fn send_oneway<T>(&self, msg: T) -> Result<(), SendError>
    where
        T: 'static,
        A: Receive<T, Retval = ()>,
    {
        let erased = Box::new(OneWayTicket { msg });
        self.sender.send(erased).await.map_err(|_| SendError)?;
        Ok(())
    }

    pub fn downgrade(&self) -> WeakAddress<A> {
        WeakAddress {
            sender: self.sender.downgrade(),
        }
    }
}

impl<A: Actor> WeakAddress<A> {
    pub fn upgrade(&self) -> Option<Address<A>> {
        self.sender.upgrade().map(|sender| Address::new(sender))
    }
}

// TODO: trace this up
async fn actor_runner<A: Actor>(
    mut actor: A,
    local_rcv: channel::Receiver<ErasedDeliverable<A>>,
    remote_rcv: channel::Receiver<ErasedRemoteDeliverable<A>>,
    mut ctl: Control<A>,
) {
    actor.enter(&mut ctl).await;

    enum Event<A> {
        Delivery(ErasedDeliverable<A>),
        RemoteDelivery(ErasedRemoteDeliverable<A>),
        Signal,
    }
    let signal_stream = ctl.signals.activate_cloned().map(|_| Event::<A>::Signal);
    let local_stream = local_rcv.map(Event::Delivery);
    let remote_stream = remote_rcv.map(Event::RemoteDelivery);
    // TODO: concurrentstream?
    let mut reply_stream = FutureGroup::<std::future::Ready<Event<A>>>::new();

    let events = signal_stream;
    // TODO: race these
    // .or(local_stream)
    // .or(remote_stream)
    // .or(&mut reply_stream);
    let mut events = pin!(events);

    loop {
        match events.next().await {
            Some(Event::Signal) => actor.interrupt(&mut ctl).await,
            Some(Event::Delivery(delivery)) => delivery.deliver(&mut actor, &mut ctl).await,
            Some(Event::RemoteDelivery(delivery)) => delivery.deliver(&mut actor, &mut ctl).await,
            None => break,
        }

        if ctl.state == State::Exiting {
            break;
        }
    }

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
    pub fn cast<A: Actor + 'static>(&self, actor: A) -> Address<A> {
        summon(
            actor,
            Rc::clone(&self.ex),
            self.rune.clone(),
            self.signal_stream.clone(),
        )
        .0
    }

    pub fn cast_remote<A: Actor + 'static>(&self, actor: A) -> RemoteAddress<A> {
        summon(
            actor,
            Rc::clone(&self.ex),
            self.rune.clone(),
            self.signal_stream.clone(),
        )
        .1
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
    impl Actor for Alice {
        const MAIL_BOX_SIZE: usize = 16;
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
        const MAIL_BOX_SIZE: usize = 16;
        async fn enter(&mut self, _ctl: &mut Control<Self>) {
            let reply = self.alice.send(5).await.unwrap();
            let reply = reply.await.unwrap();
            // BUG: this panic is not propagated to the main thread, so the test is marked as passed
            // even though it isn't
            assert_eq!(reply, 25);
        }
    }

    #[test]
    fn test_remote() {
        let main_stage = Stage::new_no_signals();
        let alice_adr = main_stage.cast_remote(Alice);

        let t1 = std::thread::spawn(|| {
            let stage = Stage::new_no_signals();
            stage.cast(Bob { alice: alice_adr });
            stage.play();
        });

        main_stage.play();
        t1.join().unwrap();
    }
}
