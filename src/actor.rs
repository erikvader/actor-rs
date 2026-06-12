use crate::{
    graceful_termination::{GracefulTermination, InactiveSignalStream, SignalStream},
    heart::{self, Rune},
};
use ::smol::{
    LocalExecutor, block_on,
    channel::{Receiver, Sender},
    prelude::*,
    stream::StreamExt,
};
use pin_project::pin_project;
use smol::{
    channel::{self, WeakSender},
    ready,
};
use snafu::prelude::*;
use std::{
    pin::Pin,
    rc::{Rc, Weak},
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
) -> Address<A>
where
    A: Actor + 'static,
{
    let (snd, rcv) = if A::MAIL_BOX_SIZE == 0 {
        channel::unbounded()
    } else {
        channel::bounded(A::MAIL_BOX_SIZE)
    };
    let home = Address::new(snd);
    let ctl2 = Control::new(Rc::downgrade(&ex), rune, home.downgrade(), signals);

    ex.spawn(actor_runner(actor, rcv, ctl2)).detach();

    home
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
        summon(actor, ex, self.rune.clone(), self.signals.clone())
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
    ) -> Pin<Box<dyn Future<Output = ()> + 'a>>;
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
    ) -> Pin<Box<dyn Future<Output = ()> + 'a>> {
        Box::pin(async move {
            let ret = actor.receive(self.msg, ctl).await;
            let _: Result<_, _> = self.returner.send(ret);
        })
    }
}

type ErasedDeliverable<A> = Box<dyn Deliverable<A>>;

// TODO: remote address
pub struct Address<A: Actor + ?Sized> {
    sender: Sender<ErasedDeliverable<A>>,
}

pub struct WeakAddress<A: Actor + ?Sized> {
    sender: WeakSender<ErasedDeliverable<A>>,
}

impl<A: Actor> Clone for Address<A> {
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

impl<A: Actor> Address<A> {
    fn new(sender: Sender<ErasedDeliverable<A>>) -> Self {
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
    rcv: Receiver<ErasedDeliverable<A>>,
    mut ctl: Control<A>,
) {
    actor.enter(&mut ctl).await;

    enum Event<A> {
        Delivery(ErasedDeliverable<A>),
        Signal,
    }
    let events = ctl
        .signals
        .activate_cloned()
        .map(|_| Event::Signal)
        .or(rcv.map(Event::Delivery));
    smol::pin!(events);

    while let Some(event) = events.next().await {
        match event {
            Event::Signal => actor.interrupt(&mut ctl).await,
            Event::Delivery(delivery) => delivery.deliver(&mut actor, &mut ctl).await,
        }

        if ctl.state == State::Exiting {
            break;
        }
    }

    actor.leave(&mut ctl).await;
}

// TODO: sub stages
pub fn stage_play(initial: impl Actor + 'static) {
    let grace = GracefulTermination::new().expect("this should just work");
    let ex = Rc::new(LocalExecutor::new());
    let (heart, rune) = heart::create();

    summon(
        initial,
        Rc::clone(&ex),
        rune,
        grace.inactive_signal_stream(),
    );
    block_on(ex.run(heart));

    assert_eq!(Rc::strong_count(&ex), 1);
}
