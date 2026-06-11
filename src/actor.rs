use crate::heart::{self, Rune};
use ::smol::{
    LocalExecutor, block_on,
    channel::{Receiver, Sender},
    prelude::*,
    stream::StreamExt,
};
use pin_project::pin_project;
use smol::{channel, ready};
use snafu::prelude::*;
use std::{
    marker::PhantomData,
    pin::Pin,
    rc::{Rc, Weak},
};

pub trait Actor {
    const MAIL_BOX_SIZE: usize;

    #[allow(async_fn_in_trait, unused_variables)]
    async fn enter(&mut self, ctl: &mut Control) {}
    #[allow(async_fn_in_trait, unused_variables)]
    async fn leave(&mut self, ctl: &mut Control) {}
}

pub struct Control {
    // NOTE: this could probably be a 'a, but I don't think i want that anyways. I think it would
    // pretty much mean all actors can borrow stack data from the main function.
    ex: Weak<LocalExecutor<'static>>,
    state: State,
    rune: Rune,
    // TODO: an actor should have a weak address to itself which it can distribute to others
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum State {
    Running,
    Closing,
    Exiting,
}

impl Control {
    pub fn summon<A: Actor + 'static>(&self, actor: A) -> Address<A> {
        let ctl2 = Control {
            ex: Weak::clone(&self.ex),
            state: State::Running,
            rune: self.rune.clone(),
        };

        let (snd, rcv) = if A::MAIL_BOX_SIZE == 0 {
            channel::unbounded()
        } else {
            channel::bounded(A::MAIL_BOX_SIZE)
        };

        let ex = self
            .ex
            .upgrade()
            .expect("the executor is always alive here");
        ex.spawn(actor_runner(actor, rcv, ctl2)).detach();

        Address {
            sender: snd,
            _phantom: PhantomData,
        }
    }

    pub fn ask_to_leave(&mut self) {
        self.state = State::Closing;
    }

    pub fn drag_out(&mut self) {
        self.state = State::Exiting;
    }
}

pub trait Receive<T>: Actor {
    type Retval;

    #[allow(async_fn_in_trait)]
    async fn receive(&mut self, msg: T, ctl: &mut Control) -> Self::Retval;
}

trait Deliverable<A: Actor> {
    fn deliver<'a>(
        self: Box<Self>,
        actor: &'a mut A,
        ctl: &'a mut Control,
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
        ctl: &'a mut Control,
    ) -> Pin<Box<dyn Future<Output = ()> + 'a>> {
        Box::pin(async move {
            let ret = actor.receive(self.msg, ctl).await;
            let _: Result<_, _> = self.returner.send(ret);
        })
    }
}

type ErasedDeliverable<A> = Box<dyn Deliverable<A>>;

#[derive(Clone)]
pub struct Address<A: Actor> {
    sender: Sender<ErasedDeliverable<A>>,
    _phantom: PhantomData<A>,
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
}

// TODO: async_broadcast signals from async_signal
async fn actor_runner<A: Actor>(
    mut actor: A,
    rcv: Receiver<ErasedDeliverable<A>>,
    mut ctl: Control,
) {
    actor.enter(&mut ctl).await;

    smol::pin!(rcv);
    while let Some(delivery) = rcv.next().await {
        delivery.deliver(&mut actor, &mut ctl).await;

        match ctl.state {
            State::Running => (),
            State::Closing => {
                rcv.close();
            }
            State::Exiting => break,
        }
    }

    actor.leave(&mut ctl).await;
}

pub fn stage_play(initial: impl Actor + 'static) {
    let ex = Rc::new(LocalExecutor::new());
    let (heart, rune) = heart::create();

    Control {
        ex: Rc::downgrade(&ex),
        rune,
        state: State::Running,
    }
    .summon(initial);

    block_on(ex.run(heart));

    assert_eq!(Rc::strong_count(&ex), 1);
}
