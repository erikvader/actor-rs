use std::{
    pin::{Pin, pin},
    task::Poll,
};

use async_channel as channel;
use futures_core::{FusedStream, Stream};
use pin_project::pin_project;

#[derive(Clone)]
pub struct Switch {
    inner: channel::Sender<()>,
}

impl Switch {
    pub fn detonate(&self) {
        self.inner.close();
    }

    pub fn into_unstable(self) -> UnstableSwitch {
        UnstableSwitch { inner: self }
    }
}

// TODO: why did i add this?
#[derive(Clone)]
/// Dropping this will surely activate it
pub struct UnstableSwitch {
    inner: Switch,
}

impl Drop for UnstableSwitch {
    fn drop(&mut self) {
        self.inner.detonate();
    }
}

#[derive(Clone)]
// TODO: this could probably be an IntoFuture, but I need a concrete type for that
pub struct Bomb {
    // RANT: this channel is !Unpin even though it doesn't have to be
    inner: channel::Receiver<()>,
}

impl Bomb {
    pub fn has_exploded(&self) -> bool {
        self.inner.is_closed()
    }

    pub async fn wait(&self) {
        // TODO: this could clone the receiver and return a future that is static
        if let Ok(()) = self.inner.recv().await {
            panic!("Nothing is ever sent here");
        }
    }

    pub async fn attach_future<T>(&self, fut: impl Future<Output = T>) -> Tick<T> {
        // NOTE: this select will always prioritize the left future
        match futures_util::future::select(pin!(self.wait()), pin!(fut)).await {
            futures_util::future::Either::Left(((), _)) => Tick::Boom,
            futures_util::future::Either::Right((val, _)) => Tick::Tock(val),
        }
    }

    pub fn attach_stream<S: Stream>(&self, stream: S) -> AttachedStream<S> {
        AttachedStream {
            stream,
            wait: self.inner.clone(),
            done: false,
        }
    }
}

pub fn create() -> (Bomb, Switch) {
    // NOTE: bounded(1) takes less space than unused unbounded, i hope
    let (snd, rcv) = channel::bounded(1);
    (Bomb { inner: rcv }, Switch { inner: snd })
}

#[derive(Debug, PartialEq, Eq)]
pub enum Tick<T> {
    Tock(T),
    Boom,
}

#[pin_project]
#[must_use = "Streams do nothing without being polled"]
pub struct AttachedStream<S> {
    #[pin]
    stream: S,
    #[pin]
    wait: channel::Receiver<()>,
    done: bool,
}

impl<S: Stream> Stream for AttachedStream<S> {
    type Item = Tick<S::Item>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.project();
        if *this.done {
            return Poll::Ready(None);
        }

        match this.wait.poll_next(cx) {
            Poll::Ready(Some(())) => {
                panic!("Nothing should ever be sent here");
            }
            Poll::Ready(None) => {
                *this.done = true;
                return Poll::Ready(Some(Tick::Boom));
            }
            Poll::Pending => (),
        }

        match this.stream.poll_next(cx) {
            Poll::Ready(None) => {
                *this.done = true;
                Poll::Ready(None)
            }
            Poll::Ready(Some(x)) => Poll::Ready(Some(Tick::Tock(x))),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<S: Stream> FusedStream for AttachedStream<S> {
    fn is_terminated(&self) -> bool {
        self.done
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_test::assert_stream_done;
    use futures_test::future::FutureTestExt;
    use futures_util::StreamExt;
    use futures_util::future as fuf;
    use futures_util::stream as fus;

    #[test]
    fn future_can_be_waited_multiple_times() {
        let (bomb, switch) = create();
        assert!(!bomb.has_exploded());
        assert_future_pending!(pin bomb.wait());
        assert_future_pending!(pin bomb.wait());

        drop(switch);
        assert!(bomb.has_exploded());
        assert_future_ready!(pin bomb.wait(), ());
        assert_future_ready!(pin bomb.wait(), ());
    }

    #[test]
    fn explode_manually() {
        let (bomb, switch) = create();
        assert!(!bomb.has_exploded());
        assert_future_pending!(pin bomb.wait());

        switch.detonate();
        assert!(bomb.has_exploded());
        assert_future_ready!(pin bomb.wait(), ());

        drop(switch);
        assert_future_ready!(pin bomb.wait(), ());
    }

    #[test]
    fn attach_stream_no_abort() {
        let (bomb, _switch) = create();
        let s = fus::iter(vec![1, 2]);
        let mut s = pin!(bomb.attach_stream(s));
        assert_future_ready!(unpin s.next(), w => matches!(w.unwrap(), Tick::Tock(1)));
        assert_future_ready!(unpin s.next(), w => matches!(w.unwrap(), Tick::Tock(2)));
        assert_stream_done!(s);
    }

    #[test]
    fn attach_stream_abort() {
        let (bomb, switch) = create();
        let s = fus::iter(vec![1, 2]);
        let mut s = pin!(bomb.attach_stream(s));
        assert_future_ready!(unpin s.next(), w => matches!(w.unwrap(), Tick::Tock(1)));

        drop(switch);
        assert_future_ready!(unpin s.next(), w => matches!(w.unwrap(), Tick::Boom));
        assert_stream_done!(s);
    }

    #[test]
    fn attach_future_no_abort() {
        let (bomb, _switch) = create();
        let s = fuf::ready(1);
        let mut s = pin!(bomb.attach_future(s).pending_once());
        assert_future_pending!(s);
        assert_future_ready!(s, Tick::Tock(1));
    }

    #[test]
    fn attach_future_abort() {
        let (bomb, switch) = create();
        let s = fuf::lazy(|_| panic!("I got polled!"));
        let mut s = pin!(bomb.attach_future(s));
        switch.detonate();
        assert_future_ready!(s, Tick::Boom);
    }

    #[test]
    fn attach_future_abort_switch_prioritized_no_fairness() {
        let (bomb, switch) = create();
        let s = fuf::lazy(|_| panic!("I got polled!"));
        let mut s = pin!(bomb.attach_future(s).pending_once());

        assert_future_pending!(s);
        switch.detonate();
        assert_future_ready!(s, Tick::Boom);
    }
}
