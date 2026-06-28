use async_channel as channel;
use futures_core::{FusedFuture, FusedStream, Stream};
use pin_project::pin_project;

#[derive(Clone)]
pub struct Rune {
    inner: channel::Sender<()>,
}

impl Rune {
    #[expect(dead_code, reason = "not used yet")]
    pub fn kill_heart(&self) {
        self.inner.close();
    }
}

/// insipiration: https://dishonored.fandom.com/wiki/The_Heart
#[pin_project]
#[derive(Clone)]
pub struct Heart {
    #[pin]
    inner: channel::Receiver<()>,
}

impl Heart {
    pub fn is_dead(&self) -> bool {
        self.inner.is_closed()
    }

    pub fn await_blocking(&self) {
        if self.inner.recv_blocking().is_ok() {
            panic!("this is not supposed to happen, nothing will ever be sent here")
        }
    }
}

impl Future for Heart {
    type Output = ();

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        if self.is_terminated() {
            return std::task::Poll::Ready(());
        }
        let this = self.project();
        match this.inner.poll_next(cx) {
            std::task::Poll::Ready(Some(())) => {
                panic!("this is not supposed to happen, nothing will ever send here")
            }
            std::task::Poll::Ready(None) => std::task::Poll::Ready(()),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

impl FusedFuture for Heart {
    fn is_terminated(&self) -> bool {
        self.inner.is_terminated()
    }
}

pub fn create() -> (Heart, Rune) {
    // NOTE: i'm hoping that an unbounded channel takes less space than a bounded(1)
    let (s, r) = channel::unbounded();
    (Heart { inner: r }, Rune { inner: s })
}
