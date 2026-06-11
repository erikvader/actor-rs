use futures_core::{FusedFuture, FusedStream};
use pin_project::pin_project;
use smol::{
    channel::{self, Receiver, Sender},
    stream::Stream,
};

#[derive(Clone)]
pub struct Rune {
    inner: Sender<()>,
}

/// insipiration: https://dishonored.fandom.com/wiki/The_Heart
#[pin_project]
pub struct Heart {
    #[pin]
    inner: Receiver<()>,
}

pub fn create() -> (Heart, Rune) {
    let (s, r) = channel::bounded(1);
    (Heart { inner: r }, Rune { inner: s })
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
