use std::{
    pin::{Pin, pin},
    task::Poll,
};

use async_channel as channel;
use async_io::block_on;
use futures_core::{FusedStream, Stream};
use futures_util::{FutureExt, StreamExt};
use pin_project::pin_project;
use snafu::Snafu;

#[derive(Debug, Snafu, Eq, PartialEq)]
#[snafu(display("{count} runes dropped while panicking"))]
pub struct PanicError {
    count: usize,
}

#[derive(Clone)]
pub struct Rune {
    inner: channel::Sender<()>,
}

impl Rune {
    pub fn kill_heart(&self) {
        self.inner.close();
    }
}

impl Drop for Rune {
    fn drop(&mut self) {
        if std::thread::panicking() {
            tracing::debug!("Rune dropped while panicking");
            let _: Result<_, _> = self.inner.try_send(());
        }
    }
}

/// insipiration: https://dishonored.fandom.com/wiki/The_Heart
// TODO: this could also be an IntoFuture if i create self-referential Wait<'a>
pub struct Heart {
    inner: channel::Receiver<()>,
    panics: usize,
}

impl Clone for Heart {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            // TODO: should this count be shared?
            panics: 0,
        }
    }
}

impl Heart {
    pub fn is_dead(&self) -> bool {
        self.inner.is_closed()
    }

    pub fn wait_blocking(mut self) -> Result<(), PanicError> {
        // NOTE: the block_on from async_io will spin up a reactor and stuff, which is not needed
        // here, but i don't want to bring in another executor dependency just for this.
        // Futures-lite has its own block_on, but futures-util doesn't have an equivalent, so i have
        // to bring in futures-executor or smth. I could simply use recv_blocking on the channel,
        // but that would mean duplicating the panic counter logic, so I would prefer not to.
        block_on(self.wait())
    }

    pub fn rune_count(&self) -> usize {
        self.inner.sender_count()
    }

    pub fn wait(&mut self) -> Wait<'_> {
        Wait {
            inner: self.inner.clone(),
            heart: self,
        }
    }

    pub async fn watch_future<T>(&mut self, fut: impl Future<Output = T>) -> Watch<T> {
        let stream = self.watch_stream(fut.into_stream());
        let mut stream = pin!(stream);
        stream.next().await.expect("will contain exactly one item")
    }

    pub fn watch_stream<S: Stream>(&mut self, stream: S) -> WatchedStream<'_, S> {
        WatchedStream {
            stream,
            wait: self.wait(),
            done: false,
        }
    }
}

#[pin_project]
#[must_use = "Streams do nothing without being polled"]
pub struct Wait<'a> {
    #[pin]
    inner: channel::Receiver<()>,
    heart: &'a mut Heart,
}

impl Future for Wait<'_> {
    type Output = Result<(), PanicError>;

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();

        loop {
            match this.inner.as_mut().poll_next(cx) {
                Poll::Ready(Some(())) => {
                    this.heart.panics += 1;
                }
                Poll::Ready(None) if this.heart.panics == 0 => break Poll::Ready(Ok(())),
                Poll::Ready(None) => {
                    break Poll::Ready(
                        PanicSnafu {
                            count: this.heart.panics,
                        }
                        .fail(),
                    );
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum Watch<T> {
    Ready(T),
    Dead,
    DeadPanic(PanicError),
}

#[pin_project]
#[must_use = "Streams do nothing without being polled"]
pub struct WatchedStream<'a, S> {
    #[pin]
    stream: S,
    #[pin]
    wait: Wait<'a>,
    done: bool,
}

impl<'a, S: Stream> Stream for WatchedStream<'a, S> {
    type Item = Watch<S::Item>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.project();
        if *this.done {
            return Poll::Ready(None);
        }

        match this.wait.poll(cx) {
            Poll::Ready(Ok(())) => {
                *this.done = true;
                return Poll::Ready(Some(Watch::Dead));
            }
            Poll::Ready(Err(error)) => {
                *this.done = true;
                return Poll::Ready(Some(Watch::DeadPanic(error)));
            }
            Poll::Pending => (),
        }

        match this.stream.poll_next(cx) {
            Poll::Ready(None) => {
                *this.done = true;
                Poll::Ready(None)
            }
            Poll::Ready(Some(x)) => Poll::Ready(Some(Watch::Ready(x))),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<'a, S: Stream> FusedStream for WatchedStream<'a, S> {
    fn is_terminated(&self) -> bool {
        self.done
    }
}

pub fn create() -> (Heart, Rune) {
    let (s, r) = channel::unbounded();
    (
        Heart {
            inner: r,
            panics: 0,
        },
        Rune { inner: s },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_test::{assert_stream_done, assert_stream_next};
    use futures_util::stream as fus;

    #[test]
    fn future_can_be_waited_multiple_times() {
        let (mut heart, rune) = create();
        assert_eq!(heart.rune_count(), 1);
        assert!(!heart.is_dead());
        assert_future_pending!(pin heart.wait());
        assert_future_pending!(pin heart.wait());

        drop(rune);
        assert_eq!(heart.rune_count(), 0);
        assert!(heart.is_dead());
        assert_future_ready!(pin heart.wait(), res => res.is_ok());
        assert_future_ready!(pin heart.wait(), res => res.is_ok());
    }

    #[test]
    fn counting_panics() {
        let (mut heart, rune) = create();
        let _res = std::panic::catch_unwind(move || {
            let _rune = rune;
            panic!("omg!");
        });
        let err = assert_future_ready!(pin heart.wait());
        if let Err(PanicError { count }) = err {
            assert_eq!(count, 1);
        } else {
            panic!("not err");
        }
    }

    #[test]
    fn watching_stream_no_abort() {
        let (mut heart, _rune) = create();
        let s = fus::iter(vec![1, 2]);
        let mut s = pin!(heart.watch_stream(s));
        assert_stream_next!(s, Watch::Ready(1));
        assert_stream_next!(s, Watch::Ready(2));
        assert_stream_done!(s);
    }

    #[test]
    fn watching_stream_abort() {
        let (mut heart, rune) = create();
        let s = fus::iter(vec![1, 2]);
        let mut s = pin!(heart.watch_stream(s));
        assert_stream_next!(s, Watch::Ready(1));

        drop(rune);
        assert_stream_next!(s, Watch::Dead);
        assert_stream_done!(s);
    }
}
