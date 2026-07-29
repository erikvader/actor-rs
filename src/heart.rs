use std::{
    pin::{Pin, pin},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize},
    },
    task::Poll,
};

use event_listener::{Event, listener};
use futures_core::{FusedStream, Stream, future::BoxFuture};
use futures_util::{FutureExt, StreamExt};
use pin_project::pin_project;
use snafu::Snafu;
use std::sync::atomic::Ordering::SeqCst;

// NOTE: I just use seqcst everywhere for simplicity. There are way to many special cases and subtle
// bugs to handle when trying to optimize each atomic access to only use the minimum required
// ordering, things like independent atomic variables getting loads reordered before stores, and
// release-acquire-chaining, etc. Testing with something like Loom should be used if these kinds of
// optimisations are required, it's so easy to miss some special case otherwise. SeqCst is not that
// much slower anyways, apparently (source required).

#[derive(Debug, Snafu)]
#[snafu(display("{count} runes dropped while panicking ({complete:?})"))]
// TODO: this is weird, do i really want to error on early exit?
pub struct PanicError {
    count: usize,
    complete: Completeness,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Completeness {
    Complete,
    Incomplete,
}

struct Inner {
    // NOTE: this is emitting SeqCst here and there, so this is synching itself. Crucially, it makes
    // sure that all operations before a notify (seqcst before) is visible after a listen (seqcst
    // after).
    event: Event,
    rune_count: AtomicUsize,
    panic_count: AtomicUsize,
    killed: AtomicBool,
}

impl Inner {
    fn new() -> Self {
        Self {
            event: Event::new(),
            rune_count: AtomicUsize::new(1),
            panic_count: AtomicUsize::new(0),
            killed: AtomicBool::new(false),
        }
    }
}

pub struct Rune {
    inner: Arc<Inner>,
}

impl Rune {
    pub fn kill_heart(&self) {
        self.inner.killed.store(true, SeqCst);
        self.inner.event.notify(usize::MAX);
    }
}

impl Clone for Rune {
    fn clone(&self) -> Self {
        let prev = self.inner.rune_count.fetch_add(1, SeqCst);
        debug_assert_ne!(prev, 0, "the counter was increased from 0");

        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl Drop for Rune {
    fn drop(&mut self) {
        let inner = &*self.inner;
        if std::thread::panicking() {
            tracing::debug!("Rune dropped while panicking");
            inner.panic_count.fetch_add(1, SeqCst);
        }
        let prev = inner.rune_count.fetch_sub(1, SeqCst);
        debug_assert_ne!(prev, 0, "the counter underflowed");
        if prev == 1 {
            self.kill_heart();
        }
    }
}

/// insipiration: https://dishonored.fandom.com/wiki/The_Heart
// TODO: this could also be an IntoFuture, maybe? I think it requires boxing the future though.
#[derive(Clone)]
pub struct Heart {
    inner: Arc<Inner>,
}

impl Heart {
    pub fn is_dead(&self) -> bool {
        self.inner.killed.load(SeqCst)
    }

    pub fn rune_count(&self) -> usize {
        self.inner.rune_count.load(SeqCst)
    }

    pub async fn wait(&self) -> Result<(), PanicError> {
        let inner = &*self.inner;
        loop {
            if self.is_dead() {
                break;
            }

            listener!(inner.event => listener);

            if self.is_dead() {
                break;
            }

            listener.await;
        }

        // NOTE: read in reverse order of how they are set to ensure that a rune_count of 0 means
        // that the panic_count is the final value.
        let complete = if self.rune_count() == 0 {
            Completeness::Complete
        } else {
            Completeness::Incomplete
        };
        let panic_count = inner.panic_count.load(SeqCst);

        if panic_count == 0
            && let Completeness::Complete = complete
        {
            Ok(())
        } else {
            PanicSnafu {
                count: panic_count,
                complete,
            }
            .fail()
        }
    }

    pub async fn watch_future<T>(&self, fut: impl Future<Output = T>) -> Watch<T> {
        // TODO: this is probably better implemented with race instead of wrapping in a stream,
        // which requires heap allocation
        let stream = self.watch_stream(fut.into_stream());
        let mut stream = pin!(stream);
        stream.next().await.expect("will contain exactly one item")
    }

    pub fn watch_stream<S: Stream>(&self, stream: S) -> WatchedStream<'_, S> {
        WatchedStream {
            stream,
            wait: self.wait().boxed(),
            done: false,
        }
    }
}

#[derive(Debug)]
pub enum Watch<T> {
    Ready(T),
    Dead,
    DeadPanic(PanicError),
}

#[pin_project]
#[must_use = "Streams do nothing without being polled"]
// TODO: this lifetime doesn't have to be here, the Arc<Inner> can be cloned, so the boxed future
// doesn't have to borrow anything.
pub struct WatchedStream<'a, S> {
    #[pin]
    stream: S,
    #[pin]
    // NOTE: i take this boxed to not have to create another custom future type which probably has
    // to heap allocate an EventListener anyways.
    wait: BoxFuture<'a, Result<(), PanicError>>,
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
    let inner = Arc::new(Inner::new());
    (
        Heart {
            inner: inner.clone(),
        },
        Rune { inner },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_test::assert_stream_done;
    use futures_util::stream as fus;

    #[test]
    fn future_can_be_waited_multiple_times() {
        let (heart, rune) = create();
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
    fn counting_one_panic() {
        let (heart, rune) = create();
        let _res = std::panic::catch_unwind(move || {
            let _rune = rune;
            panic!("omg!");
        });
        let err = assert_future_ready!(pin heart.wait());
        let PanicError { count, complete } = err.unwrap_err();
        assert_eq!(count, 1);
        assert_eq!(complete, Completeness::Complete);
    }

    #[test]
    fn counting_two_panics_with_two_receivers() {
        let (heart, rune) = create();
        let _res = std::panic::catch_unwind({
            let rune = rune.clone();
            move || {
                let _rune = rune;
                panic!("omg!");
            }
        });
        let _res = std::panic::catch_unwind(move || {
            let _rune = rune;
            panic!("omg!");
        });

        let heart2 = heart.clone();
        let err = assert_future_ready!(pin heart.wait());
        let err2 = assert_future_ready!(pin heart2.wait());

        let PanicError { count, complete } = err.unwrap_err();
        assert_eq!(count, 2);
        assert_eq!(complete, Completeness::Complete);
        let PanicError { count, complete } = err2.unwrap_err();
        assert_eq!(count, 2);
        assert_eq!(complete, Completeness::Complete);
    }

    #[test]
    fn watching_stream_no_abort() {
        let (mut heart, _rune) = create();
        let s = fus::iter(vec![1, 2]);
        let mut s = pin!(heart.watch_stream(s));
        assert_future_ready!(unpin s.next(), w => matches!(w.unwrap(), Watch::Ready(1)));
        assert_future_ready!(unpin s.next(), w => matches!(w.unwrap(), Watch::Ready(2)));
        assert_stream_done!(s);
    }

    #[test]
    fn watching_stream_abort() {
        let (mut heart, rune) = create();
        let s = fus::iter(vec![1, 2]);
        let mut s = pin!(heart.watch_stream(s));
        assert_future_ready!(unpin s.next(), w => matches!(w.unwrap(), Watch::Ready(1)));

        drop(rune);
        assert_future_ready!(unpin s.next(), w => matches!(w.unwrap(), Watch::Dead));
        assert_stream_done!(s);
    }
}
