use std::{
    num::NonZeroU32,
    pin::{Pin, pin},
    task::Poll,
};

use futures_concurrency::future::FutureGroup;
use futures_core::{FusedStream, Stream};
use pin_project::pin_project;

// TODO: remove?
// NOTE: this can't be a simple futures_util::stream::select since it will ignore the futuregroup if
// it has been empty even once.
#[must_use = "this does nothing without being polled"]
#[pin_project]
pub struct WithFutures<S, F> {
    #[pin]
    stream: S,
    // NOTE: it's fine to poll this group over and over even though it has returned Ready(None).
    // It will start to return more results after more futures have been inserted into it.
    #[pin]
    group: FutureGroup<F>,
    group_first: bool,
}

impl<S: Stream, F> WithFutures<S, F> {
    pub fn new(stream: S) -> Self
    where
        S: FusedStream,
        F: Future<Output = S::Item>,
    {
        Self {
            stream,
            group: FutureGroup::new(),
            group_first: false,
        }
    }
}

impl<S, F> WithFutures<S, F> {
    pub fn mut_pin_group(self: Pin<&mut Self>) -> Pin<&mut FutureGroup<F>> {
        self.project().group
    }
}

impl<S, F> Stream for WithFutures<S, F>
where
    S: Stream + FusedStream,
    F: Future<Output = S::Item>,
{
    type Item = S::Item;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let mut this = self.project();
        *this.group_first = !*this.group_first;

        let mut group_empty = if *this.group_first {
            match this.group.as_mut().poll_next(cx) {
                Poll::Ready(Some(x)) => return Poll::Ready(Some(x)),
                Poll::Ready(None) => Some(true),
                Poll::Pending => Some(false),
            }
        } else {
            None
        };

        let stream_empty = if !this.stream.is_terminated() {
            match this.stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(i)) => return Poll::Ready(Some(i)),
                Poll::Ready(None) => true,
                Poll::Pending => false,
            }
        } else {
            true
        };

        // NOTE: can't use map() here cuz i want the early return
        group_empty = if group_empty.is_none() {
            match this.group.as_mut().poll_next(cx) {
                Poll::Ready(Some(x)) => return Poll::Ready(Some(x)),
                Poll::Ready(None) => Some(true),
                Poll::Pending => Some(false),
            }
        } else {
            group_empty
        };

        if group_empty.unwrap() && stream_empty {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}

#[must_use = "this does nothing without being polled"]
#[pin_project]
pub struct Addon<A, M> {
    #[pin]
    addon: A,
    #[pin]
    me: M,
}

impl<S, R> Addon<S, R> {
    pub fn mut_pin_me(self: Pin<&mut Self>) -> Pin<&mut R> {
        self.project().me
    }
}

impl<S, R, I> Stream for Addon<S, R>
where
    S: FusedStream<Item = I>,
    R: Stream<Item = I>,
{
    type Item = I;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.project();
        if !this.addon.is_terminated()
            && let Poll::Ready(Some(i)) = this.addon.poll_next(cx)
        {
            return Poll::Ready(Some(i));
        }

        this.me.poll_next(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // TODO: add the size hint of all members together
        self.me.size_hint()
    }
}

// TODO: remove?
#[must_use = "this does nothing without being polled"]
#[pin_project]
pub struct TerminationWatch<S, F> {
    #[pin]
    stream: S,
    callback: Option<F>,
}

impl<S, F> Stream for TerminationWatch<S, F>
where
    S: Stream,
    F: FnOnce(),
{
    type Item = S::Item;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let res = this.stream.poll_next(cx);
        if let Poll::Ready(None) = res
            && let Some(cb) = this.callback.take()
        {
            cb();
        }
        res
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}

pub trait StreamExt: Stream {
    /// Always polls the argument first, but ignores if it has terminated or not, the whole
    /// combinator is terminated iff the self has terminated.
    fn addon<R>(self, addon: R) -> Addon<R, Self>
    where
        Self: Sized,
        R: FusedStream<Item = Self::Item>,
    {
        Addon { addon, me: self }
    }

    /// Calls the provided closure when this stream is depleted.
    fn termination_watch<F: FnOnce()>(self, callback: F) -> TerminationWatch<Self, F>
    where
        Self: Sized,
    {
        TerminationWatch {
            stream: self,
            callback: Some(callback),
        }
    }

    /// Attaches a FutureGroup to this stream
    fn with_future_group<F>(self) -> WithFutures<Self, F>
    where
        Self: Sized + FusedStream,
        F: Future<Output = Self::Item>,
    {
        WithFutures::new(self)
    }
}

impl<S> StreamExt for S where S: Stream {}

pub enum YieldPolicy {
    Never,
    Every(NonZeroU32),
}

impl YieldPolicy {
    pub const fn default() -> Self {
        Self::Every(const { NonZeroU32::new(16).unwrap() })
    }
}

pub struct Yielder<'a> {
    count: &'a std::cell::Cell<u32>,
    max: YieldPolicy,
}

impl Yielder<'_> {
    pub async fn point(&self) {
        std::future::poll_fn(|cx| {
            if let YieldPolicy::Every(max) = self.max {
                self.count.update(|old| old + 1);
                if self.count.get() >= max.get() {
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
            }
            Poll::Ready(())
        })
        .await;
    }
}

pub async fn yield_guard<F, R>(max: YieldPolicy, to_guard: F) -> R
where
    F: for<'a> AsyncFnOnce(Yielder<'a>) -> R,
{
    let count = std::cell::Cell::new(0);
    let yielder = Yielder { count: &count, max };
    let mut fut = pin!(to_guard(yielder));
    std::future::poll_fn(|cx| match fut.as_mut().poll(cx) {
        Poll::Ready(x) => Poll::Ready(x),
        Poll::Pending => {
            count.set(0);
            Poll::Pending
        }
    })
    .await
}

#[cfg(test)]
mod test {
    use futures_util::{StreamExt, stream};

    use super::*;

    #[test]
    fn test_addon() {
        let me = stream::iter(vec![3]);
        let ad = stream::iter(vec![1, 2]).fuse();
        // TODO: add:)
        // let s = me.addon(ad).collect();
    }
}
