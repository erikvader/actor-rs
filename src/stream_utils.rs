use std::{
    cell::Cell,
    num::NonZeroU32,
    pin::{Pin, pin},
    task::Poll,
};

use futures_core::{FusedStream, Stream};
use futures_util::stream::FuturesUnordered;
use pin_project::pin_project;

// NOTE: this can't be a simple futures_util::stream::select since it will ignore the futuregroup if
// it has been empty even once.
// NOTE: this is very similar to for_each_concurrent, except that this adds futures from the outside
// instead of from the stream itself.
#[must_use = "this does nothing without being polled"]
#[pin_project]
pub struct WithFutures<S, F> {
    #[pin]
    stream: S,
    // NOTE: it's fine to poll this group over and over even though it has returned Ready(None).
    // It will start to return more results after more futures have been inserted into it.
    // TODO: futuresunordered is always unpin i think, so use poll_next_unpin instead and remove pin
    // here?
    #[pin]
    group: FuturesUnordered<F>,
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
            group: FuturesUnordered::new(),
            group_first: false,
        }
    }
}

impl<S, F> WithFutures<S, F> {
    pub fn mut_pin_group(self: Pin<&mut Self>) -> Pin<&mut FuturesUnordered<F>> {
        self.project().group
    }

    pub fn group_ref(&self) -> &FuturesUnordered<F> {
        &self.group
    }

    pub fn stream_ref(&self) -> &S {
        &self.stream
    }

    #[cfg_attr(not(test), expect(dead_code, reason = "only used in tests atm"))]
    pub fn mut_group(&mut self) -> &mut FuturesUnordered<F> {
        &mut self.group
    }
}

impl<S, F> Stream for WithFutures<S, F>
where
    S: FusedStream,
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

// NOTE: this could've almost been a futures_util::select_with_strategy, but it can't since I want
// this combinator to terminate when the stream is empty, regardless of the state of the addon, and
// select_with_strategy will poll both streams to completion.
#[must_use = "this does nothing without being polled"]
#[pin_project]
pub struct Addon<A, M> {
    #[pin]
    addon: A,
    #[pin]
    main: M,
}

impl<A, R> Addon<A, R> {
    pub fn mut_pin_main(self: Pin<&mut Self>) -> Pin<&mut R> {
        self.project().main
    }

    pub fn main_ref(&self) -> &R {
        &self.main
    }

    pub fn addon_ref(&self) -> &A {
        &self.addon
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

        // NOTE: I try to mimic the futures_util::stream::Fuse and keep the stream alive instead of
        // dropping it like futures_util::future::Fuse when it is done, since streams usually are
        // long-lived and can implement Sink and stuff. I could've also taken a normal stream as
        // argument and kept track of the done state using a bool, but that felt weird since there
        // is a fusedstream trait, it's just that I haven't seen it used as a bound in other crates,
        // so it's maybe not idomatic? I have only seen it required, i think, on the select macro, i
        // think. But I like it since it avoids unnecessary fuses.
        if !this.addon.is_terminated()
            && let Poll::Ready(Some(i)) = this.addon.poll_next(cx)
        {
            return Poll::Ready(Some(i));
        }

        this.main.poll_next(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // TODO: add the size hint of all members together
        self.main.size_hint()
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
        Addon { addon, main: self }
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

#[derive(Debug, Clone, Copy)]
pub enum YieldPolicy {
    Never,
    Every(NonZeroU32),
}

impl YieldPolicy {
    pub const fn default() -> Self {
        if cfg!(test) {
            // NOTE: makes testing easier by removing a source of Poll::pending
            Self::Never
        } else {
            Self::Every(const { NonZeroU32::new(8).unwrap() })
        }
    }

    pub const fn always() -> Self {
        Self::Every(const { NonZeroU32::new(1).unwrap() })
    }
}

pub struct Yielder<'a> {
    count: &'a Cell<Option<u32>>,
    policy: YieldPolicy,
}

impl Yielder<'_> {
    pub async fn yield_point(&self) {
        std::future::poll_fn(|cx| {
            match self.policy {
                YieldPolicy::Every(max) => {
                    if let Some(mut count) = self.count.take() {
                        count += 1;
                        if count >= max.get() {
                            cx.waker().wake_by_ref();
                            return Poll::Pending;
                        }
                        self.count.set(Some(count));
                    } else {
                        self.count.set(Some(0));
                    }
                }
                YieldPolicy::Never => (),
            }
            Poll::Ready(())
        })
        .await;
    }
}

pub async fn yield_guard<F, R>(policy: YieldPolicy, to_guard: F) -> R
where
    F: for<'a> AsyncFnOnce(Yielder<'a>) -> R,
{
    let count = Cell::new(Some(0));
    let yielder = Yielder {
        count: &count,
        policy,
    };
    let mut fut = pin!(to_guard(yielder));
    std::future::poll_fn(|cx| match fut.as_mut().poll(cx) {
        Poll::Ready(x) => Poll::Ready(x),
        Poll::Pending => {
            count.update(|count| count.map(|_| 0));
            Poll::Pending
        }
    })
    .await
}

pub trait FutureExt: Future {
    fn map<F, U>(self, f: F) -> Map<Self, F>
    where
        F: FnOnce(Self::Output) -> U,
        Self: Sized,
    {
        Map {
            fut: self,
            mapper: Some(f),
        }
    }
}

impl<F: Future> FutureExt for F {}

#[pin_project]
pub struct Map<Fut, F> {
    #[pin]
    fut: Fut,
    mapper: Option<F>,
}

impl<Fut, F, U> Future for Map<Fut, F>
where
    Fut: Future,
    F: FnOnce(Fut::Output) -> U,
{
    type Output = U;

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let res = futures_core::ready!(this.fut.poll(cx));
        Poll::Ready((this
            .mapper
            .take()
            .expect("this will exist, on the first poll at least"))(
            res
        ))
    }
}

impl<Fut, F> Map<Fut, F> {
    pub fn into_future(self) -> Fut {
        self.fut
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_test::assert_stream_pending;
    use futures_test::future::FutureTestExt as _;
    use futures_test::{assert_stream_done, assert_stream_next, stream::StreamTestExt};
    use futures_util::StreamExt as _;
    use futures_util::future as fuf;
    use futures_util::stream as fus;

    mod yielder {
        use super::*;

        #[test]
        fn never() {
            let fut = yield_guard(YieldPolicy::Never, async |guard| {
                guard.yield_point().await;
                guard.yield_point().await;
                guard.yield_point().await;
            });
            let mut fut = pin!(fut);
            assert_future_ready!(fut, ());
        }

        #[test]
        fn always() {
            let fut = yield_guard(YieldPolicy::always(), async |guard| {
                guard.yield_point().await;
                guard.yield_point().await;
                guard.yield_point().await;
            });
            let mut fut = pin!(fut);
            assert_future_pending!(fut);
            assert_future_pending!(fut);
            assert_future_pending!(fut);
            assert_future_ready!(fut, ());
        }

        #[test]
        fn every_other() {
            let i = Cell::new(-99);
            let fut = yield_guard(
                YieldPolicy::Every(const { NonZeroU32::new(2).unwrap() }),
                async |guard| {
                    for j in 0..4 {
                        i.set(j);
                        guard.yield_point().await;
                    }
                },
            );
            let mut fut = pin!(fut);
            assert_future_pending!(fut);
            assert_eq!(i.get(), 1);
            assert_future_pending!(fut);
            assert_eq!(i.get(), 3);
            assert_future_ready!(fut, ());
            assert_eq!(i.get(), 3);
        }

        #[test]
        fn restores_its_count_so_it_never_yields() {
            let i = Cell::new(-99);
            let fut = yield_guard(
                YieldPolicy::Every(const { NonZeroU32::new(2).unwrap() }),
                async |guard| {
                    for j in 0..4 {
                        i.set(j);
                        fuf::ready(()).pending_once().await;

                        i.set(-1);
                        guard.yield_point().await;
                    }
                },
            );
            let mut fut = pin!(fut);
            assert_future_pending!(fut);
            assert_eq!(i.get(), 0);

            assert_future_pending!(fut);
            assert_eq!(i.get(), 1);

            assert_future_pending!(fut);
            assert_eq!(i.get(), 2);

            assert_future_pending!(fut);
            assert_eq!(i.get(), 3);

            assert_future_ready!(fut, ());
            assert_eq!(i.get(), -1);
        }

        #[test]
        fn gets_reset_once() {
            let i = Cell::new(-99);
            let fut = yield_guard(
                YieldPolicy::Every(const { NonZeroU32::new(2).unwrap() }),
                async |guard| {
                    i.set(1);
                    guard.yield_point().await;

                    i.set(2);
                    fuf::ready(()).pending_once().await;

                    i.set(3);
                    guard.yield_point().await;

                    i.set(4);
                    guard.yield_point().await;
                },
            );
            let mut fut = pin!(fut);
            assert_future_pending!(fut);
            assert_eq!(i.get(), 2);

            assert_future_pending!(fut);
            assert_eq!(i.get(), 4);

            assert_future_ready!(fut, ());
        }
    }

    mod group {
        use super::*;

        #[test]
        fn empty() {
            let s = fus::empty::<()>();
            let mut s = s.with_future_group::<fuf::Pending<_>>();
            assert_stream_done!(s);
        }

        #[test]
        fn behaves_normally_if_the_group_is_not_used() {
            let s = fus::iter(vec![1, 2]).fuse();
            let mut s = s.with_future_group::<fuf::Pending<_>>();
            assert_stream_next!(s, 1);
            assert_stream_next!(s, 2);
            assert_stream_done!(s);
        }

        #[test]
        fn fairness() {
            let s = fus::iter(vec![1, 2]).fuse();
            let mut s = s.with_future_group();
            s.mut_group().push(fuf::ready(3));
            s.mut_group().push(fuf::ready(4));
            assert_stream_next!(s, 3);
            assert_stream_next!(s, 1);
            assert_stream_next!(s, 4);
            assert_stream_next!(s, 2);
            assert_stream_done!(s);
        }

        #[test]
        fn add_future_after_group_has_been_polled() {
            let s = fus::iter(vec![1, 2]).interleave_pending().fuse();
            let mut s = s.with_future_group();
            assert_stream_pending!(s); // NOTE: the group should have been polled here
            assert_stream_next!(s, 1);
            s.mut_group().push(fuf::ready(3));
            assert_stream_next!(s, 3);
            assert_stream_pending!(s);
            assert_stream_next!(s, 2);
            assert_stream_pending!(s);
            assert_stream_done!(s);
        }

        #[test]
        fn empty_stream_non_empty_group() {
            let s = fus::empty();
            let mut s = s.with_future_group();
            s.mut_group().push(fuf::ready(1));
            assert_stream_next!(s, 1);
            assert_stream_done!(s);
        }
    }

    mod addon {
        use super::*;

        #[test]
        fn empty() {
            let s1 = fus::empty::<()>();
            let s2 = fus::empty();
            let mut a = s1.addon(s2);
            assert_stream_done!(a);
        }

        #[test]
        fn addon_always_first() {
            let s1 = fus::repeat(1);
            let s2 = fus::repeat(2);
            let mut a = s1.addon(s2);
            assert_stream_next!(a, 2);
            assert_stream_next!(a, 2);
            assert_stream_next!(a, 2);
        }

        #[test]
        fn done_if_main_is_done() {
            let s1 = fus::iter(vec![1, 2]);
            let s2 = fus::repeat(3).interleave_pending();
            let mut a = s1.addon(s2);
            assert_stream_next!(a, 1);
            assert_stream_next!(a, 3);
            assert_stream_next!(a, 2);
            assert_stream_next!(a, 3);
            assert_stream_done!(a);
        }
    }
}
