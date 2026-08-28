#[ctor::ctor(unsafe)]
pub fn init_tracing() {
    assert!(cfg!(test));
    use tracing_subscriber::prelude::*;

    let fmt = tracing_subscriber::fmt::layer()
        .with_ansi(false)
        .with_test_writer();

    tracing_subscriber::registry().with(fmt).init();
}

pub fn assert_span(expected_name: &'static str) {
    let span = tracing::Span::current()
        .metadata()
        .expect("Not entered in a span");
    assert_eq!(
        span.name(),
        expected_name,
        "the current span doesn't have the expected name"
    );
}

pub fn assert_parent_span(expected_name: &'static str) {
    use tracing_subscriber::registry::LookupSpan;

    tracing::dispatcher::get_default(|dispatch| {
        let current = dispatch.current_span();
        let cur_id = current.id().expect("Not entered in a span");

        let registry = dispatch
            .downcast_ref::<tracing_subscriber::Registry>()
            .expect("There should be a registry here");

        let curr_ref = registry
            .span(cur_id)
            .expect("Current span didn't exist in registry");

        let parent_ref = curr_ref
            .parent()
            .expect("Current span doesn't have a parent");

        assert_eq!(
            parent_ref.name(),
            expected_name,
            "the parent span didn't have the expected name"
        );
    });
}

// NOTE: these are inspired by assert_stream_next etc from futures_test
macro_rules! assert_future_pending {
    (pin $fut:expr) => {{
        let mut fut = core::pin::pin!($fut);
        assert_future_pending!(fut);
    }};
    ($fut:expr) => {{
        use core::future::Future;
        let fut = core::pin::Pin::new(&mut $fut);
        let res = fut.poll(&mut futures_test::task::noop_context());
        assert!(res.is_pending(), "Future is not pending");
    }};
}

macro_rules! assert_future_ready {
    (unpin $fut:expr) => {{
        let mut fut = $fut;
        assert_future_ready!(fut)
    }};
    (unpin $fut:expr, $($args:tt)+) => {{
        let mut fut = $fut;
        assert_future_ready!(fut, $($args)+)
    }};
    (pin $fut:expr) => {{
        let mut fut = core::pin::pin!($fut);
        assert_future_ready!(fut)
    }};
    (pin $fut:expr, $($args:tt)+) => {{
        let mut fut = core::pin::pin!($fut);
        assert_future_ready!(fut, $($args)+)
    }};
    ($fut:expr, $param:ident => $pred:expr) => {{
        use core::future::Future;
        let fut = core::pin::Pin::new(&mut $fut);
        let res = fut.poll(&mut futures_test::task::noop_context());
        match res {
            core::task::Poll::Ready(res) => {
                #[allow(unused_mut, reason="to make it usable in more situations")]
                let mut $param = res;
                assert!($pred);
            }
            core::task::Poll::Pending => panic!("Future is not ready"),
        }
    }};
    ($fut:expr, $expected:expr) => {{
        use core::future::Future;
        let fut = core::pin::Pin::new(&mut $fut);
        let res = fut.poll(&mut futures_test::task::noop_context());
        match res {
            core::task::Poll::Ready(res) => {
                assert_eq!(res, $expected, "Ready value not equal the expected value");
                res
            }
            core::task::Poll::Pending => panic!("Future is not ready"),
        }
    }};
    ($fut:expr) => {{
        use core::future::Future;
        let fut = core::pin::Pin::new(&mut $fut);
        let res = fut.poll(&mut futures_test::task::noop_context());
        match res {
            core::task::Poll::Ready(res) => res,
            core::task::Poll::Pending => panic!("Future is not ready"),
        }
    }};
}
