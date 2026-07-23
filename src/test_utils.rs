#[ctor::ctor(unsafe)]
pub fn init_tracing() {
    assert!(cfg!(test));
    use tracing_subscriber::prelude::*;

    let fmt = tracing_subscriber::fmt::layer()
        .pretty()
        .with_ansi(false)
        .with_test_writer();

    if tracing_subscriber::registry().with(fmt).try_init().is_ok() {
        let prev_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |arg| {
            let payload = arg.payload_as_str().unwrap_or("<some non-string payload>");
            let location = arg
                .location()
                .map(|l| l.to_string())
                .unwrap_or_else(|| "no location".to_string());
            tracing::error!(target: "panic", location, "{payload}");
            prev_hook(arg);
        }));
    }
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
