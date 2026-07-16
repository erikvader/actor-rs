pub fn init_tracing() {
    use tracing_subscriber::prelude::*;

    let fmt = tracing_subscriber::fmt::layer().pretty().with_test_writer();

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
