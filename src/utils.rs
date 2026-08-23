// TODO: rename this file to thread_utils or smth?
use tracing::{Span, debug_span, info_span};

// TODO: fallback or support for other platforms
#[cfg(target_os = "linux")]
unsafe extern "C" {
    fn gettid() -> i32;
}

#[must_use = "This span must be used to do anything, consider entering it"]
pub fn thread_info_span() -> Span {
    let os_id = unsafe { gettid() };
    let current = std::thread::current();
    let rust_id = current.id();
    let name = current
        .name()
        .map_or_else(|| "unnamed".to_string(), |s| s.to_string());

    let dbg = debug_span!("thread", os_id, ?rust_id, name);
    if !dbg.is_disabled() {
        return dbg;
    }

    info_span!("thread", name)
}

pub fn spawn<F, T>(name: impl Into<String>, fun: F) -> std::thread::JoinHandle<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    std::thread::Builder::new()
        .name(name.into())
        .spawn(move || {
            let _span = thread_info_span().entered();
            fun()
        })
        .expect("failed to spawn thread")
}
