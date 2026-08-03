use tracing::{Span, debug_span};

#[cfg(target_os = "linux")]
unsafe extern "C" {
    fn gettid() -> i32;
}

#[must_use = "This span must be used to do anything, consider entering it"]
pub fn thread_info_span() -> Span {
    let os_id = unsafe { gettid() };
    let rust_id = std::thread::current().id();
    let name = std::thread::current()
        .name()
        .map_or_else(|| "unnamed".to_string(), |s| s.to_string());
    debug_span!("thread", os_id, ?rust_id, name)
}
