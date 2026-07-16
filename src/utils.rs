use tracing::{Span, debug_span};

#[cfg(target_os = "linux")]
unsafe extern "C" {
    fn gettid() -> i32;
}

pub fn thread_info_span() -> Span {
    let os_thread_id = unsafe { gettid() };
    let rust_thread_id = std::thread::current().id();
    let thread_name = std::thread::current()
        .name()
        .map_or_else(|| "unnamed".to_string(), |s| s.to_string());
    debug_span!("thread", os_thread_id, ?rust_thread_id, thread_name)
}
