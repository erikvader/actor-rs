// TODO: rename this module to thread_utils since there is only thread related functions here?
use tracing::{info_span, span::EnteredSpan};

// TODO: fallback or support for other platforms
#[cfg(target_os = "linux")]
unsafe extern "C" {
    fn gettid() -> i32;
}

// NOTE: it feels wrong to return an entered span, but it feels even more wrong to NOT immediately
// enter the span returned from this. I could probably make this pub(crate), but i want to support
// users that spawn threads by their own means.
#[must_use = "This span must be saved to a variable, it will instantly get dropped otherwise"]
pub fn thread_info_span() -> EnteredSpan {
    // SAFETY: i don't think this function can fail
    let os_id = unsafe { gettid() };
    let current = std::thread::current();
    let rust_id = current.id();
    let name = current
        .name()
        .map_or_else(|| "unnamed".to_string(), |s| s.to_string());

    // TODO: it would be cool if the internal thread id could be appended to the name to make it
    // 100% unique. The name is nice since it is more describing than just a number.
    let entered = info_span!("thread", name).entered();

    // RANT: the rust thread is can't be printed in a compact way as just a number, it has to
    // include the whole debug output with its type and stuff.
    tracing::debug!(os_id, ?rust_id, name);

    entered
}

pub fn spawn<F, T>(name: impl Into<String>, fun: F) -> std::thread::JoinHandle<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    std::thread::Builder::new()
        .name(name.into())
        .spawn(move || {
            let _span = thread_info_span();
            fun()
        })
        .expect("failed to spawn thread")
}
