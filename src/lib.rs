#[cfg(test)]
#[macro_use]
mod test_utils;
pub mod utils;

// TODO: think about how the public interface should look. Most crates seem to stick to three layers
// maximum, i.e. std::task::Waker
pub mod actor;
pub mod actors;
pub mod deferred_span;
pub mod fmt_utils;
mod heart;
mod kill_switch;
pub mod signals;
mod stream_utils;

// NOTE: this will be part of the public API
pub use rootcause;
pub use rootcause_tracing;

#[doc(hidden)]
mod private {
    #[allow(
        unnameable_types,
        reason = "this is on purpose, this is how sealed traits are done"
    )]
    pub trait Sealed {}
}
