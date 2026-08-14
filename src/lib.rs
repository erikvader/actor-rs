#[cfg(test)]
#[macro_use]
mod test_utils;
pub mod utils;

// TODO: think about how the public interface should look. Most crates seem to stick to three layers
// maximum, i.e. std::task::Waker
pub mod actor;
pub mod actors;
mod heart;
mod kill_switch;
pub mod signals;
mod stream_utils;

// TODO: should this really be here?
pub mod whatever;
