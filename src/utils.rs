use tracing::{Span, debug_span};

// TODO: fallback or support for other platforms
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

pub struct DeferredSpan<'a> {
    creator: Box<dyn FnOnce(&Span) -> Span + 'a>,
}

impl<'a> DeferredSpan<'a> {
    // NOTE: this one should actually be private, but it's used by the macro
    pub fn new(creator: Box<dyn FnOnce(&Span) -> Span + 'a>) -> Self {
        Self { creator }
    }

    pub fn none() -> Self {
        Self::new(Box::new(|_| Span::none()))
    }

    pub(crate) fn create(self, parent: &Span) -> Option<Span> {
        let span = (self.creator)(parent);
        if span.is_disabled() {
            return None;
        }
        Some(span)
    }

    pub(crate) fn create_or_parent(self, parent: Span) -> Span {
        self.create(&parent).unwrap_or(parent)
    }
}

#[macro_export]
macro_rules! deferred_span {
    (target: $target:expr, $($tokens:tt)*) => {
        // NOTE: it seems from the definition of tracing::span!, that target is the only thing that
        // comes before parent
        $crate::utils::DeferredSpan::new(Box::new(move |parent| tracing::span!(target: $target, parent: parent, $($tokens)*)))
    };
    ($($tokens:tt)*) => {
        $crate::utils::DeferredSpan::new(Box::new(move |parent| tracing::span!(parent: parent, $($tokens)*)))
    };
}

#[macro_export]
macro_rules! deferred_info_span {
    (target: $target:expr, $($tokens:tt)*) => {
        // NOTE: it seems from the definition of tracing::span!, that target and parent are the only
        // things that comes before the level
        $crate::deferred_span!(target: $target, tracing::Level::INFO, $($tokens)*)
    };
    ($($tokens:tt)*) => {
        $crate::deferred_span!(tracing::Level::INFO, $($tokens)*)
    };
}

#[cfg(test)]
mod tests {
    use tracing::Level;

    #[test]
    fn deferred_span_create() {
        let parent = tracing::info_span!("parent");
        let def_child = deferred_span!(Level::INFO, "child");
        let child = def_child.create(&parent).expect("should be active");

        // TODO: somehow test that the parent link is correct?
        assert_eq!(
            child
                .metadata()
                .expect("there should be a test subscriber")
                .name(),
            "child"
        );
    }

    #[test]
    fn deferred_span_create_with_target() {
        let parent = tracing::info_span!("parent");
        let def_child = deferred_span!(target: "target::target", Level::INFO, "child");
        let child = def_child.create(&parent).expect("should be active");

        let metadata = child.metadata().expect("there should be a test subscriber");
        assert_eq!(metadata.name(), "child");
        assert_eq!(metadata.target(), "target::target");
    }
}
