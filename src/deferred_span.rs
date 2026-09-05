use tracing::Span;

pub struct DeferredDirectSpan<F> {
    creator: F,
}

pub type DeferredSpan<'a> = DeferredDirectSpan<Box<dyn FnOnce(&Span) -> Span + 'a>>;

impl<'a> DeferredSpan<'a> {
    pub fn none() -> Self {
        Self::new(Box::new(|_| Span::none()))
    }
}

// HACK: for the macros to find these private methods
pub(crate) use __private::DeferredSpanMethods;
pub mod __private {
    use super::*;

    pub trait DeferredSpanMethods<F>
    where
        Self: Sized,
    {
        fn new(creator: F) -> Self;
        fn call(self, parent: &Span) -> Span;
        fn create(self, parent: &Span) -> Option<Span>;
        fn create_or(self, parent: Span) -> Span;
    }

    impl<F> DeferredSpanMethods<F> for DeferredDirectSpan<F>
    where
        F: FnOnce(&Span) -> Span,
    {
        fn new(creator: F) -> Self {
            Self { creator }
        }

        fn call(self, parent: &Span) -> Span {
            (self.creator)(parent)
        }

        fn create(self, parent: &Span) -> Option<Span> {
            let span = self.call(parent);
            if span.is_disabled() {
                return None;
            }
            Some(span)
        }

        // RANT: this is annoying to chain, but I can't come up with a much nicer solution. Operator
        // overloading doesn't really help since all of them are left-associative, and i can't make
        // them private. Creating a method that /can/ be chained requires wrapping and unwrapping
        // span. A macro for this feels too overkill, and it has its own drawbacks. This method /is/
        // private, so it doesn't have to be completely ergonomic, and there is currently only one
        // place that has a chain of more than two spans.
        fn create_or(self, parent: Span) -> Span {
            self.create(&parent).unwrap_or(parent)
        }
    }
}

#[macro_export]
macro_rules! deferred_span {
    (target: $target:expr, $($tokens:tt)*) => {
        {
            use $crate::deferred_span::__private::DeferredSpanMethods;
            // NOTE: it seems from the definition of tracing::span!, that target is the only thing that
            // comes before parent
            $crate::deferred_span::DeferredSpan::new(
                std::boxed::Box::new(
                    move |parent| tracing::span!(target: $target, parent: parent, $($tokens)*)
                )
            )
        }
    };
    ($($tokens:tt)*) => {
        {
            use $crate::deferred_span::__private::DeferredSpanMethods;
            $crate::deferred_span::DeferredSpan::new(
                std::boxed::Box::new(
                    move |parent| tracing::span!(parent: parent, $($tokens)*)
                )
            )
        }
    };
}

#[macro_export]
macro_rules! deferred_direct_span {
    (target: $target:expr, $($tokens:tt)*) => {
        {
            use $crate::deferred_span::__private::DeferredSpanMethods;
            // NOTE: it seems from the definition of tracing::span!, that target is the only thing that
            // comes before parent
            $crate::deferred_span::DeferredDirectSpan::new(
                move |parent| tracing::span!(target: $target, parent: parent, $($tokens)*)
            )
        }
    };
    ($($tokens:tt)*) => {
        {
            use $crate::deferred_span::__private::DeferredSpanMethods;
            $crate::deferred_span::DeferredDirectSpan::new(
                move |parent| tracing::span!(parent: parent, $($tokens)*)
            )
        }
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

// NOTE: this became a macro instead of a function or operator overload to drastically reduce the
// number of heap allocations
#[macro_export]
macro_rules! deferred_span_or {
    (@internal $parent:ident; $last:expr $(;)?) => {
        $last.call($parent)
    };
    (@internal $parent:ident; $span:expr ; $($rest:tt)*) => {
        {
            if let Some(span) = $span.create($parent) {
                return span;
            }
            $crate::deferred_span_or!(@internal $parent; $($rest)*)
        }
    };
    ($($tokens:tt)*) => {
        {
            use $crate::deferred_span::__private::DeferredSpanMethods;
            $crate::deferred_span::DeferredSpan::new(
                std::boxed::Box::new(
                    move |parent| $crate::deferred_span_or!(@internal parent; $($tokens)*)
                )
            )
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing::Level;

    #[test]
    fn create() {
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
    fn create_with_target() {
        let parent = tracing::info_span!("parent");
        let def_child = deferred_span!(target: "target::target", Level::INFO, "child");
        let child = def_child.create(&parent).expect("should be active");

        let metadata = child.metadata().expect("there should be a test subscriber");
        assert_eq!(metadata.name(), "child");
        assert_eq!(metadata.target(), "target::target");
    }

    #[test]
    fn target_from_definition_site() {
        mod hej {
            use super::*;

            pub const MODULE_PATH: &str = std::module_path!();

            pub fn get_span() -> DeferredSpan<'static> {
                deferred_span!(Level::INFO, "span")
            }
        }

        let span = hej::get_span()
            .create(&Span::none())
            .expect("should be active");

        let meta = span.metadata().unwrap();
        assert_eq!(meta.target(), hej::MODULE_PATH);
    }

    #[test]
    fn choosing_the_first_with_an_or() {
        let deferred = deferred_span_or!(
            deferred_info_span!("first");
            deferred_info_span!("second");
        );
        let span = deferred.create(&Span::none()).expect("should be active");

        assert_eq!(
            span.metadata()
                .expect("there should be a test subscriber")
                .name(),
            "first"
        );
    }

    #[test]
    fn choosing_the_second_with_an_or() {
        let deferred = deferred_span_or!(
            DeferredSpan::none();
            deferred_info_span!("second");
        );
        let span = deferred.create(&Span::none()).expect("should be active");

        assert_eq!(
            span.metadata()
                .expect("there should be a test subscriber")
                .name(),
            "second"
        );
    }

    #[test]
    fn or_correct_evaluation_order() {
        let order = std::cell::RefCell::<Vec<i32>>::new(vec![]);
        let order_ref = &order;
        let deferred = deferred_span_or! {
            DeferredDirectSpan::new(|parent| {
                order_ref.borrow_mut().push(1);
                parent.clone()
            });
                DeferredDirectSpan::new(|parent| {
                order_ref.borrow_mut().push(2);
                parent.clone()
            });
                DeferredDirectSpan::new(|parent| {
                order_ref.borrow_mut().push(3);
                parent.clone()
            });
        };
        deferred.create(&Span::none());

        assert_eq!(vec![1, 2, 3], order.take());
    }
}
