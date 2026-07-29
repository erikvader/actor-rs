use async_channel as channel;
use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(display("Runes dropped while panicking"))]
pub struct PanicError;

#[derive(Clone)]
pub struct Rune {
    inner: channel::Sender<()>,
}

impl Drop for Rune {
    fn drop(&mut self) {
        if std::thread::panicking() {
            tracing::debug!("Rune dropped while panicking");
            let _: Result<_, _> = self.inner.try_send(());
        }
    }
}

/// insipiration: https://dishonored.fandom.com/wiki/The_Heart
// TODO: this could also be an IntoFuture, maybe? I think it requires boxing the future though.
// NOTE: this is not supposed to be Clone, it can't track panics reliably if it did.
pub struct Heart {
    // RANT: this channel is !Unpin even though it doesn't have to be
    inner: channel::Receiver<()>,
}

impl Heart {
    pub fn rune_count(&self) -> usize {
        self.inner.sender_count()
    }

    pub async fn wait(&self) -> Result<(), PanicError> {
        // TODO: this could clone the receiver and return a future that is static
        let mut panicked = false;
        while let Ok(()) = self.inner.recv().await {
            panicked = true;
        }

        if panicked { PanicSnafu.fail() } else { Ok(()) }
    }
}

pub fn create() -> (Heart, Rune) {
    // NOTE: this is unbounded so the drop of a rune doesn't ever block
    let (snd, rcv) = channel::unbounded();
    (Heart { inner: rcv }, Rune { inner: snd })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn future_can_be_waited_multiple_times() {
        let (heart, rune) = create();
        assert_eq!(heart.rune_count(), 1);
        assert_future_pending!(pin heart.wait());
        assert_future_pending!(pin heart.wait());

        drop(rune);
        assert_eq!(heart.rune_count(), 0);
        assert_future_ready!(pin heart.wait(), res => res.is_ok());
        assert_future_ready!(pin heart.wait(), res => res.is_ok());
    }

    #[test]
    fn counting_one_panic() {
        let (heart, rune) = create();
        let _res = std::panic::catch_unwind(move || {
            let _rune = rune;
            panic!("omg!");
        });
        assert_future_ready!(pin heart.wait(), err => err.is_err());
    }

    #[test]
    fn not_done_until_all_runes_are_gone_even_while_panicking() {
        let (heart, rune) = create();
        let _res = std::panic::catch_unwind({
            let rune = rune.clone();
            move || {
                let _rune = rune;
                panic!("omg!");
            }
        });

        assert_future_pending!(pin heart.wait());

        let _res = std::panic::catch_unwind(move || {
            let _rune = rune;
            panic!("omg!");
        });

        assert_future_ready!(pin heart.wait(), err => err.is_err());
    }
}
