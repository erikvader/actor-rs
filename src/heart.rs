use std::task::Poll;

use async_channel as channel;
use async_io::block_on;
use futures_core::Stream;
use pin_project::pin_project;
use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(display("{count} runes dropped while panicking"))]
pub struct PanicError {
    count: usize,
}

#[derive(Clone)]
pub struct Rune {
    inner: channel::Sender<()>,
}

impl Rune {
    pub fn kill_heart(&self) {
        self.inner.close();
    }
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
#[pin_project]
pub struct Heart {
    #[pin]
    inner: channel::Receiver<()>,
    panics: usize,
}

impl Clone for Heart {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            panics: 0,
        }
    }
}

impl Heart {
    pub fn is_dead(&self) -> bool {
        self.inner.is_closed()
    }

    pub fn await_blocking(self) -> Result<(), PanicError> {
        // NOTE: the block_on from async_io will spin up a reactor and stuff, which is not needed
        // here, but i don't want to bring in another executor dependency just for this.
        // Futures-lite has its own block_on, but futures-util doesn't have an equivalent, so i have
        // to bring in futures-executor or smth. I could simply use recv_blocking on the channel,
        // but that would mean duplicating the panic counter logic, so I would prefer not to.
        block_on(self)
    }

    pub fn rune_count(&self) -> usize {
        self.inner.sender_count()
    }
}

impl Future for Heart {
    type Output = Result<(), PanicError>;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();

        loop {
            match this.inner.as_mut().poll_next(cx) {
                Poll::Ready(Some(())) => {
                    *this.panics += 1;
                }
                Poll::Ready(None) if *this.panics == 0 => return Poll::Ready(Ok(())),
                Poll::Ready(None) => {
                    return Poll::Ready(Err(PanicError {
                        count: *this.panics,
                    }));
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

pub fn create() -> (Heart, Rune) {
    let (s, r) = channel::unbounded();
    (
        Heart {
            inner: r,
            panics: 0,
        },
        Rune { inner: s },
    )
}
