use actor_rs::whatever::*;
use tracing::{Level, debug, info, instrument};
use tracing_error::{ErrorLayer, SpanTrace};
use tracing_subscriber::prelude::*;

#[snafu::report]
fn main() -> Result<(), Whatever> {
    let sub = tracing_subscriber::fmt()
        .without_time()
        .with_target(false)
        .with_max_level(Level::TRACE)
        .compact()
        .finish()
        .with(ErrorLayer::default());
    tracing::subscriber::set_global_default(sub).unwrap();

    info!("test");
    let t = a(5);
    a(25);
    // println!("{t}");

    failes2()?;
    Ok(())
}

#[instrument]
fn failes() -> Result<(), Whatever> {
    whatever!("omg");
}

#[instrument]
fn failes2() -> Result<(), Whatever> {
    failes().whatever_context("hej")?;
    Ok(())
}

#[instrument(level = "debug")]
fn a(x: i32) -> SpanTrace {
    let val = x + 5;
    info!("Val={val}");
    b(val)
}

#[instrument]
fn b(x: i32) -> SpanTrace {
    let val = x * x;
    debug!("Val={val}");

    SpanTrace::capture()
}
