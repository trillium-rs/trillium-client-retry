//! Confirms that an observer handler (here, `ClientLogger`) composed with `RetryHandler` sees every
//! attempt, and that this holds regardless of their order in the tuple — what matters is that retry
//! is the only handler re-driving the conn.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
    time::Duration,
};
use trillium_client::{Client, ClientHandler, Status};
use trillium_client_retry::RetryHandler;
use trillium_logger::{
    ColorMode, Targetable,
    client::{ClientLogger, formatters},
};
use trillium_testing::{ServerConnector, TestResult, harness, prelude::Conn as ServerConn, test};

#[derive(Clone, Debug)]
struct TestTarget(
    Arc<(
        async_channel::Sender<String>,
        async_channel::Receiver<String>,
    )>,
);

impl Default for TestTarget {
    fn default() -> Self {
        Self(Arc::new(async_channel::unbounded()))
    }
}

impl Targetable for TestTarget {
    fn write(&self, data: String) {
        self.0.0.send_blocking(data).unwrap();
    }
}

impl TestTarget {
    fn drain(&self) -> Vec<String> {
        let mut lines = vec![];
        while let Ok(line) = self.0.1.try_recv() {
            lines.push(line);
        }
        lines
    }
}

/// Backoff stripped out so the test doesn't actually sleep.
fn instant_retry() -> RetryHandler {
    RetryHandler::default()
        .with_constant_backoff(Duration::ZERO)
        .without_jitter()
}

/// A logger that writes just the status of each completed attempt to `target`.
fn status_logger(target: TestTarget) -> impl ClientHandler {
    ClientLogger::new()
        .with_target(target)
        .with_color_mode(ColorMode::Off)
        .with_formatter(formatters::status)
}

/// A `ServerConnector`-backed client that fails twice with 503, then responds 200.
fn flaky_client(handler: impl ClientHandler) -> Client {
    let hits = Arc::new(AtomicU32::new(0));
    let server = move |conn: ServerConn| {
        let hits = Arc::clone(&hits);
        async move {
            if hits.fetch_add(1, Ordering::SeqCst) < 2 {
                conn.with_status(Status::ServiceUnavailable)
            } else {
                conn.ok("recovered")
            }
        }
    };
    Client::new(ServerConnector::new(server)).with_handler(handler)
}

#[test(harness)]
async fn logger_observes_every_attempt_retry_first() -> TestResult {
    let target = TestTarget::default();
    let client = flaky_client((instant_retry(), status_logger(target.clone())));

    let conn = client.get("http://example.com/").await?;
    assert_eq!(conn.status(), Some(Status::Ok));
    assert_eq!(target.drain(), ["503", "503", "200"]);
    Ok(())
}

#[test(harness)]
async fn logger_observes_every_attempt_logger_first() -> TestResult {
    let target = TestTarget::default();
    let client = flaky_client((status_logger(target.clone()), instant_retry()));

    let conn = client.get("http://example.com/").await?;
    assert_eq!(conn.status(), Some(Status::Ok));
    assert_eq!(target.drain(), ["503", "503", "200"]);
    Ok(())
}
