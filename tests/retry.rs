//! End-to-end tests for [`RetryHandler`] over an in-process [`ServerConnector`].

use std::{
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
    time::Duration,
};
use trillium_client::{Body, Client, KnownHeaderName::RetryAfter, Status};
use trillium_client_retry::{Backoff, Jitter, Methods, RetryHandler};
use trillium_testing::{
    ServerConnector, TestResult, futures_lite, harness, prelude::Conn as ServerConn, test,
};

/// A `RetryHandler` with backoff stripped out so tests don't actually sleep.
fn instant_retry() -> RetryHandler {
    RetryHandler::default()
        .with_backoff(Backoff::constant(Duration::ZERO).with_jitter(Jitter::None))
}

/// Builds an in-process client whose server fails (with `status`) for the first `fail_times`
/// requests, then responds 200 with `"recovered"`. Returns the client and a shared hit counter.
fn flaky(fail_times: u32, status: Status, handler: RetryHandler) -> (Client, Arc<AtomicU32>) {
    let hits = Arc::new(AtomicU32::new(0));
    let server_hits = Arc::clone(&hits);
    let server = move |conn: ServerConn| {
        let server_hits = Arc::clone(&server_hits);
        async move {
            let n = server_hits.fetch_add(1, Ordering::SeqCst);
            if n < fail_times {
                conn.with_status(status)
            } else {
                conn.ok("recovered")
            }
        }
    };
    (
        Client::new(ServerConnector::new(server)).with_handler(handler),
        hits,
    )
}

#[test(harness)]
async fn retries_until_success() -> TestResult {
    let (client, hits) = flaky(2, Status::ServiceUnavailable, instant_retry());
    let mut conn = client.get("http://example.com/").await?;
    assert_eq!(conn.status(), Some(Status::Ok));
    assert_eq!(conn.response_body().read_string().await?, "recovered");
    assert_eq!(hits.load(Ordering::SeqCst), 3, "original + 2 retries");
    Ok(())
}

#[test(harness)]
async fn gives_up_after_max_attempts_and_surfaces_last_response() -> TestResult {
    let (client, hits) = flaky(
        u32::MAX,
        Status::ServiceUnavailable,
        instant_retry().with_max_attempts(3),
    );
    let conn = client.get("http://example.com/").await?;
    assert_eq!(conn.status(), Some(Status::ServiceUnavailable));
    assert_eq!(hits.load(Ordering::SeqCst), 3, "3 total attempts, no error");
    Ok(())
}

#[test(harness)]
async fn does_not_retry_non_idempotent_by_default() -> TestResult {
    let (client, hits) = flaky(u32::MAX, Status::ServiceUnavailable, instant_retry());
    let conn = client.post("http://example.com/").await?;
    assert_eq!(conn.status(), Some(Status::ServiceUnavailable));
    assert_eq!(hits.load(Ordering::SeqCst), 1, "POST not retried");
    Ok(())
}

#[test(harness)]
async fn retries_post_when_methods_all() -> TestResult {
    let (client, hits) = flaky(
        1,
        Status::ServiceUnavailable,
        instant_retry().with_methods(Methods::All),
    );
    let conn = client.post("http://example.com/").await?;
    assert_eq!(conn.status(), Some(Status::Ok));
    assert_eq!(hits.load(Ordering::SeqCst), 2);
    Ok(())
}

#[test(harness)]
async fn does_not_retry_unconfigured_status() -> TestResult {
    let (client, hits) = flaky(u32::MAX, Status::InternalServerError, instant_retry());
    let conn = client.get("http://example.com/").await?;
    assert_eq!(conn.status(), Some(Status::InternalServerError));
    assert_eq!(hits.load(Ordering::SeqCst), 1, "500 not in default set");
    Ok(())
}

#[test(harness)]
async fn retries_configured_status() -> TestResult {
    let (client, hits) = flaky(
        1,
        Status::InternalServerError,
        instant_retry().with_statuses([Status::InternalServerError]),
    );
    let conn = client.get("http://example.com/").await?;
    assert_eq!(conn.status(), Some(Status::Ok));
    assert_eq!(hits.load(Ordering::SeqCst), 2);
    Ok(())
}

#[test(harness)]
async fn replays_static_body() -> TestResult {
    let hits = Arc::new(AtomicU32::new(0));
    let server_hits = Arc::clone(&hits);
    let server = move |mut conn: ServerConn| {
        let server_hits = Arc::clone(&server_hits);
        async move {
            if server_hits.fetch_add(1, Ordering::SeqCst) == 0 {
                conn.with_status(Status::ServiceUnavailable)
            } else {
                let body = conn.request_body_string().await.unwrap_or_default();
                conn.ok(body)
            }
        }
    };
    let client = Client::new(ServerConnector::new(server)).with_handler(instant_retry());

    let mut conn = client
        .put("http://example.com/")
        .with_body("replayed")
        .await?;
    assert_eq!(conn.status(), Some(Status::Ok));
    assert_eq!(conn.response_body().read_string().await?, "replayed");
    assert_eq!(hits.load(Ordering::SeqCst), 2);
    Ok(())
}

#[test(harness)]
async fn does_not_retry_one_shot_body() -> TestResult {
    let (client, hits) = flaky(u32::MAX, Status::ServiceUnavailable, instant_retry());
    let body = Body::new_streaming(futures_lite::io::Cursor::new(b"stream".to_vec()), Some(6));
    let conn = client.put("http://example.com/").with_body(body).await?;
    assert_eq!(conn.status(), Some(Status::ServiceUnavailable));
    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "streaming body can't replay"
    );
    Ok(())
}

#[test(harness)]
async fn honors_retry_after_header() -> TestResult {
    let hits = Arc::new(AtomicU32::new(0));
    let server_hits = Arc::clone(&hits);
    let server = move |conn: ServerConn| {
        let server_hits = Arc::clone(&server_hits);
        async move {
            if server_hits.fetch_add(1, Ordering::SeqCst) == 0 {
                conn.with_status(Status::ServiceUnavailable)
                    .with_response_header(RetryAfter, "0")
            } else {
                conn.ok("recovered")
            }
        }
    };
    let client = Client::new(ServerConnector::new(server)).with_handler(instant_retry());
    let conn = client.get("http://example.com/").await?;
    assert_eq!(conn.status(), Some(Status::Ok));
    assert_eq!(hits.load(Ordering::SeqCst), 2);
    Ok(())
}
