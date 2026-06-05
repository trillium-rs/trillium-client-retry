# trillium-client-retry

[![ci][ci-badge]][ci]
[![crates.io version][version-badge]][crate]
[![docs.rs][docs-badge]][docs]
[![codecov][codecov-badge]][codecov]

[ci]: https://github.com/trillium-rs/trillium-client-retry/actions?query=workflow%3ACI
[ci-badge]: https://github.com/trillium-rs/trillium-client-retry/workflows/CI/badge.svg
[version-badge]: https://img.shields.io/crates/v/trillium-client-retry.svg?style=flat-square
[crate]: https://crates.io/crates/trillium-client-retry
[docs-badge]: https://img.shields.io/badge/docs-latest-blue.svg?style=flat-square
[docs]: https://docs.rs/trillium-client-retry
[codecov-badge]: https://codecov.io/gh/trillium-rs/trillium-client-retry/graph/badge.svg
[codecov]: https://codecov.io/gh/trillium-rs/trillium-client-retry

Automatic retry/backoff middleware for the [trillium](https://trillium.rs) HTTP client. Drop
`RetryHandler` onto a `Client` and failed requests — transport errors or retryable statuses
(`429`, `503` by default) — are re-issued with configurable backoff, honoring a server-advertised
`Retry-After`, bounded by a max-attempts count and a wall-clock budget. Idempotent methods only by
default; request bodies are replayed when they can be cloned.

## Example

```rust,no_run
use std::time::Duration;
use trillium_client::Client;
use trillium_client_retry::RetryHandler;
use trillium_testing::client_config;

let client = Client::new(client_config()).with_handler(
    RetryHandler::default()
        .with_exponential_backoff(Duration::from_millis(100))
        .with_max_attempts(5),
);
```

## Safety

This crate uses `#![forbid(unsafe_code)]`.

## License

<sup>
Licensed under either of <a href="LICENSE-APACHE">Apache License, Version
2.0</a> or <a href="LICENSE-MIT">MIT license</a> at your option.
</sup>

<br/>

<sub>
Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in this crate by you, as defined in the Apache-2.0 license, shall
be dual licensed as above, without any additional terms or conditions.
</sub>
