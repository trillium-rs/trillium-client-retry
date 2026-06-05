# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Initial release: `RetryHandler`, a `trillium-client` `ClientHandler` that retries failed
  requests with configurable backoff.
- `Backoff` schedule (`constant` / `linear` / `exponential` / `custom`) with `Jitter` and an
  optional max-delay cap.
- Retry decision configurable via `with_statuses`, `with_methods` (`Methods::Idempotent` /
  `Methods::All`), and `with_transport_errors`, with `retry_when` and `with_decision` escape
  hatches.
- Limits: `with_max_attempts` and a `with_max_elapsed` wall-clock budget that clamps each
  attempt's timeout.
- `Retry-After` (delta-seconds) honored by default, capped by `with_max_retry_after`.
- Request bodies are replayed only when cloneable; one-shot streaming bodies are not retried.
