# Repository Guidelines

## Project Structure & Module Organization
- Core Redis module lives in `src/lib.rs`; it exposes `AVRO.*` commands and compiles as a `cdylib`.
- Integration tests (`tests/integration_test.rs`) spawn ephemeral Redis nodes with the release artifact, so keep fixtures compact.
- Bench scripts live in `benchmarks/test_benchmark.py`; bootstrap Python deps with `make setup`, and treat everything under `target/` as generated.

## Build, Test, and Development Commands
- `cargo build --release` or `make build` emits `target/release/libredis_avro.{dylib,so}` for Redis to load.
- `make test` (alias for `cargo test --test integration_test`) rebuilds, starts Redis, and exercises the command surface.
- `make bench` runs `pytest -s -v benchmarks/test_benchmark.py`; rely on `make run` to host the module when benchmarking interactively.
- `make run` loads the module into `redis-server`; use `make kill`/`make clean` to stop lingering daemons and purge caches.
- Always finish with `cargo fmt` and `cargo clippy -- -D warnings` before sending code for review.

## Coding Style & Naming Conventions
- Use Rust 2021 defaults: 4-space indentation, `snake_case` for functions, `SCREAMING_SNAKE_CASE` for constants such as `SCHEMA_KEY_PREFIX`.
- Bubble up validation errors via `RedisError::String` so clients receive actionable phrases (`Invalid JSON`, `Schema 'x' not found`).
- Keep module helpers private unless reuse is intentional, and name commands with the `AVRO.*` prefix to match Redis conventions.
- Run `cargo fmt` before each commit to avoid formatting churn in diffs.

## Testing Guidelines
- Every new feature needs coverage inside `tests/integration_test.rs` following the `RedisServer::new()` helper and the `test_*` naming scheme.
- Tests assume `redis-server` is on `PATH` and the module built in release mode; guard each Redis call with `expect` so failures identify the phase.
- Benchmark scripts should gate flaky scenarios via `pytest.mark.skipif` rather than comment blocks to keep the intent visible.

## Commit & Pull Request Guidelines
- Recent history shows short `wip` commits; switch to imperative subjects (`avro: cache schemas per db`) and reference issues in the body when known.
- PR descriptions must cover the motivation, Rust/Python touchpoints, and commands you ran (`make test`, `make bench`, `cargo fmt`/`clippy` outputs).
- When command responses change, include sample `redis-cli` transcripts or screenshots so reviewers can validate behavior quickly.

## Security & Configuration Notes
- Only load modules compiled from this tree via `make run`; treat binaries from elsewhere as untrusted.
- Keep secrets out of `dump.rdb` and `.env`, leave the dump local-only, and pass credentials as CLI flags or env vars when benchmarking Redis.
