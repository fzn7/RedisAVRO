# Redis Avro Module

An open-source Redis module that stores and serves Apache Avro and JSON payloads
at native Redis speed, enabling schema-safe ingestion, retrieval, and benchmarking
for analytics, event streaming, and data warehousing workloads on top of Redis Stack.

Redis Avro is a Redis module that stores structured records in the Apache Avro
binary format while exposing a friendly `AVRO.*` command surface.  Records are
kept as native Redis data types, so you can ingest JSON, write raw Avro payloads,
retrieve data as Avro or JSON, and benchmark against RedisJSON and plain string
storage.

The project ships with benchmarking suites:

* `make bench` – compares RAW strings, RedisJSON, validated Avro, and trusted
  Avro using synthetic user-profile data.
* `make bench-taxi` – reproduces the “NYC taxis” ingestion benchmark described
  in the RediSearch FTSB docs, showing how Avro behaves on a realistic
  numeric-heavy schema.

## Getting Started

```bash
make setup   # install Python deps (pytest, redis, faker, fastavro…)
make build   # cargo build --release
make test    # cargo test --test integration_test
```

Run the default benchmark suite (requires Redis in system and RedisJSON installed at
`/opt/homebrew/lib/rejson.so`, override with `REDISJSON_PATH` if needed):

```bash
make bench
```

Run the NYC taxis ingestion benchmark (controls via env vars):

```bash
NYC_TAXI_COUNT=100000 NYC_TAXI_BATCH=1000 make bench-taxi
```

Key environment variables:

| Variable            | Description                                       | Default   |
|---------------------|---------------------------------------------------|-----------|
| `REDISJSON_PATH`    | Path to `rejson.so` for benchmarks/tests          | `/opt/homebrew/lib/rejson.so` |
| `AVRO_BENCH_COUNT`  | Record count for `benchmarks/test_benchmark.py`   | `50000`   |
| `AVRO_BENCH_READ_COUNT` | Pipeline read iterations for benchmark        | `100000`  |
| `AVRO_BENCH_BIO_LEN`    | Size of text field in default benchmark       | `80`      |
| `NYC_TAXI_COUNT`    | Number of taxi documents to ingest in taxi bench  | `100000`  |
| `NYC_TAXI_BATCH`    | Pipeline batch size for taxi bench                | `1000`    |

## Running Inside Docker

To avoid installing Redis Stack, Rust, and Python locally you can drive both the
integration tests and the benchmark suite inside a Docker container based on
`redis/redis-stack-server`.

```bash
make docker-build          # build the test image
make docker-test           # cargo test inside the container
make docker-bench          # pytest benchmarks inside the container
make docker-shell          # optional interactive shell
```

`REDISJSON_PATH` defaults to `/opt/homebrew/lib/rejson.so` on macOS, but inside
the container it is automatically pointed to `/opt/redis-stack/lib/rejson.so`.
You can override either path with `REDISJSON_PATH=/custom/path make bench`.
Benchmark knobs like `AVRO_BENCH_COUNT` or `AVRO_BENCH_READ_COUNT` can be
overridden the same way and are forwarded into the Docker targets.

## Commands Overview

| Command                     | Description                                                                            |
|-----------------------------|----------------------------------------------------------------------------------------|
| `AVRO.SCHEMA.ADD name json [ID <u32>]` | Register an Avro schema under a logical name with an optional explicit numeric ID. |
| `AVRO.JSON.SET key schema json`        | Parse JSON and store it as binary Avro using the registered schema.                 |
| `AVRO.SET key schema [TRUSTED] payload`| Store raw Avro bytes. `TRUSTED` skips server-side validation when producer is trusted. |
| `AVRO.GET key schema`                  | Fetch raw Avro payload for a key.                                                    |
| `AVRO.JSON.GET key schema [path]`      | Fetch the payload as JSON (optionally using a JSON Pointer path).                   |
| `AVRO.MGET schema key...`              | Batched raw fetch for multiple keys.                                                 |
| `AVRO.JSON.MGET schema path key...`    | Batched JSON/path fetch for multiple keys.                                           |

The module stores a numeric schema ID with every value and verifies schema
consistency on every read, ensuring clients never deserialize data using the
wrong schema.

## Roadmap

**Stage 0 – Stabilization (Redis 8, Errors, Tests)** — ✅ completed

* Eliminate panics on corrupt Avro, wrong key types, missing schemas.
* Standardize error responses (`WRONGTYPE`, “Schema not found”, etc.).
* Cover all commands (positive + negative paths) with integration tests.

**Stage 1 – Benchmarks v2** — ✅ completed

* Compare Avro vs RedisJSON vs raw strings for memory and throughput.
* Track `used_memory_dataset`, fragmentation, RDB size, and relative savings.
* Measure write/read latency (p50/p95/p99) and CPU for each mode.

**Stage 2 – Storage v2** — 🔄 in progress

* Formalize the internal layout (magic/version, schema ID, payload, optional metadata).
* Store schema references efficiently and prepare for RediSearch indexing (partial: schema IDs now persisted and mapped both ways).

**Stage 3 – API Shape & Parity** — next up

* Audit command naming, argument order, and RESP3 compatibility.
* Document every command (Usage, Return types, Errors) in user-facing docs.
* Plan feature parity with RedisJSON (paths, updates, numeric ops) even if some commands carry a performance penalty.

**Stage 4 – RediSearch**

* Provide hooks so fields from Avro documents can be indexed via RediSearch.

**Stage 5 – Kafka Ingestion**

* Implement `AVRO.KAFKA.SET` that accepts Confluent-style payloads (magic byte + schema ID).
* Allow schema registration by explicit ID (already supported) and automatic registry lookups.

**Stage 6 – Production Readiness**

* Add structured logging, tunable cache limits, and command counters/metrics.
* Provide detailed documentation (examples, wire format, limitations).

## License

Redis Avro is dual-licensed:

1. **Open Source** – GNU Affero General Public License v3.0 (see `LICENSE`). You
   may use, modify, and redistribute the module as long as you comply with the
   AGPL’s network-copyleft obligations (publish your source and modifications to
   users of the service).
2. **Commercial License** – contact the maintainers if you need to embed Redis
   Avro in a proprietary product or want to run it without the AGPL obligations.
   We provide custom licensing terms for enterprise deployments.

Contributions are accepted under the AGPL with the understanding they may also be
made available to commercial licensees.
