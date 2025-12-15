import datetime
import io
import json
import os
import platform
import socket
import subprocess
import time
from enum import Enum

import numpy as np
import pytest
import redis
from faker import Faker
from fastavro import parse_schema, schemaless_writer

NYC_COUNT = int(os.getenv("NYC_TAXI_COUNT", "100000"))
NYC_BATCH = int(os.getenv("NYC_TAXI_BATCH", "1000"))
NYC_SCHEMA_NAME = "nyc_taxi_v1"
NYC_SCHEMA = json.dumps({
    "type": "record",
    "name": "NycTaxi",
    "fields": [
        {"name": "total_amount", "type": "double"},
        {"name": "improvement_surcharge", "type": "double"},
        {"name": "pickup_location_long_lat", "type": "string"},
        {"name": "pickup_datetime", "type": "string"},
        {"name": "trip_type", "type": "string"},
        {"name": "dropoff_datetime", "type": "string"},
        {"name": "rate_code_id", "type": "string"},
        {"name": "tolls_amount", "type": "double"},
        {"name": "dropoff_location_long_lat", "type": "string"},
        {"name": "passenger_count", "type": "int"},
        {"name": "fare_amount", "type": "double"},
        {"name": "extra", "type": "double"},
        {"name": "trip_distance", "type": "double"},
        {"name": "tip_amount", "type": "double"},
        {"name": "store_and_fwd_flag", "type": "string"},
        {"name": "payment_type", "type": "string"},
        {"name": "mta_tax", "type": "double"},
        {"name": "vendor_id", "type": "string"},
    ],
})
PARSED_SCHEMA = parse_schema(json.loads(NYC_SCHEMA))


def get_module_path():
    system = platform.system()
    ext = ".dylib" if system == "Darwin" else ".so"
    if system == "Windows":
        ext = ".dll"
    paths = [
        f"./target/release/libredis_avro{ext}",
        f"../target/release/libredis_avro{ext}",
        os.getenv("REDIS_AVRO_PATH", ""),
    ]
    for path in paths:
        if path and os.path.exists(path):
            return os.path.abspath(path)
    pytest.fail("Module not found. Run `cargo build --release`." )


MODULE_PATH = get_module_path()


def resolve_redisjson_module():
    candidates = [
        os.getenv("REDISJSON_PATH"),
        "/usr/lib/redis/modules/rejson.so",
        "/usr/local/lib/redis/modules/rejson.so",
        "/opt/homebrew/lib/rejson.so",
        "/opt/homebrew/lib/redis/modules/rejson.so",
    ]
    for path in candidates:
        if path and os.path.exists(path):
            return os.path.abspath(path)
    pytest.skip("RedisJSON module not available. Set REDISJSON_PATH.")


REDISJSON_MODULE = resolve_redisjson_module()


def get_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


class Mode(str, Enum):
    RAW = "RAW"
    REDISJSON = "REDISJSON"
    AVRO_VALIDATED = "AVRO_VALIDATED"
    AVRO_TRUSTED = "AVRO_TRUSTED"


@pytest.fixture(scope="module")
def redis_server():
    port = get_free_port()
    cmd = [
        "redis-server",
        "--port",
        str(port),
        "--save",
        "",
        "--appendonly",
        "no",
        "--loadmodule",
        MODULE_PATH,
        "--loadmodule",
        REDISJSON_MODULE,
        "--loglevel",
        "warning",
    ]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    time.sleep(1.5)
    client = redis.Redis(host="localhost", port=port, decode_responses=False)
    try:
        client.ping()
    except redis.ConnectionError:
        out, err = proc.communicate()
        pytest.fail(f"Redis failed to start.\nStdout: {out}\nStderr: {err}")
    yield client, proc.pid
    proc.terminate()
    proc.wait()


@pytest.fixture(scope="module")
def taxi_rows():
    fake = Faker()
    rows = []
    for _ in range(NYC_COUNT):
        pickup = fake.date_time_between(start_date='-30d', end_date='now')
        dropoff = pickup + datetime.timedelta(minutes=fake.random_int(min=5, max=120))
        row = {
            "total_amount": round(fake.random.uniform(3, 200), 2),
            "improvement_surcharge": 0.3,
            "pickup_location_long_lat": f"{fake.longitude():.6f},{fake.latitude():.6f}",
            "pickup_datetime": pickup.strftime("%Y-%m-%d %H:%M:%S"),
            "trip_type": fake.random_element(elements=("1", "2")),
            "dropoff_datetime": dropoff.strftime("%Y-%m-%d %H:%M:%S"),
            "rate_code_id": str(fake.random_int(min=1, max=6)),
            "tolls_amount": round(fake.random.uniform(0, 29), 2),
            "dropoff_location_long_lat": f"{fake.longitude():.6f},{fake.latitude():.6f}",
            "passenger_count": fake.random_int(min=1, max=6),
            "fare_amount": round(fake.random.uniform(3, 150), 2),
            "extra": round(fake.random.uniform(0, 10), 2),
            "trip_distance": round(fake.random.uniform(0.1, 30), 2),
            "tip_amount": round(fake.random.uniform(0, 50), 2),
            "store_and_fwd_flag": fake.random_element(elements=("Y", "N")),
            "payment_type": str(fake.random_int(min=1, max=5)),
            "mta_tax": 0.5,
            "vendor_id": fake.random_element(elements=("1", "2")),
        }
        rows.append(row)
    return rows


@pytest.fixture(scope="module")
def taxi_payloads(taxi_rows):
    json_payloads = [json.dumps(row) for row in taxi_rows]
    avro_payloads = []
    for row in taxi_rows:
        buf = io.BytesIO()
        schemaless_writer(buf, PARSED_SCHEMA, row)
        avro_payloads.append(buf.getvalue())
    return json_payloads, avro_payloads
    fake = Faker()
    rows = []
    for _ in range(NYC_COUNT):
        pickup = fake.date_time_between(start_date='-30d', end_date='now')
        dropoff = pickup + datetime.timedelta(minutes=fake.random_int(min=5, max=120))
        row = {
            "total_amount": round(fake.random.uniform(3, 200), 2),
            "improvement_surcharge": 0.3,
            "pickup_location_long_lat": f"{fake.longitude():.6f},{fake.latitude():.6f}",
            "pickup_datetime": pickup.strftime("%Y-%m-%d %H:%M:%S"),
            "trip_type": fake.random_element(elements=("1", "2")),
            "dropoff_datetime": dropoff.strftime("%Y-%m-%d %H:%M:%S"),
            "rate_code_id": str(fake.random_int(min=1, max=6)),
            "tolls_amount": round(fake.random.uniform(0, 29), 2),
            "dropoff_location_long_lat": f"{fake.longitude():.6f},{fake.latitude():.6f}",
            "passenger_count": fake.random_int(min=1, max=6),
            "fare_amount": round(fake.random.uniform(3, 150), 2),
            "extra": round(fake.random.uniform(0, 10), 2),
            "trip_distance": round(fake.random.uniform(0.1, 30), 2),
            "tip_amount": round(fake.random.uniform(0, 50), 2),
            "store_and_fwd_flag": fake.random_element(elements=("Y", "N")),
            "payment_type": str(fake.random_int(min=1, max=5)),
            "mta_tax": 0.5,
            "vendor_id": fake.random_element(elements=("1", "2")),
        }
        rows.append(row)
    return rows


def pipeline_writes(r: redis.Redis, json_payloads, avro_payloads, mode: Mode):
    pipe = r.pipeline()
    latencies = []
    for idx in range(len(json_payloads)):
        key = f"taxi:{idx}"
        if mode == Mode.RAW:
            pipe.set(key, json_payloads[idx])
        elif mode == Mode.REDISJSON:
            pipe.execute_command("JSON.SET", key, ".", json_payloads[idx])
        elif mode == Mode.AVRO_VALIDATED:
            pipe.execute_command("AVRO.SET", key, NYC_SCHEMA_NAME, avro_payloads[idx])
        else:
            pipe.execute_command("AVRO.SET", key, NYC_SCHEMA_NAME, "TRUSTED", avro_payloads[idx])
        if (idx + 1) % NYC_BATCH == 0:
            start = time.perf_counter()
            pipe.execute()
            latencies.append((time.perf_counter() - start) * 1000)
    if len(json_payloads) % NYC_BATCH:
        start = time.perf_counter()
        pipe.execute()
        latencies.append((time.perf_counter() - start) * 1000)
    return latencies


def summarize(latencies):
    if not latencies:
        return 0, 0
    arr = np.array(latencies)
    return np.percentile(arr, 50), np.percentile(arr, 99)


@pytest.mark.skipif(NYC_COUNT <= 0, reason="NYC_COUNT must be positive")
def test_nyc_taxi_ingest(redis_server, taxi_payloads):
    r, _ = redis_server
    json_payloads, avro_payloads = taxi_payloads
    results = {}
    for mode in (Mode.RAW, Mode.REDISJSON, Mode.AVRO_VALIDATED, Mode.AVRO_TRUSTED):
        r.flushall()
        if mode in (Mode.AVRO_VALIDATED, Mode.AVRO_TRUSTED):
            r.execute_command("AVRO.SCHEMA.ADD", NYC_SCHEMA_NAME, NYC_SCHEMA)
        start = time.perf_counter()
        latencies = pipeline_writes(r, json_payloads, avro_payloads, mode)
        total_time = time.perf_counter() - start
        p50, p99 = summarize(latencies)
        mem = r.info("memory")["used_memory_dataset"] / (1024 * 1024)
        throughput = NYC_COUNT / total_time
        results[mode] = {
            "time": total_time,
            "p50": p50,
            "p99": p99,
            "throughput": throughput,
            "mem": mem,
        }
        print(f"{mode.value}: {throughput:,.0f} docs/s | p50 {p50:.2f} ms | p99 {p99:.2f} ms | Mem {mem:.2f} MB")

    print("\n=== SUMMARY (NYC taxis ingestion) ===")
    for mode in (Mode.RAW, Mode.REDISJSON, Mode.AVRO_VALIDATED, Mode.AVRO_TRUSTED):
        data = results[mode]
        print(f"{mode.value:<15} thr {data['throughput']:>10,.0f} docs/s | p50 {data['p50']:.2f} ms | mem {data['mem']:.2f} MB")
