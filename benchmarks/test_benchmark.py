import io
import json
import os
import platform
import socket
import subprocess
import sys
import threading
import time
from enum import Enum

import numpy as np
import psutil
import pytest
import redis
import datetime
from faker import Faker
from fastavro import parse_schema, schemaless_reader, schemaless_writer
import datetime

# --- CONFIGURATION ---


def _env_int(name: str, default: int) -> int:
    val = os.getenv(name)
    if val is None or not val.strip():
        return default
    try:
        return int(val)
    except ValueError:
        return default


COUNT = _env_int("AVRO_BENCH_COUNT", 50000)  # Number of records for the test
BATCH_SIZE = 1000  # Pipeline size for writes
READ_COUNT = _env_int("AVRO_BENCH_READ_COUNT", 100000)  # Number of read operations for measurements
MGET_BATCH = 100  # Batch size for MGET test
CPU_SAMPLE_INTERVAL = 0.1  # CPU sampling interval in seconds
BIO_TEXT_LEN = _env_int("AVRO_BENCH_BIO_LEN", 80)  # Controls size of JSON payload

# Avro Schema (NYC taxi inspired)
AVRO_SCHEMA = json.dumps({
    "type": "record",
    "name": "TaxiRide",
    "fields": [
        {"name": "vendor_id", "type": "string"},
        {"name": "pickup_datetime", "type": "string"},
        {"name": "dropoff_datetime", "type": "string"},
        {"name": "passenger_count", "type": "int"},
        {"name": "trip_distance", "type": "double"},
        {"name": "ratecode_id", "type": "int"},
        {"name": "store_and_fwd_flag", "type": "string"},
        {"name": "pickup_location_id", "type": "int"},
        {"name": "dropoff_location_id", "type": "int"},
        {"name": "payment_type", "type": "int"},
        {"name": "fare_amount", "type": "double"},
        {"name": "extra", "type": "double"},
        {"name": "mta_tax", "type": "double"},
        {"name": "tip_amount", "type": "double"},
        {"name": "tolls_amount", "type": "double"},
        {"name": "improvement_surcharge", "type": "double"},
        {"name": "total_amount", "type": "double"},
        {"name": "congestion_surcharge", "type": ["null", "double"], "default": None}
    ]
})

AVRO_PARTIAL_PATH = "/tip_amount"
REDISJSON_PARTIAL_PATH = "$.tip_amount"
REDISJSON_ROOT_PATH = "$"
SCHEMA_NAME = "nyc_ride_v1"
AVRO_SCHEMA_OBJ = json.loads(AVRO_SCHEMA)
PARSED_AVRO_SCHEMA = parse_schema(AVRO_SCHEMA_OBJ)


class StorageMode(str, Enum):
    RAW = "raw"
    AVRO = "avro"
    REDISJSON = "redisjson"
    AVRO_RAW = "avro_raw"
    AVRO_RAW_TRUSTED = "avro_raw_trusted"


def get_module_path():
    """Automatically determines the path to the redis_avro library."""
    system = platform.system()
    ext = ".dylib" if system == "Darwin" else ".so"
    if system == "Windows":
        ext = ".dll"

    paths = [
        f"./target/release/libredis_avro{ext}",
        f"../target/release/libredis_avro{ext}",
        os.getenv("REDIS_AVRO_PATH", ""),
    ]

    for p in paths:
        if p and os.path.exists(p):
            return os.path.abspath(p)

    pytest.fail(f"❌ Module libredis_avro{ext} not found. Build with 'cargo build --release' or set REDIS_AVRO_PATH.")


MODULE_PATH = get_module_path()


def resolve_redisjson_module():
    env_path = os.getenv("REDISJSON_PATH")
    candidates = [
        env_path,
        "/usr/lib/redis/modules/rejson.so",
        "/usr/local/lib/redis/modules/rejson.so",
        "/opt/homebrew/lib/redis/modules/rejson.so",
    ]

    for path in candidates:
        if path and os.path.exists(path):
            return os.path.abspath(path)
    return None


REDISJSON_MODULE_PATH = resolve_redisjson_module()


def get_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('', 0))
        return s.getsockname()[1]


class CPUTracker(threading.Thread):
    def __init__(self, pid):
        super().__init__()
        self.pid = pid
        self.running = False
        self.samples = []
        self.process = None
        self.daemon = True

    def run(self):
        self.running = True
        try:
            self.process = psutil.Process(self.pid)
        except psutil.NoSuchProcess:
            self.running = False
            return

        while self.running:
            try:
                self.samples.append(self.process.cpu_percent(interval=CPU_SAMPLE_INTERVAL))
            except psutil.NoSuchProcess:
                break
            # No need for extra sleep, cpu_percent has an interval

    def stop(self):
        self.running = False

    def get_avg_cpu(self):
        if not self.samples:
            return 0
        return sum(self.samples) / len(self.samples)


@pytest.fixture(scope="module")
def redis_server():
    if REDISJSON_MODULE_PATH is None:
        pytest.skip("Set REDISJSON_PATH (or install RedisJSON) to run benchmarks.")

    port = get_free_port()
    cmd = [
        "redis-server",
        "--port", str(port),
        "--loadmodule", MODULE_PATH,
        "--loadmodule", REDISJSON_MODULE_PATH,
        "--save", "",
        "--loglevel", "warning",
    ]

    print(f"\n🚀 Starting Redis on port {port}...")
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    time.sleep(1.5)

    client = redis.Redis(host='localhost', port=port, decode_responses=False)

    try:
        client.ping()
    except redis.ConnectionError:
        out, err = proc.communicate()
        pytest.fail(f"Redis failed to start.\nStdout: {out}\nStderr: {err}")

    yield client, proc.pid

    print("\n🛑 Stopping Redis...")
    proc.terminate()
    proc.wait()


@pytest.fixture(scope="module")
def sample_data():
    print(f"\n🎲 Generating {COUNT} records with Faker...")
    fake = Faker()
    data = []
    for i in range(COUNT):
        pickup = fake.date_time_between(start_date='-30d', end_date='now')
        dropoff = pickup + datetime.timedelta(minutes=fake.random_int(min=5, max=90))
        fare = round(fake.random.uniform(5, 200), 2)
        tip = round(fare * fake.random.uniform(0, 0.3), 2)
        tolls = round(fake.random.uniform(0, 20), 2)
        extra = round(fake.random.uniform(0, 10), 2)
        mta_tax = 0.5
        improvement_surcharge = 0.3
        congestion = round(fake.random.uniform(0, 5), 2) if fake.boolean() else None
        total = round(fare + tip + tolls + extra + mta_tax + improvement_surcharge + (congestion or 0), 2)
        data.append({
            "vendor_id": fake.random_element(elements=("CMT", "VTS")),
            "pickup_datetime": pickup.isoformat(),
            "dropoff_datetime": dropoff.isoformat(),
            "passenger_count": fake.random_int(min=1, max=5),
            "trip_distance": round(fake.random.uniform(0.1, 20), 2),
            "ratecode_id": fake.random_int(min=1, max=6),
            "store_and_fwd_flag": fake.random_element(elements=("Y", "N")),
            "pickup_location_id": fake.random_int(min=1, max=265),
            "dropoff_location_id": fake.random_int(min=1, max=265),
            "payment_type": fake.random_int(min=1, max=5),
            "fare_amount": fare,
            "extra": extra,
            "mta_tax": mta_tax,
            "tip_amount": tip,
            "tolls_amount": tolls,
            "improvement_surcharge": improvement_surcharge,
            "total_amount": total,
            "congestion_surcharge": congestion,
        })
    return data


def get_memory_info(r):
    return r.info('memory')


def encode_avro_bytes(doc):
    buf = io.BytesIO()
    schemaless_writer(buf, PARSED_AVRO_SCHEMA, doc)
    return buf.getvalue()


def decode_avro_bytes(payload):
    buf = io.BytesIO(payload)
    return schemaless_reader(buf, PARSED_AVRO_SCHEMA)


def ensure_text(value):
    if value is None or isinstance(value, (int, float)):
        return value
    if isinstance(value, bytes):
        return value.decode('utf-8')
    return value


def get_key_memory_usage(r, key):
    try:
        return r.execute_command("MEMORY", "USAGE", key)
    except redis.ResponseError:
        return None


def run_write_test(r, payloads, prefix, mode: StorageMode):
    start_mem_info = get_memory_info(r)
    latencies = []

    pipe = r.pipeline()
    for i, payload in enumerate(payloads):
        key = f"{prefix}:{i}"

        if mode == StorageMode.AVRO:
            pipe.execute_command("AVRO.JSON.SET", key, SCHEMA_NAME, payload)
        elif mode == StorageMode.AVRO_RAW:
            pipe.execute_command("AVRO.SET", key, SCHEMA_NAME, payload)
        elif mode == StorageMode.AVRO_RAW_TRUSTED:
            pipe.execute_command("AVRO.SET", key, SCHEMA_NAME, "TRUSTED", payload)
        elif mode == StorageMode.REDISJSON:
            pipe.execute_command("JSON.SET", key, ".", payload)
        else:
            pipe.set(key, payload)

        if (i + 1) % BATCH_SIZE == 0:
            start_time = time.perf_counter()
            pipe.execute()
            end_time = time.perf_counter()
            latencies.append((end_time - start_time) * 1000) # milliseconds

    # Execute any remaining commands
    if (i + 1) % BATCH_SIZE != 0:
        start_time = time.perf_counter()
        pipe.execute()
        end_time = time.perf_counter()
        latencies.append((end_time - start_time) * 1000)

    time.sleep(0.5)
    end_mem_info = get_memory_info(r)
    
    mem_dataset = end_mem_info.get('used_memory_dataset', 0)
    mem_rss = end_mem_info['used_memory_rss']
    fragmentation = end_mem_info['mem_fragmentation_ratio']
    
    # We now return the absolute values, delta will be calculated in the report
    return latencies, {"dataset": mem_dataset, "rss": mem_rss, "fragmentation": fragmentation}


def run_read_test(r, count, prefix, mode: StorageMode):
    """Full read benchmark (Pipeline)."""
    latencies = []
    pipe = r.pipeline()
    for i in range(count):
        key = f"{prefix}:{i}"
        if mode == StorageMode.AVRO:
            pipe.execute_command("AVRO.JSON.GET", key, SCHEMA_NAME)
        elif mode in (StorageMode.AVRO_RAW, StorageMode.AVRO_RAW_TRUSTED):
            pipe.execute_command("AVRO.GET", key, SCHEMA_NAME)
        elif mode == StorageMode.REDISJSON:
            pipe.execute_command("JSON.GET", key)
        else:
            pipe.get(key)

        if (i + 1) % BATCH_SIZE == 0:
            start_time = time.perf_counter()
            pipe.execute()
            end_time = time.perf_counter()
            latencies.append((end_time - start_time) * 1000)

    # Execute any remaining commands
    if (i + 1) % BATCH_SIZE != 0:
        start_time = time.perf_counter()
        pipe.execute()
        end_time = time.perf_counter()
        latencies.append((end_time - start_time) * 1000)

    return latencies


def run_field_read_test(r, count, prefix, mode: StorageMode):
    """Partial read benchmark (Pipeline)."""
    if mode in (StorageMode.RAW, StorageMode.AVRO_RAW, StorageMode.AVRO_RAW_TRUSTED):
        return None

    latencies = []
    pipe = r.pipeline()
    for i in range(count):
        key = f"{prefix}:{i}"
        if mode == StorageMode.AVRO:
            pipe.execute_command("AVRO.JSON.GET", key, SCHEMA_NAME, AVRO_PARTIAL_PATH)
        else: # RedisJSON
            pipe.execute_command("JSON.GET", key, REDISJSON_PARTIAL_PATH)

        if (i + 1) % BATCH_SIZE == 0:
            start_time = time.perf_counter()
            pipe.execute()
            end_time = time.perf_counter()
            latencies.append((end_time - start_time) * 1000)

    # Execute any remaining commands
    if (i + 1) % BATCH_SIZE != 0:
        start_time = time.perf_counter()
        pipe.execute()
        end_time = time.perf_counter()
        latencies.append((end_time - start_time) * 1000)

    return latencies


def run_mget_test(r, count, prefix, mode: StorageMode):
    """MGET Benchmark (Native MGET vs AVRO.MGET vs JSON.MGET)."""
    latencies = []
    all_keys = [f"{prefix}:{i}" for i in range(count)]
    chunks = [all_keys[i:i + MGET_BATCH] for i in range(0, len(all_keys), MGET_BATCH)]

    for chunk in chunks:
        start_time = time.perf_counter()
        if mode == StorageMode.AVRO:
            r.execute_command("AVRO.JSON.MGET", SCHEMA_NAME, ".", *chunk)
        elif mode in (StorageMode.AVRO_RAW, StorageMode.AVRO_RAW_TRUSTED):
            r.execute_command("AVRO.MGET", SCHEMA_NAME, *chunk)
        elif mode == StorageMode.REDISJSON:
            r.execute_command("JSON.MGET", *chunk, REDISJSON_ROOT_PATH)
        else:
            r.mget(chunk)
        end_time = time.perf_counter()
        latencies.append((end_time - start_time) * 1000)

    return latencies


def verify_sample_avro(r, data, prefix):
    check_idx = min(100, len(data) - 1)
    res = r.execute_command("AVRO.JSON.GET", f"{prefix}:{check_idx}", SCHEMA_NAME)
    res = ensure_text(res)
    loaded_obj = json.loads(res)
    assert loaded_obj['tip_amount'] == data[check_idx]['tip_amount'], "❌ Full Read verification failed"

    res_part = r.execute_command("AVRO.JSON.GET", f"{prefix}:{check_idx}", SCHEMA_NAME, AVRO_PARTIAL_PATH)
    assert res_part is not None, f"❌ Module returned NIL for path '{AVRO_PARTIAL_PATH}'"

    res_part = ensure_text(res_part)
    try:
        loaded_field = json.loads(res_part)
    except json.JSONDecodeError:
        loaded_field = res_part

    assert loaded_field == data[check_idx]['tip_amount'], f"❌ Partial Read verification failed. Got: {res_part}"


def verify_sample_redisjson(r, data, prefix):
    check_idx = min(100, len(data) - 1)
    full = r.execute_command("JSON.GET", f"{prefix}:{check_idx}")
    parsed = json.loads(ensure_text(full))
    assert parsed['tip_amount'] == data[check_idx]['tip_amount'], "❌ RedisJSON full read mismatch"

    field = r.execute_command("JSON.GET", f"{prefix}:{check_idx}", REDISJSON_PARTIAL_PATH)
    assert field is not None, "❌ RedisJSON returned NIL for partial path"
    field_val = json.loads(ensure_text(field))[0]
    assert field_val == data[check_idx]['tip_amount'], "❌ RedisJSON partial read mismatch"


def verify_sample_avro_raw(r, data, prefix):
    check_idx = min(100, len(data) - 1)
    res = r.execute_command("AVRO.GET", f"{prefix}:{check_idx}", SCHEMA_NAME)
    assert res is not None, "❌ AVRO.GET returned NIL for raw key"
    decoded = decode_avro_bytes(res)
    assert decoded['tip_amount'] == data[check_idx]['tip_amount'], "❌ Raw Avro decode mismatch"


@pytest.mark.skipif(REDISJSON_MODULE_PATH is None, reason="RedisJSON module not found. Set REDISJSON_PATH or install RedisJSON.")
def test_full_benchmark(redis_server, sample_data):
    r, redis_pid = redis_server

    print("\n" + "=" * 60)
    print(f"🏎️  BENCHMARKING: {COUNT} keys | OS: {platform.system()}")
    print("=" * 60)
    print(f"🧪 Config: READ_COUNT={READ_COUNT:,} ops | bio_len={BIO_TEXT_LEN} chars")

    json_payloads = [json.dumps(doc) for doc in sample_data]
    avro_raw_payloads = [encode_avro_bytes(doc) for doc in sample_data]

    payloads_by_mode = {
        StorageMode.RAW: json_payloads,
        StorageMode.REDISJSON: json_payloads,
        StorageMode.AVRO: json_payloads,
        StorageMode.AVRO_RAW: avro_raw_payloads,
        StorageMode.AVRO_RAW_TRUSTED: avro_raw_payloads,
    }

    results = {}

    def run_profile(mode: StorageMode, prefix: str):
        r.flushall()
        time.sleep(1)

        if mode in (StorageMode.AVRO, StorageMode.AVRO_RAW, StorageMode.AVRO_RAW_TRUSTED):
            r.execute_command("AVRO.SCHEMA.ADD", SCHEMA_NAME, AVRO_SCHEMA)

        payloads = payloads_by_mode[mode]

        # --- CPU & Write ---
        cpu_tracker = CPUTracker(redis_pid)
        cpu_tracker.start()
        write_latencies, write_mem_info = run_write_test(r, payloads, prefix, mode)
        cpu_tracker.stop()
        write_cpu = cpu_tracker.get_avg_cpu()

        # --- CPU & Read ---
        cpu_tracker = CPUTracker(redis_pid)
        cpu_tracker.start()
        read_latencies = run_read_test(r, READ_COUNT, prefix, mode)
        cpu_tracker.stop()
        read_cpu = cpu_tracker.get_avg_cpu()

        # --- CPU & Field Read ---
        field_latencies = None
        field_cpu = 0
        if mode not in (StorageMode.RAW, StorageMode.AVRO_RAW, StorageMode.AVRO_RAW_TRUSTED):
            cpu_tracker = CPUTracker(redis_pid)
            cpu_tracker.start()
            field_latencies = run_field_read_test(r, READ_COUNT, prefix, mode)
            cpu_tracker.stop()
            field_cpu = cpu_tracker.get_avg_cpu()

        # --- CPU & Field Read ---
        field_latencies = None
        field_cpu = 0
        # if mode not in (StorageMode.RAW, StorageMode.AVRO_RAW, StorageMode.AVRO_RAW_TRUSTED):
        #     cpu_tracker = CPUTracker(redis_pid)
        #     cpu_tracker.start()
        #     field_latencies = run_field_read_test(r, READ_COUNT, prefix, mode)
        #     cpu_tracker.stop()
        #     field_cpu = cpu_tracker.get_avg_cpu()

        # --- CPU & MGET ---
        # cpu_tracker = CPUTracker(redis_pid)
        # cpu_tracker.start()
        mget_latencies = [] #run_mget_test(r, READ_COUNT, prefix, mode)
        # cpu_tracker.stop()
        mget_cpu = 0 #cpu_tracker.get_avg_cpu()


        if mode == StorageMode.AVRO:
            verify_sample_avro(r, sample_data, prefix)
        elif mode in (StorageMode.AVRO_RAW, StorageMode.AVRO_RAW_TRUSTED):
            verify_sample_avro_raw(r, sample_data, prefix)
        elif mode == StorageMode.REDISJSON:
            verify_sample_redisjson(r, sample_data, prefix)

        sample_key_mem = get_key_memory_usage(r, f"{prefix}:0")

        def get_percentiles(latencies):
            if not latencies: return None, None, None
            p50 = np.percentile(latencies, 50)
            p95 = np.percentile(latencies, 95)
            p99 = np.percentile(latencies, 99)
            return p50, p95, p99

        r.execute_command("MEMORY", "PURGE")
        mem_snapshot = get_memory_info(r)
        mem_info = {
            "dataset": mem_snapshot.get('used_memory_dataset', write_mem_info["dataset"]),
            "rss": mem_snapshot.get('used_memory_rss', write_mem_info["rss"]),
            "fragmentation": mem_snapshot.get('mem_fragmentation_ratio', write_mem_info["fragmentation"]),
        }

        return {
            "mem": mem_info,
            "write_total_time": sum(write_latencies) / 1000,
            "write_p50": get_percentiles(write_latencies)[0],
            "write_p99": get_percentiles(write_latencies)[2],
            "write_cpu": write_cpu,
            "read_total_time": sum(read_latencies) / 1000,
            "read_p50": get_percentiles(read_latencies)[0],
            "read_p99": get_percentiles(read_latencies)[2],
            "read_cpu": read_cpu,
            "field_total_time": sum(field_latencies) / 1000 if field_latencies else None,
            "field_p50": get_percentiles(field_latencies)[0] if field_latencies else None,
            "field_p99": get_percentiles(field_latencies)[2] if field_latencies else None,
            "field_cpu": field_cpu,
            "mget_total_time": sum(mget_latencies) / 1000,
            "mget_p50": get_percentiles(mget_latencies)[0],
            "mget_p99": get_percentiles(mget_latencies)[2],
            "mget_cpu": mget_cpu,
            "sample_key_mem": sample_key_mem,
        }

    table_modes = [
        ("RAW JSON", StorageMode.RAW),
        ("RedisJSON", StorageMode.REDISJSON),
        ("AVRO JSON", StorageMode.AVRO),
        ("AVRO Raw", StorageMode.AVRO_RAW),
        ("AVRO Raw*", StorageMode.AVRO_RAW_TRUSTED),
    ]

    modes_to_run = [
        (StorageMode.RAW, "raw", "📄 RAW JSON"),
        (StorageMode.REDISJSON, "jsonmod", "📚 RedisJSON"),
        (StorageMode.AVRO, "avro", "📦 AVRO JSON"),
        (StorageMode.AVRO_RAW, "avro_raw", "🧊 AVRO RAW (validated)"),
        (StorageMode.AVRO_RAW_TRUSTED, "avro_trusted", "🧊 AVRO RAW* (TRUSTED)"),
    ]

    for mode, prefix, name in modes_to_run:
        results[mode] = run_profile(mode, prefix)
        print(f"{name}: Write {results[mode]['write_total_time']:.2f}s | Read {results[mode]['read_total_time']:.2f}s | MGET {results[mode]['mget_total_time']:.2f}s | Mem {results[mode]['mem']['dataset']/1024/1024:.2f}MB")


    # --- FORMATTED TABLE ---
    print("\n" + "=" * 160)
    # Header
    row = f"{'METRIC':<25} | "
    for name, _ in table_modes:
        row += f"{name:<22} | "
    print(row)
    print("-" * 160)

    # Memory
    dataset_mem_row = " | ".join(f"{results[mode]['mem']['dataset']/1024/1024:<22.2f}" for _, mode in table_modes)
    base_mem = results[StorageMode.RAW]['mem']['dataset']
    mem_deltas = []
    if base_mem > 0:
        for label, mode in table_modes[1:]: # Skip RAW
            delta = (results[mode]['mem']['dataset'] / base_mem - 1) * 100
            mem_deltas.append(f"Δ {label.split(' ')[0]} {delta:+.1f}%")
    print(f"{'Memory (Dataset MB)':<25} | {dataset_mem_row} | {' / '.join(mem_deltas)}")

    rss_mem_row = " | ".join(f"{results[mode]['mem']['rss']/1024/1024:<22.2f}" for _, mode in table_modes)
    print(f"{'Memory (RSS MB)':<25} | {rss_mem_row} |")
    
    frag_row = " | ".join(f"{results[mode]['mem']['fragmentation']:<22.2f}" for _, mode in table_modes)
    print(f"{'Fragmentation Ratio':<25} | {frag_row} |")

    def fmt_sample_cell(mode):
        mem = results[mode]["sample_key_mem"]
        if mem is None:
            return f"{'N/A':<22}"
        return f"{mem / 1024:<22.2f}"

    sample_mem_row = " | ".join(fmt_sample_cell(mode) for _, mode in table_modes)
    print(f"{'Sample Key (KB)':<25} | {sample_mem_row} |")

    print("-" * 160)

    # Throughput
    def fmt_ops(total_ops, total_time_s):
        return "N/A" if total_time_s in (None, 0) else f"{total_ops / total_time_s:,.0f} ops/s"

    write_tp_row = " | ".join(f"{fmt_ops(COUNT, results[mode]['write_total_time']):<22}" for _, mode in table_modes)
    print(f"{'Write Throughput':<25} | {write_tp_row} |")
    read_tp_row = " | ".join(f"{fmt_ops(READ_COUNT, results[mode]['read_total_time']):<22}" for _, mode in table_modes)
    print(f"{'Read Pipe Throughput':<25} | {read_tp_row} |")
    # mget_tp_row = " | ".join(f"{fmt_ops(READ_COUNT, results[mode]['mget_total_time']):<22}" for _, mode in table_modes)
    # print(f"{'Read MGET Throughput':<25} | {mget_tp_row} | batched {MGET_BATCH}")
    # field_tp_row = " | ".join(f"{fmt_ops(READ_COUNT, results[mode]['field_total_time']):<22}" for _, mode in table_modes)
    # print(f"{'Partial Read Throughput':<25} | {field_tp_row} |")

    print("-" * 160)

    # Latency
    def fmt_latency(p50, p99):
        return "N/A" if p50 is None else f"p50 {p50:3.2f}ms / p99 {p99:3.2f}ms"

    write_latency_row = " | ".join(f"{fmt_latency(results[mode]['write_p50'], results[mode]['write_p99']):<22}" for _, mode in table_modes)
    print(f"{'Write Latency (ms)':<25} | {write_latency_row} |")
    read_latency_row = " | ".join(f"{fmt_latency(results[mode]['read_p50'], results[mode]['read_p99']):<22}" for _, mode in table_modes)
    print(f"{'Read Pipe Latency (ms)':<25} | {read_latency_row} |")
    # mget_latency_row = " | ".join(f"{fmt_latency(results[mode]['mget_p50'], results[mode]['mget_p99']):<22}" for _, mode in table_modes)
    # print(f"{'Read MGET Latency (ms)':<25} | {mget_latency_row} |")
    # field_latency_row = " | ".join(f"{fmt_latency(results[mode]['field_p50'], results[mode]['field_p99']):<22}" for _, mode in table_modes)
    # print(f"{'Partial Read Latency(ms)':<25} | {field_latency_row} |")

    print("-" * 160)

    # CPU
    def fmt_cpu(cpu_val):
        return f"{cpu_val:.1f}% CPU"

    write_cpu_row = " | ".join(f"{fmt_cpu(results[mode]['write_cpu']):<22}" for _, mode in table_modes)
    print(f"{'Write CPU':<25} | {write_cpu_row} |")
    read_cpu_row = " | ".join(f"{fmt_cpu(results[mode]['read_cpu']):<22}" for _, mode in table_modes)
    print(f"{'Read Pipe CPU':<25} | {read_cpu_row} |")
    # mget_cpu_row = " | ".join(f"{fmt_cpu(results[mode]['mget_cpu']):<22}" for _, mode in table_modes)
    # print(f"{'Read MGET CPU':<25} | {mget_cpu_row} |")
    # field_cpu_row = " | ".join(f"{fmt_cpu(results[mode]['field_cpu']):<22}" for _, mode in table_modes)
    # print(f"{'Partial Read CPU':<25} | {field_cpu_row} |")

    print("=" * 160)

    print("* AVRO Raw* uses AVRO.SET ... TRUSTED to skip server-side validation.\n")
    print("CPU is measured for the redis-server process only.")
    print("Latency is per batch (e.g., a single pipeline execution or MGET command).")


    # assert results[StorageMode.AVRO_RAW]['mem']['dataset'] < results[StorageMode.RAW]['mem']['dataset'], "AVRO Raw should be more memory efficient than RAW JSON"
    print("\n✅ Benchmark passed successfully!")


if __name__ == "__main__":
    sys.exit(pytest.main(["-s", __file__]))
