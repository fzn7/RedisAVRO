# --- Конфигурация ---
SHELL := /bin/bash
PYTHON ?= ./venv/bin/python
UNAME_S := $(shell uname -s)

# LIB_EXT for building Redis module
ifeq ($(UNAME_S),Darwin)
    LIB_EXT := dylib
else
    LIB_EXT := so
endif

LIB_PATH := ./target/release/libredis_avro.$(LIB_EXT)

# RedisJSON path can be overridden when needed (Docker uses redis-stack path)
REDISJSON_PATH ?= /opt/homebrew/lib/rejson.so

DOCKER_IMAGE ?= redis-avro-tests
DOCKER_REDISJSON_PATH ?= /opt/redis-stack/lib/rejson.so
DOCKER_RUN_USER ?= $(shell id -u):$(shell id -g)
DOCKER_RUN_BASE := docker run --rm \
	-u $(DOCKER_RUN_USER) \
	-v $(CURDIR):/workspace \
	-w /workspace \
	-e HOME=/workspace \
	-e AVRO_BENCH_COUNT \
	-e AVRO_BENCH_READ_COUNT \
	-e AVRO_BENCH_BIO_LEN \
	-e NYC_TAXI_COUNT \
	-e NYC_TAXI_BATCH

# --- Основные команды ---

.PHONY: all build test bench bench-taxi clean setup run help fmt lint kill docker-build docker-test docker-bench docker-shell

all: build test

# 1. Сборка (Release)
build:
	@echo "🦀 Building Rust module (Release)..."
	cargo build --release

# 2. Тесты
test: build
	@echo "🧪 Running Integration Tests..."
	cargo test --test integration_test

# 3. Бенчмарк
bench: build
	@echo "🚀 Running Benchmarks..."
	export REDISJSON_PATH=$(REDISJSON_PATH); \
	$(PYTHON) -m pytest -s -v benchmarks/test_benchmark.py

bench-taxi: build
	@echo "🚕 Running NYC taxi ingestion benchmark..."
	export REDISJSON_PATH=$(REDISJSON_PATH); \
	$(PYTHON) -m pytest -s -v benchmarks/nyc_taxi_benchmark.py

# 4. Настройка python
setup:
	@echo "📦 Installing Python dependencies..."
	$(PYTHON) -m pip install -r benchmarks/requirements.txt

# 5. Ручной запуск
run: build
	@echo "🏃 Starting Redis Server with Module..."
	redis-server --loadmodule $(LIB_PATH)

# 6. Только убийство процессов (Без перекомпиляции)
kill:
	@echo "🛑 Killing ALL lingering Redis servers..."
	@# Ищем процессы redis-server. Если находим - убиваем.
	@# $$2 в awk означает второй столбец (PID) в выводе ps aux
	@pids=$$(ps aux | grep '[r]edis-server' | awk '{print $$2}'); \
	if [ -n "$$pids" ]; then \
		echo "Found PIDs: $$pids"; \
		for pid in $$pids; do \
			kill -9 $$pid 2>/dev/null || true; \
		done; \
		echo "All killed."; \
	else \
		echo "No redis-server processes found."; \
	fi

# 7. Полная очистка (Файлы + Процессы)
clean: kill
	@echo "🧹 Cleaning up build artifacts..."
	cargo clean
	rm -rf benchmarks/__pycache__
	rm -rf benchmarks/.pytest_cache
	rm -rf tests/__pycache__

codex:
	codex --sandbox danger-full-access --ask-for-approval on-request

fmt:
	cargo fmt

lint:
	cargo clippy -- -D warnings

help:
	@echo "Commands:"
	@echo "  make build  - Build module"
	@echo "  make test   - Run integration tests"
	@echo "  make bench  - Run benchmarks"
	@echo "  make docker-test - Run integration tests inside Docker"
	@echo "  make docker-bench - Run benchmarks inside Docker"
	@echo "  make docker-shell - Drop into the Docker test image"
	@echo "  make kill   - Kill lingering Redis processes (NO REBUILD)"
	@echo "  make clean  - Kill processes AND remove build artifacts"

docker-build:
	@echo "🐳 Building Docker image for tests and benchmarks..."
	docker build -f docker/Dockerfile -t $(DOCKER_IMAGE) .

docker-test: docker-build
	@echo "🐳 Running integration tests inside Docker..."
	$(DOCKER_RUN_BASE) \
		-e REDISJSON_PATH=$(DOCKER_REDISJSON_PATH) \
		-e PYTHON=python3 \
		$(DOCKER_IMAGE) \
		-c "PYTHON=python3 make test"

docker-bench: docker-build
	@echo "🐳 Running benchmarks inside Docker..."
	$(DOCKER_RUN_BASE) \
		-e REDISJSON_PATH=$(DOCKER_REDISJSON_PATH) \
		-e PYTHON=python3 \
		$(DOCKER_IMAGE) \
		-c "PYTHON=python3 make bench"

docker-shell: docker-build
	@echo "🐳 Dropping into a shell with the test image..."
	$(DOCKER_RUN_BASE) \
		-e REDISJSON_PATH=$(DOCKER_REDISJSON_PATH) \
		-e PYTHON=python3 \
		-it $(DOCKER_IMAGE)
