FROM redis/redis-stack-server:latest

ARG RUST_VERSION=stable

ENV DEBIAN_FRONTEND=noninteractive \
    RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH \
    PYTHONUNBUFFERED=1

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        clang \
        curl \
        git \
        libclang-dev \
        make \
        pkg-config \
        python3 \
        python3-pip \
        python3-venv && \
    rm -rf /var/lib/apt/lists/*

RUN curl https://sh.rustup.rs -sSf | sh -s -- -y --profile minimal --default-toolchain ${RUST_VERSION} && \
    chmod -R a+rwX ${RUSTUP_HOME} ${CARGO_HOME}

RUN pip3 install --upgrade pip

WORKDIR /workspace

COPY benchmarks/requirements.txt /tmp/benchmark-requirements.txt
RUN pip3 install -r /tmp/benchmark-requirements.txt && rm /tmp/benchmark-requirements.txt

ENTRYPOINT ["/bin/bash"]
