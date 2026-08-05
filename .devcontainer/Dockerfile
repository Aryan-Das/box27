FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    gdb \
    git \
    curl \
    wget \
    pkg-config \
    libpq-dev \
    libpqxx-dev \
    postgresql-client \
    libssl-dev \
    net-tools \
    lsof \
    strace \
    unzip \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

# wrk isn't in apt, build it from source (it's a tiny repo)
RUN git clone https://github.com/wg/wrk.git /tmp/wrk \
    && cd /tmp/wrk && make \
    && cp wrk /usr/local/bin/ \
    && rm -rf /tmp/wrk

WORKDIR /workspace
