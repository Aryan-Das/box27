# Cloud File Service

A high-performance HTTP file service built from scratch in C++20. Raw sockets, an epoll event loop, a custom thread pool, an LRU cache, zero-copy file transfer via `sendfile()`, and metadata stored in PostgreSQL.

## Features

- **Custom HTTP/1.1 server**: hand-written request parsing (request line + headers)
- **epoll-based non-blocking event loop**: single-threaded reactor handling thousands of concurrent connections
- **Thread pool**: fixed worker pool with a condition-variable-based job queue, decoupling connection acceptance from request processing
- **Streaming request body support**: correctly accumulates request bodies across multiple `recv()` calls 
- **In-memory LRU cache**: O(1) get/put/evict via `std::list` + `std::unordered_map`, with a size threshold so large files bypass the cache entirely and go straight to disk
- **Zero-copy large file serving** via `sendfile()`, with bounded-retry backpressure handling for non-blocking sockets
- **File upload**: `POST /upload/<filename>`, with SHA-256 checksumming (OpenSSL) and path-traversal protection
- **PostgreSQL metadata store** (via `libpqxx`): tracks filename, size, SHA-256, and upload timestamp per file, with upsert-on-reupload and atomic rollback (uploaded file is deleted from disk if the metadata insert fails)
- **Dockerized dev environment**: Linux devcontainer (for `epoll`/`sendfile` access from macOS) with Postgres as a sibling service via Docker Compose
- **CI** via GitHub Actions 

## Architecture

```
Client
  │
  ▼
epoll event loop (single thread, level-triggered)
  │
  ├─ listening socket ready → accept(), set O_NONBLOCK, register with epoll
  │
  └─ client fd ready → in-flight check (mutex-guarded) → dispatch to thread pool
                              │
                              ▼
                    Thread pool (fixed workers, condition_variable queue)
                              │
                              ▼
              handleClient(fd): accumulate request → route by method
                    │                           │
                    ▼                           ▼
                GET /path                  POST /upload/<filename>
                    │                           │
          ┌─────────┴─────────┐        validate filename, no traversal
          ▼                   ▼                 │
     LRU cache hit      Cache miss          write to disk
     (serve from        (open + fstat            │
      memory)            + sendfile,         SHA-256 hash
                          conditionally           │
                          populate cache)    INSERT/UPSERT into Postgres
                                                   │
                                          rollback file on DB failure
```

## Performance

Currently in the process of benchmarking with a more realistic/diverse workload of files.

All benchmarks below via `wrk -t12 -c400 -d30s` on an 8-core devcontainer.

| Stage | Result |
|---|---|
| Blocking, single-threaded baseline | 14,028 req/sec |
| epoll + thread pool (small file) | ~9,500 req/sec throughput, but with much better tail latency; thread count sweep confirmed optimal pool size tracks CPU core count |
| Isolating disk I/O as the bottleneck (no-I/O diagnostic) | 526,000+ req/sec: confirmed disk I/O, not threading, was the dominant cost |
| LRU cache (warm, repeated small-file requests) | 595,000+ req/sec, ~823µs avg latency: **~20x throughput / ~16x latency** improvement over uncached disk reads |
| `sendfile()` on a 10MB file vs. `read()`+`write()` | 1,235 req/sec vs. 22 req/sec: **~56x throughput improvement**, 12+ GB/sec sustained transfer under heavy concurrent load |



## Running locally

Requires Docker Desktop and VS Code with the Dev Containers extension.

1. Clone the repo and open it in VS Code
2. **Reopen in Container** when prompted (builds the app container + a Postgres 16 image via Docker Compose)
3. Inside the container:
   ```bash
   cmake -B build
   cmake --build build
   build/fileservice_epoll
   ```
4. From another terminal (or your host machine, with the port mapped):
   ```bash
   curl http://localhost:8080/CMakeLists.txt
   curl -X POST --data-binary @somefile.txt http://localhost:8080/upload/somefile.txt
   ```



## Known limitations / Future Development
- Code refactor
- No HTTPS/TLS 
- No HTTP keep-alive / connection reuse (every response closes the connection)
- Auth, rate limiting not yet implemented
- Non-blocking write backpressure (`EAGAIN` on `sendfile()`/`write()`) is handled via bounded synchronous retry instead of full `EPOLLOUT` async writes
- LRU cache eviction is entry-count-bounded, not byte-bounded

## Tech stack

C++20 · Linux epoll · POSIX sockets · PostgreSQL (via libpqxx) · OpenSSL (SHA-256) · Docker & Docker Compose · GitHub Actions · CMake
