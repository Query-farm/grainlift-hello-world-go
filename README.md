# grainlift-hello-world-go

A complete ADBC service in about 300 lines of Go, built with
the [Grainlift Go SDK](https://github.com/Query-farm/grainlift-go). Any ADBC
application connects to it through the native Grainlift driver; the service
itself needs no database, SQL engine or downstream driver.

## Quickstart

Requires Go 1.26+ and Rust 1.97+ (to build the native Grainlift ADBC driver
once). The SQL example below also uses [uv](https://docs.astral.sh/uv/).

    git clone https://github.com/Query-farm/grainlift.git ../grainlift
    (cd ../grainlift && cargo build --locked -p adbc-driver-grainlift)

Start the service:

    go run .

Or install the command with
`go install github.com/Query-farm/grainlift-hello-world-go@latest` and run
`grainlift-hello-world-go`.

No credentials are needed. The service is read-only, so it accepts anonymous
clients (see [Authentication](#authentication)).

### Query it from SQL

[Haybarn](https://github.com/Query-farm-haybarn/haybarn), Query.Farm's DuckDB
distribution, loads the Grainlift driver through the `adbc_scanner` extension.
In a second terminal, run [`examples/query.sql`](examples/query.sql):

    export GRAINLIFT_DRIVER=$PWD/../grainlift/target/debug/libadbc_driver_grainlift.dylib  # .so on Linux
    uvx haybarn-cli < examples/query.sql

The same script runs unchanged in the DuckDB CLI. It prints:

    ┌───────────────┐
    │    message    │
    │    varchar    │
    ├───────────────┤
    │ Hello, world! │
    └───────────────┘
    ┌─────────┬────────────┐
    │ numbers │   total    │
    │  int64  │   int128   │
    ├─────────┼────────────┤
    │  100000 │ 4999950000 │
    └─────────┴────────────┘
    ...

`adbc_scan` sends its quoted SQL to this service. The rows come back as an
ordinary relation that you can join, aggregate or export locally.

### Query it from Go

[`examples/client`](examples/client/main.go) uses the Apache Arrow ADBC
[driver manager](https://pkg.go.dev/github.com/apache/arrow-adbc/go/adbc/drivermgr)
to load the native driver (it needs cgo and a C++ compiler):

    go run ./examples/client

It prints:

    message: Hello, world!
    numbers(2500): [1024 1024 452] rows per Arrow batch
    running_total(2500): last row {number: 2499, total: 3123750}
    Empty result: 0 batches, schema: ...

Any other ADBC driver manager works the same way, for example Python's
`adbc_driver_manager` with `entrypoint="AdbcDriverGrainliftInit"` and the
`grainlift.uri` and `grainlift.target` options.

## What's in the module

| Package | Contents |
| --- | --- |
| [`hello`](hello/hello.go) | The service: `Backend` → `Connection` → `Statement`, plus the two result styles below |
| [`main`](main.go) | The `grainlift-hello-world-go` command |
| [`examples/client`](examples/client/main.go) | A Go ADBC client |
| [`cmd/conformance-worker`](cmd/conformance-worker/main.go) | The Go worker for Grainlift's [shared conformance suite](https://github.com/Query-farm/grainlift/tree/main/validation/conformance) |

The service answers three queries:

| Query | Result | Demonstrates |
| --- | --- | --- |
| `SELECT 'Hello, world!' AS message` | one row | the smallest possible result |
| `SELECT * FROM numbers(n)` | 0..n-1 | a plain **`array.RecordReader`** |
| `SELECT * FROM running_total(n)` | 0..n-1 with a running sum | a serializable **`grainlift.ResultProducer`** |

`n` ranges from 0 to 100000. Anything else is an ADBC `INVALID_ARGUMENT` error
with SQLSTATE 42000. The example matches these queries exactly rather than
pretending to parse SQL.

`Statement` implements the ADBC statement lifecycle: set the SQL, then
`Prepare`, `ExecuteSchema` and `Execute`. Preparation matters because clients
such as `adbc_scanner` prepare every query before running it.

### Readers vs. producers

Both styles stream lazily in batches of at most 1024 rows, and you can mix them
freely within one service.

- **Reader** (`numbers`): return `&grainlift.QueryResult{Reader: reader}`.
  It's the simplest option, and it can hold resources such as an open database
  cursor. The reader lives in server memory until the client finishes or
  releases the result.
- **Producer** (`running_total`): implement `grainlift.ResultProducer` on a
  struct whose exported fields are the entire resumable state, register it with
  `grainlift.RegisterResultProducer` in an `init` function, and return
  `grainlift.NewProducerResult(schema, state)`. Over HTTP the state is
  gob-encoded into the encrypted continuation token after each batch. The
  server keeps no reader or replay batch between fetches, and a retried fetch
  recomputes its batch from the token. This is the same approach VGI-RPC
  streams use.

Pick a producer when the state is small and serializable, such as offsets,
keyset cursors or counters. Pick a reader when it isn't.

## Authentication

Anonymous access is opt-in in the Grainlift SDK. This example enables it because
it only serves public, read-only data: its `main` calls
`cli.Run(..., cli.Options{Auth: "anonymous"})`. Requests without credentials act
as the shared `anonymous` principal.

- Set `GRAINLIFT_TOKEN` on both sides to connect as an authenticated principal
  instead. A client that sends a wrong token is rejected, never downgraded to
  anonymous.
- Run `go run . --auth token` to require a token. The server prints a generated
  token when `GRAINLIFT_TOKEN` is unset.

For a service that can write data or expose private data, keep the default
token authentication. In your own hosting code, anonymous access is
`grainlift.HTTPAuthenticator(tokens, "anonymous")`, passed to
`Service.HTTPHandler`; pass an empty principal to require a token.

## Hosting options

`go run . -help` lists them. Any worker can use the same development host by
calling [`cli.Run`](https://pkg.go.dev/github.com/Query-farm/grainlift-go/cli)
from its `main`.

- `--host http` (default): loopback HTTP for development. It drains on
  SIGINT/SIGTERM.
- `--host mtls`: verified TCP/mTLS; client certificates identify callers.
- `--port`: listening port (default 8080). Point the client at a different
  port with `GRAINLIFT_ENDPOINT`.

For mTLS, supply the server chain, key, client CA and authorized client URI SAN:

    go run . --host mtls --port 8443 \
      --tls-cert server.pem --tls-key server-key.pem \
      --client-ca clients-ca.pem --client-uri spiffe://example.org/client

    export GRAINLIFT_ENDPOINT=tls+tcp://127.0.0.1:8443
    export GRAINLIFT_TLS_CA=server-ca.pem GRAINLIFT_TLS_CERT=client.pem GRAINLIFT_TLS_KEY=client-key.pem
    export GRAINLIFT_TLS_SERVER_NAME=localhost   # the DNS name in the server certificate
    go run ./examples/client

These hosts are for development and bind to loopback. For production
deployment, limits and the security contract, see the
[SDK README](https://github.com/Query-farm/grainlift-go#transports-and-authentication).

## Development

    test -z "$(gofmt -l .)" && go vet ./...
    GRAINLIFT_DRIVER=$PWD/../grainlift/target/debug/libadbc_driver_grainlift.dylib go test -race ./...

Native integration tests skip when `GRAINLIFT_DRIVER` is unset. They include
running `examples/query.sql` in the Haybarn CLI (`$HAYBARN`, `haybarn` on
`PATH`, or `uvx haybarn-cli`), which downloads the `adbc_scanner` extension on
first use. CI builds a pinned native-driver revision and runs everything on
Linux and macOS.

### Shared conformance

`cmd/conformance-worker` implements the worker process contract of
Grainlift's shared conformance suite (`validation/conformance` in the grainlift
repository): the synthetic `QUERY`/`FAIL` workload over HTTP, HTTPS, TCP, mTLS
and Iroh, plus the request-limit and object-storage contract
(`--max-request-bytes`, `--storage-*`, and the `STORE`/`STORED` commands). From
a grainlift checkout:

    go build -o /tmp/conformance-worker ./cmd/conformance-worker   # in this repository
    python -m pytest validation/conformance -q \
      --native-driver "$PWD/target/release/libadbc_driver_grainlift.so" \
      --worker-command '["/tmp/conformance-worker"]'

CI runs the suite on every transport.

To develop against a local SDK checkout, use an uncommitted Go workspace:
`go work init . ../grainlift-go`.

Licensed under [Apache-2.0](LICENSE).
