# Grainlift hello world (Go)

A runnable Go worker built with [grainlift-go](https://github.com/Query-farm/grainlift-go). Applications connect through the ordinary [Grainlift ADBC driver](https://github.com/Query-farm/grainlift) and receive lazily generated Arrow batches. Use this repository to learn the backend API, check interoperability, or compare the same synthetic workload across languages.

## Status

HTTP, HTTPS, loopback TCP, mTLS TCP, and Iroh use published
[VGI Go v0.30.0](https://github.com/Query-farm/vgi-rpc-go/releases/tag/v0.30.0),
which includes the network-safe serving entrypoint. The example still uses a
Go workspace when developing against the sibling Grainlift Go SDK.

The synthetic backend supports `QUERY`, `FAIL`, schema inference, and autocommit. Transactions, preparation, binding, metadata, and other optional capabilities return ADBC `NOT_IMPLEMENTED`. The SDK has positive fixture coverage for those hooks; this example is not a database or a production certification.

The example tests keep two independent connections and cursors open together, then release one while checking that the other's batches and close accounting remain correct. Cross-client commit visibility and write contention are outside this stateless worker's capabilities.

## Quickstart

Requires Go 1.26 or newer and OpenSSL for the token-generation command. Use a
fresh workspace to develop against the sibling SDK; `go.mod` records a specific
SDK commit for standalone reproducibility:

```sh
mkdir grainlift-go-workspace
cd grainlift-go-workspace
git clone https://github.com/Query-farm/grainlift-go.git
git clone https://github.com/Query-farm/grainlift-hello-world-go.git
go work init ./grainlift-go ./grainlift-hello-world-go

cd grainlift-hello-world-go
go build -o grainlift-hello-world-go .
export GRAINLIFT_HELLO_TOKEN="$(openssl rand -hex 32)"
./grainlift-hello-world-go --transport http --port 8080 --report report.json
```

The worker prints one readiness JSON line containing `endpoint` and `sample_pid`. Keep stdin open: Enter, EOF, SIGINT, or SIGTERM stops the worker and writes the optional shutdown report.

In another terminal, install `adbc-driver-manager` and `pyarrow`, export the same `GRAINLIFT_HELLO_TOKEN` value, and set `GRAINLIFT_DRIVER` to the absolute path of your built native Grainlift driver library. Then run:

```python
import os
import adbc_driver_manager.dbapi as adbc

with adbc.connect(
    driver=os.environ["GRAINLIFT_DRIVER"],
    entrypoint="AdbcDriverGrainliftInit",
    autocommit=True,
    db_kwargs={
        "grainlift.uri": "http://127.0.0.1:8080",
        "grainlift.target": "default",
        "grainlift.auth.bearer_token": os.environ["GRAINLIFT_HELLO_TOKEN"],
    },
) as connection:
    with connection.cursor() as cursor:
        cursor.execute("QUERY")
        table = cursor.fetch_arrow_table()
        print(table.schema)
        print(table.num_rows)  # 4096
```

The [Grainlift repository](https://github.com/Query-farm/grainlift) documents building and loading the native driver. Other ADBC clients use the same connection options.

## Workload and configuration

Defaults match the Python and Rust synthetic workers: 4,096 rows, 512 rows per batch, and 64 payload bytes per row.

| Column | Arrow type | Values |
| --- | --- | --- |
| `number` | Nullable int64 | Sequential row numbers starting at zero |
| `payload` | Nullable binary | Repeated `x` bytes |

`QUERY` generates batches lazily. `FAIL` returns ADBC invalid-data, SQLSTATE `22000`, vendor code 42, and a binary detail; the same statement remains usable afterward.

| Flag | Default | Meaning |
| --- | --- | --- |
| `--transport` | `http` | `http`, `https`, `tcp`, `mtls`, or `iroh` |
| `--port` | `0` | Loopback port; zero selects an available port |
| `--tls-dir` | Empty | Certificate directory for HTTPS/mTLS |
| `--rows` | `4096` | Rows per query, 1–1,000,000 |
| `--batch-rows` | `512` | Rows per batch, 1–4,096 |
| `--payload-bytes` | `64` | Bytes per payload, 0–1,024 |
| `--report` | Empty | Optional aggregate shutdown report path |

The workload also requires `batchRows * (payloadBytes + 16) <= 1 MiB`. The shutdown report records counters and actual remaining SDK resources, without query text, credentials, or handles.

HTTP(S) requires `GRAINLIFT_HELLO_TOKEN` of at least 16 bytes. Optional `GRAINLIFT_HELLO_OTHER_TOKEN` creates a second principal for ownership tests. Use your own secret outside a local demonstration.

## Transports and identities

| Transport | Endpoint | Authentication |
| --- | --- | --- |
| HTTP | `http://127.0.0.1:PORT` | Bearer token |
| HTTPS | `https://127.0.0.1:PORT` | Verified server certificate and bearer token |
| TCP | `tcp://127.0.0.1:PORT` | Shared trusted-local identity; loopback only |
| mTLS TCP | `tls+tcp://127.0.0.1:PORT` | Verified client certificate with an explicitly allowed URI SAN |
| Iroh | `iroh://ENDPOINT_ID` | Allowlisted authenticated client endpoint ID |

TCP, mTLS, and Iroh require the upstream development setup below. Raw transports do not use bearer tokens.

### HTTPS and mTLS

Both modes load `server.pem`, `server-key.pem`, and `ca.pem` from `--tls-dir`. To generate local test fixtures, run from the workspace directory:

```sh
git clone https://github.com/Query-farm/grainlift.git
bash grainlift/validation/diagnostics/make_test_tls.sh "$PWD/tls"
export GRAINLIFT_HELLO_TOKEN="$(openssl rand -hex 32)"
./grainlift-hello-world-go/grainlift-hello-world-go \
  --transport https --tls-dir "$PWD/tls" --port 8443
```

Configure the native client with `grainlift.tls.ca` pointing to `ca.pem`; certificate verification remains enabled. For mTLS, also set `grainlift.tls.cert`, `grainlift.tls.key`, and `grainlift.tls.server_name=localhost`.

The example accepts a verified certificate with exactly one URI SAN: `spiffe://benchmark.test/client` or `spiffe://benchmark.test/other`. They map to distinct principals. Other trust domains and identities are rejected. Replace this example allowlist with your deployment's explicit identity policy.

### Iroh

Iroh carries raw VGI streams over QUIC through the separate VGI bridge. Build the pinned bridge release from its source repository; the bridge package is not published to crates.io:

```sh
# From the workspace directory; requires the Rust toolchain used by VGI.
git clone --branch v0.27.3 --depth 1 \
  https://github.com/Query-farm/vgi-rpc-rust.git
cargo build --manifest-path vgi-rpc-rust/Cargo.toml \
  --locked --release -p vgi-iroh-bridge
export GRAINLIFT_IROH_BRIDGE="$PWD/vgi-rpc-rust/target/release/vgi-iroh-bridge"
```

Set `GRAINLIFT_HELLO_IROH_CLIENT_ID` to the native client's endpoint public key in hexadecimal; optionally set `GRAINLIFT_HELLO_IROH_OTHER_CLIENT_ID` for a second identity. Then launch with `--transport iroh`. The native client uses its matching `grainlift.iroh.secret_key`; the conformance suite provides a tested identity fixture.

Readiness adds `endpoint_id` and `direct_address`. For this relay-disabled local example, pass the advertised address as `grainlift.iroh.direct_address`. The bridge uses a private Unix upstream socket. The bridge and same-UID processes are trusted. Startup failure and shutdown reap the child process; unexpected bridge exit stops the worker.

### Raw transport validation

The declared VGI v0.30.0 dependency disables shared-memory negotiation on
network streams and validates parameter rows and dynamic headers. CI requires
that network-safe entrypoint and runs native ADBC conformance over all five
transports with the published dependency. No local VGI replacement is needed.

## Limits and deployment

The [SDK limits](https://github.com/Query-farm/grainlift-go#limits-and-deployment) bound sessions, statements, cursors, input sizes, retained buffers, socket admission, and handshake/read/write waits. Arrow decoding still needs process memory limits. Stop the host before closing backend resources.

Sessions, transactions, and live result cursors belong to one process; deployments need connection affinity. This example binds loopback and uses development identities. It has not established throughput or sustained real-backend production behavior.

## Testing

```sh
go test -race -count=1 ./...
go vet ./...
test -z "$(gofmt -l *.go)"
```

From a Grainlift checkout with its Python test dependencies and native driver built:

```sh
python -m pytest validation/conformance -q \
  --worker-command '["/absolute/path/grainlift-hello-world-go"]' \
  --native-driver /absolute/path/libadbc_driver_grainlift.so
```

Select another mode with `--worker-transport`; HTTPS/mTLS also need `--worker-tls-dir`, and Iroh needs `--iroh-bridge`. Tests cover real ADBC execution, typed protocol records, principal ownership, malformed requests, and cleanup.

[Recorded EC2 validation](https://github.com/Query-farm/grainlift-go/blob/main/VALIDATION.md) separates published-dependency checks from the explicit patched-upstream matrix. Test counts vary by language because fixtures and subtests are grouped differently; shared conformance behaviors are the comparison to use.

## Documentation and license

- [Go SDK, backend interfaces, and limits](https://github.com/Query-farm/grainlift-go)
- [Grainlift driver and protocol](https://github.com/Query-farm/grainlift)
- [Shared native conformance suite](https://github.com/Query-farm/grainlift/tree/main/validation/conformance)
- [VGI Go network-safety development branch](https://github.com/Query-farm/vgi-rpc-go/tree/grainlift-network-safety)

Licensed under [Apache-2.0](LICENSE).
