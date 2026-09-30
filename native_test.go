// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0

// Real C-ABI coverage through the ADBC driver manager and the native Grainlift
// driver. These tests skip unless GRAINLIFT_DRIVER names the driver library.
package main

import (
	"bufio"
	"context"
	"errors"
	grainlift "github.com/Query-farm/grainlift-go"
	"github.com/Query-farm/grainlift-hello-world-go/hello"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"sync"
	"testing"
	"time"
)

// TestMain lets the native tests run this command as a subprocess.
func TestMain(m *testing.M) {
	if os.Getenv("GRAINLIFT_HELLO_WORLD_RUN_MAIN") == "1" {
		main()
		os.Exit(0)
	}
	os.Exit(m.Run())
}

var ctx = context.Background()

func driverPath(t *testing.T) string {
	t.Helper()
	driver := os.Getenv("GRAINLIFT_DRIVER")
	if driver == "" {
		t.Skip("Set GRAINLIFT_DRIVER")
	}
	resolved, err := filepath.Abs(driver)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(resolved); err != nil {
		t.Fatal(err)
	}
	return resolved
}

// principals records who opened each connection.
type principals struct {
	hello.Backend
	mu   sync.Mutex
	seen []string
}

func (p *principals) Open(ctx context.Context, principal string, request grainlift.OpenConnectionRequest) (grainlift.Connection, error) {
	p.mu.Lock()
	p.seen = append(p.seen, principal)
	p.mu.Unlock()
	return p.Backend.Open(ctx, principal, request)
}
func (p *principals) sorted() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Sorted(slices.Values(p.seen))
}

// serve hosts a backend over HTTP on an ephemeral port with the given access.
func serve(t *testing.T, backend grainlift.Backend, tokens map[string]string, anonymous string) (string, *grainlift.Service) {
	t.Helper()
	driverPath(t)
	authenticate, err := grainlift.HTTPAuthenticator(tokens, anonymous)
	if err != nil {
		t.Fatal(err)
	}
	target := hello.Target()
	target.Backend = backend
	service, err := grainlift.NewService(map[string]grainlift.Target{hello.TargetName: target}, grainlift.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(service.HTTPHandler(authenticate))
	t.Cleanup(func() {
		server.Close()
		if err := service.Close(); err != nil {
			t.Error(err)
		}
	})
	return server.URL, service
}

// connect opens a native ADBC connection, anonymously when token is empty.
func connect(t *testing.T, endpoint, token string) (adbc.Connection, error) {
	t.Helper()
	options := map[string]string{
		"driver":           driverPath(t),
		"entrypoint":       "AdbcDriverGrainliftInit",
		"grainlift.uri":    endpoint,
		"grainlift.target": hello.TargetName,
	}
	if token != "" {
		options["grainlift.auth.bearer_token"] = token
	}
	database, err := drivermgr.Driver{}.NewDatabase(options)
	if err != nil {
		return nil, err
	}
	connection, err := database.Open(ctx)
	if err != nil {
		_ = database.Close()
		return nil, err
	}
	t.Cleanup(func() { _ = database.Close() })
	return connection, nil
}

func mustConnect(t *testing.T, endpoint, token string) adbc.Connection {
	t.Helper()
	connection, err := connect(t, endpoint, token)
	if err != nil {
		t.Fatal(err)
	}
	return connection
}

// fetch executes sql and returns every batch; the batches are released by cleanup.
func fetch(t *testing.T, connection adbc.Connection, sql string) (*arrow.Schema, []arrow.RecordBatch, error) {
	t.Helper()
	statement, err := connection.NewStatement()
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Close()
	if err := statement.SetSqlQuery(sql); err != nil {
		t.Fatal(err)
	}
	reader, _, err := statement.ExecuteQuery(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer reader.Release()
	var batches []arrow.RecordBatch
	for reader.Next() {
		batch := reader.RecordBatch()
		batch.Retain()
		t.Cleanup(batch.Release)
		batches = append(batches, batch)
	}
	return reader.Schema(), batches, reader.Err()
}

func mustFetch(t *testing.T, connection adbc.Connection, sql string) []arrow.RecordBatch {
	t.Helper()
	_, batches, err := fetch(t, connection, sql)
	if err != nil {
		t.Fatal(err)
	}
	return batches
}

func sizes(batches []arrow.RecordBatch) []int64 {
	var out []int64
	for _, batch := range batches {
		out = append(out, batch.NumRows())
	}
	return out
}

// column concatenates one int64 column across batches.
func column(batches []arrow.RecordBatch, index int) []int64 {
	var out []int64
	for _, batch := range batches {
		out = append(out, batch.Column(index).(*array.Int64).Int64Values()...)
	}
	return out
}

func adbcError(t *testing.T, err error, code adbc.Status, sqlstate string) {
	t.Helper()
	var failure adbc.Error
	if !errors.As(err, &failure) || failure.Code != code || string(failure.SqlState[:]) != sqlstate {
		t.Fatalf("want ADBC status %v SQLSTATE %s, got %v", code, sqlstate, err)
	}
}

func TestRealADBCQueriesSchemaErrorsAndCleanup(t *testing.T) {
	endpoint, service := serve(t, hello.Backend{}, map[string]string{"test-token": "alice"}, "")
	connection := mustConnect(t, endpoint, "test-token")
	if err := connection.(adbc.PostInitOptions).SetOption(adbc.OptionKeyAutoCommit, adbc.OptionValueEnabled); err != nil {
		t.Fatal(err)
	}
	hi := mustFetch(t, connection, "SELECT 'Hello, world!' AS message")
	if len(hi) != 1 || hi[0].Column(0).(*array.String).Value(0) != "Hello, world!" {
		t.Fatal("hello")
	}
	numbers := mustFetch(t, connection, "SELECT * FROM numbers(2500)")
	if got := sizes(numbers); !slices.Equal(got, []int64{1024, 1024, 452}) || column(numbers, 0)[2499] != 2499 {
		t.Fatalf("numbers batches %v", got)
	}
	totals := mustFetch(t, connection, "SELECT * FROM running_total(2500)")
	if got := sizes(totals); !slices.Equal(got, []int64{1024, 1024, 452}) || column(totals, 1)[2499] != 2499*2500/2 {
		t.Fatalf("running_total batches %v", got)
	}
	schema, empty, err := fetch(t, connection, "SELECT * FROM numbers(0)")
	if err != nil || len(empty) != 0 || !schema.Equal(hello.NumbersSchema) {
		t.Fatalf("empty result: %v %v", schema, err)
	}
	_, _, err = fetch(t, connection, "unsupported")
	adbcError(t, err, adbc.StatusInvalidArgument, "42000")
	statement, err := connection.NewStatement()
	if err != nil {
		t.Fatal(err)
	}
	if err := statement.SetSqlQuery("SELECT * FROM numbers(100000)"); err != nil {
		t.Fatal(err)
	}
	reader, _, err := statement.ExecuteQuery(ctx)
	if err != nil || !reader.Next() || reader.RecordBatch().NumRows() != 1024 {
		t.Fatalf("partial read: %v", err)
	}
	reader.Release()
	_ = statement.Close()
	if err := connection.Close(); err != nil {
		t.Fatal(err)
	}
	if n := service.ResourceCounts()["sessions"]; n != 0 {
		t.Fatalf("%d sessions after close", n)
	}
}

func TestPreparedStatements(t *testing.T) {
	endpoint, _ := serve(t, hello.Backend{}, map[string]string{"test-token": "alice"}, "")
	connection := mustConnect(t, endpoint, "test-token")
	defer connection.Close()
	statement, err := connection.NewStatement()
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Close()
	if err := statement.SetSqlQuery("SELECT * FROM running_total(3)"); err != nil {
		t.Fatal(err)
	}
	if err := statement.Prepare(ctx); err != nil {
		t.Fatal(err)
	}
	reader, _, err := statement.ExecuteQuery(ctx)
	if err != nil || !reader.Next() || !slices.Equal(reader.RecordBatch().Column(1).(*array.Int64).Int64Values(), []int64{0, 1, 3}) {
		t.Fatalf("prepared running_total: %v", err)
	}
	reader.Release()
	if err := statement.SetSqlQuery("DROP TABLE x"); err != nil {
		t.Fatal(err)
	}
	adbcError(t, statement.Prepare(ctx), adbc.StatusInvalidArgument, "42000")
}

func TestRunningTotalResumesAcrossContinuationTokens(t *testing.T) {
	endpoint, _ := serve(t, hello.Backend{}, nil, "public")
	connection := mustConnect(t, endpoint, "")
	defer connection.Close()
	totals := mustFetch(t, connection, "SELECT * FROM running_total(100000)")
	numbers, sums := column(totals, 0), column(totals, 1)
	if len(totals) != 98 || len(numbers) != 100000 {
		t.Fatalf("%d batches, %d rows", len(totals), len(numbers))
	}
	for n, total := range sums {
		if numbers[n] != int64(n) || total != int64(n)*int64(n+1)/2 {
			t.Fatalf("row %d: (%d, %d)", n, numbers[n], total)
		}
	}
}

func TestAuthenticationRequired(t *testing.T) {
	endpoint, service := serve(t, hello.Backend{}, map[string]string{"test-token": "alice"}, "")
	for _, token := range []string{"wrong-token", ""} {
		if connection, err := connect(t, endpoint, token); err == nil {
			_ = connection.Close()
			t.Fatalf("token %q accepted", token)
		}
	}
	if n := service.ResourceCounts()["sessions"]; n != 0 {
		t.Fatalf("%d sessions opened", n)
	}
}

func TestAnonymousClientQueriesWithoutAToken(t *testing.T) {
	backend := &principals{}
	endpoint, service := serve(t, backend, map[string]string{"test-token": "alice"}, "public")
	connection := mustConnect(t, endpoint, "")
	if hi := mustFetch(t, connection, "SELECT 'Hello, world!' AS message"); hi[0].Column(0).(*array.String).Value(0) != "Hello, world!" {
		t.Fatal("hello")
	}
	if got := sizes(mustFetch(t, connection, "SELECT * FROM numbers(2500)")); !slices.Equal(got, []int64{1024, 1024, 452}) {
		t.Fatalf("numbers batches %v", got)
	}
	if totals := column(mustFetch(t, connection, "SELECT * FROM running_total(2500)"), 1); totals[2499] != 2499*2500/2 {
		t.Fatal("running_total")
	}
	if got := backend.sorted(); !slices.Equal(got, []string{"public"}) {
		t.Fatalf("principals %v", got)
	}
	_ = connection.Close()
	if n := service.ResourceCounts()["sessions"]; n != 0 {
		t.Fatalf("%d sessions after close", n)
	}
}

func TestAnonymousEndpointStillAuthenticatesTokens(t *testing.T) {
	backend := &principals{}
	endpoint, service := serve(t, backend, map[string]string{"test-token": "alice"}, "public")
	alice, anonymous := mustConnect(t, endpoint, "test-token"), mustConnect(t, endpoint, "")
	if got := backend.sorted(); !slices.Equal(got, []string{"alice", "public"}) {
		t.Fatalf("principals %v", got)
	}
	for _, connection := range []adbc.Connection{alice, anonymous} {
		if got := column(mustFetch(t, connection, "SELECT * FROM numbers(3)"), 0); !slices.Equal(got, []int64{0, 1, 2}) {
			t.Fatalf("numbers(3) = %v", got)
		}
		_ = connection.Close()
	}
	if connection, err := connect(t, endpoint, "wrong-token"); err == nil {
		_ = connection.Close()
		t.Fatal("wrong token downgraded to anonymous")
	}
	if n := service.ResourceCounts()["sessions"]; n != 0 {
		t.Fatalf("%d sessions after close", n)
	}
}

// failing replaces every statement's Execute with a structured ADBC error.
type failing struct{ hello.Backend }
type failingConnection struct{ hello.Connection }
type failingStatement struct{ grainlift.Statement }

func (failing) Open(context.Context, string, grainlift.OpenConnectionRequest) (grainlift.Connection, error) {
	return &failingConnection{}, nil
}
func (c *failingConnection) NewStatement(ctx context.Context) (grainlift.Statement, error) {
	statement, err := c.Connection.NewStatement(ctx)
	return &failingStatement{statement}, err
}
func (failingStatement) Execute(context.Context) (*grainlift.QueryResult, error) {
	return nil, &grainlift.Error{Status: "invalid_data", Message: "Invalid data", SQLState: "22000", VendorCode: 42,
		Details: []grainlift.ErrorDetail{{Key: "binary", Value: []byte{0x00, 0xff}}}}
}

func TestStructuredADBCError(t *testing.T) {
	endpoint, _ := serve(t, failing{}, map[string]string{"test-token": "alice"}, "")
	connection := mustConnect(t, endpoint, "test-token")
	defer connection.Close()
	_, _, err := fetch(t, connection, "SELECT 'Hello, world!' AS message")
	// The Rust ADBC 1.1 FFI exporter uses the vendor-code slot for its
	// private-data sentinel, and the Go driver manager does not expose error
	// details, so only the status and SQLSTATE are checked here.
	adbcError(t, err, adbc.StatusInvalidData, "22000")
}

var listening = regexp.MustCompile(`listening on (http://\S+)`)

// TestCommandThroughNativeADBC runs this command, anonymous by default, with and without an exported token.
func TestCommandThroughNativeADBC(t *testing.T) {
	driverPath(t)
	for _, token := range []string{"test-token", ""} {
		command := exec.Command(os.Args[0], "--port", "0")
		command.Env = append(os.Environ(), "GRAINLIFT_HELLO_WORLD_RUN_MAIN=1", "GRAINLIFT_TOKEN="+token)
		stdout, err := command.StdoutPipe()
		if err != nil {
			t.Fatal(err)
		}
		if err := command.Start(); err != nil {
			t.Fatal(err)
		}
		lines := make(chan string, 4)
		go func() {
			scanner := bufio.NewScanner(stdout)
			for scanner.Scan() {
				lines <- scanner.Text()
			}
			close(lines)
		}()
		var endpoint string
		for endpoint == "" {
			select {
			case line, ok := <-lines:
				if !ok {
					t.Fatal("command exited before listening")
				}
				if match := listening.FindStringSubmatch(line); match != nil {
					endpoint = match[1]
				}
			case <-time.After(15 * time.Second):
				_ = command.Process.Kill()
				t.Fatal("command did not start")
			}
		}
		connection := mustConnect(t, endpoint, token)
		if hi := mustFetch(t, connection, "SELECT 'Hello, world!' AS message"); hi[0].Column(0).(*array.String).Value(0) != "Hello, world!" {
			t.Fatal("hello")
		}
		if got := sizes(mustFetch(t, connection, "SELECT * FROM numbers(2500)")); !slices.Equal(got, []int64{1024, 1024, 452}) {
			t.Fatalf("numbers batches %v", got)
		}
		_ = connection.Close()
		_ = command.Process.Signal(os.Interrupt)
		exited := make(chan error, 1)
		go func() { exited <- command.Wait() }()
		select {
		case err := <-exited:
			if err != nil {
				t.Fatalf("command exit: %v", err)
			}
		case <-time.After(20 * time.Second):
			_ = command.Process.Kill()
			t.Fatal("command did not stop")
		}
	}
}
