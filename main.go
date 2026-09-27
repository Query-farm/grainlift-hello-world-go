// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0
package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"flag"
	"fmt"
	grainlift "github.com/Query-farm/grainlift-go"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
)

type workload struct{ rows, batchRows, payloadBytes int }

func (w workload) validate() error {
	if w.rows < 1 || w.rows > 1_000_000 || w.batchRows < 1 || w.batchRows > 4096 || w.payloadBytes < 0 || w.payloadBytes > 1024 || w.batchRows*(w.payloadBytes+16) > 1<<20 {
		return fmt.Errorf("invalid workload dimensions")
	}
	return nil
}
func (w workload) schema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{{Name: "number", Type: arrow.PrimitiveTypes.Int64, Nullable: true}, {Name: "payload", Type: arrow.BinaryTypes.Binary, Nullable: true}}, nil)
}

type counters struct{ opened, closed, queries, failures, batches atomic.Int64 }
type backend struct {
	work   workload
	counts *counters
}

func (b *backend) Open(ctx context.Context, principal string, p grainlift.OpenConnectionRequest) (grainlift.Connection, error) {
	b.counts.opened.Add(1)
	return &connection{work: b.work, counts: b.counts}, nil
}

type connection struct {
	grainlift.UnimplementedConnection
	work   workload
	counts *counters
	closed bool
}

func (c *connection) NewStatement(context.Context) (grainlift.Statement, error) {
	return &statement{work: c.work, counts: c.counts}, nil
}
func (c *connection) SetOption(ctx context.Context, key string, value grainlift.OptionValue) error {
	if key == "adbc.connection.autocommit" && value.Kind == "string" && value.StringValue != nil && *value.StringValue == "true" {
		return nil
	}
	return &grainlift.Error{Status: "not_implemented", Message: "Only autocommit is supported"}
}
func (c *connection) Close() error {
	if !c.closed {
		c.closed = true
		c.counts.closed.Add(1)
	}
	return nil
}

type statement struct {
	grainlift.UnimplementedStatement
	work   workload
	counts *counters
	sql    string
}

func (s *statement) SetSQLQuery(ctx context.Context, sql string) error { s.sql = sql; return nil }
func (s *statement) ExecuteSchema(context.Context) (*arrow.Schema, error) {
	return s.work.schema(), nil
}
func (s *statement) Execute(ctx context.Context) (*grainlift.QueryResult, error) {
	if s.sql == "FAIL" {
		s.counts.failures.Add(1)
		return nil, &grainlift.Error{Status: "invalid_data", Message: "Synthetic failure", SQLState: "22000", VendorCode: 42, Details: []grainlift.ErrorDetail{{Key: "detail", Value: []byte("synthetic")}}}
	}
	if s.sql != "QUERY" {
		return nil, &grainlift.Error{Status: "invalid_arguments", Message: "Expected QUERY or FAIL"}
	}
	s.counts.queries.Add(1)
	r := &reader{work: s.work, schema: s.work.schema(), counts: s.counts}
	r.refs.Store(1)
	return &grainlift.QueryResult{Reader: r}, nil
}

type reader struct {
	work     workload
	schema   *arrow.Schema
	counts   *counters
	position int
	batch    arrow.RecordBatch
	refs     atomic.Int64
}

func (r *reader) Retain() { r.refs.Add(1) }
func (r *reader) Release() {
	if r.refs.Add(-1) == 0 && r.batch != nil {
		r.batch.Release()
		r.batch = nil
	}
}
func (r *reader) Schema() *arrow.Schema          { return r.schema }
func (r *reader) Err() error                     { return nil }
func (r *reader) RecordBatch() arrow.RecordBatch { return r.batch }
func (r *reader) Record() arrow.Record           { return r.batch }
func (r *reader) Next() bool {
	if r.batch != nil {
		r.batch.Release()
		r.batch = nil
	}
	if r.position == r.work.rows {
		return false
	}
	end := min(r.position+r.work.batchRows, r.work.rows)
	numbers := array.NewInt64Builder(memory.DefaultAllocator)
	defer numbers.Release()
	payloads := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
	defer payloads.Release()
	payload := bytes.Repeat([]byte("x"), r.work.payloadBytes)
	for i := r.position; i < end; i++ {
		numbers.Append(int64(i))
		payloads.Append(payload)
	}
	a, b := numbers.NewArray(), payloads.NewArray()
	defer a.Release()
	defer b.Release()
	r.batch = array.NewRecordBatch(r.schema, []arrow.Array{a, b}, int64(end-r.position))
	r.position = end
	r.counts.batches.Add(1)
	return true
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "Synthetic server failed")
		os.Exit(1)
	}
}
func run() error {
	port := flag.Int("port", 0, "Loopback HTTP port")
	transport := flag.String("transport", "http", "http, https, tcp, mtls, or iroh")
	tlsDir := flag.String("tls-dir", "", "TLS certificate fixture directory")
	rows := flag.Int("rows", 4096, "Rows per query")
	batchRows := flag.Int("batch-rows", 512, "Rows per batch")
	payloadBytes := flag.Int("payload-bytes", 64, "Payload bytes per row")
	report := flag.String("report", "", "Shutdown report path")
	flag.Parse()
	work := workload{*rows, *batchRows, *payloadBytes}
	if e := work.validate(); e != nil {
		return e
	}
	token := os.Getenv("GRAINLIFT_HELLO_TOKEN")
	other := os.Getenv("GRAINLIFT_HELLO_OTHER_TOKEN")
	if ((*transport == "http" || *transport == "https") && len(token) < 16) || (*port < 0 || *port > 65535) {
		return fmt.Errorf("invalid configuration")
	}
	counts := &counters{}
	svc, e := grainlift.NewService(map[string]grainlift.Target{"default": {Backend: &backend{work, counts}, Authorize: func(p string) bool { return p == "load-principal" || p == "other-principal" || iroPeers()[p] }, AllowedConnectionOptions: map[string]bool{"adbc.connection.autocommit": true}}}, grainlift.DefaultLimits())
	if e != nil {
		return e
	}
	defer svc.Close()
	auth := func(r *http.Request) (*vgirpc.AuthContext, error) {
		provided := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		principal := ""
		if subtle.ConstantTimeCompare([]byte(provided), []byte(token)) == 1 {
			principal = "load-principal"
		} else if len(other) >= 16 && subtle.ConstantTimeCompare([]byte(provided), []byte(other)) == 1 {
			principal = "other-principal"
		}
		if !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") || principal == "" {
			return nil, fmt.Errorf("invalid credentials")
		}
		return &vgirpc.AuthContext{Domain: "bearer", Principal: principal, Authenticated: true}, nil
	}
	host, e := startHost(svc, *transport, *tlsDir, *port, auth)
	if e != nil {
		return e
	}
	_ = json.NewEncoder(os.Stdout).Encode(host.ready)
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(stop)
	input := make(chan struct{})
	go func() { _, _ = bufio.NewReader(os.Stdin).ReadString('\n'); close(input) }()
	select {
	case <-stop:
	case <-input:
	case <-host.done:
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = host.close(ctx)
	if e := svc.Close(); e != nil {
		return e
	}
	if *report != "" {
		f, e := os.OpenFile(*report, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
		if e != nil {
			return e
		}
		defer f.Close()
		return json.NewEncoder(f).Encode(map[string]any{"after_shutdown": svc.ResourceCounts(), "opened": counts.opened.Load(), "closed": counts.closed.Load(), "queries": counts.queries.Load(), "failures": counts.failures.Load(), "batches": counts.batches.Load()})
	}
	return nil
}
