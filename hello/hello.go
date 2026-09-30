// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0

// Package hello is a minimal ADBC service built with the Grainlift Go SDK:
// three queries, no SQL engine.
//
//	SELECT 'Hello, world!' AS message
//
// A single-row result.
//
//	SELECT * FROM numbers(n)
//
// Rows 0..n-1 from a plain array.RecordReader. Simple, and fine whenever the
// server keeps the cursor in memory; a reader can also hold resources such as
// a database cursor.
//
//	SELECT * FROM running_total(n)
//
// Rows 0..n-1 with a running sum, from a serializable grainlift.ResultProducer.
// Over HTTP the producer's fields travel in the encrypted continuation token
// after every batch, so the server keeps no per-result reader, and a retried
// fetch recomputes its batch from the token.
package hello

import (
	"context"
	"fmt"
	grainlift "github.com/Query-farm/grainlift-go"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
)

const (
	// TargetName is the grainlift.target clients connect to.
	TargetName = "hello"
	// MaxRows bounds n in numbers(n) and running_total(n).
	MaxRows = 100_000
	// BatchRows is the largest Arrow batch either table function produces.
	BatchRows = 1024
)

// Result schemas.
var (
	HelloSchema        = arrow.NewSchema([]arrow.Field{{Name: "message", Type: arrow.BinaryTypes.String, Nullable: true}}, nil)
	NumbersSchema      = arrow.NewSchema([]arrow.Field{{Name: "number", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)
	RunningTotalSchema = arrow.NewSchema([]arrow.Field{
		{Name: "number", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "total", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}, nil)
)

var tableFunction = regexp.MustCompile(`^select \* from (numbers|running_total)\(([0-9]{1,6})\)$`)

// Target serves the hello backend to every authenticated (or anonymous) principal.
func Target() grainlift.Target {
	return grainlift.Target{
		Backend:   Backend{},
		Authorize: func(principal string) bool { return true },
		// Standard ADBC clients may enable autocommit, which this read-only service always has.
		AllowedConnectionOptions: map[string]bool{autocommit: true},
	}
}

// Numbers returns 0..count-1 in batches of at most BatchRows rows.
func Numbers(count int64) array.RecordReader {
	r := &numbers{count: count}
	r.refs.Store(1)
	return r
}

// numbers is a plain array.RecordReader; its position lives in server memory.
type numbers struct {
	refs            atomic.Int64
	count, position int64
	batch           arrow.RecordBatch
}

func (r *numbers) Next() bool {
	r.release()
	if r.position >= r.count {
		return false
	}
	end := min(r.position+BatchRows, r.count)
	b := array.NewInt64Builder(memory.DefaultAllocator)
	defer b.Release()
	for n := r.position; n < end; n++ {
		b.Append(n)
	}
	column := b.NewArray()
	defer column.Release()
	r.batch = array.NewRecordBatch(NumbersSchema, []arrow.Array{column}, end-r.position)
	r.position = end
	return true
}
func (r *numbers) release() {
	if r.batch != nil {
		r.batch.Release()
		r.batch = nil
	}
}
func (r *numbers) Schema() *arrow.Schema          { return NumbersSchema }
func (r *numbers) RecordBatch() arrow.RecordBatch { return r.batch }
func (r *numbers) Record() arrow.RecordBatch      { return r.batch }
func (r *numbers) Err() error                     { return nil }
func (r *numbers) Retain()                        { r.refs.Add(1) }
func (r *numbers) Release() {
	if r.refs.Add(-1) == 0 {
		r.release()
	}
}

// RunningTotal is the resumable state for running_total(n); each exported
// field survives between batches.
type RunningTotal struct {
	Count    int64 // Number of rows to produce.
	Position int64 // Next number to emit.
	Total    int64 // Sum of every number emitted so far.
}

// Registering the type lets the SDK restore it from continuation tokens.
func init() { grainlift.RegisterResultProducer(&RunningTotal{}) }

// Produce emits the next batch and advances the state, or returns nil when done.
func (r *RunningTotal) Produce(context.Context) (arrow.RecordBatch, error) {
	if r.Position >= r.Count {
		return nil, nil
	}
	end := min(r.Position+BatchRows, r.Count)
	b := array.NewRecordBuilder(memory.DefaultAllocator, RunningTotalSchema)
	defer b.Release()
	numbers, totals := b.Field(0).(*array.Int64Builder), b.Field(1).(*array.Int64Builder)
	for ; r.Position < end; r.Position++ {
		r.Total += r.Position
		numbers.Append(r.Position)
		totals.Append(r.Total)
	}
	return b.NewRecordBatch(), nil
}

// Query is a recognized query.
type Query struct {
	Function string // hello, numbers or running_total.
	Count    int64  // Requested row count for the table functions.
}

// ParseQuery recognizes one of the supported queries (case-insensitive,
// optional trailing semicolon).
func ParseQuery(sql string) (Query, error) {
	text := strings.ToLower(strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(sql), ";")))
	if text == "select 'hello, world!' as message" {
		return Query{Function: "hello"}, nil
	}
	if match := tableFunction.FindStringSubmatch(text); match != nil {
		if count, _ := strconv.ParseInt(match[2], 10, 64); count <= MaxRows {
			return Query{Function: match[1], Count: count}, nil
		}
	}
	return Query{}, &grainlift.Error{
		Status: "invalid_arguments",
		Message: "Supported queries: SELECT 'Hello, world!' AS message; " +
			fmt.Sprintf("SELECT * FROM numbers(n) or running_total(n), where 0 <= n <= %d", MaxRows),
		SQLState: "42000",
	}
}

// Schema is the Arrow schema of the query's result.
func (q Query) Schema() *arrow.Schema {
	switch q.Function {
	case "hello":
		return HelloSchema
	case "numbers":
		return NumbersSchema
	}
	return RunningTotalSchema
}

// Run starts producing the query's result.
func (q Query) Run() (*grainlift.QueryResult, error) {
	switch q.Function {
	case "hello":
		b := array.NewRecordBuilder(memory.DefaultAllocator, HelloSchema)
		defer b.Release()
		b.Field(0).(*array.StringBuilder).Append("Hello, world!")
		batch := b.NewRecordBatch()
		defer batch.Release()
		reader, e := array.NewRecordReader(HelloSchema, []arrow.RecordBatch{batch})
		return &grainlift.QueryResult{Reader: reader}, e
	case "numbers":
		return &grainlift.QueryResult{Reader: Numbers(q.Count)}, nil
	}
	return grainlift.NewProducerResult(RunningTotalSchema, &RunningTotal{Count: q.Count}), nil
}

// Backend opens a Connection for each client.
type Backend struct{}

// Open opens a connection for an authenticated (or anonymous) principal.
func (Backend) Open(context.Context, string, grainlift.OpenConnectionRequest) (grainlift.Connection, error) {
	return &Connection{}, nil
}

const autocommit = "adbc.connection.autocommit"

// Connection is a client connection; each statement is independent.
type Connection struct {
	grainlift.UnimplementedConnection
}

// NewStatement creates a statement with no query set.
func (*Connection) NewStatement(context.Context) (grainlift.Statement, error) {
	return &Statement{}, nil
}

// SetOption accepts enabling autocommit, which is always on.
func (*Connection) SetOption(_ context.Context, key string, value grainlift.OptionValue) error {
	if key == autocommit && value.StringValue != nil && *value.StringValue == "true" {
		return nil
	}
	return &grainlift.Error{Status: "not_implemented", Message: "Only enabling autocommit is supported"}
}

// GetOption reports that autocommit is enabled.
func (*Connection) GetOption(_ context.Context, key, kind string) (grainlift.OptionValue, error) {
	if key == autocommit && kind == "string" {
		enabled := "true"
		return grainlift.OptionValue{Kind: "string", StringValue: &enabled}, nil
	}
	return grainlift.OptionValue{}, &grainlift.Error{Status: "not_found", Message: "Unknown option"}
}

// Statement is one ADBC statement: set a query, optionally prepare it, then execute it.
type Statement struct {
	grainlift.UnimplementedStatement
	sql *string
}

// SetSQLQuery stores the query text; it is validated when prepared or executed.
func (s *Statement) SetSQLQuery(_ context.Context, sql string) error {
	s.sql = &sql
	return nil
}

func (s *Statement) query() (Query, error) {
	if s.sql == nil {
		return Query{}, &grainlift.Error{Status: "invalid_state", Message: "Set a query before executing the statement"}
	}
	return ParseQuery(*s.sql)
}

// Prepare validates the query; clients such as DuckDB's adbc_scanner prepare before executing.
func (s *Statement) Prepare(context.Context) error {
	_, e := s.query()
	return e
}

// GetParameterSchema reports that the supported queries take no parameters.
func (s *Statement) GetParameterSchema(context.Context) (*arrow.Schema, error) {
	if _, e := s.query(); e != nil {
		return nil, e
	}
	return arrow.NewSchema(nil, nil), nil
}

// ExecuteSchema returns the result schema without producing rows.
func (s *Statement) ExecuteSchema(context.Context) (*arrow.Schema, error) {
	q, e := s.query()
	if e != nil {
		return nil, e
	}
	return q.Schema(), nil
}

// Execute runs the query.
func (s *Statement) Execute(context.Context) (*grainlift.QueryResult, error) {
	q, e := s.query()
	if e != nil {
		return nil, e
	}
	return q.Run()
}
