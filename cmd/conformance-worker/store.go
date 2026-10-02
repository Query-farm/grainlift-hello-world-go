// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0
package main

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"

	grainlift "github.com/Query-farm/grainlift-go"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// store holds the rows last bound to STORE, shared by every connection of
// the process (the storage contract).
type store struct {
	mu      sync.Mutex
	schema  *arrow.Schema
	batches []arrow.RecordBatch
	// chunkBytes bounds each STORED batch so it fits a result batch.
	chunkBytes int
	stored     atomic.Int64
}

func (s *store) replace(schema *arrow.Schema, batches []arrow.RecordBatch) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, b := range s.batches {
		b.Release()
	}
	s.schema, s.batches = schema, batches
	var rows int64
	for _, b := range batches {
		rows += b.NumRows()
	}
	s.stored.Add(1)
	return rows
}

// snapshot returns the stored rows as compact batches of at most chunkBytes
// (or one row): each slice is copied through IPC, so it owns only its rows.
func (s *store) snapshot() (*arrow.Schema, []arrow.RecordBatch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	schema := s.schema
	if schema == nil {
		schema = storeSchema
	}
	var out []arrow.RecordBatch
	for _, b := range s.batches {
		for start := int64(0); start < b.NumRows(); {
			end := start + 1
			for end < b.NumRows() && rowBytes(b, start, end+1) <= int64(s.chunkBytes) {
				end++
			}
			piece, e := compact(b.NewSlice(start, end))
			if e != nil {
				for _, o := range out {
					o.Release()
				}
				return nil, nil, e
			}
			out = append(out, piece)
			start = end
		}
	}
	return schema, out, nil
}

func (s *store) close() {
	s.replace(nil, nil)
}

var storeSchema = arrow.NewSchema([]arrow.Field{{Name: "number", Type: arrow.PrimitiveTypes.Int64, Nullable: true}, {Name: "payload", Type: arrow.BinaryTypes.Binary, Nullable: true}}, nil)

// rowBytes estimates rows [start, end) of b: variable-length values plus a
// fixed allowance per row and column.
func rowBytes(b arrow.RecordBatch, start, end int64) int64 {
	size := (end - start) * 16 * b.NumCols()
	for _, c := range b.Columns() {
		if binary, ok := c.(*array.Binary); ok {
			offsets := binary.ValueOffsets() // Len()+1 entries
			size += int64(offsets[end] - offsets[start])
		}
	}
	return size
}

func compact(slice arrow.RecordBatch) (arrow.RecordBatch, error) {
	defer slice.Release()
	var buffer bytes.Buffer
	writer := ipc.NewWriter(&buffer, ipc.WithSchema(slice.Schema()))
	if e := writer.Write(slice); e != nil {
		return nil, e
	}
	if e := writer.Close(); e != nil {
		return nil, e
	}
	reader, e := ipc.NewReader(&buffer)
	if e != nil {
		return nil, e
	}
	defer reader.Release()
	if !reader.Next() {
		return nil, reader.Err()
	}
	batch := reader.RecordBatch()
	batch.Retain()
	return batch, nil
}

// storeStatement wraps a workload statement with STORE and STORED.
type storeStatement struct {
	*statement
	store *store
	bound []arrow.RecordBatch
	shape *arrow.Schema
}

func (s *storeStatement) dropBound() {
	for _, b := range s.bound {
		b.Release()
	}
	s.bound, s.shape = nil, nil
}
func (s *storeStatement) Bind(_ context.Context, b arrow.RecordBatch) error {
	s.dropBound()
	b.Retain()
	s.bound, s.shape = []arrow.RecordBatch{b}, b.Schema()
	return nil
}
func (s *storeStatement) BindStream(_ context.Context, r array.RecordReader) error {
	s.dropBound()
	s.shape = r.Schema()
	for r.Next() {
		b := r.RecordBatch()
		b.Retain()
		s.bound = append(s.bound, b)
	}
	return r.Err()
}
func (s *storeStatement) storeBound() (int64, error) {
	if s.shape == nil {
		return 0, &grainlift.Error{Status: "invalid_state", Message: "STORE needs bound parameters"}
	}
	rows := s.store.replace(s.shape, s.bound)
	s.bound, s.shape = nil, nil
	return rows, nil
}
func (s *storeStatement) ExecuteUpdate(ctx context.Context) (*int64, error) {
	if s.sql != "STORE" {
		return nil, &grainlift.Error{Status: "invalid_arguments", Message: "Only STORE is an update"}
	}
	rows, e := s.storeBound()
	return &rows, e
}
func (s *storeStatement) ExecuteSchema(ctx context.Context) (*arrow.Schema, error) {
	if s.sql == "STORED" {
		schema, batches, e := s.store.snapshot()
		for _, b := range batches {
			b.Release()
		}
		return schema, e
	}
	return s.statement.ExecuteSchema(ctx)
}
func (s *storeStatement) Execute(ctx context.Context) (*grainlift.QueryResult, error) {
	switch s.sql {
	case "STORE":
		rows, e := s.storeBound()
		if e != nil {
			return nil, e
		}
		empty := arrow.NewSchema(nil, nil)
		reader, e := array.NewRecordReader(empty, nil)
		return &grainlift.QueryResult{Reader: reader, RowsAffected: &rows}, e
	case "STORED":
		schema, batches, e := s.store.snapshot()
		if e != nil {
			return nil, e
		}
		reader, e := array.NewRecordReader(schema, batches)
		for _, b := range batches {
			b.Release()
		}
		return &grainlift.QueryResult{Reader: reader}, e
	}
	return s.statement.Execute(ctx)
}
func (s *storeStatement) Close() error {
	s.dropBound()
	return s.statement.Close()
}
