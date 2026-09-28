// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0
package main

import (
	"context"
	grainlift "github.com/Query-farm/grainlift-go"
	"testing"
)

func TestSyntheticBatchBoundaries(t *testing.T) {
	for _, rows := range []int{1, 511, 512, 513, 4096} {
		w := workload{rows, 512, 64}
		s := statement{work: w, counts: &counters{}, sql: "QUERY"}
		q, e := s.Execute(context.Background())
		if e != nil {
			t.Fatal(e)
		}
		n := int64(0)
		for q.Reader.Next() {
			b := q.Reader.RecordBatch()
			if b.NumRows() != int64(min(512, rows-int(n))) {
				t.Fatal("batch boundary")
			}
			n += b.NumRows()
		}
		q.Reader.Release()
		if n != int64(rows) {
			t.Fatal("row count")
		}
	}
}
func TestSyntheticFailureRecovery(t *testing.T) {
	s := statement{work: workload{1, 1, 0}, counts: &counters{}, sql: "FAIL"}
	_, e := s.Execute(context.Background())
	err, ok := e.(*grainlift.Error)
	if !ok || err.Status != "invalid_data" || err.SQLState != "22000" {
		t.Fatal("diagnostics")
	}
	s.sql = "QUERY"
	q, e := s.Execute(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	q.Reader.Release()
}
func TestIndependentClientsKeepSeparateCursors(t *testing.T) {
	work := workload{rows: 5, batchRows: 2, payloadBytes: 1}
	counts := &counters{}
	backend := &backend{work: work, counts: counts}
	first, e := backend.Open(context.Background(), "alice", grainlift.OpenConnectionRequest{})
	if e != nil {
		t.Fatal(e)
	}
	second, e := backend.Open(context.Background(), "bob", grainlift.OpenConnectionRequest{})
	if e != nil {
		t.Fatal(e)
	}
	firstStatement, e := first.NewStatement(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	secondStatement, e := second.NewStatement(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	for _, statement := range []grainlift.Statement{firstStatement, secondStatement} {
		if e := statement.SetSQLQuery(context.Background(), "QUERY"); e != nil {
			t.Fatal(e)
		}
	}
	firstResult, e := firstStatement.Execute(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	secondResult, e := secondStatement.Execute(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	if !firstResult.Reader.Next() || firstResult.Reader.RecordBatch().NumRows() != 2 {
		t.Fatal("first cursor did not produce its initial batch")
	}
	firstResult.Reader.Release()
	if e := first.Close(); e != nil {
		t.Fatal(e)
	}
	var remaining int64
	for secondResult.Reader.Next() {
		remaining += secondResult.Reader.RecordBatch().NumRows()
	}
	secondResult.Reader.Release()
	if remaining != 5 || counts.opened.Load() != 2 || counts.closed.Load() != 1 || counts.queries.Load() != 2 {
		t.Fatalf("independent client state: rows=%d opened=%d closed=%d queries=%d",
			remaining, counts.opened.Load(), counts.closed.Load(), counts.queries.Load())
	}
	if e := second.Close(); e != nil {
		t.Fatal(e)
	}
	if counts.closed.Load() != 2 {
		t.Fatal("second connection was not released")
	}
}
func TestWorkloadLimits(t *testing.T) {
	for _, w := range []workload{{0, 1, 1}, {1, 0, 1}, {1, 4097, 1}, {1, 1, 1025}, {1, 4096, 1024}} {
		if w.validate() == nil {
			t.Fatal("invalid dimensions")
		}
	}
}
