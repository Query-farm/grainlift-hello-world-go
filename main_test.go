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
func TestWorkloadLimits(t *testing.T) {
	for _, w := range []workload{{0, 1, 1}, {1, 0, 1}, {1, 4097, 1}, {1, 1, 1025}, {1, 4096, 1024}} {
		if w.validate() == nil {
			t.Fatal("invalid dimensions")
		}
	}
}
