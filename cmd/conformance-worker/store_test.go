// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0
package main

import (
	"bytes"
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func storeRows(t *testing.T, count, size int) arrow.RecordBatch {
	t.Helper()
	numbers := array.NewInt64Builder(memory.DefaultAllocator)
	defer numbers.Release()
	payloads := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
	defer payloads.Release()
	for i := 0; i < count; i++ {
		numbers.Append(int64(i))
		payloads.Append(bytes.Repeat([]byte{byte(i)}, size))
	}
	a, b := numbers.NewArray(), payloads.NewArray()
	defer a.Release()
	defer b.Release()
	return array.NewRecordBatch(storeSchema, []arrow.Array{a, b}, int64(count))
}

// STORED returns exactly the stored rows, in batches under the chunk limit
// (one row when a row alone is larger).
func TestStoredRowsAreChunkedAndExact(t *testing.T) {
	s := &store{chunkBytes: 200 << 10}
	defer s.close()
	statement := &storeStatement{statement: &statement{}, store: s}
	statement.sql = "STORE"
	small, large := storeRows(t, 48, 64<<10), storeRows(t, 2, 300<<10)
	defer small.Release()
	defer large.Release()
	reader, e := array.NewRecordReader(storeSchema, []arrow.RecordBatch{small, large})
	if e != nil {
		t.Fatal(e)
	}
	if e := statement.BindStream(context.Background(), reader); e != nil {
		t.Fatal(e)
	}
	reader.Release()
	rows, e := statement.ExecuteUpdate(context.Background())
	if e != nil || *rows != 50 {
		t.Fatalf("stored %v rows: %v", rows, e)
	}
	_, batches, e := s.snapshot()
	if e != nil {
		t.Fatal(e)
	}
	var got [][]byte
	for _, b := range batches {
		if b.NumRows() > 1 && rowBytes(b, 0, b.NumRows()) > int64(s.chunkBytes) {
			t.Fatalf("a %d-row batch exceeds the chunk limit", b.NumRows())
		}
		column := b.Column(1).(*array.Binary)
		for i := 0; i < column.Len(); i++ {
			got = append(got, bytes.Clone(column.Value(i)))
		}
		b.Release()
	}
	if len(got) != 50 || len(got[0]) != 64<<10 || len(got[49]) != 300<<10 || got[49][0] != 1 {
		t.Fatalf("stored rows changed: %d rows", len(got))
	}
}

func TestStoreNeedsBoundParameters(t *testing.T) {
	statement := &storeStatement{statement: &statement{}, store: &store{chunkBytes: 1 << 20}}
	statement.sql = "STORE"
	if _, e := statement.ExecuteUpdate(context.Background()); e == nil {
		t.Fatal("STORE without parameters succeeded")
	}
}

func TestStorageLimits(t *testing.T) {
	limits, options, e := storageLimits(1<<20, "", "", "auto", "", 1<<20)
	if e != nil || options.ExternalStorage != nil || limits.RequestBytes != 1<<20 || limits.BatchBytes >= limits.RequestBytes {
		t.Fatalf("1 MiB without storage: %+v %v", limits, e)
	}
	limits, options, e = storageLimits(1<<20, "http://127.0.0.1:9", "b", "us-east-1", "p/", 1<<20)
	if e != nil || options.ExternalStorage == nil || limits.BatchBytes <= limits.RequestBytes {
		t.Fatalf("1 MiB with storage: %+v %v", limits, e)
	}
	if _, _, e := storageLimits(0, "http://127.0.0.1:9", "", "auto", "", 1<<20); e == nil {
		t.Fatal("accepted an endpoint without a bucket")
	}
}
