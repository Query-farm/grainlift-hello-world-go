// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0

// Unit tests for the hello-world worker, called in-process without a transport.
package hello

import (
	"context"
	"errors"
	"fmt"
	grainlift "github.com/Query-farm/grainlift-go"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"reflect"
	"testing"
)

var ctx = context.Background()

// statement creates a statement holding the given query.
func statement(t *testing.T, sql string) grainlift.Statement {
	t.Helper()
	created, err := (&Connection{}).NewStatement(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := created.SetSQLQuery(ctx, sql); err != nil {
		t.Fatal(err)
	}
	return created
}

// execute runs a query the way the service does and collects each batch's column values.
func execute(t *testing.T, sql string) (*grainlift.QueryResult, [][][]int64) {
	t.Helper()
	result, err := statement(t, sql).Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer result.Reader.Release()
	var batches [][][]int64
	for result.Reader.Next() {
		var columns [][]int64
		for _, column := range result.Reader.RecordBatch().Columns() {
			columns = append(columns, append([]int64{}, column.(*array.Int64).Int64Values()...))
		}
		batches = append(batches, columns)
	}
	if err := result.Reader.Err(); err != nil {
		t.Fatal(err)
	}
	return result, batches
}

func status(err error) string {
	var adbc *grainlift.Error
	if errors.As(err, &adbc) {
		return adbc.Status
	}
	return ""
}

func TestNumbers(t *testing.T) {
	for _, count := range []int64{0, 1, 1023, 1024, 1025, 100000} {
		result, batches := execute(t, fmt.Sprintf("SELECT * FROM numbers(%d)", count))
		if result.Producer != nil || !result.Reader.Schema().Equal(NumbersSchema) {
			t.Fatal("numbers is a plain reader")
		}
		var next int64
		for _, batch := range batches {
			if len(batch[0]) > BatchRows {
				t.Fatalf("numbers(%d): batch of %d rows", count, len(batch[0]))
			}
			for _, number := range batch[0] {
				if number != next {
					t.Fatalf("numbers(%d): row %d is %d", count, next, number)
				}
				next++
			}
		}
		if next != count {
			t.Fatalf("numbers(%d) returned %d rows", count, next)
		}
	}
}

func TestInvalidQuery(t *testing.T) {
	for _, sql := range []string{"SELECT * FROM numbers(100001)", "SELECT * FROM numbers(-1)", "SELECT * FROM running_total(100001)", "DROP TABLE x"} {
		if err := statement(t, sql).Prepare(ctx); status(err) != "invalid_arguments" || err.(*grainlift.Error).SQLState != "42000" {
			t.Errorf("prepare %q: %v", sql, err)
		}
		if _, err := statement(t, sql).Execute(ctx); status(err) != "invalid_arguments" {
			t.Errorf("execute %q: %v", sql, err)
		}
	}
}

func TestPrepareAndSchemaWithoutExecution(t *testing.T) {
	for sql, columns := range map[string][]string{
		"SELECT 'Hello, world!' AS message;": {"message"},
		"select * from NUMBERS(5)":           {"number"},
		"  SELECT * FROM running_total(5) ":  {"number", "total"},
	} {
		prepared := statement(t, sql)
		if err := prepared.Prepare(ctx); err != nil {
			t.Fatal(err)
		}
		parameters, err := prepared.GetParameterSchema(ctx)
		if err != nil || parameters.NumFields() != 0 {
			t.Fatalf("%q parameters: %v %v", sql, parameters, err)
		}
		schema, err := prepared.ExecuteSchema(ctx)
		if err != nil || !reflect.DeepEqual(names(schema), columns) {
			t.Fatalf("%q schema: %v %v", sql, schema, err)
		}
		result, err := prepared.Execute(ctx)
		if err != nil || !reflect.DeepEqual(names(result.Reader.Schema()), columns) {
			t.Fatalf("%q result: %v", sql, err)
		}
		result.Reader.Release()
	}
}

func names(schema *arrow.Schema) []string {
	var out []string
	for _, field := range schema.Fields() {
		out = append(out, field.Name)
	}
	return out
}

func TestStatementWithoutQueryIsInvalidState(t *testing.T) {
	created, _ := (&Connection{}).NewStatement(ctx)
	if _, err := created.Execute(ctx); status(err) != "invalid_state" {
		t.Fatal(err)
	}
}

func TestRunningTotal(t *testing.T) {
	for _, count := range []int64{0, 1, 1024, 2500} {
		result, batches := execute(t, fmt.Sprintf("SELECT * FROM running_total(%d)", count))
		if result.Producer == nil {
			t.Fatal("running_total is not a producer result")
		}
		var next int64
		for _, batch := range batches {
			if len(batch[0]) > BatchRows {
				t.Fatalf("running_total(%d): batch of %d rows", count, len(batch[0]))
			}
			for i, number := range batch[0] {
				if number != next || batch[1][i] != next*(next+1)/2 {
					t.Fatalf("running_total(%d): row %d is (%d, %d)", count, next, number, batch[1][i])
				}
				next++
			}
		}
		if next != count {
			t.Fatalf("running_total(%d) returned %d rows", count, next)
		}
	}
}

func TestRunningTotalStateRoundTrips(t *testing.T) {
	producer := &RunningTotal{Count: 3000}
	first, err := producer.Produce(ctx)
	if err != nil || first == nil {
		t.Fatal(err)
	}
	first.Release()
	encoded, err := grainlift.EncodeResultProducer(producer)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := grainlift.DecodeResultProducer(encoded)
	if err != nil {
		t.Fatal(err)
	}
	resumed := decoded.(*RunningTotal)
	if *resumed != *producer {
		t.Fatalf("resumed %+v, want %+v", resumed, producer)
	}
	want, _ := producer.Produce(ctx)
	got, _ := resumed.Produce(ctx)
	defer want.Release()
	defer got.Release()
	if !array.RecordEqual(got, want) {
		t.Fatal("resumed producer diverged")
	}
}
