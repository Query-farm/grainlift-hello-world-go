// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0

// Command client queries the hello-world service from Go with the ADBC driver
// manager, which loads the native Grainlift driver (cgo required).
//
// Set GRAINLIFT_DRIVER to the native driver library, start the service, then
// run go run ./examples/client. Optionally set GRAINLIFT_ENDPOINT and
// GRAINLIFT_TOKEN (omit it to connect anonymously), or, for a tls+tcp://
// endpoint, GRAINLIFT_TLS_CA, GRAINLIFT_TLS_CERT, GRAINLIFT_TLS_KEY and
// GRAINLIFT_TLS_SERVER_NAME.
package main

import (
	"context"
	"fmt"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"os"
	"strings"
)

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// options builds the driver-manager options from the environment.
func options() (map[string]string, error) {
	driver := os.Getenv("GRAINLIFT_DRIVER")
	if driver == "" {
		return nil, fmt.Errorf("set GRAINLIFT_DRIVER to the native driver library")
	}
	endpoint := os.Getenv("GRAINLIFT_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://127.0.0.1:8080"
	}
	opts := map[string]string{
		"driver":           driver,
		"entrypoint":       "AdbcDriverGrainliftInit",
		"grainlift.uri":    endpoint,
		"grainlift.target": "hello",
	}
	if strings.HasPrefix(endpoint, "tls+tcp://") {
		for option, variable := range map[string]string{
			"grainlift.tls.ca":          "GRAINLIFT_TLS_CA",
			"grainlift.tls.cert":        "GRAINLIFT_TLS_CERT",
			"grainlift.tls.key":         "GRAINLIFT_TLS_KEY",
			"grainlift.tls.server_name": "GRAINLIFT_TLS_SERVER_NAME",
		} {
			if opts[option] = os.Getenv(variable); opts[option] == "" {
				return nil, fmt.Errorf("set %s for a tls+tcp:// endpoint", variable)
			}
		}
	} else if token := os.Getenv("GRAINLIFT_TOKEN"); token != "" {
		opts["grainlift.auth.bearer_token"] = token
	}
	return opts, nil
}

// query executes sql and returns its schema and every Arrow batch; release the batches.
func query(ctx context.Context, connection adbc.Connection, sql string) (*arrow.Schema, []arrow.RecordBatch, error) {
	statement, err := connection.NewStatement()
	if err != nil {
		return nil, nil, err
	}
	defer statement.Close()
	if err := statement.SetSqlQuery(sql); err != nil {
		return nil, nil, err
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
		batches = append(batches, batch)
	}
	return reader.Schema(), batches, reader.Err()
}

func release(batches []arrow.RecordBatch) {
	for _, batch := range batches {
		batch.Release()
	}
}

func run(ctx context.Context) error {
	opts, err := options()
	if err != nil {
		return err
	}
	database, err := drivermgr.Driver{}.NewDatabase(opts)
	if err != nil {
		return err
	}
	defer database.Close()
	connection, err := database.Open(ctx)
	if err != nil {
		return err
	}
	defer connection.Close()

	_, batches, err := query(ctx, connection, "SELECT 'Hello, world!' AS message")
	if err != nil {
		return err
	}
	fmt.Printf("message: %s\n", batches[0].Column(0).(*array.String).Value(0))
	release(batches)

	_, batches, err = query(ctx, connection, "SELECT * FROM numbers(2500)")
	if err != nil {
		return err
	}
	var sizes []int64
	for _, batch := range batches {
		sizes = append(sizes, batch.NumRows())
	}
	release(batches)
	fmt.Printf("numbers(2500): %v rows per Arrow batch\n", sizes)

	_, batches, err = query(ctx, connection, "SELECT * FROM running_total(2500)")
	if err != nil {
		return err
	}
	last, row := batches[len(batches)-1], int(batches[len(batches)-1].NumRows()-1)
	fmt.Printf("running_total(2500): last row {number: %d, total: %d}\n",
		last.Column(0).(*array.Int64).Value(row), last.Column(1).(*array.Int64).Value(row))
	release(batches)

	schema, batches, err := query(ctx, connection, "SELECT * FROM numbers(0)")
	if err != nil {
		return err
	}
	fmt.Printf("Empty result: %d batches, schema: %s\n", len(batches), schema)
	release(batches)
	return nil
}
