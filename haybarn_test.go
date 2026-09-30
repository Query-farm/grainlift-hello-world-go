// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"github.com/Query-farm/grainlift-hello-world-go/hello"
	"io"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"time"
)

// haybarn finds the Haybarn CLI: $HAYBARN, haybarn on PATH, or uvx haybarn-cli.
func haybarn(t *testing.T) []string {
	t.Helper()
	if command := os.Getenv("HAYBARN"); command != "" {
		return strings.Fields(command)
	}
	if path, err := exec.LookPath("haybarn"); err == nil {
		return []string{path}
	}
	if path, err := exec.LookPath("uvx"); err == nil {
		return []string{path, "haybarn-cli"}
	}
	t.Skip("Install haybarn-cli or uv")
	return nil
}

// TestSQLExampleInHaybarn runs examples/query.sql through adbc_scanner against an anonymous server.
func TestSQLExampleInHaybarn(t *testing.T) {
	driver := driverPath(t)
	command := haybarn(t)
	endpoint, _ := serve(t, hello.Backend{}, nil, "anonymous")
	script, err := os.ReadFile("examples/query.sql")
	if err != nil {
		t.Fatal(err)
	}
	run := exec.Command(command[0], append(command[1:], "-json")...)
	run.Stdin = strings.NewReader(strings.ReplaceAll(string(script), "http://127.0.0.1:8080", endpoint))
	run.Env = append(os.Environ(), "GRAINLIFT_DRIVER="+driver)
	var stdout, stderr bytes.Buffer
	run.Stdout, run.Stderr = &stdout, &stderr
	run.WaitDelay = 5 * time.Second
	if err := run.Start(); err != nil {
		t.Fatal(err)
	}
	timer := time.AfterFunc(2*time.Minute, func() { _ = run.Process.Kill() })
	defer timer.Stop()
	if err := run.Wait(); err != nil {
		t.Fatalf("%v: %s", err, stderr.String())
	}
	// The CLI prints one JSON array per result set.
	var results [][]map[string]any
	decoder := json.NewDecoder(&stdout)
	decoder.UseNumber()
	for {
		var rows []map[string]any
		if err := decoder.Decode(&rows); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		results = append(results, rows)
	}
	if len(results) != 4 {
		t.Fatalf("%d result sets: %v", len(results), results)
	}
	want := [][]map[string]any{
		{{"message": "Hello, world!"}},
		{{"numbers": json.Number("100000"), "total": "4999950000"}}, // HUGEINT sums are emitted as strings.
		{
			{"number": json.Number("2499"), "total": json.Number("3123750")},
			{"number": json.Number("2498"), "total": json.Number("3121251")},
			{"number": json.Number("2497"), "total": json.Number("3118753")},
		},
	}
	if !reflect.DeepEqual(results[:3], want) {
		t.Fatalf("got %v, want %v", results[:3], want)
	}
	if len(results[3]) != 1 || len(results[3][0]) != 1 {
		t.Fatalf("disconnect: %v", results[3])
	}
	for _, disconnected := range results[3][0] {
		if disconnected != true {
			t.Fatalf("disconnect: %v", results[3])
		}
	}
}
