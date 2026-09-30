// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0

// Command grainlift-hello-world-go serves the hello-world ADBC service on
// loopback; run it with -help for hosting options.
//
// The service is read-only, so it accepts anonymous clients by default; pass
// --auth token to require a bearer token. The worker itself lives in package
// hello.
package main

import (
	"context"
	"fmt"
	"github.com/Query-farm/grainlift-go/cli"
	"github.com/Query-farm/grainlift-hello-world-go/hello"
	"os"
)

func main() {
	err := cli.Run(context.Background(), hello.TargetName, hello.Target(), cli.Options{
		Description: "Grainlift hello-world ADBC service",
		Auth:        "anonymous",
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
}
