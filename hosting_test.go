// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	grainlift "github.com/Query-farm/grainlift-go"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func bridgeFixture(t *testing.T, script string) (*grainlift.Service, string) {
	t.Helper()
	if _, ok := any(&vgirpc.Server{}).(interface {
		ServeNetworkWithContext(context.Context, io.Reader, io.Writer)
	}); !ok {
		if os.Getenv("GRAINLIFT_REQUIRE_NETWORK") == "1" {
			t.Fatal("safe network entrypoint required")
		}
		t.Skip("requires upstream network safety patch")
	}
	// Keep the bridge socket path below the Unix domain socket limit on macOS.
	base := os.TempDir()
	if info, err := os.Stat("/tmp"); err == nil && info.IsDir() {
		base = "/tmp"
	}
	directory, e := os.MkdirTemp(base, "gl-")
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	t.Setenv("TMPDIR", directory)
	bridge := filepath.Join(directory, "bridge")
	if e := os.WriteFile(bridge, []byte("#!/bin/sh\n"+script), 0700); e != nil {
		t.Fatal(e)
	}
	t.Setenv("GRAINLIFT_IROH_BRIDGE", bridge)
	t.Setenv("GRAINLIFT_HELLO_IROH_CLIENT_ID", strings.Repeat("a", 64))
	t.Setenv("RUST_LOG", "debug")
	svc, e := grainlift.NewService(map[string]grainlift.Target{"default": {Backend: &backend{work: workload{1, 1, 1}, counts: &counters{}}, Authorize: func(string) bool { return true }}}, grainlift.DefaultLimits())
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = svc.Close() })
	return svc, directory
}
func assertBridgeDirectoryRemoved(t *testing.T, directory string) {
	t.Helper()
	matches, e := filepath.Glob(filepath.Join(directory, "grainlift-iroh-*"))
	if e != nil || len(matches) != 0 {
		t.Fatal("bridge upstream resources retained", matches, e)
	}
}
func TestBridgeStartupFailureCleanup(t *testing.T) {
	svc, directory := bridgeFixture(t, "printf 'invalid discovery\\n'\nexit 9\n")
	if host, e := startIroh(svc); e == nil {
		_ = host.close(context.Background())
		t.Fatal("invalid discovery accepted")
	}
	assertBridgeDirectoryRemoved(t, directory)
}
func TestBridgeUnexpectedExitAndIdempotentCleanup(t *testing.T) {
	script := "test \"$RUST_LOG\" = off || exit 7\nprintf '%s\\n' '{\"endpoint_id\":\"" + strings.Repeat("b", 64) + "\",\"direct_addresses\":[\"127.0.0.1:9999\"]}'\nsleep 0.05\nexit 9\n"
	svc, directory := bridgeFixture(t, script)
	host, e := startIroh(svc)
	if e != nil {
		t.Fatal(e)
	}
	select {
	case e := <-host.done:
		if e == nil {
			t.Fatal("nonzero bridge exit lost")
		}
	case <-time.After(time.Second):
		t.Fatal("bridge exit not propagated")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if e := host.close(ctx); e != nil {
		t.Fatal(e)
	}
	if e := host.close(ctx); e != nil {
		t.Fatal(e)
	}
	assertBridgeDirectoryRemoved(t, directory)
}
func TestClientPrincipalExactIdentity(t *testing.T) {
	for _, identity := range []string{"spiffe://benchmark.test/client", "spiffe://benchmark.test/other", "spiffe://wrong.test/client", "spiffe://benchmark.test/denied", "spiffe://benchmark.test/client?x=1"} {
		uri, e := url.Parse(identity)
		if e != nil {
			t.Fatal(e)
		}
		certificate := &x509.Certificate{URIs: []*url.URL{uri}}
		principal, e := clientPrincipal(tls.ConnectionState{PeerCertificates: []*x509.Certificate{certificate}, VerifiedChains: [][]*x509.Certificate{{certificate}}})
		allowed := identity == "spiffe://benchmark.test/client" || identity == "spiffe://benchmark.test/other"
		if (e == nil) != allowed {
			t.Fatalf("identity %s principal %s error %v", identity, principal, e)
		}
	}
	if _, e := clientPrincipal(tls.ConnectionState{}); e == nil {
		t.Fatal("unverified identity accepted")
	}
}
