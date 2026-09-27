// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0
package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	grainlift "github.com/Query-farm/grainlift-go"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

type hosted struct {
	ready map[string]any
	done  chan error
	close func(context.Context) error
}

func loadTLS(directory string) (*tls.Config, error) {
	certificate, e := tls.LoadX509KeyPair(filepath.Join(directory, "server.pem"), filepath.Join(directory, "server-key.pem"))
	if e != nil {
		return nil, errors.New("invalid server certificate")
	}
	ca, e := os.ReadFile(filepath.Join(directory, "ca.pem"))
	if e != nil {
		return nil, errors.New("missing trust roots")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return nil, errors.New("invalid trust roots")
	}
	return &tls.Config{Certificates: []tls.Certificate{certificate}, ClientCAs: roots, MinVersion: tls.VersionTLS12}, nil
}
func clientPrincipal(state tls.ConnectionState) (string, error) {
	if len(state.VerifiedChains) == 0 || len(state.PeerCertificates) == 0 {
		return "", errors.New("unverified client")
	}
	uris := state.PeerCertificates[0].URIs
	if len(uris) != 1 {
		return "", errors.New("exactly one client identity required")
	}
	switch uris[0].String() {
	case "spiffe://benchmark.test/client":
		return "load-principal", nil
	case "spiffe://benchmark.test/other":
		return "other-principal", nil
	}
	return "", errors.New("unauthorized certificate identity")
}
func iroPeers() map[string]bool {
	peers := map[string]bool{}
	for _, name := range []string{"GRAINLIFT_HELLO_IROH_CLIENT_ID", "GRAINLIFT_HELLO_IROH_OTHER_CLIENT_ID"} {
		if value := os.Getenv(name); value != "" {
			peers[value] = true
		}
	}
	return peers
}
func startHost(svc *grainlift.Service, mode, tlsDir string, port int, auth vgirpc.AuthenticateFunc) (*hosted, error) {
	if mode == "iroh" {
		return startIroh(svc)
	}
	listener, e := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if e != nil {
		return nil, e
	}
	h := &hosted{ready: map[string]any{"endpoint": mode + "://" + listener.Addr().String(), "sample_pid": os.Getpid()}, done: make(chan error, 1)}
	var tlsConfig *tls.Config
	if mode == "https" || mode == "mtls" {
		tlsConfig, e = loadTLS(tlsDir)
		if e != nil {
			_ = listener.Close()
			return nil, e
		}
	}
	switch mode {
	case "http", "https":
		server := &http.Server{Handler: svc.HTTPHandler(auth), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10}
		if mode == "https" {
			listener = tls.NewListener(listener, tlsConfig)
		}
		go func() { h.done <- server.Serve(listener) }()
		h.close = server.Shutdown
	case "tcp", "mtls":
		options := grainlift.StreamOptions{Mode: mode, LocalPrincipal: "load-principal", TLSConfig: tlsConfig, AuthenticateTLS: clientPrincipal}
		server, e := svc.ServeStreams(listener, options)
		if e != nil {
			_ = listener.Close()
			return nil, e
		}
		if mode == "mtls" {
			h.ready["endpoint"] = "tls+tcp://" + listener.Addr().String()
		}
		h.close = server.Close
	default:
		_ = listener.Close()
		return nil, errors.New("unknown transport")
	}
	return h, nil
}
func startIroh(svc *grainlift.Service) (*hosted, error) {
	bridge := os.Getenv("GRAINLIFT_IROH_BRIDGE")
	if bridge == "" || !filepath.IsAbs(bridge) {
		return nil, errors.New("explicit absolute Iroh bridge path required")
	}
	peers := iroPeers()
	if len(peers) == 0 {
		return nil, errors.New("Iroh peer allowlist required")
	}
	directory, e := os.MkdirTemp("", "grainlift-iroh-")
	if e != nil {
		return nil, e
	}
	socket := filepath.Join(directory, "worker.sock")
	listener, e := net.Listen("unix", socket)
	if e != nil {
		_ = os.RemoveAll(directory)
		return nil, e
	}
	if e = os.Chmod(socket, 0600); e != nil {
		_ = listener.Close()
		_ = os.RemoveAll(directory)
		return nil, e
	}
	server, e := svc.ServeStreams(listener, grainlift.StreamOptions{Mode: "iroh-bridge", AuthorizeIroh: func(id string) bool { return peers[id] }})
	if e != nil {
		_ = listener.Close()
		_ = os.RemoveAll(directory)
		return nil, e
	}
	command := exec.Command(bridge, "--ephemeral", "--no-relay", "--discovery-json", "--raw-upstream", "unix://"+socket, "--raw-max-connections", "64", "--raw-max-streams", "256", "--raw-max-streams-per-connection", "32", "--raw-drain-timeout", "3")
	// Keep transport child configuration explicit and avoid forwarding credentials.
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, "GRAINLIFT_") && !strings.HasPrefix(entry, "VGI_IROH_SECRET_KEY=") && !strings.HasPrefix(entry, "RUST_LOG=") {
			command.Env = append(command.Env, entry)
		}
	}
	command.Env = append(command.Env, "RUST_LOG=off")
	command.Stderr = io.Discard
	stdout, e := command.StdoutPipe()
	if e != nil {
		_ = server.Close(context.Background())
		_ = os.RemoveAll(directory)
		return nil, e
	}
	if e = command.Start(); e != nil {
		_ = server.Close(context.Background())
		_ = os.RemoveAll(directory)
		return nil, e
	}
	exited := make(chan struct{})
	done := make(chan error, 1)
	go func() { e := command.Wait(); close(exited); done <- e }()
	var closeOnce sync.Once
	closeHost := func(ctx context.Context) error {
		closeOnce.Do(func() { _ = command.Process.Signal(syscall.SIGTERM) })
		select {
		case <-exited:
		case <-ctx.Done():
			_ = command.Process.Kill()
			<-exited
		}
		e := server.Close(ctx)
		_ = os.RemoveAll(directory)
		return e
	}
	discovered := make(chan []byte, 1)
	go func() {
		reader := bufio.NewReader(stdout)
		line, _ := bufio.NewReader(io.LimitReader(reader, 65536)).ReadBytes('\n')
		discovered <- line
		_, _ = io.Copy(io.Discard, reader)
	}()
	var line []byte
	select {
	case line = <-discovered:
	case <-time.After(10 * time.Second):
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = closeHost(ctx)
		return nil, errors.New("Iroh bridge startup timeout")
	}
	var discovery struct {
		EndpointID      string   `json:"endpoint_id"`
		DirectAddresses []string `json:"direct_addresses"`
	}
	if json.Unmarshal(line, &discovery) != nil || len(discovery.EndpointID) != 64 || len(discovery.DirectAddresses) == 0 {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = closeHost(ctx)
		return nil, errors.New("invalid bridge discovery")
	}
	direct := discovery.DirectAddresses[0]
	for _, address := range discovery.DirectAddresses {
		if strings.HasPrefix(address, "127.0.0.1:") {
			direct = address
			break
		}
	}
	h := &hosted{ready: map[string]any{"endpoint": "iroh://" + discovery.EndpointID, "endpoint_id": discovery.EndpointID, "direct_address": direct, "direct_addresses": discovery.DirectAddresses, "sample_pid": os.Getpid()}, done: done, close: closeHost}
	return h, nil
}
