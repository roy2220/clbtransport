//go:build e2e

package clbtransport_test

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/roy2220/clbtransport"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type connCounter struct {
	T           *testing.T
	Dialer      net.Dialer
	OpenCount   atomic.Int64
	CloseCount  atomic.Int64
	ActiveCount atomic.Int64
}

func (c *connCounter) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	conn, err := c.Dialer.DialContext(ctx, network, addr)
	if err != nil {
		return nil, err
	}
	ts := time.Duration(time.Now().UnixNano()).Seconds()
	n := c.OpenCount.Add(1)
	m := c.ActiveCount.Add(1)
	c.T.Logf("%.6f: open_conn_count=%v active_conn_count=%v", ts, n, m)
	return &wrappedConn{
		Conn: conn,

		connCounter: c,
	}, err
}

type wrappedConn struct {
	net.Conn

	isClosed    atomic.Bool
	connCounter *connCounter
}

func (c *wrappedConn) Close() error {
	if c.isClosed.Swap(true) {
		return nil
	}

	ts := time.Duration(time.Now().UnixNano()).Seconds()
	n := c.connCounter.CloseCount.Add(1)
	m := c.connCounter.ActiveCount.Add(-1)
	c.connCounter.T.Logf("%.6f: close_conn_count=%v active_conn_count=%v", ts, n, m)
	return c.Conn.Close()
}

func TestE2E_HTTP1(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("hello"))
	})

	server := httptest.NewServer(handler)
	defer server.Close()

	connCounter := connCounter{T: t}

	transport := NewTransport(TransportConfig{
		ApproximateMaxConnAge:       3 * time.Second,
		ApproximateMaxConnAgeJitter: 0.5,
		HotConnsPerHost:             3,

		DialContext:         connCounter.DialContext,
		MaxIdleConnsPerHost: 100,
	})

	client := http.Client{
		Transport: transport,
	}

	tokens := make(chan struct{}, 10)
	go func() {
		for range 1000 {
			time.Sleep(10 * time.Millisecond)
			tokens <- struct{}{}
		}
		close(tokens)
	}()
	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() {
			for range tokens {
				req, err := http.NewRequest(http.MethodGet, server.URL, nil)
				if err != nil {
					t.Fatal(err)
				}

				resp, err := client.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()

				require.Equal(t, 1, resp.ProtoMajor)
			}
		})
	}
	wg.Wait()

	client.CloseIdleConnections()
	assert.Equal(t, int64(0), connCounter.ActiveCount.Load())
	assert.GreaterOrEqual(t, connCounter.OpenCount.Load(), int64(9))
}

func TestE2E_HTTP2(t *testing.T) {
	var protocols http.Protocols
	protocols.SetUnencryptedHTTP2(true)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("hello"))
	})

	server := httptest.NewUnstartedServer(handler)
	server.Config.Protocols = &protocols
	server.Start()
	defer server.Close()

	connCounter := connCounter{T: t}

	transport := NewTransport(TransportConfig{
		ApproximateMaxConnAge:       3 * time.Second,
		ApproximateMaxConnAgeJitter: 0.5,
		HotConnsPerHost:             3,

		Protocols:           &protocols,
		DialContext:         connCounter.DialContext,
		MaxIdleConnsPerHost: 100,
	})

	client := http.Client{
		Transport: transport,
	}

	tokens := make(chan struct{}, 10)
	go func() {
		for range 1000 {
			time.Sleep(10 * time.Millisecond)
			tokens <- struct{}{}
		}
		close(tokens)
	}()
	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() {
			for range tokens {
				req, err := http.NewRequest(http.MethodGet, server.URL, nil)
				if err != nil {
					t.Fatal(err)
				}

				resp, err := client.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()

				require.Equal(t, 2, resp.ProtoMajor)
			}
		})
	}
	wg.Wait()

	client.CloseIdleConnections()
	assert.Equal(t, int64(0), connCounter.ActiveCount.Load())
	assert.GreaterOrEqual(t, connCounter.OpenCount.Load(), int64(9))
}
