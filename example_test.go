package clbtransport_test

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"time"

	. "github.com/roy2220/clbtransport"
)

func Example() {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("hello"))
	})

	server := httptest.NewServer(handler)
	defer server.Close()

	transport := NewTransport(TransportConfig{
		ApproximateMaxConnAge:       10 * time.Minute, // default
		ApproximateMaxConnAgeJitter: 0.5,              // default
		HotConnsPerHost:             3,                // default

		// Options below are forwarded to the underlying `http.Transport`:
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConnsPerHost:   100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	})

	client := http.Client{
		Transport: transport,
	}
	defer client.CloseIdleConnections()

	req, err := http.NewRequest(http.MethodGet, server.URL, nil)
	if err != nil {
		panic(err)
	}

	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}

	fmt.Println(string(data))
	// Output:
	// hello
}
