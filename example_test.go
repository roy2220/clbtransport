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

func ExampleTransport() {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("hello"))
	})

	s := httptest.NewServer(h)
	defer s.Close()

	t := NewTransport(TransportConfig{
		ApproximateMaxConnAge:       10 * time.Minute, // default
		ApproximateMaxConnAgeJitter: 0.5,              // default
		HotConnsPerHost:             3,                // default

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

	c := http.Client{
		Transport: t,
	}

	for range 10 {
		req, err := http.NewRequest(http.MethodGet, s.URL, nil)
		if err != nil {
			panic(err)
		}
		resp, err := c.Do(req)
		if err != nil {
			panic(err)
		}
		defer resp.Body.Close()
		data, _ := io.ReadAll(resp.Body)
		fmt.Println(string(data))
	}

	// Output:
	// hello
	// hello
	// hello
	// hello
	// hello
	// hello
	// hello
	// hello
	// hello
	// hello
}
