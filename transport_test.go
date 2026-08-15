package clbtransport_test

import (
	"context"
	"crypto/tls"
	"math"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	. "github.com/roy2220/clbtransport"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockRoundTripper struct {
	RoundTripFunc            func(*http.Request) (*http.Response, error)
	CloseIdleConnectionsFunc func()
}

var _ http.RoundTripper = (*mockRoundTripper)(nil)

func (t *mockRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if f := t.RoundTripFunc; f != nil {
		return f(req)
	}
	return nil, nil
}

func (t *mockRoundTripper) CloseIdleConnections() {
	if f := t.CloseIdleConnectionsFunc; f != nil {
		f()
	}
}

type mockSource struct {
	Float64 func() float64
}

var _ rand.Source = (*mockSource)(nil)

func (s *mockSource) Uint64() uint64 {
	return uint64(float64(1<<53) * s.Float64())
}

func TestNewTransport(t *testing.T) {
	{
		var underlyingTransport *http.Transport

		transportConfig := TransportConfig{
			Proxy: func(*http.Request) (_ *url.URL, _ error) { return },
			OnProxyConnectResponse: func(ctx context.Context, proxyURL *url.URL, connectReq *http.Request, connectRes *http.Response) (_ error) {
				return
			},
			DialContext:            func(ctx context.Context, network, addr string) (_ net.Conn, _ error) { return },
			Dial:                   func(network, addr string) (_ net.Conn, _ error) { return },
			DialTLSContext:         func(ctx context.Context, network, addr string) (_ net.Conn, _ error) { return },
			DialTLS:                func(network, addr string) (_ net.Conn, _ error) { return },
			TLSClientConfig:        new(tls.Config),
			TLSHandshakeTimeout:    123,
			DisableKeepAlives:      true,
			DisableCompression:     true,
			MaxIdleConns:           111,
			MaxIdleConnsPerHost:    222,
			MaxConnsPerHost:        333,
			IdleConnTimeout:        234,
			ResponseHeaderTimeout:  345,
			ExpectContinueTimeout:  456,
			TLSNextProto:           map[string]func(authority string, c *tls.Conn) http.RoundTripper{},
			ProxyConnectHeader:     make(http.Header),
			GetProxyConnectHeader:  func(ctx context.Context, proxyURL *url.URL, target string) (_ http.Header, _ error) { return },
			MaxResponseHeaderBytes: 1000,
			WriteBufferSize:        1001,
			ReadBufferSize:         1002,
			ForceAttemptHTTP2:      true,
			HTTP2:                  new(http.HTTP2Config),
			Protocols:              new(http.Protocols),

			Clock: clock.NewMock(),
			RandFactory: func() *rand.Rand {
				return rand.New(&mockSource{Float64: func() float64 { return 0.8 }})
			},
			RoundTripperFactory: func(transport *http.Transport) http.RoundTripper {
				return &mockRoundTripper{
					RoundTripFunc: func(r *http.Request) (*http.Response, error) {
						underlyingTransport = transport
						return &http.Response{}, nil
					},
				}
			},
		}

		transport := NewTransport(transportConfig)
		transport.RoundTrip(&http.Request{})
		require.NotNil(t, underlyingTransport)

		assert.NotNil(t, underlyingTransport.Proxy)
		assert.NotNil(t, underlyingTransport.OnProxyConnectResponse)
		assert.NotNil(t, underlyingTransport.DialContext)
		assert.NotNil(t, underlyingTransport.Dial)
		assert.NotNil(t, underlyingTransport.DialTLSContext)
		assert.NotNil(t, underlyingTransport.DialTLS)
		assert.NotNil(t, underlyingTransport.TLSClientConfig)
		assert.Equal(t, transportConfig.TLSHandshakeTimeout, underlyingTransport.TLSHandshakeTimeout)
		assert.Equal(t, transportConfig.DisableKeepAlives, underlyingTransport.DisableKeepAlives)
		assert.Equal(t, transportConfig.DisableCompression, underlyingTransport.DisableCompression)
		assert.Equal(t, int(math.Ceil(float64(transportConfig.MaxIdleConns)/float64(DefaultHotConnsPerHost))), underlyingTransport.MaxIdleConns)
		assert.Equal(t, int(math.Ceil(float64(transportConfig.MaxIdleConnsPerHost)/float64(DefaultHotConnsPerHost))), underlyingTransport.MaxIdleConnsPerHost)
		assert.Equal(t, int(math.Ceil(float64(transportConfig.MaxConnsPerHost)/float64(DefaultHotConnsPerHost))), underlyingTransport.MaxConnsPerHost)
		assert.Equal(t, transportConfig.IdleConnTimeout, underlyingTransport.IdleConnTimeout)
		assert.Equal(t, transportConfig.ResponseHeaderTimeout, underlyingTransport.ResponseHeaderTimeout)
		assert.Equal(t, transportConfig.ExpectContinueTimeout, underlyingTransport.ExpectContinueTimeout)
		assert.NotNil(t, underlyingTransport.TLSNextProto)
		assert.NotNil(t, underlyingTransport.ProxyConnectHeader)
		assert.NotNil(t, underlyingTransport.GetProxyConnectHeader)
		assert.Equal(t, transportConfig.MaxResponseHeaderBytes, underlyingTransport.MaxResponseHeaderBytes)
		assert.Equal(t, transportConfig.WriteBufferSize, underlyingTransport.WriteBufferSize)
		assert.Equal(t, transportConfig.ReadBufferSize, underlyingTransport.ReadBufferSize)
		assert.Equal(t, transportConfig.ForceAttemptHTTP2, underlyingTransport.ForceAttemptHTTP2)
		assert.NotNil(t, underlyingTransport.HTTP2)
		assert.NotNil(t, underlyingTransport.Protocols)

		assert.Equal(t, TransportStats{
			Subs: []*SubTransportStats{
				{
					MaxAge:   "7m0s",
					RefCount: 1,
				},
				nil,
				nil,
			},
			NextSubIndex: 1,
		}, transport.Stats())
	}

	{
		var underlyingTransport *http.Transport

		transportConfig := TransportConfig{
			ApproximateMaxConnAge:       16 * time.Minute,
			ApproximateMaxConnAgeJitter: 0.8,
			HotConnsPerHost:             5,

			MaxIdleConns: 111,
			// MaxIdleConnsPerHost: 222,
			MaxConnsPerHost: 333,

			Clock: clock.NewMock(),
			RandFactory: func() *rand.Rand {
				return rand.New(&mockSource{Float64: func() float64 { return 0.2 }})
			},
			RoundTripperFactory: func(transport *http.Transport) http.RoundTripper {
				return &mockRoundTripper{
					RoundTripFunc: func(r *http.Request) (*http.Response, error) {
						underlyingTransport = transport
						return &http.Response{}, nil
					},
				}
			},
		}

		transport := NewTransport(transportConfig)
		transport.RoundTrip(&http.Request{})
		require.NotNil(t, underlyingTransport)

		assert.Equal(t, int(math.Ceil(float64(transportConfig.MaxIdleConns)/float64(transportConfig.HotConnsPerHost))), underlyingTransport.MaxIdleConns)
		assert.Equal(t, int(math.Ceil(float64(http.DefaultMaxIdleConnsPerHost)/float64(transportConfig.HotConnsPerHost))), underlyingTransport.MaxIdleConnsPerHost)
		assert.Equal(t, int(math.Ceil(float64(transportConfig.MaxConnsPerHost)/float64(transportConfig.HotConnsPerHost))), underlyingTransport.MaxConnsPerHost)

		assert.Equal(t, TransportStats{
			Subs: []*SubTransportStats{
				{
					MaxAge:   "23m40.8s",
					RefCount: 1,
				},
				nil,
				nil,
				nil,
				nil,
			},
			NextSubIndex: 1,
		}, transport.Stats())
	}
}

func TestTransport_RoundTrip(t *testing.T) {
	var (
		inChs          = make([]chan struct{}, 3)
		outChs         = make([]chan struct{}, 3)
		chIdx          = 0
		randFloat64Cnt = 0
	)
	for i := range inChs {
		inChs[i] = make(chan struct{})
		outChs[i] = make(chan struct{})
	}

	transportConfig := TransportConfig{
		HotConnsPerHost: 3,

		MaxIdleConns:        111,
		MaxIdleConnsPerHost: 222,
		MaxConnsPerHost:     333,

		Clock: clock.NewMock(),
		RandFactory: func() *rand.Rand {
			return rand.New(&mockSource{Float64: func() float64 {
				randFloat64Cnt++
				switch randFloat64Cnt {
				case 1:
					return 0.2
				case 2:
					return 0.4
				case 3:
					return 0.6
				default:
					return 0.8
				}
			}})
		},
		RoundTripperFactory: func(transport *http.Transport) http.RoundTripper {
			i := chIdx
			chIdx = (chIdx + 1) % len(inChs)

			return &mockRoundTripper{
				RoundTripFunc: func(r *http.Request) (*http.Response, error) {
					inChs[i] <- struct{}{}
					<-outChs[i]
					return &http.Response{Proto: "HTTP/test"}, nil
				},
			}
		},
	}

	transport := NewTransport(transportConfig)
	go func() {
		<-inChs[0]
		<-inChs[1]
		<-inChs[2]
		<-inChs[0]
		assert.Equal(t, TransportStats{
			Subs: []*SubTransportStats{
				{
					MaxAge:   "13m0s",
					RefCount: 3,
				},
				{
					MaxAge:   "11m0s",
					RefCount: 2,
				},
				{
					MaxAge:   "9m0s",
					RefCount: 2,
				},
			},
			NextSubIndex: 1,
		}, transport.Stats())
		outChs[0] <- struct{}{}
		outChs[1] <- struct{}{}
		outChs[2] <- struct{}{}
		outChs[0] <- struct{}{}
	}()
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			resp, err := transport.RoundTrip(&http.Request{})
			require.NoError(t, err)
			assert.Equal(t, "HTTP/test", resp.Proto)
		})
	}
	wg.Wait()

	assert.Equal(t, TransportStats{
		Subs: []*SubTransportStats{
			{
				MaxAge:   "13m0s",
				RefCount: 1,
			},
			{
				MaxAge:   "11m0s",
				RefCount: 1,
			},
			{
				MaxAge:   "9m0s",
				RefCount: 1,
			},
		},
		NextSubIndex: 1,
	}, transport.Stats())
}

func TestTransport_EvictSub(t *testing.T) {
	var (
		inChs                   = make([]chan struct{}, 3)
		outChs                  = make([]chan struct{}, 3)
		chIdx                   = 0
		randFloat64Cnt          = 0
		closeIdleConnectionsCnt atomic.Int64
	)
	for i := range inChs {
		inChs[i] = make(chan struct{})
		outChs[i] = make(chan struct{})
	}

	transportConfig := TransportConfig{
		HotConnsPerHost: 3,

		MaxIdleConns:        111,
		MaxIdleConnsPerHost: 222,
		MaxConnsPerHost:     333,

		Clock: clock.NewMock(),
		RandFactory: func() *rand.Rand {
			return rand.New(&mockSource{Float64: func() float64 {
				randFloat64Cnt++
				switch randFloat64Cnt {
				case 1:
					return 0.2
				case 2:
					return 0.4
				case 3:
					return 0.6
				default:
					return 0.8
				}
			}})
		},
		RoundTripperFactory: func(transport *http.Transport) http.RoundTripper {
			i := chIdx
			chIdx = (chIdx + 1) % len(inChs)

			return &mockRoundTripper{
				RoundTripFunc: func(r *http.Request) (*http.Response, error) {
					inChs[i] <- struct{}{}
					<-outChs[i]
					return &http.Response{Proto: "HTTP/test"}, nil
				},
				CloseIdleConnectionsFunc: func() {
					closeIdleConnectionsCnt.Add(1)
				},
			}
		},
	}

	transport := NewTransport(transportConfig)
	go func() {
		<-inChs[0]
		<-inChs[1]
		<-inChs[2]
		<-inChs[0]
		<-inChs[1]
		<-inChs[2]
		assert.Equal(t, TransportStats{
			Subs: []*SubTransportStats{
				{
					MaxAge:   "13m0s",
					RefCount: 3,
				},
				{
					MaxAge:   "11m0s",
					RefCount: 3,
				},
				{
					MaxAge:   "9m0s",
					RefCount: 3,
				},
			},
			NextSubIndex: 0,
		}, transport.Stats())

		transportConfig.Clock.(*clock.Mock).Add(10 * time.Minute)

		assert.Equal(t, TransportStats{
			Subs: []*SubTransportStats{
				{
					MaxAge:   "13m0s",
					RefCount: 3,
				},
				{
					MaxAge:   "11m0s",
					RefCount: 3,
				},
				nil,
			},
			NextSubIndex: 0,
		}, transport.Stats())

		assert.Equal(t, int64(0), closeIdleConnectionsCnt.Load())

		outChs[0] <- struct{}{}
		outChs[1] <- struct{}{}
		outChs[2] <- struct{}{}
		outChs[0] <- struct{}{}
		outChs[1] <- struct{}{}
		outChs[2] <- struct{}{}
	}()
	var wg sync.WaitGroup
	for range 6 {
		wg.Go(func() {
			resp, err := transport.RoundTrip(&http.Request{})
			require.NoError(t, err)
			assert.Equal(t, "HTTP/test", resp.Proto)
		})
	}
	wg.Wait()

	assert.Equal(t, TransportStats{
		Subs: []*SubTransportStats{
			{
				MaxAge:   "13m0s",
				RefCount: 1,
			},
			{
				MaxAge:   "11m0s",
				RefCount: 1,
			},
			nil,
		},
		NextSubIndex: 0,
	}, transport.Stats())

	assert.Equal(t, int64(1), closeIdleConnectionsCnt.Load())

	transportConfig.Clock.(*clock.Mock).Add(2 * time.Minute)

	assert.Equal(t, TransportStats{
		Subs: []*SubTransportStats{
			{
				MaxAge:   "13m0s",
				RefCount: 1,
			},
			nil,
			nil,
		},
		NextSubIndex: 0,
	}, transport.Stats())

	assert.Equal(t, int64(2), closeIdleConnectionsCnt.Load())
}

func TestTransport_CloseIdleConnections(t *testing.T) {
	var (
		inChs                   = make([]chan struct{}, 3)
		outChs                  = make([]chan struct{}, 3)
		chIdx                   = 0
		randFloat64Cnt          = 0
		closeIdleConnectionsCnt atomic.Int64
	)
	for i := range inChs {
		inChs[i] = make(chan struct{})
		outChs[i] = make(chan struct{})
	}

	transportConfig := TransportConfig{
		HotConnsPerHost: 3,

		MaxIdleConns:        111,
		MaxIdleConnsPerHost: 222,
		MaxConnsPerHost:     333,

		Clock: clock.NewMock(),
		RandFactory: func() *rand.Rand {
			return rand.New(&mockSource{Float64: func() float64 {
				randFloat64Cnt++
				switch randFloat64Cnt {
				case 1:
					return 0.2
				case 2:
					return 0.4
				case 3:
					return 0.6
				default:
					return 0.8
				}
			}})
		},
		RoundTripperFactory: func(transport *http.Transport) http.RoundTripper {
			i := chIdx
			chIdx = (chIdx + 1) % len(inChs)

			return &mockRoundTripper{
				RoundTripFunc: func(r *http.Request) (*http.Response, error) {
					inChs[i] <- struct{}{}
					<-outChs[i]
					return &http.Response{Proto: "HTTP/test"}, nil
				},
				CloseIdleConnectionsFunc: func() {
					closeIdleConnectionsCnt.Add(1)
				},
			}
		},
	}

	transport := NewTransport(transportConfig)
	go func() {
		<-inChs[0]
		<-inChs[1]
		<-inChs[2]
		assert.Equal(t, TransportStats{
			Subs: []*SubTransportStats{
				{
					MaxAge:   "13m0s",
					RefCount: 2,
				},
				{
					MaxAge:   "11m0s",
					RefCount: 2,
				},
				{
					MaxAge:   "9m0s",
					RefCount: 2,
				},
			},
			NextSubIndex: 0,
		}, transport.Stats())

		transport.CloseIdleConnections()

		assert.Equal(t, TransportStats{
			Subs: []*SubTransportStats{
				nil,
				nil,
				nil,
			},
			NextSubIndex: 0,
		}, transport.Stats())

		assert.Equal(t, int64(0), closeIdleConnectionsCnt.Load())

		outChs[0] <- struct{}{}
		outChs[1] <- struct{}{}
		outChs[2] <- struct{}{}
	}()
	var wg sync.WaitGroup
	for range 3 {
		wg.Go(func() {
			resp, err := transport.RoundTrip(&http.Request{})
			require.NoError(t, err)
			assert.Equal(t, "HTTP/test", resp.Proto)
		})
	}
	wg.Wait()

	assert.Equal(t, TransportStats{
		Subs: []*SubTransportStats{
			nil,
			nil,
			nil,
		},
		NextSubIndex: 0,
	}, transport.Stats())

	assert.Equal(t, int64(3), closeIdleConnectionsCnt.Load())

	go func() {
		<-inChs[0]
		outChs[0] <- struct{}{}
	}()
	resp, err := transport.RoundTrip(&http.Request{})
	require.NoError(t, err)
	assert.Equal(t, "HTTP/test", resp.Proto)

	assert.Equal(t, TransportStats{
		Subs: []*SubTransportStats{
			{
				MaxAge:   "7m0s",
				RefCount: 1,
			},
			nil,
			nil,
		},
		NextSubIndex: 1,
	}, transport.Stats())

	transport.CloseIdleConnections()

	assert.Equal(t, TransportStats{
		Subs: []*SubTransportStats{
			nil,
			nil,
			nil,
		},
		NextSubIndex: 1,
	}, transport.Stats())

	assert.Equal(t, int64(4), closeIdleConnectionsCnt.Load())
}
