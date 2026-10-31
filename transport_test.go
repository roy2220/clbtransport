package clbtransport_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

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

type mockBody struct {
	ReadFunc  func(p []byte) (n int, err error)
	CloseFunc func() error
}

var _ io.ReadCloser = (*mockBody)(nil)

func (b *mockBody) Read(p []byte) (n int, err error) {
	if f := b.ReadFunc; f != nil {
		return f(p)
	}
	return 0, io.EOF
}

func (b *mockBody) Close() error {
	if f := b.CloseFunc; f != nil {
		return f()
	}
	return nil
}

type nopCloser struct{}

var _ io.Closer = nopCloser{}

func (nopCloser) Close() error { return nil }

func TestNewTransport(t *testing.T) {
	{
		var underlyingTransport *http.Transport

		transportConfig := *(&TransportConfig{
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
			MaxIdleConns:           111 * DefaultHotConnsPerHost,
			MaxIdleConnsPerHost:    222 * DefaultHotConnsPerHost,
			MaxConnsPerHost:        333 * DefaultHotConnsPerHost,
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
		}).SetRandFactory(func() *rand.Rand {
			return rand.New(&mockSource{Float64: func() float64 { return 0.8 }})
		}).SetRoundTripperFactory(func(transport *http.Transport) http.RoundTripper {
			return &mockRoundTripper{
				RoundTripFunc: func(r *http.Request) (*http.Response, error) {
					underlyingTransport = transport
					return &http.Response{Body: http.NoBody}, nil
				},
			}
		})

		transport := NewTransport(transportConfig)
		resp, err := transport.RoundTrip(&http.Request{})
		require.NoError(t, err)
		resp.Body.Close()
		require.NotNil(t, underlyingTransport)

		require.NotNil(t, underlyingTransport.Proxy)
		require.NotNil(t, underlyingTransport.OnProxyConnectResponse)
		require.NotNil(t, underlyingTransport.DialContext)
		require.NotNil(t, underlyingTransport.Dial)
		require.NotNil(t, underlyingTransport.DialTLSContext)
		require.NotNil(t, underlyingTransport.DialTLS)
		require.NotNil(t, underlyingTransport.TLSClientConfig)
		require.Equal(t, transportConfig.TLSHandshakeTimeout, underlyingTransport.TLSHandshakeTimeout)
		require.Equal(t, transportConfig.DisableKeepAlives, underlyingTransport.DisableKeepAlives)
		require.Equal(t, transportConfig.DisableCompression, underlyingTransport.DisableCompression)
		require.Equal(t, transportConfig.MaxIdleConns/DefaultHotConnsPerHost, underlyingTransport.MaxIdleConns)
		require.Equal(t, transportConfig.MaxIdleConnsPerHost/DefaultHotConnsPerHost, underlyingTransport.MaxIdleConnsPerHost)
		require.Equal(t, transportConfig.MaxConnsPerHost/DefaultHotConnsPerHost, underlyingTransport.MaxConnsPerHost)
		require.Equal(t, transportConfig.IdleConnTimeout, underlyingTransport.IdleConnTimeout)
		require.Equal(t, transportConfig.ResponseHeaderTimeout, underlyingTransport.ResponseHeaderTimeout)
		require.Equal(t, transportConfig.ExpectContinueTimeout, underlyingTransport.ExpectContinueTimeout)
		require.NotNil(t, underlyingTransport.TLSNextProto)
		require.NotNil(t, underlyingTransport.ProxyConnectHeader)
		require.NotNil(t, underlyingTransport.GetProxyConnectHeader)
		require.Equal(t, transportConfig.MaxResponseHeaderBytes, underlyingTransport.MaxResponseHeaderBytes)
		require.Equal(t, transportConfig.WriteBufferSize, underlyingTransport.WriteBufferSize)
		require.Equal(t, transportConfig.ReadBufferSize, underlyingTransport.ReadBufferSize)
		require.Equal(t, transportConfig.ForceAttemptHTTP2, underlyingTransport.ForceAttemptHTTP2)
		require.NotNil(t, underlyingTransport.HTTP2)
		require.NotNil(t, underlyingTransport.Protocols)

		require.Equal(t, TransportStats{
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

		transportConfig := *(&TransportConfig{
			MaxConnAge:       16 * time.Minute,
			MaxConnAgeJitter: 0.8,
			HotConnsPerHost:  5,

			MaxIdleConns: 6,
			// MaxIdleConnsPerHost: 2,
			MaxConnsPerHost: 12,
		}).SetRandFactory(func() *rand.Rand {
			return rand.New(&mockSource{Float64: func() float64 { return 0.5 }})
		}).SetRoundTripperFactory(func(transport *http.Transport) http.RoundTripper {
			return &mockRoundTripper{
				RoundTripFunc: func(r *http.Request) (*http.Response, error) {
					underlyingTransport = transport
					return &http.Response{Body: http.NoBody}, nil
				},
			}
		})

		transport := NewTransport(transportConfig)
		resp, err := transport.RoundTrip(&http.Request{})
		require.NoError(t, err)
		resp.Body.Close()
		require.NotNil(t, underlyingTransport)

		require.Equal(t, 2, underlyingTransport.MaxIdleConns)
		require.Equal(t, 1, underlyingTransport.MaxIdleConnsPerHost)
		require.Equal(t, 3, underlyingTransport.MaxConnsPerHost)
		require.Equal(t, 16*time.Minute, underlyingTransport.IdleConnTimeout)

		resp, err = transport.RoundTrip(&http.Request{})
		require.NoError(t, err)
		resp.Body.Close()
		require.NotNil(t, underlyingTransport)

		require.Equal(t, 1, underlyingTransport.MaxIdleConns)
		require.Equal(t, 1, underlyingTransport.MaxIdleConnsPerHost)
		require.Equal(t, 3, underlyingTransport.MaxConnsPerHost)
		require.Equal(t, 16*time.Minute, underlyingTransport.IdleConnTimeout)

		resp, err = transport.RoundTrip(&http.Request{})
		require.NoError(t, err)
		resp.Body.Close()
		require.NotNil(t, underlyingTransport)

		require.Equal(t, 1, underlyingTransport.MaxIdleConns)
		require.Equal(t, 1, underlyingTransport.MaxIdleConnsPerHost)
		require.Equal(t, 2, underlyingTransport.MaxConnsPerHost)
		require.Equal(t, 16*time.Minute, underlyingTransport.IdleConnTimeout)

		require.Equal(t, TransportStats{
			Subs: []*SubTransportStats{
				{
					MaxAge:   "16m0s",
					RefCount: 1,
				},
				{
					MaxAge:   "16m0s",
					RefCount: 1,
				},
				{
					MaxAge:   "16m0s",
					RefCount: 1,
				},
				nil,
				nil,
			},
			NextSubIndex: 3,
		}, transport.Stats())
	}

	{
		var underlyingTransport *http.Transport

		transportConfig := *(&TransportConfig{
			MaxConnAge:       16 * time.Minute,
			MaxConnAgeJitter: 0.8,
			HotConnsPerHost:  5,

			IdleConnTimeout: time.Hour,
		}).SetRandFactory(func() *rand.Rand {
			return rand.New(&mockSource{Float64: func() float64 { return 0.5 }})
		}).SetRoundTripperFactory(func(transport *http.Transport) http.RoundTripper {
			return &mockRoundTripper{
				RoundTripFunc: func(r *http.Request) (*http.Response, error) {
					underlyingTransport = transport
					return &http.Response{Body: http.NoBody}, nil
				},
			}
		})

		transport := NewTransport(transportConfig)
		resp, err := transport.RoundTrip(&http.Request{})
		require.NoError(t, err)
		resp.Body.Close()
		require.NotNil(t, underlyingTransport)

		require.Equal(t, 16*time.Minute, underlyingTransport.IdleConnTimeout)
	}

	{
		var underlyingTransport *http.Transport

		transportConfig := *(&TransportConfig{
			MaxConnAge:       16 * time.Minute,
			MaxConnAgeJitter: 1.0,
			HotConnsPerHost:  5,

			IdleConnTimeout: time.Hour,
		}).SetRandFactory(func() *rand.Rand {
			return rand.New(&mockSource{Float64: func() float64 { return 0.9999999999999 }})
		}).SetRoundTripperFactory(func(transport *http.Transport) http.RoundTripper {
			return &mockRoundTripper{
				RoundTripFunc: func(r *http.Request) (*http.Response, error) {
					underlyingTransport = transport
					return &http.Response{Body: http.NoBody}, nil
				},
			}
		})

		transport := NewTransport(transportConfig)
		resp, err := transport.RoundTrip(&http.Request{})
		require.NoError(t, err)
		resp.Body.Close()
		require.NotNil(t, underlyingTransport)

		require.Equal(t, time.Duration(1), underlyingTransport.IdleConnTimeout)
	}
}

func TestTransport_RoundTrip(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var (
			chOff          atomic.Int64
			inChs          = make([]chan struct{}, 3)
			outChs         = make([]chan struct{}, 3)
			randFloat64Cnt = 0
		)
		for i := range inChs {
			inChs[i] = make(chan struct{})
			outChs[i] = make(chan struct{})
		}

		transportConfig := *(&TransportConfig{
			HotConnsPerHost: 3,

			MaxIdleConns:        111,
			MaxIdleConnsPerHost: 222,
			MaxConnsPerHost:     333,
		}).SetRandFactory(func() *rand.Rand {
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
		}).SetRoundTripperFactory(func(transport *http.Transport) http.RoundTripper {
			i := int(chOff.Add(1)-1) % len(inChs)

			return &mockRoundTripper{
				RoundTripFunc: func(r *http.Request) (*http.Response, error) {
					inChs[i] <- struct{}{}
					<-outChs[i]
					return &http.Response{Body: http.NoBody, Proto: "HTTP/test"}, nil
				},
			}
		})

		transport := NewTransport(transportConfig)
		go func() {
			<-inChs[0]
			<-inChs[1]
			<-inChs[2]
			<-inChs[0]
			defer func() {
				outChs[0] <- struct{}{}
				outChs[1] <- struct{}{}
				outChs[2] <- struct{}{}
				outChs[0] <- struct{}{}
			}()
			if !assert.Equal(t, TransportStats{
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
			}, transport.Stats()) {
				return
			}
		}()
		var wg sync.WaitGroup
		for range 4 {
			wg.Go(func() {
				resp, err := transport.RoundTrip(&http.Request{})
				if !assert.NoError(t, err) {
					return
				}
				resp.Body.Close()
				assert.Equal(t, "HTTP/test", resp.Proto)
			})
		}
		wg.Wait()

		require.Equal(t, TransportStats{
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
	})

	synctest.Test(t, func(t *testing.T) {
		var transportConfig TransportConfig
		var closeIdleConnectionsCnt atomic.Int64
		transportConfig = *(&TransportConfig{}).SetRandFactory(func() *rand.Rand {
			return rand.New(&mockSource{Float64: func() float64 { return 0.8 }})
		}).SetRoundTripperFactory(func(transport *http.Transport) http.RoundTripper {
			return &mockRoundTripper{
				RoundTripFunc: func(r *http.Request) (*http.Response, error) {
					time.Sleep(time.Hour)
					synctest.Wait()
					return nil, errors.New("test error")
				},
				CloseIdleConnectionsFunc: func() {
					closeIdleConnectionsCnt.Add(1)
				},
			}
		})
		transport := NewTransport(transportConfig)
		_, err := transport.RoundTrip(&http.Request{})
		require.EqualError(t, err, "test error")

		require.Equal(t, TransportStats{
			Subs: []*SubTransportStats{
				nil,
				nil,
				nil,
			},
			NextSubIndex: 1,
		}, transport.Stats())

		require.Equal(t, int64(1), closeIdleConnectionsCnt.Load())

		time.Sleep(1 * time.Second)
		synctest.Wait()

		require.Equal(t, int64(2), closeIdleConnectionsCnt.Load())
	})

	synctest.Test(t, func(t *testing.T) {
		var transportConfig TransportConfig
		var closeIdleConnectionsCnt atomic.Int64
		var randFloat64Cnt int
		transportConfig = *(&TransportConfig{
			HotConnsPerHost: 2,
		}).SetRandFactory(func() *rand.Rand {
			return rand.New(&mockSource{Float64: func() float64 {
				randFloat64Cnt++
				switch randFloat64Cnt {
				case 1:
					return 0.4
				case 2:
					return 0.2
				}
				return 0.8
			}})
		}).SetRoundTripperFactory(func(transport *http.Transport) http.RoundTripper {
			return &mockRoundTripper{
				RoundTripFunc: func(r *http.Request) (*http.Response, error) {
					return &http.Response{Body: http.NoBody}, nil
				},
				CloseIdleConnectionsFunc: func() {
					closeIdleConnectionsCnt.Add(1)
				},
			}
		})

		transport := NewTransport(transportConfig)
		resp1, err := transport.RoundTrip(&http.Request{})
		require.NoError(t, err)
		resp2, err := transport.RoundTrip(&http.Request{})
		require.NoError(t, err)
		resp2.Body.Close()

		require.Equal(t, TransportStats{
			Subs: []*SubTransportStats{
				{
					MaxAge:   "11m0s",
					RefCount: 2,
				},
				{
					MaxAge:   "13m0s",
					RefCount: 1,
				},
			},
			NextSubIndex: 0,
		}, transport.Stats())

		resp3, err := transport.RoundTrip(&http.Request{})
		require.NoError(t, err)

		require.Equal(t, TransportStats{
			Subs: []*SubTransportStats{
				{
					MaxAge:   "11m0s",
					RefCount: 2,
				},
				{
					MaxAge:   "13m0s",
					RefCount: 2,
				},
			},
			NextSubIndex: 1,
		}, transport.Stats())

		resp1.Body.Close()
		resp3.Body.Close()

		time.Sleep(11 * time.Minute)
		synctest.Wait()

		require.Equal(t, TransportStats{
			Subs: []*SubTransportStats{
				nil,
				{
					MaxAge:   "13m0s",
					RefCount: 1,
				},
			},
			NextSubIndex: 1,
		}, transport.Stats())

		resp4, err := transport.RoundTrip(&http.Request{})
		require.NoError(t, err)
		resp4.Body.Close()

		require.Equal(t, TransportStats{
			Subs: []*SubTransportStats{
				{
					MaxAge:   "7m0s",
					RefCount: 1,
				},
				{
					MaxAge:   "13m0s",
					RefCount: 1,
				},
			},
			NextSubIndex: 0,
		}, transport.Stats())
	})
}

func TestTransport_RoundTrip_Response_Body(t *testing.T) {
	{
		closeCnt := 0
		transportConfig := *(&TransportConfig{
			HotConnsPerHost: 1,
			MaxConnsPerHost: 1,
		}).SetRandFactory(func() *rand.Rand {
			return rand.New(&mockSource{Float64: func() float64 { return 0.8 }})
		}).SetRoundTripperFactory(func(transport *http.Transport) http.RoundTripper {
			return &mockRoundTripper{
				RoundTripFunc: func(r *http.Request) (*http.Response, error) {
					return &http.Response{
						Body: &mockBody{
							CloseFunc: func() error {
								closeCnt++
								return errors.New("test error")
							},
						},
					}, nil
				},
			}
		})
		transport := NewTransport(transportConfig)
		resp, err := transport.RoundTrip(&http.Request{})
		require.NoError(t, err)

		require.Equal(t, TransportStats{
			Subs: []*SubTransportStats{
				{
					MaxAge:   "7m0s",
					RefCount: 2,
				},
			},
			NextSubIndex: 0,
		}, transport.Stats())

		for i := range 3 {
			err := resp.Body.Close()
			require.EqualError(t, err, "test error")
			require.Equal(t, i+1, closeCnt)
		}

		require.Equal(t, TransportStats{
			Subs: []*SubTransportStats{
				{
					MaxAge:   "7m0s",
					RefCount: 1,
				},
			},
			NextSubIndex: 0,
		}, transport.Stats())
	}

	{
		transportConfig := *(&TransportConfig{}).SetRoundTripperFactory(func(transport *http.Transport) http.RoundTripper {
			return &mockRoundTripper{
				RoundTripFunc: func(r *http.Request) (*http.Response, error) {
					return &http.Response{
						Body: &mockBody{
							ReadFunc: func(p []byte) (n int, err error) {
								return copy(p, "12345"), errors.New("test error")
							},
						},
					}, nil
				},
			}
		})
		transport := NewTransport(transportConfig)
		resp, err := transport.RoundTrip(&http.Request{})
		require.NoError(t, err)

		buf := make([]byte, 5)
		n, err := resp.Body.Read(buf)
		require.EqualError(t, err, "test error")
		require.Equal(t, 5, n)

		_, ok := resp.Body.(io.Writer)
		require.False(t, ok)

		resp.Body.Close()
	}

	{
		transportConfig := *(&TransportConfig{}).SetRoundTripperFactory(func(transport *http.Transport) http.RoundTripper {
			return &mockRoundTripper{
				RoundTripFunc: func(r *http.Request) (*http.Response, error) {
					data := bytes.NewReader([]byte("12345"))
					return &http.Response{
						Body: &mockBody{
							ReadFunc: func(p []byte) (n int, err error) {
								return data.Read(p)
							},
						},
					}, nil
				},
			}
		})
		transport := NewTransport(transportConfig)
		resp, err := transport.RoundTrip(&http.Request{})
		require.NoError(t, err)

		buf := bytes.NewBuffer(nil)
		n, err := resp.Body.(io.WriterTo).WriteTo(buf)
		require.NoError(t, err)
		require.Equal(t, int64(5), n)
		require.Equal(t, "12345", buf.String())

		_, ok := resp.Body.(io.Writer)
		require.False(t, ok)

		resp.Body.Close()
	}

	{
		transportConfig := *(&TransportConfig{}).SetRoundTripperFactory(func(transport *http.Transport) http.RoundTripper {
			return &mockRoundTripper{
				RoundTripFunc: func(r *http.Request) (*http.Response, error) {
					data := bytes.NewReader([]byte("12345"))
					return &http.Response{
						Body: &struct {
							mockBody
							io.WriterTo
						}{
							mockBody: mockBody{},
							WriterTo: data,
						},
					}, nil
				},
			}
		})
		transport := NewTransport(transportConfig)
		resp, err := transport.RoundTrip(&http.Request{})
		require.NoError(t, err)

		buf := bytes.NewBuffer(nil)
		n, err := resp.Body.(io.WriterTo).WriteTo(buf)
		require.NoError(t, err)
		require.Equal(t, int64(5), n)
		require.Equal(t, "12345", buf.String())

		_, ok := resp.Body.(io.Writer)
		require.False(t, ok)

		resp.Body.Close()
	}

	{
		buf := bytes.NewBuffer(nil)
		transportConfig := *(&TransportConfig{}).SetRoundTripperFactory(func(transport *http.Transport) http.RoundTripper {
			return &mockRoundTripper{
				RoundTripFunc: func(r *http.Request) (*http.Response, error) {
					return &http.Response{
						Body: &struct {
							mockBody
							io.Writer
						}{
							mockBody: mockBody{},
							Writer:   buf,
						},
					}, nil
				},
			}
		})
		transport := NewTransport(transportConfig)
		resp, err := transport.RoundTrip(&http.Request{})
		require.NoError(t, err)

		n, err := resp.Body.(io.Writer).Write([]byte("12345"))
		require.NoError(t, err)
		require.Equal(t, 5, n)
		require.Equal(t, "12345", buf.String())

		resp.Body.Close()
	}

	{
		buf := bytes.NewBuffer(nil)
		transportConfig := *(&TransportConfig{}).SetRoundTripperFactory(func(transport *http.Transport) http.RoundTripper {
			return &mockRoundTripper{
				RoundTripFunc: func(r *http.Request) (*http.Response, error) {
					return &http.Response{
						Body: &struct {
							mockBody
							io.Writer
						}{
							mockBody: mockBody{},
							Writer:   buf,
						},
					}, nil
				},
			}
		})
		transport := NewTransport(transportConfig)
		resp, err := transport.RoundTrip(&http.Request{})
		require.NoError(t, err)

		n, err := resp.Body.(io.ReaderFrom).ReadFrom(bytes.NewReader([]byte("12345")))
		require.NoError(t, err)
		require.Equal(t, int64(5), n)
		require.Equal(t, "12345", buf.String())

		resp.Body.Close()
	}

	{
		buf := bytes.NewBuffer(nil)
		transportConfig := *(&TransportConfig{}).SetRoundTripperFactory(func(transport *http.Transport) http.RoundTripper {
			return &mockRoundTripper{
				RoundTripFunc: func(r *http.Request) (*http.Response, error) {
					return &http.Response{
						Body: &struct {
							mockBody
							io.Writer
							io.ReaderFrom
						}{
							mockBody:   mockBody{},
							Writer:     bytes.NewBuffer(nil),
							ReaderFrom: buf,
						},
					}, nil
				},
			}
		})
		transport := NewTransport(transportConfig)
		resp, err := transport.RoundTrip(&http.Request{})
		require.NoError(t, err)

		n, err := resp.Body.(io.ReaderFrom).ReadFrom(bytes.NewReader([]byte("12345")))
		require.NoError(t, err)
		require.Equal(t, int64(5), n)
		require.Equal(t, "12345", buf.String())

		resp.Body.Close()
	}
}

func TestTransport_EvictSub(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var (
			chOff                   atomic.Int64
			inChs                   = make([]chan struct{}, 3)
			outChs                  = make([]chan struct{}, 3)
			randFloat64Cnt          = 0
			closeIdleConnectionsCnt atomic.Int64
		)
		for i := range inChs {
			inChs[i] = make(chan struct{})
			outChs[i] = make(chan struct{})
		}

		transportConfig := *(&TransportConfig{
			HotConnsPerHost: 3,

			MaxIdleConns:        111,
			MaxIdleConnsPerHost: 222,
			MaxConnsPerHost:     333,
		}).SetRandFactory(func() *rand.Rand {
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
		}).SetRoundTripperFactory(func(transport *http.Transport) http.RoundTripper {
			i := int(chOff.Add(1)-1) % len(inChs)

			return &mockRoundTripper{
				RoundTripFunc: func(r *http.Request) (*http.Response, error) {
					inChs[i] <- struct{}{}
					<-outChs[i]
					return &http.Response{Body: http.NoBody, Proto: "HTTP/test"}, nil
				},
				CloseIdleConnectionsFunc: func() {
					closeIdleConnectionsCnt.Add(1)
				},
			}
		})

		transport := NewTransport(transportConfig)
		go func() {
			<-inChs[0]
			<-inChs[1]
			<-inChs[2]
			<-inChs[0]
			<-inChs[1]
			<-inChs[2]
			defer func() {
				outChs[0] <- struct{}{}
				outChs[1] <- struct{}{}
				outChs[2] <- struct{}{}
				outChs[0] <- struct{}{}
				outChs[1] <- struct{}{}
				outChs[2] <- struct{}{}
			}()
			if !assert.Equal(t, TransportStats{
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
			}, transport.Stats()) {
				return
			}

			time.Sleep(9 * time.Minute)
			synctest.Wait()

			if !assert.Equal(t, TransportStats{
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
			}, transport.Stats()) {
				return
			}

			assert.Equal(t, int64(0), closeIdleConnectionsCnt.Load())
		}()
		var wg sync.WaitGroup
		for range 6 {
			wg.Go(func() {
				resp, err := transport.RoundTrip(&http.Request{})
				if !assert.NoError(t, err) {
					return
				}
				resp.Body.Close()
				assert.Equal(t, "HTTP/test", resp.Proto)
			})
		}
		wg.Wait()

		require.Equal(t, TransportStats{
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

		require.Equal(t, int64(1), closeIdleConnectionsCnt.Load())

		time.Sleep(1 * time.Second)
		synctest.Wait()

		require.Equal(t, int64(2), closeIdleConnectionsCnt.Load())

		time.Sleep(2 * time.Minute)
		synctest.Wait()

		require.Equal(t, TransportStats{
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

		require.Equal(t, int64(4), closeIdleConnectionsCnt.Load())
	})
}

func TestTransport_CloseIdleConnections(t *testing.T) {
	var (
		chOff                   atomic.Int64
		inChs                   = make([]chan struct{}, 3)
		outChs                  = make([]chan struct{}, 3)
		randFloat64Cnt          = 0
		closeIdleConnectionsCnt atomic.Int64
	)
	for i := range inChs {
		inChs[i] = make(chan struct{})
		outChs[i] = make(chan struct{})
	}

	transportConfig := *(&TransportConfig{
		HotConnsPerHost: 3,

		MaxIdleConns:        111,
		MaxIdleConnsPerHost: 222,
		MaxConnsPerHost:     333,
	}).SetRandFactory(func() *rand.Rand {
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
	}).SetRoundTripperFactory(func(transport *http.Transport) http.RoundTripper {
		i := int(chOff.Add(1)-1) % len(inChs)

		return &mockRoundTripper{
			RoundTripFunc: func(r *http.Request) (*http.Response, error) {
				inChs[i] <- struct{}{}
				<-outChs[i]
				return &http.Response{Body: http.NoBody, Proto: "HTTP/test"}, nil
			},
			CloseIdleConnectionsFunc: func() {
				closeIdleConnectionsCnt.Add(1)
			},
		}
	})

	transport := NewTransport(transportConfig)
	go func() {
		<-inChs[0]
		<-inChs[1]
		<-inChs[2]
		defer func() {
			outChs[0] <- struct{}{}
			outChs[1] <- struct{}{}
			outChs[2] <- struct{}{}
		}()
		if !assert.Equal(t, TransportStats{
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
		}, transport.Stats()) {
			return
		}

		transport.CloseIdleConnections()

		if !assert.Equal(t, TransportStats{
			Subs: []*SubTransportStats{
				nil,
				nil,
				nil,
			},
			NextSubIndex: 0,
		}, transport.Stats()) {
			return
		}

		assert.Equal(t, int64(0), closeIdleConnectionsCnt.Load())
	}()
	var wg sync.WaitGroup
	for range 3 {
		wg.Go(func() {
			resp, err := transport.RoundTrip(&http.Request{})
			if !assert.NoError(t, err) {
				return
			}
			resp.Body.Close()
			assert.Equal(t, "HTTP/test", resp.Proto)
		})
	}
	wg.Wait()

	require.Equal(t, TransportStats{
		Subs: []*SubTransportStats{
			nil,
			nil,
			nil,
		},
		NextSubIndex: 0,
	}, transport.Stats())

	require.Equal(t, int64(3), closeIdleConnectionsCnt.Load())

	go func() {
		<-inChs[0]
		outChs[0] <- struct{}{}
	}()
	resp, err := transport.RoundTrip(&http.Request{})
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, "HTTP/test", resp.Proto)

	require.Equal(t, TransportStats{
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

	require.Equal(t, TransportStats{
		Subs: []*SubTransportStats{
			nil,
			nil,
			nil,
		},
		NextSubIndex: 1,
	}, transport.Stats())

	require.Equal(t, int64(4), closeIdleConnectionsCnt.Load())
}
