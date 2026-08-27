package clbtransport

import (
	"context"
	"crypto/tls"
	"io"
	"math"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/benbjohnson/clock"
)

const (
	defaultApproximateMaxConnAge       = 10 * time.Minute
	defaultApproximateMaxConnAgeJitter = 0.5
	defaultHotConnsPerHost             = 3
)

var (
	defaultClock = clock.New()

	defaultRandFactory = func() *rand.Rand {
		source := rand.NewPCG(uint64(time.Now().UnixNano()), rand.Uint64())
		return rand.New(source)
	}

	defaultRoundTripperFactory = func(transport *http.Transport) http.RoundTripper { return transport }
)

// TransportConfig configures a Transport.
type TransportConfig struct {
	// ApproximateMaxConnAge limits how long a connection may be reused for new
	// requests. This prevents a long-lived connection from pinning traffic to a
	// single backend behind a host. The bound is soft: in-use connections are
	// allowed to finish. Jitter is applied (see ApproximateMaxConnAgeJitter).
	// Values <= 0 default to 10 minutes.
	ApproximateMaxConnAge time.Duration

	// ApproximateMaxConnAgeJitter is the random jitter applied to
	// ApproximateMaxConnAge, as a fraction in [0, 1]. The effective age is
	// drawn uniformly from [(1-jitter)*MaxConnAge, (1+jitter)*MaxConnAge].
	// Values <= 0 default to 0.5; values > 1 are clamped to 1.
	ApproximateMaxConnAgeJitter float64

	// HotConnsPerHost controls how many independent connections are actively
	// used per host, to avoid pinning traffic to a single backend when a
	// host has several. Higher values improve load distribution at the cost of
	// additional connections.
	// Values <= 0 default to 3.
	HotConnsPerHost int

	// The following options mirror http.Transport; see its docs for details.
	Proxy                  func(*http.Request) (*url.URL, error)
	OnProxyConnectResponse func(ctx context.Context, proxyURL *url.URL, connectReq *http.Request, connectRes *http.Response) error
	DialContext            func(ctx context.Context, network, addr string) (net.Conn, error)
	Dial                   func(network, addr string) (net.Conn, error)
	DialTLSContext         func(ctx context.Context, network, addr string) (net.Conn, error)
	DialTLS                func(network, addr string) (net.Conn, error)
	TLSClientConfig        *tls.Config
	TLSHandshakeTimeout    time.Duration
	DisableKeepAlives      bool
	DisableCompression     bool
	MaxIdleConns           int
	MaxIdleConnsPerHost    int
	MaxConnsPerHost        int
	IdleConnTimeout        time.Duration
	ResponseHeaderTimeout  time.Duration
	ExpectContinueTimeout  time.Duration
	TLSNextProto           map[string]func(authority string, c *tls.Conn) http.RoundTripper
	ProxyConnectHeader     http.Header
	GetProxyConnectHeader  func(ctx context.Context, proxyURL *url.URL, target string) (http.Header, error)
	MaxResponseHeaderBytes int64
	WriteBufferSize        int
	ReadBufferSize         int
	ForceAttemptHTTP2      bool
	HTTP2                  *http.HTTP2Config
	Protocols              *http.Protocols

	// The options below serve for testing.

	// Clock specifies the clock interface to use for time-related operations.
	Clock clock.Clock

	// Rand specifies the random number generator to use.
	RandFactory func() *rand.Rand

	// RoundTripperFactory specifies the function to use for creating http.RoundTripper instances.
	RoundTripperFactory func(*http.Transport) http.RoundTripper
}

func (c *TransportConfig) applyDefaults() {
	if c.ApproximateMaxConnAge < 1 {
		c.ApproximateMaxConnAge = defaultApproximateMaxConnAge
	}
	switch {
	case c.ApproximateMaxConnAgeJitter <= 0.0:
		c.ApproximateMaxConnAgeJitter = defaultApproximateMaxConnAgeJitter
	case c.ApproximateMaxConnAgeJitter > 1.0:
		c.ApproximateMaxConnAgeJitter = 1.0
	}
	if c.HotConnsPerHost < 1 {
		c.HotConnsPerHost = defaultHotConnsPerHost
	}

	if c.MaxIdleConnsPerHost == 0 {
		c.MaxIdleConnsPerHost = http.DefaultMaxIdleConnsPerHost
	}

	if c.Clock == nil {
		c.Clock = defaultClock
	}
	if c.RandFactory == nil {
		c.RandFactory = defaultRandFactory
	}
	if c.RoundTripperFactory == nil {
		c.RoundTripperFactory = defaultRoundTripperFactory
	}
}

// Transport implements [http.RoundTripper] for HTTP, HTTPS, and HTTP proxies
// (including CONNECT). Unlike http.Transport, it bounds connection reuse age
// (see TransportConfig.ApproximateMaxConnAge) and spreads connections to avoid
// synchronized reconnects (see TransportConfig.ApproximateMaxConnAgeJitter and
// TransportConfig.HotConnsPerHost).
type Transport struct {
	config              TransportConfig
	maxIdleConns        int
	maxIdleConnsPerHost int
	maxConnsPerHost     int

	lock         sync.Mutex
	subs         []*subTransport
	nextSubIndex int
	rand         *rand.Rand
}

var _ http.RoundTripper = (*Transport)(nil)

// NewTransport creates a new Transport with the given config.
func NewTransport(config TransportConfig) *Transport {
	config.applyDefaults()
	var (
		maxIdleConns        = config.MaxIdleConns
		maxIdleConnsPerHost = config.MaxIdleConnsPerHost
		maxConnsPerHost     = config.MaxConnsPerHost
	)
	if n := config.HotConnsPerHost; n >= 2 {
		if maxIdleConns >= 1 {
			maxIdleConns = int(math.Ceil(float64(maxIdleConns) / float64(n)))
		}
		if maxIdleConnsPerHost >= 1 {
			maxIdleConnsPerHost = int(math.Ceil(float64(maxIdleConnsPerHost) / float64(n)))
		}
		if maxConnsPerHost >= 1 {
			maxConnsPerHost = int(math.Ceil(float64(maxConnsPerHost) / float64(n)))
		}
	}
	return &Transport{
		config:              config,
		maxIdleConns:        maxIdleConns,
		maxIdleConnsPerHost: maxIdleConnsPerHost,
		maxConnsPerHost:     maxConnsPerHost,
		subs:                make([]*subTransport, config.HotConnsPerHost),
		rand:                config.RandFactory(),
	}
}

// RoundTrip implements the [http.RoundTripper] interface.
func (t *Transport) RoundTrip(request *http.Request) (*http.Response, error) {
	sub := t.pickSub()
	defer func() {
		if sub != nil {
			if cleanup := sub.RemoveRef(); cleanup != nil {
				cleanup()
			}
		}
	}()

	resp, err := sub.RoundTripper.RoundTrip(request)
	if err != nil {
		return nil, err
	}

	if w, ok := resp.Body.(io.Writer); ok {
		resp.Body = &wrappedWritableBody{
			wrappedBody: wrappedBody{
				ReadCloser:   resp.Body,
				SubTransport: sub,
			},
			Writer: w,
		}
	} else {
		resp.Body = &wrappedBody{
			ReadCloser:   resp.Body,
			SubTransport: sub,
		}
	}
	sub = nil
	return resp, nil
}

func (t *Transport) pickSub() *subTransport {
	t.lock.Lock()
	defer t.lock.Unlock()

	i := t.nextSubIndex
	t.nextSubIndex = (t.nextSubIndex + 1) % len(t.subs)
	sub := t.subs[i]
	if sub == nil {
		sub = t.addSubLocked(i)
	}

	sub.AddRef()
	return sub
}

func (t *Transport) addSubLocked(i int) *subTransport {
	maxSubAge := t.calculateMaxSubAgeLocked()
	sub := &subTransport{
		RoundTripper: t.newRoundTripper(maxSubAge),
		Clock:        t.config.Clock,
		MaxAge:       maxSubAge,
	}
	t.subs[i] = sub
	sub.AddRef()
	sub.EvictTimer = t.config.Clock.AfterFunc(maxSubAge, func() { t.evictSub(sub) })
	return sub
}

func (t *Transport) newRoundTripper(maxSubAge time.Duration) http.RoundTripper {
	// Always set a proper IdleConnTimeout as a last resort against connection leaks.
	idleConnTimeout := t.config.IdleConnTimeout
	if !(idleConnTimeout >= 1 && idleConnTimeout <= maxSubAge) {
		idleConnTimeout = maxSubAge
	}
	return t.config.RoundTripperFactory(&http.Transport{
		Proxy:                  t.config.Proxy,
		OnProxyConnectResponse: t.config.OnProxyConnectResponse,
		DialContext:            t.config.DialContext,
		Dial:                   t.config.Dial,
		DialTLSContext:         t.config.DialTLSContext,
		DialTLS:                t.config.DialTLS,
		TLSClientConfig:        t.config.TLSClientConfig,
		TLSHandshakeTimeout:    t.config.TLSHandshakeTimeout,
		DisableKeepAlives:      t.config.DisableKeepAlives,
		DisableCompression:     t.config.DisableCompression,
		MaxIdleConns:           t.maxIdleConns,
		MaxIdleConnsPerHost:    t.maxIdleConnsPerHost,
		MaxConnsPerHost:        t.maxConnsPerHost,
		IdleConnTimeout:        idleConnTimeout,
		ResponseHeaderTimeout:  t.config.ResponseHeaderTimeout,
		ExpectContinueTimeout:  t.config.ExpectContinueTimeout,
		TLSNextProto:           t.config.TLSNextProto,
		ProxyConnectHeader:     t.config.ProxyConnectHeader,
		GetProxyConnectHeader:  t.config.GetProxyConnectHeader,
		MaxResponseHeaderBytes: t.config.MaxResponseHeaderBytes,
		WriteBufferSize:        t.config.WriteBufferSize,
		ReadBufferSize:         t.config.ReadBufferSize,
		ForceAttemptHTTP2:      t.config.ForceAttemptHTTP2,
		HTTP2:                  t.config.HTTP2,
		Protocols:              t.config.Protocols,
	})
}

func (t *Transport) calculateMaxSubAgeLocked() time.Duration {
	return time.Duration(float64(t.config.ApproximateMaxConnAge) * (1 + ((1 - 2*t.rand.Float64()) * t.config.ApproximateMaxConnAgeJitter)))
}

func (t *Transport) evictSub(sub *subTransport) {
	var cleanup func()
	t.lock.Lock()
	defer func() {
		t.lock.Unlock()
		if cleanup != nil {
			cleanup()
		}
	}()

	i := slices.Index(t.subs, sub)
	if i < 0 {
		return
	}
	t.subs[i] = nil
	cleanup = sub.RemoveRef()
}

// CloseIdleConnections closes idle keep-alive connections. It does not
// interrupt connections currently in use.
func (t *Transport) CloseIdleConnections() {
	var cleanups []func()
	t.lock.Lock()
	defer func() {
		t.lock.Unlock()
		for _, cleanup := range cleanups {
			cleanup()
		}
	}()

	for i, sub := range t.subs {
		if sub == nil {
			continue
		}
		t.subs[i] = nil
		if cleanup := sub.RemoveRef(); cleanup != nil {
			cleanups = append(cleanups, cleanup)
		}
	}
}

type subTransport struct {
	RoundTripper http.RoundTripper
	Clock        clock.Clock
	MaxAge       time.Duration // for testing only
	EvictTimer   *clock.Timer

	refCount atomic.Int64
}

func (t *subTransport) AddRef() { t.refCount.Add(1) }

func (t *subTransport) RemoveRef() func() {
	if t.refCount.Add(-1) == 0 {
		return t.close
	}
	return nil
}

func (t *subTransport) close() {
	type closeIdler interface{ CloseIdleConnections() }

	if v, ok := t.RoundTripper.(closeIdler); ok {
		v.CloseIdleConnections()
		// Even if all response bodies are closed, some connections may be on
		// the way to becoming Idle. Close them later.
		t.Clock.AfterFunc(1*time.Second, v.CloseIdleConnections)
	}
	t.EvictTimer.Stop()
}

type wrappedBody struct {
	io.ReadCloser
	SubTransport *subTransport

	isClosed atomic.Bool
}

var _ interface {
	io.Reader
	io.WriterTo
	io.Closer
} = (*wrappedBody)(nil)

func (b *wrappedBody) WriteTo(w io.Writer) (int64, error) {
	if wt, ok := b.ReadCloser.(io.WriterTo); ok {
		return wt.WriteTo(w)
	}
	return io.Copy(w, b.ReadCloser)
}

func (b *wrappedBody) Close() error {
	if b.isClosed.Swap(true) == false {
		defer func() {
			if cleanup := b.SubTransport.RemoveRef(); cleanup != nil {
				cleanup()
			}
		}()
	}
	return b.ReadCloser.Close()
}

type wrappedWritableBody struct {
	wrappedBody
	io.Writer
}

var _ interface {
	io.Reader
	io.WriterTo
	io.Writer
	io.ReaderFrom
	io.Closer
} = (*wrappedWritableBody)(nil)

func (b *wrappedWritableBody) ReadFrom(r io.Reader) (int64, error) {
	if rf, ok := b.Writer.(io.ReaderFrom); ok {
		return rf.ReadFrom(r)
	}
	return io.Copy(b.Writer, r)
}
