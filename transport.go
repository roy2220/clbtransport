package clbtransport

import (
	"context"
	"crypto/tls"
	"io"
	"maps"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
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

	// HotConnsPerHost controls the minimum number of independent connections
	// that are actively used per host under load, to avoid pinning traffic to a
	// single backend when a host has several. Higher values improve load
	// distribution at the cost of additional connections.
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
	config TransportConfig

	lock         sync.Mutex
	subs         []*subTransport
	nextSubIndex int
	rand         *rand.Rand
}

var _ http.RoundTripper = (*Transport)(nil)

// NewTransport creates a new Transport with the given config.
func NewTransport(config TransportConfig) *Transport {
	config.applyDefaults()
	return &Transport{
		config: config,
		subs:   make([]*subTransport, config.HotConnsPerHost),
		rand:   config.RandFactory(),
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
	var sub *subTransport
	t.lock.Lock()
	defer func() {
		t.lock.Unlock()
		sub.Init()
	}()

	i, n := t.nextSubIndex, len(t.subs)
	t.nextSubIndex = (i + 1) % n
	sub = t.subs[i]
	switch {
	case sub == nil:
		sub = t.addSubLocked(i)
	case n >= 2:
		// Use the Power of Two Choices (P2C) algorithm.
		j := (i + 1 + t.rand.IntN(n-1)) % n
		otherSub := t.subs[j]
		switch {
		case otherSub == nil:
			sub = t.addSubLocked(j)
		case otherSub.RefCount() < sub.RefCount():
			// RefCount reflects active in-flight requests.
			// A newly created subTransport often has a higher in-flight request count
			// due to cold-start overhead (e.g., TCP handshake and slow start).
			// P2C prevents cold subTransports from receiving excessive traffic until
			// they are warm.
			sub = otherSub
		}
	}
	sub.AddRef()
	return sub
}

func (t *Transport) addSubLocked(i int) *subTransport {
	sub := &subTransport{
		Index:         i,
		MaxAge:        t.calculateMaxSubAgeLocked(),
		HandleTimeout: t.evictSub,
		Config:        &t.config,
	}
	t.subs[i] = sub
	sub.AddRef()
	return sub
}

func (t *Transport) calculateMaxSubAgeLocked() time.Duration {
	factor := 1 + t.config.ApproximateMaxConnAgeJitter*(1-2*t.rand.Float64())
	return max(time.Duration(float64(t.config.ApproximateMaxConnAge)*factor), 1)
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

	if t.subs[sub.Index] != sub {
		return
	}
	t.subs[sub.Index] = nil
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
	Index         int
	MaxAge        time.Duration
	HandleTimeout func(*subTransport)
	Config        *TransportConfig

	RoundTripper http.RoundTripper

	refCount atomic.Int64
	initOnce sync.Once
	timer    *clock.Timer
}

func (t *subTransport) AddRef() { t.refCount.Add(1) }

func (t *subTransport) RemoveRef() func() {
	if t.refCount.Add(-1) == 0 {
		return t.close
	}
	return nil
}

func (t *subTransport) RefCount() int { return int(t.refCount.Load()) }

func (t *subTransport) Init() { t.initOnce.Do(t.init) }

func (t *subTransport) init() {
	var (
		maxIdleConns        = t.Config.MaxIdleConns
		maxIdleConnsPerHost = t.Config.MaxIdleConnsPerHost
		maxConnsPerHost     = t.Config.MaxConnsPerHost
	)
	if n := t.Config.HotConnsPerHost; n >= 2 {
		if maxIdleConns >= 1 {
			if maxIdleConns >= n && t.Index >= maxIdleConns%n {
				maxIdleConns /= n
			} else {
				maxIdleConns = maxIdleConns/n + 1
			}
		}
		if maxIdleConnsPerHost >= 1 {
			if maxIdleConnsPerHost >= n && t.Index >= maxIdleConnsPerHost%n {
				maxIdleConnsPerHost /= n
			} else {
				maxIdleConnsPerHost = maxIdleConnsPerHost/n + 1
			}
		}
		if maxConnsPerHost >= 1 {
			if maxConnsPerHost >= n && t.Index >= maxConnsPerHost%n {
				maxConnsPerHost /= n
			} else {
				maxConnsPerHost = maxConnsPerHost/n + 1
			}
		}
	}

	// Always set a proper IdleConnTimeout as a last resort against connection leaks.
	idleConnTimeout := t.Config.IdleConnTimeout
	if !(idleConnTimeout >= 1 && idleConnTimeout <= t.MaxAge) {
		idleConnTimeout = t.MaxAge
	}

	// Clone TLSClientConfig/TLSNextProto/ProxyConnectHeader/HTTP2/Protocols to avoid data race.
	var (
		tlsClientConfig    = t.Config.TLSClientConfig
		tlsNextProto       = t.Config.TLSNextProto
		proxyConnectHeader = t.Config.ProxyConnectHeader
		http2Config        = t.Config.HTTP2
		protocols          = t.Config.Protocols
	)
	if tlsClientConfig != nil {
		tlsClientConfig = tlsClientConfig.Clone()
	}
	if tlsNextProto != nil {
		tlsNextProto = maps.Clone(tlsNextProto)
	}
	if proxyConnectHeader != nil {
		proxyConnectHeader = proxyConnectHeader.Clone()
	}
	if http2Config != nil {
		clone := *http2Config
		http2Config = &clone
	}
	if protocols != nil {
		clone := *protocols
		protocols = &clone
	}

	t.RoundTripper = t.Config.RoundTripperFactory(&http.Transport{
		Proxy:                  t.Config.Proxy,
		OnProxyConnectResponse: t.Config.OnProxyConnectResponse,
		DialContext:            t.Config.DialContext,
		Dial:                   t.Config.Dial,
		DialTLSContext:         t.Config.DialTLSContext,
		DialTLS:                t.Config.DialTLS,
		TLSClientConfig:        tlsClientConfig,
		TLSHandshakeTimeout:    t.Config.TLSHandshakeTimeout,
		DisableKeepAlives:      t.Config.DisableKeepAlives,
		DisableCompression:     t.Config.DisableCompression,
		MaxIdleConns:           maxIdleConns,
		MaxIdleConnsPerHost:    maxIdleConnsPerHost,
		MaxConnsPerHost:        maxConnsPerHost,
		IdleConnTimeout:        idleConnTimeout,
		ResponseHeaderTimeout:  t.Config.ResponseHeaderTimeout,
		ExpectContinueTimeout:  t.Config.ExpectContinueTimeout,
		TLSNextProto:           tlsNextProto,
		ProxyConnectHeader:     proxyConnectHeader,
		GetProxyConnectHeader:  t.Config.GetProxyConnectHeader,
		MaxResponseHeaderBytes: t.Config.MaxResponseHeaderBytes,
		WriteBufferSize:        t.Config.WriteBufferSize,
		ReadBufferSize:         t.Config.ReadBufferSize,
		ForceAttemptHTTP2:      t.Config.ForceAttemptHTTP2,
		HTTP2:                  http2Config,
		Protocols:              protocols,
	})

	t.timer = t.Config.Clock.AfterFunc(t.MaxAge, func() { t.HandleTimeout(t) })
}

func (t *subTransport) close() {
	t.initOnce.Do(func() { panic("unreachable") })

	type closeIdler interface{ CloseIdleConnections() }

	if v, ok := t.RoundTripper.(closeIdler); ok {
		v.CloseIdleConnections()
		// Even if all response bodies are closed, some connections may be on
		// the way to becoming Idle. Close them later.
		t.Config.Clock.AfterFunc(1*time.Second, v.CloseIdleConnections)
	}
	t.timer.Stop()
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
	if !b.isClosed.Swap(true) {
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
