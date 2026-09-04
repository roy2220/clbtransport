# clbtransport

[![Go Reference](https://pkg.go.dev/badge/github.com/roy2220/clbtransport.svg)](https://pkg.go.dev/github.com/roy2220/clbtransport)
[![Coverage](./.badges/coverage.svg)](#)

A minimalist Go `http.Transport` wrapper that implements client-side load balancing.

## The problem

Kubernetes is the standard infra for cloud-native workloads today, and most backend developers eventually hit the same wall: calling another service through its Kubernetes Service DNS name over plain HTTP leads to uneven load across backend pods.

The usual fixes people reach for are:

- The Kubernetes Endpoints API
- A gateway / load balancer
- A service mesh

But does it really have to be this heavy?

### Rethinking load balancing

Think about the ideal case: if every single request picked a target node at random from the whole pool right before sending, the cluster load would end up balanced (assuming all nodes are roughly equal in capacity).

Short-lived HTTP connections combined with disabled DNS caching get you exactly that — but at a cost nobody wants to pay: every request now pays for a DNS lookup, a TCP handshake, and a TCP teardown.

So long-lived (keep-alive) connections are non-negotiable. The real problem becomes controlling *how often* a request gets to re-pick its target node:

- Too rare, and traffic piles up on whichever node was picked first.
- Too frequent, and the cost of constantly re-establishing TCP connections eats the balancing benefit.

### The approach

`clbtransport` controls that frequency along two axes:

1. **Bound connection lifetime.** Long-lived connections aren't allowed to live forever — after some time (with jitter, to avoid synchronized reconnects) they're retired, giving requests a fresh chance to land on a different node.
2. **Spread connections per host.** Since connection lifetime can't be pushed arbitrarily low without hurting latency/SLA (TCP setup blocks the request), at least a small number of independent connections are kept "hot" per host under load, multiplying the opportunities for requests to fan out across nodes.

The result behaves like a standard `http.RoundTripper` — a drop-in replacement for `http.Transport` — that quietly rebalances traffic in the background instead of relying on cluster-side machinery.

## Features

- Drop-in replacement for `http.Transport`; implements `http.RoundTripper`
- No sidecar, no Endpoints watch, no extra network hop
- Jittered connection age limit to avoid thundering-herd reconnects
- Configurable number of "hot" connections per host
- All standard `http.Transport` options (dialer, TLS, HTTP/2, timeouts, etc.) are supported and forwarded

## Quick start

```go
package main

import (
    "net"
    "net/http"
    "time"

    "github.com/roy2220/clbtransport"
)

func main() {
    transport := clbtransport.NewTransport(clbtransport.TransportConfig{
        // Retire a connection after roughly 10 minutes of reuse (jittered).
        ApproximateMaxConnAge:       10 * time.Minute,
        ApproximateMaxConnAgeJitter: 0.5,
        // Keep 3 independent connections per host "hot" at once.
        HotConnsPerHost: 3,

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

    client := &http.Client{
        Transport: transport,
    }

    resp, err := client.Get("http://my-service.my-namespace.svc.cluster.local/health")
    if err != nil {
        panic(err)
    }
    defer resp.Body.Close()
}
```

That's it — use `client` exactly as you would with the standard library.

## Configuration

| Field | Default | Description |
|---|---|---|
| `ApproximateMaxConnAge` | 10 minutes | Soft upper bound on how long a connection may be reused. In-use connections are allowed to finish. |
| `ApproximateMaxConnAgeJitter` | 0.5 | Fraction of jitter applied to `ApproximateMaxConnAge`, drawn uniformly from `[(1-jitter)*MaxAge, (1+jitter)*MaxAge]`. |
| `HotConnsPerHost` | 3 | Minimum number of independent connections kept active per host under load. |

All other fields mirror `http.Transport` and are passed through; see the [Go documentation](https://pkg.go.dev/net/http#Transport) for details.

## When (not) to use this

`clbtransport` is a good fit when:

- **☑** You're calling a Kubernetes Service over plain HTTP/HTTPS keep-alive connections — including a **ClusterIP** Service.
- **☑** More generally, any DNS name backed by multiple IPs, or any address whose actual destination is chosen anew each time a TCP connection is established.
- **☑** You want better load distribution without introducing a mesh, gateway, or client-side service discovery.

It's *not* a substitute for:

- **☒** Health-aware routing (it doesn't know which backends are unhealthy).
- **☒** Scenarios where the destination is fixed regardless of when the connection is opened (there's nothing for a new connection to be routed differently to).

## License

MIT
