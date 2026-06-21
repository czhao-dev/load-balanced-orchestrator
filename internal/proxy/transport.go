package proxy

import (
	"net"
	"net/http"
	"time"
)

// NewTransport builds an http.Transport tuned for proxying many concurrent
// requests to a small set of backends: keep-alive connections are reused
// aggressively to avoid the cost of a new TCP handshake per request.
func NewTransport() *http.Transport {
	return &http.Transport{
		Proxy: nil,
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          1000,
		MaxIdleConnsPerHost:   100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
}
