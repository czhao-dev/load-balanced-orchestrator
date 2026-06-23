package proxy

import (
	"net"
	"net/http"
	"strings"

	"github.com/czhao-dev/reverse-proxy-load-balancer/internal/backend"
)

// newOutboundRequest builds the request to send to b, rewriting the URL to
// the backend's scheme/host and forwarding the original method, headers,
// and body.
func newOutboundRequest(r *http.Request, b *backend.Backend) (*http.Request, error) {
	outReq := r.Clone(r.Context())
	outReq.URL.Scheme = b.URL.Scheme
	outReq.URL.Host = b.URL.Host
	outReq.URL.Path = singleJoiningSlash(b.URL.Path, r.URL.Path)
	outReq.Host = b.URL.Host
	outReq.RequestURI = ""

	applyForwardingHeaders(outReq, r)

	return outReq, nil
}

func singleJoiningSlash(a, b string) string {
	aSlash := strings.HasSuffix(a, "/")
	bSlash := strings.HasPrefix(b, "/")
	switch {
	case aSlash && bSlash:
		return a + b[1:]
	case !aSlash && !bSlash:
		return a + "/" + b
	default:
		return a + b
	}
}

// applyForwardingHeaders sets the standard proxy headers (X-Forwarded-For,
// X-Forwarded-Host, X-Forwarded-Proto) on the outbound request.
func applyForwardingHeaders(outReq *http.Request, original *http.Request) {
	if clientIP, _, err := net.SplitHostPort(original.RemoteAddr); err == nil {
		if prior := outReq.Header.Get("X-Forwarded-For"); prior != "" {
			outReq.Header.Set("X-Forwarded-For", prior+", "+clientIP)
		} else {
			outReq.Header.Set("X-Forwarded-For", clientIP)
		}
	}

	if outReq.Header.Get("X-Forwarded-Host") == "" {
		outReq.Header.Set("X-Forwarded-Host", original.Host)
	}

	proto := "http"
	if original.TLS != nil {
		proto = "https"
	}
	outReq.Header.Set("X-Forwarded-Proto", proto)
}
