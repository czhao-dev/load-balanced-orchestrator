// Command backend runs a minimal HTTP server used as a demo upstream for
// the reverse proxy: it answers /health for the health checker and
// identifies itself on every other path so load distribution is visible.
package main

import (
	"encoding/json"
	"flag"
	"log"
	"math/rand"
	"net/http"
	"time"
)

func main() {
	name := flag.String("name", "backend", "name reported in responses")
	addr := flag.String("addr", ":9001", "listen address")
	latency := flag.Duration("latency", 0, "artificial response latency, e.g. 50ms")
	jitter := flag.Duration("jitter", 0, "random additional latency added on top of -latency")
	flag.Parse()

	mux := http.NewServeMux()

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if *latency > 0 || *jitter > 0 {
			delay := *latency
			if *jitter > 0 {
				delay += time.Duration(rand.Int63n(int64(*jitter)))
			}
			time.Sleep(delay)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"backend": *name,
			"path":    r.URL.Path,
			"message": "hello from " + *name,
		})
	})

	log.Printf("backend %q listening on %s", *name, *addr)
	if err := http.ListenAndServe(*addr, mux); err != nil {
		log.Fatal(err)
	}
}
