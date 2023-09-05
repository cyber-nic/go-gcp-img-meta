package main

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func startWebServer(ctx context.Context, svc Service, exit chan error, port string) {
	l := loggerFromContext(ctx)

	go func() {
		p := ":" + port
		http.Handle("/metrics", promhttp.Handler())
		http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
			if svc.IsReady() {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte("ready"))
				return
			}
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("not ready"))
		})
		level.Info(l).Log("msg", fmt.Sprintf("Serving '/metrics' on port %s", p))
		level.Info(l).Log("msg", fmt.Sprintf("Serving '/health' on port %s", p))

		server := &http.Server{
			Addr:              p,
			ReadHeaderTimeout: 30 * time.Second,
		}
		exit <- server.ListenAndServe()
	}()
}
