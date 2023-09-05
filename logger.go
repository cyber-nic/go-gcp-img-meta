package main

import (
	"context"
	"os"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

type ctxLogger struct{}

func newLogger(debug bool) *log.Logger {
	var logger log.Logger
	{
		logger = log.NewLogfmtLogger(os.Stdout)
		logger = log.With(logger, "ts", log.DefaultTimestampUTC)
		logger = log.With(logger, "caller", log.DefaultCaller)
		if !debug {
			logger = level.NewFilter(logger, level.AllowInfo())
		}
	}
	return &logger
}

// ContextWithLogger adds logger to context
func contextWithLogger(ctx context.Context, l *log.Logger) context.Context {
	return context.WithValue(ctx, ctxLogger{}, l)
}

func loggerFromContext(ctx context.Context) log.Logger {
	if l, ok := ctx.Value(ctxLogger{}).(*log.Logger); ok {
		return *l
	}

	w := log.NewSyncWriter(os.Stderr)
	logger := log.NewLogfmtLogger(w)
	return logger
}
