package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"cloud.google.com/go/storage"
	"github.com/go-kit/log/level"
	"github.com/jackc/pgx/v5"
)

const (
	exitCodeSuccess   = 0
	exitCodeErr       = 1
	exitCodeInterrupt = 2
)

type DBOptions struct {
	DBUsername         string
	DBPassword         string
	DBConnectionString string
}

// SvcOptions are service specific process inputs such as arguments
func parseCLIArgs() (bool, string, SvcOptions, DBOptions) {
	// toggle debug logging
	debug := flag.Bool("debug", false, "Debug logging level")
	limit := flag.Int("limit", 0, "Number of files to process before terminating")
	port := flag.String("port", "8080", "Port to listen on")

	srcBucketName := flag.String("src", "src_bucket_name", "Source GCP S3 bucket name")
	dstBucketName := flag.String("dst", "dst_bucket_name", "Destination GCP S3 bucket name")
	prefix := flag.String("prefix", "**", "S3 bucket prefix on which to operate")

	dbUsername := flag.String("u", "database_username", "Database Username")
	dbPassword := flag.String("p", "database_password", "Database Password")
	dbConnectionString := flag.String("c", "database_connection_string", "Database Connection String")

	flag.Parse()

	// ImgDeduper svc options
	svcOpts := SvcOptions{
		SrcBucketName: *srcBucketName,
		DstBucketName: *dstBucketName,
		Prefix:        *prefix,
		Limit:         *limit,
	}
	// db options
	dbOpts := DBOptions{
		DBUsername:         *dbUsername,
		DBPassword:         *dbPassword,
		DBConnectionString: *dbConnectionString,
	}

	return *debug, *port, svcOpts, dbOpts
}

func main() {
	// args
	debug, port, svcOpts, dbOpts := parseCLIArgs()

	// context
	var ctx context.Context
	ctx = context.Background()
	ctx = contextWithLogger(ctx, newLogger(debug))
	// todo: WithTimeout terminates the SQL connection after prescribed time. Need to figure out how to keep it alive / reconnect.
	// ctx, cancel := context.WithTimeout(ctx, time.Second*5)
	// defer cancel()
	ctx, cancel := context.WithCancel(ctx)

	// logger
	l := loggerFromContext(ctx)

	// signal handling
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	defer func() {
		signal.Stop(signalChan)
		cancel()
	}()

	// interrupt handling
	done := make(chan error)
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGTERM, os.Interrupt)
	}()

	client, err := storage.NewClient(ctx)
	if err != nil {
		level.Error(l).Log("msg", "failed to create storage client", "error", err)
		panic(1)
	}
	defer client.Close()
	level.Info(l).Log("msg", "storage client created")

	// database client
	dsn := fmt.Sprintf("postgresql://%s:%s@%s", dbOpts.DBUsername, dbOpts.DBPassword, dbOpts.DBConnectionString)
	roach, err := pgx.Connect(ctx, dsn)
	defer roach.Close(ctx)
	if err != nil {
		level.Error(l).Log("msg", "failed to connect database", "error", err)
		os.Exit(exitCodeErr)
	}
	level.Info(l).Log("msg", "database connection established")

	// main service
	svc := NewSvc(ctx, client, roach, &svcOpts)
	go func() {
		if err := svc.Start(); err != nil {
			level.Error(l).Log("msg", "service failure", "error", err)
			os.Exit(exitCodeErr)
		}
		level.Info(l).Log("msg", "service process completed")
		os.Exit(exitCodeSuccess)
	}()

	// allow context cancelling
	go func() {
		select {
		case <-signalChan: // first signal, cancel context
			cancel()
			svc.Stop()
		case <-ctx.Done():
		}
		<-signalChan // second signal, hard exit
		os.Exit(exitCodeInterrupt)
	}()

	// metrics and health
	startWebServer(ctx, svc, done, port)
	level.Info(l).Log("exit", <-done)
	roach.Close(ctx)
}
