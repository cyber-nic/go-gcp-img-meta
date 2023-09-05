package main

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"cloud.google.com/go/storage"
	crdbpgx "github.com/cockroachdb/cockroach-go/v2/crdb/crdbpgxv5"
	"github.com/go-kit/log/level"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/api/iterator"
)

var (
	objectProcessed = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "meta",
			Name:      "objects_processed",
			Help:      "Total objects processed",
		},
		[]string{"status","operation"},
	)
)

// SvcOptions are service specific process inputs such as arguments
type SvcOptions struct {
	Limit         int
	Prefix        string
	SrcBucketName string
	DstBucketName string
}

// Service is a standard and generic service interface
type Service interface {
	Start() error
	Stop()
	IsReady() bool
}

// ImgDeduper is a service that performs "chunking" of a large body of images.
type ImgDeduper struct {
	Context       context.Context
	Ready         bool
	Limit         int
	Prefix        string
	SrcBucketName string
	DstBucketName string
	Client        *storage.Client
	Roach         *pgx.Conn
}

// NewSvc creates an instance of the ImageChunker service.
func NewSvc(ctx context.Context, client *storage.Client, roach *pgx.Conn, o *SvcOptions) Service {
	return &ImgDeduper{
		Context:       ctx,
		Ready:         false,
		Limit:         o.Limit,
		Prefix:        o.Prefix,
		SrcBucketName: o.SrcBucketName,
		DstBucketName: o.DstBucketName,
		Client:        client,
		Roach:         roach,
	}
}

// IsReady returns a bool describing the state of the service.
// Output:
//
//	True when the service is processing SQS messages
//	Otherwise False
func (svc *ImgDeduper) IsReady() bool {
	return svc.Ready
}

// initTable function performs a cockroachdb sql query using pgx. It uses crdbpgx for transaction handling (retries).
func initTable(ctx context.Context, tx pgx.Tx) error {
	l := loggerFromContext(ctx)

	// Create the images table
	// https://www.cockroachlabs.com/docs/stable/create-table#:~:text=Create%20a%20new%20table%20only,.%2C%20of%20the%20new%20table.
	level.Info(l).Log("msg", "creating image table")
	_, err := tx.Exec(ctx,
		"CREATE TABLE IF NOT EXISTS images (name STRING PRIMARY KEY, section STRING, prefix STRING, size FLOAT, crc32 OID)")
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			// fmt.Println(pgErr.Message) // => syntax error at end of input
			// fmt.Println(pgErr.Code) // => 42601
			if pgErr.Code != "42P07" {
				return err
			}
		}

	}

	level.Info(l).Log("msg", "image table created")
	return nil
}

func insertImage(ctx context.Context, roach *pgx.Conn, i *storage.ObjectAttrs, s string) error {
	err := crdbpgx.ExecuteTx(ctx, roach, pgx.TxOptions{}, func(tx pgx.Tx) error {
		inner := func() error {
			_, err := tx.Exec(ctx,
				"INSERT INTO images (name, section, prefix, size, crc32) VALUES ($1, $2, $3, $4, $5) ON CONFLICT (name) DO NOTHING", i.Name, s, filepath.Dir(i.Name), i.Size, i.CRC32C)
			if err != nil {
				return err
			}
			return nil
		}

		return inner()
	})
	if err != nil {
	 return err
	}

	return nil
}

// getImageCount function performs a cockroachdb sql query using pgx. It uses crdbpgx for transaction handling (retries).
// The inner function allows to return the count value from the query.
func getImageCount(ctx context.Context, roach *pgx.Conn, crc32 uint32) (int, error) {
	// init count
	count := 0

	// check if image exists in database
	err := crdbpgx.ExecuteTx(ctx, roach, pgx.TxOptions{}, func(tx pgx.Tx) error {
		inner := func() error {
			// inner function
			rows, err := tx.Query(ctx, "SELECT COUNT(*) FROM images WHERE crc32 = $1", crc32)
			if err != nil {
				return err
			}

			for rows.Next() {
				if err := rows.Scan(&count); err != nil {
					return err
				}
			}

			return nil
		}

		return inner()
	})
	if err != nil {
		return count, err
	}

	return count, nil
}

// Start begins the ImgDeduper service loop
func (svc *ImgDeduper) Start() error {
	// logger
	l := loggerFromContext(svc.Context)
	level.Info(l).Log("msg", "service started")

	// image index
	idx := 0

	// bucket handler
	dst := svc.Client.Bucket(svc.DstBucketName)
	src := svc.Client.Bucket(svc.SrcBucketName)
	level.Info(l).Log("msg", "dst bucket", "name", svc.DstBucketName)
	level.Info(l).Log("msg", "src bucket", "name", svc.SrcBucketName)

	q := &storage.Query{}
	if svc.Prefix != "" {
		q = &storage.Query{
			// Prefix: fmt.Sprintf("%s/", svc.Prefix),
			MatchGlob: fmt.Sprintf("%s/*.jpg", svc.Prefix),
		}
	}
	b := src.Objects(svc.Context, q)

	// Set up table
	err := crdbpgx.ExecuteTx(svc.Context, svc.Roach, pgx.TxOptions{}, func(tx pgx.Tx) error {
		return initTable(svc.Context, tx)
	})
	if err != nil {
		return err
	}

	// start service
	svc.Ready = true
	level.Info(l).Log("msg", "service ready", "limit", svc.Limit, "glob", q.MatchGlob)

	for svc.Ready {
		// limit the objects processed by count
		idx++
		if svc.Limit != 0 && idx > svc.Limit {
			level.Info(l).Log("msg", "limit reached", "limit", svc.Limit)
			break
		}

		// get next object
		attrs, err := b.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			level.Error(l).Log("msg", "failed to get next bucket object", "error", err)
		}

		// process image
		processImage(svc.Context, svc.Roach, src, dst, attrs)
	}

	return nil
}

func processImage(ctx context.Context, roach *pgx.Conn, src, dst *storage.BucketHandle, attrs *storage.ObjectAttrs) {
	l := loggerFromContext(ctx)
	s := strings.Split(attrs.Name, "/")[0]
	count := 0
	status := "skip"

	// check if image exists in database
	count, err := getImageCount(ctx, roach, attrs.CRC32C)
	if err != nil {
		level.Error(l).Log("msg", "failed to count existing image", "name", attrs.Name, "error", err)
		objectProcessed.With(prometheus.Labels{"status": "error"}).Inc()
		return
	} else {
		level.Debug(l).Log("msg", "count", "section", s, "name", attrs.Name, "count", count, "crc32", attrs.CRC32C)
	}

	// database insert
	if err := insertImage(ctx, roach, attrs, s); err != nil {
		objectProcessed.With(prometheus.Labels{"status": "error"}).Inc()
		level.Error(l).Log("msg", "failed to insert image", "name", attrs.Name, "error", err)
		return
	} else {
		level.Debug(l).Log("msg", "insert", "section", s, "name", attrs.Name, "count", count,  "crc32", attrs.CRC32C)
	}

	// objects
	if count == 0 {
		status = "copy"
		level.Debug(l).Log("msg", "init copy", "section", s, "name", attrs.Name, "count", count,  "crc32", attrs.CRC32C)
		srcObj := src.Object(attrs.Name)
		dstObj := dst.Object(attrs.Name)
		// https://cloud.google.com/storage/docs/copying-renaming-moving-objects#client-libraries
		dstObj = dstObj.If(storage.Conditions{DoesNotExist: true})

		if _, err := dstObj.CopierFrom(srcObj).Run(ctx); err != nil {
			level.Error(l).Log("msg", "copy", "section", s, "name", attrs.Name, "count", count,"crc32", attrs.CRC32C, "error", err)
			objectProcessed.With(prometheus.Labels{"status": "error", "operation": status}).Inc()
			return
		} else {
			level.Debug(l).Log("msg", "copy", "section", s, "name", attrs.Name, "count", count,  "crc32", attrs.CRC32C)
		}
	}

	objectProcessed.With(prometheus.Labels{"status": "success", "operation": status}).Inc()
	level.Info(l).Log("msg", "image", "section", s, "name", attrs.Name, "count", count, "crc32", attrs.CRC32C, "status", status)
}

// Stop instructs the service to stop processing new messages.
func (svc *ImgDeduper) Stop() {
	l := loggerFromContext(svc.Context)
	level.Info(l).Log("msg", "stopping service")
	svc.Ready = false
}
