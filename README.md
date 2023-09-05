# go-gcp-img-meta

The purpose of this code was to copy images from one GCP bucket to another, while also storing metadata about the images in a CockroachDB instance and filtering out duplicates. In the end, I decided to use a slightly different approach, but I'm keeping this code around for reference.

```
|---------------------------------------|------------|--------|---------|
| Name                                  | crc32      | size   | prefix  |
|---------------------------------------|------------|--------|---------|
|A/1/1_1.jpg                            | 2784486622 | 253648 | A/1     |
|---------------------------------------|------------|--------|---------|
|A/1/1_2.jpg                            | 3645130469 | 253703 | A/1     |
|A/2/2_1.jpg                            | 3645130469 | 253703 | A/2     |
|---------------------------------------|------------|--------|---------|
```

# algo

```
1. get img attr
1. check if crc32 exists in DB
1. if exists, insert into DB
1. if new, insert into DB + copy image w/ prefix to destination bucket
```

# pricing

https://cloud.google.com/storage/pricing#operations-by-class

# run

```
./bin/app \
  -src my-source-bucket \
  -dst my-destination-bucket \
  -prefix A \
  -u foo \
  -p bar \
  -c my.cockroachlabs.cloud:26257/foo?sslmode=verify-full
```

```
./bin/app \
  -debug \
  -src my-source-bucket \
  -dst my-destination-bucket \
  -u foo -p bar \
  -c my.cockroachlabs.cloud:26257/foo?sslmode=verify-full \
  -limit 25 \
  -prefix "A/**"
```

# docs

https://www.cockroachlabs.com/docs/stable/build-a-go-app-with-cockroachdb

# SQL

```
SELECT crc32, COUNT(*) AS num FROM images GROUP BY crc32;

SELECT section, crc32, COUNT(*) AS num FROM images GROUP BY section, crc32 HAVING COUNT(*) > 1 ORDER BY num;

SELECT section, COUNT(name) as files, COUNT(DISTINCT crc32) as uniq FROM images GROUP BY section;
```

# CockroachDB local

```
cockroach start-single-node --certs-dir=certs --store=node1 --http-addr=localhost:8080
cockroach sql --certs-dir=certs --host=localhost:26257
```
