# Go Beam Pipeline

## Introduction

This project contains a pipeline with a number of IO transforms developed with the Apache Beam Go SDK. The pipeline
reads from a source and writes to a sink. Which source and sink to use can be configured in a templated yaml file, which
is passed to the program as an argument. Example configuration is in the [examples/config](examples/config) folder.

Supported sources:

- BigQuery
- Cloud Storage (avro, csv, json, parquet)
- Cloud SQL (MySQL, PostgreSQL)
- Elasticsearch
- Firestore
- Memorystore (Redis)
- MongoDB

Supported sinks:

- BigQuery
- Cloud Storage (avro, csv, json, parquet)
- Cloud SQL (MySQL, PostgreSQL)
- Elasticsearch
- Firestore
- Memorystore (Redis)
- MongoDB

## Prerequisites

- Go version 1.19
- Gcloud CLI
- Docker

## Development

### Setup

Install dependencies

```bash
go mod download
```

### Testing

Run unit tests

```bash
go test ./... -short
```

Run unit tests and long-running integration tests

```bash
go test ./...
```

### Running with DirectRunner

Set variables

| Variable    | Description                                        |
|-------------|----------------------------------------------------|
| CONFIG_PATH | Path to configuration file (local or GCS path)     |
| PROJECT     | GCP project                                        |
| BUCKET      | Bucket for data storage (if source or sink is GCS) |

Run pipeline

```bash
go run main.go --configPath=${CONFIG_PATH} --project=${PROJECT} --bucket=${BUCKET}
```

## Deployment

Set variables

| Variable        | Description                                                                                                                                                                                                                                        |
|-----------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| CONFIG_PATH     | Path to configuration file (local or GCS path)                                                                                                                                                                                                     |
| PROJECT         | GCP project                                                                                                                                                                                                                                        |
| BUCKET          | Bucket for data storage (if source or sink is GCS)                                                                                                                                                                                                 |
| REGION          | Compute region                                                                                                                                                                                                                                     |
| SUBNETWORK      | Subnetwork                                                                                                                                                                                                                                         |
| SA_EMAIL        | Email of service account used for Dataflow. Needs the roles:<br/><ul><li>`roles/dataflow.worker`</li><li>`roles/bigquery.dataEditor`</li><li>`roles/bigquery.jobUser`</li><li>`roles/datastore.user`</li><li>`roles/storage.objectAdmin`</li></ul> |
| DATAFLOW_BUCKET | Bucket for Dataflow staging data                                                                                                                                                                                                                   |

### Running with DataflowRunner

```bash
go run main.go \
--configPath=${CONFIG_PATH} \
--project=${PROJECT} \
--bucket=${BUCKET} \
--runner=dataflow \
--region=${REGION} \
--subnetwork=${SUBNETWORK} \
--service_account_email=${SA_EMAIL} \
--staging_location=gs://${DATAFLOW_BUCKET}/staging \
--job_name=${JOB_NAME}-$(date +%s)
```
