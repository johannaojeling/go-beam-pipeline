# Go Beam Pipeline

## Introduction

This project contains a pipeline and a number of IO transforms developed with the Apache Beam Go SDK. It can be run with
DirectRunner or DataflowRunner.

The pipeline reads from a source and writes to a sink. Which source and sink to use can be configured in a templated
yaml file, which is passed to the program as an argument. The configuration file can be read from the local file system
or from Cloud Storage. Example configuration files can be found in the [config](config) directory.

Supported sources:

- BigQuery
- Cloud Storage (avro, csv, json)
- Firestore

Supported sinks:

- BigQuery
- Cloud Storage (avro, csv, json)
- Firestore

The image below shows a Dataflow pipeline that reads from file/json and writes to file/csv.

![Dataflow pipeline](images/dataflow.png)

## Pre-requisites

- Go version 1.16
- Gcloud SDK

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

Run unit tests and integration tests

```bash
go test ./...
```

### Running with DirectRunner

Set environment variables

| Variable    | Description                                                    |
|-------------|----------------------------------------------------------------|
| CONFIG_PATH | Path to yaml configuration file                                |
| PROJECT     | GCP project (if source or sink is one of: BigQuery, Firestore) |
| BUCKET      | Bucket for data storage (if source or sink is GCS)             |

Run pipeline

```bash
# If running with the example input: ignore PROJECT and BUCKET, and set CONFIG_PATH:
export CONFIG_PATH="config/local-json-to-csv.yaml"

go run main.go --configPath=${CONFIG_PATH}
```

## Deployment

Set environment variables

| Variable        | Description                                                                                                                                                                                                                                        |
|-----------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| CONFIG_PATH     | Path to yaml configuration file                                                                                                                                                                                                                    |
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
--runner=dataflow \
--project=${PROJECT} \
--region=${REGION} \
--subnetwork=${SUBNETWORK} \
--service_account_email=${SA_EMAIL} \
--staging_location=gs://${DATAFLOW_BUCKET}/staging \
--job_name=${JOB_NAME}-$(date +%s)
```
