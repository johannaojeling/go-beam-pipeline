package options

type Format string

const (
	BigQuery      Format = "BIGQUERY"
	Database      Format = "DATABASE"
	Elasticsearch Format = "ELASTICSEARCH"
	File          Format = "FILE"
	Firestore     Format = "FIRESTORE"
	MongoDB       Format = "MONGODB"
	Redis         Format = "REDIS"
)

type FileFormat string

const (
	Avro    FileFormat = "AVRO"
	CSV     FileFormat = "CSV"
	JSON    FileFormat = "JSON"
	Parquet FileFormat = "PARQUET"
)
