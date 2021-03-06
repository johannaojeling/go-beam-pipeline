package sink

type Format string

const (
	BigQuery      Format = "BIGQUERY"
	Database      Format = "DATABASE"
	Elasticsearch Format = "ELASTICSEARCH"
	File          Format = "FILE"
	Firestore     Format = "FIRESTORE"
	Redis         Format = "REDIS"
)
