package source

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
