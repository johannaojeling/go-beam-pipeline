package sink

type Format string

const (
	BigQuery      Format = "BIGQUERY"
	Elasticsearch Format = "ELASTICSEARCH"
	File          Format = "FILE"
	Firestore     Format = "FIRESTORE"
	Redis         Format = "REDIS"
)
