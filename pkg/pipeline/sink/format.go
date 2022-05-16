package sink

type Format string

const (
	BigQuery  Format = "BIGQUERY"
	File      Format = "FILE"
	Firestore Format = "FIRESTORE"
	Redis     Format = "REDIS"
)
