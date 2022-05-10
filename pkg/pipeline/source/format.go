package source

type Format string

const (
	BigQuery  Format = "BIGQUERY"
	Database  Format = "DATABASE"
	File      Format = "FILE"
	Firestore Format = "FIRESTORE"
)
