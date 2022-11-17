package file

type Format string

const (
	AVRO    Format = "AVRO"
	CSV     Format = "CSV"
	JSON    Format = "JSON"
	PARQUET Format = "PARQUET"
)
