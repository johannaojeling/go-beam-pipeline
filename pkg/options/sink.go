package options

import (
	"time"
)

type SinkOption struct {
	Format        Format                   `yaml:"format"`
	BigQuery      BigQueryWriteOption      `yaml:"bigquery"`
	Database      DatabaseWriteOption      `yaml:"database"`
	Elasticsearch ElasticsearchWriteOption `yaml:"elasticsearch"`
	File          FileWriteOption          `yaml:"file"`
	Firestore     FirestoreWriteOption     `yaml:"firestore"`
	MongoDB       MongoDBWriteOption       `yaml:"mongodb"`
	Redis         RedisWriteOption         `yaml:"redis"`
}

type BigQueryWriteOption struct {
	Project string `yaml:"project"`
	Dataset string `yaml:"dataset"`
	Table   string `yaml:"table"`
}

type DatabaseWriteOption struct {
	Driver  string     `yaml:"driver"`
	DSN     Credential `yaml:"dsn"`
	Table   string     `yaml:"table"`
	Columns []string   `yaml:"columns"`
}
type ElasticsearchWriteOption struct {
	URLs       Credential `yaml:"urls"`
	CloudID    Credential `yaml:"cloud_id"`
	APIKey     Credential `yaml:"api_key"`
	Index      string     `yaml:"index"`
	FlushBytes int        `yaml:"flush_bytes"`
}

type FileWriteOption struct {
	Format FileFormat      `yaml:"format"`
	Path   string          `yaml:"path"`
	Avro   AvroWriteOption `yaml:"avro"`
}

type AvroWriteOption struct {
	Schema string `yaml:"schema"`
}

type FirestoreWriteOption struct {
	Project    string `yaml:"project"`
	Collection string `yaml:"collection"`
	BatchSize  int    `yaml:"batch_size"`
}

type MongoDBWriteOption struct {
	URL        Credential `yaml:"url"`
	Database   string     `yaml:"database"`
	Collection string     `yaml:"collection"`
}

type RedisWriteOption struct {
	URL        Credential    `yaml:"url"`
	Expiration time.Duration `yaml:"expiration"`
	BatchSize  int           `yaml:"batch_size"`
	KeyField   string        `yaml:"key_field"`
}
