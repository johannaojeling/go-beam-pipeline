package options

type SourceOption struct {
	Format        Format                  `yaml:"format"`
	BigQuery      BigQueryReadOption      `yaml:"bigquery"`
	Database      DatabaseReadOption      `yaml:"database"`
	Elasticsearch ElasticsearchReadOption `yaml:"elasticsearch"`
	File          FileReadOption          `yaml:"file"`
	Firestore     FirestoreReadOption     `yaml:"firestore"`
	MongoDB       MongoDBReadOption       `yaml:"mongodb"`
	Redis         RedisReadOption         `yaml:"redis"`
}

type BigQueryReadOption struct {
	Project string `yaml:"project"`
	Dataset string `yaml:"dataset"`
	Table   string `yaml:"table"`
}

type DatabaseReadOption struct {
	Driver string     `yaml:"driver"`
	DSN    Credential `yaml:"dsn"`
	Table  string     `yaml:"table"`
}

type ElasticsearchReadOption struct {
	URLs      Credential `yaml:"urls"`
	CloudID   Credential `yaml:"cloud_id"`
	APIKey    Credential `yaml:"api_key"`
	Index     string     `yaml:"index"`
	Query     string     `yaml:"query"`
	BatchSize int        `yaml:"batch_size"`
	KeepAlive string     `yaml:"keep_alive"`
}

type FileReadOption struct {
	Format FileFormat `yaml:"format"`
	Path   string     `yaml:"path"`
}

type FirestoreReadOption struct {
	Project    string `yaml:"project"`
	Collection string `yaml:"collection"`
}

type MongoDBReadOption struct {
	URL        Credential `yaml:"url"`
	Database   string     `yaml:"database"`
	Collection string     `yaml:"collection"`
	Filter     string     `yaml:"filter"`
}

type RedisReadOption struct {
	URL         Credential `yaml:"url"`
	KeyPatterns []string   `yaml:"key_patterns"`
	BatchSize   int        `yaml:"batch_size"`
}
