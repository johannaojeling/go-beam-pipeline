package options

type PipelineOption struct {
	Source SourceOption `yaml:"source"`
	Sink   SinkOption   `yaml:"sink"`
}
