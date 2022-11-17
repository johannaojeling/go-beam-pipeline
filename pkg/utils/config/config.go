package config

import (
	"bytes"
	"fmt"
	"text/template"

	"gopkg.in/yaml.v3"
)

func ParseConfig(
	content string,
	fields any,
	out any,
) error {
	parsed, err := parseTemplate(content, fields)
	if err != nil {
		return fmt.Errorf("error parsing template: %w", err)
	}

	if err := yaml.Unmarshal(parsed, out); err != nil {
		return fmt.Errorf("error unmarshaling yaml: %w", err)
	}

	return nil
}

func parseTemplate(content string, data any) ([]byte, error) {
	tmpl := template.New("template")

	tmpl, err := tmpl.Parse(content)
	if err != nil {
		return nil, fmt.Errorf("error parsing content: %w", err)
	}

	var buffer bytes.Buffer
	if err := tmpl.Execute(&buffer, data); err != nil {
		return nil, fmt.Errorf("error applying template: %w", err)
	}

	return buffer.Bytes(), nil
}
