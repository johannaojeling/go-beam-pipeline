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
		return fmt.Errorf("error parsing template: %v", err)
	}

	err = yaml.Unmarshal(parsed, out)
	if err != nil {
		return fmt.Errorf("error unmarshaling yaml: %v", err)
	}

	return nil
}

func parseTemplate(content string, data any) ([]byte, error) {
	tmpl := template.New("template")
	tmpl, err := tmpl.Parse(content)
	if err != nil {
		return nil, fmt.Errorf("error parsing content: %v", err)
	}

	var buffer bytes.Buffer
	err = tmpl.Execute(&buffer, data)
	if err != nil {
		return nil, fmt.Errorf("error applying template: %v", err)
	}

	return buffer.Bytes(), nil
}
