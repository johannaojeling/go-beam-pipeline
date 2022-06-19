package creds

import (
	"context"
	"fmt"
	"os"

	"github.com/johannaojeling/go-beam-pipeline/pkg/utils/gcp"
)

type Credential struct {
	EnvVar     string `yaml:"env_var"`
	SecretName string `yaml:"secret_name"`
}

func (cred Credential) GetValue() (string, error) {
	if cred.SecretName != "" {
		value, err := gcp.ReadSecret(context.Background(), cred.SecretName)
		if err != nil {
			return "", fmt.Errorf("error retrieving secret: %v", err)
		}
		return value, nil
	}
	return os.Getenv(cred.EnvVar), nil
}
