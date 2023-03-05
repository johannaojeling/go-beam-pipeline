package options

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

func (cred Credential) GetValue(
	ctx context.Context,
	secretReader *gcp.SecretReader,
) (string, error) {
	if cred.SecretName != "" {
		value, err := secretReader.ReadSecret(ctx, cred.SecretName)
		if err != nil {
			return "", fmt.Errorf("error retrieving secret: %w", err)
		}

		return value, nil
	}

	return os.Getenv(cred.EnvVar), nil
}
