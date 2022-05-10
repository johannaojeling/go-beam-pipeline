package gcp

import (
	"context"
	"fmt"

	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	secretmanagerpb "google.golang.org/genproto/googleapis/cloud/secretmanager/v1"
)

func ReadSecret(ctx context.Context, secret string) (string, error) {
	client, err := secretmanager.NewClient(ctx)
	if err != nil {
		return "", fmt.Errorf("error initializing Secret Manager client: %v", err)
	}
	defer client.Close()

	request := &secretmanagerpb.AccessSecretVersionRequest{Name: secret}

	response, err := client.AccessSecretVersion(ctx, request)
	if err != nil {
		return "", fmt.Errorf("error accessing secret version: %v", err)
	}

	data := string(response.Payload.Data)
	return data, nil
}
