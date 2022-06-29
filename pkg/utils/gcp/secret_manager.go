package gcp

import (
	"context"
	"fmt"
	"sync"

	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	secretmanagerpb "google.golang.org/genproto/googleapis/cloud/secretmanager/v1"
)

type SecretReader struct {
	clientOnce *sync.Once
	client     *secretmanager.Client
}

func NewSecretReader() *SecretReader {
	return &SecretReader{
		clientOnce: &sync.Once{},
	}
}

func (reader *SecretReader) ReadSecret(ctx context.Context, secret string) (string, error) {
	client, err := reader.getClient(ctx)
	if err != nil {
		return "", fmt.Errorf("error getting client: %v", err)
	}

	request := &secretmanagerpb.AccessSecretVersionRequest{Name: secret}
	response, err := client.AccessSecretVersion(ctx, request)
	if err != nil {
		return "", fmt.Errorf("error accessing secret version: %v", err)
	}

	data := string(response.Payload.Data)
	return data, nil
}

func (reader *SecretReader) getClient(ctx context.Context) (*secretmanager.Client, error) {
	var initErr error
	reader.clientOnce.Do(func() {
		client, err := secretmanager.NewClient(ctx)
		if err != nil {
			initErr = fmt.Errorf("error initializing Secret Manager client: %v", err)
			return
		}
		reader.client = client
	})
	return reader.client, initErr
}

func (reader *SecretReader) Close() error {
	if reader.client != nil {
		return reader.client.Close()
	}
	return nil
}
