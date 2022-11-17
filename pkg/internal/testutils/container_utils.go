package testutils

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type ContainerConfig struct {
	Image      string
	Env        map[string]string
	Networks   []string
	Ports      []string
	WaitForLog string
}

func CreateNetwork(ctx context.Context, t *testing.T, name string) testcontainers.Network {
	t.Helper()

	networkRequest := testcontainers.NetworkRequest{
		Name:           name,
		CheckDuplicate: true,
	}
	genericNetworkRequest := testcontainers.GenericNetworkRequest{
		NetworkRequest: networkRequest,
	}

	network, err := testcontainers.GenericNetwork(ctx, genericNetworkRequest)
	if err != nil {
		t.Fatalf("error creating network: %v", err)
	}

	return network
}

func CreateContainer(
	ctx context.Context,
	t *testing.T,
	cfg ContainerConfig,
) testcontainers.Container {
	t.Helper()

	request := testcontainers.ContainerRequest{
		Image:        cfg.Image,
		Env:          cfg.Env,
		ExposedPorts: cfg.Ports,
		Networks:     cfg.Networks,
		WaitingFor:   wait.ForLog(cfg.WaitForLog),
	}

	genericContainerRequest := testcontainers.GenericContainerRequest{
		ContainerRequest: request,
		Started:          true,
	}

	container, err := testcontainers.GenericContainer(ctx, genericContainerRequest)
	if err != nil {
		t.Fatalf("error creating container: %v", err)
	}

	return container
}

func GetContainerAddress(
	ctx context.Context,
	t *testing.T,
	container testcontainers.Container,
	port nat.Port,
) string {
	t.Helper()

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("error getting container host: %v", err)
	}

	mappedPort, err := container.MappedPort(ctx, port)
	if err != nil {
		t.Fatalf("error getting container port: %v", err)
	}

	return net.JoinHostPort(host, mappedPort.Port())
}

func WaitUntilHealthy(ctx context.Context, t *testing.T, url string, maxElapsedTime time.Duration) {
	t.Helper()

	backOff := backoff.NewExponentialBackOff()
	backOff.MaxElapsedTime = maxElapsedTime

	client := &http.Client{}

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		t.Fatalf("error creating request: %v", err)
	}

	retryable := func() error {
		response, err := client.Do(request)
		if err != nil {
			t.Logf("Waiting for container to get healthy, retrying...")

			return fmt.Errorf("error getting response: %w", err)
		}

		defer response.Body.Close()

		return nil
	}

	if err := backoff.Retry(retryable, backOff); err != nil {
		t.Fatalf("healthcheck timed out: %v", err)
	}
}

func TerminateContainer(ctx context.Context, t *testing.T, container testcontainers.Container) {
	t.Helper()

	if err := container.Terminate(ctx); err != nil {
		t.Errorf("error terminating container: %v", err)
	}
}

func RemoveNetwork(ctx context.Context, t *testing.T, network testcontainers.Network) {
	t.Helper()

	if err := network.Remove(ctx); err != nil {
		t.Errorf("error removing network: %v", err)
	}
}
