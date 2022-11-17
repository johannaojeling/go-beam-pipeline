package elasticsearchio

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/johannaojeling/go-beam-pipeline/pkg/internal/testutils/esutils"
)

const (
	esImage = "elasticsearch:8.3.1"
	esPort  = "9200"
)

type Suite struct {
	suite.Suite
	container testcontainers.Container
	network   testcontainers.Network
	URL       string
}

func (s *Suite) SetupSuite() {
	ctx := context.Background()
	networkName := "test-network"

	network, err := createNetwork(ctx, networkName)
	if err != nil {
		s.T().Fatalf("error creating network: %v", err)
	}

	s.network = network

	container, err := createContainer(ctx, networkName)
	if err != nil {
		s.T().Fatalf("error creating Elasticsearch container: %v", err)
	}

	s.container = container

	url, err := getContainerURL(ctx, container)
	if err != nil {
		s.T().Fatalf("error getting container url: %v", err)
	}

	if err := waitUntilHealthy(ctx, url); err != nil {
		s.T().Fatalf("error waiting for Elasticsearch to get into a healthy state: %v", err)
	}

	s.URL = url
}

func createNetwork(ctx context.Context, name string) (testcontainers.Network, error) {
	networkRequest := testcontainers.NetworkRequest{
		Name:           name,
		CheckDuplicate: true,
	}
	genericNetworkRequest := testcontainers.GenericNetworkRequest{
		NetworkRequest: networkRequest,
	}

	network, err := testcontainers.GenericNetwork(ctx, genericNetworkRequest)
	if err != nil {
		return nil, fmt.Errorf("error creating network: %w", err)
	}

	return network, nil
}

func createContainer(ctx context.Context, networkName string) (testcontainers.Container, error) {
	containerRequest := testcontainers.ContainerRequest{
		Image: esImage,
		Env: map[string]string{
			"discovery.type":         "single-node",
			"xpack.security.enabled": "false",
		},
		ExposedPorts: []string{esPort + "/tcp"},
		Networks:     []string{networkName},
		WaitingFor:   wait.ForLog("started"),
	}
	genericContainerRequest := testcontainers.GenericContainerRequest{
		ContainerRequest: containerRequest,
		Started:          true,
	}

	container, err := testcontainers.GenericContainer(ctx, genericContainerRequest)
	if err != nil {
		return nil, fmt.Errorf("error creating container: %w", err)
	}

	return container, nil
}

func getContainerURL(ctx context.Context, container testcontainers.Container) (string, error) {
	host, err := container.Host(ctx)
	if err != nil {
		return "", fmt.Errorf("error getting container host: %w", err)
	}

	port, err := container.MappedPort(ctx, esPort)
	if err != nil {
		return "", fmt.Errorf("error getting container port: %w", err)
	}

	url := fmt.Sprintf("http://%s:%s", host, port.Port())

	return url, nil
}

func waitUntilHealthy(ctx context.Context, url string) error {
	healthURL := url + "/_cluster/health"

	backOff := backoff.NewExponentialBackOff()
	backOff.MaxElapsedTime = 30 * time.Second

	client := &http.Client{}

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, healthURL, http.NoBody)
	if err != nil {
		return fmt.Errorf("error creating request: %w", err)
	}

	retryable := func() error {
		response, err := client.Do(request)
		if err != nil {
			log.Printf("Waiting for Elasticsearch to get healthy, retrying...")

			return fmt.Errorf("error getting response: %w", err)
		}

		defer response.Body.Close()

		return nil
	}

	if err := backoff.Retry(retryable, backOff); err != nil {
		return fmt.Errorf("timed out: %w", err)
	}

	return nil
}

func (s *Suite) TearDownSuite() {
	ctx := context.Background()
	if err := s.container.Terminate(ctx); err != nil {
		s.T().Errorf("error terminating container: %v", err)
	}

	if err := s.network.Remove(ctx); err != nil {
		s.T().Errorf("error removing network: %v", err)
	}
}

func (s *Suite) TearDownTest(ctx context.Context, client *elasticsearch.Client, index string) {
	err := esutils.DeleteIndices(ctx, client, []string{index})
	if err != nil {
		s.T().Fatalf("error deleting index %q: %v", index, err)
	}
}
