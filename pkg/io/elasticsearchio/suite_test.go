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

const ESVersion = "8.3.1"

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

	url, err := getContainerUrl(ctx, container)
	if err != nil {
		s.T().Fatalf("error getting container url: %v", err)
	}

	err = waitUntilHealthy(url)
	if err != nil {
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
	return testcontainers.GenericNetwork(ctx, genericNetworkRequest)
}

func createContainer(ctx context.Context, networkName string) (testcontainers.Container, error) {
	containerRequest := testcontainers.ContainerRequest{
		Image: fmt.Sprintf("elasticsearch:%s", ESVersion),
		Env: map[string]string{
			"discovery.type":         "single-node",
			"xpack.security.enabled": "false",
		},
		ExposedPorts: []string{"9200/tcp"},
		Networks:     []string{networkName},
		WaitingFor:   wait.ForLog("started"),
	}
	genericContainerRequest := testcontainers.GenericContainerRequest{
		ContainerRequest: containerRequest,
		Started:          true,
	}
	return testcontainers.GenericContainer(ctx, genericContainerRequest)
}

func getContainerUrl(ctx context.Context, container testcontainers.Container) (string, error) {
	host, err := container.Host(ctx)
	if err != nil {
		return "", fmt.Errorf("error getting container host: %v", err)
	}

	port, err := container.MappedPort(ctx, "9200")
	if err != nil {
		return "", fmt.Errorf("error getting container port: %v", err)
	}

	url := fmt.Sprintf("http://%s:%s", host, port.Port())
	return url, nil
}

func waitUntilHealthy(url string) error {
	healthUrl := url + "/_cluster/health"

	backOff := backoff.NewExponentialBackOff()
	backOff.MaxElapsedTime = 30 * time.Second

	retryable := func() error {
		_, err := http.Get(healthUrl)
		if err != nil {
			log.Printf("Waiting for Elasticsearch to get healthy, retrying...")
		}
		return err
	}

	err := backoff.Retry(retryable, backOff)
	if err != nil {
		return fmt.Errorf("timed out: %v", err)
	}
	return nil
}

func (s *Suite) TearDownSuite() {
	ctx := context.Background()
	err := s.container.Terminate(ctx)
	if err != nil {
		s.T().Errorf("error terminating container: %v", err)
	}

	err = s.network.Remove(ctx)
	if err != nil {
		s.T().Errorf("error removing netowrk: %v", err)
	}
}

func (s *Suite) TearDownTest(ctx context.Context, client *elasticsearch.Client, index string) {
	err := esutils.DeleteIndices(ctx, client, []string{index})
	if err != nil {
		s.T().Fatalf("error deleting index %q: %v", index, err)
	}
}
