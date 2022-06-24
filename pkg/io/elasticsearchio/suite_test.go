package elasticsearchio

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/johannaojeling/go-beam-pipeline/pkg/internal/testutils/esutils"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
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
		s.T().Fatalf("failed to create network: %v", err)
	}
	s.network = network

	container, err := createContainer(ctx, networkName)
	if err != nil {
		s.T().Fatalf("failed to create Elasticsearch container: %v", err)
	}
	s.container = container

	url, err := getContainerUrl(ctx, container)
	if err != nil {
		s.T().Fatalf("failed to get container url: %v", err)
	}

	err = waitUntilHealthy(url)
	if err != nil {
		s.T().Fatalf("failed to wait for Elasticsearch to get into a healthy state: %v", err)
	}
	s.URL = url
}

func getContainerUrl(ctx context.Context, container testcontainers.Container) (string, error) {
	host, err := container.Host(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to get container host: %v", err)
	}

	port, err := container.MappedPort(ctx, "9200")
	if err != nil {
		return "", fmt.Errorf("failed to get container port: %v", err)
	}

	url := fmt.Sprintf("http://%s:%s", host, port.Port())
	return url, nil
}

func createContainer(ctx context.Context, networkName string) (testcontainers.Container, error) {
	containerRequest := testcontainers.ContainerRequest{
		Image: "elasticsearch:8.2.2",
		Env: map[string]string{
			"discovery.type":         "single-node",
			"xpack.security.enabled": "false",
			"ES_JAVA_OPTS":           "-Xms1g -Xmx1g",
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

func waitUntilHealthy(url string) error {
	healthUrl := url + "/_cluster/health"
	retries := 0
	maxRetries := 5
	delay := 1

	for {
		_, err := http.Get(healthUrl)
		if err == nil {
			break
		}
		if retries > maxRetries {
			return errors.New("timed out")
		}
		log.Printf("Waiting for Elasticsearch to get healthy, sleeping for %d second(s)...", delay)
		time.Sleep(time.Duration(delay) * time.Second)
		retries++
		delay = delay * 2
	}
	return nil
}

func (s *Suite) TearDownSuite() {
	ctx := context.Background()
	err := s.container.Terminate(ctx)
	if err != nil {
		s.T().Errorf("failed to terminate container: %v", err)
	}

	err = s.network.Remove(ctx)
	if err != nil {
		s.T().Errorf("failed to remove netowrk: %v", err)
	}
}

func (s *Suite) TearDownTest(ctx context.Context, client *elasticsearch.Client, index string) {
	err := esutils.DeleteIndices(ctx, client, []string{index})
	if err != nil {
		s.T().Fatalf("failed to delete index %q: %v", index, err)
	}
}
