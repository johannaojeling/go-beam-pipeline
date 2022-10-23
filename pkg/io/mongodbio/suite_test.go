package mongodbio

import (
	"context"
	"fmt"

	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/johannaojeling/go-beam-pipeline/pkg/internal/testutils/mongodbutils"
)

const (
	mongoImage = "mongo:6.0.2"
	mongoPort  = "27017"
	mongoUser  = "testuser"
	mongoPwd   = "pwd"
)

type Suite struct {
	suite.Suite
	container testcontainers.Container
	URL       string
}

func (s *Suite) SetupSuite() {
	ctx := context.Background()
	container, err := createContainer(ctx)
	if err != nil {
		s.T().Fatalf("error creating MongoDB container: %v", err)
	}
	s.container = container

	url, err := getContainerUrl(ctx, container)
	if err != nil {
		s.T().Fatalf("error getting container url: %v", err)
	}
	s.URL = url
}

func createContainer(ctx context.Context) (testcontainers.Container, error) {
	containerRequest := testcontainers.ContainerRequest{
		Image: mongoImage,
		Env: map[string]string{
			"MONGO_INITDB_ROOT_USERNAME": mongoUser,
			"MONGO_INITDB_ROOT_PASSWORD": mongoPwd,
		},
		ExposedPorts: []string{mongoPort + "/tcp"},
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

	port, err := container.MappedPort(ctx, mongoPort)
	if err != nil {
		return "", fmt.Errorf("error getting container port: %v", err)
	}

	url := fmt.Sprintf("mongodb://%s:%s@%s:%s", mongoUser, mongoPwd, host, port.Port())
	return url, nil
}

func (s *Suite) TearDownSuite() {
	ctx := context.Background()
	err := s.container.Terminate(ctx)
	if err != nil {
		s.T().Errorf("error terminating container: %v", err)
	}
}

func (s *Suite) TearDownTest(ctx context.Context, collection *mongo.Collection) {
	err := mongodbutils.DropCollection(ctx, collection)
	if err != nil {
		s.T().Fatal(err.Error())
	}
}
