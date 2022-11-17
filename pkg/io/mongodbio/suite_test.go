package mongodbio

import (
	"context"
	"fmt"

	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"

	"github.com/johannaojeling/go-beam-pipeline/pkg/internal/testutils"
)

const (
	mongoImage = "mongo:6.0.2"
	mongoPort  = "27017"
	mongoUser  = "testuser"
	mongoPwd   = "pwd"
)

type Suite struct {
	suite.Suite
	ctx       context.Context
	container testcontainers.Container
	URL       string
}

func (s *Suite) SetupSuite() {
	s.ctx = context.Background()

	env := map[string]string{
		"MONGO_INITDB_ROOT_USERNAME": mongoUser,
		"MONGO_INITDB_ROOT_PASSWORD": mongoPwd,
	}
	cfg := testutils.ContainerConfig{
		Image:      mongoImage,
		Env:        env,
		Ports:      []string{mongoPort + "/tcp"},
		WaitForLog: "started",
	}

	s.container = testutils.CreateContainer(s.ctx, s.T(), cfg)

	address := testutils.GetContainerAddress(s.ctx, s.T(), s.container, mongoPort)
	s.URL = fmt.Sprintf("mongodb://%s:%s@%s", mongoUser, mongoPwd, address)
}

func (s *Suite) TearDownSuite() {
	testutils.TerminateContainer(s.ctx, s.T(), s.container)
}
