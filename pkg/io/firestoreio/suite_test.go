package firestoreio

import (
	"context"
	"fmt"

	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"

	"github.com/johannaojeling/go-beam-pipeline/pkg/internal/testutils"
)

const (
	emulatorProject = "test-project"
	emulatorImage   = "mtlynch/firestore-emulator"
	emulatorPort    = "8080"
	emulatorHostVar = "FIRESTORE_EMULATOR_HOST"
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
		"GOOGLE_CLOUD_PROJECT": emulatorProject,
	}
	cfg := testutils.ContainerConfig{
		Image:      emulatorImage,
		Env:        env,
		Ports:      []string{emulatorPort + "/tcp"},
		WaitForLog: "Dev App Server is now running",
	}

	s.container = testutils.CreateContainer(s.ctx, s.T(), cfg)

	address := testutils.GetContainerAddress(s.ctx, s.T(), s.container, emulatorPort)
	s.URL = fmt.Sprintf(
		"http://%s/emulator/v1/projects/%s/databases/(default)/documents",
		address,
		emulatorProject,
	)

	s.T().Logf("Setting %s to %q", emulatorHostVar, s.URL)
	s.T().Setenv("FIRESTORE_EMULATOR_HOST", address)
}

func (s *Suite) TearDownSuite() {
	testutils.TerminateContainer(s.ctx, s.T(), s.container)
}
