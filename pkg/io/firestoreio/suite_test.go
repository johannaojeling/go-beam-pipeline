package firestoreio

import (
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/stretchr/testify/suite"

	"github.com/johannaojeling/go-beam-pipeline/pkg/internal/testutils"
)

const TestProjectId = "test-project"
const EmulatorHost = "FIRESTORE_EMULATOR_HOST"
const EmulatorRunningMessage = "Dev App Server is now running"
const EmulatorFlushEndpoint = "http://%s/emulator/v1/projects/%s/databases/(default)/documents"

type Suite struct {
	suite.Suite
	host string
	cmd  *exec.Cmd
}

func (s *Suite) SetupSuite() {
	port, err := testutils.FindLocalPort()
	if err != nil {
		s.T().Fatalf("error finding local port: %v", err)
	}

	host := net.JoinHostPort("localhost", strconv.Itoa(port))
	s.T().Logf("setting %s to %s", EmulatorHost, host)

	err = os.Setenv(EmulatorHost, host)
	if err != nil {
		s.T().Fatalf("error setting %s: %v", EmulatorHost, err)
	}
	s.host = host

	s.cmd = exec.Command("gcloud", "beta", "emulators", "firestore", "start", "--host-port", host)
	stderr, err := s.cmd.StderrPipe()
	if err != nil {
		s.T().Fatalf("error setting stderr: %v", err)
	}

	if err = s.cmd.Start(); err != nil {
		s.T().Fatalf("error starting process with emulator: %v", err)
	}
	s.T().Logf("started process with pid %v", s.cmd.Process.Pid)

	s.cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	waitUntilRunning(stderr)
}

func waitUntilRunning(stderr io.ReadCloser) {
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		readUntilRunning(stderr, &wg)
	}()

	wg.Wait()
}

func readUntilRunning(stderr io.ReadCloser, wg *sync.WaitGroup) {
	data := make([]byte, 512)
	for {
		n, err := stderr.Read(data)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("error reading stderr %v", err)
		}

		if n > 0 {
			output := string(data[:n])
			log.Print(output)

			if strings.Contains(output, EmulatorRunningMessage) {
				wg.Done()
			}
		}
	}
}

func (s *Suite) TearDownSuite() {
	s.T().Logf("killing process with pid %v", s.cmd.Process.Pid)
	err := syscall.Kill(-s.cmd.Process.Pid, syscall.SIGKILL)
	if err != nil {
		s.T().Fatalf("error killing process for emulator: %v", err)
	}

	s.T().Logf("unsetting %s", EmulatorHost)
	err = os.Unsetenv(EmulatorHost)
	if err != nil {
		s.T().Fatalf("error unsetting %s: %v", EmulatorHost, err)
	}
}

func (s *Suite) TearDownTest() {
	url := fmt.Sprintf(EmulatorFlushEndpoint, s.host, TestProjectId)
	request, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		s.T().Fatalf("error creating http DELETE request: %v", err)
	}

	client := new(http.Client)
	response, err := client.Do(request)
	if err != nil {
		s.T().Fatalf("error performing http DELETE operation: %v", err)
	}

	status := response.Status
	expected := "200 OK"
	if status != expected {
		s.T().Fatalf("expected status %q but was: %q", expected, status)
	}
}
