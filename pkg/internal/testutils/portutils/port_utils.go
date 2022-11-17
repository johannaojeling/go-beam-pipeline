package portutils

import (
	"fmt"
	"net"
)

func FindLocalPort() (int, error) {
	listener, err := net.Listen("tcp", ":0") //nolint: gosec
	if err != nil {
		return 0, fmt.Errorf("error listening: %w", err)
	}

	defer listener.Close()

	tcpAddr, ok := listener.Addr().(*net.TCPAddr)
	if !ok {
		return 0, fmt.Errorf("unexpected address type: %T", listener.Addr())
	}

	return tcpAddr.Port, nil
}
