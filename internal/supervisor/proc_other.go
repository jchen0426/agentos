//go:build !darwin && !linux

package supervisor

import "errors"

func startTime(int) (string, error) {
	return "", errors.New("agentd cannot tell when a process started on this platform")
}
