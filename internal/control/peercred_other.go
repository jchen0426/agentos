//go:build !darwin && !linux

package control

import "errors"

// peerUID has no implementation here: agentd runs on macOS and Linux.
func peerUID(int) (int64, error) {
	return 0, errors.New("peer credentials of a Unix socket are read on macOS and Linux only")
}
