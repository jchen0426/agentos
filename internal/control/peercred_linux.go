package control

import "golang.org/x/sys/unix"

// peerUID is the uid of the process that connected to the Unix socket fd, as the kernel recorded
// it at connect().
func peerUID(fd int) (int64, error) {
	cred, err := unix.GetsockoptUcred(fd, unix.SOL_SOCKET, unix.SO_PEERCRED)
	if err != nil {
		return 0, err
	}
	return int64(cred.Uid), nil
}
