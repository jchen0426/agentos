package control

import "golang.org/x/sys/unix"

// peerUID is the effective uid of the process that connected to the Unix socket fd, as the kernel
// recorded it at connect().
func peerUID(fd int) (int64, error) {
	cred, err := unix.GetsockoptXucred(fd, unix.SOL_LOCAL, unix.LOCAL_PEERCRED)
	if err != nil {
		return 0, err
	}
	return int64(cred.Uid), nil
}
