package codesnap

import (
	"os"

	"golang.org/x/sys/unix"
)

// cloneFile clones the file src is open on to dst, a new file: APFS shares their blocks until either
// changes. It clones by descriptor, so what src names cannot be swapped meanwhile.
func cloneFile(src *os.File, dst string) error {
	return unix.Fclonefileat(int(src.Fd()), unix.AT_FDCWD, dst, 0)
}
