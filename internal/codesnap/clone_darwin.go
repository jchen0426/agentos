package codesnap

import "golang.org/x/sys/unix"

// cloneFile clones src to dst, a new file: APFS shares their blocks until either changes.
func cloneFile(src, dst string) error {
	return unix.Clonefile(src, dst, unix.CLONE_NOFOLLOW)
}
