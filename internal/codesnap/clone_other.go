//go:build !darwin

package codesnap

import (
	"errors"
	"os"
)

// cloneFile clones nothing here: the bytes are copied.
func cloneFile(*os.File, string) error { return errors.ErrUnsupported }
