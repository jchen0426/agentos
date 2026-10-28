//go:build !darwin

package codesnap

import "errors"

// cloneFile clones nothing here: the bytes are copied.
func cloneFile(string, string) error { return errors.ErrUnsupported }
