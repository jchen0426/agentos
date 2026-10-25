//go:build linux

package supervisor

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strings"
	"syscall"
)

// startTime is when process pid started: /proc/<pid>/stat's starttime, in clock ticks since boot,
// with the boot's ID, since the ticks start over at each boot. With its pid, it tells which process
// it is, whatever it has exec'd since. errGone for none, or a zombie.
func startTime(pid int) (string, error) {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	switch {
	case errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ESRCH):
		return "", errGone
	case err != nil:
		return "", err
	}
	// The command name, in parentheses, may hold anything: the fields after it are numbers.
	i := bytes.LastIndexByte(b, ')')
	if i < 0 {
		return "", fmt.Errorf("/proc/%d/stat: %q", pid, b)
	}
	f := strings.Fields(string(b[i+1:]))
	if len(f) < 20 {
		return "", fmt.Errorf("/proc/%d/stat: %q", pid, b)
	}
	if f[0] == "Z" || f[0] == "X" { // field 3, the state
		return "", errGone
	}
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(boot)) + ":" + f[19], nil // field 22, starttime
}
