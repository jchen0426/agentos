//go:build darwin

package supervisor

import (
	"errors"
	"fmt"
	"syscall"

	"golang.org/x/sys/unix"
)

// sZomb is SZOMB, a zombie's p_stat (sys/proc.h).
const sZomb = 5

// startTime is when process pid started, to the microsecond (kinfo_proc's p_starttime, the wall
// clock at its fork): with its pid, which process it is, whatever it has exec'd since. errGone for
// none, or a zombie.
func startTime(pid int) (string, error) {
	if err := syscall.Kill(pid, 0); errors.Is(err, syscall.ESRCH) {
		return "", errGone
	}
	kp, err := unix.SysctlKinfoProc("kern.proc.pid", pid)
	switch {
	case errors.Is(err, unix.EIO) || errors.Is(err, unix.ESRCH): // an empty answer: no such process
		return "", errGone
	case err != nil:
		return "", err
	case int(kp.Proc.P_pid) != pid || kp.Proc.P_stat == sZomb:
		return "", errGone
	}
	t := kp.Proc.P_starttime
	return fmt.Sprintf("%d.%06d", t.Sec, t.Usec), nil
}
