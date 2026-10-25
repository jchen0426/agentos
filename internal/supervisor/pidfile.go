package supervisor

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"syscall"
	"time"
)

// errGone is the answer of startTime for a process that is not there, or is a zombie.
var errGone = errors.New("no such process")

// pidRecord is a pid file: the launcher of the generation running, by its pid, which is also its
// session's and its process group's, and when it started (startTime), which tells it from another
// process that has its pid later.
type pidRecord struct {
	PID        int    `json:"pid"`
	Start      string `json:"start"`
	Generation int    `json:"generation"`
}

// writePIDFile records pid, a child of this process not reaped yet: its pid is no one else's while
// it is read.
func writePIDFile(path string, pid, generation int) error {
	start, err := startTime(pid)
	if err != nil {
		return err
	}
	b, err := json.Marshal(pidRecord{PID: pid, Start: start, Generation: generation})
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func removePIDFile(path string) {
	_ = os.Remove(path)
}

// sweepWait bounds how long Sweep waits for a Worker it killed to be gone.
var sweepWait = 5 * time.Second

// Sweep stops the Worker the last agentd on this data directory left running, if it was killed
// before it could: the launcher the pid file at path names, if that process still runs and started
// when the file says, with its whole process group. agentd calls it once it holds the data
// directory's lock, before it listens: a Worker another agentd still runs is never touched, and a
// Worker left behind never reaches this one. A pid another process has now, or one that is gone,
// only has its file removed.
func Sweep(path string, log *slog.Logger) error {
	b, err := os.ReadFile(path)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return nil
	case err != nil:
		return fmt.Errorf("the last Worker's pid file: %w", err)
	}
	var r pidRecord
	if err := json.Unmarshal(b, &r); err != nil || r.PID <= 1 || r.Start == "" {
		log.Warn("removed a pid file agentd cannot read: if the last agentd's Worker still runs, stop it yourself", "path", path)
		return remove(path)
	}
	switch start, err := startTime(r.PID); {
	case errors.Is(err, errGone):
		log.Info("removed the last Worker's pid file: it is gone", "pid", r.PID)
		return remove(path)
	case err != nil:
		return fmt.Errorf("the last Worker's pid file names pid %d: %w", r.PID, err)
	case start != r.Start:
		log.Info("removed the last Worker's pid file: another process has its pid now", "pid", r.PID)
		return remove(path)
	}
	if err := syscall.Kill(-r.PID, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
		return fmt.Errorf("stop the Worker the last agentd left running (pid %d): %w", r.PID, err)
	}
	_ = syscall.Kill(r.PID, syscall.SIGKILL)
	for deadline := time.Now().Add(sweepWait); ; time.Sleep(20 * time.Millisecond) {
		start, err := startTime(r.PID)
		if errors.Is(err, errGone) || err == nil && start != r.Start {
			break
		}
		if time.Now().After(deadline) {
			log.Warn("the Worker the last agentd left running has not ended yet", "pid", r.PID, "after", sweepWait.String())
			break
		}
	}
	log.Warn("stopped the Worker the last agentd on this data directory left running", "pid", r.PID, "generation", r.Generation)
	return remove(path)
}

func remove(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}
