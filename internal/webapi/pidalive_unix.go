//go:build !windows

package webapi

import (
	"os"
	"syscall"
)

// pidAlive reports whether a process with the given pid is running. Cross-check
// for CDC liveness: a stale-but-fresh status file whose pid is gone is treated
// as stale. #t48 B.
func pidAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	p, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	return p.Signal(syscall.Signal(0)) == nil
}
