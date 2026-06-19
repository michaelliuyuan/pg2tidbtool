//go:build !windows

package webapi

import (
	"os"
	"syscall"
)

// terminateProc sends SIGTERM for a graceful shutdown (the CDC runner catches
// SIGINT/SIGTERM and exits cleanly). Stop escalates to Kill after the grace
// period. Unix-only; the Windows build tags this out.
func terminateProc(p *os.Process) error {
	return p.Signal(syscall.SIGTERM)
}
