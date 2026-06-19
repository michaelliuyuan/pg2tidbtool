//go:build windows

package webapi

import "os"

// terminateProc on Windows has no SIGTERM equivalent, so it force-kills. The
// CDC runner's graceful path is Unix-only; on Windows Stop ends the child
// immediately (the READ channel then shows not_running).
func terminateProc(p *os.Process) error {
	return p.Kill()
}
