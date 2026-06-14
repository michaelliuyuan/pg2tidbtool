//go:build windows

package webapi

// pidAlive is a no-op on Windows (no portable signal-0 probe); freshness alone
// drives stale detection there. #t48 B.
func pidAlive(pid int) bool {
	return true
}
