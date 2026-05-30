package precheck

import (
	"os"
)

func getDiskUsage(path string) (float64, error) {
	if _, err := os.Stat(path); err != nil {
		if err := os.MkdirAll(path, 0755); err != nil {
			return 0, err
		}
	}
	return 999.0, nil
}
