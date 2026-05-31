package lightning

import (
	"crypto/sha256"
	_ "embed"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"

	"go.uber.org/zap"
)

//go:embed tidb-lightning
var lightningBinary []byte

var (
	extractOnce   sync.Once
	extractedPath string
	extractErr    error
)

func EnsureExtracted(dataDir string) (string, error) {
	extractOnce.Do(func() {
		extractedPath, extractErr = extract(dataDir)
	})
	return extractedPath, extractErr
}

func extract(dataDir string) (string, error) {
	logger := zap.L()

	if len(lightningBinary) == 0 || (len(lightningBinary) < 100 && string(lightningBinary) == "placeholder\n") {
		logger.Warn("no embedded tidb-lightning binary available")
		return "", fmt.Errorf("no embedded tidb-lightning binary")
	}

	targetDir := filepath.Join(dataDir, ".lightning")
	if err := os.MkdirAll(targetDir, 0755); err != nil {
		return "", fmt.Errorf("create lightning dir: %w", err)
	}

	targetPath := filepath.Join(targetDir, "tidb-lightning")

	needWrite := true
	if info, err := os.Stat(targetPath); err == nil && info.Mode()&0111 != 0 {
		if existing, err := os.ReadFile(targetPath); err == nil {
			existingHash := sha256.Sum256(existing)
			embeddedHash := sha256.Sum256(lightningBinary)
			if existingHash == embeddedHash {
				needWrite = false
				logger.Info("embedded tidb-lightning already extracted and up-to-date",
					zap.String("path", targetPath))
			}
		}
	}

	if needWrite {
		f, err := os.OpenFile(targetPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0755)
		if err != nil {
			return "", fmt.Errorf("create lightning binary file: %w", err)
		}
		defer f.Close()

		if _, err := f.Write(lightningBinary); err != nil {
			os.Remove(targetPath)
			return "", fmt.Errorf("write lightning binary: %w", err)
		}
		f.Close()

		os.Chmod(targetPath, 0755)
		logger.Info("extracted embedded tidb-lightning binary",
			zap.String("path", targetPath),
			zap.Int("size", len(lightningBinary)))
	}

	return targetPath, nil
}

func FindBinary(dataDir string) string {
	logger := zap.L()

	if path, err := exec.LookPath("tidb-lightning"); err == nil {
		logger.Info("found tidb-lightning in PATH", zap.String("path", path))
		return path
	}

	if path, err := EnsureExtracted(dataDir); err == nil {
		logger.Info("using embedded tidb-lightning", zap.String("path", path))
		return path
	}

	logger.Warn("tidb-lightning not found in PATH and no embedded binary available")
	return ""
}

func HasEmbedded() bool {
	return len(lightningBinary) > 0 && !(len(lightningBinary) < 100 && string(lightningBinary) == "placeholder\n")
}

func EmbeddedSize() int {
	return len(lightningBinary)
}
