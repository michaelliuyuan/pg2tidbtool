package logger

import (
	"os"
	"sync"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	extraMu   sync.Mutex
	extraCore zapcore.Core
	baseLevel zapcore.Level
	baseCore  zapcore.Core
)

func RegisterExtraCore(core zapcore.Core) {
	extraMu.Lock()
	defer extraMu.Unlock()
	if extraCore == nil {
		extraCore = core
	} else {
		extraCore = zapcore.NewTee(extraCore, core)
	}
	rebuildLogger()
}

func UnregisterExtraCore() {
	extraMu.Lock()
	defer extraMu.Unlock()
	extraCore = nil
	rebuildLogger()
}

func rebuildLogger() {
	if baseCore == nil {
		return
	}
	var cores []zapcore.Core
	cores = append(cores, baseCore)
	if extraCore != nil {
		cores = append(cores, extraCore)
	}
	core := zapcore.NewTee(cores...)
	logger := zap.New(core, zap.AddCaller(), zap.AddStacktrace(zapcore.ErrorLevel))
	zap.ReplaceGlobals(logger)
}

func Init(level, format string) error {
	return InitWithOutput(level, format, "")
}

func InitWithOutput(level, format string, outputPath string) error {
	var zapLevel zapcore.Level
	if err := zapLevel.UnmarshalText([]byte(level)); err != nil {
		zapLevel = zapcore.InfoLevel
	}
	baseLevel = zapLevel

	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.TimeKey = "ts"
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder
	encoderConfig.EncodeDuration = zapcore.StringDurationEncoder
	encoderConfig.EncodeCaller = zapcore.ShortCallerEncoder

	var encoder zapcore.Encoder
	switch format {
	case "json":
		encoder = zapcore.NewJSONEncoder(encoderConfig)
	default:
		encoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
		encoder = zapcore.NewConsoleEncoder(encoderConfig)
	}

	var cores []zapcore.Core
	cores = append(cores, zapcore.NewCore(
		encoder,
		zapcore.AddSync(os.Stderr),
		zapLevel,
	))

	if outputPath != "" {
		fileEncoder := zapcore.NewJSONEncoder(encoderConfig)
		fileWriter, _, err := zap.Open(outputPath)
		if err == nil {
			cores = append(cores, zapcore.NewCore(
				fileEncoder,
				zapcore.AddSync(fileWriter),
				zapLevel,
			))
		}
	}

	baseCore = zapcore.NewTee(cores...)

	rebuildLogger()

	return nil
}

func Sync() {
	_ = zap.L().Sync()
}
