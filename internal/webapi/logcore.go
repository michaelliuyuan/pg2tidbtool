package webapi

import (
	"fmt"
	"strings"
	"sync/atomic"

	"go.uber.org/zap/zapcore"
)

type TaskLogCore struct {
	collector *LogCollector
	taskID    string
	enabled   atomic.Bool
	next      zapcore.Core
}

func NewTaskLogCore(collector *LogCollector, taskID string, next zapcore.Core) *TaskLogCore {
	c := &TaskLogCore{
		collector: collector,
		taskID:    taskID,
		next:      next,
	}
	c.enabled.Store(true)
	return c
}

func (c *TaskLogCore) Enable()  { c.enabled.Store(true) }
func (c *TaskLogCore) Disable() { c.enabled.Store(false) }

func (c *TaskLogCore) Enabled(level zapcore.Level) bool {
	if !c.enabled.Load() {
		return false
	}
	if c.next != nil {
		return c.next.Enabled(level)
	}
	return true
}

func (c *TaskLogCore) With(fields []zapcore.Field) zapcore.Core {
	var nextCore zapcore.Core
	if c.next != nil {
		nextCore = c.next.With(fields)
	}
	return &TaskLogCore{
		collector: c.collector,
		taskID:    c.taskID,
		next:      nextCore,
	}
}

func (c *TaskLogCore) Check(entry zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if c.Enabled(entry.Level) {
		ce = ce.AddCore(entry, c)
	}
	if c.next != nil {
		ce = c.next.Check(entry, ce)
	}
	return ce
}

func (c *TaskLogCore) Write(entry zapcore.Entry, fields []zapcore.Field) error {
	caller := entry.Caller.String()
	if idx := strings.LastIndex(caller, "/"); idx >= 0 {
		caller = caller[idx+1:]
	}
	message := entry.Message
	if len(fields) > 0 {
		var parts []string
		for _, f := range fields {
			switch f.Type {
			case zapcore.StringType:
				parts = append(parts, fmt.Sprintf("%s=%s", f.Key, f.String))
			case zapcore.Int64Type, zapcore.Int32Type, zapcore.Uint64Type, zapcore.Uint32Type:
				parts = append(parts, fmt.Sprintf("%s=%d", f.Key, f.Integer))
			case zapcore.BoolType:
				parts = append(parts, fmt.Sprintf("%s=%v", f.Key, f.Integer != 0))
			case zapcore.ErrorType:
				if err, ok := f.Interface.(error); ok {
					parts = append(parts, fmt.Sprintf("%s=%s", f.Key, err.Error()))
				} else {
					parts = append(parts, fmt.Sprintf("%s=%v", f.Key, f.Interface))
				}
			case zapcore.Float64Type:
				parts = append(parts, fmt.Sprintf("%s=%v", f.Key, f.Interface))
			case zapcore.DurationType:
				parts = append(parts, fmt.Sprintf("%s=%v", f.Key, f.Interface))
			default:
				if f.Interface != nil {
					parts = append(parts, fmt.Sprintf("%s=%v", f.Key, f.Interface))
				} else {
					parts = append(parts, fmt.Sprintf("%s=%d", f.Key, f.Integer))
				}
			}
		}
		if len(parts) > 0 {
			message += " (" + strings.Join(parts, ", ") + ")"
		}
	}
	c.collector.Append(c.taskID, ZapLevelString(entry.Level), message, caller)
	return nil
}

func (c *TaskLogCore) Sync() error {
	if c.next != nil {
		return c.next.Sync()
	}
	return nil
}
