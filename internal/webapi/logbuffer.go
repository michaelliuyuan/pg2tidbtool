package webapi

import (
	"sync"
	"time"

	"go.uber.org/zap/zapcore"
)

type TaskLogEntry struct {
	Timestamp time.Time `json:"timestamp"`
	Level     string    `json:"level"`
	Message   string    `json:"message"`
	Caller    string    `json:"caller,omitempty"`
}

type TaskLogBuffer struct {
	mu      sync.Mutex
	logs    []TaskLogEntry
	maxSize int
	subs    map[chan TaskLogEntry]struct{}
}

func NewTaskLogBuffer(maxSize int) *TaskLogBuffer {
	if maxSize <= 0 {
		maxSize = 1000
	}
	return &TaskLogBuffer{
		logs:    make([]TaskLogEntry, 0, maxSize),
		maxSize: maxSize,
		subs:    make(map[chan TaskLogEntry]struct{}),
	}
}

func (b *TaskLogBuffer) Append(entry TaskLogEntry) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.logs) >= b.maxSize {
		b.logs = b.logs[1:]
	}
	b.logs = append(b.logs, entry)

	for ch := range b.subs {
		select {
		case ch <- entry:
		default:
		}
	}
}

func (b *TaskLogBuffer) GetAll() []TaskLogEntry {
	b.mu.Lock()
	defer b.mu.Unlock()
	result := make([]TaskLogEntry, len(b.logs))
	copy(result, b.logs)
	return result
}

func (b *TaskLogBuffer) GetSince(since time.Time) []TaskLogEntry {
	b.mu.Lock()
	defer b.mu.Unlock()
	var result []TaskLogEntry
	for _, entry := range b.logs {
		if entry.Timestamp.After(since) {
			result = append(result, entry)
		}
	}
	return result
}

func (b *TaskLogBuffer) Subscribe() chan TaskLogEntry {
	b.mu.Lock()
	defer b.mu.Unlock()
	ch := make(chan TaskLogEntry, 64)
	b.subs[ch] = struct{}{}
	return ch
}

func (b *TaskLogBuffer) Unsubscribe(ch chan TaskLogEntry) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.subs, ch)
	close(ch)
}

type LogCollector struct {
	mu      sync.Mutex
	buffers map[string]*TaskLogBuffer
}

func NewLogCollector() *LogCollector {
	return &LogCollector{
		buffers: make(map[string]*TaskLogBuffer),
	}
}

func (c *LogCollector) GetBuffer(taskID string) *TaskLogBuffer {
	c.mu.Lock()
	defer c.mu.Unlock()
	if buf, ok := c.buffers[taskID]; ok {
		return buf
	}
	buf := NewTaskLogBuffer(1000)
	c.buffers[taskID] = buf
	return buf
}

func (c *LogCollector) Append(taskID string, level string, message string, caller string) {
	buf := c.GetBuffer(taskID)
	buf.Append(TaskLogEntry{
		Timestamp: time.Now(),
		Level:     level,
		Message:   message,
		Caller:    caller,
	})
}

func (c *LogCollector) RemoveBuffer(taskID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.buffers, taskID)
}

func ZapLevelString(l zapcore.Level) string {
	switch l {
	case zapcore.DebugLevel:
		return "DEBUG"
	case zapcore.InfoLevel:
		return "INFO"
	case zapcore.WarnLevel:
		return "WARN"
	case zapcore.ErrorLevel:
		return "ERROR"
	case zapcore.DPanicLevel, zapcore.PanicLevel, zapcore.FatalLevel:
		return "FATAL"
	default:
		return "INFO"
	}
}
