package checkpoint

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type State string

const (
	StatePending   State = "pending"
	StateRunning   State = "running"
	StateCompleted State = "completed"
	StateFailed    State = "failed"
	StateSkipped   State = "skipped"
)

type TableCheckpoint struct {
	TableName  string                   `json:"table_name"`
	State      State                    `json:"state"`
	RowsDone   int64                    `json:"rows_done"`
	RowsTotal  int64                    `json:"rows_total"`
	BytesDone  int64                    `json:"bytes_done"`
	StartedAt  time.Time                `json:"started_at"`
	UpdatedAt  time.Time                `json:"updated_at"`
	FinishedAt time.Time                `json:"finished_at,omitempty"`
	Error      string                   `json:"error,omitempty"`
	Chunks     map[int]*ChunkCheckpoint `json:"chunks,omitempty"`
}

type ChunkCheckpoint struct {
	Index      int       `json:"index"`
	State      State     `json:"state"`
	RowCount   int64     `json:"row_count"`
	ByteCount  int64     `json:"byte_count"`
	StartedAt  time.Time `json:"started_at,omitempty"`
	FinishedAt time.Time `json:"finished_at,omitempty"`
	Error      string    `json:"error,omitempty"`
}

func (tc *TableCheckpoint) Progress() float64 {
	if tc.RowsTotal <= 0 {
		return 0
	}
	p := float64(tc.RowsDone) / float64(tc.RowsTotal)
	if p > 1.0 {
		p = 1.0
	}
	return p
}

type Checkpoint struct {
	Version   string                      `json:"version"`
	CreatedAt time.Time                   `json:"created_at"`
	UpdatedAt time.Time                   `json:"updated_at"`
	Phase     string                      `json:"phase"`
	Tables    map[string]*TableCheckpoint `json:"tables"`
}

type Manager struct {
	mu       sync.Mutex
	dir      string
	filePath string
	data     *Checkpoint

	saveMu sync.Mutex
	dirty  bool
	saveCh chan struct{}
}

func NewManager(dir string) (*Manager, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create checkpoint dir: %w", err)
	}
	fp := filepath.Join(dir, "checkpoint.json")
	m := &Manager{
		dir:      dir,
		filePath: fp,
		data: &Checkpoint{
			Version:   "1.0",
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
			Tables:    make(map[string]*TableCheckpoint),
		},
		saveCh: make(chan struct{}, 1),
	}
	if err := m.load(); err != nil {
		return nil, err
	}
	go m.asyncSaver()
	return m, nil
}

func NewReadOnlyManager(dir string) (*Manager, error) {
	fp := filepath.Join(dir, "checkpoint.json")
	m := &Manager{
		dir:      dir,
		filePath: fp,
		data: &Checkpoint{
			Version:   "1.0",
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
			Tables:    make(map[string]*TableCheckpoint),
		},
	}
	if err := m.load(); err != nil {
		if os.IsNotExist(err) {
			return m, nil
		}
		return nil, err
	}
	return m, nil
}

func (m *Manager) asyncSaver() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-m.saveCh:
			m.doSave()
		case <-ticker.C:
			m.doSave()
		}
	}
}

func (m *Manager) doSave() {
	m.saveMu.Lock()
	defer m.saveMu.Unlock()
	if !m.dirty {
		return
	}
	m.mu.Lock()
	m.data.UpdatedAt = time.Now()
	data, err := json.MarshalIndent(m.data, "", "  ")
	m.mu.Unlock()
	if err != nil {
		return
	}
	tmp := m.filePath + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return
	}
	if err := os.Rename(tmp, m.filePath); err != nil {
		return
	}
	m.dirty = false
}

func (m *Manager) markDirty() {
	m.dirty = true
	select {
	case m.saveCh <- struct{}{}:
	default:
	}
}

func (m *Manager) Flush() {
	m.saveMu.Lock()
	defer m.saveMu.Unlock()
	m.mu.Lock()
	m.data.UpdatedAt = time.Now()
	data, err := json.MarshalIndent(m.data, "", "  ")
	m.mu.Unlock()
	if err != nil {
		return
	}
	tmp := m.filePath + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return
	}
	_ = os.Rename(tmp, m.filePath)
	m.dirty = false
}

func (m *Manager) load() error {
	data, err := os.ReadFile(m.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read checkpoint: %w", err)
	}
	return json.Unmarshal(data, m.data)
}

func (m *Manager) GetOrCreateTable(tableName string, totalRows int64) *TableCheckpoint {
	m.mu.Lock()
	defer m.mu.Unlock()

	if tc, ok := m.data.Tables[tableName]; ok {
		return tc
	}
	tc := &TableCheckpoint{
		TableName: tableName,
		State:     StatePending,
		RowsTotal: totalRows,
	}
	m.data.Tables[tableName] = tc
	m.markDirty()
	return tc
}

func (m *Manager) UpdateTable(tableName string, fn func(tc *TableCheckpoint)) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	tc, ok := m.data.Tables[tableName]
	if !ok {
		return fmt.Errorf("table %s not found in checkpoint", tableName)
	}
	fn(tc)
	m.markDirty()
	return nil
}

func (m *Manager) MarkTableRunning(tableName string) error {
	return m.UpdateTable(tableName, func(tc *TableCheckpoint) {
		tc.State = StateRunning
		tc.StartedAt = time.Now()
	})
}

func (m *Manager) MarkTableCompleted(tableName string, rowsDone int64) error {
	return m.UpdateTable(tableName, func(tc *TableCheckpoint) {
		tc.State = StateCompleted
		tc.RowsDone = rowsDone
		tc.FinishedAt = time.Now()
	})
}

func (m *Manager) MarkTableFailed(tableName string, errStr string) error {
	return m.UpdateTable(tableName, func(tc *TableCheckpoint) {
		tc.State = StateFailed
		tc.Error = errStr
		tc.FinishedAt = time.Now()
	})
}

func (m *Manager) UpdateTableProgress(tableName string, rowsDone int64, bytesDone int64) error {
	return m.UpdateTable(tableName, func(tc *TableCheckpoint) {
		tc.RowsDone = rowsDone
		tc.BytesDone = bytesDone
	})
}

func (m *Manager) GetPhase() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.data.Phase
}

func (m *Manager) SetPhase(phase string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	_ = m.load()
	m.data.Phase = phase
	m.markDirty()
	return nil
}

func (m *Manager) IsTableCompleted(tableName string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	tc, ok := m.data.Tables[tableName]
	return ok && tc.State == StateCompleted
}

func (m *Manager) GetPendingTables() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var tables []string
	for name, tc := range m.data.Tables {
		if tc.State != StateCompleted {
			tables = append(tables, name)
		}
	}
	return tables
}

func (m *Manager) GetTable(tableName string) (*TableCheckpoint, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	tc, ok := m.data.Tables[tableName]
	return tc, ok
}

func (m *Manager) GetAllTables() map[string]*TableCheckpoint {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make(map[string]*TableCheckpoint, len(m.data.Tables))
	for k, v := range m.data.Tables {
		result[k] = v
	}
	return result
}

func (m *Manager) ResetAllTables() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, tc := range m.data.Tables {
		tc.State = StatePending
		tc.RowsDone = 0
		tc.BytesDone = 0
	}
	m.markDirty()
}

func (m *Manager) Summary() (completed, failed, pending, running int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, tc := range m.data.Tables {
		switch tc.State {
		case StateCompleted:
			completed++
		case StateFailed:
			failed++
		case StateRunning:
			running++
		default:
			pending++
		}
	}
	return
}

func (m *Manager) Reset() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data.Tables = make(map[string]*TableCheckpoint)
	m.data.Phase = ""
	m.markDirty()
	return nil
}

func (m *Manager) IsChunkCompleted(tableName string, chunkIndex int) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	tc, ok := m.data.Tables[tableName]
	if !ok || tc.Chunks == nil {
		return false
	}
	chunk, ok := tc.Chunks[chunkIndex]
	return ok && chunk.State == StateCompleted
}

func (m *Manager) GetChunkProgress(tableName string, chunkIndex int) (rows, bytes int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	tc, ok := m.data.Tables[tableName]
	if !ok || tc.Chunks == nil {
		return 0, 0
	}
	chunk, ok := tc.Chunks[chunkIndex]
	if !ok {
		return 0, 0
	}
	return chunk.RowCount, chunk.ByteCount
}

func (m *Manager) MarkChunkCompleted(tableName string, chunkIndex int, rows, bytes int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	tc, ok := m.data.Tables[tableName]
	if !ok {
		return fmt.Errorf("table %s not found in checkpoint", tableName)
	}
	if tc.Chunks == nil {
		tc.Chunks = make(map[int]*ChunkCheckpoint)
	}
	tc.Chunks[chunkIndex] = &ChunkCheckpoint{
		Index:      chunkIndex,
		State:      StateCompleted,
		RowCount:   rows,
		ByteCount:  bytes,
		FinishedAt: time.Now(),
	}
	m.markDirty()
	return nil
}

func (m *Manager) MarkChunkFailed(tableName string, chunkIndex int, errMsg string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	tc, ok := m.data.Tables[tableName]
	if !ok {
		return fmt.Errorf("table %s not found in checkpoint", tableName)
	}
	if tc.Chunks == nil {
		tc.Chunks = make(map[int]*ChunkCheckpoint)
	}
	tc.Chunks[chunkIndex] = &ChunkCheckpoint{
		Index:      chunkIndex,
		State:      StateFailed,
		Error:      errMsg,
		FinishedAt: time.Now(),
	}
	m.markDirty()
	return nil
}

func (m *Manager) GetPendingChunks(tableName string) []int {
	m.mu.Lock()
	defer m.mu.Unlock()
	tc, ok := m.data.Tables[tableName]
	if !ok || tc.Chunks == nil {
		return nil
	}
	var pending []int
	for idx, chunk := range tc.Chunks {
		if chunk.State != StateCompleted {
			pending = append(pending, idx)
		}
	}
	return pending
}

func (m *Manager) InitChunk(tableName string, chunkIndex int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	tc, ok := m.data.Tables[tableName]
	if !ok {
		return
	}
	if tc.Chunks == nil {
		tc.Chunks = make(map[int]*ChunkCheckpoint)
	}
	if _, exists := tc.Chunks[chunkIndex]; !exists {
		tc.Chunks[chunkIndex] = &ChunkCheckpoint{
			Index: chunkIndex,
			State: StatePending,
		}
		m.markDirty()
	}
}

func (m *Manager) ResetChunks(tableName string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	tc, ok := m.data.Tables[tableName]
	if !ok {
		return
	}
	tc.Chunks = nil
	m.markDirty()
}
