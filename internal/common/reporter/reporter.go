package reporter

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"
)

type Status string

const (
	StatusPass Status = "pass"
	StatusFail Status = "fail"
	StatusWarn Status = "warn"
	StatusSkip Status = "skip"
)

type TableReport struct {
	TableName  string `json:"table_name"`
	Status     Status `json:"status"`
	Duration   string `json:"duration,omitempty"`
	SourceRows int64  `json:"source_rows,omitempty"`
	TargetRows int64  `json:"target_rows,omitempty"`
	DiffRows   int64  `json:"diff_rows,omitempty"`
	Error      string `json:"error,omitempty"`
	Suggestion string `json:"suggestion,omitempty"`
}

type Report struct {
	Tool      string        `json:"tool"`
	Version   string        `json:"version"`
	Phase     string        `json:"phase"`
	StartTime time.Time     `json:"start_time"`
	EndTime   time.Time     `json:"end_time"`
	Duration  string        `json:"duration"`
	Status    Status        `json:"overall_status"`
	Tables    []TableReport `json:"tables"`
	Summary   string        `json:"summary,omitempty"`
	Stats     ReportStats   `json:"stats"`
}

type ReportStats struct {
	TotalTables   int   `json:"total_tables"`
	PassTables    int   `json:"pass_tables"`
	FailTables    int   `json:"fail_tables"`
	WarnTables    int   `json:"warn_tables"`
	SkipTables    int   `json:"skip_tables"`
	TotalSourceRows int64 `json:"total_source_rows"`
	TotalTargetRows int64 `json:"total_target_rows"`
	TotalDiffRows   int64 `json:"total_diff_rows"`
}

func NewReport(phase string) *Report {
	return &Report{
		Tool:      "pg2tidb-migrator",
		Version:   "0.1.0",
		Phase:     phase,
		StartTime: time.Now(),
		Tables:    []TableReport{},
	}
}

func (r *Report) AddTableReport(tr TableReport) {
	r.Tables = append(r.Tables, tr)
}

func (r *Report) Finish(status Status, summary string) {
	r.EndTime = time.Now()
	r.Duration = r.EndTime.Sub(r.StartTime).String()
	r.Status = status
	r.Summary = summary
	r.computeStats()
}

func (r *Report) computeStats() {
	r.Stats = ReportStats{
		TotalTables: len(r.Tables),
	}
	for _, t := range r.Tables {
		r.Stats.TotalSourceRows += t.SourceRows
		r.Stats.TotalTargetRows += t.TargetRows
		r.Stats.TotalDiffRows += t.DiffRows
		switch t.Status {
		case StatusPass:
			r.Stats.PassTables++
		case StatusFail:
			r.Stats.FailTables++
		case StatusWarn:
			r.Stats.WarnTables++
		case StatusSkip:
			r.Stats.SkipTables++
		}
	}
}

func (r *Report) OverallStatus() Status {
	if r.Stats.FailTables > 0 {
		return StatusFail
	}
	if r.Stats.WarnTables > 0 {
		return StatusWarn
	}
	return StatusPass
}

func (r *Report) SaveJSON(path string) error {
	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal report: %w", err)
	}
	return os.WriteFile(path, data, 0644)
}

func (r *Report) SaveText(path string) error {
	var lines []string
	lines = append(lines, fmt.Sprintf("=== %s Report ===", r.Phase))
	lines = append(lines, fmt.Sprintf("Time:     %s - %s (%s)", r.StartTime.Format(time.RFC3339), r.EndTime.Format(time.RFC3339), r.Duration))
	lines = append(lines, fmt.Sprintf("Status:   %s", r.Status))
	if r.Summary != "" {
		lines = append(lines, fmt.Sprintf("Summary:  %s", r.Summary))
	}
	lines = append(lines, fmt.Sprintf("Tables:   %d total, %d pass, %d fail, %d warn, %d skip",
		r.Stats.TotalTables, r.Stats.PassTables, r.Stats.FailTables, r.Stats.WarnTables, r.Stats.SkipTables))
	lines = append(lines, fmt.Sprintf("Rows:     source=%d target=%d diff=%d",
		r.Stats.TotalSourceRows, r.Stats.TotalTargetRows, r.Stats.TotalDiffRows))
	lines = append(lines, "")
	lines = append(lines, fmt.Sprintf("%-30s %-8s %12s %12s %12s %s", "Table", "Status", "Source Rows", "Target Rows", "Diff Rows", "Error"))
	lines = append(lines, strings.Repeat("-", 100))
	for _, t := range r.Tables {
		errStr := t.Error
		if len(errStr) > 40 {
			errStr = errStr[:40] + "..."
		}
		lines = append(lines, fmt.Sprintf("%-30s %-8s %12d %12d %12d %s", t.TableName, t.Status, t.SourceRows, t.TargetRows, t.DiffRows, errStr))
	}

	return os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0644)
}

func (r *Report) Save(path string) error {
	if strings.HasSuffix(path, ".json") {
		return r.SaveJSON(path)
	}
	return r.SaveText(path)
}
