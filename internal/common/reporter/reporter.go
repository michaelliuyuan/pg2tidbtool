package reporter

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"strings"
	"time"
)

func FormatDuration(d time.Duration) string {
	hours := int(d.Hours())
	minutes := int(d.Minutes()) % 60
	seconds := math.Mod(d.Seconds(), 60)
	if hours > 0 {
		return fmt.Sprintf("%dh%dm%.3fs", hours, minutes, seconds)
	}
	if minutes > 0 {
		return fmt.Sprintf("%dm%.3fs", minutes, seconds)
	}
	return fmt.Sprintf("%.3fs", seconds)
}

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
	r.Duration = FormatDuration(r.EndTime.Sub(r.StartTime))
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
	failCount := 0
	warnCount := 0
	for _, t := range r.Tables {
		switch t.Status {
		case StatusFail:
			failCount++
		case StatusWarn:
			warnCount++
		}
	}
	if failCount > 0 {
		return StatusFail
	}
	if warnCount > 0 {
		return StatusWarn
	}
	return StatusPass
}

func (r *Report) FailedTables() []TableReport {
	var result []TableReport
	for _, t := range r.Tables {
		if t.Status == StatusFail {
			result = append(result, t)
		}
	}
	return result
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
	if strings.HasSuffix(path, ".html") {
		return r.SaveHTML(path)
	}
	return r.SaveText(path)
}

func (r *Report) SaveHTML(path string) error {
	html := r.ToHTML()
	return os.WriteFile(path, []byte(html), 0644)
}

func (r *Report) ToHTML() string {
	var sb strings.Builder
	sb.WriteString(`<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>TiMS Migration Report</title>
<style>
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #f0f2f5; color: #333; line-height: 1.6; }
.container { max-width: 960px; margin: 0 auto; padding: 24px; }
.header { background: linear-gradient(135deg, #1a1a2e, #16213e); color: #fff; padding: 32px; border-radius: 12px; margin-bottom: 24px; }
.header h1 { font-size: 28px; margin-bottom: 8px; }
.header .logo { color: #e23d3d; font-weight: 900; }
.header .subtitle { color: #aaa; font-size: 14px; }
.card { background: #fff; border-radius: 10px; padding: 24px; margin-bottom: 20px; box-shadow: 0 2px 8px rgba(0,0,0,0.06); }
.card h2 { font-size: 18px; margin-bottom: 16px; color: #1a1a2e; border-bottom: 2px solid #e8e8e8; padding-bottom: 8px; }
.stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); gap: 16px; margin-bottom: 12px; }
.stat { text-align: center; padding: 16px; background: #fafafa; border-radius: 8px; }
.stat .value { font-size: 28px; font-weight: 700; color: #1a1a2e; }
.stat .label { font-size: 12px; color: #888; margin-top: 4px; }
table { width: 100%; border-collapse: collapse; font-size: 14px; }
th { background: #1a1a2e; color: #fff; padding: 10px 12px; text-align: left; font-weight: 600; }
td { padding: 10px 12px; border-bottom: 1px solid #eee; }
tr:hover td { background: #f8f9fa; }
.badge { display: inline-block; padding: 2px 10px; border-radius: 12px; font-size: 12px; font-weight: 600; }
.badge-pass { background: #e6f7e6; color: #2e7d32; }
.badge-fail { background: #fde8e8; color: #c62828; }
.badge-warn { background: #fff3e0; color: #e65100; }
.badge-skip { background: #f0f0f0; color: #666; }
.overall-pass { color: #2e7d32; }
.overall-fail { color: #c62828; }
.overall-warn { color: #e65100; }
.footer { text-align: center; color: #aaa; font-size: 12px; margin-top: 24px; }
.summary { background: #f6f8fa; padding: 16px; border-radius: 8px; margin-bottom: 16px; font-size: 14px; }
.info-row { display: flex; justify-content: space-between; padding: 8px 0; border-bottom: 1px solid #f0f0f0; }
.info-row:last-child { border-bottom: none; }
.info-label { color: #666; min-width: 120px; }
.info-value { font-weight: 500; text-align: right; }
@media print { body { background: #fff; } .container { padding: 0; } .card { box-shadow: none; border: 1px solid #ddd; } }
</style>
</head>
<body>
<div class="container">
`)
	sb.WriteString(`<div class="header">
<h1><span class="logo">Ti</span>MS 迁移报告</h1>
<div class="subtitle">`)
	sb.WriteString(htmlEsc(r.Phase))
	sb.WriteString(` &middot; `)
	sb.WriteString(htmlEsc(r.StartTime.Format("2006-01-02 15:04:05")))
	sb.WriteString(` ~ `)
	sb.WriteString(htmlEsc(r.EndTime.Format("2006-01-02 15:04:05")))
	sb.WriteString(`</div></div>`)

	statusClass := "overall-" + string(r.Status)
	sb.WriteString(`<div class="card"><h2>概览</h2><div class="stats">`)
	sb.WriteString(fmt.Sprintf(`<div class="stat"><div class="value %s">%s</div><div class="label">状态</div></div>`, statusClass, htmlEsc(statusCN(string(r.Status)))))
	sb.WriteString(fmt.Sprintf(`<div class="stat"><div class="value">%s</div><div class="label">耗时</div></div>`, htmlEsc(r.Duration)))
	sb.WriteString(fmt.Sprintf(`<div class="stat"><div class="value">%d</div><div class="label">总表数</div></div>`, r.Stats.TotalTables))
	sb.WriteString(fmt.Sprintf(`<div class="stat"><div class="value">%d</div><div class="label">源端行数</div></div>`, r.Stats.TotalSourceRows))
	sb.WriteString(fmt.Sprintf(`<div class="stat"><div class="value">%d</div><div class="label">目标行数</div></div>`, r.Stats.TotalTargetRows))
	sb.WriteString(`</div>`)
	if r.Summary != "" {
		sb.WriteString(fmt.Sprintf(`<div class="summary">%s</div>`, htmlEsc(r.Summary)))
	}
	sb.WriteString(`</div>`)

	// Stats card
	sb.WriteString(`<div class="card"><h2>统计</h2><div class="stats">`)
	sb.WriteString(fmt.Sprintf(`<div class="stat"><div class="value" style="color:#2e7d32">%d</div><div class="label">通过</div></div>`, r.Stats.PassTables))
	sb.WriteString(fmt.Sprintf(`<div class="stat"><div class="value" style="color:#c62828">%d</div><div class="label">失败</div></div>`, r.Stats.FailTables))
	sb.WriteString(fmt.Sprintf(`<div class="stat"><div class="value" style="color:#e65100">%d</div><div class="label">警告</div></div>`, r.Stats.WarnTables))
	sb.WriteString(fmt.Sprintf(`<div class="stat"><div class="value" style="color:#666">%d</div><div class="label">跳过</div></div>`, r.Stats.SkipTables))
	if r.Stats.TotalDiffRows != 0 {
		sb.WriteString(fmt.Sprintf(`<div class="stat"><div class="value" style="color:#c62828">%d</div><div class="label">差异行数</div></div>`, r.Stats.TotalDiffRows))
	} else {
		sb.WriteString(`<div class="stat"><div class="value" style="color:#2e7d32">0</div><div class="label">差异行数</div></div>`)
	}
	sb.WriteString(`</div></div>`)

	// Table detail card
	if len(r.Tables) > 0 {
		sb.WriteString(`<div class="card"><h2>表详情</h2><table><thead><tr>`)
		sb.WriteString(`<th>#</th><th>表名</th><th>状态</th><th>源端行数</th><th>目标行数</th><th>差异</th><th>耗时</th><th>错误</th>`)
		sb.WriteString(`</tr></thead><tbody>`)
		for i, t := range r.Tables {
			badgeClass := "badge-" + string(t.Status)
			errStr := htmlEsc(t.Error)
			if len(errStr) > 60 {
				errStr = errStr[:60] + "..."
			}
			diffStr := ""
			if t.DiffRows != 0 {
				diffStr = fmt.Sprintf("%d", t.DiffRows)
			}
			sb.WriteString(fmt.Sprintf(`<tr><td>%d</td><td>%s</td><td><span class="badge %s">%s</span></td><td>%d</td><td>%d</td><td>%s</td><td>%s</td><td>%s</td></tr>`,
				i+1, htmlEsc(t.TableName), badgeClass, htmlEsc(statusCN(string(t.Status))), t.SourceRows, t.TargetRows, diffStr, htmlEsc(t.Duration), errStr))
		}
		sb.WriteString(`</tbody></table></div>`)
	}

	sb.WriteString(`<div class="footer">由 TiMS (TiDB Migration Suite) 生成 &middot; `)
	sb.WriteString(htmlEsc(time.Now().Format("2006-01-02 15:04:05")))
	sb.WriteString(`</div></div></body></html>`)

	return sb.String()
}

func htmlEsc(s string) string {
	s = strings.ReplaceAll(s, "&", "&amp;")
	s = strings.ReplaceAll(s, "<", "&lt;")
	s = strings.ReplaceAll(s, ">", "&gt;")
	s = strings.ReplaceAll(s, `"`, "&quot;")
	return s
}

func statusCN(s string) string {
	switch s {
	case "pass":
		return "通过"
	case "fail":
		return "失败"
	case "warn":
		return "警告"
	case "skip":
		return "跳过"
	default:
		return s
	}
}
