package assess

import (
	"fmt"
	"html/template"
	"io"
	"os"
	"sort"
	"strings"
)

// HTMLReportTemplate is the Go template for the HTML assessment report.
const htmlReportTemplate = `<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>PG → TiDB 兼容性评估报告</title>
<style>
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'PingFang SC', 'Microsoft YaHei', sans-serif; background: #f0f2f5; color: #333; }
.container { max-width: 1200px; margin: 0 auto; padding: 24px; }
h1 { text-align: center; color: #1a1a2e; margin-bottom: 8px; font-size: 28px; }
.subtitle { text-align: center; color: #666; margin-bottom: 32px; font-size: 14px; }

/* Score Card */
.score-card {
  background: linear-gradient(135deg, {{.ScoreGradient}});
  border-radius: 16px; padding: 32px; text-align: center;
  color: #fff; margin-bottom: 24px; box-shadow: 0 4px 12px rgba(0,0,0,0.15);
}
.score-value { font-size: 64px; font-weight: 900; line-height: 1; }
.score-label { font-size: 18px; margin-top: 8px; opacity: 0.9; }
.score-badge { display: inline-block; margin-top: 12px; padding: 4px 16px; border-radius: 20px; font-size: 14px; background: rgba(255,255,255,0.2); }

/* Summary Cards */
.summary-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; margin-bottom: 24px; }
.summary-item { background: #fff; border-radius: 12px; padding: 20px; text-align: center; box-shadow: 0 2px 8px rgba(0,0,0,0.06); }
.summary-count { font-size: 36px; font-weight: 700; line-height: 1.2; }
.summary-label { font-size: 13px; color: #666; margin-top: 4px; }
.compatible { color: #52c41a; }
.convertible { color: #faad14; }
.manual { color: #fa8c16; }
.incompatible { color: #f5222d; }

/* Dimension Table */
.section-title { font-size: 20px; font-weight: 600; margin: 24px 0 16px; color: #1a1a2e; }
.dim-table { width: 100%; border-collapse: collapse; background: #fff; border-radius: 12px; overflow: hidden; box-shadow: 0 2px 8px rgba(0,0,0,0.06); margin-bottom: 24px; }
.dim-table th { background: #1a1a2e; color: #fff; padding: 12px 16px; text-align: left; font-size: 14px; }
.dim-table td { padding: 12px 16px; border-bottom: 1px solid #f0f0f0; font-size: 14px; }
.dim-table tr:last-child td { border-bottom: none; }
.dim-table tr:hover { background: #fafafa; }
.score-bar { height: 8px; border-radius: 4px; background: #f0f0f0; width: 120px; display: inline-block; vertical-align: middle; }
.score-bar-fill { height: 100%; border-radius: 4px; }

/* Problem Table */
.problem-table { width: 100%; border-collapse: collapse; background: #fff; border-radius: 12px; overflow: hidden; box-shadow: 0 2px 8px rgba(0,0,0,0.06); margin-bottom: 24px; }
.problem-table th { background: #1a1a2e; color: #fff; padding: 10px 12px; text-align: left; font-size: 13px; }
.problem-table td { padding: 10px 12px; border-bottom: 1px solid #f0f0f0; font-size: 13px; }
.problem-table tr:last-child td { border-bottom: none; }
.problem-table tr:hover { background: #fff7e6; }
.badge { display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 12px; font-weight: 500; }
.badge-compatible { background: #f6ffed; color: #52c41a; }
.badge-convertible { background: #fffbe6; color: #d48806; }
.badge-manual { background: #fff7e6; color: #d46b08; }
.badge-incompatible { background: #fff1f0; color: #cf1322; }
/* DDL Modal */
.ddl-modal { display: none; position: fixed; z-index: 1000; left: 0; top: 0; width: 100%; height: 100%; background: rgba(0,0,0,0.5); }
.ddl-modal.active { display: flex; align-items: center; justify-content: center; }
.ddl-modal-content { background: #fff; border-radius: 12px; padding: 24px; max-width: 700px; width: 90%; max-height: 80vh; display: flex; flex-direction: column; box-shadow: 0 8px 24px rgba(0,0,0,0.2); }
.ddl-modal-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 16px; }
.ddl-modal-header h3 { margin: 0; font-size: 18px; color: #1a1a2e; }
.ddl-close { background: none; border: none; font-size: 24px; cursor: pointer; color: #999; }
.ddl-close:hover { color: #333; }
.ddl-textarea { width: 100%; min-height: 200px; font-family: "Courier New", monospace; font-size: 13px; padding: 12px; border: 1px solid #d9d9d9; border-radius: 6px; resize: vertical; background: #fafafa; }
.ddl-copy-btn { margin-top: 12px; padding: 8px 16px; background: #1a1a2e; color: #fff; border: none; border-radius: 6px; cursor: pointer; font-size: 13px; }
.ddl-copy-btn:hover { background: #333; }
.ddl-btn { padding: 4px 10px; background: #e6f7ff; color: #1890ff; border: 1px solid #91d5ff; border-radius: 4px; cursor: pointer; font-size: 12px; }
.ddl-btn:hover { background: #bae7ff; }
.footer { text-align: center; color: #999; font-size: 12px; margin-top: 32px; padding: 16px; }
</style>
</head>
<body>
<div class="container">
  <h1>PostgreSQL → TiDB 兼容性评估报告</h1>
  <p class="subtitle">自动评估数据库迁移兼容性，识别潜在风险和迁移建议</p>

  <!-- Score Card -->
  <div class="score-card">
    <div class="score-value">{{.ScoreDisplay}}</div>
    <div class="score-label">总体兼容性评分</div>
    <div class="score-badge">{{.LevelEmoji}} {{.LevelCN}}</div>
  </div>

  <!-- Summary -->
  <div class="summary-grid">
    <div class="summary-item">
      <div class="summary-count compatible">{{.SummaryCompatible}}</div>
      <div class="summary-label">✅ 兼容</div>
    </div>
    <div class="summary-item">
      <div class="summary-count convertible">{{.SummaryConvertible}}</div>
      <div class="summary-label">⚠️ 可转换</div>
    </div>
    <div class="summary-item">
      <div class="summary-count manual">{{.SummaryManual}}</div>
      <div class="summary-label">🟡 需手动处理</div>
    </div>
    <div class="summary-item">
      <div class="summary-count incompatible">{{.SummaryIncompatible}}</div>
      <div class="summary-label">❌ 不兼容</div>
    </div>
  </div>

  <!-- Dimensions -->
  <h2 class="section-title">📊 维度评分</h2>
  <table class="dim-table">
    <thead><tr><th>评估维度</th><th>对象数</th><th>得分</th><th>兼容性</th></tr></thead>
    <tbody>
    {{range .DimensionRows}}
    <tr>
      <td>{{.Name}}</td>
      <td>{{.Total}}</td>
      <td><span class="score-bar"><span class="score-bar-fill" style="width:{{.ScorePct}};background:{{.BarColor}}"></span></span> {{.ScoreDisplay}}</td>
      <td>{{.Emoji}} {{.LevelCN}}</td>
    </tr>
    {{end}}
    </tbody>
  </table>

  <!-- Problems -->
  {{if .ProblemRows}}
  <h2 class="section-title">⚠️ 需要处理的项目（{{.ProblemCount}} 项）</h2>
  <table class="problem-table">
    <thead><tr><th style="width:40px">#</th><th>类型</th><th>对象</th><th>级别</th><th>PG</th><th>TiDB</th><th>建议</th><th style="width:60px">DDL</th></tr></thead>
    <tbody>
    {{range .ProblemRows}}
    <tr>
      <td>{{.Index}}</td>
      <td>{{.ObjectType}}</td>
      <td style="max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;" title="{{.ObjectName}}">{{.ObjectName}}</td>
      <td><span class="badge badge-{{.BadgeClass}}">{{.Emoji}} {{.LevelShort}}</span></td>
      <td style="max-width:150px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;" title="{{.PGDetail}}">{{.PGDetail}}</td>
      <td style="max-width:120px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;" title="{{.TiDBDetail}}">{{.TiDBDetail}}</td>
      <td style="max-width:250px;">{{.Suggestion}}</td>
    <td>{{if .DDL}}<button class="ddl-btn" onclick="showDDL(this)" data-ddl="{{.DDLEscaped}}">查看</button>{{end}}</td>
    </tr>
    {{end}}
    </tbody>
  </table>
  {{end}}

  <div class="footer">由 pg2tidb 兼容性评估引擎自动生成</div>
</div>

  <!-- DDL Modal -->
  <div class="ddl-modal" id="ddlModal">
    <div class="ddl-modal-content">
      <div class="ddl-modal-header">
        <h3>DDL 导出</h3>
        <button class="ddl-close" onclick="closeDDL()">&times;</button>
      </div>
      <textarea class="ddl-textarea" id="ddlText" readonly></textarea>
      <button class="ddl-copy-btn" onclick="copyDDL()">📋 复制 DDL</button>
    </div>
  </div>

<script>
function showDDL(btn) {
  var ddl = btn.getAttribute("data-ddl");
  document.getElementById("ddlText").value = ddl;
  document.getElementById("ddlModal").classList.add("active");
}
function closeDDL() {
  document.getElementById("ddlModal").classList.remove("active");
}
function copyDDL() {
  var textarea = document.getElementById("ddlText");
  textarea.select();
  textarea.setSelectionRange(0, 99999);
  navigator.clipboard.writeText(textarea.value).then(function() {
    alert("DDL 已复制到剪贴板");
  });
}
window.onclick = function(event) {
  if (event.target == document.getElementById("ddlModal")) {
    closeDDL();
  }
}
</script>
</body>
</html>`

// htmlTemplateData holds data for the HTML report template.
type htmlTemplateData struct {
	ScoreDisplay     string
	ScoreGradient    string
	LevelEmoji       string
	LevelCN          string
	SummaryCompatible   int
	SummaryConvertible  int
	SummaryManual       int
	SummaryIncompatible int
	DimensionRows    []htmlDimRow
	ProblemRows      []htmlProblemRow
	ProblemCount     int
}

type htmlDimRow struct {
	Name        string
	Total       int
	ScorePct    string
	ScoreDisplay string
	BarColor    string
	Emoji       string
	LevelCN     string
}

type htmlProblemRow struct {
		DDL        string
		DDLEscaped string
	Index      int
	ObjectType string
	ObjectName string
	BadgeClass string
	Emoji      string
	LevelShort string
	PGDetail   string
	TiDBDetail string
	Suggestion string
}

// WriteHTML writes the HTML report to a writer.
func (rg *ReportGenerator) WriteHTML(w io.Writer) error {
	tmpl, err := template.New("report").Parse(htmlReportTemplate)
	if err != nil {
		return fmt.Errorf("parse template: %w", err)
	}

	data := rg.buildHTMLData()
	return tmpl.Execute(w, data)
}

// WriteHTMLFile writes the HTML report to a file.
func (rg *ReportGenerator) WriteHTMLFile(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return rg.WriteHTML(f)
}

func (rg *ReportGenerator) buildHTMLData() htmlTemplateData {
	r := rg.report
	data := htmlTemplateData{
		ScoreDisplay:       FormatScore(r.Score),
		LevelEmoji:         LevelEmoji[r.Level],
		LevelCN:            levelNameCN(r.Level),
		SummaryCompatible:  r.Summary[LevelCompatible],
		SummaryConvertible: r.Summary[LevelConvertible],
		SummaryManual:      r.Summary[LevelManualNeeded],
		SummaryIncompatible: r.Summary[LevelIncompatible],
	}

	// Score gradient
	switch {
	case r.Score >= 90:
		data.ScoreGradient = "#52c41a, #73d13d"
	case r.Score >= 70:
		data.ScoreGradient = "#faad14, #ffc53d"
	case r.Score >= 40:
		data.ScoreGradient = "#fa8c16, #ffa940"
	default:
		data.ScoreGradient = "#f5222d, #ff4d4f"
	}

	// Dimension rows
	for _, dim := range r.DimensionResults {
		level := OverallLevel(dim.Score)
		row := htmlDimRow{
			Name:        dimNameCN(dim.Dimension),
			Total:       dim.Total,
			ScoreDisplay: FormatScore(dim.Score),
			ScorePct:    fmt.Sprintf("%.0f%%", dim.Score),
			Emoji:       LevelEmoji[level],
			LevelCN:     levelNameCN(level),
		}
		switch {
		case dim.Score >= 90:
			row.BarColor = "#52c41a"
		case dim.Score >= 70:
			row.BarColor = "#faad14"
		case dim.Score >= 40:
			row.BarColor = "#fa8c16"
		default:
			row.BarColor = "#f5222d"
		}
		data.DimensionRows = append(data.DimensionRows, row)
	}

	// Problem rows (non-compatible items)
	var problems []Finding
	for _, f := range r.AllFindings {
		if f.Level != LevelCompatible {
			problems = append(problems, f)
		}
	}
	sort.Slice(problems, func(i, j int) bool {
		return levelOrder(problems[i].Level) < levelOrder(problems[j].Level)
	})

	data.ProblemCount = len(problems)
	if len(problems) > 200 {
		problems = problems[:200]
	}

	for i, f := range problems {
		badgeClass := "manual"
		switch f.Level {
		case LevelConvertible:
			badgeClass = "convertible"
		case LevelIncompatible:
			badgeClass = "incompatible"
		}
		suggestion := f.Suggestion
		if len(suggestion) > 80 {
			suggestion = suggestion[:77] + "..."
		}
		pgDetail := f.PGDetail
		if len(pgDetail) > 50 {
			pgDetail = pgDetail[:47] + "..."
		}
		tidbDetail := f.TiDBDetail
		if len(tidbDetail) > 40 {
			tidbDetail = tidbDetail[:37] + "..."
		}
		data.ProblemRows = append(data.ProblemRows, htmlProblemRow{
			DDL:        f.DDL,
			DDLEscaped: template.HTMLEscapeString(f.DDL),
			Index:      i + 1,
			ObjectType: f.ObjectType,
			ObjectName: f.ObjectName,
			BadgeClass: badgeClass,
			Emoji:      LevelEmoji[f.Level],
			LevelShort: levelShort(f.Level),
			PGDetail:   pgDetail,
			TiDBDetail: tidbDetail,
			Suggestion: suggestion,
		})
	}

	return data
}

// Helper for building HTML template data - extract object type from finding.
func objectTypeCN(ot string) string {
	switch ot {
	case "column":
		return "列"
	case "index":
		return "索引"
	case "view":
		return "视图"
	case "function":
		return "函数"
	case "trigger":
		return "触发器"
	case "enum":
		return "枚举"
	case "extension":
		return "扩展"
	case "sequence":
		return "序列"
	case "table":
		return "表"
	case "column_default":
		return "默认值"
	default:
		return ot
	}
}

// Suppress unused import warning
var _ = strings.TrimSpace
var _ = objectTypeCN
