package orchestrator

import (
	"context"
	"fmt"
	"time"

	"github.com/pg2tidb/pg2tidb-migrator/internal/api"
	"github.com/pg2tidb/pg2tidb-migrator/internal/common"
	"github.com/pg2tidb/pg2tidb-migrator/internal/common/checkpoint"
	"github.com/pg2tidb/pg2tidb-migrator/internal/common/config"
	cerrors "github.com/pg2tidb/pg2tidb-migrator/internal/common/errors"
	"github.com/pg2tidb/pg2tidb-migrator/internal/common/logger"
	"github.com/pg2tidb/pg2tidb-migrator/internal/data"
	"github.com/pg2tidb/pg2tidb-migrator/internal/precheck"
	"github.com/pg2tidb/pg2tidb-migrator/internal/schema"
	"github.com/pg2tidb/pg2tidb-migrator/internal/validator"
	"go.uber.org/zap"
)

type Orchestrator struct {
	cfg        config.Config
	schemaMig  common.SchemaMigrator
	dataMig    common.DataMigrator
	validator  common.DataValidator
	prechecker common.Prechecker
	cpMgr      *checkpoint.Manager
	webServer  *api.Server
}

func NewOrchestrator(cfg config.Config) *Orchestrator {
	return &Orchestrator{
		cfg:       cfg,
		schemaMig: schema.NewMigrator(cfg),
		dataMig:   data.NewMigrator(cfg),
		validator: validator.NewValidator(cfg),
		prechecker: precheck.NewChecker(cfg),
	}
}

func (o *Orchestrator) Run(ctx context.Context, pipelineCfg PipelineConfig) ([]PipelineResult, error) {
	logger.InitWithOutput(o.cfg.Logging.Level, o.cfg.Logging.Format, o.cfg.Logging.Output)
	defer logger.Sync()

	log := zap.L()
	log.Info("starting pg2tidb migration pipeline")

	var err error
	o.cpMgr, err = checkpoint.NewManager(o.cfg.Migration.CheckpointDir)
	if err != nil {
		return nil, cerrors.Wrap(cerrors.ErrCheckpointLoad, "init checkpoint", err)
	}

	if o.cfg.Web.Enable {
		stateAdapter := &checkpointStateReader{mgr: o.cpMgr}
		o.webServer = api.NewServer(stateAdapter, o.cfg.Web.Host, o.cfg.Web.Port)
		if err := o.webServer.Start(); err != nil {
			log.Warn("failed to start web server", zap.Error(err))
		} else {
			log.Info("web monitor started", zap.String("addr", fmt.Sprintf("%s:%d", o.cfg.Web.Host, o.cfg.Web.Port)))
		}
		defer o.webServer.Stop()
	}

	var results []PipelineResult
	startTime := time.Now()

	if !pipelineCfg.SkipPrecheck {
		o.cpMgr.SetPhase("precheck")
		result := o.runPrecheck(ctx)
		results = append(results, result)
		if !result.Success && !pipelineCfg.OnErrorContinue {
			return results, result.Error
		}
	}

	if !pipelineCfg.SkipSchema {
		o.cpMgr.SetPhase("schema")
		result := o.runSchema(ctx)
		results = append(results, result)
		if !result.Success && !pipelineCfg.OnErrorContinue {
			return results, result.Error
		}
	}

	if !pipelineCfg.SkipData {
		o.cpMgr.SetPhase("data")
		result := o.runData(ctx)
		results = append(results, result)
		if !result.Success && !pipelineCfg.OnErrorContinue {
			return results, result.Error
		}
	}

	if !pipelineCfg.SkipValidate {
		o.cpMgr.SetPhase("validate")
		result := o.runValidate(ctx)
		results = append(results, result)
		if !result.Success && !pipelineCfg.OnErrorContinue {
			return results, result.Error
		}
	}

	o.cpMgr.SetPhase("completed")
	log.Info("migration pipeline completed",
		zap.String("duration", time.Since(startTime).String()),
		zap.Int("phases", len(results)))

	return results, nil
}

func (o *Orchestrator) runPrecheck(ctx context.Context) PipelineResult {
	log := zap.L()
	log.Info("Phase: 预检查")
	start := time.Now()

	rpt, err := o.prechecker.Run(ctx, common.PrecheckOpts{
		ReportFile: "precheck-report.json",
	})

	result := PipelineResult{
		Phase:   PhasePrecheck,
		Success: err == nil,
		Error:   err,
	}

	if err != nil {
		log.Error("pre-check failed", zap.Error(err))
		return result
	}

	if rpt != nil {
		log.Info("pre-check completed",
			zap.String("status", string(rpt.Status)),
			zap.String("duration", time.Since(start).String()))
	}

	return result
}

func (o *Orchestrator) runSchema(ctx context.Context) PipelineResult {
	log := zap.L()
	log.Info("Phase: Schema 迁移")
	start := time.Now()

	err := o.schemaMig.Run(ctx, common.SchemaOpts{})

	result := PipelineResult{
		Phase:   PhaseSchema,
		Success: err == nil,
		Error:   err,
	}

	if err != nil {
		if cerrors.ShouldAbort(err, cerrors.StrategyAbort) {
			log.Error("schema migration failed", zap.Error(err))
			return result
		}
		log.Warn("schema migration had errors (continuing)", zap.Error(err))
		result.Success = true
	}

	log.Info("schema migration completed", zap.String("duration", time.Since(start).String()))
	return result
}

func (o *Orchestrator) runData(ctx context.Context) PipelineResult {
	log := zap.L()
	log.Info("Phase: 数据迁移")
	start := time.Now()

	dataResult, err := o.dataMig.Run(ctx, common.DataOpts{
		Parallel:      o.cfg.Migration.Parallel,
		BatchSize:     o.cfg.Migration.BatchSize,
		Tables:        o.cfg.Migration.Tables,
		ExcludeTables: o.cfg.Migration.ExcludeTables,
		UseLightning:  o.cfg.Migration.UseLightning,
		TempDir:       o.cfg.Migration.TempDir,
	})

	result := PipelineResult{
		Phase:   PhaseData,
		Success: err == nil,
		Error:   err,
	}

	if err != nil {
		log.Error("data migration failed", zap.Error(err))
		return result
	}

	if dataResult != nil {
		log.Info("data migration completed",
			zap.Int64("rows", dataResult.TotalRows),
			zap.Int("tables", dataResult.TotalTables),
			zap.String("duration", time.Since(start).String()))
	}

	return result
}

func (o *Orchestrator) runValidate(ctx context.Context) PipelineResult {
	log := zap.L()
	log.Info("Phase: 数据验证")
	start := time.Now()

	rpt, err := o.validator.Run(ctx, common.ValidateOpts{
		Level:      "L2",
		SampleRatio: 0.01,
		Tables:     o.cfg.Migration.Tables,
		ReportFile: "validation-report.json",
	})

	result := PipelineResult{
		Phase:   PhaseValidate,
		Success: err == nil,
		Error:   err,
	}

	if err != nil {
		log.Error("data validation failed", zap.Error(err))
		return result
	}

	if rpt != nil {
		log.Info("data validation completed",
			zap.String("status", string(rpt.Status)),
			zap.String("duration", time.Since(start).String()))
	}

	return result
}

type checkpointStateReader struct {
	mgr *checkpoint.Manager
}

func (r *checkpointStateReader) GetPhase() string {
	return r.mgr.GetPhase()
}

func (r *checkpointStateReader) GetAllTables() map[string]api.TableState {
	tables := r.mgr.GetAllTables()
	result := make(map[string]api.TableState, len(tables))
	for name, tc := range tables {
		result[name] = api.TableState{
			TableName: tc.TableName,
			State:     string(tc.State),
			RowsDone:  tc.RowsDone,
			RowsTotal: tc.RowsTotal,
			Error:     tc.Error,
		}
	}
	return result
}

func (r *checkpointStateReader) Summary() (completed, failed, pending, running int) {
	return r.mgr.Summary()
}
