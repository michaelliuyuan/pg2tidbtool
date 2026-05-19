package orchestrator

import (
	"context"
)

type Phase string

const (
	PhasePrecheck  Phase = "precheck"
	PhaseSchema    Phase = "schema"
	PhaseData      Phase = "data"
	PhaseValidate  Phase = "validate"
)

type PipelineConfig struct {
	SkipPrecheck  bool
	SkipSchema    bool
	SkipData      bool
	SkipValidate  bool
	OnErrorContinue bool
}

type PipelineResult struct {
	Phase   Phase
	Success bool
	Error   error
}

type Pipeline interface {
	Run(ctx context.Context, cfg PipelineConfig) ([]PipelineResult, error)
}
