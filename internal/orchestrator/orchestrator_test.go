package orchestrator

import (
	"testing"
)

func TestPhaseConstants(t *testing.T) {
	if PhasePrecheck != "precheck" {
		t.Errorf("expected precheck, got %s", PhasePrecheck)
	}
	if PhaseSchema != "schema" {
		t.Errorf("expected schema, got %s", PhaseSchema)
	}
	if PhaseData != "data" {
		t.Errorf("expected data, got %s", PhaseData)
	}
	if PhaseValidate != "validate" {
		t.Errorf("expected validate, got %s", PhaseValidate)
	}
}

func TestPipelineConfig(t *testing.T) {
	cfg := PipelineConfig{
		SkipPrecheck:    true,
		SkipSchema:      false,
		SkipData:        false,
		SkipValidate:    true,
		OnErrorContinue: false,
	}
	if !cfg.SkipPrecheck {
		t.Error("SkipPrecheck should be true")
	}
	if cfg.SkipSchema {
		t.Error("SkipSchema should be false")
	}
}

func TestPipelineResult(t *testing.T) {
	r := PipelineResult{
		Phase:   PhaseSchema,
		Success: true,
		Error:   nil,
	}
	if r.Phase != PhaseSchema {
		t.Error("phase mismatch")
	}
	if !r.Success {
		t.Error("should be success")
	}
}
