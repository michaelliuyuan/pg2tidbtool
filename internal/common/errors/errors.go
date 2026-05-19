package errors

import "fmt"

type ErrorCode string

const (
	ErrConfigLoad       ErrorCode = "CONFIG_LOAD"
	ErrConfigValidate   ErrorCode = "CONFIG_VALIDATE"
	ErrSourceConnect    ErrorCode = "SOURCE_CONNECT"
	ErrTargetConnect    ErrorCode = "TARGET_CONNECT"
	ErrSchemaFetch      ErrorCode = "SCHEMA_FETCH"
	ErrSchemaConvert    ErrorCode = "SCHEMA_CONVERT"
	ErrSchemaApply      ErrorCode = "SCHEMA_APPLY"
	ErrDataExport       ErrorCode = "DATA_EXPORT"
	ErrDataImport       ErrorCode = "DATA_IMPORT"
	ErrDataTransform    ErrorCode = "DATA_TRANSFORM"
	ErrLightningInit    ErrorCode = "LIGHTNING_INIT"
	ErrLightningExec    ErrorCode = "LIGHTNING_EXEC"
	ErrValidateRowCount ErrorCode = "VALIDATE_ROW_COUNT"
	ErrValidateChecksum ErrorCode = "VALIDATE_CHECKSUM"
	ErrValidateSampling ErrorCode = "VALIDATE_SAMPLING"
	ErrCheckpointSave   ErrorCode = "CHECKPOINT_SAVE"
	ErrCheckpointLoad   ErrorCode = "CHECKPOINT_LOAD"
	ErrPrecheckConnect  ErrorCode = "PRECHECK_CONNECT"
	ErrPrecheckDisk     ErrorCode = "PRECHECK_DISK"
	ErrPrecheckCompat   ErrorCode = "PRECHECK_COMPAT"
	ErrOrchestrator     ErrorCode = "ORCHESTRATOR"
	ErrInternal         ErrorCode = "INTERNAL"
)

type ErrorStrategy string

const (
	StrategyAbort ErrorStrategy = "abort"
	StrategySkip  ErrorStrategy = "skip"
	StrategyRetry ErrorStrategy = "retry"
)

type Error struct {
	Code     ErrorCode
	Message  string
	Table    string
	Phase    string
	Cause    error
	Strategy ErrorStrategy
}

func (e *Error) Error() string {
	if e.Table != "" {
		return fmt.Sprintf("[%s][%s] %s: %v", e.Code, e.Table, e.Message, e.Cause)
	}
	return fmt.Sprintf("[%s] %s: %v", e.Code, e.Message, e.Cause)
}

func (e *Error) Unwrap() error {
	return e.Cause
}

func New(code ErrorCode, message string) *Error {
	return &Error{
		Code:     code,
		Message:  message,
		Strategy: StrategyAbort,
	}
}

func Wrap(code ErrorCode, message string, cause error) *Error {
	return &Error{
		Code:     code,
		Message:  message,
		Cause:    cause,
		Strategy: StrategyAbort,
	}
}

func WithTable(e *Error, table string) *Error {
	e.Table = table
	return e
}

func WithPhase(e *Error, phase string) *Error {
	e.Phase = phase
	return e
}

func WithStrategy(e *Error, strategy ErrorStrategy) *Error {
	e.Strategy = strategy
	return e
}

func ShouldAbort(err error, defaultStrategy ErrorStrategy) bool {
	if e, ok := err.(*Error); ok {
		return e.Strategy == StrategyAbort
	}
	return defaultStrategy != StrategySkip
}

func GetCode(err error) ErrorCode {
	if e, ok := err.(*Error); ok {
		return e.Code
	}
	return ErrInternal
}
