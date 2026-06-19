package cmd

import (
	"os"
	"strconv"

	"github.com/spf13/cobra"
)

// resolveCDCEnable computes the effective CDC "enabled" state using a single
// priority chain: --enable-cdc/--disable-cdc flag > PG2TIDB_CDC_ENABLE env >
// cdc.enable (yaml). It is a pure function so the priority rules can be
// unit-tested without a live cobra command or process environment.
//
// Parameters:
//   - yamlEnable       : the cdc.enable value from the loaded config
//   - flagEnableSet    : whether --enable-cdc was passed on the command line
//   - flagEnableVal    : the boolean value of --enable-cdc (meaningful when flagEnableSet)
//   - flagDisableSet   : whether --disable-cdc was passed
//   - envEnable        : the raw value of PG2TIDB_CDC_ENABLE ("" if unset/unparseable)
func resolveCDCEnable(yamlEnable bool, flagEnableSet, flagEnableVal, flagDisableSet bool, envEnable string) bool {
	// Explicit flags win. If both are given, --enable-cdc takes precedence.
	if flagEnableSet {
		return flagEnableVal
	}
	if flagDisableSet {
		return false
	}
	// Environment variable (12-factor friendly for container deployments).
	if envEnable != "" {
		if b, err := strconv.ParseBool(envEnable); err == nil {
			return b
		}
	}
	// Config file (cdc.enable). Default false.
	return yamlEnable
}

// resolveCDCEnableFromCmd extracts the flag/env inputs from the cobra command
// and process environment, then delegates to resolveCDCEnable.
func resolveCDCEnableFromCmd(yamlEnable bool, cmd *cobra.Command) bool {
	var flagEnableSet, flagEnableVal, flagDisableSet bool
	if cmd.Flags().Changed("enable-cdc") {
		flagEnableSet = true
		flagEnableVal, _ = cmd.Flags().GetBool("enable-cdc")
	}
	if cmd.Flags().Changed("disable-cdc") {
		flagDisableSet = true
	}
	envEnable, _ := os.LookupEnv("PG2TIDB_CDC_ENABLE")
	return resolveCDCEnable(yamlEnable, flagEnableSet, flagEnableVal, flagDisableSet, envEnable)
}
