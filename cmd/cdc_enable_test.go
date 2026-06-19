package cmd

import "testing"

// TestResolveCDCEnable exhaustively checks the priority chain:
// --enable-cdc/--disable-cdc flag > PG2TIDB_CDC_ENABLE env > cdc.enable (yaml).
func TestResolveCDCEnable(t *testing.T) {
	cases := []struct {
		name           string
		yamlEnable     bool
		flagEnableSet  bool
		flagEnableVal  bool
		flagDisableSet bool
		envEnable      string
		want           bool
	}{
		{"yaml false, no overrides", false, false, false, false, "", false},
		{"yaml true, no overrides", true, false, false, false, "", true},

		// env beats yaml
		{"env=true beats yaml false", false, false, false, false, "true", true},
		{"env=false beats yaml true", true, false, false, false, "false", false},
		{"env=1 (alt spelling) beats yaml false", false, false, false, false, "1", true},
		{"env=0 beats yaml true", true, false, false, false, "0", false},

		// flag beats env beats yaml
		{"flag enable=true beats env=false and yaml false", false, true, true, false, "false", true},
		{"flag enable=false beats env=true and yaml true", true, true, false, false, "true", false},
		{"disable-cdc beats yaml true", true, false, false, true, "", false},
		{"disable-cdc beats env=true", false, false, false, true, "true", false},

		// enable-cdc precedence over disable-cdc when both set
		{"enable-cdc=true wins over disable-cdc", true, true, true, true, "", true},
		{"enable-cdc=false + disable-cdc → false", true, true, false, true, "", false},

		// invalid / empty env falls through to yaml
		{"invalid env falls through to yaml true", true, false, false, false, "yes", true},
		{"invalid env falls through to yaml false", false, false, false, false, "maybe", false},
		{"empty env falls through to yaml", true, false, false, false, "", true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := resolveCDCEnable(tc.yamlEnable, tc.flagEnableSet, tc.flagEnableVal, tc.flagDisableSet, tc.envEnable)
			if got != tc.want {
				t.Errorf("resolveCDCEnable(%v,%v,%v,%v,%q) = %v, want %v",
					tc.yamlEnable, tc.flagEnableSet, tc.flagEnableVal, tc.flagDisableSet, tc.envEnable, got, tc.want)
			}
		})
	}
}
