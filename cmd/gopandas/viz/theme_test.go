package viz

import (
	"os"
	"testing"
)

func setEnv(t *testing.T, key, value string) {
	t.Helper()

	oldValue, hadOldValue := os.LookupEnv(key)
	if err := os.Setenv(key, value); err != nil {
		t.Fatalf("failed to set %s: %v", key, err)
	}

	t.Cleanup(func() {
		if hadOldValue {
			if err := os.Setenv(key, oldValue); err != nil {
				t.Fatalf("failed to restore %s: %v", key, err)
			}
			return
		}

		if err := os.Unsetenv(key); err != nil {
			t.Fatalf("failed to unset %s: %v", key, err)
		}
	})
}

func unsetEnv(t *testing.T, key string) {
	t.Helper()

	oldValue, hadOldValue := os.LookupEnv(key)
	if err := os.Unsetenv(key); err != nil {
		t.Fatalf("failed to unset %s: %v", key, err)
	}

	t.Cleanup(func() {
		if hadOldValue {
			if err := os.Setenv(key, oldValue); err != nil {
				t.Fatalf("failed to restore %s: %v", key, err)
			}
		}
	})
}

func sameTheme(a, b Theme) bool {
	const sample = "sample"
	return a.Text.Render(sample) == b.Text.Render(sample) &&
		a.Section.Render(sample) == b.Section.Render(sample) &&
		a.Table.Render(sample) == b.Table.Render(sample) &&
		a.TableHeader.Render(sample) == b.TableHeader.Render(sample) &&
		a.Chart.Render(sample) == b.Chart.Render(sample) &&
		a.Panel.Render(sample) == b.Panel.Render(sample)
}

func TestDetectDefaultDark(t *testing.T) {
	unsetEnv(t, "GOPANDAS_THEME")
	unsetEnv(t, "COLORFGBG")
	unsetEnv(t, "TERM_PROGRAM")

	got := Detect("")
	if !sameTheme(got, DarkTheme()) {
		t.Fatal("expected default theme to be dark")
	}
}

func TestDetectFlagDark(t *testing.T) {
	setEnv(t, "GOPANDAS_THEME", "light")
	setEnv(t, "COLORFGBG", "15;255")
	setEnv(t, "TERM_PROGRAM", "Apple_Terminal")

	got := Detect("dark")
	if !sameTheme(got, DarkTheme()) {
		t.Fatal("expected dark flag to override environment")
	}
}

func TestDetectFlagLight(t *testing.T) {
	setEnv(t, "GOPANDAS_THEME", "dark")
	setEnv(t, "COLORFGBG", "0;0")
	unsetEnv(t, "TERM_PROGRAM")

	got := Detect("light")
	if !sameTheme(got, LightTheme()) {
		t.Fatal("expected light flag to override environment")
	}
}

func TestDarkAndLightDiffer(t *testing.T) {
	if sameTheme(DarkTheme(), LightTheme()) {
		t.Fatal("expected dark and light themes to differ")
	}
}

func TestDetectUsesGOPANDASTHEMEOverride(t *testing.T) {
	tests := []struct {
		name     string
		envValue string
		want     Theme
	}{
		{name: "dark", envValue: "dark", want: DarkTheme()},
		{name: "light", envValue: "light", want: LightTheme()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setEnv(t, "GOPANDAS_THEME", tt.envValue)
			setEnv(t, "COLORFGBG", "0;255")
			setEnv(t, "TERM_PROGRAM", "Apple_Terminal")

			got := Detect("")
			if !sameTheme(got, tt.want) {
				t.Fatalf("expected GOPANDAS_THEME=%s to win", tt.envValue)
			}
		})
	}
}

func TestDetectParsesCOLORFGBG(t *testing.T) {
	tests := []struct {
		name      string
		colorfgbg string
		termProg  string
		want      Theme
	}{
		{name: "dark background token", colorfgbg: "15;0", want: DarkTheme()},
		{name: "light background token", colorfgbg: "0;255", want: LightTheme()},
		{name: "invalid token falls through", colorfgbg: "0;bad", termProg: "Apple_Terminal", want: LightTheme()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			unsetEnv(t, "GOPANDAS_THEME")
			setEnv(t, "COLORFGBG", tt.colorfgbg)
			if tt.termProg == "" {
				unsetEnv(t, "TERM_PROGRAM")
			} else {
				setEnv(t, "TERM_PROGRAM", tt.termProg)
			}

			got := Detect("")
			if !sameTheme(got, tt.want) {
				t.Fatalf("unexpected theme for COLORFGBG=%q TERM_PROGRAM=%q", tt.colorfgbg, tt.termProg)
			}
		})
	}
}

func TestDetectAppleTerminalFallsBackToLight(t *testing.T) {
	unsetEnv(t, "GOPANDAS_THEME")
	unsetEnv(t, "COLORFGBG")
	setEnv(t, "TERM_PROGRAM", "Apple_Terminal")

	got := Detect("")
	if !sameTheme(got, LightTheme()) {
		t.Fatal("expected Apple Terminal fallback to light theme")
	}
}
