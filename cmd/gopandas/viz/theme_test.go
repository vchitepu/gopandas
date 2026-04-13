package viz

import "testing"

func sameTheme(a, b Theme) bool {
	const sample = "sample"
	return a.Text.Render(sample) == b.Text.Render(sample) &&
		a.Section.Render(sample) == b.Section.Render(sample) &&
		a.Table.Render(sample) == b.Table.Render(sample) &&
		a.Chart.Render(sample) == b.Chart.Render(sample) &&
		a.Panel.Render(sample) == b.Panel.Render(sample)
}

func TestDetectDefaultDark(t *testing.T) {
	t.Setenv("GOPANDAS_THEME", "")
	t.Setenv("COLORFGBG", "")
	t.Setenv("TERM_PROGRAM", "")

	got := Detect("")
	if !sameTheme(got, DarkTheme()) {
		t.Fatal("expected default theme to be dark")
	}
}

func TestDetectFlagDark(t *testing.T) {
	t.Setenv("GOPANDAS_THEME", "light")
	t.Setenv("COLORFGBG", "15;255")
	t.Setenv("TERM_PROGRAM", "Apple_Terminal")

	got := Detect("dark")
	if !sameTheme(got, DarkTheme()) {
		t.Fatal("expected dark flag to override environment")
	}
}

func TestDetectFlagLight(t *testing.T) {
	t.Setenv("GOPANDAS_THEME", "dark")
	t.Setenv("COLORFGBG", "0;0")
	t.Setenv("TERM_PROGRAM", "")

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
