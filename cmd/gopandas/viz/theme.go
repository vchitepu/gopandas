package viz

import (
	"os"
	"strconv"
	"strings"

	"github.com/charmbracelet/lipgloss"
)

type Theme struct {
	Text    lipgloss.Style
	Section lipgloss.Style
	Table   lipgloss.Style
	Chart   lipgloss.Style
	Panel   lipgloss.Style
}

func DarkTheme() Theme {
	return Theme{
		Text:    lipgloss.NewStyle().Foreground(lipgloss.Color("252")),
		Section: lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("117")),
		Table:   lipgloss.NewStyle().Foreground(lipgloss.Color("250")),
		Chart:   lipgloss.NewStyle().Foreground(lipgloss.Color("221")),
		Panel: lipgloss.NewStyle().
			BorderStyle(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color("63")).
			Padding(0, 1),
	}
}

func LightTheme() Theme {
	return Theme{
		Text:    lipgloss.NewStyle().Foreground(lipgloss.Color("236")),
		Section: lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("25")),
		Table:   lipgloss.NewStyle().Foreground(lipgloss.Color("238")),
		Chart:   lipgloss.NewStyle().Foreground(lipgloss.Color("27")),
		Panel: lipgloss.NewStyle().
			BorderStyle(lipgloss.NormalBorder()).
			BorderForeground(lipgloss.Color("245")).
			Padding(0, 1),
	}
}

func Detect(flagValue string) Theme {
	switch normalizeTheme(flagValue) {
	case "dark":
		return DarkTheme()
	case "light":
		return LightTheme()
	}

	switch normalizeTheme(os.Getenv("GOPANDAS_THEME")) {
	case "dark":
		return DarkTheme()
	case "light":
		return LightTheme()
	}

	if isDark, ok := detectFromCOLORFGBG(os.Getenv("COLORFGBG")); ok {
		if isDark {
			return DarkTheme()
		}
		return LightTheme()
	}

	if os.Getenv("TERM_PROGRAM") == "Apple_Terminal" {
		return LightTheme()
	}

	return DarkTheme()
}

func normalizeTheme(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func detectFromCOLORFGBG(value string) (bool, bool) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return false, false
	}

	parts := strings.FieldsFunc(trimmed, func(r rune) bool {
		return r == ';' || r == ':'
	})
	if len(parts) == 0 {
		return false, false
	}

	bg, err := strconv.Atoi(parts[len(parts)-1])
	if err != nil {
		return false, false
	}

	return bg < 128, true
}
