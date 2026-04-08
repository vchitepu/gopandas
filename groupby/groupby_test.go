package groupby

import (
	"testing"

	"github.com/vinaychitepu/gopandas/dataframe"
)

func testDF(t *testing.T) dataframe.DataFrame {
	t.Helper()
	df, err := dataframe.New(map[string]any{
		"name":   []string{"Alice", "Bob", "Charlie", "Diana", "Eve"},
		"dept":   []string{"Eng", "Sales", "Eng", "Sales", "Eng"},
		"salary": []float64{100000, 80000, 120000, 90000, 110000},
	})
	if err != nil {
		t.Fatalf("testDF: %v", err)
	}
	return df
}

func TestNewGroupBy_SingleKey(t *testing.T) {
	df := testDF(t)
	gb := NewGroupBy(df, "dept")
	if gb.NGroups() != 2 {
		t.Errorf("NGroups() = %d, want 2", gb.NGroups())
	}
}
