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

func testMultiKeyDF(t *testing.T) dataframe.DataFrame {
	t.Helper()
	df, err := dataframe.New(map[string]any{
		"dept":   []string{"Eng", "Sales", "Eng", "Sales", "Eng"},
		"level":  []string{"Senior", "Junior", "Junior", "Senior", "Senior"},
		"salary": []float64{120000, 60000, 80000, 100000, 130000},
	})
	if err != nil {
		t.Fatalf("testMultiKeyDF: %v", err)
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

func TestNewGroupBy_MultiKey(t *testing.T) {
	df := testMultiKeyDF(t)
	gb := NewGroupBy(df, "dept", "level")
	if gb.NGroups() != 4 {
		t.Errorf("NGroups() = %d, want 4", gb.NGroups())
	}
}

func TestNewGroupBy_MultiKey_GroupContents(t *testing.T) {
	df := testMultiKeyDF(t)
	gb := NewGroupBy(df, "dept", "level")
	groups := gb.Groups()
	engSenior, ok := groups["Eng|Senior"]
	if !ok {
		t.Fatal("Groups() missing key 'Eng|Senior'")
	}
	if len(engSenior) != 2 {
		t.Errorf("Groups()[Eng|Senior] has %d rows, want 2", len(engSenior))
	}
	if engSenior[0] != 0 || engSenior[1] != 4 {
		t.Errorf("Groups()[Eng|Senior] = %v, want [0, 4]", engSenior)
	}
}

func TestNGroups_AllSame(t *testing.T) {
	df, err := dataframe.New(map[string]any{
		"key": []string{"A", "A", "A"},
		"val": []float64{1, 2, 3},
	})
	if err != nil {
		t.Fatal(err)
	}
	gb := NewGroupBy(df, "key")
	if gb.NGroups() != 1 {
		t.Errorf("NGroups() = %d, want 1", gb.NGroups())
	}
}

func TestNGroups_AllUnique(t *testing.T) {
	df, err := dataframe.New(map[string]any{
		"key": []string{"A", "B", "C"},
		"val": []float64{1, 2, 3},
	})
	if err != nil {
		t.Fatal(err)
	}
	gb := NewGroupBy(df, "key")
	if gb.NGroups() != 3 {
		t.Errorf("NGroups() = %d, want 3", gb.NGroups())
	}
}

func TestGroups_SingleKey(t *testing.T) {
	df := testDF(t)
	gb := NewGroupBy(df, "dept")
	groups := gb.Groups()

	eng, ok := groups["Eng"]
	if !ok {
		t.Fatal("Groups() missing key 'Eng'")
	}
	if len(eng) != 3 || eng[0] != 0 || eng[1] != 2 || eng[2] != 4 {
		t.Errorf("Groups()[Eng] = %v, want [0, 2, 4]", eng)
	}

	sales, ok := groups["Sales"]
	if !ok {
		t.Fatal("Groups() missing key 'Sales'")
	}
	if len(sales) != 2 || sales[0] != 1 || sales[1] != 3 {
		t.Errorf("Groups()[Sales] = %v, want [1, 3]", sales)
	}
}
