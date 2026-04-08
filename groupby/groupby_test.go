package groupby

import (
	"math"
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

func TestSize(t *testing.T) {
	df := testDF(t)
	gb := NewGroupBy(df, "dept")
	sizes := gb.Size()
	engVal, engNull := sizes.Loc("Eng")
	if engNull || engVal != int64(3) {
		t.Errorf("Size().Loc(Eng) = %v (null=%v), want 3", engVal, engNull)
	}
	salesVal, salesNull := sizes.Loc("Sales")
	if salesNull || salesVal != int64(2) {
		t.Errorf("Size().Loc(Sales) = %v (null=%v), want 2", salesVal, salesNull)
	}
}

func TestSubDF(t *testing.T) {
	df := testDF(t)
	gb := NewGroupBy(df, "dept")
	// Eng group: rows 0, 2, 4
	sub, err := gb.subDF([]int{0, 2, 4})
	if err != nil {
		t.Fatal(err)
	}
	rows, cols := sub.Shape()
	if rows != 3 {
		t.Errorf("subDF rows = %d, want 3", rows)
	}
	if cols != 3 {
		t.Errorf("subDF cols = %d, want 3", cols)
	}
	// Check first row of sub-DF has Alice
	val, err := sub.At(0, "name")
	if err != nil {
		t.Fatal(err)
	}
	if val != "Alice" {
		t.Errorf("subDF At(0, name) = %v, want Alice", val)
	}
	// Check salary of Charlie (row 1 in sub-DF, row 2 in original)
	val, err = sub.At(1, "salary")
	if err != nil {
		t.Fatal(err)
	}
	if val != 120000.0 {
		t.Errorf("subDF At(1, salary) = %v, want 120000", val)
	}
}

func TestSum(t *testing.T) {
	df := testDF(t)
	gb := NewGroupBy(df, "dept")
	result, err := gb.Sum()
	if err != nil {
		t.Fatal(err)
	}
	rows, _ := result.Shape()
	if rows != 2 {
		t.Errorf("Sum() rows = %d, want 2", rows)
	}
	// Check Eng salary sum: 100000 + 120000 + 110000 = 330000
	// Check Sales salary sum: 80000 + 90000 = 170000
	// Result is sorted by group key, so Eng=row0, Sales=row1
	engSalary, err := result.At(0, "salary")
	if err != nil {
		t.Fatal(err)
	}
	if engSalary != 330000.0 {
		t.Errorf("Sum() Eng salary = %v, want 330000", engSalary)
	}
	salesSalary, err := result.At(1, "salary")
	if err != nil {
		t.Fatal(err)
	}
	if salesSalary != 170000.0 {
		t.Errorf("Sum() Sales salary = %v, want 170000", salesSalary)
	}
}

func TestCount(t *testing.T) {
	df := testDF(t)
	gb := NewGroupBy(df, "dept")
	result, err := gb.Count()
	if err != nil {
		t.Fatal(err)
	}
	rows, _ := result.Shape()
	if rows != 2 {
		t.Errorf("Count() rows = %d, want 2", rows)
	}
	// Count includes ALL non-key columns, not just numeric
	// Eng has 3 rows, Sales has 2
	engName, err := result.At(0, "name")
	if err != nil {
		t.Fatal(err)
	}
	if engName != int64(3) {
		t.Errorf("Count() Eng name = %v, want 3", engName)
	}
	engSalary, err := result.At(0, "salary")
	if err != nil {
		t.Fatal(err)
	}
	if engSalary != int64(3) {
		t.Errorf("Count() Eng salary = %v, want 3", engSalary)
	}
	salesName, err := result.At(1, "name")
	if err != nil {
		t.Fatal(err)
	}
	if salesName != int64(2) {
		t.Errorf("Count() Sales name = %v, want 2", salesName)
	}
}

func TestMean(t *testing.T) {
	df := testDF(t)
	gb := NewGroupBy(df, "dept")
	result, err := gb.Mean()
	if err != nil {
		t.Fatal(err)
	}
	// Eng mean salary: (100000+120000+110000)/3 = 110000
	engSalary, err := result.At(0, "salary")
	if err != nil {
		t.Fatal(err)
	}
	if engSalary != 110000.0 {
		t.Errorf("Mean() Eng salary = %v, want 110000", engSalary)
	}
	// Sales mean salary: (80000+90000)/2 = 85000
	salesSalary, err := result.At(1, "salary")
	if err != nil {
		t.Fatal(err)
	}
	if salesSalary != 85000.0 {
		t.Errorf("Mean() Sales salary = %v, want 85000", salesSalary)
	}
}

func TestMin(t *testing.T) {
	df := testDF(t)
	gb := NewGroupBy(df, "dept")
	result, err := gb.Min()
	if err != nil {
		t.Fatal(err)
	}
	// Eng min salary: min(100000, 120000, 110000) = 100000
	engSalary, err := result.At(0, "salary")
	if err != nil {
		t.Fatal(err)
	}
	if engSalary != 100000.0 {
		t.Errorf("Min() Eng salary = %v, want 100000", engSalary)
	}
	// Sales min salary: min(80000, 90000) = 80000
	salesSalary, err := result.At(1, "salary")
	if err != nil {
		t.Fatal(err)
	}
	if salesSalary != 80000.0 {
		t.Errorf("Min() Sales salary = %v, want 80000", salesSalary)
	}
}

func TestMax(t *testing.T) {
	df := testDF(t)
	gb := NewGroupBy(df, "dept")
	result, err := gb.Max()
	if err != nil {
		t.Fatal(err)
	}
	// Eng max salary: max(100000, 120000, 110000) = 120000
	engSalary, err := result.At(0, "salary")
	if err != nil {
		t.Fatal(err)
	}
	if engSalary != 120000.0 {
		t.Errorf("Max() Eng salary = %v, want 120000", engSalary)
	}
	// Sales max salary: max(80000, 90000) = 90000
	salesSalary, err := result.At(1, "salary")
	if err != nil {
		t.Fatal(err)
	}
	if salesSalary != 90000.0 {
		t.Errorf("Max() Sales salary = %v, want 90000", salesSalary)
	}
}

func TestStd(t *testing.T) {
	df := testDF(t)
	gb := NewGroupBy(df, "dept")
	result, err := gb.Std()
	if err != nil {
		t.Fatal(err)
	}
	// Eng salaries: 100000, 120000, 110000 -> mean=110000
	// variance = ((100000-110000)^2 + (120000-110000)^2 + (110000-110000)^2) / (3-1)
	//          = (1e8 + 1e8 + 0) / 2 = 1e8
	// std = sqrt(1e8) = 10000
	engSalary, err := result.At(0, "salary")
	if err != nil {
		t.Fatal(err)
	}
	engStd, ok := engSalary.(float64)
	if !ok {
		t.Fatalf("Std() Eng salary type = %T, want float64", engSalary)
	}
	if math.Abs(engStd-10000.0) > 0.01 {
		t.Errorf("Std() Eng salary = %v, want 10000", engStd)
	}
	// Sales salaries: 80000, 90000 -> mean=85000
	// variance = ((80000-85000)^2 + (90000-85000)^2) / (2-1) = 5e7
	// std = sqrt(5e7) ≈ 7071.07
	salesSalary, err := result.At(1, "salary")
	if err != nil {
		t.Fatal(err)
	}
	salesStd, ok := salesSalary.(float64)
	if !ok {
		t.Fatalf("Std() Sales salary type = %T, want float64", salesSalary)
	}
	if math.Abs(salesStd-7071.07) > 0.1 {
		t.Errorf("Std() Sales salary = %v, want ~7071.07", salesStd)
	}
}

func TestStd_SingleElement(t *testing.T) {
	df, err := dataframe.New(map[string]any{
		"key": []string{"A", "B"},
		"val": []float64{10.0, 20.0},
	})
	if err != nil {
		t.Fatal(err)
	}
	gb := NewGroupBy(df, "key")
	result, err := gb.Std()
	if err != nil {
		t.Fatal(err)
	}
	// Single element groups produce NaN
	aVal, err := result.At(0, "val")
	if err != nil {
		t.Fatal(err)
	}
	aStd, ok := aVal.(float64)
	if !ok {
		t.Fatalf("Std() A val type = %T, want float64", aVal)
	}
	if !math.IsNaN(aStd) {
		t.Errorf("Std() single element = %v, want NaN", aStd)
	}
}

func TestFirst(t *testing.T) {
	df := testDF(t)
	gb := NewGroupBy(df, "dept")
	result, err := gb.First()
	if err != nil {
		t.Fatal(err)
	}
	rows, _ := result.Shape()
	if rows != 2 {
		t.Errorf("First() rows = %d, want 2", rows)
	}
	// First includes ALL columns, not just numeric
	// Eng first: row 0 -> Alice, 100000
	engName, err := result.At(0, "name")
	if err != nil {
		t.Fatal(err)
	}
	if engName != "Alice" {
		t.Errorf("First() Eng name = %v, want Alice", engName)
	}
	engSalary, err := result.At(0, "salary")
	if err != nil {
		t.Fatal(err)
	}
	if engSalary != 100000.0 {
		t.Errorf("First() Eng salary = %v, want 100000", engSalary)
	}
	// Sales first: row 1 -> Bob, 80000
	salesName, err := result.At(1, "name")
	if err != nil {
		t.Fatal(err)
	}
	if salesName != "Bob" {
		t.Errorf("First() Sales name = %v, want Bob", salesName)
	}
}

func TestLast(t *testing.T) {
	df := testDF(t)
	gb := NewGroupBy(df, "dept")
	result, err := gb.Last()
	if err != nil {
		t.Fatal(err)
	}
	rows, _ := result.Shape()
	if rows != 2 {
		t.Errorf("Last() rows = %d, want 2", rows)
	}
	// Eng last: row 4 -> Eve, 110000
	engName, err := result.At(0, "name")
	if err != nil {
		t.Fatal(err)
	}
	if engName != "Eve" {
		t.Errorf("Last() Eng name = %v, want Eve", engName)
	}
	engSalary, err := result.At(0, "salary")
	if err != nil {
		t.Fatal(err)
	}
	if engSalary != 110000.0 {
		t.Errorf("Last() Eng salary = %v, want 110000", engSalary)
	}
	// Sales last: row 3 -> Diana, 90000
	salesName, err := result.At(1, "name")
	if err != nil {
		t.Fatal(err)
	}
	if salesName != "Diana" {
		t.Errorf("Last() Sales name = %v, want Diana", salesName)
	}
}
