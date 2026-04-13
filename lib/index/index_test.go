package index

import (
	"testing"
)

// ---- RangeIndex ----

func TestRangeIndex_Len(t *testing.T) {
	idx := NewRangeIndex(5, "")
	if idx.Len() != 5 {
		t.Errorf("Len() = %d, want 5", idx.Len())
	}
}

func TestRangeIndex_LenZero(t *testing.T) {
	idx := NewRangeIndex(0, "")
	if idx.Len() != 0 {
		t.Errorf("Len() = %d, want 0", idx.Len())
	}
}

func TestRangeIndex_Labels(t *testing.T) {
	idx := NewRangeIndex(3, "")
	labels := idx.Labels()
	if len(labels) != 3 {
		t.Fatalf("Labels() length = %d, want 3", len(labels))
	}
	for i, l := range labels {
		v, ok := l.(int)
		if !ok {
			t.Errorf("Labels()[%d] is not int", i)
			continue
		}
		if v != i {
			t.Errorf("Labels()[%d] = %d, want %d", i, v, i)
		}
	}
}

func TestRangeIndex_Loc(t *testing.T) {
	idx := NewRangeIndex(5, "")
	tests := []struct {
		label   any
		wantPos int
		wantOK  bool
	}{
		{0, 0, true},
		{4, 4, true},
		{5, -1, false},
		{-1, -1, false},
		{"a", -1, false},
	}
	for _, tt := range tests {
		pos, ok := idx.Loc(tt.label)
		if pos != tt.wantPos || ok != tt.wantOK {
			t.Errorf("Loc(%v) = (%d, %v), want (%d, %v)", tt.label, pos, ok, tt.wantPos, tt.wantOK)
		}
	}
}

func TestRangeIndex_Slice(t *testing.T) {
	idx := NewRangeIndex(10, "myidx")
	s := idx.Slice(2, 5)
	if s.Len() != 3 {
		t.Errorf("Slice(2,5).Len() = %d, want 3", s.Len())
	}
	if s.Name() != "myidx" {
		t.Errorf("Slice name = %q, want %q", s.Name(), "myidx")
	}
	labels := s.Labels()
	for i, l := range labels {
		if l.(int) != i {
			t.Errorf("Slice labels[%d] = %v, want %d", i, l, i)
		}
	}
}

func TestRangeIndex_SliceClamp(t *testing.T) {
	idx := NewRangeIndex(5, "")
	s := idx.Slice(-1, 100)
	if s.Len() != 5 {
		t.Errorf("Slice(-1,100).Len() = %d, want 5", s.Len())
	}
	s = idx.Slice(3, 3)
	if s.Len() != 0 {
		t.Errorf("Slice(3,3).Len() = %d, want 0", s.Len())
	}
}

func TestRangeIndex_Name(t *testing.T) {
	idx := NewRangeIndex(3, "row_id")
	if idx.Name() != "row_id" {
		t.Errorf("Name() = %q, want %q", idx.Name(), "row_id")
	}
}

func TestRangeIndex_ImplementsIndex(t *testing.T) {
	var _ Index = (*RangeIndex)(nil)
}

func TestNewRangeIndex_NegativePanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("NewRangeIndex(-1) did not panic")
		}
	}()
	NewRangeIndex(-1, "")
}

// ---- StringIndex ----

func TestStringIndex_Len(t *testing.T) {
	idx := NewStringIndex([]string{"a", "b", "c"}, "cols")
	if idx.Len() != 3 {
		t.Errorf("Len() = %d, want 3", idx.Len())
	}
}

func TestStringIndex_Labels(t *testing.T) {
	idx := NewStringIndex([]string{"x", "y", "z"}, "")
	labels := idx.Labels()
	want := []string{"x", "y", "z"}
	for i, l := range labels {
		s, ok := l.(string)
		if !ok {
			t.Errorf("Labels()[%d] is not string", i)
			continue
		}
		if s != want[i] {
			t.Errorf("Labels()[%d] = %q, want %q", i, s, want[i])
		}
	}
}

func TestStringIndex_Loc(t *testing.T) {
	idx := NewStringIndex([]string{"alpha", "beta", "gamma"}, "")
	tests := []struct {
		label   any
		wantPos int
		wantOK  bool
	}{
		{"alpha", 0, true},
		{"gamma", 2, true},
		{"delta", -1, false},
		{42, -1, false},
	}
	for _, tt := range tests {
		pos, ok := idx.Loc(tt.label)
		if pos != tt.wantPos || ok != tt.wantOK {
			t.Errorf("Loc(%v) = (%d, %v), want (%d, %v)", tt.label, pos, ok, tt.wantPos, tt.wantOK)
		}
	}
}

func TestStringIndex_LocDuplicate(t *testing.T) {
	idx := NewStringIndex([]string{"a", "b", "a"}, "")
	pos, ok := idx.Loc("a")
	if pos != 0 || !ok {
		t.Errorf("Loc(\"a\") = (%d, %v), want (0, true)", pos, ok)
	}
}

func TestStringIndex_Slice(t *testing.T) {
	idx := NewStringIndex([]string{"a", "b", "c", "d", "e"}, "letters")
	s := idx.Slice(1, 4)
	if s.Len() != 3 {
		t.Errorf("Slice(1,4).Len() = %d, want 3", s.Len())
	}
	if s.Name() != "letters" {
		t.Errorf("Slice name = %q, want %q", s.Name(), "letters")
	}
	labels := s.Labels()
	want := []string{"b", "c", "d"}
	for i, l := range labels {
		if l.(string) != want[i] {
			t.Errorf("Slice labels[%d] = %v, want %q", i, l, want[i])
		}
	}
}

func TestStringIndex_SliceClamp(t *testing.T) {
	idx := NewStringIndex([]string{"a", "b", "c"}, "")
	s := idx.Slice(-5, 100)
	if s.Len() != 3 {
		t.Errorf("Slice(-5,100).Len() = %d, want 3", s.Len())
	}
	s = idx.Slice(2, 2)
	if s.Len() != 0 {
		t.Errorf("Slice(2,2).Len() = %d, want 0", s.Len())
	}
}

func TestStringIndex_Name(t *testing.T) {
	idx := NewStringIndex([]string{"a"}, "city")
	if idx.Name() != "city" {
		t.Errorf("Name() = %q, want %q", idx.Name(), "city")
	}
}

func TestStringIndex_ImplementsIndex(t *testing.T) {
	var _ Index = (*StringIndex)(nil)
}

// ---- Int64Index ----

func TestInt64Index_Len(t *testing.T) {
	idx := NewInt64Index([]int64{10, 20, 30}, "ids")
	if idx.Len() != 3 {
		t.Errorf("Len() = %d, want 3", idx.Len())
	}
}

func TestInt64Index_Labels(t *testing.T) {
	idx := NewInt64Index([]int64{100, 200, 300}, "")
	labels := idx.Labels()
	want := []int64{100, 200, 300}
	for i, l := range labels {
		v, ok := l.(int64)
		if !ok {
			t.Errorf("Labels()[%d] is not int64", i)
			continue
		}
		if v != want[i] {
			t.Errorf("Labels()[%d] = %d, want %d", i, v, want[i])
		}
	}
}

func TestInt64Index_Loc(t *testing.T) {
	idx := NewInt64Index([]int64{10, 20, 30, 40, 50}, "")
	tests := []struct {
		label   any
		wantPos int
		wantOK  bool
	}{
		{int64(10), 0, true},
		{int64(50), 4, true},
		{int64(99), -1, false},
		{"hello", -1, false},
		{42, -1, false}, // int, not int64
	}
	for _, tt := range tests {
		pos, ok := idx.Loc(tt.label)
		if pos != tt.wantPos || ok != tt.wantOK {
			t.Errorf("Loc(%v) = (%d, %v), want (%d, %v)", tt.label, pos, ok, tt.wantPos, tt.wantOK)
		}
	}
}

func TestInt64Index_LocDuplicate(t *testing.T) {
	idx := NewInt64Index([]int64{5, 10, 5}, "")
	pos, ok := idx.Loc(int64(5))
	if pos != 0 || !ok {
		t.Errorf("Loc(5) = (%d, %v), want (0, true)", pos, ok)
	}
}

func TestInt64Index_Slice(t *testing.T) {
	idx := NewInt64Index([]int64{10, 20, 30, 40, 50}, "nums")
	s := idx.Slice(1, 4)
	if s.Len() != 3 {
		t.Errorf("Slice(1,4).Len() = %d, want 3", s.Len())
	}
	if s.Name() != "nums" {
		t.Errorf("Slice name = %q, want %q", s.Name(), "nums")
	}
	labels := s.Labels()
	want := []int64{20, 30, 40}
	for i, l := range labels {
		if l.(int64) != want[i] {
			t.Errorf("Slice labels[%d] = %v, want %d", i, l, want[i])
		}
	}
}

func TestInt64Index_SliceClamp(t *testing.T) {
	idx := NewInt64Index([]int64{1, 2, 3}, "")
	s := idx.Slice(-10, 999)
	if s.Len() != 3 {
		t.Errorf("Slice(-10,999).Len() = %d, want 3", s.Len())
	}
	s = idx.Slice(1, 1)
	if s.Len() != 0 {
		t.Errorf("Slice(1,1).Len() = %d, want 0", s.Len())
	}
}

func TestInt64Index_Name(t *testing.T) {
	idx := NewInt64Index([]int64{1}, "year")
	if idx.Name() != "year" {
		t.Errorf("Name() = %q, want %q", idx.Name(), "year")
	}
}

func TestInt64Index_ImplementsIndex(t *testing.T) {
	var _ Index = (*Int64Index)(nil)
}

// ---- MultiIndex ----

func TestMultiIndex_Len(t *testing.T) {
	levels := [][]any{
		{"a", "a", "b"},
		{int64(1), int64(2), int64(1)},
	}
	idx := NewMultiIndex(levels, "multi")
	if idx.Len() != 3 {
		t.Errorf("Len() = %d, want 3", idx.Len())
	}
}

func TestMultiIndex_Labels(t *testing.T) {
	levels := [][]any{
		{"x", "y"},
		{int64(10), int64(20)},
	}
	idx := NewMultiIndex(levels, "")
	labels := idx.Labels()
	if len(labels) != 2 {
		t.Fatalf("Labels() length = %d, want 2", len(labels))
	}
	row0, ok := labels[0].([]any)
	if !ok {
		t.Fatalf("Labels()[0] is not []any")
	}
	if row0[0] != "x" || row0[1] != int64(10) {
		t.Errorf("Labels()[0] = %v, want [x 10]", row0)
	}
	row1 := labels[1].([]any)
	if row1[0] != "y" || row1[1] != int64(20) {
		t.Errorf("Labels()[1] = %v, want [y 20]", row1)
	}
}

func TestMultiIndex_Loc(t *testing.T) {
	levels := [][]any{
		{"a", "a", "b"},
		{int64(1), int64(2), int64(1)},
	}
	idx := NewMultiIndex(levels, "")
	tests := []struct {
		label   any
		wantPos int
		wantOK  bool
	}{
		{[]any{"a", int64(1)}, 0, true},
		{[]any{"a", int64(2)}, 1, true},
		{[]any{"b", int64(1)}, 2, true},
		{[]any{"c", int64(1)}, -1, false},
		{[]any{"a"}, -1, false},
		{"a", -1, false},
	}
	for _, tt := range tests {
		pos, ok := idx.Loc(tt.label)
		if pos != tt.wantPos || ok != tt.wantOK {
			t.Errorf("Loc(%v) = (%d, %v), want (%d, %v)", tt.label, pos, ok, tt.wantPos, tt.wantOK)
		}
	}
}

func TestMultiIndex_Slice(t *testing.T) {
	levels := [][]any{
		{"a", "a", "b", "b"},
		{int64(1), int64(2), int64(1), int64(2)},
	}
	idx := NewMultiIndex(levels, "mi")
	s := idx.Slice(1, 3)
	if s.Len() != 2 {
		t.Errorf("Slice(1,3).Len() = %d, want 2", s.Len())
	}
	if s.Name() != "mi" {
		t.Errorf("Slice name = %q, want %q", s.Name(), "mi")
	}
	labels := s.Labels()
	row0 := labels[0].([]any)
	if row0[0] != "a" || row0[1] != int64(2) {
		t.Errorf("Slice labels[0] = %v, want [a 2]", row0)
	}
}

func TestMultiIndex_SliceClamp(t *testing.T) {
	levels := [][]any{
		{"a", "b"},
		{int64(1), int64(2)},
	}
	idx := NewMultiIndex(levels, "")
	s := idx.Slice(-1, 100)
	if s.Len() != 2 {
		t.Errorf("Slice(-1,100).Len() = %d, want 2", s.Len())
	}
	s = idx.Slice(1, 1)
	if s.Len() != 0 {
		t.Errorf("Slice(1,1).Len() = %d, want 0", s.Len())
	}
}

func TestMultiIndex_Name(t *testing.T) {
	levels := [][]any{{"a"}, {int64(1)}}
	idx := NewMultiIndex(levels, "hierarchical")
	if idx.Name() != "hierarchical" {
		t.Errorf("Name() = %q, want %q", idx.Name(), "hierarchical")
	}
}

func TestMultiIndex_ImplementsIndex(t *testing.T) {
	var _ Index = (*MultiIndex)(nil)
}

func TestNewMultiIndex_PanicsOnMismatchedLengths(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("NewMultiIndex with mismatched level lengths did not panic")
		}
	}()
	levels := [][]any{
		{"a", "b", "c"},
		{int64(1), int64(2)},
	}
	NewMultiIndex(levels, "")
}

func TestNewMultiIndex_PanicsOnNoLevels(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("NewMultiIndex with no levels did not panic")
		}
	}()
	NewMultiIndex(nil, "")
}
