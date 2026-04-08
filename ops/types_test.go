package ops

import "testing"

func TestJoinType_String(t *testing.T) {
	tests := []struct {
		jt   JoinType
		want string
	}{
		{Inner, "inner"},
		{Left, "left"},
		{Right, "right"},
		{Outer, "outer"},
	}
	for _, tt := range tests {
		if got := tt.jt.String(); got != tt.want {
			t.Errorf("JoinType(%d).String() = %q, want %q", tt.jt, got, tt.want)
		}
	}
}

func TestJoinType_Values(t *testing.T) {
	if Inner != 0 {
		t.Errorf("Inner = %d, want 0", Inner)
	}
	if Left != 1 {
		t.Errorf("Left = %d, want 1", Left)
	}
	if Right != 2 {
		t.Errorf("Right = %d, want 2", Right)
	}
	if Outer != 3 {
		t.Errorf("Outer = %d, want 3", Outer)
	}
}

func TestAggFunc_String(t *testing.T) {
	tests := []struct {
		af   AggFunc
		want string
	}{
		{AggSum, "sum"},
		{AggMean, "mean"},
		{AggCount, "count"},
		{AggMin, "min"},
		{AggMax, "max"},
		{AggStd, "std"},
		{AggFirst, "first"},
		{AggLast, "last"},
	}
	for _, tt := range tests {
		if got := tt.af.String(); got != tt.want {
			t.Errorf("AggFunc(%d).String() = %q, want %q", tt.af, got, tt.want)
		}
	}
}

func TestAggFunc_Values(t *testing.T) {
	if AggSum != 0 {
		t.Errorf("AggSum = %d, want 0", AggSum)
	}
	if AggLast != 7 {
		t.Errorf("AggLast = %d, want 7", AggLast)
	}
}
