package dtype

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
)

func TestDTypeConstants(t *testing.T) {
	tests := []struct {
		dt   DType
		want int
	}{
		{Invalid, 0},
		{Int64, 1},
		{Float64, 2},
		{String, 3},
		{Bool, 4},
		{Timestamp, 5},
		{Dictionary, 6},
	}
	for _, tt := range tests {
		if int(tt.dt) != tt.want {
			t.Errorf("DType %d: got %d, want %d", tt.dt, int(tt.dt), tt.want)
		}
	}
}

func TestDTypeString(t *testing.T) {
	tests := []struct {
		dt   DType
		want string
	}{
		{Invalid, "invalid"},
		{Int64, "int64"},
		{Float64, "float64"},
		{String, "string"},
		{Bool, "bool"},
		{Timestamp, "timestamp"},
		{Dictionary, "dictionary"},
	}
	for _, tt := range tests {
		got := tt.dt.String()
		if got != tt.want {
			t.Errorf("DType(%d).String() = %q, want %q", tt.dt, got, tt.want)
		}
	}
}

func TestDTypeStringUnknown(t *testing.T) {
	unknown := DType(99)
	got := unknown.String()
	if got != "unknown" {
		t.Errorf("DType(99).String() = %q, want %q", got, "unknown")
	}
}

func TestDTypeToArrow(t *testing.T) {
	tests := []struct {
		dt      DType
		want    arrow.DataType
		wantErr bool
	}{
		{Int64, arrow.PrimitiveTypes.Int64, false},
		{Float64, arrow.PrimitiveTypes.Float64, false},
		{String, arrow.BinaryTypes.String, false},
		{Bool, arrow.FixedWidthTypes.Boolean, false},
		{Timestamp, arrow.FixedWidthTypes.Timestamp_ns, false},
		{Dictionary, &arrow.DictionaryType{
			IndexType: arrow.PrimitiveTypes.Int32,
			ValueType: arrow.BinaryTypes.String,
		}, false},
		{Invalid, nil, true},
		{DType(99), nil, true},
	}
	for _, tt := range tests {
		got, err := DTypeToArrow(tt.dt)
		if (err != nil) != tt.wantErr {
			t.Errorf("DTypeToArrow(%v): error = %v, wantErr %v", tt.dt, err, tt.wantErr)
			continue
		}
		if !tt.wantErr && got.ID() != tt.want.ID() {
			t.Errorf("DTypeToArrow(%v) = %v, want %v", tt.dt, got, tt.want)
		}
		if !tt.wantErr && got.ID() == arrow.DICTIONARY {
			dictType, ok := got.(*arrow.DictionaryType)
			if !ok {
				t.Errorf("DTypeToArrow(Dictionary): expected *arrow.DictionaryType, got %T", got)
			} else {
				if dictType.IndexType.ID() != arrow.INT32 {
					t.Errorf("DTypeToArrow(Dictionary): IndexType = %v, want INT32", dictType.IndexType)
				}
				if dictType.ValueType.ID() != arrow.STRING {
					t.Errorf("DTypeToArrow(Dictionary): ValueType = %v, want STRING", dictType.ValueType)
				}
			}
		}
	}
}

func TestArrowToDType(t *testing.T) {
	tests := []struct {
		arrowType arrow.DataType
		want      DType
		wantErr   bool
	}{
		{arrow.PrimitiveTypes.Int64, Int64, false},
		{arrow.PrimitiveTypes.Float64, Float64, false},
		{arrow.BinaryTypes.String, String, false},
		{arrow.FixedWidthTypes.Boolean, Bool, false},
		{arrow.FixedWidthTypes.Timestamp_ns, Timestamp, false},
		{&arrow.DictionaryType{
			IndexType: arrow.PrimitiveTypes.Int32,
			ValueType: arrow.BinaryTypes.String,
		}, Dictionary, false},
		{arrow.PrimitiveTypes.Uint8, Invalid, true},
	}
	for _, tt := range tests {
		got, err := ArrowToDType(tt.arrowType)
		if (err != nil) != tt.wantErr {
			t.Errorf("ArrowToDType(%v): error = %v, wantErr %v", tt.arrowType, err, tt.wantErr)
			continue
		}
		if got != tt.want {
			t.Errorf("ArrowToDType(%v) = %v, want %v", tt.arrowType, got, tt.want)
		}
	}
}

func TestRoundTrip(t *testing.T) {
	types := []DType{Int64, Float64, String, Bool, Timestamp, Dictionary}
	for _, dt := range types {
		arrowType, err := DTypeToArrow(dt)
		if err != nil {
			t.Fatalf("DTypeToArrow(%v) unexpected error: %v", dt, err)
		}
		roundtripped, err := ArrowToDType(arrowType)
		if err != nil {
			t.Fatalf("ArrowToDType(%v) unexpected error: %v", arrowType, err)
		}
		if roundtripped != dt {
			t.Errorf("roundtrip failed: %v -> %v -> %v", dt, arrowType, roundtripped)
		}
	}
}
