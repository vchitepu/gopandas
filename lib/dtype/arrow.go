package dtype

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
)

// DTypeToArrow converts a gopandas DType to the corresponding Arrow DataType.
// Returns an error for Invalid or unrecognized values.
func DTypeToArrow(d DType) (arrow.DataType, error) {
	switch d {
	case Int64:
		return arrow.PrimitiveTypes.Int64, nil
	case Float64:
		return arrow.PrimitiveTypes.Float64, nil
	case String:
		return arrow.BinaryTypes.String, nil
	case Bool:
		return arrow.FixedWidthTypes.Boolean, nil
	case Timestamp:
		return arrow.FixedWidthTypes.Timestamp_ns, nil
	case Dictionary:
		return &arrow.DictionaryType{
			IndexType: arrow.PrimitiveTypes.Int32,
			ValueType: arrow.BinaryTypes.String,
		}, nil
	default:
		return nil, fmt.Errorf("dtype: no Arrow mapping for %v", d)
	}
}

// ArrowToDType converts an Arrow DataType to the corresponding gopandas DType.
// Returns an error for unsupported Arrow types.
func ArrowToDType(dt arrow.DataType) (DType, error) {
	switch dt.ID() {
	case arrow.INT64:
		return Int64, nil
	case arrow.FLOAT64:
		return Float64, nil
	case arrow.STRING, arrow.LARGE_STRING:
		return String, nil
	case arrow.BOOL:
		return Bool, nil
	case arrow.TIMESTAMP:
		return Timestamp, nil
	case arrow.DICTIONARY:
		return Dictionary, nil
	default:
		return Invalid, fmt.Errorf("dtype: unsupported Arrow type %v", dt)
	}
}
