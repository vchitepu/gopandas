package dtype

// DType represents the data type of a column in gopandas.
type DType int

const (
	Invalid    DType = iota // zero value, represents an unknown or unset type
	Int64                   // 64-bit signed integer
	Float64                 // 64-bit IEEE 754 floating point
	String                  // variable-length UTF-8 string
	Bool                    // boolean true/false
	Timestamp               // nanosecond-precision timestamp (UTC)
	Dictionary              // categorical / dictionary-encoded
)

// String returns the lowercase name of the DType.
func (d DType) String() string {
	switch d {
	case Invalid:
		return "invalid"
	case Int64:
		return "int64"
	case Float64:
		return "float64"
	case String:
		return "string"
	case Bool:
		return "bool"
	case Timestamp:
		return "timestamp"
	case Dictionary:
		return "dictionary"
	default:
		return "unknown"
	}
}
