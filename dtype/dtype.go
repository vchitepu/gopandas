package dtype

// DType represents the data type of a column in gopandas.
type DType int

const (
	// Invalid represents an unknown or unset data type.
	Invalid DType = iota // zero value, represents an unknown or unset type
	// Int64 represents a 64-bit signed integer type.
	Int64 // 64-bit signed integer
	// Float64 represents a 64-bit floating-point type.
	Float64 // 64-bit IEEE 754 floating point
	// String represents a variable-length UTF-8 string type.
	String // variable-length UTF-8 string (NOTE: this name shadows the Stringer method inside the package; use dtype.String externally)
	// Bool represents a boolean type.
	Bool // boolean true/false
	// Timestamp represents a nanosecond-precision UTC timestamp type.
	Timestamp // nanosecond-precision timestamp (UTC)
	// Dictionary represents a categorical dictionary-encoded type.
	Dictionary // categorical / dictionary-encoded
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
