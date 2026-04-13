package json

// JSONOrient specifies the JSON structure format for serialization/deserialization.
type JSONOrient int

const (
	// OrientRecords: [{col: val, ...}, ...] (default)
	OrientRecords JSONOrient = iota
	// OrientColumns: {col: [val, ...], ...}
	OrientColumns
	// OrientIndex: {idx: {col: val, ...}, ...}
	OrientIndex
)

// String returns the lowercase name of the JSON orientation.
func (o JSONOrient) String() string {
	switch o {
	case OrientRecords:
		return "records"
	case OrientColumns:
		return "columns"
	case OrientIndex:
		return "index"
	default:
		return "unknown"
	}
}
