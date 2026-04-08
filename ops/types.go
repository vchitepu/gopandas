package ops

// JoinType specifies the type of join operation.
type JoinType int

const (
	// Inner keeps only matching keys from both sides.
	Inner JoinType = iota
	// Left keeps all keys from the left side.
	Left
	// Right keeps all keys from the right side.
	Right
	// Outer keeps all keys from both sides.
	Outer
)

// String returns the lowercase name of the join type.
func (jt JoinType) String() string {
	switch jt {
	case Inner:
		return "inner"
	case Left:
		return "left"
	case Right:
		return "right"
	case Outer:
		return "outer"
	default:
		return "unknown"
	}
}

// AggFunc specifies the aggregation function for pivot tables.
type AggFunc int

const (
	// AggSum computes the sum of values.
	AggSum AggFunc = iota
	// AggMean computes the arithmetic mean of values.
	AggMean
	// AggCount counts non-missing values.
	AggCount
	// AggMin returns the minimum value.
	AggMin
	// AggMax returns the maximum value.
	AggMax
	// AggStd computes the standard deviation of values.
	AggStd
	// AggFirst returns the first value.
	AggFirst
	// AggLast returns the last value.
	AggLast
)

// String returns the lowercase name of the aggregation function.
func (af AggFunc) String() string {
	switch af {
	case AggSum:
		return "sum"
	case AggMean:
		return "mean"
	case AggCount:
		return "count"
	case AggMin:
		return "min"
	case AggMax:
		return "max"
	case AggStd:
		return "std"
	case AggFirst:
		return "first"
	case AggLast:
		return "last"
	default:
		return "unknown"
	}
}
