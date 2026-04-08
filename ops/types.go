package ops

// JoinType specifies the type of join operation.
type JoinType int

const (
	Inner JoinType = iota
	Left
	Right
	Outer
)

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
	AggSum AggFunc = iota
	AggMean
	AggCount
	AggMin
	AggMax
	AggStd
	AggFirst
	AggLast
)

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
