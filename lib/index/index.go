package index

// Index is the row label abstraction for gopandas.
type Index interface {
	Len() int
	Labels() []any
	Loc(label any) (int, bool)
	Slice(start, end int) Index
	Name() string
}
