package index

// Int64Index is an index backed by int64 labels.
type Int64Index struct {
	labels []int64
	lookup map[int64]int
	name   string
}

// NewInt64Index creates an Int64Index from the given labels.
// Duplicate labels are allowed; Loc returns the position of the first occurrence.
func NewInt64Index(labels []int64, name string) *Int64Index {
	cp := make([]int64, len(labels))
	copy(cp, labels)
	lookup := make(map[int64]int, len(labels))
	for i, l := range cp {
		if _, exists := lookup[l]; !exists {
			lookup[l] = i
		}
	}
	return &Int64Index{labels: cp, lookup: lookup, name: name}
}

func (idx *Int64Index) Len() int { return len(idx.labels) }

func (idx *Int64Index) Labels() []any {
	out := make([]any, len(idx.labels))
	for i, l := range idx.labels {
		out[i] = l
	}
	return out
}

// Loc returns the position of the given label. label must be of type int64;
// passing an untyped int returns (-1, false).
func (idx *Int64Index) Loc(label any) (int, bool) {
	v, ok := label.(int64)
	if !ok {
		return -1, false
	}
	pos, found := idx.lookup[v]
	if !found {
		return -1, false
	}
	return pos, true
}

func (idx *Int64Index) Slice(start, end int) Index {
	if start < 0 {
		start = 0
	}
	if end > len(idx.labels) {
		end = len(idx.labels)
	}
	if start >= end {
		return NewInt64Index(nil, idx.name)
	}
	return NewInt64Index(idx.labels[start:end], idx.name)
}

func (idx *Int64Index) Name() string { return idx.name }
