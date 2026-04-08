package index

// StringIndex is an index backed by string labels.
type StringIndex struct {
	labels []string
	lookup map[string]int
	name   string
}

// NewStringIndex creates a StringIndex from the given labels.
// Duplicate labels are allowed; Loc returns the position of the first occurrence.
func NewStringIndex(labels []string, name string) *StringIndex {
	cp := make([]string, len(labels))
	copy(cp, labels)
	lookup := make(map[string]int, len(labels))
	for i, l := range cp {
		if _, exists := lookup[l]; !exists {
			lookup[l] = i
		}
	}
	return &StringIndex{labels: cp, lookup: lookup, name: name}
}

func (s *StringIndex) Len() int { return len(s.labels) }

func (s *StringIndex) Labels() []any {
	out := make([]any, len(s.labels))
	for i, l := range s.labels {
		out[i] = l
	}
	return out
}

func (s *StringIndex) Loc(label any) (int, bool) {
	v, ok := label.(string)
	if !ok {
		return -1, false
	}
	pos, found := s.lookup[v]
	if !found {
		return -1, false
	}
	return pos, true
}

func (s *StringIndex) Slice(start, end int) Index {
	if start < 0 {
		start = 0
	}
	if end > len(s.labels) {
		end = len(s.labels)
	}
	if start >= end {
		return NewStringIndex(nil, s.name)
	}
	return NewStringIndex(s.labels[start:end], s.name)
}

func (s *StringIndex) Name() string { return s.name }
