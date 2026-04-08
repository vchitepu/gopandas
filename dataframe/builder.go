package dataframe

import "github.com/vchitepu/gopandas/series"

// Builder enables fluent DataFrame operations with deferred error handling.
type Builder struct {
	df  DataFrame
	err error
}

// Build starts a DataFrame builder chain.
func (df DataFrame) Build() *Builder {
	return &Builder{df: df}
}

// Result returns the current DataFrame and accumulated error.
func (b *Builder) Result() (DataFrame, error) {
	return b.df, b.err
}

// Select applies DataFrame.Select and accumulates any error.
func (b *Builder) Select(cols ...string) *Builder {
	if b.err != nil {
		return b
	}

	df, err := b.df.Select(cols...)
	if err != nil {
		b.err = err
		return b
	}

	b.df = df
	return b
}

// Query applies DataFrame.Query and accumulates any error.
func (b *Builder) Query(expr string) *Builder {
	if b.err != nil {
		return b
	}

	df, err := b.df.Query(expr)
	if err != nil {
		b.err = err
		return b
	}

	b.df = df
	return b
}

// Filter applies DataFrame.Filter and accumulates any error.
func (b *Builder) Filter(mask series.Series[bool]) *Builder {
	if b.err != nil {
		return b
	}

	df, err := b.df.Filter(mask)
	if err != nil {
		b.err = err
		return b
	}

	b.df = df
	return b
}

// SortBy applies DataFrame.SortBy and accumulates any error.
func (b *Builder) SortBy(cols []string, ascending []bool) *Builder {
	if b.err != nil {
		return b
	}

	df, err := b.df.SortBy(cols, ascending)
	if err != nil {
		b.err = err
		return b
	}

	b.df = df
	return b
}

// Head applies DataFrame.Head.
func (b *Builder) Head(n int) *Builder {
	if b.err != nil {
		return b
	}

	b.df = b.df.Head(n)
	return b
}

// Tail applies DataFrame.Tail.
func (b *Builder) Tail(n int) *Builder {
	if b.err != nil {
		return b
	}

	b.df = b.df.Tail(n)
	return b
}
