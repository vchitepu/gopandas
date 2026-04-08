package dataframe

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
