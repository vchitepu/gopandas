package dataframe

import "testing"

func builderTestDF(t *testing.T) DataFrame {
	t.Helper()

	df, err := New(map[string]any{
		"a": []int64{1, 2, 3},
		"b": []string{"x", "y", "z"},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	return df
}

func TestBuilder_NoOp(t *testing.T) {
	df := builderTestDF(t)

	got, err := df.Build().Result()
	if err != nil {
		t.Fatalf("Build().Result() error: %v", err)
	}

	if got.String() != df.String() {
		t.Fatalf("Build().Result() changed dataframe:\ngot:\n%s\nwant:\n%s", got.String(), df.String())
	}
}
