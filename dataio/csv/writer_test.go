package csv

import (
	"bytes"
	"strings"
	"testing"
)

func TestToCSV_Simple(t *testing.T) {
	input := "name,age,score\nAlice,30,95.5\nBob,25,88.0\n"
	df, err := FromCSV(strings.NewReader(input))
	if err != nil {
		t.Fatalf("FromCSV: %v", err)
	}

	var buf bytes.Buffer
	if err := ToCSV(df, &buf); err != nil {
		t.Fatalf("ToCSV: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 3 {
		t.Fatalf("got %d lines, want 3; output:\n%s", len(lines), buf.String())
	}

	// Check header
	if lines[0] != "name,age,score" {
		t.Errorf("header = %q, want %q", lines[0], "name,age,score")
	}

	// Check data rows
	if lines[1] != "Alice,30,95.5" {
		t.Errorf("row 1 = %q, want %q", lines[1], "Alice,30,95.5")
	}
	if lines[2] != "Bob,25,88" {
		t.Errorf("row 2 = %q, want %q", lines[2], "Bob,25,88")
	}
}

func TestToCSV_WithSep(t *testing.T) {
	input := "name,age\nAlice,30\nBob,25\n"
	df, err := FromCSV(strings.NewReader(input))
	if err != nil {
		t.Fatalf("FromCSV: %v", err)
	}

	var buf bytes.Buffer
	if err := ToCSV(df, &buf, WithSep('\t')); err != nil {
		t.Fatalf("ToCSV: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 3 {
		t.Fatalf("got %d lines, want 3; output:\n%s", len(lines), buf.String())
	}

	if lines[0] != "name\tage" {
		t.Errorf("header = %q, want %q", lines[0], "name\tage")
	}
	if lines[1] != "Alice\t30" {
		t.Errorf("row 1 = %q, want %q", lines[1], "Alice\t30")
	}
	if lines[2] != "Bob\t25" {
		t.Errorf("row 2 = %q, want %q", lines[2], "Bob\t25")
	}
}

func TestToCSV_RoundTrip(t *testing.T) {
	input := "name,age,score\nAlice,30,95.5\nBob,25,88.0\nCharlie,35,92.3\n"
	df1, err := FromCSV(strings.NewReader(input))
	if err != nil {
		t.Fatalf("FromCSV (first read): %v", err)
	}

	// Write to buffer
	var buf bytes.Buffer
	if err := ToCSV(df1, &buf); err != nil {
		t.Fatalf("ToCSV: %v", err)
	}

	// Read back
	df2, err := FromCSV(strings.NewReader(buf.String()))
	if err != nil {
		t.Fatalf("FromCSV (second read): %v", err)
	}

	// Verify shapes match
	r1, c1 := df1.Shape()
	r2, c2 := df2.Shape()
	if r1 != r2 || c1 != c2 {
		t.Fatalf("shape mismatch: original (%d, %d) vs round-trip (%d, %d)", r1, c1, r2, c2)
	}

	// Verify columns match
	cols1 := df1.Columns()
	cols2 := df2.Columns()
	for i := range cols1 {
		if cols1[i] != cols2[i] {
			t.Errorf("column %d: %q vs %q", i, cols1[i], cols2[i])
		}
	}
}
