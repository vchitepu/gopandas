package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// executeCommand runs the root cobra command with the given args and captures output.
func executeCommand(args ...string) (string, error) {
	buf := new(bytes.Buffer)
	rootCmd.SetOut(buf)
	rootCmd.SetErr(buf)
	rootCmd.SetArgs(args)
	err := rootCmd.Execute()
	return buf.String(), err
}

// resetFlags resets all package-level flag variables to their zero values
// so that flags from one test don't leak into the next.
func resetFlags() {
	readHead = 0
	readTail = 0
	readDescribe = false
	readShape = false
	readDTypes = false
	readSelect = ""
	readFilter = ""
	readGroupBy = ""
	readAgg = ""
	readSort = ""
	readSortDesc = false
	readOutput = ""
	readFormat = ""
}

func TestReadDefaultHead(t *testing.T) {
	resetFlags()
	out, err := executeCommand("read", "testdata/sample.csv")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, "Alice") {
		t.Errorf("expected output to contain Alice, got:\n%s", out)
	}
	if !strings.Contains(out, "Eve") {
		t.Errorf("expected output to contain Eve, got:\n%s", out)
	}
}

func TestReadHead2(t *testing.T) {
	resetFlags()
	out, err := executeCommand("read", "--head", "2", "testdata/sample.csv")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, "Alice") {
		t.Errorf("expected output to contain Alice, got:\n%s", out)
	}
	if !strings.Contains(out, "Bob") {
		t.Errorf("expected output to contain Bob, got:\n%s", out)
	}
	if strings.Contains(out, "Charlie") {
		t.Errorf("expected output NOT to contain Charlie, got:\n%s", out)
	}
}

func TestReadTail2(t *testing.T) {
	resetFlags()
	out, err := executeCommand("read", "--tail", "2", "testdata/sample.csv")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, "Diana") {
		t.Errorf("expected output to contain Diana, got:\n%s", out)
	}
	if !strings.Contains(out, "Eve") {
		t.Errorf("expected output to contain Eve, got:\n%s", out)
	}
	if strings.Contains(out, "Alice") {
		t.Errorf("expected output NOT to contain Alice, got:\n%s", out)
	}
}

func TestReadShape(t *testing.T) {
	resetFlags()
	out, err := executeCommand("read", "--shape", "testdata/sample.csv")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, "(5, 4)") {
		t.Errorf("expected output to contain (5, 4), got:\n%s", out)
	}
}

func TestReadDTypes(t *testing.T) {
	resetFlags()
	out, err := executeCommand("read", "--dtypes", "testdata/sample.csv")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, "name") {
		t.Errorf("expected output to contain 'name', got:\n%s", out)
	}
	if !strings.Contains(out, "age") {
		t.Errorf("expected output to contain 'age', got:\n%s", out)
	}
	if !strings.Contains(out, "salary") {
		t.Errorf("expected output to contain 'salary', got:\n%s", out)
	}
}

func TestReadDescribe(t *testing.T) {
	resetFlags()
	out, err := executeCommand("read", "--describe", "testdata/sample.csv")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, "count") && !strings.Contains(out, "mean") {
		t.Errorf("expected output to contain 'count' or 'mean', got:\n%s", out)
	}
}

func TestReadSelect(t *testing.T) {
	resetFlags()
	out, err := executeCommand("read", "--select", "name,city", "testdata/sample.csv")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, "Alice") {
		t.Errorf("expected output to contain Alice, got:\n%s", out)
	}
	// salary values should not appear
	if strings.Contains(out, "75000") {
		t.Errorf("expected output NOT to contain salary values, got:\n%s", out)
	}
}

func TestReadFilter(t *testing.T) {
	resetFlags()
	out, err := executeCommand("read", "--filter", "age > 30", "testdata/sample.csv")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, "Charlie") {
		t.Errorf("expected output to contain Charlie, got:\n%s", out)
	}
	if !strings.Contains(out, "Eve") {
		t.Errorf("expected output to contain Eve, got:\n%s", out)
	}
	if strings.Contains(out, "Bob") {
		t.Errorf("expected output NOT to contain Bob, got:\n%s", out)
	}
}

func TestReadSort(t *testing.T) {
	resetFlags()
	out, err := executeCommand("read", "--sort", "age", "testdata/sample.csv")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	bobIdx := strings.Index(out, "Bob")
	charlieIdx := strings.Index(out, "Charlie")
	if bobIdx < 0 || charlieIdx < 0 {
		t.Fatalf("expected both Bob and Charlie in output, got:\n%s", out)
	}
	if bobIdx >= charlieIdx {
		t.Errorf("expected Bob (age 25) before Charlie (age 35), got:\n%s", out)
	}
}

func TestReadSortDesc(t *testing.T) {
	resetFlags()
	out, err := executeCommand("read", "--sort", "age", "--sort-desc", "testdata/sample.csv")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	bobIdx := strings.Index(out, "Bob")
	charlieIdx := strings.Index(out, "Charlie")
	if bobIdx < 0 || charlieIdx < 0 {
		t.Fatalf("expected both Bob and Charlie in output, got:\n%s", out)
	}
	if charlieIdx >= bobIdx {
		t.Errorf("expected Charlie (age 35) before Bob (age 25) in desc sort, got:\n%s", out)
	}
}

func TestReadSelectFilter(t *testing.T) {
	resetFlags()
	out, err := executeCommand("read", "--select", "name,salary", "--filter", "salary > 80000", "testdata/sample.csv")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, "Bob") {
		t.Errorf("expected output to contain Bob, got:\n%s", out)
	}
	if !strings.Contains(out, "Diana") {
		t.Errorf("expected output to contain Diana, got:\n%s", out)
	}
	// Alice has salary 75000.50, should be filtered out
	if strings.Contains(out, "Alice") {
		t.Errorf("expected output NOT to contain Alice, got:\n%s", out)
	}
}

func TestReadGroupByCount(t *testing.T) {
	resetFlags()
	out, err := executeCommand("read", "--groupby", "city", "--agg", "count", "testdata/sample.csv")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, "New York") {
		t.Errorf("expected output to contain 'New York', got:\n%s", out)
	}
	if !strings.Contains(out, "Chicago") {
		t.Errorf("expected output to contain 'Chicago', got:\n%s", out)
	}
}

func TestReadGroupBySum(t *testing.T) {
	resetFlags()
	out, err := executeCommand("read", "--groupby", "city", "--agg", "sum", "testdata/sample.csv")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, "New York") {
		t.Errorf("expected output to contain 'New York', got:\n%s", out)
	}
	if !strings.Contains(out, "San Francisco") {
		t.Errorf("expected output to contain 'San Francisco', got:\n%s", out)
	}
}

func TestReadGroupByMean(t *testing.T) {
	resetFlags()
	out, err := executeCommand("read", "--groupby", "city", "--agg", "mean", "testdata/sample.csv")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, "New York") {
		t.Errorf("expected output to contain 'New York', got:\n%s", out)
	}
}

func TestReadGroupByDefaultAgg(t *testing.T) {
	resetFlags()
	out, err := executeCommand("read", "--groupby", "city", "testdata/sample.csv")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Default is count
	if !strings.Contains(out, "New York") {
		t.Errorf("expected output to contain 'New York', got:\n%s", out)
	}
	if !strings.Contains(out, "Chicago") {
		t.Errorf("expected output to contain 'Chicago', got:\n%s", out)
	}
}

func TestReadOutputCSV(t *testing.T) {
	resetFlags()
	tmpDir := t.TempDir()
	outPath := filepath.Join(tmpDir, "out.csv")
	_, err := executeCommand("read", "--select", "name,age", "--output", outPath, "testdata/sample.csv")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	data, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("failed to read output file: %v", err)
	}
	content := string(data)
	if !strings.Contains(content, "name") {
		t.Errorf("expected CSV output to contain 'name', got:\n%s", content)
	}
	if !strings.Contains(content, "Alice") {
		t.Errorf("expected CSV output to contain 'Alice', got:\n%s", content)
	}
}

func TestReadOutputJSON(t *testing.T) {
	resetFlags()
	tmpDir := t.TempDir()
	outPath := filepath.Join(tmpDir, "out.json")
	_, err := executeCommand("read", "--output", outPath, "--format", "json", "testdata/sample.json")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	data, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("failed to read output file: %v", err)
	}
	content := string(data)
	if !strings.Contains(content, "Alice") {
		t.Errorf("expected JSON output to contain 'Alice', got:\n%s", content)
	}
}
