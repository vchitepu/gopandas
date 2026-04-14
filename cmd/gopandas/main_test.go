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
	readParseDates = ""
	readDateFormat = ""

	// Convert command flags
	convertFrom = ""
	convertTo = ""
	convertSelect = ""
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

func TestReadParseDatesDTypes(t *testing.T) {
	resetFlags()
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "dates.csv")
	content := "Date,Description,Amount\n12/31/2025,Coffee,3.25\n12/30/2025,Lunch,15.00\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("failed to write temp csv: %v", err)
	}

	out, err := executeCommand("read", "--dtypes", "--parse-dates", "Date", path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, "Date: timestamp") {
		t.Errorf("expected Date dtype to be timestamp, got:\n%s", out)
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
	_, err := executeCommand("read", "--output", outPath, "--format", "json", "testdata/sample.csv")
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

func TestConvertCSVToJSON(t *testing.T) {
	resetFlags()

	outPath := t.TempDir() + "/output.json"
	output, err := executeCommand("convert", "testdata/sample.csv", outPath)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.Contains(output, "Converted") {
		t.Error("expected output to contain 'Converted'")
	}
	if !strings.Contains(output, "5 rows") {
		t.Error("expected output to mention '5 rows'")
	}

	data, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("failed to read output: %v", err)
	}
	if !strings.Contains(string(data), "Alice") {
		t.Error("expected JSON output to contain 'Alice'")
	}
}

func TestConvertJSONToCSV(t *testing.T) {
	resetFlags()

	outPath := t.TempDir() + "/output.csv"
	_, err := executeCommand("convert", "testdata/sample.json", outPath)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	data, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("failed to read output: %v", err)
	}
	content := string(data)

	if !strings.Contains(content, "name") {
		t.Error("expected CSV output to contain 'name' header")
	}
	if !strings.Contains(content, "Alice") {
		t.Error("expected CSV output to contain 'Alice'")
	}
}

func TestConvertWithSelect(t *testing.T) {
	resetFlags()

	outPath := t.TempDir() + "/output.csv"
	_, err := executeCommand("convert", "testdata/sample.csv", outPath, "--select", "name,age")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	data, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("failed to read output: %v", err)
	}
	content := string(data)

	if !strings.Contains(content, "name") {
		t.Error("expected output to contain 'name'")
	}
	if strings.Contains(content, "salary") {
		t.Error("expected output NOT to contain 'salary'")
	}
}

func TestConvertExplicitFormats(t *testing.T) {
	resetFlags()

	outPath := t.TempDir() + "/output.dat"
	_, err := executeCommand("convert", "testdata/sample.csv", outPath, "--from", "csv", "--to", "json")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	data, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("failed to read output: %v", err)
	}
	content := string(data)

	if !strings.Contains(content, "Alice") {
		t.Error("expected JSON output to contain 'Alice'")
	}
}

func TestReadJSON(t *testing.T) {
	resetFlags()

	output, err := executeCommand("read", "testdata/sample.json")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.Contains(output, "Alice") {
		t.Error("expected output to contain 'Alice'")
	}
	if !strings.Contains(output, "Bob") {
		t.Error("expected output to contain 'Bob'")
	}
}

func TestReadXLSXContainsExpectedNames(t *testing.T) {
	resetFlags()

	output, err := executeCommand("read", "testdata/sample.xlsx")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.Contains(output, "Alice") {
		t.Error("expected output to contain 'Alice'")
	}
	if !strings.Contains(output, "Eve") {
		t.Error("expected output to contain 'Eve'")
	}
}

func TestReadXLSXShape(t *testing.T) {
	resetFlags()

	output, err := executeCommand("read", "--shape", "testdata/sample.xlsx")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.Contains(output, "(5, 4)") {
		t.Fatalf("expected shape output to contain (5, 4), got:\n%s", output)
	}
}

func TestConvertCSVToXLSXAndReadShape(t *testing.T) {
	resetFlags()

	outPath := t.TempDir() + "/output.xlsx"
	_, err := executeCommand("convert", "testdata/sample.csv", outPath)
	if err != nil {
		t.Fatalf("unexpected convert error: %v", err)
	}

	resetFlags()
	shapeOutput, err := executeCommand("read", "--shape", outPath)
	if err != nil {
		t.Fatalf("unexpected read error: %v", err)
	}

	if !strings.Contains(shapeOutput, "(5, 4)") {
		t.Fatalf("expected shape output to contain (5, 4), got:\n%s", shapeOutput)
	}

	resetFlags()
	readOutput, err := executeCommand("read", "--head", "1", outPath)
	if err != nil {
		t.Fatalf("unexpected read error: %v", err)
	}

	if !strings.Contains(readOutput, "name") || !strings.Contains(readOutput, "salary") {
		t.Fatalf("expected read output to contain columns name and salary, got:\n%s", readOutput)
	}
	if !strings.Contains(readOutput, "75000.5") {
		t.Fatalf("expected read output to contain salary value 75000.5, got:\n%s", readOutput)
	}
}

func TestConvertXLSXToCSVContainsExpectedData(t *testing.T) {
	resetFlags()

	outPath := t.TempDir() + "/employees.csv"
	_, err := executeCommand("convert", "testdata/employees.xlsx", outPath)
	if err != nil {
		t.Fatalf("unexpected convert error: %v", err)
	}

	data, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("failed to read output: %v", err)
	}
	content := strings.TrimSpace(string(data))
	lines := strings.Split(content, "\n")
	if len(lines) < 2 {
		t.Fatalf("expected CSV output to contain header and rows, got:\n%s", content)
	}

	header := strings.TrimSuffix(lines[0], "\r")
	if header != "id,name,department,salary,hire_date,active" {
		t.Fatalf("unexpected CSV header: %q", header)
	}
	firstRow := strings.TrimSuffix(lines[1], "\r")
	if !strings.Contains(firstRow, "Alice Johnson") {
		t.Fatalf("expected first CSV row to contain 'Alice Johnson', got: %q", firstRow)
	}
}

func TestReadUnsupportedExtension(t *testing.T) {
	resetFlags()

	_, err := executeCommand("read", "testdata/sample.txt")
	if err == nil {
		t.Fatal("expected error for unsupported extension .txt")
	}
	if !strings.Contains(err.Error(), "unsupported") {
		t.Errorf("expected error to mention 'unsupported', got: %v", err)
	}
}

func TestInferFormatXLSX(t *testing.T) {
	format, err := inferFormat("report.xlsx")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if format != "xlsx" {
		t.Fatalf("inferFormat(report.xlsx) = %q, want xlsx", format)
	}
}

func TestConvertFlagUsageMentionsXLSX(t *testing.T) {
	from := convertCmd.Flags().Lookup("from")
	if from == nil {
		t.Fatal("missing --from flag")
	}
	if !strings.Contains(from.Usage, "xlsx") {
		t.Fatalf("expected --from usage to include xlsx, got: %q", from.Usage)
	}

	to := convertCmd.Flags().Lookup("to")
	if to == nil {
		t.Fatal("missing --to flag")
	}
	if !strings.Contains(to.Usage, "xlsx") {
		t.Fatalf("expected --to usage to include xlsx, got: %q", to.Usage)
	}
}

func TestReadFormatFlagUsageMentionsXLSX(t *testing.T) {
	format := readCmd.Flags().Lookup("format")
	if format == nil {
		t.Fatal("missing --format flag")
	}
	if !strings.Contains(format.Usage, "xlsx") {
		t.Fatalf("expected --format usage to include xlsx, got: %q", format.Usage)
	}
}

func TestReadMissingFile(t *testing.T) {
	resetFlags()

	_, err := executeCommand("read", "testdata/nonexistent.csv")
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestReadInvalidFilter(t *testing.T) {
	resetFlags()

	_, err := executeCommand("read", "testdata/sample.csv", "--filter", "invalid gibberish %%")
	if err == nil {
		t.Fatal("expected error for invalid filter expression")
	}
}

func TestReadInvalidAgg(t *testing.T) {
	resetFlags()

	_, err := executeCommand("read", "testdata/sample.csv", "--groupby", "city", "--agg", "median")
	if err == nil {
		t.Fatal("expected error for unsupported aggregation 'median'")
	}
	if !strings.Contains(err.Error(), "unsupported aggregation") {
		t.Errorf("expected error to mention 'unsupported aggregation', got: %v", err)
	}
}

func TestReadFullPipeline(t *testing.T) {
	resetFlags()

	outPath := t.TempDir() + "/pipeline_result.csv"
	_, err := executeCommand("read", "testdata/sample.csv",
		"--select", "name,age,salary",
		"--filter", "age >= 30",
		"--sort", "salary",
		"--sort-desc",
		"--output", outPath)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	data, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("failed to read output: %v", err)
	}
	content := string(data)

	if !strings.Contains(content, "Alice") {
		t.Error("expected output to contain 'Alice' (age 30)")
	}
	if !strings.Contains(content, "Charlie") {
		t.Error("expected output to contain 'Charlie' (age 35)")
	}
	if !strings.Contains(content, "Eve") {
		t.Error("expected output to contain 'Eve' (age 32)")
	}
	if strings.Contains(content, "Bob") {
		t.Error("expected output NOT to contain 'Bob' (age 25)")
	}
	if strings.Contains(content, "Diana") {
		t.Error("expected output NOT to contain 'Diana' (age 28)")
	}
}

func TestConvertMissingFile(t *testing.T) {
	resetFlags()

	outPath := t.TempDir() + "/output.csv"
	_, err := executeCommand("convert", "testdata/nonexistent.csv", outPath)
	if err == nil {
		t.Fatal("expected error for missing input file")
	}
}
