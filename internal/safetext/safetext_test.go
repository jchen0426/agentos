package safetext

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"unicode"
)

func TestLine(t *testing.T) {
	for in, want := range map[string]string{
		"report not posted: denied": "report not posted: denied",
		"报告未发布 ✅":                   "报告未发布 ✅",
		"a\x1b[2Jb":                 `a\x1b[2Jb`,
		"x\nstate\tSUCCEEDED\r":     `x\nstate\tSUCCEEDED\r`,
		"\u009b31m":                 `\u009b31m`,
		"abc\u202edef":              `abc\u202edef`,
		"tag\U000E0041":             `tag\U000e0041`,
		"bad\xffbyte":               "bad\uFFFDbyte",
		"del\x7f":                   `del\x7f`,
	} {
		if got := Line(in); got != want {
			t.Errorf("Line(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestJSONKeepsTheValue(t *testing.T) {
	value := map[string]any{
		"esc": "x\x1b[0m", "csi": "\u009b2J", "bidi": "a\u202eb", "tag": "\U000E0041", "del": "\x7f",
		"text": "中文 ✅ <ok>", "n": 1.5, "list": []any{"\u2028", true, nil},
	}
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	out := JSON(raw)
	for _, r := range string(out) {
		if !unicode.IsPrint(r) {
			t.Fatalf("raw %U left in %s", r, out)
		}
	}
	if !bytes.Contains(out, []byte(`\udb40\udc41`)) {
		t.Errorf("a rune above U+FFFF is not a surrogate pair: %s", out)
	}
	var got any
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if !reflect.DeepEqual(got, value) {
		t.Errorf("value changed:\n got %v\nwant %v", got, value)
	}
}

func TestJSONWriter(t *testing.T) {
	var b bytes.Buffer
	record := "{\"msg\":\"invoke started\",\"name\":\"\u009b2J\"}\n"
	n, err := JSONWriter(&b).Write([]byte(record))
	if err != nil || n != len(record) {
		t.Fatalf("Write = %d, %v", n, err)
	}
	if want := "{\"msg\":\"invoke started\",\"name\":\"\\u009b2J\"}\n"; b.String() != want {
		t.Errorf("got %q, want %q", b.String(), want)
	}
	if strings.ContainsRune(b.String(), '\u009b') {
		t.Error("raw C1 control written")
	}
}

func TestText(t *testing.T) {
	for in, want := range map[string]string{
		"--- a/x\n+++ b/x\n@@ -1 +1 @@\n-a\n+\tb\n": "--- a/x\n+++ b/x\n@@ -1 +1 @@\n-a\n+\tb\n",
		"line\r\x1b[2Kforged":                       `line\r\x1b[2Kforged`,
		"a\u202eb\u0085c":                           `a\u202eb\u0085c`,
	} {
		if got := Text(in); got != want {
			t.Errorf("Text(%q) = %q, want %q", in, got, want)
		}
	}
}
