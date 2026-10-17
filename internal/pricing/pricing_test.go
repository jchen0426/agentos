package pricing

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

const table = `
price_table_version: 2026-10-15
models:
  anthropic/claude-opus-5-5: {input: 4.00, cache_write_5m: 5.00, cache_write_1h: 8.00, cache_read: 0.20, output: 20.00}
  fake/cheap:                {input: 1.00, output: 5.00}
  fake/dear:                 {input: 2.00, output: 10.00}
  openai/not-copied-yet:     {input: null, cache_read: null, output: null}
  local/llama:               {input: 0, output: 0}
`

func load(t *testing.T, body string) *Table {
	t.Helper()
	path := filepath.Join(t.TempDir(), "prices.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	tbl, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	return tbl
}

func TestCost(t *testing.T) {
	tbl := load(t, table)
	if tbl.Version != "2026-10-15" {
		t.Errorf("version %q", tbl.Version)
	}
	for _, c := range []struct {
		model string
		u     Usage
		want  int64
	}{
		// $4/M input is 4 micros a token; $0.20/M cache reads is 0.2 micros a token.
		{"anthropic/claude-opus-5-5", Usage{InputTokens: 1000, OutputTokens: 100}, 4000 + 2000},
		{"anthropic/claude-opus-5-5", Usage{CacheWrite5mTokens: 10, CacheWrite1hTokens: 10, CacheReadTokens: 1000}, 50 + 80 + 200},
		{"anthropic/claude-opus-5-5", Usage{CacheReadTokens: 1}, 1}, // 0.2 micros, rounded up
		{"fake/cheap", Usage{InputTokens: 1300, OutputTokens: 500}, 1300 + 2500},
		{"fake/cheap", Usage{CacheReadTokens: 10}, 10}, // no cache price: charged as input
		{"local/llama", Usage{InputTokens: 1e6, OutputTokens: 1e6}, 0},
	} {
		got, err := tbl.Cost(c.model, c.u)
		if err != nil || got != c.want {
			t.Errorf("Cost(%s, %+v) = %d, %v; want %d", c.model, c.u, got, err, c.want)
		}
	}
	for _, m := range []string{"openai/not-copied-yet", "anthropic/unlisted"} {
		if _, err := tbl.Cost(m, Usage{}); !errors.Is(err, ErrNoPrice) {
			t.Errorf("Cost(%s): %v, want ErrNoPrice", m, err)
		}
	}
}

func TestBoundCoversEveryOutcome(t *testing.T) {
	tbl := load(t, table)
	// 1000 counted tokens + 10% = 1100. Each candidate may run to max_tokens within one
	// request, so the bound adds up: 1100 at $1/M + 4096 at $5/M, then 1100 at $2/M + 4096 at $10/M.
	got, err := tbl.Bound([]string{"fake/cheap", "fake/dear"}, 1000, 4096)
	if want := int64(1100*1 + 4096*5 + 1100*2 + 4096*10); err != nil || got != want {
		t.Fatalf("Bound = %d, %v; want %d", got, err, want)
	}
	// The worst case: the first model writes max_tokens and then declines, and the fallback
	// does the same, with the input cached on one side and written on the other.
	first, _ := tbl.Cost("fake/cheap", Usage{InputTokens: 1100, OutputTokens: 4096})
	second, _ := tbl.Cost("fake/dear", Usage{CacheReadTokens: 1100, OutputTokens: 4096})
	if first+second > got {
		t.Errorf("both models billing in full (%d) is over the bound %d", first+second, got)
	}
	// Cache writes are the dearest input on Opus: the bound uses them.
	opus, _ := tbl.Bound([]string{"anthropic/claude-opus-5-5"}, 1000, 1000)
	if want := int64(1100*8 + 1000*20); opus != want {
		t.Errorf("Opus bound %d, want %d", opus, want)
	}
	for name, c := range map[string][]string{"no model": nil, "unpriced fallback": {"fake/cheap", "openai/not-copied-yet"}} {
		if _, err := tbl.Bound(c, 10, 10); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
	if _, err := tbl.Bound([]string{"fake/cheap"}, 10, 0); err == nil {
		t.Error("max_tokens 0 bounded")
	}
}

func TestLoadRejectsBadTables(t *testing.T) {
	for name, body := range map[string]string{
		"no version":     "models: {}\n",
		"unknown key":    "price_table_version: v1\nmodels: {m/x: {input: 1, output: 1, cache: 1}}\n",
		"negative price": "price_table_version: v1\nmodels: {m/x: {input: -1, output: 1}}\n",
		"too precise":    "price_table_version: v1\nmodels: {m/x: {input: 0.0001, output: 1}}\n",
	} {
		path := filepath.Join(t.TempDir(), "prices.yaml")
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := Load(path); err == nil {
			t.Errorf("%s: loaded", name)
		}
	}
}
