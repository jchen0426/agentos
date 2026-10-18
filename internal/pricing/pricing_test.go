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
  openai/gpt-6.1-sol:        {input: 2.00, cache_write: 2.50, cache_read: 0.10, output: 10.00,
                              long_context: {above: 272000, input: 2.0, output: 1.5}}
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
		// One write price, 1.25x input; a tenth of a micro per cached token.
		{"openai/gpt-6.1-sol", Usage{InputTokens: 1000, CacheWriteTokens: 1000, CacheReadTokens: 1000, OutputTokens: 100},
			2000 + 2500 + 100 + 1000},
		// At the tier's edge the base prices hold; one token past it, the whole request pays 2x
		// and 1.5x, cache reads included.
		{"openai/gpt-6.1-sol", Usage{InputTokens: 272000, OutputTokens: 1000}, 544000 + 10000},
		{"openai/gpt-6.1-sol", Usage{InputTokens: 271000, CacheReadTokens: 1001, OutputTokens: 1000},
			1084000 + 201 + 15000},
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
	// The single write price is OpenAI's dearest input. Past the long-context tier, counting
	// the 10% padding, the bound pays the tier's prices; at it, the base prices.
	sol, _ := tbl.Bound([]string{"openai/gpt-6.1-sol"}, 1000, 1000)
	if want := int64(1100*2500/1000 + 1000*10); sol != want {
		t.Errorf("gpt-6.1-sol bound %d, want %d", sol, want)
	}
	at, _ := tbl.Bound([]string{"openai/gpt-6.1-sol"}, 247272, 1000)   // padded: 272000
	past, _ := tbl.Bound([]string{"openai/gpt-6.1-sol"}, 247273, 1000) // padded: 272001
	if want := int64(272000*2500/1000 + 1000*10); at != want {
		t.Errorf("bound at the tier %d, want %d", at, want)
	}
	if want := int64(272001*5000/1000 + 1000*15); past != want {
		t.Errorf("bound past the tier %d, want %d", past, want)
	}
	// Whatever the input turns out to be, the request costs no more than the bound.
	if c, _ := tbl.Cost("openai/gpt-6.1-sol", Usage{CacheWriteTokens: 272001, OutputTokens: 1000}); c > past {
		t.Errorf("cost %d over the bound %d", c, past)
	}
}

func TestLoadRejectsBadTables(t *testing.T) {
	for name, body := range map[string]string{
		"no version":     "models: {}\n",
		"unknown key":    "price_table_version: v1\nmodels: {m/x: {input: 1, output: 1, cache: 1}}\n",
		"negative price": "price_table_version: v1\nmodels: {m/x: {input: -1, output: 1}}\n",
		"too precise":    "price_table_version: v1\nmodels: {m/x: {input: 0.0001, output: 1}}\n",
		"tier below 1x":  "price_table_version: v1\nmodels: {m/x: {input: 1, output: 1, long_context: {above: 10, input: 0.5, output: 1}}}\n",
		"tier at 0":      "price_table_version: v1\nmodels: {m/x: {input: 1, output: 1, long_context: {above: 0, input: 2, output: 1}}}\n",
		"tier unknown":   "price_table_version: v1\nmodels: {m/x: {input: 1, output: 1, long_context: {above: 10, input: 2, cache: 2}}}\n",
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

func TestFree(t *testing.T) {
	tbl := load(t, table+"  local/cache-priced:        {input: 0, cache_read: 0.01, output: 0}\n")
	for model, want := range map[string]bool{"local/llama": true, "fake/cheap": false, "local/cache-priced": false} {
		if free, err := tbl.Free(model); err != nil || free != want {
			t.Errorf("Free(%s) = %v, %v", model, free, err)
		}
	}
	if _, err := tbl.Free("openai/not-copied-yet"); !errors.Is(err, ErrNoPrice) {
		t.Errorf("an unpriced model: %v", err)
	}
}
