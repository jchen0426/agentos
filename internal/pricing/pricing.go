// Package pricing turns token counts into money from prices.yaml, a versioned price table
// (design doc, section 9). The gateway reserves the most a request can cost before it
// sends it and settles what the provider billed after. Money is in micros (millionths of
// a US dollar), the ledger's unit.
package pricing

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"os"

	"go.yaml.in/yaml/v3"
)

// Usage is what one request was billed for, by kind of token.
type Usage struct {
	InputTokens        int64 `json:"input_tokens"` // input that was neither written to nor read from the cache
	CacheWrite5mTokens int64 `json:"cache_write_5m_tokens"`
	CacheWrite1hTokens int64 `json:"cache_write_1h_tokens"`
	CacheReadTokens    int64 `json:"cache_read_tokens"`
	OutputTokens       int64 `json:"output_tokens"`
}

// ErrNoPrice means the table has no complete price for a model: it cannot be called.
var ErrNoPrice = errors.New("no price for model")

// Table is a loaded price table.
type Table struct {
	Version string
	rates   map[string]rates // by "<provider>/<model>"
}

// rates are nano-dollars per token. A price in dollars per million tokens is the same
// number of micro-dollars per token; a thousandth of a micro-dollar keeps prices such as
// $0.20 exact.
type rates struct {
	input, cacheWrite5m, cacheWrite1h, cacheRead, output int64
	complete                                             bool // input and output are both set
}

type file struct {
	Version string            `yaml:"price_table_version"`
	Models  map[string]prices `yaml:"models"`
}

// prices are dollars per million tokens; null means not copied from the provider yet.
type prices struct {
	Input        *float64 `yaml:"input"`
	CacheWrite5m *float64 `yaml:"cache_write_5m"`
	CacheWrite1h *float64 `yaml:"cache_write_1h"`
	CacheRead    *float64 `yaml:"cache_read"`
	Output       *float64 `yaml:"output"`
}

// Load reads a price table. Unknown keys are an error, as in the other configuration files.
func Load(path string) (*Table, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	var f file
	if err := dec.Decode(&f); err != nil {
		return nil, fmt.Errorf("prices %s: %w", path, err)
	}
	if f.Version == "" {
		return nil, fmt.Errorf("prices %s: price_table_version is required", path)
	}
	t := &Table{Version: f.Version, rates: map[string]rates{}}
	for model, p := range f.Models {
		r, err := p.rates()
		if err != nil {
			return nil, fmt.Errorf("prices %s: %s: %w", path, model, err)
		}
		t.rates[model] = r
	}
	return t, nil
}

func (p prices) rates() (rates, error) {
	var r rates
	var err error
	nano := func(v *float64, fallback int64) int64 {
		if v == nil || err != nil {
			return fallback
		}
		n := *v * 1000
		if *v < 0 || *v > 1000 || math.Abs(n-math.Round(n)) > 1e-6 {
			err = fmt.Errorf("price %v is not dollars per million tokens with at most 3 decimals", *v)
		}
		return int64(math.Round(n))
	}
	r.input = nano(p.Input, 0)
	r.output = nano(p.Output, 0)
	r.complete = p.Input != nil && p.Output != nil
	// A cache price the provider does not publish is charged as plain input: OpenAI, for
	// one, writes to its cache for free and reports no cache writes at all.
	r.cacheWrite5m = nano(p.CacheWrite5m, r.input)
	r.cacheWrite1h = nano(p.CacheWrite1h, r.input)
	r.cacheRead = nano(p.CacheRead, r.input)
	return r, err
}

func (t *Table) lookup(model string) (rates, error) {
	r, ok := t.rates[model]
	if !ok || !r.complete {
		return rates{}, fmt.Errorf("%w %q in price table %s", ErrNoPrice, model, t.Version)
	}
	return r, nil
}

// Bound is the most a request can cost. candidates are the model and its fallbacks, and with
// a fallback more than one of them can run, and bill, within the same request: a model may
// produce output and then decline, and the fallback runs with its own max_tokens. So the
// bound is the sum, over the candidates, of each one's own worst case: the input tokens,
// counted the provider's way, plus 10%, at its highest input price (cache writes included),
// and maxTokens at its output price.
func (t *Table) Bound(candidates []string, inputTokens, maxTokens int64) (int64, error) {
	if len(candidates) == 0 {
		return 0, errors.New("no model to price")
	}
	if inputTokens < 0 || maxTokens <= 0 {
		return 0, fmt.Errorf("cannot bound %d input tokens and max_tokens %d", inputTokens, maxTokens)
	}
	padded := (inputTokens*11 + 9) / 10
	var nano int64
	for _, m := range candidates {
		r, err := t.lookup(m)
		if err != nil {
			return 0, err
		}
		nano += padded*max(r.input, r.cacheWrite5m, r.cacheWrite1h, r.cacheRead) + maxTokens*r.output
	}
	return ceilMicros(nano), nil
}

// Cost is what model's provider bills for u.
func (t *Table) Cost(model string, u Usage) (int64, error) {
	r, err := t.lookup(model)
	if err != nil {
		return 0, err
	}
	if u.InputTokens < 0 || u.CacheWrite5mTokens < 0 || u.CacheWrite1hTokens < 0 || u.CacheReadTokens < 0 || u.OutputTokens < 0 {
		return 0, fmt.Errorf("negative usage %+v", u)
	}
	return ceilMicros(u.InputTokens*r.input + u.CacheWrite5mTokens*r.cacheWrite5m + u.CacheWrite1hTokens*r.cacheWrite1h +
		u.CacheReadTokens*r.cacheRead + u.OutputTokens*r.output), nil
}

// ceilMicros rounds nano-dollars up to whole micros: rounding never favours the run.
func ceilMicros(nano int64) int64 { return (nano + 999) / 1000 }
