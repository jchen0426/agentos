// Package faults is agentd's test-only fault injection, enabled with --fault-script. It
// lets black-box tests make a gateway call hold, hang, skip heartbeats or return a
// scripted status, to reproduce the failure scenarios of the design doc's section 11.
package faults

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

// Rule matches gateway calls by name, and optionally by attempt or occurrence.
type Rule struct {
	Name       string `json:"name"`       // call name, e.g. "audit_v1" or "github.comment_commit"
	Attempt    int32  `json:"attempt"`    // 0 matches any activity attempt
	Occurrence int    `json:"occurrence"` // 0 matches any; n matches the n-th Invoke of Name in a run

	HoldWhileExists string   `json:"hold_while_exists"` // the execution blocks while this file exists
	Delay           Duration `json:"delay"`             // the execution sleeps this long first
	// Later points of the same execution: a write claimed but not sent yet; after the provider
	// or tool answered but before the ledger records it; and right after it is recorded.
	HoldAfterClaimWhileExists   string `json:"hold_after_claim_while_exists"`
	HoldBeforeRecordWhileExists string `json:"hold_before_record_while_exists"`
	HoldAfterRecordWhileExists  string `json:"hold_after_record_while_exists"`
	NoHeartbeat                 bool   `json:"no_heartbeat"` // this attempt sends no heartbeats
	Status                      string `json:"status"`       // return this status instead of executing

	// What the fake model provider does with this attempt's request (models.FakeFault).
	ProviderStatus      int    `json:"provider_status"`      // answer 429 or 5xx, unbilled
	ProviderLoseAnswer  bool   `json:"provider_lose_answer"` // bill it, then lose the answer
	ProviderUnreachable bool   `json:"provider_unreachable"` // fail before it leaves agentd
	ProviderServedBy    string `json:"provider_served_by"`   // answer as this fallback model
	ProviderBadOutput   bool   `json:"provider_bad_output"`  // answer with output that breaks the schema
}

// Effect is what the matching rules ask of one Invoke attempt.
type Effect struct {
	HoldWhileExists             string
	Delay                       time.Duration
	HoldAfterClaimWhileExists   string
	HoldBeforeRecordWhileExists string
	HoldAfterRecordWhileExists  string
	NoHeartbeat                 bool
	Status                      string
	ProviderStatus              int
	ProviderLoseAnswer          bool
	ProviderUnreachable         bool
	ProviderServedBy            string
	ProviderBadOutput           bool
}

// Script is a loaded fault script. A nil *Script injects nothing.
type Script struct {
	Rules []Rule `json:"rules"`

	mu     sync.Mutex
	counts map[string]int // workflow ID + name -> Invokes seen
}

func Load(path string) (*Script, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var s Script
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, fmt.Errorf("fault script %s: %w", path, err)
	}
	s.counts = map[string]int{}
	return &s, nil
}

// Match is called once per Invoke attempt and returns the merged effect of all matching rules.
func (s *Script) Match(workflowID, name string, attempt int32) Effect {
	if s == nil {
		return Effect{}
	}
	s.mu.Lock()
	key := workflowID + "\x00" + name
	s.counts[key]++
	occurrence := s.counts[key]
	s.mu.Unlock()

	var e Effect
	for _, r := range s.Rules {
		if r.Name != name ||
			(r.Attempt != 0 && r.Attempt != attempt) ||
			(r.Occurrence != 0 && r.Occurrence != occurrence) {
			continue
		}
		if r.HoldWhileExists != "" {
			e.HoldWhileExists = r.HoldWhileExists
		}
		if r.HoldAfterClaimWhileExists != "" {
			e.HoldAfterClaimWhileExists = r.HoldAfterClaimWhileExists
		}
		if r.HoldBeforeRecordWhileExists != "" {
			e.HoldBeforeRecordWhileExists = r.HoldBeforeRecordWhileExists
		}
		if r.HoldAfterRecordWhileExists != "" {
			e.HoldAfterRecordWhileExists = r.HoldAfterRecordWhileExists
		}
		e.Delay += time.Duration(r.Delay)
		e.NoHeartbeat = e.NoHeartbeat || r.NoHeartbeat
		if r.Status != "" {
			e.Status = r.Status
		}
		if r.ProviderStatus != 0 {
			e.ProviderStatus = r.ProviderStatus
		}
		e.ProviderLoseAnswer = e.ProviderLoseAnswer || r.ProviderLoseAnswer
		e.ProviderUnreachable = e.ProviderUnreachable || r.ProviderUnreachable
		if r.ProviderServedBy != "" {
			e.ProviderServedBy = r.ProviderServedBy
		}
		e.ProviderBadOutput = e.ProviderBadOutput || r.ProviderBadOutput
	}
	return e
}

// Stall applies the hold and delay of e to an execution before it starts: for a write, once
// its effect is recorded and before it is claimed. It stops early when done closes.
func (e Effect) Stall(done <-chan struct{}) {
	if !holdWhileExists(e.HoldWhileExists, done) {
		return
	}
	if e.Delay > 0 {
		select {
		case <-done:
		case <-time.After(e.Delay):
		}
	}
}

// StallAfterClaim holds a write after it was claimed, before its request goes out.
func (e Effect) StallAfterClaim(done <-chan struct{}) {
	holdWhileExists(e.HoldAfterClaimWhileExists, done)
}

// StallBeforeRecord holds an execution after its answer arrived, before the ledger records it.
func (e Effect) StallBeforeRecord(done <-chan struct{}) {
	holdWhileExists(e.HoldBeforeRecordWhileExists, done)
}

// StallAfterRecord holds an execution after the ledger recorded its answer.
func (e Effect) StallAfterRecord(done <-chan struct{}) {
	holdWhileExists(e.HoldAfterRecordWhileExists, done)
}

// holdWhileExists blocks while path exists; it reports false if done closed first.
func holdWhileExists(path string, done <-chan struct{}) bool {
	if path == "" {
		return true
	}
	t := time.NewTicker(50 * time.Millisecond)
	defer t.Stop()
	for {
		if _, err := os.Stat(path); err != nil {
			return true
		}
		select {
		case <-done:
			return false
		case <-t.C:
		}
	}
}

// Duration is a time.Duration written as a Go duration string ("40s") in JSON.
type Duration time.Duration

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = Duration(v)
	return nil
}
