package tools

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/jchen0426/agentos/internal/gitmcp"
	"github.com/jchen0426/agentos/internal/patch"
)

// gitSays is a git tool server that answers apply_check as given.
type gitSays gitmcp.ApplyCheckOut

func (g gitSays) Call(_ context.Context, _, tool string, _, out any) error {
	if tool != "apply_check" {
		return errors.New("unexpected tool " + tool)
	}
	b, _ := json.Marshal(gitmcp.ApplyCheckOut(g))
	return json.Unmarshal(b, out)
}

// git's check passes only when git apply makes of the patch the files agentd's application of it
// does: a patch that reads two ways, git's way and go-gitdiff's, is refused.
func TestGitApplyCheckComparesWhatGitMakes(t *testing.T) {
	fixed := []byte("fixed\n")
	changes := []patch.Change{{Path: "app/db.py", Content: fixed, OldBlob: "1111"}, {Path: "old.py", Delete: true, OldBlob: "2222"}}
	entry := func(path, mode, content string, stage int) gitmcp.IndexEntry {
		return gitmcp.IndexEntry{Path: path, Mode: mode, Object: patch.BlobID([]byte(content)), Stage: stage}
	}
	index := func(entries ...gitmcp.IndexEntry) gitSays {
		entries = append([]gitmcp.IndexEntry{}, entries...) // an empty index, as the server sends it: [], not null
		return gitSays{Applies: true, Index: &entries}
	}
	if err := GitApplyCheck(context.Background(), index(entry("app/db.py", "100644", "fixed\n", 0)), "m", "c", nil, changes); err != nil {
		t.Fatalf("git made the same: %v", err)
	}
	for name, tc := range map[string]struct {
		git  gitSays
		want string
	}{
		"another content":            {index(entry("app/db.py", "100644", "fixed elsewhere\n", 0)), "app/db.py: git apply makes another file"},
		"an executable":              {index(entry("app/db.py", "100755", "fixed\n", 0)), "app/db.py: git apply makes another file"},
		"a file agentd deletes":      {index(entry("app/db.py", "100644", "fixed\n", 0), entry("old.py", "100644", "x\n", 0)), "old.py: git apply makes another file"},
		"no file agentd makes":       {index(), "app/db.py: git apply makes another file"},
		"a file agentd knows of not": {index(entry("app/db.py", "100644", "fixed\n", 0), entry("db.py", "100644", "fixed\n", 0)), `"db.py": git apply makes a file of the patch that agentd does not`},
		"a conflict":                 {index(entry("app/db.py", "100644", "fixed\n", 2)), "app/db.py: git apply makes another file"},
		"one file twice":             {index(entry("app/db.py", "100644", "fixed\n", 0), entry("app/db.py", "100644", "fixed\n", 0)), "git apply makes \"app/db.py\" twice"},
		"git's own refusal":          {gitSays{Reason: "error: patch failed: app/db.py:1"}, "patch failed"},
	} {
		var denied *DeniedError
		if err := GitApplyCheck(context.Background(), tc.git, "m", "c", nil, changes); !errors.As(err, &denied) || !strings.Contains(denied.Reason, tc.want) {
			t.Errorf("%s: %v, want a denial about %q", name, err, tc.want)
		}
	}
	// A server that says the patch applies but not what git made of it is not a denial: it is an
	// older server, or a broken one, and the call fails.
	var denied *DeniedError
	if err := GitApplyCheck(context.Background(), gitSays{Applies: true}, "m", "c", nil, changes); err == nil || errors.As(err, &denied) {
		t.Fatalf("no index: %v, want a failure", err)
	}
}
