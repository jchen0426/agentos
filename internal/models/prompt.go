package models

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"
)

// refName is the name part of a blob argument, "<name>_ref": it becomes the section's tag.
var refName = regexp.MustCompile(`^[a-z][a-z0-9_]{0,31}$`)

// userMessage lays out a call's arguments: every argument that names a blob ("<name>_ref")
// becomes a section holding the blob, after the other arguments as one JSON section. Blobs
// come last since they are the large part that changes from call to call. Each section's tag
// carries a suffix taken from its content, so the content cannot close its own section. Only
// blobs the calling run produced are read, each at most once.
func userMessage(ctx context.Context, blobs BlobStore, workflowID string, args map[string]any) (string, error) {
	plain := map[string]any{}
	var sections []string
	seen := map[string]bool{}
	for _, k := range slices.Sorted(maps.Keys(args)) {
		name, isRef := strings.CutSuffix(k, "_ref")
		ref, isString := args[k].(string)
		if !isRef || !isString || !strings.HasPrefix(ref, "sha256:") {
			plain[k] = args[k]
			continue
		}
		if !refName.MatchString(name) || seen[ref] {
			return "", fmt.Errorf("%w: argument %q", ErrBadArguments, k)
		}
		seen[ref] = true
		b, err := blobs.GetRunBlob(ctx, workflowID, ref)
		if err != nil {
			return "", fmt.Errorf("%w: argument %s: %v", ErrBadArguments, k, err)
		}
		tag := sectionTag(name, b)
		sections = append(sections, "<"+tag+">\n"+string(b)+"\n</"+tag+">")
	}
	var parts []string
	if len(plain) > 0 {
		b, err := json.Marshal(plain)
		if err != nil {
			return "", err
		}
		parts = append(parts, "<arguments>\n"+string(b)+"\n</arguments>")
	}
	return strings.Join(append(parts, sections...), "\n\n"), nil
}

func sectionTag(name string, content []byte) string {
	sum := sha256.Sum256(content)
	h := hex.EncodeToString(sum[:])
	for n := 6; ; n += 2 {
		tag := name + "_" + h[:n]
		if n >= len(h) || !bytes.Contains(content, []byte("</"+tag+">")) {
			return tag
		}
	}
}
