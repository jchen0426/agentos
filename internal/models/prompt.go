package models

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"
)

// refName is the name part of a blob argument, "<name>_ref": it becomes the section's tag.
var refName = regexp.MustCompile(`^[a-z][a-z0-9_]{0,31}$`)

// userMessage lays out a call's arguments, its task's inputs: each names a blob of the calling
// run's own ("<name>_ref") and becomes a section holding the blob, read at most once. Each
// section's tag carries a suffix taken from its content, so the content cannot close its own
// section. Nothing else of the call goes into the prompt: an argument that is not such a blob
// fails it (the gateway refuses the call before it gets here, models.yaml's inputs).
func userMessage(ctx context.Context, blobs BlobStore, workflowID string, args map[string]any) (string, error) {
	var sections []string
	seen := map[string]bool{}
	for _, k := range slices.Sorted(maps.Keys(args)) {
		name, isRef := strings.CutSuffix(k, "_ref")
		ref, isString := args[k].(string)
		if !isRef || !isString || !strings.HasPrefix(ref, "sha256:") || !refName.MatchString(name) || seen[ref] {
			return "", fmt.Errorf("%w: an argument that is not a blob of the run's own", ErrBadArguments)
		}
		seen[ref] = true
		b, err := blobs.GetRunBlob(ctx, workflowID, ref)
		if err != nil {
			return "", fmt.Errorf("%w: argument %s: %v", ErrBadArguments, k, err)
		}
		tag := sectionTag(name, b)
		sections = append(sections, "<"+tag+">\n"+string(b)+"\n</"+tag+">")
	}
	return strings.Join(sections, "\n\n"), nil
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
