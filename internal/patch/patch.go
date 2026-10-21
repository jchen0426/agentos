// Package patch reads the patches a model proposes and checks them before any tool acts on
// one: which files it changes and how, whether the policy lets it, and what the files become.
// No git command ever reads a patch, and agentd runs none on the mirror: the files' current
// contents come from the git tool server's read_files, in its sandbox. The patch is applied
// here, in Go.
package patch

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"path"
	"regexp"
	"strings"

	"github.com/bluekeyes/go-gitdiff/gitdiff"
	"github.com/bmatcuk/doublestar/v4"

	"github.com/jchen0426/agentos/internal/gitmcp"
)

// Limits on a patch. A fix for one finding is small. The git tool server takes as many files
// and reads files as large (gitmcp.MaxPaths, gitmcp.MaxReadBytes).
const (
	MaxBytes     = 256 << 10 // the patch itself
	MaxFiles     = 20        // files it touches
	MaxFileBytes = 1 << 20   // any file before or after the patch
	MaxDepth     = 32        // parts of a path: github.open_draft_pr lists each directory above one on GitHub
)

// regularFile is the only mode a patch may leave a file with: no symlinks, submodules or
// executables.
const regularFile = 0o100644

// Violation is a patch the rules refuse. Nothing was applied or sent.
type Violation struct{ Reason string }

func (v *Violation) Error() string { return "patch refused: " + v.Reason }

func violation(format string, args ...any) error {
	return &Violation{Reason: fmt.Sprintf(format, args...)}
}

// Patch is a parsed patch.
type Patch struct {
	files []*gitdiff.File
	lines []string // the patch's own
	hunks []hunk   // one for each text fragment of files, in order
	text  []byte   // the patch, its hunk headers as the hunks' lines say
}

// Change is what a patch does to one file.
type Change struct {
	Path    string
	Content []byte // the file after the patch; nil when Delete
	Delete  bool
	// OldBlob is git's name for the file the patch was applied to, as Source read it:
	// whoever writes the change can check that it is what the commit really holds. Empty
	// when the patch creates the file.
	OldBlob string
}

// Parse reads a unified diff, git's or a plain one. A plain one names its files with a
// leading component ("a/app.py", "b/app.py") that is dropped, as git apply does. The counts in
// its hunk headers are the hunks' lines', whatever the headers say (see hunks.go).
func Parse(b []byte) (*Patch, error) {
	if len(b) > MaxBytes {
		return nil, violation("the patch is %d bytes, over the limit of %d", len(b), MaxBytes)
	}
	lines, hunks, err := count(b)
	if err != nil {
		return nil, err
	}
	text := render(lines, hunks)
	if len(text) > MaxBytes {
		return nil, violation("the patch is %d bytes once its hunk headers are rewritten, over the limit of %d", len(text), MaxBytes)
	}
	files, err := parse(text)
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return nil, violation("the patch changes no file")
	}
	if len(files) > MaxFiles {
		return nil, violation("the patch changes %d files, over the limit of %d", len(files), MaxFiles)
	}
	n := 0
	for _, f := range files {
		n += len(f.TextFragments)
	}
	if n != len(hunks) {
		return nil, violation("the patch reads as %d hunks one way and %d another", len(hunks), n)
	}
	return &Patch{files: files, lines: lines, hunks: hunks, text: text}, nil
}

// parse is go-gitdiff's reading of a patch.
func parse(text []byte) ([]*gitdiff.File, error) {
	files, _, err := gitdiff.Parse(bytes.NewReader(text))
	if err != nil {
		return nil, violation("the patch does not parse: %v", err)
	}
	if !gitHeader.Match(text) {
		for _, f := range files {
			f.OldName, f.NewName = dropComponent(f.OldName), dropComponent(f.NewName)
		}
	}
	return files, nil
}

var gitHeader = regexp.MustCompile(`(?m)^diff --git `)

func dropComponent(name string) string {
	if _, rest, ok := strings.Cut(name, "/"); ok {
		return rest
	}
	return ""
}

// name is the path a file change is about.
func name(f *gitdiff.File) string {
	if f.IsDelete {
		return f.OldName
	}
	return f.NewName
}

// Paths lists the files the patch touches, in its order.
func (p *Patch) Paths() []string {
	out := make([]string, 0, len(p.files))
	for _, f := range p.files {
		out = append(out, name(f))
	}
	return out
}

// Check applies the rules every patch must meet, then the policy's deny_paths. Patterns
// match without regard to case: on a case-insensitive file system "makefile" is "Makefile",
// and make reads either. Paths are told apart the same way.
func (p *Patch) Check(denyPaths []string) error {
	seen := map[string]string{} // lower case -> as the patch names it
	for _, f := range p.files {
		n := name(f)
		if err := checkPath(n); err != nil {
			return err
		}
		switch {
		case f.IsBinary:
			return violation("%s: binary changes are not accepted", n)
		case f.IsRename || f.IsCopy || (!f.IsNew && !f.IsDelete && f.OldName != f.NewName):
			return violation("%s: renames and copies are not accepted", n)
		case f.OldMode != 0 && f.NewMode != 0 && f.OldMode != f.NewMode:
			return violation("%s: mode changes are not accepted", n)
		case !f.IsNew && !f.IsDelete && len(f.TextFragments) == 0:
			return violation("%s: the patch changes nothing in it", n) // git apply refuses it as no patch at all
		}
		mode := f.NewMode
		if f.IsDelete {
			mode = f.OldMode
		}
		if mode != 0 && mode != regularFile {
			return violation("%s: only regular files without the executable bit (mode 100644) are accepted, not %o", n, uint32(mode))
		}
		key := strings.ToLower(n)
		if _, twice := seen[key]; twice {
			return violation("%s: changed twice in one patch", n)
		}
		seen[key] = n
		for _, pattern := range denyPaths {
			if ok, err := doublestar.Match(strings.ToLower(pattern), key); err != nil {
				return fmt.Errorf("deny_paths pattern %q: %w", pattern, err)
			} else if ok {
				return violation("%s: the policy protects %s", n, pattern)
			}
		}
	}
	// A path above another one would be a file and a directory at once, in the one tree that
	// github.open_draft_pr sends. git apply accepts a patch that creates both.
	for _, f := range p.files {
		n := name(f)
		for i := strings.LastIndexByte(n, '/'); i > 0; i = strings.LastIndexByte(n[:i], '/') {
			if above, ok := seen[strings.ToLower(n[:i])]; ok {
				return violation("%s: the patch also changes %s, a directory above it", n, above)
			}
		}
	}
	return nil
}

// pathPart is what a component of an accepted path may contain: plain ASCII, so that
// matching deny_paths without regard to case means what it says, and no git pathspec magic
// (":/", "*") or look-alike characters get through.
var pathPart = regexp.MustCompile(`^[A-Za-z0-9._+-]+$`)

// checkPath accepts only plain relative paths inside the repository, and keeps git's own
// files out of reach.
func checkPath(n string) error {
	if n == "" {
		return violation("a file without a name")
	}
	if strings.HasPrefix(n, "/") || path.Clean(n) != n {
		return violation("%q: not a plain relative path", n)
	}
	if parts := strings.Count(n, "/") + 1; parts > MaxDepth {
		return violation("%.200q: %d parts deep, over the limit of %d", n, parts, MaxDepth)
	}
	for part := range strings.SplitSeq(n, "/") {
		if !pathPart.MatchString(part) {
			return violation("%q: only letters, digits and . _ + - are accepted in path names", n)
		}
		switch strings.ToLower(part) {
		case ".", "..", ".git", ".gitmodules", ".gitattributes":
			return violation("%q: paths through %s are not accepted", n, part)
		}
	}
	return nil
}

// Source reads files as they are at the commit the patch is for, all in one go: the content of
// each path the commit has a file at. A path it has nothing at is left out.
type Source func(paths []string) (map[string][]byte, error)

// Apply applies the patch to the files src reads, each hunk where its lines are (see place): a
// hunk whose context does not match exactly is a violation, not something to fuzz. Text is then
// the patch as applied.
func (p *Patch) Apply(src Source) ([]Change, error) {
	files, err := src(p.Paths())
	if err != nil {
		return nil, fmt.Errorf("read the files at the commit: %w", err)
	}
	changes := make([]Change, 0, len(p.files))
	hunks, moved := p.hunks, 0
	for _, f := range p.files {
		n := name(f)
		orig, exists := files[n]
		switch {
		case f.IsNew && exists:
			return nil, violation("%s: the patch creates it, but it exists", n)
		case !f.IsNew && !exists:
			return nil, violation("%s: no such file at the commit", n)
		case len(orig) > MaxFileBytes:
			return nil, violation("%s: %d bytes, over the limit of %d", n, len(orig), MaxFileBytes)
		}
		if err := place(n, f, orig, hunks[:len(f.TextFragments)], &moved); err != nil {
			return nil, err
		}
		hunks = hunks[len(f.TextFragments):]
		var out bytes.Buffer
		if err := gitdiff.Apply(&out, bytes.NewReader(orig), f); err != nil {
			return nil, violation("%s: the patch does not apply: %v", n, err)
		}
		c := Change{Path: n}
		if exists {
			c.OldBlob = BlobID(orig)
		}
		if f.IsDelete {
			if out.Len() != 0 {
				return nil, violation("%s: the patch deletes it but does not remove all of it", n)
			}
			c.Delete = true
			changes = append(changes, c)
			continue
		}
		if out.Len() > MaxFileBytes {
			return nil, violation("%s: %d bytes after the patch, over the limit of %d", n, out.Len(), MaxFileBytes)
		}
		c.Content = out.Bytes()
		changes = append(changes, c)
	}
	// go-gitdiff reads the patch as applied as the hunks were applied, or it is not the patch.
	text := render(p.lines, p.hunks)
	switch read, err := parse(text); {
	case len(text) > MaxBytes:
		return nil, violation("the patch is %d bytes once its hunk headers are rewritten, over the limit of %d", len(text), MaxBytes)
	case err != nil:
		return nil, violation("the patch as applied does not read as applied: %v", err)
	case !same(read, p.files):
		return nil, violation("the patch as applied does not read as applied")
	}
	p.text = text
	return changes, nil
}

// Text is the patch as Apply last applied it, its hunk headers counted from their lines and
// placed where the lines are in the file (before Apply, only counted): what git checks, what a
// reviewer reads, and what the changes are.
func (p *Patch) Text() []byte { return p.text }

// Rewritten lists the hunk headers Text has anew.
func (p *Patch) Rewritten() []Rewrite {
	var out []Rewrite
	k := 0
	for _, f := range p.files {
		for i := range f.TextFragments {
			if h := p.hunks[k]; h.now != h.was {
				out = append(out, Rewrite{Path: name(f), Hunk: i + 1, Was: h.was.String(), Now: h.now.String()})
			}
			k++
		}
	}
	return out
}

// BlobID is the name git gives a file of content b: the SHA-1 of its blob object.
func BlobID(b []byte) string {
	h := sha1.New()
	fmt.Fprintf(h, "blob %d\x00", len(b))
	h.Write(b)
	return hex.EncodeToString(h.Sum(nil))
}

// MirrorSource reads files at commit from the read-only mirror, through the git tool server
// (read_files), which runs git in its sandbox. Only regular files count, under directories: a
// patch to a symlink or a submodule, or to a path under one, is refused when it is applied. A
// file over MaxFileBytes is refused unread. The server's refusals (a crafted tree too large for
// git to read, say) are violations: they always will be. Its failures are errors, and the call
// is tried again. agentd can check the shape of what it reads, not its word: the Changes name
// the blobs read (OldBlob), and github.open_draft_pr checks them against GitHub.
func MirrorSource(ctx context.Context, git gitmcp.Caller, mirror, commit string) Source {
	return func(paths []string) (map[string][]byte, error) {
		files, err := gitmcp.ReadFiles(ctx, git, mirror, commit, paths)
		switch {
		case gitmcp.IsToolError(err):
			return nil, violation("%s", err)
		case err != nil:
			return nil, err
		}
		contents := map[string][]byte{}
		for _, f := range files {
			switch {
			case f.NotDir != "":
				return nil, violation("%s: %s is not a directory at the commit", f.Path, f.NotDir)
			case f.Type == "":
				// nothing there: a file the patch may create
			case f.Type != "blob" || f.Mode != "100644":
				return nil, violation("%s: not a regular file at the commit (%s %s)", f.Path, f.Mode, f.Type)
			case f.Size > MaxFileBytes:
				return nil, violation("%s: %d bytes, over the limit of %d", f.Path, f.Size, MaxFileBytes)
			case int64(len(f.Content)) != f.Size:
				// Not read whole (were the server's limit below this package's): never take it
				// for an empty file, which a patch could delete.
				return nil, fmt.Errorf("%s: %d bytes read of %d", f.Path, len(f.Content), f.Size)
			default:
				contents[f.Path] = f.Content
			}
		}
		return contents, nil
	}
}
