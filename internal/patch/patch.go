// Package patch reads the patches a model proposes and checks them before any tool acts on
// one: which files it changes and how, whether the policy lets it, and what the files become.
// No git command ever reads a patch; the files' current contents come from the read-only
// mirror, through git cat-file, which runs no filters, hooks or diff drivers.
package patch

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"path"
	"regexp"
	"strconv"
	"strings"

	"github.com/bluekeyes/go-gitdiff/gitdiff"
	"github.com/bmatcuk/doublestar/v4"

	"github.com/jchen0426/agentos/internal/gitenv"
)

// Limits on a patch. A fix for one finding is small.
const (
	MaxBytes     = 256 << 10 // the patch itself
	MaxFiles     = 20        // files it touches
	MaxFileBytes = 1 << 20   // any file before or after the patch
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
type Patch struct{ files []*gitdiff.File }

// Change is what a patch does to one file.
type Change struct {
	Path    string
	Content []byte // the file after the patch; nil when Delete
	Delete  bool
}

// Parse reads a unified diff, git's or a plain one. A plain one names its files with a
// leading component ("a/app.py", "b/app.py") that is dropped, as git apply does.
func Parse(b []byte) (*Patch, error) {
	if len(b) > MaxBytes {
		return nil, violation("the patch is %d bytes, over the limit of %d", len(b), MaxBytes)
	}
	files, _, err := gitdiff.Parse(bytes.NewReader(b))
	if err != nil {
		return nil, violation("the patch does not parse: %v", err)
	}
	if len(files) == 0 {
		return nil, violation("the patch changes no file")
	}
	if len(files) > MaxFiles {
		return nil, violation("the patch changes %d files, over the limit of %d", len(files), MaxFiles)
	}
	if !gitHeader.Match(b) {
		for _, f := range files {
			f.OldName, f.NewName = dropComponent(f.OldName), dropComponent(f.NewName)
		}
	}
	return &Patch{files: files}, nil
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
// and make reads either.
func (p *Patch) Check(denyPaths []string) error {
	seen := map[string]bool{}
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
		if seen[key] {
			return violation("%s: changed twice in one patch", n)
		}
		seen[key] = true
		for _, pattern := range denyPaths {
			if ok, err := doublestar.Match(strings.ToLower(pattern), key); err != nil {
				return fmt.Errorf("deny_paths pattern %q: %w", pattern, err)
			} else if ok {
				return violation("%s: the policy protects %s", n, pattern)
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

// Source reads a file as it is at the commit the patch is for; ok is false when the commit
// has no such file.
type Source func(path string) (content []byte, ok bool, err error)

// Apply applies the patch to the files src reads, exactly where its hunks say: a hunk whose
// context does not match is a violation, not something to fuzz.
func (p *Patch) Apply(src Source) ([]Change, error) {
	changes := make([]Change, 0, len(p.files))
	for _, f := range p.files {
		n := name(f)
		orig, exists, err := src(n)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", n, err)
		}
		switch {
		case f.IsNew && exists:
			return nil, violation("%s: the patch creates it, but it exists", n)
		case !f.IsNew && !exists:
			return nil, violation("%s: no such file at the commit", n)
		case len(orig) > MaxFileBytes:
			return nil, violation("%s: %d bytes, over the limit of %d", n, len(orig), MaxFileBytes)
		}
		var out bytes.Buffer
		if err := gitdiff.Apply(&out, bytes.NewReader(orig), f); err != nil {
			return nil, violation("%s: the patch does not apply: %v", n, err)
		}
		if f.IsDelete {
			if out.Len() != 0 {
				return nil, violation("%s: the patch deletes it but does not remove all of it", n)
			}
			changes = append(changes, Change{Path: n, Delete: true})
			continue
		}
		if out.Len() > MaxFileBytes {
			return nil, violation("%s: %d bytes after the patch, over the limit of %d", n, out.Len(), MaxFileBytes)
		}
		changes = append(changes, Change{Path: n, Content: out.Bytes()})
	}
	return changes, nil
}

// MirrorSource reads files at commit from the read-only mirror. Only regular files count,
// under directories: a patch to a symlink or a submodule, or to a path under one, is refused
// when it is applied. A file over MaxFileBytes is refused before it is read.
func MirrorSource(ctx context.Context, mirror, commit string) Source {
	return func(n string) ([]byte, bool, error) {
		// The file and each directory above it, in one listing.
		parts := strings.Split(n, "/")
		paths := make([]string, len(parts))
		for i := range parts {
			paths[i] = strings.Join(parts[:i+1], "/")
		}
		out, err := git(ctx, mirror, append([]string{"ls-tree", "-z", "-l", "--full-tree", commit, "--"}, paths...)...)
		if err != nil {
			return nil, false, err
		}
		entries := map[string][]string{} // path -> mode, type, object, size
		for entry := range bytes.SplitSeq(bytes.TrimSuffix(out, []byte{0}), []byte{0}) {
			meta, p, ok := bytes.Cut(entry, []byte{'\t'})
			fields := strings.Fields(string(meta))
			if !ok || len(fields) != 4 {
				return nil, false, fmt.Errorf("git ls-tree: unexpected entry %q", entry)
			}
			entries[string(p)] = fields
		}
		for _, dir := range paths[:len(paths)-1] {
			if e, ok := entries[dir]; ok && e[1] != "tree" {
				return nil, false, violation("%s: %s is not a directory at the commit", n, dir)
			}
		}
		e, ok := entries[n]
		switch {
		case !ok:
			return nil, false, nil
		case e[1] != "blob" || e[0] != "100644":
			return nil, false, violation("%s: not a regular file at the commit (%s %s)", n, e[0], e[1])
		}
		if size, err := strconv.ParseInt(e[3], 10, 64); err != nil || size > MaxFileBytes {
			return nil, false, violation("%s: %s bytes, over the limit of %d", n, e[3], MaxFileBytes)
		}
		b, err := git(ctx, mirror, "cat-file", "blob", e[2])
		return b, err == nil, err
	}
}

// git runs a read-only git command in the mirror, in gitenv's environment: replace refs, say,
// could hand over another file than the commit's. A crafted tree too large to read within
// gitenv.AllocLimit is a violation: it always will be. Its errors name the mirror "<mirror>":
// they may reach the workflow, and from there a public report.
func git(ctx context.Context, mirror string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "git", append([]string{"-C", mirror}, args...)...)
	cmd.Env = gitenv.ForMirror(mirror)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			if gitenv.TooLarge(stderr.String()) {
				return nil, violation("the commit is too large for git to read within its memory limit")
			}
			msg := strings.ReplaceAll(strings.TrimSpace(stderr.String()), mirror, "<mirror>")
			return nil, fmt.Errorf("git %s: %s", args[0], msg)
		}
		return nil, fmt.Errorf("git %s: %w", args[0], err)
	}
	return out, nil
}
