// Package gitmcp is the first task's read-only git MCP server (design doc, sections 3 and 4)
// and agentd's client for it. agentd runs the server once per call, in a sandbox that holds one
// mirror read-only, a private temporary directory, and no network. The server never writes the
// mirror: apply_check applies the patch to a throwaway index.
//
// It is also the only place git reads a mirror: agentd resolves a run's commit (resolve_commit)
// and reads the files a patch touches (read_files) through it too, so that a git bug a crafted
// repository sets off runs in the sandbox, not in agentd, which holds the keys.
//
// The commits it reads are untrusted, the mirror's own configuration is not: git runs in
// gitenv's environment and shows every file as text, and what it reads is sized up first.
package gitmcp

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unicode"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/jchen0426/agentos/internal/gitenv"
)

// Limits on what goes in and out. show_diff sizes up a commit before git diffs it: git holds
// both sides of a file in memory, with some 60 bytes more for every line, and a small crafted
// repository can hold huge files. A 4 MiB file of random short lines, the worst shape measured,
// takes git 30 s and 360 MB to diff on an M-series Mac; 8 MiB, over a minute and 660 MB.
const (
	MaxDiffBytes   = 1 << 20   // a diff larger than this is refused, not cut
	MaxFileBytes   = 4 << 20   // show_diff refuses a commit that changes a larger file...
	MaxDiffInput   = 256 << 20 // ...or more than this in all, both sides of every file counted
	MaxCommitBytes = 1 << 20   // a larger commit object is refused unread
	MaxPatchBytes  = 256 << 10 // as internal/patch
	MaxPaths       = 20        // files a patch may touch, as internal/patch; read_files reads as many
	MaxReadBytes   = 1 << 20   // read_files leaves out a larger file's content: internal/patch refuses it
	MaxPathBytes   = 4096      // the longest path read_files looks up
	// MaxRequest bounds one message to the server: a patch at its limit with every byte
	// escaped in JSON (a control character takes six), and room.
	MaxRequest = 6*MaxPatchBytes + 64<<10
)

var (
	fullSHA      = regexp.MustCompile(`^[0-9a-f]{40}$`)
	commitDigits = regexp.MustCompile(`^[0-9a-f]{7,40}$`)
)

// zeroID stands for "no file" on one side of a change.
const zeroID = "0000000000000000000000000000000000000000"

// Server serves the git tools on one mirror.
type Server struct {
	Mirror  string    // the read-only mirror
	TempDir string    // where apply_check keeps its throwaway index; os.TempDir() when empty
	Stderr  io.Writer // gets what git writes to its stderr, for agentd's log; nil discards it
}

type ShowDiffIn struct {
	Commit string `json:"commit" jsonschema:"the commit to show, a full 40-character SHA"`
}

type ShowDiffOut struct {
	// The diff's exact bytes, base64-encoded: a commit need not be UTF-8, and base64 stays as
	// compact in JSON whatever the diff holds.
	DiffBase64 string `json:"diff_base64" jsonschema:"the commit's changes against its first parent, as a patch, base64-encoded"`
}

// Diff decodes the diff.
func (o ShowDiffOut) Diff() ([]byte, error) { return base64.StdEncoding.DecodeString(o.DiffBase64) }

type ApplyCheckIn struct {
	Commit string   `json:"commit" jsonschema:"the commit the patch is for, a full 40-character SHA"`
	Patch  string   `json:"patch" jsonschema:"a unified diff"`
	Paths  []string `json:"paths" jsonschema:"the files the patch touches"`
}

type ApplyCheckOut struct {
	Applies bool   `json:"applies"`
	Reason  string `json:"reason,omitempty" jsonschema:"why git apply refuses the patch"`
	// Index is what git apply made of the files the patch touches: the throwaway index's entries
	// once the patch is applied to it. A file the patch deletes is not there. Whoever applies the
	// patch another way can check that it makes the same files.
	Index *[]IndexEntry `json:"index,omitempty" jsonschema:"the files the patch leaves, as git apply makes them"`
}

// IndexEntry is one entry of an index, as git ls-files -s shows it.
type IndexEntry struct {
	Path   string `json:"path"`
	Mode   string `json:"mode"`
	Object string `json:"object" jsonschema:"the file's blob"`
	Stage  int    `json:"stage"`
}

type ResolveCommitIn struct {
	Commit string `json:"commit" jsonschema:"7 to 40 hexadecimal digits of a commit's SHA"`
}

type ResolveCommitOut struct {
	Commit string `json:"commit" jsonschema:"the one commit the digits name, a full 40-character SHA"`
}

type ReadFilesIn struct {
	Commit string   `json:"commit" jsonschema:"the commit to read the files at, a full 40-character SHA"`
	Paths  []string `json:"paths" jsonschema:"the files to read, as paths from the top of the tree"`
}

type ReadFilesOut struct {
	Files []File `json:"files" jsonschema:"what the commit has at each path, in the order asked"`
}

// File is what a commit has at one path.
type File struct {
	Path string `json:"path"`
	// Mode and Type are the path's tree entry: 100644 and blob for a regular file. Both are
	// empty when the commit has nothing there.
	Mode string `json:"mode,omitempty" jsonschema:"the path's mode, as git ls-tree shows it; empty when the commit has nothing there"`
	Type string `json:"type,omitempty" jsonschema:"blob, tree or commit (a submodule); empty when the commit has nothing there"`
	// NotDir is a directory above Path that the commit has as something else (a file, a
	// symbolic link, a submodule): then nothing can be at Path.
	NotDir string `json:"not_dir,omitempty" jsonschema:"a directory above the path that the commit has as something else"`
	Size   int64  `json:"size,omitempty" jsonschema:"a blob's size in bytes"`
	// The blob's exact bytes, base64-encoded, when it is at most MaxReadBytes.
	ContentBase64 string `json:"content_base64,omitempty" jsonschema:"a blob's bytes, base64-encoded, when it is small enough to read"`
	// Content is ContentBase64 decoded, by ReadFiles.
	Content []byte `json:"-"`
}

// MCP is the MCP server with its tools.
func (s *Server) MCP() *mcp.Server {
	srv := mcp.NewServer(&mcp.Implementation{Name: "agentos-git", Version: "0.1"}, nil)
	mcp.AddTool(srv, &mcp.Tool{Name: "show_diff",
		Description: "The changes a commit makes against its first parent, as a patch."}, s.showDiff)
	mcp.AddTool(srv, &mcp.Tool{Name: "apply_check",
		Description: "Whether git apply accepts a patch on top of a commit, and what it makes of the files, without changing anything."}, s.applyCheck)
	mcp.AddTool(srv, &mcp.Tool{Name: "resolve_commit",
		Description: "The one commit that the first digits of its SHA name."}, s.resolveCommit)
	mcp.AddTool(srv, &mcp.Tool{Name: "read_files",
		Description: "What a commit has at some paths: each one's mode, type and, for a file small enough, content."}, s.readFiles)
	return srv
}

// Errors come back two ways. A tool error (IsError) says git refused what it was given: an
// unknown commit, a patch that does not apply, a diff over a limit. A protocol error says
// the server could not do its work (git missing, an unreadable mirror): the call may succeed
// later. Tool errors quote what the commit names, a path say, cut short: they reach the
// workflow and its report.
func unavailable(err error) error {
	return &jsonrpc.Error{Code: jsonrpc.CodeInternalError, Message: err.Error()}
}

// failed is a git command's failure as the tool's error: a refusal when git ran out of the
// memory or the CPU time it may use, since it always will on that commit, and unavailable
// otherwise.
func failed(err error) error {
	var g *gitError
	var cpu *cpuLimitError
	switch {
	case errors.As(err, &g) && gitenv.TooLarge(g.msg):
		return errors.New("the commit is too large for git to read within its memory limit")
	case errors.As(err, &cpu):
		return errors.New("the commit is too costly for git to read within its CPU time limit")
	}
	return unavailable(err)
}

// result is the tool's own short text: the structured output carries the data once, instead
// of a second, escaped copy in the text.
func result(text string) *mcp.CallToolResult {
	return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: text}}}
}

func (s *Server) showDiff(ctx context.Context, _ *mcp.CallToolRequest, in ShowDiffIn) (*mcp.CallToolResult, ShowDiffOut, error) {
	if err := s.commit(ctx, in.Commit); err != nil {
		return nil, ShowDiffOut{}, err
	}
	if err := s.fileSizes(ctx, in.Commit); err != nil {
		return nil, ShowDiffOut{}, err
	}
	// --root shows a first commit whole; a merge is shown against its first parent only (-m
	// --first-parent would show it against each parent in turn); --text shows every file,
	// binary or not. No external diff drivers or text conversion: those would run programs.
	out, err := s.git(ctx, nil, nil, MaxDiffBytes, "diff-tree", "-p", "--text", "--no-commit-id", "--root",
		"--diff-merges=first-parent", "--no-color", "--no-ext-diff", "--no-textconv", in.Commit)
	var tooLong *outputLimitError
	switch {
	case errors.As(err, &tooLong):
		return nil, ShowDiffOut{}, fmt.Errorf("the diff is over the limit of %d bytes", MaxDiffBytes)
	case err != nil:
		return nil, ShowDiffOut{}, failed(err)
	}
	return result(fmt.Sprintf("a diff of %d bytes", len(out))), ShowDiffOut{DiffBase64: base64.StdEncoding.EncodeToString(out)}, nil
}

func (s *Server) applyCheck(ctx context.Context, _ *mcp.CallToolRequest, in ApplyCheckIn) (*mcp.CallToolResult, ApplyCheckOut, error) {
	if len(in.Patch) > MaxPatchBytes {
		return nil, ApplyCheckOut{}, fmt.Errorf("the patch is %d bytes, over the limit of %d", len(in.Patch), MaxPatchBytes)
	}
	if len(in.Paths) == 0 || len(in.Paths) > MaxPaths {
		return nil, ApplyCheckOut{}, fmt.Errorf("a patch touches 1 to %d files, not %d", MaxPaths, len(in.Paths))
	}
	if err := s.commit(ctx, in.Commit); err != nil {
		return nil, ApplyCheckOut{}, err
	}
	dir, err := os.MkdirTemp(s.TempDir, "apply-check-")
	if err != nil {
		return nil, ApplyCheckOut{}, unavailable(err)
	}
	defer os.RemoveAll(dir)
	// The blobs of the files the patch makes go to a throwaway object directory, which reads the
	// mirror's through it: the mirror is never written.
	objects, err := s.git(ctx, nil, nil, 1<<16, "rev-parse", "--path-format=absolute", "--git-path", "objects")
	if err != nil {
		return nil, ApplyCheckOut{}, failed(err)
	}
	mirrorObjects := strings.TrimSuffix(string(objects), "\n")
	if strings.ContainsAny(mirrorObjects, ":\"\n") {
		return nil, ApplyCheckOut{}, unavailable(errors.New("the mirror's object directory is not a path git takes as an alternate"))
	}
	if err := os.Mkdir(filepath.Join(dir, "objects"), 0o700); err != nil {
		return nil, ApplyCheckOut{}, unavailable(err)
	}
	// A throwaway index holding only the files the patch touches, as the commit has them: the
	// rest of the tree, however large, is never read, and neither the mirror nor any worktree
	// changes.
	env := []string{"GIT_INDEX_FILE=" + filepath.Join(dir, "index"), "GIT_OBJECT_DIRECTORY=" + filepath.Join(dir, "objects"),
		"GIT_ALTERNATE_OBJECT_DIRECTORIES=" + mirrorObjects}
	entries, err := s.git(ctx, nil, nil, 1<<20, append([]string{"ls-tree", "-z", "--full-tree", in.Commit, "--"}, in.Paths...)...)
	if err != nil {
		return nil, ApplyCheckOut{}, failed(err)
	}
	if _, err := s.git(ctx, env, bytes.NewReader(entries), 0, "update-index", "-z", "--index-info"); err != nil {
		return nil, ApplyCheckOut{}, failed(err)
	}
	_, err = s.git(ctx, env, strings.NewReader(in.Patch), 0, "apply", "--cached", "-")
	var refused *gitError
	switch {
	case errors.As(err, &refused) && (refused.code == 1 || refused.code == 128) && !gitenv.TooLarge(refused.msg):
		// git apply's own "no": the patch does not fit the commit (1), or is not a patch git
		// can apply at all (128). The index was just built, so neither is about the mirror.
		return result("the patch does not apply"), ApplyCheckOut{Applies: false, Reason: refused.Error()}, nil
	case err != nil:
		return nil, ApplyCheckOut{}, failed(err)
	}
	listing, err := s.git(ctx, env, nil, 1<<20, "ls-files", "-s", "-z")
	if err != nil {
		return nil, ApplyCheckOut{}, failed(err)
	}
	index, err := indexEntries(listing)
	if err != nil {
		return nil, ApplyCheckOut{}, unavailable(err)
	}
	return result("the patch applies"), ApplyCheckOut{Applies: true, Index: &index}, nil
}

// indexEntries reads git ls-files -s -z: "<mode> <object> <stage>\t<path>", each ending in NUL.
func indexEntries(listing []byte) ([]IndexEntry, error) {
	entries := []IndexEntry{}
	for line := range bytes.SplitSeq(listing, []byte{0}) {
		if len(line) == 0 {
			continue
		}
		meta, path, ok := bytes.Cut(line, []byte{'\t'})
		f := strings.Fields(string(meta))
		if !ok || len(f) != 3 || !fullSHA.MatchString(f[1]) {
			return nil, fmt.Errorf("git ls-files: unexpected entry %.200q", line)
		}
		stage, err := strconv.Atoi(f[2])
		if err != nil {
			return nil, fmt.Errorf("git ls-files: unexpected entry %.200q", line)
		}
		entries = append(entries, IndexEntry{Path: string(path), Mode: f[0], Object: f[1], Stage: stage})
	}
	return entries, nil
}

// resolveCommit finds the one commit some digits name, from object headers alone: git reads a
// commit whole to peel it, and a crafted one can hold gigabytes that pack into megabytes. No
// such commit, or more than one, is a refusal: what the digits name is the same every time.
// But git looks past objects it cannot read (an alternate the sandbox hides, a pack being
// replaced), saying so only on stderr: finding nothing then is the server's failure, and the
// reason goes to agentd's log.
func (s *Server) resolveCommit(ctx context.Context, _ *mcp.CallToolRequest, in ResolveCommitIn) (*mcp.CallToolResult, ResolveCommitOut, error) {
	if !commitDigits.MatchString(in.Commit) {
		return nil, ResolveCommitOut{}, errors.New("commit must be 7 to 40 hexadecimal digits")
	}
	// Every object the digits may name, of any type; a listing longer than this belongs to a
	// crafted repository.
	names, complained, err := s.run(ctx, nil, nil, 1<<20, "rev-parse", "--disambiguate="+in.Commit)
	var tooLong *outputLimitError
	switch {
	case errors.As(err, &tooLong):
		return nil, ResolveCommitOut{}, fmt.Errorf("commit %s names too many objects in the mirror: give more of it", in.Commit)
	case err != nil:
		return nil, ResolveCommitOut{}, failed(err)
	}
	var found [][]string // object, type, size
	if len(bytes.TrimSpace(names)) > 0 {
		headers, alsoComplained, err := s.run(ctx, nil, bytes.NewReader(names), 0, "cat-file", "--batch-check=%(objectname) %(objecttype) %(objectsize)")
		if err != nil {
			return nil, ResolveCommitOut{}, failed(err)
		}
		var missing bool
		found, missing = commits(headers)
		complained = complained || alsoComplained || missing
	}
	switch {
	case len(found) == 0 && complained:
		return nil, ResolveCommitOut{}, unavailable(fmt.Errorf("git found no commit %s, but could not read every object", in.Commit))
	case len(found) == 0:
		return nil, ResolveCommitOut{}, fmt.Errorf("commit %s not found in the mirror", in.Commit)
	case len(found) > 1:
		return nil, ResolveCommitOut{}, fmt.Errorf("commit %s names %d commits in the mirror: give more of it", in.Commit, len(found))
	case !fullSHA.MatchString(found[0][0]):
		return nil, ResolveCommitOut{}, unavailable(fmt.Errorf("git cat-file: unexpected object name %q", found[0][0]))
	}
	if n, err := strconv.ParseInt(found[0][2], 10, 64); err != nil || n > MaxCommitBytes {
		return nil, ResolveCommitOut{}, fmt.Errorf("commit %s is %s bytes, over the limit of %d", in.Commit, found[0][2], MaxCommitBytes)
	}
	return result("commit " + found[0][0]), ResolveCommitOut{Commit: found[0][0]}, nil
}

// commits picks the commits out of git cat-file --batch-check's answer (object, type, size),
// and says whether some object it was asked about was not there to read.
func commits(headers []byte) (found [][]string, missing bool) {
	for line := range strings.Lines(string(headers)) {
		switch f := strings.Fields(line); {
		case len(f) == 3 && f[1] == "commit":
			found = append(found, f)
		case len(f) == 2 && f[1] == "missing":
			missing = true
		}
	}
	return found, missing
}

// readFiles looks up each path on its own, then reads the blobs small enough to read, from
// object headers first: only the files asked for, never the rest of a directory.
func (s *Server) readFiles(ctx context.Context, _ *mcp.CallToolRequest, in ReadFilesIn) (*mcp.CallToolResult, ReadFilesOut, error) {
	if len(in.Paths) == 0 || len(in.Paths) > MaxPaths {
		return nil, ReadFilesOut{}, fmt.Errorf("read_files reads 1 to %d files, not %d", MaxPaths, len(in.Paths))
	}
	asked := map[string]bool{}
	for _, p := range in.Paths {
		if err := checkPath(p); err != nil {
			return nil, ReadFilesOut{}, err
		}
		if asked[p] {
			return nil, ReadFilesOut{}, fmt.Errorf("%.200q: asked for twice", p)
		}
		asked[p] = true
	}
	if err := s.commit(ctx, in.Commit); err != nil {
		return nil, ReadFilesOut{}, err
	}
	files := make([]File, len(in.Paths))
	toRead, at := map[string]int64{}, map[string]string{} // object -> size, and a path it is at
	objects := make([]string, len(in.Paths))
	for i, p := range in.Paths {
		f, object, err := s.lookUp(ctx, in.Commit, p)
		if err != nil {
			return nil, ReadFilesOut{}, err
		}
		files[i] = f
		if f.Type == "blob" && f.Size <= MaxReadBytes {
			toRead[object], at[object], objects[i] = f.Size, p, object
		}
	}
	contents, err := s.blobs(ctx, toRead, at)
	if err != nil {
		return nil, ReadFilesOut{}, err
	}
	for i, object := range objects {
		if object != "" {
			files[i].ContentBase64 = base64.StdEncoding.EncodeToString(contents[object])
		}
	}
	return result(fmt.Sprintf("%d files", len(files))), ReadFilesOut{Files: files}, nil
}

// checkPath accepts a plain path from the top of the tree, as git's trees hold them: nothing
// for git to read as a pattern, a range or a path relative to somewhere else.
func checkPath(p string) error {
	switch {
	case p == "" || len(p) > MaxPathBytes:
		return fmt.Errorf("a path is 1 to %d bytes, not %d", MaxPathBytes, len(p))
	case strings.ContainsFunc(p, unicode.IsControl):
		return fmt.Errorf("%.200q: a path without control characters", p)
	case strings.HasPrefix(p, "/") || path.Clean(p) != p || p == "." || p == ".." || strings.HasPrefix(p, "../"):
		return fmt.Errorf("%.200q: not a plain path from the top of the tree", p)
	}
	return nil
}

// treeEntry is one line of git ls-tree -l.
type treeEntry struct{ mode, typ, object, size string }

// maxListing bounds one listing of a path and the directories on the way to it: longer belongs
// to a crafted tree, thousands of directories deep.
const maxListing = 1 << 20

// listing is what git ls-tree shows for p alone, by path: with -t, the directories on the way
// to p as well as p's own entry. Nothing else: not the other files in those directories, which
// ls-tree would list if a directory and a path inside it were asked for together. A crafted
// tree may list a name twice, and git shows both: a refusal.
func (s *Server) listing(ctx context.Context, commit, p string, flags ...string) (map[string]treeEntry, error) {
	args := append(append([]string{"ls-tree", "-z", "-l", "--full-tree"}, flags...), commit, "--", p)
	out, err := s.git(ctx, nil, nil, maxListing, args...)
	var tooLong *outputLimitError
	switch {
	case errors.As(err, &tooLong):
		return nil, fmt.Errorf("%.200q: the commit's tree lists too much on the way to it", p)
	case err != nil:
		return nil, failed(err)
	}
	entries := map[string]treeEntry{}
	for line := range bytes.SplitSeq(out, []byte{0}) {
		if len(line) == 0 {
			continue
		}
		meta, name, ok := bytes.Cut(line, []byte{'\t'})
		f := strings.Fields(string(meta))
		if !ok || len(f) != 4 {
			return nil, unavailable(fmt.Errorf("git ls-tree: unexpected entry %.200q", line))
		}
		if _, twice := entries[string(name)]; twice {
			return nil, fmt.Errorf("%.200q: the commit's tree lists it more than once", name)
		}
		entries[string(name)] = treeEntry{mode: f[0], typ: f[1], object: f[2], size: f[3]}
	}
	return entries, nil
}

// lookUp finds what the commit has at p, and the blob to read for it, if any, in two listings
// at most however deep p is. The first shows the directories on the way to p and p itself.
// When p is not there, the part of p just below the deepest directory shown decides: nothing
// is there, or something that is not a directory (a file, a link, a submodule), which a
// listing of that one path shows.
func (s *Server) lookUp(ctx context.Context, commit, p string) (File, string, error) {
	f := File{Path: p}
	onTheWay, err := s.listing(ctx, commit, p, "-t")
	if err != nil {
		return File{}, "", err
	}
	if e, ok := onTheWay[p]; ok {
		f.Mode, f.Type = e.mode, e.typ
		if e.typ != "blob" {
			return f, "", nil
		}
		n, err := strconv.ParseInt(e.size, 10, 64)
		if err != nil || n < 0 || !fullSHA.MatchString(e.object) {
			return File{}, "", unavailable(fmt.Errorf("git ls-tree: unexpected blob %q of %q bytes", e.object, e.size))
		}
		f.Size = n
		return f, e.object, nil
	}
	parts := strings.Split(p, "/")
	depth := 0 // of the deepest directory on the way that is there
	for depth < len(parts)-1 {
		if e, ok := onTheWay[strings.Join(parts[:depth+1], "/")]; !ok || e.typ != "tree" {
			break
		}
		depth++
	}
	next := strings.Join(parts[:depth+1], "/")
	if next == p {
		return f, "", nil // its directory is there, and it is not
	}
	below, err := s.listing(ctx, commit, next)
	e, ok := below[next]
	switch {
	case err != nil:
		return File{}, "", err
	case !ok:
		return f, "", nil
	case e.typ == "tree":
		return File{}, "", unavailable(fmt.Errorf("git ls-tree: %.200q is a directory in one listing only", next))
	}
	f.NotDir = next
	return f, "", nil
}

// blobs reads the blobs, whose sizes git ls-tree gave, in one git cat-file. at names a path
// each one is at, for refusals.
func (s *Server) blobs(ctx context.Context, sizes map[string]int64, at map[string]string) (map[string][]byte, error) {
	if len(sizes) == 0 {
		return nil, nil
	}
	objects := slices.Sorted(maps.Keys(sizes))
	limit := 0
	for _, o := range objects {
		limit += int(sizes[o]) + 128 // "<object> blob <size>\n<content>\n"
	}
	out, err := s.git(ctx, nil, strings.NewReader(strings.Join(objects, "\n")+"\n"), limit, "cat-file", "--batch")
	if err != nil {
		return nil, failed(err)
	}
	contents := map[string][]byte{}
	for _, o := range objects {
		header, rest, _ := bytes.Cut(out, []byte{'\n'})
		f := strings.Fields(string(header)) // "<object> blob <size>", or "<object> missing"
		size := sizes[o]
		switch {
		case len(f) == 3 && f[0] == o && f[1] != "blob":
			// A crafted tree gives the path a file's mode and another kind of object: the same
			// every time.
			return nil, fmt.Errorf("%.200q: the commit's tree gives it a file's mode but a %s object", at[o], f[1])
		case len(f) != 3 || f[0] != o || f[2] != strconv.FormatInt(size, 10) || int64(len(rest)) <= size || rest[size] != '\n':
			return nil, unavailable(fmt.Errorf("git cat-file: unexpected answer %.200q for %s", header, o))
		}
		contents[o], out = rest[:size:size], rest[size+1:]
	}
	return contents, nil
}

// commit checks that c names a commit of at most MaxCommitBytes in the mirror, from the
// object's header alone: git reads a commit whole, and a crafted one can hold gigabytes that
// pack into megabytes. Another kind of object, or a commit over the limit, is a tool error,
// which no retry changes. A missing one is not: resolve_commit found the run's commit in this
// mirror when the run started, so the mirror is what changed (being cloned again, say).
func (s *Server) commit(ctx context.Context, c string) error {
	if !fullSHA.MatchString(c) {
		return errors.New("commit must be a full 40-character SHA")
	}
	out, err := s.git(ctx, nil, strings.NewReader(c+"\n"), 0, "cat-file", "--batch-check=%(objecttype) %(objectsize)")
	if err != nil {
		return failed(err)
	}
	f := strings.Fields(string(out)) // "commit 251", or "<c> missing"
	switch {
	case len(f) == 2 && f[1] == "missing":
		return unavailable(fmt.Errorf("no commit %s in the mirror", c))
	case len(f) != 2:
		return unavailable(fmt.Errorf("git cat-file: unexpected answer %q", out))
	case f[0] != "commit":
		return fmt.Errorf("%s is a %s, not a commit", c, f[0])
	}
	if n, err := strconv.ParseInt(f[1], 10, 64); err != nil || n > MaxCommitBytes {
		return fmt.Errorf("commit %s is %s bytes, over the limit of %d", c, f[1], MaxCommitBytes)
	}
	return nil
}

// fileSizes refuses a commit that changes a file over MaxFileBytes, before or after, or more
// than MaxDiffInput in all.
func (s *Server) fileSizes(ctx context.Context, c string) error {
	// A listing longer than this belongs to a diff far over MaxDiffBytes.
	raw, err := s.git(ctx, nil, nil, 4<<20, "diff-tree", "-r", "-z", "--raw", "--no-abbrev", "--no-commit-id", "--root",
		"--diff-merges=first-parent", c)
	var tooLong *outputLimitError
	switch {
	case errors.As(err, &tooLong):
		return fmt.Errorf("the commit changes too many files: its diff is over the limit of %d bytes", MaxDiffBytes)
	case err != nil:
		return failed(err)
	}
	// Each change is ":<mode> <mode> <object> <object> <status>" and a path, each ending in NUL.
	var objects []string
	paths := map[string]string{}
	fields := bytes.Split(raw, []byte{0})
	for i := 0; i+1 < len(fields); i += 2 {
		f := strings.Fields(strings.TrimPrefix(string(fields[i]), ":"))
		if len(f) != 5 {
			return unavailable(fmt.Errorf("git diff-tree: unexpected change %q", fields[i]))
		}
		for side := range 2 {
			// A submodule's object is a commit in another repository: git shows its ID only.
			if mode, object := f[side], f[2+side]; mode != "160000" && object != zeroID {
				objects = append(objects, object)
				paths[object] = string(fields[i+1])
			}
		}
	}
	if len(objects) == 0 {
		return nil
	}
	sizes, err := s.git(ctx, nil, strings.NewReader(strings.Join(objects, "\n")+"\n"), 0, "cat-file",
		"--batch-check=%(objectname) %(objecttype) %(objectsize)")
	if err != nil {
		return failed(err)
	}
	var total int64
	for _, line := range strings.Split(strings.TrimSpace(string(sizes)), "\n") {
		f := strings.Fields(line) // or "<object> missing": the diff reports that
		if len(f) != 3 || f[1] != "blob" {
			continue
		}
		n, err := strconv.ParseInt(f[2], 10, 64)
		if err != nil || n > MaxFileBytes {
			return fmt.Errorf("%.200q: %s bytes, over the limit of %d for a file the diff shows", paths[f[0]], f[2], MaxFileBytes)
		}
		total += n
	}
	if total > MaxDiffInput {
		return fmt.Errorf("the commit changes %d bytes of files, over the limit of %d for a diff", total, MaxDiffInput)
	}
	return nil
}

// gitError is git's own refusal: its exit code and message.
type gitError struct {
	code int
	msg  string
}

func (e *gitError) Error() string { return e.msg }

// outputLimitError means git wrote more than the caller allows; git was stopped there.
type outputLimitError struct{}

func (*outputLimitError) Error() string { return "output over the limit" }

// cpuLimitError means git used up the CPU time the launcher allows it.
type cpuLimitError struct{ cmd string }

func (e *cpuLimitError) Error() string { return "git " + e.cmd + ": killed at its CPU time limit" }

// cpuLimited reports whether a process died at its CPU time limit: of SIGXCPU at the soft
// limit, or of SIGKILL having used as much CPU as the limit (Linux sends SIGKILL, not SIGXCPU,
// at a hard limit equal to the soft one). The server has the same limits as git: they are
// the launcher's. Only SIGXCPU is certain: Linux kills on a CPU clock it samples at each tick,
// which can run a tenth or more ahead of the CPU time wait reports when other processes wake
// for short spells on the same CPU, so a launcher keeps the hard limit above the soft one, as
// run-tool.sh does.
func cpuLimited(st *os.ProcessState) bool {
	ws, ok := st.Sys().(syscall.WaitStatus)
	if !ok || !ws.Signaled() {
		return false
	}
	switch ws.Signal() {
	case syscall.SIGXCPU:
		return true
	case syscall.SIGKILL:
		var rl syscall.Rlimit // seconds; RLIM_INFINITY, whatever its value on this OS, is far more
		if err := syscall.Getrlimit(syscall.RLIMIT_CPU, &rl); err != nil || rl.Cur > 1<<31 {
			return false
		}
		return st.UserTime()+st.SystemTime() >= time.Duration(rl.Cur)*time.Second
	}
	return false
}

// git runs a read-only git command in the mirror. With limit > 0, it reads at most limit bytes
// of output and stops git past that: a small crafted repository can make a huge diff. Its
// errors name the mirror "<mirror>": they reach the workflow, and from there maybe a public
// report.
func (s *Server) git(ctx context.Context, env []string, stdin io.Reader, limit int, args ...string) ([]byte, error) {
	out, _, err := s.run(ctx, env, stdin, limit, args...)
	return out, err
}

// run is git, and says whether git wrote anything to stderr on the way (which goes to agentd's
// log either way).
func (s *Server) run(ctx context.Context, env []string, stdin io.Reader, limit int, args ...string) ([]byte, bool, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	cmd := exec.CommandContext(ctx, "git", append([]string{"-C", s.Mirror}, args...)...)
	cmd.Env = gitenv.ForMirror(s.Mirror, env...)
	cmd.Stdin = stdin
	stderr := &limitedBuffer{max: 16 << 10}
	cmd.Stderr = stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, false, err
	}
	if err := cmd.Start(); err != nil {
		return nil, false, fmt.Errorf("git %s: %w", args[0], err)
	}
	r := io.Reader(stdout)
	if limit > 0 {
		r = io.LimitReader(stdout, int64(limit)+1)
	}
	out, readErr := io.ReadAll(r)
	if limit > 0 && len(out) > limit {
		cancel() // stop git; whatever it says now does not matter
		_ = cmd.Wait()
		return nil, false, &outputLimitError{}
	}
	err = cmd.Wait()
	msg := stderr.String()
	if msg != "" && s.Stderr != nil {
		fmt.Fprintf(s.Stderr, "git %s: %s\n", args[0], msg)
	}
	if err != nil {
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			if cpuLimited(exit.ProcessState) {
				return nil, false, &cpuLimitError{cmd: args[0]}
			}
			if exit.ExitCode() > 0 {
				msg := strings.ReplaceAll(msg, s.Mirror, "<mirror>")
				return nil, false, &gitError{code: exit.ExitCode(), msg: fmt.Sprintf("git %s: %s", args[0], msg)}
			}
		}
		return nil, false, fmt.Errorf("git %s: %w", args[0], err)
	}
	if readErr != nil {
		return nil, false, fmt.Errorf("git %s: %w", args[0], readErr)
	}
	return out, msg != "", nil
}
