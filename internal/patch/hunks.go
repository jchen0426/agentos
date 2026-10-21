package patch

import (
	"bytes"
	"cmp"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/bluekeyes/go-gitdiff/gitdiff"
)

// The numbers in a hunk header are not taken on trust: models get them wrong, and so does a person
// who edits a patch by hand. A hunk is its lines. They say how many lines of the file it holds,
// and Parse rewrites the header's counts to match (count); they say where it goes, and Apply finds
// them in the file (place). Nothing else of the patch changes. git apply and go-gitdiff, which
// believe a header, then read the patch as its lines say, and so does a reviewer.

// MaxMoved is how many hunks of a patch Apply looks for in their files when their lines are not
// where their headers put them: each look is a pass over the file.
const MaxMoved = 32

// hunk is a hunk header of the patch: the line it is on, what it says, and what it is made to say.
type hunk struct {
	line     int    // its index in the patch's lines
	was, now span   // as the patch wrote it; as Parse counted it, and Apply placed it
	rest     string // what follows its second "@@", the line's end included
}

// span is what a hunk header says: where the hunk starts in the file before and after, and how
// many lines it holds of each.
type span struct{ oldPos, oldLines, newPos, newLines int64 }

// String is the header as git writes it, a count of 1 left out.
func (s span) String() string {
	r := func(pos, n int64) string {
		if n == 1 {
			return strconv.FormatInt(pos, 10)
		}
		return fmt.Sprintf("%d,%d", pos, n)
	}
	return "@@ -" + r(s.oldPos, s.oldLines) + " +" + r(s.newPos, s.newLines) + " @@"
}

// Rewrite is a hunk header the patch as applied has anew: what the patch said, and what the
// hunk's lines say.
type Rewrite struct {
	Path     string
	Hunk     int    // in the file, from 1
	Was, Now string // "@@ -98,17 +98,23 @@"
}

// splitLines splits s after each newline; the last line may have none.
func splitLines(s string) []string {
	lines := strings.SplitAfter(s, "\n")
	if lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	return lines
}

// parseHunkHeader reads a hunk header as go-gitdiff does: "@@ -a,b +c,d @@" and what follows, a
// count of 1 left out or not.
func parseHunkHeader(line string) (s span, rest string, ok bool) {
	const start, end = "@@ -", " @@"
	if !strings.HasPrefix(line, start) {
		return span{}, "", false
	}
	parts := strings.SplitAfterN(line, end, 2)
	if len(parts) < 2 {
		return span{}, "", false
	}
	ranges := strings.Split(parts[0][len(start):len(parts[0])-len(end)], " +")
	if len(ranges) != 2 {
		return span{}, "", false
	}
	var okOld, okNew bool
	s.oldPos, s.oldLines, okOld = parseRange(ranges[0])
	s.newPos, s.newLines, okNew = parseRange(ranges[1])
	return s, parts[1], okOld && okNew
}

func parseRange(r string) (pos, n int64, ok bool) {
	p, c, comma := strings.Cut(r, ",")
	pos, err := strconv.ParseInt(p, 10, 64)
	if err != nil {
		return 0, 0, false
	}
	if !comma {
		return pos, 1, true
	}
	n, err = strconv.ParseInt(c, 10, 64)
	return pos, n, err == nil
}

func isHunkHeader(line string) bool {
	_, _, ok := parseHunkHeader(line)
	return ok
}

// hunkLine reports whether l can be a line of a hunk: one of the file before and after (' ', or
// an empty line, as git and go-gitdiff take it), one it removes ('-') or adds ('+'), or git's
// "\ No newline at end of file", which git and go-gitdiff know by its start and its length.
func hunkLine(l string) bool {
	switch {
	case l == "\n":
		return true
	case l == "":
		return false
	case l[0] == ' ' || l[0] == '-' || l[0] == '+':
		return true
	}
	return len(l) >= 12 && strings.HasPrefix(l, "\\ ")
}

// startsFile reports whether a file's header starts at lines[j], as git and go-gitdiff find one:
// a "diff" line, or "--- " and "+++ " lines before a hunk header.
func startsFile(lines []string, j int) bool {
	if strings.HasPrefix(lines[j], "diff ") {
		return true
	}
	return strings.HasPrefix(lines[j], "--- ") && j+2 < len(lines) && strings.HasPrefix(lines[j+1], "+++ ") &&
		strings.HasPrefix(lines[j+2], "@@ -") && len(lines[j+2]) >= len("@@ -1 +1 @@\n")
}

// trailer reports whether lines[j:] are text after the patch, which applies to nothing: none of
// them a hunk's line, a hunk header or a file's header, or else git format-patch's signature ("-- "
// and the lines after it).
func trailer(lines []string, j int) bool {
	if lines[j] == "-- \n" {
		if j++; j == len(lines) || lines[j] == "\n" {
			return false // a hunk's line, which removes "- "
		}
	}
	for ; j < len(lines); j++ {
		if l := lines[j]; l != "\n" && (hunkLine(l) || strings.HasPrefix(l, "@@") || startsFile(lines, j)) {
			return false
		}
	}
	return true
}

// hunkEnd is where the hunk whose lines start at lines[from] ends: at the first line that cannot be
// a hunk's, at a file's header, or at the end. Blank lines at its end are the hunk's only when
// another of the file's hunks follows them: then they cannot be anything else, as both git and
// go-gitdiff would refuse the next hunk as a hunk of no file. Otherwise they are as likely to set
// the next file off.
func hunkEnd(lines []string, from int) int {
	j := from
	for j < len(lines) && hunkLine(lines[j]) && !startsFile(lines, j) && !(lines[j] == "-- \n" && trailer(lines, j)) {
		j++
	}
	if j < len(lines) && isHunkHeader(lines[j]) {
		return j
	}
	for j > from && lines[j-1] == "\n" {
		j--
	}
	return j
}

// countLines counts the lines of the file a hunk's lines hold, before and after it.
func countLines(lines []string) (before, after int64) {
	for _, l := range lines {
		switch {
		case l == "\n" || l[0] == ' ':
			before++
			after++
		case l[0] == '-':
			before++
		case l[0] == '+':
			after++
		}
	}
	return before, after
}

// count reads the patch's lines and finds its hunks (see hunkEnd), with the lines of the file each
// holds. After a hunk, a line in no hunk must start another file, or be text after the patch (see
// trailer): a line of a hunk after it would read two ways. The last line of the patch gets its
// newline back when it is a hunk's: git apply refuses a hunk's line without one, and go-gitdiff
// reads it as a file that ends without a newline.
func count(b []byte) ([]string, []hunk, error) {
	if len(b) == 0 {
		return nil, nil, nil
	}
	lines := splitLines(string(b))
	var hunks []hunk
	for i := 0; i < len(lines); {
		s, rest, ok := parseHunkHeader(lines[i])
		if !ok {
			if strings.HasPrefix(lines[i], "@@ -") {
				return nil, nil, violation("line %d of the patch is not a hunk header: %.80q", i+1, lines[i])
			}
			i++ // a file's header, or what comes before the first file
			continue
		}
		end := hunkEnd(lines, i+1)
		h := hunk{line: i, was: s, now: s, rest: rest}
		h.now.oldLines, h.now.newLines = countLines(lines[i+1 : end])
		hunks = append(hunks, h)
		if end == len(lines) && end > i+1 && !strings.HasSuffix(lines[end-1], "\n") {
			lines[end-1] += "\n"
		}
		for i = end; i < len(lines) && lines[i] == "\n"; i++ {
		}
		if i < len(lines) && !strings.HasPrefix(lines[i], "@@ -") && !startsFile(lines, i) {
			if !trailer(lines, i) {
				return nil, nil, violation("line %d of the patch is in no hunk and starts no file: %.80q", i+1, lines[i])
			}
			break
		}
	}
	return lines, hunks, nil
}

// render is the patch with the header of each hunk whose numbers changed rewritten.
func render(lines []string, hunks []hunk) []byte {
	var b bytes.Buffer
	k := 0
	for i, l := range lines {
		if k < len(hunks) && hunks[k].line == i {
			if h := hunks[k]; h.now != h.was {
				l = h.now.String() + h.rest
			}
			k++
		}
		b.WriteString(l)
	}
	return b.Bytes()
}

// place puts each hunk of f where its lines are in orig, the file at the commit: where its header
// puts it if its lines (its context and the lines it removes) are there, else in the one place in
// the file where they are. Lines that are nowhere, or in more than one other place, refuse the
// patch, and so does a hunk with none, but one that gives an empty or a new file its first lines:
// nothing says where it goes. Then the new side of each hunk starts where the old one does, moved
// by what the hunks before it in the file add and remove. The hunks' headers are made to say so.
func place(path string, f *gitdiff.File, orig []byte, hunks []hunk, moved *int) error {
	lines := splitLines(string(orig))
	for i, frag := range f.TextFragments {
		frag.OldPosition = hunks[i].was.oldPos // as the patch says, however often the patch is applied
		var old []string
		for _, l := range frag.Lines {
			if l.Old() {
				old = append(old, l.Line)
			}
		}
		switch {
		case len(old) == 0 && len(orig) > 0:
			return violation("%s: the patch does not apply: hunk %d holds no line of the file (no context, nothing it removes) to say where it goes", path, i+1)
		case len(old) == 0:
			frag.OldPosition = 0
		case !at(lines, old, frag.OldPosition):
			found := find(lines, old, 2)
			switch len(found) {
			case 0:
				return violation("%s: the patch does not apply: the lines of hunk %d (its context and the lines it removes) are not in the file at the commit",
					path, i+1)
			case 2:
				return violation("%s: the patch does not apply: the lines of hunk %d are at lines %d and %d of the file at the commit, not at line %d, where its header puts them",
					path, i+1, found[0], found[1], frag.OldPosition)
			}
			if *moved++; *moved > MaxMoved {
				return violation("more than %d hunks of the patch are not where their headers put them", MaxMoved)
			}
			frag.OldPosition = found[0]
		}
	}
	inFile := slices.Clone(f.TextFragments)
	slices.SortStableFunc(inFile, func(a, b *gitdiff.TextFragment) int { return cmp.Compare(a.OldPosition, b.OldPosition) })
	var shift int64
	for _, frag := range inFile {
		first := frag.OldPosition // the first line of the old side, or the line before none
		if frag.OldLines == 0 {
			first++
		}
		frag.NewPosition = first + shift
		if frag.NewLines == 0 {
			frag.NewPosition-- // the line before none
		}
		shift += frag.NewLines - frag.OldLines
	}
	for i, frag := range f.TextFragments {
		hunks[i].now = span{frag.OldPosition, frag.OldLines, frag.NewPosition, frag.NewLines}
	}
	return nil
}

// at reports whether old are the lines of the file from line pos (from 1) on.
func at(lines, old []string, pos int64) bool {
	if pos < 1 || pos-1 > int64(len(lines)-len(old)) {
		return false
	}
	return slices.Equal(lines[pos-1:pos-1+int64(len(old))], old)
}

// find is where old starts in lines (from 1), at most limit times, in the file's order. Knuth,
// Morris and Pratt's search: the lines of the file are compared no more than twice each, so that
// a file of one line many times over costs no more than any other.
func find(lines, old []string, limit int) []int64 {
	next := make([]int, len(old)) // the longest proper prefix of old[:i+1] that ends it too
	for i, k := 1, 0; i < len(old); i++ {
		for k > 0 && old[i] != old[k] {
			k = next[k-1]
		}
		if old[i] == old[k] {
			k++
		}
		next[i] = k
	}
	var found []int64
	for i, k := 0, 0; i < len(lines); i++ {
		for k > 0 && lines[i] != old[k] {
			k = next[k-1]
		}
		if lines[i] == old[k] {
			k++
		}
		if k == len(old) {
			if found = append(found, int64(i-len(old)+2)); len(found) == limit {
				break
			}
			k = next[k-1]
		}
	}
	return found
}

// same reports whether two readings of a patch are the same files with the same hunks.
func same(a, b []*gitdiff.File) bool {
	return slices.EqualFunc(a, b, func(x, y *gitdiff.File) bool {
		return x.OldName == y.OldName && x.NewName == y.NewName && x.IsNew == y.IsNew && x.IsDelete == y.IsDelete &&
			x.IsRename == y.IsRename && x.IsCopy == y.IsCopy && x.IsBinary == y.IsBinary &&
			x.OldMode == y.OldMode && x.NewMode == y.NewMode &&
			slices.EqualFunc(x.TextFragments, y.TextFragments, func(f, g *gitdiff.TextFragment) bool {
				return f.OldPosition == g.OldPosition && f.OldLines == g.OldLines && f.NewPosition == g.NewPosition &&
					f.NewLines == g.NewLines && slices.Equal(f.Lines, g.Lines)
			})
	})
}
