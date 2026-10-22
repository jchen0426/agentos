package patch

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/bluekeyes/go-gitdiff/gitdiff"
)

// The numbers in a hunk header are not taken on trust: models get them wrong, and so does a person
// who edits a patch by hand. A hunk is its lines. They say how many lines of the file it holds
// (count, for Parse), and where it goes (place, for Apply): where its header says if they are there,
// else the one place in the file they are. The patch as applied (Text) is then written anew from
// what was applied, its file headers and hunks and nothing else, so that what a reviewer reads, what
// git checks and what is committed are one patch, which git apply and go-gitdiff read alike.

// MaxSearches is how many times, in all a patch's files, Apply looks through a file for a hunk's
// lines: for a hunk that is not where its header puts it, and for one whose counts would put it
// elsewhere. Each look is a pass over the file.
const MaxSearches = 32

// hunk is a hunk header of the patch and what it is made to say.
type hunk struct {
	line   int    // its index in the patch's lines
	header string // as the patch wrote it, up to its second "@@"
	was    span   // what it says
	now    span   // what the hunk's lines say: counted by Parse, placed by Apply
	blanks int    // the blank lines that end the hunk (see place)
	rest   string // what follows its second "@@", the line's end included
}

// span is what a hunk header says: where the hunk starts in the file before and after, and how
// many lines it holds of each.
type span struct{ oldPos, oldLines, newPos, newLines int64 }

// String is the header as go-gitdiff writes it, and Text has it.
func (s span) String() string {
	return fmt.Sprintf("@@ -%d,%d +%d,%d @@", s.oldPos, s.oldLines, s.newPos, s.newLines)
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

// countLine is how many lines of the file before and after a hunk's line l stands for.
func countLine(l string) (before, after int64) {
	switch {
	case l == "\n" || l[0] == ' ':
		return 1, 1
	case l[0] == '-':
		return 1, 0
	case l[0] == '+':
		return 0, 1
	}
	return 0, 0
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

// signature reports whether lines[j:] are git format-patch's signature: "-- ", then text to the
// end, which applies to nothing.
func signature(lines []string, j int) bool {
	if lines[j] != "-- \n" || j+1 == len(lines) || lines[j+1] == "\n" {
		return false
	}
	for _, l := range lines[j+1:] {
		if l != "\n" && !text(l) {
			return false
		}
	}
	return true
}

// text reports whether l can be text in no hunk: nothing git or go-gitdiff would take for part of
// a patch, or a reader for a hunk's line.
func text(l string) bool {
	return !hunkLine(l) && !strings.HasPrefix(l, "\\") && !strings.HasPrefix(l, "@@") && !strings.HasPrefix(l, "diff ")
}

// hunkEnd is where the hunk whose header says s, and whose lines start at lines[from], ends, and
// how many blank lines end it. Its lines are those that can be a hunk's, up to a hunk header, a
// file's header ("--- " and "+++ " lines before a hunk header are one: as a line the hunk removes
// and one it adds, they would end it with no context after its last change, which only a file's
// last hunk may), a line that cannot be a hunk's, or the end. "-- " before text to the end reads
// two ways, and the header's counts say which: git format-patch's signature, unless the counts are
// not used up before it; then it removes "- ". Blank lines at the end are the hunk's here; place
// decides.
func hunkEnd(lines []string, from int, s span) (end, blanks int) {
	var before, after int64 // the lines of the file the hunk's lines so far stand for
	j := from
	for ; j < len(lines) && hunkLine(lines[j]) && !startsFile(lines, j); j++ {
		if signature(lines, j) && before == s.oldLines && after == s.newLines {
			break
		}
		b, a := countLine(lines[j])
		before, after = before+b, after+a
	}
	for k := j; k > from && lines[k-1] == "\n"; k-- {
		blanks++
	}
	return j, blanks
}

// count reads the patch's lines and finds its hunks (see hunkEnd), with the lines of the file each
// holds. Before the first file, any text may come; in a file's header, before its first hunk, no
// line that reads as a hunk's (but its own "--- " and "+++ "). After a hunk, text in no hunk may
// come between files and after the last one, but no line that reads as a hunk's, nor a hunk header
// before the next file's header: those would read two ways. git's file headers and plain ones are
// not mixed: git apply drops the plain ones' first path component where agentd would not. The last
// line of the patch gets its newline back when it is a hunk's: git apply refuses a hunk's line
// without one, and go-gitdiff reads it as a file that ends without a newline.
func count(b []byte) ([]string, []hunk, error) {
	if len(b) == 0 {
		return nil, nil, nil
	}
	lines := splitLines(string(b))
	const (
		before  = iota // the first file's header has not come
		header         // a file's header, before its first hunk
		between        // after a hunk
	)
	state, inGit, sawGit, sawPlain, afterText := before, false, false, false, false
	created := false // the file whose header this is is new or deleted: no line of its hunks is on both sides
	var hunks []hunk
	for i := 0; i < len(lines); i++ {
		l := lines[i]
		s, rest, ok := parseHunkHeader(l)
		switch {
		case ok && afterText:
			return nil, nil, violation("line %d of the patch is a hunk header after text, with no file's header between", i+1)
		case ok:
			end, blanks := hunkEnd(lines, i+1, s)
			if created {
				end, blanks = end-blanks, 0 // not the file's empty lines: it has none before, or none after
			}
			h := hunk{line: i, header: strings.TrimSuffix(l, rest), was: s, now: s, blanks: blanks, rest: rest}
			h.now.oldLines, h.now.newLines = 0, 0
			for _, body := range lines[i+1 : end] {
				b, a := countLine(body)
				h.now.oldLines, h.now.newLines = h.now.oldLines+b, h.now.newLines+a
			}
			hunks = append(hunks, h)
			if end == len(lines) && end > i+1 && !strings.HasSuffix(lines[end-1], "\n") {
				lines[end-1] += "\n"
			}
			state, i = between, end-1
			continue
		case strings.HasPrefix(l, "@@ -"):
			return nil, nil, violation("line %d of the patch is not a hunk header: %.80q", i+1, l)
		case startsFile(lines, i):
			if strings.HasPrefix(l, "diff ") {
				inGit = strings.HasPrefix(l, "diff --git ")
				sawGit = sawGit || inGit
				created = false
			} else if state != header || !inGit {
				sawPlain = true
				created = false
			}
			state, afterText = header, false
			created = created || devNull(l)
			continue
		}
		if state == header && (strings.HasPrefix(l, "new file mode ") || strings.HasPrefix(l, "deleted file mode ") ||
			strings.HasPrefix(l, "+++ ") && devNull(l)) {
			created = true
		}
		switch {
		case state == header && (hunkLine(l) && l != "\n" || strings.HasPrefix(l, "\\")) &&
			!strings.HasPrefix(l, "--- ") && !strings.HasPrefix(l, "+++ "):
			return nil, nil, violation("line %d of the patch reads as a hunk's, but comes before the hunk's header: %.80q", i+1, l)
		case state != between || l == "\n":
		case signature(lines, i):
			i = len(lines)
		case !text(l):
			return nil, nil, violation("line %d of the patch reads as a hunk's, but is in no hunk: %.80q", i+1, l)
		default:
			afterText = true
		}
	}
	if sawGit && sawPlain {
		return nil, nil, violation("the patch mixes git's file headers with plain ones")
	}
	return lines, hunks, nil
}

// devNull reports whether a "--- " or "+++ " line names no file, as a new or a deleted file's does:
// as go-gitdiff reads it, /dev/null, or any name with the epoch for its time (GNU diff -N).
func devNull(l string) bool {
	if !strings.HasPrefix(l, "--- ") && !strings.HasPrefix(l, "+++ ") {
		return false
	}
	name, ts, timed := strings.Cut(strings.TrimSuffix(l[4:], "\n"), "\t")
	if strings.TrimSpace(name) == "/dev/null" {
		return true
	}
	if len(ts) >= 3 && ts[len(ts)-3] == ':' { // a zone written +00:00
		ts = ts[:len(ts)-3] + ts[len(ts)-2:]
	}
	t, err := time.Parse("2006-01-02 15:04:05.9 -0700", ts)
	return timed && err == nil && t.Equal(time.Unix(0, 0))
}

// render is the patch with the header of each hunk whose numbers changed rewritten: for go-gitdiff
// to read, which believes the counts.
func render(lines []string, hunks []hunk) []byte {
	var b strings.Builder
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
	return []byte(b.String())
}

// place puts each hunk of f where its lines are in orig, the file at the commit, and makes its
// header say so. A hunk's lines are its context and the lines it removes; it goes where its header
// puts it if they are there, else to the one place in the file where they are. They are nowhere,
// or in more than one other place, and the patch is refused; so is a hunk that holds none, but the
// one hunk that gives an empty or a new file its first lines: nothing says where it goes. As git
// apply reads a hunk with no context after its last change, it must end the file. The blank lines
// that end a hunk may be empty lines of the file, or set the hunk off from what follows: wherever
// the hunk's other lines are, it holds as many of them as the file has empty lines there. A file's
// hunks go in the file's order. A line without its newline, which go-gitdiff joins to the next one,
// must be the file's last.
func place(path string, f *gitdiff.File, orig []byte, hunks []hunk, searches *int) error {
	lines := splitLines(string(orig))
	var empties []int // empties[j]: how many empty lines there are from lines[j] on, once a hunk needs it
	for i, frag := range f.TextFragments {
		var old []string
		for _, l := range frag.Lines {
			if l.Old() {
				old = append(old, l.Line)
			}
		}
		if len(old) == 0 {
			if len(lines) > 0 || len(f.TextFragments) > 1 {
				return violation("%s: the patch does not apply: hunk %d holds no line of the file (no context, nothing it removes) to say where it goes", path, i+1)
			}
			frag.OldPosition = 0
			continue
		}
		blanks := hunks[i].blanks
		if blanks == len(old) {
			blanks = 0 // all the hunk holds of the file: they are its own
		}
		base, tail := old[:len(old)-blanks], frag.TrailingContext-int64(blanks)
		if blanks > 0 && empties == nil {
			empties = make([]int, len(lines)+1)
			for j := len(lines) - 1; j >= 0; j-- {
				if lines[j] == "\n" {
					empties[j] = empties[j+1] + 1
				}
			}
		}
		// keep is how many of the blank lines the hunk holds where its other lines start at pos: the
		// empty lines the file has after them. Looked up, not counted: find asks at every match.
		keep := func(pos int64) int {
			if blanks == 0 {
				return 0
			}
			return min(blanks, empties[pos-1+int64(len(base))])
		}
		// fits reports whether the hunk can go where its other lines start at pos (they are there).
		fits := func(pos int64) bool {
			return tail+int64(keep(pos)) > 0 || pos-1+int64(len(base)) == int64(len(lines))
		}
		search := func() error {
			if *searches++; *searches > MaxSearches {
				return violation("more than %d hunks of the patch would have to be looked for elsewhere in their files", MaxSearches)
			}
			return nil
		}
		// counted is how many of the blank lines the header's counts take for the hunk's own, when
		// they take some: counts are a model's guess, so they decide no place, but they can make one
		// read two ways.
		counted := int64(0)
		if d := hunks[i].was.oldLines - (frag.OldLines - int64(blanks)); blanks > 0 && d > 0 && d <= int64(blanks) &&
			hunks[i].was.newLines-(frag.NewLines-int64(blanks)) == d {
			counted = d
		}
		stated, pos := frag.OldPosition, frag.OldPosition
		switch {
		case at(lines, base, stated) && fits(stated):
			if k := int64(keep(stated)); counted > k {
				if err := search(); err != nil {
					return err
				}
				if other := find(lines, base, 1, func(p int64) bool { return p != stated && int64(keep(p)) >= counted }); len(other) > 0 {
					return violation("%s: the patch does not apply: hunk %d reads two ways: its header puts it at line %d, but its counts take "+
						"the blank lines it ends with for the file's own, which the file has after the hunk's other lines at line %d", path, i+1, stated, other[0])
				}
			}
		case at(lines, base, stated):
			return violation("%s: the patch does not apply: hunk %d has no context after its last change, so git apply puts it at the file's end, "+
				"and its header at line %d, where its lines are too", path, i+1, stated)
		default:
			if err := search(); err != nil {
				return err
			}
			found := find(lines, base, 2, fits)
			switch {
			case len(found) == 0 && tail == 0 && len(find(lines, base, 1, nil)) > 0:
				return violation("%s: the patch does not apply: hunk %d has no context after its last change, so it must end the file, as git apply reads it, and its lines are not at the file's end",
					path, i+1)
			case len(found) == 0:
				return violation("%s: the patch does not apply: the lines of hunk %d (its context and the lines it removes) are not in the file at the commit",
					path, i+1)
			case len(found) > 1:
				return violation("%s: the patch does not apply: the lines of hunk %d are at lines %d and %d of the file at the commit, not at line %d, where its header puts them",
					path, i+1, found[0], found[1], stated)
			}
			pos = found[0]
		}
		if drop := blanks - keep(pos); drop > 0 {
			n := int64(drop)
			frag.Lines = frag.Lines[:len(frag.Lines)-drop]
			frag.OldLines, frag.NewLines, frag.TrailingContext = frag.OldLines-n, frag.NewLines-n, frag.TrailingContext-n
		}
		frag.OldPosition = pos
	}
	var last int64 // the last line of the file the hunks so far hold
	for i, frag := range f.TextFragments {
		if frag.OldLines > 0 && frag.OldPosition <= last {
			return violation("%s: the patch does not apply: hunk %d is not after hunk %d in the file, and a file's hunks go in its order", path, i+1, i)
		}
		end := frag.OldPosition + frag.OldLines - 1
		if frag.OldLines > 0 {
			last = end
		}
		toEnd := end == int64(len(lines)) || frag.OldLines == 0 && len(lines) == 0
		for k, l := range frag.Lines {
			if l.New() && l.NoEOL() && (!toEnd || slices.ContainsFunc(frag.Lines[k+1:], gitdiff.Line.New)) {
				return violation("%s: the patch does not apply: hunk %d leaves a line without its newline that is not the file's last", path, i+1)
			}
		}
	}
	var shift int64 // what the hunks before add and remove
	for i, frag := range f.TextFragments {
		first := frag.OldPosition // the first line of the old side, or the line before none
		if frag.OldLines == 0 {
			first++
		}
		frag.NewPosition = first + shift
		if frag.NewLines == 0 {
			frag.NewPosition-- // the line before none
		}
		shift += frag.NewLines - frag.OldLines
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

// find is where old starts in lines (from 1) and ok holds (any, for nil), at most limit times, in
// the file's order. Knuth, Morris and Pratt's search: the lines of the file are compared no more
// than twice each, so that a file of one line many times over costs no more than any other.
func find(lines, old []string, limit int, ok func(pos int64) bool) []int64 {
	if len(old) == 0 {
		return nil
	}
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
			if pos := int64(i - len(old) + 2); ok == nil || ok(pos) {
				if found = append(found, pos); len(found) == limit {
					break
				}
			}
			k = next[k-1]
		}
	}
	return found
}

// clone is a copy of f whose hunks can be placed: f stays as parsed.
func clone(f *gitdiff.File) *gitdiff.File {
	g := *f
	g.TextFragments = make([]*gitdiff.TextFragment, len(f.TextFragments))
	for i, frag := range f.TextFragments {
		c := *frag
		c.Lines = slices.Clone(frag.Lines)
		g.TextFragments[i] = &c
	}
	return &g
}

// canonical leaves of f's header only what agentd holds to: its names, and whether the patch
// creates or deletes it (a file of mode 100644, or the patch is refused). No mode, blob or score a
// patch claims, and no heading after a hunk's "@@": the patch's own words, which nothing checks.
func canonical(f *gitdiff.File) {
	f.OldOIDPrefix, f.NewOIDPrefix, f.Score, f.OldMode, f.NewMode = "", "", 0, 0, 0
	if f.IsNew {
		f.NewMode = regularFile
	}
	if f.IsDelete {
		f.OldMode = regularFile
	}
	for _, frag := range f.TextFragments {
		frag.Comment = ""
	}
}

// format writes the files as a patch, as go-gitdiff writes one: their headers and their hunks.
func format(files []*gitdiff.File) []byte {
	var b strings.Builder
	for _, f := range files {
		b.WriteString(f.String())
	}
	return []byte(b.String())
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
