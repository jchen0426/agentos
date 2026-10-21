package patch

import (
	"fmt"
	"math/rand/v2"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// numbered is a file of the lines from..to, each its number.
func numbered(from, to int) string {
	var b strings.Builder
	for i := from; i <= to; i++ {
		fmt.Fprintf(&b, "%d\n", i)
	}
	return b.String()
}

// applied applies the patch to the files: the patch as Apply applied it, and what the files
// became.
func applied(t *testing.T, patch string, m map[string]string) (string, map[string]string, error) {
	t.Helper()
	p, err := Parse([]byte(patch))
	if err != nil {
		return "", nil, err
	}
	if err := p.Check(nil); err != nil {
		t.Fatal(err)
	}
	changes, err := p.Apply(files(m))
	if err != nil {
		return "", nil, err
	}
	out := map[string]string{}
	for _, c := range changes {
		out[c.Path] = string(c.Content)
	}
	return string(p.Text()), out, nil
}

// A hunk is its lines: they say how many lines of the file it holds, whatever its header says (a
// model miscounts them, and so does a person who edits a patch), up to the next hunk or file, or
// the end. Apply rewrites a header whose counts are wrong, and nothing else of the patch.
func TestAHunkIsItsLines(t *testing.T) {
	f := numbered(1, 20)
	seven := strings.Replace(f, "7\n", "seven\n", 1)
	for _, tc := range []struct {
		name, patch, want string // want: the patch as applied; empty when it is the patch itself
		files, after      map[string]string
	}{{
		name:  "the header counts too many lines",
		patch: "--- a/f.txt\n+++ b/f.txt\n@@ -4,9 +4,9 @@ func f() {\n 4\n 5\n 6\n-7\n+seven\n 8\n 9\n 10\n",
		want:  "--- a/f.txt\n+++ b/f.txt\n@@ -4,7 +4,7 @@ func f() {\n 4\n 5\n 6\n-7\n+seven\n 8\n 9\n 10\n",
		files: map[string]string{"f.txt": f}, after: map[string]string{"f.txt": seven},
	}, {
		name: "the header counts too few, and the next hunk follows its lines",
		patch: "--- a/f.txt\n+++ b/f.txt\n@@ -2,4 +2,4 @@\n 2\n 3\n 4\n-5\n+five\n 6\n 7\n 8\n" +
			"@@ -12,7 +12,7 @@\n 12\n 13\n 14\n-15\n+fifteen\n 16\n 17\n 18\n",
		want: "--- a/f.txt\n+++ b/f.txt\n@@ -2,7 +2,7 @@\n 2\n 3\n 4\n-5\n+five\n 6\n 7\n 8\n" +
			"@@ -12,7 +12,7 @@\n 12\n 13\n 14\n-15\n+fifteen\n 16\n 17\n 18\n",
		files: map[string]string{"f.txt": f},
		after: map[string]string{"f.txt": strings.NewReplacer("5\n", "five\n", "15\n", "fifteen\n").Replace(f)},
	}, {
		// git apply and go-gitdiff read only as many lines as the header counts, and leave the
		// rest out without a word: the reviewer would see lines the pull request does not have.
		name:  "the header counts too few at the end: the lines past them are the hunk's too",
		patch: "--- a/f.txt\n+++ b/f.txt\n@@ -18,3 +18,4 @@\n 18\n 19\n 20\n+21\n+22\n",
		want:  "--- a/f.txt\n+++ b/f.txt\n@@ -18,3 +18,5 @@\n 18\n 19\n 20\n+21\n+22\n",
		files: map[string]string{"f.txt": f}, after: map[string]string{"f.txt": numbered(1, 22)},
	}, {
		name: "the hunk ends where the next file starts",
		patch: "--- a/f.txt\n+++ b/f.txt\n@@ -1,5 +1,5 @@\n 1\n-2\n+two\n 3\n" +
			"--- a/g.txt\n+++ b/g.txt\n@@ -1,2 +1,2 @@\n-a\n+A\n b\n",
		want: "--- a/f.txt\n+++ b/f.txt\n@@ -1,3 +1,3 @@\n 1\n-2\n+two\n 3\n" +
			"--- a/g.txt\n+++ b/g.txt\n@@ -1,2 +1,2 @@\n-a\n+A\n b\n",
		files: map[string]string{"f.txt": f, "g.txt": "a\nb\n"},
		after: map[string]string{"f.txt": strings.Replace(f, "2\n", "two\n", 1), "g.txt": "A\nb\n"},
	}, {
		name: "blank lines between files and at the end are no lines of a hunk",
		patch: "diff --git a/f.txt b/f.txt\n--- a/f.txt\n+++ b/f.txt\n@@ -1,3 +1,3 @@\n 1\n-2\n+two\n 3\n\n" +
			"diff --git a/g.txt b/g.txt\n--- a/g.txt\n+++ b/g.txt\n@@ -1,2 +1,2 @@\n-a\n+A\n b\n\n\n",
		files: map[string]string{"f.txt": f, "g.txt": "a\nb\n"},
		after: map[string]string{"f.txt": strings.Replace(f, "2\n", "two\n", 1), "g.txt": "A\nb\n"},
	}, {
		// A blank line inside a hunk is an empty line of the file, as git and go-gitdiff take
		// it, and so is one just before the file's next hunk: blank lines between two hunks of a
		// file would make the second a hunk of no file.
		name:  "a blank line before the next hunk is an empty line of the file",
		patch: "--- a/e.txt\n+++ b/e.txt\n@@ -1,3 +1,3 @@\n a\n-b\n+B\n c\n\n@@ -8,3 +8,3 @@\n g\n-h\n+H\n i\n",
		want:  "--- a/e.txt\n+++ b/e.txt\n@@ -1,4 +1,4 @@\n a\n-b\n+B\n c\n\n@@ -8,3 +8,3 @@\n g\n-h\n+H\n i\n",
		files: map[string]string{"e.txt": "a\nb\nc\n\nd\ne\nf\ng\nh\ni\n"},
		after: map[string]string{"e.txt": "a\nB\nc\n\nd\ne\nf\ng\nH\ni\n"},
	}, {
		name:  "a patch whose headers are right is left as it is, short headers and all",
		patch: "--- a/f.txt\n+++ b/f.txt\n@@ -7 +7 @@ the seventh\n-7\n+seven\n",
		files: map[string]string{"f.txt": f}, after: map[string]string{"f.txt": seven},
	}, {
		name: "git format-patch's signature is no line of a hunk",
		patch: "From 0123456789abcdef0123456789abcdef01234567 Mon Sep 17 00:00:00 2001\nFrom: A <a@example.com>\n" +
			"Subject: [PATCH] seven\n\n---\n f.txt | 2 +-\n 1 file changed, 1 insertion(+), 1 deletion(-)\n\n" +
			"diff --git a/f.txt b/f.txt\nindex 1111111..2222222 100644\n--- a/f.txt\n+++ b/f.txt\n" +
			"@@ -6,3 +6,3 @@\n 6\n-7\n+seven\n 8\n-- \n2.43.0\n\n",
		files: map[string]string{"f.txt": f}, after: map[string]string{"f.txt": seven},
	}, {
		name:  "a text after the last hunk changes nothing",
		patch: "--- a/f.txt\n+++ b/f.txt\n@@ -6,3 +6,3 @@\n 6\n-7\n+seven\n 8\n\nThis fixes the seventh line.\n",
		files: map[string]string{"f.txt": f}, after: map[string]string{"f.txt": seven},
	}, {
		// git apply refuses a hunk's line without its newline; go-gitdiff reads one that adds
		// a line as a file that ends without a newline.
		name:  "the last line of the patch gets its newline",
		patch: "--- a/f.txt\n+++ b/f.txt\n@@ -18,3 +18,4 @@\n 18\n 19\n 20\n+21",
		want:  "--- a/f.txt\n+++ b/f.txt\n@@ -18,3 +18,4 @@\n 18\n 19\n 20\n+21\n",
		files: map[string]string{"f.txt": f}, after: map[string]string{"f.txt": numbered(1, 21)},
	}, {
		name: "a new file and a deleted one keep their headers",
		patch: "diff --git a/gone.txt b/gone.txt\ndeleted file mode 100644\n--- a/gone.txt\n+++ /dev/null\n@@ -1,2 +0,0 @@\n-x\n-y\n" +
			"diff --git a/new.txt b/new.txt\nnew file mode 100644\n--- /dev/null\n+++ b/new.txt\n@@ -0,0 +1 @@\n+x\n",
		files: map[string]string{"gone.txt": "x\ny\n"}, after: map[string]string{"gone.txt": "", "new.txt": "x\n"},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			text, after, err := applied(t, tc.patch, tc.files)
			if err != nil {
				t.Fatal(err)
			}
			want := tc.want
			if want == "" {
				want = tc.patch
			}
			if text != want {
				t.Errorf("the patch as applied:\n%s\nwant:\n%s", text, want)
			}
			for path, content := range tc.after {
				if after[path] != content {
					t.Errorf("%s after the patch:\n%q\nwant\n%q", path, after[path], content)
				}
			}
		})
	}
}

// A hunk goes where its header puts it when its lines (its context and what it removes) are
// there; otherwise to the one place in the file where they are. Nowhere else: lines that are
// nowhere, or in more than one other place, refuse the patch. Each line must match exactly.
func TestAHunkGoesWhereItsLinesAre(t *testing.T) {
	f := numbered(1, 40)
	twice := "x\ny\nz\n" + numbered(1, 5) + "x\ny\nz\n" + numbered(6, 10) // x, y, z at lines 1 and 9
	for _, tc := range []struct {
		name, patch, want string
		files, after      map[string]string
	}{{
		name:  "its lines are elsewhere",
		patch: "--- a/f.txt\n+++ b/f.txt\n@@ -12,7 +12,7 @@\n 22\n 23\n 24\n-25\n+twenty-five\n 26\n 27\n 28\n",
		want:  "--- a/f.txt\n+++ b/f.txt\n@@ -22,7 +22,7 @@\n 22\n 23\n 24\n-25\n+twenty-five\n 26\n 27\n 28\n",
		files: map[string]string{"f.txt": f}, after: map[string]string{"f.txt": strings.Replace(f, "25\n", "twenty-five\n", 1)},
	}, {
		// Where the new side starts follows from where the old one does, and from what the
		// hunks before it in the file add and remove.
		name: "its lines are elsewhere, after a hunk that adds a line",
		patch: "--- a/f.txt\n+++ b/f.txt\n@@ -1,3 +1,4 @@\n 1\n+one and a half\n 2\n 3\n" +
			"@@ -30,7 +31,7 @@\n 22\n 23\n 24\n-25\n+twenty-five\n 26\n 27\n 28\n",
		want: "--- a/f.txt\n+++ b/f.txt\n@@ -1,3 +1,4 @@\n 1\n+one and a half\n 2\n 3\n" +
			"@@ -22,7 +23,7 @@\n 22\n 23\n 24\n-25\n+twenty-five\n 26\n 27\n 28\n",
		files: map[string]string{"f.txt": f},
		after: map[string]string{"f.txt": "1\none and a half\n" + numbered(2, 24) + "twenty-five\n" + numbered(26, 40)},
	}, {
		name:  "its lines are where its header puts them, and elsewhere too",
		patch: "--- a/t.txt\n+++ b/t.txt\n@@ -9,3 +9,3 @@\n x\n-y\n+Y\n z\n",
		files: map[string]string{"t.txt": twice}, after: map[string]string{"t.txt": "x\ny\nz\n" + numbered(1, 5) + "x\nY\nz\n" + numbered(6, 10)},
	}, {
		// Only the new side's start is wrong: the old side says where the hunk goes.
		name:  "the new side starts elsewhere",
		patch: "--- a/f.txt\n+++ b/f.txt\n@@ -22,7 +29,7 @@\n 22\n 23\n 24\n-25\n+twenty-five\n 26\n 27\n 28\n",
		want:  "--- a/f.txt\n+++ b/f.txt\n@@ -22,7 +22,7 @@\n 22\n 23\n 24\n-25\n+twenty-five\n 26\n 27\n 28\n",
		files: map[string]string{"f.txt": f}, after: map[string]string{"f.txt": strings.Replace(f, "25\n", "twenty-five\n", 1)},
	}, {
		name:  "an empty file gets its first lines",
		patch: "--- a/e.txt\n+++ b/e.txt\n@@ -1,0 +1,2 @@\n+a\n+b\n",
		want:  "--- a/e.txt\n+++ b/e.txt\n@@ -0,0 +1,2 @@\n+a\n+b\n",
		files: map[string]string{"e.txt": ""}, after: map[string]string{"e.txt": "a\nb\n"},
	}, {
		name: "the file's last line has no newline",
		patch: "--- a/n.txt\n+++ b/n.txt\n@@ -4,2 +4,2 @@\n a\n-b\n\\ No newline at end of file\n+B\n" +
			"\\ No newline at end of file\n",
		want: "--- a/n.txt\n+++ b/n.txt\n@@ -1,2 +1,2 @@\n a\n-b\n\\ No newline at end of file\n+B\n" +
			"\\ No newline at end of file\n",
		files: map[string]string{"n.txt": "a\nb"}, after: map[string]string{"n.txt": "a\nB"},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			text, after, err := applied(t, tc.patch, tc.files)
			if err != nil {
				t.Fatal(err)
			}
			want := tc.want
			if want == "" {
				want = tc.patch
			}
			if text != want {
				t.Errorf("the patch as applied:\n%s\nwant:\n%s", text, want)
			}
			for path, content := range tc.after {
				if after[path] != content {
					t.Errorf("%s after the patch:\n%q\nwant\n%q", path, after[path], content)
				}
			}
		})
	}

	for _, tc := range []struct{ name, patch, want string }{{
		name:  "its lines are nowhere",
		patch: "--- a/f.txt\n+++ b/f.txt\n@@ -3,3 +3,3 @@\n 3\n-4x\n+four\n 5\n",
		want:  "f.txt: the patch does not apply: the lines of hunk 1 (its context and the lines it removes) are not in the file at the commit",
	}, {
		name:  "a context line differs by its spaces",
		patch: "--- a/f.txt\n+++ b/f.txt\n@@ -3,3 +3,3 @@\n 3 \n-4\n+four\n 5\n",
		want:  "f.txt: the patch does not apply: the lines of hunk 1",
	}, {
		name:  "its lines are in two other places",
		patch: "--- a/t.txt\n+++ b/t.txt\n@@ -5,3 +5,3 @@\n x\n-y\n+Y\n z\n",
		want:  "t.txt: the patch does not apply: the lines of hunk 1 are at lines 1 and 9 of the file at the commit, not at line 5, where its header puts them",
	}, {
		// go-gitdiff puts the line before line 5, git apply at the end of the file.
		name:  "it holds no line of the file",
		patch: "--- a/f.txt\n+++ b/f.txt\n@@ -5,0 +6,1 @@\n+new\n",
		want:  "f.txt: the patch does not apply: hunk 1 holds no line of the file (no context, nothing it removes) to say where it goes",
	}, {
		name:  "two hunks are the same lines",
		patch: "--- a/f.txt\n+++ b/f.txt\n@@ -2,3 +2,3 @@\n 2\n-3\n+three\n 4\n@@ -9,3 +9,3 @@\n 2\n-3\n+three\n 4\n",
		want:  "the patch does not apply",
	}} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := applied(t, tc.patch, map[string]string{"f.txt": f, "t.txt": twice})
			refused(t, err, tc.want)
		})
	}
}

// Looking for a hunk's lines costs a pass over the file: a patch may have only so many hunks
// that are not where their headers put them.
func TestOnlySoManyHunksAreLookedFor(t *testing.T) {
	f := numbered(1, 1000)
	patch := func(n int) string {
		var b strings.Builder
		b.WriteString("--- a/f.txt\n+++ b/f.txt\n")
		for i := range n {
			at := 10 + 20*i
			fmt.Fprintf(&b, "@@ -%d,3 +%d,3 @@\n %d\n-%d\n+changed\n %d\n", at+5, at+5, at, at+1, at+2)
		}
		return b.String()
	}
	if _, _, err := applied(t, patch(MaxMoved), map[string]string{"f.txt": f}); err != nil {
		t.Fatalf("%d hunks out of place: %v", MaxMoved, err)
	}
	_, _, err := applied(t, patch(MaxMoved+1), map[string]string{"f.txt": f})
	refused(t, err, fmt.Sprintf("more than %d hunks of the patch are not where their headers put them", MaxMoved))
}

// A line after a hunk that is in no hunk and starts no file could be either: the patch reads two
// ways, and is refused. Text after the last hunk is not: it applies to nothing.
func TestALineInNoHunkIsRefused(t *testing.T) {
	hunk := "--- a/f.txt\n+++ b/f.txt\n@@ -6,3 +6,3 @@\n 6\n-7\n+seven\n 8\n"
	for _, tc := range []struct{ name, patch, want string }{
		{"a hunk's line after a text", hunk + "Note:\n+more\n", `line 8 of the patch is in no hunk and starts no file: "Note:\n"`},
		{"a text between two hunks", hunk + "and then\n@@ -12,3 +12,3 @@\n 12\n-13\n+thirteen\n 14\n", "line 8 of the patch is in no hunk"},
		{"a text before the next file", hunk + "Index: g.txt\n--- a/g.txt\n+++ b/g.txt\n@@ -1 +1 @@\n-a\n+A\n", "line 8 of the patch is in no hunk"},
		{"a header agentd cannot read", hunk + "@@ -12,3 +12 3 @@\n 12\n", `line 8 of the patch is not a hunk header: "@@ -12,3 +12 3 @@\n"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse([]byte(tc.patch))
			refused(t, err, tc.want)
		})
	}
}

// Differential: git's own patches, their hunk headers scrambled, come out as git wrote them, and
// git apply takes them as they are (no --recount).
func TestScrambledHeadersComeBackAsGitWroteThem(t *testing.T) {
	// Nothing from the user's git configuration (a diff driver, colours) changes what git writes.
	t.Setenv("GIT_CONFIG_GLOBAL", os.DevNull)
	t.Setenv("GIT_CONFIG_NOSYSTEM", "1")
	r := newMirror(t)
	rng := rand.New(rand.NewPCG(7, 11))
	header := regexp.MustCompile(`(?m)^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@`)
	unique := 0
	line := func() string { unique++; return fmt.Sprintf("line %d of %d\n", unique, rng.IntN(1000)) }
	for round := range 60 {
		origs, edits := map[string]string{}, map[string]string{}
		for _, name := range []string{"a.txt", "dir/b.txt"} {
			var orig, edit strings.Builder
			for range 20 + rng.IntN(60) {
				l := line()
				orig.WriteString(l)
				switch rng.IntN(12) {
				case 0: // removed
				case 1: // changed
					edit.WriteString(line())
				case 2: // added after it
					edit.WriteString(l)
					edit.WriteString(line())
				default:
					edit.WriteString(l)
				}
			}
			o, e := orig.String(), edit.String()
			if rng.IntN(4) == 0 { // the last line without its newline, before or after
				if rng.IntN(2) == 0 {
					o = strings.TrimSuffix(o, "\n")
				} else {
					e = strings.TrimSuffix(e, "\n")
				}
			}
			origs[name], edits[name] = o, e
		}
		for name, body := range origs {
			r.write(name, []byte(body), 0o644)
		}
		r.git("add", "-A")
		r.git("commit", "-q", "--allow-empty", "-m", strconv.Itoa(round))
		for name, body := range edits {
			r.write(name, []byte(body), 0o644)
		}
		gits := r.git("diff") + "\n" // the trailing newline TrimSpace took
		if strings.TrimSpace(gits) == "" {
			continue
		}
		// As git writes it, and as a model would: without git's own lines.
		plain := regexp.MustCompile(`(?m)^(diff --git|index) .*\n`).ReplaceAllString(gits, "")
		for _, patch := range []string{gits, plain} {
			scrambled := header.ReplaceAllStringFunc(patch, func(h string) string {
				m := header.FindStringSubmatch(h)
				n := func(s string, spread int) int {
					v, _ := strconv.Atoi(s)
					if s == "" {
						v = 1
					}
					return max(0, v+rng.IntN(2*spread+1)-spread)
				}
				return fmt.Sprintf("@@ -%d,%d +%d,%d @@", n(m[1], 4), n(m[2], 3), n(m[3], 4), n(m[4], 3))
			})
			text, after, err := applied(t, scrambled, origs)
			if err != nil {
				t.Fatalf("round %d: %v\n%s", round, err, scrambled)
			}
			if text != patch {
				t.Fatalf("round %d: the patch as applied:\n%s\nwant git's:\n%s", round, text, patch)
			}
			for name, body := range edits {
				if body != origs[name] && after[name] != body {
					t.Fatalf("round %d: %s after the patch:\n%q\nwant\n%q", round, name, after[name], body)
				}
			}
			r.gitIn(text, "apply", "--cached", "--check", "-")
		}
	}
}

// Parse reads a patch whose hunk headers are wrong; the patch it reads is one whose headers are
// right, which go-gitdiff reads the same way.
func TestParseCountsTheLines(t *testing.T) {
	p, err := Parse([]byte("--- a/f.txt\n+++ b/f.txt\n@@ -4,9 +4,2 @@\n 4\n-5\n+five\n 6\n"))
	if err != nil {
		t.Fatal(err)
	}
	if got := string(p.Text()); got != "--- a/f.txt\n+++ b/f.txt\n@@ -4,3 +4,3 @@\n 4\n-5\n+five\n 6\n" {
		t.Fatalf("parsed as\n%s", got)
	}
}

// Apply tells which hunk headers it rewrote, and how.
func TestApplySaysWhatItRewrote(t *testing.T) {
	p, err := Parse([]byte("--- a/f.txt\n+++ b/f.txt\n@@ -2,3 +2,3 @@\n 2\n-3\n+three\n 4\n" +
		"@@ -30,9 +30,9 @@ the twenties\n 22\n 23\n 24\n-25\n+twenty-five\n 26\n 27\n 28\n"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Apply(files(map[string]string{"f.txt": numbered(1, 40)})); err != nil {
		t.Fatal(err)
	}
	got := p.Rewritten()
	if len(got) != 1 || got[0] != (Rewrite{Path: "f.txt", Hunk: 2, Was: "@@ -30,9 +30,9 @@", Now: "@@ -22,7 +22,7 @@"}) {
		t.Fatalf("rewritten: %+v", got)
	}
	// Applied again, to other files, it starts from the patch's own headers.
	if _, err := p.Apply(files(map[string]string{"f.txt": numbered(1, 40)})); err != nil || len(p.Rewritten()) != 1 {
		t.Fatalf("applied again: %v, %+v", err, p.Rewritten())
	}
}

// The patch as counted stays within the limit the git tool server takes: a short header can
// get longer.
func TestThePatchAsCountedIsWithinTheLimit(t *testing.T) {
	const h = "@@ -1 +1 @@\n 1\n-2\n+two\n" // "@@ -1,2 +1,2 @@" once counted
	var b strings.Builder
	b.WriteString("--- a/f.txt\n+++ b/f.txt\n")
	for b.Len()+len(h) <= MaxBytes {
		b.WriteString(h)
	}
	_, err := Parse([]byte(b.String()))
	refused(t, err, "bytes once its hunk headers are rewritten, over the limit of")
}
