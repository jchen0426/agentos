package patch

import (
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
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

// diff is how Text writes a file's header, git's way, for a file that is neither new nor deleted.
func diff(name string) string {
	return "diff --git a/" + name + " b/" + name + "\n--- a/" + name + "\n+++ b/" + name + "\n"
}

// applied applies the patch to the files: the patch as Apply applied it, and what the files
// became. git apply, given the patch as applied, must make the same files.
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
	after := map[string]string{}
	for path, content := range m {
		after[path] = content
	}
	for _, c := range changes {
		if c.Delete {
			delete(after, c.Path)
		} else {
			after[c.Path] = string(c.Content)
		}
	}
	if got := gitApplies(t, string(p.Text()), m); !equal(got, after) {
		t.Fatalf("git apply makes another change of the patch as applied:\ngit:    %q\nagentd: %q\n%s", got, after, p.Text())
	}
	return string(p.Text()), after, nil
}

func equal(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if w, ok := b[k]; !ok || w != v {
			return false
		}
	}
	return true
}

// gitApplies is what git apply (strict: no --recount, no fuzz) makes of the files with the patch,
// in a directory of their own outside any repository: every file, the patch's and the others. A
// patch it refuses fails the test.
func gitApplies(t *testing.T, patch string, m map[string]string) map[string]string {
	t.Helper()
	dir := t.TempDir()
	for name, body := range m {
		if err := os.MkdirAll(filepath.Dir(filepath.Join(dir, name)), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	cmd := exec.Command("git", "-C", dir, "apply", "-")
	cmd.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL="+os.DevNull, "GIT_CONFIG_NOSYSTEM=1", "GIT_CEILING_DIRECTORIES="+filepath.Dir(dir))
	cmd.Stdin = strings.NewReader(patch)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git apply: %v\n%s\n%s", err, out, patch)
	}
	out := map[string]string{}
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		b, err := os.ReadFile(path)
		rel, _ := filepath.Rel(dir, path)
		out[filepath.ToSlash(rel)] = string(b)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// gitRaw is git's output as it is.
func (r *mirrorRepo) gitRaw(args ...string) string {
	r.t.Helper()
	out, err := exec.Command("git", append([]string{"-C", r.dir}, args...)...).Output()
	if err != nil {
		r.t.Fatalf("git %v: %v", args, err)
	}
	return string(out)
}

// A hunk is its lines: they say how many lines of the file it holds, whatever its header says (a
// model miscounts them, and so does a person who edits a patch), up to the next hunk or file, or
// the end. The patch as applied is written anew, git's way: file headers and hunks, nothing else.
func TestAHunkIsItsLines(t *testing.T) {
	f := numbered(1, 20)
	seven := strings.Replace(f, "7\n", "seven\n", 1)
	for _, tc := range []struct {
		name, patch, want string // want: the patch as applied
		files, after      map[string]string
	}{{
		name:  "the header counts too many lines",
		patch: "--- a/f.txt\n+++ b/f.txt\n@@ -4,9 +4,9 @@ func f() {\n 4\n 5\n 6\n-7\n+seven\n 8\n 9\n 10\n",
		want:  diff("f.txt") + "@@ -4,7 +4,7 @@ func f() {\n 4\n 5\n 6\n-7\n+seven\n 8\n 9\n 10\n",
		files: map[string]string{"f.txt": f}, after: map[string]string{"f.txt": seven},
	}, {
		name: "the header counts too few, and the next hunk follows its lines",
		patch: "--- a/f.txt\n+++ b/f.txt\n@@ -2,4 +2,4 @@\n 2\n 3\n 4\n-5\n+five\n 6\n 7\n 8\n" +
			"@@ -12,7 +12,7 @@\n 12\n 13\n 14\n-15\n+fifteen\n 16\n 17\n 18\n",
		want: diff("f.txt") + "@@ -2,7 +2,7 @@\n 2\n 3\n 4\n-5\n+five\n 6\n 7\n 8\n" +
			"@@ -12,7 +12,7 @@\n 12\n 13\n 14\n-15\n+fifteen\n 16\n 17\n 18\n",
		files: map[string]string{"f.txt": f},
		after: map[string]string{"f.txt": strings.NewReplacer("5\n", "five\n", "15\n", "fifteen\n").Replace(f)},
	}, {
		// git apply and go-gitdiff read only as many lines as the header counts, and leave the
		// rest out without a word: the reviewer would see lines the pull request does not have.
		name:  "the header counts too few at the end: the lines past them are the hunk's too",
		patch: "--- a/f.txt\n+++ b/f.txt\n@@ -18,3 +18,4 @@\n 18\n 19\n 20\n+21\n+22\n",
		want:  diff("f.txt") + "@@ -18,3 +18,5 @@\n 18\n 19\n 20\n+21\n+22\n",
		files: map[string]string{"f.txt": f}, after: map[string]string{"f.txt": numbered(1, 22)},
	}, {
		name: "the hunk ends where the next file starts",
		patch: "--- a/f.txt\n+++ b/f.txt\n@@ -1,5 +1,5 @@\n 1\n-2\n+two\n 3\n" +
			"--- a/g.txt\n+++ b/g.txt\n@@ -1,2 +1,2 @@\n-a\n+A\n b\n",
		want:  diff("f.txt") + "@@ -1,3 +1,3 @@\n 1\n-2\n+two\n 3\n" + diff("g.txt") + "@@ -1,2 +1,2 @@\n-a\n+A\n b\n",
		files: map[string]string{"f.txt": f, "g.txt": "a\nb\n"},
		after: map[string]string{"f.txt": strings.Replace(f, "2\n", "two\n", 1), "g.txt": "A\nb\n"},
	}, {
		name: "blank lines between files and at the end are no lines of a hunk",
		patch: "diff --git a/f.txt b/f.txt\n--- a/f.txt\n+++ b/f.txt\n@@ -1,3 +1,3 @@\n 1\n-2\n+two\n 3\n\n" +
			"diff --git a/g.txt b/g.txt\n--- a/g.txt\n+++ b/g.txt\n@@ -1,2 +1,2 @@\n-a\n+A\n b\n\n\n",
		want:  diff("f.txt") + "@@ -1,3 +1,3 @@\n 1\n-2\n+two\n 3\n" + diff("g.txt") + "@@ -1,2 +1,2 @@\n-a\n+A\n b\n",
		files: map[string]string{"f.txt": f, "g.txt": "a\nb\n"},
		after: map[string]string{"f.txt": strings.Replace(f, "2\n", "two\n", 1), "g.txt": "A\nb\n"},
	}, {
		// A blank line inside a hunk is an empty line of the file, as git and go-gitdiff take
		// it. One at a hunk's end may be too, or set it off from what follows: the file decides.
		name:  "a blank line at a hunk's end is the file's empty line, where the file has one",
		patch: "--- a/e.txt\n+++ b/e.txt\n@@ -1,3 +1,3 @@\n a\n-b\n+B\n c\n\n@@ -8,3 +8,3 @@\n g\n-h\n+H\n i\n",
		want:  diff("e.txt") + "@@ -1,4 +1,4 @@\n a\n-b\n+B\n c\n \n@@ -8,3 +8,3 @@\n g\n-h\n+H\n i\n",
		files: map[string]string{"e.txt": "a\nb\nc\n\nd\ne\nf\ng\nh\ni\n"},
		after: map[string]string{"e.txt": "a\nB\nc\n\nd\ne\nf\ng\nH\ni\n"},
	}, {
		// Taken as an empty line of the file, the hunk's lines are at line 7 only; as what sets
		// two hunks apart, at line 1, where its header puts it.
		name:  "a blank line between two hunks sets them apart, where the file has none",
		patch: "--- a/s.txt\n+++ b/s.txt\n@@ -1,3 +1,3 @@\n a\n-b\n+B\n c\n\n@@ -12,3 +12,3 @@\n r\n-s\n+S\n t\n",
		want:  diff("s.txt") + "@@ -1,3 +1,3 @@\n a\n-b\n+B\n c\n@@ -12,3 +12,3 @@\n r\n-s\n+S\n t\n",
		files: map[string]string{"s.txt": "a\nb\nc\nx\ny\nz\na\nb\nc\n\nq\nr\ns\nt\n"},
		after: map[string]string{"s.txt": "a\nB\nc\nx\ny\nz\na\nb\nc\n\nq\nr\nS\nt\n"},
	}, {
		// The same lines are at the file's end too: without the blank line, which a hunk with no
		// context after its last change must be, git apply would change the second "def a".
		name:  "an empty line of the file as the hunk's last context line, written bare",
		patch: "--- a/m.py\n+++ b/m.py\n@@ -2,3 +2,3 @@\n def a():\n-    return 1\n+    return 2\n\n",
		want:  diff("m.py") + "@@ -2,3 +2,3 @@\n def a():\n-    return 1\n+    return 2\n \n",
		files: map[string]string{"m.py": "# top\ndef a():\n    return 1\n\ndef a():\n    return 1\n"},
		after: map[string]string{"m.py": "# top\ndef a():\n    return 2\n\ndef a():\n    return 1\n"},
	}, {
		name:  "two empty lines of the file, written bare",
		patch: "--- a/m.py\n+++ b/m.py\n@@ -1,4 +1,4 @@\n def f():\n-    return 1\n+    return 3\n\n\n",
		want:  diff("m.py") + "@@ -1,4 +1,4 @@\n def f():\n-    return 1\n+    return 3\n \n \n",
		files: map[string]string{"m.py": "def f():\n    return 1\n\n\ndef g():\n    return 2\n"},
		after: map[string]string{"m.py": "def f():\n    return 3\n\n\ndef g():\n    return 2\n"},
	}, {
		name:  "a patch whose headers are right is left as it is",
		patch: fix,
		want:  fix,
		files: map[string]string{"app/db.py": vulnerable},
		after: map[string]string{"app/db.py": strings.Replace(vulnerable, `'%s'" % name)`, `?", (name,))`, 1)},
	}, {
		name:  "a short header, of a file of one line",
		patch: "--- a/one.txt\n+++ b/one.txt\n@@ -1 +1 @@\n-a\n+b\n",
		want:  diff("one.txt") + "@@ -1,1 +1,1 @@\n-a\n+b\n",
		files: map[string]string{"one.txt": "a\n"}, after: map[string]string{"one.txt": "b\n"},
	}, {
		name: "git format-patch's mail and signature are left out",
		patch: "From 0123456789abcdef0123456789abcdef01234567 Mon Sep 17 00:00:00 2001\nFrom: A <a@example.com>\n" +
			"Subject: [PATCH] seven\n\n---\n f.txt | 2 +-\n 1 file changed, 1 insertion(+), 1 deletion(-)\n\n" +
			"diff --git a/f.txt b/f.txt\nindex 1111111..2222222 100644\n--- a/f.txt\n+++ b/f.txt\n" +
			"@@ -6,3 +6,3 @@\n 6\n-7\n+seven\n 8\n-- \n2.43.0\n\n",
		want:  "diff --git a/f.txt b/f.txt\nindex 1111111..2222222 100644\n--- a/f.txt\n+++ b/f.txt\n@@ -6,3 +6,3 @@\n 6\n-7\n+seven\n 8\n",
		files: map[string]string{"f.txt": f}, after: map[string]string{"f.txt": seven},
	}, {
		name:  "text after the last hunk is left out",
		patch: "--- a/f.txt\n+++ b/f.txt\n@@ -6,3 +6,3 @@\n 6\n-7\n+seven\n 8\n\nThis fixes the seventh line.\n",
		want:  diff("f.txt") + "@@ -6,3 +6,3 @@\n 6\n-7\n+seven\n 8\n",
		files: map[string]string{"f.txt": f}, after: map[string]string{"f.txt": seven},
	}, {
		// The counts say "-- " is the hunk's: it removes "- ", the file's last line. Text after it
		// applies to nothing.
		name:  "a line that removes \"- \", then a note",
		patch: "--- a/y.yml\n+++ b/y.yml\n@@ -1,3 +1,2 @@\n items:\n-- a\n+- b\n-- \nThe empty item goes too.\n",
		want:  diff("y.yml") + "@@ -1,3 +1,2 @@\n items:\n-- a\n+- b\n-- \n",
		files: map[string]string{"y.yml": "items:\n- a\n- \n"}, after: map[string]string{"y.yml": "items:\n- b\n"},
	}, {
		// git apply refuses a hunk's line without its newline; go-gitdiff reads one that adds
		// a line as a file that ends without a newline.
		name:  "the last line of the patch gets its newline",
		patch: "--- a/f.txt\n+++ b/f.txt\n@@ -18,3 +18,4 @@\n 18\n 19\n 20\n+21",
		want:  diff("f.txt") + "@@ -18,3 +18,4 @@\n 18\n 19\n 20\n+21\n",
		files: map[string]string{"f.txt": f}, after: map[string]string{"f.txt": numbered(1, 21)},
	}, {
		name: "a new file and a deleted one",
		patch: "diff --git a/gone.txt b/gone.txt\ndeleted file mode 100644\n--- a/gone.txt\n+++ /dev/null\n@@ -1,2 +0,0 @@\n-x\n-y\n" +
			"diff --git a/new.txt b/new.txt\nnew file mode 100644\n--- /dev/null\n+++ b/new.txt\n@@ -0,0 +1 @@\n+x\n",
		want: "diff --git a/gone.txt b/gone.txt\ndeleted file mode 100644\n--- a/gone.txt\n+++ /dev/null\n@@ -1,2 +0,0 @@\n-x\n-y\n" +
			"diff --git a/new.txt b/new.txt\nnew file mode 100644\n--- /dev/null\n+++ b/new.txt\n@@ -0,0 +1,1 @@\n+x\n",
		files: map[string]string{"gone.txt": "x\ny\n"}, after: map[string]string{"new.txt": "x\n"},
	}, {
		name: "svn's headers, and text between files",
		patch: "Index: f.txt\n===================================================================\n--- a/f.txt\t(revision 1)\n" +
			"+++ b/f.txt\t(working copy)\n@@ -1,3 +1,3 @@\n 1\n-2\n+two\n 3\nOnly in b: h\n" +
			"Index: g.txt\n===================================================================\n--- a/g.txt\t(revision 1)\n" +
			"+++ b/g.txt\t(working copy)\n@@ -1,2 +1,2 @@\n-a\n+A\n b\n",
		want:  diff("f.txt") + "@@ -1,3 +1,3 @@\n 1\n-2\n+two\n 3\n" + diff("g.txt") + "@@ -1,2 +1,2 @@\n-a\n+A\n b\n",
		files: map[string]string{"f.txt": "1\n2\n3\n", "g.txt": "a\nb\n"},
		after: map[string]string{"f.txt": "1\ntwo\n3\n", "g.txt": "A\nb\n"},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			text, after, err := applied(t, tc.patch, tc.files)
			if err != nil {
				t.Fatal(err)
			}
			if text != tc.want {
				t.Errorf("the patch as applied:\n%s\nwant:\n%s", text, tc.want)
			}
			if !equal(after, tc.after) {
				t.Errorf("after the patch:\n%q\nwant\n%q", after, tc.after)
			}
		})
	}
}

// A hunk goes where its header puts it when its lines (its context and what it removes) are
// there; otherwise to the one place in the file where they are. Nowhere else: lines that are
// nowhere, or in more than one other place, refuse the patch. Each line must match exactly. As
// git apply reads them, a hunk with no context after its last change ends the file, and a file's
// hunks are in its order.
func TestAHunkGoesWhereItsLinesAre(t *testing.T) {
	f := numbered(1, 40)
	twice := "x\ny\nz\n" + numbered(1, 5) + "x\ny\nz\n" + numbered(6, 10) // x, y, z at lines 1 and 9
	for _, tc := range []struct {
		name, patch, want string
		files, after      map[string]string
	}{{
		// The heading after "@@" was the model's, for a place the hunk is not at.
		name:  "its lines are elsewhere",
		patch: "--- a/f.txt\n+++ b/f.txt\n@@ -12,7 +12,7 @@ the teens\n 22\n 23\n 24\n-25\n+twenty-five\n 26\n 27\n 28\n",
		want:  diff("f.txt") + "@@ -22,7 +22,7 @@\n 22\n 23\n 24\n-25\n+twenty-five\n 26\n 27\n 28\n",
		files: map[string]string{"f.txt": f}, after: map[string]string{"f.txt": strings.Replace(f, "25\n", "twenty-five\n", 1)},
	}, {
		// Where the new side starts follows from where the old one does, and from what the
		// hunks before it in the file add and remove.
		name: "its lines are elsewhere, after a hunk that adds a line",
		patch: "--- a/f.txt\n+++ b/f.txt\n@@ -1,3 +1,4 @@\n 1\n+one and a half\n 2\n 3\n" +
			"@@ -30,7 +31,7 @@\n 22\n 23\n 24\n-25\n+twenty-five\n 26\n 27\n 28\n",
		want: diff("f.txt") + "@@ -1,3 +1,4 @@\n 1\n+one and a half\n 2\n 3\n" +
			"@@ -22,7 +23,7 @@\n 22\n 23\n 24\n-25\n+twenty-five\n 26\n 27\n 28\n",
		files: map[string]string{"f.txt": f},
		after: map[string]string{"f.txt": "1\none and a half\n" + numbered(2, 24) + "twenty-five\n" + numbered(26, 40)},
	}, {
		name:  "its lines are where its header puts them, and elsewhere too",
		patch: "--- a/t.txt\n+++ b/t.txt\n@@ -9,3 +9,3 @@\n x\n-y\n+Y\n z\n",
		want:  diff("t.txt") + "@@ -9,3 +9,3 @@\n x\n-y\n+Y\n z\n",
		files: map[string]string{"t.txt": twice}, after: map[string]string{"t.txt": "x\ny\nz\n" + numbered(1, 5) + "x\nY\nz\n" + numbered(6, 10)},
	}, {
		// Only the new side's start is wrong: the old side says where the hunk goes.
		name:  "the new side starts elsewhere",
		patch: "--- a/f.txt\n+++ b/f.txt\n@@ -22,7 +29,7 @@\n 22\n 23\n 24\n-25\n+twenty-five\n 26\n 27\n 28\n",
		want:  diff("f.txt") + "@@ -22,7 +22,7 @@\n 22\n 23\n 24\n-25\n+twenty-five\n 26\n 27\n 28\n",
		files: map[string]string{"f.txt": f}, after: map[string]string{"f.txt": strings.Replace(f, "25\n", "twenty-five\n", 1)},
	}, {
		// With no context after its last change, the hunk ends the file: its lines are there, and
		// at line 1 too, where its header does not put it.
		name:  "it adds lines at the end of the file",
		patch: "--- a/r.txt\n+++ b/r.txt\n@@ -2,3 +2,4 @@\n a\n b\n c\n+d\n",
		want:  diff("r.txt") + "@@ -4,3 +4,4 @@\n a\n b\n c\n+d\n",
		files: map[string]string{"r.txt": "a\nb\nc\na\nb\nc\n"}, after: map[string]string{"r.txt": "a\nb\nc\na\nb\nc\nd\n"},
	}, {
		name:  "an empty file gets its first lines",
		patch: "--- a/e.txt\n+++ b/e.txt\n@@ -1,0 +1,2 @@\n+a\n+b\n",
		want:  diff("e.txt") + "@@ -0,0 +1,2 @@\n+a\n+b\n",
		files: map[string]string{"e.txt": ""}, after: map[string]string{"e.txt": "a\nb\n"},
	}, {
		name: "the file's last line has no newline",
		patch: "--- a/n.txt\n+++ b/n.txt\n@@ -4,2 +4,2 @@\n a\n-b\n\\ No newline at end of file\n+B\n" +
			"\\ No newline at end of file\n",
		want: diff("n.txt") + "@@ -1,2 +1,2 @@\n a\n-b\n\\ No newline at end of file\n+B\n" +
			"\\ No newline at end of file\n",
		files: map[string]string{"n.txt": "a\nb"}, after: map[string]string{"n.txt": "a\nB"},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			text, after, err := applied(t, tc.patch, tc.files)
			if err != nil {
				t.Fatal(err)
			}
			if text != tc.want {
				t.Errorf("the patch as applied:\n%s\nwant:\n%s", text, tc.want)
			}
			if !equal(after, tc.after) {
				t.Errorf("after the patch:\n%q\nwant\n%q", after, tc.after)
			}
		})
	}

	conf := "; config\n[main]\nallow = no\n[guest]\nallow = no\n"
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
		// git apply puts the line under [guest], at the end; the header, under [main].
		name:  "it has no context after its last change, and its header puts it elsewhere",
		patch: "--- a/c.ini\n+++ b/c.ini\n@@ -3 +3,2 @@\n allow = no\n+admin = guest\n",
		want:  "c.ini: the patch does not apply: hunk 1 has no context after its last change, so git apply puts it at the file's end, and its header at line 3",
	}, {
		name:  "it has no context after its last change, and its lines are not at the end",
		patch: "--- a/f.txt\n+++ b/f.txt\n@@ -2,1 +2,2 @@\n 3\n+three and a bit\n",
		want:  "f.txt: the patch does not apply: hunk 1 has no context after its last change, so it must end the file",
	}, {
		name:  "two hunks are the same lines",
		patch: "--- a/f.txt\n+++ b/f.txt\n@@ -2,3 +2,3 @@\n 2\n-3\n+three\n 4\n@@ -9,3 +9,3 @@\n 2\n-3\n+three\n 4\n",
		want:  "f.txt: the patch does not apply: hunk 2 is not after hunk 1 in the file",
	}, {
		// git apply applies hunks in the patch's order, each looked for from where the one
		// before left the file: it would change another "p q" than go-gitdiff.
		name:  "the hunks are not in the file's order",
		patch: "--- a/o.txt\n+++ b/o.txt\n@@ -3,9 +3,9 @@\n p\n+INSERTED\n q\n@@ -1,2 +1,2 @@\n b1\n+n1\n+n2\n b2\n",
		want:  "o.txt: the patch does not apply: hunk 2 is not after hunk 1 in the file",
	}, {
		// go-gitdiff puts the second after the first; git apply, before.
		name:  "two hunks give an empty file its first lines",
		patch: "--- a/e.txt\n+++ b/e.txt\n@@ -0,0 +1 @@\n+a\n@@ -0,0 +1 @@\n+b\n",
		want:  "e.txt: the patch does not apply: hunk 1 holds no line of the file",
	}, {
		// The two lines would be one: "ab".
		name:  "a line without its newline in the middle of what it adds",
		patch: "--- a/f.txt\n+++ b/f.txt\n@@ -19,2 +19,4 @@\n 19\n+a\n\\ No newline at end of file\n+b\n 20\n",
		want:  "f.txt: the patch does not apply: hunk 1 leaves a line without its newline that is not the file's last",
	}} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := applied(t, tc.patch, map[string]string{"f.txt": f, "t.txt": twice, "c.ini": conf, "e.txt": "",
				"o.txt": "b1\nb2\np\nq\np\nq\nend\n"})
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

// A line that reads as a hunk's, but that no hunk holds, would read two ways: a patch with one is
// refused. That a reviewer could take it for a change that nothing applies is reason enough.
func TestALineInNoHunkIsRefused(t *testing.T) {
	hunk := "--- a/f.txt\n+++ b/f.txt\n@@ -6,3 +6,3 @@\n 6\n-7\n+seven\n 8\n"
	for _, tc := range []struct{ name, patch, want string }{
		{"a hunk's line after text", hunk + "Note:\n+more\n", `line 9 of the patch reads as a hunk's, but is in no hunk: "+more\n"`},
		{"a hunk header after text", hunk + "and then\n@@ -12,3 +12,3 @@\n 12\n-13\n+thirteen\n 14\n",
			"line 9 of the patch is a hunk header after text, with no file's header between"},
		{"a header agentd cannot read", hunk + "@@ -12,3 +12 3 @@\n 12\n", `line 8 of the patch is not a hunk header: "@@ -12,3 +12 3 @@\n"`},
		// git apply takes a line that starts with "\ " after a hunk for "\ No newline at end of
		// file", however short; go-gitdiff, only from 12 bytes.
		{"a short \"\\\" line after a hunk", hunk + "\\ x\nmore text\n", `line 8 of the patch reads as a hunk's, but is in no hunk: "\\ x\n"`},
		{"a hunk's lines after a \"diff\" line, before any hunk header",
			hunk + "diff -u a/f.txt b/f.txt\n+    if not ok:\n+        raise ValueError\n",
			`line 9 of the patch reads as a hunk's, but comes before the hunk's header: "+    if not ok:\n"`},
		{"a hunk's line in the header of a new file without hunks",
			"diff --git a/x.txt b/x.txt\nnew file mode 100644\n--- /dev/null\n+++ b/x.txt\n+content nothing writes\n",
			`line 5 of the patch reads as a hunk's, but comes before the hunk's header: "+content nothing writes\n"`},
		// git apply drops a plain header's first path component even when other files have git's
		// headers; agentd would not.
		{"git's headers and plain ones", "diff --git a/f.txt b/f.txt\n--- a/f.txt\n+++ b/f.txt\n@@ -6,3 +6,3 @@\n 6\n-7\n+seven\n 8\n" +
			"--- /dev/null\n+++ b/x.txt\n@@ -0,0 +1 @@\n+new\n", "the patch mixes git's file headers with plain ones"},
		// As lines the hunk removes and adds, "--- a/x" and "+++ b/x" would end it with no context
		// after its last change, before another of the file's hunks: they are a file's header, and
		// the hunk before them changes nothing.
		{"lines that remove \"-- a/x\" and add \"++ b/x\" before a hunk header",
			"--- a/q.sql\n+++ b/q.sql\n@@ -1,2 +1,2 @@\n select 1;\n--- a/x\n+++ b/x\n@@ -6,3 +6,3 @@\n select 6;\n-select 7;\n+select seven;\n select 8;\n",
			"fragment contains no changes"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse([]byte(tc.patch))
			refused(t, err, tc.want)
		})
	}
}

// What a reviewer reads, Text, holds the files' headers and their hunks and nothing else: no line
// that looks like a change and is none, before the first file, between files or after the last,
// however it is spelled.
func TestTextHoldsOnlyWhatIsApplied(t *testing.T) {
	patch := "Here is the fix:\n+    return hmac.compare_digest(token, SECRET)\n\n" +
		"--- a/f.txt\n+++ b/f.txt\n@@ -6,3 +6,3 @@\n 6\n-7\n+seven\n 8\n" +
		"Only in b: extra.txt\n" +
		"--- a/g.txt\n+++ b/g.txt\n@@ -1,2 +1,2 @@\n-a\n+A\n b\n" +
		"\u2212    return token == SECRET\n\uff0b    return hmac.compare_digest(token, SECRET)\n"
	text, _, err := applied(t, patch, map[string]string{"f.txt": numbered(1, 20), "g.txt": "a\nb\n"})
	if err != nil {
		t.Fatal(err)
	}
	if want := diff("f.txt") + "@@ -6,3 +6,3 @@\n 6\n-7\n+seven\n 8\n" + diff("g.txt") + "@@ -1,2 +1,2 @@\n-a\n+A\n b\n"; text != want {
		t.Fatalf("the patch as applied:\n%s\nwant:\n%s", text, want)
	}
}

// Differential: git's own patches, their hunk headers scrambled, come out as git wrote them (its
// short counts written out, and without the headings of hunks that moved), and git apply makes of
// them what agentd does.
func TestScrambledHeadersComeBackAsGitWroteThem(t *testing.T) {
	// Nothing from the user's git configuration (a diff driver, colours) changes what git writes.
	t.Setenv("GIT_CONFIG_GLOBAL", os.DevNull)
	t.Setenv("GIT_CONFIG_NOSYSTEM", "1")
	r := newMirror(t)
	rng := rand.New(rand.NewPCG(7, 11))
	header := regexp.MustCompile(`(?m)^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@.*$`)
	explicit := func(patch string) string { // as Text writes the headers: counts written out, no headings
		return header.ReplaceAllStringFunc(patch, func(h string) string {
			m := header.FindStringSubmatch(h)
			n := func(s string) string {
				if s == "" {
					return "1"
				}
				return s
			}
			return fmt.Sprintf("@@ -%s,%s +%s,%s @@", m[1], n(m[2]), m[3], n(m[4]))
		})
	}
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
		gits := r.gitRaw("diff")
		if gits == "" {
			continue
		}
		// As git writes it, and as a model would: without git's own lines.
		plain := regexp.MustCompile(`(?m)^(diff --git|index) .*\n`).ReplaceAllString(gits, "")
		for _, tc := range []struct{ patch, want string }{
			{gits, explicit(gits)},
			{plain, explicit(regexp.MustCompile(`(?m)^index .*\n`).ReplaceAllString(gits, ""))},
		} {
			scrambled := header.ReplaceAllStringFunc(tc.patch, func(h string) string {
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
			if text != tc.want {
				t.Fatalf("round %d: the patch as applied:\n%s\nwant git's:\n%s", round, text, tc.want)
			}
			if !equal(after, edits) {
				t.Fatalf("round %d: after the patch:\n%q\nwant\n%q", round, after, edits)
			}
		}
	}
}

// Differential, with empty lines and lines that repeat: git's own patches, as git writes them and
// with empty context lines written bare, apply as git wrote them; scrambled, each is refused, or
// applied as git apply applies the patch as applied (applied checks that).
func TestPatchesWithEmptyAndRepeatedLines(t *testing.T) {
	t.Setenv("GIT_CONFIG_GLOBAL", os.DevNull)
	t.Setenv("GIT_CONFIG_NOSYSTEM", "1")
	r := newMirror(t)
	rng := rand.New(rand.NewPCG(3, 5))
	header := regexp.MustCompile(`(?m)^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@`)
	pool := []string{"\n", "\n", "}\n", "return nil\n", "if err != nil {\n"}
	line := func(n *int) string {
		if rng.IntN(3) == 0 {
			return pool[rng.IntN(len(pool))]
		}
		*n++
		return fmt.Sprintf("line %d\n", *n)
	}
	var scrambledApplied, scrambledRefused int
	for round := range 80 {
		var orig, edit strings.Builder
		n := 0
		for range 10 + rng.IntN(40) {
			l := line(&n)
			orig.WriteString(l)
			switch rng.IntN(10) {
			case 0:
			case 1:
				edit.WriteString(line(&n))
			case 2:
				edit.WriteString(l)
				edit.WriteString(line(&n))
			default:
				edit.WriteString(l)
			}
		}
		o, e := orig.String(), edit.String()
		if o == e {
			continue
		}
		r.write("f.txt", []byte(o), 0o644)
		r.git("add", "-A")
		r.git("commit", "-q", "--allow-empty", "-m", strconv.Itoa(round))
		r.write("f.txt", []byte(e), 0o644)
		gits := r.gitRaw("diff")
		r.git("checkout", "f.txt")
		bare := regexp.MustCompile(`(?m)^ $`).ReplaceAllString(gits, "") // " \n" written as "\n"
		for _, patch := range []string{gits, bare} {
			if _, after, err := applied(t, patch, map[string]string{"f.txt": o}); err != nil || after["f.txt"] != e {
				t.Fatalf("round %d: %v %q\n%s", round, err, after["f.txt"], patch)
			}
			scrambled := header.ReplaceAllStringFunc(patch, func(h string) string {
				m := header.FindStringSubmatch(h)
				v, _ := strconv.Atoi(m[1])
				return fmt.Sprintf("@@ -%d,%d +%d,9 @@", max(1, v+rng.IntN(5)-2), rng.IntN(9), max(1, v+rng.IntN(5)-2))
			})
			if _, _, err := applied(t, scrambled, map[string]string{"f.txt": o}); err != nil {
				scrambledRefused++
			} else {
				scrambledApplied++
			}
		}
	}
	t.Logf("scrambled: %d applied, %d refused", scrambledApplied, scrambledRefused)
}

// Parse reads a patch whose hunk headers are wrong; the patch it reads is one whose headers are
// right.
func TestParseCountsTheLines(t *testing.T) {
	p, err := Parse([]byte("--- a/f.txt\n+++ b/f.txt\n@@ -4,9 +4,2 @@\n 4\n-5\n+five\n 6\n"))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(p.Text()), diff("f.txt")+"@@ -4,3 +4,3 @@\n 4\n-5\n+five\n 6\n"; got != want {
		t.Fatalf("parsed as\n%s\nwant\n%s", got, want)
	}
}

// Apply tells which hunk headers it rewrote, and how.
func TestApplySaysWhatItRewrote(t *testing.T) {
	p, err := Parse([]byte("--- a/f.txt\n+++ b/f.txt\n@@ -2,3 +2,3 @@\n 2\n-3\n+three\n 4\n" +
		"@@ -30 +30 @@ the twenties\n 22\n 23\n 24\n-25\n+twenty-five\n 26\n 27\n 28\n"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Apply(files(map[string]string{"f.txt": numbered(1, 40)})); err != nil {
		t.Fatal(err)
	}
	got := p.Rewritten()
	if len(got) != 1 || got[0] != (Rewrite{Path: "f.txt", Hunk: 2, Was: "@@ -30 +30 @@", Now: "@@ -22,7 +22,7 @@"}) {
		t.Fatalf("rewritten: %+v", got)
	}
	// Applied again, to other files, it starts from the patch's own headers.
	if _, err := p.Apply(files(map[string]string{"f.txt": numbered(1, 40)})); err != nil || len(p.Rewritten()) != 1 {
		t.Fatalf("applied again: %v, %+v", err, p.Rewritten())
	}
}

// The patch as agentd writes it stays within the limit the git tool server takes: a short header,
// and a plain file header, get longer.
func TestThePatchAsWrittenIsWithinTheLimit(t *testing.T) {
	const h = "@@ -1 +1 @@\n 1\n-2\n+two\n" // "@@ -1,2 +1,2 @@" once counted
	var b strings.Builder
	b.WriteString("--- a/f.txt\n+++ b/f.txt\n")
	for b.Len()+len(h) <= MaxBytes {
		b.WriteString(h)
	}
	_, err := Parse([]byte(b.String()))
	refused(t, err, "bytes as agentd writes it, over the limit of")
}
