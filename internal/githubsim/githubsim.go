// Package githubsim stands in for the parts of GitHub's REST API that the GitHub tools use,
// for tests (design doc, section 11: T2 and T3 run against a simulated external API). It keeps
// what the tools write in memory and logs every request it answers; a repository's own commits
// and trees it reads from a local copy. A fault file can hold the answer to a write after the
// write took effect, which puts a crash between the write and agentd recording it, or make
// GitHub's trees disagree with that copy. It checks no credentials: it logs whether a request
// carried a token.
package githubsim

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jchen0426/agentos/internal/gitenv"
)

// Faults is the fault file: for each action ("create_pull", "create_comment", ...), a path
// the answer waits on, after the write took effect, while that path exists.
type Faults struct {
	HoldAfter map[string]string `json:"hold_after"`
	// Entries changes the entry of that name in every tree GitHub lists: GitHub then holds
	// something other than the repository's copy, and the mirror agentd reads.
	Entries map[string]Entry `json:"entries"`
}

// Entry is what a tree listing shows of one entry in place of the repository's: the fields
// set.
type Entry struct {
	Mode string `json:"mode,omitempty"`
	Type string `json:"type,omitempty"`
	SHA  string `json:"sha,omitempty"`
}

type Server struct {
	Login      string // the account the token belongs to
	LogPath    string // one JSON line per request
	FaultsPath string // read on every write and tree listing; may be absent
	// Repos holds, for "owner/name", a local copy of the repository on GitHub: GitHub has its
	// commits, trees and branches, and no other commits. A repository not listed has any
	// 40-hex SHA as a commit, whose tree nobody can list, and no branches but those set.
	Repos map[string]string

	mu       sync.Mutex
	commits  map[string]commit // created through the API
	refs     map[string]string // "owner/repo heads/branch" -> commit
	pulls    map[string][]*pull
	comments map[string][]*comment // "owner/repo commit" -> comments
	nextID   int64
}

type commit struct {
	Tree              string
	Parents           []string
	Message           string // as GitHub shows it: without git's last newline
	Author, Committer person
}

// person is a commit's author or committer, as GitHub shows one.
type person struct {
	Name  string `json:"name,omitempty"`
	Email string `json:"email,omitempty"`
	Date  string `json:"date,omitempty"` // RFC 3339
}

type pull struct {
	Number int64  `json:"number"`
	State  string `json:"state"`
	Draft  bool   `json:"draft"`
	Title  string `json:"title"`
	Body   string `json:"body"`
	URL    string `json:"html_url"`
	Head   struct {
		Ref string `json:"ref"`
	} `json:"head"`
	Base struct {
		Ref string `json:"ref"`
	} `json:"base"`
}

type comment struct {
	ID   int64  `json:"id"`
	Body string `json:"body"`
	URL  string `json:"html_url"`
	User struct {
		Login string `json:"login"`
	} `json:"user"`
}

var (
	reUser     = regexp.MustCompile(`^/user$`)
	reRepo     = regexp.MustCompile(`^/repos/([^/]+)/([^/]+)$`)
	reCommit   = regexp.MustCompile(`^/repos/([^/]+)/([^/]+)/git/commits/([0-9a-f]{40})$`)
	reCommits  = regexp.MustCompile(`^/repos/([^/]+)/([^/]+)/git/commits$`)
	reTree     = regexp.MustCompile(`^/repos/([^/]+)/([^/]+)/git/trees/([0-9a-f]{40})$`)
	reTrees    = regexp.MustCompile(`^/repos/([^/]+)/([^/]+)/git/trees$`)
	reRef      = regexp.MustCompile(`^/repos/([^/]+)/([^/]+)/git/ref/heads/(.+)$`)
	reRefs     = regexp.MustCompile(`^/repos/([^/]+)/([^/]+)/git/refs$`)
	rePulls    = regexp.MustCompile(`^/repos/([^/]+)/([^/]+)/pulls$`)
	reComments = regexp.MustCompile(`^/repos/([^/]+)/([^/]+)/commits/([0-9a-f]{40})/comments$`)
	reCompare  = regexp.MustCompile(`^/repos/([^/]+)/([^/]+)/compare/(.+)$`)
	reSHA      = regexp.MustCompile(`^[0-9a-f]{40}$`)
)

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.init()
	s.mu.Unlock()
	var body map[string]any
	if r.Method == http.MethodPost {
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			s.answer(w, r, "bad_request", http.StatusBadRequest, map[string]any{"message": "Problems parsing JSON"}, nil)
			return
		}
	}
	p := r.URL.Path
	switch m := match(p); {
	case r.Method == http.MethodGet && reUser.MatchString(p):
		s.answer(w, r, "get_user", http.StatusOK, map[string]any{"login": s.Login}, nil)
	case r.Method == http.MethodGet && m(reRepo) != nil:
		g := m(reRepo)
		s.answer(w, r, "get_repo", http.StatusOK, map[string]any{"name": g[2], "full_name": g[1] + "/" + g[2],
			"owner": map[string]any{"login": g[1]}}, nil)
	case r.Method == http.MethodGet && m(reCommit) != nil:
		g := m(reCommit)
		s.getCommit(w, r, g[1]+"/"+g[2], g[3])
	case r.Method == http.MethodGet && m(reTree) != nil:
		g := m(reTree)
		s.getTree(w, r, g[1]+"/"+g[2], g[3])
	case r.Method == http.MethodPost && m(reTrees) != nil:
		sha := hash(body)
		s.answer(w, r, "create_tree", http.StatusCreated, map[string]any{"sha": sha},
			map[string]any{"sha": sha, "base_tree": body["base_tree"]})
	case r.Method == http.MethodPost && m(reCommits) != nil:
		s.createCommit(w, r, body)
	case r.Method == http.MethodGet && m(reRef) != nil:
		g := m(reRef)
		s.getRef(w, r, g[1]+"/"+g[2], g[3])
	case r.Method == http.MethodGet && m(reCompare) != nil:
		g := m(reCompare)
		s.compare(w, r, g[1]+"/"+g[2], g[3])
	case r.Method == http.MethodPost && m(reRefs) != nil:
		g := m(reRefs)
		s.createRef(w, r, g[1]+"/"+g[2], body)
	case r.Method == http.MethodGet && m(rePulls) != nil:
		g := m(rePulls)
		s.listPulls(w, r, g[1]+"/"+g[2])
	case r.Method == http.MethodPost && m(rePulls) != nil:
		g := m(rePulls)
		s.createPull(w, r, g[1], g[2], body)
	case r.Method == http.MethodGet && m(reComments) != nil:
		g := m(reComments)
		s.listComments(w, r, g[1]+"/"+g[2]+" "+g[3])
	case r.Method == http.MethodPost && m(reComments) != nil:
		g := m(reComments)
		s.createComment(w, r, g[1]+"/"+g[2], g[3], body)
	default:
		s.answer(w, r, "unknown", http.StatusNotFound, map[string]any{"message": "Not Found"}, nil)
	}
}

// init makes the maps; s.mu is held.
func (s *Server) init() {
	if s.commits == nil {
		s.commits, s.refs, s.pulls, s.comments = map[string]commit{}, map[string]string{}, map[string][]*pull{}, map[string][]*comment{}
	}
}

func match(path string) func(*regexp.Regexp) []string {
	return func(re *regexp.Regexp) []string { return re.FindStringSubmatch(path) }
}

func (s *Server) getCommit(w http.ResponseWriter, r *http.Request, repo, sha string) {
	s.mu.Lock()
	c, ok := s.commits[sha]
	s.mu.Unlock()
	switch dir, local := s.Repos[repo]; {
	case ok:
	case local:
		// One of the repository's own.
		if c, ok = readCommit(dir, sha); !ok {
			s.answer(w, r, "get_commit", http.StatusNotFound, map[string]any{"message": "Not Found"}, nil)
			return
		}
	default:
		c = commit{Tree: hash(map[string]any{"tree of": sha})}
	}
	parents := []map[string]any{}
	for _, p := range c.Parents {
		parents = append(parents, map[string]any{"sha": p})
	}
	s.answer(w, r, "get_commit", http.StatusOK, map[string]any{"sha": sha, "tree": map[string]any{"sha": c.Tree}, "parents": parents,
		"message": c.Message, "author": c.Author, "committer": c.Committer}, nil)
}

// readCommit reads a commit's tree and parents from the repository's copy.
func readCommit(dir, sha string) (commit, bool) {
	if t, err := gitIn(dir, "cat-file", "-t", sha); err != nil || string(bytes.TrimSpace(t)) != "commit" {
		return commit{}, false
	}
	raw, err := gitIn(dir, "cat-file", "commit", sha)
	if err != nil {
		return commit{}, false
	}
	headers, message, _ := bytes.Cut(raw, []byte("\n\n"))
	c := commit{Message: strings.TrimSuffix(string(message), "\n")}
	for line := range strings.Lines(string(headers)) {
		switch k, v, _ := strings.Cut(strings.TrimSuffix(line, "\n"), " "); k {
		case "tree":
			c.Tree = v
		case "parent":
			c.Parents = append(c.Parents, v)
		case "author":
			c.Author = signature(v)
		case "committer":
			c.Committer = signature(v)
		}
	}
	return c, c.Tree != ""
}

// signature reads a commit header's "Name <email> seconds zone".
func signature(v string) person {
	name, rest, _ := strings.Cut(v, " <")
	email, when, _ := strings.Cut(rest, "> ")
	seconds, zone, _ := strings.Cut(when, " ")
	n, _ := strconv.ParseInt(seconds, 10, 64)
	at := time.Unix(n, 0).UTC()
	if offset, err := time.Parse("-0700", zone); err == nil {
		at = at.In(offset.Location())
	}
	return person{Name: name, Email: email, Date: at.Format(time.RFC3339)}
}

// getTree lists one tree of the repository's copy, one level, as GitHub's Git Trees API does
// without recursive: each entry's name, mode, type, object, and a blob's size. Like GitHub, it
// takes a commit for its tree, and refuses another kind of object (422).
func (s *Server) getTree(w http.ResponseWriter, r *http.Request, repo, sha string) {
	dir, local := s.Repos[repo]
	if r.URL.Query().Has("recursive") {
		s.answer(w, r, "get_tree", http.StatusUnprocessableEntity, map[string]any{"message": "the simulator lists one level at a time"}, nil)
		return
	}
	var out []byte
	if local {
		switch t, err := gitIn(dir, "cat-file", "-t", sha); string(bytes.TrimSpace(t)) {
		case "tree", "commit":
			out, _ = gitIn(dir, "ls-tree", "-z", "-l", sha)
		case "blob", "tag":
			if err == nil {
				s.answer(w, r, "get_tree", http.StatusUnprocessableEntity,
					map[string]any{"message": "Invalid object requested. SHA must identify a commit or a tree."}, nil)
				return
			}
		}
	}
	if out == nil {
		s.answer(w, r, "get_tree", http.StatusNotFound, map[string]any{"message": "Not Found"}, nil)
		return
	}
	faults := s.faults().Entries
	entries := []map[string]any{}
	for line := range bytes.SplitSeq(out, []byte{0}) {
		meta, name, ok := bytes.Cut(line, []byte{'\t'})
		f := strings.Fields(string(meta)) // mode, type, object, size ("-" for a tree or a commit)
		if !ok || len(f) != 4 {
			continue
		}
		e := map[string]any{"path": string(name), "mode": f[0], "type": f[1], "sha": f[2]}
		if n, err := strconv.Atoi(f[3]); err == nil {
			e["size"] = n
		}
		if fault, ok := faults[string(name)]; ok {
			for k, v := range map[string]string{"mode": fault.Mode, "type": fault.Type, "sha": fault.SHA} {
				if v != "" {
					e[k] = v
				}
			}
		}
		if t := e["type"]; t == "blob" || t == "tree" {
			e["url"] = fmt.Sprintf("https://github.example/api/repos/%s/git/%ss/%s", repo, t, e["sha"])
		}
		entries = append(entries, e)
	}
	s.answer(w, r, "get_tree", http.StatusOK, map[string]any{"sha": sha, "tree": entries, "truncated": false,
		"url": fmt.Sprintf("https://github.example/api/repos/%s/git/trees/%s", repo, sha)}, nil)
}

// gitIn runs git on a repository's copy: the tests' own, trusted.
func gitIn(dir string, args ...string) ([]byte, error) {
	cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
	cmd.Env = gitenv.ForMirror(dir)
	return cmd.Output()
}

func (s *Server) createCommit(w http.ResponseWriter, r *http.Request, body map[string]any) {
	sha := hash(body)
	tree, _ := body["tree"].(string)
	message, _ := body["message"].(string)
	var who [2]person
	for i, k := range []string{"author", "committer"} {
		b, _ := json.Marshal(body[k])
		_ = json.Unmarshal(b, &who[i])
	}
	var parents []string
	for _, p := range anySlice(body["parents"]) {
		if ps, ok := p.(string); ok {
			parents = append(parents, ps)
		}
	}
	s.mu.Lock()
	s.commits[sha] = commit{Tree: tree, Parents: parents, Message: message, Author: who[0], Committer: who[1]}
	s.mu.Unlock()
	s.answer(w, r, "create_commit", http.StatusCreated, map[string]any{"sha": sha, "tree": map[string]any{"sha": tree}},
		map[string]any{"sha": sha, "tree": tree, "parents": parents})
}

func (s *Server) getRef(w http.ResponseWriter, r *http.Request, repo, branch string) {
	// GitHub finds "a//b" as a/b, and answers under the name it was asked for; one that ends in
	// "/" it does not find (checked on github.com).
	sha, ok := "", false
	if !strings.HasSuffix(branch, "/") {
		sha, ok = s.branch(repo, strings.Join(strings.FieldsFunc(branch, func(c rune) bool { return c == '/' }), "/"))
	}
	if !ok {
		s.answer(w, r, "get_ref", http.StatusNotFound, map[string]any{"message": "Not Found"}, nil)
		return
	}
	s.answer(w, r, "get_ref", http.StatusOK, map[string]any{"ref": "refs/heads/" + branch,
		"object": map[string]any{"sha": sha, "type": "commit"}}, nil)
}

// branch is the commit a branch points at: one the API created or a test set, or else the
// repository copy's own.
func (s *Server) branch(repo, name string) (string, bool) {
	s.mu.Lock()
	sha, ok := s.refs[repo+" heads/"+name]
	s.mu.Unlock()
	if dir, local := s.Repos[repo]; !ok && local {
		out, err := gitIn(dir, "show-ref", "--verify", "-s", "refs/heads/"+name)
		sha, ok = string(bytes.TrimSpace(out)), err == nil
	}
	return sha, ok
}

// compare compares two commits of the repository's copy as GitHub's compare API does: base and
// head are each a branch or a full SHA, and the answer says whether head is in base's history
// (behind, identical) or has commits of its own (ahead, diverged). It lists those commits,
// oldest first, and the files they change since the merge base. Like GitHub, it compares a
// commit no branch holds; it does not compare commits the API created.
func (s *Server) compare(w http.ResponseWriter, r *http.Request, repo, basehead string) {
	dir, local := s.Repos[repo]
	baseName, headName, _ := strings.Cut(basehead, "...")
	base, baseFound := s.commitNamed(repo, dir, baseName)
	head, headFound := s.commitNamed(repo, dir, headName)
	if !local || !baseFound || !headFound {
		s.answer(w, r, "compare", http.StatusNotFound, map[string]any{"message": "Not Found"}, nil)
		return
	}
	out, err := gitIn(dir, "merge-base", base, head)
	if err != nil {
		s.answer(w, r, "compare", http.StatusNotFound,
			map[string]any{"message": fmt.Sprintf("No common ancestor between %s and %s.", baseName, headName)}, nil)
		return
	}
	mergeBase := string(bytes.TrimSpace(out))
	own, err := revList(dir, base+".."+head)
	theirs, err2 := revList(dir, head+".."+base)
	diff, err3 := gitIn(dir, "diff", "--name-status", "-z", "--no-renames", mergeBase, head, "--")
	if err := errors.Join(err, err2, err3); err != nil {
		s.answer(w, r, "compare", http.StatusInternalServerError, map[string]any{"message": "git: " + err.Error()}, nil)
		return
	}
	status := map[[2]bool]string{{true, true}: "identical", {false, true}: "ahead", {true, false}: "behind",
		{false, false}: "diverged"}[[2]bool{len(own) == 0, len(theirs) == 0}]
	commits := []map[string]any{}
	for i, sha := range own {
		if n, _ := strconv.Atoi(r.URL.Query().Get("per_page")); n > 0 && i == n {
			break // the first page
		}
		commits = append(commits, commitObject(dir, sha))
	}
	files := []map[string]any{}
	fields := strings.Split(strings.TrimSuffix(string(diff), "\x00"), "\x00")
	for i := 0; i+1 < len(fields); i += 2 {
		files = append(files, map[string]any{"filename": fields[i+1],
			"status": map[string]string{"A": "added", "D": "removed", "M": "modified", "T": "changed"}[fields[i]]})
	}
	s.answer(w, r, "compare", http.StatusOK, map[string]any{
		"url":      fmt.Sprintf("https://github.example/api/repos/%s/compare/%s", repo, basehead),
		"html_url": fmt.Sprintf("https://github.example/%s/compare/%s", repo, basehead),
		"status":   status, "ahead_by": len(own), "behind_by": len(theirs), "total_commits": len(own),
		"base_commit": commitObject(dir, base), "merge_base_commit": commitObject(dir, mergeBase),
		"commits": commits, "files": files,
	}, nil)
}

// commitNamed finds a commit of the repository's copy by its full SHA or a branch's name.
func (s *Server) commitNamed(repo, dir, name string) (string, bool) {
	sha, ok := name, reSHA.MatchString(name)
	if !ok {
		sha, ok = s.branch(repo, name)
	}
	if ok && dir != "" {
		_, ok = readCommit(dir, sha)
	}
	return sha, ok && dir != ""
}

// revList is the commits of a range of the repository's copy, oldest first.
func revList(dir, rng string) ([]string, error) {
	out, err := gitIn(dir, "rev-list", "--reverse", "--topo-order", rng, "--")
	return strings.Fields(string(out)), err
}

// commitObject is a commit as GitHub's compare API shows it: the parts the tools use, and its
// message, which most of such an answer's size is.
func commitObject(dir, sha string) map[string]any {
	c, _ := readCommit(dir, sha)
	parents := []map[string]any{}
	for _, p := range c.Parents {
		parents = append(parents, map[string]any{"sha": p})
	}
	return map[string]any{"sha": sha, "commit": map[string]any{"tree": map[string]any{"sha": c.Tree}, "message": c.Message},
		"parents": parents}
}

func (s *Server) createRef(w http.ResponseWriter, r *http.Request, repo string, body map[string]any) {
	ref, _ := body["ref"].(string)
	sha, _ := body["sha"].(string)
	key := repo + " " + strings.TrimPrefix(ref, "refs/")
	_, exists := s.branch(repo, strings.TrimPrefix(ref, "refs/heads/")) // the repository's own, too
	s.mu.Lock()
	if _, ok := s.refs[key]; ok || exists {
		exists = true
	} else {
		s.refs[key] = sha
	}
	s.mu.Unlock()
	if exists {
		s.answer(w, r, "create_ref", http.StatusUnprocessableEntity, map[string]any{"message": "Reference already exists"}, nil)
		return
	}
	s.answer(w, r, "create_ref", http.StatusCreated, map[string]any{"ref": ref, "object": map[string]any{"sha": sha}},
		map[string]any{"ref": ref})
}

func (s *Server) listPulls(w http.ResponseWriter, r *http.Request, repo string) {
	head := r.URL.Query().Get("head")
	state := r.URL.Query().Get("state")
	s.mu.Lock()
	out := []*pull{}
	for _, p := range s.pulls[repo] {
		owner, _, _ := strings.Cut(repo, "/")
		if (head == "" || head == owner+":"+p.Head.Ref) && (state == "all" || p.State == "open") {
			c := *p
			out = append(out, &c)
		}
	}
	s.mu.Unlock()
	s.answer(w, r, "list_pulls", http.StatusOK, out, nil)
}

func (s *Server) createPull(w http.ResponseWriter, r *http.Request, owner, name string, body map[string]any) {
	repo := owner + "/" + name
	head, _ := body["head"].(string)
	base, _ := body["base"].(string)
	if _, ok := s.branch(repo, base); !ok {
		s.answer(w, r, "create_pull", http.StatusUnprocessableEntity, map[string]any{"message": "Validation Failed",
			"errors": []any{map[string]any{"resource": "PullRequest", "field": "base", "code": "invalid"}}}, nil)
		return
	}
	s.mu.Lock()
	if _, ok := s.refs[repo+" heads/"+head]; !ok {
		s.mu.Unlock()
		s.answer(w, r, "create_pull", http.StatusUnprocessableEntity, map[string]any{"message": "Validation Failed",
			"errors": []any{map[string]any{"resource": "PullRequest", "field": "head", "code": "invalid"}}}, nil)
		return
	}
	for _, p := range s.pulls[repo] {
		if p.Head.Ref == head && p.State == "open" {
			s.mu.Unlock()
			s.answer(w, r, "create_pull", http.StatusUnprocessableEntity, map[string]any{"message": "Validation Failed",
				"errors": []any{map[string]any{"message": "A pull request already exists for " + owner + ":" + head + "."}}}, nil)
			return
		}
	}
	s.nextID++
	p := &pull{Number: s.nextID, State: "open", URL: fmt.Sprintf("https://github.example/%s/pull/%d", repo, s.nextID)}
	p.Title, _ = body["title"].(string)
	p.Body, _ = body["body"].(string)
	p.Draft, _ = body["draft"].(bool)
	p.Head.Ref = head
	p.Base.Ref, _ = body["base"].(string)
	s.pulls[repo] = append(s.pulls[repo], p)
	c := *p
	s.mu.Unlock()
	s.answer(w, r, "create_pull", http.StatusCreated, &c, map[string]any{"head": head, "number": c.Number})
}

func (s *Server) listComments(w http.ResponseWriter, r *http.Request, key string) {
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	perPage, _ := strconv.Atoi(r.URL.Query().Get("per_page"))
	if page < 1 {
		page = 1
	}
	if perPage < 1 {
		perPage = 30
	}
	s.mu.Lock()
	all := slices.Clone(s.comments[key])
	s.mu.Unlock()
	start := min((page-1)*perPage, len(all))
	end := min(start+perPage, len(all))
	if end < len(all) {
		w.Header().Set("Link", fmt.Sprintf(`<%s?page=%d&per_page=%d>; rel="next"`, r.URL.Path, page+1, perPage))
	}
	s.answer(w, r, "list_comments", http.StatusOK, all[start:end], nil)
}

var reMarker = regexp.MustCompile(`<!-- agent-effect:([0-9a-f]+) -->`)

// AddComment posts a comment as login, as someone other than agentd could.
func (s *Server) AddComment(repo, sha, login, body string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.init()
	s.nextID++
	c := &comment{ID: s.nextID, Body: body}
	c.User.Login = login
	s.comments[repo+" "+sha] = append(s.comments[repo+" "+sha], c)
}

// SetRef points a branch at a commit, as someone other than agentd could.
func (s *Server) SetRef(repo, branch, sha string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.init()
	s.refs[repo+" heads/"+branch] = sha
}

func (s *Server) createComment(w http.ResponseWriter, r *http.Request, repo, sha string, body map[string]any) {
	text, _ := body["body"].(string)
	s.mu.Lock()
	s.nextID++
	c := &comment{ID: s.nextID, Body: text, URL: fmt.Sprintf("https://github.example/%s/commit/%s#comment-%d", repo, sha, s.nextID)}
	c.User.Login = s.Login
	s.comments[repo+" "+sha] = append(s.comments[repo+" "+sha], c)
	cc := *c
	s.mu.Unlock()
	effect := ""
	if m := reMarker.FindStringSubmatch(text); m != nil {
		effect = m[1]
	}
	s.answer(w, r, "create_comment", http.StatusCreated, &cc, map[string]any{"effect": effect, "id": cc.ID})
}

// answer logs the request, holds the answer to a write that took effect when the fault file
// says so, and then sends it.
func (s *Server) answer(w http.ResponseWriter, r *http.Request, action string, code int, v any, detail map[string]any) {
	entry := map[string]any{"at": time.Now().UTC().Format(time.RFC3339Nano), "method": r.Method, "path": r.URL.Path,
		"action": action, "status": code, "token": strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ")}
	maps.Copy(entry, detail)
	s.log(entry)
	if code < 300 && r.Method == http.MethodPost {
		if hold := s.faults().HoldAfter[action]; hold != "" {
			for exists(hold) {
				time.Sleep(50 * time.Millisecond)
			}
		}
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func (s *Server) faults() Faults {
	var f Faults
	if s.FaultsPath == "" {
		return f
	}
	if b, err := os.ReadFile(s.FaultsPath); err == nil {
		_ = json.Unmarshal(b, &f)
	}
	return f
}

func (s *Server) log(entry map[string]any) {
	if s.LogPath == "" {
		return
	}
	line, _ := json.Marshal(entry)
	s.mu.Lock()
	defer s.mu.Unlock()
	f, err := os.OpenFile(s.LogPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return
	}
	defer f.Close()
	_, _ = f.Write(append(line, '\n'))
}

func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// hash names an object by its content, as git does, so the same request makes the same object.
func hash(v any) string {
	b, _ := json.Marshal(v)
	sum := sha1.Sum(b)
	return hex.EncodeToString(sum[:])
}

func anySlice(v any) []any {
	s, _ := v.([]any)
	return s
}
