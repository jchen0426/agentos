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
	// commits and trees, and no others. A repository not listed has any 40-hex SHA as a
	// commit, whose tree nobody can list.
	Repos map[string]string

	mu       sync.Mutex
	commits  map[string]commit // created through the API
	refs     map[string]string // "owner/repo heads/branch" -> commit
	pulls    map[string][]*pull
	comments map[string][]*comment // "owner/repo commit" -> comments
	nextID   int64
}

type commit struct {
	Tree    string
	Parents []string
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
	reRef      = regexp.MustCompile(`^/repos/([^/]+)/([^/]+)/git/ref/(heads/.+)$`)
	reRefs     = regexp.MustCompile(`^/repos/([^/]+)/([^/]+)/git/refs$`)
	rePulls    = regexp.MustCompile(`^/repos/([^/]+)/([^/]+)/pulls$`)
	reComments = regexp.MustCompile(`^/repos/([^/]+)/([^/]+)/commits/([0-9a-f]{40})/comments$`)
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
		s.answer(w, r, "create_tree", http.StatusCreated, map[string]any{"sha": sha}, nil)
	case r.Method == http.MethodPost && m(reCommits) != nil:
		s.createCommit(w, r, body)
	case r.Method == http.MethodGet && m(reRef) != nil:
		g := m(reRef)
		s.getRef(w, r, g[1]+"/"+g[2]+" "+g[3])
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
	s.answer(w, r, "get_commit", http.StatusOK, map[string]any{"sha": sha, "tree": map[string]any{"sha": c.Tree}, "parents": parents}, nil)
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
	var c commit
	headers, _, _ := bytes.Cut(raw, []byte("\n\n"))
	for line := range strings.Lines(string(headers)) {
		switch k, v, _ := strings.Cut(strings.TrimSuffix(line, "\n"), " "); k {
		case "tree":
			c.Tree = v
		case "parent":
			c.Parents = append(c.Parents, v)
		}
	}
	return c, c.Tree != ""
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
	var parents []string
	for _, p := range anySlice(body["parents"]) {
		if ps, ok := p.(string); ok {
			parents = append(parents, ps)
		}
	}
	s.mu.Lock()
	s.commits[sha] = commit{Tree: tree, Parents: parents}
	s.mu.Unlock()
	s.answer(w, r, "create_commit", http.StatusCreated, map[string]any{"sha": sha, "tree": map[string]any{"sha": tree}}, nil)
}

func (s *Server) getRef(w http.ResponseWriter, r *http.Request, key string) {
	s.mu.Lock()
	sha, ok := s.refs[key]
	s.mu.Unlock()
	if !ok {
		s.answer(w, r, "get_ref", http.StatusNotFound, map[string]any{"message": "Not Found"}, nil)
		return
	}
	_, branch, _ := strings.Cut(key, " ")
	ref := "refs/" + branch
	s.answer(w, r, "get_ref", http.StatusOK, map[string]any{"ref": ref, "object": map[string]any{"sha": sha, "type": "commit"}}, nil)
}

func (s *Server) createRef(w http.ResponseWriter, r *http.Request, repo string, body map[string]any) {
	ref, _ := body["ref"].(string)
	sha, _ := body["sha"].(string)
	key := repo + " " + strings.TrimPrefix(ref, "refs/")
	s.mu.Lock()
	_, exists := s.refs[key]
	if !exists {
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
