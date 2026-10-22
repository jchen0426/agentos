// Package githubtool holds the first task's GitHub write tools (design doc, section 3):
// github.open_draft_pr and github.comment_commit. Both are lookup tools (section 6): a pull
// request is found by its head branch, a comment by a hidden marker that names its effect.
package githubtool

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/google/go-github/v92/github"

	"github.com/jchen0426/agentos/internal/tools"
)

// Config is how agentd reaches GitHub.
type Config struct {
	APIURL         string        // default https://api.github.com/; a simulator in tests
	Token          string        // empty only for a simulator on a loopback address
	RequestTimeout time.Duration // per HTTP request
	Settle         time.Duration // see tools.Finder: RequestTimeout, GitHub's 10s limit and a margin
	Author         Author        // who the patch commits are by
}

// Author signs the commits the tools create.
type Author struct{ Name, Email string }

// Repo is where a logical repository lives: its read-only mirror and its name on GitHub.
type Repo struct{ Mirror, Owner, Name string }

// Client is a GitHub API client. A nil *Client means GitHub is not configured: the tools
// only dry-run.
type Client struct {
	gh  *github.Client
	cfg Config

	mu    sync.Mutex
	login string // the token's own account: a lookup trusts only its comments
}

func New(cfg Config) (*Client, error) {
	opts := []github.ClientOptionsFunc{github.WithTimeout(cfg.RequestTimeout), github.WithUserAgent("agentos"),
		github.WithTransport(recordReads{http.DefaultTransport})}
	if cfg.Token != "" {
		opts = append(opts, github.WithAuthToken(cfg.Token))
	}
	if cfg.APIURL != "" {
		opts = append(opts, github.WithURLs(&cfg.APIURL, &cfg.APIURL))
	}
	gh, err := github.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	return &Client{gh: gh, cfg: cfg}, nil
}

func (c *Client) live() bool { return c != nil }

// recordReads is the client's transport. A request made for one of agentd's own reads (a Find, a
// Describe: tools.WithReads) is recorded before it goes out, and does not go out unrecorded. The
// record names the method and the path; the token is in a header and the query is left out.
type recordReads struct{ next http.RoundTripper }

func (t recordReads) RoundTrip(req *http.Request) (*http.Response, error) {
	if reads := tools.ReadsOf(req.Context()); reads != nil {
		if err := reads(req.Context(), req.Method, req.URL.EscapedPath()); err != nil {
			if req.Body != nil {
				req.Body.Close()
			}
			return nil, fmt.Errorf("%w: agentd could not record its read of %s: %v", tools.ErrNotSent, req.URL.EscapedPath(), err)
		}
	}
	return t.next.RoundTrip(req)
}

// unconfiguredSettle is the settle of an agentd without GitHub's config. It sends nothing, but may
// find a request another agentd sent, as a person reconciles it, and cannot know that one's settle:
// it waits well past the default config's (30s requests, a 60s settle).
const unconfiguredSettle = 10 * time.Minute

func (c *Client) settle() time.Duration {
	if c == nil {
		return unconfiguredSettle
	}
	return c.cfg.Settle
}

// self is the login of the token's account, asked once.
func (c *Client) self(ctx context.Context) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.login == "" {
		u, _, err := c.gh.Users.Get(ctx, "")
		if err != nil {
			return "", err
		}
		c.login = u.GetLogin()
	}
	return c.login, nil
}

// classify sorts a failed request by what it did outside. An answer that declined is a
// refusal, retryable for rate limits; a connection that was never made sent nothing; after
// anything else (a 5xx, a timeout, a connection lost) nobody knows.
func classify(err error) error {
	var rateLimit *github.RateLimitError
	var abuse *github.AbuseRateLimitError
	var answer *github.ErrorResponse
	var dns *net.DNSError
	var op *net.OpError
	switch {
	case errors.As(err, &rateLimit), errors.As(err, &abuse):
		return &tools.RefusedError{Reason: err.Error(), Retryable: true}
	case errors.As(err, &answer) && answer.Response != nil:
		switch code := answer.Response.StatusCode; {
		case code == http.StatusTooManyRequests:
			return &tools.RefusedError{Reason: err.Error(), Retryable: true}
		case code >= 400 && code < 500:
			return &tools.RefusedError{Reason: err.Error()}
		}
	case errors.As(err, &dns), errors.As(err, &op) && op.Op == "dial":
		return fmt.Errorf("%w: %v", tools.ErrNotSent, err)
	}
	return err
}

// unseen classifies a failure that came before the request with a visible effect: whatever
// its outcome, nobody sees it (a tree or commit object nothing points at), or a retry checks
// it again (the branch), so the effect counts as not sent.
func unseen(err error) error {
	err = classify(err)
	var refused *tools.RefusedError
	if errors.As(err, &refused) || errors.Is(err, tools.ErrNotSent) {
		return err
	}
	return fmt.Errorf("%w: %v", tools.ErrNotSent, err)
}

func status(resp *github.Response) int {
	if resp == nil || resp.Response == nil {
		return 0
	}
	return resp.StatusCode
}

// marker is hidden in what a tool writes, so that a lookup can find it.
func marker(effectID string) string { return "<!-- agent-effect:" + effectID + " -->" }
