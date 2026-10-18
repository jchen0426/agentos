// githubsim serves internal/githubsim on a loopback port, for the black-box tests: the
// stand-in for GitHub that T2, T15 and T16 count writes on.
package main

import (
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"

	"github.com/jchen0426/agentos/internal/githubsim"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:0", "address to listen on")
	addrFile := flag.String("addr-file", "", "write the address it listens on to this file")
	logPath := flag.String("log", "", "log every request here, one JSON line each")
	faults := flag.String("faults", "", "fault file (githubsim.Faults), read on every write and tree listing")
	login := flag.String("login", "agentos-test-bot", "the account the token belongs to")
	repos := map[string]string{}
	flag.Func("repo", "owner/name=dir: a local copy of a repository on GitHub, whose commits and trees GitHub has (repeatable)",
		func(v string) error {
			name, dir, ok := strings.Cut(v, "=")
			if !ok || strings.Count(name, "/") != 1 || dir == "" {
				return errors.New("want owner/name=dir")
			}
			repos[name] = dir
			return nil
		})
	flag.Parse()

	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		fmt.Fprintln(os.Stderr, "githubsim:", err)
		os.Exit(1)
	}
	if *addrFile != "" {
		if err := os.WriteFile(*addrFile, []byte(ln.Addr().String()), 0o600); err != nil {
			fmt.Fprintln(os.Stderr, "githubsim:", err)
			os.Exit(1)
		}
	}
	s := &githubsim.Server{Login: *login, LogPath: *logPath, FaultsPath: *faults, Repos: repos}
	fmt.Fprintln(os.Stderr, "githubsim listening on", ln.Addr())
	if err := http.Serve(ln, s); err != nil {
		fmt.Fprintln(os.Stderr, "githubsim:", err)
		os.Exit(1)
	}
}
