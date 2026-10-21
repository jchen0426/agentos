// Package outbound keeps agentd's requests to the world outside the machine (the model
// providers, GitHub) off any proxy its environment names. HTTPS_PROXY and the like belong to
// whatever started agentd (a shell, or a sandbox runtime that hands every command a proxy of its
// own), while agentd's configuration alone says where the task's data and agentd's keys go
// (design doc, section 8). A proxy sees where every request goes, and one trusted to inspect TLS
// sees the keys too.
package outbound

import (
	"fmt"
	"net/http"
	"os"
)

// proxyVars are the variables Go's net/http, gRPC, curl and most other tools take a proxy from.
var proxyVars = []string{"HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy", "ALL_PROXY", "all_proxy",
	"NO_PROXY", "no_proxy"}

// IgnoreEnvProxies makes http.DefaultTransport take no proxy from the environment, and returns
// the proxy variables that are set, which agentd now ignores: it says so when it starts, so that
// nobody counts on them. Every HTTP client in agentd uses http.DefaultTransport, or a copy of it,
// unless it is given another: this runs before any client is made, so that a copy taken early takes
// no proxy either. gRPC connections take their own dial option, grpc.WithNoProxy. A default
// transport some import replaced with one of its own is an error: its proxy cannot be told.
func IgnoreEnvProxies() ([]string, error) {
	t, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return nil, fmt.Errorf("http.DefaultTransport is a %T: agentd cannot keep it off the environment's proxies",
			http.DefaultTransport)
	}
	t.Proxy = nil
	var set []string
	for _, v := range proxyVars {
		if _, ok := os.LookupEnv(v); ok {
			set = append(set, v)
		}
	}
	return set, nil
}
