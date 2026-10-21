package outbound

import (
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"sync/atomic"
	"testing"
	"time"
)

// onlyProxyVars sets the proxy variables in vars and unsets the others for the test.
func onlyProxyVars(t *testing.T, vars map[string]string) {
	for _, v := range proxyVars {
		if val, ok := vars[v]; ok {
			t.Setenv(v, val)
			continue
		}
		if old, ok := os.LookupEnv(v); ok {
			os.Unsetenv(v)
			t.Cleanup(func() { os.Setenv(v, old) })
		}
	}
}

// away is an address off this machine that answers nothing (TEST-NET-1): net/http takes a
// proxy for a loopback address from no environment, so only a request elsewhere shows one.
const away = "http://192.0.2.1:9/"

func TestNoProxyFromTheEnvironment(t *testing.T) {
	var proxied atomic.Int64
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxied.Add(1)
	}))
	defer proxy.Close()
	onlyProxyVars(t, map[string]string{"HTTP_PROXY": proxy.URL, "HTTPS_PROXY": proxy.URL, "NO_PROXY": ""})

	// As net/http stands, this process's environment sends a request elsewhere to the proxy.
	before := &http.Client{Transport: &http.Transport{Proxy: http.ProxyFromEnvironment}, Timeout: 5 * time.Second}
	if resp, err := before.Get(away); err != nil {
		t.Fatalf("the proxy in the environment was not used to begin with: %v", err)
	} else {
		resp.Body.Close()
	}
	if proxied.Load() != 1 {
		t.Fatalf("the proxy saw %d requests, want 1", proxied.Load())
	}

	set := IgnoreEnvProxies()
	if want := []string{"HTTPS_PROXY", "HTTP_PROXY", "NO_PROXY"}; !slices.Equal(set, want) {
		t.Errorf("IgnoreEnvProxies() = %v, want %v", set, want)
	}
	// The default client, and a transport copied from the default after, as SDKs make theirs.
	copied := &http.Client{Transport: http.DefaultTransport.(*http.Transport).Clone(), Timeout: 300 * time.Millisecond}
	for name, c := range map[string]*http.Client{"default": {Timeout: 300 * time.Millisecond}, "copied": copied} {
		if resp, err := c.Get(away); err == nil {
			resp.Body.Close()
			t.Errorf("%s client: a request to %s got an answer", name, away)
		}
	}
	if proxied.Load() != 1 {
		t.Errorf("the proxy saw %d requests after IgnoreEnvProxies, want only the first", proxied.Load())
	}
}
