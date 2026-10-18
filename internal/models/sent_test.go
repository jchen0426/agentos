package models

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
)

// Both adapters tell a request that never reached the provider (its reservation is released)
// from one that did and then failed (charged in full: the provider may have billed it) by
// whether the request was written out whole, not by the error that came back.
func TestSentIsWhetherTheRequestWasWritten(t *testing.T) {
	blobs := memBlobs{}
	adapters := map[string]func(base string) (Adapter, Request){
		"anthropic": func(base string) (Adapter, Request) {
			return NewAnthropic("anthropic", "sk-test", base, blobs), auditRequest(t, blobs)
		},
		"openai": func(base string) (Adapter, Request) {
			a, err := NewOpenAICompat("openai", Provider{Adapter: "openai_compat", API: "responses", BaseURL: base + "/v1"}, "sk", "", blobs)
			if err != nil {
				t.Fatal(err)
			}
			return a, openaiRequest(t, blobs, "")
		},
	}
	tr := http.DefaultTransport.(*http.Transport) // both SDKs send through it

	for name, adapter := range adapters {
		// Read whole, then the TLS connection breaks while the answer is awaited: a record of
		// another TLS version, which crypto/tls reports as a RecordHeaderError, as it does for
		// a server that never spoke TLS at all.
		var received atomic.Int32
		srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/api/show" {
				http.NotFound(w, r) // the server is on this machine: OpenAI's adapter asks it first
				return
			}
			_, _ = io.ReadAll(r.Body)
			received.Add(1)
			conn, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			raw := conn.(*tls.Conn).NetConn()
			_, _ = raw.Write([]byte{0x17, 0x03, 0x01, 0x00, 0x10, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
			raw.Close()
		}))
		srv.StartTLS()
		pool := x509.NewCertPool()
		pool.AddCert(srv.Certificate())
		saved := tr.TLSClientConfig
		tr.TLSClientConfig = &tls.Config{RootCAs: pool}
		a, req := adapter(srv.URL)
		_, err := a.Call(context.Background(), req)
		tr.TLSClientConfig = saved
		srv.Close()
		if received.Load() != 1 || err == nil || errors.Is(err, ErrNotSent) {
			t.Errorf("%s: received %d, then %v: a request the provider read is not 'not sent'", name, received.Load(), err)
		}

		// The same over HTTP/2: the server reads the request whole, then resets the stream.
		var h2 atomic.Int32
		srv = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/api/show" {
				http.NotFound(w, r)
				return
			}
			_, _ = io.ReadAll(r.Body)
			if r.ProtoMajor == 2 {
				h2.Add(1)
			}
			panic(http.ErrAbortHandler)
		}))
		srv.EnableHTTP2 = true
		srv.StartTLS()
		pool = x509.NewCertPool()
		pool.AddCert(srv.Certificate())
		tr.TLSClientConfig = &tls.Config{RootCAs: pool, NextProtos: []string{"h2", "http/1.1"}}
		a, req = adapter(srv.URL)
		_, err = a.Call(context.Background(), req)
		tr.TLSClientConfig = saved
		srv.Close()
		if h2.Load() != 1 || err == nil || errors.Is(err, ErrNotSent) {
			t.Errorf("%s: %d HTTP/2 requests read, then %v: a request the provider read is not 'not sent'", name, h2.Load(), err)
		}

		// A proxy refuses the tunnel: the request never reached the provider.
		proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusForbidden)
		}))
		u, _ := url.Parse(proxy.URL)
		savedProxy := tr.Proxy
		tr.Proxy = http.ProxyURL(u)
		a, req = adapter("https://provider.example")
		_, err = a.Call(context.Background(), req)
		tr.Proxy = savedProxy
		proxy.Close()
		if !errors.Is(err, ErrNotSent) {
			t.Errorf("%s: the proxy refused the tunnel, yet %v", name, err)
		}
	}
}
