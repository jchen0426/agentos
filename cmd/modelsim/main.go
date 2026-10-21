// modelsim serves internal/modelsim on a loopback port, for the black-box tests: the stand-in
// for Anthropic, OpenAI and Ollama that T13 injects faults through, and that the evaluation's
// tests script answers through.
package main

import (
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"

	"github.com/jchen0426/agentos/internal/modelsim"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:0", "address to listen on")
	addrFile := flag.String("addr-file", "", "write the address it listens on to this file")
	logPath := flag.String("log", "", "log every request here, one JSON line each")
	faults := flag.String("faults", "", "fault file (modelsim's package comment), read on every answer")
	answers := flag.String("answers", "", "answers file (modelsim's package comment), read on every answer")
	flag.Parse()

	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		fmt.Fprintln(os.Stderr, "modelsim:", err)
		os.Exit(1)
	}
	if *addrFile != "" {
		if err := os.WriteFile(*addrFile, []byte(ln.Addr().String()), 0o600); err != nil {
			fmt.Fprintln(os.Stderr, "modelsim:", err)
			os.Exit(1)
		}
	}
	s := &modelsim.Server{LogPath: *logPath, FaultsPath: *faults, AnswersPath: *answers}
	fmt.Fprintln(os.Stderr, "modelsim listening on", ln.Addr())
	if err := http.Serve(ln, s); err != nil {
		fmt.Fprintln(os.Stderr, "modelsim:", err)
		os.Exit(1)
	}
}
