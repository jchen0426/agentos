# Agent OS v0.1 (M1). See README.md.
VM ?= agentos

.PHONY: build build-linux proto sqlc test test-srt test-linux vm temporal-server

# githubsim is the black-box tests' stand-in for GitHub.
build:
	go build -o bin/ ./cmd/agentd ./cmd/agentctl ./cmd/githubsim

build-linux:
	GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -o bin/linux-arm64/ ./cmd/agentd ./cmd/agentctl ./cmd/githubsim

proto:
	./scripts/gen-proto.sh

sqlc:
	sqlc generate

# Workers run unsandboxed (development only).
test: build
	uv run pytest

# macOS: Workers run under sandbox-runtime (Seatbelt).
test-srt: build
	cd sandbox/macos && npm ci --no-fund --no-audit
	AGENTOS_SANDBOX=srt uv run pytest

# Linux: the same suite in the Lima VM, Workers under bubblewrap.
test-linux: build-linux
	limactl shell $(VM) -- bash -c 'cd $(CURDIR) && AGENTOS_SANDBOX=bwrap AGENTOS_BIN=bin/linux-arm64 ~/agentos-venv/bin/python -m pytest'

# The Lima VM for the Linux side, with a Python environment for the tests.
vm:
	limactl create --name=$(VM) vm/lima/agentos.yaml
	limactl start $(VM)
	limactl shell $(VM) -- bash -c 'python3 -m venv ~/agentos-venv && ~/agentos-venv/bin/pip install -e $(CURDIR) "pytest>=9" "grpcio>=1.84" "grpcio-tools>=1.84"'

# The full Temporal server, for tests/test_temporal_mtls.py.
temporal-server:
	./scripts/fetch-temporal-server.sh
