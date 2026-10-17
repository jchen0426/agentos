"""Can Temporal be set up to accept only connections that present agentd's certificate?

`temporal server start-dev` cannot: it has no TLS options at all (CLI 1.9.1, server 1.32.0).
This test runs the same server version from the full release binary, with SQLite and a
frontend that requires client certificates from a CA that signs only agentd (and the
server's own services). It checks that every connection without agentd's certificate is
refused, and that the whole flow still works: agentd holds the only client certificate and
the Worker keeps reaching Temporal through agentd, never with a certificate of its own.

Needs the release binary: scripts/fetch-temporal-server.sh, or AGENTOS_TEMPORAL_SERVER.
"""

import asyncio
import os
import platform
import subprocess
from pathlib import Path

import pytest
from temporalio.api.enums.v1 import IndexedValueType
from temporalio.api.operatorservice.v1 import AddSearchAttributesRequest
from temporalio.api.workflowservice.v1 import GetSystemInfoRequest, RegisterNamespaceRequest
from temporalio.client import Client
from temporalio.common import SearchAttributeKey, SearchAttributePair, TypedSearchAttributes
from temporalio.service import TLSConfig
from google.protobuf.duration_pb2 import Duration

from tests.harness import REPO, TERMINAL, Proc, Stack, free_port, wait_for

_platform = f"{platform.system().lower()}-{ {'x86_64': 'amd64', 'aarch64': 'arm64'}.get(platform.machine(), platform.machine())}"
SERVER = Path(os.environ.get("AGENTOS_TEMPORAL_SERVER", REPO / "bin/temporal-server" / _platform / "temporal-server"))
pytestmark = pytest.mark.skipif(not SERVER.exists(), reason="run scripts/fetch-temporal-server.sh first")


def make_pki(d: Path) -> None:
    """A CA that signs only agentd and the server, and a rogue CA with an 'agentd' look-alike."""
    d.mkdir()

    def ossl(*args: object) -> None:
        subprocess.run(["openssl", *map(str, args)], check=True, capture_output=True)

    for ca in ("ca", "rogue-ca"):
        ossl("req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2", "-subj", f"/CN=agentos test {ca}",
             "-addext", "basicConstraints=critical,CA:TRUE", "-addext", "keyUsage=critical,keyCertSign,cRLSign",
             "-keyout", d / f"{ca}.key", "-out", d / f"{ca}.pem")

    def leaf(name: str, ca: str, cn: str, ext: str) -> None:
        (d / f"{name}.ext").write_text("basicConstraints=CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\n" + ext)
        ossl("req", "-new", "-newkey", "rsa:2048", "-nodes", "-subj", f"/CN={cn}",
             "-keyout", d / f"{name}.key", "-out", d / f"{name}.csr")
        ossl("x509", "-req", "-in", d / f"{name}.csr", "-CA", d / f"{ca}.pem", "-CAkey", d / f"{ca}.key",
             "-CAcreateserial", "-days", "2", "-extfile", d / f"{name}.ext", "-out", d / f"{name}.pem")

    # The server's certificate is also its client certificate between its own services.
    leaf("server", "ca", "localhost", "extendedKeyUsage=serverAuth,clientAuth\nsubjectAltName=DNS:localhost,IP:127.0.0.1\n")
    leaf("agentd", "ca", "agentd", "extendedKeyUsage=clientAuth\n")
    leaf("rogue", "rogue-ca", "agentd", "extendedKeyUsage=clientAuth\n")


def server_config(pki: Path, ports: list[int]) -> str:
    frontend, frontend_m, http, matching, matching_m, history, history_m, worker, worker_m = ports
    tls_server = (f"{{certFile: {pki}/server.pem, keyFile: {pki}/server.key, "
                  f"requireClientAuth: true, clientCaFiles: [{pki}/ca.pem]}}")
    tls_client = f"{{serverName: localhost, rootCaFiles: [{pki}/ca.pem]}}"
    sqlite = ("{sql: {pluginName: sqlite, databaseName: default, connectAddr: localhost, connectProtocol: tcp, "
              "connectAttributes: {mode: memory, cache: private}, maxConns: 1, maxIdleConns: 1, maxConnLifetime: 1h}}")
    return f"""
log: {{stdout: true, level: warn}}
persistence:
  defaultStore: sqlite-default
  visibilityStore: sqlite-visibility
  numHistoryShards: 1
  datastores:
    sqlite-default: {sqlite}
    sqlite-visibility: {sqlite}
global:
  membership: {{maxJoinDuration: 30s, broadcastAddress: 127.0.0.1}}
  tls:
    internode: {{server: {tls_server}, client: {tls_client}}}
    frontend: {{server: {tls_server}, client: {tls_client}}}
    systemWorker: {{certFile: {pki}/server.pem, keyFile: {pki}/server.key, client: {tls_client}}}
services:
  frontend: {{rpc: {{grpcPort: {frontend}, membershipPort: {frontend_m}, httpPort: {http}, bindOnLocalHost: true}}}}
  matching: {{rpc: {{grpcPort: {matching}, membershipPort: {matching_m}, bindOnLocalHost: true}}}}
  history: {{rpc: {{grpcPort: {history}, membershipPort: {history_m}, bindOnLocalHost: true}}}}
  worker: {{rpc: {{grpcPort: {worker}, membershipPort: {worker_m}, bindOnLocalHost: true}}}}
clusterMetadata:
  enableGlobalNamespace: false
  failoverVersionIncrement: 10
  masterClusterName: active
  currentClusterName: active
  clusterInformation:
    active: {{enabled: true, initialFailoverVersion: 1, rpcName: frontend,
              rpcAddress: "localhost:{frontend}", httpAddress: "localhost:{http}"}}
dcRedirectionPolicy: {{policy: noop}}
"""


def tls(pki: Path, cert: str | None, ca: str = "ca") -> TLSConfig:
    return TLSConfig(
        server_root_ca_cert=(pki / f"{ca}.pem").read_bytes() if ca else None,
        client_cert=(pki / f"{cert}.pem").read_bytes() if cert else None,
        client_private_key=(pki / f"{cert}.key").read_bytes() if cert else None,
        domain="localhost",
    )


class MTLSStack(Stack):
    def __init__(self, tmp: Path) -> None:
        self.pki = tmp / "pki"
        make_pki(self.pki)
        self.temporal_tls = tls(self.pki, "agentd")
        self.agentd_tls = {"cert_file": str(self.pki / "agentd.pem"), "key_file": str(self.pki / "agentd.key"),
                           "ca_file": str(self.pki / "ca.pem"), "server_name": "localhost"}
        super().__init__(tmp)

    def _start_temporal(self) -> None:
        ports = [free_port() for _ in range(9)]
        self.temporal_address = f"127.0.0.1:{ports[0]}"
        config = self.tmp / "temporal.yaml"
        config.write_text(server_config(self.pki, ports))
        p = Proc("temporal", [SERVER, "--config-file", config, "--allow-no-auth", "start"], self.tmp / "temporal.log")
        self.procs.append(p)
        wait_for(lambda: self._temporal_ready() or not p.alive(), "temporal-server with mTLS", timeout=90)
        assert p.alive(), p.text()

        async def setup(c: Client) -> None:
            # Unlike the dev server, the full server starts with no "default" namespace.
            await c.workflow_service.register_namespace(RegisterNamespaceRequest(
                namespace="default", workflow_execution_retention_period=Duration(seconds=86400)))
            for _ in range(100):  # the new namespace takes a moment to reach every service
                try:
                    await c.operator_service.add_search_attributes(AddSearchAttributesRequest(
                        namespace="default", search_attributes={"AgentState": IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD}))
                    break
                except Exception:
                    await asyncio.sleep(0.2)
            # The mapping reaches the frontend with its namespace cache (about 10s): wait until
            # a start that sets AgentState goes through, as agentd's StartRun does.
            key = SearchAttributeKey.for_keyword("AgentState")
            for i in range(150):
                try:
                    h = await c.start_workflow("probe", id=f"sa-probe-{i}", task_queue="sa-probe",
                                               search_attributes=TypedSearchAttributes([SearchAttributePair(key, "RUNNING")]))
                    await h.terminate()
                    return
                except Exception:
                    await asyncio.sleep(0.2)
            raise TimeoutError("AgentState never became usable")
        self._temporal(setup)


def connects(address: str, t: TLSConfig | bool) -> str:
    async def go() -> str:
        c = await Client.connect(address, tls=t)
        await c.workflow_service.get_system_info(GetSystemInfoRequest())
        return "connected"
    try:
        return asyncio.run(asyncio.wait_for(go(), timeout=15))
    except Exception as e:
        return f"refused: {e.__class__.__name__}: {str(e)[:200]}"


def test_temporal_accepts_only_agentds_certificate(tmp_path: Path) -> None:
    s = MTLSStack(tmp_path)
    try:
        addr = s.temporal_address
        results = {
            "plaintext": connects(addr, False),
            "tls without a client certificate": connects(addr, tls(s.pki, None)),
            "look-alike certificate from another CA": connects(addr, tls(s.pki, "rogue")),
            "agentd's certificate": connects(addr, tls(s.pki, "agentd")),
        }
        print(results)
        assert results.pop("agentd's certificate") == "connected"
        assert all(r.startswith("refused") for r in results.values()), results

        # The whole flow with Temporal behind mTLS: agentd presents the certificate on its
        # gateway worker, its Control client and the Runtime proxy's backend connection.
        s.start_agentd()
        s.start_worker()
        run = s.start_run()
        final = s.wait_state(run, TERMINAL, timeout=90)
        assert final["state"] == "SUCCEEDED", final
    finally:
        s.close()
