"""The harness itself, where the black-box tests depend on more than it shows."""

import errno
import socket

import pytest

from tests.harness import free_port


def test_free_port_holds_its_port_for_the_server() -> None:
    """The Temporal dev server, given free_port()'s port for its frontend, asked the OS for its
    worker service's port and got the same one: it answered, then died (CI runs 39181890891 and
    39191441167). Right after free_port() the port is taken: a bind without SO_REUSEADDR is
    refused, while a server binding it with SO_REUSEADDR, as Go does, gets it. (That Linux also
    keeps it from binds to port 0 and outgoing connections, for 60 s, takes a narrowed port range
    to show: docs/m1-notes.md.)"""
    port = free_port()
    with socket.socket() as other, pytest.raises(OSError) as refused:
        other.bind(("127.0.0.1", port))
    assert refused.value.errno == errno.EADDRINUSE
    with socket.socket() as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(("127.0.0.1", port))
        server.listen()
