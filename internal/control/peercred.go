package control

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"os/user"
	"strconv"
	"unicode"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/jchen0426/agentos/internal/audit"
)

// PeerCredentials are the Control socket's transport credentials. At each connection's handshake
// they ask the kernel for the uid of the process at the other end of the Unix socket, and every
// call on the connection carries it (Peer): the audit log records who made each change from what
// the kernel says, not from what the caller says. Nothing is encrypted: the socket is in a 0700
// directory, and the bytes never leave the kernel.
func PeerCredentials() credentials.TransportCredentials { return peerCreds{} }

// Peer is what PeerCredentials learned of a connection.
type Peer struct {
	credentials.CommonAuthInfo
	UID int64
}

func (Peer) AuthType() string { return "agentd-peercred" }

type peerCreds struct{}

func (peerCreds) ServerHandshake(c net.Conn) (net.Conn, credentials.AuthInfo, error) {
	uc, ok := c.(*net.UnixConn)
	if !ok {
		return nil, nil, fmt.Errorf("the control socket takes Unix connections only, not %T", c)
	}
	raw, err := uc.SyscallConn()
	if err != nil {
		return nil, nil, err
	}
	var uid int64
	var uidErr error
	if err := raw.Control(func(fd uintptr) { uid, uidErr = peerUID(int(fd)) }); err != nil {
		return nil, nil, err
	}
	if uidErr != nil {
		return nil, nil, fmt.Errorf("the control socket's peer credentials: %w", uidErr)
	}
	return c, Peer{CommonAuthInfo: credentials.CommonAuthInfo{SecurityLevel: credentials.NoSecurity}, UID: uid}, nil
}

func (peerCreds) ClientHandshake(context.Context, string, net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return nil, nil, errors.New("the control socket's peer credentials are for its server only")
}

func (peerCreds) Info() credentials.ProtocolInfo {
	return credentials.ProtocolInfo{SecurityProtocol: "insecure"}
}

func (c peerCreds) Clone() credentials.TransportCredentials { return c }

func (peerCreds) OverrideServerName(string) error { return nil }

// operator is who makes a call: the user its connection's peer runs as, by name (or uid:<n> when
// the system has no name for it), with the uid. A call that came without peer credentials makes
// no change: its record would name nobody.
func operator(ctx context.Context) (audit.Who, error) {
	uid, err := peerUIDOf(ctx)
	if err != nil {
		return audit.Who{}, err
	}
	return audit.Who{Actor: audit.Operator(operatorName(uid, user.LookupId)), PeerUID: sql.NullInt64{Int64: uid, Valid: true}}, nil
}

// operatorName is the user name of uid, as lookup gives it, when it can stand in a record as it is;
// otherwise uid:<n>.
func operatorName(uid int64, lookup func(string) (*user.User, error)) string {
	if u, err := lookup(strconv.FormatInt(uid, 10)); err == nil && plainName(u.Username) {
		return u.Username
	}
	return "uid:" + strconv.FormatInt(uid, 10)
}

// peerUIDOf is the uid PeerCredentials gave the call's connection.
func peerUIDOf(ctx context.Context) (int64, error) {
	if p, ok := peer.FromContext(ctx); ok {
		if info, ok := p.AuthInfo.(Peer); ok {
			return info.UID, nil
		}
	}
	return 0, status.Error(codes.Unauthenticated, "the control socket gave no peer credentials for this call: "+
		"agentd records who makes each change")
}

// plainName reports whether a user name the system gave can stand in a record as it is: printable,
// without spaces, of at most 100 runes.
func plainName(s string) bool {
	n := 0
	for _, r := range s {
		if n++; n > 100 || !unicode.IsPrint(r) || unicode.IsSpace(r) {
			return false
		}
	}
	return n > 0
}
