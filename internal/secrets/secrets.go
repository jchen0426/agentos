// Package secrets resolves the key references in models.yaml. A key stays in agentd's
// memory: it is never logged, written to the ledger or handed to a Worker (D4).
package secrets

import (
	"errors"
	"fmt"
	"os/user"
	"strings"

	"github.com/zalando/go-keyring"
)

// PromptLimit is the most characters security(1) keeps when it asks for a password itself (-w
// last): it reads with getpass(3), which drops the rest without a word. OpenAI's keys are
// longer.
const PromptLimit = 128

// MayBeCut reports whether secret is exactly as long as security's prompt keeps: a longer key
// stored through it was cut there, and nothing but the provider's refusal says so.
func MayBeCut(secret string) bool {
	return len(secret) == PromptLimit
}

// Resolve reads the secret ref names. The one scheme is "keyring:<service>[/<account>]": an
// item in the OS keyring (on macOS the login keychain, read through /usr/bin/security);
// the account defaults to the user agentd runs as.
func Resolve(ref string) (string, error) {
	scheme, rest, ok := strings.Cut(ref, ":")
	if !ok || scheme != "keyring" || rest == "" {
		// Never echo the value: it may be a key pasted into the configuration by mistake.
		return "", errors.New("the key reference is not keyring:<service>[/<account>]; a key itself never goes in the configuration")
	}
	service, account, hasAccount := strings.Cut(rest, "/")
	if !hasAccount {
		u, err := user.Current()
		if err != nil {
			return "", err
		}
		account = u.Username
	}
	if service == "" || account == "" {
		return "", errors.New("the key reference names no keyring service or account")
	}
	secret, err := keyring.Get(service, account)
	switch {
	case errors.Is(err, keyring.ErrNotFound):
		return "", fmt.Errorf("no keyring item for service %q, account %q", service, account)
	case err != nil:
		return "", fmt.Errorf("read keyring item for service %q, account %q: %w", service, account, err)
	case strings.TrimSpace(secret) == "":
		return "", fmt.Errorf("the keyring item for service %q, account %q is empty", service, account)
	}
	return strings.TrimSpace(secret), nil
}
