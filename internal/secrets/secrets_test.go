package secrets

import (
	"os/user"
	"strings"
	"testing"

	"github.com/zalando/go-keyring"
)

func TestResolve(t *testing.T) {
	keyring.MockInit() // an in-memory keyring for this process
	me, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}
	if err := keyring.Set("agentos-test", me.Username, "sk-ant-mine\n"); err != nil {
		t.Fatal(err)
	}
	if err := keyring.Set("agentos-test", "someone", "sk-ant-theirs"); err != nil {
		t.Fatal(err)
	}
	for ref, want := range map[string]string{
		"keyring:agentos-test":         "sk-ant-mine", // the account defaults to this user
		"keyring:agentos-test/someone": "sk-ant-theirs",
	} {
		if got, err := Resolve(ref); err != nil || got != want {
			t.Errorf("Resolve(%q) = %q, %v", ref, got, err)
		}
	}
	for _, ref := range []string{"", "keychain:agentos-test", "keyring:", "keyring:/someone", "keyring:missing", "sk-ant-raw-key"} {
		_, err := Resolve(ref)
		if err == nil {
			t.Errorf("Resolve(%q): no error", ref)
		} else if strings.Contains(err.Error(), "sk-ant-") {
			t.Errorf("Resolve(%q): the error shows a key: %v", ref, err)
		}
	}
}

func TestMayBeCut(t *testing.T) {
	for length, want := range map[int]bool{PromptLimit - 1: false, PromptLimit: true, PromptLimit + 1: false, 164: false} {
		if got := MayBeCut(strings.Repeat("k", length)); got != want {
			t.Errorf("MayBeCut of %d characters = %v", length, got)
		}
	}
}
