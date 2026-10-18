// Package gitenv is the environment every git command agentos runs on a mirror gets, in agentd
// and in the git tool server alike. The commits are untrusted, the mirror's own configuration
// is not.
package gitenv

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// EmptyTree is git's empty tree: attributes are read from it, so a commit's own
// .gitattributes cannot mark its change binary and hide it from a diff.
const EmptyTree = "4b825dc642cb6eb9a060e54bf8d69288fbee4904"

// AllocLimit is the most git may allocate at once. A crafted repository can hold a commit, a
// tree or a file of gigabytes that packs into megabytes, and git reads each one whole; past
// the limit git stops with "attempting to allocate". Diffing two files of
// gitmcp.MaxFileBytes takes one allocation of up to 128 MiB.
const AllocLimit = "256m"

// Environ is the process's environment for git: no user or system configuration, no
// attributes from the commits, no replace refs (which could swap a commit or a file for
// another), paths taken literally and byte for byte, no optional locks, and the allocation
// limit. No GIT_ variable gets through from outside: sandbox-runtime sets
// GIT_CONFIG_PARAMETERS, for one. (A repository made on macOS sets core.precomposeUnicode,
// and git would then turn a path given in decomposed Unicode into another one.)
func Environ(extra ...string) []string {
	env := slices.DeleteFunc(os.Environ(), func(e string) bool { return strings.HasPrefix(e, "GIT_") })
	return append(append(env,
		"GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL="+os.DevNull, "GIT_TERMINAL_PROMPT=0", "GIT_LITERAL_PATHSPECS=1",
		"GIT_OPTIONAL_LOCKS=0", "GIT_NO_REPLACE_OBJECTS=1", "GIT_ATTR_NOSYSTEM=1", "GIT_ATTR_SOURCE="+EmptyTree,
		"GIT_ALLOC_LIMIT="+AllocLimit,
		"GIT_CONFIG_COUNT=1", "GIT_CONFIG_KEY_0=core.precomposeUnicode", "GIT_CONFIG_VALUE_0=false",
	), extra...)
}

// ForMirror is Environ for git commands run in the mirror (git -C mirror): git looks for the
// repository there and no further up. A mirror that is not a repository yet, inside one that
// is (a checkout), is an error, not the enclosing repository.
func ForMirror(mirror string, extra ...string) []string {
	if real, err := filepath.EvalSymlinks(mirror); err == nil {
		mirror = real // git compares real paths
	}
	return Environ(append([]string{"GIT_CEILING_DIRECTORIES=" + filepath.Dir(filepath.Clean(mirror))}, extra...)...)
}

// TooLarge reports whether git's error output says it ran out of the memory it may use: it
// stopped at AllocLimit, or (under the Linux launcher's address-space limit) a malloc or mmap
// failed. The same commit always will.
func TooLarge(stderr string) bool {
	for _, s := range []string{"attempting to allocate", "Out of memory, malloc failed", "mmap failed"} {
		if strings.Contains(stderr, s) {
			return true
		}
	}
	return false
}
