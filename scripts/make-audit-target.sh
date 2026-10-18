#!/usr/bin/env bash
# Makes the audit fixture: a git repository whose last commit introduces a SQL injection, which
# the fake model's patch fixes. The black-box tests use it as the mirror of acme/payments; pushed
# to a test repository on GitHub, it is the target of the live T2 run (tests/test_t2_live.py).
# The commits carry a fixed author and date, so every copy has the same commit IDs.
#
# Usage: make-audit-target.sh <new-directory>
set -euo pipefail
[[ $# -eq 1 ]] || { echo "usage: $0 <new-directory>" >&2; exit 2; }
dir=$1
[[ ! -e $dir ]] || { echo "$0: $dir exists" >&2; exit 1; }
mkdir -p "$dir/app"
export GIT_AUTHOR_NAME=agentos GIT_AUTHOR_EMAIL=agentos@agentos.invalid
export GIT_COMMITTER_NAME=agentos GIT_COMMITTER_EMAIL=agentos@agentos.invalid
export GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_NOSYSTEM=1
git -C "$dir" init -q -b main

printf "print('hello')\n" > "$dir/app.py"
cat > "$dir/app/db.py" <<'PY'
def find_user(conn, name):
    return conn.execute("SELECT * FROM users WHERE name = ?", (name,))
PY
git -C "$dir" add .
GIT_AUTHOR_DATE=2026-10-01T00:00:00Z GIT_COMMITTER_DATE=2026-10-01T00:00:00Z git -C "$dir" commit -q -m init

cat > "$dir/app/db.py" <<'PY'
def find_user(conn, name):
    return conn.execute("SELECT * FROM users WHERE name = '%s'" % name)
PY
GIT_AUTHOR_DATE=2026-10-02T00:00:00Z GIT_COMMITTER_DATE=2026-10-02T00:00:00Z git -C "$dir" commit -q -am "find users by name"
git -C "$dir" rev-parse HEAD
