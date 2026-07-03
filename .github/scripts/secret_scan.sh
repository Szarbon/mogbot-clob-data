#!/usr/bin/env bash
# Guard: fail the workflow if any tracked, human-readable file contains a
# known secret pattern. This repo should NEVER contain credentials -- it
# only holds public market-data recordings and a data-only recorder script.
#
# Recorded tape files (recordings/**/*.jsonl.gz) are gzip-compressed, so
# `grep -I` (skip binary) naturally excludes them; this scans source,
# workflow, and doc files only.
set -euo pipefail

PATTERN='sk-[A-Za-z0-9]{20,}|sk-ant-|sk-proj-|AKIA[0-9A-Z]{16}|ASIA[0-9A-Z]{16}|ghp_|gho_|ghu_|ghs_|github_pat_|AIza[0-9A-Za-z_-]{35}|sk_live_|rk_live_|whsec_[A-Za-z0-9]{20,}|xox[baprs]-|hf_[A-Za-z0-9]{30,}|r8_[A-Za-z0-9]{30,}|BEGIN (RSA |OPENSSH |EC |DSA |PGP |)PRIVATE KEY|eyJhbGci[A-Za-z0-9_=-]{20,}\.[A-Za-z0-9_=-]+\.[A-Za-z0-9_=+/-]+|postgres://[^:]+:[^@]+@|mongodb(\+srv)?://[^:]+:[^@]+@|mysql://[^:]+:[^@]+@|redis://[^:]+:[^@]+@|discord\.com/api/webhooks/|[0-9]{8,10}:[A-Za-z0-9_-]{35}'

# Exclude this script itself (it contains the pattern strings as literal
# text) and .git internals.
FILES=$(git ls-files | grep -v '^\.github/scripts/secret_scan\.sh$')

if [ -z "$FILES" ]; then
  echo "secret-scan: no tracked files to scan."
  exit 0
fi

if echo "$FILES" | xargs -r grep -InE "$PATTERN" 2>/dev/null; then
  echo "::error::secret-scan: possible secret pattern found in a tracked file (shown above). Blocking."
  exit 1
fi

echo "secret-scan: clean."
exit 0
