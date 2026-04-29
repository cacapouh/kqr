#!/usr/bin/env bash
# Operational check for kqr.
#
#   scripts/check.sh                # full local check (fmt, build, test, clippy)
#   scripts/check.sh --if-changed   # skip if no Rust/Cargo files differ from HEAD
#   scripts/check.sh --integration  # also boot Kafka via docker compose
#                                   #   (real integration tests land in step 8)
#
# Designed to be cheap on the happy path so it can be wired into the
# Claude Code Stop hook (.claude/settings.json) without hurting flow.

set -euo pipefail

cd "$(dirname "$0")/.."
ROOT="$(pwd)"

if_changed=false
integration=false
for arg in "$@"; do
    case "$arg" in
        --if-changed) if_changed=true ;;
        --integration) integration=true ;;
        -h|--help)
            sed -n '2,11p' "$0" | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        *)
            echo "[check] unknown arg: $arg" >&2
            exit 2
            ;;
    esac
done

if $if_changed; then
    if ! git -C "$ROOT" rev-parse --git-dir >/dev/null 2>&1; then
        echo "[check] not a git repo — skipping --if-changed gate"
    else
        tracked_diff=$(git -C "$ROOT" diff --name-only HEAD -- '*.rs' '*.toml' Cargo.lock 2>/dev/null || true)
        untracked=$(git -C "$ROOT" ls-files --others --exclude-standard -- '*.rs' '*.toml' 2>/dev/null || true)
        if [[ -z "$tracked_diff" && -z "$untracked" ]]; then
            echo "[check] no Rust/Cargo changes since HEAD — skipping"
            exit 0
        fi
    fi
fi

step() { echo "[check] $*"; }

step "cargo fmt --all -- --check"
cargo fmt --all -- --check

step "cargo build --workspace"
cargo build --workspace --quiet

step "cargo test --workspace"
cargo test --workspace --quiet

step "cargo clippy --workspace --all-targets -- -D warnings"
cargo clippy --workspace --all-targets --quiet -- -D warnings

if $integration; then
    if ! command -v docker >/dev/null 2>&1; then
        echo "[check] docker not found — cannot run --integration" >&2
        exit 3
    fi
    step "docker compose up Kafka"
    docker compose -f "$ROOT/docker/compose.yaml" up -d --wait
    trap 'docker compose -f "$ROOT/docker/compose.yaml" down -v >/dev/null 2>&1 || true' EXIT
    step "integration tests (placeholder — wired in step 8)"
fi

step "OK"
