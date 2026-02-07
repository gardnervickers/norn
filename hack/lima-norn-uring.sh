#!/usr/bin/env bash
set -euo pipefail

INSTANCE_NAME="${LIMA_INSTANCE_NAME:-norn-uring}"
HOST_MOUNT="${LIMA_HOST_MOUNT:-$HOME}"
REPO_HOST_PATH="${REPO_HOST_PATH:-$(git -C "${PWD}" rev-parse --show-toplevel 2>/dev/null || pwd)}"
NIX_BIN="/nix/var/nix/profiles/default/bin/nix"

usage() {
  cat <<'EOF'
Usage: hack/lima-norn-uring.sh <up|shell|test>

Commands:
  up     Create/start a Lima Linux VM and ensure Nix is installed.
  shell  Enter the VM and open `nix develop` in this repository.
  test   Run `cargo test -p norn-uring` inside the VM via `nix develop`.

Environment:
  LIMA_INSTANCE_NAME  VM instance name (default: norn-uring)
  LIMA_HOST_MOUNT     Host path mounted into VM as writable (default: $HOME)
  REPO_HOST_PATH      Repository host path visible in VM (default: git root / pwd)
EOF
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

instance_exists() {
  limactl list -q 2>/dev/null | grep -Fxq "${INSTANCE_NAME}"
}

instance_running() {
  [[ "$(limactl list --format '{{.Status}}' "${INSTANCE_NAME}" 2>/dev/null || true)" == "Running" ]]
}

ensure_instance() {
  if instance_exists; then
    if ! instance_running; then
      limactl start "${INSTANCE_NAME}" --tty=false
    fi
    return
  fi

  limactl start \
    --name="${INSTANCE_NAME}" \
    --mount-none \
    --mount "${HOST_MOUNT}:w" \
    template:debian \
    --tty=false
}

vm_mount_is_rw() {
  limactl shell "${INSTANCE_NAME}" -- env HOST_MOUNT="${HOST_MOUNT}" sh -lc \
    'mount | grep -F " on $HOST_MOUNT " | grep -q "(rw,"'
}

ensure_writable_mount() {
  if vm_mount_is_rw; then
    return
  fi

  limactl stop "${INSTANCE_NAME}"
  limactl edit "${INSTANCE_NAME}" --mount-none --tty=false
  limactl edit "${INSTANCE_NAME}" --mount "${HOST_MOUNT}:w" --tty=false
  limactl start "${INSTANCE_NAME}" --tty=false
}

ensure_nix() {
  if limactl shell "${INSTANCE_NAME}" -- sh -lc "[ -x '${NIX_BIN}' ]"; then
    return
  fi

  limactl shell "${INSTANCE_NAME}" -- sh -lc \
    'curl -fsSL https://install.determinate.systems/nix | sh -s -- install --no-confirm'
}

ensure_repo_visible() {
  limactl shell "${INSTANCE_NAME}" -- env REPO_HOST_PATH="${REPO_HOST_PATH}" sh -lc \
    '[ -d "$REPO_HOST_PATH" ]'
}

up() {
  require_cmd limactl
  ensure_instance
  ensure_writable_mount
  ensure_nix

  if ! ensure_repo_visible; then
    cat >&2 <<EOF
Repository path is not visible in the VM:
  ${REPO_HOST_PATH}
Adjust LIMA_HOST_MOUNT or REPO_HOST_PATH and rerun.
EOF
    exit 1
  fi
}

open_shell() {
  up
  exec limactl shell "${INSTANCE_NAME}" -- env REPO_HOST_PATH="${REPO_HOST_PATH}" bash -lc \
    'set -euo pipefail; export PATH=/nix/var/nix/profiles/default/bin:$PATH; cd "$REPO_HOST_PATH"; exec nix develop'
}

run_tests() {
  up
  limactl shell "${INSTANCE_NAME}" -- env REPO_HOST_PATH="${REPO_HOST_PATH}" bash -lc \
    'set -euo pipefail; export PATH=/nix/var/nix/profiles/default/bin:$PATH; cd "$REPO_HOST_PATH"; nix develop -c cargo test -p norn-uring'
}

case "${1:-}" in
  up)
    up
    ;;
  shell)
    open_shell
    ;;
  test)
    run_tests
    ;;
  *)
    usage
    exit 1
    ;;
esac
