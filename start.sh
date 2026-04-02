
#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

ACTION="${1:-up}"

has_gpu() {
  command -v nvidia-smi >/dev/null 2>&1 \
    && docker info --format '{{json .Runtimes}}' 2>/dev/null | grep -qi 'nvidia'
}

compose_up() {
  if has_gpu; then
    echo "GPU detected. Starting with docker-compose.gpu.yml"
    docker compose -f docker-compose.yml -f docker-compose.gpu.yml up -d --build
  else
    echo "No usable Docker GPU runtime detected. Starting in CPU mode"
    docker compose up -d --build
  fi
}

case "$ACTION" in
  up)
    compose_up
    ;;
  clean)
    docker compose down -v --remove-orphans
    compose_up
    ;;
  down)
    docker compose down --remove-orphans
    ;;
  rebuild)
    docker compose down --remove-orphans
    compose_up
    ;;
  logs)
    docker compose logs -f "${2:-}"
    ;;
  ps)
    docker compose ps
    ;;
  *)
    echo "Usage: $0 {up|clean|down|rebuild|logs [service]|ps}"
    exit 1
    ;;
esac