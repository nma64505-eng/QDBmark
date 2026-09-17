#!/usr/bin/env bash
set -euo pipefail

IMAGE_REPO="qfusion-db-benchmark-platform"
IMAGE_VERSION="2.3"
CONTAINER_NAME="mysql-bench-web"
HOST_PORT="${HOST_PORT:-12365}"
CONTAINER_PORT="12365"
QDBMARK_DOCKER_NETWORK="${QDBMARK_DOCKER_NETWORK:-qdbmark_net}"
QDBMARK_DOCKER_SUBNET="${QDBMARK_DOCKER_SUBNET:-10.245.0.0/24}"
QDBMARK_DOCKER_GATEWAY="${QDBMARK_DOCKER_GATEWAY:-10.245.0.1}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="${SCRIPT_DIR}/data"
REPORTS_DIR="${DATA_DIR}/reports"
UPLOADS_DIR="${DATA_DIR}/uploads"
ARTIFACTS_DIR="${DATA_DIR}/artifacts"

machine_arch="$(uname -m)"
case "${machine_arch}" in
  x86_64|amd64)
    IMAGE_ARCH="amd64"
    ;;
  arm64|aarch64)
    IMAGE_ARCH="arm64"
    ;;
  *)
    echo "ERROR: unsupported CPU architecture: ${machine_arch}" >&2
    exit 1
    ;;
esac

IMAGE_NAME="${IMAGE_REPO}:${IMAGE_VERSION}-${IMAGE_ARCH}"
IMAGE_ARCHIVE="${SCRIPT_DIR}/${IMAGE_REPO}_${IMAGE_VERSION}_${IMAGE_ARCH}.tar.gz"

if ! command -v docker >/dev/null 2>&1; then
  echo "ERROR: Docker is not installed or not in PATH." >&2
  exit 1
fi

if ! docker image inspect "${IMAGE_NAME}" >/dev/null 2>&1; then
  if [[ ! -f "${IMAGE_ARCHIVE}" ]]; then
    echo "ERROR: image ${IMAGE_NAME} not found and archive missing: ${IMAGE_ARCHIVE}" >&2
    exit 1
  fi
  echo "Loading image from ${IMAGE_ARCHIVE} ..."
  docker load -i "${IMAGE_ARCHIVE}"
fi

mkdir -p "${REPORTS_DIR}" "${UPLOADS_DIR}" "${ARTIFACTS_DIR}"

if ! docker network inspect "${QDBMARK_DOCKER_NETWORK}" >/dev/null 2>&1; then
  echo "Creating Docker network ${QDBMARK_DOCKER_NETWORK} (${QDBMARK_DOCKER_SUBNET}, gateway ${QDBMARK_DOCKER_GATEWAY}) ..."
  docker network create \
    --driver bridge \
    --subnet "${QDBMARK_DOCKER_SUBNET}" \
    --gateway "${QDBMARK_DOCKER_GATEWAY}" \
    "${QDBMARK_DOCKER_NETWORK}" >/dev/null
else
  existing_subnet="$(docker network inspect "${QDBMARK_DOCKER_NETWORK}" --format '{{range .IPAM.Config}}{{.Subnet}}{{end}}' 2>/dev/null || true)"
  if [[ -n "${existing_subnet}" && "${existing_subnet}" != "${QDBMARK_DOCKER_SUBNET}" ]]; then
    echo "ERROR: Docker network ${QDBMARK_DOCKER_NETWORK} already exists with subnet ${existing_subnet}, expected ${QDBMARK_DOCKER_SUBNET}." >&2
    echo "Remove it first with: docker network rm ${QDBMARK_DOCKER_NETWORK}" >&2
    echo "Or use another name: QDBMARK_DOCKER_NETWORK=qdbmark_net_2 QDBMARK_DOCKER_SUBNET=10.246.0.0/24 QDBMARK_DOCKER_GATEWAY=10.246.0.1 ./start-db-benchmark.sh" >&2
    exit 1
  fi
fi

if docker ps -a --format '{{.Names}}' | grep -qx "${CONTAINER_NAME}"; then
  echo "Removing existing container ${CONTAINER_NAME} ..."
  docker rm -f "${CONTAINER_NAME}" >/dev/null
fi

echo "Starting ${CONTAINER_NAME} (${IMAGE_ARCH}) on http://127.0.0.1:${HOST_PORT} ..."
echo "Docker network: ${QDBMARK_DOCKER_NETWORK} (${QDBMARK_DOCKER_SUBNET})"
docker run -d \
  --name "${CONTAINER_NAME}" \
  --restart unless-stopped \
  --network "${QDBMARK_DOCKER_NETWORK}" \
  -p "${HOST_PORT}:${CONTAINER_PORT}" \
  -e PORT="${CONTAINER_PORT}" \
  -v "${REPORTS_DIR}:/app/data/reports" \
  -v "${UPLOADS_DIR}:/app/data/uploads" \
  -v "${ARTIFACTS_DIR}:/app/data/artifacts" \
  --add-host host.docker.internal:host-gateway \
  "${IMAGE_NAME}" >/dev/null

echo "Waiting for service health check ..."
for _ in $(seq 1 30); do
  if curl -fsS "http://127.0.0.1:${HOST_PORT}/health" >/dev/null 2>&1; then
    echo "DB-Benchmark is ready: http://127.0.0.1:${HOST_PORT}"
    exit 0
  fi
  sleep 1
done

echo "Container started, but health check did not pass within 30 seconds." >&2
echo "Inspect logs with: docker logs ${CONTAINER_NAME}" >&2
exit 1
