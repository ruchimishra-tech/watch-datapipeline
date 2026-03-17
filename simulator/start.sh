#!/usr/bin/env bash
# Start the full Watch observability stack + simulators.
# Run from the project root: ./simulator/start.sh

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

echo "==> Starting core observability stack..."
docker compose -f infra/docker-compose.yml up -d

echo "==> Waiting for collector to be healthy..."
for i in $(seq 1 20); do
  if curl -sf http://localhost:13133/ > /dev/null 2>&1; then
    echo "    Collector is up."
    break
  fi
  echo "    Waiting... ($i/20)"
  sleep 3
done

echo "==> Building simulator images..."
docker compose -f infra/docker-compose.yml -f infra/docker-compose.simulator.yml build

echo "==> Starting simulators..."
docker compose -f infra/docker-compose.yml -f infra/docker-compose.simulator.yml up -d pipeline-simulator streaming-simulator

echo ""
echo "============================================================"
echo "  Watch Observability Stack is running!"
echo "============================================================"
echo ""
echo "  Grafana      http://localhost:3000   (admin / admin)"
echo "  Prometheus   http://localhost:9090"
echo "  Tempo        http://localhost:3200"
echo "  Alertmanager http://localhost:9093"
echo "  Collector    http://localhost:13133  (health)"
echo ""
echo "  Simulator metrics:"
echo "    Pipeline:  http://localhost:8099/metrics"
echo "    Streaming: http://localhost:8098/metrics"
echo ""
echo "  Dashboards to open in Grafana:"
echo "    L1 Estate:      uid=watch-l1-estate"
echo "    L2 Pipeline:    uid=watch-l2-pipeline"
echo "    L2 Streaming:   uid=watch-l2-streaming"
echo "    L3 Infra:       uid=watch-l3-infra"
echo ""
echo "  To tail simulator logs:"
echo "    docker compose -f infra/docker-compose.yml -f infra/docker-compose.simulator.yml logs -f pipeline-simulator"
echo ""
echo "  To stop everything:"
echo "    docker compose -f infra/docker-compose.yml -f infra/docker-compose.simulator.yml down"
echo "============================================================"
