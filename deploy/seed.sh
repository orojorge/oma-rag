#!/usr/bin/env bash
# Seed local OpenSearch with ingestion artifacts.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

# Load OPENSEARCH_HOST from .env if present
if [ -f "$REPO_ROOT/.env" ]; then
  export $(grep -v '^#' "$REPO_ROOT/.env" | grep OPENSEARCH_HOST | xargs)
fi
OPENSEARCH_URL="${OPENSEARCH_HOST:-http://localhost:9200}"

echo "Waiting for OpenSearch at $OPENSEARCH_URL ..."
until curl -s "$OPENSEARCH_URL" > /dev/null 2>&1; do
  sleep 2
done
echo "OpenSearch is ready."

cd "$REPO_ROOT/ingestion"
python streamer.py --target local