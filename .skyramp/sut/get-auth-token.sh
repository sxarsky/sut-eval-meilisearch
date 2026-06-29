#!/usr/bin/env bash
set -euo pipefail

MEILI_URL="http://localhost:7700"
MASTER_KEY="testbot-master-key"

# Wait for Meilisearch to be healthy
echo "Waiting for Meilisearch to be ready..." >&2
TIMEOUT=300
ELAPSED=0
until curl -sf "${MEILI_URL}/health" > /dev/null 2>&1; do
  if [ "${ELAPSED}" -ge "${TIMEOUT}" ]; then
    echo "Timeout waiting for Meilisearch" >&2
    exit 1
  fi
  sleep 2
  ELAPSED=$((ELAPSED + 2))
done
echo "Meilisearch is ready." >&2

# Seed sample documents for list/search/filter tests
echo "Seeding sample data..." >&2

# Create a 'movies' index and add documents
TASK_RESPONSE=$(curl -sf -X POST "${MEILI_URL}/indexes/movies/documents" \
  -H "Authorization: Bearer ${MASTER_KEY}" \
  -H "Content-Type: application/json" \
  -d '[
    {"id": 1, "title": "Interstellar", "genre": "sci-fi", "year": 2014, "rating": 8.6},
    {"id": 2, "title": "The Dark Knight", "genre": "action", "year": 2008, "rating": 9.0},
    {"id": 3, "title": "Inception", "genre": "sci-fi", "year": 2010, "rating": 8.8},
    {"id": 4, "title": "Pulp Fiction", "genre": "crime", "year": 1994, "rating": 8.9},
    {"id": 5, "title": "The Matrix", "genre": "sci-fi", "year": 1999, "rating": 8.7}
  ]' 2>&1) || echo "Seed movies failed (non-fatal)" >&2

# Create a 'books' index and add documents
curl -sf -X POST "${MEILI_URL}/indexes/books/documents" \
  -H "Authorization: Bearer ${MASTER_KEY}" \
  -H "Content-Type: application/json" \
  -d '[
    {"id": 1, "title": "The Hitchhikers Guide to the Galaxy", "author": "Douglas Adams", "year": 1979},
    {"id": 2, "title": "Dune", "author": "Frank Herbert", "year": 1965},
    {"id": 3, "title": "Foundation", "author": "Isaac Asimov", "year": 1951}
  ]' > /dev/null 2>&1 || echo "Seed books failed (non-fatal)" >&2

echo "Seed complete." >&2

# Output only the master key to stdout for Testbot to capture
printf '%s' "${MASTER_KEY}"
