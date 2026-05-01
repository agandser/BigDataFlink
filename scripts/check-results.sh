#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
docker compose exec -T postgres psql -U postgres -d pet_shop < ./sql/check_results.sql
