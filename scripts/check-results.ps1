$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")
$sql = Get-Content -Raw -Encoding UTF8 ".\\sql\\check_results.sql"
$sql | docker compose exec -T postgres psql -U postgres -d pet_shop
