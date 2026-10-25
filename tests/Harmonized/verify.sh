#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
dotnet restore tests/Harmonized/Harmonizer.UnitTests/Harmonizer.UnitTests.csproj --locked-mode
dotnet restore tests/Harmonized/Harmonizer.IntegrationTests/Harmonizer.IntegrationTests.csproj --locked-mode
dotnet build src/Harmonized/Harmonizer/Harmonizer.Worker/Harmonizer.Worker.csproj -c Release --no-restore
dotnet test tests/Harmonized/Harmonizer.UnitTests/Harmonizer.UnitTests.csproj -c Release --no-restore
dotnet test tests/Harmonized/Harmonizer.IntegrationTests/Harmonizer.IntegrationTests.csproj -c Release --no-restore